import ast
import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlencode
import zipfile


ROOT = Path(__file__).parent


def load_server_functions(namespace, names):
    functions = []
    for path in [ROOT / "server.py", *sorted((ROOT / "fota_server").glob("*.py"))]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        functions.extend(node for node in tree.body
                         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names)
    exec(compile(ast.Module(body=functions, type_ignores=[]), "server modules", "exec"), namespace)


class HTTPException(Exception):
    def __init__(self, status_code, detail):
        super().__init__(detail)
        self.status_code = status_code


class WebSocketDisconnect(Exception):
    pass


class TestSocket:
    def __init__(self, messages=(), fail_send=False):
        self.messages = iter(messages)
        self.sent = []
        self.fail_send = fail_send

    async def accept(self):
        pass

    async def receive_text(self):
        try:
            return json.dumps(next(self.messages))
        except StopIteration:
            raise WebSocketDisconnect()

    async def send_text(self, message):
        if self.fail_send:
            raise ConnectionError("Disconnected")
        self.sent.append(json.loads(message))


class DatabaseCursor:
    def __init__(self, database):
        self.cursor = database.cursor()

    def execute(self, query, parameters=()):
        self.cursor.execute(query.replace("%s", "?"), parameters)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


class SchemaCursor(DatabaseCursor):
    def execute(self, query, parameters=()):
        self.schema_rows = None
        if "information_schema.columns" in query:
            table, previous, current = parameters
            self.schema_rows = [(row[1],) for row in self.cursor.execute(f"PRAGMA table_info({table})")
                                if row[1] in (previous, current)]
        else:
            super().execute(query, parameters)

    def fetchall(self):
        return self.schema_rows if self.schema_rows is not None else super().fetchall()


class GithubHTTPError(Exception):
    def __init__(self, status_code):
        self.response = SimpleNamespace(status_code=status_code)


class CampaignTimeoutDecisionTests(unittest.TestCase):
    def setUp(self):
        self.namespace = {}
        load_server_functions(self.namespace, {"is_active_campaign_status", "campaign_timeout_error"})

    def test_heartbeat_timeout_at_ten_minutes(self):
        check = self.namespace["campaign_timeout_error"]
        self.assertIsNone(check("running", 401, 999, 1000))
        self.assertEqual(check("running", 400, 999, 1000)[0], "HEARTBEAT_TIMEOUT")

    def test_acknowledgement_stall_despite_heartbeats(self):
        for status in ("sent", "acknowledged"):
            self.assertEqual(self.namespace["campaign_timeout_error"](status, 999, 400, 1000)[0], "ACK_TIMEOUT")

    def test_running_stall_despite_heartbeats(self):
        for status in ("running", "executing", "in_progress", "Downloading 35%"):
            self.assertEqual(self.namespace["campaign_timeout_error"](status, 999, 400, 1000)[0], "RUNNING_TIMEOUT")

    def test_advancing_campaign_is_not_expired(self):
        self.assertIsNone(self.namespace["campaign_timeout_error"]("Downloading 35%", 999, 999, 1000))

    def test_unexecuted_and_terminal_campaigns_are_ignored(self):
        for status in ("pending", "created", "failed", "stopped", "completed", "closed", "checksum_failed"):
            self.assertIsNone(self.namespace["campaign_timeout_error"](status, 0, 0, 1000))


class CampaignServerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database = sqlite3.connect(":memory:")
        self.addCleanup(self.database.close)
        self.database.executescript("""
            CREATE TABLE campaigns (
                campaign_id TEXT PRIMARY KEY, vin TEXT, status TEXT, target_ecu TEXT,
                campaign_name TEXT, sgw_target_version TEXT, spaak_target_version TEXT,
                sgw_firmware TEXT, spaak_firmware TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                execution_started_at REAL, last_heartbeat_at REAL, last_progress_at REAL,
                failed_at REAL, error_code TEXT, error_message TEXT,
                max_retries INTEGER DEFAULT 0, retry_count INTEGER DEFAULT 0, completed_at REAL, stopped_at REAL,
                initiation TEXT DEFAULT 'immediate', scheduled_at REAL, description TEXT
            );
            CREATE TABLE campaign_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id TEXT, vin TEXT, event_type TEXT,
                status TEXT, detail TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE registered_tbms (vin TEXT PRIMARY KEY, sgw_version TEXT, spaak_version TEXT);
            INSERT INTO campaigns (campaign_id, vin, status) VALUES ('campaign-1', 'VIN1', 'sent');
            INSERT INTO registered_tbms VALUES ('VIN1', '1.0', '1.0');
        """)
        decorator = lambda route: lambda function: function
        firmware_directory = tempfile.TemporaryDirectory()
        self.addCleanup(firmware_directory.cleanup)
        (Path(firmware_directory.name) / "sgw.bin").write_bytes(b"firmware")
        (Path(firmware_directory.name) / "spaak.bin").write_bytes(b"spaak firmware")
        self.namespace = {
            "app": SimpleNamespace(post=decorator, websocket=decorator, get=decorator),
            "cursor": DatabaseCursor(self.database),
            "conn": self.database,
            "ensure_connection": lambda: None,
            "HTTPException": HTTPException,
            "WebSocket": TestSocket,
            "WebSocketDisconnect": WebSocketDisconnect,
            "connected_tbms": {}, "pending_tbms": {}, "pending_auth": {},
            "secrets": SimpleNamespace(token_hex=lambda size: "challenge"),
            "AUTH_SECRET": "secret", "json": json, "hashlib": hashlib,
            "time": time, "datetime": datetime, "timedelta": timedelta, "timezone": timezone,
            "add_log": lambda message: None,
            "asyncio": asyncio, "CAMPAIGN_ACK_TIMEOUT_SECONDS": 600,
            "asynccontextmanager": asynccontextmanager, "suppress": suppress,
            "CAMPAIGN_RUNNING_TIMEOUT_SECONDS": 600,
            "os": os, "UPLOAD_FOLDER": firmware_directory.name,
            "requests": SimpleNamespace(HTTPError=GithubHTTPError),
            "get_sha256": lambda path: "test-checksum"
        }
        self.namespace["router"] = self.namespace["app"]
        self.namespace["database"] = SimpleNamespace(
            conn=self.database, cursor=self.namespace["cursor"], ensure_connection=lambda: None,
            initialize_database=lambda: None, close_database=lambda: None
        )
        self.namespace["state"] = SimpleNamespace(
            connected_tbms=self.namespace["connected_tbms"], pending_tbms=self.namespace["pending_tbms"],
            pending_auth=self.namespace["pending_auth"], logs=[], add_log=self.namespace["add_log"]
        )
        self.namespace["settings"] = SimpleNamespace(
            AUTH_SECRET="secret", UPLOAD_FOLDER=firmware_directory.name,
            CAPTURE_FOLDER=firmware_directory.name,
            PUBLIC_URL="https://fota-server-v2-0.onrender.com",
            CAMPAIGN_ACK_TIMEOUT_SECONDS=600, CAMPAIGN_RUNNING_TIMEOUT_SECONDS=600,
            CAMPAIGN_WATCHDOG_INTERVAL_SECONDS=5, CAMPAIGN_RETRY_DELAY_SECONDS=30,
            CAMPAIGN_MAX_RETRIES=10, CAMPAIGN_AUTO_CLOSE_DAYS=30
        )
        self.namespace["storage"] = SimpleNamespace(get_sha256=self.namespace["get_sha256"])
        load_server_functions(self.namespace, {
            "stop_campaign", "websocket_endpoint", "campaign", "execute_campaign", "get_campaigns",
            "migrate_spaak_schema", "normalize_latest_firmware", "firmware_history", "version_key",
            "is_active_campaign_status", "campaign_timeout_error", "initialize_campaign_timers", "expire_stalled_campaigns",
            "close_expired_campaigns",
            "campaign_progress_value", "campaign_lifespan", "campaign_payload", "fail_campaign",
            "dispatch_waiting_campaigns", "record_campaign_event", "format_epoch", "start_scheduled_campaigns"
        })

    def status(self):
        return self.database.execute("SELECT status FROM campaigns").fetchone()[0]

    async def test_stop_active_and_notify(self):
        for status in ("sent", "acknowledged", "Downloading 35%", "running"):
            with self.subTest(status=status):
                self.database.execute("UPDATE campaigns SET status=?", (status,))
                socket = TestSocket()
                self.namespace["connected_tbms"]["VIN1"] = socket
                result = await self.namespace["stop_campaign"]("campaign-1")
                self.assertEqual(self.status(), "stopped")
                self.assertTrue(result["device_notified"])
                self.assertEqual(socket.sent, [{"type": "stop_campaign", "campaign_id": "campaign-1"}])

    async def test_terminal_campaigns_rejected(self):
        for status in ("completed", "executed", "closed", "failed", "send_failed", "checksum_failed", "transfer_failed"):
            with self.subTest(status=status):
                self.database.execute("UPDATE campaigns SET status=?", (status,))
                with self.assertRaises(HTTPException) as raised:
                    await self.namespace["stop_campaign"]("campaign-1")
                self.assertEqual(raised.exception.status_code, 409)
                self.assertEqual(self.status(), status)

    async def test_offline_stop_is_persistent_and_idempotent(self):
        for attempt in range(2):
            result = await self.namespace["stop_campaign"]("campaign-1")
            self.assertEqual(result["status"], "stopped")
            self.assertFalse(result["device_notified"])
            self.assertEqual(self.status(), "stopped")

    async def test_stop_before_execution_is_rejected(self):
        for status in ("pending", "created"):
            with self.subTest(status=status):
                self.database.execute("UPDATE campaigns SET status=?", (status,))
                with self.assertRaises(HTTPException) as raised:
                    await self.namespace["stop_campaign"]("campaign-1")
                self.assertEqual(raised.exception.status_code, 409)
                self.assertEqual(raised.exception.args[0]["code"], "CAMPAIGN_NOT_EXECUTED")
                self.assertEqual(self.status(), status)

    async def test_notification_failure_keeps_stopped_status(self):
        self.namespace["connected_tbms"]["VIN1"] = TestSocket(fail_send=True)
        result = await self.namespace["stop_campaign"]("campaign-1")
        self.assertFalse(result["device_notified"])
        self.assertEqual(self.status(), "stopped")

    async def test_missing_campaign(self):
        with self.assertRaises(HTTPException) as raised:
            await self.namespace["stop_campaign"]("missing")
        self.assertEqual(raised.exception.status_code, 404)

    async def create_draft(self):
        return await self.namespace["campaign"](
            campaign_id="draft-1", vin="VIN1", campaign_name="Gateway update",
            sgw_target_version="2.0", sgw_firmware="sgw.bin", execute=False
        )

    async def test_create_execute_stop_lifecycle(self):
        socket = TestSocket()
        self.namespace["connected_tbms"]["VIN1"] = socket
        self.assertEqual((await self.create_draft())["status"], "created")
        self.assertEqual(socket.sent, [])
        with self.assertRaises(HTTPException) as raised:
            await self.namespace["stop_campaign"]("draft-1")
        self.assertEqual(raised.exception.status_code, 409)
        result = await self.namespace["execute_campaign"]("draft-1")
        self.assertEqual(result["status"], "sent")
        self.assertEqual(socket.sent[0]["type"], "campaign")
        self.assertEqual(socket.sent[0]["sgw_firmware"], "sgw.bin")
        result = await self.namespace["stop_campaign"]("draft-1")
        self.assertEqual(result["status"], "stopped")
        with self.assertRaises(HTTPException) as raised:
            await self.namespace["execute_campaign"]("draft-1")
        self.assertEqual(raised.exception.status_code, 409)

    async def test_offline_execute_waits_for_vehicle_ping(self):
        await self.create_draft()
        self.database.execute("DELETE FROM campaigns WHERE campaign_id='campaign-1'")
        await self.send_device_messages([{"type": "heartbeat"}])
        self.assertEqual(self.status(), "pending")
        result = await self.namespace["execute_campaign"]("draft-1")
        self.assertEqual((result["status"], self.status()), ("waiting_vehicle", "waiting_vehicle"))
        with self.assertRaises(HTTPException) as raised:
            await self.namespace["execute_campaign"]("draft-1")
        self.assertEqual(raised.exception.status_code, 409)
        authentication = [
            {"type": "register_request", "vin": "VIN1"},
            {"type": "auth_response", "vin": "VIN1", "response": hashlib.sha256(b"challengesecret").hexdigest()},
            {"type": "heartbeat"}
        ]
        socket = TestSocket(authentication)
        await self.namespace["websocket_endpoint"](socket)
        self.assertEqual([message["type"] for message in socket.sent], ["challenge", "auth_passed", "approved", "campaign"])
        self.assertEqual(socket.sent[-1]["campaign_id"], "draft-1")
        row = self.database.execute("SELECT status, retry_count, execution_started_at FROM campaigns").fetchone()
        self.assertEqual(row[:2], ("sent", 0))
        self.assertIsNotNone(row[2])

    async def test_waiting_vehicle_campaign_can_be_stopped(self):
        await self.create_draft()
        await self.namespace["execute_campaign"]("draft-1")
        result = await self.namespace["stop_campaign"]("draft-1")
        self.assertEqual(result["status"], "stopped")

    async def test_duplicate_execution_sends_only_once(self):
        await self.create_draft()
        socket = TestSocket()
        self.namespace["connected_tbms"]["VIN1"] = socket
        await self.namespace["execute_campaign"]("draft-1")
        with self.assertRaises(HTTPException) as raised:
            await self.namespace["execute_campaign"]("draft-1")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(len(socket.sent), 1)

    async def test_duplicate_draft_rejected(self):
        await self.create_draft()
        with self.assertRaises(HTTPException) as raised:
            await self.create_draft()
        self.assertEqual(raised.exception.status_code, 409)

    async def test_campaigns_are_returned_newest_first(self):
        await self.create_draft()
        self.database.execute("UPDATE campaigns SET created_at='2026-11-06T09:00:00+00:00' WHERE campaign_id='campaign-1'")
        self.database.execute("UPDATE campaigns SET created_at='2026-11-06T10:00:00+00:00' WHERE campaign_id='draft-1'")
        self.database.execute("INSERT INTO campaigns (campaign_id, vin, status, created_at) VALUES ('legacy', 'VIN1', 'completed', NULL)")
        campaigns = await self.namespace["get_campaigns"]()
        self.assertEqual([campaign["campaign_id"] for campaign in campaigns], ["draft-1", "campaign-1", "legacy"])
        self.assertEqual(campaigns[0]["created_at"], "2026-11-06T10:00:00+00:00")
        self.assertIsNone(campaigns[-1]["created_at"])

    async def test_execution_preserves_creation_timestamp(self):
        await self.create_draft()
        created_at = self.database.execute("SELECT created_at FROM campaigns WHERE campaign_id='draft-1'").fetchone()[0]
        self.assertIsNotNone(created_at)
        self.namespace["connected_tbms"]["VIN1"] = TestSocket()
        await self.namespace["execute_campaign"]("draft-1")
        after_execution = self.database.execute("SELECT created_at FROM campaigns WHERE campaign_id='draft-1'").fetchone()[0]
        self.assertEqual(after_execution, created_at)

    async def test_spaak_campaign_execution_and_completion(self):
        result = await self.namespace["campaign"](
            campaign_id="spaak-campaign", vin="VIN1", campaign_name="SPAAK update",
            spaak_target_version="3.0", spaak_firmware="spaak.bin", execute=False
        )
        self.assertEqual(result["status"], "created")
        socket = TestSocket()
        self.namespace["connected_tbms"]["VIN1"] = socket
        await self.namespace["execute_campaign"]("spaak-campaign")
        payload = socket.sent[0]
        self.assertEqual(payload["target_ecu"], "SPAAK")
        self.assertEqual(payload["spaak_target_version"], "3.0")
        self.assertEqual(payload["spaak_firmware"], "spaak.bin")
        self.assertFalse(any("bcm" in key.lower() for key in payload))
        self.namespace["connected_tbms"].clear()
        await self.send_device_messages([{"type": "completed", "campaign_id": "spaak-campaign",
                                         "target_ecu": "SPAAK", "spaak_version": "3.0"}])
        self.assertEqual(self.database.execute("SELECT spaak_version FROM registered_tbms").fetchone()[0], "3.0")

    async def test_existing_database_is_migrated_without_data_loss(self):
        self.database.execute("ALTER TABLE registered_tbms RENAME COLUMN spaak_version TO bcm_version")
        self.database.execute("ALTER TABLE campaigns RENAME COLUMN spaak_target_version TO bcm_target_version")
        self.database.execute("ALTER TABLE campaigns RENAME COLUMN spaak_firmware TO bcm_firmware")
        self.database.execute("UPDATE campaigns SET target_ecu='BCM', bcm_target_version='2.0', bcm_firmware='existing.bin'")
        for attempt in range(2):
            self.namespace["migrate_spaak_schema"](SchemaCursor(self.database))
        self.assertEqual(self.database.execute("SELECT spaak_version FROM registered_tbms").fetchone()[0], "1.0")
        row = self.database.execute("SELECT target_ecu, spaak_target_version, spaak_firmware FROM campaigns").fetchone()
        self.assertEqual(row, ("SPAAK", "2.0", "existing.bin"))

    async def test_legacy_firmware_metadata_is_exposed_as_spaak(self):
        result = self.namespace["normalize_latest_firmware"]({
            "BCM": {"file": "BCM_v2.0.bin", "path": "firmware/BCM/BCM_v2.0.bin"}
        })
        self.assertNotIn("BCM", result)
        self.assertEqual(result["SPAAK"]["file"], "SPAAK_v2.0.bin")
        self.assertEqual(result["SPAAK"]["path"], "firmware/BCM/BCM_v2.0.bin")

    async def test_firmware_history_includes_new_and_existing_spaak_releases(self):
        def github_list(path):
            if path == "firmware/SGW":
                raise GithubHTTPError(404)
            filename = "SPAAK_v3.0.bin" if path == "firmware/SPAAK" else "BCM_v2.0.bin"
            return [{"type": "file", "name": filename, "download_url": "https://example.test/firmware"}]
        self.namespace["github_list"] = github_list
        history = await self.namespace["firmware_history"]()
        self.assertEqual([item["ecu"] for item in history], ["SPAAK", "SPAAK"])
        self.assertEqual([item["file"] for item in history], ["SPAAK_v3.0.bin", "SPAAK_v2.0.bin"])
        self.assertEqual(history[-1]["path"], "firmware/BCM/BCM_v2.0.bin")

    async def send_device_messages(self, messages):
        authentication = [
            {"type": "register_request", "vin": "VIN1"},
            {"type": "auth_response", "vin": "VIN1",
             "response": hashlib.sha256(b"challengesecret").hexdigest()}
        ]
        socket = TestSocket(authentication + messages)
        await self.namespace["websocket_endpoint"](socket)

    async def test_late_messages_cannot_reopen_stopped_campaign(self):
        self.database.execute("UPDATE campaigns SET status='stopped'")
        messages = [{"type": kind, "campaign_id": "campaign-1", "progress": 80,
                     "target_ecu": "BOTH", "sgw_version": "2.0", "spaak_version": "2.0"}
                    for kind in ("campaign_ack", "progress", "completed", "checksum_failed", "campaign_failed", "transfer_failed")]
        await self.send_device_messages(messages)
        self.assertEqual(self.status(), "stopped")
        versions = self.database.execute("SELECT sgw_version, spaak_version FROM registered_tbms").fetchone()
        self.assertEqual(versions, ("1.0", "1.0"))

    async def test_device_failure_is_recorded(self):
        await self.send_device_messages([{"type": "campaign_failed", "campaign_id": "campaign-1"}])
        self.assertEqual(self.status(), "failed")

    async def test_sgw_traffic_is_logged_for_approved_vehicle_only(self):
        logs = []
        self.namespace["state"].add_log = logs.append
        await self.send_device_messages([
            {"type": "sgw_capture", "status": "started", "detail": "interface=eth0"},
            {"type": "sgw_traffic", "lines": ["10:00:00 ec:fa:03:00:cb:11 > aa:bb IPv4 udp 64"], "dropped": 3},
            {"type": "tbm_log", "lines": ["10:00:01.000 [TBM] VEHICLE CONNECTION APPROVED"], "dropped": 0}
        ])
        self.assertTrue(any("[TBM][LOG] vin=VIN1 10:00:01.000 [TBM] VEHICLE CONNECTION APPROVED" in line for line in logs))
        self.assertFalse(any("[WS][RX]" in line and "tbm_log" in line for line in logs))
        self.assertTrue(any("[SGW][CAPTURE_STARTED] vin=VIN1 interface=eth0" in line for line in logs))
        self.assertTrue(any("[SGW][TRAFFIC] vin=VIN1 10:00:00 ec:fa:03:00:cb:11" in line for line in logs))
        self.assertTrue(any("[SGW][TRAFFIC_SUMMARIZED] vin=VIN1 3 more" in line for line in logs))
        self.assertFalse(any("[WS][RX]" in line and "sgw_traffic" in line for line in logs))
        logs.clear()
        await self.namespace["websocket_endpoint"](TestSocket([{"type": "sgw_traffic", "lines": ["spoof"]}]))
        self.assertFalse(any("[SGW][TRAFFIC]" in line for line in logs))

    async def test_failed_campaign_is_retried_only_when_vehicle_pings(self):
        await self.namespace["campaign"](
            campaign_id="retry-1", vin="VIN1", campaign_name="Retry update",
            sgw_target_version="2.0", sgw_firmware="sgw.bin", execute=False, max_retries=2
        )
        self.database.execute("DELETE FROM campaigns WHERE campaign_id='campaign-1'")
        socket = TestSocket()
        self.namespace["connected_tbms"]["VIN1"] = socket
        await self.namespace["execute_campaign"]("retry-1")
        self.namespace["connected_tbms"].clear()
        await self.send_device_messages([{"type": "campaign_failed", "campaign_id": "retry-1", "reason": "Download failed"}])
        self.assertEqual(self.status(), "waiting_retry")
        self.database.execute("UPDATE campaigns SET failed_at=1")
        await self.send_device_messages([{"type": "heartbeat"}])
        self.assertEqual(self.database.execute("SELECT status, retry_count FROM campaigns").fetchone(), ("sent", 1))
        await self.send_device_messages([{"type": "campaign_failed", "campaign_id": "retry-1"}])
        self.database.execute("UPDATE campaigns SET failed_at=1")
        await self.send_device_messages([{"type": "heartbeat"}])
        await self.send_device_messages([{"type": "campaign_failed", "campaign_id": "retry-1"}])
        self.assertEqual(self.database.execute("SELECT status, retry_count FROM campaigns").fetchone(), ("failed", 2))
        history = [row[0] for row in self.database.execute("SELECT event_type FROM campaign_events ORDER BY id")]
        self.assertEqual(history[:3], ["Campaign Created", "Campaign Sent", "Failed - Waiting for Retry"])
        self.assertIn("Retry 2 Triggered", history)
        self.assertEqual(history[-1], "Failed")

    async def test_retry_waits_for_delay_and_is_stoppable(self):
        self.database.execute("UPDATE campaigns SET status='waiting_retry', max_retries=1, failed_at=?", (time.time(),))
        await self.send_device_messages([{"type": "heartbeat"}])
        self.assertEqual(self.status(), "waiting_retry")
        result = await self.namespace["stop_campaign"]("campaign-1")
        self.assertEqual((result["status"], self.status()), ("stopped", "stopped"))

    async def test_invalid_retry_count_rejected(self):
        with self.assertRaises(HTTPException) as raised:
            await self.namespace["campaign"](
                campaign_id="bad", vin="VIN1", campaign_name="x", sgw_target_version="2.0",
                sgw_firmware="sgw.bin", execute=False, max_retries=11
            )
        self.assertEqual(raised.exception.status_code, 422)

    async def create_mode_campaign(self, initiation, scheduled_at=None):
        self.database.execute("DELETE FROM campaigns")
        result = await self.namespace["campaign"](
            campaign_id="mode-1", vin="VIN1", campaign_name="Mode", sgw_target_version="2.0",
            sgw_firmware="sgw.bin", execute=False, initiation=initiation, scheduled_at=scheduled_at,
            description="Gateway fix"
        )
        return result

    async def test_scheduled_campaign_executes_automatically_when_online(self):
        result = await self.create_mode_campaign("scheduled", time.time() + 3600)
        self.assertEqual((result["status"], self.status()), ("scheduled", "scheduled"))
        self.assertEqual(self.database.execute("SELECT description FROM campaigns").fetchone()[0], "Gateway fix")
        with self.assertRaises(HTTPException):
            await self.namespace["execute_campaign"]("mode-1")
        socket = TestSocket()
        self.namespace["connected_tbms"]["VIN1"] = socket
        self.assertEqual(await self.namespace["start_scheduled_campaigns"](), [])
        self.assertEqual(socket.sent, [])
        self.database.execute("UPDATE campaigns SET scheduled_at=?", (time.time() - 1,))
        started = await self.namespace["start_scheduled_campaigns"]()
        self.assertEqual(started, [{"campaign_id": "mode-1", "status": "sent"}])
        self.assertEqual((self.status(), socket.sent[0]["type"]), ("sent", "campaign"))

    async def test_scheduled_campaign_waits_for_offline_vehicle_ping(self):
        await self.create_mode_campaign("scheduled", time.time() + 3600)
        self.database.execute("UPDATE campaigns SET scheduled_at=?", (time.time() - 1,))
        await self.namespace["start_scheduled_campaigns"]()
        self.assertEqual(self.status(), "waiting_vehicle")
        await self.send_device_messages([{"type": "heartbeat"}])
        self.assertEqual(self.status(), "sent")

    async def test_scheduled_campaign_can_be_cancelled(self):
        await self.create_mode_campaign("scheduled", time.time() + 3600)
        self.assertEqual((await self.namespace["stop_campaign"]("mode-1"))["status"], "stopped")
        self.database.execute("UPDATE campaigns SET scheduled_at=?", (time.time() - 1,))
        self.assertEqual(await self.namespace["start_scheduled_campaigns"](), [])
        self.assertEqual(self.status(), "stopped")

    async def test_invalid_schedule_and_initiation_rejected(self):
        for initiation, scheduled_at in (("scheduled", None), ("scheduled", time.time() - 10), ("urgent", None), ("none", None)):
            with self.subTest(initiation=initiation, scheduled_at=scheduled_at):
                with self.assertRaises(HTTPException) as raised:
                    await self.namespace["campaign"](
                        campaign_id="bad", vin="VIN1", campaign_name="x", sgw_target_version="2.0",
                        sgw_firmware="sgw.bin", execute=False, initiation=initiation, scheduled_at=scheduled_at
                    )
                self.assertEqual(raised.exception.status_code, 422)

    def set_campaign_timers(self, status, heartbeat_at, progress_at):
        self.database.execute(
            "UPDATE campaigns SET status=?, execution_started_at=1, last_heartbeat_at=?, last_progress_at=?",
            (status, heartbeat_at, progress_at)
        )

    async def test_watchdog_fails_and_stops_missing_heartbeat(self):
        self.set_campaign_timers("running", 400, 999)
        socket = TestSocket()
        self.namespace["connected_tbms"]["VIN1"] = socket
        failures = await self.namespace["expire_stalled_campaigns"](1000)
        self.assertEqual(self.status(), "failed")
        self.assertEqual(failures[0]["error_code"], "HEARTBEAT_TIMEOUT")
        self.assertTrue(failures[0]["device_notified"])
        self.assertEqual(socket.sent[0]["type"], "stop_campaign")
        self.assertEqual(socket.sent[0]["error_code"], "HEARTBEAT_TIMEOUT")
        stored = (await self.namespace["get_campaigns"]())[0]
        self.assertEqual(stored["error_code"], "HEARTBEAT_TIMEOUT")
        self.assertEqual(stored["failed_at"], 1000)
        self.assertTrue(stored["error_message"])
        self.assertEqual(await self.namespace["expire_stalled_campaigns"](1100), [])
        self.assertEqual(len(socket.sent), 1)

    async def test_watchdog_detects_ack_and_running_stalls(self):
        for status, code in (("sent", "ACK_TIMEOUT"), ("acknowledged", "ACK_TIMEOUT"),
                             ("running", "RUNNING_TIMEOUT"), ("Downloading 35%", "RUNNING_TIMEOUT")):
            with self.subTest(status=status):
                self.set_campaign_timers(status, 999, 400)
                failures = await self.namespace["expire_stalled_campaigns"](1000)
                self.assertEqual(self.status(), "failed")
                self.assertEqual(failures[0]["error_code"], code)

    async def test_watchdog_persists_failure_when_device_is_offline(self):
        self.set_campaign_timers("running", 400, 999)
        failures = await self.namespace["expire_stalled_campaigns"](1000)
        self.assertEqual(self.status(), "failed")
        self.assertFalse(failures[0]["device_notified"])

    async def test_watchdog_does_not_fail_active_or_pending_campaigns(self):
        for status in ("pending", "created", "completed", "stopped", "failed", "running"):
            self.set_campaign_timers(status, 999, 999)
            self.assertEqual(await self.namespace["expire_stalled_campaigns"](1000), [])
            self.assertEqual(self.status(), status)

    async def test_watchdog_initializes_legacy_timers_only_once(self):
        self.assertEqual(await self.namespace["expire_stalled_campaigns"](1000), [])
        self.assertEqual(await self.namespace["expire_stalled_campaigns"](1599), [])
        failures = await self.namespace["expire_stalled_campaigns"](1600)
        self.assertEqual(failures[0]["error_code"], "HEARTBEAT_TIMEOUT")

    async def test_execution_starts_persistent_timeout_timers(self):
        await self.create_draft()
        self.assertIsNone(self.database.execute("SELECT execution_started_at FROM campaigns WHERE campaign_id='draft-1'").fetchone()[0])
        self.namespace["connected_tbms"]["VIN1"] = TestSocket()
        await self.namespace["execute_campaign"]("draft-1")
        started, heartbeat_at, progress_at = self.database.execute(
            "SELECT execution_started_at, last_heartbeat_at, last_progress_at FROM campaigns WHERE campaign_id='draft-1'"
        ).fetchone()
        self.assertIsNotNone(started)
        self.assertEqual((heartbeat_at, progress_at), (started, started))

    async def test_heartbeat_refreshes_only_the_heartbeat_timer(self):
        self.set_campaign_timers("running", 400, 999)
        with patch.object(time, "time", return_value=900):
            await self.send_device_messages([{"type": "heartbeat"}])
        timers = self.database.execute("SELECT last_heartbeat_at, last_progress_at FROM campaigns").fetchone()
        self.assertEqual(timers, (900, 999))
        self.assertEqual(await self.namespace["expire_stalled_campaigns"](1000), [])

    async def test_repeated_ack_does_not_reset_stall_deadline(self):
        self.set_campaign_timers("acknowledged", 999, 400)
        with patch.object(time, "time", return_value=1000):
            await self.send_device_messages([{"type": "campaign_ack", "campaign_id": "campaign-1"}])
        self.assertEqual(self.database.execute("SELECT last_progress_at FROM campaigns").fetchone()[0], 400)
        failures = await self.namespace["expire_stalled_campaigns"](1000)
        self.assertEqual(failures[0]["error_code"], "ACK_TIMEOUT")

    async def test_first_ack_starts_new_stage_deadline(self):
        self.set_campaign_timers("sent", 999, 400)
        with patch.object(time, "time", return_value=900):
            await self.send_device_messages([{"type": "campaign_ack", "campaign_id": "campaign-1"}])
        self.assertEqual(self.status(), "acknowledged")
        self.assertEqual(self.database.execute("SELECT last_progress_at FROM campaigns").fetchone()[0], 900)
        self.assertEqual(await self.namespace["expire_stalled_campaigns"](1000), [])

    async def test_only_advancing_progress_resets_stall_deadline(self):
        self.set_campaign_timers("Downloading 20%", 999, 400)
        with patch.object(time, "time", return_value=900):
            await self.send_device_messages([{"type": "progress", "campaign_id": "campaign-1", "progress": 40}])
        with patch.object(time, "time", return_value=1000):
            await self.send_device_messages([
                {"type": "progress", "campaign_id": "campaign-1", "progress": 40},
                {"type": "progress", "campaign_id": "campaign-1", "progress": 10}
            ])
        self.assertEqual(self.status(), "Downloading 40%")
        self.assertEqual(self.database.execute("SELECT last_progress_at FROM campaigns").fetchone()[0], 900)
        self.assertEqual(await self.namespace["expire_stalled_campaigns"](1499), [])
        failures = await self.namespace["expire_stalled_campaigns"](1500)
        self.assertEqual(failures[0]["error_code"], "RUNNING_TIMEOUT")

    async def test_repeated_running_message_does_not_reset_deadline(self):
        self.set_campaign_timers("running", 999, 400)
        with patch.object(time, "time", return_value=1000):
            await self.send_device_messages([{"type": "running", "campaign_id": "campaign-1"}])
        self.assertEqual(self.database.execute("SELECT last_progress_at FROM campaigns").fetchone()[0], 400)

    async def test_timeout_failure_cannot_be_overwritten_by_late_device_messages(self):
        self.set_campaign_timers("running", 400, 999)
        await self.namespace["expire_stalled_campaigns"](1000)
        messages = [{"type": kind, "campaign_id": "campaign-1", "progress": 100,
                     "target_ecu": "BOTH", "sgw_version": "9.0", "spaak_version": "9.0"}
                    for kind in ("campaign_ack", "running", "progress", "completed", "checksum_failed", "campaign_failed", "transfer_failed", "heartbeat")]
        await self.send_device_messages(messages)
        self.assertEqual(self.status(), "failed")
        self.assertEqual(self.database.execute("SELECT error_code FROM campaigns").fetchone()[0], "HEARTBEAT_TIMEOUT")
        self.assertEqual(self.database.execute("SELECT sgw_version, spaak_version FROM registered_tbms").fetchone(), ("1.0", "1.0"))

    async def test_stale_watchdog_snapshot_does_not_fail_refreshed_campaign(self):
        self.set_campaign_timers("running", 400, 999)
        original_fetchall = self.namespace["cursor"].fetchall
        def refreshed_snapshot():
            rows = original_fetchall()
            self.database.execute("UPDATE campaigns SET last_heartbeat_at=1000")
            return rows
        with patch.object(self.namespace["cursor"], "fetchall", side_effect=refreshed_snapshot):
            self.assertEqual(await self.namespace["expire_stalled_campaigns"](1000), [])
        self.assertEqual(self.status(), "running")

    async def test_failed_notification_does_not_undo_timeout_failure(self):
        self.set_campaign_timers("running", 400, 999)
        self.namespace["connected_tbms"]["VIN1"] = TestSocket(fail_send=True)
        failures = await self.namespace["expire_stalled_campaigns"](1000)
        self.assertEqual(self.status(), "failed")
        self.assertFalse(failures[0]["device_notified"])

    async def test_unauthenticated_heartbeat_cannot_refresh_timers(self):
        self.set_campaign_timers("running", 400, 999)
        await self.namespace["websocket_endpoint"](TestSocket([{"type": "heartbeat", "vin": "VIN1"}]))
        self.assertEqual(self.database.execute("SELECT last_heartbeat_at FROM campaigns").fetchone()[0], 400)

    async def test_watchdog_lifespan_starts_and_cancels_worker(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()
        async def worker():
            started.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        self.namespace["campaign_watchdog"] = worker
        async with self.namespace["campaign_lifespan"](None):
            await asyncio.wait_for(started.wait(), timeout=1)
        self.assertTrue(cancelled.is_set())

    async def test_device_messages_cannot_execute_pending_campaign(self):
        self.database.execute("UPDATE campaigns SET status='pending'")
        messages = [{"type": kind, "campaign_id": "campaign-1", "progress": 80,
                     "target_ecu": "SGW", "sgw_version": "2.0"}
                    for kind in ("campaign_ack", "progress", "completed", "checksum_failed", "campaign_failed", "transfer_failed")]
        await self.send_device_messages(messages)
        self.assertEqual(self.status(), "pending")
        self.assertEqual(self.database.execute("SELECT sgw_version FROM registered_tbms").fetchone()[0], "1.0")

    async def test_normal_completion_still_updates_versions(self):
        await self.send_device_messages([{"type": "completed", "campaign_id": "campaign-1",
                                         "target_ecu": "SGW", "sgw_version": "2.0"}])
        self.assertEqual(self.status(), "completed")
        self.assertEqual(self.database.execute("SELECT sgw_version FROM registered_tbms").fetchone()[0], "2.0")

    async def test_close_only_old_failed_and_stopped_campaigns_by_creation_date(self):
        now = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
        cutoff = now - timedelta(days=30)
        records = [
            ("old-failed", "failed", (cutoff - timedelta(seconds=1)).isoformat(sep=" ")),
            ("old-send-failed", "send_failed", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-checksum-failed", "checksum_failed", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-stopped", "stopped", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-cancelled", "cancelled", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("exactly-thirty-days", "failed", cutoff.isoformat(sep=" ")),
            ("recent-failed", "failed", (cutoff + timedelta(seconds=1)).isoformat(sep=" ")),
            ("old-completed", "completed", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-executed", "executed", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-retry", "waiting_retry", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-running", "running", (cutoff - timedelta(days=1)).isoformat(sep=" ")),
            ("old-pending", "pending", (cutoff - timedelta(days=1)).isoformat(sep=" "))
        ]
        self.database.executemany(
            """INSERT INTO campaigns
            (campaign_id, vin, status, created_at)
            VALUES (?, 'VIN1', ?, ?)""",
            records
        )

        closed = self.namespace["close_expired_campaigns"](now=now)
        expected_closed = {record[0] for record in records[:5]}
        self.assertEqual(set(closed), expected_closed)
        statuses = dict(self.database.execute("SELECT campaign_id, status FROM campaigns").fetchall())
        self.assertEqual(statuses["exactly-thirty-days"], "failed")
        self.assertEqual(statuses["recent-failed"], "failed")
        self.assertEqual(statuses["old-completed"], "completed")
        self.assertEqual(statuses["old-executed"], "executed")
        self.assertEqual(statuses["old-retry"], "waiting_retry")
        self.assertEqual(statuses["old-running"], "running")
        self.assertEqual(statuses["old-pending"], "pending")
        self.assertEqual(self.database.execute(
            "SELECT COUNT(*) FROM campaign_events WHERE status='CLOSED'"
        ).fetchone()[0], len(expected_closed))

        created_at = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat(sep=" ")
        self.database.execute(
            "INSERT INTO campaigns (campaign_id, vin, status, created_at) VALUES (?, 'VIN1', 'stopped', ?)",
            ("closed-on-read", created_at)
        )
        await self.namespace["get_campaigns"]()
        self.assertEqual(self.database.execute(
            "SELECT status FROM campaigns WHERE campaign_id='closed-on-read'"
        ).fetchone()[0], "closed")


class ModularApplicationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import server
        from fota_server import application, campaigns, config, database, state, vehicles, websocket

        self.entrypoint = server
        self.application = application
        self.campaigns = campaigns
        self.settings = config
        self.database_module = database
        self.state = state
        self.vehicles = vehicles
        self.websocket_module = websocket
        self.database = sqlite3.connect(":memory:")
        self.addCleanup(self.database.close)
        self.database.executescript("""
            CREATE TABLE campaigns (
                campaign_id TEXT PRIMARY KEY, vin TEXT, status TEXT, target_ecu TEXT,
                campaign_name TEXT, sgw_target_version TEXT, spaak_target_version TEXT,
                sgw_firmware TEXT, spaak_firmware TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                execution_started_at REAL, last_heartbeat_at REAL, last_progress_at REAL,
                failed_at REAL, error_code TEXT, error_message TEXT,
                max_retries INTEGER DEFAULT 0, retry_count INTEGER DEFAULT 0, completed_at REAL, stopped_at REAL,
                initiation TEXT DEFAULT 'immediate', scheduled_at REAL, description TEXT
            );
            CREATE TABLE campaign_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id TEXT, vin TEXT, event_type TEXT,
                status TEXT, detail TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE registered_tbms (vin TEXT PRIMARY KEY, sgw_version TEXT, spaak_version TEXT, added_on TEXT);
            INSERT INTO registered_tbms VALUES ('VIN1', '1.0', '1.0', '2026-11-06T09:00:00Z');
        """)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        (Path(directory.name) / "sgw.bin").write_bytes(b"firmware")
        patches = [
            patch.object(database, "conn", self.database),
            patch.object(database, "cursor", DatabaseCursor(self.database)),
            patch.object(database, "ensure_connection", lambda: None),
            patch.object(config, "UPLOAD_FOLDER", directory.name),
            patch.object(config, "CAPTURE_FOLDER", str(Path(directory.name) / "captures")),
            patch.object(state, "pending_auth", {}),
            patch.object(state, "pending_tbms", {}),
            patch.object(state, "connected_tbms", {}),
            patch.object(state, "logs", []),
            patch.object(state, "add_log", lambda message: state.logs.append(message)),
        ]
        for replacement in patches:
            replacement.start()
            self.addCleanup(replacement.stop)
        self.app = application.create_app()

    async def request_raw(self, method, path, parameters=None, body=b"", headers=()):
        messages = []
        response_complete = asyncio.Event()
        request_delivered = False
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1", "method": method, "scheme": "http", "path": path,
            "raw_path": path.encode(), "query_string": urlencode(parameters or {}).encode(),
            "root_path": "", "headers": [(b"host", b"testserver"), *headers],
            "client": ("127.0.0.1", 1234), "server": ("testserver", 80)
        }

        async def receive():
            nonlocal request_delivered
            if not request_delivered:
                request_delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            await response_complete.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            messages.append(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                response_complete.set()

        await self.app(scope, receive, send)
        status = next(message["status"] for message in messages if message["type"] == "http.response.start")
        response_headers = next(message["headers"] for message in messages if message["type"] == "http.response.start")
        body = b"".join(message.get("body", b"") for message in messages)
        return status, dict(response_headers), body

    async def request(self, method, path, parameters=None):
        status, _, body = await self.request_raw(method, path, parameters)
        return status, json.loads(body)

    async def test_all_http_endpoints_remain_registered(self):
        expected = {
            ("get", "/"), ("get", "/health/ready"), ("get", "/pending"), ("get", "/tbms"),
            ("get", "/logs"), ("get", "/download_logs"), ("get", "/campaigns"),
            ("put", "/upload_capture"),
            ("post", "/campaign"), ("delete", "/campaign/{campaign_id}"), ("get", "/campaign/{campaign_id}"),
            ("post", "/campaign/{campaign_id}/execute"), ("post", "/campaign/{campaign_id}/stop"),
            ("get", "/registered_tbms"), ("get", "/registered_tbm/{vin}"),
            ("delete", "/registered_tbm/{vin}"), ("post", "/register_tbm"),
            ("post", "/approve/{vin}"), ("post", "/reject/{vin}"),
            ("post", "/upload"), ("get", "/files/{filename}"),
            ("get", "/github_test"), ("post", "/upload_firmware"),
            ("get", "/latest_firmware"), ("get", "/firmware_history"),
            ("get", "/download_firmware")
        }
        actual = {(method, path) for path, methods in self.app.openapi()["paths"].items() for method in methods}
        self.assertEqual(actual, expected)
        self.assertEqual(await self.request("GET", "/"), (200, {"status": "running"}))

    async def test_download_logs_zip_contains_text_log_and_uploaded_pcapng(self):
        capture = b"\x0a\x0d\x0d\x0a" + b"pcapng test data"
        self.state.logs[:] = ["server log line"]
        status, headers, body = await self.request_raw("PUT", "/upload_capture", body=capture)
        self.assertEqual((status, json.loads(body)), (200, {"status": "uploaded", "size": len(capture)}))

        status, headers, body = await self.request_raw("GET", "/download_logs")
        self.assertEqual(status, 200)
        self.assertEqual(headers[b"content-type"], b"application/zip")
        self.assertRegex(headers[b"content-disposition"].decode(), r'FOTA_Logs_\d{8}_\d{6}\.zip')
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            self.assertEqual(set(archive.namelist()), {"FOTA_Server_Log.txt", "FOTA_Capture.pcapng"})
            self.assertIn(b"server log line", archive.read("FOTA_Server_Log.txt"))
            self.assertEqual(archive.read("FOTA_Capture.pcapng"), capture)

    async def test_download_logs_zip_includes_only_available_files(self):
        self.state.logs[:] = ["server log only"]
        status, _, body = await self.request_raw("GET", "/download_logs")
        self.assertEqual(status, 200)
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            self.assertEqual(archive.namelist(), ["FOTA_Server_Log.txt"])

        capture = b"\x0a\x0d\x0d\x0a" + b"capture only"
        await self.request_raw("PUT", "/upload_capture", body=capture)
        self.state.logs.clear()
        status, _, body = await self.request_raw("GET", "/download_logs")
        self.assertEqual(status, 200)
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            self.assertEqual(archive.namelist(), ["FOTA_Capture.pcapng"])
            self.assertEqual(archive.read("FOTA_Capture.pcapng"), capture)

        os.remove(Path(self.settings.CAPTURE_FOLDER) / "FOTA_Capture.pcapng")
        status, _, body = await self.request_raw("GET", "/download_logs")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["detail"]["code"], "LOGS_NOT_FOUND")

    async def test_readiness_reports_database_health_without_leaking_errors(self):
        status, result = await self.request("GET", "/health/ready")
        self.assertEqual((status, result), (200, {"status": "ready", "checks": {"database": "ready"}}))
        with patch.object(self.database_module, "ensure_connection", side_effect=RuntimeError("private DB detail")):
            status, result = await self.request("GET", "/health/ready")
        self.assertEqual(status, 503)
        self.assertEqual(result, {"status": "not_ready", "checks": {"database": "unavailable"}})

    async def test_real_http_campaign_lifecycle(self):
        status, result = await self.request("POST", "/campaign", {
            "campaign_id": "http-campaign", "vin": "VIN1", "campaign_name": "HTTP release",
            "sgw_target_version": "2.0", "sgw_firmware": "sgw.bin", "execute": "false"
        })
        self.assertEqual((status, result["status"]), (200, "created"))
        status, result = await self.request("POST", "/campaign/http-campaign/stop")
        self.assertEqual(status, 409)
        self.assertEqual(result["detail"]["code"], "CAMPAIGN_NOT_EXECUTED")
        socket = TestSocket()
        self.state.connected_tbms["VIN1"] = socket
        status, result = await self.request("POST", "/campaign/http-campaign/execute")
        self.assertEqual((status, result["status"]), (200, "sent"))
        self.assertEqual(socket.sent[0]["campaign_id"], "http-campaign")
        status, result = await self.request("POST", "/campaign/http-campaign/stop")
        self.assertEqual((status, result["status"]), (200, "stopped"))
        self.assertEqual(socket.sent[-1]["type"], "stop_campaign")
        status, result = await self.request("GET", "/campaigns")
        self.assertEqual((status, result[0]["status"]), (200, "stopped"))
        status, result = await self.request("GET", "/campaign/http-campaign")
        self.assertEqual((status, result["status"]), (200, "stopped"))
        self.assertEqual([event["event_type"] for event in result["history"]],
                         ["Stopped", "Campaign Sent", "Campaign Created"])
        status, result = await self.request("GET", "/campaign/missing")
        self.assertEqual(status, 404)
        status, result = await self.request("GET", "/logs")
        self.assertEqual(status, 200)
        self.assertTrue(any("Campaign stopped" in message for message in result))

    async def test_actual_router_validates_required_parameters(self):
        status, result = await self.request("POST", "/register_tbm", {"vin": "VIN1"})
        self.assertEqual(status, 422)
        self.assertTrue(result["detail"])

    async def test_real_websocket_authentication_and_heartbeat(self):
        self.database.execute(
            "INSERT INTO campaigns (campaign_id, vin, status, last_heartbeat_at) VALUES ('ws-campaign', 'VIN1', 'sent', 1)"
        )
        authentication = hashlib.sha256(("challenge" + self.settings.AUTH_SECRET).encode()).hexdigest()
        events = iter([
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": json.dumps({"type": "register_request", "vin": "VIN1"})},
            {"type": "websocket.receive", "text": json.dumps({"type": "auth_response", "vin": "VIN1", "response": authentication})},
            {"type": "websocket.receive", "text": json.dumps({"type": "heartbeat"})},
            {"type": "websocket.disconnect", "code": 1000}
        ])
        sent = []

        async def receive():
            return next(events)

        async def send(message):
            sent.append(message)

        scope = {
            "type": "websocket", "asgi": {"version": "3.0", "spec_version": "2.3"},
            "scheme": "ws", "path": "/ws", "raw_path": b"/ws", "query_string": b"",
            "root_path": "", "headers": [(b"host", b"testserver")], "subprotocols": [],
            "client": ("127.0.0.1", 1234), "server": ("testserver", 80)
        }
        with patch.object(self.websocket_module.secrets, "token_hex", return_value="challenge"):
            await self.app(scope, receive, send)
        messages = [json.loads(message["text"]) for message in sent if message["type"] == "websocket.send"]
        self.assertEqual([message["type"] for message in messages], ["challenge", "auth_passed", "approved"])
        self.assertGreater(self.database.execute("SELECT last_heartbeat_at FROM campaigns").fetchone()[0], 1)
        self.assertNotIn("VIN1", self.state.connected_tbms)

    async def test_actual_module_watchdog_uses_shared_connections(self):
        self.database.execute(
            """INSERT INTO campaigns
            (campaign_id, vin, status, execution_started_at, last_heartbeat_at, last_progress_at)
            VALUES ('timed-out', 'VIN1', 'running', 1, 400, 999)"""
        )
        socket = TestSocket()
        self.state.connected_tbms["VIN1"] = socket
        await self.campaigns.expire_stalled_campaigns(1000)
        status, result = await self.request("GET", "/campaigns")
        self.assertEqual(status, 200)
        self.assertEqual((result[0]["status"], result[0]["error_code"]), ("failed", "HEARTBEAT_TIMEOUT"))
        self.assertEqual(socket.sent[0]["type"], "stop_campaign")

    async def test_importing_entrypoint_does_not_connect_to_postgres(self):
        with patch.object(self.database_module, "connect_db", side_effect=AssertionError("Unexpected database connection")):
            imported = importlib.reload(self.entrypoint)
            self.assertTrue(imported.app.openapi()["paths"])

    async def test_reconnect_is_visible_in_every_module(self):
        from fota_server import database

        replacement_cursor = SimpleNamespace(closed=False)
        replacement_connection = SimpleNamespace(closed=False, cursor=lambda: replacement_cursor)
        with patch.object(database, "conn", None), patch.object(database, "cursor", None), \
             patch.object(database.psycopg2, "connect", return_value=replacement_connection):
            database.connect_db()
            self.assertIs(self.campaigns.database.cursor, replacement_cursor)
            self.assertIs(self.vehicles.database.conn, replacement_connection)
            self.assertIs(self.websocket_module.database.cursor, replacement_cursor)

    async def test_real_lifespan_initializes_and_closes_database(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def worker():
            started.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        with patch.object(self.database_module, "initialize_database") as initialize, \
             patch.object(self.database_module, "close_database") as close, \
             patch.object(self.application, "campaign_watchdog", worker):
            async with self.application.campaign_lifespan(self.app):
                await asyncio.wait_for(started.wait(), timeout=1)
                initialize.assert_called_once()
            close.assert_called_once()
        self.assertTrue(cancelled.is_set())


class CampaignDeviceTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("campaign_control", ROOT / "campaign_control.py")
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict("sys.modules", {"requests": SimpleNamespace()}):
            spec.loader.exec_module(self.module)
        self.control = self.module.CampaignControl()

    def test_worker_stops_without_blocking_receiver(self):
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        messages = []
        socket = SimpleNamespace(send=lambda message: messages.append(json.loads(message)))

        def handler(worker_socket):
            entered.set()
            release.wait(2)
            try:
                worker_socket.send(json.dumps({"type": "completed"}))
            except self.module.CampaignStopped:
                finished.set()

        self.control.start(socket, {"campaign_id": "first"}, handler)
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2))
        self.control.start(socket, {"campaign_id": "second"}, handler)
        self.assertEqual(messages[-1]["type"], "campaign_failed")
        self.control.stop("unrelated")
        self.assertFalse(self.control.active[1].is_set())
        self.control.stop("first")
        release.set()
        self.assertTrue(finished.wait(2))
        self.assertFalse(any(message["type"] == "completed" for message in messages))

    def test_cancelled_download_cleans_partial_file(self):
        stop_event = threading.Event()
        socket = self.module.CampaignSocket(SimpleNamespace(), stop_event)

        def chunks(chunk_size):
            yield b"first"
            stop_event.set()
            yield b"second"

        response = SimpleNamespace(raise_for_status=lambda: None, iter_content=chunks)
        context = unittest.mock.MagicMock()
        context.__enter__.return_value = response
        self.module.requests.get = lambda *args, **kwargs: context
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "firmware.bin"
            destination.write_bytes(b"original")
            with self.assertRaises(self.module.CampaignStopped):
                self.control.download(socket, "mock://firmware", str(destination))
            self.assertEqual(destination.read_bytes(), b"original")
            self.assertFalse(Path(str(destination) + ".part").exists())


class SgwDecoderTests(unittest.TestCase):
    SGW = bytes.fromhex("ecfa0300cb11")
    TESTER = bytes.fromhex("020000000001")

    def setUp(self):
        import sgw_decoder
        self.decoder = sgw_decoder

    def ipv4_frame(self, source_mac, destination_mac, protocol, transport):
        ip = bytearray(20)
        ip[0], ip[9] = 0x45, protocol
        ip[2:4] = (20 + len(transport)).to_bytes(2, "big")
        ip[12:16], ip[16:20] = bytes([192, 168, 1, 10]), bytes([192, 168, 1, 20])
        return destination_mac + source_mac + b"\x08\x00" + bytes(ip) + transport

    def tcp(self, source_port, destination_port, payload):
        header = source_port.to_bytes(2, "big") + destination_port.to_bytes(2, "big") + bytes(8)
        return header + bytes([0x50, 0x18]) + bytes(6) + payload

    def doip(self, payload_type, body):
        return bytes([0x02, 0xFD]) + payload_type.to_bytes(2, "big") + len(body).to_bytes(4, "big") + body

    def test_doip_uds_positive_and_negative_responses(self):
        positive = self.doip(0x8001, bytes.fromhex("1010 0e00 5002".replace(" ", "")))
        frame = self.ipv4_frame(self.SGW, self.TESTER, 6, self.tcp(13400, 51000, positive))
        text = self.decoder.decode_frame(frame, "ec:fa:03:00:cb:11")
        self.assertIn("SGW -> 02:00:00:00:00:01", text)
        self.assertIn("192.168.1.10:13400 -> 192.168.1.20:51000", text)
        self.assertIn("TCP [ACK,PSH]", text)
        self.assertIn("DoIP Diagnostic message 0x1010 -> 0x0E00", text)
        self.assertIn("UDS DiagnosticSessionControl +ve response (programmingSession)", text)
        negative = self.doip(0x8001, bytes.fromhex("10100e007f3472"))
        frame = self.ipv4_frame(self.SGW, self.TESTER, 6, self.tcp(13400, 51000, negative))
        self.assertIn("UDS NEGATIVE RESPONSE RequestDownload NRC 0x72 generalProgrammingFailure",
                      self.decoder.decode_frame(frame, "ec:fa:03:00:cb:11"))

    def test_routing_activation_and_transfer_data_request(self):
        payload = self.doip(0x0006, bytes.fromhex("0e00101010")) + \
            self.doip(0x8001, bytes.fromhex("0e00101036" + "05" + "aabbcc"))
        frame = self.ipv4_frame(self.TESTER, self.SGW, 6, self.tcp(51000, 13400, payload))
        text = self.decoder.decode_frame(frame, "ec:fa:03:00:cb:11")
        self.assertIn("02:00:00:00:00:01 -> SGW", text)
        self.assertIn("Routing activation response tester 0x0E00 entity 0x1010 (successfully activated)", text)
        self.assertIn("UDS TransferData request block 5 (3 data bytes)", text)

    def test_someip_sd_offer_and_arp(self):
        entry = bytes([0x01, 0, 0, 0]) + bytes.fromhex("1234") + bytes.fromhex("0001") + bytes([1, 0, 0, 3]) + bytes(4)
        sd = bytes(4) + len(entry).to_bytes(4, "big") + entry + bytes(4)
        someip = bytes.fromhex("ffff8100") + (8 + len(sd)).to_bytes(4, "big") + bytes.fromhex("00000001010102 00".replace(" ", "")) + sd
        udp = (30490).to_bytes(2, "big") * 2 + (8 + len(someip)).to_bytes(2, "big") + bytes(2) + someip
        text = self.decoder.decode_frame(self.ipv4_frame(self.SGW, b"\xff" * 6, 17, udp), "ec:fa:03:00:cb:11")
        self.assertIn("SOME/IP-SD OfferService service 0x1234 instance 0x0001", text)
        arp = b"\xff" * 6 + self.SGW + b"\x08\x06" + bytes.fromhex("0001080006040001") + self.SGW + \
            bytes([192, 168, 1, 10]) + bytes(6) + bytes([192, 168, 1, 20])
        self.assertIn("ARP who-has 192.168.1.20 tell 192.168.1.10", self.decoder.decode_frame(arp, "EC:FA:03:00:CB:11"))

    def test_tcpdump_hex_lines_and_truncated_frames(self):
        lines = ["\t0x0000:  ecfa 0300 cb11 0200 0000 0001 0806", "\t0x0010:  0001"]
        self.assertEqual(self.decoder.parse_hex_lines(lines), bytes.fromhex("ecfa0300cb110200000000010806" + "0001"))
        self.assertEqual(self.decoder.decode_frame(b"\x00" * 5, "ec:fa:03:00:cb:11"), "Truncated frame (5 bytes)")

    def test_tshark_hex_lines_ignore_ascii_column(self):
        lines = ["0000  ec fa 03 00 cb 11 02 00 00 00 00 01 08 06 00 01   ..............ab",
                 "0010  08 00                                             .."]
        self.assertEqual(self.decoder.parse_tshark_hex_lines(lines),
                         bytes.fromhex("ecfa0300cb110200000000010806000108 00".replace(" ", "")))


if __name__ == "__main__":
    unittest.main()