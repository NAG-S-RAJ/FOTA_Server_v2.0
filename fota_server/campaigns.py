import asyncio
import json
import os
import time

from fastapi import APIRouter, HTTPException

from . import config as settings, database, state, storage
from .campaign_status import campaign_timeout_error
from .events import record_campaign_event

router = APIRouter(tags=["Campaigns"])


def format_epoch(value):
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(value))


def campaign_payload(campaign_id, target_ecu, campaign_name, sgw_target_version,
                     spaak_target_version, sgw_firmware, spaak_firmware):
    message = {
        "type": "campaign",
        "campaign_id": campaign_id,
        "target_ecu": target_ecu,
        "campaign_name": campaign_name,
        "sgw_target_version": sgw_target_version,
        "spaak_target_version": spaak_target_version,
        "sgw_firmware": sgw_firmware,
        "spaak_firmware": spaak_firmware
    }
    for ecu, firmware in (("sgw", sgw_firmware), ("spaak", spaak_firmware)):
        download_url = checksum = ""
        if firmware:
            path = os.path.join(settings.UPLOAD_FOLDER, firmware)
            if os.path.basename(firmware) != firmware or not os.path.exists(path):
                state.add_log(f"[PAYLOAD][FIRMWARE_MISSING] campaign={campaign_id} ecu={ecu.upper()} file={firmware} path={path}")
                raise FileNotFoundError(f"{ecu.upper()} firmware not found")
            download_url = f"{settings.PUBLIC_URL}/files/{firmware}"
            checksum = storage.get_sha256(path)
            state.add_log(f"[PAYLOAD][FIRMWARE_READY] campaign={campaign_id} ecu={ecu.upper()} file={firmware} "
                          f"size={os.path.getsize(path)}B sha256={checksum} url={download_url}")
        message[f"{ecu}_download_url"] = download_url
        message[f"{ecu}_checksum"] = checksum
    return message


def fail_campaign(campaign_id, vin, expected_status, code, message, now,
                  terminal_status="failed", extra_where="", extra_parameters=()):
    """Fails the campaign, or parks it in waiting_retry while retries remain. Returns the new status."""
    database.cursor.execute(
        f"""
        UPDATE campaigns
        SET status=CASE WHEN COALESCE(retry_count, 0) < COALESCE(max_retries, 0)
                        THEN 'waiting_retry' ELSE %s END,
            error_code=%s, error_message=%s, failed_at=%s
        WHERE campaign_id=%s AND vin=%s AND status=%s{extra_where}
        RETURNING status, COALESCE(retry_count, 0), COALESCE(max_retries, 0)
        """,
        (terminal_status, code, message, now, campaign_id, vin, expected_status, *extra_parameters)
    )
    row = database.cursor.fetchone()
    database.conn.commit()
    if row is None:
        state.add_log(f"[FAIL][STATE_CHANGED] campaign={campaign_id} vin={vin} code={code} ignored: "
                      f"status is no longer '{expected_status}'")
        return None
    new_status, retry_count, max_retries = row
    state.add_log(f"[FAIL][{code}] campaign={campaign_id} vin={vin} {expected_status} -> {new_status} "
                  f"retries_used={retry_count}/{max_retries} reason={message}")
    if new_status == "waiting_retry":
        state.add_log(f"[RETRY][RETRY_SCHEDULED] campaign={campaign_id} vin={vin} retry={retry_count + 1}/{max_retries} "
                      f"earliest_in={settings.CAMPAIGN_RETRY_DELAY_SECONDS}s, requires vehicle online + ping")
        record_campaign_event(campaign_id, vin, "Failed - Waiting for Retry", "RUNNING",
                              f"{message} ({code}). Retry {retry_count + 1} of {max_retries} "
                              "starts when the vehicle is connected and pings.")
    else:
        if max_retries:
            state.add_log(f"[RETRY][RETRIES_EXHAUSTED] campaign={campaign_id} vin={vin} "
                          f"all {max_retries} retries used; final status={new_status}")
        record_campaign_event(campaign_id, vin, "Failed", "FAILED", f"{message} ({code})")
    return new_status


async def dispatch_waiting_campaigns(vin, websocket, now=None):
    """Sends executed campaigns queued for an offline vehicle, or due retries, once the vehicle pings."""
    now = time.time() if now is None else now
    database.ensure_connection()
    database.cursor.execute(
        """
        SELECT campaign_id, target_ecu, campaign_name, sgw_target_version,
               spaak_target_version, sgw_firmware, spaak_firmware, status, failed_at
        FROM campaigns
        WHERE vin=%s AND status IN ('waiting_vehicle', 'waiting_retry')
        ORDER BY created_at, campaign_id
        """,
        (vin,)
    )
    waiting = database.cursor.fetchall()
    if not waiting:
        return None
    database.cursor.execute(
        """
        SELECT campaign_id, status FROM campaigns
        WHERE vin=%s AND (LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress')
                          OR LOWER(status) LIKE %s)
        """,
        (vin, "downloading%")
    )
    active = database.cursor.fetchone()
    if active is not None:
        state.add_log(f"[DISPATCH][VEHICLE_BUSY] vin={vin} active campaign={active[0]} status={active[1]}; "
                      f"{len(waiting)} waiting campaign(s) deferred")
        return None
    retry_threshold = now - settings.CAMPAIGN_RETRY_DELAY_SECONDS
    row = next((item for item in waiting
                if item[7] == "waiting_vehicle" or (item[8] or 0) <= retry_threshold), None)
    if row is None:
        remaining = min((item[8] or 0) - retry_threshold for item in waiting)
        state.add_log(f"[RETRY][RETRY_DELAY] vin={vin} campaign={waiting[0][0]} next retry allowed in {remaining:.0f}s")
        return None
    campaign_id, waiting_status = row[0], row[7]
    state.add_log(f"[DISPATCH][PING_RECEIVED] vin={vin} campaign={campaign_id} status={waiting_status}; preparing dispatch")
    try:
        payload = campaign_payload(*row[:7])
    except FileNotFoundError as error:
        database.cursor.execute(
            """
            UPDATE campaigns SET status='failed', error_code='FIRMWARE_MISSING', error_message=%s, failed_at=%s
            WHERE campaign_id=%s AND status=%s
            """,
            (str(error), now, campaign_id, waiting_status)
        )
        database.conn.commit()
        state.add_log(f"[DISPATCH][FIRMWARE_MISSING] campaign={campaign_id} vin={vin} {waiting_status} -> failed: {error}")
        record_campaign_event(campaign_id, vin, "Failed", "FAILED", f"{error} (FIRMWARE_MISSING)")
        return None
    if waiting_status == "waiting_vehicle":
        database.cursor.execute(
            """
            UPDATE campaigns
            SET status='sent', execution_started_at=%s, last_heartbeat_at=%s, last_progress_at=%s
            WHERE campaign_id=%s AND status='waiting_vehicle'
            RETURNING campaign_id
            """,
            (now, now, now, campaign_id)
        )
        claimed = database.cursor.fetchone()
        database.conn.commit()
        if claimed is None:
            state.add_log(f"[DISPATCH][CLAIM_LOST] campaign={campaign_id} vin={vin} no longer waiting_vehicle")
            return None
        state.add_log(f"[DISPATCH][VEHICLE_ONLINE] campaign={campaign_id} vin={vin} waiting_vehicle -> sent")
        record_campaign_event(campaign_id, vin, "Campaign Sent", "RUNNING",
                              "Vehicle connected and pinged. Deployment package sent to the vehicle.")
        attempt_label = "Campaign"
    else:
        database.cursor.execute(
            """
            UPDATE campaigns
            SET status='sent', retry_count=COALESCE(retry_count, 0) + 1,
                last_heartbeat_at=%s, last_progress_at=%s,
                error_code=NULL, error_message=NULL, failed_at=NULL
            WHERE campaign_id=%s AND status='waiting_retry'
            RETURNING retry_count, max_retries
            """,
            (now, now, campaign_id)
        )
        claimed = database.cursor.fetchone()
        database.conn.commit()
        if claimed is None:
            state.add_log(f"[RETRY][CLAIM_LOST] campaign={campaign_id} vin={vin} no longer waiting_retry")
            return None
        state.add_log(f"[RETRY][RETRY_TRIGGERED] campaign={campaign_id} vin={vin} retry={claimed[0]}/{claimed[1]} "
                      "waiting_retry -> sent")
        record_campaign_event(campaign_id, vin, f"Retry {claimed[0]} Triggered", "RUNNING",
                              f"Vehicle connected and pinged. Retry {claimed[0]} of {claimed[1]} sent.")
        attempt_label = f"Campaign retry {claimed[0]}"
    try:
        await websocket.send_text(json.dumps(payload))
    except Exception as error:
        state.add_log(f"[DISPATCH][SEND_FAILED] campaign={campaign_id} vin={vin} {type(error).__name__}: {error}")
        fail_campaign(campaign_id, vin, "sent", "SEND_FAILED", "Unable to send campaign to the vehicle.",
                      time.time(), terminal_status="send_failed")
        return None
    state.add_log(f"[DISPATCH][SENT] {attempt_label} sent -> vin={vin} campaign={campaign_id} "
                  f"target_ecu={payload['target_ecu']}")
    return campaign_id


def initialize_campaign_timers(now):
    database.cursor.execute(
        """
        UPDATE campaigns
        SET execution_started_at=COALESCE(execution_started_at, %s),
            last_heartbeat_at=COALESCE(last_heartbeat_at, execution_started_at, %s),
            last_progress_at=COALESCE(last_progress_at, execution_started_at, %s)
        WHERE LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress')
           OR LOWER(status) LIKE %s
        """,
        (now, now, now, "downloading%")
    )
    database.conn.commit()


async def expire_stalled_campaigns(now=None):
    now = time.time() if now is None else now
    database.ensure_connection()
    initialize_campaign_timers(now)
    database.cursor.execute(
        """
        SELECT campaign_id, vin, status, last_heartbeat_at, last_progress_at
        FROM campaigns
        WHERE LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress')
           OR LOWER(status) LIKE %s
        """,
        ("downloading%",)
    )
    campaigns = database.cursor.fetchall()
    failures = []
    for campaign_id, vin, status, heartbeat_at, progress_at in campaigns:
        error = campaign_timeout_error(
            status, heartbeat_at, progress_at, now,
            ack_timeout=settings.CAMPAIGN_ACK_TIMEOUT_SECONDS,
            running_timeout=settings.CAMPAIGN_RUNNING_TIMEOUT_SECONDS
        )
        if error is None:
            continue
        code, message = error
        state.add_log(f"[WATCHDOG][{code}] campaign={campaign_id} vin={vin} status={status} "
                      f"heartbeat_age={now - heartbeat_at:.0f}s progress_age={now - progress_at:.0f}s")
        new_status = fail_campaign(
            campaign_id, vin, status, code, message, now,
            extra_where=" AND last_heartbeat_at=%s AND last_progress_at=%s",
            extra_parameters=(heartbeat_at, progress_at)
        )
        if new_status is None:
            state.add_log(f"[WATCHDOG][STATE_CHANGED] campaign={campaign_id} refreshed before timeout was applied")
            continue
        failure = {"campaign_id": campaign_id, "error_code": code, "status": new_status,
                   "error_message": message, "device_notified": False}
        websocket = state.connected_tbms.get(vin)
        if websocket is not None:
            try:
                await asyncio.wait_for(websocket.send_text(json.dumps({
                    "type": "stop_campaign", "campaign_id": campaign_id,
                    "error_code": code, "message": message
                })), timeout=5)
                failure["device_notified"] = True
                state.add_log(f"[WATCHDOG][STOP_NOTIFIED] campaign={campaign_id} vin={vin} stop_campaign sent")
            except Exception as error:
                state.add_log(f"[WATCHDOG][STOP_NOTIFY_FAILED] campaign={campaign_id} vin={vin} "
                              f"{type(error).__name__}: {error}")
        else:
            state.add_log(f"[WATCHDOG][DEVICE_OFFLINE] campaign={campaign_id} vin={vin} not connected; stop not delivered")
        state.add_log(f"[WATCHDOG][{code}] campaign={campaign_id} -> {new_status}: {message}")
        failures.append(failure)
    return failures


async def start_scheduled_campaigns(now=None):
    now = time.time() if now is None else now
    database.ensure_connection()
    database.cursor.execute(
        """
        SELECT campaign_id, vin, scheduled_at FROM campaigns
        WHERE status='scheduled' AND scheduled_at <= %s
        ORDER BY scheduled_at, campaign_id
        """,
        (now,)
    )
    started = []
    for campaign_id, vin, scheduled_at in database.cursor.fetchall():
        # Hand the campaign to the normal Execute path, which only accepts pending campaigns.
        database.cursor.execute(
            "UPDATE campaigns SET status='pending' WHERE campaign_id=%s AND status='scheduled' RETURNING campaign_id",
            (campaign_id,)
        )
        claimed = database.cursor.fetchone()
        database.conn.commit()
        if claimed is None:
            state.add_log(f"[SCHEDULE][CLAIM_LOST] campaign={campaign_id} no longer scheduled")
            continue
        state.add_log(f"[SCHEDULE][SCHEDULE_REACHED] campaign={campaign_id} vin={vin} "
                      f"scheduled_at={format_epoch(scheduled_at)}; executing automatically")
        record_campaign_event(campaign_id, vin, "Schedule Reached", "RUNNING",
                              f"Scheduled time {format_epoch(scheduled_at)} reached. Executing automatically.")
        try:
            result = await campaign(campaign_id=campaign_id, vin="", campaign_name="")
        except HTTPException as error:
            result = {"status": "error", "code": "AUTO_EXECUTE_FAILED", "message": str(error.detail)}
        if result.get("status") == "error":
            code = result.get("code") or "AUTO_EXECUTE_FAILED"
            message = result.get("message") or "Scheduled execution failed."
            database.cursor.execute(
                """
                UPDATE campaigns SET status='failed', error_code=%s, error_message=%s, failed_at=%s
                WHERE campaign_id=%s AND status='pending'
                """,
                (code, message, time.time(), campaign_id)
            )
            database.conn.commit()
            state.add_log(f"[SCHEDULE][{code}] campaign={campaign_id} vin={vin} auto-execution failed: {message}")
            record_campaign_event(campaign_id, vin, "Failed", "FAILED", f"{message} ({code})")
        started.append({"campaign_id": campaign_id, "status": result.get("status")})
    return started


async def campaign_watchdog():
    state.add_log(f"[WATCHDOG][STARTED] interval={settings.CAMPAIGN_WATCHDOG_INTERVAL_SECONDS}s "
                  f"ack_timeout={settings.CAMPAIGN_ACK_TIMEOUT_SECONDS}s "
                  f"running_timeout={settings.CAMPAIGN_RUNNING_TIMEOUT_SECONDS}s "
                  f"retry_delay={settings.CAMPAIGN_RETRY_DELAY_SECONDS}s")
    while True:
        try:
            await start_scheduled_campaigns()
        except Exception as error:
            state.add_log(f"[SCHEDULE][SCHEDULER_ERROR] {type(error).__name__}: {error}")
        try:
            await expire_stalled_campaigns()
        except Exception as error:
            state.add_log(f"[WATCHDOG][WATCHDOG_ERROR] {type(error).__name__}: {error}")
        await asyncio.sleep(settings.CAMPAIGN_WATCHDOG_INTERVAL_SECONDS)


@router.delete("/campaign/{campaign_id}")
async def delete_campaign(campaign_id: str):

    database.ensure_connection()

    database.cursor.execute(
        """
        DELETE FROM campaigns
        WHERE campaign_id = %s
        RETURNING vin, status
        """,
        (campaign_id,)
    )
    deleted = database.cursor.fetchone()
    database.cursor.execute("DELETE FROM campaign_events WHERE campaign_id=%s", (campaign_id,))

    database.conn.commit()

    if deleted is None:
        state.add_log(f"[DELETE][CAMPAIGN_NOT_FOUND] campaign={campaign_id} nothing to delete")
    else:
        state.add_log(f"[DELETE][DELETED] campaign={campaign_id} vin={deleted[0]} last_status={deleted[1]}")

    return {"status": "deleted"}


@router.get("/campaigns")
async def get_campaigns():

    database.ensure_connection()

    database.cursor.execute("""
        SELECT
            campaign_id,
            vin,
            target_ecu,
            campaign_name,
            sgw_target_version,
            spaak_target_version,
            sgw_firmware,
            spaak_firmware,
            status,
            created_at,
            error_code,
            error_message,
            failed_at,
            max_retries,
            retry_count,
            initiation,
            scheduled_at
        FROM campaigns
        ORDER BY created_at DESC NULLS LAST, campaign_id DESC
    """)

    rows = database.cursor.fetchall()

    return [
        {
            "campaign_id":row[0],
            "vin":row[1],
            "target_ecu":row[2],
            "campaign_name":row[3],
            "sgw_target_version":row[4],
            "spaak_target_version":row[5],
            "sgw_firmware":row[6],
            "spaak_firmware":row[7],
            "status":row[8],
            "created_at":row[9],
            "error_code":row[10],
            "error_message":row[11],
            "failed_at":row[12],
            "max_retries":row[13] or 0,
            "retry_count":row[14] or 0,
            "initiation":row[15] or "immediate",
            "scheduled_at":row[16]
        }       
        for row in rows
    ]


CAMPAIGN_DETAIL_FIELDS = (
    "campaign_id", "vin", "target_ecu", "campaign_name", "sgw_target_version",
    "spaak_target_version", "sgw_firmware", "spaak_firmware", "status", "created_at",
    "error_code", "error_message", "failed_at", "execution_started_at",
    "last_progress_at", "completed_at", "max_retries", "retry_count",
    "initiation", "scheduled_at", "description"
)


@router.get("/campaign/{campaign_id}")
async def get_campaign(campaign_id: str):
    database.ensure_connection()
    database.cursor.execute(
        f"SELECT {', '.join(CAMPAIGN_DETAIL_FIELDS)} FROM campaigns WHERE campaign_id=%s",
        (campaign_id,)
    )
    row = database.cursor.fetchone()
    if row is None:
        state.add_log(f"[API][CAMPAIGN_NOT_FOUND] details requested for unknown campaign={campaign_id}")
        raise HTTPException(status_code=404, detail="Campaign not found")
    result = dict(zip(CAMPAIGN_DETAIL_FIELDS, row))
    result["max_retries"] = result["max_retries"] or 0
    result["retry_count"] = result["retry_count"] or 0
    result["initiation"] = result["initiation"] or "immediate"
    result["vehicle_connected"] = result["vin"] in state.connected_tbms
    database.cursor.execute(
        """
        SELECT event_type, status, detail, created_at FROM campaign_events
        WHERE campaign_id=%s ORDER BY id DESC
        """,
        (campaign_id,)
    )
    result["history"] = [
        {"event_type": event[0], "status": event[1], "detail": event[2], "created_at": event[3]}
        for event in database.cursor.fetchall()
    ]
    return result


@router.post("/campaign/{campaign_id}/stop")
async def stop_campaign(campaign_id: str):
    database.ensure_connection()
    database.cursor.execute(
        """
        UPDATE campaigns
        SET status='stopped'
        WHERE campaign_id=%s
            AND (LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress',
                                   'waiting_retry', 'waiting_vehicle', 'scheduled')
                 OR LOWER(status) LIKE %s)
        RETURNING vin
        """,
             (campaign_id, "downloading%")
    )
    row = database.cursor.fetchone()
    database.conn.commit()
    if row is None:
        database.cursor.execute(
            "SELECT status FROM campaigns WHERE campaign_id=%s",
            (campaign_id,)
        )
        existing = database.cursor.fetchone()
        if existing is None:
            state.add_log(f"[STOP][CAMPAIGN_NOT_FOUND] campaign={campaign_id}")
            raise HTTPException(status_code=404, detail="Campaign not found")
        if existing[0].lower() in ("pending", "created"):
            state.add_log(f"[STOP][CAMPAIGN_NOT_EXECUTED] campaign={campaign_id} status={existing[0]}")
            raise HTTPException(status_code=409, detail={
                "code": "CAMPAIGN_NOT_EXECUTED",
                "message": "Campaign has not been executed. Execute it before stopping."
            })
        if existing[0] == "stopped":
            state.add_log(f"[STOP][ALREADY_STOPPED] campaign={campaign_id}")
            return {"status": "stopped", "device_notified": False,
                    "message": "Campaign is already stopped.", "already_stopped": True}
        state.add_log(f"[STOP][CAMPAIGN_FINISHED] campaign={campaign_id} status={existing[0]} cannot be stopped")
        raise HTTPException(status_code=409, detail="Campaign is already finished")

    device_notified = False
    websocket = state.connected_tbms.get(row[0])
    if websocket is not None:
        try:
            await websocket.send_text(json.dumps({
                "type": "stop_campaign",
                "campaign_id": campaign_id
            }))
            device_notified = True
            state.add_log(f"[STOP][STOP_NOTIFIED] campaign={campaign_id} vin={row[0]} stop_campaign sent")
        except Exception as error:
            state.add_log(f"[STOP][STOP_NOTIFY_FAILED] campaign={campaign_id} vin={row[0]} "
                          f"{type(error).__name__}: {error}")
    else:
        state.add_log(f"[STOP][DEVICE_OFFLINE] campaign={campaign_id} vin={row[0]} not connected; stop not delivered")
    record_campaign_event(campaign_id, row[0], "Stopped", "STOPPED", "Stopped by operator.")
    state.add_log(f"[STOP][STOPPED] Campaign stopped: {campaign_id} vin={row[0]} device_notified={device_notified}")
    return {"status": "stopped", "device_notified": device_notified,
            "message": "Campaign stopped."}


@router.post("/campaign")
async def campaign(

    campaign_id:str,

    vin:str,

    campaign_name:str,

    sgw_target_version:str="",

    spaak_target_version:str="",

    sgw_firmware:str="",

    spaak_firmware:str="",

    execute:bool=True,

    max_retries:int=0,

    initiation:str="immediate",

    scheduled_at:float=None,

    description:str=""

):
    campaign_id = campaign_id.strip()
    initiation = (initiation or "immediate").strip().lower()
    description = (description or "").strip()
    state.add_log(f"[CAMPAIGN][REQUEST] campaign={campaign_id or '<empty>'} vin={vin or '<stored>'} execute={execute} "
                  f"max_retries={max_retries} initiation={initiation} scheduled_at={scheduled_at} "
                  f"sgw={sgw_target_version or '-'}:{sgw_firmware or '-'} "
                  f"spaak={spaak_target_version or '-'}:{spaak_firmware or '-'}")

    if not campaign_id:
        state.add_log("[CAMPAIGN][CAMPAIGN_ID_REQUIRED] request rejected")
        return {
            "status": "error",
            "message": "Campaign ID is required"
        }

    if not 0 <= max_retries <= settings.CAMPAIGN_MAX_RETRIES:
        state.add_log(f"[CAMPAIGN][INVALID_RETRY_COUNT] campaign={campaign_id} max_retries={max_retries} "
                      f"allowed=0..{settings.CAMPAIGN_MAX_RETRIES}")
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_RETRY_COUNT",
            "message": f"Retry count must be between 0 and {settings.CAMPAIGN_MAX_RETRIES}."
        })

    if initiation not in ("immediate", "scheduled"):
        state.add_log(f"[CAMPAIGN][INVALID_INITIATION] campaign={campaign_id} initiation={initiation!r}")
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_INITIATION",
            "message": "Server initiated must be one of: immediate, scheduled."
        })
    if initiation == "scheduled":
        if scheduled_at is None or scheduled_at <= time.time():
            state.add_log(f"[CAMPAIGN][INVALID_SCHEDULE] campaign={campaign_id} scheduled_at={scheduled_at}")
            raise HTTPException(status_code=422, detail={
                "code": "INVALID_SCHEDULE",
                "message": "Scheduled campaigns need a start time in the future."
            })
    else:
        scheduled_at = None
    if len(description) > 1000:
        state.add_log(f"[CAMPAIGN][DESCRIPTION_TOO_LONG] campaign={campaign_id} length={len(description)}")
        raise HTTPException(status_code=422, detail={
            "code": "DESCRIPTION_TOO_LONG",
            "message": "Description must be at most 1000 characters."
        })
    
    database.ensure_connection()

    database.cursor.execute(
        """
         SELECT vin, campaign_name, sgw_target_version, spaak_target_version,
             sgw_firmware, spaak_firmware, status, initiation, scheduled_at
        FROM campaigns
        WHERE campaign_id=%s
        """,
        (campaign_id,)
    )

    existing = database.cursor.fetchone()
    if existing is not None:
        if not execute or existing[6].lower() not in ("pending", "created"):
            state.add_log(f"[CAMPAIGN][CAMPAIGN_NOT_EXECUTABLE] campaign={campaign_id} status={existing[6]} "
                          f"execute={execute}: already exists or no longer awaiting execution")
            raise HTTPException(status_code=409, detail={
                "code": "CAMPAIGN_NOT_EXECUTABLE",
                "message": "Campaign already exists or is no longer awaiting execution."
            })
        vin, campaign_name, sgw_target_version, spaak_target_version, sgw_firmware, spaak_firmware = existing[:6]
        initiation, scheduled_at = existing[7] or "immediate", existing[8]
    elif initiation == "scheduled":
        execute = False

    database.cursor.execute("SELECT 1 FROM registered_tbms WHERE vin=%s", (vin,))
    if database.cursor.fetchone() is None:
        state.add_log(f"[CAMPAIGN][VIN_NOT_REGISTERED] campaign={campaign_id} vin={vin}")
        raise HTTPException(status_code=404, detail="VIN not registered")
    vehicle_online = vin in state.connected_tbms
    state.add_log(f"[CAMPAIGN][VEHICLE_{'ONLINE' if vehicle_online else 'OFFLINE'}] campaign={campaign_id} vin={vin}")

    if sgw_firmware and spaak_firmware:
        target_ecu = "BOTH"
    elif sgw_firmware:
        target_ecu = "SGW"
    elif spaak_firmware:
        target_ecu = "SPAAK"
    else:
        target_ecu = ""

    if not target_ecu:
        validation_error = ("NO_ECU_SELECTED", "Select at least one ECU")
    elif target_ecu == "SGW" and not sgw_target_version:
        validation_error = ("SGW_VERSION_REQUIRED", "SGW target version is required")
    elif target_ecu == "SPAAK" and not spaak_target_version:
        validation_error = ("SPAAK_VERSION_REQUIRED", "SPAAK target version is required")
    elif target_ecu == "BOTH" and not (sgw_target_version and spaak_target_version):
        validation_error = ("VERSIONS_REQUIRED", "Both firmware files and versions are required")
    else:
        validation_error = None
    if validation_error:
        state.add_log(f"[CAMPAIGN][{validation_error[0]}] campaign={campaign_id} vin={vin}: {validation_error[1]}")
        return {"status": "error", "code": validation_error[0], "message": validation_error[1]}

    try:
        payload = campaign_payload(
            campaign_id, target_ecu, campaign_name, sgw_target_version,
            spaak_target_version, sgw_firmware, spaak_firmware
        )
    except FileNotFoundError as error:
        state.add_log(f"[CAMPAIGN][FIRMWARE_MISSING] campaign={campaign_id} vin={vin}: {error}")
        return {"status": "error", "code": "FIRMWARE_MISSING", "message": str(error)}

    database.ensure_connection()
    send_now = execute and vehicle_online
    if execute:
        new_status = "sent" if send_now else "waiting_vehicle"
    else:
        new_status = "scheduled" if initiation == "scheduled" else "pending"
    execution_started_at = time.time() if send_now else None
    try:
        if existing is not None:
            database.cursor.execute(
                """
                UPDATE campaigns SET status=%s, execution_started_at=%s,
                    last_heartbeat_at=%s, last_progress_at=%s, retry_count=0,
                    error_code=NULL, error_message=NULL, failed_at=NULL
                WHERE campaign_id=%s AND status IN ('pending', 'created')
                RETURNING campaign_id
                """,
                (new_status, execution_started_at, execution_started_at, execution_started_at, campaign_id)
            )
            if database.cursor.fetchone() is None:
                state.add_log(f"[CAMPAIGN][CAMPAIGN_NOT_EXECUTABLE] campaign={campaign_id} changed state during execution")
                raise HTTPException(status_code=409, detail="Campaign is no longer awaiting execution")
        else:
            database.cursor.execute(
                """
                INSERT INTO campaigns (
                    campaign_id, vin, target_ecu, campaign_name,
                    sgw_target_version, spaak_target_version,
                    sgw_firmware, spaak_firmware, status,
                    execution_started_at, last_heartbeat_at, last_progress_at,
                    max_retries, retry_count, initiation, scheduled_at, description
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s)
                """,
                (
                    campaign_id,
                    vin,
                    target_ecu,
                    campaign_name,
                    sgw_target_version,
                    spaak_target_version,
                    sgw_firmware,
                    spaak_firmware,
                    new_status,
                    execution_started_at,
                    execution_started_at,
                    execution_started_at,
                    max_retries,
                    initiation,
                    scheduled_at,
                    description
                )
            )

        database.conn.commit()

    except HTTPException:
        database.conn.rollback()
        raise

    except Exception as e:

        database.conn.rollback()
        state.add_log(f"[CAMPAIGN][DB_ERROR] campaign={campaign_id} vin={vin} {type(e).__name__}: {e}")

        return {
            "status":"error",
            "code":"DB_ERROR",
            "message":str(e)
        }

    if existing is None:
        schedule_detail = (f" Scheduled start: {format_epoch(scheduled_at)}." if scheduled_at else "")
        record_campaign_event(campaign_id, vin, "Campaign Created", "PENDING",
                              f"Target ECU: {target_ecu}. Server initiated: {initiation}.{schedule_detail} "
                              f"Retries allowed: {max_retries}.")

    if not execute:
        if new_status == "scheduled":
            state.add_log(f"[CAMPAIGN][SCHEDULED] Campaign created: {campaign_id} vin={vin} target_ecu={target_ecu} "
                          f"max_retries={max_retries}; executes automatically at {format_epoch(scheduled_at)}")
            return {"status": "scheduled", "campaign_id": campaign_id, "scheduled_at": scheduled_at,
                    "message": "Campaign created and will execute automatically at the scheduled time."}
        state.add_log(f"[CAMPAIGN][CREATED] Campaign created: {campaign_id} vin={vin} target_ecu={target_ecu} "
                      f"max_retries={max_retries}; waiting for Execute")
        return {"status": "created", "campaign_id": campaign_id,
                "message": "Campaign created and awaiting execution."}

    if not send_now:
        message = "Vehicle is offline. Campaign will start when the vehicle connects and pings."
        record_campaign_event(campaign_id, vin, "Waiting for Vehicle", "PENDING", message)
        state.add_log(f"[EXECUTE][VEHICLE_OFFLINE] campaign={campaign_id} vin={vin} initiation={initiation} "
                      "-> waiting_vehicle; will dispatch on next ping")
        return {"status": "waiting_vehicle", "campaign_id": campaign_id, "message": message}

    record_campaign_event(campaign_id, vin, "Campaign Sent", "RUNNING",
                          "Deployment package sent to the vehicle.")
    ws = state.connected_tbms[vin]

    try:
        await ws.send_text(json.dumps(payload))
    except Exception as error:
        state.add_log(f"[EXECUTE][SEND_FAILED] campaign={campaign_id} vin={vin} {type(error).__name__}: {error}")
        new_status = fail_campaign(
            campaign_id, vin, "sent", "SEND_FAILED", "Unable to send campaign to the vehicle.",
            time.time(), terminal_status="send_failed"
        )
        if new_status == "waiting_retry":
            return {"status": "waiting_retry",
                    "message": "Unable to send campaign. It will be retried when the vehicle connects and pings."}
        return {
            "status":"send_failed"
        }

    state.add_log(
        f"[EXECUTE][SENT] Campaign Sent -> vin={vin} campaign={campaign_id} target_ecu={target_ecu}"
    )

    return {
        "status": "sent"
    }


@router.post("/campaign/{campaign_id}/execute")
async def execute_campaign(campaign_id: str):
    state.add_log(f"[EXECUTE][REQUEST] campaign={campaign_id} execute clicked")
    database.ensure_connection()
    database.cursor.execute("SELECT status FROM campaigns WHERE campaign_id=%s", (campaign_id,))
    if database.cursor.fetchone() is None:
        state.add_log(f"[EXECUTE][CAMPAIGN_NOT_FOUND] campaign={campaign_id}")
        raise HTTPException(status_code=404, detail="Campaign not found")
    return await campaign(campaign_id=campaign_id, vin="", campaign_name="")
