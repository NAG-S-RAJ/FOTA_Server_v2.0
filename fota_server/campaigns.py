import asyncio
import json
import os
import time

from fastapi import APIRouter, HTTPException

from . import config as settings, database, state, storage
from .campaign_status import campaign_timeout_error
from .events import record_campaign_event

router = APIRouter(tags=["Campaigns"])


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
                raise FileNotFoundError(f"{ecu.upper()} firmware not found")
            download_url = f"{settings.PUBLIC_URL}/files/{firmware}"
            checksum = storage.get_sha256(path)
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
        return None
    new_status, retry_count, max_retries = row
    if new_status == "waiting_retry":
        record_campaign_event(campaign_id, vin, "Failed - Waiting for Retry", "RUNNING",
                              f"{message} ({code}). Retry {retry_count + 1} of {max_retries} "
                              "starts when the vehicle is connected and pings.")
    else:
        record_campaign_event(campaign_id, vin, "Failed", "FAILED", f"{message} ({code})")
    return new_status


async def dispatch_waiting_campaigns(vin, websocket, now=None):
    """Sends executed campaigns queued for an offline vehicle, or due retries, once the vehicle pings."""
    now = time.time() if now is None else now
    database.ensure_connection()
    database.cursor.execute(
        """
        SELECT 1 FROM campaigns
        WHERE vin=%s AND (LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress')
                          OR LOWER(status) LIKE %s)
        """,
        (vin, "downloading%")
    )
    if database.cursor.fetchone() is not None:
        return None
    database.cursor.execute(
        """
        SELECT campaign_id, target_ecu, campaign_name, sgw_target_version,
               spaak_target_version, sgw_firmware, spaak_firmware, status
        FROM campaigns
        WHERE vin=%s AND (status='waiting_vehicle'
                          OR (status='waiting_retry' AND COALESCE(failed_at, 0) <= %s))
        ORDER BY created_at, campaign_id
        LIMIT 1
        """,
        (vin, now - settings.CAMPAIGN_RETRY_DELAY_SECONDS)
    )
    row = database.cursor.fetchone()
    if row is None:
        return None
    campaign_id, waiting_status = row[0], row[7]
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
            return None
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
            return None
        record_campaign_event(campaign_id, vin, f"Retry {claimed[0]} Triggered", "RUNNING",
                              f"Vehicle connected and pinged. Retry {claimed[0]} of {claimed[1]} sent.")
        attempt_label = f"Campaign retry {claimed[0]}"
    try:
        await websocket.send_text(json.dumps(payload))
    except Exception:
        fail_campaign(campaign_id, vin, "sent", "SEND_FAILED", "Unable to send campaign to the vehicle.",
                      time.time(), terminal_status="send_failed")
        return None
    state.add_log(f"{attempt_label} sent -> {vin}: {campaign_id}")
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
        new_status = fail_campaign(
            campaign_id, vin, status, code, message, now,
            extra_where=" AND last_heartbeat_at=%s AND last_progress_at=%s",
            extra_parameters=(heartbeat_at, progress_at)
        )
        if new_status is None:
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
            except Exception as error:
                state.add_log(f"Campaign {campaign_id} stop notification failed: {error}")
        state.add_log(f"Campaign {campaign_id} {new_status} [{code}]: {message}")
        failures.append(failure)
    return failures


async def campaign_watchdog():
    while True:
        try:
            await expire_stalled_campaigns()
        except Exception as error:
            state.add_log(f"Campaign watchdog error: {error}")
        await asyncio.sleep(settings.CAMPAIGN_WATCHDOG_INTERVAL_SECONDS)


@router.delete("/campaign/{campaign_id}")
async def delete_campaign(campaign_id: str):

    database.ensure_connection()

    database.cursor.execute(
        """
        DELETE FROM campaigns
        WHERE campaign_id = %s
        """,
        (campaign_id,)
    )
    database.cursor.execute("DELETE FROM campaign_events WHERE campaign_id=%s", (campaign_id,))

    database.conn.commit()

    state.add_log(
        f"Campaign Deleted: {campaign_id}"
    )

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
            retry_count
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
            "retry_count":row[14] or 0
        }       
        for row in rows
    ]


CAMPAIGN_DETAIL_FIELDS = (
    "campaign_id", "vin", "target_ecu", "campaign_name", "sgw_target_version",
    "spaak_target_version", "sgw_firmware", "spaak_firmware", "status", "created_at",
    "error_code", "error_message", "failed_at", "execution_started_at",
    "last_progress_at", "completed_at", "max_retries", "retry_count"
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
        raise HTTPException(status_code=404, detail="Campaign not found")
    result = dict(zip(CAMPAIGN_DETAIL_FIELDS, row))
    result["max_retries"] = result["max_retries"] or 0
    result["retry_count"] = result["retry_count"] or 0
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
                                   'waiting_retry', 'waiting_vehicle')
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
            raise HTTPException(status_code=404, detail="Campaign not found")
        if existing[0].lower() in ("pending", "created"):
            raise HTTPException(status_code=409, detail={
                "code": "CAMPAIGN_NOT_EXECUTED",
                "message": "Campaign has not been executed. Execute it before stopping."
            })
        if existing[0] == "stopped":
            return {"status": "stopped", "device_notified": False,
                    "message": "Campaign is already stopped.", "already_stopped": True}
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
        except Exception as error:
            state.add_log(f"Stop notification failed for {campaign_id}: {error}")
    record_campaign_event(campaign_id, row[0], "Stopped", "STOPPED", "Stopped by operator.")
    state.add_log(f"Campaign stopped: {campaign_id}")
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

    max_retries:int=0

):
    campaign_id = campaign_id.strip()

    if not campaign_id:
        return {
            "status": "error",
            "message": "Campaign ID is required"
        }

    if not 0 <= max_retries <= settings.CAMPAIGN_MAX_RETRIES:
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_RETRY_COUNT",
            "message": f"Retry count must be between 0 and {settings.CAMPAIGN_MAX_RETRIES}."
        })
    
    database.ensure_connection()

    database.cursor.execute(
        """
         SELECT vin, campaign_name, sgw_target_version, spaak_target_version,
             sgw_firmware, spaak_firmware, status
        FROM campaigns
        WHERE campaign_id=%s
        """,
        (campaign_id,)
    )

    existing = database.cursor.fetchone()
    if existing is not None:
        if not execute or existing[6].lower() not in ("pending", "created"):
            raise HTTPException(status_code=409, detail={
                "code": "CAMPAIGN_NOT_EXECUTABLE",
                "message": "Campaign already exists or is no longer awaiting execution."
            })
        vin, campaign_name, sgw_target_version, spaak_target_version, sgw_firmware, spaak_firmware = existing[:6]

    database.cursor.execute("SELECT 1 FROM registered_tbms WHERE vin=%s", (vin,))
    if database.cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="VIN not registered")
    vehicle_online = vin in state.connected_tbms

    if sgw_firmware and spaak_firmware:
    
        target_ecu = "BOTH"
    
    elif sgw_firmware:
    
        target_ecu = "SGW"
    
    elif spaak_firmware:
    
        target_ecu = "SPAAK"
    
    else:
    
        return {
        
            "status":"error",
    
            "message":"Select at least one ECU"
    
        }

    if target_ecu == "SGW":

        if not sgw_target_version:
            return {
                "status":"error",
                "message":"SGW target version is required"
            }

        if not sgw_firmware:
            return {
                "status":"error",
                "message":"SGW firmware is required"
            }

    elif target_ecu == "SPAAK":

        if not spaak_target_version:
            return {
                "status":"error",
                "message":"SPAAK target version is required"
            }

        if not spaak_firmware:
            return {
                "status":"error",
                "message":"SPAAK firmware is required"
            }

    elif target_ecu == "BOTH":

        if (
            not sgw_target_version or
            not spaak_target_version or
            not sgw_firmware or
            not spaak_firmware
        ):
            return {
                "status":"error",
                "message":"Both firmware files and versions are required"
            }

    try:
        payload = campaign_payload(
            campaign_id, target_ecu, campaign_name, sgw_target_version,
            spaak_target_version, sgw_firmware, spaak_firmware
        )
    except FileNotFoundError as error:
        return {"status": "error", "message": str(error)}

    database.ensure_connection()
    send_now = execute and vehicle_online
    new_status = ("sent" if send_now else "waiting_vehicle") if execute else "pending"
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
                raise HTTPException(status_code=409, detail="Campaign is no longer awaiting execution")
        else:
            database.cursor.execute(
                """
                INSERT INTO campaigns (
                    campaign_id, vin, target_ecu, campaign_name,
                    sgw_target_version, spaak_target_version,
                    sgw_firmware, spaak_firmware, status,
                    execution_started_at, last_heartbeat_at, last_progress_at,
                    max_retries, retry_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
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
                    max_retries
                )
            )

        database.conn.commit()

    except HTTPException:
        database.conn.rollback()
        raise

    except Exception as e:

        database.conn.rollback()

        return {
            "status":"error",
            "message":str(e)
        }

    if existing is None:
        record_campaign_event(campaign_id, vin, "Campaign Created", "PENDING",
                              f"Target ECU: {target_ecu}. Retries allowed: {max_retries}.")

    if not execute:
        state.add_log(f"Campaign created: {campaign_id}")
        return {"status": "created", "campaign_id": campaign_id,
                "message": "Campaign created and awaiting execution."}

    if not send_now:
        record_campaign_event(campaign_id, vin, "Waiting for Vehicle", "PENDING",
                              "Execution requested. Vehicle is offline; campaign starts when it connects and pings.")
        state.add_log(f"Campaign queued until {vin} connects: {campaign_id}")
        return {"status": "waiting_vehicle", "campaign_id": campaign_id,
                "message": "Vehicle is offline. Campaign will start when the vehicle connects and pings."}

    record_campaign_event(campaign_id, vin, "Campaign Sent", "RUNNING",
                          "Deployment package sent to the vehicle.")
    ws = state.connected_tbms[vin]

    try:
        await ws.send_text(json.dumps(payload))
    except Exception:
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
        f"Campaign Sent -> {vin}"
    )

    return {
        "status": "sent"
    }


@router.post("/campaign/{campaign_id}/execute")
async def execute_campaign(campaign_id: str):
    database.ensure_connection()
    database.cursor.execute("SELECT status FROM campaigns WHERE campaign_id=%s", (campaign_id,))
    if database.cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return await campaign(campaign_id=campaign_id, vin="", campaign_name="")
