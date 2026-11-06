import asyncio
import json
import os
import time

from fastapi import APIRouter, HTTPException

from . import config as settings, database, state, storage
from .campaign_status import campaign_timeout_error

router = APIRouter(tags=["Campaigns"])


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
        database.cursor.execute(
            """
            UPDATE campaigns SET status='failed', error_code=%s, error_message=%s, failed_at=%s
            WHERE campaign_id=%s AND status=%s
              AND last_heartbeat_at=%s AND last_progress_at=%s
            RETURNING vin
            """,
            (code, message, now, campaign_id, status, heartbeat_at, progress_at)
        )
        changed = database.cursor.fetchone()
        database.conn.commit()
        if changed is None:
            continue
        failure = {"campaign_id": campaign_id, "error_code": code,
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
        state.add_log(f"Campaign {campaign_id} failed [{code}]: {message}")
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
            failed_at
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
            "failed_at":row[12]
        }       
        for row in rows
    ]


@router.post("/campaign/{campaign_id}/stop")
async def stop_campaign(campaign_id: str):
    database.ensure_connection()
    database.cursor.execute(
        """
        UPDATE campaigns
        SET status='stopped'
        WHERE campaign_id=%s
            AND (LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress')
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

    execute:bool=True

):
    campaign_id = campaign_id.strip()

    if not campaign_id:
        return {
            "status": "error",
            "message": "Campaign ID is required"
        }
    
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
    if execute and vin not in state.connected_tbms:
        raise HTTPException(status_code=409, detail={
            "code": "VEHICLE_NOT_CONNECTED",
            "message": "Vehicle is offline. Connect it before executing the campaign."
        })

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

    base_url = settings.PUBLIC_URL
    sgw_download_url = ""
    spaak_download_url = ""
    sgw_checksum = ""
    spaak_checksum = ""
    if sgw_firmware:

        sgw_download_url = (
            f"{base_url}/files/{sgw_firmware}"
        )

    if spaak_firmware:

        spaak_download_url = (
            f"{base_url}/files/{spaak_firmware}"
        )
    if sgw_firmware:
            
        path = os.path.join(
            settings.UPLOAD_FOLDER,
            sgw_firmware
        )
        if not os.path.exists(path):
            return {
                "status":"error",
                "message":"SGW firmware not found"
            }

        sgw_checksum = storage.get_sha256(path)
    
    if spaak_firmware:

        path = os.path.join(
            settings.UPLOAD_FOLDER,
            spaak_firmware
        )
        if not os.path.exists(path):
            return {
                "status":"error",
                "message":"SPAAK firmware not found"
            }

        spaak_checksum = storage.get_sha256(path)

    database.ensure_connection()
    execution_started_at = time.time() if execute else None
    try:
        if existing is not None:
            database.cursor.execute(
                """
                UPDATE campaigns SET status='sent', execution_started_at=%s,
                    last_heartbeat_at=%s, last_progress_at=%s,
                    error_code=NULL, error_message=NULL, failed_at=NULL
                WHERE campaign_id=%s AND status IN ('pending', 'created')
                RETURNING campaign_id
                """,
                (execution_started_at, execution_started_at, execution_started_at, campaign_id)
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
                    execution_started_at, last_heartbeat_at, last_progress_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    "sent" if execute else "pending",
                    execution_started_at,
                    execution_started_at,
                    execution_started_at
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

    if not execute:
        state.add_log(f"Campaign created: {campaign_id}")
        return {"status": "created", "campaign_id": campaign_id,
                "message": "Campaign created and awaiting execution."}

    ws = state.connected_tbms[vin]

    try:
        await ws.send_text(json.dumps({

            "type":"campaign",

            "campaign_id":campaign_id,

            "target_ecu":target_ecu,

            "campaign_name":campaign_name,

            "sgw_target_version":sgw_target_version,

            "spaak_target_version":spaak_target_version,

            "sgw_firmware":sgw_firmware,

            "spaak_firmware":spaak_firmware,

            "sgw_download_url":sgw_download_url,

            "spaak_download_url":spaak_download_url,

            "sgw_checksum":sgw_checksum,

            "spaak_checksum":spaak_checksum

        }))
    except Exception:

        database.cursor.execute(
            """
            UPDATE campaigns
            SET status='send_failed', error_code='SEND_FAILED',
                error_message='Unable to send campaign to the vehicle.', failed_at=%s
            WHERE campaign_id=%s AND status='sent'
            """,
            (time.time(), campaign_id)
        )
    
        database.conn.commit()
    
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
