import hashlib
import json
import secrets
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from . import config as settings, database, state
from .campaign_status import campaign_progress_value, is_active_campaign_status
from .campaigns import dispatch_waiting_campaigns, fail_campaign
from .events import record_campaign_event

router = APIRouter(tags=["Devices"])


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket
):

    await websocket.accept()

    vin = None

    try:

        while True:

            raw = await websocket.receive_text()

            state.add_log(
                f"RX: {raw}"
            )

            data = json.loads(raw)

            msg_type = data.get(
                "type"
            )

            if msg_type in ("campaign_ack", "running", "progress", "completed", "checksum_failed", "campaign_failed", "transfer_failed"):
                if state.connected_tbms.get(vin) is not websocket:
                    continue
                database.ensure_connection()
                database.cursor.execute(
                    "SELECT status FROM campaigns WHERE campaign_id=%s AND vin=%s",
                    (data.get("campaign_id"), vin)
                )
                campaign_row = database.cursor.fetchone()
                if campaign_row is None or not is_active_campaign_status(campaign_row[0]):
                    continue
                campaign_status = campaign_row[0]

            if msg_type == "register_request":

                vin = data["vin"]

                challenge = secrets.token_hex(16)

                state.pending_auth[vin] = {
                
                    "challenge": challenge,

                    "timestamp": time.time()

                }

                await websocket.send_text(
                    json.dumps({
                    
                        "type": "challenge",

                        "challenge": challenge

                    })
                )

                state.add_log(
                    f"Challenge sent -> {vin}"
                )

            elif msg_type == "auth_response":

               vin = data["vin"]

               response = data["response"]

               if vin not in state.pending_auth:

                   await websocket.close()

                   continue
               
               challenge = state.pending_auth[vin]["challenge"]

               if time.time() - state.pending_auth[vin]["timestamp"] > 60:

                   state.add_log(
                       f"{vin} auth timeout"
                   )

                   del state.pending_auth[vin]

                   await websocket.close()

                   continue
               
               expected = hashlib.sha256(

                   (
                       challenge +
                       settings.AUTH_SECRET
                   ).encode()

               ).hexdigest()

               if response != expected:

                   state.add_log(
                       f"{vin} authentication failed"
                   )

                   del state.pending_auth[vin]

                   await websocket.send_text(
                       json.dumps({
                           "type":"auth_failed"
                       })
                   )

                   await websocket.close()

                   continue
               
               await websocket.send_text(
                       json.dumps({
                           "type":"auth_passed"
                       })
                   )
               state.add_log(
                   f"{vin} authentication passed"
               )

               del state.pending_auth[vin]

               database.ensure_connection()

               database.cursor.execute(
                   """
                   SELECT vin
                   FROM registered_tbms
                   WHERE vin=%s
                   """,
                   (vin,)
               )

               row = database.cursor.fetchone()

               if row:

                   # Prevent duplicate VIN connections
                   if vin in state.connected_tbms:
                    
                        await websocket.send_text(
                            json.dumps({
                                "type": "already_connected"
                            })
                        )
                
                        state.add_log(
                            f"Duplicate connection rejected -> {vin}"
                        )
                
                        await websocket.close()
                
                        return

                   state.connected_tbms[vin] = websocket

                   await websocket.send_text(
                       json.dumps({
                           "type":"approved"
                       })
                   )

                   state.add_log(
                       f"{vin} auto-approved"
                   )

               else:

                   state.pending_tbms[vin] = websocket

                   state.add_log(
                       f"{vin} pending approval"
                   )

            elif msg_type in ("heartbeat", "campaign_request"):
                if state.connected_tbms.get(vin) is not websocket:
                    continue
                database.ensure_connection()
                database.cursor.execute(
                    """
                    UPDATE campaigns SET last_heartbeat_at=%s
                    WHERE vin=%s AND (
                        LOWER(status) IN ('sent', 'acknowledged', 'running', 'executing', 'in_progress')
                        OR LOWER(status) LIKE %s
                    )
                    """,
                    (time.time(), vin, "downloading%")
                )
                database.conn.commit()

                state.add_log(
                    f"Heartbeat Received -> {vin}"
                )

                # Executed campaigns for this vehicle are only sent once it pings.
                await dispatch_waiting_campaigns(vin, websocket)

            elif msg_type == "campaign_ack":
                if campaign_status.lower() != "sent":
                    continue

                database.ensure_connection()

                database.cursor.execute(
                    """
                    UPDATE campaigns
                                        SET status=%s, last_progress_at=%s
                    WHERE campaign_id=%s
                                            AND vin=%s AND status=%s
                    """,
                    (
                        "acknowledged",
                                                time.time(),
                        data["campaign_id"],
                                                vin,
                                                campaign_status
                    )
                )

                database.conn.commit()

                record_campaign_event(data["campaign_id"], vin, "Campaign Acknowledged", "RUNNING",
                                      "Vehicle acknowledged the campaign.")

                state.add_log(
                    f"{vin} campaign acknowledged"
                )

            elif msg_type == "running":
                if campaign_status.lower() not in ("sent", "acknowledged"):
                    continue
                database.cursor.execute(
                    """
                    UPDATE campaigns SET status='running', last_progress_at=%s
                    WHERE campaign_id=%s AND vin=%s AND status=%s
                    """,
                    (time.time(), data["campaign_id"], vin, campaign_status)
                )
                database.conn.commit()
                record_campaign_event(data["campaign_id"], vin, "Deployment Started", "RUNNING",
                                      "Vehicle started the deployment.")

            elif msg_type == "progress":
                progress = campaign_progress_value(f"Downloading {data.get('progress')}%")
                previous_progress = campaign_progress_value(campaign_status)
                if progress is None or (previous_progress is not None and progress <= previous_progress):
                    continue

                database.ensure_connection()

                database.cursor.execute(
                    """
                    UPDATE campaigns
                                        SET status=%s, last_progress_at=%s
                    WHERE campaign_id=%s
                                            AND vin=%s AND status=%s
                    """,
                    (
                                                f"Downloading {progress:g}%",
                                                time.time(),
                        data["campaign_id"],
                                                vin,
                                                campaign_status
                    )
                )

                database.conn.commit()

                record_campaign_event(data["campaign_id"], vin, "Download In Progress", "RUNNING",
                                      f"Progress {progress:g}%")

                state.add_log(
                    f"{vin} progress {data['progress']}%"
                )

            elif msg_type == "completed":

                database.ensure_connection()
            
                if data["target_ecu"] == "SGW":
                
                    database.cursor.execute(
                        """
                        UPDATE registered_tbms
                        SET sgw_version=%s
                        WHERE vin=%s
                        """,
                        (
                            data["sgw_version"],
                            vin
                        )
                    )
            
                elif data["target_ecu"] == "SPAAK":
                
                    database.cursor.execute(
                        """
                        UPDATE registered_tbms
                        SET spaak_version=%s
                        WHERE vin=%s
                        """,
                        (
                            data["spaak_version"],
                            vin
                        )
                    )
            
                elif data["target_ecu"] == "BOTH":
                
                    database.cursor.execute(
                        """
                        UPDATE registered_tbms
                        SET
                            sgw_version=%s,
                            spaak_version=%s
                        WHERE vin=%s
                        """,
                        (
                            data["sgw_version"],
                            data["spaak_version"],
                            vin
                        )
                    )
            
                database.cursor.execute(
                    """
                    UPDATE campaigns
                    SET status=%s, last_progress_at=%s, completed_at=%s
                    WHERE campaign_id=%s AND vin=%s AND status=%s
                    """,
                    (
                        "completed",
                        time.time(),
                        time.time(),
                        data["campaign_id"],
                        vin,
                        campaign_status
                    )
                )
            
                database.conn.commit()

                versions = ", ".join(
                    f"{ecu.upper()} {data[ecu + '_version']}"
                    for ecu in ("sgw", "spaak") if data.get(ecu + "_version")
                )
                record_campaign_event(data["campaign_id"], vin, "Completed", "SUCCESS",
                                      f"Update installed: {versions or data['target_ecu']}")
            
                state.add_log(
                    f"{vin} {data['target_ecu']} update completed"
                )

            elif msg_type == "checksum_failed":

                database.ensure_connection()
                fail_campaign(data["campaign_id"], vin, campaign_status, "CHECKSUM_FAILED",
                              "Firmware checksum verification failed.", time.time(),
                              terminal_status="checksum_failed")

            elif msg_type in ("campaign_failed", "transfer_failed"):
                database.ensure_connection()
                fail_campaign(data["campaign_id"], vin, campaign_status, msg_type.upper(),
                              str(data.get("reason") or "Vehicle reported a deployment failure.")[:1000],
                              time.time())

            elif msg_type == "status_response":

                state.add_log(
                    f"{vin}: "
                    f"{data['status']}"
                )

    except WebSocketDisconnect:

        state.add_log(
            f"{vin} disconnected"
        )

    finally:

        if vin:
        
            if vin in state.pending_tbms:
            
                if state.pending_tbms[vin] is websocket:
                
                    del state.pending_tbms[vin]
    
                    state.add_log(
                        f"Removed pending {vin}"
                    )
    
            if vin in state.connected_tbms:
            
                if state.connected_tbms[vin] is websocket:
                
                    del state.connected_tbms[vin]
    
                    state.add_log(
                        f"Removed connected {vin}"
                    )
