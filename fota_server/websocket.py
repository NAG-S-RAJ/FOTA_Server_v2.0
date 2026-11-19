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
    client = getattr(websocket, "client", None)
    state.add_log(f"[WS][CONNECTION_OPENED] client={client}")

    try:

        while True:

            raw = await websocket.receive_text()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError as error:
                state.add_log(f"[WS][INVALID_JSON] vin={vin} {error}: {raw[:200]}")
                continue
            if not isinstance(data, dict):
                state.add_log(f"[WS][INVALID_MESSAGE] vin={vin} message is not a JSON object")
                continue

            msg_type = data.get(
                "type"
            )

            # Log batches are logged line by line below instead of as one raw message.
            if msg_type not in ("sgw_traffic", "tbm_log"):
                state.add_log(
                    f"[WS][RX] vin={vin} {raw}"
                )

            if msg_type in ("campaign_ack", "running", "progress", "completed", "checksum_failed", "campaign_failed", "transfer_failed"):
                if state.connected_tbms.get(vin) is not websocket:
                    state.add_log(f"[DEVICE][UNAUTHORIZED_MESSAGE] vin={vin} type={msg_type} from unapproved connection ignored")
                    continue
                database.ensure_connection()
                database.cursor.execute(
                    "SELECT status FROM campaigns WHERE campaign_id=%s AND vin=%s",
                    (data.get("campaign_id"), vin)
                )
                campaign_row = database.cursor.fetchone()
                if campaign_row is None:
                    state.add_log(f"[DEVICE][CAMPAIGN_NOT_FOUND] vin={vin} type={msg_type} campaign={data.get('campaign_id')}")
                    continue
                if not is_active_campaign_status(campaign_row[0]):
                    state.add_log(f"[DEVICE][LATE_MESSAGE_IGNORED] vin={vin} type={msg_type} "
                                  f"campaign={data.get('campaign_id')} status={campaign_row[0]}")
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
                    f"[AUTH][CHALLENGE_SENT] vin={vin}"
                )

            elif msg_type == "auth_response":

               vin = data["vin"]

               response = data["response"]

               if vin not in state.pending_auth:

                   state.add_log(f"[AUTH][NO_PENDING_CHALLENGE] vin={vin} auth_response without challenge; closing")

                   await websocket.close()

                   continue
               
               challenge = state.pending_auth[vin]["challenge"]

               if time.time() - state.pending_auth[vin]["timestamp"] > 60:

                   state.add_log(
                       f"[AUTH][AUTH_TIMEOUT] vin={vin} challenge older than 60s; closing"
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
                       f"[AUTH][AUTH_FAILED] vin={vin} invalid challenge response; closing"
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
                   f"[AUTH][AUTH_PASSED] vin={vin}"
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
                            f"[AUTH][DUPLICATE_CONNECTION] vin={vin} already connected; new connection rejected"
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
                       f"[AUTH][CONNECTED] vin={vin} auto-approved; online vehicles={len(state.connected_tbms)}"
                   )

               else:

                   state.pending_tbms[vin] = websocket

                   state.add_log(
                       f"[AUTH][PENDING_APPROVAL] vin={vin} not registered; waiting for operator approval"
                   )

            elif msg_type in ("heartbeat", "campaign_request"):
                if state.connected_tbms.get(vin) is not websocket:
                    state.add_log(f"[DEVICE][HEARTBEAT_IGNORED] vin={vin} type={msg_type} from unapproved connection")
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
                    f"[DEVICE][PING] vin={vin} type={msg_type}"
                )

                # Executed campaigns for this vehicle are only sent once it pings.
                await dispatch_waiting_campaigns(vin, websocket)

            elif msg_type == "campaign_ack":
                if campaign_status.lower() != "sent":
                    state.add_log(f"[DEVICE][ACK_IGNORED] vin={vin} campaign={data['campaign_id']} status={campaign_status}")
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
                    f"[DEVICE][ACKNOWLEDGED] vin={vin} campaign={data['campaign_id']} sent -> acknowledged"
                )

            elif msg_type == "running":
                if campaign_status.lower() not in ("sent", "acknowledged"):
                    state.add_log(f"[DEVICE][RUNNING_IGNORED] vin={vin} campaign={data['campaign_id']} status={campaign_status}")
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
                state.add_log(f"[DEVICE][RUNNING] vin={vin} campaign={data['campaign_id']} {campaign_status} -> running")

            elif msg_type == "progress":
                progress = campaign_progress_value(f"Downloading {data.get('progress')}%")
                previous_progress = campaign_progress_value(campaign_status)
                if progress is None:
                    state.add_log(f"[DEVICE][INVALID_PROGRESS] vin={vin} campaign={data['campaign_id']} "
                                  f"progress={data.get('progress')!r}")
                    continue
                if previous_progress is not None and progress <= previous_progress:
                    state.add_log(f"[DEVICE][PROGRESS_NOT_ADVANCING] vin={vin} campaign={data['campaign_id']} "
                                  f"{progress:g}% <= {previous_progress:g}%")
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
                    f"[DEVICE][PROGRESS] vin={vin} campaign={data['campaign_id']} {data['progress']}%"
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

                else:
                    state.add_log(f"[DEVICE][UNKNOWN_TARGET_ECU] vin={vin} campaign={data['campaign_id']} "
                                  f"target_ecu={data.get('target_ecu')!r}; versions not updated")
            
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
                    f"[DEVICE][COMPLETED] vin={vin} campaign={data['campaign_id']} {data['target_ecu']} "
                    f"update completed: {versions or '-'}"
                )

            elif msg_type == "checksum_failed":

                state.add_log(f"[DEVICE][CHECKSUM_FAILED] vin={vin} campaign={data['campaign_id']} "
                              "vehicle reported checksum mismatch")
                database.ensure_connection()
                fail_campaign(data["campaign_id"], vin, campaign_status, "CHECKSUM_FAILED",
                              "Firmware checksum verification failed.", time.time(),
                              terminal_status="checksum_failed")

            elif msg_type in ("campaign_failed", "transfer_failed"):
                state.add_log(f"[DEVICE][{msg_type.upper()}] vin={vin} campaign={data['campaign_id']} "
                              f"reason={data.get('reason') or '-'}")
                database.ensure_connection()
                fail_campaign(data["campaign_id"], vin, campaign_status, msg_type.upper(),
                              str(data.get("reason") or "Vehicle reported a deployment failure.")[:1000],
                              time.time())

            elif msg_type == "status_response":

                state.add_log(
                    f"[DEVICE][STATUS] vin={vin}: "
                    f"{data.get('status')}"
                )

            elif msg_type in ("sgw_traffic", "sgw_capture", "tbm_log"):
                area = "TBM" if msg_type == "tbm_log" else "SGW"
                if state.connected_tbms.get(vin) is not websocket:
                    state.add_log(f"[{area}][UNAUTHORIZED_MESSAGE] vin={vin} type={msg_type} from unapproved connection ignored")
                    continue
                if msg_type == "sgw_capture":
                    capture_status = "".join(ch for ch in str(data.get("status") or "unknown").upper() if ch.isalnum() or ch == "_")
                    state.add_log(f"[SGW][CAPTURE_{capture_status[:20]}] vin={vin} {str(data.get('detail') or '')[:300]}")
                    continue
                code = "LOG" if msg_type == "tbm_log" else "TRAFFIC"
                lines = data.get("lines")
                if not isinstance(lines, list):
                    state.add_log(f"[{area}][INVALID_{code}] vin={vin} lines must be a list")
                    continue
                for line in lines[:50]:
                    state.add_log(f"[{area}][{code}] vin={vin} {str(line)[:400]}")
                try:
                    dropped = int(data.get("dropped") or 0) + max(0, len(lines) - 50)
                except (TypeError, ValueError):
                    dropped = 0
                if dropped > 0:
                    state.add_log(f"[{area}][{code}_SUMMARIZED] vin={vin} {dropped} more line(s) not shown; "
                                  "see the TBM log/capture file")

            else:
                state.add_log(f"[WS][UNKNOWN_MESSAGE] vin={vin} type={msg_type!r} ignored")

    except WebSocketDisconnect as disconnect:

        state.add_log(
            f"[WS][DISCONNECTED] vin={vin} code={getattr(disconnect, 'code', None)}"
        )

    except Exception as error:

        state.add_log(f"[WS][CONNECTION_ERROR] vin={vin} {type(error).__name__}: {error}")
        raise

    finally:

        if vin:
        
            if vin in state.pending_tbms:
            
                if state.pending_tbms[vin] is websocket:
                
                    del state.pending_tbms[vin]
    
                    state.add_log(
                        f"[WS][REMOVED_PENDING] vin={vin}"
                    )
    
            if vin in state.connected_tbms:
            
                if state.connected_tbms[vin] is websocket:
                
                    del state.connected_tbms[vin]
    
                    state.add_log(
                        f"[WS][VEHICLE_OFFLINE] vin={vin} removed; online vehicles={len(state.connected_tbms)}"
                    )
