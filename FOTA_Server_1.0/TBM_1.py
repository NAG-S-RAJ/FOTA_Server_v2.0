import websocket
import json
import threading
import time
import requests
import urllib3
import hashlib
import os
import socket
import uuid
import re
import subprocess
from campaign_control import CampaignControl, CampaignStopped

campaign_control = CampaignControl()

# Generate VIN Unique to the device
hostname = socket.gethostname().upper()
hostname = re.sub(r'[^A-Z0-9]','',hostname)
machine_id = subprocess.check_output(
    [
        "powershell",
        "-Command",
        "(Get-CimInstance Win32_ComputerSystemProduct).UUID"
    ],
    text=True
).strip()
print(machine_id)

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

VIN = f"VH_{hostname}_{machine_id.replace('-', '')[-4:]}"
print("VIN : ",VIN)
AUTH_SECRET = "FOTA_DEMO_SECRET"
DOWNLOAD_DIR = r"C:\FirmwareFiles"
os.makedirs(
    DOWNLOAD_DIR,
    exist_ok=True
)

# CHANGE THIS
SERVER_WS = "wss://fota-demo-v2-0.onrender.com/ws"

approved = False
print("SERVER_WS =", SERVER_WS)

def heartbeat(ws):
    while True:
        try:
            if approved:
                ws.send(json.dumps({
                    "type": "heartbeat",
                    "vin": VIN
                }))
        except:
            break

        time.sleep(10)

def calculate_sha256(filepath):

    sha256 = hashlib.sha256()

    with open(filepath, "rb") as f:

        for chunk in iter(
            lambda: f.read(4096),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()

def on_open(ws):

    print("\nVehicle Bootup")

    ws.send(json.dumps({
        "type": "register_request",
        "vin": VIN
    }))

    print("\nConnection request sent")

    threading.Thread(
        target=heartbeat,
        args=(ws,),
        daemon=True
    ).start()


def on_message(ws, message, campaign_worker=False):

    global approved

    data = json.loads(message)
    msg_type = data.get("type")

    if msg_type == "stop_campaign":
        campaign_control.stop(data["campaign_id"])
        return
    if msg_type == "campaign" and not campaign_worker:
        campaign_control.start(ws, data, lambda worker_ws: on_message(worker_ws, message, True))
        return
    
    if msg_type == "challenge":

        print(
            "\nAuthentication Challenge Received"
        )
    else:
        print("RX:", data)

    if msg_type == "challenge":

        challenge = data["challenge"]

        response = hashlib.sha256(

            (
                challenge +
                AUTH_SECRET
            ).encode()

        ).hexdigest()

        ws.send(
            json.dumps({

                "type":"auth_response",

                "vin": VIN,

                "response": response

            })
        )
    elif msg_type == "auth_passed":

        print(
            "\nAUTHENTICATION PASSED"
        )

        print(
            "Waiting for approval..."
        )
    elif msg_type == "approved":

        approved = True

        print("\nVEHICLE CONNECTION APPROVED")
        print("Waiting for campaigns...\n")

    elif msg_type == "rejected":

        global rejected
        rejected = True
        print("\nConnection rejected")
        ws.close()

    elif msg_type == "already_connected":

        print("\nERROR: Another Vehicle with VIN", VIN, "is already connected.")
        ws.close()

    elif msg_type == "campaign":

        print("\nCampaign received")
    
        campaign_id = data["campaign_id"]
        campaign_name = data["campaign_name"]
        target_ecu = data["target_ecu"]
    
        print("Campaign :", campaign_name)
        print("Target ECU :", target_ecu)
    
        ws.send(json.dumps({
        
            "type": "campaign_ack",
    
            "vin": VIN,
    
            "campaign_id": campaign_id
    
        }))
    
        try:
        
            if target_ecu in ["SGW", "BOTH"]:
            
                print("\nDownloading SGW Firmware...")
    
                sgw_path = os.path.join(
                    DOWNLOAD_DIR,
                    data["sgw_firmware"]
                )

                campaign_control.download(ws, data["sgw_download_url"], sgw_path)
    
                print("SGW Firmware Saved :", sgw_path)
    
                if calculate_sha256(sgw_path) != data["sgw_checksum"]:
                
                    print("\nSGW Checksum Failed")
    
                    ws.send(json.dumps({
                    
                        "type": "checksum_failed",
    
                        "vin": VIN,
    
                        "campaign_id": campaign_id
    
                    }))
    
                    return
    
                print("SGW Checksum Passed")
    
            if target_ecu in ["SPAAK", "BOTH"]:
            
                print("\nDownloading SPAAK Firmware...")
    
                spaak_path = os.path.join(
                    DOWNLOAD_DIR,
                    data["spaak_firmware"]
                )

                campaign_control.download(ws, data["spaak_download_url"], spaak_path)
    
                print("SPAAK Firmware Saved :", spaak_path)
    
                if calculate_sha256(spaak_path) != data["spaak_checksum"]:
                
                    print("\nSPAAK Checksum Failed")
    
                    ws.send(json.dumps({
                    
                        "type": "checksum_failed",
    
                        "vin": VIN,
    
                        "campaign_id": campaign_id
    
                    }))
    
                    return
    
                print("SPAAK Checksum Passed")
    
            print("\nFirmware Download Complete")
    
            for progress in range(0, 101, 20):
            
                time.sleep(1)
    
                ws.send(json.dumps({
                
                    "type": "progress",
    
                    "vin": VIN,
    
                    "campaign_id": campaign_id,
    
                    "progress": progress
    
                }))
    
            completed = {
            
                "type": "completed",
    
                "vin": VIN,
    
                "campaign_id": campaign_id,
    
                "target_ecu": target_ecu
    
            }
    
            if target_ecu == "SGW":
            
                completed["sgw_version"] = data["sgw_target_version"]
    
            elif target_ecu == "SPAAK":
            
                completed["spaak_version"] = data["spaak_target_version"]
    
            else:
            
                completed["sgw_version"] = data["sgw_target_version"]
    
                completed["spaak_version"] = data["spaak_target_version"]
    
            ws.send(json.dumps(completed))
    
            print("\nCampaign completed")
    
        except CampaignStopped:
            print("Campaign stopped:", campaign_id)

        except Exception as e:
        
            print("\nDownload failed :", e)
            ws.send(json.dumps({"type": "campaign_failed", "campaign_id": campaign_id}))
    

def on_error(ws, error):
    print("\nERROR:", error)


def on_close(ws, code, msg):

    global approved

    approved = False
    campaign_control.stop()

    print("\nDisconnected from server")

while True:

    try:

        approved = False
        rejected = False

        print("\nConnecting to FOTA Server...")

        ws = websocket.WebSocketApp(
            SERVER_WS,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        ws.run_forever(
            ping_interval=20,
            ping_timeout=10
        )

    except Exception as e:

        print("\nConnection Error:", e)

    if rejected:

        print(
            "\nRequest rejected.\n"
            "Retrying registration in 10 seconds..."
        )

        time.sleep(10)

    else:

        print(
            "\nConnection lost.\n"
            "Retrying in 5 seconds..."
        )

        time.sleep(5)