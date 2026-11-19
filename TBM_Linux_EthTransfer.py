import json
import threading
from tbm_monitor import TbmMonitor
monitor = TbmMonitor("TBM_Linux_EthTransfer")
from campaign_control import CampaignControl, CampaignStopped

campaign_control = CampaignControl()
import time
import requests
import urllib3
import hashlib
import os
import socket
import re

# ============================================================
# RASPBERRY PI DEVICE ID / VIN GENERATION
# ============================================================

hostname = socket.gethostname().upper()
hostname = re.sub(r'[^A-Z0-9]', '', hostname)

def get_device_id():
    """
    Get a unique device identifier for Raspberry Pi.
    Prefer Raspberry Pi product UUID if available.
    Otherwise use /etc/machine-id.
    """

    # Try product UUID
    uuid_path = "/sys/class/dmi/id/product_uuid"

    try:
        if os.path.exists(uuid_path):
            with open(uuid_path, "r") as f:
                device_id = f.read().strip()

            if device_id:
                return device_id

    except Exception:
        pass

    # Fallback to machine-id
    machine_id_path = "/etc/machine-id"

    try:
        with open(machine_id_path, "r") as f:
            device_id = f.read().strip()

        if device_id:
            return device_id

    except Exception:
        pass

    # Final fallback
    return socket.gethostname()


DEVICE_ID = get_device_id()

print("Device ID :", DEVICE_ID)

# Remove special characters
DEVICE_ID_CLEAN = re.sub(
    r'[^A-Z0-9]',
    '',
    DEVICE_ID.upper()
)

# Generate VIN
VIN = f"VH_{hostname}_{DEVICE_ID_CLEAN[-8:]}"
monitor.vin = VIN

print("VIN :", VIN)


# ============================================================
# CONFIGURATION
# ============================================================

AUTH_SECRET = "FOTA_DEMO_SECRET"

SERVER_WS = "wss://fota-server-v2-0.onrender.com/ws"

DOWNLOAD_DIR = "/home/pi/FirmwareFiles"

# Direct Ethernet connection to the Windows laptop.
# Configure the laptop Ethernet adapter as 192.168.50.2/24.
LAPTOP_IP = "192.168.50.2"
LAPTOP_UPLOAD_PORT = 8001
LAPTOP_UPLOAD_URL = f"http://{LAPTOP_IP}:{LAPTOP_UPLOAD_PORT}/upload"
ETHERNET_CONNECT_TIMEOUT = 3
FILE_TRANSFER_TIMEOUT = 180

os.makedirs(
    DOWNLOAD_DIR,
    exist_ok=True
)

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

approved = False
rejected = False

print("SERVER_WS =", SERVER_WS)
print("DOWNLOAD_DIR =", DOWNLOAD_DIR)


# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat(ws):

    while True:

        try:

            if approved:

                ws.send(
                    json.dumps({
                        "type": "heartbeat",
                        "vin": VIN
                    })
                )

        except Exception:

            break

        time.sleep(10)


# ============================================================
# SHA256
# ============================================================

def calculate_sha256(filepath):

    sha256 = hashlib.sha256()

    with open(filepath, "rb") as f:

        for chunk in iter(
            lambda: f.read(4096),
            b""
        ):

            sha256.update(chunk)

    return sha256.hexdigest()


# ============================================================
# ETHERNET CHECK + FIRMWARE TRANSFER TO LAPTOP
# ============================================================

def check_laptop_connection():
    """Check that the laptop's file receiver is reachable over Ethernet."""
    print(f"\nChecking Ethernet connection to laptop {LAPTOP_IP}:{LAPTOP_UPLOAD_PORT}...")
    try:
        with socket.create_connection(
            (LAPTOP_IP, LAPTOP_UPLOAD_PORT),
            timeout=ETHERNET_CONNECT_TIMEOUT,
        ):
            print("Laptop Ethernet connection: OK")
            return True
    except OSError as exc:
        print(f"Laptop Ethernet connection FAILED: {exc}")
        print("Check the cable, laptop IP, and that receiver.py is running on the laptop.")
        return False


def transfer_firmware_to_laptop(filepath):
    """Upload a checksum-verified firmware file to the laptop receiver."""
    filepath = os.path.abspath(filepath)
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Firmware file not found: {filepath}")

    if not check_laptop_connection():
        raise ConnectionError("Laptop receiver is not reachable over Ethernet")

    filename = os.path.basename(filepath)
    print(f"Transferring {filename} to laptop {LAPTOP_IP}...")

    with open(filepath, "rb") as firmware_file:
        response = requests.post(
            LAPTOP_UPLOAD_URL,
            files={"file": (filename, firmware_file, "application/octet-stream")},
            timeout=FILE_TRANSFER_TIMEOUT,
        )

    response.raise_for_status()
    result = response.json()
    if result.get("status") != "received":
        raise RuntimeError(f"Laptop did not confirm file receipt: {result}")

    print(f"Transfer confirmed by laptop: {result.get('filename', filename)}")
    return result


# ============================================================
# CONNECTION OPEN
# ============================================================

def on_open(ws):

    print("\nVehicle Bootup")

    ws.send(
        json.dumps({
            "type": "register_request",
            "vin": VIN
        })
    )

    print("\nConnection request sent")

    threading.Thread(
        target=heartbeat,
        args=(ws,),
        daemon=True
    ).start()


# ============================================================
# RECEIVE MESSAGE
# ============================================================

def on_message(ws, message, campaign_worker=False):

    global approved
    global rejected

    if not campaign_worker:
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SERVER RX: {message}",
            flush=True,
        )

    try:

        data = json.loads(message)
        if data.get("type") == "stop_campaign":
            campaign_control.stop(data["campaign_id"])
            return
        if data.get("type") == "campaign" and not campaign_worker:
            campaign_control.start(ws, data, lambda worker_ws: on_message(worker_ws, message, True))
            return

    except json.JSONDecodeError:

        print("Invalid JSON received:")
        print(message)

        return

    msg_type = data.get("type")

    # --------------------------------------------------------
    # AUTHENTICATION CHALLENGE
    # --------------------------------------------------------

    if msg_type == "challenge":

        print(
            "\nAuthentication Challenge Received"
        )

        challenge = data["challenge"]

        response = hashlib.sha256(
            (
                challenge +
                AUTH_SECRET
            ).encode()
        ).hexdigest()

        ws.send(
            json.dumps({
                "type": "auth_response",
                "vin": VIN,
                "response": response
            })
        )

    # --------------------------------------------------------
    # AUTHENTICATION PASSED
    # --------------------------------------------------------

    elif msg_type == "auth_passed":

        print(
            "\nAUTHENTICATION PASSED"
        )

        print(
            "Waiting for approval..."
        )

    # --------------------------------------------------------
    # APPROVED
    # --------------------------------------------------------

    elif msg_type == "approved":

        approved = True

        print(
            "\nVEHICLE CONNECTION APPROVED"
        )

        print(
            "Waiting for campaigns...\n"
        )

    # --------------------------------------------------------
    # REJECTED
    # --------------------------------------------------------

    elif msg_type == "rejected":

        rejected = True
        approved = False

        print(
            "\nConnection rejected"
        )

        ws.close()

    # --------------------------------------------------------
    # ALREADY CONNECTED
    # --------------------------------------------------------

    elif msg_type == "already_connected":

        print(
            "\nERROR: Another Vehicle with VIN",
            VIN,
            "is already connected."
        )

        ws.close()

    # --------------------------------------------------------
    # CAMPAIGN
    # --------------------------------------------------------

    elif msg_type == "campaign":

        print("\nCampaign received")

        campaign_id = data["campaign_id"]
        campaign_name = data["campaign_name"]
        target_ecu = data["target_ecu"]

        print(
            "Campaign :",
            campaign_name
        )

        print(
            "Target ECU :",
            target_ecu
        )

        # ----------------------------------------------------
        # CAMPAIGN ACK
        # ----------------------------------------------------

        ws.send(
            json.dumps({
                "type": "campaign_ack",
                "vin": VIN,
                "campaign_id": campaign_id
            })
        )

        try:

            # =================================================
            # SGW
            # =================================================

            if target_ecu in ["SGW", "BOTH"]:

                print(
                    "\nDownloading SGW Firmware..."
                )

                sgw_path = os.path.join(
                    DOWNLOAD_DIR,
                    data["sgw_firmware"]
                )

                campaign_control.download(ws, data["sgw_download_url"], sgw_path)

                print(
                    "SGW Firmware Saved :",
                    sgw_path
                )

                # SHA256 verification
                calculated_hash = calculate_sha256(
                    sgw_path
                )

                print(
                    "SGW SHA256 :",
                    calculated_hash
                )

                if calculated_hash != data["sgw_checksum"]:

                    print(
                        "\nSGW Checksum Failed"
                    )

                    ws.send(
                        json.dumps({
                            "type": "checksum_failed",
                            "vin": VIN,
                            "campaign_id": campaign_id
                        })
                    )

                    return

                print(
                    "SGW Checksum Passed"
                )

            # =================================================
            # SPAAK
            # =================================================

            if target_ecu in ["SPAAK", "BOTH"]:

                print(
                    "\nDownloading SPAAK Firmware..."
                )

                spaak_path = os.path.join(
                    DOWNLOAD_DIR,
                    data["spaak_firmware"]
                )

                campaign_control.download(ws, data["spaak_download_url"], spaak_path)

                print(
                    "SPAAK Firmware Saved :",
                    spaak_path
                )

                # SHA256 verification
                calculated_hash = calculate_sha256(
                    spaak_path
                )

                print(
                    "SPAAK SHA256 :",
                    calculated_hash
                )

                if calculated_hash != data["spaak_checksum"]:

                    print(
                        "\nSPAAK Checksum Failed"
                    )

                    ws.send(
                        json.dumps({
                            "type": "checksum_failed",
                            "vin": VIN,
                            "campaign_id": campaign_id
                        })
                    )

                    return

                print(
                    "SPAAK Checksum Passed"
                )

            # =================================================
            # DOWNLOAD COMPLETE
            # =================================================

            print(
                "\nFirmware Download Complete"
            )

            # =================================================
            # VERIFY ETHERNET AND TRANSFER CHECKSUM-VERIFIED BIN FILES
            # =================================================
            firmware_to_transfer = []
            if target_ecu in ["SGW", "BOTH"]:
                firmware_to_transfer.append(sgw_path)
            if target_ecu in ["SPAAK", "BOTH"]:
                firmware_to_transfer.append(spaak_path)

            try:
                for firmware_path in firmware_to_transfer:
                    ws.check_active()
                    transfer_firmware_to_laptop(firmware_path)
                    ws.check_active()
            except CampaignStopped:
                raise
            except Exception as transfer_error:
                print("\nFirmware transfer failed:", transfer_error)
                ws.send(json.dumps({
                    "type": "transfer_failed",
                    "vin": VIN,
                    "campaign_id": campaign_id,
                    "reason": str(transfer_error),
                }))
                return

            print("All required firmware files transferred to laptop successfully.")

            # =================================================
            # SIMULATE UPDATE PROGRESS
            # =================================================

            for progress in range(
                0,
                101,
                20
            ):

                time.sleep(1)

                ws.send(
                    json.dumps({
                        "type": "progress",
                        "vin": VIN,
                        "campaign_id": campaign_id,
                        "progress": progress
                    })
                )

                print(
                    "Update Progress :",
                    progress,
                    "%"
                )

            # =================================================
            # CAMPAIGN COMPLETE
            # =================================================

            completed = {

                "type": "completed",

                "vin": VIN,

                "campaign_id": campaign_id,

                "target_ecu": target_ecu
            }

            if target_ecu == "SGW":

                completed[
                    "sgw_version"
                ] = data[
                    "sgw_target_version"
                ]

            elif target_ecu == "SPAAK":

                completed[
                    "spaak_version"
                ] = data[
                    "spaak_target_version"
                ]

            else:

                completed[
                    "sgw_version"
                ] = data[
                    "sgw_target_version"
                ]

                completed[
                    "spaak_version"
                ] = data[
                    "spaak_target_version"
                ]

            ws.send(
                json.dumps(completed)
            )

            print(
                "\nCampaign completed"
            )

        except CampaignStopped:
            print("Campaign stopped:", campaign_id)

        except Exception as e:

            print(
                "\nDownload failed :",
                e
            )
            ws.send(json.dumps({"type": "campaign_failed", "campaign_id": campaign_id,
                                "reason": f"Download failed: {e}"}))

    # --------------------------------------------------------
    # OTHER SERVER MESSAGE
    # --------------------------------------------------------

    else:

        print(
            "RX:",
            data
        )


# ============================================================
# ERROR
# ============================================================

def on_error(ws, error):

    print(
        "\nERROR:",
        error
    )


# ============================================================
# CLOSE
# ============================================================

def on_close(
    ws,
    code,
    msg
):

    global approved

    approved = False
    campaign_control.stop()

    print(
        "\nDisconnected from server"
    )

    print(
        "Close code :",
        code
    )

    print(
        "Close message :",
        msg
    )


# ============================================================
# MAIN CONNECTION LOOP
# ============================================================

def connection_loop():

    global approved, rejected

    while not monitor.stop_event.is_set():

        try:

            approved = False
            rejected = False

            print(
                "\nConnecting to FOTA Server..."
            )

            ws = monitor.create_websocket(
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

            print(
                "\nConnection Error:",
                e
            )

        if monitor.stop_event.is_set():
            break

        if rejected:

            print(
                "\nRequest rejected.\n"
                "Retrying registration in 10 seconds..."
            )

            monitor.stop_event.wait(10)

        else:

            print(
                "\nConnection lost.\n"
                "Retrying in 5 seconds..."
            )

            monitor.stop_event.wait(5)


monitor.run(connection_loop)
