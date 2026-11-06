import json
import os
import threading

import requests


class CampaignStopped(Exception):
    pass


class CampaignSocket:
    def __init__(self, websocket, stop_event):
        self.websocket = websocket
        self.stop_event = stop_event

    def check_active(self):
        if self.stop_event.is_set():
            raise CampaignStopped("Campaign stopped")

    def send(self, message):
        self.check_active()
        return self.websocket.send(message)


class CampaignControl:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = None

    def start(self, websocket, data, handler):
        campaign_id = data["campaign_id"]
        with self.lock:
            if self.active is not None:
                websocket.send(json.dumps({
                    "type": "campaign_failed",
                    "campaign_id": campaign_id,
                    "reason": "Another campaign is still active on this vehicle"
                }))
                return
            stop_event = threading.Event()
            self.active = (campaign_id, stop_event)

        def run():
            try:
                handler(CampaignSocket(websocket, stop_event))
            except CampaignStopped:
                print("Campaign stopped:", campaign_id)
            except Exception as error:
                print("Campaign failed:", campaign_id, error)
                if not stop_event.is_set():
                    try:
                        websocket.send(json.dumps({
                            "type": "campaign_failed",
                            "campaign_id": campaign_id,
                            "reason": str(error)
                        }))
                    except Exception:
                        pass
            finally:
                with self.lock:
                    if self.active == (campaign_id, stop_event):
                        self.active = None

        threading.Thread(target=run, daemon=True).start()

    def stop(self, campaign_id=None):
        with self.lock:
            if self.active and (campaign_id is None or self.active[0] == campaign_id):
                self.active[1].set()

    def download(self, websocket, url, filepath):
        websocket.check_active()
        partial_path = filepath + ".part"
        try:
            with requests.get(url, stream=True, verify=False, timeout=(10, 10)) as response:
                response.raise_for_status()
                with open(partial_path, "wb") as firmware_file:
                    for chunk in response.iter_content(chunk_size=65536):
                        websocket.check_active()
                        if chunk:
                            firmware_file.write(chunk)
            websocket.check_active()
            os.replace(partial_path, filepath)
        finally:
            if os.path.exists(partial_path):
                os.remove(partial_path)