import base64
from datetime import datetime
import json
import os
from zoneinfo import ZoneInfo

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import Response
import requests

from . import config as settings

router = APIRouter(tags=["Firmware repository"])


@router.get("/github_test")
def github_test():

    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    response = requests.get(
        "https://api.github.com/user",
        headers=headers
    )

    return response.json()


def upload_to_github(path, content):

    url = (
        f"https://api.github.com/repos/"
        f"{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}/contents/{path}"
    )

    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    data = {
        "message": f"Upload {path}",
        "content": base64.b64encode(content).decode(),
        "branch": settings.GITHUB_BRANCH
    }

    response = requests.put(
        url,
        headers=headers,
        json=data
    )

    print("UPLOAD STATUS:", response.status_code)
    print("UPLOAD BODY:", response.text)

    return response.json()


def normalize_latest_firmware(latest):
    latest = dict(latest)
    if "BCM" in latest:
        previous = latest.pop("BCM")
        latest.setdefault("SPAAK", previous)
    if isinstance(latest.get("SPAAK"), dict):
        latest["SPAAK"] = dict(latest["SPAAK"])
        if isinstance(latest["SPAAK"].get("file"), str):
            latest["SPAAK"]["file"] = latest["SPAAK"]["file"].replace("BCM", "SPAAK")
    return latest


def read_latest_json():

    url = (
        f"https://api.github.com/repos/"
        f"{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}"
        f"/contents/latest.json"
    )

    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    r = requests.get(url, headers=headers)

    if r.status_code == 404:
        return {}, None

    r.raise_for_status()

    data = r.json()

    latest = json.loads(
        base64.b64decode(data["content"]).decode()
    )

    return normalize_latest_firmware(latest), data["sha"]


def update_latest_json(data, sha):

    url = (
        f"https://api.github.com/repos/"
        f"{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}"
        f"/contents/latest.json"
    )

    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    body = {
        "message": "Update latest firmware",
        "content": base64.b64encode(
            json.dumps(data, indent=4).encode()
        ).decode(),
        "branch": settings.GITHUB_BRANCH
    }
    
    if sha:
        body["sha"] = sha
    
    r = requests.put(
        url,
        headers=headers,
        json=body
    )
    
    print("LATEST STATUS:", r.status_code)
    print("LATEST BODY:", r.text)


def github_list(path):

    url = (
        f"https://api.github.com/repos/"
        f"{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}"
        f"/contents/{path}"
    )

    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    response = requests.get(
        url,
        headers=headers
    )

    response.raise_for_status()

    return response.json()


@router.post("/upload_firmware")
async def upload_firmware(
    ecu: str,
    version: str,
    file: UploadFile = File(...)
):

    content = await file.read()
    version = f"{float(version):.1f}"
    filename = f"{ecu}_v{version}.bin"
    uploaded_at = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%m-%Y %I:%M:%S %p")
    github_path = f"firmware/{ecu}/{filename}"

    result = upload_to_github(
        github_path,
        content
    )
    
    if "content" not in result:
    
        return {
            "status": "error",
            "github": result
        }
    
    latest, sha = read_latest_json()
    
    latest[ecu] = {
    
        "version": version,
    
        "file": github_path,

        "path": f"firmware/{ecu}/{filename}",
    
        "download_url": result["content"]["download_url"],

        "uploaded_at": uploaded_at
    
    }
    
    update_latest_json(
        latest,
        sha
    )
    
    return {
    
        "status": "success",
    
        "file": filename,

        "path": f"firmware/{ecu}/{filename}",
    
        "github": result
    
    }


@router.get("/latest_firmware")
async def latest_firmware():

    try:

        latest, _ = read_latest_json()

        return latest

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }


def version_key(item):
    try:
        return (item["ecu"], float(item.get("version", 0)))
    except (ValueError, TypeError):
        return (item["ecu"], 0.0)


@router.get("/firmware_history")
async def firmware_history():

    try:

        history = []

        for ecu, directory in [("SGW", "SGW"), ("SPAAK", "SPAAK"), ("SPAAK", "BCM")]:
            try:
                files = github_list(f"firmware/{directory}")
            except requests.HTTPError as error:
                if error.response is not None and error.response.status_code == 404:
                    continue
                raise

            if not isinstance(files, list):
                continue

            for file in files:

                if file["type"] != "file":
                    continue

                filename = file["name"]

                version = ""

                if "_v" in filename:

                    version = (
                        filename
                        .split("_v")[1]
                        .replace(".bin", "")
                    )

                history.append({

                    "ecu": ecu,

                    "version": version,

                    "file": filename.replace("BCM", "SPAAK"),

                    "path": f"firmware/{directory}/{filename}",

                    "download_url": file["download_url"]

                })

        history.sort(
            key=version_key,
            reverse=True
            )

        return history

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }


@router.get("/download_firmware")
async def download_firmware(path: str):

    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.raw"
    }

    url = f"https://api.github.com/repos/{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}/contents/{path}"

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        raise HTTPException(
            status_code=404,
            detail=f"GitHub returned {r.status_code}: {r.text}"
        )

    return Response(
        content=r.content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition":
            f'attachment; filename="{os.path.basename(path)}"'
        }
    )
