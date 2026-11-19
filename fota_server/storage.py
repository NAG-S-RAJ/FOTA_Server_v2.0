import hashlib
import os

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from . import config as settings, state

router = APIRouter(tags=["Local firmware"])


def get_sha256(filepath):

    sha256 = hashlib.sha256()

    with open(filepath, "rb") as f:

        for chunk in iter(
            lambda: f.read(4096),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


@router.post("/upload")
async def upload(
    file: UploadFile = File(...)
):

    filename = os.path.basename(file.filename or "")
    if not filename or filename != file.filename:
        state.add_log(f"[UPLOAD][INVALID_FILENAME] rejected filename={file.filename!r}")
        raise HTTPException(status_code=400, detail={"code": "INVALID_FILENAME", "message": "Invalid file name"})

    path = os.path.join(
        settings.UPLOAD_FOLDER,
        filename
    )

    try:
        content = await file.read()
        with open(path, "wb") as f:
            f.write(content)
    except OSError as error:
        state.add_log(f"[UPLOAD][WRITE_FAILED] file={filename} {type(error).__name__}: {error}")
        raise HTTPException(status_code=500, detail={"code": "UPLOAD_WRITE_FAILED", "message": "Unable to store firmware"})

    state.add_log(
        f"[UPLOAD][UPLOADED] file={filename} size={len(content)}B sha256={hashlib.sha256(content).hexdigest()}"
    )

    return {
        "status": "success",
        "file": filename
    }


@router.get("/files/{filename}")
async def file_download(
    filename: str
):

    path = os.path.join(
        settings.UPLOAD_FOLDER,
        filename
    )

    if os.path.basename(filename) != filename or not os.path.isfile(path):
        state.add_log(f"[DOWNLOAD][FILE_NOT_FOUND] file={filename!r}")
        raise HTTPException(status_code=404, detail={"code": "FILE_NOT_FOUND", "message": "Firmware not found"})

    state.add_log(f"[DOWNLOAD][SERVING] file={filename} size={os.path.getsize(path)}B")

    return FileResponse(
        path,
        filename=filename
    )
