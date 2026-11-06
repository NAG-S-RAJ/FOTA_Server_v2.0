import hashlib
import os

from fastapi import APIRouter, File, UploadFile
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

    path = os.path.join(
        settings.UPLOAD_FOLDER,
        file.filename
    )

    with open(path, "wb") as f:

        f.write(
            await file.read()
        )

    state.add_log(
        f"Firmware Uploaded: {file.filename}"
    )

    return {
        "status": "success",
        "file": file.filename
    }


@router.get("/files/{filename}")
async def file_download(
    filename: str
):

    path = os.path.join(
        settings.UPLOAD_FOLDER,
        filename
    )

    return FileResponse(
        path,
        filename=filename
    )
