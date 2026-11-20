import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
import os
import tempfile
import zipfile

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask

from . import config as settings, database, state
from .campaigns import campaign_watchdog, router as campaigns_router
from .firmware import router as firmware_router
from .storage import router as storage_router
from .vehicles import router as vehicles_router
from .websocket import router as websocket_router

router = APIRouter(tags=["System"])


@router.get("/")
async def root():

    return {
        "status": "running"
    }


@router.get("/health/ready")
async def readiness():
    try:
        database.ensure_connection()
        database.cursor.execute("SELECT 1")
        if database.cursor.fetchone() != (1,):
            raise RuntimeError("Database readiness query returned no result")
    except Exception as error:
        state.add_log(f"[SERVER][DB_UNAVAILABLE] readiness check failed: {type(error).__name__}")
        return JSONResponse(
            {"status": "not_ready", "checks": {"database": "unavailable"}},
            status_code=503
        )
    return {"status": "ready", "checks": {"database": "ready"}}


@router.get("/download_logs")
async def download_logs():
    capture_path = os.path.join(settings.CAPTURE_FOLDER, "FOTA_Capture.pcapng")
    if not os.path.isfile(capture_path):
        raise HTTPException(
            status_code=404,
            detail={"code": "CAPTURE_NOT_FOUND", "message": "No PCAPNG capture has been uploaded"}
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_name = f"FOTA_Logs_{timestamp}.zip"
    os.makedirs(settings.CAPTURE_FOLDER, exist_ok=True)
    descriptor, archive_path = tempfile.mkstemp(suffix=".zip", dir=settings.CAPTURE_FOLDER)
    os.close(descriptor)
    try:
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
            archive.writestr("FOTA_Server_Log.txt", "\n".join(state.logs))
            archive.write(capture_path, arcname="FOTA_Capture.pcapng")
    except OSError:
        if os.path.exists(archive_path):
            os.remove(archive_path)
        raise

    return FileResponse(
        archive_path,
        media_type="application/zip",
        filename=archive_name,
        background=BackgroundTask(os.remove, archive_path)
    )


@router.put("/upload_capture")
async def upload_capture(request: Request):
    os.makedirs(settings.CAPTURE_FOLDER, exist_ok=True)
    descriptor, temporary_path = tempfile.mkstemp(prefix=".capture-", suffix=".upload", dir=settings.CAPTURE_FOLDER)
    size = 0
    signature = bytearray()
    try:
        with os.fdopen(descriptor, "wb") as capture:
            async for chunk in request.stream():
                size += len(chunk)
                if size > settings.CAPTURE_MAX_BYTES:
                    raise HTTPException(status_code=413, detail="Capture exceeds the upload size limit")
                if len(signature) < 4:
                    signature.extend(chunk[:4 - len(signature)])
                capture.write(chunk)
        if bytes(signature) != b"\x0a\x0d\x0d\x0a":
            raise HTTPException(status_code=400, detail="Upload is not a PCAPNG capture")
        capture_path = os.path.join(settings.CAPTURE_FOLDER, "FOTA_Capture.pcapng")
        os.replace(temporary_path, capture_path)
    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)

    state.add_log(f"[CAPTURE][UPLOADED] pcapng size={size}B")
    return {"status": "uploaded", "size": size}


@router.get("/logs")
async def get_logs():

    return state.logs


@asynccontextmanager
async def campaign_lifespan(application):
    state.add_log("[SERVER][STARTING] initializing database and campaign watchdog")
    database.initialize_database()
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(settings.CAPTURE_FOLDER, exist_ok=True)
    watchdog = asyncio.create_task(campaign_watchdog())
    state.add_log(f"[SERVER][READY] uploads={settings.UPLOAD_FOLDER} public_url={settings.PUBLIC_URL}")
    try:
        yield
    finally:
        state.add_log("[SERVER][STOPPING] cancelling watchdog and closing database")
        watchdog.cancel()
        with suppress(asyncio.CancelledError):
            await watchdog
        database.close_database()


def create_app():
    application = FastAPI(lifespan=campaign_lifespan)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    for route_group in (router, campaigns_router, vehicles_router, firmware_router, storage_router, websocket_router):
        application.include_router(route_group)
    return application
