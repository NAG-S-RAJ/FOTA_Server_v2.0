import asyncio
from contextlib import asynccontextmanager, suppress
import os

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

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
    except Exception:
        return JSONResponse(
            {"status": "not_ready", "checks": {"database": "unavailable"}},
            status_code=503
        )
    return {"status": "ready", "checks": {"database": "ready"}}


@router.get("/download_logs")
async def download_logs():

    return PlainTextResponse(
        "\n".join(state.logs),
        headers={
            "Content-Disposition":
            "attachment; filename=fota_logs.txt"
        }
    )


@router.get("/logs")
async def get_logs():

    return state.logs


@asynccontextmanager
async def campaign_lifespan(application):
    database.initialize_database()
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    watchdog = asyncio.create_task(campaign_watchdog())
    try:
        yield
    finally:
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
