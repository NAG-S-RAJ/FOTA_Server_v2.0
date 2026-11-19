from datetime import datetime
import json
from zoneinfo import ZoneInfo

from fastapi import APIRouter
import psycopg2

from . import database, state

router = APIRouter(tags=["Vehicles"])


@router.delete("/registered_tbm/{vin}")
async def delete_registered_tbm(
    vin: str
):  
    database.ensure_connection()

    database.cursor.execute(
        """
        DELETE
        FROM registered_tbms
        WHERE vin=%s
        RETURNING vin
        """,
        (vin,)
    )
    deleted = database.cursor.fetchone()

    database.conn.commit()

    state.add_log(
        f"[VEHICLE][DELETED] vin={vin}" if deleted else f"[VEHICLE][VIN_NOT_FOUND] delete requested for vin={vin}"
    )

    return {
        "status": "deleted"
    }


@router.get("/registered_tbm/{vin}")
async def get_tbm(
    vin: str
):
    database.ensure_connection()
    database.cursor.execute("""

    SELECT

    vin,

    sgw_version,

    spaak_version

    FROM registered_tbms

    WHERE vin = %s

    """,

    (vin,)
    )

    row = database.cursor.fetchone()

    if not row:

        state.add_log(f"[VEHICLE][VIN_NOT_FOUND] version lookup for vin={vin}")

        return {
            "status": "not_found"
        }

    return {

    "vin": row[0],

    "sgw_version": row[1],

    "spaak_version": row[2]

}


@router.get("/pending")
async def pending():

    return {
        "pending": list(
            state.pending_tbms.keys()
        )
    }


@router.get("/tbms")
async def tbms():

    return {
        "online_tbms": list(
            state.connected_tbms.keys()
        )
    }


@router.post("/register_tbm")
async def register_tbm(
    vin: str,
    sgw_version: str,
    spaak_version: str
):
    database.ensure_connection()
    try:
        
        database.cursor.execute(
            """
            INSERT INTO registered_tbms
            (
                vin,
                sgw_version,
                spaak_version,
                added_on
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                vin,
                sgw_version,
                spaak_version,
                datetime.now(ZoneInfo("Asia/Kolkata"))
            )
        )

        database.conn.commit()

        state.add_log(
            f"[VEHICLE][REGISTERED] vin={vin} sgw={sgw_version} spaak={spaak_version}"
        )

        return {
            "status": "success"
        }

    except psycopg2.IntegrityError:

        database.conn.rollback()

        state.add_log(f"[VEHICLE][ALREADY_EXISTS] vin={vin} registration skipped")

        return {
            "status": "already_exists"
        }


@router.get("/registered_tbms")
async def get_registered_tbms():
    database.ensure_connection()
    database.cursor.execute("""
    SELECT

        vin,

        sgw_version,

        spaak_version,

        added_on

    FROM registered_tbms

    ORDER BY added_on DESC

    """)

    rows = database.cursor.fetchall()

    result = []

    for row in rows:

        result.append({

            "vin": row[0],

            "sgw_version": row[1],

            "spaak_version": row[2],

            "added_on": row[3]

        })

    return result


@router.post("/approve/{vin}")
async def approve(vin: str):

    if vin not in state.pending_tbms:

        state.add_log(f"[APPROVAL][NOT_PENDING] approve requested for vin={vin}")

        return {
            "status": "not_found"
        }

    ws = state.pending_tbms[vin]
    
    try:
        database.ensure_connection()
        database.cursor.execute(
            """
            INSERT INTO registered_tbms
            (
                vin,
                sgw_version,
                spaak_version,
                added_on
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (vin)
            DO NOTHING
            """,
            (
                vin,
                "1.0",
                "1.0",
                datetime.now(ZoneInfo("Asia/Kolkata"))
            )
        )

        database.conn.commit()

    except Exception as e:

        state.add_log(
            f"[APPROVAL][DB_ERROR] vin={vin} {type(e).__name__}: {e}"
        )

        return {
            "status": "db_error",
            "message": str(e)
        }

    del state.pending_tbms[vin]

    state.connected_tbms[vin] = ws

    await ws.send_text(
        json.dumps({
            "type": "approved"
        })
    )

    state.add_log(
        f"[APPROVAL][APPROVED] vin={vin} approved and registered; online vehicles={len(state.connected_tbms)}"
    )

    return {
        "status": "approved"
    }


@router.post("/reject/{vin}")
async def reject(vin: str):

    if vin not in state.pending_tbms:

        state.add_log(f"[APPROVAL][NOT_PENDING] reject requested for vin={vin}")

        return {
            "status": "not_found"
        }

    ws = state.pending_tbms[vin]

    await ws.send_text(
        json.dumps({
            "type": "rejected"
        })
    )

    await ws.close()

    del state.pending_tbms[vin]

    state.add_log(
        f"[APPROVAL][REJECTED] vin={vin}"
    )

    return {
        "status": "rejected"
    }
