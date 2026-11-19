from . import database, state


def record_campaign_event(campaign_id, vin, event_type, status="", detail=""):
    try:
        database.ensure_connection()
        database.cursor.execute(
            """
            INSERT INTO campaign_events (campaign_id, vin, event_type, status, detail)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (campaign_id, vin, str(event_type)[:40], str(status)[:50], str(detail)[:2000])
        )
        database.conn.commit()
        state.add_log(f"[HISTORY][{status}] campaign={campaign_id} vin={vin} {event_type}: {detail}")
    except Exception as error:
        state.add_log(f"[HISTORY][EVENT_WRITE_FAILED] campaign={campaign_id} event={event_type} "
                      f"{type(error).__name__}: {error}")