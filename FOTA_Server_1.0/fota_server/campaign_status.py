def is_active_campaign_status(status):
    status = str(status or "").strip().lower()
    return status in ("sent", "acknowledged", "running", "executing", "in_progress") or status.startswith("downloading")


def campaign_timeout_error(status, last_heartbeat_at, last_progress_at, now,
                           heartbeat_timeout=600, ack_timeout=600, running_timeout=600):
    if not is_active_campaign_status(status):
        return None
    if now - last_heartbeat_at >= heartbeat_timeout:
        return ("HEARTBEAT_TIMEOUT", f"No vehicle heartbeat received for {heartbeat_timeout} seconds. Campaign automatically stopped.")
    if now - last_progress_at >= (ack_timeout if status.lower() in ("sent", "acknowledged") else running_timeout):
        if status.lower() == "sent":
            return ("ACK_TIMEOUT", f"Vehicle did not acknowledge the campaign within {ack_timeout} seconds.")
        if status.lower() == "acknowledged":
            return ("ACK_TIMEOUT", f"Campaign remained acknowledged without starting deployment for {ack_timeout} seconds.")
        return ("RUNNING_TIMEOUT", f"Deployment did not advance for {running_timeout} seconds. Campaign automatically stopped.")
    return None


def campaign_progress_value(status):
    status = str(status).strip().lower()
    if not status.startswith("downloading"):
        return None
    try:
        progress = float(status.removeprefix("downloading").removesuffix("%").strip())
    except ValueError:
        return None
    return progress if 0 <= progress <= 100 else None