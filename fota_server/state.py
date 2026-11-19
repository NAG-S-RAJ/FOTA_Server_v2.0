from datetime import datetime

pending_auth = {}
pending_tbms = {}
connected_tbms = {}
logs = []
MAX_LOGS = 2000


def add_log(msg):

    msg = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}"

    print(msg)

    logs.append(msg)

    if len(logs) > MAX_LOGS:
        logs.pop(0)
