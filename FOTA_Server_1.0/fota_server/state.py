pending_auth = {}
pending_tbms = {}
connected_tbms = {}
logs = []


def add_log(msg):

    print(msg)

    logs.append(msg)

    if len(logs) > 500:
        logs.pop(0)
