<<<<<<< HEAD
# FOTA_SERVER
=======
# FOTA Server

## Code Layout

The launch file remains `server.py`; application code lives in `fota_server/`.

| File | Responsibility |
| --- | --- |
| `server.py` | Exposes `app` for Uvicorn and supports `python server.py` |
| `fota_server/application.py` | App factory, CORS, router registration, startup/shutdown, basic/log APIs |
| `fota_server/config.py` | Environment configuration, port, authentication constant, upload location |
| `fota_server/database.py` | PostgreSQL connection, reconnect logic, schema initialization and migrations |
| `fota_server/state.py` | Shared connected/pending devices, authentication challenges and activity logs |
| `fota_server/campaigns.py` | Campaign creation, execution, stopping, deletion, listing and timeout watchdog |
| `fota_server/campaign_status.py` | Pure campaign-state, progress and timeout rules |
| `fota_server/vehicles.py` | Vehicle registration, listing, versions, approval, rejection and deletion |
| `fota_server/firmware.py` | GitHub firmware repository, latest releases, history and downloads |
| `fota_server/storage.py` | Local binary upload/download and SHA-256 calculation |
| `fota_server/websocket.py` | Device authentication, heartbeats, progress and completion messages |

Database connections are created during application startup, not during imports.
Route modules access the database module directly so reconnections cannot leave
them holding an obsolete connection or cursor. Device registries and logs are
shared through the state module.

The TBM client scripts and their `campaign_control.py` helper remain separate
from the server package and keep their existing launch commands.

## Run and Build

From `FOTA_Server_v2.0`, install dependencies:

```console
python -m pip install -r requirements.txt
```

Set `DATABASE_URL` in the environment, then launch using the existing command:

```console
python server.py
```

The server listens on `0.0.0.0` and uses `PORT`, defaulting to `8000`. The existing
ASGI import target remains `server:app`. For local development with reload:

```console
python -m uvicorn server:app --host 127.0.0.1 --port 8000 --reload
```

When deploying with Uvicorn directly, pass the hosting platform's port using
`--port`. Use a single Uvicorn worker because WebSocket connections and pending
approvals are process-local. Multiple workers require shared connection routing
and coordination, which this demo does not implement.

Configuration is centralized in `fota_server/config.py`:

| Environment Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection URL, required at startup |
| `PORT` | Port used by `python server.py` |
| `PUBLIC_URL` | Public server origin used in device firmware download URLs |
| `GITHUB_TOKEN`, `GITHUB_OWNER`, `GITHUB_REPO` | Firmware repository access |
| `GITHUB_BRANCH` | Firmware repository branch, defaults to `main` |
| `CAMPAIGN_ACK_TIMEOUT_SECONDS` | Acknowledgement-stage deadline |
| `CAMPAIGN_RUNNING_TIMEOUT_SECONDS` | Running/progress stall deadline |

Local firmware binaries are stored in the `uploads/` directory next to
`server.py`, independently of the shell's working directory. Startup applies the
existing idempotent migrations; the database user needs schema-alter permissions.

Check syntax and run the test suite without a live database or vehicle:

```console
python -m compileall -q server.py fota_server
python test_campaigns.py -q
```

The tests cover campaign behavior plus actual FastAPI route registration, HTTP
requests, WebSocket authentication, shared reconnect state and startup/shutdown.
They use isolated database/network fixtures; they do not contact PostgreSQL,
GitHub or physical devices.

## Campaign Timeouts

The server starts a campaign watchdog with the FastAPI application and checks
active campaigns every 5 seconds. The dashboard does not need to be open.

| Error Code | Condition | Default |
| --- | --- | --- |
| `HEARTBEAT_TIMEOUT` | No authenticated vehicle heartbeat | 600 seconds |
| `ACK_TIMEOUT` | Campaign remains sent or acknowledged without advancing | 600 seconds per stage |
| `RUNNING_TIMEOUT` | Running/downloading deployment makes no forward progress | 600 seconds |

Acknowledgement and running deadlines can be configured with the positive-integer
environment variables `CAMPAIGN_ACK_TIMEOUT_SECONDS` and
`CAMPAIGN_RUNNING_TIMEOUT_SECONDS`. The heartbeat deadline is fixed at 10 minutes.

Timers start at execution, not creation. Heartbeats update only the heartbeat
timer. Repeated acknowledgements, repeated running notifications, and unchanged
or decreasing download percentages do not extend the progress deadline.

On timeout, the server persists `status=failed`, `error_code`, `error_message`,
and `failed_at`, then attempts to send a `stop_campaign` message to the vehicle.
Campaign History displays the failure code; explanations remain available through
the API and server logs. Late device messages
cannot reopen failed campaigns or overwrite their recorded failure. Pending and
already finished campaigns are excluded from timeout checks.

Timing is stored in the database and survives server restarts. Active campaigns
without existing timing data receive an initial grace period on their first
watchdog scan. Existing timestamps are not reset by restarting the server.

Deploy the updated server to apply the additional database columns. The database
user must have permission to alter the campaigns table. Devices must run the
updated clients that process `stop_campaign`. An offline device cannot be forcibly
interrupted; firmware already transferred or installed cannot be rolled back by
this stop request.

Validate the PostgreSQL migration and live vehicle behavior in staging before
production deployment.
>>>>>>> 9dab4e6 (New server)
