import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE_URL = os.getenv("DATABASE_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://fota-server-v2-0.onrender.com")
AUTH_SECRET = "FOTA_DEMO_SECRET"
UPLOAD_FOLDER = str(BASE_DIR / "uploads")
CAPTURE_FOLDER = str(BASE_DIR / "captures")
CAPTURE_MAX_BYTES = max(1, int(os.getenv("FOTA_CAPTURE_MAX_BYTES", str(500 * 1024 * 1024))))
CAMPAIGN_ACK_TIMEOUT_SECONDS = max(1, int(os.getenv("CAMPAIGN_ACK_TIMEOUT_SECONDS", "600")))
CAMPAIGN_RUNNING_TIMEOUT_SECONDS = max(1, int(os.getenv("CAMPAIGN_RUNNING_TIMEOUT_SECONDS", "600")))
CAMPAIGN_WATCHDOG_INTERVAL_SECONDS = 5
CAMPAIGN_RETRY_DELAY_SECONDS = max(0, int(os.getenv("CAMPAIGN_RETRY_DELAY_SECONDS", "30")))
CAMPAIGN_MAX_RETRIES = 10
PORT = int(os.getenv("PORT", "8000"))
