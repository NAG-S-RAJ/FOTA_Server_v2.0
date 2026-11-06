import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE_URL = os.getenv("DATABASE_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://fota-demo-v2-0.onrender.com")
AUTH_SECRET = "FOTA_DEMO_SECRET"
UPLOAD_FOLDER = str(BASE_DIR / "uploads")
CAMPAIGN_ACK_TIMEOUT_SECONDS = max(1, int(os.getenv("CAMPAIGN_ACK_TIMEOUT_SECONDS", "600")))
CAMPAIGN_RUNNING_TIMEOUT_SECONDS = max(1, int(os.getenv("CAMPAIGN_RUNNING_TIMEOUT_SECONDS", "600")))
CAMPAIGN_WATCHDOG_INTERVAL_SECONDS = 5
PORT = int(os.getenv("PORT", "8000"))
