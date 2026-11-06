import base64
import hashlib
import hmac
import json
import secrets
import time

from fastapi import APIRouter, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from . import config as settings, database, state

router = APIRouter(tags=["Authentication"])
TOKEN_LIFETIME_SECONDS = 8 * 60 * 60
PASSWORD_HASH_ITERATIONS = 310_000
ROLE_LEVELS = {"viewer": 0, "operator": 1, "admin": 2}


def _encode(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def hash_password(password, salt=None):
    salt_bytes = salt.encode("utf-8") if isinstance(salt, str) else salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_bytes, PASSWORD_HASH_ITERATIONS)
    return f"pbkdf2_sha256${PASSWORD_HASH_ITERATIONS}${_encode(salt_bytes)}${digest.hex()}"


def verify_password(password, encoded_hash):
    try:
        algorithm, iterations, encoded_salt, expected = encoded_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(encoded_salt + "=" * (-len(encoded_salt) % 4))
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations)).hex()
        return hmac.compare_digest(actual, expected)
    except (AttributeError, TypeError, ValueError):
        return False


def issue_token(username, role, now=None):
    if not settings.SESSION_SECRET:
        raise ValueError("FOTA_SESSION_SECRET is not configured")
    payload = json.dumps({
        "sub": username,
        "role": role,
        "exp": int((time.time() if now is None else now) + TOKEN_LIFETIME_SECONDS)
    }, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded_payload = _encode(payload)
    signature = hmac.new(settings.SESSION_SECRET.encode("utf-8"), encoded_payload.encode("ascii"), hashlib.sha256).digest()
    return encoded_payload + "." + _encode(signature)


def verify_token(token, now=None):
    if not settings.SESSION_SECRET or not isinstance(token, str):
        return None
    try:
        encoded_payload, encoded_signature = token.split(".", 1)
        signature = base64.urlsafe_b64decode(encoded_signature + "=" * (-len(encoded_signature) % 4))
        expected = hmac.new(settings.SESSION_SECRET.encode("utf-8"), encoded_payload.encode("ascii"), hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            return None
        payload_bytes = base64.urlsafe_b64decode(encoded_payload + "=" * (-len(encoded_payload) % 4))
        payload = json.loads(payload_bytes)
        username = payload["sub"]
        role = payload["role"]
        user = settings.OPERATOR_USERS.get(username)
        if not isinstance(user, dict) or user.get("role") != role or role not in ROLE_LEVELS:
            return None
        if int(payload["exp"]) <= (time.time() if now is None else now):
            return None
        return {"username": username, "role": role, "expires_at": int(payload["exp"])}
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, UnicodeDecodeError):
        return None


@router.post("/auth/login")
async def login(credentials: dict):
    username = credentials.get("username")
    password = credentials.get("password")
    if not settings.SESSION_SECRET or not settings.OPERATOR_USERS:
        raise HTTPException(status_code=503, detail="Operator authentication is not configured")
    user = settings.OPERATOR_USERS.get(username) if isinstance(username, str) else None
    password_hash = user.get("password_hash") if isinstance(user, dict) else None
    role = user.get("role") if isinstance(user, dict) else None
    if not isinstance(password, str) or not verify_password(password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if role not in ROLE_LEVELS:
        raise HTTPException(status_code=403, detail="Account role is not permitted")
    token = issue_token(username, role)
    return {"access_token": token, "token_type": "bearer", "role": role,
            "expires_in": TOKEN_LIFETIME_SECONDS}


def _required_role(method, path):
    if path in ("/logs", "/download_logs", "/github_test"):
        return "admin"
    admin_paths = (
        path == "/register_tbm" or path.startswith("/approve/") or
        path.startswith("/reject/") or path.startswith("/registered_tbm/") and method == "DELETE" or
        path.startswith("/registered_tbm/") and method == "PUT" or
        path.startswith("/upload") or path == "/upload_firmware" or
        path.startswith("/campaign/") and method == "DELETE"
    )
    if admin_paths:
        return "admin"
    if method in ("POST", "PUT", "PATCH", "DELETE"):
        return "operator"
    return "viewer"


class OperatorAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if (request.method == "OPTIONS" or request.url.path in ("/", "/auth/login") or
            request.method == "GET" and request.url.path.startswith("/files/")):
            return await call_next(request)
        authorization = request.headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        session = verify_token(token) if scheme.lower() == "bearer" else None
        if session is None:
            return JSONResponse({"detail": "Authentication required"}, status_code=401)
        required_role = _required_role(request.method, request.url.path)
        if ROLE_LEVELS[session["role"]] < ROLE_LEVELS[required_role]:
            return JSONResponse({"detail": "Insufficient permissions"}, status_code=403)
        request.state.operator = session
        response = await call_next(request)
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            try:
                target = (request.query_params.get("campaign_id") or
                          request.query_params.get("vin") or
                          request.url.path.rstrip("/").rsplit("/", 1)[-1])
                database.ensure_connection()
                database.cursor.execute(
                    """
                    INSERT INTO audit_events (actor, role, action, target, http_status)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (session["username"], session["role"], request.method,
                     target[:200], response.status_code)
                )
                database.conn.commit()
            except Exception as error:
                state.add_log(f"Audit write failed for {request.method} {request.url.path}: {error}")
        return response