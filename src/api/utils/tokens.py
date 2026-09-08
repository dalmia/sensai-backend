import base64
import hashlib
import hmac
import json
import time
from typing import Dict

from api.settings import settings


ACCESS_TOKEN_TTL_SECONDS = 120
ACCESS_TOKEN_MAX_AGE_SECONDS = 300
CLOCK_SKEW_LEEWAY_SECONDS = 30


class TokenError(Exception):
    pass


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _secret() -> str:
    if not settings.auth_secret_key:
        raise TokenError("auth_secret_key is not configured")
    return settings.auth_secret_key


def _sign(signing_input: bytes) -> str:
    return _b64url_encode(
        hmac.new(_secret().encode(), signing_input, hashlib.sha256).digest()
    )


def create_access_token(user_id: int, email: str = "", ttl_seconds: int = None) -> str:
    now = int(time.time())
    ttl = ACCESS_TOKEN_TTL_SECONDS if ttl_seconds is None else ttl_seconds
    payload = {
        "sub": str(user_id),
        "email": email,
        "aud": "api",
        "iat": now,
        "exp": now + ttl,
    }
    segments = [
        _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode()),
        _b64url_encode(json.dumps(payload, separators=(",", ":")).encode()),
    ]
    segments.append(_sign(".".join(segments).encode()))
    return ".".join(segments)


def decode_access_token(token: str) -> Dict:
    parts = token.split(".")
    if len(parts) != 3:
        raise TokenError("Malformed token")

    header_b64, payload_b64, signature = parts

    if not hmac.compare_digest(_sign(f"{header_b64}.{payload_b64}".encode()), signature):
        raise TokenError("Invalid token signature")

    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise TokenError("Malformed token payload")

    if payload.get("aud") != "api":
        raise TokenError("Token is not valid for the API")

    now = int(time.time())

    if payload.get("exp", 0) < now:
        raise TokenError("Token has expired")

    issued_at = payload.get("iat", 0)
    if issued_at > now + CLOCK_SKEW_LEEWAY_SECONDS:
        raise TokenError("Token issued in the future")

    if now - issued_at > ACCESS_TOKEN_MAX_AGE_SECONDS:
        raise TokenError("Token too old")

    try:
        user_id = int(payload["sub"])
    except (KeyError, TypeError, ValueError):
        raise TokenError("Token has no valid subject")

    payload["user_id"] = user_id
    return payload


def decode_ws_ticket(ticket: str, course_id: int) -> Dict:
    parts = ticket.split(".")
    if len(parts) != 3:
        raise TokenError("Malformed ticket")

    header_b64, payload_b64, signature = parts

    if not hmac.compare_digest(_sign(f"{header_b64}.{payload_b64}".encode()), signature):
        raise TokenError("Invalid ticket signature")

    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise TokenError("Malformed ticket payload")

    if payload.get("aud") != "ws":
        raise TokenError("Ticket is not valid for websockets")

    now = int(time.time())

    if payload.get("exp", 0) < now:
        raise TokenError("Ticket has expired")

    issued_at = payload.get("iat", 0)
    if issued_at > now + CLOCK_SKEW_LEEWAY_SECONDS:
        raise TokenError("Ticket issued in the future")

    if payload.get("course_id") != course_id:
        raise TokenError("Ticket is not valid for this course")

    try:
        payload["user_id"] = int(payload["sub"])
    except (KeyError, TypeError, ValueError):
        raise TokenError("Ticket has no valid subject")

    return payload
