from datetime import datetime, timezone, timedelta

import jwt
from fastapi import HTTPException

from api.auth.constants import ACCESS_TOKEN_EXPIRE_MINUTES, JWT_ALGORITHM
from api.auth.models import TokenResponse
from api.settings import get_settings


def _get_secret() -> str:
    secret = get_settings().jwt_secret
    if not secret:
        raise HTTPException(status_code=500, detail="JWT secret not configured")
    return secret


def create_access_token(user_id: int, email: str) -> TokenResponse:
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    payload = {
        "sub": str(user_id),
        "email": email,
        "type": "access",
        "iat": now,
        "exp": expires,
    }

    token = jwt.encode(payload, _get_secret(), algorithm=JWT_ALGORITHM)

    return TokenResponse(
        access_token=token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )


def decode_access_token(token: str) -> dict:
    """Decode and verify a JWT access token.

    Returns the payload dict on success.
    Raises HTTPException(401) on any failure.
    """
    try:
        payload = jwt.decode(token, _get_secret(), algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")

    if not payload.get("sub"):
        raise HTTPException(status_code=401, detail="Invalid token payload")

    return payload
