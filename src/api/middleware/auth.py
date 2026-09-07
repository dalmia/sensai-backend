from typing import Optional

from fastapi.responses import JSONResponse

from api.settings import settings
from api.utils.logging import logger
from api.utils.tokens import TokenError, decode_access_token

PUBLIC_PATHS = frozenset(
    {
        "/health",
        "/auth/login",
    }
)

PUBLIC_PREFIXES = ()


def is_public(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES)


def _unauthorized(detail: str) -> JSONResponse:
    return JSONResponse(status_code=401, content={"detail": detail})


def _extract_bearer(header: Optional[str]) -> Optional[str]:
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


async def auth_middleware(request, call_next):
    if request.method == "OPTIONS" or is_public(request.url.path):
        return await call_next(request)

    if not settings.auth_secret_key:
        logger.error("AUTH_SECRET_KEY is not set - rejecting request")
        return JSONResponse(
            status_code=500, content={"detail": "Server authentication misconfigured"}
        )

    token = _extract_bearer(request.headers.get("authorization"))
    if not token:
        return _unauthorized("Missing bearer token")

    try:
        payload = decode_access_token(token)
    except TokenError as exc:
        return _unauthorized(str(exc))

    request.state.user_id = payload["user_id"]
    request.state.user_email = payload.get("email")

    return await call_next(request)
