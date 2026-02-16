from typing import Optional

from fastapi import Header, HTTPException, Query

from api.auth.jwt import decode_access_token
from api.auth.models import AuthenticatedUser
from api.db.user import get_user_by_id
from api.utils.logging import logger


async def _load_user(user_id: int) -> AuthenticatedUser:
    """Load a user from the database and return an AuthenticatedUser."""
    user = await get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return AuthenticatedUser(
        id=user["id"],
        email=user["email"],
        first_name=user.get("first_name"),
        last_name=user.get("last_name"),
    )


async def get_current_user(
    authorization: str = Header(..., alias="Authorization"),
) -> AuthenticatedUser:
    """Strict JWT-only authentication dependency.

    Extracts the Bearer token from the Authorization header,
    decodes it, and returns the authenticated user.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[7:]  # Strip "Bearer "
    payload = decode_access_token(token)
    user_id = int(payload["sub"])
    return await _load_user(user_id)


async def get_current_user_transitional(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    user_id: Optional[int] = Query(None),
) -> AuthenticatedUser:
    """Migration-period dependency that accepts JWT OR legacy user_id.

    Prefers JWT when present. Falls back to user_id query param
    with a deprecation warning. Raises 401 if neither is provided.

    This dependency will be removed once all frontend calls send JWTs.
    """
    # Path 1: JWT authentication (preferred)
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        payload = decode_access_token(token)
        uid = int(payload["sub"])
        return await _load_user(uid)

    # Path 2: Legacy user_id param (deprecated)
    if user_id is not None:
        logger.warning(
            "DEPRECATED: Request authenticated via user_id query param "
            f"(user_id={user_id}). Migrate to JWT Bearer token."
        )
        return await _load_user(user_id)

    raise HTTPException(
        status_code=401,
        detail="Authentication required. Provide Authorization: Bearer <token> header.",
    )
