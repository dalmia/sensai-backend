from typing import Callable, List

from fastapi import Depends, HTTPException, Request

from api.auth.constants import ROLE_HIERARCHY
from api.auth.dependencies import get_current_user
from api.auth.models import AuthenticatedUser
from api.utils.db import execute_db_operation
from api.config import user_organizations_table_name, user_cohorts_table_name


def _role_satisfies(user_role: str, allowed_roles: List[str]) -> bool:
    """Check if user_role meets or exceeds any of the allowed_roles."""
    user_level = ROLE_HIERARCHY.get(user_role, 0)
    return any(user_level >= ROLE_HIERARCHY.get(r, 0) for r in allowed_roles)


def require_org_role(allowed_roles: List[str], org_id_param: str = "org_id") -> Callable:
    """FastAPI dependency factory: checks the user has one of the
    allowed roles (or higher) in the specified organization.

    The org_id is extracted from path parameters by name.

    Usage:
        @router.post("/organizations/{org_id}/members")
        async def add_members(
            org_id: int,
            current_user: AuthenticatedUser = Depends(get_current_user),
            _: None = Depends(require_org_role(["owner", "admin"])),
        ):
    """

    async def _check(
        request: Request,
        current_user: AuthenticatedUser = Depends(get_current_user),
    ) -> None:
        org_id = request.path_params.get(org_id_param)
        if org_id is None:
            raise HTTPException(status_code=400, detail=f"Missing path parameter: {org_id_param}")

        row = await execute_db_operation(
            f"SELECT role FROM {user_organizations_table_name} "
            "WHERE user_id = ? AND org_id = ? AND deleted_at IS NULL",
            (current_user.id, int(org_id)),
            fetch_one=True,
        )

        if not row or not _role_satisfies(row[0], allowed_roles):
            raise HTTPException(status_code=403, detail="Insufficient permissions for this organization")

    return _check


def require_cohort_role(
    allowed_roles: List[str], cohort_id_param: str = "cohort_id"
) -> Callable:
    """FastAPI dependency factory: checks the user has one of the
    allowed roles (or higher) in the specified cohort.

    Usage:
        @router.get("/cohorts/{cohort_id}/leaderboard")
        async def leaderboard(
            cohort_id: int,
            current_user: AuthenticatedUser = Depends(get_current_user),
            _: None = Depends(require_cohort_role(["learner"])),
        ):
    """

    async def _check(
        request: Request,
        current_user: AuthenticatedUser = Depends(get_current_user),
    ) -> None:
        cohort_id = request.path_params.get(cohort_id_param)
        if cohort_id is None:
            raise HTTPException(
                status_code=400, detail=f"Missing path parameter: {cohort_id_param}"
            )

        row = await execute_db_operation(
            f"SELECT role FROM {user_cohorts_table_name} "
            "WHERE user_id = ? AND cohort_id = ? AND deleted_at IS NULL",
            (current_user.id, int(cohort_id)),
            fetch_one=True,
        )

        if not row or not _role_satisfies(row[0], allowed_roles):
            raise HTTPException(
                status_code=403, detail="Insufficient permissions for this cohort"
            )

    return _check


def require_self_or_org_admin(user_id_param: str = "user_id") -> Callable:
    """FastAPI dependency factory: the caller must either be accessing
    their own data OR be an org admin/owner.

    For endpoints like GET /users/{user_id}/courses where an admin
    might view another user's data.
    """

    async def _check(
        request: Request,
        current_user: AuthenticatedUser = Depends(get_current_user),
    ) -> None:
        target_user_id = request.path_params.get(user_id_param)
        if target_user_id is None:
            raise HTTPException(
                status_code=400, detail=f"Missing path parameter: {user_id_param}"
            )

        # Self-access is always allowed
        if int(target_user_id) == current_user.id:
            return

        # Check if caller is an admin/owner of any org
        row = await execute_db_operation(
            f"SELECT role FROM {user_organizations_table_name} "
            "WHERE user_id = ? AND role IN ('owner', 'admin') AND deleted_at IS NULL "
            "LIMIT 1",
            (current_user.id,),
            fetch_one=True,
        )

        if not row:
            raise HTTPException(
                status_code=403,
                detail="You can only access your own data unless you are an admin",
            )

    return _check
