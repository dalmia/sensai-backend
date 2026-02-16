import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from fastapi import HTTPException

from api.auth.rbac import require_org_role, require_cohort_role, require_self_or_org_admin
from api.auth.models import AuthenticatedUser

TEST_SECRET = "test-secret-key-for-unit-tests"


def _make_request(path_params: dict) -> MagicMock:
    request = MagicMock()
    request.path_params = path_params
    return request


def _make_user(user_id: int = 1) -> AuthenticatedUser:
    return AuthenticatedUser(
        id=user_id, email="test@example.com", first_name="Test", last_name="User"
    )


class TestRequireOrgRole:
    @pytest.mark.asyncio
    async def test_owner_passes_admin_check(self):
        check = require_org_role(["admin"])
        request = _make_request({"org_id": "10"})
        user = _make_user()

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=("owner",),
        ):
            await check(request=request, current_user=user)
            # No exception = pass

    @pytest.mark.asyncio
    async def test_learner_fails_admin_check(self):
        check = require_org_role(["admin"])
        request = _make_request({"org_id": "10"})
        user = _make_user()

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=("learner",),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await check(request=request, current_user=user)
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_no_membership_fails(self):
        check = require_org_role(["admin"])
        request = _make_request({"org_id": "10"})
        user = _make_user()

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await check(request=request, current_user=user)
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_path_param_raises_400(self):
        check = require_org_role(["admin"])
        request = _make_request({})  # no org_id
        user = _make_user()

        with pytest.raises(HTTPException) as exc_info:
            await check(request=request, current_user=user)
        assert exc_info.value.status_code == 400


class TestRequireCohortRole:
    @pytest.mark.asyncio
    async def test_mentor_passes_learner_check(self):
        """Mentor is higher than learner in hierarchy."""
        check = require_cohort_role(["learner"])
        request = _make_request({"cohort_id": "5"})
        user = _make_user()

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=("mentor",),
        ):
            await check(request=request, current_user=user)

    @pytest.mark.asyncio
    async def test_learner_fails_mentor_check(self):
        check = require_cohort_role(["mentor"])
        request = _make_request({"cohort_id": "5"})
        user = _make_user()

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=("learner",),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await check(request=request, current_user=user)
            assert exc_info.value.status_code == 403


class TestRequireSelfOrOrgAdmin:
    @pytest.mark.asyncio
    async def test_self_access_allowed(self):
        check = require_self_or_org_admin()
        request = _make_request({"user_id": "1"})
        user = _make_user(user_id=1)

        # No DB call needed for self-access
        await check(request=request, current_user=user)

    @pytest.mark.asyncio
    async def test_admin_can_access_other_user(self):
        check = require_self_or_org_admin()
        request = _make_request({"user_id": "99"})
        user = _make_user(user_id=1)

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=("admin",),
        ):
            await check(request=request, current_user=user)

    @pytest.mark.asyncio
    async def test_non_admin_cannot_access_other_user(self):
        check = require_self_or_org_admin()
        request = _make_request({"user_id": "99"})
        user = _make_user(user_id=1)

        with patch(
            "api.auth.rbac.execute_db_operation",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await check(request=request, current_user=user)
            assert exc_info.value.status_code == 403
