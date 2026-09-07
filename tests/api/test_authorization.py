from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from api.middleware import permissions

# The autouse fixture in conftest stubs these out for route tests, so hold the
# real implementations from import time - these tests exercise the real rules.
REAL = {
    name: getattr(permissions, name)
    for name in dir(permissions)
    if name.startswith("require_")
}


def make_request(user_id=None):
    return SimpleNamespace(
        state=SimpleNamespace(user_id=user_id),
        method="GET",
        url=SimpleNamespace(path="/test"),
    )


class TestUserScope:
    @pytest.mark.asyncio
    async def test_user_may_act_on_self(self):
        await REAL["require_user_scope"](make_request(7), 7)

    @pytest.mark.asyncio
    async def test_user_may_not_act_on_another_user(self):
        with patch.object(permissions, "is_staff_over_user", AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc:
                await REAL["require_user_scope"](make_request(7), 8)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_staff_may_act_on_their_member(self):
        with patch.object(permissions, "is_staff_over_user", AsyncMock(return_value=True)):
            await REAL["require_user_scope"](make_request(7), 8)


class TestOrgStaff:
    @pytest.mark.asyncio
    async def test_non_staff_is_blocked(self):
        with patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc:
                await REAL["require_org_staff"](make_request(7), 3)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_staff_is_allowed(self):
        with patch.object(permissions, "is_org_staff", AsyncMock(return_value=True)):
            await REAL["require_org_staff"](make_request(7), 3)


class TestResourceAccess:
    @pytest.mark.asyncio
    async def test_course_outsider_is_blocked(self):
        with patch.object(permissions, "can_access_course", AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc:
                await REAL["require_course_access"](make_request(7), 42)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_task_outsider_is_blocked(self):
        with patch.object(permissions, "can_access_task", AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc:
                await REAL["require_task_access"](make_request(7), 42)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_cohort_member_is_allowed(self):
        with patch.object(permissions, "can_access_cohort", AsyncMock(return_value=True)):
            await REAL["require_cohort_access"](make_request(7), 42)

    @pytest.mark.asyncio
    async def test_batch_without_cohort_is_blocked(self):
        with patch.object(permissions, "cohort_for_batch", AsyncMock(return_value=None)):
            with pytest.raises(HTTPException) as exc:
                await REAL["require_batch_access"](make_request(7), 42)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_scorecard_from_another_org_is_blocked(self):
        with patch.object(permissions, "org_for_scorecard", AsyncMock(return_value=3)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc:
                await REAL["require_scorecard_access"](make_request(7), 42)
        assert exc.value.status_code == 403


class TestUnauthenticated:
    @pytest.mark.asyncio
    async def test_unauthenticated_is_rejected(self):
        with pytest.raises(HTTPException) as exc:
            await REAL["require_user_scope"](make_request(None), 8)
        assert exc.value.status_code == 401


class TestViolationIsLogged:
    @pytest.mark.asyncio
    async def test_block_is_logged(self):
        with patch.object(permissions, "is_staff_over_user", AsyncMock(return_value=False)), \
             patch.object(permissions.logger, "warning") as mock_warning:
            with pytest.raises(HTTPException):
                await REAL["require_user_scope"](make_request(7), 8)

        assert mock_warning.called
        assert "AUTHZ blocked" in mock_warning.call_args[0][0]
