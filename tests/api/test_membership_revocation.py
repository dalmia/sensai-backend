"""
Removal is a soft delete, so every membership predicate must exclude rows with
deleted_at set. Without this, removing a member revokes nothing: the UI shows
them gone while the backend still grants everything they had.
"""

from unittest.mock import AsyncMock, patch

import pytest

from api.utils import authorization

pytestmark = pytest.mark.real_permissions

CALLER = 42
TARGET = 100
ORG_ID = 7
COHORT_ID = 12
COURSE_ID = 5
TASK_ID = 3


def _capture_sql():
    """Record the SQL each predicate issues, returning no rows."""
    calls = []

    async def fake(query, params=None, **kwargs):
        calls.append(" ".join(query.split()))
        return None

    return calls, fake


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "coroutine",
    [
        lambda: authorization.get_org_role(CALLER, ORG_ID),
        lambda: authorization.is_cohort_member(CALLER, COHORT_ID),
        lambda: authorization.is_staff_over_user(CALLER, TARGET),
        lambda: authorization.is_mentor_over_user(CALLER, TARGET),
    ],
)
async def test_membership_predicates_exclude_removed_rows(coroutine):
    calls, fake = _capture_sql()
    with patch.object(authorization, "execute_db_operation", fake):
        await coroutine()

    assert calls, "predicate issued no query"
    for sql in calls:
        assert "deleted_at IS NULL" in sql, f"membership query does not exclude removed rows: {sql}"


@pytest.mark.asyncio
async def test_course_access_excludes_removed_enrolment():
    """
    can_access_course short-circuits when the org lookup returns nothing, so
    org_for_course must be stubbed or the enrolment join is never reached and
    this test asserts nothing at all.
    """
    calls, fake = _capture_sql()
    with patch.object(authorization, "execute_db_operation", fake), \
         patch.object(authorization, "org_for_course", AsyncMock(return_value=ORG_ID)), \
         patch.object(authorization, "is_org_staff", AsyncMock(return_value=False)):
        await authorization.can_access_course(CALLER, COURSE_ID)

    joins = [sql for sql in calls if "user_cohorts" in sql]
    assert joins, "enrolment join was never reached - the test would pass vacuously"
    for sql in joins:
        assert "deleted_at IS NULL" in sql


@pytest.mark.asyncio
async def test_task_access_excludes_removed_enrolment():
    calls, fake = _capture_sql()
    with patch.object(authorization, "execute_db_operation", fake), \
         patch.object(authorization, "org_for_task", AsyncMock(return_value=ORG_ID)), \
         patch.object(authorization, "is_org_staff", AsyncMock(return_value=False)):
        await authorization.can_access_task(CALLER, TASK_ID)

    joins = [sql for sql in calls if "user_cohorts" in sql]
    assert joins, "enrolment join was never reached - the test would pass vacuously"
    for sql in joins:
        assert "deleted_at IS NULL" in sql
        assert "course_cohorts" in sql


@pytest.mark.asyncio
async def test_a_removed_admin_is_no_longer_staff():
    """The offboarding case: the row still exists, but deleted_at is set."""
    async def no_matching_row(query, params=None, **kwargs):
        assert "deleted_at IS NULL" in " ".join(query.split())
        return None  # the soft-deleted row is filtered out by the query

    with patch.object(authorization, "execute_db_operation", no_matching_row):
        assert await authorization.is_org_staff(CALLER, ORG_ID) is False
