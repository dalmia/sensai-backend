"""
Route-level authorization tests with the permission stubs OFF.

These cover the two findings the dependency-level tests missed: an enrolled
learner reaching mutations (H1), and a not-yet-member being blocked from
self-join (H2).
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.middleware import permissions
from api.utils.tokens import create_access_token

pytestmark = pytest.mark.real_permissions

LEARNER_ID = 99
ORG_ID = 7
COURSE_ID = 5
COHORT_ID = 12


def learner_client(email="learner@example.com"):
    token = create_access_token(LEARNER_ID, email)
    return TestClient(app, headers={"Authorization": f"Bearer {token}"})


class TestLearnerCannotMutate:
    """H1: read predicates must never guard a write."""

    def test_enrolled_learner_cannot_delete_a_course(self):
        with patch.object(permissions, "org_for_course", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().delete(f"/courses/{COURSE_ID}")

        assert response.status_code == 403

    def test_enrolled_learner_cannot_delete_a_cohort(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().delete(f"/cohorts/{COHORT_ID}")

        assert response.status_code == 403

    def test_enrolled_learner_cannot_delete_a_task(self):
        with patch.object(permissions, "org_for_task", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().delete("/tasks/3")

        assert response.status_code == 403

    def test_org_staff_is_not_blocked(self):
        with patch.object(permissions, "org_for_course", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=True)), \
             patch("api.routes.course.delete_course_in_db", AsyncMock(return_value=None)):
            response = learner_client().delete(f"/courses/{COURSE_ID}")

        assert response.status_code != 403


class TestCohortSelfJoin:
    """H2: an invited learner is not yet a member and not staff."""

    def _payload(self, emails, roles=None):
        return {"emails": emails, "roles": roles or ["learner"], "org_slug": "acme"}

    def test_a_new_learner_can_enrol_themselves(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "org_id_for_slug", AsyncMock(return_value=ORG_ID)), \
             patch("api.routes.cohort.add_members_to_cohort_in_db", AsyncMock(return_value=None)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json=self._payload(["learner@example.com"]),
            )

        assert response.status_code == 200

    def test_a_learner_cannot_add_somebody_else(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json=self._payload(["someone.else@example.com"]),
            )

        assert response.status_code == 403

    def test_a_learner_cannot_enrol_themselves_as_mentor(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json=self._payload(["learner@example.com"], roles=["mentor"]),
            )

        assert response.status_code == 403

    def test_self_join_into_another_orgs_cohort_is_blocked(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "org_id_for_slug", AsyncMock(return_value=999)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json=self._payload(["learner@example.com"]),
            )

        assert response.status_code == 403
