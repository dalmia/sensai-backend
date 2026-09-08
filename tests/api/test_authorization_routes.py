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


MENTOR_ID = 200


def mentor_client():
    token = create_access_token(MENTOR_ID, "mentor@example.com")
    return TestClient(app, headers={"Authorization": f"Bearer {token}"})


class TestMentorAccess:
    """
    Every other route test uses an admin or a learner, which is how two mentor
    breakages got through: the school header and the learner-view page.
    """

    def test_mentor_can_read_a_learner_in_their_cohort(self):
        with patch.object(permissions, "is_staff_over_user", AsyncMock(return_value=False)), \
             patch.object(permissions, "is_mentor_over_user", AsyncMock(return_value=True)), \
             patch("api.routes.user.get_user_by_id", AsyncMock(return_value={"id": 100})):
            response = mentor_client().get("/users/100")

        assert response.status_code != 403

    def test_a_stranger_still_cannot_read_that_learner(self):
        with patch.object(permissions, "is_staff_over_user", AsyncMock(return_value=False)), \
             patch.object(permissions, "is_mentor_over_user", AsyncMock(return_value=False)):
            response = mentor_client().get("/users/100")

        assert response.status_code == 403

    def test_mentor_can_read_the_org_header(self):
        # get_org_by_id exposes only id/slug/name/logo_color, the same fields the
        # already-open slug route returns, so org-staff-only broke mentors for nothing.
        with patch("api.routes.org.get_org_by_id_from_db",
                   AsyncMock(return_value={"id": ORG_ID, "slug": "acme", "name": "Acme"})):
            response = mentor_client().get(f"/organizations/{ORG_ID}")

        assert response.status_code != 403


class TestSelfJoinRequiresAnInvite:
    def test_omitting_org_slug_is_rejected(self):
        """
        H5: org_slug was optional, so omitting it skipped the only check binding
        the caller to the cohort and fell through to allow.
        """
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json={"emails": ["learner@example.com"], "roles": ["learner"]},
            )

        assert response.status_code == 403

    def test_duplicated_roles_do_not_slip_through(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "is_org_staff", AsyncMock(return_value=False)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json={
                    "emails": ["learner@example.com"],
                    "roles": ["learner", "learner"],
                    "org_slug": "acme",
                },
            )

        assert response.status_code == 403

    def test_whitespace_around_the_email_still_joins(self):
        with patch.object(permissions, "org_for_cohort", AsyncMock(return_value=ORG_ID)), \
             patch.object(permissions, "org_id_for_slug", AsyncMock(return_value=ORG_ID)), \
             patch("api.routes.cohort.add_members_to_cohort_in_db", AsyncMock(return_value=None)):
            response = learner_client().post(
                f"/cohorts/{COHORT_ID}/members",
                json={
                    "emails": ["  Learner@Example.com "],
                    "roles": ["learner"],
                    "org_slug": "acme",
                },
            )

        assert response.status_code == 200
