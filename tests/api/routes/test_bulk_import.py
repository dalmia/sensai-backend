from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.middleware import permissions
from api.utils.tokens import create_access_token

COURSE_ID = 5
MILESTONE_ID = 42


def _lm(title="Intro", **overrides):
    item = {
        "milestone_id": MILESTONE_ID,
        "type": "learning_material",
        "title": title,
        "blocks": [{"type": "paragraph", "content": [{"type": "text", "text": "hi", "styles": {}}]}],
    }
    item.update(overrides)
    return item


def _quiz(title="Check", questions=None, **overrides):
    item = {
        "milestone_id": MILESTONE_ID,
        "type": "quiz",
        "title": title,
        "questions": questions
        if questions is not None
        else [{"title": "Q1", "blocks": [{"type": "paragraph", "content": []}]}],
    }
    item.update(overrides)
    return item


class TestBulkImport:
    def test_creates_learning_materials_and_quizzes(self, client):
        created = [
            {"index": 0, "task_id": 1, "milestone_id": MILESTONE_ID, "ordering": 0},
            {"index": 1, "task_id": 2, "milestone_id": MILESTONE_ID, "ordering": 1},
        ]
        with patch(
            "api.routes.course.bulk_create_draft_tasks_in_db",
            AsyncMock(return_value=created),
        ) as mock_bulk:
            response = client.post(
                f"/courses/{COURSE_ID}/tasks/bulk",
                json={"items": [_lm(), _quiz()]},
            )

        assert response.status_code == 200
        assert response.json() == {"created": created}

        course_id, items = mock_bulk.call_args[0]
        assert course_id == COURSE_ID
        assert [item["type"] for item in items] == ["learning_material", "quiz"]

    def test_question_defaults_are_filled_in(self, client):
        """A sparse CSV row must not write NULL into is_feedback_shown (NOT NULL)."""
        with patch(
            "api.routes.course.bulk_create_draft_tasks_in_db", AsyncMock(return_value=[])
        ) as mock_bulk:
            client.post(
                f"/courses/{COURSE_ID}/tasks/bulk",
                json={"items": [_quiz(questions=[{"title": "Q1"}])]},
            )

        question = mock_bulk.call_args[0][1][0]["questions"][0]
        assert question["is_feedback_shown"] is True
        assert str(question["type"]) == "objective"
        assert str(question["input_type"]) == "text"
        assert str(question["response_type"]) == "chat"
        assert question["max_attempts"] is None

    def test_title_is_trimmed(self, client):
        with patch(
            "api.routes.course.bulk_create_draft_tasks_in_db", AsyncMock(return_value=[])
        ) as mock_bulk:
            client.post(
                f"/courses/{COURSE_ID}/tasks/bulk", json={"items": [_lm(title="  Intro  ")]}
            )

        assert mock_bulk.call_args[0][1][0]["title"] == "Intro"

    @pytest.mark.parametrize(
        "items",
        [
            [],
            [_lm(type="assignment")],
            [_lm(title="")],
            [_lm(title="   ")],
            [_lm(title="x" * 256)],
            [_quiz(questions=[{"title": ""}])],
            [_quiz(questions=[{"title": "Q", "max_attempts": 0}])],
            [_lm(milestone_id="not-an-int")],
        ],
    )
    def test_rejects_invalid_payloads(self, client, items):
        response = client.post(
            f"/courses/{COURSE_ID}/tasks/bulk", json={"items": items}
        )
        assert response.status_code == 422

    def test_rejects_too_many_tasks(self, client):
        response = client.post(
            f"/courses/{COURSE_ID}/tasks/bulk", json={"items": [_lm()] * 501}
        )
        assert response.status_code == 422

    def test_rejects_too_many_questions(self, client):
        question = {"title": "Q"}
        items = [_quiz(questions=[question] * 200) for _ in range(11)]
        response = client.post(f"/courses/{COURSE_ID}/tasks/bulk", json={"items": items})
        assert response.status_code == 422
        assert "questions" in response.json()["detail"]

    def test_nothing_is_written_when_validation_fails(self, client):
        with patch(
            "api.routes.course.bulk_create_draft_tasks_in_db", AsyncMock()
        ) as mock_bulk:
            client.post(
                f"/courses/{COURSE_ID}/tasks/bulk",
                json={"items": [_lm(), _lm(type="assignment")]},
            )

        mock_bulk.assert_not_called()


@pytest.mark.real_permissions
class TestBulkImportAuthorization:
    def staff_client(self):
        return TestClient(
            app,
            headers={"Authorization": f"Bearer {create_access_token(1, 'a@b.com')}"},
        )

    def test_non_staff_cannot_import(self):
        with patch.object(permissions, "org_for_course", AsyncMock(return_value=7)), patch.object(
            permissions, "is_org_staff", AsyncMock(return_value=False)
        ):
            response = self.staff_client().post(
                f"/courses/{COURSE_ID}/tasks/bulk", json={"items": [_lm()]}
            )

        assert response.status_code == 403

    def test_milestone_outside_the_course_is_rejected(self):
        """The milestone id is in the body, so the course dependency does not cover it."""
        with patch.object(permissions, "org_for_course", AsyncMock(return_value=7)), patch.object(
            permissions, "is_org_staff", AsyncMock(return_value=True)
        ), patch.object(
            permissions, "milestones_in_course", AsyncMock(return_value=set())
        ), patch(
            "api.routes.course.bulk_create_draft_tasks_in_db", AsyncMock()
        ) as mock_bulk:
            response = self.staff_client().post(
                f"/courses/{COURSE_ID}/tasks/bulk", json={"items": [_lm()]}
            )

        assert response.status_code == 409
        mock_bulk.assert_not_called()

    def test_milestone_inside_the_course_is_allowed(self):
        with patch.object(permissions, "org_for_course", AsyncMock(return_value=7)), patch.object(
            permissions, "is_org_staff", AsyncMock(return_value=True)
        ), patch.object(
            permissions, "milestones_in_course", AsyncMock(return_value={MILESTONE_ID})
        ), patch(
            "api.routes.course.bulk_create_draft_tasks_in_db", AsyncMock(return_value=[])
        ):
            response = self.staff_client().post(
                f"/courses/{COURSE_ID}/tasks/bulk", json={"items": [_lm()]}
            )

        assert response.status_code == 200


class TestBulkCreateDraftTasksDb:
    """Ordering is resolved once per milestone and then advanced in memory."""

    def _conn(self, existing_max):
        cursor = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=list(existing_max.items()))
        cursor.lastrowid = 101
        conn = AsyncMock()
        conn.cursor = AsyncMock(return_value=cursor)
        conn.__aenter__ = AsyncMock(return_value=conn)
        conn.__aexit__ = AsyncMock(return_value=None)
        return conn, cursor

    async def _run(self, items, existing_max):
        from api.db import task as task_db

        conn, cursor = self._conn(existing_max)
        with patch.object(
            task_db, "get_new_db_connection", return_value=conn
        ), patch.object(task_db, "get_org_id_for_course", AsyncMock(return_value=3)):
            created = await task_db.bulk_create_draft_tasks(COURSE_ID, items)
        return created, cursor

    @pytest.mark.asyncio
    async def test_appends_after_existing_tasks_per_milestone(self):
        items = [
            {"milestone_id": 1, "type": "quiz", "title": "a", "questions": []},
            {"milestone_id": 2, "type": "quiz", "title": "b", "questions": []},
            {"milestone_id": 1, "type": "quiz", "title": "c", "questions": []},
        ]
        created, _ = await self._run(items, {1: 4, 2: -1})

        assert [c["ordering"] for c in created] == [5, 0, 6]
        assert [c["index"] for c in created] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_milestone_with_no_tasks_starts_at_zero(self):
        items = [{"milestone_id": 9, "type": "quiz", "title": "a", "questions": []}]
        created, _ = await self._run(items, {})
        assert created[0]["ordering"] == 0

    @pytest.mark.asyncio
    async def test_ordering_is_read_once_regardless_of_row_count(self):
        items = [
            {"milestone_id": 1, "type": "quiz", "title": str(i), "questions": []}
            for i in range(50)
        ]
        _, cursor = await self._run(items, {1: 0})

        selects = [
            call for call in cursor.execute.call_args_list if "MAX(ordering)" in call[0][0]
        ]
        assert len(selects) == 1

    @pytest.mark.asyncio
    async def test_empty_list_does_not_open_a_connection(self):
        from api.db import task as task_db

        with patch.object(task_db, "get_new_db_connection") as conn:
            assert await task_db.bulk_create_draft_tasks(COURSE_ID, []) == []
            conn.assert_not_called()
