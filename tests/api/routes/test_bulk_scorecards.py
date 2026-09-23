"""Exercise scorecard authorization, persistence and rollback on real SQLite."""

from unittest.mock import AsyncMock, patch

import aiosqlite
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from api import db as schema
from api.db import task as task_db
from api.main import app
from api.middleware import permissions
from api.models import BulkTaskItem
from api.utils.authorization import scorecards_in_org
from api.utils.db import get_new_db_connection as real_connection
from api.utils.tokens import create_access_token


@pytest_asyncio.fixture
async def scorecard_db(tmp_path):
    path = str(tmp_path / "bulk.sqlite")
    async with aiosqlite.connect(path) as conn:
        cursor = await conn.cursor()
        for create in (
            schema.create_tasks_table, schema.create_course_tasks_table,
            schema.create_questions_table, schema.create_scorecards_table,
            schema.create_question_scorecards_table,
        ):
            await create(cursor)
        await conn.executescript("""
            INSERT INTO scorecards (id, org_id, title, criteria, deleted_at) VALUES
                (7, 1, 'Rubric', '[]', NULL),
                (8, 2, 'Foreign', '[]', NULL),
                (9, 1, 'Deleted', '[]', '2026-01-01');
        """)
        await conn.commit()
    with patch("api.utils.db.sqlite_db_path", path), patch(
        "api.utils.db.get_new_db_connection", real_connection
    ), patch.object(task_db, "get_new_db_connection", real_connection), patch.object(
        task_db, "get_org_id_for_course", AsyncMock(return_value=1)
    ):
        yield path


def quiz(scorecard_id=7):
    return {
        "milestone_id": 42, "type": "quiz", "title": "Quiz",
        "questions": [{"title": "Question", "scorecard_id": scorecard_id}],
    }


@pytest.mark.real_permissions
@pytest.mark.asyncio
@pytest.mark.parametrize("scorecard_id, status", [(7, 200), (8, 409), (9, 409), (999, 409)])
async def test_authorizes_and_persists_only_own_live_scorecards(scorecard_db, scorecard_id, status):
    with patch.object(permissions, "org_for_course", AsyncMock(return_value=1)), patch.object(
        permissions, "is_org_staff", AsyncMock(return_value=True)
    ), patch.object(permissions, "milestones_in_course", AsyncMock(return_value={42})):
        client = TestClient(app, headers={
            "Authorization": f"Bearer {create_access_token(1, 'staff@example.com')}"
        })
        response = client.post("/courses/5/tasks/bulk", json={"items": [quiz(scorecard_id)]})
    assert response.status_code == status
    async with aiosqlite.connect(scorecard_db) as conn:
        tasks = await (await conn.execute("SELECT id, status FROM tasks")).fetchall()
        links = await (await conn.execute("""
            SELECT q.task_id, qs.scorecard_id FROM questions q
            JOIN question_scorecards qs ON qs.question_id = q.id
        """)).fetchall()
        if status == 200:
            assert tasks == [(response.json()["created"][0], "draft")]
            assert links == [(tasks[0][0], 7)]
        else:
            assert tasks == []
            assert links == []
        assert (await (await conn.execute("SELECT COUNT(*) FROM scorecards")).fetchone())[0] == 3


@pytest.mark.asyncio
async def test_scorecard_lookup_chunks_and_filters(scorecard_db):
    assert await scorecards_in_org(1, range(2500)) == {7}
    assert await scorecards_in_org(1, []) == set()


@pytest.mark.asyncio
async def test_link_failure_rolls_back_entire_import(scorecard_db):
    async with aiosqlite.connect(scorecard_db) as conn:
        await conn.execute("""
            CREATE TRIGGER fail_second_link BEFORE INSERT ON question_scorecards
            WHEN (SELECT COUNT(*) FROM question_scorecards) = 1
            BEGIN SELECT RAISE(ABORT, 'link failed'); END
        """)
        await conn.commit()
    items = [BulkTaskItem(**quiz()).model_dump() for _ in range(2)]
    with pytest.raises(aiosqlite.IntegrityError, match="link failed"):
        await task_db.bulk_create_draft_tasks(5, items)
    async with aiosqlite.connect(scorecard_db) as conn:
        for table in ("tasks", "course_tasks", "questions", "question_scorecards"):
            assert (await (await conn.execute(f"SELECT COUNT(*) FROM {table}")).fetchone())[0] == 0
