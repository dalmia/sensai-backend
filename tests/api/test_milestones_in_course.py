"""
`milestones_in_course` against a real SQLite file.

The conftest autouse fixture mocks `api.utils.db.execute_db_operation`, but
`api.utils.authorization` did `from api.utils.db import execute_db_operation` at
import time, so it holds the real one. Pointing `get_new_db_connection` at a temp
file therefore runs the actual SQL - which matters here, because this query is
what enforces `deleted_at IS NULL` on both joined tables. A membership lookup
that forgets that is the shape that let revoked public-API keys keep working.
"""

import os
import tempfile
from contextlib import asynccontextmanager
from unittest.mock import patch

import aiosqlite
import pytest

from api.utils.authorization import milestones_in_course

COURSE = 1
OTHER_COURSE = 2


@asynccontextmanager
async def _connect(path):
    conn = await aiosqlite.connect(path)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def db():
    path = os.path.join(tempfile.mkdtemp(), "t.sqlite")

    async with _connect(path) as conn:
        await conn.executescript(
            """
            CREATE TABLE milestones (id INTEGER PRIMARY KEY, org_id INTEGER, name TEXT, deleted_at DATETIME);
            CREATE TABLE course_milestones (
                id INTEGER PRIMARY KEY, course_id INTEGER, milestone_id INTEGER,
                ordering INTEGER, deleted_at DATETIME
            );

            INSERT INTO milestones (id, org_id, name, deleted_at) VALUES
                (10, 1, 'live',            NULL),
                (11, 1, 'link deleted',    NULL),
                (12, 1, 'milestone gone',  '2026-01-01'),
                (13, 1, 'other course',    NULL);

            INSERT INTO course_milestones (course_id, milestone_id, ordering, deleted_at) VALUES
                (1, 10, 0, NULL),
                (1, 11, 1, '2026-01-01'),
                (1, 12, 2, NULL),
                (2, 13, 0, NULL);
            """
        )
        await conn.commit()

    with patch("api.utils.db.get_new_db_connection", lambda: _connect(path)):
        yield


@pytest.mark.asyncio
async def test_returns_only_live_milestones_linked_to_this_course(db):
    assert await milestones_in_course(COURSE, {10, 11, 12, 13}) == {10}


@pytest.mark.asyncio
async def test_a_soft_deleted_link_is_not_a_membership(db):
    """The milestone still exists; the link to this course was removed."""
    assert await milestones_in_course(COURSE, {11}) == set()


@pytest.mark.asyncio
async def test_a_soft_deleted_milestone_is_not_a_membership(db):
    """The link still exists; the milestone itself was deleted."""
    assert await milestones_in_course(COURSE, {12}) == set()


@pytest.mark.asyncio
async def test_another_courses_milestone_is_not_a_membership(db):
    assert await milestones_in_course(COURSE, {13}) == set()
    assert await milestones_in_course(OTHER_COURSE, {13}) == {13}


@pytest.mark.asyncio
async def test_unknown_ids_are_simply_absent(db):
    assert await milestones_in_course(COURSE, {10, 9999}) == {10}


@pytest.mark.asyncio
async def test_empty_input_touches_nothing(db):
    assert await milestones_in_course(COURSE, set()) == set()


@pytest.mark.asyncio
async def test_chunks_past_the_sqlite_parameter_cap(db):
    """A single IN (...) over 1000+ ids would raise OperationalError."""
    assert await milestones_in_course(COURSE, set(range(1, 2500)) | {10}) == {10}
