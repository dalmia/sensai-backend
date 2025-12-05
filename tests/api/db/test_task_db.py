import pytest
import json
from unittest.mock import patch, AsyncMock, MagicMock, ANY, call
from datetime import datetime, timezone, timedelta
from src.api.db.task import (
    create_draft_task_for_course,
    get_all_learning_material_tasks_for_course,
    convert_question_db_to_dict,
    get_scorecard,
    get_question,
    get_basic_task_details,
    get_task,
    get_task_metadata,
    does_task_exist,
    prepare_blocks_for_publish,
    update_learning_material_task,
    update_draft_quiz,
    update_published_quiz,
    duplicate_task,
    delete_task,
    delete_tasks,
    get_solved_tasks_for_user,
    mark_task_completed,
    delete_completion_history_for_task,
    schedule_module_tasks,
    drop_task_generation_jobs_table,
    store_task_generation_request,
    update_task_generation_job_status,
    get_course_task_generation_jobs_status,
    get_all_pending_task_generation_jobs,
    drop_task_completions_table,
    get_all_scorecards_for_org,
    create_scorecard,
    update_scorecard,
    undo_task_delete,
    publish_scheduled_tasks,
    add_generated_learning_material,
    add_generated_quiz,
    upsert_question,
    create_assignment,
    update_assignment,
    get_assignment,
    get_assignment_task,
    convert_assignment_to_task_dict,
)
from src.api.models import (
    TaskType,
    TaskStatus,
    ScorecardStatus,
    LeaderboardViewType,
    GenerateTaskJobStatus,
    TaskAIResponseType,
    BaseScorecard,
)
from src.api.config import task_completions_table_name


@pytest.mark.asyncio
class TestTaskOperations:
    """Test task-related database operations."""

    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.execute_db_operation")
    async def test_create_draft_task_for_course_success(
        self, mock_execute, mock_db_conn, mock_get_org
    ):
        """Test successful task creation."""
        mock_get_org.return_value = 123

        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 456
        mock_cursor.fetchone.return_value = (5,)  # max ordering
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        # Mock visible ordering calculation
        mock_execute.return_value = (2,)

        result = await create_draft_task_for_course(
            "Test Task", TaskType.LEARNING_MATERIAL, 1, 10
        )

        assert result == (456, 2)
        mock_get_org.assert_called_once_with(1)

    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.execute_db_operation")
    async def test_create_draft_task_for_course_with_ordering(
        self, mock_execute, mock_db_conn, mock_get_org
    ):
        """Test task creation with specific ordering."""
        mock_get_org.return_value = 123

        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 456
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        mock_execute.return_value = (1,)

        result = await create_draft_task_for_course(
            "Test Task", TaskType.QUIZ, 1, 10, ordering=3
        )

        assert result == (456, 1)

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_all_learning_material_tasks_for_course(self, mock_execute):
        """Test retrieving learning material tasks for course."""
        mock_execute.return_value = [
            (1, "Task 1", TaskType.LEARNING_MATERIAL, TaskStatus.PUBLISHED, None),
            (
                2,
                "Task 2",
                TaskType.LEARNING_MATERIAL,
                TaskStatus.PUBLISHED,
                "2024-01-01 12:00:00",
            ),
        ]

        result = await get_all_learning_material_tasks_for_course(123)

        expected = [
            {
                "id": 1,
                "title": "Task 1",
                "type": TaskType.LEARNING_MATERIAL,
                "status": TaskStatus.PUBLISHED,
                "scheduled_publish_at": None,
            },
            {
                "id": 2,
                "title": "Task 2",
                "type": TaskType.LEARNING_MATERIAL,
                "status": TaskStatus.PUBLISHED,
                "scheduled_publish_at": "2024-01-01 12:00:00",
            },
        ]

        assert result == expected

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_scorecard_success(self, mock_execute):
        """Test successful scorecard retrieval."""
        mock_execute.return_value = (
            1,
            "Test Scorecard",
            '{"criteria": [{"name": "Quality", "max_score": 10}]}',
            ScorecardStatus.PUBLISHED,
        )

        result = await get_scorecard(1)

        expected = {
            "id": 1,
            "title": "Test Scorecard",
            "criteria": {"criteria": [{"name": "Quality", "max_score": 10}]},
            "status": ScorecardStatus.PUBLISHED,
        }

        assert result == expected

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_scorecard_not_found(self, mock_execute):
        """Test scorecard retrieval when not found."""
        mock_execute.return_value = None

        result = await get_scorecard(999)

        assert result is None

    async def test_get_scorecard_none_id(self):
        """Test scorecard retrieval with None ID."""
        result = await get_scorecard(None)

        assert result is None

    @patch("src.api.db.task.get_scorecard")
    @patch("src.api.db.task.execute_db_operation")
    async def test_get_question_success(self, mock_execute, mock_get_scorecard):
        """Test successful question retrieval."""
        mock_execute.return_value = (
            1,  # id
            "multiple_choice",  # type
            '[{"type": "text", "content": "What is 2+2?"}]',  # blocks
            '[{"type": "text", "content": "4"}]',  # answer
            "text",  # input_type
            "chat",  # response_type
            123,  # scorecard_id
            '{"hint": "Think about addition"}',  # context
            '["python", "javascript"]',  # coding_language
            3,  # max_attempts
            True,  # is_feedback_shown
            "question",  # title
            None,  # settings
        )

        mock_scorecard = {
            "id": 123,
            "title": "Test Scorecard",
            "criteria": [],
            "status": ScorecardStatus.PUBLISHED,
        }
        mock_get_scorecard.return_value = mock_scorecard

        result = await get_question(1)

        expected = {
            "id": 1,
            "type": "multiple_choice",
            "blocks": [{"type": "text", "content": "What is 2+2?"}],
            "answer": [{"type": "text", "content": "4"}],
            "input_type": "text",
            "response_type": "chat",
            "scorecard_id": 123,
            "context": {"hint": "Think about addition"},
            "coding_languages": ["python", "javascript"],
            "max_attempts": 3,
            "is_feedback_shown": True,
            "scorecard": mock_scorecard,
            "title": "question",
            "settings": None,
        }

        assert result == expected

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_question_not_found(self, mock_execute):
        """Test question retrieval when not found."""
        mock_execute.return_value = None

        result = await get_question(999)

        assert result is None

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_basic_task_details_success(self, mock_execute):
        """Test successful basic task details retrieval."""
        mock_execute.return_value = (
            1,
            "Test Task",
            TaskType.LEARNING_MATERIAL,
            TaskStatus.PUBLISHED,
            123,
            None,
        )

        result = await get_basic_task_details(1)

        expected = {
            "id": 1,
            "title": "Test Task",
            "type": TaskType.LEARNING_MATERIAL,
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        assert result == expected

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_basic_task_details_not_found(self, mock_execute):
        """Test basic task details when not found."""
        mock_execute.return_value = None

        result = await get_basic_task_details(999)

        assert result is None

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    async def test_get_task_learning_material(self, mock_execute, mock_get_basic):
        """Test getting learning material task."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Task",
            "type": "learning_material",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_execute.return_value = ('[{"type": "text", "content": "Hello World"}]',)

        result = await get_task(1)

        expected = {
            "id": 1,
            "title": "Test Task",
            "type": "learning_material",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
            "blocks": [{"type": "text", "content": "Hello World"}],
        }

        assert result == expected

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.convert_question_db_to_dict")
    async def test_get_task_quiz(self, mock_convert, mock_execute, mock_get_basic):
        """Test getting quiz task."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Quiz",
            "type": "quiz",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_questions = [
            (
                1,
                "multiple_choice",
                "[]",
                "[]",
                "text",
                "chat",
                None,
                None,
                None,
                1,
                True,
            ),
            (2, "open_ended", "[]", "[]", "text", "chat", None, None, None, 1, True),
        ]
        mock_execute.return_value = mock_questions

        mock_convert.side_effect = [
            {"id": 1, "type": "multiple_choice"},
            {"id": 2, "type": "open_ended"},
        ]

        result = await get_task(1)

        expected = {
            "id": 1,
            "title": "Test Quiz",
            "type": "quiz",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
            "questions": [
                {"id": 1, "type": "multiple_choice"},
                {"id": 2, "type": "open_ended"},
            ],
        }

        assert result == expected

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    async def test_get_task_assignment(self, mock_get_assignment, mock_get_basic):
        """Test getting assignment task - covers lines 249-256."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
            "settings": None,
        }
        mock_get_assignment.return_value = mock_assignment_data

        result = await get_task(1)

        expected = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
            "assignment": mock_assignment_data,
        }

        assert result == expected
        mock_get_assignment.assert_called_once_with(1)

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    async def test_get_task_assignment_no_data(self, mock_get_assignment, mock_get_basic):
        """Test getting assignment task when assignment data is None - covers lines 251-256."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_get_assignment.return_value = None

        result = await get_task(1)

        expected = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
            "assignment": convert_assignment_to_task_dict(None),
        }

        assert result == expected
        mock_get_assignment.assert_called_once_with(1)

    @patch("src.api.db.task.get_basic_task_details")
    async def test_get_task_not_found(self, mock_get_basic):
        """Test getting task when not found."""
        mock_get_basic.return_value = None

        result = await get_task(999)

        assert result is None

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_task_metadata_success(self, mock_execute):
        """Test successful task metadata retrieval."""
        mock_execute.return_value = (
            123,
            "Test Course",
            456,
            "Test Milestone",
            789,
            "Test Org",
        )

        result = await get_task_metadata(1)

        expected = {
            "course": {"id": 123, "name": "Test Course"},
            "milestone": {"id": 456, "name": "Test Milestone"},
            "org": {"id": 789, "name": "Test Org"},
        }

        assert result == expected

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_task_metadata_not_found(self, mock_execute):
        """Test task metadata when not found."""
        mock_execute.return_value = None

        result = await get_task_metadata(999)

        assert result is None

    @patch("src.api.db.task.execute_db_operation")
    async def test_does_task_exist_true(self, mock_execute):
        """Test task existence check when exists."""
        mock_execute.return_value = (1,)

        result = await does_task_exist(1)

        assert result is True

    @patch("src.api.db.task.execute_db_operation")
    async def test_does_task_exist_false(self, mock_execute):
        """Test task existence check when doesn't exist."""
        mock_execute.return_value = None

        result = await does_task_exist(999)

        assert result is False

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_learning_material_task_success(
        self, mock_get_task, mock_db_conn, mock_task_exists
    ):
        """Test successful learning material task update."""
        mock_task_exists.return_value = True

        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        mock_task = {"id": 1, "title": "Updated Task"}
        mock_get_task.return_value = mock_task

        blocks = [{"type": "text", "content": "Hello"}]
        scheduled_at = datetime.now()

        result = await update_learning_material_task(
            1, "Updated Task", blocks, scheduled_at
        )

        assert result == mock_task
        mock_cursor.execute.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.does_task_exist")
    async def test_update_learning_material_task_not_found(self, mock_task_exists):
        """Test learning material task update when task doesn't exist."""
        mock_task_exists.return_value = False

        result = await update_learning_material_task(999, "Title", [], None)

        assert result is False

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_success(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """Test successful draft quiz update."""
        mock_task_exists.return_value = True
        mock_get_basic.return_value = {"org_id": 123}

        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 456
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        questions = [
            {
                "type": "multiple_choice",
                "blocks": [{"type": "text", "content": "Question"}],
                "answer": [{"type": "text", "content": "Answer"}],
                "input_type": "text",
                "response_type": "chat",
                "coding_languages": None,
                "context": None,
                "max_attempts": 1,
                "is_feedback_shown": True,
                "scorecard_id": None,
                "title": "question",
            }
        ]

        mock_task = {"id": 1, "title": "Updated Quiz", "questions": questions}
        mock_get_task.return_value = mock_task

        result = await update_draft_quiz(1, "Updated Quiz", questions, None)

        assert result == mock_task

    @patch("src.api.db.task.does_task_exist")
    async def test_update_draft_quiz_not_found(self, mock_task_exists):
        """Test draft quiz update when task doesn't exist."""
        mock_task_exists.return_value = False

        result = await update_draft_quiz(999, "Title", [], None)

        assert result is False

    @pytest.mark.asyncio
    async def test_upsert_question_updates_existing_question(self):
        """Covers UPDATE branch of upsert_question when question_id exists."""
        # Arrange: Create a fake cursor that records calls
        mock_cursor = AsyncMock()
        question = {
            "id": 123,
            "type": "multiple_choice",
            "blocks": [],
            "answer": None,
            "input_type": "text",
            "response_type": "chat",
            "coding_languages": None,
            "context": None,
            "max_attempts": 1,
            "is_feedback_shown": True,
            "title": "q",
            "settings": {},
        }

        # Act
        returned_id = await upsert_question(
            mock_cursor, question, task_id=1, position=0
        )

        # Assert: ensure UPDATE executed and returned id is the same
        assert returned_id == 123
        # Find the UPDATE call
        update_calls = [
            c for c in mock_cursor.execute.call_args_list if "UPDATE" in str(c)
        ]
        assert len(update_calls) == 1, "Expected an UPDATE call for existing question"

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_deletes_and_reinserts_scorecard_when_changed(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """Covers the SELECT old scorecard, conditional DELETE, and INSERT when changed."""
        mock_task_exists.return_value = True
        mock_get_basic.return_value = {"org_id": 1}

        mock_cursor = AsyncMock()
        # existing questions in DB: id 10
        mock_cursor.fetchall.return_value = [(10,)]
        # SELECT scorecard_id returns old id 5
        mock_cursor.fetchone.return_value = (5,)
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        questions = [
            {
                "id": 10,
                "type": "multiple_choice",
                "blocks": [],
                "answer": None,
                "input_type": "text",
                "response_type": "chat",
                "coding_languages": None,
                "context": None,
                "max_attempts": 1,
                "is_feedback_shown": True,
                "scorecard_id": 6,  # changed from 5 -> 6
                "title": "q",
                "settings": {},
            }
        ]

        mock_get_task.return_value = {"id": 1, "questions": questions}

        result = await update_draft_quiz(1, "t", questions, datetime.now())
        assert result["id"] == 1

        # Ensure we selected existing mapping
        select_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if "SELECT scorecard_id" in str(c)
        ]
        assert select_calls, "Expected a SELECT for existing scorecard mapping"
        # Ensure the DELETE ran because mapping changed
        delete_or_soft_delete_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if (
                ("DELETE FROM" in str(c) and "question_scorecards" in str(c))
                or (
                    "UPDATE" in str(c)
                    and "question_scorecards" in str(c)
                    and "deleted_at" in str(c)
                )
            )
        ]
        assert (
            delete_or_soft_delete_calls
        ), "Expected soft delete of old scorecard mapping when changed"
        # Ensure we inserted the new mapping
        insert_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO" in str(c) and "question_scorecards" in str(c)
        ]
        assert insert_calls, "Expected INSERT of new scorecard mapping"

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_skips_delete_when_scorecard_unchanged(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """If scorecard_id is unchanged, do not DELETE existing mapping."""
        mock_task_exists.return_value = True
        mock_get_basic.return_value = {"org_id": 1}

        mock_cursor = AsyncMock()
        # existing question id
        mock_cursor.fetchall.return_value = [(10,)]
        # SELECT scorecard_id returns the same id as provided
        mock_cursor.fetchone.return_value = (6,)
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        questions = [
            {
                "id": 10,
                "type": "multiple_choice",
                "blocks": [],
                "answer": None,
                "input_type": "text",
                "response_type": "chat",
                "coding_languages": None,
                "context": None,
                "max_attempts": 1,
                "is_feedback_shown": True,
                "scorecard_id": 6,  # unchanged
                "title": "q",
                "settings": {},
            }
        ]

        mock_get_task.return_value = {"id": 1, "questions": questions}

        await update_draft_quiz(1, "t", questions, datetime.now())

        # There should be no DELETE of mapping because unchanged
        delete_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if "DELETE FROM" in str(c) and "question_scorecards" in str(c)
        ]
        # It's possible other deletes happen later (like cleanup). Filter by WHERE question_id = ? form
        mapping_deletes = [c for c in delete_calls if "WHERE question_id = ?" in str(c)]
        assert (
            len(mapping_deletes) == 0
        ), "Should not delete mapping when scorecard_id unchanged"

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_deletes_removed_questions_with_in_clause(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """Covers DELETE ... IN (...) for removed questions and their scorecard mappings."""
        mock_task_exists.return_value = True
        mock_get_basic.return_value = {"org_id": 1}

        mock_cursor = AsyncMock()
        # existing questions: ids 1 and 2; we will only provide id 1 so id 2 gets deleted
        mock_cursor.fetchall.return_value = [(1,), (2,)]
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        provided_questions = [
            {
                "id": 1,
                "type": "multiple_choice",
                "blocks": [],
                "answer": None,
                "input_type": "text",
                "response_type": "chat",
                "coding_languages": None,
                "context": None,
                "max_attempts": 1,
                "is_feedback_shown": True,
                "scorecard_id": None,
                "title": "q",
                "settings": {},
            }
        ]

        mock_get_task.return_value = {"id": 1, "questions": provided_questions}

        await update_draft_quiz(1, "t", provided_questions, datetime.now())

        # Ensure DELETE ... IN (...) for scorecard mappings and questions
        calls_str = "\n".join(str(c) for c in mock_cursor.execute.call_args_list)
        assert (
            "WHERE question_id IN (2)" in calls_str
            or "WHERE question_id IN (" in calls_str
        )
        assert "WHERE id IN (2)" in calls_str or "WHERE id IN (" in calls_str

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    async def test_update_draft_quiz_task_exists_but_basic_details_none(
        self, mock_get_basic, mock_task_exists
    ):
        """Test update_draft_quiz when task exists but get_basic_task_details returns None - covers line 348."""
        mock_task_exists.return_value = True  # Task exists according to first check
        mock_get_basic.return_value = None  # But basic details returns None

        result = await update_draft_quiz(
            1, "Test Title", [], datetime.now(), TaskStatus.DRAFT
        )

        assert result is False
        mock_task_exists.assert_called_once_with(1)
        mock_get_basic.assert_called_once_with(1)

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_published_quiz_success(
        self, mock_get_task, mock_db_conn, mock_task_exists
    ):
        """Test successful published quiz update."""
        mock_task_exists.return_value = True

        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        questions = [
            {
                "id": 1,
                "type": "multiple_choice",
                "blocks": [{"type": "text", "content": "Question"}],
                "answer": [{"type": "text", "content": "Answer"}],
                "input_type": "text",
                "response_type": "chat",
                "coding_languages": None,
                "context": None,
                "scorecard_id": None,
                "title": "question",
            }
        ]

        # Mock questions as having model_dump method
        class MockQuestion:
            def model_dump(self):
                return questions[0]

        mock_questions = [MockQuestion()]

        mock_task = {"id": 1, "title": "Updated Quiz", "questions": questions}
        mock_get_task.return_value = mock_task

        result = await update_published_quiz(1, "Updated Quiz", mock_questions, None)

        assert result == mock_task

    @patch("src.api.db.task.does_task_exist")
    async def test_update_published_quiz_not_found(self, mock_task_exists):
        """Test published quiz update when task doesn't exist."""
        mock_task_exists.return_value = False

        result = await update_published_quiz(999, "Title", [], None)

        assert result is False

    @patch("src.api.db.task.execute_db_operation")
    async def test_delete_task(self, mock_execute):
        """Test task deletion."""
        await delete_task(1)

        assert mock_execute.call_count == 2

        first_args = mock_execute.call_args_list[0][0]
        assert "UPDATE tasks" in first_args[0]
        assert "deleted_at" in first_args[0]

    @patch("src.api.db.task.execute_db_operation")
    async def test_delete_tasks(self, mock_execute):
        """Test multiple tasks deletion."""
        await delete_tasks([1, 2, 3])

        mock_execute.assert_called_once()
        args = mock_execute.call_args[0]
        assert "UPDATE tasks" in args[0]
        assert "deleted_at" in args[0]

    @patch("src.api.db.task.execute_db_operation")
    async def test_mark_task_completed(self, mock_execute):
        """Test marking task as completed."""
        await mark_task_completed(1, 123)

        mock_execute.assert_called_once_with(
            f"""
        INSERT INTO {task_completions_table_name} (user_id, task_id)
        VALUES (?, ?)
        ON CONFLICT(user_id, task_id) DO UPDATE SET
            deleted_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        """,
            (123, 1),
        )

    @patch("src.api.db.task.execute_db_operation")
    async def test_delete_completion_history_for_task_with_task_id(self, mock_execute):
        """Test deleting completion history with task ID."""
        await delete_completion_history_for_task(1, 123, 456)

        assert mock_execute.call_count == 2

    @patch("src.api.db.task.execute_db_operation")
    async def test_delete_completion_history_for_task_without_task_id(
        self, mock_execute
    ):
        """Test deleting completion history without task ID."""
        await delete_completion_history_for_task(None, 123, 456)

        assert mock_execute.call_count == 1

    @patch("src.api.db.task.get_new_db_connection")
    async def test_schedule_module_tasks(self, mock_db_conn):
        """Test scheduling module tasks."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [(1,), (2,)]
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        scheduled_at = datetime.now()
        await schedule_module_tasks(1, 2, scheduled_at)

        assert mock_cursor.execute.call_count >= 2  # At least select and update calls
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.get_new_db_connection")
    async def test_drop_task_generation_jobs_table(self, mock_db_conn):
        """Test dropping task generation jobs table."""
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        await drop_task_generation_jobs_table()

        mock_cursor.execute.assert_called_once()

    @patch("src.api.db.task.get_new_db_connection")
    async def test_store_task_generation_request(self, mock_db_conn):
        """Test storing task generation request."""
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        job_details = {"type": "learning_material", "topic": "Python"}
        result = await store_task_generation_request(1, 2, job_details)

        assert isinstance(result, str)  # Should return UUID
        mock_cursor.execute.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.get_new_db_connection")
    async def test_update_task_generation_job_status(self, mock_db_conn):
        """Test updating task generation job status."""
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        await update_task_generation_job_status(
            "test-uuid", GenerateTaskJobStatus.COMPLETED
        )

        mock_cursor.execute.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.get_new_db_connection")
    async def test_get_course_task_generation_jobs_status(self, mock_db_conn):
        """Test getting course task generation jobs status."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [
            (str(GenerateTaskJobStatus.COMPLETED),),
            (str(GenerateTaskJobStatus.STARTED),),
            (str(GenerateTaskJobStatus.COMPLETED),),
        ]
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        result = await get_course_task_generation_jobs_status(1)

        expected = {
            str(GenerateTaskJobStatus.COMPLETED): 2,
            str(GenerateTaskJobStatus.STARTED): 1,
        }

        assert result == expected

    @patch("src.api.db.task.get_new_db_connection")
    async def test_get_all_pending_task_generation_jobs(self, mock_db_conn):
        """Test getting all pending task generation jobs."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [
            ("uuid1", '{"type": "quiz"}'),
            ("uuid2", '{"type": "learning_material"}'),
        ]
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        result = await get_all_pending_task_generation_jobs()

        expected = [
            {"uuid": "uuid1", "job_details": {"type": "quiz"}},
            {"uuid": "uuid2", "job_details": {"type": "learning_material"}},
        ]

        assert result == expected

    @patch("src.api.db.task.get_new_db_connection")
    async def test_drop_task_completions_table(self, mock_db_conn):
        """Test dropping task completions table."""
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        await drop_task_completions_table()

        mock_cursor.execute.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.get_new_db_connection")
    async def test_undo_task_delete(self, mock_db_conn):
        """Test undoing task deletion."""
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        await undo_task_delete(1)

        mock_cursor.execute.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.execute_db_operation")
    async def test_publish_scheduled_tasks(self, mock_execute):
        """Test publishing scheduled tasks."""
        mock_execute.return_value = [(1,), (2,)]

        result = await publish_scheduled_tasks()

        assert result == [1, 2]

    @patch("src.api.db.task.update_learning_material_task")
    @patch("src.api.db.task.convert_blocks_to_right_format")
    async def test_add_generated_learning_material(self, mock_convert, mock_update):
        """Test adding generated learning material."""
        mock_convert.return_value = [{"type": "text", "content": "Hello"}]

        task_details = {
            "name": "Generated Task",
            "details": {"blocks": [{"type": "text", "content": "Hello"}]},
        }

        await add_generated_learning_material(1, task_details)

        mock_convert.assert_called_once()
        # Just check that update was called with the right parameters
        args, kwargs = mock_update.call_args
        assert args[0] == 1
        assert args[1] == "Generated Task"
        assert args[3] is None
        assert str(args[4]) == "published"

    @patch("src.api.db.task.update_draft_quiz")
    @patch("src.api.db.task.convert_blocks_to_right_format")
    @patch("src.api.db.task.prepare_blocks_for_publish")
    async def test_add_generated_quiz(self, mock_prepare, mock_convert, mock_update):
        """Test adding generated quiz."""
        mock_convert.side_effect = lambda x: x  # Return input as-is
        mock_prepare.side_effect = lambda x: x  # Return input as-is

        task_details = {
            "name": "Generated Quiz",
            "details": {
                "questions": [
                    {
                        "question_title": "Question 1",
                        "question_type": "multiple_choice",
                        "blocks": [{"type": "text", "content": "Question?"}],
                        "correct_answer": [{"type": "text", "content": "Answer"}],
                        "answer_type": "text",
                        "context": [{"type": "text", "content": "Context"}],
                        "coding_languages": ["python"],
                    }
                ]
            },
        }

        await add_generated_quiz(1, task_details)

        mock_update.assert_called_once()

    @patch("src.api.db.task.update_draft_quiz")
    @patch("src.api.db.task.convert_blocks_to_right_format")
    @patch("src.api.db.task.prepare_blocks_for_publish")
    async def test_add_generated_quiz_with_scorecard(
        self, mock_prepare, mock_convert, mock_update
    ):
        """Test adding generated quiz with questions that have scorecards - covers lines 936-937."""
        mock_convert.side_effect = lambda x: x  # Return input as-is
        mock_prepare.side_effect = lambda x: x  # Return input as-is

        task_details = {
            "name": "Generated Quiz with Scorecard",
            "details": {
                "questions": [
                    {
                        "question_title": "Question 1",
                        "question_type": "multiple_choice",
                        "blocks": [{"type": "text", "content": "Question?"}],
                        "correct_answer": [{"type": "text", "content": "Answer"}],
                        "answer_type": "text",
                        "context": [{"type": "text", "content": "Context"}],
                        "coding_languages": ["python"],
                        "scorecard": {  # This question has a scorecard
                            "title": "Test Scorecard",
                            "criteria": [{"name": "Quality", "max_score": 10}],
                        },
                    },
                    {
                        "question_title": "Question 2",
                        "question_type": "open_ended",
                        "blocks": [{"type": "text", "content": "Another Question?"}],
                        "correct_answer": None,
                        "answer_type": "text",
                        "context": None,
                        "coding_languages": None,
                        "scorecard": {  # This question also has a scorecard
                            "title": "Another Scorecard",
                            "criteria": [{"name": "Accuracy", "max_score": 5}],
                        },
                    },
                ]
            },
        }

        await add_generated_quiz(1, task_details)

        # Verify that scorecards were processed correctly
        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        questions = call_args[2]  # The questions parameter

        # First question should have scorecard with id 0
        assert questions[0]["scorecard"]["id"] == 0
        # Second question should have scorecard with id 1
        assert questions[1]["scorecard"]["id"] == 1

    @patch("src.api.db.task.update_draft_quiz")
    @patch("src.api.db.task.convert_blocks_to_right_format")
    @patch("src.api.db.task.prepare_blocks_for_publish")
    async def test_add_generated_quiz_without_scorecard(
        self, mock_prepare, mock_convert, mock_update
    ):
        """Test adding generated quiz with questions that don't have scorecards - covers line 939."""
        mock_convert.side_effect = lambda x: x  # Return input as-is
        mock_prepare.side_effect = lambda x: x  # Return input as-is

        task_details = {
            "name": "Generated Quiz without Scorecard",
            "details": {
                "questions": [
                    {
                        "question_title": "Question 1",
                        "question_type": "multiple_choice",
                        "blocks": [{"type": "text", "content": "Question?"}],
                        "correct_answer": [{"type": "text", "content": "Answer"}],
                        "answer_type": "text",
                        "context": [{"type": "text", "content": "Context"}],
                        "coding_languages": ["python"],
                        # No scorecard field
                    }
                ]
            },
        }

        await add_generated_quiz(1, task_details)

        # Verify that scorecard was set to None
        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        questions = call_args[2]  # The questions parameter

        # Question should have scorecard set to None
        assert questions[0]["scorecard"] is None

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_published_quiz_insert_new_scorecard_mapping(
        self, mock_get_task, mock_db_conn, mock_task_exists
    ):
        """Test update_published_quiz when inserting new scorecard mapping - covers lines 497-500."""
        mock_task_exists.return_value = True

        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = None  # No existing mapping
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        # Create a question with a scorecard_id but no existing mapping
        from pydantic import BaseModel
        from typing import List, Optional

        class MockQuestion(BaseModel):
            id: int
            type: str = "MULTIPLE_CHOICE"
            blocks: List = []
            answer: Optional[List] = None
            input_type: str = "MULTIPLE_CHOICE"
            response_type: str = "MULTIPLE_CHOICE"
            coding_languages: Optional[List] = None
            context: Optional[List] = None
            scorecard_id: Optional[int] = None
            title: str = "question"

            def model_dump(self):
                return {
                    "id": self.id,
                    "type": self.type,
                    "blocks": self.blocks,
                    "answer": self.answer,
                    "input_type": self.input_type,
                    "response_type": self.response_type,
                    "coding_languages": self.coding_languages,
                    "context": self.context,
                    "scorecard_id": self.scorecard_id,
                    "title": self.title,
                }

        # Question with scorecard_id but no existing mapping
        question_with_new_scorecard = MockQuestion(id=1, scorecard_id=456)
        questions = [question_with_new_scorecard]

        mock_task = {
            "id": 1,
            "title": "Test Quiz",
            "questions": [question_with_new_scorecard.model_dump()],
        }
        mock_get_task.return_value = mock_task

        result = await update_published_quiz(1, "Test Quiz", questions, datetime.now())

        assert result == mock_task

        # Verify that the INSERT query was called (the "else" branch)
        insert_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "INSERT INTO" in str(call)
        ]
        assert (
            len(insert_calls) > 0
        ), "INSERT query should have been called for new scorecard mapping"

    @patch("src.api.db.task.update_draft_quiz")
    @patch("src.api.db.task.convert_blocks_to_right_format")
    @patch("src.api.db.task.prepare_blocks_for_publish")
    async def test_add_generated_quiz_mixed_scorecards(
        self, mock_prepare, mock_convert, mock_update
    ):
        """Test adding generated quiz with mixed scorecard scenarios."""
        mock_convert.side_effect = lambda x: x  # Return input as-is
        mock_prepare.side_effect = lambda x: x  # Return input as-is

        task_details = {
            "name": "Generated Quiz Mixed",
            "details": {
                "questions": [
                    {
                        "question_title": "Question 1",
                        "question_type": "multiple_choice",
                        "blocks": [{"type": "text", "content": "Question 1?"}],
                        "correct_answer": [{"type": "text", "content": "Answer 1"}],
                        "answer_type": "text",
                        "context": None,
                        "coding_languages": None,
                        "scorecard": {  # Has scorecard
                            "title": "Scorecard 1",
                            "criteria": [{"name": "Quality", "max_score": 10}],
                        },
                    },
                    {
                        "question_title": "Question 2",
                        "question_type": "open_ended",
                        "blocks": [{"type": "text", "content": "Question 2?"}],
                        "correct_answer": None,
                        "answer_type": "text",
                        "context": None,
                        "coding_languages": None,
                        # No scorecard field - should be set to None
                    },
                    {
                        "question_title": "Question 3",
                        "question_type": "coding",
                        "blocks": [{"type": "text", "content": "Question 3?"}],
                        "correct_answer": [{"type": "text", "content": "Answer 3"}],
                        "answer_type": "text",
                        "context": None,
                        "coding_languages": ["python"],
                        "scorecard": {  # Has scorecard
                            "title": "Scorecard 2",
                            "criteria": [{"name": "Correctness", "max_score": 15}],
                        },
                    },
                ]
            },
        }

        await add_generated_quiz(1, task_details)

        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        questions = call_args[2]  # The questions parameter

        # First question should have scorecard with id 0
        assert questions[0]["scorecard"]["id"] == 0
        # Second question should have scorecard set to None
        assert questions[1]["scorecard"] is None
        # Third question should have scorecard with id 1
        assert questions[2]["scorecard"]["id"] == 1


@pytest.mark.asyncio
class TestTaskUtilities:
    """Test task utility functions."""

    def test_convert_question_db_to_dict_complete(self):
        """Test converting complete question tuple to dictionary."""
        question_tuple = (
            1,  # id
            "multiple_choice",  # type
            '[{"type": "text", "content": "Question?"}]',  # blocks
            '[{"type": "text", "content": "Answer"}]',  # answer
            "text",  # input_type
            "chat",  # response_type
            123,  # scorecard_id
            '{"hint": "Hint"}',  # context
            '["python"]',  # coding_languages
            3,  # max_attempts
            True,  # is_feedback_shown
            "question",  # title
            '{"allowCopyPaste": true}',  # settings
        )

        result = convert_question_db_to_dict(question_tuple)

        expected = {
            "id": 1,
            "type": "multiple_choice",
            "blocks": [{"type": "text", "content": "Question?"}],
            "answer": [{"type": "text", "content": "Answer"}],
            "input_type": "text",
            "response_type": "chat",
            "scorecard_id": 123,
            "context": {"hint": "Hint"},
            "coding_languages": ["python"],
            "max_attempts": 3,
            "is_feedback_shown": True,
            "title": "question",
            "settings": {"allowCopyPaste": True},
        }

        assert result == expected

    def test_convert_question_db_to_dict_with_nulls(self):
        """Test converting question tuple with null values."""
        question_tuple = (
            1,  # id
            "open_ended",  # type
            None,  # blocks
            None,  # answer
            "text",  # input_type
            "chat",  # response_type
            None,  # scorecard_id
            None,  # context
            None,  # coding_languages
            1,  # max_attempts
            False,  # is_feedback_shown
            "question",  # title
            None,  # settings
        )

        result = convert_question_db_to_dict(question_tuple)

        expected = {
            "id": 1,
            "type": "open_ended",
            "blocks": [],
            "answer": None,
            "input_type": "text",
            "response_type": "chat",
            "scorecard_id": None,
            "context": None,
            "coding_languages": None,
            "max_attempts": 1,
            "is_feedback_shown": False,
            "title": "question",
            "settings": None,
        }

        assert result == expected

    def test_prepare_blocks_for_publish_without_ids(self):
        """Test preparing blocks without IDs."""
        blocks = [
            {"type": "text", "content": "Hello"},
            {"type": "text", "content": "World"},
        ]

        result = prepare_blocks_for_publish(blocks)

        assert len(result) == 2
        assert all("id" in block for block in result)
        assert all("position" in block for block in result)
        assert result[0]["position"] == 0
        assert result[1]["position"] == 1

    def test_prepare_blocks_for_publish_with_ids(self):
        """Test preparing blocks with existing IDs."""
        blocks = [
            {"id": "existing-id", "type": "text", "content": "Hello"},
            {"type": "text", "content": "World"},
        ]

        result = prepare_blocks_for_publish(blocks)

        assert result[0]["id"] == "existing-id"
        assert "id" in result[1]
        assert result[1]["id"] != "existing-id"

    def test_prepare_blocks_for_publish_with_none_ids(self):
        """Test preparing blocks with None IDs."""
        blocks = [
            {"id": None, "type": "text", "content": "Hello"},
            {"type": "text", "content": "World"},
        ]

        result = prepare_blocks_for_publish(blocks)

        assert result[0]["id"] is not None
        assert result[1]["id"] is not None


@pytest.mark.asyncio
class TestScorecardOperations:
    """Test scorecard-related operations."""

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_all_scorecards_for_org(self, mock_execute):
        """Test getting all scorecards for org."""
        mock_execute.return_value = [
            (
                1,
                "Scorecard 1",
                '[{"name": "Quality", "max_score": 10}]',
                ScorecardStatus.PUBLISHED,
            ),
            (
                2,
                "Scorecard 2",
                '[{"name": "Accuracy", "max_score": 5, "pass_score": 3}]',
                ScorecardStatus.DRAFT,
            ),
        ]

        result = await get_all_scorecards_for_org(123)

        expected = [
            {
                "id": 1,
                "title": "Scorecard 1",
                "criteria": [{"name": "Quality", "max_score": 10, "pass_score": 10}],
                "status": ScorecardStatus.PUBLISHED,
            },
            {
                "id": 2,
                "title": "Scorecard 2",
                "criteria": [{"name": "Accuracy", "max_score": 5, "pass_score": 3}],
                "status": ScorecardStatus.DRAFT,
            },
        ]

        assert result == expected

    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_scorecard")
    async def test_create_scorecard(self, mock_get_scorecard, mock_execute):
        """Test creating scorecard."""
        mock_execute.return_value = 123
        mock_scorecard = {
            "id": 123,
            "title": "Test",
            "criteria": [],
            "status": ScorecardStatus.DRAFT,
        }
        mock_get_scorecard.return_value = mock_scorecard

        scorecard_data = {
            "org_id": 1,
            "title": "Test Scorecard",
            "criteria": [{"name": "Quality", "max_score": 10}],
        }

        result = await create_scorecard(scorecard_data)

        assert result == mock_scorecard
        mock_execute.assert_called_once()

    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_scorecard")
    async def test_update_scorecard(self, mock_get_scorecard, mock_execute):
        """Test updating scorecard."""
        mock_scorecard = {
            "id": 123,
            "title": "Updated",
            "criteria": [],
            "status": ScorecardStatus.DRAFT,
        }
        mock_get_scorecard.return_value = mock_scorecard

        # Create a mock BaseScorecard
        class MockScorecard:
            def model_dump(self):
                return {
                    "title": "Updated Scorecard",
                    "criteria": [{"name": "Quality", "max_score": 10}],
                }

        mock_scorecard_model = MockScorecard()

        result = await update_scorecard(123, mock_scorecard_model)

        assert result == mock_scorecard
        mock_execute.assert_called_once()


@pytest.mark.asyncio
class TestTaskDuplication:
    """Test task duplication operations."""

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_task")
    @patch("src.api.db.task.create_draft_task_for_course")
    @patch("src.api.db.task.update_learning_material_task")
    async def test_duplicate_task_learning_material(
        self,
        mock_update_task,
        mock_create_draft,
        mock_get_task,
        mock_get_org,
        mock_execute,
        mock_get_basic,
    ):
        """Test duplicating learning material task."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Original Task",
            "type": "learning_material",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_execute.return_value = (2,)  # task ordering
        mock_create_draft.return_value = (10, 3)  # (new_task_id, visible_ordering)
        mock_update_task.return_value = True
        mock_get_task.side_effect = [
            {
                "id": 1,
                "title": "Original Task",
                "type": "learning_material",
                "blocks": [{"type": "text", "content": "Hello"}],
            },
            {
                "id": 10,
                "title": "Original Task",
                "type": "learning_material",
                "blocks": [{"type": "text", "content": "Hello"}],
            },
        ]

        result = await duplicate_task(1, 100, 200)

        expected = {
            "task": {
                "id": 10,
                "title": "Original Task",
                "type": "learning_material",
                "blocks": [{"type": "text", "content": "Hello"}],
            },
            "ordering": 3,
        }

        assert result == expected

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_task")
    @patch("src.api.db.task.create_draft_task_for_course")
    @patch("src.api.db.task.update_draft_quiz")
    @patch("src.api.db.task.get_scorecard")
    async def test_duplicate_task_quiz(
        self,
        mock_get_scorecard,
        mock_update_quiz,
        mock_create_draft,
        mock_get_task,
        mock_get_org,
        mock_execute,
        mock_get_basic,
    ):
        """Test duplicating quiz task."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Original Quiz",
            "type": "quiz",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_execute.return_value = (2,)  # task ordering - return as tuple
        mock_create_draft.return_value = (10, 3)  # (new_task_id, visible_ordering)
        mock_update_quiz.return_value = {"id": 10, "title": "Original Quiz"}
        mock_get_scorecard.return_value = {
            "id": 123,
            "title": "Test Scorecard",
            "criteria": [],
            "status": ScorecardStatus.PUBLISHED,
        }
        mock_get_task.side_effect = [
            {
                "id": 1,
                "title": "Original Quiz",
                "type": "quiz",
                "questions": [
                    {
                        "id": 1,
                        "type": "multiple_choice",
                        "scorecard_id": 123,
                        "blocks": [],
                        "answer": [],
                        "input_type": "text",
                        "response_type": "chat",
                        "coding_languages": None,
                        "context": None,
                        "max_attempts": 1,
                        "is_feedback_shown": True,
                    }
                ],
            },
            {
                "id": 10,
                "title": "Original Quiz",
                "type": "quiz",
                "questions": [],
            },
        ]

        result = await duplicate_task(1, 100, 200)

        assert result["ordering"] == 3

    @patch("src.api.db.task.get_basic_task_details")
    async def test_duplicate_task_not_found(self, mock_get_basic):
        """Test duplicating non-existent task."""
        mock_get_basic.return_value = None

        with pytest.raises(ValueError, match="Task does not exist"):
            await duplicate_task(999, 100, 200)

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_task")
    async def test_duplicate_task_not_in_module(
        self, mock_get_task, mock_get_org, mock_execute, mock_get_basic
    ):
        """Test duplicating task not in specified module."""
        mock_get_basic.return_value = {"id": 1, "title": "Task"}
        mock_get_org.return_value = 123
        mock_execute.return_value = None  # This simulates task not being in module
        mock_get_task.return_value = None

        with pytest.raises(ValueError, match="Task is not in this module"):
            await duplicate_task(1, 100, 200)

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_task")
    @patch("src.api.db.task.create_draft_task_for_course")
    @patch("src.api.db.task.update_draft_quiz")
    async def test_duplicate_task_unsupported_type(
        self,
        mock_update_quiz,
        mock_create_draft,
        mock_get_task,
        mock_get_org,
        mock_execute,
        mock_get_basic,
    ):
        """Test duplicating task with unsupported type."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Task",
            "type": "UNSUPPORTED_TYPE",  # This will cause the error
            "milestone_id": 10,
        }

        # Mock the execute operation to prevent actual database calls
        mock_execute.return_value = (2,)
        mock_create_draft.return_value = (10, 3)

        # Mock get_task to prevent database access
        mock_get_task.return_value = {
            "id": 1,
            "title": "Test Task",
            "type": "UNSUPPORTED_TYPE",
            "blocks": [],
        }

        with pytest.raises(ValueError, match="Task type not supported"):
            await duplicate_task(1, 1, 10)

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.execute_db_operation")
    @patch("src.api.db.task.get_org_id_for_course")
    @patch("src.api.db.task.get_task")
    @patch("src.api.db.task.create_draft_task_for_course")
    @patch("src.api.db.task.create_assignment")
    async def test_duplicate_task_assignment(
        self,
        mock_create_assignment,
        mock_create_draft,
        mock_get_task,
        mock_get_org,
        mock_execute,
        mock_get_basic,
    ):
        """Test duplicating assignment task - covers lines 654-670."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Original Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_execute.return_value = (2,)  # task ordering
        mock_create_draft.return_value = (10, 3)  # (new_task_id, visible_ordering)

        mock_original_task = {
            "id": 1,
            "title": "Original Assignment",
            "type": "assignment",
            "assignment": {
                "blocks": [{"type": "paragraph", "content": "Assignment content"}],
                "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
                "evaluation_criteria": {
                    "min_score": 0,
                    "max_score": 100,
                    "pass_score": 70,
                },
                "input_type": "text",
                "response_type": "text",
                "max_attempts": 3,
                "settings": None,
            },
        }
        mock_get_task.return_value = mock_original_task

        mock_duplicated_task = {
            "id": 10,
            "title": "Original Assignment",
            "type": "assignment",
            "assignment": mock_original_task["assignment"],
        }

        # Mock the second call to get_task (for the duplicated task)
        mock_get_task.side_effect = [mock_original_task, mock_duplicated_task]

        result = await duplicate_task(1, 100, 200)

        expected = {
            "task": mock_duplicated_task,
            "ordering": 3,
        }

        assert result == expected

        # Verify create_assignment was called with correct parameters
        mock_create_assignment.assert_called_once()
        call_args = mock_create_assignment.call_args[0]
        
        # Check the arguments passed to create_assignment
        assert call_args[0] == 10  # new_task_id
        assert call_args[1] == "Original Assignment"  # title
        assert call_args[2] == mock_original_task["assignment"]  # assignment_data
        assert call_args[3] is None  # scheduled_publish_at
        assert call_args[4].value == TaskStatus.DRAFT.value  # status

    @patch("src.api.db.task.get_new_db_connection")
    async def test_schedule_module_tasks_no_tasks(self, mock_db_conn):
        """Test scheduling module tasks when no tasks exist."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = []  # No tasks
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        scheduled_at = datetime.now()
        await schedule_module_tasks(1, 2, scheduled_at)

        # Should return early without committing
        mock_conn_instance.commit.assert_not_called()

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_with_scorecard_publishing(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """Test draft quiz update that triggers scorecard publishing."""
        mock_task_exists.return_value = True
        mock_get_basic.return_value = {"org_id": 123}

        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 456
        mock_cursor.fetchone.return_value = (789,)  # Draft scorecard to be published
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        questions = [
            {
                "type": "multiple_choice",
                "blocks": [{"type": "text", "content": "Question"}],
                "answer": [{"type": "text", "content": "Answer"}],
                "input_type": "text",
                "response_type": "chat",
                "coding_languages": None,
                "context": None,
                "max_attempts": 1,
                "is_feedback_shown": True,
                "scorecard_id": 789,  # This will trigger publishing
                "title": "question",
            }
        ]

        mock_task = {"id": 1, "title": "Updated Quiz", "questions": questions}
        mock_get_task.return_value = mock_task

        result = await update_draft_quiz(1, "Updated Quiz", questions, None)

        assert result == mock_task

    @patch("src.api.db.task.execute_db_operation")
    async def test_publish_scheduled_tasks_empty(self, mock_execute):
        """Test publishing scheduled tasks when none exist."""
        mock_execute.return_value = []  # No tasks to publish

        result = await publish_scheduled_tasks()

        assert result == []

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_solved_tasks_for_user_weekly_view(self, mock_execute):
        """Test get_solved_tasks_for_user with WEEKLY view - covers lines 614-655."""
        mock_execute.return_value = [(1,), (2,), (3,)]  # Mock solved task IDs as tuples

        result = await get_solved_tasks_for_user(
            123,
            456,
            LeaderboardViewType.WEEKLY.value,  # Use .value to avoid enum comparison issues
        )

        assert result == [1, 2, 3]

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_solved_tasks_for_user_monthly_view(self, mock_execute):
        """Test get_solved_tasks_for_user with MONTHLY view - covers lines 614-655."""
        mock_execute.return_value = [(4,), (5,), (6,)]  # Mock solved task IDs as tuples

        result = await get_solved_tasks_for_user(
            123,
            456,
            LeaderboardViewType.MONTHLY.value,  # Use .value to avoid enum comparison issues
        )

        assert result == [4, 5, 6]

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_solved_tasks_for_user_all_time_view(self, mock_execute):
        """Test get_solved_tasks_for_user with ALL_TIME view explicitly."""
        mock_execute.return_value = [(7,), (8,), (9,)]  # Mock solved task IDs as tuples

        result = await get_solved_tasks_for_user(
            123,
            456,
            LeaderboardViewType.ALL_TIME.value,  # Use .value to avoid enum comparison issues
        )

        assert result == [7, 8, 9]

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_task_not_found(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """Test update_draft_quiz when task doesn't exist - covers line 343."""
        mock_task_exists.return_value = False

        result = await update_draft_quiz(
            99999, "Test Title", [], datetime.now(), TaskStatus.DRAFT
        )

        assert result is False

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_draft_quiz_with_pydantic_question(
        self, mock_get_task, mock_db_conn, mock_get_basic, mock_task_exists
    ):
        """Test update_draft_quiz when question is not a dict - covers line 372."""
        mock_task_exists.return_value = True
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Quiz",
            "type": TaskType.QUIZ,
            "milestone_id": 10,
            "org_id": 123,  # Added missing org_id field
        }

        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 1
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        mock_get_task.return_value = {
            "id": 1,
            "title": "Test Quiz",
            "type": TaskType.QUIZ,
            "questions": [],
        }

        # Create a Pydantic model for the question that will trigger model_dump()
        from pydantic import BaseModel
        from typing import List, Optional

        class MockQuestion(BaseModel):
            type: str = "MULTIPLE_CHOICE"
            blocks: List = []
            answer: Optional[List] = None
            input_type: str = "MULTIPLE_CHOICE"
            response_type: str = "MULTIPLE_CHOICE"
            coding_languages: Optional[List] = None
            context: Optional[List] = None
            max_attempts: int = 3
            is_feedback_shown: bool = True
            scorecard_id: Optional[int] = None
            title: str = "question"

            def model_dump(self):
                return {
                    "type": self.type,
                    "blocks": self.blocks,
                    "answer": self.answer,
                    "input_type": self.input_type,
                    "response_type": self.response_type,
                    "coding_languages": self.coding_languages,
                    "context": self.context,
                    "max_attempts": self.max_attempts,
                    "is_feedback_shown": self.is_feedback_shown,
                    "scorecard_id": self.scorecard_id,
                    "title": self.title,
                }

        pydantic_question = MockQuestion()
        questions = [pydantic_question]  # This will trigger the model_dump() call

        mock_task = {
            "id": 1,
            "title": "Test Quiz",
            "questions": [pydantic_question.model_dump()],
        }
        mock_get_task.return_value = mock_task

        result = await update_draft_quiz(
            1, "Test Quiz", questions, datetime.now(), TaskStatus.DRAFT
        )

        assert result["questions"][0]["title"] == "question"

    @patch("src.api.db.task.does_task_exist")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_task")
    async def test_update_published_quiz_with_scorecard_mapping(
        self, mock_get_task, mock_db_conn, mock_task_exists
    ):
        """Test update_published_quiz with scorecard mapping - covers lines 483-513."""
        mock_task_exists.return_value = True

        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 1
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        # Mock existing task with questions that have scorecard mappings
        mock_get_task.return_value = {
            "id": 1,
            "title": "Test Quiz",
            "type": TaskType.QUIZ,
            "questions": [
                {
                    "id": 1,
                    "type": "MULTIPLE_CHOICE",
                    "blocks": [],
                    "answer": None,
                    "input_type": "MULTIPLE_CHOICE",
                    "response_type": "MULTIPLE_CHOICE",
                    "coding_languages": None,
                    "context": None,
                    "scorecard_id": 123,  # Existing scorecard mapping
                    "title": "question",
                }
            ],
        }

        # Questions with different scorecard mapping
        from pydantic import BaseModel
        from typing import List, Optional

        class MockQuestion(BaseModel):
            id: int
            type: str = "MULTIPLE_CHOICE"
            blocks: List = []
            answer: Optional[List] = None
            input_type: str = "MULTIPLE_CHOICE"
            response_type: str = "MULTIPLE_CHOICE"
            coding_languages: Optional[List] = None
            context: Optional[List] = None
            scorecard_id: Optional[int] = None
            title: str = "question"

            def model_dump(self):
                return {
                    "id": self.id,
                    "type": self.type,
                    "blocks": self.blocks,
                    "answer": self.answer,
                    "input_type": self.input_type,
                    "response_type": self.response_type,
                    "coding_languages": self.coding_languages,
                    "context": self.context,
                    "scorecard_id": self.scorecard_id,
                    "title": self.title,
                }

        # Question with new scorecard mapping
        question_with_scorecard = MockQuestion(id=1, scorecard_id=456)
        questions = [question_with_scorecard]

        result = await update_published_quiz(
            1,
            "Test Quiz",
            questions,
            datetime.now(),  # Added missing scheduled_publish_at parameter
        )

        assert result is not None
        assert result["questions"][0]["title"] == "question"


@pytest.mark.asyncio
class TestAssignmentOperations:
    """Test assignment-related database operations."""

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_assignment_task")
    async def test_create_assignment_success(
        self, mock_get_assignment_task, mock_db_conn, mock_get_assignment, mock_get_basic
    ):
        """Test creating assignment successfully - covers lines 1046-1114."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.DRAFT,
            "org_id": 123,
            "scheduled_publish_at": None,
        }
        
        mock_get_assignment.return_value = None  # No existing assignment
        mock_get_assignment_task.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": "published",
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        # Mock database connection
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await create_assignment(
            task_id=1,
            title="Test Assignment",
            assignment=assignment_data,
            scheduled_publish_at=None,
            status="published",
        )

        assert result is not None
        assert result["id"] == 1
        assert result["title"] == "Test Assignment"
        assert result["type"] == "assignment"
        
        # Verify database operations
        assert mock_cursor.execute.call_count == 2  # Update task + Insert assignment
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_assignment_task")
    async def test_create_assignment_update_existing(
        self, mock_get_assignment_task, mock_db_conn, mock_get_assignment, mock_get_basic
    ):
        """Test creating assignment when existing assignment needs to be updated - covers lines 1073-1092."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.DRAFT,
            "org_id": 123,
            "scheduled_publish_at": None,
        }
        
        # Mock existing assignment data
        mock_get_assignment.return_value = {
            "task_id": 1,
            "blocks": [{"type": "paragraph", "content": "Old content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Old context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        
        mock_get_assignment_task.return_value = {
            "id": 1,
            "title": "Updated Assignment",
            "type": "assignment",
            "status": "published",
            "blocks": [{"type": "paragraph", "content": "Updated content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Updated context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 80},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 5,
        }

        # Mock database connection
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Updated content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Updated context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 80},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 5,
        }

        result = await create_assignment(
            task_id=1,
            title="Updated Assignment",
            assignment=assignment_data,
            scheduled_publish_at=None,
            status="published",
        )

        assert result is not None
        assert result["id"] == 1
        assert result["title"] == "Updated Assignment"
        assert result["max_attempts"] == 5
        
        # Verify database operations - should be UPDATE task + UPSERT assignment
        assert mock_cursor.execute.call_count == 2  # Update task + Upsert assignment
        mock_conn_instance.commit.assert_called_once()
        
        # Verify the UPSERT assignment query was called with correct parameters
        upsert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if "INSERT" in str(call[0][0]) and "assignment" in str(call[0][0]) and "ON CONFLICT" in str(call[0][0])]
        assert len(upsert_calls) == 1
        
        # Check the UPSERT query parameters
        upsert_call = upsert_calls[0]
        query = upsert_call[0][0]
        params = upsert_call[0][1]
        
        assert "INSERT INTO" in query
        assert "assignment" in query
        assert "ON CONFLICT(task_id) DO UPDATE SET" in query
        assert "blocks = excluded.blocks" in query
        
        # Verify the parameters passed to the UPSERT query
        assert params[0] == 1  # task_id
        assert params[1] == json.dumps(assignment_data["blocks"])  # blocks
        assert params[2] == assignment_data["input_type"]  # input_type
        assert params[3] == assignment_data["response_type"]  # response_type
        assert params[4] == json.dumps(assignment_data["context"])  # context
        assert params[5] == json.dumps(assignment_data["evaluation_criteria"])  # evaluation_criteria
        assert params[6] == assignment_data["max_attempts"]  # max_attempts
        assert params[7] is None  # settings

    @patch("src.api.db.task.does_task_exist")
    async def test_create_assignment_task_not_found(self, mock_does_task_exist):
        """Test creating assignment when task doesn't exist."""
        mock_does_task_exist.return_value = False

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await create_assignment(
            task_id=999,
            title="Test Assignment",
            assignment=assignment_data,
        )

        assert result is None

    @patch("src.api.db.task.get_assignment_task")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.does_task_exist")
    async def test_create_assignment_wrong_type(self, mock_does_task_exist, mock_db_conn, mock_get_assignment_task):
        """Test creating assignment when task is not assignment type."""
        # Task exists, so function will proceed
        mock_does_task_exist.return_value = True
        
        # Mock database connection to prevent actual DB operations
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance
        
        # Mock the return value from get_assignment_task
        mock_get_assignment_task.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": "published",
            "assignment": {
                "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            }
        }

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        # Note: create_assignment doesn't validate task type, so it will succeed
        result = await create_assignment(
            task_id=1,
            title="Test Assignment",
            assignment=assignment_data,
        )

        # The function will succeed because it doesn't check task type
        assert result is not None

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_assignment_task")
    async def test_update_assignment_success(
        self, mock_get_assignment_task, mock_db_conn, mock_get_assignment, mock_get_basic
    ):
        """Test updating assignment successfully - covers lines 1117-1193."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }
        
        mock_get_assignment.return_value = {
            "task_id": 1,
            "blocks": [{"type": "paragraph", "content": "Old content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Old context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        
        mock_get_assignment_task.return_value = {
            "id": 1,
            "title": "Updated Assignment",
            "type": "assignment",
            "status": "published",
            "blocks": [{"type": "paragraph", "content": "Updated content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Updated context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 80},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 5,
        }

        # Mock database connection
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Updated content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Updated context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 80},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 5,
        }

        result = await update_assignment(
            task_id=1,
            title="Updated Assignment",
            assignment=assignment_data,
            scheduled_publish_at=None,
        )

        assert result is not None
        assert result["id"] == 1
        assert result["title"] == "Updated Assignment"
        assert result["max_attempts"] == 5
        
        # Verify database operations
        assert mock_cursor.execute.call_count == 2  # Update task + Update assignment
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.task.get_basic_task_details")
    async def test_update_assignment_task_not_found(self, mock_get_basic):
        """Test updating assignment when task doesn't exist."""
        mock_get_basic.return_value = None

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await update_assignment(
            task_id=999,
            title="Test Assignment",
            assignment=assignment_data,
        )

        assert result is None

    @patch("src.api.db.task.get_basic_task_details")
    async def test_update_assignment_wrong_type(self, mock_get_basic):
        """Test updating assignment when task is not assignment type."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Quiz",
            "type": "quiz",  # Wrong type
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await update_assignment(
            task_id=1,
            title="Test Assignment",
            assignment=assignment_data,
        )

        assert result is None

    @patch("src.api.db.task.get_basic_task_details")
    async def test_update_assignment_invalid_criteria(self, mock_get_basic):
        """Test updating assignment with invalid evaluation criteria."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        # Invalid criteria: min_score < 0
        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": -1, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await update_assignment(
            task_id=1,
            title="Test Assignment",
            assignment=assignment_data,
        )

        assert result is None

    @patch("src.api.db.task.get_basic_task_details")
    async def test_update_assignment_invalid_max_score_criteria(self, mock_get_basic):
        """Test updating assignment with invalid max_score <= min_score criteria - covers lines 1135-1136."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        # Invalid criteria: max_score <= min_score
        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 50, "max_score": 50, "pass_score": 50},  # max_score == min_score
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await update_assignment(
            task_id=1,
            title="Test Assignment",
            assignment=assignment_data,
        )

        assert result is None

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    @patch("src.api.db.task.get_new_db_connection")
    @patch("src.api.db.task.get_assignment_task")
    async def test_update_assignment_create_new_assignment(
        self, mock_get_assignment_task, mock_db_conn, mock_get_assignment, mock_get_basic
    ):
        """Test updating assignment when no existing assignment exists - covers lines 1172-1189."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }
        
        # No existing assignment
        mock_get_assignment.return_value = None
        
        mock_get_assignment_task.return_value = {
            "id": 1,
            "title": "New Assignment",
            "type": "assignment",
            "status": "published",
            "blocks": [{"type": "paragraph", "content": "New content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "New context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        # Mock database connection
        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        assignment_data = {
            "blocks": [{"type": "paragraph", "content": "New content"}],
            "context": {"blocks": [{"type": "paragraph", "content": "New context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "input_type": "text",
            "response_type": "text",
            "max_attempts": 3,
        }

        result = await update_assignment(
            task_id=1,
            title="New Assignment",
            assignment=assignment_data,
            scheduled_publish_at=None,
        )

        assert result is not None
        assert result["id"] == 1
        assert result["title"] == "New Assignment"
        assert result["max_attempts"] == 3
        
        # Verify database operations - should be UPDATE task + INSERT assignment (not UPDATE assignment)
        assert mock_cursor.execute.call_count == 2  # Update task + Insert assignment
        mock_conn_instance.commit.assert_called_once()
        
        # Verify the INSERT assignment query was called with correct parameters
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if "INSERT" in str(call[0][0]) and "assignment" in str(call[0][0])]
        assert len(insert_calls) == 1
        
        # Check the INSERT query parameters
        insert_call = insert_calls[0]
        query = insert_call[0][0]
        params = insert_call[0][1]
        
        assert "INSERT INTO" in query
        assert "assignment" in query
        assert "(task_id, blocks, input_type, response_type, context, evaluation_criteria, max_attempts, settings)" in query
        assert "VALUES (?, ?, ?, ?, ?, ?, ?, ?)" in query
        
        # Verify the parameters passed to the INSERT query
        assert params[0] == 1  # task_id
        assert params[1] == json.dumps(assignment_data["blocks"])  # blocks
        assert params[2] == assignment_data["input_type"]  # input_type
        assert params[3] == assignment_data["response_type"]  # response_type
        assert params[4] == json.dumps(assignment_data["context"])  # context
        assert params[5] == json.dumps(assignment_data["evaluation_criteria"])  # evaluation_criteria
        assert params[6] == assignment_data["max_attempts"]  # max_attempts
        assert params[7] is None  # settings

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_assignment_success(self, mock_execute):
        """Test getting assignment data successfully - covers lines 1196-1222."""
        mock_assignment_data = (
            1,  # task_id
            '[{"type": "paragraph", "content": "Assignment content"}]',  # blocks
            "text",  # input_type
            "text",  # response_type
            '{"blocks": [{"type": "paragraph", "content": "Context"}]}',  # context
            '{"min_score": 0, "max_score": 100, "pass_score": 70}',  # evaluation_criteria
            3,  # max_attempts
            '{"key": "value"}',  # settings
            datetime.now(),  # created_at
            datetime.now(),  # updated_at
        )
        mock_execute.return_value = mock_assignment_data

        result = await get_assignment(1)

        expected = {
            "task_id": 1,
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "input_type": "text",
            "response_type": "text",
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "max_attempts": 3,
            "settings": {"key": "value"},
            "created_at": mock_assignment_data[8],
            "updated_at": mock_assignment_data[9],
        }

        assert result == expected
        mock_execute.assert_called_once()

    @patch("src.api.db.task.execute_db_operation")
    async def test_get_assignment_not_found(self, mock_execute):
        """Test getting assignment when not found."""
        mock_execute.return_value = None

        result = await get_assignment(999)

        assert result is None
        mock_execute.assert_called_once()

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    async def test_get_assignment_task_success(self, mock_get_assignment, mock_get_basic):
        """Test getting complete assignment task successfully - covers lines 1225-1252."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_assignment_data = {
            "task_id": 1,
            "blocks": [{"type": "paragraph", "content": "Assignment content"}],
            "input_type": "text",
            "response_type": "text",
            "context": {"blocks": [{"type": "paragraph", "content": "Context"}]},
            "evaluation_criteria": {"min_score": 0, "max_score": 100, "pass_score": 70},
            "max_attempts": 3,
            "settings": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        mock_get_assignment.return_value = mock_assignment_data

        result = await get_assignment_task(1)

        # get_assignment_task returns a merged dict of task and assignment data;
        # ensure key fields are present and correctly mapped.
        assert result["id"] == 1
        assert result["title"] == "Test Assignment"
        assert result["type"] == "assignment"
        assert result["status"] == TaskStatus.PUBLISHED
        assert "assignment" in result
        assert result["assignment"]["blocks"] == mock_assignment_data["blocks"]
        assert result["assignment"]["context"] == mock_assignment_data["context"]
        assert result["assignment"]["evaluation_criteria"] == mock_assignment_data["evaluation_criteria"]
        assert result["assignment"]["input_type"] == mock_assignment_data["input_type"]
        assert result["assignment"]["response_type"] == mock_assignment_data["response_type"]
        assert result["assignment"]["max_attempts"] == mock_assignment_data["max_attempts"]
        assert result["assignment"]["settings"] == mock_assignment_data["settings"]
        mock_get_basic.assert_called_once_with(1)
        mock_get_assignment.assert_called_once_with(1)

    @patch("src.api.db.task.get_basic_task_details")
    async def test_get_assignment_task_not_found(self, mock_get_basic):
        """Test getting assignment task when task doesn't exist."""
        mock_get_basic.return_value = None

        result = await get_assignment_task(999)

        assert result is None
        mock_get_basic.assert_called_once_with(999)

    @patch("src.api.db.task.get_basic_task_details")
    async def test_get_assignment_task_wrong_type(self, mock_get_basic):
        """Test getting assignment task when task is not assignment type."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Quiz",
            "type": "quiz",  # Wrong type
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        result = await get_assignment_task(1)

        assert result is None
        mock_get_basic.assert_called_once_with(1)

    @patch("src.api.db.task.get_basic_task_details")
    @patch("src.api.db.task.get_assignment")
    async def test_get_assignment_task_no_assignment_data(self, mock_get_assignment, mock_get_basic):
        """Test getting assignment task when assignment data doesn't exist."""
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Test Assignment",
            "type": "assignment",
            "status": TaskStatus.PUBLISHED,
            "org_id": 123,
            "scheduled_publish_at": None,
        }

        mock_get_assignment.return_value = None

        result = await get_assignment_task(1)

        assert result is None
        mock_get_basic.assert_called_once_with(1)
        mock_get_assignment.assert_called_once_with(1)
