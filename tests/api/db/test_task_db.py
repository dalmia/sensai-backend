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

        mock_execute.assert_called_once()
        args = mock_execute.call_args[0]
        assert "UPDATE tasks" in args[0]
        assert "deleted_at" in args[0]

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
            """
        INSERT OR IGNORE INTO task_completions (user_id, task_id)
        VALUES (?, ?)
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

        mock_task = {"id": 1, "title": "Test Quiz", "questions": [question_with_new_scorecard.model_dump()]}
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
                        "question_type": "open_ended",
                        "blocks": [{"type": "text", "content": "Question 2?"}],
                        "correct_answer": None,
                        "answer_type": "text",
                        "context": None,
                        "coding_languages": None,
                        # No scorecard field - should be set to None
                    },
                    {
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
        
        mock_task = {"id": 1, "title": "Test Quiz", "questions": [pydantic_question.model_dump()]}
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

