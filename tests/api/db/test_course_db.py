import pytest
import json
from unittest.mock import patch, AsyncMock, MagicMock, ANY, call
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from src.api.db.course import (
    create_course,
    get_all_courses_for_org,
    convert_course_db_to_dict,
    get_course,
    get_course_org_id,
    update_course_name,
    delete_course,
    get_tasks_for_course,
    get_milestones_for_course,
    add_milestone_to_course,
    update_milestone_orders,
    swap_milestone_ordering_for_course,
    swap_task_ordering_for_course,
    add_tasks_to_courses,
    remove_tasks_from_courses,
    update_task_orders,
    check_and_insert_missing_course_milestones,
    store_course_generation_request,
    get_course_generation_job_details,
    update_course_generation_job_status,
    update_course_generation_job_status_and_details,
    get_all_pending_course_structure_generation_jobs,
    add_course_modules,
    transfer_course_to_org,
    duplicate_course_to_org,
    get_cohorts_for_course,
    calculate_milestone_unlock_dates,
    get_courses_for_cohort,
    get_user_courses,
    drop_course_cohorts_table,
    drop_courses_table,
    delete_all_courses_for_org,
)
from src.api.models import (
    GenerateCourseJobStatus,
    TaskType,
    TaskStatus,
    ScorecardStatus,
    GenerateTaskJobStatus,
)


@pytest.mark.asyncio
class TestCourseOperations:
    """Test course-related database operations."""

    @patch("src.api.db.course.get_org_by_id")
    @patch("src.api.db.course.execute_db_operation")
    @patch("src.api.db.course.send_slack_notification_for_new_course")
    async def test_create_course_success(self, mock_slack, mock_execute, mock_get_org):
        """Test successful course creation."""
        mock_org = {"id": 1, "slug": "test-org"}
        mock_get_org.return_value = mock_org
        mock_execute.return_value = 123  # Mock course ID

        result = await create_course("Test Course", 1)

        assert result == 123
        mock_execute.assert_called_once_with(
            ANY,  # Use ANY to avoid SQL string formatting issues
            ("Test Course", 1),
            get_last_row_id=True,
        )
        mock_slack.assert_called_once_with("Test Course", 123, "test-org", 1)

    @patch("src.api.db.course.get_org_by_id")
    async def test_create_course_invalid_org(self, mock_get_org):
        """Test course creation with invalid organization."""
        mock_get_org.return_value = None

        with pytest.raises(Exception, match="Organization with id '999' not found"):
            await create_course("Test Course", 999)

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_all_courses_for_org_success(self, mock_execute):
        """Test getting all courses for an organization."""
        mock_courses = [
            (1, "Course 1"),
            (2, "Course 2"),
            (3, "Course 3"),
        ]
        mock_execute.return_value = mock_courses

        result = await get_all_courses_for_org(1)

        expected = [
            {"id": 1, "name": "Course 1"},
            {"id": 2, "name": "Course 2"},
            {"id": 3, "name": "Course 3"},
        ]

        assert result == expected

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_course_org_id_success(self, mock_execute):
        """Test getting course organization ID."""
        mock_execute.return_value = (42,)

        result = await get_course_org_id(123)

        assert result == 42
        mock_execute.assert_called_once_with(ANY, (123,), fetch_one=True)

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_course_org_id_not_found(self, mock_execute):
        """Test getting course organization ID when course doesn't exist."""
        mock_execute.return_value = None

        with pytest.raises(ValueError, match="Course not found"):
            await get_course_org_id(999)

    @patch("src.api.db.course.execute_db_operation")
    async def test_update_course_name(self, mock_execute):
        """Test updating course name."""
        await update_course_name(123, "New Course Name")

        mock_execute.assert_called_once_with(ANY, ("New Course Name", 123))

    @patch("src.api.db.course.execute_multiple_db_operations")
    async def test_delete_course(self, mock_execute_multiple):
        """Test deleting a course."""
        await delete_course(123)

        mock_execute_multiple.assert_called_once()
        # Check that multiple operations are called
        operations = mock_execute_multiple.call_args[0][0]
        assert len(operations) == 6  # Should have 6 delete operations

    @patch("src.api.db.course.execute_multiple_db_operations")
    def test_delete_all_courses_for_org(self, mock_execute_multiple):
        """Test deleting all courses for an organization."""
        delete_all_courses_for_org(123)

        mock_execute_multiple.assert_called_once()
        operations = mock_execute_multiple.call_args[0][0]
        assert len(operations) == 2

    @patch("src.api.db.course.execute_multiple_db_operations")
    def test_drop_course_cohorts_table(self, mock_execute_multiple):
        """Test dropping course cohorts table."""
        drop_course_cohorts_table()

        mock_execute_multiple.assert_called_once()

    @patch("src.api.db.course.drop_course_cohorts_table")
    @patch("src.api.db.course.execute_multiple_db_operations")
    def test_drop_courses_table(self, mock_execute_multiple, mock_drop_cohorts):
        """Test dropping courses table."""
        drop_courses_table()

        mock_drop_cohorts.assert_called_once()
        mock_execute_multiple.assert_called_once()


@pytest.mark.asyncio
class TestCourseDetails:
    """Test course detail retrieval and complex operations."""

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_course_success_published_only(self, mock_execute):
        """Test getting course with published tasks only."""
        # Mock course data
        course_data = (1, "Test Course", None)
        milestones_data = [
            (1, "Module 1", "#123456", 0),
            (2, "Module 2", "#654321", 1),
        ]
        tasks_data = [
            (
                1,
                "Task 1",
                TaskType.LEARNING_MATERIAL,
                TaskStatus.PUBLISHED,
                None,
                1,
                0,
                None,
                None,
            ),
            (2, "Task 2", TaskType.QUIZ, TaskStatus.PUBLISHED, None, 1, 1, 5, None),
            (
                3,
                "Task 3",
                TaskType.LEARNING_MATERIAL,
                TaskStatus.PUBLISHED,
                None,
                2,
                0,
                None,
                None,
            ),
        ]

        mock_execute.side_effect = [course_data, milestones_data, tasks_data]

        result = await get_course(1, only_published=True)

        expected = {
            "id": 1,
            "name": "Test Course",
            "course_generation_status": None,
            "milestones": [
                {
                    "id": 1,
                    "name": "Module 1",
                    "color": "#123456",
                    "ordering": 0,
                    "tasks": [
                        {
                            "id": 1,
                            "title": "Task 1",
                            "type": TaskType.LEARNING_MATERIAL,
                            "status": TaskStatus.PUBLISHED,
                            "scheduled_publish_at": None,
                            "ordering": 0,
                            "num_questions": None,
                            "is_generating": False,
                        },
                        {
                            "id": 2,
                            "title": "Task 2",
                            "type": TaskType.QUIZ,
                            "status": TaskStatus.PUBLISHED,
                            "scheduled_publish_at": None,
                            "ordering": 1,
                            "num_questions": 5,
                            "is_generating": False,
                        },
                    ],
                },
                {
                    "id": 2,
                    "name": "Module 2",
                    "color": "#654321",
                    "ordering": 1,
                    "tasks": [
                        {
                            "id": 3,
                            "title": "Task 3",
                            "type": TaskType.LEARNING_MATERIAL,
                            "status": TaskStatus.PUBLISHED,
                            "scheduled_publish_at": None,
                            "ordering": 0,
                            "num_questions": None,
                            "is_generating": False,
                        }
                    ],
                },
            ],
        }

        assert result == expected

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_course_not_found(self, mock_execute):
        """Test getting course when it doesn't exist."""
        mock_execute.return_value = None

        result = await get_course(999)

        assert result is None

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_course_include_unpublished(self, mock_execute):
        """Test getting course including unpublished tasks."""
        course_data = (1, "Test Course", GenerateCourseJobStatus.STARTED)
        milestones_data = []
        tasks_data = []

        mock_execute.side_effect = [course_data, milestones_data, tasks_data]

        result = await get_course(1, only_published=False)

        assert result["course_generation_status"] == GenerateCourseJobStatus.STARTED

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_tasks_for_course_with_milestone(self, mock_execute):
        """Test getting tasks for course filtered by milestone."""
        tasks_data = [
            (
                1,
                "Task 1",
                "Module 1",
                True,
                "text",
                "text",
                '["python"]',
                0,
                1,
                1,
                TaskType.LEARNING_MATERIAL,
            ),
        ]
        mock_execute.return_value = tasks_data

        result = await get_tasks_for_course(1, milestone_id=1)

        expected = [
            {
                "id": 1,
                "name": "Task 1",
                "milestone": "Module 1",
                "verified": True,
                "input_type": "text",
                "response_type": "text",
                "coding_language": ["python"],
                "ordering": 0,
                "course_task_id": 1,
                "milestone_id": 1,
                "type": TaskType.LEARNING_MATERIAL,
            }
        ]

        assert result == expected

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_tasks_for_course_all_milestones(self, mock_execute):
        """Test getting all tasks for course."""
        tasks_data = [
            (
                1,
                "Task 1",
                "Module 1",
                True,
                "text",
                "text",
                None,
                0,
                1,
                1,
                TaskType.QUIZ,
            ),
        ]
        mock_execute.return_value = tasks_data

        result = await get_tasks_for_course(1)

        assert len(result) == 1
        assert (
            result[0]["coding_language"] == []
        )  # None should be converted to empty list

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_milestones_for_course(self, mock_execute):
        """Test getting milestones for course."""
        milestones_data = [
            (1, 1, "Module 1", 0),
            (2, 2, "Module 2", 1),
        ]
        mock_execute.return_value = milestones_data

        result = await get_milestones_for_course(1)

        expected = [
            {"course_milestone_id": 1, "id": 1, "name": "Module 1", "ordering": 0},
            {"course_milestone_id": 2, "id": 2, "name": "Module 2", "ordering": 1},
        ]

        assert result == expected


@pytest.mark.asyncio
class TestMilestoneOperations:
    """Test milestone-related operations."""

    @patch("src.api.db.course.get_org_id_for_course")
    @patch("src.api.db.course.get_new_db_connection")
    async def test_add_milestone_to_course(self, mock_connection, mock_get_org):
        """Test adding milestone to course."""
        mock_get_org.return_value = 1
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_cursor.fetchone.return_value = (5,)  # max ordering
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        milestone_id, ordering = await add_milestone_to_course(
            1, "New Module", "#123456"
        )

        assert milestone_id == 123
        assert ordering == 6

    @patch("src.api.db.course.execute_many_db_operation")
    async def test_update_milestone_orders(self, mock_execute_many):
        """Test updating milestone orders."""
        milestone_orders = [(1, 1), (2, 2)]

        await update_milestone_orders(milestone_orders)

        mock_execute_many.assert_called_once_with(ANY, params_list=milestone_orders)

    @patch("src.api.db.course.execute_db_operation")
    @patch("src.api.db.course.execute_many_db_operation")
    async def test_swap_milestone_ordering_success(
        self, mock_execute_many, mock_execute
    ):
        """Test swapping milestone ordering successfully."""
        milestone_entries = [(1, 0), (2, 1)]
        mock_execute.return_value = milestone_entries

        await swap_milestone_ordering_for_course(1, 1, 2)

        mock_execute_many.assert_called_once()

    @patch("src.api.db.course.execute_db_operation")
    async def test_swap_milestone_ordering_invalid_milestones(self, mock_execute):
        """Test swapping milestone ordering with invalid milestones."""
        mock_execute.return_value = [(1, 0)]  # Only one milestone found

        with pytest.raises(ValueError, match="One or both milestones do not exist"):
            await swap_milestone_ordering_for_course(1, 1, 2)


@pytest.mark.asyncio
class TestTaskOperations:
    """Test task-related operations."""

    @patch("src.api.db.course.execute_many_db_operation")
    async def test_update_task_orders(self, mock_execute_many):
        """Test updating task orders."""
        task_orders = [(1, 1), (2, 2)]

        await update_task_orders(task_orders)

        mock_execute_many.assert_called_once_with(ANY, params_list=task_orders)

    @patch("src.api.db.course.execute_db_operation")
    @patch("src.api.db.course.execute_many_db_operation")
    async def test_swap_task_ordering_success(self, mock_execute_many, mock_execute):
        """Test swapping task ordering successfully."""
        task_entries = [(1, 1, 0), (2, 1, 1)]  # task_id, milestone_id, ordering
        mock_execute.return_value = task_entries

        await swap_task_ordering_for_course(1, 1, 2)

        mock_execute_many.assert_called_once()

    @patch("src.api.db.course.execute_db_operation")
    async def test_swap_task_ordering_invalid_tasks(self, mock_execute):
        """Test swapping task ordering with invalid tasks."""
        mock_execute.return_value = [(1, 1, 0)]  # Only one task found

        with pytest.raises(ValueError, match="One or both tasks do not exist"):
            await swap_task_ordering_for_course(1, 1, 2)

    @patch("src.api.db.course.execute_db_operation")
    async def test_swap_task_ordering_different_milestones(self, mock_execute):
        """Test swapping task ordering with tasks in different milestones."""
        task_entries = [(1, 1, 0), (2, 2, 1)]  # Different milestone_ids
        mock_execute.return_value = task_entries

        with pytest.raises(ValueError, match="Tasks are not in the same milestone"):
            await swap_task_ordering_for_course(1, 1, 2)

    @patch("src.api.db.course.check_and_insert_missing_course_milestones")
    @patch("src.api.db.course.get_new_db_connection")
    async def test_add_tasks_to_courses(self, mock_connection, mock_check_milestones):
        """Test adding tasks to courses."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (2,)  # max ordering
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        course_tasks = [(1, 1, 1), (2, 1, 1)]  # task_id, course_id, milestone_id

        await add_tasks_to_courses(course_tasks)

        mock_check_milestones.assert_called_once_with(course_tasks)

    @patch("src.api.db.course.execute_many_db_operation")
    async def test_remove_tasks_from_courses(self, mock_execute_many):
        """Test removing tasks from courses."""
        course_tasks = [(1, 1), (2, 1)]  # task_id, course_id

        await remove_tasks_from_courses(course_tasks)

        mock_execute_many.assert_called_once_with(ANY, params_list=course_tasks)

    @patch("src.api.db.course.execute_db_operation")
    async def test_check_and_insert_missing_course_milestones_with_missing(
        self, mock_execute
    ):
        """Test checking and inserting missing course milestones."""
        course_tasks = [(1, 1, 1), (2, 1, 2)]
        mock_execute.side_effect = [
            [],  # No existing milestones found
            (5,),  # Max ordering for first milestone
            None,  # Insert first milestone
            (6,),  # Max ordering for second milestone
            None,  # Insert second milestone
        ]

        await check_and_insert_missing_course_milestones(course_tasks)

        # Should be called 5 times: 1 check + 2 pairs × (1 max query + 1 insert)
        assert mock_execute.call_count == 5

    @patch("src.api.db.course.execute_db_operation")
    async def test_check_and_insert_missing_course_milestones_all_exist(
        self, mock_execute
    ):
        """Test checking milestones when all exist."""
        course_tasks = [(1, 1, 1), (2, 1, 1)]
        mock_execute.return_value = [(1, 1)]  # All milestones exist

        await check_and_insert_missing_course_milestones(course_tasks)

        # Should only be called once for the check
        mock_execute.assert_called_once()

    @patch("src.api.db.course.execute_db_operation")
    async def test_check_and_insert_missing_course_milestones_no_milestones(
        self, mock_execute
    ):
        """Test checking milestones when no milestone IDs provided."""
        course_tasks = [(1, 1, None), (2, 1, None)]

        await check_and_insert_missing_course_milestones(course_tasks)

        # Should not be called since no milestone IDs
        mock_execute.assert_not_called()


@pytest.mark.asyncio
class TestCourseGeneration:
    """Test course generation job operations."""

    @patch("src.api.db.course.get_new_db_connection")
    async def test_store_course_generation_request(self, mock_connection):
        """Test storing course generation request."""
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        job_details = {"prompt": "Generate a Python course"}
        result = await store_course_generation_request(1, job_details)

        assert isinstance(result, str)
        mock_cursor.execute.assert_called_once()

    @patch("src.api.db.course.get_new_db_connection")
    async def test_get_course_generation_job_details_success(self, mock_connection):
        """Test getting course generation job details successfully."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = ('{"prompt": "Generate course"}',)
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        result = await get_course_generation_job_details("test-uuid")

        assert result == {"prompt": "Generate course"}

    @patch("src.api.db.course.get_new_db_connection")
    async def test_get_course_generation_job_details_not_found(self, mock_connection):
        """Test getting course generation job details when not found."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        with pytest.raises(ValueError, match="Job not found"):
            await get_course_generation_job_details("invalid-uuid")

    @patch("src.api.db.course.get_new_db_connection")
    async def test_update_course_generation_job_status(self, mock_connection):
        """Test updating course generation job status."""
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        await update_course_generation_job_status(
            "test-uuid", GenerateCourseJobStatus.COMPLETED
        )

        mock_cursor.execute.assert_called_once()

    @patch("src.api.db.course.get_new_db_connection")
    async def test_update_course_generation_job_status_and_details(
        self, mock_connection
    ):
        """Test updating course generation job status and details."""
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        details = {"progress": 50}
        await update_course_generation_job_status_and_details(
            "test-uuid", GenerateCourseJobStatus.STARTED, details
        )

        mock_cursor.execute.assert_called_once()

    @patch("src.api.db.course.get_new_db_connection")
    async def test_get_all_pending_course_structure_generation_jobs(
        self, mock_connection
    ):
        """Test getting all pending course generation jobs."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [
            ("uuid1", 1, '{"prompt": "Course 1"}'),
            ("uuid2", 2, '{"prompt": "Course 2"}'),
        ]
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        result = await get_all_pending_course_structure_generation_jobs()

        expected = [
            {"uuid": "uuid1", "course_id": 1, "job_details": {"prompt": "Course 1"}},
            {"uuid": "uuid2", "course_id": 2, "job_details": {"prompt": "Course 2"}},
        ]

        assert result == expected


@pytest.mark.asyncio
class TestCourseModules:
    """Test course module operations."""

    @patch("src.api.db.course.add_milestone_to_course")
    async def test_add_course_modules(self, mock_add_milestone):
        """Test adding course modules."""
        mock_add_milestone.side_effect = [(1, 0), (2, 1), (3, 2)]
        modules = [
            {"name": "Module 1"},
            {"name": "Module 2"},
            {"name": "Module 3"},
        ]

        result = await add_course_modules(1, modules)

        assert result == [1, 2, 3]
        assert mock_add_milestone.call_count == 3


@pytest.mark.asyncio
class TestCourseTransfer:
    """Test course transfer operations."""

    @patch("src.api.db.course.execute_db_operation")
    async def test_transfer_course_to_org(self, mock_execute):
        """Test transferring course to organization."""
        milestones = [(1,), (2,)]
        tasks = [(10,), (11,)]
        questions = [(100,), (101,)]
        scorecards = [(1000,), (1001,)]

        mock_execute.side_effect = [
            None,  # Update course
            milestones,  # Get milestones
            None,  # Update milestone 1
            None,  # Update milestone 2
            tasks,  # Get tasks
            questions,  # Get questions
            scorecards,  # Get scorecards
            None,  # Update scorecards
            None,  # Update tasks
        ]

        await transfer_course_to_org(1, 123)

        # Should call execute_db_operation multiple times
        assert mock_execute.call_count == 9

    @patch("src.api.db.course.get_course")
    @patch("src.api.db.course.create_course")
    @patch("src.api.db.course.add_milestone_to_course")
    @patch("src.api.db.course.create_draft_task_for_course")
    @patch("src.api.db.course.get_task")
    @patch("src.api.db.course.update_learning_material_task")
    @patch("src.api.db.course.update_draft_quiz")
    @patch("src.api.db.course.get_scorecard")
    @patch("src.api.db.course.create_scorecard")
    @patch("src.api.db.utils.execute_db_operation")
    async def test_duplicate_course_to_org(
        self,
        mock_execute_db_operation,
        mock_create_scorecard,
        mock_get_scorecard,
        mock_update_quiz,
        mock_update_learning,
        mock_get_task,
        mock_create_task,
        mock_add_milestone,
        mock_create_course,
        mock_get_course,
    ):
        """Test duplicating course to organization."""
        # Mock course structure
        course_data = {
            "name": "Test Course",
            "milestones": [
                {
                    "name": "Module 1",
                    "color": "#123456",
                    "tasks": [
                        {"id": 1, "type": "learning_material"},
                        {"id": 2, "type": "quiz"},
                    ],
                }
            ],
        }

        learning_task = {
            "id": 1,
            "title": "Learning Task",
            "type": "learning_material",
            "status": "published",
            "org_id": 999,
            "scheduled_publish_at": None,
            "blocks": [],
        }

        quiz_task = {
            "id": 2,
            "title": "Quiz Task",
            "type": "quiz",
            "status": "published",
            "org_id": 999,
            "scheduled_publish_at": None,
            "questions": [
                {
                    "id": 1,
                    "type": "multiple_choice",
                    "blocks": [],
                    "answer": None,
                    "input_type": "multiple_choice",
                    "response_type": "single",
                    "scorecard_id": 1,
                    "context": None,
                    "coding_languages": None,
                    "max_attempts": 3,
                    "is_feedback_shown": True,
                    "text": "Question 1",
                },
                {
                    "id": 2,
                    "type": "multiple_choice",
                    "blocks": [],
                    "answer": None,
                    "input_type": "multiple_choice",
                    "response_type": "single",
                    "scorecard_id": 1,
                    "context": None,
                    "coding_languages": None,
                    "max_attempts": 3,
                    "is_feedback_shown": True,
                    "text": "Question 2",
                },
            ],
        }

        original_scorecard = {
            "title": "Original Scorecard",
            "criteria": [],
        }

        new_scorecard = {"id": 123}

        # Mock the database call for getting org_id for the new course (ID 456)
        # The get_org_id_for_course function expects a tuple with the org_id value
        mock_execute_db_operation.return_value = (999,)

        mock_get_course.return_value = course_data
        mock_create_course.return_value = 456
        mock_add_milestone.return_value = (789, 0)
        mock_create_task.side_effect = [(10, None), (11, None)]
        mock_get_task.side_effect = [learning_task, quiz_task]
        mock_get_scorecard.return_value = original_scorecard
        mock_create_scorecard.return_value = new_scorecard

        await duplicate_course_to_org(1, 999)

        mock_get_course.assert_called_once_with(1)
        mock_create_course.assert_called_once_with("Test Course", 999)
        mock_update_learning.assert_called_once()
        mock_update_quiz.assert_called_once()


@pytest.mark.asyncio
class TestCohortCourseRelations:
    """Test cohort-course relationship operations."""

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_cohorts_for_course(self, mock_execute):
        """Test getting cohorts for course."""
        cohorts_data = [
            (1, "Cohort 1", True, 7, "days", "2024-01-01"),
            (2, "Cohort 2", False, None, None, None),
        ]
        mock_execute.return_value = cohorts_data

        result = await get_cohorts_for_course(1)

        expected = [
            {
                "id": 1,
                "name": "Cohort 1",
                "drip_config": {
                    "is_drip_enabled": True,
                    "frequency_value": 7,
                    "frequency_unit": "days",
                    "publish_at": "2024-01-01",
                },
            },
            {
                "id": 2,
                "name": "Cohort 2",
                "drip_config": {
                    "is_drip_enabled": False,
                    "frequency_value": None,
                    "frequency_unit": None,
                    "publish_at": None,
                },
            },
        ]

        assert result == expected

    @patch("src.api.db.course.execute_db_operation")
    async def test_get_courses_for_cohort_without_tree(self, mock_execute):
        """Test getting courses for cohort without tree structure."""
        courses_data = [
            (1, "Course 1", True, 7, "days", "2024-01-01"),
            (2, "Course 2", False, None, None, None),
        ]
        mock_execute.return_value = courses_data

        result = await get_courses_for_cohort(1, include_tree=False)

        expected = [
            {
                "id": 1,
                "name": "Course 1",
                "drip_config": {
                    "is_drip_enabled": True,
                    "frequency_value": 7,
                    "frequency_unit": "days",
                    "publish_at": "2024-01-01",
                },
            },
            {
                "id": 2,
                "name": "Course 2",
                "drip_config": {
                    "is_drip_enabled": False,
                    "frequency_value": None,
                    "frequency_unit": None,
                    "publish_at": None,
                },
            },
        ]

        assert result == expected

    @patch("src.api.db.course.get_course")
    @patch("src.api.db.course.calculate_milestone_unlock_dates")
    @patch("src.api.db.course.execute_db_operation")
    async def test_get_courses_for_cohort_with_tree(
        self, mock_execute, mock_calculate_unlock, mock_get_course
    ):
        """Test getting courses for cohort with tree structure."""
        courses_data = [
            (1, "Course 1", True, 7, "days", "2024-01-01"),
        ]
        course_details = {"id": 1, "name": "Course 1", "milestones": []}
        calculated_course = {"id": 1, "name": "Course 1", "milestones": []}

        mock_execute.return_value = courses_data
        mock_get_course.return_value = course_details
        mock_calculate_unlock.return_value = calculated_course

        joined_at = datetime.now(timezone.utc)
        result = await get_courses_for_cohort(1, include_tree=True, joined_at=joined_at)

        assert len(result) == 1
        mock_get_course.assert_called_once_with(1)
        mock_calculate_unlock.assert_called_once()


class TestMilestoneUnlockDates:
    """Test milestone unlock date calculations."""

    async def test_calculate_milestone_unlock_dates_drip_disabled(self):
        """Test milestone unlock dates when drip is disabled."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
            ]
        }
        drip_config = {"is_drip_enabled": False}

        result = await calculate_milestone_unlock_dates(course_details, drip_config)

        for milestone in result["milestones"]:
            assert milestone["unlock_at"] is None

    async def test_calculate_milestone_unlock_dates_drip_enabled_with_publish_at(self):
        """Test milestone unlock dates with drip enabled and publish_at set."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
                {"name": "Module 3", "tasks": []},  # Empty module
            ]
        }
        # Use a future date so the unlock logic works correctly
        future_date = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

        drip_config = {
            "is_drip_enabled": True,
            "frequency_value": 1,
            "frequency_unit": "day",
            "publish_at": future_date,
        }

        result = await calculate_milestone_unlock_dates(course_details, drip_config)

        # First module should be unlocked (no unlock_at)
        assert result["milestones"][0]["unlock_at"] is None
        # Second module should be locked
        assert result["milestones"][1]["unlock_at"] is not None
        # Empty module should be unlocked
        assert result["milestones"][2]["unlock_at"] is None

    async def test_calculate_milestone_unlock_dates_different_frequency_units(self):
        """Test milestone unlock dates with different frequency units."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
            ]
        }

        # Test different frequency units
        frequency_units = ["minute", "hour", "day", "week", "month", "year"]

        for unit in frequency_units:
            drip_config = {
                "is_drip_enabled": True,
                "frequency_value": 1,
                "frequency_unit": unit,
                "publish_at": "2024-01-01T00:00:00+00:00",
            }

            result = await calculate_milestone_unlock_dates(
                course_details.copy(), drip_config
            )

            # Should not raise an error and should process correctly
            assert len(result["milestones"]) == 2

    async def test_calculate_milestone_unlock_dates_invalid_frequency_unit(self):
        """Test milestone unlock dates with invalid frequency unit."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
            ]
        }
        drip_config = {
            "is_drip_enabled": True,
            "frequency_value": 1,
            "frequency_unit": "invalid",
            "publish_at": "2024-01-01T00:00:00+00:00",
        }

        with pytest.raises(ValueError, match="Invalid frequency unit"):
            await calculate_milestone_unlock_dates(course_details, drip_config)

    async def test_calculate_milestone_unlock_dates_with_joined_at(self):
        """Test milestone unlock dates using joined_at instead of publish_at."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
            ]
        }
        drip_config = {
            "is_drip_enabled": True,
            "frequency_value": 1,
            "frequency_unit": "day",
            # No publish_at
        }

        joined_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = await calculate_milestone_unlock_dates(
            course_details, drip_config, joined_at
        )

        assert len(result["milestones"]) == 2

    async def test_calculate_milestone_unlock_dates_no_drip_config(self):
        """Test milestone unlock dates with no drip config."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
            ]
        }

        result = await calculate_milestone_unlock_dates(course_details, None)

        assert result["milestones"][0]["unlock_at"] is None

    async def test_calculate_milestone_unlock_dates_timezone_naive_joined_at(self):
        """Test milestone unlock dates with timezone-naive joined_at."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
            ]
        }
        drip_config = {
            "is_drip_enabled": True,
            "frequency_value": 1,
            "frequency_unit": "day",
            # No publish_at, will use joined_at
        }

        # Pass a timezone-naive datetime for joined_at
        joined_at_naive = datetime(2024, 1, 1)  # No tzinfo
        result = await calculate_milestone_unlock_dates(
            course_details, drip_config, joined_at_naive
        )

        assert len(result["milestones"]) == 2
        # First module should be unlocked
        assert result["milestones"][0]["unlock_at"] is None

    async def test_calculate_milestone_unlock_dates_timezone_naive_publish_at(self):
        """Test milestone unlock dates with timezone-naive publish_at."""
        course_details = {
            "milestones": [
                {"name": "Module 1", "tasks": [{"id": 1}]},
                {"name": "Module 2", "tasks": [{"id": 2}]},
            ]
        }
        drip_config = {
            "is_drip_enabled": True,
            "frequency_value": 1,
            "frequency_unit": "day",
            "publish_at": "2024-01-01T00:00:00",  # No timezone info in ISO string
        }

        result = await calculate_milestone_unlock_dates(course_details, drip_config)

        assert len(result["milestones"]) == 2


@pytest.mark.asyncio
class TestUserCourses:
    """Test user course operations."""

    @patch("src.api.db.course.get_new_db_connection")
    @patch("src.api.db.course.get_user_cohorts")
    @patch("src.api.db.course.get_courses_for_cohort")
    @patch("src.api.db.course.get_user_organizations")
    @patch("src.api.db.course.get_all_courses_for_org")
    async def test_get_user_courses_comprehensive(
        self,
        mock_get_org_courses,
        mock_get_user_orgs,
        mock_get_cohort_courses,
        mock_get_user_cohorts,
        mock_connection,
    ):
        """Test getting user courses with multiple roles."""
        # Mock database connection
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.side_effect = [
            (1, "Course 1", 1, "Org 1", "org-1"),
            (2, "Course 2", 1, "Org 1", "org-1"),
            (3, "Course 3", 2, "Org 2", "org-2"),
        ]
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        # Mock user cohorts
        mock_get_user_cohorts.return_value = [
            {"id": 1, "role": "learner"},
            {"id": 2, "role": "mentor"},
        ]

        # Mock cohort courses
        mock_get_cohort_courses.side_effect = [
            [{"id": 1, "name": "Course 1"}],
            [{"id": 2, "name": "Course 2"}],
        ]

        # Mock user organizations
        mock_get_user_orgs.return_value = [
            {"id": 2, "role": "admin"},
        ]

        # Mock organization courses
        mock_get_org_courses.return_value = [
            {"id": 3, "name": "Course 3"},
        ]

        result = await get_user_courses(123)

        assert len(result) == 3
        assert result[0]["role"] == "learner"
        assert result[1]["role"] == "mentor"
        assert result[2]["role"] == "admin"

    @patch("src.api.db.course.get_new_db_connection")
    @patch("src.api.db.course.get_user_cohorts")
    @patch("src.api.db.course.get_user_organizations")
    async def test_get_user_courses_no_courses(
        self, mock_get_user_orgs, mock_get_user_cohorts, mock_connection
    ):
        """Test getting user courses when user has no courses."""
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        mock_get_user_cohorts.return_value = []
        mock_get_user_orgs.return_value = []

        result = await get_user_courses(123)

        assert result == []


class TestCourseUtilityFunctions:
    """Test course utility and conversion functions."""

    def test_convert_course_db_to_dict_simple(self):
        """Test converting simple course tuple to dictionary."""
        course_tuple = (1, "Test Course")

        result = convert_course_db_to_dict(course_tuple)

        expected = {"id": 1, "name": "Test Course"}

        assert result == expected

    def test_convert_course_db_to_dict_with_org(self):
        """Test converting course tuple with organization data."""
        course_tuple = (1, "Test Course", 1, "Test Org", "test-org")

        result = convert_course_db_to_dict(course_tuple)

        expected = {
            "id": 1,
            "name": "Test Course",
            "org": {"id": 1, "name": "Test Org", "slug": "test-org"},
        }

        assert result == expected

    def test_convert_course_db_to_dict_extended_simple(self):
        """Test converting extended course DB tuple to dict."""
        course_tuple = (1, "Course 1", 1, "Test Org", "test-org")

        result = convert_course_db_to_dict(course_tuple)

        assert result["id"] == 1
        assert result["name"] == "Course 1"
        assert "org" in result
        assert result["org"]["id"] == 1
        assert result["org"]["name"] == "Test Org"
        assert result["org"]["slug"] == "test-org"

    def test_convert_course_db_to_dict_with_org_details(self):
        """Test converting course DB tuple with organization details."""
        # The function only handles (id, name) or (id, name, org_id, org_name, org_slug) patterns
        course_tuple = (
            1,
            "Course 1",
            1,
            "Org 1",
            "org-1",
        )

        result = convert_course_db_to_dict(course_tuple)

        assert result["id"] == 1
        assert result["name"] == "Course 1"
        assert "org" in result
        assert result["org"]["id"] == 1
        assert result["org"]["name"] == "Org 1"
        assert result["org"]["slug"] == "org-1"
