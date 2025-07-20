import pytest
from unittest.mock import patch, MagicMock
from src.api.bq.course import (
    get_bq_client,
    get_course_org_id,
    get_course,
)
from src.api.models import TaskType, TaskStatus, GenerateTaskJobStatus


class TestCourseBQ:
    """Test BigQuery course functionality."""

    @patch("src.api.bq.course.bigquery.Client")
    @patch("src.api.bq.course.settings")
    def test_get_bq_client(self, mock_settings, mock_bq_client):
        """Test BigQuery client creation."""
        mock_settings.google_application_credentials = "/path/to/creds.json"
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        client = get_bq_client()

        assert client == mock_client_instance
        mock_bq_client.assert_called_once()

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_org_id_success(self, mock_settings, mock_get_client):
        """Test successful course org ID retrieval."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_rows = [
            {
                "org_id": 123,
            }
        ]
        mock_query_job.result.return_value = mock_rows

        result = await get_course_org_id(1)

        assert result == 123

        # Verify query was called correctly
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        query = call_args[0][0]
        assert "courses" in query.lower()
        assert "test_project.test_dataset" in query

        # Check the job config
        job_config = call_args[1]["job_config"]
        assert len(job_config.query_parameters) == 1
        assert job_config.query_parameters[0].name == "course_id"
        assert job_config.query_parameters[0].value == 1

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_org_id_not_found(self, mock_settings, mock_get_client):
        """Test course org ID retrieval when course not found."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = []

        with pytest.raises(ValueError, match="Course not found"):
            await get_course_org_id(1)

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_success(self, mock_settings, mock_get_client):
        """Test successful course retrieval."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock three queries: course, milestones, tasks
        mock_client.query.side_effect = [
            # Course query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "name": "Test Course",
                            "course_generation_status": "completed",
                        }
                    ]
                )
            ),
            # Milestones query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "name": "Milestone 1",
                            "color": "#ff0000",
                            "ordering": 0,
                        },
                        {
                            "id": 2,
                            "name": "Milestone 2",
                            "color": "#00ff00",
                            "ordering": 1,
                        },
                    ]
                )
            ),
            # Tasks query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "title": "Task 1",
                            "type": TaskType.LEARNING_MATERIAL,
                            "status": TaskStatus.PUBLISHED,
                            "scheduled_publish_at": None,
                            "milestone_id": 1,
                            "ordering": 0,
                            "num_questions": None,
                            "task_generation_status": None,
                        },
                        {
                            "id": 2,
                            "title": "Task 2",
                            "type": TaskType.QUIZ,
                            "status": TaskStatus.PUBLISHED,
                            "scheduled_publish_at": None,
                            "milestone_id": 2,
                            "ordering": 0,
                            "num_questions": 3,
                            "task_generation_status": GenerateTaskJobStatus.STARTED,
                        },
                    ]
                )
            ),
        ]

        result = await get_course(1)

        assert result["id"] == 1
        assert result["name"] == "Test Course"
        assert result["course_generation_status"] == "completed"
        assert len(result["milestones"]) == 2

        # Check first milestone
        milestone1 = result["milestones"][0]
        assert milestone1["id"] == 1
        assert milestone1["name"] == "Milestone 1"
        assert milestone1["color"] == "#ff0000"
        assert len(milestone1["tasks"]) == 1
        assert milestone1["tasks"][0]["id"] == 1

        # Check second milestone
        milestone2 = result["milestones"][1]
        assert milestone2["id"] == 2
        assert milestone2["name"] == "Milestone 2"
        assert len(milestone2["tasks"]) == 1
        task2 = milestone2["tasks"][0]
        assert task2["id"] == 2
        assert task2["is_generating"] is True
        assert task2["num_questions"] == 3

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_not_found(self, mock_settings, mock_get_client):
        """Test course retrieval when course not found."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Course query returns empty
        mock_client.query.return_value = MagicMock(result=MagicMock(return_value=[]))

        result = await get_course(1)

        assert result is None

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_only_published_false(
        self, mock_settings, mock_get_client
    ):
        """Test course retrieval with only_published=False."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock three queries: course, milestones, tasks
        mock_client.query.side_effect = [
            # Course query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "name": "Test Course",
                            "course_generation_status": None,
                        }
                    ]
                )
            ),
            # Milestones query
            MagicMock(result=MagicMock(return_value=[])),
            # Tasks query (should include drafts when only_published=False)
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "title": "Draft Task",
                            "type": TaskType.LEARNING_MATERIAL,
                            "status": TaskStatus.DRAFT,
                            "scheduled_publish_at": "2024-01-01 12:00:00",
                            "milestone_id": 1,
                            "ordering": 0,
                            "num_questions": None,
                            "task_generation_status": None,
                        },
                    ]
                )
            ),
        ]

        result = await get_course(1, only_published=False)

        assert result["id"] == 1
        assert result["course_generation_status"] is None

        # Verify the tasks query contains no filter for published status
        tasks_query_call = mock_client.query.call_args_list[2]
        tasks_query = tasks_query_call[0][0]
        assert "t.status = 'published'" not in tasks_query
        assert "t.scheduled_publish_at IS NULL" not in tasks_query

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_empty_milestones_and_tasks(
        self, mock_settings, mock_get_client
    ):
        """Test course retrieval with no milestones and no tasks."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock three queries: course, empty milestones, empty tasks
        mock_client.query.side_effect = [
            # Course query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "name": "Empty Course",
                            "course_generation_status": "started",
                        }
                    ]
                )
            ),
            # Empty milestones query
            MagicMock(result=MagicMock(return_value=[])),
            # Empty tasks query
            MagicMock(result=MagicMock(return_value=[])),
        ]

        result = await get_course(1)

        assert result["id"] == 1
        assert result["name"] == "Empty Course"
        assert result["milestones"] == []

    @patch("src.api.bq.course.get_bq_client")
    @patch("src.api.bq.course.settings")
    @pytest.mark.asyncio
    async def test_get_course_task_without_generation_status(
        self, mock_settings, mock_get_client
    ):
        """Test course retrieval with task having no generation status."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock three queries: course, milestones, tasks
        mock_client.query.side_effect = [
            # Course query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "name": "Test Course",
                            "course_generation_status": None,
                        }
                    ]
                )
            ),
            # Milestones query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "name": "Milestone 1",
                            "color": "#ff0000",
                            "ordering": 0,
                        }
                    ]
                )
            ),
            # Tasks query
            MagicMock(
                result=MagicMock(
                    return_value=[
                        {
                            "id": 1,
                            "title": "Task 1",
                            "type": TaskType.LEARNING_MATERIAL,
                            "status": TaskStatus.PUBLISHED,
                            "scheduled_publish_at": None,
                            "milestone_id": 1,
                            "ordering": 0,
                            "num_questions": None,
                            "task_generation_status": None,
                        }
                    ]
                )
            ),
        ]

        result = await get_course(1)

        milestone = result["milestones"][0]
        task = milestone["tasks"][0]
        assert task["is_generating"] is False  # None status should result in False
