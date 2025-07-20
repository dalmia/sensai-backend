import pytest
import json
from unittest.mock import patch, MagicMock, AsyncMock
from src.api.bq.task import (
    get_bq_client,
    get_scorecard,
    convert_question_bq_to_dict,
    get_basic_task_details,
    get_task,
)
from src.api.models import TaskType


class TestTaskBQ:
    """Test BigQuery task functionality."""

    @patch("src.api.bq.task.bigquery.Client")
    @patch("src.api.bq.task.settings")
    def test_get_bq_client(self, mock_settings, mock_bq_client):
        """Test BigQuery client creation."""
        mock_settings.google_application_credentials = "/path/to/creds.json"
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        client = get_bq_client()

        assert client == mock_client_instance
        mock_bq_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_scorecard_none_input(self):
        """Test get_scorecard with None input."""
        result = await get_scorecard(None)
        assert result is None

    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_scorecard_success(self, mock_settings, mock_get_client):
        """Test successful scorecard retrieval."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_rows = [
            {
                "id": 1,
                "title": "Test Scorecard",
                "criteria": '{"max_score": 10, "description": "Test criteria"}',
                "status": "published",
            }
        ]
        mock_query_job.result.return_value = mock_rows

        result = await get_scorecard(1)

        assert result["id"] == 1
        assert result["title"] == "Test Scorecard"
        assert result["criteria"] == {"max_score": 10, "description": "Test criteria"}
        assert result["status"] == "published"

        # Verify query was called correctly
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        query = call_args[0][0]
        assert "scorecard" in query.lower()
        assert "test_project.test_dataset" in query

    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_scorecard_not_found(self, mock_settings, mock_get_client):
        """Test scorecard retrieval when not found."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = []

        result = await get_scorecard(1)

        assert result is None

    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_scorecard_null_criteria(self, mock_settings, mock_get_client):
        """Test scorecard retrieval with null criteria."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_rows = [
            {
                "id": 1,
                "title": "Test Scorecard",
                "criteria": None,
                "status": "published",
            }
        ]
        mock_query_job.result.return_value = mock_rows

        result = await get_scorecard(1)

        assert result["criteria"] == []

    def test_convert_question_bq_to_dict_full_data(self):
        """Test convert_question_bq_to_dict with full data."""
        question = {
            "id": 1,
            "type": "multiple_choice",
            "blocks": '["block1", "block2"]',
            "answer": '["answer1"]',
            "input_type": "radio",
            "response_type": "exam",
            "scorecard_id": 123,
            "context": '{"type": "context"}',
            "coding_language": '["python", "javascript"]',
            "max_attempts": 3,
            "is_feedback_shown": True,
            "title": "Test Question",
        }

        result = convert_question_bq_to_dict(question)

        assert result["id"] == 1
        assert result["type"] == "multiple_choice"
        assert result["blocks"] == ["block1", "block2"]
        assert result["answer"] == ["answer1"]
        assert result["input_type"] == "radio"
        assert result["response_type"] == "exam"
        assert result["scorecard_id"] == 123
        assert result["context"] == {"type": "context"}
        assert result["coding_languages"] == ["python", "javascript"]
        assert result["max_attempts"] == 3
        assert result["is_feedback_shown"] is True
        assert result["title"] == "Test Question"

    def test_convert_question_bq_to_dict_null_data(self):
        """Test convert_question_bq_to_dict with null/empty data."""
        question = {
            "id": 1,
            "type": "multiple_choice",
            "blocks": None,
            "answer": None,
            "input_type": "radio",
            "response_type": "exam",
            "scorecard_id": None,
            "context": None,
            "coding_language": None,
            "max_attempts": None,
            "is_feedback_shown": False,
            "title": "Test Question",
        }

        result = convert_question_bq_to_dict(question)

        assert result["blocks"] == []
        assert result["answer"] is None
        assert result["context"] is None
        assert result["coding_languages"] is None

    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_basic_task_details_success(self, mock_settings, mock_get_client):
        """Test successful basic task details retrieval."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_rows = [
            {
                "id": 1,
                "title": "Test Task",
                "type": "quiz",
                "status": "published",
                "org_id": 123,
                "scheduled_publish_at": "2024-01-01 12:00:00",
            }
        ]
        mock_query_job.result.return_value = mock_rows

        result = await get_basic_task_details(1)

        assert result["id"] == 1
        assert result["title"] == "Test Task"
        assert result["type"] == "quiz"
        assert result["status"] == "published"
        assert result["org_id"] == 123
        assert result["scheduled_publish_at"] == "2024-01-01 12:00:00"

    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_basic_task_details_not_found(
        self, mock_settings, mock_get_client
    ):
        """Test basic task details retrieval when not found."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = []

        result = await get_basic_task_details(1)

        assert result is None

    @patch("src.api.bq.task.get_basic_task_details")
    @pytest.mark.asyncio
    async def test_get_task_not_found(self, mock_get_basic):
        """Test get_task when task not found."""
        mock_get_basic.return_value = None

        result = await get_task(1)

        assert result is None

    @patch("src.api.bq.task.get_basic_task_details")
    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_task_learning_material_success(
        self, mock_settings, mock_get_client, mock_get_basic
    ):
        """Test get_task for learning material task type with blocks."""
        # Mock basic task details
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Learning Material Task",
            "type": "learning_material",  # Use string value instead of enum
            "status": "published",
            "org_id": 123,
            "scheduled_publish_at": "2024-01-01 12:00:00",
        }

        # Mock BigQuery client
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_rows = [{"blocks": '["block1", "block2", "block3"]'}]
        mock_query_job.result.return_value = mock_rows

        result = await get_task(1)

        assert result["id"] == 1
        assert result["title"] == "Learning Material Task"
        assert result["type"] == "learning_material"
        assert result["blocks"] == ["block1", "block2", "block3"]

        # Verify query was called correctly
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        query = call_args[0][0]
        assert "SELECT blocks" in query
        assert "test_project.test_dataset" in query

    @patch("src.api.bq.task.get_basic_task_details")
    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_task_learning_material_null_blocks(
        self, mock_settings, mock_get_client, mock_get_basic
    ):
        """Test get_task for learning material task type with null blocks."""
        # Mock basic task details
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Learning Material Task",
            "type": "learning_material",  # Use string value instead of enum
            "status": "published",
            "org_id": 123,
            "scheduled_publish_at": "2024-01-01 12:00:00",
        }

        # Mock BigQuery client
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_rows = [{"blocks": None}]
        mock_query_job.result.return_value = mock_rows

        result = await get_task(1)

        assert result["id"] == 1
        assert result["blocks"] == []

    @patch("src.api.bq.task.get_basic_task_details")
    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_task_learning_material_no_rows(
        self, mock_settings, mock_get_client, mock_get_basic
    ):
        """Test get_task for learning material task type with no rows returned."""
        # Mock basic task details
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Learning Material Task",
            "type": "learning_material",  # Use string value instead of enum
            "status": "published",
            "org_id": 123,
            "scheduled_publish_at": "2024-01-01 12:00:00",
        }

        # Mock BigQuery client
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # No rows returned
        mock_query_job.result.return_value = []

        result = await get_task(1)

        assert result["id"] == 1
        assert "blocks" not in result  # blocks key should not be added if no rows

    @patch("src.api.bq.task.get_basic_task_details")
    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.get_scorecard")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_task_quiz_with_questions_and_scorecards(
        self, mock_settings, mock_get_scorecard, mock_get_client, mock_get_basic
    ):
        """Test get_task for quiz task type with questions and scorecards."""
        # Mock basic task details
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Quiz Task",
            "type": "quiz",  # Use string value instead of enum
            "status": "published",
            "org_id": 123,
            "scheduled_publish_at": "2024-01-01 12:00:00",
        }

        # Mock BigQuery client
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_questions = [
            {
                "id": 1,
                "type": "multiple_choice",
                "blocks": '["question1"]',
                "answer": '["answer1"]',
                "input_type": "radio",
                "response_type": "exam",
                "scorecard_id": 10,
                "context": '{"type": "context"}',
                "coding_language": '["python"]',
                "max_attempts": 3,
                "is_feedback_shown": True,
                "title": "Question 1",
            },
            {
                "id": 2,
                "type": "text",
                "blocks": '["question2"]',
                "answer": None,
                "input_type": "text",
                "response_type": "practice",
                "scorecard_id": None,
                "context": None,
                "coding_language": None,
                "max_attempts": None,
                "is_feedback_shown": False,
                "title": "Question 2",
            },
        ]
        mock_query_job.result.return_value = mock_questions

        # Mock scorecard
        mock_scorecard = {
            "id": 10,
            "title": "Test Scorecard",
            "criteria": {"max_score": 10},
            "status": "published",
        }
        mock_get_scorecard.return_value = mock_scorecard

        result = await get_task(1)

        assert result["id"] == 1
        assert result["title"] == "Quiz Task"
        assert result["type"] == "quiz"
        assert len(result["questions"]) == 2

        # Check first question with scorecard
        q1 = result["questions"][0]
        assert q1["id"] == 1
        assert q1["scorecard_id"] == 10
        assert q1["scorecard"] == mock_scorecard
        assert q1["blocks"] == ["question1"]
        assert q1["answer"] == ["answer1"]

        # Check second question without scorecard
        q2 = result["questions"][1]
        assert q2["id"] == 2
        assert q2["scorecard_id"] is None
        assert (
            "scorecard" not in q2
        )  # scorecard key should not be added if scorecard_id is None
        assert q2["blocks"] == ["question2"]
        assert q2["answer"] is None

        # Verify scorecard was only called once for the question that had scorecard_id
        mock_get_scorecard.assert_called_once_with(10)

    @patch("src.api.bq.task.get_basic_task_details")
    @patch("src.api.bq.task.get_bq_client")
    @patch("src.api.bq.task.settings")
    @pytest.mark.asyncio
    async def test_get_task_quiz_no_questions(
        self, mock_settings, mock_get_client, mock_get_basic
    ):
        """Test get_task for quiz task type with no questions."""
        # Mock basic task details
        mock_get_basic.return_value = {
            "id": 1,
            "title": "Quiz Task",
            "type": "quiz",  # Use string value instead of enum
            "status": "published",
            "org_id": 123,
            "scheduled_publish_at": "2024-01-01 12:00:00",
        }

        # Mock BigQuery client
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # No questions returned
        mock_query_job.result.return_value = []

        result = await get_task(1)

        assert result["id"] == 1
        assert result["title"] == "Quiz Task"
        assert result["type"] == "quiz"
        assert result["questions"] == []

    def test_convert_question_with_actual_function_call(self):
        """Test to ensure actual function calls are tracked for coverage."""
        # Import the actual function to ensure it's loaded for coverage
        from src.api.bq.task import convert_question_bq_to_dict

        question = {
            "id": 1,
            "type": "multiple_choice",
            "blocks": '["block1"]',
            "answer": '["answer1"]',
            "input_type": "radio",
            "response_type": "exam",
            "scorecard_id": 123,
            "context": '{"type": "context"}',
            "coding_language": '["python"]',
            "max_attempts": 3,
            "is_feedback_shown": True,
            "title": "Test Question",
        }

        # Call the actual function
        result = convert_question_bq_to_dict(question)

        assert result["id"] == 1
        assert result["blocks"] == ["block1"]
        assert result["answer"] == ["answer1"]
        assert result["scorecard_id"] == 123
