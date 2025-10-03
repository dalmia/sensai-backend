import pytest
import os
from unittest.mock import patch, MagicMock
from src.api.bq.base import get_bq_client
from src.api.bq.chat import get_all_chat_history


class TestChatBQ:
    """Test BigQuery chat functionality."""

    @patch("src.api.bq.base.bigquery.Client")
    @patch("src.api.bq.base.settings")
    def test_get_bq_client(self, mock_settings, mock_bq_client):
        """Test BigQuery client creation."""
        mock_settings.google_application_credentials = "/path/to/creds.json"
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        with patch.dict(os.environ, {}, clear=True):
            client = get_bq_client()

            assert client == mock_client_instance
            mock_bq_client.assert_called_once()
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/path/to/creds.json"

    @patch("src.api.bq.chat.get_bq_client")
    @patch("src.api.bq.chat.settings")
    @pytest.mark.asyncio
    async def test_get_all_chat_history_success(self, mock_settings, mock_get_client):
        """Test successful retrieval of chat history."""
        # Setup mock settings
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        # Setup mock BigQuery client and query results
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock query job and results
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # Create mock datetime objects
        mock_created_at_1 = MagicMock()
        mock_created_at_1.strftime.return_value = "2024-01-01 12:00:00"

        mock_created_at_2 = MagicMock()
        mock_created_at_2.strftime.return_value = "2024-01-01 12:01:00"

        # Mock the query results
        mock_rows = [
            {
                "id": 1,
                "created_at": mock_created_at_1,
                "user_id": 101,
                "user_email": "test@example.com",
                "question_id": 201,
                "task_id": 301,
                "role": "user",
                "content": "Test message",
                "response_type": "text",
                "course_id": 401,
            },
            {
                "id": 2,
                "created_at": mock_created_at_2,
                "user_id": 102,
                "user_email": "test2@example.com",
                "question_id": 202,
                "task_id": 302,
                "role": "assistant",
                "content": "Test response",
                "response_type": "text",
                "course_id": 402,
            },
        ]
        mock_query_job.result.return_value = mock_rows

        org_id = 123

        # Collect results from async generator
        result = []
        async for message in get_all_chat_history(org_id):
            result.append(message)

        # Verify the query was called with correct parameters
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args

        # Check that the query contains the expected elements
        query = call_args[0][0]
        assert "test_project.test_dataset" in query
        assert "chat_history" in query
        assert "questions" in query
        assert "tasks" in query
        assert "users" in query
        assert "course_tasks" in query
        assert "task.deleted_at IS NULL" in query
        assert "task.org_id = @org_id" in query
        assert "ORDER BY message.created_at ASC" in query

        # Check the job config
        job_config = call_args[1]["job_config"]
        assert len(job_config.query_parameters) == 1
        assert job_config.query_parameters[0].name == "org_id"
        assert job_config.query_parameters[0].value == org_id

        # Verify the result
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["created_at"] == "2024-01-01 12:00:00"
        assert result[0]["user_email"] == "test@example.com"
        assert result[0]["content"] == "Test message"
        assert result[1]["id"] == 2
        assert result[1]["created_at"] == "2024-01-01 12:01:00"
        assert result[1]["user_email"] == "test2@example.com"
        assert result[1]["content"] == "Test response"

    @patch("src.api.bq.chat.get_bq_client")
    @patch("src.api.bq.chat.settings")
    @pytest.mark.asyncio
    async def test_get_all_chat_history_empty_result(
        self, mock_settings, mock_get_client
    ):
        """Test chat history retrieval with empty results."""
        # Setup mock settings
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        # Setup mock BigQuery client and query results
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock query job with empty results
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = []

        org_id = 123

        # Collect results from async generator
        result = []
        async for message in get_all_chat_history(org_id):
            result.append(message)

        # Verify empty result
        assert result == []
        mock_client.query.assert_called_once()

    @patch("src.api.bq.chat.get_bq_client")
    @patch("src.api.bq.chat.settings")
    @pytest.mark.asyncio
    async def test_get_all_chat_history_with_none_values(
        self, mock_settings, mock_get_client
    ):
        """Test chat history retrieval with None values in the response."""
        # Setup mock settings
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        # Setup mock BigQuery client and query results
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock query job and results with None values
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # Create mock datetime object
        mock_created_at = MagicMock()
        mock_created_at.strftime.return_value = "2024-01-01 12:00:00"

        # Mock the query results with None values
        mock_rows = [
            {
                "id": 1,
                "created_at": mock_created_at,
                "user_id": 101,
                "user_email": "test@example.com",
                "question_id": 201,
                "task_id": 301,
                "role": "user",
                "content": "Test message",
                "response_type": "text",
                "course_id": None,  # None value
            }
        ]
        mock_query_job.result.return_value = mock_rows

        org_id = 123

        # Collect results from async generator
        result = []
        async for message in get_all_chat_history(org_id):
            result.append(message)

        # Verify the result handles None values
        assert len(result) == 1
        assert result[0]["course_id"] is None

    @patch("src.api.bq.chat.get_bq_client")
    @pytest.mark.asyncio
    async def test_get_all_chat_history_query_exception(self, mock_get_client):
        """Test chat history retrieval when BigQuery raises an exception."""
        # Setup mock BigQuery client that raises an exception
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.query.side_effect = Exception("BigQuery error")

        org_id = 123

        # The function should let the exception bubble up
        with pytest.raises(Exception, match="BigQuery error"):
            # Try to iterate through the async generator to trigger the exception
            async for message in get_all_chat_history(org_id):
                pass

    @patch("src.api.bq.base.bigquery")
    @patch("src.api.bq.base.settings")
    def test_get_bq_client_sets_credentials(self, mock_settings, mock_bigquery):
        """Test that get_bq_client properly sets the credentials environment variable."""
        mock_settings.google_application_credentials = "/custom/path/to/creds.json"
        mock_client_instance = MagicMock()
        mock_bigquery.Client.return_value = mock_client_instance

        with patch.dict(os.environ, {}, clear=True):
            client = get_bq_client()

            # Verify credentials were set
            assert (
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
                == "/custom/path/to/creds.json"
            )
            assert client == mock_client_instance
            mock_bigquery.Client.assert_called_once()
