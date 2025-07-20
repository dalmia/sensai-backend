import pytest
import hashlib
from unittest.mock import patch, MagicMock
from src.api.bq.org import get_bq_client, get_org_id_from_api_key


class TestOrgBQ:
    """Test BigQuery org functionality."""

    @patch("src.api.bq.org.bigquery.Client")
    @patch("src.api.bq.org.settings")
    def test_get_bq_client(self, mock_settings, mock_bq_client):
        """Test BigQuery client creation."""
        mock_settings.google_application_credentials = "/path/to/creds.json"
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        client = get_bq_client()

        assert client == mock_client_instance
        mock_bq_client.assert_called_once()

    @patch("src.api.bq.org.get_bq_client")
    @patch("src.api.bq.org.settings")
    @pytest.mark.asyncio
    async def test_get_org_id_from_api_key_success(
        self, mock_settings, mock_get_client
    ):
        """Test successful API key validation and org ID retrieval."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # Create a valid API key and its hash
        api_key = "org__123__test_key_identifier"
        hashed_key = hashlib.sha256(api_key.encode()).hexdigest()

        mock_rows = [
            {
                "hashed_key": hashed_key,
            }
        ]
        mock_query_job.result.return_value = mock_rows

        result = await get_org_id_from_api_key(api_key)

        assert result == 123

        # Verify query was called correctly
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        query = call_args[0][0]
        assert "org_api_keys" in query.lower()
        assert "test_project.test_dataset" in query
        assert "org_id = @org_id" in query
        assert "created_at > DATETIME('2024-01-01 00:00:00')" in query

        # Check the job config
        job_config = call_args[1]["job_config"]
        assert len(job_config.query_parameters) == 1
        assert job_config.query_parameters[0].name == "org_id"
        assert job_config.query_parameters[0].value == 123

    @pytest.mark.asyncio
    async def test_get_org_id_from_api_key_invalid_format(self):
        """Test API key with invalid format."""
        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key("invalid_format")

        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key("org__invalid_id__key")

    @patch("src.api.bq.org.get_bq_client")
    @patch("src.api.bq.org.settings")
    @pytest.mark.asyncio
    async def test_get_org_id_from_api_key_no_rows(
        self, mock_settings, mock_get_client
    ):
        """Test API key validation when no matching rows found."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = []

        api_key = "org__123__test_key_identifier"

        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key(api_key)

    @patch("src.api.bq.org.get_bq_client")
    @patch("src.api.bq.org.settings")
    @pytest.mark.asyncio
    async def test_get_org_id_from_api_key_wrong_hash(
        self, mock_settings, mock_get_client
    ):
        """Test API key validation when hash doesn't match."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # Return a different hash
        wrong_hash = hashlib.sha256("different_key".encode()).hexdigest()
        mock_rows = [
            {
                "hashed_key": wrong_hash,
            }
        ]
        mock_query_job.result.return_value = mock_rows

        api_key = "org__123__test_key_identifier"

        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key(api_key)

    @patch("src.api.bq.org.get_bq_client")
    @patch("src.api.bq.org.settings")
    @pytest.mark.asyncio
    async def test_get_org_id_from_api_key_multiple_rows_correct_hash(
        self, mock_settings, mock_get_client
    ):
        """Test API key validation with multiple rows but correct hash found."""
        mock_settings.bq_project_name = "test_project"
        mock_settings.bq_dataset_name = "test_dataset"

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        # Create correct and wrong hashes
        api_key = "org__123__test_key_identifier"
        correct_hash = hashlib.sha256(api_key.encode()).hexdigest()
        wrong_hash = hashlib.sha256("different_key".encode()).hexdigest()

        # Return multiple rows with different hashes
        mock_rows = [
            {"hashed_key": wrong_hash},
            {"hashed_key": correct_hash},
            {"hashed_key": "another_wrong_hash"},
        ]
        mock_query_job.result.return_value = mock_rows

        result = await get_org_id_from_api_key(api_key)

        assert result == 123

    @pytest.mark.asyncio
    async def test_get_org_id_from_api_key_invalid_org_id_type(self):
        """Test API key with non-numeric org ID."""
        api_key = "org__invalid_org_id__test_key"

        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key(api_key)
