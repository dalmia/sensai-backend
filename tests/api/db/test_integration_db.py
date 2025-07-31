import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime, timezone
from src.api.db.integration import (
    create_integration,
    get_integration,
    list_integrations,
    update_integration,
    delete_integration,
)
from src.api.models import CreateIntegrationRequest, UpdateIntegrationRequest, Integration


@pytest.mark.asyncio
class TestIntegrationDatabaseOperations:
    """Test integration-related database operations."""

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_create_integration_new_integration(self, mock_db_conn):
        """Test creating a new integration successfully."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 1
        mock_cursor.fetchone.return_value = (1,)  # ID of the created integration
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        # Test data
        data = CreateIntegrationRequest(
            user_id=1,
            integration_type="slack",
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )

        result = await create_integration(data)

        assert result == 1
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_create_integration_existing_integration(self, mock_db_conn):
        """Test updating an existing integration via upsert."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 1
        # First call returns existing ID, second call returns the same ID
        mock_cursor.fetchone.return_value = (2,)  # ID of the existing integration
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        # Test data
        data = CreateIntegrationRequest(
            user_id=1,
            integration_type="slack",
            access_token="updated_access_token",
            refresh_token="updated_refresh_token",
            expires_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )

        result = await create_integration(data)

        assert result == 2
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_get_integration_success(self, mock_db_conn):
        """Test successful retrieval of integration by ID."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (
            1,  # id
            1,  # user_id
            "slack",  # integration_type
            "test_access_token",  # access_token
            "test_refresh_token",  # refresh_token
            "2024-01-01 12:00:00",  # expires_at
            "2024-01-01 10:00:00",  # created_at
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await get_integration(1)

        assert result is not None
        assert result.id == 1
        assert result.user_id == 1
        assert result.integration_type == "slack"
        assert result.access_token == "test_access_token"
        assert result.refresh_token == "test_refresh_token"

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_get_integration_not_found(self, mock_db_conn):
        """Test retrieval of non-existent integration."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await get_integration(999)

        assert result is None

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_list_integrations_all(self, mock_db_conn):
        """Test listing all integrations."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [
            (1, 1, "slack", "token1", "refresh1", "2024-01-01 12:00:00", "2024-01-01 10:00:00"),
            (2, 1, "github", "token2", "refresh2", "2024-01-01 12:00:00", "2024-01-01 10:00:00"),
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await list_integrations()

        assert len(result) == 2
        assert result[0].id == 1
        assert result[0].integration_type == "slack"
        assert result[1].id == 2
        assert result[1].integration_type == "github"

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_list_integrations_by_user_id(self, mock_db_conn):
        """Test listing integrations filtered by user ID."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [
            (1, 1, "slack", "token1", "refresh1", "2024-01-01 12:00:00", "2024-01-01 10:00:00"),
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await list_integrations(user_id=1)

        assert len(result) == 1
        assert result[0].user_id == 1
        assert result[0].integration_type == "slack"

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_list_integrations_empty(self, mock_db_conn):
        """Test listing integrations when none exist."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await list_integrations()

        assert len(result) == 0

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_update_integration_success(self, mock_db_conn):
        """Test successful update of integration."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1  # One row affected
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        # Test data
        data = UpdateIntegrationRequest(
            access_token="updated_access_token",
            refresh_token="updated_refresh_token",
            expires_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )

        result = await update_integration(1, data)

        assert result is True
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_update_integration_not_found(self, mock_db_conn):
        """Test update of non-existent integration."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 0  # No rows affected
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        # Test data
        data = UpdateIntegrationRequest(
            access_token="updated_access_token",
            refresh_token="updated_refresh_token",
            expires_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )

        result = await update_integration(999, data)

        assert result is False

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_update_integration_partial_update(self, mock_db_conn):
        """Test partial update of integration with only some fields."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        # Test data with only access_token
        data = UpdateIntegrationRequest(
            access_token="updated_access_token",
            refresh_token=None,
            expires_at=None
        )

        result = await update_integration(1, data)

        assert result is True
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_delete_integration_success(self, mock_db_conn):
        """Test successful deletion of integration."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1  # One row affected
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await delete_integration(1)

        assert result is True
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_delete_integration_not_found(self, mock_db_conn):
        """Test deletion of non-existent integration."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 0  # No rows affected
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await delete_integration(999)

        assert result is False

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_create_integration_with_null_values(self, mock_db_conn):
        """Test creating integration with null refresh_token and expires_at."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 1
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        # Test data with null values
        data = CreateIntegrationRequest(
            user_id=1,
            integration_type="slack",
            access_token="test_access_token",
            refresh_token=None,
            expires_at=None
        )

        result = await create_integration(data)

        assert result == 1
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.integration.get_new_db_connection")
    async def test_get_integration_with_null_values(self, mock_db_conn):
        """Test getting integration with null refresh_token and expires_at."""
        # Setup mock
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (
            1,  # id
            1,  # user_id
            "slack",  # integration_type
            "test_access_token",  # access_token
            None,  # refresh_token
            None,  # expires_at
            "2024-01-01 10:00:00",  # created_at
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_db_conn.return_value = mock_conn

        result = await get_integration(1)

        assert result is not None
        assert result.id == 1
        assert result.refresh_token is None
        assert result.expires_at is None 