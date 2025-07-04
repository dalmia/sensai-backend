import pytest
from unittest.mock import patch, AsyncMock, MagicMock, ANY, call
from src.api.db.batch import (
    create_batch,
    create_batch_with_members,
    delete_batch,
    get_batches_for_user_in_cohort,
    get_batch_by_id,
    get_all_batches_for_cohort,
    update_batch_name_and_members,
)


@pytest.mark.asyncio
async def test_database_operations_are_mocked():
    """Test that database operations are properly mocked and don't hit real database."""
    # This test verifies that the database operations are mocked
    # If they weren't mocked, this would fail or hit a real database

    # Test that execute_db_operation is mocked
    with patch("src.api.db.batch.execute_db_operation") as mock_execute:
        mock_execute.return_value = 999

        result = await create_batch("Mock Test Batch", 1)

        # Verify the mock was called (not real database)
        assert result == 999
        assert mock_execute.called

    # Test that get_new_db_connection is mocked
    with patch("src.api.db.batch.get_new_db_connection") as mock_conn:
        mock_conn_instance = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 888
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value.__aenter__.return_value = mock_conn_instance

        result = await create_batch_with_members("Mock Test Batch", 1, [1, 2])

        # Verify the mock was called (not real database)
        assert result == 888
        assert mock_conn.called

    print("✅ All database operations are properly mocked!")


@pytest.mark.asyncio
class TestBatchBasicOperations:
    """Test basic batch database operations."""

    @patch("src.api.db.batch.execute_db_operation")
    async def test_create_batch_success(self, mock_execute):
        """Test successful batch creation."""
        mock_execute.return_value = 123  # Mock batch ID

        result = await create_batch("Test Batch", 1)

        assert result == 123
        mock_execute.assert_called_once_with(
            ANY,  # Use ANY to avoid SQL string formatting issues
            params=("Test Batch", 1),
            get_last_row_id=True,
        )

    @patch("src.api.db.batch.get_new_db_connection")
    async def test_create_batch_with_members_success(self, mock_get_connection):
        """Test successful batch creation with initial members."""
        # Mock connection and cursor
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value.__aenter__.return_value = mock_conn

        result = await create_batch_with_members("Test Batch", 1, [1, 2])

        assert result == 123
        # Verify connection was obtained
        mock_get_connection.assert_called_once()
        # Verify cursor operations
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once()
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.batch.get_new_db_connection")
    async def test_create_batch_with_members_no_members(self, mock_get_connection):
        """Test batch creation with no initial members."""
        # Mock connection and cursor
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value.__aenter__.return_value = mock_conn

        result = await create_batch_with_members("Test Batch", 1, [])

        assert result == 123
        # Verify connection was obtained
        mock_get_connection.assert_called_once()
        # Verify only batch creation, no member addition
        mock_cursor.execute.assert_called_once()
        mock_cursor.executemany.assert_not_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.batch.get_new_db_connection")
    async def test_create_batch_with_members_none_values(self, mock_get_connection):
        """Test batch creation with None values for members."""
        # Mock connection and cursor
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value.__aenter__.return_value = mock_conn

        result = await create_batch_with_members("Test Batch", 1, None)

        assert result == 123
        # Verify connection was obtained
        mock_get_connection.assert_called_once()
        # Verify only batch creation, no member addition
        mock_cursor.execute.assert_called_once()
        mock_cursor.executemany.assert_not_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.batch.get_new_db_connection")
    async def test_create_batch_with_members_add_fails(self, mock_get_connection):
        """Test batch creation when adding members fails."""
        # Mock connection and cursor
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_cursor.executemany.side_effect = Exception(
            "Database error during member addition"
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value.__aenter__.return_value = mock_conn

        with pytest.raises(Exception, match="Database error during member addition"):
            await create_batch_with_members("Test Batch", 1, [1, 2])

        # Verify connection was obtained
        mock_get_connection.assert_called_once()
        # Verify operations were attempted
        mock_cursor.execute.assert_called_once()
        mock_cursor.executemany.assert_called_once()

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_all_batches_for_cohort_success(self, mock_execute):
        """Test getting all batches for a cohort with members."""
        # Mock the JOIN query results
        mock_results = [
            (1, "Batch 1", 1, "user1@example.com", "learner"),
            (1, "Batch 1", 2, "user2@example.com", "mentor"),
            (2, "Batch 2", 3, "user3@example.com", "learner"),
            (3, "Batch 3", None, None, None),  # Batch with no members
        ]
        mock_execute.return_value = mock_results

        result = await get_all_batches_for_cohort(1)

        expected = [
            {
                "id": 1,
                "name": "Batch 1",
                "members": [
                    {"id": 1, "email": "user1@example.com", "role": "learner"},
                    {"id": 2, "email": "user2@example.com", "role": "mentor"},
                ],
            },
            {
                "id": 2,
                "name": "Batch 2",
                "members": [
                    {"id": 3, "email": "user3@example.com", "role": "learner"},
                ],
            },
            {"id": 3, "name": "Batch 3", "members": []},
        ]

        assert result == expected

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_all_batches_for_cohort_empty(self, mock_execute):
        """Test getting all batches for a cohort when no batches exist."""
        mock_execute.return_value = []

        result = await get_all_batches_for_cohort(1)

        assert result == []


@pytest.mark.asyncio
class TestBatchDetailOperations:
    """Test batch detail retrieval operations."""

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batch_by_id_success(self, mock_execute):
        """Test successful batch retrieval by ID."""
        # Mock batch data
        batch_tuple = (1, "Test Batch", 5)  # id, name, cohort_id
        members_data = [
            (1, "user1@example.com", "learner"),
            (2, "user2@example.com", "mentor"),
        ]

        mock_execute.side_effect = [batch_tuple, members_data]

        result = await get_batch_by_id(1)

        expected = {
            "id": 1,
            "name": "Test Batch",
            "cohort_id": 5,
            "members": [
                {"id": 1, "email": "user1@example.com", "role": "learner"},
                {"id": 2, "email": "user2@example.com", "role": "mentor"},
            ],
        }

        assert result == expected

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batch_by_id_not_found(self, mock_execute):
        """Test batch retrieval when batch doesn't exist."""
        mock_execute.return_value = None

        result = await get_batch_by_id(999)

        assert result is None

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batch_by_id_no_members(self, mock_execute):
        """Test batch retrieval with no members."""
        batch_tuple = (1, "Empty Batch", 5)
        members_data = []

        mock_execute.side_effect = [batch_tuple, members_data]

        result = await get_batch_by_id(1)

        expected = {
            "id": 1,
            "name": "Empty Batch",
            "cohort_id": 5,
            "members": [],
        }

        assert result == expected


@pytest.mark.asyncio
class TestBatchUserOperations:
    """Test batch operations for specific users."""

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batches_for_user_in_cohort_success(self, mock_execute):
        """Test successfully getting batches for user in cohort."""
        # Mock cohort exists, user is in cohort, and batch results
        cohort_check = (1,)  # Cohort exists
        user_in_cohort = (1,)  # User is in cohort
        batch_results = [
            (1, "Batch 1", "learner"),
            (2, "Batch 2", "mentor"),
        ]

        mock_execute.side_effect = [cohort_check, user_in_cohort, batch_results]

        result = await get_batches_for_user_in_cohort(1, 1)

        expected = [
            {"id": 1, "name": "Batch 1", "role": "learner"},
            {"id": 2, "name": "Batch 2", "role": "mentor"},
        ]

        assert result == expected

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batches_for_user_in_cohort_cohort_not_found(self, mock_execute):
        """Test getting batches when cohort doesn't exist."""
        mock_execute.return_value = None  # Cohort doesn't exist

        with pytest.raises(Exception, match="Cohort not found"):
            await get_batches_for_user_in_cohort(1, 999)

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batches_for_user_in_cohort_user_not_in_cohort(
        self, mock_execute
    ):
        """Test getting batches when user is not in cohort."""
        cohort_check = (1,)  # Cohort exists
        user_in_cohort = None  # User is not in cohort

        mock_execute.side_effect = [cohort_check, user_in_cohort]

        with pytest.raises(
            Exception, match="User is not a member of the specified cohort"
        ):
            await get_batches_for_user_in_cohort(1, 1)

    @patch("src.api.db.batch.execute_db_operation")
    async def test_get_batches_for_user_in_cohort_no_batches(self, mock_execute):
        """Test getting batches when user has no batches in cohort."""
        cohort_check = (1,)  # Cohort exists
        user_in_cohort = (1,)  # User is in cohort
        batch_results = []  # No batches

        mock_execute.side_effect = [cohort_check, user_in_cohort, batch_results]

        result = await get_batches_for_user_in_cohort(1, 1)

        assert result == []


@pytest.mark.asyncio
async def test_delete_batch():
    with patch("src.api.db.batch.execute_multiple_db_operations") as mock_exec:
        await delete_batch(42)
        mock_exec.assert_called_once()
        ops = mock_exec.call_args[0][0]
        assert any("user_batches" in op[0] for op in ops)
        assert any("batches" in op[0] for op in ops)


@pytest.mark.asyncio
async def test_update_batch_name_and_members_add_and_remove():
    # Test both add and remove logic
    with patch("src.api.db.batch.get_new_db_connection") as mock_conn:
        mock_conn_instance = AsyncMock()
        mock_cursor = AsyncMock()
        # Simulate no existing users for add, all found for remove
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(side_effect=[[], [(1,), (2,)]])
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value.__aenter__.return_value = mock_conn_instance
        await update_batch_name_and_members(1, "NewName", [1, 2], [1, 2])
        # Should call executemany for add and execute for remove
        mock_cursor.executemany.assert_called()
        mock_cursor.execute.assert_any_call(ANY, (1, 1, 2))
        mock_conn_instance.commit.assert_called()


@pytest.mark.asyncio
async def test_update_batch_name_and_members_add_error():
    # Test error when adding existing users
    with patch("src.api.db.batch.get_new_db_connection") as mock_conn:
        mock_conn_instance = AsyncMock()
        mock_cursor = AsyncMock()
        # Simulate existing users found
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[(1,)])
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value.__aenter__.return_value = mock_conn_instance
        with pytest.raises(Exception, match="already in the batch"):
            await update_batch_name_and_members(1, "NewName", [1], None)


@pytest.mark.asyncio
async def test_update_batch_name_and_members_remove_error():
    # Test error when removing non-existent users
    with patch("src.api.db.batch.get_new_db_connection") as mock_conn:
        mock_conn_instance = AsyncMock()
        mock_cursor = AsyncMock()
        # Simulate not all members found
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[(1,)])
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value.__aenter__.return_value = mock_conn_instance
        with pytest.raises(Exception, match="not in the batch"):
            await update_batch_name_and_members(1, "NewName", None, [1, 2])
