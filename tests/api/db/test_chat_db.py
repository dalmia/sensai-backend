import pytest
from datetime import datetime
from unittest.mock import patch, AsyncMock, MagicMock
from src.api.db.chat import (
    store_messages,
    get_all_chat_history,
    convert_chat_message_to_dict,
    get_question_chat_history_for_user,
    get_task_chat_history_for_user,
    delete_message,
    update_message_timestamp,
    delete_user_chat_history_for_task,
    delete_all_chat_history,
)
from src.api.models import StoreMessageRequest, TaskType


@pytest.mark.asyncio
class TestStoreMessages:
    """Test message storage functionality."""

    @patch("src.api.db.chat.get_new_db_connection")
    @patch("src.api.db.chat.execute_db_operation")
    async def test_store_messages_success(self, mock_execute, mock_get_conn):
        """Test successful message storage."""
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn

        # Mock the fetch result
        mock_execute.return_value = [
            (123, "2024-01-01 12:00:00", 1, 1, "user", "Hello", "text")
        ]

        messages = [
            StoreMessageRequest(
                role="user",
                content="Hello",
                response_type="text",
                created_at=datetime.now(),
            )
        ]

        result = await store_messages(messages, 1, 1, False)

        assert len(result) == 1
        assert result[0]["id"] == 123
        assert result[0]["content"] == "Hello"
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.chat.get_new_db_connection")
    @patch("src.api.db.chat.execute_db_operation")
    async def test_store_messages_with_completion(self, mock_execute, mock_get_conn):
        """Test message storage with task completion."""
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn

        mock_execute.return_value = [
            (123, "2024-01-01 12:00:00", 1, 1, "user", "Hello", "text")
        ]

        messages = [
            StoreMessageRequest(
                role="user",
                content="Hello",
                response_type="text",
                created_at=datetime.now(),
            )
        ]

        result = await store_messages(messages, 1, 1, True)

        # Should insert completion record
        assert (
            mock_cursor.execute.call_count == 2
        )  # One for message, one for completion
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("task_completions" in call for call in calls)

    @patch("src.api.db.chat.get_new_db_connection")
    @patch("src.api.db.chat.execute_db_operation")
    async def test_store_multiple_messages(self, mock_execute, mock_get_conn):
        """Test storing multiple messages."""
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn

        mock_execute.return_value = [
            (123, "2024-01-01 12:00:00", 1, 1, "user", "Hello", "text"),
            (124, "2024-01-01 12:01:00", 1, 1, "assistant", "Hi", "text"),
        ]

        messages = [
            StoreMessageRequest(
                role="user",
                content="Hello",
                response_type="text",
                created_at=datetime.now(),
            ),
            StoreMessageRequest(
                role="assistant",
                content="Hi",
                response_type="text",
                created_at=datetime.now(),
            ),
        ]

        result = await store_messages(messages, 1, 1, False)

        assert len(result) == 2
        assert mock_cursor.execute.call_count == 2  # One for each message


@pytest.mark.asyncio
class TestGetChatHistory:
    """Test chat history retrieval functions."""

    @patch("src.api.db.chat.execute_db_operation")
    async def test_get_all_chat_history_success(self, mock_execute):
        """Test successful retrieval of all chat history for an organization."""
        mock_execute.return_value = [
            (
                1,
                "2024-01-01 12:00:00",
                1,
                "user@example.com",
                1,
                1,
                "user",
                "Hello",
                "text",
            ),
            (
                2,
                "2024-01-01 12:01:00",
                1,
                "user@example.com",
                1,
                1,
                "assistant",
                "Hi",
                "text",
            ),
        ]

        result = await get_all_chat_history(1)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["user_email"] == "user@example.com"
        assert result[0]["content"] == "Hello"
        assert result[1]["content"] == "Hi"

        mock_execute.assert_called_once()

    @patch("src.api.db.chat.execute_db_operation")
    async def test_get_question_chat_history_for_user_success(self, mock_execute):
        """Test successful retrieval of question chat history for user."""
        mock_execute.return_value = [
            (1, "2024-01-01 12:00:00", 1, 1, "user", "Hello", "text"),
            (2, "2024-01-01 12:01:00", 1, 1, "assistant", "Hi", "text"),
        ]

        result = await get_question_chat_history_for_user(1, 1)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["role"] == "user"
        assert result[0]["content"] == "Hello"

        mock_execute.assert_called_once_with(
            """
    SELECT id, created_at, user_id, question_id, role, content, response_type FROM chat_history WHERE question_id = ? AND user_id = ?
    """,
            (1, 1),
            fetch_all=True,
        )

    @patch("src.api.db.chat.get_basic_task_details")
    @patch("src.api.db.chat.execute_db_operation")
    async def test_get_task_chat_history_for_user_success(
        self, mock_execute, mock_get_task
    ):
        """Test successful retrieval of task chat history for user."""
        mock_get_task.return_value = {"type": "quiz"}
        mock_execute.return_value = [
            (1, "2024-01-01 12:00:00", 1, 1, "user", "Hello", "text")
        ]

        result = await get_task_chat_history_for_user(1, 1)

        assert len(result) == 1
        assert result[0]["id"] == 1
        mock_get_task.assert_called_once_with(1)

    @patch("src.api.db.chat.get_basic_task_details")
    async def test_get_task_chat_history_for_user_task_not_exist(self, mock_get_task):
        """Test task chat history retrieval when task doesn't exist."""
        mock_get_task.return_value = None

        with pytest.raises(ValueError, match="Task does not exist"):
            await get_task_chat_history_for_user(1, 1)

    @patch("src.api.db.chat.get_basic_task_details")
    async def test_get_task_chat_history_for_user_learning_material(
        self, mock_get_task
    ):
        """Test task chat history retrieval for learning material task."""
        mock_get_task.return_value = {"type": "learning_material"}

        with pytest.raises(ValueError, match="Task is not a quiz"):
            await get_task_chat_history_for_user(1, 1)


class TestChatMessageConversion:
    """Test chat message conversion utilities."""

    def test_convert_chat_message_to_dict(self):
        """Test converting chat message tuple to dictionary."""
        message_tuple = (
            1,  # id
            "2024-01-01 12:00:00",  # created_at
            1,  # user_id
            1,  # question_id
            "user",  # role
            "Hello",  # content
            "text",  # response_type
        )

        result = convert_chat_message_to_dict(message_tuple)

        expected = {
            "id": 1,
            "created_at": "2024-01-01 12:00:00",
            "user_id": 1,
            "question_id": 1,
            "role": "user",
            "content": "Hello",
            "response_type": "text",
        }

        assert result == expected


@pytest.mark.asyncio
class TestChatMessageOperations:
    """Test chat message CRUD operations."""

    @patch("src.api.db.chat.execute_db_operation")
    async def test_delete_message_success(self, mock_execute):
        """Test successful message deletion."""
        await delete_message(1)

        mock_execute.assert_called_once_with(
            "DELETE FROM chat_history WHERE id = ?", (1,)
        )

    @patch("src.api.db.chat.execute_db_operation")
    async def test_update_message_timestamp_success(self, mock_execute):
        """Test successful message timestamp update."""
        new_timestamp = datetime(2024, 1, 1, 12, 0, 0)

        await update_message_timestamp(1, new_timestamp)

        mock_execute.assert_called_once_with(
            "UPDATE chat_history SET timestamp = ? WHERE id = ?", (new_timestamp, 1)
        )

    @patch("src.api.db.chat.execute_db_operation")
    async def test_delete_user_chat_history_for_task_success(self, mock_execute):
        """Test successful deletion of user chat history for a task."""
        await delete_user_chat_history_for_task(1, 1)

        mock_execute.assert_called_once_with(
            "DELETE FROM chat_history WHERE question_id = ? AND user_id = ?", (1, 1)
        )

    @patch("src.api.db.chat.execute_db_operation")
    async def test_delete_all_chat_history_success(self, mock_execute):
        """Test successful deletion of all chat history."""
        await delete_all_chat_history()

        mock_execute.assert_called_once_with("DELETE FROM chat_history")
