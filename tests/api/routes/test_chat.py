import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.api.routes.chat import router
from fastapi import FastAPI

# Create a test app with the chat router
app = FastAPI()
app.include_router(router, prefix="/chat")
client = TestClient(app)


class TestChatRoutes:
    """Test chat route endpoints."""

    @patch("src.api.routes.chat.store_messages_in_db")
    def test_store_messages_success(self, mock_store_messages):
        """Test successful message storage."""
        # Setup mock with correct ChatMessage structure
        mock_messages = [
            {
                "id": 1,
                "created_at": "2023-01-01T00:00:00Z",
                "user_id": 123,
                "question_id": 456,
                "role": "user",
                "content": "Hello",
                "response_type": "text",
            }
        ]
        mock_store_messages.return_value = mock_messages

        # Make request
        request_data = {
            "messages": [
                {
                    "role": "user",
                    "content": "Hello",
                    "response_type": "text",
                    "created_at": "2023-01-01T00:00:00Z",
                }
            ],
            "user_id": 123,
            "question_id": 456,
            "is_complete": True,
        }
        response = client.post("/chat/", json=request_data)

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_messages

    @patch("src.api.routes.chat.get_all_chat_history_from_db")
    def test_get_all_chat_history_success(self, mock_get_chat_history):
        """Test successful chat history retrieval."""
        # Setup mock with correct ChatMessage structure
        mock_chat_data = [
            {
                "id": 1,
                "created_at": "2023-01-01T00:00:00Z",
                "user_id": 123,
                "question_id": 456,
                "role": "user",
                "content": "Hello",
                "response_type": "text",
            }
        ]
        mock_get_chat_history.return_value = mock_chat_data

        # Make request
        response = client.get("/chat/?org_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_chat_data

    @patch("src.api.routes.chat.get_task_chat_history_for_user_from_db")
    def test_get_user_chat_history_for_task_success(self, mock_get_task_chat_history):
        """Test successful user chat history retrieval for a specific task."""
        # Setup mock with correct ChatMessage structure
        mock_chat_data = [
            {
                "id": 1,
                "created_at": "2023-01-01T00:00:00Z",
                "user_id": 123,
                "question_id": 456,
                "role": "user",
                "content": "Task specific message",
                "response_type": "text",
            }
        ]
        mock_get_task_chat_history.return_value = mock_chat_data

        # Make request
        response = client.get("/chat/user/123/task/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_chat_data
        mock_get_task_chat_history.assert_called_once_with(user_id=123, task_id=456)

    @patch("src.api.routes.chat.delete_all_chat_history_from_db")
    def test_delete_all_chat_history_success(self, mock_delete_chat_history):
        """Test successful deletion of all chat history."""
        # Setup mock
        mock_delete_chat_history.return_value = None

        # Make request
        response = client.delete("/chat/")

        # Assertions
        assert response.status_code == 200
        assert response.json() == {"message": "All chat history deleted"}
