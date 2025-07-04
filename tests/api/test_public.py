import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient
from fastapi import HTTPException
from src.api.public import app, validate_api_key
from src.api.models import PublicAPIChatMessage, TaskType

client = TestClient(app)


@pytest.mark.asyncio
class TestValidateApiKey:
    """Test the validate_api_key function."""

    @patch("src.api.public.get_org_id_from_api_key")
    async def test_validate_api_key_success(self, mock_get_org_id):
        """Test successful API key validation."""
        mock_get_org_id.return_value = 123

        # Should not raise any exception
        await validate_api_key("valid_api_key", 123)
        mock_get_org_id.assert_called_once_with("valid_api_key")

    @patch("src.api.public.get_org_id_from_api_key")
    async def test_validate_api_key_invalid_key(self, mock_get_org_id):
        """Test API key validation with invalid key."""
        mock_get_org_id.side_effect = ValueError("Invalid API key")

        with pytest.raises(HTTPException) as exc_info:
            await validate_api_key("invalid_api_key", 123)

        assert exc_info.value.status_code == 403
        assert exc_info.value.detail == "Invalid API key"

    @patch("src.api.public.get_org_id_from_api_key")
    async def test_validate_api_key_wrong_org_id(self, mock_get_org_id):
        """Test API key validation with wrong organization ID."""
        mock_get_org_id.return_value = 456  # Different org_id

        with pytest.raises(HTTPException) as exc_info:
            await validate_api_key("valid_api_key", 123)

        assert exc_info.value.status_code == 403
        assert exc_info.value.detail == "Invalid API key"

    @patch("src.api.public.get_org_id_from_api_key")
    async def test_validate_api_key_none_org_id(self, mock_get_org_id):
        """Test API key validation when org_id is None."""
        mock_get_org_id.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await validate_api_key("api_key", 123)

        assert exc_info.value.status_code == 403
        assert exc_info.value.detail == "Invalid API key"


class TestGetAllChatHistory:
    """Test the get_all_chat_history endpoint."""

    @patch("src.api.public.validate_api_key")
    @patch("src.api.public.get_all_chat_history_from_db")
    def test_get_all_chat_history_success(self, mock_get_chat_history, mock_validate):
        """Test successful chat history retrieval."""
        # Setup mocks
        mock_validate.return_value = None  # Successful validation
        mock_chat_data = [
            {
                "id": 1,
                "created_at": "2023-01-01T00:00:00Z",
                "user_id": 123,
                "question_id": 456,
                "role": "user",
                "content": "Hello",
                "response_type": "text",
                "task_id": 789,
                "user_email": "test@example.com",
            }
        ]
        mock_get_chat_history.return_value = mock_chat_data

        # Make request
        response = client.get(
            "/chat_history?org_id=123", headers={"api-key": "valid_key"}
        )

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_chat_data

    @patch("src.api.public.validate_api_key")
    def test_get_all_chat_history_invalid_api_key(self, mock_validate):
        """Test chat history retrieval with invalid API key."""
        # Setup mock to raise exception
        mock_validate.side_effect = HTTPException(
            status_code=403, detail="Invalid API key"
        )

        # Make request
        response = client.get(
            "/chat_history?org_id=123", headers={"api-key": "invalid_key"}
        )

        # Assertions
        assert response.status_code == 403
        assert response.json() == {"detail": "Invalid API key"}


class TestGetTasksForCourse:
    """Test the get_tasks_for_course endpoint."""

    @patch("src.api.public.get_org_id_from_api_key")
    @patch("src.api.public.get_course_org_id")
    @patch("src.api.public.validate_api_key")
    @patch("src.api.public.get_course_from_db")
    @patch("src.api.public.get_task_from_db")
    def test_get_tasks_for_course_success_learning_material(
        self,
        mock_get_task,
        mock_get_course,
        mock_validate,
        mock_get_course_org_id,
        mock_get_org_id,
    ):
        """Test successful course retrieval with learning material tasks."""
        # Setup mocks
        mock_get_org_id.return_value = 123
        mock_get_course_org_id.return_value = 123
        mock_validate.return_value = None

        mock_course_data = {
            "id": 1,
            "name": "Test Course",
            "course_generation_status": None,
            "milestones": [
                {
                    "id": 1,
                    "name": "Milestone 1",
                    "color": "#000000",
                    "ordering": 0,
                    "tasks": [
                        {
                            "id": 1,
                            "type": "learning_material",
                            "title": "LM Task",
                            "status": "published",
                            "scheduled_publish_at": None,
                            "ordering": 0,
                            "num_questions": None,
                            "is_generating": False,
                        },
                        {
                            "id": 2,
                            "type": "quiz",
                            "title": "Quiz Task",
                            "status": "published",
                            "scheduled_publish_at": None,
                            "ordering": 1,
                            "num_questions": 1,
                            "is_generating": False,
                        },
                    ],
                }
            ],
        }
        mock_get_course.return_value = mock_course_data

        # Mock task details
        mock_get_task.side_effect = [
            {
                "id": 1,
                "blocks": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "block1", "styles": {}}],
                    }
                ],
            },  # Learning material
            {
                "id": 2,
                "questions": [
                    {
                        "id": 1,
                        "type": "objective",
                        "blocks": [],
                        "answer": [],
                        "input_type": "text",
                        "response_type": "chat",
                        "scorecard_id": None,
                        "context": None,
                        "coding_languages": None,
                        "max_attempts": None,
                        "is_feedback_shown": True,
                        "title": "question",
                    }
                ],
            },  # Quiz
        ]

        # Make request
        response = client.get("/course/1", headers={"api-key": "valid_key"})

        # Assertions
        assert response.status_code == 200
        result = response.json()
        assert result["id"] == 1
        assert len(result["milestones"]) == 1
        assert len(result["milestones"][0]["tasks"]) == 2
        assert "blocks" in result["milestones"][0]["tasks"][0]
        assert "questions" in result["milestones"][0]["tasks"][1]
        assert result["milestones"][0]["tasks"][1]["questions"][0]["title"] == "question"

    @patch("src.api.public.get_org_id_from_api_key")
    def test_get_tasks_for_course_invalid_api_key(self, mock_get_org_id):
        """Test course retrieval with invalid API key."""
        mock_get_org_id.side_effect = ValueError("Invalid API key")

        # Make request
        response = client.get("/course/1", headers={"api-key": "invalid_key"})

        # Assertions
        assert response.status_code == 403
        assert response.json() == {"detail": "Invalid API key"}

    @patch("src.api.public.get_org_id_from_api_key")
    @patch("src.api.public.get_course_org_id")
    def test_get_tasks_for_course_not_found(
        self, mock_get_course_org_id, mock_get_org_id
    ):
        """Test course retrieval when course doesn't exist."""
        mock_get_org_id.return_value = 123
        mock_get_course_org_id.side_effect = ValueError("Course not found")

        # Make request
        response = client.get("/course/999", headers={"api-key": "valid_key"})

        # Assertions
        assert response.status_code == 404
        assert response.json() == {"detail": "Course not found"}

    @patch("src.api.public.get_org_id_from_api_key")
    @patch("src.api.public.get_course_org_id")
    def test_get_tasks_for_course_wrong_org(
        self, mock_get_course_org_id, mock_get_org_id
    ):
        """Test course retrieval when API key doesn't match course org."""
        mock_get_org_id.return_value = 123
        mock_get_course_org_id.return_value = 456  # Different org

        # Make request
        response = client.get("/course/1", headers={"api-key": "valid_key"})

        # Assertions
        assert response.status_code == 403
        assert response.json() == {"detail": "Invalid API key"}
