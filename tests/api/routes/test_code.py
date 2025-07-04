import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.api.routes.code import router
from fastapi import FastAPI

# Create a test app with the code router
app = FastAPI()
app.include_router(router, prefix="/code")
client = TestClient(app)


class TestCodeRoutes:
    """Test code route endpoints."""

    @patch("src.api.routes.code.upsert_code_draft_in_db")
    def test_save_code_draft_success(self, mock_upsert_code_draft):
        """Test successful code draft saving."""
        # Setup mock
        mock_upsert_code_draft.return_value = None

        # Make request
        request_data = {
            "user_id": 123,
            "question_id": 456,
            "code": [{"language": "python", "value": "print('hello world')"}],
        }
        response = client.post("/code/", json=request_data)

        # Assertions
        assert response.status_code == 200
        assert response.json() == {"success": True}
        mock_upsert_code_draft.assert_called_once_with(
            user_id=123,
            question_id=456,
            code=[{"language": "python", "value": "print('hello world')"}],
        )

    @patch("src.api.routes.code.get_code_draft_from_db")
    def test_get_code_draft_success(self, mock_get_code_draft):
        """Test successful code draft retrieval."""
        # Setup mock
        mock_code_draft = {
            "id": 1,
            "code": [{"language": "python", "value": "print('hello world')"}],
        }
        mock_get_code_draft.return_value = mock_code_draft

        # Make request
        response = client.get("/code/user/123/question/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_code_draft
        mock_get_code_draft.assert_called_once_with(123, 456)

    @patch("src.api.routes.code.get_code_draft_from_db")
    def test_get_code_draft_not_found(self, mock_get_code_draft):
        """Test code draft retrieval when draft doesn't exist."""
        # Setup mock
        mock_get_code_draft.return_value = None

        # Make request
        response = client.get("/code/user/123/question/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() is None
        mock_get_code_draft.assert_called_once_with(123, 456)

    @patch("src.api.routes.code.delete_code_draft_in_db")
    def test_delete_code_draft_success(self, mock_delete_code_draft):
        """Test successful code draft deletion."""
        # Setup mock
        mock_delete_code_draft.return_value = None

        # Make request
        response = client.delete("/code/user/123/question/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == {"success": True}
        mock_delete_code_draft.assert_called_once_with(123, 456)
