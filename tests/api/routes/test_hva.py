import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.api.routes.hva import router
from fastapi import FastAPI

# Create a test app with the hva router
app = FastAPI()
app.include_router(router, prefix="/hva")
client = TestClient(app)


class TestHvaRoutes:
    """Test HVA route endpoints."""

    @patch("src.api.routes.hva.get_hva_openai_api_key_from_db")
    def test_get_hva_openai_api_key_success(self, mock_get_hva_openai_api_key):
        """Test successful HVA OpenAI API key retrieval."""
        # Setup mock
        mock_api_key = "test-api-key-12345"
        mock_get_hva_openai_api_key.return_value = mock_api_key

        # Make request
        response = client.get("/hva/openai_key")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_api_key
        mock_get_hva_openai_api_key.assert_called_once()

    @patch("src.api.routes.hva.get_hva_openai_api_key_from_db")
    def test_get_hva_openai_api_key_not_found(self, mock_get_hva_openai_api_key):
        """Test HVA OpenAI API key retrieval when key doesn't exist."""
        # Setup mock
        mock_get_hva_openai_api_key.return_value = None

        # Make request
        response = client.get("/hva/openai_key")

        # Assertions
        assert response.status_code == 200
        assert response.json() is None
        mock_get_hva_openai_api_key.assert_called_once()

    @patch("src.api.routes.hva.is_user_hva_learner_from_db")
    def test_is_user_hva_learner_true(self, mock_is_user_hva_learner):
        """Test user is HVA learner check returning True."""
        # Setup mock
        mock_is_user_hva_learner.return_value = True

        # Make request
        response = client.get("/hva/is_user_hva_learner?user_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() is True
        mock_is_user_hva_learner.assert_called_once_with(123)

    @patch("src.api.routes.hva.is_user_hva_learner_from_db")
    def test_is_user_hva_learner_false(self, mock_is_user_hva_learner):
        """Test user is HVA learner check returning False."""
        # Setup mock
        mock_is_user_hva_learner.return_value = False

        # Make request
        response = client.get("/hva/is_user_hva_learner?user_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() is False
        mock_is_user_hva_learner.assert_called_once_with(123)

    @patch("src.api.routes.hva.get_hva_org_id_from_db")
    def test_get_hva_org_id_success(self, mock_get_hva_org_id):
        """Test successful HVA org ID retrieval."""
        # Setup mock
        mock_org_id = 42
        mock_get_hva_org_id.return_value = mock_org_id

        # Make request
        response = client.get("/hva/org_id")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_org_id
        mock_get_hva_org_id.assert_called_once()

    @patch("src.api.routes.hva.get_hva_org_id_from_db")
    def test_get_hva_org_id_not_found(self, mock_get_hva_org_id):
        """Test HVA org ID retrieval when org doesn't exist."""
        # Setup mock
        mock_get_hva_org_id.return_value = None

        # Make request
        response = client.get("/hva/org_id")

        # Assertions
        assert response.status_code == 200
        assert response.json() is None
        mock_get_hva_org_id.assert_called_once()
