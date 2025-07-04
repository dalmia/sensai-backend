import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.api.routes.scorecard import router
from fastapi import FastAPI

# Create a test app with the scorecard router
app = FastAPI()
app.include_router(router, prefix="/scorecard")
client = TestClient(app)


class TestScorecardRoutes:
    """Test scorecard route endpoints."""

    @patch("src.api.routes.scorecard.get_all_scorecards_for_org_from_db")
    def test_get_all_scorecards_for_org_success(self, mock_get_all_scorecards):
        """Test successful retrieval of all scorecards for an organization."""
        # Setup mock
        mock_scorecards = [
            {
                "id": 1,
                "title": "Scorecard 1",
                "criteria": [
                    {
                        "name": "accuracy",
                        "description": "Accuracy criterion",
                        "min_score": 0.0,
                        "max_score": 10.0,
                        "pass_score": 7.0,
                    }
                ],
                "status": "published",
            },
            {
                "id": 2,
                "title": "Scorecard 2",
                "criteria": [
                    {
                        "name": "clarity",
                        "description": "Clarity criterion",
                        "min_score": 0.0,
                        "max_score": 10.0,
                        "pass_score": 6.0,
                    }
                ],
                "status": "draft",
            },
        ]
        mock_get_all_scorecards.return_value = mock_scorecards

        # Make request
        response = client.get("/scorecard/?org_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_scorecards
        mock_get_all_scorecards.assert_called_once_with(123)

    @patch("src.api.routes.scorecard.get_all_scorecards_for_org_from_db")
    def test_get_all_scorecards_for_org_empty(self, mock_get_all_scorecards):
        """Test retrieval of scorecards when none exist for organization."""
        # Setup mock
        mock_get_all_scorecards.return_value = []

        # Make request
        response = client.get("/scorecard/?org_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() == []
        mock_get_all_scorecards.assert_called_once_with(123)

    @patch("src.api.routes.scorecard.update_scorecard_from_db")
    def test_update_scorecard_success(self, mock_update_scorecard):
        """Test successful scorecard update."""
        # Setup mock
        updated_scorecard = {
            "id": 123,
            "title": "Updated Scorecard",
            "criteria": [
                {
                    "name": "updated_criterion",
                    "description": "Updated criterion description",
                    "min_score": 0.0,
                    "max_score": 10.0,
                    "pass_score": 8.0,
                }
            ],
            "status": "published",
        }
        mock_update_scorecard.return_value = updated_scorecard

        # Make request
        request_data = {
            "title": "Updated Scorecard",
            "criteria": [
                {
                    "name": "updated_criterion",
                    "description": "Updated criterion description",
                    "min_score": 0.0,
                    "max_score": 10.0,
                    "pass_score": 8.0,
                }
            ],
        }
        response = client.put("/scorecard/123", json=request_data)

        # Assertions
        assert response.status_code == 200
        assert response.json() == updated_scorecard
        mock_update_scorecard.assert_called_once()

    @patch("src.api.routes.scorecard.create_scorecard_from_db")
    def test_create_scorecard_success(self, mock_create_scorecard):
        """Test successful scorecard creation."""
        # Setup mock
        created_scorecard = {
            "id": 123,
            "title": "New Scorecard",
            "criteria": [
                {
                    "name": "new_criterion",
                    "description": "New criterion description",
                    "min_score": 0.0,
                    "max_score": 10.0,
                    "pass_score": 7.0,
                }
            ],
            "status": "draft",
        }
        mock_create_scorecard.return_value = created_scorecard

        # Make request
        request_data = {
            "title": "New Scorecard",
            "org_id": 456,
            "criteria": [
                {
                    "name": "new_criterion",
                    "description": "New criterion description",
                    "min_score": 0.0,
                    "max_score": 10.0,
                    "pass_score": 7.0,
                }
            ],
        }
        response = client.post("/scorecard/", json=request_data)

        # Assertions
        assert response.status_code == 200
        assert response.json() == created_scorecard
        mock_create_scorecard.assert_called_once_with(request_data)
