import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.api.routes.milestone import router
from fastapi import FastAPI

# Create a test app with the milestone router
app = FastAPI()
app.include_router(router, prefix="/milestone")
client = TestClient(app)


class TestMilestoneRoutes:
    """Test milestone route endpoints."""

    @patch("src.api.routes.milestone.get_all_milestones_for_org_from_db")
    def test_get_all_milestones_for_org_success(self, mock_get_all_milestones):
        """Test successful retrieval of all milestones for an organization."""
        # Setup mock
        mock_milestones = [
            {"id": 1, "name": "Milestone 1", "org_id": 123},
            {"id": 2, "name": "Milestone 2", "org_id": 123},
        ]
        mock_get_all_milestones.return_value = mock_milestones

        # Make request
        response = client.get("/milestone/?org_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_milestones
        mock_get_all_milestones.assert_called_once_with(123)

    @patch("src.api.routes.milestone.get_all_milestones_for_org_from_db")
    def test_get_all_milestones_for_org_empty(self, mock_get_all_milestones):
        """Test retrieval of milestones when none exist for organization."""
        # Setup mock
        mock_get_all_milestones.return_value = []

        # Make request
        response = client.get("/milestone/?org_id=123")

        # Assertions
        assert response.status_code == 200
        assert response.json() == []
        mock_get_all_milestones.assert_called_once_with(123)

    @patch("src.api.routes.milestone.update_milestone_in_db")
    def test_update_milestone_success(self, mock_update_milestone):
        """Test successful milestone update."""
        # Setup mock
        mock_update_milestone.return_value = None

        # Make request
        request_data = {"name": "Updated Milestone Name"}
        response = client.put("/milestone/123", json=request_data)

        # Assertions
        assert response.status_code == 200
        assert response.json() == {"message": "Milestone updated"}
        mock_update_milestone.assert_called_once_with(123, "Updated Milestone Name")

    @patch("src.api.routes.milestone.delete_milestone_from_db")
    def test_delete_milestone_success(self, mock_delete_milestone):
        """Test successful milestone deletion."""
        # Setup mock
        mock_delete_milestone.return_value = None

        # Make request
        response = client.delete("/milestone/123")

        # Assertions
        assert response.status_code == 200
        assert response.json() == {"message": "Milestone deleted"}
        mock_delete_milestone.assert_called_once_with(123)

    @patch("src.api.routes.milestone.get_user_metrics_for_all_milestones_from_db")
    def test_get_user_metrics_for_all_milestones_success(self, mock_get_user_metrics):
        """Test successful retrieval of user metrics for all milestones."""
        # Setup mock
        mock_metrics = [
            {"milestone_id": 1, "completion_rate": 0.8, "progress": 80},
            {"milestone_id": 2, "completion_rate": 0.6, "progress": 60},
        ]
        mock_get_user_metrics.return_value = mock_metrics

        # Make request
        response = client.get("/milestone/metrics/user/123/course/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_metrics
        mock_get_user_metrics.assert_called_once_with(123, 456)

    @patch("src.api.routes.milestone.get_user_metrics_for_all_milestones_from_db")
    def test_get_user_metrics_for_all_milestones_empty(self, mock_get_user_metrics):
        """Test retrieval of user metrics when none exist."""
        # Setup mock
        mock_get_user_metrics.return_value = []

        # Make request
        response = client.get("/milestone/metrics/user/123/course/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == []
        mock_get_user_metrics.assert_called_once_with(123, 456)

    @patch("src.api.routes.milestone.get_milestones_for_course_from_db")
    def test_get_milestones_for_course_success(self, mock_get_milestones_for_course):
        """Test successful retrieval of milestones for a course."""
        # Setup mock
        mock_milestones = [
            {"id": 1, "name": "Course Milestone 1", "course_id": 456},
            {"id": 2, "name": "Course Milestone 2", "course_id": 456},
        ]
        mock_get_milestones_for_course.return_value = mock_milestones

        # Make request
        response = client.get("/milestone/course/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == mock_milestones
        mock_get_milestones_for_course.assert_called_once_with(456)

    @patch("src.api.routes.milestone.get_milestones_for_course_from_db")
    def test_get_milestones_for_course_empty(self, mock_get_milestones_for_course):
        """Test retrieval of milestones for course when none exist."""
        # Setup mock
        mock_get_milestones_for_course.return_value = []

        # Make request
        response = client.get("/milestone/course/456")

        # Assertions
        assert response.status_code == 200
        assert response.json() == []
        mock_get_milestones_for_course.assert_called_once_with(456)
