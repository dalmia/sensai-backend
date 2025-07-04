import pytest
from unittest.mock import patch, AsyncMock
from src.api.db.milestone import (
    convert_milestone_db_to_dict,
    get_all_milestones,
    get_all_milestones_for_org,
    update_milestone,
    delete_milestone,
    get_user_metrics_for_all_milestones,
)


class TestMilestoneUtilityFunctions:
    """Test milestone utility and conversion functions."""

    def test_convert_milestone_db_to_dict(self):
        """Test converting milestone tuple to dictionary."""
        milestone_tuple = (1, "Test Milestone", "#FF5733")

        result = convert_milestone_db_to_dict(milestone_tuple)

        expected = {"id": 1, "name": "Test Milestone", "color": "#FF5733"}

        assert result == expected


@pytest.mark.asyncio
class TestMilestoneOperations:
    """Test milestone database operations."""

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_get_all_milestones_success(self, mock_execute):
        """Test successful retrieval of all milestones."""
        mock_execute.return_value = [
            (1, "Milestone 1", "#FF5733"),
            (2, "Milestone 2", "#33FF57"),
            (3, "Milestone 3", "#3357FF"),
        ]

        result = await get_all_milestones()

        expected = [
            {"id": 1, "name": "Milestone 1", "color": "#FF5733"},
            {"id": 2, "name": "Milestone 2", "color": "#33FF57"},
            {"id": 3, "name": "Milestone 3", "color": "#3357FF"},
        ]

        assert result == expected
        mock_execute.assert_called_once_with(
            "SELECT id, name, color FROM milestones", fetch_all=True
        )

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_get_all_milestones_for_org_success(self, mock_execute):
        """Test successful retrieval of milestones for organization."""
        mock_execute.return_value = [
            (1, "Org Milestone 1", "#FF5733"),
            (2, "Org Milestone 2", "#33FF57"),
        ]

        result = await get_all_milestones_for_org(1)

        expected = [
            {"id": 1, "name": "Org Milestone 1", "color": "#FF5733"},
            {"id": 2, "name": "Org Milestone 2", "color": "#33FF57"},
        ]

        assert result == expected
        mock_execute.assert_called_once_with(
            "SELECT id, name, color FROM milestones WHERE org_id = ?",
            (1,),
            fetch_all=True,
        )

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_update_milestone_success(self, mock_execute):
        """Test successful milestone update."""
        await update_milestone(1, "Updated Milestone Name")

        mock_execute.assert_called_once_with(
            "UPDATE milestones SET name = ? WHERE id = ?", ("Updated Milestone Name", 1)
        )

    @patch("src.api.db.milestone.execute_multiple_db_operations")
    async def test_delete_milestone_success(self, mock_execute_multiple):
        """Test successful milestone deletion."""
        await delete_milestone(1)

        expected_operations = [
            ("DELETE FROM milestones WHERE id = ?", (1,)),
            (
                "UPDATE course_tasks SET milestone_id = NULL WHERE milestone_id = ?",
                (1,),
            ),
            ("DELETE FROM course_milestones WHERE milestone_id = ?", (1,)),
        ]

        mock_execute_multiple.assert_called_once_with(expected_operations)

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_get_user_metrics_for_all_milestones_success(self, mock_execute):
        """Test successful retrieval of user metrics for milestones."""
        # Mock the two separate queries
        base_results = [
            (1, "Milestone 1", "#FF5733", 5, 3),
            (2, "Milestone 2", "#33FF57", 3, 1),
        ]
        null_milestone_results = [(None, "Uncategorized", "#CCCCCC", 2, 1)]

        mock_execute.side_effect = [base_results, null_milestone_results]

        result = await get_user_metrics_for_all_milestones(1, 1)

        expected = [
            {
                "milestone_id": 1,
                "milestone_name": "Milestone 1",
                "milestone_color": "#FF5733",
                "total_tasks": 5,
                "completed_tasks": 3,
            },
            {
                "milestone_id": 2,
                "milestone_name": "Milestone 2",
                "milestone_color": "#33FF57",
                "total_tasks": 3,
                "completed_tasks": 1,
            },
            {
                "milestone_id": None,
                "milestone_name": "Uncategorized",
                "milestone_color": "#CCCCCC",
                "total_tasks": 2,
                "completed_tasks": 1,
            },
        ]

        assert result == expected
        assert mock_execute.call_count == 2

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_get_user_metrics_empty_results(self, mock_execute):
        """Test user metrics when no milestones exist."""
        mock_execute.side_effect = [[], []]

        result = await get_user_metrics_for_all_milestones(1, 1)

        assert result == []

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_get_all_milestones_empty(self, mock_execute):
        """Test getting all milestones when none exist."""
        mock_execute.return_value = []

        result = await get_all_milestones()

        assert result == []

    @patch("src.api.db.milestone.execute_db_operation")
    async def test_get_all_milestones_for_org_empty(self, mock_execute):
        """Test getting org milestones when none exist."""
        mock_execute.return_value = []

        result = await get_all_milestones_for_org(999)

        assert result == []
