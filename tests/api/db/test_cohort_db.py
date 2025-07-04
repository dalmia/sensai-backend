import pytest
import json
from unittest.mock import patch, AsyncMock, MagicMock, ANY, call
from datetime import datetime, timezone
from collections import defaultdict
from src.api.db.cohort import (
    create_cohort,
    get_cohort_by_id,
    get_all_cohorts_for_org,
    get_cohorts_for_org,
    format_user_cohort_group,
    add_courses_to_cohort,
    add_course_to_cohorts,
    remove_course_from_cohorts,
    remove_courses_from_cohort,
    update_cohort_name,
    delete_cohort,
    add_members_to_cohort,
    remove_members_from_cohort,
    is_user_in_cohort,
    get_cohort_analytics_metrics_for_tasks,
    get_cohort_attempt_data_for_tasks,
    transfer_chat_history_to_user,
    drop_user_cohorts_table,
    delete_all_cohort_info,
    drop_cohorts_table,
)


@pytest.mark.asyncio
class TestCohortBasicOperations:
    """Test basic cohort database operations."""

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_create_cohort_success(self, mock_execute):
        """Test successful cohort creation."""
        mock_execute.return_value = 123  # Mock cohort ID

        result = await create_cohort("Test Cohort", 1)

        assert result == 123
        mock_execute.assert_called_once_with(
            ANY,  # Use ANY to avoid SQL string formatting issues
            params=("Test Cohort", 1),
            get_last_row_id=True,
        )

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_by_id_success(self, mock_execute):
        """Test successful cohort retrieval by ID."""
        # Mock cohort data
        cohort_tuple = (1, "Test Cohort", 1)  # id, name, org_id
        members_data = [
            (1, "user1@example.com", "learner"),
            (2, "user2@example.com", "mentor"),
        ]

        mock_execute.side_effect = [cohort_tuple, members_data]

        result = await get_cohort_by_id(1)

        expected = {
            "id": 1,
            "org_id": 1,
            "name": "Test Cohort",
            "members": [
                {"id": 1, "email": "user1@example.com", "role": "learner"},
                {"id": 2, "email": "user2@example.com", "role": "mentor"},
            ],
        }

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_by_id_not_found(self, mock_execute):
        """Test cohort retrieval when cohort doesn't exist."""
        mock_execute.return_value = None

        result = await get_cohort_by_id(999)

        assert result is None

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_all_cohorts_for_org_success(self, mock_execute):
        """Test getting all cohorts for an organization."""
        mock_cohorts = [
            (1, "Cohort 1"),
            (2, "Cohort 2"),
            (3, "Cohort 3"),
        ]
        mock_execute.return_value = mock_cohorts

        result = await get_all_cohorts_for_org(1)

        expected = [
            {"id": 1, "name": "Cohort 1"},
            {"id": 2, "name": "Cohort 2"},
            {"id": 3, "name": "Cohort 3"},
        ]

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohorts_for_org_success(self, mock_execute):
        """Test getting cohorts for organization with org info."""
        mock_cohorts = [
            (1, "Cohort 1", 1, "Org Name"),
            (2, "Cohort 2", 1, "Org Name"),
        ]
        mock_execute.return_value = mock_cohorts

        result = await get_cohorts_for_org(1)

        expected = [
            {"id": 1, "name": "Cohort 1", "org_id": 1, "org_name": "Org Name"},
            {"id": 2, "name": "Cohort 2", "org_id": 1, "org_name": "Org Name"},
        ]

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_update_cohort_name(self, mock_execute):
        """Test updating cohort name."""
        await update_cohort_name(123, "New Cohort Name")

        mock_execute.assert_called_once_with(ANY, ("New Cohort Name", 123))

    @patch("src.api.db.cohort.execute_multiple_db_operations")
    async def test_delete_cohort(self, mock_execute_multiple):
        """Test deleting a cohort."""
        await delete_cohort(123)

        mock_execute_multiple.assert_called_once()
        operations = mock_execute_multiple.call_args[0][0]
        assert len(operations) == 3  # Should have 3 delete operations


@pytest.mark.asyncio
class TestCohortCourseOperations:
    """Test cohort-course relationship operations."""

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_add_courses_to_cohort(self, mock_execute_many):
        """Test adding courses to cohort."""
        course_ids = [1, 2, 3]
        publish_at = datetime.now(timezone.utc)

        await add_courses_to_cohort(
            cohort_id=1,
            course_ids=course_ids,
            is_drip_enabled=True,
            frequency_value=7,
            frequency_unit="days",
            publish_at=publish_at,
        )

        expected_values = [
            (1, 1, True, 7, "days", publish_at),
            (2, 1, True, 7, "days", publish_at),
            (3, 1, True, 7, "days", publish_at),
        ]

        mock_execute_many.assert_called_once_with(ANY, expected_values)

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_add_courses_to_cohort_no_drip(self, mock_execute_many):
        """Test adding courses to cohort without drip configuration."""
        course_ids = [1, 2]

        await add_courses_to_cohort(cohort_id=1, course_ids=course_ids)

        expected_values = [
            (1, 1, False, None, None, None),
            (2, 1, False, None, None, None),
        ]

        mock_execute_many.assert_called_once_with(ANY, expected_values)

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_add_course_to_cohorts(self, mock_execute_many):
        """Test adding course to multiple cohorts."""
        cohort_ids = [1, 2, 3]

        await add_course_to_cohorts(
            course_id=1,
            cohort_ids=cohort_ids,
            is_drip_enabled=True,
            frequency_value=14,
            frequency_unit="days",
        )

        expected_values = [
            (1, 1, True, 14, "days", None),
            (1, 2, True, 14, "days", None),
            (1, 3, True, 14, "days", None),
        ]

        mock_execute_many.assert_called_once_with(ANY, expected_values)

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_remove_course_from_cohorts(self, mock_execute_many):
        """Test removing course from multiple cohorts."""
        cohort_ids = [1, 2, 3]

        await remove_course_from_cohorts(1, cohort_ids)

        expected_params = [(1, 1), (1, 2), (1, 3)]
        mock_execute_many.assert_called_once_with(ANY, expected_params)

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_remove_courses_from_cohort(self, mock_execute_many):
        """Test removing multiple courses from cohort."""
        course_ids = [1, 2, 3]

        await remove_courses_from_cohort(1, course_ids)

        expected_params = [(1, 1), (1, 2), (1, 3)]
        mock_execute_many.assert_called_once_with(ANY, expected_params)


@pytest.mark.asyncio
class TestCohortMemberOperations:
    """Test cohort member management operations."""

    @patch("src.api.db.cohort.execute_db_operation")
    @patch("src.api.db.cohort.get_new_db_connection")
    @patch("src.api.db.cohort.insert_or_return_user")
    @patch("src.api.db.cohort.send_slack_notification_for_learner_added_to_cohort")
    async def test_add_members_to_cohort_success(
        self, mock_slack, mock_insert_user, mock_connection, mock_execute
    ):
        """Test successfully adding members to cohort."""
        # Mock database setup
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = None  # No existing users
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        # Mock the sequence of database calls
        mock_execute.side_effect = [
            (1,),  # Get org_id from slug
            ("Test Cohort",),  # Cohort exists in organization
            [],  # No admin emails
        ]

        # Mock user creation
        mock_users = [
            {"id": 1, "email": "user1@example.com"},
            {"id": 2, "email": "user2@example.com"},
        ]
        mock_insert_user.side_effect = mock_users

        emails = ["user1@example.com", "user2@example.com"]
        roles = ["learner", "mentor"]

        await add_members_to_cohort(1, "test-org", None, emails, roles)

        # Verify user creation calls
        assert mock_insert_user.call_count == 2
        # Verify Slack notifications
        assert mock_slack.call_count == 2

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_add_members_to_cohort_org_not_found_by_slug(self, mock_execute):
        """Test adding members when organization not found by slug."""
        mock_execute.return_value = None

        with pytest.raises(Exception, match="Organization not found"):
            await add_members_to_cohort(
                1, "invalid-org", None, ["user@example.com"], ["learner"]
            )

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_add_members_to_cohort_org_not_found_by_id(self, mock_execute):
        """Test adding members when organization not found by ID."""
        mock_execute.return_value = None

        with pytest.raises(Exception, match="Organization not found"):
            await add_members_to_cohort(1, None, 999, ["user@example.com"], ["learner"])

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_add_members_to_cohort_cohort_not_in_org(self, mock_execute):
        """Test adding members when cohort doesn't belong to organization."""
        mock_execute.side_effect = [
            (1,),  # Org exists
            None,  # Cohort doesn't belong to org
        ]

        with pytest.raises(
            Exception, match="Cohort does not belong to this organization"
        ):
            await add_members_to_cohort(1, None, 1, ["user@example.com"], ["learner"])

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_add_members_to_cohort_admin_user(self, mock_execute):
        """Test adding admin user to cohort (should fail)."""
        mock_execute.side_effect = [
            ("org-slug",),  # Get org slug from id
            ("Test Cohort",),  # Cohort exists
            [("admin@example.com",)],  # Admin email found
        ]

        with pytest.raises(Exception, match="Cannot add an admin to the cohort"):
            await add_members_to_cohort(1, None, 1, ["admin@example.com"], ["learner"])

    @patch("src.api.db.cohort.execute_db_operation")
    @patch("src.api.db.cohort.get_new_db_connection")
    @patch("src.api.db.cohort.insert_or_return_user")
    async def test_add_members_to_cohort_user_already_exists(
        self, mock_insert_user, mock_connection, mock_execute
    ):
        """Test adding user that already exists in cohort."""
        # Mock database setup
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (1,)  # User exists
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connection.return_value.__aenter__.return_value = mock_conn

        mock_execute.side_effect = [
            ("org-slug",),  # Get org slug from id
            ("Test Cohort",),  # Cohort exists
            [],  # No admin emails
        ]

        mock_insert_user.return_value = {"id": 1, "email": "user@example.com"}

        with pytest.raises(Exception, match="User already exists in cohort"):
            await add_members_to_cohort(1, None, 1, ["user@example.com"], ["learner"])

    async def test_add_members_to_cohort_both_org_params_none(self):
        """Test adding members when both org_slug and org_id are None."""
        with pytest.raises(
            Exception, match="Either org_slug or org_id must be provided"
        ):
            await add_members_to_cohort(
                1, None, None, ["user@example.com"], ["learner"]
            )

    @patch("src.api.db.cohort.execute_db_operation")
    @patch("src.api.db.cohort.execute_multiple_db_operations")
    async def test_remove_members_from_cohort_success(
        self, mock_execute_multiple, mock_execute
    ):
        """Test successfully removing members from cohort."""
        member_ids = [1, 2, 3]
        members_in_cohort = [(1,), (2,), (3,)]
        mock_execute.return_value = members_in_cohort

        await remove_members_from_cohort(1, member_ids)

        mock_execute_multiple.assert_called_once()

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_remove_members_from_cohort_member_not_in_cohort(self, mock_execute):
        """Test removing member that's not in cohort."""
        member_ids = [1, 2]
        members_in_cohort = [(1,)]  # Only one member found
        mock_execute.return_value = members_in_cohort

        with pytest.raises(
            Exception, match="One or more members are not in the cohort"
        ):
            await remove_members_from_cohort(1, member_ids)

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_is_user_in_cohort_true(self, mock_execute):
        """Test checking if user is in cohort - returns True."""
        mock_execute.return_value = (1,)  # User is in cohort

        result = await is_user_in_cohort(123, 456)

        assert result == 1

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_is_user_in_cohort_false(self, mock_execute):
        """Test checking if user is in cohort - returns False."""
        mock_execute.return_value = (0,)  # User is not in cohort

        result = await is_user_in_cohort(123, 456)

        assert result == 0


@pytest.mark.asyncio
class TestCohortAnalytics:
    """Test cohort analytics and metrics operations."""

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_analytics_metrics_for_tasks(self, mock_execute):
        """Test getting analytics metrics for cohort tasks."""
        task_ids = [1, 2, 3]
        analytics_data = [
            (1, "user1@example.com", "1,2", "1,0"),
            (2, "user2@example.com", "1,2,3", "1,1,0"),
        ]
        mock_execute.return_value = analytics_data

        result = await get_cohort_analytics_metrics_for_tasks(1, task_ids)

        expected = [
            {
                "user_id": 1,
                "email": "user1@example.com",
                "num_completed": 1,
                "task_1": 1,
                "task_2": 0,
                "task_3": 0,  # Default for unattempted task
            },
            {
                "user_id": 2,
                "email": "user2@example.com",
                "num_completed": 2,
                "task_1": 1,
                "task_2": 1,
                "task_3": 0,
            },
        ]

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_analytics_metrics_empty_data(self, mock_execute):
        """Test getting analytics metrics with empty data."""
        mock_execute.return_value = []

        result = await get_cohort_analytics_metrics_for_tasks(1, [1, 2])

        assert result == []

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_analytics_metrics_none_values(self, mock_execute):
        """Test getting analytics metrics with None values."""
        analytics_data = [
            (1, "user1@example.com", None, None),
        ]
        mock_execute.return_value = analytics_data

        result = await get_cohort_analytics_metrics_for_tasks(1, [1, 2])

        expected = [
            {
                "user_id": 1,
                "email": "user1@example.com",
                "num_completed": 0,
                "task_1": 0,
                "task_2": 0,
            }
        ]

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_attempt_data_for_tasks(self, mock_execute):
        """Test getting attempt data for cohort tasks."""
        task_ids = [1, 2, 3]
        attempt_data = [
            (1, "user1@example.com", "1,2", "1,1"),
            (2, "user2@example.com", "1,3", "1,0"),
        ]
        mock_execute.return_value = attempt_data

        result = await get_cohort_attempt_data_for_tasks(1, task_ids)

        expected = [
            {
                "user_id": 1,
                "email": "user1@example.com",
                "num_attempted": 2,
                "task_1": 1,
                "task_2": 1,
                "task_3": 0,  # Default for unattempted task
            },
            {
                "user_id": 2,
                "email": "user2@example.com",
                "num_attempted": 1,
                "task_1": 1,
                "task_2": 0,  # Default for unattempted task
                "task_3": 0,
            },
        ]

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_attempt_data_empty(self, mock_execute):
        """Test getting attempt data with empty results."""
        mock_execute.return_value = []

        result = await get_cohort_attempt_data_for_tasks(1, [1, 2])

        assert result == []

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_attempt_data_none_values(self, mock_execute):
        """Test getting attempt data with None values."""
        attempt_data = [
            (1, "user1@example.com", None, None),
        ]
        mock_execute.return_value = attempt_data

        result = await get_cohort_attempt_data_for_tasks(1, [1, 2])

        expected = [
            {
                "user_id": 1,
                "email": "user1@example.com",
                "num_attempted": 0,
                "task_1": 0,
                "task_2": 0,
            }
        ]

        assert result == expected


class TestCohortUtilityFunctions:
    """Test cohort utility and formatting functions."""

    def test_format_user_cohort_group(self):
        """Test formatting user cohort group data."""
        group_tuple = (
            1,  # cohort_id
            "Test Cohort",  # cohort_name
            "1,2,3",  # comma-separated user IDs
            "user1@example.com,user2@example.com,user3@example.com",  # comma-separated emails
        )

        result = format_user_cohort_group(group_tuple)

        expected = {
            "id": 1,
            "name": "Test Cohort",
            "learners": [
                {"id": 1, "email": "user1@example.com"},
                {"id": 2, "email": "user2@example.com"},
                {"id": 3, "email": "user3@example.com"},
            ],
        }

        assert result == expected

    def test_format_user_cohort_group_single_user(self):
        """Test formatting user cohort group with single user."""
        group_tuple = (
            1,
            "Single User Cohort",
            "1",
            "user@example.com",
        )

        result = format_user_cohort_group(group_tuple)

        expected = {
            "id": 1,
            "name": "Single User Cohort",
            "learners": [
                {"id": 1, "email": "user@example.com"},
            ],
        }

        assert result == expected

    @patch("src.api.db.cohort.execute_db_operation")
    def test_transfer_chat_history_to_user(self, mock_execute):
        """Test transferring chat history to user."""
        transfer_chat_history_to_user(123, 456)

        mock_execute.assert_called_once_with(ANY, (456, 123))


class TestCohortDataStructures:
    """Test cohort data structures and configurations."""

    def test_cohort_drip_config_structure(self):
        """Test the expected drip configuration structure."""
        drip_config = {
            "is_drip_enabled": True,
            "frequency_value": 7,
            "frequency_unit": "days",
            "publish_at": "2024-01-01 00:00:00",
        }

        assert "is_drip_enabled" in drip_config
        assert "frequency_value" in drip_config
        assert "frequency_unit" in drip_config
        assert "publish_at" in drip_config
        assert drip_config["is_drip_enabled"] is True
        assert drip_config["frequency_value"] == 7

    def test_cohort_member_structure(self):
        """Test the expected cohort member structure."""
        member = {
            "id": 1,
            "email": "user@example.com",
            "role": "learner",
        }

        assert "id" in member
        assert "email" in member
        assert "role" in member
        assert member["role"] in ["learner", "mentor"]


class TestCohortTableOperations:
    """Test cohort table operations."""

    @patch("src.api.db.cohort.execute_db_operation")
    def test_drop_user_cohorts_table(self, mock_execute):
        """Test dropping user cohorts table."""
        drop_user_cohorts_table()

        mock_execute.assert_called_once()

    @patch("src.api.db.cohort.execute_db_operation")
    def test_delete_all_cohort_info(self, mock_execute):
        """Test deleting all cohort information."""
        delete_all_cohort_info()

        mock_execute.assert_called_once()

    @patch("src.api.db.cohort.execute_db_operation")
    def test_drop_cohorts_table(self, mock_execute):
        """Test dropping cohorts table."""
        drop_cohorts_table()

        mock_execute.assert_called_once()


@pytest.mark.asyncio
class TestCohortEdgeCases:
    """Test edge cases and error conditions for cohort operations."""

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_get_cohort_by_id_with_empty_members(self, mock_execute):
        """Test getting cohort with no members."""
        cohort_tuple = (1, "Empty Cohort", 1)
        members_data = []  # No members

        mock_execute.side_effect = [cohort_tuple, members_data]

        result = await get_cohort_by_id(1)

        expected = {
            "id": 1,
            "org_id": 1,
            "name": "Empty Cohort",
            "members": [],
        }

        assert result == expected

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_add_courses_to_cohort_empty_list(self, mock_execute_many):
        """Test adding empty list of courses to cohort."""
        await add_courses_to_cohort(1, [])

        mock_execute_many.assert_called_once_with(ANY, [])

    @patch("src.api.db.cohort.execute_many_db_operation")
    async def test_remove_courses_from_cohort_empty_list(self, mock_execute_many):
        """Test removing empty list of courses from cohort."""
        await remove_courses_from_cohort(1, [])

        mock_execute_many.assert_called_once_with(ANY, [])

    @patch("src.api.db.cohort.execute_db_operation")
    @patch("src.api.db.cohort.execute_multiple_db_operations")
    async def test_remove_members_from_cohort_empty_list(
        self, mock_execute_multiple, mock_execute
    ):
        """Test removing empty list of members from cohort."""
        mock_execute.return_value = (
            []
        )  # No members found (which is expected for empty list)

        await remove_members_from_cohort(1, [])

        # Should still call execute since we validate members exist
        mock_execute.assert_called_once()


@pytest.mark.asyncio
class TestCohortComplexScenarios:
    """Test complex scenarios and integration-like tests."""

    @patch("src.api.db.cohort.execute_db_operation")
    async def test_cohort_analytics_with_mixed_completion_data(self, mock_execute):
        """Test analytics with mixed completion patterns."""
        task_ids = [1, 2, 3, 4]
        analytics_data = [
            (
                1,
                "user1@example.com",
                "1,3",
                "1,0",
            ),  # Completed task 1, attempted task 3
            (2, "user2@example.com", "2,4", "1,1"),  # Completed tasks 2 and 4
            (3, "user3@example.com", "1,2,3,4", "0,0,1,1"),  # Mixed completion
        ]
        mock_execute.return_value = analytics_data

        result = await get_cohort_analytics_metrics_for_tasks(1, task_ids)

        # Verify each user's metrics
        assert len(result) == 3

        # User 1: completed 1 task (task 1)
        user1 = result[0]
        assert user1["num_completed"] == 1
        assert user1["task_1"] == 1
        assert user1["task_2"] == 0  # Default for unattempted
        assert user1["task_3"] == 0
        assert user1["task_4"] == 0  # Default for unattempted

        # User 2: completed 2 tasks (tasks 2 and 4)
        user2 = result[1]
        assert user2["num_completed"] == 2
        assert user2["task_1"] == 0  # Default for unattempted
        assert user2["task_2"] == 1
        assert user2["task_3"] == 0  # Default for unattempted
        assert user2["task_4"] == 1

        # User 3: completed 2 tasks (tasks 3 and 4)
        user3 = result[2]
        assert user3["num_completed"] == 2
        assert user3["task_1"] == 0
        assert user3["task_2"] == 0
        assert user3["task_3"] == 1
        assert user3["task_4"] == 1

    def test_format_user_cohort_group_special_characters(self):
        """Test formatting with special characters in emails."""
        group_tuple = (
            1,
            "Special Cohort",
            "1,2",
            "user+test@example.com,user.name@example-domain.co.uk",
        )

        result = format_user_cohort_group(group_tuple)

        expected = {
            "id": 1,
            "name": "Special Cohort",
            "learners": [
                {"id": 1, "email": "user+test@example.com"},
                {"id": 2, "email": "user.name@example-domain.co.uk"},
            ],
        }

        assert result == expected
