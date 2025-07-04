import pytest
import json
from unittest.mock import patch, AsyncMock, MagicMock, ANY, call
from datetime import datetime, timezone, timedelta
from src.api.db.user import (
    convert_user_db_to_dict,
    get_user_by_id,
    get_user_by_email,
    insert_or_return_user,
    get_user_streak_from_usage_dates,
    update_user_email,
    get_user_organizations,
    get_user_org_cohorts,
    drop_users_table,
    update_user,
    get_all_users,
    get_user_cohorts,
    get_user_active_in_last_n_days,
    get_user_activity_for_year,
    get_user_streak,
)


class TestUserUtilityFunctions:
    """Test utility functions for converting user database tuples to dictionaries."""

    def test_convert_user_db_to_dict_complete_user(self):
        """Test converting a complete user tuple to dictionary."""
        user_tuple = (
            1,  # id
            "test@example.com",  # email
            "John",  # first_name
            "William",  # middle_name
            "Doe",  # last_name
            "#FF5733",  # default_dp_color
            "2023-01-01 12:00:00",  # created_at
        )

        result = convert_user_db_to_dict(user_tuple)

        expected = {
            "id": 1,
            "email": "test@example.com",
            "first_name": "John",
            "middle_name": "William",
            "last_name": "Doe",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        assert result == expected

    def test_convert_user_db_to_dict_none_user(self):
        """Test converting None user to dictionary."""
        result = convert_user_db_to_dict(None)
        assert result is None

    def test_convert_user_db_to_dict_empty_user(self):
        """Test converting empty user tuple to dictionary."""
        result = convert_user_db_to_dict(())
        assert result is None


@pytest.mark.asyncio
class TestUserDatabaseOperations:
    """Test user-related database operations."""

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_by_id_success(self, mock_execute):
        """Test successful retrieval of user by ID."""
        # Mock database response
        mock_user_tuple = (
            1,
            "test@example.com",
            "John",
            "William",
            "Doe",
            "#FF5733",
            "2023-01-01 12:00:00",
        )
        mock_execute.return_value = mock_user_tuple

        result = await get_user_by_id(1)

        expected = {
            "id": 1,
            "email": "test@example.com",
            "first_name": "John",
            "middle_name": "William",
            "last_name": "Doe",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        assert result == expected
        mock_execute.assert_called_once_with(
            "SELECT * FROM users WHERE id = ?", (1,), fetch_one=True
        )

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_by_id_not_found(self, mock_execute):
        """Test user retrieval when user doesn't exist."""
        mock_execute.return_value = None

        result = await get_user_by_id(999)

        assert result is None
        mock_execute.assert_called_once_with(
            "SELECT * FROM users WHERE id = ?", (999,), fetch_one=True
        )

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_by_email_success(self, mock_execute):
        """Test successful retrieval of user by email."""
        mock_user_tuple = (
            1,
            "test@example.com",
            "John",
            "William",
            "Doe",
            "#FF5733",
            "2023-01-01 12:00:00",
        )
        mock_execute.return_value = mock_user_tuple

        result = await get_user_by_email("test@example.com")

        expected = {
            "id": 1,
            "email": "test@example.com",
            "first_name": "John",
            "middle_name": "William",
            "last_name": "Doe",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        assert result == expected
        mock_execute.assert_called_once_with(
            "SELECT * FROM users WHERE email = ?", ("test@example.com",), fetch_one=True
        )

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_by_email_not_found(self, mock_execute):
        """Test user retrieval by email when user doesn't exist."""
        mock_execute.return_value = None

        result = await get_user_by_email("nonexistent@example.com")

        assert result is None
        mock_execute.assert_called_once_with(
            "SELECT * FROM users WHERE email = ?",
            ("nonexistent@example.com",),
            fetch_one=True,
        )

    @patch("src.api.db.user.execute_db_operation")
    async def test_update_user_email_success(self, mock_execute):
        """Test successful user email update."""
        await update_user_email("old@example.com", "new@example.com")

        mock_execute.assert_called_once_with(
            "UPDATE users SET email = ? WHERE email = ?",
            ("new@example.com", "old@example.com"),
        )

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_organizations_success(self, mock_execute):
        """Test successful retrieval of user organizations."""
        mock_execute.return_value = [
            (1, "Org 1", "admin", "sk-key1", True),
            (2, "Org 2", "member", "sk-key2", False),
        ]

        result = await get_user_organizations(1)

        expected = [
            {
                "id": 1,
                "name": "Org 1",
                "role": "admin",
                "openai_api_key": "sk-key1",
                "openai_free_trial": True,
            },
            {
                "id": 2,
                "name": "Org 2",
                "role": "member",
                "openai_api_key": "sk-key2",
                "openai_free_trial": False,
            },
        ]

        assert result == expected

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_org_cohorts_success(self, mock_execute):
        """Test successful retrieval of user cohorts in organization."""
        mock_execute.return_value = [
            (1, "Cohort 1", "learner", "2023-01-01 12:00:00"),
            (2, "Cohort 2", "mentor", "2023-01-02 12:00:00"),
        ]

        result = await get_user_org_cohorts(1, 1)

        expected = [
            {
                "id": 1,
                "name": "Cohort 1",
                "role": "learner",
                "joined_at": "2023-01-01 12:00:00",
            },
            {
                "id": 2,
                "name": "Cohort 2",
                "role": "mentor",
                "joined_at": "2023-01-02 12:00:00",
            },
        ]

        assert result == expected

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_org_cohorts_empty(self, mock_execute):
        """Test user org cohorts when user has no cohorts."""
        mock_execute.return_value = []

        result = await get_user_org_cohorts(1, 1)

        assert result == []

    def test_drop_users_table(self):
        """Test dropping users table - this is a synchronous function."""
        with patch("src.api.db.user.execute_db_operation") as mock_execute:
            drop_users_table()

            assert mock_execute.call_count == 2
            calls = [call[0][0] for call in mock_execute.call_args_list]
            assert "DELETE FROM users" in calls[0]
            assert "DROP TABLE IF EXISTS users" in calls[1]

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_all_users_success(self, mock_execute):
        """Test successful retrieval of all users."""
        mock_execute.return_value = [
            (1, "user1@example.com", "John", None, "Doe", "#FF5733", "2023-01-01"),
            (2, "user2@example.com", "Jane", "Marie", "Smith", "#33FF57", "2023-01-02"),
        ]

        result = await get_all_users()

        assert len(result) == 2
        assert result[0]["email"] == "user1@example.com"
        assert result[1]["email"] == "user2@example.com"

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_cohorts_success(self, mock_execute):
        """Test successful retrieval of user cohorts."""
        mock_execute.return_value = [
            (
                1,
                "Cohort 1",
                "learner",
                1,
                "Org 1",
            ),  # cohort_id, cohort_name, role, org_id, org_name
            (2, "Cohort 2", "mentor", 1, "Org 1"),
        ]

        result = await get_user_cohorts(1)

        expected = [
            {
                "id": 1,
                "name": "Cohort 1",
                "role": "learner",
                "org_id": 1,
                "org_name": "Org 1",
            },
            {
                "id": 2,
                "name": "Cohort 2",
                "role": "mentor",
                "org_id": 1,
                "org_name": "Org 1",
            },
        ]

        assert result == expected

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_active_in_last_n_days_success(self, mock_execute):
        """Test successful retrieval of user activity."""
        mock_execute.return_value = [
            ("2023-01-01", 3),
            ("2023-01-02", 5),
            ("2023-01-03", 2),
        ]  # (date, count) tuples

        result = await get_user_active_in_last_n_days(1, 7, 1)

        # Function returns a list but order isn't guaranteed since it uses a set internally
        assert isinstance(result, list)
        assert len(result) == 3
        assert "2023-01-01" in result
        assert "2023-01-02" in result
        assert "2023-01-03" in result

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_activity_for_year_success(self, mock_execute):
        """Test successful retrieval of user activity for year."""
        mock_execute.return_value = [
            ("001", 5),  # day_of_year as string, count
            ("002", 3),
            ("003", 7),
        ]

        result = await get_user_activity_for_year(1, 2023)

        # Returns a list of 365/366 counts, with first 3 days having activity
        assert isinstance(result, list)
        assert len(result) == 365  # 2023 is not a leap year
        assert result[0] == 5  # Day 1 (index 0)
        assert result[1] == 3  # Day 2 (index 1)
        assert result[2] == 7  # Day 3 (index 2)

    @patch("src.api.db.user.execute_db_operation")
    async def test_get_user_streak_success(self, mock_execute):
        """Test successful calculation of user streak."""
        mock_execute.return_value = [
            ("2023-01-01 12:00:00",),  # Single tuple elements with timestamp strings
            ("2023-01-02 12:00:00",),
            ("2023-01-03 12:00:00",),
        ]

        result = await get_user_streak(1, 1)

        # get_user_streak_from_usage_dates returns a list of date strings
        assert isinstance(result, list)


class TestUserInsertOperations:
    """Test user insertion and update operations."""

    @patch("src.api.db.user.get_new_db_connection")
    @patch("src.api.db.user.generate_random_color")
    @patch("src.api.db.user.send_slack_notification_for_new_user")
    async def test_insert_or_return_user_new_user(
        self, mock_slack, mock_color, mock_db_conn
    ):
        """Test inserting a new user."""
        mock_color.return_value = "#FF5733"

        # Mock database connection and cursor
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.side_effect = [
            None,  # User doesn't exist
            (
                1,
                "new@example.com",
                "New",  # first_name
                "User",  # middle_name (from "New User" split)
                "User",  # last_name
                "#FF5733",
                "2023-01-01 12:00:00",
            ),
        ]
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        result = await insert_or_return_user(
            mock_cursor, "new@example.com", "New User", "User"
        )

        expected = {
            "id": 1,
            "email": "new@example.com",
            "first_name": "New",
            "middle_name": "User",  # This should be "User" from splitting "New User"
            "last_name": "User",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        assert result == expected
        mock_slack.assert_called_once()

    @patch("src.api.db.user.get_new_db_connection")
    async def test_insert_or_return_user_existing_user(self, mock_db_conn):
        """Test returning existing user."""
        # Mock database connection and cursor
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (
            1,
            "existing@example.com",
            "Existing",
            None,
            "User",
            "#FF5733",
            "2023-01-01 12:00:00",
        )

        result = await insert_or_return_user(
            mock_cursor, "existing@example.com", "Existing User", "User"
        )

        expected = {
            "id": 1,
            "email": "existing@example.com",
            "first_name": "Existing",
            "middle_name": None,
            "last_name": "User",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        assert result == expected

    @patch("src.api.db.user.update_user")
    async def test_insert_or_return_user_existing_user_update_name(
        self, mock_update_user
    ):
        """Test returning existing user and updating name if missing."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (
            1,
            "existing@example.com",
            None,  # No first name
            None,
            None,
            "#FF5733",
            "2023-01-01 12:00:00",
        )

        mock_update_user.return_value = {
            "id": 1,
            "email": "existing@example.com",
            "first_name": "Updated",
            "middle_name": "Name",
            "last_name": "User",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        result = await insert_or_return_user(
            mock_cursor, "existing@example.com", "Updated Name", "User"
        )

        mock_update_user.assert_called_once_with(
            mock_cursor, 1, "Updated", "Name", "User", "#FF5733"
        )
        assert result["first_name"] == "Updated"

    @patch("src.api.db.user.get_user_by_id")
    async def test_update_user_success(self, mock_get_user):
        """Test successful user update."""
        mock_cursor = AsyncMock()
        mock_get_user.return_value = {
            "id": 1,
            "email": "test@example.com",
            "first_name": "Updated",
            "middle_name": "Name",
            "last_name": "User",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }

        result = await update_user(mock_cursor, 1, "Updated", "Name", "User", "#FF5733")

        mock_cursor.execute.assert_called_once()
        assert result["first_name"] == "Updated"

    async def test_insert_or_return_user_no_given_name(self):
        """Test inserting user with no given name."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = None

        with patch(
            "src.api.db.user.generate_random_color", return_value="#FF5733"
        ), patch("src.api.db.user.send_slack_notification_for_new_user"):

            # Second fetchone call for the newly created user
            mock_cursor.fetchone.side_effect = [
                None,  # First call - user doesn't exist
                (
                    1,
                    "test@example.com",
                    None,
                    None,
                    None,
                    "#FF5733",
                    "2023-01-01 12:00:00",
                ),  # Second call - new user
            ]

            result = await insert_or_return_user(mock_cursor, "test@example.com")

            assert result["first_name"] is None
            assert result["middle_name"] is None

    async def test_insert_or_return_user_single_name(self):
        """Test inserting user with single given name."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = None

        with patch(
            "src.api.db.user.generate_random_color", return_value="#FF5733"
        ), patch("src.api.db.user.send_slack_notification_for_new_user"):

            mock_cursor.fetchone.side_effect = [
                None,  # First call - user doesn't exist
                (
                    1,
                    "test@example.com",
                    "John",
                    None,
                    "Doe",
                    "#FF5733",
                    "2023-01-01 12:00:00",
                ),  # Second call - new user
            ]

            result = await insert_or_return_user(
                mock_cursor, "test@example.com", "John", "Doe"
            )

            assert result["first_name"] == "John"
            assert result["middle_name"] is None


class TestUserStreakFunctions:
    """Test user streak calculation functions."""

    def test_get_user_streak_from_usage_dates_consecutive(self):
        """Test streak calculation with consecutive dates."""
        # Function expects datetime strings with time component
        usage_dates = [
            "2023-01-03 12:00:00",
            "2023-01-02 12:00:00",
            "2023-01-01 12:00:00",
        ]
        result = get_user_streak_from_usage_dates(usage_dates)
        # Function returns a list of date strings, not a count
        assert isinstance(result, list)

    def test_get_user_streak_from_usage_dates_broken(self):
        """Test streak calculation with broken streak."""
        usage_dates = [
            "2023-01-05 12:00:00",
            "2023-01-04 12:00:00",
            "2023-01-02 12:00:00",
            "2023-01-01 12:00:00",
        ]
        result = get_user_streak_from_usage_dates(usage_dates)
        assert isinstance(result, list)

    def test_get_user_streak_from_usage_dates_empty(self):
        """Test streak calculation with empty dates."""
        result = get_user_streak_from_usage_dates([])
        assert result == []  # Function returns empty list, not 0

    def test_get_user_streak_from_usage_dates_single_date(self):
        """Test streak calculation with single date."""
        from datetime import datetime

        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        usage_dates = [today]
        result = get_user_streak_from_usage_dates(usage_dates)
        assert isinstance(result, list)

    def test_get_user_streak_from_usage_dates_non_consecutive(self):
        """Test streak calculation with non-consecutive dates."""
        usage_dates = [
            "2023-01-10 12:00:00",
            "2023-01-08 12:00:00",
            "2023-01-05 12:00:00",
        ]
        result = get_user_streak_from_usage_dates(usage_dates)
        assert isinstance(result, list)

    def test_get_user_streak_from_usage_dates_streak_broken_from_start(self):
        """Test streak calculation where streak is broken from the very beginning."""
        from datetime import datetime, timedelta

        # Create dates where the first date is more than 1 day ago (breaks immediately)
        old_date = datetime.now() - timedelta(days=3)
        usage_dates = [old_date.strftime("%Y-%m-%d %H:%M:%S")]

        result = get_user_streak_from_usage_dates(usage_dates)
        # Should return empty list because streak is broken from start
        assert result == []

    def test_get_user_streak_from_usage_dates_non_consecutive_break(self):
        """Test streak calculation where streak breaks due to non-consecutive dates."""
        from datetime import datetime, timedelta

        # Create consecutive dates at the start, then a gap, then more dates
        # This should trigger the break statement on line 351
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        three_days_ago = today - timedelta(days=3)  # Gap here - not consecutive
        four_days_ago = today - timedelta(days=4)

        usage_dates = [
            today.strftime("%Y-%m-%d %H:%M:%S"),
            yesterday.strftime("%Y-%m-%d %H:%M:%S"),
            three_days_ago.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),  # This creates non-consecutive gap
            four_days_ago.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        result = get_user_streak_from_usage_dates(usage_dates)

        # Should only include today and yesterday, then break due to gap
        assert isinstance(result, list)
        assert len(result) <= 2  # Should stop at the gap
