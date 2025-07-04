import pytest
from unittest.mock import patch, AsyncMock
from collections import defaultdict
from api.db.analytics import (
    get_usage_summary_by_organization,
    get_cohort_completion,
    get_cohort_course_attempt_data,
    get_cohort_streaks,
)
from api.models import LeaderboardViewType, TaskType, TaskStatus


class TestGetUsageSummaryByOrganization:
    """Test suite for get_usage_summary_by_organization function."""

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_usage_summary_no_filter(self, mock_db):
        """Test getting usage summary without any filter period."""
        # Mock database response
        mock_db.return_value = [
            (1, "Organization A", 150),
            (2, "Organization B", 75),
            (3, "Organization C", 25),
        ]

        result = await get_usage_summary_by_organization()

        assert len(result) == 3
        assert result[0] == {
            "org_id": 1,
            "org_name": "Organization A",
            "user_message_count": 150,
        }
        assert result[1] == {
            "org_id": 2,
            "org_name": "Organization B",
            "user_message_count": 75,
        }
        assert result[2] == {
            "org_id": 3,
            "org_name": "Organization C",
            "user_message_count": 25,
        }

        # Verify no date filter was applied
        call_args = mock_db.call_args[0][0]
        assert "AND ch.created_at >=" not in call_args

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_usage_summary_last_day_filter(self, mock_db):
        """Test getting usage summary with last_day filter."""
        mock_db.return_value = [(1, "Organization A", 50)]

        result = await get_usage_summary_by_organization(filter_period="last_day")

        assert len(result) == 1
        assert result[0]["user_message_count"] == 50

        # Verify last_day filter was applied
        call_args = mock_db.call_args[0][0]
        assert "AND ch.created_at >= datetime('now', '-1 day')" in call_args

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_usage_summary_current_month_filter(self, mock_db):
        """Test getting usage summary with current_month filter."""
        mock_db.return_value = [(1, "Organization A", 100)]

        result = await get_usage_summary_by_organization(filter_period="current_month")

        assert len(result) == 1
        assert result[0]["user_message_count"] == 100

        # Verify current_month filter was applied
        call_args = mock_db.call_args[0][0]
        assert "AND ch.created_at >= datetime('now', 'start of month')" in call_args

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_usage_summary_current_year_filter(self, mock_db):
        """Test getting usage summary with current_year filter."""
        mock_db.return_value = [(1, "Organization A", 1000)]

        result = await get_usage_summary_by_organization(filter_period="current_year")

        assert len(result) == 1
        assert result[0]["user_message_count"] == 1000

        # Verify current_year filter was applied
        call_args = mock_db.call_args[0][0]
        assert "AND ch.created_at >= datetime('now', 'start of year')" in call_args

    @pytest.mark.asyncio
    async def test_get_usage_summary_invalid_filter_period(self):
        """Test that invalid filter period raises ValueError."""
        with pytest.raises(ValueError, match="Invalid filter period"):
            await get_usage_summary_by_organization(filter_period="invalid_period")

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_usage_summary_empty_results(self, mock_db):
        """Test getting usage summary with empty database results."""
        mock_db.return_value = []

        result = await get_usage_summary_by_organization()

        assert result == []


class TestGetCohortCompletion:
    """Test suite for get_cohort_completion function."""

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_basic(self, mock_db):
        """Test basic cohort completion functionality."""
        # Mock database responses in order of calls
        mock_db.side_effect = [
            # First call: completed tasks
            [(1, 101), (2, 103)],
            # Second call: completed questions
            [(1, 201), (2, 202)],
            # Third call: learning material tasks
            [(101,), (103,)],
            # Fourth call: quiz/exam questions
            [(104, 301), (104, 302), (105, 303)],
        ]

        result = await get_cohort_completion(
            cohort_id=1, user_ids=[1, 2], course_id=None
        )

        # Verify structure
        assert 1 in result
        assert 2 in result

        # User 1 should have task 101 completed and 103 not completed
        assert result[1][101]["is_complete"] is True
        assert result[1][103]["is_complete"] is False

        # User 2 should have task 103 completed and 101 not completed
        assert result[2][101]["is_complete"] is False
        assert result[2][103]["is_complete"] is True

        # Check quiz tasks
        assert 104 in result[1]
        assert 104 in result[2]
        assert len(result[1][104]["questions"]) == 2
        assert len(result[2][104]["questions"]) == 2

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_with_course_id(self, mock_db):
        """Test cohort completion with specific course ID."""
        mock_db.side_effect = [
            [(1, 101)],  # completed tasks
            [(1, 201)],  # completed questions
            [(101,)],  # learning material tasks
            [(102, 301)],  # quiz questions
        ]

        result = await get_cohort_completion(cohort_id=1, user_ids=[1], course_id=5)

        assert 1 in result
        assert 101 in result[1]

        # Verify course_id parameter was passed to queries
        call_args_list = [call[0] for call in mock_db.call_args_list]

        # Third call should include course_id filter
        assert "AND ct.course_id = ?" in call_args_list[2][0]
        assert call_args_list[2][1] == (1, 5)

        # Fourth call should include course_id filter
        assert "AND ct.course_id = ?" in call_args_list[3][0]
        assert call_args_list[3][1] == (1, 5)

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_quiz_partial_completion(self, mock_db):
        """Test quiz task with partial question completion."""
        mock_db.side_effect = [
            [],  # no completed tasks
            [(1, 301)],  # user 1 completed question 301 only
            [],  # no learning material tasks
            [(104, 301), (104, 302), (104, 303)],  # quiz with 3 questions
        ]

        result = await get_cohort_completion(cohort_id=1, user_ids=[1], course_id=None)

        # Task 104 should not be complete since only 1 of 3 questions completed
        assert result[1][104]["is_complete"] is False
        assert len(result[1][104]["questions"]) == 3

        # Check individual question completion
        questions = result[1][104]["questions"]
        completed_questions = [q for q in questions if q["is_complete"]]
        assert len(completed_questions) == 1
        assert completed_questions[0]["question_id"] == 301

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_quiz_full_completion(self, mock_db):
        """Test quiz task with full question completion."""
        mock_db.side_effect = [
            [],  # no completed tasks
            [(1, 301), (1, 302)],  # user 1 completed all questions
            [],  # no learning material tasks
            [(104, 301), (104, 302)],  # quiz with 2 questions
        ]

        result = await get_cohort_completion(cohort_id=1, user_ids=[1], course_id=None)

        # Task 104 should be complete since all questions completed
        assert result[1][104]["is_complete"] is True
        assert len(result[1][104]["questions"]) == 2

        # All questions should be marked complete
        for question in result[1][104]["questions"]:
            assert question["is_complete"] is True

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_empty_results(self, mock_db):
        """Test cohort completion with no data."""
        mock_db.side_effect = [[], [], [], []]

        result = await get_cohort_completion(
            cohort_id=1, user_ids=[1, 2], course_id=None
        )

        # Should return empty defaultdict when no tasks exist
        assert len(result) == 0

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_multiple_users(self, mock_db):
        """Test cohort completion with multiple users having different completions."""
        mock_db.side_effect = [
            [(1, 101), (2, 102), (3, 101)],  # completed tasks
            [(1, 301), (2, 302)],  # completed questions
            [(101,), (102,)],  # learning material tasks
            [(103, 301), (103, 302)],  # quiz questions
        ]

        result = await get_cohort_completion(
            cohort_id=1, user_ids=[1, 2, 3], course_id=None
        )

        # User 1: completed task 101, partially completed quiz 103 (1/2 questions)
        assert result[1][101]["is_complete"] is True
        assert result[1][102]["is_complete"] is False
        assert result[1][103]["is_complete"] is False

        # User 2: completed task 102, partially completed quiz 103 (1/2 questions)
        assert result[2][101]["is_complete"] is False
        assert result[2][102]["is_complete"] is True
        assert result[2][103]["is_complete"] is False

        # User 3: completed task 101, no quiz questions completed
        assert result[3][101]["is_complete"] is True
        assert result[3][102]["is_complete"] is False
        assert result[3][103]["is_complete"] is False

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_completion_only_learning_material(self, mock_db):
        """Test cohort completion with only learning material tasks."""
        mock_db.side_effect = [
            [(1, 101)],  # completed tasks
            [],  # no completed questions
            [(101,), (102,)],  # learning material tasks
            [],  # no quiz questions
        ]

        result = await get_cohort_completion(cohort_id=1, user_ids=[1], course_id=None)

        # Should have learning material tasks only
        assert 1 in result
        assert result[1][101]["is_complete"] is True
        assert result[1][102]["is_complete"] is False


class TestGetCohortCourseAttemptData:
    """Test suite for get_cohort_course_attempt_data function."""

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_course_attempt_data_basic(self, mock_db):
        """Test basic cohort course attempt data functionality."""
        mock_db.side_effect = [
            [(1,), (2,)],  # task completions for users 1 and 2
            [(3,)],  # chat messages for user 3
        ]

        result = await get_cohort_course_attempt_data(
            cohort_learner_ids=[1, 2, 3], course_id=5
        )

        # All users should be in result
        assert 1 in result
        assert 2 in result
        assert 3 in result

        # Users 1, 2, and 3 should have attempted the course
        assert result[1][5]["has_attempted"] is True
        assert result[2][5]["has_attempted"] is True
        assert result[3][5]["has_attempted"] is True

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_course_attempt_data_no_attempts(self, mock_db):
        """Test cohort course attempt data with no attempts."""
        mock_db.side_effect = [
            [],  # no task completions
            [],  # no chat messages
        ]

        result = await get_cohort_course_attempt_data(
            cohort_learner_ids=[1, 2], course_id=5
        )

        # Users should be in result but with no attempts
        assert 1 in result
        assert 2 in result
        assert result[1][5]["has_attempted"] is False
        assert result[2][5]["has_attempted"] is False

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_course_attempt_data_mixed_attempts(self, mock_db):
        """Test cohort course attempt data with mixed attempt patterns."""
        mock_db.side_effect = [
            [(1,)],  # only user 1 has task completions
            [(2,)],  # only user 2 has chat messages
        ]

        result = await get_cohort_course_attempt_data(
            cohort_learner_ids=[1, 2, 3], course_id=5
        )

        # Users 1 and 2 should have attempted, user 3 should not
        assert result[1][5]["has_attempted"] is True
        assert result[2][5]["has_attempted"] is True
        assert result[3][5]["has_attempted"] is False

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_course_attempt_data_user_in_both_sources(self, mock_db):
        """Test user appearing in both task completions and chat messages."""
        mock_db.side_effect = [
            [(1,)],  # user 1 in task completions
            [(1,)],  # user 1 also in chat messages
        ]

        result = await get_cohort_course_attempt_data(
            cohort_learner_ids=[1], course_id=5
        )

        # User 1 should still show as attempted (not duplicated)
        assert result[1][5]["has_attempted"] is True

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_course_attempt_data_empty_learner_list(self, mock_db):
        """Test with empty learner list."""
        mock_db.side_effect = [[], []]

        result = await get_cohort_course_attempt_data(
            cohort_learner_ids=[], course_id=5
        )

        # Should return empty result
        assert result == {}


class TestGetCohortStreaks:
    """Test suite for get_cohort_streaks function."""

    @pytest.mark.asyncio
    @patch("api.db.analytics.get_user_streak_from_usage_dates")
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_streaks_all_time(self, mock_db, mock_streak):
        """Test getting cohort streaks for all time view."""
        mock_db.return_value = [
            (
                1,
                "user1@example.com",
                "John",
                "M",
                "Doe",
                "2023-01-01,2023-01-02,2023-01-03",
            ),
            (2, "user2@example.com", "Jane", None, "Smith", "2023-01-01,2023-01-02"),
            (3, "user3@example.com", "Bob", None, "Wilson", None),
        ]

        mock_streak.side_effect = [
            ["2023-01-01", "2023-01-02", "2023-01-03"],  # 3-day streak
            ["2023-01-01", "2023-01-02"],  # 2-day streak
        ]

        result = await get_cohort_streaks(
            view=LeaderboardViewType.ALL_TIME, cohort_id=1
        )

        assert len(result) == 3

        # User 1 with 3-day streak
        assert result[0]["user"]["id"] == 1
        assert result[0]["user"]["email"] == "user1@example.com"
        assert result[0]["user"]["first_name"] == "John"
        assert result[0]["user"]["middle_name"] == "M"
        assert result[0]["user"]["last_name"] == "Doe"
        assert result[0]["streak_count"] == 3

        # User 2 with 2-day streak
        assert result[1]["user"]["id"] == 2
        assert result[1]["user"]["email"] == "user2@example.com"
        assert result[1]["user"]["first_name"] == "Jane"
        assert result[1]["user"]["middle_name"] is None
        assert result[1]["user"]["last_name"] == "Smith"
        assert result[1]["streak_count"] == 2

        # User 3 with no usage dates
        assert result[2]["user"]["id"] == 3
        assert result[2]["user"]["email"] == "user3@example.com"
        assert result[2]["streak_count"] == 0

        # Verify no date filter was applied for ALL_TIME
        call_args = mock_db.call_args[0][0]
        assert "AND DATE(datetime(timestamp" not in call_args

    @pytest.mark.asyncio
    @patch("api.db.analytics.get_user_streak_from_usage_dates")
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_streaks_weekly(self, mock_db, mock_streak):
        """Test getting cohort streaks for weekly view."""
        mock_db.return_value = [
            (1, "user1@example.com", "John", None, "Doe", "2023-01-01,2023-01-02"),
        ]

        mock_streak.return_value = ["2023-01-01", "2023-01-02"]

        result = await get_cohort_streaks(view=LeaderboardViewType.WEEKLY, cohort_id=1)

        assert len(result) == 1
        assert result[0]["streak_count"] == 2

        # Verify weekly date filter was applied
        call_args = mock_db.call_args[0][0]
        assert (
            "AND DATE(datetime(timestamp, '+5 hours', '+30 minutes')) > DATE('now', 'weekday 0', '-7 days')"
            in call_args
        )

    @pytest.mark.asyncio
    @patch("api.db.analytics.get_user_streak_from_usage_dates")
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_streaks_monthly(self, mock_db, mock_streak):
        """Test getting cohort streaks for monthly view."""
        mock_db.return_value = [
            (1, "user1@example.com", "John", None, "Doe", "2023-01-01"),
        ]

        mock_streak.return_value = ["2023-01-01"]

        result = await get_cohort_streaks(view=LeaderboardViewType.MONTHLY, cohort_id=1)

        assert len(result) == 1
        assert result[0]["streak_count"] == 1

        # Verify monthly date filter was applied
        call_args = mock_db.call_args[0][0]
        assert (
            "AND strftime('%Y-%m', datetime(timestamp, '+5 hours', '+30 minutes')) = strftime('%Y-%m', 'now')"
            in call_args
        )

    @pytest.mark.asyncio
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_streaks_empty_results(self, mock_db):
        """Test getting cohort streaks with no data."""
        mock_db.return_value = []

        result = await get_cohort_streaks(
            view=LeaderboardViewType.ALL_TIME, cohort_id=1
        )

        assert result == []

    @pytest.mark.asyncio
    @patch("api.db.analytics.get_user_streak_from_usage_dates")
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_streaks_user_with_empty_usage_dates(
        self, mock_db, mock_streak
    ):
        """Test getting cohort streaks for user with empty usage dates string."""
        mock_db.return_value = [
            (1, "user1@example.com", "John", None, "Doe", ""),
        ]

        result = await get_cohort_streaks(
            view=LeaderboardViewType.ALL_TIME, cohort_id=1
        )

        assert len(result) == 1
        assert result[0]["streak_count"] == 0

        # get_user_streak_from_usage_dates should not be called for empty string
        mock_streak.assert_not_called()

    @pytest.mark.asyncio
    @patch("api.db.analytics.get_user_streak_from_usage_dates")
    @patch("api.db.analytics.execute_db_operation")
    async def test_get_cohort_streaks_usage_dates_processing(
        self, mock_db, mock_streak
    ):
        """Test that usage dates are processed correctly."""
        mock_db.return_value = [
            (
                1,
                "user1@example.com",
                "John",
                None,
                "Doe",
                "2023-01-03,2023-01-01,2023-01-02",
            ),
        ]

        mock_streak.return_value = ["2023-01-01", "2023-01-02", "2023-01-03"]

        result = await get_cohort_streaks(
            view=LeaderboardViewType.ALL_TIME, cohort_id=1
        )

        # Verify usage dates were sorted in reverse order before being passed to streak function
        mock_streak.assert_called_once_with(["2023-01-03", "2023-01-02", "2023-01-01"])
        assert result[0]["streak_count"] == 3
