import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import aiohttp
from src.api.slack import (
    send_slack_notification,
    send_slack_notification_for_new_user,
    send_slack_notification_for_learner_added_to_cohort,
    send_slack_notification_for_member_added_to_org,
    send_slack_notification_for_new_org,
    send_slack_notification_for_new_course,
    send_slack_notification_for_usage_stats,
)


@pytest.mark.asyncio
class TestSendSlackNotification:
    """Test the core send_slack_notification function."""

    @patch("src.api.slack.aiohttp.ClientSession")
    async def test_send_slack_notification_success(self, mock_session_class):
        """Test successful Slack notification sending."""
        # Setup mocks
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200

        # Setup the nested async context managers properly
        mock_session.__aenter__.return_value = mock_session
        mock_session.__aexit__.return_value = None
        mock_session.post = MagicMock(return_value=mock_response)
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_session_class.return_value = mock_session

        message = {"text": "Test message"}
        webhook_url = "https://hooks.slack.com/test"

        # Execute
        await send_slack_notification(message, webhook_url)

        # Verify
        mock_session_class.assert_called_once()
        mock_session.post.assert_called_once_with(webhook_url, json=message)

    @patch("src.api.slack.aiohttp.ClientSession")
    async def test_send_slack_notification_failure(self, mock_session_class):
        """Test Slack notification sending with failure response."""
        # Setup mocks
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text.return_value = "Internal Server Error"

        # Setup the nested async context managers properly
        mock_session.__aenter__.return_value = mock_session
        mock_session.__aexit__.return_value = None
        mock_session.post = MagicMock(return_value=mock_response)
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_session_class.return_value = mock_session

        message = {"text": "Test message"}
        webhook_url = "https://hooks.slack.com/test"

        # Execute
        with patch("builtins.print") as mock_print:
            await send_slack_notification(message, webhook_url)

        # Verify
        mock_response.text.assert_called_once()
        mock_print.assert_called_once_with(
            "Failed to send Slack notification: 500 - Internal Server Error"
        )


@pytest.mark.asyncio
class TestSlackNotificationForNewUser:
    """Test send_slack_notification_for_new_user function."""

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_webhook_url(self, mock_send, mock_settings):
        """Test sending notification when webhook URL is configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = "https://hooks.slack.com/test"
        user = {"email": "test@example.com", "id": 123}

        # Execute
        await send_slack_notification_for_new_user(user)

        # Verify
        expected_message = {"text": "User created: test@example.com UserId: 123"}
        mock_send.assert_called_once_with(
            expected_message, "https://hooks.slack.com/test"
        )

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_without_webhook_url(
        self, mock_send, mock_settings
    ):
        """Test not sending notification when webhook URL is not configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = None
        user = {"email": "test@example.com", "id": 123}

        # Execute
        await send_slack_notification_for_new_user(user)

        # Verify
        mock_send.assert_not_called()


@pytest.mark.asyncio
class TestSlackNotificationForLearnerAddedToCohort:
    """Test send_slack_notification_for_learner_added_to_cohort function."""

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_webhook_url(self, mock_send, mock_settings):
        """Test sending notification when webhook URL is configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = "https://hooks.slack.com/test"
        user_invited = {"email": "learner@example.com", "id": 456}

        # Execute
        await send_slack_notification_for_learner_added_to_cohort(
            user_invited, "test-school", 789, "Test Cohort", 101
        )

        # Verify
        expected_message = {
            "text": "Learner added to cohort: learner@example.com UserId: 456\n"
            "School: test-school (SchoolId: 789)\n"
            "Cohort: Test Cohort (CohortId: 101)"
        }
        mock_send.assert_called_once_with(
            expected_message, "https://hooks.slack.com/test"
        )

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_without_webhook_url(
        self, mock_send, mock_settings
    ):
        """Test not sending notification when webhook URL is not configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = None
        user_invited = {"email": "learner@example.com", "id": 456}

        # Execute
        await send_slack_notification_for_learner_added_to_cohort(
            user_invited, "test-school", 789, "Test Cohort", 101
        )

        # Verify
        mock_send.assert_not_called()


@pytest.mark.asyncio
class TestSlackNotificationForMemberAddedToOrg:
    """Test send_slack_notification_for_member_added_to_org function."""

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_webhook_url(self, mock_send, mock_settings):
        """Test sending notification when webhook URL is configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = "https://hooks.slack.com/test"
        user_added = {"email": "admin@example.com", "id": 789}

        # Execute
        await send_slack_notification_for_member_added_to_org(
            user_added, "test-school", 456
        )

        # Verify
        expected_message = {
            "text": "User added as admin: admin@example.com UserId: 789\n"
            "School: test-school (SchoolId: 456)"
        }
        mock_send.assert_called_once_with(
            expected_message, "https://hooks.slack.com/test"
        )

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_without_webhook_url(
        self, mock_send, mock_settings
    ):
        """Test not sending notification when webhook URL is not configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = None
        user_added = {"email": "admin@example.com", "id": 789}

        # Execute
        await send_slack_notification_for_member_added_to_org(
            user_added, "test-school", 456
        )

        # Verify
        mock_send.assert_not_called()


@pytest.mark.asyncio
class TestSlackNotificationForNewOrg:
    """Test send_slack_notification_for_new_org function."""

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_webhook_url(self, mock_send, mock_settings):
        """Test sending notification when webhook URL is configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = "https://hooks.slack.com/test"
        created_by = {"email": "creator@example.com", "id": 111}

        # Execute
        await send_slack_notification_for_new_org("new-school", 222, created_by)

        # Verify
        expected_message = {
            "text": "New school created: new-school (SchoolId: 222)\n"
            "Created by: creator@example.com (UserId: 111)"
        }
        mock_send.assert_called_once_with(
            expected_message, "https://hooks.slack.com/test"
        )

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_without_webhook_url(
        self, mock_send, mock_settings
    ):
        """Test not sending notification when webhook URL is not configured."""
        # Setup
        mock_settings.slack_user_signup_webhook_url = None
        created_by = {"email": "creator@example.com", "id": 111}

        # Execute
        await send_slack_notification_for_new_org("new-school", 222, created_by)

        # Verify
        mock_send.assert_not_called()


@pytest.mark.asyncio
class TestSlackNotificationForNewCourse:
    """Test send_slack_notification_for_new_course function."""

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_webhook_url(self, mock_send, mock_settings):
        """Test sending notification when webhook URL is configured."""
        # Setup
        mock_settings.slack_course_created_webhook_url = "https://hooks.slack.com/test"

        # Execute
        await send_slack_notification_for_new_course(
            "Python Basics", 333, "test-school", 444
        )

        # Verify
        expected_message = {
            "text": "New course created: Python Basics (CourseId: 333)\n"
            "School: test-school (SchoolId: 444)"
        }
        mock_send.assert_called_once_with(
            expected_message, "https://hooks.slack.com/test"
        )

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_without_webhook_url(
        self, mock_send, mock_settings
    ):
        """Test not sending notification when webhook URL is not configured."""
        # Setup
        mock_settings.slack_course_created_webhook_url = None

        # Execute
        await send_slack_notification_for_new_course(
            "Python Basics", 333, "test-school", 444
        )

        # Verify
        mock_send.assert_not_called()


@pytest.mark.asyncio
class TestSlackNotificationForUsageStats:
    """Test send_slack_notification_for_usage_stats function."""

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_webhook_url(self, mock_send, mock_settings):
        """Test sending usage stats notification when webhook URL is configured."""
        # Setup
        mock_settings.slack_usage_stats_webhook_url = "https://hooks.slack.com/test"

        last_day_stats = {
            "org": [
                {"org_name": "School A", "user_message_count": 100},
                {"org_name": "School B", "user_message_count": 50},
            ],
            "model": {"gpt-4": 80, "gpt-3.5-turbo": 70},
        }

        current_month_stats = {
            "org": [{"org_name": "School A", "user_message_count": 1000}],
            "model": {"gpt-4": 800},
        }

        current_year_stats = {
            "org": [{"org_name": "School A", "user_message_count": 5000}],
            "model": {"gpt-4": 4000},
        }

        # Execute
        await send_slack_notification_for_usage_stats(
            last_day_stats, current_month_stats, current_year_stats
        )

        # Verify
        mock_send.assert_called_once()
        call_args = mock_send.call_args
        message = call_args[0][0]
        webhook_url = call_args[0][1]

        assert webhook_url == "https://hooks.slack.com/test"
        assert "text" in message
        assert "Last 24 Hours" in message["text"]
        assert "This Month" in message["text"]
        assert "This Year" in message["text"]
        assert "School A" in message["text"]
        assert "gpt-4" in message["text"]

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_without_webhook_url(
        self, mock_send, mock_settings
    ):
        """Test not sending notification when webhook URL is not configured."""
        # Setup
        mock_settings.slack_usage_stats_webhook_url = None

        # Execute
        await send_slack_notification_for_usage_stats({}, {}, {})

        # Verify
        mock_send.assert_not_called()

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_empty_stats(self, mock_send, mock_settings):
        """Test sending notification with empty stats data."""
        # Setup
        mock_settings.slack_usage_stats_webhook_url = "https://hooks.slack.com/test"

        empty_stats = {"org": [], "model": {}}

        # Execute
        await send_slack_notification_for_usage_stats(
            empty_stats, empty_stats, empty_stats
        )

        # Verify
        mock_send.assert_called_once()
        call_args = mock_send.call_args
        message = call_args[0][0]

        assert "No usage data" in message["text"]
        assert "No model data" in message["text"]

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_long_names(self, mock_send, mock_settings):
        """Test sending notification with long organization and model names."""
        # Setup
        mock_settings.slack_usage_stats_webhook_url = "https://hooks.slack.com/test"

        stats_with_long_names = {
            "org": [
                {
                    "org_name": "This is a very long organization name that exceeds the character limit",
                    "user_message_count": 100,
                }
            ],
            "model": {
                "very-long-model-name-that-exceeds-the-normal-character-limit-for-display": 50
            },
        }

        # Execute
        await send_slack_notification_for_usage_stats(
            stats_with_long_names, stats_with_long_names, stats_with_long_names
        )

        # Verify
        mock_send.assert_called_once()
        call_args = mock_send.call_args
        message = call_args[0][0]

        # Check that long names are truncated
        assert ".." in message["text"]

    @patch("src.api.slack.settings")
    @patch("src.api.slack.send_slack_notification")
    async def test_send_notification_with_many_orgs_and_models(
        self, mock_send, mock_settings
    ):
        """Test sending notification with more than 5 orgs and models."""
        # Setup
        mock_settings.slack_usage_stats_webhook_url = "https://hooks.slack.com/test"

        many_orgs = [
            {"org_name": f"School {i}", "user_message_count": 100 - i}
            for i in range(10)
        ]
        many_models = {f"model-{i}": 50 - i for i in range(10)}

        stats_with_many = {"org": many_orgs, "model": many_models}

        # Execute
        await send_slack_notification_for_usage_stats(
            stats_with_many, stats_with_many, stats_with_many
        )

        # Verify
        mock_send.assert_called_once()
        call_args = mock_send.call_args
        message = call_args[0][0]

        # Check that "+X more" appears for both orgs and models
        assert "+5 more" in message["text"]
