import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import pandas as pd
from src.api.cron import get_model_summary_stats, send_usage_summary_stats


class TestGetModelSummaryStats:
    """Test the get_model_summary_stats function."""

    @patch("src.api.cron.get_raw_traces")
    def test_get_model_summary_stats_success(self, mock_get_raw_traces):
        """Test successful model summary statistics retrieval."""
        # Create mock DataFrame
        mock_df = pd.DataFrame(
            {
                "attributes.llm.model_name": [
                    "gpt-4",
                    "gpt-4",
                    "gpt-3.5-turbo",
                    "gpt-4",
                ],
                "other_column": [1, 2, 3, 4],
            }
        )
        mock_get_raw_traces.return_value = mock_df

        # Call the function
        result = get_model_summary_stats("last_day")

        # Verify the result
        expected = {"gpt-4": 3, "gpt-3.5-turbo": 1}
        assert result == expected
        mock_get_raw_traces.assert_called_once_with("last_day")

    @patch("src.api.cron.get_raw_traces")
    def test_get_model_summary_stats_empty_df(self, mock_get_raw_traces):
        """Test model summary statistics with empty DataFrame."""
        # Create empty DataFrame
        mock_df = pd.DataFrame({"attributes.llm.model_name": []})
        mock_get_raw_traces.return_value = mock_df

        # Call the function
        result = get_model_summary_stats("last_day")

        # Verify the result is empty
        assert result == {}
        mock_get_raw_traces.assert_called_once_with("last_day")

    @patch("src.api.cron.get_raw_traces")
    def test_get_model_summary_stats_different_periods(self, mock_get_raw_traces):
        """Test model summary statistics with different time periods."""
        # Create mock DataFrame
        mock_df = pd.DataFrame(
            {
                "attributes.llm.model_name": ["claude-3", "claude-3", "gpt-4o"],
                "other_column": [1, 2, 3],
            }
        )
        mock_get_raw_traces.return_value = mock_df

        # Test different periods
        for period in ["last_day", "current_month", "current_year"]:
            result = get_model_summary_stats(period)
            expected = {"claude-3": 2, "gpt-4o": 1}
            assert result == expected
            mock_get_raw_traces.assert_called_with(period)


@pytest.mark.asyncio
class TestSendUsageSummaryStats:
    """Test the send_usage_summary_stats function."""

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_model_summary_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_success(
        self, mock_get_org_stats, mock_get_model_stats, mock_send_slack
    ):
        """Test successful usage summary statistics sending."""
        # Setup mocks
        mock_org_data = {"org1": 100, "org2": 200}
        mock_model_data = {"gpt-4": 50, "gpt-3.5-turbo": 25}

        mock_get_org_stats.return_value = mock_org_data
        mock_get_model_stats.return_value = mock_model_data
        mock_send_slack.return_value = None

        # Call the function
        await send_usage_summary_stats()

        # Verify database calls for different periods
        assert mock_get_org_stats.call_count == 3
        mock_get_org_stats.assert_any_call("last_day")
        mock_get_org_stats.assert_any_call("current_month")
        mock_get_org_stats.assert_any_call("current_year")

        # Verify model stats calls for different periods
        assert mock_get_model_stats.call_count == 3
        mock_get_model_stats.assert_any_call("last_day")
        mock_get_model_stats.assert_any_call("current_month")
        mock_get_model_stats.assert_any_call("current_year")

        # Verify Slack notification was sent with correct data structure
        mock_send_slack.assert_called_once()
        args = mock_send_slack.call_args[0]

        # Check that all three periods are passed
        assert len(args) == 3

        # Check data structure for each period
        for period_data in args:
            assert "org" in period_data
            assert "model" in period_data
            assert period_data["org"] == mock_org_data
            assert period_data["model"] == mock_model_data

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_model_summary_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_org_db_error(
        self, mock_get_org_stats, mock_get_model_stats, mock_send_slack
    ):
        """Test usage summary statistics when org database call fails."""
        # Setup mocks - org stats fails
        mock_get_org_stats.side_effect = Exception("Database error")
        mock_get_model_stats.return_value = {"gpt-4": 50}

        # Call the function and expect exception
        with pytest.raises(Exception) as exc_info:
            await send_usage_summary_stats()

        assert "Database error" in str(exc_info.value)
        mock_send_slack.assert_not_called()

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_model_summary_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_model_error(
        self, mock_get_org_stats, mock_get_model_stats, mock_send_slack
    ):
        """Test usage summary statistics when model stats call fails."""
        # Setup mocks - model stats fails
        mock_get_org_stats.return_value = {"org1": 100}
        mock_get_model_stats.side_effect = Exception("Phoenix error")

        # Call the function and expect exception
        with pytest.raises(Exception) as exc_info:
            await send_usage_summary_stats()

        assert "Phoenix error" in str(exc_info.value)
        mock_send_slack.assert_not_called()

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_model_summary_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_slack_error(
        self, mock_get_org_stats, mock_get_model_stats, mock_send_slack
    ):
        """Test usage summary statistics when Slack notification fails."""
        # Setup mocks - Slack fails
        mock_get_org_stats.return_value = {"org1": 100}
        mock_get_model_stats.return_value = {"gpt-4": 50}
        mock_send_slack.side_effect = Exception("Slack API error")

        # Call the function and expect exception
        with pytest.raises(Exception) as exc_info:
            await send_usage_summary_stats()

        assert "Slack API error" in str(exc_info.value)

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_model_summary_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_empty_data(
        self, mock_get_org_stats, mock_get_model_stats, mock_send_slack
    ):
        """Test usage summary statistics with empty data."""
        # Setup mocks with empty data
        mock_get_org_stats.return_value = {}
        mock_get_model_stats.return_value = {}
        mock_send_slack.return_value = None

        # Call the function
        await send_usage_summary_stats()

        # Verify Slack notification was still sent with empty data
        mock_send_slack.assert_called_once()
        args = mock_send_slack.call_args[0]

        # Check that all three periods are passed with empty data
        assert len(args) == 3
        for period_data in args:
            assert period_data["org"] == {}
            assert period_data["model"] == {}
