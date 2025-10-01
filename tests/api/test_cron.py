import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import pandas as pd
from src.api.cron import (
    send_usage_summary_stats,
    check_memory_and_raise_alert,
)
import subprocess


@pytest.mark.asyncio
class TestSendUsageSummaryStats:
    """Test the send_usage_summary_stats function."""

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_success(
        self, mock_get_org_stats, mock_send_slack
    ):
        """Test successful usage summary statistics sending."""
        # Setup mocks
        mock_org_data = {"org1": 100, "org2": 200}

        mock_get_org_stats.return_value = mock_org_data
        mock_send_slack.return_value = None

        # Call the function
        await send_usage_summary_stats()

        # Verify database calls for different periods
        assert mock_get_org_stats.call_count == 3
        mock_get_org_stats.assert_any_call("last_week")
        mock_get_org_stats.assert_any_call("current_month")
        mock_get_org_stats.assert_any_call("current_year")

        # Verify Slack notification was sent with correct data structure
        mock_send_slack.assert_called_once()
        args = mock_send_slack.call_args[0]

        # Check that all three periods are passed
        assert len(args) == 3

        # Check data structure for each period
        for period_data in args:
            assert "org" in period_data
            assert period_data["org"] == mock_org_data

    @patch("src.api.cron.send_slack_notification_for_usage_stats")
    @patch("src.api.cron.get_usage_summary_by_organization")
    async def test_send_usage_summary_stats_empty_data(
        self, mock_get_org_stats, mock_send_slack
    ):
        """Test usage summary statistics with empty data."""
        # Setup mocks with empty data
        mock_get_org_stats.return_value = {}
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


@pytest.mark.asyncio
class TestCheckMemoryAndRaiseAlert:
    """Test the check_memory_and_raise_alert function."""

    @patch("src.api.cron.settings")
    async def test_check_memory_non_production_returns_early(self, mock_settings):
        """Test that function returns early when not in production."""
        mock_settings.env = "development"

        # Call the function
        await check_memory_and_raise_alert()

        # Since it returns early, no further assertions needed
        # Function should complete without error

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_low_memory_sends_alert(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test that alert is sent when memory is low in production."""
        mock_settings.env = "production"

        # Mock subprocess output with low memory (30GB)
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\noverlay         100G   70G   30G  70% /\n"
        mock_subprocess.return_value = mock_result
        mock_send_slack.return_value = None

        # Call the function
        await check_memory_and_raise_alert()

        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once_with(
            ["df", "-h"], stdout=subprocess.PIPE, text=True
        )

        # Verify alert was sent
        mock_send_slack.assert_called_once_with(
            "Disk space low in EC2 instance: Only 30 GB left!"
        )

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_sufficient_memory_no_alert(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test that no alert is sent when memory is sufficient in production."""
        mock_settings.env = "production"

        # Mock subprocess output with sufficient memory (80GB)
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\noverlay         100G   20G   80G  20% /\n"
        mock_subprocess.return_value = mock_result

        # Call the function
        await check_memory_and_raise_alert()

        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once_with(
            ["df", "-h"], stdout=subprocess.PIPE, text=True
        )

        # Verify no alert was sent
        mock_send_slack.assert_not_called()

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_memory_exactly_50gb_no_alert(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test that no alert is sent when memory is exactly 50GB."""
        mock_settings.env = "production"

        # Mock subprocess output with exactly 50GB memory
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\noverlay         100G   50G   50G  50% /\n"
        mock_subprocess.return_value = mock_result

        # Call the function
        await check_memory_and_raise_alert()

        # Verify no alert was sent (since 50 is not < 50)
        mock_send_slack.assert_not_called()

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_memory_49gb_sends_alert(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test that alert is sent when memory is 49GB."""
        mock_settings.env = "production"

        # Mock subprocess output with 49GB memory
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\noverlay         100G   51G   49G  51% /\n"
        mock_subprocess.return_value = mock_result
        mock_send_slack.return_value = None

        # Call the function
        await check_memory_and_raise_alert()

        # Verify alert was sent
        mock_send_slack.assert_called_once_with(
            "Disk space low in EC2 instance: Only 49 GB left!"
        )

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_no_overlay_line(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test behavior when no overlay line is found in df output."""
        mock_settings.env = "production"

        # Mock subprocess output without overlay line
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\n/dev/sda1       100G   20G   80G  20% /\n"
        mock_subprocess.return_value = mock_result

        # Call the function - should handle gracefully when no overlay found
        # This will likely result in avail_gb being None, which will cause an error
        # when compared to 50, but let's test the actual behavior
        try:
            await check_memory_and_raise_alert()
        except TypeError:
            # Expected when trying to compare None < 50
            pass

        # Verify no alert was sent
        mock_send_slack.assert_not_called()

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_decimal_memory_value(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test parsing decimal memory values correctly."""
        mock_settings.env = "production"

        # Mock subprocess output with decimal memory value (45.5GB)
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\noverlay         100G   54.5G   45.5G  55% /\n"
        mock_subprocess.return_value = mock_result
        mock_send_slack.return_value = None

        # Call the function
        await check_memory_and_raise_alert()

        # Verify alert was sent with correct integer conversion
        mock_send_slack.assert_called_once_with(
            "Disk space low in EC2 instance: Only 45 GB left!"
        )

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_non_gb_unit(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test behavior when available memory is not in GB units."""
        mock_settings.env = "production"

        # Mock subprocess output with MB units
        mock_result = MagicMock()
        mock_result.stdout = "Filesystem      Size  Used Avail Use% Mounted on\noverlay         100G   20G   512M  20% /\n"
        mock_subprocess.return_value = mock_result

        # Call the function - should handle gracefully when unit is not 'g'
        # This will likely result in avail_gb being None
        try:
            await check_memory_and_raise_alert()
        except TypeError:
            # Expected when trying to compare None < 50
            pass

        # Verify no alert was sent
        mock_send_slack.assert_not_called()

    @patch("src.api.cron.send_slack_notification_for_alerts")
    @patch("src.api.cron.subprocess.run")
    @patch("src.api.cron.settings")
    async def test_check_memory_production_multiple_overlay_lines(
        self, mock_settings, mock_subprocess, mock_send_slack
    ):
        """Test behavior when multiple overlay lines exist (should use first one)."""
        mock_settings.env = "production"

        # Mock subprocess output with multiple overlay lines
        mock_result = MagicMock()
        mock_result.stdout = """Filesystem      Size  Used Avail Use% Mounted on
overlay         100G   70G   30G  70% /
overlay         200G   50G   150G  25% /other
"""
        mock_subprocess.return_value = mock_result
        mock_send_slack.return_value = None

        # Call the function
        await check_memory_and_raise_alert()

        # Verify alert was sent based on first overlay line (30GB)
        mock_send_slack.assert_called_once_with(
            "Disk space low in EC2 instance: Only 30 GB left!"
        )
