import pytest
from unittest.mock import patch, AsyncMock
from datetime import timezone, timedelta
from src.api.scheduler import (
    scheduler,
    check_scheduled_tasks,
    # daily_usage_stats,
    # daily_traces,
    check_memory,
    ist_timezone,
)


class TestSchedulerConfiguration:
    """Test scheduler configuration."""

    def test_scheduler_timezone(self):
        """Test that scheduler is configured with IST timezone."""
        # Verify IST timezone (UTC+5:30)
        expected_offset = timedelta(hours=5, minutes=30)
        assert ist_timezone.utcoffset(None) == expected_offset

    def test_scheduler_instance(self):
        """Test that scheduler is properly configured."""
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        assert isinstance(scheduler, AsyncIOScheduler)
        assert scheduler.timezone == ist_timezone


@pytest.mark.asyncio
class TestScheduledTasks:
    """Test scheduled task functions."""

    @patch("src.api.scheduler.publish_scheduled_tasks")
    async def test_check_scheduled_tasks(self, mock_publish_tasks):
        """Test the check_scheduled_tasks function."""
        # Call the function
        await check_scheduled_tasks()

        # Verify the database function was called
        mock_publish_tasks.assert_called_once()

    @patch("src.api.scheduler.check_memory_and_raise_alert")
    async def test_check_memory(self, mock_check_memory_alert):
        """Test the check_memory function."""
        # check_memory_and_raise_alert is not async, just a regular function
        mock_check_memory_alert.return_value = None

        # Call the function
        await check_memory()

        # Verify the memory check function was called
        mock_check_memory_alert.assert_called_once()


class TestSchedulerJobs:
    """Test scheduler job registration."""

    def test_scheduled_jobs_registered(self):
        """Test that all scheduled jobs are properly registered."""
        # Get all jobs from the scheduler
        jobs = scheduler.get_jobs()

        # Verify we have the expected number of jobs
        assert len(jobs) >= 2  # check_scheduled_tasks, check_memory

        # Find specific jobs by their function names
        job_names = [job.func.__name__ for job in jobs]

        assert "check_scheduled_tasks" in job_names
        # assert "daily_usage_stats" in job_names
        # assert "daily_traces" in job_names
        assert "check_memory" in job_names

    def test_check_scheduled_tasks_job_config(self):
        """Test check_scheduled_tasks job configuration."""
        jobs = scheduler.get_jobs()

        # Find the check_scheduled_tasks job
        task_job = None
        for job in jobs:
            if job.func.__name__ == "check_scheduled_tasks":
                task_job = job
                break

        assert task_job is not None
        # Check it's an interval job (runs every minute)
        assert str(task_job.trigger).startswith("interval")

    # def test_daily_usage_stats_job_config(self):
    #     """Test daily_usage_stats job configuration."""
    #     jobs = scheduler.get_jobs()

    #     # Find the daily_usage_stats job
    #     stats_job = None
    #     for job in jobs:
    #         if job.func.__name__ == "daily_usage_stats":
    #             stats_job = job
    #             break

    #     assert stats_job is not None
    #     # Check it's a cron job
    #     assert str(stats_job.trigger).startswith("cron")

    # def test_daily_traces_job_config(self):
    #     """Test daily_traces job configuration."""
    #     jobs = scheduler.get_jobs()

    #     # Find the daily_traces job
    #     traces_job = None
    #     for job in jobs:
    #         if job.func.__name__ == "daily_traces":
    #             traces_job = job
    #             break

    #     assert traces_job is not None
    #     # Check it's a cron job
    #     assert str(traces_job.trigger).startswith("cron")

    def test_check_memory_job_config(self):
        """Test check_memory job configuration."""
        jobs = scheduler.get_jobs()

        # Find the check_memory job
        memory_job = None
        for job in jobs:
            if job.func.__name__ == "check_memory":
                memory_job = job
                break

        assert memory_job is not None
        # Check it's a cron job
        assert str(memory_job.trigger).startswith("cron")
