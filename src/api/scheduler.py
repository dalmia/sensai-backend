import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timezone, timedelta
import bugsnag
from functools import wraps
from typing import Callable, Any

from api.db.task import publish_scheduled_tasks
from api.cron import (
    send_usage_summary_stats,
    save_daily_traces,
    check_memory_and_raise_alert,
)
from api.bq.cron import sync_to_bigquery
from api.settings import settings

# Set up logging
logger = logging.getLogger(__name__)

# Create IST timezone
ist_timezone = timezone(timedelta(hours=5, minutes=30))

scheduler = AsyncIOScheduler(timezone=ist_timezone)


def with_error_reporting(context: str):
    """Decorator to add Bugsnag error reporting to scheduled tasks"""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if settings.bugsnag_api_key:
                    bugsnag.notify(e, context=context)
                raise

        return wrapper

    return decorator


# Check for tasks to publish every minute
@scheduler.scheduled_job("interval", minutes=1)
@with_error_reporting("scheduled_task_publish")
async def check_scheduled_tasks():
    await publish_scheduled_tasks()


# BigQuery sync every 15 minutes (only if configured)
def _is_bigquery_configured() -> bool:
    """Check if BigQuery is properly configured."""
    return bool(
        settings.google_application_credentials
        and settings.bq_project_name
        and settings.bq_dataset_name
    )


if _is_bigquery_configured():

    @scheduler.scheduled_job("interval", minutes=15)
    @with_error_reporting("bigquery_sync_interval")
    async def sync_bigquery_interval():
        logger.info("Running scheduled BigQuery sync")
        await sync_to_bigquery()

    # Daily BigQuery sync as backup at 2 AM UTC (7:30 AM IST)
    @scheduler.scheduled_job("cron", hour=7, minute=30, timezone=ist_timezone)
    @with_error_reporting("bigquery_sync_daily")
    async def sync_bigquery_daily():
        logger.info("Running daily BigQuery sync backup")
        await sync_to_bigquery()


# Send usage summary stats every day at 11:15 PM IST
# @scheduler.scheduled_job("cron", hour=23, minute=15, timezone=ist_timezone)
# @with_error_reporting("daily_usage_stats")
# async def daily_usage_stats():
#     if not settings.slack_usage_stats_webhook_url:
#         return

#     await send_usage_summary_stats()


# @scheduler.scheduled_job("cron", hour=4, minute=45, timezone=ist_timezone)
# @with_error_reporting("daily_traces")
# async def daily_traces():
#     # Run save_daily_traces in a thread since it's a sync function
#     await asyncio.to_thread(save_daily_traces)


@scheduler.scheduled_job("cron", hour=23, minute=55, timezone=ist_timezone)
@with_error_reporting("memory_check")
async def check_memory():
    await check_memory_and_raise_alert()


# Additional BigQuery sync utility functions
class BigQuerySyncManager:
    """Manager for BigQuery sync operations within the main scheduler."""

    @staticmethod
    def force_sync():
        """Force a manual BigQuery sync."""
        if not _is_bigquery_configured():
            raise ValueError("BigQuery not configured")

        # Add a one-time job to run immediately
        scheduler.add_job(
            sync_to_bigquery,
            trigger="date",
            run_date=datetime.now(),
            id=f"bigquery_sync_manual_{int(datetime.now().timestamp())}",
            name="BigQuery Sync (Manual)",
            max_instances=1,
        )

        logger.info("Manual BigQuery sync triggered")

    @staticmethod
    def get_bigquery_sync_status() -> dict:
        """Get status of BigQuery sync jobs."""
        if not scheduler.running:
            return {"status": "scheduler_stopped", "bigquery_jobs": []}

        bigquery_jobs = []
        for job in scheduler.get_jobs():
            if "bigquery" in job.id.lower():
                bigquery_jobs.append(
                    {
                        "id": job.id,
                        "name": job.name,
                        "next_run": (
                            job.next_run_time.isoformat() if job.next_run_time else None
                        ),
                        "trigger": str(job.trigger),
                    }
                )

        return {
            "status": "running" if _is_bigquery_configured() else "not_configured",
            "bigquery_jobs": bigquery_jobs,
        }


# Global sync manager instance
bigquery_sync_manager = BigQuerySyncManager()


def force_manual_bigquery_sync():
    """Force a manual BigQuery sync."""
    bigquery_sync_manager.force_sync()


def get_bigquery_sync_status():
    """Get BigQuery sync status."""
    return bigquery_sync_manager.get_bigquery_sync_status()
