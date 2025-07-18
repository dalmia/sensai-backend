from apscheduler.schedulers.asyncio import AsyncIOScheduler
from api.db.task import publish_scheduled_tasks
from api.cron import (
    send_usage_summary_stats,
    save_daily_traces,
    check_memory_and_raise_alert,
)
from api.settings import settings
from datetime import timezone, timedelta
import asyncio
import bugsnag
from functools import wraps
from typing import Callable, Any

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
