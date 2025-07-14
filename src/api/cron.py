import subprocess
from typing import Dict
from api.db.analytics import get_usage_summary_by_organization
from api.slack import send_slack_notification_for_usage_stats
from api.utils.phoenix import get_raw_traces, save_daily_traces
from api.settings import settings
from api.slack import send_slack_notification_for_alerts


def get_model_summary_stats(filter_period: str) -> Dict[str, int]:
    df = get_raw_traces(filter_period)

    # Group by model name and count occurrences
    model_counts = df.groupby("attributes.llm.model_name").size().to_dict()
    return model_counts


async def send_usage_summary_stats():
    """
    Get usage summary statistics for different time periods and send them via Slack webhook.

    This function retrieves usage statistics for the last day, last month, and last year,
    then sends a formatted summary to a Slack channel via webhook.
    """
    try:
        # Get usage statistics for different time periods
        last_day_stats = {
            "org": await get_usage_summary_by_organization("last_day"),
            "model": get_model_summary_stats("last_day"),
        }
        current_month_stats = {
            "org": await get_usage_summary_by_organization("current_month"),
            "model": get_model_summary_stats("current_month"),
        }
        current_year_stats = {
            "org": await get_usage_summary_by_organization("current_year"),
            "model": get_model_summary_stats("current_year"),
        }

        # Send the statistics via Slack webhook
        await send_slack_notification_for_usage_stats(
            last_day_stats, current_month_stats, current_year_stats
        )

    except Exception as e:
        print(f"Error in get_usage_summary_stats: {e}")
        raise


async def check_memory_and_raise_alert():
    if settings.env != "production":
        return

    def get_available_memory():
        # Run df -h and parse output
        result = subprocess.run(["df", "-h"], stdout=subprocess.PIPE, text=True)
        lines = result.stdout.splitlines()

        for line in lines:
            if line.startswith("overlay"):
                parts = line.split()
                avail_str = parts[3]  # Avail column (e.g. '132G')
                if avail_str.lower().endswith("g"):
                    avail_gb = float(avail_str[:-1])
                    return avail_gb

    avail_gb = get_available_memory()

    if avail_gb < 50:
        # Send alert if less than 50GB of memory is available
        await send_slack_notification_for_alerts(
            f"Disk space low on overlay: Only {int(avail_gb)} GB left!"
        )
