from typing import Dict, List
import aiohttp
from api.settings import settings


async def send_slack_notification(message: Dict, webhook_url: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(webhook_url, json=message) as response:
            if response.status >= 400:
                response_text = await response.text()
                print(
                    f"Failed to send Slack notification: {response.status} - {response_text}"
                )


async def send_slack_notification_for_new_user(user: Dict):
    """
    Send Slack notification when a new user is created.

    Args:
        user: Dictionary containing user information
    """
    # Check if Slack webhook URL is configured
    if not settings.slack_user_signup_webhook_url:
        return

    message = {"text": f"User created: {user['email']} UserId: {user['id']}"}

    # Send notification asynchronously
    await send_slack_notification(message, settings.slack_user_signup_webhook_url)


async def send_slack_notification_for_learner_added_to_cohort(
    user_invited: Dict,
    org_slug: str,
    org_id: int,
    cohort_name: str,
    cohort_id: int,
):
    # Check if Slack webhook URL is configured
    if not settings.slack_user_signup_webhook_url:
        return

    message = {
        "text": f"Learner added to cohort: {user_invited['email']} UserId: {user_invited['id']}\n"
        f"School: {org_slug} (SchoolId: {org_id})\n"
        f"Cohort: {cohort_name} (CohortId: {cohort_id})"
    }

    # Send notification asynchronously
    await send_slack_notification(message, settings.slack_user_signup_webhook_url)


async def send_slack_notification_for_member_added_to_org(
    user_added: Dict,
    org_slug: str,
    org_id: int,
):
    # Check if Slack webhook URL is configured
    if not settings.slack_user_signup_webhook_url:
        return

    message = {
        "text": f"User added as admin: {user_added['email']} UserId: {user_added['id']}\n"
        f"School: {org_slug} (SchoolId: {org_id})"
    }

    # Send notification asynchronously
    await send_slack_notification(message, settings.slack_user_signup_webhook_url)


async def send_slack_notification_for_new_org(
    org_slug: str,
    org_id: int,
    created_by: Dict,
):
    # Check if Slack webhook URL is configured
    if not settings.slack_user_signup_webhook_url:
        return

    message = {
        "text": f"New school created: {org_slug} (SchoolId: {org_id})\n"
        f"Created by: {created_by['email']} (UserId: {created_by['id']})"
    }

    # Send notification asynchronously
    await send_slack_notification(message, settings.slack_user_signup_webhook_url)


async def send_slack_notification_for_new_course(
    course_name: str,
    course_id: int,
    org_slug: str,
    org_id: int,
):
    # Check if Slack webhook URL is configured
    if not settings.slack_course_created_webhook_url:
        return

    message = {
        "text": f"New course created: {course_name} (CourseId: {course_id})\n"
        f"School: {org_slug} (SchoolId: {org_id})"
    }

    # Send notification asynchronously
    await send_slack_notification(message, settings.slack_course_created_webhook_url)


async def send_slack_notification_for_usage_stats(
    last_day_stats: Dict[str, List[Dict]],
    current_month_stats: Dict[str, List[Dict]],
    current_year_stats: Dict[str, List[Dict]],
):
    """
    Send Slack notification with usage statistics for different time periods.

    Args:
        last_day_stats: Usage stats for the last day
        current_month_stats: Usage stats for the current month
        current_year_stats: Usage stats for the current year
    """
    # Check if Slack webhook URL is configured
    if not settings.slack_usage_stats_webhook_url:
        return

    def format_period_stats(
        org_stats: List[Dict], model_stats: Dict[str, int], period: str
    ) -> str:
        # Use different emojis for different time periods
        emoji_map = {
            "Last 24 Hours": "⚡",
            "This Month": "📈",
            "This Year": "📊",
        }
        emoji = emoji_map.get(period, "📊")

        if not org_stats:
            total_messages = 0
        else:
            total_messages = sum(org["user_message_count"] for org in org_stats)

        formatted = f"{emoji} *{period}* (Total: {total_messages:,} messages)\n\n"

        # Organization stats column
        if not org_stats:
            org_section = "📊 *Organizations*: No usage data\n"
        else:
            top_orgs = org_stats[:5]  # Show top 5 organizations

            org_section = "```\n"
            org_section += f"{'Organization':<30} {'Messages':>8}\n"
            org_section += f"{'-' * 30} {'-' * 8}\n"

            for org in top_orgs:
                org_name = (
                    org["org_name"][:25] + ".."
                    if len(org["org_name"]) > 27
                    else org["org_name"]
                )
                org_section += f"{org_name:<30} {org['user_message_count']:>8,}\n"

            if len(org_stats) > 5:
                remaining_count = len(org_stats) - 5
                remaining_messages = sum(
                    org["user_message_count"] for org in org_stats[5:]
                )
                org_section += (
                    f"{f'+{remaining_count} more':<30} {remaining_messages:>8,}\n"
                )

            org_section += "```\n"

        # Model stats column
        if not model_stats:
            model_section = "🤖 *Models*: No model data\n"
        else:
            sorted_models = sorted(
                model_stats.items(), key=lambda x: x[1], reverse=True
            )
            top_models = sorted_models[:5]  # Show top 5 models

            model_section = "```\n"
            model_section += f"{'Model':<50} {'Count':>8}\n"
            model_section += f"{'-' * 50} {'-' * 8}\n"

            for model_name, count in top_models:
                model_display = (
                    model_name[:25] + ".." if len(model_name) > 27 else model_name
                )
                model_section += f"{model_display:<50} {count:>8,}\n"

            if len(sorted_models) > 5:
                remaining_count = len(sorted_models) - 5
                remaining_calls = sum(count for _, count in sorted_models[5:])
                model_section += (
                    f"{f'+{remaining_count} more':<50} {remaining_calls:>8,}\n"
                )

            model_section += "```\n"

        # Combine both sections side by side conceptually, but Slack doesn't support true columns
        # So we'll display them sequentially but clearly separated
        formatted += org_section + "\n" + model_section

        return formatted

    # Send separate messages for each time period
    periods = [
        ("Last 24 Hours", last_day_stats),
        ("This Month", current_month_stats),
        ("This Year", current_year_stats),
    ]

    message_text = ""
    for period_name, stats in periods:
        message_text += format_period_stats(stats["org"], stats["model"], period_name)

    message = {"text": message_text}

    # Send notification asynchronously
    await send_slack_notification(message, settings.slack_usage_stats_webhook_url)
