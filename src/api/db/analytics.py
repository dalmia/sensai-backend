from typing import Dict, List, Optional
from collections import defaultdict
from api.utils.db import execute_db_operation
from api.config import (
    chat_history_table_name,
    questions_table_name,
    tasks_table_name,
    organizations_table_name,
    task_completions_table_name,
    course_tasks_table_name,
    course_cohorts_table_name,
    users_table_name,
    user_cohorts_table_name,
    user_batches_table_name,
)
from api.models import LeaderboardViewType, TaskType, TaskStatus
from api.db.user import get_user_streak_from_usage_dates


async def get_usage_summary_by_organization(
    filter_period: Optional[str] = None,
) -> List[Dict]:
    """Get usage summary by organization from chat history."""

    if filter_period and filter_period not in [
        "last_day",
        "current_month",
        "current_year",
    ]:
        raise ValueError("Invalid filter period")

    # Build the date filter condition based on the filter_period
    date_filter = ""
    if filter_period == "last_day":
        date_filter = "AND ch.created_at >= datetime('now', '-1 day')"
    elif filter_period == "current_month":
        date_filter = "AND ch.created_at >= datetime('now', 'start of month')"
    elif filter_period == "current_year":
        date_filter = "AND ch.created_at >= datetime('now', 'start of year')"

    rows = await execute_db_operation(
        f"""
        SELECT 
            o.id as org_id,
            o.name as org_name,
            COUNT(ch.id) as user_message_count
        FROM {chat_history_table_name} ch
        JOIN {questions_table_name} q ON ch.question_id = q.id
        JOIN {tasks_table_name} t ON q.task_id = t.id
        JOIN {organizations_table_name} o ON t.org_id = o.id
        WHERE ch.role = 'user' {date_filter}
        GROUP BY o.id, o.name
        ORDER BY user_message_count DESC
        """,
        fetch_all=True,
    )

    return [
        {
            "org_id": row[0],
            "org_name": row[1],
            "user_message_count": row[2],
        }
        for row in rows
    ]


async def get_cohort_completion(
    cohort_id: int, user_ids: List[int], course_id: int = None
):
    """
    Retrieves completion data for a user in a specific cohort.

    Args:
        cohort_id: The ID of the cohort
        user_ids: The IDs of the users
        course_id: The ID of the course (optional, if not provided, all courses in the cohort will be considered)

    Returns:
        A dictionary mapping task IDs to their completion status:
        {
            task_id: {
                "is_complete": bool,
                "questions": [{"question_id": int, "is_complete": bool}]
            }
        }
    """
    results = defaultdict(dict)

    # user_in_cohort = await is_user_in_cohort(user_id, cohort_id)
    # if not user_in_cohort:
    #     results[user_id] = {}
    #     continue

    # Get completed tasks for the users from task_completions_table
    completed_tasks = await execute_db_operation(
        f"""
        SELECT user_id, task_id 
        FROM {task_completions_table_name}
        WHERE user_id in ({','.join(map(str, user_ids))}) AND task_id IS NOT NULL
        """,
        fetch_all=True,
    )
    completed_task_ids_for_user = defaultdict(set)
    for user_id, task_id in completed_tasks:
        completed_task_ids_for_user[user_id].add(task_id)

    # Get completed questions for the users from task_completions_table
    completed_questions = await execute_db_operation(
        f"""
        SELECT user_id, question_id 
        FROM {task_completions_table_name}
        WHERE user_id in ({','.join(map(str, user_ids))}) AND question_id IS NOT NULL
        """,
        fetch_all=True,
    )
    completed_question_ids_for_user = defaultdict(set)
    for user_id, question_id in completed_questions:
        completed_question_ids_for_user[user_id].add(question_id)

    # Get all tasks for the cohort
    # Get learning material tasks
    query = f"""
        SELECT DISTINCT t.id
        FROM {tasks_table_name} t
        JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
        JOIN {course_cohorts_table_name} cc ON ct.course_id = cc.course_id
        WHERE cc.cohort_id = ? AND t.deleted_at IS NULL AND t.type = '{TaskType.LEARNING_MATERIAL}' AND t.status = '{TaskStatus.PUBLISHED}' AND t.scheduled_publish_at IS NULL
        """
    params = (cohort_id,)

    if course_id is not None:
        query += " AND ct.course_id = ?"
        params += (course_id,)

    learning_material_tasks = await execute_db_operation(
        query,
        params,
        fetch_all=True,
    )

    for user_id in user_ids:
        for task in learning_material_tasks:
            # For learning material, check if it's in the completed tasks list
            results[user_id][task[0]] = {
                "is_complete": task[0] in completed_task_ids_for_user[user_id]
            }

    # Get quiz and exam task questions
    query = f"""
        SELECT DISTINCT t.id as task_id, q.id as question_id
        FROM {tasks_table_name} t
        JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
        JOIN {course_cohorts_table_name} cc ON ct.course_id = cc.course_id
        LEFT JOIN {questions_table_name} q ON t.id = q.task_id AND q.deleted_at IS NULL
        WHERE cc.cohort_id = ? AND t.deleted_at IS NULL AND t.type = '{TaskType.QUIZ}' AND t.status = '{TaskStatus.PUBLISHED}' AND t.scheduled_publish_at IS NULL{
            " AND ct.course_id = ?" if course_id else ""
        } 
        ORDER BY t.id, q.position ASC
        """
    params = (cohort_id,)

    if course_id is not None:
        params += (course_id,)

    quiz_exam_questions = await execute_db_operation(
        query,
        params,
        fetch_all=True,
    )

    # Group questions by task_id
    quiz_exam_tasks = defaultdict(list)
    for row in quiz_exam_questions:
        task_id = row[0]
        question_id = row[1]

        quiz_exam_tasks[task_id].append(question_id)

    for user_id in user_ids:
        for task_id in quiz_exam_tasks:
            is_task_complete = True
            question_completions = []

            for question_id in quiz_exam_tasks[task_id]:
                is_question_complete = (
                    question_id in completed_question_ids_for_user[user_id]
                )

                question_completions.append(
                    {
                        "question_id": question_id,
                        "is_complete": is_question_complete,
                    }
                )

                if not is_question_complete:
                    is_task_complete = False

            results[user_id][task_id] = {
                "is_complete": is_task_complete,
                "questions": question_completions,
            }

    return results


async def get_cohort_course_attempt_data(cohort_learner_ids: List[int], course_id: int):
    """
    Retrieves attempt data for users in a specific cohort, focusing on whether each user
    has attempted any task from each course assigned to the cohort.

    An attempt is defined as either:
    1. Having at least one entry in task_completions_table for a learning material task in the course
    2. Having at least one message in chat_history_table for a question in a quiz/exam task in the course

    Args:
        cohort_learner_ids: The IDs of the learners in the cohort
        course_id: The ID of the course to check

    Returns:
        A dictionary with the following structure:
        {
            user_id: {
                course_id: {
                    "course_name": str,
                    "has_attempted": bool,
                    "last_attempt_date": str or None,
                    "attempt_count": int
                }
            }
        }
    """
    result = defaultdict(dict)

    # Initialize result structure with all courses for all users
    for user_id in cohort_learner_ids:
        result[user_id][course_id] = {
            "has_attempted": False,
        }

    cohort_learner_ids_str = ",".join(map(str, cohort_learner_ids))

    # Get all learning material tasks attempted for this course
    task_completions = await execute_db_operation(
        f"""
        SELECT DISTINCT tc.user_id
        FROM {task_completions_table_name} tc
        JOIN {course_tasks_table_name} ct ON tc.task_id = ct.task_id
        WHERE tc.user_id IN ({cohort_learner_ids_str}) AND ct.course_id = ?
        ORDER BY tc.created_at ASC
        """,
        (course_id,),
        fetch_all=True,
    )

    # Process task completion data
    for completion in task_completions:
        user_id = completion[0]
        result[user_id][course_id]["has_attempted"] = True

    chat_messages = await execute_db_operation(
        f"""
        SELECT DISTINCT ch.user_id
        FROM {chat_history_table_name} ch
        JOIN {questions_table_name} q ON ch.question_id = q.id
        JOIN {tasks_table_name} t ON q.task_id = t.id
        JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
        WHERE ch.user_id IN ({cohort_learner_ids_str}) AND ct.course_id = ?
        GROUP BY ch.user_id
        """,
        (course_id,),
        fetch_all=True,
    )

    # Process chat message data
    for message_data in chat_messages:
        user_id = message_data[0]
        result[user_id][course_id]["has_attempted"] = True

    # Convert defaultdict to regular dict for cleaner response
    return {user_id: dict(courses) for user_id, courses in result.items()}


async def get_cohort_streaks(
    cohort_id: int,
    view: LeaderboardViewType = LeaderboardViewType.ALL_TIME,
    batch_id: int | None = None,
):
    from collections import defaultdict


# Assume get_user_streak_from_usage_dates and other dependencies are defined elsewhere
# from .utils import get_user_streak_from_usage_dates


async def get_streaks_for_cohort(
    cohort_id: int,
    view: str,  # Using a string for LeaderboardViewType for this example
    batch_id: int | None = None,
):
    """
    Calculates the activity streak for all learners in a cohort, with optional
    filtering by time and batch.

    This function uses an optimized CTE-based query to ensure high performance.
    """
    # --- 1. Build Dynamic and Optimized Filter Conditions ---

    # We will build SQL strings for our WHERE clauses
    date_filter_sql = ""
    if view == LeaderboardViewType.WEEKLY:
        # This is "SARGable" - it allows SQLite to use an index on created_at.
        date_filter_sql = "AND created_at >= datetime('now', 'weekday 0', '-7 days')"
    elif view == LeaderboardViewType.MONTHLY:
        # This range-based check is also SARGable and highly efficient.
        date_filter_sql = "AND created_at >= date('now', 'start of month')"

    # Prepare for dynamic joins based on batch_id
    user_filter_joins = ""
    params = [cohort_id, cohort_id, cohort_id]  # Start with the base params for the CTE

    if batch_id is not None:
        # Instead of a subquery, we will add a JOIN. This is much more efficient.
        user_filter_joins = (
            f"JOIN {user_batches_table_name} ub ON uad.user_id = ub.user_id"
        )
        # The final WHERE clause will filter on batch_id
        params.append(batch_id)

    # --- 2. Construct the Final, Optimized Query ---

    # This query uses a CTE to efficiently gather all unique activity dates.
    # It incorporates the dynamic filters we just built.
    query = f"""
    WITH user_activity_dates AS (
        -- Get unique dates from chat history for the cohort
        SELECT ch.user_id, DATE(datetime(ch.created_at, '+5 hours', '+30 minutes')) as activity_date
        FROM {chat_history_table_name} ch
        WHERE ch.user_id IN (SELECT user_id FROM {user_cohorts_table_name} WHERE cohort_id = ?)
          {date_filter_sql.replace('created_at', 'ch.created_at')} -- Apply date filter here

        UNION  -- UNION automatically handles uniqueness, giving us distinct dates

        -- Get unique dates from task completions for the cohort
        SELECT tc.user_id, DATE(datetime(tc.created_at, '+5 hours', '+30 minutes')) as activity_date
        FROM {task_completions_table_name} tc
        WHERE tc.user_id IN (SELECT user_id FROM {user_cohorts_table_name} WHERE cohort_id = ?)
          {date_filter_sql.replace('created_at', 'tc.created_at')} -- Apply date filter here
    )
    -- Final selection to get user info and apply batch filtering
    SELECT
        uad.user_id,
        uad.activity_date,
        u.email,
        u.first_name,
        u.middle_name,
        u.last_name
    FROM user_activity_dates uad
    JOIN {users_table_name} u ON u.id = uad.user_id
    JOIN {user_cohorts_table_name} uc ON uad.user_id = uc.user_id AND uc.role = 'learner'
    {user_filter_joins} -- Add the JOIN for batch filtering if needed
    WHERE uc.cohort_id = ?
      {'AND ub.batch_id = ?' if batch_id is not None else ''}
    ORDER BY uad.user_id, uad.activity_date ASC;
    """

    # --- 3. Execute the Query ---

    usage_per_user = await execute_db_operation(
        query,
        tuple(params),  # Convert list to tuple for the database driver
        fetch_all=True,
    )

    # --- 4. Process the Results in Python (Your existing logic is excellent) ---

    if not usage_per_user:
        return []

    user_dates = defaultdict(list)
    user_info = {}
    for (
        user_id,
        activity_date,
        user_email,
        user_first_name,
        user_middle_name,
        user_last_name,
    ) in usage_per_user:
        user_dates[user_id].append(activity_date)
        if user_id not in user_info:
            user_info[user_id] = {
                "id": user_id,
                "email": user_email,
                "first_name": user_first_name,
                "middle_name": user_middle_name,
                "last_name": user_last_name,
            }

    streaks = []
    for user_id, dates in user_dates.items():
        # The query already sorted the dates for us
        streak_count = len(get_user_streak_from_usage_dates(dates)) if dates else 0
        streaks.append(
            {
                "user": user_info[user_id],
                "streak_count": streak_count,
            }
        )

    return streaks
