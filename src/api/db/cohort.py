from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from api.models import TaskType, TaskStatus
from api.config import (
    cohorts_table_name,
    course_cohorts_table_name,
    courses_table_name,
    tasks_table_name,
    chat_history_table_name,
    user_cohorts_table_name,
    organizations_table_name,
    user_organizations_table_name,
    users_table_name,
)
from api.utils.db import (
    execute_db_operation,
    execute_many_db_operation,
    execute_multiple_db_operations,
    get_new_db_connection,
)
from api.db.user import insert_or_return_user
from api.db.course import get_course
from api.slack import send_slack_notification_for_learner_added_to_cohort


async def add_courses_to_cohort(
    cohort_id: int,
    course_ids: List[int],
    is_drip_enabled: Optional[bool] = False,
    frequency_value: Optional[int] = None,
    frequency_unit: Optional[str] = None,
    publish_at: Optional[datetime] = None,
):
    values = []
    for course_id in course_ids:
        values.append(
            (
                course_id,
                cohort_id,
                is_drip_enabled,
                frequency_value,
                frequency_unit,
                publish_at,
            )
        )

    await execute_many_db_operation(
        f"""INSERT INTO {course_cohorts_table_name} 
            (course_id, cohort_id, is_drip_enabled, frequency_value, frequency_unit, publish_at) 
            VALUES (?, ?, ?, ?, ?, ?)""",
        values,
    )


async def add_course_to_cohorts(
    course_id: int,
    cohort_ids: List[int],
    is_drip_enabled: Optional[bool] = False,
    frequency_value: Optional[int] = None,
    frequency_unit: Optional[str] = None,
    publish_at: Optional[datetime] = None,
):
    values = []
    for cohort_id in cohort_ids:
        values.append(
            (
                course_id,
                cohort_id,
                is_drip_enabled,
                frequency_value,
                frequency_unit,
                publish_at,
            )
        )

    await execute_many_db_operation(
        f"""INSERT INTO {course_cohorts_table_name} 
            (course_id, cohort_id, is_drip_enabled, frequency_value, frequency_unit, publish_at) 
            VALUES (?, ?, ?, ?, ?, ?)""",
        values,
    )


async def remove_course_from_cohorts(course_id: int, cohort_ids: List[int]):
    await execute_many_db_operation(
        f"DELETE FROM {course_cohorts_table_name} WHERE course_id = ? AND cohort_id = ?",
        [(course_id, cohort_id) for cohort_id in cohort_ids],
    )


async def remove_courses_from_cohort(cohort_id: int, course_ids: List[int]):
    await execute_many_db_operation(
        f"DELETE FROM {course_cohorts_table_name} WHERE cohort_id = ? AND course_id = ?",
        [(cohort_id, course_id) for course_id in course_ids],
    )


async def update_cohort_name(cohort_id: int, name: str):
    await execute_db_operation(
        f"UPDATE {cohorts_table_name} SET name = ? WHERE id = ?",
        (name, cohort_id),
    )


def drop_user_cohorts_table():
    execute_db_operation(f"DROP TABLE IF EXISTS {user_cohorts_table_name}")


def delete_all_cohort_info():
    execute_db_operation(f"DELETE FROM {cohorts_table_name}")


async def delete_cohort(cohort_id: int):
    await execute_multiple_db_operations(
        [
            (
                f"DELETE FROM {user_cohorts_table_name} WHERE cohort_id = ?",
                (cohort_id,),
            ),
            (
                f"DELETE FROM {course_cohorts_table_name} WHERE cohort_id = ?",
                (cohort_id,),
            ),
            (
                f"DELETE FROM {cohorts_table_name} WHERE id = ?",
                (cohort_id,),
            ),
        ]
    )


def drop_cohorts_table():
    execute_db_operation(f"DROP TABLE IF EXISTS {cohorts_table_name}")


async def create_cohort(name: str, org_id: int) -> int:
    return await execute_db_operation(
        f"""
        INSERT INTO {cohorts_table_name} (name, org_id)
        VALUES (?, ?)
        """,
        params=(name, org_id),
        get_last_row_id=True,
    )


async def add_members_to_cohort(
    cohort_id: int, org_slug: str, org_id: int, emails: List[str], roles: List[str]
):
    if org_slug is None and org_id is None:
        raise Exception("Either org_slug or org_id must be provided")

    if org_slug is not None:
        org_id = await execute_db_operation(
            f"SELECT id FROM {organizations_table_name} WHERE slug = ?",
            (org_slug,),
            fetch_one=True,
        )

        if org_id is None:
            raise Exception("Organization not found")

        org_id = org_id[0]
    else:
        org = await execute_db_operation(
            f"SELECT slug FROM {organizations_table_name} WHERE id = ?",
            (org_id,),
            fetch_one=True,
        )

        if org is None:
            raise Exception("Organization not found")

        org_slug = org[0]

    # Check if cohort belongs to the organization
    cohort = await execute_db_operation(
        f"""
        SELECT name FROM {cohorts_table_name} WHERE id = ? AND org_id = ?
        """,
        (cohort_id, org_id),
        fetch_one=True,
    )

    if not cohort:
        raise Exception("Cohort does not belong to this organization")

    # Check if any of the emails is an admin for the org
    admin_emails = await execute_db_operation(
        f"""
        SELECT email FROM {users_table_name} u
        JOIN {user_organizations_table_name} uo ON u.id = uo.user_id
        WHERE uo.org_id = ?
        AND (uo.role = 'admin' OR uo.role = 'owner')
        AND u.email IN ({','.join(['?' for _ in emails])})
        """,
        (org_id, *emails),
        fetch_all=True,
    )

    if admin_emails:
        raise Exception(f"Cannot add an admin to the cohort.")

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        users_to_add = []

        for email in emails:
            # Get or create user
            user = await insert_or_return_user(
                cursor,
                email,
            )
            users_to_add.append(user)

        await cursor.execute(
            f"""
            SELECT 1 FROM {user_cohorts_table_name} WHERE user_id IN ({','.join(['?' for _ in [user["id"] for user in users_to_add]])}) AND cohort_id = ?
            """,
            (*[user["id"] for user in users_to_add], cohort_id),
        )

        user_exists = await cursor.fetchone()

        if user_exists:
            raise Exception("User already exists in cohort")

        for user in users_to_add:
            await send_slack_notification_for_learner_added_to_cohort(
                user, org_slug, org_id, cohort[0], cohort_id
            )

        # Add users to cohort
        await cursor.executemany(
            f"""
            INSERT INTO {user_cohorts_table_name} (user_id, cohort_id, role)
            VALUES (?, ?, ?)
            """,
            [(user["id"], cohort_id, role) for user, role in zip(users_to_add, roles)],
        )

        await conn.commit()


async def remove_members_from_cohort(cohort_id: int, member_ids: List[int]):
    members_in_cohort = await execute_db_operation(
        f"""
        SELECT user_id FROM {user_cohorts_table_name}
        WHERE cohort_id = ? AND user_id IN ({','.join(['?' for _ in member_ids])})
        """,
        (cohort_id, *member_ids),
        fetch_all=True,
    )

    if len(members_in_cohort) != len(member_ids):
        raise Exception("One or more members are not in the cohort")

    await execute_multiple_db_operations(
        [
            (
                f"""
            DELETE FROM {user_cohorts_table_name}
            WHERE user_id IN ({','.join(['?' for _ in member_ids])})
            AND cohort_id = ?
            """,
                (*member_ids, cohort_id),
            ),
        ]
    )


async def get_cohorts_for_org(org_id: int) -> List[Dict]:
    """Get all cohorts that belong to an organization"""
    results = await execute_db_operation(
        f"""
        SELECT c.id, c.name, o.id, o.name
        FROM {cohorts_table_name} c
        JOIN {organizations_table_name} o ON o.id = c.org_id
        WHERE o.id = ?
        """,
        (org_id,),
        fetch_all=True,
    )

    # Convert results into nested dict structure
    return [
        {"id": cohort_id, "name": cohort_name, "org_id": org_id, "org_name": org_name}
        for cohort_id, cohort_name, org_id, org_name in results
    ]


async def get_all_cohorts_for_org(org_id: int):
    cohorts = await execute_db_operation(
        f"""
        SELECT c.id, c.name
        FROM {cohorts_table_name} c
        WHERE c.org_id = ?
        ORDER BY c.id DESC
        """,
        (org_id,),
        fetch_all=True,
    )

    return [{"id": row[0], "name": row[1]} for row in cohorts]


async def get_cohort_by_id(cohort_id: int):
    # Fetch cohort details
    cohort = await execute_db_operation(
        f"""SELECT * FROM {cohorts_table_name} WHERE id = ?""",
        (cohort_id,),
        fetch_one=True,
    )

    if not cohort:
        return None

    # Get all users and their roles in the cohort
    members = await execute_db_operation(
        f"""
        SELECT DISTINCT u.id, u.email, uc.role
        FROM {users_table_name} u
        JOIN {user_cohorts_table_name} uc ON u.id = uc.user_id 
        WHERE uc.cohort_id = ?
        ORDER BY uc.role
        """,
        (cohort_id,),
        fetch_all=True,
    )

    cohort_data = {
        "id": cohort[0],
        "org_id": cohort[2],
        "name": cohort[1],
        "members": [
            {"id": member[0], "email": member[1], "role": member[2]}
            for member in members
        ],
    }

    return cohort_data


async def is_user_in_cohort(user_id: int, cohort_id: int):
    output = await execute_db_operation(
        f"""
        SELECT COUNT(*) > 0 FROM (
            SELECT 1
            FROM {user_cohorts_table_name} uc
            WHERE uc.user_id = ? AND uc.cohort_id = ?
            UNION
            SELECT 1 
            FROM {cohorts_table_name} c
            JOIN {organizations_table_name} o ON o.id = c.org_id
            JOIN {user_organizations_table_name} ou ON ou.org_id = o.id
            WHERE c.id = ? AND ou.user_id = ? AND ou.role IN ('admin', 'owner')
        )
        """,
        (user_id, cohort_id, cohort_id, user_id),
        fetch_one=True,
    )

    return output[0]


def format_user_cohort_group(group: Tuple):
    learners = []
    for id, email in zip(group[2].split(","), group[3].split(",")):
        learners.append({"id": int(id), "email": email})

    return {
        "id": group[0],
        "name": group[1],
        "learners": learners,
    }


async def get_cohort_analytics_metrics_for_tasks(cohort_id: int, task_ids: List[int]):
    results = await execute_db_operation(
        f"""
        WITH cohort_learners AS (
            SELECT u.id, u.email
            FROM {users_table_name} u
            JOIN {user_cohorts_table_name} uc ON u.id = uc.user_id
            WHERE uc.cohort_id = ? AND uc.role = 'learner'
        ),
        task_completion AS (
            SELECT
                cl.id as user_id,
                cl.email,
                ch.task_id,
                MAX(COALESCE(ch.is_solved, 0)) as is_solved
            FROM cohort_learners cl
            INNER JOIN {chat_history_table_name} ch
                ON cl.id = ch.user_id
                AND ch.task_id IN ({','.join('?' * len(task_ids))})
            INNER JOIN {tasks_table_name} t
                ON ch.task_id = t.id
            GROUP BY cl.id, cl.email, ch.task_id, t.name
        )
        SELECT
            user_id,
            email,
            GROUP_CONCAT(task_id) as task_ids,
            GROUP_CONCAT(is_solved) as task_completion
        FROM task_completion
        GROUP BY user_id, email
        """,
        (cohort_id, *task_ids),
        fetch_all=True,
    )

    user_metrics = []
    task_metrics = defaultdict(list)
    for row in results:
        user_task_completions = [
            int(x) if x else 0 for x in (row[3].split(",") if row[3] else [])
        ]
        user_task_ids = list(map(int, row[2].split(","))) if row[2] else []

        for task_id, task_completion in zip(user_task_ids, user_task_completions):
            task_metrics[task_id].append(task_completion)

        for task_id in task_ids:
            if task_id in user_task_ids:
                continue

            # this user did not attempt this task - add default
            task_metrics[task_id].append(0)

        num_completed = sum(user_task_completions)

        user_metrics.append(
            {
                "user_id": row[0],
                "email": row[1],
                "num_completed": num_completed,
            }
        )

    task_metrics = {task_id: task_metrics[task_id] for task_id in task_ids}

    for index, row in enumerate(user_metrics):
        for task_id in task_ids:
            row[f"task_{task_id}"] = task_metrics[task_id][index]

    return user_metrics


async def get_cohort_attempt_data_for_tasks(cohort_id: int, task_ids: List[int]):
    results = await execute_db_operation(
        f"""
        WITH cohort_learners AS (
            SELECT u.id, u.email
            FROM {users_table_name} u
            JOIN {user_cohorts_table_name} uc ON u.id = uc.user_id 
            WHERE uc.cohort_id = ? AND uc.role = 'learner'
        ),
        task_attempts AS (
            SELECT 
                cl.id as user_id,
                cl.email,
                ch.task_id,
                CASE WHEN COUNT(ch.id) > 0 THEN 1 ELSE 0 END as has_attempted
            FROM cohort_learners cl
            INNER JOIN {chat_history_table_name} ch 
                ON cl.id = ch.user_id 
                AND ch.task_id IN ({','.join('?' * len(task_ids))})
            INNER JOIN {tasks_table_name} t
                ON ch.task_id = t.id
            GROUP BY cl.id, cl.email, ch.task_id, t.name
        )
        SELECT 
            user_id,
            email,
            GROUP_CONCAT(task_id) as task_ids,
            GROUP_CONCAT(has_attempted) as task_attempts
        FROM task_attempts
        GROUP BY user_id, email
        """,
        (cohort_id, *task_ids),
        fetch_all=True,
    )

    user_metrics = []
    task_attempts = defaultdict(list)

    for row in results:
        user_task_attempts_data = [
            int(x) if x else 0 for x in (row[3].split(",") if row[3] else [])
        ]
        user_task_ids = list(map(int, row[2].split(","))) if row[2] else []

        for task_id, task_attempt in zip(user_task_ids, user_task_attempts_data):
            task_attempts[task_id].append(task_attempt)

        for task_id in task_ids:
            if task_id in user_task_ids:
                continue

            task_attempts[task_id].append(0)

        num_attempted = sum(user_task_attempts_data)

        user_metrics.append(
            {
                "user_id": row[0],
                "email": row[1],
                "num_attempted": num_attempted,
            }
        )

    task_attempts = {task_id: task_attempts[task_id] for task_id in task_ids}

    for index, row in enumerate(user_metrics):
        for task_id in task_ids:
            row[f"task_{task_id}"] = task_attempts[task_id][index]

    return user_metrics


def transfer_chat_history_to_user(prev_user_id: int, new_user_id: int):
    execute_db_operation(
        f"UPDATE {chat_history_table_name} SET user_id = ? WHERE user_id = ?",
        (new_user_id, prev_user_id),
    )
