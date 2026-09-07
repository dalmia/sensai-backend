from typing import Optional

from api.config import (
    course_milestones_table_name,
    course_tasks_table_name,
    batches_table_name,
    cohorts_table_name,
    course_cohorts_table_name,
    courses_table_name,
    milestones_table_name,
    scorecards_table_name,
    tasks_table_name,
    user_cohorts_table_name,
    user_organizations_table_name,
)
from api.utils.db import execute_db_operation

ORG_STAFF_ROLES = ("owner", "admin")


async def get_org_role(user_id: int, org_id: int) -> Optional[str]:
    row = await execute_db_operation(
        f"SELECT role FROM {user_organizations_table_name} WHERE user_id = ? AND org_id = ?",
        (user_id, org_id),
        fetch_one=True,
    )
    return row[0] if row else None


async def is_org_staff(user_id: int, org_id: int) -> bool:
    return await get_org_role(user_id, org_id) in ORG_STAFF_ROLES


async def is_cohort_member(user_id: int, cohort_id: int) -> bool:
    row = await execute_db_operation(
        f"SELECT 1 FROM {user_cohorts_table_name} WHERE user_id = ? AND cohort_id = ?",
        (user_id, cohort_id),
        fetch_one=True,
    )
    return row is not None


async def _org_of(table: str, entity_id: int) -> Optional[int]:
    row = await execute_db_operation(
        f"SELECT org_id FROM {table} WHERE id = ?",
        (entity_id,),
        fetch_one=True,
    )
    return row[0] if row else None


async def org_for_course(course_id: int) -> Optional[int]:
    return await _org_of(courses_table_name, course_id)


async def org_for_cohort(cohort_id: int) -> Optional[int]:
    return await _org_of(cohorts_table_name, cohort_id)


async def org_for_task(task_id: int) -> Optional[int]:
    return await _org_of(tasks_table_name, task_id)


async def org_for_scorecard(scorecard_id: int) -> Optional[int]:
    return await _org_of(scorecards_table_name, scorecard_id)


async def org_for_milestone(milestone_id: int) -> Optional[int]:
    return await _org_of(milestones_table_name, milestone_id)


async def cohort_for_batch(batch_id: int) -> Optional[int]:
    row = await execute_db_operation(
        f"SELECT cohort_id FROM {batches_table_name} WHERE id = ?",
        (batch_id,),
        fetch_one=True,
    )
    return row[0] if row else None


async def can_access_cohort(user_id: int, cohort_id: int) -> bool:
    org_id = await org_for_cohort(cohort_id)
    if org_id is None:
        return False
    if await is_org_staff(user_id, org_id):
        return True
    return await is_cohort_member(user_id, cohort_id)


async def can_access_course(user_id: int, course_id: int) -> bool:
    org_id = await org_for_course(course_id)
    if org_id is None:
        return False
    if await is_org_staff(user_id, org_id):
        return True

    # Learners reach a course through a cohort it has been assigned to.
    row = await execute_db_operation(
        f"""SELECT 1
            FROM {course_cohorts_table_name} cc
            JOIN {user_cohorts_table_name} uc ON uc.cohort_id = cc.cohort_id
            WHERE cc.course_id = ? AND uc.user_id = ?
            LIMIT 1""",
        (course_id, user_id),
        fetch_one=True,
    )
    return row is not None


async def can_access_task(user_id: int, task_id: int) -> bool:
    org_id = await org_for_task(task_id)
    if org_id is None:
        return False
    if await is_org_staff(user_id, org_id):
        return True

    row = await execute_db_operation(
        f"""SELECT 1
            FROM course_tasks ct
            JOIN {course_cohorts_table_name} cc ON cc.course_id = ct.course_id
            JOIN {user_cohorts_table_name} uc ON uc.cohort_id = cc.cohort_id
            WHERE ct.task_id = ? AND uc.user_id = ?
            LIMIT 1""",
        (task_id, user_id),
        fetch_one=True,
    )
    return row is not None


async def is_staff_over_user(caller_id: int, user_id: int) -> bool:
    """True when the caller is org staff of an org the target user belongs to."""
    row = await execute_db_operation(
        f"""SELECT 1
            FROM {user_organizations_table_name} caller
            WHERE caller.user_id = ?
              AND caller.role IN ('owner', 'admin')
              AND (
                EXISTS (
                    SELECT 1 FROM {user_organizations_table_name} target
                    WHERE target.user_id = ? AND target.org_id = caller.org_id
                )
                OR EXISTS (
                    SELECT 1
                    FROM {user_cohorts_table_name} uc
                    JOIN {cohorts_table_name} c ON c.id = uc.cohort_id
                    WHERE uc.user_id = ? AND c.org_id = caller.org_id
                )
              )
            LIMIT 1""",
        (caller_id, user_id, user_id),
        fetch_one=True,
    )
    return row is not None


async def courses_for_course_task_rows(row_ids) -> set:
    """course_tasks.id -> course_id (the bulk ordering endpoints send row ids)."""
    if not row_ids:
        return set()
    placeholders = ",".join("?" for _ in row_ids)
    rows = await execute_db_operation(
        f"SELECT DISTINCT course_id FROM {course_tasks_table_name} WHERE id IN ({placeholders})",
        tuple(row_ids),
        fetch_all=True,
    )
    return {row[0] for row in rows or []}


async def courses_for_course_milestone_rows(row_ids) -> set:
    if not row_ids:
        return set()
    placeholders = ",".join("?" for _ in row_ids)
    rows = await execute_db_operation(
        f"SELECT DISTINCT course_id FROM {course_milestones_table_name} WHERE id IN ({placeholders})",
        tuple(row_ids),
        fetch_all=True,
    )
    return {row[0] for row in rows or []}
