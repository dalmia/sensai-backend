from typing import Dict, Tuple
from api.utils.db import execute_db_operation, execute_multiple_db_operations
from api.config import (
    milestones_table_name,
    course_tasks_table_name,
    course_milestones_table_name,
    chat_history_table_name,
    tasks_table_name,
    uncategorized_milestone_name,
    uncategorized_milestone_color,
)


def convert_milestone_db_to_dict(milestone: Tuple) -> Dict:
    return {"id": milestone[0], "name": milestone[1], "color": milestone[2]}


async def get_all_milestones():
    milestones = await execute_db_operation(
        f"SELECT id, name, color FROM {milestones_table_name}", fetch_all=True
    )

    return [convert_milestone_db_to_dict(milestone) for milestone in milestones]


execute_db_operation


async def get_all_milestones_for_org(org_id: int):
    milestones = await execute_db_operation(
        f"SELECT id, name, color FROM {milestones_table_name} WHERE org_id = ?",
        (org_id,),
        fetch_all=True,
    )

    return [convert_milestone_db_to_dict(milestone) for milestone in milestones]


async def update_milestone(milestone_id: int, name: str):
    await execute_db_operation(
        f"UPDATE {milestones_table_name} SET name = ? WHERE id = ?",
        (name, milestone_id),
    )


async def delete_milestone(milestone_id: int):
    await execute_multiple_db_operations(
        [
            (f"DELETE FROM {milestones_table_name} WHERE id = ?", (milestone_id,)),
            (
                f"UPDATE {course_tasks_table_name} SET milestone_id = NULL WHERE milestone_id = ?",
                (milestone_id,),
            ),
            (
                f"DELETE FROM {course_milestones_table_name} WHERE milestone_id = ?",
                (milestone_id,),
            ),
        ]
    )


async def get_user_metrics_for_all_milestones(user_id: int, course_id: int):
    # Get milestones with tasks
    base_results = await execute_db_operation(
        f"""
        SELECT 
            m.id AS milestone_id,
            m.name AS milestone_name,
            m.color AS milestone_color,
            COUNT(DISTINCT t.id) AS total_tasks,
            (
                SELECT COUNT(DISTINCT ch.task_id)
                FROM {chat_history_table_name} ch
                WHERE ch.user_id = ?
                AND ch.is_solved = 1
                AND ch.task_id IN (
                    SELECT t2.id 
                    FROM {tasks_table_name} t2 
                    JOIN {course_tasks_table_name} ct2 ON t2.id = ct2.task_id
                    WHERE ct2.milestone_id = m.id 
                    AND ct2.course_id = ?
                    AND t2.deleted_at IS NULL
                )
            ) AS completed_tasks
        FROM 
            {milestones_table_name} m
        LEFT JOIN 
            {course_tasks_table_name} ct ON m.id = ct.milestone_id
        LEFT JOIN
            {tasks_table_name} t ON ct.task_id = t.id
        LEFT JOIN
            {course_milestones_table_name} cm ON m.id = cm.milestone_id AND ct.course_id = cm.course_id
        WHERE 
            t.verified = 1 AND ct.course_id = ? AND t.deleted_at IS NULL
        GROUP BY 
            m.id, m.name, m.color
        HAVING 
            COUNT(DISTINCT t.id) > 0
        ORDER BY 
            cm.ordering
        """,
        params=(user_id, course_id, course_id),
        fetch_all=True,
    )

    # Get tasks with null milestone_id
    null_milestone_results = await execute_db_operation(
        f"""
        SELECT 
            NULL AS milestone_id,
            '{uncategorized_milestone_name}' AS milestone_name,
            '{uncategorized_milestone_color}' AS milestone_color,
            COUNT(DISTINCT t.id) AS total_tasks,
            (
                SELECT COUNT(DISTINCT ch.task_id)
                FROM {chat_history_table_name} ch
                WHERE ch.user_id = ?
                AND ch.is_solved = 1
                AND ch.task_id IN (
                    SELECT t2.id 
                    FROM {tasks_table_name} t2 
                    JOIN {course_tasks_table_name} ct2 ON t2.id = ct2.task_id
                    WHERE ct2.milestone_id IS NULL 
                    AND ct2.course_id = ?
                    AND t2.deleted_at IS NULL
                )
            ) AS completed_tasks
        FROM 
            {tasks_table_name} t
        LEFT JOIN
            {course_tasks_table_name} ct ON t.id = ct.task_id
        WHERE 
            ct.milestone_id IS NULL 
            AND t.verified = 1 
            AND t.deleted_at IS NULL
            AND ct.course_id = ?
        HAVING
            COUNT(DISTINCT t.id) > 0
        ORDER BY 
            ct.ordering
        """,
        params=(user_id, course_id, course_id),
        fetch_all=True,
    )

    results = base_results + null_milestone_results

    return [
        {
            "milestone_id": row[0],
            "milestone_name": row[1],
            "milestone_color": row[2],
            "total_tasks": row[3],
            "completed_tasks": row[4],
        }
        for row in results
    ]
