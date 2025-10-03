from google.cloud import bigquery
from typing import Dict
from collections import defaultdict
from api.settings import settings
from api.config import (
    courses_table_name,
    course_milestones_table_name,
    milestones_table_name,
    course_tasks_table_name,
    questions_table_name,
    tasks_table_name,
)
from api.models import TaskType, TaskStatus, GenerateTaskJobStatus
from api.bq.base import get_bq_client


async def get_course_org_id(course_id: int) -> int:
    bq_client = get_bq_client()

    query = f"""
        SELECT org_id 
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{courses_table_name}` 
        WHERE id = @course_id AND created_at > TIMESTAMP('2024-01-01 00:00:00')
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("course_id", "INT64", course_id)
        ]
    )

    query_job = bq_client.query(query, job_config=job_config)
    rows = list(query_job.result())

    if not rows:
        raise ValueError("Course not found")

    return rows[0]["org_id"]


async def get_course(course_id: int, only_published: bool = True) -> Dict:
    bq_client = get_bq_client()

    # Get course basic info
    course_query = f"""
        SELECT c.id, c.name 
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{courses_table_name}` c 
        WHERE c.id = @course_id AND c.created_at > TIMESTAMP('2024-01-01 00:00:00')
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("course_id", "INT64", course_id)
        ]
    )

    query_job = bq_client.query(course_query, job_config=job_config)
    course_rows = list(query_job.result())

    if not course_rows:
        return None

    course = course_rows[0]

    # Get milestones
    milestones_query = f"""
        SELECT m.id, m.name, m.color, cm.ordering 
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{course_milestones_table_name}` cm
        JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{milestones_table_name}` m 
            ON cm.milestone_id = m.id
        WHERE cm.course_id = @course_id AND cm.created_at > TIMESTAMP('2024-01-01 00:00:00')
        ORDER BY cm.ordering
    """

    query_job = bq_client.query(milestones_query, job_config=job_config)
    milestones = list(query_job.result())

    # Get tasks
    published_filter = (
        f"AND t.status = '{TaskStatus.PUBLISHED}' AND t.scheduled_publish_at IS NULL"
        if only_published
        else ""
    )

    tasks_query = f"""
        SELECT t.id, t.title, t.type, t.status, t.scheduled_publish_at, ct.milestone_id, ct.ordering,
            (CASE WHEN t.type = '{TaskType.QUIZ}' THEN 
                (SELECT COUNT(*) FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{questions_table_name}` q 
                 WHERE q.task_id = t.id AND q.created_at > TIMESTAMP('2024-01-01 00:00:00'))
             ELSE NULL END) as num_questions,
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{course_tasks_table_name}` ct
        JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{tasks_table_name}` t 
            ON ct.task_id = t.id AND ct.created_at > TIMESTAMP('2024-01-01 00:00:00')
        WHERE ct.course_id = @course_id AND t.deleted_at IS NULL AND t.created_at > TIMESTAMP('2024-01-01 00:00:00')
        {published_filter}
        ORDER BY ct.milestone_id, ct.ordering
    """

    query_job = bq_client.query(tasks_query, job_config=job_config)
    tasks = list(query_job.result())

    # Group tasks by milestone_id
    tasks_by_milestone = defaultdict(list)
    for task in tasks:
        milestone_id = task["milestone_id"]

        tasks_by_milestone[milestone_id].append(
            {
                "id": task["id"],
                "title": task["title"],
                "type": task["type"],
                "status": task["status"],
                "scheduled_publish_at": task["scheduled_publish_at"],
                "ordering": task["ordering"],
                "num_questions": task["num_questions"],
            }
        )

    course_dict = {
        "id": course["id"],
        "name": course["name"],
    }
    course_dict["milestones"] = []

    for milestone in milestones:
        milestone_id = milestone["id"]
        milestone_dict = {
            "id": milestone_id,
            "name": milestone["name"],
            "color": milestone["color"],
            "ordering": milestone["ordering"],
            "tasks": tasks_by_milestone.get(milestone_id, []),
        }
        course_dict["milestones"].append(milestone_dict)

    return course_dict
