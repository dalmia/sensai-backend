from google.cloud import bigquery
import os
import json
from typing import Dict
from api.settings import settings
from api.config import (
    tasks_table_name,
    questions_table_name,
    question_scorecards_table_name,
    scorecards_table_name,
)
from api.models import TaskType


def get_bq_client():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        settings.google_application_credentials
    )
    return bigquery.Client()


async def get_scorecard(scorecard_id: int) -> Dict:
    if scorecard_id is None:
        return None

    bq_client = get_bq_client()

    query = f"""
        SELECT id, title, criteria, status
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{scorecards_table_name}`
        WHERE id = @scorecard_id AND created_at > TIMESTAMP('2024-01-01 00:00:00')
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("scorecard_id", "INT64", scorecard_id)
        ]
    )

    query_job = bq_client.query(query, job_config=job_config)
    rows = list(query_job.result())

    if not rows:
        return None

    scorecard = rows[0]
    return {
        "id": scorecard["id"],
        "title": scorecard["title"],
        "criteria": json.loads(scorecard["criteria"]) if scorecard["criteria"] else [],
        "status": scorecard["status"],
    }


def convert_question_bq_to_dict(question) -> Dict:
    result = {
        "id": question["id"],
        "type": question["type"],
        "blocks": json.loads(question["blocks"]) if question["blocks"] else [],
        "answer": json.loads(question["answer"]) if question["answer"] else None,
        "input_type": question["input_type"],
        "response_type": question["response_type"],
        "scorecard_id": question["scorecard_id"],
        "context": json.loads(question["context"]) if question["context"] else None,
        "coding_languages": (
            json.loads(question["coding_language"])
            if question["coding_language"]
            else None
        ),
        "max_attempts": question["max_attempts"],
        "is_feedback_shown": question["is_feedback_shown"],
        "title": question["title"],
    }
    return result


async def get_basic_task_details(task_id: int) -> Dict:
    bq_client = get_bq_client()

    query = f"""
        SELECT id, title, type, status, org_id, scheduled_publish_at
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{tasks_table_name}`
        WHERE id = @task_id AND deleted_at IS NULL AND created_at > TIMESTAMP('2024-01-01 00:00:00')
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("task_id", "INT64", task_id)]
    )

    query_job = bq_client.query(query, job_config=job_config)
    rows = list(query_job.result())

    if not rows:
        return None

    task = rows[0]
    return {
        "id": task["id"],
        "title": task["title"],
        "type": task["type"],
        "status": task["status"],
        "org_id": task["org_id"],
        "scheduled_publish_at": task["scheduled_publish_at"],
    }


async def get_task(task_id: int):
    task_data = await get_basic_task_details(task_id)

    if not task_data:
        return None

    bq_client = get_bq_client()

    if task_data["type"] == TaskType.LEARNING_MATERIAL:
        query = f"""
            SELECT blocks
            FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{tasks_table_name}`
            WHERE id = @task_id AND created_at > TIMESTAMP('2024-01-01 00:00:00')
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("task_id", "INT64", task_id)
            ]
        )

        query_job = bq_client.query(query, job_config=job_config)
        rows = list(query_job.result())

        if rows:
            task_data["blocks"] = (
                json.loads(rows[0]["blocks"]) if rows[0]["blocks"] else []
            )

    elif task_data["type"] == TaskType.QUIZ:
        questions_query = f"""
            SELECT q.id, q.type, q.blocks, q.answer, q.input_type, q.response_type,
                   qs.scorecard_id, q.context, q.coding_language, q.max_attempts,
                   q.is_feedback_shown, q.title
            FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{questions_table_name}` q
            LEFT JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{question_scorecards_table_name}` qs
                ON q.id = qs.question_id AND qs.created_at > TIMESTAMP('2024-01-01 00:00:00')
            WHERE q.task_id = @task_id AND q.created_at > TIMESTAMP('2024-01-01 00:00:00')
            ORDER BY q.position ASC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("task_id", "INT64", task_id)
            ]
        )

        query_job = bq_client.query(questions_query, job_config=job_config)
        questions = list(query_job.result())

        task_questions = []
        for question in questions:
            question_dict = convert_question_bq_to_dict(question)

            # Get scorecard if exists
            if question_dict["scorecard_id"] is not None:
                question_dict["scorecard"] = await get_scorecard(
                    question_dict["scorecard_id"]
                )

            task_questions.append(question_dict)

        task_data["questions"] = task_questions

    return task_data
