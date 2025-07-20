from google.cloud import bigquery
import os
from typing import AsyncGenerator, Dict, Any
from api.settings import settings
from api.config import (
    chat_history_table_name,
    questions_table_name,
    tasks_table_name,
    users_table_name,
    course_tasks_table_name,
)


def get_bq_client():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        settings.google_application_credentials
    )
    return bigquery.Client()


async def get_all_chat_history(org_id: int) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream chat history results one by one instead of loading all into memory.
    Yields each chat message as a dictionary.
    """
    bq_client = get_bq_client()

    query = f"""
        SELECT message.id, message.created_at, user.id AS user_id, user.email AS user_email, 
               message.question_id, task.id AS task_id, message.role, message.content, 
               message.response_type, course_task.course_id
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{chat_history_table_name}` message
        INNER JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{questions_table_name}` question 
            ON message.question_id = question.id
        INNER JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{tasks_table_name}` task 
            ON question.task_id = task.id
        INNER JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{users_table_name}` user 
            ON message.user_id = user.id 
        LEFT JOIN `{settings.bq_project_name}.{settings.bq_dataset_name}.{course_tasks_table_name}` course_task 
            ON task.id = course_task.task_id
        WHERE task.deleted_at IS NULL AND task.org_id = @org_id AND message.created_at > TIMESTAMP('2024-01-01 00:00:00') AND question.created_at > TIMESTAMP('2024-01-01 00:00:00') AND task.created_at > TIMESTAMP('2024-01-01 00:00:00') AND course_task.created_at > TIMESTAMP('2024-01-01 00:00:00') AND user.created_at > TIMESTAMP('2024-01-01 00:00:00')
        ORDER BY message.created_at ASC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("org_id", "INT64", org_id)]
    )

    query_job = bq_client.query(query, job_config=job_config)

    # Stream results row by row instead of loading all into memory
    for row in query_job.result():
        yield {
            "id": row["id"],
            "created_at": row["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": row["user_id"],
            "user_email": row["user_email"],
            "question_id": row["question_id"],
            "task_id": row["task_id"],
            "role": row["role"],
            "content": row["content"],
            "response_type": row["response_type"],
            "course_id": row["course_id"],
        }
