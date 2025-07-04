from typing import List, Tuple
from datetime import datetime
from api.utils.db import get_new_db_connection, execute_db_operation
from api.config import (
    chat_history_table_name,
    questions_table_name,
    tasks_table_name,
    users_table_name,
    task_completions_table_name,
)
from api.models import StoreMessageRequest, ChatMessage, TaskType
from api.db.task import get_basic_task_details


async def store_messages(
    messages: List[StoreMessageRequest],
    user_id: int,
    question_id: int,
    is_complete: bool,
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        new_row_ids = []

        for message in messages:
            # Insert the new message
            await cursor.execute(
                f"""
            INSERT INTO {chat_history_table_name} (user_id, question_id, role, content, response_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    user_id,
                    question_id,
                    message.role,
                    message.content,
                    message.response_type,
                    message.created_at,
                ),
            )

            new_row_id = cursor.lastrowid
            new_row_ids.append(new_row_id)

        if is_complete:
            await cursor.execute(
                f"""
                INSERT INTO {task_completions_table_name} (user_id, question_id)
                VALUES (?, ?) ON CONFLICT(user_id, question_id) DO NOTHING
                """,
                (user_id, question_id),
            )

        await conn.commit()

    # Fetch the newly inserted row
    new_rows = await execute_db_operation(
        f"""SELECT id, created_at, user_id, question_id, role, content, response_type
    FROM {chat_history_table_name}
    WHERE id IN ({','.join(map(str, new_row_ids))})
    """,
        fetch_all=True,
    )

    # Return the newly inserted row as a dictionary
    return [
        {
            "id": new_row[0],
            "created_at": new_row[1],
            "user_id": new_row[2],
            "question_id": new_row[3],
            "role": new_row[4],
            "content": new_row[5],
            "response_type": new_row[6],
        }
        for new_row in new_rows
    ]


async def get_all_chat_history(org_id: int):
    chat_history = await execute_db_operation(
        f"""
        SELECT message.id, message.created_at, user.id AS user_id, user.email AS user_email, message.question_id, task.id AS task_id, message.role, message.content, message.response_type
        FROM {chat_history_table_name} message
        INNER JOIN {questions_table_name} question ON message.question_id = question.id
        INNER JOIN {tasks_table_name} task ON question.task_id = task.id
        INNER JOIN {users_table_name} user ON message.user_id = user.id 
        WHERE task.deleted_at IS NULL AND task.org_id = ?
        ORDER BY message.created_at ASC
        """,
        (org_id,),
        fetch_all=True,
    )

    return [
        {
            "id": row[0],
            "created_at": row[1],
            "user_id": row[2],
            "user_email": row[3],
            "question_id": row[4],
            "task_id": row[5],
            "role": row[6],
            "content": row[7],
            "response_type": row[8],
        }
        for row in chat_history
    ]


def convert_chat_message_to_dict(message: Tuple) -> ChatMessage:
    return {
        "id": message[0],
        "created_at": message[1],
        "user_id": message[2],
        "question_id": message[3],
        "role": message[4],
        "content": message[5],
        "response_type": message[6],
    }


async def get_question_chat_history_for_user(
    question_id: int, user_id: int
) -> List[ChatMessage]:
    chat_history = await execute_db_operation(
        f"""
    SELECT id, created_at, user_id, question_id, role, content, response_type FROM {chat_history_table_name} WHERE question_id = ? AND user_id = ?
    """,
        (question_id, user_id),
        fetch_all=True,
    )

    return [convert_chat_message_to_dict(row) for row in chat_history]


async def get_task_chat_history_for_user(
    task_id: int, user_id: int
) -> List[ChatMessage]:
    task = await get_basic_task_details(task_id)

    if not task:
        raise ValueError("Task does not exist")

    if task["type"] == TaskType.LEARNING_MATERIAL:
        raise ValueError("Task is not a quiz")

    query = f"""
        SELECT ch.id, ch.created_at, ch.user_id, ch.question_id, ch.role, ch.content, ch.response_type
        FROM {chat_history_table_name} ch
        JOIN {questions_table_name} q ON ch.question_id = q.id
        WHERE q.task_id = ? 
        AND ch.user_id = ?
        ORDER BY ch.created_at ASC
    """

    chat_history = await execute_db_operation(
        query,
        (task_id, user_id),
        fetch_all=True,
    )

    return [convert_chat_message_to_dict(row) for row in chat_history]


async def delete_message(message_id: int):
    await execute_db_operation(
        f"DELETE FROM {chat_history_table_name} WHERE id = ?", (message_id,)
    )


async def update_message_timestamp(message_id: int, new_timestamp: datetime):
    await execute_db_operation(
        f"UPDATE {chat_history_table_name} SET timestamp = ? WHERE id = ?",
        (new_timestamp, message_id),
    )


async def delete_user_chat_history_for_task(question_id: int, user_id: int):
    await execute_db_operation(
        f"DELETE FROM {chat_history_table_name} WHERE question_id = ? AND user_id = ?",
        (question_id, user_id),
    )


async def delete_all_chat_history():
    await execute_db_operation(f"DELETE FROM {chat_history_table_name}")
