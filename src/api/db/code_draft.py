from typing import List, Dict
import json

from api.config import code_drafts_table_name
from api.utils.db import execute_db_operation


async def upsert_user_code_draft(user_id: int, question_id: int, code: List[Dict]):
    """Insert or update a code draft for a (user_id, question_id) pair."""

    await execute_db_operation(
        f"""
        INSERT INTO {code_drafts_table_name} (user_id, question_id, code)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, question_id) DO UPDATE SET
            code = excluded.code,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user_id, question_id, json.dumps(code)),
    )


async def get_user_code_draft(user_id: int, question_id: int):
    """Retrieve the latest code draft for the given user & question pair."""

    row = await execute_db_operation(
        f"""SELECT id, code, updated_at FROM {code_drafts_table_name}
            WHERE user_id = ? AND question_id = ?""",
        (user_id, question_id),
        fetch_one=True,
    )

    if not row:
        return None

    return {
        "id": row[0],
        "code": json.loads(row[1]),
        "updated_at": row[2],
    }


async def delete_user_code_draft(user_id: int, question_id: int):
    await execute_db_operation(
        f"DELETE FROM {code_drafts_table_name} WHERE user_id = ? AND question_id = ?",
        (user_id, question_id),
    )
