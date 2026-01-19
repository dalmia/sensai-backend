from api.models import TaskStatus, TaskType, QuestionType, TaskAIResponseType
from api.utils.db import get_new_db_connection
from api.config import (
    questions_table_name,
    organizations_table_name,
    org_api_keys_table_name,
    users_table_name,
    user_organizations_table_name,
    cohorts_table_name,
    user_cohorts_table_name,
    batches_table_name,
    user_batches_table_name,
    course_tasks_table_name,
    course_milestones_table_name,
    milestones_table_name,
    courses_table_name,
    course_cohorts_table_name,
    tasks_table_name,
    scorecards_table_name,
    question_scorecards_table_name,
    chat_history_table_name,
    task_completions_table_name,
    course_generation_jobs_table_name,
    task_generation_jobs_table_name,
    code_drafts_table_name,
    integrations_table_name,
    assignment_table_name,
)


async def add_missing_timestamp_columns():
    """Add missing timestamp columns to existing tables"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # List of tables and their missing columns
        tables_to_update = [
            (integrations_table_name, ["deleted_at"]),
        ]

        for table_name, columns_to_add in tables_to_update:
            # Check if table exists
            await cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            table_exists = await cursor.fetchone()

            if not table_exists:
                continue

            # Get existing columns
            await cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [col[1] for col in await cursor.fetchall()]

            # Add missing columns first (without setting values)
            columns_added = []
            for column in columns_to_add:
                if column not in existing_columns:
                    await cursor.execute(
                        f"ALTER TABLE {table_name} ADD COLUMN {column} DATETIME"
                    )
                    columns_added.append(column)

            # Now update the timestamp columns with appropriate values
            # Handle created_at first
            if "created_at" in columns_added:
                # Check if updated_at column already exists in the table
                await cursor.execute(f"PRAGMA table_info({table_name})")
                current_columns = [col[1] for col in await cursor.fetchall()]

                if "updated_at" in current_columns:
                    # Set created_at to existing updated_at if updated_at exists
                    await cursor.execute(
                        f"UPDATE {table_name} SET created_at = updated_at WHERE created_at IS NULL AND updated_at IS NOT NULL"
                    )
                    # For records where updated_at is also NULL, use current timestamp
                    await cursor.execute(
                        f"UPDATE {table_name} SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
                    )
                else:
                    # Otherwise set to current timestamp
                    await cursor.execute(
                        f"UPDATE {table_name} SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
                    )

            # Handle updated_at second (after created_at is set)
            if "updated_at" in columns_added:
                # Check if created_at column exists in the table
                await cursor.execute(f"PRAGMA table_info({table_name})")
                current_columns = [col[1] for col in await cursor.fetchall()]

                if "created_at" in current_columns:
                    # Set updated_at to created_at if created_at exists
                    await cursor.execute(
                        f"UPDATE {table_name} SET updated_at = created_at WHERE updated_at IS NULL"
                    )
                else:
                    # Otherwise set to current timestamp
                    await cursor.execute(
                        f"UPDATE {table_name} SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"
                    )

            # Create triggers for automatic timestamp management on future operations
            if "created_at" in columns_added:
                # Trigger to set created_at on INSERT
                trigger_name = f"set_created_at_{table_name}"
                await cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
                await cursor.execute(
                    f"""
                    CREATE TRIGGER {trigger_name}
                    AFTER INSERT ON {table_name}
                    FOR EACH ROW
                    WHEN NEW.created_at IS NULL
                    BEGIN
                        UPDATE {table_name} 
                        SET created_at = CURRENT_TIMESTAMP 
                        WHERE rowid = NEW.rowid;
                    END
                """
                )

            if "updated_at" in columns_added:
                # Trigger to set updated_at on INSERT
                insert_trigger_name = f"set_updated_at_insert_{table_name}"
                await cursor.execute(f"DROP TRIGGER IF EXISTS {insert_trigger_name}")
                await cursor.execute(
                    f"""
                    CREATE TRIGGER {insert_trigger_name}
                    AFTER INSERT ON {table_name}
                    FOR EACH ROW
                    WHEN NEW.updated_at IS NULL
                    BEGIN
                        UPDATE {table_name} 
                        SET updated_at = CURRENT_TIMESTAMP 
                        WHERE rowid = NEW.rowid;
                    END
                """
                )

                # Trigger to set updated_at on UPDATE
                update_trigger_name = f"set_updated_at_update_{table_name}"
                await cursor.execute(f"DROP TRIGGER IF EXISTS {update_trigger_name}")
                await cursor.execute(
                    f"""
                    CREATE TRIGGER {update_trigger_name}
                    AFTER UPDATE ON {table_name}
                    FOR EACH ROW
                    BEGIN
                        UPDATE {table_name} 
                        SET updated_at = CURRENT_TIMESTAMP 
                        WHERE rowid = NEW.rowid;
                    END
                """
                )

        await conn.commit()


async def create_bq_sync_table_migration():
    """
    Migration: Creates the bq_sync table if it doesn't exist.
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        from api.db import create_bq_sync_table

        await create_bq_sync_table(cursor)

        await conn.commit()


async def cleanup_invalid_chat_history():
    """
    Migration: Cleanup chat history records with empty or invalid AI responses for assignments.
    Removes assistant messages where feedback is empty or contains only default values.
    Only targets assignment chat history (where task_id IS NOT NULL).
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # Get all assistant messages for assignments only (task_id IS NOT NULL)
        await cursor.execute(
            f"""
            SELECT id, content
            FROM {chat_history_table_name}
            WHERE role = 'assistant'
            AND task_id IS NOT NULL
            AND deleted_at IS NULL
            """
        )

        messages = await cursor.fetchall()
        invalid_message_ids = []

        import json

        for message_id, content in messages:
            try:
                # Try to parse the content as JSON
                if content:
                    content_obj = json.loads(content)

                    # Check if feedback is empty or contains only whitespace
                    feedback = content_obj.get("feedback", "")

                    # Check if this is an invalid response
                    # Invalid if: feedback is empty AND evaluation_status is "in_progress"
                    # AND current_key_area is empty AND key_area_scores is empty
                    is_invalid = (
                        (not feedback or not feedback.strip()) and
                        content_obj.get("evaluation_status") == "in_progress" and
                        (not content_obj.get("current_key_area") or not content_obj.get("current_key_area").strip()) and
                        (not content_obj.get("key_area_scores") or content_obj.get("key_area_scores") == {})
                    )

                    if is_invalid:
                        invalid_message_ids.append(message_id)
            except (json.JSONDecodeError, AttributeError, TypeError):
                # Skip if content is not valid JSON
                continue

        # Soft delete invalid messages
        if invalid_message_ids:
            placeholders = ','.join('?' * len(invalid_message_ids))
            await cursor.execute(
                f"""
                UPDATE {chat_history_table_name}
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id IN ({placeholders}) AND deleted_at IS NULL
                """,
                invalid_message_ids
            )

            print(f"Cleaned up {len(invalid_message_ids)} invalid chat history records")

        await conn.commit()


async def run_migrations():
    await cleanup_invalid_chat_history()
