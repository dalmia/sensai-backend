from typing import Dict, List
from api.db.task import (
    prepare_blocks_for_publish,
    update_learning_material_task,
    update_draft_quiz,
    create_draft_task_for_course,
)
from api.db.course import (
    update_course_name,
    add_course_modules,
)
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
)


def convert_content_to_blocks(content: str) -> List[Dict]:
    lines = content.split("\n")
    blocks = []
    for line in lines:
        blocks.append(
            {
                "type": "paragraph",
                "props": {
                    "textColor": "default",
                    "backgroundColor": "default",
                    "textAlignment": "left",
                },
                "content": [{"type": "text", "text": line, "styles": {}}],
                "children": [],
            }
        )

    return blocks


def convert_task_description_to_blocks(course_details: Dict):
    for milestone in course_details["milestones"]:
        for task in milestone["tasks"]:
            task["blocks"] = convert_content_to_blocks(task["description"])

    return course_details


async def migrate_learning_material(task_id: int, task_details: Dict):
    await update_learning_material_task(
        task_id,
        task_details["name"],
        task_details["blocks"],
        None,
        TaskStatus.PUBLISHED,  # TEMP: turn to draft later
    )


async def migrate_quiz(task_id: int, task_details: Dict):
    scorecards = []

    question = {}

    question["type"] = (
        QuestionType.OPEN_ENDED
        if task_details["response_type"] == "report"
        else QuestionType.OBJECTIVE
    )

    question["blocks"] = task_details["blocks"]

    question["answer"] = (
        convert_content_to_blocks(task_details["answer"])
        if task_details.get("answer")
        else None
    )
    question["input_type"] = (
        "audio" if task_details["input_type"] == "audio" else "text"
    )
    question["response_type"] = task_details["response_type"]
    question["coding_languages"] = task_details.get("coding_language", None)
    question["generation_model"] = None
    question["context"] = (
        {
            "blocks": prepare_blocks_for_publish(
                convert_content_to_blocks(task_details["context"])
            ),
            "linkedMaterialIds": None,
        }
        if task_details.get("context")
        else None
    )
    question["max_attempts"] = (
        1 if task_details["response_type"] == TaskAIResponseType.EXAM else None
    )
    question["is_feedback_shown"] = (
        False if task_details["response_type"] == TaskAIResponseType.EXAM else True
    )

    if task_details["response_type"] == "report":
        scoring_criteria = task_details["scoring_criteria"]

        scorecard_criteria = []

        for criterion in scoring_criteria:
            scorecard_criteria.append(
                {
                    "name": criterion["category"],
                    "description": criterion["description"],
                    "min_score": criterion["range"][0],
                    "max_score": criterion["range"][1],
                }
            )

        is_new_scorecard = True
        scorecard_id = None
        for index, existing_scorecard in enumerate(scorecards):
            if existing_scorecard == scorecard_criteria:
                is_new_scorecard = False
                scorecard_id = index
                break

        question["scorecard"] = {
            "id": len(scorecards) if is_new_scorecard else scorecard_id,
            "title": "Scorecard",
            "criteria": scorecard_criteria,
        }

        if is_new_scorecard:
            scorecards.append(scorecard_criteria)
    else:
        question["scorecard"] = None

    question["scorecard_id"] = None

    await update_draft_quiz(
        task_id,
        task_details["name"],
        [question],
        None,
        TaskStatus.PUBLISHED,  # TEMP: turn to draft later
    )


async def migrate_course(course_id: int, course_details: Dict):
    await update_course_name(course_id, course_details["name"])

    module_ids = await add_course_modules(course_id, course_details["milestones"])

    for index, milestone in enumerate(course_details["milestones"]):
        for task in milestone["tasks"]:
            if task["type"] == "reading_material":
                task["type"] = str(TaskType.LEARNING_MATERIAL)
            else:
                task["type"] = str(TaskType.QUIZ)

            task_id, _ = await create_draft_task_for_course(
                task["name"],
                task["type"],
                course_id,
                module_ids[index],
            )

            if task["type"] == TaskType.LEARNING_MATERIAL:
                await migrate_learning_material(task_id, task)
            else:
                await migrate_quiz(task_id, task)


async def migrate_task_description_to_blocks(course_details: Dict):
    from api.routes.ai import migrate_content_to_blocks
    from api.utils.concurrency import async_batch_gather

    coroutines = []

    for milestone in course_details["milestones"]:
        for task in milestone["tasks"]:
            coroutines.append(migrate_content_to_blocks(task["description"]))
        #     break
        # break

    results = await async_batch_gather(coroutines)

    current_index = 0
    for milestone in course_details["milestones"]:
        for task in milestone["tasks"]:
            task["blocks"] = results[current_index]
            current_index += 1
        #     break
        # break

    return course_details


async def add_title_column_to_questions():
    """
    Migration: Adds a 'title' column to the questions table and updates all existing rows
    to have title = f"Question {position+1}".
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        # Check if 'title' column already exists
        await cursor.execute(f"PRAGMA table_info({questions_table_name})")
        columns = [col[1] for col in await cursor.fetchall()]
        if "title" not in columns:
            await cursor.execute(
                f"ALTER TABLE {questions_table_name} ADD COLUMN title TEXT NOT NULL DEFAULT ''"
            )
        # Update all rows to set title = 'Question {position+1}'
        await cursor.execute(
            f"UPDATE {questions_table_name} SET title = 'Question ' || (position + 1)"
        )
        await conn.commit()


async def get_task_titles_map():
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute("SELECT id, title FROM tasks")
        result = await cursor.fetchall()

        return {row[0]: row[1] for row in result}


async def get_question_titles_map():
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute("SELECT id, title FROM questions")
        result = await cursor.fetchall()

        return {row[0]: row[1] for row in result}


async def get_user_email_map():
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute("SELECT id, email FROM users")
        result = await cursor.fetchall()

        return {row[0]: row[1] for row in result}


async def remove_openai_columns_from_organizations():
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # Check if 'openai_api_key' column exists before dropping
        await cursor.execute(f"PRAGMA table_info({organizations_table_name})")
        columns = [col[1] for col in await cursor.fetchall()]
        if "openai_api_key" in columns:
            await cursor.execute(
                f"ALTER TABLE {organizations_table_name} DROP COLUMN openai_api_key"
            )
        if "openai_free_trial" in columns:
            await cursor.execute(
                f"ALTER TABLE {organizations_table_name} DROP COLUMN openai_free_trial"
            )


async def add_missing_timestamp_columns():
    """Add missing timestamp columns to existing tables"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # List of tables and their missing columns
        tables_to_update = [
            (organizations_table_name, ["updated_at", "deleted_at"]),
            (org_api_keys_table_name, ["updated_at", "deleted_at"]),
            (users_table_name, ["updated_at", "deleted_at"]),
            (user_organizations_table_name, ["updated_at", "deleted_at"]),
            (cohorts_table_name, ["created_at", "updated_at", "deleted_at"]),
            (user_cohorts_table_name, ["updated_at", "deleted_at"]),
            (batches_table_name, ["updated_at", "deleted_at"]),
            (user_batches_table_name, ["created_at", "updated_at", "deleted_at"]),
            (course_tasks_table_name, ["updated_at", "deleted_at"]),
            (course_milestones_table_name, ["updated_at", "deleted_at"]),
            (milestones_table_name, ["created_at", "updated_at", "deleted_at"]),
            (courses_table_name, ["updated_at", "deleted_at"]),
            (course_cohorts_table_name, ["updated_at", "deleted_at"]),
            (tasks_table_name, ["updated_at"]),
            (questions_table_name, ["updated_at"]),
            (scorecards_table_name, ["updated_at", "deleted_at"]),
            (question_scorecards_table_name, ["updated_at", "deleted_at"]),
            (chat_history_table_name, ["updated_at", "deleted_at"]),
            (task_completions_table_name, ["updated_at", "deleted_at"]),
            (course_generation_jobs_table_name, ["updated_at", "deleted_at"]),
            (task_generation_jobs_table_name, ["updated_at", "deleted_at"]),
            (code_drafts_table_name, ["created_at", "deleted_at"]),
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


async def create_integrations_table_migration():
    """
    Migration: Creates the integrations table if it doesn't exist.
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        from api.db import create_integrations_table

        await create_integrations_table(cursor)

        await conn.commit()


async def run_migrations():
    await create_integrations_table_migration()
