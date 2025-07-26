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
from api.config import questions_table_name, integrations_table_name, users_table_name


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


async def create_integrations_table_migration():
    """
    Migration: Creates the integrations table if it doesn't exist.
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        
        # Check if integrations table already exists
        await cursor.execute(f"PRAGMA table_info({integrations_table_name})")
        columns = [col[1] for col in await cursor.fetchall()]
        
        if not columns:  # Table doesn't exist
            await cursor.execute(
                f"""CREATE TABLE {integrations_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    integration_type TEXT NOT NULL,
                    access_token TEXT NOT NULL,
                    refresh_token TEXT,
                    expires_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE
                )"""
            )
            
            # Create indexes
            await cursor.execute(
                f"""CREATE INDEX idx_integration_user_id ON {integrations_table_name} (user_id)"""
            )

            await cursor.execute(
                f"""CREATE INDEX idx_integration_integration_type ON {integrations_table_name} (integration_type)"""
            )
            
            await conn.commit()