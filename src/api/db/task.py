from typing import Tuple, List, Dict
import json
from datetime import datetime, timedelta, timezone
import uuid
from api.db.utils import get_org_id_for_course
from api.config import (
    tasks_table_name,
    course_tasks_table_name,
    scorecards_table_name,
    question_scorecards_table_name,
    courses_table_name,
    milestones_table_name,
    organizations_table_name,
    questions_table_name,
    chat_history_table_name,
    course_cohorts_table_name,
    task_completions_table_name,
    task_generation_jobs_table_name,
)
from api.utils.db import (
    get_new_db_connection,
    execute_db_operation,
    serialise_list_to_str,
)
from api.models import (
    TaskType,
    TaskStatus,
    ScorecardStatus,
    LearningMaterialTask,
    LeaderboardViewType,
    GenerateTaskJobStatus,
    TaskAIResponseType,
    BaseScorecard,
)
from api.db.utils import convert_blocks_to_right_format


async def create_draft_task_for_course(
    title: str,
    type: str,
    course_id: int,
    milestone_id: int,
    ordering: int = None,
) -> Tuple[int, int]:
    org_id = await get_org_id_for_course(course_id)

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        query = f"INSERT INTO {tasks_table_name} (org_id, type, title, status) VALUES (?, ?, ?, ?)"

        await cursor.execute(
            query,
            (org_id, str(type), title, "draft"),
        )

        task_id = cursor.lastrowid

        if ordering is not None:
            # Shift all tasks at or after the given ordering down by 1
            await cursor.execute(
                f"""
                UPDATE {course_tasks_table_name}
                SET ordering = ordering + 1
                WHERE course_id = ? AND milestone_id = ? AND ordering >= ?
                """,
                (course_id, milestone_id, ordering),
            )
            insert_ordering = ordering
        else:
            # Get the maximum ordering value for this milestone
            await cursor.execute(
                f"SELECT COALESCE(MAX(ordering), -1) FROM {course_tasks_table_name} WHERE course_id = ? AND milestone_id = ?",
                (course_id, milestone_id),
            )
            max_ordering = await cursor.fetchone()
            insert_ordering = max_ordering[0] + 1 if max_ordering else 0

        await cursor.execute(
            f"INSERT INTO {course_tasks_table_name} (course_id, task_id, milestone_id, ordering) VALUES (?, ?, ?, ?)",
            (course_id, task_id, milestone_id, insert_ordering),
        )

        await conn.commit()

        # Compute the "visible" ordering (i.e., the index among non-deleted tasks)
        visible_ordering_row = await execute_db_operation(
            f"""
            SELECT COUNT(*) FROM {course_tasks_table_name} ct
            INNER JOIN {tasks_table_name} t ON ct.task_id = t.id
            WHERE ct.course_id = ? AND ct.milestone_id = ? AND ct.ordering < ? AND t.deleted_at IS NULL
            """,
            (course_id, milestone_id, insert_ordering),
            fetch_one=True,
        )

        visible_ordering = (
            visible_ordering_row[0] if visible_ordering_row else insert_ordering
        )

        return task_id, visible_ordering


async def get_all_learning_material_tasks_for_course(course_id: int):
    query = f"""
    SELECT t.id, t.title, t.type, t.status, t.scheduled_publish_at
    FROM {tasks_table_name} t
    INNER JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
    WHERE ct.course_id = ? AND t.deleted_at IS NULL AND t.type = '{TaskType.LEARNING_MATERIAL}' AND t.status = '{TaskStatus.PUBLISHED}'
    ORDER BY ct.ordering ASC
    """

    query_params = (course_id,)

    tasks = await execute_db_operation(query, query_params, fetch_all=True)

    return [
        {
            "id": task[0],
            "title": task[1],
            "type": task[2],
            "status": task[3],
            "scheduled_publish_at": task[4],
        }
        for task in tasks
    ]


def convert_question_db_to_dict(question) -> Dict:
    result = {
        "id": question[0],
        "type": question[1],
        "blocks": json.loads(question[2]) if question[2] else [],
        "answer": json.loads(question[3]) if question[3] else None,
        "input_type": question[4],
        "response_type": question[5],
        "scorecard_id": question[6],
        "context": json.loads(question[7]) if question[7] else None,
        "coding_languages": json.loads(question[8]) if question[8] else None,
        "max_attempts": question[9],
        "is_feedback_shown": question[10],
        "title": question[11],
    }

    return result


async def get_scorecard(scorecard_id: int) -> Dict:
    if scorecard_id is None:
        return

    scorecard = await execute_db_operation(
        f"SELECT id, title, criteria, status FROM {scorecards_table_name} WHERE id = ?",
        (scorecard_id,),
        fetch_one=True,
    )

    if not scorecard:
        return None

    return {
        "id": scorecard[0],
        "title": scorecard[1],
        "criteria": json.loads(scorecard[2]),
        "status": scorecard[3],
    }


async def get_question(question_id: int) -> Dict:
    question = await execute_db_operation(
        f"""
        SELECT q.id, q.type, q.blocks, q.answer, q.input_type, q.response_type, qs.scorecard_id, q.context, q.coding_language, q.max_attempts, q.is_feedback_shown, q.title
        FROM {questions_table_name} q
        LEFT JOIN {question_scorecards_table_name} qs ON q.id = qs.question_id
        WHERE q.id = ?
        """,
        (question_id,),
        fetch_one=True,
    )

    if not question:
        return None

    question = convert_question_db_to_dict(question)

    if question["scorecard_id"] is not None:
        question["scorecard"] = await get_scorecard(question["scorecard_id"])

    return question


async def get_basic_task_details(task_id: int) -> Dict:
    task = await execute_db_operation(
        f"""
        SELECT id, title, type, status, org_id, scheduled_publish_at
        FROM {tasks_table_name}
        WHERE id = ? AND deleted_at IS NULL
        """,
        (task_id,),
        fetch_one=True,
    )

    if not task:
        return None

    return {
        "id": task[0],
        "title": task[1],
        "type": task[2],
        "status": task[3],
        "org_id": task[4],
        "scheduled_publish_at": task[5],
    }


async def get_task(task_id: int):
    task_data = await get_basic_task_details(task_id)

    if not task_data:
        return None

    if task_data["type"] == TaskType.LEARNING_MATERIAL:
        result = await execute_db_operation(
            f"SELECT blocks FROM {tasks_table_name} WHERE id = ?",
            (task_id,),
            fetch_one=True,
        )

        task_data["blocks"] = json.loads(result[0]) if result[0] else []

    elif task_data["type"] == TaskType.QUIZ:
        questions = await execute_db_operation(
            f"""
            SELECT q.id, q.type, q.blocks, q.answer, q.input_type, q.response_type, qs.scorecard_id, q.context, q.coding_language, q.max_attempts, q.is_feedback_shown, q.title
            FROM {questions_table_name} q
            LEFT JOIN {question_scorecards_table_name} qs ON q.id = qs.question_id
            WHERE task_id = ? ORDER BY position ASC
            """,
            (task_id,),
            fetch_all=True,
        )

        task_data["questions"] = [
            convert_question_db_to_dict(question) for question in questions
        ]

    return task_data


async def get_task_metadata(task_id: int) -> Dict:
    result = await execute_db_operation(
        f"""
        SELECT c.id as course_id, c.name as course_name, m.id as milestone_id, m.name as milestone_name, o.id as org_id, o.name as org_name
        FROM {course_tasks_table_name} ct
        JOIN {courses_table_name} c ON ct.course_id = c.id
        JOIN {milestones_table_name} m ON ct.milestone_id = m.id
        JOIN {organizations_table_name} o ON c.org_id = o.id
        WHERE ct.task_id = ?
        LIMIT 1
        """,
        (task_id,),
        fetch_one=True,
    )

    if not result:
        return None

    return {
        "course": {
            "id": result[0],
            "name": result[1],
        },
        "milestone": {
            "id": result[2],
            "name": result[3],
        },
        "org": {
            "id": result[4],
            "name": result[5],
        },
    }


async def does_task_exist(task_id: int) -> bool:
    task = await execute_db_operation(
        f"""
        SELECT id
        FROM {tasks_table_name}
        WHERE id = ? AND deleted_at IS NULL
        """,
        (task_id,),
        fetch_one=True,
    )

    return task is not None


def prepare_blocks_for_publish(blocks: List[Dict]) -> List[Dict]:
    for index, block in enumerate(blocks):
        if "id" not in block or block["id"] is None:
            block["id"] = str(uuid.uuid4())

        block["position"] = index

    return blocks


async def update_learning_material_task(
    task_id: int,
    title: str,
    blocks: List[Dict],
    scheduled_publish_at: datetime,
    status: TaskStatus = TaskStatus.PUBLISHED,
) -> LearningMaterialTask:
    if not await does_task_exist(task_id):
        return False

    # Execute all operations in a single transaction
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"UPDATE {tasks_table_name} SET blocks = ?, status = ?, title = ?, scheduled_publish_at = ? WHERE id = ?",
            (
                json.dumps(prepare_blocks_for_publish(blocks)),
                str(status),
                title,
                scheduled_publish_at,
                task_id,
            ),
        )

        await conn.commit()

        return await get_task(task_id)


async def update_draft_quiz(
    task_id: int,
    title: str,
    questions: List[Dict],
    scheduled_publish_at: datetime,
    status: TaskStatus = TaskStatus.PUBLISHED,
):
    if not await does_task_exist(task_id):
        return False

    task = await get_basic_task_details(task_id)

    if not task:
        return False

    org_id = task["org_id"]

    scorecard_uuid_to_id = {}

    # Execute all operations in a single transaction
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"DELETE FROM {question_scorecards_table_name} WHERE question_id IN (SELECT id FROM {questions_table_name} WHERE task_id = ?)",
            (task_id,),
        )

        await cursor.execute(
            f"DELETE FROM {questions_table_name} WHERE task_id = ?",
            (task_id,),
        )

        scorecards_to_publish = []

        for index, question in enumerate(questions):
            if not isinstance(question, dict):
                question = question.model_dump()

            await cursor.execute(
                f"""
                INSERT INTO {questions_table_name} (task_id, type, blocks, answer, input_type, response_type, coding_language, generation_model, context, position, max_attempts, is_feedback_shown, title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    str(question["type"]),
                    json.dumps(prepare_blocks_for_publish(question["blocks"])),
                    (
                        json.dumps(prepare_blocks_for_publish(question["answer"]))
                        if question["answer"]
                        else None
                    ),
                    str(question["input_type"]),
                    str(question["response_type"]),
                    (
                        json.dumps(question["coding_languages"])
                        if question["coding_languages"]
                        else None
                    ),
                    None,
                    json.dumps(question["context"]) if question["context"] else None,
                    index,
                    question["max_attempts"],
                    question["is_feedback_shown"],
                    question["title"],
                ),
            )

            question_id = cursor.lastrowid

            scorecard_id = None
            if question.get("scorecard_id") is not None:
                scorecard_id = question["scorecard_id"]

                await cursor.execute(
                    f"""
                    INSERT INTO {question_scorecards_table_name} (question_id, scorecard_id) VALUES (?, ?)
                    """,
                    (question_id, scorecard_id),
                )

                await cursor.execute(
                    f"SELECT id FROM {scorecards_table_name} WHERE id = ? AND status = ?",
                    (question["scorecard_id"], str(ScorecardStatus.DRAFT)),
                )

                result = await cursor.fetchone()

                if result:
                    scorecards_to_publish.append(question["scorecard_id"])

        if scorecards_to_publish:
            await cursor.execute(
                f"UPDATE {scorecards_table_name} SET status = ? WHERE id IN ({','.join(map(str, scorecards_to_publish))})",
                (str(ScorecardStatus.PUBLISHED),),
            )

        # Update task status to published
        await cursor.execute(
            f"UPDATE {tasks_table_name} SET status = ?, title = ?, scheduled_publish_at = ? WHERE id = ?",
            (str(status), title, scheduled_publish_at, task_id),
        )

        await conn.commit()

        return await get_task(task_id)


async def update_published_quiz(
    task_id: int, title: str, questions: List[Dict], scheduled_publish_at: datetime
):
    if not await does_task_exist(task_id):
        return False

    # Execute all operations in a single transaction
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        scorecards_to_publish = []

        for question in questions:
            question = question.model_dump()

            await cursor.execute(
                f"""
                UPDATE {questions_table_name} SET blocks = ?, answer = ?, input_type = ?, coding_language = ?, context = ?, response_type = ?, type = ?, title = ? WHERE id = ?
                """,
                (
                    json.dumps(prepare_blocks_for_publish(question["blocks"])),
                    (
                        json.dumps(prepare_blocks_for_publish(question["answer"]))
                        if question["answer"]
                        else None
                    ),
                    str(question["input_type"]),
                    (
                        json.dumps(question["coding_languages"])
                        if question["coding_languages"]
                        else None
                    ),
                    json.dumps(question["context"]) if question["context"] else None,
                    str(question["response_type"]),
                    str(question["type"]),
                    question["title"],
                    question["id"],
                ),
            )

            if question.get("scorecard_id") is not None:
                # First check if there's an existing scorecard mapping
                await cursor.execute(
                    f"SELECT scorecard_id FROM {question_scorecards_table_name} WHERE question_id = ?",
                    (question["id"],),
                )
                existing_mapping = await cursor.fetchone()

                if existing_mapping:
                    # Update existing mapping
                    await cursor.execute(
                        f"UPDATE {question_scorecards_table_name} SET scorecard_id = ? WHERE question_id = ?",
                        (question["scorecard_id"], question["id"]),
                    )
                else:
                    # Insert new mapping
                    await cursor.execute(
                        f"INSERT INTO {question_scorecards_table_name} (question_id, scorecard_id) VALUES (?, ?)",
                        (question["id"], question["scorecard_id"]),
                    )

                await cursor.execute(
                    f"SELECT id FROM {scorecards_table_name} WHERE id = ? AND status = ?",
                    (question["scorecard_id"], str(ScorecardStatus.DRAFT)),
                )

                result = await cursor.fetchone()

                if result:
                    scorecards_to_publish.append(question["scorecard_id"])

        if scorecards_to_publish:
            await cursor.execute(
                f"UPDATE {scorecards_table_name} SET status = ? WHERE id IN ({','.join(map(str, scorecards_to_publish))})",
                (str(ScorecardStatus.PUBLISHED),),
            )

        # Update task status to published
        await cursor.execute(
            f"UPDATE {tasks_table_name} SET title = ?, scheduled_publish_at = ? WHERE id = ?",
            (title, scheduled_publish_at, task_id),
        )

        await conn.commit()

        return await get_task(task_id)


async def duplicate_task(task_id: int, course_id: int, milestone_id: int) -> int:
    task = await get_basic_task_details(task_id)

    if not task:
        raise ValueError("Task does not exist")

    task_ordering_in_module = await execute_db_operation(
        f"SELECT ordering FROM {course_tasks_table_name} WHERE course_id = ? AND milestone_id = ? AND task_id = ?",
        (course_id, milestone_id, task_id),
        fetch_one=True,
    )

    if task_ordering_in_module is None:
        raise ValueError("Task is not in this module")

    new_task_ordering = task_ordering_in_module[0] + 1

    new_task_id, visible_ordering = await create_draft_task_for_course(
        task["title"],
        str(task["type"]),
        course_id,
        milestone_id,
        new_task_ordering,
    )

    task = await get_task(task["id"])

    if task["type"] == TaskType.LEARNING_MATERIAL:
        await update_learning_material_task(
            new_task_id,
            task["title"],
            task["blocks"],
            None,
            TaskStatus.DRAFT,
        )
    elif task["type"] == TaskType.QUIZ:
        for question in task["questions"]:
            if question["scorecard_id"] is not None:
                question["scorecard"] = await get_scorecard(
                    question.pop("scorecard_id")
                )

        await update_draft_quiz(
            new_task_id,
            task["title"],
            task["questions"],
            None,
            TaskStatus.DRAFT,
        )
    else:
        raise ValueError("Task type not supported")

    task = await get_task(new_task_id)

    return {
        "task": task,
        "ordering": visible_ordering,
    }


async def delete_task(task_id: int):
    await execute_db_operation(
        f"""
        UPDATE {tasks_table_name} SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL
        """,
        (datetime.now(), task_id),
    )


async def delete_tasks(task_ids: List[int]):
    task_ids_as_str = serialise_list_to_str(map(str, task_ids))

    await execute_db_operation(
        f"""
        UPDATE {tasks_table_name} SET deleted_at = ? WHERE id IN ({task_ids_as_str}) AND deleted_at IS NULL
        """,
        (datetime.now(),),
    )


async def get_solved_tasks_for_user(
    user_id: int,
    cohort_id: int,
    view_type: LeaderboardViewType = LeaderboardViewType.ALL_TIME,
):
    if view_type == LeaderboardViewType.ALL_TIME:
        results = await execute_db_operation(
            f"""
        SELECT DISTINCT ch.task_id 
        FROM {chat_history_table_name} ch
        JOIN {tasks_table_name} t ON t.id = ch.task_id
        JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
        JOIN {course_cohorts_table_name} cc ON ct.course_id = cc.course_id
        WHERE ch.user_id = ? AND ch.is_solved = 1 AND cc.cohort_id = ? AND t.deleted_at IS NULL
        """,
            (user_id, cohort_id),
            fetch_all=True,
        )
    else:
        ist = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(ist)
        if view_type == LeaderboardViewType.WEEKLY:
            start_date = now - timedelta(days=now.weekday())
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:  # MONTHLY
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        results = await execute_db_operation(
            f"""
        WITH FirstSolved AS (
            SELECT ch.task_id, MIN(datetime(ch.timestamp, '+5 hours', '+30 minutes')) as first_solved_time
            FROM {chat_history_table_name} ch
            JOIN {tasks_table_name} t ON t.id = ch.task_id
            JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
            JOIN {course_cohorts_table_name} cc ON ct.course_id = cc.course_id
            WHERE ch.user_id = ? AND ch.is_solved = 1 AND cc.cohort_id = ? AND t.deleted_at IS NULL
            GROUP BY ch.task_id
        )
        SELECT DISTINCT task_id 
        FROM FirstSolved
        WHERE first_solved_time >= ?
        """,
            (user_id, cohort_id, start_date),
            fetch_all=True,
        )

    return [task[0] for task in results]


async def mark_task_completed(task_id: int, user_id: int):
    # Update task completion table using INSERT OR IGNORE to handle duplicates gracefully
    await execute_db_operation(
        f"""
        INSERT OR IGNORE INTO {task_completions_table_name} (user_id, task_id)
        VALUES (?, ?)
        """,
        (user_id, task_id),
    )


async def delete_completion_history_for_task(
    task_id: int, question_id: int, user_id: int
):
    if task_id is not None:
        await execute_db_operation(
            f"DELETE FROM {chat_history_table_name} WHERE task_id = ? AND user_id = ?",
            (task_id, user_id),
        )

    await execute_db_operation(
        f"DELETE FROM {chat_history_table_name} WHERE question_id = ? AND user_id = ?",
        (question_id, user_id),
    )


async def schedule_module_tasks(
    course_id: int, module_id: int, scheduled_publish_at: datetime
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"SELECT t.id FROM {tasks_table_name} t INNER JOIN {course_tasks_table_name} ct ON t.id = ct.task_id WHERE ct.course_id = ? AND ct.milestone_id = ? AND t.status = '{TaskStatus.PUBLISHED}'",
            (course_id, module_id),
        )

        course_module_tasks = await cursor.fetchall()

        if not course_module_tasks:
            return

        for task in course_module_tasks:
            await cursor.execute(
                f"UPDATE {tasks_table_name} SET scheduled_publish_at = ? WHERE id = ?",
                (scheduled_publish_at, task[0]),
            )

        await conn.commit()


async def drop_task_generation_jobs_table():
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(f"DROP TABLE IF EXISTS {task_generation_jobs_table_name}")


async def store_task_generation_request(
    task_id: int, course_id: int, job_details: Dict
) -> str:
    job_uuid = str(uuid.uuid4())

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"INSERT INTO {task_generation_jobs_table_name} (uuid, task_id, course_id, status, job_details) VALUES (?, ?, ?, ?, ?)",
            (
                job_uuid,
                task_id,
                course_id,
                str(GenerateTaskJobStatus.STARTED),
                json.dumps(job_details),
            ),
        )

        await conn.commit()

    return job_uuid


async def update_task_generation_job_status(
    job_uuid: str, status: GenerateTaskJobStatus
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"UPDATE {task_generation_jobs_table_name} SET status = ? WHERE uuid = ?",
            (str(status), job_uuid),
        )

        await conn.commit()


async def get_course_task_generation_jobs_status(course_id: int) -> List[str]:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"SELECT status FROM {task_generation_jobs_table_name} WHERE course_id = ?",
            (course_id,),
        )

        statuses = [row[0] for row in await cursor.fetchall()]

        return {
            str(GenerateTaskJobStatus.COMPLETED): statuses.count(
                str(GenerateTaskJobStatus.COMPLETED)
            ),
            str(GenerateTaskJobStatus.STARTED): statuses.count(
                str(GenerateTaskJobStatus.STARTED)
            ),
        }


async def get_all_pending_task_generation_jobs() -> List[Dict]:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"SELECT uuid, job_details FROM {task_generation_jobs_table_name} WHERE status = ?",
            (str(GenerateTaskJobStatus.STARTED),),
        )

        return [
            {
                "uuid": row[0],
                "job_details": json.loads(row[1]),
            }
            for row in await cursor.fetchall()
        ]


async def drop_task_completions_table():
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(f"DROP TABLE IF EXISTS {task_completions_table_name}")

        await conn.commit()


async def get_all_scorecards_for_org(org_id: int) -> List[Dict]:
    scorecards = await execute_db_operation(
        f"SELECT id, title, criteria, status FROM {scorecards_table_name} WHERE org_id = ?",
        (org_id,),
        fetch_all=True,
    )

    return [
        {
            "id": scorecard[0],
            "title": scorecard[1],
            "criteria": [
                {
                    **criterion,
                    "pass_score": criterion.get("pass_score", criterion["max_score"]),
                }
                for criterion in json.loads(scorecard[2])
            ],
            "status": scorecard[3],
        }
        for scorecard in scorecards
    ]


async def create_scorecard(
    scorecard: Dict, status: ScorecardStatus = ScorecardStatus.DRAFT
):
    scorecard_id = await execute_db_operation(
        f"INSERT INTO {scorecards_table_name} (org_id, title, criteria, status) VALUES (?, ?, ?, ?)",
        (
            scorecard["org_id"],
            scorecard["title"],
            json.dumps(scorecard["criteria"]),
            str(status),
        ),
        get_last_row_id=True,
    )

    return await get_scorecard(scorecard_id)


async def update_scorecard(scorecard_id: int, scorecard: BaseScorecard):
    scorecard = scorecard.model_dump()

    await execute_db_operation(
        f"UPDATE {scorecards_table_name} SET title = ?, criteria = ? WHERE id = ?",
        (scorecard["title"], json.dumps(scorecard["criteria"]), scorecard_id),
    )

    return await get_scorecard(scorecard_id)


async def undo_task_delete(task_id: int):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"UPDATE {tasks_table_name} SET deleted_at = NULL WHERE id = ?",
            (task_id,),
        )

        await conn.commit()


async def publish_scheduled_tasks():
    """Publish all tasks whose scheduled time has arrived"""
    current_time = datetime.now()
    # Ensure we're using UTC time for consistency
    current_time = datetime.now(timezone.utc)

    # Get all tasks that should be published now
    tasks = await execute_db_operation(
        f"""
        UPDATE {tasks_table_name}
        SET scheduled_publish_at = NULL
        WHERE status = '{TaskStatus.PUBLISHED}'
        AND scheduled_publish_at IS NOT NULL AND deleted_at IS NULL
        AND scheduled_publish_at <= ?
        RETURNING id
        """,
        (current_time,),
        fetch_all=True,
    )

    return [task[0] for task in tasks] if tasks else []


async def add_generated_learning_material(task_id: int, task_details: Dict):
    await update_learning_material_task(
        task_id,
        task_details["name"],
        convert_blocks_to_right_format(task_details["details"]["blocks"]),
        None,
        TaskStatus.PUBLISHED,  # TEMP: turn to draft later
    )


async def add_generated_quiz(task_id: int, task_details: Dict):
    current_scorecard_index = 0

    for question in task_details["details"]["questions"]:
        question["type"] = question.pop("question_type")

        question["blocks"] = convert_blocks_to_right_format(question["blocks"])

        question["answer"] = (
            convert_blocks_to_right_format(question["correct_answer"])
            if question.get("correct_answer")
            else None
        )
        question["input_type"] = (
            question.pop("answer_type") if question.get("answer_type") else "text"
        )
        question["response_type"] = (
            TaskAIResponseType.CHAT
        )  # not getting exams to be generated in course generation
        question["generation_model"] = None
        question["context"] = (
            {
                "blocks": prepare_blocks_for_publish(
                    convert_blocks_to_right_format(question["context"])
                ),
                "linkedMaterialIds": None,
            }
            if question.get("context")
            else None
        )
        question["max_attempts"] = (
            1 if question["response_type"] == TaskAIResponseType.EXAM else None
        )
        question["is_feedback_shown"] = (
            question["response_type"] != TaskAIResponseType.EXAM
        )
        if question.get("scorecard"):
            question["scorecard"]["id"] = current_scorecard_index
            current_scorecard_index += 1
        else:
            question["scorecard"] = None
        question["scorecard_id"] = None
        question["coding_languages"] = question.get("coding_languages", None)

    await update_draft_quiz(
        task_id,
        task_details["name"],
        task_details["details"]["questions"],
        None,
        TaskStatus.PUBLISHED,  # TEMP: turn to draft later
    )
