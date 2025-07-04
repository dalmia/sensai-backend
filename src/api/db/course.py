from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from uuid import uuid4
import json
import itertools
from collections import defaultdict
from api.config import (
    courses_table_name,
    course_generation_jobs_table_name,
    course_milestones_table_name,
    milestones_table_name,
    course_tasks_table_name,
    questions_table_name,
    tasks_table_name,
    scorecards_table_name,
    question_scorecards_table_name,
    cohorts_table_name,
    course_cohorts_table_name,
    uncategorized_milestone_name,
    task_generation_jobs_table_name,
    organizations_table_name,
    group_role_learner,
)
from api.db.task import (
    get_task,
    create_draft_task_for_course,
    update_learning_material_task,
    update_draft_quiz,
    get_scorecard,
    create_scorecard,
)
from api.db.utils import EnumEncoder, get_org_id_for_course
from api.utils.db import (
    execute_db_operation,
    get_new_db_connection,
    execute_multiple_db_operations,
    execute_many_db_operation,
    deserialise_list_from_str,
)
from api.db.user import get_user_cohorts, get_user_organizations
from api.db.org import get_org_by_id
from api.slack import send_slack_notification_for_new_course
from api.models import (
    GenerateCourseJobStatus,
    TaskType,
    TaskStatus,
    ScorecardStatus,
    GenerateTaskJobStatus,
)


async def calculate_milestone_unlock_dates(
    course_details: Dict, drip_config: Dict, joined_at: datetime | None = None
):
    if not drip_config or not drip_config.get("is_drip_enabled"):
        # All milestones unlocked
        for milestone in course_details["milestones"]:
            milestone["unlock_at"] = None
        return course_details

    start_date = None
    if drip_config.get("publish_at"):
        publish_at = drip_config["publish_at"]
        start_date = datetime.fromisoformat(publish_at)
    else:
        start_date = joined_at

    # Ensure start_date is always timezone-aware (UTC)
    if start_date and start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    today = datetime.now(timezone.utc)
    freq_value = drip_config["frequency_value"]
    freq_unit = drip_config["frequency_unit"]
    non_empty_module_count = 0

    for milestone in course_details["milestones"]:
        milestone_tasks = milestone.get("tasks", [])

        # Skip empty milestones
        if not milestone_tasks or not start_date:
            milestone["unlock_at"] = None
            continue

        # Calculate unlock date based on non_empty_module_count
        unlock_date = start_date
        if non_empty_module_count > 0:
            if freq_unit == "minute":
                unlock_date = unlock_date + timedelta(
                    minutes=freq_value * non_empty_module_count
                )
            elif freq_unit == "hour":
                unlock_date = unlock_date + timedelta(
                    hours=freq_value * non_empty_module_count
                )
            elif freq_unit == "day":
                unlock_date = unlock_date + timedelta(
                    days=freq_value * non_empty_module_count
                )
            elif freq_unit == "week":
                unlock_date = unlock_date + timedelta(
                    weeks=freq_value * non_empty_module_count
                )
            elif freq_unit == "month":
                unlock_date = unlock_date + relativedelta(
                    months=freq_value * non_empty_module_count
                )
            elif freq_unit == "year":
                unlock_date = unlock_date + relativedelta(
                    years=freq_value * non_empty_module_count
                )
            else:
                raise ValueError(f"Invalid frequency unit: {freq_unit}")

        # First milestone with tasks is always unlocked
        if non_empty_module_count == 0:
            milestone["unlock_at"] = None
        else:
            # Check if this milestone should be locked
            is_locked = today < unlock_date
            unlock_at = unlock_date.isoformat() if is_locked else None
            milestone["unlock_at"] = unlock_at

        non_empty_module_count += 1

    return course_details


async def get_courses_for_cohort(
    cohort_id: int, include_tree: bool = False, joined_at: datetime | None = None
):
    courses = await execute_db_operation(
        f"""
        SELECT c.id, c.name, cc.is_drip_enabled, cc.frequency_value, cc.frequency_unit, cc.publish_at
        FROM {courses_table_name} c
        JOIN {course_cohorts_table_name} cc ON c.id = cc.course_id
        WHERE cc.cohort_id = ?
        """,
        (cohort_id,),
        fetch_all=True,
    )
    courses = [
        {
            "id": course[0],
            "name": course[1],
            "drip_config": {
                "is_drip_enabled": course[2],
                "frequency_value": course[3],
                "frequency_unit": course[4],
                "publish_at": course[5],
            },
        }
        for course in courses
    ]

    if not include_tree:
        return courses

    for index, course in enumerate(courses):
        course_details = await get_course(course["id"])
        course_details = await calculate_milestone_unlock_dates(
            course_details, course["drip_config"], joined_at
        )
        courses[index] = course_details

    return courses


async def store_course_generation_request(course_id: int, job_details: Dict) -> str:
    job_uuid = str(uuid4())

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"INSERT INTO {course_generation_jobs_table_name} (uuid, course_id, status, job_details) VALUES (?, ?, ?, ?)",
            (
                job_uuid,
                course_id,
                str(GenerateCourseJobStatus.STARTED),
                json.dumps(job_details),
            ),
        )

        await conn.commit()

    return job_uuid


async def get_course_generation_job_details(job_uuid: str) -> Dict:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"SELECT job_details FROM {course_generation_jobs_table_name} WHERE uuid = ?",
            (job_uuid,),
        )

        job = await cursor.fetchone()

        if job is None:
            raise ValueError("Job not found")

        return json.loads(job[0])


async def duplicate_course_to_org(course_id: int, org_id: int):
    course = await get_course(course_id)

    new_course_id = await create_course(course["name"], org_id)

    for milestone in course["milestones"]:
        new_milestone_id, _ = await add_milestone_to_course(
            new_course_id, milestone["name"], milestone["color"]
        )

        for task in milestone["tasks"]:
            task_details = await get_task(task["id"])

            new_task_id, _ = await create_draft_task_for_course(
                task_details["title"],
                task_details["type"],
                new_course_id,
                new_milestone_id,
            )

            if task_details["type"] == TaskType.LEARNING_MATERIAL:
                await update_learning_material_task(
                    new_task_id,
                    task_details["title"],
                    task_details["blocks"],
                    None,
                    TaskStatus.PUBLISHED,
                )
            else:
                # Handle quiz tasks with scorecard duplication
                scorecard_mapping = {}  # Map original scorecard_id to new scorecard_id

                for question in task_details["questions"]:
                    if question.get("scorecard_id") is not None:
                        original_scorecard_id = question["scorecard_id"]

                        # Check if we've already duplicated this scorecard
                        if original_scorecard_id not in scorecard_mapping:
                            # Get the original scorecard
                            original_scorecard = await get_scorecard(
                                original_scorecard_id
                            )

                            # Create new scorecard for the new org
                            new_scorecard = await create_scorecard(
                                {
                                    "title": original_scorecard["title"],
                                    "criteria": original_scorecard["criteria"],
                                    "org_id": org_id,
                                },
                                ScorecardStatus.PUBLISHED,
                            )

                            scorecard_mapping[original_scorecard_id] = new_scorecard[
                                "id"
                            ]

                        # Update question to use the new scorecard
                        question["scorecard_id"] = scorecard_mapping[
                            original_scorecard_id
                        ]

                await update_draft_quiz(
                    new_task_id,
                    task_details["title"],
                    task_details["questions"],
                    None,
                    TaskStatus.PUBLISHED,
                )


async def update_course_generation_job_status_and_details(
    job_uuid: str, status: GenerateCourseJobStatus, details: Dict
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"UPDATE {course_generation_jobs_table_name} SET status = ?, job_details = ? WHERE uuid = ?",
            (str(status), json.dumps(details, cls=EnumEncoder), job_uuid),
        )

        await conn.commit()


async def update_course_generation_job_status(
    job_uuid: str, status: GenerateCourseJobStatus
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"UPDATE {course_generation_jobs_table_name} SET status = ? WHERE uuid = ?",
            (str(status), job_uuid),
        )

        await conn.commit()


async def get_all_pending_course_structure_generation_jobs() -> List[Dict]:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"SELECT uuid, course_id, job_details FROM {course_generation_jobs_table_name} WHERE status = ?",
            (str(GenerateCourseJobStatus.STARTED),),
        )

        return [
            {
                "uuid": row[0],
                "course_id": row[1],
                "job_details": json.loads(row[2]),
            }
            for row in await cursor.fetchall()
        ]


async def add_course_modules(course_id: int, modules: List[Dict]):
    import random

    module_ids = []
    for module in modules:
        color = random.choice(
            [
                "#2d3748",  # Slate blue
                "#433c4c",  # Deep purple
                "#4a5568",  # Cool gray
                "#312e51",  # Indigo
                "#364135",  # Forest green
                "#4c393a",  # Burgundy
                "#334155",  # Navy blue
                "#553c2d",  # Rust brown
                "#37303f",  # Plum
                "#3c4b64",  # Steel blue
                "#463c46",  # Mauve
                "#3c322d",  # Coffee
            ]
        )
        module_id, _ = await add_milestone_to_course(course_id, module["name"], color)
        module_ids.append(module_id)

    return module_ids


async def transfer_course_to_org(course_id: int, org_id: int):
    await execute_db_operation(
        f"UPDATE {courses_table_name} SET org_id = ? WHERE id = ?",
        (org_id, course_id),
    )

    milestones = await execute_db_operation(
        f"SELECT cm.milestone_id FROM {course_milestones_table_name} cm INNER JOIN {courses_table_name} c ON cm.course_id = c.id WHERE c.id = ?",
        (course_id,),
        fetch_all=True,
    )

    for milestone in milestones:
        await execute_db_operation(
            f"UPDATE {milestones_table_name} SET org_id = ? WHERE id = ?",
            (org_id, milestone[0]),
        )

    tasks = await execute_db_operation(
        f"SELECT ct.task_id FROM {course_tasks_table_name} ct INNER JOIN {courses_table_name} c ON ct.course_id = c.id WHERE c.id = ?",
        (course_id,),
        fetch_all=True,
    )

    task_ids = [task[0] for task in tasks]

    questions = await execute_db_operation(
        f"SELECT q.id FROM {questions_table_name} q INNER JOIN {tasks_table_name} t ON q.task_id = t.id WHERE t.id IN ({', '.join(map(str, task_ids))})",
        fetch_all=True,
    )

    question_ids = [question[0] for question in questions]

    scorecards = await execute_db_operation(
        f"SELECT qs.scorecard_id FROM {question_scorecards_table_name} qs INNER JOIN {questions_table_name} q ON qs.question_id = q.id WHERE q.id IN ({', '.join(map(str, question_ids))})",
        fetch_all=True,
    )

    scorecard_ids = [scorecard[0] for scorecard in scorecards]

    await execute_db_operation(
        f"UPDATE {scorecards_table_name} SET org_id = ? WHERE id IN ({', '.join(map(str, scorecard_ids))})",
        (org_id,),
    )

    await execute_db_operation(
        f"UPDATE {tasks_table_name} SET org_id = ? WHERE id IN ({', '.join(map(str, task_ids))})",
        (org_id,),
    )


async def get_cohorts_for_course(course_id: int):
    cohorts = await execute_db_operation(
        f"""
        SELECT ch.id, ch.name, cc.is_drip_enabled, cc.frequency_value, cc.frequency_unit, cc.publish_at
        FROM {cohorts_table_name} ch
        JOIN {course_cohorts_table_name} cc ON ch.id = cc.cohort_id
        WHERE cc.course_id = ?
        """,
        (course_id,),
        fetch_all=True,
    )

    return [
        {
            "id": cohort[0],
            "name": cohort[1],
            "drip_config": {
                "is_drip_enabled": cohort[2],
                "frequency_value": cohort[3],
                "frequency_unit": cohort[4],
                "publish_at": cohort[5],
            },
        }
        for cohort in cohorts
    ]


def drop_course_cohorts_table():
    execute_multiple_db_operations(
        [
            (f"DELETE FROM {course_cohorts_table_name}", ()),
            (f"DROP TABLE IF EXISTS {course_cohorts_table_name}", ()),
        ]
    )


def drop_courses_table():
    drop_course_cohorts_table()

    execute_multiple_db_operations(
        [
            (f"DELETE FROM {courses_table_name}", ()),
            (f"DROP TABLE IF EXISTS {courses_table_name}", ()),
        ]
    )


async def get_tasks_for_course(course_id: int, milestone_id: int = None):
    query = f"""SELECT t.id, t.name, COALESCE(m.name, '{uncategorized_milestone_name}') as milestone_name, t.verified, t.input_type, t.response_type, t.coding_language, ct.ordering, ct.id as course_task_id, ct.milestone_id, t.type
        FROM {tasks_table_name} t
        JOIN {course_tasks_table_name} ct ON ct.task_id = t.id 
        LEFT JOIN {milestones_table_name} m ON ct.milestone_id = m.id
        WHERE t.deleted_at IS NULL
        """

    params = []

    if milestone_id is not None:
        query += f" AND ct.course_id = ? AND ct.milestone_id = ?"
        params.extend([course_id, milestone_id])
    else:
        query += " AND ct.course_id = ?"
        params.append(course_id)

    query += " ORDER BY ct.ordering"

    tasks = await execute_db_operation(query, tuple(params), fetch_all=True)

    return [
        {
            "id": task[0],
            "name": task[1],
            "milestone": task[2],
            "verified": task[3],
            "input_type": task[4],
            "response_type": task[5],
            "coding_language": json.loads(task[6]) if task[6] else [],
            "ordering": task[7],
            "course_task_id": task[8],
            "milestone_id": task[9],
            "type": task[10],
        }
        for task in tasks
    ]


async def get_milestones_for_course(course_id: int):
    milestones = await execute_db_operation(
        f"SELECT cm.id, cm.milestone_id, m.name, cm.ordering FROM {course_milestones_table_name} cm JOIN {milestones_table_name} m ON cm.milestone_id = m.id WHERE cm.course_id = ? ORDER BY cm.ordering",
        (course_id,),
        fetch_all=True,
    )
    return [
        {
            "course_milestone_id": milestone[0],
            "id": milestone[1],
            "name": milestone[2],
            "ordering": milestone[3],
        }
        for milestone in milestones
    ]


async def get_all_courses_for_org(org_id: int):
    courses = await execute_db_operation(
        f"SELECT id, name FROM {courses_table_name} WHERE org_id = ? ORDER BY id DESC",
        (org_id,),
        fetch_all=True,
    )

    return [convert_course_db_to_dict(course) for course in courses]


async def delete_course(course_id: int):
    await execute_multiple_db_operations(
        [
            (
                f"DELETE FROM {course_cohorts_table_name} WHERE course_id = ?",
                (course_id,),
            ),
            (
                f"DELETE FROM {course_tasks_table_name} WHERE course_id = ?",
                (course_id,),
            ),
            (
                f"DELETE FROM {course_milestones_table_name} WHERE course_id = ?",
                (course_id,),
            ),
            (
                f"DELETE FROM {course_generation_jobs_table_name} WHERE course_id = ?",
                (course_id,),
            ),
            (
                f"DELETE FROM {task_generation_jobs_table_name} WHERE course_id = ?",
                (course_id,),
            ),
            (f"DELETE FROM {courses_table_name} WHERE id = ?", (course_id,)),
        ]
    )


def delete_all_courses_for_org(org_id: int):
    execute_multiple_db_operations(
        [
            (
                f"DELETE FROM {course_cohorts_table_name} WHERE course_id IN (SELECT id FROM {courses_table_name} WHERE org_id = ?)",
                (org_id,),
            ),
            (f"DELETE FROM {courses_table_name} WHERE org_id = ?", (org_id,)),
        ]
    )


async def swap_milestone_ordering_for_course(
    course_id: int, milestone_1_id: int, milestone_2_id: int
):
    # First, check if both milestones exist for the course
    milestone_entries = await execute_db_operation(
        f"SELECT milestone_id, ordering FROM {course_milestones_table_name} WHERE course_id = ? AND milestone_id IN (?, ?)",
        (course_id, milestone_1_id, milestone_2_id),
        fetch_all=True,
    )

    if len(milestone_entries) != 2:
        raise ValueError("One or both milestones do not exist for this course")

    # Get the IDs and orderings for the course_milestones entries
    milestone_1_id, milestone_1_ordering = milestone_entries[0]
    milestone_2_id, milestone_2_ordering = milestone_entries[1]

    update_params = [
        (milestone_2_ordering, milestone_1_id),
        (milestone_1_ordering, milestone_2_id),
    ]

    await execute_many_db_operation(
        f"UPDATE {course_milestones_table_name} SET ordering = ? WHERE id = ?",
        params_list=update_params,
    )


async def swap_task_ordering_for_course(course_id: int, task_1_id: int, task_2_id: int):
    # First, check if both tasks exist for the course
    task_entries = await execute_db_operation(
        f"SELECT task_id, milestone_id, ordering FROM {course_tasks_table_name} WHERE course_id = ? AND task_id IN (?, ?)",
        (course_id, task_1_id, task_2_id),
        fetch_all=True,
    )

    if len(task_entries) != 2:
        raise ValueError("One or both tasks do not exist for this course")

    # Get the IDs and orderings for the course_tasks entries
    task_1_id, task_1_milestone_id, task_1_ordering = task_entries[0]
    task_2_id, task_2_milestone_id, task_2_ordering = task_entries[1]

    if task_1_milestone_id != task_2_milestone_id:
        raise ValueError("Tasks are not in the same milestone")

    update_params = [
        (task_2_ordering, task_1_id),
        (task_1_ordering, task_2_id),
    ]

    await execute_many_db_operation(
        f"UPDATE {course_tasks_table_name} SET ordering = ? WHERE id = ?",
        params_list=update_params,
    )


async def create_course(name: str, org_id: int) -> int:
    org = await get_org_by_id(org_id)

    if not org:
        raise Exception(f"Organization with id '{org_id}' not found")

    course_id = await execute_db_operation(
        f"""
        INSERT INTO {courses_table_name} (name, org_id)
        VALUES (?, ?)
        """,
        (name, org_id),
        get_last_row_id=True,
    )

    await send_slack_notification_for_new_course(name, course_id, org["slug"], org_id)

    return course_id


def convert_course_db_to_dict(course: Tuple) -> Dict:
    result = {
        "id": course[0],
        "name": course[1],
    }

    if len(course) > 2:
        result["org"] = {
            "id": course[2],
            "name": course[3],
            "slug": course[4],
        }

    return result


async def get_course_org_id(course_id: int) -> int:
    course = await execute_db_operation(
        f"SELECT org_id FROM {courses_table_name} WHERE id = ?",
        (course_id,),
        fetch_one=True,
    )

    if not course:
        raise ValueError("Course not found")

    return course[0]


async def get_course(course_id: int, only_published: bool = True) -> Dict:
    course = await execute_db_operation(
        f"SELECT c.id, c.name, cgj.status as course_generation_status FROM {courses_table_name} c LEFT JOIN {course_generation_jobs_table_name} cgj ON c.id = cgj.course_id WHERE c.id = ?",
        (course_id,),
        fetch_one=True,
    )

    if not course:
        return None

    # Fix the milestones query to match the actual schema
    milestones = await execute_db_operation(
        f"""SELECT m.id, m.name, m.color, cm.ordering 
            FROM {course_milestones_table_name} cm
            JOIN milestones m ON cm.milestone_id = m.id
            WHERE cm.course_id = ? ORDER BY cm.ordering""",
        (course_id,),
        fetch_all=True,
    )

    # Fetch all tasks for this course
    tasks = await execute_db_operation(
        f"""SELECT t.id, t.title, t.type, t.status, t.scheduled_publish_at, ct.milestone_id, ct.ordering,
            (CASE WHEN t.type = '{TaskType.QUIZ}' THEN 
                (SELECT COUNT(*) FROM {questions_table_name} q 
                 WHERE q.task_id = t.id)
             ELSE NULL END) as num_questions,
            tgj.status as task_generation_status
            FROM {course_tasks_table_name} ct
            JOIN {tasks_table_name} t ON ct.task_id = t.id
            LEFT JOIN {task_generation_jobs_table_name} tgj ON t.id = tgj.task_id
            WHERE ct.course_id = ? AND t.deleted_at IS NULL
            {
                f"AND t.status = '{TaskStatus.PUBLISHED}' AND t.scheduled_publish_at IS NULL"
                if only_published
                else ""
            }
            ORDER BY ct.milestone_id, ct.ordering""",
        (course_id,),
        fetch_all=True,
    )

    # Group tasks by milestone_id
    tasks_by_milestone = defaultdict(list)
    for task in tasks:
        milestone_id = task[5]

        tasks_by_milestone[milestone_id].append(
            {
                "id": task[0],
                "title": task[1],
                "type": task[2],
                "status": task[3],
                "scheduled_publish_at": task[4],
                "ordering": task[6],
                "num_questions": task[7],
                "is_generating": task[8] is not None
                and task[8] == GenerateTaskJobStatus.STARTED,
            }
        )

    course_dict = {
        "id": course[0],
        "name": course[1],
        "course_generation_status": course[2],
    }
    course_dict["milestones"] = []

    for milestone in milestones:
        milestone_id = milestone[0]
        milestone_dict = {
            "id": milestone_id,
            "name": milestone[1],
            "color": milestone[2],
            "ordering": milestone[3],
            "tasks": tasks_by_milestone.get(milestone_id, []),
        }
        course_dict["milestones"].append(milestone_dict)

    return course_dict


async def update_course_name(course_id: int, name: str):
    await execute_db_operation(
        f"UPDATE {courses_table_name} SET name = ? WHERE id = ?",
        (name, course_id),
    )


async def check_and_insert_missing_course_milestones(
    course_tasks_to_add: List[Tuple[int, int, int]],
):
    # Find unique course, milestone pairs to validate they exist
    unique_course_milestone_pairs = {
        (course_id, milestone_id)
        for _, course_id, milestone_id in course_tasks_to_add
        if milestone_id is not None
    }

    if unique_course_milestone_pairs:
        # Verify all milestone IDs exist for their respective courses
        milestone_check = await execute_db_operation(
            f"""
            SELECT course_id, milestone_id FROM {course_milestones_table_name}
            WHERE (course_id, milestone_id) IN ({','.join(['(?,?)'] * len(unique_course_milestone_pairs))})
            """,
            tuple(itertools.chain(*unique_course_milestone_pairs)),
            fetch_all=True,
        )

        found_pairs = {(row[0], row[1]) for row in milestone_check}
        pairs_not_found = unique_course_milestone_pairs - found_pairs

        if pairs_not_found:
            # For each missing pair, get the max ordering for that course and increment
            for course_id, milestone_id in pairs_not_found:
                # Get current max ordering for this course
                max_ordering = (
                    await execute_db_operation(
                        f"SELECT COALESCE(MAX(ordering), -1) FROM {course_milestones_table_name} WHERE course_id = ?",
                        (course_id,),
                        fetch_one=True,
                    )
                )[0]

                # Insert with incremented ordering
                await execute_db_operation(
                    f"INSERT INTO {course_milestones_table_name} (course_id, milestone_id, ordering) VALUES (?, ?, ?)",
                    (course_id, milestone_id, max_ordering + 1),
                )


async def add_tasks_to_courses(course_tasks_to_add: List[Tuple[int, int, int]]):
    await check_and_insert_missing_course_milestones(course_tasks_to_add)

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # Group tasks by course_id
        course_to_tasks = defaultdict(list)
        for task_id, course_id, milestone_id in course_tasks_to_add:
            course_to_tasks[course_id].append((task_id, milestone_id))

        # For each course, get max ordering and insert tasks with incremented order
        for course_id, task_details in course_to_tasks.items():
            await cursor.execute(
                f"SELECT COALESCE(MAX(ordering), -1) FROM {course_tasks_table_name} WHERE course_id = ?",
                (course_id,),
            )
            max_ordering = (await cursor.fetchone())[0]

            # Insert tasks with incremented ordering
            values_to_insert = []
            for i, (task_id, milestone_id) in enumerate(task_details, start=1):
                values_to_insert.append(
                    (task_id, course_id, max_ordering + i, milestone_id)
                )

            await cursor.executemany(
                f"INSERT OR IGNORE INTO {course_tasks_table_name} (task_id, course_id, ordering, milestone_id) VALUES (?, ?, ?, ?)",
                values_to_insert,
            )

        await conn.commit()


async def remove_tasks_from_courses(course_tasks_to_remove: List[Tuple[int, int]]):
    await execute_many_db_operation(
        f"DELETE FROM {course_tasks_table_name} WHERE task_id = ? AND course_id = ?",
        params_list=course_tasks_to_remove,
    )


async def update_task_orders(task_orders: List[Tuple[int, int]]):
    await execute_many_db_operation(
        f"UPDATE {course_tasks_table_name} SET ordering = ? WHERE id = ?",
        params_list=task_orders,
    )


async def add_milestone_to_course(
    course_id: int, milestone_name: str, milestone_color: str
) -> Tuple[int, int]:
    org_id = await get_org_id_for_course(course_id)

    # Wrap the entire operation in a transaction
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # Get the max ordering value for this course
        await cursor.execute(
            f"INSERT INTO {milestones_table_name} (name, color, org_id) VALUES (?, ?, ?)",
            (milestone_name, milestone_color, org_id),
        )

        milestone_id = cursor.lastrowid

        await cursor.execute(
            f"SELECT COALESCE(MAX(ordering), -1) FROM {course_milestones_table_name} WHERE course_id = ?",
            (course_id,),
        )
        max_ordering = await cursor.fetchone()

        # Set the new milestone's order to be the next value
        next_order = max_ordering[0] + 1 if max_ordering else 0

        await cursor.execute(
            f"INSERT INTO {course_milestones_table_name} (course_id, milestone_id, ordering) VALUES (?, ?, ?)",
            (course_id, milestone_id, next_order),
        )

        await conn.commit()

        return milestone_id, next_order


async def update_milestone_orders(milestone_orders: List[Tuple[int, int]]):
    await execute_many_db_operation(
        f"UPDATE {course_milestones_table_name} SET ordering = ? WHERE id = ?",
        params_list=milestone_orders,
    )


async def get_user_courses(user_id: int) -> List[Dict]:
    """
    Get all courses for a user based on different roles:
    1. Courses where the user is a learner or mentor through cohorts
    2. All courses from organizations where the user is an admin or owner

    Args:
        user_id: The ID of the user

    Returns:
        List of course dictionaries with their details and user's role
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # Get all courses where the user is a learner or mentor through cohorts
        user_cohorts = await get_user_cohorts(user_id)

        # Dictionary to track user's role in each course
        course_roles = {}
        course_to_cohort = {}

        # Add courses from user's cohorts with their roles
        for cohort in user_cohorts:
            cohort_id = cohort["id"]
            user_role_in_cohort = cohort.get("role")  # Get user's role in this cohort

            cohort_courses = await get_courses_for_cohort(cohort_id)
            for course in cohort_courses:
                course_id = course["id"]
                course_to_cohort[course_id] = cohort_id

                # Only update role if not already an admin/owner
                if course_id not in course_roles or course_roles[course_id] not in [
                    "admin",
                    "owner",
                ]:
                    course_roles[course_id] = user_role_in_cohort

        # Get organizations where the user is an admin or owner
        user_orgs = await get_user_organizations(user_id)
        admin_owner_org_ids = [
            org["id"] for org in user_orgs if org["role"] in ["admin", "owner"]
        ]

        # Add all courses from organizations where user is admin or owner
        for org_id in admin_owner_org_ids:
            org_courses = await get_all_courses_for_org(org_id)
            for course in org_courses:
                course_id = course["id"]
                # Admin/owner role takes precedence
                course_roles[course_id] = "admin"

        # If no courses found, return empty list
        if not course_roles:
            return []

        # Fetch detailed information for all course IDs
        courses = []
        for course_id, role in course_roles.items():
            # Fetch course from DB including org_id
            await cursor.execute(
                f"SELECT c.id, c.name, o.id, o.name, o.slug FROM {courses_table_name} c JOIN {organizations_table_name} o ON c.org_id = o.id WHERE c.id = ?",
                (course_id,),
            )
            course_row = await cursor.fetchone()
            if course_row:
                course_dict = convert_course_db_to_dict(course_row)
                course_dict["role"] = role  # Add user's role to the course dictionary

                if role == group_role_learner:
                    course_dict["cohort_id"] = course_to_cohort[course_id]

                courses.append(course_dict)

        return courses
