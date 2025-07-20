from collections import defaultdict
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict

import numpy as np
from api.db.cohort import (
    get_all_cohorts_for_org as get_all_cohorts_for_org_from_db,
    create_cohort as create_cohort_in_db,
    get_cohort_by_id as get_cohort_by_id_from_db,
    add_members_to_cohort as add_members_to_cohort_in_db,
    remove_members_from_cohort as remove_members_from_cohort_in_db,
    delete_cohort as delete_cohort_from_db,
    update_cohort_name as update_cohort_name_in_db,
    add_courses_to_cohort as add_courses_to_cohort_in_db,
    remove_courses_from_cohort as remove_courses_from_cohort_in_db,
    get_cohort_analytics_metrics_for_tasks as get_cohort_analytics_metrics_for_tasks_from_db,
    get_cohort_attempt_data_for_tasks as get_cohort_attempt_data_for_tasks_from_db,
)
from api.db.batch import validate_batch_belongs_to_cohort
from api.db.course import get_courses_for_cohort as get_courses_for_cohort_from_db
from api.db.analytics import (
    get_cohort_completion as get_cohort_completion_from_db,
    get_cohort_course_attempt_data as get_cohort_course_attempt_data_from_db,
    get_cohort_streaks as get_cohort_streaks_from_db,
)
from api.db.course import get_course as get_course_from_db
from api.utils.db import execute_db_operation
from api.config import (
    chat_history_table_name,
    questions_table_name,
    course_tasks_table_name,
    task_completions_table_name,
    tasks_table_name,
)
from api.models import (
    CreateCohortRequest,
    CreateCohortGroupRequest,
    AddMembersToCohortRequest,
    RemoveMembersFromCohortRequest,
    UpdateCohortGroupRequest,
    AddMembersToCohortGroupRequest,
    RemoveMembersFromCohortGroupRequest,
    UpdateCohortRequest,
    UpdateCohortGroupRequest,
    AddCoursesToCohortRequest,
    CreateCohortResponse,
    RemoveCoursesFromCohortRequest,
    Streaks,
    LeaderboardViewType,
    CohortCourse,
    CourseWithMilestonesAndTasks,
    UserCourseRole,
    TaskType,
    TaskStatus,
)

router = APIRouter()


@router.get("/")
async def get_all_cohorts_for_org(org_id: int) -> List[Dict]:
    return await get_all_cohorts_for_org_from_db(org_id)


@router.post("/", response_model=CreateCohortResponse)
async def create_cohort(request: CreateCohortRequest) -> CreateCohortResponse:
    return {"id": await create_cohort_in_db(request.name, request.org_id)}


@router.get("/{cohort_id}")
async def get_cohort_by_id(cohort_id: int, batch_id: int | None = None) -> Dict:
    cohort_data = await get_cohort_by_id_from_db(cohort_id, batch_id)
    if not cohort_data:
        raise HTTPException(status_code=404, detail="Cohort not found")

    return cohort_data


@router.post("/{cohort_id}/members")
async def add_members_to_cohort(cohort_id: int, request: AddMembersToCohortRequest):
    try:
        await add_members_to_cohort_in_db(
            cohort_id, request.org_slug, request.org_id, request.emails, request.roles
        )
        return {"success": True}
    except Exception as e:
        if "User already exists in cohort" in str(e):
            raise HTTPException(status_code=400, detail=str(e))
        elif "Cannot add an admin to the cohort" in str(e):
            raise HTTPException(status_code=401, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{cohort_id}/members")
async def remove_members_from_cohort(
    cohort_id: int, request: RemoveMembersFromCohortRequest
):
    try:
        await remove_members_from_cohort_in_db(cohort_id, request.member_ids)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{cohort_id}")
async def delete_cohort(cohort_id: int):
    await delete_cohort_from_db(cohort_id)
    return {"success": True}


@router.put("/{cohort_id}")
async def update_cohort_name(cohort_id: int, request: UpdateCohortRequest):
    await update_cohort_name_in_db(cohort_id, request.name)
    return {"success": True}


@router.post("/{cohort_id}/courses")
async def add_courses_to_cohort(cohort_id: int, request: AddCoursesToCohortRequest):
    await add_courses_to_cohort_in_db(
        cohort_id,
        request.course_ids,
        is_drip_enabled=request.drip_config.is_drip_enabled,
        frequency_value=request.drip_config.frequency_value,
        frequency_unit=request.drip_config.frequency_unit,
        publish_at=request.drip_config.publish_at,
    )
    return {"success": True}


@router.delete("/{cohort_id}/courses")
async def remove_courses_from_cohort(
    cohort_id: int, request: RemoveCoursesFromCohortRequest
):
    await remove_courses_from_cohort_in_db(cohort_id, request.course_ids)
    return {"success": True}


@router.get(
    "/{cohort_id}/courses",
    response_model=List[CourseWithMilestonesAndTasks | CohortCourse],
)
async def get_courses_for_cohort(
    cohort_id: int, include_tree: bool = False, joined_at: datetime | None = None
) -> List[CourseWithMilestonesAndTasks | CohortCourse]:
    return await get_courses_for_cohort_from_db(cohort_id, include_tree, joined_at)


@router.get(
    "/{cohort_id}/completion",
    response_model=Dict,
)
async def get_cohort_completion(cohort_id: int, user_id: int) -> Dict:
    results = await get_cohort_completion_from_db(cohort_id, [user_id])
    return results[user_id]


@router.get("/{cohort_id}/leaderboard")
async def get_leaderboard_data(cohort_id: int, batch_id: int | None = None) -> Dict:
    leaderboard_data = await get_cohort_streaks_from_db(
        cohort_id=cohort_id, batch_id=batch_id
    )

    user_ids = [streak["user"]["id"] for streak in leaderboard_data]

    if not user_ids:
        return {}

    task_completions = await get_cohort_completion_from_db(cohort_id, user_ids)

    num_tasks = len(task_completions[user_ids[0]])

    for user_data in leaderboard_data:
        user_id = user_data["user"]["id"]
        num_tasks_completed = 0

        for task_completion_data in task_completions[user_id].values():
            if task_completion_data["is_complete"]:
                num_tasks_completed += 1

        user_data["tasks_completed"] = num_tasks_completed

    leaderboard_data = sorted(
        leaderboard_data,
        key=lambda x: (x["streak_count"], x["tasks_completed"]),
        reverse=True,
    )

    return {
        "stats": leaderboard_data,
        "metadata": {
            "num_tasks": num_tasks,
        },
    }


@router.get("/{cohort_id}/courses/{course_id}/metrics")
async def get_cohort_metrics_for_course(
    cohort_id: int, course_id: int, batch_id: int | None = None
):
    # Validate batch belongs to cohort if batch_id is provided
    if batch_id is not None:
        batch_valid = await validate_batch_belongs_to_cohort(batch_id, cohort_id)
        if not batch_valid:
            raise HTTPException(
                status_code=400, detail="Batch does not belong to the specified cohort"
            )

    course_data = await get_course_from_db(course_id, only_published=True)
    cohort_data = await get_cohort_by_id_from_db(cohort_id, batch_id)

    if not course_data:
        raise HTTPException(status_code=404, detail="Course not found")

    if not cohort_data:
        raise HTTPException(status_code=404, detail="Cohort not found")

    task_id_to_metadata = {}
    task_type_counts = defaultdict(int)

    for milestone in course_data["milestones"]:
        for task in milestone["tasks"]:
            task_id_to_metadata[task["id"]] = {
                "milestone_id": milestone["id"],
                "milestone_name": milestone["name"],
                "type": task["type"],
            }
            task_type_counts[task["type"]] += 1

    learner_ids = [
        member["id"]
        for member in cohort_data["members"]
        if member["role"] == UserCourseRole.LEARNER
    ]

    if not learner_ids:
        return {}

    task_completions = await get_cohort_completion_from_db(
        cohort_id, learner_ids, course_id
    )

    course_attempt_data = await get_cohort_course_attempt_data_from_db(
        learner_ids, course_id
    )

    num_tasks = len(task_completions[learner_ids[0]])

    if not num_tasks:
        return {}

    # Get last active date for each learner (reusing user streak logic pattern)
    learner_ids_str = ",".join(map(str, learner_ids))
    last_active_data = await execute_db_operation(
        f"""
        SELECT 
            user_id,
            MAX(datetime(created_at, '+5 hours', '+30 minutes')) as last_active
        FROM (
            SELECT user_id, created_at
            FROM {chat_history_table_name}
            WHERE user_id IN ({learner_ids_str}) 
            AND question_id IN (
                SELECT id FROM {questions_table_name} 
                WHERE task_id IN (
                    SELECT task_id FROM {course_tasks_table_name} 
                    WHERE course_id = ?
                )
            )
            
            UNION
            
            SELECT user_id, created_at
            FROM {task_completions_table_name}
            WHERE user_id IN ({learner_ids_str})
            AND task_id IN (
                SELECT task_id FROM {course_tasks_table_name} 
                WHERE course_id = ?
            )
        )
        GROUP BY user_id
        """,
        (course_id, course_id),
        fetch_all=True,
    )

    learner_last_active = {row[0]: row[1] for row in last_active_data}

    # Get attempt data for quiz tasks (reusing existing attempt tracking pattern)
    quiz_attempt_data = await execute_db_operation(
        f"""
        SELECT DISTINCT ch.user_id, t.id as task_id
        FROM {chat_history_table_name} ch
        JOIN {questions_table_name} q ON ch.question_id = q.id
        JOIN {tasks_table_name} t ON q.task_id = t.id
        JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
        WHERE ch.user_id IN ({learner_ids_str}) 
        AND ct.course_id = ?
        AND t.type = '{TaskType.QUIZ}'
        AND t.status = '{TaskStatus.PUBLISHED}'
        AND t.scheduled_publish_at IS NULL
        AND t.deleted_at IS NULL
        """,
        (course_id,),
        fetch_all=True,
    )

    # Track attempts per user per task type
    task_type_attempts = defaultdict(lambda: defaultdict(int))
    quiz_attempted_tasks = defaultdict(set)

    for user_id, task_id in quiz_attempt_data:
        if task_id in task_id_to_metadata:
            task_type = task_id_to_metadata[task_id]["type"]
            task_type_attempts[task_type][user_id] += 1
            quiz_attempted_tasks[user_id].add(task_id)

    # Track learning material attempts (from task_completions table)
    learning_attempt_data = await execute_db_operation(
        f"""
        SELECT DISTINCT tc.user_id, t.id as task_id
        FROM {task_completions_table_name} tc
        JOIN {tasks_table_name} t ON tc.task_id = t.id
        JOIN {course_tasks_table_name} ct ON t.id = ct.task_id
        WHERE tc.user_id IN ({learner_ids_str})
        AND ct.course_id = ?
        AND t.type = '{TaskType.LEARNING_MATERIAL}'
        AND t.status = '{TaskStatus.PUBLISHED}'
        AND t.scheduled_publish_at IS NULL
        AND t.deleted_at IS NULL
        """,
        (course_id,),
        fetch_all=True,
    )

    learning_attempted_tasks = defaultdict(set)
    for user_id, task_id in learning_attempt_data:
        if task_id in task_id_to_metadata:
            task_type = task_id_to_metadata[task_id]["type"]
            task_type_attempts[task_type][user_id] += 1
            learning_attempted_tasks[user_id].add(task_id)

    task_type_completions = defaultdict(lambda: defaultdict(int))
    task_type_completion_rates = defaultdict(list)
    task_type_attempt_rates = defaultdict(list)

    user_data = defaultdict(lambda: defaultdict(int))

    for learner_id in learner_ids:
        num_tasks_completed = 0
        num_tasks_attempted = 0

        for task_id, task_completion_data in task_completions[learner_id].items():
            if task_id not in task_id_to_metadata:
                continue

            task_type = task_id_to_metadata[task_id]["type"]

            # Track completions
            if task_completion_data["is_complete"]:
                num_tasks_completed += 1
                task_type_completions[task_type][learner_id] += 1

            # Track attempts (quiz via chat history, learning material via task completions)
            if (
                task_type == TaskType.QUIZ
                and task_id in quiz_attempted_tasks[learner_id]
            ):
                num_tasks_attempted += 1
            elif (
                task_type == TaskType.LEARNING_MATERIAL
                and task_id in learning_attempted_tasks[learner_id]
            ):
                num_tasks_attempted += 1

        user_data[learner_id]["completed"] = num_tasks_completed
        user_data[learner_id]["attempted"] = num_tasks_attempted
        user_data[learner_id]["completion_percentage"] = num_tasks_completed / num_tasks
        user_data[learner_id]["attempt_percentage"] = num_tasks_attempted / num_tasks
        user_data[learner_id]["last_active"] = learner_last_active.get(learner_id)

        # Calculate per-task-type rates
        for task_type in task_type_counts.keys():
            task_type_completion_rates[task_type].append(
                task_type_completions[task_type][learner_id]
                / task_type_counts[task_type]
            )
            task_type_attempt_rates[task_type].append(
                task_type_attempts[task_type][learner_id] / task_type_counts[task_type]
            )

    is_learner_active = {
        learner_id: course_attempt_data[learner_id][course_id]["has_attempted"]
        for learner_id in learner_ids
    }

    return {
        "average_completion": np.mean(
            [
                user_data[learner_id]["completion_percentage"]
                for learner_id in learner_ids
            ]
        ),
        "average_attempt_rate": np.mean(
            [user_data[learner_id]["attempt_percentage"] for learner_id in learner_ids]
        ),
        "num_tasks": num_tasks,
        "num_active_learners": sum(is_learner_active.values()),
        "learner_details": [
            {
                "user_id": learner_id,
                "completed": user_data[learner_id]["completed"],
                "attempted": user_data[learner_id]["attempted"],
                "completion_percentage": user_data[learner_id]["completion_percentage"],
                "attempt_percentage": user_data[learner_id]["attempt_percentage"],
                "last_active": user_data[learner_id]["last_active"],
            }
            for learner_id in learner_ids
        ],
        "task_type_metrics": {
            task_type: {
                "completion_rate": (
                    np.mean(task_type_completion_rates[task_type])
                    if task_type in task_type_completion_rates
                    else 0
                ),
                "completion_numerator": sum(
                    task_type_completions[task_type][learner_id]
                    for learner_id in learner_ids
                ),
                "completion_denominator": task_type_counts[task_type]
                * len(learner_ids),
                "attempt_rate": (
                    np.mean(task_type_attempt_rates[task_type])
                    if task_type in task_type_attempt_rates
                    else 0
                ),
                "attempt_numerator": sum(
                    task_type_attempts[task_type][learner_id]
                    for learner_id in learner_ids
                ),
                "attempt_denominator": task_type_counts[task_type] * len(learner_ids),
                "count": task_type_counts[task_type],
                "completions": (
                    task_type_completions[task_type]
                    if task_type in task_type_completions
                    else {learner_id: 0 for learner_id in learner_ids}
                ),
                "attempts": (
                    task_type_attempts[task_type]
                    if task_type in task_type_attempts
                    else {learner_id: 0 for learner_id in learner_ids}
                ),
            }
            for task_type in task_type_counts.keys()
        },
    }


@router.get("/{cohort_id}/streaks", response_model=Streaks)
async def get_all_streaks_for_cohort(
    cohort_id: int = None, view: LeaderboardViewType = str(LeaderboardViewType.ALL_TIME)
) -> Streaks:
    return await get_cohort_streaks_from_db(view=view, cohort_id=cohort_id)


@router.get("/{cohort_id}/task_metrics")
async def get_cohort_analytics_metrics_for_tasks(
    cohort_id: int, task_ids: List[int] = Query(...), batch_id: int | None = None
):
    # Validate batch belongs to cohort if batch_id is provided
    if batch_id is not None:
        batch_valid = await validate_batch_belongs_to_cohort(batch_id, cohort_id)
        if not batch_valid:
            raise HTTPException(
                status_code=400, detail="Batch does not belong to the specified cohort"
            )

    return await get_cohort_analytics_metrics_for_tasks_from_db(
        cohort_id, task_ids, batch_id
    )


@router.get("/{cohort_id}/task_attempt_data")
async def get_cohort_attempt_data_for_tasks(
    cohort_id: int, task_ids: List[int] = Query(...), batch_id: int | None = None
):
    # Validate batch belongs to cohort if batch_id is provided
    if batch_id is not None:
        batch_valid = await validate_batch_belongs_to_cohort(batch_id, cohort_id)
        if not batch_valid:
            raise HTTPException(
                status_code=400, detail="Batch does not belong to the specified cohort"
            )

    return await get_cohort_attempt_data_for_tasks_from_db(
        cohort_id, task_ids, batch_id
    )
