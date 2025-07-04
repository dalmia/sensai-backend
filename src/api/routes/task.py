from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict
from api.db.task import (
    get_solved_tasks_for_user as get_solved_tasks_for_user_from_db,
    get_task as get_task_from_db,
    delete_task as delete_task_in_db,
    delete_tasks as delete_tasks_in_db,
    create_draft_task_for_course as create_draft_task_for_course_in_db,
    update_learning_material_task as update_learning_material_task_in_db,
    update_draft_quiz as update_draft_quiz_in_db,
    update_published_quiz as update_published_quiz_in_db,
    mark_task_completed as mark_task_completed_in_db,
    duplicate_task as duplicate_task_in_db,
    get_all_learning_material_tasks_for_course as get_all_learning_material_tasks_for_course_from_db,
)
from api.models import (
    Task,
    LearningMaterialTask,
    QuizTask,
    LeaderboardViewType,
    UpdateDraftQuizRequest,
    CreateDraftTaskRequest,
    TaskCourseResponse,
    CreateDraftTaskResponse,
    PublishLearningMaterialTaskRequest,
    UpdateLearningMaterialTaskRequest,
    UpdatePublishedQuizRequest,
    DuplicateTaskRequest,
    DuplicateTaskResponse,
    MarkTaskCompletedRequest,
)

router = APIRouter()


@router.get("/course/{course_id}/learning_material")
async def get_learning_material_tasks_for_course(
    course_id: int,
) -> List[Task]:
    return await get_all_learning_material_tasks_for_course_from_db(course_id)


@router.post("/", response_model=CreateDraftTaskResponse)
async def create_draft_task_for_course(
    request: CreateDraftTaskRequest,
) -> CreateDraftTaskResponse:
    id, _ = await create_draft_task_for_course_in_db(
        request.title,
        str(request.type),
        request.course_id,
        request.milestone_id,
    )
    return {"id": id}


@router.post("/{task_id}/learning_material", response_model=LearningMaterialTask)
async def publish_learning_material_task(
    task_id: int, request: PublishLearningMaterialTaskRequest
) -> LearningMaterialTask:
    result = await update_learning_material_task_in_db(
        task_id,
        request.title,
        request.blocks,
        request.scheduled_publish_at,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    return result


@router.put("/{task_id}/learning_material", response_model=LearningMaterialTask)
async def update_learning_material_task(
    task_id: int, request: UpdateLearningMaterialTaskRequest
) -> LearningMaterialTask:
    result = await update_learning_material_task_in_db(
        task_id,
        request.title,
        request.blocks,
        request.scheduled_publish_at,
        request.status,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    return result


@router.post("/{task_id}/quiz", response_model=QuizTask)
async def update_draft_quiz(task_id: int, request: UpdateDraftQuizRequest) -> QuizTask:
    result = await update_draft_quiz_in_db(
        task_id=task_id,
        title=request.title,
        questions=request.questions,
        scheduled_publish_at=request.scheduled_publish_at,
        status=request.status,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    return result


@router.put("/{task_id}/quiz", response_model=QuizTask)
async def update_published_quiz(
    task_id: int, request: UpdatePublishedQuizRequest
) -> QuizTask:
    result = await update_published_quiz_in_db(
        task_id=task_id,
        title=request.title,
        questions=request.questions,
        scheduled_publish_at=request.scheduled_publish_at,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    return result


@router.post("/duplicate", response_model=DuplicateTaskResponse)
async def duplicate_task(
    request: DuplicateTaskRequest,
) -> DuplicateTaskResponse:
    return await duplicate_task_in_db(
        request.task_id, request.course_id, request.milestone_id
    )


@router.delete("/{task_id}")
async def delete_task(task_id: int):
    await delete_task_in_db(task_id)
    return {"success": True}


@router.delete("/")
async def delete_tasks(task_ids: List[int] = Query(...)):
    await delete_tasks_in_db(task_ids)
    return {"success": True}


@router.get("/cohort/{cohort_id}/user/{user_id}/completed", response_model=List[int])
async def get_tasks_completed_for_user(
    user_id: int,
    cohort_id: int,
    view: LeaderboardViewType = str(LeaderboardViewType.ALL_TIME),
) -> List[int]:
    return await get_solved_tasks_for_user_from_db(user_id, cohort_id, view)


@router.get("/{task_id}")
async def get_task(task_id: int) -> LearningMaterialTask | QuizTask:
    task = await get_task_from_db(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.post("/{task_id}/complete")
async def mark_task_completed(task_id: int, request: MarkTaskCompletedRequest):
    await mark_task_completed_in_db(task_id, request.user_id)
    return {"success": True}
