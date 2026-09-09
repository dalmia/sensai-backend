from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List, Dict
from api.db.course import (
    create_course as create_course_in_db,
    get_all_courses_for_org as get_all_courses_for_org_from_db,
    delete_course as delete_course_in_db,
    get_courses_for_cohort as get_courses_for_cohort_from_db,
    get_cohorts_for_course as get_cohorts_for_course_from_db,
    get_tasks_for_course as get_tasks_for_course_from_db,
    update_course_name as update_course_name_in_db,
    add_tasks_to_courses as add_tasks_to_courses_in_db,
    remove_tasks_from_courses as remove_tasks_from_courses_in_db,
    update_task_orders as update_task_orders_in_db,
    add_milestone_to_course as add_milestone_to_course_in_db,
    update_milestone_orders as update_milestone_orders_in_db,
    get_course as get_course_from_db,
    swap_milestone_ordering_for_course as swap_milestone_ordering_for_course_in_db,
    swap_task_ordering_for_course as swap_task_ordering_for_course_in_db,
    duplicate_course_to_org,
)
from api.db.cohort import (
    add_course_to_cohorts as add_course_to_cohorts_in_db,
    remove_course_from_cohorts as remove_course_from_cohorts_from_db,
)
from api.models import (
    CreateCourseRequest,
    RemoveCourseFromCohortsRequest,
    AddCourseToCohortsRequest,
    UpdateCourseNameRequest,
    AddTasksToCoursesRequest,
    RemoveTasksFromCoursesRequest,
    UpdateTaskOrdersRequest,
    UpdateMilestoneOrdersRequest,
    CreateCourseResponse,
    Course,
    CourseWithMilestonesAndTasks,
    AddMilestoneToCourseRequest,
    AddMilestoneToCourseResponse,
    SwapMilestoneOrderingRequest,
    SwapTaskOrderingRequest,
    CourseCohort,
    DuplicateCourseRequest,
)

from api.middleware.permissions import require_course_access, require_course_write, require_org_staff

from api.middleware import permissions

router = APIRouter()


@router.post("/", response_model=CreateCourseResponse)
async def create_course(http_request: Request, request: CreateCourseRequest) -> CreateCourseResponse:
    await permissions.require_org_staff(http_request, request.org_id)
    return {"id": await create_course_in_db(request.name, request.org_id)}


@router.get("/", dependencies=[Depends(require_org_staff)])
async def get_all_courses_for_org(org_id: int) -> List[Course]:
    return await get_all_courses_for_org_from_db(org_id)


@router.get("/{course_id}", dependencies=[Depends(require_course_access)], response_model=CourseWithMilestonesAndTasks)
async def get_course(
    course_id: int, only_published: bool = True
) -> CourseWithMilestonesAndTasks:
    return await get_course_from_db(course_id, only_published)


@router.post("/tasks")
async def add_tasks_to_courses(http_request: Request, request: AddTasksToCoursesRequest):
    # Tuple is (task_id, course_id, milestone_id) - both ids need authorizing,
    # or you can graft another org's task into a course you own.
    await permissions.require_courses_write(http_request, [t[1] for t in request.course_tasks])
    await permissions.require_tasks_write(http_request, [t[0] for t in request.course_tasks])
    await add_tasks_to_courses_in_db(request.course_tasks)
    return {"success": True}


@router.delete("/tasks")
async def remove_tasks_from_courses(http_request: Request, request: RemoveTasksFromCoursesRequest):
    await permissions.require_courses_write(http_request, [t[1] for t in request.course_tasks])
    await permissions.require_tasks_write(http_request, [t[0] for t in request.course_tasks])
    await remove_tasks_from_courses_in_db(request.course_tasks)
    return {"success": True}


@router.put("/tasks/order")
async def update_task_orders(http_request: Request, request: UpdateTaskOrdersRequest):
    await permissions.require_course_task_rows_write(http_request, [t[1] for t in request.task_orders])
    await update_task_orders_in_db(request.task_orders)
    return {"success": True}


@router.post("/{course_id}/milestones", dependencies=[Depends(require_course_write)])
async def add_milestone_to_course(
    course_id: int, request: AddMilestoneToCourseRequest
) -> AddMilestoneToCourseResponse:
    milestone_id, _ = await add_milestone_to_course_in_db(
        course_id,
        request.name,
        request.color,
    )
    return {"id": milestone_id}


@router.put("/milestones/order")
async def update_milestone_orders(http_request: Request, request: UpdateMilestoneOrdersRequest):
    await permissions.require_course_milestone_rows_write(http_request, [t[1] for t in request.milestone_orders])
    await update_milestone_orders_in_db(request.milestone_orders)
    return {"success": True}


@router.delete("/{course_id}", dependencies=[Depends(require_course_write)])
async def delete_course(course_id: int):
    await delete_course_in_db(course_id)
    return {"success": True}


@router.post("/{course_id}/cohorts")
async def add_course_to_cohorts(
    http_request: Request, course_id: int, request: AddCourseToCohortsRequest
):
    await permissions.require_same_org_for_course(
        http_request, course_id, request.cohort_ids
    )
    await add_course_to_cohorts_in_db(
        course_id,
        request.cohort_ids,
        is_drip_enabled=request.drip_config.is_drip_enabled,
        frequency_value=request.drip_config.frequency_value,
        frequency_unit=request.drip_config.frequency_unit,
        publish_at=request.drip_config.publish_at,
    )
    return {"success": True}


@router.delete("/{course_id}/cohorts", dependencies=[Depends(require_course_write)])
async def remove_course_from_cohorts(
    course_id: int, request: RemoveCourseFromCohortsRequest
):
    await remove_course_from_cohorts_from_db(course_id, request.cohort_ids)
    return {"success": True}


@router.get("/{course_id}/cohorts", dependencies=[Depends(require_course_access)])
async def get_cohorts_for_course(course_id: int) -> List[CourseCohort]:
    return await get_cohorts_for_course_from_db(course_id)


@router.get("/{course_id}/tasks", dependencies=[Depends(require_course_access)])
async def get_tasks_for_course(course_id: int) -> List[Dict]:
    return await get_tasks_for_course_from_db(course_id)


@router.put("/{course_id}", dependencies=[Depends(require_course_write)])
async def update_course_name(course_id: int, request: UpdateCourseNameRequest):
    await update_course_name_in_db(course_id, request.name)
    return {"success": True}


@router.put("/{course_id}/milestones/swap", dependencies=[Depends(require_course_write)])
async def swap_milestone_ordering(
    course_id: int, request: SwapMilestoneOrderingRequest
):
    await swap_milestone_ordering_for_course_in_db(
        course_id, request.milestone_1_id, request.milestone_2_id
    )
    return {"success": True}


@router.put("/{course_id}/tasks/swap", dependencies=[Depends(require_course_write)])
async def swap_task_ordering(course_id: int, request: SwapTaskOrderingRequest):
    await swap_task_ordering_for_course_in_db(
        course_id, request.task_1_id, request.task_2_id
    )
    return {"success": True}


@router.post("/{course_id}/duplicate", dependencies=[Depends(require_course_write)], response_model=CourseWithMilestonesAndTasks)
async def duplicate_course(
    http_request: Request, course_id: int, request: DuplicateCourseRequest
):
    # The dependency covers the source course; the destination org is separate.
    await permissions.require_org_staff(http_request, request.org_id)
    return await duplicate_course_to_org(course_id, request.org_id)
