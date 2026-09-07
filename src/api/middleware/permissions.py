from fastapi import HTTPException, Request

from api.utils.authorization import (
    courses_for_course_milestone_rows,
    courses_for_course_task_rows,
    can_access_cohort,
    can_access_course,
    can_access_task,
    cohort_for_batch,
    is_org_staff,
    is_staff_over_user,
    org_for_milestone,
    org_for_scorecard,
)
from api.utils.logging import logger


def _caller_id(request: Request) -> int:
    user_id = getattr(request.state, "user_id", None)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user_id


async def _decide(request: Request, allowed: bool, detail: str) -> None:
    if allowed:
        return

    logger.warning(
        f"AUTHZ blocked: user={getattr(request.state, 'user_id', None)} "
        f"{request.method} {request.url.path} - {detail}"
    )
    raise HTTPException(status_code=403, detail="Forbidden")


async def require_self(request: Request, user_id: int) -> None:
    """The caller may only act on their own user record."""
    caller = _caller_id(request)
    await _decide(request, caller == user_id, f"caller {caller} != user_id {user_id}")


async def require_user_scope(request: Request, user_id: int) -> None:
    """The caller themselves, or staff of an org the target user belongs to."""
    caller = _caller_id(request)
    allowed = caller == user_id or await is_staff_over_user(caller, user_id)
    await _decide(request, allowed, f"caller {caller} cannot act for user {user_id}")


async def require_org_staff(request: Request, org_id: int) -> None:
    caller = _caller_id(request)
    await _decide(
        request, await is_org_staff(caller, org_id), f"not staff of org {org_id}"
    )


async def require_cohort_access(request: Request, cohort_id: int) -> None:
    caller = _caller_id(request)
    await _decide(
        request, await can_access_cohort(caller, cohort_id), f"no access to cohort {cohort_id}"
    )


async def require_course_access(request: Request, course_id: int) -> None:
    caller = _caller_id(request)
    await _decide(
        request, await can_access_course(caller, course_id), f"no access to course {course_id}"
    )


async def require_task_access(request: Request, task_id: int) -> None:
    caller = _caller_id(request)
    await _decide(
        request, await can_access_task(caller, task_id), f"no access to task {task_id}"
    )


async def require_batch_access(request: Request, batch_id: int) -> None:
    caller = _caller_id(request)
    cohort_id = await cohort_for_batch(batch_id)
    allowed = cohort_id is not None and await can_access_cohort(caller, cohort_id)
    await _decide(request, allowed, f"no access to batch {batch_id}")


async def require_scorecard_access(request: Request, scorecard_id: int) -> None:
    caller = _caller_id(request)
    org_id = await org_for_scorecard(scorecard_id)
    allowed = org_id is not None and await is_org_staff(caller, org_id)
    await _decide(request, allowed, f"no access to scorecard {scorecard_id}")


async def require_milestone_access(request: Request, milestone_id: int) -> None:
    caller = _caller_id(request)
    org_id = await org_for_milestone(milestone_id)
    allowed = org_id is not None and await is_org_staff(caller, org_id)
    await _decide(request, allowed, f"no access to milestone {milestone_id}")


async def require_courses_access(request: Request, course_ids) -> None:
    """Every course in a bulk request must be reachable by the caller."""
    caller = _caller_id(request)

    for course_id in set(course_ids):
        if not await can_access_course(caller, course_id):
            await _decide(request, False, f"no access to course {course_id}")
            return


async def require_course_task_rows_access(request: Request, row_ids) -> None:
    await require_courses_access(request, await courses_for_course_task_rows(row_ids))


async def require_course_milestone_rows_access(request: Request, row_ids) -> None:
    await require_courses_access(request, await courses_for_course_milestone_rows(row_ids))
