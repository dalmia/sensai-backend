from fastapi import HTTPException, Request

from api.config import course_milestones_table_name, course_tasks_table_name
from api.utils.authorization import (
    existing_row_ids,
    org_id_for_slug,
    org_for_course,
    org_for_cohort,
    org_for_task,
    courses_for_course_milestone_rows,
    courses_for_course_task_rows,
    can_access_cohort,
    can_access_course,
    can_access_task,
    cohort_for_batch,
    is_org_staff,
    is_staff_over_user,
    is_mentor_over_user,
    org_for_milestone,
    org_for_scorecard,
)
from api.utils.logging import logger


def caller_id(request: Request) -> int:
    """The authenticated caller, from the verified token."""
    return _caller_id(request)


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
    allowed = (
        caller == user_id
        or await is_staff_over_user(caller, user_id)
        or await is_mentor_over_user(caller, user_id)
    )
    await _decide(request, allowed, f"caller {caller} cannot act for user {user_id}")


async def require_user_write(request: Request, user_id: int) -> None:
    """
    Writing to a user's record is self-or-org-staff only.

    Deliberately excludes the mentor arm in require_user_scope: a mentor may
    read the learners in their cohorts, not rename them or delete their work.
    """
    caller = _caller_id(request)
    allowed = caller == user_id or await is_staff_over_user(caller, user_id)
    await _decide(request, allowed, f"caller {caller} cannot write to user {user_id}")


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


async def require_org_staff_for(request: Request, org_id, detail: str) -> None:
    caller = _caller_id(request)
    allowed = org_id is not None and await is_org_staff(caller, org_id)
    await _decide(request, allowed, detail)


async def require_course_write(request: Request, course_id: int) -> None:
    await require_org_staff_for(
        request, await org_for_course(course_id), f"no write access to course {course_id}"
    )


async def require_cohort_write(request: Request, cohort_id: int) -> None:
    await require_org_staff_for(
        request, await org_for_cohort(cohort_id), f"no write access to cohort {cohort_id}"
    )


async def require_task_write(request: Request, task_id: int) -> None:
    await require_org_staff_for(
        request, await org_for_task(task_id), f"no write access to task {task_id}"
    )


async def require_batch_write(request: Request, batch_id: int) -> None:
    cohort_id = await cohort_for_batch(batch_id)
    org_id = await org_for_cohort(cohort_id) if cohort_id is not None else None
    await require_org_staff_for(request, org_id, f"no write access to batch {batch_id}")


async def require_courses_write(request: Request, course_ids) -> None:
    """Every course in a bulk request must be writable by the caller."""
    for course_id in set(course_ids):
        await require_course_write(request, course_id)


async def _require_rows_write(request: Request, row_ids, resolver, table: str, label: str) -> None:
    try:
        wanted = {int(row_id) for row_id in row_ids}
    except (TypeError, ValueError):
        await _decide(request, False, f"malformed {label} id")
        return

    if not wanted:
        return

    resolved = await resolver(wanted)
    missing = wanted - set(resolved)

    if missing:
        # A row deleted while the client held it is stale, not forbidden.
        # Saying "Forbidden" sends the admin to ask for access on a course they
        # own, when the fix is to refresh.
        stale = await existing_row_ids(table, missing)
        if stale:
            raise HTTPException(
                status_code=409,
                detail="Some of these items no longer exist. Refresh and try again.",
            )
        await _decide(request, False, f"unknown {label}: {sorted(missing)}")

    await require_courses_write(request, set(resolved.values()))



async def require_course_task_rows_write(request: Request, row_ids) -> None:
    await _require_rows_write(
        request,
        row_ids,
        courses_for_course_task_rows,
        course_tasks_table_name,
        "course_task row",
    )


async def require_course_milestone_rows_write(request: Request, row_ids) -> None:
    await _require_rows_write(
        request,
        row_ids,
        courses_for_course_milestone_rows,
        course_milestones_table_name,
        "course_milestone row",
    )



async def require_cohort_join_or_write(
    request: Request, cohort_id: int, emails, roles, org_slug
) -> None:
    """
    Adding members to a cohort is org-staff work, with one exception: a learner
    following an invite link enrols themselves, and is by definition not yet a
    member and not staff.

    The self-join path is deliberately narrow - only the caller's own email,
    only as a learner, and only into a cohort belonging to the invite's org.
    """
    caller = _caller_id(request)
    caller_email = (getattr(request.state, "user_email", None) or "").strip().lower()

    is_self_join = (
        len(emails) == 1
        and len(roles) == 1
        and caller_email
        and emails[0].strip().lower() == caller_email
        and roles[0].strip().lower() == "learner"
    )

    if not is_self_join:
        await require_cohort_write(request, cohort_id)
        return

    # Without org_slug there is nothing tying the caller to this cohort, so the
    # self-join path would be an open door into any cohort.
    if org_slug is None:
        await _decide(request, False, "self-join requires org_slug")
        return

    org_id = await org_for_cohort(cohort_id)
    if org_id is None:
        await _decide(request, False, f"unknown cohort {cohort_id}")
        return

    slug_org_id = await org_id_for_slug(org_slug)
    if slug_org_id != org_id:
        await _decide(
            request, False, f"org_slug {org_slug} does not own cohort {cohort_id}"
        )
        return

    # A valid self-join: the caller is enrolling only themselves as a learner.
    logger.info(f"Cohort self-join: user={caller} cohort={cohort_id}")
