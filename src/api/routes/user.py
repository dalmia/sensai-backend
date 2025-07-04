# --- START OF FILE sensai-api/sensai_backend/routes/user_routes.py ---
from fastapi import APIRouter, HTTPException
from typing import List, Dict
from datetime import datetime
from api.db.user import (
    get_user_by_id as get_user_by_id_from_db,
    update_user as update_user_in_db,
    get_user_cohorts as get_user_cohorts_from_db,
    get_user_activity_for_year as get_user_activity_for_year_from_db,
    get_user_active_in_last_n_days as get_user_active_in_last_n_days_from_db,
    get_user_streak as get_user_streak_from_db,
    get_user_organizations,
    get_user_org_cohorts as get_user_org_cohorts_from_db,
)
from api.db.course import get_user_courses as get_user_courses_from_db
from api.db.cohort import is_user_in_cohort as is_user_in_cohort_from_db
from api.utils.db import get_new_db_connection
from api.models import UserCourse, UserCohort, GetUserStreakResponse

router = APIRouter()


@router.get("/{user_id}")
async def get_user_by_id(user_id: int) -> Dict:
    user = await get_user_by_id_from_db(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.put("/{user_id}")
async def update_user(
    user_id: int,
    first_name: str,
    middle_name: str,
    last_name: str,
    default_dp_color: str,
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        user = await update_user_in_db(
            cursor, user_id, first_name, middle_name, last_name, default_dp_color
        )
        await conn.commit()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    return user


@router.get("/{user_id}/cohorts")
async def get_user_cohorts(user_id: int) -> List[Dict]:
    return await get_user_cohorts_from_db(user_id)


@router.get("/{user_id}/activity/{year}")
async def get_user_activity_for_year(user_id: int, year: int) -> List[int]:
    return await get_user_activity_for_year_from_db(user_id, year)


@router.get("/{user_id}/active_days")
async def get_user_active_days(user_id: int, days: int, cohort_id: int) -> List[str]:
    return await get_user_active_in_last_n_days_from_db(user_id, days, cohort_id)


@router.get("/{user_id}/streak")
async def get_user_streak(user_id: int, cohort_id: int) -> GetUserStreakResponse:
    streak_days = await get_user_streak_from_db(user_id, cohort_id)

    streak_count = len(streak_days)

    # Get the user's activity for the last 3 days as we are displaying a week's activity
    # with the current day in the center
    active_days = await get_user_active_in_last_n_days_from_db(user_id, 3, cohort_id)

    return {
        "streak_count": streak_count,
        "active_days": active_days,
    }


@router.get("/{user_id}/cohort/{cohort_id}/present")
async def is_user_present_in_cohort(user_id: int, cohort_id: int) -> bool:
    return await is_user_in_cohort_from_db(user_id, cohort_id)


@router.get("/{user_id}/courses", response_model=List[UserCourse])
async def get_user_courses(user_id: int) -> List[UserCourse]:
    return await get_user_courses_from_db(user_id)


@router.get("/{user_id}/org/{org_id}/cohorts", response_model=List[UserCohort])
async def get_user_org_cohorts(user_id: int, org_id: int) -> List[UserCohort]:
    return await get_user_org_cohorts_from_db(user_id, org_id)


@router.get("/{user_id}/orgs")
async def get_user_orgs(user_id: int) -> List[Dict]:
    return await get_user_organizations(user_id)
