from fastapi import APIRouter, Depends, Request
from typing import List
from api.db.task import (
    get_all_scorecards_for_org as get_all_scorecards_for_org_from_db,
    update_scorecard as update_scorecard_from_db,
    create_scorecard as create_scorecard_from_db,
)
from api.models import Scorecard, BaseScorecard, CreateScorecardRequest

from api.middleware.permissions import require_org_staff, require_scorecard_access

from api.middleware import permissions

router = APIRouter()


@router.get("/", response_model=List[Scorecard], dependencies=[Depends(require_org_staff)])
async def get_all_scorecards_for_org(org_id: int) -> List[Scorecard]:
    return await get_all_scorecards_for_org_from_db(org_id)


@router.put("/{scorecard_id}", dependencies=[Depends(require_scorecard_access)])
async def update_scorecard(scorecard_id: int, scorecard: BaseScorecard) -> Scorecard:
    return await update_scorecard_from_db(scorecard_id, scorecard)


@router.post("/", response_model=Scorecard)
async def create_scorecard(http_request: Request, scorecard: CreateScorecardRequest) -> Scorecard:
    await permissions.require_org_staff(http_request, scorecard.org_id)
    return await create_scorecard_from_db(scorecard.model_dump())
