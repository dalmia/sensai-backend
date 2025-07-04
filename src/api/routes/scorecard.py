from fastapi import APIRouter
from typing import List
from api.db.task import (
    get_all_scorecards_for_org as get_all_scorecards_for_org_from_db,
    update_scorecard as update_scorecard_from_db,
    create_scorecard as create_scorecard_from_db,
)
from api.models import Scorecard, BaseScorecard, CreateScorecardRequest

router = APIRouter()


@router.get("/", response_model=List[Scorecard])
async def get_all_scorecards_for_org(org_id: int) -> List[Scorecard]:
    return await get_all_scorecards_for_org_from_db(org_id)


@router.put("/{scorecard_id}")
async def update_scorecard(scorecard_id: int, scorecard: BaseScorecard) -> Scorecard:
    return await update_scorecard_from_db(scorecard_id, scorecard)


@router.post("/", response_model=Scorecard)
async def create_scorecard(scorecard: CreateScorecardRequest) -> Scorecard:
    return await create_scorecard_from_db(scorecard.model_dump())
