from fastapi import APIRouter
from api.db.org import (
    is_user_hva_learner as is_user_hva_learner_from_db,
    get_hva_org_id as get_hva_org_id_from_db,
)


router = APIRouter()


@router.get("/is_user_hva_learner")
async def is_user_hva_learner(user_id: int):
    return await is_user_hva_learner_from_db(user_id)


@router.get("/org_id")
async def get_hva_org_id():
    return await get_hva_org_id_from_db()
