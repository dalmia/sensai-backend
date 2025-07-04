from fastapi import APIRouter, HTTPException
from typing import Optional

from api.db.code_draft import (
    upsert_user_code_draft as upsert_code_draft_in_db,
    get_user_code_draft as get_code_draft_from_db,
    delete_user_code_draft as delete_code_draft_in_db,
)
from api.models import SaveCodeDraftRequest, CodeDraft

router = APIRouter()


@router.post("/")
async def save_code_draft(request: SaveCodeDraftRequest):
    await upsert_code_draft_in_db(
        user_id=request.user_id,
        question_id=request.question_id,
        code=[lang_code.model_dump() for lang_code in request.code],
    )
    return {"success": True}


@router.get(
    "/user/{user_id}/question/{question_id}", response_model=Optional[CodeDraft]
)
async def get_code_draft(user_id: int, question_id: int):
    return await get_code_draft_from_db(user_id, question_id)


@router.delete("/user/{user_id}/question/{question_id}")
async def delete_code_draft(user_id: int, question_id: int):
    await delete_code_draft_in_db(user_id, question_id)
    return {"success": True}
