from fastapi import APIRouter, HTTPException, Query, Depends, Request
from typing import List, Dict
from api.db.chat import (
    store_messages as store_messages_in_db,
    get_task_chat_history_for_user as get_task_chat_history_for_user_from_db,
)
from api.models import (
    ChatMessage,
    StoreMessagesRequest,
)

from api.middleware.permissions import require_user_scope

from api.middleware import permissions

router = APIRouter()


@router.post("/", response_model=List[ChatMessage])
async def store_messages(http_request: Request, request: StoreMessagesRequest) -> List[ChatMessage]:
    await permissions.require_user_scope(http_request, request.user_id)
    return await store_messages_in_db(
        messages=request.messages,
        user_id=request.user_id,
        question_id=request.question_id,
        task_id=request.task_id,
        is_complete=request.is_complete,
    )


@router.get("/user/{user_id}/task/{task_id}", dependencies=[Depends(require_user_scope)], response_model=List[ChatMessage])
async def get_user_chat_history_for_task(
    user_id: int, task_id: int
) -> List[ChatMessage]:
    return await get_task_chat_history_for_user_from_db(
        user_id=user_id, task_id=task_id
    )
