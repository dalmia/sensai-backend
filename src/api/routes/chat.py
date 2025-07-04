from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict
from api.db.chat import (
    store_messages as store_messages_in_db,
    get_all_chat_history as get_all_chat_history_from_db,
    get_task_chat_history_for_user as get_task_chat_history_for_user_from_db,
    delete_all_chat_history as delete_all_chat_history_from_db,
)
from api.models import (
    ChatMessage,
    StoreMessagesRequest,
)

router = APIRouter()


@router.post("/", response_model=List[ChatMessage])
async def store_messages(request: StoreMessagesRequest) -> List[ChatMessage]:
    return await store_messages_in_db(
        messages=request.messages,
        user_id=request.user_id,
        question_id=request.question_id,
        is_complete=request.is_complete,
    )


@router.get("/", response_model=List[ChatMessage])
async def get_all_chat_history(org_id: int) -> List[ChatMessage]:
    return await get_all_chat_history_from_db(org_id)


@router.get("/user/{user_id}/task/{task_id}", response_model=List[ChatMessage])
async def get_user_chat_history_for_task(
    user_id: int, task_id: int
) -> List[ChatMessage]:
    return await get_task_chat_history_for_user_from_db(
        user_id=user_id, task_id=task_id
    )


@router.delete("/")
async def delete_all_chat_history():
    await delete_all_chat_history_from_db()
    return {"message": "All chat history deleted"}
