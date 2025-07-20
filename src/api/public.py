from typing import Annotated, Dict, List, Optional, Tuple
from fastapi import FastAPI, Body, Header, HTTPException, Depends
from fastapi.responses import StreamingResponse
import json
from api.models import (
    PublicAPIChatMessage,
    CourseWithMilestonesAndTaskDetails,
    TaskType,
)
from api.bq.chat import (
    get_all_chat_history as get_all_chat_history_from_db,
)
from api.bq.course import (
    get_course as get_course_from_db,
    get_course_org_id,
)
from api.bq.task import get_task as get_task_from_db
from api.bq.org import get_org_id_from_api_key


app = FastAPI()


async def validate_api_key(api_key: str, org_id: int) -> None:
    """
    Validates if the provided API key is authorized to access data for the given organization ID.
    Raises an HTTP 403 exception if the API key is invalid or unauthorized.
    """
    try:
        # Get the org_id associated with the API key from the database
        key_org_id = await get_org_id_from_api_key(api_key)

        # If org_id is provided, check if it matches the org_id from the API key
        if not key_org_id or key_org_id != org_id:
            raise HTTPException(
                status_code=403,
                detail="Invalid API key",
            )
    except ValueError:
        raise HTTPException(status_code=403, detail="Invalid API key")


async def generate_chat_history_stream(org_id: int):
    """
    Generator function that yields chat history messages as JSON lines.
    Each line contains a single chat message in JSON format.
    """
    async for message in get_all_chat_history_from_db(org_id):
        # Convert the message to JSON and add a newline for streaming
        yield json.dumps(message) + "\n"


@app.get("/chat_history")
async def get_all_chat_history(
    org_id: int,
    api_key: str = Header(...),
) -> StreamingResponse:
    """
    Stream chat history messages one by one instead of loading all into memory.
    Returns JSONL format (JSON Lines) where each line is a chat message.
    """
    # Validate the API key for the given org_id
    await validate_api_key(api_key=api_key, org_id=org_id)

    return StreamingResponse(
        generate_chat_history_stream(org_id),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": "attachment; filename=chat_history.jsonl"},
    )


@app.get(
    "/course/{course_id}",
    response_model=CourseWithMilestonesAndTaskDetails,
)
async def get_tasks_for_course(
    course_id: int,
    api_key: str = Header(...),
) -> CourseWithMilestonesAndTaskDetails:
    try:
        # Get the org_id from the API key
        org_id = await get_org_id_from_api_key(api_key)
    except ValueError:
        raise HTTPException(status_code=403, detail="Invalid API key")

    try:
        course_org_id = await get_course_org_id(course_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Course not found")

    if org_id != course_org_id:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key",
        )

    # Validate the API key for the given org_id
    await validate_api_key(api_key=api_key, org_id=org_id)

    course = await get_course_from_db(course_id=course_id)

    for milestone in course["milestones"]:
        for task in milestone["tasks"]:
            task_details = await get_task_from_db(task["id"])

            if task["type"] == TaskType.LEARNING_MATERIAL:
                task["blocks"] = task_details["blocks"]
            else:
                task["questions"] = task_details["questions"]

    return course
