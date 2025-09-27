from ast import List
import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, AsyncGenerator
import json
from copy import deepcopy
from pydantic import BaseModel, Field
from api.config import openai_plan_to_model_name
from api.models import (
    AIChatRequest,
    ChatResponseType,
    TaskType,
    QuestionType,
)
from api.llm import (
    run_llm_with_openai,
    stream_llm_with_openai,
)
from api.settings import settings
from api.db.task import (
    get_task_metadata,
    get_question,
    get_task,
    get_scorecard,
)
from api.db.chat import get_question_chat_history_for_user
from api.db.utils import construct_description_from_blocks
from api.utils.s3 import (
    download_file_from_s3_as_bytes,
    get_media_upload_s3_key_from_uuid,
)
from api.utils.audio import prepare_audio_input_for_ai
from langfuse import get_client, observe

router = APIRouter()

langfuse = get_client()


def convert_chat_history_to_prompt(chat_history: List[Dict]) -> str:
    role_to_label = {
        "user": "Student",
        "assistant": "AI",
    }
    return "\n".join(
        [
            f"<{role_to_label[message['role']]}>\n{message['content']}\n</{role_to_label[message['role']]}>"
            for message in chat_history
        ]
    )


def format_chat_history_with_audio(chat_history: List[Dict]) -> str:
    chat_history = deepcopy(chat_history)

    role_to_label = {
        "user": "Student",
        "assistant": "AI",
    }

    parts = []

    for message in chat_history:
        label = role_to_label[message["role"]]

        if isinstance(message["content"], list):
            for item in message["content"]:
                if item["type"] == "input_audio":
                    item.pop("input_audio")
                    item["content"] = "<audio_message>"

        parts.append(f"<{label}>\n{message['content']}\n</{label}>")

    return "\n".join(parts)


@observe(name="rewrite_query")
async def rewrite_query(
    chat_history: List[Dict],
    question_details: str,
    user_id: str = None,
    is_root_trace: bool = False,
):
    # rewrite query
    prompt = langfuse.get_prompt("rewrite-query", type="chat", label="production")

    messages = prompt.compile(
        chat_history=convert_chat_history_to_prompt(chat_history),
        reference_material=question_details,
    )

    model = openai_plan_to_model_name["text-mini"]

    class Output(BaseModel):
        rewritten_query: str = Field(
            description="The rewritten query/message of the student"
        )

    messages += chat_history

    pred = await run_llm_with_openai(
        model=model,
        messages=messages,
        response_model=Output,
        max_output_tokens=8192,
        langfuse_prompt=prompt,
    )

    llm_input = f"""`Chat History`:\n\n{convert_chat_history_to_prompt(chat_history)}\n\n`Reference Material`:\n\n{question_details}"""

    if is_root_trace:
        langfuse_update_fn = langfuse.update_current_trace
    else:
        langfuse_update_fn = langfuse.update_current_generation

    output = pred.rewritten_query
    langfuse_update_fn(
        input=llm_input,
        output=output,
        metadata={
            "prompt_version": prompt.version,
            "prompt_name": prompt.name,
            "input": llm_input,
            "output": output,
        },
    )

    if user_id is not None and is_root_trace:
        langfuse.update_current_trace(
            user_id=user_id,
        )

    return output


@observe(name="router")
async def get_model_for_task(
    chat_history: List[Dict],
    question_details: str,
    user_id: str = None,
    is_root_trace: bool = False,
):
    class Output(BaseModel):
        chain_of_thought: str = Field(
            description="The chain of thought process for the decision to use a reasoning model or a general-purpose model"
        )
        use_reasoning_model: bool = Field(
            description="Whether to use a reasoning model to evaluate the student's response"
        )

    prompt = langfuse.get_prompt("router", type="chat", label="production")

    messages = prompt.compile(
        task_details=question_details,
    )

    messages += chat_history

    router_output = await run_llm_with_openai(
        model=openai_plan_to_model_name["router"],
        messages=messages,
        response_model=Output,
        max_output_tokens=4096,
        langfuse_prompt=prompt,
    )

    use_reasoning_model = router_output.use_reasoning_model

    if use_reasoning_model:
        model = openai_plan_to_model_name["reasoning"]
    else:
        model = openai_plan_to_model_name["text"]

    llm_input = f"""`Chat History`:\n\n{convert_chat_history_to_prompt(chat_history)}\n\n`Task Details`:\n\n{question_details}"""

    if is_root_trace:
        langfuse_update_fn = langfuse.update_current_trace
    else:
        langfuse_update_fn = langfuse.update_current_generation

    langfuse_update_fn(
        input=llm_input,
        output=use_reasoning_model,
        metadata={
            "prompt_version": prompt.version,
            "prompt_name": prompt.name,
            "input": llm_input,
            "output": use_reasoning_model,
        },
    )

    if user_id is not None and is_root_trace:
        langfuse.update_current_trace(
            user_id=user_id,
        )

    return model


def get_user_audio_message_for_chat_history(uuid: str) -> List[Dict]:
    if settings.s3_folder_name:
        audio_data = download_file_from_s3_as_bytes(
            get_media_upload_s3_key_from_uuid(uuid, "wav")
        )
    else:
        with open(os.path.join(settings.local_upload_folder, f"{uuid}.wav"), "rb") as f:
            audio_data = f.read()

    return [
        {
            "type": "input_audio",
            "input_audio": {
                "data": prepare_audio_input_for_ai(audio_data),
                "format": "wav",
            },
        },
    ]


def get_ai_message_for_chat_history(ai_message: Dict) -> str:
    message = json.loads(ai_message)

    if "scorecard" not in message or not message["scorecard"]:
        return message["feedback"]

    scorecard_as_prompt = []
    for criterion in message["scorecard"]:
        row_as_prompt = ""
        row_as_prompt += f"""- **{criterion['category']}**\n"""
        if criterion["feedback"].get("correct"):
            row_as_prompt += (
                f"""  What worked well: {criterion['feedback']['correct']}\n"""
            )
        if criterion["feedback"].get("wrong"):
            row_as_prompt += (
                f"""  What needs improvement: {criterion['feedback']['wrong']}\n"""
            )
        row_as_prompt += f"""  Score: {criterion['score']}"""
        scorecard_as_prompt.append(row_as_prompt)

    scorecard_as_prompt = "\n".join(scorecard_as_prompt)
    return f"""Feedback:\n```\n{message['feedback']}\n```\n\nScorecard:\n```\n{scorecard_as_prompt}\n```"""


@router.post("/chat")
async def ai_response_for_question(request: AIChatRequest):
    # Define an async generator for streaming
    async def stream_response() -> AsyncGenerator[str, None]:
        with langfuse.start_as_current_span(
            name="ai_chat",
        ) as trace:
            metadata = {
                "task_id": request.task_id,
                "user_id": request.user_id,
                "user_email": request.user_email,
            }

            if request.task_type == TaskType.QUIZ:
                if request.question_id is None and request.question is None:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Question ID or question is required for {request.task_type} tasks",
                    )

                if request.question_id is not None and request.user_id is None:
                    raise HTTPException(
                        status_code=400,
                        detail="User ID is required when question ID is provided",
                    )

                if request.question and request.chat_history is None:
                    raise HTTPException(
                        status_code=400,
                        detail="Chat history is required when question is provided",
                    )
                if request.question_id is None:
                    session_id = f"quiz_{request.task_id}_preview_{request.user_id}"
                else:
                    session_id = f"quiz_{request.task_id}_{request.question_id}_{request.user_id}"
            else:
                if request.task_id is None:
                    raise HTTPException(
                        status_code=400,
                        detail="Task ID is required for learning material tasks",
                    )

                if request.chat_history is None:
                    raise HTTPException(
                        status_code=400,
                        detail="Chat history is required for learning material tasks",
                    )
                session_id = f"lm_{request.task_id}_{request.user_id}"

            task = await get_task(request.task_id)
            if not task:
                raise HTTPException(status_code=404, detail="Task not found")

            metadata["task_title"] = task["title"]

            new_user_message = [
                {
                    "role": "user",
                    "content": (
                        get_user_audio_message_for_chat_history(request.user_response)
                        if request.response_type == ChatResponseType.AUDIO
                        else request.user_response
                    ),
                }
            ]

            if request.task_type == TaskType.LEARNING_MATERIAL:
                if request.response_type == ChatResponseType.AUDIO:
                    raise HTTPException(
                        status_code=400,
                        detail="Audio response is not supported for learning material tasks",
                    )

                metadata["type"] = "learning_material"

                chat_history = request.chat_history

                chat_history = [
                    {"role": message["role"], "content": message["content"]}
                    for message in chat_history
                ]

                reference_material = construct_description_from_blocks(task["blocks"])

                rewritten_query = await rewrite_query(
                    chat_history + new_user_message, reference_material
                )

                # update the last user message with the rewritten query
                new_user_message[0]["content"] = rewritten_query

                question_details = (
                    f"<Reference Material>\n{reference_material}\n</Reference Material>"
                )
            else:
                metadata["type"] = "quiz"

                if request.question_id:
                    question = await get_question(request.question_id)
                    if not question:
                        raise HTTPException(
                            status_code=404, detail="Question not found"
                        )

                    metadata["question_id"] = request.question_id

                    chat_history = await get_question_chat_history_for_user(
                        request.question_id, request.user_id
                    )

                else:
                    question = request.question.model_dump()
                    chat_history = request.chat_history

                    question["scorecard"] = await get_scorecard(
                        question["scorecard_id"]
                    )
                    metadata["question_id"] = None

                chat_history = [
                    {"role": message["role"], "content": message["content"]}
                    for message in chat_history
                ]

                metadata["question_title"] = question["title"]
                metadata["question_type"] = question["type"]
                metadata["question_purpose"] = (
                    "practice" if question["response_type"] == "chat" else "exam"
                )
                metadata["question_input_type"] = question["input_type"]
                metadata["question_has_context"] = bool(question["context"])

                question_description = construct_description_from_blocks(
                    question["blocks"]
                )
                question_details = f"<Task>\n\n{question_description}\n\n</Task>"

            task_metadata = await get_task_metadata(request.task_id)
            if task_metadata:
                metadata.update(task_metadata)

            for message in chat_history:
                if message["role"] == "user":
                    if request.response_type == ChatResponseType.AUDIO:
                        message["content"] = get_user_audio_message_for_chat_history(
                            message["content"]
                        )
                else:
                    if request.task_type == TaskType.LEARNING_MATERIAL:
                        message["content"] = json.dumps(
                            {"feedback": message["content"]}
                        )

                    message["content"] = get_ai_message_for_chat_history(
                        message["content"]
                    )

            if request.task_type == TaskType.QUIZ:
                if question["type"] == QuestionType.OBJECTIVE:
                    answer_as_prompt = construct_description_from_blocks(
                        question["answer"]
                    )
                    question_details += f"\n\n<Reference Solution (never to be shared with the learner)>\n{answer_as_prompt}\n</Reference Solution>"
                else:
                    scoring_criteria_as_prompt = ""

                    for criterion in question["scorecard"]["criteria"]:
                        scoring_criteria_as_prompt += f"""- **{criterion['name']}** [min: {criterion['min_score']}, max: {criterion['max_score']}, pass: {criterion.get('pass_score', criterion['max_score'])}]: {criterion['description']}\n"""

                    question_details += f"\n\n<Scoring Criteria>\n{scoring_criteria_as_prompt}\n</Scoring Criteria>"

            chat_history = chat_history + new_user_message

            # router
            if request.response_type == ChatResponseType.AUDIO:
                model = openai_plan_to_model_name["audio"]
                openai_api_mode = "chat_completions"
            else:
                model = await get_model_for_task(chat_history, question_details)
                openai_api_mode = "responses"

            # response
            llm_input = f"""`Chat History`:\n\n{format_chat_history_with_audio(chat_history)}\n\n`Task Details`:\n\n{question_details}"""
            response_metadata = {
                "input": llm_input,
            }

            metadata.update(response_metadata)

            llm_output = ""
            if request.task_type == TaskType.QUIZ:
                if question["type"] == QuestionType.OBJECTIVE:

                    class Output(BaseModel):
                        analysis: str = Field(
                            description="A detailed analysis of the student's response"
                        )
                        feedback: str = Field(
                            description="Feedback on the student's response; add newline characters to the feedback to make it more readable where necessary"
                        )
                        is_correct: bool = Field(
                            description="Whether the student's response correctly solves the original task that the student is supposed to solve. For this to be true, the original task needs to be completely solved and not just partially solved. Giving the right answer to one step of the task does not count as solving the entire task."
                        )

                else:

                    class Feedback(BaseModel):
                        correct: Optional[str] = Field(
                            description="What worked well in the student's response for this category based on the scoring criteria"
                        )
                        wrong: Optional[str] = Field(
                            description="What needs improvement in the student's response for this category based on the scoring criteria"
                        )

                    class Row(BaseModel):
                        category: str = Field(
                            description="Category from the scoring criteria for which the feedback is being provided"
                        )
                        feedback: Feedback = Field(
                            description="Detailed feedback for the student's response for this category"
                        )
                        score: int = Field(
                            description="Score given within the min/max range for this category based on the student's response - the score given should be in alignment with the feedback provided"
                        )
                        max_score: int = Field(
                            description="Maximum score possible for this category as per the scoring criteria"
                        )
                        pass_score: int = Field(
                            description="Pass score possible for this category as per the scoring criteria"
                        )

                    class Output(BaseModel):
                        feedback: str = Field(
                            description="A single, comprehensive summary based on the scoring criteria"
                        )
                        scorecard: Optional[List[Row]] = Field(
                            description="List of rows with one row for each category from scoring criteria; only include this in the response if the student's response is an answer to the task"
                        )

            else:

                class Output(BaseModel):
                    response: str = Field(
                        description="Response to the student's query; add proper formatting to the response to make it more readable where necessary"
                    )

            if request.task_type == TaskType.QUIZ:
                knowledge_base = ""

                if question["context"]:
                    linked_learning_material_ids = question["context"][
                        "linkedMaterialIds"
                    ]
                    knowledge_blocks = question["context"]["blocks"]

                    if linked_learning_material_ids:
                        for id in linked_learning_material_ids:
                            task = await get_task(int(id))
                            if task:
                                knowledge_blocks += task["blocks"]

                    knowledge_base = construct_description_from_blocks(knowledge_blocks)

                    if knowledge_base:
                        question_details += (
                            f"\n\n<Knowledge Base>\n{knowledge_base}\n</Knowledge Base>"
                        )

                if question["type"] == QuestionType.OBJECTIVE:
                    prompt_name = "objective-question"
                else:
                    prompt_name = "subjective-question"

                prompt = langfuse.get_prompt(
                    prompt_name, type="chat", label="production"
                )
                messages = prompt.compile(
                    task_details=question_details,
                )
            else:
                prompt = langfuse.get_prompt(
                    "doubt_solving", type="chat", label="production"
                )
                messages = prompt.compile(
                    reference_material=question_details,
                )

            messages += chat_history

            with langfuse.start_as_current_observation(
                as_type="generation", name="response", prompt=prompt
            ) as observation:
                try:
                    async for chunk in stream_llm_with_openai(
                        model=model,
                        messages=messages,
                        response_model=Output,
                        max_output_tokens=8192,
                        api_mode=openai_api_mode,
                    ):
                        content = json.dumps(chunk.model_dump()) + "\n"
                        llm_output = chunk.model_dump()
                        yield content
                except Exception as e:
                    # Check if it's the specific AsyncStream aclose error
                    if str(e) == "'AsyncStream' object has no attribute 'aclose'":
                        # Silently end partial stream on this specific error
                        pass
                    else:
                        # Re-raise other exceptions
                        raise
                finally:
                    observation.update(
                        input=llm_input,
                        output=llm_output,
                        prompt=prompt,
                        metadata={
                            "prompt_version": prompt.version,
                            "prompt_name": prompt.name,
                            **response_metadata,
                        },
                    )

            metadata["output"] = llm_output
            trace.update_trace(
                user_id=str(request.user_id),
                session_id=session_id,
                metadata=metadata,
                input=llm_input,
                output=llm_output,
            )

    # Return a streaming response
    return StreamingResponse(
        stream_response(),
        media_type="application/x-ndjson",
    )
