import os
import base64
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator, Optional, Dict
import json
from copy import deepcopy
from pydantic import BaseModel, Field, create_model
from api.config import openai_plan_to_model_name
from api.models import (
    AIChatRequest,
    ChatResponseType,
    TaskType,
    TaskInputType,
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
from api.db.chat import (
    get_question_chat_history_for_user,
    get_task_chat_history_for_user,
)
from api.db.utils import construct_description_from_blocks
from api.utils.s3 import (
    download_file_from_s3_as_bytes,
    get_media_upload_s3_key_from_uuid,
)
from api.utils.audio import prepare_audio_input_for_ai
from api.utils.file_analysis import extract_submission_file
from api.db.user import get_user_first_name
from langfuse import get_client, observe

router = APIRouter()

langfuse = get_client()

LANGFUSE_PROMPT_LABEL = settings.langfuse_tracing_environment


def convert_chat_history_to_prompt(chat_history: list[dict]) -> str:
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


def get_latest_file_uuid_from_chat_history(chat_history: list[dict]) -> Optional[str]:
    """
    Extract the latest file_uuid from chat history.
    """
    if not chat_history:
        return None

    # Iterate through chat history in reverse to find the latest file submission
    for message in reversed(chat_history):
        if message.get("role") == "user":
            content = message.get("content", "")
            if isinstance(content, str):
                try:
                    # Try to parse as JSON to check if it's a file submission
                    file_data = json.loads(content)
                    if isinstance(file_data, dict) and "file_uuid" in file_data:
                        return file_data["file_uuid"]
                except (json.JSONDecodeError, TypeError):
                    # Not a JSON string, continue
                    continue
    return None


def format_chat_history_with_audio(chat_history: list[dict]) -> str:
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

        if message["role"] == "user":
            content = message["content"]
            parts.append(f"**{label}**\n\n```\n{content}\n```\n\n")
        else:
            # Wherever there is a single \n followed by content before and either nothing after or non \n after, replace that \n with 2 \n\n
            import re

            # Replace a single newline between content with double newlines, except when already double or more
            def single_newline_to_double(text):
                # This regex matches single \n (not preceded nor followed by \n) with non-\n after, or end of string
                #  - positive lookbehind: previous char is not \n
                #  - match \n
                #  - negative lookahead: next char is not \n
                #  - next char is not \n or is end of string
                return re.sub(r"(?<!\n)\n(?!\n)", "\n\n", text)

            content_str = single_newline_to_double(
                message["content"].replace("```", "\n")
            )
            parts.append(f"**{label}**\n\n{content_str}\n\n")

    return "\n\n---\n\n".join(parts)


@observe(name="rewrite_query")
async def rewrite_query(
    chat_history: list[dict],
    question_details: str,
    user_id: str = None,
    is_root_trace: bool = False,
):
    # rewrite query
    prompt = langfuse.get_prompt(
        "rewrite-query", type="chat", label=LANGFUSE_PROMPT_LABEL
    )

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

    llm_input = f"# Chat History\n\n{convert_chat_history_to_prompt(chat_history)}\n\n# Reference Material\n\n{question_details}"

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
    chat_history: list[dict],
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

    prompt = langfuse.get_prompt("router", type="chat", label=LANGFUSE_PROMPT_LABEL)

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

    llm_input = f"# Chat History\n\n{convert_chat_history_to_prompt(chat_history)}\n\n# Task Details\n\n{question_details}"

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


def get_user_audio_message_for_chat_history(uuid: str) -> list[dict]:
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


def get_user_file_message_for_chat_history(file_uuid: str, filename: str, file_type: str = "pdf") -> list[dict]:
    try:
        if settings.s3_folder_name:
            file_data = download_file_from_s3_as_bytes(
                get_media_upload_s3_key_from_uuid(file_uuid, file_type)
            )
        else:
            with open(os.path.join(settings.local_upload_folder, f"{file_uuid}.{file_type}"), "rb") as f:
                file_data = f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {file_uuid}")
    except Exception as e:
        error_msg = str(e)
        if "NoSuchKey" in error_msg or "404" in error_msg:
            raise HTTPException(status_code=404, detail=f"File not found: {file_uuid}")
        raise HTTPException(status_code=500, detail=f"Error reading file: {error_msg}")

    base64_data = base64.b64encode(file_data).decode("utf-8")
    data_url = f"data:application/{file_type};base64,{base64_data}"

    return [
        {
            "type": "file",
            "file": {
                "filename": filename,
                "file_data": data_url,
            },
        },
    ]


def get_processed_message_content(message: dict) -> any:
    if message.get("role") != "user":
        return message.get("content")

    content = message.get("content")
    response_type = message.get("response_type")

    if response_type == ChatResponseType.FILE:
        file_data = json.loads(content)
        return get_user_file_message_for_chat_history(file_data["file_uuid"], file_data["filename"])
    elif response_type == ChatResponseType.AUDIO:
        return get_user_audio_message_for_chat_history(content)

    return content


def format_ai_scorecard_report(scorecard: list[dict]) -> str:
    scorecard_as_prompt = []
    for criterion in scorecard:
        row_as_prompt = []
        row_as_prompt.append(f"""**{criterion['category']}**: {criterion['score']}""")

        if criterion["feedback"].get("correct"):
            row_as_prompt.append(
                f"""What worked well: {criterion['feedback']['correct']}"""
            )
        if criterion["feedback"].get("wrong"):
            row_as_prompt.append(
                f"""What needs improvement: {criterion['feedback']['wrong']}"""
            )

        row_as_prompt = "\n".join(row_as_prompt)
        scorecard_as_prompt.append(row_as_prompt)

    return "\n\n".join(scorecard_as_prompt)


def convert_scorecard_to_prompt(scorecard: list[dict]) -> str:
    scoring_criteria_as_prompt = []

    for index, criterion in enumerate(scorecard["criteria"]):
        criterion_name = criterion["name"].replace('"', "")
        scoring_criteria_as_prompt.append(
            f"""Criterion {index + 1}:\n**Name**: **{criterion_name}** [min_score: {criterion['min_score']}, max_score: {criterion['max_score']}, pass_score: {criterion.get('pass_score', criterion['max_score'])}]\n\n{criterion['description']}"""
        )

    return "\n\n".join(scoring_criteria_as_prompt)


def build_evaluation_context(evaluation_criteria: dict) -> str:
    """
    Build evaluation context string with overall scoring info.
    """
    evaluation_context = "**Overall Assignment Scoring:**\n"
    evaluation_context += (
        f"- Minimum Score: {evaluation_criteria.get('min_score', 0)}\n"
    )
    evaluation_context += (
        f"- Maximum Score: {evaluation_criteria.get('max_score', 100)}\n"
    )
    evaluation_context += f"- Pass Score: {evaluation_criteria.get('pass_score', 60)}\n"

    return evaluation_context


async def build_knowledge_base_from_context(context: dict) -> str:
    """
    Build knowledge base description from a context dict that may contain
    blocks and linked material IDs.
    """
    if not context or not context.get("blocks"):
        return ""

    knowledge_blocks = context["blocks"]

    # Add linked learning materials
    linked_ids = context.get("linkedMaterialIds") or []
    for material_id in linked_ids:
        material_task = await get_task(int(material_id))
        if material_task:
            knowledge_blocks += material_task["blocks"]

    return construct_description_from_blocks(knowledge_blocks)


def get_ai_message_for_chat_history(ai_message: dict) -> str:
    message = json.loads(ai_message)

    if "scorecard" not in message or not message["scorecard"]:
        return message["feedback"]

    scorecard_as_prompt = format_ai_scorecard_report(message["scorecard"])

    return f"""Feedback:\n```\n{message['feedback']}\n```\n\nScorecard:\n```\n{scorecard_as_prompt}\n```"""


async def get_user_details_for_prompt(user_id: str) -> str:
    user_first_name = await get_user_first_name(user_id)

    if not user_first_name:
        return ""

    return f"Name: {user_first_name}"


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

            user_details = await get_user_details_for_prompt(request.user_id)

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

            # Parse content based on response type
            if request.response_type == ChatResponseType.FILE:
                if not isinstance(request.user_response, dict) or "file_uuid" not in request.user_response or "filename" not in request.user_response:
                    raise HTTPException(
                        status_code=400,
                        detail="File response requires dict with file_uuid and filename"
                    )
                file_content = get_user_file_message_for_chat_history(
                    request.user_response["file_uuid"], request.user_response["filename"]
                )
            elif request.response_type == ChatResponseType.AUDIO:
                file_content = get_user_audio_message_for_chat_history(request.user_response)
            else:
                file_content = request.user_response

            new_user_message = [
                {
                    "role": "user",
                    "content": file_content,
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

                question_details = f"**Reference Material**\n\n{reference_material}\n\n"
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
                    {
                        "role": message["role"],
                        "content": get_processed_message_content(message)
                    }
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
                question_details = f"**Task**\n\n{question_description}\n\n"

            task_metadata = await get_task_metadata(request.task_id)
            if task_metadata:
                metadata.update(task_metadata)

            for message in chat_history:
                if message["role"] == "assistant":
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
                    question_details += f"---\n\n**Reference Solution (never to be shared with the learner)**\n\n{answer_as_prompt}\n\n"
                else:
                    scorecard_as_prompt = convert_scorecard_to_prompt(
                        question["scorecard"]
                    )
                    question_details += (
                        f"---\n\n**Scoring Criteria**\n\n{scorecard_as_prompt}\n\n"
                    )

                # Add file evaluation context if input type is FILE
                if question.get("input_type") == TaskInputType.FILE:
                    question_details += "---\n\n**Note**: The student has submitted a PDF file for evaluation. Evaluate the file content against the task requirements and scoring criteria.\n\n"

            chat_history = chat_history + new_user_message

            # router
            if request.response_type == ChatResponseType.AUDIO:
                model = openai_plan_to_model_name["audio"]
                openai_api_mode = "chat_completions"
            elif request.response_type == ChatResponseType.FILE:
                # Use text model for PDF files (supports multimodal content)
                model = openai_plan_to_model_name["text"]
                openai_api_mode = "chat_completions"
            else:
                model = await get_model_for_task(chat_history, question_details)
                openai_api_mode = "responses"

            # response
            llm_input = f"""# Chat History\n\n{format_chat_history_with_audio(chat_history)}\n\n# Task Details\n\n{question_details}"""
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
                            description="Feedback on the student's response; add newline characters to the feedback to make it more readable where necessary; address the student by name if their name has been provided."
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
                        feedback: Feedback = Field(
                            description="Detailed feedback for the student's response for this category"
                        )
                        score: float = Field(
                            description="Score given within the min/max range for this category based on the student's response - the score given should be in alignment with the feedback provided"
                        )
                        max_score: float = Field(
                            description="Maximum score possible for this category as per the scoring criteria"
                        )
                        pass_score: float = Field(
                            description="Pass score possible for this category as per the scoring criteria"
                        )

                    def make_scorecard_model(fields: list[str]) -> type[BaseModel]:
                        """
                        Dynamically create a Pydantic model with fields from a list of strings.
                        Each field defaults to `str`, but you can change that if needed.
                        """
                        # build dictionary for create_model
                        field_definitions: dict[str, tuple[type, any]] = {
                            field: (Row, ...) for field in fields
                        }
                        # ... means "required"
                        return create_model("Scorecard", **field_definitions)

                    Scorecard = make_scorecard_model(
                        [
                            criterion["name"].replace('"', "")
                            for criterion in question["scorecard"]["criteria"]
                        ]
                    )

                    class Output(BaseModel):
                        chain_of_thought: str = Field(
                            description="Concise analysis of the student's response and what the scorecard should be."
                        )
                        feedback: str = Field(
                            description="A single, comprehensive summary based on the scoring criteria; address the student by name if their name has been provided."
                        )
                        scorecard: Optional[Scorecard] = Field(
                            description="Score and feedback for each criterion from the scoring criteria; only include this in the response if the student's response is a valid response to the task"
                        )

            else:

                class Output(BaseModel):
                    response: str = Field(
                        description="Response to the student's query; add proper formatting to the response to make it more readable where necessary; address the student by name if their name has been provided."
                    )

            if request.task_type == TaskType.QUIZ:
                knowledge_base = await build_knowledge_base_from_context(
                    question.get("context")
                )

                if knowledge_base:
                    question_details += (
                        f"---\n\n**Knowledge Base**\n\n{knowledge_base}\n\n"
                    )

                if question["type"] == QuestionType.OBJECTIVE:
                    prompt_name = "objective-question"
                else:
                    prompt_name = "subjective-question"

                prompt = langfuse.get_prompt(
                    prompt_name, type="chat", label=LANGFUSE_PROMPT_LABEL
                )
                messages = prompt.compile(
                    task_details=question_details,
                    user_details=user_details,
                )
            else:
                prompt = langfuse.get_prompt(
                    "doubt_solving", type="chat", label=LANGFUSE_PROMPT_LABEL
                )
                messages = prompt.compile(
                    reference_material=question_details,
                    user_details=user_details,
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


@router.post("/assignment")
async def ai_response_for_assignment(request: AIChatRequest):
    # Define an async generator for streaming
    async def stream_response() -> AsyncGenerator[str, None]:
        with langfuse.start_as_current_span(
            name="assignment_evaluation",
        ) as trace:
            metadata = {
                "task_id": request.task_id,
                "user_id": request.user_id,
                "user_email": request.user_email,
            }

            user_details = await get_user_details_for_prompt(request.user_id)

            # Validate required fields for assignment
            if request.task_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="Task ID is required for assignment tasks",
                )

            # Get assignment data
            task = await get_task(request.task_id)
            if not task:
                raise HTTPException(status_code=404, detail="Task not found")

            if task["type"] != TaskType.ASSIGNMENT:
                raise HTTPException(status_code=400, detail="Task is not an assignment")

            metadata["task_title"] = task["title"]

            assignment = task["assignment"]
            problem_blocks = assignment["blocks"]
            evaluation_criteria = assignment["evaluation_criteria"]

            if not evaluation_criteria:
                raise HTTPException(
                    status_code=400,
                    detail="Assignment is missing evaluation criteria",
                )

            if not evaluation_criteria.get("scorecard_id"):
                raise HTTPException(
                    status_code=400,
                    detail="Assignment evaluation criteria is missing scorecard_id",
                )

            context = assignment.get("context")

            # Get scorecard for evaluation
            scorecard = await get_scorecard(evaluation_criteria["scorecard_id"])

            if not scorecard:
                raise HTTPException(
                    status_code=400,
                    detail="Scorecard not found for assignment evaluation criteria",
                )

            # Get chat history for this assignment
            # Use request.chat_history if provided (for preview mode), otherwise fetch from database
            if request.chat_history:
                chat_history = request.chat_history

                if chat_history is None:
                    # For first-time submissions (file uploads), chat_history might be empty
                    # We'll initialize it as empty if not provided
                    chat_history = []
            else:
                chat_history = await get_task_chat_history_for_user(
                    request.task_id, request.user_id
                )

            # Convert chat history to the format expected by AI
            formatted_chat_history = [
                {"role": message["role"], "content": message["content"]}
                for message in chat_history
            ]

            # Add new user message
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

            # Build problem statement from blocks
            problem_statement = construct_description_from_blocks(problem_blocks)

            # Build full chat history
            if request.response_type == ChatResponseType.FILE:
                # For file uploads, include only the new user message with file_uuid
                # This branch is triggered when a learner submits an assignment as a file
                full_chat_history = new_user_message
            else:
                # This branch is triggered when a learner answers questions about the assignment with text or audio
                full_chat_history = formatted_chat_history + new_user_message

            # Handle file submission - extract code
            submission_data = None

            if request.response_type == ChatResponseType.FILE:
                submission_data = extract_submission_file(request.user_response)
            else:
                # Not a file upload, check chat history for latest file submission
                latest_file_uuid = get_latest_file_uuid_from_chat_history(
                    full_chat_history
                )

                if latest_file_uuid:
                    submission_data = extract_submission_file(latest_file_uuid)

            # Build evaluation context
            evaluation_context = build_evaluation_context(evaluation_criteria)

            # Build Key Areas section from scorecard
            key_areas_section = f"\n\n<Key Areas>\n\n{convert_scorecard_to_prompt(scorecard)}\n\n</Key Areas>"

            # Build context with linked materials if available
            knowledge_base = await build_knowledge_base_from_context(context)

            # Build the complete assignment context
            assignment_details = (
                f"<Problem Statement>\n\n{problem_statement}\n\n</Problem Statement>"
            )

            # Add Key Areas from scorecard
            if key_areas_section:
                assignment_details += key_areas_section

            if evaluation_context:
                assignment_details += f"\n\n<Evaluation Criteria>\n\n{evaluation_context}\n\n</Evaluation Criteria>"

            if knowledge_base:
                assignment_details += (
                    f"\n\n<Knowledge Base>\n\n{knowledge_base}\n\n</Knowledge Base>"
                )

            # Add submission data for file uploads
            if submission_data:
                assignment_details += f"\n\n<Student Submission Data>"
                assignment_details += f"\n\n**File Contents:**\n"
                for filename, content in submission_data["file_contents"].items():
                    assignment_details += (
                        f"\n--- {filename} ---\n{content}\n--- End of {filename} ---\n"
                    )
                assignment_details += f"\n\n</Student Submission Data>"

            # Process chat history for audio content if needed
            if request.response_type == ChatResponseType.AUDIO:
                for message in full_chat_history:
                    if (
                        message["role"] == "user"
                        and message.get("response_type") == ChatResponseType.AUDIO
                    ):
                        message["content"] = get_user_audio_message_for_chat_history(
                            message["content"]
                        )

            # Determine model based on input type
            if request.response_type == ChatResponseType.AUDIO:
                model = openai_plan_to_model_name["audio"]
                openai_api_mode = "chat_completions"
            else:
                # For assignments, use reasoning model for better evaluation
                model = openai_plan_to_model_name["reasoning"]
                openai_api_mode = "responses"

            # Enhanced feedback structure for key area scores
            class Feedback(BaseModel):
                correct: Optional[str] = Field(
                    description="What worked well in the student's response for this category based on the scoring criteria"
                )
                wrong: Optional[str] = Field(
                    description="What needs improvement in the student's response for this category based on the scoring criteria"
                )

            class KeyAreaScore(BaseModel):
                feedback: Feedback = Field(
                    description="Detailed feedback for the student's response for this category"
                )
                score: float = Field(
                    description="Score given within the min/max range for this category based on the student's response - the score given should be in alignment with the feedback provided"
                )
                max_score: float = Field(
                    description="Maximum score possible for this category as per the scoring criteria"
                )
                pass_score: float = Field(
                    description="Pass score possible for this category as per the scoring criteria"
                )

            # Base output model for all phases
            class Output(BaseModel):
                chain_of_thought: str = Field(
                    description="Concise analysis of the student's response to the question asked and what the evaluation result should be"
                )
                feedback: Optional[str] = Field(
                    description="A single, comprehensive summary based on the scoring criteria; address the student by name if their name has been provided.",
                )
                evaluation_status: Optional[str] = Field(
                    description="The status of the evaluation; can be `in_progress`, `needs_resubmission`, or `completed`",
                )
                key_area_scores: Optional[Dict[str, KeyAreaScore]] = Field(
                    description="Completed key area scores with detailed feedback",
                    default={},
                )
                current_key_area: Optional[str] = Field(
                    description="Current key area being evaluated"
                )

            # Output model for file submissions that includes project score
            class FileSubmissionOutput(Output):
                chain_of_thought: str = Field(
                    description="Concise analysis of the student's submission to the assignment and what the evaluation result should be"
                )
                assignment_score: Optional[float] = Field(
                    description="Assignment score assigned when evaluating initial file submission"
                )

            # Get Langfuse prompt for assignment evaluation
            prompt = langfuse.get_prompt(
                "assignment", type="chat", label=LANGFUSE_PROMPT_LABEL
            )

            messages = prompt.compile(
                assignment_details=assignment_details,
                user_details=user_details,
            )

            messages += full_chat_history

            # Build input for metadata
            llm_input = f"""`Assignment Details`:\n\n{assignment_details}\n\n`Chat History`:\n\n{format_chat_history_with_audio(full_chat_history)}"""
            response_metadata = {
                "input": llm_input,
            }

            metadata.update(response_metadata)

            llm_output = ""

            # Use FileSubmissionOutput for file submissions, otherwise use base Output
            response_model = (
                FileSubmissionOutput
                if request.response_type == ChatResponseType.FILE
                else Output
            )

            # Process streaming response with Langfuse observation
            with langfuse.start_as_current_observation(
                as_type="generation", name="response", prompt=prompt
            ) as observation:
                try:
                    async for chunk in stream_llm_with_openai(
                        model=model,
                        messages=messages,
                        response_model=response_model,
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

            session_id = f"assignment_{request.task_id}_{request.user_id}"
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
