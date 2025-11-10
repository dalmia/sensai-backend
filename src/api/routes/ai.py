import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator, Optional, List, Dict
import json
from copy import deepcopy
from pydantic import BaseModel, Field, create_model
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
from api.db.chat import get_question_chat_history_for_user, get_task_chat_history_for_user
from api.db.utils import construct_description_from_blocks
from api.utils.s3 import (
    download_file_from_s3_as_bytes,
    get_media_upload_s3_key_from_uuid,
)
from api.utils.audio import prepare_audio_input_for_ai
from api.utils.file_analysis import extract_submission_file
from langfuse import get_client, observe

router = APIRouter()

langfuse = get_client()


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
        scoring_criteria_as_prompt.append(
            f"""Criterion {index + 1}:\n**Name**: **{criterion['name']}** [min_score: {criterion['min_score']}, max_score: {criterion['max_score']}, pass_score: {criterion.get('pass_score', criterion['max_score'])}]\n\n{criterion['description']}"""
        )

    return "\n\n".join(scoring_criteria_as_prompt)


def get_ai_message_for_chat_history(ai_message: dict) -> str:
    message = json.loads(ai_message)

    if "scorecard" not in message or not message["scorecard"]:
        return message["feedback"]

    scorecard_as_prompt = format_ai_scorecard_report(message["scorecard"])

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
                question_details = f"**Task**\n\n{question_description}\n\n"

            task_metadata = await get_task_metadata(request.task_id)
            if task_metadata:
                metadata.update(task_metadata)

            for message in chat_history:
                if message["role"] == "user":
                    if request.response_type == ChatResponseType.AUDIO and message.get("response_type") == ChatResponseType.AUDIO:
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
                    question_details += f"---\n\n**Reference Solution (never to be shared with the learner)**\n\n{answer_as_prompt}\n\n"
                else:
                    scorecard_as_prompt = convert_scorecard_to_prompt(
                        question["scorecard"]
                    )
                    question_details += (
                        f"---\n\n**Scoring Criteria**\n\n{scorecard_as_prompt}\n\n"
                    )

            chat_history = chat_history + new_user_message

            # router
            if request.response_type == ChatResponseType.AUDIO:
                model = openai_plan_to_model_name["audio"]
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
                            criterion["name"]
                            for criterion in question["scorecard"]["criteria"]
                        ]
                    )

                    class Output(BaseModel):
                        feedback: str = Field(
                            description="A single, comprehensive summary based on the scoring criteria"
                        )
                        scorecard: Optional[Scorecard] = Field(
                            description="score and feedback for each criterion from the scoring criteria; only include this in the response if the student's response is a valid response to the task"
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
                            f"---\n\n**Knowledge Base**\n\n{knowledge_base}\n\n"
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

            # Validate required fields for assignment
            if request.task_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="Task ID is required for assignment tasks",
                )

            # For first-time submissions (file uploads), chat_history might be empty
            # We'll initialize it as empty if not provided
            if request.chat_history is None:
                request.chat_history = []

            # Get assignment data
            task = await get_task(request.task_id)
            if not task:
                raise HTTPException(status_code=404, detail="Task not found")

            if task["type"] != TaskType.ASSIGNMENT:
                raise HTTPException(
                    status_code=400,
                    detail="Task is not an assignment"
                )

            metadata["task_title"] = task["title"]
            metadata["task_type"] = "assignment"

            # Get assignment details
            assignment_data = task
            problem_blocks = assignment_data["blocks"]
            evaluation_criteria = assignment_data["evaluation_criteria"]
            context = assignment_data.get("context")
            input_type = assignment_data.get("input_type", "text")

            # Get scorecard if evaluation_criteria has scorecard_id
            scorecard = None
            if evaluation_criteria and evaluation_criteria.get("scorecard_id"):
                scorecard = await get_scorecard(evaluation_criteria["scorecard_id"])

            # Get chat history for this assignment
            # Use request.chat_history if provided (for testing), otherwise fetch from database
            if request.chat_history:
                chat_history = request.chat_history
            else:
                try:
                    chat_history = await get_task_chat_history_for_user(
                        request.task_id, request.user_id
                    )
                except Exception:
                    # If no chat history exists yet, start with empty list
                    chat_history = []

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

            # Handle file submission - extract code
            submission_data = None
            if request.response_type == ChatResponseType.FILE:
                try:
                    submission_data = extract_submission_file(request.user_response)
                except Exception as e:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Error extracting submission file: {str(e)}"
                    )

            # Build evaluation context with key areas from scorecard
            evaluation_context = ""
            key_areas = []
            
            if scorecard:
                evaluation_context = convert_scorecard_to_prompt(scorecard)
                
                # Extract key areas from scorecard criteria
                for criterion in scorecard["criteria"]:
                    key_areas.append({
                        "name": criterion["name"],
                        "description": criterion["description"],
                        "min_score": criterion["min_score"],
                        "max_score": criterion["max_score"],
                        "pass_score": criterion.get("pass_score", criterion["max_score"])
                    })
            
            # Add evaluation criteria scores
            if evaluation_criteria:
                evaluation_context += f"\n\n**Overall Project Scoring:**\n"
                evaluation_context += f"- Minimum Score: {evaluation_criteria.get('min_score', 0)}\n"
                evaluation_context += f"- Maximum Score: {evaluation_criteria.get('max_score', 100)}\n"
                evaluation_context += f"- Pass Score: {evaluation_criteria.get('pass_score', 60)}\n"

            # Build context with linked materials if available
            knowledge_base = ""
            if context and context.get("blocks"):
                knowledge_blocks = context["blocks"]
                
                # Add linked learning materials
                if context.get("linkedMaterialIds"):
                    for material_id in context["linkedMaterialIds"]:
                        material_task = await get_task(int(material_id))
                        if material_task:
                            knowledge_blocks += material_task["blocks"]
                
                knowledge_base = construct_description_from_blocks(knowledge_blocks)

            # Build the complete assignment context
            assignment_details = f"<Problem Statement>\n{problem_statement}\n</Problem Statement>"
            
            # Add Key Areas from scorecard
            if key_areas:
                assignment_details += f"\n\n<Key Areas>\n"
                for i, area in enumerate(key_areas, 1):
                    assignment_details += f"{i}. **{area['name']}**\n"
                    assignment_details += f"   Description: {area['description']}\n"
                    assignment_details += f"   Scoring: {area['min_score']}-{area['max_score']} (Pass: {area['pass_score']})\n\n"
                assignment_details += f"</Key Areas>"
            
            if evaluation_context:
                assignment_details += f"\n\n<Evaluation Criteria>\n{evaluation_context}\n</Evaluation Criteria>"
            
            if knowledge_base:
                assignment_details += f"\n\n<Knowledge Base>\n{knowledge_base}\n</Knowledge Base>"

            # Add submission data for file uploads
            if submission_data:
                assignment_details += f"\n\n<Student Submission Data>\n"
                assignment_details += f"**Files Extracted:** {submission_data['extracted_files_count']}\n"
                assignment_details += f"\n**File Contents:**\n"
                for filename, content in submission_data['file_contents'].items():
                    assignment_details += f"\n--- {filename} ---\n{content}\n--- End of {filename} ---\n"
                assignment_details += f"</Student Submission Data>"

            # Build full chat history
            if request.response_type == ChatResponseType.FILE:
                # For file uploads, include only the new user message with file_uuid
                full_chat_history = new_user_message
            else:
                full_chat_history = formatted_chat_history + new_user_message

            # Process chat history for audio content if needed
            if request.response_type == ChatResponseType.AUDIO:
                for message in full_chat_history:
                    if message["role"] == "user" and message.get("response_type") == ChatResponseType.AUDIO:
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

            # Dynamic output model based on evaluation phase
            class Output(BaseModel):
                feedback: Optional[str] = Field(description="Current feedback and response", default="")
                evaluation_status: Optional[str] = Field(description="in_progress, needs_resubmission, or completed", default="in_progress")
                key_area_scores: Optional[Dict[str, KeyAreaScore]] = Field(description="Completed key area scores with detailed feedback", default={})
                current_key_area: Optional[str] = Field(description="Current key area being evaluated")

            # Get Langfuse prompt for assignment evaluation
            prompt = langfuse.get_prompt("assignment", type="chat", label="production")
            
            # Extract evaluation criteria values for dynamic prompt
            min_score = evaluation_criteria.get('min_score', 0) if evaluation_criteria else 0
            max_score = evaluation_criteria.get('max_score', 100) if evaluation_criteria else 100
            pass_score = evaluation_criteria.get('pass_score', 60) if evaluation_criteria else 60
            
            # Compile the prompt with assignment details and evaluation criteria
            if request.response_type == ChatResponseType.AUDIO:
                # For audio responses, build messages with compiled prompt and audio content
                messages = prompt.compile(
                    assignment_details=assignment_details,
                    min_score=min_score,
                    max_score=max_score,
                    pass_score=pass_score
                )
                
                # Replace placeholders if they exist
                for msg in messages:
                    if isinstance(msg.get("content"), str):
                        msg["content"] = msg["content"].replace("{assignment_details}", assignment_details)
                        msg["content"] = msg["content"].replace("{min_score}", str(min_score))
                        msg["content"] = msg["content"].replace("{max_score}", str(max_score))
                        msg["content"] = msg["content"].replace("{pass_score}", str(pass_score))
                
                # Add chat history with audio content
                for message in full_chat_history:
                    messages.append({
                        "role": message["role"],
                        "content": message["content"]
                    })
            else:
                # For text responses, compile prompt with assignment details and add chat history
                messages = prompt.compile(
                    assignment_details=assignment_details,
                    min_score=min_score,
                    max_score=max_score,
                    pass_score=pass_score
                )
                
                # Replace placeholders if they exist
                for msg in messages:
                    if isinstance(msg.get("content"), str):
                        msg["content"] = msg["content"].replace("{assignment_details}", assignment_details)
                        msg["content"] = msg["content"].replace("{min_score}", str(min_score))
                        msg["content"] = msg["content"].replace("{max_score}", str(max_score))
                        msg["content"] = msg["content"].replace("{pass_score}", str(pass_score))
                
                messages += full_chat_history

            # Build input for metadata
            llm_input = f"""`Assignment Details`:\n\n{assignment_details}\n\n`Chat History`:\n\n{format_chat_history_with_audio(full_chat_history)}"""
            response_metadata = {
                "input": llm_input,
            }
            
            metadata.update(response_metadata)

            llm_output = ""
            
            # Process streaming response with Langfuse observation
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
