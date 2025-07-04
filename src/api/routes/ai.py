from ast import List
import os
import tempfile
import random
from collections import defaultdict
import asyncio
from fastapi import APIRouter, HTTPException, Body, BackgroundTasks
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Literal, AsyncGenerator
import json
import instructor
import openai
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticOutputParser
from api.config import openai_plan_to_model_name
from api.models import (
    TaskAIResponseType,
    AIChatRequest,
    ChatResponseType,
    TaskType,
    GenerateCourseStructureRequest,
    GenerateCourseJobStatus,
    GenerateTaskJobStatus,
    QuestionType,
)
from api.llm import run_llm_with_instructor, stream_llm_with_instructor
from api.settings import settings
from api.utils.logging import logger
from api.utils.concurrency import async_batch_gather
from api.websockets import get_manager
from api.db.task import (
    get_task_metadata,
    get_question,
    get_task,
    get_scorecard,
    create_draft_task_for_course,
    store_task_generation_request,
    update_task_generation_job_status,
    get_course_task_generation_jobs_status,
    add_generated_learning_material,
    add_generated_quiz,
    get_all_pending_task_generation_jobs,
)
from api.db.course import (
    store_course_generation_request,
    get_course_generation_job_details,
    update_course_generation_job_status_and_details,
    update_course_generation_job_status,
    get_all_pending_course_structure_generation_jobs,
    add_milestone_to_course,
)
from api.db.chat import get_question_chat_history_for_user
from api.db.utils import construct_description_from_blocks
from api.utils.s3 import (
    download_file_from_s3_as_bytes,
    get_media_upload_s3_key_from_uuid,
)
from api.utils.audio import prepare_audio_input_for_ai
from api.settings import tracer
from opentelemetry.trace import StatusCode, Status
from openinference.instrumentation import using_attributes

router = APIRouter()


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
            "type": "text",
            "text": "Student's Response:",
        },
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


def get_user_message_for_chat_history(user_response: str) -> str:
    return f"""Student's Response:\n```\n{user_response}\n```"""


@router.post("/chat")
async def ai_response_for_question(request: AIChatRequest):
    metadata = {"task_id": request.task_id, "user_id": request.user_id}

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
            session_id = (
                f"quiz_{request.task_id}_{request.question_id}_{request.user_id}"
            )
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

    if request.task_type == TaskType.LEARNING_MATERIAL:
        metadata["type"] = "learning_material"
        task = await get_task(request.task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        chat_history = request.chat_history

        reference_material = construct_description_from_blocks(task["blocks"])
        question_details = f"""Reference Material:\n```\n{reference_material}\n```"""
    else:
        metadata["type"] = "quiz"

        if request.question_id:
            question = await get_question(request.question_id)
            if not question:
                raise HTTPException(status_code=404, detail="Question not found")

            metadata["question_id"] = request.question_id

            chat_history = await get_question_chat_history_for_user(
                request.question_id, request.user_id
            )
            chat_history = [
                {"role": message["role"], "content": message["content"]}
                for message in chat_history
            ]
        else:
            question = request.question.model_dump()
            chat_history = request.chat_history

            question["scorecard"] = await get_scorecard(question["scorecard_id"])

            metadata["question_id"] = None

        metadata["question_type"] = question["type"]
        metadata["question_purpose"] = (
            "practice" if question["response_type"] == "chat" else "exam"
        )
        metadata["question_input_type"] = question["input_type"]
        metadata["question_has_context"] = bool(question["context"])

        question_description = construct_description_from_blocks(question["blocks"])
        question_details = f"""Task:\n```\n{question_description}\n```"""

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
                message["content"] = get_user_message_for_chat_history(
                    message["content"]
                )
        else:
            if request.task_type == TaskType.LEARNING_MATERIAL:
                message["content"] = json.dumps({"feedback": message["content"]})

            message["content"] = get_ai_message_for_chat_history(message["content"])

    user_message = (
        get_user_audio_message_for_chat_history(request.user_response)
        if request.response_type == ChatResponseType.AUDIO
        else get_user_message_for_chat_history(request.user_response)
    )

    user_message = {"role": "user", "content": user_message}

    if request.task_type == TaskType.QUIZ:
        if question["type"] == QuestionType.OBJECTIVE:
            answer_as_prompt = construct_description_from_blocks(question["answer"])
            question_details += f"""\n\nReference Solution (never to be shared with the learner):\n```\n{answer_as_prompt}\n```"""
        else:
            scoring_criteria_as_prompt = ""

            for criterion in question["scorecard"]["criteria"]:
                scoring_criteria_as_prompt += f"""- **{criterion['name']}** [min: {criterion['min_score']}, max: {criterion['max_score']}, pass: {criterion.get('pass_score', criterion['max_score'])}]: {criterion['description']}\n"""

            question_details += (
                f"""\n\nScoring Criteria:\n```\n{scoring_criteria_as_prompt}\n```"""
            )

    chat_history = (
        chat_history
        + [user_message]
        + [
            {
                "role": "user",
                "content": question_details,
            }
        ]
    )

    # Define an async generator for streaming
    async def stream_response() -> AsyncGenerator[str, None]:
        with tracer.start_as_current_span(
            "ai_chat", openinference_span_kind="llm"
        ) as span:
            span.set_input(chat_history)

            if request.task_type == TaskType.LEARNING_MATERIAL:
                with using_attributes(
                    session_id=session_id,
                    user_id=str(request.user_id),
                    metadata={"stage": "query_rewrite", **metadata},
                ):
                    system_prompt = f"""You are a very good communicator.\n\nYou will receive:\n- A Reference Material\n- Conversation history with a student\n- The student's latest query/message.\n\nYour role: You need to rewrite the student's latest query/message by taking the reference material and the conversation history into consideration so that the query becomes more specific, detailed and clear, reflecting the actual intent of the student."""

                    model = openai_plan_to_model_name["text-mini"]

                    messages = [
                        {"role": "system", "content": system_prompt}
                    ] + chat_history

                    class Output(BaseModel):
                        rewritten_query: str = Field(
                            description="The rewritten query/message of the student"
                        )

                    pred = await run_llm_with_instructor(
                        api_key=settings.openai_api_key,
                        model=model,
                        messages=messages,
                        response_model=Output,
                        max_completion_tokens=8192,
                    )

                    chat_history[-2]["content"] = get_user_message_for_chat_history(
                        pred.rewritten_query
                    )

            output_buffer = []

            try:
                if request.response_type == ChatResponseType.AUDIO:
                    model = openai_plan_to_model_name["audio"]
                else:

                    class Output(BaseModel):
                        use_reasoning_model: bool = Field(
                            description="Whether to use a reasoning model to evaluate the student's response"
                        )

                    format_instructions = PydanticOutputParser(
                        pydantic_object=Output
                    ).get_format_instructions()

                    system_prompt = f"""You are an intelligent routing agent that decides which type of language model should be used to evaluate a student's response to a given task. You will receive the details of a task, the conversation history with the student and the student's latest query/message.\n\nYou have two options:\n- Reasoning Model (e.g. o3): Best for complex tasks involving logical deduction, problem-solving, code generation, mathematics, research reasoning, multi-step analysis, or edge-case handling.\n- General-Purpose Model (e.g. gpt-4o): Best for everyday conversation, writing help, summaries, rephrasing, explanations, casual queries, grammar correction, and general knowledge Q&A.\n\nYour job is to classify which of the two options is best suited to evaluate the student's response for the given task. If a task can be solved by a general purpose model, avoid using a reasoning model as it takes longer and costs more. At the same time, accuracy cannot be compromised.\n\n{format_instructions}"""

                    messages = [
                        {
                            "role": "system",
                            "content": system_prompt,
                        }
                    ] + chat_history

                    with using_attributes(
                        session_id=session_id,
                        user_id=str(request.user_id),
                        metadata={"stage": "router", **metadata},
                    ):
                        router_output = await run_llm_with_instructor(
                            api_key=settings.openai_api_key,
                            model=openai_plan_to_model_name["router"],
                            messages=messages,
                            response_model=Output,
                            max_completion_tokens=4096,
                        )

                    if router_output.use_reasoning_model:
                        model = openai_plan_to_model_name["reasoning"]
                    else:
                        model = openai_plan_to_model_name["text"]

                # print(f"Using model: {model}")

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

                parser = PydanticOutputParser(pydantic_object=Output)
                format_instructions = parser.get_format_instructions()

                if request.task_type == TaskType.QUIZ:
                    knowledge_base = None

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

                        knowledge_base = construct_description_from_blocks(
                            knowledge_blocks
                        )

                    context_instructions = ""
                    if knowledge_base:
                        context_instructions = f"""\n\nMake sure to use only the information provided within ``` below for responding to the student while ignoring any other information that contradicts the information provided:\n\n```\n{knowledge_base}\n```"""

                    if question["type"] == QuestionType.OBJECTIVE:
                        system_prompt = f"""You are a Socratic tutor who guides a student step-by-step as a coach would, encouraging them to arrive at the correct answer on their own without ever giving away the right answer to the student straight away.\n\nYou will receive:\n\n- Task description\n- Conversation history with the student\n- Task solution (for your reference only; do not reveal){context_instructions}\n\nYou need to evaluate the student's response for correctness and give your feedback that can be shared with the student.\n\n{format_instructions}\n\nGuidelines on assessing correctness of the student's answer:\n\n- Once the student has provided an answer that is correct with respect to the solution provided at the start, clearly acknowledge that they have got the correct answer and stop asking any more reflective questions. Your response should make them feel a sense of completion and accomplishment at a job well done.\n- If the question is one where the answer does not need to match word-for-word with the solution (e.g. definition of a term, programming question where the logic needs to be right but the actual code can vary, etc.), only assess whether the student's answer covers the entire essence of the correct solution.\n- Avoid bringing in your judgement of what the right answer should be. What matters for evaluation is the solution provided to you and the response of the student. Keep your biases outside. Be objective in comparing these two. As soon as the student gets the answer correct, stop asking any further reflective questions.\n- The response is correct only if the question has been solved in its entirety. Partially solving a question is not acceptable.\n\nGuidelines on your feedback:\n\n- Praise → Prompt → Path: 1–2 words of praise, a targeted prompt, then one actionable path forward.\n- If the student's response is completely correct, just appreciate them. No need to give any more suggestions or areas of improvement.\n- If the student's response has areas of improvement, point them out through a single reflective actionable question. Never ever give a vague feedback that is not clearly actionable. The student should get a clear path for how they can improve their response.\n- If the question has multiple steps to reach to the final solution, assess the current step at which the student is and frame your reflection question such that it nudges them towards the right direction without giving away the answer in any shape or form.\n- Your feedback should not be generic and must be tailored to the response given by the student. This does not mean that you repeat the student's response. The question should be a follow-up for the answer given by the student. Don't just paste the student's response on top of a generic question. That would be laziness.\n- The student might get the answer right without any probing required from your side in the first couple of attempts itself. In that case, remember the instruction provided above to acknowledge their answer's correctness and to stop asking further questions.\n- Never provide the right answer or the solution, despite all their attempts to ask for it or their frustration.\n- Never explain the solution to the student unless the student has given the solution first.\n- The student does not have access to the solution. The solution has only been given to you for evaluating the student's response. Keep this in mind while responding to the student.\n\nGuidelines on the style of feedback:\n\n1. Avoid sounding monotonous.\n2. Absolutely AVOID repeating back what the student has said as a manner of acknowledgement in your summary. It makes your summary too long and boring to read.\n3. Occasionally include emojis to maintain warmth and engagement.\n4. Ask only one reflective question per response otherwise the student will get overwhelmed.\n5. Avoid verbosity in your summary. Be crisp and concise, with no extra words.\n6. Do not do any analysis of the user's intent in your overall summary or repeat any part of what the user has said. The summary section is meant to summarise the next steps. The summary section does not need a summary of the user's response.\n\nGuidelines on maintaining the focus of the conversation:\n\n- Your role is that of a tutor for this particular task and related concepts only. Remember that and absolutely avoid steering the conversation in any other direction apart from the actual task given to you and its related concepts.\n- If the student tries to move the focus of the conversation away from the task and its related concepts, gently bring it back to the task.\n- It is very important that you prevent the focus on the conversation with the student being shifted away from the task given to you and its related concepts at all odds. No matter what happens. Stay on the task and its related concepts. Keep bringing the student back. Do not let the conversation drift away."""
                    else:
                        system_prompt = f"""You are a Socratic tutor who guides a student step-by-step as a coach would, encouraging them to arrive at the correct answer on their own without ever giving away the right answer to the student straight away.\n\nYou will receive:\n\n- Task description\n- Conversation history with the student\n- Scoring Criteria to evaluate the answer of the student{context_instructions}\n\nYou need to evaluate the student's response and return the following:\n\n- A scorecard based on the scoring criteria given to you with areas of improvement and/or strengths along each criterion\n- An overall summary based on the generated scorecard to be shared with the student.\n\n{format_instructions}\n\nGuidelines for scorecard feedback:\n\n- If there is nothing to praise about the student's response for a given criterion in the scoring criteria, never mention what worked well (i.e. return `correct` as null) in the scorecard output for that criterion.\n- If the student did something well for a given criterion, make sure to highlight what worked well in the scorecard output for that criterion.\n- If there is nothing left to improve in their response for a criterion, avoid unnecessarily suggesting an improvement in the scorecard output for that criterion (i.e. return `wrong` as null). Also, the score assigned for that criterion should be the maximum score possible in that criterion in this case.\n- Make sure that the feedback for one criterion of the scorecard does not bias the feedback for another criterion.\n- When giving the feedback for one criterion of the scorecard, focus on the description of the criterion provided in the scoring criteria and only evaluate the student's response based on that.\n- For every criterion of the scorecard, your feedback for that criterion in the scorecard output must cite specific words or phrases from the student's response to back your feedback so that the student understands it better and give concrete examples for how they can improve their response as well.\n- Never ever give a vague feedback that is not clearly actionable. The student should get a clear path for how they can improve their response.\n- Avoid bringing your judgement of what the right answer should be. What matters for feedback is the scoring criteria provided to you and the response of the student. Keep your biases outside. Be objective in comparing these two.\n- The student might get the answer right without any probing required from your side in the first couple of attempts itself. In that case, remember the instruction provided above to acknowledge their answer's correctness and to stop asking further questions.\n- If you don't assign the maximum score to the student's response for any criterion in the scorecard, make sure to always include the area of improvement containing concrete steps they can take to improve their response in your feedback for that criterion in the scorecard output (i.e. `wrong` cannot be null).\n\nGuidelines for scorecard feedback style:\n\n1. Avoid sounding monotonous.\n2. Be crisp and concise, with no extra words.\n\nGuidelines for summary:\n- Praise → Prompt → Path: 1–2 words of praise, a targeted prompt, then one actionable path forward.\n- It should clearly outline what the next steps need to be based on the scoring criteria. It should be very crisp and only contain the summary of the next steps outlined in the scorecard feedback.\n- Your overall summary does not need to quote specific words from the user's response or reflect back what the user's response means. Keep that for the feedback in the scorecard output.\n- If the student's response is completely correct, just appreciate them. No need to give any more suggestions or areas of improvement.\n- If the student's response has areas of improvement, point them out through a single reflective actionable question.\n- Your summary and follow-up question should not be generic and must be tailored to the response given by the student. This does not mean that you repeat the student's response. The question should be a follow-up for the answer given by the student. Don't just paste the student's response on top of a generic question. That would be laziness.\n- Never provide the right answer or the solution, despite all their attempts to ask for it or their frustration.\n- Never explain the solution to the student unless the student has given the solution first.\n\nGuidelines for style of summary:\n\n1. Avoid sounding monotonous.\n2. Absolutely AVOID repeating back what the student has said as a manner of acknowledgement in your summary. It makes your summary too long and boring to read.\n3. Occasionally include emojis to maintain warmth and engagement.\n4. Ask only one reflective question per response otherwise the student will get overwhelmed.\n5. Avoid verbosity in your summary.\n6. Do not do any analysis of the user's intent in your overall summary or repeat any part of what the user has said. The summary section is meant to summarise the next steps. The summary section does not need a summary of the user's response.\n\nGuidelines on maintaining the focus of the conversation:\n\n- Your role is that of a tutor for this particular task and related concepts only. Remember that and absolutely avoid steering the conversation in any other direction apart from the actual task given to you and its related concepts.\n- If the student tries to move the focus of the conversation away from the task and its related concepts, gently bring it back to the task.\n- It is very important that you prevent the focus on the conversation with the student being shifted away from the task given to you and its related concepts at all odds. No matter what happens. Stay on the task and its related concepts. Keep bringing the student back. Do not let the conversation drift away.\n\nGuidelines on when to show the scorecard:\n\n- If the response by the student is not a valid answer to the actual task given to them (e.g. if their response is an acknowledgement of the previous messages or a doubt or a question or something irrelevant to the task), do not provide any scorecard in that case and only return a summary addressing their response.\n- For messages of acknowledgement, you do not need to explicitly call it out as an acknowledgement. Simply respond to it normally."""
                else:
                    system_prompt = f"""You are a teaching assistant.\n\nYou will receive:\n- A Reference Material\n- Conversation history with a student\n- The student's latest query/message.\n\nYour role:\n- You need to respond to the student's message based on the content in the reference material provided to you.\n- If the student's query is absolutely not relevant to the reference material or goes beyond the scope of the reference material, clearly saying so without indulging their irrelevant queries. The only exception is when they are asking deeper questions related to the learning material that might not be mentioned in the reference material itself to clarify their conceptual doubts. In this case, you can provide the answer and help them.\n- Remember that the reference material is in read-only mode for the student. So, they cannot make any changes to it.\n\n{format_instructions}\n\nGuidelines on your response style:\n- Be crisp, concise and to the point.\n- Vary your phrasing to avoid monotony; occasionally include emojis to maintain warmth and engagement.\n- Playfully redirect irrelevant responses back to the task without judgment.\n- If the task involves code, format code snippets or variable/function names with backticks (`example`).\n- If including HTML, wrap tags in backticks (`<html>`).\n- If your response includes rich text format like lists, font weights, tables, etc. always render them as markdown.\n- Avoid being unnecessarily verbose in your response.\n\nGuideline on maintaining focus:\n- Your role is that of a teaching assistant for this particular task and its related concepts only. Remember that and absolutely avoid steering the conversation in any other direction apart from the actual task and its related concepts give to you.\n- If the student tries to move the focus of the conversation away from the task and its related concepts, gently bring it back.\n- It is very important that you prevent the focus on the conversation with the student being shifted away from the task and its related concepts given to you at all odds. No matter what happens. Stay on the task and its related concepts. Keep bringing the student back to the task and its related concepts. Do not let the conversation drift away."""

                messages = [{"role": "system", "content": system_prompt}] + chat_history

                with using_attributes(
                    session_id=f"{session_id}",
                    user_id=str(request.user_id),
                    metadata={"stage": "feedback", **metadata},
                ):
                    stream = await stream_llm_with_instructor(
                        api_key=settings.openai_api_key,
                        model=model,
                        messages=messages,
                        response_model=Output,
                        max_completion_tokens=4096,
                    )
                    # Process the async generator
                    async for chunk in stream:
                        content = json.dumps(chunk.model_dump()) + "\n"
                        output_buffer = content
                        yield content
            except Exception as error:
                span.record_exception(error)
                span.set_status(Status(StatusCode.ERROR))
                raise error
            else:
                span.set_output("".join(output_buffer))
                span.set_status(Status(StatusCode.OK))

    # Return a streaming response
    return StreamingResponse(
        stream_response(),
        media_type="application/x-ndjson",
    )


async def migrate_content_to_blocks(content: str) -> List[Dict]:
    class BlockProps(BaseModel):
        level: Optional[Literal[1, 2, 3]] = Field(
            description="The level of a heading block"
        )
        checked: Optional[bool] = Field(
            description="Whether the block is checked (for a checkListItem block)"
        )
        language: Optional[str] = Field(
            description="The language of the code block (for a codeBlock block); always the full name of the language in lowercase (e.g. python, javascript, sql, html, css, etc.)"
        )
        name: Optional[str] = Field(
            description="The name of the image (for an image block)"
        )
        url: Optional[str] = Field(
            description="The URL of the image (for an image block)"
        )

    class BlockContentStyle(BaseModel):
        bold: Optional[bool] = Field(description="Whether the text is bold")
        italic: Optional[bool] = Field(description="Whether the text is italic")
        underline: Optional[bool] = Field(description="Whether the text is underlined")

    class BlockContentText(BaseModel):
        type: Literal["text"] = Field(description="The type of the block content")
        text: str = Field(
            description="The text of the block; if the block is a code block, this should contain the code with newlines and tabs as appropriate"
        )
        styles: BlockContentStyle | dict = Field(
            default={}, description="The styles of the block content"
        )

    class BlockContentLink(BaseModel):
        type: Literal["link"] = Field(description="The type of the block content")
        href: str = Field(description="The URL of the link")
        content: List[BlockContentText] = Field(description="The content of the link")

    class Block(BaseModel):
        type: Literal[
            "heading",
            "paragraph",
            "bulletListItem",
            "numberedListItem",
            "codeBlock",
            "checkListItem",
            "image",
        ] = Field(description="The type of block")
        props: Optional[BlockProps | dict] = Field(
            default={}, description="The properties of the block"
        )
        content: List[BlockContentText | BlockContentLink] = Field(
            description="The content of the block; empty for image blocks"
        )

    class Output(BaseModel):
        blocks: List[Block] = Field(description="The blocks of the content")

    system_prompt = f"""You are an expert course converter. The user will give you a content in markdown format. You will need to convert the content into a structured format as given below.

Never modify the actual content given to you. Just convert it into the structured format.

The `content` field of each block should have multiple blocks only when parts of the same line in the markdown content have different parameters or styles (e.g. some part of the line is bold and some is italic or some part of the line is a link and some is not).

The final output should be a JSON in the following format:

{Output.model_json_schema()}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": content},
    ]

    output = await run_llm_with_instructor(
        api_key=settings.openai_api_key,
        model=openai_plan_to_model_name["text-mini"],
        messages=messages,
        response_model=Output,
        max_completion_tokens=16000,
    )

    blocks = output.model_dump(exclude_none=True)["blocks"]

    for block in blocks:
        if block["type"] == "image":
            block["props"].update(
                {"showPreview": True, "caption": "", "previewWidth": 512}
            )

    return blocks


async def add_generated_module(course_id: int, module: BaseModel):
    websocket_manager = get_manager()
    color = random.choice(
        [
            "#2d3748",  # Slate blue
            "#433c4c",  # Deep purple
            "#4a5568",  # Cool gray
            "#312e51",  # Indigo
            "#364135",  # Forest green
            "#4c393a",  # Burgundy
            "#334155",  # Navy blue
            "#553c2d",  # Rust brown
            "#37303f",  # Plum
            "#3c4b64",  # Steel blue
            "#463c46",  # Mauve
            "#3c322d",  # Coffee
        ]
    )
    module_id, ordering = await add_milestone_to_course(course_id, module.name, color)

    # Send WebSocket update after each module is created
    await websocket_manager.send_item_update(
        course_id,
        {
            "event": "module_created",
            "module": {
                "id": module_id,
                "name": module.name,
                "color": color,
                "ordering": ordering,
            },
        },
    )

    return module_id


async def add_generated_draft_task(course_id: int, module_id: int, task: BaseModel):
    task_id, task_ordering = await create_draft_task_for_course(
        task.name,
        task.type,
        course_id,
        module_id,
    )

    websocket_manager = get_manager()

    await websocket_manager.send_item_update(
        course_id,
        {
            "event": "task_created",
            "task": {
                "id": task_id,
                "module_id": module_id,
                "ordering": task_ordering,
                "type": str(task.type),
                "name": task.name,
            },
        },
    )
    return task_id


async def _generate_course_structure(
    course_description: str,
    intended_audience: str,
    instructions: str,
    openai_file_id: str,
    course_id: int,
    course_job_uuid: str,
    job_details: Dict,
):
    class Task(BaseModel):
        name: str = Field(description="The name of the task")
        description: str = Field(
            description="a detailed description of what should the content of that task be"
        )
        type: Literal[TaskType.LEARNING_MATERIAL, TaskType.QUIZ] | str = Field(
            description="The type of task"
        )

    class Concept(BaseModel):
        name: str = Field(description="The name of the concept")
        description: str = Field(
            description="The description for what the concept is about"
        )
        tasks: List[Task] = Field(description="A list of tasks for the concept")

    class Module(BaseModel):
        name: str = Field(description="The name of the module")
        concepts: List[Concept] = Field(description="A list of concepts for the module")

    class Output(BaseModel):
        # name: str = Field(description="The name of the course")
        modules: List[Module] = Field(description="A list of modules for the course")

    system_prompt = f"""You are an expert course creator. The user will give you some instructions for creating a course along with the reference material to be used as the source for the course content.

You need to thoroughly analyse the reference material given to you and come up with a structure for the course. Each course should be structured into modules where each modules represents a full topic.

With each modules, there must be a mix of learning materials and quizzes. A learning material is used for learning about a specific concept in the topic. Keep separate learning materials for different concepts in the same topic/module. For each concept, the learning material for that concept should be followed by one or more quizzes. Each quiz contains multiple questions for testing the understanding of the learner on the actual concept.

Quizzes are where learners can practice a concept. While testing theoretical understanding is important, quizzes should go beyond that and produce practical challenges for the students to apply what they have learnt. If the reference material already has examples/sample problems, include them in the quizzes for the students to practice. If no examples are present in the reference material, generate a few relevant problem statements to test the real-world understanding of each concept for the students.

All explanations should be present in the learning materials and all practice should be done in quizzes. Maintain this separation of purpose for each task type.

No need to come up with the questions inside the quizzes for now. Just focus on producing the right structure.
Don't keep any concept too big. Break a topic down into multiple smaller, ideally independent, concepts. For each concept, follow the sequence of learning material -> quiz before moving to the next concept in that topic.
End the course with a conclusion module (with the appropriate name for the module suited to the course) which ties everything taught in the course together and ideally ends with a capstone project where the learner has to apply everything they have learnt in the course.

Make sure to never skip a single concept from the reference material provided.

The final output should be a JSON in the following format:

{Output.model_json_schema()}

Keep the sequences of modules, concepts, and tasks in mind.

Do not include the type of task in the name of the task."""

    course_structure_generation_prompt = f"""About the course: {course_description}\n\nIntended audience: {intended_audience}"""

    if instructions:
        course_structure_generation_prompt += f"\n\nInstructions: {instructions}"

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {
                    "type": "file",
                    "file": {
                        "file_id": openai_file_id,
                    },
                },
            ],
        },
        # separate into 2 user messages for prompt caching to work
        {"role": "user", "content": course_structure_generation_prompt},
    ]

    stream = await stream_llm_with_instructor(
        api_key=settings.openai_api_key,
        model=openai_plan_to_model_name["text"],
        messages=messages,
        response_model=Output,
        max_completion_tokens=16000,
    )

    module_ids = []

    module_concepts = defaultdict(lambda: defaultdict(list))

    output = None

    async for chunk in stream:
        if not chunk or not chunk.modules:
            continue

        for index, module in enumerate(chunk.modules):
            if not module or not module.name or not module.concepts:
                continue

            if index >= len(module_ids):
                module_id = await add_generated_module(course_id, module)
                module_ids.append(module_id)
            else:
                module_id = module_ids[index]

            task_index = 0

            for concept_index, concept in enumerate(module.concepts):
                if (
                    not concept
                    or not concept.tasks
                    or concept_index < len(module_concepts[module_id]) - 1
                ):
                    continue

                for task_index, task in enumerate(concept.tasks):
                    if (
                        not task
                        or not task.name
                        or not task.type
                        or task.type not in [TaskType.LEARNING_MATERIAL, TaskType.QUIZ]
                        or task_index < len(module_concepts[module_id][concept_index])
                    ):
                        continue

                    task_id = await add_generated_draft_task(course_id, module_id, task)
                    module_concepts[module_id][concept_index].append(task_id)

        # output = chunk

    output = chunk.model_dump()

    for index, module in enumerate(output["modules"]):
        module["id"] = module_ids[index]

        for concept_index, concept in enumerate(module["concepts"]):
            for task_index, task in enumerate(concept["tasks"]):
                task["id"] = module_concepts[module["id"]][concept_index][task_index]

    job_details["course_structure"] = output
    await update_course_generation_job_status_and_details(
        course_job_uuid,
        GenerateCourseJobStatus.PENDING,
        job_details,
    )

    websocket_manager = get_manager()

    await websocket_manager.send_item_update(
        course_id,
        {
            "event": "course_structure_completed",
            "job_id": course_job_uuid,
        },
    )


@router.post("/generate/course/{course_id}/structure")
async def generate_course_structure(
    course_id: int,
    background_tasks: BackgroundTasks,
    request: GenerateCourseStructureRequest,
):
    openai_client = openai.AsyncOpenAI(
        api_key=settings.openai_api_key,
    )

    if settings.s3_folder_name:
        reference_material = download_file_from_s3_as_bytes(
            request.reference_material_s3_key
        )
    else:
        with open(
            os.path.join(
                settings.local_upload_folder, request.reference_material_s3_key
            ),
            "rb",
        ) as f:
            reference_material = f.read()

    # Create a temporary file to pass to OpenAI
    with tempfile.NamedTemporaryFile(suffix=".pdf") as temp_file:
        temp_file.write(reference_material)
        temp_file.flush()

        file = await openai_client.files.create(
            file=open(temp_file.name, "rb"),
            purpose="user_data",
        )

    job_details = {**request.model_dump(), "openai_file_id": file.id}
    job_uuid = await store_course_generation_request(
        course_id,
        job_details,
    )

    background_tasks.add_task(
        _generate_course_structure,
        request.course_description,
        request.intended_audience,
        request.instructions,
        file.id,
        course_id,
        job_uuid,
        job_details,
    )

    return {"job_uuid": job_uuid}


def task_generation_schemas():

    class BlockProps(BaseModel):
        level: Optional[Literal[2, 3]] = Field(
            description="The level of a heading block"
        )
        checked: Optional[bool] = Field(
            description="Whether the block is checked (for a checkListItem block)"
        )
        language: Optional[str] = Field(
            description="The language of the code block (for a codeBlock block); always the full name of the language in lowercase (e.g. python, javascript, sql, html, css, etc.)"
        )

    class BlockContentStyle(BaseModel):
        bold: Optional[bool] = Field(description="Whether the text is bold")
        italic: Optional[bool] = Field(description="Whether the text is italic")
        underline: Optional[bool] = Field(description="Whether the text is underlined")

    class BlockContent(BaseModel):
        text: str = Field(
            description="The text of the block; if the block is a code block, this should contain the code with newlines and tabs as appropriate"
        )
        styles: BlockContentStyle | dict = Field(
            default={}, description="The styles of the block content"
        )

    class Block(BaseModel):
        type: Literal[
            "heading",
            "paragraph",
            "bulletListItem",
            "numberedListItem",
            "codeBlock",
            "checkListItem",
        ] = Field(description="The type of block")
        props: Optional[BlockProps | dict] = Field(
            default={}, description="The properties of the block"
        )
        content: Optional[List[BlockContent]] = Field(
            description="The content of the block"
        )

    class LearningMaterial(BaseModel):
        blocks: List[Block] = Field(
            description="The content of the learning material as blocks"
        )

    class Criterion(BaseModel):
        name: str = Field(
            description="The name of the criterion (e.g. grammar, relevance, clarity, confidence, pronunciation, brevity, etc.), keep it to 1-2 words unless absolutely necessary to extend beyond that"
        )
        description: str = Field(
            description="The description/rubric for how to assess this criterion - the more detailed it is, the better the evaluation will be, but avoid making it unnecessarily big - only as descriptive as it needs to be but nothing more"
        )
        min_score: int = Field(
            description="The minimum score possible to achieve for this criterion (e.g. 0)"
        )
        max_score: int = Field(
            description="The maximum score possible to achieve for this criterion (e.g. 5)"
        )

    class Scorecard(BaseModel):
        title: str = Field(
            description="what does the scorecard assess (e.g. written communication, interviewing skills, product pitch, etc.)"
        )
        criteria: List[Criterion] = Field(
            description="The list of criteria for the scorecard."
        )

    class Question(BaseModel):
        question_type: Literal["objective", "subjective", "coding"] = Field(
            description='The type of question; "objective" means that the question has a fixed correct answer and the learner\'s response must precisely match it. "subjective" means that the question is subjective, with no fixed correct answer. "coding" - a specific type of "objective" question for programming questions that require one to write code.'
        )
        answer_type: Optional[Literal["text", "audio"]] = Field(
            description='The type of answer; "text" means the student has to submit textual answer where "audio" means student has to submit audio answer. Ignore this field for questionType = "coding".',
        )
        coding_languages: Optional[
            List[Literal["HTML", "CSS", "JS", "Python", "React", "Node", "SQL"]]
        ] = Field(
            description='The languages that a student need to submit their code in for questionType=coding. It is a list because a student might have to submit their code in multiple languages as well (e.g. HTML, CSS, JS). This should only be included for questionType = "coding".',
        )
        blocks: List[Block] = Field(
            description="The actual question details as individual blocks. Every part of the question should be included here. Do not assume that there is another field to capture different parts of the question. This is the only field that should be used to capture the question details. This means that if the question is an MCQ, all the options should be included here and not in another field. Extend the same idea to other question types."
        )
        correct_answer: Optional[List[Block]] = Field(
            description='The actual correct answer to compare a student\'s response with. Ignore this field for questionType = "subjective".',
        )
        scorecard: Optional[Scorecard] = Field(
            description='The scorecard for subjective questions. Ignore this field for questionType = "objective" or "coding".',
        )
        context: List[Block] = Field(
            description="A short text that is not the question itself. This is used to add instructions for how the student should be given feedback or the overall purpose of that question. It can also include the raw content from the reference material to be used for giving feedback to the student that may not be present in the question content (hidden from the student) but is critical for providing good feedback."
        )

    class Quiz(BaseModel):
        questions: List[Question] = Field(
            description="A list of questions for the quiz"
        )

    return LearningMaterial, Quiz


def get_system_prompt_for_task_generation(task_type):
    LearningMaterial, Quiz = task_generation_schemas()
    schema = (
        LearningMaterial.model_json_schema()
        if task_type == "learning_material"
        else Quiz.model_json_schema()
    )

    quiz_prompt = """Each quiz/exam contains multiple questions for testing the understanding of the learner on the actual concept.

Important Instructions for Quiz Generation:
- For a quiz, each question must add a strong positive value to the overall learner's understanding. Do not unnecessarily add questions simply to increase the number of questions. If a quiz merits only a single question based on the reference material provided or your asseessment of how many questions are necessary for it, keep a single question itself. Only add multiple questions when the quiz merits so. 
- The `content` for each question is the only part of the question shown directly to the student. Add everything that the student needs to know to answer the question inside the `content` field for that question. Do not add anything there that should not be shown to the student (e.g. what is the correct answer). To add instructions for how the student should be given feedback or the overall purpose of that question or raw content from the reference material required as context to give adequate feedback, add it to the `context` field instead. 
- While testing theoretical understanding is important, a quiz should go beyond that and produce practical challenges for the students to apply what they have learnt. If the reference material already has examples/sample problems, include them in the a quiz for the students to practice. If no examples are present in the reference material, generate a few relevant problem statements to test the real-world understanding of each concept for the students.
- If a question references a set of options that must be shown to the student, always make sure that those options are actually present in the `content` field for that question. THIS IS SUPER IMPORTANT. As mentioned before, if the reference material does not have the options or data required for the question, generate it based on your understanding of the question and its purpose.
- Use appropriate formatting for the `blocks` in each question. Make use of all the block types available to you to make the content of each question as engaging and readable as possible.
- Do not use the name of the quiz as a heading to mark the start of a question in the `blocks` field for each question. The name of the quiz will already be visible to the student."""

    learning_material_prompt = """A learning material is used for learning about a specific concept. 
    
Make the \"content\" field in learning material contain as much detail as present in the reference material relevant to it. Do not try to summarise it or skip any point.

Use appropriate formatting for the `blocks` in the learning material. Make use of all the block types available to you to make the content as engaging and readable as possible.

Do not use the name of the learning material as a heading to mark the start of the learning material in the `blocks`.  The name of the learning material will already be visible to the student."""

    task_type_prompt = quiz_prompt if task_type == "quiz" else learning_material_prompt

    system_prompt = f"""You are an expert course creator. The user will give you an outline for a concept in a course they are creating along with the reference material to be used as the source for the course content and the name of one of the tasks from the outline.

You need to generate the content for the single task whose name is provided to you out of all the tasks in the outline. The outline contains the name of a concept in the course, its description and a list of tasks in that concept. Each task can be either a learning material, quiz or exam. You are given this outline so that you can clearly identify what part of the reference material should be used for generating the specific task you need to generate and for you to also understand what should not be included in your generated task. For each task, you have been given a description about what should be included in that task. 

{task_type_prompt}

The final output should be a JSON in the following format:

{schema}"""

    return system_prompt


async def generate_course_task(
    client,
    task: Dict,
    concept: Dict,
    file_id: str,
    task_job_uuid: str,
    course_job_uuid: str,
    course_id: int,
):

    system_prompt = get_system_prompt_for_task_generation(task["type"])

    model = openai_plan_to_model_name["text"]

    generation_prompt = f"""Concept details:

{concept}

Task to generate:

{task['name']}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {
                    "type": "file",
                    "file": {
                        "file_id": file_id,
                    },
                },
            ],
        },
        # separate into 2 user messages for prompt caching to work
        {"role": "user", "content": generation_prompt},
    ]

    LearningMaterial, Quiz = task_generation_schemas()
    response_model = (
        LearningMaterial if task["type"] == TaskType.LEARNING_MATERIAL else Quiz
    )

    output = await client.chat.completions.create(
        model=model,
        messages=messages,
        response_model=response_model,
        max_completion_tokens=16000,
        store=True,
    )

    task["details"] = output.model_dump(exclude_none=True)

    if task["type"] == TaskType.LEARNING_MATERIAL:
        await add_generated_learning_material(task["id"], task)
    else:
        await add_generated_quiz(task["id"], task)

    await update_task_generation_job_status(
        task_job_uuid, GenerateTaskJobStatus.COMPLETED
    )

    course_jobs_status = await get_course_task_generation_jobs_status(course_id)

    websocket_manager = get_manager()

    await websocket_manager.send_item_update(
        course_id,
        {
            "event": "task_completed",
            "task": {
                "id": task["id"],
            },
            "total_completed": course_jobs_status[str(GenerateTaskJobStatus.COMPLETED)],
        },
    )

    if not course_jobs_status[str(GenerateTaskJobStatus.STARTED)]:
        await update_course_generation_job_status(
            course_job_uuid, GenerateCourseJobStatus.COMPLETED
        )


@router.post("/generate/course/{course_id}/tasks")
async def generate_course_tasks(
    course_id: int,
    job_uuid: str = Body(..., embed=True),
):
    job_details = await get_course_generation_job_details(job_uuid)

    client = instructor.from_openai(
        openai.AsyncOpenAI(
            api_key=settings.openai_api_key,
        )
    )

    # Create a list to hold all task coroutines
    tasks = []

    for module in job_details["course_structure"]["modules"]:
        for concept in module["concepts"]:
            for task in concept["tasks"]:
                task_job_uuid = await store_task_generation_request(
                    task["id"],
                    course_id,
                    {
                        "task": task,
                        "concept": concept,
                        "openai_file_id": job_details["openai_file_id"],
                        "course_job_uuid": job_uuid,
                        "course_id": course_id,
                    },
                )
                # Add task to the list instead of adding to background_tasks
                tasks.append(
                    generate_course_task(
                        client,
                        task,
                        concept,
                        job_details["openai_file_id"],
                        task_job_uuid,
                        job_uuid,
                        course_id,
                    )
                )

    # Create a function to run all tasks in parallel
    async def run_tasks_in_parallel():
        try:
            # Run all tasks concurrently using asyncio.gather
            await async_batch_gather(tasks, description="Generating tasks")
        except Exception as e:
            logger.error(f"Error in parallel task execution: {e}")

    # Start the parallel execution in the background without awaiting it
    asyncio.create_task(run_tasks_in_parallel())

    return {
        "success": True,
    }


async def resume_pending_course_structure_generation_jobs():
    incomplete_course_structure_jobs = (
        await get_all_pending_course_structure_generation_jobs()
    )

    if not incomplete_course_structure_jobs:
        return

    tasks = []

    for job in incomplete_course_structure_jobs:
        tasks.append(
            _generate_course_structure(
                job["job_details"]["course_description"],
                job["job_details"]["intended_audience"],
                job["job_details"]["instructions"],
                job["job_details"]["openai_file_id"],
                job["course_id"],
                job["uuid"],
                job["job_details"],
            )
        )

    await async_batch_gather(
        tasks, description="Resuming course structure generation jobs"
    )


async def resume_pending_task_generation_jobs():
    incomplete_course_jobs = await get_all_pending_task_generation_jobs()

    if not incomplete_course_jobs:
        return

    tasks = []

    client = instructor.from_openai(
        openai.AsyncOpenAI(
            api_key=settings.openai_api_key,
        )
    )

    for job in incomplete_course_jobs:
        tasks.append(
            generate_course_task(
                client,
                job["job_details"]["task"],
                job["job_details"]["concept"],
                job["job_details"]["openai_file_id"],
                job["uuid"],
                job["job_details"]["course_job_uuid"],
                job["job_details"]["course_id"],
            )
        )

    await async_batch_gather(tasks, description="Resuming task generation jobs")
