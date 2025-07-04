from typing import Dict, List
import backoff
import openai
import instructor

from openai import OpenAI

from pydantic import BaseModel

from api.utils.logging import logger

# Test log message
logger.info("Logging system initialized")


def is_reasoning_model(model: str) -> bool:
    return model in [
        "o3-mini-2025-01-31",
        "o3-mini",
        "o1-preview-2024-09-12",
        "o1-preview",
        "o1-mini",
        "o1-mini-2024-09-12",
        "o1",
        "o1-2024-12-17",
    ]


def validate_openai_api_key(openai_api_key: str) -> bool:
    client = OpenAI(api_key=openai_api_key)
    try:
        models = client.models.list()
        model_ids = [model.id for model in models.data]

        if "gpt-4o-audio-preview-2024-12-17" in model_ids:
            return False  # paid account
        else:
            return True  # free trial account
    except Exception:
        return None


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
async def run_llm_with_instructor(
    api_key: str,
    model: str,
    messages: List,
    response_model: BaseModel,
    max_completion_tokens: int,
):
    client = instructor.from_openai(openai.AsyncOpenAI(api_key=api_key))

    model_kwargs = {}

    if not is_reasoning_model(model):
        model_kwargs["temperature"] = 0

    return await client.chat.completions.create(
        model=model,
        messages=messages,
        response_model=response_model,
        max_completion_tokens=max_completion_tokens,
        store=True,
        **model_kwargs,
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
async def stream_llm_with_instructor(
    api_key: str,
    model: str,
    messages: List,
    response_model: BaseModel,
    max_completion_tokens: int,
    **kwargs,
):
    client = instructor.from_openai(openai.AsyncOpenAI(api_key=api_key))

    model_kwargs = {}

    if not is_reasoning_model(model):
        model_kwargs["temperature"] = 0

    model_kwargs.update(kwargs)

    return client.chat.completions.create_partial(
        model=model,
        messages=messages,
        response_model=response_model,
        stream=True,
        max_completion_tokens=max_completion_tokens,
        store=True,
        **model_kwargs,
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
def stream_llm_with_openai(
    api_key: str,
    model: str,
    messages: List,
    max_completion_tokens: int,
):
    client = openai.OpenAI(api_key=api_key)

    model_kwargs = {}

    if not is_reasoning_model(model):
        model_kwargs["temperature"] = 0

    return client.chat.completions.create(
        model=model,
        messages=messages,
        stream=True,
        max_completion_tokens=max_completion_tokens,
        store=True,
        **model_kwargs,
    )
