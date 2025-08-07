from typing import Dict, List, Literal, Optional, Type, Optional, Generator, Iterable
import backoff
import openai
from openai import OpenAI
from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo
from api.utils.logging import logger
import jiter

# Test log message
logger.info("Logging system initialized")


def is_reasoning_model(model: str) -> bool:
    for model_family in ["o1", "o3", "o4", "gpt-5"]:
        if model_family in model:
            return True

    return False


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
async def run_llm_with_openai(
    api_key: str,
    model: str,
    messages: List,
    response_model: BaseModel,
    max_completion_tokens: int,
    reasoning_effort: Optional[Literal["minimal", "low", "medium", "high"]] = None,
):
    client = openai.AsyncOpenAI(api_key=api_key)

    model_kwargs = {}

    if not is_reasoning_model(model):
        model_kwargs["temperature"] = 0
    else:
        if reasoning_effort:
            model_kwargs["reasoning"] = {
                "effort": reasoning_effort,
            }

    response = await client.responses.parse(
        model=model,
        input=messages,
        text_format=response_model,
        max_output_tokens=max_completion_tokens,
        store=True,
        **model_kwargs,
    )

    return response.output[1].content[0].parsed


# This function takes any Pydantic model and creates a new one
# where all fields are optional, allowing for partial data.
def create_partial_model(model: Type[BaseModel]) -> Type[BaseModel]:
    """
    Dynamically creates a Pydantic model where all fields of the original model
    are converted to Optional and have a default value of None.
    """
    new_fields = {}
    for name, field_info in model.model_fields.items():
        # Create a new FieldInfo with Optional type and a default of None
        new_field_info = FieldInfo.from_annotation(Optional[field_info.annotation])
        new_field_info.default = None
        new_fields[name] = (new_field_info.annotation, new_field_info)

    # Create the new model with the same name prefixed by "Partial"
    return create_model(f"Partial{model.__name__}", **new_fields)


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
async def stream_llm_with_openai(
    api_key: str,
    model: str,
    messages: List,
    response_model: BaseModel,
    max_completion_tokens: int,
    reasoning_effort: Optional[Literal["minimal", "low", "medium", "high"]] = None,
):
    client = openai.AsyncOpenAI(api_key=api_key)

    model_kwargs = {}

    if not is_reasoning_model(model):
        model_kwargs["temperature"] = 0
    else:
        if reasoning_effort:
            model_kwargs["reasoning"] = {
                "effort": reasoning_effort,
            }

    partial_model = create_partial_model(response_model)

    async with client.responses.stream(
        model=model,
        input=messages,
        text_format=response_model,
        max_output_tokens=max_completion_tokens,
        store=True,
        **model_kwargs,
    ) as stream:
        json_buffer = ""
        async for event in stream:
            if event.type == "response.output_text.delta":
                # Get the content delta from the chunk
                content = event.delta or ""
                if not content:
                    continue

                json_buffer += content

                # Use jiter to parse the potentially incomplete JSON string.
                # We wrap this in a try-except block to handle cases where the buffer
                # is not yet a parsable JSON fragment (e.g., just whitespace or a comma).
                try:
                    # 'trailing-strings' mode allows jiter to parse incomplete strings at the end of the JSON.
                    parsed_data = jiter.from_json(
                        json_buffer.encode("utf-8"), partial_mode="trailing-strings"
                    )

                    # Validate the partially parsed data against our dynamic partial model.
                    # `strict=False` allows for some type coercion, which is helpful here.
                    partial_obj = partial_model.model_validate(
                        parsed_data, strict=False
                    )
                    yield partial_obj
                except:
                    # The buffer isn't a valid partial JSON object yet, so we wait for more chunks.
                    continue
