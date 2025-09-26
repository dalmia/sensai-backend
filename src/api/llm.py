from typing import Optional, Type
import backoff
from langfuse.openai import AsyncOpenAI
from pydantic import BaseModel
from pydantic import create_model
from pydantic.fields import FieldInfo
import jiter

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
    model: str,
    messages: list[dict],
    response_model: BaseModel,
    max_output_tokens: int,
    **kwargs,
):
    client = AsyncOpenAI()

    partial_model = create_partial_model(response_model)

    async with client.responses.stream(
        model=model,
        input=messages,
        text_format=response_model,
        max_output_tokens=max_output_tokens,
        store=True,
        metadata={},
        **kwargs,
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


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
async def run_llm_with_openai(
    model: str,
    messages: list[dict],
    response_model: BaseModel,
    max_output_tokens: int,
    langfuse_prompt,
    **kwargs,
):
    client = AsyncOpenAI()

    response = await client.responses.parse(
        model=model,
        input=messages,
        text_format=response_model,
        max_output_tokens=max_output_tokens,
        store=True,
        langfuse_prompt=langfuse_prompt,
        **kwargs,
    )

    return response.output_parsed
