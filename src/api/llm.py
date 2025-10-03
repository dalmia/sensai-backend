from typing import Optional, Type, Literal
import backoff
from langfuse.openai import AsyncOpenAI
from pydantic import BaseModel
from pydantic import create_model
from pydantic.fields import FieldInfo
import jiter
from langchain_core.output_parsers import PydanticOutputParser
import openai
import instructor
from api.utils.logging import logger

# Test log message
logger.info("Logging system initialized")


def is_reasoning_model(model: str) -> bool:
    if not model:
        return False

    for model_family in ["o3", "o1", "o1", "o4", "gpt-5"]:
        if model_family in model:
            return True

    return False


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
async def stream_llm_with_instructor(
    model: str,
    messages: list,
    response_model: BaseModel,
    max_completion_tokens: int,
    **kwargs,
):
    client = instructor.from_openai(openai.AsyncOpenAI())

    if not kwargs and not is_reasoning_model(model):
        kwargs["temperature"] = 0

    return client.chat.completions.create_partial(
        model=model,
        messages=messages,
        response_model=response_model,
        stream=True,
        max_completion_tokens=max_completion_tokens,
        store=True,
        **kwargs,
    )


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
    api_mode: Literal["responses", "chat_completions"] = "responses",
    **kwargs,
):
    client = AsyncOpenAI()

    partial_model = create_partial_model(response_model)

    if not kwargs and not is_reasoning_model(model):
        kwargs["temperature"] = 0

    if api_mode == "responses":
        stream = client.responses.stream(
            model=model,
            input=messages,
            text_format=response_model,
            max_output_tokens=max_output_tokens,
            store=True,
            metadata={},
            **kwargs,
        )
    else:
        if "-audio-" in model:
            # hack for audio as current audio models do not support response_format
            output_parser = PydanticOutputParser(pydantic_object=response_model)
            format_instructions = output_parser.get_format_instructions()

            messages[0]["content"] = (
                messages[0]["content"] + f"\n\nOutput format:\n{format_instructions}"
            )

            async for stream in await stream_llm_with_instructor(
                model=model,
                messages=messages,
                response_model=response_model,
                max_completion_tokens=max_output_tokens,
                **kwargs,
            ):
                yield stream

            return
        else:
            stream = client.chat.completions.stream(
                model=model,
                messages=messages,
                response_format=response_model,
                max_completion_tokens=max_output_tokens,
                store=True,
                n=1,
                **kwargs,
            )

    async with stream as stream:
        json_buffer = ""
        async for event in stream:
            if api_mode == "responses":
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
            else:
                if event.type == "chunk":
                    content = event.snapshot.choices[0].message.content
                    if not content:
                        continue

                    # Use jiter to parse the potentially incomplete JSON string.
                    # We wrap this in a try-except block to handle cases where the buffer
                    # is not yet a parsable JSON fragment (e.g., just whitespace or a comma).
                    try:
                        # 'trailing-strings' mode allows jiter to parse incomplete strings at the end of the JSON.
                        parsed_data = jiter.from_json(
                            content.encode("utf-8"), partial_mode="trailing-strings"
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
                elif event.type == "error":
                    raise event.error
                elif event.type == "content.done":
                    yield event.parsed


@backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
async def run_llm_with_openai(
    model: str,
    messages: list[dict],
    response_model: BaseModel,
    max_output_tokens: int,
    api_mode: Literal["responses", "chat_completions"] = "responses",
    **kwargs,
):
    client = AsyncOpenAI()

    if not kwargs and not is_reasoning_model(model):
        kwargs["temperature"] = 0

    if api_mode == "responses":
        response = await client.responses.parse(
            model=model,
            input=messages,
            text_format=response_model,
            max_output_tokens=max_output_tokens,
            store=True,
            **kwargs,
        )

        return response.output_parsed

    if "-audio-" in model:
        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            max_completion_tokens=max_output_tokens,
            store=True,
            **kwargs,
        )

        return response.choices[0].message.content

    response = await client.chat.completions.parse(
        model=model,
        messages=messages,
        response_format=response_model,
        max_completion_tokens=max_output_tokens,
        store=True,
        **kwargs,
    )

    return response.choices[0].message.parsed
