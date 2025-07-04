import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from pydantic import BaseModel
from src.api.llm import (
    is_reasoning_model,
    validate_openai_api_key,
    run_llm_with_instructor,
    stream_llm_with_instructor,
    stream_llm_with_openai,
)


class TestIsReasoningModel:
    """Test the is_reasoning_model function."""

    def test_is_reasoning_model_o3_mini(self):
        """Test that o3-mini models are identified as reasoning models."""
        assert is_reasoning_model("o3-mini-2025-01-31") is True
        assert is_reasoning_model("o3-mini") is True

    def test_is_reasoning_model_o1_models(self):
        """Test that o1 models are identified as reasoning models."""
        assert is_reasoning_model("o1-preview-2024-09-12") is True
        assert is_reasoning_model("o1-preview") is True
        assert is_reasoning_model("o1-mini") is True
        assert is_reasoning_model("o1-mini-2024-09-12") is True
        assert is_reasoning_model("o1") is True
        assert is_reasoning_model("o1-2024-12-17") is True

    def test_is_reasoning_model_non_reasoning(self):
        """Test that non-reasoning models are correctly identified."""
        assert is_reasoning_model("gpt-4") is False
        assert is_reasoning_model("gpt-3.5-turbo") is False
        assert is_reasoning_model("claude-3") is False
        assert is_reasoning_model("gpt-4o") is False
        assert is_reasoning_model("random-model") is False

    def test_is_reasoning_model_empty_string(self):
        """Test with empty string."""
        assert is_reasoning_model("") is False

    def test_is_reasoning_model_none(self):
        """Test with None input."""
        assert is_reasoning_model(None) is False


class TestValidateOpenaiApiKey:
    """Test the validate_openai_api_key function."""

    @patch("src.api.llm.OpenAI")
    def test_validate_openai_api_key_free_trial(self, mock_openai):
        """Test API key validation for free trial account."""
        # Setup mocks
        mock_client = MagicMock()
        mock_openai.return_value = mock_client

        # Mock models list without the premium model
        mock_models = MagicMock()
        mock_models.data = [
            MagicMock(id="gpt-3.5-turbo"),
            MagicMock(id="gpt-4"),
            MagicMock(id="text-davinci-003"),
        ]
        mock_client.models.list.return_value = mock_models

        # Call the function
        result = validate_openai_api_key("test_api_key")

        # Assertions
        assert result is True  # Free trial account
        mock_openai.assert_called_once_with(api_key="test_api_key")
        mock_client.models.list.assert_called_once()

    @patch("src.api.llm.OpenAI")
    def test_validate_openai_api_key_paid_account(self, mock_openai):
        """Test API key validation for paid account."""
        # Setup mocks
        mock_client = MagicMock()
        mock_openai.return_value = mock_client

        # Mock models list with the premium model
        mock_models = MagicMock()
        mock_models.data = [
            MagicMock(id="gpt-3.5-turbo"),
            MagicMock(id="gpt-4"),
            MagicMock(id="gpt-4o-audio-preview-2024-12-17"),  # Premium model
        ]
        mock_client.models.list.return_value = mock_models

        # Call the function
        result = validate_openai_api_key("test_api_key")

        # Assertions
        assert result is False  # Paid account
        mock_openai.assert_called_once_with(api_key="test_api_key")
        mock_client.models.list.assert_called_once()

    @patch("src.api.llm.OpenAI")
    def test_validate_openai_api_key_exception(self, mock_openai):
        """Test API key validation when exception occurs."""
        # Setup mocks
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_client.models.list.side_effect = Exception("API Error")

        # Call the function
        result = validate_openai_api_key("invalid_api_key")

        # Assertions
        assert result is None  # Exception case
        mock_openai.assert_called_once_with(api_key="invalid_api_key")
        mock_client.models.list.assert_called_once()


@pytest.mark.asyncio
class TestRunLlmWithInstructor:
    """Test the run_llm_with_instructor function."""

    class MockResponseModel(BaseModel):
        response: str

    @patch("src.api.llm.instructor.from_openai")
    @patch("src.api.llm.openai.AsyncOpenAI")
    @patch("src.api.llm.is_reasoning_model")
    async def test_run_llm_with_instructor_non_reasoning(
        self, mock_is_reasoning, mock_async_openai, mock_instructor
    ):
        """Test run_llm_with_instructor with non-reasoning model."""
        # Setup mocks
        mock_is_reasoning.return_value = False
        mock_client = AsyncMock()
        mock_instructor.return_value = mock_client
        mock_response = {"response": "test response"}
        mock_client.chat.completions.create.return_value = mock_response

        # Call the function
        result = await run_llm_with_instructor(
            api_key="test_key",
            model="gpt-4",
            messages=[{"role": "user", "content": "hello"}],
            response_model=self.MockResponseModel,
            max_completion_tokens=100,
        )

        # Assertions
        assert result == mock_response
        mock_async_openai.assert_called_once_with(api_key="test_key")
        mock_instructor.assert_called_once()
        mock_client.chat.completions.create.assert_called_once()

        # Check that temperature was set for non-reasoning model
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["temperature"] == 0

    @patch("src.api.llm.instructor.from_openai")
    @patch("src.api.llm.openai.AsyncOpenAI")
    @patch("src.api.llm.is_reasoning_model")
    async def test_run_llm_with_instructor_reasoning(
        self, mock_is_reasoning, mock_async_openai, mock_instructor
    ):
        """Test run_llm_with_instructor with reasoning model."""
        # Setup mocks
        mock_is_reasoning.return_value = True
        mock_client = AsyncMock()
        mock_instructor.return_value = mock_client
        mock_response = {"response": "reasoning response"}
        mock_client.chat.completions.create.return_value = mock_response

        # Call the function
        result = await run_llm_with_instructor(
            api_key="test_key",
            model="o1-preview",
            messages=[{"role": "user", "content": "hello"}],
            response_model=self.MockResponseModel,
            max_completion_tokens=100,
        )

        # Assertions
        assert result == mock_response
        mock_async_openai.assert_called_once_with(api_key="test_key")
        mock_instructor.assert_called_once()
        mock_client.chat.completions.create.assert_called_once()

        # Check that temperature was NOT set for reasoning model
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert "temperature" not in call_kwargs


@pytest.mark.asyncio
class TestStreamLlmWithInstructor:
    """Test the stream_llm_with_instructor function."""

    class MockResponseModel(BaseModel):
        response: str

    @patch("src.api.llm.instructor.from_openai")
    @patch("src.api.llm.openai.AsyncOpenAI")
    @patch("src.api.llm.is_reasoning_model")
    async def test_stream_llm_with_instructor_success(
        self, mock_is_reasoning, mock_async_openai, mock_instructor
    ):
        """Test stream_llm_with_instructor function."""
        # Setup mocks
        mock_is_reasoning.return_value = False
        mock_client = MagicMock()
        mock_instructor.return_value = mock_client
        mock_stream = AsyncMock()
        mock_client.chat.completions.create_partial.return_value = mock_stream

        # Call the function
        result = await stream_llm_with_instructor(
            api_key="test_key",
            model="gpt-4",
            messages=[{"role": "user", "content": "hello"}],
            response_model=self.MockResponseModel,
            max_completion_tokens=100,
            extra_param="test",
        )

        # Assertions
        assert result == mock_stream
        mock_async_openai.assert_called_once_with(api_key="test_key")
        mock_instructor.assert_called_once()
        mock_client.chat.completions.create_partial.assert_called_once()

        # Check that extra kwargs were passed
        call_kwargs = mock_client.chat.completions.create_partial.call_args[1]
        assert call_kwargs["extra_param"] == "test"
        assert call_kwargs["stream"] is True


class TestStreamLlmWithOpenai:
    """Test the stream_llm_with_openai function."""

    @patch("src.api.llm.openai.OpenAI")
    @patch("src.api.llm.is_reasoning_model")
    def test_stream_llm_with_openai_non_reasoning(self, mock_is_reasoning, mock_openai):
        """Test stream_llm_with_openai with non-reasoning model."""
        # Setup mocks
        mock_is_reasoning.return_value = False
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_stream = MagicMock()
        mock_client.chat.completions.create.return_value = mock_stream

        # Call the function
        result = stream_llm_with_openai(
            api_key="test_key",
            model="gpt-4",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=100,
        )

        # Assertions
        assert result == mock_stream
        mock_openai.assert_called_once_with(api_key="test_key")
        mock_client.chat.completions.create.assert_called_once()

        # Check that temperature was set and stream is True
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["temperature"] == 0
        assert call_kwargs["stream"] is True

    @patch("src.api.llm.openai.OpenAI")
    @patch("src.api.llm.is_reasoning_model")
    def test_stream_llm_with_openai_reasoning(self, mock_is_reasoning, mock_openai):
        """Test stream_llm_with_openai with reasoning model."""
        # Setup mocks
        mock_is_reasoning.return_value = True
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_stream = MagicMock()
        mock_client.chat.completions.create.return_value = mock_stream

        # Call the function
        result = stream_llm_with_openai(
            api_key="test_key",
            model="o1-mini",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=100,
        )

        # Assertions
        assert result == mock_stream
        mock_openai.assert_called_once_with(api_key="test_key")
        mock_client.chat.completions.create.assert_called_once()

        # Check that temperature was NOT set for reasoning model
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert "temperature" not in call_kwargs
        assert call_kwargs["stream"] is True
