import pytest
import base64
from src.api.utils.audio import prepare_audio_input_for_ai


class TestAudioUtils:
    def test_prepare_audio_input_for_ai(self):
        """Test preparing audio data for AI processing."""
        # Create sample audio data
        sample_audio = b"test audio data"

        # Expected result - base64 encoded string
        expected = base64.b64encode(sample_audio).decode("utf-8")

        # Call the function
        result = prepare_audio_input_for_ai(sample_audio)

        # Check the result
        assert result == expected
        assert isinstance(result, str)

    def test_prepare_audio_input_for_ai_empty(self):
        """Test preparing empty audio data for AI processing."""
        # Create empty audio data
        sample_audio = b""

        # Call the function
        result = prepare_audio_input_for_ai(sample_audio)

        # Check the result
        assert result == ""
        assert isinstance(result, str)
