import pytest
from unittest.mock import patch
from src.api.routes.ai import get_user_details_for_prompt


@pytest.mark.asyncio
class TestAIFunctions:
    """Test AI route helper functions."""

    @patch("src.api.routes.ai.get_user_first_name")
    async def test_get_user_details_for_prompt_with_first_name(self, mock_get_user_first_name):
        """Test get_user_details_for_prompt when user has first name."""
        mock_get_user_first_name.return_value = "John"

        result = await get_user_details_for_prompt("1")

        assert result == "Name: John"
        mock_get_user_first_name.assert_called_once_with("1")

    @patch("src.api.routes.ai.get_user_first_name")
    async def test_get_user_details_for_prompt_no_first_name(self, mock_get_user_first_name):
        """Test get_user_details_for_prompt when user has no first name."""
        mock_get_user_first_name.return_value = None

        result = await get_user_details_for_prompt("1")

        assert result == ""
        mock_get_user_first_name.assert_called_once_with("1")

    @patch("src.api.routes.ai.get_user_first_name")
    async def test_get_user_details_for_prompt_empty_first_name(self, mock_get_user_first_name):
        """Test get_user_details_for_prompt when user has empty first name."""
        mock_get_user_first_name.return_value = ""

        result = await get_user_details_for_prompt("1")

        assert result == ""
        mock_get_user_first_name.assert_called_once_with("1")

