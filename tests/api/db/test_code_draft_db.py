import pytest
import json
from unittest.mock import patch, AsyncMock
from src.api.db.code_draft import (
    upsert_user_code_draft,
    get_user_code_draft,
    delete_user_code_draft,
)


@pytest.mark.asyncio
class TestCodeDraftOperations:
    """Test code draft database operations."""

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_upsert_user_code_draft_success(self, mock_execute):
        """Test successful code draft upsert."""
        code_data = [{"type": "function", "name": "test", "code": "def test(): pass"}]

        await upsert_user_code_draft(1, 1, code_data)

        mock_execute.assert_called_once_with(
            """
        INSERT INTO code_drafts (user_id, question_id, code)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, question_id) DO UPDATE SET
            code = excluded.code,
            updated_at = CURRENT_TIMESTAMP
        """,
            (1, 1, json.dumps(code_data)),
        )

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_upsert_user_code_draft_empty_code(self, mock_execute):
        """Test code draft upsert with empty code."""
        code_data = []

        await upsert_user_code_draft(1, 1, code_data)

        mock_execute.assert_called_once_with(
            """
        INSERT INTO code_drafts (user_id, question_id, code)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, question_id) DO UPDATE SET
            code = excluded.code,
            updated_at = CURRENT_TIMESTAMP
        """,
            (1, 1, json.dumps(code_data)),
        )

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_get_user_code_draft_success(self, mock_execute):
        """Test successful code draft retrieval."""
        code_data = [{"type": "function", "name": "test", "code": "def test(): pass"}]
        mock_execute.return_value = (
            1,  # id
            json.dumps(code_data),  # code
            "2024-01-01 12:00:00",  # updated_at
        )

        result = await get_user_code_draft(1, 1)

        expected = {"id": 1, "code": code_data, "updated_at": "2024-01-01 12:00:00"}

        assert result == expected
        mock_execute.assert_called_once_with(
            """SELECT id, code, updated_at FROM code_drafts
            WHERE user_id = ? AND question_id = ?""",
            (1, 1),
            fetch_one=True,
        )

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_get_user_code_draft_not_found(self, mock_execute):
        """Test code draft retrieval when not found."""
        mock_execute.return_value = None

        result = await get_user_code_draft(1, 1)

        assert result is None
        mock_execute.assert_called_once_with(
            """SELECT id, code, updated_at FROM code_drafts
            WHERE user_id = ? AND question_id = ?""",
            (1, 1),
            fetch_one=True,
        )

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_get_user_code_draft_complex_code(self, mock_execute):
        """Test code draft retrieval with complex code structure."""
        code_data = [
            {
                "type": "function",
                "name": "calculate",
                "code": "def calculate(x, y):\n    return x + y",
                "params": ["x", "y"],
            },
            {
                "type": "class",
                "name": "TestClass",
                "code": "class TestClass:\n    def __init__(self):\n        pass",
            },
        ]
        mock_execute.return_value = (
            2,  # id
            json.dumps(code_data),  # code
            "2024-01-01 12:30:00",  # updated_at
        )

        result = await get_user_code_draft(2, 3)

        expected = {"id": 2, "code": code_data, "updated_at": "2024-01-01 12:30:00"}

        assert result == expected
        assert len(result["code"]) == 2
        assert result["code"][0]["type"] == "function"
        assert result["code"][1]["type"] == "class"

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_delete_user_code_draft_success(self, mock_execute):
        """Test successful code draft deletion."""
        await delete_user_code_draft(1, 1)

        mock_execute.assert_called_once_with(
            "DELETE FROM code_drafts WHERE user_id = ? AND question_id = ?", (1, 1)
        )

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_delete_user_code_draft_different_ids(self, mock_execute):
        """Test code draft deletion with different user and question IDs."""
        await delete_user_code_draft(5, 10)

        mock_execute.assert_called_once_with(
            "DELETE FROM code_drafts WHERE user_id = ? AND question_id = ?", (5, 10)
        )


@pytest.mark.asyncio
class TestCodeDraftEdgeCases:
    """Test edge cases for code draft operations."""

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_upsert_user_code_draft_complex_structure(self, mock_execute):
        """Test code draft upsert with complex nested structure."""
        code_data = [
            {
                "type": "function",
                "name": "complex_function",
                "code": "def complex_function(data):\n    if isinstance(data, dict):\n        return data.get('value', 0)\n    return 0",
                "metadata": {
                    "parameters": ["data"],
                    "return_type": "int",
                    "description": "A complex function",
                },
            }
        ]

        await upsert_user_code_draft(123, 456, code_data)

        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert call_args[0][1] == (123, 456, json.dumps(code_data))

    @patch("src.api.db.code_draft.execute_db_operation")
    async def test_get_user_code_draft_json_parsing(self, mock_execute):
        """Test that JSON parsing works correctly for retrieved code."""
        code_data = {"test": "value", "number": 42, "list": [1, 2, 3]}
        mock_execute.return_value = (1, json.dumps(code_data), "2024-01-01 12:00:00")

        result = await get_user_code_draft(1, 1)

        assert result["code"] == code_data
        assert isinstance(result["code"]["number"], int)
        assert isinstance(result["code"]["list"], list)
