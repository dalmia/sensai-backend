import pytest
from unittest.mock import patch
from enum import Enum
from src.api.db.utils import (
    get_org_id_for_course,
    convert_blocks_to_right_format,
    construct_description_from_blocks,
    EnumEncoder,
)


class TestEnumEncoder:
    """Test the EnumEncoder class."""

    def test_enum_encoder_with_enum(self):
        """Test encoding enum values."""

        class TestEnum(Enum):
            VALUE1 = "test_value"
            VALUE2 = 42

        encoder = EnumEncoder()

        assert encoder.default(TestEnum.VALUE1) == "test_value"
        assert encoder.default(TestEnum.VALUE2) == 42

    def test_enum_encoder_with_non_enum(self):
        """Test encoding non-enum values raises TypeError."""
        encoder = EnumEncoder()

        with pytest.raises(TypeError):
            encoder.default("not an enum")


@pytest.mark.asyncio
class TestOrgIdForCourse:
    """Test getting organization ID for course."""

    @patch("src.api.db.utils.execute_db_operation")
    async def test_get_org_id_for_course_success(self, mock_execute):
        """Test successful retrieval of org ID for course."""
        mock_execute.return_value = (123,)

        result = await get_org_id_for_course(1)

        assert result == 123
        mock_execute.assert_called_once_with(
            "SELECT org_id FROM courses WHERE id = ?", (1,), fetch_one=True
        )

    @patch("src.api.db.utils.execute_db_operation")
    async def test_get_org_id_for_course_not_found(self, mock_execute):
        """Test error when course not found."""
        mock_execute.return_value = None

        with pytest.raises(ValueError, match="Course not found"):
            await get_org_id_for_course(999)


class TestBlocksConversion:
    """Test blocks conversion functions."""

    def test_convert_blocks_to_right_format(self):
        """Test converting blocks to right format."""
        blocks = [
            {
                "content": [
                    {"text": "Hello"},
                    {"text": "World", "styles": {"bold": True}},
                ]
            }
        ]

        result = convert_blocks_to_right_format(blocks)

        expected = [
            {
                "content": [
                    {"text": "Hello", "type": "text", "styles": {}},
                    {"text": "World", "type": "text", "styles": {"bold": True}},
                ]
            }
        ]

        assert result == expected

    def test_convert_blocks_empty_list(self):
        """Test converting empty blocks list."""
        result = convert_blocks_to_right_format([])
        assert result == []

    def test_convert_blocks_with_existing_styles(self):
        """Test converting blocks that already have styles."""
        blocks = [{"content": [{"text": "Test", "styles": {"italic": True}}]}]

        result = convert_blocks_to_right_format(blocks)

        assert result[0]["content"][0]["styles"] == {"italic": True}
        assert result[0]["content"][0]["type"] == "text"


class TestDescriptionConstruction:
    """Test description construction from blocks."""

    def test_construct_description_empty_blocks(self):
        """Test constructing description from empty blocks."""
        result = construct_description_from_blocks([])
        assert result == ""

    def test_construct_description_paragraph_block(self):
        """Test constructing description from paragraph block."""
        blocks = [{"type": "paragraph", "content": [{"text": "This is a paragraph."}]}]

        result = construct_description_from_blocks(blocks)
        assert result == "This is a paragraph.\n"

    def test_construct_description_heading_block(self):
        """Test constructing description from heading block."""
        blocks = [
            {
                "type": "heading",
                "content": [{"text": "Main Heading"}],
                "props": {"level": 1},
            }
        ]

        result = construct_description_from_blocks(blocks)
        assert result == "# Main Heading\n"

    def test_construct_description_code_block(self):
        """Test constructing description from code block."""
        blocks = [
            {
                "type": "codeBlock",
                "content": [{"text": "print('Hello World')"}],
                "props": {"language": "python"},
            }
        ]

        result = construct_description_from_blocks(blocks)
        assert result == "```python\nprint('Hello World')\n```\n"

    def test_construct_description_list_items(self):
        """Test constructing description from list items."""
        blocks = [
            {"type": "numberedListItem", "content": [{"text": "First item"}]},
            {"type": "bulletListItem", "content": [{"text": "Bullet item"}]},
            {"type": "checkListItem", "content": [{"text": "Check item"}]},
        ]

        result = construct_description_from_blocks(blocks)
        expected = "1. First item\n- Bullet item\n- [ ] Check item\n"
        assert result == expected

    def test_construct_description_nested_blocks(self):
        """Test constructing description with nested blocks."""
        blocks = [
            {
                "type": "paragraph",
                "content": [{"text": "Parent paragraph"}],
                "children": [
                    {"type": "paragraph", "content": [{"text": "Nested paragraph"}]}
                ],
            }
        ]

        result = construct_description_from_blocks(blocks)
        expected = "Parent paragraph\n    Nested paragraph\n"
        assert result == expected

    def test_construct_description_with_nesting_level(self):
        """Test constructing description with custom nesting level."""
        blocks = [{"type": "paragraph", "content": [{"text": "Indented paragraph"}]}]

        result = construct_description_from_blocks(blocks, nesting_level=2)
        assert result == "        Indented paragraph\n"

    def test_construct_description_mixed_content(self):
        """Test constructing description with mixed content types."""
        blocks = [
            {
                "type": "heading",
                "content": [{"text": "Introduction"}],
                "props": {"level": 2},
            },
            {"type": "paragraph", "content": [{"text": "This is some text."}]},
            {
                "type": "codeBlock",
                "content": [{"text": "code here"}],
                "props": {"language": ""},
            },
        ]

        result = construct_description_from_blocks(blocks)
        expected = "## Introduction\nThis is some text.\n```\ncode here\n```\n"
        assert result == expected
