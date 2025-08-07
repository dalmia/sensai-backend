import pytest
from unittest.mock import patch
from enum import Enum
from src.api.db.utils import (
    EnumEncoder,
    get_org_id_for_course,
    convert_blocks_to_right_format,
    _extract_text_from_rich_text,
    _format_block_content,
    extract_text_from_notion_blocks,
    construct_description_from_blocks,
    BLOCK_TYPE_CONFIG
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


class TestRichTextExtraction:
    """Test rich text extraction functions."""

    def test_extract_text_from_rich_text_simple(self):
        """Test extracting text from simple rich_text array."""
        rich_text = [
            {"plain_text": "Hello"},
            {"plain_text": " "},
            {"plain_text": "World"}
        ]
        result = _extract_text_from_rich_text(rich_text)
        assert result == "Hello World"

    def test_extract_text_from_rich_text_empty(self):
        """Test extracting text from empty rich_text array."""
        result = _extract_text_from_rich_text([])
        assert result == ""

    def test_extract_text_from_rich_text_missing_plain_text(self):
        """Test extracting text when plain_text is missing."""
        rich_text = [
            {"plain_text": "Hello"},
            {"other_field": "value"},
            {"plain_text": "World"}
        ]
        result = _extract_text_from_rich_text(rich_text)
        assert result == "HelloWorld"


class TestFormatBlockContent:
    """Test block content formatting functions."""

    def test_format_block_content_paragraph(self):
        """Test formatting paragraph block content."""
        block = {
            "paragraph": {
                "rich_text": [{"plain_text": "This is a paragraph"}]
            }
        }
        config = BLOCK_TYPE_CONFIG["paragraph"]
        result = _format_block_content(block, "paragraph", config)
        assert result == "This is a paragraph"

    def test_format_block_content_heading(self):
        """Test formatting heading block content."""
        block = {
            "heading_1": {
                "rich_text": [{"plain_text": "Main Heading"}]
            }
        }
        config = BLOCK_TYPE_CONFIG["heading_1"]
        result = _format_block_content(block, "heading_1", config)
        assert result == "# Main Heading"

    def test_format_block_content_checkbox(self):
        """Test formatting checkbox block content."""
        block = {
            "to_do": {
                "rich_text": [{"plain_text": "Complete task"}],
                "checked": True
            }
        }
        config = BLOCK_TYPE_CONFIG["to_do"]
        result = _format_block_content(block, "to_do", config)
        assert result == "[x] Complete task"

    def test_format_block_content_callout(self):
        """Test formatting callout block content."""
        block = {
            "callout": {
                "rich_text": [{"plain_text": "Important note"}],
                "icon": {"emoji": "⚠️"}
            }
        }
        config = BLOCK_TYPE_CONFIG["callout"]
        result = _format_block_content(block, "callout", config)
        assert result == "⚠️ Important note"

    def test_format_block_content_code(self):
        """Test formatting code block content."""
        block = {
            "code": {
                "rich_text": [{"plain_text": "print('hello')"}],
                "language": "python"
            }
        }
        config = BLOCK_TYPE_CONFIG["code"]
        result = _format_block_content(block, "code", config)
        assert result == "```python\nprint('hello')\n```"

    def test_format_block_content_no_text(self):
        """Test formatting block with no text content."""
        block = {
            "paragraph": {
                "rich_text": []
            }
        }
        config = BLOCK_TYPE_CONFIG["paragraph"]
        result = _format_block_content(block, "paragraph", config)
        assert result is None

    def test_format_block_content_with_none_block_data(self):
        """Test formatting block when block_data is None."""
        block = {
            "paragraph": None
        }
        config = BLOCK_TYPE_CONFIG["paragraph"]
        result = _format_block_content(block, "paragraph", config)
        assert result is None

    def test_format_block_content_callout_with_none_icon_data(self):
        """Test formatting callout block when icon_data is None."""
        block = {
            "callout": {
                "rich_text": [{"plain_text": "Important note"}],
                "icon": None
            }
        }
        config = BLOCK_TYPE_CONFIG["callout"]
        result = _format_block_content(block, "callout", config)
        assert result == "💡 Important note"

    def test_format_block_content_list_items(self):
        """Test formatting bulleted list items."""
        block = {
            "bulleted_list": {
                "items": [
                    {
                        "bulleted_list_item": {
                            "rich_text": [{"plain_text": "First item"}]
                        }
                    },
                    {
                        "bulleted_list_item": {
                            "rich_text": [{"plain_text": "Second item"}]
                        }
                    }
                ]
            }
        }
        config = BLOCK_TYPE_CONFIG["bulleted_list"]
        result = _format_block_content(block, "bulleted_list", config)
        assert result == "- First item\n- Second item"

    def test_format_block_content_numbered_list_items(self):
        """Test formatting numbered list items."""
        block = {
            "numbered_list": {
                "items": [
                    {
                        "numbered_list_item": {
                            "rich_text": [{"plain_text": "First item"}]
                        }
                    },
                    {
                        "numbered_list_item": {
                            "rich_text": [{"plain_text": "Second item"}]
                        }
                    }
                ]
            }
        }
        config = BLOCK_TYPE_CONFIG["numbered_list"]
        result = _format_block_content(block, "numbered_list", config)
        assert result == "1. First item\n2. Second item"

    def test_format_block_content_table(self):
        """Test formatting table content."""
        block = {
            "table": {
                "table_rows": [
                    {
                        "table_row": {
                            "cells": [
                                [{"plain_text": "Header 1"}],
                                [{"plain_text": "Header 2"}]
                            ]
                        }
                    },
                    {
                        "table_row": {
                            "cells": [
                                [{"plain_text": "Cell 1"}],
                                [{"plain_text": "Cell 2"}]
                            ]
                        }
                    }
                ]
            }
        }
        config = BLOCK_TYPE_CONFIG["table"]
        result = _format_block_content(block, "table", config)
        assert result == "Header 1 | Header 2\nCell 1 | Cell 2"

    def test_format_block_content_list_items_empty(self):
        """Test formatting bulleted list items with empty items."""
        block = {
            "bulleted_list": {
                "items": []
            }
        }
        config = BLOCK_TYPE_CONFIG["bulleted_list"]
        result = _format_block_content(block, "bulleted_list", config)
        assert result == ""

    def test_format_block_content_numbered_list_items_empty(self):
        """Test formatting numbered list items with empty items."""
        block = {
            "numbered_list": {
                "items": []
            }
        }
        config = BLOCK_TYPE_CONFIG["numbered_list"]
        result = _format_block_content(block, "numbered_list", config)
        assert result == ""

    def test_format_block_content_table_empty(self):
        """Test formatting table with empty rows."""
        block = {
            "table": {
                "table_rows": []
            }
        }
        config = BLOCK_TYPE_CONFIG["table"]
        result = _format_block_content(block, "table", config)
        assert result == ""

    def test_format_block_content_table_empty_cells(self):
        """Test formatting table with empty cells."""
        block = {
            "table": {
                "table_rows": [
                    {
                        "table_row": {
                            "cells": [
                                [],
                                [{"plain_text": "Only this cell"}]
                            ]
                        }
                    }
                ]
            }
        }
        config = BLOCK_TYPE_CONFIG["table"]
        result = _format_block_content(block, "table", config)
        assert result == "Only this cell"


class TestExtractTextFromNotionBlocks:
    """Test extraction of text from Notion blocks."""

    def test_extract_text_from_notion_blocks_empty(self):
        """Test extracting text from empty blocks."""
        result = extract_text_from_notion_blocks([])
        assert result == ""

    def test_extract_text_from_notion_blocks_simple(self):
        """Test extracting text from simple blocks."""
        blocks = [
            {
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"plain_text": "Hello World"}]
                }
            }
        ]
        result = extract_text_from_notion_blocks(blocks)
        assert result == "Hello World"

    def test_extract_text_from_notion_blocks_with_children(self):
        """Test extracting text from blocks with children."""
        blocks = [
            {
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"plain_text": "Toggle item"}],
                    "children": [
                        {
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [{"plain_text": "Child content"}]
                            }
                        }
                    ]
                }
            }
        ]
        result = extract_text_from_notion_blocks(blocks)
        assert "Toggle item" in result
        assert "Child content" in result

    def test_extract_text_from_notion_blocks_multiple_types(self):
        """Test extracting text from multiple block types."""
        blocks = [
            {
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"plain_text": "Title"}]
                }
            },
            {
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"plain_text": "Content"}]
                }
            },
            {
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": [{"plain_text": "List item"}]
                }
            }
        ]
        result = extract_text_from_notion_blocks(blocks)
        assert "# Title" in result
        assert "Content" in result
        assert "- List item" in result

    def test_extract_text_from_notion_blocks_unknown_type(self):
        """Test extracting text from unknown block types."""
        blocks = [
            {
                "type": "unknown_type",
                "children": [
                    {
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"plain_text": "Child content"}]
                        }
                    }
                ]
            }
        ]
        result = extract_text_from_notion_blocks(blocks)
        assert "Child content" in result


class TestConstructDescriptionFromBlocks:
    """Test description construction from blocks."""

    def test_construct_description_empty_blocks(self):
        """Test constructing description from empty blocks."""
        result = construct_description_from_blocks([])
        assert result == ""

    def test_construct_description_paragraph_block(self):
        """Test constructing description from paragraph block."""
        blocks = [
            {
                "type": "paragraph",
                "content": [{"text": "This is a paragraph"}]
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "This is a paragraph" in result

    def test_construct_description_heading_block(self):
        """Test constructing description from heading block."""
        blocks = [
            {
                "type": "heading",
                "content": [{"text": "Main Heading"}],
                "props": {"level": 2}
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "## Main Heading" in result

    def test_construct_description_code_block(self):
        """Test constructing description from code block."""
        blocks = [
            {
                "type": "codeBlock",
                "content": [{"text": "print('hello')"}],
                "props": {"language": "python"}
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "```python" in result
        assert "print('hello')" in result

    def test_construct_description_list_items(self):
        """Test constructing description from list items."""
        blocks = [
            {
                "type": "numberedListItem",
                "content": [{"text": "First item"}]
            },
            {
                "type": "bulletListItem",
                "content": [{"text": "Second item"}]
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "1. First item" in result
        assert "- Second item" in result

    def test_construct_description_checklist_item(self):
        """Test constructing description from checklist item."""
        blocks = [
            {
                "type": "checkListItem",
                "content": [{"text": "Checklist item"}]
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "- [ ] Checklist item" in result

    def test_construct_description_nested_blocks(self):
        """Test constructing description from nested blocks."""
        blocks = [
            {
                "type": "paragraph",
                "content": [{"text": "Parent"}],
                "children": [
                    {
                        "type": "paragraph",
                        "content": [{"text": "Child"}]
                    }
                ]
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "Parent" in result
        assert "Child" in result

    def test_construct_description_with_nesting_level(self):
        """Test constructing description with specific nesting level."""
        blocks = [
            {
                "type": "paragraph",
                "content": [{"text": "Level 0"}]
            }
        ]
        result = construct_description_from_blocks(blocks, nesting_level=2)
        assert "        Level 0" in result  # 8 spaces (2 * 4)

    def test_construct_description_mixed_content(self):
        """Test constructing description from mixed content types."""
        blocks = [
            {
                "type": "heading",
                "content": [{"text": "Title"}],
                "props": {"level": 1}
            },
            {
                "type": "paragraph",
                "content": [{"text": "Content"}]
            },
            {
                "type": "codeBlock",
                "content": [{"text": "code"}],
                "props": {"language": "python"}
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "# Title" in result
        assert "Content" in result
        assert "```python" in result

    def test_construct_description_notion_block(self):
        """Test constructing description from notion block type."""
        blocks = [
            {
                "type": "notion",
                "content": [
                    {
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"plain_text": "Notion content"}]
                        }
                    }
                ]
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "Notion content" in result

    def test_construct_description_notion_block_empty_content(self):
        """Test constructing description from notion block with empty content."""
        blocks = [
            {
                "type": "notion",
                "content": []
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert result == ""

    def test_construct_description_notion_block_with_multiple_content(self):
        """Test constructing description from notion block with multiple content items."""
        blocks = [
            {
                "type": "notion",
                "content": [
                    {
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": [{"plain_text": "Notion Heading"}]
                        }
                    },
                    {
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"plain_text": "Notion paragraph"}]
                        }
                    }
                ]
            }
        ]
        result = construct_description_from_blocks(blocks)
        assert "# Notion Heading" in result
        assert "Notion paragraph" in result
