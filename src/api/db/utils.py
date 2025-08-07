from typing import List, Dict, Optional
import json
from enum import Enum
from api.config import courses_table_name
from api.utils.db import execute_db_operation


# Configuration for different integration block types
BLOCK_TYPE_CONFIG = {
    "paragraph": {
        "has_children": False
    },
    "heading_1": {
        "prefix": "# ",
        "has_children": False
    },
    "heading_2": {
        "prefix": "## ",
        "has_children": False
    },
    "heading_3": {
        "prefix": "### ",
        "has_children": False
    },
    "bulleted_list_item": {
        "prefix": "- ",
        "has_children": False
    },
    "numbered_list_item": {
        "prefix": "1. ",
        "has_children": False
    },
    "to_do": {
        "has_children": False,
        "custom_formatter": "checkbox"
    },
    "toggle": {
        "prefix": "▶ ",
        "has_children": True
    },
    "quote": {
        "prefix": "> ",
        "has_children": True
    },
    "callout": {
        "has_children": True,
        "custom_formatter": "callout"
    },
    "code": {
        "has_children": False,
        "custom_formatter": "code"
    },
    "bulleted_list": {
        "has_children": False,
        "custom_formatter": "list_items"
    },
    "numbered_list": {
        "has_children": False,
        "custom_formatter": "numbered_list_items"
    },
    "table": {
        "has_children": False,
        "custom_formatter": "table"
    },
    "column_list": {
        "has_children": True
    },
    "column": {
        "has_children": True
    }
}


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


async def get_org_id_for_course(course_id: int):
    course = await execute_db_operation(
        f"SELECT org_id FROM {courses_table_name} WHERE id = ?",
        (course_id,),
        fetch_one=True,
    )

    if not course:
        raise ValueError("Course not found")

    return course[0]


def convert_blocks_to_right_format(blocks: List[Dict]) -> List[Dict]:
    for block in blocks:
        for content in block["content"]:
            content["type"] = "text"
            if "styles" not in content:
                content["styles"] = {}

    return blocks


def _extract_text_from_rich_text(rich_text: List[Dict]) -> str:
    """
    Helper function to extract plain text from rich_text array.
    
    Args:
        rich_text: List of rich text objects
        
    Returns:
        Concatenated plain text from all rich text objects
    """
    return "".join([item.get("plain_text", "") for item in rich_text])


def _format_block_content(block: Dict, block_type: str, config: Dict) -> Optional[str]:
    """
    Generic function to format block content based on configuration.
    
    Args:
        block: The block dictionary
        block_type: Type of the block
        config: Configuration for the block type
        
    Returns:
        Formatted text content or None if no content
    """
    # Handle custom formatters first (they don't rely on main rich_text)
    custom_formatter = config.get("custom_formatter")
    
    # Get block data once
    block_data = block.get(block_type, {})
    if block_data is None:
        block_data = {}
    
    if custom_formatter == "list_items":
        items = block_data.get("items", [])
        formatted_items = []
        for item in items:
            item_text = _extract_text_from_rich_text(item.get("bulleted_list_item", {}).get("rich_text", []))
            if item_text:
                formatted_items.append(f"- {item_text}")
        return "\n".join(formatted_items)
    
    elif custom_formatter == "numbered_list_items":
        items = block_data.get("items", [])
        formatted_items = []
        for i, item in enumerate(items, 1):
            item_text = _extract_text_from_rich_text(item.get("numbered_list_item", {}).get("rich_text", []))
            if item_text:
                formatted_items.append(f"{i}. {item_text}")
        return "\n".join(formatted_items)
    
    elif custom_formatter == "table":
        table_rows = block_data.get("table_rows", [])
        formatted_rows = []
        for row in table_rows:
            cells = row.get("table_row", {}).get("cells", [])
            row_text = []
            for cell in cells:
                cell_text = _extract_text_from_rich_text(cell)
                if cell_text:
                    row_text.append(cell_text)
            if row_text:
                formatted_rows.append(" | ".join(row_text))
        return "\n".join(formatted_rows)
    
    # Get rich text from the block for other formatters
    rich_text = block_data.get("rich_text", [])
    text = _extract_text_from_rich_text(rich_text)
    
    if not text:
        return None
    
    # Handle other custom formatters
    if custom_formatter == "checkbox":
        checked = block_data.get("checked", False)
        checkbox = "[x]" if checked else "[ ]"
        return f"{checkbox} {text}"
    
    elif custom_formatter == "callout":
        icon_data = block_data.get("icon", {})
        if icon_data is not None:
            icon = icon_data.get("emoji", "💡")
        else:
            icon = "💡"
        return f"{icon} {text}"
    
    elif custom_formatter == "code":
        language = block_data.get("language", "")
        return f"```{language}\n{text}\n```"
    
    # Default formatting
    prefix = config.get("prefix", "")
    return f"{prefix}{text}"


def extract_text_from_notion_blocks(blocks: List[Dict]) -> str:
    """
    Extracts all text content from Notion blocks without media content.
    
    Args:
        blocks: A list of Notion block dictionaries
        
    Returns:
        A formatted string containing all text content from the blocks
    """
    if not blocks:
        return ""
    
    text_content = []
    
    for block in blocks:
        block_type = block.get("type", "")
        config = BLOCK_TYPE_CONFIG.get(block_type, {})
        
        # Helper function to process children with indentation
        def process_children(children: List[Dict]) -> None:
            if children:
                child_text = extract_text_from_notion_blocks(children)
                if child_text:
                    child_lines = child_text.split('\n')
                    indented_child = '\n'.join([f"  {line}" for line in child_lines if line.strip()])
                    text_content.append(indented_child)
        
        # Handle block content
        if block_type in BLOCK_TYPE_CONFIG:
            formatted_text = _format_block_content(block, block_type, config)
            if formatted_text:
                text_content.append(formatted_text)
            
            # Process children if the block type supports it
            if config.get("has_children", False):
                children = block.get(block_type, {}).get("children", [])
                process_children(children)
        
        # Handle any other block types that might have children
        elif "children" in block:
            process_children(block.get("children", []))
    
    return "\n".join(text_content)


def construct_description_from_blocks(
    blocks: List[Dict], nesting_level: int = 0
) -> str:
    """
    Constructs a textual description from a tree of block data.
    Handles both regular blocks and integration blocks.

    Args:
        blocks: A list of block dictionaries, potentially with nested children
        nesting_level: The current nesting level (used for proper indentation)

    Returns:
        A formatted string representing the content of the blocks
    """
    if not blocks:
        return ""

    description = ""
    indent = "    " * nesting_level  # 4 spaces per nesting level
    numbered_list_counter = 1  # Counter for numbered list items

    for block in blocks:
        block_type = block.get("type", "")
        content = block.get("content", [])
        children = block.get("children", [])

        # Handle integration blocks
        if block_type == "notion":
            if content:
                description += extract_text_from_notion_blocks(content)
            continue

        # Reset counter if we encounter a non-numbered list item after being in a numbered list
        if block_type != "numberedListItem" and numbered_list_counter > 1:
            numbered_list_counter = 1

        # Process based on block type
        if block_type == "paragraph":
            # Content is a list of text objects
            if isinstance(content, list):
                paragraph_text = ""
                for text_obj in content:
                    if isinstance(text_obj, dict) and "text" in text_obj:
                        paragraph_text += text_obj["text"]
                if paragraph_text:
                    description += f"{indent}{paragraph_text}\n"

        elif block_type == "heading":
            level = block.get("props", {}).get("level", 1)
            if isinstance(content, list):
                heading_text = ""
                for text_obj in content:
                    if isinstance(text_obj, dict) and "text" in text_obj:
                        heading_text += text_obj["text"]
                if heading_text:
                    # Headings are typically not indented, but we'll respect nesting for consistency
                    description += f"{indent}{'#' * level} {heading_text}\n"

        elif block_type == "codeBlock":
            language = block.get("props", {}).get("language", "")
            if isinstance(content, list):
                code_text = ""
                for text_obj in content:
                    if isinstance(text_obj, dict) and "text" in text_obj:
                        code_text += text_obj["text"]
                if code_text:
                    description += (
                        f"{indent}```{language}\n{indent}{code_text}\n{indent}```\n"
                    )

        elif block_type in ["numberedListItem", "checkListItem", "bulletListItem"]:
            if isinstance(content, list):
                item_text = ""
                for text_obj in content:
                    if isinstance(text_obj, dict) and "text" in text_obj:
                        item_text += text_obj["text"]

                if item_text:
                    # Use proper list marker based on parent list type
                    if block_type == "numberedListItem":
                        marker = f"{numbered_list_counter}. "
                        numbered_list_counter += 1
                    elif block_type == "checkListItem":
                        marker = "- [ ] "
                    elif block_type == "bulletListItem":
                        marker = "- "

                    description += f"{indent}{marker}{item_text}\n"

        if children:
            child_description = construct_description_from_blocks(
                children, nesting_level + 1
            )
            description += child_description

    return description
