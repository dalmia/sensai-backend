from typing import List, Dict
import json
from enum import Enum
from api.config import courses_table_name
from api.utils.db import execute_db_operation


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


def construct_description_from_blocks(
    blocks: List[Dict], nesting_level: int = 0
) -> str:
    """
    Constructs a textual description from a tree of block data.

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

    for block in blocks:
        block_type = block.get("type", "")
        content = block.get("content", [])
        children = block.get("children", [])

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
                        marker = "1. "
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
