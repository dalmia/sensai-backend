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
        
        # Handle different Notion block types
        if block_type == "paragraph":
            rich_text = block.get("paragraph", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(text)
                
        elif block_type == "heading_1":
            rich_text = block.get("heading_1", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"# {text}")
                
        elif block_type == "heading_2":
            rich_text = block.get("heading_2", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"## {text}")
                
        elif block_type == "heading_3":
            rich_text = block.get("heading_3", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"### {text}")
                
        elif block_type == "bulleted_list_item":
            rich_text = block.get("bulleted_list_item", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"- {text}")
                
        elif block_type == "numbered_list_item":
            rich_text = block.get("numbered_list_item", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"1. {text}")
                
        elif block_type == "to_do":
            rich_text = block.get("to_do", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            checked = block.get("to_do", {}).get("checked", False)
            if text:
                checkbox = "[x]" if checked else "[ ]"
                text_content.append(f"{checkbox} {text}")
                
        elif block_type == "toggle":
            rich_text = block.get("toggle", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"▶ {text}")
                
        elif block_type == "quote":
            rich_text = block.get("quote", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                text_content.append(f"> {text}")
                
        elif block_type == "callout":
            rich_text = block.get("callout", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            if text:
                icon = block.get("callout", {}).get("icon", {}).get("emoji", "💡")
                text_content.append(f"{icon} {text}")
                
        elif block_type == "code":
            rich_text = block.get("code", {}).get("rich_text", [])
            text = "".join([item.get("plain_text", "") for item in rich_text])
            language = block.get("code", {}).get("language", "")
            if text:
                text_content.append(f"```{language}\n{text}\n```")
                
        elif block_type == "divider":
            text_content.append("---")
            
        elif block_type == "table_of_contents":
            text_content.append("[Table of Contents]")
            
        elif block_type == "breadcrumb":
            text_content.append("[Breadcrumb]")
            
        elif block_type == "column_list":
            # Handle column content recursively
            children = block.get("children", [])
            if children:
                child_text = extract_text_from_notion_blocks(children)
                if child_text:
                    text_content.append(child_text)
                    
        elif block_type == "column":
            # Handle column content recursively
            children = block.get("children", [])
            if children:
                child_text = extract_text_from_notion_blocks(children)
                if child_text:
                    text_content.append(child_text)
        
        # Handle any other block types that might have children
        elif "children" in block:
            children = block.get("children", [])
            if children:
                child_text = extract_text_from_notion_blocks(children)
                if child_text:
                    text_content.append(child_text)
    
    return "\n".join(text_content)


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
    numbered_list_counter = 1  # Counter for numbered list items

    for block in blocks:
        block_type = block.get("type", "")
        content = block.get("content", [])
        children = block.get("children", [])

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
