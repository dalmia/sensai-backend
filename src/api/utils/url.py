import streamlit as st
import re
import os
from typing import Dict
from unidecode import unidecode


def slugify(text: str) -> str:
    """
    Convert a string to a URL-friendly slug.
    Example: "Hello World! How are you?" -> "hello-world-how-are-you"

    Args:
        text: The string to convert

    Returns:
        A URL-friendly version of the string
    """
    # Convert to lowercase and normalize unicode characters
    text = str(text).lower()
    text = unidecode(text)

    # Replace any non-alphanumeric character with a hyphen
    text = re.sub(r"[^\w\s-]", "", text)

    # Replace all spaces or repeated hyphens with a single hyphen
    text = re.sub(r"[-\s]+", "-", text.strip())

    return text


def get_home_url(params: Dict[str, str] = None) -> str:
    home_page_url = os.environ.get("APP_URL")

    if params:
        query_params = "&".join([f"{key}={value}" for key, value in params.items()])
        home_page_url += f"?{query_params}"

    return home_page_url
