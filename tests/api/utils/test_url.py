import pytest
import os
from unittest.mock import patch
from src.api.utils.url import slugify, get_home_url


class TestSlugify:
    def test_basic_slugify(self):
        """Test basic slugify functionality with a simple string."""
        result = slugify("Hello World")
        assert result == "hello-world"

    def test_slugify_special_chars(self):
        """Test slugify with special characters."""
        result = slugify("Hello World! How are you?")
        assert result == "hello-world-how-are-you"

    def test_slugify_extra_spaces(self):
        """Test slugify with extra spaces."""
        result = slugify("  Hello  World  ")
        assert result == "hello-world"

    def test_slugify_repeated_hyphens(self):
        """Test slugify with repeated hyphens."""
        result = slugify("Hello--World")
        assert result == "hello-world"

    def test_slugify_unicode_chars(self):
        """Test slugify with unicode characters."""
        result = slugify("Héllö Wörld")
        assert result == "hello-world"

    def test_slugify_numbers(self):
        """Test slugify with numbers."""
        result = slugify("Hello 123 World")
        assert result == "hello-123-world"

    def test_slugify_empty_string(self):
        """Test slugify with an empty string."""
        result = slugify("")
        assert result == ""


class TestGetHomeUrl:
    @patch.dict(os.environ, {"APP_URL": "https://example.com"})
    def test_get_home_url_no_params(self):
        """Test get_home_url with no parameters."""
        result = get_home_url()
        assert result == "https://example.com"

    @patch.dict(os.environ, {"APP_URL": "https://example.com"})
    def test_get_home_url_with_params(self):
        """Test get_home_url with parameters."""
        params = {"param1": "value1", "param2": "value2"}
        result = get_home_url(params)

        # The order of parameters might vary, so we need to check both possibilities
        possible_urls = [
            "https://example.com?param1=value1&param2=value2",
            "https://example.com?param2=value2&param1=value1",
        ]
        assert result in possible_urls

    @patch.dict(os.environ, {"APP_URL": "https://example.com/"})
    def test_get_home_url_trailing_slash(self):
        """Test get_home_url with trailing slash in APP_URL."""
        params = {"param": "value"}
        result = get_home_url(params)
        assert result == "https://example.com/?param=value"
