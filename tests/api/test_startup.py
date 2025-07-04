import pytest
import os
from src import startup


class TestStartup:
    """Test startup script functionality."""

    def test_startup_module_import(self):
        """Test that startup module can be imported without error."""
        # Verify required imports are available
        assert hasattr(startup, "init_db")
        assert hasattr(startup, "os")
        assert hasattr(startup, "asyncio")
        assert hasattr(startup, "UPLOAD_FOLDER_NAME")
        assert hasattr(startup, "root_dir")

    def test_startup_constants(self):
        """Test that startup module has required constants."""
        # Verify root_dir is set
        assert hasattr(startup, "root_dir")
        assert isinstance(startup.root_dir, str)

    def test_upload_folder_name_constant(self):
        """Test that upload folder name constant is available."""
        assert hasattr(startup, "UPLOAD_FOLDER_NAME")
        assert startup.UPLOAD_FOLDER_NAME == "uploads"

    def test_os_module_available(self):
        """Test that os module functions are available."""
        assert hasattr(startup, "os")
        assert hasattr(startup.os, "path")
        assert hasattr(startup.os.path, "exists")
        assert hasattr(startup.os.path, "join")
        assert hasattr(startup.os, "makedirs")

    def test_asyncio_module_available(self):
        """Test that asyncio module is available."""
        assert hasattr(startup, "asyncio")

    def test_init_db_function_available(self):
        """Test that init_db function is available."""
        assert hasattr(startup, "init_db")
        assert callable(startup.init_db)
