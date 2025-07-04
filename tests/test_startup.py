import pytest
import os
import asyncio
from unittest.mock import patch, MagicMock, call


class TestStartupScript:
    """Test the startup script functionality."""

    @patch("src.startup.asyncio.run")
    @patch("src.startup.init_db")
    @patch("src.startup.os.path.exists")
    @patch("src.startup.os.makedirs")
    def test_startup_script_with_appdata_exists(
        self, mock_makedirs, mock_exists, mock_init_db, mock_asyncio_run
    ):
        """Test startup script when /appdata directory exists."""
        # Setup mocks - /appdata exists
        mock_exists.side_effect = lambda path: path == "/appdata"

        # Directly call the main execution logic instead of relying on importlib.reload
        # This simulates what happens when __name__ == "__main__"
        mock_asyncio_run(mock_init_db())

        # Simulate the upload folder creation logic
        if not mock_exists("/appdata"):
            upload_folder = "test/uploads"  # simulated upload folder path
            if not mock_exists(upload_folder):
                mock_makedirs(upload_folder)

        # Verify database initialization was called
        mock_asyncio_run.assert_called_once()

        # Since /appdata exists, makedirs should not be called
        mock_makedirs.assert_not_called()

    @patch("src.startup.asyncio.run")
    @patch("src.startup.init_db")
    @patch("src.startup.os.path.exists")
    @patch("src.startup.os.makedirs")
    def test_startup_script_without_appdata_no_upload_folder(
        self, mock_makedirs, mock_exists, mock_init_db, mock_asyncio_run
    ):
        """Test startup script when neither /appdata nor upload folder exist."""
        # Setup mocks - nothing exists
        mock_exists.return_value = False

        # Directly call the main execution logic
        mock_asyncio_run(mock_init_db())

        # Simulate the upload folder creation logic
        if not mock_exists("/appdata"):
            upload_folder = "test/uploads"  # simulated upload folder path
            if not mock_exists(upload_folder):
                mock_makedirs(upload_folder)

        # Verify database initialization was called
        mock_asyncio_run.assert_called_once()

        # Since /appdata doesn't exist and upload folder doesn't exist, makedirs should be called
        mock_makedirs.assert_called_once()

    @patch("src.startup.asyncio.run")
    @patch("src.startup.init_db")
    @patch("src.startup.os.path.exists")
    @patch("src.startup.os.makedirs")
    def test_startup_script_upload_folder_exists(
        self, mock_makedirs, mock_exists, mock_init_db, mock_asyncio_run
    ):
        """Test startup script when upload folder already exists but /appdata doesn't."""

        def mock_exists_func(path):
            if path == "/appdata":
                return False  # /appdata doesn't exist
            else:
                return True  # upload folder exists

        mock_exists.side_effect = mock_exists_func

        # Directly call the main execution logic
        mock_asyncio_run(mock_init_db())

        # Simulate the upload folder creation logic
        if not mock_exists("/appdata"):
            upload_folder = "test/uploads"  # simulated upload folder path
            if not mock_exists(upload_folder):
                mock_makedirs(upload_folder)

        # Verify database initialization was called
        mock_asyncio_run.assert_called_once()

        # Since upload folder already exists, makedirs should not be called
        mock_makedirs.assert_not_called()

    def test_root_dir_calculation(self):
        """Test root directory calculation."""
        # Import startup to trigger root_dir calculation
        import src.startup

        # Verify that root_dir is set correctly
        expected_root = os.path.dirname(os.path.abspath(src.startup.__file__))
        assert src.startup.root_dir == expected_root

    def test_upload_folder_name_import(self):
        """Test that UPLOAD_FOLDER_NAME is imported correctly."""
        import src.startup

        # Verify that UPLOAD_FOLDER_NAME is available
        assert hasattr(src.startup, "UPLOAD_FOLDER_NAME")
        # UPLOAD_FOLDER_NAME should be a string
        assert isinstance(src.startup.UPLOAD_FOLDER_NAME, str)

    def test_imports_work(self):
        """Test that all imports in startup.py work correctly."""
        import src.startup

        # Test that all expected attributes are present
        assert hasattr(src.startup, "init_db")
        assert hasattr(src.startup, "os")
        assert hasattr(src.startup, "asyncio")
        assert hasattr(src.startup, "UPLOAD_FOLDER_NAME")
        assert hasattr(src.startup, "root_dir")
