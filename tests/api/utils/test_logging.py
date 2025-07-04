import pytest
import logging
from unittest.mock import patch, MagicMock, call
from src.api.utils.logging import setup_logging


class TestLoggingUtils:
    @patch("src.api.utils.logging.logging")
    def test_setup_logging(self, mock_logging):
        """Test the setup_logging function."""
        # Setup mocks
        mock_logger = MagicMock()
        mock_file_handler = MagicMock()
        mock_console_handler = MagicMock()
        mock_formatter = MagicMock()

        mock_logging.getLogger.return_value = mock_logger
        mock_logging.FileHandler.return_value = mock_file_handler
        mock_logging.StreamHandler.return_value = mock_console_handler
        mock_logging.Formatter.return_value = mock_formatter
        mock_logging.INFO = logging.INFO  # Use the actual INFO value

        # Call the function
        logger = setup_logging("/path/to/log.log")

        # Check results
        mock_logging.getLogger.assert_called_once_with("src.api.utils.logging")
        mock_logger.setLevel.assert_called_once_with(logging.INFO)

        # Check file handler setup
        mock_logging.FileHandler.assert_called_once_with("/path/to/log.log")
        mock_file_handler.setLevel.assert_called_once_with(logging.INFO)

        # Check console handler setup
        mock_logging.StreamHandler.assert_called_once()
        mock_console_handler.setLevel.assert_called_once_with(logging.INFO)

        # Check formatter setup
        mock_logging.Formatter.assert_called_once_with(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        mock_console_handler.setFormatter.assert_called_once_with(mock_formatter)
        mock_file_handler.setFormatter.assert_called_once_with(mock_formatter)

        # Check that handlers were added to the logger
        # Note: In the actual code, only the file handler is added, not the console handler
        mock_logger.addHandler.assert_called_once_with(mock_file_handler)

        # Check that the function returns the logger
        assert logger == mock_logger
