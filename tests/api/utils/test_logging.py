import pytest
import logging
from unittest.mock import patch, MagicMock, call
from src.api.utils.logging import setup_logging


class TestLoggingUtils:
    @patch("src.api.utils.logging.logging")
    def test_setup_logging(self, mock_logging):
        """Test the setup_logging function."""
        # Setup mocks
        mock_root_logger = MagicMock()
        mock_file_handler = MagicMock()
        mock_formatter = MagicMock()
        mock_root_logger.handlers = []  # Empty handlers list

        # Create mock loggers for libraries
        mock_httpx_logger = MagicMock()
        mock_httpcore_logger = MagicMock()
        mock_urllib3_logger = MagicMock()

        def getLogger_side_effect(name=None):
            if name is None:
                return mock_root_logger
            elif name == "httpx":
                return mock_httpx_logger
            elif name == "httpcore":
                return mock_httpcore_logger
            elif name == "urllib3":
                return mock_urllib3_logger
            return MagicMock()

        mock_logging.getLogger.side_effect = getLogger_side_effect
        mock_logging.handlers.RotatingFileHandler.return_value = mock_file_handler
        mock_logging.Formatter.return_value = mock_formatter
        mock_logging.INFO = logging.INFO  # Use the actual INFO value
        mock_logging.WARNING = logging.WARNING

        # Call the function
        logger = setup_logging("/path/to/log.log")

        # Check results - root logger was obtained and set to INFO level
        assert mock_logging.getLogger.called
        mock_root_logger.setLevel.assert_called_once_with(logging.INFO)

        # Check that library loggers were set to WARNING
        mock_httpx_logger.setLevel.assert_called_once_with(logging.WARNING)
        mock_httpcore_logger.setLevel.assert_called_once_with(logging.WARNING)
        mock_urllib3_logger.setLevel.assert_called_once_with(logging.WARNING)

        # Check formatter setup - now includes datefmt
        mock_logging.Formatter.assert_called_once_with(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        mock_file_handler.setFormatter.assert_called_once_with(mock_formatter)

        # Check that handlers were added to the root logger
        mock_root_logger.addHandler.assert_called_once_with(mock_file_handler)

        # Check that the function returns the root logger
        assert logger == mock_root_logger

    @patch("src.api.utils.logging.logging")
    def test_setup_logging_with_console_logging(self, mock_logging):
        """Test the setup_logging function with console logging enabled."""
        # Setup mocks
        mock_root_logger = MagicMock()
        mock_file_handler = MagicMock()
        mock_formatter = MagicMock()
        mock_root_logger.handlers = []  # Empty handlers list

        # Create mock loggers for libraries
        mock_httpx_logger = MagicMock()
        mock_httpcore_logger = MagicMock()
        mock_urllib3_logger = MagicMock()

        def getLogger_side_effect(name=None):
            if name is None:
                return mock_root_logger
            elif name == "httpx":
                return mock_httpx_logger
            elif name == "httpcore":
                return mock_httpcore_logger
            elif name == "urllib3":
                return mock_urllib3_logger
            return MagicMock()

        mock_logging.getLogger.side_effect = getLogger_side_effect
        mock_logging.handlers.RotatingFileHandler.return_value = mock_file_handler
        mock_logging.Formatter.return_value = mock_formatter
        mock_logging.INFO = logging.INFO  # Use the actual INFO value
        mock_logging.WARNING = logging.WARNING

        # Call the function with console logging enabled
        logger = setup_logging("/path/to/log.log", enable_console_logging=True)

        # Check results - root logger was obtained and set to INFO level
        assert mock_logging.getLogger.called
        mock_root_logger.setLevel.assert_called_once_with(logging.INFO)

        # Check that library loggers were set to WARNING
        mock_httpx_logger.setLevel.assert_called_once_with(logging.WARNING)
        mock_httpcore_logger.setLevel.assert_called_once_with(logging.WARNING)
        mock_urllib3_logger.setLevel.assert_called_once_with(logging.WARNING)

        # Check formatter setup - now includes datefmt
        mock_logging.Formatter.assert_called_once_with(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        mock_file_handler.setFormatter.assert_called_once_with(mock_formatter)

        # Check that handlers were added to the root logger
        mock_root_logger.addHandler.assert_called_once_with(mock_file_handler)

        # Check that the function returns the root logger
        assert logger == mock_root_logger
