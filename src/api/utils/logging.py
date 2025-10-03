import logging
import logging.handlers
import sys
from api.config import log_file_path, db_log_file_path


def setup_logging(
    log_file_path: str, enable_console_logging: bool = True, log_level: str = "INFO"
):
    """
    Set up file and console logging for FastAPI application.

    Args:
        log_file_path: Path to the log file
        enable_console_logging: Whether to also output logs to console (not needed if uvicorn handles it)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure the root logger to capture all logs
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Create formatter with detailed information
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Only add file handler if not already present (avoid duplicates on reload)
    has_file_handler = any(
        isinstance(h, logging.handlers.RotatingFileHandler)
        and h.baseFilename == log_file_path
        for h in root_logger.handlers
    )

    if not has_file_handler:
        # File handler with rotation to prevent huge log files
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Configure specific loggers to prevent excessive logging from some libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return root_logger


# Initialize file logging only (uvicorn will handle console logging)
logger = setup_logging(log_file_path, enable_console_logging=False)

db_logger = setup_logging(db_log_file_path, enable_console_logging=False)
