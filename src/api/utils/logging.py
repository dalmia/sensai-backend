import logging
import logging.handlers
import sys
from api.config import log_file_path


def setup_logging(
    log_file_path: str, enable_console_logging: bool = True, log_level: str = "INFO"
):
    """
    Set up comprehensive logging for FastAPI application.
    This captures all logs from FastAPI, uvicorn, and application modules.

    Args:
        log_file_path: Path to the log file
        enable_console_logging: Whether to also output logs to console
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure the root logger to capture all logs
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear any existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Create formatter with more detailed information
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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

    # Console handler if requested
    if enable_console_logging:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Configure specific loggers to prevent excessive logging from some libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Set uvicorn loggers to use our handlers
    uvicorn_loggers = ["uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]

    for logger_name in uvicorn_loggers:
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True  # Allow propagation to root logger

    return root_logger


# Initialize comprehensive logging
logger = setup_logging(log_file_path, enable_console_logging=True)
