import logging
from api.config import log_file_path


def setup_logging(log_file_path: str):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Add a StreamHandler to output logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Add a FileHandler to write logs to app.log
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    # logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging(log_file_path)
