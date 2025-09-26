import os
from os.path import join
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from functools import lru_cache
from api.config import UPLOAD_FOLDER_NAME

root_dir = os.path.dirname(os.path.abspath(__file__))
env_path = join(root_dir, ".env.aws")
if os.path.exists(env_path):
    load_dotenv(env_path)


class Settings(BaseSettings):
    google_client_id: str | None = None

    google_application_credentials: str | None = None
    bq_project_name: str | None = None
    bq_dataset_name: str | None = None

    openai_api_key: str | None = None
    s3_bucket_name: str | None = None  # only relevant when running the code remotely
    s3_folder_name: str | None = None  # only relevant when running the code remotely
    local_upload_folder: str = (
        UPLOAD_FOLDER_NAME  # hardcoded variable for local file storage
    )
    bugsnag_api_key: str | None = None
    env: str | None = None
    slack_user_signup_webhook_url: str | None = None
    slack_course_created_webhook_url: str | None = None
    slack_usage_stats_webhook_url: str | None = None
    slack_alert_webhook_url: str | None = None
    langfuse_secret_key: str | None = None
    langfuse_public_key: str | None = None
    langfuse_host: str | None = None
    langfuse_tracing_environment: str | None = None

    model_config = SettingsConfigDict(env_file=join(root_dir, ".env"))


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()

os.environ["LANGFUSE_PUBLIC_KEY"] = settings.langfuse_public_key
os.environ["LANGFUSE_SECRET_KEY"] = settings.langfuse_secret_key
os.environ["LANGFUSE_HOST"] = settings.langfuse_host
os.environ["LANGFUSE_TRACING_ENVIRONMENT"] = settings.langfuse_tracing_environment
os.environ["OPENAI_API_KEY"] = settings.openai_api_key
