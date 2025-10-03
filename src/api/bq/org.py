from google.cloud import bigquery
import hashlib
from api.settings import settings
from api.config import org_api_keys_table_name
from api.bq.base import get_bq_client


async def get_org_id_from_api_key(api_key: str) -> int:
    bq_client = get_bq_client()

    api_key_parts = api_key.split("__")

    if len(api_key_parts) < 3:
        raise ValueError("Invalid API key")

    try:
        org_id = int(api_key_parts[1])
    except ValueError:
        raise ValueError("Invalid API key")

    # Hash the full API key
    hashed_key = hashlib.sha256(api_key.encode()).hexdigest()

    query = f"""
        SELECT hashed_key
        FROM `{settings.bq_project_name}.{settings.bq_dataset_name}.{org_api_keys_table_name}`
        WHERE org_id = @org_id AND created_at > DATETIME('2024-01-01 00:00:00')
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("org_id", "INT64", org_id)]
    )

    query_job = bq_client.query(query, job_config=job_config)
    rows = list(query_job.result())

    if not rows:
        raise ValueError("Invalid API key")

    for row in rows:
        if hashed_key == row["hashed_key"]:
            return org_id

    raise ValueError("Invalid API key")
