from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from typing import List, Dict, Any, Optional, Callable, Awaitable
from datetime import datetime, timedelta
import asyncio
import time
from api.settings import settings
from api.utils.db import get_new_db_connection
from api.config import (
    org_api_keys_table_name,
    courses_table_name,
    milestones_table_name,
    course_tasks_table_name,
    course_milestones_table_name,
    organizations_table_name,
    scorecards_table_name,
    question_scorecards_table_name,
    task_completions_table_name,
    chat_history_table_name,
    users_table_name,
    tasks_table_name,
    questions_table_name,
    bq_sync_table_name,
)
from api.utils.logging import logger
from api.bq.base import get_bq_client


# -----------------------------
# Diff-sync helpers (BigQuery)
# -----------------------------

def _format_sqlite_datetime(dt: datetime) -> str:
    """Format a Python datetime into SQLite-compatible string (UTC naive)."""
    # Ensure naive (SQLite stores DATETIME strings without timezone)
    if dt.tzinfo is not None:
        dt = dt.astimezone(tz=None).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _get_last_activity_timestamp(
    bq_client: bigquery.Client, table_id: str
) -> Optional[datetime]:
    """
    Return the maximum activity timestamp from BigQuery table across
    updated_at, deleted_at, created_at. Handles TIMESTAMP/DATETIME types.

    If table does not exist or has no such fields/rows, returns None.
    """
    try:
        table = bq_client.get_table(table_id)
    except NotFound:
        return None

    # Collect timestampish fields present in the destination table
    dest_fields = {field.name for field in table.schema}
    candidates = [f for f in ("updated_at", "deleted_at", "created_at") if f in dest_fields]
    if not candidates:
        return None

    # Build a GREATEST() expression over normalized TIMESTAMPs for present fields
    parts = []
    for field_name in candidates:
        # Normalize to TIMESTAMP regardless of source field type (TIMESTAMP or DATETIME)
        # If SAFE_CAST to TIMESTAMP works (field is TIMESTAMP), use it; otherwise convert DATETIME using UTC
        normalized = (
            f"(CASE WHEN SAFE_CAST({field_name} AS TIMESTAMP) IS NOT NULL "
            f"THEN SAFE_CAST({field_name} AS TIMESTAMP) ELSE TIMESTAMP({field_name}, 'UTC') END)"
        )
        # Ensure NULL-safe by providing a very old default timestamp
        parts.append(
            f"COALESCE({normalized}, TIMESTAMP('1970-01-01 00:00:00 UTC'))"
        )

    greatest_expr = "GREATEST(" + ", ".join(parts) + ")"
    query = f"SELECT MAX({greatest_expr}) AS last_ts FROM `{table_id}`"

    job = _run_query_with_retry(bq_client, query)
    result = list(job.result())
    if not result:
        return None
    last_ts = result[0].get("last_ts") if isinstance(result[0], dict) else getattr(result[0], "last_ts", None)
    return last_ts


def _get_metadata_table_id() -> str:
    """Return the fully qualified table id for sync metadata tracking."""
    return f"{settings.bq_project_name}.{settings.bq_dataset_name}._sync_metadata"


def _ensure_metadata_table(bq_client: bigquery.Client) -> None:
    """Create sync metadata table if it does not exist."""
    table_id = _get_metadata_table_id()
    try:
        bq_client.get_table(table_id)
        return
    except NotFound:
        schema = [
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("last_synced_at", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)


def _get_last_sync_ts_from_metadata(
    bq_client: bigquery.Client, table_name: str
) -> Optional[datetime]:
    """Return last synced activity timestamp for a table from metadata."""
    table_id = _get_metadata_table_id()
    try:
        bq_client.get_table(table_id)
    except NotFound:
        return None

    query = (
        f"SELECT last_synced_at AS ts FROM `{table_id}` "
        f"WHERE table_name = @table_name LIMIT 1"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table_name)]
    )
    rows = list(bq_client.query(query, job_config=job_config).result())
    if not rows:
        return None
    row0 = rows[0]
    return row0.get("ts") if isinstance(row0, dict) else getattr(row0, "ts", None)


def _update_sync_ts_in_metadata(
    bq_client: bigquery.Client, table_name: str, last_synced_at: datetime
) -> None:
    """Upsert last_synced_at for a table into metadata."""
    table_id = _get_metadata_table_id()
    _ensure_metadata_table(bq_client)

    merge_sql = f"
        MERGE `{table_id}` T
        USING (SELECT @table_name AS table_name, @tsa AS last_synced_at) S
        ON T.table_name = S.table_name
        WHEN MATCHED THEN UPDATE SET T.last_synced_at = S.last_synced_at
        WHEN NOT MATCHED THEN INSERT (table_name, last_synced_at) VALUES (S.table_name, S.last_synced_at)
    "
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("tsa", "TIMESTAMP", last_synced_at),
        ]
    )
    job = _run_query_with_retry(bq_client, merge_sql, job_config=job_config)
    job.result()


def _adjust_since_for_overlap(last_ts: Optional[datetime]) -> Optional[datetime]:
    """Subtract a small buffer to avoid missing rows with identical timestamps."""
    if not last_ts:
        return None
    return last_ts - timedelta(seconds=1)


def _build_staging_table_id(table_id: str) -> str:
    """Return a staging table id in the same dataset as the destination table."""
    # table_id format: project.dataset.table
    project, dataset, table = table_id.split(".")
    staging_table = f"_staging_{table}"
    return f"{project}.{dataset}.{staging_table}"


def _recreate_staging_with_dest_schema(
    bq_client: bigquery.Client, dest_table_id: str, staging_table_id: str
) -> None:
    """Drop and recreate the staging table with the exact schema of the destination table."""
    # Drop existing staging table if present
    try:
        bq_client.delete_table(staging_table_id, not_found_ok=True)  # type: ignore[arg-type]
    except TypeError:
        # Older SDKs don't support not_found_ok
        try:
            bq_client.delete_table(staging_table_id)
        except NotFound:
            pass

    # Create staging with destination schema
    dest_table = bq_client.get_table(dest_table_id)
    staging = bigquery.Table(staging_table_id, schema=dest_table.schema)
    bq_client.create_table(staging)


def _load_rows_into_table(
    bq_client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
    write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    """Load JSON rows into the specified table with configurable write disposition."""
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        ignore_unknown_values=True,
    )
    attempt = 0
    delay = 1.0
    while True:
        attempt += 1
        job = bq_client.load_table_from_json(rows, table_id, job_config=job_config)
        try:
            job.result()
            if job.errors:
                raise Exception(f"BigQuery load job failed with errors: {job.errors}")
            break
        except Exception:
            if attempt >= 3:
                raise
            time.sleep(delay)
            delay *= 2


async def _load_rows_in_chunks(
    bq_client: bigquery.Client,
    table_id: str,
    rows: List[Dict[str, Any]],
    chunk_size: int = 10000,
    truncate_first: bool = False,
) -> None:
    """Load rows into BigQuery in chunks to handle large datasets."""
    if not rows:
        return
    start = 0
    first = True
    while start < len(rows):
        chunk = rows[start : start + chunk_size]
        disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE if (truncate_first and first) else bigquery.WriteDisposition.WRITE_APPEND
        )
        _load_rows_into_table(bq_client, table_id, chunk, write_disposition=disposition)
        first = False
        start += chunk_size


def _merge_staging_into_destination(
    bq_client: bigquery.Client,
    dest_table_id: str,
    staging_table_id: str,
    primary_key: str = "id",
) -> None:
    """
    Perform an upsert using MERGE: updates existing rows and inserts new rows
    from staging into destination, matching on the primary key.
    """
    dest_table = bq_client.get_table(dest_table_id)
    staging_table = bq_client.get_table(staging_table_id)

    dest_cols = [f.name for f in dest_table.schema]
    staging_cols = {f.name for f in staging_table.schema}
    # Only use columns present in both tables
    common_cols = [c for c in dest_cols if c in staging_cols]
    if primary_key not in common_cols:
        raise ValueError(f"Primary key '{primary_key}' must be present in staging and destination table schemas")

    update_cols = [c for c in common_cols if c != primary_key]
    insert_cols = common_cols

    update_set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
    insert_cols_list = ", ".join(insert_cols)
    insert_values_list = ", ".join([f"S.{c}" for c in insert_cols])

    has_deleted = "deleted_at" in common_cols
    if has_deleted:
        merge_sql = f"""
            MERGE `{dest_table_id}` AS T
            USING `{staging_table_id}` AS S
            ON T.{primary_key} = S.{primary_key}
            WHEN MATCHED AND S.deleted_at IS NOT NULL THEN
              DELETE
            WHEN MATCHED THEN
              UPDATE SET {update_set_clause}
            WHEN NOT MATCHED AND S.deleted_at IS NULL THEN
              INSERT ({insert_cols_list}) VALUES ({insert_values_list})
        """
    else:
        merge_sql = f"""
            MERGE `{dest_table_id}` AS T
            USING `{staging_table_id}` AS S
            ON T.{primary_key} = S.{primary_key}
            WHEN MATCHED THEN
              UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols_list}) VALUES ({insert_values_list})
        """

    job = _run_query_with_retry(bq_client, merge_sql)
    job.result()


def _run_query_with_retry(
    bq_client: bigquery.Client, sql: str, job_config: Optional[bigquery.job.QueryJobConfig] = None
):
    attempt = 0
    delay = 1.0
    last_exc: Optional[Exception] = None
    while attempt < 3:
        attempt += 1
        try:
            return bq_client.query(sql, job_config=job_config)
        except Exception as exc:  # retry on transient errors
            last_exc = exc
            time.sleep(delay)
            delay *= 2
    if last_exc:
        raise last_exc
    raise RuntimeError("Query failed without exception")


def _infer_bq_field_type(field_name: str, value: Any) -> str:
    if value is None:
        # Heuristic for timestamp-ish columns
        if field_name.endswith("_at") or field_name.endswith("_time"):
            return "TIMESTAMP"
        return "STRING"
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, (list, dict)):
        # Conservatively store complex structures as STRING
        return "STRING"
    # Default to STRING for anything else
    return "STRING"


def _create_table_with_inferred_schema(
    bq_client: bigquery.Client, table_id: str, sample_rows: List[Dict[str, Any]]
) -> None:
    if not sample_rows:
        raise ValueError("Cannot infer schema without any rows")

    first_row = sample_rows[0]
    schema = []
    for key, value in first_row.items():
        field_type = _infer_bq_field_type(key, value)
        schema.append(bigquery.SchemaField(name=key, field_type=field_type))

    table = bigquery.Table(table_id, schema=schema)
    bq_client.create_table(table)


def _get_explicit_schema_for_table(table_name: str) -> Optional[List[bigquery.SchemaField]]:
    """Return explicit BigQuery schema for known tables."""
    ts = "TIMESTAMP"
    i64 = "INT64"
    b = "BOOL"
    s = "STRING"

    schemas: Dict[str, List[bigquery.SchemaField]] = {
        org_api_keys_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("org_id", i64),
            bigquery.SchemaField("hashed_key", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        courses_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("org_id", i64),
            bigquery.SchemaField("name", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        milestones_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("org_id", i64),
            bigquery.SchemaField("name", s),
            bigquery.SchemaField("color", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        course_tasks_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("task_id", i64),
            bigquery.SchemaField("course_id", i64),
            bigquery.SchemaField("ordering", i64),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
            bigquery.SchemaField("milestone_id", i64),
        ],
        course_milestones_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("course_id", i64),
            bigquery.SchemaField("milestone_id", i64),
            bigquery.SchemaField("ordering", i64),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        organizations_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("slug", s),
            bigquery.SchemaField("name", s),
            bigquery.SchemaField("default_logo_color", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        scorecards_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("org_id", i64),
            bigquery.SchemaField("title", s),
            bigquery.SchemaField("criteria", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
            bigquery.SchemaField("status", s),
        ],
        question_scorecards_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("question_id", i64),
            bigquery.SchemaField("scorecard_id", i64),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        task_completions_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("user_id", i64),
            bigquery.SchemaField("task_id", i64),
            bigquery.SchemaField("question_id", i64),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        chat_history_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("user_id", i64),
            bigquery.SchemaField("question_id", i64),
            bigquery.SchemaField("role", s),
            bigquery.SchemaField("content", s),
            bigquery.SchemaField("response_type", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        users_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("email", s),
            bigquery.SchemaField("first_name", s),
            bigquery.SchemaField("middle_name", s),
            bigquery.SchemaField("last_name", s),
            bigquery.SchemaField("default_dp_color", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
        ],
        tasks_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("org_id", i64),
            bigquery.SchemaField("type", s),
            bigquery.SchemaField("blocks", s),
            bigquery.SchemaField("title", s),
            bigquery.SchemaField("status", s),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
            bigquery.SchemaField("scheduled_publish_at", ts),
        ],
        questions_table_name: [
            bigquery.SchemaField("id", i64),
            bigquery.SchemaField("task_id", i64),
            bigquery.SchemaField("type", s),
            bigquery.SchemaField("blocks", s),
            bigquery.SchemaField("answer", s),
            bigquery.SchemaField("input_type", s),
            bigquery.SchemaField("coding_language", s),
            bigquery.SchemaField("generation_model", s),
            bigquery.SchemaField("response_type", s),
            bigquery.SchemaField("position", i64),
            bigquery.SchemaField("created_at", ts),
            bigquery.SchemaField("updated_at", ts),
            bigquery.SchemaField("deleted_at", ts),
            bigquery.SchemaField("max_attempts", i64),
            bigquery.SchemaField("is_feedback_shown", b),
            bigquery.SchemaField("context", s),
            bigquery.SchemaField("title", s),
        ],
    }

    return schemas.get(table_name)


def _ensure_table_exists_with_schema(
    bq_client: bigquery.Client,
    table_id: str,
    table_name: str,
    sample_rows: List[Dict[str, Any]],
) -> None:
    """Ensure destination table exists with explicit schema; create if missing."""
    try:
        bq_client.get_table(table_id)
        return
    except NotFound:
        pass

    explicit = _get_explicit_schema_for_table(table_name)
    if explicit:
        table = bigquery.Table(table_id, schema=explicit)
        bq_client.create_table(table)
        return
    # Fallback to inferred schema if table is unknown
    _create_table_with_inferred_schema(bq_client, table_id, sample_rows)


def _reconcile_schema_with_rows(
    bq_client: bigquery.Client, table_id: str, rows: List[Dict[str, Any]], table_name: str
) -> None:
    """Add any missing columns present in rows but absent in BigQuery table schema."""
    if not rows:
        return
    table = bq_client.get_table(table_id)
    existing = {f.name for f in table.schema}
    keys: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in existing and k not in keys:
                keys.append(k)
    if not keys:
        return
    # build new fields
    explicit = _get_explicit_schema_for_table(table_name) or []
    explicit_map = {f.name: f.field_type for f in explicit}
    new_fields: List[bigquery.SchemaField] = []
    for k in keys:
        field_type = explicit_map.get(k)
        if not field_type:
            # find first non-None value to infer, else default STRING
            val = None
            for r in rows:
                if k in r and r[k] is not None:
                    val = r[k]
                    break
            field_type = _infer_bq_field_type(k, val)
        new_fields.append(bigquery.SchemaField(k, field_type))
    updated_schema = list(table.schema) + new_fields
    table.schema = updated_schema
    bq_client.update_table(table, ["schema"])


def _parse_sqlite_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Expecting 'YYYY-MM-DD HH:MM:SS'
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _compute_rows_max_activity_ts(rows: List[Dict[str, Any]]) -> Optional[datetime]:
    """Compute max across created_at, updated_at, deleted_at for given rows."""
    max_ts: Optional[datetime] = None
    for r in rows:
        c = _parse_sqlite_dt(r.get("created_at"))
        u = _parse_sqlite_dt(r.get("updated_at"))
        d = _parse_sqlite_dt(r.get("deleted_at"))
        for candidate in (c, u, d):
            if candidate is not None and (max_ts is None or candidate > max_ts):
                max_ts = candidate
    return max_ts


async def _diff_sync_table(
    table_name: str,
    fetcher: Callable[[Optional[str]], Awaitable[List[Dict[str, Any]]]],
    primary_key: str = "id",
) -> None:
    """
    Diff-based synchronization from SQLite to BigQuery for a single table.
    - Determines last activity timestamp present in BigQuery
    - Fetches only changed rows from SQLite (created/updated/deleted since that timestamp)
    - Upserts changes via staging table and MERGE
    """
    bq_client = get_bq_client()
    table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{table_name}"

    # Determine since timestamp using metadata first, then fallback to BQ table scan
    _ensure_metadata_table(bq_client)
    last_sync_ts = _get_last_sync_ts_from_metadata(bq_client, table_name)
    if last_sync_ts is None:
        last_sync_ts = _get_last_activity_timestamp(bq_client, table_id)

    adjusted_since = _adjust_since_for_overlap(last_sync_ts)
    since_str: Optional[str] = _format_sqlite_datetime(adjusted_since) if adjusted_since else None

    rows = await fetcher(since_str)

    # Ensure destination table exists (with explicit schema) before loading
    _ensure_table_exists_with_schema(bq_client, table_id, table_name, rows)

    if not rows:
        logger.info(f"No new or updated/deleted rows to sync for {table_name}")
        print(f"No changes for {table_name}")
        return

    # Reconcile schema to handle evolution
    _reconcile_schema_with_rows(bq_client, table_id, rows, table_name)

    staging_table_id = _build_staging_table_id(table_id)
    _recreate_staging_with_dest_schema(bq_client, table_id, staging_table_id)

    # Load in chunks to staging
    await _load_rows_in_chunks(bq_client, staging_table_id, rows, chunk_size=10000, truncate_first=True)

    # Perform merge (with soft delete handling if available)
    _merge_staging_into_destination(bq_client, table_id, staging_table_id, primary_key=primary_key)

    # Clean up staging to avoid clutter
    try:
        bq_client.delete_table(staging_table_id, not_found_ok=True)  # type: ignore[arg-type]
    except TypeError:
        try:
            bq_client.delete_table(staging_table_id)
        except NotFound:
            pass

    # Update metadata with max processed activity timestamp
    max_ts = _compute_rows_max_activity_ts(rows)
    if max_ts is not None:
        _update_sync_ts_in_metadata(bq_client, table_name, max_ts)


async def sync_org_api_keys_to_bigquery():
    """
    Sync org_api_keys table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite org_api_keys table
    2. Deletes all existing data from BigQuery org_api_keys table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of org_api_keys table to BigQuery")
        print("Starting sync of org_api_keys table to BigQuery")

        await _diff_sync_table(org_api_keys_table_name, _fetch_org_api_keys_from_sqlite)

        logger.info("Successfully completed sync of org_api_keys table to BigQuery")
        print("Org API Keys sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing org_api_keys table to BigQuery: {str(e)}")
        print(f"Org API Keys sync failed: {str(e)}")
        raise


async def sync_courses_to_bigquery():
    """
    Sync courses table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite courses table
    2. Deletes all existing data from BigQuery courses table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of courses table to BigQuery")
        print("Starting sync of courses table to BigQuery")

        await _diff_sync_table(courses_table_name, _fetch_courses_from_sqlite)

        logger.info("Successfully completed sync of courses table to BigQuery")
        print("Courses sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing courses table to BigQuery: {str(e)}")
        print(f"Courses sync failed: {str(e)}")
        raise


async def sync_milestones_to_bigquery():
    """
    Sync milestones table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite milestones table
    2. Deletes all existing data from BigQuery milestones table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of milestones table to BigQuery")
        print("Starting sync of milestones table to BigQuery")

        await _diff_sync_table(milestones_table_name, _fetch_milestones_from_sqlite)

        logger.info("Successfully completed sync of milestones table to BigQuery")
        print("Milestones sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing milestones table to BigQuery: {str(e)}")
        print(f"Milestones sync failed: {str(e)}")
        raise


async def sync_course_tasks_to_bigquery():
    """
    Sync course_tasks table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite course_tasks table
    2. Deletes all existing data from BigQuery course_tasks table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of course_tasks table to BigQuery")
        print("Starting sync of course_tasks table to BigQuery")

        await _diff_sync_table(course_tasks_table_name, _fetch_course_tasks_from_sqlite)

        logger.info("Successfully completed sync of course_tasks table to BigQuery")
        print("Course Tasks sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing course_tasks table to BigQuery: {str(e)}")
        print(f"Course Tasks sync failed: {str(e)}")
        raise


async def sync_course_milestones_to_bigquery():
    """
    Sync course_milestones table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite course_milestones table
    2. Deletes all existing data from BigQuery course_milestones table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of course_milestones table to BigQuery")
        print("Starting sync of course_milestones table to BigQuery")

        await _diff_sync_table(
            course_milestones_table_name, _fetch_course_milestones_from_sqlite
        )

        logger.info(
            "Successfully completed sync of course_milestones table to BigQuery"
        )
        print("Course Milestones sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing course_milestones table to BigQuery: {str(e)}")
        print(f"Course Milestones sync failed: {str(e)}")
        raise


async def sync_organizations_to_bigquery():
    """
    Sync organizations table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite organizations table
    2. Deletes all existing data from BigQuery organizations table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of organizations table to BigQuery")
        print("Starting sync of organizations table to BigQuery")

        await _diff_sync_table(organizations_table_name, _fetch_organizations_from_sqlite)

        logger.info("Successfully completed sync of organizations table to BigQuery")
        print("Organizations sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing organizations table to BigQuery: {str(e)}")
        print(f"Organizations sync failed: {str(e)}")
        raise


async def sync_scorecards_to_bigquery():
    """
    Sync scorecards table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite scorecards table
    2. Deletes all existing data from BigQuery scorecards table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of scorecards table to BigQuery")
        print("Starting sync of scorecards table to BigQuery")

        await _diff_sync_table(scorecards_table_name, _fetch_scorecards_from_sqlite)

        logger.info("Successfully completed sync of scorecards table to BigQuery")
        print("Scorecards sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing scorecards table to BigQuery: {str(e)}")
        print(f"Scorecards sync failed: {str(e)}")
        raise


async def sync_question_scorecards_to_bigquery():
    """
    Sync question_scorecards table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite question_scorecards table
    2. Deletes all existing data from BigQuery question_scorecards table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of question_scorecards table to BigQuery")
        print("Starting sync of question_scorecards table to BigQuery")

        await _diff_sync_table(
            question_scorecards_table_name, _fetch_question_scorecards_from_sqlite
        )

        logger.info(
            "Successfully completed sync of question_scorecards table to BigQuery"
        )
        print("Question Scorecards sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing question_scorecards table to BigQuery: {str(e)}")
        print(f"Question Scorecards sync failed: {str(e)}")
        raise


async def sync_task_completions_to_bigquery():
    """
    Sync task_completions table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite task_completions table
    2. Deletes all existing data from BigQuery task_completions table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of task_completions table to BigQuery")
        print("Starting sync of task_completions table to BigQuery")

        await _diff_sync_table(
            task_completions_table_name, _fetch_task_completions_from_sqlite
        )

        logger.info("Successfully completed sync of task_completions table to BigQuery")
        print("Task Completions sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing task_completions table to BigQuery: {str(e)}")
        print(f"Task Completions sync failed: {str(e)}")
        raise


async def sync_chat_history_to_bigquery():
    """
    Sync chat_history table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite chat_history table
    2. Deletes all existing data from BigQuery chat_history table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of chat_history table to BigQuery")
        print("Starting sync of chat_history table to BigQuery")

        await _diff_sync_table(chat_history_table_name, _fetch_chat_history_from_sqlite)

        logger.info("Successfully completed sync of chat_history table to BigQuery")
        print("Chat History sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing chat_history table to BigQuery: {str(e)}")
        print(f"Chat History sync failed: {str(e)}")
        raise


async def sync_users_to_bigquery():
    """
    Sync users table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite users table
    2. Deletes all existing data from BigQuery users table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of users table to BigQuery")
        print("Starting sync of users table to BigQuery")

        await _diff_sync_table(users_table_name, _fetch_users_from_sqlite)

        logger.info("Successfully completed sync of users table to BigQuery")
        print("Users sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing users table to BigQuery: {str(e)}")
        print(f"Users sync failed: {str(e)}")
        raise


async def sync_tasks_to_bigquery():
    """
    Sync tasks table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite tasks table
    2. Deletes all existing data from BigQuery tasks table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of tasks table to BigQuery")
        print("Starting sync of tasks table to BigQuery")

        await _diff_sync_table(tasks_table_name, _fetch_tasks_from_sqlite)

        logger.info("Successfully completed sync of tasks table to BigQuery")
        print("Tasks sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing tasks table to BigQuery: {str(e)}")
        print(f"Tasks sync failed: {str(e)}")
        raise


async def sync_questions_to_bigquery():
    """
    Sync questions table from SQLite to BigQuery.
    This method:
    1. Fetches all data from SQLite questions table
    2. Deletes all existing data from BigQuery questions table
    3. Inserts all SQLite data into BigQuery
    """
    try:
        logger.info("Starting sync of questions table to BigQuery")
        print("Starting sync of questions table to BigQuery")

        await _diff_sync_table(questions_table_name, _fetch_questions_from_sqlite)

        logger.info("Successfully completed sync of questions table to BigQuery")
        print("Questions sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing questions table to BigQuery: {str(e)}")
        print(f"Questions sync failed: {str(e)}")
        raise


async def sync_all_tables_to_bigquery(concurrency: int = 4) -> None:
    """Run all table syncs in parallel with a concurrency limit."""
    sem = asyncio.Semaphore(concurrency)

    async def _with_sem(coro):
        async with sem:
            return await coro

    tasks = [
        _with_sem(sync_org_api_keys_to_bigquery()),
        _with_sem(sync_courses_to_bigquery()),
        _with_sem(sync_milestones_to_bigquery()),
        _with_sem(sync_course_tasks_to_bigquery()),
        _with_sem(sync_course_milestones_to_bigquery()),
        _with_sem(sync_organizations_to_bigquery()),
        _with_sem(sync_scorecards_to_bigquery()),
        _with_sem(sync_question_scorecards_to_bigquery()),
        _with_sem(sync_task_completions_to_bigquery()),
        _with_sem(sync_chat_history_to_bigquery()),
        _with_sem(sync_users_to_bigquery()),
        _with_sem(sync_tasks_to_bigquery()),
        _with_sem(sync_questions_to_bigquery()),
    ]

    await asyncio.gather(*tasks)


async def _fetch_org_api_keys_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite org_api_keys table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, org_id, hashed_key, created_at, updated_at, deleted_at
            FROM {org_api_keys_table_name}
        """

        if since:
            where_clause = "WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?))"
            await cursor.execute(f"{base_query} {where_clause} ORDER BY id", (since, since, since))
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "org_id": row[1],
                    "hashed_key": row[2],
                    "created_at": row[3],
                    "updated_at": row[4],
                    "deleted_at": row[5],
                }
            )

        return data


async def _fetch_courses_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite courses table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, org_id, name, created_at, updated_at, deleted_at
            FROM {courses_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "org_id": row[1],
                    "name": row[2],
                    "created_at": row[3],
                    "updated_at": row[4],
                    "deleted_at": row[5],
                }
            )

        return data


async def _fetch_milestones_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite milestones table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, org_id, name, color, created_at, updated_at, deleted_at
            FROM {milestones_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "org_id": row[1],
                    "name": row[2],
                    "color": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                    "deleted_at": row[6],
                }
            )

        return data


async def _fetch_course_tasks_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite course_tasks table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, task_id, course_id, ordering, created_at, updated_at, deleted_at, milestone_id
            FROM {course_tasks_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "task_id": row[1],
                    "course_id": row[2],
                    "ordering": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                    "deleted_at": row[6],
                    "milestone_id": row[7],
                }
            )

        return data


async def _fetch_course_milestones_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite course_milestones table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, course_id, milestone_id, ordering, created_at, updated_at, deleted_at
            FROM {course_milestones_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "course_id": row[1],
                    "milestone_id": row[2],
                    "ordering": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                    "deleted_at": row[6],
                }
            )

        return data


async def _fetch_organizations_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite organizations table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, slug, name, default_logo_color, created_at, updated_at, deleted_at
            FROM {organizations_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "slug": row[1],
                    "name": row[2],
                    "default_logo_color": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                    "deleted_at": row[6],
                }
            )

        return data


async def _fetch_scorecards_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite scorecards table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, org_id, title, criteria, created_at, updated_at, deleted_at, status
            FROM {scorecards_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "org_id": row[1],
                    "title": row[2],
                    "criteria": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                    "deleted_at": row[6],
                    "status": row[7],
                }
            )

        return data


async def _fetch_question_scorecards_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite question_scorecards table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, question_id, scorecard_id, created_at, updated_at, deleted_at
            FROM {question_scorecards_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "question_id": row[1],
                    "scorecard_id": row[2],
                    "created_at": row[3],
                    "updated_at": row[4],
                    "deleted_at": row[5],
                }
            )

        return data


async def _fetch_task_completions_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite task_completions table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, user_id, task_id, question_id, created_at, updated_at, deleted_at
            FROM {task_completions_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "user_id": row[1],
                    "task_id": row[2],
                    "question_id": row[3],
                    "created_at": row[4],
                    "updated_at": row[5],
                    "deleted_at": row[6],
                }
            )

        return data


async def _fetch_chat_history_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite chat_history table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, user_id, question_id, role, content, response_type, created_at, updated_at, deleted_at
            FROM {chat_history_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "user_id": row[1],
                    "question_id": row[2],
                    "role": row[3],
                    "content": row[4],
                    "response_type": row[5],
                    "created_at": row[6],
                    "updated_at": row[7],
                    "deleted_at": row[8],
                }
            )

        return data


async def _fetch_users_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite users table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, email, first_name, middle_name, last_name, default_dp_color, created_at, updated_at, deleted_at
            FROM {users_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "email": row[1],
                    "first_name": row[2],
                    "middle_name": row[3],
                    "last_name": row[4],
                    "default_dp_color": row[5],
                    "created_at": row[6],
                    "updated_at": row[7],
                    "deleted_at": row[8],
                }
            )

        return data


async def _fetch_tasks_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite tasks table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, org_id, type, blocks, title, status, created_at, updated_at, deleted_at, scheduled_publish_at
            FROM {tasks_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "org_id": row[1],
                    "type": row[2],
                    "blocks": row[3],
                    "title": row[4],
                    "status": row[5],
                    "created_at": row[6],
                    "updated_at": row[7],
                    "deleted_at": row[8],
                    "scheduled_publish_at": row[9],
                }
            )

        return data


async def _fetch_questions_from_sqlite(since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch records from SQLite questions table, optionally filtered by since timestamp."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        base_query = f"""
            SELECT id, task_id, type, blocks, answer, input_type, coding_language,
                   generation_model, response_type, position, created_at, updated_at, deleted_at,
                   max_attempts, is_feedback_shown, context, title
            FROM {questions_table_name}
        """
        if since:
            await cursor.execute(
                f"{base_query} WHERE (created_at > ? OR updated_at > ? OR (deleted_at IS NOT NULL AND deleted_at > ?)) ORDER BY id",
                (since, since, since),
            )
        else:
            await cursor.execute(f"{base_query} ORDER BY id")

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {
                    "id": row[0],
                    "task_id": row[1],
                    "type": row[2],
                    "blocks": row[3],
                    "answer": row[4],
                    "input_type": row[5],
                    "coding_language": row[6],
                    "generation_model": row[7],
                    "response_type": row[8],
                    "position": row[9],
                    "created_at": row[10],
                    "updated_at": row[11],
                    "deleted_at": row[12],
                    "max_attempts": row[13],
                    "is_feedback_shown": row[14],
                    "context": row[15],
                    "title": row[16],
                }
            )

        return data


def _insert_data_to_bq_table(
    bq_client: bigquery.Client, table_id: str, data: List[Dict[str, Any]]
):
    """Deprecated: Prefer diff-based sync. Kept for backward compatibility if needed."""
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )
    job = bq_client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()
    if job.errors:
        raise Exception(f"BigQuery insert job failed with errors: {job.errors}")


# Example usage / test function
async def run_all_syncs():
    """
    Run all table syncs in sequence.
    This can be called from a cron job to sync all tables at once.
    """
    sync_id = None
    try:
        # Record start of the full BigQuery sync
        async with get_new_db_connection() as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                f"INSERT INTO {bq_sync_table_name} (started_at) VALUES (CURRENT_TIMESTAMP)"
            )
            sync_id = cursor.lastrowid
            await conn.commit()

        await sync_org_api_keys_to_bigquery()
        await sync_courses_to_bigquery()
        await sync_milestones_to_bigquery()
        await sync_course_tasks_to_bigquery()
        await sync_course_milestones_to_bigquery()
        await sync_organizations_to_bigquery()
        await sync_scorecards_to_bigquery()
        await sync_question_scorecards_to_bigquery()
        await sync_task_completions_to_bigquery()
        await sync_chat_history_to_bigquery()
        await sync_users_to_bigquery()
        await sync_tasks_to_bigquery()
        await sync_questions_to_bigquery()
        print("All table syncs completed successfully!")
    except Exception as e:
        print(f"Table sync failed: {str(e)}")
        raise
    finally:
        # Record end of the full BigQuery sync
        try:
            if sync_id is not None:
                async with get_new_db_connection() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(
                        f"UPDATE {bq_sync_table_name} SET ended_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (sync_id,),
                    )
                    await conn.commit()
        except Exception:
            # Avoid masking the original exception if any
            pass


# If running this file directly for testing
if __name__ == "__main__":
    import asyncio

    # Run all syncs at once
    asyncio.run(run_all_syncs())
