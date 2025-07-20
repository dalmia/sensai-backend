import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from api.settings import settings
from api.config import (
    # Core tables
    organizations_table_name,
    users_table_name,
    cohorts_table_name,
    batches_table_name,
    courses_table_name,
    milestones_table_name,
    tasks_table_name,
    questions_table_name,
    scorecards_table_name,
    chat_history_table_name,
    task_completions_table_name,
    # Junction tables
    user_organizations_table_name,
    user_cohorts_table_name,
    user_batches_table_name,
    course_cohorts_table_name,
    course_tasks_table_name,
    course_milestones_table_name,
    question_scorecards_table_name,
    # Job tables
    course_generation_jobs_table_name,
    task_generation_jobs_table_name,
    org_api_keys_table_name,
    code_drafts_table_name,
)
from api.utils.db import execute_db_operation, get_new_db_connection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sync tracking table name
SYNC_TRACKING_TABLE = "bq_sync_tracking"


class BigQuerySyncer:
    """Handles syncing data from SQLite to BigQuery with change tracking."""

    def __init__(self):
        self.client = self._get_bq_client()
        self.project_id = settings.bq_project_name
        self.dataset_id = settings.bq_dataset_name

        # Define tables to sync and their schemas
        self.tables_to_sync = {
            organizations_table_name: self._get_organizations_schema(),
            users_table_name: self._get_users_schema(),
            cohorts_table_name: self._get_cohorts_schema(),
            batches_table_name: self._get_batches_schema(),
            courses_table_name: self._get_courses_schema(),
            milestones_table_name: self._get_milestones_schema(),
            tasks_table_name: self._get_tasks_schema(),
            questions_table_name: self._get_questions_schema(),
            scorecards_table_name: self._get_scorecards_schema(),
            chat_history_table_name: self._get_chat_history_schema(),
            task_completions_table_name: self._get_task_completions_schema(),
            user_organizations_table_name: self._get_user_organizations_schema(),
            user_cohorts_table_name: self._get_user_cohorts_schema(),
            user_batches_table_name: self._get_user_batches_schema(),
            course_cohorts_table_name: self._get_course_cohorts_schema(),
            course_tasks_table_name: self._get_course_tasks_schema(),
            course_milestones_table_name: self._get_course_milestones_schema(),
            question_scorecards_table_name: self._get_question_scorecards_schema(),
            course_generation_jobs_table_name: self._get_course_generation_jobs_schema(),
            task_generation_jobs_table_name: self._get_task_generation_jobs_schema(),
            org_api_keys_table_name: self._get_org_api_keys_schema(),
            code_drafts_table_name: self._get_code_drafts_schema(),
        }

    def _get_bq_client(self) -> bigquery.Client:
        """Initialize BigQuery client."""
        if not settings.google_application_credentials:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not configured")

        import os

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            settings.google_application_credentials
        )
        return bigquery.Client(project=settings.bq_project_name)

    async def initialize_sync_tracking(self):
        """Create sync tracking table in SQLite if it doesn't exist."""
        await execute_db_operation(
            f"""
            CREATE TABLE IF NOT EXISTS {SYNC_TRACKING_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                last_sync_timestamp TEXT,
                last_synced_row_id INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name)
            )
        """
        )

        # Initialize tracking records for all tables
        for table_name in self.tables_to_sync.keys():
            await execute_db_operation(
                f"""
                INSERT OR IGNORE INTO {SYNC_TRACKING_TABLE} (table_name, last_sync_timestamp, last_synced_row_id)
                VALUES (?, ?, ?)
            """,
                (table_name, "2024-01-01 00:00:00", 0),
            )

    async def get_last_sync_info(self, table_name: str) -> Dict[str, Any]:
        """Get last sync information for a table."""
        result = await execute_db_operation(
            f"""
            SELECT last_sync_timestamp, last_synced_row_id 
            FROM {SYNC_TRACKING_TABLE} 
            WHERE table_name = ?
        """,
            (table_name,),
            fetch_one=True,
        )

        if result:
            return {"last_sync_timestamp": result[0], "last_synced_row_id": result[1]}
        return {"last_sync_timestamp": "2024-01-01 00:00:00", "last_synced_row_id": 0}

    async def update_sync_info(self, table_name: str, max_row_id: int):
        """Update sync tracking information."""
        current_time = datetime.now(timezone.utc).isoformat()
        await execute_db_operation(
            f"""
            UPDATE {SYNC_TRACKING_TABLE} 
            SET last_sync_timestamp = ?, last_synced_row_id = ?, updated_at = ?
            WHERE table_name = ?
        """,
            (current_time, max_row_id, current_time, table_name),
        )

    def _ensure_bq_table_exists(
        self, table_name: str, schema: List[bigquery.SchemaField]
    ):
        """Ensure BigQuery table exists with the correct schema."""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        try:
            table = self.client.get_table(table_id)
            logger.info(f"Table {table_name} already exists in BigQuery")
        except NotFound:
            logger.info(f"Creating table {table_name} in BigQuery")
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created table {table_name}")

    async def get_changed_rows(
        self, table_name: str, last_synced_row_id: int
    ) -> List[Dict[str, Any]]:
        """Get rows that have been added or modified since last sync."""
        # Get all rows with ID greater than last synced
        query = f"""
            SELECT * FROM {table_name} 
            WHERE id > ? 
            ORDER BY id
        """

        rows = await execute_db_operation(query, (last_synced_row_id,), fetch_all=True)

        if not rows:
            return []

        # Get column names
        column_info = await execute_db_operation(
            f"PRAGMA table_info({table_name})", fetch_all=True
        )
        column_names = [col[1] for col in column_info]

        # Convert rows to dictionaries
        result = []
        for row in rows:
            row_dict = dict(zip(column_names, row))
            # Convert datetime strings to proper format for BigQuery
            for key, value in row_dict.items():
                if key.endswith("_at") and value:
                    # Ensure proper datetime format for BigQuery
                    if isinstance(value, str):
                        try:
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            row_dict[key] = dt.isoformat()
                        except:
                            # Keep original value if parsing fails
                            pass
            result.append(row_dict)

        return result

    async def sync_table_to_bq(self, table_name: str):
        """Sync a single table to BigQuery."""
        logger.info(f"Starting sync for table: {table_name}")

        # Ensure BigQuery table exists
        schema = self.tables_to_sync[table_name]
        self._ensure_bq_table_exists(table_name, schema)

        # Get last sync info
        sync_info = await self.get_last_sync_info(table_name)
        last_synced_row_id = sync_info["last_synced_row_id"]

        # Get changed rows
        changed_rows = await self.get_changed_rows(table_name, last_synced_row_id)

        if not changed_rows:
            logger.info(f"No new changes for table {table_name}")
            return

        logger.info(
            f"Found {len(changed_rows)} new/modified rows for table {table_name}"
        )

        # Insert/update rows in BigQuery
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        # For tables that support updates, we use MERGE. For others, we use INSERT.
        if self._table_supports_updates(table_name):
            await self._upsert_to_bq(table_id, changed_rows, table_name)
        else:
            await self._insert_to_bq(table_id, changed_rows)

        # Update sync tracking
        max_row_id = max(row["id"] for row in changed_rows)
        await self.update_sync_info(table_name, max_row_id)

        logger.info(
            f"Successfully synced {len(changed_rows)} rows for table {table_name}"
        )

    def _table_supports_updates(self, table_name: str) -> bool:
        """Check if table supports updates (vs insert-only)."""
        # These tables might have updates to existing rows
        updatable_tables = {
            organizations_table_name,
            users_table_name,
            courses_table_name,
            tasks_table_name,
            questions_table_name,
            scorecards_table_name,
            code_drafts_table_name,
            course_generation_jobs_table_name,
            task_generation_jobs_table_name,
        }
        return table_name in updatable_tables

    async def _insert_to_bq(self, table_id: str, rows: List[Dict[str, Any]]):
        """Insert rows to BigQuery."""
        try:
            errors = self.client.insert_rows_json(table_id, rows)
            if errors:
                logger.error(f"Errors inserting to {table_id}: {errors}")
                raise Exception(f"BigQuery insert errors: {errors}")
        except Exception as e:
            logger.error(f"Failed to insert to {table_id}: {str(e)}")
            raise

    async def _upsert_to_bq(
        self, table_id: str, rows: List[Dict[str, Any]], table_name: str
    ):
        """Upsert (merge) rows to BigQuery."""
        if not rows:
            return

        # Create a temporary table with the new data
        temp_table_id = f"{table_id}_temp_{int(datetime.now().timestamp())}"

        try:
            # Create temporary table
            temp_table = bigquery.Table(temp_table_id)
            temp_table = self.client.create_table(temp_table)

            # Insert data into temp table
            errors = self.client.insert_rows_json(temp_table_id, rows)
            if errors:
                raise Exception(f"Temp table insert errors: {errors}")

            # Perform MERGE operation
            merge_query = self._build_merge_query(table_id, temp_table_id, table_name)
            query_job = self.client.query(merge_query)
            query_job.result()  # Wait for completion

        finally:
            # Clean up temp table
            try:
                self.client.delete_table(temp_table_id)
            except:
                pass  # Ignore cleanup errors

    def _build_merge_query(
        self, target_table: str, source_table: str, table_name: str
    ) -> str:
        """Build MERGE query for upsert operation."""
        # Get the schema to build the merge query
        schema = self.tables_to_sync[table_name]
        columns = [field.name for field in schema]

        # Build SET clause for updates (exclude id and created_at)
        update_columns = [col for col in columns if col not in ["id", "created_at"]]
        set_clause = ", ".join([f"{col} = source.{col}" for col in update_columns])

        # Build INSERT clause
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        merge_query = f"""
        MERGE `{target_table}` AS target
        USING `{source_table}` AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        return merge_query

    async def sync_all_tables(self):
        """Sync all tables to BigQuery."""
        logger.info("Starting full sync to BigQuery")

        # Initialize sync tracking
        await self.initialize_sync_tracking()

        # Sync each table
        for table_name in self.tables_to_sync.keys():
            try:
                await self.sync_table_to_bq(table_name)
            except Exception as e:
                logger.error(f"Failed to sync table {table_name}: {str(e)}")
                # Continue with other tables even if one fails
                continue

        logger.info("Completed full sync to BigQuery")

    # Schema definitions for each table
    def _get_organizations_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("slug", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("default_logo_color", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("openai_api_key", "STRING"),
            bigquery.SchemaField("openai_free_trial", "BOOLEAN"),
        ]

    def _get_users_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("middle_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("default_dp_color", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_cohorts_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
        ]

    def _get_batches_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cohort_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_courses_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_milestones_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("color", "STRING"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_tasks_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("blocks", "STRING"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP"),
            bigquery.SchemaField("scheduled_publish_at", "TIMESTAMP"),
        ]

    def _get_questions_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("task_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("blocks", "STRING"),
            bigquery.SchemaField("answer", "STRING"),
            bigquery.SchemaField("input_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("coding_language", "STRING"),
            bigquery.SchemaField("generation_model", "STRING"),
            bigquery.SchemaField("response_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("position", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("deleted_at", "TIMESTAMP"),
            bigquery.SchemaField("max_attempts", "INTEGER"),
            bigquery.SchemaField("is_feedback_shown", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("context", "STRING"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        ]

    def _get_scorecards_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("criteria", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
        ]

    def _get_chat_history_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("question_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("role", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("response_type", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_task_completions_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("task_id", "INTEGER"),
            bigquery.SchemaField("question_id", "INTEGER"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_user_organizations_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("role", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_user_cohorts_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("cohort_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("role", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("joined_at", "TIMESTAMP"),
        ]

    def _get_user_batches_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("batch_id", "INTEGER", mode="REQUIRED"),
        ]

    def _get_course_cohorts_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("cohort_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("is_drip_enabled", "BOOLEAN"),
            bigquery.SchemaField("frequency_value", "INTEGER"),
            bigquery.SchemaField("frequency_unit", "STRING"),
            bigquery.SchemaField("publish_at", "TIMESTAMP"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_course_tasks_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("task_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ordering", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("milestone_id", "INTEGER"),
        ]

    def _get_course_milestones_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("milestone_id", "INTEGER"),
            bigquery.SchemaField("ordering", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_question_scorecards_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("question_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("scorecard_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_course_generation_jobs_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_details", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_task_generation_jobs_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("task_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("course_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_details", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_org_api_keys_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("org_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("hashed_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _get_code_drafts_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("question_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]


# Main cron job functions
async def sync_to_bigquery():
    """Main function to sync all data to BigQuery."""
    try:
        syncer = BigQuerySyncer()
        await syncer.sync_all_tables()
        logger.info("BigQuery sync completed successfully")
    except Exception as e:
        logger.error(f"BigQuery sync failed: {str(e)}")
        raise


async def sync_specific_tables(table_names: List[str]):
    """Sync specific tables to BigQuery."""
    try:
        syncer = BigQuerySyncer()
        await syncer.initialize_sync_tracking()

        for table_name in table_names:
            if table_name in syncer.tables_to_sync:
                await syncer.sync_table_to_bq(table_name)
                logger.info(f"Successfully synced table: {table_name}")
            else:
                logger.warning(f"Table {table_name} not configured for sync")

    except Exception as e:
        logger.error(f"Specific table sync failed: {str(e)}")
        raise


# Convenience function for running the sync
if __name__ == "__main__":
    asyncio.run(sync_to_bigquery())
