from google.cloud import bigquery
import os
from typing import List, Dict, Any
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
)
from api.utils.logging import logger


def get_bq_client():
    """Get BigQuery client with proper credentials"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        settings.google_application_credentials
    )
    return bigquery.Client()


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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_org_api_keys_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite org_api_keys table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{org_api_keys_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery org_api_keys table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery org_api_keys table"
            )
        else:
            logger.info("No data to insert into BigQuery org_api_keys table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_courses_from_sqlite()
        logger.info(f"Fetched {len(sqlite_data)} records from SQLite courses table")

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{courses_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery courses table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery courses table"
            )
        else:
            logger.info("No data to insert into BigQuery courses table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_milestones_from_sqlite()
        logger.info(f"Fetched {len(sqlite_data)} records from SQLite milestones table")

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{milestones_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id, has_created_at=False)
        logger.info("Deleted all existing records from BigQuery milestones table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery milestones table"
            )
        else:
            logger.info("No data to insert into BigQuery milestones table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_course_tasks_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite course_tasks table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{course_tasks_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery course_tasks table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery course_tasks table"
            )
        else:
            logger.info("No data to insert into BigQuery course_tasks table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_course_milestones_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite course_milestones table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{course_milestones_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info(
            "Deleted all existing records from BigQuery course_milestones table"
        )

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery course_milestones table"
            )
        else:
            logger.info("No data to insert into BigQuery course_milestones table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_organizations_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite organizations table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{organizations_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery organizations table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery organizations table"
            )
        else:
            logger.info("No data to insert into BigQuery organizations table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_scorecards_from_sqlite()
        logger.info(f"Fetched {len(sqlite_data)} records from SQLite scorecards table")

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{scorecards_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery scorecards table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery scorecards table"
            )
        else:
            logger.info("No data to insert into BigQuery scorecards table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_question_scorecards_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite question_scorecards table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{question_scorecards_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info(
            "Deleted all existing records from BigQuery question_scorecards table"
        )

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery question_scorecards table"
            )
        else:
            logger.info("No data to insert into BigQuery question_scorecards table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_task_completions_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite task_completions table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{task_completions_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery task_completions table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery task_completions table"
            )
        else:
            logger.info("No data to insert into BigQuery task_completions table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_chat_history_from_sqlite()
        logger.info(
            f"Fetched {len(sqlite_data)} records from SQLite chat_history table"
        )

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{chat_history_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery chat_history table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery chat_history table"
            )
        else:
            logger.info("No data to insert into BigQuery chat_history table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_users_from_sqlite()
        logger.info(f"Fetched {len(sqlite_data)} records from SQLite users table")

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = (
            f"{settings.bq_project_name}.{settings.bq_dataset_name}.{users_table_name}"
        )

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery users table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery users table"
            )
        else:
            logger.info("No data to insert into BigQuery users table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_tasks_from_sqlite()
        logger.info(f"Fetched {len(sqlite_data)} records from SQLite tasks table")

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = (
            f"{settings.bq_project_name}.{settings.bq_dataset_name}.{tasks_table_name}"
        )

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery tasks table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery tasks table"
            )
        else:
            logger.info("No data to insert into BigQuery tasks table")

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

        # Step 1: Fetch all data from SQLite
        sqlite_data = await _fetch_questions_from_sqlite()
        logger.info(f"Fetched {len(sqlite_data)} records from SQLite questions table")

        # Step 2: Get BigQuery client and table reference
        bq_client = get_bq_client()
        table_id = f"{settings.bq_project_name}.{settings.bq_dataset_name}.{questions_table_name}"

        # Step 3: Delete all existing data from BigQuery table
        _delete_all_from_bq_table(bq_client, table_id)
        logger.info("Deleted all existing records from BigQuery questions table")

        # Step 4: Insert SQLite data into BigQuery
        if sqlite_data:
            _insert_data_to_bq_table(bq_client, table_id, sqlite_data)
            logger.info(
                f"Inserted {len(sqlite_data)} records into BigQuery questions table"
            )
        else:
            logger.info("No data to insert into BigQuery questions table")

        logger.info("Successfully completed sync of questions table to BigQuery")
        print("Questions sync completed successfully!")

    except Exception as e:
        logger.error(f"Error syncing questions table to BigQuery: {str(e)}")
        print(f"Questions sync failed: {str(e)}")
        raise


async def _fetch_org_api_keys_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite org_api_keys table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, org_id, hashed_key, created_at 
            FROM {org_api_keys_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_courses_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite courses table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, org_id, name, created_at 
            FROM {courses_table_name}
            ORDER BY id
        """
        )

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {"id": row[0], "org_id": row[1], "name": row[2], "created_at": row[3]}
            )

        return data


async def _fetch_milestones_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite milestones table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, org_id, name, color 
            FROM {milestones_table_name}
            ORDER BY id
        """
        )

        rows = await cursor.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append(
                {"id": row[0], "org_id": row[1], "name": row[2], "color": row[3]}
            )

        return data


async def _fetch_course_tasks_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite course_tasks table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, task_id, course_id, ordering, created_at, milestone_id 
            FROM {course_tasks_table_name}
            ORDER BY id
        """
        )

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
                    "milestone_id": row[5],
                }
            )

        return data


async def _fetch_course_milestones_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite course_milestones table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, course_id, milestone_id, ordering, created_at 
            FROM {course_milestones_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_organizations_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite organizations table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, slug, name, default_logo_color, created_at 
            FROM {organizations_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_scorecards_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite scorecards table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, org_id, title, criteria, created_at, status 
            FROM {scorecards_table_name}
            ORDER BY id
        """
        )

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
                    "status": row[5],
                }
            )

        return data


async def _fetch_question_scorecards_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite question_scorecards table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, question_id, scorecard_id, created_at 
            FROM {question_scorecards_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_task_completions_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite task_completions table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, user_id, task_id, question_id, created_at 
            FROM {task_completions_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_chat_history_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite chat_history table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, user_id, question_id, role, content, response_type, created_at 
            FROM {chat_history_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_users_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite users table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, email, first_name, middle_name, last_name, default_dp_color, created_at 
            FROM {users_table_name}
            ORDER BY id
        """
        )

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
                }
            )

        return data


async def _fetch_tasks_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite tasks table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, org_id, type, blocks, title, status, created_at, deleted_at, scheduled_publish_at 
            FROM {tasks_table_name}
            ORDER BY id
        """
        )

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
                    "deleted_at": row[7],
                    "scheduled_publish_at": row[8],
                }
            )

        return data


async def _fetch_questions_from_sqlite() -> List[Dict[str, Any]]:
    """Fetch all records from SQLite questions table"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"""
            SELECT id, task_id, type, blocks, answer, input_type, coding_language, 
                   generation_model, response_type, position, created_at, deleted_at, 
                   max_attempts, is_feedback_shown, context, title 
            FROM {questions_table_name}
            ORDER BY id
        """
        )

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
                    "deleted_at": row[11],
                    "max_attempts": row[12],
                    "is_feedback_shown": row[13],
                    "context": row[14],
                    "title": row[15],
                }
            )

        return data


def _delete_all_from_bq_table(
    bq_client: bigquery.Client, table_id: str, has_created_at: bool = True
):
    """Delete all records from BigQuery table"""
    if has_created_at:
        if org_api_keys_table_name in table_id:
            query = f"DELETE FROM `{table_id}` WHERE TRUE AND created_at > DATETIME('2024-01-01 00:00:00')"
        else:
            query = f"DELETE FROM `{table_id}` WHERE TRUE AND created_at > TIMESTAMP('2024-01-01 00:00:00')"
    else:
        query = f"DELETE FROM `{table_id}` WHERE TRUE"

    job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(query, job_config=job_config)

    # Wait for the job to complete
    query_job.result()


def _insert_data_to_bq_table(
    bq_client: bigquery.Client, table_id: str, data: List[Dict[str, Any]]
):
    """Insert data into BigQuery table"""
    table = bq_client.get_table(table_id)

    # Configure the job to append data and ignore unknown values
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )

    # Insert the data
    job = bq_client.load_table_from_json(data, table, job_config=job_config)

    # Wait for the job to complete
    job.result()

    if job.errors:
        raise Exception(f"BigQuery insert job failed with errors: {job.errors}")


# Example usage / test function
async def run_all_syncs():
    """
    Run all table syncs in sequence.
    This can be called from a cron job to sync all tables at once.
    """
    try:
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


# If running this file directly for testing
if __name__ == "__main__":
    import asyncio

    # Run all syncs at once
    asyncio.run(run_all_syncs())
