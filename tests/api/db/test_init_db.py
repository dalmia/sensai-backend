import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from src.api.db import (
    create_organizations_table,
    create_org_api_keys_table,
    create_users_table,
    create_user_organizations_table,
    create_cohort_tables,
    create_course_tasks_table,
    create_course_milestones_table,
    create_milestones_table,
    create_courses_table,
    create_course_cohorts_table,
    create_tasks_table,
    create_questions_table,
    create_scorecards_table,
    create_question_scorecards_table,
    create_chat_history_table,
    create_task_completion_table,
    create_course_generation_jobs_table,
    create_task_generation_jobs_table,
    create_code_drafts_table,
    init_db,
    delete_useless_tables,
)


@pytest.mark.asyncio
class TestTableCreationFunctions:
    """Test database table creation functions."""

    async def test_create_organizations_table(self):
        """Test creating organizations table."""
        mock_cursor = AsyncMock()

        await create_organizations_table(mock_cursor)

        # Should execute CREATE TABLE and CREATE INDEX
        assert mock_cursor.execute.call_count == 2
        create_table_call = mock_cursor.execute.call_args_list[0]
        create_index_call = mock_cursor.execute.call_args_list[1]

        assert "CREATE TABLE IF NOT EXISTS organizations" in create_table_call[0][0]
        assert "CREATE INDEX idx_org_slug" in create_index_call[0][0]

    async def test_create_org_api_keys_table(self):
        """Test creating org API keys table."""
        mock_cursor = AsyncMock()

        await create_org_api_keys_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS org_api_keys" in call for call in calls)
        assert any("CREATE INDEX idx_org_api_key_org_id" in call for call in calls)
        assert any("CREATE INDEX idx_org_api_key_hashed_key" in call for call in calls)

    async def test_create_users_table(self):
        """Test creating users table."""
        mock_cursor = AsyncMock()

        await create_users_table(mock_cursor)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS users" in call_args

    async def test_create_user_organizations_table(self):
        """Test creating user organizations table."""
        mock_cursor = AsyncMock()

        await create_user_organizations_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any(
            "CREATE TABLE IF NOT EXISTS user_organizations" in call for call in calls
        )
        assert any("CREATE INDEX idx_user_org_user_id" in call for call in calls)
        assert any("CREATE INDEX idx_user_org_org_id" in call for call in calls)

    async def test_create_cohort_tables(self):
        """Test creating cohort tables."""
        mock_cursor = AsyncMock()

        await create_cohort_tables(mock_cursor)

        # Should execute 2 CREATE TABLE and 3 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 5
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS cohorts" in call for call in calls)
        assert any("CREATE TABLE IF NOT EXISTS user_cohorts" in call for call in calls)

    async def test_create_course_tasks_table(self):
        """Test creating course tasks table."""
        mock_cursor = AsyncMock()

        await create_course_tasks_table(mock_cursor)

        # Should execute CREATE TABLE and 3 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 4
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS course_tasks" in call for call in calls)

    async def test_create_course_milestones_table(self):
        """Test creating course milestones table."""
        mock_cursor = AsyncMock()

        await create_course_milestones_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any(
            "CREATE TABLE IF NOT EXISTS course_milestones" in call for call in calls
        )

    async def test_create_milestones_table(self):
        """Test creating milestones table."""
        mock_cursor = AsyncMock()

        await create_milestones_table(mock_cursor)

        # Should execute CREATE TABLE and CREATE INDEX statements
        assert mock_cursor.execute.call_count == 2

    async def test_create_courses_table(self):
        """Test creating courses table."""
        mock_cursor = AsyncMock()

        await create_courses_table(mock_cursor)

        # Should execute CREATE TABLE and CREATE INDEX
        assert mock_cursor.execute.call_count == 2
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS courses" in call for call in calls)

    async def test_create_course_cohorts_table(self):
        """Test creating course cohorts table."""
        mock_cursor = AsyncMock()

        await create_course_cohorts_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any(
            "CREATE TABLE IF NOT EXISTS course_cohorts" in call for call in calls
        )

    async def test_create_tasks_table(self):
        """Test creating tasks table."""
        mock_cursor = AsyncMock()

        await create_tasks_table(mock_cursor)

        # Should execute CREATE TABLE and CREATE INDEX
        assert mock_cursor.execute.call_count == 2
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS tasks" in call for call in calls)

    async def test_create_questions_table(self):
        """Test creating questions table."""
        mock_cursor = AsyncMock()

        await create_questions_table(mock_cursor)

        # Should execute CREATE TABLE and 1 CREATE INDEX statement
        assert mock_cursor.execute.call_count == 2

    async def test_create_scorecards_table(self):
        """Test creating scorecards table."""
        mock_cursor = AsyncMock()

        await create_scorecards_table(mock_cursor)

        # Should execute CREATE TABLE and 1 CREATE INDEX statement
        assert mock_cursor.execute.call_count == 2

    async def test_create_question_scorecards_table(self):
        """Test creating question scorecards table."""
        mock_cursor = AsyncMock()

        await create_question_scorecards_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any(
            "CREATE TABLE IF NOT EXISTS question_scorecards" in call for call in calls
        )

    async def test_create_chat_history_table(self):
        """Test creating chat history table."""
        mock_cursor = AsyncMock()

        await create_chat_history_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3

    async def test_create_task_completion_table(self):
        """Test creating task completion table."""
        mock_cursor = AsyncMock()

        await create_task_completion_table(mock_cursor)

        # Should execute CREATE TABLE and 3 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 4

    async def test_create_course_generation_jobs_table(self):
        """Test creating course generation jobs table."""
        mock_cursor = AsyncMock()

        await create_course_generation_jobs_table(mock_cursor)

        # Should execute CREATE TABLE and CREATE INDEX
        assert mock_cursor.execute.call_count == 2
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any(
            "CREATE TABLE IF NOT EXISTS course_generation_jobs" in call
            for call in calls
        )

    async def test_create_task_generation_jobs_table(self):
        """Test creating task generation jobs table."""
        mock_cursor = AsyncMock()

        await create_task_generation_jobs_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any(
            "CREATE TABLE IF NOT EXISTS task_generation_jobs" in call for call in calls
        )

    async def test_create_code_drafts_table(self):
        """Test creating code drafts table."""
        mock_cursor = AsyncMock()

        await create_code_drafts_table(mock_cursor)

        # Should execute CREATE TABLE and 2 CREATE INDEX statements
        assert mock_cursor.execute.call_count == 3
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS code_drafts" in call for call in calls)


@pytest.mark.asyncio
class TestDatabaseInitialization:
    """Test database initialization functions."""

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.os.makedirs")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    async def test_init_db_creates_database_directory(
        self,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_makedirs,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db creates database directory if it doesn't exist."""
        mock_exists.return_value = False
        mock_path_exists.return_value = False  # Directory doesn't exist
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn
        mock_check_table.return_value = False

        await init_db()

        mock_makedirs.assert_called_once_with("/test/path")

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.os.makedirs")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    async def test_init_db_skips_directory_creation_if_exists(
        self,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_makedirs,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db skips directory creation if it exists."""
        mock_exists.return_value = True
        mock_path_exists.return_value = True  # Directory exists
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn
        mock_check_table.return_value = False

        await init_db()

        mock_makedirs.assert_not_called()

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    async def test_init_db_creates_all_tables(
        self,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db creates all required tables."""
        mock_exists.return_value = False  # Database doesn't exist, so create all tables
        mock_path_exists.return_value = True  # Directory exists, so don't create it
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn
        mock_check_table.return_value = False

        await init_db()

        # Verify that cursor.execute was called multiple times (for all table creations)
        assert mock_cursor.execute.call_count > 20  # We have many tables to create
        mock_conn.commit.assert_called_once()

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    async def test_init_db_sets_defaults(
        self,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db calls set_db_defaults."""
        mock_exists.return_value = False  # Database doesn't exist, so set defaults
        mock_path_exists.return_value = True  # Directory exists, so don't create it
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn
        mock_check_table.return_value = False

        await init_db()

        mock_set_defaults.assert_called_once()

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    async def test_init_db_existing_db_creates_missing_code_drafts_table(
        self,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db creates code_drafts table if database exists but table is missing."""
        mock_exists.return_value = True  # Database exists
        mock_path_exists.return_value = True  # Directory exists
        mock_check_table.return_value = False  # code_drafts table doesn't exist
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn

        await init_db()

        # Should create code_drafts table (CREATE TABLE + 2 CREATE INDEX statements)
        assert mock_cursor.execute.call_count == 3
        mock_conn.commit.assert_called_once()
        # Should not set defaults when database already exists
        mock_set_defaults.assert_not_called()

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    async def test_init_db_existing_db_with_all_tables(
        self,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db does nothing when database and all tables exist."""
        mock_exists.return_value = True  # Database exists
        mock_path_exists.return_value = True  # Directory exists
        mock_check_table.return_value = True  # code_drafts table exists
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn

        await init_db()

        # Should only commit, no table creation
        mock_cursor.execute.assert_not_called()
        mock_conn.commit.assert_called_once()
        # Should not set defaults when database already exists
        mock_set_defaults.assert_not_called()

    @patch("src.api.db.sqlite_db_path", "/test/path/test.db")
    @patch("src.api.db.exists")
    @patch("src.api.db.os.path.exists")
    @patch("src.api.db.get_new_db_connection")
    @patch("src.api.db.check_table_exists")
    @patch("src.api.db.set_db_defaults")
    @patch("src.api.db.os.remove")
    async def test_init_db_exception_handling_removes_db(
        self,
        mock_remove,
        mock_set_defaults,
        mock_check_table,
        mock_get_conn,
        mock_path_exists,
        mock_exists,
    ):
        """Test that init_db removes database file when exception occurs during table creation."""
        mock_exists.return_value = False  # Database doesn't exist
        mock_path_exists.return_value = True  # Directory exists
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn
        mock_check_table.return_value = False

        # Make cursor.execute raise an exception
        test_exception = Exception("Database error")
        mock_cursor.execute.side_effect = test_exception

        with pytest.raises(Exception) as exc_info:
            await init_db()

        assert exc_info.value == test_exception
        mock_remove.assert_called_once_with("/test/path/test.db")

    @patch("src.api.db.get_new_db_connection")
    async def test_delete_useless_tables_drops_tables(self, mock_get_conn):
        """Test deleting useless tables."""
        mock_cursor = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__aenter__.return_value = mock_conn
        mock_get_conn.return_value = mock_conn

        # Mock table info queries to return empty results (no columns)
        mock_cursor.fetchall.return_value = []

        await delete_useless_tables()

        # Should execute DROP TABLE statements
        assert mock_cursor.execute.call_count >= 8  # At least 8 DROP TABLE statements
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("DROP TABLE" in call for call in calls)
