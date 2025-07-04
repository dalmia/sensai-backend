import os
import sys
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

# Add the src directory to the Python path
root_dir = os.path.dirname(os.path.abspath(__file__)).replace("tests", "src")

if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from api.main import app


@pytest.fixture(autouse=True)
def mock_database_operations():
    """
    Auto-use fixture to mock all database operations to prevent real database access.
    """
    with patch("src.api.utils.db.get_new_db_connection") as mock_conn, patch(
        "src.api.utils.db.execute_db_operation"
    ) as mock_execute, patch(
        "src.api.utils.db.execute_many_db_operation"
    ) as mock_execute_many, patch(
        "src.api.utils.db.execute_multiple_db_operations"
    ) as mock_execute_multiple, patch(
        "src.api.db.init_db"
    ) as mock_init_db:

        # Setup default mock connection
        mock_conn_instance = AsyncMock()
        mock_cursor = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_conn.return_value = mock_conn_instance

        # Setup default return values
        mock_execute.return_value = None
        mock_execute_many.return_value = None
        mock_execute_multiple.return_value = None
        mock_init_db.return_value = None

        yield {
            "connection": mock_conn,
            "execute": mock_execute,
            "execute_many": mock_execute_many,
            "execute_multiple": mock_execute_multiple,
            "init_db": mock_init_db,
            "cursor": mock_cursor,
            "conn_instance": mock_conn_instance,
        }


@pytest.fixture
def client():
    """
    Create a test client for the FastAPI app.
    """
    return TestClient(app)


@pytest.fixture
def mock_db():
    """
    Returns a dictionary of mock database functions.
    Test functions can use these mocks to simulate different database responses.
    """
    # Dictionary to store all the mock functions for database operations
    db_mocks = {}

    # Mock DB operations with AsyncMock to handle async functions
    # Add more mocks as needed for other database functions
    with patch("api.routes.user.get_user_by_id_from_db") as get_user_mock, patch(
        "api.routes.user.get_user_cohorts_from_db"
    ) as get_cohorts_mock, patch(
        "api.routes.user.update_user_in_db"
    ) as update_user_mock, patch(
        "api.routes.user.is_user_in_cohort_from_db"
    ) as is_user_in_cohort_mock, patch(
        "api.routes.user.get_user_courses_from_db"
    ) as get_courses_mock, patch(
        "api.routes.user.get_user_active_in_last_n_days_from_db"
    ) as get_active_days_mock, patch(
        "api.routes.user.get_user_streak_from_db"
    ) as get_streak_mock, patch(
        "api.routes.user.get_user_organizations"
    ) as get_orgs_mock, patch(
        "api.routes.user.get_user_org_cohorts_from_db"
    ) as get_org_cohorts_mock, patch(
        "api.routes.user.get_user_activity_for_year_from_db"
    ) as get_activity_mock, patch(
        "api.routes.user.get_new_db_connection"
    ) as db_conn_mock, patch(
        "api.routes.org.create_organization_with_user"
    ) as create_org_mock, patch(
        "api.routes.org.get_org_by_id_from_db"
    ) as get_org_by_id_mock, patch(
        "api.routes.org.get_org_by_slug_from_db"
    ) as get_org_by_slug_mock, patch(
        "api.routes.org.update_org_in_db"
    ) as update_org_mock, patch(
        "api.routes.org.update_org_openai_api_key_in_db"
    ) as update_org_api_key_mock, patch(
        "api.routes.org.add_users_to_org_by_email_in_db"
    ) as add_users_to_org_mock, patch(
        "api.routes.org.remove_members_from_org_from_db"
    ) as remove_members_mock, patch(
        "api.routes.org.get_org_members_from_db"
    ) as get_org_members_mock:

        # Create a mock connection and cursor for database operations
        conn_mock = AsyncMock()
        cursor_mock = AsyncMock()
        conn_mock.cursor.return_value = cursor_mock
        db_conn_mock.return_value.__aenter__.return_value = conn_mock

        # Store mocks in dictionary for easy access
        db_mocks["get_user"] = get_user_mock
        db_mocks["get_cohorts"] = get_cohorts_mock
        db_mocks["update_user"] = update_user_mock
        db_mocks["is_user_in_cohort"] = is_user_in_cohort_mock
        db_mocks["get_courses"] = get_courses_mock
        db_mocks["get_user_courses"] = get_courses_mock  # Alias for test compatibility
        db_mocks["get_active_days"] = get_active_days_mock
        db_mocks["get_streak"] = get_streak_mock
        db_mocks["get_orgs"] = get_orgs_mock
        db_mocks["get_user_organizations"] = (
            get_orgs_mock  # Alias for test compatibility
        )
        db_mocks["get_org_cohorts"] = get_org_cohorts_mock
        db_mocks["get_user_org_cohorts"] = (
            get_org_cohorts_mock  # Alias for test compatibility
        )
        db_mocks["get_activity"] = get_activity_mock  # Add missing activity mock
        db_mocks["db_conn"] = db_conn_mock
        db_mocks["cursor"] = cursor_mock

        # Add organization related mocks
        db_mocks["create_org"] = create_org_mock
        db_mocks["get_org_by_id"] = get_org_by_id_mock
        db_mocks["get_org_by_slug"] = get_org_by_slug_mock
        db_mocks["update_org"] = update_org_mock
        db_mocks["update_org_api_key"] = update_org_api_key_mock
        db_mocks["add_users_to_org"] = add_users_to_org_mock
        db_mocks["remove_members"] = remove_members_mock
        db_mocks["get_org_members"] = get_org_members_mock

        yield db_mocks
