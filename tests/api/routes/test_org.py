import pytest
from fastapi import status
from unittest.mock import patch, AsyncMock


@pytest.mark.asyncio
async def test_create_organization_success(client, mock_db):
    """
    Test successful organization creation
    """
    with patch(
        "api.routes.org.create_organization_with_user"
    ) as mock_create_org, patch("api.routes.org.get_new_db_connection") as mock_db_conn:
        # Setup connection mock to use our test cursor
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {
            "name": "Test Organization",
            "slug": "test-org",
            "user_id": 1,
        }

        # Mock organization creation
        mock_create_org.return_value = 123  # Mock org_id

        # Make request
        response = client.post("/organizations/", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": 123}

        # Verify mocks called correctly
        mock_create_org.assert_called_with(
            request_data["name"],
            request_data["slug"],
            request_data["user_id"],
        )


@pytest.mark.asyncio
async def test_create_organization_error(client, mock_db):
    """
    Test organization creation with an error
    """
    with patch(
        "api.routes.org.create_organization_with_user"
    ) as mock_create_org, patch("api.routes.org.get_new_db_connection") as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {
            "name": "Test Organization",
            "slug": "test-org",
            "user_id": 1,
        }

        # Mock organization creation to raise an error
        mock_create_org.side_effect = Exception(
            "Organization with this slug already exists"
        )

        # Make request
        response = client.post("/organizations/", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Organization with this slug already exists" in response.json()["detail"]

        # Verify mocks called correctly
        mock_create_org.assert_called_with(
            request_data["name"],
            request_data["slug"],
            request_data["user_id"],
        )


@pytest.mark.asyncio
async def test_get_org_by_id_success(client, mock_db):
    """
    Test getting organization by ID successfully
    """
    with patch("api.routes.org.get_org_by_id_from_db") as mock_get_org, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Mock org details
        org_details = {
            "id": 123,
            "name": "Test Organization",
            "slug": "test-org",
            "created_at": "2023-01-01T00:00:00",
        }
        mock_get_org.return_value = org_details

        # Make request
        response = client.get("/organizations/123")

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == org_details

        # Verify mock called correctly
        mock_get_org.assert_called_with(123)


@pytest.mark.asyncio
async def test_get_org_by_id_not_found(client, mock_db):
    """
    Test getting organization by ID when it doesn't exist
    """
    with patch("api.routes.org.get_org_by_id_from_db") as mock_get_org, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Mock org not found
        mock_get_org.return_value = None

        # Make request
        response = client.get("/organizations/999")

        # Verify response
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "Organization not found" in response.json()["detail"]

        # Verify mock called correctly
        mock_get_org.assert_called_with(999)


@pytest.mark.asyncio
async def test_get_org_by_slug_success(client, mock_db):
    """
    Test getting organization by slug successfully
    """
    with patch("api.routes.org.get_org_by_slug_from_db") as mock_get_org, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Mock org details
        org_details = {
            "id": 123,
            "name": "Test Organization",
            "slug": "test-org",
            "created_at": "2023-01-01T00:00:00",
        }
        mock_get_org.return_value = org_details

        # Make request
        response = client.get("/organizations/slug/test-org")

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == org_details

        # Verify mock called correctly
        mock_get_org.assert_called_with("test-org")


@pytest.mark.asyncio
async def test_get_org_by_slug_not_found(client, mock_db):
    """
    Test getting organization by slug when it doesn't exist
    """
    with patch("api.routes.org.get_org_by_slug_from_db") as mock_get_org, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Mock org not found
        mock_get_org.return_value = None

        # Make request
        response = client.get("/organizations/slug/nonexistent")

        # Verify response
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "Organization not found" in response.json()["detail"]

        # Verify mock called correctly
        mock_get_org.assert_called_with("nonexistent")


@pytest.mark.asyncio
async def test_update_org_success(client, mock_db):
    """
    Test updating organization successfully
    """
    with patch("api.routes.org.update_org_in_db") as mock_update_org, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {"name": "Updated Organization Name"}

        # Mock update success
        mock_update_org.return_value = None

        # Make request
        response = client.put("/organizations/123", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}

        # Verify mock called correctly
        mock_update_org.assert_called_with(123, request_data["name"])


@pytest.mark.asyncio
async def test_update_org_openai_api_key_success(client, mock_db):
    """
    Test updating organization OpenAI API key successfully
    """
    with patch(
        "api.routes.org.update_org_openai_api_key_in_db"
    ) as mock_update_key, patch("api.routes.org.get_new_db_connection") as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {
            "encrypted_openai_api_key": "encrypted_key_value",
            "is_free_trial": False,
        }

        # Mock update success
        mock_update_key.return_value = None

        # Make request
        response = client.put("/organizations/123/openai_api_key", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}

        # Verify mock called correctly
        mock_update_key.assert_called_with(
            123, request_data["encrypted_openai_api_key"], request_data["is_free_trial"]
        )


@pytest.mark.asyncio
async def test_add_users_to_org_by_email_success(client, mock_db):
    """
    Test adding users to organization by email successfully
    """
    with patch(
        "api.routes.org.add_users_to_org_by_email_in_db"
    ) as mock_add_users, patch("api.routes.org.get_new_db_connection") as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {"emails": ["user1@example.com", "user2@example.com"]}

        # Mock add users success
        mock_add_users.return_value = None

        # Make request
        response = client.post("/organizations/123/members", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}

        # Verify mock called correctly
        mock_add_users.assert_called_with(123, request_data["emails"])


@pytest.mark.asyncio
async def test_add_users_to_org_by_email_error(client, mock_db):
    """
    Test adding users to organization by email with an error
    """
    with patch(
        "api.routes.org.add_users_to_org_by_email_in_db"
    ) as mock_add_users, patch("api.routes.org.get_new_db_connection") as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {"emails": ["invalid@example.com"]}

        # Mock add users error
        mock_add_users.side_effect = Exception("User not found")

        # Make request
        response = client.post("/organizations/123/members", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "User not found" in response.json()["detail"]

        # Verify mock called correctly
        mock_add_users.assert_called_with(123, request_data["emails"])


@pytest.mark.asyncio
async def test_remove_members_from_org_success(client, mock_db):
    """
    Test removing members from organization successfully
    """
    with patch(
        "api.routes.org.remove_members_from_org_from_db"
    ) as mock_remove_members, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {"user_ids": [1, 2, 3]}

        # Mock remove members success
        mock_remove_members.return_value = None

        # Make request - Use data parameter instead of json for DELETE requests
        response = client.request(
            "DELETE",
            "/organizations/123/members",
            json=request_data,
            headers={"Content-Type": "application/json"},
        )

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}

        # Verify mock called correctly
        mock_remove_members.assert_called_with(123, request_data["user_ids"])


@pytest.mark.asyncio
async def test_get_org_members_success(client, mock_db):
    """
    Test getting organization members successfully
    """
    with patch("api.routes.org.get_org_members_from_db") as mock_get_members, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Mock members data
        members_data = [
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "role": "member",
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "email": "jane@example.com",
                "role": "admin",
            },
        ]
        mock_get_members.return_value = members_data

        # Make request
        response = client.get("/organizations/123/members")

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == members_data

        # Verify mock called correctly
        mock_get_members.assert_called_with(123)


@pytest.mark.asyncio
async def test_get_all_orgs_success(client, mock_db):
    """
    Test getting all organizations successfully
    """
    with patch("api.routes.org.get_all_orgs_from_db") as mock_get_all_orgs, patch(
        "api.routes.org.get_new_db_connection"
    ) as mock_db_conn:
        # Setup connection mock
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Mock organizations data
        orgs_data = [
            {
                "id": 1,
                "name": "Organization 1",
                "slug": "org-1",
                "created_at": "2023-01-01T00:00:00Z",
            },
            {
                "id": 2,
                "name": "Organization 2",
                "slug": "org-2",
                "created_at": "2023-01-02T00:00:00Z",
            },
        ]
        mock_get_all_orgs.return_value = orgs_data

        # Make request
        response = client.get("/organizations/")

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == orgs_data

        # Verify mock called correctly
        mock_get_all_orgs.assert_called_once()
