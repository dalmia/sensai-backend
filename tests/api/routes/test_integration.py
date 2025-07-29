import pytest
from fastapi import status
from datetime import datetime, timezone
from unittest.mock import patch, AsyncMock


@pytest.mark.asyncio
async def test_create_integration_endpoint_success(client, mock_db):
    """Test successful creation of integration via API endpoint."""
    # Setup mock
    integration_id = 1
    expected_integration = {
        "id": integration_id,
        "user_id": 1,
        "integration_type": "slack",
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "expires_at": "2024-01-01T12:00:00Z",
        "created_at": "2024-01-01T10:00:00Z"
    }
    
    with patch("api.routes.integration.create_integration") as mock_create, \
         patch("api.routes.integration.get_integration") as mock_get:
        
        mock_create.return_value = integration_id
        mock_get.return_value = expected_integration

        # Test data
        request_data = {
            "user_id": 1,
            "integration_type": "slack",
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "expires_at": "2024-01-01T12:00:00+00:00"
        }

        response = client.post("/integrations/", json=request_data)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integration
        mock_create.assert_called_once()
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_create_integration_endpoint_upsert_success(client, mock_db):
    """Test successful upsert of existing integration via API endpoint."""
    # Setup mock
    integration_id = 2
    expected_integration = {
        "id": integration_id,
        "user_id": 1,
        "integration_type": "slack",
        "access_token": "updated_access_token",
        "refresh_token": "updated_refresh_token",
        "expires_at": "2024-01-01T12:00:00Z",
        "created_at": "2024-01-01T10:00:00Z"
    }
    
    with patch("api.routes.integration.create_integration") as mock_create, \
         patch("api.routes.integration.get_integration") as mock_get:
        
        mock_create.return_value = integration_id
        mock_get.return_value = expected_integration

        # Test data for existing integration
        request_data = {
            "user_id": 1,
            "integration_type": "slack",
            "access_token": "updated_access_token",
            "refresh_token": "updated_refresh_token",
            "expires_at": "2024-01-01T12:00:00+00:00"
        }

        response = client.post("/integrations/", json=request_data)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integration
        mock_create.assert_called_once()
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_create_integration_endpoint_failure(client, mock_db):
    """Test failure when integration creation succeeds but retrieval fails."""
    with patch("api.routes.integration.create_integration") as mock_create, \
         patch("api.routes.integration.get_integration") as mock_get:
        
        mock_create.return_value = 1
        mock_get.return_value = None  # Integration not found after creation

        request_data = {
            "user_id": 1,
            "integration_type": "slack",
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "expires_at": "2024-01-01T12:00:00+00:00"
        }

        response = client.post("/integrations/", json=request_data)

        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "Failed to create integration"}


@pytest.mark.asyncio
async def test_get_integration_endpoint_success(client, mock_db):
    """Test successful retrieval of integration via API endpoint."""
    integration_id = 1
    expected_integration = {
        "id": integration_id,
        "user_id": 1,
        "integration_type": "slack",
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "expires_at": "2024-01-01T12:00:00Z",
        "created_at": "2024-01-01T10:00:00Z"
    }
    
    with patch("api.routes.integration.get_integration") as mock_get:
        mock_get.return_value = expected_integration

        response = client.get(f"/integrations/{integration_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integration
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_get_integration_endpoint_not_found(client, mock_db):
    """Test retrieval of non-existent integration via API endpoint."""
    integration_id = 999
    
    with patch("api.routes.integration.get_integration") as mock_get:
        mock_get.return_value = None

        response = client.get(f"/integrations/{integration_id}")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Integration not found"}
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_list_integrations_endpoint_all(client, mock_db):
    """Test listing all integrations via API endpoint."""
    expected_integrations = [
        {
            "id": 1,
            "user_id": 1,
            "integration_type": "slack",
            "access_token": "token1",
            "refresh_token": "refresh1",
            "expires_at": "2024-01-01T12:00:00Z",
            "created_at": "2024-01-01T10:00:00Z"
        },
        {
            "id": 2,
            "user_id": 1,
            "integration_type": "github",
            "access_token": "token2",
            "refresh_token": "refresh2",
            "expires_at": "2024-01-01T12:00:00Z",
            "created_at": "2024-01-01T10:00:00Z"
        }
    ]
    
    with patch("api.routes.integration.list_integrations") as mock_list:
        mock_list.return_value = expected_integrations

        response = client.get("/integrations/")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integrations
        mock_list.assert_called_once_with(user_id=None)


@pytest.mark.asyncio
async def test_list_integrations_endpoint_by_user_id(client, mock_db):
    """Test listing integrations filtered by user ID via API endpoint."""
    user_id = 1
    expected_integrations = [
        {
            "id": 1,
            "user_id": user_id,
            "integration_type": "slack",
            "access_token": "token1",
            "refresh_token": "refresh1",
            "expires_at": "2024-01-01T12:00:00Z",
            "created_at": "2024-01-01T10:00:00Z"
        }
    ]
    
    with patch("api.routes.integration.list_integrations") as mock_list:
        mock_list.return_value = expected_integrations

        response = client.get(f"/integrations/?user_id={user_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integrations
        mock_list.assert_called_once_with(user_id=user_id)


@pytest.mark.asyncio
async def test_list_integrations_endpoint_empty(client, mock_db):
    """Test listing integrations when none exist via API endpoint."""
    with patch("api.routes.integration.list_integrations") as mock_list:
        mock_list.return_value = []

        response = client.get("/integrations/")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == []
        mock_list.assert_called_once_with(user_id=None)


@pytest.mark.asyncio
async def test_update_integration_endpoint_success(client, mock_db):
    """Test successful update of integration via API endpoint."""
    integration_id = 1
    expected_integration = {
        "id": integration_id,
        "user_id": 1,
        "integration_type": "slack",
        "access_token": "updated_access_token",
        "refresh_token": "updated_refresh_token",
        "expires_at": "2024-01-01T12:00:00Z",
        "created_at": "2024-01-01T10:00:00Z"
    }
    
    with patch("api.routes.integration.update_integration") as mock_update, \
         patch("api.routes.integration.get_integration") as mock_get:
        
        mock_update.return_value = True
        mock_get.return_value = expected_integration

        # Test data
        request_data = {
            "access_token": "updated_access_token",
            "refresh_token": "updated_refresh_token",
            "expires_at": "2024-01-01T12:00:00+00:00"
        }

        response = client.put(f"/integrations/{integration_id}", json=request_data)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integration
        # Don't check the exact call since it will be a Pydantic model
        mock_update.assert_called_once()
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_update_integration_endpoint_partial_update(client, mock_db):
    """Test partial update of integration via API endpoint."""
    integration_id = 1
    expected_integration = {
        "id": integration_id,
        "user_id": 1,
        "integration_type": "slack",
        "access_token": "updated_access_token",
        "refresh_token": "test_refresh_token",
        "expires_at": "2024-01-01T12:00:00Z",
        "created_at": "2024-01-01T10:00:00Z"
    }
    
    with patch("api.routes.integration.update_integration") as mock_update, \
         patch("api.routes.integration.get_integration") as mock_get:
        
        mock_update.return_value = True
        mock_get.return_value = expected_integration

        # Test data with only access_token
        request_data = {
            "access_token": "updated_access_token"
        }

        response = client.put(f"/integrations/{integration_id}", json=request_data)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integration
        # Don't check the exact call since it will be a Pydantic model
        mock_update.assert_called_once()
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_update_integration_endpoint_not_found(client, mock_db):
    """Test update of non-existent integration via API endpoint."""
    integration_id = 999
    
    with patch("api.routes.integration.update_integration") as mock_update:
        mock_update.return_value = False

        request_data = {
            "access_token": "updated_access_token",
            "refresh_token": "updated_refresh_token",
            "expires_at": "2024-01-01T12:00:00+00:00"
        }

        response = client.put(f"/integrations/{integration_id}", json=request_data)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Integration not found or not updated"}
        # Don't check the exact call since it will be a Pydantic model
        mock_update.assert_called_once()


@pytest.mark.asyncio
async def test_delete_integration_endpoint_success(client, mock_db):
    """Test successful deletion of integration via API endpoint."""
    integration_id = 1
    
    with patch("api.routes.integration.delete_integration") as mock_delete:
        mock_delete.return_value = True

        response = client.delete(f"/integrations/{integration_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_delete.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_delete_integration_endpoint_not_found(client, mock_db):
    """Test deletion of non-existent integration via API endpoint."""
    integration_id = 999
    
    with patch("api.routes.integration.delete_integration") as mock_delete:
        mock_delete.return_value = False

        response = client.delete(f"/integrations/{integration_id}")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Integration not found or not deleted"}
        mock_delete.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_create_integration_endpoint_with_null_values(client, mock_db):
    """Test creating integration with null values via API endpoint."""
    integration_id = 1
    expected_integration = {
        "id": integration_id,
        "user_id": 1,
        "integration_type": "slack",
        "access_token": "test_access_token",
        "refresh_token": None,
        "expires_at": None,
        "created_at": "2024-01-01T10:00:00Z"
    }
    
    with patch("api.routes.integration.create_integration") as mock_create, \
         patch("api.routes.integration.get_integration") as mock_get:
        
        mock_create.return_value = integration_id
        mock_get.return_value = expected_integration

        # Test data with null values
        request_data = {
            "user_id": 1,
            "integration_type": "slack",
            "access_token": "test_access_token",
            "refresh_token": None,
            "expires_at": None
        }

        response = client.post("/integrations/", json=request_data)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_integration
        mock_create.assert_called_once()
        mock_get.assert_called_once_with(integration_id)


@pytest.mark.asyncio
async def test_create_integration_endpoint_validation_error(client, mock_db):
    """Test validation error when creating integration with invalid data."""
    # Test data with missing required fields
    request_data = {
        "user_id": 1,
        # Missing integration_type and access_token
        "refresh_token": "test_refresh_token"
    }

    response = client.post("/integrations/", json=request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.asyncio
async def test_update_integration_endpoint_validation_error(client, mock_db):
    """Test validation error when updating integration with invalid data."""
    integration_id = 1
    
    # Test data with invalid field types
    request_data = {
        "access_token": 123,  # Should be string
        "expires_at": "invalid_date"  # Should be valid datetime
    }

    response = client.put(f"/integrations/{integration_id}", json=request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY 