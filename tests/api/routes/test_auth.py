import pytest
from fastapi import status
from unittest.mock import patch, MagicMock, AsyncMock

TEST_JWT_SECRET = "test-secret-for-auth-route-tests"


@pytest.fixture(autouse=True)
def mock_jwt_secret_for_auth():
    with patch("api.auth.jwt.get_settings") as mock_settings:
        mock_settings.return_value.jwt_secret = TEST_JWT_SECRET
        yield


@pytest.mark.asyncio
async def test_login_or_signup_user_success(client, mock_db):
    """
    Test successful login or signup — now returns JWT fields.
    """
    # Mock Google token verification
    with patch("api.routes.auth.id_token.verify_oauth2_token") as mock_verify, patch(
        "api.routes.auth.insert_or_return_user"
    ) as mock_insert_user, patch(
        "api.routes.auth.get_new_db_connection"
    ) as mock_db_conn, patch(
        "api.routes.auth.settings.google_client_id", "mock-google-client-id"
    ):
        # Setup connection mock to use our test cursor
        conn_mock = AsyncMock()
        cursor_mock = mock_db["cursor"]
        conn_mock.cursor.return_value = cursor_mock
        mock_db_conn.return_value.__aenter__.return_value = conn_mock

        # Setup request data
        request_data = {
            "id_token": "valid_token",
            "email": "test@example.com",
            "given_name": "Test",
            "family_name": "User",
        }

        # Mock token verification response
        mock_verify.return_value = {
            "email": "test@example.com",
            "name": "Test User",
            "sub": "user123",
        }

        # Mock user insertion/retrieval
        expected_user = {
            "id": 1,
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "User",
        }
        mock_insert_user.return_value = expected_user

        # Make request
        response = client.post("/auth/login", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # User fields still present
        assert data["id"] == 1
        assert data["email"] == "test@example.com"
        assert data["first_name"] == "Test"

        # JWT fields now included
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert data["expires_in"] > 0

        # Verify mocks called correctly
        mock_verify.assert_called_once()
        mock_insert_user.assert_called_with(
            cursor_mock,
            request_data["email"],
            request_data["given_name"],
            request_data["family_name"],
        )


@pytest.mark.asyncio
async def test_login_or_signup_invalid_token(client, mock_db):
    """
    Test login with invalid token
    """
    with patch("api.routes.auth.id_token.verify_oauth2_token") as mock_verify, patch(
        "api.routes.auth.settings.google_client_id", "mock-google-client-id"
    ):

        # Setup request data
        request_data = {
            "id_token": "invalid_token",
            "email": "test@example.com",
            "given_name": "Test",
            "family_name": "User",
        }

        # Mock token verification to raise error
        mock_verify.side_effect = ValueError("Invalid token")

        # Make request
        response = client.post("/auth/login", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Invalid authentication token" in response.json()["detail"]

        # Verify mock called
        mock_verify.assert_called_once()


@pytest.mark.asyncio
async def test_login_or_signup_email_mismatch(client, mock_db):
    """
    Test login with email mismatch
    """
    with patch("api.routes.auth.id_token.verify_oauth2_token") as mock_verify, patch(
        "api.routes.auth.settings.google_client_id", "mock-google-client-id"
    ):

        # Setup request data
        request_data = {
            "id_token": "valid_token",
            "email": "test@example.com",
            "given_name": "Test",
            "family_name": "User",
        }

        # Mock token verification with different email
        mock_verify.return_value = {
            "email": "different@example.com",
            "name": "Test User",
            "sub": "user123",
        }

        # Make request
        response = client.post("/auth/login", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert (
            "Email in token doesn't match provided email" in response.json()["detail"]
        )

        # Verify mock called
        mock_verify.assert_called_once()


@pytest.mark.asyncio
async def test_login_or_signup_no_client_id(client, mock_db):
    """
    Test login when Google client ID is not configured
    """
    with patch("api.routes.auth.settings.google_client_id", None):

        # Setup request data
        request_data = {
            "id_token": "valid_token",
            "email": "test@example.com",
            "given_name": "Test",
            "family_name": "User",
        }

        # Make request
        response = client.post("/auth/login", json=request_data)

        # Verify response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Google Client ID not configured" in response.json()["detail"]
