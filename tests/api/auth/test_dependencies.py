import pytest
from unittest.mock import patch, AsyncMock
from fastapi import HTTPException

from api.auth.dependencies import get_current_user, get_current_user_transitional
from api.auth.jwt import create_access_token

TEST_SECRET = "test-secret-key-for-unit-tests"

MOCK_USER_DB = {
    "id": 1,
    "email": "alice@example.com",
    "first_name": "Alice",
    "middle_name": None,
    "last_name": "Smith",
    "default_dp_color": "#abc",
    "created_at": "2024-01-01",
}


@pytest.fixture(autouse=True)
def mock_jwt_secret():
    with patch("api.auth.jwt.get_settings") as mock_settings:
        mock_settings.return_value.jwt_secret = TEST_SECRET
        yield


@pytest.fixture
def valid_token():
    return create_access_token(user_id=1, email="alice@example.com")


@pytest.fixture
def mock_get_user():
    with patch("api.auth.dependencies.get_user_by_id", new_callable=AsyncMock) as mock:
        mock.return_value = MOCK_USER_DB
        yield mock


class TestGetCurrentUser:
    @pytest.mark.asyncio
    async def test_valid_bearer_token(self, valid_token, mock_get_user):
        user = await get_current_user(
            authorization=f"Bearer {valid_token.access_token}"
        )
        assert user.id == 1
        assert user.email == "alice@example.com"
        mock_get_user.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_missing_bearer_prefix(self, valid_token):
        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(authorization=valid_token.access_token)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_invalid_token(self):
        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(authorization="Bearer not-a-real-token")
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_user_not_found(self, valid_token):
        with patch(
            "api.auth.dependencies.get_user_by_id",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await get_current_user(
                    authorization=f"Bearer {valid_token.access_token}"
                )
            assert exc_info.value.status_code == 401
            assert "not found" in exc_info.value.detail.lower()


class TestGetCurrentUserTransitional:
    @pytest.mark.asyncio
    async def test_prefers_jwt_over_user_id(self, valid_token, mock_get_user):
        user = await get_current_user_transitional(
            authorization=f"Bearer {valid_token.access_token}",
            user_id=999,  # should be ignored
        )
        assert user.id == 1  # from JWT, not 999
        mock_get_user.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_falls_back_to_user_id(self, mock_get_user):
        user = await get_current_user_transitional(
            authorization=None,
            user_id=1,
        )
        assert user.id == 1
        mock_get_user.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_rejects_no_auth(self):
        with pytest.raises(HTTPException) as exc_info:
            await get_current_user_transitional(
                authorization=None,
                user_id=None,
            )
        assert exc_info.value.status_code == 401
