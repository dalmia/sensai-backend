import pytest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
from fastapi import HTTPException

import jwt as pyjwt

from api.auth.jwt import create_access_token, decode_access_token
from api.auth.constants import JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

TEST_SECRET = "test-secret-key-for-unit-tests"


@pytest.fixture(autouse=True)
def mock_jwt_secret():
    """Ensure all tests use a known secret."""
    with patch("api.auth.jwt.get_settings") as mock_settings:
        mock_settings.return_value.jwt_secret = TEST_SECRET
        yield


class TestCreateAccessToken:
    def test_returns_token_response(self):
        result = create_access_token(user_id=42, email="alice@example.com")
        assert result.access_token
        assert result.token_type == "bearer"
        assert result.expires_in == ACCESS_TOKEN_EXPIRE_MINUTES * 60

    def test_token_contains_correct_claims(self):
        result = create_access_token(user_id=42, email="alice@example.com")
        payload = pyjwt.decode(result.access_token, TEST_SECRET, algorithms=[JWT_ALGORITHM])

        assert payload["sub"] == "42"
        assert payload["email"] == "alice@example.com"
        assert payload["type"] == "access"
        assert "iat" in payload
        assert "exp" in payload

    def test_token_expiry_is_correct(self):
        result = create_access_token(user_id=1, email="a@b.com")
        payload = pyjwt.decode(result.access_token, TEST_SECRET, algorithms=[JWT_ALGORITHM])

        issued = datetime.fromtimestamp(payload["iat"], tz=timezone.utc)
        expires = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        delta = expires - issued

        assert timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES - 1) < delta
        assert delta < timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES + 1)

    def test_raises_when_no_secret(self):
        with patch("api.auth.jwt.get_settings") as mock_settings:
            mock_settings.return_value.jwt_secret = None
            with pytest.raises(HTTPException) as exc_info:
                create_access_token(user_id=1, email="a@b.com")
            assert exc_info.value.status_code == 500


class TestDecodeAccessToken:
    def test_decodes_valid_token(self):
        token_resp = create_access_token(user_id=99, email="bob@example.com")
        payload = decode_access_token(token_resp.access_token)

        assert payload["sub"] == "99"
        assert payload["email"] == "bob@example.com"
        assert payload["type"] == "access"

    def test_rejects_expired_token(self):
        past = datetime.now(timezone.utc) - timedelta(hours=2)
        payload = {
            "sub": "1",
            "email": "a@b.com",
            "type": "access",
            "iat": past,
            "exp": past + timedelta(minutes=1),
        }
        token = pyjwt.encode(payload, TEST_SECRET, algorithm=JWT_ALGORITHM)

        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(token)
        assert exc_info.value.status_code == 401
        assert "expired" in exc_info.value.detail.lower()

    def test_rejects_wrong_signature(self):
        token_resp = create_access_token(user_id=1, email="a@b.com")
        # Tamper with the token
        tampered = token_resp.access_token[:-4] + "XXXX"

        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(tampered)
        assert exc_info.value.status_code == 401

    def test_rejects_wrong_token_type(self):
        payload = {
            "sub": "1",
            "email": "a@b.com",
            "type": "refresh",  # wrong type
            "iat": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        }
        token = pyjwt.encode(payload, TEST_SECRET, algorithm=JWT_ALGORITHM)

        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(token)
        assert exc_info.value.status_code == 401
        assert "type" in exc_info.value.detail.lower()

    def test_rejects_missing_sub(self):
        payload = {
            "email": "a@b.com",
            "type": "access",
            "iat": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        }
        token = pyjwt.encode(payload, TEST_SECRET, algorithm=JWT_ALGORITHM)

        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(token)
        assert exc_info.value.status_code == 401
