import time

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.settings import settings
from api.utils.tokens import (
    ACCESS_TOKEN_MAX_AGE_SECONDS,
    TokenError,
    create_access_token,
    decode_access_token,
)

SECRET = "test-secret-key-for-auth-middleware"


@pytest.fixture
def auth_on():
    original_secret = settings.auth_secret_key
    settings.auth_secret_key = SECRET
    yield
    settings.auth_secret_key = original_secret


@pytest.fixture
def anon(auth_on):
    return TestClient(app)


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


class TestUnauthenticatedIsRejected:
    def test_protected_route_without_token(self, anon):
        assert anon.get("/users/1").status_code == 401

    @pytest.mark.parametrize(
        "path",
        ["/users/1", "/organizations/", "/cohorts/", "/courses/", "/tasks/1", "/batches/"],
    )
    def test_every_router_is_protected(self, anon, path):
        assert anon.get(path).status_code == 401

    def test_health_is_public(self, anon):
        assert anon.get("/health").status_code == 200

    def test_malformed_authorization_header(self, anon):
        for header in [{"Authorization": "token abc"}, {"Authorization": "Bearer"}, {"Authorization": ""}]:
            assert anon.get("/users/1", headers=header).status_code == 401

    def test_garbage_token(self, anon):
        assert anon.get("/users/1", headers=bearer("not.a.token")).status_code == 401

    def test_token_signed_with_wrong_secret(self, anon):
        token = create_access_token(1, "a@b.com")
        settings.auth_secret_key = "a-different-secret"
        try:
            assert anon.get("/users/1", headers=bearer(token)).status_code == 401
        finally:
            settings.auth_secret_key = SECRET

    def test_algorithm_none_is_rejected(self, anon):
        import base64, json

        def seg(d):
            return base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip("=")

        now = int(time.time())
        forged = f"{seg({'alg': 'none', 'typ': 'JWT'})}.{seg({'sub': '1', 'iat': now, 'exp': now + 60})}."
        assert anon.get("/users/1", headers=bearer(forged)).status_code == 401

    def test_expired_token(self, anon):
        token = create_access_token(1, "a@b.com", ttl_seconds=-10)
        assert anon.get("/users/1", headers=bearer(token)).status_code == 401

    def test_missing_secret_fails_closed(self, anon):
        settings.auth_secret_key = None
        try:
            assert anon.get("/users/1").status_code == 500
        finally:
            settings.auth_secret_key = SECRET


class TestTokenDecoding:
    def test_roundtrip(self, auth_on):
        payload = decode_access_token(create_access_token(42, "x@y.com"))
        assert payload["user_id"] == 42
        assert payload["email"] == "x@y.com"

    def test_tampered_payload_rejected(self, auth_on):
        import base64, json

        header, payload, signature = create_access_token(1, "a@b.com").split(".")
        decoded = json.loads(base64.urlsafe_b64decode(payload + "=="))
        decoded["sub"] = "999"
        tampered = base64.urlsafe_b64encode(json.dumps(decoded).encode()).decode().rstrip("=")
        with pytest.raises(TokenError):
            decode_access_token(f"{header}.{tampered}.{signature}")

    def test_token_older_than_max_age_rejected(self, auth_on):
        token = create_access_token(1, "a@b.com", ttl_seconds=ACCESS_TOKEN_MAX_AGE_SECONDS + 600)
        import base64, json

        header, payload, _ = token.split(".")
        decoded = json.loads(base64.urlsafe_b64decode(payload + "=="))
        decoded["iat"] = int(time.time()) - (ACCESS_TOKEN_MAX_AGE_SECONDS + 60)
        from api.utils.tokens import _b64url_encode, _sign

        new_payload = _b64url_encode(json.dumps(decoded, separators=(",", ":")).encode())
        signature = _sign(f"{header}.{new_payload}".encode())
        with pytest.raises(TokenError):
            decode_access_token(f"{header}.{new_payload}.{signature}")

    def test_no_subject_rejected(self, auth_on):
        import base64, json
        from api.utils.tokens import _b64url_encode, _sign

        now = int(time.time())
        header = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
        payload = _b64url_encode(json.dumps({"iat": now, "exp": now + 60}, separators=(",", ":")).encode())
        signature = _sign(f"{header}.{payload}".encode())
        with pytest.raises(TokenError):
            decode_access_token(f"{header}.{payload}.{signature}")
