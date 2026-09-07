import base64
import hashlib
import hmac
import json
import time

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from api.main import app
from api.settings import settings

SECRET = "test-ws-secret"


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def make_ticket(course_id, user_id=1, aud="ws", exp_delta=30, secret=SECRET, iat_delta=0):
    now = int(time.time())
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64(
        json.dumps(
            {
                "sub": str(user_id),
                "aud": aud,
                "course_id": course_id,
                "iat": now + iat_delta,
                "exp": now + exp_delta,
            },
            separators=(",", ":"),
        ).encode()
    )
    signing_input = f"{header}.{payload}".encode()
    sig = _b64(hmac.new(secret.encode(), signing_input, hashlib.sha256).digest())
    return f"{header}.{payload}.{sig}"


@pytest.fixture
def ws_client(monkeypatch):
    monkeypatch.setattr(settings, "auth_secret_key", SECRET)
    return TestClient(app)


def _connect(client, course_id, ticket=None):
    url = f"/ws/course/{course_id}/generation"
    if ticket is not None:
        url += f"?ticket={ticket}"
    return client.websocket_connect(url)


def test_valid_ticket_is_accepted(ws_client):
    with _connect(ws_client, 7, make_ticket(7)) as ws:
        assert ws is not None


def test_missing_ticket_is_rejected(ws_client):
    with pytest.raises(WebSocketDisconnect):
        with _connect(ws_client, 7) as ws:
            ws.receive_text()


def test_garbage_ticket_is_rejected(ws_client):
    with pytest.raises(WebSocketDisconnect):
        with _connect(ws_client, 7, "not-a-ticket") as ws:
            ws.receive_text()


def test_ticket_for_another_course_is_rejected(ws_client):
    with pytest.raises(WebSocketDisconnect):
        with _connect(ws_client, 7, make_ticket(8)) as ws:
            ws.receive_text()


def test_expired_ticket_is_rejected(ws_client):
    with pytest.raises(WebSocketDisconnect):
        with _connect(ws_client, 7, make_ticket(7, exp_delta=-1)) as ws:
            ws.receive_text()


def test_ticket_signed_with_wrong_secret_is_rejected(ws_client):
    with pytest.raises(WebSocketDisconnect):
        with _connect(ws_client, 7, make_ticket(7, secret="wrong-secret")) as ws:
            ws.receive_text()


def test_access_token_cannot_be_used_as_ticket(ws_client):
    with pytest.raises(WebSocketDisconnect):
        with _connect(ws_client, 7, make_ticket(7, aud="access")) as ws:
            ws.receive_text()


def test_http_get_on_ws_path_requires_auth(ws_client):
    response = ws_client.get("/ws/course/7/generation")
    assert response.status_code == 401
