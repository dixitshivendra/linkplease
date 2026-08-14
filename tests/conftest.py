import hashlib
import hmac as hmac_mod
import json as json_mod
import pytest
from app.config import settings

TEST_API_KEY = "test-secret-key-for-hmac"
TEST_WEBHOOK_SECRET = "test-webhook-secret"


@pytest.fixture(autouse=True)
def set_test_keys():
    orig_key = settings.API_KEY
    orig_secret = settings.WEBHOOK_SECRET
    settings.API_KEY = TEST_API_KEY
    settings.WEBHOOK_SECRET = TEST_WEBHOOK_SECRET
    yield
    settings.API_KEY = orig_key
    settings.WEBHOOK_SECRET = orig_secret


def sign_body(body: bytes) -> dict:
    sig = hmac_mod.new(TEST_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return {"X-PseudoGram-Signature": f"sha256={sig}"}


def webhook_post(client, payload: dict):
    body = json_mod.dumps(payload).encode()
    headers = sign_body(body)
    headers["Content-Type"] = "application/json"
    return client.post("/webhook", content=body, headers=headers)
