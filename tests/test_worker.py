import asyncio
import time
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import engine, Base, SessionLocal
from app.models import Rule, DMDelivery
from app.worker import (
    _claim_delivery,
    _handle_send_success,
    _handle_send_429,
    _handle_send_5xx,
    _handle_send_400,
    _backoff,
    _claim_queued_delivery,
    _handle_reconcile_delivered,
    _handle_reconcile_failed,
    _handle_reconcile_retry,
    _reconcile_one,
)
from app.services.rate_limiter import RollingRateLimiter
from app.services.dm_sender import send_dm, check_dm_status
from tests.conftest import webhook_post


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client():
    return TestClient(app)


def _insert_delivery(db, rule_id="r1", user_id="usr_1", comment_id="cmt_1", status="pending", dm_id=None, attempts=0, next_retry_at=None):
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        text(
            "INSERT INTO dm_deliveries (id, rule_id, user_id, comment_id, message, status, dm_id, attempts, next_retry_at, created_at, updated_at) "
            "VALUES (:id, :rule_id, :user_id, :comment_id, :message, :status, :dm_id, :attempts, :next_retry, :at, :at)"
        ),
        {"id": f"d_{user_id}_{comment_id}", "rule_id": rule_id, "user_id": user_id,
         "comment_id": comment_id, "message": "hello", "status": status,
         "dm_id": dm_id, "attempts": attempts, "next_retry": next_retry_at, "at": now},
    )
    db.commit()


def _mock_response(status_code, json_data=None, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = headers or {}
    return resp


# ── Claim delivery ────────────────────────────────────────────


class TestClaimDelivery:
    def test_claims_pending_delivery(self, db):
        _insert_delivery(db, status="pending")
        delivery = _claim_delivery(db)
        assert delivery is not None
        assert delivery["id"] == "d_usr_1_cmt_1"
        assert delivery["attempts"] == 1

    def test_claims_retrying_delivery(self, db):
        past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        _insert_delivery(db, status="retrying", next_retry_at=past, attempts=2)
        delivery = _claim_delivery(db)
        assert delivery is not None
        assert delivery["attempts"] == 3

    def test_skips_future_retry(self, db):
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        _insert_delivery(db, status="retrying", next_retry_at=future)
        delivery = _claim_delivery(db)
        assert delivery is None

    def test_skips_sending(self, db):
        _insert_delivery(db, status="sending")
        assert _claim_delivery(db) is None

    def test_skips_queued(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_123")
        assert _claim_delivery(db) is None

    def test_skips_failed(self, db):
        _insert_delivery(db, status="failed")
        assert _claim_delivery(db) is None

    def test_oldest_first(self, db):
        now = datetime.now(timezone.utc).isoformat()
        old = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        db.execute(
            text(
                "INSERT INTO dm_deliveries (id, rule_id, user_id, comment_id, message, status, attempts, created_at, updated_at) "
                "VALUES ('d_old', 'r1', 'usr_old', 'cmt_old', 'msg', 'pending', 0, :old, :old)"
            ),
            {"old": old},
        )
        _insert_delivery(db, user_id="usr_new", comment_id="cmt_new")
        db.commit()
        delivery = _claim_delivery(db)
        assert delivery["user_id"] == "usr_old"


# ── Handle success ────────────────────────────────────────────


class TestHandleSuccess:
    def test_sets_queued_with_dm_id(self, db):
        _insert_delivery(db)
        _handle_send_success(db, "d_usr_1_cmt_1", "dm_abc")
        row = db.execute(text("SELECT status, dm_id FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "queued"
        assert row["dm_id"] == "dm_abc"


# ── Handle 429 ────────────────────────────────────────────────


class TestHandle429:
    def test_retries_with_retry_after(self, db):
        _insert_delivery(db, attempts=1)
        _handle_send_429(db, "d_usr_1_cmt_1", retry_after=10.0, attempts=1)
        row = db.execute(text("SELECT status, next_retry_at FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "retrying"
        assert row["next_retry_at"] is not None

    def test_fails_after_max_retries(self, db):
        _insert_delivery(db, attempts=3)
        _handle_send_429(db, "d_usr_1_cmt_1", retry_after=10.0, attempts=3)
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "failed"


# ── Handle 5xx ────────────────────────────────────────────────


class TestHandle5xx:
    def test_retries_with_backoff(self, db):
        _insert_delivery(db, attempts=1)
        _handle_send_5xx(db, "d_usr_1_cmt_1", attempts=1)
        row = db.execute(text("SELECT status, next_retry_at FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "retrying"
        assert row["next_retry_at"] is not None

    def test_fails_after_max_retries(self, db):
        _insert_delivery(db, attempts=5)
        _handle_send_5xx(db, "d_usr_1_cmt_1", attempts=5)
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "failed"


# ── Handle 400 ────────────────────────────────────────────────


class TestHandle400:
    def test_marks_failed(self, db):
        _insert_delivery(db)
        _handle_send_400(db, "d_usr_1_cmt_1")
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "failed"


# ── Backoff ───────────────────────────────────────────────────


class TestBackoff:
    def test_general_increase_with_attempt(self):
        avg = [sum(_backoff(i) for _ in range(50)) / 50 for i in range(1, 5)]
        for i in range(len(avg) - 1):
            assert avg[i] < avg[i + 1], f"avg({i})={avg[i]} not < avg({i+1})={avg[i+1]}"

    def test_bounded_jitter(self):
        for _ in range(20):
            val = _backoff(3)
            base = min(2 ** 3, 16)
            assert 0.8 * base <= val <= 1.2 * base


# ── Rate limiter ──────────────────────────────────────────────


class TestRateLimiter:
    def test_allows_10_quickly(self):
        limiter = RollingRateLimiter(max_requests=10, window_seconds=60)

        async def run():
            for _ in range(10):
                await limiter.acquire()

        asyncio.get_event_loop().run_until_complete(run())

    def test_blocks_at_limit(self):
        limiter = RollingRateLimiter(max_requests=2, window_seconds=60)

        async def run():
            await limiter.acquire()
            await limiter.acquire()
            start = time.monotonic()
            await limiter.acquire()
            elapsed = time.monotonic() - start
            assert elapsed >= 0.5

        asyncio.get_event_loop().run_until_complete(run())


# ── Idempotency key ──────────────────────────────────────────


class TestIdempotencyKey:
    def test_key_format(self, db):
        _insert_delivery(db, rule_id="r_price", user_id="usr_a", comment_id="cmt_1")
        delivery = _claim_delivery(db)
        key = f"{delivery['rule_id']}:{delivery['user_id']}:{delivery['comment_id']}"
        assert key == "r_price:usr_a:cmt_1"


# ── Integration: full flow ───────────────────────────────────


class TestFullFlow:
    def test_webhook_to_queued(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()

        webhook_post(client, {
            "event_id": "evt_001",
            "event_type": "comment.created",
            "sent_at": "2026-08-10T09:14:22.481Z",
            "data": {
                "comment_id": "cmt_001",
                "post_id": "post_1",
                "text": "PRICE",
                "created_at": "2026-08-10T09:14:21.900Z",
                "from": {"user_id": "usr_1", "username": "test"},
            },
        })

        delivery = _claim_delivery(db)
        assert delivery is not None
        assert delivery["user_id"] == "usr_1"
        delivery_id = delivery["id"]

        mock_resp = _mock_response(202, {"dm_id": "dm_xyz", "status": "queued"})

        async def run():
            with patch("app.services.dm_sender.send_dm", new_callable=AsyncMock, return_value=mock_resp):
                await send_dm(
                    recipient_user_id=delivery["user_id"],
                    message=delivery["message"],
                    comment_id=delivery["comment_id"],
                    idempotency_key=f"{delivery['rule_id']}:{delivery['user_id']}:{delivery['comment_id']}",
                )
            _handle_send_success(db, delivery_id, "dm_xyz")

        asyncio.get_event_loop().run_until_complete(run())

        row = db.execute(text(f"SELECT status, dm_id FROM dm_deliveries WHERE id = '{delivery_id}'")).mappings().first()
        assert row is not None
        assert row["status"] == "queued"
        assert row["dm_id"] == "dm_xyz"


# ── Reconciliation ──────────────────────────────────────────


class TestReconciliation:
    def test_claim_queued_delivery(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r1")
        delivery = _claim_queued_delivery(db)
        assert delivery is not None
        assert delivery["dm_id"] == "dm_r1"

    def test_claim_skips_without_dm_id(self, db):
        _insert_delivery(db, status="queued", dm_id=None)
        assert _claim_queued_delivery(db) is None

    def test_claim_skips_pending(self, db):
        _insert_delivery(db, status="pending")
        assert _claim_queued_delivery(db) is None

    def test_claim_skips_future_reconcile(self, db):
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        _insert_delivery(db, status="queued", dm_id="dm_r2", next_retry_at=future)
        # next_reconcile_at is separate from next_retry_at — we need to set it directly
        db.execute(
            text("UPDATE dm_deliveries SET next_reconcile_at = :next WHERE id = :id"),
            {"next": future, "id": "d_usr_1_cmt_1"},
        )
        db.commit()
        assert _claim_queued_delivery(db) is None

    def test_handle_reconcile_delivered(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r3")
        _handle_reconcile_delivered(db, "d_usr_1_cmt_1")
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "delivered"

    def test_handle_reconcile_failed(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r4")
        _handle_reconcile_failed(db, "d_usr_1_cmt_1")
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "failed"

    def test_handle_reconcile_retry(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r5")
        _handle_reconcile_retry(db, "d_usr_1_cmt_1", delay_seconds=30)
        row = db.execute(text("SELECT status, next_reconcile_at FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "queued"
        assert row["next_reconcile_at"] is not None

    def test_reconcile_one_delivered(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r6")
        mock_resp = _mock_response(200, {"dm_id": "dm_r6", "status": "delivered"})

        async def run():
            with patch("app.worker.check_dm_status", new_callable=AsyncMock, return_value=mock_resp):
                return await _reconcile_one(db)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result is True
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "delivered"

    def test_reconcile_one_failed(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r7")
        mock_resp = _mock_response(200, {"dm_id": "dm_r7", "status": "failed"})

        async def run():
            with patch("app.worker.check_dm_status", new_callable=AsyncMock, return_value=mock_resp):
                return await _reconcile_one(db)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result is True
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "failed"

    def test_reconcile_one_429_retries(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r8")
        mock_resp = _mock_response(429, headers={"Retry-After": "10"})

        async def run():
            with patch("app.worker.check_dm_status", new_callable=AsyncMock, return_value=mock_resp):
                return await _reconcile_one(db)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result is True
        row = db.execute(text("SELECT status, next_reconcile_at FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "queued"
        assert row["next_reconcile_at"] is not None

    def test_reconcile_one_500_retries(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r9")
        mock_resp = _mock_response(500)

        async def run():
            with patch("app.worker.check_dm_status", new_callable=AsyncMock, return_value=mock_resp):
                return await _reconcile_one(db)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result is True
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "queued"

    def test_reconcile_one_network_error_retries(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_r10")

        async def run():
            with patch("app.worker.check_dm_status", new_callable=AsyncMock, side_effect=Exception("timeout")):
                return await _reconcile_one(db)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result is True
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "queued"

    def test_reconcile_one_no_queued_returns_false(self, db):
        _insert_delivery(db, status="pending")

        async def run():
            return await _reconcile_one(db)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result is False


# ── Cancelled status ───────────────────────────────────────────


class TestCancelledStatus:
    def test_cancelled_not_claimed_for_send(self, db):
        _insert_delivery(db, status="cancelled")
        assert _claim_delivery(db) is None

    def test_cancelled_not_claimed_for_reconcile(self, db):
        _insert_delivery(db, status="cancelled", dm_id="dm_c1")
        assert _claim_queued_delivery(db) is None
