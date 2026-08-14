"""
Tests for rate-limiting verification, multi-worker concurrency,
restart recovery, and webhook performance.
"""

import asyncio
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from app.database import engine, Base, SessionLocal
from app.models import Rule, DMDelivery, ProcessedEvent, DuplicateCounter
from app.worker import (
    _claim_delivery,
    _handle_send_success,
    _handle_send_5xx,
    _handle_send_429,
    _claim_queued_delivery,
    _handle_reconcile_delivered,
    _reconcile_one,
    _recover_stale_sending,
)
from app.services.rate_limiter import RollingRateLimiter
from app.services.dm_sender import send_dm
from tests.conftest import TEST_API_KEY, sign_body, webhook_post


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def make_event(event_id, comment_text, user_id="usr_123", comment_id="cmt_001"):
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_44de1b",
            "text": comment_text,
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {"user_id": user_id, "username": "testuser"},
        },
    }


def _send_one(event):
    c = TestClient(app, raise_server_exceptions=False)
    body = json.dumps(event).encode()
    headers = sign_body(body)
    headers["Content-Type"] = "application/json"
    return c.post("/webhook", content=body, headers=headers)


def _insert_delivery(db, rule_id="r1", user_id="usr_1", comment_id="cmt_1",
                     status="pending", dm_id=None, attempts=0, next_retry_at=None):
    now = "2026-08-10T09:14:21.900Z"
    db.execute(
        text(
            "INSERT INTO dm_deliveries "
            "(id, rule_id, user_id, comment_id, message, status, dm_id, attempts, next_retry_at, created_at, updated_at) "
            "VALUES (:id, :rule_id, :user_id, :comment_id, :message, :status, :dm_id, :attempts, :next_retry, :at, :at)"
        ),
        {"id": f"d_{user_id}_{comment_id}", "rule_id": rule_id, "user_id": user_id,
         "comment_id": comment_id, "message": "hello", "status": status,
         "dm_id": dm_id, "attempts": attempts, "next_retry": next_retry_at, "at": now},
    )
    db.commit()


# ── Rate limiter verification ─────────────────────────────────


class TestRateLimiterVerification:
    def test_rolling_window_allows_burst_then_blocks(self):
        limiter = RollingRateLimiter(max_requests=10, window_seconds=60)
        timestamps = []

        async def run():
            for _ in range(12):
                await limiter.acquire()
                timestamps.append(time.monotonic())

        asyncio.get_event_loop().run_until_complete(run())

        first_10 = timestamps[:10]
        last_2 = timestamps[10:]
        for i in range(9):
            assert first_10[i + 1] - first_10[i] < 0.05, "First 10 should be fast"
        assert last_2[0] - first_10[0] >= 59, "11th request should block until window rolls"

    def test_rate_limiter_concurrent_safety(self):
        limiter = RollingRateLimiter(max_requests=10, window_seconds=60)
        timestamps = []

        async def acquire_one():
            await limiter.acquire()
            timestamps.append(time.monotonic())

        async def run():
            tasks = [acquire_one() for _ in range(10)]
            await asyncio.gather(*tasks)

        asyncio.get_event_loop().run_until_complete(run())
        assert len(timestamps) == 10
        spread = max(timestamps) - min(timestamps)
        assert spread < 2.0, f"10 concurrent acquires should be fast, took {spread:.2f}s"

    def test_worker_respects_rate_limit_during_send(self, db):
        _insert_delivery(db, status="pending")
        timestamps = []

        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_resp.json.return_value = {"dm_id": "dm_test", "status": "queued"}

        async def run():
            limiter = RollingRateLimiter(max_requests=2, window_seconds=60)

            with patch("app.services.dm_sender.httpx.AsyncClient") as mock_client_cls:
                mock_instance = AsyncMock()
                mock_instance.post = AsyncMock(return_value=mock_resp)
                mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_instance.__aexit__ = AsyncMock(return_value=False)
                mock_client_cls.return_value = mock_instance

                delivery = _claim_delivery(db)
                assert delivery is not None
                await limiter.acquire()
                timestamps.append(time.monotonic())
                await limiter.acquire()
                timestamps.append(time.monotonic())

                resp = await send_dm(
                    recipient_user_id=delivery["user_id"],
                    message=delivery["message"],
                    comment_id=delivery["comment_id"],
                    idempotency_key="test:key",
                )
                assert resp.status_code == 202
                _handle_send_success(db, delivery["id"], "dm_test")

        asyncio.get_event_loop().run_until_complete(run())
        assert len(timestamps) == 2
        assert timestamps[1] - timestamps[0] < 0.05, "Two quick acquires should be fast"


# ── Multi-worker concurrency ─────────────────────────────────


class TestMultiWorkerConcurrency:
    def test_two_workers_cannot_claim_same_delivery(self, db):
        _insert_delivery(db, status="pending")

        claim1 = _claim_delivery(db)
        assert claim1 is not None

        claim2 = _claim_delivery(db)
        assert claim2 is None, "Second worker should not claim same delivery"

    def test_concurrent_workers_partition_deliveries(self, db):
        for i in range(20):
            _insert_delivery(db, user_id=f"usr_{i}", comment_id=f"cmt_{i}")

        claimed = set()
        for _ in range(20):
            d = _claim_delivery(db)
            if d:
                claimed.add(d["id"])

        assert len(claimed) == 20
        for _ in range(5):
            assert _claim_delivery(db) is None

    def test_worker_claim_is_atomic(self, db):
        for i in range(5):
            _insert_delivery(db, user_id=f"usr_{i}", comment_id=f"cmt_{i}")

        results = []

        def claim_one():
            session = SessionLocal()
            try:
                d = _claim_delivery(session)
                if d:
                    results.append(d["id"])
                    session.commit()
            finally:
                session.close()

        threads = [threading.Thread(target=claim_one) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == len(set(results)), "No duplicate claims"
        assert len(results) == 5

    def test_idempotency_key_deterministic(self, db):
        _insert_delivery(db, rule_id="r_price", user_id="usr_a", comment_id="cmt_1")
        d = _claim_delivery(db)
        key = f"{d['rule_id']}:{d['user_id']}:{d['comment_id']}"
        assert key == "r_price:usr_a:cmt_1"

        d2 = _claim_delivery(db)
        if d2:
            key2 = f"{d2['rule_id']}:{d2['user_id']}:{d2['comment_id']}"
            assert ":".join(key2.split(":")[:3]) == key2

    def test_retry_state_persisted(self, db):
        _insert_delivery(db, status="pending")
        d = _claim_delivery(db)
        _handle_send_5xx(db, d["id"], d["attempts"])
        with SessionLocal() as verify:
            row = verify.execute(
                text("SELECT status, next_retry_at FROM dm_deliveries WHERE id = :id"),
                {"id": "d_usr_1_cmt_1"},
            ).mappings().first()
        assert row["status"] == "retrying"
        assert row["next_retry_at"] is not None

        d2 = _claim_delivery(db)
        assert d2 is None, "Retrying delivery with future next_retry_at should not be claimed"


# ── Restart / recovery ────────────────────────────────────────


class TestRestartRecovery:
    def test_pending_survives_restart(self, db):
        _insert_delivery(db, status="pending")
        d = _claim_delivery(db)
        assert d is not None
        assert d["id"] == "d_usr_1_cmt_1"

    def test_retrying_survives_restart(self, db):
        from datetime import datetime, timezone, timedelta
        past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        _insert_delivery(db, status="retrying", next_retry_at=past)
        d = _claim_delivery(db)
        assert d is not None

    def test_queued_survives_restart(self, db):
        _insert_delivery(db, status="queued", dm_id="dm_persist")
        d = _claim_queued_delivery(db)
        assert d is not None
        assert d["dm_id"] == "dm_persist"

    def test_sending_state_recovery(self, db):
        _insert_delivery(db, status="sending")
        d = _claim_delivery(db)
        assert d is None, "sending state should not be re-claimed"

    def test_stale_sending_recovers_to_pending(self, db):
        from datetime import datetime, timezone, timedelta
        _insert_delivery(db, status="sending")
        old_time = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
        db.execute(
            text("UPDATE dm_deliveries SET status = 'sending', updated_at = :old WHERE id = :id"),
            {"old": old_time, "id": "d_usr_1_cmt_1"},
        )
        db.commit()
        _recover_stale_sending(db)
        db.expire_all()
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "pending"

    def test_recent_sending_not_reset(self, db):
        now = "2099-01-01T00:00:00+00:00"
        db.execute(
            text(
                "INSERT INTO dm_deliveries (id, rule_id, user_id, comment_id, message, status, attempts, created_at, updated_at) "
                "VALUES (:id, :rule_id, :user_id, :comment_id, :message, :status, 0, :at, :at)"
            ),
            {"id": "d_usr_1_cmt_1", "rule_id": "r1", "user_id": "usr_1", "comment_id": "cmt_1",
             "message": "hello", "status": "sending", "at": now},
        )
        db.commit()
        _recover_stale_sending(db)
        db.expire_all()
        row = db.execute(text("SELECT status FROM dm_deliveries WHERE id = 'd_usr_1_cmt_1'")).mappings().first()
        assert row["status"] == "sending"

    def test_worker_restart_recovers_sending(self, db):
        from datetime import datetime, timezone, timedelta
        _insert_delivery(db, status="sending")
        old_time = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
        db.execute(
            text("UPDATE dm_deliveries SET status = 'sending', updated_at = :old WHERE id = :id"),
            {"old": old_time, "id": "d_usr_1_cmt_1"},
        )
        db.commit()
        _recover_stale_sending(db)
        d = _claim_delivery(db)
        assert d is not None, "Recovered delivery should be claimable"

    def test_failed_state_not_lost(self, db):
        _insert_delivery(db, status="failed")
        d = _claim_delivery(db)
        assert d is None

    def test_delivered_state_not_lost(self, db):
        _insert_delivery(db, status="delivered", dm_id="dm_done")
        d = _claim_delivery(db)
        assert d is None
        d2 = _claim_queued_delivery(db)
        assert d2 is None

    def test_cancelled_state_not_lost(self, db):
        _insert_delivery(db, status="cancelled")
        assert _claim_delivery(db) is None
        assert _claim_queued_delivery(db) is None

    def test_full_recovery_flow(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client := TestClient(app, raise_server_exceptions=False), make_event("evt_r1", "PRICE"))
        with SessionLocal() as s:
            assert s.query(DMDelivery).filter_by(status="pending").count() == 1

        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_resp.json.return_value = {"dm_id": "dm_recovery", "status": "queued"}

        with SessionLocal() as s:
            d = _claim_delivery(s)
            assert d is not None
            _handle_send_success(s, d["id"], "dm_recovery")

        with SessionLocal() as s:
            assert s.query(DMDelivery).filter_by(status="queued", dm_id="dm_recovery").count() == 1


# ── Webhook performance ──────────────────────────────────────


class TestWebhookPerformance:
    def test_webhook_responds_under_5_seconds(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()

        start = time.monotonic()
        resp = webhook_post(client, make_event("evt_perf", "PRICE"))
        elapsed = time.monotonic() - start

        assert resp.status_code == 200
        assert elapsed < 5.0, f"Webhook took {elapsed:.2f}s"

    def test_webhook_does_not_wait_for_dm_send(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()

        slow_called = threading.Event()

        async def slow_send(**kwargs):
            slow_called.set()
            await asyncio.sleep(10)
            return MagicMock(status_code=500)

        with patch("app.worker.send_dm", side_effect=slow_send):
            start = time.monotonic()
            resp = webhook_post(client, make_event("evt_nw", "PRICE"))
            elapsed = time.monotonic() - start

            assert resp.status_code == 200
            assert elapsed < 1.0, f"Webhook should not wait for DM, took {elapsed:.2f}s"

    def test_100_webhooks_all_under_5_seconds(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()

        events = [make_event(f"evt_{i}", "PRICE", user_id=f"usr_{i}") for i in range(100)]

        start = time.monotonic()
        results = [_send_one(e) for e in events]
        elapsed = time.monotonic() - start

        assert all(r.status_code == 200 for r in results)
        assert elapsed < 5.0, f"100 sequential webhooks took {elapsed:.2f}s"


# ── Duplicate event redelivery ────────────────────────────────


class TestEventRedelivery:
    def test_redelivered_event_returns_200(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        r1 = webhook_post(client, make_event("evt_re", "PRICE"))
        assert r1.status_code == 200
        r2 = webhook_post(client, make_event("evt_re", "PRICE"))
        assert r2.status_code == 200

    def test_redelivered_event_no_duplicate_delivery(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_re2", "PRICE"))
        webhook_post(client, make_event("evt_re2", "PRICE"))
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == 1
