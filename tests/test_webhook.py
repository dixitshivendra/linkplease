import hashlib
import hmac as hmac_mod
from json import dumps as json_dumps
import threading
import time
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import engine, Base, SessionLocal
from app.models import Rule, ProcessedEvent, DMDelivery, DuplicateCounter
from tests.conftest import TEST_API_KEY, TEST_WEBHOOK_SECRET, sign_body, webhook_post


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def make_event(event_id, text, user_id="usr_123", comment_id="cmt_001"):
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_44de1b",
            "text": text,
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": user_id,
                "username": "testuser",
            },
        },
    }


def make_delete_event(event_id, comment_id="cmt_001"):
    return {
        "event_id": event_id,
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
        },
    }


# ── POST /webhook basics ──────────────────────────────────────


class TestWebhookBasics:
    def test_returns_200(self, client):
        resp = webhook_post(client, make_event("evt_001", "PRICE please"))
        assert resp.status_code == 200

    def test_response_shape(self, client):
        resp = webhook_post(client, make_event("evt_001", "PRICE please"))
        assert resp.json() == {"status": "ok"}

    def test_returns_200_within_timeout(self, client):
        resp = webhook_post(client, make_event("evt_001", "PRICE please"))
        assert resp.status_code == 200


# ── HMAC signature verification ───────────────────────────────


class TestHMACVerification:
    def test_valid_signature_accepted(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        resp = webhook_post(client, make_event("evt_h1", "PRICE"))
        assert resp.status_code == 200
        assert db.query(DMDelivery).count() == 1

    def test_invalid_signature_rejected(self, client):
        body = json_dumps(make_event("evt_h2", "PRICE")).encode()
        bad_sig = hmac_mod.new(b"wrong-key", body, hashlib.sha256).hexdigest()
        resp = client.post("/webhook", content=body, headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": f"sha256={bad_sig}",
        })
        assert resp.status_code == 401

    def test_missing_signature_rejected(self, client):
        body = json_dumps(make_event("evt_h3", "PRICE")).encode()
        resp = client.post("/webhook", content=body, headers={
            "Content-Type": "application/json",
        })
        assert resp.status_code == 401

    def test_malformed_signature_no_prefix_rejected(self, client):
        body = json_dumps(make_event("evt_h4", "PRICE")).encode()
        sig = hmac_mod.new(TEST_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
        resp = client.post("/webhook", content=body, headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": sig,  # missing sha256= prefix
        })
        assert resp.status_code == 401

    def test_malformed_signature_garbage_rejected(self, client):
        body = json_dumps(make_event("evt_h5", "PRICE")).encode()
        resp = client.post("/webhook", content=body, headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": "sha256=not-a-hex-string!!!",
        })
        assert resp.status_code == 401

    def test_modified_body_rejected(self, client):
        original = make_event("evt_h6", "PRICE")
        body = json_dumps(original).encode()
        valid_sig = hmac_mod.new(TEST_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
        tampered = json_dumps({**original, "event_id": "evt_h6_tampered"}).encode()
        resp = client.post("/webhook", content=tampered, headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": f"sha256={valid_sig}",
        })
        assert resp.status_code == 401

    def test_empty_signature_rejected(self, client):
        body = json_dumps(make_event("evt_h7", "PRICE")).encode()
        resp = client.post("/webhook", content=body, headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": "",
        })
        assert resp.status_code == 401


# ── comment.created matching ──────────────────────────────────


class TestCommentMatching:
    def test_basic_keyword_match(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="Here's the price"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE please"))
        deliveries = db.query(DMDelivery).all()
        assert len(deliveries) == 1
        assert deliveries[0].user_id == "usr_123"
        assert deliveries[0].comment_id == "cmt_001"
        assert deliveries[0].message == "Here's the price"
        assert deliveries[0].status == "pending"

    def test_no_matching_keyword(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="Here's the price"))
        db.commit()
        webhook_post(client, make_event("evt_001", "Hello there"))
        assert db.query(DMDelivery).count() == 0

    def test_case_insensitive_matching(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "what is the price?"))
        assert db.query(DMDelivery).count() == 1

    def test_keyword_in_middle_of_text(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "Hey, what's the PRICE of this?"))
        assert db.query(DMDelivery).count() == 1

    def test_keyword_at_end_of_text(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "I need the PRICE"))
        assert db.query(DMDelivery).count() == 1

    def test_partial_keyword_does_not_match(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICING please"))
        assert db.query(DMDelivery).count() == 0

    def test_multiple_matching_rules(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg1"))
        db.add(Rule(id="r2", keyword="BUY", dm_message="msg2"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE and BUY please"))
        deliveries = db.query(DMDelivery).all()
        assert len(deliveries) == 2
        messages = {d.message for d in deliveries}
        assert messages == {"msg1", "msg2"}


# ── Event idempotency ─────────────────────────────────────────


class TestEventIdempotency:
    def test_same_event_id_not_processed_twice(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_dup", "PRICE please"))
        webhook_post(client, make_event("evt_dup", "PRICE please"))
        assert db.query(DMDelivery).count() == 1

    def test_same_event_id_returns_200_both_times(self, client):
        resp1 = webhook_post(client, make_event("evt_dup", "PRICE please"))
        resp2 = webhook_post(client, make_event("evt_dup", "PRICE please"))
        assert resp1.status_code == 200
        assert resp2.status_code == 200

    def test_different_event_ids_are_independent(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE"))
        webhook_post(client, make_event("evt_002", "PRICE"))
        assert db.query(DMDelivery).count() == 1


# ── DM deduplication (rule/user) ──────────────────────────────


class TestDMDeduplication:
    def test_same_user_same_rule_not_dmed_twice(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE", user_id="usr_a"))
        webhook_post(client, make_event("evt_002", "PRICE", user_id="usr_a"))
        assert db.query(DMDelivery).count() == 1

    def test_different_users_same_rule(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE", user_id="usr_a"))
        webhook_post(client, make_event("evt_002", "PRICE", user_id="usr_b"))
        assert db.query(DMDelivery).count() == 2

    def test_same_user_different_rules(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg1"))
        db.add(Rule(id="r2", keyword="BUY", dm_message="msg2"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE and BUY"))
        webhook_post(client, make_event("evt_002", "PRICE and BUY"))
        assert db.query(DMDelivery).count() == 2

    def test_duplicates_blocked_counter_increments(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", "PRICE", user_id="usr_a"))
        webhook_post(client, make_event("evt_002", "PRICE", user_id="usr_a"))
        counter = db.query(DuplicateCounter).first()
        assert counter is not None
        assert counter.count == 1

    def test_multiple_duplicates_counted_correctly(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        for i in range(5):
            webhook_post(client, make_event(f"evt_{i:03d}", "PRICE", user_id="usr_a"))
        counter = db.query(DuplicateCounter).first()
        assert counter.count == 4


# ── comment.deleted ───────────────────────────────────────────


class TestCommentDeleted:
    def test_comment_deleted_does_not_create_delivery(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_delete_event("evt_del_001", "cmt_001"))
        assert db.query(DMDelivery).count() == 0

    def test_comment_deleted_returns_200(self, client):
        resp = webhook_post(client, make_delete_event("evt_del_001"))
        assert resp.status_code == 200

    def test_comment_deleted_is_persisted(self, client, db):
        webhook_post(client, make_delete_event("evt_del_001"))
        event = db.query(ProcessedEvent).filter_by(event_id="evt_del_001").first()
        assert event is not None
        assert event.event_type == "comment.deleted"

    def test_cancelled_pending_delivery(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_c001", "PRICE", user_id="usr_a", comment_id="cmt_del"))
        assert db.query(DMDelivery).count() == 1
        delivery = db.query(DMDelivery).first()
        assert delivery.status == "pending"
        webhook_post(client, make_delete_event("evt_del_002", "cmt_del"))
        db.expire_all()
        delivery = db.query(DMDelivery).first()
        assert delivery.status == "cancelled"

    def test_cancelled_retrying_delivery(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_c002", "PRICE", user_id="usr_b", comment_id="cmt_del2"))
        delivery = db.query(DMDelivery).first()
        db.execute(
            text("UPDATE dm_deliveries SET status = 'retrying' WHERE id = :id"),
            {"id": delivery.id},
        )
        db.commit()
        webhook_post(client, make_delete_event("evt_del_003", "cmt_del2"))
        db.expire_all()
        delivery = db.query(DMDelivery).first()
        assert delivery.status == "cancelled"

    def test_already_sent_delivery_not_cancelled(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_c003", "PRICE", user_id="usr_c", comment_id="cmt_sent"))
        delivery = db.query(DMDelivery).first()
        db.execute(
            text("UPDATE dm_deliveries SET status = 'delivered' WHERE id = :id"),
            {"id": delivery.id},
        )
        db.commit()
        webhook_post(client, make_delete_event("evt_del_004", "cmt_sent"))
        db.expire_all()
        delivery = db.query(DMDelivery).first()
        assert delivery.status == "delivered"

    def test_already_failed_delivery_not_cancelled(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_c004", "PRICE", user_id="usr_d", comment_id="cmt_fail"))
        delivery = db.query(DMDelivery).first()
        db.execute(
            text("UPDATE dm_deliveries SET status = 'failed' WHERE id = :id"),
            {"id": delivery.id},
        )
        db.commit()
        webhook_post(client, make_delete_event("evt_del_005", "cmt_fail"))
        db.expire_all()
        delivery = db.query(DMDelivery).first()
        assert delivery.status == "failed"

    def test_cancelled_not_in_queued_stats(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_c005", "PRICE", user_id="usr_e", comment_id="cmt_q"))
        resp = client.get("/stats")
        assert resp.json()["queued"] == 1
        webhook_post(client, make_delete_event("evt_del_006", "cmt_q"))
        resp = client.get("/stats")
        assert resp.json()["queued"] == 0


# ── Edge cases ────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_comment_text(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        webhook_post(client, make_event("evt_001", ""))
        assert db.query(DMDelivery).count() == 0

    def test_missing_from_field(self, client):
        payload = {
            "event_id": "evt_no_from",
            "event_type": "comment.created",
            "sent_at": "2026-08-10T09:14:22.481Z",
            "data": {
                "comment_id": "cmt_001",
                "post_id": "post_44de1b",
                "text": "PRICE please",
                "created_at": "2026-08-10T09:14:21.900Z",
            },
        }
        resp = webhook_post(client, payload)
        assert resp.status_code == 200

    def test_unknown_event_type_persisted_not_crashed(self, client, db):
        payload = {
            "event_id": "evt_unknown",
            "event_type": "post.created",
            "sent_at": "2026-08-10T09:14:22.481Z",
            "data": {},
        }
        resp = webhook_post(client, payload)
        assert resp.status_code == 200
        event = db.query(ProcessedEvent).filter_by(event_id="evt_unknown").first()
        assert event is not None

    def test_no_matching_rules(self, client, db):
        webhook_post(client, make_event("evt_001", "PRICE please"))
        assert db.query(DMDelivery).count() == 0

    def test_event_id_persisted(self, client, db):
        webhook_post(client, make_event("evt_persist", "PRICE"))
        event = db.query(ProcessedEvent).filter_by(event_id="evt_persist").first()
        assert event is not None
        assert event.event_type == "comment.created"

    def test_user_id_used_not_username(self, client, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        payload = make_event("evt_uid", "PRICE", user_id="usr_real")
        payload["data"]["from"]["username"] = "fake_username"
        webhook_post(client, payload)
        delivery = db.query(DMDelivery).first()
        assert delivery.user_id == "usr_real"


# ── Concurrency / race safety ─────────────────────────────────


def _send_webhook(event):
    c = TestClient(app, raise_server_exceptions=False)
    body = json_dumps(event).encode()
    headers = sign_body(body)
    headers["Content-Type"] = "application/json"
    return c.post("/webhook", content=body, headers=headers)


class TestConcurrency:
    def test_concurrent_same_event_id(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        n = 20
        events = [make_event("evt_race", "PRICE") for _ in range(n)]
        with ThreadPoolExecutor(max_workers=n) as pool:
            results = list(pool.map(_send_webhook, events))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(ProcessedEvent).filter_by(event_id="evt_race").count() == 1
            assert verify.query(DMDelivery).count() == 1

    def test_concurrent_same_user_same_rule(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        n = 20
        events = [make_event(f"evt_{i}", "PRICE", user_id="usr_x") for i in range(n)]
        with ThreadPoolExecutor(max_workers=n) as pool:
            results = list(pool.map(_send_webhook, events))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == 1
            assert verify.query(DuplicateCounter).first().count == 19

    def test_concurrent_different_users(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        n = 20
        events = [make_event(f"evt_{i}", "PRICE", user_id=f"usr_{i}") for i in range(n)]
        with ThreadPoolExecutor(max_workers=n) as pool:
            results = list(pool.map(_send_webhook, events))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == n

    def test_concurrent_multiple_matching_rules(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg1"))
        db.add(Rule(id="r2", keyword="BUY", dm_message="msg2"))
        db.add(Rule(id="r3", keyword="YES", dm_message="msg3"))
        db.commit()
        n = 20
        events = [make_event(f"evt_{i}", "PRICE and BUY and YES") for i in range(n)]
        with ThreadPoolExecutor(max_workers=n) as pool:
            results = list(pool.map(_send_webhook, events))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == 3

    def test_concurrent_interleaved_events(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        n = 10
        events_a = [make_event(f"evt_a{i}", "PRICE", user_id="usr_a") for i in range(n)]
        events_b = [make_event(f"evt_b{i}", "PRICE", user_id="usr_b") for i in range(n)]
        with ThreadPoolExecutor(max_workers=n * 2) as pool:
            results = list(pool.map(_send_webhook, events_a + events_b))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == 2

    def test_concurrent_correct_counter_accuracy(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        events = []
        for i in range(50):
            events.append(make_event(f"evt_a{i}", "PRICE", user_id="usr_a"))
            events.append(make_event(f"evt_b{i}", "PRICE", user_id="usr_b"))
        with ThreadPoolExecutor(max_workers=100) as pool:
            results = list(pool.map(_send_webhook, events))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == 2
            assert verify.query(DuplicateCounter).first().count == 98

    def test_concurrent_500_events_single_user(self, db):
        db.add(Rule(id="r1", keyword="PRICE", dm_message="msg"))
        db.commit()
        n = 500
        events = [make_event(f"evt_{i}", "PRICE", user_id="usr_a") for i in range(n)]
        with ThreadPoolExecutor(max_workers=n) as pool:
            results = list(pool.map(_send_webhook, events))
        assert all(r.status_code == 200 for r in results)
        with SessionLocal() as verify:
            assert verify.query(DMDelivery).count() == 1
            assert verify.query(DuplicateCounter).first().count == 499
