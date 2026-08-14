import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.database import engine, Base, SessionLocal
from app.models import Rule, DMDelivery, DuplicateCounter


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


# ── POST /rules ────────────────────────────────────────────────


class TestCreateRule:
    def test_create_rule_returns_201(self, client):
        resp = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Here's the price"})
        assert resp.status_code == 201

    def test_create_rule_response_shape(self, client):
        resp = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Here's the price"})
        data = resp.json()
        assert set(data.keys()) == {"rule_id", "keyword", "dm_message"}
        assert data["keyword"] == "PRICE"
        assert data["dm_message"] == "Here's the price"
        assert isinstance(data["rule_id"], str)
        assert len(data["rule_id"]) > 0

    def test_create_rule_persists_to_db(self, client, db):
        client.post("/rules", json={"keyword": "BUY", "dm_message": "Send money"})
        rule = db.query(Rule).first()
        assert rule is not None
        assert rule.keyword == "BUY"
        assert rule.dm_message == "Send money"

    def test_create_multiple_rules(self, client):
        r1 = client.post("/rules", json={"keyword": "PRICE", "dm_message": "msg1"})
        r2 = client.post("/rules", json={"keyword": "BUY", "dm_message": "msg2"})
        assert r1.status_code == 201
        assert r2.status_code == 201
        assert r1.json()["rule_id"] != r2.json()["rule_id"]

    def test_create_rule_missing_keyword(self, client):
        resp = client.post("/rules", json={"dm_message": "hello"})
        assert resp.status_code == 422

    def test_create_rule_missing_dm_message(self, client):
        resp = client.post("/rules", json={"keyword": "PRICE"})
        assert resp.status_code == 422

    def test_create_rule_empty_body(self, client):
        resp = client.post("/rules", json={})
        assert resp.status_code == 422

    def test_create_rule_keyword_preserves_case(self, client):
        resp = client.post("/rules", json={"keyword": "price", "dm_message": "hi"})
        data = resp.json()
        assert data["keyword"] == "price"

    def test_rule_id_is_unique(self, client):
        r1 = client.post("/rules", json={"keyword": "A", "dm_message": "a"})
        r2 = client.post("/rules", json={"keyword": "B", "dm_message": "b"})
        assert r1.json()["rule_id"] != r2.json()["rule_id"]


# ── GET /stats ─────────────────────────────────────────────────


class TestStats:
    def test_empty_db_returns_zeros(self, client):
        resp = client.get("/stats")
        assert resp.status_code == 200
        assert resp.json() == {"sent": 0, "failed": 0, "queued": 0, "duplicates_blocked": 0}

    def test_sent_counts_delivered(self, client, db):
        rule = Rule(id="r1", keyword="A", dm_message="a")
        db.add(rule)
        db.flush()
        for i in range(3):
            db.add(DMDelivery(rule_id="r1", user_id=f"u{i}", comment_id=f"c{i}", message="a", status="delivered"))
        db.commit()
        resp = client.get("/stats")
        assert resp.json()["sent"] == 3

    def test_failed_counts_failed(self, client, db):
        rule = Rule(id="r1", keyword="A", dm_message="a")
        db.add(rule)
        db.flush()
        db.add(DMDelivery(rule_id="r1", user_id="u1", comment_id="c1", message="a", status="failed"))
        db.commit()
        resp = client.get("/stats")
        assert resp.json()["failed"] == 1

    def test_queued_counts_pending_sending_queued_retrying(self, client, db):
        rule = Rule(id="r1", keyword="A", dm_message="a")
        db.add(rule)
        db.flush()
        statuses = ["pending", "sending", "queued", "retrying"]
        for i, s in enumerate(statuses):
            db.add(DMDelivery(rule_id="r1", user_id=f"u{i}", comment_id=f"c{i}", message="a", status=s))
        db.commit()
        resp = client.get("/stats")
        assert resp.json()["queued"] == 4

    def test_mixed_statuses(self, client, db):
        rule = Rule(id="r1", keyword="A", dm_message="a")
        db.add(rule)
        db.flush()
        db.add(DMDelivery(rule_id="r1", user_id="u1", comment_id="c1", message="a", status="delivered"))
        db.add(DMDelivery(rule_id="r1", user_id="u2", comment_id="c2", message="a", status="delivered"))
        db.add(DMDelivery(rule_id="r1", user_id="u3", comment_id="c3", message="a", status="failed"))
        db.add(DMDelivery(rule_id="r1", user_id="u4", comment_id="c4", message="a", status="pending"))
        db.add(DMDelivery(rule_id="r1", user_id="u5", comment_id="c5", message="a", status="queued"))
        db.commit()
        resp = client.get("/stats")
        data = resp.json()
        assert data["sent"] == 2
        assert data["failed"] == 1
        assert data["queued"] == 2
        assert data["duplicates_blocked"] == 0

    def test_duplicates_blocked_from_counter(self, client, db):
        db.add(DuplicateCounter(count=42))
        db.commit()
        resp = client.get("/stats")
        assert resp.json()["duplicates_blocked"] == 42

    def test_stats_shape(self, client):
        resp = client.get("/stats")
        assert set(resp.json().keys()) == {"sent", "failed", "queued", "duplicates_blocked"}
