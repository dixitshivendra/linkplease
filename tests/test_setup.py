import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.database import engine, Base, SessionLocal
from app.models import Rule, ProcessedEvent, DMDelivery, DuplicateCounter


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    return TestClient(app)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_rule(client):
    response = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Here's the price"})
    assert response.status_code == 201
    data = response.json()
    assert data["keyword"] == "PRICE"
    assert data["dm_message"] == "Here's the price"
    assert "rule_id" in data


def test_rules_table_exists():
    from sqlalchemy import inspect
    inspector = inspect(engine)
    assert "rules" in inspector.get_table_names()


def test_deliveries_table_exists():
    from sqlalchemy import inspect
    inspector = inspect(engine)
    assert "dm_deliveries" in inspector.get_table_names()


def test_events_table_exists():
    from sqlalchemy import inspect
    inspector = inspect(engine)
    assert "processed_events" in inspector.get_table_names()
