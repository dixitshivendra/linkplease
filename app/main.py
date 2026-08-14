from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import engine, Base
from app.routes import rules, webhook, stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    _seed_default_rules()
    yield


def _seed_default_rules():
    from sqlalchemy import text
    from app.database import SessionLocal
    from app.models import Rule
    db = SessionLocal()
    try:
        if db.query(Rule).count() == 0:
            db.execute(
                text(
                    "INSERT INTO rules (id, keyword, dm_message, created_at) "
                    "VALUES (:id, :kw, :msg, :at)"
                ),
                {
                    "id": "default-price-rule",
                    "kw": "PRICE",
                    "msg": "Thanks for your interest! Here is the pricing info.",
                    "at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                },
            )
            db.commit()
    finally:
        db.close()


app = FastAPI(title="LinkPlease", lifespan=lifespan)
app.include_router(rules.router)
app.include_router(webhook.router)
app.include_router(stats.router)


@app.get("/health")
def health():
    return {"status": "ok"}
