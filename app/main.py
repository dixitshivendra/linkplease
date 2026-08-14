from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import engine, Base
from app.routes import rules, webhook, stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="LinkPlease", lifespan=lifespan)
app.include_router(rules.router)
app.include_router(webhook.router)
app.include_router(stats.router)


@app.get("/health")
def health():
    return {"status": "ok"}
