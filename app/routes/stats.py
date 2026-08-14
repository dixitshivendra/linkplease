from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.database import get_db
from app.models import DMDelivery, DuplicateCounter
from app.schemas import StatsResponse

router = APIRouter()


@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    sent = db.query(func.count()).filter(DMDelivery.status == "delivered").scalar() or 0
    failed = db.query(func.count()).filter(DMDelivery.status == "failed").scalar() or 0
    queued = (
        db.query(func.count())
        .filter(DMDelivery.status.in_(["pending", "sending", "queued", "retrying"]))
        .scalar()
        or 0
    )
    dup_row = db.query(DuplicateCounter).first()
    duplicates_blocked = dup_row.count if dup_row else 0
    return StatsResponse(
        sent=sent,
        failed=failed,
        queued=queued,
        duplicates_blocked=duplicates_blocked,
    )
