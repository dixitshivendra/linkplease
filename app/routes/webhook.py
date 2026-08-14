import hashlib
import hmac as hmac_mod
from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.config import settings
from app.database import get_db
from app.models import Rule

router = APIRouter()


def _verify_signature(raw_body: bytes, signature_header: str | None) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected_hex = signature_header[7:]
    computed = hmac_mod.new(settings.WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac_mod.compare_digest(expected_hex, computed)


@router.post("/webhook")
async def webhook(request: Request, db: Session = Depends(get_db)):
    raw_body = await request.body()

    signature = request.headers.get("X-PseudoGram-Signature")
    if not _verify_signature(raw_body, signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    payload = await request.json()

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {})

    if not event_id or not event_type:
        return {"status": "ok"}

    # Atomic insert — INSERT OR IGNORE with UNIQUE constraint on event_id
    result = db.execute(
        text(
            "INSERT OR IGNORE INTO processed_events (event_id, event_type, processed_at) "
            "VALUES (:eid, :etype, :at)"
        ),
        {"eid": event_id, "etype": event_type, "at": _utcnow_iso()},
    )
    if result.rowcount == 0:
        # Duplicate event_id — already processed
        return {"status": "ok"}

    if event_type == "comment.created":
        _process_comment_created(data, db)
    elif event_type == "comment.deleted":
        _process_comment_deleted(data, db)

    db.commit()
    return {"status": "ok"}


def _process_comment_deleted(data: dict, db: Session):
    comment_id = data.get("comment_id")
    if not comment_id:
        return
    now = _utcnow_iso()
    db.execute(
        text(
            "UPDATE dm_deliveries "
            "SET status = 'cancelled', updated_at = :now "
            "WHERE comment_id = :comment_id "
            "AND status IN ('pending', 'sending', 'queued', 'retrying')"
        ),
        {"comment_id": comment_id, "now": now},
    )


def _process_comment_created(data: dict, db: Session):
    comment_text = data.get("text", "")
    user_id = data.get("from", {}).get("user_id")
    comment_id = data.get("comment_id")

    if not user_id or not comment_id or not comment_text:
        return

    rules = db.query(Rule).all()
    matching_rules = [r for r in rules if r.keyword.lower() in comment_text.lower()]

    for rule in matching_rules:
        # Atomic insert — INSERT OR IGNORE with UNIQUE(rule_id, user_id)
        result = db.execute(
            text(
                "INSERT OR IGNORE INTO dm_deliveries "
                "(id, rule_id, user_id, comment_id, message, status, attempts, created_at, updated_at) "
                "VALUES (:id, :rule_id, :user_id, :comment_id, :message, 'pending', 0, :at, :at)"
            ),
            {
                "id": _uuid(),
                "rule_id": rule.id,
                "user_id": user_id,
                "comment_id": comment_id,
                "message": rule.dm_message,
                "at": _utcnow_iso(),
            },
        )
        if result.rowcount == 0:
            _increment_duplicates(db)


def _increment_duplicates(db: Session):
    db.execute(
        text(
            "INSERT INTO duplicate_counters (id, count, updated_at) VALUES ('global', 1, :at) "
            "ON CONFLICT(id) DO UPDATE SET count = count + 1, updated_at = :at"
        ),
        {"at": _utcnow_iso()},
    )


def _utcnow_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _uuid():
    import uuid
    return str(uuid.uuid4())
