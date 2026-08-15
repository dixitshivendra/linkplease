import asyncio
import random
import time
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from app.database import SessionLocal, engine, Base
from app.models import Rule, DMDelivery, ProcessedEvent, DuplicateCounter
from app.services.rate_limiter import RollingRateLimiter
from app.services.dm_sender import send_dm, check_dm_status

MAX_RETRIES_5XX = 5
MAX_RETRIES_429 = 3
TICK_INTERVAL = 0.5
RECONCILE_TICKS = 10
STALE_SENDING_THRESHOLD_SECONDS = 30


def _utcnow():
    return datetime.now(timezone.utc)


def _utcnow_iso():
    return _utcnow().isoformat()


def _backoff(attempt: int) -> float:
    base = min(2 ** attempt, 16)
    jitter = random.uniform(0.8, 1.2)
    return base * jitter


def _claim_delivery(db) -> dict | None:
    now = _utcnow_iso()
    row = db.execute(
        text(
            "UPDATE dm_deliveries "
            "SET status = 'sending', attempts = attempts + 1, updated_at = :now "
            "WHERE id = ("
            "  SELECT id FROM dm_deliveries "
            "  WHERE status IN ('pending', 'retrying') "
            "  AND (next_retry_at IS NULL OR next_retry_at <= :now) "
            "  ORDER BY created_at ASC "
            "  LIMIT 1"
            ") "
            "RETURNING id, rule_id, user_id, comment_id, message, attempts"
        ),
        {"now": now},
    )
    return row.mappings().first()


def _handle_send_success(db, delivery_id: str, dm_id: str):
    db.execute(
        text(
            "UPDATE dm_deliveries "
            "SET status = 'queued', dm_id = :dm_id, updated_at = :now "
            "WHERE id = :id"
        ),
        {"dm_id": dm_id, "now": _utcnow_iso(), "id": delivery_id},
    )
    db.commit()


def _handle_send_429(db, delivery_id: str, retry_after: float, attempts: int):
    if attempts >= MAX_RETRIES_429:
        db.execute(
            text("UPDATE dm_deliveries SET status = 'failed', updated_at = :now WHERE id = :id"),
            {"now": _utcnow_iso(), "id": delivery_id},
        )
    else:
        next_retry = _utcnow() + timedelta(seconds=retry_after)
        db.execute(
            text(
                "UPDATE dm_deliveries "
                "SET status = 'retrying', next_retry_at = :next, updated_at = :now "
                "WHERE id = :id"
            ),
            {"next": next_retry.isoformat(), "now": _utcnow_iso(), "id": delivery_id},
        )
    db.commit()


def _handle_send_5xx(db, delivery_id: str, attempts: int):
    if attempts >= MAX_RETRIES_5XX:
        db.execute(
            text("UPDATE dm_deliveries SET status = 'failed', updated_at = :now WHERE id = :id"),
            {"now": _utcnow_iso(), "id": delivery_id},
        )
    else:
        delay = _backoff(attempts)
        next_retry = _utcnow() + timedelta(seconds=delay)
        db.execute(
            text(
                "UPDATE dm_deliveries "
                "SET status = 'retrying', next_retry_at = :next, updated_at = :now "
                "WHERE id = :id"
            ),
            {"next": next_retry.isoformat(), "now": _utcnow_iso(), "id": delivery_id},
        )
    db.commit()


def _handle_send_400(db, delivery_id: str):
    db.execute(
        text("UPDATE dm_deliveries SET status = 'failed', updated_at = :now WHERE id = :id"),
        {"now": _utcnow_iso(), "id": delivery_id},
    )
    db.commit()


def _claim_queued_delivery(db) -> dict | None:
    now = _utcnow_iso()
    row = db.execute(
        text(
            "UPDATE dm_deliveries "
            "SET status = 'checking', updated_at = :now "
            "WHERE id = ("
            "  SELECT id FROM dm_deliveries "
            "  WHERE status = 'queued' AND dm_id IS NOT NULL "
            "  AND (next_reconcile_at IS NULL OR next_reconcile_at <= :now) "
            "  ORDER BY updated_at ASC "
            "  LIMIT 1"
            ") "
            "RETURNING id, rule_id, user_id, comment_id, dm_id"
        ),
        {"now": now},
    )
    return row.mappings().first()


def _handle_reconcile_delivered(db, delivery_id: str):
    db.execute(
        text("UPDATE dm_deliveries SET status = 'delivered', updated_at = :now WHERE id = :id"),
        {"now": _utcnow_iso(), "id": delivery_id},
    )
    db.commit()


def _handle_reconcile_failed(db, delivery_id: str):
    db.execute(
        text("UPDATE dm_deliveries SET status = 'failed', updated_at = :now WHERE id = :id"),
        {"now": _utcnow_iso(), "id": delivery_id},
    )
    db.commit()


def _handle_reconcile_retry(db, delivery_id: str, delay_seconds: float):
    next_retry = _utcnow() + timedelta(seconds=delay_seconds)
    db.execute(
        text(
            "UPDATE dm_deliveries "
            "SET status = 'queued', next_reconcile_at = :next, updated_at = :now "
            "WHERE id = :id"
        ),
        {"next": next_retry.isoformat(), "now": _utcnow_iso(), "id": delivery_id},
    )
    db.commit()


async def _reconcile_one(db) -> bool:
    delivery = _claim_queued_delivery(db)
    if delivery is None:
        return False

    delivery_id = delivery["id"]
    dm_id = delivery["dm_id"]

    try:
        resp = await check_dm_status(dm_id)
    except Exception:
        _handle_reconcile_retry(db, delivery_id, delay_seconds=5)
        return True

    if resp.status_code == 200:
        data = resp.json()
        status = data.get("status")
        if status == "delivered":
            _handle_reconcile_delivered(db, delivery_id)
        elif status == "failed":
            _handle_reconcile_failed(db, delivery_id)
        else:
            _handle_reconcile_retry(db, delivery_id, delay_seconds=2)
    elif resp.status_code == 429:
        retry_after = float(resp.headers.get("Retry-After", "5"))
        _handle_reconcile_retry(db, delivery_id, retry_after)
    elif resp.status_code == 500:
        _handle_reconcile_retry(db, delivery_id, delay_seconds=5)
    else:
        _handle_reconcile_retry(db, delivery_id, delay_seconds=5)

    return True


def _recover_stale_sending(db):
    cutoff = (_utcnow() - timedelta(seconds=STALE_SENDING_THRESHOLD_SECONDS)).isoformat()
    result = db.execute(
        text(
            "UPDATE dm_deliveries "
            "SET status = 'pending', next_retry_at = NULL, updated_at = :now "
            "WHERE status = 'sending' AND updated_at < :cutoff"
        ),
        {"now": _utcnow_iso(), "cutoff": cutoff},
    )
    recovered = result.rowcount
    db.commit()
    return recovered


async def worker_loop():
    Base.metadata.create_all(bind=engine)
    limiter = RollingRateLimiter(max_requests=10, window_seconds=60)
    tick_count = 0

    startup_db = SessionLocal()
    try:
        recovered = _recover_stale_sending(startup_db)
        print(f"[worker] started, recovered {recovered} stale sending deliveries", flush=True)
    finally:
        startup_db.close()

    while True:
        tick_count += 1
        db = SessionLocal()
        try:
            if tick_count % 20 == 0:
                recovered = _recover_stale_sending(db)
                if recovered:
                    print(f"[worker] tick {tick_count}: recovered {recovered} stale sending deliveries", flush=True)

            stats = db.execute(text(
                "SELECT "
                "  COUNT(*) FILTER (WHERE status='pending') AS pending, "
                "  COUNT(*) FILTER (WHERE status='sending') AS sending, "
                "  COUNT(*) FILTER (WHERE status='retrying') AS retrying, "
                "  COUNT(*) FILTER (WHERE status='queued') AS queued "
                "FROM dm_deliveries"
            )).mappings().first()
            print(f"[worker] tick {tick_count}: {dict(stats)}", flush=True)

            delivery = _claim_delivery(db)
            if delivery is not None:
                db.commit()
                delivery_id = delivery["id"]
                idempotency_key = f"{delivery['rule_id']}:{delivery['user_id']}:{delivery['comment_id']}"

                await limiter.acquire()

                try:
                    resp = await send_dm(
                        recipient_user_id=delivery["user_id"],
                        message=delivery["message"],
                        comment_id=delivery["comment_id"],
                        idempotency_key=idempotency_key,
                    )
                except Exception as e:
                    print(
                        f"[worker] send exception "
                        f"delivery={delivery_id} "
                        f"attempt={delivery['attempts']} "
                        f"error={type(e).__name__}: {e}",
                        flush=True,
                    )
                    _handle_send_5xx(db, delivery_id, delivery["attempts"])
                    db.close()
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                print(
                    f"[worker] DM response "
                    f"delivery={delivery_id} "
                    f"status={resp.status_code} "
                    f"body={resp.text[:500]}",
                    flush=True,
                )

                if resp.status_code in (200, 202):
                    data = resp.json()
                    _handle_send_success(db, delivery_id, data["dm_id"])
                elif resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", "5"))
                    _handle_send_429(db, delivery_id, retry_after, delivery["attempts"])
                elif resp.status_code == 500:
                    _handle_send_5xx(db, delivery_id, delivery["attempts"])
                elif resp.status_code == 400:
                    _handle_send_400(db, delivery_id)
                else:
                    _handle_send_5xx(db, delivery_id, delivery["attempts"])

            db.close()
        except Exception as e:
            print(f"[worker] tick {tick_count} error: {type(e).__name__}: {e}", flush=True)
            db.close()

        if tick_count % RECONCILE_TICKS == 0:
            db = SessionLocal()
            try:
                while await _reconcile_one(db):
                    pass
            except Exception as e:
                print(f"[worker] reconcile error: {e}", flush=True)
            finally:
                db.close()

        await asyncio.sleep(TICK_INTERVAL)


def main():
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()
