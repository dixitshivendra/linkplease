# LinkPlease Tech Intern Assignment - Implementation Plan (Final)

## Stack Selection
- **Language**: Python 3.11+
- **Framework**: FastAPI (async, good for handling webhook concurrency)
- **Database**: SQLite (simple, file-based, sufficient for this scope) with SQLAlchemy ORM
- **HTTP Client**: httpx (async, connection pooling)
- **Deployment**: Render/Railway/Fly.io (free tier)
- **Process Model**: Two processes — API server + Worker (both reading from same SQLite DB)

---

## Architecture: Database as Durable Queue

```
PseudoGram → /webhook (FastAPI) → SQLite (atomic write) → return 200
                                              ↑
                                              │ polling
                                              ��
                                    Worker Process
                                              │
                                    Rolling Rate Limiter (10/60s)
                                              │
                                    PseudoGram /v1/dm/send
                                              │
                                    Reconciliation (poll /v1/dm/{dm_id})
```

**Key invariant**: Every accepted webhook is persisted before returning 200. The database IS the queue.

---

## Part A - Core Requirements (ONLY FOCUS FOR NOW)

### Data Models

```python
# Rule: User-defined keyword -> DM mapping
Rule:
  - id (PK, string)
  - keyword (lowercase, indexed)
  - dm_message
  - created_at

# Processed Events (idempotency) - DB enforces uniqueness
ProcessedEvent:
  - id (PK)
  - event_id (UNIQUE, indexed)
  - event_type
  - processed_at

# DM Deliveries - DB enforces deduplication AND tracks full lifecycle
DMDelivery:
  - id (PK, string) - our internal ID (e.g., uuid)
  - rule_id (FK)
  - user_id
  - comment_id
  - message
  - status: pending | sending | queued | delivered | failed | retrying
  - dm_id (from API, nullable)
  - attempts
  - next_retry_at
  - created_at
  - updated_at
  
  # CRITICAL: UNIQUE(rule_id, user_id) enforces "same user never DM'd twice for same rule"
  # This handles concurrent duplicate events atomically
```

### API Endpoints

#### POST /rules
- Validate keyword, dm_message
- Create Rule in DB (id = uuid4 string)
- Return 201 with rule_id, keyword, dm_message

#### POST /webhook
- **Must return 200 within 5 seconds**
- Verify HMAC signature (implement early, even though it's Part B)
- Atomic transaction:
  1. `INSERT INTO processed_events(event_id, ...) ON CONFLICT DO NOTHING`
  2. If 0 rows inserted → duplicate event, return 200
  3. If event_type = "comment.created": parse, find matching rules
  4. For each matching rule:
     - `INSERT INTO dm_deliveries(...) ON CONFLICT(rule_id, user_id) DO NOTHING`
     - If inserted → delivery created, status=pending
     - If conflict → duplicates_blocked++
  5. Commit transaction
- Return 200 immediately

#### GET /stats
- **Derived from durable state, not manual counters**
- Single query with CASE counts:
  - sent = COUNT(status = 'delivered')
  - failed = COUNT(status = 'failed')
  - queued = COUNT(status IN ('pending', 'sending', 'queued', 'retrying'))
  - duplicates_blocked = (separate counter table or derived from conflict tracking)

---

### Worker Process (Separate from API)

**Single loop, no APScheduler needed** — `next_retry_at` is already persisted:

```python
async def worker_loop():
    while True:
        # 1. Process due retries (next_retry_at <= now)
        await process_due_retries()
        
        # 2. Reconcile queued DMs (poll /v1/dm/{dm_id})
        await reconcile_queued_dms()
        
        # 3. Send new pending DMs (rate limited)
        await send_pending_dms()
        
        await asyncio.sleep(0.5)
```

#### Rolling Rate Limiter (10 requests per rolling 60 seconds)

```python
class RollingRateLimiter:
    def __init__(self, max_requests=10, window_seconds=60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times = []  # timestamps of successful requests
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            # Remove timestamps older than window
            cutoff = now - self.window_seconds
            self.request_times = [t for t in self.request_times if t > cutoff]
            
            if len(self.request_times) < self.max_requests:
                self.request_times.append(now)
                return
            
            # Wait until oldest request exits window
            wait_time = self.request_times[0] + self.window_seconds - now
        
        await asyncio.sleep(max(0, wait_time) + 0.01)  # small buffer
        await self.acquire()  # retry
```

This guarantees **never exceeding 10 requests in any 60-second window**.

---

### DM Sending Logic

```python
async def send_dm(delivery, idempotency_key):
    headers = {
        "X-API-Key": API_KEY,
        "Idempotency-Key": idempotency_key,
    }
    payload = {
        "recipient_user_id": delivery.user_id,
        "message": delivery.message,
        "comment_id": delivery.comment_id,
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/v1/dm/send",
            json=payload,
            headers=headers,
            timeout=10.0
        )
    return response
```

---

### Retry Strategy (Persisted in `next_retry_at`)

| Attempt | Delay (5xx) | 429 Handling |
|---------|-------------|--------------|
| 1 | 1s + jitter | Retry-After header |
| 2 | 2s + jitter | Retry-After header |
| 3 | 4s + jitter | Retry-After header |
| 4 | 8s + jitter | Retry-After header |
| 5 | 16s + jitter | Retry-After header |
| 6+ | Mark failed | Mark failed |

- Max retries: 5 for 5xx, 3 for 429 (then fail)
- Jitter: ±20%
- On retry: update `next_retry_at`, `attempts += 1`, status = 'retrying'

---

### Reconciliation (Status Polling)

```python
async def reconcile_queued_dms():
    deliveries = await get_deliveries_with_dm_id(statuses=['queued', 'sending'])
    
    for delivery in deliveries:
        status = await check_dm_status(delivery.dm_id)
        if status == 'delivered':
            await update_status(delivery.id, 'delivered')
        elif status == 'failed':
            if delivery.attempts < MAX_RETRIES:
                await schedule_retry(delivery.id, exponential_backoff(delivery.attempts))
            else:
                await update_status(delivery.id, 'failed')
```

Run every loop iteration (every 0.5s) — no separate sleep needed.

---

## Part B - Security (Implement After Part A Works)

### Webhook Signature Verification
- Extract `X-PseudoGram-Signature: sha256=<hex>`
- Compute `hmac_sha256(api_key, raw_body)`
- Compare with `hmac.compare_digest()`
- Reject 401 if invalid
- **Do this in webhook handler BEFORE any DB work**

---

## Part C - Advanced (Only If Part A + B Complete)

### Comment Deleted Handling
- On `comment.deleted` event:
  - Find deliveries with matching `comment_id` AND status IN ('pending', 'sending')
  - Update status = 'cancelled' (not counted in queued)
  - If status = 'queued' (already sent): log, cannot recall

### 500 Events in 10 Seconds
- Worker processes queue at ≤10 req/60s
- `queued` will correctly show ~490 during burst
- This is **correct behavior**, not failure
- No events lost — all persisted in DB

---

## Failure Modes (FAILURES.md) - Honest Assessment

1. **Worker crash before persisting retry**: If worker dies after sending DM but before updating `next_retry_at`/`dm_id`, the delivery stays in 'sending' state. On restart, reconciliation will detect it via `/v1/dm/{dm_id}` poll. **Mitigation: Run reconciliation on startup.**

2. **Race condition on duplicate event_id**: Two identical events arrive concurrently. Both attempt `INSERT ... ON CONFLICT`. One succeeds, one gets 0 rows. The loser correctly treats as duplicate. **Handled by DB unique constraint.**

3. **Rate limit burst backlog**: 500 events in 10s → queue builds to ~490. Worker processes at 10/60s. If worker crashes, pending deliveries remain in DB with status=pending. On restart, they're picked up. **No loss, but latency.**

4. **DM status poll gap**: Polling every loop (0.5s) means near-instant detection. If process crashes before poll, deliveries stuck as 'queued' until next poll on restart. **Mitigation: Run reconciliation on startup.**

5. **Network partition during DM send**: Request sent, connection drops before response. Idempotency-Key ensures retry is safe. But if we never get response, we rely on reconciliation to detect final state. **Window of uncertainty exists.**

6. **SQLite write contention**: Under high concurrent webhook load, SQLite may return `SQLITE_BUSY`. **Mitigation: WAL mode, reasonable timeout, retry logic.**

---

## Implementation Order (PART A ONLY)

1. **Setup**: FastAPI project, SQLAlchemy models, config, SQLite WAL mode
2. **POST /rules** + **GET /stats** (derived from DB)
3. **Webhook handler**: signature verification, atomic event processing, delivery creation with ON CONFLICT
4. **Worker process**: rolling rate limiter, DM sender, retry scheduling (persisted), reconciliation
5. **Load testing** with `/v1/simulate/start`
6. **Fix bugs, deploy, verify /stats matches truth**
7. **FAILURES.md** + Loom recording

**Then and only then**: Part B (if not already done), Part C

---

## Deployment

- **API Process**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Worker Process**: `python worker.py` (same codebase, different entrypoint)
- **Shared**: SQLite DB file (mounted volume on Render)
- **Environment**: `API_KEY`, `DATABASE_URL`, `BASE_URL`

---

## Testing Strategy

1. Unit tests: keyword matching, rolling rate limiter, retry logic
2. Integration: webhook → DB → worker → mock API
3. Load test: 500 events in 10s via simulate/start
4. Verify `/stats` matches `/v1/simulate/{run_id}/truth`
5. Chaos: Kill worker mid-run, restart, verify no DM loss
6. Chaos: Kill API mid-webhook, verify event not lost

---

## Loom Talking Points

1. **Tradeoff**: SQLite + two-process model instead of Redis/Celery. Gave up horizontal scaling and high write throughput for simplicity, durability, and zero external dependencies. For production: PostgreSQL + Redis + proper queue.

2. **With one more week**: Add structured logging, metrics (Prometheus), graceful shutdown with in-flight request draining, integration tests with testcontainers, and a simple admin UI for rule management.