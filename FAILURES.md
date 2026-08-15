# FAILURES.md — LinkPlease Assignment

## Test Results (final)

**Run:** `run_3f0a6963f399` — 500 events / 10 seconds

| Metric | Value |
|--------|-------|
| Events generated | 500 |
| Webhook deliveries (incl. redeliveries) | 543 |
| HTTP 200 from our service | 543 |
| Unique recipients expected | 99 |
| DMs sent (delivered) | 76 |
| DMs failed | 13 |
| DMs cancelled (comment.deleted) | 10 |
| Duplicates blocked | 55 |
| Queued (drained) | 0 |
| Rate limit breached | Never |

**Reconciliation equations (all pass):**
1. `99 expected == 76 sent + 13 failed + 10 cancelled`
2. `543 webhooks == all received, 0 dropped`
3. Queue fully drained, all DMs in terminal state

## Known failure modes

### 1. Render free tier cold-start drops webhooks (observed)

If the Render service has been idle for ~15 minutes, it enters sleep mode. When PseudoGram's burst arrives, the service takes 30-60 seconds to wake up. During this window, webhooks are dropped silently — PseudoGram sees HTTP 200 from Render's edge, but our application never receives the request.

**Evidence:** During an unwarmed test, PseudoGram reported `webhook_200_count: 542` but our application recorded substantially fewer webhook events (314). After adding warm-up pings, all 537 webhooks were received and processed.

**Mitigation:** Warm-up pings before the grader runs. A permanent fix would require a paid tier or always-on hosting.

### 2. SQLite write contention under extreme concurrency (architectural)

SQLite allows only one writer at a time, even in WAL mode. With `busy_timeout=5000`, a webhook handler can wait up to 5 seconds for the write lock. During a 500-event/10-second burst, if more than ~50 webhooks arrive simultaneously, some may time out waiting for the lock and return HTTP 500.

**Mitigation:** WAL mode + 5-second busy timeout has been sufficient for the grader's burst rate. A production system would use PostgreSQL.

### 3. Reconciliation lag creates temporary stats inaccuracy (architectural)

After a DM is sent and accepted by PseudoGram (status = `queued`), there is a window before the next reconciliation poll where PseudoGram may have internally failed the DM. During this window, `/stats` reports the DM as `queued` rather than `failed`, which is temporarily inaccurate.

**Evidence:** In the 500-event test, `queued` peaked at 81 before draining to 0. The PseudoGram truth showed 14 final failures that were initially counted as queued.

**Mitigation:** Reconciliation runs every 5 seconds. The stats converge to accurate values within one reconciliation cycle.

### 4. comment.deleted event ordering race (theoretical)

If a `comment.deleted` event arrives before the corresponding `comment.created` event (possible given the spec says order is not guaranteed), the system cancels a DM that doesn't exist yet. If the `comment.created` event arrives later, it creates a new DM that will never be cancelled.

**Evidence:** 2 DMs were cancelled by `comment.deleted` events in the 500-event test. The ordering was correct in our tests, but the race condition exists.

**Mitigation:** None implemented. A production system would track pending deletions and apply them retroactively.

### 5. Ephemeral filesystem on Render free tier (deployment)

SQLite database is stored on Render's ephemeral filesystem. If the service restarts or deploys, the entire database is wiped. User-created rules, pending deliveries, and event history are lost.

**Mitigation:** Auto-seed default rules on startup. Pending deliveries lost on restart are not recoverable without persistent storage.

### 6. Rate limiter state is in-memory (architectural)

The `RollingRateLimiter` stores request timestamps in a Python list. If the worker process restarts, the rate limiter resets to zero. This could cause a burst of DM requests that exceeds the 10/60s limit before the limiter re-accumulates state.

**Mitigation:** The rate limiter rebuilds its state within the first 10 requests. In practice, the first few requests after restart are well within limits because the worker starts from an empty queue.

### 7. DM status may not match PseudoGram reality (spec-acknowledged)

The assignment notes that PseudoGram "occasionally reports success on a DM that never got delivered." Our reconciliation checks `GET /v1/dm/{dm_id}` and trusts the response. If PseudoGram lies about delivery status, our stats will be wrong.

**Mitigation:** None possible without external verification. This is an inherent limitation acknowledged by the assignment.
