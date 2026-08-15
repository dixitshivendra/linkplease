# LinkPlease

Automated Instagram DM system. When someone comments "PRICE" on a creator's post, we DM them the price list.

## Architecture

- **FastAPI** web server receives webhook events from PseudoGram
- **SQLite** database acts as a durable queue (WAL mode, busy_timeout=5s)
- **Background worker** (same process via `multiprocessing`) polls DB, sends DMs with rate limiting
- **Reconciliation loop** checks if accepted DMs actually got delivered or failed

## API Routes

| Route | Method | Description |
|-------|--------|-------------|
| `/webhook` | POST | Receives comment events, returns 200 immediately |
| `/rules` | POST | Create keyword → DM rules |
| `/stats` | GET | Live stats: sent, failed, queued, duplicates_blocked |
| `/health` | GET | Health check |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # add API_KEY and WEBHOOK_SECRET
```

## Run

```bash
# API + worker (combined process)
python -m app.main_proc

# API only
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Worker only
python -m app.worker
```

## Run Tests

```bash
python -m pytest tests/ -v
```

## Key Features

- HMAC webhook signature verification
- Atomic event dedup (INSERT OR IGNORE on event_id)
- Per-user rule dedup (same user never DMed twice for same rule)
- Rolling rate limiter (10 req/60s)
- Exponential backoff retries (429 with Retry-After, 5xx with backoff)
- Delivery reconciliation (checks DM status, retries failures)
- comment.deleted handling (cancels pending DMs)
- Auto-seed default PRICE rule on empty DB

## Deployment

Deployed on Render (Python 3.11). GitHub Actions keep-alive cron pings `/health` every 5 minutes to prevent sleep.

## Known Limitations

See [FAILURES.md](FAILURES.md) for documented failure modes.
