#!/usr/bin/env python3
"""
Real PseudoGram simulator load test.

Prerequisites:
  1. API_KEY env var set (from PseudoGram keygen)
  2. Your webhook publicly reachable (e.g. ngrok, deployed server)
  3. At least one rule created with POST /rules

Usage:
  export API_KEY=your-key-here
  export WEBHOOK_URL=https://your-server.com/webhook
  python scripts/load_test.py

This will:
  1. POST /rules with keyword "PRICE"
  2. Start simulator with 500 events over 10 seconds
  3. Poll until simulation completes
  4. Fetch ground truth
  5. Query our /stats
  6. Compare results
"""

import hashlib
import hmac as hmac_mod
import json
import os
import sys
import time

import httpx

API_KEY = os.environ.get("API_KEY", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
BASE_URL = os.environ.get("BASE_URL", "https://pseudogram-api.onrender.com")

HEADERS = {"X-API-Key": API_KEY}


def create_rule(keyword="PRICE", message="Thanks for your interest!"):
    resp = httpx.post(f"{BASE_URL}/v1/keygen", json={"email": "load-test@example.com"}, headers=HEADERS, timeout=30)
    print(f"[keygen] status={resp.status_code}")
    # Use our own API to create rules
    # The rule is stored in our DB, not PseudoGram's


def start_simulation(webhook_url, count=500, duration=10):
    resp = httpx.post(
        f"{BASE_URL}/v1/simulate/start",
        json={"webhook_url": webhook_url, "count": count, "duration_seconds": duration},
        headers=HEADERS,
        timeout=30,
    )
    print(f"[simulate] status={resp.status_code}")
    print(f"[simulate] response: {resp.json()}")
    return resp.json().get("run_id")


def poll_truth(run_id, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        resp = httpx.get(f"{BASE_URL}/v1/simulate/{run_id}/truth", headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "completed":
                return data
        print(f"[poll] status={resp.status_code}, waiting...")
        time.sleep(5)
    return None


def get_stats(webhook_base):
    resp = httpx.get(f"{webhook_base}/stats", timeout=10)
    return resp.json()


def main():
    if not API_KEY:
        print("ERROR: Set API_KEY env var")
        sys.exit(1)
    if not WEBHOOK_URL:
        print("ERROR: Set WEBHOOK_URL env var (must be publicly reachable)")
        sys.exit(1)

    webhook_base = WEBHOOK_URL.rsplit("/webhook", 1)[0]

    # Create rule
    print("=== Creating rule ===")
    rule_resp = httpx.post(f"{webhook_base}/rules", json={"keyword": "PRICE", "dm_message": "Thanks!"}, timeout=10)
    print(f"Rule created: {rule_resp.json()}")

    # Start simulation
    print(f"\n=== Starting simulation: 500 events over 10s ===")
    print(f"Webhook URL: {WEBHOOK_URL}")
    run_id = start_simulation(WEBHOOK_URL, count=500, duration=10)

    if not run_id:
        print("ERROR: No run_id returned")
        sys.exit(1)

    print(f"Run ID: {run_id}")

    # Wait for simulation to complete
    print("\n=== Waiting for simulation to complete ===")
    truth = poll_truth(run_id)

    if not truth:
        print("ERROR: Simulation did not complete in time")
        sys.exit(1)

    print(f"\n=== Ground Truth ===")
    print(json.dumps(truth, indent=2))

    # Get our stats
    print(f"\n=== Our /stats ===")
    stats = get_stats(webhook_base)
    print(json.dumps(stats, indent=2))

    # Compare
    print(f"\n=== Comparison ===")
    truth_events = truth.get("total_events", 0)
    truth_delivered = truth.get("deliveries_expected", 0)
    our_sent = stats.get("sent", 0)
    our_queued = stats.get("queued", 0)
    our_failed = stats.get("failed", 0)
    our_dups = stats.get("duplicates_blocked", 0)

    print(f"Truth events:     {truth_events}")
    print(f"Truth deliveries: {truth_delivered}")
    print(f"Our sent:         {our_sent}")
    print(f"Our queued:       {our_queued}")
    print(f"Our failed:       {our_failed}")
    print(f"Our duplicates:   {our_dups}")
    print(f"Match: {our_sent == truth_delivered}")


if __name__ == "__main__":
    main()
