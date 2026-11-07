#!/usr/bin/env python3
"""Create a small campaign, cancel part of recipients, wait, verify summary and idempotency."""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

API = os.environ.get("API", "http://127.0.0.1:8080")
LEAD = timedelta(seconds=10)


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)


def decode(raw, status, method, path):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        fail(f"{method} {path} returned status {status} and a non-JSON body")


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            raw = res.read()
            return res.status, decode(raw, res.status, method, path)
    except urllib.error.HTTPError as err:
        raw = err.read()
        return err.code, decode(raw, err.code, method, path)


def campaign_payload(key):
    scheduled = datetime.now(timezone.utc) + LEAD
    scheduled = scheduled.replace(microsecond=scheduled.microsecond)
    return {
        "idempotency_key": key,
        "scheduled_at": scheduled.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sender": "mock",
        "template": {
            "subject": "[name], we have an offer",
            "body": "Your discount is [sum] [currency]",
        },
        "recipients": [
            {
                "recipient_id": "demo-a",
                "to": "a@example.com",
                "variables": {"name": "Alice", "sum": "10", "currency": "UAH"},
            },
            {
                "recipient_id": "demo-b",
                "to": "b@example.com",
                "variables": {"name": "Bob", "sum": "20", "currency": "UAH"},
            },
            {
                "recipient_id": "demo-c",
                "to": "c@example.com",
                "variables": {"name": "Carol", "sum": "30", "currency": "UAH"},
            },
        ],
    }, scheduled


def main():
    status, health = request("GET", "/healthz")
    if status != 200 or health.get("status") != "ok":
        fail(f"health check failed: {status} {health}")

    key = f"demo-campaign-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    body, scheduled = campaign_payload(key)
    status, created = request("POST", "/campaigns", body)
    if status != 201:
        fail(f"create campaign failed: {status} {created}")
    campaign_id = created.get("campaign_id")
    if not campaign_id or created.get("total_jobs") != 3:
        fail(f"unexpected create response: {created}")

    status, cancel = request(
        "POST",
        f"/campaigns/{campaign_id}/cancel",
        {"recipient_ids": ["demo-b"]},
    )
    if status != 200:
        fail(f"cancel failed: {status} {cancel}")
    results = {r["recipient_id"]: r["result"] for r in cancel.get("results", [])}
    if results.get("demo-b") != "canceled":
        fail(f"expected demo-b canceled, got {results}")

    deadline = scheduled + timedelta(seconds=30)
    while datetime.now(timezone.utc) < deadline:
        status, summary = request("GET", f"/campaigns/{campaign_id}")
        if status != 200:
            fail(f"get campaign failed: {status} {summary}")
        counts = summary.get("counts", {})
        succeeded = counts.get("succeeded", 0)
        canceled = counts.get("canceled", 0)
        if succeeded == 2 and canceled == 1:
            break
        time.sleep(0.5)
    else:
        fail(f"timeout waiting for final counts: {summary}")

    status, replay = request("POST", "/campaigns", body)
    if status != 200 or replay.get("campaign_id") != campaign_id:
        fail(f"idempotent replay failed: {status} {replay}")

    print(
        json.dumps(
            {
                "campaign_id": campaign_id,
                "counts": summary.get("counts"),
                "total_jobs": summary.get("total_jobs"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
