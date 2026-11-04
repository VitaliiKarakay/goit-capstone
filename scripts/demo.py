#!/usr/bin/env python3
"""Create a job a few seconds ahead and check that the mock sender runs only after scheduled_at."""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

API = os.environ.get("API", "http://127.0.0.1:8080")
LEAD = timedelta(seconds=8)


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
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            raw = res.read()
            return res.status, decode(raw, res.status, method, path)
    except urllib.error.HTTPError as err:
        raw = err.read()
        return err.code, decode(raw, err.code, method, path)


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    status, health = request("GET", "/healthz")
    if status != 200 or health.get("status") != "ok":
        fail(f"health check failed: {status} {health}")

    scheduled = datetime.now(timezone.utc) + LEAD
    scheduled = scheduled.replace(microsecond=scheduled.microsecond)
    body = {
        "idempotency_key": "demo-" + scheduled.strftime("%Y%m%d%H%M%S%f"),
        "recipient_id": "user-demo",
        "scheduled_at": scheduled.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sender": "mock",
        "payload": {
            "to": "user@example.com",
            "subject": "Reminder",
            "body": "Demo message",
        },
    }
    status, created = request("POST", "/jobs", body)
    if status != 201 or not created.get("id"):
        fail(f"create failed: {status} {created}")
    job_id = created["id"]
    print(f"created {job_id} scheduled_at={body['scheduled_at']}")

    status, job = request("GET", f"/jobs/{job_id}")
    if status != 200 or job.get("status") != "scheduled" or job.get("send_started_at") is not None:
        fail(f"job is already in progress: {status} {job}")

    while datetime.now(timezone.utc) < scheduled - timedelta(seconds=2):
        status, job = request("GET", f"/jobs/{job_id}")
        if status != 200 or job.get("send_started_at") is not None or job.get("status") in {"processing", "succeeded", "failed"}:
            fail(f"sender started early: {job}")
        time.sleep(0.2)

    deadline = scheduled + timedelta(seconds=20)
    while datetime.now(timezone.utc) < deadline:
        status, job = request("GET", f"/jobs/{job_id}")
        if status == 200 and job.get("status") == "succeeded":
            break
        if status == 200 and job.get("status") == "failed":
            fail(f"job failed: {job}")
        time.sleep(0.3)
    else:
        fail(f"timed out waiting for succeeded: {job}")

    started = parse_time(job["send_started_at"])
    if started < scheduled:
        fail(f"send_started_at {job['send_started_at']} is before scheduled_at")
    if not job.get("worker_id"):
        fail("worker_id is empty")
    print(f"succeeded worker_id={job['worker_id']} send_started_at={job['send_started_at']}")

    status, replay = request("POST", "/jobs", body)
    if status != 200 or replay.get("id") != job_id:
        fail(f"idempotent replay failed: {status} {replay}")
    print("idempotent replay returned the same id")

    conflict = dict(body)
    conflict["payload"] = dict(body["payload"], subject="Other subject")
    status, payload = request("POST", "/jobs", conflict)
    if status != 409 or payload.get("error", {}).get("code") != "idempotency_conflict":
        fail(f"conflict was not reported: {status} {payload}")
    print("different payload returned idempotency_conflict")


if __name__ == "__main__":
    main()
