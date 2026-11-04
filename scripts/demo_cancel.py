#!/usr/bin/env python3
"""Run one job to completion and cancel another before it is processed."""

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
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            raw = res.read()
            return res.status, decode(raw, res.status, method, path)
    except urllib.error.HTTPError as err:
        raw = err.read()
        return err.code, decode(raw, err.code, method, path)


def cancel(job_id):
    req = urllib.request.Request(
        API + f"/jobs/{job_id}/cancel",
        method="POST",
        headers={},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            raw = res.read()
            return res.status, decode(raw, res.status, "POST", f"/jobs/{job_id}/cancel")
    except urllib.error.HTTPError as err:
        raw = err.read()
        return err.code, decode(raw, err.code, "POST", f"/jobs/{job_id}/cancel")


def job_body(key_suffix, subject):
    scheduled = datetime.now(timezone.utc) + LEAD
    scheduled = scheduled.replace(microsecond=scheduled.microsecond)
    return {
        "idempotency_key": f"demo-cancel-{key_suffix}-{scheduled.strftime('%Y%m%d%H%M%S%f')}",
        "recipient_id": "user-demo",
        "scheduled_at": scheduled.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sender": "mock",
        "payload": {
            "to": "user@example.com",
            "subject": subject,
            "body": "Demo message",
        },
    }, scheduled


def main():
    status, health = request("GET", "/healthz")
    if status != 200 or health.get("status") != "ok":
        fail(f"health check failed: {status} {health}")

    run_body, scheduled = job_body("run", "Will run")
    cancel_body, _ = job_body("stop", "Will cancel")

    status, run_job = request("POST", "/jobs", run_body)
    if status != 201 or not run_job.get("id"):
        fail(f"create run job failed: {status} {run_job}")
    run_id = run_job["id"]

    status, stop_job = request("POST", "/jobs", cancel_body)
    if status != 201 or not stop_job.get("id"):
        fail(f"create cancel job failed: {status} {stop_job}")
    stop_id = stop_job["id"]

    code, resp = cancel(stop_id)
    if code != 200 or resp.get("result") not in {"canceled", "already_canceled"}:
        fail(f"cancel failed: {code} {resp}")
    print(f"canceled {stop_id} result={resp.get('result')}")

    stop_view = request("GET", f"/jobs/{stop_id}")[1]
    if stop_view.get("status") != "canceled" or stop_view.get("send_started_at") is not None:
        fail(f"cancel job was processed: {stop_view}")

    deadline = scheduled + timedelta(seconds=20)
    run_view = None
    while datetime.now(timezone.utc) < deadline:
        status, run_view = request("GET", f"/jobs/{run_id}")
        if status == 200 and run_view.get("status") == "succeeded":
            break
        if status == 200 and run_view.get("status") == "failed":
            fail(f"run job failed: {run_view}")
        time.sleep(0.3)
    else:
        fail(f"timed out waiting for run job: {run_view}")

    if not run_view.get("send_started_at") or not run_view.get("worker_id"):
        fail(f"run job did not complete normally: {run_view}")

    stop_view = request("GET", f"/jobs/{stop_id}")[1]
    if stop_view.get("status") != "canceled":
        fail(f"cancel job status changed: {stop_view}")

    print(f"run job {run_id} succeeded send_started_at={run_view['send_started_at']}")
    print("cancel demo finished")


if __name__ == "__main__":
    main()
