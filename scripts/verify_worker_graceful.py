#!/usr/bin/env python3
"""Manual/CI check: a 12s mock send survives docker stop -t 30 on the worker.

Requires Compose workers with MOCK_SEND_DELAY=12s and a single worker replica so the
job is not stolen after stop:

  export API_PORT=18080 MOCK_SEND_DELAY=12s MOCK_FAILURE_RATE=0
  docker compose up -d --no-deps --scale worker=1 worker

Exit 0 if the job reaches succeeded after stopping the worker.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

API = os.environ.get("API", "http://127.0.0.1:18080")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "")
STOP_TIMEOUT = int(os.environ.get("WORKER_STOP_TIMEOUT", "30"))


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return res.status, json.loads(res.read() or b"{}")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def compose_cmd(*args):
    cmd = ["docker", "compose"]
    if COMPOSE_PROJECT:
        cmd.extend(["-p", COMPOSE_PROJECT])
    cmd.extend(list(args))
    return cmd


def worker_container_ids():
    out = subprocess.check_output(compose_cmd("ps", "-q", "worker"), text=True).strip()
    if not out:
        fail("no worker containers running")
    return [line for line in out.splitlines() if line]


def main():
    status, health = request("GET", "/healthz")
    if status != 200 or health.get("status") != "ok":
        fail(f"health check failed: {status} {health}")

    workers = worker_container_ids()
    if len(workers) != 1:
        fail(f"expected exactly one worker for this check, found {len(workers)} — use --scale worker=1")

    scheduled = (datetime.now(timezone.utc) - timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    body = {
        "idempotency_key": "grace-" + uuid_hint(),
        "recipient_id": "grace-user",
        "scheduled_at": scheduled,
        "sender": "mock",
        "payload": {"to": "u@example.com", "subject": "grace", "body": "grace"},
    }
    status, created = request("POST", "/jobs", body)
    if status != 201 or not created.get("id"):
        fail(f"create failed: {status} {created}")
    job_id = created["id"]
    print(f"created {job_id}")

    deadline = time.monotonic() + 90
    send_started = False
    while time.monotonic() < deadline:
        status, job = request("GET", f"/jobs/{job_id}")
        if status != 200:
            fail(f"get job: {status}")
        if job.get("status") == "succeeded":
            print("job already succeeded before stop (mock delay too low?)")
            sys.exit(0)
        if job.get("send_started_at"):
            send_started = True
            break
        if job.get("status") in ("failed", "canceled"):
            fail(f"unexpected terminal before stop: {job}")
        time.sleep(0.2)
    if not send_started:
        fail("timed out waiting for send_started_at — check scheduler/worker/MOCK_SEND_DELAY")

    cid = workers[0]
    print(f"stopping worker container {cid} (timeout {STOP_TIMEOUT}s)")
    subprocess.run(["docker", "stop", "-t", str(STOP_TIMEOUT), cid], check=True)

    finish_by = time.monotonic() + 25
    while time.monotonic() < finish_by:
        status, job = request("GET", f"/jobs/{job_id}")
        if status != 200:
            fail(f"get job: {status}")
        if job.get("status") == "succeeded":
            print("job succeeded after worker stop")
            return
        time.sleep(0.5)
    fail(f"job did not succeed in time: {job}")


def uuid_hint():
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")


if __name__ == "__main__":
    main()