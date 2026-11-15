#!/usr/bin/env python3
"""Crash demo: worker freezes after Claim (demorecovery build), then SIGKILL.

Requires:
  docker compose -f compose.yaml -f compose.demorecovery.yaml up -d --scale worker=1 worker

Exit 0 when the job returns to queued after controller lease recovery.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from uuid import uuid4

API = os.environ.get("API", "http://127.0.0.1:8080")
COMPOSE_FILES = os.environ.get(
    "COMPOSE_FILE", "compose.yaml:compose.demorecovery.yaml"
).split(":")


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
    for f in COMPOSE_FILES:
        cmd.extend(["-f", f])
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
        fail(f"expected one worker, found {len(workers)} (use --scale worker=1)")

    scheduled = (datetime.now(timezone.utc) - timedelta(seconds=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    key = "kill-" + uuid4().hex
    body = {
        "idempotency_key": key,
        "recipient_id": "recovery-user",
        "scheduled_at": scheduled,
        "sender": "mock",
        "payload": {"to": "u@example.com", "subject": "recovery", "body": "recovery"},
    }
    status, created = request("POST", "/jobs", body)
    if status != 201 or not created.get("id"):
        fail(f"create job failed: {status} {created}")
    job_id = created["id"]

    deadline = time.time() + 60
    while time.time() < deadline:
        _, job = request("GET", f"/jobs/{job_id}")
        if job.get("status") == "processing" and not job.get("send_started_at"):
            break
        time.sleep(0.25)
    else:
        fail("timed out waiting for processing without send_started_at (demorecovery pause)")

    target = workers[0]
    subprocess.run(["docker", "kill", "-s", "KILL", target], check=True)

    recover_deadline = time.time() + 90
    while time.time() < recover_deadline:
        _, job = request("GET", f"/jobs/{job_id}")
        if job.get("status") == "queued":
            print("job requeued after SIGKILL recovery demo", job_id)
            return
        time.sleep(0.5)
    fail(f"job did not return to queued: last status={job.get('status')}")


if __name__ == "__main__":
    main()