#!/usr/bin/env python3
"""KEDA worker autoscaling verification for scheduler-local (not Compose).

Requires: API, PROM_URL, autoscaling enabled, monitoring up.
Writes CSV (AUTOSCALE_CSV) and exits non-zero on failure. Restores manual mode on exit.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

API = os.environ.get("API", "http://127.0.0.1:18080")
PROM_URL = os.environ.get("PROM_URL", "http://127.0.0.1:9090")
KUBE_CONTEXT = os.environ.get("KUBE_CONTEXT", "scheduler-local")
KUBE_NAMESPACE = os.environ.get("KUBE_NAMESPACE", "scheduler")
CAMPAIGN_SIZE = int(os.environ.get("AUTOSCALE_CAMPAIGN_SIZE", "1000"))
LEAD_SECONDS = int(os.environ.get("AUTOSCALE_LEAD_SECONDS", "90"))
MOCK_DELAY = os.environ.get("AUTOSCALE_MOCK_SEND_DELAY", "50ms")
CSV_PATH = Path(os.environ.get("AUTOSCALE_CSV", "verify-autoscale-timeseries.csv"))
REPO_ROOT = Path(__file__).resolve().parents[2]

QUEUED_PROMQL = """
scheduler_jobs_by_status{job="scheduler-controller",status="queued"}
and on(job, instance) scheduler_db_snapshot_success{job="scheduler-controller"} == 1
and on(job, instance) (
  time() - scheduler_db_snapshot_last_success_timestamp_seconds{job="scheduler-controller"}
) < 45
""".strip()


def fail(msg: str) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(1)


def kubectl(*args: str) -> str:
    base = []
    if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file():
        base = [str(REPO_ROOT / "scripts/k8s/bin/kubectl")]
    else:
        base = ["kubectl"]
    cmd = [*base, f"--context={KUBE_CONTEXT}", "-n", KUBE_NAMESPACE, *args]
    return subprocess.check_output(cmd, text=True).strip()


def prom_query(query: str) -> float | None:
    url = f"{PROM_URL}/api/v1/query?query={urllib.parse.quote(query)}"
    try:
        with urllib.request.urlopen(url, timeout=15) as res:
            body = json.loads(res.read())
    except urllib.error.URLError as err:
        print(f"prometheus query failed: {err}", file=sys.stderr)
        return None
    if body.get("status") != "success":
        return None
    result = body.get("data", {}).get("result") or []
    if not result:
        return None
    if len(result) != 1:
        fail(f"queued query: expected 1 series, got {len(result)}")
    return float(result[0]["value"][1])


def worker_targets_up() -> int:
    url = f"{PROM_URL}/api/v1/targets"
    with urllib.request.urlopen(url, timeout=15) as res:
        body = json.loads(res.read())
    n = 0
    pods = set()
    for t in body.get("data", {}).get("activeTargets", []):
        labels = t.get("labels") or {}
        if labels.get("job") != "scheduler-workers" or t.get("health") != "up":
            continue
        pod = labels.get("pod", "")
        if pod in pods:
            continue
        pods.add(pod)
        n += 1
    return n


def hpa_desired() -> int | None:
    try:
        out = kubectl(
            "get",
            "hpa",
            "keda-hpa-worker",
            "-o",
            "jsonpath={.status.desiredReplicas}",
        )
        return int(out) if out else None
    except subprocess.CalledProcessError:
        return None


def hpa_metric_error() -> str | None:
    try:
        raw = kubectl("get", "hpa", "keda-hpa-worker", "-o", "json")
        hpa = json.loads(raw)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return "hpa missing"
    for c in hpa.get("status", {}).get("conditions") or []:
        if c.get("type") == "AbleToScale" and c.get("status") == "False":
            return c.get("message") or "AbleToScale=False"
    return None


def ready_workers() -> int:
    out = kubectl("get", "deploy", "worker", "-o", "jsonpath={.status.readyReplicas}")
    return int(out or "0")


def api_request(method: str, path: str, body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body else {},
    )
    with urllib.request.urlopen(req, timeout=60) as res:
        return res.status, json.loads(res.read() or b"{}")


def patch_mock_delay(delay: str) -> None:
    subprocess.check_call(
        [
            str(REPO_ROOT / "scripts/k8s/bin/kubectl")
            if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file()
            else "kubectl",
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
            "patch",
            "configmap",
            "scheduler-app",
            "--type",
            "merge",
            "-p",
            json.dumps(
                {"data": {"MOCK_SEND_DELAY": delay, "MOCK_FAILURE_RATE": "0"}}
            ),
        ]
    )
    subprocess.check_call(
        [
            str(REPO_ROOT / "scripts/k8s/bin/kubectl")
            if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file()
            else "kubectl",
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
            "rollout",
            "restart",
            "deployment/worker",
        ]
    )
    subprocess.check_call(
        [
            str(REPO_ROOT / "scripts/k8s/bin/kubectl")
            if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file()
            else "kubectl",
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
            "rollout",
            "status",
            "deployment/worker",
            "--timeout=300s",
        ]
    )


def build_campaign(lead: int) -> tuple[dict, datetime]:
    scheduled = datetime.now(timezone.utc) + timedelta(seconds=lead)
    key = f"autoscale-{scheduled.strftime('%Y%m%d%H%M%S%f')}"
    recipients = []
    for i in range(CAMPAIGN_SIZE):
        recipients.append(
            {
                "recipient_id": f"as-{i}",
                "to": f"u{i}@example.com",
                "variables": {
                    "name": f"User{i}",
                    "sum": str(i % 100),
                    "currency": "UAH",
                },
            }
        )
    body = {
        "idempotency_key": key,
        "scheduled_at": scheduled.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sender": "mock",
        "template": {
            "subject": "Hello [name]",
            "body": "Sum [sum] [currency]",
        },
        "recipients": recipients,
    }
    return body, scheduled


def sample_row(rows: list, queued: float | None, desired: int | None, ready: int, targets: int):
    rows.append(
        {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "queued": "" if queued is None else str(int(queued) if queued == int(queued) else queued),
            "desired_replicas": "" if desired is None else str(desired),
            "ready_replicas": str(ready),
            "worker_targets_up": str(targets),
        }
    )


def restore_stand() -> None:
    print("restore: controller=1, autoscale-disable, mock defaults", file=sys.stderr)
    subprocess.run(
        [str(REPO_ROOT / "scripts/k8s/bin/kubectl"), f"--context={KUBE_CONTEXT}", "-n", KUBE_NAMESPACE, "scale", "deployment/controller", "--replicas=1"],
        check=False,
    )
    subprocess.run(
        [str(REPO_ROOT / "scripts/k8s/bin/kubectl"), f"--context={KUBE_CONTEXT}", "-n", KUBE_NAMESPACE, "rollout", "status", "deployment/controller", "--timeout=300s"],
        check=False,
    )
    patch_mock_delay("0s")
    subprocess.run([str(REPO_ROOT / "scripts/k8s/autoscale-disable.sh")], cwd=REPO_ROOT, check=False)


def main() -> None:
    errors: list[str] = []
    rows: list[dict[str, str]] = []
    campaign_id = None
    first_queue_growth: str | None = None
    fourth_worker_ready: str | None = None

    try:
        _, health = api_request("GET", "/healthz")
        if health.get("status") != "ok":
            fail(f"healthz: {health}")

        try:
            kubectl("get", "scaledobject", "worker")
        except subprocess.CalledProcessError:
            fail("ScaledObject/worker missing — run scripts/k8s/autoscale-enable.sh")

        patch_mock_delay(MOCK_DELAY)

        # --- Main campaign cycle ---
        body, scheduled = build_campaign(LEAD_SECONDS)
        status, created = api_request("POST", "/campaigns", body)
        if status != 201:
            fail(f"create campaign: {status} {created}")
        campaign_id = created.get("campaign_id")
        print(f"RUN: python3 scripts/k8s/verify-autoscale.py")
        print(f"API={API} PROM_URL={PROM_URL} campaign_id={campaign_id} scheduled_at={body['scheduled_at']}")

        pre_deadline = scheduled - timedelta(seconds=5)
        pre_ok = False
        while datetime.now(timezone.utc) < pre_deadline:
            q = prom_query(QUEUED_PROMQL)
            sample_row(rows, q, hpa_desired(), ready_workers(), worker_targets_up())
            if q == 0 and ready_workers() == 1:
                pre_ok = True
                break
            time.sleep(5)
        if not pre_ok:
            errors.append("before start: expected queued=0 and ready_workers=1")

        while datetime.now(timezone.utc) < scheduled:
            time.sleep(1)

        scale_up_deadline = time.time() + 600
        saw_four = False
        while time.time() < scale_up_deadline:
            q = prom_query(QUEUED_PROMQL)
            ready = ready_workers()
            desired = hpa_desired()
            targets = worker_targets_up()
            sample_row(rows, q, desired, ready, targets)
            if q is not None and q > 0 and first_queue_growth is None:
                first_queue_growth = datetime.now(timezone.utc).isoformat()
            if ready >= 4 and targets >= 4:
                saw_four = True
                fourth_worker_ready = datetime.now(timezone.utc).isoformat()
                break
            time.sleep(5)
        if not saw_four:
            errors.append("scale-up: expected 4 ready workers and 4 UP prometheus targets")

        # --- Metric loss at 4 replicas (before drain completes) ---
        baseline_desired = hpa_desired() or ready_workers() or 4
        if ready_workers() < 4:
            errors.append("metric loss: expected 4 ready workers before controller shutdown")
        kubectl_ctx = [
            str(REPO_ROOT / "scripts/k8s/bin/kubectl")
            if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file()
            else "kubectl",
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
        ]
        subprocess.check_call([*kubectl_ctx, "scale", "deployment/controller", "--replicas=0"])
        subprocess.check_call([*kubectl_ctx, "rollout", "status", "deployment/controller", "--timeout=120s"])

        metric_lost_at = None
        for _ in range(60):
            if prom_query(QUEUED_PROMQL) is None:
                metric_lost_at = time.time()
                break
            time.sleep(3)
        if metric_lost_at is None:
            errors.append("metric loss: queued query did not become empty")
        else:
            err = hpa_metric_error()
            if not err:
                errors.append("metric loss: expected HPA metric/AbleToScale error")

        hold_deadline = metric_lost_at + 90 if metric_lost_at else time.time()
        min_desired = baseline_desired
        while time.time() < hold_deadline:
            d = hpa_desired()
            if d is not None and d < min_desired:
                min_desired = d
            q = prom_query(QUEUED_PROMQL)
            sample_row(rows, q, d, ready_workers(), worker_targets_up())
            time.sleep(5)
        if min_desired < baseline_desired:
            errors.append(
                f"metric loss: desired replicas dropped from {baseline_desired} to {min_desired} while metric unavailable"
            )

        subprocess.check_call([*kubectl_ctx, "scale", "deployment/controller", "--replicas=1"])
        subprocess.check_call([*kubectl_ctx, "rollout", "status", "deployment/controller", "--timeout=300s"])
        for _ in range(40):
            if prom_query(QUEUED_PROMQL) is not None:
                break
            time.sleep(3)
        else:
            errors.append("metric recovery: queued query still empty after controller restored")

        finish_deadline = time.time() + 1800
        drained = False
        while time.time() < finish_deadline:
            _, summary = api_request("GET", f"/campaigns/{campaign_id}")
            counts = summary.get("counts") or {}
            q = prom_query(QUEUED_PROMQL)
            ready = ready_workers()
            sample_row(rows, q, hpa_desired(), ready, worker_targets_up())
            if counts.get("succeeded") == CAMPAIGN_SIZE and (q == 0):
                drained = True
                break
            time.sleep(10)
        if not drained:
            errors.append(f"drain: expected succeeded={CAMPAIGN_SIZE} and queued=0")

        stable_deadline = time.time() + 180
        back_to_one = False
        while time.time() < stable_deadline:
            q = prom_query(QUEUED_PROMQL)
            ready = ready_workers()
            sample_row(rows, q, hpa_desired(), ready, worker_targets_up())
            if q == 0 and ready == 1:
                back_to_one = True
                break
            time.sleep(10)
        if not back_to_one:
            errors.append("scale-down: expected queued=0 and ready_workers=1 after stabilization")

        _, final_summary = api_request("GET", f"/campaigns/{campaign_id}")
        print(
            json.dumps(
                {
                    "campaign_id": campaign_id,
                    "total_jobs": final_summary.get("total_jobs"),
                    "counts": final_summary.get("counts"),
                    "first_queue_growth_utc": first_queue_growth,
                    "four_workers_ready_utc": fourth_worker_ready,
                },
                indent=2,
            )
        )

    finally:
        with CSV_PATH.open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "timestamp_utc",
                    "queued",
                    "desired_replicas",
                    "ready_replicas",
                    "worker_targets_up",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {CSV_PATH}")
        restore_stand()

    if errors:
        for e in errors:
            print(f"FAIL: {e}", file=sys.stderr)
        raise SystemExit(1)
    print("PASS: autoscale verification")


if __name__ == "__main__":
    main()