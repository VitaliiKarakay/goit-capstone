#!/usr/bin/env python3
"""Cancel impact on campaign start lag (2 workers, manual scale)."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import perf_lib as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = Path(__file__).resolve()
OUTPUT_DIR = Path(os.environ["CANCEL_OUTPUT_DIR"])
TRIAL = os.environ.get("CANCEL_TRIAL", "0") == "1"
GIT_SHA = os.environ.get("GIT_SHA", "unknown")
BASELINE_DIR = Path(os.environ.get("PERF_BASELINE_DIR", REPO_ROOT / "perf-workers/run-20261122T160711Z"))
EXPECTED_IMAGE_TAG = os.environ.get("EXPECTED_IMAGE_TAG", "b700b36")

WORKERS = 2
CAMPAIGN_SIZE = int(os.environ.get("CANCEL_CAMPAIGN_SIZE", "120" if TRIAL else "1000"))
LEAD_SECONDS = int(os.environ.get("CANCEL_LEAD_SECONDS", "90"))
MOCK_DELAY = os.environ.get("CANCEL_MOCK_SEND_DELAY", "200ms" if TRIAL else "500ms")

RUN_PLAN = (
    [(1, 25), (1, 50), (2, 50), (2, 25), (3, 25), (3, 50)]
    if not TRIAL
    else [(1, 25)]
)


def all_recipient_ids() -> list[str]:
    return [f"perf-{i}" for i in range(CAMPAIGN_SIZE)]


def cancel_sample(round_n: int, pct: int) -> tuple[list[str], str]:
    seed = f"cancel-impact-r{round_n}-v1"
    n_cancel = CAMPAIGN_SIZE * pct // 100
    ids = all_recipient_ids()
    rng = random.Random(seed)
    rng.shuffle(ids)
    return ids[:n_cancel], seed


def build_campaign(scheduled: datetime, round_n: int, pct: int) -> dict:
    key = f"cancel-r{round_n}-p{pct}-{scheduled.strftime('%Y%m%d%H%M%S%f')}"
    recipients = []
    for i in range(CAMPAIGN_SIZE):
        recipients.append(
            {
                "recipient_id": f"perf-{i}",
                "to": f"u{i}@example.com",
                "variables": {"name": f"U{i}", "sum": str(i % 100), "currency": "UAH"},
            }
        )
    return {
        "idempotency_key": key,
        "scheduled_at": scheduled.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sender": "mock",
        "template": {"subject": "Hello [name]", "body": "Sum [sum] [currency]"},
        "recipients": recipients,
    }


def cancel_recipients(campaign_id: str, recipient_ids: list[str]) -> None:
    status, body = pl.api_request(
        "POST",
        f"/campaigns/{campaign_id}/cancel",
        {"recipient_ids": recipient_ids},
    )
    if status != 200:
        raise RuntimeError(f"cancel API {status}: {body}")
    for r in body.get("results", []):
        if r.get("result") not in ("canceled", "already_canceled"):
            raise RuntimeError(f"cancel result for {r.get('recipient_id')}: {r.get('result')}")


def assert_pre_start_counts(campaign_id: str, n_cancel: int, scheduled: datetime) -> dict:
    deadline = scheduled - timedelta(milliseconds=500)
    last = None
    while datetime.now(timezone.utc) < deadline:
        _, summary = pl.api_request("GET", f"/campaigns/{campaign_id}")
        counts = summary.get("counts") or {}
        last = counts
        if (
            counts.get("canceled") == n_cancel
            and counts.get("scheduled") == CAMPAIGN_SIZE - n_cancel
            and counts.get("succeeded", 0) == 0
            and counts.get("queued", 0) == 0
            and counts.get("processing", 0) == 0
        ):
            return summary
        time.sleep(0.5)
    pl.fail(f"pre-start counts invalid before scheduled_at: want canceled={n_cancel}, scheduled={CAMPAIGN_SIZE - n_cancel}, got {last}")
    return {}


def wait_campaign_terminal(campaign_id: str, n_cancel: int, deadline_s: float = 3600) -> dict:
    n_ok = CAMPAIGN_SIZE - n_cancel
    end = time.time() + deadline_s
    while time.time() < end:
        _, summary = pl.api_request("GET", f"/campaigns/{campaign_id}")
        counts = summary.get("counts") or {}
        if counts.get("succeeded") == n_ok and counts.get("canceled") == n_cancel:
            bad = [k for k, v in counts.items() if k not in ("succeeded", "canceled") and v not in (0, None)]
            if not bad:
                return summary
        time.sleep(5)
    pl.fail(f"campaign {campaign_id}: terminal counts not reached (want succeeded={n_ok}, canceled={n_cancel})")
    return {}


def validate_jobs(
    jobs: list[dict],
    campaign_scheduled: datetime,
    cancel_ids: set[str],
) -> tuple[dict, str | None]:
    n_cancel = len(cancel_ids)
    n_ok = CAMPAIGN_SIZE - n_cancel
    if len(jobs) != CAMPAIGN_SIZE:
        return {}, f"expected {CAMPAIGN_SIZE} jobs, got {len(jobs)}"

    succeeded_lags: list[float] = []
    finished_ok: list[datetime] = []
    seen_cancel: set[str] = set()

    for row in jobs:
        rid = row.get("recipient_id", "")
        st = row.get("status")
        if rid in cancel_ids:
            if st != "canceled":
                return {}, f"recipient {rid} expected canceled, got {st}"
            if not row.get("canceled_at") or row.get("send_started_at"):
                return {}, f"recipient {rid}: canceled must have canceled_at, no send_started_at"
            ca = pl.parse_ts(row["canceled_at"])
            sa = pl.parse_ts(row["scheduled_at"])
            if ca >= sa:
                return {}, f"recipient {rid}: canceled_at must be < scheduled_at"
            seen_cancel.add(rid)
            continue
        if st != "succeeded":
            return {}, f"recipient {rid} expected succeeded, got {st}"
        for col in ("scheduled_at", "send_started_at", "finished_at"):
            if not row.get(col):
                return {}, f"recipient {rid}: missing {col}"
        sa = pl.parse_ts(row["scheduled_at"])
        ss = pl.parse_ts(row["send_started_at"])
        fi = pl.parse_ts(row["finished_at"])
        if not (sa <= ss <= fi):
            return {}, f"recipient {rid}: scheduled <= send_started <= finished violated"
        succeeded_lags.append((ss - sa).total_seconds())
        finished_ok.append(fi)

    if seen_cancel != cancel_ids:
        return {}, f"canceled recipient set mismatch: extra/missing {len(seen_cancel)} vs {n_cancel}"

    sched_set = {pl.parse_ts(j["scheduled_at"]).isoformat() for j in jobs}
    if len(sched_set) != 1:
        return {}, "jobs have multiple scheduled_at values"
    if abs((pl.parse_ts(jobs[0]["scheduled_at"]) - campaign_scheduled).total_seconds()) > 1.0:
        return {}, "job scheduled_at differs from campaign"

    succeeded_lags.sort()
    max_finished = max(finished_ok)
    proc_s = (max_finished - campaign_scheduled).total_seconds()
    if proc_s <= 0:
        return {}, "non-positive campaign processing duration"

    metrics = {
        "campaign_processing_seconds": proc_s,
        "throughput_jobs_per_second": n_ok / proc_s,
        "start_lag_p50_seconds": pl.percentile_nearest_rank(succeeded_lags, 50),
        "start_lag_p95_seconds": pl.percentile_nearest_rank(succeeded_lags, 95),
        "succeeded_count": n_ok,
        "canceled_count": n_cancel,
        "percentile_method": "nearest_rank",
        "max_finished_at_utc": max_finished.isoformat(),
    }
    return metrics, None


def log_run(result: dict) -> None:
    run_dir = OUTPUT_DIR / result["label"]
    (run_dir / "run.json").write_text(json.dumps(result, indent=2))
    with (OUTPUT_DIR / "runs.jsonl").open("a") as fh:
        fh.write(json.dumps(result) + "\n")


def run_one(round_n: int, pct: int) -> dict:
    label = f"round{round_n}-cancel{pct}pct"
    prefix = label
    run_dir = OUTPUT_DIR / label
    run_dir.mkdir(parents=True, exist_ok=True)
    cancel_ids, seed = cancel_sample(round_n, pct)
    cancel_set = set(cancel_ids)

    (run_dir / "cancel_recipients.txt").write_text("\n".join(cancel_ids) + "\n")
    (run_dir / "cancel_seed.json").write_text(json.dumps({"round": round_n, "seed": seed, "cancel_pct": pct}, indent=2))

    result: dict = {
        "label": label,
        "round": round_n,
        "cancel_pct": pct,
        "workers": WORKERS,
        "status": "failed",
        "seed": seed,
        "imported_baseline": False,
    }

    pl.assert_quiet_stand(prefix)
    pl.scale_workers(WORKERS)
    pl.wait_ready_and_targets(WORKERS, pl.STABLE_TARGETS_SECONDS, prefix)

    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=LEAD_SECONDS)
    body = build_campaign(scheduled_at, round_n, pct)
    status, created = pl.api_request("POST", "/campaigns", body)
    if status != 201:
        result["failure_reason"] = f"create: {status} {created}"
        log_run(result)
        pl.fail(result["failure_reason"])
    campaign_id = created["campaign_id"]
    campaign_scheduled = pl.parse_ts(created.get("scheduled_at") or body["scheduled_at"])
    result["campaign_id"] = campaign_id

    cancel_recipients(campaign_id, cancel_ids)
    assert_pre_start_counts(campaign_id, len(cancel_ids), campaign_scheduled)

    while datetime.now(timezone.utc) < campaign_scheduled:
        time.sleep(0.5)

    summary = wait_campaign_terminal(campaign_id, len(cancel_ids))
    jobs = pl.fetch_jobs_sql(campaign_id)
    fields = [
        "job_id",
        "recipient_id",
        "status",
        "scheduled_at",
        "send_started_at",
        "finished_at",
        "canceled_at",
    ]
    with (run_dir / "jobs.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(jobs)

    job_metrics, job_err = validate_jobs(jobs, campaign_scheduled, cancel_set)
    if job_err:
        result["failure_reason"] = job_err
        result["counts"] = summary.get("counts")
        log_run(result)
        pl.fail(f"{prefix}: {job_err}")

    result.update(job_metrics)
    max_fin = pl.parse_ts(job_metrics["max_finished_at_utc"])
    try:
        res_rows, mean_cpu, max_rss = pl.collect_resources(campaign_scheduled, max_fin, WORKERS)
    except Exception as err:
        res_rows, mean_cpu, max_rss = [], None, None
        result["resources_error"] = str(err)

    with (run_dir / "resources.csv").open("w", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "timestamp_utc",
                "worker_instances_cpu",
                "worker_instances_rss",
                "sum_worker_process_cpu_cores",
                "sum_worker_process_rss_mib",
            ],
        )
        w.writeheader()
        w.writerows(res_rows)

    if mean_cpu is None or max_rss is None:
        result["status"] = "incomplete"
        result["failure_reason"] = result.get("resources_error") or "missing resource samples (2 workers)"
        result["mean_sum_worker_process_cpu_cores"] = mean_cpu
        result["max_observed_sum_worker_process_rss_mib"] = max_rss
        log_run(result)
        pl.fail(f"{prefix}: {result['failure_reason']}")

    result["status"] = "ok"
    result["mean_sum_worker_process_cpu_cores"] = mean_cpu
    result["max_observed_sum_worker_process_rss_mib"] = max_rss
    result["counts"] = summary.get("counts")
    log_run(result)
    return result


def write_metadata() -> None:
    tag_file = REPO_ROOT / "scripts/k8s/.image-tag"
    doc = {
        "git_sha": GIT_SHA,
        "verify_cancel_impact_sha256": hashlib.sha256(SCRIPT_PATH.read_bytes()).hexdigest(),
        "perf_baseline_dir": str(BASELINE_DIR),
        "baseline_note": "Three 0% cancel control runs imported from iteration 9 (workers=2)",
        "expected_image_tag": EXPECTED_IMAGE_TAG,
        "worker_image": pl.kubectl("get", "deployment", "worker", "-o", "jsonpath={.spec.template.spec.containers[0].image}"),
        "controller_image": pl.kubectl("get", "deployment", "controller", "-o", "jsonpath={.spec.template.spec.containers[0].image}"),
        "mock_send_delay_run": MOCK_DELAY,
        "campaign_size": CAMPAIGN_SIZE,
        "lead_seconds": LEAD_SECONDS,
        "workers": WORKERS,
        "run_plan": RUN_PLAN,
        "acceptance_totals": {
            "new_series_campaigns": 6,
            "new_succeeded": 3750,
            "new_canceled": 2250,
            "with_baseline_campaigns": 9,
            "with_baseline_succeeded": 6750,
            "with_baseline_canceled": 2250,
        },
    }
    try:
        doc["minikube_profile"] = json.loads(
            subprocess.check_output(["minikube", "-p", "scheduler-local", "profile", "list", "-o", "json"], text=True)
        )
    except Exception as err:
        doc["minikube_profile_error"] = str(err)
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(doc, indent=2))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _, health = pl.api_request("GET", "/healthz")
    if health.get("status") != "ok":
        pl.fail(f"healthz: {health}")
    if not TRIAL and not BASELINE_DIR.is_dir():
        pl.fail(f"baseline dir missing: {BASELINE_DIR}")

    pl.patch_mock(MOCK_DELAY, "0")
    if not TRIAL:
        pl.assert_stand_compat(EXPECTED_IMAGE_TAG)
    pl.scale_workers(WORKERS)
    pl.ensure_api_port_forward()
    pl.ensure_prom_port_forward()
    write_metadata()

    ok_runs: list[dict] = []
    for round_n, pct in RUN_PLAN:
        print(f"RUN round={round_n} cancel_pct={pct}", flush=True)
        try:
            ok_runs.append(run_one(round_n, pct))
        except SystemExit as exc:
            if exc.code not in (None, 0):
                raise
            raise

    if not TRIAL and len(ok_runs) == 6:
        subprocess.check_call(
            [sys.executable, str(REPO_ROOT / "scripts/k8s/rebuild-cancel-summary.py"), "--run-dir", str(OUTPUT_DIR)],
        )
        print("PASS: cancel impact series", file=sys.stderr)
    elif TRIAL:
        print("PASS: cancel impact trial", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    finally:
        pl.stop_owned_port_forwards()