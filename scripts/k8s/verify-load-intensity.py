#!/usr/bin/env python3
"""Staggered scheduled_at load intensity (standalone jobs, 2 workers)."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import perf_lib as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = Path(__file__).resolve()
OUTPUT_DIR = Path(os.environ["LOAD_OUTPUT_DIR"])
TRIAL = os.environ.get("LOAD_TRIAL", "0") == "1"
GIT_SHA = os.environ.get("GIT_SHA", "unknown")
REF_DIR = Path(os.environ.get("LOAD_REFERENCE_DIR", REPO_ROOT / "perf-workers/run-20261122T160711Z"))
EXPECTED_IMAGE_TAG = os.environ.get("EXPECTED_IMAGE_TAG", "b700b36")

WORKERS = 2
JOB_COUNT = int(os.environ.get("LOAD_JOB_COUNT", "120" if TRIAL else "1000"))
MOCK_DELAY = os.environ.get("LOAD_MOCK_SEND_DELAY", "500ms")
CREATE_LEAD_SECONDS = int(os.environ.get("LOAD_CREATE_LEAD_SECONDS", "90"))
STABLE_SECONDS = int(os.environ.get("LOAD_STABLE_SECONDS", str(pl.STABLE_TARGETS_SECONDS)))
PROCESSING_SLACK_SECONDS = int(os.environ.get("LOAD_PROCESSING_SLACK_SECONDS", "900"))

# jobs/s -> interval ms
RATES = ((2, 500), (5, 200))
RUN_PLAN = (
    [(1, 2), (1, 5), (2, 5), (2, 2), (3, 2), (3, 5)]
    if not TRIAL
    else [(1, 2)]
)


def fail(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


class RunAbort(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason


def rate_interval_ms(jobs_per_s: int) -> int:
    for r, ms in RATES:
        if r == jobs_per_s:
            return ms
    fail(f"unknown rate {jobs_per_s}")


def run_prefix(round_n: int, jobs_per_s: int, started: datetime) -> str:
    return f"load11-r{round_n}-rate{jobs_per_s}-{started.strftime('%Y%m%dT%H%M%S')}"


def idempotency_key(prefix: str, i: int) -> str:
    return f"{prefix}-job-{i:04d}"


def format_scheduled(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def job_payload(i: int) -> dict:
    return {
        "to": f"u{i}@example.com",
        "subject": f"load {i}",
        "body": "load intensity",
    }


def build_job_request(prefix: str, i: int, scheduled: datetime) -> dict:
    return {
        "idempotency_key": idempotency_key(prefix, i),
        "recipient_id": f"{prefix}-r-{i:04d}",
        "scheduled_at": format_scheduled(scheduled),
        "sender": "mock",
        "payload": job_payload(i),
    }


def pg_now() -> datetime:
    sql = (
        "SELECT to_char((now() AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z';"
    )
    env = {**os.environ, "PGPASSWORD": pl.pg_password()}
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    cmd = [
        *base,
        f"--context={pl.KUBE_CONTEXT}",
        "-n",
        pl.KUBE_NAMESPACE,
        "exec",
        "statefulset/postgres",
        "--",
        "psql",
        "-U",
        "scheduler",
        "-d",
        "scheduler",
        "-t",
        "-A",
        "-c",
        sql,
    ]
    raw = subprocess.check_output(cmd, text=True, env=env).strip()
    if not raw:
        fail("pg_now: empty psql output")
    return pl.parse_ts(raw)


def sql_jobs_for_prefix(prefix: str) -> list[dict]:
    pattern = f"{prefix}-job-%"
    sql = f"""
    COPY (
      SELECT id::text AS job_id, idempotency_key, recipient_id, status,
             scheduled_at::text, send_started_at::text, finished_at::text, canceled_at::text
      FROM jobs
      WHERE campaign_id IS NULL AND idempotency_key LIKE '{pattern}'
      ORDER BY idempotency_key
    ) TO STDOUT WITH (FORMAT csv, HEADER true);
    """
    return list(csv.DictReader(pl.psql_raw(sql).splitlines()))


def verify_pre_start(
    prefix: str,
    manifest: list[dict],
    t0: datetime,
    interval_ms: int,
) -> dict:
    db_now = pg_now()
    checked_at_db = db_now.isoformat()
    if db_now >= t0:
        raise RunAbort(f"pre-start too late: DB time past T0 (T0={t0.isoformat()}, db={checked_at_db})")

    rows = sql_jobs_for_prefix(prefix)
    if len(rows) != JOB_COUNT:
        raise RunAbort(f"pre-start: expected {JOB_COUNT} jobs, got {len(rows)}")

    manifest_by_key = {m["idempotency_key"]: m for m in manifest}
    manifest_ids = {m["job_id"] for m in manifest if m.get("job_id")}

    for row in rows:
        if row["status"] != "scheduled":
            raise RunAbort(f"pre-start: job {row['job_id']} status={row['status']} (want scheduled)")

    db_ids = {r["job_id"] for r in rows}
    db_keys = {r["idempotency_key"] for r in rows}
    if manifest_ids and db_ids != manifest_ids:
        raise RunAbort(
            f"pre-start: job_id set mismatch manifest vs DB ({len(manifest_ids)} vs {len(db_ids)})"
        )
    if set(manifest_by_key) != db_keys:
        raise RunAbort("pre-start: idempotency_key set mismatch manifest vs DB")

    for i in range(JOB_COUNT):
        key = idempotency_key(prefix, i)
        expect = t0 + timedelta(milliseconds=interval_ms * i)
        match = next((r for r in rows if r["idempotency_key"] == key), None)
        if not match:
            raise RunAbort(f"pre-start: missing key {key}")
        got = pl.parse_ts(match["scheduled_at"])
        if abs((got - expect).total_seconds()) > 0.001:
            raise RunAbort(
                f"pre-start: job {i} scheduled_at {got.isoformat()} != {expect.isoformat()}"
            )

    return {"pre_start_checked_at_db_utc": checked_at_db, "jobs_scheduled": len(rows)}


def status_counts(prefix: str) -> dict[str, int]:
    pattern = f"{prefix}-job-%"
    sql = f"""
    SELECT status, count(*)::int
    FROM jobs
    WHERE campaign_id IS NULL AND idempotency_key LIKE '{pattern}'
    GROUP BY status ORDER BY status;
    """
    env = {**os.environ, "PGPASSWORD": pl.pg_password()}
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    cmd = [
        *base,
        f"--context={pl.KUBE_CONTEXT}",
        "-n",
        pl.KUBE_NAMESPACE,
        "exec",
        "statefulset/postgres",
        "--",
        "psql",
        "-U",
        "scheduler",
        "-d",
        "scheduler",
        "-t",
        "-A",
        "-F",
        ",",
        "-c",
        sql,
    ]
    out = subprocess.check_output(cmd, text=True, env=env).strip()
    counts: dict[str, int] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        st, cnt = line.split(",", 1)
        counts[st.strip()] = int(cnt.strip())
    return counts


def wait_all_succeeded(prefix: str, last_scheduled: datetime, label: str) -> None:
    deadline = last_scheduled + timedelta(seconds=PROCESSING_SLACK_SECONDS)
    last_log = 0.0
    while datetime.now(timezone.utc) < deadline:
        counts = status_counts(prefix)
        succ = counts.get("succeeded", 0)
        other = {k: v for k, v in counts.items() if k != "succeeded"}
        now = time.time()
        if now - last_log >= 10:
            print(
                f"WAIT {label}: succeeded={succ}/{JOB_COUNT} other={other}",
                flush=True,
            )
            last_log = now
        if succ == JOB_COUNT and not other:
            return
        if other:
            bad = {k: v for k, v in other.items() if v > 0}
            if any(k not in ("scheduled", "queued", "processing") for k in bad):
                fail(f"{label}: terminal non-success statuses: {counts}")
        time.sleep(3)
    fail(f"{label}: timeout after last_scheduled {last_scheduled.isoformat()}")


def percentile_nearest_rank(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        raise ValueError("empty")
    n = len(sorted_vals)
    k = math.ceil((p / 100.0) * n)
    k = max(1, min(k, n))
    return sorted_vals[k - 1]


def lag_stats(jobs: list[dict], interval_ms: int) -> dict:
    lags: list[tuple[int, float]] = []
    for row in jobs:
        if row["status"] != "succeeded":
            continue
        key = row["idempotency_key"]
        idx = int(key.rsplit("-", 1)[-1])
        sa = pl.parse_ts(row["scheduled_at"])
        ss = pl.parse_ts(row["send_started_at"])
        lags.append((idx, (ss - sa).total_seconds()))
    lags.sort(key=lambda x: x[0])
    vals_sorted = sorted(v for _, v in lags)
    by_hundred: list[dict] = []
    bucket_i = 0
    for lo in range(0, JOB_COUNT, 100):
        hi = min(lo + 100, JOB_COUNT)
        bucket_vals = sorted(v for i, v in lags if lo <= i < hi)
        if not bucket_vals:
            continue
        by_hundred.append(
            {
                "bucket_index": bucket_i,
                "job_index_from": lo,
                "job_index_to_exclusive": hi,
                "count": len(bucket_vals),
                "start_lag_p50_seconds": percentile_nearest_rank(bucket_vals, 50),
                "start_lag_p95_seconds": percentile_nearest_rank(bucket_vals, 95),
            }
        )
        bucket_i += 1

    finished = [pl.parse_ts(r["finished_at"]) for r in jobs if r["status"] == "succeeded" and r.get("finished_at")]
    t0_row = next(r for r in jobs if r["idempotency_key"].endswith("-job-0000"))
    t0 = pl.parse_ts(t0_row["scheduled_at"])
    last_scheduled = t0 + timedelta(milliseconds=interval_ms * (JOB_COUNT - 1))
    tail_drain = (max(finished) - last_scheduled).total_seconds() if finished else float("nan")

    return {
        "start_lag_p50_seconds": percentile_nearest_rank(vals_sorted, 50),
        "start_lag_p95_seconds": percentile_nearest_rank(vals_sorted, 95),
        "percentile_method": "nearest_rank",
        "schedule_span_seconds": (last_scheduled - t0).total_seconds(),
        "last_scheduled_at_utc": last_scheduled.isoformat(),
        "tail_after_last_scheduled_seconds": tail_drain,
        "succeeded_count": len(vals),
        "by_hundred": by_hundred,
        "t0_utc": t0.isoformat(),
    }


def log_run(result: dict) -> None:
    run_dir = OUTPUT_DIR / result["label"]
    (run_dir / "run.json").write_text(json.dumps(result, indent=2))
    with (OUTPUT_DIR / "runs.jsonl").open("a") as fh:
        fh.write(json.dumps(result) + "\n")


def run_one(round_n: int, jobs_per_s: int) -> dict:
    interval_ms = rate_interval_ms(jobs_per_s)
    started = datetime.now(timezone.utc)
    prefix = run_prefix(round_n, jobs_per_s, started)
    label = f"round{round_n}-rate{jobs_per_s}jps"
    run_dir = OUTPUT_DIR / label
    run_dir.mkdir(parents=True, exist_ok=True)
    result: dict = {
        "label": label,
        "round": round_n,
        "jobs_per_second": jobs_per_s,
        "interval_ms": interval_ms,
        "workers": WORKERS,
        "job_count": JOB_COUNT,
        "idempotency_prefix": prefix,
        "status": "failed",
    }

    print(f"RUN round={round_n} rate={jobs_per_s}j/s workers={WORKERS} jobs={JOB_COUNT}", flush=True)
    pl.assert_quiet_stand(label)
    pl.scale_workers(WORKERS)
    pl.wait_ready_and_targets(WORKERS, STABLE_SECONDS, label)

    # T0 after stabilization
    t0 = datetime.now(timezone.utc) + timedelta(seconds=CREATE_LEAD_SECONDS)
    t0 = t0.replace(microsecond=(t0.microsecond // 1000) * 1000)
    result["t0_utc"] = t0.isoformat()

    manifest: list[dict] = []
    for i in range(JOB_COUNT):
        sched = t0 + timedelta(milliseconds=interval_ms * i)
        body = build_job_request(prefix, i, sched)
        status, created = pl.api_request("POST", "/jobs", body)
        if status not in (200, 201):
            result["failure_reason"] = f"create job {i}: {status} {created}"
            log_run(result)
            fail(result["failure_reason"])
        manifest.append(
            {
                "index": i,
                "job_id": created.get("id"),
                "idempotency_key": body["idempotency_key"],
                "scheduled_at_utc": body["scheduled_at"],
            }
        )
    (run_dir / "jobs_manifest.json").write_text(json.dumps(manifest, indent=2))

    try:
        pre = verify_pre_start(prefix, manifest, t0, interval_ms)
        result.update(pre)
    except RunAbort as err:
        result["failure_reason"] = err.reason
        log_run(result)
        fail(err.reason)

    while datetime.now(timezone.utc) < t0:
        time.sleep(0.2)

    last_scheduled = t0 + timedelta(milliseconds=interval_ms * (JOB_COUNT - 1))
    wait_all_succeeded(prefix, last_scheduled, label)

    jobs = sql_jobs_for_prefix(prefix)
    fields = [
        "job_id",
        "idempotency_key",
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

    bad = [r for r in jobs if r["status"] != "succeeded"]
    if bad:
        result["failure_reason"] = f"non-succeeded jobs: {len(bad)}"
        log_run(result)
        fail(f"{label}: {result['failure_reason']}")

    stats = lag_stats(jobs, interval_ms)
    result.update(stats)
    with (run_dir / "lag_by_hundred.csv").open("w", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "bucket_index",
                "job_index_from",
                "job_index_to_exclusive",
                "count",
                "start_lag_p50_seconds",
                "start_lag_p95_seconds",
            ],
        )
        w.writeheader()
        w.writerows(stats["by_hundred"])

    t0_dt = pl.parse_ts(result["t0_utc"])
    max_fin = max(pl.parse_ts(r["finished_at"]) for r in jobs)
    try:
        res_rows, mean_cpu, max_rss = pl.collect_resources(t0_dt, max_fin, WORKERS)
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
        result["failure_reason"] = result.get("resources_error") or "missing prometheus samples"
        log_run(result)
        fail(f"{label}: {result['failure_reason']}")

    result["status"] = "ok"
    result["mean_sum_worker_process_cpu_cores"] = mean_cpu
    result["max_observed_sum_worker_process_rss_mib"] = max_rss
    log_run(result)
    return result


def write_metadata() -> None:
    doc = {
        "git_sha": GIT_SHA,
        "verify_load_intensity_sha256": hashlib.sha256(SCRIPT_PATH.read_bytes()).hexdigest(),
        "reference_dir_campaign_mass_start": str(REF_DIR),
        "reference_note": "Iteration 9 campaign (single scheduled_at), workers=2 — not comparable 1:1",
        "expected_image_tag": EXPECTED_IMAGE_TAG,
        "worker_image": pl.kubectl("get", "deployment", "worker", "-o", "jsonpath={.spec.template.spec.containers[0].image}"),
        "mock_send_delay_run": MOCK_DELAY,
        "job_count": JOB_COUNT,
        "workers": WORKERS,
        "run_plan": RUN_PLAN,
        "acceptance_succeeded_total": 6000 if not TRIAL else None,
    }
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(doc, indent=2))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _, health = pl.api_request("GET", "/healthz")
    if health.get("status") != "ok":
        fail(f"healthz: {health}")
    pl.patch_mock(MOCK_DELAY, "0")
    if not TRIAL:
        pl.assert_stand_compat(EXPECTED_IMAGE_TAG)
    pl.scale_workers(WORKERS)
    pl.ensure_api_port_forward()
    pl.ensure_prom_port_forward()
    write_metadata()

    for round_n, rate in RUN_PLAN:
        try:
            run_one(round_n, rate)
        except SystemExit:
            raise

    runs_path = OUTPUT_DIR / "runs.jsonl"
    if not TRIAL and runs_path.is_file():
        ok_runs = [json.loads(ln) for ln in runs_path.read_text().splitlines() if ln.strip()]
        if sum(1 for r in ok_runs if r.get("status") == "ok") == 6:
            subprocess.check_call(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts/k8s/rebuild-load-intensity-summary.py"),
                    "--run-dir",
                    str(OUTPUT_DIR),
                ]
            )
            print("PASS: load intensity series", file=sys.stderr)
    elif TRIAL:
        print("PASS: load intensity trial", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    finally:
        pl.stop_owned_port_forwards()
