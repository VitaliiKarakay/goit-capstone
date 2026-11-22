#!/usr/bin/env python3
"""Fixed-replica campaign performance series on scheduler-local (manual worker scale)."""

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
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = Path(__file__).resolve()
API = os.environ.get("API", "http://127.0.0.1:18081")
PROM_URL = os.environ.get("PROM_URL", "http://127.0.0.1:9090")
KUBE_CONTEXT = os.environ.get("KUBE_CONTEXT", "scheduler-local")
KUBE_NAMESPACE = os.environ.get("KUBE_NAMESPACE", "scheduler")
OUTPUT_DIR = Path(os.environ["PERF_OUTPUT_DIR"])
TRIAL = os.environ.get("PERF_TRIAL", "0") == "1"
GIT_SHA = os.environ.get("GIT_SHA", "unknown")

CAMPAIGN_SIZE = int(os.environ.get("PERF_CAMPAIGN_SIZE", "10" if TRIAL else "1000"))
LEAD_SECONDS = int(os.environ.get("PERF_LEAD_SECONDS", "30" if TRIAL else "90"))
MOCK_DELAY = os.environ.get("PERF_MOCK_SEND_DELAY", "50ms" if TRIAL else "500ms")
PROM_STEP = int(os.environ.get("PERF_PROM_STEP", "15"))
STABLE_TARGETS_SECONDS = int(os.environ.get("PERF_STABLE_TARGETS_SECONDS", "60"))

CPU_QUERY = 'rate(process_cpu_seconds_total{job="scheduler-workers"}[1m])'
RSS_QUERY = 'process_resident_memory_bytes{job="scheduler-workers"}'

RUN_PLAN = (
    [(1, 1, 1), (1, 2, 2), (1, 3, 4), (2, 1, 2), (2, 2, 4), (2, 3, 1), (3, 1, 4), (3, 2, 1), (3, 3, 2)]
    if not TRIAL
    else [(1, 1, 1)]
)


def fail(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def kubectl(*args: str) -> str:
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    return subprocess.check_output(
        [*base, f"--context={KUBE_CONTEXT}", "-n", KUBE_NAMESPACE, *args],
        text=True,
    ).strip()


def http_reachable(url: str, timeout: float = 5) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout):
            return True
    except Exception:
        return False


def api_request(method: str, path: str, body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    for _ in range(8):
        if not http_reachable(f"{API}/healthz"):
            time.sleep(1)
            continue
        try:
            with urllib.request.urlopen(req, timeout=120) as res:
                return res.status, json.loads(res.read() or b"{}")
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"API {method} {path} failed")


def pg_password() -> str:
    import base64

    raw = kubectl("get", "secret", "scheduler-secrets", "-o", "jsonpath={.data.POSTGRES_PASSWORD}")
    return base64.b64decode(raw).decode()


def psql_csv(sql: str) -> str:
    env = {**os.environ, "PGPASSWORD": pg_password()}
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    cmd = [
        *base,
        f"--context={KUBE_CONTEXT}",
        "-n",
        KUBE_NAMESPACE,
        "exec",
        "statefulset/postgres",
        "--",
        "psql",
        "-U",
        "scheduler",
        "-d",
        "scheduler",
        "-v",
        "ON_ERROR_STOP=1",
        "-A",
        "-F",
        ",",
        "-c",
        sql,
    ]
    return subprocess.check_output(cmd, text=True, env=env)


def foreign_jobs() -> list[tuple[str, str, str]]:
    sql = """
    SELECT id::text, status, COALESCE(campaign_id::text, '')
    FROM jobs
    WHERE status IN ('scheduled', 'queued', 'processing')
    ORDER BY scheduled_at, id
    LIMIT 25;
    """
    out = psql_csv(sql).strip()
    if not out:
        return []
    rows = []
    for line in out.splitlines():
        parts = line.split(",", 2)
        if len(parts) == 3:
            rows.append((parts[0], parts[1], parts[2]))
    return rows


def assert_quiet_stand(fail_prefix: str) -> None:
    rows = foreign_jobs()
    if rows:
        sample = "; ".join(f"{jid} status={st} campaign={cid or '-'}" for jid, st, cid in rows[:5])
        fail(f"{fail_prefix}: foreign jobs in scheduled/queued/processing ({len(rows)}+): {sample}")
    try:
        from urllib.parse import urlencode
        root = REPO_ROOT
        sys.path.insert(0, str(root / "scripts"))
        from demo_preflight import assert_quiet_standalone_jobs

        def req(method, path):
            return api_request(method, path)

        assert_quiet_standalone_jobs(API, req, lambda m: fail(f"{fail_prefix}: {m}"))
    except SystemExit:
        raise
    except Exception as err:
        fail(f"{fail_prefix}: standalone preflight error: {err}")


def ready_workers() -> int:
    return int(kubectl("get", "deploy", "worker", "-o", "jsonpath={.status.readyReplicas}") or "0")


def worker_targets_up() -> int:
    url = f"{PROM_URL}/api/v1/targets"
    if not http_reachable(f"{PROM_URL}/-/ready"):
        return 0
    with urllib.request.urlopen(url, timeout=15) as res:
        body = json.loads(res.read())
    pods: set[str] = set()
    for t in body.get("data", {}).get("activeTargets", []):
        labels = t.get("labels") or {}
        if labels.get("job") != "scheduler-workers" or t.get("health") != "up":
            continue
        pod = labels.get("pod", "")
        if pod and pod not in pods:
            pods.add(pod)
    return len(pods)


def scale_workers(n: int) -> None:
    subprocess.check_call(
        [str(REPO_ROOT / "scripts/k8s/scale-workers.sh"), str(n)],
        cwd=REPO_ROOT,
    )


def patch_mock(delay: str, failure_rate: str = "0") -> None:
    kubectl(
        "patch",
        "configmap",
        "scheduler-app",
        "--type",
        "merge",
        "-p",
        json.dumps({"data": {"MOCK_SEND_DELAY": delay, "MOCK_FAILURE_RATE": failure_rate}}),
    )
    for dep in ("worker", "controller"):
        subprocess.check_call(
            [
                *( [str(REPO_ROOT / "scripts/k8s/bin/kubectl")] if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file() else ["kubectl"] ),
                f"--context={KUBE_CONTEXT}",
                "-n",
                KUBE_NAMESPACE,
                "rollout",
                "restart",
                f"deployment/{dep}",
            ]
        )
    subprocess.check_call(
        [
            *( [str(REPO_ROOT / "scripts/k8s/bin/kubectl")] if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file() else ["kubectl"] ),
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
            "rollout",
            "status",
            "deployment/worker",
            "--timeout=300s",
        ]
    )


def wait_ready_and_targets(n: int, stable_s: int, label: str) -> None:
    deadline = time.time() + 600
    stable_start: float | None = None
    while time.time() < deadline:
        ready = ready_workers()
        targets = worker_targets_up()
        if ready == n and targets == n:
            if stable_start is None:
                stable_start = time.time()
            elif time.time() - stable_start >= stable_s:
                return
        else:
            stable_start = None
        time.sleep(2)
    fail(f"{label}: timed out waiting for {n} ready workers and {n} UP prometheus targets ({stable_s}s stable)")


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def build_campaign(scheduled: datetime) -> dict:
    key = f"perf-{scheduled.strftime('%Y%m%d%H%M%S%f')}"
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


def percentile_nearest_rank(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        raise ValueError("empty")
    n = len(sorted_vals)
    k = math.ceil((p / 100.0) * n)
    k = max(1, min(k, n))
    return sorted_vals[k - 1]


def prom_query_range_matrix(query: str, start: float, end: float, step: int) -> list[dict]:
    params = urllib.parse.urlencode(
        {"query": query, "start": f"{start:.3f}", "end": f"{end:.3f}", "step": str(step)}
    )
    url = f"{PROM_URL}/api/v1/query_range?{params}"
    with urllib.request.urlopen(url, timeout=120) as res:
        body = json.loads(res.read())
    if body.get("status") != "success":
        raise RuntimeError(f"prometheus range: {body}")
    return body.get("data", {}).get("result") or []


def series_by_timestamp(matrix: list[dict]) -> dict[float, dict[str, float]]:
    out: dict[float, dict[str, float]] = {}
    for series in matrix:
        inst = series.get("metric", {}).get("instance") or series.get("metric", {}).get("pod") or "unknown"
        for ts_s, val_s in series.get("values") or []:
            ts = float(ts_s)
            try:
                val = float(val_s)
            except (TypeError, ValueError):
                continue
            if math.isnan(val):
                continue
            out.setdefault(ts, {})[inst] = val
    return out


def collect_resources(start: datetime, end: datetime, n_workers: int) -> tuple[list[dict], float | None, float | None]:
    t0 = start.timestamp()
    t1 = end.timestamp()
    if t1 <= t0:
        t1 = t0 + PROM_STEP
    cpu_m = prom_query_range_matrix(CPU_QUERY, t0, t1, PROM_STEP)
    rss_m = prom_query_range_matrix(RSS_QUERY, t0, t1, PROM_STEP)
    cpu_by_ts = series_by_timestamp(cpu_m)
    rss_by_ts = series_by_timestamp(rss_m)
    all_ts = sorted(set(cpu_by_ts) | set(rss_by_ts))
    rows: list[dict] = []
    cpu_sums: list[float] = []
    rss_peaks_mib: list[float] = []
    for ts in all_ts:
        cpu_inst = cpu_by_ts.get(ts, {})
        rss_inst = rss_by_ts.get(ts, {})
        n_cpu = len(cpu_inst)
        n_rss = len(rss_inst)
        row = {
            "timestamp_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "worker_instances_cpu": str(n_cpu),
            "worker_instances_rss": str(n_rss),
            "sum_worker_process_cpu_cores": "",
            "sum_worker_process_rss_mib": "",
        }
        if n_cpu == n_workers and n_rss == n_workers:
            cpu_sum = sum(cpu_inst.values())
            rss_sum = sum(rss_inst.values())
            row["sum_worker_process_cpu_cores"] = f"{cpu_sum:.6f}"
            row["sum_worker_process_rss_mib"] = f"{rss_sum / 1048576.0:.3f}"
            cpu_sums.append(cpu_sum)
            rss_peaks_mib.append(rss_sum / 1048576.0)
        rows.append(row)
    mean_cpu = statistics.mean(cpu_sums) if cpu_sums else None
    max_rss_mib = max(rss_peaks_mib) if rss_peaks_mib else None
    return rows, mean_cpu, max_rss_mib


def fetch_jobs_sql(campaign_id: str) -> list[dict]:
    sql = f"""
    COPY (
      SELECT id::text AS job_id, status, scheduled_at::text, send_started_at::text, finished_at::text
      FROM jobs WHERE campaign_id = '{campaign_id}'::uuid
      ORDER BY recipient_id, id
    ) TO STDOUT WITH (FORMAT csv, HEADER true);
    """
    env = {**os.environ, "PGPASSWORD": pg_password()}
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    cmd = [
        *base,
        f"--context={KUBE_CONTEXT}",
        "-n",
        KUBE_NAMESPACE,
        "exec",
        "statefulset/postgres",
        "--",
        "psql",
        "-U",
        "scheduler",
        "-d",
        "scheduler",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    raw = subprocess.check_output(cmd, text=True, env=env)
    reader = csv.DictReader(raw.splitlines())
    return list(reader)


def minikube_resources() -> dict:
    try:
        prof = subprocess.check_output(
            ["minikube", "-p", "scheduler-local", "profile", "list", "-o", "json"],
            text=True,
        )
        data = json.loads(prof)
        return data.get("Valid", data) if isinstance(data, dict) else {"raw": data}
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as err:
        return {"error": str(err)}


def script_digest() -> str:
    h = hashlib.sha256()
    h.update(SCRIPT_PATH.read_bytes())
    return h.hexdigest()


def write_metadata(extra: dict | None = None) -> None:
    tag = ""
    tag_file = REPO_ROOT / "scripts/k8s/.image-tag"
    if tag_file.is_file():
        tag = tag_file.read_text().strip()
    doc = {
        "git_sha": GIT_SHA,
        "image_tag_file": tag,
        "verify_perf_workers_sha256": script_digest(),
        "mock_send_delay_run": MOCK_DELAY,
        "mock_failure_rate_run": "0",
        "campaign_size": CAMPAIGN_SIZE,
        "lead_seconds": LEAD_SECONDS,
        "prometheus_step_seconds": PROM_STEP,
        "stable_targets_before_campaign_seconds": STABLE_TARGETS_SECONDS,
        "kube_context": KUBE_CONTEXT,
        "namespace": KUBE_NAMESPACE,
        "controller_image": kubectl("get", "deployment", "controller", "-o", "jsonpath={.spec.template.spec.containers[0].image}"),
        "worker_image": kubectl("get", "deployment", "worker", "-o", "jsonpath={.spec.template.spec.containers[0].image}"),
        "minikube_profile": minikube_resources(),
        **(extra or {}),
    }
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(doc, indent=2))


def wait_campaign_done(campaign_id: str, deadline_s: float = 3600) -> dict:
    end = time.time() + deadline_s
    while time.time() < end:
        _, summary = api_request("GET", f"/campaigns/{campaign_id}")
        counts = summary.get("counts") or {}
        if counts.get("succeeded") == CAMPAIGN_SIZE:
            bad = [k for k, v in counts.items() if k != "succeeded" and v not in (0, None)]
            if not bad:
                return summary
        time.sleep(5)
    fail(f"campaign {campaign_id}: timed out waiting for {CAMPAIGN_SIZE} succeeded")


def validate_and_metrics(
    jobs: list[dict],
    campaign_scheduled: datetime,
) -> tuple[dict, str | None]:
    if len(jobs) != CAMPAIGN_SIZE:
        return {}, f"expected {CAMPAIGN_SIZE} jobs, got {len(jobs)}"
    sched_set: set[str] = set()
    lags: list[float] = []
    finished: list[datetime] = []
    for row in jobs:
        if row.get("status") != "succeeded":
            return {}, f"job {row.get('job_id')} status={row.get('status')}"
        for col in ("scheduled_at", "send_started_at", "finished_at"):
            if not row.get(col):
                return {}, f"job {row.get('job_id')}: missing {col}"
        sa = parse_ts(row["scheduled_at"])
        ss = parse_ts(row["send_started_at"])
        fi = parse_ts(row["finished_at"])
        sched_set.add(row["scheduled_at"])
        if not (sa <= ss <= fi):
            return {}, f"job {row.get('job_id')}: scheduled_at <= send_started_at <= finished_at violated"
        lags.append((ss - sa).total_seconds())
        finished.append(fi)
    if len(sched_set) != 1:
        return {}, f"jobs have {len(sched_set)} distinct scheduled_at values"
    only_sched = parse_ts(next(iter(sched_set)))
    if abs((only_sched - campaign_scheduled).total_seconds()) > 1.0:
        return {}, "job scheduled_at differs from campaign scheduled_at"
    max_finished = max(finished)
    proc_s = (max_finished - campaign_scheduled).total_seconds()
    if proc_s <= 0:
        return {}, "non-positive campaign processing duration"
    lags.sort()
    metrics = {
        "campaign_processing_seconds": proc_s,
        "throughput_jobs_per_second": CAMPAIGN_SIZE / proc_s,
        "start_lag_p50_seconds": percentile_nearest_rank(lags, 50),
        "start_lag_p95_seconds": percentile_nearest_rank(lags, 95),
        "percentile_method": "nearest_rank",
        "max_finished_at_utc": max_finished.isoformat(),
    }
    return metrics, None


def log_run(result: dict) -> None:
    run_dir = OUTPUT_DIR / result["label"]
    (run_dir / "run.json").write_text(json.dumps(result, indent=2))
    with (OUTPUT_DIR / "runs.jsonl").open("a") as fh:
        fh.write(json.dumps(result) + "\n")


def run_one(round_n: int, order_n: int, workers: int) -> dict:
    label = f"round{round_n}-order{order_n}-workers{workers}"
    prefix = f"{label}"
    run_dir = OUTPUT_DIR / label
    run_dir.mkdir(parents=True, exist_ok=True)
    result: dict = {"label": label, "round": round_n, "order": order_n, "workers": workers, "status": "failed"}

    assert_quiet_stand(prefix)
    scale_workers(workers)
    wait_ready_and_targets(workers, STABLE_TARGETS_SECONDS, prefix)

    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=LEAD_SECONDS)
    body = build_campaign(scheduled_at)
    status, created = api_request("POST", "/campaigns", body)
    if status != 201:
        result["failure_reason"] = f"create campaign: {status} {created}"
        log_run(result)
        fail(result["failure_reason"])
    campaign_id = created["campaign_id"]
    result["campaign_id"] = campaign_id
    campaign_scheduled = parse_ts(created.get("scheduled_at") or body["scheduled_at"])

    while datetime.now(timezone.utc) < campaign_scheduled:
        time.sleep(0.5)

    summary = wait_campaign_done(campaign_id)
    jobs = fetch_jobs_sql(campaign_id)
    with (run_dir / "jobs.csv").open("w", newline="") as fh:
        if jobs:
            w = csv.DictWriter(fh, fieldnames=["job_id", "status", "scheduled_at", "send_started_at", "finished_at"])
            w.writeheader()
            w.writerows(jobs)

    job_metrics, job_err = validate_and_metrics(jobs, campaign_scheduled)
    if job_err:
        result["failure_reason"] = job_err
        result["counts"] = summary.get("counts")
        log_run(result)
        fail(f"{prefix}: {job_err}")

    result.update(job_metrics)
    max_fin = parse_ts(job_metrics["max_finished_at_utc"])
    try:
        res_rows, mean_cpu, max_rss = collect_resources(campaign_scheduled, max_fin, workers)
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

    partial = mean_cpu is None or max_rss is None
    if partial:
        result["status"] = "incomplete"
        result["failure_reason"] = result.get("resources_error") or "missing worker process resource samples (full N coverage)"
        result["mean_sum_worker_process_cpu_cores"] = mean_cpu
        result["max_observed_sum_worker_process_rss_mib"] = max_rss
        log_run(result)
        fail(f"{prefix}: {result['failure_reason']}")

    result["status"] = "ok"
    result["mean_sum_worker_process_cpu_cores"] = mean_cpu
    result["max_observed_sum_worker_process_rss_mib"] = max_rss
    result["counts"] = summary.get("counts")
    log_run(result)
    return result


def write_summary(ok_runs: list[dict]) -> None:
    by_w: dict[int, list[dict]] = {1: [], 2: [], 4: []}
    for r in ok_runs:
        by_w[r["workers"]].append(r)

    def med_range(vals: list[float]) -> tuple[float, float, float]:
        if not vals:
            return (float("nan"),) * 3
        vals = sorted(vals)
        return statistics.median(vals), vals[0], vals[-1]

    rows = []
    baseline = statistics.median([r["campaign_processing_seconds"] for r in by_w[1]]) if by_w[1] else None
    for w in (1, 2, 4):
        grp = by_w[w]
        proc = [r["campaign_processing_seconds"] for r in grp]
        tp = [r["throughput_jobs_per_second"] for r in grp]
        p50 = [r["start_lag_p50_seconds"] for r in grp]
        p95 = [r["start_lag_p95_seconds"] for r in grp]
        cpu = [r["mean_sum_worker_process_cpu_cores"] for r in grp]
        rss = [r["max_observed_sum_worker_process_rss_mib"] for r in grp]
        m_proc, mn_proc, mx_proc = med_range(proc)
        row = {
            "workers": w,
            "runs_ok": len(grp),
            "median_campaign_processing_seconds": m_proc,
            "min_campaign_processing_seconds": mn_proc,
            "max_campaign_processing_seconds": mx_proc,
            "median_throughput_jobs_per_second": med_range(tp)[0],
            "median_start_lag_p50_seconds": med_range(p50)[0],
            "median_start_lag_p95_seconds": med_range(p95)[0],
            "median_mean_sum_worker_process_cpu_cores": med_range(cpu)[0],
            "median_max_observed_sum_worker_process_rss_mib": med_range(rss)[0],
        }
        if baseline and w > 1 and baseline > 0:
            row["speedup_vs_1_worker_median_time"] = baseline / m_proc
        rows.append(row)

    path = OUTPUT_DIR / "summary.csv"
    if rows:
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    print(json.dumps({"summary_csv": str(path), "rows": rows}, indent=2))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _, health = api_request("GET", "/healthz")
    if health.get("status") != "ok":
        fail(f"healthz: {health}")
    try:
        kubectl("get", "scaledobject", "worker")
        fail("ScaledObject/worker present — manual fixed-replica perf requires autoscale disabled")
    except subprocess.CalledProcessError:
        pass

    patch_mock(MOCK_DELAY, "0")
    write_metadata()

    ok_runs: list[dict] = []
    for round_n, order_n, workers in RUN_PLAN:
        print(f"RUN {round_n}/{order_n} workers={workers}", flush=True)
        try:
            ok_runs.append(run_one(round_n, order_n, workers))
        except SystemExit as exc:
            if exc.code not in (None, 0):
                if ok_runs:
                    write_summary(ok_runs)
                raise
            raise

    if not TRIAL and len(ok_runs) == 9:
        write_summary(ok_runs)
        print("PASS: worker performance series", file=sys.stderr)
    elif TRIAL:
        print("PASS: trial", file=sys.stderr)


if __name__ == "__main__":
    main()