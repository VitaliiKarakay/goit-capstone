"""Shared helpers for scheduler-local performance / cancel-impact scripts."""

from __future__ import annotations

import csv
import json
import math
import os
import statistics
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
API = os.environ.get("API", "http://127.0.0.1:18081")
PROM_URL = os.environ.get("PROM_URL", "http://127.0.0.1:9090")
KUBE_CONTEXT = os.environ.get("KUBE_CONTEXT", "scheduler-local")
KUBE_NAMESPACE = os.environ.get("KUBE_NAMESPACE", "scheduler")
PROM_STEP = int(os.environ.get("PERF_PROM_STEP", "15"))
STABLE_TARGETS_SECONDS = int(os.environ.get("PERF_STABLE_TARGETS_SECONDS", "60"))
CPU_QUERY = 'rate(process_cpu_seconds_total{job="scheduler-workers"}[1m])'
RSS_QUERY = 'process_resident_memory_bytes{job="scheduler-workers"}'

_PF_API_PROC: subprocess.Popen | None = None
_PF_PROM_PROC: subprocess.Popen | None = None


def fail(msg: str, code: int = 1) -> None:
    import sys

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


def ensure_api_port_forward() -> None:
    global _PF_API_PROC
    if http_reachable(f"{API}/healthz"):
        return
    if _PF_API_PROC and _PF_API_PROC.poll() is not None:
        _PF_API_PROC = None
    port = urllib.parse.urlparse(API).port or 18081
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    _PF_API_PROC = subprocess.Popen(
        [
            *base,
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
            "port-forward",
            "--address",
            "127.0.0.1",
            "svc/controller",
            f"{port}:8080",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        if http_reachable(f"{API}/healthz"):
            return
        time.sleep(1)
    fail(f"API unreachable at {API}")


def ensure_prom_port_forward() -> None:
    global _PF_PROM_PROC
    if http_reachable(f"{PROM_URL}/-/ready"):
        return
    if _PF_PROM_PROC and _PF_PROM_PROC.poll() is not None:
        _PF_PROM_PROC = None
    port = urllib.parse.urlparse(PROM_URL).port or 9090
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    _PF_PROM_PROC = subprocess.Popen(
        [
            *base,
            f"--context={KUBE_CONTEXT}",
            "-n",
            KUBE_NAMESPACE,
            "port-forward",
            "--address",
            "127.0.0.1",
            "svc/prometheus",
            f"{port}:9090",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        if http_reachable(f"{PROM_URL}/-/ready"):
            return
        time.sleep(1)
    fail(f"Prometheus unreachable at {PROM_URL}")


def stop_owned_port_forwards() -> None:
    global _PF_API_PROC, _PF_PROM_PROC
    for proc in (_PF_API_PROC, _PF_PROM_PROC):
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    _PF_API_PROC = _PF_PROM_PROC = None


def api_request(method: str, path: str, body: dict | None = None):
    last_err: Exception | None = None
    for _ in range(12):
        ensure_api_port_forward()
        payload = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            API + path,
            data=payload,
            method=method,
            headers={"Content-Type": "application/json"} if body is not None else {},
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as res:
                return res.status, json.loads(res.read() or b"{}")
        except Exception as err:
            last_err = err
            time.sleep(2)
    raise RuntimeError(f"API {method} {path} failed: {last_err}")


def pg_password() -> str:
    import base64

    raw = kubectl("get", "secret", "scheduler-secrets", "-o", "jsonpath={.data.POSTGRES_PASSWORD}")
    return base64.b64decode(raw).decode()


def psql_raw(sql: str) -> str:
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
    return subprocess.check_output(cmd, text=True, env=env)


def foreign_jobs() -> list[tuple[str, str, str]]:
    sql = """
    SELECT id::text, status, COALESCE(campaign_id::text, '')
    FROM jobs
    WHERE status IN ('scheduled', 'queued', 'processing')
    ORDER BY scheduled_at, id
    LIMIT 25;
    """
    cmd = [
        *( [str(REPO_ROOT / "scripts/k8s/bin/kubectl")] if (REPO_ROOT / "scripts/k8s/bin/kubectl").is_file() else ["kubectl"] ),
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
        "-t",
        "-A",
        "-F",
        ",",
        "-c",
        sql,
    ]
    env = {**os.environ, "PGPASSWORD": pg_password()}
    out = subprocess.check_output(cmd, text=True, env=env).strip()
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
        extra = f" (+{len(rows) - 5} more)" if len(rows) > 5 else ""
        fail(f"{fail_prefix}: foreign jobs in scheduled/queued/processing: {sample}{extra}")


def ready_workers() -> int:
    return int(kubectl("get", "deploy", "worker", "-o", "jsonpath={.status.readyReplicas}") or "0")


def worker_targets_up() -> int:
    ensure_prom_port_forward()
    with urllib.request.urlopen(f"{PROM_URL}/api/v1/targets", timeout=15) as res:
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
    k = REPO_ROOT / "scripts/k8s/bin/kubectl"
    base = [str(k)] if k.is_file() else ["kubectl"]
    for dep in ("worker", "controller"):
        subprocess.check_call(
            [*base, f"--context={KUBE_CONTEXT}", "-n", KUBE_NAMESPACE, "rollout", "restart", f"deployment/{dep}"]
        )
    subprocess.check_call(
        [*base, f"--context={KUBE_CONTEXT}", "-n", KUBE_NAMESPACE, "rollout", "status", "deployment/worker", "--timeout=300s"]
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
    fail(f"{label}: timed out waiting for {n} ready workers and {n} UP targets ({stable_s}s stable)")


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def percentile_nearest_rank(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        raise ValueError("empty")
    n = len(sorted_vals)
    k = math.ceil((p / 100.0) * n)
    k = max(1, min(k, n))
    return sorted_vals[k - 1]


def prom_query_range_matrix(query: str, start: float, end: float, step: int) -> list[dict]:
    ensure_prom_port_forward()
    params = urllib.parse.urlencode(
        {"query": query, "start": f"{start:.3f}", "end": f"{end:.3f}", "step": str(step)}
    )
    with urllib.request.urlopen(f"{PROM_URL}/api/v1/query_range?{params}", timeout=120) as res:
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
    cpu_by_ts = series_by_timestamp(prom_query_range_matrix(CPU_QUERY, t0, t1, PROM_STEP))
    rss_by_ts = series_by_timestamp(prom_query_range_matrix(RSS_QUERY, t0, t1, PROM_STEP))
    all_ts = sorted(set(cpu_by_ts) | set(rss_by_ts))
    rows: list[dict] = []
    cpu_sums: list[float] = []
    rss_peaks_mib: list[float] = []
    for ts in all_ts:
        cpu_inst = cpu_by_ts.get(ts, {})
        rss_inst = rss_by_ts.get(ts, {})
        n_cpu, n_rss = len(cpu_inst), len(rss_inst)
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
    return rows, (statistics.mean(cpu_sums) if cpu_sums else None), (max(rss_peaks_mib) if rss_peaks_mib else None)


def fetch_jobs_sql(campaign_id: str) -> list[dict]:
    sql = f"""
    COPY (
      SELECT id::text AS job_id, recipient_id, status,
             scheduled_at::text, send_started_at::text, finished_at::text, canceled_at::text
      FROM jobs WHERE campaign_id = '{campaign_id}'::uuid
      ORDER BY recipient_id, id
    ) TO STDOUT WITH (FORMAT csv, HEADER true);
    """
    raw = psql_raw(sql)
    return list(csv.DictReader(raw.splitlines()))


def assert_stand_compat(expected_image_tag: str) -> None:
    w_img = kubectl("get", "deployment", "worker", "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    c_img = kubectl("get", "deployment", "controller", "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    mock = kubectl("get", "configmap", "scheduler-app", "-o", "jsonpath={.data.MOCK_SEND_DELAY}")
    replicas = int(kubectl("get", "deploy", "worker", "-o", "jsonpath={.spec.replicas}") or "0")
    if expected_image_tag not in w_img or expected_image_tag not in c_img:
        fail(f"stand: expected image tag {expected_image_tag}, got worker={w_img} controller={c_img}")
    if mock != "500ms":
        fail(f"stand: expected MOCK_SEND_DELAY=500ms, got {mock}")
    if replicas != 2:
        fail(f"stand: expected worker replicas=2, got {replicas}")
    try:
        kubectl("get", "scaledobject", "worker")
        fail("stand: ScaledObject present — disable autoscaling")
    except subprocess.CalledProcessError:
        pass