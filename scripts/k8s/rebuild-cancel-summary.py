#!/usr/bin/env python3
"""Rebuild cancel-impact summary.csv offline (no Kubernetes)."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path


def med_range(vals: list[float]) -> tuple[float, float, float]:
    if not vals:
        return (float("nan"),) * 3
    vals = sorted(vals)
    return statistics.median(vals), vals[0], vals[-1]


def load_runs(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_baseline_workers2(baseline_dir: Path) -> list[dict]:
    runs = load_runs(baseline_dir / "runs.jsonl")
    out = []
    for r in runs:
        if r.get("workers") == 2 and r.get("status") == "ok" and r.get("counts", {}).get("canceled", 0) == 0:
            r = dict(r)
            r["cancel_pct"] = 0
            r["imported_baseline"] = True
            r["source"] = "iteration_9"
            out.append(r)
    if len(out) != 3:
        raise SystemExit(f"expected 3 baseline worker=2 runs in {baseline_dir}, got {len(out)}")
    return out


def summarize_group(rows: list[dict], scenario: str, pct: int, source: str) -> dict:
    p95 = [r["start_lag_p95_seconds"] for r in rows]
    p50 = [r["start_lag_p50_seconds"] for r in rows]
    proc = [r["campaign_processing_seconds"] for r in rows]
    tp = [r["throughput_jobs_per_second"] for r in rows]
    m_p95, mn_p95, mx_p95 = med_range(p95)
    m_proc, mn_proc, mx_proc = med_range(proc)
    return {
        "scenario": scenario,
        "cancel_pct": pct,
        "runs_ok": len(rows),
        "source": source,
        "median_start_lag_p95_seconds": m_p95,
        "min_start_lag_p95_seconds": mn_p95,
        "max_start_lag_p95_seconds": mx_p95,
        "median_start_lag_p50_seconds": med_range(p50)[0],
        "median_campaign_processing_seconds": m_proc,
        "min_campaign_processing_seconds": mn_proc,
        "max_campaign_processing_seconds": mx_proc,
        "median_throughput_jobs_per_second": med_range(tp)[0],
    }


def delta_row(base: float, val: float) -> tuple[float, float]:
    d = val - base
    pct = (d / base * 100.0) if base else float("nan")
    return d, pct


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--baseline-dir", type=Path, default=None)
    args = ap.parse_args()
    meta_path = args.run_dir / "metadata.json"
    baseline_dir = args.baseline_dir
    if baseline_dir is None and meta_path.is_file():
        meta = json.loads(meta_path.read_text())
        baseline_dir = Path(meta.get("perf_baseline_dir", ""))
    if baseline_dir is None or not baseline_dir.is_dir():
        raise SystemExit("baseline dir required")

    baseline = load_baseline_workers2(baseline_dir)
    new_runs = [r for r in load_runs(args.run_dir / "runs.jsonl") if r.get("status") == "ok"]
    if len(new_runs) != 6:
        raise SystemExit(f"expected 6 ok runs in {args.run_dir}, got {len(new_runs)}")

    by_pct: dict[int, list[dict]] = {25: [], 50: []}
    for r in new_runs:
        by_pct[r["cancel_pct"]].append(r)
    for pct in (25, 50):
        if len(by_pct[pct]) != 3:
            raise SystemExit(f"expected 3 runs at {pct}% cancel, got {len(by_pct[pct])}")

    rows = []
    rows.append(summarize_group(baseline, "baseline_0pct", 0, "imported_iteration_9"))
    rows.append(summarize_group(by_pct[25], "cancel_25pct", 25, "iteration_10"))
    rows.append(summarize_group(by_pct[50], "cancel_50pct", 50, "iteration_10"))

    base_p95 = rows[0]["median_start_lag_p95_seconds"]
    base_proc = rows[0]["median_campaign_processing_seconds"]
    for row in rows[1:]:
        d_p95, dp_p95 = delta_row(base_p95, row["median_start_lag_p95_seconds"])
        d_proc, dp_proc = delta_row(base_proc, row["median_campaign_processing_seconds"])
        row["delta_p95_seconds_vs_baseline"] = d_p95
        row["delta_p95_percent_vs_baseline"] = dp_p95
        row["delta_campaign_seconds_vs_baseline"] = d_proc
        row["delta_campaign_percent_vs_baseline"] = dp_proc

    rows[0]["delta_p95_seconds_vs_baseline"] = 0.0
    rows[0]["delta_p95_percent_vs_baseline"] = 0.0
    rows[0]["delta_campaign_seconds_vs_baseline"] = 0.0
    rows[0]["delta_campaign_percent_vs_baseline"] = 0.0

    fieldnames: list[str] = []
    for row in rows:
        for k in row:
            if k not in fieldnames:
                fieldnames.append(k)

    out = args.run_dir / "summary.csv"
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(json.dumps({"summary_csv": str(out), "rows": rows}, indent=2))


if __name__ == "__main__":
    main()