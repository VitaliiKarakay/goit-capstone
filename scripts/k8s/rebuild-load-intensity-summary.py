#!/usr/bin/env python3
"""Rebuild load-intensity summary offline."""

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--reference-dir", type=Path, default=None)
    args = ap.parse_args()

    runs = [r for r in load_runs(args.run_dir / "runs.jsonl") if r.get("status") == "ok"]
    if len(runs) != 6:
        raise SystemExit(f"expected 6 ok runs, got {len(runs)}")

    by_rate: dict[int, list[dict]] = {2: [], 5: []}
    for r in runs:
        by_rate[r["jobs_per_second"]].append(r)

    rows = []
    ref_dir = args.reference_dir
    if ref_dir is None and (args.run_dir / "metadata.json").is_file():
        ref_dir = Path(json.loads((args.run_dir / "metadata.json").read_text()).get("reference_dir_campaign_mass_start", ""))
    if ref_dir and Path(ref_dir).is_dir():
        ref_runs = load_runs(Path(ref_dir) / "runs.jsonl")
        ref2 = [r for r in ref_runs if r.get("workers") == 2 and r.get("status") == "ok"]
        if ref2:
            p95 = [r["start_lag_p95_seconds"] for r in ref2]
            rows.append(
                {
                    "scenario": "reference_campaign_mass_start_iter9",
                    "jobs_per_second": "campaign",
                    "runs_ok": len(ref2),
                    "source": "imported_iteration_9",
                    "median_start_lag_p95_seconds": statistics.median(p95),
                    "median_campaign_processing_seconds": statistics.median(
                        [r["campaign_processing_seconds"] for r in ref2]
                    ),
                }
            )

    summaries = []
    for rate in (2, 5):
        grp = by_rate[rate]
        if len(grp) != 3:
            raise SystemExit(f"expected 3 runs at {rate} j/s, got {len(grp)}")
        p95 = [r["start_lag_p95_seconds"] for r in grp]
        tail = [r["tail_after_last_scheduled_seconds"] for r in grp]
        m_p95, mn_p95, mx_p95 = med_range(p95)
        m_tail, mn_tail, mx_tail = med_range(tail)
        row = {
            "scenario": f"staggered_{rate}jps",
            "jobs_per_second": rate,
            "runs_ok": 3,
            "source": "iteration_11",
            "median_start_lag_p95_seconds": m_p95,
            "min_start_lag_p95_seconds": mn_p95,
            "max_start_lag_p95_seconds": mx_p95,
            "median_tail_after_last_scheduled_seconds": m_tail,
            "min_tail_after_last_scheduled_seconds": mn_tail,
            "max_tail_after_last_scheduled_seconds": mx_tail,
            "schedule_span_seconds": statistics.median([r["schedule_span_seconds"] for r in grp]),
        }
        summaries.append(row)
        rows.append(row)

    m2 = summaries[0]["median_start_lag_p95_seconds"]
    m5 = summaries[1]["median_start_lag_p95_seconds"]
    t2 = summaries[0]["median_tail_after_last_scheduled_seconds"]
    t5 = summaries[1]["median_tail_after_last_scheduled_seconds"]
    delta_row = {
        "scenario": "delta_5jps_minus_2jps",
        "jobs_per_second": "5-2",
        "runs_ok": 3,
        "source": "computed",
        "median_start_lag_p95_seconds": m5 - m2,
        "median_tail_after_last_scheduled_seconds": t5 - t2,
        "note": "positive delta means higher lag / longer tail at 5 jobs/s",
    }
    rows.append(delta_row)

    fieldnames: list[str] = []
    for row in rows:
        for k in row:
            if k not in fieldnames:
                fieldnames.append(k)

    out = args.run_dir / "summary.csv"
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    lag_all = args.run_dir / "lag_by_hundred_all_runs.csv"
    hundred_rows = []
    for r in runs:
        p = args.run_dir / r["label"] / "lag_by_hundred.csv"
        if not p.is_file():
            continue
        for line in csv.DictReader(p.open()):
            line["run_label"] = r["label"]
            line["jobs_per_second"] = str(r["jobs_per_second"])
            hundred_rows.append(line)
    if hundred_rows:
        fields = list(hundred_rows[0].keys())
        with lag_all.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(hundred_rows)

    print(json.dumps({"summary_csv": str(out), "lag_by_hundred_all": str(lag_all)}, indent=2))


if __name__ == "__main__":
    main()