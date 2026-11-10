"""
Command-line interface for `unherd`.
Usage:
  python3 -m unherd analyze examples/production_fleet.crontab
  python3 -m unherd optimize examples/production_fleet.crontab --diff
  python3 -m unherd simulate examples/production_fleet.crontab
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
from typing import List

from .engine import analyze_fleet, generate_unified_diff, optimize_fleet, simulate_retry_storm
from .parser import JobSpec, parse_fleet_config

SPARK_CHARS = " ▁▂▃▄▅▆▇█"


def _render_sparkline(values: List[float], width: int = 60) -> str:
    if not values:
        return ""
    step = max(1, len(values) // width)
    buckets = [
        max(values[i : i + step]) for i in range(0, min(len(values), width * step), step)
    ]
    hi = max(buckets) if buckets else 0.0
    if hi <= 0:
        return "▁" * len(buckets)
    out = []
    for val in buckets:
        idx = int(round((val / hi) * (len(SPARK_CHARS) - 1)))
        out.append(SPARK_CHARS[max(0, min(len(SPARK_CHARS) - 1, idx))])
    return "".join(out)


def _print_analysis_table(title: str, jobs: List[JobSpec]) -> None:
    report = analyze_fleet(jobs)
    print(f"=== {title} ===")
    print(
        f"Jobs: {report.total_jobs}  |  Daily Executions: {report.total_daily_runs}  |  "
        f"Peak Concurrency: {report.peak_concurrency} jobs  |  Peak Weighted Load: {report.peak_weighted_load}"
    )
    print(
        f"Peak-to-Avg Ratio (PAR): {report.peak_to_average_ratio}x  |  "
        f"Collision Minutes (>=2): {report.collision_minutes_count}/1440  |  "
        f"Minute :00 Pileup: {report.minute_zero_share_pct}%"
    )
    # Show first-hour 60-minute sparkline (00:00..00:59)
    hour0_spark = _render_sparkline(report.minute_weighted_load[:60], width=60)
    print(f"00:00..00:59 Temporal Wave : [{hour0_spark}]")
    if report.hotspots:
        print("Top Collision Hotspots:")
        for hs in report.hotspots[:5]:
            joined = ", ".join(hs.job_names[:5])
            if len(hs.job_names) > 5:
                joined += f" (+{len(hs.job_names) - 5} more)"
            print(
                f"  [{hs.time_label} UTC]  {hs.concurrent_jobs:2d} concurrent  "
                f"(load {hs.weighted_load:5.2f}) -> {joined}"
            )
    print()


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="unherd",
        description="Fleet Cron Collision Analyzer, Thundering-Herd De-synchronizer & Retry-Storm Simulator",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_analyze = sub.add_parser("analyze", help="Analyze temporal collision density of a schedule file")
    p_analyze.add_argument("path", help="Path to crontab, K8s CronJob YAML, or GitHub Actions YAML")
    p_analyze.add_argument("--json", action="store_true", help="Emit JSON report")

    p_opt = sub.add_parser("optimize", help="De-synchronize schedules via deterministic phase shifting")
    p_opt.add_argument("path", help="Path to schedule file")
    p_opt.add_argument("--allow-hour-shift", action="store_true", help="Allow shifting daily/multi-hour windows")
    p_opt.add_argument("--diff", action="store_true", help="Print unified diff patch")
    p_opt.add_argument("--json", action="store_true", help="Emit JSON optimization summary")

    p_sim = sub.add_parser("simulate", help="Simulate downstream outage retry storm at peak collision minute")
    p_sim.add_argument("path", help="Path to schedule file")

    args = parser.parse_args(argv)
    target = Path(args.path)
    if not target.exists():
        print(f"error: file not found: {target}", file=sys.stderr)
        return 1

    raw = target.read_text(encoding="utf-8")
    jobs = parse_fleet_config(raw)
    if not jobs:
        print(f"error: no valid schedules parsed from {target}", file=sys.stderr)
        return 1

    if args.command == "analyze":
        rep = analyze_fleet(jobs)
        if args.json:
            print(
                json.dumps(
                    {
                        "total_jobs": rep.total_jobs,
                        "total_daily_runs": rep.total_daily_runs,
                        "peak_concurrency": rep.peak_concurrency,
                        "peak_weighted_load": rep.peak_weighted_load,
                        "peak_to_average_ratio": rep.peak_to_average_ratio,
                        "collision_minutes_count": rep.collision_minutes_count,
                        "severe_minutes_count": rep.severe_minutes_count,
                        "minute_zero_share_pct": rep.minute_zero_share_pct,
                        "gini_coefficient": rep.gini_coefficient,
                        "hotspots": [hs.__dict__ for hs in rep.hotspots[:8]],
                    },
                    indent=2,
                )
            )
        else:
            _print_analysis_table(f"UNHERD TEMPORAL AUDIT // {target.name}", jobs)
        return 0

    if args.command == "optimize":
        res = optimize_fleet(jobs, allow_hour_shift=args.allow_hour_shift)
        if args.json:
            print(
                json.dumps(
                    {
                        "peak_concurrency_before": res.before_analysis.peak_concurrency,
                        "peak_concurrency_after": res.after_analysis.peak_concurrency,
                        "peak_concurrency_reduction_pct": res.peak_concurrency_reduction_pct,
                        "peak_load_before": res.before_analysis.peak_weighted_load,
                        "peak_load_after": res.after_analysis.peak_weighted_load,
                        "peak_load_reduction_pct": res.peak_load_reduction_pct,
                        "mutations": [m.__dict__ for m in res.mutations],
                    },
                    indent=2,
                )
            )
            return 0

        _print_analysis_table("BASELINE SCHEDULE (BEFORE UNHERD)", res.original_jobs)
        _print_analysis_table("PHASE-SHIFTED SCHEDULE (AFTER UNHERD)", res.optimized_jobs)
        print(
            f"Summary: Peak concurrency reduced by {res.peak_concurrency_reduction_pct}% "
            f"({res.before_analysis.peak_concurrency} -> {res.after_analysis.peak_concurrency} jobs), "
            f"Peak weighted load reduced by {res.peak_load_reduction_pct}% "
            f"({res.before_analysis.peak_weighted_load} -> {res.after_analysis.peak_weighted_load})."
        )
        print(f"Total daily executions preserved invariant: {res.before_analysis.total_daily_runs} == {res.after_analysis.total_daily_runs}\n")
        if args.diff:
            print(generate_unified_diff(raw, res, filename=target.name))
        return 0

    if args.command == "simulate":
        storm_before = simulate_retry_storm(jobs)
        opt = optimize_fleet(jobs, allow_hour_shift=True)
        storm_after = simulate_retry_storm(opt.optimized_jobs)
        print(f"=== RETRY STORM CASCADE SIMULATION @ {storm_before.outage_minute_label} UTC ===")
        print(
            f"Baseline Fleet ({storm_before.concurrent_jobs_at_spike} concurrent jobs at spike):"
        )
        print(
            f"  Naive Exponential Backoff : {storm_before.amplification_naive}x capacity overload "
            f"({storm_before.seconds_over_capacity_naive}s above threshold)"
        )
        print(
            f"  Decorrelated Jitter       : {storm_before.amplification_decorrelated}x capacity "
            f"({storm_before.seconds_over_capacity_decorrelated}s above threshold)"
        )
        print(
            f"After `unherd` Phase-Shift ({storm_after.concurrent_jobs_at_spike} concurrent jobs at spike):"
        )
        print(
            f"  Decorrelated + Phase-Shift: {storm_after.amplification_decorrelated}x capacity "
            f"({storm_after.seconds_over_capacity_decorrelated}s above threshold)"
        )
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
