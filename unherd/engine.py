"""
Temporal collision analysis, deterministic phase-shift de-synchronization solver,
and synchronized retry-storm cascade simulator.
"""

from __future__ import annotations
from dataclasses import dataclass, replace
import difflib
import hashlib
import math
import random
import re
from typing import Dict, List, Optional, Tuple

from .parser import CronSchedule, JobSpec


@dataclass
class CollisionHotspot:
    minute_of_day: int
    time_label: str
    concurrent_jobs: int
    weighted_load: float
    job_names: List[str]


@dataclass
class FleetAnalysis:
    total_jobs: int
    total_daily_runs: int
    minute_concurrency: List[int]        # length 1440
    minute_weighted_load: List[float]    # length 1440
    hourly_runs: List[int]               # length 24
    peak_concurrency: int
    peak_weighted_load: float
    mean_concurrency: float
    peak_to_average_ratio: float
    collision_minutes_count: int         # minutes where >= 2 jobs fire simultaneously
    severe_minutes_count: int            # minutes where >= 4 jobs fire simultaneously
    minute_zero_share_pct: float         # % of all runs firing at minute :00 of any hour
    top_five_divisible_share_pct: float  # % of all runs firing on multiples of 5 mins
    gini_coefficient: float
    hotspots: List[CollisionHotspot]


@dataclass
class ScheduleMutation:
    job_id: str
    job_name: str
    old_expression: str
    new_expression: str
    phase_shift_minutes: int
    hour_shift: int
    reason: str
    daily_runs_before: int
    daily_runs_after: int


@dataclass
class OptimizationResult:
    original_jobs: List[JobSpec]
    optimized_jobs: List[JobSpec]
    mutations: List[ScheduleMutation]
    before_analysis: FleetAnalysis
    after_analysis: FleetAnalysis
    peak_concurrency_reduction_pct: float
    peak_load_reduction_pct: float
    collision_minutes_reduction_pct: float


@dataclass
class RetryStormPoint:
    second: int
    naive_exponential_qps: float
    linear_jitter_qps: float
    decorrelated_jitter_qps: float
    capacity_limit_qps: float


@dataclass
class RetryStormReport:
    outage_minute_label: str
    concurrent_jobs_at_spike: int
    amplification_naive: float
    amplification_decorrelated: float
    seconds_over_capacity_naive: int
    seconds_over_capacity_decorrelated: int
    series: List[RetryStormPoint]


def _format_minute(mod: int) -> str:
    h = (mod // 60) % 24
    m = mod % 60
    return f"{h:02d}:{m:02d}"


def _compute_gini(values: List[float]) -> float:
    """Compute Gini coefficient of temporal load concentration across active slots."""
    n = len(values)
    if n == 0:
        return 0.0
    total = sum(values)
    if total <= 0:
        return 0.0
    sorted_vals = sorted(values)
    cum = 0.0
    for i, val in enumerate(sorted_vals, start=1):
        cum += i * val
    return max(0.0, min(1.0, (2.0 * cum) / (n * total) - (n + 1.0) / n))


def analyze_fleet(jobs: List[JobSpec]) -> FleetAnalysis:
    """Compute 1,440-minute temporal collision profile and herd metrics."""
    minute_concurrency = [0] * 1440
    minute_weighted_load = [0.0] * 1440
    minute_jobs: List[List[str]] = [[] for _ in range(1440)]
    hourly_runs = [0] * 24

    total_runs = 0
    minute_zero_runs = 0
    mod_five_runs = 0

    for job in jobs:
        fire_mins = job.schedule.daily_fire_minutes()
        total_runs += len(fire_mins)
        span_mins = max(1, math.ceil(job.duration_sec / 60.0))
        for fm in fire_mins:
            if (fm % 60) == 0:
                minute_zero_runs += 1
            if (fm % 5) == 0:
                mod_five_runs += 1
            hourly_runs[(fm // 60) % 24] += 1
            for offset in range(span_mins):
                slot = (fm + offset) % 1440
                minute_concurrency[slot] += 1
                decay = 1.0 if offset == 0 else 0.75
                minute_weighted_load[slot] = round(
                    minute_weighted_load[slot] + job.weight * decay, 4
                )
                label = job.name if offset == 0 else f"{job.name} (spillover)"
                minute_jobs[slot].append(label)

    peak_conc = max(minute_concurrency) if minute_concurrency else 0
    peak_load = max(minute_weighted_load) if minute_weighted_load else 0.0
    mean_conc = sum(minute_concurrency) / 1440.0
    mean_load = sum(minute_weighted_load) / 1440.0
    par = (peak_load / mean_load) if mean_load > 0 else 1.0

    collision_mins = sum(1 for c in minute_concurrency if c >= 2)
    severe_mins = sum(1 for c in minute_concurrency if c >= 4)

    m0_pct = (minute_zero_runs / total_runs * 100.0) if total_runs > 0 else 0.0
    m5_pct = (mod_five_runs / total_runs * 100.0) if total_runs > 0 else 0.0

    candidate_slots = [
        (minute_weighted_load[m], minute_concurrency[m], -m, m)
        for m in range(1440)
        if minute_concurrency[m] > 0
    ]
    candidate_slots.sort(reverse=True)
    hotspots: List[CollisionHotspot] = []
    for w_load, conc, _, m in candidate_slots[:12]:
        hotspots.append(
            CollisionHotspot(
                minute_of_day=m,
                time_label=_format_minute(m),
                concurrent_jobs=conc,
                weighted_load=round(w_load, 2),
                job_names=minute_jobs[m],
            )
        )

    return FleetAnalysis(
        total_jobs=len(jobs),
        total_daily_runs=total_runs,
        minute_concurrency=minute_concurrency,
        minute_weighted_load=minute_weighted_load,
        hourly_runs=hourly_runs,
        peak_concurrency=peak_conc,
        peak_weighted_load=round(peak_load, 2),
        mean_concurrency=round(mean_conc, 3),
        peak_to_average_ratio=round(par, 2),
        collision_minutes_count=collision_mins,
        severe_minutes_count=severe_mins,
        minute_zero_share_pct=round(m0_pct, 1),
        top_five_divisible_share_pct=round(m5_pct, 1),
        gini_coefficient=round(_compute_gini(minute_weighted_load), 3),
        hotspots=hotspots,
    )


def _detect_minute_period(minutes: Tuple[int, ...]) -> Optional[int]:
    """
    If `minutes` represents a periodic step pattern within the hour (e.g. */5, */10, */15,
    or a single minute per hour -> period 60), return its period P. Otherwise return None.
    """
    n = len(minutes)
    if n == 60:
        return 1
    if n == 1:
        return 60
    if n > 1 and 60 % n == 0:
        period = 60 // n
        base = minutes[0]
        expected = tuple((base + i * period) % 60 for i in range(n))
        if tuple(sorted(expected)) == minutes:
            return period
    return None


def _detect_hour_period(hours: Tuple[int, ...]) -> Optional[int]:
    """
    If `hours` represents a periodic step pattern across 24h (e.g. single hour -> 24,
    */2, */3, */4, */6, */8, */12), return its period in hours.
    """
    n = len(hours)
    if n == 24:
        return 1
    if n == 1:
        return 24
    if n > 1 and 24 % n == 0:
        period = 24 // n
        base = hours[0]
        expected = tuple((base + i * period) % 24 for i in range(n))
        if tuple(sorted(expected)) == hours:
            return period
    return None


def _format_shifted_minute_field(period: int, phase: int) -> Tuple[str, Tuple[int, ...]]:
    phase = phase % period
    if period == 60:
        return str(phase), (phase,)
    if period == 1:
        return "*", tuple(range(60))
    ticks = tuple(sorted((phase + i * period) % 60 for i in range(60 // period)))
    if phase == 0:
        return f"*/{period}", ticks
    return f"{phase}-59/{period}", ticks


def _format_shifted_hour_field(
    orig_hours: Tuple[int, ...],
    orig_raw: str,
    hour_shift: int,
    allow_hour_shift: bool,
) -> Tuple[str, Tuple[int, ...]]:
    if not allow_hour_shift or hour_shift == 0:
        return orig_raw, orig_hours
    period = _detect_hour_period(orig_hours)
    if period is None or period == 1:
        return orig_raw, orig_hours
    if period == 24:
        new_h = (orig_hours[0] + hour_shift) % 24
        return str(new_h), (new_h,)
    new_phase = (orig_hours[0] + hour_shift) % period
    ticks = tuple(sorted((new_phase + i * period) % 24 for i in range(24 // period)))
    if new_phase == 0:
        return f"*/{period}", ticks
    return f"{new_phase}-23/{period}", ticks


def optimize_fleet(
    jobs: List[JobSpec],
    allow_hour_shift: bool = False,
) -> OptimizationResult:
    """
    Deterministic Min-Max L_p Temporal Load De-synchronizer.
    Preserves exact daily execution frequency and cadence period for every job
    while shifting phase offsets to minimize peak concurrency and cubic collision energy.
    """
    before_analysis = analyze_fleet(jobs)

    def _sort_key(item: Tuple[int, JobSpec]) -> Tuple[int, int, int, float, str]:
        _, j = item
        m_period = _detect_minute_period(j.schedule.minutes)
        is_fixed = 0 if (j.pinned or m_period == 1) else 1
        period_rank = m_period if m_period is not None else 999
        daily_freq = len(j.schedule.daily_fire_minutes())
        return (is_fixed, period_rank, -daily_freq, -j.weight, j.id)

    indexed_jobs = list(enumerate(jobs))
    ordered = sorted(indexed_jobs, key=_sort_key)

    load_grid = [0.0] * 1440
    conc_grid = [0] * 1440

    optimized_by_index: Dict[int, JobSpec] = {}
    mutations_by_index: Dict[int, ScheduleMutation] = {}

    for orig_idx, job in ordered:
        sched = job.schedule
        m_period = _detect_minute_period(sched.minutes)
        h_period = _detect_hour_period(sched.hours)
        span_mins = max(1, math.ceil(job.duration_sec / 60.0))

        if job.pinned or m_period == 1:
            for fm in sched.daily_fire_minutes():
                for off in range(span_mins):
                    slot = (fm + off) % 1440
                    conc_grid[slot] += 1
                    load_grid[slot] += job.weight * (1.0 if off == 0 else 0.75)
            optimized_by_index[orig_idx] = job
            continue

        digest = int(hashlib.sha256(f"{job.id}:{job.name}".encode("utf-8")).hexdigest()[:8], 16)

        if m_period is not None:
            candidate_m_phases = list(range(m_period))
        else:
            candidate_m_phases = list(range(60))

        if allow_hour_shift and h_period is not None and h_period > 1:
            max_h = min(h_period, 6) if h_period == 24 else h_period
            candidate_h_shifts = list(range(max_h))
        else:
            candidate_h_shifts = [0]

        best_score: Optional[Tuple[float, int, float, float, int]] = None
        best_candidate: Optional[Tuple[str, Tuple[int, ...], str, Tuple[int, ...], int, int]] = None

        orig_base_m = sched.minutes[0]

        for h_shift in candidate_h_shifts:
            h_str, cand_hours = _format_shifted_hour_field(
                sched.hours, sched.hour_field_raw, h_shift, allow_hour_shift
            )
            for m_phase in candidate_m_phases:
                if m_period is not None:
                    m_str, cand_mins = _format_shifted_minute_field(m_period, m_phase)
                    shift_delta = (m_phase - (orig_base_m % m_period)) % m_period
                else:
                    cand_mins = tuple(sorted((m + m_phase) % 60 for m in sched.minutes))
                    m_str = ",".join(str(m) for m in cand_mins)
                    shift_delta = m_phase

                peak_after = 0.0
                peak_conc_after = 0
                cubic_energy = 0.0
                collisions_added = 0

                for h in cand_hours:
                    base = h * 60
                    for m in cand_mins:
                        fm = base + m
                        for off in range(span_mins):
                            slot = (fm + off) % 1440
                            new_w = load_grid[slot] + job.weight * (1.0 if off == 0 else 0.75)
                            new_c = conc_grid[slot] + 1
                            if new_w > peak_after:
                                peak_after = new_w
                            if new_c > peak_conc_after:
                                peak_conc_after = new_c
                            cubic_energy += (new_w ** 3) + (new_c ** 2) * 0.5
                            if new_c >= 2:
                                collisions_added += (new_c - 1)

                round_number_penalty = 0.0
                for m in cand_mins:
                    if m == 0:
                        round_number_penalty += 0.25
                    elif m % 5 == 0:
                        round_number_penalty += 0.08

                tie_breaker = ((m_phase * 37 + h_shift * 13 + digest) % 59) * 1e-4

                score = (
                    round(peak_after, 4),
                    peak_conc_after,
                    round(cubic_energy, 4),
                    round(collisions_added + round_number_penalty + tie_breaker, 6),
                    abs(h_shift) * 60 + shift_delta,
                )

                if best_score is None or score < best_score:
                    best_score = score
                    best_candidate = (m_str, cand_mins, h_str, cand_hours, shift_delta, h_shift)

        assert best_candidate is not None
        m_str, cand_mins, h_str, cand_hours, shift_delta, h_shift = best_candidate

        new_expr = f"{m_str} {h_str} {sched.dom_field_raw} {sched.month_field_raw} {sched.dow_field_raw}"
        new_sched = CronSchedule(
            expression=new_expr,
            minutes=cand_mins,
            hours=cand_hours,
            days_of_month=sched.days_of_month,
            months=sched.months,
            days_of_week=sched.days_of_week,
            minute_field_raw=m_str,
            hour_field_raw=h_str,
            dom_field_raw=sched.dom_field_raw,
            month_field_raw=sched.month_field_raw,
            dow_field_raw=sched.dow_field_raw,
        )

        for fm in new_sched.daily_fire_minutes():
            for off in range(span_mins):
                slot = (fm + off) % 1440
                conc_grid[slot] += 1
                load_grid[slot] += job.weight * (1.0 if off == 0 else 0.75)

        new_job = replace(job, expression=new_expr, schedule=new_sched)
        optimized_by_index[orig_idx] = new_job

        if new_expr != job.expression:
            reason_parts = []
            if shift_delta != 0:
                reason_parts.append(f"+{shift_delta}m phase offset")
            if h_shift != 0:
                reason_parts.append(f"+{h_shift}h window shift")
            mutations_by_index[orig_idx] = ScheduleMutation(
                job_id=job.id,
                job_name=job.name,
                old_expression=job.expression,
                new_expression=new_expr,
                phase_shift_minutes=shift_delta,
                hour_shift=h_shift,
                reason=", ".join(reason_parts) or "canonicalized",
                daily_runs_before=len(sched.daily_fire_minutes()),
                daily_runs_after=len(new_sched.daily_fire_minutes()),
            )

    optimized_jobs = [optimized_by_index[i] for i in range(len(jobs))]
    mutations = [mutations_by_index[i] for i in sorted(mutations_by_index.keys())]
    after_analysis = analyze_fleet(optimized_jobs)

    peak_c_red = (
        (before_analysis.peak_concurrency - after_analysis.peak_concurrency)
        / before_analysis.peak_concurrency
        * 100.0
        if before_analysis.peak_concurrency > 0
        else 0.0
    )
    peak_l_red = (
        (before_analysis.peak_weighted_load - after_analysis.peak_weighted_load)
        / before_analysis.peak_weighted_load
        * 100.0
        if before_analysis.peak_weighted_load > 0
        else 0.0
    )
    coll_red = (
        (before_analysis.collision_minutes_count - after_analysis.collision_minutes_count)
        / before_analysis.collision_minutes_count
        * 100.0
        if before_analysis.collision_minutes_count > 0
        else 0.0
    )

    return OptimizationResult(
        original_jobs=jobs,
        optimized_jobs=optimized_jobs,
        mutations=mutations,
        before_analysis=before_analysis,
        after_analysis=after_analysis,
        peak_concurrency_reduction_pct=round(peak_c_red, 1),
        peak_load_reduction_pct=round(peak_l_red, 1),
        collision_minutes_reduction_pct=round(coll_red, 1),
    )


def simulate_retry_storm(
    jobs: List[JobSpec],
    horizon_seconds: int = 90,
    seed: int = 42,
) -> RetryStormReport:
    """
    Simulate what happens when a downstream dependency experiences a transient 15-second
    brownout right at the fleet's worst collision minute.
    """
    analysis = analyze_fleet(jobs)
    if analysis.hotspots:
        worst = analysis.hotspots[0]
        spike_conc = worst.concurrent_jobs
        spike_label = worst.time_label
        spike_weight = worst.weighted_load
    else:
        spike_conc = max(1, len(jobs))
        spike_label = "00:00"
        spike_weight = float(spike_conc)

    initial_requests = max(4, int(round(spike_weight * 8)))
    capacity_qps = max(6.0, round(initial_requests * 0.35, 1))

    naive_qps = [0.0] * horizon_seconds
    linear_qps = [0.0] * horizon_seconds
    decorr_qps = [0.0] * horizon_seconds

    rng = random.Random(seed)

    for req in range(initial_requests):
        t0 = 0 if req % 3 != 0 else 1
        naive_qps[t0] += 1.0
        linear_qps[t0] += 1.0
        decorr_qps[t0] += 1.0

        t_naive = float(t0)
        t_linear = float(t0)
        t_decorr = float(t0)
        prev_sleep = 2.0

        for attempt in range(1, 5):
            exp_delay = 2.0 ** attempt

            t_naive += exp_delay + rng.uniform(-0.15, 0.15)
            idx_n = int(round(t_naive))
            if 0 <= idx_n < horizon_seconds:
                naive_qps[idx_n] += 1.0

            t_linear += (exp_delay * 0.5) + rng.uniform(0.0, exp_delay * 0.5)
            idx_l = int(round(t_linear))
            if 0 <= idx_l < horizon_seconds:
                linear_qps[idx_l] += 0.85 ** (attempt - 1)

            sleep_d = min(30.0, rng.uniform(2.0, prev_sleep * 3.0))
            prev_sleep = sleep_d
            t_decorr += sleep_d
            idx_d = int(round(t_decorr))
            if 0 <= idx_d < horizon_seconds:
                survival = 1.0 if idx_d < 12 else (0.45 ** (attempt - 1))
                decorr_qps[idx_d] += survival

    peak_naive = max(naive_qps) if naive_qps else 1.0
    peak_decorr = max(1.0, max(decorr_qps[2:]) if len(decorr_qps) > 2 else 1.0)

    over_naive = sum(1 for v in naive_qps if v > capacity_qps)
    over_decorr = sum(1 for v in decorr_qps if v > capacity_qps)

    series = [
        RetryStormPoint(
            second=s,
            naive_exponential_qps=round(naive_qps[s], 2),
            linear_jitter_qps=round(linear_qps[s], 2),
            decorrelated_jitter_qps=round(decorr_qps[s], 2),
            capacity_limit_qps=capacity_qps,
        )
        for s in range(horizon_seconds)
    ]

    return RetryStormReport(
        outage_minute_label=spike_label,
        concurrent_jobs_at_spike=spike_conc,
        amplification_naive=round(peak_naive / capacity_qps, 2),
        amplification_decorrelated=round(peak_decorr / capacity_qps, 2),
        seconds_over_capacity_naive=over_naive,
        seconds_over_capacity_decorrelated=over_decorr,
        series=series,
    )


def generate_unified_diff(
    raw_content: str,
    opt_result: OptimizationResult,
    filename: str = "crontab",
) -> str:
    """Produce a standard unified diff (`diff -u`) applying the optimized schedules."""
    orig_lines = raw_content.splitlines(keepends=True)
    if orig_lines and not orig_lines[-1].endswith("\n"):
        orig_lines[-1] += "\n"
    new_lines = list(orig_lines)

    mutation_by_line: Dict[int, JobSpec] = {
        j.line_number: j for j in opt_result.optimized_jobs if j.line_number > 0
    }
    orig_by_line: Dict[int, JobSpec] = {
        j.line_number: j for j in opt_result.original_jobs if j.line_number > 0
    }

    for ln_no, opt_job in mutation_by_line.items():
        orig_job = orig_by_line.get(ln_no)
        if not orig_job or orig_job.expression == opt_job.expression:
            continue
        idx = ln_no - 1
        if 0 <= idx < len(new_lines):
            line = new_lines[idx]
            if orig_job.raw_line.strip().startswith("@"):
                macro_token = orig_job.raw_line.strip().split()[0]
                new_lines[idx] = line.replace(macro_token, opt_job.expression, 1)
            else:
                pattern = re.escape(orig_job.schedule.minute_field_raw) + r"\s+" + re.escape(
                    orig_job.schedule.hour_field_raw
                )
                replacement = f"{opt_job.schedule.minute_field_raw} {opt_job.schedule.hour_field_raw}"
                new_lines[idx] = re.sub(pattern, replacement, line, count=1)

    diff = difflib.unified_diff(
        orig_lines,
        new_lines,
        fromfile=f"a/{filename}",
        tofile=f"b/{filename}",
        lineterm="\n",
    )
    return "".join(diff)
