"""
POSIX / Vixie Cron expression parser and multi-format schedule extractor.
Supports standard Crontab, Kubernetes CronJob YAML, and GitHub Actions workflows.
"""

from __future__ import annotations
from dataclasses import dataclass, field
import re
from typing import Dict, List, Optional, Set, Tuple

MONTH_MAP: Dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

DOW_MAP: Dict[str, int] = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

MACROS: Dict[str, str] = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


@dataclass(frozen=True)
class CronSchedule:
    expression: str
    minutes: Tuple[int, ...]
    hours: Tuple[int, ...]
    days_of_month: Tuple[int, ...]
    months: Tuple[int, ...]
    days_of_week: Tuple[int, ...]
    minute_field_raw: str
    hour_field_raw: str
    dom_field_raw: str
    month_field_raw: str
    dow_field_raw: str

    def fires_at_minute_of_day(self, minute_of_day: int) -> bool:
        """Return True if this schedule fires at the given minute of a representative day (0..1439)."""
        m = minute_of_day % 60
        h = (minute_of_day // 60) % 24
        return (m in self.minutes) and (h in self.hours)

    def daily_fire_minutes(self) -> List[int]:
        """Return sorted list of minute-of-day indices (0..1439) when this job triggers."""
        out: List[int] = []
        for h in self.hours:
            base = h * 60
            for m in self.minutes:
                out.append(base + m)
        return out


@dataclass
class JobSpec:
    id: str
    name: str
    expression: str
    schedule: CronSchedule
    duration_sec: int = 12
    weight: float = 1.0
    source_format: str = "crontab"
    line_number: int = 0
    raw_line: str = ""
    pinned: bool = False
    max_shift_minutes: int = 59
    retries: int = 3
    base_backoff_sec: float = 2.0
    metadata: Dict[str, str] = field(default_factory=dict)


def _replace_names(token: str, mapping: Dict[str, int]) -> str:
    out = token.lower()
    for name, val in mapping.items():
        out = re.sub(rf"\b{name}\b", str(val), out)
    return out


def _parse_field(
    field_str: str,
    min_val: int,
    max_val: int,
    names: Optional[Dict[str, int]] = None,
    is_dow: bool = False,
) -> Tuple[int, ...]:
    cleaned = field_str.strip()
    if not cleaned:
        raise ValueError("Empty cron field")
    if names:
        cleaned = _replace_names(cleaned, names)

    values: Set[int] = set()
    for part in cleaned.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            range_part, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as exc:
                raise ValueError(f"Invalid step value '{step_str}' in '{field_str}'") from exc
            if step <= 0:
                raise ValueError(f"Step must be positive in '{field_str}'")
        else:
            range_part = part

        if range_part in ("*", "?"):
            start, end = min_val, max_val
        elif "-" in range_part:
            lo_str, hi_str = range_part.split("-", 1)
            start, end = int(lo_str), int(hi_str)
            if is_dow and end == 7:
                end = 7
        else:
            val = int(range_part)
            if is_dow and val == 7:
                val = 0
            if "/" in part:
                start, end = val, max_val
            else:
                if val < min_val or val > max_val:
                    raise ValueError(
                        f"Value {val} out of range [{min_val}, {max_val}] in '{field_str}'"
                    )
                values.add(val)
                continue

        if start < min_val or end > (7 if is_dow else max_val) or start > end:
            raise ValueError(
                f"Range {start}-{end} out of bounds [{min_val}, {max_val}] in '{field_str}'"
            )
        for v in range(start, end + 1, step):
            if is_dow and v == 7:
                values.add(0)
            else:
                values.add(v)

    if not values:
        raise ValueError(f"Field '{field_str}' produced no valid schedule ticks")
    return tuple(sorted(values))


def parse_cron_expression(expr: str) -> CronSchedule:
    """Parse a 5-field cron expression or @macro into a CronSchedule."""
    raw = expr.strip()
    if raw.lower() in MACROS:
        expanded = MACROS[raw.lower()]
    else:
        expanded = raw

    fields = expanded.split()
    if len(fields) != 5:
        raise ValueError(
            f"Expected 5 cron fields (min hour dom month dow), got {len(fields)} in '{expr}'"
        )

    m_raw, h_raw, dom_raw, mon_raw, dow_raw = fields
    minutes = _parse_field(m_raw, 0, 59)
    hours = _parse_field(h_raw, 0, 23)
    doms = _parse_field(dom_raw, 1, 31)
    months = _parse_field(mon_raw, 1, 12, names=MONTH_MAP)
    dows = _parse_field(dow_raw, 0, 6, names=DOW_MAP, is_dow=True)

    return CronSchedule(
        expression=expanded,
        minutes=minutes,
        hours=hours,
        days_of_month=doms,
        months=months,
        days_of_week=dows,
        minute_field_raw=m_raw,
        hour_field_raw=h_raw,
        dom_field_raw=dom_raw,
        month_field_raw=mon_raw,
        dow_field_raw=dow_raw,
    )


def _extract_inline_tags(text: str) -> Tuple[int, float, bool]:
    """Extract optional inline tags like # duration=25s weight=2.5 pinned."""
    duration = 15
    weight = 1.0
    pinned = False

    dur_match = re.search(r"(?:duration|dur)\s*=\s*(\d+)\s*s?", text, re.IGNORECASE)
    if dur_match:
        duration = max(1, min(3600, int(dur_match.group(1))))

    w_match = re.search(r"(?:weight|load|cost)\s*=\s*([0-9.]+)", text, re.IGNORECASE)
    if w_match:
        weight = max(0.1, min(100.0, float(w_match.group(1))))

    if re.search(r"\b(?:pinned|strict|fixed)\b", text, re.IGNORECASE):
        pinned = True

    return duration, weight, pinned


def parse_fleet_config(content: str) -> List[JobSpec]:
    """
    Auto-detect and parse a fleet schedule from:
    1. POSIX Crontab lines (including inline comments for job metadata)
    2. Kubernetes CronJob YAML manifests
    3. GitHub Actions workflow YAML (`on: schedule: - cron: ...`)
    """
    lines = content.splitlines()
    jobs: List[JobSpec] = []

    # Check if YAML-like (Kubernetes CronJob or GitHub Actions)
    is_yaml = any(
        re.match(r"^\s*(?:apiVersion|kind:\s*CronJob|schedule:\s*['\"]|-?\s*cron:\s*['\"])", ln)
        for ln in lines
    )

    if is_yaml:
        current_name: Optional[str] = None
        pending_comment = ""
        for idx, line in enumerate(lines, start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                pending_comment = stripped
                continue
            name_m = re.match(r"^\s*name:\s*['\"]?([a-zA-Z0-9_.-]+)['\"]?", line)
            if name_m:
                current_name = name_m.group(1)
                continue

            sched_m = re.match(
                r"^\s*(?:-\s*)?(?:schedule|cron):\s*['\"]([^'\"]+)['\"](.*)$", line
            )
            if sched_m:
                expr = sched_m.group(1).strip()
                trailing = sched_m.group(2) + " " + pending_comment
                try:
                    sched = parse_cron_expression(expr)
                except ValueError:
                    continue
                duration, weight, pinned = _extract_inline_tags(trailing)
                job_idx = len(jobs) + 1
                jname = current_name or f"yaml-job-{job_idx:02d}"
                jobs.append(
                    JobSpec(
                        id=f"job-{job_idx:02d}",
                        name=jname,
                        expression=sched.expression,
                        schedule=sched,
                        duration_sec=duration,
                        weight=weight,
                        source_format="yaml",
                        line_number=idx,
                        raw_line=line,
                        pinned=pinned,
                    )
                )
                current_name = None
                pending_comment = ""
        if jobs:
            return jobs

    # Standard Crontab parsing
    pending_comment = ""
    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            pending_comment = ""
            continue
        if stripped.startswith("#"):
            pending_comment = stripped
            continue
        # Skip environment variable assignments like SHELL=/bin/bash
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", stripped):
            continue

        # Handle @macro command
        if stripped.startswith("@"):
            parts = stripped.split(None, 1)
            if len(parts) < 2:
                continue
            macro, cmd_part = parts[0], parts[1]
            try:
                sched = parse_cron_expression(macro)
            except ValueError:
                continue
        else:
            parts = stripped.split(None, 5)
            if len(parts) < 6:
                continue
            expr = " ".join(parts[:5])
            cmd_part = parts[5]
            try:
                sched = parse_cron_expression(expr)
            except ValueError:
                continue

        # Split inline comment in command if present
        if "#" in cmd_part:
            cmd_clean, inline_comment = cmd_part.split("#", 1)
            meta_source = inline_comment + " " + pending_comment
        else:
            cmd_clean = cmd_part
            meta_source = pending_comment

        cmd_clean = cmd_clean.strip()
        duration, weight, pinned = _extract_inline_tags(meta_source)

        # Derive readable job name from command or comment
        name_match = re.search(r"name\s*=\s*([a-zA-Z0-9_.:-]+)", meta_source)
        if name_match:
            job_name = name_match.group(1)
        else:
            tokens = [
                t for t in cmd_clean.split()
                if not t.startswith("-")
                and t not in ("/bin/sh", "/bin/bash", "python", "python3", "node", "curl", "flock")
            ]
            raw_label = tokens[-1] if tokens else cmd_clean
            job_name = raw_label.split("/")[-1][:32] or f"cron-{len(jobs)+1:02d}"

        job_id = f"job-{len(jobs)+1:02d}"
        jobs.append(
            JobSpec(
                id=job_id,
                name=job_name,
                expression=sched.expression,
                schedule=sched,
                duration_sec=duration,
                weight=weight,
                source_format="crontab",
                line_number=idx,
                raw_line=line,
                pinned=pinned,
            )
        )
        pending_comment = ""

    return jobs
