"""
unherd: Fleet Cron Collision Analyzer & Thundering-Herd De-synchronizer.
"""

__version__ = "0.1.0"
__all__ = [
    "CronSchedule",
    "JobSpec",
    "parse_cron_expression",
    "parse_fleet_config",
    "analyze_fleet",
    "optimize_fleet",
    "simulate_retry_storm",
    "generate_unified_diff",
]

from .parser import CronSchedule, JobSpec, parse_cron_expression, parse_fleet_config
from .engine import (
    analyze_fleet,
    optimize_fleet,
    simulate_retry_storm,
    generate_unified_diff,
)
