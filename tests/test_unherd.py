import unittest
from unherd.parser import parse_cron_expression, parse_fleet_config
from unherd.engine import (
    analyze_fleet,
    generate_unified_diff,
    optimize_fleet,
    simulate_retry_storm,
)


class TestUnherdParserAndEngine(unittest.TestCase):
    def test_cron_expression_parsing_variants(self) -> None:
        s1 = parse_cron_expression("*/15 * * * *")
        self.assertEqual(s1.minutes, (0, 15, 30, 45))
        self.assertEqual(len(s1.hours), 24)
        self.assertEqual(len(s1.daily_fire_minutes()), 96)

        s2 = parse_cron_expression("3-59/15 1-23/2 * * mon-fri")
        self.assertEqual(s2.minutes, (3, 18, 33, 48))
        self.assertEqual(s2.hours, (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23))
        self.assertEqual(s2.days_of_week, (1, 2, 3, 4, 5))

        s3 = parse_cron_expression("@hourly")
        self.assertEqual(s3.minutes, (0,))
        self.assertEqual(len(s3.hours), 24)

    def test_invalid_cron_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_cron_expression("65 * * * *")
        with self.assertRaises(ValueError):
            parse_cron_expression("* 25 * * *")
        with self.assertRaises(ValueError):
            parse_cron_expression("*/0 * * * *")

    def test_crontab_and_yaml_fleet_parsing(self) -> None:
        crontab_sample = """
        # Fleet header
        */5 * * * * /bin/job_a # name=job-a weight=2.5 duration=20s
        0 * * * *   /bin/job_b # name=job-b weight=3.0 pinned
        @daily      /bin/job_c # name=job-c weight=4.0
        """
        jobs = parse_fleet_config(crontab_sample)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0].name, "job-a")
        self.assertAlmostEqual(jobs[0].weight, 2.5)
        self.assertTrue(jobs[1].pinned)
        self.assertEqual(jobs[2].expression, "0 0 * * *")

        yaml_sample = """
        apiVersion: batch/v1
        kind: CronJob
        metadata:
          name: etl-worker
        spec:
          schedule: "*/10 * * * *" # weight=2.0
        """
        yjobs = parse_fleet_config(yaml_sample)
        self.assertEqual(len(yjobs), 1)
        self.assertEqual(yjobs[0].name, "etl-worker")
        self.assertEqual(yjobs[0].schedule.minutes, (0, 10, 20, 30, 40, 50))

    def test_optimizer_flattens_herd_and_preserves_daily_run_invariance(self) -> None:
        crontab = "\n".join(
            [
                "*/5 * * * * /bin/w1 # name=w1 weight=2.0",
                "*/5 * * * * /bin/w2 # name=w2 weight=2.0",
                "*/5 * * * * /bin/w3 # name=w3 weight=2.0",
                "*/5 * * * * /bin/w4 # name=w4 weight=2.0",
                "*/5 * * * * /bin/w5 # name=w5 weight=2.0",
                "0 * * * *   /bin/h1 # name=h1 weight=3.0",
                "0 * * * *   /bin/h2 # name=h2 weight=3.0",
                "0 * * * *   /bin/h3 # name=h3 weight=3.0",
                "0 * * * *   /bin/h4 # name=h4 weight=3.0 pinned",
            ]
        )
        jobs = parse_fleet_config(crontab)
        res = optimize_fleet(jobs, allow_hour_shift=True)

        # Invariant 1: Total daily executions across the fleet MUST remain strictly identical
        self.assertEqual(
            res.before_analysis.total_daily_runs,
            res.after_analysis.total_daily_runs,
        )

        # Invariant 2: Every individual job must run the exact same number of times per day
        for orig_j, opt_j in zip(res.original_jobs, res.optimized_jobs):
            self.assertEqual(
                len(orig_j.schedule.daily_fire_minutes()),
                len(opt_j.schedule.daily_fire_minutes()),
            )

        # Invariant 3: Pinned job (h4) must not be modified
        pinned_after = [j for j in res.optimized_jobs if j.name == "h4"][0]
        self.assertEqual(pinned_after.expression, "0 * * * *")

        # Invariant 4: Peak concurrency and peak weighted load must drop substantially
        self.assertEqual(res.before_analysis.peak_concurrency, 9)
        self.assertLessEqual(res.after_analysis.peak_concurrency, 2)
        self.assertGreater(res.peak_concurrency_reduction_pct, 70.0)
        self.assertGreater(res.peak_load_reduction_pct, 70.0)

        # Unified diff generation check
        patch = generate_unified_diff(crontab, res, filename="fleet.crontab")
        self.assertIn("--- a/fleet.crontab", patch)
        self.assertIn("+++ b/fleet.crontab", patch)

    def test_retry_storm_simulation(self) -> None:
        crontab = "\n".join(
            [f"0 * * * * /bin/job_{i} # name=job-{i} weight=2.5" for i in range(8)]
        )
        jobs = parse_fleet_config(crontab)
        storm = simulate_retry_storm(jobs, horizon_seconds=60)
        self.assertEqual(storm.concurrent_jobs_at_spike, 8)
        self.assertGreater(storm.amplification_naive, storm.amplification_decorrelated)
        self.assertEqual(len(storm.series), 60)


if __name__ == "__main__":
    unittest.main()
