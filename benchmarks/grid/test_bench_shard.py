#!/usr/bin/env python3
"""
benchmarks/grid/test_bench_shard.py - Pure-helper unit tests for bench_shard_r2_m1.py.

No binary, no cluster, no network. Verifies deterministic 1-based shard partitioning,
Spike R2 savings / `< 30%` stop-rule calculation, Measure M1 `<= 0.50` ratio calculation,
`oxideswarm shard --json` parsing, and NoHostname sanitization.

Run:   python3 benchmarks/grid/test_bench_shard.py
"""

import datetime
import importlib.util
import json
import os
import socket
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "bench_shard_r2_m1.py")
spec = importlib.util.spec_from_file_location("bench_shard_r2_m1", SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class ShardMathTests(unittest.TestCase):
    def test_partition_test_names_1based_modulo(self):
        tests = [f"t{i}" for i in range(7)]
        s1 = bench.partition_test_names(tests, 1, 3)
        s2 = bench.partition_test_names(tests, 2, 3)
        s3 = bench.partition_test_names(tests, 3, 3)
        self.assertEqual(s1, ["t0", "t3", "t6"])
        self.assertEqual(s2, ["t1", "t4"])
        self.assertEqual(s3, ["t2", "t5"])
        self.assertEqual(sorted(s1 + s2 + s3), sorted(tests))

    def test_partition_rejects_out_of_range_index(self):
        with self.assertRaises(ValueError):
            bench.partition_test_names(["a"], 0, 3)
        with self.assertRaises(ValueError):
            bench.partition_test_names(["a"], 4, 3)

    def test_r2_stop_rule_thresholds(self):
        # 29.9% savings -> stop rule triggered
        below = bench.compute_r2_metrics(10.0, 7.01)
        self.assertEqual(below["savings_pct"], 29.9)
        self.assertTrue(below["stop_rule_triggered"])

        # 30.0% savings -> stop rule NOT triggered
        exact = bench.compute_r2_metrics(10.0, 7.0)
        self.assertEqual(exact["savings_pct"], 30.0)
        self.assertFalse(exact["stop_rule_triggered"])

    def test_m1_target_thresholds(self):
        met = bench.compute_m1_metrics(10.0, 5.0)
        self.assertEqual(met["ratio_3w_to_1w"], 0.5)
        self.assertTrue(met["target_met"])

        missed = bench.compute_m1_metrics(10.0, 5.01)
        self.assertFalse(missed["target_met"])

    def test_parse_shard_json(self):
        sample = json.dumps({
            "shard": True,
            "status": "completed",
            "total_shards": 3,
            "completed_shards": 3,
            "failed_shards": 0,
            "machines": 3,
            "waited_ms": 1420,
            "task_time_sum_ms": 3890,
        })
        parsed = bench.parse_shard_json(sample)
        self.assertEqual(parsed["completed_shards"], 3)
        self.assertEqual(parsed["machines"], 3)
        self.assertEqual(parsed["waited_ms"], 1420)
        self.assertEqual(parsed["task_time_sum_ms"], 3890)


class NoHostnameTests(unittest.TestCase):
    SENTINEL = "SENTINEL-HOST-ax2"

    def test_environment_record_and_filename_exclude_hostname(self):
        with mock.patch.object(socket, "gethostname", return_value=self.SENTINEL), \
             mock.patch.object(bench.platform, "node", return_value=self.SENTINEL):
            hw = bench.hardware()
            osf = bench.os_info()
            with tempfile.TemporaryDirectory() as d:
                path = bench.result_path(d, datetime.datetime(2026, 11, 22), osf, hw)
        dumped = json.dumps({"hardware": hw, "os": osf, "git": bench.git_info()})
        self.assertNotIn(self.SENTINEL, dumped)
        self.assertNotIn(self.SENTINEL, path)
        self.assertIn("-r2-m1-", os.path.basename(path))

    def test_remote_report_and_privacy_checks(self):
        sample_report = json.dumps({
            "shard": True,
            "status": "completed",
            "total_shards": 3,
            "completed_shards": 3,
            "failed_shards": 0,
            "machines": 3,
            "waited_ms": 2100,
            "task_time_sum_ms": 5900,
            "shards": [
                {
                    "index": 1,
                    "worker_name": "mac-lab-worker",
                    "stdout": 'ok\nAX2_SHARD_SUMMARY {"executed_tests": 100, "idx": 1, "listed_tests": 300, "total": 3}\n',
                },
                {
                    "index": 2,
                    "worker_name": "win-case-lab",
                    "stdout": 'AX2_SHARD_SUMMARY {"executed_tests": 100, "idx": 2, "listed_tests": 300, "total": 3}\n',
                },
                {
                    "index": 3,
                    "worker_name": "linux-docker-lab",
                    "stdout": 'AX2_SHARD_SUMMARY {"executed_tests": 100, "idx": 3, "listed_tests": 300, "total": 3}\n',
                },
            ],
        })
        full = bench.parse_shard_report_full(sample_report)
        self.assertEqual(full["per_worker_shards"], {"linux-docker-lab": 1, "mac-lab-worker": 1, "win-case-lab": 1})
        self.assertEqual(sum(c["executed_tests"] for c in full["cargo_summaries"]), 300)

        bench.assert_no_private_identifiers({"ok": "mac-lab-worker", "count": 3})
        with self.assertRaises(ValueError):
            bench.assert_no_private_identifiers({"ip": "192.168.99.42"})
        with self.assertRaises(ValueError):
            bench.assert_no_private_identifiers({"tok": "oxs1-abcdefgh12345"})


if __name__ == "__main__":
    unittest.main()
