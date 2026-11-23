#!/usr/bin/env python3
"""
benchmarks/grid/test_bench_m15.py - Pure unit tests for bench_m15_quickstart.py.

No network, no cluster, no binary required.
Run: python3 benchmarks/grid/test_bench_m15.py
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
SCRIPT = os.path.join(HERE, "bench_m15_quickstart.py")
spec = importlib.util.spec_from_file_location("bench_m15_quickstart", SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class BenchM15Tests(unittest.TestCase):
    def test_expected_target_mapping(self):
        self.assertEqual(bench.expected_target("Darwin", "arm64"), ("aarch64-apple-darwin", ".tar.gz"))
        self.assertEqual(bench.expected_target("Darwin", "x86_64"), ("x86_64-apple-darwin", ".tar.gz"))
        self.assertEqual(bench.expected_target("Linux", "x86_64"), ("x86_64-unknown-linux-gnu", ".tar.gz"))
        self.assertEqual(bench.expected_target("Windows", "AMD64"), ("x86_64-pc-windows-msvc", ".zip"))
        with self.assertRaises(ValueError):
            bench.expected_target("Linux", "riscv64")

    def test_aggregate_runs_computes_medians(self):
        runs = [
            {"install_s": 1.0, "master_ready_s": 0.2, "worker_ready_s": 0.5, "first_submit_s": 0.4, "total_s": 2.1, "attestation_checked": True, "submit_exit_code": 0, "submit_stdout_bytes": 8},
            {"install_s": 3.0, "master_ready_s": 0.4, "worker_ready_s": 0.7, "first_submit_s": 0.6, "total_s": 4.7, "attestation_checked": True, "submit_exit_code": 0, "submit_stdout_bytes": 8},
            {"install_s": 2.0, "master_ready_s": 0.3, "worker_ready_s": 0.6, "first_submit_s": 0.5, "total_s": 3.4, "attestation_checked": True, "submit_exit_code": 0, "submit_stdout_bytes": 8},
        ]
        summary = bench.aggregate_runs(runs)
        self.assertEqual(summary["install_s"]["median"], 2.0)
        self.assertEqual(summary["total_s"]["median"], 3.4)
        self.assertEqual(summary["total_s"]["min"], 2.1)
        self.assertEqual(summary["total_s"]["max"], 4.7)
        self.assertTrue(summary["all_attestation_checked"])
        self.assertTrue(summary["all_succeeded"])

    def test_environment_record_and_path_exclude_hostname(self):
        sentinel = "SENTINEL-M15-HOST"
        with mock.patch.object(socket, "gethostname", return_value=sentinel), \
             mock.patch.object(bench.platform, "node", return_value=sentinel):
            hw = bench.hardware()
            osf = bench.os_info()
            with tempfile.TemporaryDirectory() as d:
                out = bench.result_path(d, datetime.datetime(2026, 11, 24), osf)
        dumped = json.dumps({"hardware": hw, "os": osf, "git": bench.git_info()})
        self.assertNotIn(sentinel, dumped)
        self.assertNotIn(sentinel, out)
        self.assertIn("-m15-", os.path.basename(out))


if __name__ == "__main__":
    unittest.main()
