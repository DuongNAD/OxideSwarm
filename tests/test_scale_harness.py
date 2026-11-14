#!/usr/bin/env python3
"""Unit tests for benchmarks/grid/scale_harness.py (`SCALE-1`)."""

from __future__ import annotations

import json
import os
import platform
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_DIR = REPO_ROOT / "benchmarks" / "grid"
if str(HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(HARNESS_DIR))

import scale_harness  # noqa: E402


class ScaleHarnessTests(unittest.TestCase):
    def test_sanitized_host_info_has_no_identifiers_or_paths(self) -> None:
        host = scale_harness.collect_sanitized_host_info()
        serialized = json.dumps(host).lower()
        hostname = platform.node().strip().lower()
        if hostname:
            self.assertNotIn(hostname, serialized)
        for env_key in ("USERNAME", "USER", "LOGNAME"):
            user = (os.environ.get(env_key) or "").strip().lower()
            if len(user) >= 3:
                self.assertNotIn(user, serialized)
        for val in host.values():
            if isinstance(val, str):
                self.assertNotIn("\\", val)
                self.assertNotIn("/", val)

    def test_aggregate_median_min_and_max(self) -> None:
        runs = [
            {
                "config": {"workers": 100, "quick": False},
                "burst": {"wall_ms": 1000.0, "tasks": 200, "failed": 0},
                "flag": True,
            },
            {
                "config": {"workers": 100, "quick": False},
                "burst": {"wall_ms": 1500.0, "tasks": 220, "failed": 0},
                "flag": True,
            },
            {
                "config": {"workers": 100, "quick": False},
                "burst": {"wall_ms": 2000.0, "tasks": 240, "failed": 0},
                "flag": True,
            },
        ]
        med = scale_harness.aggregate_median(runs)
        lo = scale_harness.aggregate_extremum(runs, min)
        hi = scale_harness.aggregate_extremum(runs, max)

        self.assertEqual(med["config"], {"workers": 100, "quick": False})
        self.assertIs(med["flag"], True)
        self.assertEqual(med["burst"]["wall_ms"], 1500.0)
        self.assertEqual(med["burst"]["tasks"], 220)
        self.assertEqual(lo["burst"]["wall_ms"], 1000.0)
        self.assertEqual(lo["burst"]["tasks"], 200)
        self.assertEqual(hi["burst"]["wall_ms"], 2000.0)
        self.assertEqual(hi["burst"]["tasks"], 240)

    def test_network_info_summary_and_output_paths(self) -> None:
        cfg = {"workers": 500, "base_latency_ms": 0, "jitter_ms": 0, "loss_prob": 0.0}
        self.assertEqual(
            scale_harness.network_info(cfg),
            {
                "mode": "loopback (127.0.0.1), master subprocess and in-process simulated workers on one host",
                "base_latency_ms": 0,
                "jitter_ms": 0,
                "loss_prob": 0.0,
            },
        )

        host = {"os": "Windows", "arch": "AMD64"}
        loop_path = scale_harness.default_result_path(HARNESS_DIR / "results", "2026-11-14", host, cfg)
        self.assertEqual(loop_path.name, "2026-11-14-scale1-windows-amd64-500w.json")

        imp_cfg = {"workers": 100, "base_latency_ms": 25, "jitter_ms": 10, "loss_prob": 0.01}
        imp_path = scale_harness.default_result_path(HARNESS_DIR / "results", "2026-11-14", host, imp_cfg)
        self.assertEqual(imp_path.name, "2026-11-14-scale1-windows-amd64-100w-imp-25ms-10ms.json")

        lines = scale_harness.format_summary_lines({"config": cfg})
        self.assertTrue(any("SCALE-1 Summary" in line for line in lines))
        self.assertTrue(any("workers=500" in line for line in lines))

    def test_python_files_have_pure_lf_endings(self) -> None:
        for path in (
            HARNESS_DIR / "scale_harness.py",
            REPO_ROOT / "tests" / "test_scale_harness.py",
        ):
            self.assertNotIn(b"\r\n", path.read_bytes(), f"CRLF detected in {path}")


if __name__ == "__main__":
    unittest.main()
