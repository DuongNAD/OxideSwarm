#!/usr/bin/env python3
"""
tests/test_bench_grid.py - checks of the pure helpers in benchmarks/grid/bench_grid.py.

No binary, no cluster, no network: percentile maths, the `Inputs:` line parser and the seeded input
tree. The benchmark itself runs with `python3 benchmarks/grid/bench_grid.py --bin <oxideswarm>`.

Run:   python3 tests/test_bench_grid.py
"""

import importlib.util
import os
import json
import socket
import tempfile
import unittest
from unittest import mock
import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "benchmarks", "grid", "bench_grid.py")
spec = importlib.util.spec_from_file_location("bench_grid", SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class Stats(unittest.TestCase):
    def test_percentile_nearest_rank(self):
        values = list(range(1, 101))
        self.assertEqual(bench.percentile(values, 50), 50)
        self.assertEqual(bench.percentile(values, 99), 99)
        self.assertEqual(bench.percentile(values, 100), 100)
        self.assertEqual(bench.percentile([7], 99), 7)

    def test_percentile_empty_is_an_error(self):
        with self.assertRaises(ValueError):
            bench.percentile([], 50)

    def test_summarize(self):
        s = bench.summarize([3, 1, 2])
        self.assertEqual((s["median"], s["min"], s["max"], s["runs"]), (2, 1, 3, 3))


class Parsing(unittest.TestCase):
    def test_inputs_line(self):
        line = "Inputs: 500 files (15.9 MiB); sent 501 of 501 blobs (16.0 MiB), 0 already on the master\n"
        p = bench.parse_inputs_line("noise\n" + line)
        self.assertEqual((p["files"], p["blobs_sent"], p["blobs_total"]), (500, 501, 501))
        self.assertEqual(p["blobs_already_on_master"], 0)

    def test_missing_line(self):
        self.assertIsNone(bench.parse_inputs_line("nothing here"))


class Tree(unittest.TestCase):
    def test_tree_is_deterministic(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            ta = bench.make_tree(os.path.join(a, "t"), 12, 5, 300)
            tb = bench.make_tree(os.path.join(b, "t"), 12, 5, 300)
            self.assertEqual(ta, tb)
            self.assertEqual(bench.dir_bytes(os.path.join(a, "t")), ta)
            fa = open(os.path.join(a, "t", "d01", "f0001.bin"), "rb").read()
            fb = open(os.path.join(b, "t", "d01", "f0001.bin"), "rb").read()
            self.assertEqual(fa, fb)


class NoHostname(unittest.TestCase):
    SENTINEL = "SENTINEL-HOST-zq9"

    def test_environment_record_and_file_name_carry_no_host(self):
        with mock.patch.object(socket, "gethostname", return_value=self.SENTINEL), \
             mock.patch.object(bench.platform, "node", return_value=self.SENTINEL):
            hw, osf = bench.hardware(), bench.os_info()
            with tempfile.TemporaryDirectory() as d:
                path = bench.result_path(d, datetime.datetime(2026, 11, 8), osf, hw)
        blob = json.dumps({"hardware": hw, "os": osf, "git": bench.git_info(), "power": bench.power_state()})
        self.assertNotIn(self.SENTINEL, blob)
        self.assertNotIn(self.SENTINEL, path)

    def test_file_name_shape_and_suffix(self):
        hw, osf = {"cpu_model": "Apple M5"}, {"system": "Darwin", "machine": "arm64"}
        with tempfile.TemporaryDirectory() as d:
            p1 = bench.result_path(d, datetime.datetime(2026, 11, 8), osf, hw)
            self.assertEqual(os.path.basename(p1), "2026-11-08-macos-arm64-apple-m5.json")
            open(p1, "w").close()
            p2 = bench.result_path(d, datetime.datetime(2026, 11, 8), osf, hw)
            self.assertEqual(os.path.basename(p2), "2026-11-08-macos-arm64-apple-m5-2.json")


if __name__ == "__main__":
    unittest.main()
