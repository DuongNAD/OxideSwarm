#!/usr/bin/env python3
"""Unit tests for benchmarks/sweep/bench_sweep.py and workload_cpu.py (BENCH-2, design 0093).

Standard library only, no oxideswarm binary: the helpers, the W1 workload and the local half of the
harness. The grid half is exercised by `bench_sweep.py --quick --no-write` against a built binary.
"""

from __future__ import annotations

import io
import json
import os
import platform
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SWEEP_DIR = REPO_ROOT / "benchmarks" / "sweep"
if str(SWEEP_DIR) not in sys.path:
    sys.path.insert(0, str(SWEEP_DIR))

import bench_sweep  # noqa: E402
import workload_cpu  # noqa: E402


class WorkloadTests(unittest.TestCase):
    def test_known_checksum_is_the_same_on_every_system(self) -> None:
        # Fixed vector: SHA-256 chained 1000 times from the state of seed 7.
        self.assertEqual(
            workload_cpu.run(7, 1000),
            "98aaab6eec02b5f265d2a3e6d560f2aaea7b557d78aa991be5ace2cdea358d26",
        )

    def test_zero_units_is_the_initial_state_and_seeds_differ(self) -> None:
        self.assertEqual(workload_cpu.run(3, 0), workload_cpu.initial_state(3).hex())
        self.assertNotEqual(workload_cpu.run(1, 10), workload_cpu.run(2, 10))
        with self.assertRaises(ValueError):
            workload_cpu.run(0, -1)

    def test_result_line_round_trips_through_the_parser(self) -> None:
        line = workload_cpu.result_line(5, 42, workload_cpu.run(5, 42))
        parsed = bench_sweep.parse_w1_line("noise\r\n" + line + "\r\n")
        self.assertEqual(parsed, {"seed": 5, "units": 42, "checksum": workload_cpu.run(5, 42)})
        self.assertIsNone(bench_sweep.parse_w1_line("W1 seed=5 units=42 checksum=xyz"))
        self.assertIsNone(bench_sweep.parse_w1_line(None))

    def test_calibrate_returns_a_positive_unit_count(self) -> None:
        self.assertGreaterEqual(workload_cpu.calibrate(0.01, probe_units=2000), 1)


class HelperTests(unittest.TestCase):
    def test_parallelism_levels_are_one_half_and_all(self) -> None:
        self.assertEqual(bench_sweep.parallelism_levels(20), [1, 10, 20])
        self.assertEqual(bench_sweep.parallelism_levels(10), [1, 5, 10])
        self.assertEqual(bench_sweep.parallelism_levels(3), [1, 3])
        self.assertEqual(bench_sweep.parallelism_levels(1), [1])
        self.assertEqual(bench_sweep.parallelism_levels(None), [1])

    def test_task_seeds_and_throughput(self) -> None:
        self.assertEqual(bench_sweep.task_seeds(3, 10), [10, 11, 12])
        with self.assertRaises(ValueError):
            bench_sweep.task_seeds(0)
        self.assertAlmostEqual(bench_sweep.tasks_per_second(10, 4.0), 2.5)
        with self.assertRaises(ValueError):
            bench_sweep.tasks_per_second(1, 0)

    def test_summarize_and_best_level(self) -> None:
        self.assertEqual(bench_sweep.summarize([3, 1, 2]), {"median": 2, "min": 1, "max": 3, "runs": 3})
        levels = [
            {"parallelism": 1, "tasks_per_s": {"median": 1.0}},
            {"parallelism": 10, "tasks_per_s": {"median": 9.0}},
            {"parallelism": 20, "tasks_per_s": {"median": 9.0}},
        ]
        # A tie goes to fewer processes.
        self.assertEqual(bench_sweep.best_level(levels)["parallelism"], 10)

    def test_scaling_uses_the_fastest_and_the_sum(self) -> None:
        s = bench_sweep.scaling(12.0, [10.0, 5.0])
        self.assertAlmostEqual(s["speedup"], 1.2)
        self.assertAlmostEqual(s["efficiency"], 0.8)
        self.assertEqual(s["fastest_local_tasks_per_s"], 10.0)
        self.assertEqual(s["sum_local_tasks_per_s"], 15.0)
        self.assertEqual(s["machines"], 2)
        with self.assertRaises(ValueError):
            bench_sweep.scaling(1.0, [])

    def test_compare_checksums_counts_matches_mismatches_and_missing(self) -> None:
        local = {0: "a" * 64, 1: "b" * 64, 2: "c" * 64}
        grid = [
            {"seed": 0, "checksum": "a" * 64},
            {"seed": 1, "checksum": "f" * 64},
            {"seed": 2, "checksum": None},
            {"seed": 9, "checksum": "d" * 64},
        ]
        c = bench_sweep.compare_checksums(local, grid)
        self.assertEqual((c["compared"], c["matched"], c["mismatched"], c["missing"]), (4, 1, 1, 2))
        self.assertEqual(c["mismatches"], [{"seed": 1, "local": "b" * 64, "grid": "f" * 64}])
        self.assertEqual(sorted(c["missing_seeds"]), [2, 9])

    def test_worker_facts_and_doctor_carry_no_name_id_or_address(self) -> None:
        hostname = (platform.node() or "my-desktop").lower()
        workers = [
            {"name": hostname, "cpu_cores": 20, "ram_mb": 48941, "has_gpu": False, "link": "Link: TCP/LAN",
             "tags": ["os:windows", "arch:x86_64", "slots:10", "iso:fs", "link:TCP/LAN",
                      "id:38aff0e6-154d-4ee8-b217-ebe1b2880709", "state:connected"]},
            {"name": "mac-mini", "cpu_cores": 10, "tags": ["os:macos", "arch:aarch64", "slots:8",
                                                            "id:11111111-2222-3333-4444-555555555555"]},
        ]
        labels = bench_sweep.worker_labels(workers)
        facts = [bench_sweep.sanitize_worker(w, labels[w["name"]]) for w in workers]
        doctor = {
            "master": "192.168.1.20:18160",
            "workers": [{"name": hostname, "id": "38aff0e6-154d-4ee8-b217-ebe1b2880709", "os": "windows",
                         "arch": "x86_64", "cores": 20, "link": "TCP/LAN", "link_kind": "tcp",
                         "link_rtt_ms": 3.5}],
            "checks": [
                {"id": "C1", "name": "Connect", "worker": None, "outcome": "pass",
                 "detail": "connected to 192.168.1.20:18160 in 2 ms"},
                {"id": "C6", "name": "Spread", "worker": None, "outcome": "pass",
                 "detail": "4 tasks of 500 ms finished in 547 ms on 2 worker(s): %s 2 mac-mini 2" % hostname},
            ],
        }
        sanitized = bench_sweep.sanitize_doctor(doctor, labels)
        text = json.dumps({"facts": facts, "doctor": sanitized}).lower()
        for secret in (hostname, "mac-mini", "38aff0e6", "11111111", "192.168.1.20"):
            self.assertNotIn(secret, text)
        self.assertEqual(facts[0]["slots"], 10 if facts[0]["os"] == "windows" else 8)
        self.assertIn("worker-1", text)
        self.assertEqual(sanitized["workers"][0]["link_rtt_ms"], 3.5)
        self.assertIn("<address>", sanitized["checks"][0]["detail"])

    def test_result_path_and_plan(self) -> None:
        import datetime

        with tempfile.TemporaryDirectory() as d:
            when = datetime.datetime(2026, 11, 18, tzinfo=datetime.timezone.utc)
            p1 = bench_sweep.result_path(d, when, "W1 Windows AMD64 loopback")
            self.assertEqual(os.path.basename(p1), "2026-11-18-w1-windows-amd64-loopback.json")
            Path(p1).write_text("{}")
            p2 = bench_sweep.result_path(d, when, "W1 Windows AMD64 loopback")
            self.assertEqual(os.path.basename(p2), "2026-11-18-w1-windows-amd64-loopback-2.json")
            plan_json = os.path.join(d, "plan.json")
            Path(plan_json).write_text('{"question": "q"}')
            self.assertEqual(bench_sweep.read_plan(plan_json), {"question": "q"})
            plan_txt = os.path.join(d, "plan.txt")
            Path(plan_txt).write_text("free text")
            self.assertEqual(bench_sweep.read_plan(plan_txt), "free text")

    def test_local_record_must_match_workload_and_units(self) -> None:
        sha = bench_sweep.file_sha256(bench_sweep.WORKLOAD)
        rec_v1 = {"record_format": 1,
                  "workload": {"units_per_task": 1000, "sha256": sha},
                  "local": {"best": {"parallelism": 10, "tasks_per_s": {"median": 7.5}}},
                  "machine": {"os": {"system": "Darwin", "machine": "arm64"}, "hardware": {"cores": 10}}}
        rec_v2 = {"record_format": 2,
                  "workload": {"units_per_task": 1000, "sha256": sha},
                  "local": {"best": {"parallelism": 10, "tasks_per_s": {"median": 7.5},
                                     "task_time_mean_s": 1.32, "task_time_median_s": 1.31}},
                  "machine": {"os": {"system": "Darwin", "machine": "arm64"}, "hardware": {"cores": 10}}}
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "mac.json")
            Path(path).write_text(json.dumps(rec_v1))
            got = bench_sweep.load_local_record(path, 1000, sha)
            self.assertEqual((got["best_tasks_per_s"], got["os"], got["cores"]), (7.5, "Darwin", 10))
            self.assertEqual(got["best_parallelism"], 10)
            self.assertIsNone(got["best_task_time_mean_s"])
            self.assertIsNone(got["best_task_time_median_s"])

            Path(path).write_text(json.dumps(rec_v2))
            got_v2 = bench_sweep.load_local_record(path, 1000, sha)
            self.assertEqual(got_v2["best_task_time_mean_s"], 1.32)
            self.assertEqual(got_v2["best_task_time_median_s"], 1.31)

            with self.assertRaises(ValueError):
                bench_sweep.load_local_record(path, 2000, sha)
            with self.assertRaises(ValueError):
                bench_sweep.load_local_record(path, 1000, "0" * 64)

    def test_summarize_task_times_and_local_run(self) -> None:
        self.assertEqual(bench_sweep.summarize_task_times([]), {"tasks": 0, "mean_s": None, "median_s": None})
        self.assertEqual(
            bench_sweep.summarize_task_times([1.0, 2.0, 6.0]),
            {"tasks": 3, "mean_s": 3.0, "median_s": 2.0},
        )
        rows = [
            {"seed": 0, "checksum": "a" * 64, "task_time_s": 1.0},
            {"seed": 1, "checksum": "b" * 64, "task_time_s": 3.0},
            # Failed task excluded from task_time mean/median even if subprocess duration was recorded.
            {"seed": 2, "checksum": None, "task_time_s": 99.0},
        ]
        summary = bench_sweep.summarize_local_run(rows, 2, 4.0)
        self.assertEqual(summary["parallelism"], 2)
        self.assertEqual(summary["tasks"], 3)
        self.assertEqual(summary["wall_s"], 4.0)
        self.assertAlmostEqual(summary["tasks_per_s"], 0.75)
        self.assertEqual(summary["failed_tasks"], 1)
        self.assertEqual(summary["task_time_mean_s"], 2.0)
        self.assertEqual(summary["task_time_median_s"], 2.0)
        with self.assertRaises(ValueError):
            bench_sweep.summarize_local_run([], 1, 1.0)

    def test_summarize_grid_run_and_detail_line(self) -> None:
        rows = [
            {"seed": 0, "checksum": "a" * 64, "worker": "worker-1", "submit_to_result_s": 2.1, "execution_time_ms": 1500},
            {"seed": 1, "checksum": "b" * 64, "worker": "worker-1", "submit_to_result_s": 2.3, "execution_time_ms": 2500},
            # Failed worker row: included in task_time_sum_s & tasks count, excluded from per_worker mean/median.
            {"seed": 2, "checksum": None, "worker": "worker-1", "submit_to_result_s": 0.6, "execution_time_ms": 500},
            {"seed": 3, "checksum": "c" * 64, "worker": "worker-2", "submit_to_result_s": 1.8, "execution_time_ms": 1200},
            {"seed": 4, "checksum": None, "worker": None, "submit_to_result_s": 5.0, "execution_time_ms": None},
        ]
        # total_slots=10 differs from len(rows)=5 (ensures slot_seconds uses total_slots, not concurrency/task count).
        run = bench_sweep.summarize_grid_run(rows, wall_s=4.0, total_slots=10)
        self.assertEqual(run["tasks"], 5)
        self.assertEqual(run["wall_s"], 4.0)
        self.assertAlmostEqual(run["tasks_per_s"], 1.25)
        self.assertEqual(run["failed_tasks"], 2)
        self.assertEqual(run["tasks_per_worker"], {"worker-1": 3, "worker-2": 1, "none": 1})
        self.assertAlmostEqual(run["submit_to_result_p50_s"], 2.1)
        self.assertAlmostEqual(run["submit_to_result_max_s"], 5.0)
        # Sum includes failed worker-1 500 ms slot time: (1.5 + 2.5 + 0.5 + 1.2) = 5.7 s
        self.assertAlmostEqual(run["task_time_sum_s"], 5.7)
        self.assertEqual(run["task_time_missing"], 1)
        self.assertAlmostEqual(run["slot_seconds"], 40.0)
        self.assertAlmostEqual(run["utilization"], 0.1425)
        self.assertEqual(
            run["per_worker"],
            {
                "none": {"tasks": 1, "mean_s": None, "median_s": None},
                "worker-1": {"tasks": 3, "mean_s": 2.0, "median_s": 2.0},
                "worker-2": {"tasks": 1, "mean_s": 1.2, "median_s": 1.2},
            },
        )
        grid = {
            "utilization": bench_sweep.summarize([run["utilization"], 0.17, 0.25]),
            "per_worker": run["per_worker"],
        }
        line = bench_sweep.format_grid_detail_line(grid)
        self.assertEqual(
            line,
            "slot utilization 17.0 % (median of 3); mean task time: none n/a, worker-1 2.000 s, worker-2 1.200 s",
        )

        # When every row lacks execution_time_ms, task_time_missing == len(rows) and utilization is None.
        all_missing = [
            {"seed": 0, "checksum": None, "worker": "worker-1", "submit_to_result_s": 1.0, "execution_time_ms": None},
            {"seed": 1, "checksum": None, "worker": "worker-1", "submit_to_result_s": 2.0, "execution_time_ms": None},
        ]
        missing_run = bench_sweep.summarize_grid_run(all_missing, wall_s=2.0, total_slots=4)
        self.assertEqual(missing_run["task_time_missing"], 2)
        self.assertEqual(missing_run["task_time_sum_s"], 0.0)
        self.assertIsNone(missing_run["utilization"])
        self.assertEqual(
            bench_sweep.format_grid_detail_line({"utilization": None, "per_worker": missing_run["per_worker"]}),
            "slot utilization n/a (median of 0); mean task time: worker-1 n/a",
        )
        with self.assertRaises(ValueError):
            bench_sweep.summarize_grid_run([], 1.0, 2)
        with self.assertRaises(ValueError):
            bench_sweep.summarize_grid_run(rows, 0.0, 2)
        with self.assertRaises(ValueError):
            bench_sweep.summarize_grid_run(rows, 1.0, 0)

    def test_process_sampler_parses_names_only_and_records_sightings(self) -> None:
        unix_ps = "/usr/bin/python3\n/Users/example/.cargo/bin/cargo\nrustc\n/bin/sh\n"
        self.assertEqual(bench_sweep.parse_watched_processes(unix_ps, is_windows=False), ["cargo", "rustc"])

        win_tasklist = (
            '"python.exe","101","Console","1","12,000 K"\r\n'
            '"cargo.exe","202","Console","1","45,000 K"\r\n'
            '"clang.exe","303","Console","1","10,000 K"\r\n'
        )
        self.assertEqual(bench_sweep.parse_watched_processes(win_tasklist, is_windows=True), ["cargo"])

        replies = iter([[], ["cargo"], ["cargo", "rustc"]])
        sampler = bench_sweep.ProcessSampler(
            probe_fn=lambda: next(replies, []),
            now_fn=lambda: "2026-11-18T22:10:00Z",
        )
        sampler._poll_once()
        sampler._poll_once()
        sampler._poll_once()
        summary = sampler.summary()
        self.assertEqual(summary["watched"], ["cargo", "rustc"])
        self.assertEqual(summary["interval_s"], 2.0)
        self.assertEqual(summary["samples"], 3)
        self.assertEqual(summary["detections"], 2)
        self.assertEqual(
            summary["sightings"],
            [
                {"utc": "2026-11-18T22:10:00Z", "processes": ["cargo"]},
                {"utc": "2026-11-18T22:10:00Z", "processes": ["cargo", "rustc"]},
            ],
        )

        deadline = time.monotonic() + 10.0
        with bench_sweep.ProcessSampler(
            interval_s=0.05,
            probe_fn=lambda: ["cargo"],
            now_fn=lambda: "2026-11-18T22:10:00Z",
        ) as live:
            while live.summary()["samples"] < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            live_summary = live.summary()
        self.assertGreaterEqual(live_summary["samples"], 1)
        self.assertGreaterEqual(live_summary["detections"], 1)
        self.assertIsNone(live._thread)


class LocalRunTests(unittest.TestCase):
    def test_local_measurement_runs_every_level_and_records_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            local, sums = bench_sweep.measure_local(sys.executable, [0, 1, 2], 200, [1, 2], 1, d, lambda _m: None)
        self.assertEqual([lvl["parallelism"] for lvl in local["levels"]], [1, 2])
        self.assertEqual(local["failed_tasks"], 0)
        for lvl in local["levels"]:
            self.assertEqual(lvl["warmup"]["tasks"], 3)
            self.assertEqual(len(lvl["runs"]), 1)
            self.assertGreater(lvl["task_time_mean_s"], 0)
            self.assertGreater(lvl["task_time_median_s"], 0)
            self.assertGreater(lvl["runs"][0]["task_time_mean_s"], 0)
            self.assertGreater(lvl["runs"][0]["task_time_median_s"], 0)
            self.assertEqual(lvl["runs"][0]["competing_processes"]["watched"], ["cargo", "rustc"])
            self.assertGreaterEqual(lvl["runs"][0]["competing_processes"]["samples"], 1)
        self.assertEqual(sums, {s: workload_cpu.run(s, 200) for s in (0, 1, 2)})
        self.assertIn(local["best"]["parallelism"], (1, 2))

    def test_local_only_record_needs_no_binary_and_feeds_local_results(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        from contextlib import redirect_stdout

        with tempfile.TemporaryDirectory() as d:
            with redirect_stdout(out), redirect_stderr(err):
                rc = bench_sweep.main(["--local-only", "--no-write", "--tasks", "2", "--units", "100",
                                       "--runs", "1", "--bin", os.path.join(d, "absent")])
            self.assertEqual(rc, 0)
            rec = json.loads(out.getvalue())
            self.assertEqual(rec["record_format"], 2)
            self.assertEqual(rec["cluster"]["mode"], "local only")
            self.assertIsNone(rec["grid"])
            self.assertIsNone(rec["scaling"])
            self.assertEqual(rec["failed_tasks"], 0)
            self.assertEqual(rec["workload"]["units_per_task"], 100)
            self.assertGreater(rec["local"]["best"]["task_time_mean_s"], 0)
            self.assertGreater(rec["local"]["best"]["task_time_median_s"], 0)
            path = os.path.join(d, "local.json")
            Path(path).write_text(json.dumps(rec))
            other = bench_sweep.load_local_record(path, 100, rec["workload"]["sha256"])
            self.assertGreater(other["best_tasks_per_s"], 0)
            self.assertEqual(other["best_task_time_mean_s"], rec["local"]["best"]["task_time_mean_s"])
        self.assertIn("local only", err.getvalue())

    def test_main_refuses_a_record_without_a_plan_and_a_missing_binary(self) -> None:
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                bench_sweep.main(["--bin", "does-not-exist"])
        self.assertIn("--plan", str(cm.exception))
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                bench_sweep.main(["--no-write", "--bin", os.path.join("no", "such", "oxideswarm")])
        self.assertIn("binary not found", str(cm.exception))


class HygieneTests(unittest.TestCase):
    def test_python_files_have_pure_lf_endings(self) -> None:
        for path in (SWEEP_DIR / "bench_sweep.py", SWEEP_DIR / "workload_cpu.py",
                     REPO_ROOT / "tests" / "test_bench_sweep.py"):
            self.assertNotIn(b"\r\n", path.read_bytes(), f"CRLF detected in {path}")


if __name__ == "__main__":
    unittest.main()
