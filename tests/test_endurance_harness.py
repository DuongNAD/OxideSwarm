#!/usr/bin/env python3
"""
tests/test_endurance_harness.py - Unit tests for benchmarks/endurance/endurance.py (TEST-24).

Standard library only; requires no built oxideswarm binary. Tests the seeded generators,
disk space guard, TaskLedger, D2/D3 threshold math, loopback ThrottleProxy against a
local TCP echo server, process/directory samplers, and record redaction/privacy checks.

Run:   python3 tests/test_endurance_harness.py
"""

from __future__ import annotations

import copy
import datetime
import hashlib
import importlib.util
import json
import os
import random
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import urllib.error
import urllib.request
from collections import namedtuple
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "benchmarks", "endurance", "endurance.py")
spec = importlib.util.spec_from_file_location("endurance", SCRIPT)
endurance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(endurance)

Usage = namedtuple("Usage", ["total", "used", "free"])


def _start_echo_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    srv.settimeout(0.2)
    stop = threading.Event()
    conns = []

    def handle(conn):
        try:
            while not stop.is_set():
                try:
                    data = conn.recv(65536)
                except socket.timeout:
                    continue
                if not data:
                    break
                conn.sendall(data)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def serve():
        while not stop.is_set():
            try:
                c, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            c.settimeout(0.5)
            try:
                c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            conns.append(c)
            threading.Thread(target=handle, args=(c,), daemon=True).start()

    t = threading.Thread(target=serve, daemon=True)
    t.start()

    def close():
        stop.set()
        try:
            srv.close()
        except OSError:
            pass
        for c in list(conns):
            try:
                c.close()
            except OSError:
                pass
        t.join(timeout=2.0)

    return srv.getsockname(), close


class SeededGeneratorTests(unittest.TestCase):
    def test_seeded_file_is_deterministic_and_differs_across_seeds(self):
        with tempfile.TemporaryDirectory() as td:
            p1 = os.path.join(td, "f1.bin")
            p2 = os.path.join(td, "f2.bin")
            p3 = os.path.join(td, "f3.bin")
            m1 = endurance.generate_seeded_file(p1, 150000, seed=42, chunk_bytes=32768)
            m2 = endurance.generate_seeded_file(p2, 150000, seed=42, chunk_bytes=32768)
            m3 = endurance.generate_seeded_file(p3, 150000, seed=43, chunk_bytes=32768)
            self.assertEqual(m1, m2)
            self.assertNotEqual(m1["sha256"], m3["sha256"])
            self.assertEqual(endurance.sha256_file(p1), (150000, m1["sha256"]))
            with open(p1, "rb") as f1, open(p2, "rb") as f2:
                self.assertEqual(f1.read(), f2.read())

    def test_seeded_tree_is_deterministic(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            ta = endurance.generate_seeded_tree(os.path.join(a, "tree"), 25, seed=99, shards=5)
            tb = endurance.generate_seeded_tree(os.path.join(b, "tree"), 25, seed=99, shards=5)
            self.assertEqual(ta, tb)
            self.assertEqual(ta["files"], 25)
            self.assertGreaterEqual(ta["total_bytes"], 25 * endurance.KIB)
            self.assertEqual(endurance.dir_bytes(os.path.join(a, "tree")), ta["total_bytes"])


class TaskLedgerTests(unittest.TestCase):
    def test_clean_audit_passes(self):
        ledger = endurance.TaskLedger()
        ledger.record_submit("t1", expected_hash="abc", expected_outcome="ok")
        ledger.record_submit("t2", expected_hash=None, expected_outcome="fail")
        ledger.record_result("t1", actual_hash="abc", outcome="ok")
        ledger.record_result("t2", actual_hash=None, outcome="fail")
        audit = ledger.audit()
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["submitted"], 2)
        self.assertEqual(audit["completed"], 2)

    def test_detects_lost_duplicate_mismatch_and_unexpected_outcome(self):
        ledger = endurance.TaskLedger()
        ledger.record_submit("t_lost", expected_hash="aaa")
        ledger.record_submit("t_dup", expected_hash="bbb")
        ledger.record_submit("t_bad_hash", expected_hash="ccc")
        ledger.record_submit("t_bad_out", expected_outcome="ok")

        ledger.record_result("t_dup", actual_hash="bbb", outcome="ok")
        ledger.record_result("t_dup", actual_hash="bbb", outcome="ok")
        ledger.record_result("t_bad_hash", actual_hash="zzz", outcome="ok")
        ledger.record_result("t_bad_out", outcome="fail")

        audit = ledger.audit()
        self.assertFalse(audit["ok"])
        self.assertEqual(audit["lost_tasks"], ["t_lost"])
        self.assertEqual(audit["duplicate_results"], ["t_dup"])
        self.assertEqual(len(audit["hash_mismatches"]), 1)
        self.assertEqual(audit["hash_mismatches"][0]["task_id"], "t_bad_hash")
        self.assertEqual(len(audit["unexpected_outcomes"]), 1)
        self.assertEqual(audit["unexpected_outcomes"][0]["task_id"], "t_bad_out")


class DiskAndThresholdTests(unittest.TestCase):
    def test_disk_check_formula_and_refusal(self):
        self.assertEqual(endurance.required_free_bytes(8 * endurance.GIB), 34 * endurance.GIB)
        self.assertEqual(endurance.required_free_bytes(3 * endurance.GIB), 19 * endurance.GIB)
        need_3g = endurance.required_free_bytes(3 * endurance.GIB)

        with mock.patch.object(endurance.shutil, "disk_usage", return_value=Usage(100 * endurance.GIB, 82 * endurance.GIB, need_3g - 1)):
            with self.assertRaises(RuntimeError):
                endurance.check_free_space(".", 3 * endurance.GIB)

        with mock.patch.object(endurance.shutil, "disk_usage", return_value=Usage(100 * endurance.GIB, 81 * endurance.GIB, need_3g)):
            res = endurance.check_free_space(".", 3 * endurance.GIB)
            self.assertTrue(res["ok"])
            self.assertEqual(res["required_bytes"], need_3g)

    def test_threshold_arithmetic(self):
        self.assertTrue(endurance.check_t1_memory_flat(100 * endurance.MIB, 228 * endurance.MIB)["passed"])
        self.assertFalse(endurance.check_t1_memory_flat(100 * endurance.MIB, 229 * endurance.MIB)["passed"])

        # Small baseline uses 32 MiB floor; large baseline uses 10%
        self.assertTrue(endurance.check_memory_plateau(100 * endurance.MIB, 132 * endurance.MIB)["passed"])
        self.assertFalse(endurance.check_memory_plateau(100 * endurance.MIB, 133 * endurance.MIB)["passed"])
        self.assertTrue(endurance.check_memory_plateau(500 * endurance.MIB, 550 * endurance.MIB)["passed"])
        self.assertFalse(endurance.check_memory_plateau(500 * endurance.MIB, 551 * endurance.MIB)["passed"])

        self.assertTrue(endurance.check_handle_plateau(200, 220)["passed"])
        self.assertFalse(endurance.check_handle_plateau(200, 221)["passed"])

        self.assertTrue(endurance.check_p99_ratio(10.0, 15.0)["passed"])
        self.assertFalse(endurance.check_p99_ratio(10.0, 15.1)["passed"])

        self.assertEqual(endurance.theoretical_single_blob_bound_mib_per_s(100.0), 40.0)
        self.assertIsNone(endurance.theoretical_single_blob_bound_mib_per_s(0.0))

    def test_parse_inputs_line(self):
        line = "Inputs: 20000 files (165.4 MiB); sent 20001 of 20001 blobs (166.1 MiB), 0 already on the master\n"
        parsed = endurance.parse_inputs_line(line)
        self.assertEqual(parsed["files"], 20000)
        self.assertEqual(parsed["blobs_sent"], 20001)
        self.assertEqual(parsed["blobs_already_on_master"], 0)
        self.assertIsNone(endurance.parse_inputs_line("no match"))


class ThrottleProxyTests(unittest.TestCase):
    def test_rejects_non_loopback_listen_host(self):
        with self.assertRaises(ValueError):
            endurance.ThrottleProxy("127.0.0.1", 8088, listen_host="0.0.0.0")

    def test_token_bucket_preserves_oversleep_and_caps_idle_credit(self):
        clock = [0.0]
        wait_mult = [1.0]

        def fake_monotonic():
            return clock[0]

        class FakeCondition:
            def __init__(self, lock):
                self._lock = lock

            def wait(self, timeout=None):
                clock[0] += (timeout or 0.0) * wait_mult[0]

            def notify_all(self):
                pass

        with (
            mock.patch.object(endurance.time, "monotonic", side_effect=fake_monotonic),
            mock.patch.object(endurance.threading, "Condition", FakeCondition),
        ):
            # 1) Long idle before starting caps idle burst credit at <= 4096 B.
            wait_mult[0] = 3.0
            bucket = endurance.TokenBucket(500_000.0)  # 4.0 Mbit/s
            clock[0] = 60.0
            bucket.consume(4_096)
            self.assertEqual(clock[0], 60.0)
            clock[0] = 120.0
            bucket.consume(5_000)
            self.assertGreater(clock[0], 120.0)

            # 2) Oversleep credit from wait() is preserved for the immediate next active chunk.
            clock[0] = 180.0
            bucket.consume(17_500)
            t_after_first = clock[0]
            clock[0] += 0.001
            bucket.consume(17_500)
            self.assertAlmostEqual(clock[0], t_after_first + 0.001, places=6)

            # 3) Both nominal (1.0x) and oversleep (3.0x) schedules hold +-10% (3.6..4.4 Mbit/s) over 512 KiB.
            total = 512 * endurance.KIB
            for idx, mult in enumerate((1.0, 3.0)):
                wait_mult[0] = mult
                b = endurance.TokenBucket(500_000.0)
                t0 = 300.0 + idx * 100.0
                clock[0] = t0
                rem = total
                while rem > 0:
                    step = min(17_500, rem)
                    b.consume(step)
                    rem -= step
                dt = clock[0] - t0
                mbps = (total * 8.0) / (dt * 1_000_000.0)
                self.assertGreaterEqual(mbps, 3.6, f"mult={mult} too slow: {mbps:.3f} Mbps ({dt:.3f}s)")
                self.assertLessEqual(mbps, 4.4, f"mult={mult} too fast: {mbps:.3f} Mbps ({dt:.3f}s)")

    def test_echo_integrity_rate_accuracy_and_cut_links(self):
        (echo_host, echo_port), close_echo = _start_echo_server()
        proxy = endurance.ThrottleProxy(echo_host, echo_port, rate_mbps=4.0, delay_ms=0.0).start()
        try:
            payload = random.Random(1234).randbytes(512 * endurance.KIB)
            expected_sha = hashlib.sha256(payload).hexdigest()

            sock = socket.create_connection((proxy.listen_host, proxy.port), timeout=15.0)
            sock.settimeout(15.0)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            received = bytearray()

            def _recv_all():
                while len(received) < len(payload):
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    received.extend(chunk)

            rt = threading.Thread(target=_recv_all, daemon=True)
            t0 = time.perf_counter()
            rt.start()
            sock.sendall(payload)
            rt.join(timeout=20.0)
            dt = time.perf_counter() - t0
            sock.close()

            self.assertEqual(len(received), len(payload))
            self.assertEqual(hashlib.sha256(bytes(received)).hexdigest(), expected_sha)
            self.assertEqual(proxy.bytes_c2s, len(payload))
            self.assertEqual(proxy.bytes_s2c, len(payload))
            # 512 KiB = 4.194 Mbit; 4.0 Mbit/s => >= 0.953 s (upper bound immune to runner starvation; see design 0087).
            measured_mbps = (len(payload) * 8.0) / (dt * 1_000_000.0)
            print(f"ThrottleProxy echo: {measured_mbps:.3f} Mbps in {dt:.3f}s")
            self.assertLessEqual(measured_mbps, 4.4, f"rate too fast: {measured_mbps:.3f} Mbps ({dt:.3f}s)")

            # Verify cut_links interrupts an active connection
            s2 = socket.create_connection((proxy.listen_host, proxy.port), timeout=5.0)
            s2.settimeout(3.0)
            try:
                s2.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            s2.sendall(b"ping")
            self.assertEqual(s2.recv(4), b"ping")
            proxy.cut_links(pause_s=0.3)
            data = s2.recv(16)
            self.assertEqual(data, b"")
            s2.close()
        finally:
            proxy.stop()
            close_echo()


class SamplerAndPrivacyTests(unittest.TestCase):
    def test_sample_processes_and_dirs_on_current_process(self):
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "a.bin"), "wb") as f:
                f.write(b"x" * 1234)
            dirs = endurance.sample_dirs({"store": td})
            self.assertEqual(dirs["store"], 1234)

        procs = endurance.sample_processes({"self": os.getpid()})
        self.assertIn("self", procs)
        me = procs["self"]
        self.assertGreater(me["rss_bytes"], 0)
        self.assertGreater(me["handles"], 0)
        self.assertGreater(me["threads"], 0)
        self.assertGreaterEqual(me["cpu_seconds"], 0.0)

    def test_record_privacy_redacts_host_user_path_and_ip(self):
        sentinel_host = "SENTINEL-HOST-ENDURANCE"
        sentinel_user = "sentineluser99"
        private_ip = "192.168.250.42"
        win_path = r"C:\Users\%s\AppData\Local\secret.txt" % sentinel_user

        with mock.patch.object(socket, "gethostname", return_value=sentinel_host), \
             mock.patch.object(endurance.platform, "node", return_value=sentinel_host), \
             mock.patch.dict(os.environ, {"USERNAME": sentinel_user, "USER": sentinel_user}), \
             mock.patch.object(endurance.getpass, "getuser", return_value=sentinel_user):
            hw = endurance.hardware()
            osf = endurance.os_info()
            raw_err = "connect to %s failed from %s (%s)" % (private_ip, win_path, sentinel_host)
            cleaned = endurance.redact_text(raw_err)
            self.assertNotIn(sentinel_host, cleaned)
            self.assertNotIn(sentinel_user, cleaned)
            self.assertNotIn(private_ip, cleaned)
            self.assertNotIn("AppData", cleaned)

            record = {
                "benchmark": "TEST-24",
                "date_utc": "2026-11-19T00:00:00Z",
                "hardware": hw,
                "os": osf,
                "error": cleaned,
            }
            with tempfile.TemporaryDirectory() as d:
                out = endurance.result_path(d, datetime.datetime(2026, 11, 19), "loopback", osf, hw)
                self.assertNotIn(sentinel_host, out)
                write_res = endurance.write_record(out, record)
                self.assertFalse(write_res["quarantined"])
                text = write_res["text"]
                self.assertNotIn(sentinel_host, text)
                self.assertNotIn(sentinel_user, text)
                self.assertNotIn(private_ip, text)

                # Unredacted leak must be rejected by assert_record_private
                with self.assertRaises(ValueError):
                    endurance.assert_record_private(json.dumps({"leak": private_ip}))
                with self.assertRaises(ValueError):
                    endurance.assert_record_private(json.dumps({"leak": sentinel_host}))

    def test_privacy_whole_token_matching_and_quarantine(self):
        # Short tokens like 'ubuntu' or 'max' must NOT match '-Ubuntu' or 'max_cpu_seconds'
        with mock.patch.object(socket, "gethostname", return_value="max-host.local"), \
             mock.patch.object(endurance.platform, "node", return_value="max-host"), \
             mock.patch.dict(os.environ, {"USERNAME": "max", "USER": "ubuntu"}), \
             mock.patch.object(endurance.getpass, "getuser", return_value="ubuntu"):
            benign = {
                "os": {"version": "5.15.0-100-Ubuntu"},
                "summary": {"max_cpu_seconds": 12.5, "max-rate": "ok"},
            }
            with tempfile.TemporaryDirectory() as d:
                target = os.path.join(d, "ok.json")
                res = endurance.write_record(target, benign)
                self.assertFalse(res["quarantined"])
                self.assertEqual(res["hits"], [])
                self.assertTrue(os.path.isfile(target))

                # Actual standalone leak in a nested field goes to <stem>.quarantine.json without raising
                leaked = {
                    "os": {"version": "5.15.0-100-Ubuntu"},
                    "scenarios": [{"id": "T1", "error": "failed on max-host via ubuntu"}],
                }
                qtarget = os.path.join(d, "bad.json")
                qres = endurance.write_record(qtarget, leaked)
                self.assertTrue(qres["quarantined"])
                self.assertFalse(os.path.exists(qtarget))
                self.assertTrue(os.path.isfile(qres["path"]))
                self.assertTrue(qres["path"].endswith(".quarantine.json"))
                self.assertEqual(qres["hits"][0]["field"], "$.scenarios[0].error")

    def test_transfer_refuses_non_loopback_and_remote_flags(self):
        ap = endurance.build_parser()
        ok_args = ap.parse_args(["transfer"])
        endurance.validate_transfer_args(ok_args)

        for bad_argv in (
            ["transfer", "--link", "lan-tcp"],
            ["transfer", "--link", "quic-relay"],
            ["transfer", "--master", "127.0.0.1:8088"],
            ["transfer", "--secret-file", "secret.txt"],
            ["transfer", "--insecure"],
        ):
            bad_args = ap.parse_args(bad_argv)
            with self.assertRaises(ValueError, msg="expected ValueError for %s" % bad_argv):
                endurance.validate_transfer_args(bad_args)


class SoakHarnessTests(unittest.TestCase):
    def test_parse_duration_seconds_and_soak_arg_validation(self):
        self.assertEqual(endurance.parse_duration_seconds("180s"), 180.0)
        self.assertEqual(endurance.parse_duration_seconds("45m"), 2700.0)
        self.assertEqual(endurance.parse_duration_seconds("24h"), 86400.0)
        self.assertAlmostEqual(endurance.parse_duration_seconds("1.5h"), 5400.0)
        self.assertEqual(endurance.parse_duration_seconds("90"), 90.0)
        for bad in ("", "0", "-5m", "10w", "abc"):
            with self.assertRaises(ValueError):
                endurance.parse_duration_seconds(bad)

        ap = endurance.build_parser()
        parsed = endurance.validate_soak_args(ap.parse_args(["soak", "--duration", "45m", "--fault-scale", "0.25"]))
        self.assertEqual(parsed["duration_s"], 2700.0)
        self.assertEqual(parsed["sample_interval_s"], 60.0)
        self.assertEqual(parsed["fault_scale"], 0.25)
        self.assertEqual(parsed["workers"], 3)

        for bad_argv in (
            ["soak", "--link", "lan-tcp"],
            ["soak", "--workers", "1"],
            ["soak", "--workers", "4"],
            ["soak", "--fault-scale", "0"],
            ["soak", "--max-queue-size", "0"],
        ):
            with self.assertRaises(ValueError, msg="expected ValueError for %s" % bad_argv):
                endurance.validate_soak_args(ap.parse_args(bad_argv))

    def test_plan_soak_minute_and_faults_deterministic(self):
        a = endurance.plan_soak_minute(0, seed=42, load_scale=1.0, workspaces=True)
        b = endurance.plan_soak_minute(0, seed=42, load_scale=1.0, workspaces=True)
        c = endurance.plan_soak_minute(1, seed=42, load_scale=1.0, workspaces=True)
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertEqual(len(a), 44)  # 30 + 5 + 5 + 2 + 1 + 1

        counts = {}
        for item in a:
            counts[item["kind"]] = counts.get(item["kind"], 0) + 1
            if item["kind"] == "new_input":
                self.assertGreaterEqual(item["size_bytes"], 10 * endurance.MIB)
                self.assertLessEqual(item["size_bytes"], 100 * endurance.MIB)
            if item["kind"] == "output":
                self.assertEqual(item["size_bytes"], 50 * endurance.MIB)
        self.assertEqual(
            counts,
            {"noop": 30, "new_input": 5, "reused_input": 5, "output": 2, "workspace_sync": 1, "cancel": 1},
        )

        no_ws = endurance.plan_soak_minute(0, seed=42, load_scale=1.0, workspaces=False)
        self.assertEqual(len(no_ws), 43)

        faults_full = endurance.plan_soak_faults(24 * 3600.0, fault_scale=1.0, n_workers=3)
        kinds_full = {f["kind"] for f in faults_full}
        self.assertEqual(kinds_full, {"worker_kill", "link_cut", "queue_burst", "master_restart"})

        faults_45m = endurance.plan_soak_faults(45 * 60.0, fault_scale=0.25, n_workers=3)
        kinds_45m = {}
        for f in faults_45m:
            kinds_45m[f["kind"]] = kinds_45m.get(f["kind"], 0) + 1
        self.assertGreaterEqual(kinds_45m.get("worker_kill", 0), 8)
        self.assertGreaterEqual(kinds_45m.get("link_cut", 0), 5)
        self.assertGreaterEqual(kinds_45m.get("queue_burst", 0), 2)
        self.assertGreaterEqual(kinds_45m.get("master_restart", 0), 1)

    def test_soak_ledger_retries_refusals_and_allowed_outcomes(self):
        self.assertTrue(endurance.is_queue_full_error("Task queue is full (64 queued or running tasks; wait, or raise --max-queue-size)"))
        self.assertFalse(endurance.is_queue_full_error("Failed to connect to Master: Connection refused"))
        self.assertFalse(endurance.is_queue_full_error("[harness] client timed out after 120.0 s"))
        self.assertEqual(endurance.classify_soak_submit_error("queue_burst", "Task queue is full (64 queued)"), "refusal")
        self.assertEqual(endurance.classify_soak_submit_error("queue_burst", "Connection refused", 1), "failure")
        self.assertEqual(endurance.classify_soak_submit_error("noop", "Task queue is full (64 queued)"), "failure")

        lg = endurance.TaskLedger()
        lg.record_submit("t-ok", expected_hash="h1", expected_outcome="ok", kind="new_input")
        lg.record_retry("t-ok", 2)
        lg.set_task_retries("t-ok", 1)  # idempotent max does not downgrade
        self.assertEqual(lg.audit()["retries_total"], 2)
        lg.set_task_retries("t-ok", 3)
        lg.record_result("t-ok", actual_hash="h1", outcome="ok", worker_id="w0")

        lg.record_submit("t-cancel", expected_outcome="cancelled", kind="cancel", allowed_outcomes=("cancelled",))
        lg.record_result("t-cancel", outcome="cancelled", worker_id="w1")
        lg.record_submit_refusal("queue_burst", "Task queue is full (capacity: 64)")

        audit = lg.audit()
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["submitted"], 2)
        self.assertEqual(audit["completed"], 2)
        self.assertEqual(audit["retries_total"], 3)
        self.assertEqual(audit["retried_tasks"], 1)
        self.assertEqual(audit["refusals"], 1)
        self.assertEqual(audit["refusals_by_kind"], {"queue_burst": 1})
        self.assertEqual(audit["refusal_reason_counts"], {"Task queue is full (capacity: 64)": 1})
        self.assertEqual(audit["submit_failures_count"], 0)
        self.assertEqual(audit["by_kind"]["cancel"]["failed_check"], 0)

        # Any unexpected submit failure (e.g. connection refused, timeout rc=124) fails the ledger audit
        lg_bad = endurance.TaskLedger()
        lg_bad.record_submit("t-1", expected_outcome="ok", kind="noop")
        lg_bad.record_result("t-1", outcome="ok")
        lg_bad.record_submit_failure("noop", "Connection refused", rc=1)
        bad_audit = lg_bad.audit()
        self.assertFalse(bad_audit["ok"])
        self.assertEqual(bad_audit["submit_failures_count"], 1)
        self.assertEqual(bad_audit["submit_failures_by_kind"], {"noop": 1})

    def _make_recorded_soak(self, *, hours_24: bool = False):
        if hours_24:
            duration_s = 24 * 3600.0
            times = (
                [0.0]
                + [3600.0 + i * 300.0 for i in range(12)]
                + [82800.0 + i * 300.0 for i in range(12)]
            )
            base_probes = [{"t_s": 3600.0 + i * 240.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
            late_probes = [{"t_s": 82800.0 + i * 240.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
        else:
            duration_s = 2700.0
            times = (
                [0.0]
                + [420.0 + i * 70.0 for i in range(12)]
                + [1900.0 + i * 70.0 for i in range(12)]
            )
            base_probes = [{"t_s": 300.0 + i * 80.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
            late_probes = [{"t_s": 1520.0 + i * 80.0, "latency_ms": 20.0, "ok": True} for i in range(12)]

        samples = [
            {
                "t_s": t,
                "processes": {
                    "master": {"pid": 1001 if t < 43200.0 else 1002, "rss_bytes": 60 * endurance.MIB, "handles": 120},
                    "worker0": {"pid": 2001, "rss_bytes": 40 * endurance.MIB, "handles": 90},
                },
                "dirs": {
                    "blob_store_bytes": 100 * endurance.MIB,
                    "worker_cache_w0_bytes": 50 * endurance.MIB,
                },
            }
            for t in times
        ]
        probes = base_probes + late_probes
        lg = endurance.TaskLedger()
        tid1 = "11111111-1111-1111-1111-111111111111"
        lg.record_submit(tid1, expected_hash="abc", kind="new_input")
        lg.record_result(tid1, actual_hash="abc", outcome="ok")
        master_audit = endurance.audit_master_log_texts(
            ["INFO Task execution result received worker_id=w0 task_id=%s exit_code=0\n" % tid1],
            expected_task_ids=lg.submitted_task_ids(),
        )
        return {
            "ledger_audit": lg.audit(),
            "samples": samples,
            "probes": probes,
            "duration_s": duration_s,
            "blob_store_limit_bytes": 256 * endurance.MIB,
            "worker_cache_limit_bytes": 128 * endurance.MIB,
            "pinned_headroom_bytes": 64 * endurance.MIB,
            "worker_pinned_headroom_bytes": 64 * endurance.MIB,
            "idle_sandbox_entries": {"w0": ["cache", ".locks"], "w1": ["cache", ".locks"]},
            "tmp_leftovers": {"master_blobs_tmp": [], "w0_cache_tmp": [], "w0_locks": [], "w1_locks": []},
            "compactions": [{"before_bytes": 50000, "after_bytes": 24}],
            "leftover_pids": [],
            "leftover_task_trees": [],
            "idle_blob_store_bytes": 100 * endurance.MIB,
            "idle_worker_cache_bytes": {"worker_cache_w0_bytes": 50 * endurance.MIB},
            "master_audit": master_audit,
            "fault_plan": [],
            "faults_executed": [],
            "fault_errors": [],
        }

    def test_evaluate_soak_pass_checks_pass_and_fail_cases(self):
        # Clean 24-hour and clean 45-minute recorded inputs both pass D3 item 6 + fault check
        rec24 = self._make_recorded_soak(hours_24=True)
        res24 = endurance.evaluate_soak_pass_checks(**rec24)
        self.assertTrue(res24["all_passed"], res24)
        self.assertEqual(res24["checks"]["2_resource_plateau"]["mode"], "hour2_vs_last_hour")

        rec45 = self._make_recorded_soak(hours_24=False)
        res45 = endurance.evaluate_soak_pass_checks(**rec45)
        self.assertTrue(res45["all_passed"], res45)
        self.assertEqual(res45["checks"]["2_resource_plateau"]["mode"], "short_run_early_vs_late")

        # 1a) Master duplicate result evidence fails 1_accounting even if ledger recorded once
        bad1 = self._make_recorded_soak(hours_24=False)
        tid = "11111111-1111-1111-1111-111111111111"
        bad1["master_audit"] = endurance.audit_master_log_texts(
            [
                "INFO Task execution result received worker_id=w0 task_id=%s exit_code=0\n"
                "INFO Task execution result received worker_id=w1 task_id=%s exit_code=0\n" % (tid, tid)
            ],
            expected_task_ids={tid},
        )
        r1 = endurance.evaluate_soak_pass_checks(**bad1)
        self.assertFalse(r1["all_passed"])
        self.assertFalse(r1["checks"]["1_accounting"]["passed"])
        self.assertEqual(r1["checks"]["1_accounting"]["master_duplicate_executions"], 1)

        # 1b) unique_result_tasks != submitted fails 1_accounting
        bad1b = self._make_recorded_soak(hours_24=False)
        bad1b["master_audit"] = endurance.audit_master_log_texts([], expected_task_ids={tid})
        r1b = endurance.evaluate_soak_pass_checks(**bad1b)
        self.assertFalse(r1b["checks"]["1_accounting"]["passed"])
        self.assertFalse(r1b["all_passed"])

        # 1c) missing master evidence fails 1_accounting
        bad1c = self._make_recorded_soak(hours_24=False)
        bad1c["master_audit"] = None
        r1c = endurance.evaluate_soak_pass_checks(**bad1c)
        self.assertFalse(r1c["checks"]["1_accounting"]["passed"])

        # 2) Last-hour memory or handle drift fails 2_resource_plateau
        bad2 = self._make_recorded_soak(hours_24=True)
        for s in bad2["samples"]:
            if s["t_s"] >= 82800.0:
                s["processes"]["master"]["handles"] = 160
        r2 = endurance.evaluate_soak_pass_checks(**bad2)
        self.assertFalse(r2["checks"]["2_resource_plateau"]["passed"])

        # 3a) Non-shrinking journal fails 3_disk_and_journal
        bad3a = self._make_recorded_soak(hours_24=False)
        bad3a["compactions"] = [{"before_bytes": 50000, "after_bytes": 50000}]
        r3a = endurance.evaluate_soak_pass_checks(**bad3a)
        self.assertFalse(r3a["checks"]["3_disk_and_journal"]["journal_compaction_ok"])
        self.assertFalse(r3a["checks"]["3_disk_and_journal"]["passed"])

        # 3b) Leftover file/dir beside cache/ and .locks/ in idle sandbox or stray .lock fails 3_disk_and_journal
        bad3b = self._make_recorded_soak(hours_24=False)
        bad3b["idle_sandbox_entries"]["w0"] = ["cache", ".locks", "orphan-task-dir"]
        r3b = endurance.evaluate_soak_pass_checks(**bad3b)
        self.assertFalse(r3b["checks"]["3_disk_and_journal"]["sandbox_ok"])
        self.assertFalse(r3b["checks"]["3_disk_and_journal"]["passed"])

        bad3c = self._make_recorded_soak(hours_24=False)
        bad3c["tmp_leftovers"]["w0_locks"] = ["stray.lock"]
        r3c = endurance.evaluate_soak_pass_checks(**bad3c)
        self.assertFalse(r3c["checks"]["3_disk_and_journal"]["sandbox_ok"])
        self.assertFalse(r3c["checks"]["3_disk_and_journal"]["passed"])

        # 3d) Mid-run worker cache exceeding limit + pinned headroom fails 3_disk_and_journal
        bad3d = self._make_recorded_soak(hours_24=False)
        bad3d["samples"][4]["dirs"]["worker_cache_w0_bytes"] = 200 * endurance.MIB  # > 128 + 64 MiB
        r3d = endurance.evaluate_soak_pass_checks(**bad3d)
        self.assertFalse(r3d["checks"]["3_disk_and_journal"]["worker_cache_peak_ok"])
        self.assertFalse(r3d["checks"]["3_disk_and_journal"]["passed"])

        # 4) Probe p99 ratio > 1.5x fails 4_probe_p99; fault-window probe is excluded from baseline
        bad4 = self._make_recorded_soak(hours_24=False)
        bad4["fault_plan"] = [{"kind": "queue_burst", "seq": 0, "worker_index": None, "at_s": 900.0, "down_s": 0.0}]
        bad4["faults_executed"] = [{"kind": "queue_burst", "seq": 0, "worker_index": None, "t_s": 900.0, "end_s": 905.0, "ok": True}]
        # Huge spike during fault window at t=902 does NOT hide a late slowdown at t=2400
        bad4["probes"].append({"t_s": 902.0, "latency_ms": 11098.0, "ok": True})
        bad4["probes"].append({"t_s": 2400.0, "latency_ms": 40.0, "ok": True})
        r4 = endurance.evaluate_soak_pass_checks(**bad4)
        self.assertEqual(r4["checks"]["4_probe_p99"]["excluded_fault_probes"], 1)
        self.assertFalse(r4["checks"]["4_probe_p99"]["passed"])

        # 4b) Too few probes in a window fails 4_probe_p99 with insufficient_probes_per_window
        bad4b = self._make_recorded_soak(hours_24=False)
        bad4b["probes"] = bad4b["probes"][:4] + bad4b["probes"][-12:]
        r4b = endurance.evaluate_soak_pass_checks(**bad4b)
        self.assertFalse(r4b["checks"]["4_probe_p99"]["passed"])
        self.assertEqual(r4b["checks"]["4_probe_p99"]["reason"], "insufficient_probes_per_window")

        # 5) Leftover PID or escaped task process tree fails 5_clean_stop
        bad5 = self._make_recorded_soak(hours_24=False)
        bad5["leftover_pids"] = [999999]
        r5 = endurance.evaluate_soak_pass_checks(**bad5)
        self.assertFalse(r5["checks"]["5_clean_stop"]["passed"])
        self.assertFalse(r5["all_passed"])

        bad5b = self._make_recorded_soak(hours_24=False)
        bad5b["leftover_task_trees"] = [{"pid": 55555, "ppid": 2001, "reason": "descendant_of_started_pid", "cmd": "python"}]
        r5b = endurance.evaluate_soak_pass_checks(**bad5b)
        self.assertFalse(r5b["checks"]["5_clean_stop"]["passed"])
        self.assertFalse(r5b["all_passed"])

        # 6) Missing or errored fault fails 6_fault_injection
        bad6 = self._make_recorded_soak(hours_24=False)
        bad6["fault_plan"] = [{"kind": "worker_kill", "seq": 0, "worker_index": 0, "at_s": 300.0, "down_s": 15.0}]
        bad6["faults_executed"] = []
        bad6["fault_errors"] = [{"kind": "worker_kill", "seq": 0, "error": "kill failed"}]
        r6 = endurance.evaluate_soak_pass_checks(**bad6)
        self.assertFalse(r6["checks"]["6_fault_injection"]["passed"])
        self.assertFalse(r6["all_passed"])

    def test_audit_master_log_texts_ansi_real_info_lines_and_task_api_retries(self):
        # INFO-only logs parse real Rust messages and explicitly mark debug stale/late as unmeasured
        t_a = "aaaaaaaa-1111-2222-3333-444444444444"
        t_b = "bbbbbbbb-1111-2222-3333-444444444444"
        gen0_info = "\n".join(
            [
                "\x1b[32mINFO\x1b[0m Task execution result received \x1b[2mworker_id=\x1b[0mw0 \x1b[2mtask_id=\x1b[0m%s exit_code=0" % t_a,
                "\x1b[33mWARN\x1b[0m Worker disconnected; executing failover sweep worker_id=w0",
                "\x1b[32mINFO\x1b[0m Re-enqueued orphaned tasks for reassignment worker_id=w0 count=1 tasks=[TaskId(%s)]" % t_a,
                "\x1b[32mINFO\x1b[0m Reaper triggered task failover reassignment count=2",
                "\x1b[32mINFO\x1b[0m \x1b[2mtask_id=\x1b[0m%s Task execution result received exit_code=0" % t_a,
            ]
        )
        gen1_info = "\x1b[32mINFO\x1b[0m Task execution result received task_id=%s exit_code=0\n" % t_b
        audit = endurance.audit_master_log_texts([gen0_info, gen1_info], expected_task_ids={t_a, t_b})
        self.assertEqual(audit["master_generations"], 2)
        self.assertEqual(audit["result_messages_logged"], 3)
        self.assertEqual(audit["unique_result_tasks"], 2)
        self.assertEqual(audit["multi_result_tasks"], {t_a: 2})
        self.assertEqual(audit["failover_sweeps"], 1)
        self.assertEqual(audit["orphan_requeues"], 1)
        self.assertEqual(audit["reaper_failovers"], 2)
        self.assertEqual(audit["requeues_logged"], 3)
        self.assertEqual(audit["requeued_task_ids"], [t_a])
        self.assertFalse(audit["stale_late_results_measured"])
        self.assertIsNone(audit["stale_results_ignored"])
        self.assertIsNone(audit["late_results_ignored"])
        self.assertEqual(audit["missing_from_master_results"], [])
        self.assertEqual(audit["untracked_master_results"], [])

        # When DEBUG lines are present, stale/late ignored counters are measured
        dbg = "\n".join(
            [
                "DEBUG Ignored result from a worker/session that does not own the task",
                "DEBUG Ignoring result for terminal task",
            ]
        )
        dbg_audit = endurance.audit_master_log_texts([dbg])
        self.assertTrue(dbg_audit["stale_late_results_measured"])
        self.assertEqual(dbg_audit["stale_results_ignored"], 1)
        self.assertEqual(dbg_audit["late_results_ignored"], 1)

        # /api/tasks telemetry feeds TaskLedger.retries_total
        lg = endurance.TaskLedger()
        lg.record_submit(t_a, expected_outcome="ok")
        lg.record_result(t_a, outcome="ok")
        stats = endurance.apply_master_task_retries_to_ledger(
            lg,
            {t_a: {"retry_count": 2, "failover_count": 2, "evacuation_count": 0}, t_b: {"retry_count": 0, "failover_count": 0, "evacuation_count": 0}},
        )
        self.assertEqual(stats["source"], "master_task_api")
        self.assertEqual(stats["retries_total"], 2)
        self.assertEqual(lg.audit()["retries_total"], 2)
        self.assertEqual(lg.audit()["retried_tasks"], 1)

    def test_process_lifetimes_and_leftover_process_tree_detection(self):
        samples = [
            {"t_s": 0.0, "processes": {"master": {"pid": 100, "rss_bytes": 20 * endurance.MIB, "handles": 150}}},
            {"t_s": 60.0, "processes": {"master": {"pid": 100, "rss_bytes": 28 * endurance.MIB, "handles": 160}}},
            {"t_s": 120.0, "processes": {"master": {"pid": 200, "rss_bytes": 18 * endurance.MIB, "handles": 140}}},
            {"t_s": 180.0, "processes": {"master": {"pid": 200, "rss_bytes": 30 * endurance.MIB, "handles": 155}}},
        ]
        lifetimes = endurance.summarize_process_lifetimes(samples)
        self.assertEqual(len(lifetimes["master"]), 2)
        self.assertEqual(lifetimes["master"][0]["pid"], 100)
        self.assertEqual(lifetimes["master"][0]["rss_delta_bytes"], 8 * endurance.MIB)
        self.assertEqual(lifetimes["master"][1]["pid"], 200)
        self.assertEqual(lifetimes["master"][1]["rss_delta_bytes"], 12 * endurance.MIB)

        proc_table = [
            {"pid": 5000, "ppid": 2001, "cmd": "python.exe sleep.py", "created_epoch": 1050.0},
            {"pid": 5001, "ppid": 5000, "cmd": "subworker.exe", "created_epoch": 1051.0},
            {"pid": 5002, "ppid": 999, "cmd": r"python.exe C:\ox_tmp\soak\run1\worker.py", "created_epoch": 1052.0},
            {"pid": 6000, "ppid": 2001, "cmd": "unrelated-reused-pid.exe", "created_epoch": 500.0},
            {"pid": 7000, "ppid": 1, "cmd": "oxideswarm.exe usb-trial-worker", "created_epoch": 100.0},
        ]
        leftovers = endurance.find_leftover_process_trees(
            proc_table,
            root_pids={2001},
            workdir=r"C:\ox_tmp\soak\run1",
            ignore_pids={99999},
            min_created_epoch=1000.0,
        )
        self.assertEqual([x["pid"] for x in leftovers], [5000, 5001, 5002])

        # With per-PID lifetime windows: OS PID reuse after an exited client/worker does NOT false-positive,
        # while a real task child spawned BEFORE worker exit IS still caught alongside un-exited roots.
        reused_table = [
            {"pid": 3001, "ppid": 1, "cmd": "chrome.exe", "created_epoch": 2000.0},
            {"pid": 3002, "ppid": 3001, "cmd": "chrome-helper.exe", "created_epoch": 2001.0},
            {"pid": 4001, "ppid": 1, "cmd": "unrelated-reused-after-worker-kill.exe", "created_epoch": 2500.0},
            {"pid": 4002, "ppid": 4001, "cmd": "sleep-escaped-during-worker-life.exe", "created_epoch": 1400.0},
            {"pid": 4003, "ppid": 4002, "cmd": "grandchild.exe", "created_epoch": 1401.0},
            {"pid": 4004, "ppid": 4001, "cmd": "unrelated-child-after-reuse.exe", "created_epoch": 2505.0},
            {"pid": 5005, "ppid": 1, "cmd": "oxideswarm.exe still-alive", "created_epoch": 1200.0},
        ]
        pid_windows = {
            3001: [{"start_epoch": 1100.0, "end_epoch": 1101.0}],  # exited client, later reused by Chrome
            4001: [{"start_epoch": 1000.0, "end_epoch": 1500.0}],  # killed worker whose child 4002 escaped at 1400s
            5005: [{"start_epoch": 1200.0, "end_epoch": None}],    # worker never exited cleanly
        }
        windowed = endurance.find_leftover_process_trees(
            reused_table,
            pid_windows.keys(),
            workdir=r"C:\ox_tmp\soak\run1",
            ignore_pids={99999},
            min_created_epoch=1000.0,
            root_pid_windows=pid_windows,
        )
        self.assertEqual([x["pid"] for x in windowed], [4002, 4003, 5005])

        posix_table = [
            {"pid": 8001, "ppid": 1, "cmd": "python3 /tmp/ox_soak/run1/worker.py", "created_epoch": 1050.0},
            {"pid": 8002, "ppid": 1, "cmd": r"python3 /tmp/ox_soak\run1\child.py", "created_epoch": 1051.0},
            {"pid": 8003, "ppid": 1, "cmd": "python3 /tmp/ox_soak/other/worker.py", "created_epoch": 1052.0},
            {"pid": 8004, "ppid": 1, "cmd": "python3 /TMP/OX_SOAK/RUN1/worker.py", "created_epoch": 1053.0},
        ]
        posix_leftovers = endurance.find_leftover_process_trees(
            posix_table,
            root_pids=set(),
            workdir="/tmp/ox_soak/run1",
            ignore_pids=set(),
            min_created_epoch=1000.0,
        )
        expected_posix = [8001, 8002, 8004] if endurance.IS_WINDOWS else [8001, 8002]
        self.assertEqual([x["pid"] for x in posix_leftovers], expected_posix)

        with mock.patch.object(endurance, "IS_WINDOWS", False):
            posix_mode_win_wd = endurance.find_leftover_process_trees(
                proc_table,
                root_pids={2001},
                workdir=r"C:\ox_tmp\soak\run1",
                ignore_pids={99999},
                min_created_epoch=1000.0,
            )
            self.assertEqual([x["pid"] for x in posix_mode_win_wd], [5000, 5001, 5002])
            posix_mode_posix_wd = endurance.find_leftover_process_trees(
                posix_table,
                root_pids=set(),
                workdir="/tmp/ox_soak/run1",
                ignore_pids=set(),
                min_created_epoch=1000.0,
            )
            self.assertEqual([x["pid"] for x in posix_mode_posix_wd], [8001, 8002])

        with mock.patch.object(endurance, "IS_WINDOWS", True):
            win_mode_posix_wd = endurance.find_leftover_process_trees(
                posix_table,
                root_pids=set(),
                workdir="/tmp/ox_soak/run1",
                ignore_pids=set(),
                min_created_epoch=1000.0,
            )
            self.assertEqual([x["pid"] for x in win_mode_posix_wd], [8001, 8002, 8004])


class RemoteSoakCryptoAndValidationTests(unittest.TestCase):
    @staticmethod
    def _mock_sh_for_fake_bin(fake_bin: str):
        orig_sh = endurance.sh
        abs_bin = os.path.abspath(fake_bin)

        def _fake_sh(cmd: List[str], timeout: float = 10.0) -> str:
            if cmd and os.path.abspath(str(cmd[0])) == abs_bin and "--version" in cmd[1:]:
                return "rusty-grid 0.4.4"
            return orig_sh(cmd, timeout=timeout)

        return _fake_sh

    def test_secret_loading_and_control_auth_round_trip_and_rejections(self):
        rs = endurance.remote_soak
        for bad_name in ("", ".", "..", "win a", "../evil", "a/b", "x" * 33):
            with self.assertRaises(ValueError):
                rs.validate_worker_name(bad_name)
        for good_name in ("win-a", "mac_1.box", "w0"):
            self.assertEqual(rs.validate_worker_name(good_name), good_name)

        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "missing.secret")
            with self.assertRaises(ValueError):
                rs.load_secret_file(missing)
            short = os.path.join(tmp, "short.secret")
            with open(short, "w", encoding="utf-8") as f:
                f.write("ab" * 8 + "\n")
            with self.assertRaises(ValueError):
                rs.load_secret_file(short)
            for bad_txt in ("# only comment\n\n", "z" * 32 + "\n", "a" * 33 + "\n", "ab" * 257 + "\n"):
                bad_p = os.path.join(tmp, "bad.secret")
                with open(bad_p, "w", encoding="utf-8") as f:
                    f.write(bad_txt)
                with self.assertRaises(ValueError):
                    rs.load_secret_file(bad_p)

            sec_path = os.path.join(tmp, "run.secret")
            secret_text = secrets.token_hex(32)
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write("# cluster run secret comment\n\n  %s  \nsecond-line-ignored\n" % secret_text.upper())
            if os.name != "nt":
                os.chmod(sec_path, 0o600)
            loaded_path, loaded_secret = rs.load_secret_file(sec_path)
            self.assertEqual(loaded_secret, secret_text)
            self.assertEqual(os.path.abspath(sec_path), loaded_path)

            # Permission warning when secret file is readable by group/others
            fake_stat = types.SimpleNamespace(st_mode=0o100644)
            with mock.patch.object(rs.os, "name", "posix"), mock.patch.object(rs.os, "stat", return_value=fake_stat):
                with self.assertWarns(UserWarning):
                    rs.load_secret_file(sec_path)

        key_secret = secrets.token_hex(32)
        wrong_secret = secrets.token_hex(32)
        while wrong_secret == key_secret:
            wrong_secret = secrets.token_hex(32)
        key = rs.derive_control_key(key_secret)
        wrong_key = rs.derive_control_key(wrong_secret)
        expected_settings = {
            "slots": 4,
            "blob_cache_mb": 2048,
            "workspaces": True,
            "rate_mbps": None,
            "delay_ms": 0.0,
        }
        srv = rs.SoakControlServer(
            "127.0.0.1:0",
            key,
            expected_workers=["win-a", "win-b"],
            expected_count=2,
            sample_interval_s=5.0,
            worker_settings=expected_settings,
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            url = "http://%s" % srv.connect_addr

            def raw_post(path, body_bytes, headers):
                req = urllib.request.Request(url + path, data=body_bytes, method="POST", headers=headers)
                try:
                    with opener.open(req, timeout=5.0) as resp:
                        return resp.status, dict(resp.headers), resp.read()
                except urllib.error.HTTPError as exc:
                    return exc.code, dict(exc.headers or {}), exc.read() if exc.fp else b""

            reg_a_payload = {"name": "win-a", "instance_id": "inst-a", "settings": dict(expected_settings)}
            body = json.dumps(reg_a_payload).encode("utf-8")
            # Missing auth headers -> 401
            st, _h, _r = raw_post("/v1/register", body, {"Content-Type": "application/json"})
            self.assertEqual(st, 401)

            # Wrong secret key -> 401 and must NOT burn nonce in ReplayGuard
            ts_str = "%.3f" % time.time()
            reusable_nonce = "nonce1234567890"
            bad_sig = rs.sign_request(wrong_key, "POST", "/v1/register", "win-a", ts_str, reusable_nonce, body)
            st, _h, _r = raw_post(
                "/v1/register",
                body,
                {
                    "Content-Type": "application/json",
                    "X-Soak-Worker": "win-a",
                    "X-Soak-Ts": ts_str,
                    "X-Soak-Nonce": reusable_nonce,
                    "X-Soak-Sig": bad_sig,
                },
            )
            self.assertEqual(st, 401)

            # Tampered body under real signature -> 401
            good_sig = rs.sign_request(key, "POST", "/v1/register", "win-a", ts_str, reusable_nonce, body)
            st, _h, _r = raw_post(
                "/v1/register",
                b'{"name":"win-a","instance_id":"inst-a","tampered":true}',
                {
                    "Content-Type": "application/json",
                    "X-Soak-Worker": "win-a",
                    "X-Soak-Ts": ts_str,
                    "X-Soak-Nonce": reusable_nonce,
                    "X-Soak-Sig": good_sig,
                },
            )
            self.assertEqual(st, 401)

            # Short nonce -> 401
            short_sig = rs.sign_request(key, "POST", "/v1/register", "win-a", ts_str, "tiny", body)
            st, _h, _r = raw_post(
                "/v1/register",
                body,
                {
                    "Content-Type": "application/json",
                    "X-Soak-Worker": "win-a",
                    "X-Soak-Ts": ts_str,
                    "X-Soak-Nonce": "tiny",
                    "X-Soak-Sig": short_sig,
                },
            )
            self.assertEqual(st, 401)

            # Expired timestamp -> 401
            old_ts = "%.3f" % (time.time() - 400.0)
            old_nonce = "oldnonce12345678"
            old_sig = rs.sign_request(key, "POST", "/v1/register", "win-a", old_ts, old_nonce, body)
            st, _h, _r = raw_post(
                "/v1/register",
                body,
                {
                    "Content-Type": "application/json",
                    "X-Soak-Worker": "win-a",
                    "X-Soak-Ts": old_ts,
                    "X-Soak-Nonce": old_nonce,
                    "X-Soak-Sig": old_sig,
                },
            )
            self.assertEqual(st, 401)

            # Valid request with reusable_nonce succeeds -> 200, replay of SAME nonce -> 401
            st, _h, resp_raw = raw_post(
                "/v1/register",
                body,
                {
                    "Content-Type": "application/json",
                    "X-Soak-Worker": "win-a",
                    "X-Soak-Ts": ts_str,
                    "X-Soak-Nonce": reusable_nonce,
                    "X-Soak-Sig": good_sig,
                },
            )
            self.assertEqual(st, 200)
            reg_json = json.loads(resp_raw.decode("utf-8"))
            self.assertEqual(reg_json["worker_name"], "win-a")

            st, _h, _r = raw_post(
                "/v1/register",
                body,
                {
                    "Content-Type": "application/json",
                    "X-Soak-Worker": "win-a",
                    "X-Soak-Ts": ts_str,
                    "X-Soak-Nonce": reusable_nonce,
                    "X-Soak-Sig": good_sig,
                },
            )
            self.assertEqual(st, 401)

            # Duplicate live instance with different instance_id -> 403; same instance_id -> 200
            client_a = rs.SignedControlClient(url, "win-a", key)
            st, _ = client_a.post(
                "/v1/register",
                {"name": "win-a", "instance_id": "inst-a-dupe", "settings": dict(expected_settings)},
            )
            self.assertEqual(st, 403)
            st, _ = client_a.post(
                "/v1/register",
                {"name": "win-a", "instance_id": "inst-a", "settings": dict(expected_settings)},
            )
            self.assertEqual(st, 200)

            # Unknown worker in expected_workers allowlist -> 403
            rogue_client = rs.SignedControlClient(url, "rogue", key)
            st, _ = rogue_client.post(
                "/v1/register",
                {"name": "rogue", "instance_id": "inst-r", "settings": dict(expected_settings)},
            )
            self.assertEqual(st, 403)

            # Mismatched worker settings -> 400
            client_b = rs.SignedControlClient(url, "win-b", key)
            bad_settings = dict(expected_settings)
            bad_settings["blob_cache_mb"] = 1024
            st, reg_bad = client_b.post(
                "/v1/register",
                {"name": "win-b", "instance_id": "inst-b", "settings": bad_settings},
            )
            self.assertEqual(st, 400)
            self.assertIn("settings", reg_bad.get("error", ""))

            # Valid win-b register -> 200 and disallowed op in /v1/ack -> 400
            st, reg_b = client_b.post(
                "/v1/register",
                {"name": "win-b", "instance_id": "inst-b", "settings": dict(expected_settings)},
            )
            self.assertEqual(st, 200)
            self.assertEqual(srv.wait_for_agents(2, timeout_s=5.0), ["win-a", "win-b"])
            st, _ = client_b.post(
                "/v1/ack",
                {"run_id": reg_b["run_id"], "cmd_id": "cmd1", "op": "exec_shell", "ok": True},
            )
            self.assertEqual(st, 400)

            # Client rejects tampered response signature (including req_nonce and run_id binding)
            self.assertFalse(
                rs.verify_response(
                    key,
                    200,
                    "/v1/poll",
                    "win-b",
                    "1700000000.0",
                    "respnonce1234",
                    b"{}",
                    "0" * 64,
                    "reqnonce1234",
                    reg_b["run_id"],
                )
            )
            self.assertTrue(
                srv._replay is not None and srv._replay.check_and_record("freshnonce1234", time.time())
            )
            self.assertFalse(srv._replay.check_and_record("freshnonce1234", time.time()))
        finally:
            srv.stop()

    def test_remote_soak_arg_validation_and_sample_merge(self):
        ap = endurance.build_parser()
        with tempfile.TemporaryDirectory() as tmp:
            sec_path = os.path.join(tmp, "soak.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32) + "\n")

            parsed = endurance.validate_soak_args(
                ap.parse_args(
                    [
                        "soak",
                        "--master-only",
                        "--link",
                        "lan-tcp",
                        "--secret-file",
                        sec_path,
                        "--workers",
                        "2",
                        "--expect-workers",
                        "win-a,win-b",
                        "--duration",
                        "24h",
                    ]
                )
            )
            self.assertTrue(parsed["master_only"])
            self.assertEqual(parsed["link"], "lan-tcp")
            self.assertEqual(parsed["expect_workers"], ["win-a", "win-b"])

            for bad in (
                ["soak", "--master-only", "--link", "lan-tcp"],
                ["soak", "--master-only", "--secret-file", sec_path, "--workers", "2", "--expect-workers", "win-a"],
                ["soak", "--master-only", "--secret-file", sec_path, "--workers", "2", "--expect-workers", "win-a,win-a"],
                ["soak", "--master-only", "--secret-file", sec_path, "--workers", "2", "--link", "lan-quic"],
                ["soak", "--master-only", "--secret-file", sec_path, "--workers", "2", "--link", "quic-relay"],
                ["soak", "--master-only", "--secret-file", sec_path, "--workers", "2", "--link", "bogus"],
            ):
                with self.assertRaises(ValueError, msg="expected ValueError for %s" % bad):
                    endurance.validate_soak_args(ap.parse_args(bad))

            wa_ok = endurance.validate_worker_agent_args(
                ap.parse_args(
                    [
                        "worker-agent",
                        "--master",
                        "127.0.0.1:18500",
                        "--control",
                        "127.0.0.1:18501",
                        "--secret-file",
                        sec_path,
                        "--name",
                        "win-a",
                    ]
                )
            )
            self.assertEqual(wa_ok["name"], "win-a")
            self.assertEqual(wa_ok["orphan_timeout_s"], 900.0)

            wa_ot = endurance.validate_worker_agent_args(
                ap.parse_args(
                    [
                        "worker-agent",
                        "--master",
                        "127.0.0.1:18500",
                        "--control",
                        "127.0.0.1:18501",
                        "--secret-file",
                        sec_path,
                        "--name",
                        "win-a",
                        "--orphan-timeout",
                        "2m",
                    ]
                )
            )
            self.assertEqual(wa_ot["orphan_timeout_s"], 120.0)

            for bad_wa in (
                ["worker-agent", "--master", "127.0.0.1:18500", "--control", "127.0.0.1:18501", "--secret-file", sec_path, "--name", "../bad"],
                ["worker-agent", "--master", "127.0.0.1:18500", "--control", "127.0.0.1:18501", "--secret-file", sec_path, "--name", "win-a", "--poll-interval", "0"],
                ["worker-agent", "--master", "127.0.0.1:18500", "--control", "127.0.0.1:18501", "--secret-file", sec_path, "--name", "win-a", "--orphan-timeout", "0"],
                ["worker-agent", "--master", "127.0.0.1:18500", "--control", "127.0.0.1:18501", "--secret-file", sec_path, "--name", "win-a", "--orphan-timeout=-1m"],
            ):
                with self.assertRaises(ValueError):
                    endurance.validate_worker_agent_args(ap.parse_args(bad_wa))

        master_samples = [
            {"t_s": 0.0, "processes": {"master": {"pid": 1, "rss_bytes": 100, "handles": 10}}, "dirs": {"blob_store_bytes": 10}},
            {"t_s": 60.0, "processes": {"master": {"pid": 1, "rss_bytes": 100, "handles": 10}}, "dirs": {"blob_store_bytes": 20}},
        ]
        remote_samples = {
            "win-a": [
                {"t_s": 1.2, "processes": {"worker_win-a": {"pid": 2, "rss_bytes": 200, "handles": 20}}, "dirs": {"worker_cache_win-a_bytes": 50}},
                {"t_s": 95.0, "processes": {"worker_win-a": {"pid": 2, "rss_bytes": 210, "handles": 21}}, "dirs": {"worker_cache_win-a_bytes": 60}},
            ],
            "win-b": [
                {"t_s": 59.5, "processes": {"worker_win-b": {"pid": 3, "rss_bytes": 300, "handles": 30}}, "dirs": {"worker_cache_win-b_bytes": 70}},
            ],
        }
        merged = endurance.remote_soak.merge_remote_samples(master_samples, remote_samples, sample_interval_s=60.0)
        self.assertEqual([r["t_s"] for r in merged], [0.0, 60.0, 95.0])
        self.assertIn("worker_win-a", merged[0]["processes"])
        self.assertIn("worker_win-b", merged[1]["processes"])
        disk_res = endurance.check_soak_disk_and_journal(
            merged,
            blob_store_limit_bytes=1000,
            worker_cache_limit_bytes=100,
            pinned_headroom_bytes=100,
            idle_sandbox_entries={"win-a": ["cache"], "win-b": ["cache"]},
            tmp_leftovers={"master_blobs_tmp": []},
            compactions=[{"before_bytes": 200, "after_bytes": 24}],
            idle_blob_store_bytes=20,
            idle_worker_cache_bytes={"worker_cache_win-a_bytes": 60, "worker_cache_win-b_bytes": 70},
        )
        self.assertTrue(disk_res["passed"])

    def test_is_worker_entry_connected_accepts_busy_and_rejects_disconnected(self):
        fn = endurance.is_worker_entry_connected
        self.assertTrue(fn({"name": "win-a", "tags": ["id:1", "state:connected"]}))
        self.assertTrue(fn({"name": "win-a", "tags": ["id:1", "state:busy"]}))
        self.assertTrue(fn({"name": "win-a", "tags": ["id:1", "state:idle"]}))
        self.assertTrue(fn({"name": "win-a", "tags": ["id:1"]}))
        self.assertFalse(fn({"name": "win-a", "tags": ["id:1", "state:disconnected"]}))
        self.assertFalse(fn({"name": "win-a", "tags": ["id:1", "state:Disconnected"]}))
        self.assertFalse(fn("not-a-dict"))

    def test_remote_soak_two_mock_worker_agents_loopback(self):
        rs = endurance.remote_soak
        (_echo_host, echo_port), echo_close = _start_echo_server()
        tmp_dir = tempfile.mkdtemp(prefix="ox-mock-remote-soak-")
        agent_threads = []
        srv = None
        try:
            sec_path = os.path.join(tmp_dir, "run.secret")
            secret_text = secrets.token_hex(32)
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secret_text + "\n")
            fake_bin = os.path.join(tmp_dir, "oxideswarm-fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")

            srv = rs.SoakControlServer(
                "127.0.0.1:0",
                rs.derive_control_key(secret_text),
                expected_workers=["win-a", "win-b"],
                expected_count=2,
                sample_interval_s=0.5,
                max_concurrent_per_source=4,
            )

            agents = {}

            def fake_start_worker(agent_self):
                p = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                agent_self._record_pid_start(p.pid)
                with agent_self._lock:
                    agent_self._worker_proc = p
                    agent_self._worker_log = None
                return int(p.pid)

            with (
                mock.patch.object(rs.WorkerAgent, "_start_worker", fake_start_worker),
                mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
                mock.patch.object(endurance, "check_free_space", return_value={"ok": True}),
                mock.patch.object(endurance, "hardware", return_value={"cpu": "mock-cpu", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
                mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1.0", "machine": "amd64"}),
                mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
            ):
                for name in ("win-a", "win-b"):
                    w_root = os.path.join(tmp_dir, "wroot-" + name)
                    os.makedirs(w_root, exist_ok=True)
                    ag = rs.WorkerAgent(
                        endurance,
                        binary=fake_bin,
                        master_addr="127.0.0.1:%d" % echo_port,
                        control_addr=srv.connect_addr,
                        secret_file=sec_path,
                        name=name,
                        slots=2,
                        sample_interval_s=0.5,
                        work_dir=w_root,
                        poll_interval_s=0.1,
                        connect_timeout_s=120.0,
                    )
                    agents[name] = ag
                    t = threading.Thread(target=ag.run, daemon=True)
                    t.start()
                    agent_threads.append((t, ag))

                self.assertEqual(srv.wait_for_agents(2, timeout_s=120.0), ["win-a", "win-b"])

                # Unauthenticated control attempt is refused while agents are running
                req = urllib.request.Request(
                    "http://%s/v1/poll" % srv.connect_addr,
                    data=b"{}",
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=5.0)
                self.assertEqual(ctx.exception.code, 401)
                ctx.exception.close()

                srv.mark_soak_started(time.monotonic())
                time.sleep(0.8)

                ack_kr = srv.dispatch_command("win-a", "kill_restart", {"down_s": 0.2}, timeout_s=60.0)
                res_kr = ack_kr["result"]
                self.assertGreater(res_kr["old_pid"], 0)
                self.assertGreater(res_kr["new_pid"], 0)
                self.assertNotEqual(res_kr["old_pid"], res_kr["new_pid"])
                self.assertTrue(endurance.pid_alive(res_kr["new_pid"]))

                ack_lc = srv.dispatch_command("win-b", "link_cut", {"down_s": 0.2}, timeout_s=60.0)
                self.assertGreaterEqual(ack_lc["result"]["cuts"], 1)

                time.sleep(0.7)
                snap_a = srv.dispatch_command("win-a", "idle_snapshot", {}, timeout_s=60.0)["result"]
                snap_b = srv.dispatch_command("win-b", "idle_snapshot", {}, timeout_s=60.0)["result"]
                merged = rs.merge_remote_samples(
                    [{"t_s": 0.0, "processes": {}, "dirs": {}}, {"t_s": 1.0, "processes": {}, "dirs": {}}],
                    {"win-a": snap_a["samples"], "win-b": snap_b["samples"]},
                    sample_interval_s=0.5,
                )
                all_roles = set()
                for row in merged:
                    all_roles.update(row["processes"].keys())
                self.assertIn("worker_win-a", all_roles)
                self.assertIn("worker_win-b", all_roles)

                sh_a = srv.dispatch_command("win-a", "shutdown", {}, timeout_s=60.0)["result"]
                sh_b = srv.dispatch_command("win-b", "shutdown", {}, timeout_s=60.0)["result"]
                self.assertEqual(sh_a["leftover_pids"], [])
                self.assertEqual(sh_b["leftover_pids"], [])
                self.assertEqual(sh_a["leftover_task_trees"], [])
                self.assertEqual(sh_b["leftover_task_trees"], [])

                for t, _ag in agent_threads:
                    t.join(timeout=120.0)
                    self.assertFalse(t.is_alive())

                endurance.assert_record_private(
                    json.dumps({"samples": merged, "snap_a": snap_a["idle_disk_snapshot"], "snap_b": snap_b["idle_disk_snapshot"]}),
                    forbidden=[secret_text],
                )
        finally:
            for _t, ag in agent_threads:
                ag.request_stop()
            if srv is not None:
                srv.stop()
            echo_close()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_fix1_control_server_stalled_timeout_and_503_cap(self):
        rs = endurance.remote_soak
        key = rs.derive_control_key(secrets.token_hex(32))
        srv = rs.SoakControlServer(
            "127.0.0.1:0",
            key,
            expected_workers=["win-a"],
            expected_count=1,
            sample_interval_s=5.0,
            handler_timeout_s=30.0,
            max_concurrent_connections=2,
            max_concurrent_per_source=4,
        )
        srv_short = rs.SoakControlServer(
            "127.0.0.1:0",
            key,
            expected_workers=["win-a"],
            expected_count=1,
            sample_interval_s=5.0,
            handler_timeout_s=0.45,
            max_concurrent_connections=2,
            max_concurrent_per_source=2,
        )
        socks = []
        try:
            for _ in range(2):
                s = socket.create_connection(("127.0.0.1", srv.port), timeout=5.0)
                s.sendall(b"POST /v1/poll HTTP/1.1\r\nHost: 127.0.0.1\r\n")
                socks.append(s)

            # Hold both concurrency slots on srv (30 s handler timeout) so 3rd sees 503 reliably on slow CI
            cap_wait_end = time.monotonic() + 5.0
            while srv.active_connections < 2 and time.monotonic() < cap_wait_end:
                time.sleep(0.01)
            self.assertEqual(srv.active_connections, 2)

            s3 = socket.create_connection(("127.0.0.1", srv.port), timeout=5.0)
            socks.append(s3)
            s3.sendall(b"POST /v1/poll HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\n\r\n{}")
            resp3 = s3.recv(1024)
            self.assertIn(b"503", resp3)

            for s in socks:
                try:
                    s.close()
                except OSError:
                    pass
            socks.clear()

            free_end = time.monotonic() + 5.0
            while srv.active_connections > 0 and time.monotonic() < free_end:
                time.sleep(0.01)
            self.assertEqual(srv.active_connections, 0)

            # Headerless request declaring huge Content-Length is rejected 401 before body arrives
            s4 = socket.create_connection(("127.0.0.1", srv.port), timeout=5.0)
            socks.append(s4)
            s4.sendall(b"POST /v1/poll HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 1048576\r\n\r\n")
            resp4 = s4.recv(1024)
            self.assertIn(b"401", resp4)

            # Healthy authenticated client works cleanly afterwards
            client = rs.SignedControlClient("http://%s" % srv.connect_addr, "win-a", key)
            st, reg = client.post("/v1/register", {"name": "win-a", "instance_id": "inst-1"})
            self.assertEqual(st, 200)
            self.assertTrue(reg["run_id"])

            # Separate short-timeout server closes a stalled pre-auth connection on timeout (EOF)
            stalled = socket.create_connection(("127.0.0.1", srv_short.port), timeout=5.0)
            socks.append(stalled)
            stalled.sendall(b"POST /v1/poll HTTP/1.1\r\nHost: 127.0.0.1\r\n")
            stalled.settimeout(5.0)
            self.assertEqual(stalled.recv(1024), b"")

            short_free_end = time.monotonic() + 3.0
            while srv_short.active_connections > 0 and time.monotonic() < short_free_end:
                time.sleep(0.02)
            self.assertEqual(srv_short.active_connections, 0)
        finally:
            for s in socks:
                try:
                    s.close()
                except OSError:
                    pass
            srv_short.stop()
            srv.stop()

    @staticmethod
    def _drain_http_post_request(conn: socket.socket) -> None:
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                return
            buf += chunk
        head, _, tail = buf.partition(b"\r\n\r\n")
        content_length = 0
        for line in head.split(b"\r\n")[1:]:
            name, sep, value = line.partition(b":")
            if sep and name.strip().lower() == b"content-length":
                content_length = int(value.strip())
                break
        remaining = max(0, content_length - len(tail))
        while remaining > 0:
            chunk = conn.recv(min(4096, remaining))
            if not chunk:
                break
            remaining -= len(chunk)

    def test_fix2_client_rejects_oversized_reply_before_reading(self):
        rs = endurance.remote_soak
        key = rs.derive_control_key(secrets.token_hex(32))

        def serve_one(raw_response_head: bytes, body_chunks: Iterable[bytes]):
            lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lsock.bind(("127.0.0.1", 0))
            lsock.listen(2)
            lsock.settimeout(5.0)
            port = lsock.getsockname()[1]

            def _run():
                try:
                    conn, _addr = lsock.accept()
                    with conn:
                        conn.settimeout(5.0)
                        RemoteSoakCryptoAndValidationTests._drain_http_post_request(conn)
                        conn.sendall(raw_response_head)
                        for ch in body_chunks:
                            conn.sendall(ch)
                        try:
                            conn.shutdown(socket.SHUT_WR)
                            conn.settimeout(1.0)
                            conn.recv(64)
                        except OSError:
                            pass
                except OSError:
                    pass
                finally:
                    try:
                        lsock.close()
                    except OSError:
                        pass

            th = threading.Thread(target=_run, daemon=True)
            th.start()
            return port, th

        # Case A: Content-Length > cap rejected before reading body
        p_cl, th_cl = serve_one(
            b"HTTP/1.1 200 OK\r\nContent-Length: 67108864\r\nConnection: close\r\n\r\n",
            [],
        )
        c1 = rs.SignedControlClient("http://127.0.0.1:%d" % p_cl, "win-a", key, max_reply_bytes=1024)
        with self.assertRaises(rs.ControlProtocolError) as ctx_cl:
            c1.post("/v1/poll", {"run_id": "r1"}, timeout_s=5.0)
        self.assertIn("Content-Length", str(ctx_cl.exception))
        th_cl.join(timeout=5.0)

        # Case B: Unbounded stream (no Content-Length) stopped at cap + 1 before HMAC verify
        p_st, th_st = serve_one(
            b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n",
            [b"x" * 4096],
        )
        c2 = rs.SignedControlClient("http://127.0.0.1:%d" % p_st, "win-a", key, max_reply_bytes=1024)
        with self.assertRaises(rs.ControlProtocolError) as ctx_st:
            c2.post("/v1/poll", {"run_id": "r1"}, timeout_s=5.0)
        self.assertIn("cap", str(ctx_st.exception))
        th_st.join(timeout=5.0)

    def test_fix3_response_binds_req_nonce_and_run_id_and_agent_dedupes_cmd_id(self):
        rs = endurance.remote_soak
        key = rs.derive_control_key(secrets.token_hex(32))

        # Request nonce and run_id are MAC-bound into response signatures
        ts_str = "%.3f" % time.time()
        body = b'{"run_id":"run-A","command":{"cmd_id":"c1","op":"link_cut","payload":{"down_s":0.1}}}\n'
        sig = rs.sign_response(key, 200, "/v1/poll", "win-a", ts_str, "srvnonce1111", body, "reqnonce1111", "run-A")
        self.assertTrue(
            rs.verify_response(key, 200, "/v1/poll", "win-a", ts_str, "srvnonce1111", body, sig, "reqnonce1111", "run-A")
        )
        self.assertFalse(
            rs.verify_response(key, 200, "/v1/poll", "win-a", ts_str, "srvnonce1111", body, sig, "reqnonce2222", "run-A")
        )
        self.assertFalse(
            rs.verify_response(key, 200, "/v1/poll", "win-a", ts_str, "srvnonce1111", body, sig, "reqnonce1111", "run-B")
        )

        # Replay of genuine signed server reply under a second request's req_nonce must be rejected
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.bind(("127.0.0.1", 0))
        lsock.listen(2)
        lsock.settimeout(5.0)
        port = lsock.getsockname()[1]

        def _replay_srv():
            try:
                conn, _addr = lsock.accept()
                with conn:
                    conn.settimeout(5.0)
                    RemoteSoakCryptoAndValidationTests._drain_http_post_request(conn)
                    reply = (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: application/json\r\n"
                        "Content-Length: %d\r\n"
                        "X-Soak-Ts: %s\r\n"
                        "X-Soak-Nonce: srvnonce1111\r\n"
                        "X-Soak-Req-Nonce: reqnonce1111\r\n"
                        "X-Soak-Run-Id: run-A\r\n"
                        "X-Soak-Sig: %s\r\n"
                        "Connection: close\r\n\r\n"
                        % (len(body), ts_str, sig)
                    ).encode("utf-8") + body
                    conn.sendall(reply)
                    try:
                        conn.shutdown(socket.SHUT_WR)
                        conn.settimeout(1.0)
                        conn.recv(64)
                    except OSError:
                        pass
            finally:
                lsock.close()

        th = threading.Thread(target=_replay_srv, daemon=True)
        th.start()
        client = rs.SignedControlClient("http://127.0.0.1:%d" % port, "win-a", key)
        with self.assertRaises(rs.ControlProtocolError):
            client.post("/v1/poll", {"run_id": "run-A"}, timeout_s=5.0)
        th.join(timeout=5.0)

        # WorkerAgent executes a repeated cmd_id only once and re-acks cached result
        (_echo_host, echo_port), echo_close = _start_echo_server()
        with tempfile.TemporaryDirectory() as tmp:
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32) + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")
            w_root = os.path.join(tmp, "wroot")
            os.makedirs(w_root, exist_ok=True)

            exec_calls: List[str] = []
            ack_calls: List[Dict[str, Any]] = []
            poll_seq = iter(
                [
                    (200, {"run_id": "run-dedup", "soak_running": True, "elapsed_s": 0.0, "command": {"cmd_id": "dup-1", "op": "link_cut", "payload": {"down_s": 0.01}}}),
                    (200, {"run_id": "run-dedup", "soak_running": True, "elapsed_s": 0.1, "command": {"cmd_id": "dup-1", "op": "link_cut", "payload": {"down_s": 0.01}}}),
                    (200, {"run_id": "run-dedup", "soak_running": True, "elapsed_s": 0.2, "command": {"cmd_id": "stop-1", "op": "shutdown", "payload": {}}}),
                ]
            )

            with (
                mock.patch.object(rs.WorkerAgent, "_start_worker", lambda s: 0),
                mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
                mock.patch.object(endurance, "check_free_space", return_value={"ok": True}),
                mock.patch.object(endurance, "hardware", return_value={"cpu": "mock", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
                mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1", "machine": "amd64"}),
                mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
            ):
                ag = rs.WorkerAgent(
                    endurance,
                    binary=fake_bin,
                    master_addr="127.0.0.1:%d" % echo_port,
                    control_addr="127.0.0.1:18501",
                    secret_file=sec_path,
                    name="win-a",
                    work_dir=w_root,
                    poll_interval_s=0.1,
                )
                orig_exec = ag._execute_command

                def spy_exec(cmd):
                    exec_calls.append(str(cmd.get("cmd_id")))
                    return orig_exec(cmd)

                def fake_post(path, payload, timeout_s=15.0):
                    if path == "/v1/register":
                        return 200, {"run_id": "run-dedup", "sample_interval_s": 60.0, "soak_running": True, "elapsed_s": 0.0}
                    if path == "/v1/poll":
                        return next(poll_seq)
                    if path == "/v1/ack":
                        ack_calls.append(dict(payload))
                        return 200, {"ok": True, "cmd_id": payload["cmd_id"]}
                    raise AssertionError("unexpected path %s" % path)

                ag._execute_command = spy_exec  # type: ignore[method-assign]
                ag._client.post = fake_post  # type: ignore[method-assign]
                rc = ag.run()
            echo_close()
            self.assertEqual(rc, 0)
            self.assertEqual(exec_calls, ["dup-1", "stop-1"])
            self.assertEqual([a["cmd_id"] for a in ack_calls], ["dup-1", "dup-1", "stop-1"])
            self.assertTrue(all(a["ok"] for a in ack_calls))

    def test_fix4_agent_orphan_timeout_skipped_verify_and_session_end(self):
        rs = endurance.remote_soak
        (_echo_host, echo_port), echo_close = _start_echo_server()
        tmp = tempfile.mkdtemp(prefix="ox-fix4-")
        srv = None
        try:
            sec_text = secrets.token_hex(32)
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(sec_text + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")

            srv = rs.SoakControlServer(
                "127.0.0.1:0",
                rs.derive_control_key(sec_text),
                expected_workers=["win-a"],
                expected_count=1,
                sample_interval_s=60.0,
                max_concurrent_per_source=8,
            )

            started_pids: List[int] = []

            def fake_start_worker(agent_self):
                p = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                agent_self._record_pid_start(p.pid)
                started_pids.append(int(p.pid))
                with agent_self._lock:
                    agent_self._worker_proc = p
                    agent_self._worker_log = None
                return int(p.pid)

            with (
                mock.patch.object(rs.WorkerAgent, "_start_worker", fake_start_worker),
                mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
                mock.patch.object(endurance, "check_free_space", return_value={"ok": True}),
                mock.patch.object(endurance, "hardware", return_value={"cpu": "mock", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
                mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1", "machine": "amd64"}),
                mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
            ):
                # Subtest A: orphan timeout stops child worker and returns 1
                ag_orphan = rs.WorkerAgent(
                    endurance,
                    binary=fake_bin,
                    master_addr="127.0.0.1:%d" % echo_port,
                    control_addr=srv.connect_addr,
                    secret_file=sec_path,
                    name="win-a",
                    work_dir=os.path.join(tmp, "w-orphan"),
                    poll_interval_s=0.1,
                    connect_timeout_s=30.0,
                    orphan_timeout_s=0.45,
                )
                rc_box: List[int] = []
                t = threading.Thread(target=lambda: rc_box.append(ag_orphan.run()), daemon=True)
                t.start()
                self.assertEqual(srv.wait_for_agents(1, timeout_s=30.0), ["win-a"])
                srv.stop()
                srv = None
                t.join(timeout=15.0)
                self.assertFalse(t.is_alive())
                self.assertEqual(rc_box, [1])
                self.assertEqual(len(started_pids), 1)
                self.assertFalse(endurance.pid_alive(started_pids[0]))

                # Subtest B: ControlProtocolError skips poll instead of crashing; 400 run_id mismatch exits 0
                ag_skip = rs.WorkerAgent(
                    endurance,
                    binary=fake_bin,
                    master_addr="127.0.0.1:%d" % echo_port,
                    control_addr="127.0.0.1:18501",
                    secret_file=sec_path,
                    name="win-a",
                    work_dir=os.path.join(tmp, "w-skip"),
                    poll_interval_s=0.1,
                    orphan_timeout_s=30.0,
                )
                poll_actions = iter(
                    [
                        rs.ControlProtocolError("bad reply mac"),
                        (200, {"run_id": "r1", "soak_running": True, "elapsed_s": 0.1, "command": None}),
                        (400, {"error": "run_id mismatch"}),
                    ]
                )

                def stub_post(path, payload, timeout_s=15.0):
                    if path == "/v1/register":
                        return 200, {"run_id": "r1", "sample_interval_s": 60.0, "soak_running": True, "elapsed_s": 0.0}
                    if path == "/v1/poll":
                        act = next(poll_actions)
                        if isinstance(act, Exception):
                            raise act
                        return act
                    raise AssertionError("unexpected path %s" % path)

                ag_skip._client.post = stub_post  # type: ignore[method-assign]
                rc_skip = ag_skip.run()
                self.assertEqual(rc_skip, 0)
                self.assertEqual(ag_skip._skipped_polls_verify, 1)
                self.assertEqual(len(started_pids), 2)
                self.assertFalse(endurance.pid_alive(started_pids[1]))
        finally:
            if srv is not None:
                srv.stop()
            echo_close()
            shutil.rmtree(tmp, ignore_errors=True)

    def test_fix5_missing_remote_evidence_fails_checks_and_writes_record(self):
        rs = endurance.remote_soak
        # Direct unit checks on evaluate_soak_pass_checks
        times = [0.0] + [300.0 + i * 70.0 for i in range(12)] + [1900.0 + i * 70.0 for i in range(12)]
        samples_only_a = [
            {
                "t_s": t,
                "processes": {
                    "master": {"pid": 1001, "rss_bytes": 40 * endurance.MIB, "handles": 100},
                    "worker_win-a": {"pid": 2001, "rss_bytes": 30 * endurance.MIB, "handles": 80},
                },
                "dirs": {"blob_store_bytes": 10 * endurance.MIB, "worker_cache_win-a_bytes": 10 * endurance.MIB},
            }
            for t in times
        ]
        # worker_win-b present only in baseline window, missing from late window
        samples_b_early_only = copy.deepcopy(samples_only_a)
        samples_b_early_only[1]["processes"]["worker_win-b"] = {"pid": 3001, "rss_bytes": 30 * endurance.MIB, "handles": 80}

        lg = endurance.TaskLedger()
        tid = "t-ok-1"
        lg.record_submit(tid, expected_outcome="ok")
        lg.record_result(tid, outcome="ok")
        master_audit = {
            "unique_result_tasks": 1,
            "multi_result_tasks": {},
            "queue_record_errors": 0,
            "missing_from_master_results": [],
            "untracked_master_results": [],
        }
        probes = (
            [{"t_s": 300.0 + i * 80.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
            + [{"t_s": 1520.0 + i * 80.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
        )
        common_kw = dict(
            ledger_audit=lg.audit(),
            probes=probes,
            duration_s=2700.0,
            blob_store_limit_bytes=256 * endurance.MIB,
            worker_cache_limit_bytes=128 * endurance.MIB,
            pinned_headroom_bytes=64 * endurance.MIB,
            worker_pinned_headroom_bytes=64 * endurance.MIB,
            idle_sandbox_entries={"win-a": ["cache", ".locks"], "win-b": ["cache", ".locks"]},
            tmp_leftovers={"master_blobs_tmp": []},
            compactions=[{"before_bytes": 50000, "after_bytes": 24}],
            idle_blob_store_bytes=10 * endurance.MIB,
            idle_worker_cache_bytes={"worker_cache_win-a_bytes": 10 * endurance.MIB, "worker_cache_win-b_bytes": 10 * endurance.MIB},
            master_audit=master_audit,
        )

        # Missing worker_win-b samples entirely -> fail 2_resource_plateau
        res_missing_b = endurance.evaluate_soak_pass_checks(
            samples=samples_only_a,
            leftover_pids=[],
            expected_process_roles=["master", "worker_win-a", "worker_win-b"],
            missing_remote_shutdowns=[],
            **common_kw,
        )
        self.assertFalse(res_missing_b["checks"]["2_resource_plateau"]["passed"])
        self.assertEqual(res_missing_b["checks"]["2_resource_plateau"]["missing_roles"], ["worker_win-b"])

        # worker_win-b only in baseline window -> fail 2_resource_plateau
        res_early_b = endurance.evaluate_soak_pass_checks(
            samples=samples_b_early_only,
            leftover_pids=[],
            expected_process_roles=["master", "worker_win-a", "worker_win-b"],
            missing_remote_shutdowns=[],
            **common_kw,
        )
        self.assertFalse(res_early_b["checks"]["2_resource_plateau"]["passed"])
        self.assertEqual(res_early_b["checks"]["2_resource_plateau"]["missing_roles"], ["worker_win-b"])

        # Missing remote shutdown + duplicate raw PID on two different workers
        res_shutdown = endurance.evaluate_soak_pass_checks(
            samples=samples_only_a,
            leftover_pids=[{"worker": "win-a", "pid": 42}, {"worker": "win-b", "pid": 42}],
            expected_process_roles=["master", "worker_win-a"],
            missing_remote_shutdowns=["win-b"],
            **common_kw,
        )
        self.assertFalse(res_shutdown["checks"]["5_clean_stop"]["passed"])
        self.assertEqual(res_shutdown["checks"]["5_clean_stop"]["missing_remote_shutdowns"], ["win-b"])
        self.assertEqual(
            res_shutdown["checks"]["5_clean_stop"]["leftover_pids"],
            [{"worker": "win-a", "pid": 42}, {"worker": "win-b", "pid": 42}],
        )

        # End-to-end cmd_soak with mocked RemoteSoakCluster where win-b fails idle_snapshot & shutdown
        with tempfile.TemporaryDirectory() as tmp:
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32) + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")
            out_json = os.path.join(tmp, "soak_record.json")

            def _fake_subprocess_run(cmd, **_kw):
                if isinstance(cmd, list) and "secret" in cmd and "show" in cmd:
                    return subprocess.CompletedProcess(cmd, 0, "dash-tok-secret\n", "")
                return subprocess.CompletedProcess(cmd, 0, "ok\n", "")

            with (
                mock.patch.object(rs.subprocess, "run", side_effect=_fake_subprocess_run),
                mock.patch.object(endurance, "check_free_space", return_value={"ok": True, "free_bytes": 100 * endurance.GIB, "required_bytes": 1 * endurance.GIB}),
                mock.patch.object(endurance, "hardware", return_value={"cpu": "mock", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
                mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1", "machine": "amd64"}),
                mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
                mock.patch.object(endurance, "build_profile", return_value="release"),
                mock.patch.object(endurance, "measure_tcp_rtt_ms", return_value=0.1),
                mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
            ):
                cl = rs.create_remote_soak_cluster(
                    endurance,
                    binary=fake_bin,
                    workdir=os.path.join(tmp, "cl"),
                    workers=2,
                    slots=2,
                    blob_store_mb=256,
                    blob_cache_mb=128,
                    max_queue_size=8,
                    workspaces=False,
                    session_resume_hold_secs=None,
                    master_listen="127.0.0.1:0",
                    control_listen="127.0.0.1:0",
                    secret_file=sec_path,
                    expect_workers=["win-a", "win-b"],
                    register_timeout_s=10.0,
                    sample_interval_s=0.5,
                )

                class _FakeSrv:
                    def dispatch_command(self, wname, op, _payload=None, timeout_s=120.0):
                        if wname == "win-b" and op in ("idle_snapshot", "shutdown"):
                            raise RuntimeError("timed out talking to %s" % wname)
                        if op == "idle_snapshot":
                            return {
                                "ok": True,
                                "result": {
                                    "samples": [
                                        {"t_s": 0.1, "processes": {"worker_win-a": {"pid": 11, "rss_bytes": 100, "handles": 10, "threads": 4, "cpu_seconds": 0.1}}, "dirs": {"worker_cache_win-a_bytes": 10}},
                                        {"t_s": 0.6, "processes": {"worker_win-a": {"pid": 11, "rss_bytes": 100, "handles": 10, "threads": 4, "cpu_seconds": 0.1}}, "dirs": {"worker_cache_win-a_bytes": 10}},
                                    ],
                                    "idle_disk_snapshot": {"sandbox_entries": ["cache", ".locks"], "worker_cache_bytes": 10, "cache_tmp": [], "locks": []},
                                },
                            }
                        if op == "shutdown":
                            return {"ok": True, "result": {"leftover_pids": [77], "leftover_task_trees": []}}
                        return {"ok": True, "result": {}}

                    def worker_metadata(self):
                        return [{"name": "win-a"}, {"name": "win-b"}]

                    def mark_soak_started(self, _t0):
                        return

                    def stop(self):
                        return

                cl.control_server = _FakeSrv()
                cl.worker_names = ["win-a", "win-b"]
                cl.start = lambda: cl  # type: ignore[method-assign]
                cl.stop_master = lambda reason="final_stop": {"reason": reason, "before_bytes": 100, "after_bytes": 24, "shrank": True}  # type: ignore[method-assign]
                cl.active_pids = lambda: {"master": 999}  # type: ignore[method-assign]
                cl.audit_master_logs = lambda expected_task_ids=(): {  # type: ignore[method-assign]
                    "unique_result_tasks": len(list(expected_task_ids)),
                    "multi_result_tasks": {},
                    "queue_record_errors": 0,
                    "missing_from_master_results": [],
                    "untracked_master_results": [],
                    "failover_sweeps": 0,
                    "requeues_logged": 0,
                    "requeued_task_ids": [],
                }
                cl.leftover_process_trees = lambda: []  # type: ignore[method-assign]

                def _fake_run_client(args_list, **_kw):
                    if args_list and args_list[0] == "status":
                        return subprocess.CompletedProcess(args_list, 0, '{"pending_tasks":0,"running_tasks":0}\n', "")
                    return subprocess.CompletedProcess(args_list, 0, '{"status":"completed","exit_code":0,"task_id":"t-mock"}\n', "")

                cl.run_client = _fake_run_client  # type: ignore[method-assign]

                ap = endurance.build_parser()
                ns = ap.parse_args(
                    [
                        "soak",
                        "--bin",
                        fake_bin,
                        "--master-only",
                        "--secret-file",
                        sec_path,
                        "--expect-workers",
                        "win-a,win-b",
                        "--workers",
                        "2",
                        "--duration",
                        "1s",
                        "--sample-interval",
                        "0.5s",
                        "--fault-scale",
                        "0.01",
                        "--load-scale",
                        "0.1",
                        "--work-dir",
                        os.path.join(tmp, "work"),
                        "--out",
                        out_json,
                    ]
                )
                with mock.patch.object(rs, "create_remote_soak_cluster", return_value=cl):
                    rc_soak = endurance.cmd_soak(ns)
                self.assertEqual(rc_soak, 1)
                self.assertTrue(os.path.isfile(out_json))
                with open(out_json, encoding="utf-8") as rf:
                    rec = json.load(rf)
                self.assertFalse(rec["all_passed"])
                c_map = rec["soak"]["pass_checks"]
                self.assertFalse(c_map["2_resource_plateau"]["passed"])
                self.assertIn("worker_win-b", c_map["2_resource_plateau"].get("missing_roles", []))
                self.assertFalse(c_map["3_disk_and_journal"]["passed"])
                self.assertFalse(c_map["5_clean_stop"]["passed"])
                self.assertEqual(c_map["5_clean_stop"]["missing_remote_shutdowns"], ["win-b"])
                self.assertEqual(c_map["5_clean_stop"]["leftover_pids"], [{"worker": "win-a", "pid": 77}])
                errs = rec["soak"].get("remote_evidence_errors") or []
                self.assertEqual([(e["worker"], e["phase"]) for e in errs], [("win-b", "idle_snapshot"), ("win-b", "shutdown")])

    def test_fix8_resolve_dashboard_token_private_env_and_raises(self):
        rs = endurance.remote_soak
        with tempfile.TemporaryDirectory() as tmp:
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32) + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")
            workdir = os.path.join(tmp, "cluster")
            seen_envs: List[Dict[str, str]] = []

            def fake_ok_run(cmd, **kw):
                if isinstance(cmd, list) and "secret" in cmd and "show" in cmd:
                    seen_envs.append(dict(kw.get("env") or {}))
                    return subprocess.CompletedProcess(cmd, 0, "tok-from-private-env\n", "")
                return subprocess.CompletedProcess(cmd, 0, "", "")

            def fake_fail_run(cmd, **_kw):
                if isinstance(cmd, list) and "secret" in cmd and "show" in cmd:
                    return subprocess.CompletedProcess(cmd, 1, "", "boom")
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with mock.patch.dict(os.environ, {"OXIDESWARM_SECRET": "must-be-stripped", "RUSTY_GRID_CONFIG": "/real/cfg.toml"}):
                with mock.patch.object(rs.subprocess, "run", side_effect=fake_ok_run):
                    cl = rs.create_remote_soak_cluster(
                        endurance,
                        binary=fake_bin,
                        workdir=workdir,
                        workers=2,
                        slots=4,
                        blob_store_mb=256,
                        blob_cache_mb=128,
                        max_queue_size=8,
                        workspaces=True,
                        session_resume_hold_secs=None,
                        master_listen="127.0.0.1:0",
                        control_listen="127.0.0.1:0",
                        secret_file=sec_path,
                        expect_workers=["win-a", "win-b"],
                        register_timeout_s=10.0,
                        sample_interval_s=60.0,
                    )
            self.assertEqual(cl.dashboard_token, "tok-from-private-env")
            self.assertEqual(len(seen_envs), 1)
            env_used = seen_envs[0]
            self.assertTrue(env_used["HOME"].startswith(os.path.abspath(workdir)))
            self.assertTrue(env_used["USERPROFILE"].startswith(os.path.abspath(workdir)))
            self.assertTrue(env_used["OXIDESWARM_HOME"].startswith(os.path.abspath(workdir)))
            self.assertTrue(env_used["RUSTY_GRID_CONFIG"].endswith("empty.toml"))
            self.assertNotIn("OXIDESWARM_SECRET", env_used)
            self.assertEqual(env_used["OXIDESWARM_SECRET_FILE"], os.path.abspath(sec_path))

            # Must raise RuntimeError on non-zero returncode or empty stdout
            with mock.patch.object(rs.subprocess, "run", side_effect=fake_fail_run):
                with self.assertRaises(RuntimeError):
                    rs.create_remote_soak_cluster(
                        endurance,
                        binary=fake_bin,
                        workdir=workdir,
                        workers=2,
                        slots=4,
                        blob_store_mb=256,
                        blob_cache_mb=128,
                        max_queue_size=8,
                        workspaces=True,
                        session_resume_hold_secs=None,
                        master_listen="127.0.0.1:0",
                        control_listen="127.0.0.1:0",
                        secret_file=sec_path,
                        expect_workers=["win-a", "win-b"],
                        register_timeout_s=10.0,
                        sample_interval_s=60.0,
                    )

    @staticmethod
    def _make_passing_soak_check_inputs():
        times = [0.0] + [300.0 + i * 70.0 for i in range(12)] + [1900.0 + i * 70.0 for i in range(12)]
        samples = [
            {
                "t_s": t,
                "processes": {
                    "master": {"pid": 1001, "rss_bytes": 40 * endurance.MIB, "handles": 100},
                    "worker_win-a": {"pid": 2001, "rss_bytes": 30 * endurance.MIB, "handles": 80},
                },
                "dirs": {"blob_store_bytes": 10 * endurance.MIB, "worker_cache_win-a_bytes": 10 * endurance.MIB},
            }
            for t in times
        ]
        lg = endurance.TaskLedger()
        lg.record_submit("t-ok-1", expected_outcome="ok")
        lg.record_result("t-ok-1", outcome="ok")
        master_audit = {
            "unique_result_tasks": 1,
            "multi_result_tasks": {},
            "queue_record_errors": 0,
            "missing_from_master_results": [],
            "untracked_master_results": [],
        }
        probes = (
            [{"t_s": 300.0 + i * 80.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
            + [{"t_s": 1520.0 + i * 80.0, "latency_ms": 20.0, "ok": True} for i in range(12)]
        )
        return dict(
            ledger_audit=lg.audit(),
            samples=samples,
            probes=probes,
            duration_s=2700.0,
            blob_store_limit_bytes=256 * endurance.MIB,
            worker_cache_limit_bytes=128 * endurance.MIB,
            pinned_headroom_bytes=64 * endurance.MIB,
            worker_pinned_headroom_bytes=64 * endurance.MIB,
            idle_sandbox_entries={"win-a": ["cache", ".locks"]},
            tmp_leftovers={"master_blobs_tmp": []},
            compactions=[{"before_bytes": 50000, "after_bytes": 24}],
            leftover_pids=[],
            idle_blob_store_bytes=10 * endurance.MIB,
            idle_worker_cache_bytes={"worker_cache_win-a_bytes": 10 * endurance.MIB},
            master_audit=master_audit,
            expected_process_roles=["master", "worker_win-a"],
            missing_remote_shutdowns=[],
        )

    @staticmethod
    def _make_mock_remote_cluster(tmp_dir: str, fake_bin: str, sec_path: str, expect_workers):
        rs = endurance.remote_soak
        with mock.patch.object(
            rs.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(["oxideswarm"], 0, "dash-tok-secret\n", ""),
        ):
            cl = rs.create_remote_soak_cluster(
                endurance,
                binary=fake_bin,
                workdir=os.path.join(tmp_dir, "cl"),
                workers=len(expect_workers),
                slots=2,
                blob_store_mb=256,
                blob_cache_mb=128,
                max_queue_size=8,
                workspaces=False,
                session_resume_hold_secs=None,
                master_listen="127.0.0.1:0",
                control_listen="127.0.0.1:0",
                secret_file=sec_path,
                expect_workers=list(expect_workers),
                register_timeout_s=10.0,
                sample_interval_s=0.5,
            )
        cl.start = lambda: cl  # type: ignore[method-assign]
        cl.stop_master = lambda reason="final_stop": {"reason": reason, "before_bytes": 100, "after_bytes": 24, "shrank": True}  # type: ignore[method-assign]
        cl.active_pids = lambda: {"master": 999}  # type: ignore[method-assign]
        cl.audit_master_logs = lambda expected_task_ids=(): {  # type: ignore[method-assign]
            "unique_result_tasks": len(list(expected_task_ids)),
            "multi_result_tasks": {},
            "queue_record_errors": 0,
            "missing_from_master_results": [],
            "untracked_master_results": [],
            "failover_sweeps": 0,
            "requeues_logged": 0,
            "requeued_task_ids": [],
        }
        cl.leftover_process_trees = lambda: []  # type: ignore[method-assign]

        def _fake_run_client(args_list, **_kw):
            if args_list and args_list[0] == "status":
                return subprocess.CompletedProcess(args_list, 0, '{"pending_tasks":0,"running_tasks":0}\n', "")
            return subprocess.CompletedProcess(args_list, 0, '{"status":"completed","exit_code":0,"task_id":"t-mock"}\n', "")

        cl.run_client = _fake_run_client  # type: ignore[method-assign]
        return cl

    def _run_common_soak_mocks(self, fake_bin: str):
        return (
            mock.patch.object(endurance, "check_free_space", return_value={"ok": True, "free_bytes": 100 * endurance.GIB, "required_bytes": 1 * endurance.GIB}),
            mock.patch.object(endurance, "hardware", return_value={"cpu": "mock", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
            mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1", "machine": "amd64"}),
            mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
            mock.patch.object(endurance, "build_profile", return_value="release"),
            mock.patch.object(endurance, "measure_tcp_rtt_ms", return_value=0.1),
            mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
        )

    def test_fix4_skipped_poll_counters_recorded_and_fail_all_passed(self):
        rs = endurance.remote_soak
        # Part 1: evaluate_soak_pass_checks accepts transport skips but fails all_passed on verify skips
        base_kw = self._make_passing_soak_check_inputs()
        ok_res = endurance.evaluate_soak_pass_checks(
            remote_workers=[{"name": "win-a", "skipped_polls_verify": 0, "skipped_polls_transport": 4}],
            **base_kw,
        )
        self.assertTrue(ok_res["all_passed"])
        self.assertNotIn("remote_verify_failures", ok_res)

        bad_res = endurance.evaluate_soak_pass_checks(
            remote_workers=[{"name": "win-a", "skipped_polls_verify": 1, "skipped_polls_transport": 0}],
            **base_kw,
        )
        self.assertFalse(bad_res["all_passed"])
        self.assertEqual(bad_res.get("remote_verify_failures"), {"win-a": 1})

        nameless_bad = endurance.evaluate_soak_pass_checks(
            remote_workers=[{"skipped_polls_verify": 2}],
            **base_kw,
        )
        self.assertFalse(nameless_bad["all_passed"])

        # Part 2: real SoakControlServer + WorkerAgent preserve skip counters through idle_snapshot and shutdown
        (_echo_host, echo_port), echo_close = _start_echo_server()
        tmp = tempfile.mkdtemp(prefix="ox-fix4-cnt-")
        srv = None
        try:
            sec_text = secrets.token_hex(32)
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(sec_text + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")
            srv = rs.SoakControlServer(
                "127.0.0.1:0",
                rs.derive_control_key(sec_text),
                expected_workers=["win-a"],
                expected_count=1,
                sample_interval_s=60.0,
                max_concurrent_per_source=8,
            )
            with (
                mock.patch.object(rs.WorkerAgent, "_start_worker", lambda s: 0),
                mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
                mock.patch.object(endurance, "check_free_space", return_value={"ok": True}),
                mock.patch.object(endurance, "hardware", return_value={"cpu": "mock", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
                mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1", "machine": "amd64"}),
                mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
            ):
                ag = rs.WorkerAgent(
                    endurance,
                    binary=fake_bin,
                    master_addr="127.0.0.1:%d" % echo_port,
                    control_addr=srv.connect_addr,
                    secret_file=sec_path,
                    name="win-a",
                    work_dir=os.path.join(tmp, "w"),
                    poll_interval_s=0.05,
                    connect_timeout_s=30.0,
                    orphan_timeout_s=30.0,
                )
                orig_post = ag._client.post
                idle_seen = threading.Event()
                pre_idle_faults = iter([rs.ControlProtocolError("bad mac 1"), OSError("reset 1")])
                post_idle_faults = iter([rs.ControlProtocolError("bad mac 2")])

                def wrap_post(path, payload, timeout_s=15.0):
                    if path == "/v1/poll":
                        if not idle_seen.is_set():
                            act = next(pre_idle_faults, None)
                            if act is not None:
                                raise act
                        else:
                            act = next(post_idle_faults, None)
                            if act is not None:
                                raise act
                    st, resp = orig_post(path, payload, timeout_s=timeout_s)
                    if path == "/v1/poll" and st == 200 and isinstance(resp.get("command"), dict):
                        if resp["command"].get("op") == "idle_snapshot":
                            idle_seen.set()
                    return st, resp

                ag._client.post = wrap_post  # type: ignore[method-assign]
                th = threading.Thread(target=ag.run, daemon=True)
                th.start()
                self.assertEqual(srv.wait_for_agents(1, timeout_s=30.0), ["win-a"])
                snap = srv.dispatch_command("win-a", "idle_snapshot", {}, timeout_s=30.0)["result"]
                self.assertEqual(snap["skipped_polls_verify"], 1)
                self.assertEqual(snap["skipped_polls_transport"], 1)
                sh_res = srv.dispatch_command("win-a", "shutdown", {}, timeout_s=30.0)["result"]
                self.assertEqual(sh_res["skipped_polls_verify"], 2)
                self.assertEqual(sh_res["skipped_polls_transport"], 1)
                th.join(timeout=15.0)
                self.assertFalse(th.is_alive())
                meta = srv.worker_metadata()
                self.assertEqual(meta[0]["skipped_polls_verify"], 2)
                self.assertEqual(meta[0]["skipped_polls_transport"], 1)
        finally:
            if srv is not None:
                srv.stop()
            echo_close()
            shutil.rmtree(tmp, ignore_errors=True)

        # Part 3: cmd_soak writes shutdown-phase counters into record["remote_workers"] and fails all_passed
        with tempfile.TemporaryDirectory() as tmp:
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32) + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")
            out_json = os.path.join(tmp, "soak_cnt_record.json")

            class _FakeSkipSrv:
                def dispatch_command(self, wname, op, _payload=None, timeout_s=120.0):
                    if op == "idle_snapshot":
                        return {
                            "ok": True,
                            "result": {
                                "samples": [],
                                "idle_disk_snapshot": {"sandbox_entries": ["cache", ".locks"], "worker_cache_bytes": 10, "cache_tmp": [], "locks": []},
                                "skipped_polls_verify": 0,
                                "skipped_polls_transport": 1 if wname == "win-a" else 0,
                            },
                        }
                    if op == "shutdown":
                        return {
                            "ok": True,
                            "result": {
                                "leftover_pids": [],
                                "leftover_task_trees": [],
                                "skipped_polls_verify": 2 if wname == "win-a" else 0,
                                "skipped_polls_transport": 3 if wname == "win-a" else 0,
                            },
                        }
                    return {"ok": True, "result": {}}

                def worker_metadata(self):
                    return [{"name": "win-a"}, {"name": "win-b"}]

                def registered_workers(self):
                    return ["win-a", "win-b"]

                def mark_soak_started(self, _t0):
                    return

                def stop(self):
                    return

            patches = self._run_common_soak_mocks(fake_bin)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                cl = self._make_mock_remote_cluster(tmp, fake_bin, sec_path, ["win-a", "win-b"])
                cl.control_server = _FakeSkipSrv()
                cl.worker_names = ["win-a", "win-b"]
                ap = endurance.build_parser()
                ns = ap.parse_args(
                    [
                        "soak",
                        "--bin",
                        fake_bin,
                        "--master-only",
                        "--secret-file",
                        sec_path,
                        "--expect-workers",
                        "win-a,win-b",
                        "--workers",
                        "2",
                        "--duration",
                        "1s",
                        "--sample-interval",
                        "0.5s",
                        "--fault-scale",
                        "0.01",
                        "--load-scale",
                        "0.1",
                        "--work-dir",
                        os.path.join(tmp, "work"),
                        "--out",
                        out_json,
                    ]
                )
                with mock.patch.object(rs, "create_remote_soak_cluster", return_value=cl):
                    rc_soak = endurance.cmd_soak(ns)
            self.assertEqual(rc_soak, 1)
            with open(out_json, encoding="utf-8") as rf:
                rec = json.load(rf)
            self.assertFalse(rec["all_passed"])
            self.assertEqual(rec["soak"].get("remote_verify_failures"), {"win-a": 2})
            by_name = {w["name"]: w for w in rec["remote_workers"]}
            self.assertEqual(by_name["win-a"]["skipped_polls_verify"], 2)
            self.assertEqual(by_name["win-a"]["skipped_polls_transport"], 3)
            self.assertEqual(by_name["win-b"]["skipped_polls_verify"], 0)
            self.assertEqual(by_name["win-b"]["skipped_polls_transport"], 0)

    def test_n2_run_soak_startup_cleanup_on_keyboard_interrupt_and_sampler_error(self):
        rs = endurance.remote_soak
        with tempfile.TemporaryDirectory() as tmp:
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32) + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")

            # Case A: KeyboardInterrupt during cluster.start() wait_for_agents stops master + registered workers and deletes sc_dir
            patches = self._run_common_soak_mocks(fake_bin)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                cl = self._make_mock_remote_cluster(tmp, fake_bin, sec_path, ["win-a", "win-b"])
                shutdown_dispatched = []
                stop_master_calls = []

                class _StartupSrv:
                    def registered_workers(self):
                        return ["win-a"]

                    def dispatch_command(self, wname, op, _payload=None, timeout_s=120.0):
                        if op == "shutdown":
                            shutdown_dispatched.append(wname)
                            return {"ok": True, "result": {"leftover_pids": [], "leftover_task_trees": [], "skipped_polls_verify": 0, "skipped_polls_transport": 0}}
                        return {"ok": True, "result": {}}

                    def worker_metadata(self):
                        return [{"name": "win-a"}]

                    def stop(self):
                        return

                cl.control_server = _StartupSrv()
                cl.worker_names = []  # wait_for_agents has not completed yet

                def _abort_start():
                    raise KeyboardInterrupt("user aborted during wait_for_agents")

                cl.start = _abort_start  # type: ignore[method-assign]
                cl.stop_master = lambda reason="final_stop": stop_master_calls.append(reason) or {}  # type: ignore[method-assign]

                work_a = os.path.join(tmp, "work-abort")
                os.makedirs(work_a, exist_ok=True)
                ns_a = endurance.build_parser().parse_args(
                    [
                        "soak",
                        "--bin",
                        fake_bin,
                        "--master-only",
                        "--secret-file",
                        sec_path,
                        "--expect-workers",
                        "win-a,win-b",
                        "--workers",
                        "2",
                        "--duration",
                        "1s",
                        "--work-dir",
                        work_a,
                    ]
                )
                # Exercise run_soak directly with cl via create_remote_soak_cluster patch
                with mock.patch.object(rs, "create_remote_soak_cluster", return_value=cl):
                    with self.assertRaises(KeyboardInterrupt):
                        endurance.run_soak(
                            ns_a,
                            work_a,
                            {
                                "duration_s": 1.0,
                                "sample_interval_s": 0.5,
                                "fault_scale": 0.01,
                                "load_scale": 0.1,
                                "workers": 2,
                                "slots": 2,
                                "max_queue_size": 8,
                                "blob_store_mb": 256,
                                "blob_cache_mb": 128,
                                "master_only": True,
                                "master_listen": "127.0.0.1:0",
                                "control_listen": "127.0.0.1:0",
                                "secret_file": sec_path,
                                "expect_workers": ["win-a", "win-b"],
                                "register_timeout_s": 10.0,
                            },
                        )
                self.assertEqual(shutdown_dispatched, ["win-a"])
                self.assertEqual(stop_master_calls, ["final_stop"])
                self.assertFalse(os.path.exists(os.path.join(work_a, "soak")))

                # Case B: Exception in BackgroundSampler.start after cluster.start runs full teardown & writes record with run_error
                cl_b = self._make_mock_remote_cluster(tmp, fake_bin, sec_path, ["win-a", "win-b"])
                stop_b_calls = []

                class _SamplerFailSrv:
                    def registered_workers(self):
                        return ["win-a", "win-b"]

                    def dispatch_command(self, wname, op, _payload=None, timeout_s=120.0):
                        if op == "idle_snapshot":
                            return {"ok": True, "result": {"samples": [], "idle_disk_snapshot": {"sandbox_entries": ["cache"], "worker_cache_bytes": 10, "cache_tmp": [], "locks": []}}}
                        if op == "shutdown":
                            return {"ok": True, "result": {"leftover_pids": [], "leftover_task_trees": []}}
                        return {"ok": True, "result": {}}

                    def worker_metadata(self):
                        return [{"name": "win-a"}, {"name": "win-b"}]

                    def mark_soak_started(self, _t0):
                        return

                    def stop(self):
                        return

                cl_b.control_server = _SamplerFailSrv()
                cl_b.worker_names = ["win-a", "win-b"]
                orig_stop_b = cl_b.stop

                def _spy_stop_b():
                    stop_b_calls.append(True)
                    orig_stop_b()

                cl_b.stop = _spy_stop_b  # type: ignore[method-assign]
                out_b = os.path.join(tmp, "sampler_err_record.json")
                ns_b = endurance.build_parser().parse_args(
                    [
                        "soak",
                        "--bin",
                        fake_bin,
                        "--master-only",
                        "--secret-file",
                        sec_path,
                        "--expect-workers",
                        "win-a,win-b",
                        "--workers",
                        "2",
                        "--duration",
                        "1s",
                        "--sample-interval",
                        "0.5s",
                        "--work-dir",
                        os.path.join(tmp, "work-b"),
                        "--out",
                        out_b,
                    ]
                )
                with (
                    mock.patch.object(rs, "create_remote_soak_cluster", return_value=cl_b),
                    mock.patch.object(endurance.BackgroundSampler, "start", side_effect=ValueError("bad ps output")),
                ):
                    rc_b = endurance.cmd_soak(ns_b)
                self.assertEqual(rc_b, 1)
                self.assertEqual(stop_b_calls, [True])
                self.assertTrue(os.path.isfile(out_b))
                with open(out_b, encoding="utf-8") as rf:
                    rec_b = json.load(rf)
                self.assertFalse(rec_b["all_passed"])
                self.assertIn("bad ps output", rec_b["soak"].get("run_error") or "")

    def test_n3_per_source_cap_and_total_request_deadline(self):
        rs = endurance.remote_soak
        key = rs.derive_control_key(secrets.token_hex(32))
        # Part A: per-source cap (2) triggers 503 before global cap (4) fills
        srv = rs.SoakControlServer(
            "127.0.0.1:0",
            key,
            expected_workers=["win-a"],
            expected_count=1,
            sample_interval_s=5.0,
            handler_timeout_s=30.0,
            max_concurrent_connections=4,
            max_concurrent_per_source=2,
        )
        # Part B: cumulative deadline covers both header trickle and body trickle together
        srv_dl = rs.SoakControlServer(
            "127.0.0.1:0",
            key,
            expected_workers=["win-a"],
            expected_count=1,
            sample_interval_s=5.0,
            handler_timeout_s=0.6,
            max_concurrent_connections=4,
            max_concurrent_per_source=4,
        )
        socks = []
        try:
            for _ in range(2):
                s = socket.create_connection(("127.0.0.1", srv.port), timeout=5.0)
                s.sendall(b"POST /v1/poll HTTP/1.1\r\nHost: 127.0.0.1\r\n")
                socks.append(s)

            wait_end = time.monotonic() + 5.0
            while srv.active_connections < 2 and time.monotonic() < wait_end:
                time.sleep(0.01)
            self.assertEqual(srv.active_connections, 2)
            self.assertEqual(srv.active_connections_by_source.get("127.0.0.1"), 2)

            s3 = socket.create_connection(("127.0.0.1", srv.port), timeout=5.0)
            socks.append(s3)
            s3.sendall(b"POST /v1/poll HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\n\r\n{}")
            self.assertIn(b"503", s3.recv(1024))

            # Another source IP is not blocked while 127.0.0.1 holds all per-source slots
            self.assertTrue(srv._try_acquire_conn_slot("192.0.2.50"))
            srv._release_conn_slot("192.0.2.50")
            self.assertFalse(srv._try_acquire_conn_slot("127.0.0.1"))

            def assert_trickle_aborts(initial_bytes: bytes, max_ticks: int = 20):
                t0 = time.monotonic()
                st = socket.create_connection(("127.0.0.1", srv_dl.port), timeout=5.0)
                socks.append(st)
                st.settimeout(2.0)
                st.sendall(initial_bytes)
                aborted = False
                for _ in range(max_ticks):
                    try:
                        st.sendall(b"a")
                    except (TimeoutError, socket.timeout) as exc:
                        self.fail("unexpected client send timeout: %s" % (exc,))
                    except OSError:
                        aborted = True
                        break
                    time.sleep(0.15)
                if not aborted:
                    st.settimeout(1.5)
                    try:
                        tail = st.recv(1024)
                        if tail == b"":
                            aborted = True
                    except (TimeoutError, socket.timeout):
                        aborted = False
                    except OSError:
                        aborted = True
                elapsed = time.monotonic() - t0
                try:
                    st.close()
                except OSError:
                    pass
                self.assertTrue(aborted, "server did not close trickling request by deadline")
                self.assertGreaterEqual(elapsed, 0.45, "connection closed before 0.6s deadline (%.3fs)" % elapsed)
                self.assertLess(elapsed, 2.5, "trickle was not cut before 3s budget (%.3fs)" % elapsed)
                rel_end = time.monotonic() + 3.0
                while srv_dl.active_connections > 0 and time.monotonic() < rel_end:
                    time.sleep(0.02)
                self.assertEqual(srv_dl.active_connections, 0)

            # Trickling header bytes (0.15s per byte < 0.6s read timeout, total > 0.6s)
            assert_trickle_aborts(b"POST /v1/poll HTTP/1.1\r\nX-Pad: ")

            # Trickling body bytes after syntactically valid headers (pre-auth read_body path)
            sig_placeholder = secrets.token_hex(32)
            hdrs = (
                "POST /v1/poll HTTP/1.1\r\n"
                "Host: 127.0.0.1\r\n"
                "Content-Length: 64\r\n"
                "X-Soak-Worker: win-a\r\n"
                "X-Soak-Ts: %.3f\r\n"
                "X-Soak-Nonce: reqnonce123456\r\n"
                "X-Soak-Sig: %s\r\n\r\n"
                % (time.time(), sig_placeholder)
            ).encode("utf-8")
            assert_trickle_aborts(hdrs)
        finally:
            for s in socks:
                try:
                    s.close()
                except OSError:
                    pass
            srv_dl.stop()
            srv.stop()

    def test_r2_malformed_http_reply_counts_as_verify_skip(self):
        rs = endurance.remote_soak
        sec_text = secrets.token_hex(32)
        key = rs.derive_control_key(sec_text)

        def serve_raw_once(raw_reply: bytes):
            lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lsock.bind(("127.0.0.1", 0))
            lsock.listen(2)
            lsock.settimeout(5.0)
            port = lsock.getsockname()[1]

            def _srv():
                try:
                    conn, _addr = lsock.accept()
                    with conn:
                        conn.settimeout(5.0)
                        RemoteSoakCryptoAndValidationTests._drain_http_post_request(conn)
                        conn.sendall(raw_reply)
                        try:
                            conn.shutdown(socket.SHUT_WR)
                            conn.settimeout(0.5)
                            conn.recv(64)
                        except OSError:
                            pass
                finally:
                    lsock.close()

            th = threading.Thread(target=_srv, daemon=True)
            th.start()
            return port, th

        # Part A: garbage status line and truncated chunked body both raise ControlProtocolError
        for raw_payload in (
            b"HELLO\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab",
        ):
            port_bad, th_bad = serve_raw_once(raw_payload)
            client = rs.SignedControlClient("http://127.0.0.1:%d" % port_bad, "win-a", key)
            with self.assertRaises(rs.ControlProtocolError):
                client.post("/v1/poll", {"run_id": "run-r2"}, timeout_s=5.0)
            th_bad.join(timeout=5.0)
            self.assertFalse(th_bad.is_alive())

        # Part B: live WorkerAgent survives a garbage status line poll reply, counts verify skip, and completes shutdown
        (_echo_host, echo_port), echo_close = _start_echo_server()
        tmp = tempfile.mkdtemp(prefix="ox-r2-badhttp-")
        srv = None
        port_garbage = None
        th_garbage = None
        try:
            sec_path = os.path.join(tmp, "run.secret")
            with open(sec_path, "w", encoding="utf-8") as f:
                f.write(sec_text + "\n")
            fake_bin = os.path.join(tmp, "fake.bin")
            with open(fake_bin, "wb") as f:
                f.write(b"mock")
            srv = rs.SoakControlServer(
                "127.0.0.1:0",
                key,
                expected_workers=["win-a"],
                expected_count=1,
                sample_interval_s=60.0,
                max_concurrent_per_source=8,
            )
            port_garbage, th_garbage = serve_raw_once(b"HELLO\r\n\r\n")
            garbage_client = rs.SignedControlClient("http://127.0.0.1:%d" % port_garbage, "win-a", key)

            with (
                mock.patch.object(rs.WorkerAgent, "_start_worker", lambda s: 0),
                mock.patch.object(endurance, "sh", side_effect=self._mock_sh_for_fake_bin(fake_bin)),
                mock.patch.object(endurance, "check_free_space", return_value={"ok": True}),
                mock.patch.object(endurance, "hardware", return_value={"cpu": "mock", "cores": 4, "ram_bytes": 8 * endurance.GIB}),
                mock.patch.object(endurance, "os_info", return_value={"system": "MockOS", "release": "1", "machine": "amd64"}),
                mock.patch.object(endurance, "windows_realtime_av_enabled", return_value=False),
            ):
                ag = rs.WorkerAgent(
                    endurance,
                    binary=fake_bin,
                    master_addr="127.0.0.1:%d" % echo_port,
                    control_addr=srv.connect_addr,
                    secret_file=sec_path,
                    name="win-a",
                    work_dir=os.path.join(tmp, "w"),
                    poll_interval_s=0.05,
                    connect_timeout_s=30.0,
                    orphan_timeout_s=30.0,
                )
                orig_post = ag._client.post
                first_poll_done = threading.Event()
                lock_flag = threading.Lock()
                injected = [False]

                def route_first_poll_to_garbage(path, payload, timeout_s=15.0):
                    do_bad = False
                    if path == "/v1/poll":
                        with lock_flag:
                            if not injected[0]:
                                injected[0] = True
                                do_bad = True
                    if do_bad:
                        try:
                            return garbage_client.post(path, payload, timeout_s=5.0)
                        finally:
                            first_poll_done.set()
                    return orig_post(path, payload, timeout_s=timeout_s)

                ag._client.post = route_first_poll_to_garbage  # type: ignore[method-assign]
                rc_box = []
                th_ag = threading.Thread(target=lambda: rc_box.append(ag.run()), daemon=True)
                th_ag.start()
                self.assertEqual(srv.wait_for_agents(1, timeout_s=30.0), ["win-a"])
                self.assertTrue(first_poll_done.wait(timeout=10.0))
                sh_res = srv.dispatch_command("win-a", "shutdown", {}, timeout_s=30.0)["result"]
                th_ag.join(timeout=15.0)
                self.assertFalse(th_ag.is_alive())
                self.assertEqual(rc_box, [0])
                self.assertEqual(sh_res["skipped_polls_verify"], 1)
                self.assertEqual(srv.worker_metadata()[0]["skipped_polls_verify"], 1)
        finally:
            if th_garbage is not None:
                th_garbage.join(timeout=5.0)
            if srv is not None:
                srv.stop()
            echo_close()
            shutil.rmtree(tmp, ignore_errors=True)


@unittest.skipUnless(
    os.environ.get("OXIDESWARM_TEST_BIN"),
    "set OXIDESWARM_TEST_BIN to run the real-binary two-worker-agent loopback soak test",
)
class RemoteSoakRealBinaryTests(unittest.TestCase):
    def test_remote_soak_two_worker_agents_real_binary(self):
        binary = os.path.abspath(os.environ["OXIDESWARM_TEST_BIN"])
        self.assertTrue(os.path.isfile(binary), "missing OXIDESWARM_TEST_BIN: %s" % binary)
        base_root = os.environ.get("OXIDESWARM_TEST_ROOT")
        test_root = tempfile.mkdtemp(prefix="ox-remote-soak-e2e-", dir=base_root or None)
        master_proc = None
        worker_procs = []
        try:
            home_dir = os.path.join(test_root, "home")
            tmp_dir = os.path.join(test_root, "tmp")
            work_master = os.path.join(test_root, "work-master")
            work_a = os.path.join(test_root, "work-win-a")
            work_b = os.path.join(test_root, "work-win-b")
            for d in (os.path.join(home_dir, ".oxideswarm"), tmp_dir, work_master, work_a, work_b):
                os.makedirs(d, exist_ok=True)

            secret_path = os.path.join(test_root, "run.secret")
            secret_text = secrets.token_hex(32)
            with open(secret_path, "w", encoding="utf-8") as f:
                f.write(secret_text + "\n")
            if os.name != "nt":
                os.chmod(secret_path, 0o600)

            master_port_file = os.path.join(test_root, "master.port")
            control_port_file = os.path.join(test_root, "control.port")
            out_json = os.path.join(test_root, "remote_soak_record.json")

            child_env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith("RUSTY_GRID_") and not k.startswith("OXIDESWARM_")
            }
            child_env.update(
                {
                    "HOME": home_dir,
                    "USERPROFILE": home_dir,
                    "OXIDESWARM_HOME": os.path.join(home_dir, ".oxideswarm"),
                    "TEMP": tmp_dir,
                    "TMP": tmp_dir,
                    "TMPDIR": tmp_dir,
                    "PYTHONUNBUFFERED": "1",
                }
            )

            master_cmd = [
                sys.executable,
                "-u",
                SCRIPT,
                "soak",
                "--bin",
                binary,
                "--worker-python",
                sys.executable,
                "--master-only",
                "--master-listen",
                "127.0.0.1:0",
                "--master-port-file",
                master_port_file,
                "--control-listen",
                "127.0.0.1:0",
                "--control-port-file",
                control_port_file,
                "--secret-file",
                secret_path,
                "--expect-workers",
                "win-a,win-b",
                "--workers",
                "2",
                "--slots",
                "4",
                "--max-queue-size",
                "8",
                "--blob-store-mb",
                "512",
                "--blob-cache-mb",
                "256",
                "--duration",
                "250s",
                "--fault-scale",
                "0.03",
                "--load-scale",
                "0.1",
                "--sample-interval",
                "1s",
                "--register-timeout",
                "60s",
                "--work-dir",
                work_master,
                "--out",
                out_json,
            ]
            master_proc = subprocess.Popen(
                master_cmd,
                env=child_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            deadline = time.monotonic() + 30.0
            master_addr = ""
            control_addr = ""
            while time.monotonic() < deadline:
                if master_proc.poll() is not None:
                    out_txt = master_proc.stdout.read() if master_proc.stdout else ""
                    self.fail("master soak exited early (%s):\n%s" % (master_proc.returncode, out_txt))
                if os.path.isfile(master_port_file) and os.path.isfile(control_port_file):
                    with open(master_port_file, encoding="utf-8") as mpf:
                        mtxt = mpf.read().strip()
                    with open(control_port_file, encoding="utf-8") as cpf:
                        ctxt = cpf.read().strip()
                    if mtxt and ctxt:
                        master_addr = mtxt
                        control_addr = ctxt
                        break
                time.sleep(0.1)
            self.assertTrue(master_addr and control_addr, "master/control port files not published in time")

            for wname, wdir in (("win-a", work_a), ("win-b", work_b)):
                wcmd = [
                    sys.executable,
                    "-u",
                    SCRIPT,
                    "worker-agent",
                    "--bin",
                    binary,
                    "--master",
                    master_addr,
                    "--control",
                    control_addr,
                    "--secret-file",
                    secret_path,
                    "--name",
                    wname,
                    "--slots",
                    "4",
                    "--blob-cache-mb",
                    "256",
                    "--sample-interval",
                    "1s",
                    "--poll-interval",
                    "0.25",
                    "--work-dir",
                    wdir,
                ]
                wp = subprocess.Popen(
                    wcmd,
                    env=child_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                worker_procs.append((wname, wp))

            master_out, _ = master_proc.communicate(timeout=420.0)
            worker_outputs = []
            for wname, wp in worker_procs:
                wout, _ = wp.communicate(timeout=60.0)
                worker_outputs.append((wname, wp.returncode, wout))

            raw_text = ""
            rec = None
            if os.path.isfile(out_json):
                with open(out_json, encoding="utf-8") as rf:
                    raw_text = rf.read()
                rec = json.loads(raw_text)

            diag = ""
            if rec is not None:
                soak_obj = rec.get("soak") or {}
                diag = "\nfault_errors=%s\npass_checks=%s" % (
                    json.dumps(soak_obj.get("fault_errors") or [], indent=2),
                    json.dumps(soak_obj.get("pass_checks") or {}, indent=2),
                )
            worker_diag = "".join("\n[%s rc=%s] %s" % (wn, rc, wo[-500:]) for wn, rc, wo in worker_outputs)
            for wname, rc, wout in worker_outputs:
                self.assertEqual(rc, 0, "worker-agent %s failed:\n%s" % (wname, wout))

            self.assertTrue(os.path.isfile(out_json), "missing output record JSON:\n%s%s" % (master_out, worker_diag))
            self.assertIsNotNone(rec)
            endurance.assert_record_private(raw_text, forbidden=[secret_text, test_root, secret_path])
            self.assertTrue(rec["parameters"].get("master_only"))
            self.assertEqual([w["name"] for w in rec["remote_workers"]], ["win-a", "win-b"])

            checks = rec["soak"]["pass_checks"]
            for cname in (
                "1_accounting",
                "2_resource_plateau",
                "5_clean_stop",
                "6_fault_injection",
            ):
                self.assertTrue(checks[cname]["passed"], "%s failed: %s\nmaster_out:\n%s%s%s" % (cname, checks[cname], master_out, diag, worker_diag))

            c4 = checks["4_probe_p99"]
            self.assertEqual(c4.get("ok_probes"), c4.get("total_probes"), "probe failures: %s" % (c4,))
            print("NOTE: 4_probe_p99 = %s" % json.dumps(c4), flush=True)

            c3 = checks["3_disk_and_journal"]
            self.assertTrue(c3["blob_store_ok"], "blob_store_ok failed: %s" % (c3,))
            self.assertTrue(c3["worker_cache_ok"], "worker_cache_ok failed: %s" % (c3,))
            self.assertTrue(c3["journal_compaction_ok"], "journal_compaction_ok failed: %s" % (c3,))
            self.assertEqual(c3["unexpected_sandbox_entries"], {}, "unexpected_sandbox_entries: %s" % (c3,))
            non_part_leftovers = {}
            for key, entries in (c3.get("tmp_leftovers") or {}).items():
                if key.endswith("_cache_tmp"):
                    bad_entries = [name for name in entries if not str(name).endswith(".part")]
                else:
                    bad_entries = list(entries)
                if bad_entries:
                    non_part_leftovers[key] = bad_entries
            self.assertEqual(non_part_leftovers, {}, "unexpected non-.part leftovers: %s" % (non_part_leftovers,))
            # In oxideswarm <= 0.4.4, BlobCache::open keeps tmp/*.part newer than STALE_TMP_AGE (24h)
            # after a hard worker_kill mid-write; 3_disk_and_journal must still catch and fail on it.
            only_worker_kill_part = (
                (not c3["passed"])
                and bool(c3.get("tmp_leftovers"))
                and (not non_part_leftovers)
            )
            if only_worker_kill_part:
                self.assertEqual(master_proc.returncode, 1, "strict harness must exit 1 on tmp_leftovers:\n%s" % (master_out,))
                self.assertFalse(rec["all_passed"])
                print(
                    "NOTE: caught v0.4.4 BlobCache STALE_TMP_AGE .part leftover on worker_kill: %s"
                    % (c3["tmp_leftovers"],),
                    flush=True,
                )
            else:
                self.assertTrue(c3["passed"], "3_disk_and_journal failed: %s%s" % (c3, diag))
                expect_all_passed = bool(c4.get("passed"))
                self.assertEqual(bool(rec["all_passed"]), expect_all_passed, json.dumps(checks, indent=2))
                self.assertEqual(
                    master_proc.returncode,
                    0 if expect_all_passed else 1,
                    "unexpected master soak exit code:\n%s%s%s" % (master_out, diag, worker_diag),
                )
                if expect_all_passed:
                    print("NOTE: all 6/6 D3 checks passed cleanly", flush=True)
                else:
                    print("NOTE: 5/6 functional D3 checks passed; 4_probe_p99 ratio=%s under host load" % (c4.get("ratio"),), flush=True)

            self.assertGreaterEqual(rec["soak"]["master_audit"]["task_api_snapshots"], 1)
            kinds_seen = {e["kind"] for e in rec["soak"]["faults_executed"]}
            self.assertEqual(kinds_seen, {"worker_kill", "link_cut", "queue_burst", "master_restart"})

            seen_procs = set()
            seen_dirs = set()
            for row in rec["soak"]["samples"]:
                seen_procs.update(row.get("processes", {}).keys())
                seen_dirs.update(row.get("dirs", {}).keys())
            self.assertIn("master", seen_procs)
            self.assertIn("worker_win-a", seen_procs)
            self.assertIn("worker_win-b", seen_procs)
            self.assertIn("worker_cache_win-a_bytes", seen_dirs)
            self.assertIn("worker_cache_win-b_bytes", seen_dirs)
        finally:
            if master_proc is not None:
                if master_proc.poll() is None:
                    master_proc.kill()
                    master_proc.wait(timeout=5.0)
                if master_proc.stdout is not None:
                    master_proc.stdout.close()
            for _wname, wp in worker_procs:
                if wp.poll() is None:
                    wp.kill()
                    wp.wait(timeout=5.0)
                if wp.stdout is not None:
                    wp.stdout.close()
            shutil.rmtree(test_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
