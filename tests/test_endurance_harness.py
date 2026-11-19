#!/usr/bin/env python3
"""
tests/test_endurance_harness.py - Unit tests for benchmarks/endurance/endurance.py (TEST-24).

Standard library only; requires no built oxideswarm binary. Tests the seeded generators,
disk space guard, TaskLedger, D2/D3 threshold math, loopback ThrottleProxy against a
local TCP echo server, process/directory samplers, and record redaction/privacy checks.

Run:   python3 tests/test_endurance_harness.py
"""

from __future__ import annotations

import datetime
import hashlib
import importlib.util
import json
import os
import random
import re
import shutil
import socket
import tempfile
import threading
import time
import unittest
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


class EnduranceCiWorkflowTests(unittest.TestCase):
    """Static checks for TEST-26 scheduled/dispatch `endurance` job in .github/workflows/ci.yml."""

    @staticmethod
    def _extract_endurance_block():
        ci_path = os.path.join(HERE, "..", ".github", "workflows", "ci.yml")
        with open(ci_path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        start = None
        for idx, ln in enumerate(lines):
            if ln.strip() == "endurance:" and ln.startswith("  "):
                start = idx
                break
        if start is None:
            raise AssertionError("endurance job block not found in .github/workflows/ci.yml")
        end = len(lines)
        for idx in range(start + 1, len(lines)):
            ln = lines[idx]
            if ln and not ln.startswith("  #") and re.match(r"^  [a-zA-Z0-9_-]+:\s*$", ln):
                end = idx
                break
        return "\n".join(lines[start:end])

    def test_endurance_ci_job_schedule_dispatch_pins_and_commands(self):
        block = self._extract_endurance_block()
        self.assertIn("if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'", block)
        self.assertIn("runs-on: ubuntu-latest", block)

        uses_lines = [ln.strip() for ln in block.splitlines() if "uses:" in ln]
        self.assertGreaterEqual(len(uses_lines), 5)
        for uline in uses_lines:
            self.assertRegex(
                uline,
                r"uses:\s+[^@\s]+@[0-9a-f]{40}\s+#\s+v?[0-9]",
                msg="expected 40-hex pinned SHA + tag comment in %s" % uline,
            )

        self.assertIn("--scenarios T1,T2,T4,T6,T7,T8", block)
        self.assertIn("--t1-sizes-gib 1", block)
        self.assertIn("--t2-files 20000", block)
        self.assertRegex(block, r"endurance\.py\s+soak\b")
        self.assertIn("--duration 45m", block)
        self.assertRegex(block, r"if:\s*always\(\)\s*\n\s*uses:\s*actions/upload-artifact@[0-9a-f]{40}\s+#")


if __name__ == "__main__":
    unittest.main()
