#!/usr/bin/env python3
"""
tests/test_study_timer.py - Unit tests for scripts/study_timer.py (ADOPT-1).

Standard library only; requires no built oxideswarm binary. Tests participant label
validation, consent gating, PII/secret filtering, fake-clock step timing, interactive
session prompts, M15-M17 measure derivation, Markdown/JSON summaries, and CLI invocations.

Run:   python3 tests/test_study_timer.py
"""

from __future__ import annotations

import datetime
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "scripts", "study_timer.py")
spec = importlib.util.spec_from_file_location("study_timer", SCRIPT)
study_timer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(study_timer)


class FakeClock:
    def __init__(self, start_epoch: float = 1_763_568_000.0) -> None:
        self._mono = 1000.0
        self._epoch = start_epoch

    def advance(self, seconds: float) -> None:
        self._mono += seconds
        self._epoch += seconds

    def monotonic(self) -> float:
        return self._mono

    def now_utc(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self._epoch, tz=datetime.timezone.utc)


class ValidationTests(unittest.TestCase):
    def test_participant_labels_allow_only_p1_through_p5(self):
        for label in ("P1", "P2", "P3", "P4", "P5"):
            self.assertEqual(study_timer.validate_participant(label), label)
        for bad in ("alice", "P0", "P6", "P1-mac", "macbook", "p1", ""):
            with self.assertRaises(ValueError, msg=f"expected rejection for {bad!r}"):
                study_timer.validate_participant(bad)

    def test_consent_is_required_to_init_or_run(self):
        with tempfile.TemporaryDirectory() as td:
            rec = os.path.join(td, "p1.json")
            with self.assertRaises(ValueError):
                study_timer.init_session(rec, "P1", "macos/aarch64", "readme", "0.4.4", consent=False)
            self.assertFalse(os.path.exists(rec))

            out = io.StringIO()
            with self.assertRaises(ValueError):
                study_timer.run_interactive(
                    rec,
                    "P1",
                    "macos/aarch64",
                    "readme",
                    "0.4.4",
                    consent=False,
                    input_stream=io.StringIO("\n"),
                    output_stream=out,
                )
            self.assertFalse(os.path.exists(rec))

    def test_pii_and_secret_guard_blocks_unsafe_strings(self):
        unsafe_samples = [
            "token oxs1-invite:abcd1234",
            "copied cluster.secret manually",
            "deleted node.key from disk",
            "opened invite.oxs file",
            "connected to 192.0.2.1 on LAN",
            "emailed person@example.invalid after session",
            "path /Users/example/doc/project failed",
            "looked in /home/example/doc/.cargo",
            r"checked C:\Users\example\doc\file.txt",
        ]
        for sample in unsafe_samples:
            with self.assertRaises(ValueError, msg=f"expected rejection for {sample!r}"):
                study_timer.validate_safe_text("note", sample)

        safe_samples = [
            "",
            "hesitated on SHA256SUMS command under PowerShell",
            "checked ~/ placeholder and <STUDY_MASTER_IP>:<STUDY_PORT>",
            "missing PATH entry after unpacking archive on macOS arm64",
        ]
        for sample in safe_samples:
            self.assertEqual(study_timer.validate_safe_text("note", sample), sample)


class LifecycleAndMeasuresTests(unittest.TestCase):
    def test_full_lifecycle_derives_m15_m16_m17_and_excludes_post_first_result_help(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as td:
            rec = os.path.join(td, "p1.json")
            study_timer.init_session(
                rec,
                "P1",
                "windows/x86_64",
                "readme",
                "0.4.4",
                consent=True,
                second_os="macos/aarch64",
                now_utc=clock.now_utc,
            )

            # install: 180 s with a pause, no help
            study_timer.begin_step(rec, "install", now_utc=clock.now_utc, monotonic=clock.monotonic)
            clock.advance(60.0)
            study_timer.record_event(
                rec,
                "install",
                kind="pause",
                note="read xattr note in README",
                now_utc=clock.now_utc,
                monotonic=clock.monotonic,
            )
            clock.advance(120.0)
            study_timer.end_step(rec, "install", now_utc=clock.now_utc, monotonic=clock.monotonic)

            # join: 150 s, no help
            study_timer.begin_step(rec, "join", now_utc=clock.now_utc, monotonic=clock.monotonic)
            clock.advance(150.0)
            study_timer.end_step(rec, "join", now_utc=clock.now_utc, monotonic=clock.monotonic)

            # first_task: 90 s -> cumulative to first result = 420 s = 7.0 min, unaided
            study_timer.begin_step(rec, "first_task", now_utc=clock.now_utc, monotonic=clock.monotonic)
            clock.advance(90.0)
            study_timer.end_step(rec, "first_task", now_utc=clock.now_utc, monotonic=clock.monotonic)

            # Help on file_round_trip AFTER first_task must NOT flip m16_unaided_first_result to False
            study_timer.begin_step(
                rec, "file_round_trip", now_utc=clock.now_utc, monotonic=clock.monotonic
            )
            clock.advance(45.0)
            study_timer.record_event(
                rec,
                "file_round_trip",
                kind="help",
                help_level="hint",
                note="pointed at --output-dir syntax",
                now_utc=clock.now_utc,
                monotonic=clock.monotonic,
            )
            clock.advance(45.0)
            study_timer.end_step(
                rec, "file_round_trip", now_utc=clock.now_utc, monotonic=clock.monotonic
            )

            study_timer.begin_step(rec, "own_job", now_utc=clock.now_utc, monotonic=clock.monotonic)
            clock.advance(60.0)
            study_timer.end_step(rec, "own_job", now_utc=clock.now_utc, monotonic=clock.monotonic)
            study_timer.record_own_job(rec, "Rust test suite (A2)", "yes", note="cargo test passed")

            study_timer.begin_step(
                rec, "stop_worker", now_utc=clock.now_utc, monotonic=clock.monotonic
            )
            clock.advance(10.0)
            study_timer.end_step(
                rec, "stop_worker", now_utc=clock.now_utc, monotonic=clock.monotonic
            )

            final = study_timer.record_followup(
                rec, "yes", reason="offloaded cargo check", date="2026-12-03"
            )
            m = final["measures"]
            self.assertEqual(m["m15_first_result_min"], 7.0)
            self.assertIs(m["m16_unaided_first_result"], True)
            self.assertEqual(m["m17_repeat_use_14d"], "yes")
            self.assertEqual(m["help_count_to_first_result"], 0)
            self.assertEqual(m["help_count_total"], 1)
            self.assertEqual(m["pause_count_total"], 1)
            self.assertEqual(m["total_elapsed_s"], 580.0)

    def test_help_before_first_result_marks_m16_false(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as td:
            rec = os.path.join(td, "p2.json")
            study_timer.init_session(
                rec, "P2", "linux/x86_64", "readme", "0.4.4", consent=True, now_utc=clock.now_utc
            )
            for step_id, secs in (("install", 120.0), ("join", 240.0), ("first_task", 60.0)):
                study_timer.begin_step(rec, step_id, now_utc=clock.now_utc, monotonic=clock.monotonic)
                if step_id == "join":
                    clock.advance(30.0)
                    study_timer.record_event(
                        rec,
                        "join",
                        kind="help",
                        help_level="command",
                        note="clarified pbpaste pipe into join",
                        now_utc=clock.now_utc,
                        monotonic=clock.monotonic,
                    )
                    clock.advance(secs - 30.0)
                else:
                    clock.advance(secs)
                study_timer.end_step(rec, step_id, now_utc=clock.now_utc, monotonic=clock.monotonic)

            data = study_timer.load_record(rec)
            self.assertEqual(data["measures"]["m15_first_result_min"], 7.0)
            self.assertIs(data["measures"]["m16_unaided_first_result"], False)
            self.assertEqual(data["measures"]["help_count_to_first_result"], 1)

    def test_interactive_run_handles_pauses_help_skip_and_quit(self):
        clock = FakeClock()
        scripted_lines = [
            "",  # start install
            "p read quarantine instructions",  # pause during install
            "",  # complete install
            "",  # start join
            "h hint reminded about stdin pipe",  # help during join
            "",  # complete join
            "",  # start first_task
            "",  # complete first_task
            "s skipped optional check in test",  # skip file_round_trip
            "q",  # quit before own_job
        ]
        # Advance clock on each prompt read via custom wrapper so durations are non-zero.
        class SteppingInput:
            def __init__(self, lines):
                self._lines = list(lines)

            def readline(self) -> str:
                if not self._lines:
                    return ""
                clock.advance(15.0)
                return self._lines.pop(0) + "\n"

        out = io.StringIO()
        with tempfile.TemporaryDirectory() as td:
            rec = os.path.join(td, "p3.json")
            result = study_timer.run_interactive(
                rec,
                "P3",
                "macos/aarch64",
                "usb-kit",
                "0.4.4",
                consent=True,
                now_utc=clock.now_utc,
                monotonic=clock.monotonic,
                input_stream=SteppingInput(scripted_lines),
                output_stream=out,
            )
            self.assertEqual(result["participant"], "P3")
            self.assertEqual(result["steps"]["install"]["outcome"], "completed")
            self.assertEqual(result["steps"]["install"]["elapsed_s"], 30.0)
            self.assertEqual(result["steps"]["join"]["elapsed_s"], 30.0)
            self.assertEqual(result["steps"]["first_task"]["elapsed_s"], 15.0)
            self.assertEqual(result["steps"]["file_round_trip"]["outcome"], "skipped")
            self.assertEqual(result["steps"]["own_job"]["status"], "pending")
            self.assertEqual(result["measures"]["m15_first_result_min"], 1.25)
            self.assertIs(result["measures"]["m16_unaided_first_result"], False)
            self.assertEqual(result["measures"]["pause_count_total"], 1)


class SummarizeAndCliTests(unittest.TestCase):
    def _write_synthetic_session(
        self,
        path: str,
        participant: str,
        durations: tuple[float, float, float],
        aided: bool,
        repeat_use: str | None,
    ) -> None:
        clock = FakeClock()
        study_timer.init_session(
            path,
            participant,
            "macos/aarch64",
            "readme",
            "0.4.4",
            consent=True,
            second_os="windows/x86_64",
            now_utc=clock.now_utc,
        )
        for step_id, secs in zip(study_timer.FIRST_RESULT_STEPS, durations):
            study_timer.begin_step(path, step_id, now_utc=clock.now_utc, monotonic=clock.monotonic)
            if aided and step_id == "join":
                clock.advance(secs / 2.0)
                study_timer.record_event(
                    path,
                    "join",
                    kind="help",
                    help_level="hint",
                    note="explained firewall prompt",
                    now_utc=clock.now_utc,
                    monotonic=clock.monotonic,
                )
                clock.advance(secs / 2.0)
            else:
                clock.advance(secs)
            study_timer.end_step(path, step_id, now_utc=clock.now_utc, monotonic=clock.monotonic)
        study_timer.record_own_job(path, "cargo test", "yes")
        if repeat_use is not None:
            study_timer.record_followup(path, repeat_use, date="2026-12-03")

    def test_summarize_markdown_and_json_aggregates(self):
        with tempfile.TemporaryDirectory() as td:
            p2 = os.path.join(td, "p2.json")
            p1 = os.path.join(td, "p1.json")
            p3 = os.path.join(td, "p3.json")
            self._write_synthetic_session(p1, "P1", (120.0, 120.0, 60.0), aided=False, repeat_use="yes")  # 5.0 min
            self._write_synthetic_session(p2, "P2", (180.0, 240.0, 60.0), aided=True, repeat_use="no")  # 8.0 min
            self._write_synthetic_session(p3, "P3", (240.0, 300.0, 60.0), aided=False, repeat_use=None)  # 10.0 min

            summary = study_timer.summarize_records([p3, p1, p2])
            self.assertEqual(summary["session_count"], 3)
            self.assertEqual([s["participant"] for s in summary["sessions"]], ["P1", "P2", "P3"])
            self.assertEqual(summary["m15_median_min"], 8.0)
            self.assertEqual(summary["m15_min_min"], 5.0)
            self.assertEqual(summary["m15_max_min"], 10.0)
            self.assertEqual(summary["m16_eligible_count"], 3)
            self.assertEqual(summary["m16_unaided_count"], 2)
            self.assertEqual(summary["m17_answered_count"], 2)
            self.assertEqual(summary["m17_repeat_yes_count"], 1)

            md = study_timer.format_markdown_summary(summary)
            self.assertIn("| P1 |", md)
            self.assertIn("| P2 |", md)
            self.assertIn("| P3 |", md)
            self.assertIn("median 8.00 min (min 5.00, max 10.00; n=3)", md)
            self.assertIn("**M16 (unaided completion):** 2 / 3.", md)
            self.assertIn("**M17 (14-day repeat use):** 1 / 2 (out of 3 sessions).", md)

    def test_cli_subcommands_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            rec = os.path.join(td, "p4.json")
            base = [sys.executable, SCRIPT]
            subprocess.run(
                base
                + [
                    "init",
                    "--record",
                    rec,
                    "--participant",
                    "P4",
                    "--os",
                    "linux/x86_64",
                    "--install-method",
                    "readme",
                    "--release",
                    "0.4.4",
                    "--consent",
                ],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            for step_id in ("install", "join", "first_task"):
                subprocess.run(
                    base + ["begin", "--record", rec, "--step", step_id],
                    check=True,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                )
                if step_id == "install":
                    subprocess.run(
                        base
                        + [
                            "event",
                            "--record",
                            rec,
                            "--step",
                            "install",
                            "--kind",
                            "pause",
                            "--note",
                            "checked sha256sum syntax",
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                    )
                subprocess.run(
                    base + ["end", "--record", rec, "--step", step_id, "--outcome", "completed"],
                    check=True,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                )
            res = subprocess.run(
                base + ["summarize", rec, "--format", "json"],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            payload = json.loads(res.stdout)
            self.assertEqual(payload["session_count"], 1)
            self.assertEqual(payload["m16_unaided_count"], 1)
            self.assertEqual(payload["sessions"][0]["participant"], "P4")


if __name__ == "__main__":
    unittest.main()
