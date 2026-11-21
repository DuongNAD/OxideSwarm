"""Unit tests for the classification logic of scripts/release_skew.py (PROTO-2, design 0076 D2)."""

import importlib.util
import io
import os
import re
import stat
import subprocess
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from unittest import mock

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPEC = importlib.util.spec_from_file_location(
    "release_skew", os.path.join(ROOT, "scripts", "release_skew.py")
)
skew = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(skew)

REFUSAL = (
    "Failed to connect to Master: Incompatible protocol version 10 from client "
    "(supported: 9..=9; upgrade both master and client to matching builds)"
)
WORKERS_OK = "Workers (1 connected, 1 registered):\n- Worker [skew-w1] (Cores: 10)\n"
SUBMIT_OK = "Task abc completed successfully.\nExit Code: 0\nStdout:\nhi\n"


class ClassifyTests(unittest.TestCase):
    def test_works(self):
        verdict, ok, versions, _ = skew.classify(0, WORKERS_OK, 0, SUBMIT_OK, "")
        self.assertEqual((verdict, ok, versions), (skew.WORKS, True, None))

    def test_refused_with_version_error_passes(self):
        verdict, ok, versions, _ = skew.classify(1, REFUSAL, 1, REFUSAL, "")
        self.assertEqual((verdict, ok, versions), (skew.REFUSED, True, (10, 9)))

    def test_version_error_only_in_worker_log_passes(self):
        log = "reason=" + REFUSAL.split(": ", 1)[1]
        verdict, ok, _, _ = skew.classify(0, "no workers", 1, "no workers", log)
        self.assertEqual((verdict, ok), (skew.REFUSED, True))

    def test_refusal_without_version_text_is_broken(self):
        verdict, ok, versions, _ = skew.classify(1, "connection closed", 1, "connection closed", "")
        self.assertEqual((verdict, ok, versions), (skew.BROKEN, False, None))

    def test_same_version_refusal_is_broken(self):
        text = "Incompatible protocol version 9 from client (supported: 9..=9; upgrade)"
        verdict, ok, _, detail = skew.classify(1, text, 1, text, "")
        self.assertEqual((verdict, ok), (skew.BROKEN, False))
        self.assertIn("inside the supported range", detail)

    def test_submit_failure_with_worker_listed_is_broken(self):
        verdict, ok, _, _ = skew.classify(0, WORKERS_OK, 1, "task failed", "")
        self.assertEqual((verdict, ok), (skew.BROKEN, False))

    def test_timeout_is_broken(self):
        verdict, ok, _, _ = skew.classify(124, "[timed out]", 124, "[timed out]", "")
        self.assertEqual((verdict, ok), (skew.BROKEN, False))


class TableTests(unittest.TestCase):
    def test_three_rows(self):
        rows = [("release", "pull request", skew.WORKS, "same or compatible", "task completed")] * 3
        lines = skew.render_table(rows).splitlines()
        self.assertEqual(len(lines), 5)
        self.assertTrue(lines[0].startswith("| Master |"))

    def test_versions_text(self):
        self.assertEqual(skew.versions_text(1, (10, 9)), "release 9, pull request 10")
        self.assertEqual(skew.versions_text(2, (9, 10)), "pull request 10, release 9")
        self.assertEqual(skew.versions_text(3, None), "same or compatible")

    def test_versions_text_with_release_label(self):
        self.assertEqual(
            skew.versions_text(1, (10, 9), release_label="release (v0.3.0)"),
            "release (v0.3.0) 9, pull request 10",
        )
        self.assertEqual(
            skew.versions_text(2, (9, 10), release_label="release (v0.3.0)"),
            "pull request 10, release (v0.3.0) 9",
        )
        self.assertEqual(
            skew.versions_text(3, None, release_label="release (v0.3.0)"),
            "same or compatible",
        )


PROTOCOL_RS = """
/// Current wire protocol version.
pub const PROTOCOL_VERSION_CURRENT: u32 = 10;
/// Minimum wire protocol version compatible with this build.
pub const PROTOCOL_VERSION_MIN_COMPATIBLE: u32 = 9;
"""


class ExpectedVerdictTests(unittest.TestCase):
    """The skew table of design 0083: release master + PR peers, PR master + release peers, PR/PR."""

    def test_parse_protocol_constants(self):
        self.assertEqual(skew.parse_protocol_constants(PROTOCOL_RS), (10, 9))
        with self.assertRaises(ValueError):
            skew.parse_protocol_constants("nothing here")

    def test_window_open_release_one_behind(self):
        # Release 9 (min 9), pull request 10 with the window open (min 9).
        self.assertEqual(
            skew.expected_verdicts((9, 9), (10, 9)), [skew.REFUSED, skew.WORKS, skew.WORKS]
        )

    def test_window_closed(self):
        # The pull request closes the window (min 10): pair 2 is refused too.
        self.assertEqual(
            skew.expected_verdicts((9, 9), (10, 10)), [skew.REFUSED, skew.REFUSED, skew.WORKS]
        )

    def test_same_version(self):
        self.assertEqual(
            skew.expected_verdicts((9, 9), (9, 9)), [skew.WORKS, skew.WORKS, skew.WORKS]
        )

    def test_release_with_its_own_window_accepts_the_newer_peer_only_inside_it(self):
        # A release at 10 with min 9 accepts a pull request at 10 and refuses one at 11.
        self.assertEqual(skew.expected_verdicts((10, 9), (10, 9))[0], skew.WORKS)
        self.assertEqual(skew.expected_verdicts((10, 9), (11, 10))[0], skew.REFUSED)

    def test_pair_two_refused_with_the_window_open_fails_the_job(self):
        want = skew.expected_verdicts((9, 9), (10, 9))[1]
        refusal = "Incompatible protocol version 9 from worker (supported: 10..=10; upgrade)"
        verdict, ok, _, _ = skew.classify(1, refusal, 1, refusal, refusal)
        # The classification alone passes it (9 is outside the refuser's printed range) ...
        self.assertEqual((verdict, ok), (skew.REFUSED, True))
        # ... but the master's window says 9 must work, so the verdict mismatches.
        self.assertTrue(skew.mismatch(verdict, want))

    def test_matching_and_undetermined_verdicts_pass(self):
        self.assertFalse(skew.mismatch(skew.WORKS, skew.WORKS))
        self.assertFalse(skew.mismatch(skew.REFUSED, skew.REFUSED))
        self.assertFalse(skew.mismatch(skew.REFUSED, None))
        self.assertTrue(skew.mismatch(skew.WORKS, skew.REFUSED))

    def test_table_with_expected_column(self):
        rows = [("release", "pull request", skew.REFUSED, "release 9, pull request 10", "d", skew.REFUSED)] * 3
        lines = skew.render_table(rows).splitlines()
        self.assertTrue(lines[0].endswith("| Expected |"))
        self.assertTrue(lines[2].endswith(f"| {skew.REFUSED} |"))
        self.assertEqual(len(lines[0].split("|")), len(lines[2].split("|")))


PROTOCOL_9_RS = """
pub const PROTOCOL_VERSION_CURRENT: u32 = 9;
pub const PROTOCOL_VERSION_MIN_COMPATIBLE: u32 = 9;
"""


class MainCliTests(unittest.TestCase):
    @staticmethod
    def _make_executable(directory, name):
        path = os.path.join(directory, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write("#!/bin/sh\nexit 0\n")
        os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
        return path

    def test_main_with_release_tag_writes_distinct_step_summaries(self):
        with tempfile.TemporaryDirectory() as tmp:
            rel_044_bin = self._make_executable(tmp, "rel-0.4.4")
            rel_030_bin = self._make_executable(tmp, "rel-0.3.0")
            pr_bin = self._make_executable(tmp, "pr-bin")
            proto_10 = os.path.join(tmp, "proto10.rs")
            proto_9 = os.path.join(tmp, "proto9.rs")
            summary_path = os.path.join(tmp, "step_summary.md")
            with open(proto_10, "w", encoding="utf-8") as f:
                f.write(PROTOCOL_RS)
            with open(proto_9, "w", encoding="utf-8") as f:
                f.write(PROTOCOL_9_RS)

            v044_results = [
                (skew.WORKS, True, None, "task completed", ""),
                (skew.WORKS, True, None, "task completed", ""),
                (skew.WORKS, True, None, "task completed", ""),
            ]
            v030_results = [
                (skew.REFUSED, True, (10, 9), "peer speaks 10, refuser supports 9..=9", ""),
                (skew.WORKS, True, None, "task completed", ""),
                (skew.WORKS, True, None, "task completed", ""),
            ]

            with mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": summary_path}):
                with mock.patch.object(skew, "run_pair", side_effect=v044_results) as mock_run:
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        rc1 = skew.main([
                            rel_044_bin,
                            pr_bin,
                            "--release-protocol-rs",
                            proto_10,
                            "--pr-protocol-rs",
                            proto_10,
                            "--release-tag",
                            "v0.4.4",
                        ])
                    self.assertEqual(rc1, 0)
                    self.assertEqual(mock_run.call_count, 3)

                with mock.patch.object(skew, "run_pair", side_effect=v030_results) as mock_run:
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        rc2 = skew.main([
                            rel_030_bin,
                            pr_bin,
                            "--release-protocol-rs",
                            proto_9,
                            "--pr-protocol-rs",
                            proto_10,
                            "--release-tag",
                            "v0.3.0",
                        ])
                    self.assertEqual(rc2, 0)
                    self.assertEqual(mock_run.call_count, 3)

            with open(summary_path, encoding="utf-8") as f:
                summary = f.read()
            self.assertIn("### Release skew (v0.4.4)\n\n", summary)
            self.assertIn("### Release skew (v0.3.0)\n\n", summary)
            self.assertIn("\n\n### Release skew (v0.3.0)", summary)
            self.assertIn(
                "| release (v0.3.0) | pull request | refused with the version error | "
                "release (v0.3.0) 9, pull request 10 |",
                summary,
            )
            self.assertIn("| pull request | release (v0.3.0) | works |", summary)
            self.assertTrue(summary.endswith("\n\n"))

    def test_main_without_release_tag_preserves_default_heading_and_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            rel_bin = self._make_executable(tmp, "rel")
            pr_bin = self._make_executable(tmp, "pr")
            proto_10 = os.path.join(tmp, "proto10.rs")
            summary_path = os.path.join(tmp, "step_summary.md")
            with open(proto_10, "w", encoding="utf-8") as f:
                f.write(PROTOCOL_RS)

            results = [(skew.WORKS, True, None, "task completed", "")] * 3
            with mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": summary_path}):
                with mock.patch.object(skew, "run_pair", side_effect=results):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        rc = skew.main([
                            rel_bin,
                            pr_bin,
                            "--release-protocol-rs",
                            proto_10,
                            "--pr-protocol-rs",
                            proto_10,
                        ])
            self.assertEqual(rc, 0)
            with open(summary_path, encoding="utf-8") as f:
                summary = f.read()
            self.assertTrue(summary.startswith("### Release skew\n\n"))
            self.assertIn("| release | pull request | works |", summary)

    def test_main_mismatch_returns_nonzero_and_writes_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            rel_bin = self._make_executable(tmp, "rel")
            pr_bin = self._make_executable(tmp, "pr")
            proto_10 = os.path.join(tmp, "proto10.rs")
            proto_9 = os.path.join(tmp, "proto9.rs")
            summary_path = os.path.join(tmp, "step_summary.md")
            with open(proto_10, "w", encoding="utf-8") as f:
                f.write(PROTOCOL_RS)
            with open(proto_9, "w", encoding="utf-8") as f:
                f.write(PROTOCOL_9_RS)

            # Pair 2 refused even though window 9..=10 should allow v0.3.0
            bad_results = [
                (skew.REFUSED, True, (10, 9), "peer speaks 10, refuser supports 9..=9", ""),
                (skew.REFUSED, True, (9, 10), "peer speaks 9, refuser supports 10..=10", ""),
                (skew.WORKS, True, None, "task completed", ""),
            ]
            with mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": summary_path}):
                with mock.patch.object(skew, "run_pair", side_effect=bad_results):
                    buf_out = io.StringIO()
                    buf_err = io.StringIO()
                    with redirect_stdout(buf_out), mock.patch("sys.stderr", buf_err):
                        rc = skew.main([
                            rel_bin,
                            pr_bin,
                            "--release-protocol-rs",
                            proto_9,
                            "--pr-protocol-rs",
                            proto_10,
                            "--release-tag",
                            "v0.3.0",
                        ])
            self.assertEqual(rc, 1)
            self.assertIn("FAILED:", buf_err.getvalue())
            with open(summary_path, encoding="utf-8") as f:
                self.assertIn("### Release skew (v0.3.0)", f.read())


class ReleaseSkewCiWorkflowTests(unittest.TestCase):
    """Static checks for PROTO-5 one-protocol-back skew CI job in .github/workflows/ci.yml."""

    @staticmethod
    def _extract_release_skew_block():
        ci_path = os.path.join(ROOT, ".github", "workflows", "ci.yml")
        with open(ci_path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        start = None
        for idx, ln in enumerate(lines):
            if ln.strip() == "release-skew:" and ln.startswith("  "):
                start = idx
                break
        if start is None:
            raise AssertionError("release-skew job block not found in .github/workflows/ci.yml")
        end = len(lines)
        for idx in range(start + 1, len(lines)):
            ln = lines[idx]
            if ln and not ln.startswith("  #") and re.match(r"^  [a-zA-Z0-9_-]+:\s*$", ln):
                end = idx
                break
        return "\n".join(lines[start:end])

    @classmethod
    def _extract_release_skew_run_script(cls):
        block = cls._extract_release_skew_block()
        lines = block.splitlines()
        run_idx = None
        run_indent = None
        for idx, ln in enumerate(lines):
            m = re.match(r"^(\s+)run:\s*\|\s*$", ln)
            if m:
                run_idx = idx
                run_indent = len(m.group(1))
        if run_idx is None:
            raise AssertionError("Multiline run: | block not found in release-skew job")
        script_lines = []
        for ln in lines[run_idx + 1:]:
            if not ln.strip():
                script_lines.append("")
                continue
            indent = len(ln) - len(ln.lstrip(" "))
            if indent <= run_indent:
                break
            script_lines.append(ln)
        return textwrap.dedent("\n".join(script_lines)).lstrip("\n")

    def test_release_skew_ci_pins_previous_protocol_release_and_shas(self):
        block = self._extract_release_skew_block()
        uses_lines = [ln.strip() for ln in block.splitlines() if "uses:" in ln]
        self.assertGreaterEqual(len(uses_lines), 4)
        for uline in uses_lines:
            self.assertRegex(
                uline,
                r"uses:\s+[^@\s]+@[0-9a-f]{40}\s+#\s+v?[0-9]",
                msg=f"expected 40-hex pinned SHA + tag comment in {uline}",
            )

        self.assertIn("PREVIOUS_PROTOCOL_RELEASE: v0.3.0", block)
        self.assertIn("release-dl/${tag}", block)
        self.assertIn("release-bin/${tag}", block)
        self.assertIn("--release-protocol-rs", block)
        self.assertIn("--release-tag", block)

    def test_release_skew_ci_guards_empty_latest_tag_before_previous_release(self):
        script = self._extract_release_skew_run_script()
        guard_pos = script.find('if [ -z "$latest_tag" ]; then')
        prev_pos = script.find('tags+=("${PREVIOUS_PROTOCOL_RELEASE}")')
        self.assertNotEqual(guard_pos, -1, "empty latest_tag guard missing from release-skew script")
        self.assertNotEqual(prev_pos, -1, "PREVIOUS_PROTOCOL_RELEASE append missing from release-skew script")
        self.assertLess(
            guard_pos,
            prev_pos,
            "empty latest_tag guard must run before PREVIOUS_PROTOCOL_RELEASE is appended to tags",
        )
        guard_block = script[guard_pos:prev_pos]
        self.assertIn("::notice::No release exists yet", guard_block)
        self.assertIn("$GITHUB_STEP_SUMMARY", guard_block)
        self.assertIn("exit 0", guard_block)

    @unittest.skipIf(os.name == "nt", "release-skew workflow step runs bash on ubuntu-latest")
    def test_release_skew_ci_bash_empty_latest_tag_exits_cleanly_before_previous_release(self):
        script = self._extract_release_skew_run_script()
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = os.path.join(tmp, "bin")
            os.makedirs(bin_dir)
            marker = os.path.join(tmp, "download_called")
            gh_path = os.path.join(bin_dir, "gh")
            with open(gh_path, "w", encoding="utf-8") as f:
                f.write(
                    "#!/usr/bin/env bash\n"
                    'if [ "${1:-}" = "release" ] && [ "${2:-}" = "view" ]; then\n'
                    "  exit 1\n"
                    "fi\n"
                    f'printf "%s\\n" "$*" >> "{marker}"\n'
                    "exit 99\n"
                )
            os.chmod(gh_path, os.stat(gh_path).st_mode | stat.S_IXUSR)

            summary_path = os.path.join(tmp, "step_summary.md")
            runner_temp = os.path.join(tmp, "runner_temp")
            os.makedirs(runner_temp)

            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
            env["PREVIOUS_PROTOCOL_RELEASE"] = "v0.3.0"
            env["GH_TOKEN"] = "dummy-token"
            env["GH_REPO"] = "fork/OxideSwarm"
            env["GITHUB_STEP_SUMMARY"] = summary_path
            env["RUNNER_TEMP"] = runner_temp

            proc = subprocess.run(
                ["bash", "-c", script],
                cwd=tmp,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                proc.returncode,
                0,
                msg=f"expected rc=0 on empty latest_tag, got {proc.returncode}; stderr={proc.stderr!r}",
            )
            self.assertIn(
                "::notice::No release exists yet, so there is nothing to run against the pull request's build.",
                proc.stdout,
            )
            self.assertFalse(
                os.path.exists(marker),
                "gh release download / api must not be invoked when latest_tag is empty",
            )
            with open(summary_path, encoding="utf-8") as f:
                self.assertEqual(f.read(), "### Release skew\nNo release exists yet.\n")


if __name__ == "__main__":
    unittest.main()
