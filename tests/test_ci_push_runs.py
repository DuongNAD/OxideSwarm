#!/usr/bin/env python3
"""Tests of scripts/ci_push_runs.py: classification over recorded JSON (no network, stdlib only)."""
import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ci_push_runs", os.path.join(HERE, "..", "scripts", "ci_push_runs.py"))
cpr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpr)


def run(rid, conclusion, status="completed"):
    return {"id": rid, "status": status, "conclusion": conclusion, "head_sha": "0123456789abcdef",
            "created_at": "2026-11-16T10:00:00Z", "event": "push", "head_branch": "master"}


def job(jid, name, conclusion, steps=None):
    return {"id": jid, "name": name, "conclusion": conclusion, "steps": steps or []}


def ann(message, title=""):
    return {"annotation_level": "failure", "title": title, "message": message}


class Classify(unittest.TestCase):
    def test_green_and_unfinished_runs_are_skipped(self):
        self.assertIsNone(cpr.classify_run(run(1, "success"), [job(10, "a", "success")]))
        self.assertIsNone(cpr.classify_run(run(2, None, status="in_progress"), [job(11, "a", None)]))

    def test_cancelled_by_an_account(self):
        jobs = [job(10, "fmt", "cancelled"), job(11, "clippy", "success")]
        res = cpr.classify_run(run(3, "cancelled"), jobs, {10: [ann("The run was canceled by @someone.")]})
        self.assertEqual(res, (cpr.CANCELLED, "fmt"))

    def test_never_acquired(self):
        jobs = [job(10, "clippy (macos-latest)", "cancelled")]
        notes = {10: [ann("The job was not acquired by Runner of type hosted even after multiple attempts"),
                      ann("Capacity for macOS arm64 runners is limited", title="notice")]}
        self.assertEqual(cpr.classify_run(run(4, "failure"), jobs, notes), (cpr.NEVER_ACQUIRED, "clippy (macos-latest)"))

    def test_failing_test_from_a_log(self):
        jobs = [job(10, "workspace tests (macos-latest)", "failure",
                    [{"name": "cargo test", "conclusion": "failure"}])]
        logs = {10: "test result: FAILED. 3 passed; 1 failed"}
        self.assertEqual(cpr.classify_run(run(5, "failure"), jobs, {}, logs)[0], cpr.FAILING_TEST)

    def test_failing_test_line_and_python_unittest(self):
        self.assertTrue(cpr.log_has_failed_test("test foo::bar ... FAILED"))
        self.assertTrue(cpr.log_has_failed_test("FAILED (failures=1)"))
        self.assertFalse(cpr.log_has_failed_test("test foo::bar ... ok\ntest result: ok. 3 passed"))

    def test_failed_test_step_without_a_log(self):
        jobs = [job(10, "tests", "failure", [{"name": "run tests", "conclusion": "failure"}])]
        self.assertEqual(cpr.classify_run(run(6, "failure"), jobs)[0], cpr.FAILING_TEST)

    def test_test_step_with_a_log_but_no_failed_test_is_other(self):
        # exit code -1073741502 before cargo started: the step is a test step, the log has no failure
        jobs = [job(10, "workspace tests (windows-latest)", "failure",
                    [{"name": "cargo test --workspace", "conclusion": "failure"}])]
        logs = {10: ""}
        self.assertEqual(cpr.classify_run(run(7, "failure"), jobs, {}, logs)[0], cpr.OTHER)

    def test_toolchain_download_is_other(self):
        jobs = [job(10, "fuzz (tree_manifest)", "failure", [{"name": "dtolnay/rust-toolchain@master", "conclusion": "failure"}])]
        self.assertEqual(cpr.classify_run(run(8, "failure"), jobs, {10: []}, {10: ""})[0], cpr.OTHER)

    def test_priority_failing_test_over_cancelled(self):
        jobs = [job(10, "a", "cancelled"), job(11, "b", "failure")]
        res = cpr.classify_run(run(9, "failure"), jobs, {10: [ann("The run was canceled by @x.")]},
                               {11: "test result: FAILED. 0 passed; 1 failed"})
        self.assertEqual(res, (cpr.FAILING_TEST, "b"))

    def test_run_without_a_failed_job(self):
        res = cpr.classify_run(run(12, "startup_failure"), [])
        self.assertEqual(res[0], cpr.OTHER)


class Summary(unittest.TestCase):
    def test_window_without_failing_test(self):
        lines = [(run(1, "cancelled"), cpr.CANCELLED, "fmt"), (run(2, "failure"), cpr.NEVER_ACQUIRED, "x")]
        s = cpr.window_summary(lines, 20, "2026-11-16", "2026-11-17")
        self.assertIn("NO run with a failing test", s)
        self.assertIn("1 cancelled", s)
        self.assertIn("1 never acquired", s)

    def test_window_with_failing_test(self):
        s = cpr.window_summary([(run(1, "failure"), cpr.FAILING_TEST, "t")], 5, "a", "b")
        self.assertIn("1 run(s) WITH a failing test", s)

    def test_format_line(self):
        line = cpr.format_line(run(77, "failure"), cpr.OTHER, "fuzz")
        self.assertTrue(line.startswith("77  2026-11-16T10:00:00  0123456"))
        self.assertTrue(line.endswith("fuzz"))


if __name__ == "__main__":
    unittest.main()
