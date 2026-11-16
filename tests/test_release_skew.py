"""Unit tests for the classification logic of scripts/release_skew.py (PROTO-2, design 0076 D2)."""

import importlib.util
import os
import unittest

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


if __name__ == "__main__":
    unittest.main()
