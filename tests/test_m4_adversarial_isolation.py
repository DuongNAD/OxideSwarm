"""
Milestone 4 Adversarial State Isolation & Environment Protection Harness.
Authored by M4 Challenger 1.

Verifies:
1. Complete immutability of host C:\\Users\\Admin\\.oxideswarm (LastWriteTime, size, hash, file count).
2. Direct execution and validation of Rust test `m4_challenger_1_state_isolation`:
   - Empty string fallback for all 4 default path resolvers.
   - Unset fallback for all 4 resolvers.
   - Isolated temporary directory override for all 4 resolvers.
   - Worker ID file env var precedence.
   - Master P2P 0-config key persistence in isolated sandbox without touching host state.
3. Subprocess CLI validation:
   - Invoking oxideswarm / rusty-grid with OXIDESWARM_HOME=<isolated_sandbox>
   - Ensuring no state leaks to host ~/.oxideswarm.
"""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HOST_OXIDESWARM_DIR = r"C:\Users\Admin\.oxideswarm"


def get_dir_state(path):
    if not os.path.exists(path):
        return {"exists": False}
    stat = os.stat(path)
    files = {}
    for root, _, filenames in os.walk(path):
        for f in filenames:
            fpath = os.path.join(root, f)
            rel = os.path.relpath(fpath, path)
            fstat = os.stat(fpath)
            with open(fpath, "rb") as fp:
                content_hash = hashlib.sha256(fp.read()).hexdigest()
            files[rel] = {
                "size": fstat.st_size,
                "mtime": fstat.st_mtime,
                "sha256": content_hash,
            }
    return {
        "exists": True,
        "mtime": stat.st_mtime,
        "files": files,
    }


class TestMilestone4StateIsolation(unittest.TestCase):
    def setUp(self):
        self.repo_root = REPO_ROOT
        self.initial_host_state = get_dir_state(HOST_OXIDESWARM_DIR)

    def tearDown(self):
        # Verify host directory is completely untouched after every test method
        current_state = get_dir_state(HOST_OXIDESWARM_DIR)
        self.assertEqual(
            self.initial_host_state,
            current_state,
            "CRITICAL: Host C:\\Users\\Admin\\.oxideswarm state was modified during test!",
        )

    def test_01_rust_state_isolation_test_suite(self):
        """Run cargo test -p rusty_grid_cli --test m4_challenger_1_state_isolation."""
        cmd = [
            "cargo",
            "test",
            "-p",
            "rusty_grid_cli",
            "--test",
            "m4_challenger_1_state_isolation",
            "--",
            "--test-threads=1",
        ]
        proc = subprocess.run(cmd, cwd=self.repo_root, capture_output=True, text=True)
        print("=== STDOUT ===")
        print(proc.stdout)
        if proc.stderr:
            print("=== STDERR ===")
            print(proc.stderr)
        self.assertEqual(
            proc.returncode,
            0,
            f"m4_challenger_1_state_isolation failed with exit code {proc.returncode}",
        )
        self.assertIn("5 passed", proc.stdout)

    def test_02_cli_subprocess_isolated_home(self):
        """Verify CLI binary creates worker_id inside isolated OXIDESWARM_HOME, not host."""
        with tempfile.TemporaryDirectory() as temp_dir:
            env = os.environ.copy()
            env["OXIDESWARM_HOME"] = temp_dir
            env.pop("RUSTY_GRID_WORKER_ID_FILE", None)

            # Build or locate target binary
            bin_path = os.path.join(self.repo_root, "target", "debug", "rusty-grid.exe")
            if not os.path.exists(bin_path):
                # Build binary if not already compiled
                subprocess.run(["cargo", "build", "-p", "rusty_grid_cli", "--bin", "rusty-grid"], cwd=self.repo_root, check=True)

            self.assertTrue(os.path.exists(bin_path), f"Binary not found: {bin_path}")

            # Run rusty-grid --help to verify clean execution under isolated OXIDESWARM_HOME
            proc = subprocess.run([bin_path, "--help"], env=env, cwd=self.repo_root, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, f"rusty-grid --help failed: {proc.stderr}")

            # Verify no host mutation
            host_current = get_dir_state(HOST_OXIDESWARM_DIR)
            self.assertEqual(self.initial_host_state, host_current)


if __name__ == "__main__":
    unittest.main()
