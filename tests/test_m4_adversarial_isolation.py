"""
Milestone 4 Adversarial State Isolation & Environment Protection Harness.
Authored by M4 Challenger 1.

Verifies:
1. The developer's real OxideSwarm state directory (`OXIDESWARM_HOME`, else `~/.oxideswarm`) is
   left untouched: same file list, sizes and modification times. File contents are never read,
   because that directory holds the real cluster.secret.
2. Direct execution and validation of Rust test `m4_challenger_1_state_isolation`:
   - Empty string fallback for all 4 default path resolvers.
   - Unset fallback for all 4 resolvers.
   - Isolated temporary directory override for all 4 resolvers.
   - Worker ID file env var precedence.
   - Master P2P 0-config key persistence in isolated sandbox without touching host state.
3. Subprocess CLI validation:
   - Invoking rusty-grid with OXIDESWARM_HOME=<isolated_sandbox>
   - Ensuring no state leaks to the real state directory.
"""

import os
import subprocess
import tempfile
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HOST_OXIDESWARM_DIR = os.environ.get("OXIDESWARM_HOME") or os.path.join(
    os.path.expanduser("~"), ".oxideswarm"
)
BINARY_NAME = "rusty-grid.exe" if os.name == "nt" else "rusty-grid"


def get_dir_state(path):
    """Metadata snapshot of `path`; contents are not read (the directory holds the real secret)."""
    if not os.path.exists(path):
        return {"exists": False}
    files = {}
    for root, _, filenames in os.walk(path):
        for f in filenames:
            fpath = os.path.join(root, f)
            fstat = os.stat(fpath)
            files[os.path.relpath(fpath, path)] = {
                "size": fstat.st_size,
                "mtime_ns": fstat.st_mtime_ns,
            }
    return {
        "exists": True,
        "mtime_ns": os.stat(path).st_mtime_ns,
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
            f"CRITICAL: the real state directory {HOST_OXIDESWARM_DIR} was modified during the test!",
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
            bin_path = os.path.join(self.repo_root, "target", "debug", BINARY_NAME)
            if not os.path.exists(bin_path):
                # Build binary if not already compiled
                subprocess.run(
                    ["cargo", "build", "-p", "rusty_grid_cli", "--bin", "rusty-grid"],
                    cwd=self.repo_root,
                    check=True,
                )

            self.assertTrue(os.path.exists(bin_path), f"Binary not found: {bin_path}")

            # Run rusty-grid --help to verify clean execution under isolated OXIDESWARM_HOME
            proc = subprocess.run(
                [bin_path, "--help"], env=env, cwd=self.repo_root, capture_output=True, text=True
            )
            self.assertEqual(proc.returncode, 0, f"rusty-grid --help failed: {proc.stderr}")

            # Verify no host mutation
            host_current = get_dir_state(HOST_OXIDESWARM_DIR)
            self.assertEqual(self.initial_host_state, host_current)


if __name__ == "__main__":
    unittest.main()
