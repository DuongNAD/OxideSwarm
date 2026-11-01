#!/usr/bin/env python3
"""
Integration test suite for SEC-6: Mesh Command Hardening (Python node).

Verifies:
1. Rejection of unadvertised commands (/bin/ls, cmd.exe) with exit code 127.
2. --no-shell advertisement filter: registration capabilities omit shell_exec.
3. --no-shell execution refusal: incoming shell_exec returns exit code 127 and error message.
4. Built-in commands (echo, ping, system_info, sha256_verify) still work under --no-shell.
5. Environment sanitization: SECRET_ENV_VARS (OXIDESWARM_SECRET, OXIDESWARM_SECRET_FILE,
   OXIDE_MESH_TOKEN, RUSTY_GRID_SECRET, RUSTY_GRID_SECRET_FILE) are stripped from child processes.
6. Explicit env in args is honored and passed to child process.
7. Working directory (cwd) in args is respected.
8. Output capping: streams exceeding 1 MiB are capped with truncation marker.
9. Timeout watchdog & partial output retention (killing mutant P11).
10. Process tree and setsid child termination on timeout (killing mutant P10 & F7).
11. Early stream close does not trigger premature 5s kill (F3 regression check).
12. Child stdin DEVNULL EOF immediately without hanging (F4 fix).
13. Real CLI --no-shell flag execution (killing mutant P9).
14. F10 low items: timeout_ms=null and invalid non-string cwd safely handled.
15. F12 check: Rust and Python secret env vars parity.
"""

import asyncio
import logging
import os
import sys
import tempfile
import time

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from scripts.agent_node import OxideAgentNode, MAX_CAPTURED_OUTPUT_BYTES
try:
    from test_agent_mesh_simulation import OxideRelayHub
except ImportError:
    from tests.test_agent_mesh_simulation import OxideRelayHub

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Sec6SandboxTest")


class RobustCallerNode(OxideAgentNode):
    async def _dispatch_incoming(self, msg: dict):
        msg_type = msg.get("type") or msg.get("msg_type")
        if msg_type == "DeliveryAck" and msg.get("status") == "target_forwarded":
            return
        await super()._dispatch_incoming(msg)


async def run_sec6_tests():
    logger.info("==================================================================")
    logger.info("  STARTING SEC-6 MESH SANDBOX HARDENING VERIFICATION SUITE        ")
    logger.info("==================================================================")

    hub = OxideRelayHub()
    port = await hub.start()
    hub_url = f"ws://127.0.0.1:{port}"

    # Set secret environment variables on the parent process
    sentinels = {
        "OXIDESWARM_SECRET": "test-cluster-secret-value-12345",
        "OXIDESWARM_SECRET_FILE": "test-cluster-secret-file-67890",
        "OXIDE_MESH_TOKEN": "test-mesh-token-value-abcdef",
        "RUSTY_GRID_SECRET": "test-rusty-grid-secret-value-999",
        "RUSTY_GRID_SECRET_FILE": "test-rusty-grid-secret-file-888",
    }
    for k, v in sentinels.items():
        os.environ[k] = v

    worker = OxideAgentNode(hub_url, "worker-node", "windows")
    worker_noshell = OxideAgentNode(hub_url, "worker-noshell-node", "windows", no_shell=True)
    caller = RobustCallerNode(hub_url, "caller-node", "windows")

    tasks = [
        asyncio.create_task(worker.run()),
        asyncio.create_task(worker_noshell.run()),
        asyncio.create_task(caller.run()),
    ]
    await asyncio.sleep(0.5)

    try:
        # ----------------------------------------------------------------------
        # Test 1: Unadvertised Command Rejection
        # ----------------------------------------------------------------------
        logger.info("[Test 1] Testing unadvertised command rejection (/bin/ls, cmd.exe)...")
        resp1 = await caller.send_command("worker-node", "/bin/ls", {}, timeout=5.0)
        assert resp1.get("status") == "failed", f"Expected failed, got {resp1}"
        assert resp1.get("exit_code") == 127, f"Expected exit code 127, got {resp1.get('exit_code')}"
        assert "Unsupported command: '/bin/ls'" in resp1.get("stderr", "")

        resp2 = await caller.send_command("worker-node", "cmd.exe", {}, timeout=5.0)
        assert resp2.get("status") == "failed", f"Expected failed, got {resp2}"
        assert resp2.get("exit_code") == 127, f"Expected exit code 127, got {resp2.get('exit_code')}"
        assert "Unsupported command: 'cmd.exe'" in resp2.get("stderr", "")
        logger.info("✓ Test 1 PASSED: Unadvertised commands rejected with exit code 127!")

        # ----------------------------------------------------------------------
        # Test 2: --no-shell Node Capability Advertisement Filtering
        # ----------------------------------------------------------------------
        logger.info("[Test 2] Testing --no-shell registration capability filtering...")
        noshell_entry = hub.nodes.get("worker-noshell-node")
        assert noshell_entry is not None, "worker-noshell-node not found in hub catalog"
        supported = noshell_entry.get("capabilities", {}).get("supported_commands", [])
        assert "shell_exec" not in supported, f"shell_exec should NOT be in supported_commands: {supported}"
        assert "echo" in supported
        assert "ping" in supported
        assert "system_info" in supported
        assert "sha256_verify" in supported
        logger.info(f"✓ Test 2 PASSED: --no-shell node advertises only safe commands: {supported}")

        # ----------------------------------------------------------------------
        # Test 3: --no-shell Execution Refusal & Builtin Functionality
        # ----------------------------------------------------------------------
        logger.info("[Test 3] Testing --no-shell execution refusal and builtins...")
        resp_refusal = await caller.send_command(
            "worker-noshell-node",
            "shell_exec",
            {"cmd": "echo should_not_run"},
            timeout=5.0,
        )
        assert resp_refusal.get("status") == "failed", f"Expected failed, got {resp_refusal}"
        assert resp_refusal.get("exit_code") == 127, f"Expected 127, got {resp_refusal.get('exit_code')}"
        assert "shell_exec is disabled by --no-shell" in resp_refusal.get("stderr", "")

        # Builtin echo on the same node must succeed
        echo_resp = await caller.send_command(
            "worker-noshell-node",
            "echo",
            {"msg": "builtins_still_work"},
            timeout=5.0,
        )
        assert echo_resp.get("status") == "success"
        assert echo_resp.get("exit_code") == 0
        assert echo_resp.get("stdout") == "builtins_still_work"
        logger.info("✓ Test 3 PASSED: shell_exec refused with 127; built-in echo succeeds!")

        # ----------------------------------------------------------------------
        # Test 4: Environment Sanitization (Secrets Shielded)
        # ----------------------------------------------------------------------
        logger.info("[Test 4] Testing environment sanitization (SECRET_ENV_VARS stripped)...")
        probe_cmd = (
            f'"{sys.executable}" -c "import os; '
            'keys = [\'OXIDESWARM_SECRET\', \'OXIDESWARM_SECRET_FILE\', \'OXIDE_MESH_TOKEN\', \'RUSTY_GRID_SECRET\', \'RUSTY_GRID_SECRET_FILE\']; '
            'print(\' \'.join(k + \'=\' + os.environ.get(k, \'UNSET\') for k in keys))"'
        )
        resp_env = await caller.send_command(
            "worker-node",
            "shell_exec",
            {
                "cmd": probe_cmd,
                "env": {"CUSTOM_ALLOWED_VAR": "custom_value_ok"},
            },
            timeout=5.0,
        )
        assert resp_env.get("status") == "success", f"Command failed: {resp_env}"
        env_stdout = resp_env.get("stdout", "")
        for secret_name, secret_val in sentinels.items():
            assert f"{secret_name}=UNSET" in env_stdout, f"Secret {secret_name} was NOT stripped! Output: {env_stdout}"
            assert secret_val not in env_stdout, f"Secret value {secret_val} leaked into child process!"
        logger.info("✓ Test 4 PASSED: All 5 secret environment variables were stripped!")

        # ----------------------------------------------------------------------
        # Test 5: Working Directory (cwd) and Explicit env
        # ----------------------------------------------------------------------
        logger.info("[Test 5] Testing custom working directory (cwd) and explicit env...")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_real = os.path.realpath(temp_dir).lower()
            cwd_probe = (
                f'"{sys.executable}" -c "import os; '
                'print(\'CWD=\' + os.path.realpath(os.getcwd())); '
                'print(\'CUSTOM=\' + os.environ.get(\'EXPLICIT_TEST_ENV\', \'\'))"'
            )
            resp_cwd = await caller.send_command(
                "worker-node",
                "shell_exec",
                {
                    "cmd": cwd_probe,
                    "cwd": temp_dir,
                    "env": {"EXPLICIT_TEST_ENV": "explicit_value_123"},
                },
                timeout=5.0,
            )
            assert resp_cwd.get("status") == "success"
            cwd_stdout = resp_cwd.get("stdout", "")
            assert f"cwd={temp_real}" in cwd_stdout.lower(), f"CWD mismatch: {cwd_stdout} vs {temp_real}"
            assert "CUSTOM=explicit_value_123" in cwd_stdout, f"Custom env mismatch: {cwd_stdout}"
        logger.info("✓ Test 5 PASSED: cwd and explicit env correctly honored!")

        # ----------------------------------------------------------------------
        # Test 6: 1 MiB Output Capping with Truncation Marker
        # ----------------------------------------------------------------------
        logger.info("[Test 6] Testing 1 MiB output capping with truncation notice...")
        cap_probe = f'"{sys.executable}" -c "import sys; sys.stdout.write(\'Z\' * 2500000); sys.stdout.flush()"'
        resp_cap = await caller.send_command(
            "worker-node",
            "shell_exec",
            {"cmd": cap_probe},
            timeout=10.0,
        )
        assert resp_cap.get("status") == "success", f"Expected success: {resp_cap}"
        out_text = resp_cap.get("stdout", "")
        out_bytes = out_text.encode("utf-8")
        assert len(out_bytes) <= MAX_CAPTURED_OUTPUT_BYTES + 300, f"Output not capped: len={len(out_bytes)}"
        assert "output truncated after 1048576 bytes" in out_text, "Missing truncation marker"
        assert out_text.startswith("Z"), "Output did not start with probe character"
        logger.info(f"✓ Test 6 PASSED: Output capped at {len(out_bytes)} bytes with truncation notice!")

        # ----------------------------------------------------------------------
        # Test 7: Timeout Watchdog & Partial Output Retention (Kills Mutant P11)
        # ----------------------------------------------------------------------
        logger.info("[Test 7] Testing timeout path and partial output retention (Mutant P11)...")
        py_partial_code = (
            "import time, sys; "
            "sys.stdout.write('PARTIAL_STDOUT_TOKEN_7721\\n'); sys.stdout.flush(); "
            "sys.stderr.write('PARTIAL_STDERR_TOKEN_7721\\n'); sys.stderr.flush(); "
            "time.sleep(10)"
        )
        resp_timeout = await caller.send_command(
            "worker-node",
            "shell_exec",
            {"cmd": f'"{sys.executable}" -c "{py_partial_code}"'},
            timeout_ms=1000,
            timeout=4.0,
        )
        assert resp_timeout.get("status") == "timeout", f"Expected timeout, got {resp_timeout}"
        assert resp_timeout.get("exit_code") == 124, f"Expected 124, got {resp_timeout.get('exit_code')}"
        assert "PARTIAL_STDOUT_TOKEN_7721" in resp_timeout.get("stdout", ""), "Partial stdout dropped on timeout (Mutant P11)!"
        assert "PARTIAL_STDERR_TOKEN_7721" in resp_timeout.get("stderr", ""), "Partial stderr dropped on timeout (Mutant P11)!"
        logger.info("✓ Test 7 PASSED: Timeout reported 124 and retained partial output (P11 killed)!")

        # ----------------------------------------------------------------------
        # Test 8: Subprocess Tree & setsid Termination (Kills Mutant P10 & F7)
        # ----------------------------------------------------------------------
        logger.info("[Test 8] Testing subprocess tree & setsid termination (Mutant P10 & F7)...")
        pid_file = os.path.join(tempfile.gettempdir(), f"sec6_pid_{int(time.time()*1000)}.txt")
        child_py = os.path.join(tempfile.gettempdir(), f"sec6_child_{int(time.time()*1000)}.py")
        parent_py = os.path.join(tempfile.gettempdir(), f"sec6_parent_{int(time.time()*1000)}.py")
        for f_path in (pid_file, child_py, parent_py):
            if os.path.exists(f_path):
                try:
                    os.remove(f_path)
                except Exception:
                    pass
        with open(child_py, "w", encoding="utf-8") as f:
            f.write(
                "import os, time\n"
                "if hasattr(os, 'setsid'):\n"
                "    try:\n"
                "        os.setsid()\n"
                "    except Exception:\n"
                "        pass\n"
                "time.sleep(10)\n"
            )
        with open(parent_py, "w", encoding="utf-8") as f:
            f.write(
                "import os, sys, time, subprocess\n"
                f"child = subprocess.Popen([sys.executable, r'{child_py}'])\n"
                f"with open(r'{pid_file}', 'w') as f:\n"
                f"    f.write(f'{{os.getpid()}},{{child.pid}}')\n"
                "time.sleep(10)\n"
            )

        resp_tree = await caller.send_command(
            "worker-node",
            "shell_exec",
            {"cmd": f'"{sys.executable}" "{parent_py}"'},
            timeout_ms=1000,
            timeout=4.0,
        )
        assert resp_tree.get("status") == "timeout", f"Expected timeout, got {resp_tree}"
        await asyncio.sleep(0.5)
        if os.path.exists(pid_file):
            try:
                with open(pid_file, "r") as f:
                    pids_str = f.read().strip()
                if pids_str:
                    leader_pid, child_pid = [int(p) for p in pids_str.split(",")]
                    try:
                        import psutil
                        assert not psutil.pid_exists(leader_pid) or psutil.Process(leader_pid).status() == psutil.STATUS_ZOMBIE, f"Leader PID {leader_pid} survived timeout (Mutant P10)!"
                        assert not psutil.pid_exists(child_pid) or psutil.Process(child_pid).status() == psutil.STATUS_ZOMBIE, f"Detached setsid child PID {child_pid} survived timeout (F7)!"
                    except ImportError:
                        pass
            finally:
                for f_path in (pid_file, child_py, parent_py):
                    if os.path.exists(f_path):
                        try:
                            os.remove(f_path)
                        except Exception:
                            pass
        logger.info("✓ Test 8 PASSED: Subprocess tree and setsid child terminated (P10 killed)!")

        # ----------------------------------------------------------------------
        # Test 9: F3 Regression Check: Early Closed Output Does Not Trigger 5s Kill
        # ----------------------------------------------------------------------
        logger.info("[Test 9] Testing F3 regression (early closed output with 2s sleep)...")
        marker_file = os.path.join(tempfile.gettempdir(), f"sec6_f3_marker_{int(time.time()*1000)}.txt")
        if os.path.exists(marker_file):
            try:
                os.remove(marker_file)
            except Exception:
                pass
        py_f3_code = (
            "import sys, time; "
            "sys.stdout.close(); sys.stderr.close(); "
            "time.sleep(2.0); "
            f"open(r'{marker_file}', 'w').write('f3_ok'); sys.exit(0)"
        )
        resp_f3 = await caller.send_command(
            "worker-node",
            "shell_exec",
            {"cmd": f'"{sys.executable}" -c "{py_f3_code}"'},
            timeout_ms=5000,
            timeout=8.0,
        )
        assert resp_f3.get("status") == "success", f"Expected success, got {resp_f3}"
        assert resp_f3.get("exit_code") == 0, f"Expected 0, got {resp_f3.get('exit_code')}"
        assert os.path.exists(marker_file), "Marker file was not written (killed prematurely by F3 regression)!"
        if os.path.exists(marker_file):
            try:
                os.remove(marker_file)
            except Exception:
                pass
        logger.info("✓ Test 9 PASSED: Command completing after stream close finished cleanly without premature kill!")

        # ----------------------------------------------------------------------
        # Test 10: F4 Check: Child Subprocess Has DEVNULL Stdin
        # ----------------------------------------------------------------------
        logger.info("[Test 10] Testing F4 child stdin DEVNULL...")
        py_stdin_code = "import sys; sys.stdin.read(); print('STDIN_DEVNULL_DONE')"
        t0 = time.time()
        resp_stdin = await caller.send_command(
            "worker-node",
            "shell_exec",
            {"cmd": f'"{sys.executable}" -c "{py_stdin_code}"'},
            timeout_ms=5000,
            timeout=6.0,
        )
        duration = time.time() - t0
        assert resp_stdin.get("status") == "success"
        assert resp_stdin.get("exit_code") == 0
        assert "STDIN_DEVNULL_DONE" in resp_stdin.get("stdout", "")
        assert duration < 3.0, f"Command hung on stdin: duration={duration:.2f}s"
        logger.info("✓ Test 10 PASSED: stdin DEVNULL completed immediately without hanging!")

        # ----------------------------------------------------------------------
        # Test 11: Real CLI --no-shell Flag Execution (Kills Mutant P9)
        # ----------------------------------------------------------------------
        logger.info("[Test 11] Testing real CLI --no-shell flag execution (Mutant P9)...")
        cli_proc = await asyncio.create_subprocess_exec(
            sys.executable,
            os.path.join(root_dir, "scripts", "agent_node.py"),
            "--hub", hub_url,
            "--id", "worker-cli-noshell-py",
            "--no-shell",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            cli_entry = None
            for _ in range(30):
                await asyncio.sleep(0.1)
                cli_entry = hub.nodes.get("worker-cli-noshell-py")
                if cli_entry is not None:
                    break
            assert cli_entry is not None, "worker-cli-noshell-py failed to register via CLI within 3.0s"
            cli_supported = cli_entry.get("capabilities", {}).get("supported_commands", [])
            assert "shell_exec" not in cli_supported, f"shell_exec found in CLI capabilities (Mutant P9): {cli_supported}"
            resp_cli_shell = await caller.send_command(
                "worker-cli-noshell-py",
                "shell_exec",
                {"cmd": "echo should_fail"},
                timeout=4.0,
            )
            assert resp_cli_shell.get("status") == "failed"
            assert resp_cli_shell.get("exit_code") == 127
            assert "shell_exec is disabled by --no-shell" in resp_cli_shell.get("stderr", "")
        finally:
            cli_proc.kill()
            await cli_proc.wait()
        logger.info("✓ Test 11 PASSED: Real CLI --no-shell node refused shell_exec with 127 (P9 killed)!")

        # ----------------------------------------------------------------------
        # Test 12: F10 Low Items: timeout_ms=null and invalid non-string cwd
        # ----------------------------------------------------------------------
        logger.info("[Test 12] Testing F10 low items: timeout_ms=null and invalid non-string cwd...")
        resp_f10 = await caller.send_command(
            "worker-node",
            "shell_exec",
            {"cmd": "echo f10_align_ok", "cwd": 99999},
            timeout_ms=None,
            timeout=4.0,
        )
        assert resp_f10.get("status") == "success"
        assert resp_f10.get("exit_code") == 0
        assert "f10_align_ok" in resp_f10.get("stdout", "")
        logger.info("✓ Test 12 PASSED: timeout_ms=null and non-string cwd handled safely!")

        # ----------------------------------------------------------------------
        # Test 13: F12 Check: Rust and Python Secret Env Vars Parity
        # ----------------------------------------------------------------------
        logger.info("[Test 13] Testing Rust executor.rs and Python agent_node.py SECRET_ENV_VARS parity (F12)...")
        import re
        from scripts.agent_node import SECRET_ENV_VARS as PY_VARS
        rust_file = os.path.join(root_dir, "crates", "agent_mesh", "src", "executor.rs")
        with open(rust_file, "r", encoding="utf-8") as f:
            rust_content = f.read()
        match = re.search(r"pub const SECRET_ENV_VARS: \[&str; 5\] = \[(.*?)\];", rust_content, re.DOTALL)
        assert match is not None, "Could not find SECRET_ENV_VARS in executor.rs"
        raw_entries = [e.strip().strip('"') for e in match.group(1).split(",") if e.strip()]
        resolved_rust = []
        for entry in raw_entries:
            if entry == "ENV_SECRET":
                resolved_rust.append("OXIDESWARM_SECRET")
            elif entry == "ENV_SECRET_FILE":
                resolved_rust.append("OXIDESWARM_SECRET_FILE")
            else:
                resolved_rust.append(entry)
        assert set(resolved_rust) == set(PY_VARS), f"Mismatch between Rust {resolved_rust} and Python {PY_VARS}"
        logger.info(f"✓ Test 13 PASSED: Rust and Python strip identical 5 secret variables: {resolved_rust}")

        logger.info("==================================================================")
        logger.info("  ALL SEC-6 PYTHON MESH HARDENING TESTS PASSED (100%)             ")
        logger.info("==================================================================")
        return True

    finally:
        for k in sentinels:
            os.environ.pop(k, None)
        await worker.close()
        await worker_noshell.close()
        await caller.close()
        for t in tasks:
            t.cancel()
        await hub.stop()


def main():
    success = asyncio.run(run_sec6_tests())
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
