#!/usr/bin/env python3
"""
Integration test suite for SEC-6: Mesh Sandbox Hardening (Python node).

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
            'python -c "import os; '
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
                'python -c "import os; '
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
        cap_probe = 'python -c "import sys; sys.stdout.write(\'Z\' * 2500000); sys.stdout.flush()"'
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

        logger.info("==================================================================")
        logger.info("  ALL SEC-6 PYTHON MESH SANDBOX HARDENING TESTS PASSED (100%)    ")
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
