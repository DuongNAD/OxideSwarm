#!/usr/bin/env python3
"""
OxideSwarm Challenger 2 - Subprocess Safety & Exit Code Adversarial Test
File: tests/test_challenger_subprocess_safety.py

Empirically challenges:
1. Non-zero exit codes & stderr capture:
   - Script explicit exit code (42) and stderr output
   - Invalid command execution failure
   - Unknown command rejection (code 127)
2. Subprocess timeout safety:
   - Timeout detection (status: 'timeout', exit_code: 124)
   - Process termination: whether hung child process is terminated or orphaned
"""

import asyncio
import json
import logging
import os
import psutil
import sys
import tempfile
import time
import uuid

# Ensure root directory in sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from scripts.agent_node import OxideAgentNode
try:
    from test_agent_mesh_simulation import OxideRelayHub
except ImportError:
    from tests.test_agent_mesh_simulation import OxideRelayHub

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SubprocessChallenge")

results = {
    "non_zero_exit_explicit": False,
    "stderr_captured": False,
    "invalid_cmd_failed": False,
    "unknown_cmd_rejected": False,
    "timeout_detected": False,
    "timeout_exit_code_124": False,
    "process_killed_on_timeout": False,
    "agent_node_delivery_ack_bug_detected": False,
    "details": {},
}


class RobustCallerNode(OxideAgentNode):
    """
    Subclass that correctly filters out intermediate DeliveryAck('target_forwarded')
    to prevent premature resolution of command execution futures, matching
    the reference behavior in crates/agent_mesh/src/client.rs.
    """
    async def _dispatch_incoming(self, msg: dict):
        msg_type = msg.get("type") or msg.get("msg_type")
        corr_id = msg.get("correlation_id") or msg.get("id")

        if msg_type == "DeliveryAck" and msg.get("status") == "target_forwarded":
            # Intermediate hub ack: do NOT resolve command response future yet!
            logger.info(f"Hub forwarded ACK received for {corr_id} (waiting for actual CommandResponse)")
            return

        await super()._dispatch_incoming(msg)


async def run_challenges():
    logger.info("==================================================================")
    logger.info("  STARTING SUBPROCESS EXECUTION & TIMEOUT SAFETY CHALLENGE        ")
    logger.info("==================================================================")

    hub = OxideRelayHub()
    port = await hub.start()
    hub_url = f"ws://127.0.0.1:{port}"

    worker = OxideAgentNode(hub_url, "worker-exec-node", "windows")
    caller = RobustCallerNode(hub_url, "caller-exec-node", "windows")

    worker_task = asyncio.create_task(worker.run())
    caller_task = asyncio.create_task(caller.run())
    await asyncio.sleep(0.3)

    try:
        # ----------------------------------------------------------------------
        # Challenge 0: Test Standard OxideAgentNode DeliveryAck premature resolution
        # ----------------------------------------------------------------------
        logger.info("[Challenge 0] Testing default OxideAgentNode for DeliveryAck race bug...")
        raw_caller = OxideAgentNode(hub_url, "raw-caller-node", "windows")
        raw_caller_task = asyncio.create_task(raw_caller.run())
        await asyncio.sleep(0.2)

        raw_resp = await raw_caller.send_command("worker-exec-node", "echo", {"msg": "test_ack_race"}, timeout=2.0)
        logger.info(f"Default OxideAgentNode.send_command received: type={raw_resp.get('type')}, status={raw_resp.get('status')}")
        if raw_resp.get("type") == "DeliveryAck" and raw_resp.get("status") == "target_forwarded":
            logger.warning("CONFIRMED BUG: Default OxideAgentNode resolves send_command on DeliveryAck('target_forwarded') instead of waiting for CommandResponse!")
            results["agent_node_delivery_ack_bug_detected"] = True
            results["details"]["delivery_ack_bug"] = {
                "received_type": raw_resp.get("type"),
                "received_status": raw_resp.get("status"),
                "explanation": "scripts/agent_node.py resolves pending_futures on DeliveryAck without checking if status == 'target_forwarded', returning the Hub ACK before execution finishes."
            }
        raw_caller_task.cancel()
        await raw_caller.close()

        # ----------------------------------------------------------------------
        # Challenge 1: Non-Zero Exit Code (Exit 42) & Stderr Capture
        # ----------------------------------------------------------------------
        logger.info("[Challenge 1] Testing explicit non-zero exit code (exit 42) and stderr capture...")
        err_msg = "CRITICAL_FAILURE_CUSTOM_STDERR_STREAM_7719"
        py_fail_script = (
            f"import sys\n"
            f"sys.stdout.write('stdout_before_crash\\n')\n"
            f"sys.stdout.flush()\n"
            f"sys.stderr.write('{err_msg}\\n')\n"
            f"sys.stderr.flush()\n"
            f"sys.exit(42)\n"
        )
        fail_py_file = os.path.join(tempfile.gettempdir(), f"fail_script_{uuid.uuid4().hex[:8]}.py")
        with open(fail_py_file, "w") as f:
            f.write(py_fail_script)

        cmd_req = f'python "{fail_py_file}"'
        resp = await caller.send_command("worker-exec-node", "shell_exec", {"cmd": cmd_req}, timeout=5.0)

        logger.info(f"Response received: status={resp.get('status')}, exit_code={resp.get('exit_code')}")
        logger.info(f"Stdout: {repr(resp.get('stdout'))}")
        logger.info(f"Stderr: {repr(resp.get('stderr'))}")

        assert resp.get("status") == "failed", f"Expected status 'failed', got {resp.get('status')}"
        assert resp.get("exit_code") == 42, f"Expected exit_code 42, got {resp.get('exit_code')}"
        assert err_msg in resp.get("stderr", ""), f"Expected stderr to contain {err_msg}, got {resp.get('stderr')}"
        assert "stdout_before_crash" in resp.get("stdout", ""), "Expected stdout before crash"

        results["non_zero_exit_explicit"] = True
        results["stderr_captured"] = True
        results["details"]["explicit_exit"] = {
            "exit_code": resp.get("exit_code"),
            "stderr": resp.get("stderr").strip(),
            "stdout": resp.get("stdout").strip(),
            "status": resp.get("status"),
        }
        logger.info("✓ Challenge 1 PASSED: Exit code 42 and stderr successfully captured!")

        if os.path.exists(fail_py_file):
            os.remove(fail_py_file)

        # ----------------------------------------------------------------------
        # Challenge 2: Invalid Shell Command Execution
        # ----------------------------------------------------------------------
        logger.info("[Challenge 2] Testing invalid / non-existent shell command...")
        bogus_cmd = f"non_existent_binary_{uuid.uuid4().hex[:12]}_invalid"
        resp_invalid = await caller.send_command("worker-exec-node", "shell_exec", {"cmd": bogus_cmd}, timeout=5.0)

        logger.info(f"Invalid cmd response: status={resp_invalid.get('status')}, exit_code={resp_invalid.get('exit_code')}")
        logger.info(f"Invalid cmd stderr: {repr(resp_invalid.get('stderr'))}")

        assert resp_invalid.get("status") == "failed"
        assert resp_invalid.get("exit_code") != 0
        assert len(resp_invalid.get("stderr", "").strip()) > 0 or len(resp_invalid.get("stdout", "").strip()) > 0

        results["invalid_cmd_failed"] = True
        results["details"]["invalid_cmd"] = {
            "exit_code": resp_invalid.get("exit_code"),
            "stderr": resp_invalid.get("stderr").strip(),
            "status": resp_invalid.get("status"),
        }
        logger.info("✓ Challenge 2 PASSED: Invalid command failed with non-zero exit code!")

        # ----------------------------------------------------------------------
        # Challenge 3: Unknown Builtin Command Rejection
        # ----------------------------------------------------------------------
        logger.info("[Challenge 3] Testing unknown builtin command opcode rejection...")
        resp_unknown = await caller.send_command("worker-exec-node", "unsupported_command_xyz", {}, timeout=5.0)

        logger.info(f"Unknown command response: status={resp_unknown.get('status')}, exit_code={resp_unknown.get('exit_code')}")
        logger.info(f"Unknown command stderr: {repr(resp_unknown.get('stderr'))}")

        assert resp_unknown.get("status") == "failed"
        assert resp_unknown.get("exit_code") == 127
        assert "Unsupported command" in resp_unknown.get("stderr", "")

        results["unknown_cmd_rejected"] = True
        results["details"]["unknown_cmd"] = {
            "exit_code": resp_unknown.get("exit_code"),
            "stderr": resp_unknown.get("stderr").strip(),
        }
        logger.info("✓ Challenge 3 PASSED: Unknown command code rejected with 127!")

        # ----------------------------------------------------------------------
        # Challenge 4: Subprocess Timeout Safety & Process Killing
        # ----------------------------------------------------------------------
        logger.info("[Challenge 4] Testing subprocess timeout safety & process termination...")
        pid_file = os.path.join(tempfile.gettempdir(), f"timeout_pid_{uuid.uuid4().hex[:8]}.txt")
        if os.path.exists(pid_file):
            os.remove(pid_file)

        # Python script that records PID then sleeps 10s
        py_hang_script = (
            f"import os, sys, time\n"
            f"pid = os.getpid()\n"
            f"with open(r'{pid_file}', 'w') as f:\n"
            f"    f.write(str(pid))\n"
            f"time.sleep(10)\n"
        )
        hang_py_file = os.path.join(tempfile.gettempdir(), f"hang_script_{uuid.uuid4().hex[:8]}.py")
        with open(hang_py_file, "w") as f:
            f.write(py_hang_script)

        timeout_duration_ms = 1000  # 1.0 second timeout
        t0 = time.time()
        resp_timeout = await caller.send_command(
            "worker-exec-node",
            "shell_exec",
            {"cmd": f'python "{hang_py_file}"'},
            timeout=1.0,
        )
        elapsed_s = time.time() - t0

        logger.info(f"Timeout response in {elapsed_s:.3f}s: status={resp_timeout.get('status')}, exit_code={resp_timeout.get('exit_code')}")
        logger.info(f"Stderr: {repr(resp_timeout.get('stderr'))}")

        assert resp_timeout.get("status") == "timeout", f"Expected status 'timeout', got {resp_timeout.get('status')}"
        assert resp_timeout.get("exit_code") == 124, f"Expected exit_code 124, got {resp_timeout.get('exit_code')}"
        results["timeout_detected"] = True
        results["timeout_exit_code_124"] = True

        # Now check if the process PID was actually terminated or is still alive
        await asyncio.sleep(0.5)
        if os.path.exists(pid_file):
            with open(pid_file, "r") as f:
                child_pid = int(f.read().strip())
            logger.info(f"Inspecting spawned child PID {child_pid}...")
            is_running = psutil.pid_exists(child_pid)
            if is_running:
                try:
                    p = psutil.Process(child_pid)
                    if p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                        logger.warning(f"CRITICAL DEFECT: Subprocess PID {child_pid} ({p.name()}) is STILL RUNNING after timeout!")
                        results["process_killed_on_timeout"] = False
                        results["details"]["process_kill"] = {
                            "status": "ORPHANED_RUNNING",
                            "pid": child_pid,
                            "name": p.name(),
                            "process_status": p.status(),
                            "finding": "Subprocess is NOT killed upon timeout in scripts/agent_node.py. proc.kill() is absent in except asyncio.TimeoutError handler."
                        }
                        # Kill it to clean up the system
                        p.kill()
                    else:
                        results["process_killed_on_timeout"] = True
                except psutil.NoSuchProcess:
                    results["process_killed_on_timeout"] = True
            else:
                results["process_killed_on_timeout"] = True
                logger.info(f"✓ Child PID {child_pid} was properly killed.")
        else:
            logger.warning("PID file was not written in time before timeout.")

        if os.path.exists(hang_py_file):
            os.remove(hang_py_file)
        if os.path.exists(pid_file):
            os.remove(pid_file)

        logger.info("==================================================================")
        logger.info(f"SUBPROCESS CHALLENGE SUMMARY:")
        logger.info(f"  Non-zero exit (42): {results['non_zero_exit_explicit']}")
        logger.info(f"  Stderr capture: {results['stderr_captured']}")
        logger.info(f"  Invalid command handling: {results['invalid_cmd_failed']}")
        logger.info(f"  Unknown opcode 127: {results['unknown_cmd_rejected']}")
        logger.info(f"  Timeout detected (124): {results['timeout_detected']}")
        logger.info(f"  Process killed on timeout: {results['process_killed_on_timeout']}")
        logger.info(f"  DeliveryAck race bug detected: {results['agent_node_delivery_ack_bug_detected']}")
        logger.info("==================================================================")

        return results

    finally:
        worker_task.cancel()
        caller_task.cancel()
        await worker.close()
        await caller.close()
        await hub.stop()


def main():
    res = asyncio.run(run_challenges())
    print("\nFINAL_JSON_RESULT:" + json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
