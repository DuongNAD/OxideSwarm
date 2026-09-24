#!/usr/bin/env python3
"""
Challenger 2 (Iteration 2) - Adversarial Process Tree Termination Stress Test
File: tests/test_challenger_process_tree_termination.py

Adversarially tests whether CommandExecutor / agent_node terminates
not just the immediate child process, but grandchild / descendant processes
spawned by the command when it times out.
"""

import asyncio
import os
import psutil
import sys
import tempfile
import time
import uuid

# Ensure root in sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from scripts.agent_node import OxideAgentNode
try:
    from test_agent_mesh_simulation import OxideRelayHub
except ImportError:
    from tests.test_agent_mesh_simulation import OxideRelayHub


async def test_grandchild_process_termination():
    print("==================================================================")
    print("  ADVERSARIAL CHALLENGE: GRANDCHILD PROCESS TREE TERMINATION     ")
    print("==================================================================")

    hub = OxideRelayHub()
    port = await hub.start()
    hub_url = f"ws://127.0.0.1:{port}"

    worker = OxideAgentNode(hub_url, "tree-worker-node", "windows")
    caller = OxideAgentNode(hub_url, "tree-caller-node", "windows")

    worker_task = asyncio.create_task(worker.run())
    caller_task = asyncio.create_task(caller.run())
    await asyncio.sleep(0.3)

    pids_file = os.path.join(tempfile.gettempdir(), f"pids_tree_{uuid.uuid4().hex[:8]}.txt")
    grandchild_script = os.path.join(tempfile.gettempdir(), f"grandchild_{uuid.uuid4().hex[:8]}.py")
    parent_script = os.path.join(tempfile.gettempdir(), f"parent_{uuid.uuid4().hex[:8]}.py")

    # Grandchild script writes its PID and sleeps for 30s
    with open(grandchild_script, "w") as f:
        f.write(
            f"import os, sys, time\n"
            f"pid = os.getpid()\n"
            f"with open(r'{pids_file}', 'a') as f:\n"
            f"    f.write('GRANDCHILD:' + str(pid) + '\\n')\n"
            f"time.sleep(30)\n"
        )

    # Parent script spawns grandchild subprocess, writes parent PID, and sleeps 30s
    with open(parent_script, "w") as f:
        f.write(
            f"import os, sys, time, subprocess\n"
            f"pid = os.getpid()\n"
            f"with open(r'{pids_file}', 'a') as f:\n"
            f"    f.write('PARENT:' + str(pid) + '\\n')\n"
            f"gc = subprocess.Popen([sys.executable, r'{grandchild_script}'])\n"
            f"time.sleep(30)\n"
        )

    try:
        t0 = time.time()
        print(f"Spawning parent script with 1000ms timeout...")
        resp = await caller.send_command(
            "tree-worker-node",
            "shell_exec",
            {"cmd": f'python "{parent_script}"'},
            timeout=1.0,
        )
        elapsed = time.time() - t0
        print(f"Response received in {elapsed:.3f}s: status={resp.get('status')}, exit_code={resp.get('exit_code')}")

        assert resp.get("status") == "timeout", f"Expected timeout, got {resp.get('status')}"
        assert resp.get("exit_code") == 124, f"Expected 124, got {resp.get('exit_code')}"

        # Wait a short moment to inspect OS process table
        await asyncio.sleep(0.8)

        pids = {}
        if os.path.exists(pids_file):
            with open(pids_file, "r") as f:
                for line in f:
                    parts = line.strip().split(":")
                    if len(parts) == 2:
                        pids[parts[0]] = int(parts[1])

        print(f"Captured process tree PIDs: {pids}")
        assert "PARENT" in pids, "Parent PID was not recorded!"
        assert "GRANDCHILD" in pids, "Grandchild PID was not recorded!"

        parent_pid = pids["PARENT"]
        grandchild_pid = pids["GRANDCHILD"]

        parent_alive = psutil.pid_exists(parent_pid) and psutil.Process(parent_pid).status() != psutil.STATUS_ZOMBIE if psutil.pid_exists(parent_pid) else False
        grandchild_alive = psutil.pid_exists(grandchild_pid) and psutil.Process(grandchild_pid).status() != psutil.STATUS_ZOMBIE if psutil.pid_exists(grandchild_pid) else False

        print(f"Parent PID {parent_pid} alive: {parent_alive}")
        print(f"Grandchild PID {grandchild_pid} alive: {grandchild_alive}")

        # Clean up any remnants if test fails
        if parent_alive:
            try:
                psutil.Process(parent_pid).kill()
            except Exception:
                pass
        if grandchild_alive:
            try:
                psutil.Process(grandchild_pid).kill()
            except Exception:
                pass

        assert not parent_alive, f"Parent PID {parent_pid} survived timeout!"
        assert not grandchild_alive, f"Grandchild PID {grandchild_pid} survived timeout! Process tree was NOT fully killed."

        print("SUCCESS: Both parent and grandchild processes were completely terminated on timeout!")
        return True

    finally:
        worker_task.cancel()
        caller_task.cancel()
        await worker.close()
        await caller.close()
        await hub.stop()

        for fpath in [pids_file, grandchild_script, parent_script]:
            if os.path.exists(fpath):
                try:
                    os.remove(fpath)
                except Exception:
                    pass


if __name__ == "__main__":
    success = asyncio.run(test_grandchild_process_termination())
    print(f"GRANDCHILD_PROCESS_TREE_TEST_RESULT: {'PASSED' if success else 'FAILED'}")
    sys.exit(0 if success else 1)
