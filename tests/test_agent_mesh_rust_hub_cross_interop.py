#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Mesh - Rust Hub & Python Node Cross-Interop Runner
File: tests/test_agent_mesh_rust_hub_cross_interop.py

Tests the compiled Rust binary (`agent-mesh` Hub) with Python agent nodes across 5 suites:
1. Suite 1 (Cross-Interop):
   - Spawns Rust Hub subprocess (`agent-mesh hub --listen 127.0.0.1:0`).
   - Verifies REST status & nodes endpoints.
   - Connects 3 Python agent nodes (Win, Mac, Android).
   - Routes command Node 1 -> Node 3 and asserts response.
   - Verifies strict Node 2 isolation (Node 2 receives 0 commands).
   - Verifies DeliveryNack for nonexistent target.
   - Performs 50-packet bidirectional data burst.
2. Suite 2 (`test_agent_mesh_token_auth.py`):
   - Token authentication, duplicate node ID rejection, forged `from` NACK, foreign Origin 403,
     and `--token` / `OXIDE_MESH_TOKEN` CLI flags.
3. Suite 3 (`test_agent_mesh_adversarial_stress.py`):
   - High-concurrency stress against Rust Hub with and without `--grid-master`.
4. Suite 4 (`test_challenger_m5_rapid_disconnect.py`):
   - 25 concurrent Python clients rapidly connecting, sending commands, and abruptly dropping sockets.
5. Suite 5 (`test_challenger_m5_ecosystem_stress.py`):
   - `scripts/agent_node.py` CLI one-shot flags against unattached Rust Hub, intermediate
     `DeliveryAck("target_forwarded")` handling, timeout cleanup, socket drop, and corrupt JSON.
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
import time
import urllib.request

try:
    import websockets
except ImportError:
    print("websockets required")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("CrossInterop")

# Ensure tests and root directory are in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

from test_agent_mesh_simulation import SimulatedAgentNode
from test_agent_mesh_token_auth import run_token_auth_tests
from test_agent_mesh_adversarial_stress import run_adversarial_stress_tests
from test_challenger_m5_rapid_disconnect import run_rapid_disconnect_stress
from test_challenger_m5_ecosystem_stress import run_m5_ecosystem_stress_tests


def _find_binary() -> str:
    target_dir = os.environ.get("CARGO_TARGET_DIR") or os.path.join(parent_dir, "target")
    for name in ("agent-mesh.exe", "agent-mesh"):
        bin_path = os.path.join(target_dir, "debug", name)
        if os.path.exists(bin_path):
            return bin_path
    raise FileNotFoundError(f"agent-mesh binary not found in {target_dir}/debug")


def _drain_stream(stream):
    try:
        for _ in stream:
            pass
    except Exception:
        pass


async def run_cross_interop_tests():
    logger.info("==================================================================")
    logger.info("  STARTING RUST HUB + PYTHON AGENT CROSS-INTEROP ADVERSARIAL TEST ")
    logger.info("==================================================================")

    bin_path = _find_binary()

    logger.info("Launching Rust Hub binary on an ephemeral port...")
    proc = subprocess.Popen(
        [bin_path, "hub", "--listen", "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    try:
        hub_ws_url = None
        deadline = time.time() + 15.0
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            if "running at " in line:
                hub_ws_url = line.split("running at ", 1)[1].strip()
                break
        assert hub_ws_url is not None, "Rust Hub failed to report its bound address"
        threading.Thread(target=_drain_stream, args=(proc.stdout,), daemon=True).start()
        hub_http_url = "http://" + hub_ws_url.split("://", 1)[1].rsplit("/ws", 1)[0]

        # Wait for hub to become healthy
        online = False
        for _ in range(30):
            try:
                with urllib.request.urlopen(f"{hub_http_url}/api/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        online = True
                        break
            except Exception:
                await asyncio.sleep(0.1)

        assert online, "Rust Hub failed to respond to /api/health within 3s"
        logger.info("✓ Rust Hub online and responding to HTTP health checks")

        # Connect 3 Python nodes
        node1 = SimulatedAgentNode("cross-win-1", "windows")
        node2 = SimulatedAgentNode("cross-mac-2-isolated", "macos")
        node3 = SimulatedAgentNode("cross-android-3", "android")

        await node1.connect(hub_ws_url)
        await node2.connect(hub_ws_url)
        await node3.connect(hub_ws_url)
        await asyncio.sleep(0.15)

        # Check /api/nodes on Rust Hub
        with urllib.request.urlopen(f"{hub_http_url}/api/nodes", timeout=2.0) as resp:
            catalog = json.loads(resp.read().decode("utf-8"))
            active_ids = {n["node_id"] for n in catalog}
            assert "cross-win-1" in active_ids
            assert "cross-mac-2-isolated" in active_ids
            assert "cross-android-3" in active_ids
            logger.info(f"✓ Rust Hub active catalog contains all 3 Python nodes: {active_ids}")

        # Targeted Command Dispatch: Node 1 -> Node 3
        logger.info("Dispatching command: Python Node 1 -> Rust Hub -> Python Node 3...")
        cmd_res = await node1.send_command(
            target_id="cross-android-3",
            command="echo",
            args={"message": "Cross-Platform Interop Verified"},
        )
        assert cmd_res["type"] == "CommandResponse"
        assert cmd_res["from"] == "cross-android-3"
        assert cmd_res["to"] == "cross-win-1"
        assert cmd_res["status"] == "success"
        assert cmd_res["exit_code"] == 0
        assert cmd_res["stdout"] == "Cross-Platform Interop Verified"
        logger.info("✓ Cross-interop command routing successful!")

        # Strict Node 2 Isolation Check
        assert node3.executed_commands_count == 1
        assert node2.executed_commands_count == 0
        assert node1.executed_commands_count == 0
        logger.info("✓ Node 2 isolation strictly preserved through Rust Hub (0 executions)")

        # Negative Routing Check: Nonexistent Target
        logger.info("Testing DeliveryNack for unknown destination...")
        nack_res = await node1.send_command(
            target_id="phantom-node-404",
            command="ping",
            args={},
        )
        assert nack_res["type"] == "DeliveryNack"
        assert nack_res["error_code"] == "ERR_NODE_NOT_FOUND"
        logger.info(f"✓ Rust Hub returned DeliveryNack: {nack_res['reason']}")

        # Bidirectional 50-packet data burst
        logger.info("Executing 50-packet data burst across Rust Hub...")
        burst_tasks = []
        for i in range(25):
            fwd_data = f"FWD-INTEROP-{i}-{os.urandom(64).hex()}".encode("utf-8")
            rev_data = f"REV-INTEROP-{i}-{os.urandom(64).hex()}".encode("utf-8")
            burst_tasks.append(node1.send_data_payload("cross-android-3", fwd_data))
            burst_tasks.append(node3.send_data_payload("cross-win-1", rev_data))

        await asyncio.gather(*burst_tasks)
        await asyncio.sleep(0.3)

        assert len(node3.received_data_payloads) == 25
        assert len(node1.received_data_payloads) == 25
        assert len(node2.received_data_payloads) == 0, "Node 2 intercepted packets during burst!"
        logger.info("✓ 50-packet burst through Rust Hub succeeded with 0 data loss and strict isolation!")

        # Close clients
        await node1.close()
        await node2.close()
        await node3.close()

        logger.info("==================================================================")
        logger.info("  RUST HUB + PYTHON AGENT CROSS-INTEROP TEST PASSED (100%)        ")
        logger.info("==================================================================")
        return True

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()


async def run_all_rust_hub_python_suites() -> bool:
    assert await run_cross_interop_tests()
    assert await run_token_auth_tests()
    assert await run_adversarial_stress_tests()
    assert await run_rapid_disconnect_stress()
    assert await run_m5_ecosystem_stress_tests()
    logger.info("==================================================================")
    logger.info("  ALL 5 RUST HUB + PYTHON AGENT INTEROP SUITES PASSED (100%)      ")
    logger.info("==================================================================")
    return True


def main():
    try:
        success = asyncio.run(run_all_rust_hub_python_suites())
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Cross-interop test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
