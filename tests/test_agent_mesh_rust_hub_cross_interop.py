#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Mesh - Rust Hub & Python Node Cross-Interop Test
File: tests/test_agent_mesh_rust_hub_cross_interop.py

Author: Challenger 1 (teamwork_preview_challenger)

Tests the actual compiled Rust binary (`agent-mesh` Hub) with Python agent nodes:
1. Spawns Rust Hub subprocess (`target/debug/agent-mesh.exe hub --listen 127.0.0.1:23890`).
2. Verifies REST status & nodes endpoints.
3. Connects 3 Python agent nodes (Win, Mac, Android).
4. Routes command Node 1 -> Node 3 and asserts response.
5. Verifies strict Node 2 isolation (Node 2 receives 0 commands).
6. Verifies DeliveryNack for nonexistent target.
7. Performs 50-packet bidirectional data burst.
"""

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import sys
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

# Import SimulatedAgentNode
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

from test_agent_mesh_simulation import SimulatedAgentNode


async def run_cross_interop_tests():
    logger.info("==================================================================")
    logger.info("  STARTING RUST HUB + PYTHON AGENT CROSS-INTEROP ADVERSARIAL TEST ")
    logger.info("==================================================================")

    # Find agent-mesh binary
    bin_path = os.path.join(parent_dir, "target", "debug", "agent-mesh.exe")
    if not os.path.exists(bin_path):
        bin_path = os.path.join(parent_dir, "target", "debug", "agent-mesh")
    if not os.path.exists(bin_path):
        raise FileNotFoundError(f"agent-mesh binary not found at {bin_path}")

    # Use a high port for testing
    test_port = 23890
    hub_ws_url = f"ws://127.0.0.1:{test_port}/ws"
    hub_http_url = f"http://127.0.0.1:{test_port}"

    logger.info(f"Launching Rust Hub binary on port {test_port}...")
    proc = subprocess.Popen(
        [bin_path, "hub", "--listen", f"127.0.0.1:{test_port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
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


def main():
    try:
        success = asyncio.run(run_cross_interop_tests())
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Cross-interop test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
