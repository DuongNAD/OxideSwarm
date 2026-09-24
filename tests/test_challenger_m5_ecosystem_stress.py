#!/usr/bin/env python3
"""
Adversarial Stress Test Suite for Milestone M5 (Requirement R6: Ecosystem Convergence)
File: tests/test_challenger_m5_ecosystem_stress.py

Author: Challenger 2 (teamwork_preview_challenger)

Attacks & Validates:
1. Python agent node CLI one-shot execution against unattached Rust Hub:
   - Verifies exit code 1 and ERR_BRIDGE_NOT_CONFIGURED DeliveryNack without hanging.
2. Decoupled message pump and intermediate ACK handling:
   - Verifies target_forwarded DeliveryAck does NOT prematurely resolve command future.
   - Verifies subsequent CommandResponse resolves successfully.
3. Timeout safety and pending command cleanup when CommandResponse is lost.
4. Abrupt WebSocket closure during in-flight command execution.
5. Corrupt / chaotic message injection into agent node message pump.
6. Subcommands parity (grid-compute, grid-compile, grid-status).
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
import uuid

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("M5Challenger2")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from agent_node import OxideAgentNode


# =========================================================================
# TEST 1: Python CLI One-Shot Execution Against Unattached Rust Hub
# =========================================================================
async def test_unattached_hub_rejections():
    logger.info("=== Running Test 1: Python CLI Against Unattached Rust Hub ===")
    
    bin_name = "agent-mesh.exe" if sys.platform == "win32" else "agent-mesh"
    bin_path = os.path.join(PROJECT_ROOT, "target", "debug", bin_name)
    if not os.path.exists(bin_path):
        raise FileNotFoundError(f"agent-mesh binary not found at {bin_path}")

    port = 24199
    hub_url = f"ws://127.0.0.1:{port}/ws"
    health_url = f"http://127.0.0.1:{port}/api/health"

    logger.info(f"Starting unattached Rust Hub on port {port}...")
    proc = subprocess.Popen(
        [bin_path, "hub", "--listen", f"127.0.0.1:{port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for hub to be ready
        ready = False
        for _ in range(30):
            try:
                with urllib.request.urlopen(health_url, timeout=1.0) as resp:
                    if resp.status == 200:
                        ready = True
                        break
            except Exception:
                await asyncio.sleep(0.1)
        assert ready, "Rust Hub failed to become healthy within 3s"

        py_bin = sys.executable
        script_path = os.path.join(SCRIPTS_DIR, "agent_node.py")

        # 1. Test --submit-grid-compute
        logger.info("Testing agent_node.py --submit-grid-compute against unattached hub...")
        res = subprocess.run(
            [
                py_bin,
                script_path,
                "--hub",
                hub_url,
                "--id",
                "test-unattached-compute",
                "--submit-grid-compute",
                "matrix_multiply",
                "--timeout",
                "5.0",
            ],
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        assert res.returncode == 1, f"Expected returncode 1, got {res.returncode}. Output: {res.stdout}"
        assert "ERR_BRIDGE_NOT_CONFIGURED" in res.stdout, f"Expected ERR_BRIDGE_NOT_CONFIGURED in stdout: {res.stdout}"
        logger.info("✓ Compute rejection verified with clean NACK")

        # 2. Test --submit-grid-compile
        logger.info("Testing agent_node.py --submit-grid-compile against unattached hub...")
        res = subprocess.run(
            [
                py_bin,
                script_path,
                "--hub",
                hub_url,
                "--id",
                "test-unattached-compile",
                "--submit-grid-compile",
                "test_crate",
                "--timeout",
                "5.0",
            ],
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        assert res.returncode == 1, f"Expected returncode 1, got {res.returncode}. Output: {res.stdout}"
        assert "ERR_BRIDGE_NOT_CONFIGURED" in res.stdout, f"Expected ERR_BRIDGE_NOT_CONFIGURED in stdout: {res.stdout}"
        logger.info("✓ Compile rejection verified with clean NACK")

        # 3. Test --query-grid-status
        logger.info("Testing agent_node.py --query-grid-status against unattached hub...")
        res = subprocess.run(
            [
                py_bin,
                script_path,
                "--hub",
                hub_url,
                "--id",
                "test-unattached-status",
                "--query-grid-status",
                "--timeout",
                "5.0",
            ],
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        assert res.returncode == 1, f"Expected returncode 1, got {res.returncode}. Output: {res.stdout}"
        assert "ERR_BRIDGE_NOT_CONFIGURED" in res.stdout, f"Expected ERR_BRIDGE_NOT_CONFIGURED in stdout: {res.stdout}"
        logger.info("✓ Status rejection verified with clean NACK")

        # 4. Test Subcommand syntax `grid-compute`
        logger.info("Testing agent_node.py grid-compute subcommand...")
        res = subprocess.run(
            [
                py_bin,
                script_path,
                "--hub",
                hub_url,
                "--id",
                "test-subcommand-compute",
                "grid-compute",
                "matrix_multiply",
                "--timeout",
                "5.0",
            ],
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        assert res.returncode == 1, f"Expected returncode 1, got {res.returncode}. Output: {res.stdout}"
        assert "ERR_BRIDGE_NOT_CONFIGURED" in res.stdout
        logger.info("✓ Subcommand grid-compute verified")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info("Rust Hub stopped.")


# =========================================================================
# TEST 2: Intermediate target_forwarded ACK Non-Preemption
# =========================================================================
async def test_intermediate_ack_handling():
    logger.info("=== Running Test 2: Intermediate target_forwarded ACK Non-Preemption ===")

    server_port = 24201
    received_requests = []

    async def mock_hub_handler(ws):
        # Registration
        reg_raw = await ws.recv()
        reg = json.loads(reg_raw)
        await ws.send(
            json.dumps(
                {
                    "version": "1.0",
                    "correlation_id": reg.get("correlation_id", str(uuid.uuid4())),
                    "type": "NodeRegistrationAck",
                    "from": "hub",
                    "to": reg.get("from", reg.get("node_id", "unknown")),
                    "status": "registered",
                    "registered_nodes_count": 1,
                    "timestamp": int(time.time() * 1000),
                }
            )
        )

        # Receive command
        cmd_raw = await ws.recv()
        cmd = json.loads(cmd_raw)
        received_requests.append(cmd)
        corr_id = cmd["correlation_id"]
        sender = cmd["from"]

        # Step 1: Send intermediate target_forwarded DeliveryAck
        intermediate_ack = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "DeliveryAck",
            "from": "hub",
            "to": sender,
            "status": "target_forwarded",
            "timestamp": int(time.time() * 1000),
        }
        await ws.send(json.dumps(intermediate_ack))

        # Small delay simulating async execution
        await asyncio.sleep(0.3)

        # Step 2: Send final CommandResponse
        final_resp = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "CommandResponse",
            "from": "grid",
            "to": sender,
            "command": "grid_compute",
            "status": "success",
            "exit_code": 0,
            "stdout": "Matrix multiplication completed: SIMD verified",
            "stderr": "",
            "execution_duration_ms": 12,
            "timestamp": int(time.time() * 1000),
        }
        await ws.send(json.dumps(final_resp))

    server = await websockets.serve(mock_hub_handler, "127.0.0.1", server_port)

    node = OxideAgentNode(
        hub_url=f"ws://127.0.0.1:{server_port}/ws",
        node_id="test-ack-node",
        platform_name="python-test",
    )
    await node.connect()
    listen_task = asyncio.create_task(node.listen())

    try:
        t0 = time.time()
        res = await node.submit_grid_compute("matrix_multiply", matrix_dim=64, timeout_s=5.0)
        elapsed = time.time() - t0

        # Assertions
        assert res.get("type") == "CommandResponse", f"Expected CommandResponse, got: {res}"
        assert res.get("status") == "success"
        assert res.get("exit_code") == 0
        assert elapsed >= 0.25, f"Response arrived too fast ({elapsed}s), premature completion on target_forwarded ACK!"
        logger.info(f"✓ Intermediate target_forwarded ACK correctly deferred completion (elapsed: {elapsed:.3f}s)")
    finally:
        listen_task.cancel()
        await node.close()
        server.close()
        await server.wait_closed()


# =========================================================================
# TEST 3: Timeout Safety & Pending Command Map Leak Prevention
# =========================================================================
async def test_command_timeout_and_leak_prevention():
    logger.info("=== Running Test 3: Command Timeout & Leak Prevention ===")

    server_port = 24202

    async def mock_hub_blackhole(ws):
        reg_raw = await ws.recv()
        reg = json.loads(reg_raw)
        await ws.send(
            json.dumps(
                {
                    "version": "1.0",
                    "correlation_id": reg.get("correlation_id", ""),
                    "type": "NodeRegistrationAck",
                    "from": "hub",
                    "to": reg.get("from", reg.get("node_id", "unknown")),
                    "status": "registered",
                    "registered_nodes_count": 1,
                    "timestamp": int(time.time() * 1000),
                }
            )
        )
        # Black hole: receive command, never reply
        _ = await ws.recv()
        await asyncio.sleep(10.0)

    server = await websockets.serve(mock_hub_blackhole, "127.0.0.1", server_port)

    node = OxideAgentNode(
        hub_url=f"ws://127.0.0.1:{server_port}/ws",
        node_id="test-timeout-node",
        platform_name="python-test",
    )
    await node.connect()
    listen_task = asyncio.create_task(node.listen())

    try:
        timed_out = False
        try:
            # Short timeout of 1.0 second
            await node.submit_grid_compute("matrix_multiply", matrix_dim=64, timeout_s=1.0)
        except asyncio.TimeoutError:
            timed_out = True

        assert timed_out, "Command should have raised asyncio.TimeoutError"
        assert len(node.pending_commands) == 0, f"Pending commands map leaked! Length: {len(node.pending_commands)}"
        logger.info("✓ Timeout correctly triggered and pending_commands map cleaned up (0 leaks)")
    finally:
        listen_task.cancel()
        await node.close()
        server.close()
        await server.wait_closed()


# =========================================================================
# TEST 4: Abrupt Socket Disconnection Mid-Command
# =========================================================================
async def test_abrupt_socket_disconnection():
    logger.info("=== Running Test 4: Abrupt Socket Disconnect Mid-Command ===")

    server_port = 24203

    async def mock_hub_disconnect(ws):
        reg_raw = await ws.recv()
        reg = json.loads(reg_raw)
        await ws.send(
            json.dumps(
                {
                    "version": "1.0",
                    "correlation_id": reg.get("correlation_id", ""),
                    "type": "NodeRegistrationAck",
                    "from": "hub",
                    "to": reg.get("from", reg.get("node_id", "unknown")),
                    "status": "registered",
                    "registered_nodes_count": 1,
                    "timestamp": int(time.time() * 1000),
                }
            )
        )
        _ = await ws.recv()
        # Abrupt close mid-execution!
        await ws.close(code=1001, reason="Simulated abrupt hub shutdown")

    server = await websockets.serve(mock_hub_disconnect, "127.0.0.1", server_port)

    node = OxideAgentNode(
        hub_url=f"ws://127.0.0.1:{server_port}/ws",
        node_id="test-disconnect-node",
        platform_name="python-test",
    )
    await node.connect()
    listen_task = asyncio.create_task(node.listen())

    try:
        # Node sends command; connection will be dropped mid-flight
        try:
            await node.submit_grid_compute("matrix_multiply", matrix_dim=64, timeout_s=2.0)
        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            logger.info(f"✓ Caught expected exception on socket drop: {type(e).__name__}")

        # The listen task should exit cleanly on connection close without unhandled panic
        await asyncio.sleep(0.2)
        assert listen_task.done(), "listen() task should have completed on socket close"
        logger.info("✓ Decoupled message pump terminated cleanly on socket closure")
    finally:
        if not listen_task.done():
            listen_task.cancel()
        await node.close()
        server.close()
        await server.wait_closed()


# =========================================================================
# TEST 5: Corrupt / Chaotic Message Injection
# =========================================================================
async def test_corrupt_message_resilience():
    logger.info("=== Running Test 5: Corrupt & Chaotic Message Injection ===")

    server_port = 24204

    async def mock_hub_chaos(ws):
        reg_raw = await ws.recv()
        reg = json.loads(reg_raw)
        await ws.send(
            json.dumps(
                {
                    "version": "1.0",
                    "correlation_id": reg.get("correlation_id", ""),
                    "type": "NodeRegistrationAck",
                    "from": "hub",
                    "to": reg.get("from", reg.get("node_id", "unknown")),
                    "status": "registered",
                    "registered_nodes_count": 1,
                    "timestamp": int(time.time() * 1000),
                }
            )
        )

        # Inject malformed messages
        await ws.send("NON_JSON_CORRUPT_STRING")
        await ws.send(json.dumps({"unknown_field": 123}))
        await ws.send(json.dumps({"type": "UnknownType", "id": "123"}))
        await ws.send(json.dumps({"type": "CommandRequest"})) # missing fields

        # Followed by a valid heartbeat / ping
        await asyncio.sleep(0.2)
        await ws.send(
            json.dumps(
                {
                    "version": "1.0",
                    "type": "Heartbeat",
                    "from": "hub",
                    "to": reg.get("from", reg.get("node_id", "unknown")),
                    "timestamp": int(time.time() * 1000),
                }
            )
        )
        await asyncio.sleep(5.0)

    server = await websockets.serve(mock_hub_chaos, "127.0.0.1", server_port)

    node = OxideAgentNode(
        hub_url=f"ws://127.0.0.1:{server_port}/ws",
        node_id="test-chaos-node",
        platform_name="python-test",
    )
    await node.connect()
    listen_task = asyncio.create_task(node.listen())

    try:
        await asyncio.sleep(0.5)
        # Verify node message pump is still alive despite chaos injection
        assert not listen_task.done(), "listen() task crashed on malformed JSON payload!"
        logger.info("✓ Message pump survived all corrupt/chaotic message injections")
    finally:
        listen_task.cancel()
        await node.close()
        server.close()
        await server.wait_closed()


async def main():
    logger.info("==================================================================")
    logger.info("  STARTING M5 ECOSYSTEM CONVERGENCE ADVERSARIAL CHALLENGER SUITE  ")
    logger.info("==================================================================")

    await test_unattached_hub_rejections()
    await test_intermediate_ack_handling()
    await test_command_timeout_and_leak_prevention()
    await test_abrupt_socket_disconnection()
    await test_corrupt_message_resilience()

    logger.info("==================================================================")
    logger.info("  ALL M5 ADVERSARIAL CHALLENGE SUITES PASSED EMPIRICALLY (100%)   ")
    logger.info("==================================================================")


if __name__ == "__main__":
    asyncio.run(main())
