#!/usr/bin/env python3
"""
Adversarial Stress Test: Rapid Concurrent Client Disconnections & Hub Lifecycle Resiliency
File: tests/test_challenger_m5_rapid_disconnect.py

Author: Challenger 2 (teamwork_preview_challenger)

Tests:
1. Spawns compiled Rust Hub (`agent-mesh hub`).
2. Concurrently connects 25 Python nodes.
3. Nodes submit commands with varying latencies while abruptly terminating sockets mid-flight.
4. Asserts Rust Hub remains healthy, doesn't crash on broken write channels, and cleans up dead sessions.
"""

import asyncio
import json
import logging
import os
import random
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("RapidDisconnect")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _find_binary() -> str:
    target_dir = os.environ.get("CARGO_TARGET_DIR") or os.path.join(PROJECT_ROOT, "target")
    for name in ("agent-mesh.exe", "agent-mesh"):
        p = os.path.join(target_dir, "debug", name)
        if os.path.exists(p):
            return p
    raise FileNotFoundError("agent-mesh binary not found; build target/debug first")


def _drain_stream(stream):
    try:
        for _ in stream:
            pass
    except Exception:
        pass


async def run_rapid_disconnect_stress() -> bool:
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

        http_base = "http://" + hub_ws_url.split("://", 1)[1].rsplit("/ws", 1)[0]
        hub_health_url = f"{http_base}/api/health"
        hub_nodes_url = f"{http_base}/api/nodes"

        # Await health
        online = False
        for _ in range(30):
            try:
                with urllib.request.urlopen(hub_health_url, timeout=1.0) as resp:
                    if resp.status == 200:
                        online = True
                        break
            except Exception:
                await asyncio.sleep(0.1)
        assert online, "Rust Hub failed to become healthy within 3s"

        logger.info("Rust Hub online. Launching 25 concurrent client connection/drop bursts...")

        async def turbulent_client(idx: int):
            node_id = f"turbulent-node-{idx}"
            try:
                async with websockets.connect(hub_ws_url, close_timeout=1.0) as ws:
                    # Register
                    reg = {
                        "version": "1.0",
                        "correlation_id": str(uuid.uuid4()),
                        "type": "NodeRegistration",
                        "from": node_id,
                        "to": "hub",
                        "platform": "windows",
                        "hostname": "test-host",
                        "timestamp": int(time.time() * 1000),
                    }
                    await ws.send(json.dumps(reg))

                    # Send a few commands (some to grid, some to other nodes)
                    for c_idx in range(5):
                        cmd = {
                            "version": "1.0",
                            "correlation_id": str(uuid.uuid4()),
                            "type": "CommandRequest",
                            "from": node_id,
                            "to": "grid" if c_idx % 2 == 0 else f"turbulent-node-{(idx + 1) % 25}",
                            "command": "grid_compute" if c_idx % 2 == 0 else "echo",
                            "args": {"matrix_dim": 32},
                            "require_ack": True,
                            "timestamp": int(time.time() * 1000),
                        }
                        await ws.send(json.dumps(cmd))
                        await asyncio.sleep(random.uniform(0.001, 0.02))

                    # Abrupt violent exit (close socket with error code or immediately exit context)
                    if random.random() < 0.5:
                        await ws.close(code=1006)  # Abnormal closure simulation
            except Exception:
                # Connection / socket dropped as intended
                pass

        # Run 25 clients concurrently
        await asyncio.gather(*[turbulent_client(i) for i in range(25)])

        logger.info("All 25 turbulent clients finished connection bursts.")
        await asyncio.sleep(0.5)

        # Check Rust Hub health after turbulence
        with urllib.request.urlopen(hub_health_url, timeout=2.0) as resp:
            health_body = json.loads(resp.read().decode())
            assert resp.status == 200, f"Expected 200 OK from health check, got {resp.status}"
            assert health_body.get("status") == "ok", f"Hub health abnormal: {health_body}"

        # Check catalog cleanup
        with urllib.request.urlopen(hub_nodes_url, timeout=2.0) as resp:
            nodes_body = json.loads(resp.read().decode())
            active_nodes = nodes_body if isinstance(nodes_body, list) else nodes_body.get("nodes", [])
            logger.info(f"Post-test active registered nodes on Hub: {len(active_nodes)}")
            # Dead sockets should have been culled or dropped from active sessions
            assert len(active_nodes) == 0, f"Expected 0 active nodes after all closed, got {len(active_nodes)}"

        logger.info("✓ Rust Hub survived all 25 concurrent turbulent client drops with 0 crashes and clean session cleanup")
        return True

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info("Rust Hub stopped.")


if __name__ == "__main__":
    asyncio.run(run_rapid_disconnect_stress())
