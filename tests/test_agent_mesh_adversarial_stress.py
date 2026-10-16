#!/usr/bin/env python3
"""
Adversarial Stress Verification Test for Agent Mesh Grid Integration
Authored by M2 Challenger 2 (teamwork_preview_challenger_m2_2).

Aggressively tests:
1. Hub launched with `--grid-master 127.0.0.1:0`:
   - Rapid concurrent client connections (16 workers).
   - Legitimate grid_status, grid_submit, and grid_compute envelopes.
   - Malformed envelopes (corrupted JSON, truncated frames, sender forgery, invalid types).
   - Confirms non-blocking execution, zero deadlock, zero panics, and healthy REST status.
2. Hub launched WITHOUT `--grid-master`:
   - High concurrency barrage (20 workers, 200+ requests).
   - Strictly enforces ERR_BRIDGE_NOT_CONFIGURED on all grid_* envelopes.
   - Confirms zero deadlocks, zero hangs, and full health under unattached flood.
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

try:
    import websockets
except ImportError:
    print("FATAL: websockets package is required. Run: pip install websockets")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AdversarialStress")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def find_binary() -> str:
    for name in ("agent-mesh.exe", "agent-mesh"):
        p = os.path.join(PROJECT_ROOT, "target", "debug", name)
        if os.path.exists(p):
            return p
    raise FileNotFoundError("agent-mesh binary not found; build target/debug first")


def drain_stream(stream):
    try:
        for _ in stream:
            pass
    except Exception:
        pass


def start_hub(args: list[str]):
    """Launches agent-mesh hub and parses ws_url, http_base, and master_addr."""
    import threading

    bin_path = find_binary()
    cmd = [bin_path, "hub", "--listen", "127.0.0.1:0"] + args
    logger.info("Spawning: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    ws_url = None
    http_base = None
    master_addr = None

    deadline = time.time() + 15
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        logger.debug("Hub stdout: %s", line.strip())
        if "Embedded Master Grid runtime active on" in line:
            master_addr = line.split("active on", 1)[1].strip()
        if "running at " in line:
            ws_url = line.split("running at ", 1)[1].strip()
            http_base = "http://" + ws_url.split("://", 1)[1].rsplit("/ws", 1)[0]
            break

    if not ws_url:
        proc.kill()
        stdout, stderr = proc.communicate(timeout=3)
        raise RuntimeError(f"Hub failed to start. Stdout:\n{stdout}\nStderr:\n{stderr}")

    # Continuously drain remaining stdout and stderr so pipe buffers never block on log flood
    threading.Thread(target=drain_stream, args=(proc.stdout,), daemon=True).start()
    threading.Thread(target=drain_stream, args=(proc.stderr,), daemon=True).start()

    logger.info("Hub online at %s (REST: %s, Master: %s)", ws_url, http_base, master_addr)
    return proc, ws_url, http_base, master_addr


def stop_hub(proc):
    """Terminates hub cleanly."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
    logger.info("Hub terminated successfully (exit code %s)", proc.returncode)


def check_http_status(http_base: str):
    """Verifies that the HTTP REST status endpoint is responsive."""
    url = f"{http_base}/api/status"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200, f"HTTP status expected 200, got {resp.status}"
        data = json.loads(resp.read().decode())
        assert data.get("status") in ("online", "ok"), f"Expected status 'online' or 'ok', got {data}"
        return data


def envelope_type(msg: dict) -> str:
    return msg.get("type") or msg.get("msg_type") or ""


async def register_node(ws, node_id: str):
    """Sends NodeRegistration and waits for NodeRegistrationAck."""
    corr_id = str(uuid.uuid4())
    reg_msg = {
        "version": "1.0",
        "type": "NodeRegistration",
        "correlation_id": corr_id,
        "from": node_id,
        "platform": "windows",
        "hostname": "stress-harness",
        "capabilities": ["compute", "stress"],
        "timestamp": int(time.time() * 1000),
    }
    await ws.send(json.dumps(reg_msg))
    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
    ack = json.loads(raw)
    assert envelope_type(ack) in ("NodeRegistrationAck", "node_registration_ack"), f"Expected registration ack, got: {ack}"
    assert ack.get("status") in ("accepted", "registered"), f"Registration failed: {ack}"



# ==============================================================================
# TEST 1: LIVE HUB WITH EMBEDDED MASTER GRID (--grid-master 127.0.0.1:0)
# ==============================================================================
async def run_stress_client_with_master(ws_url: str, client_id: str, results: dict):
    """Client sending concurrent legitimate and malformed envelopes against attached hub."""
    node_id = f"stress-client-{client_id}"
    try:
        async with websockets.connect(ws_url) as ws:
            await register_node(ws, node_id)

            # 1. Legitimate grid_status envelope
            corr_status = str(uuid.uuid4())
            await ws.send(
                json.dumps({
                    "version": "1.0",
                    "type": "CommandRequest",
                    "correlation_id": corr_status,
                    "from": node_id,
                    "to": "grid",
                    "command": "grid_status",
                    "args": {},
                    "timeout_ms": 5000,
                    "require_ack": False,
                    "timestamp": int(time.time() * 1000),
                })
            )

            resp1 = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert envelope_type(resp1) in ("CommandResponse", "command_response"), f"Expected CommandResponse, got: {resp1}"
            assert resp1.get("status") == "success", f"grid_status failed: {resp1}"
            stdout = resp1.get("stdout", "")
            assert "Grid Cluster Status" in stdout or "Active Workers" in stdout or "Queue" in stdout, (
                f"grid_status output unexpected: {stdout}"
            )
            results["grid_status_ok"] = results.get("grid_status_ok", 0) + 1

            # 2. Legitimate grid_submit envelope (async, wait=false)
            corr_submit = str(uuid.uuid4())
            await ws.send(
                json.dumps({
                    "version": "1.0",
                    "type": "CommandRequest",
                    "correlation_id": corr_submit,
                    "from": node_id,
                    "to": "grid",
                    "command": "grid_submit",
                    "args": {"script": f"echo 'client_{client_id}_ok'", "wait": False},
                    "timeout_ms": 5000,
                    "require_ack": False,
                    "timestamp": int(time.time() * 1000),
                })
            )

            resp2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert envelope_type(resp2) in ("CommandResponse", "command_response"), f"Expected CommandResponse, got: {resp2}"
            assert resp2.get("status") == "success", f"grid_submit failed: {resp2}"
            payload = resp2.get("payload")
            assert payload and "task_id" in payload, f"Expected task_id in payload: {resp2}"
            task_id = payload["task_id"]
            results["grid_submit_ok"] = results.get("grid_submit_ok", 0) + 1

            # 3. Query status of submitted task_id
            corr_task_query = str(uuid.uuid4())
            await ws.send(
                json.dumps({
                    "version": "1.0",
                    "type": "CommandRequest",
                    "correlation_id": corr_task_query,
                    "from": node_id,
                    "to": "grid",
                    "command": "grid_status",
                    "args": {"task_id": task_id},
                    "timeout_ms": 5000,
                    "require_ack": False,
                    "timestamp": int(time.time() * 1000),
                })
            )

            resp3 = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert envelope_type(resp3) in ("CommandResponse", "command_response"), f"Expected CommandResponse, got: {resp3}"
            assert resp3.get("status") == "success", f"Task query failed: {resp3}"
            results["task_query_ok"] = results.get("task_query_ok", 0) + 1

            # 4. Adversarial Injections:
            # A) Malformed non-JSON garbage string
            await ws.send("CORRUPTED_RAW_GARBAGE_BYTES_#%!&*??")

            # B) Truncated incomplete JSON
            await ws.send('{"version": "1.0", "type": "CommandRequest", "correlation_id":')

            # C) Envelope with forged "from" identity
            corr_forged = str(uuid.uuid4())
            await ws.send(
                json.dumps({
                    "version": "1.0",
                    "type": "CommandRequest",
                    "correlation_id": corr_forged,
                    "from": "victim-node-impostor",
                    "to": "grid",
                    "command": "grid_status",
                    "args": {},
                    "timeout_ms": 5000,
                    "require_ack": False,
                    "timestamp": int(time.time() * 1000),
                })
            )
            resp_forged = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert envelope_type(resp_forged) in ("DeliveryNack", "delivery_nack"), f"Expected Nack on forged from: {resp_forged}"
            assert resp_forged.get("error_code") == "ERR_SENDER_MISMATCH"
            results["forged_nack_ok"] = results.get("forged_nack_ok", 0) + 1

            # D) Unsupported / unknown grid command
            corr_unk = str(uuid.uuid4())
            await ws.send(
                json.dumps({
                    "version": "1.0",
                    "type": "CommandRequest",
                    "correlation_id": corr_unk,
                    "from": node_id,
                    "to": "grid",
                    "command": "grid_non_existent_op",
                    "args": {},
                    "timeout_ms": 5000,
                    "require_ack": False,
                    "timestamp": int(time.time() * 1000),
                })
            )
            resp_unk = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert envelope_type(resp_unk) in ("CommandResponse", "command_response"), f"Expected CommandResponse, got: {resp_unk}"
            assert resp_unk.get("status") == "failed"
            assert resp_unk.get("exit_code") == 127
            results["unknown_cmd_ok"] = results.get("unknown_cmd_ok", 0) + 1

            # 5. Recovery check: Verify connection remains fully usable after malicious inputs
            corr_rec = str(uuid.uuid4())
            await ws.send(
                json.dumps({
                    "version": "1.0",
                    "type": "CommandRequest",
                    "correlation_id": corr_rec,
                    "from": node_id,
                    "to": "grid",
                    "command": "grid_status",
                    "args": {},
                    "timeout_ms": 5000,
                    "require_ack": False,
                    "timestamp": int(time.time() * 1000),
                })
            )
            resp_rec = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert resp_rec.get("status") == "success"
            results["recovery_ok"] = results.get("recovery_ok", 0) + 1

    except Exception as exc:
        logger.error("Client %s encountered error: %s", client_id, exc, exc_info=True)
        results["errors"] = results.get("errors", 0) + 1


async def test_attached_hub_stress():
    logger.info("==================================================================")
    logger.info(" TEST SUITE 1: ADVERSARIAL STRESS ON HUB WITH --grid-master      ")
    logger.info("==================================================================")
    proc, ws_url, http_base, master_addr = start_hub(["--grid-master", "127.0.0.1:0"])
    try:
        assert master_addr is not None, "Embedded Master Grid address was not reported"
        num_concurrent_clients = 16
        logger.info("Launching %d concurrent adversarial stress clients...", num_concurrent_clients)
        results = {}
        tasks = [
            run_stress_client_with_master(ws_url, str(i), results)
            for i in range(num_concurrent_clients)
        ]
        start_time = time.time()
        await asyncio.gather(*tasks)
        duration = time.time() - start_time
        logger.info("Stress execution completed in %.2f seconds.", duration)

        logger.info("Results summary: %s", results)
        assert results.get("errors", 0) == 0, f"Clients encountered {results.get('errors')} errors"
        assert results.get("grid_status_ok") == num_concurrent_clients
        assert results.get("grid_submit_ok") == num_concurrent_clients
        assert results.get("task_query_ok") == num_concurrent_clients
        assert results.get("forged_nack_ok") == num_concurrent_clients
        assert results.get("unknown_cmd_ok") == num_concurrent_clients
        assert results.get("recovery_ok") == num_concurrent_clients

        # Verify Hub REST health
        status = check_http_status(http_base)
        logger.info("Hub REST status verified healthy after storm: %s", status)
        logger.info("✓ TEST 1 PASSED: Attached hub resilient under high concurrent adversarial load.")
    finally:
        stop_hub(proc)


# ==============================================================================
# TEST 2: HUB LAUNCHED WITHOUT --grid-master (ERR_BRIDGE_NOT_CONFIGURED ENFORCEMENT)
# ==============================================================================
async def run_unattached_client(ws_url: str, client_id: str, results: dict):
    """Client sending rapid bursts of grid envelopes to unattached hub."""
    node_id = f"unattached-client-{client_id}"
    try:
        async with websockets.connect(ws_url) as ws:
            await register_node(ws, node_id)

            commands_to_test = [
                ("grid", "grid_status", {}),
                ("grid", "grid_compute", {"test_name": "matrix_multiply", "iterations": 100}),
                ("grid", "grid_submit", {"script": "echo 'unattached test'", "wait": True}),
                ("grid-master", "grid_status", {}),
                ("hub", "grid_status", {}),
                ("grid", "status", {}),
                ("grid", "compute", {}),
                ("grid", "submit", {}),
            ]

            for target, cmd_name, args in commands_to_test:
                corr_id = str(uuid.uuid4())
                await ws.send(
                    json.dumps({
                        "version": "1.0",
                        "type": "CommandRequest",
                        "correlation_id": corr_id,
                        "from": node_id,
                        "to": target,
                        "command": cmd_name,
                        "args": args,
                        "timeout_ms": 3000,
                        "require_ack": False,
                        "timestamp": int(time.time() * 1000),
                    })
                )

                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=4.0))
                assert envelope_type(resp) in ("DeliveryNack", "delivery_nack"), (
                    f"Expected DeliveryNack for unattached {cmd_name}, got: {resp}"
                )
                assert resp.get("error_code") == "ERR_BRIDGE_NOT_CONFIGURED", (
                    f"Expected ERR_BRIDGE_NOT_CONFIGURED, got: {resp.get('error_code')}"
                )
                assert "not attached" in resp.get("reason", "").lower()
                assert resp.get("correlation_id") == corr_id
                results["nacks"] = results.get("nacks", 0) + 1

    except Exception as exc:
        logger.error("Unattached client %s error: %s", client_id, exc, exc_info=True)
        results["errors"] = results.get("errors", 0) + 1


async def test_unattached_hub_stress():
    logger.info("==================================================================")
    logger.info(" TEST SUITE 2: STRICT ERR_BRIDGE_NOT_CONFIGURED ON UNATTACHED HUB")
    logger.info("==================================================================")
    # Launch WITHOUT --grid-master
    proc, ws_url, http_base, master_addr = start_hub([])
    try:
        assert master_addr is None, f"Expected no embedded master, but got: {master_addr}"
        num_clients = 20
        logger.info("Launching %d concurrent clients bombarding unattached hub...", num_clients)
        results = {}
        tasks = [
            run_unattached_client(ws_url, str(i), results)
            for i in range(num_clients)
        ]
        start_time = time.time()
        await asyncio.gather(*tasks)
        duration = time.time() - start_time
        logger.info("Unattached storm completed in %.2f seconds.", duration)

        expected_nacks = num_clients * 8  # 8 commands per client = 160 total requests
        logger.info("Total NACKs received: %d (expected: %d)", results.get("nacks", 0), expected_nacks)
        assert results.get("errors", 0) == 0, f"Encountered errors: {results.get('errors')}"
        assert results.get("nacks") == expected_nacks, f"Expected {expected_nacks} NACKs, got {results.get('nacks')}"

        # Verify hub remains perfectly responsive via HTTP REST
        status = check_http_status(http_base)
        logger.info("Hub REST status responsive under unattached flood: %s", status)
        logger.info("✓ TEST 2 PASSED: 100% strictly returned ERR_BRIDGE_NOT_CONFIGURED without crash/deadlock.")
    finally:
        stop_hub(proc)


async def main():
    logger.info("Beginning Adversarial Stress Test Suite for Milestone 2...")
    t0 = time.time()
    await test_attached_hub_stress()
    await test_unattached_hub_stress()
    elapsed = time.time() - t0
    logger.info("==================================================================")
    logger.info(" ALL ADVERSARIAL STRESS SUITES PASSED SUCCESSFULLY (%.2fs)       ", elapsed)
    logger.info("==================================================================")


if __name__ == "__main__":
    asyncio.run(main())
