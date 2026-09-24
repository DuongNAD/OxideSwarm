#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Mesh - Simulation Test Suite
File: tests/test_agent_mesh_simulation.py

Acceptance Criteria (AC1 & AC2) from ORIGINAL_REQUEST.md ## 2026-09-23T09:50:43Z:
1. Spawns an in-process OxideRelay Hub on an ephemeral port (127.0.0.1:0).
2. Spawns at least 3 simulated agent nodes:
   - Node 1: Windows Agent ("node-win-1")
   - Node 2: macOS Agent ("node-mac-2")
   - Node 3: Android Agent ("node-android-3")
3. Verifies all 3 nodes register in the hub catalog via NodeList query.
4. Dispatches a structured command from Node 1 specifically targeted to Node 3.
5. Verifies Node 3 receives, executes the command, and responds back to Node 1 with exit code 0, duration, and output.
6. Verifies Node 2 isolation: Node 2 does not receive or execute commands intended for Node 3.
7. Tests error handling: routing to unknown node returns DeliveryNack.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid

try:
    import websockets
except ImportError:
    print("[ERROR] 'websockets' library is required. Install via: pip install websockets")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TestSimulation")


# ==============================================================================
# 1. In-Process OxideRelay Hub
# ==============================================================================
class OxideRelayHub:
    """Asynchronous WebSocket Relay Hub routing messages between connected agent nodes."""

    def __init__(self, host="127.0.0.1", port=0):
        self.host = host
        self.port = port
        self.server = None
        self.actual_port = None
        # Registry: node_id -> {"ws": websocket, "platform": str, "hostname": str, ...}
        self.nodes = {}
        # In-flight tracking: correlation_id -> dict
        self.inflight = {}
        self.stats = {
            "registered_nodes": 0,
            "routed_commands": 0,
            "routed_responses": 0,
            "routed_data": 0,
            "delivered_acks": 0,
            "delivered_nacks": 0,
        }

    async def start(self):
        """Start the hub on the configured host and ephemeral port."""
        self.server = await websockets.serve(self._handle_client, self.host, self.port)
        self.actual_port = self.server.sockets[0].getsockname()[1]
        logger.info(f"OxideRelay Hub started on ws://{self.host}:{self.actual_port}")
        return self.actual_port

    async def stop(self):
        """Stop the hub and terminate all active connections."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("OxideRelay Hub stopped.")

    async def _handle_client(self, websocket):
        current_node_id = None
        try:
            async for raw_message in websocket:
                try:
                    msg = json.loads(raw_message)
                except Exception as e:
                    logger.error(f"Malformed JSON from client: {e}")
                    continue

                msg_type = msg.get("type") or msg.get("msg_type")
                corr_id = msg.get("id") or msg.get("correlation_id") or str(uuid.uuid4())

                # A. Handle Node Registration
                if msg_type == "NodeRegistration":
                    node_id = msg.get("from") or msg.get("node_id")
                    current_node_id = node_id
                    self.nodes[node_id] = {
                        "ws": websocket,
                        "platform": msg.get("platform", "unknown"),
                        "hostname": msg.get("hostname", "unknown"),
                        "capabilities": msg.get("capabilities", {}),
                        "connected_at": time.time(),
                        "last_heartbeat": time.time(),
                    }
                    self.stats["registered_nodes"] += 1
                    logger.info(f"Hub: Node registered -> '{node_id}' (platform={msg.get('platform')})")
                    ack = {
                        "version": "1.0",
                        "correlation_id": corr_id,
                        "type": "NodeRegistrationAck",
                        "from": "hub",
                        "to": node_id,
                        "status": "accepted",
                        "assigned_node_id": node_id,
                        "heartbeat_interval_ms": 10000,
                        "cluster_id": "oxideswarm-sim-cluster",
                        "timestamp": int(time.time() * 1000),
                    }
                    await websocket.send(json.dumps(ack))

                # B. Handle NodeList Query
                elif msg_type == "NodeList":
                    catalog = []
                    for nid, info in self.nodes.items():
                        catalog.append({
                            "node_id": nid,
                            "platform": info["platform"],
                            "hostname": info["hostname"],
                            "status": "online",
                            "capabilities": info["capabilities"],
                        })
                    res = {
                        "version": "1.0",
                        "correlation_id": corr_id,
                        "type": "NodeListResponse",
                        "from": "hub",
                        "to": msg.get("from"),
                        "nodes": catalog,
                        "timestamp": int(time.time() * 1000),
                    }
                    await websocket.send(json.dumps(res))

                # C. Handle Heartbeat
                elif msg_type == "Heartbeat":
                    nid = msg.get("from")
                    if nid in self.nodes:
                        self.nodes[nid]["last_heartbeat"] = time.time()
                    ack = {
                        "version": "1.0",
                        "correlation_id": corr_id,
                        "type": "HeartbeatAck",
                        "from": "hub",
                        "to": nid,
                        "timestamp": int(time.time() * 1000),
                    }
                    await websocket.send(json.dumps(ack))

                # D. Handle CommandRequest
                elif msg_type == "CommandRequest":
                    target = msg.get("to")
                    sender = msg.get("from")
                    self.stats["routed_commands"] += 1

                    if target not in self.nodes:
                        logger.warning(f"Hub: Routing failed - target '{target}' not in registry.")
                        self.stats["delivered_nacks"] += 1
                        nack = {
                            "version": "1.0",
                            "correlation_id": corr_id,
                            "type": "DeliveryNack",
                            "from": "hub",
                            "to": sender,
                            "error_code": "ERR_NODE_NOT_FOUND",
                            "reason": f"Target node '{target}' is not registered with the hub.",
                            "timestamp": int(time.time() * 1000),
                        }
                        await websocket.send(json.dumps(nack))
                    else:
                        target_ws = self.nodes[target]["ws"]
                        self.inflight[corr_id] = {
                            "from": sender,
                            "to": target,
                            "timestamp": time.time(),
                        }
                        await target_ws.send(raw_message)

                        if msg.get("require_ack", True):
                            self.stats["delivered_acks"] += 1
                            ack = {
                                "version": "1.0",
                                "correlation_id": corr_id,
                                "type": "DeliveryAck",
                                "from": "hub",
                                "to": sender,
                                "status": "target_forwarded",
                                "timestamp": int(time.time() * 1000),
                            }
                            await websocket.send(json.dumps(ack))

                # E. Handle CommandResponse
                elif msg_type == "CommandResponse":
                    target = msg.get("to")
                    self.stats["routed_responses"] += 1
                    if target in self.nodes:
                        target_ws = self.nodes[target]["ws"]
                        await target_ws.send(raw_message)
                        self.inflight.pop(corr_id, None)
                    else:
                        logger.warning(f"Hub: Response target '{target}' disconnected.")

                # F. Handle DataPayload
                elif msg_type == "DataPayload":
                    target = msg.get("to")
                    self.stats["routed_data"] += 1
                    if target in self.nodes:
                        target_ws = self.nodes[target]["ws"]
                        await target_ws.send(raw_message)
                    else:
                        nack = {
                            "version": "1.0",
                            "correlation_id": corr_id,
                            "type": "DeliveryNack",
                            "from": "hub",
                            "to": msg.get("from"),
                            "error_code": "ERR_NODE_NOT_FOUND",
                            "reason": f"Data target '{target}' not found.",
                            "timestamp": int(time.time() * 1000),
                        }
                        await websocket.send(json.dumps(nack))

                # G. Handle DeliveryAck / Nack forwarding
                elif msg_type in ("DeliveryAck", "DeliveryNack"):
                    target = msg.get("to")
                    if target in self.nodes and target != "hub":
                        await self.nodes[target]["ws"].send(raw_message)

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if current_node_id and current_node_id in self.nodes:
                logger.info(f"Hub: Node disconnected -> '{current_node_id}'")
                del self.nodes[current_node_id]


# ==============================================================================
# 2. Simulated Agent Node
# ==============================================================================
class SimulatedAgentNode:
    """Simulated cross-platform agent node connecting to OxideRelay Hub."""

    def __init__(self, node_id, platform, hostname="sim-host"):
        self.node_id = node_id
        self.platform = platform
        self.hostname = hostname
        self.ws = None
        self.running = False
        self.pending_responses = {}  # corr_id -> asyncio.Future
        self.received_data_payloads = []
        self.executed_commands_count = 0
        self.ignored_commands_count = 0

    async def connect(self, hub_url):
        """Establish outbound WebSocket connection and register with Hub."""
        self.ws = await websockets.connect(hub_url)
        self.running = True

        reg = {
            "version": "1.0",
            "correlation_id": str(uuid.uuid4()),
            "type": "NodeRegistration",
            "from": self.node_id,
            "to": "hub",
            "platform": self.platform,
            "hostname": self.hostname,
            "capabilities": {
                "os": self.platform,
                "cpu_cores": 4,
                "supported_commands": ["echo", "shell_exec", "sha256_verify"],
            },
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(reg))
        asyncio.create_task(self._listen_loop())

    async def _listen_loop(self):
        """Listen for incoming messages from Hub."""
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                msg_type = msg.get("type") or msg.get("msg_type")
                corr_id = msg.get("correlation_id") or msg.get("id")
                target = msg.get("to")

                # Verify non-interference isolation: ignore messages targeted to other nodes
                if target and target != self.node_id and target != "all" and target != "hub":
                    self.ignored_commands_count += 1
                    logger.warning(f"[{self.node_id}] Message addressed to '{target}' ignored by {self.node_id}.")
                    continue

                if msg_type == "CommandRequest":
                    await self._handle_command_request(msg)
                elif msg_type in ("CommandResponse", "DeliveryNack", "NodeListResponse"):
                    if corr_id in self.pending_responses:
                        fut = self.pending_responses.pop(corr_id)
                        if not fut.done():
                            fut.set_result(msg)
                elif msg_type == "DataPayload":
                    self.received_data_payloads.append(msg)
                    if corr_id in self.pending_responses:
                        fut = self.pending_responses.pop(corr_id)
                        if not fut.done():
                            fut.set_result(msg)

        except websockets.exceptions.ConnectionClosed:
            self.running = False

    async def _handle_command_request(self, req):
        """Execute command locally and send CommandResponse back to caller."""
        self.executed_commands_count += 1
        corr_id = req.get("correlation_id") or req.get("id")
        sender = req.get("from")
        cmd_name = req.get("command")
        args = req.get("args", {})
        start_time = time.time()

        logger.info(f"[{self.node_id}] Executing '{cmd_name}' from '{sender}' (corr_id={corr_id})")

        stdout = ""
        stderr = ""
        exit_code = 0
        status = "success"

        if cmd_name == "echo":
            if isinstance(args, dict):
                stdout = args.get("message", "")
            elif isinstance(args, list):
                stdout = " ".join(args)
            else:
                stdout = str(args)
        elif cmd_name == "shell_exec":
            if isinstance(args, dict):
                cmd_str = args.get("cmd", "")
            elif isinstance(args, list):
                cmd_str = " ".join(args)
            else:
                cmd_str = str(args)
            try:
                proc = await asyncio.create_subprocess_shell(
                    cmd_str,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, err = await asyncio.wait_for(proc.communicate(), timeout=5.0)
                stdout = out.decode("utf-8", errors="replace")
                stderr = err.decode("utf-8", errors="replace")
                exit_code = proc.returncode
            except Exception as e:
                status = "failed"
                exit_code = 1
                stderr = str(e)
        else:
            status = "failed"
            exit_code = 127
            stderr = f"Unknown command '{cmd_name}'"

        duration_ms = int((time.time() - start_time) * 1000)
        res = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "CommandResponse",
            "from": self.node_id,
            "to": sender,
            "command": cmd_name,
            "status": status,
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "execution_duration_ms": duration_ms,
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(res))

    async def send_command(self, target_id, command, args, timeout=5.0):
        """Send a CommandRequest to target node and await CommandResponse."""
        corr_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[corr_id] = fut

        req = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "CommandRequest",
            "from": self.node_id,
            "to": target_id,
            "command": command,
            "args": args,
            "require_ack": True,
            "timeout_ms": int(timeout * 1000),
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(req))
        return await asyncio.wait_for(fut, timeout=timeout)

    async def send_data_payload(self, target_id, data_bytes, timeout=5.0):
        """Send raw binary DataPayload and compute SHA-256."""
        corr_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[corr_id] = fut

        sha256_hash = hashlib.sha256(data_bytes).hexdigest()
        data_text = data_bytes.decode("latin1")

        payload_msg = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "DataPayload",
            "from": self.node_id,
            "to": target_id,
            "payload_type": "raw_bytes",
            "sequence_number": 0,
            "total_chunks": 1,
            "chunk_size_bytes": len(data_bytes),
            "data": data_text,
            "checksum_sha256": sha256_hash,
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(payload_msg))
        return corr_id, sha256_hash

    async def query_node_list(self, timeout=3.0):
        """Query active node catalog from Hub."""
        corr_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[corr_id] = fut

        req = {
            "version": "1.0",
            "correlation_id": corr_id,
            "type": "NodeList",
            "from": self.node_id,
            "to": "hub",
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send(json.dumps(req))
        return await asyncio.wait_for(fut, timeout=timeout)

    async def close(self):
        """Close WebSocket connection cleanly."""
        self.running = False
        if self.ws:
            await self.ws.close()


# ==============================================================================
# 3. Test Suite Execution Logic
# ==============================================================================
async def run_simulation_tests():
    """Execute complete 3-node simulation and command routing test suite."""
    logger.info("==================================================================")
    logger.info("  STARTING 3-NODE MESH SIMULATION & COMMAND ROUTING SUITE         ")
    logger.info("==================================================================")

    # 1. Start Hub on ephemeral port
    hub = OxideRelayHub()
    port = await hub.start()
    hub_url = f"ws://127.0.0.1:{port}"

    # 2. Initialize 3 heterogeneous simulated nodes
    node1 = SimulatedAgentNode("node-win-1", "windows", "windows-desktop")
    node2 = SimulatedAgentNode("node-mac-2", "macos", "macbook-m3")
    node3 = SimulatedAgentNode("node-android-3", "android", "galaxy-s24")

    try:
        # Step 1: Connect and Register
        logger.info("[Step 1] Connecting and registering nodes (Windows, macOS, Android)...")
        await node1.connect(hub_url)
        await node2.connect(hub_url)
        await node3.connect(hub_url)
        await asyncio.sleep(0.1)

        # Step 2: Verify Catalog Query
        logger.info("[Step 2] Querying Hub Active Directory Catalog...")
        catalog_res = await node1.query_node_list()
        assert catalog_res["type"] == "NodeListResponse", f"Expected NodeListResponse, got {catalog_res.get('type')}"
        active_nodes = {n["node_id"]: n for n in catalog_res["nodes"]}

        assert "node-win-1" in active_nodes, "Node 1 missing from active catalog"
        assert "node-mac-2" in active_nodes, "Node 2 missing from active catalog"
        assert "node-android-3" in active_nodes, "Node 3 missing from active catalog"
        assert active_nodes["node-win-1"]["platform"] == "windows"
        assert active_nodes["node-mac-2"]["platform"] == "macos"
        assert active_nodes["node-android-3"]["platform"] == "android"
        logger.info(f"✓ All 3 nodes verified in active catalog: {list(active_nodes.keys())}")

        # Step 3: Targeted Command Routing (Node 1 -> Node 3)
        logger.info("[Step 3] Dispatching targeted command: Node 1 -> Node 3 (echo)...")
        test_payload = "OxideSwarm Core Mesh Routing Verification"
        echo_res = await node1.send_command(
            target_id="node-android-3",
            command="echo",
            args={"message": test_payload},
        )
        assert echo_res["type"] == "CommandResponse", f"Expected CommandResponse, got {echo_res['type']}"
        assert echo_res["from"] == "node-android-3", f"Expected from node-android-3, got {echo_res['from']}"
        assert echo_res["to"] == "node-win-1", f"Expected to node-win-1, got {echo_res['to']}"
        assert echo_res["status"] == "success", f"Command failed: {echo_res}"
        assert echo_res["exit_code"] == 0, f"Expected exit code 0, got {echo_res['exit_code']}"
        assert echo_res["stdout"] == test_payload, f"Stdout mismatch: expected '{test_payload}', got '{echo_res['stdout']}'"
        assert echo_res["execution_duration_ms"] >= 0, "Execution duration should be non-negative"
        logger.info(f"✓ Node 3 executed echo command and responded to Node 1 in {echo_res['execution_duration_ms']} ms")

        # Step 4: Verify Node 2 Non-Interference Isolation
        logger.info("[Step 4] Verifying Node 2 isolation (zero command cross-talk)...")
        assert node3.executed_commands_count == 1, f"Node 3 execution count expected 1, got {node3.executed_commands_count}"
        assert node2.executed_commands_count == 0, f"Node 2 should have 0 executions, got {node2.executed_commands_count}"
        logger.info("✓ Node 2 isolation verified: executed_commands_count = 0 (no leaked execution)")

        # Step 5: Subprocess Execution on Node 3 (shell_exec)
        logger.info("[Step 5] Real subprocess shell_exec on Node 3...")
        shell_res = await node1.send_command(
            target_id="node-android-3",
            command="shell_exec",
            args={"cmd": f'{sys.executable} -c "import platform; print(\'OS_\' + platform.system())"'},
        )
        assert shell_res["exit_code"] == 0, f"Subprocess failed: {shell_res}"
        assert "OS_" in shell_res["stdout"], f"Unexpected subprocess output: {shell_res['stdout']}"
        logger.info(f"✓ Real subprocess execution on Node 3 verified: {shell_res['stdout'].strip()}")

        # Step 6: Negative Routing Test -> DeliveryNack
        logger.info("[Step 6] Negative routing test (target 'node-ghost-404')...")
        nack_res = await node1.send_command(
            target_id="node-ghost-404",
            command="ping",
            args={},
        )
        assert nack_res["type"] == "DeliveryNack", f"Expected DeliveryNack, got {nack_res['type']}"
        assert nack_res["error_code"] == "ERR_NODE_NOT_FOUND", f"Unexpected error code: {nack_res['error_code']}"
        logger.info(f"✓ Negative routing correctly rejected with DeliveryNack: {nack_res['reason']}")

        # Step 7: Unknown command error handling
        logger.info("[Step 7] Unknown command execution error handling...")
        unknown_res = await node1.send_command(
            target_id="node-android-3",
            command="non_existent_command_xyz",
            args={},
        )
        assert unknown_res["exit_code"] == 127, f"Expected exit code 127, got {unknown_res['exit_code']}"
        assert unknown_res["status"] == "failed", f"Expected failed status, got {unknown_res['status']}"
        logger.info("✓ Unknown command safely rejected by agent node with exit code 127")

        logger.info("==================================================================")
        logger.info("  3-NODE MESH SIMULATION SUITE PASSED (100% SUCCESS)              ")
        logger.info("==================================================================")
        return True

    finally:
        await node1.close()
        await node2.close()
        await node3.close()
        await hub.stop()


def main():
    try:
        success = asyncio.run(run_simulation_tests())
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Simulation test suite failed with exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
