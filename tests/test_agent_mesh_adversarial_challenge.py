#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Mesh - Adversarial Challenge Suite
File: tests/test_agent_mesh_adversarial_challenge.py

Author: Challenger 1 (teamwork_preview_challenger)

Mission:
Adversarially challenge the communication mesh protocol, hub relay, and command routing:
1. Empirically verify that Node 1 can send commands to Node 3 and receive valid responses.
2. Verify strict node isolation: ensure Node 2 NEVER receives or executes commands addressed to Node 3.
3. Test edge cases and adversarial scenarios:
   - Send commands to nonexistent node IDs (verify DeliveryNack is returned).
   - Rapid registration / deregistration of nodes (churn stress test).
   - Large or rapid message bursts (burst stress test, concurrent traffic).
4. Run or construct empirical test harnesses to validate these behaviors.
5. Document all commands, test scripts, and execution outputs.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid

# Ensure root and tests directory are in path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from test_agent_mesh_simulation import OxideRelayHub, SimulatedAgentNode
except ImportError:
    from tests.test_agent_mesh_simulation import OxideRelayHub, SimulatedAgentNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("AdvChallenge")


class AdversarialMeshTester:
    def __init__(self):
        self.hub = None
        self.hub_url = None

    async def setup(self):
        self.hub = OxideRelayHub()
        port = await self.hub.start()
        self.hub_url = f"ws://127.0.0.1:{port}"
        logger.info(f"Adversarial Test Hub online at {self.hub_url}")

    async def teardown(self):
        if self.hub:
            await self.hub.stop()

    async def test_01_node1_to_node3_command_flow(self):
        """1. Verify Node 1 -> Node 3 command routing and response accuracy."""
        logger.info("\n--- [Test 1] Node 1 -> Node 3 Command Routing & Execution ---")
        node1 = SimulatedAgentNode("node-win-1", "windows", "host-win")
        node2 = SimulatedAgentNode("node-mac-2", "macos", "host-mac")
        node3 = SimulatedAgentNode("node-android-3", "android", "host-s24")

        try:
            await node1.connect(self.hub_url)
            await node2.connect(self.hub_url)
            await node3.connect(self.hub_url)
            await asyncio.sleep(0.1)

            # Echo command
            echo_res = await node1.send_command(
                target_id="node-android-3",
                command="echo",
                args={"message": "Adversarial Flow Test Payload 2026"},
            )
            assert echo_res["type"] == "CommandResponse"
            assert echo_res["from"] == "node-android-3"
            assert echo_res["to"] == "node-win-1"
            assert echo_res["status"] == "success"
            assert echo_res["exit_code"] == 0
            assert echo_res["stdout"] == "Adversarial Flow Test Payload 2026"
            logger.info("✓ Echo command executed on Node 3 and returned to Node 1 successfully")

            # Real subprocess execution
            cmd = f'{sys.executable} -c "import platform; print(\'SYSTEM_\' + platform.system())"'
            shell_res = await node1.send_command(
                target_id="node-android-3",
                command="shell_exec",
                args={"cmd": cmd},
            )
            assert shell_res["exit_code"] == 0
            assert "SYSTEM_" in shell_res["stdout"]
            logger.info(f"✓ Subprocess shell_exec output: {shell_res['stdout'].strip()}")

            # Verify execution counts
            assert node3.executed_commands_count == 2
            assert node2.executed_commands_count == 0
            assert node1.executed_commands_count == 0
            logger.info("✓ Command flow and execution counts verified")

        finally:
            await node1.close()
            await node2.close()
            await node3.close()

    async def test_02_strict_node_isolation(self):
        """2. Verify strict node isolation under high concurrency."""
        logger.info("\n--- [Test 2] Strict Node Isolation Under High Concurrency ---")
        node1 = SimulatedAgentNode("sender-node-1", "windows", "host-1")
        node2 = SimulatedAgentNode("isolated-node-2", "macos", "host-2")
        node3 = SimulatedAgentNode("target-node-3", "android", "host-3")

        try:
            await node1.connect(self.hub_url)
            await node2.connect(self.hub_url)
            await node3.connect(self.hub_url)
            await asyncio.sleep(0.1)

            concurrency = 40
            tasks = []
            for i in range(concurrency):
                payload_str = f"concurrent-req-{i}"
                tasks.append(
                    node1.send_command(
                        target_id="target-node-3",
                        command="echo",
                        args={"message": payload_str},
                    )
                )

            responses = await asyncio.gather(*tasks)
            assert len(responses) == concurrency
            for resp in responses:
                assert resp["type"] == "CommandResponse"
                assert resp["status"] == "success"
                assert resp["exit_code"] == 0

            # Allow event loop to process any remaining messages
            await asyncio.sleep(0.1)

            # STRICT ISOLATION ASSERTIONS
            assert node3.executed_commands_count == concurrency, (
                f"Target node 3 executed {node3.executed_commands_count}, expected {concurrency}"
            )
            assert node2.executed_commands_count == 0, (
                f"CRITICAL ISOLATION FAILURE: Intermediary Node 2 executed {node2.executed_commands_count} commands!"
            )
            assert node1.executed_commands_count == 0, (
                f"Sender node executed {node1.executed_commands_count} commands!"
            )
            assert len(node2.received_data_payloads) == 0, (
                f"CRITICAL ISOLATION FAILURE: Node 2 received {len(node2.received_data_payloads)} data packets!"
            )

            logger.info(f"✓ Strict isolation maintained over {concurrency} concurrent requests: Node 2 executions = 0")

        finally:
            await node1.close()
            await node2.close()
            await node3.close()

    async def test_03_negative_routing_delivery_nack(self):
        """3. Adversarial Edge Case: DeliveryNack on nonexistent node IDs."""
        logger.info("\n--- [Test 3] Negative Routing & DeliveryNack Handling ---")
        node1 = SimulatedAgentNode("caller-node", "windows")

        try:
            await node1.connect(self.hub_url)
            await asyncio.sleep(0.1)

            # Case A: Standard nonexistent node ID
            nack_1 = await node1.send_command(
                target_id="node-ghost-404",
                command="ping",
                args={},
            )
            assert nack_1["type"] == "DeliveryNack", f"Expected DeliveryNack, got {nack_1.get('type')}"
            assert nack_1["error_code"] == "ERR_NODE_NOT_FOUND"
            assert "node-ghost-404" in nack_1["reason"]
            logger.info(f"✓ Case A passed: Nonexistent node rejected with ERR_NODE_NOT_FOUND")

            # Case B: Unicode nonexistent node ID
            nack_2 = await node1.send_command(
                target_id="alien-👽-node",
                command="echo",
                args={"msg": "hello"},
            )
            assert nack_2["type"] == "DeliveryNack"
            assert nack_2["error_code"] == "ERR_NODE_NOT_FOUND"
            logger.info("✓ Case B passed: Unicode nonexistent node rejected with ERR_NODE_NOT_FOUND")

            # Case C: Unknown command code rejection
            node_target = SimulatedAgentNode("exec-target", "linux")
            await node_target.connect(self.hub_url)
            await asyncio.sleep(0.1)

            unk_res = await node1.send_command(
                target_id="exec-target",
                command="totally_invalid_command_xyz",
                args={},
            )
            assert unk_res["type"] == "CommandResponse"
            assert unk_res["exit_code"] == 127
            assert unk_res["status"] == "failed"
            logger.info("✓ Case C passed: Unknown command rejected with exit code 127")
            await node_target.close()

        finally:
            await node1.close()

    async def test_04_rapid_node_churn(self):
        """4. Adversarial Stress Test: Rapid node registration / deregistration."""
        logger.info("\n--- [Test 4] Rapid Node Registration / Deregistration (Churn) ---")
        anchor_node = SimulatedAgentNode("anchor-node", "windows")

        try:
            await anchor_node.connect(self.hub_url)
            await asyncio.sleep(0.05)

            # Phase A: 25 sequential rapid registrations and disconnects
            logger.info("Executing 25 sequential rapid node churn cycles...")
            for i in range(25):
                cid = f"ephemeral-seq-{i}"
                ephemeral = SimulatedAgentNode(cid, "ubuntu")
                await ephemeral.connect(self.hub_url)
                await asyncio.sleep(0.01)
                await ephemeral.close()
                await asyncio.sleep(0.01)

            # Phase B: 15 concurrent rapid registrations
            logger.info("Executing 15 concurrent rapid node registrations...")
            concurrent_nodes = [SimulatedAgentNode(f"ephemeral-par-{i}", "android") for i in range(15)]
            await asyncio.gather(*(c.connect(self.hub_url) for c in concurrent_nodes))
            await asyncio.sleep(0.1)

            # Verify catalog reflects anchor + 15 = 16 nodes
            cat = await anchor_node.query_node_list()
            active_ids = {n["node_id"] for n in cat["nodes"]}
            assert len(active_ids) == 16, f"Expected 16 active nodes, got {len(active_ids)}"
            assert "anchor-node" in active_ids

            # Disconnect all 15 concurrent nodes
            logger.info("Disconnecting all 15 concurrent nodes...")
            await asyncio.gather(*(c.close() for c in concurrent_nodes))
            await asyncio.sleep(0.15)

            # Verify catalog returns to 1 node
            cat_final = await anchor_node.query_node_list()
            final_ids = {n["node_id"] for n in cat_final["nodes"]}
            assert len(final_ids) == 1, f"Expected 1 active node after churn, got {len(final_ids)} ({final_ids})"
            assert "anchor-node" in final_ids

            logger.info("✓ Rapid churn passed: 40 total churn nodes processed without leak or deadlock")

        finally:
            await anchor_node.close()

    async def test_05_large_and_rapid_burst(self):
        """5. Adversarial Stress Test: Large and rapid message bursts."""
        logger.info("\n--- [Test 5] High-Throughput Burst Stress Test & Boundaries ---")
        node1 = SimulatedAgentNode("burst-node-1", "macos")
        node2 = SimulatedAgentNode("burst-node-2-spy", "windows")
        node3 = SimulatedAgentNode("burst-node-3", "android")

        try:
            await node1.connect(self.hub_url)
            await node2.connect(self.hub_url)
            await node3.connect(self.hub_url)
            await asyncio.sleep(0.1)

            # Part A: Boundary payload sizes up to 192 KB (within 1 MiB JSON frame)
            sizes = [0, 512, 65536, 131072, 196608]  # 0B, 512B, 64KB, 128KB, 192KB
            for sz in sizes:
                test_bytes = os.urandom(sz) if sz > 0 else b""
                expected_sha = hashlib.sha256(test_bytes).hexdigest()
                _, sent_sha = await node1.send_data_payload("burst-node-3", test_bytes)
                assert sent_sha == expected_sha

                await asyncio.sleep(0.1)
                rx_pkt = node3.received_data_payloads[-1]
                rx_bytes = rx_pkt["data"].encode("latin1")
                assert len(rx_bytes) == sz
                assert hashlib.sha256(rx_bytes).hexdigest() == expected_sha
                logger.info(f"✓ Boundary payload {sz} bytes verified with bit-for-bit SHA-256 match")

            # Part B: 100-packet high-speed concurrent burst
            logger.info("Executing 100-packet concurrent bidirectional burst (50 fwd + 50 rev)...")
            start_rx_3 = len(node3.received_data_payloads)
            start_rx_1 = len(node1.received_data_payloads)

            burst_fwd_tasks = []
            burst_rev_tasks = []

            for i in range(50):
                fwd_data = f"BURST-FWD-{i}-{os.urandom(128).hex()}".encode("utf-8")
                rev_data = f"BURST-REV-{i}-{os.urandom(128).hex()}".encode("utf-8")
                burst_fwd_tasks.append(node1.send_data_payload("burst-node-3", fwd_data))
                burst_rev_tasks.append(node3.send_data_payload("burst-node-1", rev_data))

            t0 = time.time()
            await asyncio.gather(*burst_fwd_tasks, *burst_rev_tasks)
            await asyncio.sleep(0.3)
            duration = time.time() - t0

            # Verify packet delivery
            total_rx_3 = len(node3.received_data_payloads) - start_rx_3
            total_rx_1 = len(node1.received_data_payloads) - start_rx_1

            assert total_rx_3 == 50, f"Forward loss! Received {total_rx_3}/50"
            assert total_rx_1 == 50, f"Reverse loss! Received {total_rx_1}/50"

            # Verify Node 2 was never sent any packets
            assert len(node2.received_data_payloads) == 0, (
                f"ISOLATION LEAK: Node 2 received {len(node2.received_data_payloads)} packets during burst!"
            )

            logger.info(
                f"✓ 100 packets verified in {duration:.3f}s ({100 / duration:.1f} pkts/s) with 0.00% DATA LOSS"
            )
            logger.info("✓ Node 2 isolation strictly preserved during burst (0 leaked packets)")

        finally:
            await node1.close()
            await node2.close()
            await node3.close()


async def run_all_adversarial_challenges():
    tester = AdversarialMeshTester()
    await tester.setup()
    try:
        await tester.test_01_node1_to_node3_command_flow()
        await tester.test_02_strict_node_isolation()
        await tester.test_03_negative_routing_delivery_nack()
        await tester.test_04_rapid_node_churn()
        await tester.test_05_large_and_rapid_burst()
        logger.info("\n" + "=" * 80)
        logger.info("  ALL ADVERSARIAL CHALLENGES COMPLETED WITH 100% SUCCESS  ")
        logger.info("=" * 80)
        return True
    finally:
        await tester.teardown()


def main():
    try:
        success = asyncio.run(run_all_adversarial_challenges())
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Adversarial challenge failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
