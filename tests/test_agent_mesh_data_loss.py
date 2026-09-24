#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Mesh - Bidirectional Zero-Data-Loss Test Suite
File: tests/test_agent_mesh_data_loss.py

Acceptance Criteria (AC3) from ORIGINAL_REQUEST.md ## 2026-09-23T09:50:43Z:
1. Automated bidirectional data transmission test between nodes.
2. Node 1 sends 64 KB binary payload to Node 3; verifies SHA-256 bit-for-bit integrity.
3. Node 3 sends 64 KB binary payload back to Node 1; verifies SHA-256 bit-for-bit integrity.
4. Concurrent burst test (at least 50 bidirectional packets) proving ZERO DATA LOSS.
5. Boundary edge cases: 0-byte payload boundary, 128 KB extended boundary payload.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid

# Ensure current and parent directories are in sys.path
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
logger = logging.getLogger("TestDataLoss")


async def run_data_loss_tests():
    """Execute complete bidirectional data loss and high-throughput burst test suite."""
    logger.info("==================================================================")
    logger.info("  STARTING BIDIRECTIONAL ZERO-DATA-LOSS VERIFICATION SUITE        ")
    logger.info("==================================================================")

    # 1. Spawn Hub on ephemeral port
    hub = OxideRelayHub()
    port = await hub.start()
    hub_url = f"ws://127.0.0.1:{port}"

    # 2. Spawn 3 agent nodes (Windows, macOS, Android)
    node1 = SimulatedAgentNode("node-win-1", "windows", "windows-workstation")
    node2 = SimulatedAgentNode("node-mac-2", "macos", "macbook-air")
    node3 = SimulatedAgentNode("node-android-3", "android", "pixel-8-pro")

    try:
        logger.info("[Setup] Connecting and registering nodes...")
        await node1.connect(hub_url)
        await node2.connect(hub_url)
        await node3.connect(hub_url)
        await asyncio.sleep(0.1)

        # ------------------------------------------------------------------
        # Phase 1: Forward 64 KB Binary Transmission (Node 1 -> Node 3)
        # ------------------------------------------------------------------
        logger.info("[Phase 1] Forward 64 KB Binary Transmission (Node 1 -> Node 3)...")
        payload_size_64k = 64 * 1024  # Exactly 65,536 bytes
        raw_bytes_forward = os.urandom(payload_size_64k)
        expected_sha256_forward = hashlib.sha256(raw_bytes_forward).hexdigest()

        corr_fwd, hash_fwd = await node1.send_data_payload("node-android-3", raw_bytes_forward)
        assert hash_fwd == expected_sha256_forward, "Sent hash must match pre-calculated SHA-256"

        # Wait for delivery
        await asyncio.sleep(0.1)

        assert len(node3.received_data_payloads) >= 1, "Node 3 did not receive forward DataPayload"
        fwd_received = node3.received_data_payloads[-1]
        assert fwd_received["from"] == "node-win-1", f"Sender mismatch: {fwd_received.get('from')}"
        assert fwd_received["to"] == "node-android-3", f"Target mismatch: {fwd_received.get('to')}"

        fwd_bytes = fwd_received["data"].encode("latin1")
        fwd_sha256 = hashlib.sha256(fwd_bytes).hexdigest()
        assert len(fwd_bytes) == payload_size_64k, f"Length mismatch: expected {payload_size_64k}, got {len(fwd_bytes)}"
        assert fwd_sha256 == expected_sha256_forward, (
            f"Bit-for-bit SHA-256 mismatch in forward direction!\n"
            f"Expected: {expected_sha256_forward}\n"
            f"Received: {fwd_sha256}"
        )
        logger.info(f"✓ Forward 64 KB verified! Bit-for-bit SHA-256: {fwd_sha256} ({len(fwd_bytes)} bytes)")

        # ------------------------------------------------------------------
        # Phase 2: Reverse 64 KB Binary Transmission (Node 3 -> Node 1)
        # ------------------------------------------------------------------
        logger.info("[Phase 2] Reverse 64 KB Binary Transmission (Node 3 -> Node 1)...")
        raw_bytes_reverse = os.urandom(payload_size_64k)
        expected_sha256_reverse = hashlib.sha256(raw_bytes_reverse).hexdigest()

        corr_rev, hash_rev = await node3.send_data_payload("node-win-1", raw_bytes_reverse)
        assert hash_rev == expected_sha256_reverse, "Sent reverse hash must match pre-calculated SHA-256"

        await asyncio.sleep(0.1)

        assert len(node1.received_data_payloads) >= 1, "Node 1 did not receive reverse DataPayload"
        rev_received = node1.received_data_payloads[-1]
        assert rev_received["from"] == "node-android-3", f"Sender mismatch: {rev_received.get('from')}"
        assert rev_received["to"] == "node-win-1", f"Target mismatch: {rev_received.get('to')}"

        rev_bytes = rev_received["data"].encode("latin1")
        rev_sha256 = hashlib.sha256(rev_bytes).hexdigest()
        assert len(rev_bytes) == payload_size_64k, f"Length mismatch: expected {payload_size_64k}, got {len(rev_bytes)}"
        assert rev_sha256 == expected_sha256_reverse, (
            f"Bit-for-bit SHA-256 mismatch in reverse direction!\n"
            f"Expected: {expected_sha256_reverse}\n"
            f"Received: {rev_sha256}"
        )
        logger.info(f"✓ Reverse 64 KB verified! Bit-for-bit SHA-256: {rev_sha256} ({len(rev_bytes)} bytes)")

        # ------------------------------------------------------------------
        # Phase 3: Boundary Value Edge Case - 0 Byte Payload
        # ------------------------------------------------------------------
        logger.info("[Phase 3] Boundary Value Test: 0-byte empty payload transfer...")
        empty_bytes = b""
        expected_empty_sha256 = hashlib.sha256(empty_bytes).hexdigest()
        _, empty_hash = await node1.send_data_payload("node-android-3", empty_bytes)
        assert empty_hash == expected_empty_sha256

        await asyncio.sleep(0.05)
        empty_received = node3.received_data_payloads[-1]
        assert empty_received["data"] == "", "Expected empty data string"
        assert hashlib.sha256(empty_received["data"].encode("latin1")).hexdigest() == expected_empty_sha256
        logger.info("✓ 0-byte boundary payload successfully transmitted and verified")

        # ------------------------------------------------------------------
        # Phase 4: Boundary Value Edge Case - 128 KB Extended Payload
        # ------------------------------------------------------------------
        logger.info("[Phase 4] Boundary Value Test: 128 KB large binary payload transfer...")
        payload_size_128k = 128 * 1024
        raw_bytes_128k = os.urandom(payload_size_128k)
        expected_sha256_128k = hashlib.sha256(raw_bytes_128k).hexdigest()

        _, hash_128k = await node3.send_data_payload("node-win-1", raw_bytes_128k)
        await asyncio.sleep(0.1)

        received_128k = node1.received_data_payloads[-1]
        bytes_128k = received_128k["data"].encode("latin1")
        assert len(bytes_128k) == payload_size_128k
        assert hashlib.sha256(bytes_128k).hexdigest() == expected_sha256_128k
        logger.info(f"✓ 128 KB extended boundary payload verified ({len(bytes_128k)} bytes)")

        # ------------------------------------------------------------------
        # Phase 5: High-Frequency Concurrent Burst Stress Test (50 Bidirectional Exchanges = 100 Packets)
        # ------------------------------------------------------------------
        logger.info("[Phase 5] Concurrent Burst Stress Test (50 bidirectional packets = 100 total packets)...")
        burst_rounds = 50

        # Record starting count on each node
        start_count_node3 = len(node3.received_data_payloads)
        start_count_node1 = len(node1.received_data_payloads)

        sent_hashes_fwd = []
        sent_hashes_rev = []

        start_time = time.time()

        # Send 50 forward and 50 reverse packets concurrently
        for seq in range(burst_rounds):
            # Forward packet (Node 1 -> Node 3)
            fwd_chunk = f"BURST-FWD-SEQ-{seq:04d}-{os.urandom(256).hex()}".encode("utf-8")
            fwd_h = hashlib.sha256(fwd_chunk).hexdigest()
            sent_hashes_fwd.append((seq, fwd_chunk, fwd_h))
            await node1.send_data_payload("node-android-3", fwd_chunk)

            # Reverse packet (Node 3 -> Node 1)
            rev_chunk = f"BURST-REV-SEQ-{seq:04d}-{os.urandom(256).hex()}".encode("utf-8")
            rev_h = hashlib.sha256(rev_chunk).hexdigest()
            sent_hashes_rev.append((seq, rev_chunk, rev_h))
            await node3.send_data_payload("node-win-1", rev_chunk)

        # Allow network event loop to process all frames
        await asyncio.sleep(0.3)
        duration = time.time() - start_time

        # Verify Node 3 received all 50 forward packets
        total_fwd_received = len(node3.received_data_payloads) - start_count_node3
        assert total_fwd_received == burst_rounds, (
            f"Forward packet loss detected! Expected {burst_rounds}, received {total_fwd_received}"
        )

        # Verify Node 1 received all 50 reverse packets
        total_rev_received = len(node1.received_data_payloads) - start_count_node1
        assert total_rev_received == burst_rounds, (
            f"Reverse packet loss detected! Expected {burst_rounds}, received {total_rev_received}"
        )

        # Verify bit-for-bit integrity of every burst packet
        for idx, (seq, orig_bytes, orig_hash) in enumerate(sent_hashes_fwd):
            rx_envelope = node3.received_data_payloads[start_count_node3 + idx]
            rx_bytes = rx_envelope["data"].encode("latin1")
            assert rx_bytes == orig_bytes, f"Payload corrupted in forward burst seq={seq}"
            assert hashlib.sha256(rx_bytes).hexdigest() == orig_hash, f"Hash mismatch in burst seq={seq}"

        for idx, (seq, orig_bytes, orig_hash) in enumerate(sent_hashes_rev):
            rx_envelope = node1.received_data_payloads[start_count_node1 + idx]
            rx_bytes = rx_envelope["data"].encode("latin1")
            assert rx_bytes == orig_bytes, f"Payload corrupted in reverse burst seq={seq}"
            assert hashlib.sha256(rx_bytes).hexdigest() == orig_hash, f"Hash mismatch in reverse burst seq={seq}"

        logger.info(
            f"✓ 50 forward + 50 reverse = 100 packets transmitted and verified in {duration:.3f}s "
            f"({100 / duration:.1f} pkts/sec) with 0.00% DATA LOSS!"
        )

        # ------------------------------------------------------------------
        # Phase 6: Intermediary Node 2 Isolation Check
        # ------------------------------------------------------------------
        logger.info("[Phase 6] Verifying Node 2 isolation under continuous data burst...")
        assert len(node2.received_data_payloads) == 0, (
            f"Node 2 isolation breach! Received {len(node2.received_data_payloads)} data packets meant for other nodes."
        )
        logger.info("✓ Node 2 isolation strictly preserved: 0 leaked packets under load")

        logger.info("==================================================================")
        logger.info("  BIDIRECTIONAL ZERO-DATA-LOSS SUITE PASSED (100% SUCCESS)        ")
        logger.info("==================================================================")
        return True

    finally:
        await node1.close()
        await node2.close()
        await node3.close()
        await hub.stop()


def main():
    try:
        success = asyncio.run(run_data_loss_tests())
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Data loss test suite failed with exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
