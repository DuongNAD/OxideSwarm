#!/usr/bin/env python3
"""
OxideSwarm Challenger 2 - Bidirectional Data Transmission & High-Concurrency Burst Test
File: tests/test_challenger_bidirectional_burst.py

Mission Verification:
1. Forward transmission (Node 1 -> Node 3) with cryptographic SHA-256 verification.
2. Reverse transmission (Node 3 -> Node 1) with cryptographic SHA-256 verification.
3. Concurrency stress test: high-frequency bursts (100, 200, 500 packets).
4. 0.00% packet loss verification and intermediary Node 2 non-interference isolation.
5. Boundary limit analysis: 64 KB standard, 128 KB extended, and 256 KB frame-limit failure boundary.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid

# Ensure root directory in sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

try:
    from test_agent_mesh_simulation import OxideRelayHub, SimulatedAgentNode
except ImportError:
    from tests.test_agent_mesh_simulation import OxideRelayHub, SimulatedAgentNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DataLossBurstChallenge")


async def run_burst_stress_tests():
    logger.info("==================================================================")
    logger.info("  BIDIRECTIONAL DATA LOSS & HIGH-CONCURRENCY BURST STRESS TEST    ")
    logger.info("==================================================================")

    hub = OxideRelayHub()
    port = await hub.start()
    hub_url = f"ws://127.0.0.1:{port}"

    node1 = SimulatedAgentNode("stress-win-1", "windows", "win-station")
    node2 = SimulatedAgentNode("stress-mac-2", "macos", "mac-laptop")
    node3 = SimulatedAgentNode("stress-android-3", "android", "phone-s24")

    burst_report = {
        "forward_tests": [],
        "reverse_tests": [],
        "burst_tests": [],
        "boundary_findings": {},
        "node2_leak_count": 0,
        "overall_packet_loss_pct": 0.0,
    }

    try:
        await node1.connect(hub_url)
        await node2.connect(hub_url)
        await node3.connect(hub_url)
        await asyncio.sleep(0.2)

        # ----------------------------------------------------------------------
        # Test 1: Forward Transmission Multi-Scale SHA-256 Verification
        # ----------------------------------------------------------------------
        test_sizes = [
            ("16 KB", 16 * 1024),
            ("32 KB", 32 * 1024),
            ("64 KB", 64 * 1024),
            ("128 KB", 128 * 1024),
        ]

        logger.info("[Phase 1] Forward Transmission (Node 1 -> Node 3) Cryptographic Verification...")
        for name, size_bytes in test_sizes:
            raw_data = os.urandom(size_bytes)
            expected_sha = hashlib.sha256(raw_data).hexdigest()

            start_idx = len(node3.received_data_payloads)
            t0 = time.time()
            corr_id, sent_hash = await node1.send_data_payload("stress-android-3", raw_data)
            assert sent_hash == expected_sha

            # Wait for delivery
            for _ in range(50):
                if len(node3.received_data_payloads) > start_idx:
                    break
                await asyncio.sleep(0.05)

            elapsed = time.time() - t0
            assert len(node3.received_data_payloads) > start_idx, f"Timeout waiting for forward {name} payload"

            rx_env = node3.received_data_payloads[-1]
            rx_bytes = rx_env["data"].encode("latin1")
            actual_sha = hashlib.sha256(rx_bytes).hexdigest()

            assert len(rx_bytes) == size_bytes, f"Size mismatch for {name}: expected {size_bytes}, got {len(rx_bytes)}"
            assert actual_sha == expected_sha, f"SHA-256 mismatch for {name} forward: expected {expected_sha}, got {actual_sha}"

            throughput_mbps = (size_bytes / (1024 * 1024)) / elapsed
            logger.info(f"✓ Forward {name:6s} ({size_bytes:7d} bytes) verified! SHA-256: {actual_sha} in {elapsed:.3f}s ({throughput_mbps:.2f} MB/s)")
            burst_report["forward_tests"].append({
                "size_name": name,
                "size_bytes": size_bytes,
                "sha256": actual_sha,
                "elapsed_s": round(elapsed, 4),
                "throughput_mb_s": round(throughput_mbps, 2),
                "verified": True,
            })

        # ----------------------------------------------------------------------
        # Test 2: Reverse Transmission Multi-Scale SHA-256 Verification
        # ----------------------------------------------------------------------
        logger.info("[Phase 2] Reverse Transmission (Node 3 -> Node 1) Cryptographic Verification...")
        for name, size_bytes in test_sizes:
            raw_data = os.urandom(size_bytes)
            expected_sha = hashlib.sha256(raw_data).hexdigest()

            start_idx = len(node1.received_data_payloads)
            t0 = time.time()
            corr_id, sent_hash = await node3.send_data_payload("stress-win-1", raw_data)
            assert sent_hash == expected_sha

            for _ in range(50):
                if len(node1.received_data_payloads) > start_idx:
                    break
                await asyncio.sleep(0.05)

            elapsed = time.time() - t0
            assert len(node1.received_data_payloads) > start_idx, f"Timeout waiting for reverse {name} payload"

            rx_env = node1.received_data_payloads[-1]
            rx_bytes = rx_env["data"].encode("latin1")
            actual_sha = hashlib.sha256(rx_bytes).hexdigest()

            assert len(rx_bytes) == size_bytes, f"Size mismatch for reverse {name}"
            assert actual_sha == expected_sha, f"SHA-256 mismatch for reverse {name}"

            throughput_mbps = (size_bytes / (1024 * 1024)) / elapsed
            logger.info(f"✓ Reverse {name:6s} ({size_bytes:7d} bytes) verified! SHA-256: {actual_sha} in {elapsed:.3f}s ({throughput_mbps:.2f} MB/s)")
            burst_report["reverse_tests"].append({
                "size_name": name,
                "size_bytes": size_bytes,
                "sha256": actual_sha,
                "elapsed_s": round(elapsed, 4),
                "throughput_mb_s": round(throughput_mbps, 2),
                "verified": True,
            })

        # ----------------------------------------------------------------------
        # Test 3: Concurrency Burst Stress Tests (100, 200, 500 Packets)
        # ----------------------------------------------------------------------
        burst_configs = [50, 100, 250]  # Pairs: each pair sends 1 fwd + 1 rev -> total packets = 2 * count

        for burst_pairs in burst_configs:
            total_pkts = burst_pairs * 2
            logger.info(f"[Phase 3] High-Frequency Burst Test: {burst_pairs} fwd + {burst_pairs} rev = {total_pkts} packets...")

            start_count_node3 = len(node3.received_data_payloads)
            start_count_node1 = len(node1.received_data_payloads)

            fwd_records = []
            rev_records = []

            t_burst_start = time.time()

            # Dispatch burst concurrently
            fwd_tasks = []
            rev_tasks = []

            for seq in range(burst_pairs):
                # Forward chunk
                fwd_data = f"BURST-FWD-PAIR{burst_pairs:03d}-SEQ{seq:04d}-{os.urandom(256).hex()}".encode("latin1")
                fwd_hash = hashlib.sha256(fwd_data).hexdigest()
                fwd_records.append((seq, fwd_data, fwd_hash))
                fwd_tasks.append(node1.send_data_payload("stress-android-3", fwd_data))

                # Reverse chunk
                rev_data = f"BURST-REV-PAIR{burst_pairs:03d}-SEQ{seq:04d}-{os.urandom(256).hex()}".encode("latin1")
                rev_hash = hashlib.sha256(rev_data).hexdigest()
                rev_records.append((seq, rev_data, rev_hash))
                rev_tasks.append(node3.send_data_payload("stress-win-1", rev_data))

            # Interleave dispatch
            for f_task, r_task in zip(fwd_tasks, rev_tasks):
                await f_task
                await r_task

            # Wait for all packets to be delivered
            max_wait_s = 6.0
            wait_t0 = time.time()
            while time.time() - wait_t0 < max_wait_s:
                fwd_rcvd = len(node3.received_data_payloads) - start_count_node3
                rev_rcvd = len(node1.received_data_payloads) - start_count_node1
                if fwd_rcvd >= burst_pairs and rev_rcvd >= burst_pairs:
                    break
                await asyncio.sleep(0.05)

            burst_duration = time.time() - t_burst_start

            fwd_received_count = len(node3.received_data_payloads) - start_count_node3
            rev_received_count = len(node1.received_data_payloads) - start_count_node1
            total_received = fwd_received_count + rev_received_count
            packets_lost = total_pkts - total_received
            loss_rate = (packets_lost / total_pkts) * 100.0

            assert fwd_received_count == burst_pairs, f"Forward loss in {total_pkts}-burst: {fwd_received_count}/{burst_pairs}"
            assert rev_received_count == burst_pairs, f"Reverse loss in {total_pkts}-burst: {rev_received_count}/{burst_pairs}"

            # Verify cryptographic SHA-256 for all burst packets
            for idx, (seq, orig_bytes, orig_hash) in enumerate(fwd_records):
                rx_p = node3.received_data_payloads[start_count_node3 + idx]
                rx_b = rx_p["data"].encode("latin1")
                assert rx_b == orig_bytes
                assert hashlib.sha256(rx_b).hexdigest() == orig_hash

            for idx, (seq, orig_bytes, orig_hash) in enumerate(rev_records):
                rx_p = node1.received_data_payloads[start_count_node1 + idx]
                rx_b = rx_p["data"].encode("latin1")
                assert rx_b == orig_bytes
                assert hashlib.sha256(rx_b).hexdigest() == orig_hash

            rate_pkts_sec = total_pkts / burst_duration
            logger.info(f"✓ Burst {total_pkts} packets passed! Duration: {burst_duration:.3f}s ({rate_pkts_sec:.1f} pkts/s), Loss: {loss_rate:.2f}%")

            burst_report["burst_tests"].append({
                "burst_packets_total": total_pkts,
                "fwd_packets": fwd_received_count,
                "rev_packets": rev_received_count,
                "duration_s": round(burst_duration, 3),
                "packets_per_sec": round(rate_pkts_sec, 1),
                "loss_pct": loss_rate,
                "sha256_verified_all": True,
            })

        # ----------------------------------------------------------------------
        # Test 4: Node 2 Isolation Check
        # ----------------------------------------------------------------------
        node2_payloads = len(node2.received_data_payloads)
        logger.info(f"[Phase 4] Intermediary Node 2 Isolation Check: received {node2_payloads} packets.")
        assert node2_payloads == 0, f"Node 2 received {node2_payloads} packets meant for other nodes!"
        burst_report["node2_leak_count"] = node2_payloads
        logger.info("✓ Node 2 isolation strictly preserved under maximum burst stress (0 leaked packets)")

        # ----------------------------------------------------------------------
        # Test 5: Boundary Finding - 256 KB JSON Latin1 Bloat Breach
        # ----------------------------------------------------------------------
        raw_256k = os.urandom(256 * 1024)
        json_utf8_len = len(json.dumps({"data": raw_256k.decode("latin1")}).encode("utf-8"))
        burst_report["boundary_findings"] = {
            "finding": "Default websockets max_size is 1 MiB (1,048,576 bytes). Due to json.dumps escaping high-byte latin1 characters into \\u00xx (6 bytes each), 256 KB of binary data expands to >1.06 MB, exceeding max_size and causing connection drop code 1009.",
            "raw_size_bytes": 262144,
            "json_encoded_bytes": json_utf8_len,
            "websockets_max_size": 1048576,
            "exceeds_default_frame": json_utf8_len > 1048576,
            "recommendation": "Use Base64 encoding as specified in PROJECT.md contract ('data_b64') or configure max_size on websockets server and client."
        }

        logger.info("==================================================================")
        logger.info("  BIDIRECTIONAL ZERO-DATA-LOSS CHALLENGE: 100% PASSED             ")
        logger.info("==================================================================")

        return burst_report

    finally:
        await node1.close()
        await node2.close()
        await node3.close()
        await hub.stop()


def main():
    report = asyncio.run(run_burst_stress_tests())
    print("\nFINAL_BURST_REPORT:" + json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
