#!/usr/bin/env python3
"""
Python agent node: verified, reassembled MESH-2 data delivery (design 0107).
File: tests/test_agent_mesh_python_node_chunks.py

Checks that `scripts/agent_node.py` answers DataPayload envelopes the way the Rust consumer does:
1. A multi-chunk transfer (the Rust sender's format: per-chunk SHA-256 plus metadata
   total_sha256) gets DeliveryAck(chunk_received) for every chunk but the last, and one
   DeliveryAck(data_received_verified) once the whole payload matched its SHA-256; the
   reassembled payload is what the node keeps.
2. A missing checksum, a wrong chunk checksum, a wrong whole-payload checksum and a multi-chunk
   transfer without total_sha256 are refused with DeliveryNack(ERR_PAYLOAD_CORRUPT) and never
   acknowledged as verified; late chunks of a refused transfer are dropped.
3. `ChunkReassembler` is bounded: per-transfer size cap, number of transfers, aggregate bytes,
   and expiry (driven by a fake clock, no sleeps).

Runs without a hub: a fake WebSocket records what the node sends. Standard library plus the
`websockets` import that agent_node.py itself tries (it is optional here).
"""

import asyncio
import hashlib
import json
import os
import sys
import unittest
import uuid

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir, os.path.join(parent_dir, "scripts")]:
    if p not in sys.path:
        sys.path.insert(0, p)

import agent_node  # noqa: E402
from agent_node import ChunkReassembler, OxideAgentNode, PayloadRejected  # noqa: E402


class FakeWs:
    def __init__(self):
        self.sent = []

    async def send(self, text):
        self.sent.append(json.loads(text))


def make_chunks(sender, target, data: bytes, chunk_size: int):
    """DataPayload envelopes in the format of the Rust `new_data_payload_chunks`."""
    corr = str(uuid.uuid4())
    total_sha = hashlib.sha256(data).hexdigest()
    if len(data) <= chunk_size:
        return corr, [{
            "type": "DataPayload", "correlation_id": corr, "from": sender, "to": target,
            "payload_type": "raw_bytes", "sequence_number": 0, "total_chunks": 1,
            "chunk_size_bytes": len(data), "data": data.decode("latin1"),
            "checksum_sha256": total_sha,
        }]
    parts = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    envs = []
    for i, part in enumerate(parts):
        envs.append({
            "type": "DataPayload", "correlation_id": corr, "from": sender, "to": target,
            "payload_type": "raw_bytes", "sequence_number": i, "total_chunks": len(parts),
            "chunk_size_bytes": len(part), "data": part.decode("latin1"),
            "checksum_sha256": hashlib.sha256(part).hexdigest(),
            "metadata": {"total_sha256": total_sha, "total_bytes": str(len(data))},
        })
    return corr, envs


def new_node():
    node = OxideAgentNode("ws://127.0.0.1:1/ws", "py-consumer", "linux")
    node.ws = FakeWs()
    return node


def deliver(node, envs):
    async def run():
        for env in envs:
            await node._dispatch_incoming(env)
    asyncio.run(run())
    return node.ws.sent


class PythonNodeChunkDeliveryTests(unittest.TestCase):
    def test_multi_chunk_transfer_is_reassembled_and_acked_once_as_verified(self):
        node = new_node()
        data = bytes(range(256)) * 9 + b"\x00\xff\r\n"  # every byte value, 2308 bytes
        corr, envs = make_chunks("rust-sender", "py-consumer", data, 1000)
        self.assertEqual(len(envs), 3)
        replies = deliver(node, envs)
        self.assertEqual([r["type"] for r in replies], ["DeliveryAck"] * 3)
        self.assertEqual(
            [r["status"] for r in replies],
            ["chunk_received", "chunk_received", "data_received_verified"],
        )
        self.assertTrue(all(r["correlation_id"] == corr and r["to"] == "rust-sender" for r in replies))
        self.assertEqual(len(node.received_data_payloads), 1)
        kept = node.received_data_payloads[-1]
        self.assertEqual(kept["data"].encode("latin1"), data)
        self.assertEqual(kept["checksum_sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(node._reassembler.total_bytes, 0)

    def test_out_of_order_chunks_still_verify(self):
        node = new_node()
        data = os.urandom(5000)
        _, envs = make_chunks("s", "py-consumer", data, 1024)
        replies = deliver(node, list(reversed(envs)))
        self.assertEqual(replies[-1]["status"], "data_received_verified")
        self.assertEqual([r["status"] for r in replies[:-1]], ["chunk_received"] * 4)
        self.assertEqual(node.received_data_payloads[-1]["data"].encode("latin1"), data)

    def test_single_chunk_payload_is_verified_directly(self):
        node = new_node()
        _, envs = make_chunks("s", "py-consumer", b"hello", 1024)
        replies = deliver(node, envs)
        self.assertEqual(replies, [dict(replies[0], status="data_received_verified")])
        self.assertEqual(len(node.received_data_payloads), 1)

    def test_missing_checksum_is_nacked_not_acked(self):
        node = new_node()
        _, envs = make_chunks("s", "py-consumer", b"no-checksum", 1024)
        del envs[0]["checksum_sha256"]
        replies = deliver(node, envs)
        self.assertEqual(len(replies), 1)
        self.assertEqual(replies[0]["type"], "DeliveryNack")
        self.assertEqual(replies[0]["error_code"], "ERR_PAYLOAD_CORRUPT")
        self.assertIn("Missing SHA-256", replies[0]["reason"])
        self.assertEqual(len(node.received_data_payloads), 0)

    def test_wrong_chunk_checksum_is_nacked_and_late_chunks_dropped(self):
        node = new_node()
        _, envs = make_chunks("s", "py-consumer", b"a" * 30, 10)
        envs[0]["checksum_sha256"] = "0" * 64
        replies = deliver(node, envs)
        self.assertEqual(len(replies), 1, replies)
        self.assertEqual(replies[0]["type"], "DeliveryNack")
        self.assertEqual(node._reassembler.total_bytes, 0)
        self.assertEqual(len(node.received_data_payloads), 0)

    def test_wrong_whole_payload_checksum_is_nacked_on_the_last_chunk(self):
        node = new_node()
        _, envs = make_chunks("s", "py-consumer", b"b" * 30, 10)
        for env in envs:
            env["metadata"]["total_sha256"] = hashlib.sha256(b"other").hexdigest()
        replies = deliver(node, envs)
        self.assertEqual([r["type"] for r in replies], ["DeliveryAck", "DeliveryAck", "DeliveryNack"])
        self.assertEqual([r.get("status") for r in replies[:2]], ["chunk_received"] * 2)
        self.assertIn("Assembled payload SHA-256 mismatch", replies[2]["reason"])
        self.assertEqual(len(node.received_data_payloads), 0)

    def test_multi_chunk_without_total_sha256_is_refused(self):
        node = new_node()
        _, envs = make_chunks("s", "py-consumer", b"c" * 30, 10)
        for env in envs:
            del env["metadata"]
        replies = deliver(node, envs)
        self.assertEqual(len(replies), 1)
        self.assertEqual(replies[0]["type"], "DeliveryNack")
        self.assertIn("total_sha256", replies[0]["reason"])

    def test_non_latin1_data_is_refused(self):
        node = new_node()
        text = "rocket-\U0001F680"
        env = {
            "type": "DataPayload", "correlation_id": "u", "from": "s", "to": "py-consumer",
            "data": text, "checksum_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
        replies = deliver(node, [env])
        self.assertEqual(replies[0]["type"], "DeliveryNack")
        self.assertIn("non-Latin-1", replies[0]["reason"])


class ChunkReassemblerBoundsTests(unittest.TestCase):
    def test_per_transfer_cap(self):
        r = ChunkReassembler(max_assembled_bytes=100, max_total_bytes=1000)
        self.assertIsNone(r.insert("p", "t", 0, 3, "aa", b"x" * 60))
        with self.assertRaises(PayloadRejected):
            r.insert("p", "t", 1, 3, "aa", b"x" * 60)
        self.assertEqual(r.total_bytes, 0)
        self.assertEqual(len(r.inflight), 0)

    def test_pending_transfer_count_and_aggregate_bytes_evict_oldest_first(self):
        r = ChunkReassembler(max_pending=2, max_assembled_bytes=100, max_total_bytes=120)
        r.insert("p", "t1", 0, 2, "aa", b"1" * 50)
        r.insert("p", "t2", 0, 2, "aa", b"2" * 50)
        # A third transfer evicts the oldest (count and bytes).
        r.insert("p", "t3", 0, 2, "aa", b"3" * 50)
        self.assertEqual(list(r.inflight), [("p", "t2"), ("p", "t3")])
        self.assertEqual(r.total_bytes, 100)
        # Completing t3 past the aggregate cap evicts t2, never t3 itself.
        whole = r.insert("p", "t3", 1, 2, "aa", b"4" * 40)
        self.assertEqual(whole, b"3" * 50 + b"4" * 40)
        self.assertEqual(r.total_bytes, 0)
        self.assertEqual(len(r.inflight), 0)

    def test_expiry_uses_the_injected_clock(self):
        now = [1000.0]
        r = ChunkReassembler(ttl_s=60.0, clock=lambda: now[0])
        r.insert("p", "stale", 0, 2, "aa", b"s" * 32)
        now[0] += 60.0  # exactly the TTL: kept
        r.insert("p", "other", 0, 2, "aa", b"o" * 8)
        self.assertIn(("p", "stale"), r.inflight)
        now[0] += 0.5  # past the TTL of "stale" only
        r.insert("p", "fresh", 0, 2, "aa", b"f" * 16)
        self.assertNotIn(("p", "stale"), r.inflight)
        self.assertEqual(r.total_bytes, 24)

    def test_inconsistent_total_or_sha_is_refused(self):
        r = ChunkReassembler()
        r.insert("p", "t", 0, 2, "aa", b"x")
        with self.assertRaises(PayloadRejected):
            r.insert("p", "t", 1, 3, "aa", b"y")
        r.insert("p", "u", 0, 2, "aa", b"x")
        with self.assertRaises(PayloadRejected):
            r.insert("p", "u", 1, 2, "bb", b"y")
        self.assertEqual(r.total_bytes, 0)

    def test_limits_match_the_rust_consumer(self):
        self.assertEqual(agent_node.MAX_ASSEMBLED_BYTES, 64 * 1024 * 1024)
        self.assertEqual(agent_node.MAX_TOTAL_REASSEMBLER_BYTES, 128 * 1024 * 1024)
        self.assertEqual(agent_node.MAX_PENDING_ASSEMBLIES, 64)
        self.assertEqual(agent_node.MAX_CHUNKS_PER_TRANSFER, 1024)
        self.assertEqual(agent_node.CHUNK_ASSEMBLY_TTL_S, 60.0)


async def run_python_node_chunk_tests() -> bool:
    """Entry point for tests/run_mesh_simulation_tests.py."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(PythonNodeChunkDeliveryTests))
    suite.addTests(loader.loadTestsFromTestCase(ChunkReassemblerBoundsTests))
    # The cases call asyncio.run themselves, so run them off the caller's event loop.
    result = await asyncio.get_running_loop().run_in_executor(
        None, lambda: unittest.TextTestRunner(verbosity=2).run(suite)
    )
    return result.wasSuccessful()


if __name__ == "__main__":
    unittest.main()
