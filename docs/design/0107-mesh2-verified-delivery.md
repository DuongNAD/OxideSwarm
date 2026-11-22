# 0107: Agent Mesh verified data delivery (MESH-2)

Status: Implemented (branch `claude/wave2-mesh2-fixes`)
Created: 2026-11-22    Updated: 2026-11-22

## Summary

`agent-mesh data` reports delivery only when the target node itself confirms that the whole
payload matched its SHA-256 and, with `--inbox-dir`, was written to disk. Multi-chunk transfers are
paced by per-chunk acknowledgements, so a transfer never puts more than two chunks into the hub's
queue for the consumer. The Python node reassembles chunked payloads like the Rust node. The inbox
directory has a byte and a file quota and private permissions, and the hub's table of open
transfers is bounded per sender.

## Context

Commits `ea3b5e0c` and `85a53f71` added MESH-2: `DataPayload` chunking above 1 MiB, SHA-256
verification, `agent-mesh node --inbox-dir`, `agent-mesh data --payload-file` / `--timeout-ms`,
and honest `/api/status` telemetry. A review found these problems:

- `scripts/agent_node.py` (`_handle_data_payload`) did not reassemble chunks. It answered every
  chunk with `DeliveryAck(data_received_verified)`, also when `checksum_sha256` was missing.
  `AgentMeshClient::send_data_payload_chunked_with_ack` (`crates/agent_mesh/src/client.rs`)
  resolved on the first non-hub ACK of the correlation id, so a 3-chunk transfer to a Python node
  printed "Delivery verified" after chunk 0. It also accepted an ACK from any node.
- `persist_payload_atomic` wrote into `--inbox-dir` without a limit, so any peer could fill the
  disk, and the files got the process umask.
- The hub queues at most `DEFAULT_MAX_QUEUED_BYTES` (32 MiB) per connection
  (`crates/agent_mesh/src/hub.rs`, `Outbox::push`) and disconnects a connection whose queue
  overflows. A sender wrote all chunks at once; one chunk of 1 MiB raw bytes is up to 6 MiB of JSON
  (each byte is a Latin-1 character, a control byte is written as `\u00XX`), so a fast 64 MiB
  transfer got a healthy consumer disconnected.
- The hub's `InflightDataTracker` (used to NACK senders whose target disconnects) held 1,024 entries
  for all senders, keyed by (target, correlation id): one flooding node evicted the entries of
  others, and a second sender reusing a correlation id took the first sender's NACK.
- Two tests asserted wall-clock windows (`elapsed < 4 s`; a 120 ms stale window expected "online"
  right after connect and 50 ms after a heartbeat).

## Goals

- G1. "Delivered" means the target verified the SHA-256 of the whole payload and persisted it.
- G2. Python and Rust consumers answer the same envelopes the same way, with the same limits.
- G3. No peer can grow a consumer's disk, or the hub's memory, without bound.
- G4. A legitimate transfer of the largest accepted size cannot get its consumer disconnected.
- G5. Tests assert order and state, not timing.

## Non-goals

- Authenticating payload content end to end: the hub can still read and alter payloads in transit
  for any node that holds the hub token; the SHA-256 detects transport corruption, not a malicious
  hub (`docs/SECURITY.md`).
- Resuming an interrupted transfer, or retransmitting a lost chunk.
- A chunked sender in `scripts/agent_node.py`: its `send_data_payload` still sends one frame and does
  not wait for the receipt.

## Design

### D1 Consumer verification (`crates/agent_mesh/src/delivery.rs`, `scripts/agent_node.py`)

`DataConsumer::accept` (Rust) and `OxideAgentNode._verify_data_payload` (Python) check each
`DataPayload` in this order and answer `DeliveryNack(ERR_PAYLOAD_CORRUPT)` on the first failure:

1. `data` decodes as Latin-1 (one character per byte), or a JSON `payload` is present.
2. `checksum_sha256` is present and matches the chunk's bytes.
3. `sequence_number < total_chunks <= 1024` (a lone frame may carry `0` / `0`).
4. A single chunk is the whole payload (`metadata.total_sha256` is checked when present).
5. A multi-chunk transfer must carry `metadata.total_sha256`; chunks are reassembled by
   (sender, correlation id) in any order; `total_chunks` and `total_sha256` must agree across
   chunks; when the last chunk arrives the SHA-256 of the reassembled payload must equal
   `total_sha256`.

Limits, identical in both: `64 MiB` per transfer, `64` open transfers, `128 MiB` buffered in all
(oldest transfer evicted first), and a transfer that receives no chunk for `60 s` is dropped. A
consumer remembers the last `256` refused transfers and drops their late chunks without an answer.
The Python node handles `DataPayload` frames inline in its read loop, so its answers leave in the
order the chunks arrived.

### D2 Acknowledgements and what the sender accepts

- A verified chunk that does not complete its transfer is answered with
  `DeliveryAck(status = "chunk_received")` (`ACK_STATUS_CHUNK_RECEIVED`). It is flow control, not
  a receipt.
- A complete, verified payload is persisted (D4) and then answered with exactly one
  `DeliveryAck(status = "data_received_verified")` (`ACK_STATUS_DATA_VERIFIED`).
- The sender (`next_transfer_event` in `client.rs`) counts only a `DeliveryAck` whose `from` is the
  target, and a `DeliveryNack` from the target or from `hub` (the hub refuses `hub` as a node id, and
  checks `from` on every frame, so a peer cannot forge it). Envelopes from any other node are logged
  and ignored.
- A consumer sends its chunk ACKs before its receipt on one socket, and the hub forwards a socket's
  frames in order, so the sender requires `total_chunks - 1` chunk ACKs before the receipt. A
  receipt that overtakes them (a consumer that does not reassemble, such as the old Python node) is
  an error, as is a receipt that arrives before every chunk was sent.
- The receipt's `responder` is the target id.
- Pending request/response waits (`send_command`, the raw `pending` map) are never resolved by a
  `chunk_received` or `target_forwarded` ACK, in Rust and in Python.

### D3 Pacing instead of a larger hub queue

The sender keeps at most `DATA_CHUNK_WINDOW = 2` chunks unacknowledged: it sends chunk `k` only
after the chunk ACK for chunk `k - 2`. One transfer, whatever its size, therefore queues at most
`2 * max_chunk_json_bytes(1 MiB) = 2 * (6 MiB + 4 KiB)` for its consumer at the hub, which is less
than half of `DEFAULT_MAX_QUEUED_BYTES` (32 MiB), leaving room for a second transfer and for command
traffic. `hub.rs` checks both this and "one chunk fits in `DEFAULT_MAX_MESSAGE_BYTES`" with
compile-time assertions, so a change to the chunk size, the window or the queue that breaks it
does not build.

Pacing was chosen over a larger queue because the queue would have to hold `6 * 64 MiB` for one
worst-case transfer, times the number of concurrent senders, and the hub would buffer it in memory
for a consumer that may be slow. With the window, a slow consumer slows its sender instead.

`AgentMeshClient::send_data_payload`, which does not wait for a receipt, still sends a payload of
one chunk (up to 1 MiB) at once; a larger payload goes through the paced path and returns after the
target's receipt, because pacing needs the chunk acknowledgements.

`--timeout-ms` (and the `timeout_ms` argument of `send_data_payload_with_ack`) bounds each wait for
the next acknowledgement, not the whole transfer, because the duration of a paced transfer depends
on its size and on the consumer.

### D4 Inbox quota and permissions

- `--inbox-max-mb <MiB>` (default `1024`) and `--inbox-max-files <N>` (default `10000`), both at
  least `1`, set `InboxQuota` (`AgentMeshClient::with_inbox_quota`).
- Before each write, `persist_payload_atomic` measures the regular files directly in the directory
  (links are not followed). If the new file would exceed the file count or the byte total, it
  answers `DeliveryNack(ERR_INBOX_FULL)` with a reason that names the flag, and writes nothing.
  Usage is measured from the directory, so files a downstream reader removes free their share.
- Files are created with mode `0600` on Unix (`OpenOptionsExt::mode`; the rename keeps it). A missing
  inbox directory, and missing parents, are created with mode `0700`. An existing directory keeps
  its permissions.

### D5 Hub table of open transfers

`InflightDataTracker` keys an entry by (target, correlation id, sender) and keeps a queue per
sender. A sender holds at most `256` entries; past that it loses its own oldest. The whole table
holds at most `16,384`; past that the sender with the most entries loses its oldest. A final ACK
(neither `chunk_received` nor `target_forwarded`) or a NACK from the target resolves only the
entry of the sender it is addressed to. `chunk_received` keeps the entry, so a target that
disconnects mid-transfer still produces `DeliveryNack(ERR_NODE_UNAVAILABLE)` for its sender.
`/api/status` `delivered_acks` counts final ACKs only.

### D6 Tests without timing assertions

- `HubState::get_catalog_at(now)` takes the clock, so the stale-window test checks `online` at the
  window's end and `stale` one millisecond past it, with a 30 s window and no sleep.
- `ChunkReassembler::insert_chunk` takes `now: Instant`; Python's `ChunkReassembler` takes a `clock`.
- Waits for asynchronous state poll with a 60 s bound that only stops a hung test. Ordering on the
  hub is forced with a round trip on the same socket (`query_node_list`) instead of a sleep.
- The pacing test asserts that no third chunk arrives while two are unacknowledged; a correct
  sender passes this however slow the runner is.

## Compatibility

- Wire: no new envelope or field. `chunk_received` is a new `DeliveryAck.status` value and
  `ERR_INBOX_FULL` a new `DeliveryNack.error_code`. The grid wire protocol is untouched.
- MESH-2 was not released before this change, so no released consumer or sender speaks the
  earlier form. A consumer built from `ea3b5e0c`/`85a53f71` does not send chunk ACKs: a new sender
  stalls after two chunks and times out. A Python node from before this change acknowledges each
  chunk as verified: a new sender refuses the receipt (D2). Single-chunk payloads (up to 1 MiB)
  work in every combination.
- The hub needs no change to forward `chunk_received`; an older hub counts it in `delivered_acks`
  and resolves its open-transfer entry early (a disconnect after that is reported by the sender's
  timeout instead of a NACK).

## Security

- New untrusted inputs: chunk metadata and `metadata.total_sha256` from any registered peer. All
  buffers are bounded (D1), refused transfers cannot restart reassembly, and the disk is bounded by
  the quota (D4).
- A third node that learns a correlation id cannot confirm or fail someone else's transfer (D2),
  cannot redirect a disconnect NACK, and cannot evict other senders' table entries (D5).
- Inbox files are readable by the node's user only. The SHA-256 is an integrity check against
  corruption, not an authenticity check (see Non-goals).

## Test plan

- Unit (`delivery.rs`): consumer chunk ACKs then whole-payload verification; missing checksum,
  missing `total_sha256`, wrong whole SHA-256 refused and late chunks dropped; quota by count and
  bytes, freed on removal; `0600` files and `0700` created directories; reassembler caps and expiry
  with an explicit clock.
- Unit (`hub.rs`): per-sender bound and sender-keyed resolution of `InflightDataTracker`; telemetry
  with `get_catalog_at`.
- Integration (`crates/agent_mesh/tests/mesh_data_delivery.rs`): forged ACK/NACK from a third node
  ignored; a receipt that does not cover every chunk refused (2 and 3 chunks); the window of 2
  respected by a slow raw consumer; a 24-chunk all-NUL transfer through a hub queue sized for the
  window only; `--inbox-max-files` refusal with `ERR_INBOX_FULL` and file modes through the CLI.
  Disabling the window makes the pacing tests fail.
- Python (`tests/test_agent_mesh_python_node_chunks.py`, Suite 5 of
  `tests/run_mesh_simulation_tests.py`): reassembly in and out of order, ACK sequence, NACK cases,
  reassembler bounds with a fake clock, limits equal to Rust.
- Cross-interop (`tests/test_agent_mesh_rust_hub_cross_interop.py`): the Rust `agent-mesh data`
  sends a 2.5 MiB, 3-chunk file to `scripts/agent_node.py` through the Rust hub and succeeds only
  with the Python node's receipt.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| MESH-2a | Python reassembly and whole-payload verification; Rust sender waits for the receipt that covers every chunk and checks the responder | `scripts/agent_node.py`, `crates/agent_mesh/src/client.rs`, `crates/agent_mesh/src/delivery.rs` | Rust-to-Python 3-chunk interop passes; forged and partial receipts are refused | Implemented |
| MESH-2b | Inbox quota and private permissions | `crates/agent_mesh/src/delivery.rs`, `crates/agent_mesh/src/main.rs` | `ERR_INBOX_FULL` through the CLI; `0600` / `0700` asserted | Implemented |
| MESH-2c | Chunk window and compile-time queue bound | `crates/agent_mesh/src/protocol.rs`, `client.rs`, `hub.rs` | Pacing tests pass and fail without the window | Implemented |
| MESH-2d | Per-sender open-transfer table | `crates/agent_mesh/src/hub.rs` | Tracker unit test passes | Implemented |
| MESH-2e | Timing-free tests | `hub.rs`, `delivery.rs`, `tests/mesh_data_delivery.rs` | No wall-clock window asserted | Implemented |

## Status log

- 2026-11-22: Written with the fixes for the MESH-2 review findings. The number was assigned by the
  coordinator of the session; `docs/README.md` still needs its index line (outside the files this
  change was allowed to touch).
