# 0018: Unambiguous JSON routing for `InboundMessage::Worker(BlobRequest)`

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

- `HumanInboundMessage` (`crates/core/src/protocol.rs`) now orders `Worker(WorkerMessage)` before
  `Client(ClientMessage)`, matching `InboundMessage` and `BinaryInboundMessage`.
- A JSON-encoded `WorkerMessage::BlobRequest { request_id, request }` decoded as `InboundMessage`
  now routes to `InboundMessage::Worker` with `request_id` intact instead of being swallowed by
  `HumanClientMessage::BlobRequest { request }`.
- Turns the `"InboundMessage::Worker(BlobRequest)"` known divergence in
  `crates/core/src/wire_layout_tests.rs` into a plain JSON round trip.

## Context

`InboundMessage` (`crates/core/src/protocol.rs`) multiplexes `WorkerMessage` and `ClientMessage`
frames arriving at the Master. Its JSON mirror `HumanInboundMessage` uses `#[serde(untagged)]` and
previously listed `Client(ClientMessage)` before `Worker(WorkerMessage)`.

Both `HumanWorkerMessage::BlobRequest { request_id, request }` and
`HumanClientMessage::BlobRequest { request }` serialize with `"type": "BlobRequest"`. Because
`HumanClientMessage::BlobRequest` does not have `request_id` and serde ignores unknown fields by
default, trying `Client(ClientMessage)` first caused a worker's JSON `BlobRequest` to match
`HumanClientMessage::BlobRequest`, silently dropping `request_id` and misrouting the message to
`InboundMessage::Client`.

## Goals

- **G1** Route `InboundMessage::Worker(WorkerMessage::BlobRequest { request_id, request })` to
  `InboundMessage::Worker` with `request_id` preserved across all JSON read paths (`0x01`-prefixed
  JSON, raw JSON, and whitespace-prefixed raw JSON).
- **G2** Keep `InboundMessage::Client(ClientMessage::BlobRequest { request })` routing to
  `InboundMessage::Client` across both Bincode and JSON codecs.
- **G3** Remove the `KNOWN DIVERGENCE` exception in `crates/core/src/wire_layout_tests.rs` so
  every canonical `InboundMessage` instance (apart from the bincode-only `UnknownVariant`)
  round-trips through JSON.

## Non-goals

- Changing the JSON field names or `type` tags of `WorkerMessage::BlobRequest` or
  `ClientMessage::BlobRequest`.
- Altering Bincode wire encodings (`crates/core/tests/data/wire_layout.txt`).

## Design

### D1 Variant order in `HumanInboundMessage`

`crates/core/src/protocol.rs`:

- `HumanInboundMessage` lists `Worker(WorkerMessage)` first and `Client(ClientMessage)` second.
- All `WorkerMessage` and `ClientMessage` variants other than `BlobRequest` have disjoint `"type"`
  tags, so their JSON routing does not depend on variant order.
- For `"type": "BlobRequest"`, `HumanWorkerMessage::BlobRequest` requires `request_id: u64` (no
  `#[serde(default)]`) in addition to `request: BlobRequest`:
  - A worker's `BlobRequest` carries `request_id: u64`, so `Worker(WorkerMessage)` succeeds on the
    first branch and preserves `request_id`.
  - A client's `BlobRequest` omits `request_id`, so `Worker(WorkerMessage)` fails with a missing
    `request_id` field and `#[serde(untagged)]` falls through to `Client(ClientMessage)`, which
    succeeds.

### D2 Wire layout test update

`crates/core/src/wire_layout_tests.rs`:

- Removes `JsonOutcome::Diverges` and the `"InboundMessage::Worker(BlobRequest)"` arm in
  `json_outcome`, so `inbound_message_routes_bare_first_frames_and_wraps_every_message` asserts a
  full JSON round trip for `InboundMessage::Worker(WorkerMessage::BlobRequest { .. })`.

## Compatibility

- **Bincode:** Unchanged. `BinaryInboundMessage` already listed `Worker` before `Client`, and
  `crates/core/tests/data/wire_layout.txt` is identical.
- **JSON:** Wire frames produced by workers and clients are byte-for-byte unchanged. Decoders now
  classify a JSON `BlobRequest` with `request_id` as `InboundMessage::Worker` and one without
  `request_id` as `InboundMessage::Client`.

## Security

- No new inputs or attack surface. Untrusted frames remain bounded by `MAX_FRAME_SIZE` and
  authenticated by the cluster secret before `InboundMessage` decoding on the Master.

## Test plan

- **Unit:** `cargo test -p rusty_grid_core --lib --locked`
  (`wire_layout_tests::canonical_instances_round_trip_through_both_codecs`,
  `wire_layout_tests::inbound_message_routes_bare_first_frames_and_wraps_every_message`,
  `wire_layout_tests::bincode_encoding_matches_the_recorded_wire_layout`).
- **Gates:** `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --locked -- -D warnings`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Order `Worker` before `Client` in `HumanInboundMessage` | `crates/core/src/protocol.rs` | Worker and client JSON `BlobRequest` both route to their respective `InboundMessage` arm | Implemented |
| W2 | Remove `KNOWN DIVERGENCE` in wire layout test | `crates/core/src/wire_layout_tests.rs` | `inbound_message_routes_bare_first_frames_and_wraps_every_message` passes with plain round trip | Implemented |

## Status log

- 2026-11-04: Implemented `BUG-14` on `mac/bug-14-worker-json-blob-request`.
