# 0092: Agent Mesh hub waits for reply room before it handles a request

Status: Implemented (pull request 303)
Created: 2026-11-17    Updated: 2026-11-17

## Summary

The Agent Mesh hub answers every request on the sender's own outbound queue (an ACK, a NACK or a
response). When a sender sent a burst of requests to a node that had just gone away, the hub's read
loop queued one NACK per request faster than its write pump drained them, the queue overflowed, and
`Outbox::push` disconnected the sender as a slow consumer although it read its replies. The hub now
waits until the sender's own queue has room for the replies before it handles the next request frame,
so a burst slows the sender down instead of disconnecting it.

## Context

- `crates/agent_mesh/src/hub.rs`, `handle_socket`: each connection has a bounded outbound queue
  (`max_queued_messages`, default `DEFAULT_MAX_QUEUED_MESSAGES`). A push into a full queue kicks the
  connection as a slow consumer, which protects the hub's memory from a peer that does not read.
- TEST-27: `test_slow_consumer_is_disconnected_instead_of_buffering_forever` (`crates/agent_mesh/tests/mesh_auth.rs`)
  failed on `macos-latest` on 2026-11-15 and 2026-11-17. The 2026-11-17 log shows the healthy node
  answering in 21 ms and the flooder's own `send_command` failing with "channel closed": after the
  victim was removed, the NACKs for the flooder's queued 100 KiB messages overflowed the flooder's
  queue of 8, and the hub kicked the flooder.

## Goals

- G1. A peer that reads its replies is never disconnected because of replies to its own requests.
- G2. A peer that does not read is still dropped, and the hub's memory per connection stays bounded.

## Design

### D1 Reply headroom

Before `handle_text` runs for a text frame, the connection loop waits for `REPLY_HEADROOM` (2) free
slots in the connection's own outbound queue (`reserve_many`, capped at the queue size), in a `select`
with the existing kick signal, a closed writer, and the idle timeout. While it waits, the hub does not
read the next frame, so TCP and WebSocket flow control push back on the sender.

- A sender that reads its replies is slowed to the speed of its own reads.
- A sender that does not read is dropped by the idle timeout ("not draining its replies"), or by the
  existing kick when other peers' messages to it overflow its queue.

The wait is per connection; other connections are not affected.

## Compatibility

No wire, CLI or configuration change. A client that sent bursts and was disconnected now stays
connected and is slowed down. The Python node (`scripts/agent_node.py`) and the cross-interop tests
are unchanged and pass.

## Security

The bound on queued messages and bytes per connection is unchanged; a peer cannot grow the hub's
memory by not reading, because the hub stops reading from it.

## Test plan

- `test_burst_of_nacks_does_not_disconnect_a_healthy_sender` (`crates/agent_mesh/tests/mesh_auth.rs`):
  60 messages to an unknown node id with a queue of 8; it failed on every run of the old hub and passes
  with this change.
- The TEST-27 test: 0 of 100 failures under CPU load and 0 of 50 plain after the change; `cargo test -p
  agent_mesh` and `tests/test_agent_mesh_rust_hub_cross_interop.py` pass.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TEST-27 | Find why the `mesh_auth` slow-consumer test times out on macOS | `crates/agent_mesh/src/hub.rs`, `crates/agent_mesh/tests/mesh_auth.rs` | The regression test passes in CI and the TEST-27 test passes its loops | Implemented (pull request 303) |

## Status log

- 2026-11-17: Written by the coordinator for pull request 303 (cause and fix by a subagent of the
  coordinator session); number claimed on the hub with `agent_bridge.py claim`.
