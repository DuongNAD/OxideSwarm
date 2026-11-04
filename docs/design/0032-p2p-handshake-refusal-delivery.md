# 0032: P2P handshake refusal delivery (`BUG-17`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

On a multiplexed P2P (`iroh` QUIC) worker connection (`handle_multiplexed_connection` in
`crates/master/src/server.rs`), returning immediately after writing a pre-registration refusal frame
drops the last `iroh::endpoint::Connection` handle and causes `iroh` to send `ApplicationClosed(0)`,
which can abort the peer's control-stream read before the refusal frame arrives. The master now
finishes the control send stream (`AsyncWriteExt::shutdown`, mapped to `SendStream::finish()`) and
waits up to `REFUSAL_DRAIN_TIMEOUT` (`1 s`) in total for the peer to close the QUIC connection
(`conn.closed()`) on all handshake and cluster-authentication refusal paths (`BUG-17`).

## Context

`handle_multiplexed_connection` (`crates/master/src/server.rs`) can refuse a peer on the control
stream before registering a worker session in five ways:

1. Cluster authentication fails (`authenticate_and_read_first_message` sends an auth `REJECT` frame:
   `REJECT_AUTH_REQUIRED`, `REJECT_AUTH_FAILED`, `REJECT_UNSUPPORTED_VERSION`, or `REJECT_NO_SECRET`
   and returns `Err(GridError::AuthFailed(_))`).
2. `WorkerMessage::Register` has `protocol_version < PROTOCOL_VERSION_MIN_COMPATIBLE`
   (`MasterMessage::RegisterAck { accepted: false, .. }`).
3. `WorkerMessage::Register` has `capabilities.cpu_cores == 0`
   (`MasterMessage::RegisterAck { accepted: false, .. }`).
4. The first message is `WorkerMessage::ClaimMasterRole` (`MasterMessage::RoleHandoverAck { accepted: false, .. }`,
   see [0026](0026-refuse-claim-master-role.md)).
5. The first message is any other `WorkerMessage` variant such as `Heartbeat`
   (`MasterMessage::RegisterAck { accepted: false, .. }`).

Previously, `handle_multiplexed_connection` returned immediately after `authenticate_and_read_first_message`
failed or after `ctrl_transport.send_msg(&reject_ack).await` completed. Returning dropped both
`c_conn` (in `handle_multiplexed_connection`) and `conn` (in the P2P accept task), causing `iroh`
to transmit a connection-level `ApplicationClosed(0)` frame that raced ahead of or aborted the
control-stream read on the peer (`connection lost` instead of the refusal or authentication error).
In addition, `send_msg` on the refusal path had no timeout, so a peer that granted no QUIC stream
flow-control credit could hold its connection slot indefinitely.

## Goals

- **G1**: Deliver all four `WorkerMessage` handshake refusals (`ClaimMasterRole`, incompatible
  `protocol_version`, `cpu_cores == 0`, unexpected first `WorkerMessage`) and the cluster-auth
  `REJECT` frames (`REJECT_AUTH_REQUIRED`, `REJECT_NO_SECRET`, `REJECT_AUTH_FAILED`,
  `REJECT_UNSUPPORTED_VERSION`) over real `iroh` QUIC connections, including when the peer
  half-closes its control send stream immediately after writing its first message.
- **G2**: Bound the entire refusal send, stream finish, and connection-close wait by a single named
  constant `REFUSAL_DRAIN_TIMEOUT` (`1 s`), after dropping `handshake_permit` and before calling
  `register_session`.

## Non-goals

- Changing the TCP connection handler (`handle_connection`), where the OS kernel flushes written
  TCP send-buffer bytes on socket close.
- Changing active-session message handling after `register_session`.

## Design

### D1: `send_refusal_and_drain` and `drain_refusal_stream` (`crates/master/src/server.rs`)

- `const REFUSAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);` bounds the refusal drain phase.
- `finish_and_wait_for_peer_close` calls `AsyncWriteExt::shutdown(transport.get_mut()).await`
  (which calls `SendStream::finish()` on `GridStream::P2p(BiStream)`), and when `p2p_conn` is
  `Some(conn)`, awaits `conn.closed().await`. Waiting on `conn.closed()` rather than peer-send-stream
  EOF ensures that a peer which half-closes its own send stream after writing its first frame still
  receives the master's refusal before `conn` is dropped.
- `send_refusal_and_drain` wraps `transport.send_msg(msg).await` and `finish_and_wait_for_peer_close`
  in a single `tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, ...)`. All four `WorkerMessage` handshake
  refusal arms in `handle_multiplexed_connection` call `send_refusal_and_drain`.
- In `handle_multiplexed_connection`, `handshake_permit` is dropped immediately after
  `authenticate_and_read_first_message` returns. When `authenticate_and_read_first_message` returns
  `Err(GridError::AuthFailed(_))` or `Err(GridError::AuthRequired(_))`, `handle_multiplexed_connection`
  calls `drain_refusal_stream` (`tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, finish_and_wait_for_peer_close)`)
  before returning the error.

## Compatibility

- Wire format, CLI flags, and protocol version (`2`) are unchanged.
- An `--insecure` P2P worker connecting to a secret-protected master (or a secret-configured P2P
  worker connecting to an `--insecure` master) now receives the auth `REJECT` frame over QUIC and
  surfaces `GridError::AuthRequired` / `GridError::AuthFailed` (triggering the 30 s
  authentication-failure backoff in the worker's reconnect loop rather than a rapid generic
  reconnect; exit code `6` only applies to the start-up secret check before the worker connects)
  instead of a generic `HandshakeFailed` (`connection lost`).

## Security

- `handshake_permit` is dropped before `drain_refusal_stream` or `send_refusal_and_drain` runs, and
  `register_session` has not been called, so a refused peer holds no pre-auth handshake permit and
  no `WorkerRegistry` entry during the drain wait.
- Each refused P2P connection runs in its own spawned task and holds one `max_connections` slot
  (out of `1024` by default) for at most `REFUSAL_DRAIN_TIMEOUT` (`1 s`) if the peer neither closes
  nor grants flow-control credit, after which the timeout expires and `conn` is dropped.

## Test plan

- **Unit (`crates/master/src/server.rs`):** `test_p2p_handshake_refusals_delivered_over_real_iroh`
  (using `iroh::endpoint::presets::Minimal` with `RelayMode::Disabled`, both with
  `cluster_secret = None` and `cluster_secret = Some`, and both with `half_close = false` and
  `half_close = true` across 8 iterations per combination) verifies:
  1. all four `WorkerMessage` handshake refusal arms (`ClaimMasterRole`, `cpu_cores == 0`,
     `protocol_version == 0`, unexpected `Heartbeat`) and the cluster-auth refusals
     (`REJECT_NO_SECRET` and `REJECT_AUTH_REQUIRED`) arrive on the peer followed by clean stream EOF;
  2. under `max_pending_handshakes(1)`, a second peer connecting while a first peer that failed
     cluster authentication is still parked inside `drain_refusal_stream` is admitted immediately
     (proving `handshake_permit` is dropped before `drain_refusal_stream`), and a refused peer that
     stays silent after reading the refusal is closed by the master after `0.9 s ..= 3.0 s` (bounded
     by a `5 s` test timeout on `closed().await`);
  3. a refused peer configured with a 1-byte QUIC stream receive window (`stream_receive_window(1)`)
     on a `max_connections = 1` master is closed within `3.0 s` (bounded by a `5 s` test timeout on
     `closed().await`) and frees the connection slot for the next peer.
- **Integration (`tests/cluster_auth_e2e.rs`):**
  `test_quic_control_stream_requires_the_cluster_secret` verifies that an unauthenticated QUIC
  `Register` receives `ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)` (`GridError::AuthRequired`).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| BUG-17 | Finish control send stream and wait up to `1 s` for `conn.closed()` on P2P handshake and auth refusals | `crates/master/src/server.rs`, `tests/cluster_auth_e2e.rs`, `docs/design/0026-refuse-claim-master-role.md`, `docs/design/0032-p2p-handshake-refusal-delivery.md`, `docs/README.md` | Normal and half-closing P2P peers receive all 4 handshake refusals and both auth refusals across 10 runs in a row, silent/stalled peers close within `0.9..=1.5 s`, and mutations fail the test | Implemented |

## Status log

- 2026-11-04: Implemented `send_refusal_and_drain` and `drain_refusal_stream` with `REFUSAL_DRAIN_TIMEOUT` (`1 s`) in `crates/master/src/server.rs`, updated `test_p2p_handshake_refusals_delivered_over_real_iroh` and `test_quic_control_stream_requires_the_cluster_secret`, and recorded design 0032.
