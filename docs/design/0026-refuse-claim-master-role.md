# 0026: Refuse automatic master role claims (`ClaimMasterRole`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Refuse every `WorkerMessage::ClaimMasterRole` on the master and drop `ProtocolCapabilities::ROLE_ARBITRAGE` from `ProtocolCapabilities::all_supported()`:

- all four `WorkerMessage::ClaimMasterRole` arms in `crates/master/src/server.rs` call `reject_master_role_claim`, which returns `MasterMessage::RoleHandoverAck { accepted: false, reason: Some("Automatic master failover is not supported; use manual role switching".into()), task_queue_snapshot: Vec::new(), connected_workers: Vec::new() }`;
- `ProtocolCapabilities::all_supported()` in `crates/core/src/protocol.rs` excludes `ProtocolCapabilities::ROLE_ARBITRAGE` (`1 << 7`);
- coordinator role switching remains manual (`switch_role.sh`, `windows/switch_role.cmd`), while automatic coordinator election stays **Design only** in [0002](0002-mobile-coordination.md).

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Open-cluster role claim (`cluster_secret.is_none()`) | When a master ran without a cluster secret (for example a local loopback master or `--insecure`), `handle_connection` and `handle_multiplexed_connection` checked only `ticket.authority_score >= ELECTION_THRESHOLD_SCORE` (`2500`) without verifying any cryptographic signature, answered `RoleHandoverAck { accepted: true, task_queue_snapshot, connected_workers }`, sent the entire queued task snapshot (`TaskSpec` command arguments, shell scripts, environment variables, and stdin bytes) and connected worker capabilities to any peer that could reach the grid port, and returned from the connection handler. | `handle_connection`, `handle_multiplexed_connection` (`crates/master/src/server.rs`) |
| Authenticated role claim (`cluster_secret.is_some()`) | When a cluster secret was configured, the master synthesised an incumbent ticket with fixed score `3500` and accepted any validly signed `MasterLeaseTicket` with `authority_score > 3500`, returning the task queue snapshot and closing the worker session, even though no worker or CLI code ever constructed `WorkerMessage::ClaimMasterRole` or took over the master listener. | `handle_connection`, `handle_multiplexed_connection` (`crates/master/src/server.rs`) |
| Capability bit | `ProtocolCapabilities::all_supported()` advertised `ROLE_ARBITRAGE` (`1 << 7`) during worker registration even though automatic coordinator election had no production caller. | `ProtocolCapabilities::all_supported` (`crates/core/src/protocol.rs`) |

## Goals

- **G1** Prevent any peer (with or without a cluster secret) from extracting the master's queued task snapshot or terminating a worker session via `WorkerMessage::ClaimMasterRole`.
- **G2** Keep wire-format compatibility with protocol v2 peers by retaining the `WorkerMessage::ClaimMasterRole`, `MasterMessage::RoleHandoverAck`, and `ProtocolCapabilities::ROLE_ARBITRAGE` definitions while refusing every claim and masking bit 7 out of `ProtocolCapabilities::all_supported()`.
- **G3** Align `README.md`, `PROJECT.md`, `ANDROID_ARCHITECTURE.md`, and [0002](0002-mobile-coordination.md) with decision D1 (`FEAT-6`).

## Non-goals

- Removing the `MasterLeaseTicket` or authority-scoring library helpers in `crates/core/src/arbitrage.rs` (kept as **Design only** library code with unit tests).
- Changing the wire encoding of `WorkerMessage::ClaimMasterRole` or `MasterMessage::RoleHandoverAck`.

## Design

### D1 Refuse every `ClaimMasterRole` in `crates/master/src/server.rs`

Per owner decision D1 (`FEAT-6`), automatic master failover is not enabled; role switching between machines is performed explicitly by the operator via `switch_role.sh` and `windows/switch_role.cmd`.

A single helper `reject_master_role_claim(ticket: &MasterLeaseTicket) -> MasterMessage` in `crates/master/src/server.rs` logs a warning with `claimant_master_id` and `authority_score` and returns:

```rust
MasterMessage::RoleHandoverAck {
    accepted: false,
    reason: Some("Automatic master failover is not supported; use manual role switching".into()),
    task_queue_snapshot: Vec::new(),
    connected_workers: Vec::new(),
}
```

All four `WorkerMessage::ClaimMasterRole` match arms in `crates/master/src/server.rs` call `reject_master_role_claim`:
1. `handle_connection` handshake arm: sends the refusal `RoleHandoverAck` and returns `Ok(())` without registering a worker.
2. `handle_connection` active-session arm: sends the refusal `RoleHandoverAck` on `outbound_tx` and keeps the registered worker session alive (`WorkerStatus::Connected`).
3. `handle_multiplexed_connection` handshake arm: sends the refusal `RoleHandoverAck` on `ctrl_transport` via `send_refusal_and_drain` (which finishes the control send stream via `AsyncWriteExt::shutdown` and waits up to `1 s` for the peer to reach EOF or close the QUIC connection before dropping `p2p_conn`, shared with the three `RegisterAck { accepted: false, .. }` handshake refusal arms so iroh does not abort the connection with `ApplicationClosed(0)` before the refusal frame is delivered) and returns `Ok(())` without registering a worker.
4. `handle_multiplexed_connection` active-session control arm: sends the refusal `RoleHandoverAck` on `ctrl_sender` and keeps the multiplexed worker session alive (`WorkerStatus::Connected`), while the data stream catch-all arm in `handle_multiplexed_connection` logs and ignores unexpected control frames on the data lane.

### D2 Stop advertising `ROLE_ARBITRAGE` in `ProtocolCapabilities::all_supported()`

`ProtocolCapabilities::ROLE_ARBITRAGE` (`Self(1 << 7)`) remains defined in `crates/core/src/protocol.rs` so existing bitflag parsers and tests continue to compile, while `ProtocolCapabilities::all_supported()` (`crates/core/src/protocol.rs:73-84`) omits `ProtocolCapabilities::ROLE_ARBITRAGE`. During registration, `worker_features & ProtocolCapabilities::all_supported()` masks out `ROLE_ARBITRAGE`.

## Compatibility

- Wire format is unchanged (`PROTOCOL_VERSION_CURRENT = 2`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 2`).
- Mixed-version clusters remain compatible: workers never send `ClaimMasterRole` in production, and if an older worker advertises `ROLE_ARBITRAGE` in `Register.features`, the upgraded master masks out bit 7 in `RegisterAck.features`.
- **Upgrade order:** only the master binary needs to be upgraded to enforce the refusal (`FEAT-6`). Older masters that have not been upgraded continue to evaluate and accept `ClaimMasterRole` frames under the pre-0026 logic.

## Security

- Closes the task-queue disclosure and session step-down vector on both open (`cluster_secret: None`) and authenticated (`cluster_secret: Some`) masters: `RoleHandoverAck` never populates `task_queue_snapshot` or `connected_workers`, and an active worker session is never stepped down or unregistered by a `ClaimMasterRole` frame.

## Test plan

- **Unit (`crates/core/src/protocol.rs`):** `test_all_supported_does_not_advertise_role_arbitrage` verifies `!ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::ROLE_ARBITRAGE)`.
- **Unit (`crates/master/src/server.rs`):** `test_claim_master_role_is_refused_with_and_without_secret_and_sends_no_tasks` verifies, both with `cluster_secret = None` and `cluster_secret = Some`:
  1. TCP handshake `ClaimMasterRole` before and after queuing a task is refused with an empty `task_queue_snapshot`, returns `Ok(())`, and leaves `WorkerRegistry` empty;
  2. multiplexed P2P handshake `ClaimMasterRole` as the first control message after queuing a task is refused with an empty `task_queue_snapshot`, returns `Ok(())`, and leaves `WorkerRegistry` empty;
  3. active TCP and active multiplexed worker sessions refuse `ClaimMasterRole` with an empty `task_queue_snapshot`, keep the worker `Connected` in `WorkerRegistry`, and complete a subsequent `Heartbeat` / `HeartbeatAck` round-trip.
- **Unit (`crates/master/src/server.rs`):** `test_p2p_handshake_refusals_delivered_over_real_iroh` verifies over real loopback `iroh` endpoints (with relays disabled, both with `cluster_secret = None` and `cluster_secret = Some`) that `RoleHandoverAck { accepted: false, .. }` and `RegisterAck { accepted: false, .. }` refusals on the multiplexed handshake path are reliably delivered before the QUIC connection closes (`BUG-17`).
- **Integration (`crates/android_bridge`):** `mobile_coordination_test`, `m5a_challenger_1_stress`, and `m5a_challenger_2_stress`.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p rusty_grid_core --lib`, `cargo test -p rusty_grid_master --lib`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Stop advertising `ROLE_ARBITRAGE` in `all_supported()` | `crates/core/src/protocol.rs` | `!ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::ROLE_ARBITRAGE)` and all capability negotiation tests pass | Implemented |
| W2 | Refuse every `ClaimMasterRole` across TCP and multiplexed handlers | `crates/master/src/server.rs` | All four `ClaimMasterRole` arms call `reject_master_role_claim`, return `accepted: false` with empty snapshots, and keep active sessions registered and responsive to heartbeats | Implemented |
| W3 | Update design and architecture documentation | `docs/design/0002-mobile-coordination.md`, `docs/design/0026-refuse-claim-master-role.md`, `docs/README.md`, `README.md`, `PROJECT.md`, `ANDROID_ARCHITECTURE.md` | W4 of 0002 points to 0026 with status `Design only`, and reference docs match the code | Implemented |

## Status log

- 2026-11-04: Implemented decision D1 (`FEAT-6`) in `crates/core/src/protocol.rs` and `crates/master/src/server.rs`, added handshake and active-session refusal tests across TCP and multiplexed connections, and documented the change in design record 0026.
