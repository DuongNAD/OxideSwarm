# 0030: MCP post-reply cancellation, EOF grace window, and P2P endpoint lifecycle

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

1. In `run_stdio_server` (`crates/cli/src/mcp.rs`), `notifications/cancelled` for a recently
   completed `submit` call (bounded to the last 256 request IDs in `completed_submits`) cancels the
   submitted task on the master if and only if the request ID has not been reused and the task is
   still non-terminal.
2. In `run_stdio_server` (`crates/cli/src/mcp.rs`), the stdio EOF grace window increases from `2 s`
   to `4 s` so piped multi-probe `doctor` and burst `submit {wait: true}` invocations on loaded
   hosts complete before remaining waits are cancelled.
3. In `MasterServer::spawn_with_config` (`crates/master/src/server.rs`), the P2P relay readiness
   wait increases from `3 s` to `5 s`, and the P2P accept loop calls `endpoint.close().await` on
   `MasterServer::shutdown`.

## Context

- **MCP pipelined `submit {wait: false}` + `notifications/cancelled`**: In `run_stdio_server`
  (`crates/cli/src/mcp.rs`), each `tools/call` runs in a spawned `call_tasks` future while
  `notifications/cancelled` runs inline in the `stdin` reader loop. When a client pipelines a fast
  `submit {wait: false}` request immediately followed by `notifications/cancelled`, the spawned
  `submit` task can receive `ClientResponse::TaskSubmitted` over loopback TCP and remove its request
  ID from `in_flight` before the reader loop reads the `notifications/cancelled` line. Under
  [0012](0012-mcp-concurrent-deadlines-and-cancel.md), `notifications/cancelled` only inspected
  `in_flight`, leaving the task queued on the master when the `submit {wait: false}` task won the
  race.
- **MCP stdio EOF grace period**: [0012](0012-mcp-concurrent-deadlines-and-cancel.md) gave in-flight
  tool calls a `2 s` grace window after `stdin` EOF before cancelling remaining calls. A piped
  `doctor` call executes 4 probe tasks (CPU, shell, multi-core, GPU/skip) and `mcp_server_e2e`
  pipes 3 concurrent `submit {wait: true}` requests before closing `stdin`; under heavy CPU or
  process-creation load (especially on shared CI VMs and Windows), those short calls can exceed the
  `2 s` window after `stdin` EOF.
- **P2P master relay readiness and shutdown**: `MasterServer::spawn_with_config`
  (`crates/master/src/server.rs`) waited up to `3 s` for `endpoint.online()` before writing the P2P
  ticket file, which occasionally timed out on shared cloud VMs when the initial DNS/TLS/DERP
  handshake to `*.relay.n0.iroh.link` exceeded `3 s`. On `MasterServer::shutdown()`, the P2P accept
  loop exited without calling `endpoint.close().await`, leaving the previous `iroh::Endpoint` open
  when a restarted master immediately bound the same persistent `SecretKey`.

## Goals

- **G1**: Cancel a task when `notifications/cancelled` arrives immediately after `submit {wait: false}`
  has emitted its reply and left `in_flight`, while preventing a reused request ID or a completed
  task from triggering an unintended cancellation.
- **G2**: Give piped one-shot `doctor` and short `submit {wait: true}` calls `4 s` after `stdin`
  EOF to emit their JSON-RPC replies before cancelling long-running waits.
- **G3**: Wait up to `5 s` for DERP relay discovery at P2P master start-up and close the master's
  `iroh::Endpoint` deterministically on `MasterServer::shutdown()`.

## Non-goals

- Changing the MCP JSON-RPC wire schema or tool signatures.
- Changing the `500 ms` post-grace cancellation cleanup bound on `stdin` EOF.

## Design

### D1: Bounded post-reply cancellation (`completed_submits`) with request-ID reuse guard

`run_stdio_server` (`crates/cli/src/mcp.rs`) maintains
`completed_submits: Arc<Mutex<VecDeque<(String, TaskId)>>>` (capped at 256 entries):

1. Whenever a `submit` tool call finishes without having been cancelled, its `(request_id_key, task_id)`
   pair is recorded at the back of `completed_submits` (replacing any prior entry with the same key
   and evicting the oldest entry when `len() >= 256`) before the key is removed from `in_flight`.
2. Whenever any new JSON-RPC request carrying `id` (other than `notifications/cancelled`) arrives on
   `stdin`, `run_stdio_server` removes `request_id_key(id)` from `completed_submits` before
   dispatching the request. Consequently, if a client reuses a request ID for a subsequent call, the
   old `TaskId` mapping is invalidated immediately and cannot be cancelled by a later
   `notifications/cancelled` for that reused ID.
3. When `notifications/cancelled` arrives for `key` and `in_flight` does not contain `key`,
   `run_stdio_server` removes the matching `(key, task_id)` entry from `completed_submits` (so it
   can be consumed at most once) and spawns a background task that queries
   `ClientMessage::GetTaskStatus { task_id }`. Only if the master reports a non-terminal status
   (`!status.is_terminal()`) does the server send `ClientMessage::CancelTask { task_id }`.

### D2: 4-second stdio EOF grace period

When `reader.next_line()` reaches `stdin` EOF in `run_stdio_server` (`crates/cli/src/mcp.rs`),
`grace_deadline` is set to `tokio::time::Instant::now() + Duration::from_secs(4)`. In-flight calls
that finish within `4 s` emit their JSON-RPC responses normally; calls still running after `4 s`
receive `cancel_tx.send(true)` and have up to `500 ms` to send `CancelTask` before
`call_tasks.abort_all()`.

### D3: P2P master relay readiness (`5 s`) and endpoint shutdown (`endpoint.close().await`)

- In `MasterServer::spawn_with_config` (`crates/master/src/server.rs`),
  `tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await` waits up to `5 s` for home
  DERP relay connection and STUN discovery before building the P2P ticket (an offline or air-gapped
  start with `--p2p` therefore waits `5 s` instead of `3 s` before proceeding with local addresses).
- In `MasterServer::run` (`crates/master/src/server.rs`), when `p2p_shutdown.changed()` fires in the
  P2P accept loop (or if `*p2p_shutdown.borrow_and_update()` is already true when the loop starts),
  the loop calls `endpoint.close().await` before breaking so `MasterServer::shutdown()` closes the
  `iroh::Endpoint`.

## Compatibility

- Wire protocol and CLI flags are unchanged.
- Existing persistent P2P master keys (`~/.oxideswarm/master_key.bin`) preserve the same `NodeId`
  and publish a relay-only ticket across restarts; even if iroh selects a different regional
  `*.relay.n0.iroh.link` node on restart, existing workers holding the previous ticket reconnect
  and re-register automatically via the master's `NodeId`.

## Security

- Evicting `request_id_key` from `completed_submits` on any new request with that ID and checking
  `GetTaskStatus` (`!status.is_terminal()`) before sending `CancelTask` prevents accidental
  cancellation of an older task when a client reuses numeric or string request IDs.

## Test plan

- `crates/cli/tests/test_mcp_concurrent_and_deadlines.rs`:
  `test_mcp1_cancelled_async_submit_leaves_no_orphan_tasks_and_default_wait_margin_surfaces_exit_124`
  verifies:
  1. after `id: 300` replies, polling `master.queue().list_tasks()` reaches 0 pending tasks and
     every task from a skipped `200..225` reply is `Cancelled`;
  2. an explicit probe (`id: 299`) that submits `wait: false`, reads the reply, and then sends
     `notifications/cancelled` transitions the task to `Cancelled`;
  3. reusing a request ID (`id: 298`) for `ping` before sending `notifications/cancelled`, followed
     by a positive-control cancellation (`id: 297`) that reaches `Cancelled`, leaves the `id: 298`
     task `Queued`.
- `tests/test_p2p_wan_fallback.rs`: `test_master_persistent_key_yields_relay_ticket` and
  `test_ticket_determinism_and_reconnection` verify that `master.shutdown()` alone closes the P2P
  endpoint (`is_closed()`) and that restarting with the same key preserves `NodeId`, publishes a
  relay-only ticket, and lets existing workers re-register.
- `tests/test_m2_identity_scripts.rs`: `test_worker_auto_reconnect_under_3_seconds` verifies that a
  worker tuned with a `1.2 s` connect timeout (`with_connect_timeout(Duration::from_millis(1200))`)
  and a `100–400 ms` reconnect backoff (`BackoffConfig { initial: 100 ms, max: 400 ms, factor: 1.5, jitter_ratio: 0.1 }`)
  reconnects and re-registers in under `3.0 s` after master restart (with default worker settings,
  reconnect passed `9/10` under load up to `3.02 s`).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TEST-15 | Stabilize MCP pipelined cancel, EOF grace, and P2P master restart tests | `crates/cli/src/mcp.rs`, `crates/master/src/server.rs`, `crates/cli/tests/test_mcp_concurrent_and_deadlines.rs`, `tests/test_p2p_wan_fallback.rs`, `tests/test_m2_identity_scripts.rs`, `crates/core/tests/m1_challenger_1_3d_serialization.rs` | All TEST-15 targets pass repeatedly under load and required mutations fail tests | Implemented |

## Status log

- 2026-11-04: Implemented and verified against review feedback on pull request 83.
