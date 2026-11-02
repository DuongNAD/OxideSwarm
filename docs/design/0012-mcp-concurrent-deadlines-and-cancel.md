# 0012: Concurrent MCP server with client deadlines, cancellation, and progress

Status: Implemented
Created: 2026-11-02    Updated: 2026-11-02

## Summary

Refactor `oxideswarm mcp` (`run_stdio_server`) from a single-threaded sequential read-execute-write loop into a concurrent actor pipeline:

- run a dedicated reader loop, a dedicated stdout writer task, and one spawned task per `tools/call` managed in a `tokio::task::JoinSet`;
- bound client wait calls (`submit {wait: true}` and `task_result {wait: true}`) by `wait_secs` (defaulting to `timeout_secs + 5` on `submit` so worker watchdog timeouts surface before the client deadline) and bound single-frame master RPCs by `DEFAULT_MASTER_RPC_TIMEOUT` (`2 s`) without holding the pooled transport lock across network I/O;
- expose the `cancel` tool (`status`, `workers`, `submit`, `task_result`, `cancel`, `doctor` = 6 tools) and handle `notifications/cancelled` across both synchronous (`wait: true`) and asynchronous (`wait: false`) submissions;
- warn when no connected worker matches a submitted task's placement or resource constraints, leave omitted `max_retries` as `None` (cluster default), and emit `notifications/progress` frames when a client supplies a `progressToken`.

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Stdio Server Loop | `run_stdio_server` awaited `call_tool` directly inside its `stdin` line-reading loop. While `submit {wait: true}` or `task_result {wait: true}` waited on a task, the server could not read or respond to `ping`, `status`, or `notifications/cancelled`. | `crates/cli/src/mcp.rs` |
| Client Deadlines | `submit {wait: true}` waited indefinitely on `recv_msg` with no client-side `wait_secs` deadline; `McpState::request` held its `Mutex` across unbounded network I/O. | `crates/cli/src/mcp.rs` |
| Task Cancellation | No `cancel` MCP tool existed and `notifications/cancelled` was ignored. | `crates/cli/src/mcp.rs` |
| Default `max_retries` | `tool_submit` defaulted `max_retries` to `0` (`with_max_retries(0)`), overriding the master queue's default retry policy on worker disconnect. | `crates/cli/src/mcp.rs` |
| Unmatched Placement | `tool_submit` gave no indication when no connected worker matched the task's placement or resource constraints. | `crates/cli/src/mcp.rs` |

## Goals

- **G1** Answer concurrent requests (`ping`, `status`, `workers`, `cancel`, `notifications/cancelled`) immediately while one or more `submit` or `task_result` calls wait on the master.
- **G2** Enforce a client-side `wait_secs` deadline on `submit` and `task_result`, returning `"timed_out": true` and `task_id` when the wait expires, and bound all single-frame master RPCs against an unresponsive master.
- **G3** Support task cancellation both explicitly via the `cancel` tool and reactively via `notifications/cancelled` (and bounded stdio EOF shutdown).
- **G4** Preserve `TaskRequirements::max_retries = None` when `max_retries` is omitted, warn when no connected worker matches placement/resource constraints, and emit `notifications/progress` when `progressToken` is provided.

## Non-goals

- Adding HTTP/SSE transport to `oxideswarm mcp` (stdio remains the only transport).
- Estimating or adding unbound cluster queue wait time into the default `wait_secs` calculation.

## Design

### D1 Reader loop, writer task, and `JoinSet` per-call tasks

`run_stdio_server` separates protocol I/O from tool execution:
1. A dedicated writer task owns `tokio::io::stdout()` and writes newline-terminated JSON-RPC frames from an `mpsc::UnboundedReceiver<String>`.
2. The main loop uses `tokio::select!` to read incoming lines from `stdin` while continuously reaping completed tool tasks from a `tokio::task::JoinSet<()>`.
3. Lightweight methods (`initialize`, `notifications/initialized`, `ping`, `tools/list`, `notifications/cancelled`) execute inline in the reader loop.
4. Each `tools/call` spawns a task in `call_tasks` (`JoinSet`). Request IDs in flight are tracked in `in_flight: Arc<Mutex<HashMap<String, InFlightCall>>>`. A duplicate in-flight request ID is rejected immediately with `INVALID_REQUEST` (`-32600`) without replacing or dropping the existing call's cancellation sender. Each `ToolCallContext` retains its `watch::Sender<bool>` so the cancellation channel never closes prematurely (including on `tools/call` frames without an `id`), and `tokio::select!` guards `cancel_rx.changed()` against closed senders.

### D2 Client wait deadline (`wait_secs`), RPC timeouts, and `timed_out: true`

- `submit` accepts `wait_secs` (integer seconds). When omitted, `wait_secs` defaults to `timeout_secs + 5` (`DEFAULT_WAIT_MARGIN_SECS = 5`). The 5-second margin ensures that a task that starts immediately and hits its worker execution watchdog (`timeout_secs`, `exit_code: 124`) finishes and reports its failure before the client wait deadline fires. Queue wait time before a worker picks up the task is not separately covered; if a task stays queued past `wait_secs`, the client deadline fires first.
- `--master auto` discovery (`resolve_master_addr` across UDP, HTTP, and rendezvous) runs before starting the per-RPC timeout or `wait_secs` deadline and caches the resolved `host:port` in `McpState` (invalidating the cache on connection failure).
- `deadline = Instant::now() + Duration::from_secs(wait_secs)` is started after target resolution. On `submit {wait: true}`, connecting and sending `SubmitTask` use a minimum `1 s` RPC bound so `wait_secs: 0` still submits the task and immediately reports its state (`timed_out: true` if not yet complete).
- When `deadline` expires during `submit {wait: true}`, the dedicated wait connection is dropped (leaving the task queued or running on the master), `GetTaskStatus` is queried with a `500 ms` timeout (falling back to `"status": "Unknown", "state": "unknown"` if the status query fails or times out), and `submit` returns `{"task_id", "status", "state", "timed_out": true}`.
- `task_result` accepts `wait_secs` (or `timeout_secs`, default `60`) and bounds `connect_dedicated_until`, `send_msg`, `recv_msg`, and polling sleep by `deadline`.
- `McpState::request_with_timeout` takes the pooled `MessageTransport` out of the `Mutex` before performing network I/O and restores it only on success, so a slow or silent master never blocks other concurrent tool calls. Frame reads and writes (`send_msg_with_idle_timeout` and `recv_msg_with_idle_timeout`) bound idle time between progress chunks (`2 s`) rather than total frame transfer duration so large responses (up to `64 MB`) over slow links succeed as long as bytes continue arriving.

### D3 `cancel` tool, `notifications/cancelled`, and stdio EOF shutdown

- The `cancel` tool takes `{"task_id": "<uuid>"}` and sends `ClientMessage::CancelTask { task_id }` to the master, returning `{"task_id", "cancelled"}`.
- `tool_submit` constructs the `Task` and records `Some(task.id)` in `ctx.submitted_task_id` before any network `.await`, checks `ctx.is_cancelled()` before and after target resolution and the pre-flight check, and in the `wait: false` path checks `ctx.is_cancelled()` again immediately after `TaskSubmitted` — sending `CancelTask` if a cancellation notification arrived while `SubmitTask` was in flight.
- When `notifications/cancelled` arrives for an in-flight request ID, the server marks `client_cancelled = true`, signals `cancel_tx.send(true)`, spawns a `CancelTask` request if `submitted_task_id` is already known, and suppresses the JSON-RPC response for the cancelled request ID.
- On `stdin` EOF, `run_stdio_server` gives in-flight calls a `2 s` (`2000 ms`) grace period to finish naturally (preserving piped `doctor` and short `submit {wait: true}` calls up to `2 s`), then signals `cancel_tx.send(true)` on any remaining active calls (which cancels any task still waited on by `submit {wait: true}`), waits up to `500 ms` for cleanup (still emitting the reply of any non-aborted call that completes during cleanup), and calls `call_tasks.abort_all()`.

### D4 Unmatched placement warning, default `max_retries`, and `notifications/progress`

- Before submitting a task, `check_matching_worker_warning` queries `ListWorkers` and checks whether any connected worker satisfies the placement constraints and resource requirements (`cpu_cores`, `ram_mb`, `gpu_required`). If none matches, a warning is logged to `stderr` and included in the JSON response under `"warning"`.
- When `max_retries` is omitted from `submit`, `TaskRequirements::max_retries` remains `None` so the master applies its cluster default.
- When `_meta.progressToken` or `progressToken` is supplied, `submit` and `task_result` emit `notifications/progress` frames through the writer channel.

## Compatibility

- All existing MCP tool names and parameters remain supported; `cancel` is added as the 6th tool and `wait_secs` is an optional parameter on `submit` and `task_result`.
- Wire protocol between CLI/MCP and master is unchanged.

## Security

- Authentication and secret resolution in `run_mcp` are unchanged (fail-closed exit code `6` when connecting to a remote master without a secret unless `--insecure`).
- Rejecting duplicate in-flight request IDs and bounding all master RPCs prevents resource exhaustion and CPU spin loops from malformed client frames or unresponsive masters.

## Test plan

- `cargo test -p rusty_grid_cli --lib`
- `cargo test -p rusty_grid_cli --test test_mcp_concurrent_and_deadlines`
- `cargo test -p rusty_grid_cli --test mcp_server_e2e`
- `cargo test -p rusty_grid_cli --test m5c_challenger_1_stress`

## Work items

- `MCP-1` in `docs/BACKLOG.md`.

## Status log

- 2026-11-02: Implemented concurrent MCP stdio server, `wait_secs` deadlines, `cancel` tool, `notifications/cancelled`, unmatched worker warning, default `max_retries = None`, and `notifications/progress` in `crates/cli/src/mcp.rs`; added integration tests in `crates/cli/tests/test_mcp_concurrent_and_deadlines.rs`.
