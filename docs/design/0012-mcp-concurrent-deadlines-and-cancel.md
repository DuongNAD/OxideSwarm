# 0012 — Concurrent MCP Server with Client Deadlines, Cancellation, and Progress

- **Status:** Implemented (`MCP-1`)
- **Date:** 2026-11-02
- **Scope:** `crates/cli/src/mcp.rs`, `crates/cli/tests/test_mcp_concurrent_and_deadlines.rs`

## Summary

Refactors `oxideswarm mcp` (`run_stdio_server`) from a single-threaded sequential read-execute-write loop into a concurrent actor pipeline (reader task, writer task, and one spawned task per `tools/call`), adds client wait deadlines (`wait_secs`) to `submit` and `task_result`, adds the `cancel` tool and `notifications/cancelled` handling, warns when no connected worker matches a submitted task's placement/resource constraints, preserves the cluster default `max_retries` when `max_retries` is omitted, and emits `notifications/progress` frames when a client supplies a `progressToken`.

## Context & Problem

In `crates/cli/src/mcp.rs`:
1. `run_stdio_server` awaited `call_tool` directly inside its `stdin` line-reading loop. While any `tools/call` (such as `submit` with `wait: true`) was blocked waiting on a grid task, the server could not read or respond to `ping`, `status`, or `notifications/cancelled`.
2. The synchronous wait path in `tool_submit` had no client deadline: `submit {wait: true}` with an unfulfillable placement tag blocked indefinitely.
3. There was no `cancel` MCP tool and `notifications/cancelled` was ignored.
4. `tool_submit` defaulted `max_retries` to `Some(0)` instead of `None` (cluster default), causing any transient worker disconnect to fail the task immediately.
5. `tool_submit` did not warn when no connected worker matched the task's placement constraints.

## Decisions

### D1 — Reader Task, Writer Task, and Per-Call Spawned Tasks
- `run_stdio_server` spawns a dedicated stdout writer task reading serialized JSON-RPC lines from an unbounded channel (`mpsc::UnboundedSender<String>`).
- The reader loop parses incoming JSON-RPC frames from `stdin`. Lightweight methods (`initialize`, `ping`, `tools/list`, `notifications/initialized`, `notifications/cancelled`) are handled immediately in the reader loop without blocking on network I/O.
- Each `tools/call` request spawns its own tokio task (`tokio::spawn`) registered in an in-flight table keyed by the request ID, with a `watch::Receiver<bool>` cancellation signal and a shared `Arc<Mutex<Option<TaskId>>>` slot recording any grid task submitted by that call.

### D2 — Client Wait Deadline (`wait_secs`) and `timed_out: true`
- `submit` accepts an optional `wait_secs` integer parameter (falling back to `timeout_secs`, default `60` seconds) bounding how long `submit {wait: true}` waits for task completion.
- If the deadline expires before the master delivers `ClientResponse::TaskCompleted`, `submit` drops the wait connection (leaving the task queued or running on the master), queries `GetTaskStatus` for the current state, and returns a structured JSON result containing `"task_id"`, `"status"`, `"state"`, and `"timed_out": true`.

### D3 — `cancel` Tool and `notifications/cancelled`
- Adds a `cancel` tool (`{ "task_id": "<uuid>" }`) that sends `ClientMessage::CancelTask` to the master and returns `{"task_id", "cancelled"}`.
- When `notifications/cancelled` arrives with `params.requestId`, the server signals the matching in-flight `tools/call` task. If that call submitted a grid task and was waiting on it, the spawned task sends `ClientMessage::CancelTask` to the master and exits without emitting a JSON-RPC response for the cancelled request ID.

### D4 — Unmatched Placement Warning, Default `max_retries`, and `notifications/progress`
- Before submitting a task, `tool_submit` queries `ClientMessage::ListWorkers` and checks whether at least one connected worker matches the placement constraints and resource requirements. When none matches, it logs a warning to `stderr` and attaches a `"warning"` field to the JSON response.
- When `max_retries` is omitted from `submit` arguments, `TaskRequirements::max_retries` stays `None` so the master applies the cluster default retry limit.
- When the client supplies a progress token (`params._meta.progressToken`, `params.progressToken`, or `arguments.progressToken`), `submit` and `task_result` emit JSON-RPC `notifications/progress` notifications to `stdout` via the writer channel.

## Status Log

- 2026-11-02: Implemented in `crates/cli/src/mcp.rs` and verified by `crates/cli/tests/test_mcp_concurrent_and_deadlines.rs` (`MCP-1`).
