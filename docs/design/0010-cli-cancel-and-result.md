# 0010: CLI `cancel` and `result` subcommands

Status: Implemented
Created: 2026-11-02    Updated: 2026-11-02

## Summary

Expose task cancellation and asynchronous result retrieval directly in the `oxideswarm` / `rusty-grid` CLI:

- `oxideswarm cancel <ID> [--master <ADDR>] [--json] [--insecure]` sends `ClientMessage::CancelTask` and reports whether the task was newly cancelled.
- `oxideswarm result <ID> [--master <ADDR>] [--wait] [--json] [--output-dir <DIR>] [--insecure]` polls or queries `ClientMessage::GetTaskResult`, prints the task's exit code, stdout, stderr, and error, and downloads any blob or inline output files into `--output-dir`.

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Asynchronous `submit` | Submitting a task without `--wait` prints the assigned task UUID and exits immediately, but the CLI had no subcommand to retrieve `TaskResult` (`stdout`, `stderr`, exit code, or output files) afterwards (`status --task-id <ID>` returns only `TaskStatusInfo`). | `crates/cli/src/cli_app.rs` |
| Task cancellation | `ClientMessage::CancelTask` and `ClientMessage::GetTaskResult` existed on the wire protocol and in the master coordinator, but only `oxideswarm doctor` used `GetTaskResult` from the CLI crate. | `crates/core/src/protocol.rs`, `crates/master/src/server.rs` |

## Goals

- **G1** Allow users and scripts to cancel a queued or running task by UUID from the CLI (`oxideswarm cancel <ID>`).
- **G2** Allow users and scripts to fetch the `TaskResult` and download output files of a task submitted without `--wait` (`oxideswarm result <ID> [--wait] [--json] [--output-dir <DIR>]`).

## Non-goals

- Changing the wire format of `ClientMessage::CancelTask`, `ClientMessage::GetTaskResult`, `ClientResponse::TaskCancelled`, or `ClientResponse::TaskCompleted`.

## Design

### D1 `oxideswarm cancel <ID>`

`CancelArgs` accepts the positional task UUID `<ID>` plus `--master`, `--json`, and `--insecure` (inheriting `master`, `secret` / `secret_file`, and `insecure` from the `[status]` section of `rusty-grid.toml`). It connects via `connect_master`, exchanges `ClientMessage::CancelTask { task_id }`, and exits with code `0` when `ClientResponse::TaskCancelled { success: true, .. }` is returned (`1` when the task ID is unknown or already in a terminal state, `6` on authentication failure).

### D2 `oxideswarm result <ID>`

`ResultArgs` accepts the positional task UUID `<ID>` plus `--master`, `--wait` (`-w`), `--json`, `--output-dir <DIR>`, and `--insecure`. It sends `ClientMessage::GetTaskResult { task_id }`:

- When the master answers `ClientResponse::TaskCompleted { task_id, result }`, the CLI downloads any content-addressed `result.outputs` via `BlobConnection` and saves any inline `result.artifacts` into `--output-dir` (default `.`), prints the outcome in human or `--json` format, and exits with `0` on `result.is_success()` or the clamped non-zero exit code otherwise.
- When the master answers `ClientResponse::TaskStatusInfo` for a non-terminal task (`Queued` or `Running`) and `--wait` is set, the CLI sleeps 100 ms and polls again on the same connection. Without `--wait`, or when `TaskStatusInfo` is already terminal (`Cancelled` or `Failed` without a worker execution record), it prints the status and exits immediately.

## Compatibility

- Additive CLI subcommands (`cancel` and `result`); existing subcommands and wire messages are unchanged.

## Security

- Both subcommands enforce `client_secret` (`master_requires_secret`) and exit with code `6` before connecting to a non-loopback or auto-discovered master without a cluster secret unless `--insecure` is explicitly set.
- Output downloads through `result --output-dir` use `crate::blobs::download_outputs`, which refuses symbolic links and path escapes.

## Test plan

- **Unit:** CLI argument parsing test `test_cancel_and_result_cli_flags_parse` in `crates/cli/src/cli_app.rs`.
- **Integration:** End-to-end tests in `crates/cli/tests/test_cli_cancel_and_result.rs` verifying `cancel` against a running `sleep` task and `result` (with `--wait`, `--json`, and `--output-dir`) against a task submitted without `--wait`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Add `cancel` and `result` subcommands | `crates/cli/src/cli_app.rs`, `README.md` | `oxideswarm cancel <ID>` and `oxideswarm result <ID>` implemented and documented | Implemented |
| W2 | End-to-end cancel and result tests | `crates/cli/tests/test_cli_cancel_and_result.rs` | Running `sleep` task is cancelled and finished result is retrieved | Implemented |
