# 0010: CLI `cancel` and `result` subcommands

Status: Implemented
Created: 2026-11-02    Updated: 2026-11-04

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

`CancelArgs` accepts the positional task UUID `<ID>` plus `--master`, `--json`, and `--insecure` (inheriting `master`, `secret` / `secret_file`, and `insecure` from the `[status]` section of `rusty-grid.toml`). It connects via `connect_master`, exchanges `ClientMessage::CancelTask { task_id }`, prints human or `--json` (`{"task_id", "cancelled"}`) output, and exits with code `0` when `ClientResponse::TaskCancelled { success: true, .. }` is returned (`1` when the task ID is unknown or already in a terminal state, `2` on an invalid UUID, `6` on authentication failure).

### D2 `oxideswarm result <ID>`

`ResultArgs` accepts the positional task UUID `<ID>` plus `--master`, `--wait` (`-w`), `--json`, `--output-dir <DIR>`, and `--insecure`. It sends `ClientMessage::GetTaskResult { task_id }`:

- When the master answers `ClientResponse::TaskCompleted { task_id, result }`, the CLI downloads any content-addressed `result.outputs` via `BlobConnection` and saves any inline `result.artifacts` into `--output-dir` (default `.`), prints the outcome in human or `--json` format (printing `"was cancelled."` when `exit_code == 130` and omitting `Worker:` when `worker_id` is nil), and exits with:
  - `1` (`ExitCode::FAILURE`) if downloading blob outputs or saving inline artifacts failed (also recording `"output_download_error"` or `"artifact_save_error"` in `--json`);
  - `0` on `result.is_success()`;
  - `result.exit_code.clamp(1, 255)` otherwise, except that in `oxideswarm result` a task exit code of `3` is mapped to process exit code `1` so that process exit code `3` is reserved exclusively for a non-terminal task (`--json` always preserves the exact integer `exit_code`). This exit-code `3 -> 1` mapping applies to `oxideswarm result` only (`oxideswarm submit --wait` never reports an in-progress task and passes task exit code `3` through directly; `submit --output` uses content-addressed `result.outputs`, while both `result` and `submit --wait` exit with `1` if saving inline `result.artifacts` fails).
- When the master answers `ClientResponse::TaskStatusInfo` for a non-terminal task (`Submitted`, `Queued`, `Scheduled`, `Running`, or `Retrying`) and `--wait` is set, the CLI sleeps `100 ms` and polls again on the same connection. Without `--wait`, it prints the status and exits with code `3` so callers can unambiguously distinguish a task that is still in progress (`3`) from a task that failed (`1` or its non-zero exit code). When `TaskStatusInfo` is already terminal (`Cancelled` or `Failed` without a `TaskResult`), it prints the status and exits `1` (`0` if `Completed`).

## Compatibility

- Additive CLI subcommands (`cancel` and `result`); existing subcommands and wire messages are unchanged.

## Security

- Both subcommands enforce `client_secret` (`master_requires_secret`) and exit with code `6` before connecting to a non-loopback or auto-discovered master without a cluster secret unless `--insecure` is explicitly set.
- Both content-addressed output downloads (`crate::blobs::download_outputs`) and inline artifact writes (`crate::connect::save_output_artifacts`) validate paths with `crate::blobs::prepare_destination`: a symlinked `--output-dir` root directory is accepted when its target is a directory (such as `/tmp -> private/tmp` on macOS), while symbolic links at any subdirectory below `--output-dir` or at the destination file itself and any `..` / root path traversal are refused, and files are written to a sibling `.part` temporary file before renaming into place. Any failure exits non-zero.

## Test plan

- **Unit:** CLI argument parsing test `test_cancel_and_result_cli_flags_parse` in `crates/cli/src/cli_app.rs`.
- **Integration:** End-to-end tests in `crates/cli/tests/test_cli_cancel_and_result.rs` verifying `cancel` against a running `sleep` task and a queued task (omitting `Worker:` when `worker_id` is nil), `result` (with `--wait`, `--json`, and `--output-dir`), exit code pass-through (`7`), task exit code `3` mapped to process exit code `1` in `result` (with `"exit_code": 3` preserved in `--json`), non-terminal exit code (`3`), inline artifact saving (including through a symlinked `--output-dir` root directory), symbolic-link refusal at the artifact destination, and non-zero exit on blob/inline save errors in both `result` and `submit --wait`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Add `cancel` and `result` subcommands | `crates/cli/src/cli_app.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/blobs.rs`, `README.md`, `CLAUDE.md` | `oxideswarm cancel <ID>` and `oxideswarm result <ID>` implemented, hardened against symlinks, and documented | Implemented |
| W2 | End-to-end cancel and result tests | `crates/cli/tests/test_cli_cancel_and_result.rs` | Running `sleep` task is cancelled, finished result is retrieved, and symlink/failure/exit-code cases are verified | Implemented |

## Status log

- 2026-11-02: Implemented `oxideswarm cancel <ID>` and `oxideswarm result <ID>`, hardened `save_output_artifacts` with `prepare_destination` and atomic temp-file writes, added `artifact_save_error` and exit code `3` for non-terminal tasks, and verified in `crates/cli/tests/test_cli_cancel_and_result.rs`.
- 2026-11-02: Mapped task exit code `3` to process exit code `1` in `oxideswarm result` (preserving `"exit_code": 3` in `--json`) and accepted a symlinked `--output-dir` root directory in `prepare_destination` while still refusing symlinks below `--output-dir`.
- 2026-11-04: Added tests in `crates/cli/tests/test_cli_cancel_and_result.rs` killing the nil-`worker_id` `Worker:` mutant (`F30-a`) and the `submit --wait` `artifact_save_error` exit-code mutant (`F30-b`), and updated `docs/design/0010-cli-cancel-and-result.md` (`F30-c`, `F30-d`).
