# 0022: Head-and-tail worker stream capture and midpoint truncation detection

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Preserve both the startup header and the terminal summary when a task's `stdout` or `stderr` exceeds the worker capture limit (`DEFAULT_MAX_OUTPUT_BYTES = 10 MiB`):

- split `StreamCapture` in `crates/worker/src/runner.rs` into a fixed `head` (`max_bytes / 2`) and a rolling ring buffer `tail` (`max_bytes - max_bytes / 2`);
- insert `\n[rusty_grid: output truncated after exceeding size limit]\n` between `head` and `tail` when the stream overflows `max_bytes`;
- update `stdout_was_truncated` in `crates/master/src/mapreduce.rs` to detect the truncation marker at both the legacy tail position and the midpoint between `head` and `tail`.

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| Worker stream capture | `StreamCapture` kept only the first `max_bytes` (`10 MiB`) of `stdout` and `stderr`, drained and discarded every subsequent byte, and appended `\n[rusty_grid: output truncated after exceeding size limit]\n` at the end. When a build or test task wrote more than 10 MiB, the final compiler error or test summary at the end of the stream was lost. | `crates/worker/src/runner.rs` |
| Map/Reduce truncation detector | `stdout_was_truncated` only checked the last `marker.len() + 8` bytes of `result.stdout`. | `crates/master/src/mapreduce.rs` |

## Goals

- **G1** Keep the first `max_bytes / 2` bytes (`head`) and the last `max_bytes - max_bytes / 2` bytes (`tail`) of each stream while still draining the pipe to EOF so the child process never blocks on a full pipe.
- **G2** Emit `\n[rusty_grid: output truncated after exceeding size limit]\n` between `head` and `tail` whenever at least one byte was dropped (`len > max_bytes`), and emit the exact unmodified stream when `len <= max_bytes`.
- **G3** Ensure `MapReduceEngine` (`stdout_was_truncated`) and MCP (`stream_was_worker_truncated`) reliably detect worker-truncated streams without false positives on short output that prints the marker text.

## Non-goals

- Changing `TaskResult` wire layout or increasing `DEFAULT_MAX_OUTPUT_BYTES` beyond 10 MiB per stream.

## Design

### D1 Head-and-tail `StreamCapture`

`StreamCapture` in `crates/worker/src/runner.rs` partitions `max_bytes` into:

- `head_cap = max_bytes / 2` stored in `head: BytesMut`;
- `tail_cap = max_bytes - head_cap` stored in `tail: VecDeque<u8>`.

Each incoming chunk first fills any remaining room in `head`. Any remaining slice `rest` is pushed into `tail`:

- If `rest.len() >= tail_cap`, `truncated` is set to `true` whenever `tail.len() + rest.len() > tail_cap`, `tail` is cleared, and the last `tail_cap` bytes of `rest` are copied into `tail`.
- Otherwise (`rest.len() < tail_cap`), any overflow `(tail.len() + rest.len()).saturating_sub(tail_cap)` sets `truncated = true` and drains `overflow` oldest bytes from the front of `tail` before appending `rest`.

When the reader reaches EOF (or on timeout flush), `StreamCapture::finish` concatenates `head`, `TRUNCATION_NOTICE` (`b"\n[rusty_grid: output truncated after exceeding size limit]\n"`, only when `truncated` is `true`), and the two contiguous slices of `tail`.

### D2 Midpoint truncation detection in `MapReduceEngine` and MCP

In `crates/master/src/mapreduce.rs`, `stdout_was_truncated` checks:

1. The legacy tail window (`out[out.len().saturating_sub(marker.len() + 8)..]`) so a newer master still recognizes truncation markers appended by older workers.
2. The exact midpoint slice `out[head_cap..head_cap + notice_len]` where `notice_len = marker.len() + 2` and `head_cap = (out.len() - notice_len) / 2`, requiring `s[0] == b'\n'`, `&s[1..1 + marker.len()] == marker`, and `s[1 + marker.len()] == b'\n'`.

In `crates/cli/src/mcp.rs`, `stream_was_worker_truncated` checks the same midpoint slice (`head_cap = (bytes.len() - notice_len) / 2 > 0`) for any configured worker capture cap (`DEFAULT_MAX_OUTPUT_BYTES = 10 MiB` by default, or smaller caps configured through the library), reporting `worker_truncated` (`stdout_worker_truncated`, `stderr_worker_truncated`) alongside `captured_bytes`.

## Compatibility

- **Wire format:** Unchanged. `TaskResult.stdout` and `TaskResult.stderr` remain `task::Bytes` bounded by `DEFAULT_MAX_OUTPUT_BYTES + 59 bytes`.
- **Rolling upgrade order (master first):** An older master only inspects the last `marker.len() + 8` bytes of `result.stdout` and would miss the midpoint marker produced by a new worker when a mapper writes more than 10 MiB. **Upgrade the master before upgrading workers**; a new master accepts both the legacy tail marker from older workers and the midpoint marker from new workers.

## Security

- Child stdout and stderr pipes are still drained to EOF even after `max_bytes` is exceeded, preventing child processes from deadlocking on full OS pipe buffers while bounding worker memory to `max_bytes` per stream.

## Test plan

- **Unit (`crates/worker/src/runner.rs`):** `test_read_bounded_stream_keeps_head_and_tail_when_truncated` and `test_read_bounded_stream_chunked_boundaries_exact_plus_one_and_many_wraps` across caps (`1` to `4096`), lengths (`cap - 1`, `cap`, `cap + 1`, `head_cap + 1 + tail_cap`, `cap * 4 + 37`), and chunk read sizes (`1` to `8192`).
- **Unit (`crates/master/src/mapreduce.rs`):** `test_truncation_marker_matches_what_the_real_worker_appends` against `rusty_grid_worker::read_bounded_stream` with caps `>= 1024` (even and odd), verifying both exact fit and `+1` / multi-wrap overflow.
- **Integration (`tests/mcp_server_e2e.rs`):** `test_mcp_safe_llm_outputs_1mib_binary_and_20mib_stdout` verifying a 20 MiB stdout task preserves both the startup banner and the final summary marker.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Split head-and-tail `StreamCapture` | `crates/worker/src/runner.rs` | `head` + `TRUNCATION_NOTICE` + `tail` implemented and chunked boundary tests pass | Implemented |
| W2 | Midpoint truncation check in Map/Reduce and MCP | `crates/master/src/mapreduce.rs`, `crates/cli/src/mcp.rs` | `stdout_was_truncated` and `stream_was_worker_truncated` detect midpoint notice at `>= 1 KiB` / `10 MiB` caps | Implemented |

## Status log

- 2026-11-04: Implemented and verified in PR #62 (`MCP-2`).
