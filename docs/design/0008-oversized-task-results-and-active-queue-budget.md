# 0008: Oversized task results and active queue byte budget

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-01

## Summary

Keep a single oversized `TaskResult` from tearing down a worker's session, preserve the fields of
that result that still fit in a wire frame, charge `TaskResult::asset_bytes` against the master's
terminal retention budget, and bound the memory held by active tasks in `TaskQueue` with a
configurable 256 MiB cap (`--max-active-mb`, `RUSTY_GRID_MAX_ACTIVE_MB`, `max_active_mb`).

## Context

Behaviour before this change:

- `lane_writer` in `crates/worker/src/client.rs` treated every `ProtocolError::FrameTooLarge` from
  `MessageWriter::send_msg` as a fatal lane error. When one task produced a `TaskResult` larger
  than `MAX_FRAME_SIZE` (64 MiB), such as a 70 MiB `.glb` mesh in `asset_bytes`, the worker closed
  its connection to the master, stranding every other task running on that worker and causing the
  master to requeue the oversized task until its failover limit ran out.
- `retained_bytes` in `crates/master/src/queue.rs` did not count `TaskResult::asset_bytes`, and
  `TaskQueue` enforced its 256 MiB byte budget only on finished (`terminal`) tasks. Active
  (queued, scheduled, running, retrying) tasks and their mid-task checkpoints were unbounded in
  bytes.

## Goals

- G1: Fail only the task whose `TaskResult` exceeds `MAX_FRAME_SIZE`, keeping the worker session
  and any sibling tasks on that worker alive.
- G2: Preserve as much of an oversized `TaskResult` as fits on the wire, and report what was
  dropped in both `error` and `stderr`.
- G3: Charge `TaskResult::asset_bytes` toward the terminal retention budget in `TaskQueue`.
- G4: Cap the bytes held by active tasks in `TaskQueue` at 256 MiB by default, let the operator
  change the cap via `--max-active-mb`, `RUSTY_GRID_MAX_ACTIVE_MB` and `max_active_mb`, and make
  Map/Reduce jobs wait for room when the active queue is full.

## Non-goals

- Raising `MAX_FRAME_SIZE` above 64 MiB or streaming inline `asset_bytes` across multiple frames.
  Large files should use the content-addressed blob store (`docs/design/0006-content-addressed-task-files.md`).
- Persisting active or terminal tasks to disk.

## Design

### D1: Progressive rewriting of oversized task results

When `MessageWriter::send_msg` returns `ProtocolError::FrameTooLarge { size, max }` for a
`WorkerMessage::TaskResult` in `lane_writer` (`crates/worker/src/client.rs`),
`rewrite_oversized_task_result` builds a failed `TaskResult` via `TaskResult::failure`:

1. It keeps a non-zero `exit_code` from the original result, or uses `EXIT_CODE_GENERAL_ERROR` (1)
   when the original `exit_code` was 0.
2. It always keeps `outputs` (content-addressed blob references) and `device_name`.
3. It drops payload fields in order and stops at the first step whose serialized `WorkerMessage`
   fits within `MAX_FRAME_SIZE` (including the 17-byte AEAD overhead when the lane is encrypted):
   - first `asset_bytes`,
   - then `artifacts`,
   - then `stdout` and `stderr`.
4. It records the reason — naming `size`, `max`, and the fields that were dropped — in `error` and
   appends it to `stderr` (or sets `stderr` to the reason when `stdout` and `stderr` themselves had
   to be dropped). When only `asset_bytes` is too large, the client receives the engine's `stdout`
   and `stderr` together with the reason.

An oversized `WorkerMessage::Checkpoint` is logged and dropped by `lane_writer` without closing the
lane.

### D2: Terminal retention accounting for `asset_bytes`

`retained_bytes` in `crates/master/src/queue.rs` now includes
`r.asset_bytes.as_ref().map_or(0, |b| b.0.len())`. Because `asset_bytes` counts against
`max_terminal_bytes` (`DEFAULT_MAX_TERMINAL_BYTES = 256 MiB`, `--max-retained-mb`), a finished task
that carries a large inline 3D asset can evict older small results from the master's terminal
history, while the newest finished task is always kept so its waiter can read it.

### D3: Active task byte cap and configuration

`TaskQueue` tracks the bytes held by all non-terminal tasks (`Queued`, `Scheduled`, `Running`,
`Retrying`) in `active_sizes` and `active_bytes`, bounded by `max_active_bytes`
(`DEFAULT_MAX_ACTIVE_BYTES = 256 MiB`, re-exported from `rusty_grid_master`):

- `TaskQueue::with_retention_limits` accepts `max_active_bytes` alongside `max_terminal_tasks` and
  `max_terminal_bytes`.
- `ServerConfig::max_active_bytes` and `ServerConfig::with_max_active_bytes` pass the cap to the
  master's queue.
- The CLI and configuration engine (`crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`)
  expose `--max-active-mb <MB>`, `RUSTY_GRID_MAX_ACTIVE_MB`, and `max_active_mb` in `[master]`.
- When a submission would push `active_bytes` above `max_active_bytes`, `TaskQueue::submit` returns
  `GridError::QueueByteLimitExceeded { in_use_bytes, max_bytes }`, which names the bytes in use,
  the limit in bytes, and `--max-active-mb`. `GridError::QueueFull { capacity }` remains for the
  task-count limit. Both the CLI (`ClientResponse::Error`) and REST (`POST /api/tasks`, HTTP 503 in
  `crates/master/src/web_ui.rs`) surface this message.
- When a task transitions to a terminal state (`Completed`, `Failed`, `Cancelled`) or is removed,
  its active byte charge is released.

### D4: Checkpoint growth check

`TaskQueue::record_checkpoint` and `TaskQueue::record_checkpoint_from` share `is_newer_checkpoint`
with `apply_checkpoint`. `checkpoint_exceeds_active_cap` refuses a newer checkpoint with
`GridError::QueueByteLimitExceeded` only when the new checkpoint is larger than the checkpoint it
replaces (`new_cp > old_cp`) and the resulting `active_bytes` would exceed `max_active_bytes`. If
`retry_history` growth on a failover has already pushed `active_bytes` over the cap, an equal or
smaller newer checkpoint is still accepted so failover resumption is not stuck on an outdated
snapshot.

### D5: Map/Reduce backoff on a full queue

In `MapReduceEngine::submit_and_await` (`crates/master/src/mapreduce.rs`), when `queue.submit`
returns `GridError::QueueByteLimitExceeded` or `GridError::QueueFull`:

- If the task alone is larger than `max_bytes` (`task_active_bytes(&task) > max_bytes`) or
  `capacity == 0`, `submit_and_await` fails immediately because the task can never fit.
- Otherwise, `submit_and_await` waits in 25 ms steps and retries until room opens up or the job's
  idle timeout expires. On timeout it returns `GridError::Timeout` naming the queue-limit error
  that prevented submission.

## Compatibility

No wire format tags or message layouts changed. `GridError::QueueByteLimitExceeded` is a library
error variant serialized into existing `ClientResponse::Error { message }` and HTTP 503 JSON error
responses.

## Security

Bounding `active_bytes` prevents queued tasks and mid-task checkpoints from exhausting master
memory before tasks finish. Rewriting oversized `TaskResult` frames on the worker prevents a noisy
or faulty task from disconnecting the worker and disrupting co-scheduled tasks.

## Test plan

- Unit tests in `crates/core/src/error.rs` for `GridError::QueueByteLimitExceeded`.
- Unit tests in `crates/master/src/queue.rs` for `asset_bytes` terminal eviction, active byte cap
  enforcement and release on completion/cancellation, and checkpoint acceptance when equal/smaller
  versus refusal when growing over the cap.
- Unit tests in `crates/master/src/mapreduce.rs` for Map/Reduce jobs waiting for queue room,
  immediate failure when a single task exceeds the cap, and timeout reporting when the queue stays
  full.
- Unit tests in `crates/master/src/web_ui.rs` and `crates/cli/src/cli_app.rs` for REST and CLI
  error messages and `--max-active-mb` configuration.
- Unit and integration tests in `crates/worker/src/client.rs` for `rewrite_oversized_task_result`,
  oversized checkpoint dropping, and a 70 MiB asset task failing with its stdout, stderr, and
  reason preserved while a sibling task on the same worker completes.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Progressive oversized `TaskResult` rewriting | `crates/worker/src/client.rs`, `crates/worker/src/runner.rs` | 70 MiB asset task preserves stdout, stderr, outputs, non-zero exit code, and drop reason while sibling task completes | Implemented |
| W2 | Charge `asset_bytes` and bound active queue bytes | `crates/core/src/error.rs`, `crates/master/src/queue.rs`, `crates/master/src/lib.rs`, `crates/master/src/server.rs`, `crates/master/src/web_ui.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs` | `--max-active-mb` / `RUSTY_GRID_MAX_ACTIVE_MB` configures active cap; CLI and REST report byte limit in bytes; equal/smaller checkpoints pass over cap | Implemented |
| W3 | Map/Reduce wait for queue room | `crates/master/src/mapreduce.rs` | Map/Reduce job waits for room and completes when freed, or times out with reason when kept full | Implemented |

## Status log

- 2026-11-01: Implemented W1–W3 on branch `mac/bug-4-oversized-result-and-budget` (BUG-4).
