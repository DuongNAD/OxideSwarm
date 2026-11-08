# 0036: Bounded uncharged worker evacuations (`MOB-3`)

Status: Implemented
Created: 2026-11-07    Updated: 2026-11-07

## Summary

1. Announced worker drains (`WorkerMessage::Disconnecting`, handled by
   `handle_worker_evacuate_with_broadcast` in `crates/master/src/server.rs` and
   `TaskQueue::handle_session_evacuated` in `crates/master/src/queue.rs`) return in-flight tasks to
   `TaskState::Queued` (`ready_queue`, 0 ms delay) without charging `retry_count` or
   `failover_count`, up to a dedicated per-task cap `RetryPolicy::max_evacuations` (default
   `DEFAULT_MAX_EVACUATIONS = 10`).
2. Each uncharged drain increments `TaskEntry::evacuation_count` (exposed on `TaskInfo` as
   `#[serde(default)] pub evacuation_count: u32`) and appends an audit entry to `retry_history`.
3. Past `max_evacuations` (or when `retry_on_worker_disconnect` is `false`, or when a `Running`
   task has `max_retries == 0` via `requirements.max_retries` or `RetryPolicy::max_retries`), the
   drain takes the charged failover path so no task can run without bound or violate "run at most
   once".

## Context

- Before `MOB-3`, `WorkerMessage::Disconnecting` in `crates/master/src/server.rs` called
  `handle_worker_disconnect_with_broadcast(..., immediate_reschedule = true)`, which invoked
  `TaskQueue::handle_session_disconnected` (`crates/master/src/queue.rs`). That path incremented
  both `retry_count` and `failover_count` against `failover_budget`. Consequently, a long job on a
  mobile worker that underwent repeated pre-sleep evacuations (`trigger_fast_evacuation_impl` in
  `crates/android_bridge/src/lib.rs`) exhausted its failover or per-task `max_retries` budget and
  failed even though every handoff was an announced voluntary drain.
- Conversely, making `handle_session_evacuated` requeue tasks unconditionally without any cap or
  `max_retries == 0` check would allow a task (or a phone that repeatedly sleeps before a long task
  finishes) to re-run forever: because `WorkerMessage::Disconnecting` is also sent on `SIGTERM` or
  `Ctrl-C` (`crates/cli/src/cli_app.rs`, `crates/worker/src/client.rs`), a task subprocess running
  as the worker's user could `kill -TERM $PPID` and be reassigned across the cluster without limit,
  and a `Running` task with `max_retries = 0` ("never run this task twice") would be run a second
  time.

## Goals

- **G1**: Return tasks on an announced draining worker (`WorkerMessage::Disconnecting`) to `Queued`
  with 0 ms delay without incrementing `retry_count` or `failover_count` while under
  `RetryPolicy::max_evacuations` (`DEFAULT_MAX_EVACUATIONS = 10`).
- **G2**: Bound uncharged evacuations per task by `max_evacuations`, falling back to the charged
  failover path once `evacuation_count >= max_evacuations`, and record every drain in
  `TaskEntry::retry_history`.
- **G3**: Preserve the "run at most once" invariant (`max_retries == 0` on `TaskRequirements` or
  `RetryPolicy`) for `Running` tasks, and honor `RetryPolicy::retry_on_worker_disconnect == false`.

## Non-goals

- Changing the wire format of `WorkerMessage::Disconnecting`.
- Changing the exponential backoff path for unannounced disconnects or heartbeat timeouts
  (`TaskQueue::handle_session_disconnected` with `immediate_reschedule = false`).

## Design

### D1: `evacuation_count` and `RetryPolicy::max_evacuations`

- `crates/master/src/queue.rs` defines `pub const DEFAULT_MAX_EVACUATIONS: u32 = 10` and adds
  `#[serde(default = "default_max_evacuations")] pub max_evacuations: u32` to `RetryPolicy` (and
  `pub max_evacuations: u32` with `.with_max_evacuations(u32)` on `ServerConfig` in
  `crates/master/src/server.rs`).
- `TaskEntry` and `TaskInfo` (`crates/master/src/queue.rs`) carry `evacuation_count: u32`
  (`#[serde(default)]` on `TaskInfo`) Alongside `retry_count` and `failover_count`.

### D2: Bounded uncharged drain handling (`handle_session_evacuated`)

When `TaskQueue::handle_session_evacuated(worker_id, session_id, reason)` processes an active task
(`Scheduled` or `Running`) owned by `(worker_id, session_id)`:

1. It checks `running_with_zero_retries = entry.state == TaskState::Running && policy.zero_retries_for(entry)`,
   where `zero_retries_for` is `true` when `entry.task.requirements.max_retries == Some(0)` or
   `(entry.task.requirements.max_retries.is_none() && policy.max_retries == 0)`.
2. **Uncharged evacuation branch** (`policy.retry_on_worker_disconnect && !running_with_zero_retries && entry.evacuation_count < policy.max_evacuations`):
   - Increments `entry.evacuation_count += 1` (`retry_count` and `failover_count` remain unchanged).
   - Appends a `RetryAttempt` to `entry.retry_history` with `attempt: entry.retry_count`,
     `failed_worker_id: Some(*worker_id)`, `reason`, and
     `error: Some("Worker draining (<reason>); uncharged evacuation <evacuation_count>/<max_evacuations>")`.
   - Transitions `entry.state` to `TaskState::Queued`, clears `assigned_worker_id`,
     `assigned_session`, `scheduled_instant`, `started_instant`, and `retry_after_instant`, inserts
     the task into `ready_queue` with 0 ms delay, and updates `active_bytes` accounting.
3. **Charged fallback branch** (when `entry.evacuation_count >= policy.max_evacuations`,
   `running_with_zero_retries`, or `!policy.retry_on_worker_disconnect`):
   - Evaluates `(budget_left, cap) = policy.failover_budget(entry)`.
   - If `policy.retry_on_worker_disconnect && !running_with_zero_retries && budget_left`, increments
     `entry.retry_count += 1` and `entry.failover_count += 1`, appends a `Worker evicted (<reason>)`
     entry to `entry.retry_history`, and re-enqueues the task to `ready_queue` with 0 ms delay.
   - Otherwise, transitions `entry.state` to `TaskState::Failed`, records the terminal failure
     result, and tracks the terminal task in the queue's retention budget.
4. In `crates/master/src/server.rs`, `handle_worker_evacuate_with_broadcast` unregisters the
   draining session, calls `queue.handle_session_evacuated`, broadcasts dashboard updates, calls
   `resolve_terminal_waiters` so any `wait_task` / `--wait` client wakes immediately if a task
   reached `TaskState::Failed`, and notifies the scheduler.

## Compatibility

- Wire protocol frames (`WorkerMessage::Disconnecting`) are unchanged.
- `TaskInfo::failover_count`, `TaskInfo::evacuation_count`, and `RetryPolicy::max_evacuations` carry
  `#[serde(default)]`, preserving JSON serialization compatibility for dashboard and REST consumers.

## Security

- Bounding uncharged drains by `max_evacuations` (default 10) and falling back to `failover_budget`
  ensures that a task subprocess which repeatedly triggers graceful worker shutdown (`kill -TERM $PPID`)
  cannot re-run more than `max_evacuations + max(max_failovers, max_retries)` times (or
  `max_evacuations + requirements.max_retries`), and a `Running` task with `max_retries = 0` cannot
  force a second execution via `SIGTERM`.
- Every uncharged drain appends a `RetryAttempt` to `retry_history`, which is included in
  `retained_bytes(entry)` so `active_bytes` and `max_active_bytes` bound memory growth.

## Test plan

- **Unit (`crates/master/src/queue.rs`):**
  `test_repeated_evacuations_leave_failover_and_retry_counts_at_zero_and_preserve_budgets` verifies:
  1. `1..=max_evacuations` drains leave `retry_count == 0` and `failover_count == 0`, increment
     `evacuation_count`, and append an audit entry to `retry_history`;
  2. drain `max_evacuations + 1` takes the charged path (`retry_count == 1`, `failover_count == 1`),
     and the subsequent drain after the charged budget is spent ends in `TaskState::Failed`;
  3. with 0 charged budget on a `Scheduled` task, `N = max_evacuations` drains succeed and the
     `(N + 1)`-th drain ends in `TaskState::Failed`;
  4. a `Running` task with `with_max_retries(0)` or `RetryPolicy { max_retries: 0, .. }` transitions
     to `TaskState::Failed` on the first drain without re-running.
- **Integration (`tests/mobile_coordination_test.rs`, `tests/m9_stress_challenge_2.rs`, `tests/m5a_challenger_2_stress.rs`, `tests/m9_stress_challenge_1.rs`):**
  - `test_fast_evacuation_under_50ms_without_task_loss` verifies two consecutive pre-sleep
     evacuations across three workers complete with `retry_count == 0`, `failover_count == 0`, and
     `evacuation_count == 2`.
  - `test_boundary_max_retries_5_overrides_lower_server_default` verifies `max_evacuations` uncharged
     graceful disconnects followed by 5 charged graceful disconnects and terminal failure on the
     next disconnect.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| MOB-3 | Bounded uncharged worker evacuations on `WorkerMessage::Disconnecting` | `crates/master/src/queue.rs`, `crates/master/src/server.rs`, `tests/mobile_coordination_test.rs`, `tests/m5a_challenger_2_stress.rs`, `tests/m9_stress_challenge_1.rs`, `tests/m9_stress_challenge_2.rs`, `docs/design/0002-mobile-coordination.md`, `docs/design/0036-bounded-uncharged-worker-evacuations.md`, `docs/README.md` | Repeated evacuations up to `max_evacuations` leave `failover_count` and `retry_count` at 0 and record `retry_history`; `N + 1` drains end in `Failed`; a `Running` task with `max_retries(0)` is not run again | Implemented |

## Status log

- 2026-11-07: Implemented `TaskQueue::handle_session_evacuated`, `RetryPolicy::max_evacuations` (`DEFAULT_MAX_EVACUATIONS = 10`), `TaskEntry::evacuation_count`, `retry_history` audit entries, and `Running` `max_retries == 0` enforcement (`MOB-3`).
