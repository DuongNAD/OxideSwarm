# 0075: Non-blocking scheduler dispatch (`BUG-20`)

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

`WorkloadScheduler::dispatch` (`crates/master/src/scheduler.rs`) now attempts a non-blocking
`sender.try_reserve()` on each worker session's outbound channel before falling back to a bounded
per-assignment background waiter (`DISPATCH_RESERVE_TIMEOUT = 5 s`, capped at
`MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER = 16` per worker). A worker whose outbound channel is
full no longer stalls delivery to healthy workers in the same scheduling pass or delays subsequent
scheduling passes (`BUG-20`).

## Context

In `crates/master/src/scheduler.rs`:

- `WorkloadScheduler::schedule_batch` transitions placed tasks to `TaskState::Scheduled` in batch
  (`TaskQueue::schedule_tasks_batch_in_sessions` in `crates/master/src/queue.rs`) and then loops
  sequentially over `report.assignments`, awaiting `self.dispatch(assignment, msg).await` for each
  assignment.
- Previously, `WorkloadScheduler::dispatch` awaited `sender.reserve()` on the worker session's
  bounded `mpsc::Sender<MasterMessage>` (`128` slots for TCP in `crates/master/src/session.rs`)
  with no timeout.
- When one worker stopped draining its outbound channel while still trickling inbound bytes (which
  satisfy `RxClock` in `crates/master/src/reaper.rs` past the `10 s` heartbeat timeout up to
  `RX_LIVENESS_CEILING_FACTOR`), `sender.reserve().await` blocked the entire scheduling pass:
  healthy workers ordered after the stalled worker received nothing from that pass, and the
  scheduler loop could not start the next pass.

## Goals

- **G1**: Prevent a worker with a full outbound channel from blocking delivery to other workers in
  the same pass or delaying subsequent scheduling passes.
- **G2**: Preserve the cancellation invariant that a task cancelled while waiting for channel room
  is never sent (`TaskQueue::deliver_if_scheduled` under the queue read lock).
- **G3**: Bound both how long a deferred delivery waits (`DISPATCH_RESERVE_TIMEOUT = 5 s`) and how
  many deferred waiters can exist per worker (`MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER = 16`),
  releasing all registry and queue reservations (`WorkerRegistry::release_dispatch` and
  `TaskQueue::requeue_dispatch_failure`) when a waiter times out or exceeds the cap.
- **G4**: Keep `Engine`, `hold_back`, `rank`, speed-aware hold-back (`0060`), and VRAM accounting
  (`0068`) unchanged.

## Non-goals

- Changing worker session channel sizes in `crates/master/src/session.rs` or reaper liveness rules
  in `crates/master/src/reaper.rs`.
- Changing wire messages or protocol version (`9`).

## Design

### D1: Non-blocking `try_reserve` fast path and bounded deferred waiter (`crates/master/src/scheduler.rs`)

- After reserving the dispatch slot on the target session via
  `WorkerRegistry::reserve_dispatch(&worker_id, session_id)`, `WorkloadScheduler::dispatch` calls
  `sender.try_reserve()`:
  - `Ok(permit)`: delivers synchronously via
    `TaskQueue::deliver_if_scheduled(&task_id, &worker_id, session_id, move || permit.send(msg))`.
    If the task left `TaskState::Scheduled` on that session in the meantime (`None`), calls
    `WorkerRegistry::release_dispatch(&worker_id, session_id)`.
  - `Err(TrySendError::Closed(()))`: runs `WorkloadScheduler::rollback_closed_dispatch`
    synchronously, releasing the registry dispatch slot, calling
    `TaskQueue::requeue_dispatch_failure(&task_id, &worker_id, session_id)`, and tearing down the
    session via `handle_worker_disconnect_with_broadcast`.
  - `Err(TrySendError::Full(()))`: acquires an `InFlightDispatchGuard` for `worker_id` (capped at
    `MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER = 16`, configurable via
    `WorkloadScheduler::with_max_in_flight_dispatches_per_worker`). If the cap is reached, the
    assignment is released via `WorkerRegistry::release_dispatch` and returned to `TaskState::Queued`
    uncharged via `TaskQueue::requeue_dispatch_failure` without spawning a task.
- When under the per-worker waiter cap, `dispatch` pins `sender.reserve_owned()` and polls it once
  to register the waiter node in the channel's FIFO semaphore waitlist in exact pass order before
  spawning a background task that awaits `tokio::time::timeout(DISPATCH_RESERVE_TIMEOUT, reserve_fut)`
  (configurable via `WorkloadScheduler::with_dispatch_reserve_timeout`).
- On `Ok(Ok(permit))`, the spawned waiter delivers through `TaskQueue::deliver_if_scheduled` and
  calls `WorkerRegistry::release_dispatch` if `None`. On `Ok(Err(_))` (`Closed`), it runs
  `rollback_closed_dispatch`. On `Err(_)` (deadline elapsed), it calls
  `WorkerRegistry::release_dispatch(&worker_id, session_id)` and
  `TaskQueue::requeue_dispatch_failure(&task_id, &worker_id, session_id)`, notifying the scheduler
  trigger when the task is requeued.

### D2: Reservation release parity (`crates/master/src/queue.rs`, `crates/master/src/registry.rs`)

- While a deferred waiter is waiting for channel capacity, the task remains `TaskState::Scheduled`
  in `TaskQueue::worker_assignments`, so `TaskQueue::assigned_loads` continues to reserve its task
  count, CPU core slots (`slots`), unstarted RAM (`unstarted_ram_mb`), and video memory (`vram_mb`),
  and `workspace_active_tasks` keeps its workspace occupied.
- When a deferred waiter times out or is rejected by the per-worker waiter cap, calling
  `WorkerRegistry::release_dispatch` decrements `WorkerEntry::active_tasks` on that session, and
  `TaskQueue::requeue_dispatch_failure` transitions the task back to `TaskState::Queued` uncharged,
  removes it from `worker_assignments` (releasing `tasks`, `slots`, `unstarted_ram_mb`, and
  `vram_mb` in `assigned_loads`), and calls `sync_workspace_active` and
  `refresh_worker_resume_protection`.

## Compatibility

- Internal master scheduler change only. Wire format, CLI flags, configuration files, and protocol
  version (`9`) are unchanged.

## Security

- Bounding both the reserve wait time (`DISPATCH_RESERVE_TIMEOUT = 5 s`) and the per-worker
  in-flight waiter count (`MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER = 16`) prevents a slow or
  wedged worker connection from stalling cluster-wide scheduling or exhausting master memory with
  unbounded spawned tasks.

## Test plan

- **Unit (`crates/master/src/scheduler.rs`):**
  - `test_stalled_worker_full_channel_does_not_block_healthy_worker_and_requeues_after_deadline`:
    with paused Tokio time, one worker whose channel is full and never drained and one healthy
    worker receive placements in the same pass; the healthy worker receives its `AssignTask`
    messages within the pass, and the stalled worker's task returns to `TaskState::Queued` uncharged
    after `DISPATCH_RESERVE_TIMEOUT` with its registry slot, CPU slots, RAM, and VRAM reservations
    released.
  - `test_task_cancelled_while_deferred_delivery_waits_is_never_sent`: a task cancelled while its
    deferred waiter is parked on a full worker channel is never sent when the channel drains, and
    its registry dispatch slot is released.
  - `test_deferred_dispatch_waiter_cap_requeues_excess_and_releases_reservations`: assignments
    exceeding the per-worker in-flight waiter cap are requeued immediately without spawning, and
    their reservations are released.
  - Existing scheduler unit and integration tests (`test_task_cancelled_while_the_pass_waits_on_a_full_channel_is_never_assigned`,
    `m4_scheduling`, `sched4_speed_aware`, `master_limits_and_scheduling`) pass unchanged.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| BUG-20 | Non-blocking `try_reserve` dispatch with bounded per-assignment waiter timeout and per-worker waiter cap | `crates/master/src/scheduler.rs`, `docs/design/0075-non-blocking-dispatch.md`, `docs/README.md`, `CHANGELOG.md` | Healthy worker receives assignments while a peer's channel is full, stalled assignment requeues after `DISPATCH_RESERVE_TIMEOUT`, cancelled waiting task is never sent, and scheduler test targets pass | Implemented |

## Status log

- 2026-11-15: Implemented `DISPATCH_RESERVE_TIMEOUT`, `MAX_IN_FLIGHT_DISPATCH_WAITERS_PER_WORKER`,
  `InFlightDispatchGuard`, and non-blocking `try_reserve` + bounded deferred waiters in
  `WorkloadScheduler::dispatch` (`crates/master/src/scheduler.rs`, `BUG-20`).
