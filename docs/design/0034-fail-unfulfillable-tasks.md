# 0034: Fail unfulfillable tasks when no registered worker can satisfy requirements or placement constraints

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-07

## Summary

Wire `SchedulerConfig::fail_unfulfillable_tasks` (`--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`, default `false`) so that when `WorkloadScheduler::schedule_batch` runs with at least one schedulable worker, any queued task whose static capabilities, protocol features, or `Placement` constraints no registered worker in `WorkerRegistry` can satisfy is transitioned from `TaskState::Queued` to `TaskState::Failed` via `TaskQueue::fail_unfulfillable_task`, wakes any `wait = true` caller in `WaiterMap`, and broadcasts `DashboardStreamMessage::TaskUpdated` and `DashboardStreamMessage::StatsUpdated`.

## Context

Before this change (`SCHED-3` in `docs/BACKLOG.md`):

- `Engine::place` (`crates/master/src/scheduler.rs`) already skipped unplaceable tasks without head-of-line blocking (`Verdict::Skip`) and returned `Verdict::Unfulfillable` when `view.placement` was `Err`, or when `fail_unfulfillable_tasks` was `true` and `view.requires_gpu && !self.any_gpu_worker`, `nobody_has_features`, or `nobody_matches` held across `self.seats`.
- However:
  1. `Engine::place` did not check whether any worker satisfied the combined static resource, GPU, feature, and placement requirements (`statically_fits`, `placement_admits`, `can_execute_gpu`, `features.contains`), so a task requiring more CPU cores or RAM than any matching worker had returned `Verdict::Skip(ScheduleSkipReason::AllEligibleWorkersSaturated)` instead of `Verdict::Unfulfillable`.
  2. `Engine` checked only `list_schedulable_workers()`, which excludes workers that are `WorkerStatus::Disconnected` (not yet removed by `prune_stale_workers`) or cooling down under `excluded_until` after `record_rejection`.
  3. `WorkloadScheduler::schedule_batch` recorded `report.unfulfillable` on `ScheduleReport` without transitioning those tasks in `TaskQueue` or notifying `self.waiters`.
  4. `SchedulerConfig::fail_unfulfillable_tasks` had no builder method, CLI flag (`--fail-unfulfillable-tasks`), environment variable (`RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`), or config field (`[master].fail_unfulfillable_tasks`).

## Goals

- **G1** Run the static unfulfillability check in `Engine::place` whenever at least one connected (`WorkerStatus::Connected` or `WorkerStatus::Busy`) worker is schedulable, checking against every registered worker in `WorkerRegistry` (`registered` from `WorkerRegistry::scheduler_snapshot`, including `WorkerStatus::Disconnected` and cooling-down `excluded_until` workers), so tasks are never failed while a matching worker is still registered. A task whose only matching worker is `WorkerStatus::Disconnected` waits in `TaskState::Queued` until that worker reconnects or is pruned `1 hour` after its last heartbeat (`ReaperConfig::stale_worker_timeout` in `crates/master/src/reaper.rs:31`, which has no CLI flag, so a `--wait` client blocks that long unless `--timeout-secs` expires first).
- **G2** Expose `SchedulerConfig::with_fail_unfulfillable_tasks(mut self, fail: bool) -> Self` and wire `--fail-unfulfillable-tasks` (`RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`, default `false`) on `oxideswarm master`.
- **G3** Transition each `(task_id, reason)` in `report.unfulfillable` from `TaskState::Queued` to `TaskState::Failed` via `TaskQueue::fail_unfulfillable_task`, wake `self.waiters`, and broadcast `DashboardStreamMessage::TaskUpdated` and `DashboardStreamMessage::StatsUpdated`, while avoiding redundant queue scans on saturated clusters when neither the registry nor the ready queue has changed.

## Non-goals

- Changing the default (`false`) when `--fail-unfulfillable-tasks` is not enabled.
- Failing tasks when no connected worker is schedulable (`workers.is_empty()` returns `ScheduleSkipReason::NoEligibleWorkers` before `Engine::place` runs).

## Design

### D1 Static capability and placement check against all registered workers (`Engine::place`)

- `WorkloadScheduler::schedule_batch` (`crates/master/src/scheduler.rs`) requires at least one connected, schedulable worker (`!workers.is_empty()`) before walking `ready_queue` through `Engine::place`.
- `WorkerRegistry::scheduler_snapshot(&self) -> (Vec<WorkerInfo>, Vec<WorkerInfo>, u64)` (`crates/master/src/registry.rs`) returns `(schedulable, registered, registration_epoch)` under a single read lock, where `registered` includes `WorkerStatus::Disconnected` and cooling-down (`excluded_until`) workers, and `registration_epoch` increments in `register_session`, `remove_worker`, and `prune_stale_workers`. Because a `WorkerStatus::Disconnected` worker stays in `registered` until `prune_stale_workers` removes it `1 hour` after its last heartbeat (`ReaperConfig::stale_worker_timeout: Some(Duration::from_secs(3600))` in `crates/master/src/reaper.rs:31`, with no CLI flag), a task whose only matching worker is `Disconnected` stays `Queued` (and any `--wait` client blocks) until that worker reconnects, is pruned after 1 hour, or the client's `--timeout-secs` elapses.
- `Engine` (`crates/master/src/scheduler.rs`) stores `seats: Vec<Seat<'a>>` (built from `workers`) and `registered: &'a [WorkerInfo]`, plus `dead: HashMap<Shape, Verdict>` so repeated tasks of the same `Shape` within a pass reuse the cached `Verdict::Skip` or `Verdict::Unfulfillable` without re-checking workers.
- `Engine::place(&mut self, view: &TaskView) -> Verdict` returns:
  - `Verdict::Unfulfillable(format!("Invalid placement constraints: {e}"))` when `view.placement` is `Err(e)`.
  - When `self.config.fail_unfulfillable_tasks` is `true`, checks `self.registered`:
    - If `view.requires_gpu && !self.any_gpu_worker`: `Verdict::Unfulfillable("Cluster lacks workers with GPU capabilities".into())`.
    - If `nobody_has_features`: `Verdict::Unfulfillable(format!("Cluster lacks workers with {} capability", feature_names(view.required_features)))`.
    - If `nobody_matches`: `Verdict::Unfulfillable(format!("No registered worker matches the placement constraints ({placement})"))`.
    - If `nobody_fits` (`!self.registered.iter().any(|w| statically_fits(&view.requirements, w) && placement_admits(placement, w) && (!view.requires_gpu || w.capabilities.can_execute_gpu()) && w.features.contains(view.required_features))`): `Verdict::Unfulfillable(format!("No registered worker satisfies task requirements (cpu_cores={}, ram_mb={}, gpu={})", view.requirements.cpu_cores, view.requirements.ram_mb, view.requires_gpu))`.
  - `Verdict::Skip(ScheduleSkipReason)` when no seat in `self.seats` can run the task right now.
  - `Verdict::Placed(usize)` when a seat in `self.seats` is selected.
- `WorkloadScheduler` stores `unfulfillable_sweep: std::sync::Mutex<UnfulfillableSweep>` tracking `epochs: Option<(u64, u64)>` (`(registration_epoch, ready_epoch)`, where `TaskQueue::ready_epoch` increments whenever a task enters `ready_queue` via `TaskQueueInner::insert_ready`), `cursor: Option<QueueOrderKey>`, and `completed: bool`. When `ready_queue` is longer than `max_scan_per_pass`, saturated passes (`engine.free_slots == 0`) resume from `cursor` and set `completed = true` once `ready_views` reaches the end of the queue (`next == None`), after which subsequent saturated passes with unchanged `(registration_epoch, ready_epoch)` make no `ready_views` call.

### D2 Queue transition and waiter notification (`TaskQueue::fail_unfulfillable_task`)

- `pub async fn fail_unfulfillable_task(&self, task_id: &TaskId, reason: String) -> Option<TaskResult>` (`crates/master/src/queue.rs`):
  - Verifies `entry.state == TaskState::Queued`, removes `QueueOrderKey` from `inner.ready_queue`, constructs `TaskResult::failure(Uuid::nil(), *task_id, 1, "", "", 0, Some(reason.clone()))`, sets `entry.state = TaskState::Failed`, `entry.error_message = Some(reason)`, and `entry.result = Some(result.clone())`, and calls `inner.track_terminal_task(*task_id)` to release active bytes and enforce terminal retention limits.
- In `WorkloadScheduler::schedule_batch` (`crates/master/src/scheduler.rs`), for each `(task_id, reason)` in `&report.unfulfillable`, if `self.queue.fail_unfulfillable_task(task_id, reason.clone()).await` returns `Some(result)`, the scheduler logs `warn!(task_id = %task_id, reason = %reason, "Failing unfulfillable task")` and sends `result.clone()` to any waiting channels removed from `self.waiters`.
- In `crates/master/src/server.rs`, when `!report.assignments.is_empty() || !report.unfulfillable.is_empty()`, the scheduler loop sends `DashboardStreamMessage::TaskUpdated(info)` for each `(task_id, _)` in `&report.unfulfillable` and `DashboardStreamMessage::StatsUpdated(sched_queue.stats().await)`.

### D3 CLI, environment, and config wiring

- `MasterArgs` (`crates/cli/src/cli_app.rs`) gains `--fail-unfulfillable-tasks [BOOL]` (`pub fail_unfulfillable_tasks: Option<bool>`, with `num_args = 0..=1` and `default_missing_value = "true"`).
- `MasterFileConfig` (`crates/cli/src/config.rs`) gains `pub fail_unfulfillable_tasks: Option<bool>`.
- `resolve_fail_unfulfillable_tasks(flag: Option<bool>, master_cfg: Option<&MasterFileConfig>) -> bool` (`crates/cli/src/cli_app.rs`) calls `resolve_bool(flag, &["RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS"], master_cfg.and_then(|m| m.fail_unfulfillable_tasks), false)`, and `run_master` passes the result into `build_master_scheduler_config`.

## Compatibility

- Wire format: unchanged. Unfulfillable tasks produce standard `ClientResponse::TaskCompleted` / `TaskResult` (`exit_code: 1`, `error: Some(reason)`) responses.
- Default behaviour: unchanged (`fail_unfulfillable_tasks = false`).

## Security

- Failing unfulfillable tasks when `fail_unfulfillable_tasks` is enabled prevents tasks with impossible placement constraints or resource requirements from occupying active queue slots (`--max-queue-size` and `--max-active-mb`, [design 0024](0024-wire-cli-flags-and-retire-ttl-eviction.md)).

## Test plan

- Unit tests in `scheduler::tests` (`crates/master/src/scheduler.rs`):
  - `test_fail_unfulfillable_tasks_default_false_keeps_queued_and_true_fails_with_reason_and_wakes_waiter`
  - `test_fail_unfulfillable_tasks_sweep_cursor_scans_large_queue_once_across_saturated_passes`
  - `test_fail_unfulfillable_tasks_keeps_gpu_task_queued_when_gpu_worker_is_disconnected_until_pruned`
  - `test_fail_unfulfillable_tasks_keeps_gpu_task_queued_when_gpu_worker_is_cooling_down_until_pruned`
  - `test_fail_unfulfillable_tasks_combined_placement_and_gpu_checks_in_nobody_fits`
- Live `run_master` and precedence test in `cli_app::tests::test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket` (`crates/cli/src/cli_app.rs`).
- Multiplexed control and data lane codec test in `server::tests::test_master_server_bind_enforces_max_queue_size_and_json_wire_codec` (`crates/master/src/server.rs`).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `SCHED-3` | Add `fail_unfulfillable_tasks` (`--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`) to transition unfulfillable tasks to `TaskState::Failed` with a descriptive reason and wake `--wait` callers | `crates/master/src/registry.rs`, `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/server.rs`, `crates/cli/src/config.rs`, `crates/cli/src/cli_app.rs`, `README.md`, `docs/README.md` | Submitting `--require-tag nothing` against a worker without that tag stays `Queued` under default and transitions to `Failed` with a descriptive reason under `--fail-unfulfillable-tasks`, waking `--wait` | Implemented |

## Status log

- 2026-11-04: Implemented `SchedulerConfig::fail_unfulfillable_tasks`, `TaskQueue::fail_unfulfillable_task`, and `--fail-unfulfillable-tasks` (`RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`), plus the `SCHED-2` follow-up tests and source-prefixed error diagnostics (`SCHED-3`).
- 2026-11-07: Checked static fulfillability against all registered workers (`WorkerRegistry::scheduler_snapshot`, including `Disconnected` and cooling-down workers until pruned by `prune_stale_workers`), cached verdicts per `Shape` in `Engine::dead` and tracked `(registration_epoch, ready_epoch)` and sweep `cursor` in `WorkloadScheduler::unfulfillable_sweep`, added combined placement/GPU `nobody_fits`, multi-pass sweep cursor, and `resolve_fail_unfulfillable_tasks` tests, and aligned this design record with the codebase.
