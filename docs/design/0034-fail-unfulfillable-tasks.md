# 0034: Fail unfulfillable tasks when no registered worker can satisfy requirements or placement constraints

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Add `SchedulerConfig::fail_unfulfillable_tasks` (`--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`, default `false`) so that when at least one worker is registered in `WorkerRegistry`, `WorkloadScheduler::schedule_batch` immediately transitions a task that no registered worker could ever satisfy (by static hardware/engine capabilities or `PlacementConstraints`) to `TaskStatus::Failed` with a descriptive reason, wakes any `--wait` caller, and broadcasts `DashboardEvent::TaskStatusChange` and `DashboardEvent::TaskCompleted`.

## Context

Before this change (`SCHED-3` in `docs/BACKLOG.md`):

- In `Engine::place` (`crates/master/src/scheduler.rs`), when no currently schedulable worker matched a queued task (for example, `require:tag=nothing` or a GPU requirement when only CPU workers exist), `place` returned `None`.
- In `WorkloadScheduler::schedule_batch` (`crates/master/src/scheduler.rs`), a `None` placement caused the loop to increment `skipped_tasks`, push the task back onto `TaskQueue` (`self.queue.requeue(task)`), and `break` out of the scheduling batch without inspecting subsequent queued tasks.
- A task whose static hardware capabilities or placement constraints no registered worker could satisfy remained in `TaskStatus::Queued` forever (`oxideswarm submit --wait` hung until client timeout or manual cancellation), and also blocked the head of the scheduling batch on each pass.

## Goals

- **G1** Distinguish between transient unavailability (no workers registered yet, or a matching registered worker is currently busy/overloaded/disconnected) and permanent unfulfillability across all currently registered workers (`all_workers`).
- **G2** Add `SchedulerConfig::fail_unfulfillable_tasks: bool` (default `false` so workloads that submit tasks before matching workers join continue to queue by default) and expose it via `--fail-unfulfillable-tasks` (`RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`) on `oxideswarm master`.
- **G3** When `fail_unfulfillable_tasks` is `true` and at least one worker is registered, transition any unfulfillable queued task to `TaskStatus::Failed` with an actionable reason naming the unsatisfied placement constraints or task requirements, notify `wait_for_task` callers (`oxideswarm submit --wait`), broadcast the status change to the dashboard, and continue scheduling remaining tasks in the batch.

## Non-goals

- Changing the default (`false`) when `--fail-unfulfillable-tasks` is not enabled: clusters where workers join dynamically after task submission rely on tasks waiting in `TaskStatus::Queued` by default.
- Failing tasks when zero workers are registered in `WorkerRegistry` (`!all_workers.is_empty()` is required before a task can be declared unfulfillable).

## Design

### D1 Capability and placement check against all registered workers (`Engine::place`)

- `PlacementDecision` (`crates/master/src/scheduler.rs`) is an internal enum returned by `Engine::place`:
  - `PlacementDecision::Assigned(Uuid)` when a schedulable worker is selected.
  - `PlacementDecision::Defer` when no currently schedulable worker can run the task right now, but either `all_workers` is empty or at least one registered worker in `all_workers` satisfies the task's static capability and placement constraints (so the worker is merely busy, overloaded, or temporarily unavailable).
  - `PlacementDecision::Unfulfillable(String)` when `!all_workers.is_empty()` and no registered worker in `all_workers` satisfies both `PlacementConstraints::matches_worker` and `WorkerCapabilities::can_execute(&task.requirements, &task.spec)`.
- Reason formatting:
  - If placement constraints (`PlacementConstraints::from_task_tags(&task.tags)`) are non-empty and no registered worker in `all_workers` matches `placement.matches_worker(&w.capabilities, w.worker_id)`, the reason is `"No registered worker matches the placement constraints ({placement})"`.
  - Otherwise, the reason is `"No registered worker satisfies the task requirements (cpu_cores={}, min_ram_mb={}, requires_gpu={}, task_type={:?})"`.

### D2 Queue transition and waiter notification (`TaskQueue::fail_unfulfillable_task`)

- `TaskQueue::fail_unfulfillable_task(&self, task: Task, reason: String) -> Result<TaskResult>` (`crates/master/src/queue.rs`) transitions a task that was popped from the ready queue directly to `TaskStatus::Failed` without incrementing `retry_count` or re-queuing it:
  - Constructs `TaskResult::failure(task_id, Vec::new(), 1, reason, 0)`.
  - Stores the terminal `TaskEntry` (`TaskStatus::Failed`), enforces terminal retention limits (`enforce_retention_limits`), and wakes any `wait_for_task` subscriber via `notify.notify_waiters()`.
- `BatchScheduleReport` (`crates/master/src/scheduler.rs`) gains `pub unfulfillable: Vec<(Uuid, String)>`.
- In `WorkloadScheduler::schedule_batch`, when `Engine::place` returns `PlacementDecision::Unfulfillable(reason)`:
  - If `self.config.fail_unfulfillable_tasks` is `true`, `schedule_batch` calls `self.queue.fail_unfulfillable_task(task, reason)`, appends `(task_id, reason)` to `report.unfulfillable`, increments `report.skipped_tasks`, and continues the loop to schedule subsequent tasks in the batch rather than breaking early.
  - If `self.config.fail_unfulfillable_tasks` is `false`, `schedule_batch` requeues the task (`self.queue.requeue(task)`) and keeps it in `TaskStatus::Queued`.
- In `crates/master/src/server.rs`, the background scheduler loop iterates `report.unfulfillable` and broadcasts `DashboardEvent::TaskStatusChange` (`status: "Failed"`) and `DashboardEvent::TaskCompleted` (`success: false`) for each failed unfulfillable task.

### D3 CLI, environment, and config wiring

- `MasterArgs` (`crates/cli/src/cli_app.rs`) gains `--fail-unfulfillable-tasks [BOOL]` (`Option<bool>`, with `num_args = 0..=1` and `default_missing_value = "true"`, so bare `--fail-unfulfillable-tasks` sets `Some(true)` while `--fail-unfulfillable-tasks false` sets `Some(false)`).
- `MasterFileConfig` (`crates/cli/src/config.rs`) gains `pub fail_unfulfillable_tasks: Option<bool>`.
- `run_master` resolves `fail_unfulfillable_tasks` via `resolve_bool(args.fail_unfulfillable_tasks, &["RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS"], master_cfg.and_then(|m| m.fail_unfulfillable_tasks), false)` and passes it to `build_master_scheduler_config`.

## Compatibility

- Wire format: unchanged. Unfulfillable tasks produce standard `ClientResponse::TaskCompleted` / `TaskResult` (`exit_code: 1`, `error: Some(reason)`) responses.
- Default behaviour: unchanged (`fail_unfulfillable_tasks = false`). Existing deployments that submit tasks before matching workers register see no change unless `--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS=true`, or `[master] fail_unfulfillable_tasks = true` is set.

## Security

- Failing unfulfillable tasks when `fail_unfulfillable_tasks` is enabled prevents tasks with impossible placement tags or hardware requirements from occupying active queue slots (`--max-queue-size` and `--max-active-mb`, [design 0024](0024-wire-cli-flags-and-retire-ttl-eviction.md)) or stalling head-of-line scheduling indefinitely.

## Test plan

- Unit test `scheduler::tests::test_fail_unfulfillable_tasks_default_false_keeps_queued_and_true_fails_with_reason_and_wakes_waiter` (`crates/master/src/scheduler.rs`):
  - registers one worker without `tag=nothing`;
  - verifies that with `SchedulerConfig::default()` (`fail_unfulfillable_tasks == false`), a task requiring `require:tag=nothing` stays `TaskStatus::Queued` after `schedule_once()`;
  - verifies that with `SchedulerConfig::default().with_fail_unfulfillable_tasks(true)`, `schedule_once()` transitions the task to `TaskStatus::Failed`, populates `report.unfulfillable` and `TaskResult.error` with `"No registered worker matches the placement constraints (tag=nothing)"`, and wakes a concurrent `queue.wait_for_task(task_id)` caller immediately.
- Live `run_master` test in `cli_app::tests::test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket` (`crates/cli/src/cli_app.rs`):
  - verifies `resolve_bool` precedence for `fail_unfulfillable_tasks` (`CLI > RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS > [master].fail_unfulfillable_tasks > false`);
  - starts `run_master` with `--scheduler-policy round-robin --fail-unfulfillable-tasks`, connects two simulated workers (`rr-w1` and `rr-w2`), and verifies `run_submit_with_secret` with `--require-tag nothing --wait` exits with `ExitCode::FAILURE` and returns `TaskResult.error == Some("No registered worker matches the placement constraints (tag=nothing)")`.
- Mutation checks:
  - Setting `sched_config.policy = SchedulingPolicy::WeightedLeastLoaded` at `MasterServer::spawn_with_config` in `run_master` causes `test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket` to fail (`wt2` times out waiting for `AssignTask`).
  - Setting `sched_config.fail_unfulfillable_tasks = false` at `MasterServer::spawn_with_config` in `run_master` causes `test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket` to fail (`--wait` times out).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `SCHED-3` | Add `fail_unfulfillable_tasks` (`--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`) to transition unfulfillable tasks to `TaskStatus::Failed` with a descriptive reason and wake `--wait` callers | `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/server.rs`, `crates/cli/src/config.rs`, `crates/cli/src/cli_app.rs`, `README.md`, `docs/README.md` | Submitting `--require-tag nothing` against a worker without that tag stays `Queued` under default and transitions to `Failed` with a descriptive reason under `--fail-unfulfillable-tasks`, waking `--wait` | Implemented |

## Status log

- 2026-11-04: Implemented `SchedulerConfig::fail_unfulfillable_tasks`, `TaskQueue::fail_unfulfillable_task`, and `--fail-unfulfillable-tasks` (`RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`), plus the `SCHED-2` follow-up tests and source-prefixed error diagnostics (`SCHED-3`).
