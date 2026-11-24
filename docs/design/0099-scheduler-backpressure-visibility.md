# 0099: Scheduler host-CPU backpressure visibility (`SCHED-5`)

Status: Implemented
Created: 2026-11-21    Updated: 2026-11-21

## Summary

When every eligible connected worker reports host CPU usage above `max_host_cpu_pct` (default `85.0`), the scheduler leaves queued tasks in `TaskState::Queued` with `ScheduleSkipReason::AllEligibleWorkersSaturated`. Previously that holdback happened silently at default log levels (`info`), so an operator saw tasks stay `Queued` with no explanation. `WorkloadScheduler::schedule_batch` now emits a single `warn!` log when the set of connected, free-slotted, CPU-busy workers holding back `AllEligibleWorkersSaturated` tasks appears or changes, and clears silently (logging at `info!` without the word `"saturated"`) when the holdback ends.

## Context

- `crates/master/src/scheduler.rs` (`Engine::new`, `SeatStaticMeta::host_busy`): marks each seat `host_busy = seat.worker.cpu_usage_pct > config.max_host_cpu_pct`, and `crates/master/src/worker_index.rs` excludes `host_busy` seats from `free_slots_mask` and `unblocked_mask`.
- `crates/master/src/scheduler.rs` (`Engine::skip_reason`): distinguishes `ScheduleSkipReason::NoGpuWorkersAvailable`, `ScheduleSkipReason::ClusterLacksCapability`, and `ScheduleSkipReason::AllEligibleWorkersSaturated`.
- In `TEST-81` (pull request `404`), queued tasks stalled silently at `max_host_cpu_pct = 85.0`, and a temporary per-pass `debug!` log emitted `51` lines in `5 s`. Three review requirements came out of that run:
  1. Count only skips whose reason is `ScheduleSkipReason::AllEligibleWorkersSaturated`.
  2. Filter candidate busy workers to connected (`status != WorkerStatus::Disconnected`) seats only.
  3. Report once per change of the busy-worker set rather than on every scheduling pass.

## Goals

- G1. Operators running `oxideswarm master` at the default `info` log level immediately see why queued tasks are held back by host-CPU backpressure.
- G2. Repeated `100 ms` scheduler ticks or fluctuating CPU percentages above the threshold do not flood master logs.
- G3. Capability mismatches (`NoGpuWorkersAvailable`, `ClusterLacksCapability`), `Disconnected` workers, and ordinary full-slot saturation (`cpu_usage_pct <= max_host_cpu_pct`) never emit a misleading host-CPU backpressure warning.

## Non-goals

- Changing `ClientResponse::ClusterStatus` or bumping the wire protocol (`PROTOCOL_VERSION_CURRENT` stays `10`) or store format (`STORE_FORMAT_VERSION` stays `4`).
- Changing the `max_host_cpu_pct` admission threshold or `Engine` placement decisions.

## Design

### D1: Transition-triggered `warn!` reporting in `WorkloadScheduler`

`WorkloadScheduler` (`crates/master/src/scheduler.rs`) stores `host_cpu_backpressure_reported: std::sync::Mutex<Vec<Uuid>>`, holding the sorted IDs of connected workers last reported as held back by host-CPU backpressure.

On each `WorkloadScheduler::schedule_batch` pass:

1. If `workers.is_empty()` (`NoEligibleWorkers`), clear `host_cpu_backpressure_reported` (logging `"Host-CPU backpressure holdback cleared"` at `info!` if it had been active) and return.
2. While walking queued tasks in Step 3 (both fair-share and standard scans), count `saturated_skips` strictly for `Verdict::Skip(ScheduleSkipReason::AllEligibleWorkersSaturated)` outcomes (not gated on `report.skipped.len() < max_batch_size`, and ignoring `NoGpuWorkersAvailable` and `ClusterLacksCapability`).
3. Immediately after Step 3 and before `if placements.is_empty()`, when `saturated_skips > 0`, collect all seats in `engine.seats` matching:
   - `seat.worker.status != WorkerStatus::Disconnected`,
   - `engine.worker_index.meta(idx).host_busy`, and
   - `seat.remaining_slots() > 0` (so workers already running at full slot capacity are not misreported as CPU-held idle capacity).
   Because `workers` was sorted by `worker_id` at the start of the pass, the collected IDs form a canonical sorted `Vec<Uuid>`.
4. Compare the new busy-worker ID list (`next_ids`, empty when `saturated_skips == 0 || busy_workers.is_empty()`) against `host_cpu_backpressure_reported`:
   - If `*prev == next_ids`, return without logging (preventing log flooding across periodic ticks or fluctuating CPU readings above `max_host_cpu_pct`).
   - Otherwise replace `*prev = next_ids`:
     - When `next_ids` is non-empty, emit `warn!` containing `"saturated"`, the `skipped` count, `max_host_cpu_pct`, and `busy_workers` `(worker_id, name, cpu_usage_pct, load)`.
     - When `next_ids` becomes empty after being active, emit `info!` (`"Host-CPU backpressure holdback cleared"`, omitting the word `"saturated"`).

## Compatibility

- Zero wire protocol changes (`PROTOCOL_VERSION_CURRENT = 10`) and zero store format changes (`STORE_FORMAT_VERSION = 4`).
- Compiles and behaves identically under both default features and `--no-default-features`.

## Security

- Logs only worker UUIDs, worker names, CPU percentage, active load, and skip counts already visible to the master operator; introduces no new network endpoint or secret exposure.

## Test plan

- Unit tests in `crates/master/src/scheduler.rs`:
  - `sched5_emits_once_per_busy_worker_set_change_and_clears_on_recovery`: verifies 1 `warn!` line on first saturated pass, no additional lines across repeated ticks or CPU fluctuations while the busy-worker set is unchanged, a 2nd `warn!` line when a second connected busy worker joins the set, silent clearing at `info!` without `"saturated"` on recovery, and re-emission on subsequent saturation.
  - `sched5_ignores_non_saturated_skips_and_disconnected_workers`: verifies 0 `"saturated"` lines for `NoGpuWorkersAvailable` on a CPU-busy non-GPU worker, `Disconnected` CPU-busy workers, and slot-full non-CPU-busy workers.
- Integration test in `tests/e2e_cluster.rs`:
  - `sched5_backpressure_warns_once_and_dispatches_on_recovery`: verifies end-to-end with `MasterServer::spawn_with_config` and a mock wire worker whose heartbeat transitions from `92.0%` CPU (`max_host_cpu_pct = 50.0`) to `15.0%` CPU.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SCHED-5 | Host-CPU backpressure holdback visibility | `crates/master/src/scheduler.rs`, `docs/reference.md`, `tests/e2e_cluster.rs` | Design record written, connected-only transition reporting implemented without flooding, unit and integration tests pass | Implemented |

## Status log

- 2026-11-21: Created design record and implemented transition-triggered host-CPU backpressure visibility in `WorkloadScheduler::schedule_batch` (`SCHED-5`).
