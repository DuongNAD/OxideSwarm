# 0014: Worker `--max-concurrency` Slots Tag on the Master

- **Status:** Implemented (`SCHED-1`)
- **Date:** 2026-11-02

## Context & Problem

Workers enforce their concurrent task execution limit via a local semaphore (`crates/worker/src/client.rs:912-924`) sized from `--max-concurrency <N>` (or `default_max_concurrency(cpu_cores)` when cores are auto-detected). However, the master's `SchedulerConfig::effective_max_concurrency` (`crates/master/src/scheduler.rs:127`) computed the worker's slot limit solely from `worker.capabilities.cpu_cores * core_concurrency_multiplier`.

As a result, an 8-core worker started with `--max-concurrency 1` (or `2`) could be assigned up to 8 tasks by the master. The excess tasks waited in state `Scheduled` behind the worker's local semaphore instead of being routed to an idle peer worker.

## Decision

1. **Reserved `slots:<n>` worker tag (`crates/core/src/placement.rs`):**
   - Add `pub const SLOTS_TAG_PREFIX: &str = "slots:";` and include it in `RESERVED_TAG_PREFIXES` so user-supplied `--tag slots:...` entries are rejected by `validate_user_tag` and hidden from user-tag listings in `oxideswarm doctor`.
   - Add `pub fn advertised_slots(caps: &WorkerCapabilities) -> Option<usize>` which parses a positive integer (`>= 1`) from the worker's `slots:<n>` tag.
2. **Worker advertisement (`crates/worker/src/client.rs`):**
   - Compute `concurrency_permits` when initializing `WorkerClient` and include `slots:<concurrency_permits>` in `advertised_tags(&config.tags, concurrency_permits)` right after `system_tags()` (`os:<os>`, `arch:<arch>`).
3. **Master scheduler (`crates/master/src/scheduler.rs`):**
   - In `SchedulerConfig::effective_max_concurrency`, use `rusty_grid_core::placement::advertised_slots(&worker.capabilities)` when present (still bounded by `self.max_tasks_per_worker` if configured), falling back to `(cpu_cores * core_concurrency_multiplier).ceil().max(1)` when a worker does not advertise `slots:<n>`.

## Compatibility

No wire protocol or `bincode` schema changes are required because `WorkerCapabilities::tags` is already transmitted in `WorkerMessage::Register`. Older workers without a `slots:<n>` tag continue to use the `cpu_cores * core_concurrency_multiplier` calculation.
