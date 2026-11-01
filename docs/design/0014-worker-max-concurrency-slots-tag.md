# 0014 — Worker `--max-concurrency` (`slots:<n>`) Tag on the Master

**Status:** Implemented (`SCHED-1`)  
**Created:** 2026-11-02  
**Updated:** 2026-11-02  

## Summary

Propagates a worker's effective task-concurrency semaphore limit (`--max-concurrency <N>` or default `cores - 1` headroom) to the master via a reserved `slots:<n>` tag in `WorkerCapabilities::tags`, and enforces it in the master scheduler as a concurrent task-count cap (`max_tasks`) alongside the existing CPU-core slot budget (`max_slots`), without changing the wire protocol or `bincode` layout.

## Context

Workers enforce their concurrent task execution limit via a local semaphore (`crates/worker/src/client.rs`) sized from `--max-concurrency <N>` (or `default_max_concurrency(cpu_cores)` when cores are auto-detected, or `cores` when `--cores` is set explicitly). Previously, the master scheduler knew only `worker.capabilities.cpu_cores` and `SchedulerConfig::core_concurrency_multiplier`, and had no visibility into the worker's semaphore permit count.

As a result, an 8-core worker started with `--max-concurrency 1` (or `2`) could be assigned up to 8 tasks at once by the master. The excess tasks waited inside the worker behind its local semaphore instead of spilling over to an idle peer worker.

## Goals

- Advertise the worker's effective concurrent task limit in `WorkerCapabilities::tags` as `slots:<n>` (`n >= 1`) without altering `bincode` positional wire structs.
- Reserve the `slots:` tag prefix (`SLOTS_TAG_PREFIX`) so user `--tag slots:...` flags are rejected and stripped from user tag lists.
- Enforce both the CPU-core slot budget (`max_slots`, compared against `AssignedLoad::slots` where each task weighs `cpu_cores`) and the concurrent task-count cap (`max_tasks`, from `slots:<n>`, compared against `AssignedLoad::tasks`) in the master scheduler (`F34-1`).

## Non-goals

- Changing `WorkerCapabilities` struct fields on the wire.
- Adding a CLI flag for `SchedulerConfig::core_concurrency_multiplier`.

## Design

### 1. Reserved `slots:<n>` Worker Tag (`crates/core/src/placement.rs`)

- Defines `pub const SLOTS_TAG_PREFIX: &str = "slots:";` and adds it to `RESERVED_TAG_PREFIXES` (now six reserved prefixes: `os:`, `arch:`, `slots:`, `link:`, `id:`, `state:`).
- `validate_user_tag` rejects any user tag starting with `slots:`, and `user_tags_of` hides `slots:` from user-tag summaries in `oxideswarm doctor`.
- `pub fn advertised_slots(caps: &WorkerCapabilities) -> Option<usize>` scans `caps.tags` for `slots:<n>` and returns `Some(n)` when `n >= 1` (`slots:0` and malformed values are ignored).

### 2. Worker Advertisement (`crates/worker/src/client.rs`)

- `WorkerClient::new` computes `concurrency_permits` first (`config.max_concurrency`, else `cores` when `--cores` was explicitly overridden, else `default_max_concurrency(cpu_cores)` which reserves 1 core on multi-core hosts) and passes it to `advertised_tags(&config.tags, concurrency_permits)`.
- `advertised_tags` emits `os:<os>`, `arch:<arch>`, and `slots:<n>`, then appends user tags while filtering out any duplicate or `slots:`-prefixed entry (`F34-3`).

### 3. Two-Limit Master Scheduling (`crates/master/src/scheduler.rs`)

- **Core budget (`max_slots`):** `SchedulerConfig::effective_max_concurrency(worker)` continues to return the CPU-core budget `(cpu_cores * core_concurrency_multiplier).ceil().max(1)` (bounded by `max_tasks_per_worker`). A task requesting `cpu_cores` occupies `weight = cpu_cores.max(1)` core slots (`AssignedLoad::slots`).
- **Task-count cap (`max_tasks`):** `SchedulerConfig::effective_max_tasks(worker)` returns `advertised_slots(&worker.capabilities)` when present (bounded by `max_tasks_per_worker`), falling back to `effective_max_concurrency(worker)` when `slots:<n>` is absent. Each task occupies `1` task slot (`AssignedLoad::tasks`).
- Keeping `max_slots` (cores) separate from `max_tasks` (task permits) ensures that a default 8-core worker (`slots:7`) or 10-core worker (`slots:9`) can still run two half-machine tasks (two 4-core tasks on 8 cores, or two 5-core tasks on 10 cores) side by side (`4 + 4 <= 8` cores and `2 <= 7` tasks), while an 8-core worker started with `--max-concurrency 1` (`slots:1`) receives at most 1 task at a time (`F34-1`).
- **Interaction with `core_concurrency_multiplier` (`F34-2`):** Because every current worker advertises `slots:<n>`, `core_concurrency_multiplier` scales the CPU-core budget (`max_slots`) on current workers and only determines the task-count cap (`max_tasks`) as a fallback for older workers or synthetic test `WorkerInfo` structs that do not carry a `slots:<n>` tag.

## Compatibility

- **Wire compatibility:** No wire format or `bincode` changes; `WorkerCapabilities::tags` already existed in `WorkerMessage::Register`.
- **Old worker with new master:** An older worker that does not advertise `slots:<n>` falls back to `effective_max_concurrency(worker)` for both `max_slots` and `max_tasks`.
- **New worker with old master:** An older master ignores the unknown `slots:<n>` tag (except that it appears in `WorkerCapabilities::tags` unless filtered by `user_tags_of`).
- **Default multi-core workers (`cores - 1` permits):** A default worker on an $N$-core host ($N \ge 2$) advertises `slots:<N-1>`, so at most $N - 1$ 1-core tasks are dispatched to it at once (matching its local semaphore), while wide multi-core tasks continue to use the full $N$-core budget as long as the concurrent task count does not exceed $N - 1$.

## Security

- `slots:` is a reserved worker-capability prefix validated on the client/worker and parsed with bounded integer parsing (`usize`, `>= 1`). A worker can only influence its own scheduling capacity on the master, which it already controls via `WorkerCapabilities::cpu_cores`.

## Test plan

- `cargo test -p rusty_grid_core --lib` (`placement::tests::test_advertised_slots_parses_valid_tag_and_ignores_invalid`, `test_user_tag_validation`)
- `cargo test -p rusty_grid_worker --lib` (`client::tests::test_worker_advertises_system_tags_then_user_tags`, including filtering a user-supplied `slots:99` tag)
- `cargo test -p rusty_grid_master --lib` (`scheduler::tests::test_worker_slots_tag_caps_concurrency_and_spills_overflow_to_idle_peer`, covering `slots:2` spilling to a peer, `slots:7` on 8 cores running two 4-core tasks concurrently, and `slots:1` on 8 cores serializing 4-core tasks)
- `cargo test -p rusty_grid_master --test m4_scheduling`

## Work items

- `SCHED-1`: Advertise worker `--max-concurrency` via `slots:<n>` tag and honour it in master scheduling.

## Status log

- 2026-11-02: Implemented `SCHED-1` across `crates/core`, `crates/worker`, and `crates/master`; addressed PR #34 review findings `F34-1` through `F34-4` by separating `max_slots` (core budget) and `max_tasks` (`slots:<n>` task-count cap).
