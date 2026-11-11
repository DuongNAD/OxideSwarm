# 0060: Place tasks by measured worker speed and re-run stragglers (`SCHED-4`)

Status: Planned
Created: 2026-11-10    Updated: 2026-11-10

## Summary

The master learns how long each worker takes for each kind of task, from the results it already
receives. Placement then leaves a task queued for a faster worker when the slow one would clearly
finish later, instead of filling every free slot. A task the submitter marks as safe to repeat can
also get a second copy on an idle faster worker when it overruns; the first result wins. Tasks with
no history keep today's placement exactly.

## Context

1. **Placement ignores speed.** `Engine::rank` (`crates/master/src/scheduler.rs`) orders the
   candidates of a task by load: `LeastLoaded` by running tasks, `WeightedLeastLoaded` (the default)
   by tasks per core plus half the reported CPU use, with GPU preference and core count as tie
   breaks. `Engine::candidates` admits every worker with a free slot. Nothing in the master records
   how long a worker took.
2. **The cost, measured on 2026-11-10.** Two Windows workers over direct P2P links: a desktop
   (i5-14600KF, 20 threads, 10 slots) and a laptop (i7-1355U, 2 performance and 8 efficient cores,
   12 threads, 6 slots; the USB kit gives half the threads as slots).

   | Run | Result |
   |---|---|
   | One PowerShell sum task, alone | desktop about 2.3 s, laptop 3.5 to 8.8 s |
   | 32 tasks, no worker pinned | 26 on the desktop (2.4 to 4.6 s each), 6 on the laptop (about 51 s each); batch 52 s |
   | The same tasks pinned to the laptop, 2 / 4 / 6 at once | 2.9 s / 12.5 s / 39.6 s each, about 112 MB each |
   | Verified 8192 GEMM | RTX 5060 Ti 855 ms, MX550 11.9 s |
   | 12 GEMM 4096, no worker pinned | 9 on the RTX 5060 Ti, 3 on the MX550, batch 6.4 s |

   With 10 slots the desktop alone would finish the 32 tasks in about 12 s, so the slow worker's
   6 tasks more than quadrupled the batch time.
3. **Results already carry the time.** Every `TaskResult` variant has `duration_ms`
   (`crates/core/src/task.rs`), and the master handles each result in `server.rs`/`session.rs`
   before the queue records it.

## Goals

- G1. A batch of identical tasks on workers of very different speed finishes close to the time the
  fast workers alone would need, and never later than today.
- G2. A task that is safe to repeat and stuck on a slow or overloaded worker finishes on another
  worker without waiting for the slow one.
- G3. Tasks with no history, single tasks on an idle cluster, and every existing placement rule
  (placement constraints, GPU routing, workspace affinity, `hold_back`, mobile tiers) behave exactly
  as today.
- G4. The measured speed is visible to the operator.

## Non-goals

- Predicting the run time of a task never seen before, or modelling the task itself.
- Persisting the history across master restarts (it rebuilds in minutes).
- Repeating tasks that were not marked safe to repeat.
- Changing the worker's own slot count; that is the USB kit's job (`USB-4`).

## Design

### D1: Speed history

`SpeedBook` in a new `crates/master/src/speed.rs`, owned by the scheduler:

- **Key** `TaskKey`: a 64-bit hash of the task type and, for command, shell and compile tasks, the
  program and arguments; for GPU tasks the matrix edge; for 3D and rigging tasks the task type only.
  Inputs and environment are not part of the key.
- **Entry** per (worker id, key): an exponential moving average of `duration_ms` with weight 0.3,
  the sample count, and the time of the last sample. Only successful results count; a result whose
  `duration_ms` is 0 or above the task's timeout is ignored.
- **Bounds:** at most 4,096 keys, least recently used evicted first, and at most 64 workers per key.
  A worker that leaves (its sessions swept and its registry entry pruned) loses its entries.
- **Update** happens where the master records a terminal result, under the scheduler's own lock,
  not the queue lock.

### D2: Placement

In `Engine::place`, after `candidates` and before `rank`, for a task whose key has at least
2 samples on at least one matching worker:

1. `best` is the matching worker (ignoring free slots) with the lowest average for the key.
2. For each candidate `c` with a free slot: `expected(c)` is its average, or `best`'s average
   times 1.0 when `c` has no samples (unknown workers are treated as equal, so they still get work
   and get measured).
3. `ready(best)` is 0 when `best` has a free slot in this pass, else the shortest time left among
   its running tasks of known key: its average minus how long the task has run, at least 0.
4. If `expected(c) >= 1.5 * (ready(best) + avg(best))` for every candidate, the task stays queued
   with a new skip reason `WaitingForFasterWorker`, and `hold_back` reserves `best` for it in this
   pass. Otherwise the candidates are re-ordered by expected finish time
   (`ready + expected`), with today's `rank` order as the tie break.
5. A task never waits longer than its expected time on the slow worker: once it has waited that
   long, the rule above is skipped for it.

The factor 1.5 and the minimum of 2 samples are `SchedulerConfig` fields (`speed_hold_factor`,
`speed_min_samples`) with these defaults; `speed_hold_factor = 0` turns D2 off.

### D3: Straggler re-run (opt-in)

- `submit --repeatable` sets the task tag `opt:repeatable`. `opt:` becomes the tenth reserved tag
  prefix in `crates/core/src/placement.rs`, set only through this flag.
- The reaper's periodic pass looks for running repeatable tasks whose elapsed time is above
  3 times the average of the fastest matching worker for their key and above 10 s. If a matching
  worker other than the current one has a free slot, the master assigns a copy there as a second
  attempt of the same task.
- The first successful result completes the task; the master cancels the other copy through the
  existing cancel path, and its later result is discarded. A failure of one copy does not fail the
  task while the other runs.
- At most one extra copy per task, and at most 10 % of the cluster's slots used for copies.

### D4: Visibility

`ListWorkers` and `ClusterStatus` add a master tag `speed:<factor>` per worker: the median, over
keys with samples on both, of this worker's average divided by the fastest worker's (`speed:1.0` is
the fastest). `speed:` becomes a reserved prefix as well. The dashboard shows the factor in the
worker table.

## Compatibility

- D1, D2 and D4 change only the master. Workers and clients of protocol 7 and 8 are unaffected;
  the new tags are strings in the existing `tags` field.
- D3 adds a CLI flag and a reserved tag prefix. An older master ignores `opt:repeatable` as an
  ordinary tag, so the task simply runs once; a user tag that starts with `opt:` or `speed:` is
  refused from this version on (`Task::validate`).
- No wire or protocol version change.

## Security

- A worker controls its own reported `duration_ms`. A worker that reports fast times attracts more
  work; one that reports slow times gets less. Neither can see or change another worker's tasks, so
  the worst case is a skewed share of work, which a worker with a valid key can already cause by
  advertising more slots.
- D3 runs a task twice. It is opt-in per task because a task with side effects (a push, a payment,
  a file outside its sandbox) must not run twice.

## Test plan

- Unit (`speed.rs`): average and sample count, the bounds and eviction, ignored samples.
- Unit (`scheduler.rs`): one fast and one slow worker with a seeded `SpeedBook`; a batch of 32
  identical tasks places so that the computed finish time is at least 30 % below today's order;
  a key with no history and a single task on an idle cluster place exactly as today; placement
  constraints, GPU routing and workspace affinity still win; the wait cap of D2 step 5 holds.
- Integration (new `tests/sched4_speed_aware.rs`, registered under `rusty_grid_master`): two
  in-process workers, one with a test hook that sleeps 5 times longer per task; a batch finishes
  within 1.5 times the fast worker's own time; with `speed_hold_factor = 0` the old timing returns.
- Integration for D3: a repeatable task stuck on a slow worker completes on the other worker; the
  slow copy is cancelled and its result discarded; a task without `--repeatable` is never copied.
- Live: the two-machine batch of the Context table finishes within 1.5 times the desktop's own time.
- All existing scheduler tests pass unchanged.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SCHED-4a | Speed history | `crates/master/src/speed.rs`, `server.rs` | Unit tests of D1 | Planned |
| SCHED-4b | Speed-aware placement | `crates/master/src/scheduler.rs` | Unit and integration tests of D2 | Planned |
| SCHED-4c | Straggler re-run | `crates/master/src/reaper.rs`, `queue.rs`, `crates/core/src/placement.rs`, `crates/cli/src/cli_app.rs` | Integration tests of D3 | Planned |
| SCHED-4d | Speed in `workers` and the dashboard | `registry.rs`, `web_ui.rs`, `dashboard.html` | `workers --json` shows `speed:` | Planned |

## Status log

- 2026-11-10: Created from the two-machine trial measurements in Context. Planned.
