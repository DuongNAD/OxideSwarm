# 0090: A worker hands a task back when its sandbox base is unusable

Status: Implemented
Created: 2026-11-17    Updated: 2026-11-17

## Summary

When a worker cannot use its sandbox base for an assigned task, it answers with a refusal the master
already knows how to handle: the task goes to another worker and no execution retry is charged. No
wire change (protocol stays 10) (`BUG-25`).

## Context

- At assignment, at the input fetch and in `Sandbox::create`, a worker whose base is missing, not
  writable or no longer private answered with an ordinary failed `TaskResult` ("Sandbox base: ...",
  exit code 1, run time 0).
- The master treats a result as a worker refusal (`is_worker_rejection`) only when its text holds one
  of `WORKER_REJECTION_MARKERS`. The sandbox text had none, so it counted as an execution failure, and
  `retry_on_execution_failure` is `false` by default: the task ended `Failed` with retries left while
  another worker could have run it (seen in TEST-33).

## Decisions

- **D1** The three refusals use the text `Task rejected by worker: sandbox base unusable: <reason>`
  (`rusty_grid_core::sandbox_base_refusal`, constant `SANDBOX_BASE_REFUSAL_PREFIX`), in both the error
  and stderr, exit code 1, `execution_time_ms == 0`. It contains `rejected by worker`, which every
  master since the battery refusal matches, so a 0.4.x master hands the task back as well; the master
  of this change also lists the full prefix in `WORKER_REJECTION_MARKERS`. No message enum or
  mirror changes.
- **D2** What counts: `BaseDirUnusable` and `BaseDirUnsafe` from the per-task re-check (at
  assignment and again before the input fetch), and from `Sandbox::create` also an I/O error making the
  lock or task directory under the base (`SandboxError::is_create_refusal`). A failed blob fetch is
  still an input failure, not a refusal.
- **D3** The master needs no new guard. A refusal already records the worker in the task's
  `avoid_workers`, counts against `MAX_WORKER_REJECTIONS_PER_TASK` (3) and the failover budget, and
  requeues; the scheduler prefers other workers and uses an avoided one only when no other fits. A
  worker with a broken base therefore receives each task at most a few times, and a cluster of only
  broken workers fails the task after three refusals instead of looping.
- **D4** The per-task re-check is repeated once after 250 ms before refusing, since on Windows a base
  being removed was seen to come back (the re-check re-creates it). The delay applies only to a base
  that already failed.

## Tests

- `queue::tests::test_worker_rejection_detection_matches_worker_messages_only` (exact message).
- `crates/master/tests/sandbox_base_hand_back.rs`: two workers, the base of one replaced by a file
  after it registered, eight tasks with `max_retries` 3 all complete on the other worker.
