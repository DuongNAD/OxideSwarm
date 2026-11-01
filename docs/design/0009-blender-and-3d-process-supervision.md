# 0009: Process supervision for Blender and 3D workloads

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-01

## Summary

Generative 3D model inference (TripoSR, Shap-E) and Blender mesh auto-rigging run as external
subprocesses. Previously, these workloads relied on ad-hoc runner logic and an isolated Win32
`JobObject` wrapper, bypassing the worker's unified task supervision. This design unifies 3D
and Blender execution under `TaskRunner::execute_process_supervised`, standardizes process tree
supervision across Windows and Unix, ensures output files are collected without arbitrary size
caps while following symbolic links, parses progress markers post-exit, and strictly sanitizes
cluster secrets from child process environments.

## Context

Prior to SEC-5, 3D workloads and Blender auto-rigging exhibited architectural gaps:
- 3D tasks created an isolated `JobObject` instance (`crates/worker/src/job_object.rs`), duplicating
  containment logic and bypassing Unix process group supervision.
- Blender auto-rigging (`crates/worker/src/autorig/blender_provider.rs`) built a throwaway `TaskRunner`,
  forcing execution at default `BelowNormal` priority (nice 10 / `BELOW_NORMAL_PRIORITY_CLASS`) and
  ignoring `--task-priority`. It also assigned a nil worker ID and random task ID.
- Version probes (`blender --version`, `python --version`) executed via plain `std::process::Command`,
  inheriting sensitive environment variables (`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`).
- Output file collection across generative 3D workloads was fragmented between inline reading and
  unintended staging calls.

## Goals

- G1: Unified process tree containment across Windows (Job Objects) and Unix (process groups),
  guaranteeing zero orphaned child or grandchild processes on cancellation, timeout, or crash.
- G2: Consistent CPU priority inheritance adhering to `ProcessPriority` (`normal`, `below-normal`, `idle`).
- G3: Strict environment hygiene: child tasks receive `RUSTY_GRID_TASK_ID`, `RUSTY_GRID_WORKER_ID`,
  and `RUSTY_GRID_SANDBOX_DIR`, while cluster secrets are strictly stripped from all subprocesses and probes.
- G4: Unbounded output file reads with symbolic link resolution matching master grid behavior.
- G5: Progress checkpointing parsed from captured stdout following process termination.

## Non-goals

- Streaming stdout checkpoints while a process is running (Blender execution completes before stdout
  is evaluated for checkpoints).
- Memory or disk quota enforcement (resource constraints remain limited to CPU priority and wall-clock timeout).

## Design

### D1: Supervised execution via TaskRunner

All 3D model executions and Blender auto-rigging jobs route through `TaskRunner::execute_process_supervised`.
This helper encapsulates:
- Registering child processes into `ProcessTree` (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` on Windows;
  `process_group(0)` on Unix).
- Setting process priority classes (`BELOW_NORMAL_PRIORITY_CLASS`, `IDLE_PRIORITY_CLASS` or POSIX `nice`).
- Spawning child processes with piped stdout/stderr and asynchronous cancellation monitoring.
- Two-phase teardown: graceful termination followed by uncatchable tree kill on timeout or cancellation.

The standalone `crates/worker/src/job_object.rs` module and `TaskRunner::terminate_process_tree` are
deleted as dead code.

### D2: Output artifact collection with symlink following

Output model files (e.g. `output.glb`) are collected after process completion:
- The path is verified using `Path::is_file()`, which traverses and resolves symbolic links.
- File contents are loaded via `tokio::fs::read` without arbitrary buffer caps, ensuring large 3D
  models are returned intact in `TaskResult.asset_bytes`.

### D3: Post-execution progress checkpointing

To prevent pipe deadlocks and synchronization races, Blender and 3D workloads capture stdout in full.
Upon process completion, stdout is parsed for `[PROGRESS: <val>]` markers:
- Markers are transformed into sequential `WorkerMessage::Checkpoint` deltas and dispatched via
  `checkpoint_tx`.
- Cancelled or timed-out tasks discard checkpoints and immediately destroy the sandbox.

### D4: Environment isolation and secret stripping

Subprocess execution environments and version inspection probes are stripped of all cluster authentication
tokens (`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`). Tasks receive the standard
Tier D environment:
- `RUSTY_GRID_TASK_ID`: Matching task UUID.
- `RUSTY_GRID_WORKER_ID`: Matching worker node UUID.
- `RUSTY_GRID_SANDBOX_DIR`: Absolute path to task workspace.

## Compatibility

Wire protocol message structures (`WorkerMessage`, `TaskResult`) remain 100% compatible. Existing
tasks specifying `Generative3DSpec` or `AutoRigSpec` execute without modifications.

## Security

Fulfills the CPU priority specification in `docs/SECURITY.md:164` for Blender. Eliminates secret
leakage to external Python and Blender scripts.

## Test plan

- `crates/worker/tests/test_sec5_hardened_runner.rs`: Verifies priority inheritance, secret stripping,
  large stderr non-blocking drain, and grandchild process termination on abort.
- `crates/worker/tests/test_challenger_m2_job_object.rs`: Validates grandchild cleanup under cancellation,
  watchdog timeout, and future drop.
- `crates/worker/tests/test_challenger_m3_blender_job_object.rs`: Validates host Blender headless execution,
  graceful fallback, and priority propagation.
- `crates/worker/tests/test_gen3d_adversarial_stress.rs`: Stress-tests concurrency and process teardown.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Hardened process runner integration | `runner.rs`, `blender_provider.rs` | Blender and 3D use `execute_process_supervised` | Implemented |
| W2 | Secret stripping on probes | `cluster_auth.rs`, `blender_provider.rs`, `runner.rs` | Probes see 0 cluster secrets | Implemented |
| W3 | Uncapped symlink output collection | `runner.rs` | `Path::is_file()` reads full asset | Implemented |
| W4 | Clean up dead code & update headers | `runner.rs`, `*_job_object.rs` | `terminate_process_tree` removed; headers updated | Implemented |
| W5 | Test suite verification | `test_sec5_*.rs`, `test_challenger_*.rs` | All suites pass on Windows | Implemented |

## Status log

- 2026-11-01: Implemented process supervision for 3D and Blender workloads via TaskRunner (SEC-5).
