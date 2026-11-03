# 0009: Process supervision for Blender and 3D workloads

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-03

## Summary

Generative 3D mesh workloads (the entrypoint `engines/generative_3d/entrypoint.py` loads no model and
writes a fixed octahedron) and Blender mesh auto-rigging run as external subprocesses. Previously,
these workloads relied on ad-hoc runner logic and an isolated Win32 `JobObject` wrapper, bypassing
the worker's unified task supervision. This design unifies 3D and Blender execution under
`TaskRunner::execute_process_supervised`, standardizes process tree supervision across Windows and
Unix, parses progress markers post-exit, and strictly sanitizes cluster secrets from child process
environments.

## Context

On `master` at `e34788a`:
- Generative 3D tasks created an isolated `JobObject` instance (`crates/worker/src/job_object.rs`) on
  Windows, duplicating containment logic and bypassing Unix process group supervision.
- 3D tasks ran Python at normal priority with both `RUSTY_GRID_TASK_ID` and `RUSTY_GRID_WORKER_ID`
  set, but without stripping cluster secret environment variables (`OXIDESWARM_SECRET`,
  `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`). There were no staging calls in the 3D path.
- Blender auto-rigging (`crates/worker/src/autorig/blender_provider.rs`) spawned `tokio::process::Command`
  directly at normal priority, with no task ID, no worker ID, and no throwaway `TaskRunner`.
- Version probes (`blender --version`, `python --version`) executed via plain `std::process::Command`,
  inheriting sensitive cluster secret environment variables.

## Goals

- G1: Unified process tree containment across Windows (Job Objects) and Unix (process groups),
  guaranteeing zero orphaned child or grandchild processes on cancellation, timeout, or task
  future abort/drop.
- G2: Consistent CPU priority inheritance adhering to `ProcessPriority` (`normal`, `below-normal`, `idle`).
- G3: Strict environment hygiene: child tasks receive `RUSTY_GRID_TASK_ID`, `RUSTY_GRID_WORKER_ID`,
  and `RUSTY_GRID_SANDBOX_DIR`. Cluster secrets are stripped from subprocesses and from Python and
  Blender version probes, while system discovery probes (`nvcc`, `nvidia-smi`, `sysctl`, `pmset`,
  `sw_vers`, `dumpsys`, `getprop`) retain their environment.
- G4: Progress checkpointing parsed from captured stdout following process termination in generative 3D workloads.

## Non-goals

- Streaming stdout checkpoints while a process is running (subprocess execution completes before stdout
  is evaluated).
- Memory or disk quota enforcement (resource constraints remain limited to CPU priority and wall-clock timeout).

## Design

### D1: Supervised execution via TaskRunner

All 3D model executions and Blender auto-rigging jobs route through `TaskRunner::execute_process_supervised`.
This helper encapsulates:
- Registering child processes into `ProcessTree` (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` on Windows;
  `process_group(0)` on Unix).
- Setting process priority classes (`BELOW_NORMAL_PRIORITY_CLASS`, `IDLE_PRIORITY_CLASS` or POSIX `nice`).
- Spawning child processes with piped stdout/stderr and asynchronous cancellation monitoring.
- Concurrent non-blocking stdout/stderr readers, preventing OS pipe deadlocks.
- Two-phase teardown: graceful termination followed by uncatchable tree kill on timeout or cancellation.

The standalone `crates/worker/src/job_object.rs` module and `TaskRunner::terminate_process_tree` are
deleted as dead code. Primary output asset loading is decoupled into `read_output_file(path)`, which
queries `tokio::fs::symlink_metadata` to refuse symbolic links, non-regular files, and files exceeding
the 64 MiB wire protocol frame limit (`MAX_FRAME_SIZE`) before reading them into memory.

### D2: Post-execution progress checkpointing

Captured stdout is capped at `max_output_bytes` (default 10 MiB), so it is not captured in full if a
process generates voluminous output.
- In the generative 3D path, captured stdout is parsed following process completion for
  `[PROGRESS: <val>]` markers, which are dispatched as `WorkerMessage::Checkpoint` deltas via
  `checkpoint_tx`. Cancelled or timed-out tasks discard checkpoints.
- In Blender auto-rigging, progress markers are parsed and logged to tracing (`info!`) after the
  process exits; no checkpoints are sent over the wire.

### D3: Environment isolation and secret stripping

Subprocess environments for task executions are stripped of all cluster authentication tokens
(`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`) via the `env_remove` loop in
`execute_process_supervised`, while version inspection probes for Python and Blender use
`rusty_grid_core::cluster_auth::probe_command`. Other host capability discovery probes (`nvcc`,
`nvidia-smi`, `sysctl`, `pmset`, `sw_vers`, `dumpsys`, `getprop`) inherit the process environment
unchanged. Tasks receive standard environment variables:
- `RUSTY_GRID_TASK_ID`: Matching task UUID.
- `RUSTY_GRID_WORKER_ID`: Matching worker node UUID.
- `RUSTY_GRID_SANDBOX_DIR`: Absolute path to task workspace.

## Known limits

- **Wire frame limit on output assets**: `read_output_file` queries `tokio::fs::symlink_metadata`
  first, refusing symbolic links, non-regular files (such as FIFOs or directories), and files
  exceeding the 64 MiB wire protocol frame limit (`MAX_FRAME_SIZE` in `rusty_grid_core::protocol`)
  before reading them into memory (BUG-10). Assets larger than 64 MiB cannot be transmitted inline
  in `TaskResult`.
- **Worker crash on Unix**: On Unix, task processes run under process groups (`killpg`). Normal
  cancellation, watchdog timeout, or task drop terminates the process group. However, after an
  abrupt worker crash (SIGKILL/panic) on Unix, nothing kills the process groups; they are left
  running until they exit (`crates/worker/src/process_tree.rs:24`).

## Compatibility

Wire protocol message structures (`WorkerMessage`, `TaskResult`) remain compatible. Existing tasks
specifying `Generative3DSpec` or `AutoRigSpec` execute without modifications.
A default worker now runs Python and Blender at `below-normal` priority (`ProcessPriority::BelowNormal`),
whereas `master` ran them at normal priority. Consequently, these workloads yield CPU cycles to
interactive foreground processes and run slower on a busy machine.

## Security

Fulfills the CPU priority specification in `docs/SECURITY.md:164` for Blender.
The three cluster secret environment variables (`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`,
`OXIDE_MESH_TOKEN`) are no longer passed in the environment of child processes or Python/Blender
version probes. However, because tasks execute under the worker's OS account without containerization,
a task process can still read the worker account's secret file on disk, as documented in
`docs/SECURITY.md:35`.

## Test plan

- `crates/worker/tests/test_sec5_hardened_runner.rs`: Verifies priority inheritance (`nice` / Windows
  priority classes), cluster secret stripping from child environments and version probes, large stderr
  non-blocking drain, and process group / tree termination on cancellation.
- `crates/worker/tests/test_challenger_m2_job_object.rs`: Validates grandchild process tree cleanup
  under cancellation, watchdog timeout, and future drop via `TaskRunner` and `ProcessTree`, plus an
  OS-level abrupt process termination crash test via Python.
- `crates/worker/tests/test_challenger_m3_blender_job_object.rs`: Validates host Blender headless
  execution, process tree termination on cancellation, and graceful fallback to pure-Rust auto-rigging
  when Blender is missing or exits abnormally.
- `crates/worker/tests/test_gen3d_adversarial_stress.rs`: Tests generative 3D SDF preset resolution,
  latency benchmarks, manifoldness, binary glTF conformance, PNG encoding, and Blender headless
  import compatibility.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Hardened process runner integration | `runner.rs`, `blender_provider.rs` | Blender and 3D use `execute_process_supervised` | Implemented |
| W2 | Secret stripping on probes | `cluster_auth.rs`, `capabilities.rs`, `blender_provider.rs`, `gen3d/runner.rs` | Python/Blender probes stripped via `probe_command` | Implemented |
| W3 | Output file reading helper | `runner.rs` | `read_output_file` refuses symlinks, non-regular files, and files above `MAX_FRAME_SIZE` | Implemented |
| W4 | Clean up dead code & update headers | `runner.rs`, `*_job_object.rs` | `terminate_process_tree` removed; headers updated | Implemented |
| W5 | Test suite verification | `test_sec5_*.rs`, `test_challenger_*.rs` | All suites pass on Windows; CI runs `test_sec5_hardened_runner` on Linux and macOS | Implemented |

## Status log

- 2026-11-01: Implemented process supervision for 3D and Blender workloads via TaskRunner (SEC-5).
- 2026-11-01: Updated design record for second round review: corrected master baseline, Unix crash semantics, probe secret isolation, and output frame limit.
- 2026-11-01: Updated design record for round 3 review: corrected default output buffer limit, metadata regular file check, task env stripping vs probe_command distinction, CI scope, and W2 file list.
- 2026-11-03: Guarded `read_output_file` with `symlink_metadata` to refuse symbolic links and files exceeding `MAX_FRAME_SIZE` before reading (BUG-10).

