# 0009: Process supervision for Blender and 3D workloads

Status: Implemented
Created: 2026-11-01    Updated: 2026-11-03

## Summary

Neural 3D mesh workloads (`triposr` via `engines/generative_3d/entrypoint.py`, which refuses execution
when checkpoint weights are not installed, while `auto` and `sdf` execute in-process via pure-Rust Tier 0
SDF per Design 0017) and Blender mesh auto-rigging run as external subprocesses. Previously,
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
opens the file first without following symbolic links or blocking on FIFOs (`O_NOFOLLOW | O_NONBLOCK`
on Unix and `FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS` on Windows via
`OpenOptionsExt::custom_flags`), inspects `file.metadata()` on the open handle to refuse symbolic
links/reparse points, non-regular files, and files exceeding `MAX_FRAME_SIZE` (`64 * 1024 * 1024 = 67,108,864`
bytes, 64 MiB) before reading, and reads through a capped reader (`file.take(MAX_FRAME_SIZE as u64 + 1)`)
so a file that grows past `MAX_FRAME_SIZE` during the read is also refused (`"is more than 67108864 bytes"`).
In the generative 3D path (`crates/worker/src/runner.rs:2126`), a `read_output_file` refusal fails the
task with `EXIT_CODE_GENERAL_ERROR`; in Blender auto-rigging (`crates/worker/src/runner.rs:2340-2366`),
a `read_output_file` refusal logs a `warn!` and falls back to the built-in pure-Rust auto-rigging
engine without telling the caller in `TaskResult`.

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

- **Wire frame limit on output assets**: `read_output_file` opens the output path first with
  `O_NOFOLLOW | O_NONBLOCK` (Unix) or `FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS`
  (Windows), checks `file.metadata()` on the open handle to refuse symbolic links/reparse points,
  non-regular files (such as FIFOs or directories), and files larger than `MAX_FRAME_SIZE`
  (`64 * 1024 * 1024 = 67,108,864` bytes, 64 MiB) before reading, and caps the read via
  `file.take(MAX_FRAME_SIZE as u64 + 1)` so files that grow past `MAX_FRAME_SIZE` while being read
  are refused with `"is more than 67108864 bytes"` (BUG-10). In Blender auto-rigging
  (`crates/worker/src/runner.rs:2346`), a refusal from `read_output_file` logs a warning and falls
  back to the pure-Rust auto-rigging engine without telling the caller. On the wire
  (`crates/worker/src/client.rs:667`), BUG-4's `rewrite_oversized_task_result` fails and rewrites
  `TaskResult` earlier whenever the entire serialized `WorkerMessage::TaskResult` frame — including
  `TaskResult` envelope fields, `stdout`/`stderr`, `artifacts`, codec framing (bincode or JSON/base64),
  and on encrypted lanes `AEAD_RECORD_OVERHEAD` (`29` bytes) — exceeds `MAX_FRAME_SIZE`
  (`67,108,864` bytes).
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

- `crates/worker/src/runner.rs` (`mod tests`): Unit tests for `read_output_file`:
  - `test_read_output_file_regular_missing_and_directory`: missing path returns `Ok(None)`, regular file returns `Ok(Some(bytes))`, directory returns `Err(InvalidData)`.
  - `test_read_output_file_fifo_returns_error_immediately` (Unix): FIFO at output path returns `Err(InvalidData)` immediately via `O_NONBLOCK` without hanging.
  - `test_read_output_file_refuses_symlink_and_oversized_or_sparse_file`: refuses symbolic links/reparse points and concurrent symlink swaps (`O_NOFOLLOW` / `FILE_FLAG_OPEN_REPARSE_POINT`), refuses `MAX_FRAME_SIZE + 1` (`67,108,865` bytes) before reading (`"is 67108865 bytes"`), and refuses files that grow past `MAX_FRAME_SIZE` during the read via the `take(MAX_FRAME_SIZE as u64 + 1)` cap (`"is more than 67108864 bytes"`).
- `crates/worker/tests/test_sec5_hardened_runner.rs`: Verifies priority inheritance (`nice` / Windows
  priority classes), cluster secret stripping from child environments and version probes, large stderr
  non-blocking drain, process group / tree termination on cancellation, and BUG-10 output file refusals
  for both 3D (`triposr`) and `BlenderRigProvider` (`test_bug10_3d_and_blender_refuse_symlink_output`,
  `test_bug10_3d_and_blender_refuse_oversized_sparse_output_without_reading`).
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
- 2026-11-03: Guarded `read_output_file` by opening with `O_NOFOLLOW | O_NONBLOCK` (Unix) and `FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS` (Windows) before inspecting `file.metadata()`, refusing symbolic links/reparse points, non-regular files, and files exceeding `MAX_FRAME_SIZE` (`67,108,864` bytes) before reading, and capping reads with `take(MAX_FRAME_SIZE as u64 + 1)` (BUG-10).

