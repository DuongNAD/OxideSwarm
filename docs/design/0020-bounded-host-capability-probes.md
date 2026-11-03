# 0020: Bounded host capability probes at worker start-up

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Bound every host capability probe subprocess spawned during `WorkerCapabilities::detect` / `WorkerCapabilities::detect_with_overrides` (`blender --version`, `sysctl`, `nvidia-smi`, `pmset`, `dumpsys`, `getprop`, `sw_vers`, and `nvcc`) to `HOST_PROBE_TIMEOUT` (`3 s`), kill the probe's process group (Unix `libc::killpg` with `libc::SIGKILL`) or Windows Job Object (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`) on both child exit and timeout, reap timed-out children on a detached thread so uninterruptible kernel sleeps never block start-up, and treat a timed-out Blender probe as "no Blender" (`has_blender = false, blender_version = None`) with a warning that names the binary path.

## Context

- [Design 0009](0009-blender-and-3d-process-supervision.md) (Goal **G3**) stripped cluster secret environment variables from `blender --version` and `python --version` probes via `rusty_grid_core::cluster_auth::probe_command`, while leaving system discovery probes (`nvcc`, `nvidia-smi`, `sysctl`, `pmset`, `sw_vers`, `dumpsys`, `getprop`) with their inherited environment.
- However, all host capability probes in `crates/core/src/capabilities.rs` still ran via synchronous `std::process::Command::output()` with no wall-clock timeout:

| Area | Behaviour before this change | Code |
|---|---|---|
| `blender --version` probe | `query_blender_version` called `probe_command(program).arg("--version").output()` with no timeout. A hung Blender binary (broken installation, license dialog, or GPU driver prompt) blocked `WorkerClient::new` indefinitely. Moreover, when `query_blender_version` returned `None` on an existing file (`explicit_path`, `RUSTY_GRID_BLENDER_PATH`, or standard install paths), `detect_blender` fell through and advertised `has_blender = true` (`"Blender (Custom Path)"` / `"Blender (Env Path)"`). | `crates/core/src/capabilities.rs` |
| Other host probes | `sysctl`, `nvidia-smi`, `pmset`, `dumpsys`, `getprop`, `sw_vers`, and `nvcc` called `.output()` with no timeout; a wedged `nvidia-smi` (uninterruptible sleep on a hung GPU driver) blocked worker start-up forever. | `crates/core/src/capabilities.rs` |

## Goals

- **G1** Bound every start-up host capability probe in `crates/core/src/capabilities.rs` to `HOST_PROBE_TIMEOUT` (`3 s`).
- **G2** Treat a timed-out Blender probe as unavailable (`(false, None)`), logging a `tracing::warn!` that names the binary path instead of advertising `"Blender (Env Path)"` / `"Blender (Custom Path)"` or hanging start-up.
- **G3** Guarantee that neither a grandchild holding stdout open after the direct child exits nor a child stuck in uninterruptible sleep (`D` state) can block `run_command_bounded` past the deadline, and ensure no probe child or grandchild process survives a probe.

## Non-goals

- Changing task-time subprocess supervision in `crates/worker` (covered by [design 0009](0009-blender-and-3d-process-supervision.md)).
- Bounding the task-time `blender --version` check inside `crates/worker/src/autorig/blender_provider.rs` (tracked separately).

## Design

### D1 Bounded probe runner (`run_command_bounded`)

`run_command_bounded(cmd, timeout)` runs `cmd` with `Stdio::null()` stdin/stderr and piped stdout:
1. On Unix, `cmd.process_group(0)` places the probe in its own process group whose PGID equals the child PID. On Windows, `cmd.creation_flags(WINDOWS_CREATE_SUSPENDED)` spawns the child suspended so `attach_suspended_process_to_kill_on_close_job` (shared between `rusty_grid_core::capabilities` and `rusty_grid_worker::process_tree`) can create an anonymous Win32 Job Object configured with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, call `AssignProcessToJobObject` before any child instruction runs, and resume the child via `NtResumeProcess`.
2. A reader thread reads up to `64 KiB` from the child's stdout pipe (`out.take(64 * 1024).read_to_end(&mut buf)`) and sends the collected bytes over a `std::sync::mpsc::sync_channel(1)` when EOF (or the `64 KiB` cap) is reached.
3. The caller polls `child.try_wait()` with exponential backoff (`5 ms` up to `25 ms`) until `deadline = Instant::now() + timeout`:
   - **When the direct child exits (`Ok(Some(status))`):** on Unix, `kill_probe_group(pid)` (`libc::killpg(pgid, libc::SIGKILL)`) is called immediately; on Windows, `job_guard.terminate_and_close()` (`TerminateJobObject` + `CloseHandle`) terminates the `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` Job Object immediately. Any background grandchild spawned in the probe's process group or Job Object (even if the direct `cmd.exe` or shell wrapper has already exited) is terminated and closes its inherited stdout descriptor. The caller then waits on `rx.recv_timeout(remaining)` for the remaining probe budget; if a grandchild that escaped the Unix process group via `setsid` still holds the pipe open past `deadline`, `rx.recv_timeout(remaining)` times out and `run_command_bounded` returns `ProbeExecOutcome::TimedOut`.
   - **When `deadline` expires (`now >= deadline`):** `job_guard.terminate_and_close()` terminates and closes the Windows Job Object, and `reap_probe_child_detached(child)` kills the Unix process group (`libc::killpg(pgid, libc::SIGKILL)`) and direct child (`child.kill()`), then spawns a detached thread calling `child.wait()`. Reaping on a detached thread guarantees that a process wedged in uninterruptible kernel sleep (`nvidia-smi` on a hung GPU driver) never blocks worker start-up past `HOST_PROBE_TIMEOUT`.

### D2 Blender and host probe semantics (`probe_blender_with_timeout`, `run_host_probe`)

- `probe_blender_with_timeout(program, timeout)` builds the command with `crate::cluster_auth::probe_command(program)` (preserving [design 0009](0009-blender-and-3d-process-supervision.md) **G3** secret stripping) and classifies the outcome as `BlenderProbeResult::{Version(String), SucceededWithoutVersion, Failed, TimedOut}`.
- On `BlenderProbeResult::TimedOut`, `probe_blender_with_timeout` emits a `tracing::warn!` naming the probed path and timeout, and both `detect_blender` and `probe_blender_binary` return `(false, None)` immediately rather than falling back to `"Blender (Env Path)"` / `"Blender (Custom Path)"` or probing additional fallback candidates.
- All other host capability probes in `crates/core/src/capabilities.rs` (`sysctl`, `nvidia-smi`, `pmset`, `dumpsys`, `getprop`, `sw_vers`, `nvcc`) execute through `run_host_probe(cmd, label)` with `HOST_PROBE_TIMEOUT` (`3 s`).

## Compatibility

- Normal Blender installations (typically `< 300 ms`) and responsive system utilities are unaffected.
- A Blender binary that takes longer than `3 s` to answer `--version` is now treated as unavailable (`has_blender = false`, `blender_version = None`), unless `--blender` (`overrides.blender == Some(true)`) is explicitly set.

## Security

- Preserves [design 0009](0009-blender-and-3d-process-supervision.md) **G3**: `probe_blender_with_timeout` uses `crate::cluster_auth::probe_command` so cluster secret environment variables (`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`) are stripped from `blender --version` probes.
- Using `libc::killpg` directly on Unix and Win32 Job Objects (`CreateJobObjectW` / `SetInformationJobObject` / `AssignProcessToJobObject` / `TerminateJobObject`) on Windows avoids invoking external `kill` or `taskkill` binaries from `PATH` and eliminates the race where a root `cmd.exe` exits before `taskkill /T` can walk its child list.

## Test plan

- `capabilities::tests::test_probe_blender_with_timeout_handles_version_non_version_failure_and_timeout` in `crates/core/src/capabilities.rs` (covering normal version output, non-version output, non-zero exit, wrapper script leaving a background grandchild holding stdout, detached-session `setsid` grandchild holding stdout past the deadline, and hung script timeout + grandchild termination via PID file on both Unix and Windows).
- `capabilities::tests::test_hung_blender_probe_via_override_times_out_and_reports_no_blender` in `crates/core/src/capabilities.rs` (calling `detect_blender(Some(path))` directly with a 60s hang script so `RUSTY_GRID_NO_BLENDER` in the environment cannot bypass the probe, using a pure `cmd.exe` wrapper on Windows (`ping -n 60 127.0.0.1 >NUL`) so the `3 s` production `HOST_PROBE_TIMEOUT` test is independent of cold Python start time, and verifying `has_blender == false`, `blender_version == None`, and no surviving grandchild on Unix).
- `client::tests::test_worker_client_with_hung_blender_path_reports_no_blender_within_timeout` in `crates/worker/src/client.rs`.

## Work items

- `BUG-15` in `WORK_PLAN.md` / `docs/BACKLOG.md`.

## Status log

- 2026-11-04: Implemented `HOST_PROBE_TIMEOUT` (`3 s`), `run_command_bounded`, `probe_blender_with_timeout`, and `run_host_probe` in `crates/core/src/capabilities.rs`; shared Windows Job Object helper (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`) between `rusty_grid_core::capabilities` and `rusty_grid_worker::process_tree`; bounded stdout reader join with `rx.recv_timeout(remaining)`, killed process group via `libc::killpg(pgid, libc::SIGKILL)` (Unix) or Job Object termination (Windows) on both child exit and timeout, reaped timed-out probes on a detached thread, and verified grandchild cleanup in unit tests.
- 2026-11-04: Widened Windows test budgets in `capabilities::tests::test_probe_blender_with_timeout_handles_version_non_version_failure_and_timeout` (`15 s` probe timeout with `10 s` elapsed bound on the background-stdout wrapper, `5 s` probe timeout with `15 s` elapsed bound on the hung wrapper, `15 s` on simple `cmd.exe` probes, and `5 s` process-table wait in `assert_pid_file_process_dead`) and made the Windows variant of `capabilities::tests::test_hung_blender_probe_via_override_times_out_and_reports_no_blender` call `detect_blender(Some(path))` directly with a pure `cmd.exe` hang wrapper (`ping -n 60 127.0.0.1 >NUL`) so neither test depends on cold Python startup completing within `3 s` on Windows CI runners (`TEST-12`).
