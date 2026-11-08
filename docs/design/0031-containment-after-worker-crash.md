# 0031: Containment after a worker crash

Status: Accepted
Created: 2026-11-08    Updated: 2026-11-08

## Summary

- On Unix (`Linux`, `macOS`, `Android`), each task process group gets a small supervisor process that holds the read end of an anonymous lifeline pipe from the worker and runs `killpg` when that pipe reaches end of file (`read == 0`), replacing Linux's thread-bound `PR_SET_PDEATHSIG` (`FEAT-2` in [BACKLOG.md](../BACKLOG.md) and Phase B of [ROADMAP.md](../ROADMAP.md)).
- Each task spawn generates a fresh random per-attempt UUID v4 token (`RUSTY_GRID_RUN_TOKEN=<uuid v4>`). On Linux, when a writable cgroup v2 subtree is delegated to the worker's user, the task leader moves itself into `<delegated_cgroup>/oxideswarm-run-<run_token>` in `pre_exec` before `execve` so `<leaf>/cgroup.kill` terminates every descendant including `setsid()` processes; without delegated cgroup v2 (and on macOS and Android), a best-effort environment sweep kills `setsid()` descendants that still carry `RUSTY_GRID_RUN_TOKEN=<run_token>` without touching a retried attempt of the same `TaskId`.
- After `kill -9` (`SIGKILL`) of a worker, the kernel closes the worker's write end of every task lifeline pipe with zero bytes written, and each supervisor terminates its task group within 5 s so an orphaned task cannot keep running while the master runs it elsewhere.
- `run_chaos_test.sh` is tightened so that any `done:` line written by the `SIGKILL`ed victim worker after the kill fails the test.

## Context

What exists at `master` `01a9786`:

| Area | Today | Code |
|---|---|---|
| Windows job containment | `configure_command` spawns the child with `CREATE_SUSPENDED`, and `ProcessTree::attach` assigns it to a Job Object configured with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. When the worker dies (`TerminateProcess` or crash), the Windows kernel closes the job handle and terminates all processes in the job | `crates/worker/src/process_tree.rs:140-216`, `crates/core/src/capabilities.rs:1886-1929` |
| Unix process groups | `configure_command` calls `cmd.process_group(0)` so the task child leads a new process group (`pgid == child_pid`). Normal task completion, timeout, cancellation, or dropping `ProcessTree` sends `SIGTERM` and `SIGKILL` via `libc::killpg(pgid, sig)` | `crates/worker/src/process_tree.rs:122-187`, `crates/worker/src/runner.rs:2640-2805` |
| Parent-death signal | `libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL)` is compiled only for `#[cfg(target_os = "linux")]`. It is not compiled on `target_os = "android"`, does not exist on macOS, is cleared across `fork()` (so grandchildren never inherit it), and is tied to the spawning OS thread rather than the worker process | `crates/worker/src/process_tree.rs:128-131` |
| Session escape (`setsid`) | A descendant that calls `setsid()` or `setpgid(0, 0)` leaves the task's process group `pgid` and is missed by `libc::killpg(pgid, SIGKILL)` | `crates/worker/src/process_tree.rs:179-186` |
| Chaos test check | After `kill -KILL "$VICTIM_PID"`, `run_chaos_test.sh` notes that "the orphaned shell of a SIGKILLed worker keeps running and may add its own `done:` line from the victim's directory; only the survivor's line counts" | `run_chaos_test.sh:295, 344-345` |

The effect: after `kill -9` of a worker on macOS or Android, the entire task tree keeps running; on Linux, children and grandchildren of the top-level shell keep running. Meanwhile the master detects the dead worker and reschedules the task on another worker, so the same task runs twice at once.

## Goals

- **G1** When a worker process terminates for any reason (`SIGKILL`, OOM kill, abort, or normal exit) on Linux, macOS, or Android, every process in every active task's process group is terminated within 5 s.
- **G2** Containment works for both the `rusty-grid` (`oxideswarm`) CLI binary and in-process `TaskRunner` integration tests without requiring root privileges or an external helper executable on `PATH`.
- **G3** When a task ends or the worker crashes, descendants that leave the task's process group (`setsid()` or `setpgid`) are contained in full on Linux when a cgroup v2 subtree is delegated to the worker's user; when cgroup v2 is not delegated (and on macOS and Android), they are contained on a best-effort basis by matching the per-attempt `RUSTY_GRID_RUN_TOKEN` in the process environment, which a process started with a cleared environment (`env -i`) or, on macOS, an Apple platform binary (`/bin/sh`, `bash`, `zsh`, `/usr/bin/perl`, `sleep`) whose environment macOS omits from `KERN_PROCARGS2` can escape.
- **G4** The supervisor never holds a task's `stdin`, `stdout`, or `stderr` pipes open and is never a child of the task leader, so `waitpid(-1)` inside the task process returns `ECHILD` immediately when the task has no children of its own.
- **G5** `run_chaos_test.sh` fails if a task that was running on the victim worker at the time of `kill -KILL` writes a `done:${SB_VICTIM}` marker line after the kill.

## Non-goals

- Changing the Windows Job Object supervision path (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` already terminates the job tree in the kernel when the worker process dies).
- Filesystem or network sandboxing of running tasks (`FEAT-3`, design 0033).
- Surviving a worker restart and adopting the still-running task (`FEAT-1`).

## Design

### D1 Lifeline pipe between worker and task supervisor (`crates/worker/src/process_tree.rs`)

For each task command on Unix:

1. Before `cmd.spawn()` in `execute_process_supervised` (`crates/worker/src/runner.rs:2640`), `configure_command` generates a random per-attempt UUID v4 (`RUSTY_GRID_RUN_TOKEN=<uuid v4>`), queries `getrlimit(RLIMIT_NOFILE)` before `fork`, and creates an anonymous lifeline pipe `(lifeline_read_fd, lifeline_write_fd)` with `FD_CLOEXEC` set on both descriptors:
   - On Linux and Android, `libc::pipe2(fds, libc::O_CLOEXEC)` sets `FD_CLOEXEC` atomically.
   - On macOS (which has no `pipe2`), `libc::pipe(fds)` followed by `libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC)` is not atomic against a concurrent `fork` on another thread. A process-wide mutex (`SPAWN_LOCK: Mutex<()>`) is therefore held both across `pipe` + `fcntl(FD_CLOEXEC)` and across every task `cmd.spawn()` call on macOS, so no task `fork` can occur while any lifeline pipe descriptor lacks `FD_CLOEXEC`. (Short-lived host capability probes such as `pmset` in `crates/core/src/capabilities.rs` do not take `SPAWN_LOCK`; if one forks during the sub-microsecond window between `pipe` and `fcntl`, `pmset` exits within milliseconds and closes any inherited descriptor.)
2. `ProcessTree` stores `lifeline_write: Option<std::os::unix::io::OwnedFd>` alongside `id: Option<RawId>`.
3. Because `lifeline_write_fd` has `FD_CLOEXEC`, neither the task child nor any concurrently spawned task child inherits the write end across `execve`. Only the worker process holds `lifeline_write_fd`.
4. When `ProcessTree` is dropped in a live worker after worker-side teardown (`imp::kill(id)`) has completed, it writes a single dismissal byte (`b"0"`) to `lifeline_write` before closing it so the supervisor exits immediately without repeating the teardown sweep. When the worker crashes or is killed with `SIGKILL`, the kernel closes all file descriptors of the worker process with zero bytes written, immediately delivering EOF (`read == 0`) on `lifeline_read_fd`.

### D2 Double-forked supervisor inside `pre_exec` (`crates/worker/src/process_tree.rs`)

Inside `cmd.pre_exec` on Unix (which runs in the child process after `cmd.process_group(0)` has placed it as the leader of `pgid = libc::getpid()` and before `execve`):

1. `PR_SET_PDEATHSIG` is removed (the lifeline pipe replaces it; `PR_SET_PDEATHSIG` is tied to the OS thread that called `fork` rather than the worker process and would kill the task if the spawning thread ever exited early). `PR_SET_CHILD_SUBREAPER` is **not** set on the task leader, because setting it on the task leader would cause the double-forked supervisor C (and any orphaned task grandchild) to be reparented back to the task leader A instead of PID 1, breaking `waitpid(-1)` (`os.wait()`) inside the task.
2. Apply `ProcessPriority::unix_nice` (`libc::setpriority(libc::PRIO_PROCESS, 0, nice)`) as today.
3. Fork a tiny supervisor via a double fork so the supervisor is reparented to PID 1 (`init` / `launchd`) and the task leader (`pid == pgid`) has zero child processes when `execve` begins:
   - Process A (the task leader, `pid == pgid`) calls `libc::fork()` to create intermediate child B.
   - Child B calls `libc::fork()` to create supervisor grandchild C, and child B immediately calls `libc::_exit(0)`.
   - Process A reaps child B with `libc::waitpid(child_b, &mut status, 0)`, closes `lifeline_read_fd`, moves itself into its cgroup v2 leaf if configured (D3.1), and returns `Ok(())` from `pre_exec` to `execve` the task program.
4. In supervisor grandchild C (which never calls `execve` and uses only async-signal-safe `libc` calls):
   - Call `libc::setsid()` so the supervisor is in its own session and does not signal itself when calling `libc::killpg(pgid, ...)`.
   - Close every file descriptor except `lifeline_read_fd` (and the optional cgroup `cgroup.procs` and `cgroup.kill` descriptors on Linux, D3.1), using `SYS_close_range` on Linux (`#[cfg(target_os = "linux")]`) and a bounded `close` loop up to the `RLIMIT_NOFILE` limit queried before `fork` on macOS, Android, and Linux kernels without `close_range` (Bionic's app seccomp allowlist only added `close_range` in Android 13 / API 33 while `minSdk` is 24, so `SYS_close_range` is never called on Android). This closes the supervisor's copy of `lifeline_write_fd`, the task's `stdin`/`stdout`/`stderr` pipes, and Rust stdlib's internal `CLOEXEC` exec-error pipe so `cmd.spawn()` in the worker returns without waiting on the supervisor.
   - Block in `libc::read(lifeline_read_fd, &mut byte, 1)` (retrying on `EINTR`).
   - If `read` returns `> 0` (the live worker completed `ProcessTree` teardown and wrote the dismissal byte), close remaining descriptors and call `libc::_exit(0)`.
   - On EOF (`read == 0`, or an unrecoverable read error):
     1. Send `libc::killpg(pgid, libc::SIGTERM)`.
     2. Poll whether `libc::killpg(pgid, 0) == 0` in 20 ms steps up to 200 ms (`stop_grace`).
     3. Send `libc::killpg(pgid, libc::SIGKILL)`.
     4. Run the session-escape cleanup (D3).
     5. Close remaining descriptors and call `libc::_exit(0)`.

### D3 Containing `setsid()` descendants on Linux, Android, and macOS (`crates/worker/src/process_tree.rs`)

A child that calls `setsid()` or `setpgid(0, 0)` moves out of `pgid` and is not reached by `libc::killpg(pgid, SIGKILL)`. Two mechanisms handle it:

1. **Linux cgroup v2 leaf before `execve` (when delegated):**
   - At worker start-up on Linux, `process_tree` probes `/proc/self/cgroup` and `/sys/fs/cgroup` to check whether a writable cgroup v2 directory is delegated to the worker's effective UID.
   - When writable, before `cmd.spawn()` the worker creates a per-attempt leaf cgroup directory `<delegated_cgroup>/oxideswarm-run-<run_token>` (named with the per-attempt `run_token` rather than `task_id` so a retried attempt of the same task never hits `EEXIST`) and opens `<leaf>/cgroup.procs` and `<leaf>/cgroup.kill` with `O_CLOEXEC`.
   - Inside `cmd.pre_exec` — **after** double-forking supervisor C (so C stays outside the task leaf cgroup) and **before** `execve` — the task leader A writes its own decimal PID (`libc::getpid()`) to `<leaf>/cgroup.procs` and closes that descriptor. Because the move happens before `execve`, the task leader and every child or grandchild it ever forks (including `setsid()` and `env -i` processes) are born inside `<leaf>`.
   - On task stop (`ProcessTree::kill_async` / `Drop`) or lifeline EOF in supervisor C, writing `"1"` to `<leaf>/cgroup.kill` (Linux 5.14+, falling back to reading `<leaf>/cgroup.procs` and sending `SIGKILL` to each PID) kills every descendant in the cgroup regardless of `setsid()` or environment changes, after which `<leaf>` is removed with `rmdir` (retrying briefly on `EBUSY` while the kernel reaps the killed processes).
2. **Per-attempt environment tag scan (`RUSTY_GRID_RUN_TOKEN`) when cgroup v2 is not delegated (Linux, Android, and macOS):**
   - `configure_command` injects a fresh per-attempt `RUSTY_GRID_RUN_TOKEN=<uuid v4>` into each spawned task's environment. Matching on `RUSTY_GRID_RUN_TOKEN` rather than `RUSTY_GRID_TASK_ID` ensures that when a worker disconnects and the master retries the same `TaskId` on another worker on the same host, the first attempt's teardown or supervisor never kills the retried attempt.
   - Both worker-side teardown (`ProcessTree::kill_async`, run on Tokio's `spawn_blocking` pool so it never blocks a runtime worker thread, and `ProcessTree::drop` after releasing the `LIVE` registry lock) and supervisor C (on lifeline EOF) run a post-`killpg` sweep for same-UID processes outside `pgid` whose environment still contains `RUSTY_GRID_RUN_TOKEN=<run_token>` (excluding the worker and the supervisor):
     - On Linux and Android: scans `/proc/<pid>/environ` for same-UID processes (`st_uid == geteuid()`) matching `RUSTY_GRID_RUN_TOKEN=<run_token>\0` and sends `libc::kill(pid, libc::SIGKILL)`.
     - On macOS: queries PIDs via `libc::proc_listallpids`, filters via `libc::proc_pidinfo(..., PROC_PIDTBSDINFO, ...)` to same-UID processes started at or after the task's `spawn_tvsec` (`pbi_start_tvsec >= spawn_tvsec`), and inspects each candidate's environment block via `libc::sysctl` (`[CTL_KERN, KERN_PROCARGS2, pid]`) for `RUSTY_GRID_RUN_TOKEN=<run_token>\0`, sending `libc::kill(pid, libc::SIGKILL)` to any match. On macOS, `KERN_PROCARGS2` returns `argv` only (omitting `envp`) for SIP-protected Apple platform binaries (`/bin/sh`, `bash`, `zsh`, `/usr/bin/perl`, `sleep`), so the macOS environment sweep catches non-platform binaries (such as user-compiled binaries and Homebrew/pyenv Python, Node, or Blender) while platform binaries that call `setsid()` without a non-platform wrapper escape the scan.

### D4 Tightening `run_chaos_test.sh` (`run_chaos_test.sh`)

In `run_chaos_test.sh`:

1. In Stage 2 (`run_chaos_test.sh:237-287`), each task in `VICTIM_TASKS` is verified to have written `start:${SB_VICTIM}` and not `done:` before `kill -KILL "$VICTIM_PID"`.
2. In Stage 5 (`run_chaos_test.sh:344-365`), after all tasks complete on the survivor worker, add an explicit check over `VICTIM_TASKS`: if any orphaned task's marker log contains `^done:${SB_VICTIM}`, `run_chaos_test.sh` calls `log_fail` instead of ignoring the victim's `done:` line.

## Compatibility

- Wire format: unchanged. No protocol messages, CLI flags, or configuration keys change.
- Platform support: Linux, macOS, and Android use the lifeline pipe and double-forked supervisor; Windows continues to use `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
- Android phantom-process budget: Android 12+ (`ActivityManager`) limits an app to 32 phantom processes across all background child trees. Because the double-forked supervisor is spawned by a child of the worker, it counts as 1 process toward this 32-process limit for the duration of each active task on both the in-process Android worker (`liboxideworker.so`) and Termux. Mobile workers run with 1 to 4 concurrent task slots (`Hot` thermal tier caps slots at 1), so the supervisor adds 1 to 4 processes while tasks are running.

## Security

- Prevents orphaned task processes in the task's process group from continuing to execute code, consume CPU/RAM, or modify files on a worker host after the worker process is killed or crashes.
- On Linux with a delegated cgroup v2 subtree, `<leaf>/cgroup.kill` contains all descendants even if they call `setsid()` and clear their environment (`env -i`).
- Limitation without delegated cgroup v2: on macOS, Android, and Linux hosts without a writable cgroup v2 subtree, `setsid()` containment relies on `RUSTY_GRID_RUN_TOKEN` in `/proc/<pid>/environ` or `KERN_PROCARGS2`. A process that both calls `setsid()` and strips `RUSTY_GRID_RUN_TOKEN` from its environment (for example `env -i setsid ...`) escapes that scan. In addition, on macOS `sysctl(KERN_PROCARGS2)` omits environment variables for Apple platform binaries (`/bin/sh`, `bash`, `zsh`, `/usr/bin/perl`, `sleep`), so a `setsid()` descendant that executes an Apple platform binary directly escapes the macOS environment sweep. Scanning `/proc` or `sysctl` by PID also carries a theoretical PID-reuse window between reading `environ` and `kill(pid, SIGKILL)`. Full kernel-enforced containment on Linux without cgroup v2 delegation requires PID/mount namespaces (`FEAT-3`, design 0033).
- On macOS, `SPAWN_LOCK` serializes worker task spawns across `pipe` + `fcntl(FD_CLOEXEC)` and `cmd.spawn()`; short-lived host capability probes (`pmset` in `crates/core/src/capabilities.rs`) do not acquire `SPAWN_LOCK` and could inherit a lifeline pipe fd during the sub-microsecond window before `fcntl`, which closes as soon as `pmset` exits a few milliseconds later.

## Test plan

- **Unit tests (`crates/worker/src/process_tree.rs`):**
  - Lifeline pipe creation sets `FD_CLOEXEC` on both ends (and holds `SPAWN_LOCK` on macOS).
  - `build_task_env_needle` formats `RUSTY_GRID_RUN_TOKEN=<uuid>\0` and matches exact `\0`-delimited entries.
- **Integration tests (`crates/worker/tests/test_sec5_hardened_runner.rs`):**
  1. **Direct `waitpid(-1)` (`ECHILD`) test:** Runs `python3` calling `os.wait()` (`waitpid(-1, ..., 0)`) inside a supervised task on Linux and macOS and asserts that `waitpid(-1)` returns `ECHILD` immediately, proving the supervisor is not a child of the task leader.
  2. **Normal task completion with a `setsid` survivor:** Runs a task that spawns a background child in a new session (`os.setsid()`), records its PID, and exits 0. Asserts that worker-side teardown (`ProcessTree::kill_async`) kills the `setsid` child within 1 s (while the supervisor is dismissed without sweeping).
  3. **Concurrent attempts with the same `TaskId`:** Runs two concurrent attempts sharing the exact same `TaskId` (`python3` tasks); ends one attempt (including a `setsid` child) while the second is mid-flight, and asserts the second attempt completes with exit code 0.
  4. **Short task completion and no pipe-drain note:** Asserts that a short task completes in under 1 s without any `"kept the output pipes open"` note in `stderr`, catching any supervisor descriptor leak of `stdout`/`stderr`.
  5. **Supervisor exit after normal task completion:** Asserts that every supervisor process is gone within 1 s after a normal task completion.
  6. **Worker `SIGKILL` containment test (single task and 8 concurrent tasks):** Starts a worker subprocess, runs 1 task (and in a separate test 8 concurrent tasks) each with a child, grandchild, and `setsid` process, sends `kill -9` (`SIGKILL`) to the worker once all PIDs are recorded, and asserts that none of their processes remain alive within 5 s.
  7. **macOS platform-binary `setsid` escape documentation test (`#[cfg(target_os = "macos")]`):** Spawns a `setsid` `/usr/bin/perl` child on macOS, documents that `KERN_PROCARGS2` omits `envp` for Apple platform binaries so the process survives the environment sweep, and cleans up the PID at the end of the test.
- **Chaos script (`run_chaos_test.sh`):**
  - Run `./run_chaos_test.sh --duration 4 --tasks-count 8` and verify that no killed victim task writes `done:${SB_VICTIM}` after `kill -KILL`, and that injecting a `done:${SB_VICTIM}` line causes `run_chaos_test.sh` to fail.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-2` | Add per-task lifeline pipe, double-forked process-group supervisor, pre-`execve` Linux cgroup v2 leaf + per-attempt `RUSTY_GRID_RUN_TOKEN` `setsid` sweep, and `run_chaos_test.sh` victim completion rejection | `crates/worker/src/process_tree.rs`, `crates/worker/src/runner.rs`, `crates/worker/tests/test_sec5_hardened_runner.rs`, `tests/cli_worker_signals_e2e.rs`, `run_chaos_test.sh`, `docs/design/0031-containment-after-worker-crash.md`, `docs/README.md` | Direct `waitpid(-1)` in a task returns `ECHILD` at once; normal completion kills a `setsid` child; two attempts with the same `TaskId` do not kill each other; short task finishes in `< 1 s` with no pipe note; supervisors exit within `1 s`; 8 concurrent tasks are all dead within `5 s` of `kill -9` on the worker; `run_chaos_test.sh` fails if the victim writes `done:` after the kill | Proposed |

## Status log

- 2026-11-08: Initial draft written for `FEAT-2` (Phase B of `docs/ROADMAP.md`).
- 2026-11-08: Addressed review `116-fix`: dropped `PR_SET_CHILD_SUBREAPER` and `PR_SET_PDEATHSIG` from the task leader, moved Linux cgroup v2 leaf assignment into `pre_exec` before `execve`, held `SPAWN_LOCK` across `pipe` + `fcntl(FD_CLOEXEC)` and `cmd.spawn()` on macOS, chose `KERN_PROCARGS2` on macOS and stated the `env -i` + `setsid` limitation in G3 and Security, documented Android's 32 phantom-process limit in Compatibility, and added the direct `waitpid(-1)` (`ECHILD`) and normal-exit `setsid` cleanup tests to the Test plan.
- 2026-11-08: Addressed review `123-fix`: switched the environment sweep needle and cgroup v2 leaf name from `TaskId` to a per-attempt `RUSTY_GRID_RUN_TOKEN` UUID v4, documented the macOS `KERN_PROCARGS2` platform-binary limitation in G3, Security, and D3.2, gated `SYS_close_range` to Linux only (never Android), removed the 16384 fd cap in favor of `RLIMIT_NOFILE`, moved worker-side `ProcessTree` kills off runtime threads and outside the `LIVE` lock, added a lifeline dismissal byte on normal `ProcessTree` teardown, and consolidated all `FEAT-2` tests into `crates/worker/tests/test_sec5_hardened_runner.rs`.
