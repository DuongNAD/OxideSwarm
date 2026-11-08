# 0031: Containment after a worker crash

Status: Draft
Created: 2026-11-08    Updated: 2026-11-08

## Summary

- On Unix (`Linux`, `macOS`, `Android`), each task process group gets a small supervisor process that holds the read end of an anonymous lifeline pipe from the worker and runs `killpg` when that pipe reaches end of file (`FEAT-2` in [BACKLOG.md](../BACKLOG.md) and Phase B of [ROADMAP.md](../ROADMAP.md)).
- On Linux and Android, the supervisor and task leader also enable `PR_SET_PDEATHSIG` and `PR_SET_CHILD_SUBREAPER`, and use a per-task cgroup v2 leaf (`cgroup.kill`) when a writable cgroup v2 subtree is delegated to the worker's user, with a `/proc` fallback for descendants that call `setsid()`.
- After `kill -9` (`SIGKILL`) of a worker, the kernel closes the worker's write end of every task lifeline pipe, and each supervisor terminates its task group within 5 s so an orphaned task cannot keep running while the master runs it elsewhere.
- `run_chaos_test.sh` is tightened so that any `done:` line written by the `SIGKILL`ed victim worker after the kill fails the test.

## Context

What exists at `master` `01a9786`:

| Area | Today | Code |
|---|---|---|
| Windows job containment | `configure_command` spawns the child with `CREATE_SUSPENDED`, and `ProcessTree::attach` assigns it to a Job Object configured with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. When the worker dies (`TerminateProcess` or crash), the Windows kernel closes the job handle and terminates all processes in the job | `crates/worker/src/process_tree.rs:140-216`, `crates/core/src/capabilities.rs:1886-1929` |
| Unix process groups | `configure_command` calls `cmd.process_group(0)` so the task child leads a new process group (`pgid == child_pid`). Normal task completion, timeout, cancellation, or dropping `ProcessTree` sends `SIGTERM` and `SIGKILL` via `libc::killpg(pgid, sig)` | `crates/worker/src/process_tree.rs:122-187`, `crates/worker/src/runner.rs:2640-2805` |
| Parent-death signal | `libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL)` is compiled only for `#[cfg(target_os = "linux")]`. It is not compiled on `target_os = "android"`, does not exist on macOS, is cleared across `fork()` (so grandchildren never inherit it), and is tied to the spawning Tokio worker thread rather than the worker process | `crates/worker/src/process_tree.rs:128-131` |
| Session escape (`setsid`) | A descendant that calls `setsid()` or `setpgid(0, 0)` leaves the task's process group `pgid` and is missed by `libc::killpg(pgid, SIGKILL)` | `crates/worker/src/process_tree.rs:179-186` |
| Chaos test check | After `kill -KILL "$VICTIM_PID"`, `run_chaos_test.sh` notes that "the orphaned shell of a SIGKILLed worker keeps running and may add its own `done:` line from the victim's directory; only the survivor's line counts" | `run_chaos_test.sh:295, 344-345` |

The effect: after `kill -9` of a worker on macOS or Android, the entire task tree keeps running; on Linux, children and grandchildren of the top-level shell keep running. Meanwhile the master detects the dead worker and reschedules the task on another worker, so the same task runs twice at once.

## Goals

- **G1** When a worker process terminates for any reason (`SIGKILL`, OOM kill, abort, or normal exit) on Linux, macOS, or Android, every process in every active task's process group is terminated within 5 s.
- **G2** Containment works for both the `rusty-grid` (`oxideswarm`) CLI binary and in-process `TaskRunner` integration tests without requiring root privileges or an external helper executable on `PATH`.
- **G3** On Linux, descendants that call `setsid()` or `setpgid` are also contained and terminated when the task ends or the worker crashes.
- **G4** The supervisor never holds a task's `stdin`, `stdout`, or `stderr` pipes open and never interferes with `wait` or `waitpid(-1)` inside the task process.
- **G5** `run_chaos_test.sh` fails if a task that was running on the victim worker at the time of `kill -KILL` writes a `done:${SB_VICTIM}` marker line after the kill.

## Non-goals

- Changing the Windows Job Object supervision path (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` already terminates the job tree in the kernel when the worker process dies).
- Filesystem or network sandboxing of running tasks (`FEAT-3`, design 0033).
- Surviving a worker restart and adopting the still-running task (`FEAT-1`).

## Design

### D1 Lifeline pipe between worker and task supervisor (`crates/worker/src/process_tree.rs`)

For each task command on Unix:

1. Before `cmd.spawn()` in `execute_process_supervised` (`crates/worker/src/runner.rs:2640`), `configure_command` creates an anonymous lifeline pipe `(lifeline_read_fd, lifeline_write_fd)` with `O_CLOEXEC` (`FD_CLOEXEC`) set on both descriptors (`libc::pipe2(..., libc::O_CLOEXEC)` on Linux and Android; `libc::pipe` followed by `libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC)` under a creation lock on macOS).
2. `ProcessTree` stores `lifeline_write: Option< std::os::unix::io::OwnedFd >` alongside `id: Option<RawId>`.
3. Because `lifeline_write_fd` has `FD_CLOEXEC`, neither the task child nor any concurrently spawned task child inherits the write end across `execve`. Only the worker process holds `lifeline_write_fd`.
4. When `ProcessTree` is dropped in a live worker, dropping `lifeline_write` closes the write end after `imp::kill(id)` runs. When the worker crashes or is killed with `SIGKILL`, the kernel closes all file descriptors of the worker process, immediately delivering EOF (`read == 0`) on `lifeline_read_fd`.

### D2 Double-forked supervisor inside `pre_exec` (`crates/worker/src/process_tree.rs`)

Inside `cmd.pre_exec` on Unix (which runs in the child process after `cmd.process_group(0)` has placed it as the leader of `pgid = libc::getpid()` and before `execve`):

1. On Linux and Android (`#[cfg(any(target_os = "linux", target_os = "android"))]`), set `libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL)` and `libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1)`.
2. Apply `ProcessPriority::unix_nice` (`libc::setpriority(libc::PRIO_PROCESS, 0, nice)`) as today.
3. Fork a tiny supervisor via a double fork so the task leader (`pid == pgid`) has no extra child process when `execve` begins:
   - Process A (the task leader, `pid == pgid`) calls `libc::fork()` to create intermediate child B.
   - Child B calls `libc::fork()` to create supervisor grandchild C, and child B immediately calls `libc::_exit(0)`.
   - Process A reaps child B with `libc::waitpid(child_b, &mut status, 0)`, closes `lifeline_read_fd`, and returns `Ok(())` from `pre_exec` to `execve` the task program.
4. In supervisor grandchild C (which never calls `execve` and uses only async-signal-safe `libc` calls):
   - Call `libc::setsid()` so the supervisor is in its own session and does not signal itself when calling `libc::killpg(pgid, ...)`.
   - Close every file descriptor except `lifeline_read_fd` (using `close_range` on Linux/Android and `fcntl` / bounded descriptor loop on macOS). This closes the supervisor's copy of `lifeline_write_fd`, the task's `stdin`/`stdout`/`stderr` pipes, and Rust stdlib's internal `CLOEXEC` exec-error pipe so `cmd.spawn()` in the worker returns without waiting on the supervisor.
   - Block in `libc::read(lifeline_read_fd, &mut byte, 1)` (retrying on `EINTR`) until EOF (`<= 0`).
   - On EOF:
     1. Send `libc::killpg(pgid, libc::SIGTERM)`.
     2. Poll whether `libc::killpg(pgid, 0) == 0` in 20 ms steps up to 200 ms (`stop_grace`).
     3. Send `libc::killpg(pgid, libc::SIGKILL)`.
     4. Run the platform session-escape sweep (D3).
     5. Close `lifeline_read_fd` and call `libc::_exit(0)`.

### D3 Containing `setsid()` descendants on Linux and macOS (`crates/worker/src/process_tree.rs`)

A malicious or careless child that calls `setsid()` or `setpgid(0, 0)` moves out of `pgid` and is not reached by `libc::killpg(pgid, SIGKILL)`. Two layers close this gap:

1. **Linux cgroup v2 leaf (when delegated):**
   - At worker start-up on Linux, `process_tree` probes `/proc/self/cgroup` and `/sys/fs/cgroup` to check whether a writable cgroup v2 directory is delegated to the worker's effective UID.
   - When writable, `ProcessTree::attach` creates a leaf cgroup `<delegated_cgroup>/oxideswarm-task-<pgid>`, writes `pgid` to `cgroup.procs` before the task forks grandchildren, and passes the cgroup path to `ProcessTree` and the supervisor.
   - On task stop or lifeline EOF, writing `"1"` to `<leaf>/cgroup.kill` (Linux 5.14+) or sending `SIGKILL` to every PID in `<leaf>/cgroup.procs` atomically kills every descendant in the cgroup regardless of `setsid()`, after which `<leaf>` is removed with `rmdir`.
2. **Inherited task-token descriptor and process scan (unprivileged Linux, Android, and macOS):**
   - `execute_process_supervised` already injects `RUSTY_GRID_TASK_ID=<uuid>` into the task's environment (`crates/worker/src/runner.rs:2621`).
   - On Linux and Android, when cgroup v2 is not writable, both `ProcessTree::kill` and the supervisor on lifeline EOF scan `/proc/<pid>/environ` for processes owned by the same UID carrying `RUSTY_GRID_TASK_ID=<task_id>` (excluding the supervisor itself) and send `libc::kill(pid, libc::SIGKILL)`.
   - On macOS, the supervisor and `ProcessTree::kill` record a per-task token file lock / descriptor in the task's sandbox (`<sandbox>/.oxideswarm_tree_lock`) or match `RUSTY_GRID_TASK_ID=<task_id>` via `sysctl` (`KERN_PROCARGS2` for same-UID processes) and send `libc::kill(pid, libc::SIGKILL)` to any survivor outside `pgid`.

### D4 Tightening `run_chaos_test.sh` (`run_chaos_test.sh`)

In `run_chaos_test.sh`:

1. In Stage 2 (`run_chaos_test.sh:237-287`), each task in `VICTIM_TASKS` is verified to have written `start:${SB_VICTIM}` and not `done:` before `kill -KILL "$VICTIM_PID"`.
2. In Stage 5 (`run_chaos_test.sh:344-365`), after all tasks complete on the survivor worker, add an explicit check over `VICTIM_TASKS`: if any orphaned task's marker log contains `^done:${SB_VICTIM}` written after the kill (with a narrow 200 ms pre-kill race guard for a task that finished between the last Stage 2 check and `kill -KILL`, avoided by using a task workload duration >= 4 s and checking before `T_KILL_MS`), `run_chaos_test.sh` calls `log_fail` instead of ignoring the victim's `done:` line.

## Compatibility

- Wire format: unchanged. No protocol messages, CLI flags, or configuration keys change.
- Platform support: Linux, macOS, and Android use the lifeline pipe and double-forked supervisor; Windows continues to use `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.

## Security

- Prevents orphaned task processes from continuing to execute arbitrary code, consume CPU/RAM, or modify files on a worker host after the worker process is killed or crashes.
- Prevents a descendant that calls `setsid()` from surviving task cancellation, task timeout, or worker crash.
- The supervisor runs with the same UID/GID as the task, closes all inherited file descriptors except `lifeline_read_fd`, and signals only its own task `pgid` and processes carrying its task's `RUSTY_GRID_TASK_ID`.

## Test plan

- **Unit tests (`crates/worker/src/process_tree.rs`):**
  - Lifeline pipe creation sets `FD_CLOEXEC` on both ends.
  - Supervisor double-fork leaves no child process visible to `waitpid(-1, WNOHANG)` in the task leader.
- **Integration tests (`crates/worker/tests/test_worker_crash_containment.rs`):**
  - **Worker `SIGKILL` containment test ("Done when" of `FEAT-2`):** Starts a real `rusty-grid` worker subprocess connected to an in-process master, submits a shell task that spawns a child and a grandchild (plus a `setsid` background process) writing their PIDs to a marker file and sleeping, sends `kill -9` (`SIGKILL`) to the worker PID once all descendant PIDs are recorded, and asserts that the worker, child, grandchild, and `setsid` descendant are all dead (`kill(pid, 0)` returns `ESRCH`) within 5 s on Linux and macOS.
  - **Normal task exit and `waitpid(-1)` hygiene test:** Verifies that a task running a shell `wait` loop exits cleanly with code 0 and that the supervisor process exits within 1 s after the task finishes without holding `stdout` or `stderr` open.
- **Chaos script (`run_chaos_test.sh`):**
  - Run `./run_chaos_test.sh --duration 4 --tasks-count 8` and verify that no killed victim task writes `done:${SB_VICTIM}` after `kill -KILL`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-2` | Add per-task lifeline pipe, double-forked process-group supervisor, Linux `PR_SET_CHILD_SUBREAPER` / cgroup v2 / `/proc` `setsid` sweep, and `run_chaos_test.sh` victim completion rejection | `crates/worker/src/process_tree.rs`, `crates/worker/src/runner.rs`, `crates/worker/tests/test_worker_crash_containment.rs`, `run_chaos_test.sh`, `docs/design/0031-containment-after-worker-crash.md`, `docs/README.md` | A test starts the real worker, runs a script with a child and a grandchild (and `setsid` child), sends `kill -9` to the worker, and asserts all are dead within 5 s on Linux and macOS; `run_chaos_test.sh` fails if the victim writes `done:` after the kill | Planned |

## Status log

- 2026-11-08: Initial draft written for `FEAT-2` (Phase B of `docs/ROADMAP.md`).
