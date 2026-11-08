# 0031: Containment after a worker crash

Status: Accepted
Created: 2026-11-08    Updated: 2026-11-08

## Summary

- On Unix (`Linux`, `macOS`, `Android`), each task process group gets a small supervisor process that holds the read end of an anonymous lifeline pipe from the worker and runs `killpg` when that pipe reaches end of file, replacing Linux's thread-bound `PR_SET_PDEATHSIG` (`FEAT-2` in [BACKLOG.md](../BACKLOG.md) and Phase B of [ROADMAP.md](../ROADMAP.md)).
- On Linux, when a writable cgroup v2 subtree is delegated to the worker's user, the task leader moves itself into a per-task cgroup v2 leaf in `pre_exec` before `execve` so `<leaf>/cgroup.kill` terminates every descendant including `setsid()` processes; without delegated cgroup v2 (and on macOS and Android), a best-effort environment sweep kills `setsid()` descendants that still carry `RUSTY_GRID_TASK_ID`.
- After `kill -9` (`SIGKILL`) of a worker, the kernel closes the worker's write end of every task lifeline pipe, and each supervisor terminates its task group within 5 s so an orphaned task cannot keep running while the master runs it elsewhere.
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
- **G3** When a task ends or the worker crashes, descendants that leave the task's process group (`setsid()` or `setpgid`) are contained in full on Linux when a cgroup v2 subtree is delegated to the worker's user; when cgroup v2 is not delegated (and on macOS and Android), they are contained on a best-effort basis by matching `RUSTY_GRID_TASK_ID` in the process environment, which a process started with a cleared environment (`env -i`) can escape.
- **G4** The supervisor never holds a task's `stdin`, `stdout`, or `stderr` pipes open and is never a child of the task leader, so `waitpid(-1)` inside the task process returns `ECHILD` immediately when the task has no children of its own.
- **G5** `run_chaos_test.sh` fails if a task that was running on the victim worker at the time of `kill -KILL` writes a `done:${SB_VICTIM}` marker line after the kill.

## Non-goals

- Changing the Windows Job Object supervision path (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` already terminates the job tree in the kernel when the worker process dies).
- Filesystem or network sandboxing of running tasks (`FEAT-3`, design 0033).
- Surviving a worker restart and adopting the still-running task (`FEAT-1`).

## Design

### D1 Lifeline pipe between worker and task supervisor (`crates/worker/src/process_tree.rs`)

For each task command on Unix:

1. Before `cmd.spawn()` in `execute_process_supervised` (`crates/worker/src/runner.rs:2640`), `configure_command` creates an anonymous lifeline pipe `(lifeline_read_fd, lifeline_write_fd)` with `FD_CLOEXEC` set on both descriptors:
   - On Linux and Android, `libc::pipe2(fds, libc::O_CLOEXEC)` sets `FD_CLOEXEC` atomically.
   - On macOS (which has no `pipe2`), `libc::pipe(fds)` followed by `libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC)` is not atomic against a concurrent `fork` on another thread. A process-wide mutex (`SPAWN_LOCK: Mutex<()>`) is therefore held both across `pipe` + `fcntl(FD_CLOEXEC)` and across every task `cmd.spawn()` call on macOS, so no task `fork` can occur while any lifeline pipe descriptor lacks `FD_CLOEXEC`.
2. `ProcessTree` stores `lifeline_write: Option<std::os::unix::io::OwnedFd>` alongside `id: Option<RawId>`.
3. Because `lifeline_write_fd` has `FD_CLOEXEC`, neither the task child nor any concurrently spawned task child inherits the write end across `execve`. Only the worker process holds `lifeline_write_fd`.
4. When `ProcessTree` is dropped in a live worker, dropping `lifeline_write` closes the write end after `imp::kill(id)` runs. When the worker crashes or is killed with `SIGKILL`, the kernel closes all file descriptors of the worker process, immediately delivering EOF (`read == 0`) on `lifeline_read_fd`.

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
   - Close every file descriptor except `lifeline_read_fd` (and the optional cgroup `cgroup.kill` descriptor on Linux, D3.1), using `close_range` on Linux/Android and `fcntl` / bounded descriptor loop on macOS. This closes the supervisor's copy of `lifeline_write_fd`, the task's `stdin`/`stdout`/`stderr` pipes, and Rust stdlib's internal `CLOEXEC` exec-error pipe so `cmd.spawn()` in the worker returns without waiting on the supervisor.
   - Block in `libc::read(lifeline_read_fd, &mut byte, 1)` (retrying on `EINTR`) until EOF (`<= 0`).
   - On EOF:
     1. Send `libc::killpg(pgid, libc::SIGTERM)`.
     2. Poll whether `libc::killpg(pgid, 0) == 0` in 20 ms steps up to 200 ms (`stop_grace`).
     3. Send `libc::killpg(pgid, libc::SIGKILL)`.
     4. Run the session-escape cleanup (D3).
     5. Close remaining descriptors and call `libc::_exit(0)`.

### D3 Containing `setsid()` descendants on Linux, Android, and macOS (`crates/worker/src/process_tree.rs`)

A child that calls `setsid()` or `setpgid(0, 0)` moves out of `pgid` and is not reached by `libc::killpg(pgid, SIGKILL)`. Two mechanisms handle it:

1. **Linux cgroup v2 leaf before `execve` (when delegated):**
   - At worker start-up on Linux, `process_tree` probes `/proc/self/cgroup` and `/sys/fs/cgroup` to check whether a writable cgroup v2 directory is delegated to the worker's effective UID.
   - When writable, before `cmd.spawn()` the worker creates a leaf cgroup directory `<delegated_cgroup>/oxideswarm-task-<task_id>` and opens `<leaf>/cgroup.procs` and `<leaf>/cgroup.kill` with `O_WRONLY | O_CLOEXEC`.
   - Inside `cmd.pre_exec` — **after** double-forking supervisor C (so C stays outside the task leaf cgroup) and **before** `execve` — the task leader A writes its own decimal PID (`libc::getpid()`) to `<leaf>/cgroup.procs` and closes that descriptor. Because the move happens before `execve`, the task leader and every child or grandchild it ever forks (including `setsid()` and `env -i` processes) are born inside `<leaf>`.
   - On task stop (`ProcessTree::kill`) or lifeline EOF in supervisor C, writing `"1"` to `<leaf>/cgroup.kill` (Linux 5.14+, falling back to reading `<leaf>/cgroup.procs` and sending `SIGKILL` to each PID) kills every descendant in the cgroup regardless of `setsid()` or environment changes, after which `<leaf>` is removed with `rmdir`.
2. **Environment tag scan (`RUSTY_GRID_TASK_ID`) when cgroup v2 is not delegated (Linux, Android, and macOS):**
   - `execute_process_supervised` injects `RUSTY_GRID_TASK_ID=<uuid>` into every task's environment (`crates/worker/src/runner.rs:2621`).
   - Both `ProcessTree::kill` (on normal task exit, cancellation, or timeout) and supervisor C (on lifeline EOF) run a post-`killpg` sweep for same-UID processes outside `pgid` whose environment still contains `RUSTY_GRID_TASK_ID=<task_id>` (excluding the worker and the supervisor):
     - On Linux and Android (when cgroup v2 is unavailable): scans `/proc/<pid>/environ` for same-UID processes matching `RUSTY_GRID_TASK_ID=<task_id>\0` and sends `libc::kill(pid, libc::SIGKILL)`.
     - On macOS: queries same-UID PIDs via `libc::proc_listpids` and inspects each process's environment block via `libc::sysctl` (`[CTL_KERN, KERN_PROCARGS2, pid]`) for `RUSTY_GRID_TASK_ID=<task_id>\0`, sending `libc::kill(pid, libc::SIGKILL)` to any match.

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
- Limitation without delegated cgroup v2: on macOS, Android, and Linux hosts without a writable cgroup v2 subtree, `setsid()` containment relies on `RUSTY_GRID_TASK_ID` in `/proc/<pid>/environ` or `KERN_PROCARGS2`. A process that both calls `setsid()` and strips `RUSTY_GRID_TASK_ID` from its environment (for example `env -i setsid ...`) escapes that scan, and scanning `/proc` or `sysctl` by PID carries a theoretical PID-reuse window between reading `environ` and `kill(pid, SIGKILL)`. Full kernel-enforced containment on Linux without cgroup v2 delegation requires PID/mount namespaces (`FEAT-3`, design 0033).

## Test plan

- **Unit tests (`crates/worker/src/process_tree.rs`):**
  - Lifeline pipe creation sets `FD_CLOEXEC` on both ends (and holds `SPAWN_LOCK` on macOS).
- **Integration tests (`crates/worker/tests/test_worker_crash_containment.rs`):**
  1. **Direct `waitpid(-1)` (`ECHILD`) test (Item 1 of review):** Runs `python3 -c "import os, errno; ... os.wait()"` (or a small Rust test helper calling `libc::waitpid(-1, ..., 0)` directly, not shell `wait`) inside a supervised task on Linux and macOS and asserts that `waitpid(-1)` returns `ECHILD` immediately, proving the supervisor is not a child of the task leader.
  2. **Normal task completion with a `setsid` survivor (Item 7 of review):** Runs a task that spawns a background child in a new session (`python3 -c "import os, time; os.setsid(); time.sleep(60)"`), records its PID, and exits 0. Asserts that `ProcessTree::kill` on normal task completion kills the `setsid` child within 1 s on Linux and macOS.
  3. **Worker `SIGKILL` containment test ("Done when" of `FEAT-2`):** Starts a real `rusty-grid` worker subprocess connected to an in-process master, submits a shell task that spawns a child and a grandchild (plus a `setsid` background process) writing their PIDs to a marker file and sleeping, sends `kill -9` (`SIGKILL`) to the worker PID once all descendant PIDs are recorded, and asserts that the worker, child, grandchild, and `setsid` descendant are all dead (`kill(pid, 0)` returns `ESRCH`) within 5 s on Linux and macOS.
- **Chaos script (`run_chaos_test.sh`):**
  - Run `./run_chaos_test.sh --duration 4 --tasks-count 8` and verify that no killed victim task writes `done:${SB_VICTIM}` after `kill -KILL`, and that injecting a `done:${SB_VICTIM}` line causes `run_chaos_test.sh` to fail.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-2` | Add per-task lifeline pipe, double-forked process-group supervisor, pre-`execve` Linux cgroup v2 leaf + `RUSTY_GRID_TASK_ID` `setsid` sweep, and `run_chaos_test.sh` victim completion rejection | `crates/worker/src/process_tree.rs`, `crates/worker/src/runner.rs`, `crates/worker/tests/test_worker_crash_containment.rs`, `tests/cli_worker_signals_e2e.rs`, `run_chaos_test.sh`, `docs/design/0031-containment-after-worker-crash.md`, `docs/README.md` | Direct `waitpid(-1)` in a task returns `ECHILD` at once; normal completion kills a `setsid` child; a test starts the real worker, runs a script with a child and a grandchild, sends `kill -9` to the worker, and asserts all are dead within 5 s on Linux and macOS; `run_chaos_test.sh` fails if the victim writes `done:` after the kill | Implemented |

## Status log

- 2026-11-08: Initial draft written for `FEAT-2` (Phase B of `docs/ROADMAP.md`).
- 2026-11-08: Addressed review `116-fix`: dropped `PR_SET_CHILD_SUBREAPER` and `PR_SET_PDEATHSIG` from the task leader, moved Linux cgroup v2 leaf assignment into `pre_exec` before `execve`, held `SPAWN_LOCK` across `pipe` + `fcntl(FD_CLOEXEC)` and `cmd.spawn()` on macOS, chose `KERN_PROCARGS2` on macOS and stated the `env -i` + `setsid` limitation in G3 and Security, documented Android's 32 phantom-process limit in Compatibility, and added the direct `waitpid(-1)` (`ECHILD`) and normal-exit `setsid` cleanup tests to the Test plan.
- 2026-11-08: Design accepted (PR #116 merged) and implemented in `crates/worker/src/process_tree.rs`, `crates/worker/src/runner.rs`, `crates/worker/tests/test_worker_crash_containment.rs`, `tests/cli_worker_signals_e2e.rs`, and `run_chaos_test.sh`.
