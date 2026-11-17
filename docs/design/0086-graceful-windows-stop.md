# 0086: Stop the Windows master and worker with Ctrl+C before taskkill

Status: Implemented
Created: 2026-11-17    Updated: 2026-11-17

## Summary

`windows\oxctl.ps1 stop` (and every launcher that calls it: `stop_worker.cmd`, `switch_role.cmd`,
`start_master.cmd` before a restart) first sends a console Ctrl+C event to the tracked master or
worker, waits up to 15 s for it to exit, and only then runs `taskkill /F /T /PID`. It prints which way
the process stopped. A master with `--state-dir` therefore compacts its journal on `stop` (`BUG-23`).

## Context

- `Start-Tracked` (`windows\oxctl.ps1`) starts the master and the worker with
  `Start-Process -WindowStyle Hidden`, so each runs in its own hidden console. `Stop-Tracked` ended them
  with `taskkill.exe /F /T /PID`, a forced end.
- A forced end skips the handlers of `wait_for_worker_shutdown_signal` (`crates/cli/src/cli_app.rs`:
  Ctrl+C, Ctrl+Break, close and shutdown events), which both the master
  (`crates/cli/src/cli_app/master.rs`) and the worker (`crates/cli/src/cli_app/worker.rs`) wait on.
- The master compacts `journal.bin` into `snapshot.bin` on that clean path
  (`docs/design/0046-master-durability.md`). Across a protocol bump the next master refuses an
  uncompacted journal (`DEPLOYMENT.md` section 3.5). The Windows smoke test of 0.4.0 confirmed it: after
  `taskkill /F /T /PID` the 0.4.0 master refused the 0.3.0 journal, and after a Ctrl+C in the master's
  console the 3 tasks carried over (hub report `test_runs/win_smoke_0.4.0_118fd88.md`, S1 and S2).
- On the clean path the worker's `client.run` cancels its running tasks and their process trees end,
  then `terminate_all_task_process_trees()` runs as a net (`crates/cli/src/cli_app/worker.rs`).
- `switch_role.sh` already sends SIGTERM, waits 5 s, then SIGKILL.

## Goals

- **G1** `oxctl.ps1 stop` lets a master with `--state-dir` compact its journal.
- **G2** A process that does not answer still stops: after a grace period, the old forced end.
- **G3** The console that runs `oxctl.ps1` is never sent the event, and nothing is stopped by image
  name; the PID files and the recycled-PID guard stay.
- **G4** The output says whether the process stopped cleanly or was forced.

## Non-goals

- A stop request over the network or a named pipe. The console event needs no new interface.
- The Windows service wrapper (`packaging\windows`), which has its own stop path.
- A master or worker started by hand: `oxctl.ps1` only stops what its PID files name.

## Design

### D1. Sending Ctrl+C to one console

`Send-ConsoleCtrlC` starts a separate hidden `powershell.exe` with `-EncodedCommand`. That helper calls
`FreeConsole()`, `AttachConsole(<pid>)`, `SetConsoleCtrlHandler(NULL, TRUE)` (so the helper ignores the
event) and `GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0)`, which raises the event for every process
attached to the target's console: the tracked process and its console host. The helper exits with
`0` when the event was raised, `2` when the attach failed and `3` when the event failed; the caller
waits up to 20 s for it. The script that runs `oxctl.ps1` never attaches to another console, so its
own console gets no event (G3).

### D2. Grace period and fallback

`Stop-Tracked` waits up to `$StopGraceSeconds = 15` for the PID to exit after the event. If it exits,
the script prints `[OK] <name> stopped cleanly after Ctrl+C in <t> s.`. If the helper fails or the
process is still running, it prints a warning and runs `taskkill.exe /F /T /PID` as before, then says
so (for the master with a note that the journal was not compacted and how to compact it). The
recycled-PID guard (`Get-TrackedPid`) is checked again before the forced end.

Why 15 s: on the development desktop (Windows 11, 20 cores, NTFS) the time from the call to the exit
was 0.5 to 0.8 s, helper start included: a master with 3 tasks and with 1003 tasks (`journal.bin`
690949 B compacted into a `snapshot.bin` of 1052016 B), and a worker with a running
`Start-Sleep 60` task. The master keeps at most 10,000 terminal tasks by default
(`DEFAULT_MAX_TERMINAL_TASKS`, `crates/master/src/queue.rs`), so a full store is about ten times that
snapshot; 15 s leaves room for that on a slow disk, and the wait ends as soon as the process exits.

### D3. The worker gets the same stop

The worker's clean stop cancels its tasks and ends their process trees (Context). Checked with the
0.4.0 build: a worker started by `oxctl.ps1` with a running `powershell -Command Start-Sleep 60` task
stopped cleanly after Ctrl+C in 0.7 s, and no `Start-Sleep 60` process was left 1 s later. The worker
log shows `Worker shutdown initiated; notifying Master ... reason=Graceful worker shutdown` and
`RustyGrid Worker finished cleanly.`, so the master learns of the stop at once.

## Compatibility

- Works with every released binary: 0.3.0 and 0.4.0 already handle Ctrl+C. Only the script changes, so
  the improvement reaches a machine with the `oxctl.ps1` of the next release; for 0.3.0 and 0.4.0
  installs the manual way stays (`DEPLOYMENT.md` section 3.5).
- Windows PowerShell 5.1: the helper compiles the four `kernel32` declarations with
  `Add-Type -MemberDefinition` at each stop; the whole stop, helper included, took 0.5 to 0.8 s (D2).
  Where `Add-Type` is blocked (for example Constrained Language mode) the helper fails and
  `Stop-Tracked` falls back to `taskkill /F /T` after printing a warning; that case was not tested.

## Security

- No new input: the PID comes from the PID file and passes the existing guard (the process must still
  be `oxideswarm` or `rusty-grid`).
- The event reaches only the processes of the target's console. The helper runs as the same user
  and needs no elevation.

## Test plan

- `tests/test_launcher_scripts.py` `WindowsOxctlStopTests`:
  - `test_stop_tracked_sends_ctrl_c_before_taskkill` (every system): the helper makes the four calls,
    `Stop-Tracked` calls it before `taskkill.exe /F /T /PID`, the PID guard stays, and nothing kills by
    image name.
  - `test_stop_ends_master_and_worker_with_ctrl_c_and_falls_back_to_taskkill` (Windows; CI runs it in
    the `agent bridge (windows-latest)` job): a fake `oxideswarm.exe` writes a marker from its Ctrl+C
    handler. `start master` then `stop all`: both stop cleanly, both markers exist, no taskkill. A
    fake that ignores Ctrl+C: `stop master` warns after 15 s and ends it with `taskkill /F /T`.
  - With `OX_SCRIPT_ROOT` set to a checkout with the previous `oxctl.ps1`, both tests fail.
- Recorded run with the 0.4.0 binary and a private home, ports and state dir: `oxctl.ps1 start master`
  with `OX_EXTRA_ARGS="--state-dir <dir> --discovery-port 0"`, 3 tasks, `oxctl.ps1 stop master`:
  `journal.bin` 24 B and a `snapshot.bin`; `start master` again: `status` lists the 3 tasks. The same
  run with the previous script leaves `journal.bin` uncompacted and no `snapshot.bin`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Ctrl+C helper and graceful `Stop-Tracked` | `windows/oxctl.ps1` | A master with `--state-dir` leaves a compacted journal after `stop` | Implemented |
| W2 | Launcher test | `tests/test_launcher_scripts.py` | Clean and forced paths covered on Windows; fails on the previous script | Implemented |
| W3 | Docs | `docs/BACKLOG.md`, `CHANGELOG.md`, `DEPLOYMENT.md` | BUG-23 Implemented; the Windows sentence of section 3.5 names the release | Planned |

## Status log

- 2026-11-17: Drafted and implemented with `BUG-23` (W1, W2). W3 waits until the 0.4.0 release
  pull request, which adds the BUG-23 entry and the section 3.5 sentence, is merged.
