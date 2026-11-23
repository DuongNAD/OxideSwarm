# 0108: Windows task processes run without a console window

Status: Implemented
Created: 2026-11-23    Updated: 2026-11-23

## Summary

On Windows, every task process and every host probe the worker starts gets the creation flag
`CREATE_NO_WINDOW`, also under `--isolation off`. A console program then runs with a console that
has no window. The AppContainer path (`--isolation fs|fs+net`) already used this flag. Tests that
start helper processes on Windows use the same flag. That stops the console windows that appeared
during test loops, and `PING.EXE` is no longer left running after a test (TEST-88).

## Context

Incident `INC-20261123-001` on the case PC (Windows 11 Pro): `cargo test -p rusty_grid_worker` was
run from a script that captures output, which starts it without a console. Each run of about 138 s
opened 5 to 7 empty console or Windows Terminal windows (`OpenConsole.exe -Embedding`) that took the
keyboard focus. `PING.EXE` processes stayed alive for up to 120 s after the tests finished. The S9
loop was stopped after 19 of 50 runs, all of which had passed.

The causes:

- `configure_command` (`crates/worker/src/process_tree.rs`) set only the priority class and
  `CREATE_SUSPENDED` for `--isolation off`. A console task inherits the worker's console when the
  worker has one. When the worker has none (a test harness, a service, or a launcher that captures
  output), Windows creates a new console with a visible window for every task.
- `run_command_bounded` (`crates/core/src/capabilities.rs`, the host probes such as `nvidia-smi`
  and `cargo --version`) had the same gap.
- `test_a_task_cannot_signal_or_inspect_foreign_processes` and
  `test_console_and_terminal_injection_is_blocked` (`crates/worker/tests/test_feat3_os_isolation.rs`)
  started `cmd /C "ping -n 120 127.0.0.1 >nul"`, the second one with `CREATE_NEW_CONSOLE`. Killing
  `cmd.exe` left `PING.EXE` running.
- `tests/m3_challenger_2_stress.rs` started its sleeping grandchildren with `Start-Process
  powershell` without `-NoNewWindow`. Each call opened a window, and when the test killed the tree,
  Windows Terminal kept an error window (`0x800700e8`) open.
- Several Windows tests ran `cmd /C mklink /J`, `reg.exe`, `icacls.exe` or `subst.exe` without the
  flag.

## Goals

- A worker never opens a visible window for a task or a probe, with or without its own console.
- A Windows test run leaves no window and no stray process behind, and every assertion stays.

## Non-goals

- GUI programs. `CREATE_NO_WINDOW` affects only console programs. A task that opens a GUI window
  still does so.
- The Unix side, which has no console windows.

## Design

### D1 Task processes and host probes

`rusty_grid_core::capabilities::WINDOWS_CREATE_NO_WINDOW` (`0x0800_0000`) is ORed into the creation
flags of `configure_command` and of `run_command_bounded`. A task's stdout and stderr are pipes in
both cases, so the task loses no output. Its stdin is the same as before.

What changes for a user who starts the worker in a console window: a task no longer shares that
console. Ctrl+C in the worker's window reaches the worker only. The worker then stops its tasks
through their Job Objects, as it already did for AppContainer tasks and for tasks of a worker
without a console.

### D2 Tests

- The Windows module of `test_feat3_os_isolation.rs` starts every helper through `hidden_command`,
  which sets `CREATE_NO_WINDOW`.
- The two ping tests start `PING.EXE` directly, so killing the handle ends the process.
- The console injection test keeps its positive control: a process started with `CREATE_NO_WINDOW`
  has a console of its own without a window, and `AttachConsole(helper)` from an unconfined task
  still succeeds.
- `m3_challenger_2_stress.rs` passes `-NoNewWindow` to every `Start-Process`, so the grandchildren
  share the task's windowless console. The process tree and Job Object membership stay the same,
  and the containment assertions are unchanged. Its helper worker process gets `CREATE_NO_WINDOW`.
- The `mklink /J` helpers in `isolation.rs`, `blob_cache.rs` and `client/tests.rs` set the flag too.

## Compatibility

There is no wire change and no new flag. The task environment is unchanged.

## Security

A task can no longer attach to the console of a worker that was started in a console window, or
inject keystrokes into it, under `--isolation off`. That level makes no isolation promise. The
AppContainer path is unchanged.

## Test plan

- Windows: run `cargo test -p rusty_grid_worker` 20 times under 4 busy loops with
  `RUSTY_GRID_REQUIRE_APPCONTAINER=1` from a parent without a console
  (`creationflags=CREATE_NO_WINDOW`). Expected: 0 new console windows and 0 leftover `PING.EXE` or
  `powershell` processes after each run. `test_console_and_terminal_injection_is_blocked` passes,
  including its positive control.
- CI: the Windows jobs (clippy, `workspace-test`) compile and run the changed tests.

## Work items

- TEST-88 (`docs/BACKLOG.md`).

## Status log

- 2026-11-23: Implemented in the pull request of TEST-88. The Windows loop of the test plan runs on
  the case PC.
