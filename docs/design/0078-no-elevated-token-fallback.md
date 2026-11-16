# 0078: Refuse instead of falling back to the elevated token under Windows isolation (`SEC-11`)

Status: Implemented
Created: 2026-11-16    Updated: 2026-11-16

## Summary

An elevated Windows worker starts an AppContainer task from a restricted (LUA) token through `CreateProcessAsUserW`. When that path fails, the worker no longer calls `CreateProcessW` with its own elevated token. The spawn fails with a message that names the cause and the Win32 error, the reason is logged once per worker process, and no task process starts. A worker that is not elevated keeps `CreateProcessW`.

## Context

`spawn_with_security_capabilities` in `crates/worker/src/isolation.rs` (`pub mod windows`) asked `get_or_init_worker_lua_token` for a LUA token. If it returned `None`, or `CreateProcessAsUserW` returned 0, the code called `CreateProcessW`. That call builds the child token from the worker's own token, which is the elevated one on a worker started as Administrator. Nothing was logged. The child still ran in the AppContainer, but with a token derived from the elevated token that the LUA path exists to avoid ([design 0033](0033-os-level-task-isolation.md), round 7).

## Goals

- **G1**: When the worker is elevated and has no LUA token, fail the spawn.
- **G2**: When the worker is elevated and `CreateProcessAsUserW` fails, fail the spawn with the Win32 error.
- **G3**: Log the reason once per worker process at `warn`.
- **G4**: Keep `CreateProcessW` for a worker that is not elevated.

## Non-goals

- Changing how the LUA token is built or cached.
- Making the LUA path succeed on more hosts. A host where it fails now refuses isolated tasks instead of running them.

## Design

### D1 Decision functions

Three small functions in `crates/worker/src/isolation.rs` hold the decision and compile on every OS: `plan_token_spawn(elevated, lua_token_available)` returns `Lua`, `Plain` or `RefuseNoLuaToken`; `after_lua_spawn_failure(elevated, win32_error)` returns the refusal message for an elevated worker and `None` otherwise; `elevated_token_refusal_message(cause, win32_error)` builds the text.

### D2 Spawn path

The spawn calls `plan_token_spawn`. On `Lua` it calls `CreateProcessAsUserW`; if that fails it asks `after_lua_spawn_failure`. A refusal becomes an `io::Error` of kind `PermissionDenied`. The error text is stable:

```text
refusing to start the task with the worker's elevated token: <cause> (Win32 error N)
```

The text for a missing LUA token has no Win32 error part. The error travels the same way as any spawn error to the `TaskResult` the master sees. The first refusal in a worker process is logged at `warn` (an `AtomicBool`).

### D3 Test seam

A `#[cfg(test)]` thread-local `lua_test_hook` makes the spawn behave as an elevated worker whose LUA token is missing (`NO_TOKEN`) or invalid (`BAD_TOKEN`, an invalid handle, so `CreateProcessAsUserW` really fails). It is per thread, so parallel tests are not affected, and it does not exist in a build without `cfg(test)`. There is no environment variable or CLI switch.

## Compatibility

No wire change. A worker on a host where the LUA path fails now fails tasks under `--isolation fs|fs+net` where it used to run them with the weaker token.

## Security

Closes a silent downgrade: the child token of an elevated worker always comes from the LUA path or the task does not run.

## Test plan

- `test_plan_token_spawn_never_falls_back_to_plain_when_elevated` and `test_after_lua_spawn_failure_refuses_only_when_elevated` (all OSes).
- `test_windows_elevated_worker_refuses_when_lua_token_is_missing` and `test_windows_elevated_worker_refuses_when_create_process_as_user_fails` (Windows): the spawn returns the error naming the cause, and a marker file the child would write is absent.
- The existing Windows isolation tests must keep passing on `windows-latest`, whose runner is elevated.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `SEC-11` | Refuse instead of the elevated fallback | `crates/worker/src/isolation.rs` | A Windows test forces the LUA path to fail and sees the spawn fail with the cause and no process | Implemented |

## Status log

- 2026-11-16: Implemented (pull request 256).
