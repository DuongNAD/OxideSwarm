# 0021: Private default sandbox base on Unix

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

On Unix the default sandbox base is now `<temp>/rusty_grid-<euid>/sandboxes`.
The worker creates the `rusty_grid-<euid>` directory with mode 700. At start-up and before each task
sandbox it refuses that directory when another local user could change what is inside it.
A sandbox directory the operator names is used as given, and Windows is unchanged.

## Context

When nothing names a sandbox directory, `resolve_default_base_dir` (`crates/worker/src/sandbox.rs`)
picks `<TMPDIR>/rusty_grid/sandboxes`, else `<platform temp dir>/rusty_grid/sandboxes`. On Linux,
`TMPDIR` is usually unset, so this is `/tmp/rusty_grid/sandboxes`, inside the world-writable `/tmp`.

| Area | Behaviour before this change | Code |
|---|---|---|
| Creating the base | `SandboxConfig::ensure_usable` calls `create_dir_all` and writes a probe file. It accepts a directory that already exists, whoever owns it. | `crates/worker/src/sandbox.rs` |
| Creating a task sandbox | `Sandbox::create` runs `create_dir_all(<base>/<task id>)` and then sets mode 700 on the task directory only. | `crates/worker/src/sandbox.rs` |
| Two users on one host | The first worker creates `/tmp/rusty_grid` with mode 755. The second user's worker cannot write there and refuses to start (`BaseDirUnusable`). | `crates/worker/src/sandbox.rs` |

Another local user can create `/tmp/rusty_grid` before the worker's first start, and make it writable
for everyone. The worker then creates every task sandbox inside a directory that user owns. The owner
of a directory can rename and replace its entries. So that user can swap `<base>/<task id>` for a
symbolic link after the worker created it. The worker would then write the task's files into any
directory the worker's account can write, and collect outputs from it. Design 0003 states that the
cleanup of `<base>/cache` must not follow such links. That check covers the cleanup only.

## Goals

- **G1** On Unix, the default sandbox base belongs to the worker's user. No other user can create it
  in advance or change its entries.
- **G2** A worker refuses an unsafe default base with an error that names the directory, the reason
  and the fix. It never runs a task there.
- **G3** Two users on one host can each run a worker with the default settings.

## Non-goals

- Directories the operator names: `--sandbox-base-dir`, `RUSTY_GRID_SANDBOX_BASE_DIR`,
  `WorkerConfig::with_sandbox_base_dir` and `SandboxConfig::new`. The Android app passes its cache
  directory this way. These are used as given, as before.
- Windows. Its temp directory (`%LOCALAPPDATA%\Temp`) belongs to one user, and its access control
  lists are not Unix modes. The default stays `<temp>\rusty_grid\sandboxes`.
- Checking the directories above the temp directory. `TMPDIR` is trusted to name a real temp
  directory.
- Isolating a task from the worker's own account. A task still runs as that account (see
  `docs/SECURITY.md`, section 7).

## Design

### D1 Default path

On Unix, `resolve_default_base_dir` returns `<TMPDIR or platform temp>/rusty_grid-<euid>/sandboxes`.
`<euid>` is the worker's effective user id (`libc::geteuid`). The precedence is unchanged: an
explicit directory, then `RUSTY_GRID_SANDBOX_BASE_DIR`, then `TMPDIR`, then the platform temp dir.
`SandboxConfig::default()` also records `<temp>/rusty_grid-<euid>` as the private root. A base that
comes from `RUSTY_GRID_SANDBOX_BASE_DIR` has no private root.

### D2 The private root check

`ensure_private_dir` (`crates/worker/src/sandbox.rs`) works on the private root as follows.
`parent_refusal` and `root_refusal` hold the checks.

1. Refuse the temp directory above it when it belongs to a user other than root or the worker's user,
   or when it is writable by group or others without the sticky bit. Either way another user could
   rename the private root away and put their own in its place.
2. Create the private root with mode 700 if it is missing. An existing one is kept.
3. Read its metadata without following links. Refuse it when it is a symbolic link, is not a
   directory, belongs to another user, or has any group or other permission bit.

A refusal is `SandboxError::BaseDirUnsafe { path, reason }`. Its message names the directory and the
reason. It says how to fix it: remove the directory, or name a private one with `--sandbox-base-dir`
or `RUSTY_GRID_SANDBOX_BASE_DIR`. The worker never changes the mode of an existing directory, because
another user may already have put links into one that was open.

### D3 When the check runs

- `SandboxConfig::ensure_usable` runs it before it creates the base. `WorkerClient::run` calls
  `ensure_usable` at start-up, so a worker with an unsafe default base does not start.
- `Sandbox::create` runs it again before it creates each task sandbox. A temp cleaner (for example
  `systemd-tmpfiles`) may delete an idle private root while the worker runs, and another user may
  then create one. That task then fails with `BaseDirUnsafe` and runs nowhere. The check costs one
  `mkdir` and two `stat` calls per task.

The check applies only while the base lies inside the private root. Code that sets
`SandboxConfig::base_dir` to another directory (`WorkerClient::new` does this for an explicit
directory) therefore skips it.

## Compatibility

- No wire or CLI change.
- An existing `<temp>/rusty_grid/sandboxes` is no longer used, so its blob cache starts empty once.
  The worker leaves the old directory alone: it may belong to another user. Delete it by hand, or
  let the system's temp cleaner remove it.
- A host where a worker refused to start because another user owned `/tmp/rusty_grid` now works
  without settings.
- macOS already gave each user a private `TMPDIR`. The base there moves from `$TMPDIR/rusty_grid` to
  `$TMPDIR/rusty_grid-<euid>`, and the check passes.

## Security

- No new input. The private root's name is predictable, so another user can still create it first.
  The worker then refuses to start. That user can stop the default worker this way, but cannot
  redirect its files. The message gives the fix.
- An attacker cannot take over the private root after the check, for two reasons. Only its owner
  (and root) can change entries in a directory with mode 700. The checked temp directory keeps others
  from renaming the root.

## Test plan

Unit tests in `crates/worker/src/sandbox.rs`. All are Unix-only except the first and the last.

- `test_default_base_dir_resolution_order`: the default path carries `rusty_grid-<euid>` on Unix.
- `test_default_base_is_created_private`: `ensure_usable` creates the private root with mode 700, and
  the base inside it.
- `test_default_base_refuses_an_open_root`: mode 755 gives `BaseDirUnsafe`. The mode stays as it was,
  and nothing is created inside.
- `test_default_base_refuses_a_symlinked_root`: a link to a directory the test owns gives
  `BaseDirUnsafe`. Nothing is created behind the link.
- `test_default_base_refuses_a_root_that_is_a_file`: a file gives `BaseDirUnsafe`.
- `test_default_base_refuses_a_root_or_temp_dir_owned_by_another_user`:
  - `root_refusal` refuses the root for any other user.
  - `parent_refusal` refuses the temp dir for any other user.
  - `parent_refusal` accepts a root-owned temp dir (`/`) for every user.
- `test_default_base_refuses_an_open_temp_dir_without_sticky_bit`: a temp dir with mode 777 gives
  `BaseDirUnsafe`. The same dir with mode 1777 is accepted.
- `test_sandbox_create_rechecks_the_private_root`:
  - A root removed after `ensure_usable` is created again with mode 700.
  - A root replaced by a link fails `Sandbox::create`, and nothing is written behind the link.
- `test_explicit_base_dir_is_used_as_given`:
  - `SandboxConfig::new` on a directory with mode 777 is usable.
  - A default configuration whose `base_dir` was set elsewhere skips the check, and does not create
    the private root.
- `test_base_from_the_variable_has_no_private_root`: `RUSTY_GRID_SANDBOX_BASE_DIR` and
  `SandboxConfig::new` give no private root.

Each refusal and each call of the check is covered by a mutation: disabling it fails at least one of
these tests.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-8 | Private default sandbox base on Unix | `crates/worker/src/sandbox.rs` | The tests above pass, and removing any one refusal fails one of them | Implemented |

## Status log

- 2026-11-04: Implemented D1 to D3, found while reviewing the cache cleanup of pull request 58
  (BUG-8).
