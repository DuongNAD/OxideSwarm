# 0021: Private default sandbox base on Unix

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-11

## Summary

On Unix the default sandbox base is `<temp>/rusty_grid-<euid>/sandboxes`. The worker creates the
`rusty_grid-<euid>` directory with mode 700, and refuses it when another local user could change
what is inside it. On Windows the worker treats `base_dir` itself (and the workspace root) as the
private root: a missing directory is created with a protected owner-only DACL, an unsafe one
(junction or link, non-directory, volume without persistent ACLs, or owner outside the current
user, `SYSTEM` and `Administrators`) is refused, and an existing trusted-owner directory with wider
DACL grants logs a warning with a locale-independent SID `icacls` command. The check runs at
start-up and before each task touches the directory.

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

- Directories the operator names on Unix: `--sandbox-base-dir`, `RUSTY_GRID_SANDBOX_BASE_DIR`,
  `WorkerConfig::with_sandbox_base_dir` and `SandboxConfig::new`. The Android app passes its cache
  directory this way. On Unix these are used as given, as before. On Windows the check covers
  explicit sandbox bases as well, because installers such as the USB kit pass a folder on a data
  drive where new directories otherwise inherit `Authenticated Users: Modify`.
- Changing the default Windows sandbox path. It stays `<temp>\rusty_grid\sandboxes`; section D4
  describes how `ensure_private_dir` creates and checks it on Windows.
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

1. Refuse the temp directory that holds it when that directory belongs to a user other than root or
   the worker's user, or is writable by group or others without the sticky bit. Either way another
   user could rename the private root away and put their own in its place.
2. Create the private root with mode 700 if it is missing. An existing one is kept.
3. Read its metadata without following links. Refuse it when it is a symbolic link, is not a
   directory, belongs to another user, or has any group or other permission bit.

A refusal is `SandboxError::BaseDirUnsafe { path, reason, remedy }`. `path` is the directory at fault:
the temp dir for step 1, the private root for step 3. The message names it and gives the reason. It
also gives the fix for that case:
- For the temp dir: set `TMPDIR` to a directory only this user can write, or add the sticky bit.
- For the root: remove it.
- In both cases: name a private directory with `--sandbox-base-dir` or `RUSTY_GRID_SANDBOX_BASE_DIR`.

The worker never changes the mode of an existing directory, because another user may already have put
links into one that was open.

The check uses blocking file calls. `SandboxConfig::ensure_private_root` runs it on a blocking thread,
and `ensure_private_root_blocking` serves callers without a runtime, such as the Android bridge.

### D3 When the check runs

- `SandboxConfig::ensure_usable` runs it before it creates the base. `WorkerClient::run` calls
  `ensure_usable` at start-up, so a worker with an unsafe default base does not start.
- A temp cleaner (for example `systemd-tmpfiles`) may delete an idle private root while the worker
  runs, and another user may then create one. So the check runs again before each task touches the
  base:
  - `WorkerClient::handle_assign_task` runs it before it reclaims a leftover sandbox of the task.
  - It runs again before the task's input files are fetched into the blob cache, because the task may
    have waited for a slot in between.
  - `Sandbox::create` runs it before it creates the task sandbox.

  A refused task fails with `BaseDirUnsafe` and runs nowhere. Each check costs two `mkdir` and three
  `stat` calls.

The check applies on Unix only while the base lies inside the private root. Code on Unix that sets
`SandboxConfig::base_dir` to another directory (`WorkerClient::new` does this for an explicit
directory) therefore skips it.

### D4 Windows sandbox base and workspace root

Windows has no per-user root directory above `base_dir`. Instead `SandboxConfig::checked_private_root`
returns `Some(&self.base_dir)`, `ensure_locks_dir` recreates a missing base only through
`ensure_private_dir`, `TaskRunner::blob_cache` runs `ensure_private_root_blocking` before opening
`<base_dir>/cache/blobs`, and `WorkspaceStore` runs `ensure_private_dir` in both `open` and
`acquire`. Every start-up and per-task check from D3 therefore runs on Windows as well.

`ensure_private_dir` (`crates/worker/src/sandbox.rs`) calls `create_or_open_private_dir_all` and
`inspect_dir_security_no_follow` (`crates/core/src/private_file.rs`). Both inspect the entry
without following links (`symlink_metadata` and `CreateFileW` with
`FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT`):

1. **Creation and creation race.** When nothing exists at `root`, `CreateDirectoryW` creates it
   with the protected owner-only descriptor (`D:P` granting `(OI)(CI)FA` to the current user SID,
   `SYSTEM` and `BUILTIN\Administrators`). If another creator wins the race (`ERROR_ALREADY_EXISTS`),
   the outcome is `Existing` rather than an unchecked `Ok(())`, so all checks below run on the
   winner's entry.
2. **Hard refusals (`SandboxError::BaseDirUnsafe`).** `ensure_private_dir` refuses `root` when:
   - `FILE_ATTRIBUTE_REPARSE_POINT` is set (junction, symbolic link or mount point), or `root` is
     not a directory;
   - `GetVolumeInformationByHandleW` lacks `FILE_PERSISTENT_ACLS` (`FAT`, `exFAT` and non-ACL
     virtual drives);
   - the owner SID cannot be read or is outside `{current user, S-1-5-18 (SYSTEM), S-1-5-32-544
     (BUILTIN\Administrators)}`;
   - `CreateDirectoryW` created the directory in this call (`Outcome::Created`), yet reading back
     its security descriptor fails, shows `SE_DACL_PROTECTED == false`, or shows any sensitive
     grant in `dir_open_grants`. This catches cloud or virtual filesystems that advertise
     `FILE_PERSISTENT_ACLS` while silently dropping the security descriptor.
3. **Existing trusted-owner directories: warn rather than refuse on wider DACL grants.** When an
   existing real directory on a persistent-ACL volume is owned by the current user, `SYSTEM` or
   `Administrators`, `dir_open_grants` checks all five Windows allow ACE types (`ACCESS_ALLOWED`,
   `ACCESS_ALLOWED_CALLBACK`, `ACCESS_ALLOWED_COMPOUND`, `ACCESS_ALLOWED_OBJECT` and
   `ACCESS_ALLOWED_CALLBACK_OBJECT`). Any sensitive grant to a SID outside `{current user, SYSTEM,
   Administrators, S-1-3-0 (CREATOR OWNER), S-1-3-4 (OWNER RIGHTS)}` logs a `WARN` (never `DEBUG`,
   including when reading the DACL fails) with a locale-independent SID `icacls` command:
   `icacls "<path>" /inheritance:r /grant:r "*<user_sid>:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F"`.

   We chose **warn** rather than **refuse** for existing trusted-owner directories for two reasons:
   - Standard Windows 11 user profiles leave inherited `Everyone` and `BUILTIN\Users` read/execute
     entries on `%TEMP%` and `%LOCALAPPDATA%`. Refusing existing trusted-owner directories with
     inherited entries would break in-place upgrades of `0.1.0` and `0.1.1` workers whose sandbox
     base or workspace root already exists, and would reject caller-created temp directories passed
     to `SandboxConfig::new`.
   - Unprivileged local accounts lack `SeRestorePrivilege` and cannot assign ownership of a
     directory or junction to another user, `SYSTEM` or `Administrators`. Pre-creation squatting and
     `ERROR_ALREADY_EXISTS` races by another account are therefore caught unconditionally by the
     owner and reparse-point refusals in step 2.

   `DIR_SENSITIVE_MASK` leaves out traverse-only (`FILE_TRAVERSE | FILE_READ_ATTRIBUTES |
   SYNCHRONIZE` = `0x0010_00A0`), so the non-inherited `oxideswarm.toolchain` traverse entry that
   Windows task isolation (`protect_sandbox_base` in `crates/worker/src/isolation.rs`) places on
   `sandbox_base` and `workspace_root` does not trigger a warning.

## Compatibility

- No wire or CLI change.
- An existing `<temp>/rusty_grid/sandboxes` is no longer used, so its blob cache starts empty once.
  The worker leaves the old directory alone: it may belong to another user. Delete it by hand, or
  let the system's temp cleaner remove it.
- A host where a worker refused to start because another user owned `/tmp/rusty_grid` now works
  without settings.
- macOS already gave each user a private `TMPDIR`. The base there moves from `$TMPDIR/rusty_grid` to
  `$TMPDIR/rusty_grid-<euid>`, and the check passes.
- Android `adb shell` workers run as `shell` in `/data/local/tmp`, which is mode 771 and owned by
  `shell`. That is group-writable without the sticky bit, so the check refuses it. The launches that
  `build_android_worker.sh` prints and that `run_cluster_benchmark.sh` runs on a phone therefore pass
  `--sandbox-base-dir /data/local/tmp/rusty_grid/sandboxes`, the path they used before. The app names
  its cache directory as before. When the app names none, the bridge creates the private root with
  mode 700 itself (`check_sandbox_usable` in `crates/android_bridge/src/lib.rs`).
- Not changed here: a blob cache whose `tmp/` and `objects/` a temp cleaner removed is not created
  again. Tasks with new input files then fail until the worker restarts.

## Security

- No new input. The private root's name is predictable, so another user can still create it first.
  The worker then refuses to start. That user can stop the default worker this way, but cannot
  redirect its files. The message gives the fix.
- An attacker cannot take over the private root after the check, for two reasons. Only its owner
  (and root) can change entries in a directory with mode 700. The checked temp directory keeps others
  from renaming the root.
- An alternative was considered: a per-user cache directory, such as
  `$XDG_CACHE_HOME/oxideswarm/sandboxes`. It avoids the predictable name in a shared directory, and with
  it both the refusal another user can cause and the temp cleaner. But it moves scratch data and the
  2 GiB blob cache into the home directory, with its quotas, backups and network homes, and it changes
  the documented precedence. Termux workers already pass `~/.cache/oxideswarm/sandboxes`
  explicitly. It stays a candidate for a later change.

## Test plan

Unit tests in `crates/worker/src/sandbox.rs`, all Unix-only except the first and the last, and one in
`crates/worker/src/client.rs`.

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
  - `parent_refusal` refuses the temp dir for any other user. This part is skipped when the test runs
    as root.
  - `parent_refusal` accepts a root-owned temp dir (`/`) for every user.
- `test_default_base_refuses_an_open_temp_dir_without_sticky_bit`: a temp dir with mode 777 or 770
  gives `BaseDirUnsafe` for the temp dir. The same dir with mode 1777 is accepted.
- `test_sandbox_create_rechecks_the_private_root`:
  - A root removed after `ensure_usable` is created again with mode 700.
  - A root replaced by a link fails `Sandbox::create`, and nothing is written behind the link.
- `test_explicit_base_dir_is_used_as_given`:
  - `SandboxConfig::new` on a directory with mode 777 is usable.
  - A default configuration whose `base_dir` was set elsewhere skips the check, and does not create
    the private root.
- `test_base_from_the_variable_has_no_private_root`: `RUSTY_GRID_SANDBOX_BASE_DIR` and
  `SandboxConfig::new` give no private root.
- `client::tests::test_assign_refuses_a_task_before_touching_a_replaced_sandbox_root`: a task whose
  private root was replaced by a link is refused with `BaseDirUnsafe`. A leftover sandbox of that
  task behind the link is not reclaimed.
- `tests::test_bridge_check_creates_the_default_private_root_with_mode_700` in
  `crates/android_bridge/src/lib.rs`: the bridge's fallback creates the root with mode 700, and the
  worker accepts it. This test catches a missing call only under a umask that leaves group or other
  bits, such as the usual 022.

Disabling any refusal fails at least one of these tests. Disabling any call of the check fails one too,
with one exception: the second check in `handle_assign_task`'s task, before the input fetch, has no
test. A test for it would have to replace the root while the task waits for a slot.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-8 | Private default sandbox base on Unix | `crates/worker/src/sandbox.rs`, `crates/worker/src/client.rs`, `crates/android_bridge/src/lib.rs`, `build_android_worker.sh`, `run_cluster_benchmark.sh` | The tests above pass, and removing any one refusal fails one of them | Implemented |

## Status log

- 2026-11-04: Implemented D1 to D3, found while reviewing the cache cleanup of pull request 58
  (BUG-8).
- 2026-11-04: After review:
  - The check now runs when a task is assigned and before its inputs are fetched.
  - A refusal of the temp dir names that directory and its fix.
  - The Android launches pass `--sandbox-base-dir`, and the Android bridge creates the private root
    itself.
  - Tests were added for a group-writable temp dir and for the client.
- 2026-11-10: Windows. The USB kit trial put a worker's sandbox base on a data drive
  (`E:\OxideSwarm\work`), where a new folder inherits `Authenticated Users: Modify`. The Windows
  worker never checked its base. Now `ensure_usable` runs `ensure_private_dir` on the base itself
  (Windows has no private root above it), and so does the workspace root:
  - a missing directory is created with the protected owner-only DACL of SEC-9 (design 0047), so
    task folders inherit only the current user, `SYSTEM` and `Administrators`;
  - an existing directory is used as it is, with a warning that names the open groups (`Everyone`,
    `BUILTIN\Users`, `Authenticated Users`, `INTERACTIVE`, `BUILTIN\Guests`) and the `icacls`
    command that fixes it. It warns rather than refuses, as SEC-9 does for an existing secret file,
    so a worker that ran before keeps starting.

  `local_account_grants` (pure, tested on every system) picks the open groups; Windows-only tests
  check the created DACL and that an existing directory is left as it is.
- 2026-11-11: Windows review fixes (PR 207 findings S1 to S4):
  - S1: `ensure_private_dir` refuses junctions/reparse points, non-directories, and owners outside
    the current user, `SYSTEM` and `Administrators` (`BaseDirUnsafe`), warns at `WARN` when the
    DACL cannot be read, and replaces `local_account_grants` with `dir_open_grants` so any untrusted
    grantee SID (another local user, `Domain Users`, `NETWORK`, `ANONYMOUS LOGON`, `Remote Desktop
    Users`, `ALL APPLICATION PACKAGES`, as well as the broad local groups) triggers a warning while
    allowing `CREATOR OWNER`, `OWNER RIGHTS` and FEAT-3d's traverse-only `oxideswarm.toolchain`
    ACE. Documented in D4 why existing trusted-owner directories warn rather than refuse.
  - S2: `SandboxConfig::checked_private_root` returns `Some(&self.base_dir)` on Windows,
    `ensure_locks_dir` and `TaskRunner::blob_cache` go through `ensure_private_dir`, and
    `WorkspaceStore::acquire` re-checks the workspace root before each task.
  - S3: `create_or_open_private_dir_all` treats `ERROR_ALREADY_EXISTS` as `Existing` so the
    winner's entry undergoes the full reparse-point, volume, owner and DACL checks.
  - S4: `inspect_dir_security_no_follow` checks `FILE_PERSISTENT_ACLS` via
    `GetVolumeInformationByHandleW`, and newly created directories verify the read-back protected
    DACL, refusing `FAT`, `exFAT` and security-dropping virtual volumes.
  - Nits: `open_dir_acl_warning` uses locale-independent SIDs (`*<user_sid>`, `*S-1-5-18` and
    `*S-1-5-32-544`), and `read_dacl` parses all five Windows allow ACE types.
