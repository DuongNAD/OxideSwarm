# 0021: Private task directories

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-11

## Summary

On Unix the default sandbox base is `<temp>/rusty_grid-<euid>/sandboxes`, created with mode `700` and refused when another user could alter it or its parent. On Windows every sandbox base and workspace root (default or explicit) and its ancestor chain are checked without following reparse points: missing directories and ancestors get a protected owner-only DACL, unsafe ancestors, reparse points, non-ACL volumes, untrusted owners and foreign write grants are refused, and read-only foreign grants warn once per path. The check runs at start-up and before each task.

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
  explicit sandbox bases and workspace roots as well, because installers such as the USB kit pass a
  folder on a data drive where new directories otherwise inherit `Authenticated Users: Modify`.
- Changing the default Windows sandbox path. It stays `<temp>\rusty_grid\sandboxes`; section D4
  describes how `ensure_private_dir` creates and checks it and its ancestor chain on Windows.
- Checking the directories above the temp directory on Unix. `TMPDIR` is trusted to name a real temp
  directory.
- Defending against local Windows administrators (`SYSTEM`, `BUILTIN\Administrators`, and
  `NT SERVICE\TrustedInstaller` on system ancestors), or replacing Win32 path-based calls inside
  `<base>\<task_id>` with `NtCreateFile` relative directory handles. Instead, `check_windows_ancestor`
  refuses non-admin rename rights on every ancestor up to the drive root. An ancestor such as
  `C:\ProgramData` that grants `BUILTIN\Users` `FILE_ADD_SUBDIRECTORY` without `DELETE` or
  `FILE_DELETE_CHILD` cannot rename an existing child, though another local user could pre-create a
  missing child first (refused by the owner check) or place sibling files in an unprotected shared
  parent (for example `.cargo\config.toml`, which `cargo` reads upward). `install_windows_service.ps1`
  therefore locks `C:\ProgramData\OxideSwarm` as well as `C:\ProgramData\OxideSwarm\data`.
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
`ensure_private_dir_recheck`, `TaskRunner::ensure_blob_cache` runs `ensure_private_root_recheck_blocking`
on `spawn_blocking` before opening `<base_dir>/cache/blobs` (retrying on a later task if an earlier
open failed), and `WorkspaceStore` runs `ensure_private_dir` in `open` and `ensure_private_dir_recheck`
in `acquire`. Every start-up and per-task check from D3 therefore runs on Windows as well, and
`is_real_owned_dir`, `is_real_owned_file`, and `is_real_owned_open_file` verify both absence of
`FILE_ATTRIBUTE_REPARSE_POINT` and trusted ownership (`is_path_trusted_owner_no_follow` and
`is_handle_trusted_owner`).

`ensure_private_dir` (`crates/worker/src/sandbox.rs`) calls `check_windows_ancestor`,
`create_or_open_private_dir_all`, and `inspect_dir_security_no_follow`
(`crates/core/src/private_file.rs`). Every check inspects entries without following links
(`symlink_metadata` and a single `CreateFileW` handle opened with
`FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT`, requesting `READ_CONTROL` and falling
back to desired access `0` on `PermissionDenied`):

1. **Ancestor chain and private ancestor creation.** Before and after opening or creating `root`,
   `check_windows_ancestor` walks every existing ancestor from the drive root down to `root`'s
   parent (porting `Test-RootSafety` from `packaging/usb/install.ps1`). It refuses with
   `SandboxError::BaseDirUnsafe` when an ancestor below the drive root has
   `FILE_ATTRIBUTE_REPARSE_POINT` (every reparse tag is refused: junctions, symbolic links, mount
   points, OneDrive/Cloud Files placeholders, NTFS deduplication, and WOF compressed entries), sits
   on a volume without `FILE_PERSISTENT_ACLS`, has an owner outside `{current user, SYSTEM,
   BUILTIN\Administrators, NT SERVICE\TrustedInstaller}`, or grants any foreign SID an effective
   `ANCESTOR_RENAME_MASK` right (`DELETE | FILE_DELETE_CHILD | WRITE_DAC | WRITE_OWNER |
   GENERIC_ALL`, ignoring `INHERIT_ONLY` ACEs that do not apply to the ancestor itself). Drive roots
   are refused when a foreign SID holds `DRIVE_ROOT_UNSAFE_MASK` (`FILE_DELETE_CHILD | WRITE_DAC |
   WRITE_OWNER | GENERIC_ALL`). Missing intermediate ancestors are created top-down via
   `create_missing_private_ancestors` (`CreateDirectoryW` with the protected owner-only descriptor,
   never `create_dir_all`).
2. **Creation, creation race, and hard refusals (`SandboxError::BaseDirUnsafe`).** When nothing
   exists at `root`, `CreateDirectoryW` creates it with the protected owner-only descriptor (`D:P`
   granting `(OI)(CI)FA` to the current user SID, `SYSTEM`, and `BUILTIN\Administrators`). If
   another creator wins the race (`ERROR_ALREADY_EXISTS`), the outcome is `Existing` so all checks
   below run on the winner's entry. `ensure_private_dir` refuses `root` when:
   - `FILE_ATTRIBUTE_REPARSE_POINT` is set (junctions, symbolic links, mount points, OneDrive/Cloud
     Files placeholders, NTFS deduplication, or WOF entries), or `root` is not a directory;
   - `GetVolumeInformationByHandleW` lacks `FILE_PERSISTENT_ACLS` (`FAT`, `exFAT`, and non-ACL
     virtual drives; real Win32 failures from `GetVolumeInformationByHandleW` surface as
     `BaseDirUnusable` with the OS error);
   - the owner SID cannot be read or is outside `{current user, S-1-5-18 (SYSTEM), S-1-5-32-544
     (BUILTIN\Administrators)}`;
   - `CreateDirectoryW` created `root` in this call (`Outcome::Created`), yet reading back its
     security descriptor fails, shows `SE_DACL_PROTECTED == false`, or shows any grant in
     `dir_open_grants` (catching virtual filesystems that advertise `FILE_PERSISTENT_ACLS` while
     dropping the security descriptor);
   - `root` already existed (`Outcome::Existing`) and `dir_write_grants` finds any foreign SID
     (outside `{current user, SYSTEM, Administrators, S-1-3-0 (CREATOR OWNER), S-1-3-4 (OWNER
     RIGHTS)}`) holding a write-class right (`DIR_WRITE_MASK`: `FILE_ADD_FILE |
     FILE_ADD_SUBDIRECTORY | FILE_WRITE_EA | FILE_DELETE_CHILD | FILE_WRITE_ATTRIBUTES | DELETE |
     WRITE_DAC | WRITE_OWNER | GENERIC_WRITE | GENERIC_ALL`, including `INHERIT_ONLY` ACEs that
     children inside `root` would inherit).
3. **Read-only foreign grants on existing trusted-owner directories: warn once.** Standard Windows 11
   user profiles protect `C:\Users\<name>`, `%LOCALAPPDATA%`, and `%TEMP%` to `{current user,
   SYSTEM, Administrators}` (or, on profiles upgraded from older builds, inherit at most read-only
   `RX` entries without write rights), so refusing foreign write grants in step 2 breaks neither
   `0.1.x` default bases, USB kit work folders, nor `tempfile` tests, while rejecting open shared
   folders such as an unhardened `C:\ProgramData\OxideSwarm\data` (`install_windows_service.ps1`
   locks `$DataDir` with `/inheritance:r` to `SYSTEM`, `Administrators`, and the service SID). When
   an existing trusted-owner directory has no foreign write grant, `dir_read_grants`
   (`DIR_READ_MASK`: `FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | READ_CONTROL | GENERIC_READ`)
   checks all five Windows allow ACE types (`ACCESS_ALLOWED`, `ACCESS_ALLOWED_CALLBACK`,
   `ACCESS_ALLOWED_COMPOUND`, `ACCESS_ALLOWED_OBJECT`, and `ACCESS_ALLOWED_CALLBACK_OBJECT`,
   bounds-checked via unaligned reads against both `AclSize` and `AclBytesInUse`). Any foreign
   read-only grant — or an unreadable DACL — logs a `WARN` at most once per normalized path per
   process (`claim_warning_for_path`, suppressed on per-task `ensure_private_dir_recheck` calls)
   with an unlocalized SID `icacls` command that also strips explicit foreign ACEs:
   `icacls "<path>" /inheritance:r /grant:r "*<user_sid>:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F" /remove:g "*<foreign_sid>"`.

   `DIR_SENSITIVE_MASK` and `SENSITIVE_MASK` exclude `GENERIC_EXECUTE`, `FILE_READ_EA`, and
   traverse-only (`FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE` = `0x0010_00A0`), so neither
   execute/EA-only entries nor the non-inherited `oxideswarm.toolchain` traverse entry placed by
   Windows task isolation (`protect_sandbox_base` in `crates/worker/src/isolation.rs`) triggers a
   refusal or warning.

## Compatibility

- No wire or CLI change.
- An existing `<temp>/rusty_grid/sandboxes` on Unix is no longer used, so its blob cache starts
  empty once. The worker leaves the old directory alone: it may belong to another user. Delete it by
  hand, or let the system's temp cleaner remove it.
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
- On Windows, an existing sandbox base or workspace root that grants write access to another account
  (for example a manually created folder on a data root that inherited `Authenticated Users: Modify`,
  or an older `C:\ProgramData\OxideSwarm\data` service folder before `Set-PrivateDirAcl`) is refused
  at start-up with the exact `icacls` command to lock it down; re-running `install_windows_service.ps1`
  or `packaging/usb/install.ps1` applies the private DACL automatically.
- Not changed here: a blob cache whose `tmp/` and `objects/` a temp cleaner removed is not created
  again. Tasks with new input files then fail until the worker restarts.

## Security

- No new input. The private root's name is predictable, so another user can still create it first.
  The worker then refuses to start. That user can stop the default worker this way, but cannot
  redirect its files. The message gives the fix.
- An attacker cannot take over the private root after the check, for two reasons. Only its owner
  (and root, or `SYSTEM`/`Administrators` on Windows) can change entries in a private directory, and
  the checked parent/ancestor chain prevents non-admin users from renaming an ancestor and swapping
  the directory in between checks.
- An alternative was considered: a per-user cache directory, such as
  `$XDG_CACHE_HOME/oxideswarm/sandboxes`. It avoids the predictable name in a shared directory, and with
  it both the refusal another user can cause and the temp cleaner. But it moves scratch data and the
  2 GiB blob cache into the home directory, with its quotas, backups and network homes, and it changes
  the documented precedence. Termux workers already pass `~/.cache/oxideswarm/sandboxes`
  explicitly. It stays a candidate for a later change.

## Test plan

Unit tests in `crates/worker/src/sandbox.rs`, `crates/core/src/private_file.rs`,
`crates/worker/src/runner.rs`, and `crates/worker/src/client.rs`:

- Unix tests (`crates/worker/src/sandbox.rs`, `crates/worker/src/client.rs`,
  `crates/android_bridge/src/lib.rs`):
  - `test_default_base_dir_resolution_order`: the default path carries `rusty_grid-<euid>` on Unix.
  - `test_default_base_is_created_private`: `ensure_usable` creates the private root with mode 700,
    and the base inside it.
  - `test_default_base_refuses_an_open_root`: mode 755 gives `BaseDirUnsafe`.
  - `test_default_base_refuses_a_symlinked_root`: a link to a directory gives `BaseDirUnsafe`.
  - `test_default_base_refuses_a_root_that_is_a_file`: a file gives `BaseDirUnsafe`.
  - `test_default_base_refuses_a_root_or_temp_dir_owned_by_another_user`: `root_refusal` and
    `parent_refusal` refuse untrusted owners while accepting `/`.
  - `test_default_base_refuses_an_open_temp_dir_without_sticky_bit`: mode 777/770 temp dirs are
    refused; mode 1777 is accepted.
  - `test_sandbox_create_rechecks_the_private_root`, `test_explicit_base_dir_is_used_as_given`,
    `test_base_from_the_variable_has_no_private_root`,
    `client::tests::test_assign_refuses_a_task_before_touching_a_replaced_sandbox_root`, and
    `android_bridge::tests::test_bridge_check_creates_the_default_private_root_with_mode_700`.
- Cross-platform and Windows tests (`crates/core/src/private_file.rs`,
  `crates/worker/src/sandbox.rs`, `crates/worker/src/runner.rs`, `tests/test_launcher_scripts.py`):
  - `dir_open_grants_catches_untrusted_sids_while_allowing_trusted_and_toolchain_traverse`,
    `entries_without_data_access_including_generic_execute_and_read_ea_are_ignored`, and
    `ancestor_unsafe_grants_skips_inherit_only_and_catches_rename_rights`: verify write vs read
    masks (`dir_write_grants`, `dir_read_grants`), `GENERIC_EXECUTE`/`FILE_READ_EA`/traverse
    exclusions, and ancestor rename masks.
  - `parse_dacl_aces_extracts_all_five_windows_allow_ace_types` and
    `parse_dacl_aces_rejects_malformed_acl_running_past_bounds_without_panic`: parse all five allow
    ACE types and reject malformed ACL headers or ACEs running past `AclBytesInUse`/`AclSize`
    without panicking.
  - `icacls_printed_remedies_remove_both_inherited_and_explicit_foreign_grants` and
    `s1_write_grants_are_refused_and_read_only_grants_warn_once_with_working_remedy`: foreign write
    grants (including `ProgramData`-style `(CI)(WD,AD,WEA,WA)`) are refused, foreign read-only
    grants warn once per path while per-task re-checks stay silent, and executing the printed
    `icacls` command leaves zero foreign ACEs.
  - `s1_junction_and_file_bases_are_refused`, `s1_base_owned_by_untrusted_sid_is_refused`,
    `s2_parent_chain_refuses_mutable_or_reparse_ancestors_and_creates_missing_ancestors_privately`,
    `s2_per_task_recheck_runs_and_deleted_base_is_recreated_privately`,
    `s3_creation_race_runs_the_existing_directory_checks`,
    `s4_non_acl_volume_and_dropped_security_descriptor_are_refused`, and
    `runner::tests::test_runner_blob_cache_retries_after_initial_base_failure`.

Disabling any refusal fails at least one of these tests. Disabling any call of the check fails one too,
with one exception: the second check in `handle_assign_task`'s task, before the input fetch, has no
test. A test for it would have to replace the root while the task waits for a slot.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-8 | Private task directories on Unix and Windows | `crates/core/src/private_file.rs`, `crates/worker/src/sandbox.rs`, `crates/worker/src/workspace.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/android_bridge/src/lib.rs`, `packaging/usb/install.ps1`, `packaging/windows/install_windows_service.ps1` | The Unix, Windows, isolation (`FEAT-3d`), and launcher tests above pass | Implemented |

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
    ACE.
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
- 2026-11-11 (review round 2, PR 213):
  - Refused existing directories with foreign write-class grants (`dir_write_grants`, including
    `INHERIT_ONLY` child grants), restricted foreign read-only warnings (`dir_read_grants`) to once
    per normalized path per process (`claim_warning_for_path`, silent on per-task re-checks), added
    Windows owner checks to `is_real_owned_dir` and `is_real_owned_file`, and locked `$DataDir` (and
    `C:\ProgramData\OxideSwarm`) in `packaging/windows/install_windows_service.ps1` with
    `Set-PrivateDirAcl`.
  - Added `check_windows_ancestor` and `create_missing_private_ancestors` so rename-capable or
    reparse-point ancestors are refused and missing intermediate ancestors are created with the
    protected private descriptor; closed the `Test-RootSafety` -> `SetAccessControl` race window in
    `packaging/usb/install.ps1` by re-listing existing folders after locking their DACL.
  - Bounds-checked unaligned `ACE_HEADER` and ACE slices against `AclSize` and `AclBytesInUse` in
    `parse_dacl_aces`, added `/remove:g "*<sid>"` to both directory and secret-file `icacls`
    remedies, preserved real `GetVolumeInformationByHandleW` errors, consolidated directory
    inspection onto a single `CreateFileW` handle with cached `current_user_sid()`, made
    `TaskRunner::ensure_blob_cache` retryable via `spawn_blocking`, excluded `GENERIC_EXECUTE` and
    `FILE_READ_EA` from sensitive masks, and updated `README.md` and `docs/SECURITY.md`.
