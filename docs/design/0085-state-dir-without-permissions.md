# 0085: Refuse a master state directory on a file system without permissions

Status: Implemented
Created: 2026-11-17    Updated: 2026-11-17

## Summary

A master started with `--state-dir` on a volume that keeps no file permissions (exFAT, FAT32)
now refuses at start-up, before it writes anything there. The message names the file system and asks
for an NTFS directory. Before this change, the master started once and then refused every later
start because of `members.json`, with an `icacls` hint that cannot work on such a volume. The same
message replaces the `icacls` hint when an existing private file is found on such a volume
(`BUG-24`). Windows only; macOS and Linux are not covered.

## Context

- On Windows a volume reports whether it keeps ACLs with the `FILE_PERSISTENT_ACLS` flag of
  `GetVolumeInformationW` / `GetVolumeInformationByHandleW`. NTFS and ReFS set it; exFAT and FAT32 do
  not. Measured on the development machine: `C:` NTFS `0x03E72EFF`, `D:` exFAT `0x00020206`.
- On such a volume `GetNamedSecurityInfoW` returns a NULL DACL, which
  `crates/core/src/private_file.rs` reports as full control for `Everyone`, and `icacls` prints
  `No permissions are set. All users have full control.`
- The master writes `master_noise.key` and `members.json` into the state directory on its first start
  (`crates/cli/src/cli_app/master.rs`, after the state directory is resolved). On the next start
  `MemberStore::load_file` runs `check_members_file_permissions` (`crates/master/src/members.rs`),
  which finds `Everyone` and exits with
  `members file '<state>\members.json' is accessible by other accounts (Everyone); to make it private run: icacls ...`.
  The 0.3.0 and 0.4.0 masters behave the same (Windows smoke test of 0.4.0, finding G1).
- The worker already refuses a sandbox base on such a volume (`crates/worker/src/sandbox.rs`,
  `its volume does not support persistent ACLs (such as FAT or exFAT)`), using the
  `persistent_acls` field of `inspect_dir_security_no_follow`.
- `warn_if_readable_by_others` (`private_file.rs`) logs the same `icacls` hint for other private files,
  for example `node.key` in a home directory on exFAT (seen with the 0.3.0 master).

## Goals

- **G1** A master refuses a state directory on a volume without permissions at start-up, before it
  creates, reads or writes anything there.
- **G2** The message names the file system and asks for an NTFS directory; it never suggests `icacls`.
- **G3** An existing `members.json` (or its directory) on such a volume gets the same message instead
  of the `icacls` hint, and so do the warnings for other private files.
- **G4** NTFS behaves exactly as before.

## Non-goals

- macOS and Linux. There the mode bits of an exFAT or FAT mount come from the mount options and
  `chmod` has no effect; a check would need the file-system type (`statfs`) or a probe write, and no CI
  job can mount such a volume cheaply. Not covered.
- `--blob-dir` and the worker's directories (the worker already checks its sandbox base).
- Moving an existing state directory: the operator moves it.

## Design

### D1. The decision (every platform)

`private_file.rs` gains `FILE_PERSISTENT_ACLS`, `volume_keeps_permissions(fs_flags)` and
`no_permissions_volume_message(path, what, fs_name)`. The message is:

```text
<what> '<path>' is on a file system that keeps no file permissions (<fs>), so every account on this machine could read or replace it; use a directory on an NTFS drive instead
```

### D2. Reading the volume (Windows)

`volume_info(path)` returns the file-system name and flags of the volume that holds `path`, or of its
nearest existing ancestor when `path` does not exist yet (a state directory before the first start).
It opens that directory with `FILE_FLAG_BACKUP_SEMANTICS` and desired access `0`, following links, so
the answer is the volume the data would land on, and calls `GetVolumeInformationByHandleW`. It
creates nothing. `ensure_volume_keeps_permissions(path, what)` turns a missing flag into the D1
message. When the flags cannot be read (an unusual network file system) it returns `Ok` and logs at
debug level, so the per-file checks still decide. On other systems it is always `Ok`.

### D3. Where the master checks

- The CLI (`run_master`, `crates/cli/src/cli_app/master.rs`) checks right after it resolves the state
  directory (flag, `RUSTY_GRID_STATE_DIR` or `[master].state_dir`). Nothing has been written there
  yet: `master_noise.key`, `members.json`, `store.lock` and `journal.bin` come later. It prints
  `Refusing to start the master: <message>` and exits with code 1.
- `MasterServer::bind_full` (`crates/master/src/server.rs`) checks the same way before it loads
  members or opens the task store, so library callers get `GridError::Config` with the message.

### D4. Existing private files

`open_acl_or_volume_message(path, what, offenders, user_sid)` returns the D1 message when the file's
volume keeps no permissions, and the existing `icacls` warning otherwise. `check_members_file_permissions`
(for `members.json` and its directory) and `warn_if_readable_by_others` use it. The latter warns for the
cluster secret file (`cluster_auth.rs`), the identity key files `node.key` and `master_noise.key`
(`noise_auth.rs`), the P2P master key file (`server.rs`) and the task store directory (`store.rs`).

Private files checked:

- In the state directory: `master_noise.key`, `members.json`, `store.lock`, `journal.bin`,
  `snapshot.bin`, `blobs/`. None is reached on such a volume after D3.
- `node.key` and `cluster.secret` in a home directory on such a volume: still only a warning, now with
  the D1 text.

## Compatibility

- No wire or store format change. An NTFS state directory starts as before.
- A state directory that already lives on exFAT could not be restarted before this change either;
  after it, every start refuses with the new message. To keep its tasks, copy the directory to NTFS
  and start the master there; this copy was not tested.

## Security

- No new input. The check makes a misconfiguration fail closed at the first start instead of
  leaving the master's keys and member list readable by every account until the second start.

## Test plan

- Unit, every system (`private_file::tests`): `volume_keeps_permissions` with the measured NTFS and
  exFAT flags, `0`, and every flag but `FILE_PERSISTENT_ACLS`; the message names the file system, asks
  for NTFS and never contains `icacls`.
- Unit, Windows (`private_file::windows_tests`): a temp directory's volume keeps permissions, a missing
  child resolves to its parent's volume and is not created, and `ensure_volume_keeps_permissions` is
  `Ok`. With `OXIDESWARM_TEST_NO_ACL_DIR` set to a folder on exFAT, that folder is refused with the
  message and nothing is created (skipped otherwise; CI has no such volume).
- Recorded run on Windows: `master --state-dir <exFAT dir>` refused at the first start with the
  message, nothing written; the same for an existing state directory with a `members.json` from a
  previous start; on NTFS, start, stop and start as before.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Decision, message and volume query | `crates/core/src/private_file.rs` | Unit tests pass on every system; Windows tests pass | Implemented |
| W2 | Start-up check in the CLI and in `bind_full` | `crates/cli/src/cli_app/master.rs`, `crates/master/src/server.rs` | A state directory on exFAT is refused before anything is written | Implemented |
| W3 | Message for existing private files | `crates/master/src/members.rs`, `crates/core/src/private_file.rs` | No `icacls` hint on a volume without permissions | Implemented |
| W4 | Docs | `docs/BACKLOG.md`, `CHANGELOG.md`, `DEPLOYMENT.md` | BUG-24 Implemented; section 3.5 says where a state directory may live | Planned |

## Status log

- 2026-11-17: Drafted and implemented with `BUG-24` (W1 to W3). W4 waits until the 0.4.0 release pull
  request, which adds the BUG-24 entry, is merged.
