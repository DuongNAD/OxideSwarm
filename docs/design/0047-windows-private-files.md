# 0047: Private files on Windows

Status: Implemented
Created: 2026-11-08    Updated: 2026-11-08

## Summary

On Windows, the files OxideSwarm creates as private (the cluster secret and the master P2P key, plus the P2P ticket file and the directories that hold them) are created with a protected DACL that grants full control only to the current user, `SYSTEM` and `BUILTIN\Administrators`, applied by `CreateFileW` itself so no moment exists at which another account can read them. Loading the cluster secret or the master key logs a warning, with the exact `icacls` fix, when the file grants access to anyone else. Unix behaviour is unchanged (`SEC-9`).

## Context

On Unix `write_private_new` (`crates/core/src/cluster_auth.rs`) and `write_new_private_key` (`crates/master/src/server.rs`) create files with mode 0600, `ClusterSecret::load_file` warns when group or others can read the file, and `tighten_key_permissions` fixes an exposed key. On Windows none of this existed: a new file inherits the DACL of its directory. On a typical machine the home folder `C:\Users\<name>` grants `Everyone:(RX)` and `BUILTIN\Users:(RX)` with inheritance, so `~\.oxideswarm\cluster.secret` and `master_key.bin` were readable by every local account and service, and nothing warned. `docs/SECURITY.md` listed mode 600 for them without saying that it did not hold on Windows.

## Goals

- **G1** Every file the master or the CLI creates as private on Windows has a protected DACL (no inherited entries) with exactly three allowed entries: the current user's SID, `SYSTEM` and `BUILTIN\Administrators`, all with full control. This equals `icacls <file> /inheritance:r /grant:r <user>:F SYSTEM:F Administrators:F`.
- **G2** The DACL is applied at creation (`CreateFileW` with a `SECURITY_ATTRIBUTES`), never afterwards. If the descriptor cannot be built or applied, the creation fails with the OS error and no file is left.
- **G3** A directory that OxideSwarm creates to hold such files (`~\.oxideswarm`) gets the same protected DACL, with object and container inheritance so the files created in it inherit only those three entries.
- **G4** Loading an existing secret or key file whose DACL allows any other account logs a warning that names the accounts and the `icacls` command that fixes it. It never refuses to start (Unix only warns for the secret as well).
- **G5** Unix code paths are unchanged.

## Non-goals

- Rewriting the DACL of an existing file or directory (Unix tightens an exposed key; on Windows the warning gives the command and the owner decides, because rewriting an ACL can lock out a service account the owner set up on purpose).
- The Windows service installer files under `C:\ProgramData\OxideSwarm` (the installer sets their ACL) and the worker id file (an identity, not a credential).
- The task store directory of `FEAT-1` (design 0046, D1.3): it can call `create_private_dir_all` and `warn_if_readable_by_others` from this module.

## Design

### D1 Module `rusty_grid_core::private_file`

Pure functions, built and tested on every platform:

- `private_sddl(user_sid, directory)` returns `D:P(A;;FA;;;<sid>)(A;;FA;;;SY)(A;;FA;;;BA)`. `P` makes the DACL protected. For a directory every entry carries `OICI`.
- `open_grants(aces, current_user_sid)` returns the SIDs, one each, that an allowed entry gives read, write, append, `WRITE_DAC`, `WRITE_OWNER` or a generic right and that are not the current user, `SYSTEM` or `Administrators`. Entries that carry only attribute, `READ_CONTROL` or `SYNCHRONIZE` bits are ignored.
- `open_acl_warning(path, what, offenders, user_name)` builds the warning text: `<what> '<path>' is accessible by other accounts (Everyone, BUILTIN\Users); to make it private run: icacls "<path>" /inheritance:r /grant:r "<user>:F" SYSTEM:F Administrators:F`.

Windows-only functions (`windows-sys`, features `Win32_Security_Authorization` and `Win32_Storage_FileSystem` added to `crates/core`):

- `create_private_new(path)`: `CreateFileW(CREATE_NEW)` with the descriptor from `ConvertStringSecurityDescriptorToSecurityDescriptorW`; an existing file gives `AlreadyExists`, as `create_new` does.
- `create_private_dir_all(dir)`: creates missing ancestors normally and `dir` itself with `CreateDirectoryW` and the directory descriptor; an existing directory is left alone, as on Unix where only a directory the call created is tightened.
- `write_private_replace(path, data)`: writes a private temporary file next to `path` and renames it over `path`, for files that are rewritten (the ticket file).
- `read_dacl(path)` (`GetNamedSecurityInfoW`, `GetAclInformation`, `GetAce`) and `current_user_sid()` (process token); a NULL DACL reads as full access for `Everyone`.
- `warn_if_readable_by_others(path, what)`: `tracing::warn!` of `open_acl_warning`; a failure to read the DACL is logged at `debug` only.

Every system allocation (the security descriptor, the string SID) is released through a `LocalFree` guard.

### D2 Call sites

| File | Where | Windows behaviour |
|---|---|---|
| `cluster.secret` (`oxideswarm secret generate`, the master's generated secret) | `write_private_new`, `ClusterSecret::save_file` (new parent directory), `ClusterSecret::load_file` | protected DACL at creation; private parent directory; warning at load |
| `master_key.bin` | `write_new_private_key`, `resolve_p2p_secret_key` in `crates/master/src/server.rs` | protected DACL at creation; private parent directory; warning at load |
| P2P ticket file (`--p2p-ticket-file`) | the master's ticket publication in `server.rs` | written with `write_private_replace` |

## Compatibility

No wire or CLI change. An existing file keeps its DACL; only new files are private, and the warning tells the owner how to fix an old one. A cluster secret that another process reads through a service account other than `SYSTEM`, `Administrators` or the owner needs an explicit `icacls /grant`.

## Testing

- Pure tests (all platforms): `private_sddl`, `open_grants` (private DACL, `Everyone`, `Users`, `Authenticated Users`, case-insensitive user SID, ignored attribute-only entries) and `open_acl_warning`.
- Windows tests (`private_file::windows_tests`, run on `windows-latest`): in a temp directory whose DACL grants `Everyone` and `BUILTIN\Users` read with inheritance, a file created with `std::fs::write` is flagged, while a file from `create_private_new` has a protected DACL with exactly the three allowed SIDs and no `Everyone` or `Users`, refuses an existing file, a directory from `create_private_dir_all` is protected and the files in it inherit only the three entries, and `write_private_replace` replaces and stays private. The tests use temp directories only and never touch the real `~\.oxideswarm`.
