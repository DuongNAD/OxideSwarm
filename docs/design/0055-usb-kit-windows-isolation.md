# 0055: Opt-in Windows task isolation in the USB setup kit (`USB-2`, `BUG-18`)

Status: Implemented
Created: 2026-11-10    Updated: 2026-11-10

## Summary

Adds opt-in Windows task isolation (`-Isolation off|fs`, default `off`, or `--isolation off|fs`
baked into the kit by `make_kit.sh`) to the USB setup kit (`packaging/usb/`), makes
`acl_grant_manifest_path` honour `OXIDESWARM_HOME` so an isolated USB-kit worker never writes to
`%USERPROFILE%\.oxideswarm`, makes `uninstall.ps1` revoke all `oxideswarm.toolchain` ACL grants
before deleting the install folder, and resolves bare commands under Windows isolation against
`%SystemRoot%\System32` before `Git\usr\bin` (`BUG-18`).

## Context

1. **Manifest path ignored `OXIDESWARM_HOME`.** In `crates/worker/src/isolation.rs`,
   `windows::acl_grant_manifest_path` checked only `RUSTY_GRID_ACL_GRANT_LOG` and then fell back to
   `<host.user_home>/.oxideswarm/windows_acl_grants.txt`. Even when `packaging/usb/install.ps1` set
   `OXIDESWARM_HOME = <InstallRoot>\home`, starting a worker with `--isolation fs` on Windows wrote
   `windows_acl_grants.txt` into the machine owner's `%USERPROFILE%\.oxideswarm`, breaking the USB
   kit's promise (`packaging/usb/README.md`) that `%USERPROFILE%\.oxideswarm` is never touched.
2. **No isolation option or ACL cleanup in the Windows USB kit.** `packaging/usb/install.ps1`
   always started the worker without `--isolation` (defaulting to `off`), had no `-Isolation`
   parameter, and lacked `[CmdletBinding()]` so PowerShell accepted unknown flags as positional
   arguments. `packaging/usb/uninstall.ps1` deleted `$InstallRoot` without revoking the
   `oxideswarm.toolchain` (`S-1-15-3-...`) capability SID ACEs granted on `%RUSTUP_HOME%` and
   `%CARGO_HOME%`.
3. **Bare `whoami` under `--isolation fs` exited `0xC0000142` (`BUG-18`).** In
   `crates/core/src/capabilities.rs`, `augment_windows_path` prepends `C:\Program Files\Git\usr\bin`
   ahead of `existing_path`, and `resolve_windows_executable` checks `C:\Program Files\Git\usr\bin`
   before `effective_path`. For `oxideswarm submit -- whoami`, the worker resolved bare `whoami` to
   MSYS2's `C:\Program Files\Git\usr\bin\whoami.exe` (which links against `msys-2.0.dll`) instead of
   `C:\Windows\System32\whoami.exe`. Inside a Win32 AppContainer, `msys-2.0.dll` fails during
   `DllMain(DLL_PROCESS_ATTACH)` Cygwin shared-heap initialization and terminates the child process
   with `0xC0000142` (`STATUS_DLL_INIT_FAILURE`, exit code `-1073741502`), whereas
   `C:\Windows\System32\whoami.exe` links only Windows system DLLs and exits `0`.

## Goals

- **G1:** `windows::acl_grant_manifest_path` honours `OXIDESWARM_HOME` with precedence
  `RUSTY_GRID_ACL_GRANT_LOG > <OXIDESWARM_HOME>/windows_acl_grants.txt > ~/.oxideswarm/windows_acl_grants.txt`.
  When `OXIDESWARM_HOME` is set, no Windows isolation code creates or writes to
  `%USERPROFILE%\.oxideswarm`.
- **G2:** `packaging/usb/install.ps1` and `packaging/usb/uninstall.ps1` declare `[CmdletBinding()]`
  so unknown flags fail immediately. `install.ps1` accepts `-Isolation off|fs` (default `off` on
  Windows so host ACLs are untouched unless requested), `install.sh` accepts `--isolation off|fs`
  (keeping its default `fs` on macOS and Linux), and `make_kit.sh` accepts `--isolation off|fs` to
  bake a kit-wide default into `$KIT/isolation` and ships `windows/revoke-isolation-acls.ps1` in the
  kit.
- **G3:** `packaging/usb/uninstall.ps1` stops the worker, runs `revoke-isolation-acls.ps1` against
  `$InstallRoot\home\windows_acl_grants.txt` before deleting `$InstallRoot`, and is idempotent when
  no manifest exists.
- **G4 (`BUG-18`):** Under `--isolation fs` and `--isolation fs+net` on Windows, bare command names
  resolve against native directories (`%SystemRoot%\System32`, `Git\cmd`, `Git\bin`) before MSYS2
  `usr\bin` directories (`Git\usr\bin`, `msys64\usr\bin`), so `oxideswarm submit -- whoami` resolves
  to `C:\Windows\System32\whoami.exe` and exits `0`.

## Non-goals

- Changing the default isolation mode on Windows from `off` to `fs` when neither `-Isolation fs` nor
  `make_kit.sh --isolation fs` was given: because `--isolation fs` on Windows grants the
  `oxideswarm.toolchain` capability SID read+execute access on `%RUSTUP_HOME%` and `%CARGO_HOME%`,
  touching host toolchain ACLs on someone else's Windows PC stays opt-in.
- Making MSYS2 POSIX executables linked against `msys-2.0.dll` (`sh.exe`, `bash.exe` in
  `Git\usr\bin`) run inside a Windows AppContainer: `msys-2.0.dll`'s Cygwin shared heap is
  incompatible with Win32 AppContainer token restrictions (`0xC0000142`). Windows tasks under
  `--isolation fs|fs+net` use `cmd.exe`, `powershell.exe`, native Win32 tools in `System32`,
  `Git\cmd\git.exe`, `cargo.exe`, `rustc.exe`, and `python.exe`.

## Design

### D1: `OXIDESWARM_HOME` precedence for `acl_grant_manifest_path`

In `crates/worker/src/isolation.rs`:

1. `windows::acl_grant_manifest_path_with(host, grant_log_env, oxideswarm_home_env)` resolves the
   manifest path in three tiers:
   - `RUSTY_GRID_ACL_GRANT_LOG` when set and non-empty;
   - `<OXIDESWARM_HOME>/windows_acl_grants.txt` when `OXIDESWARM_HOME` is set and non-empty;
   - `<host.user_home>/.oxideswarm/windows_acl_grants.txt` (falling back to
     `std::env::temp_dir().join("windows_acl_grants.txt")` only when `host.user_home` is empty).
2. `windows::prepare_worker_toolchains` and `windows::probe_with_config` also check
   `<OXIDESWARM_HOME>/cluster.secret` and `<OXIDESWARM_HOME>/p2p_secret.key` (when those files
   already exist on disk) in addition to `<host.user_home>/.oxideswarm/...`, without creating
   `<host.user_home>/.oxideswarm`.
3. `windows::validate_sandbox_base_safe` and `windows::validate_workspace_root_safe` accept paths
   inside a dedicated `OXIDESWARM_HOME` directory under `user_home`.
4. `windows/revoke-isolation-acls.ps1` uses the same three-tier default for `-ManifestPath` and
   checks for running `worker` processes by command line (`CommandLine -like '* worker*'`) rather
   than any `oxideswarm` process, so a co-located `oxideswarm master` on the same host does not
   block sweeping leftover `oxideswarm.task.*` AppContainer profiles after the worker stops.

### D2: Opt-in isolation in `make_kit.sh`, `install.ps1`, and `install.sh`

1. `packaging/usb/make_kit.sh` gains `--isolation off|fs`:
   - validates that the value is `off` or `fs` (any other value exits `1`);
   - writes the chosen mode to `$KIT/isolation` when `--isolation` is passed;
   - copies `windows/revoke-isolation-acls.ps1` to `$KIT/revoke-isolation-acls.ps1`.
2. `packaging/usb/install.ps1`:
   - adds `[CmdletBinding()]` above `param(...)` so PowerShell rejects unknown parameters;
   - adds `[ValidateSet('off', 'fs')][string]$Isolation = ""`;
   - resolves `$Isolation`: CLI `-Isolation` > `$Kit\isolation` file > `'off'`;
   - copies `revoke-isolation-acls.ps1` into `$InstallRoot\revoke-isolation-acls.ps1`;
   - passes `--isolation $Isolation` in `$workerArgs` (used for both the immediate `wscript.exe`
     launch and the `HKCU:\Software\Microsoft\Windows\CurrentVersion\Run` `OxideSwarmUsbWorker`
     autostart launcher);
   - prints `  connected (isolation: $Isolation)`.
3. `packaging/usb/install.sh`:
   - adds `--isolation off|fs`;
   - resolves `ISOLATION`: CLI `--isolation` > `$KIT_DIR/isolation` file > `fs` (falling back to
     `off` if the worker cannot start with `fs`).

### D3: Reversible ACL cleanup in `uninstall.ps1`

`packaging/usb/uninstall.ps1`:

1. Declares `[CmdletBinding()]` above `param(...)`.
2. Stops the worker matching `$InstallRoot\bin\oxideswarm.exe` by PID and waits up to 5 seconds for
   the process to exit so file locks are released.
3. Checks `$Manifest = Join-Path $InstallRoot 'home\windows_acl_grants.txt'`:
   - if `$Manifest` exists, runs `revoke-isolation-acls.ps1 -ManifestPath $Manifest -RemoveManifest`
     (which removes `oxideswarm.toolchain` allow and deny ACEs from every path in the manifest,
     restores the original `sandbox_base` SDDL, and deletes leftover `oxideswarm.task.*`
     AppContainer profiles) and prints what was revoked;
   - if `$Manifest` does not exist, prints that no isolation ACL manifest was present.
4. Deletes `$InstallRoot`.

### D4: Native Win32 command resolution under Windows isolation (`BUG-18`)

In `crates/core/src/capabilities.rs` and `crates/worker/src/runner.rs`:

1. `is_msys_usr_bin_dir(dir)` identifies MSYS2 POSIX `usr\bin` directories (`...\Git\usr\bin` and
   `...\msys64\usr\bin`).
2. When `self.config.isolation != IsolationLevel::Off` on Windows, `TaskRunner` calls
   `augment_windows_path_for_isolation` and `resolve_windows_executable_for_isolation`:
   - `augment_windows_path_for_isolation` keeps native directories (`%SystemRoot%\System32`,
     `Git\cmd`, `Git\bin`) ahead of MSYS2 `usr\bin` directories even when the worker's own `PATH`
     came from Git Bash;
   - `resolve_windows_executable_for_isolation` searches all non-MSYS2 directories in
     `effective_path` (plus `%SystemRoot%\System32`, `Git\cmd`, and `Git\bin`) before falling back
     to MSYS2 `usr\bin` directories.
3. When `self.config.isolation == IsolationLevel::Off`, `augment_windows_path` and
   `resolve_windows_executable` are unchanged.

## Compatibility

No wire protocol change. CLI flags on `oxideswarm` are unchanged; `make_kit.sh`, `install.sh`, and
`install.ps1` gain the optional `--isolation` / `-Isolation` flag. Existing kits without an
`isolation` file keep the previous defaults (`off` on Windows, `fs` with fallback to `off` on macOS
and Linux).

## Security

- On Windows, `--isolation off` remains the default in `install.ps1` unless `-Isolation fs` or
  `make_kit.sh --isolation fs` is requested, so running the kit on someone else's Windows PC makes
  zero changes to `%RUSTUP_HOME%` or `%CARGO_HOME%` ACLs unless opted in.
- When `-Isolation fs` is used on Windows, all ACL grants use the `oxideswarm.toolchain` capability
  SID (`S-1-15-3-...`, never `ALL APPLICATION PACKAGES` or `Everyone`), are recorded in
  `$InstallRoot\home\windows_acl_grants.txt`, and are revoked automatically by `uninstall.ps1` before
  `$InstallRoot` is removed.
- `[CmdletBinding()]` on `install.ps1` and `uninstall.ps1` ensures a mistyped flag (such as
  `-NoAutostartt`) fails immediately instead of being silently ignored.

## Test plan

- **Unit tests:**
  - `isolation::tests::test_acl_grant_manifest_path_precedence_honours_oxideswarm_home` in
    `crates/worker/src/isolation.rs`;
  - `capabilities::tests::test_windows_executable_resolution_for_isolation_prefers_system32_over_msys_usr_bin`
    in `crates/core/src/capabilities.rs`.
- **Integration tests:**
  - `windows::test_bare_whoami_succeeds_under_isolation_fs` in
    `crates/worker/tests/test_feat3_os_isolation.rs`;
  - `UsbKitTests::test_make_kit_and_install_support_isolation_option_and_reject_invalid_values` and
    `UsbKitTests::test_windows_usb_scripts_enforce_cmdletbinding_isolation_and_manifest_revocation`
    in `tests/test_launcher_scripts.py`.
- **Live Windows verification:**
  - Build a kit with `make_kit.sh`, install with `install.ps1 -Isolation fs` under a temporary
    `USERPROFILE`, run the trial-guide commands (`cmd /c ver`, `powershell ...`, six `echo` tasks +
    `status`, `whoami`, `cmd /c dir %USERPROFILE%\Documents`), run `uninstall.ps1 -Yes`, and verify
    that `icacls` shows zero `oxideswarm.toolchain` ACEs remaining and `%USERPROFILE%\.oxideswarm`
    does not exist.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `USB-2` | Opt-in Windows isolation in the USB kit (`-Isolation off\|fs`, `OXIDESWARM_HOME` manifest path, `uninstall.ps1` ACL revocation, `[CmdletBinding()]`) | `crates/worker/src/isolation.rs`, `windows/revoke-isolation-acls.ps1`, `packaging/usb/{make_kit.sh,install.ps1,uninstall.ps1,install.sh,README.md}`, `tests/test_launcher_scripts.py`, `docs/design/0055-usb-kit-windows-isolation.md` | Unit, launcher, and live Windows install/trial/uninstall checks pass with zero leftover `oxideswarm.toolchain` ACEs and untouched `%USERPROFILE%\.oxideswarm` | Implemented |
| `BUG-18` | Resolve bare commands under Windows `--isolation fs\|fs+net` against `%SystemRoot%\System32` before `Git\usr\bin` so `whoami` does not fail with `0xC0000142` (`STATUS_DLL_INIT_FAILURE`) | `crates/core/src/capabilities.rs`, `crates/worker/src/runner.rs`, `crates/worker/tests/test_feat3_os_isolation.rs`, `docs/design/0033-os-level-task-isolation.md`, `docs/SECURITY.md` | `test_bare_whoami_succeeds_under_isolation_fs` and live `oxideswarm submit -- whoami` under `--isolation fs` exit `0` | Implemented |

## Status log

- 2026-11-10: `USB-2` and `BUG-18` implemented and verified on Windows 11.
