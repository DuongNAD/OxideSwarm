# USB setup kit (kit format v0)

These scripts put OxideSwarm on a USB stick, so a machine joins a cluster as a worker after one
double-click. This is the first version of [design 0053](../../docs/design/0053-usb-setup-kit.md). It
carries a **join token** ([design 0048](../../docs/design/0048-join-token.md)), which holds the cluster
secret. Once ENROLL-1 ([design 0051](../../docs/design/0051-machine-identity-and-invites.md)) is built,
`oxideswarm usb create` replaces the token with an invite that is encrypted with a passphrase and
works a limited number of times.

| File | What it does |
|---|---|
| `make_kit.sh` | Writes `<DIR>/OxideSwarm/`: these installers, the release archives for macOS (arm64 and x86_64), Windows and Linux with `SHA256SUMS`, and the join token read from standard input |
| `install.sh`, `Install OxideSwarm.command` | Install on Linux and macOS |
| `install.ps1`, `Install OxideSwarm.cmd` | Install on Windows |
| `Update OxideSwarm.command`, `Update OxideSwarm.cmd` | Update an install in place (`install.sh --update`, `install.ps1 -Update`) |
| `uninstall.sh`, `Uninstall OxideSwarm.command`, `uninstall.ps1`, `Uninstall OxideSwarm.cmd` | Remove what the installer added (on Windows, also revokes any `oxideswarm.toolchain` ACL grants recorded in `<folder>\home\windows_acl_grants.txt` via `revoke-isolation-acls.ps1` and removes `%TEMP%\oxideswarm-usb`) |
| `revoke-isolation-acls.ps1` | Copied from `windows/revoke-isolation-acls.ps1` so `uninstall.ps1` can revoke Windows isolation ACLs cleanly |
| `KIT_README.txt` | Becomes `README.txt` on the stick |

## Make a kit

On the master machine, with the stick mounted (for example at `/Volumes/<STICK>`):

```bash
oxideswarm secret join-token --master <MASTER_IP>:8088 | bash packaging/usb/make_kit.sh /Volumes/<STICK> --version 0.1.0
```

`make_kit.sh` downloads the archives of that release from GitHub and checks them against
`SHA256SUMS`. `--archives <DIR>` takes them from a folder instead, and `--replace` replaces an
existing kit.

For a trial on someone else's machine, add `--no-autostart`. The kit then holds a `no-autostart`
file, and its installers add no start at login: the worker stops at logout or shutdown, and nothing
stays behind once the uninstaller has run. `--isolation off|fs` bakes a default worker isolation
mode into the kit (`isolation` file) so double-clicking `Install OxideSwarm.cmd` on Windows or
`Install OxideSwarm.command` on macOS uses that mode without command-line flags ([design 0055](../../docs/design/0055-usb-kit-windows-isolation.md)).

`--gpu` also puts the `-gpu` archives on the kit (built with `gpu-wgpu`, design 0053 D2). Only that
build can offer a GPU: the plain build never does, even on a machine with a graphics card. On a kit
with a GPU build, the installer asks `Let the cluster use this computer's GPU as well? [Y/n]` and
installs the GPU build on yes. Without a terminal to ask, it installs the plain build. `--gpu` and
`--no-gpu` (`-Gpu` and `-NoGpu` on Windows) answer the question in advance. The GPU build skips
software adapters, so on a machine without a GPU it behaves like the plain build.

## Networks where machines cannot see each other

On school, office or café Wi-Fi, the machines often cannot reach each other directly (client
isolation). In that case, connect through iroh P2P instead of a LAN address:

1. Start the master with P2P. It needs outbound Internet access to the public relays:

   ```bash
   oxideswarm master --listen 0.0.0.0:8088 --p2p --p2p-ticket-file ~/.oxideswarm/master_ticket.txt
   ```

2. Make the kit from a token that carries the ticket. With no `--master`, the token holds only the
   ticket:

   ```bash
   oxideswarm secret join-token --p2p-ticket-file ~/.oxideswarm/master_ticket.txt | bash packaging/usb/make_kit.sh /Volumes/<STICK> --version <VERSION>
   ```

The installer saves the ticket, and the worker and the installer's checks connect through it, either
directly or through a relay. The master keeps the same ticket across restarts as long as
`master_key.bin` is kept. Check the network's rules before you run a cluster on it.

## Use it on a new machine

- **macOS:** double-click `Install OxideSwarm.command`.
- **Windows:** double-click `Install OxideSwarm.cmd`.
- **Linux:** run `bash install.sh` in a terminal opened in the kit folder.

Each installer does the following, without administrator rights:
1. It checks the archive against `SHA256SUMS`.
2. It installs into a per-user folder: `~/Library/Application Support/OxideSwarm`,
   `%LOCALAPPDATA%\OxideSwarm` or `~/.local/share/oxideswarm`.
3. It runs `oxideswarm join` with the token on standard input. The installed worker keeps its state
   in `<folder>/home` (through `OXIDESWARM_HOME`), so an existing `~/.oxideswarm` is never read or
   changed.
4. It starts the worker:
   - with the machine's name and `--no-portal`;
   - with `--max-concurrency` set to half of the CPU cores (at least 1), so the machine stays
     responsive for its owner;
   - with `--workspaces` when `cargo` is on `PATH`;
   - with `--isolation fs` on macOS and Linux (falling back to `off` only when `fs` was the default
     and the host kernel cannot start an isolated worker; an explicit `--isolation fs` or baked `fs`
     in the kit's `isolation` file fails instead of falling back), and `--isolation off` by default
     on Windows (or `-Isolation fs` / `make_kit.sh --isolation fs` when opted in; on Windows,
     `install.ps1` and `start-worker.vbs` set `OXIDESWARM_HOME=<folder>\home`,
     `RUSTY_GRID_ACL_GRANT_LOG=<folder>\home\windows_acl_grants.txt`, and
     `RUSTY_GRID_SANDBOX_BASE_DIR=%TEMP%\oxideswarm-usb\sandboxes` (`<folder>\work` for a folder
     outside `%LOCALAPPDATA%`), while `start-worker.vbs` also
     sets `HOME=<folder>\home` — so worker child processes read `HOME`-based git/ssh config from
     `<folder>\home` in both `off` and `fs` modes — and explicit `RUSTUP_HOME` / `CARGO_HOME` so
     `oxideswarm.toolchain` ACL grants are recorded in `<folder>\home\windows_acl_grants.txt` on
     release **0.1.1+** without touching `%USERPROFILE%\.oxideswarm`, and `uninstall.ps1` revokes
     them before deleting `<folder>` and `%TEMP%\oxideswarm-usb`.
     Note: release **0.1.1** predates `FEAT-3d` and `BUG-18`, so under `-Isolation fs` on `0.1.1`,
     `install.ps1` omits `--workspaces` and a bare `whoami` task resolves to `Git\usr\bin\whoami.exe`
     and exits `0xC0000142` unless invoked as `C:\Windows\System32\whoami.exe`; release **0.1.2+**
     supports `--workspaces` under `-Isolation fs` and resolves bare `whoami` to
     `%SystemRoot%\System32\whoami.exe`).
5. It waits until `oxideswarm workers --json` lists the worker as connected.
6. It adds a start at login: a LaunchAgent, an `HKCU` Run entry, or a `systemd --user` unit.
7. It runs `oxideswarm doctor`.

`install.sh --no-autostart` and `install.ps1 -NoAutostart` skip the start at login, as does a
`no-autostart` file in the kit. `--isolation off|fs` (`-Isolation off|fs` on Windows) overrides the
default or baked `isolation` file. `--name` and `--install-root` (`-Name` and `-InstallRoot` on
Windows) change the defaults. Both `install.ps1` and `uninstall.ps1` declare `[CmdletBinding()]` so
unknown parameters fail immediately.

### Update a machine

Double-click `Update OxideSwarm.command` (macOS) or `Update OxideSwarm.cmd` (Windows), or run
`bash install.sh --update` on Linux, from a stick that carries the newer kit. The update:

- finds the install (`--install-root`, else on Windows the folder recorded in
  `HKCU\Software\OxideSwarm\UsbKit`, else the default folder) and stops if there is none;
- keeps its name, build (GPU or plain) and workspace choice from `kit-install.conf`
  (`kit-install.txt` on Windows), which the installer writes; for an older install the name comes
  from its `home/worker_id.<name>` file and, without a terminal, the plain build is kept;
- keeps the start at login as it is, and does not join again: the state in `<folder>/home` (cluster
  secret, worker id) stays;
- stops the worker (and its LaunchAgent or systemd unit), replaces the binaries after the same
  checksum check, starts it again and waits until the master lists it. It does not run the doctor.

An install that uses the GPU build is not updated from a kit without one, unless `--no-gpu`
(`-NoGpu`) asks for the plain build.

### Another drive on Windows

When the machine has another fixed NTFS or ReFS drive, `install.ps1` asks where to keep its files
and lists `%LOCALAPPDATA%\OxideSwarm` (the default) and `<drive>:\OxideSwarm` for each such drive,
with the free space. Without a terminal to ask, it takes the default. `-InstallRoot <DIR>` names
the folder in advance. A folder outside `%LOCALAPPDATA%`:

- must be on a fixed NTFS or ReFS drive, not on the kit's drive, not the root of a drive, and new,
  empty or an earlier install of the kit. FAT and exFAT keep no permissions, and a removable drive
  can be pulled out while a task runs;
- is limited to the current user, `SYSTEM` and `Administrators` (`icacls /inheritance:r`) before
  anything is written into it, because a new folder at the root of a data drive lets every local
  account change its content;
- also holds the task folders and the file cache (`RUSTY_GRID_SANDBOX_BASE_DIR=<folder>\work`)
  and the workspaces (`--workspace-root <folder>\workspaces`), which otherwise go to `%TEMP%` and the
  user's cache folder on drive C.

The installer records the folder in `HKCU\Software\OxideSwarm\UsbKit`, and `uninstall.ps1` removes
that install when it gets no `-InstallRoot`. While an install exists in one folder, the installer
refuses another one unless `-InstallRoot` asks for it.

The checks in steps 5 and 7 run with `RUST_LOG=off`, because release 0.1.x writes informational
logs to standard output. On Windows PowerShell 5.1, `install.ps1` escapes the double quotes of a P2P
ticket before it passes the ticket to `oxideswarm`, because PowerShell 5.1 drops them.

## Security

- **The token is the cluster secret.** Anyone holding the stick can join the cluster and run work on
  its workers. Keep the stick with people you trust. If it is lost, change the cluster secret
  (`docs/SECURITY.md` section 5).
- **Checksums catch damage, not swaps.** They catch a damaged or outdated copy. They cannot catch
  someone who replaced the archives, `SHA256SUMS` and the scripts together.
- **Joining gives the cluster code execution.** A machine that joins a cluster runs that cluster's
  work.

## Tests

`python3 tests/test_launcher_scripts.py` (class `UsbKitTests`) runs `make_kit.sh`, `install.sh` and
`uninstall.sh` against fake binaries in a private home directory, on Linux and macOS. The Windows
scripts were run by hand from a stick on Windows 11 on 2026-11-09: `install.ps1` joined the worker, and `uninstall.ps1` stopped it.
On 2026-11-10, a P2P-only trial kit made with `--no-autostart` passed under Windows PowerShell 5.1
through `Install OxideSwarm.cmd` with no manual change, and from a stick on macOS.
