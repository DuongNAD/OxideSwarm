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
| `uninstall.sh`, `Uninstall OxideSwarm.command`, `uninstall.ps1`, `Uninstall OxideSwarm.cmd` | Remove what the installer added |
| `KIT_README.txt` | Becomes `README.txt` on the stick |

## Make a kit

On the master machine, with the stick mounted (for example at `/Volumes/<STICK>`):

```bash
oxideswarm secret join-token --master <MASTER_IP>:8088 | bash packaging/usb/make_kit.sh /Volumes/<STICK> --version 0.1.0
```

`make_kit.sh` downloads the archives of that release from GitHub and checks them against
`SHA256SUMS`. `--archives <DIR>` takes them from a folder instead, and `--replace` replaces an
existing kit.

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
   - with `--workspaces` when `cargo` is on `PATH`;
   - with `--isolation fs` on macOS and Linux, falling back to `off` if the worker cannot start with
     it.
5. It waits until `oxideswarm workers --json` lists the worker as connected.
6. It adds a start at login: a LaunchAgent, an `HKCU` Run entry, or a `systemd --user` unit.
7. It runs `oxideswarm doctor`.

`install.sh --no-autostart` and `install.ps1 -NoAutostart` skip the start at login. `--name` and
`--install-root` (`-Name` and `-InstallRoot` on Windows) change the defaults.

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
