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

For a trial on someone else's machine, add `--no-autostart`. The kit then holds a `no-autostart`
file, and its installers add no start at login: the worker stops at logout or shutdown, and nothing
stays behind once the uninstaller has run.

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
   - with `--isolation fs` on macOS and Linux, falling back to `off` if the worker cannot start with
     it.
5. It waits until `oxideswarm workers --json` lists the worker as connected.
6. It adds a start at login: a LaunchAgent, an `HKCU` Run entry, or a `systemd --user` unit.
7. It runs `oxideswarm doctor`.

`install.sh --no-autostart` and `install.ps1 -NoAutostart` skip the start at login, as does a
`no-autostart` file in the kit. `--name` and `--install-root` (`-Name` and `-InstallRoot` on
Windows) change the defaults.

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
