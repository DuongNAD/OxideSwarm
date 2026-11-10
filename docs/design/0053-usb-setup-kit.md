# 0053: Set up machines from a USB stick

Status: Accepted (decision Q13, 2026-11-09). Kit format v0 is built in `packaging/usb/`; `usb create` needs ENROLL-1b
Created: 2026-11-09    Updated: 2026-11-10

## Summary

`oxideswarm usb create <DIR> --machines N` writes a setup kit to a USB stick. The kit holds the
release archives for macOS, Windows and Linux with their checksums, an invite for up to N machines
encrypted with a passphrase, and one installer per system. On a new machine the owner opens the
stick, double-clicks the installer for that system and types the passphrase. The machine then
installs the worker for the current user, joins the cluster, starts at login and runs the doctor
checks. The kit never contains the cluster secret. Item `USB-1`.

## Context

- A new machine needs three things today: the binary (`README.md` Install), the credential (a
  secret file or a join token, [design 0048](0048-join-token.md)) and a background start (the
  installers under `scripts/`, `windows/` and `packaging/`). These come from three places, through
  several commands.
- The owner often sets up machines that have no tools yet, or no convenient way to receive a
  string: a friend's laptop, a lab PC, a machine without a browser session. A USB stick is the
  channel `README.md` already recommends for the secret.
- Invites of [design 0051](0051-machine-identity-and-invites.md) can be limited in uses and time
  and revoked, so a lost stick need not cost the cluster's secret.

## Goals

- **G1** On a new machine, with the stick plugged in: one double-click, one passphrase, and the
  machine is a worker of the cluster within about a minute. No terminal knowledge is needed. No
  download is needed except the connection to the master.
- **G2** A lost stick reveals nothing without the passphrase. With the passphrase, it gives at most
  N enrolments before the expiry, each one approved on the master unless the owner chose
  otherwise, and the owner can revoke the kit's invite at once.
- **G3** No administrator rights by default. The installers install for the current user, with a
  user-level background start.
- **G4** Every binary installed from the stick is checked against the stick's `SHA256SUMS` before it
  is unpacked.

## Non-goals

- Updating machines that are already installed (a later `usb update`).
- An autorun on insertion. Current macOS, Windows and Linux disable it, and it would be unsafe.
- Signing or notarizing the binaries (a separate release item).
- A bootable image.

## Design

### D1 Creating the kit

```
oxideswarm usb create <DIR> --machines N [--expires 7d] [--roles worker] [--workspaces] [--gpu]
                      [--archives <DIR> | --download] [--no-approval] [--passphrase-stdin] [--replace]
```

1. An admin runs it (0051 D6). It creates an invite (0051 D3) with `uses = N`, the expiry (7 days by
   default, at most 30) and the roles (`worker` by default; `admin` is refused).
2. The archives come from `--archives <DIR>`, a folder holding release archives and their
   `SHA256SUMS`, or, with `--download`, from the GitHub release whose version matches the running
   binary, over HTTPS. Each archive is checked against `SHA256SUMS`, and a mismatch stops the
   command before anything is written.
3. The passphrase is four words from the word list of [design 0052](0052-short-code-pairing.md) D1
   (44 bits; whichever of JOIN-2a and USB-1a lands first adds the list). It is generated, printed
   once on the terminal and never written to the stick.
   `--passphrase-stdin` takes the owner's own passphrase instead; it must be at least 16
   characters.
4. The invite is encrypted with ChaCha20-Poly1305 under a key from Argon2id (64 MiB, 3 passes, one
   lane, a random 16-byte salt) and written as `invite.oxs`. The header holds the format version, the
   Argon2id parameters, the salt and the nonce, and it is the AEAD's associated data, so a changed
   header fails decryption. A reader refuses parameters above 256 MiB, 10 passes or 4 lanes before
   it runs Argon2id.
5. The command writes the layout of D2 to `<DIR>/OxideSwarm/`. It refuses a non-empty
   `OxideSwarm/` folder unless `--replace` is given, and it never follows symbolic links.

### D2 Layout on the stick

```text
OxideSwarm/
  README.txt                      what this is, the three steps, how to uninstall
  Install OxideSwarm.command      macOS
  Install OxideSwarm.cmd          Windows
  install.sh                      Linux
  uninstall.command  uninstall.cmd  uninstall.sh
  invite.oxs                      the encrypted invite
  kit.json                        version, created, expires, machines, role, workspaces (no secrets)
  archives/SHA256SUMS
  archives/oxideswarm-<version>-<target>.tar.gz | .zip
```

The archives cover `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-pc-windows-msvc` and
`x86_64-unknown-linux-gnu`, without `gpu-wgpu` by default. `--gpu` also copies the `-gpu`
archives, and the installer then asks whether this machine's GPU should do work.

### D3 What an installer does

The installers follow the launcher rules of `CLAUDE.md`: bash 3.2 for `.command` and `.sh`, CRLF
for `.cmd`, no hard-coded host names or addresses, and processes tracked by PID files, never by
name.

1. It detects the system and the architecture, picks the archive, and checks it against
   `archives/SHA256SUMS`.
2. It unpacks the archive into a per-user folder: `~/Library/Application Support/OxideSwarm/bin`,
   `%LOCALAPPDATA%\OxideSwarm\bin` or `~/.local/share/oxideswarm/bin`.
3. It runs `oxideswarm join --invite-file <stick>/OxideSwarm/invite.oxs`. `join` asks for the
   passphrase with echo off, decrypts the invite in memory and enrols as in 0051 D4. It prints the
   master's fingerprint and the pairing check, waits for the approval, and asks the user to confirm
   that the owner sees the same check. The installers therefore run in a terminal window: the
   `.command` and `.cmd` files open one, and on Linux the user runs `bash install.sh` in a terminal,
   because sticks with FAT file systems are often mounted without execute permission and a
   double-click gives no terminal to type in.
4. It installs a user-level start that needs no administrator rights:
   - a LaunchAgent on macOS;
   - an `HKCU\Software\Microsoft\Windows\CurrentVersion\Run` entry on Windows, which starts the
     worker silently through the existing `windows/run_worker_silent.vbs` pattern (a scheduled task
     at logon often needs elevation, so it is not used);
   - a `systemd --user` unit on Linux, with an XDG autostart file as the fallback.

   The worker runs with:
   - the name the owner typed (the host name is the default);
   - `--workspaces` when the kit says so and `tool:cargo` is found;
   - `--isolation fs` on macOS and Linux. If the worker refuses to start because isolation is not
     available (for example Linux without Landlock), the installer restarts it with
     `--isolation off` and says so.
5. It waits until `oxideswarm workers` (read-only, allowed for every member, 0051 D6) lists the new
   worker as connected, runs the doctor's task probes only if the kit grants `client`, prints a
   summary, and writes an install log to the per-user folder. The log is never written to the
   stick.
6. A failed step is undone in reverse order and printed in plain words. The installer leaves no
   half-installed service behind.

`uninstall.*` stops the worker by its PID file, removes the user-level start and the binaries, and
asks whether to delete `node.key`. An admin can then run `members revoke` on the master.

### D4 Revoking a kit

`oxideswarm invite --list` shows the kit's invite with its uses left. `invite --revoke <ID>` ends
it at once. Machines that already joined stay members until they are revoked one by one with
`members revoke`.

## Compatibility

No wire change beyond 0051. `usb create` and the `--invite-file` option of `join` are new. The
installers call only `oxideswarm` subcommands that exist in the version on the stick.

## Security

- **Approval at the machine.** An admin member can approve from any machine (0051 D4), for example
  from the owner's own laptop, so the owner does not need the master's host at hand and
  has less reason to use `--no-approval`.
- **The stick holds no long-term credential.** It holds an invite, encrypted, limited to N uses and
  an expiry, and approved per machine by default. `--no-approval` makes a found stick with a
  known passphrase enough to join; `usb create` warns and allows it only with an expiry of at most
  48 hours.
- **Passphrase strength.** 44 bits under Argon2id (64 MiB) costs an attacker about 2^44 slow
  guesses, while the invite expires within days. A user passphrase shorter than 16 characters is
  refused.
- **Tampering.** Someone holding the stick could replace the binaries, `SHA256SUMS` and the
  installers together. Nothing on the stick can prove otherwise, because the code that would check
  is itself on the stick: a swapped stick also skips any online check. Both checks of the installer,
  the stick's `SHA256SUMS` and, when online, the release's `SHA256SUMS` fetched from GitHub over
  HTTPS, only catch a corrupted or outdated copy. The stick must stay with people the owner trusts,
  and `README.txt` says so. Signed binaries (a separate release item) would change this.
- **Execution.** Gatekeeper and SmartScreen check the quarantine mark, which files written by
  `usb create` do not carry. The installers never disable either protection and never ask for an
  administrator password.

## Test plan

- **Unit:**
  - encrypt and decrypt round trip;
  - a wrong passphrase, a truncated file, a changed byte and a changed header are refused;
  - Argon2id parameters above the bounds are refused before any work;
  - the archive check against `SHA256SUMS`, with the online comparison stubbed.
- **Launcher tests** (`tests/test_launcher_scripts.py`, with fake binaries and a private HOME):
  - each installer and uninstaller on Linux and macOS (bash 3.2), with failure rollback and the
    isolation fallback;
  - the `.cmd` files in the Windows job, run as a standard user.
- **Integration** (`tests/cli_usb_kit_e2e.rs`): `usb create` into a temporary folder from fake
  archives, then `join --invite-file` against a loopback master. Approval enrols a worker. The
  (N+1)th use is refused. A revoked kit invite is refused.
- **Live:** a stick made on the Mac sets up a Windows machine, and the run is recorded with its date
  in the add-a-machine guide.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| USB-1a | `usb create`, `invite.oxs` format, `join --invite-file`, archive checks (offline and online), `kit.json` | `crates/cli/src/usb_kit.rs` (new), `crates/cli/src/join.rs`, `crates/cli/src/cli_app.rs` | `cli_usb_kit_e2e` passes on Linux, macOS and Windows | Planned |
| USB-1b | The installers and uninstallers for the three systems, user-level start, rollback | `packaging/usb/` (new), `tests/test_launcher_scripts.py` | Launcher tests pass on Linux and macOS, and the `.cmd` path passes on Windows CI | Planned |
| USB-1c | Guide section and live record | `docs/guides/add-a-machine.md`, `README.md` | A stick made on one system sets up a machine of another, recorded | Planned |

## Status log

- 2026-11-09: Accepted with Q13. It starts after ENROLL-1b. USB-1b can start earlier against a fake
  `join`.
- 2026-11-09: Revised after an independent review, before any code. Changes:
  - an `HKCU` Run entry instead of a scheduled task at logon;
  - the isolation fallback on Linux, and `bash install.sh` in a terminal;
  - the worker check is read-only;
  - bounded Argon2id parameters, with the header as associated data;
  - the tampering note states plainly what the checks catch;
  - approval from an admin member, the shared word list and the full command synopsis.
- 2026-11-09: Kit format v0 built in `packaging/usb/` (the first part of USB-1b). It carries a join
  token instead of an encrypted invite, until ENROLL-1b. It contains:
  - `make_kit.sh`;
  - installers for macOS, Windows and Linux that install for the current user, keep their state in
    `<folder>/home` through `OXIDESWARM_HOME`, start the worker and add a start at login;
  - uninstallers.

  It was tested from a 16 GB exFAT stick on macOS (Apple M5) and on Windows 11 (Intel Core
  i7-1355U) against a demo cluster with a throw-away secret. The worker joined, and the probe
  tasks of the doctor checks completed without errors, and the uninstaller stopped it. The installer waits for `state:connected` in `workers --json`, because the plain
  listing also showed disconnected workers (fixed in `oxideswarm workers`).
- 2026-11-10: Trial kits for someone else's machine. `make_kit.sh --no-autostart` writes a
  `no-autostart` file, and the installers then add no start at login. The worker gets
  `--max-concurrency` of half the CPU cores. The installers' checks run with `RUST_LOG=off`, because
  release 0.1.x writes informational logs to standard output, which broke the JSON parsing of
  `workers --json` over P2P. On Windows PowerShell 5.1, `install.ps1` escapes the double quotes of
  the P2P ticket, which the legacy argument passing dropped. A P2P-only kit passed end to end under
  PowerShell 5.1 on Windows 11 and from a stick on macOS.
- 2026-11-10: Opt-in Windows isolation in the kit ([design 0055](0055-usb-kit-windows-isolation.md),
  `USB-2`): `make_kit.sh --isolation off|fs` bakes a kit default into `isolation` and ships
  `revoke-isolation-acls.ps1`, `install.ps1` accepts `-Isolation off|fs` (default `off`) with
  `[CmdletBinding()]`, `acl_grant_manifest_path` honours `OXIDESWARM_HOME` so an isolated USB-kit
  worker writes `<InstallRoot>\home\windows_acl_grants.txt` and never touches
  `%USERPROFILE%\.oxideswarm`, and `uninstall.ps1` revokes all recorded `oxideswarm.toolchain` ACL
  grants before deleting `<InstallRoot>`.
- 2026-11-10: D2 `--gpu` in kit format v0. During the trial, a laptop with a discrete GPU joined
  with `GPU: none`, because the kit only held the plain archives. `make_kit.sh --gpu` adds the `-gpu`
  archive of each target. The installers ask whether this machine's GPU should do work and install
  the GPU build on yes; without a terminal they keep the plain build. `install.sh --gpu|--no-gpu` and
  `install.ps1 -Gpu|-NoGpu` answer in advance.
- 2026-11-10: Install folder on another drive (Windows). The trial showed task folders and the file
  cache in `%TEMP%` on drive C even with `-InstallRoot`. `install.ps1` now offers each fixed NTFS or
  ReFS drive other than the kit's, and checks a folder given with `-InstallRoot` the same way. A
  folder outside `%LOCALAPPDATA%` is limited to the user, `SYSTEM` and `Administrators` before
  anything is written into it, and the worker gets its sandbox base and `--workspace-root`
  inside it. The folder is recorded in `HKCU\Software\OxideSwarm\UsbKit` for `uninstall.ps1`. The
  worker itself does not check the permissions of a custom sandbox base on Windows
  (`ensure_private_dir` acts on Unix only); that is a separate fix.
- 2026-11-10: The install-folder menu was checked on a Windows 11 laptop with an internal `E:`
  (NTFS): it offered `E:\OxideSwarm`, skipped the kit stick (`D:`, exFAT, removable) and Google
  Drive (`G:`, FAT32), and refused a second install while one existed on drive C. Google Drive is
  skipped only because it reports FAT32, so the installer now also requires a disk partition behind
  the drive letter (`Win32_LogicalDiskToPartition`; a cloud or virtual drive has none) and refuses
  disks on the USB bus. The worker-side fix landed the same day (design 0021 status log).
- 2026-11-10: Update in place (`install.sh --update`, `install.ps1 -Update`, and the `Update
  OxideSwarm` launchers). During the trial every new kit meant uninstall and install on each
  machine. The update keeps the folder, name, build and start at login, does not join again and
  skips the doctor; the installer now records the name, build and workspace choice in
  `kit-install.conf` / `kit-install.txt`. The normal installer also stops this install's LaunchAgent
  or systemd unit before it replaces the binary.
