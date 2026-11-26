# 0110: Tools on kit machines: Python in the USB kit, a `tool:python` probe, and tool packs

Status: Draft
Created: 2026-11-26    Updated: 2026-11-26

## Summary

A machine set up from the USB kit can run only what its owner already installed. On the friend's
laptop of the 2026-11-26 trial that was `cmd`, PowerShell, `tar` and `curl`, so no Python script and no
Rust build could go there. This record adds three things. The worker probes `python` and
advertises `tool:python` (TOOL-1). The Windows kit can carry a pinned Python embeddable package and
install it, when the person at the keyboard agrees, inside the kit's own folder (TOOL-1). Tool packs
let a task bring a portable tool as a cached blob, with nothing installed at all (TOOL-2, Design only).
Rust is not put in the kit; the owner decides how Rust reaches kit machines (Q99).

## Context

- **Probed tools.** `HOST_PROBED_TOOLS` in `crates/core/src/capabilities.rs` is `cargo`, `rustc`, `git`.
  `detect_host_tools` runs `<tool> --version` bounded by `HOST_PROBE_TIMEOUT`
  ([design 0020](0020-bounded-host-capability-probes.md)) and the worker advertises `tool:<name>` for
  each that exits 0 (`crates/worker/src/client.rs`, about line 875). Clients select with
  `submit --require-tool <name>` (`require:tool=<name>`, `crates/core/src/placement.rs`). Python is not
  probed, so a Python task cannot be placed on a machine that has it.
- **The kit installs the worker only.** `packaging/usb/install.ps1` copies the release archive into the
  install folder, asks two questions (folder, GPU build: `Let the cluster use this computer's GPU as
  well? [Y/n]`, about line 727), writes `start-worker.vbs`, which sets `OXIDESWARM_HOME` and `HOME` for
  the worker process (about line 1255), and registers it in `HKCU\...\Run`. `uninstall.ps1` removes
  the folder and revokes the `oxideswarm.toolchain` ACL grants of an isolated worker
  ([design 0055](0055-usb-kit-windows-isolation.md)).
- **What the trial machine had (2026-11-26, `msi`, Windows 11, kit 0.4.5, `--isolation off`).** A task
  ran `where python py node git cargo rustc ffmpeg 7z tar curl powershell`. Found: `tar`, `curl`,
  PowerShell, and `python.exe` under `%LOCALAPPDATA%\Microsoft\WindowsApps`, which is the App Execution
  Alias of the Microsoft Store, not an installed Python. A real task on that machine (a 372-file
  source tree compressed with `tar` and hashed with PowerShell) worked; anything needing Python or Rust
  could only run on the Mac.
- **Stubs that look like tools.** On Windows the Store alias `python.exe` prints a hint and exits non-zero
  when Python is not installed. On macOS without the Command Line Tools, `/usr/bin/python3` is a stub
  that opens a GUI dialog offering to install them. A probe must not open that dialog on someone's
  machine.
- **Rust on Windows needs a linker.** The `x86_64-pc-windows-msvc` toolchain needs the Visual Studio
  Build Tools (several GB, administrator install). The `x86_64-pc-windows-gnu` toolchain ships its own
  linker but is still large (hundreds of MB). Both are far beyond the size of everything else on the
  stick, and an elevated isolated worker already drops `tool:cargo` (`client.rs`, about line 980).

## Goals

- G1. A worker advertises `tool:python` when a working Python 3 is on its `PATH`, and never when only a
  Store alias or the macOS stub is there; the probe never opens a window or a dialog.
- G2. A Windows kit made with `make_kit.sh --python` can install a pinned Python embeddable package for
  the worker, only after the person at the keyboard agrees, inside the install folder, removed by
  uninstall, with no change to the user's `PATH`, registry or file associations.
- G3. After such an install the kit checks it end to end: a task pinned to this worker with
  `--require-tool python` prints the expected version.
- G4. A design for tool packs (TOOL-2) that delivers a portable tool to any worker as a cached,
  verified blob, without installing anything, so the same path can later carry Rust.
- G5. The owner decides how Rust reaches kit machines (Q99) before any Rust work starts.

## Non-goals

- Installing Python system wide, through `winget`, the Store or the python.org installer.
- `pip`, virtual environments or third-party packages in the kit's Python (the embeddable package has
  no `pip`; tasks that need packages are a later tool-pack question).
- Python in the macOS and Linux kits: macOS and most Linux machines have Python 3 already, and the probe
  of G1 covers them.
- Rust in the kit (see D4 and Q99).
- Any wire change. `tool:python` is a new value of an existing tag prefix.

## Design

### D1 Probe `python` (TOOL-1)

1. Add `python` to the probed tools. The probe tries `python` and then `python3` with
   `--version`, under the existing bound, and advertises the single tag `tool:python` when either exits
   0 and its output starts with `Python 3.`.
2. Windows: skip a candidate whose resolved path lies under `%LOCALAPPDATA%\Microsoft\WindowsApps`
   (the Store alias). Resolution already goes through `resolve_windows_executable`.
3. macOS: skip `/usr/bin/python3` unless `xcode-select -p` exits 0 (Command Line Tools present), so the
   probe never triggers the install dialog. A Python from Homebrew or python.org earlier on `PATH` is
   used as is.
4. `docs/reference.md`: `tool:python` joins the list of probed tools next to `cargo`, `rustc`, `git`.

### D2 Python in the Windows kit (TOOL-1)

1. **Pin.** `packaging/usb/tools.lock` (new, text) names the Python embeddable package for
   `amd64` by version, file name, download URL on `python.org` and SHA-256. A pull request changes the
   pin; nothing downloads an unpinned file.
2. **Build the kit.** `make_kit.sh --python` downloads the pinned file once (or takes it from
   `--tools-dir <DIR>`), refuses it when its SHA-256 differs from the lock, and writes it to
   `<kit>/tools/` with an entry in the kit's `SHA256SUMS`. Without `--python` the kit is unchanged.
   The package's own `LICENSE.txt` (PSF licence) stays inside the zip and is extracted with it.
3. **Ask.** When the kit holds the package, `install.ps1` asks after the GPU question:
   `Install a private Python 3 for tasks (about 10 MB, only inside <install folder>)? [y/N]`.
   The default is No: this is someone else's machine. `-Python` / `-NoPython` answer it without a
   prompt; `-Update` keeps the earlier answer.
4. **Install.** Verify the zip against `SHA256SUMS`, extract it to `<InstallRoot>\tools\python-<ver>`,
   and write a marker file there. Nothing outside the install folder changes.
5. **Reach the worker.** `start-worker.vbs` prepends `<InstallRoot>\tools\python-<ver>` to `PATH` in
   the worker's process environment only (the same mechanism that sets `OXIDESWARM_HOME`). Task
   processes inherit it; the user's own `PATH` does not change.
6. **Isolation.** Under `-Isolation fs` the folder needs read and execute for the capability SID
   `oxideswarm.toolchain`: the installer grants it the way the worker grants toolchain roots, and
   records the grant in the install's `windows_acl_grants.txt`, so `uninstall.ps1` revokes it with the
   others ([design 0055](0055-usb-kit-windows-isolation.md)).
7. **Check (G3).** After the worker is listed, the installer submits
   `--worker <name> --require-tool python --wait --command python -- -c "import sys; print(sys.version)"`
   and prints the version line, next to the existing doctor checks. A failure is reported and leaves
   the worker running without Python.
8. **Update and uninstall.** `-Update` replaces the folder when the kit's pinned version differs and
   removes the old one. `uninstall.ps1` deletes `tools\` with the install folder (it already deletes the
   whole folder; the test asserts it).

### D3 Tool packs (TOOL-2, Design only)

A tool pack is a portable archive (for example the Python embeddable zip, or a Rust GNU toolchain
tree) that a task names; the worker places it read-only next to the task instead of anyone installing
it.

1. **Naming a pack.** A task carries a list of pack references: name, version, and the `BlobId` of the
   archive ([design 0006](0006-content-addressed-data-plane.md)). It travels in the extension slot of
   [design 0082](0082-task-extension-slot.md) as a critical `Worker` extension, so the master places such a
   task only on workers that carry `ext:<id>` (no protocol bump).
2. **Upload once.** The client uploads the archive like any `--input`; the master's blob store and the
   worker's blob cache keep it, so a cluster sends each pack to each worker once.
3. **Unpack once.** The worker extracts a pack into `<sandbox base>/cache/tools/<blake3>/`, verifying the
   archive id first, marks the tree read-only, and evicts it with the blob cache's LRU. Every task
   that names the pack gets the directory prepended to its `PATH`.
4. **CLI.** `submit --tool-pack <name>=<archive>` (a local archive, hashed and uploaded) and later a
   named catalogue. The first packs are the Python embeddable package and, if Q99 chooses it, a Rust
   GNU toolchain.
5. **Why this over installing.** Nothing persists outside the worker's cache; the uninstall stays a
   folder delete; the same path serves Windows, macOS and Linux workers; and the owner, not each
   machine, chooses tool versions.

A manual version of this (the archive as a plain `--input`, unpacked by the task itself in its
sandbox) runs on 0.4.x today and is the first live check of the idea (Test plan T4).

### D4 Rust on kit machines (Q99)

Three options for the owner:

- **A. None.** Rust tasks run only on machines whose owners installed Rust. Nothing to build.
- **B. Opt-in GNU toolchain in the kit.** Like D2, behind its own question; hundreds of MB on the stick
  and on the machine; licence notices for Rust (MIT/Apache-2.0) and the bundled MinGW parts.
- **C. Tool packs only.** No Rust in the kit; once TOOL-2 is built, a Rust pack is sent to the workers
  that run Rust tasks and cached there.

Default recommendation: **C**, with A until TOOL-2 lands. B puts the largest and least removable
install on the machines whose owners asked for the least.

## Compatibility

- No wire change, no protocol bump. `tool:python` is a value of the `tool:` prefix that every
  release since the tag was introduced already passes through; an older master places
  `require:tool=python` on it like any tool.
- A kit made without `--python` behaves exactly as today. An older installer ignores `tools/` on a
  newer stick.
- TOOL-2 uses the extension slot of design 0082 (protocol 10), so a worker that does not understand
  the extension never receives such a task.

## Security

- **Supply chain.** The Python package is pinned by SHA-256 in `packaging/usb/tools.lock`, checked by
  `make_kit.sh` before it reaches the stick and by `install.ps1` before extraction (kit `SHA256SUMS`).
  A pin change is a reviewed pull request.
- **Consent and footprint.** Default No; inside the install folder only; no `PATH`, registry or file
  association change for the user; removed by uninstall.
- **Trust.** Tasks already run arbitrary programs as the worker's user; a Python on the task `PATH`
  adds convenience, not a new right. Under `--isolation fs` the folder gets read and execute only.
- **Probes.** The probe never opens a window or dialog (D1.2, D1.3) and stays bounded by
  `HOST_PROBE_TIMEOUT`.
- **Tool packs** are verified by `BlobId` before unpacking, unpacked without following links (as
  `staging.rs` does for outputs), and read-only to tasks.

## Test plan

- T1. Unit: probe with a fake `python` that prints `Python 3.12.0` (tag), one that prints
  `Python 2.7.18` (no tag), one that exits 9009 (no tag), and Windows paths under `WindowsApps` (no
  tag); macOS stub path without Command Line Tools (not run at all).
- T2. `tests/test_launcher_scripts.py`: `make_kit.sh --python` with a fake download whose hash matches
  (kit holds it, `SHA256SUMS` lists it) and one whose hash differs (refused); `install.ps1` with
  `-Python` and `-NoPython` against a fake binary; `uninstall.ps1` leaves no `tools\`.
- T3. Live, Windows machine without Python: install from a `--python` kit, answer y; worker shows
  `tool:python`; the D2.7 check prints the version; `Update` from the same kit keeps it; uninstall
  leaves nothing. Repeat with `-Isolation fs`.
- T4. Live, today's manual tool pack: send the Python embeddable zip as `--input` to a worker without
  Python, unpack it inside the task's sandbox, run a script, bring an output back, and run it again to
  show the master does not upload the zip twice.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TOOL-1 | Probe `python`; optional pinned Python in the Windows kit | `crates/core/src/capabilities.rs`, `packaging/usb/{make_kit.sh,install.ps1,uninstall.ps1,tools.lock}`, `tests/test_launcher_scripts.py`, `docs/reference.md` | T1 to T3 pass | Planned |
| TOOL-2 | Tool packs as cached blobs | `crates/core/src/task/extension.rs`, `crates/worker/src/{blob_cache.rs,staging.rs}`, `crates/cli` | Design accepted, then built behind the design 0082 extension | Design only |
| Q99 | Owner decision: how Rust reaches kit machines (A, B or C of D4) | this record | The owner answers | Open |

## Status log

- 2026-11-26: Draft, from the owner's question during the friend trial ("install and check Rust and
  Python on the other machine when installing from USB"). Design number, TOOL-1, TOOL-2 and Q99
  claimed with `agent_bridge.py claim`.
