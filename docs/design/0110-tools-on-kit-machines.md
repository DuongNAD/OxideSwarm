# 0110: A developer toolset on kit machines: Python, Rust and Git in the USB kit, probes, and tool packs

Status: Accepted (Q99 decided by the owner on 2026-11-26: the full toolset)
Created: 2026-11-26    Updated: 2026-11-26

## Summary

A machine set up from the USB kit can run only what its owner already installed. On the friend's
laptop of the 2026-11-26 trial that was `cmd`, PowerShell, `tar` and `curl`: no Python script, no Rust
build and no workspace could go there. The owner chose the full toolset (Q99), so that every kit
machine can take every kind of task the project has. This record adds:

- probes for `python` (new) and stub-safe probes for `cargo`, `rustc`, `git` (TOOL-1);
- a Windows kit option `--toolset full` that carries pinned, private copies of Python (TOOL-1), a Rust
  GNU toolchain (TOOL-3), a MinGW-w64 C toolchain for crates that compile C (TOOL-5) and MinGit (TOOL-4), installed after one question inside the kit's own folder,
  visible to the worker only, and removed by uninstall;
- tool packs, which deliver any portable tool to any worker as a cached blob without installing it
  (TOOL-2), for machines set up without the kit and for other tool versions.

## Context

- **Probed tools.** `HOST_PROBED_TOOLS` in `crates/core/src/capabilities.rs` is `cargo`, `rustc`, `git`.
  `detect_host_tools` runs `<tool> --version` bounded by `HOST_PROBE_TIMEOUT`
  ([design 0020](0020-bounded-host-capability-probes.md)) and the worker advertises `tool:<name>` for
  each that exits 0 (`crates/worker/src/client.rs`, about line 875). Clients select with
  `submit --require-tool <name>` (`require:tool=<name>`, `crates/core/src/placement.rs`).
- **What `tool:cargo` unlocks.** Persistent workspaces for coding agents
  ([design 0039](0039-agent-workspaces.md)), `RustCompilation` tasks and the build-once mode of `shard`
  ([design 0103](0103-ax2-sharded-runs.md)) need `cargo` and `rustc` on the worker. The Windows kit
  turns on `--workspaces` only when `Get-Command cargo` succeeds in the installer's own session
  (`packaging/usb/install.ps1`, about line 1228), so a kit machine without Rust has no workspaces.
- **The kit installs the worker only.** `install.ps1` copies the release archive into the install
  folder, asks for the folder and the GPU build (`Let the cluster use this computer's GPU as well?
  [Y/n]`, about line 727), writes `start-worker.vbs`, which sets `OXIDESWARM_HOME` and `HOME` for the
  worker process (about line 1255), and registers it in `HKCU\...\Run`. `uninstall.ps1` removes the
  folder and revokes the `oxideswarm.toolchain` ACL grants of an isolated worker
  ([design 0055](0055-usb-kit-windows-isolation.md)). Under isolation the worker grants that capability
  SID read and execute on the Rust toolchain roots it finds (`crates/worker/src/isolation/windows/toolchain.rs`).
- **The trial machine (2026-11-26, `msi`, Windows 11, kit 0.4.5, `--isolation off`).** A task ran
  `where python py node git cargo rustc ffmpeg 7z tar curl powershell`: found `tar`, `curl`,
  PowerShell, and `python.exe` under `%LOCALAPPDATA%\Microsoft\WindowsApps`, which is the App Execution
  Alias of the Microsoft Store, not an installed Python.
- **Stubs that look like tools.** The Store alias prints a hint and exits non-zero. On macOS without the
  Command Line Tools, `/usr/bin/python3` and `/usr/bin/git` are stubs that open a GUI dialog offering to
  install them. A probe must not open that dialog on someone's machine.
- **Sizes measured on 2026-11-26.**
  - Python embeddable package 3.15.2 for amd64: 13.7 MB.
  - Rust 1.100.0 for `x86_64-pc-windows-gnu`, the four default components without docs (`rustc` 104.2 MB,
    `rust-std` 28.2 MB, `cargo` 12.1 MB, `rust-mingw` 5.5 MB, `.tar.xz` from the stable channel
    manifest): 150.0 MB compressed. `rust-mingw` is the self-contained MinGW linker, so no Visual Studio
    Build Tools are needed.
  - MinGit 2.56.1 64-bit (Git for Windows): 39.8 MB.
  - Together about 204 MB on the stick, beside about 100 MB of release archives. The trial sticks hold
    15.7 GB.
- **`rust-mingw` links but does not compile C.** Its `self-contained\GCC-WARNING.txt` says the bundled
  `gcc.exe` is only a linker, and T6 (Status log) confirmed it: a crate with a `cc` build script fails with
  `failed to find tool "gcc.exe"`. The project itself depends on `ring`, which compiles C, so building
  OxideSwarm or any crate with C parts on a kit machine needs a real MinGW-w64 C compiler (TOOL-5).
- **Rust with MSVC is out of reach.** `x86_64-pc-windows-msvc` needs the Visual Studio Build Tools
  (several GB, administrator install, a licence that does not allow redistribution), so the kit uses the
  GNU toolchain.

## Goals

- G1. A worker advertises `tool:python`, `tool:cargo`, `tool:rustc` and `tool:git` exactly when a working
  tool is on its `PATH`; a probe never opens a window or a dialog.
- G2. A Windows kit made with `make_kit.sh --toolset full` installs Python, Rust (GNU) and MinGit for the
  worker after one question, pinned and verified, inside the install folder, with no change to the
  user's `PATH`, registry or file associations, and uninstall removes all of it.
- G3. A machine set up that way takes every task kind of the project: command and shell tasks,
  Python scripts, `RustCompilation`, `cargo` builds in persistent workspaces, `shard --build-once`,
  GPU tasks (unchanged) and blob inputs and outputs.
- G4. The kit checks each installed tool end to end with a task pinned to the new worker.
- G5. Tool packs deliver a portable tool to any worker as a cached, verified blob, with nothing installed
  (TOOL-2).

## Non-goals

- Installing tools system wide, through `winget`, the Store or vendor installers; administrator rights.
- `pip` packages and virtual environments.
- Bundling Rust or Git in the macOS and Linux kits: their linkers come with the system (Command Line
  Tools on macOS, the distribution's `cc` on Linux) and cannot be redistributed with the kit. The probes
  of D1 and tool packs cover those machines.
- Any wire change. The new tags are values of the existing `tool:` prefix.

## Design

### D1 Probes (TOOL-1)

1. Add `python` to `HOST_PROBED_TOOLS`. The probe tries `python`, then `python3`, with `--version`, under
   the existing bound, and advertises `tool:python` when one exits 0 and prints `Python 3.`.
2. Windows: skip any candidate that resolves under `%LOCALAPPDATA%\Microsoft\WindowsApps` (Store
   aliases); resolution already goes through `resolve_windows_executable`.
3. macOS: skip `/usr/bin/python3` and `/usr/bin/git` unless `xcode-select -p` exits 0, so a probe never
   triggers the Command Line Tools dialog. A Homebrew or python.org Python earlier on `PATH` is used as is.
4. `docs/reference.md`: `tool:python` joins `cargo`, `rustc`, `git` in the list of probed tools.

### D2 The toolset lock and the kit builder

1. `packaging/usb/tools.lock` (new, text, one line per file) pins each file by tool, version, file name,
   download URL and SHA-256:
   - Python: the embeddable package for amd64 from `python.org/ftp/python/<ver>/`; its SHA-256 is
     published on the python.org release page.
   - Rust: `rustc`, `rust-std`, `cargo`, `rust-mingw` for `x86_64-pc-windows-gnu` from
     `static.rust-lang.org/dist/`; their SHA-256 values are in the channel manifest
     `channel-rust-<ver>.toml`. The pinned version is the project's CI toolchain (1.96.0 on 2026-11-26),
     so a kit machine builds the project with the toolchain CI checks.
   - C toolchain: a MinGW-w64 GCC distribution for `x86_64` with the `x86_64-w64-mingw32` target
     (candidates: w64devkit, WinLibs, LLVM-MinGW; TOOL-5 picks the smallest one that builds `ring` and the
     T6 crate, and records its size and licences).
   - MinGit: `MinGit-<ver>-64-bit.zip` from the `git-for-windows/git` GitHub release; its SHA-256 is
     listed in the release notes.
   A pull request changes a pin; nothing downloads an unpinned file.
2. `make_kit.sh --toolset full` (or `--toolset python`, `--toolset python,rust,git`) downloads each pinned
   file once into a cache directory (or takes it from `--tools-dir <DIR>`), refuses a file whose SHA-256
   differs from the lock, and writes it to `<kit>/tools/` with entries in the kit's `SHA256SUMS`. It also
   writes `<kit>/tools/NOTICE.txt` (D6). Without `--toolset` the kit is unchanged.
3. `kit.json` records the toolset (`"toolset": ["python","rust","git"]`), so `install.ps1` knows what to
   offer and `-Update` knows what a newer kit changes.

### D3 One question at install time

1. When the kit carries tools, `install.ps1` asks once, after the GPU question:
   `Install the developer tools for tasks (Python 3.15, Rust 1.96, Git 2.56, <N> MB on disk,
   only inside <install folder>)? [Y/n]`. The sizes and versions come from `tools.lock` and the files on
   the stick.
2. The default is Yes for a kit made with `--toolset`: the owner chose it when making the kit, and the
   person at the keyboard still sees and answers the question. `-Tools` / `-NoTools` answer it without a
   prompt (for scripted installs); `-Update` keeps the earlier answer.
3. On No the install is exactly today's.

### D4 Private installs (TOOL-1, TOOL-3, TOOL-4)

1. Verify each file against `SHA256SUMS`, then extract under `<InstallRoot>\tools\`:
   - `python-<ver>\` from the Python zip;
   - `rust-<ver>\` from the four Rust tarballs, merging each component's tree as its `install.sh`
     manifest lists (rustc, cargo, rust-std and rust-mingw into one sysroot), done by PowerShell without
     running the shell script;
   - `mingw-<ver>\` from the C toolchain archive;
   - `git-<ver>\` from the MinGit zip.
2. `<InstallRoot>\cargo-home\` becomes `CARGO_HOME` of the worker, so the registry and git caches of
   builds stay inside the install folder. `RUSTUP_HOME` stays unset (no rustup).
3. `start-worker.vbs` prepends `tools\python-<ver>`, `tools\rust-<ver>\bin`, `tools\mingw-<ver>\bin` and
   `tools\git-<ver>\cmd`
   to `PATH` and sets `CARGO_HOME`, in the worker's process environment only. Task processes inherit
   them; the user's own `PATH` and environment do not change.
4. Workspaces: the installer decides `--workspaces` from the private `cargo` it just installed, not only
   from `Get-Command cargo`, so a kit machine with the toolset gets persistent workspaces
   ([design 0039](0039-agent-workspaces.md)) and `tool:cargo`.
5. Isolation (`-Isolation fs`): the worker's existing toolchain grant covers `CARGO_HOME`; the installer
   grants `oxideswarm.toolchain` read and execute on `tools\` the same way and records it in the
   install's `windows_acl_grants.txt`, so `uninstall.ps1` revokes it with the others
   ([design 0055](0055-usb-kit-windows-isolation.md)). The elevated-worker rule that drops `tool:cargo`
   stays.
6. Update: `-Update` from a kit with newer pins installs the new versions beside the old ones, switches
   `start-worker.vbs`, restarts the worker, and then deletes the old folders. Uninstall deletes the whole
   install folder as today; the test asserts `tools\` and `cargo-home\` are gone.

### D5 End-to-end check (G4)

After the worker is listed, the installer submits one task per installed tool, pinned with
`--worker <name>` and `--require-tool <tool>`:

- `python -c "import sys; print(sys.version)"`;
- `rustc --version` and a `RustCompilation` of a one-file program from the kit (`tools\check\hello.rs`)
  whose output it runs;
- a `cargo build --offline` of a small vendored crate from the kit whose build script compiles C
  (`tools\check\cc-check\`), which checks the C toolchain;
- `git --version`.

It prints one line per tool. A failure is reported with the task's stderr and leaves the worker running
with the tools that passed.

### D6 Licences

`<kit>/tools/NOTICE.txt` lists each bundled tool, its version, licence and source:

- Python: PSF License (the package's own `LICENSE.txt` is extracted with it).
- Rust: MIT or Apache-2.0; `rust-mingw` carries MinGW-w64 runtime files under their licences (included
  in the component).
- C toolchain: GCC and binutils under the GNU GPL (version 3) with the GCC Runtime Library Exception,
  MinGW-w64 headers and runtime under their permissive licences; `NOTICE.txt` names the exact source
  release, as for MinGit.
- MinGit: GNU GPL version 2. Redistributing the binary requires the corresponding source: `NOTICE.txt`
  names the exact Git for Windows release and links its source archive, and `make_kit.sh` refuses a
  toolset kit without `NOTICE.txt`.

### D7 Tool packs (TOOL-2)

A tool pack is a portable archive that a task names; the worker places it read-only next to the task
instead of anyone installing it. Packs serve machines set up without the kit (macOS, Linux, a Windows
machine whose owner said No), other versions than the kit's, and tools the kit does not carry.

1. **Naming a pack.** A task carries a list of pack references: name, version, and the `BlobId` of the
   archive ([design 0006](0006-content-addressed-data-plane.md)). It travels in the extension slot of
   [design 0082](0082-task-extension-slot.md) as a critical `Worker` extension, so the master places such
   a task only on workers that carry `ext:<id>` (no protocol bump).
2. **Upload once.** The client uploads the archive like any `--input`; the master's blob store and the
   worker's blob cache keep it, so a cluster sends each pack to each worker once.
3. **Unpack once.** The worker extracts a pack into `<sandbox base>/cache/tools/<blake3>/`, verifying the
   archive id first and never following links, marks the tree read-only, and evicts it with the blob
   cache's LRU. Every task that names the pack gets the directory prepended to its `PATH`.
4. **CLI.** `submit --tool-pack <name>=<archive>` (a local archive, hashed and uploaded), and later a named
   catalogue built from `tools.lock`.

## Compatibility

- No wire change, no protocol bump. `tool:python` is a new value of the `tool:` prefix; an older master
  places `require:tool=python` on it like any tool.
- A kit made without `--toolset` behaves exactly as today. An older installer ignores `tools/` on a newer
  stick. A worker of any 0.4.x release benefits from D4: it only sees a different `PATH`.
- TOOL-2 uses the extension slot of design 0082 (protocol 10), so a worker that does not understand the
  extension never receives such a task.

## Security

- **Supply chain.** Every file is pinned by SHA-256 in `tools.lock`, checked by `make_kit.sh` before it
  reaches the stick and by `install.ps1` before extraction. A pin change is a reviewed pull request that
  cites the vendor's published hash (python.org release page, Rust channel manifest, Git for Windows
  release notes).
- **Consent and footprint.** One question with sizes and versions; inside the install folder only; no
  `PATH`, registry or file association change for the user; removed by uninstall.
- **Trust.** Tasks already run arbitrary programs as the worker's user. A toolset on the task `PATH`
  makes them more capable, not more privileged. `cargo` builds fetch crates from the network under
  `--isolation off` and `fs`; `fs+net` keeps them offline as today.
- **Probes.** Bounded by `HOST_PROBE_TIMEOUT`; never a window or dialog (D1.2, D1.3).
- **Tool packs** are verified by `BlobId` before unpacking and are read-only to tasks.

## Test plan

- T1. Unit: probe with fakes that print `Python 3.12.0` (tag), `Python 2.7.18` (no tag), exit 9009 (no
  tag); Windows candidates under `WindowsApps` (skipped); macOS stubs without Command Line Tools (not run).
- T2. `tests/test_launcher_scripts.py`: `make_kit.sh --toolset full` with fake downloads whose hashes
  match (kit holds them, `SHA256SUMS` and `NOTICE.txt` list them) and one that differs (refused);
  `install.ps1` with `-Tools` and `-NoTools` against fake binaries (the `start-worker.vbs` environment
  lines are checked); `uninstall.ps1` leaves no `tools\` or `cargo-home\`.
- T3. Live, a Windows machine without Python, Rust or Git: install from a `--toolset full` kit, answer
  the default; the worker lists `tool:python`, `tool:cargo`, `tool:rustc`, `tool:git` and
  `feat:workspaces`; the D5 checks pass; a `cargo build` of a small crate with dependencies runs in a
  workspace; `Update` from the same kit keeps the tools; uninstall leaves nothing. Repeat with
  `-Isolation fs`.
- T4. Live, the manual tool pack of today: send the Python embeddable zip as `--input` to a worker
  without Python, unpack it inside the task sandbox, run a script, bring the output back, run it again to
  show the inputs are not sent twice.
- T5. Live, the same for the four Rust GNU components: unpack them in a sandbox on a worker without Rust
  and build a pure-Rust crate with `cargo build --offline` against vendored sources.
- T6. Live, a crate whose build script compiles C (for example one that uses the `cc` crate): record
  whether `rust-mingw` alone is enough. If not, a full MinGW-w64 or LLVM-MinGW toolchain becomes a tool
  pack (new item), and D5 says which crates a kit machine cannot build.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TOOL-1 | Probe `python` (stub-safe probes); pinned private Python in the Windows kit; `tools.lock`, `--toolset`, the one question, `NOTICE.txt` | `crates/core/src/capabilities.rs`, `packaging/usb/{make_kit.sh,install.ps1,uninstall.ps1,tools.lock}`, `tests/test_launcher_scripts.py`, `docs/reference.md` | T1, T2 and the Python part of T3 pass | Planned |
| TOOL-3 | Private Rust GNU toolchain in the Windows kit, `CARGO_HOME` in the install folder, workspaces from the private `cargo`, isolation grant | `packaging/usb/{install.ps1,uninstall.ps1,tools.lock}`, `tests/test_launcher_scripts.py` | The Rust part of T3, T5 and T6 recorded | Planned |
| TOOL-5 | Private MinGW-w64 C toolchain in the Windows kit (and as a tool pack), so crates that compile C build | `packaging/usb/{make_kit.sh,install.ps1,tools.lock}` | T6 passes on a kit machine and a `cargo build` of OxideSwarm (it needs `ring`) passes there | Planned |
| TOOL-4 | Private MinGit in the Windows kit with its GPL source notice | `packaging/usb/{make_kit.sh,install.ps1,tools.lock}` | The Git part of T3 passes and `NOTICE.txt` names the source | Planned |
| TOOL-2 | Tool packs as cached blobs | `crates/core/src/task/extension.rs`, `crates/worker/src/{blob_cache.rs,staging.rs}`, `crates/cli` | A Python pack and a Rust pack run on a worker that has neither installed, each archive sent once per worker | Planned |
| Q99 | Owner decision: how Rust reaches kit machines | this record | Decided 2026-11-26: the full toolset (kit and tool packs) | Decided |

## Status log

- 2026-11-26: Draft, from the owner's question during the friend trial ("install and check Rust and
  Python on the other machine when installing from USB"). Design number, TOOL-1, TOOL-2 and Q99 claimed
  with `agent_bridge.py claim`.
- 2026-11-26: T4 run live on the trial cluster (master 0.4.4 on an Apple M5 Mac, worker `msi`: Windows 11
  10.0.26200, kit 0.4.5, `--isolation off`, no Python installed, direct P2P link). Inputs:
  `python-3.15.2-embed-amd64.zip` (13,740,486 bytes, SHA-256 `d012f3aa…b9ad1f62`, equal to the value on
  the python.org release page), a 30-line `analyze.py` and the `crates/` tree of `origin/master`. The task
  (`cmd /c run.cmd`: `Expand-Archive` into the sandbox, then `py\python.exe analyze.py`) printed Python
  3.15.2 on host `MSI` and returned `out/report.json`, whose tree SHA-256 and per-crate counts equal those
  of the same script run on the Mac. Run 1 waited 2.884 s, run 2 1.572 s (task time 1.40 s both): the
  inputs were not sent again. Nothing was installed on the laptop; the zip stays only in the worker's blob
  cache.
- 2026-11-26: The owner decided Q99: the full toolset ("full, all of it, otherwise it cannot do much;
  make it convenient and let the project work as fully as possible"). Status Accepted. Rust (TOOL-3) and
  MinGit (TOOL-4) added beside Python; one question with default Yes for a `--toolset` kit; tool packs
  (TOOL-2) moved from Design only to Planned. TOOL-3 and TOOL-4 claimed with `agent_bridge.py claim`.
  Sizes measured from the vendors' servers the same day (Context).
- 2026-11-26: T5 and T6 run live on the same worker `msi` (owner approved the download). Toolchain: Rust
  1.96.0 `x86_64-pc-windows-gnu`, the four components downloaded from `static.rust-lang.org/dist/2026-05-28/`
  (`.tar.xz`, 157 MB together, each SHA-256 equal to `channel-rust-1.96.0.toml`), merged on the Mac into one
  tree (816 MB, 199 files) and sent as one zip input (231.6 MB). The task (`run.cmd`: `tar -xf` in the sandbox,
  the tree's `bin` on `PATH`, `CARGO_HOME` in the sandbox, `cargo build --release --offline`) printed
  `rustc 1.96.0` and `cargo 1.96.0`. The first submission of the zip waited 29.0 s; later ones 4.4 to 7.1 s
  (the zip came from the worker's cache).
  - T5: a crate depending on `sha2` 0.10 (vendored) built in 2.47 s and printed the same SHA-256 as on the
    Mac. A crate depending on `blake3` 1 built in 2.96 s and printed the same hash as on the Mac; `blake3`
    falls back to Rust code when no C compiler is found, so it proves nothing about C.
  - T6: a crate whose `build.rs` compiles `c/add.c` with the `cc` crate failed on `msi` with `failed to find
    tool "gcc.exe": program not found` (it runs on the Mac). `rust-mingw` is a linker only, as its
    `GCC-WARNING.txt` says. TOOL-5 added (claimed with `agent_bridge.py claim`): the kit also needs a
    MinGW-w64 C compiler, or OxideSwarm itself (`ring`) cannot be built on a kit machine.
- 2026-11-26: TOOL-5 checked live on `msi`. w64devkit 2.11.0 x64 (`w64devkit-x64-2.11.0.7z.exe`, 68.8 MB, SHA-256
  equal to the GitHub release digest; unpacked on the Mac with `bsdtar`, 739 MB, sent as a 187.4 MB zip) put
  `gcc 16.2.0` on the task `PATH`. The T6 crate then compiled its C file but failed to link with
  `cannot find -lgcc_eh` (w64devkit's `ld` was picked as the linker). With
  `CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER` set to Rust's own
  `lib\rustlib\x86_64-pc-windows-gnu\bin\self-contained\x86_64-w64-mingw32-gcc.exe` (C compiled by
  w64devkit, linking by Rust's linker) it built in 1.71 s and printed `add_c(40, 2) = 42`. D4 must set that
  variable in `start-worker.vbs`.
- 2026-11-26: OxideSwarm itself built on `msi` from source (`origin/master` `df06768d`, 494 vendored crates,
  a 95.6 MB zip, `cargo build --offline -p rusty_grid_cli --bin oxideswarm` with the toolchains above).
  The debug build compiled 332 crates (including `ring`) and failed to link `iroh` with
  `ld: error: export ordinal too large: 110023` (more than 65,535 exports in a DLL on the GNU target when
  generics are shared in debug builds). The release build passed: `Finished release profile in 18m 04s`,
  `rusty-grid 0.4.5`, a 35.9 MB `oxideswarm.exe` returned to the Mac. The kit's check (D5) and any verify
  profile on a GNU Windows worker therefore build with `--release`, or set `-Zshare-generics=n` once that is
  stable.
- 2026-11-26: Relay measurements (the Mac blocked UDP with a `pf` anchor, so the worker fell back to the public
  n0 relay, as two machines on different networks without hole punching would): the link changed from
  direct (about 4 ms) to `Relay (DERP)` without a reconnect; small tasks returned in 0.58 to 0.70 s; a 50 MB
  input took 431 s (about 0.12 MB/s) and its hash matched; the 35.9 MB build output came back at the same
  rate. `doctor` against the relayed worker: C3 to C6 passed, C7 warned (relayed link, RTT 2,199.8 ms), and C8
  failed with `no result within 60s (last state Running)`, because 8 MiB at 0.12 MB/s needs about 67 s;
  BUG-41 records that. Large tool hand-offs (packs, kit updates) must use a direct link or a relay the owner
  runs.
- 2026-11-26: The owner asked that the work of this record and of design 0111
  come before release 0.5.0. The parts without a wire change (TOOL-1, TOOL-3, TOOL-4, TOOL-5) fit 0.4.x
  after the stability release; TOOL-2 uses the extension slot of design 0082 (protocol 10), so it fits too.
- 2026-11-26: T3 live throwaway kit run passed on Windows 11 x86_64 (`PR #479` at `386f060e`, merged with
  `origin/master` `034f5725`). A kit built with `make_kit.sh --toolset full --no-autostart --replace`
  from the `tools.lock` archives installed cleanly with `install.ps1 -Tools -NoAutostart` under a
  stripped host `PATH` to isolated NTFS throwaway folders in both `-Isolation off` and `-Isolation fs`
  modes: `doctor` passed 8/8 checks (1 skipped GPU), all D5 checks passed (`python` 3.15.2, `rustc`
  1.96.0, `hello.rs` -> `oxideswarm-tool-check-ok`, `cc-check --release` -> `add_c(40, 2) = 42`, `git`),
  worker advertised `feat:workspaces`, `tool:cargo`, `tool:rustc`, `tool:git`, and `iso:off`/`iso:fs`,
  explicit `python` and persistent-workspace `cargo build --release --offline` tasks succeeded
  (`add_c(40, 2) = 42`), `-Update` preserved the installed tools and reconnected the worker, and
  `uninstall.ps1 -Yes` removed the throwaway install folder and revoked all AppContainer ACL grants under
  `-Isolation fs`.

