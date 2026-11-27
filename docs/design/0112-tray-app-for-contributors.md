# 0112: A tray app for contributors: the worker in the background, its state in view, and a pause at hand

Status: Draft
Created: 2026-11-26    Updated: 2026-11-26

## Summary

A person who lends a machine to a cluster needs three things the worker does not give today: to know
that it runs, to pause it without uninstalling, and to stop it from getting in the way (a game, a
battery, a metered network). This record adds a small app in the Windows notification area, the macOS
menu bar and the Linux tray (TRAY-1). It starts with the user's session, starts and watches the worker
in the background, shows its state, and offers Pause, Resume, the limits of NET-9, logs and Quit. It also
joins the machine to a cluster on first run from a join token or a USB kit (TRAY-2), ships in the USB
kit and the installers with start at logon on (TRAY-3), and pauses the worker while a game or another
full-screen program runs (TRAY-4). The worker's own controls stay in the worker (NET-9); the app is their
face.

## Context

- **Start at logon exists, a face does not.** The Windows USB kit registers `start-worker.vbs` under
  `HKCU\Software\Microsoft\Windows\CurrentVersion\Run` (value `OxideSwarmUsbWorker`), which starts the
  worker hidden at logon without administrator rights (`packaging/usb/install.ps1`, about line 150); the
  macOS and Linux installers use a launchd agent and a systemd user unit. A kit made with
  `make_kit.sh --no-autostart` registers nothing ([design 0053](0053-usb-setup-kit.md)).
- **What happened in the friend trial of 2026-11-26.** The sticks were made with `--no-autostart`, so
  after every restart someone had to find the install folder and start the worker again; the owner asked
  for a background start or a desktop app. With start at logon on, the worker runs with no window, and the
  person at the machine cannot see whether it runs, what it runs, or stop it except through Task Manager
  or by uninstalling.
- **What already limits the worker.** `--max-concurrency` (the kit picks half the processors on AC power,
  a quarter on battery), `--task-priority below-normal` by default (task processes only), `--isolation`,
  and the master's host CPU threshold. GPU tasks run inside the worker process and do not yield to a game.
  The worker has no pause; the only local stop is ending the process, which ends every task tree.
- **NET-9** (Planned, [design 0094](0094-road-to-a-shared-research-grid.md)) adds controls the
  contributor sets and the worker enforces: its own CPU threshold, hours, no task on battery, no task on a
  metered network, and `oxideswarm worker pause` / `resume` that takes no new task and ends running tasks
  within 5 s while the worker stays connected (the tasks go back without a retry charged,
  [design 0090](0090-sandbox-base-refusal-hand-back.md)).
- **What a contributor fears.** Fans and heat, a slower game, a drained battery, data on a metered
  connection, a program that cannot be stopped, and code from strangers. Each must have a visible answer.

## Goals

- G1. After one install, the worker runs whenever the user is logged in, with no window and no folder to
  find, on Windows, macOS and Linux.
- G2. The person at the machine sees at a glance whether the worker is connected, idle, busy or paused,
  and on what.
- G3. Pause and Resume take one click and act within 5 s; a paused worker stays in the cluster.
- G4. The NET-9 limits are set from the app, stored in the user's own configuration, and enforced by the
  worker even if the app is closed.
- G5. A game or another full-screen program pauses the worker by default.
- G6. A machine joins a cluster from the app without a terminal.
- G7. The app needs no administrator rights, installs per user, and uninstalls with the worker.

## Non-goals

- A full desktop client for submitting work or managing a cluster; the master's dashboard stays the
  place for that, and the app only opens it.
- Running the worker as a system service or before logon.
- Code signing and notarisation (a release task of its own, LV-5); the record notes what is unsigned.

## Design

### D1 The app and the worker (TRAY-1)

1. One binary, `oxideswarm-tray`, built from the workspace (a new crate `crates/tray`), for Windows, macOS
   and Linux. It is the process that starts at logon; it starts `oxideswarm worker` with the install's
   arguments and environment (today's `start-worker.vbs` contents), restarts it with backoff if it dies,
   and ends it on Quit. On Linux without a tray (some desktops), it falls back to today's systemd unit and a
   `--status` command line.
2. It talks to the worker over the local control channel of design 0113 (#472)
   D2.4 (`crates/worker/src/control.rs`), which only the same user can open: the named pipe
   `\\.\pipe\oxideswarm-<user>-<name>` on Windows, the socket `[<name>.]control.sock` (mode 600) in
   `$XDG_RUNTIME_DIR/oxideswarm/` or the OxideSwarm home elsewhere, where `<name>` is the worker's
   `--name` (`default` when unset); the tray passes the same `--name` or `--control-socket` it gives the
   worker. One tagged JSON line per request: `status`, `pause`, `resume`, `limits_get`, `limits_set`. No
   network port is opened.
   The status snapshot of NET-9a has the state (`running`, `held`, `paused`), its reason, the pause end
   time, the number of active tasks and the limits. TRAY-1 adds to it, in `control.rs`, what D1.3 to D1.5
   need and NET-9a does not: whether the worker is connected and over which link (`connected`,
   `link_kind`), the last error, the master's address and dashboard URL, and a `tasks_started` counter
   that only grows (a fast task can start and end between two polls). The fields are optional, so an
   older worker still answers.
3. Icon states: connected and idle, busy (number of running tasks), paused (with the reason: by hand, on
   battery, a game, hours, metered network), not connected (relay or direct shown on hover), error.
4. Menu: state line; Pause for 1 h / until I resume (`pause` with `until_epoch_ms` one hour ahead, or
   none); Resume; Limits (CPU %, hours, daily task hours, on battery, metered network, GPU on/off: the
   `[worker]` keys of design 0113 D1, set with `limits_set`; and while gaming, which is a setting of the
   tray itself, D4.1); Open dashboard (the URL from `status`); Open logs; Check for updates; Quit (no
   request: the tray ends the worker it started, which stops it until next logon).
5. A notice (once) when the first task arrives after install, naming the cluster and how to pause
   (`tasks_started` going above zero for the first time).

### D2 First run and join (TRAY-2)

1. If the install has no cluster membership yet, the app shows a small window: paste a join token
   ([design 0048](0048-join-token.md)) or an invite ([design 0051](0051-machine-identity-and-invites.md)),
   or "Read from a USB kit" (it finds `join.token` on a mounted kit). It then runs `oxideswarm join` and
   starts the worker. The token never appears on screen after it is pasted and is not logged.
2. Updates: "Check for updates" compares the running release with the newest GitHub release, downloads the
   archive for this machine only after the user clicks, verifies it against `SHA256SUMS` and the release
   attestation (LV-5), and restarts the worker on the new binary. A cluster's master stays on the same or a
   newer protocol (design 0083, upgrade the master first).

### D3 Install, kit and start at logon (TRAY-3)

1. The USB kit and the installers install `oxideswarm-tray` next to `oxideswarm` and register the tray, not
   the bare worker, at logon (`HKCU\...\Run` on Windows, a launchd agent on macOS, an XDG autostart entry on
   Linux). `make_kit.sh` keeps `--no-autostart` for kits that must leave nothing behind, and adds
   `--no-tray` for headless machines (the worker starts at logon as today).
2. Uninstall quits the tray, removes the logon entry, then removes the install folder as today.

### D4 Pause while gaming, and the GPU (TRAY-4)

1. By default the worker pauses while a full-screen program runs in the foreground: on Windows,
   `SHQueryUserNotificationState` reports `QUNS_RUNNING_D3D_FULL_SCREEN` or `QUNS_BUSY`; on macOS, a
   full-screen window of another app on the active display; on Linux, the `_NET_WM_STATE_FULLSCREEN`
   window or the desktop's inhibit API. The tray reads it and sends `pause` with the reason `fullscreen`,
   and `resume` 60 s after it ends.
   The worker has one pause slot, so the tray must not overwrite the contributor's own pause or lift it:
   it sends `pause` only when `status` shows `running`, and `resume` only when the state is still
   `paused` with the reason `fullscreen`. A pause by hand while a game runs therefore replaces the
   fullscreen one and stays after the game ends.
2. "GPU on/off" in Limits is the `no_gpu_tasks` key of design 0113 (`limits_set`): the worker refuses GPU
   tasks while it is set and the master holds back only GPU tasks for it, so a gamer can lend only the
   CPU. No re-registration is needed.
3. The worker already runs task processes below normal priority; GPU work inside the worker cannot be
   pre-empted mid-dispatch (an already submitted wgpu dispatch cannot be interrupted,
   `crates/worker/src/wgpu_engine.rs`), which is why the pause matters for G5.

### D5 Toolkit (Q102)

Two ways to draw the app:

- **A. Native tray menu only** (`tray-icon` and `muda` crates, the same ones Tauri uses underneath): a
  small binary, no web engine; the first-run window and Limits are simple native dialogs, and "Open
  dashboard" opens the master's dashboard in the browser.
- **B. Tauri**: a web view that can host richer screens (task list, charts) by reusing
  `crates/master/src/dashboard.html`; larger, and it needs WebView2 on Windows (present on Windows 11).

Decision (Q102, the owner, 2026-11-27): **A**, the native tray menu. The app's job is presence and control; rich views exist in the dashboard. Tauri stays possible later for a richer window without changing the control channel.

Measured on Windows 11 by agent-win on 2026-11-26 (spike; both drove the NET-9a control channel of
#478, both pass the licence check of `deny.toml`):

| | A: `tray-icon` 0.21.3 + `muda` 0.17.2 | B: Tauri 2.12.7 |
|---|---|---|
| Release binary (size-optimised) | 272 KB | 4.85 MB |
| Idle memory, tray only | 8.6 MB | 10.6 MB |
| Idle memory with one window open | (native dialog) | 361 MB over 7 processes |
| Cold build | 15 s | 192 s |
| Crates pulled in | 24 | 244 |

These numbers are the basis of the decision above.

## Compatibility

- TRAY-1 to TRAY-3 add no wire change: the control channel is local, between the tray and its worker.
- A worker without the control channel (any release before NET-9) is still started and watched by the
  tray; Pause then falls back to ending the worker process (tasks are re-run elsewhere as today) and the
  menu says so.
- Kits made with `--no-tray` or `--no-autostart` behave as today.

## Security

- The control channel is reachable only by the same user (pipe ACL, socket mode 600). It never carries the
  cluster secret.
- The app never asks for administrator rights. It is unsigned until LV-5 signs releases, so Windows
  SmartScreen and macOS Gatekeeper may warn on first start; the kit's guide says what the user sees.
- The join token is read once, passed to `oxideswarm join` through standard input, and not stored by the
  app or written to its log.
- Updates download only after a click and only verified archives.
- The tray does not change what tasks may do; the recommended setting for machines of other people stays
  `--isolation fs` ([design 0055](0055-usb-kit-windows-isolation.md)).

## Test plan

- T1. Unit: icon state from status records; limits round trip through the configuration file; reasons of
  pause; the full-screen detector against recorded inputs on each system.
- T2. Integration: tray starts a fake worker, restarts it after a crash with backoff, Quit ends it; the
  control channel refuses another user (Windows and Unix).
- T3. `tests/test_launcher_scripts.py`: kit install registers the tray at logon; `--no-tray` registers the
  worker; uninstall leaves no logon entry, no tray process and no folder.
- T4. Live: a Windows laptop and a Mac from a USB kit: log out and in, the worker runs; Pause and Resume
  within 5 s with a task running (it reruns elsewhere); a full-screen game pauses it and it resumes 60 s
  after; battery unplugged pauses it when the limit is set.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TRAY-1 | Tray app: start and watch the worker, state, Pause/Resume, Limits, logs, Quit | `crates/tray` (new), `crates/worker` (control channel, with NET-9) | T1, T2 pass | Planned |
| TRAY-2 | First run join from token, invite or USB kit; verified updates | `crates/tray`, `crates/cli/src/join.rs` | T2 (join part) passes | Planned |
| TRAY-3 | Tray in the USB kit and installers, start at logon by default, `--no-tray` | `packaging/usb/*`, installers under `scripts/`, `windows/`, `packaging/` | T3 passes | Planned |
| TRAY-4 | Pause while a full-screen program runs; GPU on/off | `crates/tray`, `crates/worker` | T1 (detector) and T4 pass | Planned |
| Q102 | Owner decision: native tray menu (A) or Tauri (B) | this record | The owner answers or accepts A | Done (A, 2026-11-27) |

## Status log

- 2026-11-26: Draft, at the owner's request during the friend trial: starting the worker by hand after
  every restart is a nuisance; run it in the background, and if that has drawbacks, give the user a
  desktop app. The same day the owner's two USB sticks were rebuilt without `--no-autostart`, so a kit
  install starts the worker at logon again (the guide `HUONG-DAN-THU-NGHIEM` says how to stop it). Design
  number, TRAY-1 to TRAY-4 and Q102 claimed with `agent_bridge.py claim`.
- 2026-11-27: Revised after agent-win-2's design-vs-code review (comment on #469 against #478 at
  `c6542ee9`): D1.2 uses the endpoints and requests of design 0113 and lists the status fields TRAY-1
  adds; D1.4 names the keys; D4.1 guards the single pause slot; D4.2 is decided (`no_gpu_tasks`). D5 has
  the Q102 spike measurements.
- 2026-11-27: Q102 decided by the owner: the native tray menu (D5 option A, `tray-icon` and `muda`).
