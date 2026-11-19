# 0095: Contributor controls on each machine (NET-9)

Status: Draft (implementation open in draft pull request 377 until the stability release)
Created: 2026-11-19    Updated: 2026-11-19

## Summary

Adds local contributor controls to `oxideswarm worker` (`NET-9`, Stage 1 of
[design 0094](0094-road-to-a-shared-research-grid.md)): a host CPU cap, a weekly local-time
schedule, a daily cumulative task-hours cap, pause on battery, pause on metered network where the OS
reports it, and a local stop switch (`oxideswarm worker pause`, `resume`, and `status`) that ends
every running task within `5 s` while keeping the worker connected. Uses zero wire changes (protocol
stays `10`, store format stays `4`).

## Context

- Today, host CPU backpressure (`max_host_cpu_pct`, default `85.0`) lives on the master
  (`crates/master/src/scheduler.rs`), and the worker only refuses tasks when battery is below `15%`
  and discharging (`crates/worker/src/client/tasks.rs`).
- Stopping active tasks on a worker currently requires stopping the entire worker process via
  `SIGINT`/`SIGTERM`.
- In a Stage 1 trusted circle ([design 0094](0094-road-to-a-shared-research-grid.md) D4), the
  organiser operates the master and participants lend machines to it. A contributor's resource and
  pause limits must hold locally on their own machine even if the master sends an assignment.

## Goals

- **G1:** Let each contributor configure their own CPU cap, allowed weekly local-time windows, daily
  cumulative task-hours cap, pause on battery, and pause on metered network via CLI flags,
  `RUSTY_GRID_*` environment variables, and `[worker]` config keys.
- **G2:** Provide a local stop switch (`oxideswarm worker pause` / `resume` / `status`) that blocks
  new tasks and stops running tasks within `5 s` without shutting down the worker process.
- **G3:** Prevent wasted dispatch loops with zero wire changes, while still enforcing every rule
  locally at assignment time.
- **G4:** Keep tests deterministic via injectable CPU, clock, power, and network-cost probes, with
  condition waits and no wall-clock timing windows.

## Non-goals

- Opt-in per campaign, task-class allowlists, and coordination across multiple worker processes on
  one host (stays in `NET-4`, design 0054).
- Idle-screen or user-presence detection (stays in `GREEN-1`, design 0056).
- Wire protocol or journal store format changes (per D3 below; Q97 keeps Stage 1 changes free of
  unneeded wire changes).

## Design

### D1 Configuration (`WorkerArgs`, `RUSTY_GRID_*`, `[worker]`)

Precedence follows existing worker options: CLI flag > `RUSTY_GRID_*` environment variable >
`[worker]` section of `ConfigFile` (`crates/cli/src/config.rs`) > off by default.

| Control | CLI flag | Environment variable | `[worker]` TOML key | Default |
|---|---|---|---|---|
| Host CPU cap | `--max-host-cpu-pct <PCT>` | `RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT` | `max_host_cpu_pct` | None (`off`) |
| Weekly schedule | `--allow-hours <SPEC>` | `RUSTY_GRID_ALLOW_HOURS` | `allow_hours` | None (`off`) |
| Daily task-hours cap | `--daily-task-hours <HOURS>` | `RUSTY_GRID_DAILY_TASK_HOURS` | `daily_task_hours` | None (`off`) |
| Pause on battery | `--pause-on-battery[=true\|false]` | `RUSTY_GRID_PAUSE_ON_BATTERY` | `pause_on_battery` | `false` |
| Pause on metered network | `--pause-on-metered[=true\|false]` | `RUSTY_GRID_PAUSE_ON_METERED` | `pause_on_metered` | `false` |
| Stop-switch control file | `--control-file <PATH>` | `RUSTY_GRID_CONTROL_FILE` | `control_file` | `<OXIDESWARM_HOME>/worker_pause[.<NAME>]` |

Pass `--pause-on-battery=false` or `--pause-on-metered=false` to override `true` from the environment or config file. A relative `--control-file` / `[worker].control_file` path resolves against the process working directory, which differs between a background service/daemon and an interactive shell; configure an absolute path (or omit it to use `<OXIDESWARM_HOME>`) for daemonized workers.

Validation rules checked at worker startup (`crates/worker/src/controls.rs`):
- `--max-host-cpu-pct`: finite `f32` in `1.0..=100.0` (blocks when host CPU strictly exceeds `>`, i.e., `usage_pct > limit_pct`). (Distinct env name `RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT` avoids colliding with master's `RUSTY_GRID_MAX_HOST_CPU_PCT`, which is also validated to `1.0..=100.0`.)
- `--allow-hours`: comma-separated clauses `<Days> <HH:MM>-<HH:MM>` in local time (`chrono::Local`),
  supporting single days (`Mon`), day ranges including wrap-around (`Mon-Fri`, `Fri-Mon`), `Daily`
  or `All`, and `00:00-24:00`. An overnight window where `end <= start` (such as `Mon-Fri 22:00-07:00`)
  covers `[start..24:00)` on each named day and `[00:00..end)` on `day.succ()`.
- `--daily-task-hours`: finite `f64 > 0.0`. Counts cumulative completed task execution duration per
  local calendar date and resets automatically when the local date changes. This budget is **in-memory
  only**: it resets if the worker restarts, sums durations across concurrent tasks running in
  parallel slots, and charges each task's duration only upon task completion (not while running or
  when interrupted by `worker pause`).

### D2 Per-OS detection and what is out of scope

| Signal | macOS | Linux | Windows | Out of scope / undetectable |
|---|---|---|---|---|
| Host CPU % | `sysinfo` background sampler (`heartbeat.rs`) | `sysinfo` background sampler (`heartbeat.rs`) | `sysinfo` background sampler (`heartbeat.rs`) | Containers without `/proc/stat` fall back to `0.0%` |
| Local clock & date | `chrono::Local` (`Clock` trait) | `chrono::Local` (`Clock` trait) | `chrono::Local` (`Clock` trait) | Clock skew adjustments external to OS timezone |
| Battery power | `pmset -g batt` via `crates/core/src/capabilities.rs` + `MobileTelemetryHandle` | `/sys/class/power_supply` (plus Android `dumpsys` / external mobile telemetry) | Win32 `GetSystemPowerStatus` (`ACLineStatus == 0 && BatteryFlag != 128`) | Desktop machines without battery hardware report `PowerSource::Unknown` |
| Metered network | `scutil --nwi` (primary active interface + `IsWWAN`) and `/sbin/ifconfig -v <iface>` (`EXPENSIVE` / `CONSTRAINED`) | NetworkManager `nmcli -t -f METERED general`, fallback `busctl get-property ... NetworkManager Metered` (`1`/`3` = metered, `2`/`4` = unmetered, `0` = unknown) | Bounded `%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe` WinRT query (`NetworkInformation::GetInternetConnectionProfile().GetConnectionCost()`, `Fixed`/`Variable`/`OverDataLimit`/`Roaming` = metered, `Unrestricted` = unmetered) | Linux without NetworkManager (`systemd-networkd`, plain DHCP/static), Android/Termux without platform feed, and upstream mobile hotspots over unmarked Wi-Fi/Ethernet |

Startup diagnostics & resilient probing:
- When `--pause-on-metered` is enabled and the initial probe returns `Unsupported`, the worker
  logs and prints a clear startup warning stating that metered-network state is unavailable on this
  OS setup, so no false sense of protection is given.
- When `--pause-on-battery` is enabled and the initial probe returns `Unknown`, the worker logs
  and prints a startup notice that no battery sensor is readable.
- OS probes are polled on a bounded interval (`30 s`) on `tokio::task::spawn_blocking` only when the
  corresponding control is enabled, never stalling the async runtime.
- If an OS probe command fails or times out, the worker keeps its **last known** `PowerSource` /
  `NetworkCost` and logs a warning instead of silently failing open; any genuine probe transition to
  `PowerSource::Unknown` or `NetworkCost::Unsupported` is also logged as a warning.

### D3 Signalling the master without a wire change

Protocol version stays `10`.
- **Proactive gate (`WorkerMessage::Heartbeat`):** While any local contributor control holds back new
  tasks (paused, CPU above cap, outside `--allow-hours`, daily cap reached, on battery, or on
  metered network), `spawn_adaptive_heartbeat_task_with_controls` sends
  `cpu_usage_pct = CONTRIBUTOR_HOLD_CPU_PCT` (`101.0`, exported from `rusty_grid_core`) in
  `WorkerMessage::Heartbeat` (while real non-held host CPU readings are clamped to `0.0..=100.0`).
  On the master, `SeatStaticMeta::host_busy` triggers whenever
  `seat.worker.cpu_usage_pct > config.max_host_cpu_pct` (default `85.0`, validated `1.0..=100.0`,
  `crates/master/src/scheduler.rs`), skipping new assignments while keeping the worker `Connected`
  and its running tasks intact—even when `--max-host-cpu-pct 100` is set to disable real-CPU
  throttling. Newer masters also map connected workers with `cpu_usage_pct > 100.0` to a
  `state:held` listing tag (`crates/master/src/server/client_and_members.rs`, accepted as connected
  by `oxideswarm workers` / `status`) and show `"Paused"` on the dashboard CPU readout instead of
  `100%`. Pause/resume transitions, OS probe changes, and exact `--allow-hours` window entry/exit
  minute boundaries wake the heartbeat task immediately so no dispatch gap lags behind a window edge.
- **Defense in depth at assignment (`crates/worker/src/client/tasks.rs`):** Because a task already in
  flight or a custom master scheduler may still deliver `AssignTask`, `handle_assign_task_with_checkpoint`
  evaluates `controls.check_admission()` before touching the sandbox or semaphore (and re-checks
  `controls.is_paused()` under the `active_task_table` write lock). If blocked, it logs the exact
  control holding the task back and replies with `TaskResult`:
  - `exit_code = 1`, `execution_time_ms = 0`, `stdout = ""`,
  - `stderr` and `error` = `Task rejected by worker: contributor control: <reason>`
    (`rusty_grid_core::CONTRIBUTOR_CONTROL_REFUSAL_PREFIX`).
  Every 0.4.x master already matches `"rejected by worker"` in `is_worker_rejection`
  (`crates/master/src/queue.rs`, [design 0090](0090-sandbox-base-refusal-hand-back.md)).
- **Note for the protocol 11 train:** replace the `101.0` heartbeat sentinel overload with a typed
  worker hold state / pause reason carried explicitly on `Heartbeat` or a dedicated control message
  gated behind a negotiated protocol feature bit.

### D4 Local stop switch (`worker pause` / `resume` / `status`)

- CLI subcommands:
  - `oxideswarm worker pause [--name <NAME>] [--control-file <PATH>]`
  - `oxideswarm worker resume [--name <NAME>] [--control-file <PATH>]`
  - `oxideswarm worker status [--name <NAME>] [--control-file <PATH>]`
- Default path matches `crates/worker/src/identity.rs`: `<OXIDESWARM_HOME>/worker_pause.<sanitized_name>`
  (or `<OXIDESWARM_HOME>/worker_pause` when unnamed), inside `<HOME>/.oxideswarm` unless
  `OXIDESWARM_HOME` or `--control-file` / `RUSTY_GRID_CONTROL_FILE` is set.
- `pause` writes an owner-only marker file (`0600` file on Unix, protected owner-only ACL on Windows)
  without altering permissions of any pre-existing parent directory (`chmod`s `0700` only on missing
  parent directories it newly creates inside a safe ancestor, matching `sandbox.rs`, and refuses
  symlinks/reparse points and foreign-owned or group/world-writable parent directories and files).
  `resume` removes the marker file; `status` reports `paused` or `active`.
- **`.watch` PID indicator:** While `WorkerClient::run` is active, it maintains a companion
  `<control_file>.watch` PID file (with the same safety checks, removed cleanly on exit if still
  owned by its PID). `oxideswarm worker pause` and `worker status` inspect `<control_file>.watch`,
  probe PID liveness (`kill(pid, 0)` on Unix; `OpenProcess` on Windows), and print a warning on
  `stderr` if no live worker watches that path (e.g., wrong `--name` or a service running under
  another account).
- The running worker polls the pause file every `100 ms` across its full `WorkerClient::run`
  lifetime—including while disconnected and waiting in reconnect backoff during a session-resume
  grace window—so pausing while disconnected still stops running tasks promptly.

### D5 Interaction with running tasks (cancel vs `exit_code 130` vs requeue)

- **Soft controls** (CPU cap, weekly hours, daily cap, battery, metered network) refuse *new*
  assignments only; already-running tasks finish normally.
- **Local stop switch (`worker pause`):**
  1. Immediately blocks new task admission and signals an immediate heartbeat with
     `cpu_usage_pct = 101.0` while keeping the session connected (no `WorkerMessage::Disconnecting`,
     no reconnect loop).
  2. Drains `active_task_table` and sends `CancelSignal::ContributorPause` to every active task.
  3. Each active task aborts input fetching or terminates its supervised process tree
     (`ProcessTree::kill_async`, waited up to `PAUSE_STOP_TIMEOUT = 5 s`), clears any partial
     outputs/artifacts, and keeps `suppress_result = false`. If a task does not stop within `5 s`
     (e.g., blocked inside non-cancellable input staging), `join.abort()` cancels the task future,
     drops/reaps any `ProcessTree`, and still dispatches the design 0090 refusal so the master
     never leaves the task stuck `Running` on a connected worker.
  4. Why **design 0090 refusal** instead of `exit_code 130`: `crates/master/src/queue/lifecycle.rs`
     treats `exit_code == 130` as terminal `TaskState::Cancelled` (reserved for master-initiated
     `CancelTask`, leaving the task cancelled for the submitter). For a contributor pause (`NET-9`),
     the task must return to `Queued` so another worker completes it without charging execution
     retries. Therefore the interrupted task emits the design 0090 refusal (`exit_code = 1`,
     `execution_time_ms = 0`, empty `stdout`, `Task rejected by worker: contributor control: paused by contributor`),
     so `is_worker_rejection` returns `true` and the master immediately requeues the task onto
     another worker with `retry_count - failover_count == 0` (zero execution retries charged).
  5. Note on refusal limits: **any** worker refusal of a task submitted with `max_retries == 0`
     fails that task immediately—even before it has started running (`crates/master/src/queue.rs`
     `zero_retries_for`)—and the **4th** worker refusal of any task
     (`MAX_WORKER_REJECTIONS_PER_TASK = 3`, `crates/master/src/queue/lifecycle.rs`) charges an
     execution retry or fails it once retries are exhausted.

## Compatibility

- Protocol version stays `10`; store format stays `4`.
- Works against any `0.4.x` master whose `--max-host-cpu-pct` is at or below `100.0` (including the
  default `85.0` and `100.0`, because `101.0 > 100.0` always triggers `host_busy`; the master CLI
  validates `--max-host-cpu-pct` to `1.0..=100.0`), and `"Task rejected by worker: ..."` matches
  `"rejected by worker"` in `is_worker_rejection`.

## Security

- Controls live solely on the contributor's machine; no master directive can raise or bypass them.
- Control files (`.pause` and `.watch`) validate existing parent directories without changing their
  permissions, create only missing directories with owner-only permissions (`0700` on Unix /
  protected DACL on Windows), refuse symlinks/reparse points and group/world-writable or
  foreign-owned parents/files, preventing another local user from spoofing pause/resume or tricking
  `worker pause` into `chmod`ing `$HOME` or `/tmp`.
- No unauthenticated local TCP socket is opened.

## Test plan

- **Unit (`crates/worker/src/controls.rs`, `heartbeat.rs`, `client/tests.rs`):** schedule parsing &
  overnight rollover, `next_transition_after` window boundary timing, invalid config rejection
  (worker and master `--max-host-cpu-pct`), injected clock/CPU/battery/metered probes, daily
  cumulative hours & midnight reset, probe failure retaining last known state, all per-OS metered &
  Win32 power parsers + Windows PowerShell path resolution, control-file safe parent handling &
  symlink/mode refusal, `.watch` PID lifecycle, heartbeat `101.0` emission, `--pause-on-*=false`
  CLI overrides, and `5 s` pause-timeout abort still emitting the design 0090 refusal.
- **Integration (`crates/worker/tests/test_net9_contributor_controls.rs`):** 2-worker master-worker
  tests with injected probes: CPU cap, outside-hours schedule, daily hours cap, battery pause,
  metered network pause, paused worker refusal & `state:held` tag, soft controls letting running
  tasks finish while holding new tasks against master `--max-host-cpu-pct 100`, active-task
  interruption by `worker pause` (proving run 1's PID is dead via `is_process_alive` and completing
  on worker 2 with `retry_count - failover_count == 0`), and `worker pause` stopping a running task
  while disconnected during reconnect.
- **CLI (`crates/cli/src/cli_app/worker.rs`, `master.rs`, `tests.rs`):** flag/env/config precedence,
  `worker pause|resume|status` & `.watch` checks, master `--max-host-cpu-pct` validation, and
  `state:held` counted as connected.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| NET-9 | Contributor controls on each machine (Stage 1 of design 0094) | `crates/core/src/capabilities.rs`, `crates/core/src/error.rs`, `crates/master/src/server/client_and_members.rs`, `crates/master/src/dashboard.html`, `crates/worker/src/controls.rs`, `crates/worker/src/controls/tests.rs`, `crates/worker/src/client*.rs`, `crates/worker/src/heartbeat.rs`, `crates/cli/src/...`, `README.md`, `docs/reference.md`, `crates/worker/tests/test_net9_contributor_controls.rs` | Injected-probe worker and integration tests pass; `pause` stops running tasks within 5 s (including across disconnects and abort fallback) and hands them back without charging execution retries | Partial (design and draft PR open; merges after stability release) |

## Status log

- 2026-11-19: Claimed `design 0095` on the hub and wrote initial `Draft` design for `NET-9` before implementation on `mac/net-9-contributor-controls`.
- 2026-11-19: Implemented `NET-9` and opened draft pull request 377 (zero wire changes; verified on macOS).
- 2026-11-19: Addressed pull request 377 review fixes (`101.0` `CONTRIBUTOR_HOLD_CPU_PCT` + `state:held` / dashboard `"Paused"`, safe parent/file checks + `.watch` PID warning, abort-path refusal, worker-lifetime pause watcher, resilient OS probes, `--allow-hours` boundary heartbeats, doc/test additions).
