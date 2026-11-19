# 0095: Contributor controls on each machine (NET-9)

Status: Draft (implementation open in a draft pull request until the stability release)
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
| Pause on battery | `--pause-on-battery` | `RUSTY_GRID_PAUSE_ON_BATTERY` | `pause_on_battery` | `false` |
| Pause on metered network | `--pause-on-metered` | `RUSTY_GRID_PAUSE_ON_METERED` | `pause_on_metered` | `false` |
| Stop-switch control file | `--control-file <PATH>` | `RUSTY_GRID_CONTROL_FILE` | `control_file` | `<OXIDESWARM_HOME>/worker_pause[.<NAME>]` |

Validation rules checked at worker startup (`crates/worker/src/controls.rs`):
- `--max-host-cpu-pct`: finite `f32` in `1.0..=100.0`. (Distinct env name
  `RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT` avoids colliding with master's `RUSTY_GRID_MAX_HOST_CPU_PCT`.)
- `--allow-hours`: comma-separated clauses `<Days> <HH:MM>-<HH:MM>` in local time (`chrono::Local`),
  supporting single days (`Mon`), day ranges including wrap-around (`Mon-Fri`, `Fri-Mon`), `Daily`
  or `All`, and `00:00-24:00`. An overnight window where `end <= start` (such as `Mon-Fri 22:00-07:00`)
  covers `[start..24:00)` on each named day and `[00:00..end)` on `day.succ()`.
- `--daily-task-hours`: finite `f64 > 0.0`. Counts cumulative completed task execution duration per
  local calendar date and resets automatically when the local date changes.

### D2 Per-OS detection and what is out of scope

| Signal | macOS | Linux | Windows | Out of scope / undetectable |
|---|---|---|---|---|
| Host CPU % | `sysinfo` background sampler (`heartbeat.rs`) | `sysinfo` background sampler (`heartbeat.rs`) | `sysinfo` background sampler (`heartbeat.rs`) | Containers without `/proc/stat` fall back to `0.0%` |
| Local clock & date | `chrono::Local` (`Clock` trait) | `chrono::Local` (`Clock` trait) | `chrono::Local` (`Clock` trait) | Clock skew adjustments external to OS timezone |
| Battery power | `pmset -g batt` via `crates/core/src/capabilities.rs` + `MobileTelemetryHandle` | `/sys/class/power_supply` (plus Android `dumpsys` / external mobile telemetry) | Win32 `GetSystemPowerStatus` (`ACLineStatus == 0 && BatteryFlag != 128`) | Desktop machines without battery hardware report `PowerSource::Unknown` |
| Metered network | `scutil --nwi` (primary active interface + `IsWWAN`) and `/sbin/ifconfig -v <iface>` (`EXPENSIVE` / `CONSTRAINED`) | NetworkManager `nmcli -t -f METERED general`, fallback `busctl get-property ... NetworkManager Metered` (`1`/`3` = metered, `2`/`4` = unmetered, `0` = unknown) | Bounded PowerShell WinRT query (`NetworkInformation::GetInternetConnectionProfile().GetConnectionCost()`, `Fixed`/`Variable`/`OverDataLimit`/`Roaming` = metered, `Unrestricted` = unmetered) | Linux without NetworkManager (`systemd-networkd`, plain DHCP/static), Android/Termux without platform feed, and upstream mobile hotspots over unmarked Wi-Fi/Ethernet |

Startup diagnostics:
- When `--pause-on-metered` is enabled and the initial probe returns `Unsupported`, the worker
  logs and prints a clear startup warning stating that metered-network state is unavailable on this
  OS setup, so no false sense of protection is given.
- When `--pause-on-battery` is enabled and the initial probe returns `Unknown`, the worker logs
  and prints a startup notice that no battery sensor is readable.
- OS probes are polled on a bounded interval (`30 s`) on `tokio::task::spawn_blocking` only when the
  corresponding control is enabled, never stalling the async runtime.

### D3 Signalling the master without a wire change

Protocol version stays `10`.
- **Proactive gate (`WorkerMessage::Heartbeat`):** While any local contributor control holds back new
  tasks (paused, CPU above cap, outside `--allow-hours`, daily cap reached, on battery, or on
  metered network), `spawn_adaptive_heartbeat_task_with_controls` sends `cpu_usage_pct = 100.0` in
  `WorkerMessage::Heartbeat`. On the master, `SeatStaticMeta::host_busy` triggers whenever
  `seat.worker.cpu_usage_pct > config.max_host_cpu_pct` (default `85.0`, `crates/master/src/scheduler.rs`),
  skipping new assignments while keeping the worker `Connected` and its running tasks intact. A
  pause or resume transition wakes the heartbeat task via `Notify` to send an immediate heartbeat.
- **Defense in depth at assignment (`crates/worker/src/client/tasks.rs`):** Because a task already in
  flight or a custom master scheduler may still deliver `AssignTask`, `handle_assign_task_with_checkpoint`
  evaluates `controls.check_admission()` before touching the sandbox or semaphore. If blocked, it
  logs the exact control holding the task back and replies with `TaskResult`:
  - `exit_code = 1`, `execution_time_ms = 0`, `stdout = ""`,
  - `stderr` and `error` = `Task rejected by worker: contributor control: <reason>`
    (`rusty_grid_core::CONTRIBUTOR_CONTROL_REFUSAL_PREFIX`).
  Every 0.4.x master already matches `"rejected by worker"` in `is_worker_rejection`
  (`crates/master/src/queue.rs`, [design 0090](0090-sandbox-base-refusal-hand-back.md)), and the
  updated master also lists `"rejected by worker: contributor control"` explicitly in
  `WORKER_REJECTION_MARKERS`.

### D4 Local stop switch (`worker pause` / `resume` / `status`)

- CLI subcommands:
  - `oxideswarm worker pause [--name <NAME>] [--control-file <PATH>]`
  - `oxideswarm worker resume [--name <NAME>] [--control-file <PATH>]`
  - `oxideswarm worker status [--name <NAME>] [--control-file <PATH>]`
- Default path matches `crates/worker/src/identity.rs`: `<OXIDESWARM_HOME>/worker_pause.<sanitized_name>`
  (or `<OXIDESWARM_HOME>/worker_pause` when unnamed), inside `<HOME>/.oxideswarm` unless
  `OXIDESWARM_HOME` or `--control-file` / `RUSTY_GRID_CONTROL_FILE` is set.
- `pause` writes an owner-only marker file (`0700` parent directory, `0600` file on Unix, symlinks
  refused); `resume` removes it; `status` reports `paused` or `active`.
- The running worker session polls the pause file every `100 ms` (and also accepts in-process
  `WorkerControlsHandle::pause()` / `resume()` for tests and embedders).

### D5 Interaction with running tasks (cancel vs `exit_code 130` vs requeue)

- **Soft controls** (CPU cap, weekly hours, daily cap, battery, metered network) refuse *new*
  assignments only; already-running tasks finish normally.
- **Local stop switch (`worker pause`):**
  1. Immediately blocks new task admission and signals an immediate heartbeat with `cpu_usage_pct = 100.0`
     while keeping the session connected (no `WorkerMessage::Disconnecting`, no reconnect loop).
  2. Drains `active_task_table` and sends `CancelSignal::ContributorPause` to every active task.
  3. Each active task aborts input fetching or terminates its supervised process tree
     (`ProcessTree::kill_async`, waited up to `PAUSE_STOP_TIMEOUT = 5 s`), clears any partial
     outputs/artifacts, and leaves `suppress_result = false`.
  4. Why **design 0090 refusal** instead of `exit_code 130`: `crates/master/src/queue/lifecycle.rs`
     treats `exit_code == 130` as terminal `TaskState::Cancelled` (reserved for master-initiated
     `CancelTask`, leaving the task cancelled for the submitter). For a contributor pause (`NET-9`),
     the task must return to `Queued` so another worker completes it without charging execution
     retries. Therefore the interrupted task emits the design 0090 refusal (`exit_code = 1`,
     `execution_time_ms = 0`, empty `stdout`, `Task rejected by worker: contributor control: paused by contributor`),
     so `is_worker_rejection` returns `true` and the master immediately requeues the task onto
     another worker with `retry_count - failover_count == 0` (zero execution retries charged).
  5. Note that tasks explicitly submitted with `max_retries == 0` ("run at most once") remain
     non-requeueable once `Running`, matching voluntary drain rules in [design 0036](0036-bounded-uncharged-worker-evacuations.md).

## Compatibility

- Protocol version stays `10`; store format stays `4`.
- Works against any `0.4.x` master because `Task rejected by worker: ...` already contains the
  `"rejected by worker"` marker matched by `is_worker_rejection` and `cpu_usage_pct = 100.0` triggers
  the existing `host_busy` gate.

## Security

- Controls live solely on the contributor's machine; no master directive can raise or bypass them.
- The pause file uses owner-only directory (`0700`) and file (`0600`) permissions on Unix and
  refuses symlinks (`symlink_metadata`), preventing another local user from toggling pause/resume.
- No unauthenticated local TCP socket is opened.

## Test plan

- **Unit (`crates/worker/src/controls.rs`):** schedule parsing & overnight rollover, invalid config
  rejection, injected clock/CPU/battery/metered probes, daily cumulative hours & midnight reset, all
  per-OS metered & Win32 power parsers, control-file permissions and symlink refusal.
- **Integration (`crates/worker/tests/test_net9_contributor_controls.rs`):** 2-worker master-worker
  tests with injected probes: CPU cap, outside-hours schedule, daily hours cap, battery pause,
  metered network pause, paused worker refusal & handback, and active-task interruption by `worker pause`
  completing on the second worker with zero execution retries charged (`retry_count - failover_count == 0`).
- **CLI (`crates/cli/src/cli_app/worker.rs`):** flag/env/config precedence and `worker pause|resume|status`
  command tests.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| NET-9 | Contributor controls on each machine (Stage 1 of design 0094) | `crates/core/src/error.rs`, `crates/master/src/queue.rs`, `crates/worker/src/controls.rs`, `crates/worker/src/client*.rs`, `crates/worker/src/heartbeat.rs`, `crates/cli/src/...`, `docs/reference.md`, `tests/test_net9_contributor_controls.rs` | Injected-probe worker and integration tests pass; `pause` stops running tasks within 5 s and hands them back without charging execution retries | Partial (design and draft PR open; merges after stability release) |

## Status log

- 2026-11-19: Claimed `design 0095` on the hub and wrote initial `Draft` design for `NET-9` before implementation on `mac/net-9-contributor-controls`.
