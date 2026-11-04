# 0028: Host battery probe override (`RUSTY_GRID_NO_BATTERY`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

`WorkerConfig::with_no_battery(true)` disables host battery and thermal probing at worker start-up
and in the background mobile-status poller so tests do not depend on the host laptop's battery. For
CLI subprocess tests, `oxideswarm worker` reads a single environment variable,
`RUSTY_GRID_NO_BATTERY`, passes it to `WorkerConfig::with_no_battery`, and logs a start-up warning
whenever battery and thermal protection is disabled.

## Context

`WorkerClient::new` (`crates/worker/src/client.rs`) calls
`WorkerCapabilities::detect_with_overrides` (`crates/core/src/capabilities.rs`), which probes
the host battery (`/sys/class/power_supply` on Linux/Android, `pmset -g batt` on macOS,
`/system/bin/dumpsys battery` on Android) and seeds `MobileTelemetryHandle`
(`crates/worker/src/client.rs`). When a laptop runs below 15 % battery while discharging,
`MobileLiveStatus::compute_battery_tier` classifies it as `BatteryTier::Critical`, causing both
`WorkloadScheduler::schedule_batch` on the master and `WorkerClient::handle_assign_task` on the
worker to refuse tasks. Consequently, running the test suite on an unplugged laptop below 15 %
failed or hung 47 test targets (`TEST-2`).

An initial fix read `RUSTY_GRID_NO_BATTERY`, `OXIDE_SWARM_NO_BATTERY`, and
`RUSTY_GRID_DISABLE_BATTERY`, accepted `no_battery` in `rusty-grid.toml`, read the environment
directly inside `crates/core/src/capabilities.rs`, turned battery protection off inside the public
`WorkerClient::from_options` constructor, and logged nothing when protection was off.

## Goals

- **G1**: Isolate in-process and CLI integration tests from the host's physical battery state.
- **G2**: Expose exactly one documented environment variable (`RUSTY_GRID_NO_BATTERY`), read only
  in the CLI layer (`crates/cli/src/cli_app.rs`) and passed explicitly via
  `WorkerConfig::with_no_battery`.
- **G3**: Prevent configuration files (`rusty-grid.toml` / `rusty-grid.json`) and undocumented
  alias variables from disabling battery and thermal protection.
- **G4**: Emit an explicit start-up warning in the worker log and on `stderr` whenever battery and
  thermal protection is disabled.
- **G5**: Keep `WorkerClient::from_options` with battery protection on by default, and provide an
  explicit test-only constructor (`WorkerClient::for_testing`) for integration tests.

## Non-goals

- Adding a `--no-battery` CLI flag or a `rusty-grid.toml` key.
- Changing the 15 % critical battery threshold (`BatteryTier::Critical`) or external mobile
  telemetry injection (`WorkerConfig::with_mobile_telemetry`).

## Design

### D1: Explicit `HardwareOverrides::no_battery` and `WorkerConfig::no_battery`

`HardwareOverrides` (`crates/core/src/capabilities.rs`) and `WorkerConfig`
(`crates/worker/src/client.rs`) carry `pub no_battery: bool` (default `false`), set via
`WorkerConfig::with_no_battery(bool)`. When `no_battery` is `true`:

1. `WorkerCapabilities::detect_with_overrides` (`crates/core/src/capabilities.rs`) skips
   `detect_mobile_capabilities()` and sets `capabilities.mobile = None`.
2. `WorkerClient::run` (`crates/worker/src/client.rs`) does not spawn the background
   `spawn_mobile_poller` task when `self.config.no_battery` is `true`.
3. External telemetry injected via `WorkerConfig::with_mobile_telemetry` or
   `WorkerClient::mobile_telemetry().update(...)` continues to work unchanged, so tests that
   explicitly test low-battery or thermal gating can inject synthetic readings without racing the
   host battery poller.

`crates/core/src/capabilities.rs` does not read `RUSTY_GRID_NO_BATTERY` or any other battery
environment variable directly.

### D2: Single CLI environment variable (`RUSTY_GRID_NO_BATTERY`) and start-up warning

`resolve_no_battery` (`crates/cli/src/cli_app.rs`) calls
`resolve_bool(None, &["RUSTY_GRID_NO_BATTERY"], None, false)`. Neither `WorkerFileConfig` nor
`WorkerHardwareConfig` (`crates/cli/src/config.rs`) defines a `no_battery` field, so a
`rusty-grid.toml` file in the working directory or home directory cannot turn battery protection
off.

When `config.no_battery` is `true`:
- `WorkerClient::new` (`crates/worker/src/client.rs`) logs a `tracing::warn!` event stating
  that host battery and thermal protection is disabled.
- `run_worker` (`crates/cli/src/cli_app.rs`) prints
  `Warning: host battery and thermal protection is disabled (RUSTY_GRID_NO_BATTERY); ...` to
  `stderr`.

### D3: `WorkerClient::for_testing` vs `WorkerClient::from_options`

`WorkerClient::from_options` (`crates/worker/src/client.rs`) leaves `no_battery` at `false`.
`WorkerClient::for_testing` (`crates/worker/src/client.rs`) is documented as test-only and
sets `.with_no_battery(true)` and `enable_redirection_portal = false` for integration tests.

## Compatibility

No wire protocol, CLI flag, or config file schema changes for valid configurations. Unknown
`no_battery` keys in existing TOML files are ignored by serde while having no effect on worker
behaviour.

## Security

Disabling battery and thermal protection on a battery-powered device can drain the battery or run
tasks under thermal throttling. Restricting the override to `WorkerConfig::with_no_battery` in Rust
code and `RUSTY_GRID_NO_BATTERY` in the process environment prevents an untrusted or stale
`./rusty-grid.toml` in a working directory from silently disabling protection, and the start-up
warning makes the override visible in operator logs.

## Test plan

- Unit test `client::tests::test_worker_config_no_battery_disables_host_battery_probe`
  (`crates/worker/src/client.rs`) uses `with_test_battery_probe((Some(10), Some(false)), ...)`
  so that on any host (including headless CI without a battery), `WorkerConfig::new` and
  `WorkerClient::from_options` observe the 10 % critical battery while `with_no_battery(true)` and
  `WorkerClient::for_testing` suppress it.
- Unit test
  `cli_app::tests::test_resolve_no_battery_only_accepts_rusty_grid_no_battery_env_and_not_config_or_aliases`
  (`crates/cli/src/cli_app.rs`) verifies that `OXIDE_SWARM_NO_BATTERY`,
  `RUSTY_GRID_DISABLE_BATTERY`, and `no_battery = true` in `rusty-grid.toml` do not enable
  `resolve_no_battery()`, and that `RUSTY_GRID_NO_BATTERY=1` does.
- End-to-end test `test_sigterm_stops_the_worker_gracefully_and_takes_its_task_trees_with_it`
  (`tests/cli_worker_signals_e2e.rs`) asserts that a worker spawned with
  `RUSTY_GRID_NO_BATTERY=1` prints the start-up warning to its log.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TEST-2 | Isolate test suite from host battery and restrict CLI override to `RUSTY_GRID_NO_BATTERY` with start-up warning | `crates/core/src/capabilities.rs`, `crates/worker/src/client.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `README.md`, `docs/README.md`, `docs/design/0028-host-battery-probe-override.md` | All unit and integration tests pass with a simulated 10 % battery probe; only `RUSTY_GRID_NO_BATTERY` disables probing from the CLI, and a warning is logged at start-up | Implemented |

## Status log

- 2026-11-04: Implemented and verified against review feedback on pull request 76.
