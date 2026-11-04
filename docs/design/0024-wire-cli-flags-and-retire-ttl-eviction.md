# 0024: Wire CLI flags and retire dead TTL eviction

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Wire `--max-queue-size` (`RUSTY_GRID_MAX_QUEUE_SIZE`, `[master].max_queue_size`), `--scheduler-policy` (`RUSTY_GRID_SCHEDULER_POLICY`, `[master].scheduler_policy`), `--wire-codec` (`RUSTY_GRID_WIRE_CODEC`, `[master].wire_codec`, `[worker].wire_codec`), and `[submit].timeout_secs` in `crates/cli/src/cli_app.rs` and `crates/master`, ensure an explicit `--master` on `oxideswarm worker` takes precedence over `[worker].p2p_ticket`, and delete the unreachable `terminal_task_ttl` / `prune_terminal_older_than` TTL eviction code from `crates/master/src/reaper.rs` and `crates/master/src/queue.rs`.

## Context

Before this change (`SCHED-2` in `docs/BACKLOG.md`):

| Area | Behaviour before this change | Code |
|---|---|---|
| `--max-queue-size` | Parsed into `MasterArgs::max_queue_size` and `MasterFileConfig::max_queue_size`, but never read in `run_master`. Both `MasterServer::bind` and `MasterServer::spawn_with_config` passed `None` as the task-count cap to `TaskQueue::with_retention_limits`, leaving `GridError::QueueFull` unreachable outside unit tests. | `crates/cli/src/cli_app.rs`, `crates/master/src/server.rs` |
| `--scheduler-policy` | Parsed into `MasterArgs::scheduler_policy` and `MasterFileConfig::scheduler_policy`, but never read in `run_master`, which always constructed `SchedulerConfig` with `SchedulingPolicy::WeightedLeastLoaded`. | `crates/cli/src/cli_app.rs`, `crates/master/src/scheduler.rs` |
| `--wire-codec` | Declared on `MasterArgs` and `WorkerArgs` as `String` with `#[arg(long, default_value = "bincode")]` and passed into `resolve_string(Some(args.wire_codec), ...)`. Because the CLI tier was always `Some("bincode")` when `--wire-codec` was omitted, `RUSTY_GRID_WIRE_CODEC` and config `wire_codec` could never take effect, and invalid codec values fell back to `WireCodec::Bincode` via `.unwrap_or_default()`. Furthermore, `handle_connection` and `handle_multiplexed_connection` in `crates/master/src/server.rs` unconditionally overwrote `transport.set_outbound_codec(detected_codec)` with the client's inbound codec even when the master was configured with `WireCodec::Json`. | `crates/cli/src/cli_app.rs`, `crates/cli/src/connect.rs`, `crates/master/src/server.rs` |
| `[submit].timeout_secs` | Parsed into `SubmitFileConfig::timeout_secs` (`crates/cli/src/config.rs`), but `SubmitArgs::timeout_secs` and `MapReduceArgs::timeout_secs` used `#[arg(long, default_value = "60")]` (`u64`) and never consulted `submit_cfg`. | `crates/cli/src/cli_app.rs` |
| Worker `--master` vs config `p2p_ticket` | `run_worker` resolved `master_addr` and `p2p_ticket` independently. When `~/.oxideswarm/rusty-grid.toml` held a stale `[worker] p2p_ticket`, passing `--master <HOST>:8088` on the command line still loaded `p2p_ticket = Some(...)` from the config file, causing `WorkerClient` to dial the stale ticket instead of `--master`. | `crates/cli/src/cli_app.rs` |
| Terminal task TTL pruning | `ReaperConfig::terminal_task_ttl` defaulted to `None` with no CLI flag, environment variable, or config field to enable it, leaving `TaskQueue::prune_terminal_older_than` and `TaskEntry::finished_instant` dead in production while finished-task retention is bounded by count (`--max-retained-tasks`) and bytes (`--max-retained-mb`). | `crates/master/src/reaper.rs`, `crates/master/src/queue.rs` |

## Goals

- **G1** Wire `--max-queue-size`, `RUSTY_GRID_MAX_QUEUE_SIZE`, and `[master].max_queue_size` into `ServerConfig::max_queue_size` and `TaskQueue::with_retention_limits`, refusing `0` at startup.
- **G2** Wire `--scheduler-policy`, `RUSTY_GRID_SCHEDULER_POLICY`, and `[master].scheduler_policy` into `SchedulerConfig::policy`, refusing unknown policy names at startup.
- **G3** Make `--wire-codec` optional on `MasterArgs` and `WorkerArgs` so `RUSTY_GRID_WIRE_CODEC` and `[master]` / `[worker]` `wire_codec` take effect according to standard precedence (`CLI > Env > Config > Default`), fail with a non-zero exit code on invalid codec names, and honor `RUSTY_GRID_WIRE_CODEC` in `connect_master`.
- **G4** Wire `[submit].timeout_secs` as the config-file fallback for `oxideswarm submit` and `oxideswarm mapreduce` when `--timeout-secs` is not passed on the command line.
- **G5** Ensure an explicit `--master` (or `RUSTY_GRID_MASTER_ADDR` / `RUSTY_GRID_MASTER`) on `oxideswarm worker` clears a lower-precedence `[worker].p2p_ticket` from the config file.
- **G6** Delete the unused `terminal_task_ttl`, `prune_terminal_older_than`, and `finished_instant` code from `crates/master/src/reaper.rs` and `crates/master/src/queue.rs`.

## Non-goals

- Adding a new time-based retention flag for finished tasks (count-based `--max-retained-tasks` and byte-based `--max-retained-mb` already bound terminal task memory).
- Changing the wire framing or adding new scheduling policies beyond `WeightedLeastLoaded`, `LeastLoaded`, and `RoundRobin`.

## Design

### D1 Active task-count limit (`--max-queue-size`)

- `ServerConfig` (`crates/master/src/server.rs`) gains `pub max_queue_size: Option<usize>` (default `None`) and builder `ServerConfig::with_max_queue_size(mut self, max: Option<usize>) -> Self`.
- Both `MasterServer::bind` and `MasterServer::spawn_with_config` pass `config.max_queue_size` as the `max_size` argument to `TaskQueue::with_retention_limits`.
- In `crates/cli/src/cli_app.rs`, `apply_max_queue_size` resolves `--max-queue-size` > `RUSTY_GRID_MAX_QUEUE_SIZE` > `[master].max_queue_size`. Passing `0` returns an error (`"--max-queue-size / max_queue_size must be at least 1 (got 0)"`) and `run_master` exits with `ExitCode::FAILURE`.
- When queued or running (non-terminal) tasks in `TaskQueue` reach `max_queue_size`, `TaskQueue::submit` returns `GridError::QueueFull { capacity }`, formatted as `"Task queue is full ({capacity} queued or running tasks; wait, or raise --max-queue-size)"`, and `oxideswarm submit` prints the error and exits `1` (`ExitCode::FAILURE`).

### D2 Scheduler policy selection (`--scheduler-policy`)

- `SchedulingPolicy` (`crates/master/src/scheduler.rs`) implements `Display` and `FromStr`, accepting `"weighted"` / `"weighted-least-loaded"`, `"least-loaded"` / `"leastloaded"`, and `"round-robin"` / `"roundrobin"` / `"rr"` (case-insensitive, with `_` normalized to `-`).
- `WorkloadScheduler` (`crates/master/src/scheduler.rs`) persists `rr_cursor: AtomicUsize` across scheduling passes and sorts schedulable workers by `worker_id` in `schedule_batch`, so `SchedulingPolicy::RoundRobin` rotates evenly from one `schedule_once` / `schedule_batch` pass to the next rather than resetting to `0` on each pass.
- `resolve_scheduler_policy` and `build_master_scheduler_config` in `crates/cli/src/cli_app.rs` resolve `--scheduler-policy` > `RUSTY_GRID_SCHEDULER_POLICY` > `[master].scheduler_policy` (defaulting to `SchedulingPolicy::WeightedLeastLoaded`) via `resolve_sourced_string` (`crates/cli/src/config.rs`) and construct `SchedulerConfig { policy: scheduler_policy, ... }` for `run_master`. Unknown values prefix the error with the source that supplied the invalid string (`--scheduler-policy: ...`, `RUSTY_GRID_SCHEDULER_POLICY: ...`, or `scheduler_policy: ...`) and exit with `ExitCode::FAILURE`.

### D3 Wire codec resolution and outbound master codec (`--wire-codec`, `RUSTY_GRID_WIRE_CODEC`)

- `MasterArgs::wire_codec` and `WorkerArgs::wire_codec` (`crates/cli/src/cli_app.rs`) are changed from `String` (`default_value = "bincode"`) to `Option<String>`.
- `resolve_wire_codec` resolves `--wire-codec` > `RUSTY_GRID_WIRE_CODEC` > `[master|worker].wire_codec` via `resolve_sourced_string` (`crates/cli/src/config.rs`) and parses the value via `WireCodec::from_str` (`crates/core/src/protocol.rs`, which trims whitespace before matching `"bincode"` / `"bin"` / `"binary"` or `"json"`), defaulting to `WireCodec::Bincode` when unset and exiting with `ExitCode::FAILURE` with a source-prefixed error (`--wire-codec: ...`, `RUSTY_GRID_WIRE_CODEC: ...`, or `wire_codec: ...`) on invalid values.
- In `run_master`, `--wire-codec` is validated alongside `--max-queue-size` and `--scheduler-policy` before `decide_master_secret`, so an invalid `--wire-codec` on a non-loopback or `--p2p` master exits `1` without creating `~/.oxideswarm/cluster.secret` or printing secret bootstrap instructions.
- In `handle_connection` and `handle_multiplexed_connection` (`crates/master/src/server.rs`), both the control transport (`transport`) and each subsequent P2P data/blob bi-stream (`data_transport`) mirror the peer's `detected_codec` only when `wire_codec == WireCodec::Bincode` (the default); when the master is explicitly configured with `WireCodec::Json`, its outbound control and data transports keep `WireCodec::Json`.
- `connect_master` (`crates/cli/src/connect.rs`) validates `RUSTY_GRID_WIRE_CODEC` via `resolve_client_wire_codec()` before connecting, returning `GridError::Config("RUSTY_GRID_WIRE_CODEC: ...")` (so client subcommands exit `1` on invalid values) and constructing the client `MessageTransport` with the resolved codec.

### D4 Config-file fallback for submission timeout (`[submit].timeout_secs`)

- `SubmitArgs::timeout_secs` and `MapReduceArgs::timeout_secs` (`crates/cli/src/cli_app.rs`) are changed from `u64` to `Option<u64>`.
- `run_submit_with_secret` and `run_mapreduce` resolve `args.timeout_secs` > `submit_cfg.timeout_secs` > `60` (clamped to `.max(1)`).

### D5 Worker target precedence (`resolve_worker_target`)

- `resolve_worker_target` (`crates/cli/src/cli_app.rs`) resolves `(master_addr, p2p_ticket)` across the CLI, environment, and `[worker]` config tiers together:
  1. If `--p2p-ticket` is passed on the CLI, `p2p_ticket` is used.
  2. Otherwise, if `--master` is passed on the CLI, `p2p_ticket` is `None` (ignoring `RUSTY_GRID_P2P_TICKET` and `[worker].p2p_ticket`).
  3. Otherwise, if `RUSTY_GRID_P2P_TICKET` is set in the environment, it is used.
  4. Otherwise, if `RUSTY_GRID_MASTER_ADDR` or `RUSTY_GRID_MASTER` is set in the environment, `p2p_ticket` is `None` (ignoring `[worker].p2p_ticket`).
  5. Otherwise, `[worker].p2p_ticket` from the config file is used if present.

### D6 Removal of dead TTL eviction code

- Removed `ReaperConfig::terminal_task_ttl`, `ReaperConfig::with_terminal_task_ttl`, and the reaper loop calls to `queue.prune_terminal_older_than` in `crates/master/src/reaper.rs`.
- Removed `TaskEntry::finished_instant`, `TaskQueueInner::prune_terminal_older_than`, and `TaskQueue::prune_terminal_older_than` in `crates/master/src/queue.rs`.

## Compatibility

- Wire format: unchanged. Both `WireCodec::Bincode` (`0x02`) and `WireCodec::Json` (`0x01`) are existing frame formats (`crates/core/src/protocol.rs`), and receivers decode each frame using its 1-byte format discriminator regardless of the outbound codec setting.
- CLI and config: existing valid invocations and config files continue to work. Config files or environment variables that previously set `max_queue_size`, `scheduler_policy`, `wire_codec`, or `[submit].timeout_secs` now take effect instead of being ignored. Invalid values for `--max-queue-size 0`, `--scheduler-policy`, `--wire-codec`, or `RUSTY_GRID_WIRE_CODEC` (on master, worker, or client commands) now fail fast with exit code `1` (`ExitCode::FAILURE`).

## Security

- Bounding active task count via `--max-queue-size` complements the 256 MiB active byte cap (`--max-active-mb`, [design 0008](0008-oversized-task-results-and-active-queue-budget.md)), letting operators cap the number of small tasks held in memory at once. Refusing `--max-queue-size 0` at startup prevents accidental denial of service where every submission is rejected.
- Failing closed on unknown `--wire-codec` and `--scheduler-policy` values (before generating a default `cluster.secret`) prevents silent misconfiguration and unwanted side effects.

## Test plan

- Unit and in-process integration test `cli_app::tests::test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket` in `crates/cli/src/cli_app.rs`:
  - verifies `apply_max_queue_size` (including rejection of `0`), `resolve_scheduler_policy`, `build_master_scheduler_config` (confirming `SchedulerConfig.policy` carries the parsed policy), `resolve_wire_codec` (confirming `--wire-codec` beats `RUSTY_GRID_WIRE_CODEC` and config `wire_codec`), and `resolve_worker_target` (confirming `--master` overrides `[worker].p2p_ticket`);
  - verifies `run_master`, `run_worker`, and `connect_master` fail immediately on `--max-queue-size 0`, `--scheduler-policy not-a-policy`, `--wire-codec xml` (leaving a fresh `OXIDESWARM_HOME` empty even with `--listen 0.0.0.0:0`), `--wire-codec protobuf`, and `RUSTY_GRID_WIRE_CODEC=xml`;
  - starts `run_master` with `--max-queue-size 2`, `--scheduler-policy round-robin`, and `RUSTY_GRID_WIRE_CODEC=json`, clears `RUSTY_GRID_WIRE_CODEC` before connecting so the client sends `WireCodec::Bincode` (`0x02`) and asserts the master encodes its `ClusterStatus` reply with `WireCodec::Json` (`0x01`), verifies the first two `run_submit_with_secret` calls succeed while the third exits `1` with `"Task queue is full (2 queued or running tasks; wait, or raise --max-queue-size)"`, and verifies `[submit].timeout_secs = 42` sets `timeout_secs == 42` on the wire for both `oxideswarm submit` and `oxideswarm mapreduce` when `--timeout-secs` is omitted and yields to `15` when `--timeout-secs 15` is passed.
- Unit tests `scheduler::tests::test_round_robin_rotates_across_schedule_once_passes` and `server::tests::test_master_server_bind_enforces_max_queue_size_and_json_wire_codec` in `crates/master`.
- Existing CLI and master scheduling suites (`cargo test -p rusty_grid_cli --lib`, `cargo test -p rusty_grid_master --lib`, `cargo test -p rusty_grid_cli --test test_bug2_cli_flags --test test_challenger_m4_cli_adversarial`, `cargo test --test master_limits_and_scheduling`).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `SCHED-2` | Wire `--max-queue-size`, `--scheduler-policy`, `--wire-codec`, `[submit].timeout_secs`, and worker `--master` precedence; delete dead TTL eviction | `crates/cli/src/cli_app.rs`, `crates/cli/src/connect.rs`, `crates/core/src/protocol.rs`, `crates/master/src/queue.rs`, `crates/master/src/reaper.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/server.rs`, `README.md`, `CLAUDE.md` | `--max-queue-size 2` rejects the third submit in a test, and `RUSTY_GRID_WIRE_CODEC=json` takes effect in a test | Implemented |

## Status log

- 2026-11-04: Wired `--max-queue-size`, `--scheduler-policy`, `--wire-codec`, `[submit].timeout_secs`, and `resolve_worker_target`, removed dead `terminal_task_ttl` / `prune_terminal_older_than` TTL eviction code, updated `README.md` and `CLAUDE.md`, and verified with `test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket` (`SCHED-2`).
