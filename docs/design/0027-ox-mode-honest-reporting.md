# 0027: Honest `ox-mode` Workflow Mode Reporting and Engine Placement (`MODE-1`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Move the `ox-mode` workflow-mode execution engine out of `crates/core` into `crates/cli/src/mode_cli.rs` (keeping only pure descriptors and a runner hook in `crates/core/src/mode.rs`) and make every mode action report only what ran:
- `research distributed` and `research report` label the in-memory synthetic scheduling simulation (`simulation_only: true`), use generic synthetic node names (`sim-cpu-node-a`, `sim-cpu-node-b`, `sim-gpu-node`, `sim-mobile-node`), and fail if writing `target/research_report.json` fails;
- `research bench` benchmarks real `rusty_grid_core::protocol` Bincode wire-codec roundtrips (`serialize_message` + `deserialize_message` on `MasterMessage::AssignTask`) alongside `Task` JSON serde roundtrips;
- `dev cluster` and `dev stop` fail in non-CLI contexts (`execute_mode_action`), `dev cluster` verifies that spawned children stay alive and both workers register on `/api/status`, and `dev status` checks HTTP status codes, probes only `--port` when explicitly passed, and derives the dashboard Bearer token from the cluster secret;
- `test quick` sums `passed;` and `failed;` counts across every `test result:` line, `doc build` checks `target/doc/rusty_grid_core/index.html`, `doc export` generates `target/docs/OXIDESWARM_SPECIFICATION.md` from live protocol constants and workspace metadata, and `doc verify` checks relative markdown links.

## Context

Behaviour before this change (`MODE-1` in `docs/BACKLOG.md`):

| Area | Behaviour | Code |
|---|---|---|
| Crate boundary | `crates/core/src/mode.rs` embedded the full CLI workflow-mode execution engine, invoking `Command::new("cargo")` and `Command::new("curl")` inside the core protocol library crate. | `crates/core/src/mode.rs` |
| Synthetic simulation & benchmarks | `run_distributed_benchmark` (`ox-mode research distributed`) ran an in-memory loop with invented hardware strings (`"NVIDIA RTX 4090"`, `"Snapdragon 8 Gen 2"`, `"mac-orchestrator"`, `"linux-worker-cpu"`, `"android-mobile-edge"`, `"worker-gpu-rtx4090"`) and printed `[PASSED - OPTIMAL DISPATCH]`, while `generate_research_report` wrote `"status": "OPTIMAL"` and ignored `fs::create_dir_all` / `fs::write` errors. `run_micro_benchmarks` (`ox-mode research bench`) timed a `Vec::extend_from_slice` byte-copy loop and reported it as wire-protocol codec throughput. | `run_distributed_benchmark`, `generate_research_report`, `run_micro_benchmarks` |
| Cluster lifecycle & status | `execute_mode_action` returned `success = true` with canned text for `(WorkflowMode::Dev, "cluster")` and `(WorkflowMode::Dev, "stop")` when called via the Master Web UI (`/api/modes/run`). `launch_local_dev_cluster` reported `success = true` after fixed sleeps without checking `try_wait()` or polling `/api/status` for worker registration. `probe_cluster_health` ignored HTTP `401`/`403` errors, fell back to `.dev_cluster.json` and default ports (`8080`, `3000`, `8088`) even when `--port` was explicitly passed, and did not derive the dashboard Bearer token from the cluster secret. | `execute_mode_action`, `launch_local_dev_cluster`, `probe_cluster_health` |
| Test & documentation actions | `run_quick_tests` parsed only the first `test result:` line of `cargo test --workspace --lib`. `run_doc_build` did not verify that `target/doc/rusty_grid_core/index.html` was generated. `export_specification` wrote a static template claiming a 16 MB frame limit and JSON-only payloads. `verify_documentation` did not check relative markdown links. | `run_quick_tests`, `run_doc_build`, `export_specification`, `verify_documentation` |

## Goals

- **G1** Remove all `cargo` and external process execution from `crates/core` by moving the workflow-mode execution engine to `crates/cli/src/mode_cli.rs` behind a callback hook registered by `rusty_grid_cli`.
- **G2** Ensure `research bench`, `research distributed`, and `research report` measure real Bincode wire-codec roundtrips, clearly label synthetic scheduling simulations without invented hardware or `OPTIMAL` claims, and propagate file I/O errors.
- **G3** Make `dev cluster`, `dev stop`, and `dev status` report real process and HTTP state, honour an explicit `--port` without fallback, and authenticate with the dashboard Bearer token derived from the resolved cluster secret.
- **G4** Make `test quick` sum all `test result:` summary lines, `doc build` verify `target/doc/rusty_grid_core/index.html`, `doc export` generate `target/docs/OXIDESWARM_SPECIFICATION.md` from live constants and workspace crates, and `doc verify` validate relative markdown links.

## Non-goals

- Changing the wire protocol or dashboard `/api/modes` and `/api/modes/run` JSON schema (`ModeDescriptor`, `ModeExecutionResult`).
- Launching or stopping external OS processes from inside the Master Web UI handler (`/api/modes/run`).

## Design

### D1 Move workflow-mode engine to `crates/cli/src/mode_cli.rs` with a runner hook in `crates/core/src/mode.rs`

`crates/core/src/mode.rs` retains only `WorkflowMode`, `ModeExecutionResult`, `ModeDescriptor`, `ModeActionDescriptor`, `get_mode_descriptors()`, `find_workspace_root()`, and a process-local runner hook (`set_mode_runner_hook` / `dispatch_mode_action`). When `rusty_grid_cli` starts (`run_cli_with` in `crates/cli/src/cli_app.rs` and `run_mode_cli` in `crates/cli/src/mode_cli.rs`), it registers `execute_mode_action` via `set_mode_runner_hook`. If `dispatch_mode_action` is called in a process where no hook is installed, it returns `ModeExecutionResult` with `success = false` and `exit_code = Some(1)`.

### D2 Honest `research bench`, `research distributed`, and `research report`

- `run_micro_benchmarks` measures `Task` JSON serde roundtrips and `MasterMessage::AssignTask` Bincode wire-codec roundtrips (`serialize_message` + `deserialize_message` with `WireCodec::Bincode`), recording `serde_ops_per_sec`, `codec_ops_per_sec`, `codec_frame_bytes`, `codec_wire_discriminator` (`0x02`), and `codec_decoded_task_id`.
- `run_distributed_benchmark` uses generic synthetic worker profiles (`sim-cpu-node-a`, `sim-cpu-node-b`, `sim-gpu-node`, `sim-mobile-node`), prints `=== In-Memory Synthetic Scheduling Simulation (not a live cluster) ===` and `Status: [COMPLETED - SYNTHETIC SIMULATION]`, and sets metric `"simulation_only": true`.
- `generate_research_report` checks both `fs::create_dir_all` and `fs::write` on `target/research_report.json` and returns `success = false` (`exit_code = Some(1)`) on any I/O error.

### D3 Fail non-CLI `dev cluster` / `dev stop`, verify `--dashboard-port-file` and child readiness in `launch_local_dev_cluster`, and verify PID ownership in `stop_local_dev_cluster`

- `execute_mode_action` returns `success = false` (`exit_code = Some(1)`) for `(WorkflowMode::Dev, "cluster")` and `(WorkflowMode::Dev, "stop")`, and the Launch/Stop Cluster buttons are removed from `crates/master/src/dashboard.html`.
- `launch_local_dev_cluster` (and `launch_local_dev_cluster_with_ports`) checks the result of its pre-clean `stop_local_dev_cluster` when `.dev_cluster.pids` or `.dev_cluster.json` exists, starts the master with `--dashboard-port-file <workspace_root>/.dev_cluster.dashboard_port`, checks worker spawn errors, records each spawned child's `sysinfo` start time in `"pid_start_times"` inside `.dev_cluster.json`, calls `try_wait()` on the master and both worker children (`dev-worker-cpu-1`, `dev-worker-sim-gpu-2`), and waits for `.dev_cluster.dashboard_port` to confirm the master bound `web_ui_port` before polling `/api/status` or sending any Bearer token. If `.dev_cluster.dashboard_port` does not appear within 8 s, it reports that the dashboard port file did not appear within 8 s rather than guessing a cause. On any child exit, missing dashboard port file, or readiness timeout, it kills and reaps the spawned `Child` handles directly, removes `.dev_cluster.pids`, `.dev_cluster.json`, and `.dev_cluster.dashboard_port`, and returns `success = false` (`exit_code = Some(1)`).
- `stop_local_dev_cluster` refuses `pid <= 1` and `ox-mode`'s own PID, parses JSON `"master_pid"` with `u32::try_from` (never truncating `u64` via `as u32`), verifies via `sysinfo` that each target process executable name is `rusty-grid` / `oxideswarm` and matches the recorded `"pid_start_times"` entry (a start-time mismatch always skips the PID, even if its command line matches; when no start time was recorded, it falls back to this workspace's command-line signature / tracking file timestamp), signals verified processes, reaps in-process `Child` handles via `try_wait()`/`wait()` and polls the existing `System` on Windows (`refresh_existing_process` / `GetExitCodeProcess`) or a fresh `System` plus `ps -o stat=` on Unix up to 3 s, counts any verified process that could not be signalled as failed (`signal_failed_count`), and preserves `.dev_cluster.pids` and `.dev_cluster.json` whenever `stop` fails. `.dev_cluster.*` and `.dev_cluster_home/` are gitignored in `.gitignore`.

### D4 Strict `--port` probing and cluster-secret Bearer auth in `dev status`

- `probe_cluster_health(port: Option<u16>)`, `probe_cluster_health_with_root(port: Option<u16>, workspace_root: &Path)`, and `probe_cluster_health_with_secret(port: Option<u16>, workspace_root: &Path, secret: Option<&ClusterSecret>)` keep `port` as `Option<u16>`:
  - when `Some(p)` is passed (`ox-mode dev status --port <P>` or `RUSTY_GRID_DASHBOARD_PORT=<P>`), only port `p` is probed (no fallback to `.dev_cluster.json` or default ports);
  - when `None` is passed, `dev status` checks `.dev_cluster.json` (`web_ui_port`) and falls back only to HTTP dashboard default `8080` (dropping port `3000` and excluding grid TCP port `8088`).
- `resolve_dashboard_bearer_token()` resolves the cluster secret via `load_config_file(None)` and `SecretOpts::default().resolve(config.as_ref(), section)` across `[Status, Master, Submit, Worker]` and derives the dashboard admin Bearer token via `rusty_grid_master::dashboard_admin_token(&secret)`. On `GET /api/status`, the initial request is always sent without an `Authorization` header; `Authorization: Bearer <token>` is sent only on retry to an explicit `--port` or `.dev_cluster.json` port after receiving HTTP `401` with `WWW-Authenticate: Bearer realm="OxideSwarm"`. HTTP `401`/`403` or non-2xx responses set `success = false` and `exit_code = Some(1)`.

### D5 Summed test counts in `test quick`, verified `test lint` / `test flaky`, and checked `doc build`

- `parse_cargo_test_counts(stdout: &str) -> (usize, usize)` sums `passed;` and `failed;` across every `test result:` line emitted by `cargo test --workspace --lib`.
- `evaluate_lint_outcomes` and `run_flaky_detection_with` require every step/iteration to succeed (`clippy_ok && fmt_ok` and `passed_runs == iterations`).
- `finalize_doc_build_output` (and `resolve_cargo_target_dir`) verifies that `<CARGO_TARGET_DIR or target>/doc/rusty_grid_core/index.html` exists after `cargo doc --workspace --no-deps`; if missing, `run_doc_build` fails with `exit_code = Some(1)`.

### D6 Generated `doc export` and relative-link checking in `doc verify`

- `export_specification` generates `target/docs/OXIDESWARM_SPECIFICATION.md` from live `rusty_grid_core::protocol` constants (`PROTOCOL_VERSION_CURRENT`, `PROTOCOL_VERSION_MIN_COMPATIBLE`, `MAX_FRAME_SIZE`), workspace members parsed from `Cargo.toml`, and `docs/design/*.md` headings.
- `verify_documentation` parses markdown links outside fenced code blocks and inline code spans, resolves relative file paths against each document's directory, and fails (`exit_code = Some(1)`) when any target is missing.

## Compatibility

- CLI flags and JSON output fields of `ox-mode` and `oxideswarm mode` remain backward-compatible; `probe_cluster_health` and `probe_cluster_health_with_root` take `port: Option<u16>` so callers can distinguish an explicit `--port` from default port discovery.
- Wire protocol is unchanged (`PROTOCOL_VERSION_CURRENT = 2`).

## Security

- `ox-mode dev status` never sends the raw cluster secret over HTTP; it derives the domain-separated dashboard Bearer token via `rusty_grid_master::dashboard_admin_token(&secret)` using `HMAC-SHA256(secret, "oxs1-dashboard-admin")` (`crates/master/src/auth.rs:12-19`).
- To prevent leaking the Admin dashboard token to unverified or guessed local ports:
  1. Port `3000` is dropped from default candidate probing (`dev status` without `--port` checks only `.dev_cluster.json` `web_ui_port`, then `DEFAULT_FALLBACK_STATUS_PORTS = &[8080]`).
  2. The Bearer token is never allowed on unconfigured fallback ports; it is permitted only when probing an explicit `--port` or the port recorded in `.dev_cluster.json`.
  3. `http_get_local_status` always sends its first `GET /api/status` request without an `Authorization` header and only retries with `Authorization: Bearer <token>` if the server responds `401 Unauthorized` with a `WWW-Authenticate` header containing `Bearer realm="OxideSwarm"`.
  4. `launch_local_dev_cluster_with_ports` passes `--dashboard-port-file` to the spawned master and never polls `http_get_local_status(web_ui_port, ...)` or sends the Admin Bearer token until `.dev_cluster.dashboard_port` confirms that our newly spawned master bound `web_ui_port` (preventing token disclosure when another process already holds `web_ui_port` and the master continues RPC without a Web UI per `crates/master/src/server.rs:714-721`).
- To prevent `ox-mode dev stop` (or `dev cluster` pre-clean) from killing arbitrary processes via stale or reused PIDs in `.dev_cluster.pids` / `.dev_cluster.json`, `stop_local_dev_cluster` refuses `pid <= 1` and `std::process::id()`, checks executable identity (`rusty-grid` / `oxideswarm`) and process start time against the tracking file timestamp via `sysinfo`, and skips any non-matching or stale entry.
- Token-protected masters that return `401 Unauthorized` or `403 Forbidden` when no matching credential is available are reported as unhealthy (`exit_code = 1`) rather than healthy with 0 workers.

## Test plan

- **Unit (`crates/core/src/mode.rs`):** `test_dispatch_mode_action_without_hook_fails` verifies that `dispatch_mode_action` returns `success = false` (`exit_code = Some(1)`) when no runner hook is installed.
- **Unit (`crates/cli/src/cli_app.rs`):** `test_run_cli_registers_mode_runner_hook` verifies that `run_cli_with` registers the `mode_cli` hook so `dispatch_mode_action` executes through `mode_cli`.
- **Unit (`crates/cli/src/mode_cli.rs`):**
  - `test_parse_cargo_test_counts_two_passed_one_failed` verifies multi-crate `test result:` summation (`2` passed, `1` failed);
  - `test_lint_and_flaky_fail_when_only_part_passes` verifies `evaluate_lint_outcomes` and `run_flaky_detection_with` fail when only part passes;
  - `test_research_report_and_doc_build_fail_on_missing_or_unwritable_output` verifies `generate_research_report` fails when `target` is a regular file and `finalize_doc_build_output` fails when `target/doc/rusty_grid_core/index.html` is absent;
  - `test_micro_benchmarks_execution` and `test_distributed_simulation_is_honestly_labeled` verify Bincode wire-codec roundtrip metrics and synthetic node names.
- **Integration (`tests/workflow_modes_test.rs`):**
  - `test_dev_status_against_protected_master_exits_nonzero`;
  - `test_dev_status_closed_port_fails_while_another_master_is_up`;
  - `test_dev_status_succeeds_against_cluster_with_secret`;
  - `test_dev_status_never_sends_token_to_guessed_ports_or_3000`;
  - `test_dev_cluster_never_sends_token_when_web_ui_port_is_held`;
  - `test_dev_stop_skips_unrelated_and_stale_pids_and_reports_honestly`;
  - `test_dev_cluster_in_process_launch_then_stop_and_signal_failure_handling`;
  - `test_dev_cluster_cross_process_launch_then_stop_via_ox_mode_cli`;
  - `test_dev_cluster_fails_when_port_is_held`;
  - `test_doc_mode_verify_and_export`, `test_research_mode_profile_and_bench`, `test_research_mode_distributed_and_report`, `test_unknown_and_non_runnable_actions_fail`, and `test_core_has_no_cargo_invocation`.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p rusty_grid_core --lib --locked`, `cargo test -p rusty_grid_cli --lib --locked`, `cargo test -p rusty_grid_cli --test workflow_modes_test --locked`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Move mode execution engine from `crates/core` to `crates/cli/src/mode_cli.rs` with runner hook | `crates/core/src/mode.rs`, `crates/cli/src/mode_cli.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/Cargo.toml` | `crates/core` has no `Command::new("cargo")`, `dispatch_mode_action` fails without a hook, and `run_cli_with` registers the hook | Implemented |
| W2 | Honest `research bench`, `research distributed`, and `research report` | `crates/cli/src/mode_cli.rs`, `tests/workflow_modes_test.rs` | `research bench` benchmarks `WireCodec::Bincode` roundtrips, `research distributed` uses synthetic node profiles without `OPTIMAL` or `RTX 4090`, and `research report` fails on write errors | Implemented |
| W3 | Verified `dev cluster`, `dev stop`, and `dev status` (`--port` + secret Bearer token) | `crates/cli/src/mode_cli.rs`, `crates/master/src/dashboard.html`, `tests/workflow_modes_test.rs` | `dev cluster` checks `try_wait()` and `/api/status` worker registration, `dev status --port` probes only the given port and authenticates with `dashboard_admin_token(&secret)`, and dashboard Launch/Stop Cluster buttons are removed | Implemented |
| W4 | Multi-crate `test quick` summation, `doc build` index check, generated `doc export`, and `doc verify` relative links | `crates/cli/src/mode_cli.rs`, `tests/workflow_modes_test.rs`, `docs/design/0027-ox-mode-honest-reporting.md`, `docs/README.md`, `README.md` | All unit and integration tests in `crates/cli` and `tests/workflow_modes_test.rs` pass | Implemented |

## Status log

- 2026-11-04: Implemented `MODE-1` across `crates/core/src/mode.rs`, `crates/cli/src/mode_cli.rs`, `crates/cli/src/cli_app.rs`, `crates/master/src/dashboard.html`, and `tests/workflow_modes_test.rs`, and documented the design in 0027.
