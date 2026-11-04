# 0027: Honest `ox-mode` Workflow Mode Reporting and Engine Placement (`MODE-1`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Moves the `ox-mode` workflow-mode execution engine out of `crates/core` into `crates/cli/src/mode_cli.rs`
(leaving only the pure data types, static descriptors, and a runner callback hook in
`crates/core/src/mode.rs`), and updates every mode action to report only what actually happened:
1. `research distributed` and `research report` are explicitly labeled as an in-memory synthetic
   scheduling simulation (`simulation_only: true`) with generic synthetic node names and without
   printing `OPTIMAL` or invented hardware model names.
2. `research bench` benchmarks real `rusty_grid_core::protocol` Bincode wire-codec roundtrips
   (`serialize_message` + `deserialize_message` on `MasterMessage::AssignTask`) alongside `Task`
   JSON serde roundtrips instead of a `Vec` byte-copy loop.
3. `dev cluster` and `dev stop` return failure (`success: false`, `exit_code: Some(1)`) when invoked
   through the non-CLI engine/dashboard path (`execute_mode_action`), where background cluster process
   management cannot run.
4. `dev status` performs a direct HTTP/1.1 `GET /api/status` check, validates the HTTP status code
   (`200..300`) and JSON response shape (`workers` array and `tasks` object), accepts `--port` /
   `RUSTY_GRID_DASHBOARD_PORT`, and fails (exiting non-zero) when a protected master responds with
   HTTP `401` or `403`.
5. `doc export` generates `target/docs/OXIDESWARM_SPECIFICATION.md` from live
   `rusty_grid_core::protocol` constants (`PROTOCOL_VERSION_CURRENT`, `PROTOCOL_VERSION_MIN_COMPATIBLE`,
   `MAX_FRAME_SIZE` = 64 MiB), workspace crates parsed from `Cargo.toml`, and design records on disk
   (`docs/design/*.md`), replacing the static template that contained outdated claims.
6. `doc verify` checks relative markdown links (`[label](target)` outside code fences and inline code
   spans) in addition to file existence, heading counts, and balanced code fences.

## Context

Previously (`MODE-1` in `docs/BACKLOG.md`):
- `crates/core/src/mode.rs` embedded the full CLI workflow-mode execution engine, invoking
  `Command::new("cargo")` and `Command::new("curl")` inside the core protocol library crate.
- `run_distributed_benchmark` (`ox-mode research distributed`) ran an in-memory loop with invented
  hardware strings (`"NVIDIA RTX 4090"`, `"Snapdragon 8 Gen 2"`, `"mac-orchestrator"`,
  `"linux-worker-cpu"`, `"android-mobile-edge"`) and printed `[PASSED - OPTIMAL DISPATCH]`, while
  `generate_research_report` wrote `"status": "OPTIMAL"`. `launch_local_dev_cluster` also passed an
  invented `--gpu-name "NVIDIA RTX 4090 (Dev Sim)"`.
- `run_micro_benchmarks` (`ox-mode research bench`) measured a `Vec::extend_from_slice` loop over a
  fixed byte literal and reported it as wire-protocol codec throughput.
- `execute_mode_action` returned `success = true` with canned text for `(WorkflowMode::Dev, "cluster")`
  and `(WorkflowMode::Dev, "stop")` when called from the Master Web UI (`/api/modes/run`).
- `probe_cluster_health_with_root` (`ox-mode dev status`) ran `curl -s` without checking the HTTP
  status code; a token-protected master answering `401 Unauthorized` with a JSON error body was
  reported as `Cluster Healthy` with 0 workers.
- `export_specification` (`ox-mode doc export`) wrote a hardcoded template claiming a 16 MB frame
  limit, JSON-only payloads, and `/api/chat`.
- `verify_documentation` (`ox-mode doc verify`) advertised relative-link checking in its descriptor
  but only counted lines, `#` headings, and ` ``` ` fences.

## Decision

1. **Crate boundary (`crates/core/src/mode.rs` vs `crates/cli/src/mode_cli.rs`):**
   - `crates/core/src/mode.rs` retains only `WorkflowMode`, `ModeExecutionResult`, `ModeDescriptor`,
     `ModeActionDescriptor`, `get_mode_descriptors()`, `find_workspace_root()`, and a process-local
     runner hook (`set_mode_runner_hook` / `dispatch_mode_action`) used by `crates/master/src/web_ui.rs`
     when `oxideswarm master` is started by `rusty_grid_cli`. `crates/core` contains no
     `Command::new("cargo")`.
   - All mode execution functions live in `crates/cli/src/mode_cli.rs` (also re-exported as
     `rusty_grid_cli::mode`).
2. **Honest benchmark and simulation reporting (`research bench`, `research distributed`, `research report`):**
   - `run_micro_benchmarks` measures `Task` JSON serde roundtrips and `MasterMessage::AssignTask`
     Bincode wire-codec roundtrips (`serialize_message` + `deserialize_message`).
   - `run_distributed_benchmark` uses generic synthetic worker profiles (`sim-cpu-node-a`,
     `sim-cpu-node-b`, `sim-gpu-node`, `sim-mobile-node`), prints
     `=== In-Memory Synthetic Scheduling Simulation (not a live cluster) ===` and
     `Status: [COMPLETED - SYNTHETIC SIMULATION]`, and sets metric `"simulation_only": true`.
   - `generate_research_report` records `"status": "completed"` / `"failed"` and `"simulation_only": true`.
3. **Failure on non-runnable actions (`dev cluster`, `dev stop`):**
   - `execute_mode_action` returns `success = false` (`exit_code = Some(1)`) for `dev cluster` and
     `dev stop`, directing the caller to `ox-mode dev cluster` / `ox-mode dev stop`.
4. **HTTP status verification in `dev status` (`--port` / `RUSTY_GRID_DASHBOARD_PORT`):**
   - `probe_cluster_health_with_root` performs an HTTP/1.1 `GET /api/status` request over `TcpStream`,
     parses the status line, requires HTTP `200..300` and a JSON body containing `"workers"` (array)
     and `"tasks"` (object), and fails with `exit_code = Some(1)` and metric `http_status` when a
     master returns `401`, `403`, or another error status.
5. **Generated `doc export` and relative-link checking in `doc verify`:**
   - `export_specification` generates `target/docs/OXIDESWARM_SPECIFICATION.md` from live
     `rusty_grid_core::protocol` constants, `Cargo.toml` workspace members, and `docs/design/*.md`
     headings.
   - `verify_documentation` parses `[label](target)` links outside fenced code blocks and inline code
     spans, resolves relative file paths against each document's directory, and fails when any target
     is missing.
