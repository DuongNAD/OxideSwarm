# 0104: Remote Master Bridge and Full `grid_submit` Parity (`MESH-3`)

Status: Implemented
Created: 2026-11-22    Updated: 2026-11-22

## Summary

`AgentGridBridge` (`crates/agent_mesh/src/bridge.rs`) is decoupled from a concrete in-process `MasterHandle` behind an object-safe async `GridBackend` trait (`crates/agent_mesh/src/grid_backend.rs`) with two implementations: `InProcessBackend` (`--grid-master`) and `RemoteMasterBackend` (`--grid-connect <HOST:PORT>`, `crates/agent_mesh/src/remote_backend.rs`). `agent-mesh hub --grid-connect` attaches to an already-running OxideSwarm Master over `rusty_grid_core` TCP/P2P transport and OXS-AUTH v2/v3 without creating a cyclic dependency with `rusty_grid_cli`, while `grid_submit` (and all bridged task handlers) gain caller tag preservation, placement constraints, strict inline `files` staging, `outputs`/`artifacts` harvesting, `grid_result`, and `grid_cancel`.

## Context

- Previously, `AgentGridBridge` held `MasterHandle` directly (`--grid-master`), so `agent-mesh hub` could only bridge to an embedded Master running inside the hub process rather than an existing cluster Master on port `8088`.
- `crates/cli/Cargo.toml` dev-depends on `agent_mesh`, preventing `agent_mesh` from depending on `rusty_grid_cli` to reuse `connect_master`.
- All five task handlers in `bridge.rs` (`handle_compute`, `handle_compile`, `handle_submit`, `handle_3d_generate`, `handle_auto_rig`) replaced caller `tags` with hardcoded vectors and lacked `placement`, inline `files` (`StagedFile`), `outputs` (`expected_outputs`), `artifacts` serialization, `grid_result`, and `grid_cancel`.
- Security PRs 114 and 132 established that the hub must never read hub-local disk files named by mesh peers and must refuse path-valued inputs before submitting tasks.

## Goals

- G1. Decouple `AgentGridBridge` behind an object-safe async `GridBackend` trait (`InProcessBackend` + `RemoteMasterBackend`) using `futures::future::BoxFuture` with zero new Cargo dependencies and zero cyclic crate edges.
- G2. Add `agent-mesh hub --grid-connect <HOST:PORT>` (`OXIDE_GRID_CONNECT`, conflicting with `--grid-master`) with `oxideswarm`-aligned credentials (`--secret`/`OXIDESWARM_SECRET`, `--secret-file`/`OXIDESWARM_SECRET_FILE`, `--master-key`/`RUSTY_GRID_MASTER_KEY`, `--relay-url`/`RUSTY_GRID_RELAY_URL`, `--insecure`).
- G3. Surface live Master reachability in `/api/nodes` (`"online"` vs `"offline"` via `bridge.health_check()`) and advertise `grid_result`/`result` and `grid_cancel`/`cancel` in `supported_commands`.
- G4. Preserve caller `tags` and parse `placement` + top-level constraints (`os`, `arch`, `worker`, `isolation`, `require_tags`, `require_tools`/`tools`) across all bridged task handlers.
- G5. Support strict inline-only `files` (`StagedFile`, never reading hub-local disk paths) and `outputs` (`expected_outputs` returned in `payload["artifacts"]` with SHA-256), plus `grid_result`/`grid_cancel` and `AgentMeshClient` helpers.

## Non-goals

- Changing `crates/core` wire structs or bumping `PROTOCOL_VERSION_CURRENT` (`10`), `STORE_FORMAT_VERSION` (`4`), or `EXTENSION_REGISTRY` (`&[]`).
- Adding `priority` to Protocol 10 `ClientMessage::SubmitTask` (`RemoteMasterBackend` submits at priority `0`, matching `oxideswarm submit`).
- Allowing mesh peers to reference or read files from the hub's local filesystem.

## Design

### D1: `GridBackend` trait and `InProcessBackend` (`crates/agent_mesh/src/grid_backend.rs`)

- `GridBackend: Send + Sync` exposes boxed-future async operations: `submit_task`, `submit_and_wait`, `get_task_status`, `get_task_result`, `wait_task`, `cancel_task`, `cluster_status`, `list_workers`, `health_check`, and `close`.
- `InProcessBackend(MasterHandle)` preserves `priority` forwarding (`submit_task_with_priority`), `TaskInfo` + `CheckpointData` JSON compatibility in `grid_status`, and `ClientResponse::TaskCancelled { success }` semantics in `cancel_task` (`true` only when newly cancelled, `false` when already terminal or unknown).

### D2: `RemoteMasterBackend` (`crates/agent_mesh/src/remote_backend.rs`)

- Built directly on `rusty_grid_core` (`ClusterSecret`, `client_handshake`, `v3_client_handshake`, `HandshakePurpose::Member`, `MessageTransport`, `GridStream`, `configure_tcp_stream`, P2P/relay, and `auto` discovery).
- **Concurrency & resilience**:
  - Short RPCs (`SubmitTask { wait: false }`, `GetTaskStatus`, `GetTaskResult`, `CancelTask`, `ClusterStatus`, `ListWorkers`, `Hello`) borrow a pooled `tokio::sync::Mutex<Option<MessageTransport<GridStream>>>` transport by taking it out of the lock before network I/O. Idempotent RPCs retry once on a fresh connection when a pooled transport encounters EOF or reset; non-idempotent `SubmitTask` never retries after a completed pooled write.
  - Long-lived waits (`submit_and_wait` via `ClientMessage::SubmitTask { wait: true }`, and `wait_task` polling `ClientMessage::GetTaskResult`) open dedicated connections so concurrent `grid_status`, `grid_cancel`, and `/api/nodes` probes are never head-of-line blocked.
- **Health & shutdown**: `health_check` executes a bounded `ClientMessage::Hello` probe (`rpc_timeout`, default `2s`); `close` shuts down the pooled transport and drains pending P2P endpoint closes.

### D3: CLI `--grid-connect` and `hub.rs` integration (`crates/agent_mesh/src/main.rs`, `hub.rs`)

- `Commands::Hub` accepts `--grid-connect <HOST:PORT>` (`OXIDE_GRID_CONNECT`, `conflicts_with = "grid_master"`), `--secret-file` (`OXIDESWARM_SECRET_FILE`), `--master-key` (`RUSTY_GRID_MASTER_KEY`), and `--relay-url` (`RUSTY_GRID_RELAY_URL`) alongside `--secret` (`OXIDESWARM_SECRET`) and `--insecure`.
- `RemoteMasterConfig::validate()` enforces `master_requires_secret` (refusing non-loopback, `auto`, and P2P targets without a cluster secret or pinned master key unless `--insecure` is set).
- At startup, an auth, version, or configuration failure (`AuthFailed`, `AuthRequired`, `IncompatibleVersion`, `Config`) fails fast; transient reachability errors attach the bridge with a warning so `/api/nodes` reflects `"offline"` until the Master comes up and transitions to `"online"` once reachable.
- `HubState::get_catalog` keeps `"grid"` conditional on `bridge.is_some()`, sets `"status"` from `bridge.health_check().await.is_ok()`, and includes `"grid_result"`, `"result"`, `"grid_cancel"`, and `"cancel"` in `supported_commands`.

### D4: Tags, placement, inline `files`, `outputs`, `grid_result`, and `grid_cancel` (`bridge.rs`, `client.rs`)

- **Worker join requirement**: `agent-mesh hub --grid-connect` connects as a **client**, not a worker. At least one `oxideswarm worker` must join the running Master for submitted tasks to schedule and run.
- **`apply_tags_and_placement`**: Used across all five task handlers (`handle_compute`, `handle_compile`, `handle_submit`, `handle_3d_generate`, `handle_auto_rig`). Preserves caller `tags`, appends the handler's default command tag and `"agent_bridge"` if absent, parses `placement` object and top-level `os`, `arch`, `worker`, `isolation` (`IsolationLevel::parse_required`), `require_tags`, and `require_tools`/`tools`, validates with `validate_task_user_tags` and `Placement::from_task_tags`, and calls `task.validate()`. Invalid `requirements` JSON in `handle_submit` returns an explicit error instead of falling back silently.
- **Inline `files` and `outputs`**: `handle_submit` accepts inline `files` (`staged_files` alias) as an array of objects (`path`/`name`, optional `executable`, and exactly one of `text`/`content`, `base64`/`data_base64`/`bytes_base64`, or `data`/`bytes`) or a map `{path: text_or_obj}`, validated via `validate_relative_sandbox_path` and 32 MiB limits. `outputs` (`expected_outputs` alias) validates sandbox-relative paths; both are restricted to `Command`, `ShellScript`, and `RustCompilation` tasks. `map_task_result_to_response` serializes `TaskResult::artifacts` into `payload["artifacts"]` (`path`, `size`, `sha256`, `data_base64`).
- **`grid_result` / `grid_cancel` and client helpers**: `bridge.rs` dispatches `grid_result`/`result` (supporting `wait: false/true`) and `grid_cancel`/`cancel`. `AgentMeshClient` adds `submit_grid`, `get_grid_result`, `cancel_grid_task`, and `extract_output_artifacts` (verifying `sha256` and `size`).

## Compatibility

- Protocol 10 (`PROTOCOL_VERSION_CURRENT = 10`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 9`) and Store v4 (`STORE_FORMAT_VERSION = 4`) remain unchanged; `EXTENSION_REGISTRY` and `crates/core/tests/data/wire_layout.txt` are untouched.
- `AgentGridBridge::new(MasterHandle)` and `--grid-master` startup banners remain backward-compatible for all existing callers and tests.
- Unattached hubs (neither `--grid-master` nor `--grid-connect`) continue omitting `"grid"` from `/api/nodes` and returning `ERR_BRIDGE_NOT_CONFIGURED`.

## Security

- **Zero hub-local disk reads**: `parse_inline_staged_files` and `parse_binary_data` never touch `std::fs` / `tokio::fs` and explicitly reject `Value::Array` of `Value::String`s (e.g. `["/etc/passwd"]`) without submitting tasks.
- **Fail-closed remote auth**: Non-loopback, `auto`, and P2P `--grid-connect` targets require a cluster secret or `--master-key` unless `--insecure` is explicitly supplied; empty `--secret` / `OXIDESWARM_SECRET` / `--secret-file` / `OXIDESWARM_SECRET_FILE` fail closed.
- **Sandbox path safety**: All staged file paths and expected output paths pass `validate_relative_sandbox_path`, and `extract_output_artifacts` verifies SHA-256 and byte length on decoded artifacts.

## Test plan

- **Unit tests (`crates/agent_mesh/src/{bridge.rs, remote_backend.rs, main.rs, client.rs}`)**: tag preservation, placement parsing/validation, inline `files` object/map decoding, string-path `files` refusal without hub disk reads, `outputs`/`artifacts` serialization, `RemoteMasterConfig` fail-closed auth, CLI `--grid-connect` / `--secret-file` / `--master-key` / `--relay-url` / `--grid-master` conflict checks, and `extract_output_artifacts` SHA-256 verification.
- **E2E tests (`crates/agent_mesh/tests/grid_remote_master_e2e.rs`)**: spawns separate `oxideswarm master` + `oxideswarm worker` + `agent-mesh hub --grid-connect` processes with isolated `HOME`/`USERPROFILE`/`TMPDIR`, fake `pmset`, and `ChildGuard` cleanup, exercising `/api/nodes` online/offline transitions, `grid_submit` with placement + inline `files` + `outputs` + SHA-256 artifact verification, non-matching placement hold/refusal, `grid_submit(wait=false)` -> `grid_result`, `grid_cancel`, and hub-local path rejection.
- **Verification gates**: `cargo fmt --all -- --check`, `cargo clippy -p agent_mesh --all-targets --locked -- -D warnings`, `ox-mode test size`, `cargo test -p agent_mesh --locked`, and `cargo test -p rusty_grid_core --lib wire_layout --locked`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| MESH-3 | Attach bridge to running Master + full `grid_submit` parity | `crates/agent_mesh/src/grid_backend.rs`, `crates/agent_mesh/src/remote_backend.rs`, `crates/agent_mesh/src/bridge.rs`, `crates/agent_mesh/src/hub.rs`, `crates/agent_mesh/src/client.rs`, `crates/agent_mesh/src/main.rs`, `crates/agent_mesh/src/lib.rs`, `crates/agent_mesh/tests/grid_remote_master_e2e.rs` | Unit, integration, and `agent-mesh` + `oxideswarm` E2E tests pass with zero leaked processes | Implemented |

## Status log

- 2026-11-22: Created design record `0104` and implemented `GridBackend`, `RemoteMasterBackend`, `--grid-connect`, placement/tags, inline `files`/`outputs`/`artifacts`, `grid_result`, and `grid_cancel` (`MESH-3`).
