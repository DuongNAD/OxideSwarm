# 0044: Remove dead code, unify master connection loops, and sweep leftover sandboxes

Status: Draft
Created: 2026-11-08    Updated: 2026-11-08

## Summary

- Delete unused files and symbols across `crates/core`, `crates/master`, `crates/worker`, and
  `crates/android_bridge`, or wire them to a production caller (`FEAT-11`).
- Merge the two worker connection loops in `crates/master/src/server.rs` (`handle_connection` for
  TCP and `handle_multiplexed_connection` for 2-lane QUIC) into a single `WorkerSession` state
  machine in `crates/master/src/session.rs` before Phase B (`FEAT-1`) modifies worker registration
  and task state transitions.
- Add a lock-guarded start-up sweep of leftover task sandboxes in `crates/worker/src/sandbox.rs`,
  and move `gen3d` and `autorig` behind an opt-in Cargo feature `3d` per `docs/ROADMAP.md` decision
  Q2.

## Context

What exists at `master` `01a9786`:

| Area | Today | Code |
|---|---|---|
| Unused assets and types | `crates/master/src/dashboard/index.html` and `crates/worker/src/shaders/hash.wgsl` are never included (`web_ui.rs:64` includes `crates/master/src/dashboard.html`; `wgpu_engine.rs:14` includes `crates/worker/src/shaders/gemm.wgsl`). `DashboardState`, `create_dashboard_router`, and `create_dashboard_router_with_auth` (`crates/master/src/dashboard/mod.rs:17-105`), `Sandbox::prepare_spec` (`crates/worker/src/sandbox.rs:714-727`), and `probe_physical_adapter` (`crates/worker/src/wgpu_engine.rs:183-185`) have no callers anywhere in the workspace | `crates/master/src/dashboard/`, `crates/worker/src/shaders/`, `crates/worker/src/sandbox.rs`, `crates/worker/src/wgpu_engine.rs` |
| Duplicate auto-rig provider routing | `auto_rig_mesh_async` (`crates/worker/src/autorig/mod.rs:113-156`) is called only by `crates/worker/tests/test_challenger_m3_blender_job_object.rs`, while `TaskRunner::execute_auto_rig_workload` (`crates/worker/src/runner.rs:2424-2477`) duplicates the same Blender-vs-builtin routing inline | `crates/worker/src/autorig/mod.rs`, `crates/worker/src/runner.rs` |
| Unused `compute_intensity` field | `TaskSpec::GpuCompute` (`crates/core/src/task.rs:1046`) and `BinaryTaskSpec::GpuCompute` (`:1157`) carry `compute_intensity: u32`, which `crates/worker/src/runner.rs:36` notes is never read by the worker or `wgpu_engine` | `crates/core/src/task.rs`, `crates/worker/src/runner.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/doctor.rs` |
| Dead role-arbitrage scoring and election code | Per `FEAT-6` ([design 0026](0026-refuse-claim-master-role.md)), the master refuses every `WorkerMessage::ClaimMasterRole` with `reject_master_role_claim` (`crates/master/src/server.rs:3211-3231`). In `crates/core/src/arbitrage.rs` (869 lines), only `MasterLeaseTicket` has a production caller; `MasterRefusalReason`, `MobileEnvironmentTelemetry`, `evaluate_mobile_master_refusal_matrix`, `is_cellular_or_metered`, `check_mobile_master_eligibility`, `evaluate_mobile_master_refusal`, `PlatformType`, `AuthorityEvaluation`, `calculate_authority_score`, `CandidateRanking`, `resolve_election_tie`, `ClaimVerdict`, and `evaluate_master_claim` have only test callers | `crates/core/src/arbitrage.rs`, `crates/core/src/lib.rs`, `tests/mobile_coordination_test.rs`, `tests/m5a_challenger_2_stress.rs` |
| Duplicate Android JNI exports | `crates/android_bridge/src/lib.rs:528-786` exports 11 `Java_com_oxideswarm_worker_service_OxideWorkerBridge_*` functions for a `com.oxideswarm.worker.service.OxideWorkerBridge` class that does not exist, plus `nativeStartWorkerLegacy` (`:578`, `:850`), while the Kotlin singleton in `packaging/android/app/app/src/main/java/com/oxideswarm/worker/runner/OxideWorkerBridge.kt` lives in `com.oxideswarm.worker.runner` | `crates/android_bridge/src/lib.rs`, `packaging/android/app/app/src/main/java/com/oxideswarm/worker/runner/OxideWorkerBridge.kt` |
| Duplicated TCP and QUIC worker session loops | `crates/master/src/server.rs` contains two worker session handlers: `handle_connection` (`:3284-3761`, 478 lines, single stream) and `handle_multiplexed_connection` (`:3765-4425`, 660 lines, 2-lane QUIC control + data streams). Both repeat authentication, `Register` validation, `WorkerRegistry::register_session`, superseded-session failover, `RegisterAck` delivery, `BlobService` setup, the inbound message dispatch table (`Heartbeat`, `Disconnecting`, `TaskProgress`, `TaskResult`, `Checkpoint`, `BlobRequest`, `MobileTelemetry`, `ClaimMasterRole`), and disconnect cleanup | `crates/master/src/server.rs` |
| Leftover task sandboxes after a worker crash | `Sandbox::create` (`crates/worker/src/sandbox.rs:625`) creates `<base_dir>/<task_id>` and `Sandbox::destroy` removes it when a task finishes. After `SIGKILL` or power loss, leftover `<base_dir>/<task_id>` directories remain on disk forever; `SandboxConfig::ensure_usable` (`:277-295`) sweeps only legacy `<base_dir>/cache/objects` and `<base_dir>/cache/tmp` | `crates/worker/src/sandbox.rs` |
| Always-compiled 3D modules | `crates/worker/src/gen3d` and `crates/worker/src/autorig` are compiled unconditionally into every `rusty_grid_worker` build even though `docs/ROADMAP.md` decision Q2 freezes 3D and moves it behind an opt-in Cargo feature | `crates/worker/Cargo.toml`, `crates/worker/src/lib.rs`, `crates/worker/src/runner.rs` |

## Goals

- **G1** Every symbol and file listed in `FEAT-11` (`docs/BACKLOG.md:1223-1235`) either has a
  production caller or is removed.
- **G2** TCP and 2-lane QUIC worker sessions share one session state machine (`WorkerSession` in
  `crates/master/src/session.rs`), with one implementation of authentication, registration,
  inbound message dispatch, and disconnect cleanup.
- **G3** When a worker starts with `keep_sandboxes == false`, `SandboxConfig::ensure_usable` sweeps
  leftover `<base_dir>/<task_id>` directories from crashed runs without deleting a sandbox that
  another live worker on the same host is using.
- **G4** `gen3d` and `autorig` compile only when the opt-in Cargo feature `3d` is enabled
  (`docs/ROADMAP.md` decision Q2).

## Non-goals

- Changing the wire behaviour of 2-lane QUIC multiplexing (`STREAM_CONTROL` and `STREAM_DATA` stay
  separate lanes with control-lane head-of-line bypass).
- Adding `STREAM_CLIENT` over P2P (`W7` of [design 0039](0039-agent-workspaces.md)); the unified
  session entry point simply makes `W7` a one-line branch in the P2P accept path.
- Implementing real master failover (`FEAT-6`, Phase E).
- Changing `TaskSpec::Generative3D` or `TaskSpec::AutoRig` wire definitions in `rusty_grid_core`.

## Design

### D1 Unused files, functions, and `compute_intensity`

1. **Delete unused static files:**
   - Remove `crates/master/src/dashboard/index.html` and `crates/worker/src/shaders/hash.wgsl`.
2. **Simplify `crates/master/src/dashboard/mod.rs`:**
   - Remove `DashboardState`, `create_dashboard_router`, and `create_dashboard_router_with_auth`
     (`crates/master/src/dashboard/mod.rs:17-105`), keeping `pub mod dto; pub use dto::*;`.
3. **Remove `Sandbox::prepare_spec` and `probe_physical_adapter`:**
   - Delete `Sandbox::prepare_spec` (`crates/worker/src/sandbox.rs:714-727`); `TaskRunner`
     materializes shell scripts (`crates/worker/src/runner.rs:1718`) and Rust sources (`:1859`)
     directly with task-specific failure messages.
   - Delete `probe_physical_adapter` (`crates/worker/src/wgpu_engine.rs:183-185`); `gpu.rs` probes
     the GPU through `WgpuEngine::new()`.
4. **Unify `auto_rig_mesh_async` with `TaskRunner::execute_auto_rig_workload`:**
   - Update `auto_rig_mesh_async` in `crates/worker/src/autorig/mod.rs:113` to accept a progress
     callback `on_progress: F` (`F: FnMut(f32, &'static str, &str) + Send + 'static`) and route
     between `BlenderRigProvider::execute_rigging` and `auto_rig_mesh_with_progress`.
   - Call `auto_rig_mesh_async` from `TaskRunner::execute_auto_rig_workload`
     (`crates/worker/src/runner.rs:2424-2477`) so production execution and
     `crates/worker/tests/test_challenger_m3_blender_job_object.rs` go through the same function.
5. **Remove `compute_intensity` from `TaskSpec::GpuCompute`:**
   - Remove `compute_intensity: u32` from `TaskSpec::GpuCompute` (`crates/core/src/task.rs:1046`)
     and `BinaryTaskSpec::GpuCompute` (`:1157`), plus its conversions in `task.rs:1211-1421` and
     call sites in `crates/cli/src/cli_app.rs:2399`, `crates/cli/src/mcp.rs:1735`,
     `crates/cli/src/doctor.rs:506`, and `crates/agent_mesh/src/bridge.rs:196-215`.
   - Bump `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` by 1 and regenerate
     `crates/core/src/wire_layout.txt` with `UPDATE_WIRE_LAYOUT=1` (as enforced by `FEAT-8`,
     [design 0040](0040-protocol-versioning.md)).

### D2 Prune `crates/core/src/arbitrage.rs`

- Remove the unreferenced role-arbitrage scoring, election, and mobile master refusal items from
  `crates/core/src/arbitrage.rs` and `crates/core/src/lib.rs`:
  - Types: `MasterRefusalReason`, `MobileEnvironmentTelemetry`, `PlatformType`,
    `AuthorityEvaluation`, `CandidateRanking`, `ClaimVerdict`.
  - Functions: `evaluate_mobile_master_refusal_matrix`, `is_cellular_or_metered`,
    `check_mobile_master_eligibility`, `evaluate_mobile_master_refusal`,
    `calculate_authority_score`, `resolve_election_tie`, `evaluate_master_claim`.
  - Constants: `MIN_MASTER_BATTERY_PCT`, `MAX_MASTER_BATTERY_TEMP_C`, `ELECTION_THRESHOLD_SCORE`,
    `SCORE_MIN`, `SCORE_MAX`, `LEASE_RENEWAL_INTERVAL_MS`.
- Keep `MasterLeaseTicket`, `LeaseValidationError`, `LEASE_VALIDITY_DURATION_MS`, and
  `LEASE_HMAC_LABEL` in `crates/core/src/arbitrage.rs` for the `WorkerMessage::ClaimMasterRole`
  wire struct (`crates/core/src/protocol.rs:479`) and its refusal tests.
- Remove the dead-code tests of those deleted functions from `crates/core/src/arbitrage.rs`,
  `tests/mobile_coordination_test.rs`, and `tests/m5a_challenger_2_stress.rs`, keeping the tests
  that verify `MasterLeaseTicket` signing/verification and the master's refusal of
  `WorkerMessage::ClaimMasterRole`.

### D3 Remove duplicate `service` JNI exports in `crates/android_bridge`

- In `crates/android_bridge/src/lib.rs:528-970`, delete all 11
  `Java_com_oxideswarm_worker_service_OxideWorkerBridge_*` functions and
  `Java_com_oxideswarm_worker_runner_OxideWorkerBridge_nativeStartWorkerLegacy`.
- Move the JNI argument extraction into the 10
  `Java_com_oxideswarm_worker_runner_OxideWorkerBridge_*` exports that match the `external fun`
  declarations in
  `packaging/android/app/app/src/main/java/com/oxideswarm/worker/runner/OxideWorkerBridge.kt`
  (`nativeStartWorker`, `nativeSetSandboxDir`, `nativeSetClusterSecret`, `nativeUpdateTelemetry`,
  `nativeUpdateTelemetryDetailed`, `nativeStopWorker`, `nativeIsRunning`, `nativeGetWorkerStatus`,
  `nativeDiscoverMaster`, and `nativeTriggerFastEvacuation`).

### D4 Unify TCP and QUIC worker connection loops (`crates/master/src/session.rs`)

Create `crates/master/src/session.rs` and replace the duplicated worker bodies of
`handle_connection` (`crates/master/src/server.rs:3284-3761`) and `handle_multiplexed_connection`
(`crates/master/src/server.rs:3765-4425`) with a single `WorkerSession` state machine:

1. **Pre-registration transport abstraction (`PreAuthSession`):**
   - `PreAuthSession::Single { transport, remote_addr }` (TCP)
   - `PreAuthSession::Multiplexed { ctrl_transport, data_transport, remote_addr, p2p_conn }` (QUIC)
   - Both run `authenticate_and_read_first_message` on the primary stream (`transport` or
     `ctrl_transport`), drop `handshake_permit` immediately after authentication, and on
     authentication failure call `drain_refusal_stream` when `p2p_conn.is_some()`.
   - If the first message is `InboundMessage::Client(client_msg)`:
     - `Single` delegates to `handle_client_connection`.
     - `Multiplexed` returns `Err(GridError::HandshakeFailed("Client connection not supported over multiplexed P2P worker transport"))`.
2. **Shared registration step (`WorkerSession::register`):**
   - Validates `WorkerMessage::Register` (`is_protocol_version_compatible(protocol_version)` and
     `capabilities.cpu_cores > 0`), refuses `WorkerMessage::ClaimMasterRole` via
     `reject_master_role_claim`, and rejects any other initial variant. Refusal frames use
     `send_refusal_and_drain` when `p2p_conn.is_some()` and `send_msg` otherwise.
   - Calls `registry.register_session`, attaches `rx_clock`, runs `failover_superseded_session` if
     `registration.superseded_session` is `Some`, registers `P2pConnGuard` if `p2p_conn` is
     present, sends `MasterMessage::RegisterAck`, and broadcasts
     `DashboardStreamMessage::WorkerRegistered`.
3. **Active session lanes (`ActiveLanes`):**
   - `ActiveLanes::Single`: splits `transport` into `reader` and `writer`, spawns one outbound
     writer task (`outbound_rx -> writer` with `handle_oversized_master_message`), and uses
     `outbound_tx` for both control replies (`HeartbeatAck`, `RoleHandoverAck`, `Shutdown`) and
     data frames.
   - `ActiveLanes::Multiplexed`: splits `ctrl_transport` and `data_transport`, spawns the
     head-of-line-free `router_task` (`outbound_rx -> ctrl_out_tx / data_out_tx`),
     `ctrl_writer_task`, and `data_writer_task`, and uses `ctrl_out_tx` for immediate control
     replies.
   - `ActiveLanes::recv_next(&mut self) -> Result<Option<(LaneKind, WorkerMessage)>, ProtocolError>`
     reads the next `WorkerMessage` from the single stream (`LaneKind::Unified`) or via
     `tokio::select!` across `ctrl_reader` (`LaneKind::Control`) and `data_reader`
     (`LaneKind::Data`).
4. **Single event loop (`WorkerSession::run`):**
   - One `tokio::select!` loop over `lanes.recv_next()` and `shutdown_rx.changed()`:
     - On `LaneKind::Data`, accepts `TaskProgress`, `TaskResult`, `Checkpoint`, and `BlobRequest`,
       logging a warning on any control-only variant (`Heartbeat`, `Disconnecting`,
       `MobileTelemetry`, `ClaimMasterRole`, `Register`), preserving the exact lane separation of
       `handle_multiplexed_connection`.
     - On `LaneKind::Control`, accepts `Heartbeat`, `Disconnecting`, `MobileTelemetry`,
       `ClaimMasterRole`, and `BlobRequest`, logging a warning on unexpected variants.
     - On `LaneKind::Unified`, accepts all `WorkerMessage` variants.
   - On loop exit (graceful `Disconnecting`, EOF, socket error, or master `Shutdown`), runs the
     single cleanup block (`handle_worker_disconnect_with_broadcast`, `P2pConnGuard::unlist`, and
     dropping the `AbortOnDrop` writer guards).

### D5 Start-up sweep of leftover task sandboxes (`crates/worker/src/sandbox.rs`)

1. **Per-sandbox advisory lock file (`.sandbox.lock`):**
   - In `Sandbox::create` (`crates/worker/src/sandbox.rs:625`), after creating `<base_dir>/<task_id>`
     with mode `0o700`, open `<base_dir>/<task_id>/.sandbox.lock` with `create(true).write(true)`
     and acquire an exclusive non-blocking lock via `std::fs::File::try_lock()`. Store the locked
     `std::fs::File` in `Sandbox { ..., lock_file: Option<std::fs::File> }`.
   - In `Sandbox::destroy` (and `Drop`), drop `lock_file` before removing `<base_dir>/<task_id>`.
     If the worker process is killed with `SIGKILL` or crashes, the operating system kernel closes
     the file descriptor and releases the lock automatically.
2. **Start-up sweep in `SandboxConfig::ensure_usable`:**
   - When `!self.keep_sandboxes`, `SandboxConfig::ensure_usable` (`crates/worker/src/sandbox.rs:277`)
     calls `self.sweep_leftover_sandboxes().await` after `remove_legacy_staging_cache().await`.
   - `sweep_leftover_sandboxes` iterates direct entries of `self.base_dir`:
     - Parses the entry's file name with `uuid::Uuid::parse_str`. Non-UUID entries (`cache`,
       `worker.id`, `.write_probe_*`, or anything else) are ignored.
     - Inspects the entry with `tokio::fs::symlink_metadata` and skips symbolic links,
       non-directories, or (on Unix) directories whose `uid != current_euid()`.
     - Checks `<entry>/.sandbox.lock` via `symlink_metadata` (refusing if it is a symbolic link),
       opens or creates it, and calls `try_lock()`.
     - If `try_lock()` fails (`TryLockError::WouldBlock`), another worker process on the same host
       is actively running that task, so the directory is left untouched.
     - If `try_lock()` succeeds, drops the lock handle and removes `<entry>` with
       `tokio::fs::remove_dir_all`, logging a summary count at `info!` when one or more leftover
       sandboxes were swept.

### D6 Gate `gen3d` and `autorig` behind Cargo feature `3d`

- In `crates/worker/Cargo.toml`, add `3d = []` under `[features]`. In `crates/cli/Cargo.toml`, add
  `3d = ["rusty_grid_worker/3d"]`.
- In `crates/worker/src/lib.rs`, gate `pub mod autorig;` and `pub mod gen3d;` with
  `#[cfg(feature = "3d")]`.
- In `crates/worker/src/runner.rs`, compile the full `execute_generative_3d_workload` and
  `execute_auto_rig_workload` implementations under `#[cfg(feature = "3d")]`, and provide
  `#[cfg(not(feature = "3d"))]` stubs that return `TaskResult::failure` with a clear message telling
  the operator to rebuild with `--features 3d` (mirroring `spawn_real_gpu_job` when built without
  `gpu-wgpu`).
- Add `required-features = ["3d"]` to the 3D test targets (`test_challenger_m3_blender_job_object`,
  `m8_challenger_stress`, `m8b_challenger_stress`, `m8_autorig_verification`) so default builds skip
  compiling `gen3d` and `autorig`, while `--features 3d` (or `--all-features`) runs them.

## Compatibility

- Removing `compute_intensity` from `TaskSpec::GpuCompute` changes the bincode layout of `Task` and
  `WorkerMessage::AssignTask` / `ClientMessage::SubmitTask`. Per `FEAT-8`
  ([design 0040](0040-protocol-versioning.md)), `PROTOCOL_VERSION_CURRENT` and
  `PROTOCOL_VERSION_MIN_COMPATIBLE` advance by 1 and `crates/core/src/wire_layout.txt` is updated
  in the same commit.
- Upgrade order follows `DEPLOYMENT.md` 3.5: upgrade and restart the master first, then workers and
  CLI/MCP clients.
- `WorkerMessage::ClaimMasterRole { ticket: MasterLeaseTicket }` is unchanged on the wire.
- On Android, `OxideWorkerBridge.kt` already lives in `com.oxideswarm.worker.runner`, so removing
  the `com.oxideswarm.worker.service` JNI symbols does not affect the APK bridge.

## Security

- `sweep_leftover_sandboxes` checks `symlink_metadata` on `<base_dir>`, `<base_dir>/<uuid>`, and
  `<base_dir>/<uuid>/.sandbox.lock`, never follows symbolic links, checks `uid == current_euid()`
  on Unix, and touches only valid UUID-named subdirectories that are not locked by a live worker.
- The unified `WorkerSession` preserves the pre-authentication `handshake_permit` budget,
  `handshake_timeout` Slowloris defence, cluster-secret handshake before reading `InboundMessage`,
  and QUIC control/data lane separation.

## Test plan

- **Unit:**
  - `crates/core/src/wire_layout_tests.rs`: passes with the bumped protocol version and updated
    `wire_layout.txt` after removing `compute_intensity`.
  - `crates/master/src/session.rs` and `crates/master/src/server.rs`: existing TCP and 2-lane QUIC
    registration, refusal, heartbeat, mobile telemetry, oversized-frame, and superseded-session
    unit tests pass through `WorkerSession`.
  - `crates/worker/src/sandbox.rs`: unit tests verifying that:
    1. `ensure_usable` removes unlocked leftover `<base_dir>/<uuid>` directories when
       `keep_sandboxes == false`;
    2. `ensure_usable` leaves a live `Sandbox` (holding `.sandbox.lock`) untouched even when called
       by a second `SandboxConfig` sharing the same `base_dir`;
    3. `ensure_usable` leaves `<base_dir>/cache/blobs`, `worker.id`, non-UUID directories, and
       symbolic links untouched;
    4. `ensure_usable` removes nothing when `keep_sandboxes == true`.
  - `cargo check --workspace --locked` and `cargo test --workspace --lib --locked` pass both with
    default features (without `3d`) and with `--features 3d`.
- **Integration:**
  - `cargo test -p rusty_grid_master --test master_quic_limits --locked`
  - `cargo test -p rusty_grid_master --test cluster_auth_e2e --locked`
  - `cargo test -p rusty_grid_worker --test worker_connection_lifecycle --locked`
  - `cargo test -p rusty_grid_worker --test test_challenger_m3_blender_job_object --features 3d --locked`
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `cargo check -p rusty_grid_master --no-default-features --locked`, `cargo test --workspace --lib --locked`,
  and `cargo audit`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| F11-1 | Delete `dashboard/index.html`, `shaders/hash.wgsl`, `DashboardState`, `Sandbox::prepare_spec`, `probe_physical_adapter`, and `TaskSpec::GpuCompute::compute_intensity`; wire `auto_rig_mesh_async` into `TaskRunner::execute_auto_rig_workload`; bump protocol version and `wire_layout.txt` | `crates/core/src/task.rs`, `crates/core/src/protocol.rs`, `crates/core/src/wire_layout.txt`, `crates/master/src/dashboard/`, `crates/worker/src/shaders/`, `crates/worker/src/sandbox.rs`, `crates/worker/src/wgpu_engine.rs`, `crates/worker/src/autorig/mod.rs`, `crates/worker/src/runner.rs`, `crates/cli/src/`, `crates/agent_mesh/src/bridge.rs` | None of the removed files/symbols exist, `auto_rig_mesh_async` is called by `execute_auto_rig_workload`, and `wire_layout_tests` passes | Planned |
| F11-2 | Prune `crates/core/src/arbitrage.rs` to `MasterLeaseTicket` and remove unreferenced scoring/election/refusal exports | `crates/core/src/arbitrage.rs`, `crates/core/src/lib.rs`, `tests/mobile_coordination_test.rs`, `tests/m5a_challenger_2_stress.rs` | Every public item in `arbitrage.rs` has a production caller or is required by `WorkerMessage::ClaimMasterRole` | Planned |
| F11-3 | Remove `com.oxideswarm.worker.service.OxideWorkerBridge` JNI exports and `nativeStartWorkerLegacy` | `crates/android_bridge/src/lib.rs` | Only `Java_com_oxideswarm_worker_runner_OxideWorkerBridge_*` JNI symbols are exported | Planned |
| F11-4 | Merge TCP `handle_connection` and QUIC `handle_multiplexed_connection` into `WorkerSession` in `crates/master/src/session.rs` | `crates/master/src/session.rs`, `crates/master/src/server.rs`, `crates/master/src/lib.rs` | `server.rs` delegates both TCP and 2-lane QUIC worker sessions to `WorkerSession`, and all master/worker unit and integration tests pass | Planned |
| F11-5 | Add `.sandbox.lock` and start-up leftover sandbox sweep in `SandboxConfig::ensure_usable` | `crates/worker/src/sandbox.rs` | Unit tests prove crashed UUID sandboxes are swept on start-up while live locked sandboxes and `cache/blobs` are preserved | Planned |
| F11-6 | Gate `gen3d` and `autorig` behind Cargo feature `3d` (decision Q2) | `crates/worker/Cargo.toml`, `crates/worker/src/lib.rs`, `crates/worker/src/runner.rs`, `crates/cli/Cargo.toml`, `Cargo.toml` | Workspace builds and tests pass both with default features and with `--features 3d` | Planned |

## Status log

- 2026-11-08: Draft written from `docs/BACKLOG.md` (`FEAT-11`) and `docs/ROADMAP.md` (decision Q2)
  at `master` `01a9786`.
