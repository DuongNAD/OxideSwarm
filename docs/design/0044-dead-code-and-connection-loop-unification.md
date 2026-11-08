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
  production caller, is removed, or (for `compute_intensity` on the wire) is deprecated until the
  next breaking protocol bump.
- **G2** TCP and 2-lane QUIC worker sessions share one session state machine (`WorkerSession` in
  `crates/master/src/session.rs`), with one implementation of authentication, registration,
  inbound message dispatch, and disconnect cleanup.
- **G3** When a worker starts with `keep_sandboxes == false`, `SandboxConfig::ensure_usable` sweeps
  leftover `<base_dir>/<task_id>` directories from crashed runs without deleting a sandbox that
  another live worker on the same host is using, even if the running task deletes everything inside
  its own sandbox directory.
- **G4** `gen3d` and `autorig` compile only when the opt-in Cargo feature `3d` is enabled
  (`docs/ROADMAP.md` decision Q2), CI still lints and tests the `3d` feature, and the master does
  not place 3D tasks onto workers built without `3d`.

## Non-goals

- Changing the wire behaviour of 2-lane QUIC multiplexing (`STREAM_CONTROL` and `STREAM_DATA` stay
  separate lanes with control-lane head-of-line bypass).
- Adding `STREAM_CLIENT` over P2P (`W7` of [design 0039](0039-agent-workspaces.md)); the unified
  session entry point simply makes `W7` a one-line branch in the P2P accept path.
- Implementing real master failover (`FEAT-6`, Phase E).
- Changing `TaskSpec::Generative3D` or `TaskSpec::AutoRig` wire definitions in `rusty_grid_core`.
- Forcing a breaking protocol version bump solely to drop `compute_intensity` from the wire in
  `FEAT-11`.

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
5. **Deprecate `compute_intensity` on `TaskSpec::GpuCompute` without a wire break:**
   - After `FEAT-8` ([design 0040](0040-protocol-versioning.md)) and `DEPLOYMENT.md` 3.5, every
     bincode layout change to `TaskSpec` forces a breaking `PROTOCOL_VERSION_CURRENT` bump that
     makes old peers refuse each other.
   - Keep `compute_intensity: u32` on the wire in `TaskSpec::GpuCompute`
     (`crates/core/src/task.rs:1046`), `HumanTaskSpec::GpuCompute` (`:1100`), and
     `BinaryTaskSpec::GpuCompute` (`:1157`) so `crates/core/tests/data/wire_layout.txt` is
     unchanged, and mark `compute_intensity` `#[deprecated]` in `crates/core/src/task.rs` (adding
     `#[allow(deprecated)]` on the internal mirror conversions in `task.rs:1211-1421` and in
     `crates/core/src/wire_layout_tests.rs:235-243`).
   - Stop setting `compute_intensity` from the CLI (`crates/cli/src/cli_app.rs:2399`), MCP
     (`crates/cli/src/mcp.rs:1735`), `doctor` (`crates/cli/src/doctor.rs:506`), and `agent_mesh`
     (`crates/agent_mesh/src/bridge.rs:196-215`), passing `0` internally under
     `#[allow(deprecated)]`.
   - Remove `compute_intensity` from the wire in the same release as the next required protocol
     version bump: `W1` of [design 0039](0039-agent-workspaces.md).

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
   - Calls `registry.register_session`, attaches `rx_clock`, spawns `failover_superseded_session`
     in its own detached task if `registration.superseded_session` is `Some`, registers
     `P2pConnGuard` if `p2p_conn` is present, sends `MasterMessage::RegisterAck`, and broadcasts
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

#### Preserved invariants

Every invariant of the existing TCP and QUIC session handlers is preserved and verified by its
existing guard test(s):

- **A failover sweep of session `s` also takes every older session (`<= s`) of that worker:**
  guarded by `queue::tests::test_sweep_of_a_later_session_recovers_tasks_stranded_on_an_older_one`
  (`crates/master/src/queue.rs:2924`) and
  `test_a_third_registration_of_a_worker_id_leaves_no_task_on_a_dead_session`
  (`tests/master_failover_cancel_liveness.rs:409`).
- **A connection handler that a newer registration aborted never sweeps the new session:**
  guarded by `registry::tests::test_registry_session_isolation_on_reconnect`
  (`crates/master/src/registry.rs:1219`),
  `queue::tests::test_session_scoped_failover_spares_the_newer_session`
  (`crates/master/src/queue.rs:2891`),
  `test_reregistering_worker_id_fails_over_the_superseded_session_exactly_once`
  (`tests/master_failover_cancel_liveness.rs:299`), and
  `test_a_restarted_worker_keeps_its_identity_at_the_master`
  (`tests/worker_connection_lifecycle.rs:904`).
- **The supersede sweep (`failover_superseded_session`) runs in its own spawned task:**
  guarded by `server::tests::test_superseded_session_sweep_survives_the_abort_of_its_caller`
  (`crates/master/src/server.rs:5162`).
- **`BlobService` answers off the inbound loop, on the data stream:**
  guarded by `test_adversarial_master_multiplexed_heartbeat_under_backpressured_data_lane`
  (`tests/m2_challenger_2_stress.rs:1015`),
  `test_adversarial_data_lane_backpressure_and_concurrent_traffic`
  (`tests/m3_challenger_2_checkpoint_stress.rs:570`), and `tests/blob_transfer_e2e.rs`.
- **The handshake permit is held until authentication finishes, then dropped:**
  guarded by `test_workers_over_quic_release_the_pre_auth_slot_once_registered`
  (`tests/master_quic_limits.rs:123`) and
  `server::tests::test_connection_limits_keep_two_separate_budgets_and_do_not_leak_permits`
  (`crates/master/src/server.rs:5231`).
- **P2P refusals go through `send_refusal_and_drain` (and `drain_refusal_stream` on pre-registration
  auth failure):**
  guarded by `server::tests::test_p2p_handshake_refusals_delivered_over_real_iroh`
  (`crates/master/src/server.rs:6553`).

### D5 Start-up sweep of leftover task sandboxes (`crates/worker/src/sandbox.rs`)

1. **External per-sandbox advisory lock file (`<base_dir>/.locks/<task_id>.lock`):**
   - Lock files live in `<base_dir>/.locks/<task_id>.lock`, outside the task-writable directory
     `<base_dir>/<task_id>/`, so a running task cannot unlink or replace its own lock file and the
     lock file never appears in the task's directory listings or output globs.
   - In `Sandbox::create` (`crates/worker/src/sandbox.rs:625`):
     1. Ensure `<base_dir>/.locks` exists with mode `0o700` on Unix (verifying via
        `symlink_metadata` that `<base_dir>/.locks` is a real directory owned by `current_euid()`).
     2. Open `<base_dir>/.locks/<task_id>.lock` (refusing symlinks) with `create(true).write(true)`
        and acquire an exclusive non-blocking lock via `std::fs::File::try_lock()` **before**
        creating `<base_dir>/<task_id>`, so a concurrent sweep can never observe a newly created
        sandbox directory that is not locked yet.
     3. Create `<base_dir>/<task_id>` with mode `0o700` and store the locked `std::fs::File` in
        `Sandbox { ..., lock_file: Option<std::fs::File> }`.
   - In `Sandbox::destroy` (and `Drop`), remove `<base_dir>/<task_id>` first, then unlink
     `<base_dir>/.locks/<task_id>.lock` and drop `lock_file`. If the worker process is killed with
     `SIGKILL` or crashes, the operating system kernel closes the file descriptor and releases the
     advisory lock automatically.
2. **Start-up sweep in `SandboxConfig::ensure_usable`:**
   - When `!self.keep_sandboxes`, `SandboxConfig::ensure_usable` (`crates/worker/src/sandbox.rs:277`)
     calls `self.sweep_leftover_sandboxes().await` after `remove_legacy_staging_cache().await`.
   - `sweep_leftover_sandboxes` ensures `<base_dir>/.locks` exists (`0o700`, non-symlink) and
     iterates direct entries of `self.base_dir`:
     - Parses the entry's file name with `uuid::Uuid::parse_str`. Non-UUID entries (`.locks`,
       `cache`, `worker.id`, `.write_probe_*`, or anything else) are ignored.
     - Inspects the entry with `tokio::fs::symlink_metadata` and skips symbolic links,
       non-directories, or (on Unix) directories whose `uid != current_euid()`.
     - Opens or creates `<base_dir>/.locks/<task_id>.lock` (checking `symlink_metadata` to refuse
       symbolic links) and calls `try_lock()`.
     - If `try_lock()` fails (`TryLockError::WouldBlock`), another worker process on the same host
       is actively running (or creating) that task, so `<base_dir>/<task_id>` is left untouched even
       if the task has deleted everything inside `<base_dir>/<task_id>`.
     - If `try_lock()` succeeds, removes `<base_dir>/<task_id>` with `tokio::fs::remove_dir_all`,
       unlinks `<base_dir>/.locks/<task_id>.lock`, and drops the lock handle, logging a summary
       count at `info!` when one or more leftover sandboxes were swept.
     - Sweeps any orphan `<task_id>.lock` file in `<base_dir>/.locks` whose `<base_dir>/<task_id>`
       directory no longer exists and whose `try_lock()` succeeds.
3. **Interaction with `--keep-sandboxes`:**
   - Starting a worker with `--keep-sandboxes` (`keep_sandboxes == true`) skips both the start-up
     sweep and `Sandbox::destroy` cleanup. A later worker start on the same `base_dir` *without*
     `--keep-sandboxes` sweeps any unlocked `<base_dir>/<task_id>` directories that an earlier
     `--keep-sandboxes` run kept. Document this explicitly in `README.md` next to
     `--keep-sandboxes` (`README.md:424` and `README.md:610`).

### D6 Gate `gen3d` and `autorig` behind Cargo feature `3d`

1. **Cargo feature and worker compilation:**
   - In `crates/worker/Cargo.toml`, add `3d = []` under `[features]`. In `crates/cli/Cargo.toml`
     and `crates/master/Cargo.toml`, add `3d = ["rusty_grid_worker/3d"]`.
   - In `crates/worker/src/lib.rs`, gate `pub mod autorig;` and `pub mod gen3d;` with
     `#[cfg(feature = "3d")]`.
   - In `crates/worker/src/runner.rs`, compile the full `execute_generative_3d_workload` and
     `execute_auto_rig_workload` implementations under `#[cfg(feature = "3d")]`, and provide
     `#[cfg(not(feature = "3d"))]` stubs that return `TaskResult::failure` with a clear message
     telling the operator to rebuild with `--features 3d` (mirroring `spawn_real_gpu_job` when
     built without `gpu-wgpu`).
2. **Cluster placement for 3D tasks (`ProtocolCapabilities::THREE_D`):**
   - Add `ProtocolCapabilities::THREE_D = 1 << 11` in `crates/core/src/protocol.rs` (leaving
     `1 << 10` for `0039`'s `WORKSPACES`).
   - Include `THREE_D` in `ProtocolCapabilities::all_supported()` on the master; in
     `crates/worker/src/client.rs`, advertise `ProtocolCapabilities::THREE_D` in
     `WorkerMessage::Register.features` only when compiled with `#[cfg(feature = "3d")]`.
   - In `crates/master/src/queue.rs:436` (`required_features(task)`), insert
     `ProtocolCapabilities::THREE_D` whenever
     `matches!(task.spec, TaskSpec::Generative3D(_) | TaskSpec::AutoRig(_))`.
   - In a mixed cluster, a `Generative3D` or `AutoRig` task never lands on a worker built without
     `3d`: it waits for a `3d`-capable worker with `UnschedulableReason::ClusterLacksCapability`
     (or fails immediately with `TaskResult::failure` when `--fail-unfulfillable-tasks` is enabled).
3. **Test targets and CI coverage:**
   - Every test file matched by
     `git grep -ln 'Generative3D\|TaskSpec::AutoRig\|gen3d::\|autorig::' -- 'tests/*.rs' 'crates/*/tests/*.rs'`
     is handled according to what it tests:
     - **Pure-3D targets (`required-features = ["3d"]` in `Cargo.toml`):**
       - `crates/cli/tests/test_cli_3d_submission.rs`
       - `crates/worker/tests/test_3d1_honest_sdf.rs`
       - `crates/worker/tests/test_autorig_adversarial_stress.rs`
       - `crates/worker/tests/test_autorig_pipeline.rs`
       - `crates/worker/tests/test_challenger_m3_blender_job_object.rs`
       - `crates/worker/tests/test_gen3d_adversarial_stress.rs`
       - `crates/worker/tests/test_gen3d_pipeline.rs`
       - `tests/test_3d_autorig_pipeline.rs`
       - `tests/test_challenger_m5_3d_pipeline.rs`
       - `tests/m8_challenger_stress.rs`, `tests/m8b_challenger_stress.rs`, and
         `tests/m8_autorig_verification.rs`
     - **Mixed targets (stay in default builds without `3d`; gate individual 3D helpers and test
       functions with `#[cfg(feature = "3d")]`):**
       - `crates/worker/tests/common/mod.rs` (gate 3D helper functions with `#[cfg(feature = "3d")]`)
       - `crates/worker/tests/test_challenger_m2_job_object.rs` (gate the 3D test functions with
         `#[cfg(feature = "3d")]`)
       - `crates/worker/tests/test_sec5_hardened_runner.rs` (required CI job `sec5 hardened runner`:
         gate only the 3D test functions with `#[cfg(feature = "3d")]` so non-3D hardened runner
         tests continue running in the required job, and run the 3D tests in the `3d` CI step)
       - `tests/test_challenger_m4_scheduler_runner.rs` (scheduler tests only construct
         `TaskSpec::Generative3D`/`AutoRig`; gate the `TaskRunner` 3D execution tests with
         `#[cfg(feature = "3d")]`, and update the scheduler tests to register workers with
         `ProtocolCapabilities::THREE_D` when testing 3D placement)
     - **Core/CLI-flag-only targets (use only `rusty_grid_core::TaskSpec`, which stays
       unconditional; run in default builds unchanged):**
       - `crates/cli/tests/test_bug2_cli_flags.rs`
       - `crates/core/tests/m1_3d_adversarial_stress.rs`
       - `crates/core/tests/m1_challenger_1_3d_serialization.rs`
   - **CI job/step:** Add a step/job in `.github/workflows/ci.yml` that runs
     `cargo clippy --workspace --all-targets --features 3d --locked -- -D warnings` and runs the
     `3d` test suite (`cargo test --workspace --features 3d --locked` or the `3d`-gated test targets
     including `test_sec5_hardened_runner --features 3d`) so the frozen 3D code never rots.

## Compatibility

- No wire format change is made in `FEAT-11`: `TaskSpec::GpuCompute::compute_intensity` remains on
  the wire (marked `#[deprecated]`) until `0039` W1 bumps the protocol version, and
  `WorkerMessage::ClaimMasterRole { ticket: MasterLeaseTicket }` is unchanged.
- `ProtocolCapabilities::THREE_D` (`1 << 11`) uses the additive `features` bitfield introduced in
  `0035`; workers built without `3d` simply omit the bit, and the master holds or refuses 3D tasks
  until a worker advertising `THREE_D` is available.
- On Android, `OxideWorkerBridge.kt` already lives in `com.oxideswarm.worker.runner`, so removing
  the `com.oxideswarm.worker.service` JNI symbols does not affect the APK bridge.

## Security

- `Sandbox::create` and `sweep_leftover_sandboxes` keep advisory lock files in
  `<base_dir>/.locks/<task_id>.lock` (`0o700`), outside `<base_dir>/<task_id>/`, acquire the lock
  before creating `<base_dir>/<task_id>`, check `symlink_metadata` on `<base_dir>`,
  `<base_dir>/.locks`, `<base_dir>/.locks/<task_id>.lock`, and `<base_dir>/<task_id>`, never follow
  symbolic links, check `uid == current_euid()` on Unix, and touch only valid UUID-named
  subdirectories whose `.locks/<task_id>.lock` is not held by a live worker.
- The unified `WorkerSession` preserves all six session, lane, permit, and P2P refusal invariants
  listed in D4.

## Test plan

- **Unit:**
  - `crates/core/src/wire_layout_tests.rs`: passes with `crates/core/tests/data/wire_layout.txt`
    unchanged while `TaskSpec::GpuCompute::compute_intensity` is marked `#[deprecated]`.
  - `crates/worker/src/sandbox.rs`: unit tests verifying that:
    1. `ensure_usable` removes unlocked leftover `<base_dir>/<uuid>` directories and their
       `<base_dir>/.locks/<uuid>.lock` files when `keep_sandboxes == false`;
    2. `ensure_usable` leaves a live `Sandbox` untouched even when a task deletes **everything**
       inside `<base_dir>/<task_id>` and a second `SandboxConfig` sharing the same `base_dir` runs
       `ensure_usable`;
    3. `ensure_usable` leaves `<base_dir>/.locks`, `<base_dir>/cache/blobs`, `worker.id`, non-UUID
       directories, and symbolic links untouched;
    4. `ensure_usable` removes nothing when `keep_sandboxes == true`, and a subsequent
       `ensure_usable` with `keep_sandboxes == false` sweeps those retained unlocked sandboxes.
  - `crates/master/src/queue.rs`: unit test verifying that `Generative3D` and `AutoRig` tasks are
    not placed on a worker registered without `ProtocolCapabilities::THREE_D` (returning
    `UnschedulableReason::ClusterLacksCapability` or failing under `--fail-unfulfillable-tasks`),
    and are placed once a worker with `ProtocolCapabilities::THREE_D` registers.
  - `crates/master/src/session.rs` and `crates/master/src/server.rs`: all existing TCP and 2-lane
    QUIC registration, refusal, heartbeat, mobile telemetry, oversized-frame, and
    superseded-session unit tests pass through `WorkerSession`.
- **Integration and Gates:**
  - Default build (without `3d`): `cargo test --workspace --locked` passes without compiling
    `gen3d` or `autorig`.
  - `3d` CI job/step: `cargo clippy --workspace --all-targets --features 3d --locked -- -D warnings`
    and all `3d`-gated test targets pass with `--features 3d`.
  - For PR (d) (`F11-d`, D4 session merge): **every `[[test]]` target of `rusty_grid_master` and
    `rusty_grid_worker`**, plus `bash test_integration.sh` and `bash run_chaos_test.sh`, must pass.
  - Standard gates on every PR: `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets --locked -- -D warnings`,
    `cargo check -p rusty_grid_master --no-default-features --locked`,
    `cargo test --workspace --lib --locked`, and `cargo audit`.

## Work items

Implementation is split into four separate pull requests, opened and reviewed in this exact order:

| ID | Pull Request | Scope | Files | Done when | Status |
|---|---|---|---|---|---|
| F11-a | PR (a): Dead-code deletions (`D1.1`–`D1.5`, `D2`, `D3`) | Delete `dashboard/index.html`, `shaders/hash.wgsl`, `DashboardState`, `Sandbox::prepare_spec`, `probe_physical_adapter`, dead `arbitrage.rs` items, and duplicate `service` JNI exports; wire `auto_rig_mesh_async` into `TaskRunner::execute_auto_rig_workload`; mark `compute_intensity` `#[deprecated]` in `task.rs` and stop setting it in CLI/MCP (no wire change; wire removal waits for `0039` W1) | `crates/core/src/task.rs`, `crates/core/src/arbitrage.rs`, `crates/core/src/lib.rs`, `crates/master/src/dashboard/`, `crates/worker/src/shaders/`, `crates/worker/src/sandbox.rs`, `crates/worker/src/wgpu_engine.rs`, `crates/worker/src/autorig/mod.rs`, `crates/worker/src/runner.rs`, `crates/cli/src/`, `crates/agent_mesh/src/bridge.rs`, `crates/android_bridge/src/lib.rs`, `tests/mobile_coordination_test.rs`, `tests/m5a_challenger_2_stress.rs` | None of the removed files/symbols exist, `auto_rig_mesh_async` is called by `execute_auto_rig_workload`, `compute_intensity` is deprecated with zero wire changes, and workspace tests pass | Planned |
| F11-b | PR (b): Start-up sandbox sweep (`D5`) | Add `<base_dir>/.locks/<task_id>.lock` advisory locking (acquired before creating `<base_dir>/<task_id>`) and start-up leftover sandbox sweep in `SandboxConfig::ensure_usable`; document `--keep-sandboxes` interaction in `README.md` | `crates/worker/src/sandbox.rs`, `README.md` | A unit test deletes everything inside a live sandbox, runs a second `ensure_usable`, and the sandbox directory survives; unlocked crashed UUID sandboxes are swept | Planned |
| F11-c | PR (c): Opt-in `3d` feature (`D6`) | Gate `gen3d` and `autorig` behind Cargo feature `3d`, add `ProtocolCapabilities::THREE_D` placement requirement for `Generative3D`/`AutoRig`, gate 3D test targets/functions, and add the `--features 3d` CI step | `crates/core/src/protocol.rs`, `crates/master/src/queue.rs`, `crates/master/Cargo.toml`, `crates/worker/Cargo.toml`, `crates/worker/src/lib.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/cli/Cargo.toml`, `crates/worker/tests/`, `crates/cli/tests/`, `tests/`, `.github/workflows/ci.yml` | `cargo test --workspace` passes without `3d`, the new CI step passes with `--features 3d`, and unit tests verify 3D task placement when only a non-`3d` worker is connected | Planned |
| F11-d | PR (d): Worker session loop merge (`D4`) | Merge TCP `handle_connection` and QUIC `handle_multiplexed_connection` into `WorkerSession` in `crates/master/src/session.rs` | `crates/master/src/session.rs`, `crates/master/src/server.rs`, `crates/master/src/lib.rs` | All six preserved invariants hold, and every `[[test]]` target of `rusty_grid_master` and `rusty_grid_worker` plus `test_integration.sh` and `run_chaos_test.sh` pass | Planned |

## Status log

- 2026-11-08: Draft written from `docs/BACKLOG.md` (`FEAT-11`) and `docs/ROADMAP.md` (decision Q2)
  at `master` `01a9786`.
- 2026-11-08: Updated per owner review (`118-fix`): moved sandbox locks to
  `<base_dir>/.locks/<task_id>.lock` acquired before creating the sandbox directory, changed
  `compute_intensity` from a wire removal to `#[deprecated]` until `0039` W1, added full 3D test
  target inventory, `--features 3d` CI step, and `ProtocolCapabilities::THREE_D` placement gating,
  documented the six preserved session invariants and full test gates for D4, noted `--keep-sandboxes`
  sweep behavior, and split implementation into four ordered PRs (`F11-a` through `F11-d`).
