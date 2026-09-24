# Technical Blueprint: OxideSwarm Architectural Upgrade & Optimization

**Status**: Consolidated Architectural Plan  
**Governing Document**: `.agents/teamwork/ORIGINAL_REQUEST.md` (Requirements R1–R6)  
**Authors**: Survey Team (Explorer 1, Explorer 2, Explorer 3) & Project Orchestrator  
**Date**: 2026-09-24  

---

## 1. Architectural Overview & System Decomposition

OxideSwarm is a distributed supercomputing grid and coding agent mesh designed for cross-platform compute execution, distributed compilation, and autonomous agent collaboration. This blueprint integrates the technical surveys into a unified, phased implementation roadmap across 6 key requirements:

```
OxideSwarm Architecture
├── Layer 1: Rust Core Engine & Networking
│   ├── crates/core: Zero-copy `bytes::Bytes` I/O, WireCodecs, TaskCheckpoint protocol, QUIC transport
│   ├── crates/master: Scheduler micro-batching, checkpoint retention & delta failover, unified Axum server (:8080)
│   ├── crates/worker: Modular `wgpu` compute shaders, deterministic CPU SIMD fallback, bounded stream capture
│   └── crates/cli: Multi-binary targets (`rusty-grid`, `oxideswarm`), RBAC auth configuration flags
├── Layer 2: Mobile & Packaging Bridge
│   ├── crates/android_bridge: JNI C-dynamic library, Android foreground service bindings
│   └── packaging/: System service automation (Windows Service, macOS launchd, Termux/Android daemon)
├── Layer 3: Coding Agent Mesh Coordination
│   ├── crates/agent_mesh: Axum WebSocket Relay Hub, AgentMeshClient, process sandboxing
│   └── scripts/agent_node.py: Multi-platform Python agent node
├── Layer 4: Inter-Layer Convergence & Security
│   ├── AgentGridBridge: In-process translation bridge between Layer 3 (Mesh) and Layer 1 (Master)
│   └── Zero-Trust RBAC Middleware: Granular permission scopes (Admin, Worker, Submitter, Observer) with dev bypass
```

---

## 2. Milestone Decomposition & Work Breakdown Structure

### Milestone 1 (M1): Web Observability & Zero-Trust RBAC (R1 & R5)
**Scope**:
1. **Consolidated Single Server**: Unify `crates/master/src/dashboard` and `crates/master/src/web_ui.rs` into a single Axum HTTP listener bound to port `:8080` (or `dashboard_port` when overridden).
2. **Real-Time Telemetry Streaming**: Port `/ws`, `/api/stream`, and `/api/stream/sse` into `crates/master/src/web_ui.rs`, driven by the Master `broadcast_tx` channel.
3. **Route & DTO Standardization**:
   - Standardize REST route syntax to `:task_id` (`/api/tasks/:task_id`). Return HTTP 404 for invalid/non-existent UUIDs.
   - Harmonize `/api/status` DTOs with custom deserializer in `WorkerSummaryDto` so that both `ClusterStatusDto` (for `dashboard_integration.rs`) and `DashboardStatus` (for `dashboard.html` / `test_m4_dashboard_telemetry.rs`) deserialize cleanly.
4. **Zero-Trust Security & RBAC Middleware (`crates/master/src/auth.rs`)**:
   - Permission scopes: `Admin`, `Worker`, `Submitter`, `Observer`.
   - Credential extraction: `Authorization: Bearer <token>`, `X-API-Key: <key>`, `?token=<token>`.
   - Local development bypass: Automatic `Admin` scope for loopback (`127.0.0.1` / `::1`) in `--dev` mode.
5. **Compiler Warnings Cleanliness**:
   - Annotate `unused_mut` in `server.rs:463`.
   - Feature-gate `pub mod web_ui;` behind `#[cfg(feature = "dashboard")]` in `crates/master/src/lib.rs`.
   - Verify `cargo check --workspace --all-targets` and `cargo check -p rusty_grid_master --no-default-features` pass with 0 errors and 0 warnings.

### Milestone 2 (M2): High-Performance Zero-Copy I/O & QUIC Stream Multiplexing (R2)
**Scope**:
1. **Zero-Copy Buffers (`bytes::Bytes`)**:
   - Refactor `TaskResult.stdout` and `TaskResult.stderr` to `bytes::Bytes` in `crates/core/src/task.rs`.
   - Refactor `TaskSpec::Command.stdin` and `TaskSpec::GpuCompute.input_data` to `bytes::Bytes`.
   - Refactor `read_bounded_stream` in `crates/worker/src/runner.rs` to accumulate in `bytes::BytesMut` and `.freeze()`.
   - Provide backward-compatible string accessors (`stdout_str()`, `stderr_str()`) on `TaskResult`.
2. **Bidirectional QUIC Stream Multiplexing**:
   - In `crates/core/src/transport.rs`, `crates/worker/src/client.rs`, and `crates/master/src/server.rs`, multiplex two distinct bidirectional streams over `iroh::endpoint::Connection`:
     - Stream 1 (`0x01`): Dedicated Control / Heartbeat Stream (high priority).
     - Stream 2 (`0x02`): Dedicated Task Data & Result Stream (bulk priority).
   - Guarantees heartbeat telemetry is never blocked by multi-megabyte task outputs or compiler archives.
3. **Scheduler Micro-Batching**:
   - In `crates/master/src/scheduler.rs`, implement a 5ms debounce micro-batch accumulation window under burst task submissions.
   - In `crates/master/src/queue.rs`, add `schedule_tasks_batch` to atomically transition up to 128 queued tasks to `Scheduled` under a single write lock acquisition.

### Milestone 3 (M3): Fault-Tolerant Mid-Task Checkpointing & Delta Resumption (R3)
**Scope**:
1. **Wire Protocol Checkpointing Extension**:
   - Define `TaskCheckpoint` struct (task_id, sequence, progress_pct, state_delta: Bytes, created_at_utc, description) in `crates/core/src/task.rs`.
   - Add `WorkerMessage::Checkpoint { worker_id, checkpoint }` in `crates/core/src/protocol.rs`.
   - Add `MasterMessage::AssignTaskWithCheckpoint { task, checkpoint }` in `crates/core/src/protocol.rs`.
   - Implement lossless serialization across both JSON and Bincode codecs.
2. **Master TaskQueue State Retention**:
   - Add `latest_checkpoint: Option<TaskCheckpoint>` to `TaskEntry` in `crates/master/src/queue.rs`.
   - Implement `record_checkpoint` and `get_checkpoint` in `TaskQueue`.
   - Maintain `latest_checkpoint` during worker disconnection / task re-queueing in `handle_worker_disconnected`.
3. **Scheduler Resumption Dispatch**:
   - In `crates/master/src/scheduler.rs`, when dispatching a task that possesses a `latest_checkpoint`, issue `MasterMessage::AssignTaskWithCheckpoint`.
4. **Worker Runner Delta Execution**:
   - In `crates/worker/src/runner.rs`, accept `Option<&TaskCheckpoint>` in `execute_task`.
   - Emit `WorkerMessage::Checkpoint` periodically during iterative tasks.
   - On resumption, decode state delta and resume from the saved iteration rather than 0%.

### Milestone 4 (M4): Cross-Platform Hardware-Accelerated Compute Shaders (`wgpu`) (R4)
**Scope**:
1. **Modular Feature Gating**:
   - In `crates/worker/Cargo.toml`, declare feature `gpu-wgpu = ["dep:wgpu", "dep:pollster"]` with cached `wgpu = "22"` and `pollster = "0.3"`.
2. **WGSL Compute Shaders**:
   - Tiled matrix multiplication kernel (`gemm.wgsl`) with 16x16 workgroups.
   - Verification hashing / parallel reduction kernel (`hash.wgsl`).
3. **Cross-Platform Hardware Adapter Negotiation**:
   - Support Metal (macOS), DirectX 12 / Vulkan (Windows), and Vulkan (Linux/Android).
4. **Deterministic CPU SIMD Fallback (`cpu_simd.rs`)**:
   - Unrolled 8-wide chunked matrix multiplication ensuring exact mathematical equivalence with reference oracle.
   - Automatic fallback when GPU adapter is unavailable or in `--simulate-gpu` mode.
5. **Structured Device Telemetry in `TaskResult`**:
   - Add `device_name: Option<String>` to `TaskResult` and stdout formatting (`Device: ...`, `Execution Time: ... ms`, `Status: VERIFIED_OK`).

### Milestone 5 (M5): Ecosystem Convergence & In-Process Agent Mesh Bridge (R6)
**Scope**:
1. **In-Process Bridge (`AgentGridBridge`)**:
   - Connect Layer 3 (`AgentMeshHub`) directly to Layer 1 (`MasterHandle`).
   - Intercept grid-targeted envelopes (`grid_compute`, `grid_compile`, `grid_submit`, `grid_status`).
   - Translate envelopes into `Task` instances, submit to `MasterHandle`, await completion, and return `CommandResponse`.
2. **Client Helper Extensions**:
   - Add `submit_grid_compute` and `submit_grid_compilation` to `AgentMeshClient` (`crates/agent_mesh/src/client.rs`).
   - Add `submit_grid_compute` and `submit_grid_compile` to `scripts/agent_node.py`.
3. **Integration Test (`tests/test_ecosystem_convergence.rs`)**:
   - End-to-end verification of Python and Rust coding agents submitting compute and compilation workloads to the grid.

### Milestone 6 (M6): Comprehensive Turnkey Verification & Hardening
**Scope**:
1. **Cargo Workspace Verification**:
   - `cargo check --workspace --all-targets` (exit code 0, 0 warnings).
   - `cargo test --workspace` (100% test pass).
2. **Python Agent Mesh & Challenger Verification**:
   - `python tests/run_all_mesh_tests.py` (100% pass across all suites).
   - `python tests/test_challenger_bidirectional_burst.py` (0.00% packet loss).
   - `python tests/test_challenger_process_tree_termination.py` (0 orphan processes).
   - `python tests/test_challenger_subprocess_safety.py` (exit codes & error safety).
3. **Forensic Integrity Audit**:
   - Systematic check of all implemented modules to verify authentic logic with zero dummy facades or hardcoded bypasses.
4. **Sentinel Final Report**:
   - Comprehensive handoff to Sentinel signaling project victory.

---

## 3. Dependency Graph & Execution Sequence

```
[M1: Web Observability & RBAC] ──────┐
                                     ├──► [M5: Ecosystem Convergence Bridge] ──► [M6: Turnkey Verification]
[M2: Zero-Copy I/O & QUIC] ──┐       │
                             ├──► [M4: wgpu Shaders] ──┘
[M3: Checkpointing Resumption] ─┘
```

1. **M1 (R1 & R5)** and **M2 (R2)** can be initiated in parallel as they touch orthogonal modules (`web_ui/server/auth` vs `core/transport/runner`).
2. **M3 (R3 Checkpointing)** builds on `bytes::Bytes` from M2.
3. **M4 (R4 wgpu Shaders)** builds on runner compute execution.
4. **M5 (R6 Ecosystem Bridge)** bridges the unified Master scheduler to the Agent Mesh.
5. **M6 (Verification)** runs the full multi-tier verification suite and forensic audit.
