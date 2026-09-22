# Project: OxideSwarm (formerly rusty_grid)

## Architecture
`OxideSwarm` (binary `rusty-grid` / `oxideswarm`) is a high-performance, open-source distributed computing framework in Rust designed for master-worker computing, distributed Rust crate compilation, and GPU-accelerated workloads across multiple connected machines (macOS, Linux, Windows, Android).

### Key Architecture Components:
1. **Cargo Workspace**:
   - `crates/core` (`rusty_grid_core`): Protocol wire types, `LengthDelimitedCodec` framing with 1-byte Format Discriminator (`0x02` Bincode, `0x01` Tagged JSON, `0x7B` Raw Legacy JSON), `is_human_readable()` dual-mode Serde for `WorkerMessage`, `MasterMessage`, `ClientMessage`, `ClientResponse`, `TaskSpec`, `TaskResult`, `WorkerCapabilities`, error handling.
   - `crates/master` (`rusty_grid_master`): Tokio TCP server with auto-codec negotiation, `WorkerRegistry` with idempotent unregistration, task queue with exponential backoff & 0ms graceful disconnect rescheduling, workload-aware scheduling engine (GPU routing constraint + spread batch scheduling + anti-stuttering host protection), client RPC server, embedded `axum` HTTP server for real-time web observability dashboard with WebSocket telemetry streaming.
   - `crates/worker` (`rusty_grid_worker`): Tokio TCP client with configurable wire codec (`bincode` default), capability detection (CPU cores, RAM, `--simulate-gpu` flag, mobile/Android constraints via `sysinfo`), heartbeat loop with CPU/RAM metrics, task execution runner (`tokio::process::Command`, sandbox directory isolation, timeout watchdog), simulated GPU executor. Cross-compilable for Android (`aarch64-linux-android`).
   - `crates/cli` (`rusty_grid_cli`): Unified binary `rusty-grid` with subcommands `master`, `worker`, `submit`, `status`, `workers`, supporting `--wire-codec`, `--dashboard-port`, `--dashboard-port-file`, and `--default-retry-max`.
2. **Dynamic Ephemeral Port Publication**:
   - Master supports `--listen 127.0.0.1:0` and writes bound port to `--port-file <path>` for zero-collision automated testing.
   - Dashboard supports `--dashboard-port 0` and writes bound port to `--dashboard-port-file <path>`.
3. **Android Worker Support & Mobile Capabilities**:
   - Target `aarch64-linux-android` support with `build_android_worker.sh`.
   - Android worker advertises battery, thermal throttling state, and SoC model in `WorkerCapabilities.mobile`.
4. **Native P2P NAT Traversal (`iroh`) & Lightweight Map/Reduce Pipeline**:
   - Embedded `iroh` (`iroh-net`) for native P2P QUIC + DERP relay hole-punching with persistent P2P Secret Key (`--p2p-key-file`).
   - Transport-agnostic binary wire protocol works seamlessly over TCP and `iroh` QUIC streams.
   - Lightweight, dependency-free Map/Reduce task distribution engine.
5. **Dynamic System Load Awareness & Host Throttling**:
   - Heartbeat telemetry includes real-time host metrics (`cpu_usage_pct`, `ram_used_mb`, `ram_total_mb` via `sysinfo`).
   - Master scheduler dynamically routes tasks away from heavily loaded hosts (>85% CPU), providing host stuttering protection.
6. **Fault-Tolerant Worker Eviction & Dynamic Task Rescheduling**:
   - Fast-path TCP EOF/error detection (<10ms) and slow-path heartbeat timeout reaper (10s).
   - In-flight task recovery with defense-in-depth sweeps across `worker_assignments` and active task entries.
   - Dynamic task rescheduling: 0ms immediate re-enqueue for graceful disconnects, exponential backoff for ungraceful crashes, retry tracking up to configurable maximum limit (`--default-retry-max` or `TaskRequirements.max_retries`), and descriptive terminal `Failed` state.
   - Waiters resolution in both server connection loop and reaper loop, ensuring client `wait_task` never hangs.
7. **Embedded Real-Time Web Observability Dashboard**:
   - Embedded `axum` HTTP server (flag `--dashboard-port <PORT>`).
   - Self-contained Single Page Application (`index.html` embedded via `include_str!`, zero external CDNs or npm dependencies).
   - Real-time telemetry streaming via WebSockets (`/ws` and `/api/stream`) broadcasting initial `ClusterSnapshot` followed by incremental updates.
   - REST API endpoints (`/api/status`, `/api/workers`, `/api/tasks`, `/api/tasks/:task_id`).
8. **Automated Integration Testing & Verification**:
   - Standalone bash script `test_integration.sh` verifying AC1–AC5 end-to-end.
   - Rust integration test suite `tests/e2e_cluster.rs` with RAII child process teardown.
   - Automated worker crash recovery and dynamic rescheduling integration test (`tests/worker_crash_rescheduling_test.rs`).
   - Automated dashboard HTTP endpoints & WebSocket stream integration test (`tests/dashboard_integration.rs`).
   - Programmatic serialization performance benchmark (`crates/core/tests/serialization_benchmark.rs`).

---

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Wire Protocol & Framing | Tokio TCP 4-byte length-delimited framing with JSON serialization (`WorkerMessage`, `MasterMessage`) | M1 | Survey (Explorer 1) |
| 2 | Capabilities & Handshake | `WorkerCapabilities` (CPU cores, RAM, GPU presence, simulated GPU flag); UUID registration handshake | M1, M2 | Survey (Explorer 1 & 2) |
| 3 | Task Schema & FSM | Typed `TaskSpec` (Command, ShellScript, RustCompilation, GpuCompute), `TaskRequirements`, Master 8-state FSM | M1, M4 | Survey (Explorer 2) |
| 4 | Connection Maintenance | 3s heartbeat, 10s timeout reaper, immediate TCP EOF disconnect handling, exponential backoff reconnect | M2 | Survey (Explorer 1) |
| 5 | Worker Execution & Sandbox | `tokio::process::Command` runner, sandbox directory isolation, stdout/stderr capture, timeout watchdog, simulated GPU compute | M3 | Survey (Explorer 2) |
| 6 | Workload-Specific Routing | Strict GPU capability filtering (`gpu_required -> GPU workers ONLY`); non-GPU workers never receive GPU tasks | M4 | Survey (Explorer 2) |
| 7 | Parallel Batch Spread Scheduling | Spread scheduling across all eligible idle/least-loaded workers for independent sub-tasks (e.g. 5 compilation tasks) | M4 | Survey (Explorer 2) |
| 8 | Unified CLI & Advanced Node Configuration | `rusty-grid` binary (`master`, `worker`, `submit`, `status`, `workers`), robust config (CLI flags, env vars, config files), custom IP/ports, adjustable heartbeat timeouts, worker concurrency limits, manual hardware capability overrides (cores, GPU toggle), `--port-file` | M5 | Survey (Explorer 1 & 3), User Follow-up |
| 9 | Automated Integration Script | `test_integration.sh` executing AC1–AC5 end-to-end with 1 Master and 3 Workers (1 simulated GPU) | M5 | Survey (Explorer 3) |
| 10 | 4-Tier E2E Integration Suite | Native Rust E2E test suite (`tests/e2e_cluster.rs`) covering Tiers 1–4 (Category-Partition, BVA, Pairwise, Real-World) | E2E Track, M6 | Survey (Explorer 3) |
| 11 | Final E2E Pass & Tier 5 Hardening | 100% E2E test suite pass + Tier 5 white-box adversarial coverage hardening and audit | M6 | Orchestrator Protocol |
| 12 | Android Worker Support | Cross-compilable worker for Android (`aarch64-linux-android`), advertising mobile capabilities (SoC, cores, battery/mobile constraints, mobile GPU) | M5 | User Follow-up |
| 13 | Native P2P NAT Traversal (`iroh`) | Embedded `iroh` QUIC/DERP hole punching for out-of-the-box remote internet connectivity without port forwarding or VPNs | M5 | User Follow-up |
| 14 | Dynamic System Load Awareness | Real-time host CPU & RAM utilization reported in Worker heartbeats (via sysinfo) and used by Master Scheduler to route tasks away from heavily loaded hosts | M5 | User Follow-up |
| 15 | Lightweight Map/Reduce Pipeline | Pure in-memory Map/Reduce task orchestration (inspired by Paladin) without external dependencies (no Redis) | M5 | User Follow-up |
| 16 | Windows Worker Cross-Compilation | Cross-compilation harness (`build_windows_worker.sh`) to compile Windows `.exe` worker binaries (`x86_64-pc-windows-gnu`) directly from macOS | M5 | User Follow-up |
| 17 | Open-Source Documentation & README | Comprehensive public `README.md` covering architecture, crate diagrams, features, macOS/Windows/Android usage, and testing | M5 | User Follow-up |
| 18 | High-Performance Binary Wire Protocol | Bincode wire serialization codec with `is_human_readable()` dual-mode Serde, 1-byte format discriminator (`0x02` Bincode, `0x01` Tagged JSON, `0x7B` Raw Legacy JSON), auto-detection on Master, 100% backward compatibility | M8 | Survey 5.1 (R1) |
| 19 | Serialization Performance Benchmark | Programmatic comparison benchmark verifying measurable throughput speedup (3x-8x) and payload reduction (~78% on binary data) for Bincode vs JSON | M8 | Survey 5.1 (R1, R4) |
| 20 | Fault-Tolerant Worker Eviction & Task Recovery | Idempotent worker unregistration upon TCP EOF, crash, or 10s reaper heartbeat timeout; defensive extraction of all active tasks from `worker_assignments` and `tasks` map | M9 | Survey 5.2 (R2) |
| 21 | Dynamic Task Rescheduling & Retry Engine | Configurable cluster-level `--default-retry-max`, per-task `TaskRequirements.max_retries` override, 0ms immediate re-enqueue for graceful disconnects, exponential backoff for crashes, descriptive terminal failure on max retries, reaper client waiter resolution | M9 | Survey 5.2 (R2) |
| 22 | Embedded Real-Time Web Observability Dashboard | Embedded `axum` HTTP server in Master (flags `--dashboard-port`, `--dashboard-port-file`), self-contained Single Page Application (`index.html` via `include_str!`, zero external CDNs or npm dependencies), dark industrial cockpit UI, live worker telemetry CPU/RAM/GPU/mobile, task queue filtering | M10 | Survey 5.3 (R3) |
| 23 | Real-Time Telemetry Streaming & REST APIs | WebSocket endpoint `/ws` and `/api/stream` backed by non-blocking broadcast ring buffer, initial `ClusterSnapshot` frame, live event updates (`WorkerHeartbeat`, `TaskUpdated`, `StatsUpdated`), REST endpoints `/api/status`, `/api/workers`, `/api/tasks`, `/api/tasks/:task_id` | M10 | Survey 5.3 (R3) |
| 24 | Comprehensive Integration Test Suite & Regression Prevention | Automated worker crash recovery & dynamic task rescheduling integration test, automated dashboard REST endpoints and WebSocket stream integration test, serialization benchmark test, 100% full workspace test suite pass | M11 | Survey 5.1, 5.2, 5.3 (R4) |

---

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Workspace Setup & Core Protocol | Setup Cargo workspace, create `crates/core` with wire messages, framing codec, capabilities, task types, serialization tests | none | DONE |
| M2 | Master-Worker Registration & Lifecycle | `crates/master` TCP listener & `WorkerRegistry`; `crates/worker` client, `--simulate-gpu`, heartbeat & reaper loop | M1 | DONE |
| M3 | Worker Task Execution & Sandboxing | `crates/worker` task runner, sandbox isolation, process execution, stdout/stderr capture, timeout watchdog, simulated GPU executor | M1, M2 | DONE |
| M4 | Master Scheduling & Workload Routing | `crates/master` task queue, FSM, strict GPU routing constraint, parallel batch spread scheduling across workers | M1, M2, M3 | DONE |
| M5 | Unified CLI, P2P NAT, Cross-Platform & `test_integration.sh` | `crates/cli` binary `rusty-grid` with full configurability (flags/envs/config, IP/ports, heartbeat timeouts, worker max concurrency, hardware overrides), Android (`build_android_worker.sh`) & Windows (`build_windows_worker.sh`) cross-compilation harnesses, embedded `iroh` P2P NAT traversal, lightweight Map/Reduce, standalone `test_integration.sh`, public `README.md` | M1, M2, M3, M4 | DONE |
| M6 | E2E Test Pass (100%) & Coverage Hardening | Pass 100% of E2E test suite (Tiers 1–4) and perform Tier 5 adversarial coverage hardening | M5, E2E Track | DONE |
| M7 | P2P Secret Key Persistence & Ticket Stability | Persistent `iroh::SecretKey` via `--p2p-key-file`, ticket invariance across restarts, early key validation, worker endpoint caching | M5 | DONE |
| M8 | High-Performance Binary Wire Protocol | Add `bincode = "1.3"`, implement `is_human_readable()` dual-mode Serde for `TaskSpec`, `WorkerMessage`, `MasterMessage`, `ClientMessage`, `ClientResponse`, 1-byte format discriminator (`0x02` Bincode, `0x01` JSON, `0x7B` Raw JSON) in `MessageTransport`, auto-detection in Master, worker `--wire-codec`, serialization benchmark test | none | DONE |
| M9 | Fault-Tolerant Worker Lifecycle & Dynamic Task Rescheduling | Idempotent worker unregistration, defense-in-depth task recovery, dynamic task rescheduling with retry tracking (`retries: u32`, cluster default `--default-retry-max`, per-task `TaskRequirements.max_retries`), 0ms immediate re-enqueue for graceful disconnects, exponential backoff for crashes, terminal failure with descriptive reason, reaper client waiters resolution | M8 | DONE |
| M10 | Embedded Real-Time Web Observability Dashboard | Add `axum` v0.7 + `tower-http`, implement `create_dashboard_router`, embedded SPA (`index.html` via `include_str!`), WebSocket telemetry stream (`/ws`, `/api/stream`) with broadcast channel, REST endpoints (`/api/status`, `/api/workers`, `/api/tasks`), Master `--dashboard-port` & `--dashboard-port-file` | M8, M9 | DONE |
| M11 | Comprehensive Integration Testing & Verification | Automated worker crash recovery integration test, automated dashboard HTTP/WebSocket integration test, serialization benchmark test, full regression verification (`cargo test --workspace` passes 100%) | M8, M9, M10 | DONE |

---

## Interface Contracts

### 1. Wire Protocol & Serialization (`crates/core/src/protocol.rs`)
- **Framing**: `tokio_util::codec::LengthDelimitedCodec` (4-byte length prefix, max frame 64 MB).
- **Format Discriminator Byte** (First byte of frame payload):
  - `0x02` (`WIRE_FORMAT_BINCODE`): Payload encoded with Bincode (compact binary).
  - `0x01` (`WIRE_FORMAT_JSON`): Payload encoded with `serde_json` (tagged JSON).
  - `0x7B` / whitespace (`Raw Legacy JSON`): Entire frame payload parsed with `serde_json`.
- **Dual-Mode Serde**:
  - `serializer.is_human_readable() == true` -> Uses `#[serde(tag = "type")]` for 100% JSON backward compatibility.
  - `serializer.is_human_readable() == false` -> Uses compact index discriminant for high-throughput Bincode serialization.
- **Messages**:
  - `WorkerMessage`: `Register`, `Heartbeat`, `TaskProgress`, `TaskResult`, `Disconnecting`.
  - `MasterMessage`: `RegisterAck`, `HeartbeatAck`, `AssignTask`, `CancelTask`.
  - `ClientMessage`: `SubmitTask`, `GetTaskStatus`, `ListWorkers`, `CancelTask`.
  - `ClientResponse`: `TaskSubmitted`, `TaskStatus`, `WorkerList`, `TaskCancelled`, `Error`.

### 2. Task Requirements & Retry Contract (`crates/core/src/task.rs`)
```rust
pub struct TaskRequirements {
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub gpu_required: bool,
    pub timeout_secs: u64,
    pub max_retries: Option<u32>, // None inherits cluster default (--default-retry-max, default: 3)
}
```

### 3. CLI Arguments Contract (`rusty-grid`)
- `rusty-grid master`:
  - `--listen <ADDR>` (supports `127.0.0.1:0`)
  - `--port-file <PATH>`
  - `--wire-codec <bincode|json>` (default: `bincode`)
  - `--default-retry-max <N>` (default: 3)
  - `--dashboard-port <PORT>` (supports `0` for ephemeral)
  - `--dashboard-port-file <PATH>`
  - `--p2p-key-file <PATH>`
- `rusty-grid worker`:
  - `--master <ADDR>`
  - `--wire-codec <bincode|json>` (default: `bincode`)
  - `--simulate-gpu`
  - `--cores <N>`, `--override-ram-mb <MB>`
- `rusty-grid submit --master <ADDR> --type <generic|gpu|compile> [--command <CMD>] [--wait] [--json]`
- `rusty-grid status --master <ADDR> [--task-id <UUID>] [--json]`
- `rusty-grid workers --master <ADDR> [--json]`

### 4. Dashboard REST & WebSocket Contract
- `GET /`: Self-contained HTML cockpit UI (`include_str!("dashboard/index.html")`).
- `GET /api/status`: JSON `ClusterStatusDto` (version, uptime, worker counts, task queue counts).
- `GET /api/workers`: JSON `Vec<WorkerInfo>` (id, status, capabilities, live cpu_usage_pct, ram_available_mb, active_tasks).
- `GET /api/tasks[?state=<state>&limit=<n>]`: JSON `Vec<TaskInfo>`.
- `GET /api/tasks/:task_id`: JSON `TaskInfo` or 404.
- `GET /ws` & `GET /api/stream`: WebSocket upgrading to live telemetry stream:
  - Initial frame: `DashboardStreamMessage::Snapshot(ClusterSnapshotDto)`.
  - Subsequent frames: `WorkerHeartbeat`, `WorkerRegistered`, `WorkerDisconnected`, `TaskUpdated`, `StatsUpdated`.

---

## Code Layout
```
d:\teamwork_projects\OxideSwarm
├── Cargo.toml                  # Workspace root (dependencies: bincode, axum, tower-http, etc.)
├── test_integration.sh         # Standalone bash integration test harness (AC1-AC5)
├── crates/
│   ├── core/
│   │   ├── Cargo.toml          # bincode, serde, serde_json, bytes
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── protocol.rs     # WireCodec, format discriminator (0x02/0x01/0x7B), dual-mode Serde
│   │   │   ├── capabilities.rs # WorkerCapabilities, MobileCapabilities
│   │   │   ├── task.rs         # Task, TaskId, TaskSpec, TaskRequirements (with max_retries), TaskResult
│   │   │   ├── transport.rs    # GridStream (TCP, Mock, P2P iroh)
│   │   │   └── error.rs        # GridError, ProtocolError
│   │   └── tests/
│   │       └── serialization_benchmark.rs # Programmatic comparison test (Bincode vs JSON)
│   ├── master/
│   │   ├── Cargo.toml          # axum, tower-http, tokio
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── server.rs       # TCP listener, auto-codec negotiation, dashboard server spawn
│   │       ├── registry.rs     # WorkerRegistry, idempotent unregister, active_tasks reset
│   │       ├── queue.rs        # TaskQueue, retry_count tracking, 0ms graceful re-enqueue, terminal failure
│   │       ├── reaper.rs       # Heartbeat reaper, stale worker eviction, waiter resolution
│   │       ├── scheduler.rs    # Workload-specific routing (GPU filter, spread scheduling, anti-stuttering)
│   │       └── dashboard/      # Embedded Web Dashboard module
│   │           ├── mod.rs      # DashboardState, create_dashboard_router, axum routes
│   │           ├── dto.rs      # ClusterStatusDto, ClusterSnapshotDto, DashboardStreamMessage
│   │           └── index.html  # Embedded dark industrial Single-Page Application
│   ├── worker/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs       # TCP client connection, wire_codec configuration, graceful disconnect
│   │       ├── heartbeat.rs    # Heartbeat sender (cpu_usage_pct, ram_available_mb via sysinfo)
│   │       └── runner.rs       # Task execution, sandboxing, simulated GPU executor
│   └── cli/
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs         # Unified CLI (clap): master, worker, submit, status, workers
│           └── config.rs       # Configuration hierarchy
└── tests/
    ├── e2e_cluster.rs                      # Native Rust 4-tier integration test suite
    ├── worker_crash_rescheduling_test.rs   # Automated worker kill & task reassignment integration test
    └── dashboard_integration.rs            # Automated dashboard HTTP & WebSocket stream integration test
```
