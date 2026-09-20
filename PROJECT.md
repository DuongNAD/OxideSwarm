# Project: OxideSwarm (formerly rusty_grid)

## Architecture
`OxideSwarm` (binary `oxideswarm`, backwards-compatible alias `rusty-grid`) is a high-performance, open-source distributed computing framework in Rust designed for master-worker computing, distributed Rust crate compilation, and GPU-accelerated workloads across multiple connected machines (macOS, Linux, Windows, Android).

### Key Architecture Components:
1. **Cargo Workspace**:
   - `crates/core` (`rusty_grid_core`): Protocol wire types, `LengthDelimitedCodec` framing, `WorkerCapabilities`, `Task` and `TaskResult` definitions, error handling.
   - `crates/master` (`rusty_grid_master`): Tokio TCP server, `WorkerRegistry`, task queue, workload-aware scheduling engine (GPU routing constraint + spread batch scheduling), client RPC server.
   - `crates/worker` (`rusty_grid_worker`): Tokio TCP client, capability detection (CPU cores, RAM, `--simulate-gpu` flag, mobile/Android constraints), heartbeat loop, task execution runner (`tokio::process::Command`, sandbox directory isolation, timeout watchdog), simulated GPU executor. Cross-compilable for Android (`aarch64-linux-android`).
   - `crates/cli` (`rusty_grid_cli`): Unified binary `rusty-grid` with subcommands `master`, `worker`, `submit`, `status`, `workers`.
2. **Dynamic Ephemeral Port Publication**:
   - Master supports `--listen 127.0.0.1:0` and writes bound port to `--port-file <path>` for zero-collision automated testing.
3. **Android Worker Support & Mobile Capabilities**:
   - Target `aarch64-linux-android` support with `build_android_worker.sh`.
   - Android worker advertises battery, thermal throttling state, and SoC model in `WorkerCapabilities.mobile`.
4. **Native P2P NAT Traversal (`iroh`) & Lightweight Map/Reduce Pipeline**:
   - Embedded `iroh` (`iroh-net`) for native P2P QUIC + DERP relay hole-punching, enabling out-of-the-box secure remote connectivity across the internet without port forwarding or VPNs.
   - Lightweight, dependency-free Map/Reduce task distribution engine (inspired by `Paladin`, using purely in-memory Rust channels and Tokio async tasks without Redis).
5. **Dynamic System Load Awareness & Host Throttling**:
   - Heartbeat telemetry includes real-time host metrics (`cpu_usage_pct`, `ram_used_mb`, `ram_total_mb` via `sysinfo`).
   - Master scheduler dynamically routes tasks away from heavily loaded hosts (e.g., during active gaming or Blender rendering), providing host stuttering protection and graceful backpressure.
6. **Automated Integration Testing & Verification**:
   - Standalone bash script `test_integration.sh` verifying AC1–AC5 with 1 Master and 3 Workers (1 simulated GPU).
   - Rust integration test suite `tests/e2e_cluster.rs` with RAII child process teardown (`ClusterHarness`).

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

---

## Interface Contracts

### 1. Wire Protocol: `crates/core` ↔ `crates/master` & `crates/worker`
- **Framing**: `tokio_util::codec::LengthDelimitedCodec` (4-byte length prefix, max frame 64 MB).
- **Serialization**: `serde_json`.
- **Worker -> Master Messages (`WorkerMessage`)**:
  - `Register { worker_id: Uuid, capabilities: WorkerCapabilities }`
  - `Heartbeat { worker_id: Uuid, timestamp: u64, active_tasks: usize }`
  - `TaskProgress { worker_id: Uuid, task_id: Uuid, status: TaskStatus }`
  - `TaskResult { worker_id: Uuid, task_id: Uuid, exit_code: i32, stdout: String, stderr: String, execution_time_ms: u64, is_gpu_executed: bool, error: Option<String> }`
- **Master -> Worker Messages (`MasterMessage`)**:
  - `RegisterAck { accepted: bool, worker_id: Uuid, heartbeat_interval_secs: u64, message: Option<String> }`
  - `HeartbeatAck { timestamp: u64 }`
  - `AssignTask { task: Task }`
  - `CancelTask { task_id: Uuid }`

### 2. Capabilities Contract: `WorkerCapabilities`
```rust
pub struct MobileCapabilities {
    pub os_version: String,
    pub soc_model: String,
    pub battery_pct: Option<u8>,
    pub is_charging: Option<bool>,
    pub thermal_throttled: bool,
}

pub struct WorkerCapabilities {
    pub name: String,
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub has_gpu: bool,
    pub is_simulated_gpu: bool,
    pub gpu_device_name: Option<String>,
    pub mobile: Option<MobileCapabilities>,
}
```

### 3. Task Specification: `Task` & `TaskRequirements`
```rust
pub struct TaskRequirements {
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub gpu_required: bool,
    pub timeout_secs: u64,
}

pub enum TaskSpec {
    Command { command: String, args: Vec<String>, env: HashMap<String, String> },
    ShellScript { script: String },
    RustCompilation { crate_name: String, source_files: HashMap<String, String>, cargo_args: Vec<String> },
    GpuCompute { kernel_name: String, input_data: Vec<u8>, compute_intensity: u32 },
    BuiltinTest { duration_ms: u64, should_fail: bool, require_gpu: bool },
}
```

### 4. CLI Contract (`rusty-grid`)
- `rusty-grid master --listen <ADDR> [--port-file <PATH>] [--heartbeat-timeout-secs <SECS>] [--config <PATH>]`
- `rusty-grid worker --master <ADDR> [--name <NAME>] [--cores <N>] [--simulate-gpu] [--max-concurrent-tasks <N>] [--override-ram-mb <MB>] [--heartbeat-interval-secs <SECS>] [--config <PATH>]`
- `rusty-grid submit --master <ADDR> --type <generic|gpu|compile> [--command <CMD>] [--wait] [--json]`
- `rusty-grid workers --master <ADDR> [--json]`
- `rusty-grid status --master <ADDR> [--task-id <UUID>] [--json]`
- Configuration sources: CLI arguments > Environment variables (`RUSTY_GRID_*`) > Configuration files (TOML/JSON via `--config`).
- Manual hardware overrides: `--cores <N>` forces CPU core count, `--simulate-gpu` toggles GPU presence, `--override-ram-mb <MB>` overrides RAM advertising.

---

## Code Layout
```
/Volumes/KINGSTON/teamwork_projects/rusty_grid
├── Cargo.toml                  # Workspace root
├── test_integration.sh         # Standalone bash integration test harness (AC1-AC5)
├── crates/
│   ├── core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── protocol.rs     # MasterMessage, WorkerMessage, LengthDelimited framing
│   │       ├── capabilities.rs # WorkerCapabilities, hardware detection
│   │       ├── task.rs         # Task, TaskId, TaskSpec, TaskRequirements, TaskResult
│   │       └── error.rs        # Common error types
│   ├── master/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── server.rs       # TCP listener & connection dispatcher
│   │       ├── registry.rs     # WorkerRegistry, state tracking
│   │       ├── queue.rs        # TaskQueue, FSM transitions
│   │       └── scheduler.rs    # Workload-specific routing (GPU filter, spread scheduling)
│   ├── worker/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs       # TCP client connection & reconnect loop
│   │       ├── heartbeat.rs    # Heartbeat sender & liveness
│   │       └── runner.rs       # Process execution, sandboxing, simulated GPU executor
│   └── cli/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs         # Unified CLI (clap): master, worker, submit, status, workers
└── tests/
    └── e2e_cluster.rs          # Native Rust 4-tier integration test suite
```
