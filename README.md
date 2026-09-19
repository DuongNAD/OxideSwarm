# OxideSwarm

<div align="center">

![OxideSwarm Logo](https://raw.githubusercontent.com/rust-lang/rust-artwork/master/logo/rust-logo-blk.svg)

**High-Performance Distributed Computing Grid & Workload Orchestrator in Rust**

[![Rust 2021](https://img.shields.io/badge/Rust-2021_Edition-orange.svg?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg?style=flat-square)](LICENSE)
[![CI Status](https://img.shields.io/badge/CI-Passing-brightgreen.svg?style=flat-square&logo=github-actions)](test_integration.sh)
[![P2P NAT](https://img.shields.io/badge/P2P%20NAT-iroh%20QUIC%2FDERP-purple.svg?style=flat-square)](https://iroh.computer)
[![Platforms](https://img.shields.io/badge/Platforms-macOS%20|%20Linux%20|%20Windows%20|%20Android-lightgrey.svg?style=flat-square)](#cross-platform-worker-support)

</div>

---

## Overview

**OxideSwarm** (formerly `rusty_grid`) is an enterprise-grade, memory-safe distributed computing framework engineered from the ground up in Rust. It coordinates general-purpose compute jobs, distributed Rust crate compilation workloads, GPU-accelerated pipelines, and in-memory Map/Reduce data processing across heterogeneous device clusters.

Designed to turn your spare laptops, desktop gaming rigs, cloud virtual machines, and mobile devices into a unified, self-balancing computational swarm, OxideSwarm delivers production capabilities without external broker dependencies:

* **Zero-Broker Architecture**: No external message queues (Kafka, RabbitMQ) or distributed key-value stores (Redis, Zookeeper). The cluster runs 100% self-contained using asynchronous Tokio actors, pure in-memory state channels, and length-delimited TCP/QUIC streams.
* **Native P2P NAT Traversal**: Embedded **`iroh`** QUIC networking with automated DERP relay hole-punching. Connect worker nodes located anywhere across the internet directly to the master coordinator without opening router ports, setting up static public IPs, or installing 3rd-party VPNs (such as Tailscale or WireGuard).
* **Dynamic System Load Awareness & Host Anti-Stuttering**: Real-time host CPU and RAM telemetry sampled via `sysinfo` within periodic worker heartbeats. The master scheduler applies automatic backpressure, dynamically throttling tasks away from host workstations engaged in heavy external tasks (e.g., gaming or 3D rendering in Blender).
* **Workload-Specific Routing & Strict GPU Isolation**: Hardware requirements are dual-gated. GPU tasks are dispatched *strictly* to workers advertising verified physical or simulated GPU capabilities; non-GPU workers are mathematically excluded from receiving GPU compute kernels.
* **Paladin-Inspired In-Memory Map/Reduce**: Distributed Map/Reduce engine supporting built-in aggregation operators (`word_count`, `line_count`, `identity`) as well as arbitrary shell scripts and executable binaries with parallel chunk partitioning and memory-buffered shuffle stages.
* **Universal Multi-Platform Support**: Unified binary (`oxideswarm`, preserved with `rusty-grid` backwards compatibility) running natively on macOS, Linux, Windows 64-bit (`.exe`), and Android mobile devices (`aarch64-linux-android`).

---

## Architectural Topology

```
                              ┌─────────────────────────────────────────────────────────┐
                              │                    OxideSwarm Client                     │
                              │           (CLI / Submitter / RPC Controller)            │
                              └────────────────────────────┬────────────────────────────┘
                                                           │
                                                           │  Client RPC Stream
                                                           ▼  (Submit / Status / Workers / MapReduce)
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                 OxideSwarm Master Node                                                 │
│                                                                                                                        │
│   ┌───────────────────────────────────┐    ┌──────────────────────────────────┐    ┌───────────────────────────────┐   │
│   │         Dual-Listen Server        │    │         Task Queue & FSM         │    │      Workload Scheduler       │   │
│   │  • TCP Listener (0.0.0.0:Port)    │───▶│  • Priority-FIFO Ordering        │───▶│  • Strict GPU Isolation       │   │
│   │  • P2P QUIC Endpoint (iroh/DERP)  │    │  • 8-State Atomic Lifecycle      │    │  • Batch Spread Balancing     │   │
│   │  • Ephemeral Dynamic Port File    │    │  • Exponential Backoff Retries   │    │  • Dynamic Host CPU Throttling│   │
│   └───────────────────────────────────┘    └──────────────────────────────────┘    └───────────────────────────────┘   │
│                     │                                       │                                      │                   │
│                     ▼                                       ▼                                      ▼                   │
│   ┌───────────────────────────────────┐    ┌──────────────────────────────────┐    ┌───────────────────────────────┐   │
│   │          Worker Registry          │    │      In-Memory Map/Reduce        │    │        Heartbeat Reaper       │   │
│   │  • Capability Tracking (CPU/GPU)  │    │  • Chunk Partitioning            │    │  • Periodic Dead-Node Sweep   │   │
│   │  • Real-Time sysinfo Telemetry    │    │  • Intermediate Shuffle Buffer   │    │  • Automatic Task Re-Queue    │   │
│   │  • Mobile Battery/Thermal Status  │    │  • Aggregation Pipeline          │    │  • Monotonic Clock Assurance  │   │
│   └───────────────────────────────────┘    └──────────────────────────────────┘    └───────────────────────────────┘   │
└───────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────────┘
                                                            │
                     ┌──────────────────────────────────────┼──────────────────────────────────────┐
                     │                                      │                                      │
                     ▼                                      ▼                                      ▼
       ┌───────────────────────────┐          ┌───────────────────────────┐          ┌───────────────────────────┐
       │   Linux / macOS Worker    │          │  Windows 11 GPU Worker    │          │   Android Mobile Worker   │
       │   (Cloud VM / Server)     │          │  (Desktop RTX 4090 / PC)  │          │   (ARM64 Snapdragon SoC)  │
       │                           │          │                           │          │                           │
       │ • High-Concurrency CPU    │          │ • Physical/Simulated GPU  │          │ • Energy-Aware Compute    │
       │ • Cargo Compilation Farm  │          │ • Anti-Stutter Protection │          │ • Thermal Throttle Gating │
       │ • Isolated Temp Sandboxes │          │ • Windows .exe Subsystem  │          │ • Low Battery Safety Stop │
       │ • Continuous Stream Drain │          │ • Matrix Engine Simulator │          │ • Termux / ADB Daemon     │
       └───────────────────────────┘          └───────────────────────────┘          └───────────────────────────┘
```

---

## Workspace Crate Layout

The project is structured as a modular, cleanly decoupled Cargo workspace:

```
rusty_grid/
├── Cargo.toml                     # Workspace root & shared dependency manifest
├── README.md                      # Public open-source release documentation
├── PROJECT.md                     # Engineering blueprint & milestone tracking
├── test_integration.sh            # Automated end-to-end integration test harness (AC1-AC5)
├── build_windows_worker.sh        # Windows cross-compilation harness (x86_64-pc-windows-gnu)
├── build_android_worker.sh        # Android cross-compilation harness (aarch64-linux-android)
├── crates/
│   ├── core/                      # rusty_grid_core
│   │   ├── src/
│   │   │   ├── protocol.rs        # Wire framing (4-byte length prefix), Worker/Master/Client messages
│   │   │   ├── capabilities.rs    # WorkerCapabilities, MobileCapabilities, HardwareOverrides
│   │   │   ├── task.rs            # Task, TaskId, TaskSpec, TaskRequirements, TaskResult
│   │   │   ├── transport.rs       # GridStream / BiStream unifying TCP, in-memory duplex, and iroh QUIC
│   │   │   ├── mapreduce.rs       # MapReduceJobSpec, MapFunctionSpec, ReduceFunctionSpec
│   │   │   └── error.rs           # GridError and GridResult unified error types
│   │   └── Cargo.toml
│   ├── master/                    # rusty_grid_master
│   │   ├── src/
│   │   │   ├── server.rs          # Dual-listen TCP + iroh QUIC server, connection dispatching
│   │   │   ├── registry.rs        # WorkerRegistry, capability queries, session isolation
│   │   │   ├── queue.rs           # TaskQueue, 8-state Task FSM, exponential retry policy
│   │   │   ├── scheduler.rs       # WorkloadScheduler, strict GPU routing, batch spread balancing
│   │   │   ├── mapreduce.rs       # MapReduceEngine, in-memory shuffle and aggregation
│   │   │   └── reaper.rs          # Dead worker detection and automatic unacknowledged task recovery
│   │   └── Cargo.toml
│   ├── worker/                    # rusty_grid_worker
│   │   ├── src/
│   │   │   ├── client.rs          # WorkerClient, TCP/QUIC lifecycle, automatic reconnect loop
│   │   │   ├── runner.rs          # Multi-threaded TaskRunner, watchdog timeouts, GPU matrix engine
│   │   │   ├── sandbox.rs         # Scratch directory isolation, path traversal guards, cleanup
│   │   │   ├── heartbeat.rs       # Periodic liveness loop, lock-free sysinfo telemetry collection
│   │   │   └── backoff.rs         # Exponential backoff with jitter
│   │   └── Cargo.toml
│   └── cli/                       # rusty_grid_cli
│       ├── src/
│       │   ├── main.rs            # Unified CLI (oxideswarm / rusty-grid) with 6 subcommands
│       │   └── config.rs          # 4-tier configuration engine (CLI > Env > Config File > Defaults)
│       └── Cargo.toml
└── tests/                         # Comprehensive multi-tier integration and adversarial test suites
```

### Deep Dive into Crates

#### 1. `crates/core` (`rusty_grid_core`)
* **Wire Protocol & Framing**: Built atop `tokio_util::codec::LengthDelimitedCodec` using a big-endian 4-byte length header with a 64 MB maximum frame guard (`MAX_FRAME_SIZE`). Prevents memory denial-of-service and unbounded socket buffer pre-allocation.
* **Unified Transport Abstraction (`GridStream` / `BiStream`)**: Seamlessly wraps raw Tokio `TcpStream`, in-memory test duplex channels (`tokio::io::DuplexStream`), and native `iroh` QUIC bidirectional streams (`iroh::endpoint::RecvStream` + `SendStream`).
* **Typed Task Specifications (`TaskSpec`)**:
  * `Command`: Arbitrary executable with argument vectors, environment maps, and working directories.
  * `ShellScript`: Inline POSIX shell scripts executed securely under `/bin/sh -c`.
  * `RustCompilation`: Specialized compilation jobs compiling isolated crates with compiler flags.
  * `GpuCompute`: Matrix compute kernels with configurable dimensions, workgroup sizes, and intensity.
  * `BuiltinTest`: Synthetic test workloads for deterministic validation and failure injection.

#### 2. `crates/master` (`rusty_grid_master`)
* **Dynamic Ephemeral Port Publication**: Binding `--listen 127.0.0.1:0` causes the kernel to assign a non-colliding ephemeral port. The master publishes this port atomically to `--port-file <PATH>` (with automatic parent directory creation), allowing automated test scripts and local clusters to discover coordinates with zero port collisions.
* **8-State Task FSM**: Every task advances through an explicit finite state machine:
  $$\text{Submitted} \longrightarrow \text{Queued} \longrightarrow \text{Scheduled} \longrightarrow \text{Running} \longrightarrow \begin{cases} \text{Completed} \\ \text{Failed} \\ \text{Retrying} \\ \text{Cancelled} \end{cases}$$
* **Workload-Specific Scheduler (`WorkloadScheduler`)**:
  * *Dual-Gated GPU Isolation*: Tasks requiring a GPU will *never* be assigned to a CPU-only worker, even under extreme queue pressure.
  * *GPU Preservation*: Generic CPU tasks will preferentially route to CPU-only workers to preserve valuable GPU-capable nodes for upcoming GPU kernels.
  * *Batch Spread Scheduling*: When 5 compilation tasks arrive simultaneously, the scheduler uses in-pass provisional load tracking to spread tasks evenly across all eligible least-loaded workers instead of saturating the first worker.
* **In-Memory Map/Reduce Engine**: Splits text or record datasets across workers, coordinates mapper execution, buffers and groups intermediate key-value pairs, dispatches reducer tasks, and returns aggregated JSON/tabular results.

#### 3. `crates/worker` (`rusty_grid_worker`)
* **Multi-Threaded Supervised Execution**: Tasks execute concurrently up to `--max-concurrency`.
* **Sandbox Directory Isolation**: Each task executes in its own dedicated sandbox (`/tmp/rusty_grid_sandboxes/<task-id>`), preventing cross-contamination. Relative file paths are strictly sanitized to prevent directory traversal attacks (`..`).
* **Non-Blocking Stream Capture**: Child process `stdout` and `stderr` are read concurrently via `read_bounded_stream`. Once the buffer reaches limit (default 10 MB), the worker safely drains remainder bytes to EOF, preventing child process write pipe deadlocks while guarding host RAM.
* **Watchdog Timers**: Every task is bounded by `--timeout-secs`. If exceeded, the worker terminates the process and emits standard GNU timeout exit code `124` (`EXIT_CODE_TIMEOUT`).
* **Simulated GPU Matrix Engine**: For machines without physical GPUs, the worker implements genuine tiled matrix multiplication ($C = A \times B$) simulating GPU thread blocks with configurable matrix dimensions and workgroup tile sizes.
* **Mobile Environmental Safety**: Respects battery levels (<15% threshold aborts heavy tasks) and pauses compute when mobile thermal throttling is active.

#### 4. `crates/cli` (`rusty_grid_cli`)
* Unified binary `oxideswarm` (aliased as `rusty-grid`) providing 6 distinct subcommands: `master`, `worker`, `submit`, `status`, `workers`, and `mapreduce`.
* Backed by a 4-tier hierarchical configuration resolver.

---

## Key Features Detailed

### 1. Native P2P NAT Traversal via Embedded `iroh`

Traditional distributed grids require public IP addresses, router port forwarding rules, or complex VPN installations (such as Tailscale or WireGuard) to connect nodes over the internet.

OxideSwarm embeds **`iroh`** directly into the core networking layer:
* When starting the master with `--p2p`, OxideSwarm initializes a QUIC endpoint that connects to global DERP relay nodes.
* It generates a base64/JSON cryptographically signed **P2P Connection Ticket**.
* Remote workers anywhere in the world connect simply by passing `--p2p-ticket "<TICKET>"`.
* The nodes perform automated STUN hole-punching to establish direct, peer-to-peer, encrypted UDP QUIC connections through NATs and firewalls. If direct UDP is blocked, traffic flows seamlessly through low-latency DERP relays.

```bash
# On Master (e.g., Office Server):
oxideswarm master --p2p --p2p-ticket-file /tmp/master.ticket

# On Worker (e.g., Home Desktop or Laptop):
oxideswarm worker --p2p-ticket "$(cat /tmp/master.ticket)"
```

### 2. Dynamic System Load Awareness (Host Anti-Stuttering)

When contributing a personal workstation or gaming PC to a compute grid, users frequently experience mouse lag, frame drops, or audio stuttering when external software is running.

OxideSwarm solves this with **Host Telemetry Backpressure**:
* Worker heartbeat loops continuously monitor host CPU usage % and available RAM using the `sysinfo` crate.
* Heartbeats transmit `cpu_usage_pct` alongside active task counts to the master.
* The master's `WorkloadScheduler` checks each worker against `--max-host-cpu-pct` (default: 85.0%).
* If a user launches a demanding 3D game or begins a Blender render on their machine, that worker's total CPU load exceeds 85%. The scheduler immediately halts assigning new tasks to that worker until host activity subsides, protecting user experience.

### 3. 4-Tier Hierarchical Configuration Engine

Node behavior is governed by a strict, deterministic configuration hierarchy:

$$\text{CLI Flags} \;\;>\;\; \text{Environment Variables } (\texttt{RUSTY\_GRID\_*}) \;\;>\;\; \text{Config File (TOML/JSON)} \;\;>\;\; \text{Hardcoded Defaults}$$

OxideSwarm automatically loads `rusty-grid.toml` or `rusty-grid.json` from the current working directory, or from an explicit path specified by `--config <PATH>` or `RUSTY_GRID_CONFIG`.

#### Example `rusty-grid.toml`:
```toml
[master]
listen = "0.0.0.0:8080"
port_file = "/tmp/oxideswarm.port"
heartbeat_interval_secs = 3
heartbeat_timeout_secs = 10
reaper_interval_secs = 1
max_host_cpu_pct = 85.0
preserve_gpu = true
p2p = false

[worker]
master = "127.0.0.1:8080"
name = "workstation-node"
cores = 8
ram_mb = 16384
max_concurrency = 8
simulate_gpu = true
gpu_name = "NVIDIA GeForce RTX 4090"

[submit]
master = "127.0.0.1:8080"
timeout_secs = 120
```

### 4. Hardware Capability Overrides

Workers auto-detect hardware upon startup, but allow fine-grained CLI overrides:
* `--cores <N>`: Artificially constrain worker CPU allocation (e.g., reserve cores for local desktop apps).
* `--ram-mb <MB>`: Override reported available memory.
* `--gpu`: Force advertisement of physical GPU hardware.
* `--no-gpu`: Explicitly disable GPU advertising on a machine equipped with a GPU.
* `--simulate-gpu`: Enable the virtual tiled matrix multiplication engine for GPU kernel validation.
* `--gpu-name "<NAME>"`: Custom GPU descriptor string (e.g., `"NVIDIA A100-SXM4-80GB"`).
* `--max-concurrency <N>`: Bound maximum simultaneous tasks executing on this worker.

### 5. Pure In-Memory Map/Reduce Task Orchestration

OxideSwarm includes a lightweight, distributed Map/Reduce engine inspired by *Paladin*:
* **Zero External Dependencies**: Operates entirely in memory without requiring a Redis broker, database, or disk-backed distributed file system.
* **Built-in & Custom Operators**: Supports pre-compiled high-performance operators (`word_count`, `line_count`, `identity`) as well as user-supplied POSIX shell scripts and binaries.
* **Automatic Data Partitioning**: Divides input files or datasets into $N$ balanced chunks (`--chunks <N>`) and schedules parallel mapper tasks across the cluster.
* **In-Memory Shuffle & Reduce**: Groups intermediate key-value pairs and runs parallel reduce tasks, aggregating outputs into JSON or human-readable formats.

---

## Quickstart & CLI Usage Guide

### 1. Building the Binaries

Ensure you have Rust 1.75+ installed ([rustup.rs](https://rustup.rs)).

```bash
# Clone the repository
git clone https://github.com/your-org/rusty_grid.git
cd rusty_grid

# Build optimized release binaries
cargo build --release --bin rusty-grid
```

The unified binary is located at `./target/release/rusty-grid` (or run directly via `cargo run --bin rusty-grid --`).

---

### 2. The 6 Subcommands in Action

#### A. Starting the Master Coordinator (`master`)

```bash
# Start master listening on TCP port 8080
./target/release/rusty-grid master --listen 0.0.0.0:8080

# Start master with dynamic ephemeral port allocation and port publication
./target/release/rusty-grid master --listen 127.0.0.1:0 --port-file /tmp/master.port

# Start master with P2P NAT Traversal enabled
./target/release/rusty-grid master --listen 0.0.0.0:8080 --p2p --p2p-ticket-file /tmp/cluster.ticket
```

#### B. Spawning Worker Nodes (`worker`)

```bash
# Connect standard CPU worker
./target/release/rusty-grid worker \
  --master 127.0.0.1:8080 \
  --name "worker-cpu-01" \
  --cores 4 \
  --ram-mb 8192

# Connect worker with Simulated GPU capabilities
./target/release/rusty-grid worker \
  --master 127.0.0.1:8080 \
  --name "worker-gpu-01" \
  --cores 8 \
  --ram-mb 16384 \
  --simulate-gpu \
  --gpu-name "Simulated NVIDIA RTX 4090"

# Connect worker across the internet using a P2P ticket
./target/release/rusty-grid worker --p2p-ticket "$(cat /tmp/cluster.ticket)"
```

#### C. Submitting Tasks (`submit`)

```bash
# 1. Submit a generic command and await result synchronously (--wait)
./target/release/rusty-grid submit \
  --master 127.0.0.1:8080 \
  --type generic \
  --wait \
  --command echo -- "Hello OxideSwarm"

# 2. Submit a GPU compute kernel (strictly routes to GPU workers)
./target/release/rusty-grid submit \
  --master 127.0.0.1:8080 \
  --type gpu \
  --gpu \
  --wait

# 3. Submit a distributed Rust crate compilation task
./target/release/rusty-grid submit \
  --master 127.0.0.1:8080 \
  --type compile \
  --command "auth_service" \
  --wait \
  -- "cargo" "build" "--release"

# 4. Asynchronous submission returning JSON Task ID
./target/release/rusty-grid submit \
  --master 127.0.0.1:8080 \
  --type shell \
  --command "sleep 5 && date" \
  --json
```

#### D. Inspecting Cluster & Task Status (`status`)

```bash
# Query overall cluster health and active workers
./target/release/rusty-grid status --master 127.0.0.1:8080

# Query specific task execution lifecycle state
./target/release/rusty-grid status \
  --master 127.0.0.1:8080 \
  --task-id "7b99c158-b6df-4fa3-80b6-20ec18bb39c9"

# Query cluster status formatted as machine-readable JSON
./target/release/rusty-grid status --master 127.0.0.1:8080 --json
```

#### E. Listing Registered Workers (`workers`)

```bash
# List all active workers and their advertised capabilities
./target/release/rusty-grid workers --master 127.0.0.1:8080

# Output worker registry as JSON
./target/release/rusty-grid workers --master 127.0.0.1:8080 --json
```

#### F. Executing Distributed Map/Reduce (`mapreduce`)

```bash
# Run distributed word count across 4 dataset chunks
./target/release/rusty-grid mapreduce \
  --master 127.0.0.1:8080 \
  --input /path/to/large_dataset.txt \
  --mapper word_count \
  --reducer sum \
  --chunks 4 \
  --json
```

---

## Cross-Platform Worker Support

### 1. Windows Worker Cross-Compilation

OxideSwarm provides an automated, production-grade cross-compilation harness (`build_windows_worker.sh`) to build native 64-bit Windows `.exe` binaries directly from macOS or Linux.

#### Prerequisites:
* **Rust target**: `rustup target add x86_64-pc-windows-gnu`
* **MinGW-w64 Toolchain**:
  * macOS (Homebrew): `brew install mingw-w64`
  * Ubuntu / Debian: `sudo apt-get install gcc-mingw-w64-x86-64`
  * Fedora: `sudo dnf install mingw64-gcc`

#### Build Options:
```bash
# Verify host prerequisites without compiling
./build_windows_worker.sh --check

# Dry-run compilation plan
./build_windows_worker.sh --dry-run

# Compile optimized release binary
./build_windows_worker.sh --release
```

The output binary will be placed at `target/x86_64-pc-windows-gnu/release/rusty-grid.exe`.

#### Deployment to Windows Host:
```powershell
# Copy binary to Windows PC and launch worker via PowerShell:
.\rusty-grid.exe worker --master 192.168.1.50:8080 --name "windows-desktop" --simulate-gpu
```

---

### 2. Android Mobile Worker Support

OxideSwarm supports turning Android smartphones and tablets into edge computing nodes. Android workers automatically detect and advertise mobile-specific telemetry, including SoC model, battery percentage, charging state, and thermal throttling status.

#### Prerequisites:
* **Rust target**: `rustup target add aarch64-linux-android`
* **Android NDK**: Version r25b+ installed (set `ANDROID_NDK_HOME=/path/to/ndk`)

#### Build Options:
```bash
# Check cross-compilation toolchain
./build_android_worker.sh --check

# Compile release binary for ARM64 Android (API 24+)
./build_android_worker.sh --release
```

The output binary will be placed at `target/aarch64-linux-android/release/rusty-grid`.

#### Deployment via ADB or Termux:
```bash
# Push binary to device
adb push target/aarch64-linux-android/release/rusty-grid /data/local/tmp/rusty-grid
adb shell chmod +x /data/local/tmp/rusty-grid

# Run worker on device
adb shell /data/local/tmp/rusty-grid worker \
  --master 192.168.1.50:8080 \
  --name "pixel-android-node"
```

---

## Testing & Verification Guide

OxideSwarm includes comprehensive automated testing infrastructure ensuring mathematical correctness, network fault tolerance, and zero resource leaks.

### 1. Automated Integration Acceptance Test (`test_integration.sh`)

The root repository includes `test_integration.sh`, a self-contained, automated end-to-end acceptance harness verifying Acceptance Criteria AC1 through AC5:

* **Dynamic Port Allocation**: Starts master on `127.0.0.1:0` with port file publication to ensure zero TCP port collisions.
* **Multi-Node Cluster Formation**: Spawns 1 Master and 3 Workers (Worker 1 CPU, Worker 2 CPU, Worker 3 Simulated GPU).
* **AC1 & AC2**: Verifies all 3 workers register and advertise hardware capabilities.
* **AC3**: Executes generic task submission and verifies output text.
* **AC4**: Submits a GPU-specific task and verifies it is strictly routed to the GPU worker.
* **AC5**: Submits a batch of 5 independent compilation tasks and confirms parallel spread execution.
* **Map/Reduce Validation**: Executes distributed Map/Reduce word count across 3 chunks and validates key-value aggregation.
* **POSIX Trap Teardown**: Uses POSIX `SIGTERM`/`SIGKILL` traps on `EXIT` to guarantee clean child process termination with zero orphan processes.

Run the test suite with:
```bash
./test_integration.sh
```

---

### 2. Cargo Test Suites & Clippy Lints

```bash
# Run unit tests across all workspace crates
cargo test --workspace

# Run multi-tier integration and stress test suites
cargo test --test m1_stress_challenge
cargo test --test m2_lifecycle
cargo test --test memory_bench
cargo test --test stream_stress

# Run strict Clippy linter
cargo clippy --workspace --all-targets -- -D warnings
```

---

## Configuration Reference

Complete listing of configuration parameters across CLI arguments, environment variables, and config file keys:

| Feature / Setting | CLI Flag | Environment Variable | Config File Key | Default |
|---|---|---|---|---|
| Master Bind Address | `--listen <ADDR>` | `RUSTY_GRID_MASTER_LISTEN` | `master.listen` | `127.0.0.1:0` |
| Master Port File | `--port-file <PATH>` | `RUSTY_GRID_PORT_FILE` | `master.port_file` | `None` |
| Heartbeat Interval | `--heartbeat-interval-secs <S>` | `RUSTY_GRID_HEARTBEAT_INTERVAL_SECS` | `master.heartbeat_interval_secs` | `3` |
| Heartbeat Timeout | `--heartbeat-timeout-secs <S>` | `RUSTY_GRID_HEARTBEAT_TIMEOUT_SECS` | `master.heartbeat_timeout_secs` | `10` |
| Dead Reaper Interval | `--reaper-interval-secs <S>` | `RUSTY_GRID_REAPER_INTERVAL_SECS` | `master.reaper_interval_secs` | `1` |
| Host CPU Threshold | `--max-host-cpu-pct <PCT>` | `RUSTY_GRID_MAX_HOST_CPU_PCT` | `master.max_host_cpu_pct` | `85.0` |
| Preserve GPU Nodes | `--preserve-gpu` | `RUSTY_GRID_PRESERVE_GPU` | `master.preserve_gpu` | `true` |
| P2P QUIC Listener | `--p2p` | `RUSTY_GRID_P2P` | `master.p2p` | `false` |
| P2P Ticket File | `--p2p-ticket-file <PATH>` | `RUSTY_GRID_P2P_TICKET_FILE` | `master.p2p_ticket_file` | `None` |
| Worker Master Target | `--master <ADDR>` | `RUSTY_GRID_MASTER_ADDR` | `worker.master` | `127.0.0.1:8080` |
| Worker P2P Ticket | `--p2p-ticket <TICKET>` | `RUSTY_GRID_P2P_TICKET` | `worker.p2p_ticket` | `None` |
| Worker Core Override | `--cores <N>` | `RUSTY_GRID_CORES` | `worker.cores` | Auto-detected |
| Worker RAM Override | `--ram-mb <MB>` | `RUSTY_GRID_RAM_MB` | `worker.ram_mb` | Auto-detected |
| Worker GPU Force Enable | `--gpu` | `RUSTY_GRID_GPU` | `worker.gpu` | Auto-detected |
| Worker GPU Force Disable| `--no-gpu` | `RUSTY_GRID_NO_GPU` | `worker.no_gpu` | `false` |
| Worker Simulate GPU | `--simulate-gpu` | `RUSTY_GRID_SIMULATE_GPU` | `worker.simulate_gpu` | `false` |
| Worker Concurrency Cap | `--max-concurrency <N>` | `RUSTY_GRID_MAX_CONCURRENCY` | `worker.max_concurrency` | Equal to cores |

---

## Contributing

Contributions are welcome! Please ensure that:
1. All changes follow the safe Rust philosophy (`#![forbid(unsafe_code)]` where applicable).
2. All pull requests include passing unit or integration tests.
3. `./test_integration.sh` passes with 100% success.
4. Code passes `cargo clippy --workspace --all-targets -- -D warnings`.

---

## License

OxideSwarm is licensed under either of:

* Apache License, Version 2.0 ([LICENSE-APACHE](http://www.apache.org/licenses/LICENSE-2.0))
* MIT License ([LICENSE-MIT](http://opensource.org/licenses/MIT))

at your option.
