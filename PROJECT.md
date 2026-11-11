# Project: OxideSwarm Architecture & Specification

OxideSwarm is a zero-broker distributed computing framework and cross-platform agent mesh written in Rust (edition 2021). A Master coordinator dispatches computational workloads (commands, shell scripts, compile jobs, map/reduce chunks, and hardware GPU matrix kernels) to worker nodes spanning macOS (Apple Silicon / Intel), Windows (x86_64), Linux servers, and Android devices (ARM64).

Purpose, as [docs/ROADMAP.md](docs/ROADMAP.md) states it: a coding agent runs builds and tests on the machines its owner already has.

---

## 1. System Architecture

The repository is organized into four layers:

1. **Layer 1: Rust Core Engine & Networking** (`crates/core`, `crates/master`, `crates/worker`, `crates/cli`, `packaging/windows`, `packaging/macos`): Distributed task orchestration, binary wire protocol, P2P WAN NAT traversal (Iroh QUIC/DERP), CLI (`rusty-grid` and `oxideswarm`).
2. **Layer 2: JNI / Android Bridge & Packaging** (`crates/android_bridge`, `packaging/android`): Native Android shared library (`liboxideworker.so`) and Android Studio app with Foreground Service compliance and live telemetry injection.
3. **Layer 3: Agent Mesh Coordination** (`crates/agent_mesh`, `packaging/agent_mesh`, `scripts/agent_node.py`): Axum WebSocket relay hub (token-authenticated), multi-platform agent client, and an embedded `AgentGridBridge` connecting agent workflows directly to the compute grid.
4. **Layer 4: Cross-Machine Reachability & Benchmarks** (`scripts/sync_network`, `benchmarks/interconnect`): Zero-dependency cloud-shared folder reachability probe (`sync_network` on port 8085) and empirical WAN benchmark datasets.

---

## 2. Security Model Summary

- **Cluster Secret Authentication**: A 32-byte cryptographic hex secret (`~/.oxideswarm/cluster.secret`) authenticates nodes via mutual HMAC-SHA256 challenge-response before exchanging wire frames on TCP (port 8088), Iroh QUIC streams, and UDP discovery beacons (port 8089).
- **Transport Security**: Plain TCP connections on trusted LANs are authenticated via HMAC-SHA256; remote WAN connections leverage Iroh QUIC with TLS encryption. Authenticated stream encryption (`0x04`) via ChaCha20-Poly1305 secures non-loopback TCP frames.
- **Web Dashboard Security**: Axum HTTP dashboard on port 8080 binds to loopback (`127.0.0.1:8080`) by default. LAN access requires explicit `--web-ui-addr 0.0.0.0:8080` and authentication via token (`oxideswarm secret show --dashboard-token`).
- **Process & Privilege Isolation**: Worker tasks execute under unprivileged user accounts (never root or LocalSystem). On Windows, services run under virtual account `NT SERVICE\OxideSwarmWorker`. Child processes are contained via Windows Job Objects (`kill-on-close`) and Unix process groups supervised by a double-forked lifeline-pipe supervisor ([design 0031](docs/design/0031-containment-after-worker-crash.md)).
- **Network Ports**:
  - `8088/tcp`: Master Grid coordinator listener (and default `agent-mesh hub` listener; rebind hub to `8090` when co-located).
  - `8080/tcp`: Master Web Dashboard & REST API (and Worker standby HTTP 307 redirect portal).
  - `8089/udp`: Master UDP discovery beacon (`--master auto`).
  - `8085/tcp`: Cross-machine sync reachability probe (`scripts/sync_network`).

---

## 3. Directory & Workspace Layout

```text
OxideSwarm/
├── Cargo.toml                       # Workspace root (6 member crates)
├── Cargo.lock
├── README.md                        # Overview, install, quick start, CLI and environment reference
├── PROJECT.md                       # Architecture, layout and feature inventory (this file)
├── CLAUDE.md                        # Architecture map for AI coding assistants
├── docs/ROADMAP.md                  # Direction and order of work (the roadmap)
├── docs/BACKLOG.md                  # Work items and the status of each
├── docs/SECURITY.md                 # Threat model, ports, secret handling and rotation
├── ANDROID_ARCHITECTURE.md          # Mobile cluster architecture specification
├── DEPLOYMENT.md                    # Turnkey multi-platform deployment guide
│
├── crates/                          # Rust Core & Specialized Modules
│   ├── core/                        # [Layer 1] Wire codecs, Iroh QUIC transport, discovery, cluster_auth
│   ├── master/                      # [Layer 1] Registry, scheduler, task queue, failover, web UI
│   ├── worker/                      # [Layer 1] Execution engine, load-aware heartbeat, process sandbox
│   ├── cli/                         # [Layer 1] Unified CLI binary (rusty-grid & oxideswarm), ox-mode
│   ├── android_bridge/              # [Layer 2] JNI cdylib (liboxideworker.so) for Android
│   └── agent_mesh/                  # [Layer 3] Axum WebSocket Relay Hub & Agent Client
│
├── packaging/                       # Platform-Specific Packaging & Service Automation
│   ├── android/                     # [Layer 2] Android Studio app (Kotlin service) & Termux scripts
│   ├── agent_mesh/                  # [Layer 3] Node launcher scripts for Win, Mac, Ubuntu, Android
│   ├── macos/                       # [Layer 1] launchd plist & macOS service install scripts
│   └── windows/                     # [Layer 1] PowerShell Windows Service scripts (WinSW, C# wrapper)
│
├── windows/                         # Windows launchers (oxctl.ps1 + .cmd/.vbs wrappers)
├── switch_role.sh, connect_remote.sh|cmd, test_integration.sh, run_chaos_test.sh, run_cluster_benchmark.sh
├── scripts/                         # Operational Scripts & Sync Protocol
│   ├── sync_network/                # [Layer 4] Shared-folder role claim + HTTP ping probe (port 8085)
│   ├── agent_node.py                # [Layer 3] Pure Python fallback client for Agent Mesh
│   └── mode.sh                      # [Layer 1] Workflow modes runner (test, dev, doc, research)
│
├── benchmarks/                      # Empirical Benchmarks & Performance Hardening
│   └── interconnect/                # [Layer 4] Recorded Iroh-vs-VPN comparison data + validator script
│
└── tests/                           # Multi-Tier Test Suites & Integration Harnesses
    ├── e2e_cluster.rs               # Master integration test suite
    ├── run_mesh_simulation_tests.py # [Layer 3] Python reference-hub simulation test runner for agent mesh
    ├── test_launcher_scripts.py     # Static repository and launcher verification
    └── ...                          # Rust integration tests registered per crate
```

---

## 4. Feature Inventory

The table covers the subsystems of milestones M1 to M5. Later work has no row here: durable master state (`--state-dir`), agent workspaces, OS-level task isolation, join tokens, the USB kit and per-machine keys are tracked per item, with their status, in [docs/BACKLOG.md](docs/BACKLOG.md).

| Subsystem | Feature | Description | Status |
| :--- | :--- | :--- | :--- |
| **Core Protocol** | Wire Framing | 4-byte length-delimited framing, dual-mode Serde (`0x01` JSON, `0x02` Bincode, `0x03` Auth, `0x04` Encrypted) | Implemented |
| **Core Protocol** | Mutual Authentication | Mutual HMAC-SHA256 handshake over TCP and Iroh QUIC streams | Implemented |
| **Core Protocol** | Stream Encryption | ChaCha20-Poly1305 AEAD (`0x04`) wire transport derived from cluster secret | Implemented |
| **Core Protocol** | Version Negotiation | Workers send `protocol_version` and `ProtocolCapabilities` in `Register`. Every client message carries a version, and a client opens with `ClientMessage::Hello`. Each side refuses a peer outside `PROTOCOL_VERSION_MIN_COMPATIBLE..=PROTOCOL_VERSION_CURRENT` ([design 0040](docs/design/0040-protocol-versioning.md)) | Implemented |
| **Core Protocol** | Task Placement | `require:` task tags (os, arch, worker, iso, tag, tool) and worker tags (`os:`, `arch:`, `slots:`, `iso:`, `tool:`, user tags); no wire change ([design 0001](docs/design/0001-two-node-readiness.md)) | Implemented |
| **Master Grid** | 8-State Task FSM | `Submitted` ➔ `Queued` ➔ `Scheduled` ➔ `Running` ➔ `Completed`/`Failed`/`Retrying`/`Cancelled` | Implemented |
| **Master Grid** | Scheduler & Dispatch | Weighted least-loaded scheduling, real-GPU priority over simulated GPU, batch dispatch, anti-stuttering backpressure | Implemented |
| **Master Grid** | Memory Bounding | Finish-order eviction of terminal tasks in `TaskQueue` with count and byte-size bounds (`--max-retained-tasks`, `--max-retained-mb`) and active queue caps (`--max-queue-size`, `--max-active-mb`) | Implemented |
| **CLI** | Cluster Check | `oxideswarm doctor`: connection, workers, pinned in-process / process / GPU probes, spreading (checks C1-C6) | Implemented |
| **CLI** | Task Inputs | `submit --stdin-file`, `--script-file`, `--interpreter`, `--env`; `--matrix-dim` / `--work-group-size` for GPU tasks | Implemented |
| **Worker Engine** | Execution Runner | Bounded head-and-tail stdout/stderr capture ([design 0022](docs/design/0022-head-and-tail-stream-capture.md)), cancellation, configurable process priority | Implemented |
| **Worker Engine** | Child Containment | Windows Job Objects (`kill-on-close` via `CREATE_SUSPENDED`, with a `taskkill /F /T` fallback if job assignment fails) and Unix process groups with a double-forked lifeline-pipe supervisor, optional Linux cgroup v2 leaf, and per-attempt `RUSTY_GRID_RUN_TOKEN` `setsid()` sweep ([design 0031](docs/design/0031-containment-after-worker-crash.md)) | Implemented |
| **Worker Engine** | GPU Acceleration | Hardware `wgpu` compute engine (Metal, Vulkan, DX12) and simulated CPU SIMD fallback | Implemented |
| **Android Bridge** | In-Process JNI | `liboxideworker.so` cdylib running the Android worker inside Tokio runtime threads (the Android app is a worker only) | Implemented |
| **Android Bridge** | Cluster Secret Auth | Native secret injection (`nativeSetClusterSecret`) | Partial |
| **Agent Mesh** | Relay Hub | Axum WebSocket hub routing envelopes with token authentication and SHA-256 validation | Implemented |
| **Agent Mesh** | Grid Bridge | `--grid-master` CLI integration starting an embedded master and routing `grid_*` commands to it | Implemented |
| **Mobile Coordination** | Dynamic Throttling & Arbitrage | Multi-tier thermal/battery throttling, RRC adaptive keepalive, and pre-sleep evacuation; the 5-rule mobile master refusal matrix (`check_mobile_master_eligibility`, not called in production) and automatic coordinator failover/election (`ClaimMasterRole`, refused by `reject_master_role_claim` in `server.rs`) are design only ([design 0002](docs/design/0002-mobile-coordination.md)) | Partial |
| **File Staging** | Task input and output files | `submit --input` (files and directories) and `--output` patterns as BLAKE3 blobs through a bounded master store and worker cache across both the CLI and the MCP server, nothing sent twice, `RustCompilation` artifacts returned ([design 0006](docs/design/0006-content-addressed-data-plane.md)); legacy inline staging retained for wire compatibility ([design 0003](docs/design/0003-distributed-file-staging.md)) | Partial |
| **AI Tooling** | MCP Server | Model Context Protocol server over JSON-RPC 2.0 stdio (`oxideswarm mcp`) exposing `status`, `workers`, `submit`, `task_result`, `cancel`, `doctor` and `workspaces` (MCP-1 to MCP-3 are Implemented in [docs/BACKLOG.md](docs/BACKLOG.md); [design 0004](docs/design/0004-mcp-and-discovery.md), [design 0012](docs/design/0012-mcp-concurrent-deadlines-and-cancel.md), [design 0022](docs/design/0022-head-and-tail-stream-capture.md)) | Implemented |
| **Discovery** | Subnet-Isolated Fallback | 3-tier discovery hierarchy: UDP broadcast, authenticated HTTP probe (`GET /api/discovery/probe`), and signed rendezvous registry (`OXIDESWARM_RENDEZVOUS`) ([design 0004](docs/design/0004-mcp-and-discovery.md)) | Partial |

---

## 5. Milestone Roadmap

Milestones M1 to M5 are past work. The direction after the completion backlog is in [docs/ROADMAP.md](docs/ROADMAP.md).

| Milestone | Scope | Deliverables & Verification | Status |
| :--- | :--- | :--- | :--- |
| **M1: Baseline Hygiene & Doc Rewrite** | Fix clippy blocker, normalize script line endings, remove legacy status files, update inbound links, repair ANDROID_ARCHITECTURE anchors, translate Vietnamese content, full doc rewrite | Clippy clean (`-D warnings`), launcher scripts tests pass, doc mode verification passes | Implemented |
| **M2: Functional Gap Completion** | Android JNI cluster secret auth, Agent Mesh `--grid-master` bridge, Real-GPU priority scheduler | Clean build, targeted integration tests pass (Kotlin app pending `AND-1`) | Partial |
| **M3: Security & Process Hardening** | Authenticated wire stream encryption (`0x04`), protocol version & capability negotiation, Windows Job Object & Linux PDEATHSIG containment | Wire encryption tests, version negotiation tests, process containment tests pass | Partial |
| **M4: Isolation & Verification** | Test state isolation (`OXIDESWARM_HOME`, ephemeral keys), final documentation synchronization, full workspace verification | Workspace unit, integration, and Python test suites with `OXIDESWARM_HOME` isolation | Implemented |
| **M5: Production Completion & Verification** | Two-node readiness ([design 0001](docs/design/0001-two-node-readiness.md)), mobile coordination & dynamic policies ([design 0002](docs/design/0002-mobile-coordination.md)), distributed file staging ([design 0003](docs/design/0003-distributed-file-staging.md)), MCP server & discovery fallback ([design 0004](docs/design/0004-mcp-and-discovery.md)), full workspace verification | Unit and integration tests, 2-worker live `doctor` checks C1–C6 pass, documentation sync | Partial |

---

## 6. Technology choices

Versions are not repeated here; they are in the manifests named in the third column. Where a design record explains a choice, the last column names it.

| Layer | Choice | Declared in | Record |
| :--- | :--- | :--- | :--- |
| Language | Rust, edition 2021; the minimum version is `rust-version` | `Cargo.toml` | |
| Async runtime | `tokio` | `Cargo.toml` | |
| Transport across networks | `iroh` (QUIC with NAT traversal), feature `p2p` | `Cargo.toml` | |
| Wire encoding | `bincode` and tagged JSON through `serde` | `Cargo.toml` | [design 0040](docs/design/0040-protocol-versioning.md) |
| Content addressing | `blake3` | `crates/core/Cargo.toml`, `crates/master/Cargo.toml` | [design 0006](docs/design/0006-content-addressed-data-plane.md) |
| Per-machine keys | Noise handshake through `snow` | `crates/core/Cargo.toml` | [design 0051](docs/design/0051-machine-identity-and-invites.md) |
| Command line | `clap` | `Cargo.toml` | |
| Dashboard and Agent Mesh hub | `axum` | `Cargo.toml` | |
| GPU compute | `wgpu`, feature `gpu-wgpu`, off by default | `crates/worker/Cargo.toml` | |
| Operating system interfaces | `libc` on Unix, `windows-sys` on Windows | the crate manifests | [design 0033](docs/design/0033-os-level-task-isolation.md) |

OxideSwarm writes no inference engine of its own. Language model work, when it starts, drives existing model servers ([design 0038](docs/design/0038-large-language-model-inference.md)).
