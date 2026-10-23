# Project: OxideSwarm Architecture & Specification

OxideSwarm is a zero-broker distributed computing framework and cross-platform agent mesh written in Rust (edition 2021). A Master coordinator dispatches computational workloads (commands, shell scripts, compile jobs, map/reduce chunks, and hardware GPU matrix kernels) to worker nodes spanning macOS (Apple Silicon / Intel), Windows (x86_64), Linux servers, and Android devices (ARM64).

---

## 1. System Architecture

The repository is organized into four cleanly decoupled functional layers:

1. **Layer 1: Rust Core Engine & Networking** (`crates/core`, `crates/master`, `crates/worker`, `crates/cli`, `packaging/windows`, `packaging/macos`): Distributed task orchestration, high-throughput binary wire protocol, P2P WAN NAT traversal (Iroh QUIC/DERP), CLI (`rusty-grid` and `oxideswarm`).
2. **Layer 2: JNI / Android Bridge & Packaging** (`crates/android_bridge`, `packaging/android`): Native Android shared library (`liboxideworker.so`) and Android Studio app with Foreground Service compliance and live telemetry injection.
3. **Layer 3: Agent Mesh Coordination** (`crates/agent_mesh`, `packaging/agent_mesh`, `scripts/agent_node.py`): Axum WebSocket relay hub (token-authenticated), multi-platform agent client, and an embedded `AgentGridBridge` connecting agent workflows directly to the compute grid.
4. **Layer 4: Cross-Machine Reachability & Benchmarks** (`scripts/sync_network`, `benchmarks/interconnect`): Zero-dependency cloud-shared folder reachability probe (`sync_network` on port 8085) and empirical WAN benchmark datasets.

---

## 2. Security Model Summary

- **Cluster Secret Authentication**: A 32-byte cryptographic hex secret (`~/.oxideswarm/cluster.secret`) authenticates nodes via mutual HMAC-SHA256 challenge-response before exchanging wire frames on TCP (port 8088), Iroh QUIC streams, and UDP discovery beacons (port 8089).
- **Transport Security**: Plain TCP connections on trusted LANs are authenticated via HMAC-SHA256; remote WAN connections leverage Iroh QUIC with TLS encryption. Authenticated stream encryption (`0x04`) via ChaCha20-Poly1305 secures non-loopback TCP frames.
- **Web Dashboard Security**: Axum HTTP dashboard on port 8080 binds to loopback (`127.0.0.1:8080`) by default. LAN access requires explicit `--web-ui-addr 0.0.0.0:8080` and authentication via token (`oxideswarm secret show --dashboard-token`).
- **Process & Privilege Isolation**: Worker tasks execute under unprivileged user accounts (never root or LocalSystem). On Windows, services run under virtual account `NT SERVICE\OxideSwarmWorker`. Child processes are contained via Windows Job Objects (`kill-on-close`) and Unix process groups with `PR_SET_PDEATHSIG`.
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
├── README.md                        # Master architectural documentation & usage guide
├── PROJECT.md                       # Authoritative project specification & roadmap
├── CLAUDE.md                        # Architecture map for AI coding assistants
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
    ├── run_all_mesh_tests.py        # [Layer 3] Master test runner for agent mesh
    ├── test_launcher_scripts.py     # Static repository and launcher verification
    └── ...                          # Rust integration tests registered per crate
```

---

## 4. Feature Inventory

| Subsystem | Feature | Description | Status |
| :--- | :--- | :--- | :--- |
| **Core Protocol** | Wire Framing | 4-byte length-delimited framing, dual-mode Serde (`0x01` JSON, `0x02` Bincode, `0x03` Auth, `0x04` Encrypted) | Implemented |
| **Core Protocol** | Mutual Authentication | Mutual HMAC-SHA256 handshake over TCP and Iroh QUIC streams | Implemented |
| **Core Protocol** | Stream Encryption | ChaCha20-Poly1305 AEAD (`0x04`) wire transport derived from cluster secret | Implemented |
| **Core Protocol** | Version Negotiation | Workers send `PROTOCOL_VERSION` and `ProtocolCapabilities` at registration; the master refuses versions below the minimum. Client messages carry no version | Implemented |
| **Core Protocol** | Task Placement | `require:` task tags (os, arch, worker, tag) and worker tags (`os:`, `arch:`, user tags); no wire change ([design 0001](docs/design/0001-two-node-readiness.md)) | Implemented |
| **Master Grid** | 8-State Task FSM | `Submitted` ➔ `Queued` ➔ `Scheduled` ➔ `Running` ➔ `Completed`/`Failed`/`Retrying`/`Cancelled` | Implemented |
| **Master Grid** | Scheduler & Dispatch | Least-loaded / round-robin scheduling, real-GPU priority over simulated GPU, batch dispatch, anti-stuttering backpressure | Implemented |
| **Master Grid** | Memory Bounding | LRU task eviction and TTL bounding for terminal states in TaskQueue | Implemented |
| **CLI** | Cluster Check | `oxideswarm doctor`: connection, workers, pinned in-process / process / GPU probes, spreading (checks C1-C6) | Implemented |
| **CLI** | Task Inputs | `submit --stdin-file`, `--script-file`, `--interpreter`, `--env`; `--matrix-dim` / `--work-group-size` for GPU tasks | Implemented |
| **Worker Engine** | Execution Runner | Bounded stdout/stderr capture, cancellation, configurable process priority | Implemented |
| **Worker Engine** | Child Containment | Atomic Windows Job Objects (`kill-on-close` via `CREATE_SUSPENDED`) and Linux `PR_SET_PDEATHSIG` process tree binding | Implemented |
| **Worker Engine** | GPU Acceleration | Hardware `wgpu` compute engine (Metal, Vulkan, DX12) and simulated CPU SIMD fallback | Implemented |
| **Android Bridge** | In-Process JNI | `liboxideworker.so` cdylib running worker/master inside Tokio runtime threads | Implemented |
| **Android Bridge** | Cluster Secret Auth | Native secret injection (`nativeSetClusterSecret`, `nativeStartWorkerWithSecret`) | Implemented |
| **Agent Mesh** | Relay Hub | Axum WebSocket hub routing envelopes with token authentication and SHA-256 validation | Implemented |
| **Agent Mesh** | Grid Bridge | `--grid-master` CLI integration routing `grid_*` commands to live master | Implemented |

---

## 5. Milestone Roadmap

| Milestone | Scope | Deliverables & Verification | Status |
| :--- | :--- | :--- | :--- |
| **M1: Baseline Hygiene & Doc Rewrite** | Fix clippy blocker, normalize script line endings, remove legacy status files, update inbound links, repair ANDROID_ARCHITECTURE anchors, translate Vietnamese content, full doc rewrite | Clippy clean (`-D warnings`), launcher scripts tests pass, doc mode verification passes | **COMPLETED** |
| **M2: Functional Gap Completion** | Android JNI cluster secret auth, Agent Mesh `--grid-master` bridge, Real-GPU priority scheduler | Clean build, targeted integration tests pass | **COMPLETED** |
| **M3: Security & Process Hardening** | Authenticated wire stream encryption (`0x04`), protocol version & capability negotiation, Windows Job Object & Linux PDEATHSIG containment | Wire encryption tests, version negotiation tests, process containment tests pass | **COMPLETED** |
| **M4: Isolation & Verification** | Test state isolation (`OXIDESWARM_HOME`, ephemeral keys), final documentation synchronization, full workspace verification | 100% test pass across all workspace crates and python suites with 0 host pollution | **COMPLETED** |
| **M5: Two-Node Readiness** | Task placement, task inputs, GPU task size, `oxideswarm doctor`, documentation standard and two-machine guide ([design 0001](docs/design/0001-two-node-readiness.md)) | Unit and integration tests, live `doctor` run; macOS + Windows run by the user | **IN PROGRESS** |
