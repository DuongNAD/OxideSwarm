# Project: OxideSwarm Codebase Consolidation & Delivery

## Architecture
The consolidated OxideSwarm repository is organized into four cleanly decoupled functional layers:
1. **Layer 1: Rust Core Engine & Networking** (`crates/core`, `crates/master`, `crates/worker`, `crates/cli`, `packaging/windows`, `packaging/macos`): Distributed task orchestration, high-throughput binary wire protocol, P2P WAN NAT traversal (Iroh QUIC/DERP), CLI (`rusty-grid` and `oxideswarm`).
2. **Layer 2: JNI / Android Bridge & Packaging** (`crates/android_bridge`, `packaging/android`): Native Android shared library (`liboxideworker.so`) and Android Studio app with Foreground Service.
3. **Layer 3: Agent Mesh Coordination** (`crates/agent_mesh`, `packaging/agent_mesh`, `scripts/agent_node.py`): Axum WebSocket relay hub, multi-platform agent client, process sandboxing, turnkey launchers.
4. **Layer 4: Cross-Machine Synchronization & Benchmarks** (`scripts/sync_network`, `benchmarks/interconnect`): Zero-dependency cloud-folder mailbox synchronization, empirical WAN comparative benchmark suite (Iroh vs WireGuard vs Cloudflare Tunnel).

```
OxideSwarm/
├── Cargo.toml                       # Workspace root (6 member crates)
├── Cargo.lock
├── README.md                        # Master architectural documentation & usage guide
├── PROJECT.md                       # Unified project specification
├── ORIGINAL_REQUEST.md              # Authoritative multi-milestone requirements
├── ANDROID_ARCHITECTURE.md          # Mobile cluster architecture specification
├── DEPLOYMENT.md                    # Turnkey multi-platform deployment guide
│
├── crates/                          # Rust Core & Specialized Modules
│   ├── core/                        # [Layer 1] Wire codecs, Iroh QUIC transport, discovery
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
├── scripts/                         # Operational Scripts & Sync Protocol
│   ├── sync_network/                # [Layer 4] Cross-machine file coordination & auto-debug
│   ├── agent_node.py                # [Layer 3] Pure Python fallback client for Agent Mesh
│   └── mode.sh                      # [Layer 1] Workflow modes runner (test, dev, doc, research)
│
├── benchmarks/                      # Empirical Benchmarks & Performance Hardening
│   └── interconnect/                # [Layer 4] Empirical network benchmark suite (Iroh vs VPN)
│
└── tests/                           # Multi-Tier Test Suites & Integration Harnesses
    ├── e2e_cluster.rs               # Master integration test suite
    ├── run_all_mesh_tests.py        # [Layer 3] Master test runner for agent mesh
    ├── test_agent_mesh_*.py         # [Layer 3] Agent mesh simulation & data loss suites
    ├── test_challenger_*.py         # [Layer 3] Burst, process termination, subprocess safety
    └── ...                          # Other Rust integration tests wired to Cargo.toml
```

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| F1 | Git Snapshot & Staging | Snapshot local uncommitted additions (crates/agent_mesh, crates/android_bridge, packaging, tests) into feature branch | M1 | Survey 1, R1 |
| F2 | Fast-Forward Master to Remote | Fast-forward local master to origin/feat/cross-machine-network-sync (commit 52af543) | M1 | Survey 1, R1 |
| F3 | Zero-Data-Loss Branch Merge | Merge local additions branch into master, reconciling README.md cleanly | M1 | Survey 1, R1 |
| F4 | Chronological Request Reconciliation | Reconcile ORIGINAL_REQUEST.md conflict block in chronological order with zero requirement loss | M1 | Survey 1, R1 |
| F5 | 4-Layer Modular Decoupling | Streamline repository into 4 decoupled layers (Rust Core, Android Bridge, Agent Mesh, Sync Network) | M2 | Survey 2, R2 |
| F6 | Binary Alias Branding | Add [[bin]] name = "oxideswarm" in crates/cli/Cargo.toml alongside rusty-grid | M2 | Survey 2, R2 |
| F7 | Redundancy & Stray Log Cleanup | Delete 8 stray .log files in windows/, add test JSON dumps to .gitignore, deprecate obsolete scripts | M2 | Survey 2, R2 |
| F8 | Hardcoded Path Sanitization | Sanitize 7 machine-specific developer paths (/Users/duongnad/...) and macOS Chrome paths to dynamic discovery | M3 | Survey 2, R3 |
| F9 | Worker Process Tree Termination | Implement process tree termination (taskkill /F /T on Windows, process groups on Unix) in TaskRunner | M3 | Survey 3, R3 |
| F10 | Master Bounded Memory & LRU Retention | Implement task retention TTL and LRU eviction for terminal states in TaskQueue and WorkerRegistry | M3 | Survey 3, R3 |
| F11 | Android JNI Clean Shutdown | Manage P2P endpoint closure within dedicated Tokio worker thread in Android bridge | M3 | Survey 3, R3 |
| F12 | Discovery TCP RST Fix | Drain incoming HTTP request stream before writing response in discovery candidate probing | M3 | Survey 3, R3 |
| F13 | Cross-Platform Windows Bash Test Fix | Resilient Git Bash detection / Windows fallback for shell invocation integration tests | M3 | Survey 3, R3 |
| F14 | Cargo Workspace Test Verification | Execute cargo check --workspace and cargo test --workspace with 100% pass | M4 | Survey 3, R4 |
| F15 | Python Agent Mesh & Challenger Verification | Run python tests/run_all_mesh_tests.py and all test_challenger_*.py scripts | M4 | Survey 3, R4 |
| F16 | Cross-Machine Sync Network Validation | Validate protocol compliance and syntax of scripts/sync_network/ | M4 | Survey 3, R4 |
| F17 | Master Documentation Synchronization | Synchronize README.md, PROJECT.md at root, and specs with 4-layer architecture & usage | M5 | Survey 2, R5 |
| F18 | Conventional Commits & Clean Tree | Create clean conventional commits and verify git status is clean | M5 | Survey 1, R5 |
| F19 | Push to origin/master | Push consolidated master branch to origin/master | M5 | User Request R5 |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Git Branch Consolidation & Conflict Resolution | Snapshot local work, advance master to remote, merge feature branch, reconcile ORIGINAL_REQUEST.md chronologically | None | DONE |
| M2 | Architectural Reorganization & Modular Decoupling | Structure 4 layers, add oxideswarm binary alias, remove stray logs, gitignore test dumps, deprecate obsolete scripts | M1 | DONE |
| M3 | Performance Optimization & Modernization | Worker process tree kill, Master memory bounding, Android JNI clean shutdown, path sanitization, discovery TCP fix, bash test fix | M2 | DONE |
| M4 | Comprehensive Verification & Testing | cargo check, cargo test --workspace, python mesh tests, challenger stress suites, sync network validation | M3 | DONE |
| M5 | Git Delivery & Documentation Synchronization | Update README.md, PROJECT.md at root, clean commit history, push to origin/master | M4 | DONE |

## Code Layout
- `crates/core`: Core primitives, wire codecs, Iroh QUIC transport, discovery
- `crates/master`: Master cluster coordinator, scheduler, task queue, web UI & dashboard
- `crates/worker`: Worker daemon, task runner, load telemetry, sandboxing
- `crates/cli`: CLI binaries (`rusty-grid`, `oxideswarm`, `ox-mode`)
- `crates/android_bridge`: JNI cdylib (`liboxideworker.so`)
- `crates/agent_mesh`: WebSocket relay hub, multi-platform agent client, process executor
- `packaging/`: Platform service configs (Android app, macOS plist, Windows service, Agent mesh runners)
- `scripts/`: Operational scripts and `sync_network` mailbox suite
- `benchmarks/`: Interconnect benchmarks and comparative whitepapers
- `tests/`: Integration tests and Python test suites
