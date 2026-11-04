# Changelog

All notable changes to OxideSwarm are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Cluster authentication v2 (`crates/core/src/cluster_auth.rs`) and ChaCha20-Poly1305 AEAD record layer (`0x04`) for post-handshake TCP/QUIC frames ([design 0005](docs/design/0005-cluster-auth-v2-record-layer.md)).
- Content-addressed blob data plane (`crates/core/src/blob.rs`, `crates/master/src/blob_store.rs`, `crates/worker/src/blob_cache.rs`) for deduplicated file staging and artifact retrieval ([design 0006](docs/design/0006-content-addressed-data-plane.md)).
- Concurrent MCP stdio server (`oxideswarm mcp`) with per-call deadlines, cancellation (`cancel` tool and `notifications/cancelled`), progress notifications, head+tail output bounding, and artifact writing ([design 0004](docs/design/0004-mcp-and-discovery.md), [design 0012](docs/design/0012-mcp-concurrent-deadlines-and-cancel.md)).
- CLI `oxideswarm cancel <TASK_ID>` and `oxideswarm result <TASK_ID> [--wait] [--json] [--output-dir]` commands ([design 0010](docs/design/0010-cli-cancel-and-result.md)).
- Pure-Rust Tier-0 analytical SDF 3D generator (`crates/worker/src/gen3d`), Blender/3D process supervision, and 15-joint topological DAG auto-rigger (`crates/worker/src/autorig`) ([design 0009](docs/design/0009-blender-and-3d-process-supervision.md), [design 0015](docs/design/0015-engine-names-and-cpu-fallback.md), [design 0017](docs/design/0017-honest-3d-sdf-fallback.md)).
- Hardened subprocess execution (`crates/worker/src/runner.rs`) with process-group / Windows Job Object cleanup, environment sanitization, output validation/harvesting, and private Unix sandbox base `<temp>/rusty_grid-<euid>/sandboxes` ([design 0013](docs/design/0013-task-output-validation-and-harvesting.md), [design 0021](docs/design/0021-private-default-sandbox-base.md)).
- Agent Mesh capability enforcement, `--no-shell` restriction, `wss://` TLS validation (`rustls-tls-webpki-roots`), inbox persistence, disconnect cleanup, and external master attachment (`--master-addr`) ([design 0011](docs/design/0011-agent-mesh-hardening.md), [design 0019](docs/design/0019-agent-node-disconnect-fails-pending-commands.md)).

### Changed
- Removed `oxideswarm mcp --sse` in favour of stdio-only MCP transport and fail-closed client secret enforcement on non-loopback masters ([design 0004](docs/design/0004-mcp-and-discovery.md)).
- Enforced bounded host capability probes, active queue budgets, oversized task result handling, and inbound JSON/blob request routing ([design 0008](docs/design/0008-oversized-task-results-and-active-queue-budget.md), [design 0018](docs/design/0018-inbound-json-blob-request-routing.md), [design 0020](docs/design/0020-bounded-host-capability-probes.md)).

## [0.1.0] - Initial Workspace Baseline

### Added
- Six-crate Rust workspace (`rusty_grid_core`, `rusty_grid_master`, `rusty_grid_worker`, `rusty_grid_cli`, `oxideswarm_mobile_ffi`, `agent_mesh`).
- Master task scheduler, worker capability matching, failover sweeps, UDP LAN discovery (`8089/udp`), and iroh QUIC P2P transport (`--p2p`).
