# Changelog

All notable changes to OxideSwarm are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Upgrading

This release changes the wire protocol: the master's `PROTOCOL_VERSION_CURRENT` is now 6
(`crates/core/src/protocol.rs`), and a peer outside the supported range is refused. Upgrade and
restart the master first, then the workers and clients ([DEPLOYMENT.md](DEPLOYMENT.md) section 3.5).
A master that already has a format-1 `--state-dir` must be stopped cleanly (`SIGTERM`, so it writes
its snapshot) before the binary is replaced ([design 0046](docs/design/0046-master-durability.md)).

### Added
- Cluster authentication v2 (`crates/core/src/cluster_auth.rs`) and ChaCha20-Poly1305 AEAD record layer (`0x04`) for post-handshake TCP/QUIC frames ([design 0005](docs/design/0005-cluster-auth-v2-record-layer.md)).
- Content-addressed blob data plane (`crates/core/src/blob.rs`, `crates/master/src/blob_store.rs`, `crates/worker/src/blob_cache.rs`) for deduplicated file staging and artifact retrieval ([design 0006](docs/design/0006-content-addressed-data-plane.md)).
- Concurrent MCP stdio server (`oxideswarm mcp`) with per-call `wait_secs` deadlines, task cancellation (`cancel` tool and `notifications/cancelled`), and `notifications/progress` frames ([design 0004](docs/design/0004-mcp-and-discovery.md), [design 0012](docs/design/0012-mcp-concurrent-deadlines-and-cancel.md)).
- CLI `oxideswarm cancel <TASK_ID>` and `oxideswarm result <TASK_ID> [--wait] [--json] [--output-dir]` commands ([design 0010](docs/design/0010-cli-cancel-and-result.md)).
- Pure-Rust Tier-0 analytical SDF 3D generator (`crates/worker/src/gen3d`), Blender/3D process supervision, and 15-joint topological DAG auto-rigger (`crates/worker/src/autorig`) ([design 0009](docs/design/0009-blender-and-3d-process-supervision.md), [design 0015](docs/design/0015-engine-names-and-cpu-fallback.md), [design 0017](docs/design/0017-honest-3d-sdf-fallback.md)).
- Hardened subprocess execution (`crates/worker/src/runner.rs`) with process-group / Windows Job Object cleanup, environment sanitization, output validation/harvesting, and private Unix sandbox base `<temp>/rusty_grid-<euid>/sandboxes` ([design 0013](docs/design/0013-task-output-validation-and-harvesting.md), [design 0021](docs/design/0021-private-default-sandbox-base.md)).
- Agent Mesh command whitelist (`echo`, `ping`, `system_info`, `sha256_verify`, `shell_exec`), `--no-shell` restriction, secret environment variable sanitization (`SECRET_ENV_VARS`), 1 MiB stream capping, stdin detachment, process-tree termination on timeout, and failing unresolved pending commands/ACKs on WebSocket disconnect in `scripts/agent_node.py` ([design 0011](docs/design/0011-agent-mesh-hardening.md), [design 0019](docs/design/0019-agent-node-disconnect-fails-pending-commands.md)).
- Master durability: `master --state-dir <DIR>` keeps queued, running and finished tasks in a journal and snapshot and replays them at start. A worker keeps running its tasks across a short disconnect, and the master adopts them again when it resumes the session (`--session-resume-hold-secs`, `--session-resume-grace-secs`) ([design 0046](docs/design/0046-master-durability.md)).
- OS-level task isolation on macOS (`sandbox-exec`) and Linux (Landlock, seccomp, namespaces): `worker --isolation off|fs|fs+net`, an `iso:` worker tag, `submit --require-isolation` (`require:iso=`) and an isolation column in `oxideswarm doctor`. Windows is not covered yet ([design 0033](docs/design/0033-os-level-task-isolation.md)).
- Workspaces groundwork, not usable end to end yet: protocol 6 adds `Task::workspace` and a `Hello` version probe, the worker has a workspace store with sync, locks and size bounds, the master places a workspace task with affinity and one task at a time, and the client scans a source tree with `.gitignore` and limits. The `submit --workspace` command and the MCP tools are still open ([design 0039](docs/design/0039-agent-workspaces.md)).
- Tool probes and Cargo builds: workers advertise `tool:cargo`, `tool:rustc` and `tool:git`, `submit --require-tool` and the MCP `tools` field pin a task with `require:tool=`, and a compile task with a `Cargo.toml` runs `cargo build --offline` (`submit --type compile --source`, MCP `type: "compile"`) ([design 0039](docs/design/0039-agent-workspaces.md) D8, [design 0020](docs/design/0020-bounded-host-capability-probes.md)).
- Several blob uploads in flight from one client, and up to 16 blob requests handled at once per client connection on the master ([design 0006](docs/design/0006-content-addressed-data-plane.md), [design 0039](docs/design/0039-agent-workspaces.md) W9).
- `oxideswarm doctor` shows whether each worker link is direct or relayed (`LINK` column, check C7) and measures blob throughput to each worker (check C8, `--skip-throughput`) ([design 0001](docs/design/0001-two-node-readiness.md)).
- `oxideswarm secret join-token` and `oxideswarm join`: one token, read from standard input, carries the master's address or ticket and the cluster secret, writes the secret at mode 600 and runs the `doctor` checks ([design 0048](docs/design/0048-join-token.md)).
- Release workflow `.github/workflows/release.yml`: archives for Linux, macOS and Windows, with and without `gpu-wgpu`, plus `SHA256SUMS`, built on a `v*` tag, and an install section in `README.md`. No release has been tagged yet.
- Benchmarks `benchmarks/grid/bench_grid.py` for task overhead and repeated uploads, with a dated record from one machine.

### Changed
- Removed `oxideswarm mcp --sse` in favour of stdio-only MCP transport and fail-closed client secret enforcement on non-loopback masters ([design 0004](docs/design/0004-mcp-and-discovery.md)).
- Enforced bounded host capability probes, active queue budgets, oversized task result handling, and inbound JSON/blob request routing ([design 0008](docs/design/0008-oversized-task-results-and-active-queue-budget.md), [design 0018](docs/design/0018-inbound-json-blob-request-routing.md), [design 0020](docs/design/0020-bounded-host-capability-probes.md)).
- Protocol version 6. Version 4 removed `compute_intensity` from the GPU task spec and added `ResumeSession`, and version 6 added the `Hello` probe and `Task::workspace`. Older peers are refused ([design 0040](docs/design/0040-protocol-versioning.md)).
- `gen3d` and `autorig` build only with the opt-in Cargo feature `3d`; a worker without it does not take 3D tasks. Dead code, the Android lease shims and the second connection loop of the master were removed, and leftover task sandboxes are swept at worker start-up ([design 0044](docs/design/0044-dead-code-and-connection-loop-unification.md)).

### Security
- Private files on Windows (the cluster secret and similar) get an owner-only DACL, as mode 600 does on Unix ([design 0047](docs/design/0047-windows-private-files.md)).
- After a worker is killed with `kill -9`, a supervisor per task group ends the task's processes within 5 s on Linux and macOS ([design 0031](docs/design/0031-containment-after-worker-crash.md)).
- Agent Mesh refuses binary inputs that are neither base64 nor hex, and no longer reads a hub-local file named by a peer.

## [0.1.0] - Initial Workspace Baseline

### Added
- Six-crate Rust workspace (`rusty_grid_core`, `rusty_grid_master`, `rusty_grid_worker`, `rusty_grid_cli`, `rusty_grid_android_bridge` (`oxideworker` library), `agent_mesh`).
- Master task scheduler, worker capability matching, failover sweeps, UDP LAN discovery (`8089/udp`), and iroh QUIC P2P transport (`--p2p`).
