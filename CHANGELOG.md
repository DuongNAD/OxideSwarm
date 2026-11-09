# Changelog

All notable changes to OxideSwarm are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed
- `oxideswarm workers` printed every registered worker under "Connected Workers", including workers that had disconnected, and `status` counted them as connected. Both now count only workers whose `state:` tag is `connected`, and `workers` marks the others, for example `[disconnected]`. Use `workers --json` in scripts.
- Stop raising modal pop-up dialogs and Windows Firewall prompts on Windows desktops: workers now call `SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX)` at start-up (and before bounded capability probes) so child task processes inherit it even with `--isolation off` instead of hanging on 16-bit/64-bit incompatibility or crash dialogs (`crates/core/src/capabilities.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/cli/src/cli_app.rs`); a master listening on loopback no longer binds the `0.0.0.0:<discovery_port>` UDP discovery socket (`crates/master/src/server.rs`); and a worker targeting a loopback master binds its standby HTTP portal to `127.0.0.1` instead of `0.0.0.0` (`crates/worker/src/client.rs`).

## [0.1.0] - 2026-11-09

The first tagged release: archives for Linux, macOS and Windows, with and without `gpu-wgpu`, and
`SHA256SUMS` on the [Releases page](https://github.com/DuongNAD/OxideSwarm/releases). The user story
it ships: a coding agent on one machine runs `cargo test` on another machine of the grid, and the
second run reuses the remote `target/` ([offload guide](docs/guides/offload-cargo-test.md)).

### Upgrading

Every machine of a cluster must run 0.1.0. The wire protocol is version 7
(`PROTOCOL_VERSION_CURRENT` in `crates/core/src/protocol.rs`), and the oldest version a peer accepts
is also 7, so a master, worker or client built from an earlier commit is refused with an
`Incompatible protocol version` error instead of misreading messages. In a running cluster:

1. Stop the master cleanly (`SIGTERM`, so a master with a format-1 `--state-dir` writes its snapshot
   first, [design 0046](docs/design/0046-master-durability.md)), replace its binary and start it.
2. Replace and restart the workers. The upgraded master refuses a worker that still runs an
   earlier build, before it registers.
3. Replace the binary that clients, scripts and `oxideswarm mcp` use.

The order and the reasons are in [DEPLOYMENT.md](DEPLOYMENT.md) section 3.5.

### Added
- Cluster authentication v2 (`crates/core/src/cluster_auth.rs`) and ChaCha20-Poly1305 AEAD record layer (`0x04`) for post-handshake TCP/QUIC frames ([design 0005](docs/design/0005-cluster-auth-v2-record-layer.md)).
- Content-addressed blob data plane (`crates/core/src/blob.rs`, `crates/master/src/blob_store.rs`, `crates/worker/src/blob_cache.rs`) for deduplicated file staging and artifact retrieval ([design 0006](docs/design/0006-content-addressed-data-plane.md)).
- Concurrent MCP stdio server (`oxideswarm mcp`) with per-call `wait_secs` deadlines, task cancellation (`cancel` tool and `notifications/cancelled`), and `notifications/progress` frames ([design 0004](docs/design/0004-mcp-and-discovery.md), [design 0012](docs/design/0012-mcp-concurrent-deadlines-and-cancel.md)).
- CLI `oxideswarm cancel <TASK_ID>` and `oxideswarm result <TASK_ID> [--wait] [--json] [--output-dir]` commands ([design 0010](docs/design/0010-cli-cancel-and-result.md)).
- Pure-Rust Tier-0 analytical SDF 3D generator (`crates/worker/src/gen3d`), Blender/3D process supervision, and 15-joint topological DAG auto-rigger (`crates/worker/src/autorig`) ([design 0009](docs/design/0009-blender-and-3d-process-supervision.md), [design 0015](docs/design/0015-engine-names-and-cpu-fallback.md), [design 0017](docs/design/0017-honest-3d-sdf-fallback.md)).
- Hardened subprocess execution (`crates/worker/src/runner.rs`) with process-group / Windows Job Object cleanup, environment sanitization, output validation/harvesting, and private Unix sandbox base `<temp>/rusty_grid-<euid>/sandboxes` ([design 0013](docs/design/0013-task-output-validation-and-harvesting.md), [design 0021](docs/design/0021-private-default-sandbox-base.md)).
- Agent Mesh command whitelist (`echo`, `ping`, `system_info`, `sha256_verify`, `shell_exec`), `--no-shell` restriction, secret environment variable sanitization (`SECRET_ENV_VARS`), 1 MiB stream capping, stdin detachment, process-tree termination on timeout, and failing unresolved pending commands/ACKs on WebSocket disconnect in `scripts/agent_node.py` ([design 0011](docs/design/0011-agent-mesh-hardening.md), [design 0019](docs/design/0019-agent-node-disconnect-fails-pending-commands.md)).
- Master durability: `master --state-dir <DIR>` keeps queued, running and finished tasks in a journal and snapshot and replays them at start. A worker keeps running its tasks across a short disconnect, and the master adopts them again when it resumes the session (`--session-resume-hold-secs`, `--session-resume-grace-secs`) ([design 0046](docs/design/0046-master-durability.md)).
- OS-level task isolation on macOS (`sandbox-exec`), Linux (Landlock, seccomp, namespaces) and Windows (per-task AppContainer profile, `oxideswarm.toolchain` capability SID and private Desktop): `worker --isolation off|fs|fs+net`, an `iso:` worker tag, `submit --require-isolation` (`require:iso=`), `windows/revoke-isolation-acls.ps1` (removes the toolchain grants that Windows isolation records) and an isolation column in `oxideswarm doctor`. On Windows, `cargo` does not run under `--isolation fs|fs+net` yet, so a Windows worker refuses `--workspaces` together with isolation (`FEAT-3d`) ([design 0033](docs/design/0033-os-level-task-isolation.md)).
- Workspaces: protocol 7 adds `Task::workspace`, a `Hello` version probe and `ClientMessage::ListWorkspaces`, a worker started with `--workspaces` runs a workspace task in a persistent synced tree (with a lock per workspace and size bounds, so `target/` survives between tasks), the master places a workspace task with affinity and one task at a time, and the client scans a source tree with `.gitignore` and limits (`submit --workspace`, `--sync`/`--no-sync`, `--cwd`, `oxideswarm workspaces`, and the MCP `workspace` field and `workspaces` tool). Client commands and `oxideswarm mcp` also accept an iroh P2P ticket via `--master <TICKET>`, `OXIDESWARM_P2P_TICKET`, or `[status].master` over `STREAM_CLIENT` (`0x03`) ([design 0039](docs/design/0039-agent-workspaces.md)).
- Tool probes and Cargo builds: workers advertise `tool:cargo`, `tool:rustc` and `tool:git`, `submit --require-tool` and the MCP `tools` field pin a task with `require:tool=`, and a compile task with a `Cargo.toml` runs `cargo build --offline` (`submit --type compile --source`, MCP `type: "compile"`) ([design 0039](docs/design/0039-agent-workspaces.md) D8, [design 0020](docs/design/0020-bounded-host-capability-probes.md)).
- Several blob uploads in flight from one client, and up to 16 blob requests handled at once per client connection on the master ([design 0006](docs/design/0006-content-addressed-data-plane.md), [design 0039](docs/design/0039-agent-workspaces.md) W9).
- `oxideswarm doctor` shows whether each worker link is direct or relayed (`LINK` column, check C7) and measures blob throughput to each worker (check C8, `--skip-throughput`) ([design 0001](docs/design/0001-two-node-readiness.md)).
- `oxideswarm secret join-token` and `oxideswarm join`: one token, read from standard input, carries the master's address or ticket and the cluster secret, writes the secret at mode 600 and runs the `doctor` checks ([design 0048](docs/design/0048-join-token.md)).
- Release workflow `.github/workflows/release.yml`: archives for Linux, macOS and Windows, with and without `gpu-wgpu`, plus `SHA256SUMS`, built on a `v*` tag, and an install section in `README.md`.
- `--relay-url <URL>` (`RUSTY_GRID_RELAY_URL`, `relay_url`) for the master, workers and clients, so P2P traffic can use your own iroh relay instead of the public n0 relays, and a [self-hosted relay guide](docs/guides/self-hosted-relay.md) ([design 0049](docs/design/0049-custom-relay.md)).
- Guide [Offload cargo test to another machine](docs/guides/offload-cargo-test.md), from the command line and over MCP.
- Benchmarks `benchmarks/grid/bench_grid.py` for task overhead, repeated uploads and the offload of `cargo test` to a workspace on another machine (`--only c`), each with a dated record ([benchmarks/grid/README.md](benchmarks/grid/README.md)).

### Changed
- Removed `oxideswarm mcp --sse` in favour of stdio-only MCP transport and fail-closed client secret enforcement on non-loopback masters ([design 0004](docs/design/0004-mcp-and-discovery.md)).
- Enforced bounded host capability probes, active queue budgets, oversized task result handling, and inbound JSON/blob request routing ([design 0008](docs/design/0008-oversized-task-results-and-active-queue-budget.md), [design 0018](docs/design/0018-inbound-json-blob-request-routing.md), [design 0020](docs/design/0020-bounded-host-capability-probes.md)).
- Protocol version 7. Version 4 removed `compute_intensity` from the GPU task spec and added `ResumeSession`, version 6 added the `Hello` probe and `Task::workspace`, and version 7 added `ListWorkspaces` / `WorkspacesList`. Older peers are refused ([design 0040](docs/design/0040-protocol-versioning.md), [design 0039](docs/design/0039-agent-workspaces.md)).
- For client commands (`submit`, `status`, `doctor`, `cancel`, `result`, `workspaces`, `mapreduce`, and `join`), `OXIDESWARM_P2P_TICKET` and `OXIDESWARM_MASTER` now outrank the legacy `RUSTY_GRID_MASTER_ADDR` environment variable ([design 0039](docs/design/0039-agent-workspaces.md) D7).
- `gen3d` and `autorig` build only with the opt-in Cargo feature `3d`; a worker without it does not take 3D tasks. Dead code, the Android lease shims and the second connection loop of the master were removed, and leftover task sandboxes are swept at worker start-up ([design 0044](docs/design/0044-dead-code-and-connection-loop-unification.md)).

### Security
- Private files on Windows (the cluster secret and similar) get an owner-only DACL, as mode 600 does on Unix ([design 0047](docs/design/0047-windows-private-files.md)).
- After a worker is killed with `kill -9`, a supervisor per task group ends the task's processes within 5 s on Linux and macOS ([design 0031](docs/design/0031-containment-after-worker-crash.md)).
- Agent Mesh refuses binary inputs that are neither base64 nor hex, and no longer reads a hub-local file named by a peer.

### Baseline

What the workspace already held before this changelog was kept:

- Six-crate Rust workspace (`rusty_grid_core`, `rusty_grid_master`, `rusty_grid_worker`, `rusty_grid_cli`, `rusty_grid_android_bridge` (`oxideworker` library), `agent_mesh`).
- Master task scheduler, worker capability matching, failover sweeps, UDP LAN discovery (`8089/udp`), and iroh QUIC P2P transport (`--p2p`).
