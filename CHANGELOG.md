# Changelog

All notable changes to OxideSwarm are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Machine identity and OXS-AUTH v3 (`ENROLL-1a`): per-machine X25519 static keys (`~/.oxideswarm/node.key` and `master_noise.key`, or in `--state-dir` when set, stored as private files and printed with key fingerprint on master startup), Noise `IK` (`Noise_IK_25519_ChaChaPoly_BLAKE2s`) authentication with cleartext `HELLO` prologue and `Split()` key derivation into the `0x04` ChaCha20-Poly1305 record layer, `members.json` read at master startup with worker-role enforcement on `Register`, `--master-key <HEX>` (`RUSTY_GRID_MASTER_KEY`, `master_key`), and `master --auth secret|members|both` (`RUSTY_GRID_AUTH`, `[master].auth`) ([design 0051](docs/design/0051-machine-identity-and-invites.md)).
- Windows AppContainer support for offline `cargo` builds and `--workspaces` under `--isolation fs|fs+net` on non-elevated workers (`FEAT-3d`, [design 0033](docs/design/0033-os-level-task-isolation.md) D4, [design 0039](docs/design/0039-agent-workspaces.md)): `<ws>/tree` and `<ws>/tmp` are confined to the per-attempt AppContainer SID `oxideswarm.task.<worker8>.<run_token>` while `<ws>/state.json`, `<ws>/lock`, and sibling workspaces stay unreadable and unwritable; `validate_sandbox_base_safe` accepts the default `%TEMP%\rusty_grid\sandboxes` directory under `host.user_home`; and a Windows worker started elevated with `--isolation fs|fs+net` logs a startup warning and omits `tool:cargo` from its advertised capability tags because elevated logon sessions disallow opening `NUL` inside an AppContainer (`crates/worker/src/isolation.rs`, `crates/worker/src/client.rs`, `crates/worker/src/runner.rs`).
- `packaging/usb/make_kit.sh --no-autostart` writes a trial kit: its installers add no start at login. The kit's workers use half of the CPU cores ([design 0053](docs/design/0053-usb-setup-kit.md)).
- `packaging/usb/make_kit.sh --gpu` also puts the `gpu-wgpu` (`-gpu`) archives on the kit. The installers then ask whether this machine's GPU should do work and install the GPU build on yes; `install.sh --gpu|--no-gpu` and `install.ps1 -Gpu|-NoGpu` answer in advance ([design 0053](docs/design/0053-usb-setup-kit.md) D2).
- USB kit: a reinstall keeps the machine's worker id (a SHA-256 of the machine id, name and cluster secret written to `worker_id.<name>` before the first start), so the master shows one worker per machine; the `Install`, `Update` and `Uninstall` launchers end with an offer to eject the stick ([design 0053](docs/design/0053-usb-setup-kit.md)).
- USB kit updates in place: `Update OxideSwarm.command` / `Update OxideSwarm.cmd` (`install.sh --update`, `install.ps1 -Update`) replace the binaries and restart the worker while keeping its folder, name, GPU choice, cluster membership and start at login, without asking anything or joining again ([design 0053](docs/design/0053-usb-setup-kit.md)).
- The Windows USB installer can keep its files on another fixed NTFS or ReFS drive: it asks when one exists (or takes `-InstallRoot`), limits that folder to the user, `SYSTEM` and `Administrators`, puts the task folders, file cache and workspaces inside it (`--sandbox-base-dir`, `--workspace-root`), and records it in `HKCU\Software\OxideSwarm\UsbKit` for `uninstall.ps1` ([design 0053](docs/design/0053-usb-setup-kit.md)).

### Changed
- Windows workers create a missing task sandbox base and workspace root with the owner-only DACL (current user, `SYSTEM`, `Administrators`), and warn with the `icacls` fix when an existing one lets any local account read or change task files ([design 0021](docs/design/0021-private-default-sandbox-base.md)).
- The Windows USB installer skips cloud or virtual drives (no disk partition) and external USB disks when it offers or checks an install folder ([design 0053](docs/design/0053-usb-setup-kit.md)).

### Fixed
- P2P errors now include the operating system's reason. A worker or client that could not open its P2P sockets (seen with a VPN active on the machine) only printed `Failed to bind iroh endpoint: Failed to bind sockets`; the message now ends with the cause, for example `: Can't assign requested address`. The same applies to `P2P connect failed` and to opening a QUIC stream (`rusty_grid_core::error::display_with_sources`).
- The Windows USB installer passes a P2P ticket intact under Windows PowerShell 5.1, which dropped its double quotes, and the installers' checks no longer break on log lines that release 0.1.x writes to standard output.
- CLI client commands (`doctor`, `workers`, `submit`, `status`, `result`, `cancel`, `workspaces`, `mapreduce`, `join`, `secret`, `mcp`) now close their ephemeral `iroh::Endpoint`s (`P2pClientGuard` and `BlobConnection`) gracefully before exiting instead of logging `ERROR iroh::socket: Endpoint dropped without calling Endpoint::close. Aborting ungracefully.` to `stderr`, and route all client-command tracing to `stderr` so `iroh` `INFO` lines over a P2P ticket never land on `stdout` ahead of `--json` output (`LOG-1`, [design 0039](docs/design/0039-agent-workspaces.md)).

## [0.1.1] - 2026-11-10

A patch release with fixes found while running 0.1.0 on real desktops. The protocol is still
version 7, so 0.1.0 and 0.1.1 machines work together; upgrade in any order.

### Added
- The USB setup kit (`packaging/usb/`, [design 0053](docs/design/0053-usb-setup-kit.md)) also works where machines cannot reach each other directly, as on school or office Wi-Fi with client isolation. When the join token carries a P2P ticket (`secret join-token --p2p-ticket-file ...`, from a master started with `--p2p`), the installers run their checks through that ticket, as the worker does.

### Fixed
- `oxideswarm workers` printed every registered worker under "Connected Workers", including workers that had disconnected, and `status` counted them as connected. Both now count only workers whose `state:` tag is `connected`, and `workers` marks the others, for example `[disconnected]`. Use `workers --json` in scripts.
- Stop raising modal pop-up dialogs and Windows Firewall prompts on Windows desktops: workers now call `SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX)` at start-up (and before bounded capability probes) so child task processes inherit it even with `--isolation off` instead of hanging on 16-bit/64-bit incompatibility or crash dialogs (`crates/core/src/capabilities.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/cli/src/cli_app.rs`); a master listening on loopback now binds its UDP discovery socket to that loopback address (`127.0.0.1:<discovery_port>`) instead of `0.0.0.0:<discovery_port>` and answers local unicast probes without sending broadcast beacons (`crates/master/src/server.rs`); and a worker targeting a loopback master binds its standby HTTP portal to `127.0.0.1` instead of `0.0.0.0` (`crates/worker/src/client.rs`).

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
