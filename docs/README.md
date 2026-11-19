# Documentation

Index of the OxideSwarm documentation and the standard every document follows. Read the standard
before adding or changing a document.

## Index

| Document | Scope | Audience |
|---|---|---|
| [README.md](../README.md) | Overview: what it does, install, quick start, adding machines, status and limits | Users |
| [reference.md](reference.md) | Reference: install details, cluster set-up, troubleshooting, platform support, the full status and limits, CLI flags, ports, environment variables, Agent Mesh summary, development and tests | Users, contributors |
| [guides/two-machine-cluster.md](guides/two-machine-cluster.md) | Runbook: a macOS machine and a Windows machine as one cluster, verified with `oxideswarm doctor` | Users |
| [guides/add-a-machine.md](guides/add-a-machine.md) | Runbook: add machines via pairing code (`JOIN-2`) or a USB setup kit (`USB-1`) | Users |
| [guides/self-hosted-relay.md](guides/self-hosted-relay.md) | Runbook: run a self-hosted `iroh-relay` server and point master, worker, and clients at it with `--relay-url` | Users |
| [guides/mcp-client-setup.md](guides/mcp-client-setup.md) | Runbook: connect Claude Code, Codex, or Cursor to `oxideswarm mcp` over stdio | Users, coding agents |
| [guides/offload-cargo-test.md](guides/offload-cargo-test.md) | Runbook: offload `cargo test` from a local checkout to a persistent worker workspace via `submit --workspace` or `oxideswarm mcp` | Users, coding agents |
| [guides/multi-agent-collaboration.md](guides/multi-agent-collaboration.md) | Runbook: cross-machine coding agent coordination and root-cause diagnostics via `OxideSwarm_Agent_Hub` | Coding agents |
| [guides/gpu-workloads.md](guides/gpu-workloads.md) | Runbook: run GPU command tasks (CUDA, OptiX, Metal, `wgpu`, Blender, PyTorch) under `--isolation fs` with `--isolation-read-dir` and `--vram-mb` | Users, coding agents |
| [guides/cut-a-release.md](guides/cut-a-release.md) | Runbook: prepare a release pull request, tag, verify `SHA256SUMS` and provenance attestations, and smoke-test the macOS and Windows archives (`LV-15`) | Owner, releasers |
| [fuzz/README.md](../fuzz/README.md) | Runbook: the `cargo fuzz` targets for the frame reader, the server handshake and `TreeManifest::decode` (`LV-1`), the encrypted record layer, join tokens and invites, the master's journal and snapshot readers, and the JSON-RPC reader of `oxideswarm mcp` (`LV-18`), how to run them, the one-hour and five-minute records, open targets | Contributors, coding agents |
| [SECURITY.md](SECURITY.md) | Threat model, cluster secret, ports, tokens, who runs tasks | Users, reviewers |
| [DEPLOYMENT.md](../DEPLOYMENT.md) | Agent Mesh deployment and operations (not the compute grid) | Users |
| [PROJECT.md](../PROJECT.md) | Architecture, workspace layout, feature inventory, and a link to the roadmap | Contributors |
| [CLAUDE.md](../CLAUDE.md) | Build and test commands, architecture map for coding agents | Coding agents |
| [ANDROID_ARCHITECTURE.md](../ANDROID_ARCHITECTURE.md) | Android design; parts are design only (see its status section) | Contributors |
| [windows/README.md](../windows/README.md) | Windows launchers | Users |
| [packaging/README.md](../packaging/README.md) and the READMEs below it | Background services: launchd, Windows service, Android app, Termux | Users |
| [scripts/sync_network/README.md](../scripts/sync_network/README.md) | Shared-folder reachability probe (not part of the grid) | Users |
| [benchmarks/interconnect/](../benchmarks/interconnect/COMPARATIVE_INTERCONNECT_BENCHMARK.md) | Recorded interconnect comparison and its validator | Contributors |
| [benchmarks/model-bringup/README.md](../benchmarks/model-bringup/README.md) | Spike R14 of design 0056: where the time goes between a machine with no model and the first result. Session 1 has a script and one record. Sessions for spikes R15 to R18 of design 0062 are Planned. Session 8 is spike R22 of design 0067 (the concurrency knee of a model server on the GPU machine), commands only, with one record | Owner, contributors |
| [benchmarks/gpu-run/README.md](../benchmarks/gpu-run/README.md) | GPU tasks across two machines: `wgpu`, PyTorch CUDA and Blender Cycles from a macOS master to a Windows worker, isolation off (one record, 2026-11-16). The run under `--isolation fs` is Planned | Owner, contributors |
| [benchmarks/campaigns/README.md](../benchmarks/campaigns/README.md) | Pilot campaign sheet, one-page participant guide, 200-unit manifest, and Python driver (`APP-6`, design 0094) | Owner, contributors |
| [design/](design/) | Design records, one per change to behaviour, CLI or wire format | Contributors |
| [design/0001-two-node-readiness.md](design/0001-two-node-readiness.md) | Implemented: Two-node readiness: task placement, task inputs and a cluster check | Contributors |
| [design/0002-mobile-coordination.md](design/0002-mobile-coordination.md) | Partial: Mobile coordination: thermal and battery throttling, adaptive keepalive and role arbitrage | Contributors |
| [design/0003-distributed-file-staging.md](design/0003-distributed-file-staging.md) | Implemented: Distributed file staging and output artifact collection | Contributors |
| [design/0004-mcp-and-discovery.md](design/0004-mcp-and-discovery.md) | Implemented: Model Context Protocol server and subnet-isolated discovery fallback | Contributors |
| [design/0005-cluster-auth-v2-record-layer.md](design/0005-cluster-auth-v2-record-layer.md) | Implemented: Cluster authentication v2 for the encrypted record layer | Contributors |
| [design/0006-content-addressed-data-plane.md](design/0006-content-addressed-data-plane.md) | Implemented: Content-addressed task inputs and output files | Contributors |
| [design/0007-agent-bridge-message-trigger.md](design/0007-agent-bridge-message-trigger.md) | Implemented: Message trigger for the agent bridge | Contributors |
| [design/0008-oversized-task-results-and-active-queue-budget.md](design/0008-oversized-task-results-and-active-queue-budget.md) | Implemented: Oversized task results and active queue byte budget | Contributors |
| [design/0009-blender-and-3d-process-supervision.md](design/0009-blender-and-3d-process-supervision.md) | Implemented: Process supervision for Blender and 3D workloads | Contributors |
| [design/0010-cli-cancel-and-result.md](design/0010-cli-cancel-and-result.md) | Implemented: CLI `cancel` and `result` subcommands | Contributors |
| [design/0011-agent-mesh-hardening.md](design/0011-agent-mesh-hardening.md) | Implemented: Agent Mesh Command Hardening, Process Controls, and Launcher Integration | Contributors |
| [design/0012-mcp-concurrent-deadlines-and-cancel.md](design/0012-mcp-concurrent-deadlines-and-cancel.md) | Implemented: Concurrent MCP server with client deadlines, cancellation, and progress | Contributors |
| [design/0013-task-output-validation-and-harvesting.md](design/0013-task-output-validation-and-harvesting.md) | Implemented: Task Output Validation, Mandatory Harvesting Failure Semantics, and Staging Flag Enforcement | Contributors |
| [design/0014-worker-max-concurrency-slots-tag.md](design/0014-worker-max-concurrency-slots-tag.md) | Implemented: Worker `--max-concurrency` (`slots:<n>`) Tag on the Master | Contributors |
| [design/0015-engine-names-and-cpu-fallback.md](design/0015-engine-names-and-cpu-fallback.md) | Implemented: 3D Engine Name Validation, Strict Fallback Removal, and Default CPU Execution | Contributors |
| [design/0016-mapreduce-cli-script-shipping.md](design/0016-mapreduce-cli-script-shipping.md) | Implemented: Ship Local Mapper/Reducer Scripts and Command Stages in `oxideswarm mapreduce` | Contributors |
| [design/0017-honest-3d-sdf-fallback.md](design/0017-honest-3d-sdf-fallback.md) | Implemented: Honest 3D Procedural SDF Fallback, Neural Model Refusal, and Environment Isolation | Contributors |
| [design/0018-inbound-json-blob-request-routing.md](design/0018-inbound-json-blob-request-routing.md) | Implemented: Unambiguous JSON routing for `InboundMessage::Worker(BlobRequest)` | Contributors |
| [design/0019-agent-node-disconnect-fails-pending-commands.md](design/0019-agent-node-disconnect-fails-pending-commands.md) | Implemented: Fail pending Python Agent Mesh commands on socket disconnect | Contributors |
| [design/0020-bounded-host-capability-probes.md](design/0020-bounded-host-capability-probes.md) | Implemented: Bounded host capability probes at worker start-up | Contributors |
| [design/0021-private-default-sandbox-base.md](design/0021-private-default-sandbox-base.md) | Implemented: Private task directories | Contributors |
| [design/0022-head-and-tail-stream-capture.md](design/0022-head-and-tail-stream-capture.md) | Implemented: Head-and-tail worker stream capture and midpoint truncation detection | Contributors |
| [design/0023-generative-3d-and-autorig-subsystem.md](design/0023-generative-3d-and-autorig-subsystem.md) | Generative 3D (`gen3d`) and skeletal auto-rigging (`autorig`) architecture, Python/Blender drivers (`engines/`), and current limits (`3D-2`, `3D-3`) | Contributors |
| [design/0024-wire-cli-flags-and-retire-ttl-eviction.md](design/0024-wire-cli-flags-and-retire-ttl-eviction.md) | Wire `--max-queue-size`, `--scheduler-policy`, `--wire-codec`, `[submit].timeout_secs`, and worker `--master` precedence, and remove dead TTL eviction (`SCHED-2`) | Contributors |
| [design/0025-windows-executable-resolution.md](design/0025-windows-executable-resolution.md) | Implemented: Windows task executable resolution and PE validation | Contributors |
| [design/0026-refuse-claim-master-role.md](design/0026-refuse-claim-master-role.md) | Refuse `WorkerMessage::ClaimMasterRole` on the master and drop `ROLE_ARBITRAGE` from `ProtocolCapabilities::all_supported()` (`FEAT-6`, decision D1) | Contributors |
| [design/0027-ox-mode-honest-reporting.md](design/0027-ox-mode-honest-reporting.md) | Move the `ox-mode` workflow-mode execution engine to `crates/cli` and report real benchmark, simulation, cluster status, test, and doc results (`MODE-1`) | Contributors |
| [design/0028-host-battery-probe-override.md](design/0028-host-battery-probe-override.md) | Host battery probe override (`WorkerConfig::with_no_battery` and `RUSTY_GRID_NO_BATTERY`) and start-up warning | Contributors |
| [design/0029-mesh-client-wss-tls.md](design/0029-mesh-client-wss-tls.md) | Enable `wss://` TLS in the Rust `agent-mesh` client and add `--tls-ca` (`OXIDE_MESH_TLS_CA`) for private TLS proxies (`MESH-1`) | Contributors |
| [design/0030-mcp-post-reply-cancel-and-p2p-endpoint-lifecycle.md](design/0030-mcp-post-reply-cancel-and-p2p-endpoint-lifecycle.md) | MCP post-reply `notifications/cancelled` (`completed_submits`), `4 s` stdio EOF grace window, and P2P master relay readiness (`5 s`) and endpoint shutdown | Contributors |
| [design/0031-containment-after-worker-crash.md](design/0031-containment-after-worker-crash.md) | Implemented: per-task lifeline pipe and supervisor so a task tree dies within 5 s after a worker `SIGKILL` on Linux, macOS, and Android (`FEAT-2`) | Contributors |
| [design/0032-p2p-handshake-refusal-delivery.md](design/0032-p2p-handshake-refusal-delivery.md) | P2P QUIC handshake and cluster-auth refusal delivery via stream finish and bounded `conn.closed()` drain (`BUG-17`) | Contributors |
| [design/0033-os-level-task-isolation.md](design/0033-os-level-task-isolation.md) | Implemented: OS-level task isolation (`--isolation off\|fs\|fs+net`) via Landlock/`seccomp`/namespaces on Linux (`FEAT-3b`), `sandbox-exec` on macOS (`FEAT-3a`), and per-task AppContainer on Windows (`FEAT-3c`), plus `iso:` tag, `require:iso=` placement and `doctor` reporting (`FEAT-3`, all three platforms implemented) | Contributors |
| [design/0034-fail-unfulfillable-tasks.md](design/0034-fail-unfulfillable-tasks.md) | Fail unfulfillable tasks (`--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`) when no registered worker can satisfy capabilities or placement constraints (`SCHED-3`) | Contributors |
| [design/0036-bounded-uncharged-worker-evacuations.md](design/0036-bounded-uncharged-worker-evacuations.md) | Bounded uncharged voluntary worker drains (`WorkerMessage::Disconnecting`, `RetryPolicy::max_evacuations`, `TaskInfo::evacuation_count`), `retry_history` audit entries, and `Running` `max_retries == 0` guard (`MOB-3`) | Contributors |
| [design/0038-large-language-model-inference.md](design/0038-large-language-model-inference.md) | Planned: language model inference on the grid, by expert routing to local model servers and by llama.cpp RPC over an authenticated tunnel (`LLM-1` to `LLM-5`) | Contributors |
| [design/0039-agent-workspaces.md](design/0039-agent-workspaces.md) | Implemented, W1–W11 built and recorded live on two machines (`Task::workspace`, `Hello`, `ListWorkspaces`, protocol version 7, `oxideswarm-tree/2`): persistent workspaces for coding agents on workers, synced from a local directory, with affinity and one task per workspace, plus P2P for clients and `cargo` builds (`FEAT-4`, `FEAT-10`) | Contributors |
| [design/0040-protocol-versioning.md](design/0040-protocol-versioning.md) | Protocol version 9 (initially 3), golden-bytes version-bump enforcement, `RegisterAck` feature-bit gating, and client-path protocol versioning (`FEAT-8`, `FEAT-1`, `ENROLL-1b`, `FEAT-9a`) | Contributors |
| [design/0044-dead-code-and-connection-loop-unification.md](design/0044-dead-code-and-connection-loop-unification.md) | Implemented: remove dead code, merge TCP and 2-lane QUIC worker connection loops into `WorkerSession`, sweep leftover sandboxes at start-up, and gate `gen3d`/`autorig` behind Cargo feature `3d` (`FEAT-11`, decision Q2) | Contributors |
| [design/0045-distributed-training.md](design/0045-distributed-training.md) | Planned, after the core of the roadmap: training on several workers with a framework the owner installs, by independent runs, low-communication rounds and adapter rounds (`TRAIN-1` to `TRAIN-5`), and a first workload for independent runs: grading edits to the weights of a small model (spike `R19`) | Contributors |
| [design/0046-master-durability.md](design/0046-master-durability.md) | Implemented: append-only task journal and snapshot store (`--state-dir`), startup replay, store-health reporting (`ClusterStatus.store_error`, protocol version 5), and worker session resume (`WorkerMessage::ResumeSession`) (`FEAT-1`) | Contributors |
| [design/0047-windows-private-files.md](design/0047-windows-private-files.md) | Implemented: files created as private on Windows (cluster secret, master P2P key, ticket file and their directory) get a protected DACL for the current user, SYSTEM and Administrators at creation, and loading them warns with the `icacls` fix when the DACL is wider (`SEC-9`) | Contributors |
| [design/0048-join-token.md](design/0048-join-token.md) | Implemented: `oxideswarm secret join-token` prints one token (master address, optional P2P ticket, cluster secret, integrity tag) and `oxideswarm join` reads it from standard input, saves the secret at mode 600 and the address in `rusty-grid.toml`, and runs the doctor checks (`JOIN-1`) | Contributors |
| [design/0049-custom-relay.md](design/0049-custom-relay.md) | Implemented: `--relay-url`, `RUSTY_GRID_RELAY_URL`, and `relay_url` across master, worker, and client P2P endpoints (`RelayMode::Custom` on `presets::Minimal`), URL validation, and self-hosted `iroh-relay` guide (`LINK-1`) | Contributors |
| [design/0050-scale-out-architecture.md](design/0050-scale-out-architecture.md) | Partial: limits of the single master with evidence, and a staged architecture for scale; load harness (`SCALE-1`, 100/500/1000 simulated workers), bitset scheduler index, per-submitter fair share, and single-flight batched cold-result storage (`SCALE-2`, post-change records at 500 and 1000 workers), and observability (`GET /metrics` and `--features otel` spans, `ECO-4`) are built, while worker-to-worker blobs, master high availability, autoscaling providers, federation, and multi-tenancy remain planned (`SCALE-1` to `SCALE-6`, `ECO-3`, `ECO-4`) | Contributors |
| [design/0051-machine-identity-and-invites.md](design/0051-machine-identity-and-invites.md) | Accepted (Partial: ENROLL-1a and ENROLL-1b built; ENROLL-1c and ENROLL-1d planned): a key per machine, OXS-AUTH v3 (Noise `IK`), invites that work once, expire and need approval, roles, `invite` / `members`, revocation across all lanes (`ENROLL-1`) | Contributors |
| [design/0052-short-code-pairing.md](design/0052-short-code-pairing.md) | Implemented: `invite --code` and `join --code` on a LAN with SPAKE2 and burn rule, discovery on UDP 8087, and recorded Windows/macOS LAN run (`JOIN-2`) | Contributors |
| [design/0053-usb-setup-kit.md](design/0053-usb-setup-kit.md) | Partial (`USB-1a`/`1b` built, `1c` live test pending): `oxideswarm usb create`, `invite.oxs` + `kit.json`, per-user installers (`USB-1`) | Contributors |
| [design/0054-open-compute-network.md](design/0054-open-compute-network.md) | Draft, long-term vision: an open compute network (WebAssembly and VM sandboxes, result verification, federation, credits, governance), stages H0 to H4 (`NET-1` to `NET-8`) | Contributors |
| [design/0055-usb-kit-windows-isolation.md](design/0055-usb-kit-windows-isolation.md) | Implemented: opt-in Windows task isolation (`-Isolation off\|fs`) in the USB setup kit, `OXIDESWARM_HOME/windows_acl_grants.txt` manifest precedence, `uninstall.ps1` ACL revocation, and native `System32` resolution over `Git\usr\bin` inside Windows AppContainers (`USB-2`, `BUG-18`) | Contributors |
| [design/0056-frontier-program.md](design/0056-frontier-program.md) | Draft, research and plan: the frontier program after the core. Tasks that are portable, movable and checkable; bets A to E, each with a spike and a lighthouse run; waves after `0.2.0` (`AX-1` to `AX-7`, `WASM-2` to `WASM-5`, `PROOF-1` to `PROOF-5`, `LLM-6` to `LLM-8`, `EDU-1` to `EDU-3`, `GREEN-1`, `GREEN-2`, `IMG-1` to `IMG-4`) | Owner, contributors |
| [design/0057-open-results-charter.md](design/0057-open-results-charter.md) | Accepted in principle, not built: a charter for donated compute. A project that uses the public pool publishes its outputs and task code, later products stay free, private clusters are not covered, and the charter binds the owner (`CHARTER-1` to `CHARTER-5`) | Owner, contributors |
| [design/0058-lasting-value.md](design/0058-lasting-value.md) | Draft, plan: lasting value. Assurance (fuzzing, deterministic simulation, a checked model, an `unsafe` inventory, release provenance, an independent review), a written specification with a second decoder, citable evidence, and stewardship beyond the founder (`LV-1` to `LV-17`) | Owner, contributors |
| [design/0059-first-users.md](design/0059-first-users.md) | Partial: first users. Who gains what from the grid today, the first audience (a developer with a coding agent and machines on several operating systems), a study of first use, a run summary (`ADOPT-2` built), install scripts and guides for common jobs (`ADOPT-1` to `ADOPT-4`), measures M15 to M17 | Owner, contributors |
| [design/0060-speed-aware-placement.md](design/0060-speed-aware-placement.md) | Partial: per-worker speed history (`SCHED-4a`) and placement via `--speed-hold-factor` that holds for or prefers a clearly faster worker while preserving `rank` on near-equal workers (`SCHED-4b`); opt-in re-run of stragglers (`--repeatable`, `SCHED-4c`) and `speed:` worker tag (`SCHED-4d`) planned (`SCHED-4`) | Contributors |
| [design/0061-first-application-model-tunnel.md](design/0061-first-application-model-tunnel.md) | Draft, plan: a first application for the model track (LIVA, the owner's desktop assistant). The roles of the two projects, a trial with the model on another machine (`APP-1`), a client tunnel to a loopback model server (`LLM-9`), usage records (`LLM-10`), a balance per member (`LLM-11`); decisions Q40 to Q43 | Owner, contributors |
| [design/0062-model-weights-into-remote-memory.md](design/0062-model-weights-into-remote-memory.md) | Draft, plan: a worker runs a model whose file is on another machine, with no copy written to its disk first. What llama.cpp's RPC backend does today, four spikes (`R15` to `R18`), a model server that takes its tensors from a stream (`LLM-12`), a client command that sends them (`LLM-13`), and the rule that the head of a split model runs on a worker. Builds nothing | Owner, contributors |
| [design/0063-two-directions-in-parallel.md](design/0063-two-directions-in-parallel.md) | Draft, plan: two directions followed in parallel, compute for coding agents and a study kit for a class. One trial for each that needs nothing built (`APP-2`, `APP-3`), "an operating system for agents" read as a list of what exists, and the owner's simulation project read as a first workload (`APP-4`). Builds nothing | Owner, contributors |
| [design/0064-research-of-2026-11-13-and-route.md](design/0064-research-of-2026-11-13-and-route.md) | Draft, plan: what five research documents of 2026-11-13 found, with the basis of each finding, and the order of work that follows. Two spikes (`R20`: a GPU inside the sandbox; `R21`: what Mesh-LLM already does), one item (`SCHED-5`), and a proposal for long-running MCP calls. Builds nothing | Owner, contributors |
| [design/0065-large-transfer-and-long-run-tests.md](design/0065-large-transfer-and-long-run-tests.md) | Draft, plan: a harness that moves files of 1 to 8 GiB and large trees between machines on LAN and through a relay, and a 24-hour run under load and faults, with records and a short run on a schedule in CI (`TEST-24` to `TEST-26`). Builds nothing | Owner, contributors |
| [design/0066-usable-rendezvous-discovery.md](design/0066-usable-rendezvous-discovery.md) | Implemented: standalone `oxideswarm rendezvous` registry (`:8090`), deterministic Ed25519 keyed slots and read auth, MAC'd `p2p_ticket` with P2P fallback, upfront `https://` rejection (`FEAT-7`) | Contributors |
| [design/0067-multi-node-llm-serving-and-admission.md](design/0067-multi-node-llm-serving-and-admission.md) | Draft, research and plan: what NVIDIA's two-station vLLM run (GLM-5.3, 2026-11-13) means for a home or lab cluster. No tensor parallelism over Ethernet, admission at the KV-cache knee with interactive and batch bands (`LLM-14`), usable rather than physical GPU memory (input to `FEAT-9b`), authenticated paired services and direct fast links (`Q47`), and spike `R22`; decisions Q60 to Q63. Builds nothing | Owner, contributors |
| [design/0068-task-vram-accounting.md](design/0068-task-vram-accounting.md) | Implemented: optional `TaskRequirements::vram_mb`, CLI `--vram-mb`, MCP `vram_mb`, Web UI `vram_mb`, protocol version 9, store format 3, and scheduler VRAM accounting (`FEAT-9a`, decision `Q56`) | Contributors |
| [design/0069-bounded-quic-data-lane.md](design/0069-bounded-quic-data-lane.md) | Implemented: cap 2-lane QUIC worker session `pending_data` backlog at `256` messages and `64 MiB` and enforce a `60 s` data-lane write timeout (`BUG-19`) | Contributors |
| [design/0070-no-working-directory-config.md](design/0070-no-working-directory-config.md) | Implemented: skip `./rusty-grid.toml` and `./rusty-grid.json` in the working directory for every CLI command with a `stderr` notice unless `--config` or `RUSTY_GRID_CONFIG` is set (`SEC-10`) | Contributors |
| [design/0071-remove-dashboard-chat.md](design/0071-remove-dashboard-chat.md) | Implemented: remove `POST /api/chat`, `run_agy` child-process execution on the master host, and the dashboard chat UI (`SEC-14`, decision D9) | Contributors |
| [design/0072-collect-outputs-without-following-links.md](design/0072-collect-outputs-without-following-links.md) | Implemented: collect task outputs through an `O_NOFOLLOW` descriptor verified by `fstat` to be a regular file, verifying `dev`/`ino` on move and refusing swapped symlinks and FIFOs (`SEC-13`) | Contributors |
| [design/0073-android-bridge-nonblocking-stop.md](design/0073-android-bridge-nonblocking-stop.md) | Implemented: non-blocking `stop_worker_impl` background join, atomic `start_worker_impl` `compare_exchange` claim, poison-tolerant mutex locking in `crates/android_bridge`, and `aarch64-linux-android` CI target check (`AND-5`) | Contributors |
| [design/0074-external-runtime-allowlist-and-gpu-workloads.md](design/0074-external-runtime-allowlist-and-gpu-workloads.md) | Partial: opt-in worker `--isolation-read-dir` (`RUSTY_GRID_ISOLATION_READ_DIRS`, `[worker].isolation_read_dirs`) across macOS Seatbelt, Linux Landlock, and Windows AppContainer, GPU workload guide (`FEAT-3e`), and recorded macOS/Windows `R20` GPU sandbox results (`R20` Linux planned) | Contributors |
| [design/0075-non-blocking-dispatch.md](design/0075-non-blocking-dispatch.md) | Implemented: non-blocking `try_reserve` scheduler dispatch with bounded per-assignment waiter timeout (`DISPATCH_RESERVE_TIMEOUT = 5 s`) and per-worker waiter cap (`BUG-20`) | Contributors |
| [design/0076-protocol-compatibility-and-release-train.md](design/0076-protocol-compatibility-and-release-train.md) | Implemented: every wire change broke older peers (protocol 1 to 9 in 31 days, no release at 9). A release train for wire changes, a release-to-build skew job in CI, an extension slot for optional task fields, and a one-version window before 1.0 (`REL-6`, `PROTO-1` to `PROTO-4`); decisions Q64 to Q67. The code of `PROTO-3` and `PROTO-4` is described in designs 0082 and 0083 | Owner, contributors |
| [design/0077-windows-relative-task-executables.md](design/0077-windows-relative-task-executables.md) | Implemented: resolve relative Windows task executables (`./`, `.\`, `sub/`, `sub\`) against the task working directory inside the sandbox across `--isolation off|fs|fs+net`, refuse sandbox escapes, and pass an absolute `lpApplicationName` (`BUG-21`) | Contributors |
| [design/0078-no-elevated-token-fallback.md](design/0078-no-elevated-token-fallback.md) | Implemented: an elevated Windows worker refuses to start an isolated task, with the Win32 error, instead of falling back to `CreateProcessW` with its elevated token (`SEC-11`) | Contributors |
| [design/0079-agent-mesh-launchers-safe-by-default.md](design/0079-agent-mesh-launchers-safe-by-default.md) | Implemented: `-NoShell` / `OXIDE_MESH_NO_SHELL` for the PowerShell mesh launcher, hub token on stdin (not the command line) in the `adb` launcher, and a `ws://` token warning in both nodes (`SEC-15`) | Contributors |
| [design/0080-source-file-size-check.md](design/0080-source-file-size-check.md) | Implemented: `ox-mode test size` and CI fail when a Rust file's production part exceeds 3,000 lines, with an allowlist that may only shrink (`MAINT-1` step 3) | Contributors |
| [design/0081-agent-activity-status.md](design/0081-agent-activity-status.md) | Implemented: `agent_bridge.py activity` publishes what an agent is doing to `nodes/<id>_activity.json`, separate from the machine heartbeat, with `--show` and a STALE mark after 20 minutes | Contributors |
| [design/0082-task-extension-slot.md](design/0082-task-extension-slot.md) | Implemented (pull request 272): an extension slot for optional task fields (`Extension { id, critical, bytes }` at the end of `Task` and `TaskRequirements`, protocol 10): unknown non-critical ignored and kept on relay, unknown critical refused by master and worker, `ext:` worker tags keep such tasks off workers that lack them, bounds, a registry, and the rule for a v9 peer after PROTO-4 (`PROTO-3`, design 0076 D3); decisions Q68 to Q72 made 2026-11-17 | Contributors |
| [design/0083-one-version-window.md](design/0083-one-version-window.md) | Implemented (pull request 274): a one-version compatibility window (`PROTOCOL_VERSION_MIN_COMPATIBLE = CURRENT - 1`, 10 accepts 9): the master learns each peer's version, echoes it in stamps, encodes `Task` carriers for a v9 peer without extensions, decodes a v9 `SubmitTask`, refuses newer peers (master first), a range-enforcing layout test with the previous release's layout as oracle, and the new skew table (`PROTO-4`, design 0076 D4); decisions Q73 to Q79. Builds nothing | Contributors |
| [design/0084-discovery-beacon-port.md](design/0084-discovery-beacon-port.md) | Implemented: `master --discovery-port <PORT>` (`RUSTY_GRID_DISCOVERY_PORT`, `[master].discovery_port`) moves the UDP discovery beacon off `8089`, `0` turns it off, and a worker finds a moved beacon with `--master auto:<PORT>` (`CLI-2`) | Operators |
| [design/0085-state-dir-without-permissions.md](design/0085-state-dir-without-permissions.md) | Implemented (Windows): a master refuses a `--state-dir` on a volume that keeps no file permissions (exFAT, FAT32) at start-up, before it writes there, with a message that names the file system and asks for NTFS; the same message replaces the `icacls` hint for private files on such a volume (`BUG-24`) | Operators |
| [design/0086-graceful-windows-stop.md](design/0086-graceful-windows-stop.md) | Implemented: `windows\oxctl.ps1 stop` sends the tracked master or worker a console Ctrl+C, waits up to 15 s, and only then uses `taskkill /F /T`, so a master with `--state-dir` compacts its journal (`BUG-23`) | Operators |
| [design/0087-shared-coordination-state.md](design/0087-shared-coordination-state.md) | Implemented: id claims on the hub (design numbers, Q ids, phases, item ids) with create-only files and a settle check, files on the activity record with an overlap check, and one shared place for process lessons (COORD-1 to COORD-3, pull requests 294 and 295; Q84 to Q86 decided) | Coding agents, owner |
| [design/0088-evaluation-of-2026-11-17-and-what-finished-means.md](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md) | Accepted: scores on ten criteria at `master` `16b2a38`, a stop line, one exit check per criterion that together mean "finished" for the purpose of Q1 (Q82), the items that reach each check and who holds the next step; proposes `CI-1`, `MAINT-2` and `DOC-11`; bet A may start before release `0.5.0` ships (Q83) | Contributors, the owner |
| [design/0089-anima-engine-first-workload.md](design/0089-anima-engine-first-workload.md) | Accepted: Anima Engine, the owner's simulation project, as the first workload: a trial on two machines (APP-4), `submit --verify` to run a task again on other workers and compare outputs (VERIFY-1), on the first worker's operating system and architecture unless `--verify-any-system` (VERIFY-2, D6), AX-1 and AX-2 with Anima Engine as their first user, and a guide (APP-5) | Owner, coding agents |
| [design/0093-benchmarks-worth-showing.md](design/0093-benchmarks-worth-showing.md) | Draft: the measurements that answer a newcomer's first questions, under one method (plan committed before the run, the fastest machine alone as the baseline, efficiency against the sum of the machines): sweep scaling on two machines, offload against local work, per-task overhead and the break-even task length, two networks, and the README headlines (`BENCH-2` to `BENCH-7`); Q90 to Q92 open | Owner, contributors |
| [design/0090-sandbox-base-refusal-hand-back.md](design/0090-sandbox-base-refusal-hand-back.md) | Implemented: a worker whose sandbox base is unusable refuses the task with a text the master already treats as a worker refusal, so the task goes to another worker without a retry charged (BUG-25) | Coding agents |
| [design/0091-restart-recovery-races.md](design/0091-restart-recovery-races.md) | Implemented: two worker races found by looping `master_restart_recovery` (`TEST-23`): a result buffered as failed when the connection ended during an output upload, and a task lock won on an unlinked lock file. |
| [design/0092-mesh-hub-reply-headroom.md](design/0092-mesh-hub-reply-headroom.md) | Implemented: the Agent Mesh hub waits for room for its replies in the sender's own outbound queue before it handles the next request, so a burst of requests slows the sender down instead of getting it disconnected as a slow consumer (TEST-27) | Contributors |
| [design/0094-road-to-a-shared-research-grid.md](design/0094-road-to-a-shared-research-grid.md) | Accepted, plan: the owner's ultimate goal, a grid on which users pool their machines to research one problem, as research campaigns in four stages (personal clusters, trusted circles, research campaigns, an open research grid), each with an exit check. Orders Phase 9 and design 0054 by stage, keeps money and tokens out (credits only display), and adds the Stage 1 items `ECO-9`, `NET-9`, `NET-10` and `APP-6` (Phase 19); Q94 to Q97 decided on 2026-11-19 (Q96 closes Q14 and Q43). Builds nothing | Owner, contributors |
| [BACKLOG.md](BACKLOG.md) | Open work in execution order, each item with files and a check | Coding agents, contributors |
| [BACKLOG-ARCHIVE.md](BACKLOG-ARCHIVE.md) | The text of every Implemented backlog item, under the phase headings of the backlog (DOC-11); the Index of BACKLOG.md lists every item | Coding agents, contributors |
| [ROADMAP.md](ROADMAP.md) | Direction after the backlog: the purpose, the core (phases A to D) and the later work, what waits, and the owner's decisions | Owner, contributors |

Design record numbers 0035, 0037, 0041 and 0043 were never used. 0042 (remote workspaces) was a
draft that design 0039 replaced before it merged.

## Document types

| Type | Location | Purpose | Required sections |
|---|---|---|---|
| Reference | `docs/reference.md`, `docs/SECURITY.md`, READMEs next to the scripts they describe | What exists and how to call it | Free, but one topic per document |
| Guide | `docs/guides/<slug>.md` | One task from start to finish, with a check that it worked | Goal, Prerequisites, Steps, Verify, Troubleshooting |
| Design record | `docs/design/NNNN-<slug>.md`, numbered in order | Why and how a change is made, and its work items | See the template below |
| Backlog | `docs/BACKLOG.md` | Open work in execution order | Index table; per item: Evidence, Problem, Do, Done when |
| Roadmap | `docs/ROADMAP.md` | Direction and order of the work beyond single items | Status line, Summary, Where the project stands, Phases with exit checks, Decisions, Status log |

## Writing standard

1. **English only.** One idea per sentence. No marketing words ("blazing", "100%", "enterprise-grade").
2. **Only what the code does.** Name the file or function behind a claim a reader may want to check.
   Anything not built is marked **Planned** or **Design only**.
3. **Steps are numbered and imperative.** Every command block has a language tag (`bash`,
   `powershell`, `text`), runs on one machine, and the line above it says which machine.
4. **Placeholders in angle brackets:** `<MASTER_IP>`, `<TICKET>`, `<USER>`. Never a real private IP,
   hostname, secret or ticket.
5. **One source of truth.** Ports, environment variables and CLI flags are listed in `docs/reference.md`.
   Other documents link there instead of copying the tables.
6. **Nothing that drifts silently:** no item counts in headings, no performance number without the
   script that measured it.
7. **Fixed vocabulary.** Status: Implemented, Partial, Planned, Design only. Dates: ISO 8601
   (`2026-10-23`). Units: MB, MiB, ms, GFLOP/s. The `Status:` line of a design record is the
   exception: it states where the record stands (Draft, Accepted, Partial, Implemented or
   Superseded by NNNN), and its work-item table uses the fixed vocabulary. A record moves to
   Implemented when every work item in its table is Implemented.
8. **Headings** in sentence case, without emoji; numbered only in long reference documents.
9. **Code and documentation land together.** A change to the wire protocol, a CLI flag, a port, an
   environment variable or a default updates the `docs/reference.md` reference in the same change (commit,
   or pull request for a multi-commit branch), `CLAUDE.md` when the architecture map changes, and
   gets a design record when it changes behaviour.
   A pull request that bumps `PROTOCOL_VERSION_CURRENT` says "wire change" in its description, and a release follows within 48 hours (design 0076 D1).

## Templates

Design record:

```text
# NNNN: <Title>

Status: Draft | Accepted | Partial | Implemented | Superseded by NNNN
Created: YYYY-MM-DD    Updated: YYYY-MM-DD

## Summary          what changes, in five lines or fewer
## Context          behaviour before the change, with file references
## Goals            numbered G1, G2, ...
## Non-goals
## Design           one subsection per part, D1, D2, ...
## Compatibility    wire format, mixed versions, upgrade order
## Security         new inputs, trust, limits
## Test plan        unit, integration, live, gates
## Work items       table: ID | Item | Files | Done when | Status
## Status log       dated entries, newest last
```

Guide:

```text
# <Task, imperative>

## Goal             one sentence: what works at the end
## Prerequisites    hardware, software, files
## Steps            numbered; one machine per command block
## Verify           the command that proves it worked and its expected output
## Troubleshooting  table: Symptom | Cause | Fix
```
