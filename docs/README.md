# Documentation

Index of the OxideSwarm documentation and the standard every document follows. Read the standard
before adding or changing a document.

## Index

| Document | Scope | Audience |
|---|---|---|
| [README.md](../README.md) | Overview, quickstart, CLI reference, ports, environment variables | Users |
| [guides/two-machine-cluster.md](guides/two-machine-cluster.md) | Runbook: a macOS machine and a Windows machine as one cluster, verified with `oxideswarm doctor` | Users |
| [guides/self-hosted-relay.md](guides/self-hosted-relay.md) | Runbook: run a self-hosted `iroh-relay` server and point master, worker, and clients at it with `--relay-url` | Users |
| [guides/mcp-client-setup.md](guides/mcp-client-setup.md) | Runbook: connect Claude Code, Codex, or Cursor to `oxideswarm mcp` over stdio | Users, coding agents |
| [guides/offload-cargo-test.md](guides/offload-cargo-test.md) | Runbook: offload `cargo test` from a local checkout to a persistent worker workspace via `submit --workspace` or `oxideswarm mcp` | Users, coding agents |
| [guides/multi-agent-collaboration.md](guides/multi-agent-collaboration.md) | Runbook: cross-machine coding agent coordination and root-cause diagnostics via `OxideSwarm_Agent_Hub` | Coding agents |
| [SECURITY.md](SECURITY.md) | Threat model, cluster secret, ports, tokens, who runs tasks | Users, reviewers |
| [DEPLOYMENT.md](../DEPLOYMENT.md) | Agent Mesh deployment and operations (not the compute grid) | Users |
| [PROJECT.md](../PROJECT.md) | Architecture, workspace layout, feature inventory, roadmap | Contributors |
| [CLAUDE.md](../CLAUDE.md) | Build and test commands, architecture map for coding agents | Coding agents |
| [ANDROID_ARCHITECTURE.md](../ANDROID_ARCHITECTURE.md) | Android design; parts are design only (see its status section) | Contributors |
| [windows/README.md](../windows/README.md) | Windows launchers | Users |
| [packaging/README.md](../packaging/README.md) and the READMEs below it | Background services: launchd, Windows service, Android app, Termux | Users |
| [scripts/sync_network/README.md](../scripts/sync_network/README.md) | Shared-folder reachability probe (not part of the grid) | Users |
| [benchmarks/interconnect/](../benchmarks/interconnect/COMPARATIVE_INTERCONNECT_BENCHMARK.md) | Recorded interconnect comparison and its validator | Contributors |
| [design/](design/) | Design records, one per change to behaviour, CLI or wire format | Contributors |
| [design/0023-generative-3d-and-autorig-subsystem.md](design/0023-generative-3d-and-autorig-subsystem.md) | Generative 3D (`gen3d`) and skeletal auto-rigging (`autorig`) architecture, Python/Blender drivers (`engines/`), and current limits (`3D-2`, `3D-3`) | Contributors |
| [design/0024-wire-cli-flags-and-retire-ttl-eviction.md](design/0024-wire-cli-flags-and-retire-ttl-eviction.md) | Wire `--max-queue-size`, `--scheduler-policy`, `--wire-codec`, `[submit].timeout_secs`, and worker `--master` precedence, and remove dead TTL eviction (`SCHED-2`) | Contributors |
| [design/0026-refuse-claim-master-role.md](design/0026-refuse-claim-master-role.md) | Refuse `WorkerMessage::ClaimMasterRole` on the master and drop `ROLE_ARBITRAGE` from `ProtocolCapabilities::all_supported()` (`FEAT-6`, decision D1) | Contributors |
| [design/0027-ox-mode-honest-reporting.md](design/0027-ox-mode-honest-reporting.md) | Move the `ox-mode` workflow-mode execution engine to `crates/cli` and report real benchmark, simulation, cluster status, test, and doc results (`MODE-1`) | Contributors |
| [design/0028-host-battery-probe-override.md](design/0028-host-battery-probe-override.md) | Host battery probe override (`WorkerConfig::with_no_battery` and `RUSTY_GRID_NO_BATTERY`) and start-up warning | Contributors |
| [design/0029-mesh-client-wss-tls.md](design/0029-mesh-client-wss-tls.md) | Enable `wss://` TLS in the Rust `agent-mesh` client and add `--tls-ca` (`OXIDE_MESH_TLS_CA`) for private TLS proxies (`MESH-1`) | Contributors |
| [design/0030-mcp-post-reply-cancel-and-p2p-endpoint-lifecycle.md](design/0030-mcp-post-reply-cancel-and-p2p-endpoint-lifecycle.md) | MCP post-reply `notifications/cancelled` (`completed_submits`), `4 s` stdio EOF grace window, and P2P master relay readiness (`5 s`) and endpoint shutdown | Contributors |
| [design/0031-containment-after-worker-crash.md](design/0031-containment-after-worker-crash.md) | Per-task lifeline pipe and supervisor so a task tree dies within 5 s after a worker `SIGKILL` on Linux, macOS, and Android (`FEAT-2`) | Contributors |
| [design/0032-p2p-handshake-refusal-delivery.md](design/0032-p2p-handshake-refusal-delivery.md) | P2P QUIC handshake and cluster-auth refusal delivery via stream finish and bounded `conn.closed()` drain (`BUG-17`) | Contributors |
| [design/0033-os-level-task-isolation.md](design/0033-os-level-task-isolation.md) | Accepted: OS-level task isolation (`--isolation off\|fs\|fs+net`) via Landlock/`seccomp`/namespaces on Linux (`FEAT-3b`), `sandbox-exec` on macOS (`FEAT-3a`), and per-task AppContainer on Windows (`FEAT-3c`), plus `iso:` tag, `require:iso=` placement and `doctor` reporting (`FEAT-3`, all three platforms implemented) | Contributors |
| [design/0034-fail-unfulfillable-tasks.md](design/0034-fail-unfulfillable-tasks.md) | Fail unfulfillable tasks (`--fail-unfulfillable-tasks`, `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS`, `[master].fail_unfulfillable_tasks`) when no registered worker can satisfy capabilities or placement constraints (`SCHED-3`) | Contributors |
| [design/0036-bounded-uncharged-worker-evacuations.md](design/0036-bounded-uncharged-worker-evacuations.md) | Bounded uncharged voluntary worker drains (`WorkerMessage::Disconnecting`, `RetryPolicy::max_evacuations`, `TaskInfo::evacuation_count`), `retry_history` audit entries, and `Running` `max_retries == 0` guard (`MOB-3`) | Contributors |
| [design/0038-large-language-model-inference.md](design/0038-large-language-model-inference.md) | Planned: language model inference on the grid, by expert routing to local model servers and by llama.cpp RPC over an authenticated tunnel (`LLM-1` to `LLM-5`) | Contributors |
| [design/0039-agent-workspaces.md](design/0039-agent-workspaces.md) | Accepted, W1–W10 built and recorded live on two machines (`Task::workspace`, `Hello`, `ListWorkspaces`, protocol version 7): persistent workspaces for coding agents on workers, synced from a local directory, with affinity and one task per workspace, plus P2P for clients and `cargo` builds (`FEAT-4`, `FEAT-10`) | Contributors |
| [design/0040-protocol-versioning.md](design/0040-protocol-versioning.md) | Protocol version 7 (initially 3), golden-bytes version-bump enforcement, `RegisterAck` feature-bit gating, and client-path protocol versioning (`FEAT-8`, `FEAT-1`) | Contributors |
| [design/0044-dead-code-and-connection-loop-unification.md](design/0044-dead-code-and-connection-loop-unification.md) | Planned: remove dead code, merge TCP and 2-lane QUIC worker connection loops into `WorkerSession`, sweep leftover sandboxes at start-up, and gate `gen3d`/`autorig` behind Cargo feature `3d` (`FEAT-11`, decision Q2) | Contributors |
| [design/0045-distributed-training.md](design/0045-distributed-training.md) | Planned, after the core of the roadmap: training on several workers with a framework the owner installs, by independent runs, low-communication rounds and adapter rounds (`TRAIN-1` to `TRAIN-5`) | Contributors |
| [design/0046-master-durability.md](design/0046-master-durability.md) | Implemented: append-only task journal and snapshot store (`--state-dir`), startup replay, store-health reporting (`ClusterStatus.store_error`, protocol version 5), and worker session resume (`WorkerMessage::ResumeSession`) (`FEAT-1`) | Contributors |
| [design/0047-windows-private-files.md](design/0047-windows-private-files.md) | Implemented: files created as private on Windows (cluster secret, master P2P key, ticket file and their directory) get a protected DACL for the current user, SYSTEM and Administrators at creation, and loading them warns with the `icacls` fix when the DACL is wider (`SEC-9`) | Contributors |
| [design/0048-join-token.md](design/0048-join-token.md) | Implemented: `oxideswarm secret join-token` prints one token (master address, optional P2P ticket, cluster secret, integrity tag) and `oxideswarm join` reads it from standard input, saves the secret at mode 600 and the address in `rusty-grid.toml`, and runs the doctor checks (`JOIN-1`) | Contributors |
| [design/0049-custom-relay.md](design/0049-custom-relay.md) | Implemented: `--relay-url`, `RUSTY_GRID_RELAY_URL`, and `relay_url` across master, worker, and client P2P endpoints (`RelayMode::Custom` on `presets::Minimal`), URL validation, and self-hosted `iroh-relay` guide (`LINK-1`) | Contributors |
| [design/0050-scale-out-architecture.md](design/0050-scale-out-architecture.md) | Draft: limits of the single master with evidence, and a staged architecture for scale: load harness, scheduler, worker-to-worker blobs, master high availability, autoscaling providers, federation, multi-tenancy and observability (`SCALE-1` to `SCALE-6`, `ECO-3`, `ECO-4`) | Contributors |
| [design/0051-machine-identity-and-invites.md](design/0051-machine-identity-and-invites.md) | Accepted, not built: a key per machine, OXS-AUTH v3 (Noise `IK`), invites that work once, expire and need approval, roles, `invite` / `members`, revocation across all lanes (`ENROLL-1`) | Contributors |
| [design/0052-short-code-pairing.md](design/0052-short-code-pairing.md) | Accepted, not built: `invite --code` and `join --code` on a LAN with SPAKE2, discovery on UDP 8087 (`JOIN-2`) | Contributors |
| [design/0053-usb-setup-kit.md](design/0053-usb-setup-kit.md) | Accepted, not built: `usb create`, an encrypted invite for N machines and per-user installers for macOS, Windows and Linux (`USB-1`) | Contributors |
| [design/0054-open-compute-network.md](design/0054-open-compute-network.md) | Draft, long-term vision: an open compute network (WebAssembly and VM sandboxes, result verification, federation, credits, governance), stages H0 to H4 (`NET-1` to `NET-8`) | Contributors |
| [design/0055-usb-kit-windows-isolation.md](design/0055-usb-kit-windows-isolation.md) | Implemented: opt-in Windows task isolation (`-Isolation off\|fs`) in the USB setup kit, `OXIDESWARM_HOME/windows_acl_grants.txt` manifest precedence, `uninstall.ps1` ACL revocation, and native `System32` resolution over `Git\usr\bin` inside Windows AppContainers (`USB-2`, `BUG-18`) | Contributors |
| [design/0056-frontier-program.md](design/0056-frontier-program.md) | Draft, research and plan: the frontier program after the core. Tasks that are portable, movable and checkable; bets A to E, each with a spike and a lighthouse run; waves after `0.2.0` (`AX-1` to `AX-7`, `WASM-2` to `WASM-5`, `PROOF-1` to `PROOF-5`, `LLM-6` to `LLM-8`, `EDU-1` to `EDU-3`, `GREEN-1`, `GREEN-2`, `IMG-1` to `IMG-4`) | Owner, contributors |
| [design/0057-open-results-charter.md](design/0057-open-results-charter.md) | Accepted in principle, not built: a charter for donated compute. A project that uses the public pool publishes its outputs and task code, later products stay free, private clusters are not covered, and the charter binds the owner (`CHARTER-1` to `CHARTER-5`) | Owner, contributors |
| [design/0058-lasting-value.md](design/0058-lasting-value.md) | Draft, plan: lasting value. Assurance (fuzzing, deterministic simulation, a checked model, an `unsafe` inventory, release provenance, an independent review), a written specification with a second decoder, citable evidence, and stewardship beyond the founder (`LV-1` to `LV-17`) | Owner, contributors |
| [design/0059-first-users.md](design/0059-first-users.md) | Draft, plan: first users. Who gains what from the grid today, the first audience (a developer with a coding agent and machines on several operating systems), a study of first use, a run summary, install scripts and guides for common jobs (`ADOPT-1` to `ADOPT-4`), measures M15 to M17 | Owner, contributors |
| [design/0060-speed-aware-placement.md](design/0060-speed-aware-placement.md) | Planned: per-worker speed history, placement that waits for a clearly faster worker, opt-in re-run of stragglers (`--repeatable`), and a `speed:` worker tag (`SCHED-4`) | Contributors |
| [design/0061-first-application-model-tunnel.md](design/0061-first-application-model-tunnel.md) | Draft, plan: a first application for the model track (LIVA, the owner's desktop assistant). The roles of the two projects, a trial with the model on another machine (`APP-1`), a client tunnel to a loopback model server (`LLM-9`), usage records (`LLM-10`), a balance per member (`LLM-11`); decisions Q40 to Q43 | Owner, contributors |
| [BACKLOG.md](BACKLOG.md) | Open work in execution order, each item with files and a check | Coding agents, contributors |
| [ROADMAP.md](ROADMAP.md) | Direction after the backlog: the purpose, the core (phases A to D) and the later work, what waits, and the owner's decisions | Owner, contributors |

## Document types

| Type | Location | Purpose | Required sections |
|---|---|---|---|
| Reference | `README.md`, `docs/SECURITY.md`, READMEs next to the scripts they describe | What exists and how to call it | Free, but one topic per document |
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
5. **One source of truth.** Ports, environment variables and CLI flags are listed in `README.md`.
   Other documents link there instead of copying the tables.
6. **Nothing that drifts silently:** no item counts in headings, no performance number without the
   script that measured it.
7. **Fixed vocabulary.** Status: Implemented, Partial, Planned, Design only. Dates: ISO 8601
   (`2026-10-23`). Units: MB, MiB, ms, GFLOP/s.
8. **Headings** in sentence case, without emoji; numbered only in long reference documents.
9. **Code and documentation land together.** A change to the wire protocol, a CLI flag, a port, an
   environment variable or a default updates the `README.md` reference in the same change (commit,
   or pull request for a multi-commit branch), `CLAUDE.md` when the architecture map changes, and
   gets a design record when it changes behaviour.

## Templates

Design record:

```text
# NNNN: <Title>

Status: Draft | Accepted | Implemented | Superseded by NNNN
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
