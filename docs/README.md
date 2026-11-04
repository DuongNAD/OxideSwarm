# Documentation

Index of the OxideSwarm documentation and the standard every document follows. Read the standard
before adding or changing a document.

## Index

| Document | Scope | Audience |
|---|---|---|
| [README.md](../README.md) | Overview, quickstart, CLI reference, ports, environment variables | Users |
| [guides/two-machine-cluster.md](guides/two-machine-cluster.md) | Runbook: a macOS machine and a Windows machine as one cluster, verified with `oxideswarm doctor` | Users |
| [guides/mcp-client-setup.md](guides/mcp-client-setup.md) | Runbook: connect Claude Code, Codex, or Cursor to `oxideswarm mcp` over stdio | Users, coding agents |
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
| [design/0026-refuse-claim-master-role.md](design/0026-refuse-claim-master-role.md) | Refuse `WorkerMessage::ClaimMasterRole` on the master and drop `ROLE_ARBITRAGE` from `ProtocolCapabilities::all_supported()` (`FEAT-6`, decision D1) | Contributors |
| [design/0028-host-battery-probe-override.md](design/0028-host-battery-probe-override.md) | Host battery probe override (`WorkerConfig::with_no_battery` and `RUSTY_GRID_NO_BATTERY`) and start-up warning | Contributors |
| [design/0030-mcp-post-reply-cancel-and-p2p-endpoint-lifecycle.md](design/0030-mcp-post-reply-cancel-and-p2p-endpoint-lifecycle.md) | MCP post-reply `notifications/cancelled` (`completed_submits`), `4 s` stdio EOF grace window, and P2P master relay readiness (`5 s`) and endpoint shutdown | Contributors |
| [design/0032-p2p-handshake-refusal-delivery.md](design/0032-p2p-handshake-refusal-delivery.md) | P2P QUIC handshake and cluster-auth refusal delivery via stream finish and bounded `conn.closed()` drain (`BUG-17`) | Contributors |
| [BACKLOG.md](BACKLOG.md) | Open work in execution order, each item with files and a check | Coding agents, contributors |

## Document types

| Type | Location | Purpose | Required sections |
|---|---|---|---|
| Reference | `README.md`, `docs/SECURITY.md`, READMEs next to the scripts they describe | What exists and how to call it | Free, but one topic per document |
| Guide | `docs/guides/<slug>.md` | One task from start to finish, with a check that it worked | Goal, Prerequisites, Steps, Verify, Troubleshooting |
| Design record | `docs/design/NNNN-<slug>.md`, numbered in order | Why and how a change is made, and its work items | See the template below |
| Backlog | `docs/BACKLOG.md` | Open work in execution order | Index table; per item: Evidence, Problem, Do, Done when |

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
