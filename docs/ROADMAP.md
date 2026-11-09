# Roadmap

Status: Accepted. The owner accepted decisions Q1 to Q5 on 2026-11-07, and Q6 and Q7 on 2026-11-08
Created: 2026-11-07    Updated: 2026-11-09

This document sets the direction of OxideSwarm after the completion backlog. It states what the
project is for, the order of the work, and what waits. Item IDs refer to [BACKLOG.md](BACKLOG.md)
and to the design records. The status of each item lives in the BACKLOG Index, not here.

## Summary

- Today one master on a trusted network runs command tasks on macOS, Windows, Linux and Android
  workers. Transport is authenticated and encrypted, files move as content-addressed blobs, and an
  MCP server lets coding agents submit work.
- Purpose: **a coding agent runs builds and tests on the machines its owner already has.**
  Work that serves this purpose goes first.
- Position: one binary per machine turns the macOS, Windows, Linux and Android machines that one
  owner already has into a backend for coding agents, over encrypted P2P links. The research of
  2026-11-08 (below) supports this position. It adds a one-command join, link reporting, published
  benchmarks and a result cache.
- Order: close the open foundations, make the master durable and tasks isolated, build persistent
  agent workspaces, then ship a release. These four phases, A to D, are the core.
- Later work starts after the core: the launch, Phase E and the table of what waits. Android, master
  failover, 3D, language model inference and distributed training are later work.

## Where the project stands

The state at `master` `b7f112f` on 2026-11-09.

### What works

| Area | What the code does | Code |
|---|---|---|
| Scheduling | Task state machine with retries, placement by OS, architecture, worker and tag, GPU gating, host CPU backpressure, requeue when a worker is lost | `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/core/src/placement.rs` |
| Trust and transport | Cluster-secret handshake on TCP and iroh QUIC. ChaCha20-Poly1305 records on authenticated TCP | `crates/core/src/cluster_auth.rs`, `crates/core/src/transport.rs` |
| Files | Files and directories in, glob patterns out, as BLAKE3 blobs. A blob the other side has is not sent again | `crates/core/src/blob.rs`, `crates/master/src/blob_store.rs`, `crates/worker/src/blob_cache.rs` |
| Containment | After `kill -9` of a worker, a supervisor per task group kills the task's processes within 5 s on Linux and macOS. Windows uses a Job Object | `crates/worker/src/process_tree.rs`, `tests/cli_worker_signals_e2e.rs` |
| Agents | `oxideswarm mcp` with the tools `status`, `workers`, `submit`, `task_result`, `cancel` and `doctor`, wait deadlines, cancellation and head-and-tail output | `crates/cli/src/mcp.rs` |
| Durability | `master --state-dir` keeps queued, running and finished tasks in a journal and replays it at start. A worker keeps its tasks across a short disconnect, and the master adopts them again (`ResumeSession`) | `crates/master/src/store.rs`, `crates/master/src/session.rs`, `tests/master_restart_recovery.rs` |
| Isolation | `--isolation off\|fs\|fs+net` on macOS (`sandbox-exec`) and Linux (Landlock, seccomp, namespaces). A task reads neither `cluster.secret` nor other sandboxes | `crates/worker/src/isolation.rs`, design 0033 |
| Operations | `oxideswarm doctor`, the dashboard, launchers and service installers | `crates/cli/src/doctor.rs`, `crates/master/src/web_ui.rs`, `switch_role.sh`, `windows/oxctl.ps1`, `packaging/` |
| Checks | CI runs rustfmt, clippy, unit tests, the full workspace suite, the launcher tests and the Python suites on Linux, macOS and Windows. Every job is a required check on `master`. A changed wire layout fails CI without a protocol version bump | `.github/workflows/ci.yml`, `crates/core/src/wire_layout_tests.rs` |

### What is missing

Each row is a limit a user meets today, and the item that removes it.

| Limit | Effect | Item |
|---|---|---|
| There is one master and no failover | The master is a single point of failure | FEAT-6 (Design only, decision D1) |
| Delivery is at least once | A task can run twice after a worker is lost. A master with `--state-dir` and a worker that resumes shrink the window | FEAT-1, FEAT-2 |
| On Windows a task runs as the worker's user without OS isolation. macOS and Linux have `--isolation fs` and `fs+net` (FEAT-3a, FEAT-3b) | A task on Windows can read `cluster.secret` and other sandboxes | FEAT-3 (`FEAT-3c`, pull request 152 open) |
| A task still gets a fresh sandbox: the workspace store (W2a) and the scheduler support (W3) are merged, but the runner and the CLI do not use them yet | An agent's `cargo test` builds from zero every time | FEAT-4 (W2b, W5, W6) |
| Clients connect over TCP only (`connect_master` in `crates/cli/src/connect.rs`) | A client on another network cannot reach a master that is reachable only over P2P | FEAT-4 |
| Input trees carry no symbolic links or empty directories (design 0039 W11). The `.gitignore` scan (W4) is not wired to `submit` yet | The client picks paths by hand | FEAT-4 |
| The release workflow exists, but no `v*` tag has been cut | A user must still build from source | REL-4 (owner cuts the tag) |
| The Android app does not compile | There is no Android worker app to install | AND-1 |
| GPU compute is a seeded matrix benchmark | No user workload runs on the GPU | FEAT-9 (decision D6) |

## Purpose

### The user story

1. A coding agent works on machine A.
2. Machine B is idle. It has more cores, another operating system, or a GPU.
3. The agent asks the grid, through MCP, to run `cargo test` at its current commit on machine B.
4. Machine B fetches only the files it lacks and reuses its build cache.
5. Machine B runs the tests in isolation and returns the output and the artifacts.
6. The owner installed the binary on both machines and copied the cluster secret. Nothing else was
   set up.

This story generalises the "Done when" of FEAT-4.

### Why this purpose

- It uses what is built: the MCP server, the blob plane, placement by OS and tag, and P2P transport.
- It needs what the backlog already plans: durability (FEAT-1), isolation (FEAT-3), workspaces
  (FEAT-4) and a release (REL-4).
- Its main alternative is SSH to the other machine. OxideSwarm adds placement, deduplicated file
  transfer, deadlines, cancellation, structured results for an agent, and NAT traversal.

### Neighbouring tools

These tools solve nearby problems. The right column says what OxideSwarm does instead, not what the
other tools lack.

| Tool | Built for | OxideSwarm instead |
|---|---|---|
| SSH | Running a command on one known machine | Picks the machine, moves files by content hash, and returns a structured result |
| Bazel remote execution servers, for example Buildbarn or BuildBuddy | Hermetic build actions with a shared action cache | Runs the project's own commands, with no build rewrite |
| sccache-dist, distcc | Distributing compiler invocations | Runs whole commands, test runs included |
| HTCondor | High-throughput batch jobs on pools of machines | Targets a few machines that one person owns, with one binary |
| Ray | Distributed Python applications | Runs any executable, with no API to adopt |
| Nomad, Kubernetes | Services and batch jobs in data centres | Runs from one binary per machine |
| HyperQueue | Task graphs on HPC clusters, from one binary with prebuilt archives for Linux | Also runs workers on macOS, Windows and Android, over authenticated P2P links |
| BOINC | Volunteer computing for research projects | Serves the machines of one owner and the coding agents that owner runs |
| prima.cpp, exo, distributed-llama | Running one language model over several home devices | Runs no model itself. Design 0038 supervises llama.cpp and carries its traffic |

### What the research of 2026-11-08 found

A web research run on 2026-11-08 checked each claim below against its source, with three
independent votes per claim. Each row gives the finding, its source and what this roadmap does
with it.

| Finding | Source | Consequence |
|---|---|---|
| Ray supports multi-node clusters officially on Linux only. macOS and Windows nodes need an opt-in environment variable and run at the user's own risk. Ray on Golem is an MVP tested on Ubuntu and WSL only | [Ray cluster guide](https://github.com/ray-project/ray/blob/ray-2.11.0/doc/source/cluster/getting-started.rst), [Golem limits](https://docs.golem.network/docs/creators/ray/supported-versions-and-other-limitations) | Mixed-OS clusters are a real opening for OxideSwarm |
| Nomad runs clients on Windows, and BOINC has Android clients | [Nomad Windows clients](https://developer.hashicorp.com/validated-designs/nomad/installation-guide/windows-clients) | OxideSwarm is not the only cross-platform grid, and its documents do not claim it |
| HyperQueue ships one statically linked binary. Its README states a per-task overhead below 0.1 ms without a method | [HyperQueue](https://github.com/It4innovations/hyperqueue) | BENCH-1 publishes a method with every number. REL-4 and JOIN-1 match the one-binary install on three systems |
| Joining a Tailscale network takes installing the client and logging in on each device | [What is Tailscale](https://tailscale.com/docs/concepts/what-is-tailscale.md) | JOIN-1 |
| iroh dials by public key, tries a direct path and falls back to a relay. The n0 public relays are rate-limited and meant for development | [iroh hole punching](https://docs.iroh.computer/concepts/holepunching) | LINK-1 shows the path of each link and lets the owner name a relay of their own |
| REAPI v2 names BLAKE3 as a digest function and allows a cache-only server | [remote_execution.proto](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto) | CACHE-1 borrows the ActionCache model. REAPI-1 serves the blob store to REAPI clients without rehashing |
| Over Wi-Fi, splitting a model by layers beats splitting each layer, because a split layer costs a network round trip per layer per token. A published run split a 70B model over a Mac, two PCs with RTX GPUs and a phone. Nobody has reproduced it, and its authors build a competing system | [prima.cpp](https://arxiv.org/abs/2504.08791) | Supports design 0038: a layer split through llama.cpp RPC (D2 to D5). Whether a phone helps as a stage stays open |
| A Hacker News launch adds stars on average, but most of the gain goes to a few repositories. The research could not confirm an effect of the hour of posting | [Study of AI repositories on Hacker News](https://arxiv.org/abs/2511.04453) | Launch only after the release, a demo and the benchmark page (see "Launch") |

Open questions: how often iroh reaches a direct path between the owner's own machines, how often
CACHE-1 hits in the user story, and which features of hosted agent sandboxes coding agents depend
on. The research found no checked measurements of compile farms such as sccache-dist or distcc.

## Principles

1. **One user story first.** Work that does not serve it waits, unless it fixes a security or
   correctness defect.
2. **Honest status.** Use the fixed vocabulary. A claim about hardware needs a dated record of the
   run (BACKLOG rule 9).
3. **Secure off loopback.** Every new path, such as workspaces and tunnels, runs behind the cluster
   handshake.
4. **Small core.** Subsystems outside the user story move behind opt-in Cargo features.
5. **Each phase ends with a check that runs.** A phase is finished when a test passes or a live run is
   recorded.
6. **Core before later work.** Phases A to D are the core. Work in Phase E or in the table of what
   waits starts after the Phase D exit check, unless its row names an earlier gate. Security fixes
   may land at any time. A design record may be written at any time, because it builds nothing.

## Phases

Phases A to D are the core: at their end the user story works and ships as a release. Phase E and
the table of what waits are later work (principle 6).

### Phase A: close the foundations

Goal: finish the short open items, and make CI block on the full suite.

| Item | Work | State on 2026-11-08 (`master` `6723945`) |
|---|---|---|
| SCHED-3 | Give impossible tasks an outcome | Implemented (pull requests 96, 107) |
| MODE-1 | Make `ox-mode` report only what happened | Implemented (pull request 92) |
| TEST-16 | Keep the iroh tests off the network | Implemented (pull requests 95, 109). Neither has a section in BACKLOG |
| TEST-4 | Record a full-suite run with a dummy `cluster.secret` in `HOME` | Implemented (pull requests 29, 112, 115) |
| TEST-6 | Remove `continue-on-error` from `workspace-test` and make every job required on `master` | Implemented (pull requests 67, 124). 21 required checks |
| FEAT-8 | Make the layout tests force a version bump, version the client path, and plan the move off `bincode` 1.x | Implemented (pull requests 40, 110, 120). Dependabot pull request 86 proposes `bincode` 3 |
| MOB-3 | Do not charge evacuation to retry budgets | Implemented (pull request 108) |
| FEAT-11 | Remove dead code and merge the two connection loops in `server.rs` | Implemented (pull requests 125, 126, 127, 131). `compute_intensity` leaves the wire with FEAT-1 |

**Exit check:** no CI job has `continue-on-error`, every job is required on `master`, and the items
above are Implemented.

Passed on 2026-11-08 at `master` `6723945`. The one CI job that is not required is
"p2p wan relay (ignored)", which runs only on a schedule or by hand.

### Phase B: durable and contained

Goal: a master restart loses nothing, and a task cannot reach the worker's secrets.

| Item | Work | State on 2026-11-09 (`master` `b7f112f`) |
|---|---|---|
| FEAT-1 | Journal in `--state-dir`, replay at start, and `ResumeSession` so a master adopts running tasks | Implemented (pull requests 129, 141, 147, 158, 167, design 0046). `tests/master_restart_recovery.rs` is the "Done when" test |
| FEAT-2 | A supervisor per task group that kills the group when the worker dies, on Linux and macOS | Implemented (pull request 123, design 0031) |
| FEAT-3 | An `--isolation` flag with the levels `off`, `fs` and `fs+net`, using Landlock and namespaces on Linux, `sandbox-exec` on macOS, and AppContainer or a restricted token on Windows | Partial. Design 0033 (Accepted). The core and macOS (`FEAT-3a`, pull request 133) and Linux (`FEAT-3b`, pull request 150) are merged. Windows (`FEAT-3c`) is open as pull request 152 |

This phase comes before workspaces. A workspace keeps the owner's source code on another machine
and runs commands an agent chose. Without FEAT-3 such a command can read `cluster.secret`. Without
FEAT-1 a master restart loses a long build.

**Exit check:** the "Done when" tests of FEAT-1, FEAT-2 and FEAT-3 pass in CI. FEAT-1 and FEAT-2
pass; FEAT-3 waits for Windows (`FEAT-3c`).

### Phase C: agent workspaces

Goal: the user story works between two real machines. The design record is
`design/0039-agent-workspaces.md` (Accepted). Its work items W1 to W11 carry out the steps below.

| ID | Work | Removes | State on 2026-11-09 (`master` `b7f112f`) |
|---|---|---|---|
| WS-1 | A named workspace per worker that outlives a task. It holds its own build cache, such as `target/`, under a size bound with least-recently-used eviction | FEAT-4: fresh sandbox per task | Partial. The store (W2a, pull request 164) and the scheduler side (W3, pull request 166: required feature, affinity, one task per workspace) are merged. The runner and client wiring (W2b) is open |
| WS-2 | Source by tree sync: the client sends its working directory as a blob tree that follows `.gitignore` and leaves out `.git/`. Only changed files cross the network, files deleted locally are deleted on the worker, and build output stays. Workers need no `git`. Symbolic links and empty directories come later (design 0039, W11) | FEAT-4: whole-project uploads, `.gitignore` | Partial. The scan with `.gitignore` and limits (W4, pull request 156) and the worker's sync (W2a) are merged; neither is wired to `submit` yet |
| WS-3 | P2P tickets in `connect_master`, with a `STREAM_CLIENT` stream tag, so every client and the MCP server reach a P2P-only master | FEAT-4: TCP-only clients | Planned (W7). The `Hello` probe of protocol 6 (W1, pull request 157) is merged |
| WS-4 | `cargo build --offline` when `Cargo.toml` is present, and a `tool:rustc` worker tag | FEAT-10 | Implemented (W8, pull request 165) |
| WS-5 | MCP tools for workspaces, with timeouts suited to builds rather than the 60 s default | FEAT-4: default timeout | Partial. W5 (`submit --workspace`, the MCP `workspace` field and the `workspaces` tool) is open as pull request 168 |
| WS-6 | Pipelined and compressed uploads from the client | The input-file limits listed in `README.md` | Partial. Several blobs in flight from one client (W9, pull request 160) is merged; compression is out of scope (design 0006) |
| WS-7 | A guide in `docs/guides/` for offloading `cargo test` from a coding agent, with a recorded two-machine run | | Planned (W6 end-to-end test and W10 guide) |
| CACHE-1 | Reuse the result of an identical task, keyed by the command, the environment and the input digests (decision Q7) | Exact repeats of a run | Planned |

Workspaces are not usable end to end yet: a task cannot name a workspace from the CLI until W5 and
W2b land.

CACHE-1 is not part of the exit check. It starts with a design record, and MCP turns it on by
default only after its hit rate on the WS-7 story is recorded.

**Exit check:** the FEAT-4 "Done when" passes between two machines, the second run reuses `target/`,
and the run is recorded with its date.

### Phase D: first release

Goal: someone without Rust installed follows the README and reaches the user story.

| Item | Work | State on 2026-11-09 (`master` `b7f112f`) |
|---|---|---|
| REL-4 | Release workflow on `v*` tags with Linux, macOS and Windows archives, with and without `gpu-wgpu`, and `SHA256SUMS`. An install section in `README.md` | Implemented (pull request 155). No tag exists yet |
| Quick start | From install to the first offloaded `cargo test`, linking [guides/mcp-client-setup.md](guides/mcp-client-setup.md) and the WS-7 guide | Planned |
| Release notes | A `CHANGELOG.md` entry for the release, with the upgrade order for mixed versions | Planned. `[Unreleased]` carries the entries and an upgrade note |
| JOIN-1 | One join token that carries the master's address or ticket and the cluster secret, read from standard input (decision Q7) | Partial (pull request 163, design 0048). The QR code step waits for AND-1 |
| LINK-1 | `doctor` shows whether each worker is direct or relayed, and the owner can name a relay of their own (decision Q7) | Partial (pull request 159: `LINK` column, checks C7 and C8). The relay option and the guide are Planned |
| BENCH-1 | A benchmark page for task overhead, repeated uploads and the WS-7 offload, each number with its script and date (decision Q7) | Partial (pull request 162: overhead and repeated uploads). The WS-7 offload waits for WS-7 |

**Exit check:** a tagged release exists with archives and checksums, and a machine without Rust runs
the WS-7 guide from the README alone, joining with one token (JOIN-1). The benchmark page holds a
dated run of the WS-7 offload (BENCH-1).

### Launch

After the Phase D exit check, and not before it:

1. Record a short demo of the user story: an agent on one operating system runs `cargo test` on a
   machine with another, and the second run is faster.
2. Link the demo, the release and the BENCH-1 page from the top of `README.md`. Every number on the
   page links to its run record (writing standard rule 6).
3. Announce the release, for example on Hacker News.
4. A second demo, one language model larger than any single machine split over several of them,
   waits for LLM-2 and LLM-4 of design 0038 (decision Q4). LLM-1, which routes a prompt to the
   worker that holds a model, can be shown earlier.

### Phase E: reach

After the first release, in order of value to the user story:

1. **FEAT-7:** usable rendezvous discovery, so machines on different networks find the master.
2. **FEAT-5:** checkpoints for process tasks, so long jobs resume after a worker loss.
3. **FEAT-6:** real master failover. It starts only after FEAT-1, and needs replicated state,
   renewed leases and a fencing epoch (decision D1).
4. **AND-1 to AND-4:** the Android app. Phones add little to build and test, so the app is built when
   a user needs it.
5. **MESH-2:** deliver relay data to a consumer. It belongs to the mesh's relay role, which
   decision Q3 keeps. **MESH-3** adds grid features to the mesh, which Q3 rules out. Only its
   security part goes ahead, at any time: remove the fallback in which `parse_binary_data` reads a
   hub-local file that a peer names (`crates/agent_mesh/src/bridge.rs`).

## What waits, and why

| Area | Items | Why it waits | Plan |
|---|---|---|---|
| 3D generation and rigging | 3D-2, 3D-3 | It does not serve the user story | Frozen. Since FEAT-11 (pull request 127), `gen3d` and `autorig` build only with the opt-in Cargo feature `3d` |
| Language model inference | LLM-1 to LLM-5 ([design 0038](design/0038-large-language-model-inference.md)) | LLM-2 needs FEAT-1 and FEAT-8. LLM-4 is a new transport | LLM-1 (a guide and a script, size S) can start at any time. LLM-2 to LLM-5 start after Phase C. The research of 2026-11-08 supports the layer split of D2 to D5 for home networks |
| Remote Execution API cache | REAPI-1 | It serves Bazel and Buck2 users rather than the user story, and adds a gRPC listener | After CACHE-1, cache-only. The `Execution` service is not planned |
| GPU kernels | FEAT-9 | Decision D6 keeps GPU compute a benchmark | State "benchmark only" in `README.md` and `PROJECT.md`, as FEAT-9 asks |
| Agent Mesh grid features | MESH-3 | MCP already lets agents submit work. Two agent interfaces split the effort | Not built (decision Q3). The removal of the hub-local file fallback, a security fix, landed in pull requests 114 and 132 |
| Agent Mesh relay data | MESH-2 | It does not serve the user story | Phase E |
| Distributed training | TRAIN-1 to TRAIN-5 ([design 0045](design/0045-distributed-training.md)) | It does not serve the user story. Until FEAT-3 a training task can read the worker's `cluster.secret`, and long rounds need FEAT-1 and FEAT-5 | After the Phase D exit check (decision Q6). TRAIN-1, independent runs, comes first |

## Decisions for the owner

The owner accepted Q1 to Q5 on 2026-11-07, and Q6 and Q7 on 2026-11-08.

| ID | Question | Decision |
|---|---|---|
| Q1 | Is "a coding agent runs builds and tests on the machines its owner has" the purpose? | Yes |
| Q2 | Freeze 3D and move it behind an opt-in Cargo feature? | Yes, together with FEAT-11 |
| Q3 | Keep the Agent Mesh as a second way for agents to submit grid work? | No. MCP is the agent interface. The mesh keeps its relay role and gets no new grid features |
| Q4 | When does language model work start? | LLM-1 at any time. LLM-2 to LLM-5 after Phase C |
| Q5 | Make every CI job a required check on `master` (TEST-6)? | Yes |
| Q6 | When does distributed training start? | After the Phase D exit check. Design 0045 records the direction, and nothing is built before then |
| Q7 | Add the items from the research of 2026-11-08? | Yes. JOIN-1, LINK-1 and BENCH-1 join Phase D. CACHE-1 joins Phase C outside its exit check. REAPI-1 waits |

## Risks

| Risk | Mitigation |
|---|---|
| Effort spreads over many partial subsystems | Principle 1, and the table of what waits |
| Agents run arbitrary commands on the owner's machines | Phase B before Phase C. MCP already refuses a remote master without a secret (SEC-2) |
| A wire layout change breaks mixed versions | FEAT-8 in Phase A |
| `bincode` 1.x is unmaintained (RUSTSEC-2025-0141) | The FEAT-8 plan. Review pull request 86 against the layout tests in `crates/core/src/wire_layout_tests.rs` |
| Documents drift from the code | The writing standard in [README.md](README.md), and a BACKLOG Index kept current |
| A launch claims more than the evidence shows | BENCH-1, writing standard rule 6, and the order in "Launch" |
| Links that fall back to the n0 public relays are rate-limited | LINK-1 |

## Status log

- 2026-11-07: Draft written from the completion backlog at `master` `5c518b5` and from design 0038.
- 2026-11-07: The owner accepted Q1 to Q5. Status set to Accepted.
- 2026-11-08: Phase A states updated at `master` `5df82df`. WS-2 follows design 0039, a tree
  sync instead of a commit checkout. Decision Q3 applied to MESH-2 and MESH-3. The protocol version
  row corrected: the master does check a worker's version.
- 2026-11-08: Phases A to D named the core, and principle 6 added: later work starts after the
  Phase D exit check. The owner accepted Q6. Distributed training (design 0045) added to the table
  of what waits.
- 2026-11-08: Phase A passed its exit check at `master` `6723945`. Phase B states added: FEAT-2 is
  Implemented, FEAT-1 is Partial (its wire layer and store, pull requests 141 and 129, are merged),
  and FEAT-3 has an open pull request. "Where the project stands" moved to the
  same commit: containment and the wire layout check now work, and their rows left "What is
  missing". The 3D and Agent Mesh rows of "What waits" record what FEAT-11 and MESH-3 landed.
- 2026-11-08: FEAT-3 is Partial at `master` `4e1aeb3`: the core and macOS (`FEAT-3a`, pull request
  133) are merged. The "What is missing" row now names Linux and Windows only.
- 2026-11-08: A web research run added "What the research of 2026-11-08 found", three rows of
  neighbouring tools and the Launch section. The owner accepted Q7: JOIN-1, LINK-1 and BENCH-1 in
  Phase D, CACHE-1 in Phase C outside its exit check, and REAPI-1 in the table of what waits.
- 2026-11-09: States updated at `master` `b7f112f`. FEAT-1 is Implemented (pull requests 147, 158,
  167 on top of 129 and 141) and leaves "What is missing", with "Durability" and "Isolation" rows
  added to "What works". FEAT-3 has Linux (pull request 150); Windows is open as pull request 152.
  FEAT-10 and WS-4 are Implemented (pull request 165). Phase C and Phase D tables gained a state
  column: workspaces are Partial (W1, W2a, W3, W4, W8, W9 merged; W5 open as pull request 168; W2b,
  W6, W7, W10 not started), and REL-4 is Implemented with no tag cut. JOIN-1, LINK-1 and BENCH-1
  are Partial.
