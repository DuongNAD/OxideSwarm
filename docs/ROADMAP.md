# Roadmap

Status: Accepted. The owner accepted decisions Q1 to Q5 on 2026-11-07, Q6 and Q7 on 2026-11-08, and
Q8 to Q12 (Phases F and G) and Q13 (ecosystem model A) on 2026-11-09, Q27 to Q29 (the charter of
design 0057) on 2026-11-10, and Q37 (the owner runs the study of first use of design 0059) on
2026-11-11.
Created: 2026-11-07    Updated: 2026-11-11

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
- Ambition (Draft, 2026-11-09): grow from one owner's machines to a grid that a team or a community
  runs at scale, with many workers, many submitters and standard tools around it. Phase F (scale
  out) and Phase G (product and ecosystem) carry this work. They follow the core, and the
  architecture is in [design 0050](design/0050-scale-out-architecture.md).
- Long-term goal (Draft, 2026-11-10): an open compute network where anyone can offer and use idle
  compute, safely and with checkable results. Phase H carries it, and the architecture is in
  [design 0054](design/0054-open-compute-network.md).
- Frontier program (Draft, 2026-11-10): what a person outside the project would switch to
  OxideSwarm for. Tasks become portable, movable and checkable, through bets A to E. Each bet
  starts with a spike that can end it and finishes with a recorded run on real machines. The plan
  is in [design 0056](design/0056-frontier-program.md), and decisions Q19 to Q26 and Q30 are open.
- Lasting value (Draft, 2026-11-10): why anyone should trust the grid, build on it, cite it and
  find it alive after its founder. Fuzzing, deterministic simulation, a written specification with
  a second decoder, citable releases, and a release without the owner. The plan is in
  [design 0058](design/0058-lasting-value.md). Its first four items are in the backlog, and
  decisions Q31 and Q33 to Q35 are open.
- First users (Draft, 2026-11-10): who gains what from the grid today, and the order of work that
  serves them. The first audience is a developer with a coding agent and machines on more than one
  operating system. A study of first use comes before more is built. The plan is in
  [design 0059](design/0059-first-users.md). The owner accepted Q37 on 2026-11-11 and runs the
  first round of that study. Q36, Q38 and Q39 are open.

## Where the project stands

The state at `master` `3ce2a65` on 2026-11-09, after the live two-machine run.

### What works

| Area | What the code does | Code |
|---|---|---|
| Scheduling | Task state machine with retries, placement by OS, architecture, worker and tag, GPU gating, host CPU backpressure, requeue when a worker is lost | `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/core/src/placement.rs` |
| Trust and transport | Cluster-secret handshake on TCP and iroh QUIC. ChaCha20-Poly1305 records on authenticated TCP | `crates/core/src/cluster_auth.rs`, `crates/core/src/transport.rs` |
| Files | Files and directories in, glob patterns out, as BLAKE3 blobs. A blob the other side has is not sent again | `crates/core/src/blob.rs`, `crates/master/src/blob_store.rs`, `crates/worker/src/blob_cache.rs` |
| Containment | After `kill -9` of a worker, a supervisor per task group kills the task's processes within 5 s on Linux and macOS. Windows uses a Job Object | `crates/worker/src/process_tree.rs`, `tests/cli_worker_signals_e2e.rs` |
| Agents | `oxideswarm mcp` with the tools `status`, `workers`, `submit`, `task_result`, `cancel` and `doctor`, wait deadlines, cancellation and head-and-tail output | `crates/cli/src/mcp.rs` |
| Durability | `master --state-dir` keeps queued, running and finished tasks in a journal and replays it at start. A worker keeps its tasks across a short disconnect, and the master adopts them again (`ResumeSession`) | `crates/master/src/store.rs`, `crates/master/src/session.rs`, `tests/master_restart_recovery.rs` |
| Isolation | `--isolation off\|fs\|fs+net` on macOS (`sandbox-exec`), Linux (Landlock, seccomp, namespaces) and Windows (AppContainer). A task reads neither `cluster.secret` nor other sandboxes | `crates/worker/src/isolation.rs`, design 0033 |
| Workspaces | `submit --workspace` and the MCP `workspace` field sync a source tree that follows `.gitignore` into a named workspace on a worker, which keeps `target/` between runs. Clients reach a master over TCP or a P2P ticket | `crates/worker/src/workspace.rs`, `crates/cli/src/blobs.rs`, design 0039, [guide](guides/offload-cargo-test.md) |
| Operations | `oxideswarm doctor`, the dashboard, launchers and service installers | `crates/cli/src/doctor.rs`, `crates/master/src/web_ui.rs`, `switch_role.sh`, `windows/oxctl.ps1`, `packaging/` |
| Checks | CI runs rustfmt, clippy, unit tests, the full workspace suite, the launcher tests and the Python suites on Linux, macOS and Windows. Every job is a required check on `master`. A changed wire layout fails CI without a protocol version bump | `.github/workflows/ci.yml`, `crates/core/src/wire_layout_tests.rs` |

### What is missing

Each row is a limit a user meets today, and the item that removes it.

| Limit | Effect | Item |
|---|---|---|
| There is one master and no failover | The master is a single point of failure | FEAT-6 (Design only, decision D1) |
| Delivery is at least once | A task can run twice after a worker is lost. A master with `--state-dir` and a worker that resumes shrink the window | FEAT-1, FEAT-2 |
| `cargo` does not run inside the Windows AppContainer, so a Windows worker refuses `--workspaces` with `--isolation fs\|fs+net` | A Windows workspace worker runs `cargo` without OS isolation | FEAT-3d |
| Workspace trees carry no symbolic links or empty directories | A project that relies on a link inside its tree does not build remotely | Design 0039 W11 |
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

| Item | Work | State on 2026-11-09 |
|---|---|---|
| FEAT-1 | Journal in `--state-dir`, replay at start, and `ResumeSession` so a master adopts running tasks | Implemented (pull requests 129, 141, 147, 158, 167, design 0046). `tests/master_restart_recovery.rs` is the "Done when" test |
| FEAT-2 | A supervisor per task group that kills the group when the worker dies, on Linux and macOS | Implemented (pull request 123, design 0031) |
| FEAT-3 | An `--isolation` flag with the levels `off`, `fs` and `fs+net`, using Landlock and namespaces on Linux, `sandbox-exec` on macOS, and AppContainer or a restricted token on Windows | Implemented. Design 0033 (Accepted). macOS (`FEAT-3a`, pull request 133), Linux (`FEAT-3b`, pull request 150) and Windows (`FEAT-3c`, pull request 152) are merged. `cargo` inside the Windows AppContainer is `FEAT-3d` (Planned), so a Windows workspace worker runs with `--isolation off` |

This phase comes before workspaces. A workspace keeps the owner's source code on another machine
and runs commands an agent chose. Without FEAT-3 such a command can read `cluster.secret`. Without
FEAT-1 a master restart loses a long build.

**Exit check:** the "Done when" tests of FEAT-1, FEAT-2 and FEAT-3 pass in CI. All three pass
(FEAT-3 on Windows since pull request 152); Phase B is complete.

### Phase C: agent workspaces

Goal: the user story works between two real machines. The design record is
`design/0039-agent-workspaces.md` (Accepted). Its work items W1 to W11 carry out the steps below.

| ID | Work | Removes | State on 2026-11-09 |
|---|---|---|---|
| WS-1 | A named workspace per worker that outlives a task. It holds its own build cache, such as `target/`, under a size bound with least-recently-used eviction | FEAT-4: fresh sandbox per task | Implemented (W2a, pull request 164; W2b, pull request 176; W3, pull request 166) |
| WS-2 | Source by tree sync: the client sends its working directory as a blob tree that follows `.gitignore` and leaves out `.git/`. Only changed files cross the network, files deleted locally are deleted on the worker, and build output stays. Workers need no `git`. Symbolic links and empty directories come later (design 0039, W11) | FEAT-4: whole-project uploads, `.gitignore` | Implemented (W4, pull request 156; W2a and W2b; W5). Symbolic links and empty directories wait for W11 |
| WS-3 | P2P tickets in `connect_master`, with a `STREAM_CLIENT` stream tag, so every client and the MCP server reach a P2P-only master | FEAT-4: TCP-only clients | Implemented (W1, pull request 157; W7, pull requests 170 and 171) |
| WS-4 | `cargo build --offline` when `Cargo.toml` is present, and a `tool:rustc` worker tag | FEAT-10 | Implemented (W8, pull request 165) |
| WS-5 | MCP tools for workspaces, with timeouts suited to builds rather than the 60 s default | FEAT-4: default timeout | Implemented (W5, pull request 168). With a workspace, the task timeout defaults to 1800 s |
| WS-6 | Pipelined and compressed uploads from the client | The input-file limits listed in `README.md` | Partial. Several blobs in flight from one client (W9, pull request 160) is merged; compression is out of scope (design 0006) |
| WS-7 | A guide in `docs/guides/` for offloading `cargo test` from a coding agent, with a recorded two-machine run | | Implemented. W6 (pull request 174) tests it on Linux, macOS and Windows; W10 (pull request 177) is the [guide](guides/offload-cargo-test.md), with the live run of 2026-11-09 |
| CACHE-1 | Reuse the result of an identical task, keyed by the command, the environment and the input digests (decision Q7) | Exact repeats of a run | Planned |

Workspaces work end to end from the CLI and over MCP, between two machines, since 2026-11-09.

CACHE-1 is not part of the exit check. It starts with a design record, and MCP turns it on by
default only after its hit rate on the WS-7 story is recorded.

**Exit check:** the FEAT-4 "Done when" passes between two machines, the second run reuses `target/`,
and the run is recorded with its date. Passed on 2026-11-09: over MCP, a Mac ran `cargo test -p
rusty_grid_core --lib` on a Windows worker at a commit that machine did not have, in 207.4 s cold and
6.6 s warm ([record](guides/offload-cargo-test.md#recorded-two-machine-run)). Phase C is complete
except W11, which is optional.

### Phase D: first release

Goal: someone without Rust installed follows the README and reaches the user story.

| Item | Work | State on 2026-11-09 |
|---|---|---|
| REL-4 | Release workflow on `v*` tags with Linux, macOS and Windows archives, with and without `gpu-wgpu`, and `SHA256SUMS`. An install section in `README.md` | Implemented (pull request 155). The first tag is `v0.1.0` |
| Quick start | From install to the first offloaded `cargo test`, linking [guides/mcp-client-setup.md](guides/mcp-client-setup.md) and the WS-7 guide | Implemented (`README.md`, Quick start) |
| Release notes | A `CHANGELOG.md` entry for the release, with the upgrade order for mixed versions | Implemented: `CHANGELOG.md` `[0.1.0]` with the upgrade order |
| JOIN-1 | One join token that carries the master's address or ticket and the cluster secret, read from standard input (decision Q7) | Partial (pull request 163, design 0048). Checked live on Windows on 2026-11-09. The QR code step waits for AND-1 |
| LINK-1 | `doctor` shows whether each worker is direct or relayed, and the owner can name a relay of their own (decision Q7) | Partial (pull request 159: `LINK` column, checks C7 and C8; pull request 173: `--relay-url` and the [relay guide](guides/self-hosted-relay.md)). A live run through a self-hosted relay is not recorded yet |
| BENCH-1 | A benchmark page for task overhead, repeated uploads and the WS-7 offload, each number with its script and date (decision Q7) | Partial (pull requests 162 and 175). The dated WS-7 offload record (c) exists; the script still has to be run on Linux and Windows |

**Exit check:** a tagged release exists with archives and checksums, and a machine without Rust runs
the WS-7 guide from the README alone, joining with one token (JOIN-1). The benchmark page holds a
dated run of the WS-7 offload (BENCH-1).

Checked on 2026-11-09. [`v0.1.0`](https://github.com/DuongNAD/OxideSwarm/releases/tag/v0.1.0)
has 8 archives and `SHA256SUMS`. The macOS archive (checksum verified) ran the master and the
client, and the Windows archive (checksum verified, no SmartScreen prompt) ran the worker. With
those binaries only, the Quick start command offloaded `cargo test -p rusty_grid_core --lib` from
the Mac to the Windows worker: 249 tests passed, and the repeated run took 7.2 s. The client
machine has Rust installed, but the run did not use it. `oxideswarm join` was checked on the
Windows machine with a build of `5c9e126`, which has the same join code as the release. BENCH-1 (c)
is recorded. Phase D is complete; the Launch section comes next.

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

0. **Machines anywhere (decision Q13, model A):** each person or team runs a cluster, and trusted
   people add their machines to it from any network.
   - **ENROLL-1** ([design 0051](design/0051-machine-identity-and-invites.md)): a key per machine,
     invites that work once, expire and need approval, revocation, and a handshake with forward
     secrecy. Only the master keeps the cluster secret.
   - **JOIN-2** ([design 0052](design/0052-short-code-pairing.md)): join on a LAN with a short code
     such as `7-rocket-violin`.
   - **USB-1** ([design 0053](design/0053-usb-setup-kit.md)): a USB stick that installs, joins and
     starts a worker on macOS, Windows and Linux with one double-click and a passphrase.

   ENROLL-1 comes first. JOIN-2 and USB-1 follow in parallel. The release that ships them is
   `0.2.0`, because ENROLL-1 raises the protocol version to 8.
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

### Phase F: scale out

Draft, after the core (principle 6). Goal: the grid runs with hundreds of workers and many
submitters, survives the loss of its master, and grows and shrinks with its queue. The design
record is [design 0050](design/0050-scale-out-architecture.md) (Draft). The target scale is open
(Q8), and SCALE-1 measures where the limits are before any other item is sized.

| ID | Work | Relates to |
|---|---|---|
| SCALE-1 | Load harness: simulated workers at 100, 500 and 1000, with published numbers under the rules of BENCH-1 | Extends BENCH-1 |
| SCALE-2 | Scheduler and queue at scale: indexes, larger batches, cold results out of memory, fair share per submitter | Needs SCALE-1. Fair share needs ECO-3 |
| SCALE-3 | Worker-to-worker blob distribution: workers advertise cached blob ids, the master is tracker and fallback | Needs SCALE-1 |
| SCALE-4 | Master high availability: replicated journal or active-passive with a fencing epoch (Q9) | Supersedes the Design-only part of FEAT-6. Builds on FEAT-1 |
| SCALE-5 | Autoscaling providers driven by queue depth: SSH pool, cloud VMs, Kubernetes (Q12) | Uses JOIN-1 tokens. Needs SCALE-1 |
| SCALE-6 | Multi-site federation. Last, and only if Q8 needs more than one site | Needs SCALE-2 to SCALE-4 |

**Exit check:** the SCALE-1 harness records, with date and hardware, a grid at the worker and
submitter counts of Q8, with scheduling latency inside the bound the owner sets. A run that kills
the master leader loses no accepted task (SCALE-4). A queue backlog starts workers through at least
one provider and idle time drains them (SCALE-5).

### Phase G: product and ecosystem

Draft, after the Phase D exit check; its items run in parallel with Phase F where they do not
depend on it. Goal: other people and tools can install, call and trust OxideSwarm without the
owner's help. Order of priority is open (Q10, Q11).

| ID | Work | Relates to |
|---|---|---|
| ECO-1 | Remote Execution API compatibility: grow the cache facade into remote execution for Bazel, Buck2 and Pants | Grows REAPI-1 (cache only) into the `Execution` service. Needs ECO-2 |
| ECO-2 | Action cache: reuse the result of an identical task | Is CACHE-1. Not duplicated |
| ECO-3 | Multi-tenancy and quotas: tenants, projects, scoped tokens, quotas, audit log | Design 0050 D6 |
| ECO-4 | Observability: Prometheus metrics and OpenTelemetry traces | Design 0050 D7 |
| ECO-5 | Packaging: Homebrew, winget, apt and rpm, a container image, signed and notarized binaries | Extends REL-4 |
| ECO-6 | Client SDKs and a versioned public API | Needs a stable subset of the client protocol |
| ECO-7 | Integrations for coding agents and CI: MCP setups for common agents, a GitHub Actions runner pool on the grid | Extends DOC-7 and `mcp` |
| ECO-8 | Documentation site and community: contributing, governance, security reporting | Extends REL-5 |

**Exit check:** a Bazel project builds twice against the grid and the second build hits the cache
(ECO-1 cache part through ECO-2), a user on each of macOS, Windows and Linux installs from a package
manager and runs the quick start (ECO-5), a Prometheus scrape of a running master returns the
design 0050 series (ECO-4), and a second tenant is refused when over quota (ECO-3). Each result is
recorded with its date.

### Phase H: an open compute network (long term)

Draft, set by the owner on 2026-11-10 as the long-term goal: grow OxideSwarm into an open network
where anyone can offer idle compute and anyone can use it, safely and with checkable results. It is
an operating system for a planet-sized cluster, not a desktop OS. The architecture is
[design 0054](design/0054-open-compute-network.md).

It takes years. Each stage is useful alone:

| Stage | Items | What it adds |
|---|---|---|
| H0 | NET-1 | A WebAssembly task type in today's trusted clusters: one binary for every OS, a first sandbox |
| H1 | NET-2 | Federation between trusted clusters with signed credit accounting (builds on SCALE-6) |
| H2 | NET-3, NET-4 | Public pool alpha: WebAssembly only, redundancy and spot checks, reputation, contributor controls |
| H3 | NET-5 | Native tasks in lightweight VMs; public beta |
| H4 | NET-6, NET-7 | Distributed directory and scheduler; open specification and a neutral steward |
| later | NET-8 | Trusted execution environments for confidential tasks |

The hard problems are not the network. They are:
- trusting strangers' code (sandboxes, design 0054 D2);
- trusting strangers' results (verification, D4);
- fair exchange (credits, D6);
- abuse and governance (D7).

H0 can start after ENROLL-1, because it is useful inside trusted clusters. H1 needs SCALE-6. H2
needs ECO-3 and an independent security review of its threat model.

A project that receives donated compute publishes its outputs and its task code. The draft charter
is [design 0057](design/0057-open-results-charter.md). The owner accepted its principle on
2026-11-10 (Q27 to Q29), and its text still needs a legal review. It is a
condition for using the public pool, not a term of the software licence, and it does not cover a
cluster on its owner's machines.

**Exit check per stage:** the stage table of design 0054, each result recorded with its date.

### Frontier program: bets across the phases

Draft, proposed on 2026-11-10; decisions Q19 to Q26 and Q30 are open. The phases above say how the
grid grows. [Design 0056](design/0056-frontier-program.md) says what a person outside the project
would switch to it for, from a research run of 2026-11-10. It moves nothing the owner accepted.

Three properties are the test for its work. A task runs on any machine (portable), outlives the
machine it started on (movable), and leaves evidence of what it did (checkable).

| Bet | What it builds | Items | Lighthouse run |
|---|---|---|---|
| A | Compute shaped for coding agents: several operating systems in one call, sharded test runs, live output, workspace forks, device leases | AX-1 to AX-7, CACHE-1 | One call from an agent tests a change on macOS, Windows, Linux and a real Android phone |
| B | Portable and movable tasks on WebAssembly, with checkpoints a task takes itself | NET-1, WASM-2 to WASM-5 | A job starts on a laptop, the lid closes, and it finishes on another machine with identical output |
| C | Signed receipts, quorum runs, a reproducible release, independent rebuilds | PROOF-1 to PROOF-5 | Two machines with different owners build a release, and the archives match |
| D | Language models served by ordinary machines: one small model per machine behind a gateway | LLM-6 to LLM-8, EDU-1, EDU-2 | A school lab becomes a private assistant for the class from a USB stick, without Internet access |
| E | The commons: Phase H with a first workload (independent rebuilds), rules for a good guest, and a worker image that starts a whole machine from USB or the network | GREEN-1, GREEN-2, EDU-3, IMG-1 to IMG-4, with NET-2 to NET-4 | A school lab works for the grid at night and is back on its own system in the morning. The stage checks of design 0054 |

Order: each bet starts with a short spike that can end it (R1 to R10). Waves follow the releases:
agents after `0.2.0`, then evidence and portability, then models and classrooms, then the commons.
Each wave has one protocol version bump at most and ends with a recorded lighthouse run.

The research also questions step 4 of "Launch". In the published run, one model split over home
machines reached about 1.5 tokens per second over Wi-Fi. Design 0056 proposes the classroom run as
the model demo instead (Q25).

**Exit check per bet:** its lighthouse run in design 0056 D7, recorded with script and date.

### Lasting value: trust, a specification, evidence and stewardship

Draft, proposed on 2026-11-10; decisions Q31 and Q33 to Q35 are open.
[Design 0058](design/0058-lasting-value.md) adds no feature. It plans why anyone should trust the
grid, build on it, cite it, and find it alive after its founder has moved on.

| Track | What it builds | Items | Proof run |
|---|---|---|---|
| Assurance | Fuzzing of every parser, deterministic simulation of master and workers with injected faults, a checked model of failover, an inventory of `unsafe`, provenance for releases, an independent review | LV-1 to LV-7 | The nightly simulation ran its seeds and every invariant held |
| Specification | The cluster protocol and the formats written down, with test vectors | LV-8, LV-9 | A decoder written from the specification alone reads every vector |
| Evidence | A citation file and a DOI per release, a measurement study of a school lab, a case study of the development method | LV-10 to LV-13 | The records and the texts are public |
| Stewardship | Governance files, a release runbook, a second person who can release, funding applications | LV-14 to LV-17 | A second person cuts a release from the runbook alone |

Rule: every release ships at least one item of design 0058, and the record takes one lane of work
at most. The first items need nothing else: LV-10, LV-14, LV-5 and the first fuzz targets of LV-1.
They are in [BACKLOG.md](BACKLOG.md) since 2026-11-10, as its Phase 10.

**Exit check per track:** its proof run in design 0058 D6, recorded with its date.

### First users: who gains what, and in which order

Draft, proposed on 2026-11-10. The owner accepted Q37 on 2026-11-11; Q36, Q38 and Q39 are open.
[Design 0059](design/0059-first-users.md) asks what a person gains from installing the grid, and
orders the work by that. Its statements about users are reasoning until its study or a recorded
run supports them.

| Audience | The job they already have | What is missing | Items |
|---|---|---|---|
| A developer with a coding agent and machines on more than one operating system (first) | Checking a change on Windows, macOS and Linux | One call that fans out and returns one table | AX-1, AX-3 |
| A developer with a slow test suite and a second machine | Waiting for the suite | Shards over several machines, and a number that shows the gain | AX-2, ADOPT-2 |
| The owner of a desktop with a GPU who works on a laptop | Using a local model away from the desk | A routed prompt, and an endpoint that existing clients call | LLM-1, LLM-6 |
| A small team, a lab or a class with idle machines | Batch jobs that run on one machine | A guide per job, batch inference, the classroom profile | ADOPT-4, LLM-8, EDU-1 |
| A maintainer who waits for hosted CI runners or pays for them | CI on three operating systems | Their own machines as runners | AX-7 |

Order: a study of first use with three to five people (ADOPT-1), a run summary (ADOPT-2), AX-1,
AX-2, install scripts with a scripted time to first result (ADOPT-3), guides for common jobs
(ADOPT-4), then LLM-1 and LLM-6. The study may reorder the later steps. No item of design 0059 is
in [BACKLOG.md](BACKLOG.md).

**Exit check:** the second round of the study is recorded, with measures M15 to M17 of design
0059.

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

LLM inference, training, Android and 3D become candidates for a later phase once Phase F lands.
This makes no commitment: each still needs its own owner decision. Workloads that split well, such
as TRAIN-1's independent runs, may also run in the open network of Phase H.

## Decisions for the owner

The owner accepted Q1 to Q5 on 2026-11-07, and Q6 and Q7 on 2026-11-08. Q8 to Q12 were open until the owner accepted their recommended defaults on 2026-11-09: the
right column is a recommended default, not a decision, and no work depends on it until the owner
answers.

| ID | Question | Decision |
|---|---|---|
| Q1 | Is "a coding agent runs builds and tests on the machines its owner has" the purpose? | Yes |
| Q2 | Freeze 3D and move it behind an opt-in Cargo feature? | Yes, together with FEAT-11 |
| Q3 | Keep the Agent Mesh as a second way for agents to submit grid work? | No. MCP is the agent interface. The mesh keeps its relay role and gets no new grid features |
| Q4 | When does language model work start? | LLM-1 at any time. LLM-2 to LLM-5 after Phase C |
| Q5 | Make every CI job a required check on `master` (TEST-6)? | Yes |
| Q6 | When does distributed training start? | After the Phase D exit check. Design 0045 records the direction, and nothing is built before then |
| Q7 | Add the items from the research of 2026-11-08? | Yes. JOIN-1, LINK-1 and BENCH-1 join Phase D. CACHE-1 joins Phase C outside its exit check. REAPI-1 waits |
| Q8 | What scale does the Phase F exit check target: how many workers and how many submitters? | 500 workers and 20 submitters on one site, with 1000 workers as a stretch run of SCALE-1 |
| Q9 | Which HA approach does SCALE-4 build: active-passive with a fencing epoch, or a Raft-replicated journal? | Active-passive first, with the journal format and the epoch fixed so Raft can replace the replication layer later |
| Q10 | Which comes first for builds: compatibility with the Remote Execution API (ECO-1) or the grid's own API and MCP (ECO-6, ECO-7)? | The own API and MCP first, REAPI cache-only next, remote execution last |
| Q11 | Which licence and is there a commercial or hosted offering? | Keep `MIT OR Apache-2.0`. No hosted offering decided; revisit after the first release |
| Q12 | Which autoscaling providers does SCALE-5 build first? | SSH pool, then Kubernetes, then one cloud |
| Q14 | How does the open network (Phase H) count contribution: credits, money or tokens, or nothing? | Open. Recommended: plain credits, earned by verified work and spent on running work; no cryptocurrency |
| Q15 | Which sandbox for strangers' code comes first: WebAssembly or lightweight VMs? | Open. Recommended: WebAssembly first (one binary for every OS, deterministic, cheap to verify), VMs in H3 |
| Q16 | Which workloads does the public pool take first? | Open. Recommended: open-source CI builds and tests, Monte Carlo and parameter sweeps, rendering; all split well and can be checked |
| Q17 | Who governs the network once it has outside users? | Open. Recommended: the owner until then, then a neutral steward with an open specification |
| Q18 | When does Phase H start? | Open. Recommended: H0 after ENROLL-1; H1 after SCALE-6; H2 after ECO-3 and an external security review |
| Q19 | Accept the direction of design 0056: tasks that are portable, movable and checkable, and the dogfood rule? | Open. Recommended: yes |
| Q20 | May the spikes R1 to R4 of design 0056 run now, beside Phase E? | Open. Recommended: yes. A spike builds nothing in the product |
| Q21 | Move NET-1, the WebAssembly task type, from stage H0 into the second wave of design 0056? | Open. Recommended: yes, after spike R1, behind a Cargo feature |
| Q22 | Which bet follows the `0.2.0` release? | Open. Recommended: bet A (AX-1, AX-2, AX-3) |
| Q23 | Which format do execution receipts use, and where is the log? | Open. Recommended: an in-toto statement with a SLSA provenance predicate, and a log of our own first |
| Q24 | Does spike R6, GPU for WebAssembly tasks, reopen decision D6? | Open. Recommended: no. D6 stands until R6 has a record |
| Q25 | Is there a lab for the classroom run (EDU-1), and in which term? Accepting it adds a second user story | Open. Owner |
| Q26 | How does the agents' time split between design 0056 and Phases F and G? | Open. Recommended: one lane on Phases F and G, two on the current wave |
| Q27 | Accept the principle of the charter in design 0057: outputs and task code of donated compute are open, later products are free, private clusters are not covered, and the charter binds the owner? | Yes, decided by the owner on 2026-11-10. The charter's text still needs a legal review (CHARTER-1) |
| Q28 | How long may a project keep outputs of donated compute before it publishes them? | At most six months after a batch ends, and no delay for outputs that are checks. Decided by the owner on 2026-11-10 |
| Q29 | Which licences do the outputs and the task code of donated compute use? | CC BY 4.0 for outputs, CC0 allowed; any licence the Open Source Initiative approves for code. Decided by the owner on 2026-11-10 |
| Q30 | Does OxideSwarm aim to replace the operating systems installed on machines? The owner raised it on 2026-11-10 | Open. Recommended: no. It stays a program on every system. The worker image of design 0056 (IMG-1 to IMG-4), which the owner added to the plan the same day, is the part that is built |
| Q31 | Accept design 0058 and its rule: every release ships at least one of its items, and it takes one lane at most? | Open. Recommended: yes |
| Q32 | Which items of design 0058 come first? | Decided by the owner on 2026-11-10 for the first items: LV-10, LV-14, LV-5 and the first targets of LV-1 are in the backlog. The order of the rest is open. Recommended: LV-2 and LV-8 next |
| Q33 | May the repository's history and review records be the data of a published case study (LV-12)? | Open. Owner. Recommended: yes for what is already public in the repository |
| Q34 | Will the owner apply for an independent security review and for funding (LV-6, LV-16)? | Open. Owner |
| Q35 | May user guides be translated into Vietnamese, with English as the source? | Open. Recommended: yes. Writing standard rule 1 then says so |
| Q36 | Accept the first audience of design 0059 (a developer with a coding agent and machines on more than one operating system, then one with a slow test suite) and its order of work? | Open. Recommended: yes |
| Q37 | Will the owner run the first round of the study of first use (ADOPT-1), and with how many people? | Yes, decided by the owner on 2026-11-11: the owner runs it with three to five people, before AX-1 starts. No session has run yet |
| Q38 | Which items of design 0059 enter the backlog first? | Open. Recommended: ADOPT-2 for one task now; ADOPT-3 and ADOPT-4 after the first round of ADOPT-1 |
| Q39 | Keep the rule that the binaries send no usage data, so that numbers about users come only from the study and from public counts? | Open. Recommended: yes |
| Q13 | Which ecosystem comes first: (A) each person or team runs a cluster that trusted people add machines to from any network, or (B) a public grid where strangers run each other's code? | A, decided by the owner on 2026-11-09: ENROLL-1, JOIN-2 and USB-1 at the top of Phase E. B needs VM-level isolation per task, abuse handling and its own design, and waits for real users |

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
| Phases F and G pull effort from the user story before the release | Principle 6: they start after the Phase D exit check. SCALE-1 first, to size the rest from numbers |
| A scale claim without a measurement | SCALE-1 and writing standard rule 6 |

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
- 2026-11-09: Draft added at the owner's request to grow OxideSwarm into a large project: Phase F
  (scale out, SCALE-1 to SCALE-6), Phase G (product and ecosystem, ECO-1 to ECO-8), the ambition in
  the Summary, a note in "What waits", and open decisions Q8 to Q12. The architecture is
  [design 0050](design/0050-scale-out-architecture.md) (Draft). Nothing the owner decided moved.
- 2026-11-09: The owner accepted Q8 to Q12 with their recommended defaults. Phases F and G are
  Accepted; design 0050 stays a Draft until SCALE-1 has numbers.
- 2026-11-09: Phases B and C are complete. FEAT-3 is Implemented with Windows (pull request 152),
  and the live two-machine run of the user story passed (Mac client and master, Windows worker),
  recorded in [guides/offload-cargo-test.md](guides/offload-cargo-test.md#recorded-two-machine-run)
  and as BENCH-1 (c) in `benchmarks/grid/`. Phase D: the Quick start and the `0.1.0` release notes
  are written, and the first tag is `v0.1.0`.
- 2026-11-09: Phase D exit check passed with the released `v0.1.0` binaries on a Mac and a Windows
  laptop (details under Phase D). Phases A to D, the core, are complete.
- 2026-11-09: The owner chose ecosystem model A (Q13) and asked for machines on any network and a
  USB setup kit. ENROLL-1, JOIN-2 and USB-1 (designs 0051 to 0053) head Phase E.
- 2026-11-10: The owner set a long-term goal: an open, worldwide compute network on top of
  OxideSwarm. Phase H (NET-1 to NET-8) is added from [design 0054](design/0054-open-compute-network.md)
  (Draft), with open decisions Q14 to Q18. Nothing in Phases E to G moved.
- 2026-11-10: Draft added at the owner's request for a research and development plan: the frontier
  program of [design 0056](design/0056-frontier-program.md), with bets A to E, spikes R1 to R9,
  lighthouse runs and open decisions Q19 to Q26. A web research run of 2026-11-10 supplied its
  findings. Nothing in Phases E to H moved.
- 2026-11-10: The owner proposed that research done with compute the project mobilises is free and
  public. [Design 0057](design/0057-open-results-charter.md) (Draft) turns this into a charter for
  the public pool of Phase H, with open decisions Q27 to Q29. It builds nothing.
- 2026-11-10: The owner accepted Q27 to Q29 with their recommended defaults: the charter's
  principle, an embargo of at most six months, and CC BY 4.0 for outputs. The charter's text still
  needs a legal review (CHARTER-1), and nothing is built.
- 2026-11-10: The owner asked whether OxideSwarm should become an operating system that replaces
  the installed ones, and then added a bootable worker image to the plan. Design 0056 gained IMG-1
  to IMG-4, spike R10 and lighthouse L6. Q30 is open, and the first non-goal of design 0054
  stands until it is decided.
- 2026-11-10: Draft added at the owner's request for further directions that make the project
  worth remembering: [design 0058](design/0058-lasting-value.md), with items LV-1 to LV-17, spikes
  R11 to R13, proof runs V1 to V4 and open decisions Q31 to Q35. It adds no feature, and nothing
  moved.
- 2026-11-10: The owner put the first items of design 0058 into the backlog (LV-1, LV-5, LV-10 and
  LV-14, Phase 10 of [BACKLOG.md](BACKLOG.md)). Q32 is decided for these.
- 2026-11-10: Draft added at the owner's request on what a user gains and which upgrades make
  people use the project: [design 0059](design/0059-first-users.md), with items ADOPT-1 to
  ADOPT-4, measures M15 to M17 and open decisions Q36 to Q39. Nothing moved, and no item entered
  the backlog.
- 2026-11-11: The owner accepted Q37 with the recommended default: the owner runs the first round
  of the study of first use (ADOPT-1 of [design 0059](design/0059-first-users.md)) with three to
  five people, before AX-1 starts. No session has run yet. Q36, Q38 and Q39 stay open, and no
  item entered the backlog.
- 2026-11-11: [Design 0059](design/0059-first-users.md) gained a screening question before the
  first round of ADOPT-1, from the owner's observation that fellow students have no job that
  waits for a machine. The order of work stays as it is until the answers are in.
- 2026-11-11: [Design 0059](design/0059-first-users.md) names LIVA, the owner's desktop assistant,
  as a first application for the audience with a GPU desktop and a laptop. It needs LLM-1 and
  LLM-6 here and an endpoint setting in LIVA; neither is built, and no order changed.
