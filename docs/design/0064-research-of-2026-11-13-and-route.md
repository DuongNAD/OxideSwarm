# 0064: Research of 2026-11-13 and the route that follows

Status: Draft (plan; builds nothing; Q54 to Q56 are open)
Created: 2026-11-13    Updated: 2026-11-13

## Summary

On 2026-11-13 the owner supplied five research documents and asked for a research record and a
route for OxideSwarm and for Anima Engine. This record is the one for OxideSwarm. It says what
the documents found, which statements were checked, and what changes in the order of work. It
adds two spikes (R20, R21) and one item (SCHED-5), and it changes the text of no existing item.
The record for Anima Engine belongs to that repository.

## Context

### What was read

| Document | Subject | Answers |
|---|---|---|
| A | Scheduling by several resources, GPU sharing, GPU inside a sandbox, neighbouring tools, MCP for long-running work, adoption | A question written for OxideSwarm |
| B | Long-running services, tunnels, identity, checking results, sweeps, credits, reuse, the contract with an application | A question written for OxideSwarm |
| C | Experiment design and determinism for an artificial-life simulation | A question written for Anima Engine |
| D | A world shared over personal machines, and the release of a desktop application | A question written for Anima Engine |
| E | A feasibility study of OxideSwarm and Anima Engine together | Not known. Its questions are read from its text |

All five were read in full. Their authors could not read this repository. Document E says so
and marks every statement about OxideSwarm as unverified.

### Basis of a finding

- `source`: a file of this repository was read.
- `issue`: the title, state and date of an issue were read on GitHub on 2026-11-13.
- `outside`: the statement comes from a research document and was not checked.
- `reading`: a conclusion of a research document, or of this record, from such statements.

### Findings

| # | Finding | Basis | Consequence |
|---|---|---|---|
| 1 | Two coding agents do not yet support the tasks extension of MCP `2026-07-28`: [anthropics/claude-code 102347](https://github.com/anthropics/claude-code/issues/102347) and [openai/codex 48617](https://github.com/openai/codex/issues/48617) are open requests | issue | AX-3 as written serves no client of today. A call that works without the extension comes first (D2) |
| 2 | Document A proposes for that: a call that blocks for a bounded time with progress, then returns a job id and an output tail; a second call that waits; a third that cancels | outside | D2 takes the shape. The length of the wait is not given |
| 3 | The Landlock allowlist of `--isolation` names four character devices: `/dev/null`, `/dev/zero`, `/dev/urandom` and `/dev/random`. It names no GPU device node | source: `crates/worker/src/isolation/linux.rs` | By the code, a task under `--isolation` on Linux cannot open a GPU. Spike R20 confirms it and finds the rules |
| 4 | Reports of CUDA under Landlock and seccomp in another project: [NVIDIA/OpenShell 1486](https://github.com/NVIDIA/OpenShell/issues/1486) (missing file access) and [1696](https://github.com/NVIDIA/OpenShell/issues/1696) (seccomp blocked `memfd_create`). Both are closed as completed. Document A gives another cause for the second, a writable `/proc` | issue; outside | CUDA in such a sandbox is possible. The exact rules are not settled by reading. This repository's filter does not name `memfd_create` |
| 5 | Metal blocked by a sandbox that denies by default: [openai/codex 17644](https://github.com/openai/codex/issues/17644), open. The worker's Seatbelt profile allows by default (design 0063, D6) | issue; source | Not expected to apply here. Not tried: R20 |
| 6 | No run of CUDA or of a native Vulkan driver inside a Windows AppContainer was found, working or failing. Document A calls it a dead end and proposes GPU tasks outside the AppContainer | outside | Unknown, not a dead end. R20 tries it before anything is designed around it |
| 7 | Ways to read free video memory without root: NVML on NVIDIA; files under `/sys/class/drm` for AMD and Intel on Linux; a Metal property on macOS; a DXGI call on Windows whose figures are per process. On unified memory, video memory and RAM are one pool | outside | Input to the design record of FEAT-9. None was tried |
| 8 | Document A proposes: a task states its video memory; one GPU task for each physical GPU unless the task says it can share; no MPS and no time slicing | outside | Agrees with what design 0063 (D6) lists as missing. It is a wire change: Q56 |
| 9 | A second copy of a slow task pays only if the task is safe to repeat and a faster worker is idle. A copy at submission time is a dead end | outside | Design 0060 already plans a second copy for tasks marked as safe to repeat. Unchanged |
| 10 | Document A proposes to lower a worker's slots on battery, under thermal pressure or under memory pressure, with a system call named for each operating system | outside | New item SCHED-5. The kit already sizes slots to the fast cores (USB-4) |
| 11 | Neighbours: self-hosted CI runners, remote build execution, hosted sandboxes for agents, remote development environments. By document A, none serves one developer with two or more machines of different operating systems and a local agent. It sees no advantage as a sandbox on one machine or as a distributed compiler | outside | Agrees with direction 1 of design 0063. It is step 3 of that record's D4, done by a research tool: Q49 still needs the reading checked |
| 12 | No study of the time to a first result or of the loss at each install step was found | outside | ADOPT-1 stays the only source: watch people install |
| 13 | A service on a task grid, the least that serves: the process tree is killed as one, the service reports ready and alive over a local pipe, restarts back off, a time limit covers start and stop | outside | Input to LLM-2. The worker kills process trees today (`process_tree.rs`). The pipe is new |
| 14 | A tick of 60 Hz in lockstep over a relay or a wide-area link is called a dead end. The measurement behind it is one vendor's article on one route | outside | The verdict agrees with design 0038 on round trips. The number is not used. An application of that kind exchanges state in coarse steps (D5) |
| 15 | Identity: a key for each machine, an invite used once, revocation that closes live connections | outside | Design 0051 (ENROLL-1) has all three. Short-lived certificates, which the document adds, are not taken |
| 16 | Checking results: equal checksums only where runs are bit-exact; comparison with a tolerance is unsound for chaotic systems; spot checks with replication that adapts to a worker's record; comparison only inside a class of equal platform | outside | Input to PROOF-1 to PROOF-5. For Anima Engine the class is operating system, architecture and build (finding 20) |
| 17 | Sweeps: one record for a sweep, leases for batches, resume by sweep and seed, a failed input stops while a failed machine reassigns, results gathered as they arrive | outside | Input to AX-1. The master's queue is durable already (FEAT-1) |
| 18 | Credits: a count of checked work units now, no transferable points. Known abuses of volunteer credit are listed | outside | Agrees with the recommended answer to Q14 |
| 19 | Reuse: a BOINC wrapper is called a dead end. Mesh-LLM can run as a supervised service. Wasmtime fits single-threaded CPU tasks from strangers later, and not a GPU kernel | outside | Mesh-LLM: spike R21 and Q54. Wasmtime: NET-1, unchanged |
| 20 | Document E on the two projects together: remote build and test of Anima Engine is a good fit and comes first; sweeps of whole-world runs are a good fit; a real-time world cut into shards is a poor fit, because one desktop is far from full | outside | Agrees with APP-2 and APP-4 of design 0063. Its commands are not usable: finding 21 |
| 21 | Document E writes commands with subcommands and flags that neither program has. `anima-lab` takes `--model` and one manifest path. `submit` has `--require-isolation` and no `--isolation`, which is an option of the worker | source: `src-tauri/src/bin/anima-lab.rs` of Anima Engine, read 2026-11-13; `crates/cli/src/cli_app.rs` | Its four experiments are rewritten before use. APP-4 keeps its own steps |

### Found wrong or doubtful

- Document A calls GPU work in a Windows AppContainer a dead end. It cites no run (finding 6).
- Document A's cause for the CUDA failure differs from the title of the issue it cites
  (finding 4).
- Document A's recipe for Metal assumes a profile that denies by default. The worker's profile
  allows by default (finding 5).
- Document B assumes one checksum for a whole grid. Anima Engine's determinism contract
  promises no equality across operating systems.
- Documents A and B propose parts that exist: join tokens, `doctor`, the kill of a process
  tree, files addressed by BLAKE3, iroh.
- Document E takes a debug measurement with 10 agents as the price of a tick in its estimate
  of sweep times, and its tables cite a source with no address.

### Not checked, and not repeated here

Every figure of latency, loss and throughput; the version numbers of other projects; the
details of the MCP specification beyond finding 1; the system calls named for cores, battery
and heat; the comparison table of eleven neighbouring tools; the survey of credit systems.

## Goals

- **G1** Every finding names its basis, and no figure of a research document is repeated as a
  result.
- **G2** The route orders work that already has an item. A new item appears only where no item
  fits.
- **G3** What is unknown about a GPU inside the sandbox is settled by a run, on each system,
  before FEAT-9 is designed.
- **G4** No part that another open project already provides is built before that project is
  read.

## Non-goals

- A change to the text of AX-1, AX-3, LLM-2, ENROLL-1, FEAT-9, SCHED-4 or PROOF-1 to PROOF-5.
  This record proposes, and the owner decides.
- A real-time world in lockstep through the grid.
- A BOINC wrapper, MPS, time slicing of a GPU, or short-lived certificates.
- A plan for Anima Engine. That repository holds its own.

## Design

### D1 The route

Each step starts when the step before it has a record. Steps 1 and 2 build nothing.

| Step | What | Items | Why now |
|---|---|---|---|
| 1 | Use the grid for real work on two machines: the build and tests of Anima Engine, then its sweeps | APP-2, APP-4 (design 0063) | Findings 20 and 12: the first value is there, and only use shows what is missing |
| 2 | Two spikes: a GPU inside the sandbox on each system, and a reading of Mesh-LLM | R20, R21 | Findings 3 to 6 and 19: both decide the shape of later items |
| 3 | Long-running calls for agents that lack the tasks extension, and sweeps | AX-3 as D2 proposes, AX-1 | Findings 1 and 17 |
| 4 | The whole machine: video memory for a task, GPU work beyond the benchmark, speed, slots that follow battery and heat | FEAT-9, SCHED-4, SCHED-5 | Findings 7 to 10. After R20 |
| 5 | Services for applications | LLM-2, with the first service chosen by Q54 | Findings 13 and 19. After R21 |
| 6 | People who are trusted less: machine keys and checked results | ENROLL-1, PROOF-1 to PROOF-5 | Findings 15 and 16 |
| 7 | State exchanged between machines in coarse steps | LLM-9, and a record of its own | Finding 14. Last, and only if an application needs it |

### D2 A long-running call without the tasks extension (proposal for AX-3)

AX-3 names the tasks extension of MCP `2026-07-28`. Finding 1 says the two agents the owner
uses do not have it. The proposal, from finding 2:

1. `submit` over MCP blocks for a bounded time and sends progress.
2. If the task ends in that time, the call returns the result as today.
3. If not, it returns a job id, the state "running" and the last lines of output.
4. A second tool waits on a job id for another bounded time. A third cancels and kills the
   process tree.
5. When a client declares the tasks extension, the server uses it instead.

The length of the wait, the names of the tools and the size of the tail belong to the design
record of AX-3. Whether this replaces the present text of AX-3 is Q55.

### D3 Spikes

A spike is throwaway work that ends with a dated record and builds nothing in the product.
The numbers continue those of design 0045 (R19).

| ID | Question | Method | Stop rule |
|---|---|---|---|
| R20 | Does a task under `--isolation fs` reach the GPU, on each system the owner has? | On macOS, Windows and Linux: one small program that opens the GPU and computes (Metal, CUDA or Vulkan as the machine has), run as a task with isolation off and then on. Where it fails with isolation on, the denied call is read from the system's log, and the smallest rule that lets it through is noted. Nothing is changed in the product. The record holds, for each system, the GPU, the driver, the result and the rule | If no small rule exists on a system, FEAT-9 says for that system that GPU tasks run only on workers without isolation, and the record of FEAT-9 states the risk |
| R21 | What does Mesh-LLM already do of what designs 0038, 0061 and 0062 plan? | Its documents and its code are read for: how a model reaches a node, whether a node needs the file, how a request is routed, how a model is split, what authenticates a node, which systems it runs on. It is installed on the owner's two machines and serves one model. The record holds a table: planned item, what Mesh-LLM does, what is left | If it covers an item, that item is closed in favour of running it as a service (Q54) |

### D4 SCHED-5: slots that follow the machine

A worker lowers the number of tasks it runs at once while it is on battery, while the system
reports thermal pressure, or while memory pressure is high, and raises it again afterwards. It
reports the present number with its heartbeat, as `slots:` does today. Finding 10 names the
calls. The item needs a design record of its own, because it changes what a worker reports
and when.

### D5 What an application of the kind of Anima Engine can ask of the grid

From findings 13, 14, 16 and 17, in the order in which the grid can give it:

1. **Today:** one task for each seed, files in and out, placement by operating system, and a
   checksum compared by the submitter. No change is needed (APP-4).
2. **With AX-1:** a sweep as one call, with resume.
3. **With LLM-2:** a process that keeps running, reports ready and alive, and is stopped in
   order.
4. **With LLM-9 and a record of its own:** a channel between two such processes, for state
   that is exchanged every few seconds or less often. Not for every tick.

Whether a shared world uses this channel or a transport of its own inside the application is
that application's decision.

## Compatibility

Nothing changes. D2 would add tools to the MCP server and keep the present ones. Q56 would add
a field to a task, which is a change of the wire format and follows design 0040.

## Security

- R20 runs with isolation off for its baseline, on the owner's machines, with a program the
  owner built.
- A rule that R20 finds is a wider sandbox. Each one is listed in the record with what it
  opens, and none enters the product without a design record.
- R21 installs another project's program. It runs on the owner's two machines, outside the
  grid, and its access control is read before it listens beyond loopback.
- Finding 16 stands against comparing results across platforms by equality: a check that
  raises false alarms teaches people to ignore it.

## Test plan

No code. R20 and R21 end with dated records under [benchmarks/](../../benchmarks/), with the
script or the commands of each run.

## Work items

Sizes are the backlog's S, M and L. No item is in [BACKLOG.md](../BACKLOG.md).

| ID | Item | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|
| R20 | Spike: a GPU inside the sandbox on each system (D3) | S for each system | A machine of each system with a GPU | `benchmarks/` | The record holds, for each system, the result with isolation off and on, and the rule where one was found | Planned |
| R21 | Spike: what Mesh-LLM already does (D3) | S | The owner's two machines | `benchmarks/`, this record | The table of D3 is in this record's status log | Planned |
| SCHED-5 | A worker lowers its slots on battery, heat or memory pressure (D4) | M | A design record | `crates/worker/src/` | A worker on battery reports fewer slots and takes fewer tasks, and a test shows both | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision. They are listed in
[ROADMAP.md](../ROADMAP.md) as Q54 to Q56.

| ID | Question | Recommended default |
|---|---|---|
| Q54 | Do the model items (LLM-1, LLM-6, LLM-12, LLM-13) wait for spike R21, and close where Mesh-LLM already does the work? | Yes. No model item is built before R21 has a record |
| Q55 | Does AX-3 get a long-running call that works without the tasks extension (D2), with the extension used when a client has it? | Yes |
| Q56 | Does a task get a field for the video memory it needs, as part of FEAT-9? | Yes, after R20. It is a wire change and gets a design record |

## Risks

| Risk | Answer |
|---|---|
| A research document is taken as a result | Every finding names its basis, and the figures are left out (G1) |
| The route grows to seven steps of building | Steps 1 and 2 build nothing, and each later step waits for the record before it |
| Mesh-LLM changes or stops | R21 records its version and licence. A closed item can be opened again |
| A GPU rule for the sandbox opens too much | Security: each rule is listed with what it opens |
| The agents gain the tasks extension soon | D2 uses it when it is there, so the work is not lost |

## Records

None yet.

## Status log

- 2026-11-13: Written at the owner's request from five research documents. The statements
  marked `issue` and `source` were checked that day. Nothing is built or measured, and no item
  is in the backlog.
