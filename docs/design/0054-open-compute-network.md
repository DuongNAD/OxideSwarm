# 0054: An open compute network (long-term vision)

Status: Draft (vision; the owner's long-term goal of 2026-11-10; decisions Q14 to Q18 are open)
Created: 2026-11-10    Updated: 2026-11-10

## Summary

The long-term goal is to grow OxideSwarm from one owner's machines into an open network. In it,
anyone can offer idle compute and anyone can use it, safely, with results that can be checked. It
would act as an operating system for a planet-sized cluster: identity, scheduling, storage and
networking across machines that their owners keep running as before. It does not replace Windows,
macOS or Linux. This record states the problems that stand in the way and a staged plan. Each stage
is useful alone, and none needs the next one to be worth building. It builds nothing yet. Phase H
of `docs/ROADMAP.md` carries the plan.

## Context

- **What volunteer computing has shown.** It works for work that splits into independent pieces.
  Folding@home and BOINC have run science projects on volunteers' machines for two decades
  ([Folding@home](https://foldingathome.org), [BOINC](https://boinc.berkeley.edu)). Recent projects
  have trained language models over the Internet across continents, for example Prime Intellect's
  INTELLECT-1 in 2024, with methods that tolerate slow links.
- **Where they stopped.** None of them is general purpose. Each runs its own workload, built for
  untrusted hosts.
- **What OxideSwarm already has.** Today it runs one owner's cluster (decision Q13, model A).
  It already has the parts of a cluster operating system for trusted machines:
  - a scheduler with placement and a durable queue (FEAT-1);
  - content-addressed storage (design 0006);
  - P2P networking through NATs (iroh);
  - task isolation on three operating systems (design 0033);
  - per-machine keys and invites, being built (ENROLL-1, design 0051).
- **What Phase F and Q13 already plan.**
  - Phase F (design 0050) plans larger clusters and federation between sites (SCALE-6).
  - Q13 deferred model B, a public grid where strangers run each other's code, until there are
    real users and a design. This record is that design's starting point.

## Goals

- **G1 Safe for contributors.** A machine that offers compute cannot be harmed, spied on or misused
  by the work it runs: no access to the owner's files, no network unless allowed, and bounded CPU,
  memory, disk, time and battery. The owner can stop it at any moment.
- **G2 Trustworthy for users.** A user can tell whether a result is correct, even if some
  contributors cheat or are broken.
- **G3 Open.** The protocol is documented and versioned, so other implementations can exist. No
  single company or server is required to run the network. Clusters can join and leave it.
- **G4 Fair.** Contributing earns the right to use. Abuse (mining for profit, attacks, illegal
  content) is refused by design and by policy.
- **G5 Useful at every stage.** Each stage of the plan helps real users before the next one starts.

## Non-goals

- Replacing desktop operating systems. "Operating system" here means the layer that runs a fleet
  of machines, as a cluster scheduler does for a data centre.
- Tightly coupled work over the Internet, such as classic large-model training that needs a fast
  interconnect. It stays on local clusters unless methods that tolerate slow links fit the case.
- Confidentiality of data on untrusted machines in the first stages. Work in the public pool must
  treat its inputs and outputs as visible to the contributor, until trusted execution environments
  (D3) exist.
- A cryptocurrency. Q14 decides how contribution is counted. The default is plain credits.

## Design

### D1 Identity

Every person and every machine has a key pair. ENROLL-1 already does this for machines inside a
cluster. A cluster vouches for its machines, and the network vouches for clusters by their history,
with no central authority: public keys are the names, as iroh node ids already are. Reputation
(D4) is kept per key.

### D2 Sandboxes for strangers

Today's `--isolation fs` limits files, not intent. Work from strangers needs a stronger box:

- **WebAssembly first** (WASI, through a runtime such as `wasmtime`).
  - One task binary runs on every OS and architecture.
  - Memory, CPU fuel, files and network are granted explicitly.
  - Execution can be deterministic, which makes D4 cheap.
- **Lightweight virtual machines next,** for native code: Firecracker or Cloud Hypervisor on Linux,
  Virtualization.framework on macOS, Hyper-V on Windows. Each task gets a throw-away VM with no host
  files and a metered network.
- **No network by default.** A task that needs the network names the hosts it may reach, and a
  contributor can refuse that class of task.

### D3 Trusted hardware (later)

Some CPUs and GPUs offer trusted execution environments: AMD SEV-SNP, Intel TDX, Apple Secure
Enclave for keys, NVIDIA confidential computing. They can prove what code ran and keep data from
the host. They are optional and come later, for users who need confidentiality.

### D4 Verifying results

- **Redundancy.** Run a task on k independent contributors and accept the result when they agree.
  Deterministic WebAssembly gives byte-identical outputs, so comparing hashes is enough.
- **Spot checks.** Send tasks whose answers are known, unannounced. A wrong answer costs
  reputation.
- **Reputation.** A per-key history of agreement and spot-check results lowers k for reliable
  contributors and raises it for new ones. It costs nothing to create a key, so new keys start with
  low trust.
- **Domain checks.** Some results are cheap to check without recomputing (a solution, a hash, a
  test verdict that can be re-run). The task type may declare such a check.

### D5 From one master to a network

1. **Federation (SCALE-6).** Clusters lend idle workers to each other under explicit agreements.
2. **Directory.** Clusters publish offers (capacity, task classes, sandboxes, prices in credits) in
   a distributed directory: gossip or a DHT over iroh, with no central server.
3. **Scheduling across the wide area.** It places work near its data, takes link quality into
   account (LINK-1 measures it), and prefers batches that fit one machine.

### D6 Fair exchange

- **Credits.** A contributor earns credits for verified work and spends them to run work, and a
  cluster can grant credits to its own users. Both sides sign each record of work, so the
  accounting can be checked.
- **Payments later.** Money or tokens would bring payments, fraud and law in with them. Q14
  decides; the default is credits only.

### D7 Safety and governance

- **Contributors stay in control.** Contributing is opt-in per machine, with caps (CPU share,
  hours, battery, metered networks off) and a local switch that stops work at once.
- **Task classes and policy.** Tasks are allowed by class. Mining, scanning and denial of service
  are refused, and repeat offenders are blocked by key.
- **Transparency.** Reports say what ran, how much and for whom.
- **Open results.** A project that receives donated compute publishes its outputs and its task
  code. The draft charter is [design 0057](0057-open-results-charter.md).
- **Governance.** An open specification, a public security process and, when the network has
  outside users, a neutral steward (Q17).

## Stages

| Stage | What it adds | Useful because | Exit check |
|---|---|---|---|
| H0 | A WebAssembly task type (`TaskSpec::Wasm`) in today's trusted clusters | One task binary runs on every OS; a first sandbox | The same `.wasm` task gives byte-identical output on macOS, Windows and Linux workers |
| H1 | Federation between trusted clusters, with credit accounting | Friends and teams pool idle machines | Cluster A runs a task on a worker that cluster B lends it, and both credit logs agree |
| H2 | Public pool alpha: WebAssembly only, redundancy and spot checks, reputation, opt-in contributors, invited users | First open network, safe by construction | A planted cheating contributor is caught, and its results are never accepted |
| H3 | Native tasks in lightweight VMs; public beta; open contributor sign-up | Real workloads such as builds, rendering and simulations | A VM task cannot read host files or reach the network beyond its grant (attack tests) |
| H4 | Distributed directory and scheduler; open specification; steward | No single point of control | Two independent implementations interoperate on the specification's test suite |

The first workloads come from Q16. The recommended ones are open-source CI builds and tests,
Monte Carlo and parameter sweeps, and rendering. They split well and can be checked.

## Compatibility

Nothing changes until H0. The cluster protocol stays the trusted-cluster protocol. The network
layer (H1 onwards) is a separate, versioned protocol between clusters, so a cluster that never
joins the network is unaffected.

## Security

The threat model widens with each stage:

| Stages | Who may be hostile |
|---|---|
| Up to H1 | Nobody: clusters trust each other by agreement |
| H2 | The tasks and some contributors |
| H3 and H4 | Anyone on the Internet |

Every stage starts with its own threat model and an independent review, as ENROLL-1 did. The
contributor's safety (G1) outranks every other goal: when in doubt, a task does not run.

## Test plan

Each stage's exit check above, plus adversarial tests:
- escapes from the sandbox;
- cheating contributors;
- forged credit records;
- abuse task classes.

Fuzzing covers every parser of network input.

## Work items

| ID | Item | Stage | Status |
|---|---|---|---|
| NET-1 | WebAssembly task type with resource limits and deterministic mode | H0 | Planned |
| NET-2 | Federation protocol and signed credit accounting between clusters | H1 | Planned (builds on SCALE-6) |
| NET-3 | Result verification: redundancy, spot checks, reputation per key | H2 | Planned |
| NET-4 | Contributor controls: opt-in, caps, local stop switch, task-class policy | H2 | Planned |
| NET-5 | Lightweight VM sandboxes on Linux, macOS and Windows | H3 | Planned |
| NET-6 | Distributed directory of offers over iroh | H4 | Planned |
| NET-7 | Open protocol specification, conformance tests, security process, steward | H4 | Planned |
| NET-8 | Trusted execution environments for confidential tasks | after H3 | Planned |

## Status log

- 2026-11-10: Draft. The owner set the long-term goal of an open, worldwide compute network on top
  of OxideSwarm. Phase H in `docs/ROADMAP.md`; decisions Q14 to Q18 are open.
- 2026-11-10: [Design 0056](0056-frontier-program.md) proposes to build NET-1 earlier, inside
  trusted clusters (Q21), and names independent rebuilds (PROOF-5) as a first public workload for
  Q16. Its research found Wasmtime at tier 3 on Android, and no measurement of bit-identical
  results across CPU architectures. The exit check of H0 therefore waits for spike R1. Nothing in
  this record changed.
- 2026-11-10: D7 gained "Open results", from the owner's proposal of the same day. The draft
  charter is [design 0057](0057-open-results-charter.md). The owner accepted its principle the
  same day (Q27 to Q29).
- 2026-11-10: The owner asked whether OxideSwarm should replace installed operating systems. The
  first non-goal of this record stands until Q30 is decided. Design 0056 adds a worker image that
  starts a whole machine without touching its installed system (IMG-1 to IMG-4).
