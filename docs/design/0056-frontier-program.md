# 0056: Frontier program (bets after the core)

Status: Draft (research and plan; builds nothing; decisions Q19 to Q26 and Q30 are open)
Created: 2026-11-10    Updated: 2026-11-11

## Summary

The roadmap says how OxideSwarm grows: more machines (Phase E), more scale (Phase F), more tools
around it (Phase G) and, in the end, an open network (Phase H). It does not yet say what a person
outside the project would switch to it for. This record answers that from a research run of
2026-11-10.

- **Direction.** A person or a coding agent runs a workload on spare machines, their own first and
  other people's later. The machine is safe from the task, and the result can be checked.
- **Three properties** carry the direction. A task runs on any machine (portable). It outlives the
  machine it started on (movable). It leaves evidence of what it did (checkable).
- **Bets.** Each bet builds one part of that:
  - A: compute shaped for coding agents;
  - B: portable and movable tasks on WebAssembly;
  - C: signed receipts, quorum runs and independent rebuilds;
  - D: language models served by ordinary machines, with a classroom as the first site;
  - E: the commons, which is Phase H with a first workload, rules for a good guest, and a worker
    image that starts a whole machine.
- **Lighthouse runs.** Each bet ends in one recorded run on real machines that a stranger can
  retell in one sentence (D7).
- **Order.** Bet A comes first, because it uses what is shipped and serves the user story of the
  roadmap. Every bet starts with a short spike that can end it (D9).

Nothing in this record is built. Its items enter [BACKLOG.md](../BACKLOG.md) only when the owner
accepts the bet they belong to. Nothing that the owner already accepted moves.

## Context

### Where the project stands

`v0.1.1` is released. Phases A to D of [ROADMAP.md](../ROADMAP.md) are complete: a coding agent on
one machine runs `cargo test` on another machine of the same owner through `oxideswarm mcp`, in a
persistent workspace, under OS-level isolation. Phase E is in progress with ENROLL-1, JOIN-2 and
USB-1. Phases F, G and H are planned in designs 0050 and 0054.

The parts this record builds on:

| Part | Code |
|---|---|
| MCP server with the tools `status`, `workers`, `submit`, `task_result`, `cancel` and `doctor`. It follows MCP `2024-11-05` | `crates/cli/src/mcp.rs` (`MCP_PROTOCOL_VERSION`) |
| Persistent named workspaces with tree sync, one task per workspace | `crates/worker/src/workspace.rs`, `crates/core/src/task.rs` (`WorkspaceRef`) |
| Placement by `require:<os\|arch\|worker\|iso\|tag\|tool>` | `crates/core/src/placement.rs` |
| Content-addressed blobs (BLAKE3) and tree manifests | `crates/core/src/blob.rs`, `crates/core/src/blob_transfer.rs` |
| Checkpoint messages and reassignment with a checkpoint | `crates/core/src/protocol.rs` (`WorkerMessage::Checkpoint`, `MasterMessage::AssignTaskWithCheckpoint`) |
| Task isolation on macOS, Linux and Windows | `crates/worker/src/isolation.rs` |
| Task journal and replay | `crates/master/src/store.rs` |
| Release workflow with a pinned toolchain | `.github/workflows/release.yml` |
| Benchmark records with script and date | `benchmarks/grid/` |

Task types today are `Command`, `ShellScript`, `RustCompilation`, `GpuCompute`, `BuiltinTest`,
`Generative3D` and `AutoRig` (`TaskSpec` in `crates/core/src/task.rs`). A task's output reaches the
client only when the task ends: `WorkerMessage::TaskProgress` carries a status and no output.

### What the research of 2026-11-10 found

Four research runs searched the web on 2026-11-10. The "Basis" column says how each row was
checked: **page** means the research run reported reading the primary page, **summary** means the
claim comes from search results or a secondary article. A summary row must be checked at its
primary page before `README.md` or a launch text quotes it (writing standard rule 6).

Compute for coding agents:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| Hosted agent sandboxes bill about $0.04 to $0.14 per vCPU-hour and offer Linux guests. Windows desktops are rare, macOS is invite-only preview at one vendor, and none lists real phones or real GPUs | [Fly comparison](https://fly.io/learn/ai-sandbox-pricing.md) (a vendor's page), [Cua](https://cua.ai/blog/cloud-windows-ga-macos-preview) | summary | Mixed operating systems and real hardware are where owned machines differ. Bet A |
| Agent vendors now let the agent's commands run on the customer's machines: Claude Code `self-hosted-runner` (2026-08, Linux and macOS hosts, one user per runner), Cursor self-hosted cloud agents (2026-03, Linux and Mac workers), Copilot through Actions runners | [Claude Code runner](https://startdebugging.net/2026/08/claude-code-self-hosted-runner-cloud-sessions-on-your-own-hosts/), [Cursor](https://www.developer-tech.com/news/cursor-self-hosted-cloud-agents/), [Copilot](https://github.blog/changelog/2026-04-03-organization-runner-controls-for-copilot-cloud-agent/) | summary | The demand is confirmed. Each option serves one vendor's agent. OxideSwarm serves any agent through MCP and can host these runners as tasks (AX-6) |
| GitHub-hosted runners cost $0.006 per minute for Linux (2 cores), $0.010 for Windows and $0.062 for macOS. A charge of $0.002 per minute on self-hosted runners was announced for 2026-03-01 and postponed | [Runner pricing](https://docs.github.com/en/billing/reference/actions-runner-pricing), [changelog](https://github.blog/changelog/2025-12-16-coming-soon-simpler-pricing-and-a-better-experience-for-github-actions/) | page | A Mac the owner has replaces the most expensive minutes. AX-7 |
| `cargo nextest archive` packs test binaries without source. `cargo nextest run --archive-file <file> --workspace-remap <dir> --partition slice:<m>/<n>` runs one slice on a machine without `cargo`. The archive needs the same OS, architecture and nextest version, and the source at the same revision | [Archiving](https://nexte.st/docs/ci-features/archiving/), [Partitioning](https://nexte.st/docs/ci-features/partitioning/) | page | Build once, test on every machine. Workspace sync supplies the source. AX-2 |
| Real devices cost $0.17 per device minute at AWS Device Farm and $5 per device hour at Firebase Test Lab. OpenSTF, the open-source device farm, had its last push in 2023 | [AWS](https://aws.amazon.com/device-farm/pricing), [Firebase](https://firebase.google.com/docs/test-lab/usage-quotas-pricing), [article](https://dev.to/omnarayan/openstf-is-dead-the-best-alternative-for-mobile-device-labs-in-2025-2j5o) | page (prices), summary (OpenSTF) | A phone on a worker's USB port is a test target nobody else gives an agent cheaply. AX-5 |
| MCP `2026-07-28` makes requests stateless and moves long-running work to the extension `io.modelcontextprotocol/tasks`, polled with `tasks/get` | [MCP changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog) | page | `oxideswarm mcp` follows `2024-11-05` and blocks until a wait deadline. AX-3 |

Sandboxes:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| Wasmtime supports `x86_64` Windows and Linux at tier 1 and `aarch64` macOS at tier 2. Android and iOS on `aarch64` are tier 3. Pulley, its interpreter, is tier 2 | [Stability tiers](https://docs.wasmtime.dev/stability-tiers.html) | page | A WebAssembly task type can ship on the three desktop systems. Android is open (R1) |
| WASI 0.3 with async was ratified on 2026-06-11, and Wasmtime 46 turns it on by default. Every twelfth Wasmtime release gets security fixes for 24 months | [WASI 0.3](https://wasi.dev/releases/wasi-p3), [LTS](https://bytecodealliance.org/articles/wasmtime-lts) | summary | NET-1 pins an LTS release |
| One 2026 benchmark of a cryptography library puts Wasmtime at 2.41 times native time without SIMD and 1.46 times with it | [Benchmark](https://00f.net/2026/06/23/webassembly-runtimes-2026/) | summary | The cost is real and workload-dependent. R1 measures it on our machines before any claim |
| Wasmtime has switches for canonical NaN values and deterministic relaxed SIMD. No source promises or measures bit-identical results across `x86_64` and `aarch64` | [Deterministic execution](https://docs.wasmtime.dev/examples-deterministic-wasm-execution.html) | summary | The exit check of stage H0 in design 0054 is unproven. R1 measures it |
| Live migration of a running WebAssembly instance across machines is research: an ahead-of-time compiler with cross-architecture checkpoints (2025), and checkpoints between interpreters only (2024) | [Wanco](https://popl25.sigplan.org/details/POPL-2025-student-research-competition/25/Wanco-WebAssembly-AOT-Compiler-that-supports-Live-Migration), [poster](https://apsys2024.github.io/posters/apsys24posters-final73.pdf) | summary | Bet B starts with checkpoints the task takes itself. Transparent snapshots stay a spike (R5) |
| `wasi:webgpu` is a phase 2 proposal with no upstream Wasmtime support. No source treats shader validation outside a browser as a security boundary, and it had a memory-safety defect in 2024 (CVE-2024-36761) | [wasi-gfx](https://github.com/WebAssembly/wasi-gfx) | summary | GPU code from strangers is research only. R6 |
| CPython supports WASI at its tier 2 | CPython `Platforms/WASI` | summary | A Python task can run inside the WebAssembly sandbox. WASM-2 |
| Small virtual machines start from user programs on current systems: Hyperlight on Windows, Apple's `container` on macOS 26, libkrun on Linux and macOS | [Apple container](https://github.com/apple/container) | summary | Candidates for NET-5. Whether each starts without administrator rights is not checked |

Checkable results:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| The xz backdoor (2024-03) was in the release archives and not in the git repository | [Qualys](https://blog.qualys.com/vulnerabilities-threat-research/2024/03/29/xz-utils-sshd-backdoor) | summary | A rebuild from the repository on an unrelated machine gives a different hash. Bet C |
| The compromise of `tj-actions/changed-files` (2025-03) and the npm worm of 2025-09 used stolen credentials and changed what the source itself does | [CISA](https://www.cisa.gov/news-events/alerts/2025/03/18/supply-chain-compromise-third-party-github-action-cve-2025-30066) | summary | A rebuild does not catch this class. Bet C does not claim it |
| Google OSS Rebuild (2025-07-21) rebuilds popular PyPI, npm and crates.io packages and publishes SLSA provenance. It is one operator. Debian's reproducible builds team lists independent rebuilders as a goal | [OSS Rebuild](https://github.com/google/oss-rebuild), [Debian notes](https://lists.reproducible-builds.org/pipermail/rb-commits/2025-February/012422.html) | summary | Rebuilders run by unrelated owners, also on Windows and macOS, are the gap. PROOF-5 uses the existing attestation formats |
| The Go toolchain is bit-for-bit reproducible since 1.21. An independent party reproduced every toolchain since then. macOS needs the signature stripped before comparing | [Go blog](https://go.dev/blog/rebuild), [Source Spotter](https://www.agwa.name/blog/post/verifying_go_reproducible_builds) | summary | The method for PROOF-3 |
| Rekor v2, the Sigstore transparency log, is generally available since 2025-10-10 and serves its log as static tiles | [Sigstore](https://blog.sigstore.dev/rekor-v2-ga/) | summary | PROOF-4 uses the same tile layout |
| BOINC lowers replication for hosts with a history of validated results, and reports an overhead of a few percent | [BOINC paper](https://arxiv.org/pdf/1903.01699) | summary | The model for NET-3 |
| Proving general computation costs about six orders of magnitude over running it. A memory interposer under $1000 extracted attestation keys from SGX, TDX and SEV-SNP in 2025-10. No consumer device was found that attests a confidential environment for arbitrary code | [Jolt](https://www.a16zcrypto.com/posts/article/building-jolt), [TEE.fail report](https://thehackernews.com/2025/10/new-teefail-side-channel-attack.html) | summary | The program checks results by agreement between machines, not by proofs or trusted hardware |

Models on ordinary machines:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| Over Wi-Fi, a 70B model split across four mixed home devices produced about 1.5 tokens per second. In the same paper the split was slower than plain llama.cpp at 45B | [prima.cpp](https://arxiv.org/html/2504.08791v2) | page | Splitting one model over an ordinary network is slow. It is not the headline of bet D |
| The fast multi-machine results, about 30 tokens per second on a 235B model, use four high-end Macs linked by Thunderbolt 5 with RDMA. llama.cpp RPC lost speed as nodes were added | [Geerling](https://jeffgeerling.com/blog/2025/15-tb-vram-on-mac-studio-rdma-over-thunderbolt-5) | page | That hardware is not a school lab |
| llama.cpp takes a draft model for speculative decoding only from a local file | [Issue 23982](https://github.com/ggml-org/llama.cpp/issues/23982) | summary | No draft model on a second machine |
| In an older study of classroom labs the CPUs were idle 97.9 % of the time, and N shared machines did the work of about N/2 dedicated ones | [Study](https://old.cisuc.uc.pt/publication/show/1105) | summary | Lab capacity is plausible. R4 measures it on current machines |
| Vietnam pilots an AI curriculum for grades 1 to 12 from the 2025 to 2026 school year. In a 2025 survey 76 % of about 35,000 teachers had used AI | [VnExpress](https://e.vnexpress.net/news/tech/vietnam-innovation/vietnam-to-pilot-ai-teaching-across-grades-1-12-5007313.html) | summary | Schools need AI tools and have labs. Lighthouse L4 |
| Africa and Latin America together hold 3 % of global AI compute | [Brookings](https://www.brookings.edu/articles/how-to-bridge-the-global-ai-divide/) | summary | The audience of goal G5 |

Volunteer networks and energy:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| Compute networks that pay in tokens report revenue figures that disagree with on-chain figures by a wide margin | [Overview](https://blockeden.xyz/blog/2026/03/12/depin-compute-revenue-pivot-akash-ionet-aethir/) | summary | Supply without a workload people need does not make a network. Bet E starts from one workload and uses no token (default of Q14) |
| Simulations of shifting batch jobs in time and place report 18 % to 54 % lower emissions, depending on how long a job may wait | [CI jobs](https://arxiv.org/pdf/2310.18718), [DAG workloads](https://arxiv.org/html/2512.07799v1) | summary | Deferrable tasks are worth a flag. GREEN-2 |

Starting a worker image from USB or the network:

| Finding | Source | Basis | Consequence |
|---|---|---|---|
| A PC with Secure Boot starts a Linux system only through a boot loader signed under Microsoft's third-party certificate. The 2011 certificate expired on 2026-06-27. Loaders signed under it still start, new loaders carry the 2023 signature, and a machine whose firmware lacks the 2023 certificate refuses them | [Google Cloud](https://docs.cloud.google.com/compute/docs/security/ms-secure-boot-certificates-expiration), [AlmaLinux](https://almalinux.org/blog/2026-07-14-secure-boot-2023-certificates/) | summary | The image uses a distribution's signed boot chain. Whether it starts depends on each machine's firmware. R10 |
| Secured-core PCs ship with the third-party certificate turned off since 2022. A firmware setting turns it on | [The Register](https://www.theregister.com/2022/07/11/lenovo_secured_core/) | summary | Some PCs refuse the image until an administrator changes one setting. R10 counts them |
| BitLocker asks for its recovery key when the Secure Boot configuration that the TPM measures changes. No source was found on a single start from USB | [Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/5907570/fve-secure-boot-changed) | summary | No guide may tell a user to turn Secure Boot off. R10 tests a machine with BitLocker |

### What the research rules out

These ideas sound large and do not hold up. The program leaves them out:

- **One large model split over a lab's network as the main feature.** It works and it is slow.
  LLM-2 to LLM-5 of design 0038 keep their place for models that fit no single machine.
- **Cryptographic proofs of general computation.** The cost is out of reach for volunteers.
- **Trusted hardware on contributors' machines.** It does not exist for arbitrary code on consumer
  devices, and the server versions were broken by physical access in 2025. NET-8 stays last.
- **Transparent migration of any running process as a first step.** It is research. Cooperative
  checkpoints come first.
- **GPU code from strangers through WebGPU.** No one claims that boundary outside a browser.
- **A token.** Q14 keeps plain credits.

### What the existing plan lacks

- The exit checks of Phases E to H prove capabilities. None is a run a stranger would retell.
- The agent interface is one blocking `submit` to one worker. An agent cannot ask for several
  operating systems at once, split a test suite, see output before the end, or try two changes in
  parallel.
- Phase H has no first workload (Q16 is open). Its sandbox (NET-1) and its verification (NET-3) are
  both useful inside trusted clusters long before a public pool, and the plan schedules them last.
- Design 0038 puts most of its work into splitting one model (LLM-2 to LLM-5). The research favours
  one small model per machine with requests spread across machines, which today is only LLM-1, a
  guide.

## Goals

- **G1 Shaped for agents.** One call from a coding agent verifies a change on every operating
  system and device the owner has. Time to verdict falls as machines are added.
- **G2 Portable.** One task artefact runs unchanged on macOS, Windows and Linux workers, and on
  Android when R1 allows it.
- **G3 Movable.** A task that loses its machine continues on another from its last checkpoint.
- **G4 Checkable.** Every finished task has a signed receipt. Independent machines can confirm a
  result, and a mismatch is reported with both digests.
- **G5 Reachable.** A school lab without a cloud budget and without Internet access gets a working
  cluster with a local model from a USB stick.
- **G6 Dogfood.** Each bet first serves OxideSwarm's own development, and its record comes from
  that use.

## Non-goals

- An inference engine, a WebAssembly runtime or a build system of our own. The grid places, moves,
  isolates and records. Existing engines do the compute.
- A lower cost per unit of compute than a data centre. The case is machines already owned, coverage
  of operating systems and devices, and data that stays local.
- Confidential computing on strangers' machines.
- Payments.
- A change to the order of ENROLL-1, JOIN-2 and USB-1, or to any accepted decision.

## Design

### D1 Properties, the dogfood rule and the life of a bet

The three properties are the test for new work in this program: an item belongs if it makes tasks
more portable, more movable or more checkable, or if it lets an agent or a classroom use one of
the three. Anything else belongs to Phases E to G.

Every bet moves through the same steps:

1. **Spike.** A time-boxed experiment (D9) that ends with a dated record and a go or a stop.
2. **Design record.** The bet's first item that changes behaviour, a CLI flag or the wire format
   gets a record of its own, as the documentation standard requires.
3. **Build.** The items enter `BACKLOG.md` with files and a "Done when".
4. **Lighthouse run.** A recorded run on real machines (D7).
5. **Default.** The feature is on by default only after its record exists.

A bet whose spike meets its stop rule is closed in the status log. It is not reshaped to survive.

The dogfood rule (G6): the first user of each item is this repository. Its test suite, its CI and
its release are the workloads of the first records.

### D2 Bet A: compute shaped for coding agents

**Why.** An agent's loop is edit, then verify. The verify step sets its pace. Hosted sandboxes give
agents Linux machines by the second. They do not give the owner's other operating systems, GPU or
phones, and each vendor's self-hosted runner serves that vendor's agent only.

**What exists.** `oxideswarm mcp`, workspaces, placement and isolation. One call runs one command
on one worker and returns when it ends.

**Parts.**

- **A1 Matrix fan-out (AX-1).**
  - `submit --matrix os=macos,windows,linux -- <command>` submits one task per value with the
    matching `require:` entry. `--matrix` may repeat for a product, up to a fixed cell limit.
  - With `--workspace`, the tree syncs to each chosen worker. Blobs the master already holds are
    not uploaded again.
  - The client waits for every cell. `--json` prints one record per cell: cell, worker, exit code,
    duration and the head and tail of the output. The exit code is non-zero when any cell fails.
  - A cell no worker matches is reported as unplaced after a check against `ListWorkers`. It does
    not hang.
  - The MCP tool `submit` takes the same `matrix` field and returns the array.
  - The expansion happens in the client, so the wire format does not change.
- **A2 Sharded runs (AX-2).**
  - `oxideswarm shard --count <n> -- <command>` submits n tasks. Each gets `OX_SHARD_INDEX` and
    `OX_SHARD_TOTAL` in its environment, and the client spreads them over the matching workers by
    their `slots:` tags.
  - A guide gives the recipe per test runner: `cargo nextest run --partition
    slice:<index>/<total>`, and the shard options of `pytest`, `jest` and `go test`.
  - Build-once mode for Rust: one task per operating system and architecture runs `cargo nextest
    archive` in the workspace and returns the archive as an output blob. The shard tasks take it as
    an input and run with `--archive-file` and `--workspace-remap`. A `tool:cargo-nextest` tag
    carries the version, because the archive needs the same one.
  - The client merges the verdicts and the JUnit files.
- **A3 Live output and MCP tasks (AX-3).**
  - Wire: a worker sends output chunks while a task runs, and the master forwards them to waiting
    clients from a bounded buffer per task. This adds message variants and a
    `ProtocolCapabilities` bit, and raises the protocol version. The final result keeps the head
    and tail rule of design 0022.
  - CLI: `submit --wait --follow` and `oxideswarm logs <task>`.
  - MCP: the server speaks `2026-07-28` and the tasks extension, so a long build is a task the
    agent polls for state and an output tail. It keeps answering `2024-11-05` clients.
  - An agent can then cancel a run at the first failing test.
- **A4 Workspace forks and speculative runs (AX-4).**
  - `submit --workspace <base> --fork <name>` clones the base workspace on its worker and runs in
    the clone. The clone is copy-on-write where the file system offers it (`clonefile` on APFS,
    `FICLONE` on Linux file systems with reflinks, block cloning on ReFS). Elsewhere it is a copy
    bounded by `--workspace-limit-mb`.
  - A fork starts with the base's warm `target/`. The tree sync then brings only the files that
    differ, for example from another git worktree of the agent.
  - Forks run concurrently with each other. They count toward `--max-workspaces` and are removed
    after the task unless `--keep` is given.
  - `--first-success` on a set of forks cancels the rest when one passes. An agent tries several
    candidate changes at once and keeps the first that is green.
  - `WorkspaceRef` gains a field, so this is a wire change. It shares a protocol version bump with
    AX-3 when both land in one release.
- **A5 Device leases (AX-5).**
  - A worker probes for attached devices at start-up and at an interval, bounded by
    `HOST_PROBE_TIMEOUT`: Android devices through `adb`, simulators through `xcrun simctl`.
  - It advertises `tool:adb` and one `device:<kind>:<model>` tag per device. `device:` becomes a
    reserved tag prefix, and `require:device=<pattern>` a placement entry.
  - The master grants one task per device at a time, as it grants one task per workspace. The task
    gets the device's serial in its environment.
  - Screenshots and reports come back through the output patterns that exist.
  - Desktop sessions for agents that drive a graphical interface are a later part. They need screen
    capture, which R8 looks at.
- **A6 Agent sessions (AX-6).** Needs LLM-2 (service tasks).
  - `oxideswarm session start --workspace <name> -- <agent command>` runs a long-lived task that
    survives the client's disconnect. `session attach` streams its output (AX-3) and forwards
    input lines. `session list` and `session stop` complete the set.
  - The command may be a vendor's own runner. The grid then supplies the machine, the placement,
    the isolation and the workspace, and does not compete with the runner.
  - Under `--isolation fs+net` a session needs a network grant that names the model provider's
    host. That grant is part of the item's design record.
- **A7 CI runner pool (AX-7).** The CI half of ECO-7.
  - A command on the master's host asks GitHub for a just-in-time runner registration and starts
    an ephemeral runner as a grid task on an idle worker, under isolation, one job per runner.
  - Code from forks must not reach these runners. The guide restricts them to pushes by
    collaborators and requires approval for outside contributors.
- **CACHE-1** stays as written. Matrix and shard results are cached per cell.

**Stop rule.** If R2 shows that sharding this repository's suite over three workers saves less
than 30 % of the wall time, AX-2 ships as a recipe in the guide only, without the build-once mode.

### D3 Bet B: portable and movable tasks

**Why.** A native task belongs to one operating system and one architecture, and it dies with its
machine. Both facts make spare machines hard to use: the pool is split by platform, and a laptop
that closes its lid wastes the work. WebAssembly removes the first limit. A checkpoint that is
plain bytes removes the second.

**Parts.**

- **NET-1, moved forward (Q21).** `TaskSpec::Wasm` as design 0054 plans it, with these choices
  proposed:
  - Wasmtime at an LTS release, behind a Cargo feature `wasm` that `rusty_grid_cli` forwards, as it
    does for `3d`. A worker built without it does not advertise the capability, and the master
    never places such a task on it (`TaskView::required_features`).
  - The module is an input blob. The task sees its sandbox directory and nothing else. It has no
    network unless granted.
  - Limits: a memory cap, a deadline by epoch interruption, and fuel in deterministic mode.
  - Deterministic mode: canonical NaN values, no relaxed SIMD, one thread, a seeded random source
    and a fixed clock.
  - Android stays out until R1 shows a supported path.
- **WASM-2 Interpreter tasks.**
  - A pinned CPython build for WASI is a blob. `oxideswarm run-python <script> --input <file>` and
    an MCP tool `run_code` run a script inside it on any worker with the capability.
  - Pure-Python packages travel in the input tree. Native extensions are out of scope.
  - This gives an agent a place to run code it wrote, with no container and no trust in the code.
- **WASM-3 Cooperative checkpoints.**
  - Two host functions: save a byte string, and read the last saved one. The task serialises its
    own state at points it chooses.
  - The worker sends the bytes as `CheckpointData` through the checkpoint path that exists. A
    reassigned task starts with them.
  - The bytes are defined by the task, so a checkpoint taken on one architecture resumes on
    another.
  - A small guest library for Rust and a helper for Python tasks hide the two calls.
- **WASM-4 Moving on a signal.**
  - Signals: the worker is draining (`WorkerMessage::Disconnecting`, design 0036), the lid closes,
    the battery is low, or the owner returns to the machine (GREEN-1).
  - The worker asks the task for a checkpoint, waits a bounded time, and drains. The master places
    the task again with the checkpoint.
- **WASM-5 GPU for WebAssembly tasks.** Research only (R6). Decision D6 of the backlog keeps GPU
  compute a benchmark until R6 has a record (Q24).
- **NET-5** stays as design 0054 plans it. The research names candidates per system.

**Stop rules.** If R1 shows a median slowdown above three times native on the reference set, the
task type stays for interpreter tasks and for strangers' code, and G2 is narrowed in this record.
If deterministic mode is not bit-identical across `x86_64` and `aarch64`, quorum runs (PROOF-2)
compare within one architecture, and the exit check of stage H0 in design 0054 changes.

### D4 Bet C: receipts, quorum runs and independent rebuilds

**Why.** Today a result is trusted because the cluster is. That does not carry to machines of other
owners, and it leaves no evidence afterwards. The research rules out proofs and trusted hardware.
What remains is cheap: sign what ran, run it more than once, and compare hashes. Inputs and outputs
are already BLAKE3 blobs, and ENROLL-1 gives each machine a key.

**Parts.**

- **PROOF-1 Execution receipts.** Needs ENROLL-1.
  - When a task ends, the worker signs a receipt with its machine key: a digest of the task (the
    command, the argument list, the names of the environment variables, the input blob ids, the
    workspace manifest id), the output blob ids, the exit code, the isolation level, the operating
    system and architecture, the versions behind its `tool:` tags, and the start and end times.
  - The master countersigns and keeps the receipt in its journal.
  - `oxideswarm receipt <task>` prints it. `oxideswarm receipt verify <file>` checks both
    signatures and the digests.
  - The export format is an in-toto statement with a SLSA provenance predicate (Q23), so existing
    verifiers read it.
  - A receipt holds the names of environment variables and never their values.
- **PROOF-2 Quorum runs.**
  - `submit --quorum <k>` runs the task on k distinct workers and accepts the result when the
    output digests agree. `--distinct os` or `--distinct owner` adds a rule for which workers
    count as independent.
  - On a mismatch the result is a failure that names both digests and the first differing file,
    and it keeps both outputs.
  - Inside one cluster this already finds flaky tests and outputs that depend on the machine.
  - Digest agreement fits tasks with deterministic outputs. Floating-point output from GPUs of
    different vendors or drivers can differ in its last bits and still be correct (design 0054
    D4). PROOF-2 builds no tolerance rule: such a task reports a mismatch, and its replicas
    belong on the same architecture and adapter.
  - This is the redundancy part of NET-3, built early. Spot checks and reputation stay in NET-3.
- **PROOF-3 A twice-built release.** Dogfood.
  - The release build becomes reproducible: remapped path prefixes, a fixed source date, locked
    dependencies, and archives with sorted entries and fixed timestamps.
  - Before a release is published, a second machine with another owner rebuilds each archive
    through the grid with quorum 2. The receipts are attached to the release.
  - macOS binaries are compared after the signature is stripped, as the Go project does.
- **PROOF-4 A transparency log.**
  - An append-only Merkle log of receipts in the tile layout that Rekor v2 uses, served as static
    files.
  - `oxideswarm log verify` checks that a receipt is in the log and that the log only grew.
  - Whether the project runs a log of its own or writes to the public Sigstore log is Q23.
- **PROOF-5 The rebuild network.** The first workload for other people's machines.
  - A task class: fetch a published package and its source at the tagged revision, build it in
    isolation with a network grant for the registry and the repository only, compare the result
    with the published artefact, and log the receipt.
  - It starts with crates.io packages and with projects that publish reproducible binaries.
  - Volunteers run it first inside federated clusters (NET-2) and then in the public pool (stage
    H2).
  - A public page lists, per package and version, how many unrelated machines agree.
  - Windows and macOS binaries need Windows and macOS machines. Volunteers have them, and a Linux
    data centre does not.
  - It reads and writes the attestation formats that existing rebuilders use, so the logs add up.

**What it does not prove.** A matching rebuild shows that the artefact comes from the published
source. It says nothing about whether the source is safe, and it does not catch stolen publishing
credentials. No document of the project may claim more.

**Stop rule.** If R3 finds that the release cannot be made reproducible on a platform within the
spike's time box, PROOF-3 ships for the platforms where it works, and the record lists the causes
for the others.

### D5 Bet D: models on ordinary machines, a classroom first

**Why.** A lab of ordinary PCs cannot run one large model fast. It can run one small model on each
PC and answer a whole class at once, because requests from different students need no traffic
between machines. The grid already knows which machines are up, what they have and how to reach
them.

**Parts.**

- **LLM-1** stays as written: a prompt goes to the worker whose tag names the model.
- **LLM-6 A gateway.**
  - `oxideswarm llm serve` offers the chat completion and model list routes that OpenAI-compatible
    clients use. It listens on loopback by default. Any other address needs a token, under the
    same policy as the dashboard.
  - Each request goes to the least loaded worker that holds the model. A queue per model bounds the
    waiting requests, and a limit per client keeps one user from taking the lab.
  - First version: a request is a task and the reply returns whole. Second version: replies stream
    through the tunnel of LLM-4 to a model server that runs as a service task (LLM-2).
- **LLM-7 Model distribution.** Needs SCALE-3.
  - `oxideswarm llm pull <model> --workers <selector>` moves a model file as a blob to many
    workers. With SCALE-3 the workers fetch from each other, and the master sends each chunk once.
  - A hardware probe (memory, GPU memory) picks the largest listed model a worker can hold.
- **LLM-8 Batch inference.**
  - `oxideswarm llm batch --input <prompts.jsonl> --model <name>` splits the prompts into tasks,
    spreads them over workers, and merges the replies in input order.
  - A lost worker loses no prompt and adds no duplicate, because each shard is a task with a
    retry.
  - Uses: grading, embeddings, labelling, evaluation runs.
- **EDU-1 The classroom kit.** Needs USB-1 and LLM-6.
  - The USB kit gains a `classroom` profile. On a lab PC it installs the worker for the current
    user, joins with the stick's invite, and starts a local model server with the model the probe
    picked. It needs no administrator rights and no Internet access.
  - The teacher's machine runs the master, the gateway and one chat page on the lab's network.
  - A guide in Vietnamese and English covers one lesson's set-up from an empty lab.
- **EDU-2 A course pack.** Exercises that teach distributed systems on the lab's own cluster:
  submit a job, stop a worker, watch the task move, read a receipt.
- **LLM-2 to LLM-5** stay. LLM-2 is needed by LLM-6 and AX-6. The split of one model (LLM-5) is for
  models that fit no single machine and is known to be slow on an ordinary network.

**Sizing.** No capacity number is known for current lab PCs. R4 measures tokens per second per
machine class and the number of students one lab serves, before any document names a figure.

**Stop rule.** If R4 shows that a typical lab PC without a GPU gives a small model fewer than five
tokens per second, EDU-1 targets labs with GPUs or a few stronger machines, and the guide says so.

### D6 Bet E: the commons

Stages H0 to H4 of design 0054 stay. This record proposes these things around them:

- **A condition for donated compute.** A project that uses the public pool publishes its outputs
  and its task code. The draft charter is [design 0057](0057-open-results-charter.md). The
  owner accepted its principle on 2026-11-10, so a receipt names the project and the charter from
  the start (PROOF-1).
- **A first workload.** PROOF-5 answers part of Q16. Its results are checked by comparing hashes,
  so it needs none of the harder verification.
- **An earlier sandbox.** NET-1 moves to the second wave (Q21), because bets B and C use it inside
  trusted clusters.
- **Rules for a good guest.**
  - **GREEN-1 Idle-aware contribution on desktops.** A worker can run tasks only while the screen
    is locked, or after a time without input, or on mains power, or by a schedule. This extends the
    battery tiers of `crates/core/src/capabilities.rs` from phones to desktops. When the owner
    returns, movable tasks checkpoint and leave (WASM-4), and native tasks are evacuated (design
    0036).
  - **GREEN-2 Deferrable tasks.** `submit --deferrable --deadline <time>` lets the master hold a
    task until a low-cost window. The window is first a schedule in the master's configuration,
    and later a signal from a provider the owner configures. A receipt carries a labelled estimate
    of the energy used.
- **A first federation user.** **EDU-3 Night lending:** labs that are idle at night lend their
  machines to another cluster under NET-2, for example to the rebuild network.
- **A worker image (IMG-1 to IMG-4).** On 2026-11-10 the owner asked whether OxideSwarm should
  become an operating system that replaces the installed ones, so that every machine can join.
  This record recommends against it (Q30). The grid reaches a machine sooner as a program on the
  system its owner already uses. A replacement needs drivers and applications for every machine,
  and none of that work connects one more machine. The part of the idea that serves the grid is an
  image that turns a whole machine into a worker without touching what is installed on it:
  - **IMG-1 A bootable worker image.** A small Linux system for `x86_64` PCs with the worker in
    it. It starts from a USB stick, runs from memory, and makes the machine a worker until it is
    switched off.
    - It never mounts the machine's internal disks, and it marks them read-only.
    - The worker and its tasks run without root, under `--isolation fs+net`.
    - It joins with the invite on the stick (USB-1) or with a short code (JOIN-2).
    - A console screen shows the cluster, the link and the running tasks.
    - It uses the signed boot chain of a Linux distribution, so it starts with Secure Boot on.
    - It has no desktop and no packages for users. A script in CI builds it reproducibly, and each
      release publishes it with its checksum.
  - **IMG-2 Identity and updates.** The machine key lives on the stick's data partition under the
    kit's passphrase, or it is discarded at power-off. Every protocol bump breaks older peers, so
    a stick goes stale. When the master is newer, the image fetches the matching worker through
    the blob plane, verifies it against the release checksums, and runs it from memory.
  - **IMG-3 Network boot for a lab.** The master's host offers the image over the network, so a
    lab starts without sticks. A schedule starts the machines in the evening and restarts them
    into their installed system before class. The lab's administrator sets this up once.
  - **IMG-4 The image as a virtual machine.** The same image runs as a guest on macOS and Windows,
    and is then the Linux side of NET-5.
  - **Limits.** Phones and Apple silicon Macs do not start from such a stick. They join through
    the worker on their installed system. A bootable image is a non-goal of the USB kit
    ([design 0053](0053-usb-setup-kit.md)) and stays one. The image is a separate item that reuses
    the kit's invite.

The research adds one rule: a network grows from a workload people need. Supply alone, and tokens
that pay for it, did not produce demand elsewhere.

### D7 Lighthouse runs

Each run is recorded with its script and date under `benchmarks/grid/`, as BENCH-1 requires.

| ID | Name | The sentence | Needs | Measured |
|---|---|---|---|---|
| L1 | Four systems, one call | One call from a coding agent tests a change on macOS, Windows, Linux and a real Android phone | AX-1, AX-3, AX-5 | Time to verdict, cold and warm, per cell |
| L2 | The task that outlives its machine | A job starts on a laptop, the lid closes, and the job finishes on a PC with another operating system, with output identical to an uninterrupted run | NET-1, WASM-3, WASM-4 | Compute lost at the move, in seconds. Output digests |
| L3 | The twice-built release | Two machines with different owners build a release, the archives match byte for byte, and anyone can verify the receipts | PROOF-1, PROOF-2, PROOF-3 | Matching archives per platform. Causes of any mismatch |
| L4 | The classroom cluster | One person turns a lab of ordinary PCs into a private assistant for the class from a USB stick, without Internet access | USB-1, LLM-6, EDU-1 | Set-up minutes, machines, students served at once, median tokens per second |
| L5 | The grid builds the grid | This repository's tests run sharded on the grid, its CI jobs run on grid runners, and its release is the one of L3 | AX-2, AX-7, L3 | Suite wall time against one machine. A week of CI queue times and failures |
| L6 | The lab that works at night | A school lab starts from the network in the evening, works for the grid all night, and is back on its own system in the morning with its disks untouched | IMG-1, IMG-2, IMG-3 | Machines, hours served, tasks done. Every machine starts its own system the next morning without a prompt |

### D8 Order of work

The waves follow the release in progress. Each wave has one protocol version bump at most, because
every bump breaks older peers ([design 0040](0040-protocol-versioning.md)).

| Wave | Release | Theme | Items | Lighthouse | Spikes that run during it |
|---|---|---|---|---|---|
| 0 | `0.2.0` | Machines anywhere (Phase E, unchanged) | ENROLL-1, JOIN-2, USB-1 | | R1, R2, R3, R4 |
| 1 | `0.3.0` | Agents | AX-1, AX-2, AX-3, CACHE-1 | L1 on desktop systems | R5, R7, R8, R9 |
| 2 | `0.4.0` | Evidence and portability | PROOF-1, PROOF-2, PROOF-3, NET-1, WASM-2, AX-4, AX-5, AX-7 | L3, L1 with the phone, L5 | R6, R10 |
| 3 | `0.5.0` | Models and classrooms | LLM-1, LLM-2, LLM-6, LLM-8, SCALE-3, LLM-7, EDU-1, EDU-2, WASM-3, WASM-4, GREEN-1, IMG-1, IMG-2 | L4, L2 | |
| 4 | later | Commons | PROOF-4, NET-2, PROOF-5, NET-3, NET-4, GREEN-2, EDU-3, AX-6, NET-5, IMG-3, IMG-4 | L6. Stage checks of design 0054 | |

Lanes inside a wave, for the three implementing agents:

- Wave 1: AX-1 then AX-2 (CLI and MCP); AX-3 wire and master; AX-3 MCP and CACHE-1.
- Wave 2: PROOF-1 to PROOF-3; NET-1 and WASM-2; AX-4, AX-5 and AX-7.
- Wave 3: LLM-2, LLM-6 and LLM-8; SCALE-3, LLM-7 and EDU-1; WASM-3, WASM-4 and GREEN-1. IMG-1 and
  IMG-2 follow EDU-1 in the second lane.

Relation to the other phases:

- Phase F: SCALE-1 stays first there. This program needs SCALE-3 in wave 3 and SCALE-6 in wave 4.
- Phase G: AX-7 is half of ECO-7. ECO-3 is needed before any public pool. ECO-4, ECO-5 and ECO-6
  run beside the waves and are not ordered by this record.
- How the agents' time splits between this program and Phases F and G is Q26.

Sizes in the work item table are the backlog's S, M and L. No dates are given. A wave ends with
its lighthouse record, not on a day.

### D9 Spikes

A spike is throwaway code on a branch, or a script under `benchmarks/`. It builds nothing in the
product and ends with a dated record. Each is sized S.

| ID | Question | Method | Stop rule |
|---|---|---|---|
| R1 | What does WebAssembly cost on our machines, and is deterministic mode bit-identical across architectures? | Run four CPU workloads (hashing, compression, a floating-point simulation, a CPython benchmark) natively and in Wasmtime on an Apple silicon Mac, an `x86_64` Windows PC, a Linux machine and an Android phone. Compare output digests | See D3 |
| R2 | How much wall time does sharding save on this repository's suite? | `cargo nextest archive` on one machine, slices on one, two and three workers through today's `submit` | See D2 |
| R3 | Is our release reproducible? | Build the tag twice on two machines per platform. Compare with `diffoscope`. List each cause | See D4 |
| R4 | What does a lab PC give a small model? | Tokens per second for three small models on three machine classes (no GPU, integrated GPU, a mid-range GPU). Then concurrent requests through a stub gateway | See D5 |
| R5 | Can a WebAssembly task be snapshotted without its cooperation? | Read the cited work, and try a memory and stack snapshot in Wasmtime with a compile-time transformation | If it needs a fork of the runtime, cooperative checkpoints (WASM-3) are the product and this closes |
| R6 | Can a WebAssembly task use the GPU through `wgpu` on three vendors' drivers, and what is the trust boundary? | A prototype host binding for one compute shader. A written threat model | If the boundary rests on shader validation alone, WASM-5 closes for strangers' code |
| R7 | What does a workspace fork cost per file system? | Clone a 1 GiB workspace on APFS, ReFS, NTFS, btrfs and ext4. Time it and measure the disk used | Where no copy-on-write exists and the copy takes longer than a warm incremental build, forks are refused there |
| R8 | Does a device lease work under isolation, and what does a graphical session need? | Run an instrumented Android test through `adb` from an isolated task on each desktop system. List what screen capture needs per system | A system where `adb` cannot run under `--isolation fs` documents `off` for device tasks |
| R9 | Which features of hosted sandboxes do coding agents depend on? | Read the public interfaces of four sandbox products and of the vendors' self-hosted runners. List the calls an agent makes. This is the open question of the 2026-11-08 research | None. It sets the scope of AX-6 |
| R10 | Which base system gives a worker image that starts on ordinary lab PCs with Secure Boot on? | Build the image from two distribution bases. Start it on real PCs: one with BitLocker, one Secured-core, one older than 2020 and one with an NVIDIA GPU. Record whether it starts, whether the installed system starts unchanged afterwards, whether the GPU is usable, the image size and the time to a registered worker | If most test machines need a firmware change, the image is documented for machines whose owner controls the firmware, and IMG-3 closes for locked labs |

### D10 Measures

Each measure has a script and a dated record before any document quotes it.

| ID | Measure | Target |
|---|---|---|
| M1 | Time to verdict: from the agent's call to the verdict of this repository's suite | With three workers, at most half of the time on one |
| M2 | Cells per call: operating systems and devices verified by one call | Four in L1 |
| M3 | Portable share: task types that run unchanged on all desktop systems | `Wasm` and `run_code` at 100 %, by a cross-system digest test |
| M4 | Receipt cover: finished tasks with a receipt that verifies | 100 % in clusters with machine keys |
| M5 | Independent agreement: package versions with at least three agreeing rebuilds from unrelated owners | 100 package versions for the first public page |
| M6 | Classroom set-up: minutes from an empty lab to the first reply, and students served at once | Set by R4 |
| M7 | Work lost at a move: compute seconds repeated after a task changes machine | At most one checkpoint interval |
| M8 | Start to worker: seconds from power-on of a machine with the worker image to a registered worker | Set by R10 |

## Compatibility

- **Wire changes.** AX-3 (output chunks, a capability bit), AX-4 (a `WorkspaceRef` field), NET-1 (a
  `TaskSpec` variant, a capability bit) and PROOF-1 (the receipt and its journal entry) change the
  wire or the journal. Each needs the mirror enums and conversions that `CLAUDE.md` lists, golden
  bytes, and a protocol version bump. D8 groups them so each wave has one bump.
- **No wire change.** AX-1, AX-2, AX-5 (tags and placement entries), LLM-6 (a new HTTP listener),
  LLM-8, GREEN-1, IMG-1 to IMG-4 and the spikes.
- **MCP.** The server keeps answering `2024-11-05` clients after it learns `2026-07-28`.
- **Reserved tag prefixes.** `device:` joins the reserved prefixes of `placement.rs`. A user tag
  with that prefix is refused from then on, which the release notes must say.
- **Upgrade order.** Unchanged: the master first.

## Security

| Bet | New exposure | Rule |
|---|---|---|
| A | An agent starts more tasks per call, on more machines | Every path stays behind the cluster handshake. The cell limit of AX-1 bounds a call |
| A | A task with a device lease can read what is on the device | The guide says to use test devices. A device is leased only on a worker whose owner tagged it |
| A | CI runners execute code from pull requests | AX-7 refuses fork pull requests by configuration, runs under isolation, and uses one job per runner |
| B | A WebAssembly runtime is new attack surface | An LTS release, `cargo audit` in CI, memory and time limits, no network by default, and attack tests in the item's "Done when" |
| C | A receipt reveals command lines and file digests | Variable values never enter a receipt. Publishing to a log is opt-in per cluster |
| C | A worker's signing key is its machine key | Revocation follows ENROLL-1. A receipt from a revoked key fails verification from the revocation time on |
| D | The gateway is an HTTP listener that spends compute | Loopback by default, a token elsewhere, a limit per client |
| D | Students' prompts cross the lab's network and reach the teacher's machine | The guide states it. The kit stores no prompts by default |
| E | Strangers' tasks and strangers' results | The threat model of each stage in design 0054, with an independent review before stage H2 |
| E | A started worker image has the machine's internal disks within reach | The image never mounts them and marks them read-only. The worker and its tasks run without root. An attack test tries to open a disk from a task |
| E | A stick carries an old or altered image | A reproducible build with published checksums. The update of IMG-2 verifies what it fetches |
| E | A guide tells users to change firmware settings | No guide tells a user to turn Secure Boot off. A change of that setting can make BitLocker ask for its recovery key |

## Test plan

- Each item's "Done when" in the table below, as a unit or integration test where one machine is
  enough.
- Each lighthouse run (D7) as a live record with script and date.
- Attack tests for WASM-2 (read a host file, open a socket, exhaust memory, never end) and for
  PROOF-2 (a planted worker that returns another output).
- A golden-bytes test for the receipt encoding, so a signature made by one release verifies in the
  next.
- A compatibility test that an MCP client on `2024-11-05` still works after AX-3.

## Work items

"Needs" lists items of this record and of the backlog. Sizes are the backlog's S, M and L.

| ID | Item | Bet | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|---|
| AX-1 | Matrix fan-out in `submit` and in MCP | A | M | | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/blobs.rs`, `README.md` | An end-to-end test with three tagged workers returns three cell records from one call, and a cell without a worker is reported unplaced. A live record shows one MCP call testing on macOS and Windows | Planned |
| AX-2 | Sharded runs, with a build-once mode for Rust | A | M | AX-1, R2 | `crates/cli/src/`, `docs/guides/`, `benchmarks/grid/` | Measure M1 is recorded for this repository on three workers | Planned |
| AX-3 | Live output, `logs`, and MCP `2026-07-28` with the tasks extension | A | L | | `crates/core/src/protocol.rs`, `crates/worker/src/runner.rs`, `crates/master/src/session.rs`, `crates/cli/src/mcp.rs` | A client sees the first output line while the task runs. An MCP client polls a running task and reads an output tail. The `2024-11-05` test still passes | Planned |
| AX-4 | Workspace forks and `--first-success` | A | L | R7 | `crates/core/src/task.rs`, `crates/worker/src/workspace.rs`, `crates/cli/src/` | Two forks of one base run at the same time, the base is unchanged, and a warm fork does not rebuild dependencies (recorded) | Planned |
| AX-5 | Device tags, `require:device=` and one task per device | A | M | R8 | `crates/core/src/placement.rs`, `crates/worker/src/`, `crates/master/src/scheduler.rs`, `docs/guides/` | A live record: an MCP call runs an instrumented test on a phone attached to another machine and returns a screenshot. A second task for that device waits | Planned |
| AX-6 | Agent sessions: start, attach, list, stop | A | L | AX-3, LLM-2, R9 | `crates/cli/src/`, `crates/master/src/` | A live record: a session runs for 30 minutes on another machine, the client disconnects and attaches again, and no output is lost | Planned |
| AX-7 | CI runner pool for GitHub Actions | A | M | AX-1 | a new module in `crates/cli/src/`, `docs/guides/` | One required CI job per operating system of this repository runs on grid runners for seven days, with queue times recorded | Planned |
| NET-1 | WebAssembly task type (design 0054), moved to wave 2 | B | L | ENROLL-1, R1 | `crates/core/src/task.rs`, `crates/worker/src/`, `crates/cli/src/` | The exit check of stage H0, adjusted by the result of R1 | Planned |
| WASM-2 | Interpreter tasks: `run-python` and MCP `run_code` | B | M | NET-1 | `crates/cli/src/`, `crates/cli/src/mcp.rs`, `docs/guides/` | One script prints the same bytes on macOS, Windows and Linux workers. The four attack tests end as failed tasks with a clear reason | Planned |
| WASM-3 | Cooperative checkpoints for WebAssembly tasks | B | M | NET-1 | `crates/worker/src/`, a guest library | A task killed in the middle resumes on a worker with another operating system and architecture, and its output digest equals that of an uninterrupted run | Planned |
| WASM-4 | Checkpoint and move on drain, lid, battery and owner activity | B | M | WASM-3, GREEN-1 | `crates/worker/src/client.rs`, `crates/master/src/queue.rs` | Lighthouse L2 is recorded with measure M7 | Planned |
| WASM-5 | GPU for WebAssembly tasks | B | L | R6 | to be designed | Opens only if R6 does not meet its stop rule | Design only |
| PROOF-1 | Signed execution receipts and `receipt verify` | C | M | ENROLL-1 | `crates/core/src/`, `crates/worker/src/client.rs`, `crates/master/src/store.rs`, `crates/cli/src/` | A genuine receipt verifies, and one with a changed output id is refused. The encoding has a golden-bytes test | Planned |
| PROOF-2 | Quorum runs | C | M | PROOF-1 | `crates/cli/src/`, `crates/master/src/queue.rs` | A planted worker with another output makes the result a quorum failure that names both digests | Planned |
| PROOF-3 | Reproducible release, rebuilt by a second owner before publishing | C | M | PROOF-2, R3 | `.github/workflows/release.yml`, `Cargo.toml`, `docs/guides/` | Lighthouse L3 is recorded for each platform R3 cleared | Planned |
| PROOF-4 | Transparency log of receipts and `log verify` | C | L | PROOF-1 | a new module, `docs/SECURITY.md` | An inclusion proof verifies offline, and a log with a removed entry fails the consistency check | Planned |
| PROOF-5 | Rebuild network: task class, page, attestation formats | C, E | L | PROOF-2, PROOF-4, NET-2 | to be designed | Measure M5 is met and the page is published with its date | Planned |
| LLM-6 | OpenAI-compatible gateway | D | M | LLM-1; LLM-2 and LLM-4 for streaming | `crates/cli/src/`, `README.md` | An unchanged OpenAI client completes a chat through the gateway, and requests spread over two workers | Planned |
| LLM-7 | Model distribution and model choice by probe | D | M | SCALE-3 | `crates/cli/src/`, `crates/worker/src/` | A 4 GiB model reaches N workers while the master sends each chunk once (the SCALE-3 check), with the time recorded | Planned |
| LLM-8 | Batch inference | D | M | LLM-1 | `crates/cli/src/` | 10,000 prompts complete on three workers. Stopping a worker in the middle loses no prompt and adds no duplicate | Planned |
| EDU-1 | Classroom profile of the USB kit, chat page, guide | D | L | USB-1, LLM-6, R4 | `packaging/usb/`, `crates/master/src/web_ui.rs`, `docs/guides/` | Lighthouse L4 is recorded in a real lab | Planned |
| EDU-2 | Course pack on the lab's own cluster | D | S | EDU-1 | `docs/guides/` | A class completes the exercises, and the feedback is recorded | Planned |
| EDU-3 | Night lending between labs | E | M | NET-2, GREEN-1 | to be designed | One lab's idle machines run another cluster's tasks for a week, and both credit logs agree | Planned |
| GREEN-1 | Idle-aware contribution on desktops | E | M | | `crates/core/src/capabilities.rs`, `crates/worker/src/heartbeat.rs`, `crates/worker/src/client.rs` | A test with a simulated activity signal drains the worker within a bound and readmits it afterwards | Planned |
| GREEN-2 | Deferrable tasks and an energy estimate | E | M | PROOF-1 | `crates/master/src/queue.rs`, `crates/cli/src/` | A simulation with a recorded schedule moves deferrable tasks into the low window and meets every deadline | Planned |
| IMG-1 | Bootable worker image for `x86_64` PCs | E | L | USB-1, R10 | `packaging/image/` (new), `.github/workflows/release.yml`, `docs/guides/` | A virtual machine test in CI starts the image with a second disk attached and runs a task. The disk's digest is unchanged, and a task cannot open the disk. A live record shows a PC from stick to registered worker with measure M8 | Planned |
| IMG-2 | Machine key on the stick and a verified update of the worker | E | M | IMG-1, ENROLL-1 | `packaging/image/`, `crates/cli/src/` | A stick made for one release joins a master of the next release after a verified update, and refuses a worker whose checksum does not verify | Planned |
| IMG-3 | Network boot and a night schedule for a lab | E | M | IMG-2 | `packaging/image/`, `docs/guides/` | Lighthouse L6 is recorded | Planned |
| IMG-4 | The image as a guest on macOS and Windows | E | L | IMG-1, NET-5 | to be designed | The image runs as a guest on both systems and registers as a Linux worker | Planned |
| R1 to R10 | Spikes (D9) | all | S each | | `benchmarks/`, scratch branches | The dated record and the go or stop are in this record's status log | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision. They are listed in
[ROADMAP.md](../ROADMAP.md) as Q19 to Q26 and Q30.

| ID | Question | Recommended default |
|---|---|---|
| Q19 | Accept the direction, the three properties and the dogfood rule? | Yes |
| Q20 | May spikes R1 to R4 run now, beside Phase E? | Yes. A spike builds nothing in the product, and principle 6 already lets a design record be written at any time |
| Q21 | Move NET-1 from stage H0 into wave 2? | Yes, after R1, behind the Cargo feature `wasm` |
| Q22 | Which bet follows the `0.2.0` release? | Bet A: AX-1, AX-2 and AX-3 |
| Q23 | Which receipt format and which log? | An in-toto statement with a SLSA provenance predicate. A log file of our own first. The public Sigstore log only for this project's releases |
| Q24 | Does R6 reopen decision D6, which keeps GPU compute a benchmark? | No. D6 stands until R6 has a record |
| Q25 | Is there a lab for lighthouse L4, and in which term? | Owner. Without a lab, EDU-1 waits and wave 3 ends with L2 |
| Q26 | How does the agents' time split between this program and Phases F and G? | One lane on Phase F and G items, two lanes on the current wave |
| Q30 | Does OxideSwarm aim to replace the operating systems installed on machines? The owner raised it on 2026-11-10 | No. It stays a program on every system. The worker image (IMG-1 to IMG-4), which the owner added to the plan the same day, is the part that is built |

Principle 1 of the roadmap says one user story first. Bets A to C serve that story. Bet D adds a
second one, the classroom. Accepting Q25 accepts the second story.

## Risks

| Risk | Mitigation |
|---|---|
| The program spreads effort over many parts | One wave at a time, one lighthouse per bet, and stop rules that close a bet |
| A claim rests on a search summary | The "Basis" column. A summary row is checked at its source before it is quoted elsewhere |
| WebAssembly is too slow or not deterministic across architectures | R1 runs first, and D3 names what narrows |
| A release cannot be made reproducible on Windows or macOS | R3 lists the causes. PROOF-3 ships per platform |
| Self-hosted CI runners run hostile code | The rules of AX-7. The pool stays off for public fork pull requests |
| A classroom run disappoints on weak machines | R4 before any promise, and the stop rule of D5 |
| Wire changes pile up and break mixed versions | One protocol bump per wave |
| Another rebuilder already covers the need | PROOF-5 uses the existing formats and targets what they lack: unrelated owners, Windows and macOS |
| Model files have licences that forbid redistribution | The kit lists only models whose licence allows it, and the guide names the licence |
| The worker image grows into a general-purpose system | It holds a base system and the worker, with no desktop and no packages for users. Q30 keeps the first non-goal of design 0054 |
| Secure Boot certificates changed in 2026 and differ per machine | R10 tests real machines before IMG-1 is sized |

## Status log

- 2026-11-10: Draft, at the owner's request for a research and development plan beyond the
  roadmap. Four web research runs on 2026-11-10 supplied the findings in Context. Decisions Q19 to
  Q26 are open. Nothing is built.
- 2026-11-10: D6 gained the condition for donated compute, which is the draft charter of
  [design 0057](0057-open-results-charter.md).
- 2026-11-10: The owner asked whether OxideSwarm should become an operating system that replaces
  the installed ones, and then added a bootable worker image to the plan. D6 gained IMG-1 to
  IMG-4, D7 lighthouse L6, D9 spike R10 and D10 measure M8. Q30 is open.
- 2026-11-10: [Design 0059](0059-first-users.md) proposes that its items ADOPT-2 and ADOPT-3 join
  wave 1 and that ADOPT-4 follows AX-1 (Q36). D8 is unchanged until the owner decides.
- 2026-11-11: PROOF-2 gained a note on the limit of digest agreement for GPU results, from an
  outside reading of the plans. No item changed.
- 2026-11-11: [Design 0061](0061-first-application-model-tunnel.md) proposes that the
  streaming version of LLM-6 uses a client tunnel (LLM-9) and comes before the version that
  returns each reply whole. Q41 decides, after a trial. The text of LLM-6 is unchanged.
