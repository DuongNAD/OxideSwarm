# 0063: Two directions in parallel: compute for coding agents, and a study kit for a class

Status: Draft (plan; builds nothing; the owner decided Q48 on 2026-11-12; Q49, Q50, Q52, Q53 open)
Created: 2026-11-12    Updated: 2026-11-13

## Summary

The owner asked which part of the projects takes a road of its own and would be chosen by other
people, and on 2026-11-12 agreed to follow two directions side by side (Q48). Direction 1 is
bet A of [design 0056](0056-frontier-program.md): a coding agent uses every machine its owner
has. Direction 2 is a study kit for a class. This record gives each direction one trial that
needs nothing built (APP-2, APP-3). It also reads the owner's simulation project, Anima Engine,
as a first workload of the owner's own (APP-4), and maps what the owner's other projects could
give and what the grid does with CPU and GPU today (D6). It adds no feature.

## Context

### What the owner said

Four statements of 2026-11-12, paraphrased from Vietnamese:

1. The final goal is to gather the computing power of all people for the open problems of
   science.
2. An operating system for AI agents could be the breakthrough. A large vendor is ahead, and
   this project still has the potential for it.
3. Suggest the part that takes another road, and that people would choose to use.
4. To follow both suggested directions in parallel is fine.

### What was checked on 2026-11-12

- **A sandbox for agent code on one machine exists elsewhere.** The README of
  [anthropics/sandbox-runtime](https://github.com/anthropics/sandbox-runtime) names
  `sandbox-exec` on macOS, bubblewrap on Linux, and on Windows a local account of its own with
  a network filter. A code search of `openai/codex` finds a sandbox crate that names
  AppContainer. A sandbox alone therefore does not set this project apart.
- **Not checked:** whether either of the two places work on a second machine of the same
  owner. The nearest neighbours of direction 1, which are self-hosted CI runners and hosted
  sandbox services, were not read either. D4 puts that check before any public sentence.
- **Splitting one model over machines** gives capacity and no speed.
  [Design 0062](0062-model-weights-into-remote-memory.md) holds what was read and checked.
- **The final goal.** A question on volunteer computing was written for a research tool on the
  same day. No answer is in yet, and this record takes nothing from it.

### "An operating system for agents", read as a list

Q30 asks whether OxideSwarm becomes a full operating system. It stays open, with "no" as the
recommended answer. Read another way, the name means a layer that gives agents, on several
machines, what an operating system gives programs. That list is:

| What an operating system gives | What is here | Status |
|---|---|---|
| An isolated process | A task under `--isolation fs` or `fs+net` on macOS, Linux and Windows (design 0033) | Implemented |
| A file system | Workspaces (design 0039) and the content-addressed blob store (design 0006) | Implemented |
| A scheduler | Placement by operating system, architecture, GPU, tags and slots | Implemented |
| Calls that an agent can make | `oxideswarm mcp`, with six tools | Implemented |
| Messages between agents | Agent Mesh. Its command executor is not a sandbox | Partial |
| An identity for each machine, and rights | ENROLL-1 (design 0051). The backlog says Planned, and release 0.1.2 names a first part that is opt-in | Planned |
| Services that keep running | LLM-2 (design 0038) | Planned |
| Access to a model | LLM-1, LLM-6, LLM-9 | Planned |
| Limits, and a record of what ran | LLM-10, and PROOF-1 to PROOF-5 of design 0056 | Planned |
| Live output and sessions | AX-3, AX-6 | Planned |

Every row that is not Implemented already has an item. The name describes the road that exists,
and no new item follows from it.

## Goals

- **G1** Each direction has a first trial that needs nothing built and ends with a dated
  record.
- **G2** The records, and not a guess, say which existing item is built first.
- **G3** Neither direction waits for the other.
- **G4** No public sentence claims a difference before the neighbours of D4 are read.

## Non-goals

- A full operating system (Q30).
- A new feature, flag or message.
- A pool of strangers' machines. Q13 stays at A: clusters of people who trust each other.
- A claim that either direction is the only one of its kind.
- A change to the owner's study tools, to LIVA or to Anima Engine. They are other repositories.

## Design

### D1 Direction 1: a coding agent uses every machine its owner has

This is bet A (design 0056, D2) and audience A1 ([design 0059](0059-first-users.md), D1). This
record adds the order: use it first, then build.

- **Today.** The owner's two coding agents, one on macOS and one on Windows, send each other
  requests through a shared folder ([guide](../guides/multi-agent-collaboration.md)). Each runs
  the tests on its own machine.
- **The trial (APP-2).** For one week of real work on this repository, the agent on one machine
  verifies its changes on the other operating system through the grid: the MCP `submit` tool
  with an operating system, a workspace, and isolation on. This follows the dogfood rule of
  design 0056 (G6).
- **What is written down.** One line for each run: the command, the two systems, whether the
  result came back and was right, the time from the call to the result, and what was missing.
- **What it decides.** The item of AX-1 to AX-6 that the week missed most often is built first.
  If the shared folder served as well as the grid, the record says so, and bet A is asked
  again.
- **One run kept.** One run of the week, in which an agent on one system fixes a fault that
  shows only on the other, becomes a guide of ADOPT-4.

### D2 Direction 2: a study kit for a class

- **Who.** The owner's fellow students. Design 0059 (D4) records that they do not wait for
  their machines, so the grid alone gives them nothing. They do study.
- **What exists outside this repository.** The owner's study tools: a set of MCP servers for a
  course plan, lessons, spaced review and practice labs. They run inside a coding agent.
- **What is planned here.** EDU-1 (the classroom profile of the USB kit, with a chat page),
  EDU-2 (a course pack) and LLM-9 (a client tunnel to a model server). None is built.
- **The gap.** A student needs an agent and a model to use the tools, and a class has one or
  two strong machines. A model served from such a machine to the others is what LLM-1, LLM-9
  and EDU-1 plan.
- **The trial (APP-3).** The owner sits with three to five fellow students, one at a time,
  while each studies one lesson with the study tools on the owner's machine. The notes say
  what the student did, where they stopped, what they asked for, whether they would use it
  again, and whether their own machine could run it.
- **What it decides.** If no student wants to go on, direction 2 stops here, and EDU-1 keeps
  its place in design 0056. If some do, Q50 is asked with the notes in hand.

APP-3 can run in the same week as the first round of ADOPT-1 (Q37). The two ask different
questions: ADOPT-1 watches a first use of the grid, and APP-3 watches a lesson.

### D3 How the two meet

| Item | Direction | Its part |
|---|---|---|
| `oxideswarm mcp`, isolation, workspaces | 1 | They exist. APP-2 uses them |
| AX-1 to AX-6 | 1 | APP-2 sets their order |
| ADOPT-4 | 1 | One recorded run becomes a guide |
| LLM-9 | 2, and LIVA | A model on a strong machine reaches a weak one. APP-1 of [design 0061](0061-first-application-model-tunnel.md) is its trial |
| EDU-1 | 2 | It waits for APP-3 |
| Design 0062 | 2, later | A class machine that lacks the model file |
| Anima Engine | 1, and a workload | Its build and tests are a case for APP-2. Its sweeps of experiments are APP-4 (D5) |

Both directions give a person a reason to install a worker. A pool for science needs that
before anything else. The pool itself is Q13 B, and it still waits for real users and a design
of its own.

### D4 Order

1. APP-2 can start now. It needs release 0.1.2 on the owner's two machines.
2. APP-3 can start now. It needs the owner's machine and three to five fellow students.
3. Before Q49: the neighbours of direction 1 are read, which are self-hosted CI runners and
   hosted sandbox services, and whether a vendor's sandbox places work on a second machine.
4. Build items follow the trial records. Each enters the backlog when the owner accepts it.
5. APP-4 follows APP-2. It uses the two machines as APP-2 leaves them.

### D5 A workload of the owner's own: Anima Engine

**What it is.** [Anima Engine](https://github.com/DuongNAD/Anima-Engine) is the owner's
simulation of a living world: terrain, water, climate, plants, animals and their evolution.
Its core is Rust, inside a desktop application. Its README and its experiment contract were
read on 2026-11-13 at `main` (`a43872c5e1db`).

**What those two documents say.**

- An experiment is described by a manifest that holds every input, the seed among them. A
  runner executes it with no window. One environment variable overrides the seed for sweeps.
- A deterministic mode exists, and the contract requires that a replay gives the same
  checksum.
- The model runs on the GPU or on the CPU.
- As of 2026-07-27: the experiment path is verified with no window and is not called ready
  for experiments, no run of the desktop application is recorded, no performance number
  exists, and one unresolved licence text blocks a release.
- A full build of its backend has hung the owner's development machine, and its tests run
  with two build jobs for that reason.

**The owner's question** of 2026-11-13: can Anima Engine make OxideSwarm grow and be used by
more people?

**Reading.**

- **As work for the grid: yes, by its shape.** A comparison under its contract needs a set of
  seeds for a control and for a treatment. Those runs do not depend on each other, which is
  what the grid places ([design 0045](0045-distributed-training.md), D1). A run ends in a
  checksum, so a second worker can repeat a run and compare. That is a check of results which
  the grid lacks for other work.
- **As relief for its own development: yes.** A heavy build and heavy tests are the case of
  direction 1: an agent on one machine builds and tests on a stronger one.
- **As a reason for other people to install OxideSwarm: not today.** It has no release and few
  users of its own, and its README does not call it ready for experiments. That a world which
  many machines evolve together would draw people is a guess.
- **It needs no partner and no stranger's machine.** A first run for science with an outside
  group needs both.

**The trial (APP-4).** On the owner's two machines, with no change to either repository:

1. The runner that needs no window is built on each machine. The record says on which
   operating systems it builds.
2. One manifest runs with one seed on both machines, in deterministic mode on the CPU. The
   two checksums are compared.
3. A sweep of seeds runs through `submit`, first on one worker and then on both. The record
   holds the runs finished per hour in each case.

**What it decides.** If the checksums of step 2 differ, a repeat on a second worker cannot
check a result by equality, and the record says so. If step 3 works, Anima Engine is the first
workload of the owner's own with a record, and one run becomes a guide of ADOPT-4. Whether it
becomes a public showcase is Q52.

**Limits.** It is a fourth project for one owner. The grid adds nothing to the science of the
simulation: by its own README, the experiment tooling is not a scientific result.

### D6 The owner's other projects, and what the grid does with CPU and GPU

**The owner's question** of 2026-11-13: which of the owner's projects can contribute what, and
can the grid use the whole of each machine, GPU and CPU?

**Basis.** The names and descriptions of the owner's public repositories on GitHub were read
on 2026-11-13. Their code was not read, except for OxideSwarm, LIVA and Anima Engine. A role in
the table is taken from a description unless a design record is named.

| Role | Projects | What each could give |
|---|---|---|
| Work for the grid | Anima Engine | Sweeps of experiments, one run for each seed (D5). CPU, with the GPU as an option |
| | `terra_forge`, `pygmalion-mcp` | 3D terrain and characters made through Blender, one asset for each task. A worker already reports whether it has Blender |
| | Machine-learning experiments, most of them not public | If their runs do not depend on each other, they are the workload of [design 0045](0045-distributed-training.md) (D1, D6). Not read |
| A reason to install a worker | LIVA | A desktop assistant that needs a model on a strong machine ([design 0061](0061-first-application-model-tunnel.md)) |
| | The study tools: `polaris-mcp`, `aurora-slides-mcp`, `vega-mcp`, `orion-mcp`, `mira-mcp`, `rigel-mcp`, `sirius-mcp` | Direction 2 (D2) |
| The side that calls the grid | `Genius` | Its README calls it a multi-agent framework: a sender of tasks. Not read further |
| | `constellation-mcp` | It registers MCP servers into coding agents, so it could register `oxideswarm mcp` as well |

Coursework and web applications are left out: they need no second machine. Repositories whose
names alone suggest a simulation were not read and are left out as well.

**What the grid does with CPU and GPU today**, read from the code of this branch on 2026-11-13:

- **A worker reports** its CPU cores, its RAM, whether it has a GPU, the GPU's name, its video
  memory, whether it has CUDA and whether it has Blender (`WorkerCapabilities`,
  `crates/core/src/capabilities.rs`).
- **A task states** the cores and the RAM it needs and whether it needs a GPU
  (`TaskRequirements`, `crates/core/src/task.rs`).
- **The scheduler uses both.** A GPU task never goes to a worker without a GPU. While GPU work
  waits or is likely, CPU tasks prefer workers without a GPU, and a GPU worker keeps slots
  free. With no GPU work around, a GPU worker gives its cores to CPU tasks
  (`crates/master/src/scheduler.rs`).

**What is missing before the whole of a machine is used.**

- GPU work other than the benchmark: FEAT-9, Planned.
- A task cannot state the video memory it needs. Two GPU tasks on one worker can therefore
  compete for it.
- Placement by measured speed: SCHED-4 ([design 0060](0060-speed-aware-placement.md)), Planned.
- A GPU inside the sandbox is not tried on Windows or on Linux (design 0062, row 33).

**An order of workloads**, each after the record of the one before it:

1. Anima Engine's sweeps (APP-4). They run on the CPU, and the runner exists.
2. 3D assets through Blender. Workers report Blender, and one asset is one task.
3. Grading and training of models on a GPU, after FEAT-9 has a design record. Design 0045 (D6)
   names the first of them.

Which workload FEAT-9 is designed for first is Q53.

## Compatibility

Nothing changes. Both trials use release 0.1.2 as it is.

## Security

- **APP-2** runs this repository's own code on the owner's two machines, with isolation on.
  It opens no new port and uses the cluster secret that exists.
- **APP-3.** The notes hold no name and nothing else that points to a student.
- **The study tools** run under an agent on the owner's machine. They are outside the grid's
  sandbox, and this record makes no statement about their safety.

## Test plan

No code. Each trial ends with a dated entry under "Trial records".

## Work items

Sizes are the backlog's S, M and L. No item is in [BACKLOG.md](../BACKLOG.md).

| ID | Item | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|
| APP-2 | Trial: for one week the owner's coding agent verifies its changes on a second operating system through the grid (D1) | S, owner | Release 0.1.2 on two machines | This record | "Trial records" holds one line for each run, and a summary that names the item of AX-1 to AX-6 that was missed most | Planned |
| APP-3 | Trial: three to five fellow students each study one lesson with the owner's study tools while the owner watches (D2) | S, owner | The owner's study tools | This record | "Trial records" holds the notes of at least three sessions, each with the answer to "would you use it again" | Planned |
| APP-4 | Trial: experiments of Anima Engine on the owner's two machines, with a comparison of checksums and a sweep of seeds through the grid (D5) | S, owner | APP-2, and a build of Anima Engine's runner on both machines | This record | "Trial records" holds the systems on which the runner builds, the two checksums, and the runs per hour on one worker and on both | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision, unless it says "Decided". They are
listed in [ROADMAP.md](../ROADMAP.md) as Q48 to Q50, Q52 and Q53.

| ID | Question | Recommended default |
|---|---|---|
| Q48 | Are both directions followed in parallel: compute for coding agents (bet A of design 0056) and a study kit for a class? | Decided on 2026-11-12: yes |
| Q49 | Does `README.md` get a sentence that puts direction 1 first: a coding agent uses every machine its owner has? | Not yet. After APP-2 and after step 3 of D4. It is public text |
| Q50 | After APP-3, how do the study tools reach a fellow student: through the student's own agent, or through a model served from one strong machine of the class (EDU-1, LLM-9)? | Wait for the notes of APP-3 |
| Q52 | Does Anima Engine become the public showcase of the grid: a world that the machines of several people evolve together? | Not yet. After APP-4, and after Anima Engine has a release of its own |
| Q53 | Which of the owner's workloads is FEAT-9 designed for first: 3D assets through Blender, or the grading and training of models? | 3D assets through Blender, after APP-4. Workers already report Blender |

## Risks

| Risk | Answer |
|---|---|
| The week of APP-2 shows that the shared folder is enough | The record says so, and the first items of bet A are asked again |
| Fellow students do not want the study tools | Direction 2 stops. Its cost was a few hours |
| Two directions halve the owner's time | Each trial is sized S and builds nothing. After that, one item at a time, in the order the records give |
| A large vendor ships the same for its own agent | The grid serves any agent that speaks MCP, on machines the owner already has. This was checked only as far as "What was checked" says |
| "Operating system" is read as a promise | This record uses the name for a list. Q30 is unchanged |
| The same experiment gives different checksums on two machines | APP-4 finds out in its second step. The record then says that equality is not a check |
| Four projects for one owner | APP-4 is sized S, changes no code, and waits for APP-2 |

## Trial records

None yet.

## Status log

- 2026-11-12: Written at the owner's word, from four statements of the same day (see Context).
  The owner agreed to follow both directions in parallel (Q48). Nothing is built, no session
  has run, and no item is in the backlog.
- 2026-11-13: Added D5, trial APP-4 and decision Q52 at the owner's word. The owner asked
  whether Anima Engine can make OxideSwarm grow and be used by more people. D5 reads it as a
  workload of the owner's own and as a case for direction 1, and not yet as a reason for other
  people to install a worker. Its README and experiment contract were read, and no code of it
  was run.
- 2026-11-13: Added D6 and decision Q53 at the owner's word. The owner asked which projects
  can contribute what, and whether the grid can use GPU and CPU together. D6 maps the public
  repositories by their descriptions, and states from the code what a worker reports, what a
  task can ask for, and what is still missing (FEAT-9, video memory for a task, SCHED-4).
