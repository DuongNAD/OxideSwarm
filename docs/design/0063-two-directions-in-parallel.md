# 0063: Two directions in parallel: compute for coding agents, and a study kit for a class

Status: Draft (plan; builds nothing; the owner decided Q48 on 2026-11-12; Q49 and Q50 are open)
Created: 2026-11-12    Updated: 2026-11-12

## Summary

The owner asked which part of the projects takes a road of its own and would be chosen by other
people, and on 2026-11-12 agreed to follow two directions side by side (Q48). Direction 1 is
bet A of [design 0056](0056-frontier-program.md): a coding agent uses every machine its owner
has. Direction 2 is a study kit for a class. This record gives each direction one trial that
needs nothing built (APP-2, APP-3). It adds no feature.

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
- A change to the owner's study tools or to LIVA. They are other repositories.

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

Both directions give a person a reason to install a worker. A pool for science needs that
before anything else. The pool itself is Q13 B, and it still waits for real users and a design
of its own.

### D4 Order

1. APP-2 can start now. It needs release 0.1.2 on the owner's two machines.
2. APP-3 can start now. It needs the owner's machine and three to five fellow students.
3. Before Q49: the neighbours of direction 1 are read, which are self-hosted CI runners and
   hosted sandbox services, and whether a vendor's sandbox places work on a second machine.
4. Build items follow the trial records. Each enters the backlog when the owner accepts it.

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

## Decisions for the owner

The right column is a recommended default, not a decision, unless it says "Decided". They are
listed in [ROADMAP.md](../ROADMAP.md) as Q48 to Q50.

| ID | Question | Recommended default |
|---|---|---|
| Q48 | Are both directions followed in parallel: compute for coding agents (bet A of design 0056) and a study kit for a class? | Decided on 2026-11-12: yes |
| Q49 | Does `README.md` get a sentence that puts direction 1 first: a coding agent uses every machine its owner has? | Not yet. After APP-2 and after step 3 of D4. It is public text |
| Q50 | After APP-3, how do the study tools reach a fellow student: through the student's own agent, or through a model served from one strong machine of the class (EDU-1, LLM-9)? | Wait for the notes of APP-3 |

## Risks

| Risk | Answer |
|---|---|
| The week of APP-2 shows that the shared folder is enough | The record says so, and the first items of bet A are asked again |
| Fellow students do not want the study tools | Direction 2 stops. Its cost was a few hours |
| Two directions halve the owner's time | Each trial is sized S and builds nothing. After that, one item at a time, in the order the records give |
| A large vendor ships the same for its own agent | The grid serves any agent that speaks MCP, on machines the owner already has. This was checked only as far as "What was checked" says |
| "Operating system" is read as a promise | This record uses the name for a list. Q30 is unchanged |

## Trial records

None yet.

## Status log

- 2026-11-12: Written at the owner's word, from four statements of the same day (see Context).
  The owner agreed to follow both directions in parallel (Q48). Nothing is built, no session
  has run, and no item is in the backlog.
