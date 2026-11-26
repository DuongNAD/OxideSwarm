# 0111: One project, several coding agents and machines: a shared plan, path leases and a verify gate

Status: Draft
Created: 2026-11-26    Updated: 2026-11-26

## Summary

Several coding agents work on one repository at the same time, each in its own git worktree, and the
grid's machines check their work. This record adds the coordination layer that bet A of
[design 0056](0056-frontier-program.md) lacks:

- a **shared plan** in the repository (`.oxideswarm/plan.toml`) that every agent and person reads, with a
  snapshot of about 30 lines that an agent loads at the start of each task (TEAM-1);
- **task claims and path leases** stored as git refs, so two agents never edit the same files and a
  task starts only when the tasks it depends on are merged (TEAM-2);
- a **verify gate**: one MCP call syncs the agent's worktree to the right machines, runs the task's
  checks there, and returns a short JSON list of failures (TEAM-3);
- a **circuit breaker** that stops an agent after three failed verifies with the same failure and calls
  a person (TEAM-4);
- a **merge flow** in which the pull request carries the verify table and a person merges (TEAM-5).

Git stays the single source of truth. Agents do not message each other. The first version needs no wire
change.

## Context

- **What the grid already gives an agent.** `oxideswarm mcp` (MCP `2024-11-05`) has the tools `status`,
  `workers`, `submit`, `task_result`, `cancel`, `doctor` and `workspaces` (`crates/cli/src/mcp.rs`).
  `submit --matrix` fans one command out per operating system (AX-1, [design 0101](0101-matrix-fanout.md)),
  `shard` splits a test run over workers (AX-2, [design 0103](0103-ax2-sharded-runs.md)), workspaces keep
  a synced tree and a warm `target/` per worker ([design 0039](0039-agent-workspaces.md)), placement
  selects machines by `os:`, `arch:`, `tool:`, `iso:` and user tags, and `submit --verify` re-runs a task
  elsewhere and compares outputs (VERIFY-1). Live output and MCP tasks are designed in AX-3
  ([design 0109](0109-ax3-live-output-and-mcp-tasks.md), draft).
- **What the grid does not give.** Nothing says which agent works on what, which files it may change,
  which task must wait for another, or when an agent should stop and ask a person. One `submit` returns
  the head and tail of the output; an agent reads a long log to find the first error.
- **How this project is built today.** Three Gemini agents and one Claude coordinator work in parallel on
  three machines. They coordinate through a Google Drive hub (`scripts/agent_bridge.py`): create-only id
  claims, `activity --files --overlap` to spot shared files, mailboxes, and day queues
  (`PROMPTS_<date>.md`, often 100 lines and more). Pull requests need a peer "reviewed at <sha>" comment;
  the coordinator merges through a squash train. What went wrong there is the evidence for this design:
  - agents reported work as done or verified when it was not (2026-11-07, 2026-11-11), so a check must
    come from a machine, not from an agent's text;
  - two agents changed the same files in parallel, which cost merge conflicts in `docs/BACKLOG.md`,
    `CHANGELOG.md` and test files (for example the conflict resolved on PR 395, 2026-11-25);
  - day queues on Drive and `docs/BACKLOG.md` (3,364 lines on 2026-11-26) are too long to load into an agent's
    context each time, and messages between agents cost tokens and drift;
  - a test that failed on one system was retried by an agent many times before a person saw it.
- **Measured on 2026-11-26 (friend trial, [design 0110](0110-tools-on-kit-machines.md)).** A worker on the
  same LAN fetched an 8 MiB blob at 9.3 MB/s; the same worker forced through the public n0 relay moved a
  50 MB input in 431 s (about 0.12 MB/s) at a round-trip time of about 460 ms, while small tasks still
  returned in 0.6 to 0.7 s. A Windows worker builds with the GNU Rust toolchain, where a debug build of
  this project failed to link (`export ordinal too large`) although CI's MSVC build passes. A verify
  result must therefore say which worker, link and toolchain produced it.

## Goals

- G1. One shared, versioned plan per project that people and agents read and change only through pull
  requests.
- G2. An agent learns the state of the whole team from a snapshot of at most 30 lines, without reading
  other agents' messages.
- G3. Two agents never hold overlapping file paths at the same time, and a task cannot be claimed before
  the tasks it depends on are merged.
- G4. One call verifies an agent's uncommitted worktree on the machines a task names and returns at most
  4 KB of structured failures.
- G5. An agent that fails the same way three times stops and a person is told.
- G6. A person merges every pull request; the pull request shows which machines verified which commit.
- G7. No wire change for the first version; any 0.4.x master serves it.

## Non-goals

- Agents merging, approving pull requests, or changing the plan's architecture decisions.
- A chat or message bus between agents.
- Replacing GitHub (or another host's) pull requests, reviews or CI.
- Running the agents themselves on the grid (that is AX-6, agent sessions).
- Security against a hostile agent: leases are coordination, not access control (see Security).

## Design

### D1 The shared plan (TEAM-1)

`.oxideswarm/plan.toml` at the repository root is the shared plan. People write it; agents read it and
change only the `status` of their own task, in the task's pull request.

```text
[project]
base = "master"                     # branch that tasks start from and merge into
contracts = ["crates/core/src/protocol.rs", "docs/reference.md"]   # shared definitions

[profile.unit]                      # a verify profile (D4)
command = ["cargo", "test", "--locked", "--lib"]
matrix  = { os = ["macos", "windows", "linux"] }
require = ["tool:cargo"]

[[task]]
id          = "T-12"
title       = "Count cancelled tasks apart from failed ones in status --json"
status      = "open"                # open | review | done | dropped
depends_on  = ["T-9"]
owned_paths = ["crates/master/src/queue.rs", "crates/cli/src/cli_app/tasks.rs"]
verify      = ["unit"]
hours       = 2
person      = "<owner>"             # the human who answers for it
notes       = "Keep the JSON field names of design 0040."
```

1. **One wave at a time.** The file holds the active wave (about 30 tasks at most); longer lists stay in
   the project's backlog, which tasks link to. A task is sized to one to two hours of agent work.
2. **Status in git.** `claimed` and `blocked` are not statuses in the file: they live in leases (D3). A
   task's pull request sets its own status to `done`; merging it is what marks it done.
3. **Contracts.** A task whose `owned_paths` include a contract path needs the person's review before
   any other task may claim paths that read it; the snapshot flags it.
4. **`oxideswarm plan check`** validates the file: unknown dependencies, cycles, two open tasks whose
   `owned_paths` overlap, profiles that name unknown tags. A CI job runs it on every pull request.

### D2 The snapshot (TEAM-1)

`oxideswarm plan status` and the MCP tool `plan_snapshot` print at most 30 lines, built from the plan on
the base branch, the lease refs (D3) and the last verify records:

```text
base master@fc95e4c9  contracts: protocol.rs@3b1e…  reference.md@91c0…
T-9   done     merged #470
T-12  claimed  agent-win (lease until 06:40Z)  verify 2/3 fail: test queue::tests::cancel_counts
T-13  open     needs T-12
T-14  blocked  agent-mac (3 x same failure)  -> person <owner>
recent merges: #470 T-9, #468 T-7
```

An agent calls it when it starts and before it claims; it never needs another agent's messages.

### D3 Task claims and path leases as git refs (TEAM-2)

A lease is the ref `refs/oxideswarm/lease/<task-id>` on the project's remote. It points to a commit (no
tree changes) whose message holds the lease as TOML: holder (agent id and person), `owned_paths`, the
worktree branch, `expires_at`, and the verify counters of D5.

1. **Claim** (`task_claim`, MCP and CLI): read the plan on the base branch; refuse when a dependency is not
   `done`, when the task is not `open`, or when any live lease's paths overlap the task's paths
   (prefix-based, conservative: `a/**` overlaps every path below `a/`). Then
   `git push origin <lease-commit>:refs/oxideswarm/lease/<id>` without force. Git refuses to create a ref
   that exists, so of two agents that claim at once exactly one wins, on any git host.
2. **Worktree.** The tool then runs `git worktree add ../wt-<id> -b team/<id> origin/<base>` (the location
   is configurable) and returns its path.
3. **Renew.** Every verify, and `task_renew`, pushes a new lease commit whose parent is the current one: a
   fast-forward, so only the holder that has the current commit renews. Default lease 45 minutes.
4. **Expiry and takeover.** After `expires_at`, another agent may replace the lease with
   `git push --force-with-lease=refs/oxideswarm/lease/<id>:<old-sha>`; the old holder's next renew then
   fails and it must stop.
5. **Release.** `task_release` deletes the ref; the merge flow (D6) deletes it when the task is `done` on
   the base branch.
6. **Path discipline.** The verify gate (D4) and a CI job (`oxideswarm plan check-diff <base>..<head>`)
   refuse a diff that touches a path outside the task's `owned_paths`. Shared files that every task
   touches (a changelog, a backlog index) are owned by a separate "bookkeeping" task of the person, so
   agents never conflict on them.

Q100 asks where leases live first. Default: git refs, because they work today on any host, survive a
master restart, are visible to people (`git ls-remote origin 'refs/oxideswarm/*'`), and need no wire
change. The master can take them over later (a protocol bump) if refs prove too slow or too coarse.

### D4 The verify gate (TEAM-3)

`verify` (MCP) and `oxideswarm verify --task <id>` (CLI) check the agent's current worktree, committed or
not:

1. **Sync.** The worktree syncs to a workspace named `<project>-<task>` on each selected worker
   ([design 0039](0039-agent-workspaces.md)): only changed files travel, and `target/` stays warm. With
   AX-4 the workspace becomes a fork of the base workspace.
2. **Run.** Each profile named by the task runs as a matrix (AX-1) or shards (AX-2) with the profile's
   `require` tags. The gate prefers workers with a direct link (`link:` tag) when the sync is large, and
   reports an estimate when only relayed workers match (0.12 MB/s was measured over the public relay).
3. **Reduce.** The result is one JSON object of at most 4 KB:

   ```text
   {"task":"T-12","commit":"<sha or worktree hash>","ok":false,
    "cells":[{"profile":"unit","os":"windows","worker":"msi","link":"direct","toolchain":"rustc 1.96.0 x86_64-pc-windows-gnu",
              "exit":101,"secs":48.2,"task_id":"<id>",
              "failures":[{"kind":"test","name":"queue::tests::cancel_counts",
                           "file":"crates/master/src/queue.rs","line":812,
                           "message":"assertion `left == right` failed: left: 2, right: 1"}]}],
    "omitted":0}
   ```

   Failures come from `cargo`'s JSON diagnostics (`--message-format=json`) for compile errors and from
   libtest or nextest output for tests; at most five per cell, each message at most three lines. Anything
   else stays in the task's full output, which `task_result <task_id>` returns on demand.
4. **Record.** Each verify is recorded (worker ids, task ids, commit, toolchain) for the pull request table
   of D6 and renews the lease.

### D5 Circuit breaker (TEAM-4)

1. A failure fingerprint is the hash of the first failure's kind, file and normalised message.
2. The lease commit counts consecutive failing verifies per fingerprint. At three, the gate marks the
   lease `blocked`, refuses further verifies of the task, and notifies the task's person (the hub
   mailbox for this project today; a pull request or issue comment where the project uses one).
3. `oxideswarm plan unblock <id>` (a person) clears it. A per-agent cap on verifies per hour (default 20)
   bounds the grid time one agent can take.

### D6 Merge flow (TEAM-5)

1. The agent opens a pull request from `team/<id>`. The tool writes its body: the task, the verify table
   (one row per cell: worker, OS, link, toolchain, result, task id) and the base commit verified.
2. A person reviews and merges; agents never merge. Before the merge the gate re-verifies on the latest
   base (a merge queue or the project's train), so a green table is never stale.
3. The pull request sets its task to `done`; once merged, the snapshot unblocks dependent tasks and the
   lease is deleted.

### D7 This project's own process

The first user is this repository. The active wave moves from the Drive hub's day queues into
`.oxideswarm/plan.toml`; the hub keeps what must not be in git (machine-local facts, owner messages, id
claims through `agent_bridge.py claim`). One wave runs both ways side by side before the day queues stop.

## Compatibility

- No wire change: the plan, leases and records are files and refs in git; verify uses `submit`,
  `--matrix`, `shard` and workspaces through the existing client messages. Any 0.4.x master and worker
  serve it.
- New MCP tools: `plan_snapshot`, `task_claim`, `task_renew`, `task_release`, `verify`. Existing tools do
  not change. With AX-3, `verify` becomes an MCP task that the agent polls.
- A repository without `.oxideswarm/plan.toml` sees no change.

## Security

- **Leases are coordination, not access control.** Anyone with push rights can delete a ref. Path
  discipline is enforced where it matters: the CI check of D3.6 and the person who merges.
- **Test output is untrusted input to an agent.** The failure JSON quotes compiler and test text, which a
  malicious change could shape into instructions. The tool labels it as data, truncates it, and never
  passes it to a shell.
- **Other people's machines.** A profile may require `iso:fs` (or stronger) for workers that are not the
  person's own; the friend machines of design 0110 then run agents' code only in isolation.
- **No secrets** in the plan, lease commits or verify records; the cluster secret stays where design
  0048 puts it.

## Test plan

- T1. Unit: plan parsing, cycle and overlap detection, prefix overlap of path globs, fingerprints, the
  4 KB cap and the failure parsers on recorded `cargo` JSON and libtest output.
- T2. Integration with a local bare repository: two clients claim the same task at once (exactly one
  wins), a non-holder cannot renew, an expired lease is taken over and the old holder's renew fails,
  release deletes the ref.
- T3. Integration on a loopback cluster: a worktree with a failing test returns the expected JSON; the
  third identical failure blocks the task; `plan unblock` clears it.
- T4. Live: three agents on three machines run one wave of six tasks with two dependency chains. Record
  conflicts (expected 0), verify round trips, tokens used per task, and compare with a wave run through
  the hub's day queues.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TEAM-1 | Shared plan file, `plan check`, `plan status` / `plan_snapshot` | `crates/cli/src/plan.rs` (new), `crates/cli/src/mcp.rs`, `docs/reference.md` | T1 (plan parts) passes; the snapshot of this repository's first wave fits 30 lines | Planned |
| TEAM-2 | Task claims and path leases as git refs, worktrees, `plan check-diff` | `crates/cli/src/plan/lease.rs` (new), CI workflow | T2 passes | Planned |
| TEAM-3 | Verify gate with compact failure JSON | `crates/cli/src/verify_gate.rs` (new), `crates/cli/src/mcp.rs` | T3 (verify part) passes | Planned |
| TEAM-4 | Circuit breaker and notice to the person | `crates/cli/src/plan/lease.rs`, `scripts/agent_bridge.py` | T3 (breaker part) passes | Planned |
| TEAM-5 | Merge flow: pull request table, re-verify on the latest base, this project's first wave | `crates/cli`, `.oxideswarm/plan.toml` | T4 recorded | Planned |
| Q100 | Where leases live first: git refs (default) or the master | this record | The owner answers or accepts the default | Open |

## Status log

- 2026-11-26: Draft, at the owner's request after a proposal to make OxideSwarm the execution and
  verification layer for a team of coding agents with git as the source of truth. The owner added that a
  shared plan document is needed (D1, D2). Design number, TEAM-1 to TEAM-5 and Q100 claimed with
  `agent_bridge.py claim`.
