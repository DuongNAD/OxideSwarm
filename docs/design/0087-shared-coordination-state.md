# 0087: Shared coordination state for agents: id claims, a work board with files, shared lessons

Status: Draft (plan; Q84 to Q86 are open)
Created: 2026-11-17    Updated: 2026-11-17

## Summary

Several coding agents and Claude Code sessions work on this repository at once, through the hub
of `scripts/agent_bridge.py` (`docs/guides/multi-agent-collaboration.md`). Knowledge has durable
homes (this repository and the hub), but coordination state does not: which design number, Q id,
phase or item id someone is about to use, which files an agent is changing now, and the process
lessons one session learned. This record adds three small things to the existing hub instead of a
new memory system: id claims (D1), files on the activity record with an overlap check (D2), and
one shared place for process lessons (D3). Nothing in the grid changes.

## Context

### What happened on 2026-11-17

- **Two "Phase 13".** Two Claude Code sessions on the owner's Mac each opened a pull request that
  added `## Phase 13` to `docs/BACKLOG.md` within minutes (pull requests 292 and 293). The second was
  renumbered by hand after the first was seen.
- **Design numbers held in one session's private memory.** Numbers 0085 to 0088 were reserved for
  agent-win and a subagent in the coordinator session's own Claude memory
  (`~/.claude/projects/.../memory/`). Another session chose 0088 without a shared list to check. The
  Windows agents cannot read that memory at all.
- **The same files in parallel.** Pull requests 282, 284 and 286 each added lines under
  `## [Unreleased]` in `CHANGELOG.md` and next to each other in `docs/BACKLOG.md` and `docs/README.md`.
  After the first merged, the other two conflicted; the coordinator stacked them by hand to avoid three
  CI rounds. Nobody knew in advance that three agents were editing the same lines.
- **Lessons that stay private.** A rerun of a superseded CI run cancelled the newer run of the same pull
  request (`cancel-in-progress` in `.github/workflows/ci.yml`); a bash script edited while it ran; a
  helper started without a notification. Each lesson was written to the coordinator's private memory,
  where agent-win and agent-win-2 never see it.

### What exists

- **Repository:** `CLAUDE.md`, `docs/BACKLOG.md` (items, status log), `docs/ROADMAP.md` (decisions),
  design records, `CHANGELOG.md`. Reviewed, versioned, the source of truth. Slow to change: a pull request.
- **Hub (Google Drive, `OxideSwarm_Agent_Hub`):** `AGENT_RULES.md`, mailboxes, `nodes/<id>_state.json`
  (machine heartbeat), `nodes/<id>_activity.json` (Rule 13: state, task, step, note; `activity --show`
  marks a silent agent STALE), `test_runs/`, `incidents/`. Fast, but a synced folder with a 2 to 15 s
  delay that makes a conflicted copy when two hosts write one file (AGENT_RULES.md Rule 1). Every file
  therefore has one writer.
- **Claude memory:** per user and project on one machine, shared by the Claude Code sessions of that
  project on that machine, invisible to every other agent.

## Goals

- G1. No two agents or sessions use the same design number, Q id, BACKLOG phase number or item id.
- G2. Before an agent starts a change, it can see who else is changing the same files.
- G3. A process lesson that would have saved another agent a failure is readable by every agent.
- G4. No new service, database or credential; stdlib Python and the existing hub only. Every hub file
  keeps a single writer.

## Non-goals

- A store of project knowledge. The repository stays the source of truth; nothing here duplicates the
  BACKLOG, ROADMAP or design records.
- Locks that block work. Claims and overlap warnings inform; the merge order still settles conflicts.
- Replacing Claude memory. It keeps the owner's preferences and how the owner wants to be answered.

## Design

### D1 Id claims on the hub

A claim is one create-only file, never modified, so two hosts never write the same file:
`claims/<kind>/<value>__<agent>__<UTC timestamp>.json`, with `kind`, `value`, `agent`, an optional
`session` label, `title`, `branch`, `pr` and `created_at`.

Kinds: `design` (`0089`), `q` (`Q84`), `phase` (`14`) and `item` (`TEST-49`, by prefix).

`agent_bridge.py claim <kind> [--prefix TEST] --title "..." [--branch B] [--pr N] [--value V]`:

1. Compute the highest value in use: on `origin/master` and every `origin/*` branch (`git ls-tree` of
   `docs/design/` for `design`; the text of `docs/` for `q`, `phase` and `item`), and in every claim
   file on the hub. Without `--value`, the next free value is one above it.
2. Write the claim file.
3. Wait the settle time (default 20 s, longer than the hub's delay), list the claims of that value
   again, and keep the claim only if it is the earliest (equal times: the smaller agent id). Otherwise
   delete the own file and repeat from step 1.
4. Print the value. Exit code 0 when it holds, non-zero when it could not settle after three tries.

`claim --list [<kind>]` prints claims with their state: `in use` when the value now exists on
`origin/master`, `open` otherwise, and `stale` when open for more than 14 days with no branch on
`origin`. `claim --release <file>` deletes an own claim (a dropped plan). Only the claim's writer
deletes it.

### D2 Files on the activity record, and an overlap check

`activity` gains `--branch`, `--pr` and `--files` (comma-separated paths, or `--files-from-git` for
`git diff --name-only origin/master...HEAD` plus the uncommitted paths). The record keeps them next to
`task` and `step`.

`activity --overlap` (and the start of `activity --state active` when files are given) compares the
agent's files with the files of every other agent whose record is `active` or `blocked` and not STALE,
and prints each shared path with the other agent, its branch and pull request. Paths that many changes
touch by design (`CHANGELOG.md`, `docs/BACKLOG.md`, `docs/README.md`, `Cargo.lock`) print as
"shared, expect a one-line merge" instead of a warning. It never blocks.

### D3 One shared place for process lessons

- **Hub `AGENT_RULES.md`, new section "Lessons"**: one line per lesson, with its date, what failed and
  the rule that prevents it, for lessons about working on this hub and this repository's CI (for
  example: rerun only a run whose head is the pull request's head; count `test result: FAILED` in the
  logs of the continue-on-error `workspace tests` job; one cargo target per worktree; stop processes by
  PID; never edit a script while it runs; keep CHANGELOG lines under `[Unreleased]` after a release).
- **`CLAUDE.md`, "Conventions and pitfalls"**: only the lessons that hold for any contributor without
  the hub, such as the CHANGELOG rule.
- A session that writes a lesson to its private memory also writes it to one of the two places when
  another agent could meet the same failure. The coordinator moves the existing lessons from its
  memory in the first pass (COORD-3).

### Who uses it

Every agent that changes the repository: agent-mac, agent-win, agent-win-2 and the Claude Code
sessions on the owner's machines (`CLAUDE.md` gets two lines: claim ids with `agent_bridge.py claim`,
report files with `activity`). On a clone without a hub both commands print one notice and exit 0, so nothing breaks.

## Compatibility

Hub only; the grid, its wire format and its files are untouched. An agent on an older checkout keeps
working: `activity` records without files are read as "files unknown" by the overlap check, and the
hub's mirrored copy of `agent_bridge.py` is updated in the same step as the repository's.

## Security

Claims and file lists are project metadata: ids, titles, paths, branch names. The same text checks as
`activity` (`validate_activity_text`) apply to titles. No secret, token, host name or address enters
them (AGENT_RULES.md Rule 2). A claim cannot remove another agent's claim; deletion is by the writer only.

## Test plan

- Unit (`tests/test_agent_bridge.py`, already in CI on Linux, macOS and Windows): next free value for
  each kind from a fake repository and a temporary hub; two claims of one value with different times,
  the later one withdraws; equal times, the smaller agent id keeps it; `--list` states; activity with
  files; the overlap report, including the shared-file class.
- Live: two agents claim a design number within one second through the real hub; one ends with the next
  number. Recorded under `test_runs/`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| COORD-1 | Id claims on the hub (D1) | `scripts/agent_bridge.py`, `tests/test_agent_bridge.py`, `docs/guides/multi-agent-collaboration.md`, hub `AGENT_RULES.md` | The unit tests of D1 pass in CI, and the live run of two near-simultaneous claims is recorded | Planned |
| COORD-2 | Files on the activity record and the overlap check (D2) | `scripts/agent_bridge.py`, `tests/test_agent_bridge.py`, `docs/guides/multi-agent-collaboration.md` | The unit tests of D2 pass in CI, and `activity --overlap` names the shared file in a run with two agents | Planned |
| COORD-3 | Shared lessons (D3) | Hub `AGENT_RULES.md`, `CLAUDE.md` | The "Lessons" section holds the coordinator's existing process lessons, each with its date, and `CLAUDE.md` names the claim and activity steps | Planned |

## Decisions for the owner

| ID | Question | Recommended default |
|---|---|---|
| Q84 | Do claims live on the hub (fast, single-writer files) rather than in a file in the repository (reviewed, but a pull request per claim)? | The hub. The repository already shows a duplicate late, as a merge conflict |
| Q85 | Is claiming an id and reporting files with `activity` a rule for every agent, including the Claude Code sessions (a line in `CLAUDE.md` and a rule in `AGENT_RULES.md`)? | Yes. It costs two commands per change |
| Q86 | May the coordinator copy its process lessons from its private Claude memory into `AGENT_RULES.md` and `CLAUDE.md` (COORD-3)? They contain no personal data, only rules about CI, the hub and processes | Yes |

## Status log

- 2026-11-17: Drafted after the duplicate "Phase 13" of pull requests 292 and 293, at the owner's
  request. Q84 to Q86 are open; nothing enters the BACKLOG Index until the owner decides them.
