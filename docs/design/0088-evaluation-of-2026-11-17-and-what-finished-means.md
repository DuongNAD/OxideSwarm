# 0088: Evaluation of 2026-11-17 and what finished means

Status: Accepted (plan; builds nothing; the owner decided Q82 and Q83 on 2026-11-17)
Created: 2026-11-17    Updated: 2026-11-17

## Summary

This record scores the project on ten criteria at `master` `16b2a38`, after release `0.4.0`. It
then states one exit check per criterion. Together the ten checks are what "finished" means for
the purpose of Q1 in [ROADMAP.md](../ROADMAP.md) (Q82, decided on 2026-11-17). Most of the work that
reaches them already has an item. Three new items are proposed, and nine items that exist only in
other design records are named for the backlog. Eight of the ten checks need a step that only the
owner can take. Nothing in the product changes in this record.

## Context

### Why this record exists

[ROADMAP.md](../ROADMAP.md) orders the work, and the Index of [BACKLOG.md](../BACKLOG.md) has 166 rows on
2026-11-17, 47 of them Planned and 13 Partial. Neither document says at which point the project is finished. The
phases continue through scale out, an ecosystem and an open compute network, and 41 decisions for
the owner are open (37 in the table of the roadmap, and Q60 to Q63 in
[design 0067](0067-multi-node-llm-serving-and-admission.md)). A reader cannot tell the work that
completes the project from the work that grows it.

### Method

The evaluation was made on 2026-11-17 by a reviewing session that read the repository at
`origin/master` `16b2a38`, the CI runs through the `gh` command, and the public counts of the
repository. It ran no test and no cluster. A score is the reviewer's judgment on a scale of 0 to
10. A number in the evidence column is a count, and "Test plan" below gives the command that
produced it.

### Scores

| # | Criterion | Score | Evidence |
|---|---|---|---|
| 1 | Architecture and design | 8 | Four crates with one shared contract (`crates/core`). A versioned wire protocol with a one-version window ([design 0083](0083-one-version-window.md)). 81 design records |
| 2 | Documentation | 8.5 | `README.md` states its limits (delivery at least once, one master). `docs/BACKLOG.md` has 3,629 lines, and the text of its 106 Implemented items stays in it |
| 3 | Tests and CI | 7 | 2,411 test attributes and 99,994 lines of test files. CI on Linux, macOS and Windows, three fuzz targets, a release skew job. Of the last 60 runs for pushes on `master`, 35 passed, 13 failed and 12 were cancelled |
| 4 | Security | 7 | A secret handshake and ChaCha20-Poly1305 records (`crates/core/src/cluster_auth.rs`), keys per machine (`crates/core/src/noise_auth.rs`), task isolation on three systems (`crates/worker/src/isolation.rs`). No review by a person outside the project. 290 lines contain `unsafe`, 125 of them in `isolation.rs` |
| 5 | Releases and operations | 7 | Six releases since 2026-11-09, each with archives, a changelog and upgrade notes. One master and no failover. No run across two real networks is recorded |
| 6 | Code maintainability | 6.5 | 177,133 lines under `crates/*/src`. A CI check caps the production part of a source file at 3,000 lines (MAINT-1), and three files are on its allowlist. 100 `#[allow(...)]` attributes, 44 of them `clippy::too_many_arguments` |
| 7 | Performance and scale | 6.5 | Scripted numbers in [benchmarks/grid](../../benchmarks/grid/README.md): 1000 simulated workers on loopback, and a two-machine offload. TEST-24 and TEST-25 are Planned, so no record shows a large transfer between machines or a run of 24 hours ([design 0065](0065-large-transfer-and-long-run-tests.md)) |
| 8 | Feature completeness | 6 | The path of command tasks, files, workspaces and MCP is Implemented. Bet A of [design 0056](0056-frontier-program.md), the work that serves the purpose of Q1 most directly, has shipped in no release. The Android app does not build (AND-1), and the model tier of the 3D pipeline is a stub |
| 9 | Stewardship | 4 | 1,235 commits in 59 days, all by one person with coding agents, apart from 3 by a dependency bot. The release runbook with a second releaser (LV-15) and the governance files (LV-14) are Planned |
| 10 | Users | 2.5 | 6 stars, 0 forks and 0 open issues on 2026-11-17. No session of the study of first use has run (ADOPT-1, Q37) |

The first seven criteria describe the engineering, and the last three describe the project as
something other people use. The gap between the two groups is the main finding.

### What the CI runs show

The 60 runs cover 2026-11-15 to 2026-11-17 at 09:45 UTC.

- 2026-11-16: 18 passed, 10 failed and 12 were cancelled. The section "Stability" of the roadmap
  already describes these runs.
- 2026-11-17: 9 passed and 2 failed. Neither failure is a failing test. In run `44036878952` the
  job `fuzz (tree_manifest)` could not download the pinned nightly toolchain. In run `44034218213`
  the test step of `workspace tests (windows-latest)` ended three minutes after the job started,
  with exit code `-1073741502`.
- The cancelled macOS jobs of 2026-11-16 have no item, as the roadmap notes.

## Goals

- G1: One page states what "finished" means, as checks that a reader can run or look up.
- G2: Each check names the items that reach it and who holds the next step.
- G3: Work without an item is named, with a proposed item.
- G4: The work outside "finished" is listed, so it is not counted as open work for completion.

## Non-goals

- This record adds no row to the BACKLOG Index. The owner accepted Q82 on 2026-11-17, so the
  items of D4 enter the backlog in a change of their own, each with files and a check.
- It reopens no decision the owner has taken, and it moves no phase of the roadmap.
- It is not a second roadmap ([ROADMAP.md](../ROADMAP.md) stays the only one, DOC-10). It orders
  only the items inside the stop line of D1.
- The scores are not a measure to optimise. The exit checks are.

## Design

### D1 The stop line

The purpose is decision Q1: a coding agent runs builds and tests on the machines its owner already
has. The project is finished when the ten exit checks of D2 hold. Work that no check needs is
growth. It keeps its place in the roadmap and is not counted as open work for completion:

| Outside the stop line | Items |
|---|---|
| Scale out, Phase F | SCALE-3 to SCALE-6 |
| Product and ecosystem, Phase G | ECO-1, ECO-3, ECO-5 to ECO-8, REAPI-1. ECO-2 is CACHE-1 and is inside the line |
| An open compute network, Phase H | NET-1 to NET-8 |
| Bets B to E of design 0056 | The items of those bets |
| Language models and training | Designs 0038, 0045, 0061, 0062 and 0067, and release `0.5.0` |
| The Android app | AND-1 to AND-4 |
| 3D and the Agent Mesh | 3D-2, 3D-3, MESH-2, MESH-3 |
| Checkpoints for process tasks and a GPU kernel | FEAT-5, FEAT-9 |

`README.md` already states the limit that each of these leaves (its platform table and its table
of limits). A single master stays a stated limit: `--state-dir` makes a restart lossless, and
failover is SCALE-4.

### D2 Exit checks

| # | Criterion | Exit check | Items | Next step is held by |
|---|---|---|---|---|
| E1 | Architecture and design | Holds today. Kept by the rules that exist: a design record per change, and the wire layout tests (`crates/core/src/wire_layout_tests.rs`) | None | Nobody |
| E2 | Documentation | `docs/BACKLOG.md` shows the open items inside the stop line first, and the text of Implemented items is in an archive file. No decision that an item inside the stop line needs is open | DOC-11 (new) | Agents, then the owner (the decisions that those items need, such as Q57 and Q59) |
| E3 | Tests and CI | The exit check of Q80 in the roadmap: every flaky test has an Implemented item with its loop of runs, TEST-24 and TEST-25 each have one dated record, and `master` has seven days of runs for pushes in which no test fails. In the same seven days no job is cancelled, or a record names what cancels them | TEST-23, TEST-29, TEST-32, TEST-41 to TEST-47, TEST-24, TEST-25, CI-1 (new) | Agents. The owner for TEST-24 and TEST-25 (Q57) |
| E4 | Security | Every parser of untrusted bytes has a fuzz target in CI (measure M10 of [design 0058](0058-lasting-value.md)). Every `unsafe` site has a `SAFETY:` comment and the lint is a required check (M11). Release archives carry provenance that verifies (M12). `members revoke` closes every lane of a member (ENROLL-1c, [design 0051](0051-machine-identity-and-invites.md)). SEC-16 has a cause on record | LV-1, LV-4, LV-5, ENROLL-1c, SEC-16 | Agents. The owner for LV-5 and SEC-16 |
| E5 | Releases and operations | One run across two real networks is recorded for the link report and for the USB kit | LINK-1, USB-1c ([design 0053](0053-usb-setup-kit.md)) | The owner (a second network) |
| E6 | Code maintainability | `scripts/rust-file-size-allowlist.txt` has no entry | MAINT-2 (new) | Agents |
| E7 | Performance and scale | The record of TEST-25 shows that memory, handles and disk level off over 24 hours, and the record of TEST-24 shows a file of 8 GiB arriving intact between two machines | TEST-24, TEST-25 | The owner (Q57, Q59) |
| E8 | Feature completeness | A release ships AX-1, AX-2, AX-3 and CACHE-1, with a record of one call from a coding agent that tests a change on macOS, Windows and Linux. The Android phone of lighthouse run L1 is outside this check (D1) | AX-1 to AX-3, CACHE-1 | The owner first (ADOPT-1 comes before AX-1, Q37) |
| E9 | Stewardship | Proof run V3 of design 0058 is recorded: a second person cuts a release from the runbook alone, and its archives verify. The governance files exist | LV-14, LV-5, LV-15 | The owner (a second person) |
| E10 | Users | The second round of the study of first use is recorded with measures M15 to M17 of [design 0059](0059-first-users.md), and at least one participant ran a task of their own again within 14 days | ADOPT-1 to ADOPT-4 | The owner (Q37, decided on 2026-11-11; no session has run) |

An independent security review (LV-6) is not part of E4, because it depends on people and money
outside the project (Q34). Without it, the security of the grid rests on the project's own tests.
`docs/SECURITY.md` should say so until a review is published.

### D3 Order

The order follows the roadmap. The three parts of step 1 have no dependency on each other and
start now.

1. In parallel:
   - The open test items of E3, and CI-1. Agents hold them, and they are the content of the
     stability release (Q80).
   - The first round of ADOPT-1. The owner holds it, it needs nothing built, and its result may
     reorder every later step. It is the longest wait on the path: E8 and E10 both start from it.
   - LV-14 and DOC-11. Both are small and change no code.
2. TEST-24, then TEST-25, on the owner's machines (Q57). They close E7 and the last part of E3.
3. The stability release, when the exit check of Q80 holds.
4. The items that are in a design record only enter the backlog, each with files and a check (D4).
5. AX-1 to AX-3 and CACHE-1, then the release that carries them and the record of E8.
6. LV-4 and MAINT-2, beside step 5. `crates/worker/src/isolation.rs` is in both: it is the largest
   production file and holds the most `unsafe` lines, so one pass serves E4 and E6.
7. LV-5, then LV-15 with a second person (E9), and the two-network run of E5.
8. The second round of ADOPT-1 (E10).

Release `0.5.0`, language model serving, is outside the stop line. The items of step 5 may start
before it ships, as soon as the first round of ADOPT-1 is recorded (Q83, decided on 2026-11-17).

### D4 Work without an item

Three items are new. Their texts are in "Work items" below.

Nine items exist in a design record and in no row of the BACKLOG Index. A check of D2 needs each
of them:

| Items | Where their text is | Check |
|---|---|---|
| ADOPT-1 to ADOPT-4 | [Design 0059](0059-first-users.md) | E10 |
| AX-1 to AX-3 | [Design 0056](0056-frontier-program.md) | E8 |
| LV-4, LV-15 | [Design 0058](0058-lasting-value.md) | E4, E9 |

ENROLL-1c is named in the status log of the backlog and has no row of its own; it is part of
ENROLL-1, which is Partial.

### D5 How this record is kept

The scores are a snapshot and are not updated. When a check of D2 holds, a line in the status log
below names the record or the run that shows it. The record moves to Implemented when all ten
lines exist.

## Compatibility

None. No wire format, flag, port or default changes.

## Security

None. The record adds no input and no code. E4 lists the open security work.

## Test plan

The counts of "Scores" come from these commands, run on a development machine in a checkout of
`master`:

```bash
git ls-files 'crates/*/src/*.rs' | xargs cat | wc -l                      # lines under crates/*/src
git ls-files 'tests/*.rs' 'crates/*/tests/*.rs' | xargs cat | wc -l       # lines of test files
git grep -cE '#\[(tokio::)?test' -- crates tests | awk -F: '{s+=$NF} END{print s}'   # test attributes
grep -rn 'unsafe ' --include='*.rs' crates/*/src | wc -l                  # lines that contain `unsafe`
grep -rnE '#!?\[allow\(' --include='*.rs' crates | wc -l                  # allow attributes
git rev-list --count HEAD                                                 # commits
gh run list --branch master --workflow CI --event push --limit 60 --json conclusion,createdAt
gh repo view --json stargazerCount,forkCount
```

Each exit check of D2 names its own evidence: a record with a date, a file, or a count of CI runs.

## Work items

The three items are in [BACKLOG.md](../BACKLOG.md) since 2026-11-17, as part of its Phase 13, with
their full text. The backlog is the place where their status is kept.

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| CI-1 | Keep runs for pushes on `master` from ending red or cancelled without a failing test | `.github/workflows/ci.yml`, `docs/guides/multi-agent-collaboration.md`, a new `scripts/ci_push_runs.py` | The "Done when" of CI-1 in [BACKLOG.md](../BACKLOG.md): a rule that no agent cancels a run for a push, a script that classes each run that is not green, and a cause or a retry for the two failures of 2026-11-17 (runs `44036878952` and `44034218213`) | Planned |
| MAINT-2 | Bring the last three files under the size cap | `crates/worker/src/isolation.rs` (5,346 production lines), `crates/cli/src/mcp.rs` (3,692), `crates/cli/src/mode_cli.rs` (3,240), `scripts/rust-file-size-allowlist.txt` | The allowlist has no entry, `ox-mode test size` passes, and `crates/core/tests/data/wire_layout.txt` is unchanged | Planned |
| DOC-11 | Make the backlog show open work first | `docs/BACKLOG.md`, a new `docs/BACKLOG-ARCHIVE.md` | The Index marks which open items are inside the stop line of D1. The text of every Implemented item is in the archive file, and each Index row still links to its text. Every relative link resolves (`ox-mode doc verify`) | Planned |

## Decisions

The owner decided both in chat on 2026-11-17, at the recommended default.

| ID | Question | Decision |
|---|---|---|
| Q82 | Are the ten exit checks of D2 the meaning of "finished", so that the items of D4 enter the backlog and the work of D1 is not counted as open work for completion? | Yes, decided by the owner on 2026-11-17 |
| Q83 | May AX-1 to AX-3 and CACHE-1 start as soon as the first round of ADOPT-1 is recorded, whether or not release `0.5.0` has shipped? | Yes, decided by the owner on 2026-11-17. They serve the purpose of Q1, and no item of `0.5.0` is in the backlog yet. The release numbers follow what merges (design 0076, D1) |

## Status log

- 2026-11-17: Draft written from an evaluation of `master` `16b2a38`. No exit check beyond E1 holds.
- 2026-11-17: The owner decided Q82 and Q83 at the recommended default. Status set to Accepted.
  The items of D4 and the three new items are not in the BACKLOG Index yet.
- 2026-11-17: The twelve items of D4 entered [BACKLOG.md](../BACKLOG.md) as Phase 13, all Planned.
  The cause of the cancelled runs of 2026-11-16 was read from the job annotations while CI-1 was
  written: the owner's account cancelled twelve runs, and no hosted macOS runner acquired the jobs of
  six others. CI-1 now asks for a rule and a script instead of the search.
