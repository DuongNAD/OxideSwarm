# 0059: First users (who gains what, and the order of work that serves them)

Status: Draft (plan; builds nothing; decisions Q36 to Q39 are open)
Created: 2026-11-10    Updated: 2026-11-10

## Summary

On 2026-11-10 the owner asked what a user gains from the project, and which upgrades would make
people use it. [Design 0056](0056-frontier-program.md) plans what the grid does, and
[design 0058](0058-lasting-value.md) plans why to trust it. This record states who gains
something, what each group still lacks, and the order of work that follows.

- **Premise:** a person installs the grid when a job they already have ends sooner or costs less,
  and they see that within minutes. Pooled compute alone is not such a job.
- **First audience:** a developer who works with a coding agent and owns machines with more than
  one operating system. This is the user story of [ROADMAP.md](../ROADMAP.md).
- **Order:** watch people install it (ADOPT-1), show what a run took (ADOPT-2), AX-1 and AX-2 of
  design 0056, a shorter install (ADOPT-3), guides for common jobs (ADOPT-4), then LLM-1 and LLM-6.
- **Evidence:** every statement about users in this record is reasoning until ADOPT-1 or a
  recorded run supports it.

Nothing here is built, and nothing enters the backlog before the owner adds it.

## Context

### Where the project stands

The state at `origin/master` `4a07fcb` on 2026-11-10.

| Area | State | Where |
|---|---|---|
| What a new reader is offered | Offloading `cargo test` to a second machine and the MCP server for coding agents lead the page | `README.md`, "Why OxideSwarm" |
| Measured gain | The rows compare a first run with a second run on the remote machine. No row compares a run on the grid with the same run on the user's own machine | `README.md`, "Measured"; `benchmarks/grid/README.md` |
| Install | Download an archive from the release page, compare it with `SHA256SUMS` by hand, unpack it and put the binaries on `PATH`. The binaries are not signed or notarized, and on macOS a browser download needs its quarantine mark removed. The USB kit installs with one double-click. No script downloads a release. Package managers and signing are ECO-5, Planned | `README.md`, "Install"; `packaging/usb/README.md`; [BACKLOG.md](../BACKLOG.md) |
| First result | Five steps on two machines, joined with one token | `README.md`, "Quick start"; [design 0048](0048-join-token.md) |
| Operating systems per call | `submit --os` and the MCP `submit` tool take one operating system. A check on three systems is three calls | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs` |
| What a run reports | `submit --wait` prints `Execution Time` from `TaskResult::execution_time_ms`, and the MCP result carries `duration_ms`. Neither reports how long the caller waited, and nothing sums over several tasks | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs` |
| Guides for a job | One: offloading `cargo test` | `docs/guides/offload-cargo-test.md` |
| Tools a worker reports | `cargo`, `rustc` and `git` (`HOST_PROBED_TOOLS`). Any other tool needs a user tag (`worker --tag`, `submit --require-tag`) | `crates/core/src/capabilities.rs`, `crates/cli/src/cli_app.rs` |
| Usage numbers | The binaries send no usage data. The repository holds no record of a use by a person outside the project | |

### What the earlier records already say

- The purpose in [ROADMAP.md](../ROADMAP.md): a coding agent runs builds and tests on the machines
  its owner already has.
- Design 0056, Context: hosted agent sandboxes offer Linux guests, and hosted CI runners are billed
  per minute, with macOS the most expensive. Those rows name their sources and say which were read
  at the page.
- Design 0056 D2 plans the matrix call (AX-1), shards (AX-2) and live output (AX-3). Measure M1
  there is the first comparison of several workers with one.
- ECO-5 plans package managers and signed binaries, and ECO-7 plans MCP set-ups for common agents.
- Design 0058 plans the evidence a careful person asks for before running a worker.

### What this record adds

- A table of audiences, each with the job it already has and what the grid lacks for it.
- A study of first use with a few people, before more is built.
- Three small items that no record has: a run summary, an install script with a scripted measure,
  and guides for common jobs.
- Measures M15 to M17, read without telemetry.

## Goals

- **G1** Name the audiences, the job each already has and what the grid lacks for it, and choose
  the first.
- **G2** A person who has not used the grid reaches a first result on two machines within a stated
  time (M15). The time is measured with people and by script.
- **G3** Every waited run shows how long the caller waited and what the tasks took, with no claim
  about the user's own machine.
- **G4** A user finds a guide for the job they came with, and each guide has a recorded run.
- **G5** What people did in the study sets the order of this work. The table of D1 does not.
- **G6** No number about users comes from telemetry.

## Non-goals

- A launch or marketing plan. The "Launch" section of [ROADMAP.md](../ROADMAP.md) holds the steps.
- Usage telemetry, an update check or any other call home in the binaries.
- Package managers, code signing and notarization. They are ECO-5 and need the owner's accounts.
- Payment, credits or tokens as a reason to install.
- A change to Phase E, or to the waves of design 0056 beyond what D3 states.

## Design

### D1 Audiences

Each row is a hypothesis. The last column says where it comes from.

| ID | Audience | The job they already have | What the grid gives today | What is missing | Items | Basis |
|---|---|---|---|---|---|---|
| A1 | A developer with a coding agent and machines on more than one operating system | Checking a change on Windows, macOS and Linux before pushing it | The MCP `submit` tool with an operating system, workspaces, and isolation on the three desktop systems | One call that fans out and returns one table. Live output | AX-1, AX-3 | The user story of the roadmap. Design 0056, Context |
| A2 | A developer with a slow test suite and a second machine | Waiting for the suite | Offload to one machine with a reused build directory. It frees the first machine. It is faster only when the second machine is faster | Shards over several machines with one build. A number that shows the gain | AX-2, ADOPT-2, measure M1 | `README.md`, "Measured" |
| A3 | The owner of a desktop with a GPU who works on a laptop | Using a local model away from the desk | Transport through NATs. GPU compute is a benchmark only (FEAT-9 in the roadmap's table of what waits) | A prompt routed to the worker that holds a model, and an endpoint that existing clients call | LLM-1, LLM-6 | Reasoning |
| A4 | A small team, a lab or a class with idle machines | Batch jobs that run on one machine: transcoding, rendering, transcription. A class that needs a model | Queue, retries, files in and out, the USB kit | A guide per job. Batch inference. The classroom profile | ADOPT-4, LLM-8, EDU-1 | Reasoning. Q25 asks whether a lab exists |
| A5 | A maintainer who waits for hosted CI runners or pays for them | CI on three operating systems | Nothing direct | Their own machines as runners | AX-7 | Design 0056, Context |

The first audience is A1, and A2 is the same person on another day. Reasons:

1. It is the purpose the roadmap already states.
2. The parts that exist serve it: MCP, workspaces, placement and isolation.
3. What is missing is two items of size M (AX-1, AX-2).
4. By the research of design 0056, hosted sandboxes do not offer the user's own Windows and macOS
   machines.

A3 is the second audience. It waits for LLM-1 and LLM-6, and nothing in the product serves it
today beyond transport.

### D2 What counts as adoption work

An item belongs to this record when it does one of these:

1. It shortens the time from the release page to a first result (M15).
2. It makes the gain of a run visible to the person who ran it.
3. It lets a person run the job they came with, without writing glue first.
4. It removes a reason to refuse the install. A worker executes commands that are sent to it, so a
   careful person asks who built the binary and what a task can touch. LV-1 and LV-5 of design 0058
   and worker isolation ([design 0033](0033-os-level-task-isolation.md)) answer that, and they stay
   in their records.

An item that does none of these is a feature and belongs to design 0056.

### D3 Order

| Step | Item | Why here | Where it lives |
|---|---|---|---|
| 1 | ADOPT-1, first round | It needs nothing, and its result may reorder every later step | This record. Owner |
| 2 | ADOPT-2 for one task | Size S, no wire change. Every later demo shows its lines | This record |
| 3 | AX-1 | One call checks a change on the user's own three systems. ADOPT-2 gains its lines over several tasks | Design 0056, wave 1 |
| 4 | AX-2 with measure M1 | The first number that compares several workers with one | Design 0056, wave 1 |
| 5 | ADOPT-3 | The install is the first thing a stranger does | This record |
| 6 | ADOPT-4 | A guide per job. The first is the matrix of AX-1 | This record |
| 7 | ADOPT-1, second round | The same protocol after steps 2 to 6 | This record. Owner |
| 8 | LLM-1, then LLM-6 | The second audience | Designs 0038 and 0056, wave 3 |

Beside these steps:

- Phase E continues unchanged. JOIN-2 and USB-1 shorten the join, so they already serve M15.
- LV-1 and LV-5 are in the backlog and serve point 4 of D2.
- AX-3 follows AX-1 in its wave.

What this changes in design 0056: ADOPT-2 and ADOPT-3 join wave 1, and ADOPT-4 follows AX-1. No
wave gains a protocol version bump.

### D4 A study of first use (ADOPT-1)

The study finds obstacles. It does not measure a market.

**Who.** Three to five people who write code and have two machines. People the owner knows are
acceptable in the first round. The second round includes at least two people the owner has not
helped before.

**What each person does.**

1. Start from the release page or from the USB kit. The record says which.
2. Install on two machines and join them.
3. Follow the Quick start to its first result.
4. Run one job of their own choice.

**What the owner records per session.**

- The operating systems of the two machines.
- Minutes to the end of steps 2, 3 and 4.
- Every point where the person stopped or asked for help. The owner does not help before that.
- The job chosen at step 4, and whether the grid could run it.
- After 14 days: whether the person ran a task of their own again. The owner asks. Nothing in the
  product measures it.

**Record.** A dated table under "Study records" in this record, one row per session. Each person
agrees to the record beforehand. It holds no name, host name, address, secret or ticket.

**How the result changes the order.**

| Finding in the first round | Consequence |
|---|---|
| Most of the time goes to steps 1 and 2 | ADOPT-3 and JOIN-2 move ahead of AX-1 |
| Fewer than half of the people choose a job of A1 or A2 at step 4 | The first audience is reviewed with the owner before AX-2 starts |
| Two or more people choose a job outside D1 | It gets a row in D1 and becomes a candidate of ADOPT-4 |
| Nobody runs a task again within 14 days | The reasons they give are recorded, and D3 is reviewed with the owner before ADOPT-3 or ADOPT-4 starts |

### D5 Run summary (ADOPT-2)

After a waited run the CLI prints two lines next to the existing `Execution Time` line:

```text
Tasks: <N> finished, <F> failed, on <M> machines
Waited: <W> s    Sum of task times: <S> s
```

- `Waited` runs on the client, from the submission to the last result.
- `Sum of task times` adds `TaskResult::execution_time_ms` over the tasks.
- `machines` counts distinct worker ids.
- The JSON output of `submit` and the MCP results carry the same values as `waited_ms`,
  `task_time_sum_ms` and `machines`.
- For one task, `Waited` minus the task time is queueing and file transfer. It tells the user what
  the grid costs.
- For several tasks (AX-1, AX-2), the sum is what these tasks took on these workers. The CLI never
  calls it the time on the user's machine, and it prints no ratio and no percentage. The
  comparison with one machine is measure M1 of design 0056, which has a script.

The values come from fields that exist, so the wire format does not change.

### D6 Install scripts and the scripted measure (ADOPT-3)

`scripts/install.sh` (macOS and Linux, bash 3.2) and `scripts/install.ps1` (Windows) do these
steps:

1. Detect the system and the architecture, and name them when no archive exists for them.
2. Download the archive of one release and its `SHA256SUMS`, and compare them.
3. When LV-5 has landed and `gh` is present, verify the attestation of the archive.
4. Unpack into a directory of the current user, and print it with the next two commands of the
   Quick start.

Rules:

- No `sudo`, no service, no start of a master or a worker, and no question about the secret.
- `--version <VERSION>` pins the release, `--dir <DIR>` sets the target, and `--dry-run` writes
  nothing.
- `README.md` shows two commands: download the script, then run it. It does not show a pipe into a
  shell.
- An environment variable sets the base address of the downloads, for tests and mirrors. It is
  listed in `README.md` with the other variables.
- The item checks on a clean Mac whether a file fetched by the script carries a quarantine mark,
  and records the answer. If it carries none, the `xattr` step of the README applies to browser
  downloads only.

The scripted measure of M15 times the Quick start from an empty home directory: install, master,
join, worker, first `submit --wait`. Its records go next to the other benchmark records in
`benchmarks/grid/`, one per operating system.

ECO-5 stays the full answer for package managers and signed binaries. ADOPT-3 is the part that
needs no certificate and no package repository.

### D7 Guides for common jobs (ADOPT-4)

Each one is a guide by the template of [docs/README.md](../README.md), with a "Verify" block and
a recorded run that names its date, machines and release.

| Candidate | Audience | Needs | Tool on the worker |
|---|---|---|---|
| A test matrix on three operating systems from one MCP call | A1 | AX-1 | `tool:cargo`, or the project's test tool by user tag |
| A test suite in shards: Rust first, then `pytest` or a JavaScript runner | A2 | AX-2 | The same |
| Transcode a directory of videos | A4 | Nothing for one `submit` per file. AX-2 for shards | `ffmpeg`, by user tag |
| Transcribe a directory of recordings with a local speech model | A4 | The same | The model's program, by user tag. The model is an input blob or is present on the worker |
| Render the frames of a scene | A4 | The same | Blender, by user tag |

Rules:

- The first round of ADOPT-1 chooses among the candidates. A job that two people named replaces a
  candidate that nobody named.
- A guide uses tools that the machine's owner installed. It never makes a task download a program
  and run it.
- No guide shows `--insecure`, and every worker command uses `--isolation fs` where the system
  supports it. A guide whose task needs the network says so, because a task under
  `--isolation fs+net` has none.
- A guide names the release of its last recorded run. One that fails at a later release is fixed
  or removed.
- The item may add the tools of its guides to `HOST_PROBED_TOOLS`, so that `--require-tool`
  selects them. Tags are strings, so this changes no wire format. Each probe costs start-up time,
  and the item decides per tool.

### D8 Measures

Each measure has a dated record before any document quotes it. The numbering continues that of
designs 0056 and 0058.

| ID | Measure | How it is read | Target |
|---|---|---|---|
| M15 | Time to first result: minutes from the release page to the first result of the Quick start on two machines | By a person in ADOPT-1, and by script in ADOPT-3 | Baseline from the first round. Working target for a person: 10 minutes |
| M16 | Unaided completion: participants who reach the first result without help | ADOPT-1 | Baseline from the first round |
| M17 | Repeat use: participants who ran a task of their own again within 14 days | Asked by the owner in ADOPT-1 | Recorded. No target before the second round |

Public counts are read from GitHub at each release and noted in the status log: downloads per
release, and issues and pull requests from people outside the project. They have no target.
Measures M1 and M2 of design 0056 measure the gain itself and stay there.

### D9 What this record does not rely on

- An appeal to donate compute. This record treats the public pool of
  [design 0054](0054-open-compute-network.md) as something that follows users. It is not a reason
  for a first install.
- Credits or a token. Design 0056 rules out a token. The plain credits of design 0054 (Q14)
  account for use between people who already run the grid.
- Replacing the installed operating system. Decision Q30 is open, with the recommendation "no".

## Compatibility

- No item changes the wire format or the protocol version.
- ADOPT-2 adds lines to the human output of `submit --wait`, and fields to its JSON output and to
  the MCP results. Existing lines and fields keep their names and meaning. A script that reads the
  human output should read `--json` instead. `CHANGELOG.md` lists the change.
- ADOPT-3 adds two scripts and one environment variable. `README.md` gains them in the same change.
- ADOPT-4 adds guides, and tool probes if the item chooses them. A worker without a probed tool
  does not report its tag, as today.

## Security

- **Install script.** It is the first code a stranger runs. It pins a release, compares SHA-256
  before unpacking, installs for the current user only, and never reads or writes
  `~/.oxideswarm`. A checksum from the server that holds the archive detects a damaged download.
  It does not detect a replaced release. The attestation of LV-5 does, which is why step 3 exists.
- **Download address.** The environment variable can point the script at another server. The
  attestation check does not depend on it.
- **Study.** It runs on a cluster made for it, with its own secret. A participant's machine never
  joins the owner's own cluster. The record holds no personal data.
- **Guides.** The rules of D7: installed tools only, isolation on, no `--insecure`.
- **Run summary.** It shows values the caller already has. The worker count is visible through
  `oxideswarm workers`.
- **No telemetry.** Goal G6 is also a security property: a worker makes no connection the owner
  did not configure.

## Test plan

- **ADOPT-2, unit:** the summary from a list of results. Cases: no task, one task, one failed
  task, and two tasks on one worker (one machine).
- **ADOPT-2, integration:** a waited run on loopback with ephemeral ports prints the two lines,
  and `--json` carries the three fields. `mcp_server_e2e` finds the fields in the result, and
  standard output stays JSON-RPC only.
- **ADOPT-3, launcher tests:** `tests/test_launcher_scripts.py` serves a release directory on
  `127.0.0.1` at an ephemeral port, with a private home directory. A matching checksum installs. A
  mismatch refuses and leaves no file. An unknown architecture is named. `--dry-run` writes
  nothing. A second run replaces the binaries.
- **ADOPT-3, live:** the scripted measure on a clean machine per operating system.
- **ADOPT-4:** the relative-link check of `ox-mode doc`, and each guide's "Verify" output in its
  run record (live).
- **ADOPT-1:** live. The record of D4.
- **Gates:** the ADOPT-2 tests join `workspace-test`, and the ADOPT-3 tests join
  `launcher-scripts`.

## Work items

"Needs" lists items of this record, of designs 0056 and 0058 and of the backlog. Sizes are the
backlog's S, M and L. No item is in [BACKLOG.md](../BACKLOG.md).

| ID | Item | Audience | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|---|
| ADOPT-1 | A study of first use, in two rounds | All | S, owner | Release `0.1.1` | This record | "Study records" holds at least three sessions of the first round, with M15 to M17 | Planned |
| ADOPT-2 | Run summary in `submit --wait`, in its JSON output and in the MCP results | A1, A2 | S | Nothing for one task. AX-1 and AX-2 for several | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `README.md`, `CHANGELOG.md` | The ADOPT-2 tests of the test plan pass | Planned |
| ADOPT-3 | Install scripts and the scripted measure of M15 | All | M | REL-4. LV-5 for the attestation step | `scripts/install.sh`, `scripts/install.ps1`, `tests/test_launcher_scripts.py`, `benchmarks/grid/`, `README.md` | The ADOPT-3 tests pass, and M15 by script is recorded for macOS, Windows and Linux | Planned |
| ADOPT-4 | Guides for common jobs, each with a recorded run | A1, A2, A4 | M | The first round of ADOPT-1. AX-1 and AX-2 for the first two guides | `docs/guides/`, `docs/README.md`, `crates/core/src/capabilities.rs` if probes are added | Three guides each have a "Verify" block and a dated run record, and the index lists them | Planned |

## Decisions for the owner

The right column is a recommended default, not a decision. They are listed in
[ROADMAP.md](../ROADMAP.md) as Q36 to Q39.

| ID | Question | Recommended default |
|---|---|---|
| Q36 | Accept the first audience (A1, then A2) and the order of D3? | Yes |
| Q37 | Will the owner run the first round of ADOPT-1, and with how many people? | Owner. Three to five people, before AX-1 starts |
| Q38 | Which items of this record enter the backlog first? | ADOPT-2 for one task now. ADOPT-3 and ADOPT-4 after the first round of ADOPT-1 |
| Q39 | Keep the rule that the binaries send no usage data, so that numbers about users come only from the study and from public counts? | Yes |

## Risks

| Risk | Mitigation |
|---|---|
| The table of D1 is reasoning, and the work follows the wrong audience | ADOPT-1 runs first, and the table of D4 states what each finding changes |
| A study with people the owner knows flatters the project | The record holds what people did, with times and stops. The second round includes people the owner has not helped |
| Five sessions are read as proof of demand | D4 says what the study is for. No document quotes its numbers as a market figure |
| The install script becomes a way to replace the binaries | A pinned release, the checksum, the attestation of LV-5, and launcher tests for the refusals |
| The run summary is quoted as a speed-up | The labels of D5. The CLI prints no ratio. The comparison is M1, with its script |
| Guides decay | Each names the release of its last run, and a failing guide is fixed or removed |
| This record competes with design 0056 for the agents' time | It adds three small items to wave 1 and reorders nothing else without the study |

## Study records

None yet.

## Status log

- 2026-11-10: Draft written at the owner's request: what does a user gain, and which upgrades make
  people use the project. The state was checked at `origin/master` `4a07fcb`. Nothing is built,
  and no item entered the backlog.
