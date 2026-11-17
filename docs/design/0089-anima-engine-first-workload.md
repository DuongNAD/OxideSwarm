# 0089: Anima Engine as the first workload: sweeps, verified results and build offload

Status: Accepted (plan; Q88 and Q89 decided on 2026-11-17 by the owner's delegation)
Created: 2026-11-17    Updated: 2026-11-17

## Summary

The owner asked on 2026-11-17 that OxideSwarm support the owner's simulation project, Anima Engine,
as far as it can. Anima Engine already runs the kind of work the grid is built for: sweeps of
independent experiment trials that each end in a checksum, and a Rust build that is too heavy for
the owner's development machine. This record maps what Anima Engine needs to what OxideSwarm 0.4.1
does today, and adds the missing pieces in order: the trial of [design 0063](0063-two-directions-in-parallel.md)
D5 as a backlog item (APP-4), Anima Engine as the first user of AX-1 and AX-2, a way to run a task
again on another worker and compare its outputs (VERIFY-1), and a guide with a recorded run (APP-5).

## Context

### What Anima Engine is and runs (read on 2026-11-17 at `main` `28c45efd7bdc`)

- A simulation of a living world (terrain, water, climate, plants, animals, evolution). Rust core in
  `src-tauri/` inside a Tauri desktop application; a frontend in TypeScript.
- **Experiments:** an `ExperimentManifest` holds every input (artifact, rules, initial conditions,
  interventions, seeds, metrics). A runner executes it with no window. `ANIMA_SIM_SEED` overrides the
  seed for sweeps, `ANIMA_DETERMINISTIC` turns on the deterministic mode whose replay must give the same
  checksum (contract ER08), `ANIMA_USE_GPU=0` falls back from `burn-wgpu` to the CPU.
- **Sweeps in use today:** the E-SEL1 smoke runs of 2026-11-17 (`artifacts/experiments/e-sel1-smoke-record/`)
  ran on a release build on macOS arm64, driven by a Node script. Each trial (`n0-<N0>_fa-<cap>_fs-<cap>`)
  is one runner invocation whose directory holds `checksums.sha256`, `provenance.json`, `runs.csv`, a
  report and a summary; the driver reads counts, microseconds per tick and each run's final checksum.
- **Build and tests:** `cargo test --manifest-path src-tauri/Cargo.toml --features desktop -j 2`; one
  `cargo test` process at a time; a full backend build has hung the owner's development machine.
- **Secrets:** `GEMINI_API_KEY` and Neo4j credentials come from a `.env` file.

### What OxideSwarm 0.4.1 does for it today

| Need | Today | Missing |
|---|---|---|
| A sweep of trials | Each trial can be one `submit --command` task: the runner binary and manifest as `--input` (content-addressed, uploaded once and cached on each worker), the trial directory as `--output`; placement by `--os`, `--arch`, `--worker` or `--require-tag`; `--json` for a driver | One command for a whole grid of trials (AX-1); no record of a run |
| Checking a result | Each output file comes back with its BLAKE3 digest | Running a trial again on another worker and comparing (VERIFY-1); whether checksums are equal across operating systems is not known (APP-4, step 2) |
| The heavy build and tests | The core user story: an agent offloads `cargo test` to a stronger machine over MCP (FEAT-4, workspaces); a worker with `--slots 1` runs one test process at a time | Build once per operating system and reuse the binary across trials (AX-2) |
| The GPU (`burn-wgpu`) | `--gpu`, `--vram-mb` and VRAM accounting (FEAT-9a); runtime directories under isolation (`--isolation-read-dir`, design 0074); first cross-machine GPU run recorded on 2026-11-16 | No run of Anima Engine on the GPU through the grid |

## Goals

- G1. A sweep of Anima Engine trials runs across the owner's machines through OxideSwarm, and each
  trial's outputs equal those of the same trial run locally on the same operating system.
- G2. A trial's result can be checked by running it again on a second worker and comparing its outputs.
- G3. Anima Engine's heavy build and tests run on the strongest machine on request from the owner's
  coding agent.
- G4. Anima Engine trials run on the GPU worker when asked.

## Non-goals

- Changes to Anima Engine itself. Its driver calling `oxideswarm submit` instead of a local process is
  work in that repository; this record only says what it needs from the grid.
- The science of the simulation. The grid adds nothing to it.
- Anima Engine as the public showcase of the grid (Q52 stays open).

## Design

### D1 The trial first (APP-4, needs nothing built)

As design 0063 D5, on the owner's Mac and the Windows case machine:

1. Build the headless runner (release) on each machine; record which operating systems build it.
2. Run one manifest with one seed in deterministic mode on the CPU on both machines and compare the
   checksums. Equal: a second worker can check a result by equality on any system (VERIFY-1 works across
   systems). Different: checks compare only runs on the same operating system and architecture, and the
   record says so.
3. Run a sweep through `submit` (a shell loop or the driver with `--json --wait`), first on one worker,
   then on both; record trials finished per hour in each case.

Rules for the trial: task environments carry only what a trial needs (`ANIMA_SIM_SEED`,
`ANIMA_DETERMINISTIC`, `ANIMA_USE_GPU`); the `.env` file with API keys is never staged as an input; workers
run with `--isolation fs` where the runner works under it.

### D2 One command for a sweep (AX-1 and AX-2, Anima Engine as their first user)

AX-1 (matrix fan-out, design 0056) maps an Anima grid directly: axes `seed`, `n0`, `food_cap` give one task
per combination, each with its own output directory, and one summary of all results. AX-2 (sharded runs,
build-once for Rust) builds the runner once per operating system and architecture and passes the binary to
every trial as the same input blob. Neither needs a wire change for this use. Q88 asks whether they start
with Anima Engine as the first user once APP-4 has a record, instead of after the first round of ADOPT-1 (Q83).

### D3 Run again and compare (VERIFY-1)

`submit --verify <N>` (client side, no wire change): the client submits the task, then submits it again
pinned to N other workers (`require:worker=<id>`, chosen among workers that match the task's placement and
are not the first worker), waits for all, and compares the outputs that the task declares (`--output`) by
their BLAKE3 digests, or a checksum file the task names (`--verify-file checksums.sha256`). It prints
`verified` when all match and `mismatch` with the differing files otherwise, and exits non-zero on a
mismatch. The MCP `submit` tool gets the same option. A task that is not deterministic must not use it;
the help text says so. Q89 asks whether it ships in a `0.4.x` release (it changes no wire format).

### D4 Build and test offload (exists; documented by APP-5)

The owner's coding agent runs Anima Engine's backend build and tests on the strongest machine through
MCP with a workspace (FEAT-4), on a worker with `--slots 1` so that one `cargo test` runs at a time, and
passes `-j 2` where the target machine needs it.

### D5 The guide (APP-5)

`docs/guides/anima-engine-sweeps.md`: how to run Anima Engine trials and sweeps on OxideSwarm, with the
GPU and CPU variants and VERIFY-1, and a recorded run (machines, versions, trials per hour), following
the guides of ADOPT-4.

## Compatibility

D1 and D4 use OxideSwarm 0.4.1 as it is. VERIFY-1 is client side and works against any master that runs
the tasks (protocol 9 and 10). AX-1 and AX-2 follow design 0056.

## Security

Anima Engine trials are command tasks: they run as the worker's user, under `--isolation fs` where they
work. The guide forbids staging `.env` or any file with credentials as an input, and passes only the
`ANIMA_*` variables a trial needs. VERIFY-1 adds no new input to the master.

## Test plan

- APP-4: the dated trial record of D1 (checksums on both machines, trials per hour on one and two workers).
- VERIFY-1: CLI tests on a loopback cluster with two workers: a deterministic task gives `verified`; a task
  whose output differs per worker gives `mismatch` and a non-zero exit; fewer matching workers than N gives
  a clear error. MCP test of the new option.
- APP-5: the guide's commands run as written; its record is linked.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| APP-4 | Trial: Anima Engine trials on the owner's two machines, checksums compared, a sweep through the grid (D1; from design 0063) | `docs/design/0063-two-directions-in-parallel.md` "Trial records", or a record under `benchmarks/` | The record holds the checksums of one deterministic CPU run on each machine and the trials per hour on one and two workers | Planned |
| VERIFY-1 | Run a task again on other workers and compare its outputs (D3) | `crates/cli/src/cli_app.rs` and its submit path, `crates/cli/src/mcp.rs`, a CLI test, `README.md` | The tests of D3 pass in CI | Planned |
| APP-5 | Guide: Anima Engine sweeps on OxideSwarm, with a recorded run (D5) | `docs/guides/anima-engine-sweeps.md` | The guide exists with a recorded sweep and links from `README.md` | Planned |

AX-1 and AX-2 are in the backlog already (Phase 13); D2 names Anima Engine as their first user.

## Decisions for the owner

| ID | Question | Recommended default |
|---|---|---|
| Q88 | Do AX-1 and AX-2 start with Anima Engine as their first user once APP-4 has a record, instead of waiting for the first round of ADOPT-1 (Q83)? | Yes. Anima Engine is a real user with a real sweep today; ADOPT-1 still runs and may reorder later steps. Decided on 2026-11-17 (delegated) |
| Q89 | Does VERIFY-1 ship in a `0.4.x` release, since it changes no wire format? | Yes. Decided on 2026-11-17 (delegated) |

## Status log

- 2026-11-17: Drafted at the owner's request ("dự án này hỗ trợ tối đa cho dự án Anima-engine của tôi").
  Ids claimed on the hub with `agent_bridge.py claim` (design 0087): design 0089, Q88, Q89, phase 16,
  VERIFY-1, APP-5. Q88 and Q89 are open.
- 2026-11-17: The owner delegated the open choices ("bạn cứ làm những thứ tốt nhất theo ý bạn"); the coordinator
  took the recommended defaults of Q88 and Q89. Status Accepted. VERIFY-1 starts now; APP-4 needs a build of
  Anima Engine on each machine and is left to the owner or the Windows agent after the stability items, since a
  full backend build has hung the owner's development machine.
