# 0093: Benchmarks a newcomer can trust and compare

Status: Draft
Created: 2026-11-18    Updated: 2026-11-18

## Summary

The benchmark records are scripted and honest, but none of them answers the three questions a newcomer asks
first: does a second machine make my work finish sooner, how does this compare with what I do today, and
does it work between two networks. This record plans the measurements that answer them on the owner's
machines, under one method: sweep scaling against the fastest machine alone (BENCH-3), offload against
local work (BENCH-4), per-task overhead and the shortest task that pays off (BENCH-5), two networks
(BENCH-6), and the README headlines (BENCH-7). BENCH-2 is the method and the harness. It builds nothing in
the product.

## Context

What the records answer today, and what they leave open:

| Record | File | Answers | Leaves open |
|---|---|---|---|
| BENCH-1 (a): no-op task overhead | `benchmarks/grid/README.md`, `bench_grid.py` | `submit --wait` p50 of 6.3 ms on Linux, 26 ms on macOS, 69 ms on Windows, on loopback | Overhead on a LAN; how long a task must be before a second machine helps |
| BENCH-1 (b): repeated upload | same | 0 of 501 blobs sent again | Nothing for this record |
| BENCH-1 (c): `cargo test` offload | same, and `docs/guides/offload-cargo-test.md` | 209.6 s cold and 7.6 s warm on a Windows laptop (median of 3) | The same command on the Mac itself was not timed, so the record shows that the offload works, not that it is faster. The warm time comes from the reused `target/` |
| SCALE-1 and SCALE-2 | `benchmarks/grid/scale_harness.py` | One master handles 1000 simulated workers on one machine at 419.7 tasks/s | Real work on several machines |
| GPU run | `benchmarks/gpu-run/README.md` | Three GPU tasks pass from a Mac master to a Windows worker | Speed, against anything |
| Interconnect report | `benchmarks/interconnect/COMPARATIVE_INTERCONNECT_BENCHMARK.md` | Numbers typed in by hand from one run each | Its own header says to read "verified" as "recorded once", and that some "Tailscale / Cloudflare RAM" cells look like throughput values. `README.md` links it as the comparative benchmark |
| APP-4, Windows half | design 0089 D6, the hub record `test_runs/win_app4_2026-11-17.md` | The Anima Engine runner builds on Windows; a 3-arm smoke run takes about 16 s; checksums differ between Windows x86_64 and macOS arm64 | Trials per hour on one and two workers (D1 step 3 of design 0089) |

DOC-2 of `docs/BACKLOG.md` shows why the baseline matters: an earlier report compared the cluster with the
phone alone, and the same data put the cluster at 0.37x of the Mac alone, at 30 % parallel efficiency.

LAUNCH-4 puts the benchmarks at the top of the README for the public launch. Design 0054 (Phase H of the
roadmap) plans clusters of volunteers' machines across the internet. No record measures two machines on
different networks.

## Goals

- G1. One number answers "does a second machine help": the throughput of a sweep of real trials on the
  fastest machine alone, on each machine through the grid, and on both, with the parallel efficiency
  against the sum of what each machine does alone.
- G2. Offload is compared with running the same command on the laptop itself, cold and warm, including the
  laptop's own CPU time.
- G3. The per-task overhead on a LAN and the shortest task for which a second worker pays off are known, so
  a reader can tell which workloads fit.
- G4. Link kind, round-trip time, transfer rate and sweep throughput between two different networks are
  recorded, next to a LAN run of the same day.
- G5. Every number at the top of the README comes from a script under `benchmarks/`, a dated JSON record and
  the method of D1. The hand-typed interconnect numbers leave the README.

## Non-goals

- Claims about clusters larger than the machines measured. SCALE-1 and SCALE-2 stay the records of the
  master's capacity.
- Runs against Ray, Dask, HTCondor or BOINC. They target other scales and setups; an item can add one when a
  user asks for it.
- GPU throughput (FEAT-9, spike R20 of design 0064).
- Product changes. A defect found by a measurement becomes its own backlog item or failing test.
- Machines other than the owner's.

## Design

### D1 The method (BENCH-2)

Every record follows these rules:

1. **Plan first.** The record's `plan` field states the question, the workload, the sizes and the expected
   result. It is written and committed before the run and is not edited after it. A poor result is
   published like a good one.
2. **Baseline: the fastest machine alone.** Each machine runs the same work locally, without OxideSwarm, at
   the parallelism that gives it the highest throughput. The harness finds it by trying 1, half and all
   logical cores, and records all three.
3. **Metrics.** Wall clock; throughput (tasks per hour or per second); speedup = grid throughput / the
   fastest machine's local throughput; parallel efficiency = grid throughput / the sum of each machine's
   best local throughput. Efficiency is the fair figure for machines of different speed.
4. **Runs.** One warm-up run, reported and not counted, then at least three measured runs; the record gives
   the median and the range.
5. **Record.** One JSON file under `benchmarks/<name>/results/<UTC date>-<slug>.json`: date, OxideSwarm
   commit and version, the workload's commit, each machine (CPU, logical cores, RAM, OS, architecture,
   power), the `link:` tag and round-trip time from `doctor --json`, isolation mode, worker slots, load
   averages before and after, the `plan` field, every raw run, and the count of failed tasks.
6. **Harness.** Python standard library only, under the rules of `benchmarks/grid/bench_grid.py`: its own
   processes, a private `HOME` and `USERPROFILE`, its own ports, stop by process handle, no host name, user
   name or IP address in a record, `--quick --no-write` for a smoke run, and helper tests in
   `tests/test_<script>.py` that the CI job `agent-bridge` runs.
7. **Results are checked.** Each trial's checksum is compared with the local run of the same trial on a
   machine of the same operating system and architecture (design 0089 D6). The record counts mismatches.
8. **Workloads.**
   - W1, synthetic: `benchmarks/sweep/workload_cpu.py`, a deterministic CPU-bound task in the standard
     library whose work per task is a parameter (0.01 s to 60 s on the Mac). It needs nothing built and runs
     on every system.
   - W2, Anima Engine: the E-SEL1 runner `run_esel1_experiment --smoke` (design 0089), a real deterministic
     simulation, with a grid of manifests over seed and food cap. The runner is built once per system; the
     Mac build needs the owner present, because a full backend build has hung that machine.
   - W3, `cargo`: `cargo test -p rusty_grid_core --lib --offline` as in BENCH-1 (c), and
     `cargo build --release --locked --bin oxideswarm` as a build a faster machine can win.

### D2 Sweep scaling (BENCH-3)

The headline. Machines: the owner's Mac (Apple M5, 10 cores) and the Windows case machine (20 logical
cores) on one LAN.

1. Run K trials on each machine alone, without OxideSwarm, at its best parallelism (D1 rule 2):
   `T_mac`, `T_win` in trials per hour.
2. Start a master for the run on the Mac (private `HOME`, its own port, a secret made for the run, as in
   `benchmarks/gpu-run/README.md`). Run the same K trials through `submit --json` on one worker, first the
   Mac's, then the Windows machine's, with `--slots` at that machine's best parallelism. The runner binary
   and the manifests are `--input` (uploaded once, cached on the worker); each trial directory is
   `--output`.
3. Run them on both workers.
4. Report trials per hour for each case, the speedup against `max(T_mac, T_win)` and the efficiency against
   `T_mac + T_win`.

Sizes: K = 200 for W1 at 10 s per task; K = 60 for W2. W2 is D1 step 3 of design 0089, measured under this
method, and its record also completes that step of APP-4.

Expected (to be fixed in the `plan` field before the run): efficiency of at least 0.9 for W1 and 0.85 for W2.

Headline form: "<K> trials: <x> min on the fastest machine alone, <y> min on two machines through
OxideSwarm (<s>x, <e> % of the ideal)".

### D3 Offload against local work (BENCH-4)

W3 on four arms, each cold (fresh `target/`) and warm, plus once after a one-line edit:

| Arm | Where the command runs | How |
|---|---|---|
| L-mac | The Mac | Locally |
| L-win | The Windows machine | Locally |
| O | The Windows machine | `submit --workspace` from the Mac (BENCH-1 (c)) |
| M | The Windows machine | Copy the tree and run the command over ssh (only if Q92 is yes) |

Besides the wall clock, the record holds the CPU seconds the Mac itself spent in each arm (the client
process tree, or the local build), because freeing the laptop is part of the benefit even when the wall
clock is close.

Headline form: "`cargo test` on a second machine: <x> s warm against <y> s on the laptop, and <z> CPU
seconds of the laptop's own".

### D4 Per-task overhead and the break-even task length (BENCH-5)

This extends BENCH-1 (a) from loopback to the LAN and to task lengths. For W1 tasks of 0, 10 ms, 100 ms, 1 s
and 10 s of work, run N tasks (enough for 60 s or 1000 tasks) on the fastest machine alone, through the grid
on one worker and on two. Report the submit-to-result p50 and p95, the throughput and the efficiency for
each length, and the shortest length at which two workers beat the fastest machine alone. Measure both
ways to submit: one CLI process per task (as BENCH-1 (a)) and one long-lived client that keeps its
connection (a `submit --json` driver or MCP), because they differ by the connection set-up.

Headline form: "a task must run for at least <L> before a second machine pays off; per-task overhead on a
LAN is <x> ms at p50".

### D5 Two networks (BENCH-6)

This needs the owner: the owner moves the Mac to a phone's hotspot (another public address, usually behind
carrier NAT) while the Windows machine stays on the home LAN. Agents never change network settings.

1. Start a master for the run on the Mac with `--p2p` and a secret made for the run; the Windows worker
   joins with that run's ticket and secret.
2. Record `doctor --json`: the link kind of C7 (direct or relayed) with its round-trip time, and the MB/s of
   C8.
3. Run 100 empty tasks for the submit p50 and p95, move one 100 MiB input (the `transfer` scenario of
   `benchmarks/endurance/endurance.py` once TEST-24 has it, else a plain `--input`), and run BENCH-3 with W1
   at K = 50.
4. Repeat steps 2 and 3 on the LAN the same day as the control.

A relayed link is recorded as it is; the public n0 relays are rate-limited (LINK-1). This run does not
close LINK-1, which asks for a live run through a self-hosted relay.

Headline form: "between two networks: <direct or relayed> link, <x> ms round trip, <y> MB/s, the sweep at
<e> % of the LAN run".

### D6 README headlines and the interconnect report (BENCH-7)

- The first rows of the "Measured" table in `README.md` become the headlines of BENCH-3 to BENCH-6, each
  linked to its record. The rows there today stay below them. LAUNCH-4 then points at this table.
- The interconnect report, by the default of Q91: the README stops calling it a comparative benchmark and
  stops linking it from "How it works"; the file stays under `benchmarks/interconnect/` with its header as a
  record of one hand-typed run. BENCH-6 is its scripted successor for the OxideSwarm side.

### D7 Order and who runs what

| Step | Needs | Who |
|---|---|---|
| 1. BENCH-2: harness, W1, loopback smoke, helper tests in CI | Nothing | A coding agent |
| 2. BENCH-5 on the LAN with W1 | Both machines | The coordinator session on the Mac, agent-win on Windows |
| 3. BENCH-3 with W1 | Both machines | Same |
| 4. BENCH-3 with W2 | The runner built on the Mac with the owner present; Q90 | The owner and agent-win |
| 5. BENCH-4 | Both machines | The coordinator session and agent-win |
| 6. BENCH-6 | The owner moves the Mac to a hotspot | The owner, with both agents |
| 7. BENCH-7 | Steps 3 to 6 recorded | A coding agent |

No run touches the owner's trial cluster in `~/OxideSwarm-trial` or its port 8088.

## Compatibility

No change to the wire format, the CLI or any default. The scripts call released commands (`submit --json
--wait`, `doctor --json`, `workers`). Both machines run the same release, 0.4.1 or later, and each record
names it.

## Security

- A master for a run has a private `HOME`, its own ports and a secret made for that run, deleted after it.
  Nothing reads or writes `~/.oxideswarm`.
- Records hold no secret, ticket, host name, user name or IP address; machines are described by their
  hardware.
- Workloads never stage `.env` or API keys (design 0089). The record states the isolation mode; runs use
  `--isolation fs` where the workload works under it.
- The two-network run opens a master to the internet through iroh for the length of the run, behind the
  run's secret.
- Q92 needs an SSH server on the Windows machine. That is the owner's setting; agents do not turn it on.

## Test plan

- Unit: helper tests for each new script (`tests/test_bench_sweep.py`), standard library only, no binary,
  in the CI job `agent-bridge`, like `tests/test_scale_harness.py`.
- Smoke: each script's `--quick --no-write` on loopback, in the helper tests where it needs no binary.
- Live: the records themselves.
- Gate: a headline enters the README only when its record is committed and its `plan` field predates the
  run (the commit of the plan comes first).

## Decisions

| ID | Question | Default |
|---|---|---|
| Q90 | May the README headline of BENCH-3 name Anima Engine and show its numbers, or does it use W1 only? | Measure both. Name Anima Engine only if the owner agrees and its repository is public; otherwise say "a deterministic agent-based simulation" |
| Q91 | What happens to the hand-typed interconnect report? | Unlink it from the README and stop calling it a benchmark there; keep the file with its header as a record of one run |
| Q92 | Is "copy and run over ssh" a baseline in BENCH-4? | No: local baselines only, because it needs an SSH server on the Windows machine. Yes if the owner turns one on |

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| BENCH-2 | The method and the sweep harness | `benchmarks/sweep/bench_sweep.py`, `benchmarks/sweep/workload_cpu.py`, `benchmarks/sweep/README.md`, `tests/test_bench_sweep.py`, `.github/workflows/ci.yml` (job `agent-bridge`) | The harness runs W1 locally and through a loopback cluster with `--quick --no-write`; the helper tests pass in CI on Linux, macOS and Windows | Planned |
| BENCH-3 | Sweep scaling: the fastest machine alone, each machine through the grid, both | `benchmarks/sweep/results/` | Records for W1, and for W2 by Q90, with speedup and efficiency | Planned |
| BENCH-4 | Offload against local work | `benchmarks/grid/bench_grid.py` (extend (c)) or `benchmarks/sweep/` | A record of the arms of D3, cold, warm and after one edit, with the Mac's CPU seconds | Planned |
| BENCH-5 | Per-task overhead and the break-even task length on a LAN | `benchmarks/sweep/` | A LAN record with throughput and efficiency per task length for one and two workers, and the break-even length | Planned |
| BENCH-6 | Two networks | `benchmarks/sweep/`, `doctor --json` | A record with the Mac on a hotspot and the worker on the home LAN, and the same-day LAN control | Planned |
| BENCH-7 | README headlines; the interconnect report by Q91 | `README.md`, `benchmarks/interconnect/`, `docs/BACKLOG.md` (LAUNCH-4) | The README's first "Measured" rows link to the records of BENCH-3 to BENCH-6; `ox-mode doc` passes | Planned |

## Status log

- 2026-11-18: Drafted by the coordinator session at the owner's request ("soạn kế hoạch benchmark") after a
  review found the records honest but without a scaling number against the fastest machine, a comparison
  with local work, or a run between two networks. Q90 to Q92 open.
