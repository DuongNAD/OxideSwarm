# Completion backlog

Open work on OxideSwarm, in execution order. Each work item stands alone, so it can be handed to a
coding agent without other context.
The text of each Implemented item is in [BACKLOG-ARCHIVE.md](BACKLOG-ARCHIVE.md), under the same phase heading.

Snapshot: 2026-11-01, commit `3a1c120` (`master` at `c427f8b` plus pull request 3).
Line numbers refer to that commit. If a line has moved, search for the named symbol.
The Index and the status log are kept current. The rest of the file describes the snapshot.

## Is the project finished?

This section is the snapshot of 2026-11-01 and is not updated. The current state is the Index
below and the status log at the end.

No. One master on a trusted LAN running command tasks works and is well tested. Most features
beyond that path are partial, and several documents mark them as finished.

| Area | Status | State at the snapshot |
|---|---|---|
| Task queue, scheduler, placement, worker failover | Implemented | Integration and chaos scripts pass |
| Secret handshake and AEAD on TCP | Implemented | Hardening waits on a branch (INT-3) |
| Inline file staging | Partial | A task can corrupt the cache (BUG-1). Declared outputs can vanish silently (BUG-2) |
| Content-addressed file transfer | Partial | On a branch, not merged. Review fixes are open (INT-4, INT-5) |
| MCP server | Partial | Runs short commands. Security gaps (SEC-1, SEC-2). Cannot offload a build (MCP-1, FEAT-4) |
| Master restart and master failover | Design only | Nothing survives a restart (FEAT-1). Nothing sends the claim message (FEAT-6) |
| Mobile coordination | Partial | Battery tiers work. Thermal tiers and keepalive are wrong (MOB-1, MOB-2) |
| Android app | Partial | Does not compile (AND-1). The in-app master is a stub (SEC-3) |
| 3D pipeline | Partial | SDF tier and built-in rigger work. The AI tier is a stub (3D-1). Default submit fails (BUG-3) |
| GPU compute | Partial | Seeded matrix benchmark only (FEAT-9) |
| Mid-task checkpoints | Partial | `BuiltinTest` only (FEAT-5) |
| Agent Mesh | Partial | Hub access control works. Data ends in memory (MESH-2). No attach to a running master (MESH-3) |
| Release and packaging | Planned | No tags, no binaries, truncated Apache licence (REL-1, REL-4) |

These parts were checked and need no rework:

- Framing and format dispatch, and the secret handshake on TCP and QUIC.
- Session-bound task ownership, worker failover and failover sweeps.
- Placement, `Engine::hold_back`, strict GPU gating and host CPU backpressure.
- The GEMM product check before `Status: VERIFIED_OK`.
- Process tree kill on cancel and timeout for `Command`, `ShellScript` and `RustCompilation`.
- Path validation and symlink-safe artifact collection.
- `oxideswarm doctor` checks C1 to C6.
- Agent Mesh hub token, `from` enforcement and `Origin` / `Host` guards.
- All 106 relative Markdown links. All 65 `[[test]]` paths.

## Rules for the executing agent

1. Read `CLAUDE.md` and `docs/README.md` before the first change.
2. Work in the order of the index below. An item starts after the items in its "Needs" column.
3. Use one branch and one pull request per item. Never push to `master`. Merge only when the checks
   are green and the owner has approved the merge.
4. For a bug, write the failing test first, then the fix.
5. Evidence "Reproduced" means the behaviour was observed on 2026-11-01. Evidence "Read" means it
   comes from reading the code. Confirm a "Read" item before you fix it. If the code disagrees,
   write that in the status log and skip the item.
6. Follow the wire rule in `CLAUDE.md`: a new variant or field goes into the public enum, both
   mirror enums and all four `From` impls.
7. A change to behaviour, a CLI flag, a port, an environment variable or a default updates
   `docs/reference.md` and gets a design record, in the same pull request.
8. Never read or write the real `~/.oxideswarm`. Give child processes a temporary `HOME` and
   `USERPROFILE` with `Command::env`.
9. Do not write "verified on hardware" without a dated record of the run.
10. Stop and ask the owner when an item is marked "Owner" in the index.
11. Set an item to Implemented only after its "Done when" checks pass. Add a status log line.

Run these gates before every pull request. All of them must pass.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p rusty_grid_master --no-default-features --locked
cargo test --workspace --lib --locked
cargo audit
```

## Test baseline

Results on an Apple Silicon Mac with Rust 1.96.0, at `3a1c120`, with a temporary `HOME`:

| Check | Result |
|---|---|
| CI on Linux, macOS and Windows (fmt, clippy, unit tests, audit) | Pass, run 40269419930 |
| `./test_integration.sh` | Pass |
| `./run_chaos_test.sh` | Pass |
| `python3 tests/test_launcher_scripts.py` | Pass |
| `python3 tests/run_mesh_simulation_tests.py` | Pass, but it tests a Python reference hub only |
| `cargo test --workspace --no-fail-fast` | 1392 passed, 19 failed in 9 test targets |
| The same, with a dummy `cluster.secret` in `HOME` | Six more tests fail (TEST-4) |

Use this baseline to tell an old failure from a new one.

| Group | Tests | Cause | Item |
|---|---|---|---|
| Needs Blender on a Windows path | Seven tests in `crates/worker/tests/test_challenger_m3_blender_job_object.rs` | Not gated | TEST-1 |
| Unix process tree | Three tests in `test_challenger_m2_job_object.rs`, `test_job_object_tree_kill_resilience`, `test_job_object_creation_and_lifecycle` | `JobObject` is a no-op on Unix, and two tests spawn without their own process group | TEST-1, SEC-5 |
| Timing budget | `test_marching_cubes_watertightness_and_speed` | 50 ms budget in a debug build | TEST-1 |
| Host battery | `test_a_healthy_master_that_acks_heartbeats_is_never_dropped`, `test_aborting_the_worker_task_closes_its_connection_to_the_master`, `test_adversarial_cancel_worker_queued_concurrency_throttle` | Pass when `pmset` reports no battery | TEST-2 |
| Stale or starved | `test_challenger_cascading_worker_crashes_exhaust_retries`, `test_capability_queries_under_connect_disconnect_stress` | See TEST-3 | TEST-3 |
| Flaky | `test_adversarial_worker_connect_via_master_p2p_ticket` | 5 s budget for a P2P connection | TEST-3 |

Run the suite without host noise as follows, on a macOS development machine. The fake `pmset`
reports no battery. Replace `<SCRATCH>` with an empty temporary directory.

```bash
mkdir -p <SCRATCH>/bin <SCRATCH>/home
printf '#!/bin/sh\nexit 0\n' > <SCRATCH>/bin/pmset
chmod +x <SCRATCH>/bin/pmset
env PATH=<SCRATCH>/bin:$PATH HOME=<SCRATCH>/home USERPROFILE=<SCRATCH>/home \
  CARGO_HOME=$HOME/.cargo RUSTUP_HOME=$HOME/.rustup \
  cargo test --workspace --locked --no-fail-fast
```

## Decisions for the owner

The defaults let work continue. The owner can overrule any of them before the item starts.

| ID | Question | Default | Items |
|---|---|---|---|
| D1 | Build master failover now, or record it as design only? | Design only until FEAT-1 is done | FEAT-6 |
| D2 | Keep the in-app Android master? | Delete it, the app is a worker only | SEC-3 |
| D3 | Integrate a real 3D model, or stay with the SDF tier? | Stay with the SDF tier and fail honestly | 3D-1 |
| D4 | Publish crates to crates.io? | No, set `publish = false` | REL-4 |
| D5 | Rewrite git history to remove owner data? | No, scrub the current tree only | REL-2 |
| D6 | Is a user GPU kernel in scope? | No, label GPU compute as a benchmark | FEAT-9 |
| D7 | Keep `scripts/agent_bridge.py serve-secret`? | Delete it | SEC-4 |
| D8 | May a `rusty-grid.toml` in the working directory still configure `worker` and `submit`? | No. Skip it with a notice on stderr, as `mcp` already does. `--config ./rusty-grid.toml` still loads it | SEC-10 |
| D9 | Keep the `/api/chat` route, which runs `agy` on the master host, in the master dashboard? | Remove it from the master. If it stays, it needs Admin scope and an opt-in flag | SEC-14 |
| D10 | Should the quick start lead with `oxideswarm invite` (a revocable key per machine) instead of the join token (the raw cluster secret, with no expiry)? | Yes, once ENROLL-1c cuts every lane of a revoked member | ENROLL-1, DOC-9 |
| Q13 | Which ecosystem comes first: (A) each person or team runs a cluster that trusted people add machines to, or (B) a public grid where strangers run each other's code? | A, decided by the owner on 2026-11-09. B waits for real users and its own design | ENROLL-1, JOIN-2, USB-1 |

## Index

Size is S, M or L. "Owner" means the item needs a person, a device, a decision or an approval to
merge.

On 2026-11-15, at `master` `cd0f2c19` plus SCALE-2 (PR 227) and FEAT-9a (PR 232): 77 items Implemented, 8 Partial and 37 Planned. Five items
were added on 2026-11-08 from the research recorded in [ROADMAP.md](ROADMAP.md): CACHE-1, REAPI-1,
JOIN-1, LINK-1 and BENCH-1. Fourteen items (SCALE-1 to SCALE-6 and ECO-1 to ECO-8) were added on
2026-11-09 from Phases F and G of the roadmap and [design 0050](design/0050-scale-out-architecture.md). Four items (LV-1, LV-5, LV-10 and LV-14) were added on 2026-11-10 from [design 0058](design/0058-lasting-value.md).
One item (TEST-23) was added on 2026-11-11 from a CI failure on Windows.
Three items (TEST-24 to TEST-26) were added on 2026-11-14 from
[design 0065](design/0065-large-transfer-and-long-run-tests.md).
Eleven items (BUG-19, BUG-20, SEC-10 to SEC-15, AND-5, DOC-9 and DOC-10) were added on 2026-11-15
from the audit of `c2462e3` (Phase 11). With FEAT-9a (pull request 232) the Index then held 77
items Implemented, 8 Partial and 48 Planned. On 2026-11-16, after BUG-19, BUG-20, SEC-14, DOC-9
and DOC-10, with SEC-10, SEC-13 and AND-5, and with TEST-27 added, it holds 85 Implemented, 8 Partial
and 41 Planned. After BUG-21 ([design 0077](design/0077-windows-relative-task-executables.md)), it holds
86 Implemented, 8 Partial and 41 Planned. After SEC-11, SEC-12, SEC-15 and MAINT-1 (pull requests 256 to 259), it holds
90 Implemented, 7 Partial and 38 Planned (135 items).
Phase 12 adds twelve items (PROTO-1 to PROTO-4, TEST-28 to TEST-32, SEC-16, BUG-22 and CLI-2), so
the table holds 147 items: 91 Implemented (PROTO-1 counts as Implemented with its pull request),
7 Partial and 49 Planned.
After TEST-28 and TEST-30 (Implemented), TEST-27 and TEST-31 (Partial) and three new flaky tests
(TEST-33 to TEST-35), it held 150 items: 93 Implemented, 9 Partial and 48 Planned.
Recounted on 2026-11-17 after pull requests 267 to 286 and PROTO-3 and PROTO-4 (pull request 274; PROTO-5, TEST-39 to
TEST-47, BUG-23 and BUG-24 added): 165 items, 105 Implemented, 13 Partial and 47 Planned.
On 2026-11-17 the twelve items of Phase 13 were added from
[design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md) (Q82). Counted from the rows, the table
then holds 178 items: 106 Implemented, 13 Partial and 59 Planned.
Then Phase 14 (LAUNCH-1 to LAUNCH-5) and TEST-48 were added: the table holds 184 items, 106 Implemented, 13 Partial and 65 Planned.
Then Phase 15 (COORD-1 to COORD-3, design 0087) was added and implemented: 187 items, 109 Implemented, 13 Partial and 65 Planned.
Then TEST-49 and Phase 16 (APP-4, VERIFY-1, APP-5, design 0089) were added: 191 items, 109 Implemented, 13 Partial and 69 Planned.
After VERIFY-1 (pull request 296): 191 items, 110 Implemented, 13 Partial and 68 Planned.
Recounted on 2026-11-18 after pull requests 299 to 318 (BUG-25, BUG-26, TEST-23 Partial, TEST-27, TEST-42 to
TEST-51, CI-1 Partial, VERIFY-2, BENCH-2; Phase 17 from design 0093 added; TEST-32 corrected to Implemented by
pull request 267; APP-4 Partial): 201 items, 126 Implemented, 15 Partial and 60 Planned.
Recounted on 2026-11-19 from the rows, after pull requests 340 to 359 (354 still open): LV-4, LV-5, LV-15,
BENCH-3, TEST-23 and TEST-41 Partial; BUG-29 to BUG-32, BENCH-8, ADOPT-2 and TEST-55 Implemented; BENCH-8, BENCH-9,
BUG-32 and TEST-55 added. With MAINT-2 (pull requests 323, 325 and 331) and TEST-29 Implemented, SEC-16 Partial
and SEC-17 added, the table holds 215 items: 143 Implemented, 18 Partial and 54 Planned; 26 open items are In and
46 are Out.
Then TEST-56 (pull request 361, open; its `Line` value `In` stays until merged): 216 distinct items, 144 Implemented, 18 Partial and 54 Planned; 26 remaining open items are In and 46 are Out.
Then BENCH-9 Partial (pull request 354) and SEC-18 added: 216 items, 143 Implemented, 19 Partial and 54 Planned;
27 open items are In and 46 are Out.
Recounted on 2026-11-21 from the rows after items through TEST-84, APP-6, and FEAT-9 Implemented: 247 items, 172 Implemented, 21 Partial and 54 Planned; 25 open items are In and 50 are Out (TEST-56 keeps `Line` `In` while pull request 361 is marked open).
Recounted on 2026-11-21 after LV-10 (steps 1–3) and PROTO-5 Partial: 247 items, 172 Implemented, 23 Partial and 52 Planned; 25 open items are In and 50 are Out.
Recounted on 2026-11-21 after SCHED-5 Implemented (plus SCHED-4c and SCHED-4d implemented while SCHED-4 stays Partial pending live 2-machine hardware batch): 247 items, 173 Implemented, 23 Partial and 51 Planned; 25 open items are In and 49 are Out.
"Pull requests" lists them by number, "(open)" marks one
that is not merged yet, and "(design)" marks a design record that builds nothing.
"Line" places an open item against the stop line of
[design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md): "In" when an exit check of
its D2 names the item, or when the item fixes a defect or a test (BUG, SEC, TEST); "Out" when its D1 puts
the item outside, or when the owner placed it there (JOIN-1, SCHED-4, LV-10, PROTO-5, LAUNCH-1 to LAUNCH-5,
APP-4, APP-5 and BENCH-3 to BENCH-7, decided on 2026-11-18). An Implemented item has no value there.

| ID | Item | Priority | Size | Needs | Owner | Status | Line | Pull requests |
|---|---|---|---|---|---|---|---|---|
| INT-1 | Merge pull request 3 | P0 | S |  | Yes | Implemented |  | 3, 4 |
| INT-2 | Close superseded pull requests and branches | P0 | S | INT-1 | Yes | Implemented |  |  |
| INT-3 | Land the AEAD hardening branch | P0 | S | INT-1 | Yes | Implemented |  | 5 |
| INT-4 | Land the content-addressed file branch | P0 | M | INT-3, INT-5 | Yes | Implemented |  | 6 |
| INT-5 | Fix the review findings of pull request 6 | P0 | M | INT-3 |  | Implemented |  | 6 |
| SEC-1 | Remove `mcp --sse` | P0 | S | INT-4 |  | Implemented |  | 11 |
| SEC-2 | Make MCP refuse a remote master without a secret | P0 | S | INT-4 |  | Implemented |  | 15 |
| SEC-3 | Delete the Android stub master | P0 | M | INT-4 | D2 | Implemented |  | 13 |
| SEC-4 | Delete the plain-HTTP secret transfer | P0 | S | INT-4 | D7 | Implemented |  | 14 |
| SEC-5 | Run Python and Blender through the hardened runner | P0 | M | INT-4 |  | Implemented |  | 17 |
| SEC-6 | Restrict Agent Mesh node commands | P0 | M | INT-4 |  | Implemented |  | 25 |
| SEC-7 | Correct public route claims and redact the lease | P1 | S | INT-4 |  | Implemented |  | 12 |
| SEC-9 | Private files on Windows | P1 | S | INT-4 |  | Implemented |  | 154 |
| SEC-10 | Stop a working-directory config file from configuring `worker` and `submit` | P1 | S |  | D8 | Implemented |  | 238 (design 0070) |
| SEC-11 | Refuse instead of falling back to the elevated token under Windows isolation | P2 | S | FEAT-3 |  | Implemented |  | 256 (design 0078) |
| SEC-12 | Test and narrow the macOS isolation profile | P2 | M | FEAT-3 |  | Implemented |  | 257 (design 0033) |
| SEC-13 | Collect outputs through the descriptor that was checked | P2 | S |  |  | Implemented |  | 239 (design 0072) |
| SEC-14 | Take the `agy` chat route out of the master dashboard | P2 | S |  | D9 | Implemented |  | 237 (design 0071) |
| SEC-15 | Make the Agent Mesh launchers safe by default | P2 | S | SEC-6 |  | Implemented |  | 258 (design 0079) |
| BUG-1 | Stop hard-linking the staging cache | P0 | S | INT-4 |  | Implemented |  | 16 |
| BUG-2 | Fail a task when a declared output is missing | P0 | S | INT-4 |  | Implemented |  | 20 |
| BUG-3 | Fix the default 3D submit | P0 | S | INT-4 |  | Implemented |  | 21, 44 |
| BUG-4 | Fail only the task when a result is too large | P1 | S | INT-4 |  | Implemented |  | 18 |
| BUG-5 | Close the remaining gaps in the blob store and cache | P1 | M | INT-4 |  | Implemented |  | 26 |
| BUG-18 | Resolve bare commands under Windows isolation against `System32` before `Git\usr\bin` | P1 | S | FEAT-3 |  | Implemented |  | 204 (design 0055) |
| BUG-19 | Bound the data-lane backlog of a QUIC worker session | P1 | S |  |  | Implemented |  | 236 (design 0069) |
| BUG-20 | Keep one slow worker from stalling every dispatch | P1 | M |  |  | Implemented |  | 241 (design 0075) |
| BUG-21 | Resolve relative staged executables against the task working directory on Windows | P1 | S | FEAT-3 |  | Implemented |  | 251 (design 0077) |
| 3D-1 | Stop reporting the stub as TripoSR | P0 | S | INT-4 | D3 | Implemented |  | 22, 46 |
| TEST-1 | Gate host-bound tests | P1 | M | INT-4 |  | Implemented |  | 51 |
| TEST-2 | Isolate tests from the host battery | P1 | M | INT-4 |  | Implemented |  | 76 |
| TEST-3 | Repair stale, starved and flaky tests | P1 | M | TEST-1 |  | Implemented |  | 39 |
| TEST-4 | Stop tests from reading the real home directory | P1 | S | INT-4 |  | Implemented |  | 29, 112 |
| TEST-5 | Register or delete the orphan Rust test | P1 | S | INT-4 |  | Implemented |  | 19 |
| TEST-6 | Extend CI to the full suite | P1 | M | TEST-1, TEST-2, TEST-3, TEST-4 |  | Implemented |  | 67, 124 |
| TEST-7 | Run or delete the orphan Python tests | P2 | M | INT-4 |  | Implemented |  | 55, 66 |
| TEST-19 | Fix flaky required tests | P1 | S | TEST-3 |  | Implemented |  | 161 |
| TEST-20 | Fix Windows disconnect stress timeouts | P1 | S | TEST-19 |  | Implemented |  | 181 |
| TEST-22 | Ignore UUID and metric `4090` tokens in `workflow_modes_test` | P1 | S | TEST-19 |  | Implemented |  | 200 |
| TEST-21 | Fix Windows `m6_challenger_1_stress` and `test_sec6_mesh_sandbox` flakes | P1 | S | TEST-20 |  | Implemented |  | 198 |
| TEST-23 | Find why `master_restart_recovery` loses `out.txt` on Windows | P1 | M | FEAT-1 |  | Partial | In | 305, 357 |
| TEST-24 | Test large transfers between machines | P2 | M | BENCH-1 | Yes | Partial | In | 374 (open), (design 0065) |
| TEST-25 | Run a cluster for 24 hours under load and faults | P2 | L | TEST-24 | Yes | Partial | In | (design 0065) |
| TEST-26 | Run short versions of TEST-24 and TEST-25 on a schedule in CI | P3 | S | TEST-24, TEST-25 |  | Partial | In | 382 (open), (design 0065) |
| TEST-27 | Find why the `mesh_auth` slow-consumer test times out on macOS | P2 | S | |  | Implemented |  | 263, 303 |
| TEST-28 | Make the group-commit timing test meaningful on slow runners | P1 | S |  |  | Implemented |  | 265 |
| TEST-29 | Find why `workspace tests (windows-latest)` loses its runner at 60 minutes | P1 | M |  |  | Implemented |  | 283, 358 (open) |
| TEST-30 | Stop `test_agent_bridge.py` from depending on the host's open ports | P2 | S |  |  | Implemented |  | 263 |
| TEST-31 | Remove the race in the detached-grandchild Blender probe test | P2 | S |  |  | Implemented |  | 265, 363 (open) |
| TEST-32 | Make the cancel-during-upload worker test independent of `NoDefaultCurrentDirectoryInExePath` | P2 | S |  |  | Implemented |  | 267 |
| TEST-33 | Make the worker-churn stress test deterministic on Windows | P2 | S |  |  | Implemented |  | 286, 289 |
| TEST-34 | Show and fix the Agent Mesh simulation Suite 2 failure on macOS | P2 | S |  |  | Implemented |  | 277 |
| TEST-35 | Make the `switch_role` launcher test stable on macOS | P3 | S |  |  | Implemented |  | 277, 338 |
| TEST-36 | Remove the 50 ms bound from the symlink-chain manifest test | P2 | S |  |  | Implemented |  | 276 |
| TEST-37 | Remove the 100 ms window from the port-file reader test | P2 | S |  |  | Implemented |  | 276 |
| TEST-38 | Remove the 800 ms window from the re-dispatched task id test | P2 | S |  |  | Implemented |  | 276 |
| TEST-39 | Wait for the `--no-shell` mesh node to register before calling it | P2 | S |  |  | Implemented |  | 278 |
| TEST-40 | Wait for each payload in the Agent Mesh large-payload test | P2 | S |  |  | Implemented |  | 278 |
| TEST-41 | Find why a task finished during master shutdown can run twice | P1 | M |  |  | Partial | In | 288, 359, 363 (open) |
| TEST-42 | Make the reaper boundary test independent of scheduler delay | P2 | S |  |  | Implemented |  | 302 |
| TEST-43 | Show and fix the late dashboard port file of the in-process dev cluster test | P2 | S |  |  | Implemented |  | 307 |
| TEST-44 | Remove the 1.5x wall-clock cap from the speed-aware batch test | P2 | S |  |  | Implemented |  | 304 |
| TEST-45 | Keep the second worker of the reaper failover stress test alive | P2 | S |  |  | Implemented |  | 308 |
| TEST-46 | Remove the 100 ms bound from the failover latency stress test | P2 | S |  |  | Implemented |  | 309 |
| TEST-47 | Count only this test's handles in the capability-detection leak test | P2 | S |  |  | Implemented |  | 310 |
| TEST-48 | Give each Android bridge test its own worker state | P2 | S |  |  | Implemented |  | 300 |
| TEST-49 | Wait for the master by condition in the sched2 runtime test | P2 | S |  |  | Implemented |  | 301 |
| TEST-50 | Wait for the dashboard banner lines before asserting in cli_dashboard_policy_e2e | P2 | S |  |  | Implemented |  | 315 |
| TEST-51 | Let the run_master unit tests pick their own port instead of bind-and-drop | P2 | S |  |  | Implemented |  | 318 |
| TEST-52 | Let the remaining tests pick ports through the process that binds them, not bind-and-drop | P2 | S |  |  | Implemented |  | 320, 364 (open) |
| TEST-53 | Wait by condition in the oversized task result worker test | P2 | S |  |  | Implemented |  | 327 |
| TEST-54 | Wait by condition in the mapreduce parallelism test | P2 | S |  |  | Implemented |  | 333 |
| TEST-55 | Windows Home: sandbox s1 grant test without the Remote Desktop Users group | P2 | S |  |  | Implemented |  | 356 |
| TEST-56 | Cut the time of `workspace tests (windows-latest)` without losing coverage | P1 | S | TEST-29 |  | Implemented | In | 361 (open) |
| TEST-58 | Find why the keep_sandboxes sweep test fails on macOS | P2 | S |  |  | Implemented |  | 366 (open) |
| TEST-57 | m2 capability stress test: wait for reconnects by condition, not a fixed 1.8 s window | P2 | S |  |  | Implemented |  | 365 (open) |
| TEST-59 | Zero-duration task test: drop the 100 ms wall-clock window | P2 | S |  |  | Implemented |  | 365 (open) |
| TEST-60 | Windows: worker client::tests time out at 5 s under parallel load | P2 | S |  |  | Implemented |  | 378 (open) |
| TEST-63 | master_limits_and_scheduling: prove MapReduce concurrency with a barrier instead of a 7 s window | P2 | S |  |  | Implemented |  | 389 (open) |
| TEST-62 | blob_transfer_e2e: print CLI submit output on failure and harden loaded timeouts | P2 | S |  |  | Implemented |  | 390 (open) |
| TEST-64 | worker lib process_tree_tests: wait for partial stdout by condition before cancelling | P2 | S |  |  | Implemented |  | 388 (open) |
| TEST-65 | test_sec5_hardened_runner: condition waits for short-task pipe EOF and setsid survivor kill | P2 | S |  |  | Implemented |  | 388 (open) |
| TEST-66 | worker_cluster_auth: drop wall-clock upper bounds on handshake timeout and shutdown tests | P2 | S |  |  | Implemented |  | 387 (open) |
| TEST-61 | macOS: hard-link isolation test control printed empty stdout | P2 | S |  |  | Partial | In | 392 (open) |
| TEST-67 | Campaign driver tests flake on Windows log appends and macOS 0.2 s submit timeout | P1 | S |  |  | Implemented |  | 394 (open) |
| TEST-79 | scale1_simulated_worker: widen 5-20 s burst, reconnect, and drain deadlines to 60 s | P2 | S |  |  | Implemented |  | 401 (open) |
| TEST-78 | rendezvous_discovery_e2e: widen 10-30 s command and registration deadlines to 60 s | P2 | S |  |  | Implemented |  | 400 (open) |
| TEST-74 | m5_challenger_2_stress: wait 60 s for reduce Running on w1 and use TEST-54 MapReduce timeouts | P2 | S |  |  | Implemented |  | 398 (open) |
| TEST-77 | e2e_cluster: widen 3-5 s worker and wait_task deadlines to 60 s | P2 | S |  |  | Implemented |  | 399 (open) |
| TEST-73 | m4_scheduling: prove batch concurrency with a barrier and drop 1.2 s / 1.5 s wall-clock windows | P2 | S |  |  | Implemented |  | 397 (open) |
| TEST-68 | gpu_wgpu_compute_test: replace short 3-8 s waits with 60 s condition deadlines | P2 | S |  |  | Implemented |  | 396 (open) |
| TEST-69 | Windows AppContainer isolation tests wait too briefly under load | P1 | S |  |  | Partial | In | 403 (open) |
| TEST-80 | test_sec5_hardened_runner: drop 5 s wall-clock upper bounds in oversized sparse output test | P2 | S |  |  | Implemented |  | 402 (open) |
| TEST-81 | Test masters keep 85 % host-CPU backpressure, stalling queued tasks under load | P1 | M |  |  | Implemented |  | 404 (open) |
| TEST-82 | `rusty_grid_worker`: replace wall-clock upper bounds and bound `scale1` status queries | P2 | M |  |  | Implemented |  | 405 (open) |
| TEST-83 | Replace wall-clock upper bounds in master and core tests with structural proofs and 60 s hang guards | P2 | M |  |  | Implemented |  | 406 (open) |
| TEST-84 | Replace wall-clock upper bounds in `android_bridge`, `cli`, `agent_mesh`, and Python tests with structural checks and 60 s guards | P2 | M |  |  | Implemented |  | 407 (open) |
| DOC-1 | Correct false status claims | P1 | M | INT-4 |  | Implemented |  | 60 |
| DOC-2 | Remove unbacked performance numbers | P1 | S | INT-4 |  | Implemented |  | 61 |
| DOC-3 | Align design records with the code | P1 | M | INT-4 |  | Implemented |  | 68 |
| DOC-4 | Complete the CLI and environment reference | P2 | M | INT-4 |  | Implemented |  | 69 |
| DOC-5 | Document the 3D subsystem | P2 | S | 3D-1 |  | Implemented |  | 70 |
| DOC-6 | Correct the minimum Rust version | P1 | S | INT-4 |  | Implemented |  | 56 |
| DOC-7 | Add an MCP client setup guide | P2 | S | SEC-2 |  | Implemented |  | 32 |
| DOC-8 | Update `CLAUDE.md` and `SECURITY.md` for Windows isolation | P1 | S | FEAT-3 |  | Implemented |  | 196 |
| DOC-9 | Correct the statuses and statements that the audit found wrong | P1 | M |  |  | Implemented |  | 234 |
| DOC-10 | Keep one roadmap | P2 | S | DOC-9 | Yes | Implemented |  | 235 |
| REL-1 | Restore the Apache licence text | P0 | S |  |  | Implemented |  | 8, 82 |
| REL-2 | Remove owner data from tracked files | P1 | M | INT-4 | D5 | Implemented |  | 74 |
| REL-3 | Small repository hygiene | P2 | S | INT-4 |  | Implemented |  | 71 |
| CLI-1 | Add `cancel` and `result` commands | P1 | M | INT-4 |  | Implemented |  | 30, 47 |
| LOG-1 | Close P2P client endpoints cleanly and send client logs to stderr | P1 | S | FEAT-4 |  | Implemented |  | 194 |
| MCP-1 | Make the MCP server concurrent, with deadlines | P1 | M | SEC-2 |  | Implemented |  | 31, 41 |
| MCP-2 | Return outputs safely to an LLM | P1 | M | MCP-1 |  | Implemented |  | 62 |
| MCP-3 | Correct JSON-RPC and argument handling | P2 | S | MCP-1 |  | Implemented |  | 97 |
| SCHED-1 | Respect `--max-concurrency` on the master | P1 | S | INT-4 |  | Implemented |  | 34, 38 |
| SCHED-2 | Wire or delete dead flags | P1 | M | INT-4 |  | Implemented |  | 78 |
| SCHED-3 | Give impossible tasks an outcome | P2 | S | INT-4 |  | Implemented |  | 96, 107 |
| MOB-1 | Fix the adaptive keepalive | P1 | S | INT-4 |  | Implemented |  | 98 |
| MOB-2 | Keep thermal state on the master | P1 | M | INT-4 |  | Implemented |  | 99 |
| MOB-3 | Do not charge evacuation to retry budgets | P2 | S | INT-4 |  | Implemented |  | 108 |
| 3D-2 | Wire VRAM and Blender routing | P2 | M | BUG-3 |  | Planned | Out |  |
| 3D-3 | Make AutoRig options real or refuse them | P2 | M | SEC-5 |  | Planned | Out |  |
| MR-1 | Let a CLI user ship a mapper and reducer | P2 | S | INT-4 |  | Implemented |  | 33 |
| MESH-1 | Make `wss://` work in the Rust mesh client | P2 | S | INT-4 |  | Implemented |  | 94 |
| MESH-2 | Deliver mesh data to a consumer | P2 | M | INT-4 |  | Planned | Out |  |
| MESH-3 | Attach the bridge to a running master | P2 | M | INT-4 |  | Partial | Out | 114, 132 |
| MODE-1 | Make `ox-mode` report only what happened | P2 | M | INT-4 |  | Implemented |  | 92 |
| BRIDGE-1 | Repair `agent_bridge.py diagnose` | P2 | S | SEC-4 |  | Implemented |  | 77 |
| AND-1 | Make the Android app build | P1 | M | SEC-3 | Yes | Planned | Out |  |
| AND-2 | Keep the master across restarts | P2 | M | AND-1 | Yes | Planned | Out |  |
| AND-3 | Wire pre-sleep evacuation | P2 | M | AND-1 | Yes | Planned | Out |  |
| AND-4 | Report real worker status | P2 | M | AND-1 | Yes | Planned | Out |  |
| AND-5 | Stop the Android worker without blocking the calling thread, and build the bridge in CI | P2 | S | SEC-3 |  | Implemented |  | 240 (design 0073) |
| FEAT-1 | Master durability | P1 | L | TEST-6 |  | Implemented |  | 122 (design), 129, 141, 147, 158, 167 |
| FEAT-2 | Containment after a worker crash | P1 | L | SEC-5 |  | Implemented |  | 123 |
| FEAT-3 | OS-level task isolation | P2 | L | SEC-5 |  | Implemented |  | 117 (design), 133, 150, 152 |
| FEAT-3d | cargo under Windows isolation | P2 | M | FEAT-3 |  | Implemented |  | 183 |
| FEAT-4 | Remote workspaces for coding agents | P1 | L | MCP-2 |  | Implemented |  | 105 (design), 157, 156, 164, 166, 160, 165, 168, 170, 176, 174, 177, 186 |
| FEAT-5 | Checkpoints for process tasks | P3 | L | FEAT-1 |  | Planned | Out |  |
| FEAT-6 | Refuse the master-role claim (failover itself is SCALE-4) | P3 | L | FEAT-1 | D1 | Implemented |  | 81 |
| FEAT-7 | Usable rendezvous discovery | P3 | M | INT-4 |  | Implemented |  | (design 0066), 224 |
| FEAT-8 | Real protocol versioning | P2 | M | INT-4 |  | Implemented |  | 40, 110, 120 |
| FEAT-9 | GPU workloads beyond the benchmark | P3 | L | INT-4 | D6 | Implemented |  | 232 (`FEAT-9a`, design 0068) |
| FEAT-10 | Compile crates with cargo | P3 | M | INT-4 |  | Implemented |  | 165 |
| FEAT-11 | Remove dead code and split large modules | P3 | L | TEST-6 |  | Implemented |  | 118 (design), 125, 126, 127, 131 |
| MAINT-1 | Split the six largest source files and cap file size in CI | P2 | L | FEAT-11 |  | Implemented |  | 211, 212, 230, 231, 242, 249, 250, 253, 259 |
| CACHE-1 | Reuse the result of an identical task | P2 | L | FEAT-1 |  | Planned | In |  |
| REAPI-1 | Serve the blob store as a Remote Execution API cache | P3 | L | CACHE-1 | Yes | Planned | Out |  |
| REL-4 | Release workflow and install path | P2 | M | TEST-6 | D4 | Implemented |  | 155 |
| REL-5 | Contributor and security files | P3 | S | REL-1 |  | Implemented |  | 75 |
| JOIN-1 | Join a machine with one token | P2 | M |  |  | Partial | Out | 163 |
| LINK-1 | Show whether each link is direct or relayed | P2 | M |  | Yes | Partial | In | 159 |
| BENCH-1 | Publish benchmarks with the scripts that measured them | P2 | M |  |  | Implemented |  | 162, 175, 187, 189 |
| ENROLL-1 | A key per machine, invites and revocation | P1 | L | JOIN-1 |  | Partial | In | (design 0051), 190, 201, 328 |
| JOIN-2 | Join on a LAN with a short code | P2 | M | ENROLL-1 |  | Implemented |  | (design 0052), 221 |
| USB-1 | Set up machines from a USB stick | P2 | M | ENROLL-1, REL-4 |  | Partial | In | (design 0053), 185, 195, 207, 228 |
| USB-2 | Opt-in Windows isolation in the USB kit | P2 | S | USB-1, FEAT-3d |  | Implemented |  | 204 (design 0055) |
| USB-3 | Keep the Windows USB install folder apart from the default workspace root | P2 | S | USB-1, W2 |  | Implemented |  | 226 |
| USB-4 | Size the kit's task slots to the machine's fast cores | P2 | S | USB-1 |  | Implemented |  | 209, 207, 210 |
| SCALE-1 | Load harness with simulated workers | P2 | L | BENCH-1 |  | Implemented |  | 222, (design 0050) |
| SCALE-2 | Scheduler and queue at scale | P2 | L | SCALE-1 |  | Implemented |  | 227, (design 0050) |
| SCHED-4 | Place tasks by measured worker speed and re-run stragglers | P2 | L | SCHED-1 |  | Partial | Out | 214, (design 0060) |
| SCHED-5 | Tell the user why a queued task waits when every eligible worker is above max_host_cpu_pct | P2 | M |  |  | Implemented |  | (design 0095) |
| SCALE-3 | Worker-to-worker blob distribution | P2 | L | SCALE-1 |  | Planned | Out |  |
| SCALE-4 | Master high availability | P2 | L | SCALE-1, FEAT-1 |  | Planned | Out |  |
| SCALE-5 | Autoscaling providers | P3 | L | SCALE-1, JOIN-1 |  | Planned | Out |  |
| SCALE-6 | Multi-site federation | P3 | L | SCALE-2, SCALE-3, SCALE-4 |  | Planned | Out |  |
| ECO-1 | Remote Execution API compatibility | P3 | L | REAPI-1, ECO-2 |  | Planned | Out |  |
| ECO-2 | Action cache (is CACHE-1) | P2 | L | FEAT-1 |  | Planned | In |  |
| ECO-3 | Multi-tenancy and quotas | P2 | L | SCALE-1 |  | Planned | Out |  |
| ECO-4 | Observability: metrics and traces | P2 | M |  |  | Implemented |  | (design 0050), 225 |
| ECO-5 | Packaging for package managers and containers | P2 | M | REL-4 | Yes | Planned | Out |  |
| ECO-6 | Client SDKs and a versioned public API | P3 | L | FEAT-8 |  | Planned | Out |  |
| ECO-7 | Integrations for coding agents and CI | P2 | M | DOC-7 |  | Planned | Out |  |
| ECO-8 | Documentation site and community | P3 | M | REL-5 | Yes | Planned | Out |  |
| NET-1 | WebAssembly task type with limits and a deterministic mode | P3 | L | ENROLL-1 | Yes | Planned | Out | (design 0054) |
| NET-2 | Federation and signed credit accounting between clusters | P3 | L | SCALE-6 | Yes | Planned | Out | (design 0054) |
| NET-3 | Result verification: redundancy, spot checks, reputation | P3 | L | NET-1 |  | Planned | Out | (design 0054) |
| NET-4 | Contributor controls: opt-in, caps, stop switch, task-class policy | P3 | M | NET-1 |  | Planned | Out | (design 0054) |
| NET-5 | Lightweight VM sandboxes on Linux, macOS and Windows | P3 | L | NET-3 |  | Planned | Out | (design 0054) |
| NET-6 | Distributed directory of offers over iroh | P3 | L | NET-2 |  | Planned | Out | (design 0054) |
| NET-7 | Open protocol specification, conformance tests and a steward | P3 | L | NET-6 | Yes | Planned | Out | (design 0054) |
| NET-8 | Trusted execution environments for confidential tasks | P3 | L | NET-5 |  | Planned | Out | (design 0054) |
| LV-1 | Fuzz targets for the parsers of untrusted bytes (first targets) | P1 | M |  |  | Implemented |  | 270, (design 0058) |
| LV-5 | Supply chain: dependency policy, release provenance, outside checklist | P2 | M | REL-4 | Yes | Partial | In | 342, (design 0058) |
| LV-10 | Citation file and a DOI per release | P2 | S | REL-4 | Yes | Partial | Out | (design 0058) |
| LV-14 | Governance files and a licence that GitHub detects | P2 | S | REL-5 | Yes | Planned | In | (design 0058) |
| PROTO-1 | The release-train rule for wire changes | P1 | S |  |  | Implemented |  | 262, (design 0076) |
| PROTO-2 | A CI job that runs the newest release against the pull request's build | P1 | M | PROTO-1, REL-6 |  | Implemented |  | (design 0076) |
| PROTO-3 | An extension slot for optional wire fields | P1 | L | PROTO-1 |  | Implemented |  | (pull request 272), (design 0082) |
| PROTO-4 | A one-version compatibility window | P1 | L | PROTO-3 |  | Implemented |  | (pull request 274), (design 0083) |
| PROTO-5 | Keep a skew job against the release one protocol back (pinned) | P2 | M | PROTO-4 |  | Partial | Out | (design 0083) |
| SEC-16 | Find what put a full-control entry for an unknown capability on `AppData` | P2 | S |  | Yes | Partial | In | 358 (open) |
| SEC-17 | Windows workspace root: refuse a reparse point in any folder between home and the root, and hard-linked marker or lock files | P2 | S | BUG-32 |  | Implemented |  | 360 (open) |
| SEC-18 | Windows workspace root: check every entry the DACL rewrite reaches, write the DACL through a handle, and tests that prove each walk | P2 | S | SEC-17 |  | Implemented |  | 372 (open) |
| BUG-22 | Count cancelled tasks apart from failed tasks in `status --json` | P3 | S |  |  | Planned | In |  |
| BUG-23 | Stop the Windows master gracefully in `oxctl.ps1` | P2 | S |  |  | Implemented |  | 282 |
| BUG-24 | Let a master restart with `--state-dir` on a file system without permissions | P2 | S |  |  | Implemented |  | 284 |
| BUG-25 | Hand a task back to the master when the worker's sandbox base is unusable at assignment | P2 | S |  |  | Implemented |  | 299 |
| BUG-26 | Keep the `ox-mode dev cluster` master off the default discovery port | P3 | S |  |  | Implemented |  | 311 |
| BUG-27 | Compute the result acknowledgement token only when result resume is on | P3 | S |  |  | Implemented |  | 332 |
| BUG-28 | Refuse a P2P ticket passed to `worker --master` with a `--p2p-ticket` hint | P2 | S |  |  | Implemented |  | 334 |
| BUG-29 | Offer in the USB installer's folder menu only drives whose root the install accepts | P2 | S |  |  | Implemented |  | 341 |
| BUG-30 | Let the USB uninstaller remove an install from a kit older than the workspaces marker | P2 | S |  |  | Implemented |  | 341 |
| BUG-31 | Report a USB-kit worker that stops after its start, with its log, and keep that log | P2 | S |  |  | Implemented |  | 341 |
| BUG-32 | Allow the USB kit's workspace root under `-Isolation fs` | P2 | S |  |  | Implemented |  | 352 |
| BUG-33 | macOS: the lifeline supervisor closes descriptors one by one up to the soft limit for every task | P2 | S |  |  | Implemented |  | 367 (open) |
| BUG-34 | Clean up leftover blob-cache `.part` files of a killed worker without breaking shared sandbox bases | P2 | S |  |  | Implemented |  | 391 (open) |
| CLI-2 | Add a flag to move or disable the UDP discovery beacon | P3 | S |  |  | Implemented |  | 275 |
| CI-1 | Keep runs for pushes on `master` from ending red or cancelled without a failing test | P1 | S |  |  | Partial | In | (design 0088; PR 306) |
| MAINT-2 | Bring the last three files under the size cap | P2 | M | MAINT-1 |  | Implemented |  | 323, 325, 331, (design 0088) |
| DOC-11 | Make the backlog show open work first | P2 | S |  |  | Implemented |  | 322, (design 0088) |
| ADOPT-1 | A study of first use, in two rounds | P1 | S |  | Yes | Planned | In | study kit ready (PR 393), (design 0059) |
| ADOPT-2 | Run summary in `submit --wait`, its JSON output and the MCP results | P2 | S |  |  | Implemented |  | 350, (design 0059) |
| ADOPT-3 | Install scripts and the scripted time to first result | P2 | M | REL-4, ADOPT-1 |  | Planned | In | (design 0059) |
| ADOPT-4 | Guides for common jobs, each with a recorded run | P2 | M | ADOPT-1, AX-1, AX-2 |  | Planned | In | (design 0059) |
| AX-1 | Matrix fan-out in `submit` and in MCP | P1 | M | ADOPT-1 |  | Planned | In | (design 0056) |
| AX-2 | Sharded runs, with a build-once mode for Rust | P2 | M | AX-1 |  | Planned | In | (design 0056) |
| AX-3 | Live output, `logs`, and MCP tasks | P2 | L | PROTO-4 | Q55 | Planned | In | (design 0056) |
| LV-4 | An inventory of `unsafe`, with comments, a lint and Miri | P2 | M |  |  | Partial | In | 324, 330, 336, 337, 340, (design 0058) |
| LV-15 | Release runbook, secret inventory and a second releaser | P2 | M | LV-5 | Yes | Partial | In | 351, (design 0058) |
| LV-18 | Fuzz targets for the remaining parsers of untrusted bytes | P1 | M | LV-1 |  | Implemented |  | 326, 335, (design 0058) |
| LAUNCH-1 | Hold the public launch for the stability release and its records | P1 | S | TEST-24, TEST-25, LINK-1, ADOPT-1 | Yes | Planned | Out |  |
| LAUNCH-2 | Install and run from the README alone on clean machines | P1 | S | LAUNCH-1 |  | Planned | Out |  |
| LAUNCH-3 | Record the demo of the user story | P1 | M | LAUNCH-2 | Yes | Planned | Out |  |
| LAUNCH-4 | Put the demo, the release and the benchmarks at the top of the README | P1 | S | LAUNCH-3, BENCH-7 |  | Planned | Out |  |
| LAUNCH-5 | Announce the release | P2 | S | LAUNCH-4 | Yes | Planned | Out |  |
| COORD-1 | Claim design numbers, Q ids, phases and item ids on the hub | P2 | S |  |  | Implemented |  | 295, (design 0087) |
| COORD-2 | Changed files on the activity record, with an overlap check | P2 | S |  |  | Implemented |  | 295, (design 0087) |
| COORD-3 | One shared place for process lessons | P2 | S |  |  | Implemented |  | 294, (design 0087) |
| APP-4 | Trial: Anima Engine trials on the owner's two machines, checksums compared, a sweep through the grid | P1 | S |  | Yes | Partial | Out | (design 0063), (design 0089) |
| VERIFY-1 | Run a task again on other workers and compare its outputs (`submit --verify`) | P1 | M |  |  | Implemented |  | 296, (design 0089) |
| VERIFY-2 | Replicas of `submit --verify` on the same operating system and architecture by default (`--verify-any-system` for any) | P1 | S | VERIFY-1 |  | Implemented |  | 312, (design 0089) |
| APP-5 | Guide: Anima Engine sweeps on OxideSwarm, with a recorded run | P2 | S | APP-4, VERIFY-1 |  | Planned | Out | (design 0089) |
| BENCH-2 | The method and the sweep harness for benchmark records | P1 | M |  |  | Implemented |  | 314, (design 0093) |
| BENCH-3 | Sweep scaling: the fastest machine alone, each machine through the grid, both | P1 | M | BENCH-2 | Q90 | Partial | Out | 349, (design 0093) |
| BENCH-4 | Offload against local work, with the laptop's own CPU time | P2 | S | BENCH-2 | Q92 | Planned | Out | (design 0093) |
| BENCH-5 | Per-task overhead and the break-even task length on a LAN | P2 | S | BENCH-2 |  | Planned | Out | (design 0093) |
| BENCH-6 | Two networks: link kind, latency, transfer rate and a sweep | P1 | S | BENCH-3, BENCH-5 | Yes | Planned | Out | (design 0093) |
| BENCH-7 | Benchmark headlines at the top of the README; the interconnect report by Q91 | P1 | S | BENCH-3, BENCH-4, BENCH-5, BENCH-6 | Q91 | Planned | Out | (design 0093) |
| BENCH-8 | Find why the two-machine W1 sweep reached 73 % efficiency, not 85 % | P2 | S | BENCH-3 |  | Implemented |  | 355, (design 0093) |
| BENCH-9 | Split the per-machine cost of running W1 through the grid into task process time and slot idle time | P2 | S | BENCH-8 | Q93 | Partial | Out | 354, (design 0093) |
| ECO-9 | Per-participant quotas: running tasks, slots and priority | P2 | L | ENROLL-1 | Q97 (decided 2026-11-19) | Planned | Out | (design 0094) |
| NET-9 | Contributor controls on each machine: CPU, hours, battery, metered network, a local stop | P2 | M |  | Q97 (decided 2026-11-19) | Planned | Out | (design 0094) |
| NET-10 | Contribution credits per participant, display only | P2 | M | ECO-9 | Q96, Q97 (decided 2026-11-19) | Planned | Out | (design 0094) |
| APP-6 | Pilot campaign: an Anima Engine sweep on several owners' machines, every result verified | P2 | M | ENROLL-1, APP-4, NET-9, NET-10 | Q94, Q95 (decided 2026-11-19) | Planned | Out | 376, 383 (open), (design 0094) |

## Phase 0: integrate open work

Do this phase first. Later items assume its result.

## Phase 1: stop the harm

These items remove ways to lose data or to run code without authorisation.

## Phase 2: make the tests trustworthy

The full suite must pass on a clean machine before CI can require it.

### TEST-23: Find why `master_restart_recovery` loses `out.txt` on Windows

- **Evidence:** CI run `42817398752`, job `129833857865`, "workspace tests (windows-latest)", on
  pull request 206 at `e1fcf70` (2026-11-10). That pull request changes only `docs/`.
  `test_master_restart_recovers_finished_queued_and_adopts_running_task_once` panicked at
  `tests/master_restart_recovery.rs:575` with "t_run must succeed". The result of `t_run` had
  `exit_code: 1`, an empty `stdout`, `execution_time_ms: 746`, no outputs, and the error
  "Output files: the task did not produce 'out.txt'". The same job passed on `master` `0354b28`,
  on the two earlier heads of the pull request (`0266d8e`, `7af1f4d`) and on the rerun of the
  failed job (attempt 2). Linux and macOS passed in the same run. Seen once.
- **Problem:** The cause is not known. In the test, `t_run` starts on `w1` under the first master
  and waits for a gate file. The first master shuts down, a second one starts on the same address
  and `--state-dir`, `w1` reconnects, and the second master adopts `t_run` as Running. The test
  then writes the gate file and expects `t_run` to finish with the declared output `out.txt`.
  On Windows the task is the PowerShell command built by `gated_marker_task`, which writes the
  file with `[System.IO.File]::WriteAllText($env:OUT_FILE, $env:OUT_BODY)` and the relative name
  `out.txt`. Two explanations fit the message, and neither has been tested:
  1. The test is at fault: the PowerShell command ends without writing the file where the worker
     looks for it (the Unix variant uses a shell redirect, the Windows one a .NET call).
  2. The product is at fault: after the worker is adopted by a restarted master, output
     collection reads the sandbox before the file is there, or reads another sandbox. That would
     be a defect in the recovery path of FEAT-1, not in the test.
- **Do:**
  1. On Windows, run `cargo test -p rusty_grid_master --test master_restart_recovery --locked`
     in a loop under CPU load until it fails, and keep the worker and master logs of each run.
  2. For a failing run, record the exit code of PowerShell itself, the sandbox path the worker
     collected from, whether `out.txt` exists anywhere under the sandbox base, how many lines
     the marker file holds, and the time from assignment to the gate file (compare it with
     `execution_time_ms`).
  3. If the test is at fault, change only the test, and keep every assertion. If the product is
     at fault, follow rule 4: add a test that fails without the fix, then fix the product.
- **Done when:** The cause is written in the status log, and the test passes 50 times in a row
  on Windows under CPU load. If production code changed, a regression test covers the cause.

- **Note (2026-11-16):** The failed result narrows the search. The worker sets `exit_code` 1
  only when a process that exited 0 misses a declared output, and it appends its note to the
  process's own stderr (`crates/worker/src/runner.rs:1776-1786` and `append_note` at `0354b28`).
  The result held that note and nothing else, and `error` held only the output message. So
  PowerShell ran the whole script, exited 0 and printed nothing: no command in it failed. Either
  `$env:OUT_FILE` was empty in that run, or `WriteAllText` wrote `out.txt` relative to a .NET
  current directory other than the directory the worker read. 746 ms is short for a run that spans
  PowerShell start-up, a master restart and a reconnect, and a second run of `t_run` that started
  after the gate was written would fit it. Step 6 of the test cannot tell the two apart: adoption
  and a new assignment to `w1` both show `Running` with `active_tasks == 1`. A failing run should
  therefore also record the number of `run` lines in the marker file (two means a second run), the
  worker log line "Task assigned again while its previous run is still active"
  (`retire_previous_run` in `crates/worker/src/client.rs`), and, from the script,
  `[Environment]::CurrentDirectory` and `$env:OUT_FILE`.

- **Lead (2026-11-17, pull request 288):** looping the whole `master_restart_recovery` binary on macOS under CPU load
  (140 runs), `test_worker_disconnected_outbox_uploads_output_blobs_and_flushes_result_on_resume` failed 9 times
  ("uploading output files to the master failed: blob transfer failed: the connection to the master ended") and
  `test_master_restart_recovers_finished_queued_and_adopts_running_task_once` once (`out.txt: No such file or
  directory`, then "Output files: cannot read the task sandbox": the task sandbox was gone). Unverified hypothesis: the
  tests share `<tmp>/rusty_grid-<uid>/sandboxes`, and the start-up sweep of a worker that starts (`sweep_leftover_sandboxes`)
  takes the lock of, and removes, a sandbox directory whose creator does not hold its lock yet.

- **Implemented (2026-11-17, pull request 305, [design 0091](design/0091-restart-recovery-races.md)), Partial:** On macOS, with eight copies of the tests in parallel and twenty busy loops, the unchanged code failed `test_worker_disconnected_outbox_uploads_output_blobs_and_flushes_result_on_resume` 3 times in 192 runs ("the connection to the master ended"). The cause is not the shared sandbox base: when the connection ended during the upload of a finished task's outputs, the worker overwrote the buffered result with the failure of that upload, and the resume sent the failure. The worker now keeps the buffered result when the session ended (0 failures in 128 runs of the same two tests). Separately, a real race in the sandbox lock was found and fixed: `open_and_try_lock_task` could keep a lock on a lock file that a sweep had just unlinked, after which a second opener could lock a fresh file, so a sweep could remove the sandbox of a running task (`sandbox::tests::test_task_lock_taken_on_an_unlinked_file_is_not_kept` fails without the fix). That race was reproduced with a hook, not in the loops, so it is not shown to be the cause of the `out.txt` failure on Windows or of the one "sandbox gone" run seen in the lead. Looping the whole binary with the fix (48 runs, twelve busy loops) showed no `out.txt` or upload failure, only two 5-second `wait_for` timeouts in other steps under that load. TEST-23 stays open until the Windows job has passed 50 runs in a row; the Windows cause is still unknown.
- **Partial (2026-11-19, pull request 357):** Not reproduced on Windows (`x86_64`, 20 logical cores, `rustc 1.98.1`) in 200 completed runs (0 failures) of `cargo test -p rusty_grid_master --test master_restart_recovery --locked` under 12 busy loops on 20 logical cores (about 60 %): batch 1 passed 50/50 at `979ec2c` in 1,689.1 s; an earlier 24-spinner attempt stalled on run 51 (0 runs completed, 0 failures) before pausing for BENCH-8, and a restarted batch-2 attempt with 12 spinners was stopped at 18:28:25Z for BENCH-8 after 0 completed runs (0 failures); batches 2 to 4 then ran 150 unbroken consecutive runs (3 batches of 50) at `b079dc3` in 1,429.0 s, 1,302.1 s and 1,309.0 s (5,729.2 s across the 4 measured batches; 0 `out.txt` failures and 0 test failures). The 200 passed runs therefore span the BENCH-8 interruption and two commits (`979ec2c` and `b079dc3`, whose diff touched no `crates/master`, `crates/worker` or `tests/master_restart_recovery.rs` files; 150 runs are unbroken on `b079dc3`). Running 24 normal-priority CPU spinners on 20 logical cores starved the worker's default `BELOW_NORMAL_PRIORITY_CLASS` child task processes and stalled progress without producing an `out.txt` failure, whereas 12 busy loops on 20 logical cores (about 60 %) allowed child tasks to schedule. Because zero runs failed in 200 completed loops, no Windows cause is guessed here.

### TEST-24: Test large transfers between machines

- **Evidence:** Read, on 2026-11-14 at `master` `f486885`. The largest file in the end-to-end
  blob tests is 5 MiB plus 123 bytes (`tests/blob_transfer_e2e.rs:183`). The largest tree is
  10,000 small files (`tests/blob_upload_pipelining_e2e.rs`). The defaults allow far more: the
  master's blob store holds 4 GiB (`DEFAULT_BLOB_STORE_MAX_BYTES`), the worker cache 2 GiB
  (`DEFAULT_BLOB_CACHE_MAX_BYTES`) and a task's outputs 1 GiB (`DEFAULT_OUTPUT_LIMIT_BYTES`).
  From the code, a dropped upload starts again at offset 0 (`blob_store.rs`: `write_chunk`), and
  one blob moves at most 4 MiB per round trip (`blob_transfer.rs`: `upload_blob`,
  `download_blob`).
- **Problem:** Nobody knows whether memory stays flat as a file grows, how long a large input
  takes over LAN or a relay, or whether a slow TCP link can delay a heartbeat past the 10 s
  timeout during a transfer. Design 0062 plans to move model weights of several GB.
- **Do:** Follow D1 and D2 of [design 0065](design/0065-large-transfer-and-long-run-tests.md).
  1. Add `benchmarks/endurance/endurance.py` with the subcommands `transfer`, `throttle` and
     `sample`, standard library only, under the rules of `benchmarks/grid/bench_grid.py`: own
     processes, private homes, stop by handle, no host names in records.
  2. Run scenarios T1 to T9 on loopback, then on two machines over LAN TCP, LAN QUIC and a relay.
     Use a master started for the run with a temporary home and its own ports, never the trial
     cluster or `~/.oxideswarm`.
  3. Add `tests/test_endurance_harness.py` (helpers without a binary) and run it in the CI job
     `agent-bridge`.
  4. For every failed check, open an item or write a failing regression test (rule 4).
- **Done when:** The records of T1 to T9 for loopback and for two machines are committed under
  `benchmarks/endurance/results/`, and every failed check has its own item or regression test.
- **Partial (2026-11-19, pull request 374):** Added `benchmarks/endurance/endurance.py` (`transfer`, `throttle`,
  `sample`, Python 3 standard library only), `benchmarks/endurance/README.md`, and
  `tests/test_endurance_harness.py` (`14/14` passed, wired into CI `agent-bridge`), and ran
  loopback scenarios on Windows `x86_64` (`rusty-grid 0.4.3` at `a3e5b71`, Windows Defender
  real-time protection on): `7/7` passed in `2,648.17 s`
  (`benchmarks/endurance/results/2026-11-19-loopback-windows-amd64-intel-core-i5-14600kf.json`):
  `T1` (`1 GiB` `10.033 s`, `3 GiB` `67.899 s`; peak RSS delta `-3.9 MiB` master / `-2.8 MiB`
  worker / `+0.64 MiB` client vs `128 MiB` bound), `T2` (`20,000` files, `173,998,510 B`,
  `233.124 s`, `20,001` blobs stored), `T3` (`1,000 MiB` output `10.757 s` ok; `1,100 MiB`
  refused naming `--output-limit-mb`), `T4` (second `3 GiB` submit uploaded `0` blobs / `0 B`),
  `T6` (`64 MiB` fetch cut via loopback proxy recovered cleanly in `4.440 s`, `0` `.part`
  leftovers), `T7` (`256 MiB` in + `256 MiB` out over `2.0 Mbit/s` / `50 ms` RTT TCP finished in
  `2,164.622 s` with `worker_reaped = false`), and `T8` (`--blob-store-mb 64` refused `72 MiB`
  and concurrent `40 + 40 MiB` uploads naming `--blob-store-mb`, `0` tmp leftovers). `T1 (8 GiB)`,
  `T2 (100,000 files)`, `T5`, `T9`, and two-machine `lan-tcp` / `lan-quic` / `quic-relay` links
  remain for step 2.

### TEST-25: Run a cluster for 24 hours under load and faults

- **Evidence:** Read, on 2026-11-14 at `master` `f486885`. Every CI job has a timeout of at most
  90 minutes (`.github/workflows/ci.yml`). The longest script outside CI, `bench_grid.py`, takes
  about 10 minutes. No committed record measures the memory, handles or disk of a cluster over
  hours.
- **Problem:** A slow leak of memory, handles or disk, or a task lost after many restarts, shows up
  only after hours. The USB kit runs workers for days on other people's machines.
- **Do:** Follow D3 of [design 0065](design/0065-large-transfer-and-long-run-tests.md).
  1. Add the subcommand `soak` to `benchmarks/endurance/endurance.py`, with a seeded load, the
     faults of D3 (worker kill, master restart on its state directory, link cuts, bursts to the
     queue cap) and a ledger of every task submitted.
  2. Run it for 24 hours on a Mac master and two Windows workers, with temporary homes and their
     own ports (Q57).
- **Done when:** The 24-hour record is committed. It passes the checks of D3, or every failed
  check has its own item or regression test.
- **Partial (2026-11-19, step 1, pull request 381):** Added `soak` subcommand
  of D3 to `benchmarks/endurance/endurance.py` (`--duration`, `--fault-scale`, `--load-scale`,
  `--sample-interval`, `--workers`, `--slots`, `--workspaces`, `--max-queue-size`, `--state-dir`
  master restart, worker kill/restart, `ThrottleProxy` link cuts, queue-cap burst, `TaskLedger`
  + master `/api/tasks` + log evidence audit, D3 item 6 checks + fault-completeness check), unit
  tests in `tests/test_endurance_harness.py` (`20/20` passed), and one 45-minute Windows `x86_64`
  loopback record on `rusty-grid 0.4.4` at `4c11a45`
  (`benchmarks/endurance/results/2026-11-19-soak-45m-loopback-windows-amd64-intel-core-i5-14600kf.json`):
  all `6/6` checks passed in `2,704.22 s` across `2,185` submitted and unique master-logged tasks
  (`1,350` `noop`, `225` `new_input`, `225` `reused_input`, `90` `output`, `45` `workspace_sync`,
  `45` `cancel`, `45` `probe`, `160` `queue_burst`; `2,140` `ok`, `45` `cancelled`, `0`
  lost/duplicate/hash mismatch/submit failure/queue errors; `4` retries/orphan requeues reported by
  `/api/tasks` and master `INFO` logs; debug-only stale/late counts unmeasured at `INFO`), `16/16`
  injected faults (`8` worker kills, `5` link cuts, `2` queue bursts, `1` master restart), per-process
  master RSS `16.0 -> 35.5 MiB` (gen 0) and `29.2 -> 40.3 MiB` (gen 1) and worker RSS
  `15.2–31.7 MiB`, `journal.bin` shrinking `1,525,821 -> 24 B` and `801,453 -> 24 B`, idle stores
  and mid-run worker cache (`<= 2,125.0 MiB` peak during pinned work, `<= 2,030.0 MiB` idle) within
  bounds, fault-excluded clean probe p99 `130.85 -> 146.51 ms` (`14`/`16` samples, ratio `1.1197`),
  and `0` leftover PIDs or task process trees. The 24-hour 3-machine run (Mac master + two Windows
  workers, Q57) remains for step 2.
- **Update (2026-11-20, 22.7 h Windows loopback soak record, branch `win/test-25-24h-soak-record`):**
  Ran `endurance.py soak --duration 24h` (`rusty-grid 0.4.4` at `cfb0679`, `--fault-scale 1.0`,
  `--load-scale 1.0`, `3` workers x `4` slots, `max_queue_size=64`) from `2026-11-19T14:22:47Z` to
  `2026-11-20T13:06:33Z` (`81,826.46 s` / `22.73 h`), stopped early at owner decision with ~`1.27 h`
  left. Assembled
  `benchmarks/endurance/results/2026-11-19-soak-24h-loopback-windows-amd64-intel-core-i5-14600kf.json`
  from preserved master/worker logs, live CLI status (`pending_tasks=0`, `running_tasks=0`,
  `active_bytes=0`), `GET /api/tasks`, periodic RSS/handle snapshots, and idle disk/state checks
  (`endurance.py` kept in-memory `TaskLedger`, 60 s `BackgroundSampler`, `probe_samples`, and
  `fault_events_log` without writing a partial record on interrupt):
  - Check 1 (Accounting): `63,059` logged master results across `12` master generations and `71`
    worker logs (`63,059` unique, `0` duplicate results, `0` queue record errors, `112` failover
    sweeps, `27` orphan requeues, `0` log-level `ERROR` or panics); exit codes `61,642` `0`, `1,360`
    `130` (cancelled), and `57` `124` (`60 s` worker watchdog timeout clustered at
    `2026-11-20T06:30–06:31Z` [`9`] and `10:33–11:55Z` [`48`] coincident with `25–52 s` master log
    gaps); client-side `TaskLedger` SHA-256 checks `not_evaluated_due_to_early_stop`.
  - Check 2 (Resource plateau): `not_evaluated_due_to_early_stop` (continuous 60 s means lost);
    observational snapshots remained flat late in the run (master `62.7 -> 86.9 -> 85.16 MiB` /
    `191` handles; workers `30.0–31.1 -> 54.78–55.84 MiB` / `301–309` handles at `+22h43m`).
  - Check 3 (Disk & journal): idle `master-blobs` `4,244,637,442 B` (`<= 4 GiB` cap, `7,279` LRU
    eviction events evicting `12,221` blobs) and idle worker caches `w0/w1/w2`
    `2,102,395,622 / 2,105,542,715 / 2,082,471,936 B` (`<= 2 GiB` cap), `0` `.part`/`.tmp`/`.locks`
    leftovers; automatic runtime journal compaction shrank `journal.bin` from `2,104,993 B`
    (`13:02:16Z`) to `54,043 B` (`snapshot.bin` `13,315,075 B`) around `13:05Z`; continuous 60 s
    peak worker cache and scheduled-restart compactions `not_evaluated_due_to_early_stop`.
  - Check 4 (Probe p99) & fault-events list: `not_evaluated_due_to_early_stop`.
  - Check 5 (Clean stop): `passed` (`0` leftover soak/cluster/keep-awake processes, work dir removed).
  Stays `Partial` pending the full 24-hour 3-machine (Mac master + two Windows workers) run.

### TEST-26: Run short versions of TEST-24 and TEST-25 on a schedule in CI

- **Evidence:** `.github/workflows/ci.yml` already runs the jobs `audit` and `p2p-wan-relay` on a
  weekly schedule and on `workflow_dispatch`.
- **Problem:** Without a scheduled run, a change can bring back a defect that TEST-24 or TEST-25
  found, and nothing notices until the next full run.
- **Do:** Add a job `endurance` on `ubuntu-latest` for `workflow_dispatch` and the weekly
  schedule, never for pull requests. It runs T1 at 1 GiB, T2 at 20,000 files, T4, T6, T7 and T8 on
  loopback and a soak of 45 minutes, and uploads the JSON record as an artifact. It is not a
  required check.
- **Done when:** The job is green on two scheduled runs.
- **Partial (2026-11-19, branch `win/test-26-ci-endurance`):** Added the `endurance` job on
  `ubuntu-latest` (`timeout-minutes: 150`, pinned action commit SHAs) to
  `.github/workflows/ci.yml` for `schedule` (`cron: "17 3 * * 1"`) and `workflow_dispatch` only
  (never on pull requests, not a required check). It builds `target/release/oxideswarm`, selects
  the larger of `/mnt` and `$RUNNER_TEMP`, runs `endurance.py transfer` (`T1,T2,T4,T6,T7,T8`,
  `--t1-sizes-gib 1 --t2-files 20000`) and `endurance.py soak` (`--duration 45m`), and uploads
  `endurance-records` (`transfer-loopback.json` and `soak-45m-loopback.json`, `if: always()`).
  Static checks in `tests/test_endurance_harness.py` (`EnduranceCiWorkflowTests`) verify trigger,
  pins, commands, and upload step. Stays Partial until green on two scheduled runs.

### TEST-61: macOS: hard-link isolation test control printed empty stdout

- **Evidence:** Failed once on `macos-latest` in pull request 380 (docs-only commit `ad915a9b`, run `44537862155` attempt 1, job `135607992314`, 2026-11-19 ~14:23 UTC; attempt 2 passed): `macos::test_a_hard_link_to_a_secret_does_not_expose_its_content` (`crates/worker/tests/test_feat3_os_isolation.rs:1757`) panicked at its `--isolation off` control step 45 ms after the binary started with `the control must read the linked secret: ` and empty stdout (not even `END`).
- **Problem:** The control task printed nothing at all even under `--isolation off`, and the assertion omitted `exit_code`, `error`, `execution_time_ms`, and `stderr`, leaving unknown whether the process exited non-zero, failed to spawn, failed sandbox setup, or hit an fd/pipe race after `BUG-33` (pull request 367, `close_open_fds_macos`).
- **Do:** Capture full `TaskResult` diagnostics when the control prints empty stdout; loop the test (50 runs) and its binary (20 runs) with and without 6 CPU busy loops; prove whether `BUG-33`'s `close_open_fds_macos` path can race with the task's stdout pipe or spawn; fix any defect or harden diagnostics the `TEST-60` way.
- **Done when:** `BUG-33` pipe/spawn safety is proven, diagnostics capture any future failure, and the root cause is fixed or left for the owner's decision after non-reproduction under loaded loops.
- **Partial (2026-11-19, pull request 392, open):** Not reproduced locally on Apple M5 (10 cores, private `HOME`, `origin/master` `8641b67d`): `0/140` failures across `50/50` alone unloaded (`49.15 s`), `50/50` alone under 6 Python busy loops (`77.32 s`), `20/20` full `test_feat3_os_isolation` unloaded (`547.76 s`), and `20/20` full `test_feat3_os_isolation` under 6 busy loops (`628.09 s`), plus `5/5` in the 5 full loaded workspace runs on `origin/master`. Proof on `BUG-33` (`crates/worker/src/process_tree.rs`): `close_open_fds_macos(&[read_raw])` runs solely inside the double-forked `setsid()` grandchild `child_c` (separate OS PID and XNU descriptor table), skips `read_raw` via `fd_is_kept`, and closes only `child_c`'s inherited duplicates, so it cannot close or alter `child_a`'s stdout write end or the worker's pipe read end; a new 128-spawn 8-worker multithreaded stress test (`process_tree::tests::test_concurrent_short_spawns_preserve_piped_stdout_under_supervisor_fd_close`, with 64 extra `FD_CLOEXEC` descriptors open) passes `128/128` exact stdout captures. Fixes landed: (1) `PreparedUnix::drop` (`crates/worker/src/process_tree.rs`, sharing `imp::dismiss_lifeline` with `TreeInner::kill_and_release_once`) writes `b"0"` before closing `lifeline_write` when dropped unattached (e.g., failed spawn after `pre_exec` forked `child_c`), preventing an orphan supervisor from running `killpg` and a process-table sweep (`test_prepared_unix_drop_dismisses_lifeline_without_attach`); (2) `read_into_capture` (`crates/worker/src/runner/capture.rs`) warns on non-`EINTR` stream read errors; (3) `format_result_diag` (`crates/worker/tests/test_feat3_os_isolation.rs`) reports `exit_code`, `error`, `execution_time_ms`, `stdout`, and `stderr` on `test_a_hard_link_to_a_secret_does_not_expose_its_content` and sibling control assertions. Post-fix verification on `mac/test-61-hardlink-control-empty-stdout`: `process_tree` `23/23` (`2.20 s`), `runner::process_tree_tests` `13/13` (`2.08 s`), `test_feat3_os_isolation` `28/28` + `1` ignored (`27.02 s`), `test_sec5_hardened_runner` `11/11` (`2.58 s`), and under 6 Python busy loops `20/20` `process_tree::tests` (`50.59 s`, `0.98–2.07 s`) and `20/20` `test_a_hard_link_to_a_secret_does_not_expose_its_content` (`36.82 s`, `0.58–1.20 s`). Closing `TEST-61` without a reproduced cause (keeping diagnostics for any future CI hit) stays the owner's decision.

## Phase 3: make the documents true

The standard in `docs/README.md` requires "only what the code does". These items apply it.

## Phase 4: finish partial features

### 3D-2: Wire VRAM and Blender routing

- **Evidence:** Read. Confirmed by grep.
- **Problem:** `required_vram_mb` (`crates/master/src/scheduler.rs`) still returns preset defaults for `Generative3D` that are not automatically applied when `--vram-mb` is omitted, and `crates/master` does not read `has_blender` (explicit `TaskRequirements::vram_mb` via `--vram-mb` / MCP `vram_mb` is now enforced under `FEAT-9a` / [design 0068](design/0068-task-vram-accounting.md)). `--blender-path` reaches only the capability probe, and `resolve_blender_binary` misses the macOS and Linux paths that `detect_blender` searches. A Mac can advertise Blender and then fall back silently.
- **Do:** Wire both into `view_fits`. Share one Blender resolver.
- **Done when:** A `--preferred-provider blender` task lands only on a Blender worker. A `high` task
  skips a 2 GB worker.

### 3D-3: Make AutoRig options real or refuse them

- **Evidence:** Read.
- **Problem:** `--rig-preset quadruped` gives the biped skeleton. `engines/autorig/blender_rig.py:23`
  parses `--preset` and never reads it. `turnaround` produces an empty action in Blender. OBJ input
  fails, because it is always written as `input_mesh.glb`. The Blender timeout is fixed at 60 s
  (`crates/worker/src/runner.rs:2201`). A failure falls back with only a `warn!` and the same device
  label. The blocking engines take no timeout or cancel.
- **Do:** Refuse unimplemented presets or implement them. Pick the extension from the magic bytes.
  Pass the task timeout. Report the provider. Fail when Blender was requested explicitly.
- **Done when:** `quadruped` gives a non-biped skeleton or an error. An OBJ goes through Blender. The
  result names the provider. A timeout returns within 1 s of expiry.

### MESH-2: Deliver mesh data to a consumer

- **Evidence:** Read.
- **Problem:** Nodes push payloads into an unbounded `Vec` that only tests read
  (`crates/agent_mesh/src/client.rs:252`). `agent-mesh data` sleeps 200 ms and exits 0 without reading
  the ACK (`crates/agent_mesh/src/main.rs:424`). `total_chunks` is always 1. Node status is always
  `online`, and telemetry fields are placeholders.
- **Do:** Add `agent-mesh node --inbox-dir` with a bounded buffer. Wait for the ACK and exit non-zero
  on failure. Report real values, or remove the fields.
- **Done when:** `agent-mesh data --to missing-node` exits non-zero. A payload appears on disk with a
  verified hash.

### MESH-3: Attach the bridge to a running master

- **Evidence:** Read.
- **Problem:** `AgentGridBridge::new` takes an in-process `MasterHandle`
  (`crates/agent_mesh/src/bridge.rs:29`). Mesh tasks therefore run on a second master with no workers
  until someone points them at it. `grid_submit` overwrites tags, has no files or outputs, and has no
  cancel. `parse_binary_data` reads hub-local paths that a peer names (`bridge.rs:857`).
- **Do:** Add a `GridBackend` trait with in-process and `connect_master` versions, and
  `--grid-connect <HOST>:8088`. Add placement, files, outputs, result and cancel. Remove the file
  fallback. Document that workers must join.
- **Done when:** A test with `CARGO_BIN_EXE_agent-mesh` runs `grid_submit` on a separately started
  master.

### AND-1: Make the Android app build

- **Evidence:** Read. The compiler was not run, because the audit machine has no SDK platform or NDK.
- **Problem:**
  - `MainActivity.kt:317` uses `WifiManager` without an import.
  - `gradle-wrapper.jar` has no `Main-Class`, so `packaging/android/app/gradlew` cannot start.
  - No `gradlew.bat` exists.
  - No CI job or test covers the app. Nothing shows that it was ever built.
- **Do:**
  1. Add the import, and regenerate the wrapper.
  2. Add a CI job with the NDK, `build_android_jni.sh` and `./gradlew assembleDebug`.
- **Done when:** CI produces `app-debug.apk` containing `lib/arm64-v8a/liboxideworker.so`. An owner
  run on an emulator shows the worker in `oxideswarm workers`.

### AND-2: Keep the master across restarts

- **Evidence:** Read.
- **Problem:** The P2P ticket is never saved. After a reboot the app dials the literal host
  `iroh-p2p` (`BootCompletedReceiver.kt:37`). A `START_STICKY` restart has a null intent and defaults
  to `127.0.0.1:8088` (`OxideWorkerService.kt:158`). Android 9 and later block the cleartext probe in
  `MainActivity.kt:353`. `crates/android_bridge/src/lib.rs:222` assigns a new worker id on every
  start.
- **Do:** Store the ticket beside the secret. Load saved settings on a null intent. Allow cleartext
  for the probe, or drop it. Persist the worker id.
- **Done when:** With a ticket configured, a process kill and `adb reboot` both re-register under the
  same worker id.

### AND-3: Wire pre-sleep evacuation

- **Evidence:** Read.
- **Problem:** `nativeTriggerFastEvacuation` has no Kotlin declaration, and no Kotlin code listens for
  device idle. `trigger_fast_evacuation_impl` sends the permanent shutdown signal, so nothing
  reconnects after sleep. `Disconnecting` has no drain field. The test named `_under_50ms` allows
  100 ms (`tests/mobile_coordination_test.rs:901`).
- **Do:** Declare the external, and trigger it from `OxideWorkerService.kt` on a real OS signal. Make
  evacuation resumable.
- **Done when:** On an emulator, `dumpsys deviceidle force-idle` produces a `Disconnecting` frame, the
  master requeues the task, and the worker registers again after idle ends.

### AND-4: Report real worker status

- **Evidence:** Read.
- **Problem:** `onNativeLog` and `onNativeStatusUpdate` have no caller. The app reports RUNNING when
  the thread starts and shows "Connected" before any connection. In-process worker logs go nowhere.
  An unknown temperature of `-1.0` passes as a real reading.
- **Do:** Poll `nativeGetWorkerStatus` or add JVM callbacks. Install a logcat subscriber. Report
  RUNNING only after registration. Fix the sentinel.
- **Done when:** A wrong master address shows an error in the notification, and `adb logcat` shows
  worker logs.

## Phase 5: larger designs

Each item below needs a design record before code.

### FEAT-5: Checkpoints for process tasks

- **Evidence:** Read.
- **Problem:** Checkpoints work for the `BuiltinTest` hash branch only
  (`crates/worker/src/runner.rs:853-917`). `Command`, `ShellScript`, `RustCompilation` and
  `GpuCompute` never receive one. The 3D tasks send progress as checkpoints.
- **Do:** Add `OXIDESWARM_CHECKPOINT_DIR` in the sandbox, stored in capped `Checkpoint` frames and
  restored before a re-run. Separate progress from state.
- **Done when:** A `Command` test kills the worker, and a second worker finds the state file and skips
  finished steps.

### CACHE-1: Reuse the result of an identical task

- **Evidence:** Read, on 2026-11-08 at `master` `fec164e`. No code in `crates/` maps a task to an
  earlier result: every submission is placed and run. Input files already have content digests
  (`BlobId` is an unkeyed BLAKE3 hash, `crates/core/src/blob.rs:55`), so a task's inputs can be
  named by hash. The Bazel Remote Execution API keys its `ActionCache` by the digest of an `Action`
  (command, input root and platform) and stores an `ActionResult`
  ([remote_execution.proto](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)).
- **Problem:** A coding agent that reruns `cargo test` on an unchanged tree waits for a full run.
  Workspaces (WS-1 of design 0039) make a rebuild after an edit incremental, but an exact repeat
  still runs from the start.
- **Do:**
  1. Write a design record first. It fixes the key: a BLAKE3 hash over a canonical encoding of the
     command and arguments, the environment, the working directory, the input tree digests, the
     output patterns, and the placement constraints and worker tags that can change a result
     (`os:`, `arch:`, `iso:`).
  2. Make caching opt-in per task (`submit` flag and MCP argument). A task without declared inputs
     cannot be cached, because the key cannot cover what its command reads. The design record
     states that a command which reads the network, the clock or files outside its inputs can
     return a stale result, and that this is why caching is opt-in.
  3. Store only results with exit code 0 and every declared output present. A hit returns the stored
     `TaskResult`, marked as cached, and its output blobs, without placing the task. Bound the cache
     by size with least-recently-used eviction, and pin the output blobs of each entry.
  4. Follow the wire rule for the cached marker, with a protocol version bump (FEAT-8).
  5. Record the hit rate on the WS-7 user story before MCP turns caching on by default.
- **Done when:** A test submits the same cacheable task twice: the worker runs it once, and the
  second result downloads byte-identical outputs. A change to one input file, one environment
  variable or the `os` constraint is a miss. A failed run is not stored. An entry survives a master
  restart.

### REAPI-1: Serve the blob store as a Remote Execution API cache

- **Evidence:** Read, on 2026-11-08. REAPI v2 defines `DigestFunction` `BLAKE3 = 9`, and a server
  may implement only `Capabilities`, `ContentAddressableStorage`, `ActionCache` and
  `google.bytestream.ByteStream`, with execution disabled
  ([remote_execution.proto](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)).
  NativeLink has advertised BLAKE3 since
  [pull request 403](https://github.com/TraceMachina/nativelink/pull/403) (2023-11-20). A `BlobId`
  equals a REAPI BLAKE3 `Digest.hash`, and blob sizes already travel next to ids, so no rehashing
  is needed.
- **Problem:** Bazel, Buck2 and other REAPI clients cannot use the owner's machines as a remote
  cache.
- **Do:**
  1. Write a design record. It puts the gRPC listener behind the cluster secret, on a port of its
     own, behind an opt-in Cargo feature, and lists the new dependencies for the owner to approve.
  2. Implement the cache-only profile: `Capabilities` reports BLAKE3 only, CAS and `ByteStream`
     read and write the master's `BlobStore`, and `ActionCache` reads and writes the CACHE-1 store.
  3. Leave the `Execution` service out.
- **Done when:** A REAPI client set to the BLAKE3 digest function, for example Bazel with
  `--digest_function=blake3`, builds a sample project twice against the master, and the second
  build reports remote cache hits. A client without the cluster credential is refused.

## Phase 6: release

### JOIN-1: Join a machine with one token

- **Evidence:** Read, on 2026-11-08 at `master` `fec164e`. A master tells the owner to copy
  `cluster.secret` to each machine by hand, at mode 600 (`crates/cli/src/secret_cmd.rs:252`). A
  worker then also needs the master's address or a P2P ticket (`--p2p-ticket`,
  `crates/cli/src/cli_app.rs:371`). `docs/guides/two-machine-cluster.md` walks through both steps.
  Tailscale, which users compare a join against, asks only to install the client and log in on
  each device ([what is Tailscale](https://tailscale.com/docs/concepts/what-is-tailscale.md)).
- **Problem:** Joining a machine takes two values carried over two channels, and a wrong file mode
  or a stale ticket shows up only as a refused handshake.
- **Do:**
  1. Write a design record first, because this adds a CLI command.
  2. Let the master print, on explicit request only, one join token that carries the master's
     address or P2P ticket and the cluster secret. The token is as sensitive as the secret: it is
     never logged and never written to a config file.
  3. Add a join command that reads the token from standard input, so it stays out of shell history
     and process lists. It writes the secret to `~/.oxideswarm/cluster.secret` at mode 600,
     refuses to replace a different secret without an explicit flag, saves the address or ticket,
     and then runs the `doctor` checks.
  4. Add a QR code form for the Android app after AND-1.
  5. Update `README.md` and `docs/guides/two-machine-cluster.md`.
- **Done when:** A test with two temporary `HOME` directories creates a token on the master side,
  joins the worker side with it on standard input, and `doctor` passes. The written secret file has
  mode 600 on Unix. A token with one changed byte is refused. Neither stdout nor stderr of either
  process contains the secret.

### LINK-1: Show whether each link is direct or relayed

- **Evidence:** Read, on 2026-11-08 at `master` `fec164e`. The master tags each P2P worker with
  `link:Direct P2P (QUIC):<rtt>` or `link:Relay (DERP):<rtt>` (`crates/master/src/server.rs:2869`).
  `doctor` reads the tag into `WorkerSummary::link` (`crates/cli/src/doctor.rs:121`), but `render`
  (`doctor.rs:205`) never prints it, and no check warns about a relayed worker. The P2P endpoint
  uses either n0's public relays (`presets::N0`) or none (`presets::Minimal`,
  `crates/master/src/server.rs:552`). No option names a relay of the owner's own. The n0 public
  relays are rate-limited and meant for development, and a self-hosted relay needs a public IP
  address and a DNS name ([iroh hole punching](https://docs.iroh.computer/concepts/holepunching)).
- **Problem:** A worker that falls back to a relay moves blobs more slowly, and the owner cannot see
  it from `doctor`. A cluster that outgrows the public relays has nowhere else to go.
- **Do:**
  1. Print each worker's link kind and round-trip time in the `doctor` table, and add a check that
     warns when a worker is relayed.
  2. Add a `doctor` probe that moves a fixed-size blob to each worker and reports MB/s.
  3. Add a relay URL option for master, worker and client that selects a custom iroh relay. Write
     its design record and its `README.md` reference entry.
  4. Write a guide in `docs/guides/` for running a self-hosted relay.
- **Done when:** A `doctor` unit test renders the link column and a warning for a relayed worker. A
  test resolves the relay option to a custom relay map. A live run through a self-hosted relay is
  recorded with its date (steps 1 and 2 done in pull request 159; steps 3 and 4 done in
  [design 0049](design/0049-custom-relay.md) and
  [docs/guides/self-hosted-relay.md](guides/self-hosted-relay.md); the live relay run is pending
  for the owner).

## Phase 6b: machines anywhere

Added on 2026-11-09 after the owner chose ecosystem model A (Q13): each person or team runs a
cluster, and trusted people add their machines to it from any network. Order: ENROLL-1 first,
then JOIN-2 and USB-1 in parallel.

### ENROLL-1: A key per machine, invites and revocation

- **Evidence:** Read, on 2026-11-09 at `master` `bd27111`. The cluster secret is the only credential
  (`crates/core/src/cluster_auth.rs`, OXS-AUTH v2). Every machine holds it, a join token carries it
  (design 0048), removing one machine means a new secret everywhere (`docs/SECURITY.md` section 5),
  and the v2 handshake has no forward secrecy.
- **Problem:** A machine of a friend or a teammate, or a machine on another network, can only join
  if the owner sends it the secret. That channel must not be chat or e-mail, and the secret cannot
  be taken back from one machine.
- **Do:** [Design 0051](design/0051-machine-identity-and-invites.md): a key per machine, OXS-AUTH v3
  (Noise `IK`) with the master's key pinned, invites that work once, expire and need approval,
  `oxideswarm invite`, `oxideswarm members`, `join` with invites and a pairing check that both sides
  confirm, roles per message, worker ids owned by their member, revocation across every connection
  lane, and `--auth secret|members|both`. Work items ENROLL-1a to 1d, in that order.
- **Done when:** `tests/cli_enroll_e2e.rs` and `tests/cli_revoke_e2e.rs` pass on Linux, macOS and
  Windows. A revoked member loses every connection, its submitted tasks are cancelled, and a task on
  its worker is requeued at once. A v2 secret worker keeps working next to a member worker. A
  machine on another network joins through `--p2p` with an invite sent by chat, and the run is
  recorded with its date.
- **Status (2026-11-21, Partial):** ENROLL-1c, first part (pull request 328): a revoked member
  loses every connection at once (worker sessions over TCP and QUIC, client connections over TCP
  and P2P, blob and MCP connections), and the tasks on its workers are requeued at once without the
  resume hold and without their checkpoints; `tests/cli_revoke_e2e.rs` passes on macOS. ENROLL-1d
  dashboard API/UI, tests, and docs are built (`crates/master/src/web_ui/members.rs`,
  `crates/master/src/dashboard.html`, `tests/dashboard_api_test.rs`, `tests/dashboard_security_test.rs`,
  `docs/reference.md`, `docs/guides/add-a-machine.md`, `docs/SECURITY.md`, `README.md`). Open:
  cancelling the member's submitted tasks (design 0051 D5 step 2, a store format change for the
  owner) and the recorded live `--p2p` invite run from another network.

### USB-1: Set up machines from a USB stick

- **Evidence:** Read. A new machine needs a binary (`README.md` Install), a credential and a
  background start (`scripts/`, `windows/`, `packaging/`), from three places.
- **Problem:** Setting up a friend's laptop or a lab PC takes several commands and some terminal
  knowledge.
- **Do:** [Design 0053](design/0053-usb-setup-kit.md): `usb create <DIR> --machines N` writes the
  release archives with `SHA256SUMS`, an invite for N machines encrypted with a passphrase, and
  installers for macOS, Windows and Linux that install for the current user, join, start at
  login and run the doctor checks. Work items USB-1a, 1b and 1c. USB-1b can start before
  ENROLL-1b against a fake `join`.
- **Done when:** `tests/cli_usb_kit_e2e.rs` and the installer launcher tests pass, and a stick made
  on one system sets up a machine of another system, recorded with its date.

## Phase 7: scale out

Draft, added on 2026-11-09 with Phase F of [ROADMAP.md](ROADMAP.md). The design is
[design 0050](design/0050-scale-out-architecture.md). Every item starts after the Phase D exit check
of the roadmap, and SCALE-1 comes first because it measures the limits the other items remove. The
owner's decisions Q8, Q9 and Q12 are open.

### SCALE-3: Worker-to-worker blob distribution

- **Evidence:** Read. A worker fetches input blobs from the master's `BlobStore` and uploads
  outputs to it (`crates/worker/src/blob_client.rs`, `crates/master/src/blob_store.rs`, design 0006).
  A worker's cache (`crates/worker/src/blob_cache.rs`) already verifies BLAKE3 ids.
- **Problem:** N workers that need one tree make the master send it N times, so the master's disk and
  uplink bound the grid.
- **Do:**
  1. Add a `ProtocolCapabilities` bit and a message pair so a worker advertises the blob ids it
     holds as deltas, and the master answers a missing-blob request with a short list of peers
     (design 0050 D3). Follow the wire rule and bump the protocol version.
  2. Peer fetch uses the cluster handshake and encrypted records and 4 MiB chunks, verifies the id,
     and falls back to the master when no peer answers. A worker that cannot listen (mobile,
     Windows service) stays a client only.
  3. Record the master's blob bytes with SCALE-1 before and after.
- **Done when:** A test starts N workers that need one blob: the master sends it once and the others
  fetch it from peers, and the result bytes match. A dropped peer falls back to the master.

### SCALE-4: Master high availability

- **Evidence:** Read. One `MasterServer` holds scheduling, sessions and the blob store
  (`crates/master/src/server.rs`). Design 0046 gives a journal and replay
  (`crates/master/src/store.rs`) and workers resume (`ResumeSession`), so a restart loses nothing,
  but there is no standby. `FEAT-6` refuses `ClaimMasterRole` (design 0026) and says real failover
  needs replicated state, renewed leases and a fencing epoch.
- **Problem:** The master host is a single point of failure for submission and placement.
- **Do:**
  1. Write the HA choice into design 0050 D4 after the owner answers Q9 (active-passive with a
     fencing epoch, or a Raft-replicated journal). The journal format and the epoch are fixed first.
  2. Ship the journal to a standby, add the lease and the epoch, and make every assignment carry the
     epoch so a worker refuses a lower one.
  3. Teach workers and clients a list of master addresses and dial the next on failure.
  4. Add a kill-the-leader scenario to the SCALE-1 harness.
- **Done when:** A test kills the leader with tasks queued and running: the standby takes over, no
  accepted task is lost, and no task runs more often than the at-least-once bound. A deposed leader
  places nothing after it sees a higher epoch. This item supersedes the "Design only" note of
  `FEAT-6`, which stays Implemented as the refusal.

### SCALE-5: Autoscaling providers

- **Evidence:** Read. Nothing starts or stops a worker on queue depth. `oxideswarm join` and join
  tokens exist (design 0048). Workers drain without a retry charge (`WorkerMessage::Disconnecting`,
  design 0036).
- **Problem:** The grid has only the workers someone started, so a burst waits and idle machines
  cost money.
- **Do:**
  1. Define a provider interface (`start`, `stop`, `list`) and a scaler that reads queue depth and
     free capacity per tag and respects `min`, `max` and a cooldown.
  2. Build providers in the order the owner picks (Q12). Recommended: an SSH pool, then Kubernetes,
     then one cloud. A cloud provider needs a hard `max` and a budget line.
  3. A started worker gets a scoped, short-lived join token. Scale-down drains idle workers only.
  4. Keep provider credentials in the scaler's own files at mode 600, never in `cluster.secret`.
- **Done when:** A test with a fake provider shows a backlog adding workers and idle time removing
  them within the cooldown. A live run through the first real provider on two machines is recorded
  with its date.

### SCALE-6: Multi-site federation

- **Evidence:** Read. A master knows only its own workers. Blob ids are global, so a blob moves by
  id between any two stores.
- **Problem:** A grid in one site cannot use capacity in another.
- **Do:** Only if the owner's target (Q8) needs more than one site. Design the layer in design 0050
  D8 (a metadata coordinator or peering masters), give each site its own secret and a narrow
  forward permission, and forward a task when the local site lacks a capability or capacity.
- **Done when:** A task submitted at one site runs at a worker of another, and its input blob is not
  sent twice. A site without the forward permission is refused.

### SCHED-4: Place tasks by measured worker speed and re-run stragglers

- **Evidence:** Reproduced on 2026-11-10 on a two-worker cluster over direct P2P links: a desktop
  (Intel Core i5-14600KF, 20 threads, 10 slots) and a laptop (Intel Core i7-1355U, 2 performance
  and 8 efficient cores, 12 threads, 6 slots). One PowerShell task that sums 1 to 2,000,000 takes
  about 2.3 s on the desktop and 3.5 s on the laptop when run alone. A batch of 32 such tasks with no
  worker pinned put 26 on the desktop (2.4 to 4.6 s each) and 6 on the laptop (about 51 s each), so
  the batch took 52 s; the desktop alone would have finished in about 12 s. Pinned to the laptop,
  2, 4 and 6 concurrent tasks took 2.9 s, 12.5 s and 39.6 s each (about 112 MB per task, so not
  memory). GPU work shows the same gap: a verified 8192 GEMM took 855 ms on an RTX 5060 Ti and
  11.9 s on an MX550, and 12 unpinned 4096 GEMMs went 9 and 3.
- **Problem:** `Engine::rank` (`crates/master/src/scheduler.rs`) orders candidates by load per core
  and reported CPU use; it knows nothing about how fast a worker actually runs a task. Every free
  slot is filled, so one slow or overloaded worker sets the finish time of a whole batch, and a task
  stuck on it is never moved while faster workers sit idle.
- **Do:** follow [design 0060](design/0060-speed-aware-placement.md):
  1. Record per worker an exponential moving average of the run time of each task key (task type,
     program and arguments), bounded in size, from the results the master already receives.
  2. In placement, when a worker's expected run time for the key is at least 1.5 times what a
     faster matching worker would take, counting the time until that worker frees a slot, leave the
     task queued for the faster worker. Keys with no history keep today's placement.
  3. Opt-in straggler re-run: a task the submitter marks as safe to repeat runs a second copy on an
     idle faster worker once it runs past 3 times the expected time; the first result wins and the
     other copy is cancelled.
  4. Show each worker's measured speed in `workers --json` and the dashboard.
- **Done when:** a scheduler test with one fast and one slow simulated worker shows a batch of
  identical tasks finishing at least 30 % sooner than with today's placement, unknown keys keep
  today's placement exactly (the existing scheduler tests pass unchanged), and a live two-machine
  batch like the one above finishes within 1.5 times the fast machine's own time.

## Phase 8: product and ecosystem

Draft, added on 2026-11-09 with Phase G of [ROADMAP.md](ROADMAP.md). The items start after the
Phase D exit check and run in parallel with Phase 7 where they do not depend on it. The owner's
decisions Q10 and Q11 are open. Items that overlap an existing one say which they extend.

### ECO-1: Remote Execution API compatibility

- **Evidence:** Read. REAPI-1 plans a cache-only profile: `Capabilities`, CAS, `ByteStream` and
  `ActionCache`, with the `Execution` service left out. A `BlobId` equals a REAPI BLAKE3
  `Digest.hash` (REAPI-1 evidence).
- **Problem:** Bazel, Buck2 and Pants can use the grid as a cache but cannot run actions on it.
- **Do:** After REAPI-1 and ECO-2, implement the `Execution` service: turn an `Action` into a task
  with the input root as a blob tree and the output paths as output patterns, run it under
  `--isolation`, and return an `ActionResult`. This item extends REAPI-1; it does not replace it,
  and the cache part stays REAPI-1. Order against ECO-6 follows decision Q10.
- **Done when:** A sample Bazel project with remote execution pointed at the master builds, and a
  second build hits the action cache. A client without the cluster credential is refused.

### ECO-2: Action cache

- **Evidence:** See CACHE-1. It is the same work and keeps its own section.
- **Problem:** Identical tasks run again.
- **Do:** Carry out CACHE-1 as written. ECO-2 is its name in Phase G so the exit check can reference
  it. Do not write a second design.
- **Done when:** The "Done when" of CACHE-1 passes.

### ECO-3: Multi-tenancy and quotas

- **Evidence:** Read. `AuthScope` (`crates/master/src/auth.rs`) covers the dashboard only. The
  cluster secret is one trust domain, and `Task` has no tenant or project field
  (`crates/core/src/task.rs`). `retry_history` is the only audit-like record.
- **Problem:** A shared grid cannot tell submitters apart, limit them or show who did what.
- **Do:**
  1. Add tenant and project to `Task` and a protocol feature bit (wire rule, version bump).
  2. Issue scoped tokens (a new HMAC label per purpose) so a client does not hold the secret.
  3. Add quotas per tenant: concurrent tasks, queued bytes, CPU-seconds per day, blob share.
     Admission refuses with a typed error.
  4. Add an append-only audit log of submit, cancel and read, in the journal store.
  5. Document that tenants are not mutually hostile (design 0050 D6).
- **Done when:** A second tenant over its quota is refused with the typed error, a scoped token
  cannot do what another purpose's token does, and the audit log shows the refusal.

### ECO-5: Packaging for package managers and containers

- **Evidence:** Read. REL-4 (`.github/workflows/release.yml`) builds archives and `SHA256SUMS` on
  `v*` tags, and no tag exists. `ls` of the repository root shows no container file. `packaging/`
  holds launchd, Windows service and Termux installers.
- **Problem:** Installing means downloading an archive and unpacking it by hand.
- **Do:**
  1. After the first tag, add a Homebrew formula, a winget manifest, `.deb` and `.rpm` packages and
     a container image of the master and worker. Each is built in CI from the release archives.
  2. Sign the Windows binary, and sign and notarize the macOS binary. This needs the owner's
     certificates and accounts (owner item).
  3. Never put the secret in an image or a package; the README shows how to mount it.
- **Done when:** A fresh macOS, Windows and Linux machine installs from its package manager and runs
  `oxideswarm doctor`. The macOS binary passes Gatekeeper and the Windows binary shows a publisher.

### ECO-6: Client SDKs and a versioned public API

- **Evidence:** Read. The client protocol is `ClientMessage` / `ClientResponse` with
  `protocol_version` (design 0040), and MCP exposes six tools (`crates/cli/src/mcp.rs`). Both
  change with the wire protocol, and `PROTOCOL_VERSION_MIN_COMPATIBLE` equals the current version.
- **Problem:** A program outside this repository must track the wire format to submit work.
- **Do:**
  1. Choose a public surface (an HTTP/JSON API on the dashboard listener, or a stable subset of the
     client protocol) and version it separately from the internal protocol, with a stated
     deprecation rule.
  2. Publish typed SDKs for Python and one more language (decision Q10), generated from one
     description of the API.
  3. Add a conformance test that runs each SDK against a master.
- **Done when:** The API description and the SDKs are in the repository, and each SDK passes the
  conformance test in CI against the built master. A change to the internal protocol does not fail
  it.

### ECO-7: Integrations for coding agents and CI

- **Evidence:** Read. `docs/guides/mcp-client-setup.md` covers Claude Code, Codex and Cursor over
  stdio (DOC-7). Nothing runs CI jobs on the grid.
- **Problem:** Other agents need their own setup, and a team's CI cannot use idle machines.
- **Do:**
  1. Add tested MCP setups for more common agents and a one-line check that each lists the tools.
  2. Add a GitHub Actions runner pool: a worker mode or a small adapter that registers as a
     self-hosted runner, picks jobs by label, and runs each in a fresh sandbox under `--isolation`.
     State that a workflow from a fork must not run on it without approval.
- **Done when:** A guide per agent has a recorded run. A workflow in a test repository runs on a
  grid worker and reports its result to GitHub.

### ECO-8: Documentation site and community

- **Evidence:** Read. `CONTRIBUTING.md` and the REL-5 files exist. Documents are Markdown under
  `docs/` and no site renders them.
- **Problem:** A newcomer has no single place to read, ask or report a vulnerability.
- **Do:**
  1. Build a documentation site from `docs/` in CI, with the same links check.
  2. Add a governance file (who decides, how a change is accepted), a code of conduct, a security
     reporting policy with a contact, and issue and discussion templates. The owner decides each
     (owner item, and Q11 for licence).
  3. Link all of it from `README.md`.
- **Note (2026-11-10):** LV-14 carries out step 2, except the security reporting policy, which
  REL-5 delivered.
- **Done when:** The site builds in CI and is reachable, `README.md` links the governance and the
  security policy, and a test report of a vulnerability reaches the named contact.

## Phase 9: an open compute network

Draft, added on 2026-11-10 from Phase H of [ROADMAP.md](ROADMAP.md). The architecture and the stage
exit checks are in [design 0054](design/0054-open-compute-network.md). Decisions Q15 to Q18 of the
roadmap are open; Q96 closed Q14 on 2026-11-19. Each item starts with a threat model of its own and an independent review.
[Design 0094](design/0094-road-to-a-shared-research-grid.md) (Accepted, 2026-11-19) orders these items
as stages of a shared research grid: NET-1, NET-3 and NET-4 in its Stage 2, and NET-2 and NET-5 to
NET-8 in Stage 3. The first part of NET-4 is NET-9, in Phase 19.

| ID | Do | Done when |
|---|---|---|
| NET-1 | `TaskSpec::Wasm` run by a WebAssembly runtime with memory, CPU fuel, file and network grants, and a deterministic mode | The same `.wasm` task gives byte-identical output on macOS, Windows and Linux workers |
| NET-2 | A versioned protocol between clusters to lend workers, with credit records that both sides sign | Cluster A runs a task on a worker that cluster B lends it, and both credit logs agree |
| NET-3 | k-of-n redundancy for deterministic tasks, unannounced spot checks, reputation per key | A planted cheating contributor is caught, and its results are never accepted |
| NET-4 | Opt-in per machine, CPU, hours and battery caps, metered networks off, a local stop switch, a task-class policy | A contributor's stop switch ends every task within 5 s, and caps hold under load |
| NET-5 | Throw-away VMs per task: Firecracker or Cloud Hypervisor, Virtualization.framework, Hyper-V | Attack tests cannot read host files or reach the network beyond the grant |
| NET-6 | Clusters publish capacity and task classes through gossip or a DHT over iroh | Clusters find each other with no central server |
| NET-7 | A written protocol specification, a conformance test suite, a security process and a neutral steward | Two independent implementations pass the suite together |
| NET-8 | Optional trusted execution (AMD SEV-SNP, Intel TDX, GPU confidential computing) with attestation | A task's attestation is checked before its inputs are released |

## Phase 10: lasting value

Added on 2026-11-10 from [design 0058](design/0058-lasting-value.md), at the owner's word, for its
first items. The record plans assurance, a written specification, citable evidence and stewardship,
and adds no feature. Its other items (LV-2, LV-3, LV-6 to LV-9, LV-11 to LV-13, LV-16 and
LV-17) stay in the design record until the owner adds them here. LV-4 and LV-15 are in Phase 13
since 2026-11-17. The four items below do not depend
on each other and can run in parallel. None changes the wire format, a CLI flag or a default, so
none needs a design record of its own.

### LV-5: Supply chain: dependency policy, release provenance, outside checklist

- **Evidence:** Read on 2026-11-10 at `master` `2f22c48`. CI runs `cargo audit`
  (`.github/workflows/ci.yml`, job `audit`), and Dependabot is configured. No `deny.toml` exists.
  `.github/workflows/release.yml` writes `SHA256SUMS` and attaches no signature and no provenance.
  The workflows name actions by tag (`actions/checkout@v7`, `Swatinem/rust-cache@v2`,
  `taiki-e/install-action@v2`), not by commit.
- **Problem:** A checksum proves that a download is intact. It does not prove where and from which
  commit an archive was built. A dependency with an unwanted licence or from an unexpected source
  would pass CI. A moved action tag would change what CI runs.
- **Do:**
  1. Add `deny.toml` and a CI job on Linux that runs
     `cargo deny --locked check licenses bans sources`. `cargo audit` keeps the advisories. Allow
     the licences that the current dependency tree uses, and list them for the owner in the pull
     request. Allow crates.io as the only source, plus any git source that `Cargo.lock` already
     has.
  2. Build provenance. In `release.yml`, attest every archive with `actions/attest`, in the job
     that holds the archives, with the permissions `id-token: write` and `attestations: write` on
     that job only. Add a manual `workflow_dispatch` run that builds and attests and creates no
     release, so that the step can be checked without a tag. Skip the step on pull requests from
     forks.
  3. Add the verify command to the install section of `README.md`:
     `gh attestation verify <ARCHIVE> --repo <OWNER>/<REPO>`. Say what it proves (where and from
     which commit the archive was built) and what it does not prove (that the source is safe).
  4. Pin every `uses:` in `.github/workflows/` to a full commit hash, with the tag in a comment.
     Dependabot keeps them current. Keep `permissions: contents: read` as the default of every
     workflow.
  5. Add the OpenSSF Scorecard workflow, on a schedule and on pushes to `master`, and its badge to
     `README.md`.
  6. Owner: make the `cargo deny` job a required check once it is green on `master`, and fill in
     the OpenSSF Best Practices questionnaire.
- **Done when:** The `cargo deny` job passes on `master`. A manual run of `release.yml` produces an
  attestation per archive, `gh attestation verify` accepts an archive of that run and refuses a
  copy with one changed byte, and both outputs are in the pull request. No `uses:` line in
  `.github/workflows/` names a tag or a branch. The Scorecard workflow has run once.
- **Status (2026-11-18, Partial):** Steps 1 to 5 are in pull request 342; step 6 stays with the owner.
  - Step 1: `deny.toml` allows ten licences (`Apache-2.0`, `BSD-2-Clause`, `BSD-3-Clause`,
    `CDLA-Permissive-2.0`, `ISC`, `MIT`, `MPL-2.0`, `Unicode-3.0`, `Unlicense`, `Zlib`, each the only
    choice of at least one crate) plus `NCSA` for `libfuzzer-sys` only, crates.io as the only source
    (neither lock file has a git source), bans nothing and warns on duplicates. The CI job
    `cargo deny` (cargo-deny 0.20.2, not a required check) checks the workspace and `fuzz/`; both
    pass on macOS.
  - Step 2: the `checksums` job of `release.yml` attests every archive with `actions/attest`;
    `workflow_dispatch` takes `mode=attest-only` and never creates a release. Manual run
    44284015567 on `597fa14`: one attestation for 8 archives, `gh attestation verify` accepts an
    archive of that run and refuses a copy with one changed byte (HTTP 404, no attestation for the
    new digest).
  - Step 3: `README.md` (Install) has the verify command and what it proves; `docs/SECURITY.md`
    points to it.
  - Step 4: every `uses:` names a full commit with its tag in a comment; Dependabot already had the
    `github-actions` ecosystem.
  - Step 5: `.github/workflows/scorecard.yml` (weekly, pushes to `master`, SARIF to code scanning,
    results published) and its badge. It runs on the default branch only, so it has not run yet:
    its first run is the push of the merge.
  - Owner (step 6): make `cargo deny` a required check once it is green on `master`, and fill in
    the OpenSSF Best Practices questionnaire.

### LV-10: Citation file and a DOI per release

- **Evidence:** Reproduced on 2026-11-10 at `master` `2f22c48`. The repository root has no
  `CITATION.cff`, and `README.md` has no section on how to cite the project.
- **Problem:** A paper, a course or another project that uses OxideSwarm has nothing stable to
  cite, and a release has no identifier that outlives the repository.
- **Do:**
  1. Add `CITATION.cff` in the repository root, in format version 1.2.0, with the keys
     `cff-version`, `message`, `title`, `abstract`, `authors`, `version`, `date-released`,
     `license`, `repository-code` and `keywords`. Take `version` and the licences (`MIT`,
     `Apache-2.0`) from `[workspace.package]` in `Cargo.toml`. Use the entity "The OxideSwarm
     Authors" as the author, as `Cargo.toml` does, unless the owner gives a personal name and an
     ORCID.
  2. Add a test to `tests/test_m1_challenger_hygiene.py`, with the standard library only: the file
     has every key listed above, and its `version` equals the workspace version in `Cargo.toml`. A
     version bump without the citation file then fails CI.
  3. Add a "How to cite" section to `README.md`, above "License".
  4. Owner: switch the repository on in Zenodo, so that Zenodo archives each GitHub release and
     gives it a DOI. After the next release, add the DOI to `CITATION.cff` (`identifiers`) and to
     the "How to cite" section.
- **Done when:** The new test passes on Linux, macOS and Windows, and the repository page shows
  "Cite this repository". Step 4 is finished when the DOI of a release is in `README.md`. Until
  then the item is Partial.
- **Status (2026-11-21, Partial):** Steps 1 to 3 are complete (`CITATION.cff` at format version `1.2.0`
  matching `Cargo.toml` `0.4.4` and `MIT` / `Apache-2.0`, stdlib check in
  `tests/test_m1_challenger_hygiene.py`, and `"How to cite"` in `README.md`). Step 4 (enabling
  Zenodo archiving and adding the release DOI) stays with the owner.

### LV-14: Governance files and a licence that GitHub detects

- **Evidence:** Reproduced on 2026-11-10 at `master` `2f22c48`. The repository has no
  `CODE_OF_CONDUCT.md`, no `GOVERNANCE.md` and no `.github/FUNDING.yml`.
  `gh api repos/<OWNER>/<REPO> --jq .license.spdx_id` prints `NOASSERTION`: the root holds
  `LICENSE-MIT`, `LICENSE-APACHE` and a four-line `LICENSE` that points to both. `git shortlog -sn`
  lists one human author.
- **Problem:** A newcomer cannot tell who decides, how a change is accepted or how to become a
  maintainer. Funds ask for a governance file. Tools that read the licence from GitHub see none.
- **Do:** This item carries out step 2 of ECO-8, except the security reporting policy, which REL-5
  delivered. ECO-8 keeps the documentation site.
  1. Add `GOVERNANCE.md`. State the facts as they are: one owner decides, through design records
     and the decisions table of `docs/ROADMAP.md`; coding agents implement and review; a pull
     request merges when its required checks are green and it has a review. Say how a person
     becomes a maintainer and which rights that gives, and that a neutral steward is planned for
     the public network (decision Q17). Write nothing that is not true today.
  2. Add `CODE_OF_CONDUCT.md`. Propose the Contributor Covenant 2.1 to the owner, with the private
     reporting route of `docs/SECURITY.md` as the contact. The owner decides the text.
  3. Licence detection. Ask the owner before changing any licence file. The proposal: remove the
     pointer file `LICENSE`, keep `LICENSE-MIT` and `LICENSE-APACHE`, and update what names the
     pointer file (`.github/workflows/release.yml` copies it into the archives). Check the
     repository page after the change. If GitHub still shows no licence, restore the file and
     record the result in the status log.
  4. Add `.github/FUNDING.yml` only when the owner has a sponsorship account and names it.
  5. Link `GOVERNANCE.md` and `CODE_OF_CONDUCT.md` from `README.md` and `CONTRIBUTING.md`.
  6. Add a short section to `README.md` that says how the project is built: coding agents write
     and review the code, and the owner decides and approves. The owner approves the wording.
     Funds and journals ask for this statement.
- **Done when:** The two files and the `README.md` section exist and are linked, the links test of
  `tests/test_m1_challenger_hygiene.py` passes, and the status log records what the repository
  page shows as the licence after step 3.

## Phase 11: findings of the audit of 2026-11-15

These items come from a read-only audit of `master` at `c2462e3` (after 0.2.0 and SCALE-2). Line
numbers refer to that commit. Evidence "Read" means the code was read and nothing was run, so
confirm each item before you fix it (rule 5). On that commit `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo fmt --all -- --check` and `cargo test --workspace --lib`
(1139 passed, 0 failed) passed on an Apple Silicon Mac.

## Phase 12: 0.4.0, upgrade without stopping the cluster, and stability

The goal of 0.4.0 is that a cluster can be upgraded without stopping all of it, and that it runs
stably. The first half is [design 0076](design/0076-protocol-compatibility-and-release-train.md);
the second half is the test and CI fixes below and the endurance and fuzz items that already
exist. LLM serving across machines ([design 0067](design/0067-multi-node-llm-serving-and-admission.md)) is 0.5.0.

Order:

1. PROTO-1 and the test fixes TEST-28 to TEST-32, first.
2. PROTO-2.
3. PROTO-3: a design record first, then code.
4. PROTO-4, after PROTO-3 and an update of [design 0040](design/0040-protocol-versioning.md).
5. TEST-24, TEST-25, TEST-26 and LV-1 alongside. They are already in the Index (TEST-23 and
   TEST-27 too) and are not repeated here.

REL-6 (release `v0.3.0` at protocol 9, pull request 260) is the step before this phase.

The Windows smoke test of 0.3.0 passed 9 of 9 steps at `3ecc926`, and the macOS one passed 10 of
10 at `9acc06e`. SEC-16, TEST-32, BUG-22 and CLI-2 come from the Windows report.

### PROTO-5: Keep a skew job against the release one protocol back

- **Evidence:** Read. Review of pull request 274.
- **Problem:** The skew job compares the pull request with the newest release only. After 0.4.0 that is
  protocol 10, so the one-version window (protocol 10 accepts 9) is no longer exercised against a real
  protocol 9 binary.
- **Do:** Pin a second release (v0.3.0 while the window reaches back to 9) in the skew job, with its
  `protocol.rs` for the expected verdicts, and move the pin at each wire change. Do not build it now.
- **Done when:** Each pull request shows a table row of a real release one protocol back.
- **Status (2026-11-21, Partial):** Local implementation and tests pass (`.github/workflows/ci.yml`
  `release-skew` pins `PREVIOUS_PROTOCOL_RELEASE: v0.3.0` alongside the newest release with
  per-tag directory isolation, `scripts/release_skew.py` adds `--release-tag` for distinct summary
  tables, `tests/test_release_skew.py` tests both CLI behavior and static CI workflow invariants,
  and `CLAUDE.md` / `docs/reference.md` are updated); awaiting a live GitHub pull request CI run of
  `release skew (ubuntu-latest)` showing the `v0.3.0` table row per "Done when".

### SEC-16: Find what put a full-control entry for an unknown capability on `AppData`

- **Evidence:** Reproduced on the owner's Windows machine, 2026-11-16, `master` `3ecc926` (Windows
  smoke test, finding F1). The worker exits with: "another account
  (S-1-15-3-3557520199-3666692283-...) can rename, delete or re-permission ancestor directory
  `AppData`". A read-only `icacls` shows two explicit entries for that SID, `(F)` and
  `(OI)(CI)(IO)(F)`. It is not the `oxideswarm.toolchain` capability, which derives to
  `S-1-15-3-1024-...`.
- **Problem:** The worker's check is correct, but no `fs` worker can use any sandbox base below
  `AppData`, the default `%TEMP%` included, and the worker unit tests fail under the default
  `%TEMP%` there. Nobody knows what created the entry. A confined process that holds that
  capability would have full control of everything that inherits from `AppData`.
- **Do:**
  1. Find what created the entry and whether any confined process holds that capability. Do not
     change the ACL without the owner.
  2. Decide whether the worker's error message should suggest a fix.
- **Done when:** The cause is written here, and the owner has decided what to do with the entry.
  This needs the owner's machine.
- **Note (2026-11-18):** On the second Windows machine (a laptop), the foreign write grants on `%TEMP%`
  come from the Codex CLI sandbox (its capability SIDs, recorded in `%USERPROFILE%\.codex\cap_sid`) and
  from one deleted local account. No OxideSwarm SID appears on any inspected folder. OxideSwarm's only
  capability SID (`DeriveCapabilitySidsFromName("oxideswarm.toolchain")`) has the sub-authority 1024,
  which the SID of this item lacks, so that SID is not OxideSwarm's either.
- **Partial (2026-11-19, pull request 358):** The cause is on record on both Windows machines. A read-only check on the
  case machine matched the SID of this item to the AppContainer of the Claude Desktop app's MSIX package
  (through the AppContainer mappings and `DeriveAppContainerSidFromAppContainerName`): it belongs to that
  package, not to OxideSwarm and not to the Codex CLI. On 2026-11-19 the entry is no longer on
  `%USERPROFILE%\AppData`. None of the foreign grants found on either machine is OxideSwarm's. Open: the
  owner's decision of "Done when" (the entry itself is gone) and step 2.

### TEST-41: Find why a task finished during master shutdown can run twice

- **Evidence:** `rv158_probe_result_during_master_shutdown_is_lost_and_task_reruns`
  (`tests/master_restart_recovery.rs:1001`) failed on `ubuntu-latest` on 2026-11-17, in pull request 282 at
  `7a17104` (run `44018512676`, job `133822123752`): `RV158 P3: rounds=12 ran_twice=1`, then
  `task ran twice in 1/12 rounds`. Round 0 (`delay_ms=0`) ran the task twice; rounds 1 to 11 ran it once.
  Pull request 282 changes only `windows/oxctl.ps1`, a Python launcher test and docs.
- **Problem:** The probe (review item H2 of pull request 158) states that a task which finishes as the master
  shuts down keeps its result in the worker's `pending_resume_outbox` until the restarted master acknowledges
  it, so the task runs exactly once. One round in twelve ran it twice. Either the hand-over has a window at
  delay 0, or the probe counts a rerun the design allows. A command task that runs twice is visible to users.
- **Do:** Run the probe in a loop, also under CPU starvation as for TEST-36 to TEST-38, and find which side
  lost the result or dispatched the task again. Fix that cause. If the design allows a rerun in that window,
  say so beside the probe and in the guarantee it tests, and change the assertion with that reason.
- **Done when:** The cause is named here, and the probe passes 200 runs in a row on Linux and on Windows.
- **Partial (2026-11-17, pull request 288):** not reproduced on macOS in 6,884 rounds of the probe (about 570 runs of
  12 rounds: single, 3 to 8 parallel copies, the whole test binary next to three copies of itself, 6 `yes` CPU spinners
  on 10 cores, `RV158_ZERO` with no delay after the task finished, and a variant that kept the task alive for up to
  450 ms after its done marker). No round ran the task twice. What reading the code ruled out: the journal cannot lose an
  acknowledged result (`store.rs`: the writer drains and syncs on drop, a snapshot is taken under the queue lock after
  every enqueued append, and a result that arrives after `close_and_compact_store_sync` is refused with no
  acknowledgment); the worker keeps a result in `pending_resume_outbox` and lists it in `ResumeSession` whether the task
  is still in the active table or already in the outbox; a master-side rerun without a `ResumeSession` that names the task
  needs `adopt_session_tasks` to release the hold (`recovery.rs`), which charges nothing, matching the `retry=0
  failover=0` of the CI round. That is the one path that fits the evidence, and no test has shown the worker omitting
  the id. The probe now keeps the debug log of the master and worker crates in memory and, when a round ran the task
  twice, prints the marker file, the task and worker id, every line that names either, and every warning and error since
  the round began, and the round line shows `evacuation=` next to `retry=` and `failover=`. `RV158_ROUNDS` and
  `RV158_ZERO` let a local loop run more rounds and shut the master down with no delay. The assertion is unchanged.
  Next step: the next CI failure of this probe now carries the evidence; read which side sent `ResumeSession` without the
  task (worker) or released the hold (master). Two other tests of the same binary failed under load: see the lead
  under TEST-23.
- **Partial (2026-11-19, pull request 359):** Not reproduced on Windows (`x86_64`, 20 logical cores, `rustc 1.98.1 (48a229cea 2026-09-01)`, `cargo 1.98.1 (797e8a9bc 2026-08-05)`) at commit `9b918d3` across 232 runs (2,784 rounds; 0 double runs and 0 failures; 4,718.3 s elapsed across the 5 batches) of `rv158_probe_result_during_master_shutdown_is_lost_and_task_reruns` (`--exact --nocapture`, `ran_twice=0` on every run): 40 alone default-delay runs (480 rounds, 852.4 s), 40 alone `RV158_ZERO=1` runs (480 rounds, 863.0 s), 60 default-delay runs under 12 busy loops on 20 logical cores (about 60 %, 720 rounds, 1,429.9 s), 60 `RV158_ZERO=1` runs under 12 busy loops on 20 logical cores (about 60 %, 720 rounds, 1,426.0 s; batches 1–4 = 200 sequential `cargo test` runs / 2,400 rounds, with a clean docs-only PR 357 pause between batches 3 and 4), and 32 runs across 8 waves of 4 parallel test-binary copies with 8 busy loops and `RV158_ZERO=1` (384 rounds, 147.0 s). Stays Partial until Linux also reaches 200 runs without a double run.
- **Partial (2026-11-19, pull request 363, open):** Not reproduced on Linux (`aarch64`, 10 logical cores, `Linux 7.0.14-linuxkit`, `Debian GNU/Linux 12 (bookworm)` in the Docker Desktop container `rust:1.96-bookworm`, `rustc 1.96.1 (31fca3adb 2026-06-26)`, `cargo 1.96.1 (356927216 2026-06-26)`, `--locked`, fresh temporary `HOME` per run) at commit `ccafa90` (`ccafa9023`) across 200 sequential `cargo test --locked` runs (`2,400` rounds; 0 double runs and 0 failures; `1,697.06 s` elapsed across the 4 batches; plus a 1/1 `RV158_ZERO=1` smoke run beforehand in 7.94 s, `12` rounds, `ran_twice=0`) of `rv158_probe_result_during_master_shutdown_is_lost_and_task_reruns` (`--exact --nocapture`, `ran_twice=0` on every run): 40 alone default-delay runs (`480` rounds, `327.44 s`), 40 alone `RV158_ZERO=1` runs (`480` rounds, `340.93 s`), 60 default-delay runs under 6 Python busy loops on 10 logical cores (`60 %`, `720` rounds, `506.52 s`), and 60 `RV158_ZERO=1` runs under 6 Python busy loops on 10 logical cores (`60 %`, `720` rounds, `522.17 s`). The 200-run part of "Done when" is now met on both Windows (pull request 359) and Linux (this pull request), alongside macOS (pull request 288), while no cause is named because the single double-run of pull request 282 never reproduced; closing TEST-41 as "not reproduced in 200 runs on Linux, Windows and macOS" (keeping the pull request 288 debug-log capture) is the owner's decision.

### BUG-22: Count cancelled tasks apart from failed tasks in `status --json`

- **Evidence:** Reproduced once (smoke test, finding F6). After 9 failed and 2 cancelled tasks,
  `status --json` read `failed_tasks: 11`.
- **Problem:** A cancel is not a failure, and a reader of the counts cannot tell them apart.
- **Do:** Report cancelled tasks in their own field, and keep `failed_tasks` for failed tasks.
  Check the dashboard and the MCP `status` tool for the same sum.
- **Done when:** A test with one failed and one cancelled task shows 1 and 1.
- **After 0.4.0 (2026-11-17):** a separate count needs a new field in `ClientResponse::ClusterStatus`,
  which is a wire change. Under the one-version window (design 0083) a protocol 9 client would also need
  its own encoding of that response, and design 0076 allows one wire change per release, so BUG-22 waits
  for the next wire change after 0.4.0.
- **Owner decision (2026-11-19):** no protocol bump for BUG-22 alone; it goes into the next planned wire
  change.

### BUG-34: Clean up leftover blob-cache `.part` files of a killed worker without breaking shared sandbox bases

- **Evidence:** Reproduced during the `win/test-25-remote-workers` real-binary two-worker soak E2E test on Windows (`oxideswarm 0.4.4`) when a `worker_kill` fault terminated `win-a` mid-transfer (`tmp_leftovers: {"win-a_cache_tmp": ["51645078-14b0-4d61-a207-01d634f772cf.part"]}`), failing D3 check `3_disk_and_journal` (`sandbox_ok: false`).
- **Root cause:** `BlobCache::open` (`crates/worker/src/blob_cache.rs`) only removed files in `<sandbox base>/cache/blobs/tmp/` older than `STALE_TMP_AGE` (24 hours) so a worker starting on a shared `--sandbox-base-dir` would not delete another live worker's in-flight write. Hard-killed workers never run `TempPartFile::drop`, so their `.part` files remained in `tmp/` for 24 hours across restarts.
- **Fix:** Each `BlobCache` instance now holds an exclusive advisory file lock (`File::try_lock`) on `<sandbox base>/cache/blobs/owners/<owner_uuid>.lock` and names its `temp_path()` files `<owner_uuid>.<part_uuid>.part`, unlinking and calling `unlock()` on clean `Drop`. On `BlobCache::open` and worker startup (`sweep_dead_owner_tmp` via `crates/worker/src/sandbox.rs`, without creating missing cache directories), a worker probes `owners/<owner_uuid>.lock`: if `try_lock()` succeeds (`lock_file_is_still_named`), the owner is gone so its `.part` files and orphan `.lock` are removed; if `WouldBlock`, the live writer's files stay untouched. Legacy unowned `.part` files retain the 24-hour `STALE_TMP_AGE` rule, and Windows/Unix security checks cover `owners/` against symlinks, junctions, and foreign grants.
- **Done when:** A killed writer's `.part` file is removed at the next worker startup / `BlobCache::open`, while a live writer holding its owner lock in another process keeps its `.part` file untouched, on both Windows and Unix.

## Phase 13: the path to finished

Added on 2026-11-17 from [design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md),
after the owner accepted Q82. That record states ten exit checks that together mean "finished" for
the purpose of Q1. This phase holds the items those checks need that had no row in the Index. Nine
of them have their full text in designs 0056, 0058 and 0059; the item here names the files as they
are at `master` `16b2a38` and the check. Three are new: CI-1, MAINT-2 and DOC-11.

A row in the Index starts no item:

- ADOPT-1 is the owner's (Q37). AX-1 starts after its first round, and so do ADOPT-3 and ADOPT-4, as
  Q38 recommends. Q36, Q38 and Q55 stay open.
- AX-1 to AX-3 may start before release `0.5.0` ships (Q83).

Order:

1. CI-1, DOC-11 and ADOPT-1, beside the test items of Phase 12.
2. ADOPT-2, LV-4 and MAINT-2. None of them waits for the study.
3. AX-1, then AX-2, AX-3, ADOPT-3 and ADOPT-4.
4. LV-15, after LV-5.

Every "Evidence" below was read on 2026-11-17 at `master` `16b2a38`, unless it says otherwise.

### CI-1: Keep runs for pushes on `master` from ending red or cancelled without a failing test

- **Evidence:** Read through `gh run list --branch master --workflow CI --event push --limit 60` and
  the job annotations (`gh api repos/<OWNER>/<REPO>/check-runs/<JOB_ID>/annotations`).
  - Twelve runs of 2026-11-16 ended as cancelled (`43800093872` to `43889536272`). The annotation of
    their jobs is "The run was canceled by" the owner's account. The workflow itself cancels only
    runs for pull requests (`.github/workflows/ci.yml`, `concurrency`), so each was cancelled by
    hand or by a session that uses the owner's `gh` login.
  - Six runs of 2026-11-16 hold one or two cancelled macOS jobs (`43889143995`, `43894129643`,
    `43914764615`, `43928207482`, `43952618376`, `43964491108`). The annotation is "The job was not
    acquired by Runner of type hosted even after multiple attempts", beside a notice about capacity
    for macOS arm64 runners. In the two runs read in detail (`43914764615`, `43964491108`) the jobs
    waited 32 to 49 minutes and never got a runner. Five of the six runs count as failed with no
    failed job.
  - Two runs of 2026-11-17 failed with no failing test. In `44036878952` the job
    `fuzz (tree_manifest)` could not download the pinned nightly toolchain. In `44034218213` the
    test step of `workspace tests (windows-latest)` ended three minutes after the job started, with
    exit code `-1073741502`.
- **Problem:** The exit check of Q80 asks for seven days of runs for pushes in which no test fails.
  A run that is red or cancelled for another reason has to be told apart by hand, and a cancelled
  run tests nothing for its commit.
- **Do:**
  1. Add a rule to `docs/guides/multi-agent-collaboration.md`: no agent or session cancels a run
     for a push on `master`. Say what to do instead when a newer push makes a run look
     superfluous: leave it.
  2. For a macOS job that was never acquired, rerun the failed jobs of that run on its current
     head (`gh run rerun <RUN_ID> --failed`), and record the run in the status log. Do not change
     the workflow for it.
  3. In the `fuzz` job, make the toolchain step retry a failed download, with the options the
     action offers or with a retry around the `rustup` call. Do not change the pinned date.
  4. For exit code `-1073741502` in run `44034218213`, read the full log of the step and the
     diagnostics step of TEST-29. Record what the first line with the code follows. If the step
     never started `cargo`, it is a runner failure: say so and rerun. If a test binary returned
     it, open an item for that test.
  5. Add a script `scripts/ci_push_runs.py` (standard library and `gh` only) that prints, for the
     last N days of runs for pushes on `master`, one line per run that is not green with its
     class: failing test, job never acquired, cancelled by an account, or other. The exit check
     of Q80 is then read from its output.
- **Done when:** The guide has the rule of step 1. The script of step 5 has a test with recorded
  JSON input (`tests/test_ci_push_runs.py`, standard library only) that CI runs on Linux, macOS
  and Windows, and its output for 2026-11-16 names
  twelve runs as cancelled by an account and six as holding a job that was never acquired. The
  two failures of 2026-11-17 each have a status log line with a cause, or a retry in the workflow.

- **Status (2026-11-17, Partial):** Steps 1, 3, 4 and 5 are done in the pull request (306); step 2 stays with the
  coordinator (no code).
  - Step 1: rule 7 of `docs/guides/multi-agent-collaboration.md`: no agent or session cancels a run for a push on `master`; leave it.
  - Step 3: the `fuzz` job installs `nightly-2026-11-10` first in a bounded loop (four tries, 15 s, 30 s, 45 s apart) before
    `dtolnay/rust-toolchain@master`, which then finds it installed. The date is unchanged.
  - Step 4: the log of job `133873087335` (run `44034218213`) shows the step before it (`test_feat3_os_isolation`, 30 passed)
    ending normally. The line `Process completed with exit code -1073741502` (`0xC0000142`, STATUS_DLL_INIT_FAILED) then
    follows the start of the step `python scripts/ci_heartbeat.py ... cargo test --workspace` 31 ms after its header, with no output
    of its own, and the diagnostics step after it (a fresh `pwsh`) fails with the same code. No `Running` line of a test
    binary and no `cargo` output follow: `cargo` never started, so it is a runner failure (new processes could not initialise),
    not a test. No test item is opened. Untested hypothesis: the isolation tests of the step before it create a private Desktop per
    task and may leave the runner short of desktop heap. If the code comes back after `test_feat3_os_isolation`, open an item for that.
  - Step 5: `scripts/ci_push_runs.py` and `tests/test_ci_push_runs.py` (13 tests over hand-made JSON; run by the `agent-bridge`
    job on Linux, macOS and Windows). Real output for 2026-11-16: 12 runs `cancelled`, 5 `never acquired`, 5 `failing test`.
    `43952618376` has a never-acquired macOS job and also a real failing Ubuntu test, so it counts under `failing test` (the
    priority is failing test, never acquired, cancelled, other). For 2026-11-17: 2 `other` (the two failures of this item), 1 failing
    test, 1 never acquired. The exit check of Q80 is read from its last line.

### ADOPT-1: A study of first use, in two rounds

- **Evidence:** [Design 0059](design/0059-first-users.md), D4. The owner decided on 2026-11-11 to
  run the first round with three to five people (Q37). The section "Study records" of that record
  says "None yet". The repository has 6 stars and 0 forks, and no issue has been filed.
- **Problem:** No person outside the project has installed the grid under observation. The order
  of AX-1, ADOPT-3 and ADOPT-4 rests on reasoning about users. Exit checks E8 and E10 of design
  0088 both start from this study, so it is the longest wait on the path.
- **Do:** Owner. Follow D4 of design 0059 as written:
  1. Ask 10 to 20 people the screening question, with no install, and record the answers as
     counts per kind of job.
  2. Run three to five sessions with people who named a job, on a cluster made for the study with
     its own secret. Record per session the two operating systems, the minutes to the end of each
     step, every point where the person stopped, and the job they chose.
  3. After 14 days, ask each person whether they ran a task of their own again.
  4. Write the dated table under "Study records" in design 0059. It holds no name, host name,
     address, secret or ticket.
  5. Apply the table "How the result changes the order" of D4, and record the outcome in the
     status log here.
  6. Run the second round after ADOPT-2, AX-1, AX-2, ADOPT-3 and ADOPT-4, with at least two
     people the owner has not helped before.
- **Done when:** "Study records" holds at least three sessions of the first round with measures
  M15 to M17. The item is Partial from then until the second round is recorded.
- **Status (2026-11-19, Planned):** study kit ready (PR 393): `docs/study/2026-11-adopt-1/` (English and Vietnamese participant guide, facilitator sheet, consent template, and round-one record procedure), `scripts/study_timer.py` (`P1`–`P5` labels only, standard library), and `tests/test_study_timer.py` (wired into CI `agent-bridge`); no session has run yet and nothing in the kit is a result.

### ADOPT-3: Install scripts and the scripted time to first result

- **Evidence:** [Design 0059](design/0059-first-users.md), D6. `scripts/install.sh` and
  `scripts/install.ps1` do not exist. `README.md` tells a user to download an archive, compare
  `SHA256SUMS` and unpack by hand. No record gives the time from the release page to a first
  result.
- **Problem:** The install is the first thing a stranger does, and it is the step with the most
  manual commands.
- **Do:** As D6 of design 0059, with its rules: no `sudo`, no service, no start of a master or a
  worker, a pinned release, a checksum comparison before unpacking, `--version`, `--dir` and
  `--dry-run`, and no pipe into a shell in `README.md`. Verify the attestation of the archive
  when LV-5 has landed and `gh` is present. Then write the scripted measure of M15, which times
  the Quick start from an empty home directory, with its records in `benchmarks/grid/`.
- **Done when:** The ADOPT-3 cases in `tests/test_launcher_scripts.py` pass (a matching checksum
  installs, a mismatch refuses and leaves no file, an unknown architecture is named, `--dry-run`
  writes nothing, a second run replaces the binaries), and M15 by script is recorded for macOS,
  Windows and Linux.

### ADOPT-4: Guides for common jobs, each with a recorded run

- **Evidence:** [Design 0059](design/0059-first-users.md), D7. `docs/guides/` has one guide for a
  job a user brings (`offload-cargo-test.md`) and one for GPU command tasks (`gpu-workloads.md`).
  `HOST_PROBED_TOOLS` in `crates/core/src/capabilities.rs` probes `cargo`, `rustc` and `git` only.
- **Problem:** A user with a job other than `cargo test` has no worked example to copy.
- **Do:** As D7 of design 0059. The first round of ADOPT-1 chooses among its candidates. Each
  guide follows the guide template of `docs/README.md`, uses tools the machine's owner installed,
  shows no `--insecure`, uses `--isolation fs` where the system supports it, and names the
  release of its last recorded run. The item may add a tool to `HOST_PROBED_TOOLS`.
- **Done when:** Three guides each have a "Verify" block and a dated run record that names its
  machines and release, and the index of `docs/README.md` lists them.

### AX-1: Matrix fan-out in `submit` and in MCP

- **Evidence:** [Design 0056](design/0056-frontier-program.md), D2, part A1. One `submit` call
  places one task on one worker. `crates/cli/src/cli_app/submit.rs` has no `--matrix` flag (the
  flag `--matrix-dim` belongs to the GPU benchmark task), and the MCP tool `submit` has no `matrix`
  field (`crates/cli/src/mcp.rs`).
- **Problem:** Checking one change on macOS, Windows and Linux takes three calls and three
  results that the caller merges. This is the first job of the first audience of design 0059, and
  the purpose of Q1.
- **Do:** As part A1 of design 0056. The expansion happens in the client, so the wire format does
  not change:
  1. `submit --matrix os=macos,windows,linux -- <command>` submits one task per value with the
     matching `require:` entry. `--matrix` may repeat for a product, up to a fixed cell limit.
  2. With `--workspace`, the tree syncs to each chosen worker.
  3. The client waits for every cell. `--json` prints one record per cell: cell, worker, exit
     code, duration, and the head and tail of the output. The exit code is non-zero when any cell
     fails.
  4. A cell that no worker matches is reported as unplaced after a check against `ListWorkers`.
     It does not hang.
  5. The MCP tool `submit` takes the same `matrix` field and returns the array.
  6. A design record, the `README.md` reference and `docs/guides/mcp-client-setup.md` land in the
     same pull request.
- **Done when:** An end-to-end test with three tagged workers returns three cell records from one
  call, and a cell without a worker is reported unplaced. A live record shows one MCP call
  testing a change on macOS, Windows and Linux. That record is exit check E8 of design 0088,
  together with AX-2, AX-3 and CACHE-1 in a release.

### AX-2: Sharded runs, with a build-once mode for Rust

- **Evidence:** [Design 0056](design/0056-frontier-program.md), D2, part A2, and spike R2. The
  CLI has no `shard` command, and no task gets `OX_SHARD_INDEX` or `OX_SHARD_TOTAL`. Spike R2
  (how much wall time sharding saves on this repository's suite) has no record.
- **Problem:** A slow test suite runs on one worker however many machines are idle.
- **Do:** Run spike R2 first, with today's `submit`. The stop rule of design 0056 applies: if
  sharding this repository's suite over three workers saves less than 30 % of the wall time,
  AX-2 ships as a recipe in a guide only, without the build-once mode. Otherwise build part A2:
  `oxideswarm shard --count <n> -- <command>`, the two environment variables, a guide with the
  recipe per test runner, and the build-once mode with `cargo nextest archive`.
- **Done when:** R2 has a dated record. Measure M1 of design 0056 is recorded for this repository
  on three workers with its script in `benchmarks/grid/`, or the status log states that the stop
  rule applied and the guide holds the recipe.

### AX-3: Live output, `logs`, and MCP tasks

- **Evidence:** [Design 0056](design/0056-frontier-program.md), D2, part A3. A client sees the
  output of a task only in its final result. `MCP_PROTOCOL_VERSION` in `crates/cli/src/mcp.rs` is
  `2024-11-05`. The CLI has no `--follow` flag and no `logs` command.
- **Problem:** An agent waits for a long build to end before it learns that the first test
  failed, and it cannot cancel early on what it sees.
- **Do:** A design record first, because this is a wire change: new message variants (in the
  public enum, both mirror enums and all four `From` impls), a `ProtocolCapabilities` bit and a
  protocol version bump, so the release-train rule of design 0076 applies. Q55 is open and
  decides whether a long-running MCP call works without the tasks extension. Then build part A3:
  output chunks from the worker while a task runs (`crates/worker/src/runner.rs`), a bounded
  buffer per task on the master (`crates/master/src/session.rs`), `submit --wait --follow`,
  `oxideswarm logs <task>`, and an MCP server that speaks `2026-07-28` with the tasks extension
  and keeps answering `2024-11-05` clients.
- **Done when:** A client sees the first output line while the task runs. An MCP client polls a
  running task and reads an output tail. The `2024-11-05` test in `mcp_server_e2e` still passes,
  and the `release skew` job passes against the previous release.

### LV-4: An inventory of `unsafe`, with comments, a lint and Miri

- **Evidence:** [Design 0058](design/0058-lasting-value.md), D2. A search for `unsafe {`,
  `unsafe fn`, `unsafe impl` and `unsafe extern` in `crates/*/src` finds 285 sites: 176 in
  `crates/worker`, 77 in `crates/core`, 29 in `crates/cli`, 2 in `crates/agent_mesh` and 1 in
  `crates/master`. A search for `// SAFETY` finds 103 comments, 23 of them in `crates/worker`. No
  crate sets `clippy::undocumented_unsafe_blocks`, `forbid(unsafe_code)` or `deny(unsafe_code)`,
  and no workflow runs Miri.
- **Problem:** Most `unsafe` sites are in the code that confines tasks and kills process trees. A
  reviewer cannot tell which invariant each one relies on, and nothing stops a new site without a
  comment.
- **Do:**
  1. Give every `unsafe` site a `SAFETY:` comment that states the invariant it relies on. Read
     the site before writing the comment. Where the invariant does not hold, write the failing
     test first, then the fix (rule 4), and use the private route of `docs/SECURITY.md` if a
     remote peer can reach it.
  2. Turn on `clippy::undocumented_unsafe_blocks` for the workspace, as an error.
  3. Add `#![forbid(unsafe_code)]` to every crate and module that has no `unsafe` site.
  4. Add a CI job on Linux that runs Miri on the unit tests it can run, and list in the job's
     comment which tests it skips and why. Miri cannot run calls into the operating system.
  5. Start with `crates/worker/src/isolation.rs`, together with MAINT-2.
- **Done when:** Measure M11 of design 0058 is met: every `unsafe` site has a `SAFETY:` comment.
  The clippy job fails on a site without one, and that job is a required check. The Miri job has
  run once on `master`.
- **Note (2026-11-18):** Part 1 (all crates except `isolation.rs`, `mcp.rs`, `mode_cli.rs`): pull
  request 324, 145 sites documented.
- **Note (2026-11-18):** Part 2 (test files): pull request 330, 34 sites.
- **Note (2026-11-18):** Unix isolation files: pull request 336, 22 sites.
- **Note (2026-11-18):** Part 3 (Windows isolation files): pull request 337, 117 sites.
- **Note (2026-11-18):** Do steps 2 and 4: pull request 340. `clippy::undocumented_unsafe_blocks`
  is `deny` in `[workspace.lints.clippy]`, and every crate has `[lints] workspace = true`; the clippy
  jobs are required checks. The `miri (rusty_grid_core)` CI job runs `cargo miri test -p
  rusty_grid_core --lib` on Linux; 93 of the 295 tests that Miri cannot run carry
  `#[cfg_attr(miri, ignore = "Miri: <reason>")]`. Open: the Miri job has not run on GitHub yet.
- **Note (2026-11-19):** Do step 3 (`win/lv-4-forbid-unsafe`): `#![forbid(unsafe_code)]` added at the
  root of all 4 zero-`unsafe` binaries (`oxideswarm`, `rusty-grid`, `ox-mode`, `agent-mesh`), and
  `#![deny(unsafe_code)]` at the root of all 6 library crates (`android_bridge`, `agent_mesh`,
  `master`, `core`, `cli`, `worker`) with `#[allow(unsafe_code)]` (one-line `// LV-4:` reason on
  each) limited to the modules/test submodules/10 `#[no_mangle]` JNI functions that require it, plus
  `#[forbid(unsafe_code)]` on `master::store::fuzzing`; production code in `core::{blob,task}` and
  `worker::{client,runner,staging}` stays denied. Complete module inventory is recorded in
  [design 0058](design/0058-lasting-value.md). Open: CI Miri job run on `master`.

### LV-15: Release runbook, secret inventory and a second releaser

- **Evidence:** [Design 0058](design/0058-lasting-value.md), D5 and proof run V3. Six releases
  were cut between 2026-11-09 and 2026-11-17. `docs/guides/` has no guide for a release. No file lists the secrets, accounts and
  domains the project depends on. `git shortlog -sn` lists one human author.
- **Problem:** If the owner is away, nobody can cut a release or knows which accounts one needs.
- **Do:**
  1. Write `docs/guides/cut-a-release.md` from the guide template: from the release pull request
     (versions, `CHANGELOG.md`, `fuzz/Cargo.lock`) to the tag, the `release.yml` run, the check of
     the archives against `SHA256SUMS` and, once LV-5 has landed, the attestation.
  2. List every secret, account and domain the project depends on, and who holds each, in the
     same guide. Name the kind of secret and its holder. Never write a value.
  3. Owner: give a second person the rights to merge and to release.
  4. That person cuts one release from the guide alone. Record the date, what was unclear and
     whether the archives verified. This is proof run V3.
- **Done when:** The guide exists and is in the index of `docs/README.md`. Proof run V3 is
  recorded with its date in the status log of design 0058. Until a second person has cut a
  release, the item is Partial.
- **Note (2026-11-18):** Pull request 351 adds `docs/guides/cut-a-release.md`
  (covering wire-change rules, release PR preparation, owner merge and `v*` tag push, `SHA256SUMS`
  and `gh attestation verify` checks, isolated macOS and Windows loopback smoke checks, and the step
  2 accounts and workflow-permissions skeleton table) and adds it to `docs/README.md`. Confirming
  holders in step 2, granting a second person merge/release rights (step 3), and proof run V3
  (step 4) stay with the owner.

## Phase 14: public launch

The launch steps of [ROADMAP.md](ROADMAP.md) ("Launch") as items. They wait for the stability
release: the exit check of Q80 (E3 of [design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md)),
the records of TEST-24 and TEST-25 (E7) and one run across two real networks (E5). Until then a
release is for early users who accept its limits (ROADMAP.md, "What is missing").

Order:

1. LAUNCH-1, the gate. Agents hold the open test items; the owner holds TEST-24, TEST-25, the
   second network and the first round of ADOPT-1. Phase 13 (the path to finished, design 0088) runs
   beside it.
2. LAUNCH-2, on the release that passes the gate.
3. LAUNCH-3, then LAUNCH-4.
4. LAUNCH-5.

The second demo of ROADMAP.md (a language model larger than one machine) waits for 0.5.0 and is
not part of this phase.

### LAUNCH-1: Hold the public launch for the stability release and its records

- **Evidence:** Read on 2026-11-17, after release `0.4.1`. The flaky tests TEST-23, TEST-29, TEST-41 to
  TEST-48 and BUG-25 are open; TEST-24 and TEST-25 have no record; no run across two real networks is
  recorded (LINK-1, USB-1c). A first visitor meets exactly these: a red CI, a task that runs twice or
  loses its output after a restart, a link that does not cross their router.
- **Problem:** A launch reaches most people once. If the first run fails, it does not get a second look.
- **Do:** Keep a checklist in this section, updated as each part holds:
  1. The exit check of Q80 holds: every flaky test has an Implemented item with its loop of runs, and
     `master` has seven days of runs for pushes in which no test fails.
  2. TEST-24 and TEST-25 each have one dated record.
  3. One run across two real networks is recorded (LINK-1), with the USB kit if it is used (USB-1c).
  4. The first round of the study of first use (ADOPT-1, Q37) is recorded, and the defects it found that
     stop a first run are fixed.
  5. The release that passes 1 to 4 is tagged (a `0.4.x` with no wire change).
- **Done when:** All five lines hold, each with the record or run that shows it, and the owner names the
  launch date.

### LAUNCH-2: Install and run from the README alone on clean machines

- **Evidence:** Read. The release archives were checked by the people who built them (smoke tests of
  0.4.0 and 0.4.1); nobody has followed only the README on a machine that never had Rust or OxideSwarm.
- **Problem:** Every step that needs knowledge outside the README stops a new user.
- **Do:** On a clean macOS, Windows and Linux machine (a new user account is enough), follow only the
  README's install and quick start with the release archive, up to one task that runs on the other
  machine, then the MCP setup of [guides/mcp-client-setup.md](guides/mcp-client-setup.md) with one coding
  agent, using the install scripts of ADOPT-3 where they exist. Note the time to the first finished task
  and every step where the text was not enough, and fix the text. ADOPT-3 scripts the same measure (M15);
  this item is the walk-through by a person on each system.
- **Done when:** A dated record lists the three systems, the times and the fixes, and a second pass on
  one system needs no knowledge outside the README.

### LAUNCH-3: Record the demo of the user story

- **Evidence:** Step 1 of "Launch" in ROADMAP.md; the run of 2026-11-09 (Phase C) shows the story works
  but is a log, not a demo.
- **Problem:** The user story (an agent on one system runs `cargo test` on a machine with another, and the
  second run is faster) is the reason to try the project, and it is not visible without a demo.
- **Do:** Record a short screen capture (two minutes or less) of a coding agent on one machine that, over
  MCP, runs `cargo test` of a real repository on a machine with another operating system, twice, with the
  second run faster. Keep the run record (commands, versions, times) beside it, and use only numbers
  from that record.
- **Done when:** The video and its run record are published and linked from the record.

### LAUNCH-4: Put the demo, the release and the benchmarks at the top of the README

- **Evidence:** Step 2 of "Launch" in ROADMAP.md.
- **Problem:** A visitor reads the top of the README and leaves.
- **Do:** At the top of `README.md`: one sentence on what the project is, the demo (LAUNCH-3), the
  latest release with its install line, and the BENCH-1 page. Every number links to its run record
  (writing standard rule 6 of [docs/README.md](README.md)). Keep "What is missing" current, and say in
  `docs/SECURITY.md` and the README that no independent security review has been published (LV-6,
  design 0088).
- **Done when:** The README top holds those parts, every link resolves (`ox-mode doc`), and the limits
  match ROADMAP.md.

### LAUNCH-5: Announce the release

- **Evidence:** Step 3 of "Launch" in ROADMAP.md; the research of 2026-11-08 there on launch posts.
- **Problem:** Without an announcement nobody finds the release.
- **Do:** The owner posts a short announcement (for example a "Show HN"): what it is, the demo, how to
  try it in five minutes, and the limits. Answer the first day's questions; turn each defect report into
  an item.
- **Done when:** The post is published and its link and date are in the status log, with the items it
  produced.

## Phase 15: coordination between agents

From [design 0087](design/0087-shared-coordination-state.md) (accepted on 2026-11-17, Q84 to Q86). Several
agents and Claude Code sessions change this repository at once; these items keep them from taking the
same id or changing the same lines unawares, and share what one of them learned. Nothing in the grid
changes.

## Phase 16: Anima Engine as the first workload

From [design 0089](design/0089-anima-engine-first-workload.md). The owner asked on 2026-11-17 that
OxideSwarm support the owner's simulation project, Anima Engine, as far as it can. Anima Engine runs sweeps
of independent trials that each end in a checksum, and a Rust build too heavy for the owner's development
machine. Order: APP-4 (needs nothing built), VERIFY-1, then AX-1 and AX-2 of Phase 13 with Anima Engine as
their first user (Q88), then APP-5.

### APP-4: Trial: Anima Engine trials on the owner's two machines, checksums compared, a sweep through the grid

- **Evidence:** [Design 0063](design/0063-two-directions-in-parallel.md) D5 and design 0089 D1. Anima Engine
  ran the E-SEL1 smoke sweeps of 2026-11-17 on one Mac.
- **Do:** As design 0089 D1: build the headless runner on the Mac and the Windows case machine; run one
  manifest and seed in deterministic mode on the CPU on both and compare checksums; run a sweep through
  `submit` on one and then two workers. Never stage `.env`. A full backend build has hung the owner's
  development machine: build with `-j 2` and only while someone watches.
- **Done when:** A dated record holds the checksums of both machines and the trials per hour on one and two
  workers.
- **Partial (2026-11-18):** the Windows half ran on 2026-11-17 (agent-win, with the owner's yes). The runner
  builds in release on Windows x86_64, and the committed smoke trial gives checksums that differ from the
  Mac's at the same commit and inputs while each system repeats its own (design 0089 D6, VERIFY-2). The
  sweep through the grid (step 3) is BENCH-3 of design 0093; the Mac run of `28c45ef` is the owner's.

### APP-5: Guide: Anima Engine sweeps on OxideSwarm, with a recorded run

- **Do:** `docs/guides/anima-engine-sweeps.md` as design 0089 D5, with GPU and CPU variants and VERIFY-1.
- **Done when:** The guide exists with a recorded sweep and is linked from `README.md`.

## Phase 17: benchmarks worth showing

From [design 0093](design/0093-benchmarks-worth-showing.md). The owner asked on 2026-11-18 whether the
benchmarks are strong enough to show. The records are scripted and honest, but none of them measures a
second machine against the fastest machine alone, offload against the laptop itself, or two machines on
different networks. Every record follows the method of design 0093 D1: the plan is committed before the run,
the baseline is the fastest machine alone, and efficiency is measured against the sum of what each machine
does alone. Order: BENCH-2, BENCH-5, BENCH-3, BENCH-4, BENCH-6, then BENCH-7 before LAUNCH-4.

### BENCH-3: Sweep scaling

- **Evidence:** Design 0093 D2. No record runs real work on two machines against the fastest one alone.
- **Do:** On the Mac and the Windows case machine, as D2: K trials on each machine alone, through the grid
  on each worker, then on both; W1 at K = 200 and, by Q90, the Anima Engine runner (W2) at K = 60. A master
  for the run has a private home, its own port and a secret made for the run. The W2 record also completes
  step 3 of APP-4.
- **Done when:** The records are committed under `benchmarks/sweep/results/` with speedup against the
  fastest machine and efficiency against the sum of both.
- **Partial (2026-11-18, pull request 349):** W1 on both machines, by the plan committed before the run
  (`benchmarks/sweep/plans/2026-11-18-bench-3-w1-two-machines.json`, 120 tasks of 16,000,000 units, one
  warm-up and 3 measured runs). Release 0.4.3 binaries; the plan names 0.4.2, and 0.4.3 shipped the same day
  without a wire change. Each machine alone, best level (median of 3): the Windows case machine (i5-14600KF,
  20 processes) 1.631 tasks/s, the Mac (M5, 10 processes) 1.412 tasks/s. Both through one master on the LAN
  (Windows worker 20 slots, Mac worker 10 slots): 2.233 tasks/s, speedup 1.37 against the fastest machine
  alone, efficiency 0.73 against the sum (3.04 tasks/s); 360 of 360 checksums equal to the local ones, no
  failed task. The plan expected at least 0.85: BENCH-8 finds why. Not done: each worker alone through the
  grid (the plan's step 5 controls), K = 200, and W2 (Q90).

### BENCH-4: Offload against local work

- **Evidence:** Design 0093 D3. BENCH-1 (c) never timed the same command on the Mac itself.
- **Do:** Run `cargo test -p rusty_grid_core --lib --offline` and `cargo build --release --locked --bin
  oxideswarm` locally on each machine and offloaded from the Mac, cold, warm and after a one-line edit, and
  record the CPU seconds the Mac spends in each arm. The ssh arm runs only if Q92 is yes.
- **Done when:** A record holds every arm of D3.

### BENCH-5: Per-task overhead and the break-even task length

- **Evidence:** Design 0093 D4. BENCH-1 (a) measures empty tasks on loopback only.
- **Do:** On the LAN, W1 tasks of 0, 10 ms, 100 ms, 1 s and 10 s on the fastest machine alone and through the
  grid on one and two workers, with one CLI process per task and with one long-lived client.
- **Done when:** A record gives p50 and p95, throughput and efficiency per task length, and the shortest
  length at which two workers beat the fastest machine alone.

### BENCH-6: Two networks

- **Evidence:** Design 0093 D5. No record measures two machines on different networks; design 0054 plans
  clusters across the internet.
- **Do:** With the Mac on a phone's hotspot (the owner switches it) and the Windows worker on the home LAN:
  `doctor --json` (link kind, round-trip time, MB/s), 100 empty tasks, one 100 MiB input, and BENCH-3 with W1
  at K = 50; then the same on the LAN the same day.
- **Done when:** Both records are committed. A relayed link is recorded as it is.

### BENCH-7: Benchmark headlines at the top of the README

- **Evidence:** Design 0093 D6. `README.md` links the hand-typed interconnect report as the comparative
  benchmark.
- **Do:** Lead the "Measured" table of `README.md` with the headlines of BENCH-3 to BENCH-6, each linked to
  its record. Handle the interconnect report by Q91 (default: unlink it from the README and keep the file
  with its header).
- **Done when:** The first "Measured" rows link to the records, and `ox-mode doc` passes.

### BENCH-9: Split the per-machine cost of running W1 through the grid into task process time and slot idle time

- **Evidence:** BENCH-8: each worker alone through the grid reaches about 0.78 of its machine alone. On the
  Windows case machine the master on another machine over the LAN costs 0.16 (about 3 s more per task than
  with the master on the same machine); on the Mac, with the master on the same Mac, 0.16 remains after the
  0.05 of the default task priority.
- **Problem:** The records keep only the median submit-to-result time per run, so they cannot tell whether a
  task process runs slower through the grid (priority, heat, cores) or whether slots stand idle between
  tasks (the round trip from one result to the next assignment).
- **Do:**
  1. The harness records, per grid run, the sum of the worker-reported task times, the slot seconds
     (slots times wall time), their ratio, and per worker the mean and median task time; per local level the
     mean and median time of a task process. It also samples for cargo and rustc processes during each run
     and records what it saw.
  2. Under a plan committed first: the Windows worker alone with the master on the Mac over the LAN, and
     with the master on the same machine; the Mac worker alone, and the Mac's local sweep at 10 processes,
     back to back.
  3. If slots stand idle on the LAN path, find which step of the round trip takes the time (master and
     worker logs at debug level for a few tasks).
- **Done when:** The records split the per-machine cost into process time and idle time on both machines,
  and this section names the cause of the LAN cost on Windows and of the open 0.16 on the Mac, or the change
  that closes them.
- **Partial (2026-11-18, pull request 354):** Step 1 implemented (`benchmarks/sweep/bench_sweep.py`,
  record format 2): grid runs record `task_time_sum_s`, `task_time_missing`, `slot_seconds`, `utilization`, and
  per-worker `tasks`, `mean_s`, and `median_s` (over succeeded tasks); local runs record `task_time_mean_s` and
  `task_time_median_s` per run and level; and a low-rate sampler watches for `cargo` / `rustc` during each run
  (`competing_processes`). Steps 2 and 3 (the live runs on Windows and Mac under a committed plan, and debug-log
  round-trip breakdown if idle slots remain) are next.

## Phase 19: Stage 1 of the research grid, trusted circles and the first campaign

From [design 0094](design/0094-road-to-a-shared-research-grid.md) (Accepted, 2026-11-19). The owner's
ultimate goal is that the people who use OxideSwarm pool their machines to research one problem
together. Stage 1 is a circle of people who know each other, such as friends or a lab, who join one
cluster from several homes and complete a first campaign. Its exit check: about five owners and ten
machines complete one campaign, and every result is verified.

The items are outside the stop line of
[design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md): its D1 puts ECO-3 and
Phase H outside, and the owner placed APP-4 and APP-5 there. Gate: implementation merges after the
stability release (Q80); docs-only and test-only work may merge before it, and so may the dry run of
APP-6, which builds nothing. Order: ECO-9 and NET-9 in parallel, then NET-10, then APP-6. The
recorded join from another network that APP-6 needs is part of ENROLL-1 (its "Done when", and
ENROLL-1d).

The owner decided Q94 to Q97 on 2026-11-19 at the recommended answers ("Approve as recommended"):

- **Q94:** the pilot runs an Anima Engine parameter sweep in deterministic CPU mode. The first Stage 2
  campaign is chosen after the pilot's record, by the rules of design 0094: independent tasks, results
  verified by a checksum or a cheap check, code and outputs publishable under design 0057, no personal
  data in the inputs.
- **Q95:** the owner and three to five people the owner already knows, with about ten machines on at
  least two home networks and at least two machines of each operating system and architecture in use.
- **Q96:** credits only display: not transferable, never spent or sold, with no cash value. It closes
  Q14 and Q43.
- **Q97:** Stage 1 runs in parallel with `0.5.0`, as one lane at most, with implementation merged after
  the stability release.

### ECO-9: Per-participant quotas: running tasks, slots and priority

- **Evidence:** Read on 2026-11-19 at `master` `f77eabd`. A member is one machine key (`MemberEntry`
  in `crates/core/src/noise_auth.rs`: id, name, key, roles and times), and nothing groups the
  machines of one person. Fair share is weighted round robin per submitter
  (`crates/master/src/fair_share.rs`). The submitter is a new id for every client connection
  (`SubmitterId::Client(Uuid::new_v4())` in `crates/master/src/server/client_and_members.rs`), and
  it is not persisted (`crates/master/src/queue.rs`). The queue and the journal do not record which
  member submitted a task; step 2 of revocation in design 0051 D5 needs it and is Planned
  (ENROLL-1c). The only caps are cluster-wide: `--max-queue-size` and `--max-active-mb`.
- **Problem:** In a circle one participant can fill every slot. A participant who runs one `submit`
  per task gets one fair share per process.
- **Do:**
  1. Write a design record. Add a participant label to each member, set when the invite is made or
     later by an admin, and show it in `members list`. Secret (v2) peers and the master's host count
     as the organiser.
  2. Record the submitting member of each task in the queue and the journal, as one change with
     step 2 of ENROLL-1c (a store format change that the owner approves there).
  3. Key the fair share on the participant, with a weight per participant.
  4. Add per-participant caps on running tasks, slots held and queued tasks. A task over a running or
     slot cap waits with a reason that `status` shows. A submission over the queued cap is refused
     with a typed error.
  5. State in `README.md` and `docs/SECURITY.md` that quotas share the grid between people who trust
     each other and are not a boundary against a hostile participant (design 0050 D6).
- **Done when:** Tests on a loopback cluster pass: a participant with a cap of two running tasks
  never has more than two running; under contention, a participant who submits through one
  connection and one who submits through twenty each get half the slots; a submission over the
  queued cap gets the typed error; the submitter and the caps survive a master restart with
  `--state-dir`. `members list` shows the label.

### NET-9: Contributor controls on each machine

- **Evidence:** Read on 2026-11-19 at `master` `f77eabd`. A worker has `--max-concurrency`,
  `--task-priority normal|below-normal|idle` (default `below-normal`) and `--isolation` (`WorkerArgs`
  in `crates/cli/src/cli_app.rs`). The host CPU threshold for new tasks is the master's
  `--max-host-cpu-pct` (default 85, `crates/master/src/scheduler.rs`). A worker on a laptop reports
  its battery (`spawn_mobile_poller` in `crates/worker/src/heartbeat.rs`), and the master's scheduler
  lowers its concurrency by battery tier (`calculate_effective_concurrency` in
  `crates/core/src/capabilities.rs`); the contributor cannot ask for no task on battery. The worker
  reads no network cost and has no schedule. The only local stop is ending the worker process, whose
  signal handler ends every task process tree (`terminate_all_task_process_trees` in
  `crates/worker/src/process_tree.rs`).
- **Problem:** In a circle the master belongs to the organiser. A contributor's limits must hold on
  the contributor's machine, whatever the master asks.
- **Do:** Write a design record. Then add these controls to the worker, set in the contributor's own
  configuration (flags, `RUSTY_GRID_*`, `[worker]` of the config file) and enforced by the worker:
  1. a host CPU threshold of the worker's own, above which it takes no new task;
  2. hours: a weekly schedule in local time, and a cap of task hours per day;
  3. no new task on battery;
  4. no new task on a metered network, where the operating system reports it (connection cost on
     Windows, the network path on macOS, NetworkManager on Linux); where it cannot tell, the worker
     says so at start;
  5. a local stop switch (for example `oxideswarm worker pause` and `resume`) that takes no new task
     and ends running tasks within 5 s, while the worker stays connected; the tasks go back with the
     refusal of [design 0090](design/0090-sandbox-base-refusal-hand-back.md) and run elsewhere
     without a retry charged;
  6. a log line that names the control holding a task back, and the controls in `README.md`.

  Opt-in per campaign, the policy of task classes and limits across several worker processes of one
  machine stay in NET-4. A rule for an idle screen stays in GREEN-1 of design 0056.
- **Done when:** Worker tests with an injected clock, battery probe and network-cost probe pass: no
  task starts outside the schedule, on battery, on a metered network or above the CPU threshold; a
  task assigned while the worker is paused is handed back; `pause` ends a running task within 5 s,
  and the task completes on another worker with its retry count unchanged. The record of APP-6 lists
  the controls each contributor set.

### NET-10: Contribution credits per participant, display only

- **Evidence:** Read on 2026-11-19 at `master` `f77eabd`. A task result carries the worker id and the
  run time (`worker_id` and `execution_time_ms` of `TaskResult` in `crates/core/src/task.rs`).
  `members.json` maps each worker id to the member that owns it (`worker_owners` of `MembersFile` in
  `crates/master/src/members.rs`). The master evicts finished tasks beyond `--max-retained-tasks` and
  `--max-retained-mb`, so the task table cannot be the account. No credit or usage record exists.
  [Design 0061](design/0061-first-application-model-tunnel.md) proposes usage records and a balance
  per member for model tunnels (LLM-10, LLM-11), display only by the recommendation of Q43.
- **Problem:** A participant cannot see what their machines gave to the circle or what their tasks
  took from it.
- **Do:** Write a design record. Then:
  1. Keep a bounded, append-only ledger on the master, in `--state-dir` when it is set and in memory
     otherwise. For each finished task it holds the participant whose machine ran it, the participant
     who submitted it, the run time, the cores and GPU the task reserved, the exit code and the time.
     Each entry credits one participant and debits another by the same amount (design 0054 D6). It
     has the format of the usage records of LLM-10; the item that lands first defines it.
  2. Show per participant the tasks and core-seconds contributed and used, over the last day, the
     last week and all time, in `members list`, its `--json` and the dashboard.
  3. Keep credits display only: no rule reads them, they cannot move between participants, and they
     have no cash value (Q96). Nothing sends them off the master (Q39).
- **Done when:** On a loopback cluster with two participants, the core-seconds each contributed and
  used match the run times of their tasks, contributed minus used sums to zero over all
  participants, the numbers survive a master restart with `--state-dir`, and the ledger stays within
  its bound.

### APP-6: Pilot campaign: an Anima Engine sweep on several owners' machines, every result verified

- **Evidence:** Design 0094 D4. APP-4 is Partial: the Anima Engine runner builds on Windows x86_64
  and macOS arm64, and one trial gives the same checksum on repeat runs of one system and different
  checksums on the two systems (design 0089 D6). `submit --verify <N>` runs N replicas on the first
  worker's operating system and architecture and compares the outputs by BLAKE3 (VERIFY-1, VERIFY-2,
  `crates/cli/src/verify.rs`). BENCH-3 ran a sweep on two machines of one owner. No run has had
  machines of more than one owner, and no join from another network is recorded (ENROLL-1).
- **Problem:** Nothing shows that a group of people can complete a campaign together, or what stops
  them.
- **Do:**
  1. Before the run, commit the campaign sheet under `benchmarks/campaigns/<date>-pilot/`: the
     workload (the runner at a fixed Anima Engine commit and its hash, deterministic mode,
     `ANIMA_USE_GPU=0`), the input set (the manifest and the grid of seeds and parameters), the
     verification rule (`submit --verify 1` with the trial's `checksums.sha256` as `--verify-file`),
     what the results are and who sees them, the controls each contributor is asked to set, and the
     participants by label (Q95). Never stage `.env` (design 0089).
  2. Dry run, which builds nothing and may come before NET-9 and NET-10: two or three participants
     on a released `0.4.x`, joined by invite with the role `worker`; the organiser is the only
     `client`.
  3. The pilot: about five participants and ten machines on at least two networks, with at least two
     machines of each operating system and architecture in use. Each worker runs `--isolation fs+net`
     where the runner works under it, `fs` otherwise, with the controls of NET-9; the master shows
     the credits of NET-10.
  4. The organiser runs the sweep with a driver over `submit --json --verify 1`. A mismatch is run
     again, and its cause is recorded.
- **Done when:** A record under `benchmarks/campaigns/` gives, per participant label and without
  names, host names or addresses: the machines with system and cores, the network and link kind
  (`doctor` check C7), the hours online, the tasks run, the credits shown and the controls set; the
  verification result of every task, with each mismatch explained; and what each participant found
  hard. It is the exit check of Stage 1 in design 0094.
- **Partial (2026-11-19, pull request 376, merged; item stays `Planned`):** Steps 1 and 4 done: committed
  the campaign sheet and one-page participant guide (`benchmarks/campaigns/2026-11-pilot/README.md`), the
  200-unit deterministic CPU manifest (`benchmarks/campaigns/2026-11-pilot/manifest.json`, noting the
  Anima Engine runner blocker and switching `--verify-file` to deterministic `out/smoke/verify_checksums.txt`),
  `dry_run_manifest.json` + `standin_workload.py` for step 2 on `0.4.4`, the standard-library Python driver
  (`benchmarks/campaigns/driver.py`), and `tests/test_campaign_driver.py` (wired in
  `.github/workflows/ci.yml`), verified against a fake `oxideswarm` and on a 2-worker local loopback
  cluster on `dry_run_manifest.json`. Steps 2 (dry run) and 3 (pilot run, blocked on the Anima Engine
  runner update as well as `NET-9` and `NET-10`) remain **Planned**.
- **Partial, runner ready (2026-11-19, pull request 383, open; item stays `Planned`):** the runner is ready (Anima Engine pull
  request 167, merge commit `963f436e` on `feat/ecological-selection`): `run_esel1_experiment --trial`
  takes the seed and the four grid parameters, stages no files, and writes a deterministic
  `out/trial/verify_checksums.txt`. The sheet and `manifest.json` pin that commit with `runner_ready: true`
  and `--verify-file out/trial/verify_checksums.txt`; the organiser fills in the per-system runner hashes
  before the dry run. The owner allowed this pilot on invited participants' machines as an exception to
  Anima Engine's rule `QĐ-OXS` (2026-11-19). `driver.py` stages a `windows/...` runner as
  `run_esel1_experiment.exe`. Steps 2 and 3 remain **Planned** (step 3 waits for `NET-9` and `NET-10`).

## Status log

Add one dated line per finished item, newest last.

- 2026-11-01: Backlog written from a code audit and the runs in the test baseline.
- 2026-11-01: INT-1 completed; pull request 3 (`b76d85b`) and pull request 4 (`c92d1dd`) merged and `master` CI run `41276400780` passed.
- 2026-11-01: INT-2 completed; closed pull requests 1 and 2, deleted merged remote branches, and copied PR 2 CI details into TEST-6 (`origin/claude/trusting-wing-d9a27c` kept until TEST-6).
- 2026-11-01: INT-3 completed; rebased `d0dddb8` (cluster auth v2 and record-layer hardening) onto `master` and verified all gates plus `tests/cluster_record_layer_e2e.rs`.
- 2026-11-01: INT-4 completed; rebased `fae2622` (content-addressed data plane, design 0006) onto `master` after `INT-3` and verified all gates plus `tests/blob_transfer_e2e.rs`.
- 2026-11-01: INT-4 reopened. Pull request 6 is open at `e1215ee`, and `clippy (windows-latest)` fails. INT-5 holds the review findings to fix before the merge, and BUG-5 holds the smaller ones.
- 2026-11-01: `agent_bridge.py watch` gained once-only delivery and a Claude Code wake (design 0007),
  which also fixed the `--auto-pull` part of BRIDGE-1. This needs no backlog item of its own.
- 2026-11-01: INT-5 completed; implemented F1 to F6 on `int-4-content-addressed-data-plane` (Windows clippy, symlinked output parent rejection, output blob pin retention on Master, immediate output file pinning in worker cache, bounded in-flight blob requests per session, and protocol version 2 enforcement).
- 2026-11-03: DOC-2 completed; removed unbacked throughput, scaling, and working-set RAM figures from `README.md` and `windows/README.md`.
- 2026-11-04: DOC-1 completed; mapped all status cells in `README.md`, `PROJECT.md`, and `ANDROID_ARCHITECTURE.md` to the fixed vocabulary (`Implemented`, `Partial`, `Planned`, `Design only`), corrected false claims across `README.md`, `PROJECT.md`, `CLAUDE.md`, `DEPLOYMENT.md`, `ANDROID_ARCHITECTURE.md`, `docs/SECURITY.md`, `docs/design/0002-mobile-coordination.md`, and `packaging/android/README.md` (intentionally keeping MCP `cancel` in `README.md:94,116` because `mcp.rs:592` implements it, and keeping `--mapper` / `--reducer` script shipping in `README.md:78` because `cli_app.rs:822-828` implements it), added a dated per-platform verification table backed by recorded runs, and restored the `Exactly-once execution` (`Planned`) limitation row in `README.md`.
- 2026-11-04: The Unix default sandbox base became `<temp>/rusty_grid-<euid>/sandboxes`. The worker now refuses
  a `rusty_grid-<euid>` that another local user could change (work item SEC-8 of design 0021).
- 2026-11-04: REL-3 completed on branch `win/rel-3-repo-hygiene`; added `test_reports/` and `**/jniLibs/**/*.so` to `.gitignore` with a regression check in `tests/test_launcher_scripts.py`.
- 2026-11-04: DOC-5 completed on branch `win/doc-5-3d-subsystem`; added `docs/design/0023-generative-3d-and-autorig-subsystem.md`, documented `gen3d`, `autorig`, and `engines/` (including `3D-2` and `3D-3` limits) in `README.md` and `CLAUDE.md`, and updated the macOS, Windows, and Android Termux worker installers to deploy `engines/` and set `OXIDESWARM_GEN3D_SCRIPT`.
- 2026-11-07: Index synced with the pull requests merged up to `5c518b5`.
  These items are now Implemented (date of the first merge, pull requests):
  - INT-4: 2026-11-01, pull request 6.
  - REL-1: 2026-11-01, pull requests 8, 82.
  - SEC-1: 2026-11-01, pull request 11.
  - SEC-7: 2026-11-01, pull request 12.
  - SEC-3: 2026-11-01, pull request 13.
  - SEC-4: 2026-11-01, pull request 14.
  - SEC-2: 2026-11-01, pull request 15.
  - BUG-1: 2026-11-01, pull request 16.
  - SEC-5: 2026-11-02, pull request 17.
  - BUG-4: 2026-11-02, pull request 18.
  - BUG-2: 2026-11-02, pull request 20.
  - BUG-5: 2026-11-02, pull request 26.
  - DOC-7: 2026-11-02, pull request 32.
  - MR-1: 2026-11-02, pull request 33.
  - TEST-5: 2026-11-03, pull request 19.
  - BUG-3: 2026-11-03, pull requests 21, 44.
  - 3D-1: 2026-11-03, pull requests 22, 46.
  - SEC-6: 2026-11-03, pull request 25.
  - CLI-1: 2026-11-03, pull requests 30, 47.
  - MCP-1: 2026-11-03, pull requests 31, 41.
  - SCHED-1: 2026-11-03, pull requests 34, 38.
  - TEST-3: 2026-11-03, pull request 39.
  - TEST-1: 2026-11-03, pull request 51.
  - TEST-7: 2026-11-03, pull requests 55, 66.
  - DOC-6: 2026-11-03, pull request 56.
  - MCP-2: 2026-11-04, pull request 62.
  - DOC-3: 2026-11-04, pull request 68.
  - DOC-4: 2026-11-04, pull request 69.
  - REL-2: 2026-11-04, pull request 74.
  - REL-5: 2026-11-04, pull request 75.
  - TEST-2: 2026-11-04, pull request 76.
  - BRIDGE-1: 2026-11-04, pull request 77.
  - SCHED-2: 2026-11-04, pull request 78.
  - FEAT-6: 2026-11-04, pull request 81.
  - MESH-1: 2026-11-07, pull request 94.
  - MCP-3: 2026-11-07, pull request 97.
  - MOB-1: 2026-11-07, pull request 98.
  - MOB-2: 2026-11-07, pull request 99.
- 2026-11-07: These items are Partial:
  - TEST-4: pull request 29 fixed the six named tests. No run of the full suite with a dummy
    `cluster.secret` in `HOME` has been recorded.
  - TEST-6: every job exists and passed on `master` in run 41798333188 (`d021094`). The
    `workspace-test` job still has `continue-on-error: true`, and `master` has no required checks.
  - FEAT-8: pull request 40 pins the bincode layout of every message in
    `crates/core/src/wire_layout_tests.rs`. Nothing forces a protocol version bump yet, and
    `ClientMessage` has no version.
- 2026-11-07: Planned items with an open pull request: SCHED-3 (96) and MODE-1 (92).
- 2026-11-07: Items found after the audit have no section in this file: BUG-6 to BUG-17, TEST-8
  to TEST-16, SEC-8, BRIDGE-2 and BRIDGE-3. Each has a merged pull request except TEST-16
  (pull request 95, open).
- 2026-11-08: Index synced with `master` `5df82df`.
  - SCHED-3 is Implemented: 2026-11-07, pull requests 96, 107.
  - TEST-16 merged on 2026-11-07 in pull request 95. Its follow-up TEST-16b is pull request 109,
    open.
  - Open pull requests: MODE-1 (92), MOB-3 (108), FEAT-8 (110).
  - FEAT-4: design 0039 (pull request 105) is Accepted and builds nothing yet. The draft design
    0042 (pull request 111) was not taken.
- 2026-11-08: The roadmap (`ROADMAP.md`) sets the order after this backlog. Its decision Q2 freezes
  3D-2 and 3D-3. Its decision Q3 drops the grid features of MESH-3, and keeps only the removal of
  the hub-local file fallback in `parse_binary_data`. These items stay Planned.
- 2026-11-08: TEST-4 is Implemented: pull requests 29, 112. At `master` `9ed146e` on macOS,
  `cargo test --workspace --locked --no-fail-fast` gave the same result with an empty temporary
  `HOME` and with a dummy `cluster.secret` in it: 1700 passed, 0 failed and 4 ignored in 124 test
  binaries. Neither run wrote anything else under `HOME/.oxideswarm`. Pull request 112 records the
  same check at `5b16f55` in `README.md`.
- 2026-11-08: Four items are Implemented. Each "Done when" was checked at `master` `553b53e`.
  - MODE-1 (pull request 92): `crates/core` has no `Command::new("cargo")`, no source in `crates/`
    prints `OPTIMAL`, and the engine is in `crates/cli/src/mode_cli.rs`.
  - MOB-3 (pull request 108): `tests/mobile_coordination_test.rs` and the queue unit tests assert
    `failover_count` 0 after repeated evacuations.
  - FEAT-8 (pull requests 40, 110, 120): the CI step "verify wire_layout.txt version bump against
    baseline" fails a layout change without a version bump. Design 0040 records the version refusal
    and, in D4, the plan to leave `bincode` 1.x.
  - TEST-6 (pull requests 67, 124): `ci.yml` has no `continue-on-error`. The 21 required checks on
    `master` are every check of the `553b53e` run, and all passed. The one exception is
    "p2p wan relay (ignored)", which runs only on a schedule or by hand and so cannot be required.
- 2026-11-08: FEAT-11 is Implemented: design 0044 (pull request 118) and its four parts, pull
  requests 125, 126, 127 and 131. Checked at `master` `b03172a`:
  - `dashboard/index.html`, `shaders/hash.wgsl`, `DashboardState`, `prepare_spec`,
    `probe_physical_adapter` and the `service` JNI exports are gone from `crates/`.
  - `auto_rig_mesh_async` has a production caller in `crates/worker/src/runner.rs`.
    `crates/core/src/arbitrage.rs` keeps 181 lines, and `MasterLeaseTicket` is used by
    `protocol.rs` and by `reject_master_role_claim` in `crates/master/src/server.rs`.
  - `handle_connection` and `handle_multiplexed_connection` in `server.rs` both call
    `WorkerSession::run_connection` (`crates/master/src/session.rs`).
  - Leftover sandboxes are swept at start-up (pull request 126).
  - `compute_intensity` (design 0044 D1.5): marked `#[deprecated]` without a wire break in `F11-a`
    (pull request 125) and removed from `TaskSpec::GpuCompute`, `HumanTaskSpec::GpuCompute`, and
    `BinaryTaskSpec::GpuCompute` at the protocol version `3 -> 4` bump in `FEAT-1` (`F1-b` wire,
    pull request 141, design 0046).
- 2026-11-08: FEAT-2 is Implemented: pull request 123, design 0031. Checked at `master` `6723945`:
  `test_sigkill_of_the_worker_kills_its_child_grandchild_and_setsid_descendants_within_five_seconds`
  in `tests/cli_worker_signals_e2e.rs` starts the real worker, sends `kill -9`, and asserts that a
  child, a grandchild and a `setsid()` descendant are dead within 5 s. It is compiled on Unix only,
  so CI runs it on Linux and macOS. `run_chaos_test.sh` fails when the killed worker's sandbox
  writes a `done:` line. Android is covered by the design, not by a test.
- 2026-11-08: MESH-3 is Partial: pull request 114 removed the hub-local file fallback from
  `parse_binary_data`, and pull request 132 refuses a string that is neither base64 nor hex.
  Decision Q3 of `ROADMAP.md` drops the rest of the item (a bridge to a running master, placement,
  files, outputs, result and cancel), so no further work is planned on it.
- 2026-11-08: FEAT-1 is Partial. Of the work items in design 0046, the wire layer (`F1-b wire`,
  pull request 141) is merged: protocol version 4, `WorkerMessage::ResumeSession`, and the
  `SESSION_RESUME` bit, which is not advertised yet. The task store (`crates/master/src/store.rs`,
  pull request 129) is merged but not used by the master yet. `F1-a` (replay at start), the
  master's adoption of resumed tasks and `F1-c` are Planned. The review on pull request 129 lists
  four store findings that `F1-a` should settle before the store goes live: a lost unsynced frame
  before a surviving one stops `open`; any compaction error blocks every later write; a state
  directory open to others is tightened instead of refused, and symbolic links in it are followed;
  a missing journal next to a later-generation snapshot is recreated without a warning.
- 2026-11-08: FEAT-3 is Partial: pull request 133 built `FEAT-3a` of design 0033, the core and
  macOS (`--isolation off|fs|fs+net`, the `iso:` tag, `require:iso=`, `doctor` reporting and a
  `sandbox-exec` profile). On macOS a confined task cannot read the home, the per-user temp and
  cache directories, the cluster secret file or the config file in use, and under `fs+net` it has
  no network. `FEAT-3b` (Linux) and `FEAT-3c` (Windows) are Planned, so on those systems a task can
  still read `cluster.secret`.
- 2026-11-08: Added CACHE-1, REAPI-1, JOIN-1, LINK-1 and BENCH-1 as Planned, from the research
  recorded in [ROADMAP.md](ROADMAP.md) (decision Q7). Their evidence was read at `master` `fec164e`.
- 2026-11-08: REL-4 implemented in pull request 155: `.github/workflows/release.yml` builds archives for Linux, macOS (arm64, x86_64) and Windows, with and without `gpu-wgpu`, and `SHA256SUMS`; a `v*` tag creates the release, a pull request only uploads artifacts. No tag exists yet: the owner cuts the first one. Crates have `publish = false`, `authors` match the licence and `README.md` has an Install section.
- 2026-11-09: LINK-1 is Partial: steps 1 and 2 are in pull request 159. `doctor` has a `LINK` column, check C7 (warning for a relayed worker) and check C8 (an 8 MiB blob to each worker, MB/s, `--skip-throughput`). Steps 3 (relay URL option) and 4 (guide) are Planned; they touch `server.rs` and the worker client.
- 2026-11-09: BENCH-1 is Partial: (a) and (b) are in pull request 162 with `benchmarks/grid/bench_grid.py` and a dated record from one Mac (loaded, noted in the record). (c), the two-machine `cargo test` offload, waits for WS-7.
- 2026-11-09: FEAT-1 is Implemented: pull requests 129 (the store), 141 (wire layer), 147 (`F1-a`:
  `--state-dir` and replay at start), 158 (`F1-b` master adoption and `F1-c` worker session resume)
  and 167 (follow-ups: BLAKE3 `result_ack_token`, a session-keyed connected tracker, a SIGKILL test
  with the CLI binary), design 0046 (pull request 122, status Implemented). The "Done when" test is
  `tests/master_restart_recovery.rs`. The four store findings that the 2026-11-08 FEAT-1 entry lists were not
  re-checked for this change.
- 2026-11-09: FEAT-3 stays Partial: `FEAT-3b` (Linux: Landlock, seccomp and namespaces, design 0033)
  is merged in pull request 150 next to macOS (pull request 133). `FEAT-3c` (Windows) is open as
  pull request 152, so on Windows a task can still read `cluster.secret`.
- 2026-11-09: FEAT-4 is Partial. Merged from design 0039: W1 (pull request 157: `Task::workspace`,
  `Hello` probe, protocol version 6), W4 (156: source selection with `.gitignore` and limits), W2a
  (164: the worker's workspace store, not wired into the runner yet), W3 (166: required feature,
  affinity, one task per workspace), W9 (160: parallel blob uploads) and W8 (165: tool probes,
  `require:tool=`). Open: W2b (runner and client wiring, `--workspaces`), W5 (`submit --workspace`
  and the MCP field, pull request 168, open), W6 (end-to-end test), W7 (P2P for clients) and W10
  (guide and live record). No workspace task can run end to end yet.
- 2026-11-09: FEAT-10 is Implemented: pull request 165. A `RustCompilation` task with a
  `Cargo.toml` runs `cargo build --offline` and returns the binary; `submit --type compile --source`
  and the MCP `type: "compile"` send the crate; workers advertise `tool:cargo`, `tool:rustc`,
  `tool:git` and a task can name `require:tool=`. The runner test
  `test_rust_compilation_with_cargo_toml_runs_cargo_build_offline` builds a crate with a local path
  dependency and checks the binary in the outputs. No run on a second machine is recorded.
- 2026-11-09: JOIN-1 stays Partial and is now merged: pull request 163 adds `oxideswarm secret
  join-token` and `oxideswarm join` (design 0048) with `tests/cli_join_e2e.rs`. Step 4, the QR code
  for the Android app, waits for AND-1.
- 2026-11-09: SEC-9 (pull request 154, design 0047) and TEST-19 (pull request 161) were already
  Implemented in the Index; this log records them. REL-4, LINK-1 and BENCH-1 rows no longer say
  "(open)" because their pull requests 155, 159 and 162 are merged.
- 2026-11-09: LINK-1 steps 3 and 4 are done on branch `claude/link-1-relay`: `--relay-url <URL>`,
  `RUSTY_GRID_RELAY_URL`, and `relay_url` in `[master]`, `[worker]`, and `[client]` / `[submit]` /
  `[status]` select `iroh::RelayMode::Custom` on `presets::Minimal` with `https://` and credential
  validation ([design 0049](design/0049-custom-relay.md)), and
  [docs/guides/self-hosted-relay.md](guides/self-hosted-relay.md) documents running `iroh-relay`.
  LINK-1 stays Partial until a live run through a self-hosted relay is recorded by the owner.
- 2026-11-09: Phases 7 and 8 added with SCALE-1 to SCALE-6 and ECO-1 to ECO-8, all Planned, from
  Phases F and G of [ROADMAP.md](ROADMAP.md) and [design 0050](design/0050-scale-out-architecture.md).
  ECO-2 is CACHE-1 under its Phase G name, ECO-1 extends REAPI-1 with the `Execution` service, and
  SCALE-4 supersedes the "Design only" note of FEAT-6, which stays Implemented as the refusal. The
  Index counts changed from 12 to 26 Planned.
- 2026-11-09: FEAT-3 is Implemented: Windows isolation merged in pull request 152, and its "Done when"
  passes in CI on macOS, Linux and Windows. `cargo` inside the Windows AppContainer is split off as
  FEAT-3d (Planned). FEAT-4 is Implemented: on 2026-11-09 a coding-agent call over MCP on a Mac ran
  `cargo test -p rusty_grid_core --lib` on a Windows worker at a commit that machine did not have,
  and the second run reused `target/` (6.6 s against 207.4 s; record in
  [guides/offload-cargo-test.md](guides/offload-cargo-test.md#recorded-two-machine-run)). BENCH-1
  gained the dated (c) record; it stays Partial until the script is run on Linux and Windows.
  JOIN-1 was checked live on Windows (`join` exit 0, 8 doctor checks passed) and stays Partial for
  the QR code step. The Index counts are now 61 Implemented, 4 Partial and 27 Planned.
- 2026-11-09: Phase 6b added after decision Q13 (ecosystem model A): ENROLL-1 (design 0051),
  JOIN-2 (design 0052) and USB-1 (design 0053), all Planned. The Index counts are now 61
  Implemented, 4 Partial and 30 Planned.
- 2026-11-09: USB-1 is Partial. Kit format v0 is in `packaging/usb/`: `make_kit.sh`, and installers
  and uninstallers for macOS, Windows and Linux that carry a join token. It was tested from a stick
  on macOS and on Windows 11 against a demo cluster. `usb create`, with an invite encrypted by a
  passphrase, still waits for ENROLL-1b. The Index counts are now 61 Implemented, 5 Partial and
  29 Planned.
- 2026-11-09: Added and implemented TEST-20 (pull request 181): fixed the Windows
  `m4_challenger_2_stress` failover timeouts by checking `cancel_rx` before sandbox creation and
  before process spawn, raising `MIN_LIVE_KILL_AGE` in `process_tree.rs` from 80 ms to 250 ms (and
  honouring it in `TreeInner::drop`), and setting `.with_ram_mb(4096)` in
  `WorkerClient::for_testing` and `spawn_test_worker`. `m4_challenger_2_stress` passed 20/20
  locally inside `Global\OxideSwarmFullSuite`. The Index counts are now 62 Implemented, 5 Partial
  and 29 Planned.
- 2026-11-10: BENCH-1 is Implemented. Its "Done when" passes:
  - `benchmarks/grid/bench_grid.py` ran on macOS (pull request 162), Windows (187) and Linux (189,
    arm64 in a Docker Desktop VM, labelled as such);
  - (c) is recorded on two machines (pull request 175 and the live run of 2026-11-09);
  - `README.md` links the page.

  The Index counts are now 63 Implemented, 4 Partial and 29 Planned.
- 2026-11-10: Phase 9 added with NET-1 to NET-8, all Planned, from Phase H of the roadmap and design
  0054 (Draft): the owner's long-term goal of an open compute network. The Index counts are now
  63 Implemented, 4 Partial and 37 Planned.
- 2026-11-10: Implemented FEAT-3d (pull request 183, open): enabled `cargo build --offline` and
  `--workspaces` under `--isolation fs|fs+net` on non-elevated Windows workers without Object
  Manager DACL edits (`get_or_init_worker_lua_token` using `CreateRestrictedToken` with `LUA_TOKEN`
  + `CreateProcessAsUserW`, per-attempt AppContainer SID `oxideswarm.task.<worker8>.<run_token>` on
  `<ws>/tree` and `<ws>/tmp`, default `%TEMP%\rusty_grid\sandboxes` accepted under `host.user_home`
  in `validate_sandbox_base_safe`, and a startup `WARN` + `tool:cargo` tag removal when a Windows
  worker runs elevated with isolation). Verified in CI run `42678349017` across all 22 jobs and
  locally on Windows 11. The Index counts are now 64 Implemented, 4 Partial and 36 Planned.
- 2026-11-10: Added and implemented TEST-22 (pull request 200, open): updated `contains_standalone_number` in
  `tests/workflow_modes_test.rs` so UUIDv4 hex groups (`-4090-`, `4090a...`) and JSON/unit-labelled
  numeric measurements (`: 4090`, `4090 ms`, `4090 MB`) in `research_report.json` are not mistaken
  for an invented RTX 4090 GPU model name. The Index counts are now 65 Implemented, 4 Partial and 36 Planned.
- 2026-11-10: Added and implemented TEST-21 (pull request 198, open): fixed the Windows CI flakes in
  `tests/m6_challenger_1_stress.rs` and `crates/agent_mesh/tests/test_sec6_mesh_sandbox.rs` by
  delegating Windows task process termination on drop to `ProcessTree` (`#[cfg(not(windows))]
  cmd.kill_on_drop(true)` in `runner.rs` with leader-PID fallback in `ProcessTree::drop`) so
  `tokio::process::Child::drop` does not call `TerminateProcess` synchronously ahead of
  `MIN_LIVE_KILL_AGE`, keeping `shutdown_tx` alive across `ChallengerTestCluster::abort_worker`,
  aborting the actual assigned worker (`info.assigned_worker_id`) with `SINGLE_TASK_TIMEOUT` in
  `test_adversarial_unexpected_tcp_eof_during_active_task_execution`, summing numbers in pure POSIX
  shell before `sleep` in `test_adversarial_mapreduce_worker_drop_during_reduce_phase`, and retrying
  transient socket buffer/port exhaustion (`WSAENOBUFS` `10055`, `WSAEADDRINUSE` `10048`) in
  `AgentMeshClient::connect` and `AgentMeshHub::start`. `m6_challenger_1_stress` passed 20/20
  locally inside `Global\OxideSwarmFullSuite`. The Index counts are now 66 Implemented, 4 Partial and 36 Planned.
- 2026-11-10: Added and implemented DOC-8 (pull request 196, open): updated `CLAUDE.md` and
  `docs/SECURITY.md` to match `crates/worker/src/isolation.rs` after pull request 152 (`FEAT-3c`),
  recording that `--isolation fs|fs+net` confines tasks on macOS (`sandbox-exec`), Linux (Landlock,
  seccomp, namespaces) and Windows (per-task AppContainer + NTFS DACLs + private Desktop), that
  `cargo` under the Windows AppContainer is `Planned` as `FEAT-3d`, and that a Windows worker
  refuses `--workspaces` with `--isolation fs|fs+net`. The Index counts are now 67 Implemented, 4 Partial and 36 Planned.
- 2026-11-10: Added and implemented LOG-1 (pull request 194, open): client P2P endpoints in
  `crates/core/src/transport.rs` and `crates/cli/src/blobs.rs` now close gracefully before process
  exit (with `drain_pending_p2p_client_closes` awaited before `run_doctor`, `McpState::close` and
  `run_cli_with` return), and `run_cli_with` routes all tracing of client commands (`submit`,
  `status`, `result`, `cancel`, `workers`, `workspaces`, `mapreduce`, `doctor`, `join`, `secret`,
  `mcp`) to `stderr` so `iroh` `INFO` lines never land on `stdout` ahead of `--json` output. The
  Index counts are now 68 Implemented, 4 Partial and 36 Planned.
- 2026-11-10: Added and implemented USB-2 and BUG-18 ([design 0055](design/0055-usb-kit-windows-isolation.md), open):
  `windows::acl_grant_manifest_path` and `windows/revoke-isolation-acls.ps1` now honour
  `OXIDESWARM_HOME` (`RUSTY_GRID_ACL_GRANT_LOG > <OXIDESWARM_HOME>/windows_acl_grants.txt > ~/.oxideswarm/windows_acl_grants.txt`);
  `packaging/usb/make_kit.sh` accepts `--isolation off|fs` and ships `revoke-isolation-acls.ps1`;
  `packaging/usb/install.ps1` and `uninstall.ps1` declare `[CmdletBinding()]` and support
  `-Isolation off|fs` (default `off`) with ACL revocation in `uninstall.ps1` before removing
  `$InstallRoot`; and under `--isolation fs|fs+net` on Windows `TaskRunner` resolves bare commands
  against `%SystemRoot%\System32` before `Git\usr\bin` so `whoami` exits `0`. The Index counts are
  now 70 Implemented, 4 Partial and 36 Planned.
- 2026-11-10: USB-2 and BUG-18 merged in pull request 204 (`4a07fcb`). A live run on Windows 11
  as a normal user with a binary built from `4a07fcb` passed the Done when of USB-2: install with
  `-Isolation fs`, the trial guide's `cmd /c ver`, the PowerShell sum and bare `whoami` exit `0`,
  reading and listing `Documents` is denied, and `uninstall.ps1 -Yes` restores the sandbox base
  SDDL and leaves no `oxideswarm.toolchain` ACE. Release 0.1.1 lacks BUG-18, and `install.ps1`
  keeps `--workspaces` off under isolation for 0.1.0 and 0.1.1, so a kit needs 0.1.2 or later to
  use both. Added USB-3 (Planned). The Index counts are now 70 Implemented, 4 Partial and 37 Planned.
- 2026-11-10: Added SCHED-4 ([design 0060](design/0060-speed-aware-placement.md)) and USB-4 (both
  Planned) from a two-machine trial: a 32-task batch took 52 s because 6 tasks landed on a slow
  laptop, while the desktop alone would have taken about 12 s. The Index counts are now
  70 Implemented, 4 Partial and 39 Planned.
- 2026-11-10: Added LV-1, LV-5, LV-10 and LV-14 as Phase 10, from
  [design 0058](design/0058-lasting-value.md), at the owner's word. Nothing is built. The Index
  counts are now 70 Implemented, 4 Partial and 43 Planned.
- 2026-11-11: Added TEST-23 (Planned) at the owner's word.
  `test_master_restart_recovers_finished_queued_and_adopts_running_task_once` failed once in
  "workspace tests (windows-latest)" on pull request 206 at `e1fcf70` (run `42817398752`, job
  `129833857865`); that pull request changes only `docs/`. The same job passed on `master`
  `0354b28` and on the rerun. The cause is not known. The Index counts are now 70 Implemented,
  4 Partial and 44 Planned.
- 2026-11-11: Added MAINT-1 (Planned): the six largest source files hold 6,184 to 8,639 lines each,
  about 40 % of it tests, and `cli_app.rs` changed 101 times in a month. The Index counts are now
  70 Implemented, 4 Partial and 45 Planned.
- 2026-11-14: Added TEST-24, TEST-25 and TEST-26 (Planned) at the owner's request, from
  [design 0065](design/0065-large-transfer-and-long-run-tests.md): the end-to-end blob tests move
  at most about 5 MiB per file, and no record measures a cluster over hours. Nothing is built.
  The Index counts are now 70 Implemented, 4 Partial and 48 Planned.
- 2026-11-14: Implemented SCHED-4a and SCHED-4b ([design 0060](design/0060-speed-aware-placement.md), pull request 214):
  `SpeedBook` (`crates/master/src/speed.rs`, 4,096 keys, 64 workers per key, EMA weight 0.3, skipping checkpoint-resumed attempts) records
  successful task durations on terminal completion (`crates/master/src/server.rs`), drops entries of
  removed or pruned workers (`crates/master/src/registry.rs`), and drives D2 speed-aware placement (`--speed-hold-factor`, `RUSTY_GRID_SPEED_HOLD_FACTOR`, `[master].speed_hold_factor`)
  and `ScheduleSkipReason::WaitingForFasterWorker` hold-back in `crates/master/src/scheduler.rs`
  (`tests/sched4_speed_aware.rs`). SCHED-4c and SCHED-4d remain Planned. The Index counts are now
  70 Implemented, 5 Partial and 47 Planned.
- 2026-11-14: ENROLL-1 is Partial: `ENROLL-1a` merged in pull request 190 (per-machine X25519 keys,
  OXS-AUTH v3 Noise `IK` handshake and `Split()` record layer, `members.json` loaded at startup,
  `--master-key`, `master --auth secret|members|both`, `tests/enroll_v3_e2e.rs`), and `ENROLL-1b` is
  merged in pull request 201 (`oxs1-invite:` invites, `oxideswarm invite`, `oxideswarm members`
  including `release-worker`, `oxideswarm join` with invites and 6-digit pairing check confirmation,
  pending enrollment requests with use deducted at approval, auto-enrolment of the master's host
  `node.key` as `admin`, worker-id ownership per member, protocol version 8,
  `tests/cli_enroll_e2e.rs`). `ENROLL-1c` (live revocation across all lanes and task
  cancellation/requeue) and `ENROLL-1d` (dashboard members page and docs/guide) are Planned. The
  Index counts are now 70 Implemented, 6 Partial and 46 Planned.
- 2026-11-14: Synced BACKLOG with `master` `8596c5fd`: USB-4 is Implemented (pull requests 207 and
  210, `29ccd3e`: default worker slots sized to performance cores via Windows
  `GetSystemCpuSetInformation`, macOS `hw.perflevel0.logicalcpu`, and Linux
  `/sys/devices/cpu_core/cpus` with battery fallback, `--slots` / `-Slots` override preserved across
  `--update`, `tests/test_launcher_scripts.py` and live probe picking 4 slots on the Core i7-1355U
  laptop and 10 on the Core i5-14600KF desktop); MAINT-1 is Partial (pull requests 211 and 212,
  `681012d`: `crates/worker/src/client.rs` unit tests moved to `crates/worker/src/client/tests.rs`,
  8,252 to 4,363 lines); USB-1 lists merged pull requests 185, 195 and 207; and ENROLL-1b merged in
  pull request 201 (`8596c5fd`). The Index counts are now 71 Implemented, 7 Partial and 44 Planned.
- 2026-11-14: Implemented JOIN-2 ([design 0052](design/0052-short-code-pairing.md), pull request 221, `5d05932`): BIP-0039 CC0-1.0 2048-word list (`crates/cli/src/wordlist.rs`), UDP port 8087 broadcast discovery, SPAKE2 over Ed25519 with joiner-first confirmation and single-guess burn rule, and HKDF/ChaCha20-Poly1305 invite and `PairPendingNotice` handoff (`crates/cli/src/pair.rs`), `invite --code`, `join --code [--pair-host]`, `tests/cli_pair_e2e.rs` green on Linux, macOS and Windows (CI run `43590921426`), and a live LAN run between macOS arm64 and Windows 11 x86_64 recorded in [guides/add-a-machine.md](guides/add-a-machine.md#recorded-two-machine-run) (UDP 8087 broadcast with `422-tool-humor`, matching pairing check `711 627`, member `269b134d8914e3de`, `submit --wait -- echo paired-ok` exit `0` in `348 ms`). The Index counts are now 72 Implemented, 7 Partial and 43 Planned.
- 2026-11-14: Implemented FEAT-7 ([design 0066](design/0066-usable-rendezvous-discovery.md), pull request 224, `e7bb420`): standalone `oxideswarm rendezvous` HTTP registry on `:8090` with deterministic Ed25519 `lease_key` / `key_signature` multi-cluster slot isolation and `X-OxideSwarm-Lease-Auth` read authorization (`crates/master/src/rendezvous.rs`, `crates/core/src/discovery.rs`), MAC-covered `p2p_ticket` and P2P fallback when discovered TCP fails (`crates/worker/src/client.rs`, `crates/cli/src/connect.rs`), upfront `https://` rejection, `2 s` master registration retries (`crates/master/src/server.rs`), updated `README.md` / `CLAUDE.md` / `CHANGELOG.md`, and `tests/rendezvous_discovery_e2e.rs`. The Index counts are now 73 Implemented, 7 Partial and 42 Planned.
- 2026-11-14: Implemented ECO-4 ([design 0050](design/0050-scale-out-architecture.md), pull request 225, `a93f708`): added
  authenticated Prometheus `GET /metrics` (`AuthScope::Observer`, `crates/master/src/metrics.rs`,
  `web_ui.rs`, `auth.rs`, `queue.rs`, `store.rs`) and opt-in zero-SDK `--features otel` OTLP/HTTP
  JSON trace export (`crates/master/src/otel.rs`, `scheduler.rs`, `crates/cli/src/cli_app.rs`, `0`
  new external packages in `Cargo.lock`) emitting deterministic `task_id`-seeded `submit`,
  `place`, `assign`, `run`, and `result` spans without secret or task-input leakage
  (`tests/observability_metrics_otel_test.rs`). The Index counts are now 74 Implemented, 7 Partial
  and 41 Planned.
- 2026-11-14: Implemented USB-3 (Keep the Windows USB install folder apart from the default
  workspace root, pull request 226): `packaging/usb/install.ps1` defaults `-InstallRoot` to
  `%LOCALAPPDATA%\OxideSwarm-USB` (and `<drive>:\OxideSwarm-USB`), refuses fresh installs in
  `%LOCALAPPDATA%\OxideSwarm` or `%LOCALAPPDATA%\oxideswarm\workspaces` while continuing `-Update`
  and `uninstall.ps1` fallback for legacy `%LOCALAPPDATA%\OxideSwarm` installs, always passes
  `--workspace-root <folder>\workspaces` (or `<folder>\usb-kit\workspaces` on legacy updates)
  marked with `.oxideswarm-usb-workspaces`, and `packaging/usb/uninstall.ps1` refuses to delete a
  folder containing an unmarked or foreign-used `workspaces` directory while naming the worker using
  it (`tests/test_launcher_scripts.py` + isolated live Windows cluster check). The Index counts are
  now 75 Implemented, 7 Partial and 40 Planned.
- 2026-11-14: `USB-1a` and `USB-1b` implemented (`win2`, pull request 228): added `oxideswarm usb create`
  (`crates/cli/src/usb_kit.rs`), `oxideswarm join --invite-file` (`crates/cli/src/join.rs`),
  `invite.oxs` + `kit.json`, v1 `packaging/usb/` installers and uninstallers, `tests/cli_usb_kit_e2e.rs`,
  Windows/POSIX v1 launcher tests in `tests/test_launcher_scripts.py`, and docs updates across
  [design 0053](design/0053-usb-setup-kit.md), [guides/add-a-machine.md](guides/add-a-machine.md),
  and [README.md](../README.md). `USB-1` stays `Partial` until the cross-machine live stick test is
  recorded; the Index counts remain 75 Implemented, 7 Partial and 40 Planned.
- 2026-11-14: Completed design 0039 W11 under FEAT-4 (pull request 186, open): symbolic links and empty
  directories in `TreeManifest` (`oxideswarm-tree/2`, `ProtocolCapabilities::TREE_V2`), with
  case- and Unicode-folded `FoldedSymlinkTrie` / `FoldedPathTrie` validation, manifest complexity
  budgets, segment-by-segment on-disk link target checks in `WorkspaceGuard::sync`, and fail-closed
  `requires_tree_v2` gating on `TaskQueue`. The Index counts stay 75 Implemented, 7 Partial and 40 Planned.
- 2026-11-15: Implemented SCALE-1 ([design 0050](design/0050-scale-out-architecture.md), pull request 222): simulated workers (`WorkerClient::simulated`, `FakeRunnerConfig`, `SimulatedLinkStream` in `crates/worker/src/sim.rs`, `crates/worker/tests/scale1_simulated_worker.rs`), 7-phase fleet driver (`crates/cli/examples/scale_load_driver.rs`), `benchmarks/grid/scale_harness.py` (`tests/test_scale_harness.py`), and committed 3-run baseline records at 100, 500 and 1000 simulated workers on loopback plus 100 workers under a `25 ms + 10 ms`, `1 %`-loss simulated link (`benchmarks/grid/results/2026-11-14-scale1-windows-amd64-*.json`, linked from `README.md` and `benchmarks/grid/README.md`). The Index counts are now 76 Implemented, 7 Partial and 39 Planned.
- 2026-11-15: Implemented SCALE-2 ([design 0050](design/0050-scale-out-architecture.md), pull request 227): added `WorkerIndex` / `RegisteredIndex` capability and attribute bitsets (`WORKSPACES`, `TREE_V2`, free slots, mobile buckets), `SCAN_PAGE = 512`, `DEFAULT_MASTER_MAX_BATCH_SIZE = 1024` (`--max-batch-size` / `RUSTY_GRID_MAX_BATCH_SIZE` / `[master].max_batch_size`), adaptive micro-batch debounce and 16-batch drain, per-submitter `FairShareQueues` weighted round-robin, dashboard `receiver_count() > 0` guards, single-flight batched `ColdResultStore` (`OXSWCLD1`, `DEFAULT_MAX_HOT_TERMINAL_TASKS = 1024`) with uncompacted replay reuse, and committed 3-run post-`SCALE-2` records (`benchmarks/grid/results/2026-11-15-scale2-windows-amd64-{500w,1000w}.json`, `500w` `409.1` vs `375.2 tasks/s` and `16.69` vs `31.01 ms` unloaded assign p50, `1000w` `414.4 tasks/s` and `91.8 MiB` steady RSS, `0` failed tasks). The Index counts are now 77 Implemented, 7 Partial and 38 Planned.
- 2026-11-15: Implemented FEAT-9a / Q56 ([design 0068](design/0068-task-vram-accounting.md), pull request 232): added optional `TaskRequirements.vram_mb` (`--vram-mb` / `--vram`, MCP `vram_mb`, web `vram_mb`) with `effective_vram_mb()`, `requires_gpu_or_vram()`, and `WorkerCapabilities::satisfies()` VRAM checks, bumped wire `PROTOCOL_VERSION_CURRENT` to `9` (`LegacyPreV9TaskRequirements`, `LegacyV6ToV8Task`, and `LegacyV6ToV8BinaryClientMessage` version-error decoders) and `STORE_FORMAT_VERSION` to `3`, updated `statically_fits_caps()`, `TaskQueue::assigned_loads`, `WorkloadScheduler` VRAM packing, VRAM `hold_back`, `--fail-unfulfillable-tasks` diagnostics, and `gpu_demand` preservation; `FEAT-9b` (runtime `nvidia-smi` / `rocm-smi` / Metal VRAM polling) remains open. The Index counts are now 77 Implemented, 8 Partial and 37 Planned.
- 2026-11-15: Audit of `c2462e3` (read-only, with clippy, fmt and `cargo test --workspace --lib` run on macOS): added BUG-19, BUG-20, SEC-10 to SEC-15, AND-5, DOC-9 and DOC-10 (Phase 11), decisions D8 to D10, and a size note on MAINT-1. Statuses of existing items are unchanged; DOC-9 lists the ones to correct. The Index counts are now 77 Implemented, 8 Partial and 48 Planned (FEAT-9 became Partial with FEAT-9a, pull request 232).
- 2026-11-15: DOC-9 implemented (pull request 234): statuses and statements corrected across `docs/BACKLOG.md`, `docs/ROADMAP.md`, `docs/README.md`, `CLAUDE.md`, `PROJECT.md`, `README.md`, designs 0031 and 0033, and two help texts in `crates/cli`. The Index counts are now 78 Implemented, 8 Partial and 47 Planned.
- 2026-11-15: DOC-10 implemented (pull request 235): `docs/ROADMAP.md` is the only roadmap and describes `f4a4a74`, `PROJECT.md` section 5 links to it, and the untracked `docs/ROADMAP_AND_VISION.md` left the repository. The Index counts are now 79 Implemented, 8 Partial and 46 Planned.
- 2026-11-16: Phase 11 brought up to date before merge: BUG-19 (pull request 236, design 0069), BUG-20 (pull request 241, design 0075) and SEC-14 (pull request 237, design 0071) were merged before this phase reached `master` and are Implemented; SEC-10, SEC-13, AND-5 and MAINT-1 name their open pull requests (238, 239, 240, 230, 231, 242); "Where the project stands" in `docs/ROADMAP.md` describes `96ce571`. The Index counts are now 82 Implemented, 8 Partial and 43 Planned.
- 2026-11-16: TEST-27 added from a macOS CI failure of `mesh_auth` on `master` `6a88eff`, and a note on TEST-23 from the result of its failed run (the process exited 0 and printed nothing). SEC-13 is Implemented (pull request 239, [design 0072](design/0072-collect-outputs-without-following-links.md)) and SEC-10 is Implemented (pull request 238, [design 0070](design/0070-no-working-directory-config.md)), both merged 2026-11-16. MAINT-1 stays Partial after the splits of `client.rs`, `runner.rs` and `protocol.rs` (pull requests 230, 231, 242). The Index counts are now 84 Implemented, 8 Partial and 42 Planned.
- 2026-11-16: AND-5 Implemented (pull request 240, [design 0073](design/0073-android-bridge-nonblocking-stop.md)). MAINT-1 stays Partial after the splits of `queue.rs` and `server.rs` (pull requests 249 and 250): five of six files are split, `cli_app.rs` and the CI size check remain. The Index counts are now 85 Implemented, 8 Partial and 41 Planned.
- 2026-11-16: BUG-21 is Implemented ([design 0077](design/0077-windows-relative-task-executables.md)): `resolve_windows_executable_in_dir`, `resolve_windows_executable_for_isolation_in_dir`, `resolve_relative_windows_program_in_dir`, `windows::resolve_program`, and `spawn_with_security_capabilities` now resolve `./`, `.\`, `sub/`, and `sub\` against the task working directory inside the sandbox across all three Windows isolation modes, refuse sandbox escapes, wrap `.bat`/`.cmd` via `cmd.exe /d /s /c`, and pass an absolute `lpApplicationName` (`crates/core/src/capabilities.rs`, `crates/worker/src/{isolation,runner/supervise}.rs`, `tests/cli_isolation_e2e.rs`). The Index counts are now 86 Implemented, 8 Partial and 41 Planned.
- 2026-11-16: MAINT-1 stays Partial after the split of `cli_app.rs` (pull request 253): all six files are split, and the CI size check of step 3 remains. The BUG-21 row names pull request 251. The Index counts are unchanged: 86 Implemented, 8 Partial and 41 Planned.
- 2026-11-16: MAINT-1 Implemented (pull request 259, [design 0080](design/0080-source-file-size-check.md)): `ox-mode test size`, `ox-mode test lint` and the Linux `clippy` job fail when a Rust file's production part exceeds 3,000 lines, with `isolation.rs` (5,377), `mcp.rs` (3,692) and `mode_cli.rs` (3,242) on an allowlist that may only shrink. The Index counts are not recounted here.
- 2026-11-16: SEC-11, SEC-12, SEC-15 and MAINT-1 are Implemented (pull requests 256, 257, 258 and 259). The Index counts, recounted from the status column, are 90 Implemented, 7 Partial and 38 Planned (135 items).
- 2026-11-16: Phase 12 (0.4.0: upgrade without stopping the cluster, and stability) added at the owner's go-ahead in chat: PROTO-1 (Implemented by the pull request that adds this line), PROTO-2 to PROTO-4, TEST-28 to TEST-32, and SEC-16, BUG-22 and CLI-2 from the Windows smoke test of 0.3.0 (9 of 9 at `3ecc926`; macOS 10 of 10 at `9acc06e`). The Index counts are now 91 Implemented, 7 Partial and 49 Planned (the recount of the table before this phase gave 90, 7 and 38).
- 2026-11-16: TEST-28 and TEST-30 Implemented (pull requests 265 and 263); TEST-27 and TEST-31 Partial (fixes in, the CI loop of each "Done when" not run); TEST-33 to TEST-35 added from flaky jobs seen while merging 260 to 265. The Index counts are now 93 Implemented, 9 Partial and 48 Planned.
- 2026-11-17: PROTO-3 Implemented by draft pull request 272 ([design 0082](design/0082-task-extension-slot.md)): protocol 10, store format 4, the extension slot and the `ext:` worker tag; Q68 to Q72 decided by the owner. The pull request holds until PROTO-4 (design 0076 D1); PROTO-3d waits for PROTO-4.
- 2026-11-17: PROTO-4 Implemented by draft pull request 274 ([design 0083](design/0083-one-version-window.md)): the one-version window (protocol 10 accepts 9), Q73 to Q79 decided by the owner; PROTO-3d built with it. The pull request merges together with PROTO-3 (pull request 272).
- 2026-11-17: CLI-2 Implemented (pull request 275): `master --discovery-port`; design 0084. The Index counts are now 99 Implemented, 11 Partial and 43 Planned (153 items, recounted when pull request 277 merged master).
- 2026-11-17: LV-1 Partial (pull request 270): `fuzz/` with three targets, seeds, regression inputs, a one-hour run of each (no finding) and a `fuzz` CI job that has not run yet. The Index counts are unchanged.
- 2026-11-17: TEST-34 Implemented and TEST-35 Partial (pull request 277): Suite 2 waits for the expected number of payloads instead of a fixed sleep; the `switch_role` launcher test prints its output and logs on failure (cause not reproduced). TEST-36 to TEST-38 Implemented (pull request 276). The Index counts are 99 Implemented, 11 Partial and 43 Planned (153 items).
- 2026-11-17: TEST-39 and TEST-40 added and Implemented (pull request 278): two Agent Mesh tests wait for registration and for each payload instead of a fixed sleep; the Index gains two Implemented items. BUG-22 waits for the next wire change after 0.4.0.
- 2026-11-17: Release `0.4.0` prepared (protocol 10 with the one-version window, PROTO-3 and PROTO-4): versions, CHANGELOG, README install lines and DEPLOYMENT section 3.5; the size allowlist of `crates/worker/src/isolation.rs` lowered to 5346. BUG-23 added (Planned), and BUG-24 from the Windows smoke test of 0.4.0 (Planned). The Index counts are 103 Implemented, 11 Partial and 44 Planned (158 items).
- 2026-11-17: TEST-41 to TEST-46 added (Planned) for failing tests without an item: the probe that ran a task
  twice in pull request 282, the two tests ROADMAP.md "Stability" listed, the speed-aware batch test that
  failed in pull request 282, and two stress tests agent-win saw fail under CPU starvation. The Index counts
  are 103 Implemented, 11 Partial and 50 Planned (164 items).
- 2026-11-17: Pull requests 282 (BUG-23), 283 (TEST-29 Partial), 284 (BUG-24) and 286 (TEST-33 Partial) merged;
  TEST-47 added (Planned) for the handle-count test that failed in pull request 286. The Index counts are
  105 Implemented, 13 Partial and 47 Planned (165 items).
- 2026-11-17: Release `0.4.1` prepared (no wire change; BUG-23, BUG-24, TEST-29 diagnostics, TEST-33 and
  TEST-41): versions, CHANGELOG with an "Upgrading from 0.4.0" note, README install lines, DEPLOYMENT section
  3.5 and the ROADMAP release table. The Index counts are unchanged.
- 2026-11-17: Phase 13 added from [design 0088](design/0088-evaluation-of-2026-11-17-and-what-finished-means.md),
  after the owner accepted Q82: CI-1, MAINT-2, DOC-11, ADOPT-1 to ADOPT-4, AX-1 to AX-3, LV-4 and LV-15, all
  Planned. CI-1 records what the job annotations say about 2026-11-16: twelve runs for pushes on `master` were
  cancelled by the owner's account, and six runs hold macOS jobs that no hosted runner acquired. Counted from
  the rows, the Index holds 178 items: 106 Implemented, 13 Partial and 59 Planned.
- 2026-11-17: Phase 14 (public launch) added with LAUNCH-1 to LAUNCH-5, the launch steps of ROADMAP.md as items with
  their gate (Q80, TEST-24, TEST-25, a two-network run, the first round of ADOPT-1); TEST-48 added for the
  Android bridge test that failed in pull request 289. Counted from the rows, the Index holds 184 items:
  106 Implemented, 13 Partial and 65 Planned.
- 2026-11-17: Phase 15 (coordination between agents) added from design 0087 after the owner decided Q84 to
  Q86, and done the same day: COORD-1 and COORD-2 in pull request 295, COORD-3 in pull request 294. Counted
  from the rows, the Index holds 187 items: 109 Implemented, 13 Partial and 65 Planned.
- 2026-11-17: TEST-49 added (claimed on the hub) and Phase 16 added from design 0089 (Anima Engine as the
  first workload; Q88 and Q89 decided by the owner's delegation): APP-4, VERIFY-1 (pull request 296) and APP-5.
  Counted from the rows, the Index holds 191 items: 109 Implemented, 13 Partial and 69 Planned.
- 2026-11-17: VERIFY-1 Implemented (pull request 296). The Index counts are 110 Implemented, 13 Partial and 68 Planned (191 items).
- 2026-11-18: Phase 17 added from design 0093 (benchmarks worth showing; ids claimed on the hub):
  BENCH-2 to BENCH-7, all Planned; LAUNCH-4 now also needs BENCH-7. Q90 to Q92 open.
- 2026-11-18: Recounted after pull requests 299 to 318 (307, 308 and 309 merged through 309, 310 to 315
  through the integration pull request 316, and 317 with TEST-51 (pull request 318)): TEST-32 corrected to Implemented (pull request 267), VERIFY-2 and
  BENCH-2 Implemented, APP-4 Partial (the Windows half). Counted from the rows, the Index holds 201 items: 126
  Implemented, 15 Partial and 60 Planned.
- 2026-11-18: Release `0.4.2` prepared (no wire change; VERIFY-1, VERIFY-2, BUG-25, BUG-26, the mesh hub
  fix of TEST-27, TEST-23 partial, TEST-42 to TEST-52): versions, CHANGELOG with an "Upgrading from 0.4.1"
  note, README install lines, DEPLOYMENT section 3.5 and the ROADMAP release table. The Index counts are
  unchanged.
- 2026-11-18: DOC-11: Implemented items' text moved to BACKLOG-ARCHIVE.md; Index column Line added (In/Out per design 0088).
- 2026-11-18: The owner placed the 16 open items that design 0088 does not name outside the stop line ("Out"):
  JOIN-1, SCHED-4, LV-10, PROTO-5, LAUNCH-1 to LAUNCH-5, APP-4, APP-5 and BENCH-3 to BENCH-7. They keep their
  priority. PROTO-1's pull request cell corrected to 262. Counted from the rows: 202 items, 127 Implemented,
  16 Partial and 59 Planned; 30 open items In, 45 Out.
- 2026-11-18: LV-1 Implemented (its fuzz CI job is green on `master`), its text moved to the archive;
  LV-18 Partial (pull request 326, the MCP reader is open); TEST-53 Implemented (pull request 327).
- 2026-11-18: LV-18 Implemented (pull request 335: the target `mcp_request_line` for the JSON-RPC reader of
  `oxideswarm mcp`), its text moved to the archive.
- 2026-11-18: Release `0.4.3` prepared (no wire change; ENROLL-1c revocation across every lane, LV-18, LV-4
  comments, BUG-27, BUG-28, TEST-35, TEST-53, TEST-54): versions, CHANGELOG with an "Upgrading from 0.4.2"
  note, README install lines, DEPLOYMENT section 3.5 and the ROADMAP release table.
- 2026-11-18: BENCH-3 Partial (pull request 349): W1 on the Mac and the Windows case machine, speedup 1.37,
  efficiency 0.73, 360/360 checksums; BENCH-8 added (Planned, Out) to find the gap to the plan's 0.85.
- 2026-11-18: BENCH-8 Implemented (pull request 355): combining the two machines costs 0.01 to 0.06; the
  tail and the missing queue are below 0.05; the gap is the cost of running through the grid on each machine
  (about 0.78 of each machine alone): on Windows the master on another machine over the LAN (0.16), on the Mac
  the default task priority (0.05) and 0.16 still open. BENCH-9 added (Planned, Out); Q93 open.
- 2026-11-18: BUG-29, BUG-30 and BUG-31 added and Implemented (pull request 341): the USB kit's folder
  menu, the removal of installs from older kits, and failed-start reporting with a kept log, found on a Windows 11
  laptop with kit 0.4.3; the Windows sandbox base decision is in the design 0053 status log. Texts in the archive.
- 2026-11-19: BUG-32 added and Implemented (pull request 352): `windows::validate_workspace_root_safe` statically allowlists `<user_home>\AppData\Local\OxideSwarm-USB\workspaces` and `<user_home>\AppData\Local\OxideSwarm\usb-kit\workspaces` and accepts regular non-reparse `.oxideswarm-usb-workspaces` files ([design 0033](design/0033-os-level-task-isolation.md)), and `packaging/usb/install.ps1` stops before creating `$InstallRoot` when `-Isolation fs` with workspaces targets any other path under `%USERPROFILE%`.
- 2026-11-18: ADOPT-2 Implemented (pull request 350): D5 run summary in `submit --wait`, `--json`, and MCP `submit` / `task_result`, with `waited_ms: null` when the MCP submission timestamp is unknown.
- 2026-11-18: LV-15 Partial (pull request 351): guide and accounts skeleton; step 2 holders, steps 3 and 4 stay with the owner.
- 2026-11-19: TEST-55 added and Implemented (pull request 356): `s1_write_grants_are_refused_and_read_only_grants_warn_once_with_working_remedy`
  in `crates/worker/src/sandbox.rs` uses `BUILTIN\Guests` (`S-1-5-32-546`) instead of
  `BUILTIN\Remote Desktop Users` (`S-1-5-32-555`, absent on Windows Home), so `cargo test -p rusty_grid_worker --lib`
  passes in full on Windows 11 Home; text in the archive.
- 2026-11-18: BENCH-9 Partial (pull request 354): `bench_sweep.py` record format 2 adds task process timing, slot utilization, `task_time_missing`, succeeded per-worker means/medians, and `cargo`/`rustc` process sampling (step 1); live runs of steps 2 and 3 remain.
- 2026-11-19: Rows brought up to date after pull requests 340 to 359 (354 still open): "(open)" dropped for
  LV-4, LV-5, BUG-29 to BUG-32, BENCH-3, LV-15, BENCH-8, ADOPT-2, TEST-23, TEST-41 and TEST-55, and ADOPT-2's Line
  cleared. MAINT-2 Implemented (pull requests 323, 325 and 331; the allowlist has no entry). TEST-29 Implemented:
  no `workspace tests (windows-latest)` job of 2026-11-18 lost its runner (82 passed); the speed work is TEST-56.
  The texts of TEST-29, BUG-32, MAINT-2, ADOPT-2 and BENCH-8 moved to the archive. SEC-17 added from the review of
  352. SEC-16 Partial: its cause is on record on both Windows machines, and none of the foreign grants is
  OxideSwarm's. Counted from the rows: 215 items, 143 Implemented, 18 Partial and 54 Planned; 26 open items In,
  46 Out.
- 2026-11-19: TEST-56 added and Implemented (pull request 361, open): `workflow_modes_test` runs
  `run_watch_check` on the full repo once (`reload` alias tested on a minimal temp crate),
  `serialization_benchmark` and `serialization_benchmark_multi_size` run compact default loops and gate full
  benchmarks and `opt-level=0` timing checks behind `OXIDESWARM_RUN_BENCHMARKS=1`, the two blob e2e tests
  use `1,600` files under `#[cfg(windows)]`, and Windows CI Step 8 sets
  `RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8` so the 30 AppContainer tests print `skipped:` after Step 7
  (`workspace tests (windows-latest)` `1,416 s` / `23.60 min` vs `30.2 min` median baseline); text in the
  archive.
- 2026-11-19: BENCH-9 row and texts updated after pull request 354 merged (duplicate rows and count lines from the
  merge removed). SEC-18 added from the review of pull request 360. BUG-22: owner decision recorded (next planned
  wire change). Counted from the rows: 216 items, 143 Implemented, 19 Partial and 54 Planned; 27 open items In,
  46 Out.
- 2026-11-19: TEST-31 Implemented (text in the archive) and TEST-41 Partial (pull request 363, open): on Linux (`aarch64`, `Linux 7.0.14-linuxkit`, Docker Desktop container `rust:1.96-bookworm`, `rustc 1.96.1`, 10 cores) at `ccafa90`, TEST-31 passed `200/200` (`100` alone + `100` under `6` busy loops, `0` failures, `1,343.1 s`), and TEST-41 passed `200/200` runs / `2,400` rounds (`ran_twice=0` everywhere; `40` alone default + `40` alone `RV158_ZERO=1` + `60` under `6` busy loops default + `60` under `6` busy loops `RV158_ZERO=1`, `1,697.06 s`), meeting the 200-run part of "Done when" on Linux and Windows while closing without a named cause stays with the owner.
- 2026-11-19: TEST-57 and TEST-59 added and Implemented (pull request 365, open): `test_capability_queries_under_connect_disconnect_stress` (`tests/m2_challenger_2_stress.rs`) polls every `50 ms` up to `60 s` until `registrations >= 16` and each query count `> 50` instead of a fixed `1.8 s` sleep, and `test_tier2_bva_zero_duration_builtin_task_instant_completion` (`tests/e2e_cluster.rs`) checks task outcome, `0 ms` builtin stdout, and worker `execution_time_ms` while printing elapsed time instead of asserting `< 100 ms`; each passed `50/50` on Windows (`25` alone + `25` under 12 CPU busy loops); texts in the archive.
- 2026-11-19: SEC-17 Implemented (pull request 360, open; text in the archive): `validate_workspace_root_safe` and a second `validate_workspace_root_ancestors_safe` check under `PREPARE_LOCK` right before `protect_sandbox_base` refuse reparse points on `--workspace-root` and along any raw, lexical, or canonical ancestor when `--workspace-root` is under or resolves into `host.user_home` or `%USERPROFILE%` (including external junctions above or pointing into home, while roots outside home keep no ancestor check and redirected home / `AppData\Local` junctions document the `--workspace-root` target workaround), and `.oxideswarm-usb-workspaces` and `store.lock` read `nNumberOfLinks == 1` directly from `GetFileInformationByHandle` (so ReFS / Dev Drive / network volumes stay accepted); remaining path-based `SetNamedSecurityInfoW` TOCTOU window and extra entries move to SEC-18.
- 2026-11-19: TEST-52 Implemented (pull request 364, open, owner decision 2026-11-19): option (a) moves `ox-mode dev cluster` (`launch_local_dev_cluster` / `launch_local_dev_cluster_with_ports`) and `tests/workflow_modes_test.rs` onto port `0` with `--port-file` and `--dashboard-port-file` (20/20 under 6 CPU busy loops on 10 cores, `692.74 s`); options (b) and (c) are declined so `--discovery-port 0` still means "beacon off" ([design 0084](design/0084-discovery-beacon-port.md)) and the four `// Bind-and-drop kept (TEST-52)` discovery sites stay accepted; text in the archive.
- 2026-11-19: TEST-58 Implemented (pull request 366, open): `Sandbox::destroy`, `Drop for Sandbox`, and the sweep/retry lock-release paths in `crates/worker/src/sandbox.rs` now call `File::unlock()` (`LOCK_UN` on Unix) via `release_task_lock` before closing the descriptor, because a concurrent `fork()` (`process_tree.rs` `pre_exec` lifeline supervisor) inherits an open-file-description duplicate on which `close` alone leaves the `flock` held (`2/50` failures before the fix alongside concurrent `runner::*` tests; deterministic `test_keep_sandboxes_destroy_unlocks_even_with_inherited_lock_fd` fails before and passes after; `0/200` alone, `0/200` `sandbox::tests`, `0/200` concurrent with `runner::*`, and `0/20` full `rusty_grid_worker --lib`); text in the archive.
- 2026-11-19: Phase 19 added from design 0094 (Draft): ECO-9, NET-9, NET-10 and APP-6, Planned and Out; Q94 to
  Q97 open. The recorded join from another network stays in ENROLL-1.
- 2026-11-19: Q94 to Q97 decided by the owner at the recommended answers; design 0094 Accepted. ECO-9, NET-9,
  NET-10 and APP-6 stay Planned.
- 2026-11-19: Release 0.4.4 prepared (no wire change: protocol 10, store format 4): the Windows USB kit fixes BUG-29 to
  BUG-32, SEC-17, ADOPT-2, LV-5 build provenance, TEST-52 and test fixes; approved by the owner in chat.
- 2026-11-19: SEC-18 Implemented (pull request 372, open; text in the archive): `validate_workspace_root_entries_safe` and `validate_workspace_dir_entries_safe` refuse unexpected top-level files, reparse entries, non-directory `.locks`/workspace folders, and hard-linked `.locks/*` and `<ws>/{state.json,lock,state.json.tmp}` files; `open_verified_dacl_handle` and `set_protected_dacl_on_handle` verify `GetFileInformationByHandle` and `GetFinalPathNameByHandleW` against the lexical long path (`GetLongPathNameW`) before calling `SetSecurityInfo` on `FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT` handles; `check_chain_from_home` matches home only on `Normal` components (no bare `C:` CWD expansion); and tests prove each home walk independently, assert refusal texts, exercise `PREPARE_LOCK` / handle / metadata race hooks, and fail under `RUSTY_GRID_REQUIRE_APPCONTAINER=1` instead of skipping.
- 2026-11-19: TEST-24 Partial (step 1, pull request 374, open, [design 0065](design/0065-large-transfer-and-long-run-tests.md)): added `benchmarks/endurance/endurance.py` (`transfer`, `throttle`, `sample`), `benchmarks/endurance/README.md`, `tests/test_endurance_harness.py` (CI `agent-bridge`), and Windows `x86_64` loopback record (`T1` `1`/`3 GiB`, `T2` `20,000` files, `T3`, `T4`, `T6`, `T7` `2 Mbit/s` / `50 ms`, `T8`: `7/7` passed in `2,648.17 s`; `T5`, `T9`, `T1 8 GiB`, `T2 100k`, and two-machine links remain for step 2). Counted from the rows: 216 items, 146 Implemented, 18 Partial and 52 Planned; 24 open items In, 46 Out.
- 2026-11-19: BUG-33 Implemented (pull request 367, open): `crates/worker/src/process_tree.rs` (`close_fds_except`) now lists open descriptors on macOS via `proc_pidinfo(PROC_PIDLISTFDS)` into a 1,024-entry stack buffer (`close_open_fds_macos`, keeping `close_fds_range_fallback` on error or overflow), cutting per-task close time from `111.889 ms` wall / `111.095 ms` CPU to `0.688 ms` / `0.183 ms` at `1,048,576` and `7.683 ms` / `7.248 ms` to `0.678 ms` / `0.181 ms` at `65,536`, verified by `test_supervisor_closes_high_numbered_fd`, `rusty_grid_worker --lib`, and `test_sec5_hardened_runner` (with and without `--features 3d`); text in the archive.
- 2026-11-19: TEST-25 Partial (step 1, pull request 381, open, [design 0065](design/0065-large-transfer-and-long-run-tests.md)): added `soak` subcommand of D3 to `benchmarks/endurance/endurance.py`, unit tests in `tests/test_endurance_harness.py` (`20/20`), and 45-minute Windows `x86_64` loopback record `benchmarks/endurance/results/2026-11-19-soak-45m-loopback-windows-amd64-intel-core-i5-14600kf.json` at `4c11a45` (`6/6` checks passed over `2,704.22 s` across `2,185` submitted and unique master-logged tasks, `0` multi-result tasks, `0` submit failures, `4` retries/orphan requeues from `/api/tasks` + master `INFO` logs [debug-only stale/late counts unmeasured at `INFO`], `16/16` injected faults, master per-process RSS `16.0 -> 35.5 MiB` / `29.2 -> 40.3 MiB`, `journal.bin` compaction `1,525,821 -> 24 B` / `801,453 -> 24 B`, idle stores within `4 GiB` / `2 GiB` bounds and mid-run worker cache `<= 2,125.0 MiB`, fault-excluded clean probe p99 `130.85 -> 146.51 ms` [ratio `1.1197`], `0` leftover PIDs or task process trees; 24-hour 3-machine run remains for step 2, awaiting an owner date). Counted from the rows: 225 items, 151 Implemented, 19 Partial and 55 Planned; 24 open items In, 50 Out.
- 2026-11-19: TEST-60 Implemented (pull request 378, open; text in the archive): replaced short `1–5 s` timeouts and the `< 2 s` wall-clock assertion in `crates/worker/src/client/tests.rs` with generous condition waits (`TEST_WAIT_TIMEOUT = 60 s` via `timeout_at` / `timeout`, `30 s` on `junk .exe` and Unix hung-Blender checks) and raised tight mock task timeouts to `30–60 s` (reproduced on `origin/master` with `9/20` failures in full `rusty_grid_worker --lib` under `6` busy loops hitting all four `5 s` `Elapsed(())` timeouts; post-fix `20/20` alone `client::tests`, `20/20` `client::tests` under `6` busy loops, and `20/20` full `rusty_grid_worker --lib` under `6` busy loops).
- 2026-11-19: APP-6 Partial (item stays `Planned`, pull request 376, open): steps 1 and 4 done (`benchmarks/campaigns/2026-11-pilot/README.md`, 200-unit `manifest.json` with `verify_checksums.txt` and runner blocker, `dry_run_manifest.json` + `standin_workload.py`, `benchmarks/campaigns/driver.py`, `tests/test_campaign_driver.py`); steps 2 and 3 remain Planned.
- 2026-11-19: TEST-66 Implemented (pull request 387, open; text in the archive): replaced fixed sleeps and wall-clock upper bounds (`< 2 s`, `< 3 s`, `10 s` stop timeout, `5 s` backoff-interrupt timeout) in `tests/worker_cluster_auth.rs` with `TEST_WAIT_TIMEOUT` (`60 s`) condition waits, a `Notify` connect gate in `test_shutdown_interrupts_a_handshake_with_a_silent_master`, and a `25 s` (< `30 s` floor) shutdown check in `test_auth_failure_backs_off_instead_of_hot_looping_and_shutdown_interrupts_it` (`1/5` workspace Run 5 fail on `origin/master`; `20/20` before-fix and `20/20` after-fix in `worker_cluster_auth` under `6` Python busy loops on 10-core Apple Silicon macOS).
- 2026-11-19: LV-4 Partial (step 3, branch `win/lv-4-forbid-unsafe`, [design 0058](design/0058-lasting-value.md)): added `#![forbid(unsafe_code)]` to all 4 zero-`unsafe` binary crate roots (`oxideswarm`, `rusty-grid`, `ox-mode`, `agent-mesh`) and `#![deny(unsafe_code)]` to all 6 library crate roots with surgical `#[allow(unsafe_code)]` + `// LV-4:` reasons on `android_bridge`'s 10 `#[no_mangle]` JNI functions, `agent_mesh::executor`, `master::store` (`store::fuzzing` `forbid`), `core::{capabilities, noise_auth, private_file}` + test-only `blob::tests`/`task::tests`, `cli::{join, mode_cli::dev_launch}`, and `worker::{blob_cache, isolation, process_tree, sandbox, workspace}` + test-only `client::tests`/`runner::{outcome_tests, process_tree_tests}`/`staging::tests`; full module inventory recorded in design 0058, zero runtime change (items unchanged: still Waiting for CI Miri run on `master`).
- 2026-11-19: APP-6 stays Partial (item stays `Planned`, pull request 383, open): the runner is ready (Anima Engine pull request 167, merge commit `963f436e`); the pilot sheet and `manifest.json` moved to `run_esel1_experiment --trial` with `runner_ready: true` and `out/trial/verify_checksums.txt`, and record the owner's exception to Anima Engine rule `QĐ-OXS` for this pilot; steps 2 and 3 remain Planned.
- 2026-11-19: BUG-34 added and Implemented (pull request 391, open): `BlobCache` (`crates/worker/src/blob_cache.rs`) holds an exclusive advisory `File::try_lock` on `cache/blobs/owners/<owner_uuid>.lock`, names temp files `<owner_uuid>.<part_uuid>.part`, and sweeps dead-owner `.part` + orphan `.lock` files on `BlobCache::open` and on worker startup (`sweep_dead_owner_tmp` in `crates/worker/src/sandbox.rs`) while keeping files of live writers (`WouldBlock`) and preserving the 24-hour `STALE_TMP_AGE` fallback for legacy unowned files; verified on Windows (`rustc 1.96.0`) by `clippy -p rusty_grid_worker --all-targets -- -D warnings`, `blob_cache::tests` (`12/12` including cross-process kill-vs-live, owner-lock race retry, and Windows junction/ACL checks), `blob_client::tests` (`6/6`), and `sandbox::tests` (`18/18`).
- 2026-11-19: TEST-63 Implemented (pull request 389, open; text in the archive): `test_mapreduce_tasks_run_on_all_workers_at_once` (`tests/master_limits_and_scheduling.rs`) replaces `sleepy_job` and the `< 7 s` wall-clock check with `barrier_job`, where all 4 mappers announce `$RUSTY_GRID_WORKER_ID` into a temporary barrier directory and wait until all 4 `active-*` files exist at once (asserting `4` markers split `2`/`2` across both workers, `WAIT = 180 s`, `mapper_timeout_secs = 540`, and clean worker shutdown before `TempDir` drop); on 10-core Apple Silicon macOS under `6` busy loops, it failed `1/5` in workspace Run 5 (`took 20.682465417s`) and passed `20/20` before (`120.02 s`, `5.05–7.33 s`) and `20/20` after (`57.08 s`, `1.90–5.25 s`).
- 2026-11-19: ADOPT-1 stays `Planned` (study kit ready, pull request 393, open): added `docs/study/2026-11-adopt-1/` (English/Vietnamese participant guide, facilitator sheet, consent template, and round-one record guide), `scripts/study_timer.py`, and `tests/test_study_timer.py` (CI `agent-bridge`); no round-one session has run yet.
- 2026-11-19: TEST-62 Implemented (pull request 390, open; text in the archive): `cli::test_cli_submit_input_and_output_round_trip_with_dedup` (`tests/blob_transfer_e2e.rs`) now runs blocking CLI child processes on `tokio::task::spawn_blocking` so Tokio worker threads running the in-process `WorkerClient` heartbeat loop are never blocked, passes `--heartbeat-timeout-secs 60` to `rusty-grid master`, raises port/worker registration waits to `60 s` async loops, raises `submit` `--timeout-secs` to `120`, prints stdout/stderr/`master.log` on submit failures, and shuts down the worker cleanly before exit; on 10-core Apple Silicon macOS under `6` busy loops, it failed `1/5` in workspace Run 5 (`Some(1)` instead of `Some(7)`), passed `20/20` before (`98.11 s`, `3.61–6.98 s`; full binary `1/1` loaded probe `30.08 s`) and `20/20` after (`98.23 s`, `3.88–6.83 s`).
- 2026-11-19: TEST-64 and TEST-65 Implemented (pull request 388, open; texts in the archive): `runner::process_tree_tests::test_partial_output_survives_timeout_and_cancellation` (`crates/worker/src/runner/process_tree_tests.rs`) now waits by condition (polling a marker file written after `echo partial-out` up to `60 s`) before cancelling instead of sleeping `600 ms` and gives the timeout subtest `3 s`; `test_sec5_hardened_runner` (`crates/worker/tests/test_sec5_hardened_runner.rs`) and `crates/worker/tests/common/mod.rs` replace `< 1 s` wall-clock assertions and `1–2 s` / `15 s` fixed waits with `60 s` condition waits, check pipe EOF without the drain note under `pipe_drain_grace = 10 s`, treat `ProcessStatus::Dead` as dead alongside `Zombie`, and retry `EINTR` in Linux `kill_cgroup_leaf` and `kill_escaped_by_env` (`crates/worker/src/process_tree.rs`). Both failed `1/5` in workspace Run 5 on macOS (and TEST-65 failed on `ubuntu-latest` in CI run 44525741499); on 10-core Apple Silicon macOS under `6` busy loops, `TEST-64` passed `20/20` before (`62.54 s`, `2.04–4.11 s`) and `20/20` after (`86.09 s`, `3.45–5.45 s`), and `TEST-65` (`test_sec5_hardened_runner`) passed `20/20` before (`84.62 s`, `3.03–6.03 s`) and `20/20` after (`76.26 s`, `3.02–4.08 s`); Linux verification (`sec5 hardened runner (ubuntu-latest)` and `workspace tests (ubuntu-latest)`) is covered by GitHub Actions CI because Docker is not installed on this Mac.
- 2026-11-19: TEST-61 Partial (pull request 392, open): not reproduced on macOS in 140 targeted runs (`0/50` alone unloaded, `0/50` alone under 6 busy loops, `0/20` full `test_feat3_os_isolation` unloaded, `0/20` loaded) plus `5/5` full loaded workspace runs; proved `BUG-33`'s `close_open_fds_macos` runs solely in the detached grandchild's descriptor table and passes 128 concurrent multithreaded piped-stdout spawns; added `PreparedUnix::drop` lifeline dismissal, `capture.rs` read-error warning, and `TaskResult` diagnostics across `mod macos` (and Linux signal) control assertions (`20/20` post-fix loaded loops each on `process_tree::tests` and the hard-link test).
- 2026-11-19: TEST-26 Partial (pull request 382, open, [design 0065](design/0065-large-transfer-and-long-run-tests.md) D4.2): added scheduled (`17 3 * * 1`) and `workflow_dispatch` `endurance` job on `ubuntu-latest` to `.github/workflows/ci.yml` (`T1` `1 GiB`, `T2` `20,000` files, `T4`, `T6`, `T7`, `T8`, and `45m` soak, uploading `endurance-records`; pinned action commits; static check in `tests/test_endurance_harness.py`); stays Partial until green on two scheduled runs.
- 2026-11-19: TEST-67 Implemented (pull request 394, open; text in the archive): confirmed both `agent bridge` CI flakes are in `tests/test_campaign_driver.py` rather than `benchmarks/campaigns/driver.py`, replaced shared `open("a")` log appends with per-invocation atomic directory files (`FAKE_OXIDESWARM_LOG_DIR`, `ts_ns` + `pid` + `uuid4`) sorted chronologically by `read_fake_cli_logs`, raised `--submit-timeout-secs` in `test_timeout_records_failed_and_cancels_task` from `0.2` to `5` (keeping `u002`'s `sleep_secs: 30.0`, 6x the watchdog, because Windows `proc.kill()` ends only `.cmd` wrappers and not child processes holding pipes), and raised `test_interrupt_records_interrupted_cancels_and_resumes` waits/sleeps to `60.0 s` / `600.0 s` with full attempt diagnostics; 20-run loaded loops under 6 busy loops on Apple M5 passed `20/20` before (`74.61 s`, `3.64–3.84 s`) and `20/20` after (`173.79 s`, `8.52–9.21 s`).
- 2026-11-19: TEST-79 Implemented (pull request 401 (open); text in the archive): in `crates/worker/tests/scale1_simulated_worker.rs`, added `TEST_WAIT_TIMEOUT = 60 s` and widened registration (`20 s`), 200-task burst `recv_msg` (`5 s`), reconnection (`20 s`), post-reconnect `recv_msg` (`15 s`), `wrapped_idle.read` (`2 s`), and shared `timeout_at` worker-shutdown drain (`15 s`) deadlines to `60 s` plus non-timeout `builtin_task` limits (`30 s` -> `60 s`; cancel `shared_delay_nanos` -> `60 s` / `builtin_task(false, 120)`) while keeping the `1 s` `EXIT_CODE_TIMEOUT` check; 20-run loaded loops under 6 busy loops on Apple M5 passed `20/20` before (`37.29 s`, `1.72–3.59 s`, expected `0/20` since the `5 s` flake occurred on a 12-thread Windows laptop) and `20/20` after (`35.51 s`, `1.70–1.85 s`; single run `3/3` in `1.64 s`).
- 2026-11-19: TEST-78 Implemented (pull request 400 (open); text in the archive): in `tests/rendezvous_discovery_e2e.rs`, added `TEST_WAIT_TIMEOUT = 60 s` and `run_with_timeout_with_logs` (draining child `stdout`/`stderr` in background reader threads and dumping child output plus `Rendezvous`/`Master`/`Worker` logs on timeout without blocking on inherited grandchild pipe handles), and widened `wait_for_port_file` (`10 s`/`15 s` -> `60 s`), registration deadline (`30 s` -> `60 s`), `run_with_timeout` CLI waits (`10–20 s` -> `60 s`), and `discover_master_rendezvous` checks (`1–2 s` -> `60 s`); 20-run loaded loops under 6 busy loops on Apple M5 passed `20/20` before (`195.42 s`, `9.54–11.58 s`, expected `0/20` since the `20 s` timeout flake occurred on a 12-thread Windows laptop) and `20/20` after (`193.82 s`, `9.39–10.00 s`; single run `3/3` in `9.16 s`).
- 2026-11-19: TEST-74 Implemented (pull request 398, open; text in the archive): in `tests/m5_challenger_2_stress.rs`, repaired crash injection where `.find(...)` over `list_tasks` `HashMap` order could match a `Completed` map task on `w1` instead of a `Running` reduce task (polling up to `60 s` to crash whichever worker `w1`/`w2` holds the `Running` map/reduce task with `task_summary` diagnostics), protected against the `60 s` idle stall (stalled task unknown; two consecutive `~67.4 s` local runs point to a load burst / 85 % CPU backpressure) via `TEST-54` `MR_WAIT = 180 s` / `540 s` MapReduce timeouts with `queue_stats` diagnostics, asserted `tasks.len() == 7` (2 map + 5 reduce, no map recomputation) and `>= 1` `Completed` task with `failover_count >= 1` on the surviving worker, widened `wait_for_workers` (`5 s` -> `60 s`), and added `TEST-20` worker config flags in `spawn_test_worker`; reproduced `2/20` failures before at `:623:10` (`252.31 s`, `4.97–67.79 s`) and passed `20/20` after (`74.45 s`, `3.61–3.86 s`; single run `11/11` in `3.51 s`) under 6 busy loops on Apple M5.
- 2026-11-19: TEST-77 Implemented (pull request 399 (open); text in the archive): in `tests/e2e_cluster.rs`, added `TEST_WAIT_TIMEOUT = 60 s`, floored `wait_for_workers`, `submit_and_wait`, and `poll_until` at `60 s`, widened direct `wait_task` (`5–15 s` -> `60 s`), cancellation target sleeps (`120 s`/`180 s`), `2–3 s` deadlines, MapReduce specs (`180 s`), and non-timeout `TaskRequirements` (`10–30 s` -> `60 s`) while preserving `elapsed >= 250 ms` serial throttling in `test_tier2_bva_max_concurrency_limit_throttles_worker` and the `1 s` timeout test, and added `TEST-20` no-GPU/no-Blender/no-CUDA/no-portal flags in `TestCluster::spawn_worker` and the concurrency test; 20-run loaded loops under 6 busy loops on Apple M5 passed `20/20` before (`162.16 s`, `7.52–9.50 s`, expected `0/20` since the `5 s` flake occurred on a 12-thread Windows laptop) and `20/20` after (`63.93 s`, `2.99–3.38 s`).
- 2026-11-19: TEST-73 Implemented (pull request 397, open; text in the archive): in `tests/m4_scheduling.rs`, replaced the `< 800 ms` / `< 1200 ms` parallel-batch wall-clock cap in `test_m4_parallel_batch_spread_scheduling` with a cross-platform `started_*`/`release` barrier proving `>= 3` concurrent running tasks within `60 s` (keeping 3-worker spread assertions and printing both timings), dropped the `< 1.5 s` cancellation wall-clock check in `test_m4_in_flight_task_cancellation` (`sleep 120`, printing `cancel_duration`), gated failover retry disconnect on markers instead of `sleep 0.8`, and widened registration, worker disconnect, `wait_task`, and task timeouts to `TEST_WAIT_TIMEOUT = 60 s` (`wait_for_workers` / `wait_for_task_running` diagnostics); 20-run loaded loops under 6 busy loops on Apple M5 passed `20/20` before (`53.70 s`, `2.56–3.88 s`, expected `0/20` since the flake occurred on a 12-thread Windows laptop) and `20/20` after (`34.18 s`, `1.60–1.81 s`).
- 2026-11-19: TEST-68 Implemented (pull request 396, open; text in the archive): raised fixed `3–8 s` waits (`wait_for`, `wait_task`) and `5–15 s` task timeouts to `60 s` (`TEST_WAIT_TIMEOUT`, shared `wait_for_workers` dumping observed worker statuses on failure) in `tests/gpu_wgpu_compute_test.rs` (`test_e2e_cluster_gpu_task_distribution` and `test_adversarial_scheduling_isolation_mixed_cluster`), kept every strict routing check and the `500 ms` `Queued` negative check, and re-verified `stranded_id` stays `Queued` after `quick_cpu` completes on a free CPU worker; compiles with `--features gpu-wgpu` and passed `20/20` (`97.83 s`, `4.71–6.37 s`) before and `20/20` (`95.01 s`, `4.69–4.91 s`) after under 6 busy loops on Apple M5.
- 2026-11-20: TEST-80 Implemented (pull request 402 (open); text in the archive): in `crates/worker/tests/test_sec5_hardened_runner.rs` (`test_bug10_3d_and_blender_refuse_oversized_sparse_output_without_reading`), dropped both `elapsed < 5 s` wall-clock upper bounds (logging elapsed times instead), kept the metadata-refusal structural checks (`"is {sparse_bytes} bytes"`, `!"is more than"`, `asset_bytes.is_none()`), and raised the `BlenderRigProvider::execute_rigging` timeout from `15 s` to `60 s`; `--features 3d` 20-run loaded loops under 6 busy loops on Apple M5 passed `20/20` before (`12.88 s`, `0.54–1.91 s`) and `20/20` after (`11.47 s`, `0.54–0.61 s`; single full binary `28/28` in `2.83 s`).
- 2026-11-20: TEST-69 added and Partial (pull request 403, open; text in the archive): hardened five Windows AppContainer isolation tests in `crates/worker/tests/test_feat3_os_isolation.rs` (`mod windows`) against CPU starvation using `WIN_WAIT_TIMEOUT = 60 s`, `wait_for_barrier` with `AtomicBool` early-exit detection and `format_result_diag`, `GO_SEEN` barrier confirmation in `test_concurrent_tasks_under_fs_cannot_read_or_list_each_others_sandboxes`, `ChildKillGuard` + stdout `"READY"` pipe synchronization, `30 s` `DeleteAppContainerProfile` cleanup polling, and `90 s` `win_task` budgets on barrier-coordinated tests (`5/5` failures of `test_attack_junctions_and_hardlinks_inside_sandbox_cannot_expose_denied_secret` during full loaded workspace runs under `6` busy loops; pre-shutdown `20/20` passed before [`150.03–169.37 s`, median `154.22 s`] and `20/20` passed after [`148.63–157.44 s`, median `151.07 s`] in isolated `--test-threads=1` runs under `4` busy loops with `RUSTY_GRID_REQUIRE_APPCONTAINER=1`, plus pre-shutdown `clippy`; rebased post-restart onto `8513b79` and verified `1/1` [`31 passed`, `211.33 s`] with `RUSTY_GRID_REQUIRE_APPCONTAINER=1 --test-threads=1`, then merged `origin/master` `fd0df6a` and added `GO_SEEN` [verified by `windows-latest` CI; local `1/1` check to follow once free RAM `>= 6 GiB`]). Remains `Partial` because the failing full loaded configuration under `6` busy loops crashed this laptop twice and was not re-run here (the fix was looped only as this binary alone under `4` busy loops, where the old code also passed); confirmation in the failing configuration runs on `agent-win` after its 24-hour soak.
- 2026-11-20: TEST-81 Implemented (pull request 404, open; text in the archive): confirmed with a temporary debug master log under 6 busy loops plus concurrent `cargo test -p rusty_grid_cli` that `SchedulerConfig::max_host_cpu_pct` (`85.0`) holds workers back (`51` `DEBUG` lines logging `max_host_cpu_pct=85.0`, `cpu_usage_pct=99.60392`, stalling the 3rd submit after `5 s`; removed per review and replaced by `SCHED-5` for the product behavior), added `SchedulerConfig::with_max_host_cpu_pct` and `SchedulerConfig::without_host_cpu_backpressure` in `crates/master/src/scheduler.rs`, and opted `57` non-backpressure test files out (`without_host_cpu_backpressure()` / `--max-host-cpu-pct 100`) while keeping production default `85.0` and all backpressure/config tests unchanged; `blob_transfer_e2e` 20-run loaded loops passed `20/20` before (`576.66 s`, `26.18–36.63 s`; reproduced in Q80 workspace Run 2) and `20/20` after (`627.62 s`, `29.01–33.98 s`), and the full workspace suite passed (`137` binaries, `2,228 passed; 0 failed; 5 ignored`).
- 2026-11-20: Added SCHED-5 (Planned, Out): tell the user why a queued task waits when every eligible worker is above `max_host_cpu_pct` (open question from TEST-81, with PR 404 review inputs: `AllEligibleWorkersSaturated` skips only, connected workers only, once per change of the busy-worker set; visible form to be decided in a design record).
- 2026-11-20: TEST-82 Implemented (pull request 405 (open); text in the archive): replaced `24` wall-clock upper bounds across `rusty_grid_worker` (keeping `4` release-only `!cfg!(debug_assertions)` SLAs untouched and skipping `3` hits in `worker_connection_lifecycle` [TEST-76] and `mod windows` [TEST-69]) and bounded all `3` status-query `recv_msg` awaits in `crates/worker/tests/scale1_simulated_worker.rs` (`:268`, `:313`, `:343`) by the remaining `TEST_WAIT_TIMEOUT` budget; verified `cargo fmt --all -- --check`, workspace + worker `--features 3d,gpu-wgpu` clippy (`-D warnings`), and `20/20` loaded runs under 6 CPU busy loops across all 7 touched binaries (plus `--features gpu-wgpu` `wgpu_engine`).
- 2026-11-20: TEST-83 Implemented (pull request 406, open; text in the archive): replaced all `36` audited Category 1 wall-clock upper bounds plus `4` adjacent upper bounds across `18` files / `15` binaries in `rusty_grid_core` (`lib`, `m1_3d_adversarial_stress`, `m1_stress_challenge`) and `rusty_grid_master` (`lib`, `cluster_auth_e2e`, `m10_streaming_stress`, `m2_challenger_2_stress`, `m2_stress_challenge`, `m4_challenger_2_stress`, `m9_stress_challenge_2`, `master_limits_and_scheduling`, `master_quic_limits`, `test_challenger_m4_scheduler_runner`, `worker_crash_rescheduling_test`, `zero_copy_quic_test`), skipping `m2_challenger_1_stress` (`TEST-70`), `m4_challenger_1_stress` (`TEST-71`), and `master_restart_recovery` (`TEST-75`) as instructed, with `0` `!cfg!(debug_assertions)` release-only SLAs in `core`/`master`/`tests`; replaced proof proxies with structural checks (`mapreduce`, `recovery_tests`, `scheduler`, QUIC flow-control pause in `m2_challenger_2_stress` and `zero_copy_quic_test`, Slowloris `try_read` `WouldBlock` + `120 s` malformed handshake in `m2_stress_challenge`, `first.close_reason().is_none()` in `master_quic_limits`), removed `1000`/`5000 msg/s` throughput floors in `m1_stress_challenge`, fixed the raw-worker `AssignTask`/reaper race in `worker_crash_rescheduling_test` and `m9_stress_challenge_2` (`ReaperConfig(50 ms, 1 s)`, heartbeating until `AssignTask` under a `60 s` `select!` guard, keeping `(writer, reader)` open until `wait_task` resolves), and widened hang guards to `60 s` (and inner sleep targets to `120 s`); `5` cheap targeted before-loops passed `20/20` on `f485533e` (expensive full-binary before-loops skipped), and all `15` touched binaries passed `20/20` after under 6 Python CPU busy loops on Apple M5 with private `HOME` (`core --lib` `165.40 s`, `m1_3d_adversarial_stress` `3640.83 s`, `m1_stress_challenge` `48.74 s`, `master --lib` `485.30 s`, `cluster_auth_e2e` `62.43 s`, `m10_streaming_stress` `18.92 s`, `m2_challenger_2_stress` `22.97 s`, `m2_stress_challenge` `211.20 s`, `m4_challenger_2_stress` `103.60 s`, `m9_stress_challenge_2` `48.16 s`, `master_limits_and_scheduling` `87.71 s`, `master_quic_limits` `73.76 s`, `test_challenger_m4_scheduler_runner` `10.05 s`, `worker_crash_rescheduling_test` `50.04 s`, `zero_copy_quic_test` `20.48 s`).
- 2026-11-20: TEST-25 Partial (branch `win/test-25-24h-soak-record`, [design 0065](design/0065-large-transfer-and-long-run-tests.md)): committed `22.73 h` (`81,826.46 s`, stopped early at owner decision with ~`1.27 h` left) Windows `x86_64` loopback soak record `benchmarks/endurance/results/2026-11-19-soak-24h-loopback-windows-amd64-intel-core-i5-14600kf.json` on `rusty-grid 0.4.4` (`cfb0679`), assembled from preserved logs, pre-stop status (`pending=0`, `running=0`), `/api/tasks`, periodic snapshots, and idle disk/state captures: `63,059` unique master-logged task results across `12` master generations and `71` worker logs (`61,642` `exit_code=0`, `1,360` `exit_code=130`, `57` `exit_code=124` 60 s watchdog timeouts clustered in `06:30–06:31Z` [`9`] and `10:33–11:55Z` [`48`] during `25–52 s` log gaps), `0` duplicates, `0` log-level `ERROR`/panics, `112` failover sweeps, `27` orphan requeues; idle `master-blobs` (`4,244,637,442 B`), worker caches (`<= 2,105,542,715 B`), `0` `.part`/`.tmp`/`.locks` leftovers, runtime `journal.bin` compaction (`2,104,993 -> 54,043 B`), and `0` leftover processes; in-memory `TaskLedger` SHA-256 verification, 60 s `BackgroundSampler` means, continuous `probe_samples` p99, and `fault_events_log` marked `not_evaluated_due_to_early_stop`. Stays Partial pending the 3-machine run.
- 2026-11-20: TEST-84 Implemented (pull request 407, open; text in the archive): replaced all `19` audited Category 1 wall-clock upper bounds across `12` files / `12` binaries in `agent_mesh` (`lib`, `adversarial_challenge`), `rusty_grid_android_bridge` (`lib`, `m5a_challenger_2_stress`, `mobile_coordination_test`), `rusty_grid_cli` (`mcp_server_e2e`, `test_cli_cancel_and_result`, `test_m2_identity_scripts`, `test_mcp_concurrent_and_deadlines`, `test_p2p_key_persistence`), and Python tests (`tests/test_agent_bridge.py`, `tests/test_sec6_mesh_sandbox.py`), with `0` `!cfg!(debug_assertions)` release-only SLAs in those crates or scripts; replaced proof proxies with structural checks (`drain_finished` in `android_bridge`, `Cancelled` queue state in `test_cli_cancel_and_result`, `release.gate` + `ReleaseOnDrop` release-gated evacuation tasks with `evacuation_count == 1`/`2` + `ReaperConfig(1 s, 120 s)` in `m5a_challenger_2_stress` and `mobile_coordination_test`, `watch_*.lock` + `first.poll() is None` in `test_agent_bridge.py`), replaced the dead `"RustyGrid Worker started"` wait in `test_p2p_key_persistence` with `saw_warn` + `saw_dialing` (keeping a `2 s` post-dial window), preserved `>= 900 ms` / `>= 950 ms` `1 s` lower bounds (`30 s` `task_result` guard), renamed `test_worker_auto_reconnect_preserves_identity` and `test_fast_evacuation_without_task_loss`, and widened hang guards to `60 s` (`120 s` inner sleeps); `5` cheap targeted before-loops passed `20/20` on `origin/master` (`e31b69e3`), and all `12` touched binaries passed `20/20` after (including post-review re-runs of the 5 review-updated binaries) under 6 Python CPU busy loops on Apple M5 with private `HOME` (`agent_mesh --lib` `53.26 s`, `adversarial_challenge` `38.18 s`, `android_bridge --lib` `91.60 s`, `m5a_challenger_2_stress` `62.05 s`, `mobile_coordination_test` `1045.91 s`, `mcp_server_e2e` `75.44 s`, `test_cli_cancel_and_result` `59.86 s`, `test_m2_identity_scripts` `32.13 s`, `test_mcp_concurrent_and_deadlines` `160.00 s`, `test_p2p_key_persistence` `84.91 s`, `test_agent_bridge.py` `1121.88 s`, `test_sec6_mesh_sandbox.py` `192.31 s`).
- 2026-11-21: FEAT-9 Implemented (default of D6; text in the archive): updated `PROJECT.md` and `README.md` (alongside `docs/reference.md`) to state explicitly that built-in `GpuCompute` is benchmark only (seeded matrix multiplication; no user WGSL kernels) while user GPU workloads run as `Command` tasks (`docs/guides/gpu-workloads.md`), alongside `FEAT-9a` VRAM accounting (pull request 232, [design 0068](design/0068-task-vram-accounting.md)).
- 2026-11-21: Recounted `docs/BACKLOG.md` Index summary header from the rows after `TEST-84`, `APP-6`, and `FEAT-9` Implemented: 247 items, 172 Implemented, 21 Partial, 54 Planned, 0 Design only; 25 open items are In and 50 are Out (`TEST-56` keeps `Line` `In` while pull request 361 is marked open).
- 2026-11-21: LV-10 Partial (steps 1–3, [design 0058](design/0058-lasting-value.md)): added `CITATION.cff` (CFF 1.2.0, `version: "0.4.4"`, `MIT` / `Apache-2.0`, `"The OxideSwarm Authors"`), stdlib metadata/version-sync test in `tests/test_m1_challenger_hygiene.py`, and `"How to cite"` in `README.md`; step 4 (Zenodo DOI) stays with the owner.
- 2026-11-21: PROTO-5 Partial ([design 0083](design/0083-one-version-window.md)): pinned `PREVIOUS_PROTOCOL_RELEASE: v0.3.0` alongside the newest release in `.github/workflows/ci.yml` `release-skew` with per-tag directory isolation, added `--release-tag` to `scripts/release_skew.py`, added CLI and static CI workflow tests in `tests/test_release_skew.py`, and updated `CLAUDE.md` and `docs/reference.md`; stays Partial awaiting live GitHub pull request CI run of `release skew (ubuntu-latest)` showing the `v0.3.0` table row.
- 2026-11-21: Recounted `docs/BACKLOG.md` Index summary header after `LV-10` and `PROTO-5` Partial: 247 items, 172 Implemented, 23 Partial, 52 Planned, 0 Design only; 25 open items are In and 50 are Out.
- 2026-11-21: ENROLL-1d Partial (API/UI/tests/docs complete; [design 0051](design/0051-machine-identity-and-invites.md)): wired `MasterAuthContext` and `WaiterMap` onto `MasterHandle` and `WebUiState`; added `MemberConnections::list_active`, `revoke_member_with_teardown` reuse, `AuthScope::Admin` enforcement on all `/api/members*` and `/api/invites*` routes (`crates/master/src/auth.rs`), `Method::DELETE` CORS preflight, and `crates/master/src/web_ui/members.rs`; added XSS-safe `Members` tab to `crates/master/src/dashboard.html`; expanded `tests/dashboard_api_test.rs` and `tests/dashboard_security_test.rs`; updated `docs/reference.md`, `docs/guides/add-a-machine.md`, `docs/SECURITY.md`, `README.md`, and `design/0051`. `ENROLL-1` remains `Partial` pending `ENROLL-1c` step 2 (store format change) and the recorded live two-network `--p2p` run.
- 2026-11-21: SCHED-5 Implemented ([design 0095](design/0095-scheduler-backpressure-visibility.md); text in the archive) and SCHED-4c / SCHED-4d implemented ([design 0060](design/0060-speed-aware-placement.md)): `WorkloadScheduler::schedule_batch` now emits a transition-triggered `warn!` once per change of the connected, free-slotted, CPU-busy worker set when tasks skip with `AllEligibleWorkersSaturated` (clearing silently at `info!` without `"saturated"` on recovery and ignoring `Disconnected` workers and non-`AllEligibleWorkersSaturated` skips); `crates/core/src/placement.rs` reserves `opt:` (`opt:repeatable`) and `speed:` (`speed:<factor>`) without bumping `PROTOCOL_VERSION_CURRENT` (`10`) or `STORE_FORMAT_VERSION` (`4`); CLI `--repeatable` / MCP `repeatable`, in-memory `SpeculativeAttempt` + sibling cancellation/failure tolerance in `TaskQueue`, and bounded `(total_cluster_slots / 10).max(1)` straggler re-run in `crates/master/src/reaper.rs` complete `SCHED-4c`; `SpeedBook::relative_speed_factors`, `WorkerRegistry::list_all_workers` `speed:<factor>` tags, `workers --json` `"speed"`, `WorkerUiInfo::speed_factor`, and `.badge-speed` / `#mSpeed` in `dashboard.html` complete `SCHED-4d`. `SCHED-4` remains `Partial` (`Out`) pending a dated live two-machine hardware batch (`<= 1.5x` the fast machine's own time).
- 2026-11-21: Recounted `docs/BACKLOG.md` Index summary header after `SCHED-5` Implemented: 247 items, 173 Implemented, 23 Partial, 51 Planned, 0 Design only; 25 open items are In and 49 are Out.
