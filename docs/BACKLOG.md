# Completion backlog

Open work on OxideSwarm, in execution order. Each work item stands alone, so it can be handed to a
coding agent without other context.

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
   `README.md` and gets a design record, in the same pull request.
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
"Pull requests" lists them by number, "(open)" marks one
that is not merged yet, and "(design)" marks a design record that builds nothing.

| ID | Item | Priority | Size | Needs | Owner | Status | Pull requests |
|---|---|---|---|---|---|---|---|
| INT-1 | Merge pull request 3 | P0 | S |  | Yes | Implemented | 3, 4 |
| INT-2 | Close superseded pull requests and branches | P0 | S | INT-1 | Yes | Implemented |  |
| INT-3 | Land the AEAD hardening branch | P0 | S | INT-1 | Yes | Implemented | 5 |
| INT-4 | Land the content-addressed file branch | P0 | M | INT-3, INT-5 | Yes | Implemented | 6 |
| INT-5 | Fix the review findings of pull request 6 | P0 | M | INT-3 |  | Implemented | 6 |
| SEC-1 | Remove `mcp --sse` | P0 | S | INT-4 |  | Implemented | 11 |
| SEC-2 | Make MCP refuse a remote master without a secret | P0 | S | INT-4 |  | Implemented | 15 |
| SEC-3 | Delete the Android stub master | P0 | M | INT-4 | D2 | Implemented | 13 |
| SEC-4 | Delete the plain-HTTP secret transfer | P0 | S | INT-4 | D7 | Implemented | 14 |
| SEC-5 | Run Python and Blender through the hardened runner | P0 | M | INT-4 |  | Implemented | 17 |
| SEC-6 | Restrict Agent Mesh node commands | P0 | M | INT-4 |  | Implemented | 25 |
| SEC-7 | Correct public route claims and redact the lease | P1 | S | INT-4 |  | Implemented | 12 |
| SEC-9 | Private files on Windows | P1 | S | INT-4 |  | Implemented | 154 |
| SEC-10 | Stop a working-directory config file from configuring `worker` and `submit` | P1 | S |  | D8 | Implemented | 238 (design 0070) |
| SEC-11 | Refuse instead of falling back to the elevated token under Windows isolation | P2 | S | FEAT-3 |  | Implemented | 256 (design 0078) |
| SEC-12 | Test and narrow the macOS isolation profile | P2 | M | FEAT-3 |  | Implemented | 257 (design 0033) |
| SEC-13 | Collect outputs through the descriptor that was checked | P2 | S |  |  | Implemented | 239 (design 0072) |
| SEC-14 | Take the `agy` chat route out of the master dashboard | P2 | S |  | D9 | Implemented | 237 (design 0071) |
| SEC-15 | Make the Agent Mesh launchers safe by default | P2 | S | SEC-6 |  | Implemented | 258 (design 0079) |
| BUG-1 | Stop hard-linking the staging cache | P0 | S | INT-4 |  | Implemented | 16 |
| BUG-2 | Fail a task when a declared output is missing | P0 | S | INT-4 |  | Implemented | 20 |
| BUG-3 | Fix the default 3D submit | P0 | S | INT-4 |  | Implemented | 21, 44 |
| BUG-4 | Fail only the task when a result is too large | P1 | S | INT-4 |  | Implemented | 18 |
| BUG-5 | Close the remaining gaps in the blob store and cache | P1 | M | INT-4 |  | Implemented | 26 |
| BUG-18 | Resolve bare commands under Windows isolation against `System32` before `Git\usr\bin` | P1 | S | FEAT-3 |  | Implemented | 204 (design 0055) |
| BUG-19 | Bound the data-lane backlog of a QUIC worker session | P1 | S |  |  | Implemented | 236 (design 0069) |
| BUG-20 | Keep one slow worker from stalling every dispatch | P1 | M |  |  | Implemented | 241 (design 0075) |
| BUG-21 | Resolve relative staged executables against the task working directory on Windows | P1 | S | FEAT-3 |  | Implemented | 251 (design 0077) |
| 3D-1 | Stop reporting the stub as TripoSR | P0 | S | INT-4 | D3 | Implemented | 22, 46 |
| TEST-1 | Gate host-bound tests | P1 | M | INT-4 |  | Implemented | 51 |
| TEST-2 | Isolate tests from the host battery | P1 | M | INT-4 |  | Implemented | 76 |
| TEST-3 | Repair stale, starved and flaky tests | P1 | M | TEST-1 |  | Implemented | 39 |
| TEST-4 | Stop tests from reading the real home directory | P1 | S | INT-4 |  | Implemented | 29, 112 |
| TEST-5 | Register or delete the orphan Rust test | P1 | S | INT-4 |  | Implemented | 19 |
| TEST-6 | Extend CI to the full suite | P1 | M | TEST-1, TEST-2, TEST-3, TEST-4 |  | Implemented | 67, 124 |
| TEST-7 | Run or delete the orphan Python tests | P2 | M | INT-4 |  | Implemented | 55, 66 |
| TEST-19 | Fix flaky required tests | P1 | S | TEST-3 |  | Implemented | 161 |
| TEST-20 | Fix Windows disconnect stress timeouts | P1 | S | TEST-19 |  | Implemented | 181 |
| TEST-22 | Ignore UUID and metric `4090` tokens in `workflow_modes_test` | P1 | S | TEST-19 |  | Implemented | 200 |
| TEST-21 | Fix Windows `m6_challenger_1_stress` and `test_sec6_mesh_sandbox` flakes | P1 | S | TEST-20 |  | Implemented | 198 |
| TEST-23 | Find why `master_restart_recovery` loses `out.txt` on Windows | P1 | M | FEAT-1 |  | Planned |  |
| TEST-24 | Test large transfers between machines | P2 | M | BENCH-1 | Yes | Planned | (design 0065) |
| TEST-25 | Run a cluster for 24 hours under load and faults | P2 | L | TEST-24 | Yes | Planned | (design 0065) |
| TEST-26 | Run short versions of TEST-24 and TEST-25 on a schedule in CI | P3 | S | TEST-24, TEST-25 |  | Planned | (design 0065) |
| TEST-27 | Find why the `mesh_auth` slow-consumer test times out on macOS | P2 | S | |  | Partial | 263 |
| TEST-28 | Make the group-commit timing test meaningful on slow runners | P1 | S |  |  | Implemented | 265 |
| TEST-29 | Find why `workspace tests (windows-latest)` loses its runner at 60 minutes | P1 | M |  |  | Partial | 283 |
| TEST-30 | Stop `test_agent_bridge.py` from depending on the host's open ports | P2 | S |  |  | Implemented | 263 |
| TEST-31 | Remove the race in the detached-grandchild Blender probe test | P2 | S |  |  | Partial | 265 |
| TEST-32 | Make the cancel-during-upload worker test independent of `NoDefaultCurrentDirectoryInExePath` | P2 | S |  |  | Planned |  |
| TEST-33 | Make the worker-churn stress test deterministic on Windows | P2 | S |  |  | Implemented | 286, 289 |
| TEST-34 | Show and fix the Agent Mesh simulation Suite 2 failure on macOS | P2 | S |  |  | Implemented | 277 |
| TEST-35 | Make the `switch_role` launcher test stable on macOS | P3 | S |  |  | Partial | 277 |
| TEST-36 | Remove the 50 ms bound from the symlink-chain manifest test | P2 | S |  |  | Implemented | 276 |
| TEST-37 | Remove the 100 ms window from the port-file reader test | P2 | S |  |  | Implemented | 276 |
| TEST-38 | Remove the 800 ms window from the re-dispatched task id test | P2 | S |  |  | Implemented | 276 |
| TEST-39 | Wait for the `--no-shell` mesh node to register before calling it | P2 | S |  |  | Implemented | 278 |
| TEST-40 | Wait for each payload in the Agent Mesh large-payload test | P2 | S |  |  | Implemented | 278 |
| TEST-41 | Find why a task finished during master shutdown can run twice | P1 | M |  |  | Partial | 288 |
| TEST-42 | Make the reaper boundary test independent of scheduler delay | P2 | S |  |  | Planned |  |
| TEST-43 | Show and fix the late dashboard port file of the in-process dev cluster test | P2 | S |  |  | Planned |  |
| TEST-44 | Remove the 1.5x wall-clock cap from the speed-aware batch test | P2 | S |  |  | Planned |  |
| TEST-45 | Keep the second worker of the reaper failover stress test alive | P2 | S |  |  | Planned |  |
| TEST-46 | Remove the 100 ms bound from the failover latency stress test | P2 | S |  |  | Planned |  |
| TEST-47 | Count only this test's handles in the capability-detection leak test | P2 | S |  |  | Planned |  |
| TEST-48 | Give each Android bridge test its own worker state | P2 | S |  |  | Implemented | 300 |
| TEST-49 | Wait for the master by condition in the sched2 runtime test | P2 | S |  |  | Planned |  |
| DOC-1 | Correct false status claims | P1 | M | INT-4 |  | Implemented | 60 |
| DOC-2 | Remove unbacked performance numbers | P1 | S | INT-4 |  | Implemented | 61 |
| DOC-3 | Align design records with the code | P1 | M | INT-4 |  | Implemented | 68 |
| DOC-4 | Complete the CLI and environment reference | P2 | M | INT-4 |  | Implemented | 69 |
| DOC-5 | Document the 3D subsystem | P2 | S | 3D-1 |  | Implemented | 70 |
| DOC-6 | Correct the minimum Rust version | P1 | S | INT-4 |  | Implemented | 56 |
| DOC-7 | Add an MCP client setup guide | P2 | S | SEC-2 |  | Implemented | 32 |
| DOC-8 | Update `CLAUDE.md` and `SECURITY.md` for Windows isolation | P1 | S | FEAT-3 |  | Implemented | 196 |
| DOC-9 | Correct the statuses and statements that the audit found wrong | P1 | M |  |  | Implemented | 234 |
| DOC-10 | Keep one roadmap | P2 | S | DOC-9 | Yes | Implemented | 235 |
| REL-1 | Restore the Apache licence text | P0 | S |  |  | Implemented | 8, 82 |
| REL-2 | Remove owner data from tracked files | P1 | M | INT-4 | D5 | Implemented | 74 |
| REL-3 | Small repository hygiene | P2 | S | INT-4 |  | Implemented | 71 |
| CLI-1 | Add `cancel` and `result` commands | P1 | M | INT-4 |  | Implemented | 30, 47 |
| LOG-1 | Close P2P client endpoints cleanly and send client logs to stderr | P1 | S | FEAT-4 |  | Implemented | 194 |
| MCP-1 | Make the MCP server concurrent, with deadlines | P1 | M | SEC-2 |  | Implemented | 31, 41 |
| MCP-2 | Return outputs safely to an LLM | P1 | M | MCP-1 |  | Implemented | 62 |
| MCP-3 | Correct JSON-RPC and argument handling | P2 | S | MCP-1 |  | Implemented | 97 |
| SCHED-1 | Respect `--max-concurrency` on the master | P1 | S | INT-4 |  | Implemented | 34, 38 |
| SCHED-2 | Wire or delete dead flags | P1 | M | INT-4 |  | Implemented | 78 |
| SCHED-3 | Give impossible tasks an outcome | P2 | S | INT-4 |  | Implemented | 96, 107 |
| MOB-1 | Fix the adaptive keepalive | P1 | S | INT-4 |  | Implemented | 98 |
| MOB-2 | Keep thermal state on the master | P1 | M | INT-4 |  | Implemented | 99 |
| MOB-3 | Do not charge evacuation to retry budgets | P2 | S | INT-4 |  | Implemented | 108 |
| 3D-2 | Wire VRAM and Blender routing | P2 | M | BUG-3 |  | Planned |  |
| 3D-3 | Make AutoRig options real or refuse them | P2 | M | SEC-5 |  | Planned |  |
| MR-1 | Let a CLI user ship a mapper and reducer | P2 | S | INT-4 |  | Implemented | 33 |
| MESH-1 | Make `wss://` work in the Rust mesh client | P2 | S | INT-4 |  | Implemented | 94 |
| MESH-2 | Deliver mesh data to a consumer | P2 | M | INT-4 |  | Planned |  |
| MESH-3 | Attach the bridge to a running master | P2 | M | INT-4 |  | Partial | 114, 132 |
| MODE-1 | Make `ox-mode` report only what happened | P2 | M | INT-4 |  | Implemented | 92 |
| BRIDGE-1 | Repair `agent_bridge.py diagnose` | P2 | S | SEC-4 |  | Implemented | 77 |
| AND-1 | Make the Android app build | P1 | M | SEC-3 | Yes | Planned |  |
| AND-2 | Keep the master across restarts | P2 | M | AND-1 | Yes | Planned |  |
| AND-3 | Wire pre-sleep evacuation | P2 | M | AND-1 | Yes | Planned |  |
| AND-4 | Report real worker status | P2 | M | AND-1 | Yes | Planned |  |
| AND-5 | Stop the Android worker without blocking the calling thread, and build the bridge in CI | P2 | S | SEC-3 |  | Implemented | 240 (design 0073) |
| FEAT-1 | Master durability | P1 | L | TEST-6 |  | Implemented | 122 (design), 129, 141, 147, 158, 167 |
| FEAT-2 | Containment after a worker crash | P1 | L | SEC-5 |  | Implemented | 123 |
| FEAT-3 | OS-level task isolation | P2 | L | SEC-5 |  | Implemented | 117 (design), 133, 150, 152 |
| FEAT-3d | cargo under Windows isolation | P2 | M | FEAT-3 |  | Implemented | 183 |
| FEAT-4 | Remote workspaces for coding agents | P1 | L | MCP-2 |  | Implemented | 105 (design), 157, 156, 164, 166, 160, 165, 168, 170, 176, 174, 177, 186 |
| FEAT-5 | Checkpoints for process tasks | P3 | L | FEAT-1 |  | Planned |  |
| FEAT-6 | Refuse the master-role claim (failover itself is SCALE-4) | P3 | L | FEAT-1 | D1 | Implemented | 81 |
| FEAT-7 | Usable rendezvous discovery | P3 | M | INT-4 |  | Implemented | (design 0066), 224 |
| FEAT-8 | Real protocol versioning | P2 | M | INT-4 |  | Implemented | 40, 110, 120 |
| FEAT-9 | GPU workloads beyond the benchmark | P3 | L | INT-4 | D6 | Partial | 232 (`FEAT-9a`, design 0068) |
| FEAT-10 | Compile crates with cargo | P3 | M | INT-4 |  | Implemented | 165 |
| FEAT-11 | Remove dead code and split large modules | P3 | L | TEST-6 |  | Implemented | 118 (design), 125, 126, 127, 131 |
| MAINT-1 | Split the six largest source files and cap file size in CI | P2 | L | FEAT-11 |  | Implemented | 211, 212, 230, 231, 242, 249, 250, 253, 259 |
| CACHE-1 | Reuse the result of an identical task | P2 | L | FEAT-1 |  | Planned |  |
| REAPI-1 | Serve the blob store as a Remote Execution API cache | P3 | L | CACHE-1 | Yes | Planned |  |
| REL-4 | Release workflow and install path | P2 | M | TEST-6 | D4 | Implemented | 155 |
| REL-5 | Contributor and security files | P3 | S | REL-1 |  | Implemented | 75 |
| JOIN-1 | Join a machine with one token | P2 | M |  |  | Partial | 163 |
| LINK-1 | Show whether each link is direct or relayed | P2 | M |  | Yes | Partial | 159 |
| BENCH-1 | Publish benchmarks with the scripts that measured them | P2 | M |  |  | Implemented | 162, 175, 187, 189 |
| ENROLL-1 | A key per machine, invites and revocation | P1 | L | JOIN-1 |  | Partial | (design 0051), 190, 201 |
| JOIN-2 | Join on a LAN with a short code | P2 | M | ENROLL-1 |  | Implemented | (design 0052), 221 |
| USB-1 | Set up machines from a USB stick | P2 | M | ENROLL-1, REL-4 |  | Partial | (design 0053), 185, 195, 207, 228 |
| USB-2 | Opt-in Windows isolation in the USB kit | P2 | S | USB-1, FEAT-3d |  | Implemented | 204 (design 0055) |
| USB-3 | Keep the Windows USB install folder apart from the default workspace root | P2 | S | USB-1, W2 |  | Implemented | 226 |
| USB-4 | Size the kit's task slots to the machine's fast cores | P2 | S | USB-1 |  | Implemented | 209, 207, 210 |
| SCALE-1 | Load harness with simulated workers | P2 | L | BENCH-1 |  | Implemented | 222, (design 0050) |
| SCALE-2 | Scheduler and queue at scale | P2 | L | SCALE-1 |  | Implemented | 227, (design 0050) |
| SCHED-4 | Place tasks by measured worker speed and re-run stragglers | P2 | L | SCHED-1 |  | Partial | 214, (design 0060) |
| SCALE-3 | Worker-to-worker blob distribution | P2 | L | SCALE-1 |  | Planned |  |
| SCALE-4 | Master high availability | P2 | L | SCALE-1, FEAT-1 |  | Planned |  |
| SCALE-5 | Autoscaling providers | P3 | L | SCALE-1, JOIN-1 |  | Planned |  |
| SCALE-6 | Multi-site federation | P3 | L | SCALE-2, SCALE-3, SCALE-4 |  | Planned |  |
| ECO-1 | Remote Execution API compatibility | P3 | L | REAPI-1, ECO-2 |  | Planned |  |
| ECO-2 | Action cache (is CACHE-1) | P2 | L | FEAT-1 |  | Planned |  |
| ECO-3 | Multi-tenancy and quotas | P2 | L | SCALE-1 |  | Planned |  |
| ECO-4 | Observability: metrics and traces | P2 | M |  |  | Implemented | (design 0050), 225 |
| ECO-5 | Packaging for package managers and containers | P2 | M | REL-4 | Yes | Planned |  |
| ECO-6 | Client SDKs and a versioned public API | P3 | L | FEAT-8 |  | Planned |  |
| ECO-7 | Integrations for coding agents and CI | P2 | M | DOC-7 |  | Planned |  |
| ECO-8 | Documentation site and community | P3 | M | REL-5 | Yes | Planned |  |
| NET-1 | WebAssembly task type with limits and a deterministic mode | P3 | L | ENROLL-1 | Yes | Planned | (design 0054) |
| NET-2 | Federation and signed credit accounting between clusters | P3 | L | SCALE-6 | Yes | Planned | (design 0054) |
| NET-3 | Result verification: redundancy, spot checks, reputation | P3 | L | NET-1 |  | Planned | (design 0054) |
| NET-4 | Contributor controls: opt-in, caps, stop switch, task-class policy | P3 | M | NET-1 |  | Planned | (design 0054) |
| NET-5 | Lightweight VM sandboxes on Linux, macOS and Windows | P3 | L | NET-3 |  | Planned | (design 0054) |
| NET-6 | Distributed directory of offers over iroh | P3 | L | NET-2 |  | Planned | (design 0054) |
| NET-7 | Open protocol specification, conformance tests and a steward | P3 | L | NET-6 | Yes | Planned | (design 0054) |
| NET-8 | Trusted execution environments for confidential tasks | P3 | L | NET-5 |  | Planned | (design 0054) |
| LV-1 | Fuzz targets for the parsers of untrusted bytes (first targets) | P1 | M |  |  | Partial | (design 0058) |
| LV-5 | Supply chain: dependency policy, release provenance, outside checklist | P2 | M | REL-4 | Yes | Planned | (design 0058) |
| LV-10 | Citation file and a DOI per release | P2 | S | REL-4 | Yes | Planned | (design 0058) |
| LV-14 | Governance files and a licence that GitHub detects | P2 | S | REL-5 | Yes | Planned | (design 0058) |
| PROTO-1 | The release-train rule for wire changes | P1 | S |  |  | Implemented | (open), (design 0076) |
| PROTO-2 | A CI job that runs the newest release against the pull request's build | P1 | M | PROTO-1, REL-6 |  | Implemented | (design 0076) |
| PROTO-3 | An extension slot for optional wire fields | P1 | L | PROTO-1 |  | Implemented | (pull request 272), (design 0082) |
| PROTO-4 | A one-version compatibility window | P1 | L | PROTO-3 |  | Implemented | (pull request 274), (design 0083) |
| PROTO-5 | Keep a skew job against the release one protocol back (pinned) | P2 | M | PROTO-4 |  | Planned | (design 0083) |
| SEC-16 | Find what put a full-control entry for an unknown capability on `AppData` | P2 | S |  | Yes | Planned |  |
| BUG-22 | Count cancelled tasks apart from failed tasks in `status --json` | P3 | S |  |  | Planned |  |
| BUG-23 | Stop the Windows master gracefully in `oxctl.ps1` | P2 | S |  |  | Implemented | 282 |
| BUG-24 | Let a master restart with `--state-dir` on a file system without permissions | P2 | S |  |  | Implemented | 284 |
| BUG-25 | Hand a task back to the master when the worker's sandbox base is unusable at assignment | P2 | S |  |  | Planned |  |
| CLI-2 | Add a flag to move or disable the UDP discovery beacon | P3 | S |  |  | Implemented | 275 |
| CI-1 | Keep runs for pushes on `master` from ending red or cancelled without a failing test | P1 | S |  |  | Planned | (design 0088) |
| MAINT-2 | Bring the last three files under the size cap | P2 | M | MAINT-1 |  | Planned | (design 0088) |
| DOC-11 | Make the backlog show open work first | P2 | S |  |  | Planned | (design 0088) |
| ADOPT-1 | A study of first use, in two rounds | P1 | S |  | Yes | Planned | (design 0059) |
| ADOPT-2 | Run summary in `submit --wait`, its JSON output and the MCP results | P2 | S |  |  | Planned | (design 0059) |
| ADOPT-3 | Install scripts and the scripted time to first result | P2 | M | REL-4, ADOPT-1 |  | Planned | (design 0059) |
| ADOPT-4 | Guides for common jobs, each with a recorded run | P2 | M | ADOPT-1, AX-1, AX-2 |  | Planned | (design 0059) |
| AX-1 | Matrix fan-out in `submit` and in MCP | P1 | M | ADOPT-1 |  | Planned | (design 0056) |
| AX-2 | Sharded runs, with a build-once mode for Rust | P2 | M | AX-1 |  | Planned | (design 0056) |
| AX-3 | Live output, `logs`, and MCP tasks | P2 | L | PROTO-4 | Q55 | Planned | (design 0056) |
| LV-4 | An inventory of `unsafe`, with comments, a lint and Miri | P2 | M |  |  | Planned | (design 0058) |
| LV-15 | Release runbook, secret inventory and a second releaser | P2 | M | LV-5 | Yes | Planned | (design 0058) |
| LAUNCH-1 | Hold the public launch for the stability release and its records | P1 | S | TEST-24, TEST-25, LINK-1, ADOPT-1 | Yes | Planned |  |
| LAUNCH-2 | Install and run from the README alone on clean machines | P1 | S | LAUNCH-1 |  | Planned |  |
| LAUNCH-3 | Record the demo of the user story | P1 | M | LAUNCH-2 | Yes | Planned |  |
| LAUNCH-4 | Put the demo, the release and the benchmarks at the top of the README | P1 | S | LAUNCH-3 |  | Planned |  |
| LAUNCH-5 | Announce the release | P2 | S | LAUNCH-4 | Yes | Planned |  |
| COORD-1 | Claim design numbers, Q ids, phases and item ids on the hub | P2 | S |  |  | Implemented | 295, (design 0087) |
| COORD-2 | Changed files on the activity record, with an overlap check | P2 | S |  |  | Implemented | 295, (design 0087) |
| COORD-3 | One shared place for process lessons | P2 | S |  |  | Implemented | 294, (design 0087) |
| APP-4 | Trial: Anima Engine trials on the owner's two machines, checksums compared, a sweep through the grid | P1 | S |  | Yes | Planned | (design 0063), (design 0089) |
| VERIFY-1 | Run a task again on other workers and compare its outputs (`submit --verify`) | P1 | M |  |  | Planned | 296 (open), (design 0089) |
| APP-5 | Guide: Anima Engine sweeps on OxideSwarm, with a recorded run | P2 | S | APP-4, VERIFY-1 |  | Planned | (design 0089) |

## Phase 0: integrate open work

Do this phase first. Later items assume its result.

### INT-1: Merge pull request 3

- **Evidence:** Reproduced. All checks are green in run 40269419930.
- **Problem:** `master` has no CI, and its unit tests fail on any machine with a battery.
- **Do:** Merge pull request 3 (`claude/ci-github-actions`).
- **Done when:** The CI run on `master` passes.

### INT-2: Close superseded pull requests and branches

- **Evidence:** Reproduced.
- **Problem:** Pull request 1 (`fix/review-blockers`) has a tree identical to `master`.
  Pull request 2 (`claude/trusting-wing-d9a27c`) is an older CI attempt, 21 commits behind.
  `origin/feat/cross-machine-network-sync` and `origin/feat/two-node-readiness` are fully merged.
- **Do:**
  1. Copy what pull request 2 has and `ci.yml` lacks into the description of TEST-6.
  2. Ask the owner to close both pull requests and delete the merged remote branches.
- **Done when:** `gh pr list` shows neither pull request, and the merged branches are gone.

### INT-3: Land the AEAD hardening branch

- **Evidence:** Reproduced. The trial merge with INT-1 is clean.
- **Problem:** Commit `d0dddb8` on branch `claude/compassionate-mestorf-b58176` has no pull request
  yet. It adds auth version 2, a fail-closed record reader, and tamper and injection tests.
- **Do:**
  1. Rebase it on `master`, and open a pull request.
  2. State in the pull request that a version 1 peer is refused, so all nodes upgrade together.
- **Done when:** The pull request is merged and the gates pass. `tests/cluster_record_layer_e2e.rs`
  passes.

### INT-4: Land the content-addressed file branch

- **Evidence:** Reproduced. A trial merge of INT-1, INT-3 and INT-4 passed clippy, the
  `--no-default-features` check, all unit tests, `blob_transfer_e2e`, `cluster_record_layer_e2e` and
  `file_staging_e2e`. The full suite was not run on the merged tree.
- **Problem:** Commit `fae2622` on branch `claude/dreamy-kilby-6adcea` has no pull request yet. It
  adds BLAKE3 blobs, a master blob store, a worker blob cache, `submit --input` and glob outputs. It adds about
  6,400 lines in one commit.
- **Do:**
  1. Rebase it on `master` after INT-3.
  2. Resolve `crates/cli/Cargo.toml` by keeping the `[[test]]` entries of both branches.
  3. Both branches add a design record numbered 0005. Rename this branch's record to
     `docs/design/0006-content-addressed-data-plane.md` and update the references to it in
     `README.md` and `PROJECT.md`.
  4. Review the change by subsystem, and split it if a reviewer asks.
  5. State in the pull request that `Task` and `TaskResult` gained fields, so every component runs
     the same build and the master restarts first.
- **Done when:** The pull request is merged, the gates pass, and `tests/blob_transfer_e2e.rs`
  passes.

### INT-5: Fix the review findings of pull request 6

- **Evidence:** Reproduced at commit `e1215ee`, except F5, which is Read. The runs used real
  binaries on loopback with a temporary `HOME`. Line numbers in this item refer to that commit.
- **Problem:** Pull request 6 is not ready to merge. CI fails on Windows, and a task can move files
  from outside its sandbox. F1 to F6 describe each problem.
- **Do:** Work through F1 to F6 in order. Commit every fix on branch
  `int-4-content-addressed-data-plane`, so pull request 6 carries it. This item has no pull request
  of its own. Write the failing test first for F2 to F6. Do not merge, and do not start SEC-1.
- **Done when:** Each fix meets its check. `cargo test -p rusty_grid_cli --test blob_transfer_e2e`
  passes. Every check of pull request 6 is green. The owner has then been asked to approve the
  merge under INT-4.

#### F1: Make clippy pass on Windows

- **Evidence:** Reproduced. CI run `41277852801`, job `clippy (windows-latest)`, fails with
  `large_enum_variant` at `crates/core/src/protocol.rs:1867`, `:1899` and `:1925`. On Windows
  `SubmitTask` holds 385 bytes and the next variant 184 bytes. The same check passes on Linux and
  macOS.
- **Problem:** Clippy stopped at `rusty_grid_core`, so the other crates were not linted on Windows.
- **Do:** Add `#[allow(clippy::large_enum_variant)]` to `ClientMessage`, `HumanClientMessage` and
  `BinaryClientMessage`, as `crates/cli/src/cli_app.rs:124` does. Do not box `Task`, because that
  changes every caller. Then fix whatever the next CI run reports for the other crates.
- **Done when:** `clippy (windows-latest)` is green on pull request 6.

#### F2: Never follow a symbolic link in an output path

- **Evidence:** Reproduced. A task ran `ln -s <OUTSIDE> link` with `--output link/important.txt`.
  The task succeeded, `<OUTSIDE>/important.txt` was gone, and the client received the file.
  `--output 'link/sub/*.txt'` moved every matching file of `<OUTSIDE>/sub` in the same way. A trial
  patch left both directories unchanged.
- **Problem:** `collect_outputs` (`crates/worker/src/staging.rs:150`) calls `symlink_metadata` on the
  whole path, so links in parent directories are followed. `insert_file(path, true)` then renames
  the file into the cache. This breaks goal G6 of design 0006 and the module documentation of
  `staging.rs`. A task that links a shared directory loses files there.
- **Do:**
  1. Before `symlink_metadata(&start)`, check every directory on the way to the literal prefix,
     one component at a time, from the canonical sandbox root. Use `file_type().is_symlink()`.
  2. A link on the way fails a literal pattern with an error that names the pattern.
  3. A link on the way makes a wildcard pattern select nothing.
  4. Add unit tests in `staging.rs` and an end-to-end test in `tests/blob_transfer_e2e.rs`. A Windows
     test skips when the system refuses to create a link.
- **Done when:** Both commands above leave `<OUTSIDE>` unchanged. The literal form fails the task
  with a message that names the pattern. `--output '**/*.txt'` still returns the files of the
  sandbox and nothing from `<OUTSIDE>`.

#### F3: Keep output blobs until the client can fetch them

- **Evidence:** Reproduced. With `master --blob-store-mb 3`, task A wrote a 2 MiB output while its
  client was stopped with `SIGSTOP`. A second task then stored a 2 MiB output. The resumed client of
  task A failed with `output 'a.bin' ... is no longer on the master`.
- **Problem:** `apply_task_result` (`crates/master/src/server.rs:1963`) checks that the outputs exist,
  but nothing pins them. Any later upload can evict them before the client downloads them.
- **Do:**
  1. Pin the output blobs in the same store call that checks them, so the check cannot go stale.
  2. Keep that pin for 10 minutes after the task reaches a terminal state. Release it from
     `spawn_blob_store_sweeper` (`crates/master/src/server.rs:1839`), which today unpins every
     terminal task at once.
  3. Keep the input pins of a task that is retried. Drop the new pin for a stale result.
  4. Put the duration in `BlobStoreConfig`, so a test can shorten it. Add no CLI flag.
  5. State in design 0006 D3 that pinned outputs can hold the store above its bound for that time.
- **Done when:** A test runs the scenario above and the client receives the file. A second test
  shows that the blob can be evicted after the pin ends.

#### F4: Pin output files from the moment they enter the worker cache

- **Evidence:** Reproduced. With `worker --blob-cache-mb 1 --max-concurrency 8`, one task wrote 4000
  files of 1 KiB with `--output 'out/*'` while four clients kept running tasks with small outputs.
  The first attempt failed with `output blob ... is no longer in the worker's cache`.
- **Problem:** `collect_outputs` moves files into the cache (`crates/worker/src/staging.rs:210`). The
  pin comes later, in `upload_task_outputs` (`crates/worker/src/blob_client.rs:236`). Between the
  two, `evict_to_fit` of another task can delete them, and the sandbox copy is already gone.
  `crates/worker/src/blob_cache.rs:14` promises that this never happens.
- **Do:**
  1. Hold a pin from the moment a file enters the cache until its upload ends or is skipped.
  2. `TaskResult` is a wire type and cannot carry the pin, so keep it in the cache or the runner,
     keyed by task id.
  3. Release it on every path, including a suppressed result, a failed collection and a failed
     upload.
- **Done when:** A unit test calls `evict_to_fit` between collection and upload, and the files
  survive. The scenario above passes 20 runs in a row.

#### F5: Keep the blob queue from blocking the master's inbound loop

- **Evidence:** Read. No run showed a visible failure, so this is the lowest risk of the six.
- **Problem:** `serve_worker_blob_request` (`crates/master/src/server.rs:1814`) awaits a send into a
  queue of 32 requests. A worker with five transferring tasks can have 40 requests in flight,
  because `TRANSFER_CONCURRENCY` is 8. The inbound loop then waits for the disk, and so do the
  heartbeats and results of that worker. The wait is short on a healthy disk and long on a stalled
  one. The comment at `crates/master/src/server.rs:1775` says this never happens.
- **Do:**
  1. Add a limit of requests in flight per session to `crates/core/src/blob_transfer.rs`, smaller
     than the queue. Build the master queue from that constant.
  2. Enforce the limit in `BlobClient` with a semaphore. Start `BLOB_REPLY_TIMEOUT` after the permit
     is taken.
  3. Keep the bounded queue for a peer that ignores the limit, and correct the comment.
- **Done when:** A unit test starts 100 concurrent `BlobClient` calls against a fake session. It
  never sees more requests in flight than the limit, and every call finishes.

#### F6: Refuse a mismatched worker and master at registration

- **Evidence:** Reproduced with binaries from `e1215ee` and `ee2d13b`. A new worker on an old master
  registered, and the first task ended the connection with
  `Bincode error: io error: unexpected end of file`. The task failed and the worker reconnected. A
  trial patch with both constants at 2 made each side refuse the other with a message that names
  the versions. The unit tests of `rusty_grid_core` passed on that patch.
- **Problem:** `Task` and `TaskResult` changed layout, but `PROTOCOL_VERSION_CURRENT` and
  `PROTOCOL_VERSION_MIN_COMPATIBLE` (`crates/core/src/protocol.rs:31`) stay at 1. `DEPLOYMENT.md` 3.5
  still says that a worker newer than its master keeps working. An old worker on a new master ran a
  plain command only because bincode ignores trailing bytes. `AssignTaskWithCheckpoint` has a field
  after `task`, so that worker can misread a checkpoint (Read).
- **Do:** Set both constants to 2. Correct `DEPLOYMENT.md` 3.5 and the Compatibility section of
  design 0006: all nodes run one build. Update FEAT-8 to match, and leave the rest of it for later.
- **Done when:** A test shows that the master refuses a version 1 worker. A test shows that the
  worker refuses a `RegisterAck` with version 1.

## Phase 1: stop the harm

These items remove ways to lose data or to run code without authorisation.

### SEC-1: Remove `mcp --sse`

- **Evidence:** Reproduced. A `POST` with `Origin: https://evil.example` and
  `Content-Type: text/plain` to an arbitrary path ran a task through `mcp --sse`.
- **Problem:** `run_sse_server` (`crates/cli/src/mcp.rs:1224`) accepts any `POST` on any path. It
  checks no `Origin`, `Host`, `Content-Type` or token. While the mode runs, a web page in the
  owner's browser can run commands on the cluster with the owner's secret. The mode appears in no
  document. Its default port 8085 is also the `sync_network` probe port.
- **Do:** Delete `--sse`, `--sse-bind` and `run_sse_server`.
- **Done when:** Clap rejects `mcp --sse`, and `git grep run_sse_server` finds nothing.

### SEC-2: Make MCP refuse a remote master without a secret

- **Evidence:** Reproduced. With an empty `HOME`, `mcp --master <REMOTE_IP>:8088` answers
  `initialize` and exits 0. `status` with the same arguments exits 6.
- **Problem:** `run_mcp` (`crates/cli/src/mcp.rs:1339`) resolves the secret but never applies the
  refusal policy. `McpState.insecure` (`mcp.rs:216`) is stored and never read. `submit` therefore
  sends the command, environment, stdin and file contents in clear text. MCP also ignores
  `RUSTY_GRID_INSECURE` and the `[status]` configuration.
- **Do:**
  1. Move `client_secret` and `peer_insecure` from `crates/cli/src/cli_app.rs` to
     `crates/cli/src/secret_cmd.rs`, and call them from `run_mcp`.
  2. Pass the `[status]` configuration to `run_mcp`, and remove `McpState.insecure`.
- **Done when:** With an empty `HOME`, `mcp --master <REMOTE_IP>:8088` exits 6 before it reads
  stdin. With `--insecure` it starts and warns on stderr. `tests/mcp_server_e2e.rs` covers both.

### SEC-3: Delete the Android stub master

- **Evidence:** Read. The stub and the dependency list were confirmed. `rusty_grid_master` is only a
  dev-dependency of `crates/android_bridge`.
- **Problem:** `start_master_impl` (`crates/android_bridge/src/lib.rs:556`) binds `0.0.0.0`.
  `handle_coordinator_stream` (`crates/android_bridge/src/lib.rs:816`) runs no handshake and acknowledges registrations. It
  has no queue, no scheduler and no task dispatch. The "dashboard" returns one hard-coded string.
  `CLAUDE.md`, `PROJECT.md` and `ANDROID_ARCHITECTURE.md` call this a master.
- **Do (default of D2):**
  1. Delete `start_master_impl`, `handle_coordinator_stream`, `nativeStartMaster`,
     `nativeStopMaster`, and the Kotlin `startMaster` and `stopMaster`.
  2. Remove or rewrite the tests that use them. `adversarial_challenge_tests.rs` can start a real
     `MasterServer` through the dev-dependency.
  3. State in the three documents that the app is a worker only.
- **Done when:** `git grep nativeStartMaster` finds nothing, and the gates pass.

### SEC-4: Delete the plain-HTTP secret transfer

- **Evidence:** Read.
- **Problem:** `serve-secret` in `scripts/agent_bridge.py` serves `cluster.secret` over plain HTTP on
  `0.0.0.0:8086` for up to 600 s. Without peer state it serves any client. The secret then crosses
  the LAN unencrypted. Port 8086 and this command are missing from `docs/SECURITY.md` and the
  `README.md` port table.
- **Do (default of D7):** Delete `serve-secret` and `pull-secret`. Point
  `docs/guides/multi-agent-collaboration.md` at `scp` or another channel the owner trusts.
- **Done when:** `git grep serve-secret` finds nothing.

### SEC-5: Run Python and Blender through the hardened runner

- **Evidence:** Read. The missing `env_remove` was confirmed.
- **Problem:** The Python tier (`crates/worker/src/runner.rs:1809-1825`) and Blender
  (`crates/worker/src/autorig/blender_provider.rs:149-173`) build their own commands. Compared with
  `execute_process_supervised`:
  - `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE` and `OXIDE_MESH_TOKEN` reach the child.
  - They get no nice value, no `PR_SET_PDEATHSIG`, and no `CREATE_SUSPENDED`.
  - `terminate_all_task_process_trees` does not see them.
  - `crates/worker/src/job_object.rs` is a second containment implementation, and its `Drop` does
    nothing on Unix, so an aborted future leaves grandchildren alive.
  - Blender's stderr is piped and never read, so it can block.
- **Do:** Run both through `execute_process_supervised`, extended with an output-file step. Delete
  `job_object.rs`.
- **Done when:** A 3D task and a fake-Blender task report the secret variables as unset and run at
  the task nice value. Their grandchildren die on abort on Linux and macOS.

### SEC-6: Restrict Agent Mesh node commands

- **Evidence:** Read.
- **Problem:**
  - The Rust node spawns any `command` string, not only `shell_exec`
    (`crates/agent_mesh/src/executor.rs:174`).
  - The Python node passes no `env=` (`scripts/agent_node.py:359-364`), so `shell_exec` children
    inherit `OXIDE_MESH_TOKEN` and `OXIDESWARM_SECRET`. Its output has no size cap.
  - Any token holder can run commands on every node.
- **Do:**
  1. Restrict the Rust node to the five commands it advertises.
  2. Add `--no-shell` to both runtimes.
  3. In `agent_node.py`, remove the secrets from the environment, cap output at 1 MiB, and honour
     `cwd` and `env`.
- **Done when:** `send --command /bin/ls` returns 127 on both runtimes. `shell_exec env` on a Python
  node shows neither secret. `--no-shell` refuses `shell_exec`.

### SEC-7: Correct public route claims and redact the lease

- **Evidence:** Read.
- **Problem:** `docs/SECURITY.md:25` says every dashboard route needs a token. `GET /`,
  `/api/discovery/probe`, `GET /api/rendezvous/master` and `POST /api/rendezvous/register` need none
  (`crates/master/src/auth.rs:374-377`). The lease read exposes the P2P ticket and addresses. A
  master without a secret accepts unsigned lease writes (`crates/master/src/web_ui.rs:1636-1649`).
  `docs/SECURITY.md:78` says the CLI has no switch for the portal, but `--no-portal` exists.
- **Do:** Remove the ticket from unauthenticated reads. List every public route in
  `docs/SECURITY.md`.
- **Done when:** A test asserts that the anonymous lease response has no ticket.

### SEC-9: Private files on Windows

- **Evidence:** Read; the ACL as reported from the owner's Windows machine. `C:\Users\<name>` grants
  `Everyone:(RX)` and `BUILTIN\Users:(RX)` with inheritance.
- **Problem:** On Unix the cluster secret and `master_key.bin` are created with mode 0600 and
  `ClusterSecret::load_file` warns about a wider mode. On Windows the files inherit the DACL of their
  directory, so every local account can read the secret and the master identity, and nothing warns.
- **Do:** Create each private file (cluster secret, master key, ticket file), the task store
  directory (`--state-dir`) and the `~\.oxideswarm` directory with a protected DACL for the current user, `SYSTEM` and
  `Administrators`, applied by `CreateFileW` at creation. Warn at load, with the `icacls` fix, when
  the DACL grants anyone else. Design 0047.
- **Done when:** A Windows test creates a private file under a parent that grants `Everyone` read
  and reads back a protected DACL with exactly the three allowed SIDs. A pure test covers the
  warning check.

### BUG-1: Stop hard-linking the staging cache

- **Evidence:** Reproduced. Three tasks with the same `--file`: the second appended to it, and the
  third read `original` plus the appended line.
- **Problem:** On a cache hit `Sandbox::stage_files` hard-links the cache object into the sandbox
  (`crates/worker/src/sandbox.rs:483`). The object is not hashed again. A task that edits its input
  in place rewrites the cache, and every later task with that hash gets the changed bytes.
  `set_permissions` at `crates/worker/src/sandbox.rs:510` also changes the shared inode. The payload is already in
  memory, so the link saves no network traffic.
- **Do:** Write `staged.data` to the target, and remove the inline `cache/objects` use. Update
  `docs/design/0003-distributed-file-staging.md`.
- **Done when:** A test stages one file for three tasks, task 2 appends, task 3 reads the original
  bytes, and the file in the sandbox has `nlink` 1.

### BUG-2: Fail a task when a declared output is missing

- **Evidence:** Reproduced. `submit --output nonexistent.txt --output-dir out --wait --command true`
  exits 0 and prints nothing about the file.
- **Problem:** Missing, symlinked, over-size and over-aggregate outputs are skipped with a worker log
  only (`crates/worker/src/runner.rs:1204-1231`). `--file` and `--output` are ignored by GPU,
  `BuiltinTest`, 3D and rig tasks. `README.md:280` shows `--output-dir` without `--output`, which
  collects nothing.
- **Do:** Fail the task with the path and the reason, as design 0006 does for the blob path. Refuse
  these flags on task kinds that ignore them. Correct `README.md:280`.
- **Done when:** A missing or 11 MiB `--output` gives a non-zero result naming the path.
  `--type gpu --file <PATH>` fails before submit.

### BUG-3: Fix the default 3D submit

- **Evidence:** Reproduced. `submit --prompt robot --wait` fails with
  `TaskSpec requires GPU, but TaskRequirements.gpu_required is false`.
- **Problem:** The default engine leaves `engine_preference` as `None`, so `requires_gpu()` is true
  (`crates/core/src/task.rs:1485-1491`). The CLI sets `gpu_required` only from `--gpu` or
  `--type gpu` (`crates/cli/src/cli_app.rs:2302-2309`). CLI tests always pass `--engine sdf`.
  Engine names also disagree across `crates/worker/src/runner.rs:1697`,
  `crates/core/src/task.rs:1489` and `crates/agent_mesh/src/bridge.rs:611`.
- **Do:** Make `auto` CPU-capable. Use one engine-name enum in `crates/core`.
- **Done when:** `submit --prompt robot --wait` succeeds on a CPU worker, with a test.

### BUG-4: Fail only the task when a result is too large

- **Evidence:** Read.
- **Problem:** A result frame above 64 MiB ends the session and cancels every task on that worker
  (`crates/core/src/protocol.rs:177`, `crates/worker/src/client.rs:654`). `asset_bytes` is not
  charged to the retention budget (`crates/master/src/queue.rs:601-610`). The byte budget covers
  terminal tasks only.
- **Do:** Fail the oversized result as one task. Charge `asset_bytes`. Add a byte cap for active
  tasks.
- **Done when:** An oversized result fails only its task, and other tasks on that worker finish.

### 3D-1: Stop reporting the stub as TripoSR

- **Evidence:** Read. `engines/generative_3d/entrypoint.py` was confirmed.
- **Problem:** `entrypoint.py` prints "neural volume" progress with `time.sleep`, loads no model, and
  writes a fixed mesh (`entrypoint.py:45-57`). With the default `auto` it is chosen before the SDF
  tier when Python and the working-directory-relative script resolve
  (`crates/worker/src/runner.rs:1704-1728`). The result is labelled `TripoSR-CUDA` or `TripoSR-CPU`.
  The same request therefore gives an octahedron or a humanoid, depending on the worker's directory.
  The prompt selects one of four fixed shapes, and the image is ignored by the SDF tier
  (`crates/worker/src/gen3d/mod.rs:38`). `scripts/agent_node.py:721` offers engines that have no
  code.
- **Do (default of D3):** Make `entrypoint.py` exit non-zero with the reason. Make `auto` use the SDF
  tier. Take the device label from what ran. Reject `--image` on the SDF tier. Trim
  `agent_node.py:721`.
- **Done when:** `--engine triposr` without a model fails and names the cause.
  `--engine sdf --image <PATH>` fails clearly. A test asserts that no `device_name` contains TripoSR
  unless a model ran.

### BUG-5: Close the remaining gaps in the blob store and cache

- **Evidence:** Read at commit `e1215ee`, except defect 6, which is Reproduced. If a line has moved,
  search for the named symbol.
- **Problem:** The review of pull request 6 found these smaller defects. None of them blocks the
  merge.
  1. Eviction removes a blob from the index under the lock and deletes its file after the lock
     (`delete_objects`, `crates/master/src/blob_store.rs:563`). A concurrent commit of the same id
     can lose its new file while the index still lists it.
  2. Uploads in `tmp/` do not count towards `max_bytes` (`crates/master/src/blob_store.rs:9`), so
     concurrent uploads can exceed the bound.
  3. `walk` skips selected names that are not UTF-8 or not valid blob paths, and it hides read
     errors (`crates/worker/src/staging.rs:234`). The task still succeeds.
  4. A cancelled fetch leaves its `.part` file in the cache `tmp/` directory
     (`crates/worker/src/client.rs:1449`). The file is outside the size accounting, and only a
     worker start removes it, after 24 hours.
  5. `abort_uploads` (`crates/master/src/blob_store.rs:623`) cannot see an upload whose chunk is
     being written, and `append` puts that upload back afterwards. Its file stays until the
     10-minute sweep.
  6. When an output download fails, `submit` returns before it prints the exit code, stdout and
     stderr of the task (`run_submit_with_secret`, `crates/cli/src/cli_app.rs`).
- **Do:** Confirm each defect against the code. Write a failing test for it, then fix it.
- **Done when:** Each defect has a test that failed before the fix.
  `cargo test -p rusty_grid_cli --test blob_transfer_e2e` passes.

## Phase 2: make the tests trustworthy

The full suite must pass on a clean machine before CI can require it.

### TEST-1: Gate host-bound tests

- **Evidence:** Reproduced. See the test baseline.
- **Problem:**
  All paths below are in `crates/worker/tests/`.
  - Tests in `test_challenger_m3_blender_job_object.rs` assert `C:\Program Files\...\blender.exe`.
  - `test_gen3d_pipeline.rs:277`, `test_autorig_adversarial_stress.rs:1226` and
    `test_gen3d_adversarial_stress.rs:693` hard-code an `E:\` path. On Unix the first writes a stray
    file into `crates/worker/`.
  - `test_gen3d_pipeline.rs:99` and `test_gen3d_adversarial_stress.rs:173` use a 50 ms budget in
    debug builds.
  - `test_gen3d_pipeline.rs:289` and `test_gen3d_adversarial_stress.rs:633` spawn a child without its
    own process group, so `kill -9 -pid` misses it.
  - `test_challenger_m2_job_object.rs` needs `python` on `PATH`.
- **Do:**
  1. Use `tempfile` for all output paths.
  2. Gate Windows tests with `#[cfg(windows)]`.
  3. Skip Blender tests without Blender, and add `RUSTY_GRID_REQUIRE_BLENDER`, like the GPU switch.
  4. Use `process_group(0)` on Unix spawns and a `python3` fallback.
  5. Run speed budgets in release builds only, or move them to a benchmark.
- **Done when:** `cargo test -p rusty_grid_worker` passes on macOS and Linux without Blender or an
  `E:` drive. `git grep 'E:.teamwork_projects'` finds nothing in the test files. `git status` is
  clean after the run.

### TEST-2: Isolate tests from the host battery

- **Evidence:** Reproduced. Three tests pass only when `pmset` reports no battery.
- **Problem:** `WorkerClient::new` reads the host battery. On a laptop that gives a 60 s heartbeat.
  Below 15 % the worker refuses every task, so any test that runs a task fails.
  `crates/worker/src/client.rs:2848` and other unit tests share this dependency.
- **Do:** Add a `WorkerConfig` option that disables the host probe, and use it in every test that
  starts a worker. Spawned binaries need a matching environment variable.
- **Done when:** The full suite gives the same result with a fake `pmset` that reports no battery and
  with one that reports `5%; discharging`.

### TEST-3: Repair stale, starved and flaky tests

- **Evidence:** Reproduced failures. The causes below are from reading, so confirm them.
- **Problem:**
  - `test_challenger_cascading_worker_crashes_exhaust_retries`: `ChallengerCluster::spawn` sets only
    `with_max_retries`. A worker loss consumes `max_failovers` (default 10,
    `crates/master/src/queue.rs:183`), so after three crashes the task waits with no workers.
  - `test_capability_queries_under_connect_disconnect_stress`: each query loop ran once in 1.8 s.
    The likely cause is `#[tokio::test]` on one thread, plus blocking hardware probes in
    `WorkerClient::new` (`crates/core/src/capabilities.rs:694`).
  - `test_adversarial_worker_connect_via_master_p2p_ticket`: it allows 5 s for a P2P connection
    through public relays. It also failed on Ubuntu and Windows in the only CI run of the full suite.
  - `test_aead_sessions_roundtrip_tamper_and_replay` in `crates/core/src/protocol.rs` was reported to
    pass for the wrong reason, because it decrypts with the wrong key.
- **Do:**
  1. Set `with_max_failovers` in the cascading test, or define what exhausting retries means.
  2. Time `WorkerClient::new`. If it blocks, move the probes to `spawn_blocking`, and use a
     multi-thread test runtime.
  3. Give the P2P test a longer budget or a local relay.
  4. Make the tamper test fail only because of the tampering.
- **Done when:** Each test passes ten times in a row on an idle machine.

### TEST-4: Stop tests from reading the real home directory

- **Evidence:** Reproduced. These tests pass with an empty `HOME` and fail with a dummy
  `cluster.secret` in `HOME`:
  - `crates/cli/tests/test_cli_3d_submission.rs:13` and `test_cli_3d_submission.rs:87`
  - `crates/cli/tests/test_challenger_m4_cli_adversarial.rs:498`
  - `crates/core/tests/m1_3d_adversarial_stress.rs:740` and `m1_3d_adversarial_stress.rs:852`
  - `tests/test_3d_autorig_pipeline.rs:1306`
- **Problem:** These tests start a client or worker without a private `HOME`. They read the
  developer's real `~/.oxideswarm/cluster.secret`. The two in `m1_3d_adversarial_stress.rs` also
  write `worker_id.*` files there, which breaks the test isolation rule in `CLAUDE.md`.
- **Do:** Give every spawned binary and every in-process CLI run a temporary `HOME` and
  `USERPROFILE`.
- **Done when:** The full suite gives the same result with a dummy `cluster.secret` in `HOME` and
  with an empty `HOME`.

### TEST-5: Register or delete the orphan Rust test

- **Evidence:** Reproduced. A script compared `tests/*.rs` with the `[[test]]` entries.
- **Problem:** `tests/test_challenger_m4_scheduler_runner.rs` is the only file in `tests/` without a
  `[[test]]` entry, so it never runs. Its `WorkerInfo` literal also lacks the `features` field.
- **Do:** Repair and register it in `crates/master/Cargo.toml`, or delete it after 3D-2. Add a check
  that compares `tests/*.rs` with the `[[test]]` entries, and run it in CI.
- **Done when:** The check passes, and CI fails if a file is added without an entry.

### TEST-6: Extend CI to the full suite

- **Evidence:** Reproduced. `.github/workflows/ci.yml` runs unit tests only.
- **Problem:** CI never runs `tests/`, `crates/*/tests/`, `--features gpu-wgpu`, the Python suites, or
  the launcher tests. The only CI run of the full suite was in pull request 2, at an old base, and
  it failed.
- **Do:**
  1. Add a job for `cargo test --workspace --locked --no-fail-fast` on Linux, macOS and Windows (`timeout-minutes: 90`).
  2. Add a launcher job on Ubuntu and macOS that does not build anything, because
     `tests/test_launcher_scripts.py` also scans `target/`. On macOS it sets `export PATH="/bin:$PATH"` so it runs under `/bin/bash` 3.2 instead of Homebrew bash.
  3. Add `actions/setup-python@v7` (`python-version: '3.12'`) with `python -m pip install --disable-pip-version-check websockets` for `tests/test_ecosystem_convergence.rs`.
  4. Add `CARGO_PROFILE_DEV_DEBUG=line-tables-only` to keep test binaries small, and configure `Swatinem/rust-cache@v2` with `save-if: ${{ github.ref == 'refs/heads/master' }}` and `cache-on-failure: true`.
  5. Set `concurrency` to `group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.sha }}` and `cancel-in-progress: ${{ github.event_name == 'pull_request' }}` so pushes to `master` are never cancelled.
  6. Add a clippy and `--lib` job with `-p rusty_grid_cli --features gpu-wgpu`.
  7. Add `tests/test_agent_mesh_rust_hub_cross_interop.py`, which tests the Rust hub.
  8. Delete `origin/claude/trusting-wing-d9a27c` once the full CI file is in place.
- **Done when:** All jobs are required and green on `master`.

### TEST-7: Run or delete the orphan Python tests

- **Evidence:** Read.
- **Problem:** Seventeen Python files in `tests/` have no runner and no reference, for example
  `test_m7_empirical_challenge.py` and `test_challenger_m5_rapid_disconnect.py`.
  `tests/run_all_mesh_tests.py` calls itself authoritative and tests a Python reference hub only.
  `tests/verify_p2p_persistence.sh` is unreferenced.
- **Do:** Run each file. Add those with value to a runner, and delete the rest. Rename the mesh
  runner to say that it is a simulation.
- **Done when:** Every file in `tests/` is named by a runner or a `[[test]]` entry.

### TEST-19: Fix flaky required tests

- **Evidence:** CI failures of 2026-11-09 and full-suite runs on macOS; each test passed when rerun.
- **Problem:**
  - `queue::tests::rvb_drop_submit_future_mid_ack_loses_task_from_ready_queue` slept 20 ms and
    then aborted the spawned `submit`, assuming it had reached its durable-ack wait. On a loaded
    host the task had not run yet, so the abort left no task at all. It then slept a fixed 300 ms
    for the detached ack task to publish the task.
  - `store::tests::classifying_a_damaged_tail_is_linear` asserted a wall-clock bound (1 s) on
    opening an 8 MiB damaged journal, which fails when the machine is busy.
  - `e2e_cluster::test_tier2_bva_large_output_stream_truncation` waited 5 s for MSYS2 `sh` on a
    Windows runner; the work is tiny, the process start is slow under load.
- **Do:** The first test waits for the task to appear before it aborts, and polls (20 s bound) for
  it to reach the ready queue. The second counts probes and hashed bytes (`ScanWork`) on inputs of
  N and 4N bytes and asserts the algorithm's own bounds. The third waits up to 60 s, with a 40 s
  task timeout, and keeps every assertion. No production code changed.
- **Done when:** Each test passes 50 times in a row under a CPU load, and the Windows jobs pass
  on three runs.

### TEST-20: Fix Windows disconnect stress timeouts

- **Evidence:** CI failures on `windows-latest` in `tests/m4_challenger_2_stress.rs` (for example `master` `5c9e126`, job `129153917725`): `test_adversarial_worker_disconnect_multi_tasks_in_flight` and `test_adversarial_cascading_worker_disconnects_failover` timed out after 12 s in `wait_task` while all nine other tests in the binary finished in under 5 s.
- **Problem:**
  1. `WorkerClient::handle_assign_task_with_checkpoint` emits `TaskProgress { status: TaskStatus::Running }` before `Sandbox::create` and before `configured.spawn`. When a disconnect test aborts the worker as soon as `TaskState::Running` is observed, `WorkerClient::drop` sends `"WorkerClient dropped"` on the task cancellation channel, but `execute_command_workload`, `execute_shell_script_workload`, `execute_rust_compilation_workload`, and `execute_process_supervised_with_workspace` did not check `cancel_rx` before spawning the child process. The dropped worker therefore still spawned `sh.exe` and immediately terminated its Job Object milliseconds later.
  2. When `sh.exe -c "sleep ..."` was already running at abort time, `TreeInner::kill_and_release_once` waited only `MIN_LIVE_KILL_AGE = 80 ms` (and `TreeInner::drop` passed `child_already_exited = true`, skipping the wait altogether). Under 11-way parallel test load on Windows, `80 ms` lands while `sh.exe` is inside MSYS2 `child_info_spawn` starting `sleep.exe` (`msys-2.0.dll` `dll_crt0_1`), stalling MSYS2's global shared-memory initialization lock for 15 s and freezing the surviving `sh.exe` children on the failover workers past the 12 s `wait_task` budget.
  3. `spawn_test_worker` in `tests/m4_challenger_2_stress.rs` and `WorkerClient::for_testing` did not pass `.with_ram_mb(...)`, so every worker spawn ran `sysinfo::System::new()` + `refresh_memory()` synchronously on the single-threaded test runtime.
- **Do:**
  1. Check `cancel_rx` via `check_cancelled_now` before `Sandbox::create` in `execute_command_workload`, `execute_shell_script_workload`, and `execute_rust_compilation_workload`, and both at entry and immediately before `configured.spawn` in `execute_process_supervised_with_workspace`.
  2. Raise `MIN_LIVE_KILL_AGE` in `crates/worker/src/process_tree.rs` from `80 ms` to `250 ms` so both `sh.exe` and its immediate MSYS2 child finish `dll_crt0_1` before `TerminateJobObject`, and pass `child_already_exited = false` in `TreeInner::drop`.
  3. Set `.with_ram_mb(4096)` in `WorkerClient::for_testing` and `spawn_test_worker`.
- **Done when:** `cargo test -p rusty_grid_master --test m4_challenger_2_stress --locked` passes 20 times in a row on Windows inside the full-suite lock, and `runner::tests::test_runner_pre_cancelled_task_does_not_spawn_child` verifies that a pre-cancelled task does not spawn a child process.

### TEST-22: Ignore UUID and metric `4090` tokens in `workflow_modes_test`

- **Evidence:** CI failure on `macos-latest` (run `42690928796`, job `129410135311`) in `tests/workflow_modes_test.rs:313` (`test_research_mode_distributed_and_report`): `output must not print invented RTX / worker-gpu-rtx4090 hardware or node names`.
- **Problem:** `generate_research_report` writes `expected_task_id` and `codec_decoded_task_id` (random UUIDv4 strings `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`) plus integer RAM/timing metrics into `target/research_report.json`. `contains_standalone_number` only checked `c.is_ascii_digit() || c == '.'` on the characters immediately before and after `"4090"`, so a UUIDv4 with `-4090-` in its third hyphen-delimited group (or `4090` adjacent to hex digits `'a'..='f'`), or a JSON/unit-labelled metric equal to `4090`, was mistaken for an `"RTX 4090"` GPU model name.
- **Do:** Teach `contains_standalone_number` in `tests/workflow_modes_test.rs` to ignore `"4090"` when adjacent to ASCII hex digits, inside a hyphen-delimited UUID hex group (`-4090-`), or formatted as a JSON numeric value (`: 4090`) or unit-suffixed measurement (`4090 ms`, `4090 mb`, `4090 ops/sec`, `4090 tasks`), while still matching standalone GPU model strings (`"gpu: geforce 4090 ti"`, `"nvidia rtx 4090"`, `"worker-gpu-rtx4090"`, `"4090"`).
- **Done when:** `test_standalone_number_check_ignores_digits_inside_measurements` and `test_research_mode_distributed_and_report` pass in `cargo test -p rusty_grid_cli --test workflow_modes_test --locked`.

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

## Phase 3: make the documents true

The standard in `docs/README.md` requires "only what the code does". These items apply it.

### TEST-27: Find why the `mesh_auth` slow-consumer test times out on macOS

- **Evidence:** Reproduced once in CI. The `master` push of `6a88eff` on 2026-11-15 failed in
  `workspace tests (macos-latest)` (run `43735264003`, job `132876345396`):
  `test_slow_consumer_is_disconnected_instead_of_buffering_forever` in
  `crates/agent_mesh/tests/mesh_auth.rs` printed "victim disconnected after 20 x 100 KiB sends",
  then the `ping` to the healthy node, a `send_command` with a 3000 ms deadline, ended in
  `Elapsed(())` (line 873). The runs of the next two pushes (`3413fb2`, `96ce571`) passed.
- **Problem:** The test checks that a slow consumer does not hold up other peers. A timeout at this
  line is either a busy runner or the delay the test exists to catch, and nothing in the output
  tells the two apart.
- **Do:**
  1. Run the test 50 times on `macos-latest` and record how often it fails.
  2. Print the time from the victim's removal to the healthy node's answer, so a failure shows
     whether the time went to the flooder's socket, the hub's writer to the healthy node, or the
     healthy node itself.
  3. If the delay is in the hub, fix it there. If it is the runner, raise the deadline by the
     measured margin, with a comment that names this item.
- **Done when:** 50 runs in a row pass on `macos-latest`, and the test prints that latency.
- **Partial (2026-11-16, pull request 263):** not reproduced in 60 local runs on macOS (30 plain, 30
  with 12 CPU-burning processes). Steps 2 and 3 are done: the healthy-node ping now goes through a
  separate client with a 10 s deadline, the dead-peer NACK check has 30 s, and both latencies since
  the victim's removal are printed (at most about 8.7 ms under load). Step 1, 50 runs on
  `macos-latest`, is not done.

### DOC-1: Correct false status claims

- **Evidence:** Reproduced or read, per row.
- **Problem:** `README.md` and `PROJECT.md` use statuses outside the fixed vocabulary, and several
  are false.

| Location | Claim | Correction |
|---|---|---|
| `README.md:103` | Automatic failover: Completed | Design only. `README.md:81` already says there is none |
| `README.md:94`, `README.md:104` | Tools `oxideswarm_status` and others, including cancel | The tools are `status`, `workers`, `submit`, `task_result`, `doctor` |
| `README.md:106`, `README.md:179`, `CLAUDE.md:86`, `DEPLOYMENT.md:256`, `PROJECT.md:96`, `ANDROID_ARCHITECTURE.md:20` | `nativeStartWorkerWithSecret` | This export does not exist. The path is `nativeSetClusterSecret` |
| `README.md:110`, `docs/SECURITY.md:152`, `PROJECT.md:93` | Containment guarantees cleanup | True for Windows Job Objects. On Linux it covers the direct child. macOS has nothing |
| `README.md:87`, `README.md:109` | Reassigned workers resume | `BuiltinTest` only |
| `README.md:111` | Version negotiation prevents frame drops across all components | Partial, see FEAT-8 |
| `README.md:82`, `README.md:108` | Discovery fallback: Completed | Partial, see FEAT-7 |
| `README.md:79`, `README.md:80` | Hardware-verified, launcher tests | No PowerShell, cmd or VBS file runs in any test. Replace with a dated per-platform table |
| `README.md:78` | Your own mapper and reducer | Not reachable from the CLI, see MR-1 |
| `PROJECT.md:89` | LRU and TTL eviction | Finish-order eviction with count and size bounds. TTL is off |
| `PROJECT.md:88` | Least-loaded and round-robin | Only weighted least-loaded runs |
| `PROJECT.md:98` | Bridge routes to a live master | It starts its own embedded master |
| `PROJECT.md:113` | 100 % test pass | Remove |
| `CLAUDE.md:75`, `docs/design/0002-mobile-coordination.md:81` | `AdaptiveKeepaliveController` | No such type exists |
| `packaging/android/README.md:39` | Production-grade, turnkey app | The app does not compile |

- **Do:**
  1. Apply the table.
  2. Map every status to Implemented, Partial, Planned or Design only.
  3. Restore a limitation row that says delivery is at-least-once and that a task can run twice.
- **Done when:** A grep for `Completed`, `Done`, `Hardened` and `COMPLETED` in status cells finds
  nothing. No document contradicts `README.md:81`.

### DOC-2: Remove unbacked performance numbers

- **Evidence:** Reproduced. The source report is outside the repository.
- **Problem:** `README.md:85` calls 341.3 MB/s and "1.55x scaling" an empirical baseline. The source
  compared the cluster only with the phone alone. The same report shows the cluster at 0.37x of the
  Mac alone, at 30 % parallel efficiency. No log or report is in the repository. `README.md:32` and
  `windows/README.md:11` give memory figures without a script.
- **Do:** Delete the numbers, or commit a generated report under `benchmarks/` with hardware, commit
  and date, compared with the fastest single node.
- **Done when:** `git grep 341.3` finds nothing, or it points at a committed report.

### DOC-3: Align design records with the code

- **Evidence:** Read.
- **Problem:**
  - `docs/design/0002-mobile-coordination.md` names `evaluate_effective_concurrency`,
    `compare_node_authority` and `evacuate_before_sleep`, which do not exist. Its battery tiers
    (30, 15, 5) differ from the code (50, 30, 15). Its weights and tie-break differ from
    `crates/core/src/arbitrage.rs`.
  - `docs/design/0003-distributed-file-staging.md` names `SandboxManager::stage_file`,
    `OXIDESWARM_MAX_RETAINED_MB` and `CapabilityMismatch`, which do not exist. It calls inline
    staging content-addressed, but the payload travels in every task message.
  - `docs/design/0004-mcp-and-discovery.md` names arguments the server ignores.
  - `packaging/android/README.md:222` gives 75 degrees C, while the code uses 37, 40 and 42.
    `packaging/android/README.md:126` still gives 3 s heartbeats.
  - All four records say Implemented.
- **Do:** Correct each record from the code. Set the record status and work items to what is true.
- **Done when:** Every symbol in `docs/design` resolves in the code.

### DOC-4: Complete the CLI and environment reference

- **Evidence:** Read.
- **Problem:** `docs/README.md` says flags and variables are listed in `README.md`. About 58 of 96
  long flags and 26 environment variables are missing. Port 8086 is missing. `scripts/mode.sh:275`
  and `windows/switch_role.cmd:50` are not in English.
- **Do:** Complete the tables. Add a test that walks `Cli::command()` and fails if a long flag is
  missing from `README.md`.
- **Done when:** The test passes.

### DOC-5: Document the 3D subsystem

- **Status:** Implemented (`win/doc-5-3d-subsystem`, design `0023`)
- **Evidence:** Reproduced. A grep of all tracked Markdown finds no mention.
- **Problem:** `crates/worker/src/gen3d`, `crates/worker/src/autorig`, `TaskSpec::Generative3D`,
  `TaskSpec::AutoRig` and `engines/` have no design record and no `README.md` section. No installer
  ships `engines/`, and `crates/worker/src/gen3d/runner.rs:128-132` finds the script by a relative path.
- **Do:** Add a design record, a `README.md` section and a `CLAUDE.md` entry. State the limits. Ship
  `engines/` or set `OXIDESWARM_GEN3D_SCRIPT` in the installers.
- **Done when:** `CLAUDE.md` and a design record describe both modules.

### DOC-6: Correct the minimum Rust version

- **Evidence:** Reproduced. `iroh` 1.2.0 and 1.4.1 both declare `rust-version = "1.91"`.
- **Problem:** `Cargo.toml:17`, `CLAUDE.md:7` and `docs/guides/two-machine-cluster.md:11` say 1.75.
  No crate inherits the value, so nothing enforces it.
- **Do:** Set `rust-version = "1.91"`, inherit it in all crates, and correct the documents.
- **Done when:** `cargo metadata` reports `rust_version` 1.91 for every workspace package.

### DOC-7: Add an MCP client setup guide

- **Evidence:** Read. A search for `mcpServers` and `claude mcp add` finds nothing relevant.
- **Problem:** No document shows how to connect a client. `docs/SECURITY.md` does not mention MCP.
- **Do:** Add `docs/guides/mcp-client-setup.md` with snippets for Claude Code, Codex and Cursor. Add
  an MCP row to `docs/SECURITY.md`.
- **Done when:** Each snippet was run once, and the guide follows the guide template.

### DOC-8: Update `CLAUDE.md` and `SECURITY.md` for Windows isolation

- **Evidence:** Read after pull request 152 (`FEAT-3c`) merged.
- **Problem:** Two documents still described the state before pull request 152 (`FEAT-3c`):
  - `CLAUDE.md` omitted that `cargo` under the Windows AppContainer (`--isolation fs|fs+net`) is
    `Planned` as `FEAT-3d` and that a Windows worker refuses `--workspaces` with isolation inside the
    `isolation.rs` summary;
  - `docs/SECURITY.md` listed task sandboxing only under "Not considered / not provided" (section 2)
    and omitted `--isolation off|fs|fs+net` (`crates/worker/src/isolation.rs`, design 0033) from
    section 7 ("Who runs the tasks").
- **Do:** Update `CLAUDE.md` and `docs/SECURITY.md` to match `crates/worker/src/isolation.rs`:
  Windows `--isolation fs|fs+net` uses a per-task Win32 AppContainer (`FEAT-3c`), `cargo` under it is
  `Planned` as `FEAT-3d`, and a Windows worker refuses `--workspaces` with `--isolation fs|fs+net`.
- **Done when:** Neither `CLAUDE.md` nor `docs/SECURITY.md` claims that Windows AppContainer
  isolation is unimplemented or that task sandboxing is not provided when `--isolation fs|fs+net` is
  enabled, and both state the `FEAT-3d` (`cargo`) and `--workspaces` limits on Windows.

- **Note (2026-11-10):** FEAT-3d (pull request 183) merged before DOC-8, so the merged documents
  state the FEAT-3d behaviour (cargo and workspaces under the Windows AppContainer on a worker that
  is not elevated) instead of the limits above.

### REL-1: Restore the Apache licence text

- **Evidence:** Reproduced. `LICENSE-APACHE` is 948 bytes, and sections 2 to 9 are missing.
- **Problem:** The repository is public and offers Apache-2.0 without the licence terms.
- **Do:** Replace the file with the verbatim text from `apache.org/licenses/LICENSE-2.0.txt`.
- **Done when:** The file matches that text.

### REL-2: Remove owner data from tracked files

- **Evidence:** Read.
- **Problem:** `docs/README.md` forbids real addresses and hostnames. The public tree contains owner
  paths, LAN addresses and device identifiers, including in
  `scripts/sync_network/ping_success.txt`, `scripts/run_milestones_1_and_2.ps1:174`,
  `scripts/agent_bridge.py:175`, `tests/test_launcher_scripts.py:416` and
  `ANDROID_ARCHITECTURE.md:654`. The guard test scans only shell, cmd, PowerShell and VBS files.
- **Do (default of D5):**
  1. Delete the two script files, or sanitise them.
  2. Store hashes in the guard instead of the values, and extend it to all tracked text.
  3. Replace addresses with placeholders or RFC 5737 values.
- **Done when:** The extended guard passes.

### REL-3: Small repository hygiene

- **Status:** Implemented (`win/rel-3-repo-hygiene`)
- **Evidence:** Reproduced.
- **Problem:** `run_chaos_test.sh` writes reports to `test_reports/`, which is not in `.gitignore`.
  `.so` files under `jniLibs` are not ignored.
- **Do:** Ignore both paths.
- **Done when:** `git status --short` is empty after `./run_chaos_test.sh`.

## Phase 4: finish partial features

### CLI-1: Add `cancel` and `result` commands

- **Evidence:** Reproduced. The help output has neither. After `submit` without `--wait`, `status`
  shows no stdout.
- **Problem:** `ClientMessage::CancelTask` and `GetTaskResult` exist, but only `doctor` uses them. A
  user cannot cancel a task or read the output of a task submitted without `--wait`.
- **Do:** Add `oxideswarm cancel <ID>` and `oxideswarm result <ID> [--wait] [--json] [--output-dir]`.
- **Done when:** An end-to-end test cancels a running `sleep`, and another reads a finished result.

### MCP-1: Make the MCP server concurrent, with deadlines

- **Evidence:** Read.
- **Problem:** `run_stdio_server` awaits `call_tool` inside its read loop (`crates/cli/src/mcp.rs:1101`,
  `mcp.rs:1172`). While a tool runs, `ping` and `notifications/cancelled` are not read. The wait path
  has no deadline (`mcp.rs:844`). `submit {wait:true}` with no matching worker blocks the server.
  There is no `cancel` tool. Tool defaults set `max_retries` to 0 (`mcp.rs:765`), so a worker drop
  fails the task.
- **Do:**
  1. Use a reader task, a writer task and one spawned task per `tools/call`.
  2. Make `submit` wait against a client deadline, and return `timed_out: true` with the task id.
  3. Add a `cancel` tool, and honour `notifications/cancelled`.
  4. Warn when no worker matches.
  5. Send `notifications/progress` when the client passes a token.
- **Done when:** A test sends `submit {wait:true, require_tags:["absent"], wait_secs:2}` and `ping`.
  `ping` is answered first, and `submit` returns a task id with `timed_out: true`.

### MCP-2: Return outputs safely to an LLM

- **Evidence:** Read.
- **Problem:** `task_result` returns only path, size and hash for artifacts. `data_preview` is the
  whole file through `to_string_lossy` (`mcp.rs:854`), so binaries are corrupted. stdout and stderr
  return up to 10 MiB each, and there is no truncation field. The worker keeps the first 10 MiB
  (`crates/worker/src/runner.rs:278`), so a `cargo test` summary at the end is lost. MCP still sends
  inline files after INT-4.
- **Do:**
  1. Add `output_dir` and `max_output_bytes`, with a default near 16 KiB, head plus tail.
  2. Return `truncated` and `total_bytes`, and remove `data_preview`.
  3. Make `StreamCapture` keep head and tail.
  4. Move MCP file transfer to blobs.
- **Done when:** A task writes a 1 MiB binary and 20 MiB of stdout ending in a marker.
  `task_result {output_dir, max_output_bytes:4096}` gives a file with the right hash, a reply under
  8 KiB, and the marker.

### MCP-3: Correct JSON-RPC and argument handling

- **Evidence:** Read.
- **Problem:** An error is sent in reply to a notification. Valid JSON that is not a request returns
  -32700. An unknown tool returns `isError` instead of -32602. A missing `command` runs a demo echo
  and reports success (`mcp.rs:716`). An unknown `type` becomes a command. `status` hard-codes
  `"cluster_status":"ok"` (`mcp.rs:545`). A failed `doctor` is `isError:false`.
- **Do:** Drop messages without an id silently. Separate -32700 from -32600. Return -32602 for bad
  tools and arguments. Add `required` to the schemas. Use `ClusterStatus`. Align design 0004.
- **Done when:** A test covers `notifications/cancelled` (no output), a missing `method` (-32600) and
  `submit {}` (an error, nothing queued). Update `tests/m5c_challenger_1_stress.rs:326`, which pins
  the old code.

### SCHED-1: Respect `--max-concurrency` on the master

- **Evidence:** Read.
- **Problem:** The worker limits a semaphore (`crates/worker/src/client.rs:874`), but the master
  counts `cpu_cores` (`crates/master/src/scheduler.rs:127`). A worker with `--max-concurrency 1` on 8
  cores receives up to 8 tasks. Seven wait in state `Scheduled` and cannot move to an idle worker.
- **Do:** Add a reserved `slots:<n>` worker tag, which needs no wire change. Use it in
  `effective_max_concurrency`.
- **Done when:** A scheduler test with `slots:2` and an idle peer sends the overflow to the peer.

### SCHED-2: Wire or delete dead flags

- **Evidence:** Read. Confirmed by grep.
- **Problem:**
  - `--max-queue-size` and `--scheduler-policy` are parsed and never read. Queue capacity is
    hard-coded to `None`, so `QueueFull` is unreachable.
  - `RUSTY_GRID_WIRE_CODEC` and config `wire_codec` can never win over the clap default.
  - `[submit] timeout_secs` is never read.
  - TTL eviction code exists, but nothing enables it (`crates/master/src/reaper.rs:45`).
  - A config `p2p_ticket` beats an explicit `--master`.
- **Do:** Wire each one or delete it. Make `wire_codec` optional with a hard error on bad values.
- **Done when:** `--max-queue-size 2` rejects the third submit in a test.
  `RUSTY_GRID_WIRE_CODEC=json` takes effect in a test.

### SCHED-3: Give impossible tasks an outcome

- **Evidence:** Reproduced. `submit --require-tag nothing --wait` warns and then waits without limit.
- **Problem:** `fail_unfulfillable_tasks` has no flag, and it only fills a report that the server
  discards (`crates/master/src/scheduler.rs:111`).
- **Do:** Add a queue timeout, or fail unplaceable tasks when configured.
- **Done when:** An unplaceable task ends Failed when configured, with a test.

### MOB-1: Fix the adaptive keepalive

- **Evidence:** Reproduced. A MacBook worker ignored a 1 s master interval.
- **Problem:** `recommended_keepalive_interval` returns 60 s for any non-cellular link, even with
  active tasks (`crates/core/src/capabilities.rs:180`). Design 0002 says 10 s when active. Laptops
  have no network type, so every battery-powered worker heartbeats at 60 s. The master's dead-worker
  window becomes 240 s. Backpressure reads CPU load from heartbeats, so a laptop can stay
  unschedulable for up to 60 s.
- **Do:** Return 10 s when `active_tasks > 0` on any link. Keep the negotiated interval when the
  network type is unknown. Update `crates/worker/src/heartbeat.rs`,
  `crates/worker/src/client.rs:1937` and `crates/master/src/registry.rs`.
- **Done when:** Unit tests cover Wi-Fi with one task (10 s) and a laptop (negotiated interval). A
  reaper test reaps a battery worker at four times its real interval.

### MOB-2: Keep thermal state on the master

- **Evidence:** Read. `with_live_status` and `live_status` were confirmed.
- **Problem:** `with_live_status` keeps three fields (`crates/core/src/capabilities.rs:71`), and
  `live_status` returns temperature, tiers and network as `None` (`capabilities.rs:82`). The master
  therefore discards what a phone reports, and thermal tiers above Warm can never apply. A phone at
  45 degrees C keeps getting work. A cellular phone's interval resets to 60 s at its next heartbeat.
  `detect_battery` has no Windows branch.
- **Do:** Store the last `MobileLiveStatus` on `WorkerEntry` and `WorkerInfo`, and use it in
  `Engine::new` and `candidates`. Add or document Windows and macOS sensing.
- **Done when:** Telemetry with `thermal_tier: Critical` yields no assignment, `Hot` yields at most
  one slot, and a cellular interval stays 25 s across heartbeats.

### MOB-3: Do not charge evacuation to retry budgets

- **Evidence:** Read.
- **Problem:** Design 0002 says an evacuation does not increase retry counts. `immediate_reschedule`
  removes only the delay, and both counters still increase (`crates/master/src/queue.rs:1669`).
  `tests/mobile_coordination_test.rs:946` asserts the opposite of the design.
- **Do:** Add an uncharged path for announced drains. Correct the test.
- **Done when:** Repeated evacuations leave `failover_count` at 0.

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

### MR-1: Let a CLI user ship a mapper and reducer

- **Evidence:** Read.
- **Problem:** A path passed to `mapreduce` becomes `ShellScript { script: <the path> }`
  (`crates/cli/src/cli_app.rs:2778`). The file is never read, so it must already exist on every
  worker. `--help` names `uppercase`, which is unsupported. `max` and `min` ignore floats.
- **Do:** Read the file into `script`. Add `--map-cmd` and `--reduce-cmd`. Handle workers without `sh`
  with a clear error.
- **Done when:** `mapreduce --mapper ./local.sh --reducer ./sum.sh` succeeds on a worker that lacks
  those files, in a registered end-to-end test.

### MESH-1: Make `wss://` work in the Rust mesh client

- **Evidence:** Read. `Cargo.toml:33` enables no TLS feature for `tokio-tungstenite`.
- **Problem:** `README.md:467`, `DEPLOYMENT.md:9` and `crates/agent_mesh/src/main.rs:294` advise
  `wss://`, but the Rust client cannot open it. The hub token then travels in clear text.
- **Do:** Enable a rustls feature in `Cargo.toml`, or state that Rust nodes need a VPN or SSH tunnel.
- **Done when:** A test connects the Rust client through a TLS-terminating proxy, and `cargo audit`
  passes.

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

### MODE-1: Make `ox-mode` report only what happened

- **Evidence:** Read.
- **Problem:**
  - `research distributed` is an in-memory simulation with invented hardware names
    (`run_distributed_benchmark`, `crates/core/src/mode.rs:1083`). It prints
    `PASSED - OPTIMAL DISPATCH`.
  - `research bench` runs a `Vec` loop as its "codec" benchmark.
  - `dev cluster` and `dev stop` return success with canned text in the dashboard path.
  - `dev status` reports healthy against a master that answers 401.
  - `doc export` is a template with false facts (16 MB frames, JSON payload).
  - `doc verify` promises link checks that do not exist.
- **Do:** Label or remove the simulation. Return failure for actions that cannot run. Check HTTP
  status. Generate or delete `doc export`. Move the engine out of `crates/core`.
- **Done when:** `rusty_grid_core` has no `Command::new("cargo")`, no output prints `OPTIMAL` or
  invented hardware, and `dev status` against a protected master exits non-zero.

### BRIDGE-1: Repair `agent_bridge.py diagnose`

- **Evidence:** Read.
- **Problem:** `diagnose` prints `HEALTHY` when peer state is missing, never checks its age, and
  always exits 0. Identity comes from the platform, so two Macs overwrite one state file. Owner
  paths and a private gateway address are hard-coded. `tests/test_agent_bridge.py` covers `watch`,
  `peek` and `send`, but not `diagnose`.
- **Do:** Report `UNKNOWN` for missing or stale peers, and exit non-zero on problems. Add
  `--agent-id`. Remove owner paths. Add tests for `diagnose`.
- **Done when:** `diagnose` without a peer exits non-zero, and two simulated agents on one platform
  do not collide.

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

### FEAT-1: Master durability

- **Evidence:** Read. No journal or store exists in `crates/master`.
- **Problem:** The queue and registry are in-memory (`crates/master/src/queue.rs:516`). A master
  restart loses queued tasks, running tasks and finished results. `Register` carries no task list.
  Workers cancel all running tasks when any session ends (`crates/worker/src/client.rs:1144`), so a
  short network drop restarts tasks from zero.
- **Do:**
  1. Add `crates/master/src/store.rs` with an append-only journal for submit, state changes,
     checkpoints and terminal results, and `--state-dir`.
  2. Replay it at start. Scheduled and Running tasks become Queued without charge.
  3. Add a worker grace period and a `ResumeSession { running }` message, gated by a feature
     announcement, so a master can adopt running tasks.
- **Done when:** `tests/master_restart_recovery.rs` kills a master holding one running, two queued
  and one finished task, and restarts it. The result reads back, queued tasks finish, and the running
  task finishes exactly once.

### FEAT-2: Containment after a worker crash

- **Evidence:** Read. `run_chaos_test.sh:309` states that the orphaned shell keeps running.
- **Problem:** `PR_SET_PDEATHSIG` is compiled for `target_os = "linux"` only
  (`crates/worker/src/process_tree.rs:128`), and it covers the direct child. macOS and Android have
  nothing. After `kill -9` of a worker, the task keeps running while the master runs it elsewhere. A
  process that calls `setsid()` also escapes `killpg`.
- **Do:** Add a small supervisor in each task group that holds a pipe from the worker and runs
  `killpg` on end of file. On Linux add `PR_SET_CHILD_SUBREAPER` or cgroup v2.
- **Done when:** A test starts the real worker, runs a script with a child and a grandchild, sends
  `kill -9`, and asserts that all are dead within 5 s on Linux and macOS. `run_chaos_test.sh` fails
  if the victim writes `done:` after the kill.

### FEAT-3: OS-level task isolation

- **Evidence:** Read. The worker uses none of `setuid`, Landlock, seccomp, `sandbox-exec`,
  AppContainer or cgroups.
- **Problem:** The sandbox is a directory with mode 0700. A task can read `cluster.secret`, other
  sandboxes and the cache. Network, memory, disk and process count are unrestricted. This matters
  more when coding agents submit tasks.
- **Do:** Add `--isolation off|fs|fs+net` in `configure_command`. Use Landlock and namespaces on Linux,
  `sandbox-exec` on macOS, and AppContainer or a restricted token on Windows. Advertise an `iso:` tag.
- **Done when:** With `fs`, a probe that reads `cluster.secret` fails on each OS and writing the
  sandbox works. With `fs+net`, an outbound connection fails. `doctor` reports the level.

### FEAT-3d: cargo under Windows isolation

- **Evidence:** Reproduced on `windows-latest` (`Windows Server 2025`, CI run `42608546025`, job
  `129130623354`, commit `c9891fe`). Round 10/11 diagnostics inside the confined AppContainer task
  (`tiny`) proved:
  1. `icacls` on `<rustup_home>\toolchains\staged-x86_64-pc-windows-msvc\bin` and `bin\rustc.exe` shows
     both `oxideswarm.toolchain` (`S-1-15-3-1024-...`) and `CurrentUser` carry `(I)(OI)(CI)(RX)` on
     `bin` and `(I)(RX)` on `rustc.exe` after `SetNamedSecurityInfoW` with
     `UNPROTECTED_DACL_SECURITY_INFORMATION`.
  2. Job object basic limits queried via `QueryInformationJobObject` are `LimitFlags=0x2000`
     (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` only) and `ActiveProcessLimit=0`.
  3. The spawn working directory `tiny` is readable and traversable (`DIAG_READ_DIR_CWD=Ok(2)`), and
     anonymous pipe creation succeeds (`DIAG_ANON_PIPE=Ok("OK")`).
  4. Spawning `rustc.exe -vV` directly from inside the AppContainer succeeds (`ExitStatus(0)`, prints
     `rustc 1.96.0 (ac68faa20 2026-05-25)`) when `stdin` is `Stdio::inherit()`
     (`DIAG_SPAWN_INHERIT=Ok(ExitStatus(0))` and `DIAG_SPAWN_STDIO_PIPED=Ok(ExitStatus(0))`).
  5. Opening `"NUL"` (`\Device\Null` / `\\.\GLOBALROOT\Device\Null`) from inside the AppContainer
     under an elevated administrator session (`runneradmin` on `windows-latest`) fails Stage 1 of
     `SepAccessCheck` with `ERROR_ACCESS_DENIED (os error 5)`
     (`DIAG_OPEN_NUL_READ=Err(Os { code: 5 })`, `DIAG_OPEN_NUL_WRITE=Err(Os { code: 5 })`,
     `DIAG_OPEN_GLOBALROOT_NULL=Err(Os { code: 5 })`). Because Rust's `std::process::Command::output()`
     (used by `cargo` when running `rustc -vV`, even with `RUSTC` and `CARGO_BUILD_JOBS=1` set) opens
     `"NUL"` (`Stdio::null()`) for `stdin` inside `cargo.exe` before calling `CreateProcessW`, every
     spawn that uses `Stdio::null()` (`DIAG_SPAWN_STDIN_NULL`, `DIAG_SPAWN_FULL_OUTPUT`, `cargo -vV`,
     `cargo build --offline`) fails with `Access is denied. (os error 5)` (`never executed`).
- **Problem:** On Windows, `cargo` builds under `--isolation fs|fs+net` fail when `cargo` spawns
  `rustc -vV` with `Stdio::null()`, and `--workspaces` combined with `--isolation fs|fs+net` is
  refused on Windows (design 0033 D4, design 0039 W2b).
- **Do:** Enable `cargo` builds (and `--workspaces` with `--isolation fs|fs+net`) inside a Windows
  AppContainer without editing Object Manager namespace DACLs (`\??`, `\GLOBAL??`, `\Device\Null`) or
  adding `BUILTIN\Users` / `Everyone` to user or toolchain files, and un-ignore
  `test_cargo_build_offline_works_under_fs` and
  `test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home` on Windows.
- **Done when:** Both `test_cargo_build_offline_works_under_fs` and
  `test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home` pass un-ignored in
  `test_feat3_os_isolation` on `windows-latest`, and workspaces with `--isolation fs|fs+net` work on
  Windows.
  - *Verified (PR 183, CI run `42678349017`):* On a non-elevated Windows worker (`TokenElevation == 0`), `\Sessions\<S>\DosDevices\<UserLogonId>` carries `(A;;CCDCLCSWSDRCWDWO;;;<CurrentUser>)` and `cargo build --offline` (including cached registry dependencies under a denied `USERPROFILE`) and `--workspaces` (`<ws>/tree` and `<ws>/tmp` confined to the per-attempt AppContainer SID `oxideswarm.task.<worker8>.<run_token>`, with non-inherited traverse-only for `oxideswarm.toolchain` on `<workspace_root>` and `<workspace_root>/<ws_name>`) pass under `--isolation fs` and `--isolation fs+net`, both locally and on `windows-latest` (via a non-elevated scheduled task `InteractiveToken` / `LeastPrivilege`), and a worker with default sandbox settings (`%TEMP%\rusty_grid\sandboxes` inside `host.user_home`) starts cleanly.
  - *Residual limitation:* On an **elevated** Windows worker (`TokenElevation != 0`), UAC creates `\Sessions\<S>\DosDevices\<AdminLogonId>` with `TokenOwner = BUILTIN\Administrators` (`O:BA D:(A;OICI;...;;;SY)(A;OICI;...;;;BA)`) and no ACE for `CurrentUser`, and `ntoskrnl.exe!SeIsTokenAssignableToProcess` refuses switching to a fresh logon session via `CreateProcessAsUserW` without `SeAssignPrimaryTokenPrivilege` (`os error 1314`), so `cargo` and other programs that open `NUL` fail with `os error 5` inside the AppContainer. Workers must run non-elevated; when an elevated Windows worker starts with `--isolation fs|fs+net`, it logs a startup `WARN` and drops `tool:cargo` from its advertised capability tags.

### FEAT-4: Remote workspaces for coding agents

- **Evidence:** Read.
- **Problem:** An agent cannot run `cargo test` on another machine through MCP. Staging takes single
  files up to 32 MiB. The sandbox is per task, so `target/` is never reused. `working_dir` is
  hard-coded to `None`. All clients use TCP only, so a client on another network cannot reach a
  P2P-only master. The default timeout is 60 s.
- **Do:** Design a named persistent workspace per worker and a git or bundle source. Add P2P to
  `connect_master` for every client.
- **Done when:** An agent on one machine runs `cargo test -p rusty_grid_core --lib` over MCP on another
  machine at a commit that machine did not have. The second run reuses `target/`.

### FEAT-5: Checkpoints for process tasks

- **Evidence:** Read.
- **Problem:** Checkpoints work for the `BuiltinTest` hash branch only
  (`crates/worker/src/runner.rs:853-917`). `Command`, `ShellScript`, `RustCompilation` and
  `GpuCompute` never receive one. The 3D tasks send progress as checkpoints.
- **Do:** Add `OXIDESWARM_CHECKPOINT_DIR` in the sandbox, stored in capped `Checkpoint` frames and
  restored before a re-run. Separate progress from state.
- **Done when:** A `Command` test kills the worker, and a second worker finds the state file and skips
  finished steps.

### FEAT-6: Master failover

- **Evidence:** Read. A grep confirms that nothing constructs `ClaimMasterRole`.
- **Problem:** The three handlers in `crates/master/src/server.rs` (`server.rs:2771`, `server.rs:3106`,
  `server.rs:3718`) fabricate an incumbent ticket with a hard-coded score of 3500. On accept the
  master logs a step-down and keeps running. `calculate_authority_score`, `resolve_election_tie` and
  `check_mobile_master_eligibility` have no production caller. Without a secret, a claim is accepted
  unsigned.
- **Do (default of D1):** Answer every claim with `accepted: false` from one helper. Remove
  `ROLE_ARBITRAGE` from `all_supported()`. Mark the design as Design only. Real failover needs
  replicated state, renewed leases and a fencing epoch, and it starts after FEAT-1.
- **Done when:** A test shows that a claim is refused, and no document says failover is finished.
- **Note (2026-11-15):** Implemented means the refusal of decision D1 (pull request 81, design
  0026). The master still has no failover. Master high availability is SCALE-4, which is Planned
  (DOC-9).

### FEAT-7: Usable rendezvous discovery

- **Evidence:** Read.
- **Problem:** Tier 2 lives on the dashboard listener, which is loopback by default. Tier 3 has no
  host: the registry is a single overwritable slot on a master's own dashboard. `p2p_ticket` is
  outside the MAC (`crates/core/src/discovery.rs:237`) and dropped by `to_beacon`. `https://` URLs
  silently use plain TCP (`discovery.rs:789`). No test reaches Tier 3 through
  `discover_master_tiered`.
- **Do:** Add a standalone registry command with keyed leases. Cover the ticket with the MAC. Reject
  `https://` or implement TLS. State the real preconditions in `README.md`.
- **Done when:** A worker with `--master auto --rendezvous <URL>` registers in a test with Tiers 1 and
  2 blocked. A tampered ticket fails verification.

### FEAT-8: Real protocol versioning

- **Evidence:** Read. `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` were bumped to 2
  in INT-5 F6.
- **Problem:** Before INT-5 F6, `Task` and `TaskResult` changed layout without a version bump. There are
  no golden-bytes tests to catch future unbumped layout changes. The worker ignores the acknowledged
  feature bits, and only `FILE_STAGING` is read. `ClientMessage` has no version, so a client cannot
  learn the master's version. A client from `e1215ee` on a master from `ee2d13b` lost `--wait` without
  an error. A client from `ee2d13b` on a master from `e1215ee` was disconnected with a hint about the
  cluster secret (both reproduced). `bincode` 1.x is unmaintained (RUSTSEC-2025-0141).
- **Do:** Bump on every layout change, enforced by golden-bytes tests. Use the acknowledged bits. Add
  a version to the client path. Plan the move away from `bincode` 1.x.
- **Done when:** A changed `Task` layout fails CI without a bump. Mismatched peers refuse with a clear
  message.

### FEAT-9: GPU workloads beyond the benchmark

- **Evidence:** Read.
- **Status:** Partial (`FEAT-9a` / decision `Q56` implemented in [design 0068](design/0068-task-vram-accounting.md)). Tasks now accept `--vram-mb` / `vram_mb` (protocol v9, store v3) with master scheduler VRAM budget tracking and anti-starvation hold-back.
- **Problem (remaining `D6`):** `GpuCompute` multiplies seeded matrices. `kernel_name` is a label, `compute_intensity` is never read, and custom WGSL user kernels remain unimplemented. Real-GPU preference still prefers an idle `wgpu` worker over a simulated GPU worker when an idle real GPU is available.
- **Do (default of D6):** State "benchmark only" in `README.md:26` and `PROJECT.md:3`. If the owner
  puts kernels in scope, add a `TaskSpec` with user WGSL and buffers through blobs.
- **Done when:** The documents say benchmark only, or a user kernel matches a CPU reference on Metal,
  Vulkan and DirectX 12.

### FEAT-10: Compile crates with cargo

- **Evidence:** Read.
- **Problem:** `RustCompilation` runs bare `rustc` on one entry file, with no dependencies
  (`crates/worker/src/runner.rs:1604`). `Cargo.toml` is written and ignored. `submit --type compile`
  is refused (`crates/cli/src/cli_app.rs:2010`). No capability says that a worker has `rustc`.
- **Do:** Run `cargo build --offline` when `Cargo.toml` is present. Add a `tool:rustc` tag and a CLI
  source option.
- **Done when:** A crate with a local dependency builds from `submit`, and the binary reaches the
  client.

### FEAT-11: Remove dead code and split large modules

- **Evidence:** Read.
- **Problem:**
  - Unused: `dashboard/index.html`, `shaders/hash.wgsl`, `DashboardState`, `prepare_spec`,
    `probe_physical_adapter`, `auto_rig_mesh_async`, `compute_intensity`.
  - `crates/core/src/arbitrage.rs` has almost no production caller.
  - `crates/master/src/server.rs` holds two long connection loops, one for TCP and one for QUIC.
  - The Android bridge exports 13 functions under a `service` package that has no Kotlin class.
  - Leftover sandboxes after a crash have no start-up sweep.
- **Do:** Give each item a production caller or delete it. Merge the two connection loops into one
  session state machine. Add the sweep.
- **Done when:** Each listed symbol has a production caller or is gone.

### MAINT-1: Split the six largest source files and cap file size in CI

- **Evidence:** Measured on `master` `0354b28` on 2026-11-11 (lines; the test part is everything
  from the file's first `#[cfg(test)]`; commits are those on `master` since 2026-10-11):

  | File | Lines | Production | Tests | Commits |
  |---|---|---|---|---|
  | `crates/master/src/queue.rs` | 8,639 | 5,044 | 3,595 | 36 |
  | `crates/master/src/server.rs` | 8,593 | 4,941 | 3,652 | 73 |
  | `crates/worker/src/client.rs` | 8,252 | 4,361 | 3,891 | 87 |
  | `crates/cli/src/cli_app.rs` | 8,099 | 5,149 | 2,950 | 101 |
  | `crates/worker/src/runner.rs` | 6,293 | 4,141 | 2,152 | 79 |
  | `crates/core/src/protocol.rs` | 6,184 | 3,232 | 2,952 | 41 |

  Each holds 120 to 243 functions. FEAT-11 merged the two connection loops into
  `crates/master/src/session.rs` and removed dead code, but split none of these files.
- **Problem:** Each of these files mixes several jobs, so a reviewer has to read thousands of lines
  to judge one change, and parallel pull requests are likely to collide in them: `cli_app.rs`
  changed 101 times in a month. ENROLL-1b adds more to `server.rs`. Nothing stops a file from growing further.
- **Do:** one pull request per file, with no change in behaviour, starting each only after the open
  pull requests that touch that file have merged (ENROLL-1b for `server.rs` and `cli_app.rs`,
  SCHED-4 for `scheduler.rs`):
  1. Move each file's `#[cfg(test)]` module to a file of its own next to it (`mod tests;` with the
     tests in `<module>/tests.rs`). This alone removes about 40 % of the lines.
  2. Split the production code by job, keeping the public API through `pub use` re-exports:
     - `cli_app.rs`: one module per subcommand (`master`, `worker`, `submit`, `status`/`result`/
       `cancel`, `mapreduce`, `workspaces`), with the clap types and `run_cli` dispatch left in
       `cli_app.rs`;
     - `client.rs`: connection, registration and reconnect; the active-task table and assignment
       handling; blob and staging glue;
     - `runner.rs`: process spawning and supervision; output capture and limits; Windows executable
       resolution;
     - `server.rs`: listeners and handshakes; client request handlers; enrollment and members;
     - `queue.rs`: the task state machine; retention and eviction; checkpoints;
     - `protocol.rs`: the message enums; the `Human*`/`Binary*` mirrors and their `From` impls; the
       codec.
  3. Add a size check to `cargo xtask`/`ox-mode test lint` and CI: a Rust source file whose
     production part exceeds 3,000 lines fails, except files on an allowlist that may only shrink.
- **Done when:** none of the six files has a production part above 3,000 lines, the allowlist is
  empty or only shrinks, the CI size check runs on every pull request, and the full test suite and
  `crates/core/tests/data/wire_layout.txt` are unchanged by the split.
- **Note (2026-11-15):** At `c2462e3` the production parts (lines before each file's `mod tests`)
  have grown in five of the six files since the table above: `server.rs` 6,275, `queue.rs` 5,915,
  `cli_app.rs` 5,463, `protocol.rs` 5,269 and `runner.rs` 4,333. `client.rs` is 4,540 after its
  tests moved to `client/tests.rs`. Each new item that touches these files makes the split harder.
- **Note (2026-11-16):** All six files are split (pull requests 230, 231, 242, 249, 250 and 253).
  At `03ace60` their tests live in files of their own, and the whole of each file is at most:
  `server.rs` 2,569 lines, `client.rs` 1,684, `cli_app.rs` 1,629, `queue.rs` 1,469, `runner.rs`
  1,336 and `protocol.rs` 864. The largest new non-test submodule is `queue/lifecycle.rs` with
  1,536 lines. The CI size check of step 3 is built (pull request 259, design 0080): `ox-mode test
  size`, part of `ox-mode test lint` and a step of the Linux `clippy` job, fails when a Rust file's
  production part exceeds 3,000 lines. Three other files were above the limit and are on the
  allowlist at their current size: `crates/worker/src/isolation.rs` 5,377, `crates/cli/src/mcp.rs`
  3,692 and `crates/cli/src/mode_cli.rs` 3,242. Every Done-when point holds, so the item is
  Implemented.
- **Implemented (2026-11-16, pull request 259, [design 0080](design/0080-source-file-size-check.md)):**
  `crates/cli/src/size_check.rs` measures every `*.rs` file (test-only files excepted) and compares
  it with `scripts/rust-file-size-allowlist.txt`: an unlisted file above 3,000 production lines or a
  listed file above its maximum fails; a shrunk entry is a warning. `ox-mode test size` runs it,
  `ox-mode test lint` includes it, and the Linux `clippy` job runs it on every pull request.
  `crates/core/tests/data/wire_layout.txt` is unchanged. The allowlist can now only shrink; splitting
  `isolation.rs`, `mcp.rs` and `mode_cli.rs` would empty it and is not part of this item.

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

### REL-4: Release workflow and install path

- **Evidence:** Reproduced. `git tag --list` and `gh release list` are empty.
- **Problem:** A user can get a binary only by building from source.
  `packaging/macos/install_mac_daemon.sh` expects a binary that nothing provides. Path dependencies
  have no version, 5 of 6 crates have no description, and `authors` differs from the licence.
- **Do:**
  1. Add `.github/workflows/release.yml` on `v*` tags, with Linux, macOS and Windows archives, with
     and without `gpu-wgpu`, and `SHA256SUMS`.
  2. Add an install section to `README.md`.
  3. Set `publish = false` (default of D4), and align `authors`.
- **Done when:** A tagged release has archives and checksums. A machine without Rust installs from the
  README alone.

### REL-5: Contributor and security files

- **Evidence:** Reproduced.
- **Problem:** No `CONTRIBUTING`, `CHANGELOG`, issue template or dependabot configuration exists.
  `docs/SECURITY.md:172` offers only a public issue for security reports.
- **Do:** Add the files, and add a private reporting route.
- **Done when:** The files exist, and `docs/SECURITY.md` names a private route.

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

### BENCH-1: Publish benchmarks with the scripts that measured them

- **Evidence:** Read, on 2026-11-08 at `master` `fec164e`. `run_cluster_benchmark.sh` measures
  chunk hashing and matrix products on two nodes. `benchmarks/interconnect/` holds a one-off WAN
  dataset. `ox-mode research` benchmarks the wire codec. Nothing measures per-task scheduling
  overhead, the bytes a repeated blob upload saves, or the user story end to end. HyperQueue, the
  nearest Rust task scheduler, states a per-task overhead below 0.1 ms in its README without a
  method ([HyperQueue](https://github.com/It4innovations/hyperqueue)).
- **Problem:** Writing standard rule 6 forbids a performance number without its script, so the
  project cannot yet state how fast it is.
- **Do:**
  1. Add `benchmarks/grid/` with a script that runs on loopback and measures: (a) no-op task
     throughput and the p50 and p99 time from submit to result, with one to four workers; (b) a
     first and a repeated upload of the same input tree, in bytes sent and time.
  2. After WS-7, add (c): the end-to-end offload of `cargo test` for a fixed public crate at a fixed
     commit, cold and warm, between two machines.
  3. Write each run to a results file with its date, commit, hardware, operating systems and
     network.
  4. Link the results from `README.md`. Compare with HyperQueue only with a run of both on the same
     machine.
- **Done when:** The script runs on macOS, Linux and Windows. Dated records of (a) and (b) on one
  machine, and of (c) on two machines, are committed, and `README.md` links them.

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

### JOIN-2: Join on a LAN with a short code

- **Evidence:** Read. An invite (ENROLL-1) is a long string, which is hard to type on a fresh
  machine.
- **Problem:** On a LAN, the owner wants to read a code aloud or type it from another screen.
- **Do:** [Design 0052](design/0052-short-code-pairing.md): `invite --code` prints
  `<nameplate>-<word>-<word>`, `join --code <CODE>` finds the inviter with a broadcast query to UDP
  port 8087, and SPAKE2 delivers an invite. The joiner confirms first, and any session that does
  not confirm cancels the code. Approval stays.
- **Done when:** `tests/cli_pair_e2e.rs` passes on Linux, macOS and Windows, and a live LAN run
  between Windows and macOS is recorded.

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

### SCALE-1: Load harness with simulated workers

- **Evidence:** Read, on 2026-11-09 at `master` `09a8990`. `benchmarks/grid/bench_grid.py` starts one
  to four real workers on loopback on one machine (BENCH-1). No run has more than four workers,
  more than one submitter or a network between master and workers. `docs/README.md` rule 6 forbids
  a scale claim without a script.
- **Problem:** The project cannot say how many workers one master handles, where the limit is, or
  what a mass reconnect costs. Every later item needs that number.
- **Do:**
  1. Add a simulated worker: a `WorkerClient` with a fake runner that answers after a set delay,
     with optional link delay and loss, so 1000 of them run on one host.
  2. Extend `benchmarks/grid/` with scenarios at 100, 500 and 1000 workers and several submitters:
     submit-to-assign p50 and p99, throughput, master CPU and memory, journal bytes, replay time
     and the time to settle after every worker reconnects at once.
  3. Write each run to a results file under the BENCH-1 rules: date, commit, hardware, operating
     system, network and raw runs.
- **Done when:** Dated records at 100, 500 and 1000 simulated workers are committed on one named
  machine, and `README.md` links them.

### SCALE-2: Scheduler and queue at scale

- **Evidence:** Read. `WorkloadScheduler::schedule_batch` and the placement `Engine`
  (`crates/master/src/scheduler.rs`) run in one process. `TaskQueue` holds all tasks in memory
  (`crates/master/src/queue.rs`), bounded by `--max-active-mb` and the retained-task limits. No code
  shares capacity between submitters.
- **Problem:** A placement pass, the memory of finished tasks and one submitter's backlog can each
  limit a grid with hundreds of workers. Nothing measures which does.
- **Do:**
  1. After SCALE-1, profile the pass and fix the largest cost first: index workers by OS,
     architecture, tag and free slots, and raise the micro-batch size where the profile supports it.
  2. Move cold terminal results out of memory into the store.
  3. Add fair share: a queue per submitter (the ECO-3 identity) with weighted round robin before
     placement. Until ECO-3 exists, the submitter key is the connection's client id.
  4. Keep the 8-state machine and `Engine::hold_back`.
- **Done when:** The SCALE-1 numbers improve by the bound the owner sets (Q8) with identical
  placement results on the existing scheduler tests, and a test shows a second submitter's task
  placed while a first submitter's backlog is larger than the free slots.
- **Implemented (`2026-11-15`):** Pull request 227 (`ec228a01`, [design 0050](design/0050-scale-out-architecture.md))
  added `WorkerIndex` / `RegisteredIndex` capability and resource bitsets (`crates/master/src/worker_index.rs`,
  including `WORKSPACES` and `TREE_V2`), `SCAN_PAGE = 512`, `DEFAULT_MASTER_MAX_BATCH_SIZE = 1024`
  (`--max-batch-size`), adaptive debounce and 16-batch drain, weighted round-robin per-submitter
  `FairShareQueues` (`crates/master/src/fair_share.rs`, `queue.rs`), dashboard broadcast receiver guards,
  and single-flight batched `ColdResultStore` disk spill (`crates/master/src/cold_results.rs`), with
  post-`SCALE-2` 3-run `scale_harness.py` records at `500w` (`409.1` vs `375.2 tasks/s`, `16.69` vs
  `31.01 ms` unloaded assign p50) and `1000w` (`414.4 tasks/s`, `91.8 MiB` steady RSS, `0` failures).
- **Note (2026-11-15):** The 1000-worker run is the stretch run of Q8 and shows no gain: the burst
  rate went from `419.7` (SCALE-1) to `414.4 tasks/s`, and the burst assign p50 from `17.4` to
  `18.9 ms` (`benchmarks/grid/results/2026-11-14-scale1-windows-amd64-1000w.json`,
  `2026-11-15-scale2-windows-amd64-1000w.json`). The status rests on the 500-worker target
  (DOC-9).

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

### ECO-4: Observability

- **Evidence:** Read. No Prometheus or OpenTelemetry code is in `crates/`. The dashboard
  (`crates/master/src/web_ui.rs`) shows live state to a person.
- **Problem:** An operator cannot alert on queue depth or scheduling latency with the tools they
  already use.
- **Do:**
  1. Add `/metrics` in Prometheus text format on the dashboard listener, behind the Observer scope:
     queue depth by state, tasks per second, scheduling latency histogram, workers by state and
     link, blob bytes, journal size, store health.
  2. Add OpenTelemetry traces behind an opt-in Cargo feature, exported over OTLP.
  3. List new dependencies for the owner before adding them. Never put the secret or task inputs in
     a metric label or span.
- **Done when:** A test scrapes `/metrics` from a running master and finds each listed series, a
  request without the Observer scope is refused, and `cargo check -p rusty_grid_master
  --no-default-features` still passes.

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

### LOG-1: Close P2P client endpoints cleanly and send client logs to stderr

- **Evidence:** Reproduced on 2026-11-09 during two-machine P2P runs and the Windows USB kit check.
- **Problem:**
  1. Every CLI client command (`doctor`, `workers`, `submit`, `status`, `mcp`) that connects over a
     P2P ticket logs `ERROR iroh::socket: Endpoint dropped without calling Endpoint::close.
     Aborting ungracefully.` on exit because `P2pClientGuard::drop` spawns a detached
     `endpoint.close()` task that is aborted when the CLI drops the Tokio runtime, and
     `BlobConnection::drop` only aborts its reader task without awaiting endpoint shutdown.
  2. `run_cli_with` in `crates/cli/src/cli_app.rs` routed `INFO` tracing events to `stdout` for
     every command except `mcp`, so `iroh` `INFO` lines printed before `workers --json` output over
     a P2P ticket and broke JSON parsing.
- **Do:**
  1. Add explicit `close` / `start_close` and `drain_pending_p2p_client_closes` in
     `crates/core/src/transport.rs`, close client transports and `BlobConnection`s in
     `crates/cli/src/blobs.rs`, `crates/cli/src/doctor.rs`, `crates/cli/src/mcp.rs` and
     `crates/cli/src/cli_app.rs`, and drain pending client endpoint closes before `run_doctor`,
     `McpState::close` and `run_cli_with` return.
  2. Route all tracing of client commands (`submit`, `status`, `result`, `cancel`, `workers`,
     `workspaces`, `mapreduce`, `doctor`, `join`, `secret`, `mcp`) to `stderr` in `run_cli_with`,
     keeping `master`, `worker` and `mode` unchanged.
- **Done when:** `test_log_1_cli_client_commands_over_p2p_ticket_close_endpoints_without_stderr_error`
  in `tests/test_p2p_wan_fallback.rs` runs `status`, `workers --json` and `submit` (with input and
  output blob transfers) over a P2P ticket with `RUST_LOG=info,iroh=info`, finds no
  `Endpoint dropped without calling` line in `stderr`, and parses the `stdout` of `workers --json`
  as JSON.

### BUG-18: Resolve bare commands under Windows isolation against `System32` before `Git\usr\bin`

- **Evidence:** Reproduced on 2026-11-10 during the Windows USB kit isolation check: `oxideswarm submit -- whoami` against a worker started with `--isolation fs` failed with exit code `-1073741502` (`0xC0000142` / `STATUS_DLL_INIT_FAILURE`).
- **Problem:** `augment_windows_path` and `resolve_windows_executable` (`crates/core/src/capabilities.rs:1602,1699`) checked `C:\Program Files\Git\usr\bin` ahead of `%SystemRoot%\System32`. Bare `whoami` therefore resolved to MSYS2's `C:\Program Files\Git\usr\bin\whoami.exe`, which links `msys-2.0.dll`. Inside a Win32 AppContainer, `msys-2.0.dll` fails during `DllMain(DLL_PROCESS_ATTACH)` Cygwin shared-heap initialization (`0xC0000142`), whereas `C:\Windows\System32\whoami.exe` is a native Win32 binary that exits `0`.
- **Do:** Add `augment_windows_path_for_isolation` and `resolve_windows_executable_for_isolation` in `crates/core/src/capabilities.rs` and call them from `TaskRunner::execute_process_supervised_with_workspace` (`crates/worker/src/runner.rs`) when `self.config.isolation != IsolationLevel::Off`, so native directories (`%SystemRoot%\System32`, `Git\cmd`, `Git\bin`) are searched before MSYS2 `usr\bin` directories (`Git\usr\bin`, `msys64\usr\bin`).
- **Done when:** `test_windows_executable_resolution_for_isolation_prefers_system32_over_msys_usr_bin` in `crates/core/src/capabilities.rs` and `test_bare_whoami_succeeds_under_isolation_fs` in `crates/worker/tests/test_feat3_os_isolation.rs` pass, and a live `oxideswarm submit -- whoami` against a Windows worker running with `--isolation fs` exits `0`.

### USB-2: Opt-in Windows isolation in the USB kit

- **Evidence:** Reproduced on 2026-11-10 ([design 0055](design/0055-usb-kit-windows-isolation.md)).
- **Problem:**
  1. `windows::acl_grant_manifest_path` in `crates/worker/src/isolation.rs` checked only `RUSTY_GRID_ACL_GRANT_LOG` and then `<user_home>/.oxideswarm/windows_acl_grants.txt`, ignoring `OXIDESWARM_HOME`. Starting a USB-kit worker with `--isolation fs` on Windows therefore wrote `windows_acl_grants.txt` into the machine owner's `%USERPROFILE%\.oxideswarm`.
  2. `packaging/usb/install.ps1` had no `-Isolation` flag, `packaging/usb/make_kit.sh` could not bake an isolation mode into the kit, `packaging/usb/uninstall.ps1` deleted `$InstallRoot` without revoking `oxideswarm.toolchain` ACL grants on `%RUSTUP_HOME%` and `%CARGO_HOME%`, and neither `install.ps1` nor `uninstall.ps1` had `[CmdletBinding()]` to reject unknown parameters.
- **Do:**
  1. Update `windows::acl_grant_manifest_path` (and `windows/revoke-isolation-acls.ps1`) to use precedence `RUSTY_GRID_ACL_GRANT_LOG > <OXIDESWARM_HOME>/windows_acl_grants.txt > ~/.oxideswarm/windows_acl_grants.txt`, with unit test `test_acl_grant_manifest_path_precedence_honours_oxideswarm_home`.
  2. Add `[CmdletBinding()]` to `packaging/usb/install.ps1` and `packaging/usb/uninstall.ps1`; add `-Isolation off|fs` (default `off`) to `install.ps1`, `--isolation off|fs` (default `fs`) to `install.sh`, and `--isolation off|fs` to `make_kit.sh` (writing `$KIT/isolation` and copying `windows/revoke-isolation-acls.ps1` into `$KIT/revoke-isolation-acls.ps1`).
  3. Update `packaging/usb/uninstall.ps1` to stop the worker and run `revoke-isolation-acls.ps1` against `$InstallRoot\home\windows_acl_grants.txt` before deleting `$InstallRoot`.
- **Done when:** `tests/test_launcher_scripts.py` passes, and a live Windows install with `-Isolation fs` runs the trial-guide checks, revokes all `oxideswarm.toolchain` ACEs on `uninstall.ps1`, and leaves `%USERPROFILE%\.oxideswarm` untouched.

### USB-3: Keep the Windows USB install folder apart from the default workspace root

- **Evidence:** Found by reading on 2026-11-10 during the review of pull request 204. On Windows the
  USB kit installs into `%LOCALAPPDATA%\OxideSwarm` (`packaging/usb/install.ps1` and
  `uninstall.ps1`, parameter `-InstallRoot`), and `worker::workspace::default_root_with`
  (`crates/worker/src/workspace.rs`) puts the default workspace root at
  `%LOCALAPPDATA%\oxideswarm\workspaces`. NTFS compares names without case, so both are the same
  folder.
- **Problem:** `uninstall.ps1` removes `$InstallRoot` recursively. On a user account that also runs
  a worker from a normal install with `--workspaces` and the default root, removing the USB kit
  deletes that worker's workspaces. The reverse also holds: the other worker's workspace store sits
  inside the kit's private install folder and its lock and state files can make the kit's checks
  refuse the folder or the uninstall fail half way.
- **Do:**
  1. Give the kit an install folder name that no other component uses (for example
     `%LOCALAPPDATA%\OxideSwarm-USB`), and keep reading the old name for upgrades and uninstall.
  2. Make the kit always pass `--workspace-root <install folder>\workspaces`, also for the default
     folder, so the kit never shares the default root.
  3. Make `uninstall.ps1` refuse to delete a folder that holds a `workspaces` directory the kit did
     not create, and say which worker uses it.
- **Done when:** a test in `tests/test_launcher_scripts.py` shows that the kit's install folder and
  workspace root never equal `default_root_with(Platform::Windows, ..)` for any case spelling, and a
  live Windows run with a normal worker using the default root plus the USB kit installed and then
  uninstalled leaves the normal worker's workspaces intact.

### USB-4: Size the kit's task slots to the machine's fast cores

- **Evidence:** Measured on 2026-11-10 (see SCHED-4): the kit gives a worker half of the logical
  processors as task slots (`packaging/usb/install.ps1`, `[Environment]::ProcessorCount / 2`, and
  the same rule in `install.sh`). On an Intel Core i7-1355U laptop that is 6 slots, and 6 concurrent
  CPU tasks took 39.6 s each against 2.9 s for 2.
- **Problem:** Laptop processors with few performance cores, a low power limit and the owner's own
  programs running slow down far more than linearly when every slot is busy, so the kit's default
  makes a borrowed laptop both slow for the cluster and sluggish for its owner.
- **Do:**
  1. On Windows, count the performance cores (`GetSystemCpuSetInformation`, highest
     `EfficiencyClass`) and on macOS the performance level (`sysctl hw.perflevel0.logicalcpu`); use
     their logical processors, at most half of all, as the default slots. Fall back to a quarter of
     the logical processors when the count is unavailable and the machine has a battery.
  2. Let `-Slots N` / `--slots N` override it, keep it on `-Update`, and print the chosen value.
- **Done when:** `tests/test_launcher_scripts.py` covers the rule with fake processor data, and on
  the i7-1355U laptop the kit picks 4 slots or fewer.

## Phase 9: an open compute network

Draft, added on 2026-11-10 from Phase H of [ROADMAP.md](ROADMAP.md). The architecture and the stage
exit checks are in [design 0054](design/0054-open-compute-network.md). Decisions Q14 to Q18 of the
roadmap are open. Each item starts with a threat model of its own and an independent review.

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

### LV-1: Fuzz targets for the parsers of untrusted bytes (first targets)

- **Evidence:** Read on 2026-11-10 at `master` `2f22c48`. The repository has no `fuzz/` directory
  and no property-testing dependency. Bytes from another machine reach these parsers, the first two
  before authentication completes: the frame reader and format dispatch of `MessageTransport`
  (`crates/core/src/protocol.rs`: `recv_raw_frame_max`, `recv_msg_with_codec_max`, frames up to
  `MAX_FRAME_SIZE`, 64 MiB), the handshake frames (`crates/core/src/cluster_auth.rs`,
  `crates/core/src/noise_auth.rs`), and `TreeManifest::decode` (`crates/core/src/blob.rs`). The
  `audit` job of CI reports `bincode` 1.x as unmaintained.
- **Problem:** Nothing has fed these parsers malformed input at scale. A panic or an unbounded
  allocation in one of them is a remote denial of service, and before the handshake it needs no
  secret.
- **Do:**
  1. Add a `fuzz/` crate for `cargo fuzz` (libFuzzer), outside the workspace members (add it to
     `exclude` in the root `Cargo.toml`), with a `Cargo.lock` of its own. List its dependencies for
     the owner in the pull request.
  2. Write three targets:
     - `inbound_frame`: the input is a byte stream that a `MessageTransport` over an in-memory
       stream reads as `InboundMessage`, once per `WireCodec`, with a small frame limit. The frame
       reader, the format byte dispatch and both decoders are then in the path.
     - `server_handshake`: the input is what a peer sends first. Run the server side of the
       handshake against it over an in-memory stream, with a fixed test secret and a fixed test
       key.
     - `tree_manifest`: the input goes to `TreeManifest::decode`.

     If a parser is only reachable through private functions, expose it behind a Cargo feature
     `fuzzing` of `rusty_grid_core` that is off by default. Do not change a normal build.
  3. Seed each target's corpus with valid encodings, for example the messages that
     `crates/core/src/wire_layout_tests.rs` builds.
  4. Run each target for one hour locally. Record the date, the machine, the number of executions
     and every finding in `fuzz/README.md`. This is spike R12 of design 0058.
  5. For each finding follow rule 4: a failing test first, then the fix. A finding that a remote
     peer can trigger goes through the private route of `docs/SECURITY.md` before any public pull
     request.
  6. Add a CI job on Linux that builds the targets and runs each for 60 seconds on pull requests,
     and for 10 minutes each on the weekly schedule. The job pins a dated nightly toolchain,
     because `cargo fuzz` needs one. Keep crash inputs as files under `fuzz/regressions/`, and run
     them in the same job.
- **Done when:** The three targets build and run in CI. `fuzz/README.md` holds the one-hour record.
  A malformed frame placed in `fuzz/regressions/` is read without a panic. The later targets of
  design 0058 LV-1 (the encrypted record reader, the join token and the invite, the journal reader,
  the JSON-RPC reader of `oxideswarm mcp`) are listed in `fuzz/README.md` as open.
- **Status:** Partial (2026-11-17, pull request 270). Built: `fuzz/` with the targets `inbound_frame`,
  `server_handshake` (v2 and v3) and `tree_manifest`, seed corpora, `fuzz/regressions/`, `fuzz/README.md`
  with the one-hour record (no finding; 24.5 million, 7.7 million and 34.6 million executions) and the
  `fuzz` CI job on a pinned nightly. Every function the targets call is public, so no `fuzzing`
  feature was needed. The local runs used a stable toolchain with `-s none`. Open: the `fuzz` job has
  not run yet on GitHub (Done when: the three targets build and run in CI); Implemented once it is green.

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

### BUG-19: Bound the data-lane backlog of a QUIC worker session

- **Evidence:** Read.
- **Problem:** In the QUIC branch of `WorkerSession`, `router_task` moves `AssignTask`,
  `AssignTaskWithCheckpoint`, `CancelTask` and `BlobReply` into `pending_data` whenever the 64-slot
  `data_out_tx` is full (`crates/master/src/session.rs:854-925`). `pending_data` is a `VecDeque`
  with no bound. The router keeps draining the 128-slot `outbound_rx` (`session.rs:647`) into it,
  so that channel no longer limits memory. The data writer awaits `send_msg` with no write
  timeout (`session.rs:957-983`). A worker that keeps its control lane alive but stops reading its
  data lane makes the master hold every assignment and every `BlobReply` chunk (up to 4 MiB each)
  for it. The comment on `BlobService` (`crates/master/src/server.rs:2321-2324`) says the bounded
  channel caps this buffering.
- **Do:**
  1. Cap `pending_data` by message count and by bytes.
  2. At the cap, stop reading `outbound_rx`, so its bound pushes back on the senders again.
  3. Add a write timeout on the data lane. When it expires, end the session with a logged reason,
     so the failover sweep requeues its tasks.
- **Done when:** A test with a QUIC worker that heartbeats but never reads its data lane shows the
  master's buffered bytes for that session stay under the cap. The session ends within the
  timeout, and its tasks are requeued.
- **Implemented (2026-11-15, pull request 236, [design 0069](design/0069-bounded-quic-data-lane.md)):**
  the data-lane backlog of a QUIC session is capped by count and bytes, and the data lane has a
  write timeout (`crates/master/src/session.rs`).

### BUG-20: Keep one slow worker from stalling every dispatch

- **Evidence:** Read.
- **Problem:** `schedule_batch` delivers its assignments one after another
  (`crates/master/src/scheduler.rs:1956-1969`). Each delivery awaits `sender.reserve()` on the
  worker's channel with no timeout (`scheduler.rs:2019`). The comment above it says a slow worker
  can keep the pass there for a long time. While one worker's channel is full, no other worker
  receives an assignment from that pass. The reaper does not end such a session, because any
  received byte counts as proof of life and the timeout is 10 s (`crates/master/src/reaper.rs:29`).
- **Do:** Deliver without blocking the pass on one worker. Either reserve with `try_reserve` and
  requeue the task for that worker when the channel is full (`requeue_dispatch_failure`), or
  dispatch to different workers concurrently with a per-worker deadline. Keep the check under the
  queue lock (`TaskQueue::deliver_if_scheduled`) that a cancelled task is never assigned.
- **Done when:** A test with one worker whose channel stays full and one healthy worker shows the
  healthy worker receives its assignments within one scheduling interval. The existing scheduler
  tests and the `Engine::hold_back` tests pass unchanged.
- **Implemented (2026-11-15, pull request 241, [design 0075](design/0075-non-blocking-dispatch.md)):**
  `WorkloadScheduler::dispatch` tries `try_reserve` first and defers a full channel instead of
  holding the pass (`crates/master/src/scheduler.rs`).

### BUG-21: Resolve relative staged executables against the task working directory on Windows

- **Evidence:** Reproduced in the `R20` Windows probe (`OxideSwarm_Agent_Hub`
  `test_runs/win_r20_gpu_probe.md`, row `W2_relpath_direct`). Staging `gemm_bench.exe` with
  `submit --input` and running `--command ./gemm_bench.exe -- 1024` fails on Windows under
  `--isolation off` with `os error 2` at spawn and under `--isolation fs|fs+net` with
  `"program './gemm_bench.exe' was not found in PATH"`.
- **Problem:** `resolve_windows_executable` and `resolve_windows_executable_for_isolation`
  (`crates/core/src/capabilities.rs`) and `windows::resolve_program`
  (`crates/worker/src/isolation.rs`) checked relative paths containing `/` or `\` against the
  worker process's current directory instead of the task's working directory (`eff_cwd` inside
  the sandbox), and `CreateProcessW` / `CreateProcessAsUserW` passed `NULL` for `lpApplicationName`.
- **Do:** Resolve `./`, `.\`, `sub/`, and `sub\` against the task working directory inside the
  sandbox across `--isolation off`, `fs`, and `fs+net`, preserve `.com`/`.exe`/`.bat`/`.cmd` and
  `MZ` rules of [design 0025](design/0025-windows-executable-resolution.md), refuse paths that
  escape the sandbox (`..`, drive/root prefixes, junctions targeting outside), wrap `.bat`/`.cmd`
  via `cmd.exe /d /s /c`, and pass an absolute `lpApplicationName` to `CreateProcessW` /
  `CreateProcessAsUserW`.
- **Done when:** Windows unit, `TaskRunner`, AppContainer (`test_feat3_os_isolation`), and CLI E2E
  (`cli_isolation_e2e`) tests stage a small `.exe` and run `./prog.exe`, `.\prog.exe`, and
  `sub/prog.exe` under `--isolation off` and `fs`, and refuse `..\prog.exe` without running
  outside decoys.
- **Implemented (2026-11-16, [design 0077](design/0077-windows-relative-task-executables.md)):**
  implemented relative task executable resolution and sandbox confinement in
  `crates/core/src/capabilities.rs`, `crates/worker/src/runner/supervise.rs`, and
  `crates/worker/src/isolation.rs`.

### SEC-10: Stop a working-directory config file from configuring `worker` and `submit`

- **Evidence:** Read.
- **Problem:** Every command except `mcp` loads `./rusty-grid.toml` or `./rusty-grid.json` from
  the working directory before `~/.oxideswarm/rusty-grid.toml`, and prints nothing
  (`crates/cli/src/config.rs:466-477`, called from `crates/cli/src/cli_app.rs:1239`). That file
  can set `master`, `secret` and `insecure` (`config.rs:190-210`). The quick start tells users to
  run `submit --workspace` from a project root (`README.md:81-85`). A checkout from someone else
  that ships a `rusty-grid.toml` could point `submit` at another master, or start `worker`
  against a master that then runs its commands on this machine. `mcp` already skips these files
  and prints a notice (`config.rs:452-455`).
- **Do (default of D8):** Skip a working-directory config file for every command, with the same
  stderr notice that `mcp` prints. `--config <PATH>` and `RUSTY_GRID_CONFIG` still load any file.
  Update `README.md`, `CLAUDE.md` (the `config.rs` precedence line) and a design record, in the
  same pull request (rule 7).
- **Done when:** A test runs `submit` and `worker` in a directory with a `rusty-grid.toml` that
  names another master. Neither connects to that master, and both print the notice.

### SEC-11: Refuse instead of falling back to the elevated token under Windows isolation

- **Evidence:** Read.
- **Problem:** On an elevated Windows worker, the AppContainer child starts from a restricted
  (LUA) token through `CreateProcessAsUserW` (`crates/worker/src/isolation.rs:4646-4670`). If
  `get_or_init_worker_lua_token` returns `None` (`isolation.rs:2321-2381`), or
  `CreateProcessAsUserW` fails, the code calls `CreateProcessW` with the worker's own elevated
  token (`isolation.rs:4674-4689`). It logs nothing. The child still runs in the AppContainer, but
  its token derives from the elevated token that the LUA path exists to avoid.
- **Do:** When the worker is elevated and the LUA path fails, fail the task with the Win32 error,
  and log the reason once. Keep `CreateProcessW` for a worker that is not elevated.
- **Done when:** A Windows test that forces the LUA token to fail sees the task fail with a
  message naming the cause, and no task process starts.
- **Implemented (2026-11-16, pull request 256, [design 0078](design/0078-no-elevated-token-fallback.md)):**
  an elevated worker whose LUA token is missing, or whose `CreateProcessAsUserW` fails, fails the
  spawn with the Win32 error and logs the reason once (`crates/worker/src/isolation.rs`). The
  Windows tests `test_windows_elevated_worker_refuses_when_lua_token_is_missing` and
  `test_windows_elevated_worker_refuses_when_create_process_as_user_fails` cover it.

### SEC-12: Test and narrow the macOS isolation profile

- **Evidence:** Read. The escape below is inferred and has not been tried.
- **Problem:** The Seatbelt profile starts with `(allow default)` and then denies file writes
  outside the sandbox and reads of selected directories (`crates/worker/src/isolation.rs:640-667`).
  Mach and XPC service lookups stay open apart from the pasteboard, which design 0033 states. A
  task might ask `launchd` or another XPC service to start a process outside the sandbox, for
  example with `launchctl submit`. Linux uses an allowlist (Landlock and seccomp) instead.
- **Do:**
  1. Add tests under `--isolation fs` that try `launchctl submit`, `open -a`, and `osascript`, and
     that hard-link a file from the owner's home into the sandbox.
  2. Deny `mach-lookup` by default, with an allowlist of the services that `cargo`, `rustc`,
     `git` and the system linker need.
  3. Record the result and any remaining limit in design 0033 and `docs/SECURITY.md`.
- **Done when:** The tests above pass on `macos-latest` in CI, and `cargo build` of a workspace
  still works under `--isolation fs` on macOS.
- **Implemented (2026-11-16, pull request 257, [design 0033](design/0033-os-level-task-isolation.md)):**
  the macOS profile denies `mach-lookup` and `job-creation`; the allowlist has one service,
  `com.apple.MTLCompilerService` for Metal GPU tasks; `cargo`, `rustc`, `git`, `cc` and `ld` needed none. `launchctl submit` was already refused
  (`deny(1) job-creation`). Tests for `launchctl submit`, `open -a`, `osascript` and a hard link
  to a secret were added. The profile is still a denylist inside `(allow default)`.

### SEC-13: Collect outputs through the descriptor that was checked

- **Evidence:** Read. The attack needs a task process that outlives the task.
- **Problem:** Output collection checks each entry's type without following links
  (`crates/worker/src/staging.rs:329-352`). The worker then opens the path again with
  `File::open`, which follows a symbolic link (`hash_file` in `crates/core/src/blob.rs:250`,
  called from `BlobCache::insert_file_inner` in `crates/worker/src/blob_cache.rs:377-395`, and
  `copy_verified` at `blob_cache.rs:606`). On macOS, and on Linux without a delegated cgroup, a
  task process that calls `setsid` and drops `RUSTY_GRID_RUN_TOKEN` survives the end of the task
  (`crates/worker/src/process_tree.rs:513-545`, `1210-1231`). Between the check and the open it
  can replace an output with a link to a file that only the worker can read. The worker would then
  upload that file. Under `--isolation off` the task can read such files anyway, so this matters
  under `--isolation fs|fs+net`.
- **Do:** Open each selected output once with `O_NOFOLLOW` (on Windows,
  `FILE_FLAG_OPEN_REPARSE_POINT` and a reparse-point check), check type and size on that
  descriptor, and hash and copy from it.
- **Done when:** A test replaces an output with a symbolic link between the check and the upload,
  and the task fails or the link is skipped. The target's content never reaches the master.

### SEC-14: Take the `agy` chat route out of the master dashboard

- **Evidence:** Read.
- **Problem:** `POST /api/chat` (`crates/master/src/web_ui.rs:333`) starts an `agy -p <prompt>`
  process on the master host (`run_agy` at `web_ui.rs:1255`, binary search at `web_ui.rs:1440-1476`).
  The route needs Submitter scope only (`crates/master/src/auth.rs:389-390`). The prompt is one
  argument, a timeout applies and the process group is killed, but what `agy` may do depends on
  that tool's own configuration. This is developer tooling inside the production master.
- **Do (default of D9):** Remove the route and its dashboard panel from the master. If the owner
  keeps it, require Admin scope and an opt-in master flag, documented in `README.md` and
  `docs/SECURITY.md`.
- **Done when:** `POST /api/chat` answers 404 on a default master, or 403 to a Submitter token and
  404 without the opt-in flag, with a test for each case.
- **Implemented (2026-11-15, pull request 237, [design 0071](design/0071-remove-dashboard-chat.md)):**
  the route, `run_agy` and the dashboard panel were removed from the master
  (`crates/master/src/web_ui.rs`, `tests/dashboard_security_test.rs`).

### SEC-15: Make the Agent Mesh launchers safe by default

- **Evidence:** Read.
- **Problem:** A mesh node runs `shell_exec` unless it is started with `--no-shell`
  (`crates/agent_mesh/src/client.rs:140`), so one hub token gives a shell on every node.
  `packaging/agent_mesh/windows/run_agent_node.ps1` has no way to pass `--no-shell`, while its
  `.cmd` sibling (`run_agent_node.cmd:24-27`) and the macOS launcher
  (`packaging/agent_mesh/macos/run_agent_node.sh:15-21`) do. The Android launcher puts the token
  on the `adb shell` command line (`packaging/agent_mesh/android/run_agent_node_adb.sh:56`). The
  hub serves `ws://` only, so a token sent to a hub that is not on loopback travels in clear.
- **Do:**
  1. Add `-NoShell` and `OXIDE_MESH_NO_SHELL` to `run_agent_node.ps1`.
  2. Pass the token to the phone through a file with mode 600 or through standard input.
  3. Make a node print a warning when it sends a token over `ws://` to a host that is not on
     loopback.
- **Done when:** `tests/test_launcher_scripts.py` checks `-NoShell` in the `.ps1` launcher. The
  adb launcher's command line holds no token. A test shows the `ws://` warning.
- **Implemented (2026-11-16, pull request 258, [design 0079](design/0079-agent-mesh-launchers-safe-by-default.md)):**
  `run_agent_node.ps1` has `-NoShell` and honours `OXIDE_MESH_NO_SHELL=1`, `run_agent_node_adb.sh`
  sends the token on standard input, and the Rust node and `scripts/agent_node.py` log a warning
  for a token over `ws://` to a non-loopback host. Tests are in `tests/test_launcher_scripts.py`
  and `client::tests::test_cleartext_token_warning_predicate`.

### AND-5: Stop the Android worker without blocking the calling thread, and build the bridge in CI

- **Evidence:** Read.
- **Problem:** `stop_worker_impl` joins the worker thread on the calling thread
  (`crates/android_bridge/src/lib.rs:353-355`). The worker can drain for up to 10 s
  (`TASK_DRAIN_TIMEOUT`, `crates/worker/src/client.rs:143`). The Kotlin app calls this from its
  activity and service (`MainActivity.kt`, `OxideWorkerService.kt`), so the user interface can
  freeze long enough for an "application not responding" dialog. `running` is read at `lib.rs:187`
  and set at `lib.rs:243`, so two quick starts can both pass the check. No CI job compiles the
  bridge for an Android target.
- **Do:**
  1. Make the stop call return at once, and join the worker thread on a thread of its own.
  2. Claim `running` with `compare_exchange` at the start.
  3. Add a CI step that runs `cargo check -p rusty_grid_android_bridge --target aarch64-linux-android`.
- **Done when:** A host-side test shows the stop call returns in under 100 ms while a task drains.
  Two concurrent start calls start one worker. The CI step runs on every pull request.
- **Implemented (2026-11-16, pull request 240, [design 0073](design/0073-android-bridge-nonblocking-stop.md)):**
  `stop_worker_impl` returns at once and joins the worker thread on a thread of its own, and a start
  claims `running` with `compare_exchange` (`crates/android_bridge/src/lib.rs`). The host tests
  `test_stop_worker_returns_in_under_100ms_while_worker_drains` and
  `test_interleaved_stop_start_at_most_one_worker_thread` cover both, and the CI step "android
  bridge compiles for aarch64-linux-android" runs in `.github/workflows/ci.yml`.

### DOC-9: Correct the statuses and statements that the audit found wrong

- **Evidence:** Read. Each line below was checked against the code at `c2462e3`.
- **Problem:** These statements disagree with the code or with git:
  1. The Index row of FEAT-6 is titled "Master failover" and set to Implemented. What was built is
     the refusal of `ClaimMasterRole` (design 0026). The item's own "Done when" says no document
     may say failover is finished. Master high availability is SCALE-4.
  2. The SCALE-2 note quotes the 1000-worker result without its baseline. At 500 workers, the
     target of Q8, the burst rate rose from 375.2 to 409.1 tasks/s. At 1000 workers, the stretch
     run of Q8, it fell from 419.7 to 414.4 tasks/s, and the burst assign p50 rose from 17.4 to
     18.9 ms (`benchmarks/grid/results/2026-11-14-scale1-windows-amd64-1000w.json`,
     `2026-11-15-scale2-windows-amd64-1000w.json`). The status Implemented stands on the
     500-worker target.
  3. Six Index rows mark merged pull requests "(open)": TEST-21 (198), TEST-22 (200), DOC-8 (196),
     LOG-1 (194), FEAT-3d (183) and FEAT-4 (186).
  4. `docs/ROADMAP.md`, "Where the project stands", describes `3ce2a65` of 2026-11-09. Its table
     "What is missing" still lists FEAT-3d and design 0039 W11, which both shipped in 0.2.0.
  5. `CLAUDE.md` lists six MCP tools (`mcp.rs` serves seven, with `workspaces`). It names doctor
     checks C1-C6 (`crates/cli/src/doctor.rs` runs C1-C8). It says the project is "verified across"
     Android ARM64 devices, while `README.md` says the worker has not run on a device.
  6. `PROJECT.md` cites `check_mobile_master_eligibility`, which FEAT-11 removed, and marks
     discovery Partial after FEAT-7 shipped.
  7. The help text of `worker --isolation` says "macOS only for now"
     (`crates/cli/src/cli_app.rs:453`). Linux and Windows are implemented.
  8. `README.md` says revoking a member "does not yet cut active lanes". The revoke handler does
     disconnect the member's worker sessions and requeues their tasks
     (`crates/master/src/server.rs:4654-4676`). Client connections stay open until ENROLL-1c.
  9. `README.md` does not say that the network namespace of Linux `fs+net` is best effort. Where
     user namespaces are forbidden, seccomp and Landlock remain
     (`crates/worker/src/isolation/linux.rs:26-28`).
  10. `docs/README.md` shows design 0044 as Planned (the record says Implemented) and design 0039
      as Accepted. It has no entry for designs 0001 to 0022 and 0025. Records 0031 and 0033 still
      say Accepted, though both are built. Records use both the template vocabulary (Draft,
      Accepted) and the status vocabulary (Implemented, Partial, Planned, Design only).
  11. The table "Is the project finished?" at the top of this file shows the snapshot of
      2026-11-01 (AEAD "on a branch", restart "Design only") without a date on the table itself.
- **Do:** Correct each statement in its own document. Rename the FEAT-6 row to "Refuse the
  master-role claim" and keep it Implemented, or set it to Design only; name SCALE-4 for real
  failover. Add the 1000-worker baseline to the SCALE-2 note, so the stretch run reads as no
  gain. Date the snapshot table. Pick one status vocabulary for design records and state it in
  `docs/README.md`.
- **Done when:** Each of the eleven points is corrected or marked as decided by the owner, and a
  search for "macOS only for now", "C1-C6" and "(open)" next to a merged pull request finds
  nothing.
- **Implemented (2026-11-15, pull request 234):** each point, in order:
  1. FEAT-6 row renamed, with a note.
  2. A 1000-worker note was added to SCALE-2.
  3. The six "(open)" markers were removed.
  4. Two rows left the table "What is missing" in `docs/ROADMAP.md`, its summary no longer
     counts Android among the running workers, and a dated note points to DOC-10.
  5. `CLAUDE.md` names seven MCP tools and checks C1 to C8, and states the Android limit.
  6. `PROJECT.md` drops `check_mobile_master_eligibility`, sets discovery to Implemented with
     design 0066, and names checks C1 to C8.
  7. The `--isolation` help text and the MCP `doctor` description were corrected.
  8. The revocation row of `README.md` now says what `members revoke` does.
  9. The Linux row of the platform table states that the network namespace is best effort.
  10. In `docs/README.md`:
      - rows for designs 0001 to 0022 and 0025 were added;
      - 0031, 0033, 0039 and 0044 now show Implemented;
      - the unused numbers are named;
      - rule 7 states the design status vocabulary.
      The records 0031 and 0033 say Implemented.
  11. The snapshot section is dated.

  The remaining "C1 to C6" and "(open)" strings are in the dated snapshot section and in this
  item's own text.

### DOC-10: Keep one roadmap

- **Evidence:** Read.
- **Problem:** Four documents describe direction and status: `docs/ROADMAP.md` (updated
  2026-11-13, before 0.2.0), section 5 of `PROJECT.md`, the preamble of this file, and the untracked
  `docs/ROADMAP_AND_VISION.md`. The last calls itself "Approved for Implementation". Several of its
  statements disagree with the code: the Agent Mesh hub port (it says 8765; the default is
  `127.0.0.1:8088`, `crates/agent_mesh/src/main.rs:35`), the checkpoint type (`CheckpointData` in
  `crates/core/src/task.rs`), the chunk size and hash (4 MiB and BLAKE3,
  `crates/core/src/blob_transfer.rs:29`), the tree manifest version (`tree/2`,
  `crates/core/src/blob.rs:54`) and "no Prometheus `/metrics`" (ECO-4 added it).
- **Do:** Keep `docs/ROADMAP.md` as the only roadmap and bring "Where the project stands" up to
  0.2.0. Replace section 5 of `PROJECT.md` with a link to it. The owner decides whether
  `docs/ROADMAP_AND_VISION.md` is deleted, or corrected and merged into `docs/ROADMAP.md`; it is
  not committed as it stands.
- **Done when:** One roadmap document exists, `PROJECT.md` links to it, and no committed document
  calls a plan "Approved for Implementation" without a decision in `docs/ROADMAP.md`.
- **Implemented (2026-11-15, pull request 235):**
  - "Where the project stands" in `docs/ROADMAP.md` now describes `96ce571` (2026-11-16).
  - Section 5 of `PROJECT.md` is a link to `docs/ROADMAP.md`.
  - The owner left the choice to Claude. Claude moved the untracked
    `docs/ROADMAP_AND_VISION.md` out of the repository into an archive folder of the owner's, with
    a note, instead of deleting it. Its six sound points had already landed through pull request
    206.
  - A search of the tracked files finds no "Approved for Implementation".

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

### PROTO-1: The release-train rule for wire changes

- **Evidence:** Read. Design 0076 D1.
- **Problem:** A pull request that bumps `PROTOCOL_VERSION_CURRENT` makes `master` speak a version
  that no release speaks, and nothing says when a release must follow.
- **Do:** Add `.github/pull_request_template.md` with a "Wire change" checkbox, one line in
  `CLAUDE.md` where the protocol-version rules are, and one line in `docs/README.md` where the
  design-record rules are.
- **Done when:** The three files carry the rule.
- **Implemented (2026-11-16, this pull request):** the template, the `CLAUDE.md` line and the
  `docs/README.md` line exist. Q64 takes its recommended default: a release within 48 hours, and
  never across two wire changes.

### PROTO-2: A CI job that runs the newest release against the pull request's build

- **Evidence:** Read. Design 0076 D2.
- **Problem:** Nothing in CI shows whether the build of a pull request still works with the
  newest release.
- **Do:** Add a job to `.github/workflows/ci.yml` that runs the newest release against the
  pull request's build in three pairs (as design 0076 D2 lists them) and writes a table of the
  results to the job summary.
- **Done when:** Each pull request shows the release-to-build table.
- **Implemented (2026-11-17, pull request 269):** `scripts/release_skew.py` (Python standard library, unit tests in
  `tests/test_release_skew.py`) runs the three pairs on loopback with private HOMEs and prints the table; the CI job
  `release skew (ubuntu-latest)` builds the pull request's `oxideswarm`, downloads the newest release archive with
  `gh release download`, checks `SHA256SUMS` and runs the script. It is not a required check, and it passes with a
  notice when no release exists.

### PROTO-3: An extension slot for optional wire fields

- **Evidence:** Read. Design 0076 D3.
- **Problem:** Every added field moves `PROTOCOL_VERSION_MIN_COMPATIBLE`, because bincode is
  positional.
- **Do:** Write a design record first (done: [design 0082](design/0082-task-extension-slot.md), 2026-11-17). Then build the slot at the next wire change (Q65), in
  `crates/core/src/task.rs` and `crates/core/src/protocol.rs`.
- **Done when:** An optional field can be added with no change to
  `PROTOCOL_VERSION_MIN_COMPATIBLE`.
- **Status:** Implemented by pull request 272 (draft: it merges together with PROTO-4, one-version
  window, by the release-train rule of design 0076 D1). Protocol 10, store format 4,
  `Extension` and the empty registry in `crates/core/src/task.rs`, the `ext:` worker tag and the
  scheduler check, and the frozen v9 `SubmitTask` decoder. Q68 to Q72 decided by the owner on
  2026-11-17 (the recommended defaults).

### PROTO-4: A one-version compatibility window

- **Evidence:** Read. Design 0076 D4.
- **Problem:** Today `PROTOCOL_VERSION_MIN_COMPATIBLE` equals `PROTOCOL_VERSION_CURRENT`, so a
  master upgrade stops every older worker.
- **Do:** After PROTO-3 (Q66), make the master accept the previous version, update design 0040
  and `crates/core/src/wire_layout_tests.rs`.
- **Done when:** A worker one version back registers and runs a task.
- **Design:** [design 0083](design/0083-one-version-window.md) (2026-11-17, based on pull request 272): Q73 to Q79 decided by the owner in chat on 2026-11-17, all on the recommended defaults.
- **Status:** Implemented by draft pull request 274 (it merges together with PROTO-3, pull request 272, and a release follows within 48 hours, design 0076 D1). `PROTOCOL_VERSION_MIN_COMPATIBLE` is 9 at protocol 10; the master reads each peer's version, encodes for it (`serialize_message_for_peer`, `Task::for_peer`), serves protocol 9 workers and clients, and tags them `proto:9`; the layout test is a range check with `wire_layout_previous.txt` (v0.3.0) as the oracle; the skew job derives its expected table. PROTO-3d (the cross-version tests of design 0082) is built with it.

### PROTO-5: Keep a skew job against the release one protocol back

- **Evidence:** Read. Review of pull request 274.
- **Problem:** The skew job compares the pull request with the newest release only. After 0.4.0 that is
  protocol 10, so the one-version window (protocol 10 accepts 9) is no longer exercised against a real
  protocol 9 binary.
- **Do:** Pin a second release (v0.3.0 while the window reaches back to 9) in the skew job, with its
  `protocol.rs` for the expected verdicts, and move the pin at each wire change. Do not build it now.
- **Done when:** Each pull request shows a table row of a real release one protocol back.

### TEST-28: Make the group-commit timing test meaningful on slow runners

- **Evidence:** Reproduced once. `test_concurrent_submissions_share_group_commit_and_recover_across_restart`
  (`crates/master/src/server/persistence_tests.rs:906`) asserts that 1,000 durable submissions
  finish in under 2 s (line 935). On `windows-latest` (pull request 260, job `133322408308`,
  2026-11-16) they took 2.0422361 s.
- **Problem:** A wall-clock bound of 2 s says more about the runner than about group commit.
- **Do:** Measure group commit by the count of fsyncs or commits, or use a much wider bound. Keep
  what the test proves: submissions share commits, and the state survives a restart.
- **Done when:** The test passes on a slow runner and fails if every submission commits alone.
- **Implemented (2026-11-16, pull request 265):** the 2 s wall-clock bound is gone; a 60 s guard
  catches a hang, and the existing `store_sync_count()` assertion (more than 0 and fewer than 200
  syncs for 1,000 submissions) fails if every submission commits alone. 20 of 20 local runs
  passed, one of them with 12 CPU-burning processes.

### TEST-29: Find why `workspace tests (windows-latest)` loses its runner at 60 minutes

- **Evidence:** Reproduced twice on 2026-11-16: pull request 251 at `72b05c5` (job
  `133129128530`) and pull request 256 at `ede7d01` (job `133256035744`). GitHub annotated both
  "The hosted runner lost communication with the server". Reruns of the same heads passed in
  about 30 minutes.
- **Problem:** Some test hangs or starves the runner until the job limit. Nothing in the log says
  which.
- **Do:** Find the test: per-test timeouts, a step timeout below the job limit that prints the
  running tests, or running the suite under the Windows agent. Then fix it.
- **Done when:** The job finishes in its usual time on ten runs in a row, or the test that hung is
  fixed and named here.
- **Partial (2026-11-17, pull request 283):** investigated on a Windows 11 machine with toolchain
  1.96.0 (`OxideSwarm_Agent_Hub` `test_runs/win_test29_2026-11-17.md`). In both lost jobs steps 1 to 7
  had succeeded and step 8 (`cargo test --workspace`) was still running. Three CI-like runs found no
  hang and no memory or handle exhaustion, only three timing flakes under CPU starvation (TEST-36 to
  TEST-38). The cause of the runner loss is not known. The workspace step now stops at
  `timeout-minutes: 45`, prints a heartbeat line with the last output line once a minute
  (`scripts/ci_heartbeat.py`), and an `if: always()` step prints the process tree, memory and the
  last 120 lines of the test output, so the next occurrence names the running test.

### TEST-30: Stop `test_agent_bridge.py` from depending on the host's open ports

- **Evidence:** Reproduced on a machine where a real master listens on port 8088.
- **Problem:** `Diagnose.test_two_simulated_agents_on_one_platform_do_not_collide`
  (`tests/test_agent_bridge.py:745`) fails there, because the test probes the real port.
- **Do:** Make the test use a port it binds itself, or stub the probe.
- **Done when:** The test passes with a listener on 8088.
- **Implemented (2026-11-16, pull request 263):** `agent_bridge.py` reads the environment variable
  `OXIDESWARM_BRIDGE_ASSUME_LISTENING` (ports reported as listening; unset means probe as before),
  the tests set it to empty, and a new test sets it to 8088 and still sees `[SPLIT-BRAIN MASTER]`.
  The whole file passed (45, then 53 tests with pull request 261) with a real master on 8088.

### TEST-31: Remove the race in the detached-grandchild Blender probe test

- **Evidence:** Failed once on the ubuntu unit tests (pull request 238, before 2026-11-16).
- **Problem:** The test in `crates/core/src/capabilities.rs` (script near line 3737) kills the
  process group before the grandchild has called `setsid`, so the grandchild may still be in the
  group.
- **Do:** Have the script write a second file after `setsid`, and wait for it before the kill.
- **Done when:** The test passes 200 times in a loop on Linux.
- **Partial (2026-11-16, pull request 265):** the grandchild writes a ready marker after `setsid`
  and the child waits for it (at most 5 s) before it exits; the Unix timeouts were widened
  (2.5 s, 4 s, 10 s) because under load the script had not written its PID file within 800 ms.
  50 local runs on macOS passed (30 with 12 CPU-burning processes). The 200-run loop on Linux of
  "Done when" is not done.

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

### TEST-32: Make the cancel-during-upload worker test independent of `NoDefaultCurrentDirectoryInExePath`

- **Evidence:** Reproduced 5 of 5 times on Windows (smoke test, finding F2).
  `client::tests::test_cancel_during_output_upload_reports_result_and_decrements_master_active_tasks`
  (`crates/worker/src/client/tests.rs`) panicked with "task 2 did not write exit marker in time".
  With `Remove-Item Env:NoDefaultCurrentDirectoryInExePath` the same commit passes (248 passed).
  Claude Code sessions set that variable.
- **Problem:** The test starts `cmd.exe /C drain.cmd` with a bare name. With the variable set,
  `cmd.exe` does not search the current directory, so the script never runs. The product is not at
  fault: under `--isolation off` a task inherits the worker's environment, as designed.
- **Do:** Start `.\drain.cmd`, or clear the variable for the child.
- **Done when:** The test passes with the variable set to 1.

### TEST-33: Make the worker-churn stress test deterministic on Windows

- **Evidence:** Reproduced once on 2026-11-16: pull request 263 at `282de6f` (run `43879650203`, job
  `133341802791`, `workspace tests (windows-latest)`): `test_stress_high_volume_worker_churn`
  panicked at `tests/m9_stress_challenge_1.rs:763` with "All 30 tasks must complete despite rapid
  worker churn". The rerun passed. The pull request did not touch the master or the worker.
- **Problem:** The test waits a fixed time for 30 tasks while workers connect and drop; on a slow
  runner the requeue and backoff can take longer than the wait.
- **Do:** Find the wait or backoff the test depends on, and wait for the condition (all tasks
  terminal) with a bound that does not depend on runner speed, or count requeues.
- **Done when:** The test passes 50 times in a loop on Windows, one run with CPU load.
- **Partial (2026-11-17, pull request 286):** not reproduced on Windows 11 in 125 runs (exact and whole binary, no load,
  2 cores with 4 CPU spinners, 1 core with 6 spinners); the drain after the churn took at most 2.4 s against the
  20 s bound, and the CI log of the failing job stops before this binary. An aborted worker cannot linger: `Drop for
  Session` (`crates/worker/src/client/session.rs`) stops its lane tasks, so the socket closes. The drain wait is now
  60 s and the registration wait 30 s (both end as soon as the condition holds), and a timeout lists every task that
  is not `Completed` with its state, counts, worker and error. The changed test passed 50 runs on 1 core with 6
  spinners and 50 without load. On 1 core two other tests of the file failed: `test_stress_reaper_detection_and_failover`
  (2 of 20, line 1151) and `test_stress_immediate_failover_latency_under_load` (1 of 20, `129.9476ms` against `<100ms`).
- **Implemented (2026-11-17, pull request 289):** cause found in CI (pull request 288, run `44044328538`, job
  `133906405213`, windows-latest, with the diagnostics of pull request 286): one of the 30 tasks was `Failed` with
  `retries=1 failovers=1 evacuations=1` and `error: Sandbox base: The sandbox base directory '...\.tmpZk4loM' is
  not writable: The system cannot find the file specified. (os error 2)`. The churn loop dropped each retired
  `ManagedWorker` right after `graceful_disconnect()` or `abort()`, and a `ManagedWorker` owns the `TempDir` that is
  the worker's `--sandbox-base-dir`; the directory was deleted while the worker could still be given a task, the
  worker refused it (`client/tasks.rs`, `ensure_private_root_recheck`), and the master, which retries only the
  refusals it recognises (see BUG-25), failed the task for good although `with_max_retries(10)`. The test now
  keeps every retired worker in a `retired` list until the end, so no sandbox base disappears under a worker
  that is still alive; every assertion is unchanged. The other tests of the file keep their workers in a vector
  until the end and need no change. Loop: 50 runs of the exact test on macOS, 25 without load and 25 with four `yes` CPU burners, all passed, then the whole binary once (6 passed). The Windows 50-run loop of "Done when" is not run (the failure was seen only on windows-latest). This was a defect of the test; the product side (a worker
  with an unusable base fails the task terminally instead of handing it back) is BUG-25.

### TEST-34: Show and fix the Agent Mesh simulation Suite 2 failure on macOS

- **Evidence:** Reproduced once on 2026-11-16: pull request 262 at `eb68ed9` (job `133354770430`,
  `agent bridge (macos-latest)`): `tests/run_mesh_simulation_tests.py` printed "[FAILED] Suite 2 raised
  exception:" with no exception text, then "Suite 2: Bidirectional Zero-Data-Loss & Burst ... FAILED".
  The rerun passed. The pull request changed documents only.
- **Problem:** The runner prints no exception text, so the cause is unknown.
- **Do:** Print the exception type, message and traceback for a failing suite; then find the cause.
- **Done when:** The cause is written here and fixed, or the suite passes 50 runs on `macos-latest`.
- **Implemented (2026-11-17, pull request 277):** the runner now prints the type, `repr` and
  traceback of a failing suite on stderr. With that, the failure reproduced locally on macOS (6 of
  about 20 plain runs, while other jobs loaded the machine): `AssertionError` with no message at
  `tests/test_agent_mesh_data_loss.py` Phase 4, `assert len(bytes_128k) == payload_size_128k`. The cause
  is in the test, not in the Python reference hub: after sending a payload the test slept a fixed 0.1 s
  and then read `received_data_payloads[-1]`; when the hub had not yet delivered the 128 KB frame, that
  was the earlier 64 KB payload. The fixed sleeps of Phases 1 to 5 are now a bounded wait
  (`wait_for_payload_count`, 10 s) for the expected count, and every count and every payload is still
  asserted exactly (Phase 5 keeps a 0.1 s settle so a duplicate frame is still counted). After the fix:
  50 runs of `tests/run_mesh_simulation_tests.py` and 50 with 12 `yes` CPU burners, all passed. The
  `macos-latest` 50-run loop of "Done when" is not run.

### TEST-35: Make the `switch_role` launcher test stable on macOS

- **Evidence:** Reproduced twice: pull request 206 on 2026-11-12, and pull request 260 at `9d8b9cc` on
  2026-11-16 (job `133322408476`, `launcher scripts (macos-latest)`):
  `ShellLauncherTests.test_switch_role_master_flow_and_tracked_stop` failed with "AssertionError: 1 != 0"
  on the launcher's return code. Reruns passed.
- **Problem:** The launcher returned 1 once in a while; the test does not print why.
- **Do:** Print the launcher's output on failure, then find the cause.
- **Done when:** The cause is written here and fixed, or the test passes 50 runs on `macos-latest`.
- **Partial (2026-11-17, pull request 277):** diagnostics added, cause not found. A failing launcher
  run now fails with its command, return code, combined output, `master.log` / `worker.log` and the
  fake binary's recorded calls. The failure did not reproduce locally: 30 runs plain and 30 with 12
  `yes` CPU burners, each with the default `PATH` and with `PATH="/bin:$PATH"` (120 runs, 0 failures).
  The launcher returns 1 only when the master port is in use, the master process is not recognised
  right after it starts, or its log shows no "RustyGrid Master started" within 10 s; one suspected
  race (a process still showing the launcher's command line just after the fork) was not seen in 300
  tries. The launcher is unchanged. The next failure on `macos-latest` will print which of these it was.

### TEST-36: Remove the 50 ms bound from the symlink-chain manifest test

- **Evidence:** Reproduced on the owner's Windows machine under CPU starvation (4 cores, then 2 cores
  plus 4 CPU spinners; hub report `test_runs/win_test29_2026-11-17.md`):
  `manifest_v2_symlinks_and_empty_dirs_round_trip_and_refuse_escapes` in `crates/core/src/blob.rs`
  asserted that refusing the chained-link manifest takes under 50 ms and measured 52.19 ms and 78.52 ms.
- **Problem:** The test wants to prove that validation is bounded (40 chained links and 1,000 compound
  links must not expand exponentially), but a 50 ms limit also measures the runner's load.
- **Do:** Keep the refusal assertion and bound the time at 10 s: the unbounded expansion never finishes,
  so the bound still tells the two apart.
- **Done when:** Implemented by this PR. The test passed 50 of 50 runs next to 12 `yes` processes on a
  Mac, and 50 of 50 next to 48 (the old code also passed there: this Mac did not reproduce the failure).

### TEST-37: Remove the 100 ms window from the port-file reader test

- **Evidence:** Reproduced on the same Windows machine and report as TEST-36:
  `test_atomic_port_file_publication_concurrent_client_reads` in `tests/m2_challenger_2_stress.rs` let
  ten readers poll the port file for a fixed 100 ms and required at least one successful read.
- **Problem:** Readers add their reads to the total only when they stop, and under starvation they may not
  be scheduled inside 100 ms, so the total can be 0 although no read was ever corrupt.
- **Do:** Count each successful read as it happens and wait (60 s deadline against a hang) until 50 reads
  have been made before stopping the readers. Every read is still checked to be non-empty and a valid
  port.
- **Done when:** Implemented by this PR. 50 of 50 runs next to 12 `yes` processes and 50 of 50 next to 48.

### TEST-38: Remove the 800 ms window from the re-dispatched task id test

- **Evidence:** Reproduced on the same Windows machine and report as TEST-36:
  `test_reassigned_task_id_stops_the_old_run_silently_and_the_new_run_owns_the_slot` in
  `crates/worker/src/client/tests.rs` collected results for 800 ms after the re-dispatch and found none
  when `echo second` had not run yet.
- **Problem:** The wait for the new run's result was a fixed window, and the final 100 ms sleep decided
  when the slots were free.
- **Do:** Wait up to 60 s for the first result, then until the active table is empty, then drain the
  channel and still require exactly one result; poll for the two slots instead of sleeping.
- **Done when:** Implemented by this PR. 50 of 50 runs next to 12 `yes` processes and 50 of 50 next to 48.

### TEST-39: Wait for the `--no-shell` mesh node to register before calling it

- **Evidence:** `test_sec6_real_cli_no_shell_flag_execution` (`crates/agent_mesh/tests/test_sec6_mesh_sandbox.rs`)
  failed on `macos-latest` in pull request 274 with `ERR_NODE_NOT_FOUND` and passed on a rerun.
- **Problem:** The test spawned `agent-mesh node --no-shell` and slept 600 ms before sending `shell_exec`; a
  slow start left the node unregistered. Locally the old test failed 30 of 30 runs next to 24 `yes`
  processes, and once with no load on a cold binary.
- **Do:** Poll `AgentMeshClient::query_node_list` until the node is listed (30 s bound), then send.
- **Done when:** Implemented by pull request 278: 30 of 30 runs next to 24 `yes` processes.

### TEST-40: Wait for each payload in the Agent Mesh large-payload test

- **Evidence:** `test_adv_large_payload_boundaries` (`crates/agent_mesh/tests/adversarial_mesh_challenge.rs:520`)
  failed on `macos-latest` in pull request 275 (run `43957863018`, job `133617088614`): "Chunk size
  mismatch for size 131072", left `65536`. It passed on a rerun.
- **Problem:** Same cause as TEST-34: the test slept 150 ms after each send and checked the last payload
  received, which was still the 64 KiB one when the 128 KiB frame had not arrived.
- **Do:** Wait (20 ms polls, 30 s bound) until the receiver holds as many payloads as were sent, then run
  the same assertions. Pull request 278 also replaced the fixed sleeps before the burst counts of
  `adversarial_mesh_challenge.rs`, `adversarial_challenge.rs` and `mesh_integration.rs` with the same wait.
- **Done when:** Implemented by pull request 278. The old test did not fail locally (30 of 30 under load),
  so the fix rests on the cause seen in CI.

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

### TEST-42: Make the reaper boundary test independent of scheduler delay

- **Evidence:** `test_reaper_accuracy_silent_worker_boundary_timing` (`tests/m2_challenger_2_stress.rs:96`)
  failed on `windows-latest` on 2026-11-16 (run `43944560507`, job `133570626681`):
  `Reaper must NOT reap worker before timeout (at 100ms < 200ms)`, `left: Disconnected`, `right: Connected`.
  The same job failed the port-file reader test that TEST-37 fixed. ROADMAP.md "Stability" lists it.
- **Problem:** The test checks the worker 100 ms into a 200 ms timeout. A likely cause, not verified: on a
  starved runner more than 200 ms passed before the check, so the reaper acted as designed.
- **Do:** Measure the time that really passed before the check (or drive the reaper with a paused clock) and
  assert on that, as TEST-36 to TEST-38 did. If the reaper fired early, fix the reaper instead.
- **Done when:** The cause is named here, and the test passes 200 runs in a row under CPU starvation on Windows.

### TEST-43: Show and fix the late dashboard port file of the in-process dev cluster test

- **Evidence:** `test_dev_cluster_in_process_launch_then_stop_and_signal_failure_handling`
  (`tests/workflow_modes_test.rs:1017`) failed on `ubuntu-latest` on 2026-11-16 (run `43952618376`, job
  `133598809232`): `Dashboard port file (<tmp>/.dev_cluster.dashboard_port) did not appear within 8s for
  127.0.0.1:<port>`. ROADMAP.md "Stability" lists it.
- **Problem:** `ox-mode dev` waits a fixed 8 s for the dashboard port file. The log does not say whether the
  in-process dashboard started late or did not start.
- **Do:** Print the in-process master's output when the wait fails, find which of the two happened, then fix
  the start-up or wait on the condition with a bounded but generous limit.
- **Done when:** The cause is named here, and the test passes 200 runs in a row on Linux.

### TEST-44: Remove the 1.5x wall-clock cap from the speed-aware batch test

- **Evidence:** `sched4_batch_finishes_within_1_5x_fast_worker_and_zero_factor_restores_old_timing`
  (`tests/sched4_speed_aware.rs:194`) failed on `ubuntu-latest` on 2026-11-17, in pull request 282 at
  `3783987` (run `44021280672`, job `133834113011`): `D2 batch elapsed 766.662836ms exceeded 1.5x fast
  worker's own time (464.419518ms, cap=696.629277ms)`. Pull request 282 changes only `windows/oxctl.ps1`, a
  Python launcher test and docs.
- **Problem:** The test compares two wall-clock times of a few hundred milliseconds, so a loaded runner
  can break the 1.5x ratio without any change in placement.
- **Do:** Assert what design 0060 D2 decides (which worker ran each task, and that the slow worker was held
  back), and keep the timing as a printed measurement or as a much wider bound, as TEST-36 to TEST-38 did.
- **Done when:** The test passes 200 runs in a row under CPU starvation on Linux.

### TEST-45: Keep the second worker of the reaper failover stress test alive

- **Evidence:** Reported by agent-win with pull request 286 on 2026-11-17: on its Windows desktop with one
  core and six spinning processes, `test_stress_reaper_detection_and_failover`
  (`tests/m9_stress_challenge_1.rs:1151`, "Task must complete on Worker 2 after Worker 1 is reaped") failed
  in 2 of 20 runs of the whole test binary. Not seen in CI yet.
- **Problem:** The agent's reading, not proven: Worker 2 is a raw connection that sends no heartbeats while
  the test's reaper timeout is short, so the reaper can take Worker 2 too before its `TaskResult` counts.
- **Do:** Confirm the cause from the master's log in a failing run, then make Worker 2 send heartbeats (or
  give it a timeout it cannot reach) without weakening the assertion about Worker 1.
- **Done when:** The cause is named here, and the test passes 200 runs in a row under the same starvation.

### TEST-46: Remove the 100 ms bound from the failover latency stress test

- **Evidence:** Reported by agent-win with pull request 286 on 2026-11-17: under the same starvation,
  `test_stress_immediate_failover_latency_under_load` (`tests/m9_stress_challenge_1.rs:637`) failed in 1 of
  20 runs: `129.9476ms` measured against a strict `< 100ms`. Not seen in CI yet.
- **Problem:** A wall-clock bound of 100 ms, like the ones TEST-36 to TEST-38 removed.
- **Do:** Assert the order of events (the task was re-queued after the disconnect, without waiting for the
  reaper timeout) and print the latency, or use a bound that a starved runner cannot reach.
- **Done when:** The test passes 200 runs in a row under the same starvation.

### TEST-47: Count only this test's handles in the capability-detection leak test

- **Evidence:** `test_rapid_repeated_detection_sequential_leak_and_stability`
  (`crates/core/tests/m1_3d_adversarial_stress.rs:696`) failed on `windows-latest` on 2026-11-17, in pull
  request 286 at `c1238c6` (run `44027023301`, job `133849820395`): `File handle leak detected! Handle count
  grew by 8 (initial: 144, final: 152)` after 200 detections in 257 ms. The rerun of the job passed. Pull
  request 286 changes only `tests/m9_stress_challenge_1.rs` and docs.
- **Problem:** The test compares the handle count of the whole test process before and after its loop, while
  other tests of the same binary (for example `test_rapid_repeated_detection_concurrent_stress`) run on other
  threads and open handles of their own. A likely cause, not verified: those tests, not a leak.
- **Do:** Run the test alone in its process (a separate test binary, or a serial guard shared by the tests of
  this file that open processes), or compare several rounds and fail only on growth that keeps rising. If a
  real leak shows up alone, fix it in capability detection.
- **Done when:** The cause is named here, and the test passes 200 runs in a row on Windows next to the other
  tests of its binary.

### TEST-48: Give each Android bridge test its own worker state

- **Evidence:** `test_bridge_set_cluster_secret_hex_and_file_parsing`
  (`crates/android_bridge/tests/cluster_auth_test.rs:40`) failed on `ubuntu-latest` on 2026-11-17, in pull
  request 289 at `16313f5` (run `44050351817`, job `133926449804`): `Status must report error for invalid
  secret: {... "status":"STOPPED" ...}`. The rerun passed. Pull request 289 changes only a test of the master
  crate and docs.
- **Problem:** The bridge keeps its worker in process-global state (`OnceLock<Arc<WorkerState>>`, see
  `ANDROID_ARCHITECTURE.md`), and the tests of this binary run in parallel. A likely cause, not verified:
  another test started or stopped the worker between this test's call and its status read, so the status no
  longer shows the error this test caused.
- **Do:** Serialise the tests of the binary that touch the global worker (one lock shared by them, or one
  test per process), or give the bridge a way to reset its state between tests. Keep every assertion.
- **Done when:** The cause is named here, and the binary passes 200 runs in a row on Linux.
- **Implemented (2026-11-17, pull request 300):** The cause is not another test's call. All tests of the
  file already hold one `TEST_MUTEX`. `stop_worker_impl` returns while the worker still drains: it sets the
  status `STOPPING` and a detached thread writes `STOPPED` after it has joined the worker thread. That late
  write ran after the previous test released the lock, so it could land between this test's failing
  `set_cluster_secret_impl` (which writes `ERROR: Invalid cluster secret: ...`) and its status read. The
  tests now call `common::stop_and_settle()` (`crates/android_bridge/tests/common/mod.rs`), which stops the
  worker and waits (30 s bound) until the status is no longer `STOPPING`; the four test files that read the
  status use it. No change to the bridge, every assertion kept.

### TEST-49: Wait for the master by condition in the sched2 runtime test

- **Evidence:** `cli_app::runtime_tests::test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket`
  (`crates/cli/src/cli_app/runtime_tests.rs:886`, "round-robin master bound") failed in `gpu-wgpu (ubuntu-latest)`
  on 2026-11-17, in pull request 294 at `c5dfc08` (run `44086935350`, job `134051751538`). The rerun passed.
- **Problem:** The test tries to connect to the in-process master 50 times, 20 ms apart: one second. The
  `gpu-wgpu` job builds and runs heavier code on the same runner, so the master can take longer to bind.
- **Do:** Wait for the master's port file or for a successful connect with a bounded but generous limit
  (for example 30 s), as TEST-36 to TEST-38 did, and keep every assertion.
- **Done when:** The test passes 200 runs in a row under CPU starvation on Linux.

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

### BUG-23: Stop the Windows master gracefully in `oxctl.ps1`

- **Evidence:** Read on 2026-11-17 while preparing 0.4.0: `windows/oxctl.ps1` starts the master with
  `Start-Process -WindowStyle Hidden` and stops it with `taskkill.exe /F /T /PID` (line 135). A forced end
  skips the master's Ctrl+C, Ctrl+Break and close handlers (`wait_for_worker_shutdown_signal` in
  `crates/cli/src/cli_app.rs`), so a master with `--state-dir` never compacts `journal.bin`. Confirmed by
  the Windows smoke test of 0.4.0 on 2026-11-17 (`OxideSwarm_Agent_Hub` `test_runs/win_smoke_0.4.0_118fd88.md`,
  S1): after `taskkill /F /T /PID` the 0.4.0 master refuses the journal with the documented message. The way
  out also works there (S2): the 0.3.0 master in a visible console, Ctrl+C, exit code 0, journal compacted,
  and 0.4.0 then shows the 3 tasks.
- **Problem:** Across a protocol bump the next master refuses an uncompacted journal (DEPLOYMENT.md
  section 3.5), and a hidden master has no console to press Ctrl+C in. The launchers do not pass
  `--state-dir`, so only a master whose `state_dir` was set by hand (flag, environment or config file) is
  affected. `switch_role.sh` already sends SIGTERM and waits before SIGKILL.
- **Do:** Ask the master to stop first (for example a console control event to its process group, or a
  local stop request), wait a grace period, and use `taskkill /F /T` only after it; print which way it
  stopped.
- **Done when:** `oxctl.ps1 stop` on a master with `--state-dir` leaves a compacted journal (`journal.bin`
  with 0 records), shown by a launcher test or a recorded Windows run.
- **Implemented (2026-11-17, pull request 282, [design 0086](design/0086-graceful-windows-stop.md)):**
  `Stop-Tracked` first sends a console Ctrl+C from a separate hidden `powershell` (`FreeConsole`,
  `AttachConsole`, `SetConsoleCtrlHandler(NULL, TRUE)`, `GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0)`), waits up
  to 15 s and only then runs `taskkill /F /T`, and prints which way the process stopped; the worker gets the
  same stop (its clean stop cancels its tasks and ends their trees). Recorded run with the 0.4.0 binary and a
  private state dir: before, `journal.bin` 2157 B and no `snapshot.bin`; after, `master stopped cleanly after
  Ctrl+C in 0.7 s`, `journal.bin` 24 B, `snapshot.bin` 3331 B, and a restart lists the 3 tasks. A worker with a
  running `Start-Sleep 60` task stopped cleanly in 0.7 s with no task process left. `WindowsOxctlStopTests` in
  `tests/test_launcher_scripts.py` covers the clean path and the `taskkill` fallback on Windows.

### BUG-24: Let a master restart with `--state-dir` on a file system without permissions

- **Evidence:** Windows smoke test of 0.4.0 on 2026-11-17 (`OxideSwarm_Agent_Hub`
  `test_runs/win_smoke_0.4.0_118fd88.md`, finding G1). With `--state-dir` on an exFAT drive the master writes
  `members.json` and `master_noise.key` on its first start. On the next start `MemberStore::load_file`
  (`crates/master/src/members.rs`) runs `check_members_file_permissions`, which reads the DACL; exFAT has no
  ACLs, every account counts as having full control, and the master exits with code 1:
  `members file '<state>\members.json' is accessible by other accounts (Everyone); to make it private run: icacls ...`.
  The 0.3.0 and 0.4.0 masters behave the same, so it is not an upgrade regression. The 0.3.0 master also logged
  the same warning for `node.key` in a private HOME on exFAT; 0.4.0 and the other files under the private-file
  check were not tried. macOS and Linux (mode bits from the mount options) were not tried.
- **Problem:** A master whose state lives on an exFAT or FAT stick or SSD works once and then never starts again,
  and the message suggests an `icacls` command that cannot work on that file system.
- **Do:** Detect a file system without permissions (Windows: `GetVolumeInformationW` flags without
  `FILE_PERSISTENT_ACLS`; Unix: the mode cannot be narrowed) at the first start, before the master writes
  anything there, and refuse with a message that names the file system and asks for a directory on NTFS, APFS
  or ext4. Say the same instead of the `icacls` command when a later start finds it.
- **Done when:** A test (or a recorded Windows run) shows that a master pointed at a `--state-dir` without
  permissions refuses at the first start with that message and writes nothing there, and that NTFS behaves as
  before.
- **Implemented on Windows (2026-11-17, pull request 284, [design 0085](design/0085-state-dir-without-permissions.md)):**
  the CLI master (right after the state directory is resolved) and `MasterServer::bind_full` read the volume flags
  of the state directory, or of its nearest existing ancestor (`GetVolumeInformationByHandleW`, nothing created),
  and refuse a volume without `FILE_PERSISTENT_ACLS` with `state directory '<dir>' is on a file system that keeps
  no file permissions (exFAT), so every account on this machine could read or replace it; use a directory on an
  NTFS drive instead`. `members.json` and the private-file warnings (cluster secret, `node.key`,
  `master_noise.key`, the P2P key, the task store directory) use the same text on such a volume instead of the
  `icacls` hint. Recorded run: exFAT refused at the first start with nothing created, an exFAT state directory
  written by 0.4.0 refused and unchanged, NTFS start, stop and start as before. macOS and Linux are not covered.

### BUG-25: Hand a task back to the master when the worker's sandbox base is unusable at assignment

- **Evidence:** Found while closing TEST-33 (pull request 288, run `44044328538`): a task with
  `with_max_retries(10)` ended `Failed` after one failover with `error: Sandbox base: The sandbox base
  directory '...' is not writable: ... (os error 2)`. A worker that cannot use its sandbox base at assignment
  answers with a failed `TaskResult` (`crates/worker/src/client/tasks.rs`, around line 520: `ensure_private_root_recheck`
  fails, `TaskResult::failure(.., exit code 1, "Sandbox base: {e}")`, run time 0). The master treats a result as
  "the worker refused the task" only when its text holds one of the markers of `WORKER_REJECTION_MARKERS`
  (`crates/master/src/queue.rs`: "rejected by worker", "exceed worker capabilities", "capabilities insufficient",
  "lacks gpu capability"; `is_worker_rejection`). The sandbox message has none, so it is an execution failure,
  and `RetryPolicy::retry_on_execution_failure` is `false` by default (`queue.rs`, `queue/lifecycle.rs`), so the
  task becomes `Failed` with retries left. The same holds for the input-fetch check before the blob cache
  and for `Sandbox::create` failing on the base.
- **Problem:** The task is lost although another worker could run it, and the worker keeps looking idle, so it
  can absorb further tasks the same way (the situation `is_worker_rejection` was written for). Scope: the base
  is checked at start-up (`WorkerClient::run`, `ensure_usable`), so this needs a base that breaks while the worker
  runs. The default base (`<temp>/rusty_grid-<euid>/sandboxes`) is re-created on the next task (the per-task
  re-check recreates the private root, and `Sandbox::create` and the lock directory use `create_dir_all`), so a
  temp cleaner that removes it is survived on Unix; on Windows the same re-check recreates the base, yet the CI
  run above saw `os error 2` while the directory was being deleted. An explicit `--sandbox-base-dir` is checked
  the same way on Windows (any base) and, on Unix, re-created by `create_dir_all` but not re-checked for safety.
  What is not recreated: a base whose parent is gone and cannot be re-created, a full or read-only disk, or
  changed permissions. In those cases every assigned task fails for good.
- **Do:** Make a `BaseDirUnusable` or `BaseDirUnsafe` refusal at assignment, at the input fetch and in
  `Sandbox::create` a worker refusal: add a marker the master recognises (for example "rejected by worker:
  sandbox base") to the failure text, or a typed flag, keeping `execution_time_ms == 0`. Consider retrying the
  re-check once after a short delay, since the Windows case looked like a removal in progress.
- **Done when:** A test removes (or makes unwritable) the base of one of two workers after it registered, submits
  tasks with `max_retries` above 0, and all complete on the other worker; `is_worker_rejection` has a unit test
  with the exact message.

### CLI-2: Add a flag to move or disable the UDP discovery beacon

- **Evidence:** Read in the smoke test (finding F5): the master binds UDP `127.0.0.1:8089`, and
  the CLI has no flag to move it. It was free during the run and released when the master stopped.
- **Problem:** A second loopback master on the same machine cannot start its beacon.
- **Do:** Add a flag and an environment variable (`RUSTY_GRID_*`) to set the beacon port or turn
  the beacon off. Update the `README.md` reference.
- **Done when:** Two masters on one machine run at the same time, one with the beacon off.
- **Implemented (2026-11-17, pull request 275):** `master --discovery-port <PORT>` (`RUSTY_GRID_DISCOVERY_PORT`,
  `[master].discovery_port`) moves the beacon and `0` turns it off; design 0084. A worker finds a moved
  beacon only with `--master auto:<PORT>`. `crates/cli/tests/discovery_port_e2e.rs` runs two masters at
  once, one with the beacon off.

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

### MAINT-2: Bring the last three files under the size cap

- **Evidence:** `scripts/rust-file-size-allowlist.txt` lists three files whose production part
  (every line before the first `#[cfg(test)]`) exceeds 3,000 lines: `crates/worker/src/isolation.rs`
  (5,346), `crates/cli/src/mcp.rs` (3,692) and `crates/cli/src/mode_cli.rs` (3,240). MAINT-1 split
  six other files and left these at their size. `isolation.rs` holds the macOS code in an inline
  `mod macos` and the Windows code in an inline `pub mod windows`. Only the Linux code is in a file
  of its own (`crates/worker/src/isolation/linux.rs`). It also holds 125 of the 290 lines of `crates/*/src`
  that contain `unsafe`.
- **Problem:** The code that confines a task is the hardest file to review, and it is the largest
  production file. Two platforms cannot be changed without a merge conflict in one file.
- **Do:** One pull request per file. Move code, and change no behaviour, no public name and no
  message text.
  1. `isolation.rs`: move `mod macos` to `isolation/macos.rs` and `pub mod windows` to
     `isolation/windows.rs`, beside `isolation/linux.rs`. Keep the shared types, the level
     parsing and the start-up check in `isolation.rs`. Do LV-4 for these files in the same pull
     request or the next one, so each `unsafe` site is read once.
  2. `mcp.rs`: move each tool (`status`, `workers`, `submit`, `task_result`, `cancel`, `doctor`,
     `workspaces`) to a file under `crates/cli/src/mcp/`, and keep the JSON-RPC loop in `mcp.rs`.
  3. `mode_cli.rs`: move each workflow mode (`test`, `dev`, `doc`, `research`) to a file under
     `crates/cli/src/mode_cli/`.
  4. After each split, delete the file's line from the allowlist.
- **Done when:** The allowlist has no entry, `ox-mode test size` passes,
  `cargo test -p rusty_grid_worker --test test_feat3_os_isolation` passes on Linux, macOS and
  Windows, `cargo test -p rusty_grid_cli --test mcp_server_e2e` passes, and
  `crates/core/tests/data/wire_layout.txt` is unchanged.

### DOC-11: Make the backlog show open work first

- **Evidence:** This file has more than 3,600 lines. Its Index has 178 rows after this phase, and
  106 of them are Implemented. The text of every Implemented item stays in the phases below, so an
  agent that looks for open work reads the finished work first. No column says whether an open
  item is inside the stop line of design 0088 (D1).
- **Problem:** The length hides the open work, and the Index counts growth work (Phases 7 to 9) as
  open work for completion.
- **Do:**
  1. Add a column "Line" to the Index: "In" for an open item that an exit check of design 0088
     (D2) names, "Out" for an open item that its D1 puts outside the stop line, and empty for an
     Implemented item. An open item that neither names is "In" when it fixes a defect or a test
     (BUG, SEC, TEST), and otherwise goes to the owner as a question in the pull request.
  2. Move the text of every Implemented item to `docs/BACKLOG-ARCHIVE.md`, under the same phase
     headings and in the same order. Keep the Index whole, and keep the status log here.
  3. Add both files to the index of `docs/README.md`, and fix every link that pointed at a moved
     heading.
  4. Do this in one pull request, at a time when no other pull request that edits this file is
     open, and say so on the agent hub first.
- **Done when:** No item with status Implemented has its text in this file, every Index row of an
  open item has "In" or "Out", `python3 tests/test_m1_challenger_hygiene.py` reports no broken
  link, and a line count in the pull request shows that the two files together hold the item
  texts unchanged.

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

### ADOPT-2: Run summary in `submit --wait`, its JSON output and the MCP results

- **Evidence:** [Design 0059](design/0059-first-users.md), D5. `submit --wait` prints
  `Execution Time` per task (`crates/cli/src/cli_app/submit.rs`). No line or JSON field gives the
  time the caller waited, the sum of the task times or the number of machines: a search for
  `waited_ms` and `task_time_sum_ms` in `crates/cli/src` finds nothing.
- **Problem:** A user cannot see what the grid cost or gained on a run without a stopwatch.
- **Do:** As D5 of design 0059, for one task first:
  1. After a waited run, print the two lines of D5 (`Tasks:` and `Waited:`) next to
     `Execution Time`. `Waited` is measured on the client, and the sum adds
     `TaskResult::execution_time_ms`.
  2. Add `waited_ms`, `task_time_sum_ms` and `machines` to the JSON output of `submit` and to the
     result of the MCP tools `submit` and `task_result` (`crates/cli/src/mcp.rs`).
  3. Print no ratio and no percentage, and never call the sum the time on the user's machine.
  4. Update `README.md` and `CHANGELOG.md`. The values come from fields that exist, so the wire
     format does not change.
- **Done when:** The ADOPT-2 tests of the test plan of design 0059 pass: the unit cases (no task,
  one task, one failed task, two tasks on one worker), a waited run on loopback that prints both
  lines and carries the three fields under `--json`, and `mcp_server_e2e` finds the fields while
  standard output stays JSON-RPC only.

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

### COORD-1: Claim design numbers, Q ids, phases and item ids on the hub

- **Evidence:** On 2026-11-17 two Claude Code sessions each added `## Phase 13` (pull requests 292 and
  293), and design numbers were reserved only in one session's private memory (design 0087, Context).
- **Problem:** Nothing tells an agent which id another agent is about to use.
- **Do:** As D1 of design 0087: `agent_bridge.py claim design|q|phase|item`, create-only claim files on
  the hub with a settle check, `--list` and `--release`; the rule of Q85 in `CLAUDE.md` and `AGENT_RULES.md`.
- **Done when:** The unit tests pass in CI, and two near-simultaneous claims on the real hub end with two
  different values (a dated record).
- **Implemented (2026-11-17, pull request 295):** `claim` with create-only files
  (`<value>__<agent>__<time>__<nonce>.json`), a 20 s settle check and the order (time, agent, nonce);
  `--list`, `--release`. Live on the real hub (`OxideSwarm_Agent_Hub` `test_runs/coord_live_2026-11-17.md`):
  two claims 0.2 s apart as agent `owner` got `LIVETEST-1` and `LIVETEST-2`; the two-machine race is covered
  by the unit tests.

### COORD-2: Changed files on the activity record, with an overlap check

- **Evidence:** Pull requests 282, 284 and 286 added lines at the same places in `CHANGELOG.md`,
  `docs/BACKLOG.md` and `docs/README.md`; the conflicts showed only after the first merged.
- **Problem:** An agent cannot see before it starts that another agent changes the same files.
- **Do:** As D2 of design 0087: `activity --branch --pr --files --files-from-git` and
  `activity --overlap`.
- **Done when:** The unit tests pass in CI, and `activity --overlap` names a shared file in a run with
  two agents on the real hub.
- **Implemented (2026-11-17, pull request 295):** `activity --branch --pr --files --files-from-git` and
  `activity --overlap`; live on the real hub it printed the shared-file line for `CHANGELOG.md` and a
  warning for `scripts/agent_bridge.py` (same record).

### COORD-3: One shared place for process lessons

- **Evidence:** Process lessons lived in the coordinator's private Claude memory, which the Windows agents
  cannot read (design 0087, Context).
- **Do:** As D3 of design 0087.
- **Implemented (2026-11-17, pull request 294):** the hub's `AGENT_RULES.md` has section 3 "Lessons"
  (fifteen dated lessons; section 0 tells every agent to read it), and `CLAUDE.md` "Conventions and
  pitfalls" has the lessons that hold without the hub.

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

### VERIFY-1: Run a task again on other workers and compare its outputs

- **Evidence:** Design 0089 D3. The grid has no way to check a result.
- **Do:** `submit --verify <N>` and `--verify-file`, and the same in the MCP `submit` tool, client side with
  no wire change (Q89).
- **Done when:** The tests of design 0089 D3 pass in CI.

### APP-5: Guide: Anima Engine sweeps on OxideSwarm, with a recorded run

- **Do:** `docs/guides/anima-engine-sweeps.md` as design 0089 D5, with GPU and CPU variants and VERIFY-1.
- **Done when:** The guide exists with a recorded sweep and is linked from `README.md`.

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
