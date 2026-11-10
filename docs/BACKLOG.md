# Completion backlog

Open work on OxideSwarm, in execution order. Each work item stands alone, so it can be handed to a
coding agent without other context.

Snapshot: 2026-11-01, commit `3a1c120` (`master` at `c427f8b` plus pull request 3).
Line numbers refer to that commit. If a line has moved, search for the named symbol.
The Index and the status log are kept current. The rest of the file describes the snapshot.

## Is the project finished?

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
| Q13 | Which ecosystem comes first: (A) each person or team runs a cluster that trusted people add machines to, or (B) a public grid where strangers run each other's code? | A, decided by the owner on 2026-11-09. B waits for real users and its own design | ENROLL-1, JOIN-2, USB-1 |

## Index

Size is S, M or L. "Owner" means the item needs a person, a device, a decision or an approval to
merge.

On 2026-11-10, at `master` `4a07fcb`: 70 items Implemented, 4 Partial and 41 Planned. Five items
were added on 2026-11-08 from the research recorded in [ROADMAP.md](ROADMAP.md): CACHE-1, REAPI-1,
JOIN-1, LINK-1 and BENCH-1. Fourteen items (SCALE-1 to SCALE-6 and ECO-1 to ECO-8) were added on
2026-11-09 from Phases F and G of the roadmap and [design 0050](design/0050-scale-out-architecture.md). Four items (LV-1, LV-5, LV-10 and LV-14) were added on 2026-11-10 from [design 0058](design/0058-lasting-value.md). "Pull requests" lists them by number, "(open)" marks one
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
| BUG-1 | Stop hard-linking the staging cache | P0 | S | INT-4 |  | Implemented | 16 |
| BUG-2 | Fail a task when a declared output is missing | P0 | S | INT-4 |  | Implemented | 20 |
| BUG-3 | Fix the default 3D submit | P0 | S | INT-4 |  | Implemented | 21, 44 |
| BUG-4 | Fail only the task when a result is too large | P1 | S | INT-4 |  | Implemented | 18 |
| BUG-5 | Close the remaining gaps in the blob store and cache | P1 | M | INT-4 |  | Implemented | 26 |
| BUG-18 | Resolve bare commands under Windows isolation against `System32` before `Git\usr\bin` | P1 | S | FEAT-3 |  | Implemented | 204 (design 0055) |
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
| TEST-22 | Ignore UUID and metric `4090` tokens in `workflow_modes_test` | P1 | S | TEST-19 |  | Implemented | 200 (open) |
| TEST-21 | Fix Windows `m6_challenger_1_stress` and `test_sec6_mesh_sandbox` flakes | P1 | S | TEST-20 |  | Implemented | 198 (open) |
| DOC-1 | Correct false status claims | P1 | M | INT-4 |  | Implemented | 60 |
| DOC-2 | Remove unbacked performance numbers | P1 | S | INT-4 |  | Implemented | 61 |
| DOC-3 | Align design records with the code | P1 | M | INT-4 |  | Implemented | 68 |
| DOC-4 | Complete the CLI and environment reference | P2 | M | INT-4 |  | Implemented | 69 |
| DOC-5 | Document the 3D subsystem | P2 | S | 3D-1 |  | Implemented | 70 |
| DOC-6 | Correct the minimum Rust version | P1 | S | INT-4 |  | Implemented | 56 |
| DOC-7 | Add an MCP client setup guide | P2 | S | SEC-2 |  | Implemented | 32 |
| DOC-8 | Update `CLAUDE.md` and `SECURITY.md` for Windows isolation | P1 | S | FEAT-3 |  | Implemented | 196 (open) |
| REL-1 | Restore the Apache licence text | P0 | S |  |  | Implemented | 8, 82 |
| REL-2 | Remove owner data from tracked files | P1 | M | INT-4 | D5 | Implemented | 74 |
| REL-3 | Small repository hygiene | P2 | S | INT-4 |  | Implemented | 71 |
| CLI-1 | Add `cancel` and `result` commands | P1 | M | INT-4 |  | Implemented | 30, 47 |
| LOG-1 | Close P2P client endpoints cleanly and send client logs to stderr | P1 | S | FEAT-4 |  | Implemented | 194 (open) |
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
| FEAT-1 | Master durability | P1 | L | TEST-6 |  | Implemented | 122 (design), 129, 141, 147, 158, 167 |
| FEAT-2 | Containment after a worker crash | P1 | L | SEC-5 |  | Implemented | 123 |
| FEAT-3 | OS-level task isolation | P2 | L | SEC-5 |  | Implemented | 117 (design), 133, 150, 152 |
| FEAT-3d | cargo under Windows isolation | P2 | M | FEAT-3 |  | Implemented | 183 (open) |
| FEAT-4 | Remote workspaces for coding agents | P1 | L | MCP-2 |  | Implemented | 105 (design), 157, 156, 164, 166, 160, 165, 168, 170, 176, 174, 177 |
| FEAT-5 | Checkpoints for process tasks | P3 | L | FEAT-1 |  | Planned |  |
| FEAT-6 | Master failover | P3 | L | FEAT-1 | D1 | Implemented | 81 |
| FEAT-7 | Usable rendezvous discovery | P3 | M | INT-4 |  | Planned |  |
| FEAT-8 | Real protocol versioning | P2 | M | INT-4 |  | Implemented | 40, 110, 120 |
| FEAT-9 | GPU workloads beyond the benchmark | P3 | L | INT-4 | D6 | Planned |  |
| FEAT-10 | Compile crates with cargo | P3 | M | INT-4 |  | Implemented | 165 |
| FEAT-11 | Remove dead code and split large modules | P3 | L | TEST-6 |  | Implemented | 118 (design), 125, 126, 127, 131 |
| CACHE-1 | Reuse the result of an identical task | P2 | L | FEAT-1 |  | Planned |  |
| REAPI-1 | Serve the blob store as a Remote Execution API cache | P3 | L | CACHE-1 | Yes | Planned |  |
| REL-4 | Release workflow and install path | P2 | M | TEST-6 | D4 | Implemented | 155 |
| REL-5 | Contributor and security files | P3 | S | REL-1 |  | Implemented | 75 |
| JOIN-1 | Join a machine with one token | P2 | M |  |  | Partial | 163 |
| LINK-1 | Show whether each link is direct or relayed | P2 | M |  | Yes | Partial | 159 |
| BENCH-1 | Publish benchmarks with the scripts that measured them | P2 | M |  |  | Implemented | 162, 175, 187, 189 |
| ENROLL-1 | A key per machine, invites and revocation | P1 | L | JOIN-1 |  | Planned | (design 0051) |
| JOIN-2 | Join on a LAN with a short code | P2 | M | ENROLL-1 |  | Planned | (design 0052) |
| USB-1 | Set up machines from a USB stick | P2 | M | ENROLL-1, REL-4 |  | Partial | (design 0053), (open) |
| USB-2 | Opt-in Windows isolation in the USB kit | P2 | S | USB-1, FEAT-3d |  | Implemented | 204 (design 0055) |
| USB-3 | Keep the Windows USB install folder apart from the default workspace root | P2 | S | USB-1, W2 |  | Planned |  |
| SCALE-1 | Load harness with simulated workers | P2 | L | BENCH-1 |  | Planned |  |
| SCALE-2 | Scheduler and queue at scale | P2 | L | SCALE-1 |  | Planned |  |
| SCALE-3 | Worker-to-worker blob distribution | P2 | L | SCALE-1 |  | Planned |  |
| SCALE-4 | Master high availability | P2 | L | SCALE-1, FEAT-1 |  | Planned |  |
| SCALE-5 | Autoscaling providers | P3 | L | SCALE-1, JOIN-1 |  | Planned |  |
| SCALE-6 | Multi-site federation | P3 | L | SCALE-2, SCALE-3, SCALE-4 |  | Planned |  |
| ECO-1 | Remote Execution API compatibility | P3 | L | REAPI-1, ECO-2 |  | Planned |  |
| ECO-2 | Action cache (is CACHE-1) | P2 | L | FEAT-1 |  | Planned |  |
| ECO-3 | Multi-tenancy and quotas | P2 | L | SCALE-1 |  | Planned |  |
| ECO-4 | Observability: metrics and traces | P2 | M |  |  | Planned |  |
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
| LV-1 | Fuzz targets for the parsers of untrusted bytes (first targets) | P1 | M |  |  | Planned | (design 0058) |
| LV-5 | Supply chain: dependency policy, release provenance, outside checklist | P2 | M | REL-4 | Yes | Planned | (design 0058) |
| LV-10 | Citation file and a DOI per release | P2 | S | REL-4 | Yes | Planned | (design 0058) |
| LV-14 | Governance files and a licence that GitHub detects | P2 | S | REL-5 | Yes | Planned | (design 0058) |

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

## Phase 3: make the documents true

The standard in `docs/README.md` requires "only what the code does". These items apply it.

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
- **Problem:** `required_vram_mb` (`crates/master/src/scheduler.rs:138`) has no caller. `crates/master`
  reads neither `has_blender` nor `vram_mb`. `--blender-path` reaches only the capability probe, and
  `resolve_blender_binary` misses the macOS and Linux paths that `detect_blender` searches. A Mac can
  advertise Blender and then fall back silently.
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
- **Problem:** `GpuCompute` multiplies seeded matrices. `kernel_name` is a label, `compute_intensity`
  is never read, the CLI hard-codes `input_data`, and the product is dropped. `wgpu` is 22.1.0.
  Real-GPU preference applies only to a fully idle GPU (`crates/master/src/scheduler.rs:679`).
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
and adds no feature. Its other items (LV-2 to LV-4, LV-6 to LV-9, LV-11 to LV-13 and LV-15 to
LV-17) stay in the design record until the owner adds them here. The four items below do not depend
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
- 2026-11-10: Added LV-1, LV-5, LV-10 and LV-14 as Phase 10, from
  [design 0058](design/0058-lasting-value.md), at the owner's word. Nothing is built. The Index
  counts are now 70 Implemented, 4 Partial and 41 Planned.
