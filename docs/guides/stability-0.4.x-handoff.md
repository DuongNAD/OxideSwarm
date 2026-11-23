# Prompts for the 0.4.x stability milestone

Written on 2026-11-23 at `master` `78d0d0f6`. Each section below is a self-contained prompt for one
coding agent (Gemini/Antigravity on the Mac or on a Windows machine). Copy one section, from its
heading to the next, into a new agent session. The agent sees only the repository, so every prompt
names its files, its rules and its check.

## What the milestone needs

The exit check of Q80 ([ROADMAP.md](../ROADMAP.md), "Stability"):

1. Every failing test has a [BACKLOG.md](../BACKLOG.md) item, and each of those items is
   Implemented with its loop of runs recorded.
2. TEST-24 and TEST-25 each have one dated record. Both have one (loopback records of 2026-11-19
   and 2026-11-20), so this part is met; their two-machine runs (S6, S7) finish the items but do not
   block Q80.
3. `master` has seven days of CI runs for pushes in which no test fails. The last failing test was
   TEST-85 on 2026-11-20 13:49 UTC, so the earliest date is 2026-11-27 if nothing else fails.

Open items and who moves them:

| Item | State | Who | Prompt |
|---|---|---|---|
| TEST-85 | Planned: Windows `submit --type compile` test timed out | Agent, Windows | S1 |
| BUG-35 | Pull request 409 has conflicts with `master` | Agent, Mac | S2 |
| TEST-41 | Partial: needs 200 rounds in a row on Linux and Windows; then it can be closed the same way | Agent, Windows + Linux container | S3 |
| TEST-69 | Partial: needs the failing loaded configuration on the case PC | Agent, Windows case PC | S4 |
| CI streak and TEST-26 | Seven clean days; two green scheduled `endurance` runs | Agent, any machine, daily | S5 |
| TEST-24 step 2 | Two-machine transfer records | Agent, Mac + Windows | S6 |
| TEST-25 step 2 | Pull request 395, then a 24-hour three-machine run | Agent, Mac + 2 Windows | S7 |
| Windows check of pull requests 410, 413, 414 | AX-1, AX-2, FEAT-5, ENROLL-1d and MESH-2 never run by hand on Windows | Agent, Windows case PC | S8 |
| Real GPU record | wgpu tests, GEMM bench and VRAM routing with the case PC's GPU | Agent, Windows case PC | S9 |
| sysinfo 0.40 | Dependabot pull request 346 fails CI; needs the API changes | Agent, Windows case PC | S10 |
| TEST-23, TEST-61, SEC-16, CI-1 | Closed on 2026-11-23 (owner's delegation): loops recorded, no reproduced cause, diagnostics stay | none | none |
| BUG-22 | Needs a wire change, so it is not part of a 0.4.x release | Owner (next wire change) | none |

## Rules for every prompt

Paste these rules with each prompt (they are repeated at the end of each one as "Common rules").

- Read `CLAUDE.md` first. Work on a new branch from `origin/master` (`git fetch` first), in your own
  worktree with its own `target/` directory. Commit with clear messages; open a pull request; never
  merge it, never tag, never force-push, never push to `master`.
- Never read or write the real `~/.oxideswarm` (it holds the real cluster secret). Give every
  process you start a temporary `HOME`/`USERPROFILE`, its own ports (`127.0.0.1:0` or a port
  file), and stop what you start by its process handle or PID file, never by name.
- Never assert a wall-clock window in a test (TEST-36 rule): assert order or state, print the
  time, use guards of at least 60 s only against hangs.
- Never weaken or delete an assertion to make a test pass. If the product is wrong: first a test
  that fails without the fix, then the fix (BACKLOG rule 4).
- Record every loop of runs in the item's text in `docs/BACKLOG.md` with the date, commit, machine
  (CPU and core count, no host names), command, load, and counts of passes and failures. Update the
  Index row and the status log at the end of `docs/BACKLOG.md` in the same pull request.
- A change to behaviour, a CLI flag, an environment variable or the wire format needs a design
  record and a `docs/reference.md` entry in the same commit. No wire change is allowed in 0.4.x.
- Before you push: `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --locked -- -D warnings`, and the tests you touched.
- Report at the end: branch, pull request number, what changed, the exact commands you ran and
  their results, and anything left undone. Do not claim a result you did not run.
- Windows machines: keep at least 6 GB of RAM free, build with `-j 2` on the Dell laptop, and run
  no more than one test binary and two busy loops at a time there (it crashed twice under load).

---

## S1: TEST-85, the Windows `submit --type compile` timeout

Machine: a Windows machine (the case PC preferred). About half a day.

GOAL: find why `cli_app::runtime_tests::test_submit_compile_builds_crate_with_local_dependency_and_avoids_worker_without_cargo`
timed out on Windows CI, and make it pass reliably without weakening it.

EVIDENCE: CI run `44747726996` on `master` (2026-11-20), job `gpu-wgpu (windows-latest)`, step
`cargo test -p rusty_grid_cli --lib --features gpu-wgpu --locked`: the test printed
"Timed out after 60s waiting for task ... to complete" and failed "submit --type compile must succeed"
(`ExitCode(1)`). Read it with `gh run view --job 136327278116 --log-failed`.

FILES: `crates/cli/src/cli_app/runtime_tests.rs` (the test, `submit_args`,
`run_submit_with_secret`), the `--wait` handling of `crates/cli/src/cli_app/submit.rs`, and the
item TEST-85 in `docs/BACKLOG.md`.

DO:
1. Find the 60 s budget the test runs under, and where it comes from.
2. Run the test alone 20 times, then the whole `cargo test -p rusty_grid_cli --lib --features gpu-wgpu --locked`
   20 times under 4 busy loops. For each run record pass/fail, the task's `execution_time_ms`, and
   whether `cargo`/`rustc` was still running when the wait ended.
3. If the build finishes but late, raise the test's wait so it cannot be the cause (at least 600 s;
   prefer passing it through the test's own arguments rather than changing a product default), keep
   every assertion, and print the build time. If the task hangs, write a failing test for the hang
   first, then fix the product.
4. Run the fixed test 50 times in a row under 4 busy loops on Windows.

DONE WHEN: the cause is written in TEST-85, the test passes 50/50 in a row on Windows under 4 busy
loops (recorded), CI is green on the pull request, and TEST-85 is set to Implemented with the pull
request number.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S2: BUG-35, bring pull request 409 up to date

Machine: the Mac. About half a day.

GOAL: pull request 409 (branch `mac/bug-35-macos-spawn-fd-inheritance`, BUG-35: serialise macOS
`Command::spawn` calls with one process-wide lock) has conflicts with `master`. Make it mergeable
and green without changing its approved design.

CONTEXT: the approved fix is one shared spawn lock on macOS held around `Command::spawn` only (never
across `wait`, `wait_with_output` or pipe reads), used by every worker, CLI and agent mesh spawn
site, with an ordering-based regression test and one status-log line in design 0031; no new design
record. Read the pull request description (`gh pr view 409`) and its review comments first. If
someone else pushed to the branch in the last 24 hours, stop and report instead of editing.

FILES: the files of the pull request (`gh pr diff 409 --name-only`), mainly
`crates/core/src/process_spawn.rs` and the spawn sites; `docs/design/0031-containment-after-worker-crash.md`;
`docs/BACKLOG.md` (BUG-35).

DO:
1. Check out the branch, `git merge origin/master` (merge, not rebase), and resolve the conflicts.
   `master` gained about 45,000 lines since the branch was cut (pull requests 410, 413, 414), so
   find every `Command::spawn`, `.output()`, `.status()` and `tokio::process::Command` call in
   production code with `git grep` and route any new one through the shared helpers too.
2. Run `cargo test -p rusty_grid_core --lib process_spawn`, `cargo test -p rusty_grid_worker --lib`,
   `cargo test -p rusty_grid_worker --test test_feat3_os_isolation --locked`, and the regression
   test of the pull request 20 times in a row under 6 busy loops.
3. Push to the same branch (no force-push) and wait for CI.

DONE WHEN: pull request 409 has no conflicts, every production spawn site uses the shared helper,
CI is green on its newest head, and the loop is recorded in BUG-35.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S3: TEST-41, 200 rounds in a row on Windows and Linux

Machines: a Windows machine, and Linux in a Docker container on the Mac. About a day of machine
time, little work.

GOAL: record the loops that TEST-41's "Done when" asks for: the probe
`rv158_probe_result_during_master_shutdown_is_lost_and_task_reruns` (`tests/master_restart_recovery.rs`)
passes 200 runs in a row on Linux and on Windows, now that the shutdown and durability races of
commit `09c3accd` are on `master`.

FILES: `tests/master_restart_recovery.rs` (the probe reads `RV158_ROUNDS`, default 12, and
`RV158_ZERO`), the item TEST-41 in `docs/BACKLOG.md`. Change no code unless a run fails.

DO:
1. Windows: loop `cargo test -p rusty_grid_master --test master_restart_recovery --locked rv158 -- --nocapture`
   200 times under 4 busy loops, half of the runs with `RV158_ZERO=1`. Keep the output of any
   failing run (the `ran_twice` line, master and worker logs).
2. Linux: on the Mac, run the same loop in a container, for example
   `docker run --rm -v "$PWD":/src -w /src -e CARGO_TARGET_DIR=/tmp/t rust:1.96 bash -lc '<loop>'`,
   with 4 busy loops inside the container. Use the repository checkout of your worktree, never a
   path under `~/.oxideswarm`.
3. If any run prints `ran_twice` greater than 0, stop, keep its logs, and write a failing test for
   that round before you change product code.

DONE WHEN: TEST-41 records both loops (machine, commit, command, load, 200/200 or the failures with
their logs). If both loops pass 200/200, set TEST-41 to Implemented (closed without a reproduced
cause, like TEST-23) and move its text to `docs/BACKLOG-ARCHIVE.md` under its phase heading.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S4: TEST-69, the failing loaded configuration on the case PC

Machine: the Windows case PC only (not the Dell laptop: this configuration crashed it twice). About
half a day of machine time.

GOAL: show that the hardened waits of pull request 403 hold in the configuration that failed:
the full loaded `rusty_grid_worker` workspace run under 6 busy loops with AppContainer isolation.

FILES: `crates/worker/tests/test_feat3_os_isolation.rs` (`mod windows`, in particular
`test_attack_junctions_and_hardlinks_inside_sandbox_cannot_expose_denied_secret`), the item TEST-69
in `docs/BACKLOG.md` (its Partial text and the 2026-11-20 status log line).

DO:
1. With at least 6 GB free, run `RUSTY_GRID_REQUIRE_APPCONTAINER=1 cargo test -p rusty_grid_worker --locked`
   (the whole crate, as in the failing run) 20 times under 6 busy loops, from a non-elevated shell.
2. Record pass/fail per run and the durations of the five hardened tests. For a failure, keep
   `format_result_diag` output and the test log.
3. If a test fails, find out whether it is a wait that is still too short or a product defect, and
   follow the rules above.

DONE WHEN: the 20 runs are recorded in TEST-69; if all pass, set TEST-69 to Implemented with this
pull request and pull request 403.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S5: The CI streak and the scheduled endurance runs (daily, 10 minutes)

Machine: any. Run once a day until the milestone is met.

GOAL: keep the Q80 streak of seven clean days on `master` honest, and record the first two green
scheduled runs of the `endurance` job (TEST-26).

DO each day:
1. `python3 scripts/ci_push_runs.py --days 7` and `gh run list --branch master --event push --workflow ci.yml --limit 30`.
2. For every run that is not green, read the failed job (`gh run view --job <ID> --log-failed`):
   - A failing test: search `docs/BACKLOG.md` for its name. If it has no item, add one (next free
     TEST number; check `gh pr list --state all --search TEST-` first), with the run and job ids
     as evidence, in its own small docs pull request. The streak restarts at that run.
   - A runner problem (never acquired, network, toolchain download, rustc crash): note it in the
     status log; do not open a test item; rerun only the failed jobs of that run, and only if its
     head is still the head of `master`.
3. On Mondays after 12:00 UTC: `gh run list --workflow ci.yml --event schedule --limit 3`. If the
   `endurance` job of a scheduled run is green, add a line to TEST-26 with the run id and the date;
   after two green scheduled runs set TEST-26 to Implemented.

DONE WHEN: seven consecutive days of push runs on `master` show no failing test (write the date
range and the `ci_push_runs.py` output into the ROADMAP status log in a docs pull request), and
TEST-26 has two green scheduled runs.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S6: TEST-24 step 2, large transfers between two machines

Machines: the Mac and the Windows case PC on the same LAN; the owner must be present to allow the
firewall prompts. About one day.

GOAL: record scenarios T1 to T9 of design 0065 between two machines, over LAN TCP, LAN QUIC and a
relay, plus the loopback scenarios that the 2026-11-19 record left out (T1 at 8 GiB, T2 at 100,000
files, T5, T9).

FILES: `docs/design/0065-large-transfer-and-long-run-tests.md` (D1, D2, the T1 to T9 table, Q58),
`benchmarks/endurance/endurance.py` and `README.md`, `benchmarks/endurance/results/`, TEST-24 in
`docs/BACKLOG.md`.

DO:
1. Build `cargo build --release --locked --bin oxideswarm` on both machines from the same commit.
2. Start a master for the run only, with a temporary home, its own ports and a secret file made for
   the run (`oxideswarm secret generate` into the temporary home); never use the trial cluster or a
   real `~/.oxideswarm`. Copy that run secret to the other machine by hand, never through a script
   that carries secrets.
3. Run `endurance.py transfer` for each link kind as the README describes, and commit one JSON
   record per link under `benchmarks/endurance/results/` (no host names or private IPs in records).
4. For every failed check, open an item or write a failing regression test. T5 and T9 over the
   relay answer Q58: write what they show in Q58's row without changing the store.

DONE WHEN: the records of T1 to T9 for loopback and two machines (LAN TCP, LAN QUIC, relay) are
committed, every failed check has its own item or regression test, and TEST-24 is set to Implemented.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S7: TEST-25 step 2, pull request 395 and the 24-hour three-machine run

Machines: the Mac as master and two Windows workers (the case PC and one more), on one LAN, for 24
hours. The owner must agree to the time window first.

GOAL: finish pull request 395 (`win/test-25-remote-workers`: `soak --master-only` and `worker-agent`),
then record one 24-hour soak with a Mac master and two Windows workers (design 0065 D3, D4.3, Q57).

DO:
1. Read `gh pr view 395` and its comments. Merge `origin/master` into the branch (merge, not
   rebase), run its opt-in real-binary end-to-end test on the newest head as its description says,
   push, and get CI green. It merges only after that end-to-end run is recorded in the pull request.
2. After it merges: run `endurance.py soak --duration 24h` with `--master-only` on the Mac and
   `worker-agent` on each Windows machine, temporary homes, run-only secret, own ports. Do not stop it
   early unless the owner says so; if stopped, the record must state the real duration.
3. Commit the record under `benchmarks/endurance/results/`, evaluate all five checks of D3, and for
   every failed check open an item or write a failing regression test. The 57 worker watchdog
   timeouts of the 22.7-hour loopback record (`124` exits during 25 to 52 s master log gaps) need
   their own item if they recur.

DONE WHEN: pull request 395 is merged, the 24-hour three-machine record is committed with all five
checks evaluated, and TEST-25 is set to Implemented or has items for its failed checks.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S8: Run the features merged on 2026-11-22 by hand on Windows

Machine: the Windows case PC. After S1, S3 and S4. About half a day.

GOAL: run the features of pull requests 410, 413 and 414 for real on Windows and turn every defect
into a BACKLOG item with a failing test. CI ran only their unit and loopback tests; nobody has used
them by hand on Windows.

FILES: [design 0101](../design/0101-matrix-fanout.md) (AX-1), [0102](../design/0102-process-task-checkpoints.md)
(FEAT-5), [0103](../design/0103-ax2-sharded-runs.md) (AX-2), [0107](../design/0107-mesh2-verified-delivery.md)
(MESH-2), `crates/master/src/members.rs` and `crates/master/src/web_ui/members.rs` (ENROLL-1d).

DO: build `cargo build --locked --bin rusty-grid`; start one master and two workers on loopback, each
with a temporary `USERPROFILE`/`HOME`, and stop them by PID. Record each command and its output.
1. AX-1 matrix fan-out: `submit --matrix env:FOO=a,b,c --command cmd -- /c echo %FOO%`, a two-axis
   matrix, a cell no worker can take (`--matrix os=linux`) that must fail fast without hanging, and
   `--output` files that land in separate `<output-dir>/<cell>/` folders.
2. AX-2 shards: `shard --count 4` with a command that prints `OX_SHARD_INDEX` and `OX_SHARD_TOTAL`;
   both workers must get shards; `--fail-fast` with one shard that exits 1; `--build-once --junit-out`
   on a small cargo crate if `cargo-nextest` is installed (otherwise skip and say so).
3. FEAT-5 process checkpoints: a task that writes a counter into `%OXIDESWARM_CHECKPOINT_DIR%` every
   second; kill its worker mid-run; the task must resume from the counter on the other worker. Repeat
   with the worker started with `--isolation fs`: no checkpoint may be captured, the worker logs the
   one-time warning of design 0102 D5, and the task still finishes.
4. ENROLL-1d members mode: create an invite, approve it, revoke by name and by id prefix; the master
   log has one line per action that names the actor.
5. MESH-2: `cargo build -p agent_mesh`, run the hub and two `scripts/agent_node.py` nodes, send a
   file over 1 MiB (chunked) and one over `--inbox-max-mb` (expect `ERR_INBOX_FULL`).

For every defect: claim an item id (`python3 scripts/agent_bridge.py claim item --title "..."`; if the
hub times out, take the next free number after checking `docs/BACKLOG.md` and `gh pr list --state all`,
and say so in the pull request), write a test that fails without the fix, and fix it only if the fix
is under about 100 lines and changes no wire format; otherwise leave the item Planned with the test
marked `#[ignore = "<ITEM-ID>"]`.

DONE WHEN: one pull request records all five checks (pass, fail, or skipped with the reason) in the
item texts or in a new section of this guide, with the new BACKLOG items and green fmt, clippy and
touched tests.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S9: Real GPU stability record on the case PC

Machine: the Windows case PC (NVIDIA GPU). About two hours.

GOAL: record that the real-GPU path (the wgpu engine) is stable on this machine, and that VRAM
routing ([design 0105](../design/0105-3d2-vram-blender-routing.md)) places tasks correctly with a
real GPU worker.

FILES: `crates/worker/src/gpu.rs`, `crates/worker/src/wgpu_engine.rs`, `crates/master/src/scheduler.rs`
(`required_vram_mb`), `docs/BACKLOG.md`.

NOTES: on this PC a GPU worker needs `--sandbox-base-dir` under `%LOCALAPPDATA%` (seen on
2026-11-16). A worker does not detect its VRAM; pass `--vram-mb` with the value `nvidia-smi` shows.

DO:
1. `nvidia-smi`: record GPU model, VRAM and driver (no host name).
2. 50 runs of `cargo test --locked -p rusty_grid_worker --features gpu-wgpu` with
   `RUSTY_GRID_REQUIRE_GPU=1`; count passes and failures, keep the log of any failure.
3. `cargo run --locked --release -p rusty_grid_worker --features gpu-wgpu --example gemm_bench -- 1024`
   and `-- 2048`, three runs each; record the times and the adapter line.
4. `cargo build --locked --release --bin oxideswarm --features gpu-wgpu,3d`. On loopback (temporary
   `USERPROFILE`, own ports, stop by PID): one master, one GPU worker (`--gpu --vram-mb <real>`) and
   one CPU-only worker. Submit 20 `--gpu` GEMM tasks: all land on the GPU worker and print
   `VERIFIED_OK`. A `Generative3D` task with engine `triposr` and quality `high` must be placed on the
   GPU worker only (the stub engine then fails; that is expected, record the placement). An `auto` or
   `sdf` task may run on the CPU-only worker.
5. Kill the GPU worker while a GEMM task runs: the task is requeued and never runs on the CPU worker
   (a real-GPU task never falls back to the CPU).

Record it under a new TEST item (claim the id with `python3 scripts/agent_bridge.py claim item --title
"Real GPU stability record on Windows"`, same fallback as S8). A failure gets its own item and a
failing test.

DONE WHEN: one pull request with the record (CPU, cores, GPU, VRAM, driver, commit, commands, counts),
the Index row and the status log line.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## S10: sysinfo 0.40, in place of Dependabot's pull request 346

Machine: the Windows case PC (any machine can build it; the telemetry check needs Windows). About
two hours.

GOAL: move the workspace from `sysinfo` 0.30 to 0.40.3 with clippy, unit tests, `cargo deny` and the
fuzz builds green on all three systems. Pull request 346 bumps only the version and fails clippy
(macOS, Windows), `gpu-wgpu`, `cargo deny` and three fuzz jobs. Do not push to its branch or close it.

FILES: every `Cargo.toml` that names `sysinfo`, `Cargo.lock`, `fuzz/Cargo.lock`, `deny.toml`,
`crates/worker/src/heartbeat.rs` and the other callers `git grep sysinfo` finds, `CHANGELOG.md`.

DO:
1. Check the `rust-version` of sysinfo 0.40.3; if it is above the workspace MSRV (1.91), stop and
   report. Do not raise the MSRV.
2. Bump it everywhere, update both lock files (`--locked` must still work), and fix every API change
   (refresh calls, CPU usage, memory units, process lookup). The numbers the worker reports keep their
   units (heartbeat telemetry, `workers` output).
3. `cargo deny check`; fix findings without loosening `deny.toml`, or stop and report.
4. On loopback (temporary `USERPROFILE`, own ports, stop by PID) compare `rusty-grid workers --json`
   CPU cores, RAM and load with Task Manager, once with an `origin/master` build and once with yours.
5. fmt; `cargo clippy --workspace --all-targets --locked -- -D warnings`, again with `--features 3d`
   and `--features otel`; `cargo clippy -p rusty_grid_cli --all-targets --features gpu-wgpu --locked -- -D warnings`;
   `cargo test --workspace --lib --locked`; `cargo check -p rusty_grid_master --no-default-features`.

DONE WHEN: a pull request "deps: sysinfo 0.40.3 (supersedes #346)" with green local checks, the
before and after telemetry table, and a CHANGELOG line under `[Unreleased]`.

Common rules: see "Rules for every prompt" in `docs/guides/stability-0.4.x-handoff.md`.

---

## Order

Run S1 and S2 first (they are the only code changes and S1's test is the newest failure), S3 and S4
on idle machine time, S5 every day from now on. S8, S9 and S10 (added 2026-11-23) follow S1,
S3 and S4 on the case PC, in that order. S6 and S7 finish TEST-24 and TEST-25 but do not
block the Q80 exit check, so run them when the machines and the owner are free.

---

## S8 Windows manual check record (2026-11-23)

- **Pull request**: 422 (`win/s8-windows-manual-check`)
- **Commit**: `8e55dac9` (`origin/master`, PRs 410, 413 and 414)
- **Machine**: Windows 11 Pro `x86_64`, `Intel Core i5-14600KF` (`14` physical / `20` logical cores); loopback `127.0.0.1` with isolated temporary `USERPROFILE`/`HOME`/`OXIDESWARM_HOME` and separate `TEMP`/`TMP` dirs per process; all processes stopped cleanly by PID.
- **Build**: `cargo +1.96.0 build --locked --bin rusty-grid -p rusty_grid_cli` and `cargo +1.96.0 build --locked -p agent_mesh`.
- **Defects found**: none (`0` new BACKLOG items).

1. **AX-1 matrix fan-out — PASS**
   - 1D axis (`submit --matrix env:FOO=a,b,c --wait --json --command cmd -- /c echo FOO=%FOO%`): exit `0`, 3 cells returned `FOO=a`, `FOO=b`, `FOO=c`.
   - 2-axis (`submit --matrix env:A=1,2 --matrix env:B=x,y --wait --json --command cmd -- /c echo %A%-%B%`): exit `0`, 4 cells returned `1-x`, `1-y`, `2-x`, `2-y`.
   - Unplaceable cell (`submit --matrix os=linux --wait --command cmd -- /c echo hi`): failed fast in `0.031 s` (`0.002 s` wait reported by CLI) with exit `1`, `"status": "unplaced"`, and `no connected worker matches cell constraints (os=linux)`.
   - Per-cell `--output` (`submit --matrix env:CELL=alpha,beta --output out.txt --output-dir <out> --wait --json --command cmd -- /c echo %CELL%>out.txt`): exit `0`, outputs saved separately to `<out>/cell-00-alpha/out.txt` (`alpha`) and `<out>/cell-01-beta/out.txt` (`beta`).
2. **AX-2 shards — PASS (with 1 sub-check skipped)**
   - 4 shards across 2 workers (`shard --count 4 --json --type command --command python3 -- -c ...OX_SHARD_INDEX/OX_SHARD_TOTAL...`): exit `0`, `machines: 2`, outputs `SHARD=1/4`, `SHARD=2/4`, `SHARD=3/4`, `SHARD=4/4`, 2 shards executed on `win-w1` and 2 on `win-w2`.
   - Fail-fast (`shard --count 4 --fail-fast --json ...` with shard `1` exiting `1` at `0.2 s` and shards `2..4` sleeping `25 s`): exited `1` in `0.344 s` with `failed_shards: 1` and `cancelled_shards: 3`.
   - `--build-once --junit-out`: **SKIPPED** (`cargo +1.96.0 nextest --version` returned `error: no such command: nextest`; `cargo-nextest` is not installed on this machine).
3. **FEAT-5 process checkpoints — PASS**
   - `--isolation off` failover (master with `--heartbeat-interval-secs 1 --heartbeat-timeout-secs 3 --reaper-interval-secs 1 --session-resume-hold-secs 0`; two workers; Python task writing `%OXIDESWARM_CHECKPOINT_DIR%\counter.txt` every `1 s` up to `7`): started on `ckpt-w2`, master logged debug checkpoints `sequence=1`, `2`, `3`; killed `ckpt-w2` (`TerminateProcess` / PID `25092`); task resumed on `ckpt-w1` from `RESUME_START=3`, logged checkpoints `sequence=4`, `5`, `6`, and completed with `DONE=7` (exit `0`).
   - `--isolation fs` (worker `iso-fs-w1` with `RUST_LOG=info`): ran two `cmd /c` tasks (`"echo probe>%OXIDESWARM_CHECKPOINT_DIR%\probe.txt"`); both exited `0` (`ISO_DONE=ok`, `SECOND_DONE=ok`), master recorded `0` mid-task checkpoints (`debug` log absent), and worker logged the design 0102 D5 warning exactly once: `process task checkpoints are not captured on this system under --isolation (design 0102 D5); tasks still run and resume from checkpoints they are given`.
4. **ENROLL-1d members mode — PASS**
   - Started master with `--auth members --auth-token <token>:win-admin:admin`. Created a 2-use invite via `POST /api/invites`, joined `win-member-a` and `win-member-b` interactively via `rusty-grid join --no-doctor` in Windows ConPTY, approved both via `POST /api/members/{pending}/approve`, revoked `win-member-a` by name (`POST /api/members/win-member-a/revoke`), and revoked `win-member-b` by 8-char ID prefix (`POST /api/members/b49e1f63/revoke`).
   - Master log emitted all 5 audit lines (`dashboard: invite created`, 2x `dashboard: member approved`, 2x `dashboard: member revoked`), every line naming `actor="win-admin"`.
5. **MESH-2 verified delivery — PASS**
   - Started `agent-mesh hub`, two `scripts/agent_node.py` nodes (`py-node-a`, `py-node-b`), and `agent-mesh node --inbox-dir <dir> --inbox-max-mb 1 --inbox-max-files 10`.
   - Sent `1,572,864` bytes (`1.5 MiB`) to `py-node-b` via `agent-mesh data --payload-file ... --timeout-ms 60000`: exit `0`, `Chunks: 2`, `Responder: py-node-b (data_received_verified)`, and `py-node-b` logged `Received valid DataPayload (1572864 bytes, SHA-256 verified)`.
   - Sent `204,800` bytes (`200 KiB`) to `rust-inbox-node`: exit `0`, persisted on disk with matching SHA-256. Sent `1,572,864` bytes (`1.5 MiB`) to `rust-inbox-node` under `--inbox-max-mb 1`: exit `1` with `ERR_INBOX_FULL`.
