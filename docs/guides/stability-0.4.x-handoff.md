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

## Order

Run S1 and S2 first (they are the only code changes and S1's test is the newest failure), S3 and S4
on idle machine time, S5 every day from now on. S6 and S7 finish TEST-24 and TEST-25 but do not
block the Q80 exit check, so run them when the machines and the owner are free.
