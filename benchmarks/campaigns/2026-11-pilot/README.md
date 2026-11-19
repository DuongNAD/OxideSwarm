# Pilot campaign 2026-11: Anima Engine deterministic sweep (`APP-6`)

- **Status:** Partial (`APP-6` steps 1 and 4 committed). The Anima Engine runner is ready: Anima Engine pull request 167 added `run_esel1_experiment --trial`, and [`manifest.json`](manifest.json) pins it with `runner_ready: true`. The organiser builds the per-system runner binaries and fills in their hashes before the dry run. Dry run step 2 runs on `0.4.4` without `NET-9` or `NET-10`. Full pilot step 3 waits for `NET-9`, `NET-10`, and the owner's participant list.
- **Decisions:** Q94 (workload: Anima Engine sweep in deterministic CPU mode) and Q95 (organiser plus 3 to 5 people they know, about 10 machines, at least two home networks, and at least two machines of each OS and CPU architecture in use), decided on `2026-11-19` in [design 0094](../../../docs/design/0094-road-to-a-shared-research-grid.md). The owner's exception to Anima Engine's rule `QĐ-OXS` for this pilot, decided on `2026-11-19`, is in Section 1.
- **Rollout order:** First a dry run of `APP-6` step 2 with 2 to 3 participants (`P1` to `P3`) on released `0.4.4` (no `NET-9` or `NET-10` required): a `--limit` slice of [`manifest.json`](manifest.json) once the runner hashes are filled in, or the stand-in [`dry_run_manifest.json`](dry_run_manifest.json), which needs no Anima Engine binary. Then the full 10-machine Anima Engine pilot (`APP-6` step 3) after `NET-9` (contributor controls) and `NET-10` (display-only credits) land ([`docs/BACKLOG.md`](../../../docs/BACKLOG.md)).

## 1. Workload

- **Target program:** the headless Anima Engine E-SEL1 runner `run_esel1_experiment` in trial mode (`--trial`), built from `src-tauri/examples/run_esel1_experiment.rs` in the `Anima-Engine` repository.
- **Pinned commit:** `963f436e68c7197bace3e9e7bf86bd8cd6a6601e` on branch `feat/ecological-selection`, the merge commit of Anima Engine pull request 167 (head `c2e3ba69`, CI 4 of 4 green), recorded as `workload.commit` in [`manifest.json`](manifest.json) with `workload.runner_ready: true`.
  The earlier pin `7a825cc` could not run the grid: under `--smoke` it ran only the smoke seed at `18,000` ticks and had no parameter flags, it exited `2` without a staged `manifests/esel1-preregistration.json`, and its `checksums.sha256` hashed files with wall-clock values. Trial mode removes all three blockers.
- **One work unit:**

```text
run_esel1_experiment --trial --out out --seed <SEED> --founders <N0> --food-cap-abundant <FA> --food-cap-scarce <FS> --duration-ticks <T>
```

  - `--trial` is required: the runner never infers a mode.
  - A flag left out takes the value of the E-SEL1 smoke manifest (`N0` 30, `FA` 50, `FS` 25, `18,000` ticks). [`manifest.json`](manifest.json) passes all five.
  - Each trial runs three arms: `control` at `FA`, `treatment` at `FS`, and `drift` at `FS` with mutation rate 0.
  - `--seed` wins over `ANIMA_SIM_SEED`. When both are set and differ, the runner refuses with exit code `2`. [`../driver.py`](../driver.py) sets both to the unit's seed.
  - Refused with exit code `2` before a tick runs: the E-SEL1 confirmation seeds `760001`, `761001`, ..., `771001`, equal food caps, founders above `4096`, and a flag given twice or with a non-integer value. The sweep seeds `100001` to `100010` are allowed.
  - A task stages no files besides the runner itself: the preregistration and the three smoke manifests are compiled into the binary. The manifest passes no `--manifest-dir`, and the driver needs no `--extra-input`.
  - Output in `out/trial/`: `verify_checksums.txt`, `runs.csv`, `trial-series.csv`, `provenance.json`, `checksums.sha256`, and `timing.json`. Only `timing.json` holds wall-clock values. No body-mass observable is written (the blinding of the E-SEL1 smoke).
  - Source: `docs/ai/planning/2026-11-19-esel1-runner-trial-mode.md` in `Anima-Engine` at the pinned commit.
- **Stand-in workload for step 2 (`APP-6` dry run on `0.4.4`, optional):**
  The committed deterministic Python stand-in [`standin_workload.py`](standin_workload.py) and [`dry_run_manifest.json`](dry_run_manifest.json) rehearse the dry run without an Anima Engine binary. It takes `--seed`, `--founders`, `--food-cap-abundant`, `--food-cap-scarce`, `--duration-ticks`, and `--out`, computes three synthetic deterministic arms (`control`, `treatment`, `drift`), writes deterministic `out/smoke/verify_checksums.txt` (`seed,arm,final_checksum`), and writes non-deterministic wall-clock timings into `out/smoke/runs.csv` so `--verify-file out/smoke/verify_checksums.txt` exercises real selective verification under `--isolation fs` (or `fs+net`).
- **How a participant gets the runner, and how its hash is checked:** a participant never builds, downloads or installs Anima Engine. The organiser's driver stages the binary with every task, after checking its hash.
  1. Before the dry run, the organiser builds one release binary per target `(os, arch)` pair at the pinned commit, in the `Anima-Engine` repository (never in OxideSwarm).
     On the organiser machine of that system, in a checkout of `Anima-Engine` at the pinned commit, from `src-tauri/`:

```bash
cargo build --release --features desktop --example run_esel1_experiment
```

     The binary is `run_esel1_experiment` (`run_esel1_experiment.exe` on Windows) under `release/examples/` in the Cargo target directory.
  2. The organiser computes the SHA-256 digest of each binary, records it under `workload.runner_sha256` in [`manifest.json`](manifest.json) (replacing `<fill-before-run>`), and commits the manifest before the run. A hash comes only from the organiser's build for that system, never from a participant's own build.
     - On macOS/Linux (organiser machine):

```bash
shasum -a 256 <RUNNER_BINARY_PATH>
```

     - On Windows (organiser machine):

```powershell
Get-FileHash -Algorithm SHA256 <RUNNER_BINARY_PATH>
```

  3. When [`../driver.py`](../driver.py) runs with `--runner <RUNNER_BINARY_PATH> --runner-system <os>/<arch>`, it checks the local file's SHA-256 digest against `manifest.json` and refuses a different digest or a `<fill-before-run>` placeholder (`verify_runner_hash`). It then stages the binary as a content-addressed BLAKE3 input (`--input`, [design 0006](../../../docs/design/0006-content-addressed-data-plane.md)) named `run_esel1_experiment`, or `run_esel1_experiment.exe` for a `windows/...` system, because a Windows worker starts only `.exe`, `.com`, `.bat` and `.cmd` files ([design 0025](../../../docs/design/0025-windows-executable-resolution.md)). It pins every submitted task to `--os <os> --arch <arch>`.
- **Deterministic CPU settings:** every task sets `ANIMA_DETERMINISTIC=1`, `ANIMA_USE_GPU=0`, and `ANIMA_SIM_SEED=<SEED>` (design 0089, Context and D1). In trial mode the runner installs deterministic mode and a CPU brain whatever these variables say, and records them in `provenance.json`. Workers advertise CPU execution only (`--no-gpu`).
- **Cross-system behaviour (`VERIFY-2`):** on identical inputs at commit `7a825cc` (`n0-30_fa-50_fs-13`, seed `999971`, `18,000` ticks), repeat deterministic CPU `final_checksum` values on the same `(os, arch)` match identically, whereas `macos/aarch64` (`1859816024`) and `windows/x86_64` (`2546342991`) differ (design 0089 D6). Trial mode keeps this: `final_checksum` differs across OS and architecture. Replicas therefore run on the same `(os, arch)` by default, never pass `--verify-any-system`, and each runner binary targets one `(os, arch)` pool via `--runner-system <os>/<arch>`.
- **Secrets rule:** the runner binary is the only staged input. Never stage `.env` or any file containing `GEMINI_API_KEY`, Neo4j credentials, or cluster secrets as a task `--input` (design 0089, Security). [`../driver.py`](../driver.py) rejects forbidden names lexically and recursively walks every directory `--input` (including any `--extra-input`) to refuse `.env`, `.env.*`, `*.secret`, or `invite.oxs` anywhere inside.

### Owner's exception to Anima Engine rule `QĐ-OXS`

Anima Engine's workflow rule `QĐ-OXS` (2026-11-19) sends Anima Engine work on OxideSwarm only to grid workers, never to friends' machines. On 2026-11-19 the owner answered "Yes, allow it" in the coordinator's chat: this pilot may run on the machines of the participants the owner invites. The exception covers only this pilot, and only while all three conditions hold:

1. Deterministic CPU only (`ANIMA_USE_GPU=0`, workers started with `--no-gpu`).
2. Every result is verified by a replica on another machine of the same OS and architecture (`--verify 1`, Section 3).
3. No secrets or private data in the inputs (the runner binary is the only input; Secrets rule above).

Any other Anima Engine run on participants' machines needs a new decision by the owner. The decision is also noted in the description of Anima Engine pull request 167.

### What this sweep can answer

The sweep mainly tests the grid: whether about five people and ten machines complete 200 verified units, the exit check of Stage 1 ([design 0094](../../../docs/design/0094-road-to-a-shared-research-grid.md) D4). It answers little about biology. The earlier E-SEL1 smokes found the food cap a weak scarcity factor in this world: it moved the grass (standing crop) by at most about 6 % between caps. A trial is exploratory and not part of the E-SEL1 preregistration. The first Stage 2 campaign is chosen after this pilot's record, by the rule of Q94 in design 0094: independent tasks of minutes, results that a checksum or a cheap check verifies, code and outputs publishable under [design 0057](../../../docs/design/0057-open-results-charter.md), and no personal data in the inputs.

## 2. Input set and runtime estimate

[`manifest.json`](manifest.json) defines **200** independent work units (`u001` through `u200`) across three axes:

| Axis | Values | Count |
|---|---|---|
| `live.founders` | `30`, `45`, `60`, `80` | `4` |
| `(live.food_cap_abundant, live.food_cap_scarce)` | `(50, 25)`, `(50, 17)`, `(50, 13)`, `(50, 8)`, `(50, 4)` | `5` |
| `ANIMA_SIM_SEED` | `100001` through `100010` | `10` |
| **Total grid** | `4 x 5 x 10` at `duration_ticks = 450,000` per arm | **`200` units (`400` runs with `--verify 1`)** |

The step-2 dry run takes either a `--limit` slice of [`manifest.json`](manifest.json) with the Anima Engine runner, or the stand-in [`dry_run_manifest.json`](dry_run_manifest.json), a 12-unit subset (`2` founders x `3` food pairs x `2` seeds at `duration_ticks = 18,000`) for [`standin_workload.py`](standin_workload.py).

### Runtime estimate and basis (step 3 Anima Engine sweep)

- **Measured:** the Anima Engine trial-mode note (Section 1, Source; times from `timing.json` and `/usr/bin/time -p`) gives, on one Apple M5 (`macos/aarch64`), release build, deterministic CPU, one core per trial, on a machine shared with other jobs:
  - one `18,000`-tick trial (`N0` 30, `FA` 50, `FS` 25, three arms): `10.9 s` and `11.7 s`;
  - one `450,000`-tick unit (three arms): `349 s` (`N0` 30, `FA` 50, `FS` 25) and `199 s` (`N0` 80, `FA` 50, `FS` 4, where two arms went extinct early).
  - The short trial scales to the long unit: `25 x 10.9` to `25 x 11.7 s` = `273` to `293 s`, inside the measured `199` to `349 s`.
- **Per execution:** the estimate takes `199` to `349 s` for one unit run once. It rests on two units, not the whole grid: a unit whose three populations all survive may take longer than `349 s`. `windows/x86_64` has no `--trial` measurement yet, and the table assumes the same range there.
- **Work:** `200` units x `2` executions (`--verify 1`: one primary and one replica) = `400` executions. That is `400 x 199 s = 79,600 s` (`22.1 h`) to `400 x 349 s = 139,600 s` (`38.8 h`) of slot time in all.
- **Split by system:** each runner binary serves one `(os, arch)` pool, so the units are divided between the pools by machine count. Example with `10` machines: `6` `macos/aarch64` machines take `120` units (`240` executions) and `4` `windows/x86_64` machines take `80` units (`160` executions), so every machine runs `40` executions. With `--shard K/5` the manifest splits into five shards of `40` units, each the share of two machines: shards `1/5` to `3/5` go to the macOS pool and `4/5` and `5/5` to the Windows pool (Section 8).
- **Wall clock:** per pool, each slot runs `40 / s` executions, where `s` is the slots per machine (`--max-concurrency`). The last units add a tail of at most one primary and its replica (`2 x 199 s` to `2 x 349 s` = `6.6` to `11.6 min`), because a replica starts only after its primary. Both pools take the same time, since both run `40` executions per machine.

| Slots per machine (`--max-concurrency`) | Slots (`6` macOS + `4` Windows) | Executions per slot (`40 / s`) | Compute per slot (`199` to `349 s` each) | Tail | Estimated wall clock |
|---|---|---|---|---|---|
| `2` | `12` + `8` = `20` | `20` | `3,980` to `6,980 s` (`66` to `116 min`) | up to `6.6` to `11.6 min` | **`1.2` to `2.1 h`** |
| `3` | `18` + `12` = `30` | `13.3` (`14` on the busiest slot) | `14 x`: `2,786` to `4,886 s` (`46` to `81 min`) | up to `6.6` to `11.6 min` | **`0.9` to `1.6 h`** |
| `4` | `24` + `16` = `40` | `10` | `1,990` to `3,490 s` (`33` to `58 min`) | up to `6.6` to `11.6 min` | **`0.7` to `1.2 h`** |

- **Not in the table:** the runner binary is the only staged input (the master stores it once by its BLAKE3 id, design 0006), each verification mismatch adds one rerun of its unit, and the per-task timeout of `1,800 s` is `5.2 x` the slowest measured unit.
- **Assumption check:** the M5 numbers come from one trial at a time. Two to four trials at once on one machine, slower contributor machines, and `--task-priority below-normal` under the contributor's own work can each slow an execution. Measure the per-execution time on the dry run's `--limit` slice and scale the table by it before step 3; the target is `1` to `3 h`.

## 3. Verification rule

Every unit is submitted through:

```text
oxideswarm submit --json --wait --verify 1 --verify-file out/trial/verify_checksums.txt --os <OS> --arch <ARCH> ...
```

- **Why `verify_checksums.txt`:** the runner writes `out/trial/verify_checksums.txt` for this purpose: three sorted LF lines `seed,arm,final_checksum` (`control`, `drift`, `treatment`), byte-identical across runs on the same OS and architecture. Design 0089 D6's deterministic equality is this `final_checksum` of each `(seed, arm)`. The trial's `checksums.sha256` also holds no wall-clock value (only `timing.json` does), but it covers `provenance.json`, which records how the seed was passed (`--seed`, `ANIMA_SIM_SEED`, or both). `verify_checksums.txt` depends on neither.
- **Replica selection (`crates/cli/src/verify.rs`):** `--verify 1` runs the task once on a matching worker pinned to the target `--os`/`--arch`, then runs one replica pinned (`require:worker=<id>`) to a second connected worker reporting the same normalized `os` and `arch`. Both copies of `out/trial/verify_checksums.txt` are compared by BLAKE3 digest.
- **What a mismatch triggers (`verify.status == "mismatch"`):**
  1. [`../driver.py`](../driver.py) records attempt 1 (sanitized worker labels, participant labels, `os`/`arch`, and differing digests). (A task exiting with exit code `4` without `verify.status == "mismatch"` is classified as `failed`, not `mismatch`.)
  2. [`../driver.py`](../driver.py) automatically reruns that unit once with the same `--verify 1 --verify-file out/trial/verify_checksums.txt` flags and records attempt 2 in the same unit entry.
  3. Any unit that mismatched at least once is marked `needs_investigation: true` regardless of attempt 2's outcome:
     - If attempt 2 verifies (`verified_on_rerun`): keep both records, flag the machine pair from attempt 1 in the campaign report, and inspect outputs before accepting the unit.
     - If attempt 2 also mismatches (`mismatch_persisted`): exclude that unit from campaign results until root-caused (e.g., bad hardware, dirty runtime environment, non-deterministic edge case in the runner), and pause or revoke any worker behind repeated mismatches.
- **What a verify error triggers (`verify.status == "error"`):** for example, fewer than two online workers of the unit's `(os, arch)`. The unit is recorded as `verify_error`, counted in `summary.pending`, and automatically retried on `--resume` once another same-system worker is online.

## 4. Results and privacy

- **Published campaign record:** written by [`../driver.py`](../driver.py) under `benchmarks/campaigns/2026-11-pilot/results/` as JSON (plus a human-readable dated report for the `APP-6` Done-when check). It reports only:
  - participant labels (`P1` to `P5`) and neutral worker labels (`P1-w1`, `P2-w1`, ...),
  - normalized OS, CPU architecture, logical core count, RAM size, configured isolation mode, link kind and RTT from `doctor` check C7,
  - task execution counts, primary vs replica counts, execution-time sums, verification outcomes (`verified`, `verified_on_rerun`, `mismatch_persisted`, `interrupted`), and scrubbed explanations.
- **What stays out of the record:** real names, machine hostnames, OS user names, `--master` hosts, `--relay-url` hosts, interactive `Hint:` lines, raw worker `--name` values (including late joiners refreshed dynamically via `workers --json`, plus unknown/offline worker fallbacks), worker/task UUIDs, IP addresses, secrets, and invite strings. `tests/test_campaign_driver.py` verifies scrubbing across all of these.
- **Who sees simulation outputs:** only the organiser collects the `out/trial/` files of each unit onto their local machine via `submit --output-dir`. They hold no body-mass observable (Section 1). Participants may request summaries or checksums from the organiser; any public publication of Anima Engine outputs awaits Q52 and follows [design 0057](../../../docs/design/0057-open-results-charter.md).
- **What the organiser's master sees during the live run ([design 0094](../../../docs/design/0094-road-to-a-shared-research-grid.md) D9):** each connected worker's `--name`, OS, architecture, cores, RAM, battery/load telemetry, IP address or relay path, and online hours. Every enrolled member can list active workers with `oxideswarm workers`. Nothing leaves the master automatically (Q39).

## 5. Controls each contributor sets

Full CLI flags and environment variables are documented in [`docs/reference.md`](../../../docs/reference.md) and [`README.md`](../../../README.md).

### Today (`0.4.4`, `crates/cli/src/cli_app.rs` `WorkerArgs`)

Contributors start their pilot worker with these flags:

| Control | Recommended flag for pilot | Purpose |
|---|---|---|
| Filesystem / network isolation | `--isolation fs+net` (or `--isolation fs` if needed) | Confines tasks to their own scratch sandbox; `fs+net` also blocks outbound sockets |
| Concurrency cap | `--max-concurrency 2` (or `<= half of logical cores`) | Keeps host CPU responsive and avoids thermal throttling |
| CPU priority | `--task-priority below-normal` (or `idle`) | Yields CPU to interactive work |
| CPU-only pilot | `--no-gpu` | Ensures the pilot does not touch contributor GPUs |
| Secondary worker portal | `--no-portal` | Avoids binding port `8080` if the contributor runs a personal cluster |
| Readable worker name | `--name <WORKER_NAME>` | Keeps one stable worker identity across restarts |
| Separate profile dir | `OXIDESWARM_HOME=<PILOT_HOME_DIR>` | Keeps pilot keys/config isolated from `~/.oxideswarm` |
| Immediate local stop | `Ctrl+C` or `SIGTERM` on the worker process | `terminate_all_task_process_trees` stops active tasks at once |

### Planned controls for step 3 (`NET-9`, `ECO-9`, `NET-10`, **Planned**)

Step 2 (the dry run) runs on `0.4.4` without waiting for these. The full Stage 1 pilot (step 3) waits for three **Planned** features of [design 0094](../../../docs/design/0094-road-to-a-shared-research-grid.md) D4:

- **`NET-9` (Planned):** contributor-enforced host CPU cap, weekly active hours window and daily cap, automatic pause on battery and metered networks, and a local pause/stop switch ending tasks within `5 s` cleanly without charging a retry.
- **`ECO-9` (Planned):** per-participant quotas (running tasks, slots, priority, fair-share key) persisted on the master.
- **`NET-10` (Planned):** display-only credit ledger (`core-seconds` contributed vs used, zero-sum, non-transferable, no cash value, Q96).

## 6. Participants by label (`P1` to `P5`, Q95)

The circle uses participant labels only (`P1` to `P5`), with `~10` machines across at least two home networks (`N1`, `N2`, ...) and at least two machines per `(os, arch)` pair so `--verify 1` always finds a same-system replica. Contributors enroll with role `worker` only; only `P1` holds `admin,client,worker`.

| Label | Role | Target home network | Target OS / arch coverage | Dry run (`step 2`) | Full pilot (`step 3`) | Controls set before start |
|---|---|---|---|---|---|---|
| `P1` (Organiser) | `admin,client,worker` | `N1` | e.g. `macos/aarch64`, `windows/x86_64` | Included | Included | Recorded per run |
| `P2` | `worker` | `N2` | Shares OS/arch with >= 1 other worker | Included | Included | Recorded per run |
| `P3` | `worker` | `N2` or `N3` | Shares OS/arch with >= 1 other worker | Optional | Included | Recorded per run |
| `P4` | `worker` | `N3` or `N4` | Shares OS/arch with >= 1 other worker | — | Included | Recorded per run |
| `P5` | `worker` | `N4` or `N5` | Shares OS/arch with >= 1 other worker | — | Included | Recorded per run |

Fill a machine-mapping file locally (kept outside git, passed via `--participants <PATH>`) mapping each enrolled worker name or UUID to `P1`..`P5` before running [`../driver.py`](../driver.py), or tag workers at start with `--tag participant:P2`.

## 7. Invite flow for another network (`invite` + `join` + `members approve`)

This flow uses OXS-AUTH v3 and P2P tickets built in [design 0051](../../../docs/design/0051-machine-identity-and-invites.md) D7 (`ENROLL-1b`). Do not use `--no-approval` for remote contributors.

1. On the organiser machine (`P1`), start the master with P2P enabled and member authentication, using a dedicated state directory if needed:

```bash
oxideswarm master --p2p --auth members --state-dir <MASTER_STATE_DIR>
```

2. On the organiser machine (`P1`), generate one single-use invite (default role `worker`, default approval required) and send the printed `oxs1-invite:...` line to the contributor over private chat:

```bash
oxideswarm invite --roles worker --uses 1 --expires 24h
```

3. On the contributor machine (macOS/Linux; use a separate `OXIDESWARM_HOME` if you already run your own cluster so `~/.oxideswarm` stays untouched), copy the invite string to the clipboard, pipe it into `oxideswarm join` over standard input (never pass an invite as a command argument), and read the printed 6-digit pairing code `<NNNNNN>` and fingerprint back to `P1`:

```bash
export OXIDESWARM_HOME="$HOME/.oxideswarm-pilot"
pbpaste | oxideswarm join
```

   On Linux with X11/Wayland, substitute `xclip -o -selection clipboard` or `wl-paste` for `pbpaste`. On Windows PowerShell:

```powershell
$env:OXIDESWARM_HOME = "$env:USERPROFILE\.oxideswarm-pilot"
Get-Clipboard | oxideswarm join
```

4. On the organiser machine (`P1`), inspect pending requests and approve once you confirm the 6-digit pairing code `<NNNNNN>` matches:

```bash
oxideswarm members pending
oxideswarm members approve <PENDING_ID> --code <NNNNNN>
```

5. On the contributor machine (macOS/Linux), start the worker with isolation and resource limits:

```bash
export OXIDESWARM_HOME="$HOME/.oxideswarm-pilot"
oxideswarm worker --isolation fs+net --max-concurrency 2 --task-priority below-normal --no-gpu --no-portal --name <WORKER_NAME>
```

   On Windows PowerShell:

```powershell
$env:OXIDESWARM_HOME = "$env:USERPROFILE\.oxideswarm-pilot"
oxideswarm worker --isolation fs+net --max-concurrency 2 --task-priority below-normal --no-gpu --no-portal --name <WORKER_NAME>
```

6. On the organiser machine (`P1`), verify every worker registered and check `C7` link kinds before starting the sweep:

```bash
oxideswarm workers
oxideswarm doctor --expect-workers <N>
```

7. When the run finishes, or whenever a participant wants to leave, press `Ctrl+C` in the worker terminal; on `P1`, revoke the machine key if membership has ended:

```bash
oxideswarm members revoke <MEMBER_NAME_OR_ID>
```

## 8. Running the sweep (`driver.py`)

### Step 2: Dry run on released `0.4.4`

The dry run uses one of two workloads:

- **Anima Engine runner,** once the organiser has filled in `workload.runner_sha256`: a `--limit 6` slice of one shard of [`manifest.json`](manifest.json), run as in step 3 below with `--limit 6` added and a `-dry-run` record name. Six units are `12` executions: on four slots, `3` per slot at `199` to `349 s` (`10` to `17 min`), plus a tail of up to `6.6` to `11.6 min`. Its per-execution times feed the assumption check of Section 2.
- **Stand-in workload:** [`dry_run_manifest.json`](dry_run_manifest.json) with [`standin_workload.py`](standin_workload.py), which needs no Anima Engine binary.

For the stand-in, preview first on the organiser machine (`--dry-run`, contacts nothing):

```bash
python3 benchmarks/campaigns/driver.py \
  --manifest benchmarks/campaigns/2026-11-pilot/dry_run_manifest.json \
  --runner-system macos/aarch64 \
  --command /usr/bin/python3 \
  --participants <PARTICIPANTS_MAP_JSON> \
  --output-dir <TRIAL_OUTPUT_DIR> \
  --record benchmarks/campaigns/2026-11-pilot/results/<YYYY-MM-DD>-dry-run.json \
  --dry-run
```

Run `--limit 6` (or all `12`) against two or more matching workers on the same `(os, arch)` (`--runner-system` pins `--os` and `--arch` on every unit; pass `--command /usr/bin/python3` on macOS under `--isolation fs` / `fs+net`, or `--command python` on Windows). On the organiser machine:

```bash
python3 benchmarks/campaigns/driver.py \
  --manifest benchmarks/campaigns/2026-11-pilot/dry_run_manifest.json \
  --runner-system macos/aarch64 \
  --command /usr/bin/python3 \
  --participants <PARTICIPANTS_MAP_JSON> \
  --output-dir <TRIAL_OUTPUT_DIR> \
  --concurrency 4 \
  --limit 6 \
  --record benchmarks/campaigns/2026-11-pilot/results/<YYYY-MM-DD>-dry-run.json
```

### Step 3: Per-system Anima Engine sweep

The organiser runs one `driver.py` per shard, each with `--runner` and the `--runner-system <OS>/<ARCH>` of the pool that serves it, and its own `--record` file, so every unit receives the right binary and a same-system `--verify 1` replica. All of them run on the organiser machine: `--runner` names a local copy of each system's binary, whose hash the driver checks before staging it.

- With the split of Section 2 (`6` macOS and `4` Windows machines), shards `1/5` to `3/5` run on `macos/aarch64` and shards `4/5` and `5/5` on `windows/x86_64`.
- Each shard is the share of two machines, so set `--concurrency` to `2 x` the slots per machine (`4` at `--max-concurrency 2`, `6` at `3`, `8` at `4`).
- No `--extra-input` is needed: the runner stages nothing else.
- Add `--dry-run` to preview a shard: it checks the runner hash and prints the first commands without contacting the master.

On the organiser machine, macOS shard 1 of 5 (repeat with `--shard 2/5` and `3/5`):

```bash
python3 benchmarks/campaigns/driver.py \
  --manifest benchmarks/campaigns/2026-11-pilot/manifest.json \
  --runner <MACOS_RUNNER_BINARY_PATH> \
  --runner-system macos/aarch64 \
  --shard 1/5 \
  --participants <PARTICIPANTS_MAP_JSON> \
  --output-dir <TRIAL_OUTPUT_DIR> \
  --concurrency 4 \
  --record benchmarks/campaigns/2026-11-pilot/results/<YYYY-MM-DD>-pilot-shard-1-of-5.json
```

On the organiser machine, Windows shard 4 of 5 (repeat with `--shard 5/5`):

```bash
python3 benchmarks/campaigns/driver.py \
  --manifest benchmarks/campaigns/2026-11-pilot/manifest.json \
  --runner <WINDOWS_RUNNER_EXE_PATH> \
  --runner-system windows/x86_64 \
  --shard 4/5 \
  --participants <PARTICIPANTS_MAP_JSON> \
  --output-dir <TRIAL_OUTPUT_DIR> \
  --concurrency 4 \
  --record benchmarks/campaigns/2026-11-pilot/results/<YYYY-MM-DD>-pilot-shard-4-of-5.json
```

### Resuming after `Ctrl+C` or failures (`--resume`, `--retry-failed`)

- On the first `Ctrl+C` (`SIGINT`) / `SIGTERM`, `driver.py` stops starting new units immediately, waits up to `--interrupt-grace-secs` (default `5.0 s`; set `0` to stop active children immediately) for in-flight `submit` calls, terminates any remaining child process groups on a second `Ctrl+C` or when the grace window expires, records affected units with `status: "interrupted"`, and runs `oxideswarm cancel` on any known `task_id` (saving unresolved IDs in `<record>.orphans.json`).
- **Plain `--resume`** keeps `verified`, `verified_on_rerun`, `mismatch_persisted`, and `failed` units, and **automatically resubmits all `interrupted`, `verify_error`, `mismatch_rerun_failed`, and pending units** after cancelling any tracked IDs in `<record>.orphans.json`:

```bash
python3 benchmarks/campaigns/driver.py \
  --manifest benchmarks/campaigns/2026-11-pilot/dry_run_manifest.json \
  --runner-system macos/aarch64 \
  --command /usr/bin/python3 \
  --participants <PARTICIPANTS_MAP_JSON> \
  --output-dir <TRIAL_OUTPUT_DIR> \
  --concurrency 4 \
  --record benchmarks/campaigns/2026-11-pilot/results/<YYYY-MM-DD>-dry-run.json \
  --resume
```

- Pass **`--retry-failed`** alongside `--resume` if you also want to re-run units whose status was `failed`.
- **Master orphan note on `0.4.4`:** if an in-flight `submit --wait` child had to be force-killed before it printed its JSON `task_id` (always check the driver warning on `stderr`), run `oxideswarm status` before `--resume` so no killed submission keeps running on the master alongside its resumed replacement.

---

## One-page participant guide (plain words)

### What you are joining
You are lending spare CPU time on your computer to run a batch of short deterministic trials for a small group of people the organiser knows. Every trial runs the deterministic simulation program `run_esel1_experiment` of the Anima Engine project with different seeds and food limits. The organiser builds it from one fixed version, checks its SHA-256 fingerprint before every run, and sends it with each task, so you install nothing besides `oxideswarm`. A dry run (step 2) may instead use a tiny built-in Python script ([`standin_workload.py`](standin_workload.py)). The project owner allowed this pilot on your machine on three conditions: CPU only, every result checked again on a second machine, and no secrets or private data in what is sent (Section 1). No AI model weights, passwords, or personal files leave or enter your computer.

### What runs on your computer (and what stays protected)
- **Files:** Because your worker starts with `--isolation fs+net`, each trial can only write inside its temporary folder. It cannot read your home folder, documents, or other projects. If a platform restriction prevents `fs+net`, the sheet falls back to `--isolation fs`.
- **Network:** Under `--isolation fs+net`, the simulation process cannot open network connections at all.
- **Hardware:** Tasks use CPU only (`--no-gpu`) and run at `--task-priority below-normal` with `--max-concurrency 2`, leaving your graphics card untouched and your desktop responsive.
- **What the organiser sees while you are online:** your chosen worker node name (`--name`), OS, CPU type, core count, RAM, battery/load percentage, IP or relay route, and connected hours. See [`docs/SECURITY.md`](../../../docs/SECURITY.md).
- **What gets published in the campaign report:** only an anonymous participant label (`P1`..`P5`), OS/architecture, core count, link type, task counts, and pass/mismatch results. Your name, hostname, username, worker name, and IP address are never published.

### Quickstart (five steps)
1. **Install `oxideswarm`** from the signed `0.4.4` (or newer) release archive listed in [`README.md`](../../../README.md).
2. **Keep a separate folder if you already use OxideSwarm:** set `OXIDESWARM_HOME` to `$HOME/.oxideswarm-pilot` (macOS/Linux) or `$env:USERPROFILE\.oxideswarm-pilot` (Windows) in that terminal so your personal settings are untouched.
3. **Accept the invite:** copy the `oxs1-invite:...` string sent by the organiser, then pipe it into `oxideswarm join` (on macOS `pbpaste | oxideswarm join`; on Windows `Get-Clipboard | oxideswarm join`).
4. **Confirm the 6-digit code:** tell the organiser the 6-digit number shown on your screen so they can approve your machine.
5. **Start the worker:** run the `oxideswarm worker --isolation fs+net --max-concurrency 2 --task-priority below-normal --no-gpu --no-portal --name <YOUR_LABEL>` command from Section 7.

### How to keep your computer quiet or stop at any time
- Leave `--max-concurrency 2` (or lower it to `1` if you hear loud fans) and use `--task-priority idle` if you work on the machine while connected.
- To stop lending immediately, press `Ctrl+C` in the worker terminal. Your worker terminates active tasks on your machine and disconnects cleanly. (Scheduled hours, battery/metered pauses, and one-key remote pause are **Planned** under `NET-9` for step 3.)
- If your computer shuts down or disconnects mid-run, any running trial is automatically sent to another worker; your files stay clean.

### Quick troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `oxideswarm join` says the invite is expired or damaged | Invite string was wrapped/cut when copying or expired (`24h`) | Copy the whole single line from `oxs1-invite:` to the end, or ask `P1` for a fresh `oxideswarm invite` |
| Worker waits at approval | `P1` has not run `oxideswarm members approve` yet | Send your 6-digit code and pending ID to `P1` |
| Worker refuses to start with `--isolation fs+net` | Sandbox backend unsupported on this OS setup | Switch to `--isolation fs` as instructed by `P1`, or check `oxideswarm doctor` |
| Port `8080` address already in use | Another personal worker already serves the standby portal | Add `--no-portal` to your `oxideswarm worker` command |
