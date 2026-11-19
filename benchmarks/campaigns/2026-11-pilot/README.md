# Pilot campaign 2026-11: Anima Engine deterministic sweep (`APP-6`)

- **Status:** Partial (`APP-6` steps 1 and 4 committed; dry run step 2 is ready on `0.4.4` using [`dry_run_manifest.json`](dry_run_manifest.json) and [`standin_workload.py`](standin_workload.py) without waiting for `NET-9` or `NET-10`; full pilot step 3 waits for the Anima Engine runner update listed below, `NET-9`, `NET-10`, and owner participant list).
- **Decisions:** Q94 (workload: Anima Engine sweep in deterministic CPU mode) and Q95 (organiser plus 3 to 5 people they know, about 10 machines, at least two home networks, and at least two machines of each OS and CPU architecture in use), decided on `2026-11-19` in [design 0094](../../../docs/design/0094-road-to-a-shared-research-grid.md).
- **Rollout order:** First a dry run of `APP-6` step 2 with 2 to 3 participants (`P1` to `P3`) on released `0.4.4` (no `NET-9` or `NET-10` required, using [`dry_run_manifest.json`](dry_run_manifest.json)), then the full 10-machine Anima Engine pilot (`APP-6` step 3) after the Anima Engine runner update, `NET-9` (contributor controls), and `NET-10` (display-only credits) land ([`docs/BACKLOG.md`](../../../docs/BACKLOG.md)).

## 1. Workload

- **Target program:** the headless Anima Engine E-SEL1 runner (`run_esel1_experiment`, built from `src-tauri/examples/run_esel1_experiment.rs` in the `Anima-Engine` repository; `src-tauri/src/bin/anima-lab.rs` is the repository's general headless CLI).
- **Pinned baseline commit:** `7a825cc56135df83d5a513ecf59fe3b2e9f1dd92` (which built the recorded E-SEL1 smoke output `Anima-Engine/artifacts/experiments/e-sel1-smoke-record/run-2-stopped/`, committed at `Anima-Engine` `a3734c3`), matching [design 0089](../../../docs/design/0089-anima-engine-first-workload.md) D6. Once the updated runner lands, lock its new commit in `workload.commit`, `workload.runner_sha256`, `workload.runner_ready = true`, and this sheet in git before submitting step 3.
- **Blocker before the Anima Engine pilot (`runner_ready: false` in [`manifest.json`](manifest.json)):**
  At `Anima-Engine` commit `7a825cc56135df83d5a513ecf59fe3b2e9f1dd92`, `run_esel1_experiment` cannot execute the 200-unit grid directly (`manifest.json` marks `"runner_ready": false`, so `driver.py` refuses live runs on it until updated):
  1. **Seed and grid parameters ignored:** under `--smoke`, `run_esel1_experiment` runs only the fixed smoke seed at `T_smoke` (`18,000`) ticks and ignores `ANIMA_SIM_SEED` (`live_experiment.rs:1679`); it has no flags for the grid parameters (`live.founders`, `live.food_cap_abundant`, `live.food_cap_scarce`, `duration_ticks`).
  2. **External manifest directory required:** without `manifests/esel1-preregistration.json` staged inside the task sandbox, `run_esel1_experiment` exits `2`.
  3. **Wall-clock hashes in `checksums.sha256`:** `out/smoke/checksums.sha256` hashes `runs.csv` (which contains wall-clock `us_per_tick`), `smoke-report.json`, and `provenance.json` (which contains a timing block). On trial `n0-30_fa-50_fs-13` in `run-2-stopped/`, repeat deterministic runs produce identical control `final_checksum = 1859816024`, yet their `runs.csv` SHA-256 digests differ (`aaf8...` vs `21d8...`). Verifying `checksums.sha256` with `submit --verify 1` would fail every unit twice (`200 x mismatch_persisted`, exit `4`).
  - **What the Anima Engine change (owner's repository; never modify or build Anima Engine in OxideSwarm) must provide before step 3:**
    - Accept CLI flags `--manifest-dir manifests --out out --seed <SEED> --founders <N0> --food-cap-abundant <FA> --food-cap-scarce <FS> --duration-ticks <T>` (with `defaults.args` already wired to those placeholders in [`manifest.json`](manifest.json)), plus staged external input `--extra-input <LOCAL_MANIFESTS_DIR>:manifests` containing `esel1-preregistration.json` (never `.env`).
    - Write a deterministic verification file `out/smoke/verify_checksums.txt` containing sorted LF lines `seed,arm,final_checksum` (design 0089 D6's deterministic `final_checksum` equality, excluding timing blocks and `us_per_tick`).
- **Stand-in workload for step 2 (`APP-6` dry run on `0.4.4`):**
  Until the Anima Engine runner update lands, the step-2 dry run uses the committed deterministic Python stand-in [`standin_workload.py`](standin_workload.py) and [`dry_run_manifest.json`](dry_run_manifest.json). It takes `--seed`, `--founders`, `--food-cap-abundant`, `--food-cap-scarce`, `--duration-ticks`, and `--out`, computes three synthetic deterministic arms (`control`, `treatment`, `drift`), writes deterministic `out/smoke/verify_checksums.txt` (`seed,arm,final_checksum`), and writes non-deterministic wall-clock timings into `out/smoke/runs.csv` so `--verify-file out/smoke/verify_checksums.txt` exercises real selective verification under `--isolation fs` (or `fs+net`).
- **How the Anima Engine runner binary hash is checked (step 3):**
  1. Before step 3 starts, the organiser builds one release binary per target `(os, arch)` pair in the `Anima-Engine` repository (contributors never build `Anima-Engine` during the campaign).
  2. Compute the SHA-256 digest of each built binary and record it under `workload.runner_sha256` in [`manifest.json`](manifest.json) (replace `<fill-before-run>`).
     - On macOS/Linux (organiser machine):

```bash
shasum -a 256 <RUNNER_BINARY_PATH>
```

     - On Windows (organiser machine):

```powershell
Get-FileHash -Algorithm SHA256 <RUNNER_BINARY_PATH>
```

  3. When [`../driver.py`](../driver.py) runs with `--runner <RUNNER_BINARY_PATH> --runner-system <os/arch>`, it verifies the local file's SHA-256 digest against `manifest.json`, stages the binary as a content-addressed BLAKE3 input (`--input`, [design 0006](../../../docs/design/0006-content-addressed-data-plane.md)), and automatically pins every submitted task to `--os <os> --arch <arch>`.
- **Deterministic CPU settings:** every task sets `ANIMA_DETERMINISTIC=1`, `ANIMA_USE_GPU=0`, and `ANIMA_SIM_SEED=<SEED>` (design 0089, Context and D1). Workers must advertise CPU execution only (`--no-gpu`).
- **Cross-system behaviour (`VERIFY-2`):** on identical inputs at commit `7a825cc` (`n0-30_fa-50_fs-13`, seed `999971`, `18,000` ticks), repeat deterministic CPU `final_checksum` values on the same `(os, arch)` match identically, whereas `macos/aarch64` (`1859816024`) and `windows/x86_64` (`2546342991`) differ (design 0089 D6). Replicas therefore run on the same `(os, arch)` by default, never pass `--verify-any-system`, and each runner binary targets one `(os, arch)` pool via `--runner-system <os>/<arch>`.
- **Secrets rule:** never stage `.env` or any file containing `GEMINI_API_KEY`, Neo4j credentials, or cluster secrets as a task `--input` (design 0089, Security). [`../driver.py`](../driver.py) rejects forbidden names lexically and recursively walks every directory `--input` (including `--extra-input`) to refuse `.env`, `.env.*`, `*.secret`, or `invite.oxs` anywhere inside.

## 2. Input set and runtime estimate

[`manifest.json`](manifest.json) defines **200** independent work units (`u001` through `u200`) across three axes:

| Axis | Values | Count |
|---|---|---|
| `live.founders` | `30`, `45`, `60`, `80` | `4` |
| `(live.food_cap_abundant, live.food_cap_scarce)` | `(50, 25)`, `(50, 17)`, `(50, 13)`, `(50, 8)`, `(50, 4)` | `5` |
| `ANIMA_SIM_SEED` | `100001` through `100010` | `10` |
| **Total grid** | `4 x 5 x 10` at `duration_ticks = 450,000` per arm | **`200` units (`400` runs with `--verify 1`)** |

Step-2 dry run ([`dry_run_manifest.json`](dry_run_manifest.json)) defines a 12-unit subset (`2` founders x `3` food pairs x `2` seeds at `duration_ticks = 18,000`) for [`standin_workload.py`](standin_workload.py).

### Runtime estimate and basis (step 3 Anima Engine sweep)

- **Measured anchor (`18,000` ticks):** in `Anima-Engine/artifacts/experiments/e-sel1-smoke-record/run-2-stopped/smoke-decisions.json` (committed at `Anima-Engine` `a3734c3`; release binary built at `7a825cc` on macOS arm64, `N0 = 30`), one `18,000`-tick unit executes three arms (`control`, `treatment`, `drift`) at `291.1–316.9 us/tick` (`control`), `272.1–467.1 us/tick` (`treatment`), and `117.0–155.5 us/tick` (`drift`). Across the four recorded trials, one 3-arm invocation takes `12.7–16.9 s` compute time (median `~13.5 s`).
- **Scaled unit time (`450,000` ticks = `25 x 18,000`):** linear scaling gives `~338 s` (`5.6 min`) on a lightly loaded arm64 core and `~360–504 s` (`6.0–8.4 min`, midpoint **`7.2 min`**) across mixed desktop/laptop contributors running at `--task-priority below-normal` plus OxideSwarm staging and collection overhead.
- **Total cluster time across `10` machines split into per-OS/arch pools with `--verify 1` (`400` task executions = `200` primary + `200` replicas):**
  Because each native runner binary runs on one `(os, arch)` pool (`--runner-system macos/aarch64` vs `--runner-system windows/x86_64`), the organiser partitions the 200 units across the per-system `driver.py` instances proportionally to machine counts (for example `6` `macos/aarch64` machines running `120` units / `240` execs and `4` `windows/x86_64` machines running `80` units / `160` execs = `20.0` units per machine = `40.0` execs per machine in each pool; `--shard K/N` handles equal `N`-way splits):

| Contributor `--max-concurrency` per machine | Slots per pool (`6` macOS / `4` Windows; `10` total) | Compute time (`7.2 min` midpoint, `40.0` execs/machine) | End-of-sweep tail + staging | Estimated wall clock (target `1–3 h`) |
|---|---|---|---|---|
| `2` slots / machine | `12` + `8` = `20` slots | `144.0 min` (`2.40 h`) | `~12 min` | **`2.6 h`** (range `2.2–3.0 h`) |
| `3` slots / machine | `18` + `12` = `30` slots | `96.0 min` (`1.60 h`) | `~10 min` | **`1.8 h`** (range `1.5–2.0 h`) |
| `4` slots / machine | `24` + `16` = `40` slots | `72.0 min` (`1.20 h`) | `~10 min` | **`1.4 h`** (range `1.2–1.6 h`) |

- **Assumption check:** linear scaling assumes live agent populations stay comparable from `18,000` to `450,000` ticks. Measure actual unit durations on a small slice (`--limit`) after the updated Anima Engine binary lands and adjust `duration_ticks` if needed so all 200 units stay within `1–3 h`.

## 3. Verification rule

Every unit is submitted through:

```text
oxideswarm submit --json --wait --verify 1 --verify-file out/smoke/verify_checksums.txt --os <OS> --arch <ARCH> ...
```

- **Why `verify_checksums.txt`, not `checksums.sha256`:** `out/smoke/checksums.sha256` includes wall-clock `us_per_tick` (`runs.csv`) and timing blocks (`provenance.json`, `smoke-report.json`), which always differ between identical runs. Design 0089 D6's deterministic equality is the `final_checksum` of each `(seed, arm)`, so `--verify-file` compares `out/smoke/verify_checksums.txt` (`seed,arm,final_checksum` per line, sorted by arm with LF line endings).
- **Replica selection (`crates/cli/src/verify.rs`):** `--verify 1` runs the task once on a matching worker pinned to the target `--os`/`--arch`, then runs one replica pinned (`require:worker=<id>`) to a second connected worker reporting the same normalized `os` and `arch`. Both copies of `out/smoke/verify_checksums.txt` are compared by BLAKE3 digest.
- **What a mismatch triggers (`verify.status == "mismatch"`):**
  1. [`../driver.py`](../driver.py) records attempt 1 (sanitized worker labels, participant labels, `os`/`arch`, and differing digests). (A task exiting with exit code `4` without `verify.status == "mismatch"` is classified as `failed`, not `mismatch`.)
  2. [`../driver.py`](../driver.py) automatically reruns that unit once with the same `--verify 1 --verify-file out/smoke/verify_checksums.txt` flags and records attempt 2 in the same unit entry.
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
- **Who sees simulation outputs:** only the organiser collects `out/smoke/` trial artifacts onto their local machine via `submit --output-dir`. Participants may request summaries or checksums from the organiser; any public publication of Anima Engine outputs awaits Q52 and follows [design 0057](../../../docs/design/0057-open-results-charter.md).
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

### Step 2: Dry run on released `0.4.4` (stand-in workload)

Preview first (`--dry-run`, contacts nothing):

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

Run `--limit 6` (or all `12`) against two or more matching workers on the same `(os, arch)` (`--runner-system` pins `--os` and `--arch` on every unit; pass `--command /usr/bin/python3` on macOS under `--isolation fs` / `fs+net`, or `--command python` on Windows):

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

### Step 3: Per-system Anima Engine sweep (once the runner update lands)

Run one `driver.py` per `(os, arch)` worker pool with `--runner-system <OS>/<ARCH>`, `--shard <K>/<N>` (or distinct unit manifests), `--extra-input <LOCAL_MANIFESTS_DIR>:manifests`, and a pool-specific `--record` file so every unit receives the right OS binary and `--verify 1` replica pin:

```bash
python3 benchmarks/campaigns/driver.py \
  --manifest benchmarks/campaigns/2026-11-pilot/manifest.json \
  --runner <RUNNER_BINARY_PATH> \
  --runner-system macos/aarch64 \
  --shard 1/2 \
  --extra-input <LOCAL_MANIFESTS_DIR>:manifests \
  --participants <PARTICIPANTS_MAP_JSON> \
  --output-dir <TRIAL_OUTPUT_DIR> \
  --concurrency 12 \
  --record benchmarks/campaigns/2026-11-pilot/results/<YYYY-MM-DD>-pilot-macos-aarch64.json
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
You are lending spare CPU time on your computer to run a batch of short deterministic trials for a small group of people the organiser knows. For step 2 (the dry run), each trial runs a tiny built-in Python verification script ([`standin_workload.py`](standin_workload.py)); for step 3 (the full pilot), every trial runs the deterministic simulation program (`run_esel1_experiment`) with different seeds and food limits. No AI model weights, passwords, or personal files leave or enter your computer.

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
