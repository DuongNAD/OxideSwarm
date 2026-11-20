# Facilitator Sheet — `ADOPT-1` Round-One Study

Protocol and observation checklist for running round one of the first-use study ([design 0059 D4](../../design/0059-first-users.md#d4-a-study-of-first-use-adopt-1), decision Q37).
**Status:** **Planned** (no session has run yet; record zero findings here).

---

## 1. Pre-study screening (before inviting anyone)

Per [design 0059 D4](../../design/0059-first-users.md#d4-a-study-of-first-use-adopt-1), screen 10 to 20 developers or students before any installation:

1. Ask one question without mentioning OxideSwarm installation:
   > "In the last week, did you wait more than five minutes for your computer to finish something? What was it?"
2. Tally the answers offline as counts per category (`Rust/C++ build or test`, `ML/GPU run`, `video/render batch`, `none / instant`, etc.) with **no names attached**.
3. Invite **3 to 5 people** who answered "yes", named a job, and have two machines (or pair with a second study machine on your LAN/WAN). People you already know are acceptable in round one (round two requires at least two strangers).
4. Apply the screening check before proceeding:

| Screening outcome | Action |
|---|---|
| Most named jobs are slow builds or tests | Proceed with round one as written |
| Most named jobs need a GPU or a language model | Review target first audience (`A1` vs `A3`/`A4`) with owner notes before starting `AX-1` |
| Fewer than 3 people named any waiting job | Recruit outside the owner's circle (same rule as round two) |

---

## 2. Pre-session set-up (dedicated study cluster)

Per [design 0059 Security](../../design/0059-first-users.md#security), **never** let a participant join your personal cluster or trial cluster (`~/OxideSwarm-trial`). Start a dedicated throwaway master with its own directory and state.

1. Assign the next unused label `P1` through `P5` in private offline notes (never commit the name-to-label map).
2. Send [participant-guide.md](participant-guide.md) and adapt [consent.md](consent.md) beforehand. Wait for explicit consent before starting the timer or running any command.
3. On the facilitator machine (`P1` / master host), prepare an isolated profile directory and start the dedicated study master (`--auth members` only; add `--p2p` if the participant is on another network):

```bash
export OXIDESWARM_HOME="$(mktemp -d)"
oxideswarm master --listen 0.0.0.0:<STUDY_PORT> --auth members --p2p --state-dir "$OXIDESWARM_HOME/study-state"
```

4. On the facilitator machine in a second terminal (using the same `OXIDESWARM_HOME`), generate a single-use invite **right before** step 2 (`join`). Note why `ADOPT-1` grants `worker,client` here (whereas the separate Stage 1 pilot of `design 0094` / `APP-6` uses `--roles worker` only): in `ADOPT-1`, the participant themselves runs `oxideswarm submit` during steps 3 and 4.
   - Same LAN (spoken pairing code, [add-a-machine guide](../../guides/add-a-machine.md)):

```bash
export OXIDESWARM_HOME="<FACILITATOR_OXIDESWARM_HOME>"
oxideswarm invite --code --roles worker,client --master <STUDY_MASTER_IP>:<STUDY_PORT> --expires 10m
```

   - Another network (`oxs1-invite:...` via private chat, `--auth members --p2p`):

```bash
export OXIDESWARM_HOME="<FACILITATOR_OXIDESWARM_HOME>"
oxideswarm invite --roles worker,client --uses 1 --expires 2h --master <STUDY_MASTER_IP>:<STUDY_PORT> --p2p-ticket-file "$OXIDESWARM_HOME/master_ticket.txt"
```

5. Start the facilitator timer in interactive mode (or use subcommands `begin` / `end` / `event` from [`scripts/study_timer.py`](../../../scripts/study_timer.py)):

```bash
python3 scripts/study_timer.py run \
  --record <STUDY_OUTPUT_DIR>/<YYYY-MM-DD>-<PARTICIPANT_LABEL>.json \
  --participant <PARTICIPANT_LABEL> \
  --os <PRIMARY_OS_ARCH> \
  --second-os <SECOND_OS_ARCH> \
  --install-method readme \
  --release <RELEASE_VERSION> \
  --consent
```

---

## 3. Ordered study tasks and what to observe

Give the participant only [README.md](../../../README.md) (starting at `#install`, `#quick-start`, and `#add-your-other-machines`), [docs/guides/add-a-machine.md](../../guides/add-a-machine.md), the temporary `<FACILITATOR_MASTER>` invite when step 2 starts, and the sample command hints only if step 3 or 4 stalls after asking. Do **not** type commands for them unless logging `hands_on` help.

| Step ID (`study_timer.py`) | Participant task | What to observe and log |
|---|---|---|
| `install` | Start from the [Releases page](https://github.com/DuongNAD/OxideSwarm/releases) and [`README.md#install`](../../../README.md#install) (or USB kit if measuring USB). Verify `SHA256SUMS` (and `gh attestation verify` if `gh` is present), unpack, add to `PATH`, handle macOS `xattr -dr com.apple.quarantine` if downloaded by browser. | Where they hesitate (archive target name, `SHA256SUMS` vs `shasum`, missing `PATH`, macOS Gatekeeper / Windows SmartScreen). |
| `join` | Redeem the one-time invite (`oxideswarm join --code ...` or pipe `oxs1-invite:...` into `oxideswarm join`), verify the 6-digit pairing code, then start `oxideswarm worker --isolation fs` (using a separate `OXIDESWARM_HOME` if needed). Wait on facilitator terminal to approve with `oxideswarm members approve <PENDING_ID> --code <NNNNNN>`. | Whether `stdin` piping vs argument causes error, UDP `8087` fallback needed (`--pair-host`), `--isolation fs` host error, or approval delay. |
| `first_task` | Confirm worker shows up (`oxideswarm workers`) and run the first command task with `--wait`, e.g. `oxideswarm submit --wait -- hostname` (from `README.md#quick-start`). Remind yourself never to paste any printed hostname into notes. | Confusion between master/worker `--master` host or port, missing `--`, misreading `Waited` / `Sum of task times` (`ADOPT-2` summary). |
| `file_round_trip` | Create a tiny dummy input file `study_in.txt` (`echo study-input > study_in.txt`), submit a task sending `--input study_in.txt`, copying it to `study_out.txt`, and bringing back `--output study_out.txt --output-dir study_out`. | Difficulty with `--input` syntax, glob/path quoting, `--output-dir` expectation, or Windows `cmd /C` vs Unix `cp`. |
| `own_job` | Try a minimal version of the slow job named during screening (for example a test suite via `--workspace` or a small batch command). Mark skipped if no feasible job exists. | Whether existing flags fit the job (`category`, `ran: yes|no|skipped`) and what stopped them (missing tool probe, environment setup, network denied under `fs+net`). |
| `stop_worker` | Press `Ctrl+C` in the worker terminal to stop cleanly; verify on facilitator master with `oxideswarm workers` that the worker disconnects, then revoke the temporary member key. | Whether `Ctrl+C` exits promptly, whether participant understands the difference between worker exit and revoking membership. |

### Reference commands for the facilitator and target commands for `file_round_trip`

On the facilitator machine (`P1`) after the participant runs `join` (`--auth members`):

```bash
export OXIDESWARM_HOME="<FACILITATOR_OXIDESWARM_HOME>"
oxideswarm members pending
oxideswarm members approve <PENDING_ID> --code <NNNNNN>
oxideswarm workers
```

Target command for step `file_round_trip` if the participant gets stuck and asks (`help-level: hint` or `command`):

On a participant machine running macOS or Linux (`bash`):

```bash
echo study-input > study_in.txt
oxideswarm submit --master <STUDY_MASTER_IP>:<STUDY_PORT> --input study_in.txt --output study_out.txt --output-dir study_out --wait -- cp study_in.txt study_out.txt
```

On a participant machine running Windows (`powershell`):

```powershell
Set-Content -Path .\study_in.txt -Value "study-input"
oxideswarm.exe submit --master <STUDY_MASTER_IP>:<STUDY_PORT> --input study_in.txt --output study_out.txt --output-dir study_out --wait --command cmd -- /C copy study_in.txt study_out.txt
```

On the facilitator machine (`P1`) right after step `stop_worker`:

```bash
export OXIDESWARM_HOME="<FACILITATOR_OXIDESWARM_HOME>"
oxideswarm workers
oxideswarm members revoke <STUDY_MEMBER_ID>
```

---

## 4. When to help and how to note it

1. **Wait silently first.** Do **not** rescue a participant while they are reading the docs or thinking. Let them struggle through typos and doc lookup up to **3 minutes** unless they ask aloud or hit a dead end (for example a missing host firewall prompt or blocked UDP broadcast).
2. **Log pauses without helping.** Whenever the participant stops scrolling/typing for > 20 seconds or re-reads a section visibly confused, log a pause:
   - interactive timer: `p hesitated on macOS xattr line in README`
   - CLI subcommand: `python3 scripts/study_timer.py event --record <REC> --step install --kind pause --note "hesitated on macOS xattr line in README"`
3. **Log every intervention with its level:** whenever you step in (only after they ask or stay stuck > 3 minutes), record an `event` of kind `help` with one of three levels:
   - `hint` — pointed to a section or explained a concept in plain words without naming the exact command (`h hint reminded about stdin pipe rule`).
   - `command` — gave the exact command or flag they should type (`h command gave --pair-host fallback flag`).
   - `hands_on` — typed/fixed something for them on their keyboard or terminal (`h hands_on edited PATH export`).
4. **Effect on `M16`:** Any `help` event (`hint`, `command`, or `hands_on`) during `install`, `join`, or `first_task` sets `m16_unaided_first_result = false` automatically in [`scripts/study_timer.py`](../../../scripts/study_timer.py). Help during `file_round_trip` or `own_job` is logged for UX findings without failing `M16`.

---

## 5. Measures recorded in round one ([design 0059 D8](../../design/0059-first-users.md#d8-measures))

Measures `M1` to `M14` belong to benchmarks and assurance in designs 0056 and 0058. Round one of `ADOPT-1` records `M15` through `M17`:

| ID | Measure | How it is read | Round-one target |
|---|---|---|---|
| `M15` | Time to first result (`install` + `join` + `first_task` minutes) | Timed in [`scripts/study_timer.py`](../../../scripts/study_timer.py) per session | Baseline from round one (working target for a person: 10 minutes) |
| `M16` | Unaided completion | `True` only when `install`..`first_task` complete with zero `help` events | Baseline from round one |
| `M17` | Repeat use within 14 days | Facilitator asks each participant once after 14 days; recorded via `followup` subcommand | Recorded (`yes` / `no` and reason; no target before round two) |

---

## 6. Post-session wrap-up and 14-day follow-up

1. Check that the participant stopped their worker (`Ctrl+C`) and deleted or kept only what they want from the temporary directory; revoke their temporary study member key on your master.
2. Delete the throwaway study master state directory (`rm -rf "$OXIDESWARM_HOME"`) once round-one sessions finish so no study keys remain on disk.
3. Run `python3 scripts/study_timer.py summarize ...` to inspect notes immediately while fresh, verifying zero PII or secret strings exist.
4. Set a calendar reminder 14 days after each session to ask the participant one question ("Did you run a task of your own again in the last 14 days? Why or why not?") and record it with `python3 scripts/study_timer.py followup`.
5. Once at least three round-one sessions have `M15`–`M17` complete, follow [`README.md`](README.md#how-results-become-the-round-one-record) to publish the table in [`docs/design/0059-first-users.md`](../../design/0059-first-users.md#study-records) and apply [design 0059 D4](../../design/0059-first-users.md#d4-a-study-of-first-use-adopt-1)'s priority consequence table:

| Finding in round one | Priority consequence (design 0059 D4) |
|---|---|
| Most of the time goes to `install` and `join` | `ADOPT-3` (install scripts) and `JOIN-2` improvements move ahead of `AX-1` |
| Fewer than half choose a job of `A1` or `A2` at `own_job` | Review first audience with owner before starting `AX-2` |
| Two or more people choose a job outside D1 | Add it to D1 table and make it an `ADOPT-4` guide candidate |
| Nobody runs a task again within 14 days | Record reasons given and review D3 order before starting `ADOPT-3` or `ADOPT-4` |
