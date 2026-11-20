# First-use study round one (`ADOPT-1`, 2026-11)

- **Status:** **Planned** (study kit ready; no round-one session has run yet, and nothing in this folder is a result).
- **Basis:** [Design 0059](../../design/0059-first-users.md) (sections D4 and D8) and [design 0094](../../design/0094-road-to-a-shared-research-grid.md) (Q95 participant pool relation).
- **Backlog item:** [`ADOPT-1`](../../BACKLOG.md#adopt-1-a-study-of-first-use-in-two-rounds) (`Planned`, gate for [`LAUNCH-1`](../../BACKLOG.md), `ADOPT-3`, `ADOPT-4`, and `AX-1`).

## Files in this kit

| File | Audience | Purpose |
|---|---|---|
| [participant-guide.md](participant-guide.md) | Participant | Plain one-page sheet in English and Vietnamese explaining the steps, isolation, data recorded, and how to stop |
| [facilitator-sheet.md](facilitator-sheet.md) | Facilitator (owner) | Pre-study screening, dedicated study cluster set-up, ordered observation steps, help grading, and D4/D8 tables |
| [consent.md](consent.md) | Participant and facilitator | Adaptable English and Vietnamese consent text with placeholder markers before first use |
| [`scripts/study_timer.py`](../../../scripts/study_timer.py) | Facilitator (owner) | Standard-library timer and JSON session recorder (enforces `P1`–`P5` labels, `--consent`, and PII checks) and table summarizer |
| [`tests/test_study_timer.py`](../../../tests/test_study_timer.py) | Contributors / CI | Unit tests for label validation, consent checks, PII filtering, `M15`–`M17` derivation, and summary output |

## How results become the round-one record

1. **Collect raw session files outside git first.** During the round-one sessions and the 14-day follow-up window, keep raw JSON session files produced by [`scripts/study_timer.py`](../../../scripts/study_timer.py) in a private directory on the facilitator machine (never inside the repository checkout until checked). Keep any offline participant mapping (who took `P1` through `P5`) out of the repository and out of the cluster hub permanently.
2. **Enforce strict anonymisation before committing or publishing.** Only files using labels `P1` to `P5` may leave the facilitator's local notes. Verify each JSON record and free-text note contains **none** of the following:
   - real names, initials, email addresses, or chat handles;
   - machine hostnames, OS usernames, or paths containing a username (`/Users/<USER>`, `/home/<USER>`, `C:\Users\<USER>`);
   - IP addresses, DNS names, `--master` hosts, or `--relay-url` endpoints;
   - secrets, join tokens, `oxs1-invite:...` strings, `cluster.secret`, `node.key`, `invite.oxs`, or 6-digit pairing codes;
   - contents of participant files or screenshots.
3. **Update each record at day 14 (`M17`).** After asking each participant whether they ran a task of their own again within 14 days, record the answer on the facilitator machine:

```bash
python3 scripts/study_timer.py followup --record <STUDY_DIR>/p1.json --repeat-use yes --reason "<ANONYMIZED_REASON>" --date <YYYY-MM-DD>
```

4. **Generate the round-one summary table and verify privacy checks.** On the facilitator machine, run the summarizer across all round-one JSON records (it re-validates `P1`–`P5` labels and blocks PII/secrets before printing):

```bash
python3 scripts/study_timer.py summarize <STUDY_DIR>/p*.json --format markdown
```

5. **Publish the official record in design 0059.** Per [`ADOPT-1`](../../BACKLOG.md#adopt-1-a-study-of-first-use-in-two-rounds) ("Done when"), the official round-one record lives under [`## Study records`](../../design/0059-first-users.md#study-records) in [design 0059](../../design/0059-first-users.md), alongside the anonymized pre-study screening counts per job category. Optionally commit the checked `P1`–`P5` session JSON files under `docs/study/2026-11-adopt-1/records/<YYYY-MM-DD>-<PARTICIPANT>.json`.
6. **Re-order or update the backlog from the findings.** Using the `"How the result changes the order"` table in [design 0059 D4](../../design/0059-first-users.md#d4-a-study-of-first-use-adopt-1), update the `## Status log` in `docs/BACKLOG.md` and move `ADOPT-1` from `Planned` to `Partial` once at least three round-one sessions with `M15` to `M17` are recorded there (`ADOPT-1` moves to `Implemented` after round two).
7. **Run repository checks before opening the record pull request.** On the facilitator machine:

```bash
python3 tests/test_study_timer.py
python3 tests/test_m1_challenger_hygiene.py
```

8. **Separate pilot participation (optional).** Per Q95 of [design 0094](../../design/0094-road-to-a-shared-research-grid.md), a participant who completes `ADOPT-1` may later be invited to the Stage 1 pilot ([`benchmarks/campaigns/2026-11-pilot/README.md`](../../../benchmarks/campaigns/2026-11-pilot/README.md)). Joining that pilot requires reading the separate pilot sheet and accepting a separate invite (`--roles worker`), after any temporary study credentials have been revoked.
