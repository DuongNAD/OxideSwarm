# Research campaigns (`APP-6`)

This directory holds the campaign sheets, manifests, run records, and submission driver for Stage 1 of [design 0094](../../docs/design/0094-road-to-a-shared-research-grid.md).

## Contents

| Path | What it is |
|---|---|
| [`driver.py`](driver.py) | Standard-library Python campaign driver over `oxideswarm submit --json --verify 1` (`APP-6` step 4) |
| [`2026-11-pilot/README.md`](2026-11-pilot/README.md) | Campaign sheet and one-page participant guide for the Stage 1 Anima Engine pilot (`APP-6` step 1) |
| [`2026-11-pilot/manifest.json`](2026-11-pilot/manifest.json) | Generated 200-unit sweep manifest (`4 x 5 x 10` grid at `450,000` ticks per arm; blocked on Anima Engine runner update) |
| [`2026-11-pilot/dry_run_manifest.json`](2026-11-pilot/dry_run_manifest.json) | 12-unit runnable dry-run manifest for `APP-6` step 2 on `0.4.4` |
| [`2026-11-pilot/standin_workload.py`](2026-11-pilot/standin_workload.py) | Deterministic Python stand-in workload writing `out/smoke/verify_checksums.txt` and `out/smoke/runs.csv` |

## Rules

1. Commit a campaign sheet and manifest before running the campaign ([design 0094](../../docs/design/0094-road-to-a-shared-research-grid.md) D1).
2. Keep records under `<campaign>/results/` free of personal names, host names, user names, worker names, worker UUIDs, IP addresses, secrets, and invite strings.
3. Verify the driver with `python3 tests/test_campaign_driver.py` (`unittest`, standard library only).
