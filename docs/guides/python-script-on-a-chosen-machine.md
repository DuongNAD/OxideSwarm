# Run a Python script on a chosen machine

## Goal

Stage a Python script and a CSV input file onto a specific worker machine (`--worker`) under `--isolation fs`, configure the run with `--env` and `--timeout-secs`, and collect generated CSV and JSON outputs into `--output-dir`.

## Prerequisites

- **The chosen worker machine must already have Python and any libraries your script imports installed.** OxideSwarm stages your script and data files with `--input`, not a Python interpreter or virtual environment, and tasks never download programs at runtime (`HOST_PROBED_TOOLS` in `crates/core/src/capabilities.rs` does not probe `python` by default in `0.4.5`).
- An OxideSwarm `0.4.5` or later master and one or more workers authenticated with `--secret-file` (see [two-machine-cluster.md](two-machine-cluster.md) and [reference.md](../reference.md#complete-flag-reference); never use `--insecure`).
- You can practice on a single Windows machine by setting `<MASTER_IP>` to `127.0.0.1` and running the master (`--listen 127.0.0.1:8088`), workers (each with `--no-portal`, its own `--name`, and its own `--sandbox-base-dir`), and client commands from separate terminals.
- Note for Windows `--isolation fs` workers: if Python is installed inside your user profile (for example `%LOCALAPPDATA%\Programs\Python\Python313`), pass `--isolation-read-dir` so confined AppContainer tasks can read and run it ([design 0074](../design/0074-external-runtime-allowlist-and-gpu-workloads.md)).

## Steps

### 1. Start the chosen Python worker (and optionally a plain worker without Python)

On Windows with `--isolation fs` ([design 0033](../design/0033-os-level-task-isolation.md)), tasks can read `C:\Windows\System32` and `C:\Program Files`, whereas per-user folders under `%LOCALAPPDATA%` require `--isolation-read-dir` pointing to the exact Python directory (`--tag python` is optional metadata while `--worker` selects the worker by name; see [reference.md](../reference.md#oxideswarm-worker-flags)). On `%TEMP%` directories with foreign ACEs (`ensure_private_dir`), pass `--sandbox-base-dir` under `$env:LOCALAPPDATA\OxideSwarm\`.

On the Windows worker machine that has Python installed (`friend-pc`, PowerShell):

```powershell
oxideswarm.exe worker `
  --master <MASTER_IP>:8088 `
  --name friend-pc `
  --tag python `
  --isolation fs `
  --isolation-read-dir "$env:LOCALAPPDATA\Programs\Python\Python313" `
  --max-concurrency 2 `
  --sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes-friend" `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret"
```

If you also keep a machine without Python in `PATH` (`plain-pc`) connected to the same cluster, start it without `--isolation-read-dir` and pass `--no-portal` if both workers share one host.

On a second Windows worker machine without Python in `PATH` (`plain-pc`, PowerShell):

```powershell
oxideswarm.exe worker `
  --master <MASTER_IP>:8088 `
  --name plain-pc `
  --isolation fs `
  --max-concurrency 2 `
  --no-portal `
  --sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes-plain" `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret"
```

Verify from the client that both workers appear with `"state:connected"` and `"iso:fs"`.

On the client machine (PowerShell):

```powershell
oxideswarm.exe workers `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --json
```

### 2. Prepare a Python script and CSV input file on the client

Create `analyze.py` and `data\readings.csv` using only Python standard library modules (`csv`, `json`, `os`, `pathlib`, `platform`, `statistics`) so the script runs cleanly wherever Python 3 is installed. If your real script imports third-party packages such as `numpy` or `pandas`, verify those packages are installed inside the Python environment on `friend-pc` first.

On the client machine (create `data\readings.csv`):

```csv
sample,group,value
s1,A,12.5
s2,A,24.0
s3,A,31.5
s4,B,18.0
s5,B,27.5
s6,B,35.0
```

On the client machine (create `analyze.py`):

```python
import csv
import json
import os
import pathlib
import platform
import statistics

threshold = float(os.environ.get("THRESHOLD", "0.0"))
label = os.environ.get("RUN_LABEL", "default")

with open(os.path.join("data", "readings.csv"), newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

kept = [r for r in rows if float(r["value"]) >= threshold]
out_dir = pathlib.Path("out")
out_dir.mkdir(parents=True, exist_ok=True)

with open(out_dir / "filtered.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["sample", "group", "value"])
    writer.writeheader()
    writer.writerows(kept)

groups = {}
for g in sorted({r["group"] for r in kept}):
    vals = [float(r["value"]) for r in kept if r["group"] == g]
    groups[g] = {
        "count": len(vals),
        "mean": round(statistics.mean(vals), 3),
        "max": max(vals),
    }

summary = {
    "label": label,
    "threshold": threshold,
    "total_rows": len(rows),
    "matched_rows": len(kept),
    "groups": groups,
    "python_version": platform.python_version(),
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
print(f"label={label} threshold={threshold} matched={len(kept)}/{len(rows)} python={platform.python_version()}")
```

### 3. Run the script on `--worker friend-pc` and collect outputs

Use `--worker friend-pc` (`--os windows` and `--require-isolation fs` optional here as an extra guard), pass configuration via repeatable `--env KEY=VALUE`, stage both inputs via `--input <LOCAL>:<SANDBOX>`, and fetch every file under `out/` with `--output "out/*"` and `--output-dir .\py-out` (see [reference.md](../reference.md#oxideswarm-submit-flags)). Pass an explicit `--timeout-secs` above the `60` s default if your real dataset runs longer.

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --worker friend-pc `
  --os windows `
  --require-isolation fs `
  --timeout-secs 120 `
  --env THRESHOLD=20.0 `
  --env RUN_LABEL=lab-run-1 `
  --input .\analyze.py:analyze.py `
  --input .\data\readings.csv:data/readings.csv `
  --output "out/*" `
  --output-dir .\py-out `
  -- python.exe analyze.py
```

Expected output:

```text
Inputs: 2 files (1.3 KiB); sent 2 of 2 blobs (1.3 KiB), 0 already on the master
Task <TASK_ID> completed successfully.
Worker: friend-pc (<WORKER_UUID>)
Exit Code: 0
Execution Time: <MS>ms
Tasks: 1 finished, 0 failed, on 1 machines
Waited: <W> s    Sum of task times: <S> s
Output files (2 saved to .\py-out):
  - out/filtered.csv (64 bytes, blake3: <BLAKE3_SHORT>) -> .\py-out\out\filtered.csv
  - out/summary.json (305 bytes, blake3: <BLAKE3_SHORT>) -> .\py-out\out\summary.json
Stdout:
label=lab-run-1 threshold=20.0 matched=4/6 python=3.13.3
```

### 4. Re-run with new `--env` settings and no re-upload

Because `analyze.py` and `data\readings.csv` are unchanged, repeating the submission with higher `--env THRESHOLD=30.0` skips uploading file blobs (`sent 0 of 2 blobs (0 B), 2 already on the master`) and writes the new outputs into `.\py-out-strict`.

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --worker friend-pc `
  --os windows `
  --require-isolation fs `
  --timeout-secs 120 `
  --env THRESHOLD=30.0 `
  --env RUN_LABEL=strict-run `
  --input .\analyze.py:analyze.py `
  --input .\data\readings.csv:data/readings.csv `
  --output "out/*" `
  --output-dir .\py-out-strict `
  -- python.exe analyze.py
```

### 5. What happens when a worker lacks Python or a task hits `--timeout-secs`

If you target a worker that does not have `python.exe` in its `PATH` (`--worker plain-pc`), `--isolation fs` fails immediately before launching a process with `program 'python.exe' was not found in PATH`:

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --worker plain-pc `
  --os windows `
  --require-isolation fs `
  --timeout-secs 120 `
  --wait `
  --input .\analyze.py:analyze.py `
  --input .\data\readings.csv:data/readings.csv `
  -- python.exe analyze.py
```

Expected error output (`Exit Code: 1`):

```text
Stderr:
Cannot run 'python.exe' with --isolation fs: program 'python.exe' was not found in PATH
Error: Isolation error for 'python.exe' (fs): program 'python.exe' was not found in PATH
```

If your script runs longer than `--timeout-secs`, the waited client exits `1` when the deadline expires (and the worker terminates the task process tree):

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --worker friend-pc `
  --os windows `
  --require-isolation fs `
  --timeout-secs 3 `
  --wait `
  -- python.exe -c "import time; time.sleep(10)"
```

Expected error output (exit code `1`):

```text
Timed out after 3s waiting for task <TASK_ID> to complete
```

If you later stop all workers and turn off `--isolation fs` on a Windows host where `--isolation-read-dir` was used, run `windows\revoke-isolation-acls.ps1` to remove the `oxideswarm.toolchain` ACL entry recorded in `windows_acl_grants.txt`.

## Verify

On the client machine (PowerShell), inspect both collected files from Step 3 (`.\py-out\out\filtered.csv` and `.\py-out\out\summary.json`) or re-run Step 3 and verify the outputs exist locally:

```powershell
Get-Content .\py-out\out\filtered.csv
Get-Content .\py-out\out\summary.json
```

Expected `.\py-out\out\summary.json`:

```json
{
  "label": "lab-run-1",
  "threshold": 20.0,
  "total_rows": 6,
  "matched_rows": 4,
  "groups": {
    "A": {
      "count": 2,
      "mean": 27.75,
      "max": 31.5
    },
    "B": {
      "count": 2,
      "mean": 31.25,
      "max": 35.0
    }
  },
  "python_version": "3.13.3"
}
```

### Recorded run

Run on 2026-11-27 between 05:28:39 and 05:30:15 UTC on a Windows 11 laptop using an isolated loopback cluster (ephemeral TCP and dashboard ports, `--discovery-port 0`, `--state-dir`, `--secret-file`, no `--insecure`) built from `origin/master` commit `07caa5aef340` (`rusty-grid 0.4.5`):

| Field | Value |
|---|---|
| Host hardware and OS | Laptop with Intel Core i7-1355U, 16 GB RAM, Windows 11 Home x86_64 |
| Release / binary | `rusty-grid 0.4.5` (`07caa5aef340141f4a2de5f967480a29b174a3ed`) |
| Python runtime on `friend-pc` | CPython `3.13.3` (`%LOCALAPPDATA%\Programs\Python\Python313\python.exe`), allowlisted via `--isolation-read-dir` |
| Workers | `friend-pc` (`--tag python --isolation fs`) and `plain-pc` (`--isolation fs`, no Python in `PATH`) |

| Scenario | Command focus | Client exit | Wall clock | Worker time | Result |
|---|---|---|---|---|---|
| Run on `--worker friend-pc` (`THRESHOLD=20.0`) | `--worker friend-pc`, `--env`, `--input`, `--output "out/*"`, `--output-dir .\py-out` | `0` | `16.381 s` | `16134 ms` | Uploaded 2 of 2 blobs; `matched=4/6 python=3.13.3`; saved `filtered.csv` (64 B) and `summary.json` (305 B) |
| Re-run with `--env THRESHOLD=30.0` | Same inputs, new `--env`, `--output-dir .\py-out-strict` | `0` | `15.814 s` | `15556 ms` | `sent 0 of 2 blobs (0 B), 2 already on the master`; `matched=2/6`; saved `summary.json` (304 B) |
| Chosen worker lacks Python (`--worker plain-pc`) | `python.exe analyze.py` on `plain-pc` (`--wait`) | `1` | `0.181 s` | `20 ms` | Failed: `Isolation error for 'python.exe' (fs): program 'python.exe' was not found in PATH` |
| Short timeout (`--timeout-secs 3`) | `time.sleep(10)` on `friend-pc` (`--timeout-secs 3 --wait`) | `1` | `3.098 s` | — | Exited `1`: `Timed out after 3s waiting for task <TASK_ID> to complete` |

After stopping the workers, `powershell -NoProfile -ExecutionPolicy Bypass -File windows\revoke-isolation-acls.ps1 -RemoveManifest` exited `0` and `icacls` confirmed zero leftover `oxideswarm.toolchain` capability SIDs on the Python directory.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Isolation error for 'python.exe' (fs): program 'python.exe' was not found in PATH` | Chosen worker has no Python installed in `PATH`, or Python lives in a user folder not on `PATH` | Install Python on the worker host (or use an already-installed interpreter path), ensure it is in `PATH`, and pass `--isolation-read-dir` if it lives under `%LOCALAPPDATA%`. |
| Windows worker rejects `--command C:\Users\<USER>\...\python.exe` under `--isolation fs` | Python directory inside user profile is not allowlisted for AppContainer reads | Restart the worker with `--isolation-read-dir "$env:LOCALAPPDATA\Programs\Python\Python313"` (exact version folder, never `%LOCALAPPDATA%` itself). |
| Script fails with `ModuleNotFoundError: No module named ...` | Chosen worker has Python but lacks a third-party library, or `--isolation-read-dir` did not cover its site-packages / venv | Pre-install required packages on the worker machine (OxideSwarm stages your script and data, not pip environments) or allowlist the virtual environment directory with `--isolation-read-dir`. |
| Task exits `1` with `Timed out after <N>s waiting for task ...` | Script took longer than `--timeout-secs` (default `60` s) | Raise `--timeout-secs` on `submit` to cover the expected runtime plus margin. |
| Worker refuses `%TEMP%\rusty_grid\sandboxes` at start-up | `%TEMP%` DACL grants access to another account or leftover SID (`ensure_private_dir`) | Pass `--sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes"`. See [gpu-workloads.md](gpu-workloads.md#troubleshooting). |
| `submit --worker <NAME>` stays `Queued` or warns no matching worker is connected | Worker `--name` typo, worker offline, or conflicting `--os` / `--require-isolation` flag | Run `oxideswarm.exe workers --json` and match the exact `name` and advertised tags. |
