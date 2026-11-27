# Run batch file jobs on Windows workers

## Goal

Stage folders and individual input files onto Windows workers under `--isolation fs`, run a built-in Windows tool (`certutil -hashfile`) over those files, download output artifacts into `--output-dir`, and control retries and mid-task worker stops.

## Prerequisites

- An OxideSwarm `0.4.5` or later master and at least one Windows 10 or Windows 11 worker (two workers to observe mid-task failover), authenticated with `--secret-file` (see [two-machine-cluster.md](two-machine-cluster.md) and [reference.md](../reference.md#complete-flag-reference); never use `--insecure`).
- You can test everything on a single Windows machine by setting `<MASTER_IP>` to `127.0.0.1` and running the master (`--listen 127.0.0.1:8088`), workers (each with `--no-portal`, its own `--name`, and its own `--sandbox-base-dir`), and client commands from separate terminals.
- `oxideswarm.exe` in your `PATH` (or invoked by path from the release or build folder).
- Built-in Windows system tools (`cmd.exe` and `certutil.exe`, present in `C:\Windows\System32` on every Windows installation; no extra software installed and no program downloaded by tasks).
- On Windows workstations where `%TEMP%` carries inherited permissions for another account or leftover SID (`ensure_private_dir` in `crates/worker/src/sandbox.rs`), choose a worker sandbox root under `$env:LOCALAPPDATA\OxideSwarm\` with `--sandbox-base-dir`.

## Steps

### 1. Start Windows workers with user tags and `fs` isolation

Start each Windows worker with `--isolation fs` ([design 0033](../design/0033-os-level-task-isolation.md)), a user tag such as `--tag batch` so batch submissions target only those workers, and `--secret-file` (see [reference.md](../reference.md#oxideswarm-worker-flags)).

On the Windows worker machine (PowerShell):

```powershell
oxideswarm.exe worker `
  --master <MASTER_IP>:8088 `
  --name win-batch-a `
  --tag batch `
  --isolation fs `
  --max-concurrency 2 `
  --sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes-a" `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret"
```

When running a second worker on another Windows machine (or a second worker on the same machine for local failover testing), give it a distinct `--name` and `--sandbox-base-dir`, and add `--no-portal` if two workers share one host so they do not compete for port `8080`.

On a second Windows worker machine (PowerShell):

```powershell
oxideswarm.exe worker `
  --master <MASTER_IP>:8088 `
  --name win-batch-b `
  --tag batch `
  --isolation fs `
  --max-concurrency 2 `
  --no-portal `
  --sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes-b" `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret"
```

### 2. Verify that the workers advertise Windows, `fs` isolation, and your tag

On the client machine (PowerShell):

```powershell
oxideswarm.exe workers `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --json
```

Expected: each worker entry contains `"os:windows"`, `"iso:fs"`, `"batch"`, and `"state:connected"` in `tags`.

### 3. Stage an input folder, run `certutil -hashfile`, and collect outputs

Create a small sample folder on the client with nested files so you can check both recursive `--input` staging and output collection.

On the client machine (PowerShell):

```powershell
New-Item -ItemType Directory -Force .\batch-in\sub | Out-Null
[System.IO.File]::WriteAllText((Resolve-Path .\batch-in).Path + "\alpha.txt", "OxideSwarm batch sample alpha`n", [System.Text.Encoding]::ASCII)
[System.IO.File]::WriteAllText((Resolve-Path .\batch-in).Path + "\beta.txt", "OxideSwarm batch sample beta`n", [System.Text.Encoding]::ASCII)
[System.IO.File]::WriteAllText((Resolve-Path .\batch-in\sub).Path + "\gamma.txt", "OxideSwarm batch sample gamma`n", [System.Text.Encoding]::ASCII)
```

Stage `.\batch-in` at sandbox path `incoming` (`--input .\batch-in:incoming`), pin placement to Windows `--isolation fs` workers carrying `batch`, hash every file under `incoming\` with `certutil -hashfile`, and pull `out/checksums.txt` back into `.\batch-out` (`--output` implies `--wait`; `--output-dir` requires `--output`; see [reference.md](../reference.md#oxideswarm-submit-flags)). Note the escaped `\"%F\"` inside single quotes so PowerShell 5.1 preserves double quotes when forwarding the command string to `oxideswarm.exe`.

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --os windows `
  --require-tag batch `
  --require-isolation fs `
  --max-retries 2 `
  --input .\batch-in:incoming `
  --output "out/checksums.txt" `
  --output-dir .\batch-out `
  -- cmd.exe /c 'mkdir out & (for /r incoming %F in (*) do @certutil -hashfile \"%F\" SHA256) > out\checksums.txt'
```

What happens on this first run:

1. `prepare_inputs` and `upload_inputs` (`crates/cli/src/blobs.rs`) walk `.\batch-in`, upload the missing content-addressed file blobs and tree manifest (`sent 4 of 4 blobs`), and stage them under `<SANDBOX>\incoming\` on the assigned worker.
2. The worker spawns `cmd.exe` inside a per-attempt Windows AppContainer profile (`oxideswarm.task.<worker8>.<run_token>`, `crates/worker/src/isolation/windows/mod.rs`), writes `out\checksums.txt` inside the task sandbox, and uploads files matching `--output` back to the master blob store.
3. The client downloads the matched files to `.\batch-out\out\checksums.txt`, preserving the relative sandbox path.

Expected output:

```text
Inputs: 3 files (89 B); sent 4 of 4 blobs (292 B), 0 already on the master
Task <TASK_ID> completed successfully.
Worker: win-batch-b (<WORKER_UUID>)
Exit Code: 0
Execution Time: <MS>ms
Tasks: 1 finished, 0 failed, on 1 machines
Waited: <W> s    Sum of task times: <S> s
Output files (1 saved to .\batch-out):
  - out/checksums.txt (789 bytes, blake3: <BLAKE3_SHORT>) -> .\batch-out\out\checksums.txt
```

If you submit the exact same folder again, `upload_inputs` skips uploading unchanged file blobs (`sent 0 of 4 blobs (0 B), 4 already on the master`).

### 4. Fan out one task per file with wildcard `--output`

When each file should run as an independent queued item across available `batch` workers, loop over the folder on the client and collect each task's result with a wildcard `--output "out/*.sha256"`.

On the client machine (PowerShell):

```powershell
Get-ChildItem -Path .\batch-in -File -Recurse | Sort-Object FullName | ForEach-Object {
  oxideswarm.exe submit `
    --master <MASTER_IP>:8088 `
    --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
    --os windows `
    --require-tag batch `
    --require-isolation fs `
    --max-retries 2 `
    --input "$($_.FullName):in/$($_.Name)" `
    --output "out/*.sha256" `
    --output-dir .\batch-out-per-file `
    -- cmd.exe /c "mkdir out & certutil -hashfile in\$($_.Name) SHA256 > out\$($_.BaseName).sha256"
}
```

Expected: each task finishes with `Exit Code: 0`, and `.\batch-out-per-file\out\` holds `alpha.sha256`, `beta.sha256`, and `gamma.sha256`.

### 5. Control retries (`--max-retries`) and mid-task worker stops

`RetryPolicy` in `crates/master/src/queue.rs` separates task command failures from worker infrastructure losses:

- **Command failure (`retry_on_execution_failure = false` by default):** a non-zero exit code from your program fails immediately without consuming `--max-retries`. For example, hashing a missing file fails on attempt 1 with exit code `-2147024894` (`0x80070002 ERROR_FILE_NOT_FOUND`):

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --os windows `
  --require-tag batch `
  --require-isolation fs `
  --max-retries 2 `
  --wait `
  --input .\batch-in:incoming `
  -- certutil.exe -hashfile incoming\does-not-exist.txt SHA256
```

- **Graceful worker stop (`Ctrl+C`, not part of the recorded run):** sends `WorkerMessage::Disconnecting` so the master returns any running task to `Queued` immediately without waiting out the session-resume hold and without charging `retry_count`, unless `--max-retries 0` is in effect (`TaskQueue::handle_session_evacuated` in `crates/master/src/queue.rs`, [design 0036](../design/0036-bounded-uncharged-worker-evacuations.md)).
- **Sudden worker stop (`Stop-Process -Force`, crash, or dropped connection) with `--max-retries 2`:** by default `oxideswarm master` holds running tasks for `--session-resume-hold-secs 30` ([design 0046](../design/0046-master-durability.md)) so a quickly reconnecting worker can resume its session. Once that `30` s hold expires (or immediately when the master runs with `--session-resume-hold-secs 0`), the master re-queues the task onto another matching worker (`failover_count`), up to the `--max-retries` limit.
- **Sudden worker stop with `--max-retries 0` ("run at most once"):** once the hold expires, the master transitions the task to `Failed` (`maximum retry limit (0) reached after worker eviction`) and never re-dispatches it to a second worker.

To observe failover across two connected workers (`win-batch-a` and `win-batch-b`), run a slow task that sleeps 8 seconds before writing its output, and terminate the worker executing it while it is running.

On the client machine (PowerShell):

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --os windows `
  --require-tag batch `
  --require-isolation fs `
  --max-retries 2 `
  --timeout-secs 120 `
  --input .\batch-in:incoming `
  --output "out/failover.sha256" `
  --output-dir .\batch-out-failover `
  -- cmd.exe /c "powershell.exe -NoProfile -Command Start-Sleep -Seconds 8 & mkdir out & certutil -hashfile incoming\alpha.txt SHA256 > out\failover.sha256"
```

While the command is sleeping (wait ~1–2 s after submission), stop the worker currently running the task (`Stop-Process -Id <WORKER_PID> -Force`, or kill its terminal). After the master's default `30` s hold expires, the surviving worker executes the task and `.\batch-out-failover\out\failover.sha256` is saved. Repeating the same command with `--max-retries 0` fails after the hold expires with client exit `1` and writes no output file.

## Verify

On the client machine (PowerShell), repeat Step 3 into a fresh output directory and compare the collected SHA-256 digests with local `Get-FileHash`:

```powershell
oxideswarm.exe submit `
  --master <MASTER_IP>:8088 `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret" `
  --os windows `
  --require-tag batch `
  --require-isolation fs `
  --max-retries 2 `
  --input .\batch-in:incoming `
  --output "out/checksums.txt" `
  --output-dir .\batch-verify `
  -- cmd.exe /c 'mkdir out & (for /r incoming %F in (*) do @certutil -hashfile \"%F\" SHA256) > out\checksums.txt'

Get-FileHash -Algorithm SHA256 .\batch-in\alpha.txt, .\batch-in\beta.txt, .\batch-in\sub\gamma.txt
Get-Content .\batch-verify\out\checksums.txt
```

Expected: `submit` exits `0`, prints `Tasks: 1 finished, 0 failed, on 1 machines`, and every SHA-256 hex digest in `.\batch-verify\out\checksums.txt` matches `Get-FileHash`.

### Recorded run

Run on 2026-11-27 between 05:10:46 and 05:12:04 UTC (plus AppContainer tool compatibility check at 05:05:38 UTC) on a single Windows 11 laptop using an isolated loopback cluster (ephemeral TCP and dashboard ports, `--discovery-port 0`, `--state-dir`, `--secret-file`, no `--insecure`) built from `origin/master` commit `07caa5aef340` (`rusty-grid 0.4.5`):

| Field | Value |
|---|---|
| Host hardware and OS | Laptop with Intel Core i7-1355U, 16 GB RAM, Windows 11 Home x86_64 |
| Release / binary | `rusty-grid 0.4.5` (`07caa5aef340141f4a2de5f967480a29b174a3ed`) |
| Workers | Two local Windows workers (`win-batch-a`, `win-batch-b`) with `--tag batch --isolation fs --max-concurrency 2 --no-portal` and isolated `--sandbox-base-dir` roots |
| Inputs | `batch-in\alpha.txt` (30 B), `batch-in\beta.txt` (29 B), `batch-in\sub\gamma.txt` (30 B) |

| Scenario | Command focus | Client exit | Wall clock | Worker time | Result |
|---|---|---|---|---|---|
| Folder batch (cold upload) | `--input .\batch-in:incoming`, `certutil -hashfile`, `--output "out/checksums.txt"` | `0` | `0.599 s` | `323 ms` | Sent 4 of 4 blobs; all 3 SHA-256 digests matched local `Get-FileHash` |
| Folder batch (warm dedup) | Identical `submit` command into `.\batch-out-dedup` | `0` | `0.681 s` | — | `sent 0 of 4 blobs (0 B), 4 already on the master` |
| Per-file batch loop | 3 `submit` calls with `--output "out/*.sha256"` | `0` (`3/3`) | `0.320 s`, `0.457 s`, `0.536 s` | `160 ms`, `207 ms`, `256 ms` | Collected `alpha.sha256`, `beta.sha256`, `gamma.sha256` matching digests |
| Execution failure | Missing file (`does-not-exist.txt`) with `--max-retries 2 --wait` | `2` | `0.435 s` | `259 ms` | Immediate fail (`Exit Code: -2147024894`, `ERROR_FILE_NOT_FOUND`); no retry |
| Mid-task worker stop (`--max-retries 2`) | `Stop-Process -Force` on active `win-batch-b` | `0` | `40.318 s` | `8635 ms` | Re-queued after 30 s hold; finished on `win-batch-a`; `failover.sha256` verified |
| Mid-task worker stop (`--max-retries 0`) | `Stop-Process -Force` on active `win-batch-b` | `1` | `31.530 s` | `0 ms` | `Failed` after hold (`maximum retry limit (0) reached after worker eviction`); no output written |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `--output-dir requires at least one --output pattern` | `--output-dir` was passed without `--output` | Add one or more `--output <GLOB>` patterns (with forward slashes, e.g. `"out/*.sha256"`). |
| Default `%TEMP%\rusty_grid\sandboxes` is refused at worker start-up | `%TEMP%` (or an ancestor) grants access to another local account or leftover SID (`ensure_private_dir`) | Pass `--sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes"` on `oxideswarm worker`. See also [gpu-workloads.md](gpu-workloads.md#troubleshooting). |
| Two workers on one Windows host refuse sandboxes or collide | Both workers share `--sandbox-base-dir` or default worker ID / `:8080` portal | Give each local worker its own `--name`, `--sandbox-base-dir`, and `--no-portal`. |
| PowerShell `New-Item -Force`, `Compress-Archive`, or `Set-Content` fails with `PermissionDenied: (C:\out:String)` / `DriveNotFound`, or `tar.exe` fails with `GetDiskFreeSpace failed: 5` | Windows AppContainer (`--isolation fs`) restricts volume-root disk queries and ancestor browsing used by PowerShell's `FileSystem` provider and `bsdtar` | Invoke native Win32 tools through `cmd.exe` inside `--isolation fs` tasks (for example `mkdir` and `certutil.exe`), and run PowerShell packaging or post-processing on the client over `--output-dir`. |
| First `--isolation fs` worker start takes tens of seconds on a Rust developer PC | `prepare_worker_toolchains` (`crates/worker/src/isolation/windows/toolchain.rs`) walks `RUSTUP_HOME\toolchains` and `CARGO_HOME\registry` at start-up to check `oxideswarm.toolchain` ACLs | Allow start-up to finish once; dedicated Windows batch workers without `%USERPROFILE%\.rustup` or `%USERPROFILE%\.cargo` register immediately. |
| Task stays `Queued` or prints `no connected worker matches the placement constraints` | No connected worker matches `--os windows`, `--require-tag`, and `--require-isolation fs` | Inspect `oxideswarm.exe workers --json` for `os:windows`, `iso:fs`, and your `--tag` string, and restart missing workers. |
| Non-zero exit code does not retry even with `--max-retries 2` | `RetryPolicy::retry_on_execution_failure` in `crates/master/src/queue.rs` defaults to `false` | Fix the failing input path or command; `--max-retries` bounds retries after worker disconnection or when execution retries are enabled on the master. |
| Failover waits ~30 s after killing a worker process | `oxideswarm master` defaults to `--session-resume-hold-secs 30` so reconnecting workers can resume running tasks | Pass `--session-resume-hold-secs 0` when starting the master for immediate crash failover, or stop workers gracefully with `Ctrl+C` (`WorkerMessage::Disconnecting`). |
| `--output` collects zero files even though the task succeeded | Backslashes or absolute paths in `--output`, or output written outside the task sandbox directory | Use forward slashes relative to the task sandbox root (for example `"out/*.sha256"`). |
