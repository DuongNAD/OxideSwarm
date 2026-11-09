# Offload cargo test to another machine

## Goal

A client or MCP coding agent syncs a local Rust checkout into a persistent named workspace on a remote worker (`--workspaces`, [design 0039](../design/0039-agent-workspaces.md)) and runs `cargo test -p rusty_grid_core --lib`, reusing the remote `target/` directory on the second run.

## Prerequisites

- A two-machine OxideSwarm cluster verified with `oxideswarm doctor` ([two-machine-cluster.md](two-machine-cluster.md)).
- On the worker machine: the Rust toolchain (`cargo` and `rustc` in `PATH`, probed at worker start-up as `tool:cargo` and `tool:rustc` in `crates/core/src/capabilities.rs`) and free disk space under the workspace store (`--workspace-root`, default `~/Library/Caches/oxideswarm/workspaces` on macOS, `$XDG_CACHE_HOME/oxideswarm/workspaces` or `~/.cache/oxideswarm/workspaces` on Linux, and `%LOCALAPPDATA%\oxideswarm\workspaces` on Windows; `20480` MiB per workspace by default).
- On the client machine: the repository checkout to test (even at a commit the worker machine does not have) and the cluster secret (`~/.oxideswarm/cluster.secret` or `%USERPROFILE%\.oxideswarm\cluster.secret`).
- For MCP offload: `oxideswarm mcp` configured in Claude Code, Codex, or Cursor ([mcp-client-setup.md](mcp-client-setup.md)).

## Steps

### 1. Start the worker with persistent workspaces enabled

Workers keep persistent workspaces disabled by default (`crates/worker/src/client.rs`). Pass `--workspaces` (or set `RUSTY_GRID_WORKSPACES=1` or `workspaces = true` under `[worker]` in `rusty-grid.toml`; full flag and variable tables are in [README.md](../../README.md#oxideswarm-worker-flags)) so the worker opens `WorkspaceStore` (`crates/worker/src/workspace.rs`) and advertises `ProtocolCapabilities::WORKSPACES`.

On a macOS or Linux worker machine:

```bash
./target/release/oxideswarm worker --master <MASTER_IP>:8088 --workspaces --isolation fs
```

On a Windows worker machine (PowerShell):

```powershell
.\target\release\oxideswarm.exe worker --master <MASTER_IP>:8088 --workspaces
```

On macOS and Linux, `--isolation fs` ([design 0033](../design/0033-os-level-task-isolation.md)) confines each workspace task to `<workspace_root>/<WORKSPACE>/tree` and `<workspace_root>/<WORKSPACE>/tmp` while mounting the host's `RUSTUP_HOME` and `CARGO_HOME` read-only (`$CARGO_HOME/credentials.toml` is denied). On Windows, `--isolation fs` is **Planned** (`FEAT-3c`), so Windows workers run with `--isolation off`.

### 2. Check that the worker advertises workspaces and cargo

On the client machine:

```bash
./target/release/oxideswarm workers --master <MASTER_IP>:8088 --json
```

Expected: the remote worker's `tags` include `state:connected`, `feat:workspaces` and `tool:cargo` (the plain `workers` listing shows the worker and its link, but not its tags).

### 3. Offload cargo test from the command line

On the client machine (from the root of the repository checkout):

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --workspace oxideswarm-dev \
  --require-tool cargo \
  --wait \
  -- cargo test -p rusty_grid_core --lib
```

What happens on this first (cold) run:

1. `scan_workspace_source` (`crates/cli/src/blobs.rs`) walks the current directory (`.` by default, or `--sync <DIR>`), applies `.gitignore`, `.ignore`, and `.git/info/exclude` inside the root, skips `.git/` and ignored directories such as `target/`, and hashes regular files with BLAKE3.
2. `upload_inputs` (`crates/cli/src/blobs.rs`) asks the master which file blobs are missing and uploads only the new blobs (up to 8 blobs and 16 requests in flight).
3. `Engine::place` (`crates/master/src/scheduler.rs`) routes the task to a connected worker with `WORKSPACES` and `tool:cargo`, preferring the worker that last ran `oxideswarm-dev` (`TaskQueue` in `crates/master/src/queue.rs`), and allows at most one active task per workspace name at a time.
4. `WorkspaceGuard::sync` (`crates/worker/src/workspace.rs`) writes new or changed files and removes any previously tracked files that were deleted locally in `<workspace_root>/oxideswarm-dev/tree`, while leaving untracked build outputs such as `target/` untouched.
5. `TaskRunner` (`crates/worker/src/runner.rs`) runs `cargo test -p rusty_grid_core --lib` in `<workspace_root>/oxideswarm-dev/tree` with `CARGO_TARGET_DIR=<workspace_root>/oxideswarm-dev/tree/target` and a default timeout of `1800` s (`--timeout-secs`).

Expected output on the first (cold) run:

- Standard error prints the blob upload summary and Cargo's compilation output:

```text
Workspace 'oxideswarm-dev': <N> files (<SIZE>); sent <M> of <M> blobs (<SIZE>), 0 already on the master
Stderr:
   Compiling ...
   Compiling rusty_grid_core v0.1.0 (...)
    Finished `test` profile [unoptimized + debuginfo] target(s) in <S>s
     Running unittests src/lib.rs (target/debug/deps/rusty_grid_core-...)
```

- Standard output prints the task summary and Cargo's test summary:

```text
Task <TASK_ID> completed successfully.
Worker: <WORKER_NAME> (<WORKER_UUID>)
Exit Code: 0
Execution Time: <COLD_MS>ms
Stdout:
running <K> tests
...
test result: ok. <K> passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in <S>s
```

When you edit or delete a tracked source file locally and run the same command again, only changed blobs are uploaded (`sent <CHANGED> of <M> blobs`), deleted tracked files are removed from `<workspace_root>/oxideswarm-dev/tree`, and `<workspace_root>/oxideswarm-dev/tree/target` is preserved for incremental compilation.

To run another command in the same remote workspace without scanning or syncing local files at all, pass `--no-sync` (and optional `--cwd <REL>` or `--env KEY=VALUE`):

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --workspace oxideswarm-dev \
  --no-sync \
  --require-tool cargo \
  --wait \
  -- cargo test -p rusty_grid_core --lib -- --nocapture
```

### 4. Offload cargo test over MCP

When a coding agent is connected to `oxideswarm mcp` ([mcp-client-setup.md](mcp-client-setup.md)), it calls the `submit` tool (`crates/cli/src/mcp.rs`) with `type: "command"`, `command: "cargo"`, `tools: ["cargo"]`, `workspace: { "name": "oxideswarm-dev", "sync_dir": "." }`, and `wait: true`. `scan_mcp_workspace_in_dir` (`crates/cli/src/mcp.rs`) checks that `sync_dir` stays inside the MCP working directory without traversing symbolic links.

On the client machine (from the repository root):

```bash
printf '%s\n%s\n%s\n%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"offload","version":"1.0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"submit","arguments":{"type":"command","command":"cargo","args":["test","-p","rusty_grid_core","--lib"],"tools":["cargo"],"workspace":{"name":"oxideswarm-dev","sync_dir":"."},"wait":true,"wait_secs":120}}}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspaces","arguments":{}}}' \
  | ./target/release/oxideswarm mcp --master <MASTER_IP>:8088
```

The JSON text inside `result.content[0].text` of response `id: 2` includes:

```json
{
  "status": "completed",
  "task_id": "<TASK_ID>",
  "worker_id": "<WORKER_UUID>",
  "exit_code": 0,
  "execution_time_ms": 0,
  "stdout": "running <K> tests\n...",
  "stderr": "    Finished `test` profile ...\n     Running unittests src/lib.rs ...\n",
  "workspace": {
    "name": "oxideswarm-dev",
    "worker": "<WORKER_UUID>",
    "files": 0,
    "uploaded_bytes": 0,
    "skipped": 0
  }
}
```

On the warm second run with unchanged files, `"uploaded_bytes"` is `0` and neither `"stderr"` nor `"stdout"` contains `Compiling`. Response `id: 3` (`workspaces`) returns the master's affinity table as JSON (`[{"name": "oxideswarm-dev", "worker": "<WORKER_UUID>", "last_used_utc": "<TIMESTAMP>"}]`).

## Verify

Run the same workspace test a second time and inspect the workspace affinity map.

On the client machine:

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --workspace oxideswarm-dev \
  --require-tool cargo \
  --wait \
  -- cargo test -p rusty_grid_core --lib
./target/release/oxideswarm workspaces --master <MASTER_IP>:8088
```

Expected output on the second (warm) run:

- Standard error reports zero uploaded bytes and Cargo skips `Compiling`:

```text
Workspace 'oxideswarm-dev': <N> files (<SIZE>); sent 0 of <M> blobs (0 B), <M> already on the master
Stderr:
    Finished `test` profile [unoptimized + debuginfo] target(s) in <S>s
     Running unittests src/lib.rs (target/debug/deps/rusty_grid_core-...)
```

- `oxideswarm workspaces` lists the worker affinity entry:

```text
Workspaces (1):
- oxideswarm-dev -> worker <WORKER_UUID> (last_used_utc: <TIMESTAMP>)
```

To measure cold sync versus warm no-change and one-file-touch runs automatically, run the grid benchmark harness (`benchmarks/grid/bench_grid.py`):

On the client machine:

```bash
python3 benchmarks/grid/bench_grid.py \
  --only c \
  --offload-master <MASTER_IP>:8088 \
  --offload-dir . \
  --offload-cargo-args "test -p rusty_grid_core --lib"
```

### Recorded two-machine run

Status: **Planned** (placeholder for the live two-machine run on the owner's cluster per [design 0039](../design/0039-agent-workspaces.md) W10; automated end-to-end loopback coverage of CLI and MCP workspace builds, warm `target/` reuse, deleted-file sync, and `workspaces` affinity runs in `tests/agent_workspace_e2e.rs` on Linux, macOS, and Windows in CI).

| Field | Live run record (to be filled by owner) |
|---|---|
| Date (UTC) | `<TBD>` |
| Client commit | `<TBD>` (at a commit the worker machine does not have checked out) |
| Client / master host | `<TBD>` |
| Worker host (`--workspaces`) | `<TBD>` |
| Command | `cargo test -p rusty_grid_core --lib` over MCP (`workspace: { "name": "oxideswarm-dev", "sync_dir": "." }`, `tools: ["cargo"]`) |
| Cold first run (`target/` populated) | `<TBD>` (`files: <TBD>`, `uploaded_bytes: <TBD>`, `execution_time_ms: <TBD>`, `Compiling rusty_grid_core` present) |
| Warm second run (`target/` reused) | `<TBD>` (`uploaded_bytes: 0`, `execution_time_ms: <TBD>`, no `Compiling` line in `stderr`) |
| `workspaces` affinity output | `oxideswarm-dev -> worker <TBD> (last_used_utc: <TBD>)` |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Warning: no connected worker supports workspaces yet; the task will wait` | No connected worker was started with `--workspaces`, or the worker is running an older binary | Restart the worker with `--workspaces` (`RUSTY_GRID_WORKSPACES=1`) and check `oxideswarm workers` for `feat:workspaces`. |
| `Warning: no connected worker matches the placement constraints (require:tool=cargo)` | No connected worker found `cargo` in `PATH` during its start-up probe | Install Rust on the worker host and ensure `~/.cargo/bin` (`%USERPROFILE%\.cargo\bin` on Windows) is in `PATH`, then restart the worker. |
| `workspace source ... has more than 100000 files` or `more than 2147483648 bytes` | A large build or data directory is not listed in `.gitignore` or `.ignore` | Add generated directories to `.gitignore` or `.ignore`, or raise `--workspace-max-files <N>` and `--workspace-max-mb <MB>` on `submit` or `oxideswarm mcp`. |
| `invalid workspace name '...'` | The workspace name uses uppercase letters, path separators, leading punctuation, or a reserved Windows device name (`con`, `nul`, `com1`, ...) | Use 1 to 64 lowercase ASCII characters from `[a-z0-9][a-z0-9._-]{0,63}` without a trailing `.` or space (`validate_workspace_name` in `crates/core/src/task.rs`). |
| A second `submit --workspace <NAME>` waits in `Queued` while the first is still running | The scheduler allows at most one active (`Scheduled` or `Running`) task per workspace name at a time so two Cargo processes do not contend on `target/` | Wait for the running task in `<NAME>` to finish, or use a second workspace name (`<NAME>-2`) if you want an independent build tree. |
| Cargo fails with `attempt to download ...` when `--offline` or `--isolation fs+net` is used | Crate dependencies are not cached in the worker's `$CARGO_HOME` or vendored in the workspace tree | Run once without `--offline` (under `--isolation fs` or `off`) so Cargo populates `$CARGO_HOME/registry`, or vendor dependencies in the repository before running under `--isolation fs+net`. |
| Windows worker refuses to start when given `--workspaces --isolation fs` | Windows AppContainer confinement (`FEAT-3c`) is **Planned** | Start the Windows worker without `--isolation fs` (`--isolation off`, the default). |
