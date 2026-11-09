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
./target/release/oxideswarm workers --master <MASTER_IP>:8088
```

Expected: the remote worker is `connected` and its `TAGS` column includes `feat:workspaces` and `tool:cargo` (`crates/cli/src/cli_app.rs`).

### 3. Offload cargo test from the command line

On the client machine (from the root of the repository checkout):

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --workspace oxideswarm-dev \
  --require-tool cargo \
  --wait \
  --command cargo -- test -p rusty_grid_core --lib
```

What happens on this run:

1. `scan_workspace_source` (`crates/cli/src/blobs.rs`) walks the current directory (`.` by default, or `--sync <DIR>`), applies `.gitignore`, `.ignore`, and `.git/info/exclude` inside the root, skips `.git/` and ignored directories such as `target/`, and hashes regular files with BLAKE3.
2. `upload_inputs` (`crates/cli/src/blobs.rs`) asks the master which file blobs are missing and uploads only the new blobs (up to 8 blobs and 16 requests in flight).
3. `Engine::place` (`crates/master/src/scheduler.rs`) routes the task to a connected worker with `WORKSPACES` and `tool:cargo`, preferring the worker that last ran `oxideswarm-dev` (`TaskQueue` in `crates/master/src/queue.rs`), and allows at most one active task per workspace name at a time.
4. `WorkspaceGuard::sync` (`crates/worker/src/workspace.rs`) updates changed files and removes deleted tracked files in `<workspace_root>/oxideswarm-dev/tree` while leaving untracked build outputs such as `target/` untouched.
5. `TaskRunner` (`crates/worker/src/runner.rs`) runs `cargo test -p rusty_grid_core --lib` in `<workspace_root>/oxideswarm-dev/tree` with `CARGO_TARGET_DIR=<workspace_root>/oxideswarm-dev/tree/target` and a default timeout of `1800` s (`--timeout-secs`).

To run a second command in the same remote workspace without scanning or syncing local files again, pass `--no-sync` (and optional `--cwd <REL>`):

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --workspace oxideswarm-dev \
  --no-sync \
  --require-tool cargo \
  --wait \
  --command cargo -- test -p rusty_grid_core --lib -- --nocapture
```

### 4. Offload cargo test over MCP

When a coding agent is connected to `oxideswarm mcp` ([mcp-client-setup.md](mcp-client-setup.md)), it calls the `submit` tool (`crates/cli/src/mcp.rs`) with `workspace` and `tools: ["cargo"]`. `scan_mcp_workspace_in_dir` (`crates/cli/src/mcp.rs`) checks that `sync_dir` stays inside the MCP working directory without traversing symbolic links.

On the client machine (from the repository root):

```bash
printf '%s\n%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"offload","version":"1.0"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"submit","arguments":{"command":"cargo","args":["test","-p","rusty_grid_core","--lib"],"tools":["cargo"],"workspace":{"name":"oxideswarm-dev","sync_dir":"."}}}}' \
  | ./target/release/oxideswarm mcp --master <MASTER_IP>:8088
```

The JSON payload inside response `id: 2` includes `"exit_code": 0` and `"workspace": { "name": "oxideswarm-dev", "worker": "<WORKER_UUID>", "files": <N>, "uploaded_bytes": <BYTES>, "skipped": 0 }`.

## Verify

Run the same workspace test twice in a row and inspect the workspace affinity map.

On the client machine:

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --workspace oxideswarm-dev \
  --require-tool cargo \
  --wait \
  --command cargo -- test -p rusty_grid_core --lib
./target/release/oxideswarm workspaces --master <MASTER_IP>:8088
```

Expected output on the second run:

- Standard error reports `sent 0 of <M> blobs (0 B), <M> already on the master` (`uploaded_bytes: 0` over MCP).
- Standard error from Cargo contains `Finished` and `Running unittests` without `Compiling rusty_grid_core`, proving that `<workspace_root>/oxideswarm-dev/tree/target` on the worker was reused.
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

Status: **Planned** (awaiting the live two-machine run on the owner's cluster per [design 0039](../design/0039-agent-workspaces.md) W10; automated loopback coverage runs in `tests/agent_workspace_e2e.rs` on Linux, macOS, and Windows in CI).

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
