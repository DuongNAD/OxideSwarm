# 0042: Remote workspaces and P2P client transport for coding agents (`FEAT-4`)

Status: Draft
Created: 2026-11-08    Updated: 2026-11-08

## Summary

Let a coding agent run `cargo test`, `cargo check`, or other repository commands on a remote worker
over CLI or MCP, reusing the worker's incremental build directory (`target/`) across runs and
reaching a P2P-only master without a direct TCP route:

- **Persistent named workspaces** on each worker under `<base_dir>/workspaces/<workspace_name>`,
  protected by a per-workspace lock and bounded by an LRU disk budget (D1).
- **Git bundle + working-tree diff synchronization** (`WorkspaceSpec`) carried over the existing
  content-addressed blob store (`Task::inputs`), so a worker checks out a commit it never had
  before and applies uncommitted local edits before running the command (D2).
- **`working_dir` and workspace-aware timeout defaults** in CLI `submit` and MCP `submit` (D3).
- **Unified `GridStream` client connections** (`connect_master` and `BlobConnection`) over TCP or
  an iroh P2P ticket (`STREAM_CLIENT = 0x03`) for every client command (D4).

Nothing in this record is built yet; code follows after design approval (`FEAT-4`, working rule 7).

## Context

What exists on `master` at `5df82df` (`docs/BACKLOG.md:1134-1145`):

| Area | Today | Code |
|---|---|---|
| Per-task sandbox | `Sandbox::create` creates `<base_dir>/<task_id>` with mode `0700` and `Sandbox::destroy` (and `Drop`) deletes it when the task ends. Cargo's `target/` directory is discarded after every task. | `crates/worker/src/sandbox.rs:625-651`, `crates/worker/src/sandbox.rs:799-829`, `crates/worker/src/runner.rs:1562-1662` |
| File staging and blobs | Inline `StagedFile` entries are capped at `MAX_STAGED_FILE_BYTES` (`32 MiB`) and `MAX_AGGREGATE_STAGED_BYTES` (`32 MiB`). MCP `files` scans regular files under `cwd` up to `DEFAULT_MAX_INPUT_FILES` (`1,000`) and `DEFAULT_MAX_INPUT_BYTES` (`256 MiB`), with no git history or incremental commit transport. | `crates/core/src/task.rs:1699-1702`, `crates/cli/src/blobs.rs:40-43`, `crates/cli/src/mcp.rs:1188-1278` |
| MCP `working_dir` and timeout | `tool_submit` in the MCP server hard-codes `working_dir: None` on `TaskSpec::Command` and defaults `timeout_secs` to `60` seconds, which is too short for a cold `cargo test` build. | `crates/cli/src/mcp.rs:1798-1804`, `crates/cli/src/mcp.rs:1820-1823` |
| Client transport | `connect_master` returns `MessageTransport<TcpStream>` via `TcpStream::connect`, and `BlobConnection` wraps `Mutex<MessageTransport<TcpStream>>`. The Master's P2P accept loop accepts only two-stream worker connections (`STREAM_CONTROL = 0x01`, `STREAM_DATA = 0x02`) and rejects single-stream clients. | `crates/cli/src/connect.rs:180-216`, `crates/cli/src/blobs.rs:464-478`, `crates/core/src/transport.rs:11-16`, `crates/master/src/server.rs:1445-1524` |

## Goals

- **G1** A coding agent on one machine can run `cargo test -p rusty_grid_core --lib` on another
  machine over MCP (`submit`) or CLI (`oxideswarm submit`) at a local commit that the remote worker
  did not previously have.
- **G2** A second task targeting the same named workspace on that worker reuses the existing git
  object database and Cargo `target/` directory so incremental compilation runs instead of a cold
  rebuild.
- **G3** Concurrent tasks targeting the same named workspace on one worker serialize cleanly (or wait
  bounded by task timeout) without corrupting the git index or `target/` lockfiles.
- **G4** Every client path (`submit`, `status`, `workers`, `cancel`, `doctor`, `mcp`, and blob
  upload/download) can connect to a Master either by TCP (`HOST:PORT`) or by an iroh P2P ticket
  (`--p2p-ticket <TICKET>` / `p2p://<TICKET>`).

## Non-goals

- Distributed shared filesystems (NFS/SMB/FUSE) or live bidirectional file watching during task
  execution.
- Preserving untracked build caches across different workers (each worker maintains its own local
  `<base_dir>/workspaces/<workspace_name>` cache).
- Running git credential helpers or SSH agents inside the worker workspace; all repository objects
  needed by the task are packaged by the client before submission.

## Design

### D1 Persistent named workspaces on the worker (`crates/worker/src/sandbox.rs`, `crates/worker/src/runner.rs`)

1. **Workspace name validation:**
   - A workspace name (`WorkspaceName`, 1 to 64 ASCII characters matching `[a-zA-Z0-9._-]`, never
     `.` or `..`, and never a Windows reserved device name in `WINDOWS_RESERVED_NAMES` at
     `crates/worker/src/sandbox.rs:11-14`) identifies a persistent directory under
     `<base_dir>/workspaces/<workspace_name>`.
2. **Directory layout and permissions:**
   - `<base_dir>/workspaces/` and each `<base_dir>/workspaces/<workspace_name>` are created with
     mode `0700` on Unix after `SandboxConfig::ensure_private_root` (`crates/worker/src/sandbox.rs:300-314`)
     verifies that `<base_dir>` is owned by the effective user and not a symbolic link.
   - Each workspace holds:
     - `tree/`: the checked-out working tree and preserved untracked build directories (such as
       `target/`),
     - `meta.json`: last-used timestamp, last checked-out commit OID, and disk usage estimate.
3. **Per-workspace concurrency lock:**
   - `TaskRunner` (`crates/worker/src/runner.rs:685-694`) holds a
     `WorkspaceManager` with an in-process `Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>`
     plus an advisory lockfile `<base_dir>/workspaces/<workspace_name>.lock` so two tasks naming the
     same workspace on the same worker run one after the other instead of racing on `.git/index.lock`
     or `target/.cargo-lock`.
   - Waiting for the workspace lock is bounded by the task's `timeout_secs` and `cancel_rx`.
4. **Retention and disk budget:**
   - `WorkerConfig` (`crates/worker/src/client.rs`) and `RunnerConfig` add
     `workspace_cache_bytes: u64` (default `8 GiB`, configurable via `--workspace-cache-mb`,
     `RUSTY_GRID_WORKSPACE_CACHE_MB`, `[worker].workspace_cache_mb`).
   - Before and after a workspace run, `WorkspaceManager` evicts the least-recently-used unlocked
     workspaces whose aggregate size exceeds `workspace_cache_bytes`. An active workspace is pinned
     while its task holds the workspace lock.

### D2 Git bundle and working-tree diff synchronization (`crates/core/src/task.rs`, `crates/cli/src/blobs.rs`)

1. **Wire representation (`WorkspaceSpec`):**
   - `Task` (`crates/core/src/task.rs`) gains `pub workspace: Option<WorkspaceSpec>` (mirrored on
     `BinaryTask` and `HumanTask`), gated by a new capability bit
     `ProtocolCapabilities::REMOTE_WORKSPACE` (`1 << 6`) in `crates/core/src/protocol.rs`.
   - `WorkspaceSpec` contains:
     - `name: String` — validated workspace identifier,
     - `head_commit: String` — 40-hex SHA-1 (or 64-hex SHA-256) commit OID to check out,
     - `bundle_blob: Option<BlobId>` — content-addressed blob ID of a `git bundle` containing the
       commit history needed to reach `head_commit`,
     - `patch_blob: Option<BlobId>` — optional binary git diff (`git diff --binary HEAD`) capturing
       staged and unstaged tracked changes plus untracked regular files inside the client's working
       tree,
     - `clean_untracked: bool` — when `true`, runs `git clean -fd -e target/` (preserving `target/`
       unless full clean is requested) before applying `patch_blob`.
2. **Client-side preparation (`crates/cli/src/workspace.rs`):**
   - When CLI `submit --workspace <NAME>` (or `--workspace auto`, which derives `<repo-name>-<short-hash-of-remote-url-or-root>`)
     or MCP `submit` with `"workspace": "<NAME>"` is invoked inside a git repository:
     1. Resolves `HEAD` commit OID via `git rev-parse --verify HEAD`.
     2. Produces a deterministic bundle (`git bundle create <tmp> HEAD` or bounded `--since` / base
        ref range when configured) and records its `BlobId` in `PreparedInputs` (`crates/cli/src/blobs.rs:120-130`)
        so `upload_missing_blobs` deduplicates identical bundles across consecutive runs at the same
        `HEAD`.
     3. When the working tree has uncommitted changes (`git status --porcelain`), generates a binary
        diff patch (`git diff --binary HEAD`) and packs untracked non-ignored regular files (refusing
        symbolic links, matching `scan_mcp_inputs_in_dir` in `crates/cli/src/mcp.rs:1188-1278`),
        uploading that patch as `patch_blob` via `PreparedInputs`.
3. **Worker-side materialization (`crates/worker/src/workspace.rs`):**
   - With the workspace lock held:
     1. Initializes `<base_dir>/workspaces/<name>/tree` with `git init` if `.git` does not exist.
     2. If `head_commit` is not yet present (`git cat-file -e <head_commit>^{commit}` fails),
        unbundles `bundle_blob` from the worker's `BlobCache` (`crates/worker/src/runner.rs:732-745`)
        using `git bundle unbundle <bundle_path>`.
     3. Resets the tracked tree to `head_commit` (`git checkout --force --detach <head_commit>`) and
        cleans untracked files outside preserved build caches (`git clean -fd -e target/`).
     4. If `patch_blob` is present, applies it with `git apply --binary --index --whitespace=nowarn`.
     5. Stages any explicit `Task::staged_files` or `Task::inputs` on top, executes the task with
        `OXIDESWARM_WORKSPACE=<name>` set in the task environment, harvests declared artifacts/outputs,
        and leaves `tree/` intact for the next task.

### D3 `working_dir` and workspace-aware timeout defaults (`crates/cli/src/mcp.rs`, `crates/cli/src/cli_app.rs`)

1. **`working_dir` in CLI and MCP:**
   - CLI `submit` (`crates/cli/src/cli_app.rs`) adds `--working-dir <REL_PATH>`, and MCP `submit`
     (`crates/cli/src/mcp.rs:1798-1804`) reads optional `"working_dir": "<REL_PATH>"` for both
     `TaskSpec::Command` and `TaskSpec::ShellScript` (adding `working_dir: Option<PathBuf>` to
     `TaskSpec::ShellScript` or resolving `effective_cwd` identically in
     `execute_shell_script_workload` at `crates/worker/src/runner.rs:1666-1733`).
   - `sanitize_relative_path` (`crates/worker/src/sandbox.rs:120-160`) validates `working_dir` on
     the worker so absolute paths, `..`, symbolic links escaping the root, and Windows reserved
     names are rejected before spawning the process.
2. **Default timeout when a workspace is attached:**
   - Plain tasks without a workspace keep the existing `60 s` default timeout
     (`crates/cli/src/mcp.rs:1820-1823`).
   - When `workspace` is set on CLI `submit` or MCP `submit` and the caller did not specify
     `timeout_secs` (or `[submit].timeout_secs`), the default timeout increases to
     `DEFAULT_WORKSPACE_TIMEOUT_SECS = 300` (`5 minutes`) so a cold remote `cargo test` compile does
     not time out at `60 s`.

### D4 P2P client transport in `connect_master` (`crates/core/src/transport.rs`, `crates/master/src/server.rs`, `crates/cli/src/connect.rs`)

1. **Single-stream P2P client discriminator (`STREAM_CLIENT`):**
   - `crates/core/src/transport.rs:13-16` adds `pub const STREAM_CLIENT: u8 = 0x03;` alongside
     `STREAM_CONTROL = 0x01` and `STREAM_DATA = 0x02`.
   - In `crates/master/src/server.rs:1445-1524`, when the Master's P2P accept loop reads `tag1[0]`
     on the first bidirectional QUIC stream:
     - `STREAM_CONTROL` / `STREAM_DATA` continue to accept the second stream and call
       `handle_multiplexed_connection` for workers.
     - `STREAM_CLIENT` wraps `BiStream::new(recv1, send1)` in `GridStream::P2p` and calls
       `handle_connection` (`crates/master/src/server.rs:3285-3348`), which authenticates the client
       with `authenticate_and_read_first_message` and dispatches `InboundMessage::Client` to
       `handle_client_connection`.
2. **Unified `connect_master` return type (`MessageTransport<GridStream>`):**
   - `connect_master` in `crates/cli/src/connect.rs:180-216` returns
     `GridResult<MessageTransport<GridStream>>` (and `BlobConnection` in
     `crates/cli/src/blobs.rs:464-478` wraps `Mutex<MessageTransport<GridStream>>`).
   - Address resolution in `connect_master`:
     - If `addr` starts with `p2p://` or is a JSON P2P ticket parseable by `parse_p2p_ticket`
       (`crates/core/src/transport.rs:177-179`), or `--p2p-ticket` / `OXIDESWARM_P2P_TICKET` /
       `[client].p2p_ticket` is provided, `connect_master` binds an ephemeral client `iroh::Endpoint`,
       connects to the Master's `EndpointAddr` with `GRID_ALPN`, opens a bidirectional stream, writes
       `[STREAM_CLIENT]`, and runs the existing `client_handshake` + AEAD setup on the resulting
       `GridStream::P2p`.
     - Otherwise `connect_master` connects via `TcpStream::connect`, applies `configure_tcp_stream`,
       and wraps the socket in `GridStream::Tcp`.
   - A `P2pClientGuard` (or attached endpoint handle inside `GridStream::P2p`) keeps the ephemeral
     `iroh::Endpoint` alive for the lifetime of the stream and closes it when the stream drops.

## Compatibility

- **Wire protocol:** Adding `workspace: Option<WorkspaceSpec>` to `Task` (and `working_dir` to
  `TaskSpec::ShellScript` if extended) changes the positional bincode layout of `Task` / `MasterMessage`
  and therefore bumps `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` in
  `crates/core/src/protocol.rs` and `crates/core/tests/data/wire_layout.txt` (enforced by `FEAT-8`).
- **Capability gating:** Workers that support persistent git workspaces advertise
  `ProtocolCapabilities::REMOTE_WORKSPACE`. The scheduler (`crates/master/src/scheduler.rs`) requires
  `REMOTE_WORKSPACE` (and `BLOB_TRANSFER`) for any task with `task.workspace.is_some()`, so older or
  non-git workers are never assigned workspace tasks.
- **P2P stream tag:** `STREAM_CLIENT = 0x03` is only sent by P2P clients; existing TCP clients and
  two-stream P2P workers (`STREAM_CONTROL = 0x01`, `STREAM_DATA = 0x02`) are unchanged.

## Security

- **Path and symlink isolation:** Workspace names are restricted to `[a-zA-Z0-9._-]{1,64}` and
  cannot traverse outside `<base_dir>/workspaces/`. After `git checkout` and `git apply`, any
  `working_dir`, staged file, or output artifact is checked with `sanitize_relative_path`
  (`crates/worker/src/sandbox.rs:120-160`) and `symlink_metadata` so a repository symlink cannot read
  or write outside the workspace root.
- **Client-side symlink refusal:** `patch_blob` and bundle generation in MCP `submit` refuse
  symlinks and paths outside `cwd`, preserving the boundary of `scan_mcp_inputs_in_dir`
  (`crates/cli/src/mcp.rs:1188-1278`).
- **Git hook suppression:** All worker-side `git` invocations pass `-c core.hooksPath=/dev/null` and
  `-c protocol.file.allow=never` so a malicious repository config or bundle cannot execute arbitrary
  hooks during `unbundle`, `checkout`, or `apply`.
- **Disk exhaustion:** Bundle and patch blobs are bounded by `InputLimits` (`256 MiB` default) and
  the worker's `BlobCache` quota, and `<base_dir>/workspaces/` is bounded by `workspace_cache_bytes`
  with LRU eviction.
- **Cluster authentication over P2P:** `STREAM_CLIENT` connections run the exact same
  `authenticate_and_read_first_message` (`crates/master/src/server.rs:3072-3138`) OXS-AUTH challenge
  and ChaCha20-Poly1305 session encryption as TCP connections before any `ClientMessage` is read.

## Test plan

- **Unit tests:**
  - `crates/worker/src/sandbox.rs`: workspace name validation (rejects `..`, `/`, `\`, colons, empty,
    `> 64` chars, and `WINDOWS_RESERVED_NAMES`), per-workspace lock serialization, and LRU eviction
    under `workspace_cache_bytes`.
  - `crates/cli/src/workspace.rs`: git bundle + uncommitted binary diff generation and `PreparedInputs`
    deduplication when `HEAD` is unchanged.
  - `crates/worker/src/workspace.rs`: unbundling a commit unknown to the worker, applying a dirty
    working-tree patch, preserving `target/` across a second run, and rejecting hooks/symlinks.
- **Integration tests:**
  - End-to-end MCP test (`tests/mcp_server_e2e.rs` or `tests/remote_workspace_e2e.rs`): create a
    temporary git repository with a new commit, invoke MCP `submit` to run `cargo test` (or a multi-step
    build test) on a worker whose workspace directory starts empty, verify the commit is checked out
    and incremental artifacts in `target/` are reused on the second MCP `submit` call.
  - End-to-end P2P client test: start a Master with `enable_p2p(true)`, connect CLI/MCP client via
    its P2P ticket (`STREAM_CLIENT = 0x03`) with a `ClusterSecret`, and run `ListWorkers`,
    `SubmitTask`, and `BlobRequest` over P2P without using the TCP listener.
- **Gates:**
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace --lib`

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| `FEAT-4a` | P2P client transport (`STREAM_CLIENT = 0x03`, `connect_master -> MessageTransport<GridStream>`, `BlobConnection` over P2P, `--p2p-ticket` on client and MCP commands) | `crates/core/src/transport.rs`, `crates/master/src/server.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/blobs.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs` | A client submits a task with blob inputs and receives the result from a Master over an iroh P2P ticket alone | Planned |
| `FEAT-4b` | Worker persistent named workspaces (`<base_dir>/workspaces/<name>`, per-workspace lock, LRU disk quota, git bundle + diff materialization) and `WorkspaceSpec` wire types | `crates/core/src/protocol.rs`, `crates/core/src/task.rs`, `crates/worker/src/sandbox.rs`, `crates/worker/src/runner.rs`, `crates/master/src/scheduler.rs` | Two consecutive tasks targeting workspace `w` at a new git commit check out the commit once and preserve `target/` for the second task | Planned |
| `FEAT-4c` | Client git bundle/diff packager, `--workspace` / `--working-dir` CLI flags, MCP `submit` `workspace` / `working_dir` properties, and `300 s` workspace default timeout | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/blobs.rs`, `README.md`, `CLAUDE.md` | An agent runs `cargo test -p rusty_grid_core --lib` over MCP in a named workspace at an unpushed commit and the second run reuses `target/` | Planned |

## Status log

- 2026-11-08: Initial draft opened for review before implementation (`FEAT-4`, working rule 7).
