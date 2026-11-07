# 0039: Persistent workspaces for coding agents

Status: Draft
Created: 2026-11-07    Updated: 2026-11-07

## Summary

- A task can name a **workspace**: a directory on a worker that outlives the task. A second build
  in the same workspace reuses its `target/`.
- The client **syncs** a local directory into the workspace. Only changed files cross the network,
  files deleted locally are deleted in the workspace, and build output the worker made is kept.
- The client honours `.gitignore` and never sends `.git/`. The master sends the next task of a
  workspace to the worker that holds it, and runs the tasks of one workspace one at a time.
- An agent uses it through one new field of the MCP `submit` tool, and a person through
  `submit --workspace NAME`. Workers offer workspaces only when started with `--workspaces`.
- This is Phase C of the roadmap (`docs/ROADMAP.md`, pull request 103) and the design FEAT-4
  asks for. FEAT-10 and P2P
  connections for clients are work items here as well.

## Context

What exists at `master` `5c518b5`:

| Area | Today | Code |
|---|---|---|
| Sandbox | `Sandbox::create` makes `<base>/<task id>` with mode 0700, and `destroy` removes it at the end of every workload. The default base is under the temporary directory (design 0021) | `crates/worker/src/sandbox.rs`, `crates/worker/src/runner.rs` |
| State kept across tasks | Only the blob cache `<base>/cache/blobs` (2 GiB, least recently used) and the worker id file | `crates/worker/src/blob_cache.rs`, `crates/worker/src/identity.rs` |
| Working directory | `TaskSpec::Command { working_dir }` is a directory relative to the sandbox, checked by `sanitize_relative_path`. `ShellScript` runs at the sandbox root. The MCP server always sets `working_dir: None` | `crates/core/src/task.rs`, `crates/worker/src/runner.rs`, `crates/cli/src/mcp.rs` |
| Input selection | `scan_directory` skips symbolic links, special files and empty directories. Nothing reads `.gitignore`. The MCP server confines `files` to its own current directory, refuses symbolic links in a tree, and caps a scan at `DEFAULT_MAX_INPUT_FILES` (1,000) and `DEFAULT_MAX_INPUT_BYTES` (256 MiB) | `crates/cli/src/blobs.rs`, `crates/cli/src/mcp.rs` |
| Directory model | `TreeManifest` lists regular files as `BlobFile { path, id, size, executable }`. It has no symbolic links and no empty directories | `crates/core/src/blob.rs` |
| Timeout | `TaskRequirements::timeout_secs` defaults to 60. The MCP `submit` tool also defaults to 60, and waits `timeout_secs` plus 5 s | `crates/core/src/task.rs`, `crates/cli/src/mcp.rs` |
| Compilation | `RustCompilation` runs `rustc` on one entry file with an empty environment. A `Cargo.toml` among its sources is written and not used | `crates/worker/src/runner.rs` |
| Connections | `connect_master` connects over TCP only, and the MCP server holds a `MessageTransport<TcpStream>`. The master refuses a client on a P2P connection ("Client connection not supported over multiplexed P2P worker transport") | `crates/cli/src/connect.rs`, `crates/cli/src/mcp.rs`, `crates/master/src/server.rs` |
| Features and tags | `ProtocolCapabilities` ends at `BLOB_TRANSFER` (`1 << 9`). `required_features` in the queue limits where a task may go. Six tag prefixes are reserved, and `require:tag=` refuses them | `crates/core/src/protocol.rs`, `crates/master/src/queue.rs`, `crates/core/src/placement.rs` |

The effect: an agent can send a project and run `cargo test` on another worker, but every run starts
from an empty sandbox and compiles every dependency again. A project with more than 1,000 files
cannot be sent through MCP at all.

## Goals

- **G1** A task runs in a named workspace that outlives it, so a second build reuses `target/`.
- **G2** A sync sends only files the worker lacks, deletes files removed locally, and keeps files the
  worker's builds created.
- **G3** The client's selection follows `.gitignore` and never includes `.git/`.
- **G4** An agent needs one new MCP field. A person needs one new CLI flag.
- **G5** The tasks of one workspace run one at a time, on the worker that holds it when that worker
  fits.
- **G6** Workspaces are opt-in per worker, bounded in size and count, private to the worker's user,
  and never written through a symbolic link.
- **G7** A client on another network reaches a master that only P2P can reach.

## Non-goals

- A cache of action results keyed by their inputs, as Bazel remote execution keeps.
- Moving a workspace or its build cache between workers. A task that lands on another worker starts
  cold there.
- Git history on workers, or git bundles. The tree sync below needs no `git` on the worker.
- Removing a workspace on request. In this design only eviction removes one.
- Two tasks in one workspace at the same time.
- Compression of uploads (still a non-goal of design 0006).
- Workspaces on Android workers.

## Design

### D1 The workspace reference

`crates/core/src/task.rs`:

- `Task` gains `workspace: Option<WorkspaceRef>`.
- `WorkspaceRef { name: String, source: Option<WorkspaceSource> }`.
- `WorkspaceSource::Tree { manifest: BlobId, manifest_size: u64 }` is the tree to sync before the
  run. `None` runs in the workspace as it is, and fails when the worker does not hold it.
- A name is 1 to 64 bytes of ASCII letters, digits, `.`, `_` and `-`, and does not start with `.`.
  `Task::validate` refuses any other name, so a name can never form a path.
- In a workspace task, `TaskSpec::Command { working_dir }` is relative to the workspace root, with
  the same checks as today. `ShellScript` runs at the workspace root.
- Workspace tasks accept `Command` and `ShellScript` only. `Task::validate` refuses a workspace with
  any other spec, or together with `inputs` or `staged_files`.

Names are scoped to one master. Two agents that use one name share one workspace. Agents should use
a name such as `<repository>-<agent>`.

### D2 The worker's workspace store

New module `crates/worker/src/workspace.rs`:

- **Root.** `<cache dir>/oxideswarm/workspaces`, where the cache directory is `$XDG_CACHE_HOME` or
  `~/.cache` on Linux, `~/Library/Caches` on macOS, and `%LOCALAPPDATA%` on Windows. The worker
  option `--workspace-root`, `RUSTY_GRID_WORKSPACE_ROOT` or the config key `workspace_root`
  replaces it. The root gets the checks of design 0021: owned by the worker's user, mode 0700, not a symbolic
  link. It is not under the sandbox base, because the operating system may empty a temporary
  directory.
- **Layout.** `<root>/<name>/tree/` holds the files and is the task's current directory.
  `<root>/<name>/state.json` holds the synced manifest id, the state (`Ready` or `Syncing`), the last
  use and the measured size. `<root>/<name>/tmp/` is the task's `TMPDIR`, emptied before each task.
- **Opt-in.** Only a worker started with `--workspaces` (`RUSTY_GRID_WORKSPACES=1`, config
  `workspaces = true`) advertises `ProtocolCapabilities::WORKSPACES`. The option stays off by default
  at least until FEAT-3 is Implemented.
- **Bounds.** `--workspace-limit-mb` (default 20,480) and `--max-workspaces` (default 8). After each
  task the worker measures the workspace, with a cap on directory entries. It then evicts whole
  workspaces, least recently used first, until the total fits. It never evicts a workspace that has a
  task. A single workspace above the limit stays, and the task's stderr carries a warning.
- **Lock.** One task per workspace at a time, enforced by an in-process lock keyed by name. The
  master already avoids a second task (D4). The lock is the safety net.
- **Environment.** The task gets `RUSTY_GRID_WORKSPACE` (the name) and `RUSTY_GRID_WORKSPACE_DIR`
  (the tree path) in addition to the variables `execute_process_supervised` sets today.

### D3 Sync

Before the run, when the task carries a `WorkspaceSource`:

1. Write `state: Syncing` to `state.json` and flush it.
2. Fetch the blobs the cache lacks, through the existing `BlobClient`.
3. For each file of the new manifest: keep it if it is a regular file whose size and modification
   time match the values the last sync recorded. Otherwise compare its BLAKE3 hash with the id.
   Write a file that differs to a temporary name in its directory, set its executable bit, and
   rename it over the old one.
4. Delete each file that the previous manifest listed and the new one does not. Then remove the
   directories this left empty. Files no manifest ever listed, such as `target/`, stay.
5. Write `state: Ready` with the new manifest id, the recorded sizes and times, and flush it.

Rules:

- When the new manifest id equals the recorded one and the state is `Ready`, step 3 checks only
  sizes and times. A task that edited a tracked file is therefore caught by its time.
- A workspace found in `Syncing` (a crash during a sync) is synced with every file hashed.
- Paths come from a validated `TreeManifest` (`validate_blob_path`). Every parent directory must be
  a real directory, never a symbolic link. A manifest path where the tree has a directory or a
  symbolic link fails the task with a message that names the path.
- A failed sync fails the task before anything runs. The workspace stays in `Syncing`.

### D4 Master: features, affinity, one task at a time

`crates/core/src/protocol.rs`, `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`:

- New bit `ProtocolCapabilities::WORKSPACES` (`1 << 10`). `required_features` adds it to a task
  that has a workspace, and adds `BLOB_TRANSFER` when the workspace has a source.
- `admit_blob_task` admits and pins the tree's blobs, as for `inputs` today.
- **Affinity.** The master remembers, per workspace name, the worker that last finished a task in
  it. `Engine::rank` puts that worker first when it is among the candidates. A `require:worker=`
  entry still wins. When that worker is gone, the task goes to any worker that fits, and starts cold.
  The map is in memory until FEAT-1 adds the journal, so a master restart costs one cold build.
- **One at a time.** The placement engine does not place a workspace task while another task with
  the same workspace name is `Scheduled` or `Running`. The task stays queued and reserves no seat.

### D5 Client: choosing the files

`crates/cli/src/blobs.rs`:

- New `scan_workspace_source(dir, limits)` walks `dir` with the `ignore` crate. It honours
  `.gitignore`, `.ignore` and `.git/info/exclude`, includes hidden files, and always skips `.git/`.
- Symbolic links and special files are skipped and listed, as `scan_directory` does today. They
  appear in the reply so an agent sees what was left out.
- Limits for workspace sources: 100,000 files and 2 GiB by default, below the `TreeManifest` limits.
  The CLI and `mcp` take `--workspace-max-files` and `--workspace-max-mb`.
- The result is one `TreeManifest`. The client uploads the blobs the master lacks, as `--input`
  does.

### D6 CLI and MCP

`crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`:

- `submit --workspace NAME` syncs the current directory. `--sync DIR` syncs another directory, and
  `--no-sync` runs in the workspace as it is. `--cwd REL` sets the working directory inside the
  workspace for `--command`.
- A workspace task without `--timeout-secs` gets 1,800 s, not 60 s. The same default applies to MCP
  `timeout_secs`.
- MCP `submit` gains `workspace: { name, sync_dir, cwd }`. `sync_dir` defaults to `.`, is confined
  to the MCP server's current directory as `files` is, and `null` means no sync. `workspace` together
  with `files` is refused with `-32602`.
- The MCP reply gains `workspace: { name, worker, files, uploaded_bytes, skipped }`.
- New MCP tool `workspaces` and CLI command `workspaces` list the affinity map: name, worker and last
  use. They read it through a new `ClientMessage::ListWorkspaces`, answered by
  `ClientResponse::Workspaces`, both appended last to their enums.

### D7 P2P for clients

`crates/core/src/transport.rs`, `crates/cli/src/connect.rs`, `crates/master/src/server.rs`:

- `connect_master` accepts a P2P ticket as well as `HOST:PORT`. It reuses `parse_p2p_ticket`,
  `GRID_ALPN` and `GridStream::P2p`. The worker's private `establish_p2p` becomes a shared helper in
  `crates/core/src/transport.rs`.
- The master's P2P handler accepts `InboundMessage::Client` after the cluster handshake instead of
  refusing it. Blob requests of clients go on the data stream.
- The MCP server and `BlobConnection` use `GridStream` instead of `TcpStream`.
- A ticket always needs a cluster secret (`master_requires_secret`), as for workers.

### D8 Tools on workers, and FEAT-10

`crates/core/src/capabilities.rs`, `crates/core/src/placement.rs`, `crates/worker/src/runner.rs`:

- At start-up the worker probes `cargo --version`, `rustc --version` and `git --version`, each
  bounded by `HOST_PROBE_TIMEOUT` (design 0020). It advertises `tool:cargo`, `tool:rustc` and
  `tool:git` for the tools it found.
- `tool:` becomes the seventh reserved tag prefix. A new placement entry `require:tool=<name>`
  matches it. The CLI takes `--require-tool NAME`, and MCP `submit` takes `tools: [NAME]`.
- `RustCompilation` runs `cargo build --offline` when its sources contain `Cargo.toml`, as FEAT-10
  asks. Workspace tasks do not need this: they run `cargo` as a `Command`.

## Compatibility

- `Task` gains a field, and bincode is positional. Every node must run one build.
  `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` move to 3, so the master and the
  worker refuse a version 2 peer at registration.
- Clients carry no version today, so an old client and a new master can fail without a clear
  message. FEAT-8 adds a client version, and should land before this design ships.
- `ClientMessage::ListWorkspaces` and `ClientResponse::Workspaces` are appended last. An older
  master closes the connection on them, and the CLI says the master may be older.
- A worker without `--workspaces`, or an older one, never advertises `WORKSPACES`. A workspace task
  then waits as `ClusterLacksCapability`, or fails when the master is configured to fail
  unplaceable tasks (SCHED-3).
- Upgrade order as in `DEPLOYMENT.md` 3.5: the master first, then workers and clients.

## Security

- **Opt-in.** A workspace keeps the owner's source code on the worker's disk, and runs commands an
  agent chose. Workers offer it only with `--workspaces`.
- **Same user as today.** Until FEAT-3, a workspace task runs as the worker's user and can read what
  that user reads, including `cluster.secret` and other workspaces. This is no wider than any task
  today. With FEAT-3, a workspace task runs at least at level `fs`, with write access to
  `<root>/<name>/` only and no access to other workspaces.
- **Paths.** Names cannot form paths (D1). Sync never follows a symbolic link, and deletes only
  files an earlier manifest listed. The root is private to the worker's user (D2).
- **Bounds.** Workspace size and count (D2), files and bytes per source (D5), and the manifest
  limits of design 0006.
- **Secrets.** Task processes still do not inherit `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE` or
  `OXIDE_MESH_TOKEN`.
- **Network.** A build may download dependencies. FEAT-3 level `fs+net` blocks that, and then a
  project needs vendored dependencies.

## Test plan

- **Unit:** name validation and `Task::validate` refusals (`task.rs`). Sync cases in
  `workspace.rs`: a new file, a changed file, a deleted file, an untracked file that stays, a
  tracked path over an untracked file, a symbolic link in a parent, recovery from `Syncing`, the
  unchanged fast path, eviction order and the lock. Scheduler cases: affinity first, `require:worker`
  wins, no second task of one workspace in a pass. Scan cases: `.gitignore`, `.git/` skipped,
  limits. Both codecs round-trip the new field. `crates/core/src/wire_layout_tests.rs` and
  `crates/core/tests/data/wire_layout.txt` record the new layout.
- **Integration:** `tests/agent_workspace_e2e.rs`, registered as a `[[test]]` of `rusty_grid_cli`.
  One master and one worker with `--workspaces` on loopback, with a temporary `HOME` and workspace
  directory. A small crate without dependencies builds with `cargo build --offline`. A second build
  prints no `Compiling` line. A file deleted locally is gone from the workspace after the next sync,
  and `target/` is still there. The same flow runs through `oxideswarm mcp` over stdio.
- **Live:** two machines from [the two-machine guide](../guides/two-machine-cluster.md). An agent on
  one runs `cargo test -p rusty_grid_core --lib` over MCP on the other at a commit that machine did
  not have. The second run reuses `target/`. The run is recorded with its date in the guide of W10.
- **Gates:** the gates in [BACKLOG.md](../BACKLOG.md), plus the integration test above.

## Work items

Roadmap Phase C names these steps WS-1 to WS-7. The column "Roadmap" maps them.

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | `Task::workspace`, `WorkspaceRef`, name rules, the `WORKSPACES` bit, protocol version 3, both mirrors and all four `From` impls. Needs FEAT-8. Roadmap WS-1 | `crates/core/src/task.rs`, `crates/core/src/protocol.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt` | Both codecs round-trip, the layout test records version 3, and `Task::validate` refuses bad names | Planned |
| W2 | The workspace store, sync, lock, bounds and worker options. Roadmap WS-1, WS-2 | `crates/worker/src/workspace.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `README.md` | The sync unit tests of the test plan pass | Planned |
| W3 | Required feature, affinity, one task per workspace. Roadmap WS-1 | `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/server.rs` | The scheduler unit tests of the test plan pass | Planned |
| W4 | Source selection with `.gitignore` and limits. Roadmap WS-2 | `crates/cli/src/blobs.rs`, `crates/cli/Cargo.toml` | The scan unit tests pass, and `cargo audit` passes with the `ignore` crate | Planned |
| W5 | `submit --workspace`, `--sync`, `--no-sync`, `--cwd`, the MCP `workspace` field, the `workspaces` tool and command. Roadmap WS-5 | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `crates/core/src/protocol.rs`, `README.md` | `tests/mcp_server_e2e.rs` covers a workspace submission and the `workspaces` tool | Planned |
| W6 | End-to-end test | `tests/agent_workspace_e2e.rs`, `crates/cli/Cargo.toml` | The integration test of the test plan passes on Linux, macOS and Windows in CI | Planned |
| W7 | P2P for clients, the MCP server and blob connections. Roadmap WS-3 | `crates/core/src/transport.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/blobs.rs`, `crates/master/src/server.rs` | A test submits a task and downloads its output through a ticket, and a client without a secret is refused | Planned |
| W8 | Tool probes, `require:tool=`, and `cargo build --offline` in `RustCompilation`. Roadmap WS-4, FEAT-10 | `crates/core/src/capabilities.rs`, `crates/core/src/placement.rs`, `crates/worker/src/runner.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs` | The FEAT-10 "Done when" passes, and a `require:tool=cargo` task avoids a worker without cargo | Planned |
| W9 | Several blob uploads in flight from one client. Roadmap WS-6 | `crates/cli/src/blobs.rs`, `crates/core/src/blob_transfer.rs` | A test with 10,000 small files sees more than one request in flight, and `blob_transfer_e2e` passes | Planned |
| W10 | Guide and live record. Roadmap WS-7 | `docs/guides/offload-cargo-test.md`, `docs/README.md` | The live run of the test plan is recorded with its date, and the guide follows the guide template | Planned |
| W11 | Symbolic links and empty directories in manifests (`oxideswarm-tree/2`). Optional | `crates/core/src/blob.rs`, `crates/worker/src/workspace.rs`, `crates/cli/src/blobs.rs` | A link inside the tree arrives as a link, and a link that leaves the tree is refused | Planned |

Order: W1, then W2 to W5 in parallel, then W6 and W10. W7 to W9 and W11 can follow in any order.
The roadmap keeps Phase B (FEAT-1, FEAT-2, FEAT-3) before Phase C ships, and `--workspaces` stays
off by default until FEAT-3 is Implemented.

## Status log

- 2026-11-07: Draft written from the code at `master` `5c518b5`, after the owner accepted the
  roadmap decisions Q1 to Q5.
