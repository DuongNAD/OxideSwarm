# 0039: Persistent workspaces for coding agents

Status: Implemented. W1 to W11 are built and W10 is recorded live
Created: 2026-11-07    Updated: 2026-11-09

## Summary

- A task can name a **workspace**: a directory on a worker that outlives the task. A second build
  in the same workspace reuses its `target/`.
- The client **syncs** a local directory into the workspace. Only changed files cross the network,
  files deleted locally are deleted in the workspace, and build output the worker made is kept.
- The client honours `.gitignore` and never sends `.git/`. The master sends the next task of a
  workspace to the worker that holds it, and runs the tasks of one workspace one at a time.
- An agent uses it through one new field of the MCP `submit` tool, and a person through
  `submit --workspace NAME`. Workers offer workspaces only when started with `--workspaces`.
- This is Phase C of the roadmap (`docs/ROADMAP.md`) and the design FEAT-4 asks for. FEAT-10 and
  P2P connections for clients are work items here as well.

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
- A name is 1 to 64 bytes of lowercase ASCII letters, digits, `.`, `_` and `-`. It starts with a
  letter or a digit and does not end with `.`. Its part before the first `.` is not a Windows device
  name (`WINDOWS_RESERVED_NAMES` in `crates/worker/src/sandbox.rs`: `con`, `prn`, `aux`, `nul`,
  `com1` to `com9`, `lpt1` to `lpt9`). `Task::validate` refuses any other name, so a name can never
  form a path or a device.
- Names are lowercase because the file systems of macOS and Windows ignore case by default. `Foo`
  and `foo` would be one directory there, but two keys for the master (D4) and for the worker's lock
  (D2), and two tasks could then run in one directory. Windows also drops a trailing `.`, so `foo.`
  and `foo` would be one directory.
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
  `<root>/<name>/state.json` holds the state (`Ready` or `Syncing`), the records and pending paths of
  D3, the last use and the measured size. `<root>/<name>/tmp/` is the task's `TMPDIR`, emptied before
  each task. `<root>/<name>/lock` is the lock file below.
- **Opt-in.** Only a worker started with `--workspaces` (`RUSTY_GRID_WORKSPACES=1`, config
  `workspaces = true`) advertises `ProtocolCapabilities::WORKSPACES`. The option stays off by default
  at least until FEAT-3 is Implemented.
- **Bounds.** `--workspace-limit-mb` (default 20,480) and `--max-workspaces` (default 8) apply to the
  whole root. After each task the worker measures the workspace, with a cap on directory entries. It
  then evicts whole workspaces, least recently used first, until the total fits. It skips a
  workspace whose lock it cannot take. A workspace that alone exceeds the limit is evicted as soon
  as its task ends, so the next task in it starts cold. The task's stderr carries a warning with the
  size and the limit. A `target/` that grows with every commit therefore cannot fill the disk.
- **Lock.** One task per workspace at a time. The worker holds an exclusive lock on
  `<root>/<name>/lock` (`std::fs::File::try_lock`, stable since Rust 1.89) during the sync, the run
  and the measurement, and takes it before it evicts the workspace. An in-process map keyed by name
  serialises the tasks of one worker before they reach the file. The file lock matters when two
  workers share one root, as workers started under one `HOME` do (`test_integration.sh` starts
  three). The master already avoids a second task (D4). The lock is the safety net.
- **Store lock.** Measuring the root and choosing what to evict happen under an exclusive lock on
  `<root>/store.lock`, so two workers on one root never evict against stale totals.
- **Environment.** The task gets `RUSTY_GRID_WORKSPACE` (the name) and `RUSTY_GRID_WORKSPACE_DIR`
  (the tree path) in addition to the variables `execute_process_supervised` sets today.

### D3 Sync

`state.json` keeps two lists, so the worker never needs an old manifest blob, which the blob cache
may have evicted:

- **Records:** for each path of the last completed sync, the blob id, and the size and modification
  time of the file as the sync left it.
- **Pending paths:** the paths of every manifest whose sync started after the last completed one.

Before the run, when the task carries a `WorkspaceSource`:

1. Add the paths of the new manifest to the pending paths, write `state: Syncing`, and flush
   `state.json`.
2. Fetch the blobs the cache lacks, through the existing `BlobClient`.
3. For each file of the new manifest, keep it only when all of these hold: the records list its
   path with the same blob id as the new manifest, and the file is a regular file whose size and
   modification time equal the record. Otherwise hash the file. When the hash differs from the
   id, write the blob to a temporary name in the same directory, set the executable bit, and rename
   it over the old file.
4. Delete each path that the records or the pending paths list and the new manifest does not. Then
   remove the directories this left empty. Files no manifest ever listed, such as `target/`, stay.
5. Replace the records with the new manifest's paths, ids, sizes and times. Clear the pending
   paths, write `state: Ready`, and flush `state.json`.

Rules:

- A file whose content changed on the client has a new blob id, so step 3 always rewrites it, even
  when its size is the same. Example: `assert_eq!(x, 1)` changed to `assert_eq!(x, 2)`.
- When nothing changed on the client, step 3 hashes no file. A task that edited a tracked file
  changed its modification time, so the next sync hashes it and restores it.
- A workspace found in `Syncing` (a crash or a failed sync) is synced with every file hashed.
  Step 4 also deletes the files that the interrupted sync wrote and the new manifest drops, because
  their paths are still pending. Without this, a test file that the client deleted after a failed
  sync, such as `tests/new.rs`, would stay, and `cargo test` would still build it.
- Paths come from a validated `TreeManifest` (`validate_blob_path`). Every parent directory must be
  a real directory, never a symbolic link. A manifest path where the tree has a directory or a
  symbolic link fails the task with a message that names the path. Step 4 deletes regular files
  only, and never follows a link.
- A failed sync fails the task before anything runs. The workspace stays in `Syncing`, and the next
  sync repairs it.

Why a tree and not a git bundle: FEAT-4 suggests "a git or bundle source", and the draft design
0042 (pull request 111) proposed a git bundle plus a binary patch. This design syncs a tree
instead, for four reasons:

- Workers need no `git`, and run no `git` command on content an agent sent, so no hook or
  repository setting can act on the worker.
- Uncommitted edits and new files travel the same way as committed ones, with no patch to apply.
- Deduplication works per file through the blob store. After a commit, only the changed files
  cross the network, while a bundle of `HEAD` is a new blob after every commit.
- The worker keeps no history. That is a non-goal.

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
- A client opens one bidirectional stream and writes a new tag `STREAM_CLIENT` (`0x03`) as its
  first byte. Workers keep `STREAM_CONTROL` (`0x01`) and `STREAM_DATA` (`0x02`) on two streams.
  The master's P2P accept loop (`server.rs`, where it reads the first tag today) runs the cluster
  handshake on that one stream and hands it to the client handler. The multiplexed worker path
  keeps refusing `InboundMessage::Client`, as it does today. This tag comes from the draft design
  0042 (pull request 111).
- The client's iroh endpoint lives as long as its connection, and closes when the connection drops.
- The MCP server and `BlobConnection` use `GridStream` instead of `TcpStream`.
- A ticket always needs a cluster secret (`master_requires_secret`), as for workers.

### D8 Tools on workers, and FEAT-10

`crates/core/src/capabilities.rs`, `crates/core/src/placement.rs`, `crates/worker/src/runner.rs`,
`crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`:

- At start-up the worker probes `cargo --version`, `rustc --version` and `git --version`, each
  bounded by `HOST_PROBE_TIMEOUT` (`3 s`, design 0020). On Windows the probe uses
  `augment_windows_path` and `resolve_windows_executable` (`crates/core/src/capabilities.rs`),
  shared with `TaskRunner`, so start-up probing and task execution agree on tool paths. The worker
  advertises `tool:cargo`, `tool:rustc` and `tool:git` for the tools it found.
- `tool:` becomes the eighth reserved tag prefix (alongside `os:`, `arch:`, `slots:`, `iso:`,
  `id:`, `state:` and `link:`). A new placement entry `require:tool=<name>` matches it. The CLI
  takes `--require-tool NAME`, and MCP `submit` takes `tools: [NAME]`. When a `RustCompilation`
  task (`submit --type compile` or MCP `submit` with `type: "compile"`) has no explicit tool
  requirement, the client automatically adds `require:tool=cargo` when `Cargo.toml` is present in
  the task's sources/inputs and `require:tool=rustc` otherwise.
- `RustCompilation` runs `cargo build --offline` when its sources contain `Cargo.toml`, as FEAT-10
  asks. Workspace tasks do not need this: they run `cargo` as a `Command`.
- **Hardening for `cargo build --offline` in `RustCompilation`:**
  - `compiler_flags` are validated against an allowlist of safe `cargo build` flags before spawning
    `cargo`; `--config`, `-Z`, `--manifest-path`, `--online`, and any `--target-dir` that resolves
    outside the task sandbox are refused.
  - `CARGO_HOME` and `RUSTUP_HOME` are set to the worker's `HostDirs` (`CARGO_NET_OFFLINE=true`) so
    `cargo build --offline` can read pre-warmed crate caches in `$CARGO_HOME/registry` and
    `$CARGO_HOME/git`. Under `--isolation fs|fs+net` (design 0038), `CARGO_HOME` and `RUSTUP_HOME`
    are mounted/ACL'd read-only while `$CARGO_HOME/credentials.toml` is denied.
  - Any `rust-toolchain` or `rust-toolchain.toml` file materialized in the sandbox root is removed
    before invoking `cargo`, `RUSTUP_TOOLCHAIN` is pinned to the worker's active toolchain (from
    `RUSTUP_TOOLCHAIN` or `default_toolchain` in `<rustup_home>/settings.toml`), and
    `RUSTUP_AUTO_INSTALL=0` is set so `rustup` never attempts network access.

## Compatibility

- `Task` gains a field, and bincode is positional. Every node must run one build.
  `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` move up by one, so the master
  refuses an older worker at registration. FEAT-8 (pull request 110) moves them to 3, so this
  design takes the number after the one `master` has when W1 lands.
- Clients carry no version today, so an old client and a new master can fail without a clear
  message. FEAT-8 adds a client version, and should land before this design ships.
- `ClientMessage::ListWorkspaces` and `ClientResponse::Workspaces` are appended last. An older
  master closes the connection on them, and the CLI says the master may be older.
- A worker without `--workspaces`, or an older one, never advertises `WORKSPACES`. A workspace task
  then waits as `ClusterLacksCapability`, or fails when the master is configured to fail
  unplaceable tasks (SCHED-3).
- `tool:` becomes a reserved tag prefix (D8). A user tag that starts with `tool:` is then refused,
  as tags with the other seven prefixes are. Such a tag must be renamed before the upgrade.
- A P2P client sends `STREAM_CLIENT` (D7). An older master reads it as an invalid first tag and
  closes the connection, so the client says the master may be older.
- Upgrade order as in `DEPLOYMENT.md` 3.5: the master first, then workers and clients.

## Security

- **Opt-in.** A workspace keeps the owner's source code on the worker's disk, and runs commands an
  agent chose. Workers offer it only with `--workspaces`.
- **Same user as today.** Until FEAT-3, a workspace task runs as the worker's user and can read what
  that user reads, including `cluster.secret` and other workspaces. This is no wider than any task
  today. With FEAT-3, a workspace task runs at least at level `fs`, with write access to
  `<root>/<name>/` only and no access to other workspaces.
- **Cargo `build.rs` and procedural macros.** During `cargo build`, build scripts (`build.rs`) and
  procedural macros execute arbitrary code on the worker host. A Cargo `RustCompilation` task is
  therefore security-equivalent to a `Command` task and relies on `--isolation fs|fs+net` (FEAT-3 /
  design 0038) for filesystem and network confinement.
- **Paths.** Names cannot form paths (D1). Sync never follows a symbolic link, and deletes only
  files an earlier manifest listed. The root is private to the worker's user (D2).
- **Bounds.** Workspace size and count (D2), files and bytes per source (D5), and the manifest
  limits of design 0006.
- **Secrets.** Task processes still do not inherit `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE` or
  `OXIDE_MESH_TOKEN`.
- **Network.** A build may download dependencies. FEAT-3 level `fs+net` blocks that, and then a
  project needs vendored dependencies.

## Test plan

- **Unit:** name validation and `Task::validate` refusals (`task.rs`): uppercase letters, a
  trailing `.`, `con` and `nul.txt`. Sync cases in `workspace.rs`: a new file, a changed file, a
  changed file of the same size, a deleted file, an untracked file that stays, a tracked path over
  an untracked file, a symbolic link in a parent, recovery from `Syncing`, a file written by a
  failed sync and dropped by the next manifest, the unchanged fast path, eviction order, eviction of
  a single workspace above the limit, and the lock. Two stores on one root never sync one workspace
  at the same time. Scheduler cases: affinity first, `require:worker`
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
| W1 | `Task::workspace`, `WorkspaceRef`, name rules, the `WORKSPACES` bit, a lightweight `ClientMessage::Hello` / `ClientResponse::Hello` version probe variant appended last (replacing `connect_master`'s `ClusterStatus` probe from `FEAT-8b`), the next protocol version, both mirrors and all four `From` impls. Needs FEAT-8. Roadmap WS-1 | `crates/core/src/task.rs`, `crates/core/src/protocol.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/cli/src/connect.rs`, `crates/master/src/server.rs` | Both codecs round-trip, `connect_master` probes with `Hello`, the layout test records the new version, and `Task::validate` refuses bad names | Implemented |
| W2 | The workspace store, sync, lock, bounds and worker options. Roadmap WS-1, WS-2 | `crates/worker/src/workspace.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client.rs`, `crates/worker/src/isolation.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `README.md` | The sync unit tests of the test plan pass | Done (W2a + W2b; workspaces with Windows isolation wait for FEAT-3d) |
| W3 | Required feature, affinity, one task per workspace. Roadmap WS-1 | `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/server.rs` | The scheduler unit tests of the test plan pass | Done |
| W4 | Source selection with `.gitignore` and limits. Roadmap WS-2 | `crates/cli/src/blobs.rs`, `crates/cli/Cargo.toml` | The scan unit tests pass, and `cargo audit` passes with the `ignore` crate | Done |
| W5 | `submit --workspace`, `--sync`, `--no-sync`, `--cwd`, the MCP `workspace` field, the `workspaces` tool and command. Roadmap WS-5 | `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs`, `crates/core/src/protocol.rs`, `README.md` | `tests/mcp_server_e2e.rs` covers a workspace submission and the `workspaces` tool | Done (`workspaces` lists the master's affinity map only, not the workspaces stored on workers, which needs W2b; `submit --workspace` warns when no connected worker advertises `WORKSPACES` yet) |
| W6 | End-to-end test | `tests/agent_workspace_e2e.rs`, `crates/cli/Cargo.toml` | The integration test of the test plan passes on Linux, macOS and Windows in CI | Done |
| W7 | P2P for clients, the MCP server and blob connections. Roadmap WS-3 | `crates/core/src/transport.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/mcp.rs`, `crates/cli/src/blobs.rs`, `crates/master/src/server.rs` | A test submits a task and downloads its output through a ticket, and a client without a secret is refused | Done |
| W8 | Tool probes, `require:tool=`, and `cargo build --offline` in `RustCompilation`. Roadmap WS-4, FEAT-10 | `crates/core/src/capabilities.rs`, `crates/core/src/placement.rs`, `crates/worker/src/runner.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs` | The FEAT-10 "Done when" passes, and a `require:tool=cargo` task avoids a worker without cargo | Implemented |
| W9 | Several blob uploads in flight from one client. Roadmap WS-6 | `crates/cli/src/blobs.rs`, `crates/core/src/blob_transfer.rs` | A test with 10,000 small files sees more than one request in flight, and `blob_transfer_e2e` passes | Done |
| W10 | Guide and live record. Roadmap WS-7 | `docs/guides/offload-cargo-test.md`, `docs/README.md` | The live run of the test plan is recorded with its date, and the guide follows the guide template | Done |
| W11 | Symbolic links and empty directories in manifests (`oxideswarm-tree/2`). Optional | `crates/core/src/blob.rs`, `crates/worker/src/workspace.rs`, `crates/cli/src/blobs.rs` | A link inside the tree arrives as a link, and a link that leaves the tree is refused | Done |

Order: W1, then W2 to W5 in parallel, then W6 and W10. W7 to W9 and W11 can follow in any order.
The roadmap keeps Phase B (FEAT-1, FEAT-2, FEAT-3) before Phase C ships, and `--workspaces` stays
off by default until FEAT-3 is Implemented.

## Status log

- 2026-11-07: Draft written from the code at `master` `5c518b5`, after the owner accepted the
  roadmap decisions Q1 to Q5.
- 2026-11-08: Review fixes. Step 3 of the sync keeps a file only when its recorded blob id equals
  the new one. Pending paths delete the files of an interrupted sync. Names are lowercase and refuse
  Windows device names. A file lock guards workspaces that two workers share. A workspace above the
  limit is evicted when its task ends. The protocol version moves to the number after FEAT-8's. The
  owner chose this design over the draft design 0042 (pull request 111), and took two parts of it:
  the Windows device names and the `STREAM_CLIENT` tag. Status set to Accepted.
- 2026-11-08: `W1` implemented in protocol version 6. `Task::workspace`, `WorkspaceRef`,
  `WorkspaceSource::Tree` and `validate_workspace_name` are in `crates/core/src/task.rs`;
  `Task::validate` also refuses a workspace together with a spec other than `Command` or
  `ShellScript`, with `inputs` or `staged_files`, or with a source manifest size outside
  `1..=MAX_MANIFEST_BYTES`. `ProtocolCapabilities::WORKSPACES` is `1 << 10` and is **not** in
  `all_supported()` until W2 and W3 land. `ClientMessage::Hello` / `ClientResponse::Hello` replace
  the `ClusterStatus` probe of `connect_master`. Details and the legacy decoding are in
  `docs/design/0040-protocol-versioning.md`. The Windows device names are a lowercase copy of
  `WINDOWS_RESERVED_NAMES` in core, which cannot depend on the worker crate.
- 2026-11-08: W4 done. `scan_workspace_source(root, WorkspaceLimits)` in `crates/cli/src/blobs.rs`
  returns the `TreeManifest`, its blob id and the blobs for `upload_inputs`. Limits are the D5
  defaults, as constants: `DEFAULT_WORKSPACE_MAX_FILES` = 100,000 and `DEFAULT_WORKSPACE_MAX_BYTES`
  = 2 GiB in total; D5 sets no single-file limit, so none exists. The scan reads only the ignore files
  inside the root (no parent directories, no global excludes), so the result does not depend on the
  machine. The CLI flags `--workspace-max-files` and `--workspace-max-mb` and the existing `--input
  <dir>` are unchanged; W5 wires them.
- 2026-11-09: W9 done, no wire change. `BlobConnection` (`crates/cli/src/blobs.rs`) now writes
  requests back to back and a reader task hands each reply to the oldest waiting call; replies are
  matched by position, which is sound because the master answers the requests of one connection in
  arrival order (design 0006 D3). The client keeps at most `MAX_IN_FLIGHT_REQUESTS` (16) requests
  in flight, and `TRANSFER_CONCURRENCY` (8) blobs move at the same time, so 8 were in flight in the
  test. On the master, `handle_client_connection` now runs up to `MAX_IN_FLIGHT_REQUESTS` blob
  requests of one connection at the same time (different blobs overlap their disk work), sends the
  replies in request order, answers any other message only after the pending blob replies, and
  stops reading while 16 are pending, so a client that floods requests is held back by the socket
  buffers. Two chunks of the same blob must still be sent one after the other, which `upload_blob`
  does. A new client works with an older master (it answers one request at a time) and an older
  client with this master. Measured on one machine (macOS, release build, loopback, 10,000 files
  of about 20 bytes): 42.6 s with one request in flight, 29.6 s with up to 8. The master's
  per-blob `sync_all` bounds the gain on loopback; the larger win is a link with real latency.
  Compression is left out: design 0006 and the non-goals above keep it out of scope, and this
  record does not change that. The existence check stays one `Missing` request per 65,536 ids, so
  10,000 files need one.
- 2026-11-09: W2a done, not wired. `crates/worker/src/workspace.rs` holds `WorkspaceStore` (root and
  the design 0021 checks, `acquire`, `list`, `evict_to_fit`), `WorkspaceGuard` (`sync`,
  `prepare_tmp`, `finish`, `task_env`, `tree_dir`, `tmp_dir`), the `BlobSource` trait the sync reads
  blobs from, `state.json` and the unit tests of the test plan. Two details differ from D3 without
  changing its outcome: deletions run before the writes (a path that turns from a file into a
  directory needs no special case), and temporary files live in `<name>/tmp/` (same file system, so
  the rename stays atomic, and a crash leaves nothing in the tree). On Windows the sandbox path
  rules apply in full; elsewhere a file such as `aux.rs` is legal. `File::try_lock` is stable since
  1.89, below the MSRV 1.91. W2b is pending: the runner and client wiring, `--workspaces`,
  `--workspace-root`, `--workspace-limit-mb`, `--max-workspaces`, and advertising `WORKSPACES`; it
  waits for the pull requests that rewrite `runner.rs` and `client.rs`.
- 2026-11-09: W2a review fixes. `sync` recreates `tmp/` through `prepare_tmp`, so a task that
  replaced it with a link cannot make the sync write outside the workspace, and stale temporary
  files are removed; temporary names carry a random suffix. A manifest whose paths differ only by
  case (Unicode lowercasing) is refused by the store on every host, because such a tree cannot exist
  on the case-insensitive file systems of macOS and Windows; names that differ only by NFC/NFD
  normalisation are a residual and are not detected. A kept file with more than one hard link keeps
  its mode. A workspace with a missing or malformed `state.json` is listed as `Syncing` with its
  measured size, `finish` writes a record for it, and eviction removes the whole `<name>/`
  directory except a held lock.
- 2026-11-09: W3 done. `required_features` (`crates/master/src/queue.rs`) adds `WORKSPACES` for any
  workspace task and `BLOB_TRANSFER` when `workspace.source.is_some()`. `TaskQueueInner` tracks per
  workspace name the worker that last finished (`Completed` or `Failed`, excluding worker rejections)
  a task in it (`MAX_WORKSPACE_AFFINITIES = 4,096`, oldest evicted) and refuses to transition a
  second task of the same workspace name to `Scheduled` while one is already `Scheduled` or `Running`.
  `Engine::place` (`crates/master/src/scheduler.rs`) skips a workspace task without reserving a seat
  when another task of that workspace is active or placed earlier in the pass, and `Engine::rank`
  places the affinity worker first when it is among the eligible `candidates` (so `require:worker=`
  in `candidates` wins over affinity). `admit_blob_task` (`crates/master/src/server.rs`) and
  `repin_recovered_blobs` (`crates/master/src/queue.rs`) resolve and pin `WorkspaceSource::Tree`
  manifests and their listed file blobs.
- 2026-11-09: W8 and FEAT-10 implemented. Workers probe `cargo --version`, `rustc --version` and
  `git --version` at start-up under `HOST_PROBE_TIMEOUT` (`crates/core/src/capabilities.rs`) and
  advertise `tool:cargo`, `tool:rustc` and `tool:git` (`crates/worker/src/client.rs`). `tool:` is now
  a reserved tag prefix alongside `os:`, `arch:`, `slots:`, `link:`, `id:`, `state:` and `iso:`, and
  `Placement` (`crates/core/src/placement.rs`) parses, validates and matches `require:tool=<name>`
  (exposed as `submit --require-tool <TOOL>` in `crates/cli/src/cli_app.rs` and `tools: [...]` on the
  MCP `submit` tool in `crates/cli/src/mcp.rs`; `submit --type compile` and MCP `submit`
  `type: "compile"` automatically add `require:tool=cargo` when `Cargo.toml` is present and
  `require:tool=rustc` otherwise). `TaskSpec::RustCompilation` (`crates/worker/src/runner.rs`)
  runs `cargo build --offline` with validated `compiler_flags`, pinned `RUSTUP_TOOLCHAIN`, and the
  worker's `CARGO_HOME` / `RUSTUP_HOME` when `Cargo.toml` is present in the sandbox, collects the
  built binary into `result.outputs`, and `submit --type compile --source <PATH>` builds a crate
  with a local dependency and downloads the binary to the client.
- 2026-11-09: W5 done in protocol version 7. `ClientMessage::ListWorkspaces` and
  `ClientResponse::Workspaces { workspaces: Vec<WorkspaceAffinity> }` (`WorkspaceAffinity { name,
  worker, last_used_utc }` in `crates/core/src/task.rs`, shared by `TaskQueue::list_workspaces`)
  are appended last to `ClientMessage` and `ClientResponse` (and their `Human*` / `Binary*`
  mirrors), answered from `TaskQueue::list_workspaces` in `crates/master/src/server.rs`, and
  recorded in `crates/core/tests/data/wire_layout.txt`. `oxideswarm submit`
  (`crates/cli/src/cli_app.rs`) adds `--workspace`, `--sync`, `--no-sync`, `--cwd`,
  `--workspace-max-files` and `--workspace-max-mb` (with `1,800` s default timeout when
  `--workspace` is set and `--timeout-secs` is omitted, and `--wait` bounded by `--timeout-secs`),
  plus the `oxideswarm workspaces [--json]` subcommand. `oxideswarm mcp` (`crates/cli/src/mcp.rs`)
  adds `--workspace-max-files` and `--workspace-max-mb`, the `workspace: { name, sync_dir, cwd }`
  property on `submit` (refusing `workspace` together with `files` with JSON-RPC `-32602`,
  confining `sync_dir` to the MCP working directory without symbolic links, and returning
  `workspace: { name, worker, files, uploaded_bytes, skipped }` in the `submit` reply), and the
  `workspaces` MCP tool. Current limitation until W2b lands: `workspaces` lists the master's
  affinity map only, not the workspaces stored on workers, and no worker advertises `WORKSPACES`
  yet (`listed_worker` stamps `feat:workspaces` when a worker negotiated `WORKSPACES`, and CLI and
  MCP `submit` warn `"no connected worker supports workspaces yet; the task will wait"` when no
  connected worker carries `feat:workspaces`).
- 2026-11-09: W7 done. `STREAM_CLIENT = 0x03` (`crates/core/src/transport.rs`) tags single-stream
  QUIC connections opened by CLI and MCP clients and `BlobConnection`. On the master
  (`crates/master/src/server.rs`), the P2P accept loop hands a `STREAM_CLIENT` stream directly to
  `handle_connection` without waiting for a second stream. `connect_master`
  (`crates/cli/src/connect.rs`), `BlobConnection` (`crates/cli/src/blobs.rs`), `McpState`
  (`crates/cli/src/mcp.rs`), `doctor` (`crates/cli/src/doctor.rs`) and the CLI client commands
  (`crates/cli/src/cli_app.rs`) now operate over `MessageTransport<GridStream>`, dialing an iroh P2P
  ticket when `--master`, `OXIDESWARM_MASTER`, `RUSTY_GRID_MASTER_ADDR`, `RUSTY_GRID_MASTER`, or
  `[submit]`/`[status]` `p2p_ticket` (or `master`) holds a ticket (`{...` or `node1...`), keeping the
  client endpoint alive for the lifetime of the `GridStream::P2p` via `P2pClientGuard`, requiring a
  cluster secret unless `--insecure`, and mapping an older master's stream close after `STREAM_CLIENT`
  to the protocol version upgrade hint.
- 2026-11-09: W2b done. `crates/cli/src/cli_app.rs` and `crates/cli/src/config.rs` add `--workspaces`
  (`RUSTY_GRID_WORKSPACES`, `[worker].workspaces`, off by default), `--workspace-root`
  (`RUSTY_GRID_WORKSPACE_ROOT`, `[worker].workspace_root`), `--workspace-limit-mb`
  (`RUSTY_GRID_WORKSPACE_LIMIT_MB`, `[worker].workspace_limit_mb`, default `20480`) and
  `--max-workspaces` (`RUSTY_GRID_MAX_WORKSPACES`, `[worker].max_workspaces`, default `8`), passed
  through `WorkerConfig` (`crates/worker/src/client.rs`) and `RunnerConfig`
  (`crates/worker/src/runner.rs`). Only with `--workspaces` does the worker open `WorkspaceStore` at
  start-up (refusing to start with `GridError::Config` if the root fails the private-dir checks) and
  advertise `ProtocolCapabilities::WORKSPACES` in `Register` (`WORKSPACES` remains out of
  `ProtocolCapabilities::all_supported()`). For a task with `Task::workspace`, `fetch_task_inputs`
  (`crates/worker/src/blob_client.rs`) fetches and pins the `WorkspaceSource::Tree` manifest and its
  referenced blobs into `BlobCache` (which implements `workspace::BlobSource`), and
  `TaskRunner::execute_workspace_workload` acquires the workspace lock (`DEFAULT_LOCK_WAIT`), syncs
  the tree (or requires `is_ready()` when `source` is `None`), prepares `tmp/`, runs the command or
  shell script with `cwd = tree_dir()` joined with the validated relative `working_dir`,
  `TMPDIR`/`TMP`/`TEMP = tmp_dir()`, and `task_env()` (`RUSTY_GRID_WORKSPACE`,
  `RUSTY_GRID_WORKSPACE_DIR`), copies `output_globs` from `tree_dir()` without moving files out of
  the tree, calls `finish()` in `spawn_blocking` and appends any quota warning to `stderr`. Under
  `--isolation fs|fs+net` on macOS (`sandbox-exec`) and Linux (`Landlock`), `Jail` grants read/write
  access to `<ws>/tree` and `<ws>/tmp` while denying `<workspace_root>` (other workspaces and
  `<ws>/state.json`), sandboxes, and the owner's home/secrets. Windows isolation + workspaces is
  deferred until FEAT-3c (`WorkerClient::run` refuses `--workspaces` together with
  `--isolation fs|fs+net` on Windows at start-up).
- 2026-11-09: W2b security review fix (tree-swap). Every workspace run checks via `symlink_metadata`
  and canonical path matching that `<ws>/tree` and `<ws>/tmp` are real directories under
  `<workspace_root>/<name>` (wiping and marking the workspace for repair on mismatch), `isolation`
  refuses symlinked or non-canonical jail directories, and the macOS Seatbelt profile denies
  `file-write-unlink` and `file-write-create` on the `tree` and `tmp` directory nodes themselves.
- 2026-11-09: W6 done (`tests/agent_workspace_e2e.rs`, registered as a `[[test]]` of
  `rusty_grid_cli`). Spawns a loopback `MasterServer` with a temporary blob store and a
  `WorkerClient` configured with `.with_workspaces(true)` and `.with_workspace_root(...)`. Covers
  the full test-plan integration flow with a temporary `HOME` / `OXIDESWARM_HOME`, isolated
  `CARGO_HOME`, and workspace directory over both `oxideswarm submit --workspace` and
  `oxideswarm mcp` stdio JSON-RPC: first `cargo build --offline` compiles a dependency-free crate and
  populates `target/`, second build reuses `target/` and prints no `Compiling` line (with
  `uploaded_bytes == 0` over MCP), third build after deleting a local source file removes the deleted
  file from the workspace tree while keeping `target/`, and `workspaces` lists the worker affinity.
- 2026-11-09: W10 guide (`docs/guides/offload-cargo-test.md`, indexed in `docs/README.md`) written
  to match the CLI and MCP commands, flags, cold/warm expected output, and deleted-file sync behavior
  verified in W6 (`tests/agent_workspace_e2e.rs`), with a placeholder section (`### Recorded
  two-machine run`) ready for the owner's live two-machine run.
- 2026-11-09: W10 done. The live two-machine run is recorded in
  [guides/offload-cargo-test.md](../guides/offload-cargo-test.md#recorded-two-machine-run): a Mac
  client and master, a Windows worker with `--workspaces`, and a synced tree at a commit the worker
  machine did not have. Over the CLI and over MCP, `cargo test -p rusty_grid_core --lib` passed;
  the cold runs took 210.7 s and 207.4 s and the warm runs 6.9 s and 6.6 s, with no `Compiling`
  line and nothing uploaded. The run found that closing standard input cancels a running MCP call,
  and the guide's MCP example now keeps the pipe open.
- 2026-11-09: W11 done (`oxideswarm-tree/2`). `TreeManifest` (`crates/core/src/blob.rs`,
  `MANIFEST_MAGIC = b"oxideswarm-tree/2\n"` when symbolic links or empty directories are present and
  `MANIFEST_MAGIC_V1 = b"oxideswarm-tree/1\n"` otherwise, while `TreeManifest::decode` accepts both)
  adds `symlinks: Vec<BlobSymlink>` (`BlobSymlink { path, target }`) and `empty_dirs: Vec<String>`
  (empty directories), negotiated with workers via `ProtocolCapabilities::TREE_V2` (`1 << 13`).
  `TreeManifest::validate`, `validate_symlink_target` and `check_no_symlink_traversal` enforce sorted
  unique paths across `files`, `symlinks` and `empty_dirs`, no path overlap or ancestor/descendant
  conflict with a link or empty directory, and that every `BlobSymlink::target` is a relative `/`
  path (`.`, `..` and normal components only, no `:`, `\` or NUL) whose lexical resolution from
  `path`'s parent stays strictly inside the tree root and never steps onto or through another
  `symlinks` entry (compared after `fold_blob_path` case-folding and Unicode NFD decomposition).
  `scan_workspace_source` (`crates/cli/src/blobs.rs`) records in-tree relative symbolic links in
  `manifest.symlinks()`, skips links whose target is non-UTF-8, absolute, leaves the tree root or
  traverses through another link (reporting each in `prepared.skipped`), and records empty
  non-ignored directories in `manifest.empty_dirs()`. `WorkspaceGuard::sync`
  (`crates/worker/src/workspace.rs`) checks case and Unicode NFC/NFD collisions via
  `fold_blob_path`, tracks `symlinks` and `empty_dirs` in `state.json`, removes stale tracked links
  and empty directories before creating new entries, creates empty directories and symbolic links
  after verifying non-symlink parents, and on Windows skips symbolic links when creation returns
  `ERROR_PRIVILEGE_NOT_HELD` (`1314`), recording `SyncReport { skipped_symlinks, warning }` (with a
  hint to enable Windows Developer Mode) forwarded to the task's `stderr`. Windows symlink tests
  skip cleanly when symlink creation returns OS error `1314` unless `RUSTY_GRID_REQUIRE_SYMLINKS=1`
  is set.
