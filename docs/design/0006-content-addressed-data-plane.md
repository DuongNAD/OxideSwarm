# 0006: Content-addressed task inputs and output files

Status: Implemented
Created: 2026-10-26    Updated: 2026-10-26

## Summary

- Task input and output files move as blobs named by their BLAKE3 hash, through a bounded on-disk
  store on the Master, instead of inside task and result messages.
- A peer first asks which blobs the Master lacks and sends only those. Workers keep a bounded cache
  and fetch only the blobs they lack, when a task needs them. Unchanged files are never sent twice.
- Directories travel as a manifest blob plus the blobs of their files. Outputs are chosen with glob
  patterns and capped in size. `RustCompilation` returns what it compiled.
- CLI: `submit --input LOCAL[:SANDBOX]` (file or directory), `--output PATTERN`, `--output-dir DIR`.

## Context

Behaviour before this change (design record 0003, inline staging):

| Area | Behaviour | Code |
|---|---|---|
| Inputs | `submit --file` read each file into `Task::staged_files` (SHA-256 and the bytes, inline). At most 32 MiB per task, files only. The whole payload crossed client → Master → worker on every submission. | `crates/cli/src/connect.rs` (`prepare_staged_files`), `crates/core/src/task.rs` |
| Worker cache | Staged files were kept by SHA-256 under `cache/objects/`, which only saved a disk write: the bytes had already arrived inline. Unbounded. | `crates/worker/src/sandbox.rs` (`stage_files`) |
| Outputs | `--output PATH` named exact paths, returned inline in `TaskResult::artifacts`: 10 MiB per file, 32 MiB per task. | `crates/worker/src/runner.rs` (`harvest_output_artifacts`) |
| Compilation | `RustCompilation` listed its artifacts in stdout and deleted them with the sandbox. | `execute_rust_compilation_workload` |
| Frames | `MAX_FRAME_SIZE` (64 MiB) bounds every message, so inline data cannot grow past it. | `crates/core/src/protocol.rs` |

## Goals

- **G1** A file or directory that did not change is not sent again: not from a client to the
  Master, not from the Master to a worker that already has it.
- **G2** Inputs and outputs of any size up to the store and cache bounds, in chunks far below the
  frame limit.
- **G3** Directories as inputs; outputs by glob pattern, with a size cap and a clear error.
- **G4** `RustCompilation` returns its artifacts.
- **G5** On QUIC, bulk transfer never delays heartbeats (data stream). On TCP it holds a heartbeat
  back by at most one chunk.
- **G6** Paths from peers cannot escape a sandbox or the client's output directory, and symbolic
  links are never followed.

## Non-goals

- Removing inline staging. `Task::staged_files`, `Task::expected_outputs` and
  `TaskResult::artifacts` stay as they are; the MCP server and library callers use them.
- Compression. zstd would make first uploads of source trees 2-4x smaller, but it adds a C
  dependency and CPU work on phones. Chunks carry no encoding field, so adding it is a wire change.
- Pipelined transfers from the CLI (one blob request in flight per connection), `.gitignore`-aware
  input selection, empty directories and symbolic links inside input trees.
- A blob store shared between Masters; collection beyond least-recently-used eviction.

## Design

### D1 Blob ids, manifests, inputs and patterns

`crates/core/src/blob.rs`:

- `BlobId` is the BLAKE3 hash of the content: 64 hex digits in JSON, 32 raw bytes in bincode.
- `TreeManifest` is a directory: its regular files as `BlobFile { path, id, size, executable }`,
  sorted by path. Its encoding (header `oxideswarm-tree/1\n`, then bincode) is itself a blob.
  `TreeManifest::decode` refuses unsorted lists, duplicates, a path that is both a file and the
  directory of another file, more than 500 000 files and more than 64 MiB.
- Paths are canonical and relative (`validate_blob_path`): no `.` or `..` segment, no leading `/`,
  no `\`, `:` or NUL. Rules of one platform (Windows device names) are checked where a file is
  created, so a Unix tree with `src/aux.rs` can still go to a Unix worker.
- `TaskInput { path, source }` places a `File { id, size, executable }` or a
  `Tree { manifest, manifest_size }` at a sandbox path; a tree may go to `.` (the sandbox root).
- `OutputPattern`: `*` and `?` match within a segment, a `**` segment matches any number of
  directories. A pattern selects the files it matches and every file inside a directory it matches.
  Absolute paths, `..` and `:` are refused.

### D2 Wire protocol

`crates/core/src/blob_transfer.rs` and `crates/core/src/protocol.rs`:

| Message | Direction | Notes |
|---|---|---|
| `ClientMessage::BlobRequest { request }` / `ClientResponse::BlobReply { reply }` | client ↔ Master | Answered in order on one connection. Appended last to both enums. |
| `WorkerMessage::BlobRequest { request_id, request }` / `MasterMessage::BlobReply { request_id, reply }` | worker ↔ Master | Appended after `UnknownVariant`, so existing variant indices do not move. A worker sends them only to a Master that announced `master_features::BLOBS`. |

| Request | Reply |
|---|---|
| `Missing { ids }` (at most 65 536) | `Missing { ids }`: the ids the Master lacks |
| `Read { id, offset, max_len }` | `Chunk { id, offset, total_size, data }`, or `NotFound { id }` |
| `Write { id, total_size, offset, data }` | `Written { id, received, complete }` |
| any | `Failed { message }` |

- Senders use chunks of 4 MiB (`BLOB_CHUNK_BYTES`); receivers refuse chunks over 8 MiB or past
  the end of the blob. A whole blob is hashed by its receiver and refused unless the hash is its id.
- An upload's chunks must arrive at increasing offsets on one connection; the first chunk of a
  blob the Master already holds is answered `complete` at once.
- New fields, with their `Human*` / `Binary*` mirrors: `Task::inputs`, `Task::output_globs`,
  `TaskResult::outputs` and `WorkerMessage::TaskResult::outputs`.
- `ProtocolCapabilities::BLOB_TRANSFER` (bit 9): workers advertise it; the scheduler places a task
  with inputs or output patterns only on a worker that has it (`TaskView::required_features`).
- `master_features::BLOBS` (`blobs`) is announced in `RegisterAck` only by a Master that runs a
  blob store.

### D3 Master blob store

`crates/master/src/blob_store.rs`, wired in `crates/master/src/server.rs`:

- Layout `<dir>/objects/<first two hex digits>/<hex id>`, uploads in progress in `<dir>/tmp/`
  (emptied at start-up). The directories are created, private to the owner, when the first blob
  arrives: a master that never stores a blob creates nothing in `~/.oxideswarm`. The CLI uses `~/.oxideswarm/blobs` and 4096 MiB unless
  `master --blob-dir` / `--blob-store-mb` (or `RUSTY_GRID_BLOB_DIR` / `RUSTY_GRID_BLOB_STORE_MB`,
  or `blob_dir` / `blob_store_mb` in the config file) say otherwise. A library `ServerConfig` has no
  store unless `with_blob_store` is called.
- A stored blob makes the store evict least recently used blobs down to its bound. Blobs of a task
  between admission and its terminal state are pinned and never evicted. A sweeper (every 30 s)
  releases the pins of finished tasks that no result released and drops uploads idle for 10 minutes.
- Admission (`admit_blob_task`): every input blob, and every file of every input tree, must be in
  the store with the announced size; they are pinned for the task. Otherwise the submission gets a
  `ClientResponse::Error` naming the input. A Master without a store refuses tasks with inputs or
  output patterns.
- A result whose outputs are not all in the store gets an error naming the first missing one.
- Each worker connection has a `BlobService` task that answers its requests in order, off the
  inbound loop. On QUIC the replies are routed to the data stream; client requests are answered
  inline on their own connection.

### D4 Worker cache, fetch and collection

`crates/worker/src/blob_cache.rs`, `blob_client.rs`, `staging.rs`, wired in `client.rs` and
`runner.rs`:

- Cache layout `<sandbox base>/cache/blobs/objects/...` (private to the owner), bound 2048 MiB (`worker --blob-cache-mb`,
  `RUSTY_GRID_BLOB_CACHE_MB`, config `blob_cache_mb`). Eviction (least recently used, unpinned)
  runs after each task that used blobs.
- Before the run the worker fetches what its cache lacks: manifests first, then their files,
  eight blobs at a time, one chunk request per blob in flight. Everything the task needs is pinned
  until the run ends. A cancellation stops the fetch.
- Inputs are copied into the sandbox, never hard-linked (a task that rewrites an input must not
  change the cached blob), hashed on the way, and created with `create_new`.
- After the run (not after a cancellation) the files the patterns select are collected: regular
  files only, symbolic links never followed, at most 1024 MiB (`worker --output-limit-mb`,
  `RUSTY_GRID_OUTPUT_LIMIT_MB`, config `output_limit_mb`) and 10 000 files. Exceeding a cap, or a
  pattern without wildcards that matched nothing in a run that succeeded, fails the task with a
  message that names the cause. Collected files move into the cache.
- The worker asks the Master which output blobs it lacks, uploads those, and only then sends the
  result. A failed upload fails the task.
- `RustCompilation` adds the pattern `<out dir>/*`, so `target/lib<crate>.rlib` (or the binary) is
  returned. With a Master that has no store these implicit outputs are dropped with a note in
  stderr; outputs the task asked for fail it.

### D5 CLI

`crates/cli/src/blobs.rs`, wired in `crates/cli/src/cli_app.rs`:

- `submit --input LOCAL[:SANDBOX]`, repeatable, `--file` is an alias. A file or a directory.
  Without `:SANDBOX` a relative local path keeps its path, anything else its last component, and
  `.` goes to the sandbox root; `LOCAL:.` places a directory's content at the root. Symbolic links,
  special files and names that are not portable relative paths are skipped with a warning.
- Inputs are hashed before connecting. Only missing blobs are uploaded, on a connection of their
  own; stderr reports how many were sent (`--json`: `input_upload`).
- `--output PATTERN`, repeatable, implies `--wait`. Outputs are downloaded into `--output-dir`
  (default: the current directory) at their sandbox paths: to a temporary file, checked against
  the blob id, then renamed. A destination path through a symbolic link is refused (`--json`:
  `saved_outputs`).

## Compatibility

- `Task` and `TaskResult` gained fields and bincode is positional, so every component must run the
  same build. Restart the Master first, then workers and clients (as in `DEPLOYMENT.md` 3.5).
- A Master built before this change cannot decode `ClientMessage::BlobRequest` and closes the
  connection; the CLI then says the Master may be older. A worker never sends
  `WorkerMessage::BlobRequest` to a Master that did not announce `blobs`.
- Inline staging is unchanged, but the CLI's `--file` now uses blobs, so it needs a Master with a
  blob store (every `oxideswarm master` has one unless it has no home directory and no
  `--blob-dir`).

## Security

- The store and the worker cache are directories private to their owner (mode 0700 on Unix).
- The store is reached only over authenticated connections (cluster secret). Uploads are verified
  against their ids, chunk sizes are checked before anything is written, and files are named after
  hashes, never after anything a peer sent.
- Manifests and output patterns are validated on receipt; sandbox paths are checked again with the
  platform's rules; input files are created with `create_new`; output collection never follows
  symbolic links; client downloads never write through a symbolic link.
- Bounds: store and cache size, output size and count, 65 536 ids per `Missing`, 500 000 files and
  64 MiB per manifest, 1 000 000 directory entries per collection walk, idle uploads dropped after
  10 minutes.
- Tasks still run as the worker's user (`docs/SECURITY.md`), so a task can read whatever that user
  can; this change adds no new way for it to do so.

## Test plan

- **Unit:** `crates/core/src/blob.rs` (ids, manifests, paths, patterns),
  `crates/core/src/blob_transfer.rs` (chunking, verification, early stop),
  `crates/master/src/blob_store.rs` (verification, restart, eviction, pins, input resolution),
  `crates/worker/src/blob_cache.rs` (copies, corruption, eviction), `crates/worker/src/staging.rs`
  (placement, symbolic links, caps), `crates/worker/src/blob_client.rs` (fetch once, upload),
  `crates/cli/src/blobs.rs` (input specs, directory scans, safe downloads).
- **Integration:** `tests/blob_transfer_e2e.rs` (`cargo test -p rusty_grid_cli --test
  blob_transfer_e2e`): a directory round trip with a multi-chunk file, a second submission that
  uploads and fetches nothing, a `RustCompilation` artifact, refusals, and the same round trip
  through the `rusty-grid` binary.
- **Gates:** `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Blob model and transfer routines | `crates/core/src/blob.rs`, `crates/core/src/blob_transfer.rs` | Unit tests pass | Implemented |
| W2 | Protocol fields and messages | `crates/core/src/protocol.rs`, `crates/core/src/task.rs` | Both codecs round-trip | Implemented |
| W3 | Master blob store, admission, scheduling | `crates/master/src/blob_store.rs`, `server.rs`, `queue.rs`, `scheduler.rs` | Unit tests pass | Implemented |
| W4 | Worker cache, fetch, inputs, outputs, compilation artifacts | `crates/worker/src/blob_cache.rs`, `blob_client.rs`, `staging.rs`, `client.rs`, `runner.rs` | Unit tests pass | Implemented |
| W5 | CLI options | `crates/cli/src/blobs.rs`, `cli_app.rs`, `config.rs` | E2E round trip passes | Implemented |
| W6 | End-to-end tests | `tests/blob_transfer_e2e.rs` | Round trip and second-submission checks pass | Implemented |

## Status log

- 2026-10-26: Created and implemented on branch `claude/dreamy-kilby-6adcea`; unit and
  `blob_transfer_e2e` tests pass.
