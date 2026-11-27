# 0109: Live output streaming, `logs`, and MCP tasks (`AX-3`)

Status: Draft
Created: 2026-11-24    Updated: 2026-11-27

## Summary

Add **opt-in** per-task live `stdout`/`stderr` streaming from executing workers to subscribed CLI and MCP clients, a `crates/master` bounded per-task output ring buffer (`TaskOutputBroker`) backing `oxideswarm submit --wait --follow` and `oxideswarm logs <task>`, and MCP `2026-07-28` tasks-extension negotiation (`io.modelcontextprotocol/tasks`) alongside existing `2024-11-05` support. Per owner decision **Q101** (`2026-11-26`, [ROADMAP.md](../ROADMAP.md) / [design 0111](0111-team-plan-leases-and-verify-gate.md)), implementation targets **`0.5.0`** on the **protocol `11`** release train ([design 0076](0076-protocol-compatibility-and-release-train.md)) with zero wire changes or implementation code merged during `0.4.x`, and waits on owner approval of open decision **Q55**.

## Context

- Today, a client receives a task's output only inside `WorkerMessage::TaskResult` (`crates/core/src/protocol.rs`) once execution finishes, while `WorkerMessage::TaskProgress` carries only `TaskStatus`.
- Worker process supervision (`crates/worker/src/runner.rs` and `crates/worker/src/runner/capture.rs`) collects child `stdout` and `stderr` with `StreamCapture` up to `DEFAULT_MAX_OUTPUT_BYTES` (`10 MiB` per stream) using head-and-tail capture and midpoint truncation ([design 0022](0022-head-and-tail-stream-capture.md)), keeping child stdio piped with `CREATE_NO_WINDOW` on Windows ([design 0108](0108-windows-tasks-without-console-windows.md), `TEST-88`).
- `crates/core/src/lib.rs` enforces the twin-mirror rule across `crates/core/src/protocol/{worker_master_mirrors,client_msg_mirrors,client_resp_mirrors}.rs` (`Human*` and `Binary*` mirrors plus all four `From` conversions per public enum), and `crates/core/src/wire_layout_tests.rs` + `crates/core/src/window_tests.rs` lock positional bincode variant indices against `crates/core/tests/data/wire_layout.txt` and `wire_layout_previous.txt`.
- [Design 0082](0082-task-extension-slot.md) (`Task::extensions`, `EXTENSION_REGISTRY` in `crates/core/src/task/extension.rs`) reserves extensible per-task flags without adding new positional fields to `Task`.
- `MCP_PROTOCOL_VERSION` in `crates/cli/src/mcp.rs` is `"2024-11-05"`, tested end-to-end by `tests/mcp_server_e2e.rs`. The CLI (`crates/cli/src/cli_app.rs` / `crates/cli/src/cli_app/submit.rs`) has `--wait: bool` (no `--detach` flag) and no `--follow` flag or `logs` subcommand.
- Under **Q101** (`2026-11-26`), `0.5.0` carries coding agents (`AX-1`..`AX-3`, `CACHE-1`, and `TEAM-1`..`TEAM-7`) on **protocol `11`**, while language-model serving moves to `0.6.0`. [Design 0064](0064-research-of-2026-11-13-and-route.md) (`D2`) raises **Q55** because two coding agents used by the owner do not yet implement the MCP `2026-07-28` tasks extension.

## Goals

- G1. Stream opt-in live `stdout` and `stderr` chunks from workers to subscribed CLI and MCP clients while a task is still running, enabling early inspection and cancellation (`cancel`) before completion, without adding streaming overhead to unfollowed batch/matrix/sharded tasks.
- G2. Preserve [design 0022](0022-head-and-tail-stream-capture.md) final `TaskResult` head-and-tail output byte-for-byte regardless of live streaming or subscriber backpressure, and never let chatty live output delay `TaskResult`, `Checkpoint`, or `BlobRequest` on QUIC `STREAM_DATA`.
- G3. Enforce bounded worker queues and bounded master memory (`per-task` and global ring buffers, coupled with `TaskQueue` eviction) so chatty tasks or slow subscribers can never OOM a worker/master or stall child pipe draining.
- G4. Upgrade the MCP server to support `2026-07-28` (`io.modelcontextprotocol/tasks`) while keeping `"2024-11-05"` clients (`tests/mcp_server_e2e.rs`) working unmodified.
- G5. Ship cleanly on the **`0.5.0` protocol `11`** release train following [design 0076](0076-protocol-compatibility-and-release-train.md), [design 0082](0082-task-extension-slot.md), and [design 0083](0083-one-version-window.md) (`PROTOCOL_VERSION_MIN_COMPATIBLE = 10`).

## Non-goals

- Merging `AX-3` code or bumping `PROTOCOL_VERSION_CURRENT` in `0.4.x` (this record stays Draft until `0.5.0` / protocol `11` opens).
- Guaranteeing lossless unbounded live output retention across arbitrary disconnections (`TaskResult` head/tail remains the bounded durable record; live buffers intentionally drop/overwrite oldest chunks under congestion or memory pressure).
- Streaming binary output artifacts or interactive stdin / PTY allocation (`--attach` / ConPTY) over the grid wire protocol (`CREATE_NO_WINDOW` piped stdio stays preserved).
- Breaking compatibility with the one-version window (`PROTOCOL_VERSION_MIN_COMPATIBLE = 10`).

## Design

### D1: Per-task opt-in extension, wire format, mirrors, capability bit, and protocol `11` train

1. **Per-task opt-in via [design 0082](0082-task-extension-slot.md) `Task::extensions` (`crates/core/src/task/extension.rs`):**
   - Streaming is **opt-in per task** (default **off** so unfollowed batch, matrix, and sharded jobs pay zero chunk-framing or broker overhead).
   - Register one non-critical worker-scoped task extension in `EXTENSION_REGISTRY` (`crates/core/src/task/extension.rs`):
     - `id`: next assigned `1..=0x7FFF` id on the protocol `11` train (`1`, unless claimed earlier by another protocol `11` extension).
     - `name`: `"live-output"`
     - `list`: `ExtensionList::Task`
     - `critical`: `false` (non-critical: older v10 workers simply omit live streaming and `Task::for_peer(10)` round-trips stay clean)
     - `scope`: `ExtensionScope::Worker`
     - `decode`: accepts strictly `[0x01]` (`bytes == &[1]`); rejects empty or multi-byte payloads.
   - `submit --follow` (and MCP long-running calls / explicit follow requests) attach the `"live-output"` extension (`bytes: vec![0x01]`) to `Task::extensions`.
   - Workers emit `TaskOutputChunk` **only** when **both** conditions hold: (a) the accepted session negotiated `ProtocolCapabilities::LIVE_OUTPUT` on `RegisterAck`, **and** (b) the assigned `Task` carries `"live-output"`.
2. **New stream discriminator (`crates/core/src/protocol.rs`):**
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
   pub enum OutputStream {
       Stdout,
       Stderr,
   }
   ```
3. **Additive tail variants on protocol `11` (appended after all existing variants, coordinated across the protocol `11` train — PR `#386`, `TEAM-6`, and `AX-3` — so positional bincode indices never collide):**
   - `WorkerMessage` (currently ending at `ResumeSession` in `v10`):
     ```rust
     /// Incremental live stdout/stderr chunk emitted by a worker during task execution (`AX-3`).
     TaskOutputChunk {
         worker_id: Uuid,
         task_id: TaskId,
         stream: OutputStream,
         seq: u64,
         bytes: TaskBytes,
     },
     ```
     Update `WorkerMessage::name()` and `WorkerMessage::worker_id()` (`crates/core/src/protocol.rs`) to handle `TaskOutputChunk`.
   - `ClientMessage` (currently ending at `ReleaseWorkerId` in `v10`):
     ```rust
     /// Request buffered and optional live output chunks for a running or recently finished task.
     SubscribeTaskOutput {
         task_id: TaskId,
         after_seq: Option<u64>,
         follow: bool,
     },
     ```
   - `ClientResponse` (currently ending at `WorkerIdReleased` in `v10`):
     ```rust
     /// One buffered or live chunk in reply to `ClientMessage::SubscribeTaskOutput`.
     TaskOutputChunk {
         task_id: TaskId,
         stream: OutputStream,
         /// Master-monotonic broker sequence across task retries/failover (1, 2, ...).
         seq: u64,
         /// Attempt-scoped retry epoch starting at 1 so clients can render attempt transitions.
         attempt: u32,
         dropped_bytes: u64,
         bytes: TaskBytes,
     },
     /// Marks the end of a `SubscribeTaskOutput` stream (replay finished or task reached terminal state).
     TaskOutputEnd {
         task_id: TaskId,
         dropped_bytes: u64,
         terminal_status: Option<TaskStatus>,
     },
     ```
4. **Mirror enums, `protocol_version`, and `From` implementations (`crates/core/src/lib.rs` & [design 0083](0083-one-version-window.md)):**
   - Append the identical variants at the exact tail indices of BOTH `Human*` (`serde` JSON) and `Binary*` (positional bincode) mirror enums across:
     - `crates/core/src/protocol/worker_master_mirrors.rs` (`HumanWorkerMessage` / `BinaryWorkerMessage` + all four `From` impls)
     - `crates/core/src/protocol/client_msg_mirrors.rs` (`HumanClientMessage` / `BinaryClientMessage` + all four `From` impls + `protocol_version: u32` field and `from_client_message` / `protocol_version()` match arms)
     - `crates/core/src/protocol/client_resp_mirrors.rs` (`HumanClientResponse` / `BinaryClientResponse` + all four `From` impls + `protocol_version: u32` field and `from_client_response` / `protocol_version()` / `is_error()` match arms)
5. **Capability bit (`crates/core/src/protocol.rs`):**
   - Allocate the next unused flag bit after `TREE_V2` (`1 << 13`) — checked against the open protocol `11` draft branches (`#386`, `#487` `TEAM-6`, and `#412` `CACHE-1`), all of which stop at `TREE_V2 = 1 << 13`:
     ```rust
     /// Live task stdout/stderr chunk streaming (`WorkerMessage::TaskOutputChunk`, `AX-3`).
     pub const LIVE_OUTPUT: Self = Self(1 << 14);
     ```
   - Include `Self::LIVE_OUTPUT.0` in `ProtocolCapabilities::all_supported()`.
6. **Protocol version `11` & post-decode chunk bound:**
   - Bump `PROTOCOL_VERSION_CURRENT` from `10` to **`11`** on the shared `0.5.0` release train (alongside `TEAM-6` / any protocol `11` train items) with `PROTOCOL_VERSION_MIN_COMPATIBLE = 10` (`PREVIOUS = 10` in `crates/core/src/window_tests.rs`).
   - Bound each chunk payload at `MAX_LIVE_OUTPUT_CHUNK_BYTES = 16 * 1024` (`16 KiB`), well below `MAX_FRAME_SIZE`. As documented in [design 0082](0082-task-extension-slot.md) (`S1`), length bounds inside `TaskBytes` are checked **post-decode / pre-buffer** immediately after bincode/JSON frame decode: any worker frame with `bytes.len() > MAX_LIVE_OUTPUT_CHUNK_BYTES` is rejected as a protocol violation that drops/closes the offending worker session (and logs at `warn!`).

### D2: Worker live chunk tee & QUIC `STREAM_DATA` head-of-line isolation

- Inside `crates/worker/src/runner/capture.rs`, the pipe-reader loop that feeds `StreamCapture` (`DEFAULT_MAX_OUTPUT_BYTES = 10 MiB` head/tail with midpoint notice per [design 0022](0022-head-and-tail-stream-capture.md)) adds an optional non-blocking live-chunk tee sink (`Option<LiveOutputSink>`), enabled **only** when both `LIVE_OUTPUT` is negotiated and `"live-output"` is present in `Task::extensions`.
- For each stdout and stderr read:
  1. Update `StreamCapture` synchronously first so final `TaskResult.stdout` and `TaskResult.stderr` remain byte-for-byte invariant.
  2. Append bytes to a per-stream `16 KiB` staging buffer, flushing to a dedicated per-task bounded channel (`LIVE_QUEUE_CAPACITY = 64` chunks, max `1 MiB` queued per task) whenever a newline (`\n`) arrives, `staging.len() >= 16 KiB`, or `LIVE_FLUSH_INTERVAL = 100 ms` elapses.
  3. **Non-blocking tee:** the pipe reader always uses `try_send` — never `.await` — so backpressure never stalls child pipe draining.
- **QUIC lane pinning & head-of-line isolation (`crates/worker/src/client/session.rs` & `crates/master/src/session.rs`):**
  - Keep `WorkerMessage::TaskOutputChunk` **off** `Session::data_tx` (`DATA_QUEUE_CAPACITY = 64`, reserved for `TaskProgress`, `TaskResult`, `Checkpoint`, and `BlobRequest`).
  - Route `TaskOutputChunk` into a **separate** bounded low-priority live-output channel on the worker session (`LIVE_SESSION_QUEUE_CAPACITY = 64` chunks) via `try_send` (dropping/coalescing oldest or excess live chunks immediately when full).
  - In the worker's `STREAM_DATA` writer task (`crates/worker/src/client/session.rs`), poll `data_rx` and `live_rx` using `tokio::select! { biased; ... }` so `data_rx` (`TaskResult`, `Checkpoint`, `BlobRequest`, `TaskProgress`) **always** takes strict priority over `live_rx`. On congested or slow relayed QUIC links (e.g. `~0.12 MB/s`), `live_rx` drops/coalesces excess live frames instead of filling `data_tx` or QUIC stream flow-control windows.
  - At task completion, drain only immediately-writable `live_rx` chunks within at most `<= 50 ms`; drop any remaining unsent live chunks immediately rather than holding back `WorkerMessage::TaskResult`.
  - On the master side, `WorkerSession::handle_inbound_message` (`crates/master/src/session.rs`) accepts `WorkerMessage::TaskOutputChunk` **strictly on `LaneKind::Unified | LaneKind::Data`** (never `LaneKind::Control` / heartbeats), and ingests it into `TaskOutputBroker` in O(1) without `.await`ing slow client subscribers and without touching `PendingDataBuffer` / `DATA_LANE_WRITE_TIMEOUT`.
- **Note on line endings & Windows pipe block-buffering:**
  - `\r\n` flushes cleanly on `\n`, and `\r`-only progress lines rely on `LIVE_FLUSH_INTERVAL = 100 ms`.
  - Because Windows worker tasks run with piped stdio and `CREATE_NO_WINDOW` (no console/PTY per [design 0108](0108-windows-tasks-without-console-windows.md)), MSVC CRT `stdio`, Python, and Rust `BufWriter` block-buffer stdout (typically `4 KiB`) on pipes. Document this explicitly in `docs/reference.md` and ensure the `AX-3` E2E test flushes stdout after line 1 so Windows does not block-buffer until process exit.

### D3: Master bounded output broker, `TaskQueue` budget coupling, and retry monotonicity

- Introduce a `TaskOutputBroker` (owned alongside `TaskQueue` / `WaiterMap` in `crates/master`) storing chunks as zero-copy `Bytes` / `TaskBytes` (`Bytes::clone` to subscribers so fan-out never copies payload buffers):
  - **Per-task buffer cap:** `256 KiB` (`DEFAULT_TASK_LIVE_BUFFER_BYTES`). Evicts oldest chunks from the front and accumulates `dropped_bytes: u64`.
  - **Global broker budget (`32 MiB`, additive on top of `TaskQueue` caps):** `DEFAULT_GLOBAL_LIVE_BUFFER_BYTES = 32 MiB`, tunable via CLI flag `--max-live-output-mb <N>` / env `OXIDESWARM_MAX_LIVE_OUTPUT_MB` (`0` disables live buffering; documented in `docs/reference.md`). This `32 MiB` budget is accounted explicitly **on top of** `TaskQueue`'s `DEFAULT_MAX_ACTIVE_BYTES = 256 MiB` (`--max-active-mb`) and `DEFAULT_MAX_TERMINAL_BYTES = 256 MiB` (`--max-retained-mb`) + `DEFAULT_MAX_HOT_TERMINAL_BYTES = 4 MiB` cold spill (`crates/master/src/cold_results.rs`). Under pressure, evict terminal task buffers first, then least-recently-updated active buffers.
  - **Per-task & global subscriber caps:** cap at `16` concurrent subscribers per task, **`256` active subscribers globally** (`DEFAULT_MAX_GLOBAL_LOG_SUBSCRIBERS`), and `64` queued frames per subscriber (`mpsc::Sender::try_send`, zero-copy `Bytes::clone`), dropping/closing lagged slow subscribers without blocking `TaskQueue` or `WorkerSession`.
  - **Coupled terminal lifecycle & `TaskQueue` eviction:** when a task reaches a terminal state, `TaskOutputBroker` broadcasts `TaskOutputEnd`, retains the live buffer for at most a brief `30 s` post-completion grace window (so racing `oxideswarm logs` handoffs still work without duplicating `256 MiB` terminal `TaskQueue` retention), **and** `TaskQueue` terminal eviction (`--max-retained-mb` / `--max-retained-tasks`) calls `TaskOutputBroker::remove(task_id)` immediately so a live buffer can never outlive its `TaskQueue` entry. Once released, late `oxideswarm logs <task>` falls back transparently to `GetTaskResult`.
- **Master-monotonic `broker_seq` across retries / failover:**
  - Worker `seq` (`1, 2, ...` per attempt) is validated only for per-attempt monotonic drop/gap detection; `TaskOutputBroker` assigns a single master-monotonic `broker_seq` (`1, 2, ...`) per `TaskId` across all execution attempts (`attempt: 1, 2, ...`), incrementing cleanly even if attempt `1` disconnects and attempt `2` starts on another worker from `worker_seq = 1`.
  - `SubscribeTaskOutput { task_id, after_seq, follow }` filters on `chunk.seq > after_seq.unwrap_or(0)` using `broker_seq`, so attempt `2` output is never dropped on failover/retry. When `attempt` increments, CLI `--follow` / `logs` may optionally emit a short boundary notice to `stderr`.
- **Master session auth, connection lifecycle & RBAC:**
  - `WorkerSession` authorizes every `WorkerMessage::TaskOutputChunk` strictly from the TLS/OXS-AUTH-verified session `(worker_id, session_id)` owning `task_id`; mismatched or post-terminal chunks are dropped (`warn!` on claimed mismatch).
  - Under OXS-AUTH v3, add `ClientMessage::SubscribeTaskOutput { .. }` to the `member.has_client_role()` match arm of `check_v3_client_message_role` (`crates/master/src/server/handshakes.rs`), matching `GetTaskStatus` and `GetTaskResult` (cluster-scoped client authorization; OxideSwarm has no per-submitter task ACL today).
  - In `handle_client_connection` (`crates/master/src/server/client_and_members.rs`), `ClientMessage::SubscribeTaskOutput` transitions that dedicated client connection into a multi-frame streaming loop sending `ClientResponse::TaskOutputChunk*` -> `ClientResponse::TaskOutputEnd` and then closing cleanly (or terminating early on client disconnect, write timeout, `shutdown_rx`, or OXS-AUTH v3 `MemberConnections` revocation disconnect per `handshakes.rs`). `ClientMessage::SubmitTask { wait: true }` retains its existing single-response semantics on its own connection.

### D4: CLI (`submit --wait --follow` and `oxideswarm logs <task>`)

- **Flags & `clap` validation (`crates/cli/src/cli_app.rs` & `crates/cli/src/cli_app/submit.rs`):**
  - `SubmitArgs` adds `--follow: bool`, which implies `--wait` (matching `--matrix` in [design 0101](0101-matrix-fanout.md) D1) and conflicts with `--matrix` and `--verify` (rejected by `clap` with exit code `2`). Note `SubmitArgs` has `--wait: bool` and no `--detach` flag.
- **Race-free streaming submission flow:**
  - When `--follow` is active, CLI attaches `ExtensionInfo` `"live-output"` (`[0x01]`), issues `ClientMessage::SubmitTask { wait: false, .. }` **first** to obtain `ClientResponse::TaskSubmitted { task_id }`, then opens the dedicated companion `SubscribeTaskOutput { task_id, after_seq: None, follow: true }` stream while waiting for completion / `GetTaskResult` (and downloading `--output` files). This eliminates any client-side `task_id` admission race against `SubscribeTaskOutput`.
- **Stream formatting & UTF-8 rules:**
  - In raw mode, write `OutputStream::Stdout` bytes verbatim to local `stdout` and `OutputStream::Stderr` bytes verbatim to local `stderr` (emitting at most one `stderr` notice if `dropped_bytes > 0` or `attempt > 1`). Avoid re-printing `TaskResult` raw `stdout`/`stderr` at exit if streaming completed cleanly without drops.
  - In `--json` mode and MCP JSON-RPC text payloads, decode non-UTF-8 sequences / split multi-byte sequences at `16 KiB` chunk boundaries using `String::from_utf8_lossy` (buffering trailing incomplete UTF-8 sequences `<= 3` bytes until the next chunk or stream end) rather than failing JSON serialization.
  - Document `Ctrl+C` semantics cleanly: `Ctrl+C` during `oxideswarm submit --wait --follow` sends best-effort `CancelTask` (matching `oxideswarm shard` in `crates/cli/src/cli_app/shard.rs`) and exits `130`, whereas `Ctrl+C` during `oxideswarm logs -f <task>` detaches locally and exits `130` without cancelling the remote task.
- **`oxideswarm logs <TASK_ID>` (`crates/cli/src/cli_app/tasks.rs`):**
  - Flags: `<TASK_ID>`, `--follow` / `-f`, `--tail <LINES>` (last `N` buffered lines), `--tail-bytes <BYTES>` (last `N` buffered bytes; mutually exclusive with `--tail`, failing via `clap` with exit code `2`), and `--json`.
  - Streams via `SubscribeTaskOutput` and falls back transparently to `GetTaskResult` when a task ran on a `v10` worker, omitted `"live-output"`, or aged past live broker eviction.

### D5: MCP `2026-07-28` (`io.modelcontextprotocol/tasks`) and `2024-11-05` / `Q55` Fallback (`crates/cli/src/mcp.rs`)

- **Protocol negotiation (`initialize`):**
  - Support `SUPPORTED_MCP_VERSIONS = ["2026-07-28", "2024-11-05"]` (`LEGACY_MCP_PROTOCOL_VERSION = "2024-11-05"`).
  - Clients negotiating `"2026-07-28"` with `"capabilities": { "tasks": ... }` (`io.modelcontextprotocol/tasks`) receive `"protocolVersion": "2026-07-28"` and `"capabilities": { "tools": {}, "tasks": { ... } }`, creating MCP tasks (`tasks/get`, `tasks/result`, `tasks/cancel`, `tasks/list`) backed by `TaskOutputBroker` + `TaskId`.
  - `"2024-11-05"` (and non-tasks) clients receive `"protocolVersion": "2024-11-05"` and `"capabilities": { "tools": {} }`, keeping `tests/mcp_server_e2e.rs` unmodified and green.
- **Actionable `Q55` fallback defaults ([design 0064](0064-research-of-2026-11-13-and-route.md) D2):**
  - Recommend **Yes** for agents lacking the `2026-07-28` tasks extension, with concrete defaults:
    - Bounded `wait_ms` on `submit` (default `30_000` ms when `wait: true`, max `120_000` ms); if still active when the slice expires, return `{ task_id, status: "running", tail }` (latest `4 KiB` / `80`-line lossy UTF-8 tail excerpt from `TaskOutputBroker`) so agent tool calls never hit short MCP client timeouts.
    - Polling via `task_result { task_id, wait_ms }` (returning incremental `tail` while running, final bounded result on completion), paired with `cancel { task_id }` killing the worker process tree.

## Compatibility

- **Protocol `11` Release Train ([design 0076](0076-protocol-compatibility-and-release-train.md) D1, Q101):**
  - Ships on **`0.5.0`** (`PROTOCOL_VERSION_CURRENT = 11`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 10`). Implementation PR states `"wire change"` (protocol `11`), shares/coordinates tail variant indices across any protocol `11` changes (`PR #386`, `TEAM-6`, `AX-3`), passes `scripts/release_skew.py` (`CI/release skew`) against `0.4.x` (`v10`), and follows the 48-hour release train rule.
- **Clean [design 0083](0083-one-version-window.md) checklist:**
  - Gate worker live-output emission strictly on `negotiated.contains(ProtocolCapabilities::LIVE_OUTPUT)` (`RegisterAck.features` bitmask AND `RegisterAck.protocol_version >= 11`) plus the task's `"live-output"` `Task::extensions` entry (do **not** conflate with `master_features::announced(...)` in `RegisterAck.message`). Note that `is_master_echo_acceptable` (`crates/core/src/protocol.rs`, `crates/worker/src/client/session.rs`) already enforces [design 0083](0083-one-version-window.md) D5 during handshake.
  - Because `PROTOCOL_VERSION_MIN_COMPATIBLE = 10` (`v10` worker/client <-> `v11` master):
    - Older `v10` worker + `v11` master: `LIVE_OUTPUT` bit unset + non-critical `"live-output"` extension ignored cleanly; `TaskResult` returns normally via `GetTaskResult`.
    - Older `v10` client + `v11` master: never sends `SubscribeTaskOutput`; master rejects/forbids emitting `TaskOutputChunk`/`TaskOutputEnd` unless solicited on a `v11` `SubscribeTaskOutput` stream.
  - Implementation updates all Design `0083` artifacts:
    - `crates/core/tests/data/wire_layout.txt` (`v11`)
    - `crates/core/tests/data/wire_layout_previous.txt` (`v10`, checked by `verify_previous_layout_file`)
    - `crates/core/src/window_tests.rs` (`PREVIOUS = 10` and `DIFFERS_FROM_PREVIOUS`)
    - `crates/core/src/protocol/{worker_master_mirrors,client_msg_mirrors,client_resp_mirrors}.rs` (mirrors, `From`, `protocol_version`, `is_error`).
- **On-disk store (`crates/master/src/store.rs`):** `STORE_FORMAT_VERSION` unchanged (`TaskOutputBroker` is intentionally in-memory/ephemeral).

## Security

- **Peer identity & spoofing:** `WorkerSession` authorizes `WorkerMessage::TaskOutputChunk` strictly from the TLS/OXS-AUTH-verified `(worker_id, session_id)` owning `task_id`; body `worker_id` values from another worker or stale session are ignored (`warn!`).
- **OXS-AUTH v3 RBAC & revocation:** `SubscribeTaskOutput` uses the same cluster-scoped `member.has_client_role()` check in `check_v3_client_message_role` (`crates/master/src/server/handshakes.rs`) as `GetTaskStatus` and `GetTaskResult` (no per-submitter task ACL exists today), and active `SubscribeTaskOutput` streams terminate immediately on `MemberConnections` revocation.
- **Exhaustion defenses:** Post-decode / pre-buffer `MAX_LIVE_OUTPUT_CHUNK_BYTES = 16 KiB` (session closed on violation), `256 KiB` per-task ring cap, `32 MiB` global broker budget coupled to `TaskQueue` eviction, `16`/task + `256` global subscriber caps with `64`-depth zero-copy `try_send`, `LIVE_QUEUE_CAPACITY = 64`, and biased writer prioritization preserving `TaskResult`/`Checkpoint`/`BlobRequest`.
- **MCP/JSON hygiene & Windows console rules:** `CREATE_NO_WINDOW` stays enforced on all Windows worker/E2E subprocesses ([design 0108](0108-windows-tasks-without-console-windows.md)), raw bytes stay strictly out of MCP stdout (`lossy UTF-8` JSON-RPC escaping), and `truncate_stream_for_llm` bounds tail excerpts.

## Test plan

1. **Core protocol, extensions & window gates (`crates/core`):**
   - `Task::extensions` `"live-output"` registration & `decode` validation (`[0x01]` valid, invalid lengths/bytes rejected) + `Task::for_peer(10)` round-trip cleanliness.
   - `WireCodec::Bincode` & `WireCodec::Json` round-trips for `TaskOutputChunk`, `SubscribeTaskOutput`, `TaskOutputEnd`, all `From` impls, `window_tests.rs` (`PREVIOUS = 10`, `verify_previous_layout_file`), `wire_layout_tests.rs`, and `CI/release skew` against `0.4.x`.
2. **Worker tee, QUIC priority isolation & Design 0022 invariance (`crates/worker`):**
   - Streaming > `DEFAULT_MAX_OUTPUT_BYTES` matches `StreamCapture` head+tail byte-for-byte; full `live_rx` drops live chunks without blocking pipe EOF or delaying `TaskResult` behind congested `STREAM_DATA`.
3. **Master broker, retry monotonicity, eviction & RBAC (`crates/master`):**
   - `256 KiB` wraparound + `dropped_bytes`, `32 MiB` global eviction, `TaskQueue` terminal eviction coupling, master-monotonic `broker_seq` across task retries (`attempt` 1 -> 2), spoofed `WorkerSession` rejection, `check_v3_client_message_role`, and `MemberConnections` revocation disconnect.
4. **End-to-end CLI & MCP (`tests/` / `crates/cli/tests/`):**
   - Live streaming E2E (flushing stdout line 1 on Windows with `CREATE_NO_WINDOW`) proving `submit --wait --follow` and `oxideswarm logs <task> --follow` observe line 1 before task exit, plus `v10` worker fallback.
   - MCP `2026-07-28` (`io.modelcontextprotocol/tasks`) + `Q55` `wait_ms` tail test, plus unmodified `tests/mcp_server_e2e.rs` (`2024-11-05`) green.
   - `miri` on `rusty_grid_core`, `cargo fmt`, `clippy -D warnings`, `ox-mode test size`, `test_m1_challenger_hygiene.py`.

## Work items

Implementation targets **`0.5.0` (protocol `11`, Q101)** after `0.4.6`–`0.4.9` and owner approval of `Q55`:

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AX-3a | Extension `"live-output"`, wire types, mirrors, `LIVE_OUTPUT`, protocol `11` & window tests | `crates/core/src/task/extension.rs`, `crates/core/src/protocol.rs`, `crates/core/src/protocol/{worker_master_mirrors,client_msg_mirrors,client_resp_mirrors,codec,version_and_aead_tests}.rs`, `crates/core/src/window_tests.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/{wire_layout,wire_layout_previous}.txt` | All `From` impls, extension decode, `window_tests`, `wire_layout_tests`, and `release skew` pass against `v10` | Planned |
| AX-3b | Worker opt-in live tee & biased writer isolation off `Session::data_tx` | `crates/worker/src/runner/capture.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client/session.rs` | Live chunks emit only with extension + capability; `TaskResult` never blocked by slow `STREAM_DATA` | Planned |
| AX-3c | Master `TaskOutputBroker`, `broker_seq` retries, `TaskQueue` eviction coupling & OXS-AUTH v3 RBAC | `crates/master/src/session.rs`, `crates/master/src/queue.rs`, `crates/master/src/server/{client_and_members,handshakes}.rs`, `crates/master/src/lib.rs` | `SubscribeTaskOutput` streams + revokes cleanly; `32 MiB` cap coupled to `TaskQueue`; retries keep monotonic `broker_seq` | Planned |
| AX-3d | CLI `submit --wait --follow`, `oxideswarm logs <task>`, docs & deployment notes | `crates/cli/src/cli_app.rs`, `crates/cli/src/cli_app/{submit,tasks}.rs`, `docs/reference.md`, `DEPLOYMENT.md` | E2E sees line 1 before exit across macOS/Linux/Windows; `docs/reference.md` documents `--max-live-output-mb` & pipe buffering | Planned |
| AX-3e | MCP `2026-07-28` (`io.modelcontextprotocol/tasks`) + `2024-11-05` / `Q55` fallback | `crates/cli/src/mcp.rs`, `crates/cli/src/mcp/{submit,task_result}.rs`, `tests/mcp_server_e2e.rs`, `docs/reference.md` | `2026-07-28` client polls running task tail; `Q55` bounded wait works; `2024-11-05` suite stays green | Planned |

## Decisions for the owner

The right column is a recommended default from [design 0064](0064-research-of-2026-11-13-and-route.md) and stays **Open** for owner approval before implementation starts:

| ID | Question | Recommended default | Status |
|---|---|---|---|
| **Q55** | Should `AX-3` also include a long-running MCP call flow for agents that do not support the `2026-07-28` tasks extension (bounded `wait_ms` `submit` defaulting to `30_000` ms / max `120_000` ms returning `{ task_id, status: "running", tail }` with a `4 KiB` / `80`-line tail excerpt, paired with `task_result` polling and process-tree `cancel`), while using `io.modelcontextprotocol/tasks` automatically whenever a client declares it? | **Yes** ([design 0064](0064-research-of-2026-11-13-and-route.md) D2) | Open |

## Status log

- 2026-11-24: Draft design record `0109` written (`E-M4`, docs only — zero code merged in `0.4.x`); `Q55` remains open.
- 2026-11-26: Updated against `origin/master` (`034f5725`) and `agent-win` reviews (`2026-11-25` & `2026-11-26`): targeted `0.5.0` / protocol `11` (`Q101`), specified opt-in [design 0082](0082-task-extension-slot.md) `"live-output"` task extension, `Session::data_tx` isolation with `biased` writer priority, `32 MiB` broker accounting coupled with `TaskQueue` eviction, master-monotonic `broker_seq` across retries, `SubscribeTaskOutput` OXS-AUTH v3 lifecycle, [design 0083](0083-one-version-window.md) mirror/previous-layout checklist, Windows pipe-buffering / UTF-8 / `Ctrl+C` semantics, and concrete `Q55` defaults.
- 2026-11-27: Merged `origin/master` (`07caa5ae`), verified `ProtocolCapabilities::LIVE_OUTPUT` (`1 << 14`) against open protocol `11` drafts (`#386`, `#487`, `#412`), finalized cluster-scoped `member.has_client_role()` RBAC, `--follow` (`clap` implies `--wait`, conflicts with `--matrix` and `--verify`), `--tail <LINES>` / `--tail-bytes <BYTES>`, and `Ctrl+C` (`130`) semantics; ready for owner acceptance (`Status: Draft` and `Q55` open until owner decision).
