# 0109: Live output streaming, `logs`, and MCP tasks (`AX-3`)

Status: Draft
Created: 2026-11-24    Updated: 2026-11-24

## Summary

Add opt-in live `stdout`/`stderr` streaming from executing workers to waiting CLI and MCP clients, a `crates/master` bounded per-task output ring buffer backing `oxideswarm submit --wait --follow` and `oxideswarm logs <task>`, and MCP `2026-07-28` tasks-extension negotiation alongside existing `2024-11-05` support. Because this adds wire variants and a `ProtocolCapabilities` bit, implementation waits for owner approval of `Q55` and ships on the post-`0.4.x` release train ([design 0076](0076-protocol-compatibility-and-release-train.md)) with zero code merged in `0.4.x`.

## Context

- Today, a client receives a task's output only inside `WorkerMessage::TaskResult` (`crates/core/src/protocol.rs`) once execution finishes, while `WorkerMessage::TaskProgress` carries only `TaskStatus`.
- Worker process supervision (`crates/worker/src/runner.rs` and `crates/worker/src/runner/capture.rs`) collects child `stdout` and `stderr` with `StreamCapture` up to `DEFAULT_MAX_OUTPUT_BYTES` (`10 MiB` per stream) using head-and-tail capture and midpoint truncation ([design 0022](0022-head-and-tail-stream-capture.md)).
- `crates/core/src/lib.rs` enforces the twin-mirror rule across `crates/core/src/protocol/{worker_master_mirrors,client_msg_mirrors,client_resp_mirrors}.rs` (`Human*` and `Binary*` mirrors plus all four `From` conversions per public enum), and `crates/core/src/wire_layout_tests.rs` locks positional bincode variant indices against `crates/core/tests/data/wire_layout.txt`.
- `MCP_PROTOCOL_VERSION` in `crates/cli/src/mcp.rs` is `"2024-11-05"`, tested end-to-end by `tests/mcp_server_e2e.rs`. The CLI (`crates/cli/src/cli_app.rs`) has no `--follow` flag on `submit` and no `logs` subcommand.
- [Design 0056](0056-frontier-program.md) (`D2`, part `A3`) defines `AX-3` for `0.6.0` ([design 0088](0088-evaluation-of-2026-11-17-and-what-finished-means.md) / Q81–Q83), and [design 0064](0064-research-of-2026-11-13-and-route.md) (`D2`) raises **Q55** because two coding agents used by the owner do not yet implement the MCP `2026-07-28` tasks extension.

## Goals

- G1. Stream live `stdout` and `stderr` chunks from workers to subscribed CLI and MCP clients while a task is still running, enabling early inspection and cancellation (`cancel`) before completion.
- G2. Preserve [design 0022](0022-head-and-tail-stream-capture.md) final `TaskResult` head-and-tail output byte-for-byte regardless of live streaming or subscriber backpressure.
- G3. Enforce bounded worker queues and bounded master memory (`per-task` and global ring buffers) so chatty tasks or slow subscribers can never OOM a worker/master or stall child pipe draining.
- G4. Upgrade the MCP server to support `2026-07-28` and the tasks extension while keeping `"2024-11-05"` clients (`tests/mcp_server_e2e.rs`) working unmodified.
- G5. Follow [design 0076](0076-protocol-compatibility-and-release-train.md) (release train) and [design 0083](0083-one-version-window.md) (one-version window) cleanly with additive wire tail variants and explicit open decision `Q55`.

## Non-goals

- Implementing `AX-3` code or bumping `PROTOCOL_VERSION_CURRENT` in `0.4.x` (this record is draft design only).
- Guaranteeing lossless unbounded live output retention across arbitrary disconnections (`TaskResult` head/tail remains the bounded durable record; live buffers intentionally overwrite oldest chunks when full).
- Streaming binary output artifacts or interactive stdin (`--attach` / PTY) over the grid wire protocol.
- Breaking compatibility with the one-version window (`PROTOCOL_VERSION_MIN_COMPATIBLE = CURRENT - 1`).

## Design

### D1: Wire format, mirror enums, capability bit, and version bump

1. **New stream discriminator (`crates/core/src/protocol.rs`):**
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
   pub enum OutputStream {
       Stdout,
       Stderr,
   }
   ```
2. **Additive tail variants (strictly after all existing variants in each public enum in `crates/core/src/protocol.rs`):**
   - `WorkerMessage` (currently ending at `ResumeSession`):
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
   - `ClientMessage` (currently ending at `ReleaseWorkerId`):
     ```rust
     /// Request buffered and optional live output chunks for a running or recently finished task.
     SubscribeTaskOutput {
         task_id: TaskId,
         after_seq: Option<u64>,
         follow: bool,
     },
     ```
   - `ClientResponse` (currently ending at `WorkerIdReleased`):
     ```rust
     /// One buffered or live chunk in reply to `ClientMessage::SubscribeTaskOutput`.
     TaskOutputChunk {
         task_id: TaskId,
         stream: OutputStream,
         seq: u64,
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
3. **Mirror enums and `From` implementations (`crates/core/src/lib.rs` invariant):**
   - Every added public variant must appear at the exact corresponding tail position of BOTH `Human*` (`serde` JSON) and `Binary*` (positional bincode) mirror enums across:
     - `crates/core/src/protocol/worker_master_mirrors.rs`: `HumanWorkerMessage` and `BinaryWorkerMessage` plus all four `From` impls (`Human <-> Public`, `Binary <-> Public`).
     - `crates/core/src/protocol/client_msg_mirrors.rs`: `HumanClientMessage` and `BinaryClientMessage` plus all four `From` impls.
     - `crates/core/src/protocol/client_resp_mirrors.rs`: `HumanClientResponse` and `BinaryClientResponse` plus all four `From` impls.
4. **Capability bit (`crates/core/src/protocol.rs`):**
   - Allocate the next unused flag bit after `TREE_V2` (`1 << 13`):
     ```rust
     /// Live task stdout/stderr chunk streaming (`WorkerMessage::TaskOutputChunk`, `AX-3`).
     pub const LIVE_OUTPUT: Self = Self(1 << 14);
     ```
   - Include `Self::LIVE_OUTPUT.0` in `ProtocolCapabilities::all_supported()`. Workers advertising `LIVE_OUTPUT` emit `TaskOutputChunk` frames when a task runs.
5. **Protocol version bump and `MAX_CHUNK_BYTES` bound:**
   - Bump `PROTOCOL_VERSION_CURRENT` by `+1` on the target wire train (after the Protocol `11` train in draft design `0098` / PR `#386` lands).
   - Bound each chunk payload at `MAX_LIVE_OUTPUT_CHUNK_BYTES = 16 * 1024` (`16 KiB`), well below `MAX_FRAME_SIZE`. Master refuses oversized `TaskOutputChunk` payloads via protocol violation handling before buffering.

### D2: Worker live chunk tee preserving Design 0022 capture (`crates/worker/src/runner.rs`)

- Inside `crates/worker/src/runner/capture.rs`, the pipe-reader loop that feeds `StreamCapture` (`DEFAULT_MAX_OUTPUT_BYTES = 10 MiB` head/tail with midpoint notice per [design 0022](0022-head-and-tail-stream-capture.md)) adds an optional non-blocking live-chunk tee callback/channel (`Option<LiveOutputSink>`).
- For each stdout and stderr read:
  1. First update `StreamCapture` synchronously so final `TaskResult.stdout` and `TaskResult.stderr` are always exact and never lose head/tail bytes.
  2. Append bytes to a per-stream `16 KiB` staging buffer, flushing to a bounded per-task channel (`LIVE_QUEUE_CAPACITY = 64` chunks, max `1 MiB` queued per task) whenever a newline (`\n`) arrives, `staging.len() >= 16 KiB`, or `LIVE_FLUSH_INTERVAL = 100 ms` elapses.
- **Backpressure invariant:** sending from the pipe reader uses `try_send` — never `.await`. If the session uplink or outbound channel fills up, excess live chunks are dropped locally (incrementing a local dropped counter or coalescing) while the pipe continues draining uninterrupted to EOF.
- `crates/worker/src/client/session.rs` forwards queued `WorkerMessage::TaskOutputChunk` messages on the existing worker control/data connection with monotonically increasing per-task `seq` (`1, 2, ...`). Task completion drains at most a short flush (`<= 50 ms`) before sending `WorkerMessage::TaskResult`.

### D3: Master bounded output buffer and subscription broker (`crates/master/src/session.rs`)

- Introduce a `TaskOutputBroker` (owned alongside `TaskQueue` / `WaiterMap` in `crates/master`) holding per-task ring buffers:
  - **Per-task buffer cap:** `256 KiB` (`DEFAULT_TASK_LIVE_BUFFER_BYTES`). When appending a chunk would exceed `256 KiB`, oldest buffered chunks for that task are evicted from the front and their byte lengths are added to `dropped_bytes` (`u64`), preserving exact `seq` monotonicity (`next_seq`).
  - **Global buffer budget:** `32 MiB` across all tasks combined (`DEFAULT_GLOBAL_LIVE_BUFFER_BYTES`), evicting oldest buffered entries from terminal tasks first, then least-recently-updated tasks under memory pressure.
  - **Subscriber cap & slow-client drop:** cap concurrent `SubscribeTaskOutput` streams per task (e.g. `16`) and per-subscriber `mpsc` channel depth (`64` messages). Slow subscribers whose channel fills lose intermediate live frames cleanly (or receive closed stream with lagged error notice) without stalling `TaskQueue` or `WorkerSession`.
  - **Post-completion retention:** keep a task's terminal ring buffer briefly (`300 s`) for late `oxideswarm logs <task>` fetches before reaping, unless reclaimed earlier by the `32 MiB` global budget.
- **Master session dispatch (`crates/master/src/session.rs`):**
  - When `WorkerSession` receives `WorkerMessage::TaskOutputChunk { worker_id: claimed, task_id, stream, seq, bytes }`, enforce connection-authenticated identity: validate that the connection's authenticated `worker_id` (not `claimed`) currently owns `(worker_id, session_id)` on `task_id` before appending to `TaskOutputBroker`. Ignore mismatched or post-terminal chunks and log at `warn!` on claimed mismatch.
  - On `ClientMessage::SubscribeTaskOutput { task_id, after_seq, follow }`, send all retained chunks with `chunk.seq > after_seq.unwrap_or(0)` in order. If `!follow` or `task_id` is already terminal, immediately follow with `ClientResponse::TaskOutputEnd` and finish. If `follow` and active, keep forwarding live broadcast chunks until terminal status triggers `ClientResponse::TaskOutputEnd` or the client disconnects.
  - Submitting via `ClientMessage::SubmitTask { wait: true, .. }` can keep its existing single-response semantics unless the client opens a companion `SubscribeTaskOutput` stream (keeps existing RPC request/response clients simple and avoids interleaving streamed chunks on unsubscribed `wait: true` connections).

### D4: CLI (`submit --wait --follow` and `oxideswarm logs <task>`)

- **`crates/cli/src/cli_app.rs` & `crates/cli/src/cli_app/submit.rs`:**
  - Add `--follow` (`bool`, requires `--wait` or implies waiting for task completion; rejected with `-2` / usage error if combined with `--detach`).
  - When `--follow` is active, open a companion connection issuing `ClientMessage::SubscribeTaskOutput { task_id, after_seq: None, follow: true }` concurrently with `SubmitTask { wait: true }` (or reconnect-safe polling after receiving `TaskId`).
  - Route `OutputStream::Stdout` chunks verbatim to local `stdout` and `OutputStream::Stderr` chunks verbatim to local `stderr`. If `dropped_bytes > 0` is observed, emit a one-time notice to `stderr`.
  - On `--json`, emit newline-delimited JSON objects for each streamed chunk when `--follow` is set, followed by the final task result JSON line.
  - When `submit --wait --follow` reaches completion, avoid re-printing raw buffered `stdout`/`stderr` again at the bottom if streaming succeeded cleanly, while still printing exit status, worker name, timing, and downloaded `--output` files.
- **`oxideswarm logs <TASK_ID>` (`crates/cli/src/cli_app/tasks.rs`):**
  - Flags: `<TASK_ID>`, `--follow` / `-f`, `--tail <BYTES_OR_LINES>`, `--json`.
  - Fetches buffered history via `SubscribeTaskOutput` (`follow` mirrors `--follow`), displays `[live output unavailable: worker lacks LIVE_OUTPUT or buffer evicted]` cleanly if no live chunks were buffered, and falls back to `GetTaskResult` on completion so users still see captured `TaskResult` output.

### D5: MCP `2026-07-28` + Tasks Extension and `2024-11-05` Fallback (`crates/cli/src/mcp.rs`)

- **Protocol negotiation (`initialize`):**
  - Keep `SUPPORTED_MCP_VERSIONS = ["2026-07-28", "2024-11-05"]` with `LEGACY_MCP_PROTOCOL_VERSION = "2024-11-05"`.
  - If the client sends `"protocolVersion": "2026-07-28"` (and optionally `"capabilities": { "tasks": ... }`), reply with `"protocolVersion": "2026-07-28"` and advertise `"capabilities": { "tools": {}, "tasks": { ... } }`.
  - If the client sends `"2024-11-05"` or an unrecognized older version, reply with `"protocolVersion": "2024-11-05"` and `"capabilities": { "tools": {} }` — keeping every existing `2024-11-05` lifecycle and tool contract (`tests/mcp_server_e2e.rs`) identical.
- **Tasks extension vs non-extension agents (`Q55`):**
  - When a `2026-07-28` client declares tasks-extension support (`tasks/get`, `tasks/result`, `tasks/cancel`, `tasks/list`), `submit` / long-running tool calls create a protocol-level MCP task backed by the `TaskOutputBroker` tail + master `TaskId`, reporting `working` -> `completed`/`failed`/`cancelled` states with incremental output tail metadata.
  - **Subject to Open Decision Q55 ([design 0064](0064-research-of-2026-11-13-and-route.md) D2):** when a client speaks `2024-11-05` (or `2026-07-28` without the tasks extension), two options exist — keeping `submit` purely synchronous as today, or providing the `Q55` bounded-wait + job polling fallback described in "Decisions for the owner" below (`wait_ms` bounded `submit` returning `{ job_id / task_id, status: "running", tail }`, a polling wait tool, and a `cancel` tool that kills the worker process tree). `Q55` stays open for the owner before implementation begins.

## Compatibility

- **Wire version bump & Release Train ([design 0076](0076-protocol-compatibility-and-release-train.md) D1):**
  - Adding `WorkerMessage::TaskOutputChunk`, `ClientMessage::SubscribeTaskOutput`, and `ClientResponse::TaskOutputChunk`/`TaskOutputEnd` modifies wire message enums and bumps `PROTOCOL_VERSION_CURRENT` (`+1`, target version assigned when scheduled onto the next release train after Protocol `11` PR `#386` / draft design `0098` lands; **zero wire changes or code merged in `0.4.x`**).
  - The implementation PR must state `"wire change"` with the exact new protocol version in its PR description, update `crates/core/tests/data/wire_layout.txt` and `crates/core/src/window_tests.rs`, pass `scripts/release_skew.py` (`CI/release skew`), and trigger a release within 48 hours of landing on `master` (or before the following wire change).
- **One-version compatibility window ([design 0083](0083-one-version-window.md) D1):**
  - Because every new variant is appended strictly at the tail of each enum (and existing variants/structs keep their exact positional layouts), `PROTOCOL_VERSION_MIN_COMPATIBLE` stays `PROTOCOL_VERSION_CURRENT - 1` (assuming no non-additive change rides on the same bump):
    - Older (`CURRENT - 1`) worker + newer master: worker lacks `ProtocolCapabilities::LIVE_OUTPUT`; master runs tasks normally, records no live chunks, and still serves `GetTaskResult` on completion; `crates/cli/src/cli_app` & `mcp` clearly report live streaming unavailable rather than hanging.
    - Older (`CURRENT - 1`) client + newer master: older client never sends `SubscribeTaskOutput`, and master filters/forbids sending `TaskOutputChunk`/`TaskOutputEnd` unless solicited by `SubscribeTaskOutput` on a `CURRENT` connection (and validates `ClientMessage`/`ClientResponse` versions per `crates/core/src/protocol/codec.rs`).
    - Newer worker + older (`CURRENT - 1`) master: worker checks `RegisterAck.master_features` / negotiated peer `protocol_version >= AX3_PROTOCOL_VERSION` before sending any `WorkerMessage::TaskOutputChunk`.
- **On-disk store (`crates/master/src/store.rs`):** `STORE_FORMAT_VERSION` is unchanged (`TaskOutputBroker` is intentionally ephemeral/in-memory).

## Security

- **Peer identity & spoofing:** `WorkerSession` authorizes every `WorkerMessage::TaskOutputChunk` strictly from the TLS/OXS-AUTH-verified session `(worker_id, session_id)` owning `task_id`; body `worker_id` values from another worker or stale session are ignored.
- **Frame & memory exhaustion defenses:** Enforce `MAX_LIVE_OUTPUT_CHUNK_BYTES = 16 KiB` at decode/ingest, `256 KiB` ring limit per task, `32 MiB` global broker budget, `16` max concurrent subscribers per task, and `LIVE_QUEUE_CAPACITY = 64` non-blocking worker tee queue so neither a runaway child process nor slow/malicious subscriber can exhaust memory or stall worker pipe draining.
- **MCP stdout hygiene:** All streamed chunks live inside JSON-RPC frame payloads (never raw bytes to MCP `stdout`), and oversized line prevention caps tail excerpts per `truncate_stream_for_llm`.

## Test plan

1. **Core protocol & layout gates (`crates/core`):**
   - Round-trip tests in both `WireCodec::Bincode` and `WireCodec::Json` for `TaskOutputChunk`, `SubscribeTaskOutput`, and `TaskOutputEnd`, verifying all four `From` conversions per enum.
   - Cross-version refusal and window tests in `crates/core/src/protocol/version_and_aead_tests.rs`, `window_tests.rs`, `wire_layout_tests.rs`, and `tests/test_release_skew.py` (CI `release skew` against the prior release binary).
2. **Worker tee & Design 0022 invariance (`crates/worker`):**
   - Unit tests verifying a streaming child producing > `DEFAULT_MAX_OUTPUT_BYTES` still returns the exact head+tail midpoint truncation in `TaskResult` while emitting incremental live chunks, and verifying a blocked/full tee channel drops live chunks without blocking process completion.
3. **Master broker & session auth (`crates/master`):**
   - Unit tests for per-task `256 KiB` wraparound + `dropped_bytes`, global `32 MiB` eviction, connection-authenticated rejection of spoofed `WorkerMessage::TaskOutputChunk`, and `SubscribeTaskOutput` replay + live follow -> `TaskOutputEnd`.
4. **End-to-end CLI & MCP (`tests/` / `crates/cli/tests/`):**
   - Live streaming E2E proving `submit --wait --follow` and `oxideswarm logs <task> --follow` observe line 1 before the task exits cleanly.
   - MCP `2026-07-28` tasks polling test showing incremental tail retrieval on a running task, plus unmodified `tests/mcp_server_e2e.rs` (`2024-11-05`) green.
   - Miri CI job check on `rusty_grid_core` and `cargo fmt`/`clippy -D warnings`/`ox-mode test size`/`ox-mode doc verify`/`test_m1_challenger_hygiene.py`.

## Work items

Implementation waits for owner approval of `Q55` and opening of the post-`0.4.x` wire release train.

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| AX-3a | Wire types, mirrors, capability bit `LIVE_OUTPUT`, version bump & window tests | `crates/core/src/protocol.rs`, `crates/core/src/protocol/{worker_master_mirrors,client_msg_mirrors,client_resp_mirrors,codec,version_and_aead_tests}.rs`, `crates/core/src/window_tests.rs`, `crates/core/tests/data/wire_layout.txt` | All four `From` impls and codecs round-trip; `wire_layout_tests` and `release skew` pass | Planned |
| AX-3b | Worker bounded live tee preserving Design 0022 capture | `crates/worker/src/runner/capture.rs`, `crates/worker/src/runner.rs`, `crates/worker/src/client/session.rs` | Live chunks emit before exit without affecting `DEFAULT_MAX_OUTPUT_BYTES` head/tail output or stalling under full queue | Planned |
| AX-3c | Master `TaskOutputBroker` ring buffers & authenticated dispatch | `crates/master/src/session.rs`, `crates/master/src/server/client_and_members.rs`, `crates/master/src/lib.rs` | `SubscribeTaskOutput` delivers buffered + live chunks; `256 KiB`/`32 MiB` caps and session ownership enforced | Planned |
| AX-3d | CLI `submit --wait --follow` & `oxideswarm logs <task>` | `crates/cli/src/cli_app.rs`, `crates/cli/src/cli_app/{submit,tasks}.rs` | E2E test sees line 1 while task still runs; fallback works cleanly on `CURRENT - 1` workers | Planned |
| AX-3e | MCP `2026-07-28` tasks support + `2024-11-05` fallback (`Q55`) | `crates/cli/src/mcp.rs`, `crates/cli/src/mcp/submit.rs`, `tests/mcp_server_e2e.rs` | `2026-07-28` client polls running task tail; existing `2024-11-05` suite in `tests/mcp_server_e2e.rs` stays green | Planned |

## Decisions for the owner

The right column is a recommended default from [design 0064](0064-research-of-2026-11-13-and-route.md) and stays **Open** for owner approval before implementation starts:

| ID | Question | Recommended default | Status |
|---|---|---|---|
| **Q55** | Should `AX-3` also include a long-running MCP call flow for agents that do not support the `2026-07-28` tasks extension (bounded-wait `submit` streaming progress and returning `{ task_id, status: "running", tail }` when incomplete, paired with a polling wait tool and a process-tree `cancel` tool), while using the native MCP tasks extension automatically whenever a client declares it? | **Yes** ([design 0064](0064-research-of-2026-11-13-and-route.md) D2) | Open |

## Status log

- 2026-11-24: Draft design record `0109` written (`E-M4`, docs only — zero code merged in `0.4.x`); `Q55` remains open for owner decision before implementation starts on the post-`0.4.x` wire release train.
