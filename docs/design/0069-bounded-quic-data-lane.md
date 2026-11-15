# 0069: Bounded QUIC worker data-lane backlog and write timeout (`BUG-19`)

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

Bounds the outbound data-lane backlog of a 2-lane QUIC worker session on the master
(`crates/master/src/session.rs`) to at most `MAX_PENDING_DATA_MESSAGES` (`256`) messages and
`MAX_PENDING_DATA_BYTES` (`64 MiB`) of estimated payload bytes, and enforces a per-frame
`DATA_LANE_WRITE_TIMEOUT` (`60 s`) on `data_writer_task` so a worker that keeps heartbeating on its
control lane without draining its data lane cannot buffer unbounded memory or hold in-flight tasks
indefinitely (`BUG-19`).

## Context

In `PreAuthSession::Multiplexed` (`crates/master/src/session.rs`):

- `WorkerSession::register` creates a 128-slot `outbound_tx` / `outbound_rx` channel and spawns
  `router_task` to split outbound `MasterMessage` frames between the 64-slot `ctrl_out_tx` channel
  and the 64-slot `data_out_tx` channel (`AssignTask`, `AssignTaskWithCheckpoint`, `CancelTask`, and
  `BlobReply`).
- When `data_out_tx` was full, `router_task` pushed overflow data messages into an unbounded
  `pending_data: VecDeque<MasterMessage>` and kept reading `outbound_rx` on every `tokio::select!`
  iteration. Because `outbound_rx` never stayed full, `outbound_tx` could not apply backpressure to
  `BlobService` (`crates/master/src/server.rs`) or task dispatch.
- `data_writer_task` awaited `data_writer.send_msg(&msg)` with no timeout, and when a write failed it
  only exited `data_writer_task` without waking `WorkerSession::run` if the inbound streams stayed
  open.
- Consequently, a worker that continued heartbeating on `STREAM_CONTROL` while never reading
  `STREAM_DATA` could cause the master to buffer `AssignTask` and 4 MiB `BlobReply::Chunk` frames
  without limit.

## Goals

- **G1**: Cap `pending_data` in `router_task` by both message count (`MAX_PENDING_DATA_MESSAGES =
  256`) and estimated payload bytes (`MAX_PENDING_DATA_BYTES = 64 MiB`), stopping reads from
  `outbound_rx` while at the cap so the 128-slot `outbound_tx` channel pushes back on its senders.
- **G2**: Enforce `DATA_LANE_WRITE_TIMEOUT` (`60 s`) on each `data_writer.send_msg(&msg)` call in
  `data_writer_task`, and route data-lane write timeouts and write errors through the existing
  session disconnect and task failover path in `WorkerSession::run`.
- **G3**: Update the `BlobService` documentation in `crates/master/src/server.rs` to describe the
  actual buffering bound across `BlobService`, `outbound_tx`, `data_out_tx`, and `pending_data`.

## Non-goals

- Changing the wire protocol, message enums, `PROTOCOL_VERSION_CURRENT`, or single-stream TCP
  session framing.
- Changing `WorkloadScheduler` (`crates/master/src/scheduler.rs`) or `TaskQueue`
  (`crates/master/src/queue.rs`).

## Design

### D1: Bounded `pending_data` in `router_task` (`crates/master/src/session.rs`)

- Define `MAX_PENDING_DATA_MESSAGES: usize = 256`, `MAX_PENDING_DATA_BYTES: usize = 64 * 1024 *
  1024` (`64 MiB`), and `ESTIMATED_DATA_MESSAGE_BYTES: usize = 1024`.
- `estimate_data_message_bytes` sizes `MasterMessage::BlobReply` by its payload length
  (`BlobReply::Chunk { data, .. }` -> `data.len()`, `BlobReply::Missing { ids }` -> `ids.len() *
  size_of::<BlobId>()`, `BlobReply::Failed { message }` -> `message.len()`, and
  `Written` / `NotFound` -> `ESTIMATED_DATA_MESSAGE_BYTES`) and sizes `AssignTask`,
  `AssignTaskWithCheckpoint`, and `CancelTask` as `ESTIMATED_DATA_MESSAGE_BYTES`.
- `PendingDataBuffer` tracks both queued message count and total estimated bytes (recording current
  and high-water values in `RouterBacklogStats`).
- When `pending_data.is_at_cap()` (`len >= MAX_PENDING_DATA_MESSAGES || bytes >=
  MAX_PENDING_DATA_BYTES`), `router_task` awaits only `data_out_tx.reserve()` and stops polling
  `outbound_rx.recv()` until `pending_data` drains below both caps. If a data message read while
  `pending_data` is non-empty would push `bytes + msg_bytes > MAX_PENDING_DATA_BYTES`,
  `router_task` drains front entries into `data_out_tx` before pushing the new message so buffered
  bytes never exceed `MAX_PENDING_DATA_BYTES`.
- Direct control replies (`HeartbeatAck`, `RoleHandoverAck`, and master `Shutdown`) continue to use
  `ctrl_reply_tx` (`ctrl_out_tx`), bypassing `outbound_rx` and `router_task`, so control-lane
  heartbeats remain responsive even when `pending_data` and `outbound_tx` are full.

### D2: Data-lane write timeout and unified session teardown (`crates/master/src/session.rs`)

- Define `DATA_LANE_WRITE_TIMEOUT: Duration = Duration::from_secs(60)`.
- In `data_writer_task`, wrap `data_writer.send_msg(&msg)` in
  `tokio::time::timeout(data_write_timeout, data_writer.send_msg(&msg))`. When a write exceeds the
  timeout, log a warning naming `worker_id`, `session_id`, the message variant, and `timeout_secs`,
  and synthesize `Err(ProtocolError::Io(io::Error::new(io::ErrorKind::TimedOut, ...)))`.
- In the `Err(e)` branch of `data_writer_task` (shared by socket write errors and write timeouts),
  forward `e` over `data_writer_err_tx` (`mpsc::channel::<ProtocolError>(1)`) and exit the writer
  loop.
- `ActiveLanes::Multiplexed::recv_next` selects on `data_writer_err_rx.recv()` and returns
  `Err((LaneKind::Data, err))`, which `WorkerSession::run` handles through its existing
  `Err((lane, e))` branch (`handle_worker_disconnect_with_broadcast` followed by session loop exit
  and `P2pConnGuard::unlist`), unregistering the session and failing over its in-flight tasks.

### D3: `BlobService` documentation (`crates/master/src/server.rs`)

- Update the doc comment on `BlobService` (`crates/master/src/server.rs`) to document how per-session
  buffering is bounded across `BlobService.requests` (`MAX_IN_FLIGHT_REQUESTS * 2`), the 128-slot
  `outbound_tx` channel, the 64-slot `data_out_tx` channel, and `router_task`'s `pending_data` cap
  (`MAX_PENDING_DATA_MESSAGES = 256`, `MAX_PENDING_DATA_BYTES = 64 MiB`), with
  `DATA_LANE_WRITE_TIMEOUT` (`60 s`) terminating sessions whose data stream stalls.

## Compatibility

- Wire format, message enums, `PROTOCOL_VERSION_CURRENT`, CLI flags, and configuration files are
  unchanged.

## Security

- A registered QUIC worker that keeps heartbeating on `STREAM_CONTROL` while refusing to read from
  `STREAM_DATA` can no longer force unbounded master heap growth in `pending_data`: `pending_data`
  holds at most `256` messages and `64 MiB` of estimated payload bytes, `outbound_tx` backpressures
  `BlobService` after `128` additional messages, and `DATA_LANE_WRITE_TIMEOUT` (`60 s`) terminates
  the stalled session and requeues its tasks.

## Test plan

- **Unit (`crates/master/src/session.rs`):**
  - `test_multiplexed_data_lane_backlog_capped_by_messages_and_bytes`: verifies on an undrained data
    lane that `pending_data` never exceeds `MAX_PENDING_DATA_MESSAGES` (`256`) under a flood of
    `AssignTask` messages or `MAX_PENDING_DATA_BYTES` (`64 MiB`) under a flood of 4 MiB
    `BlobReply::Chunk` messages (including mixed message sizes), that `outbound_tx` pushes back once
    full, and that `Heartbeat` / `HeartbeatAck` continue flowing on the control lane while
    `pending_data` is at its cap.
  - `test_multiplexed_data_lane_write_timeout_ends_session_and_requeues_tasks`: uses
    `#[tokio::test(start_paused = true)]` to verify that a data-lane write blocked for longer than
    `DATA_LANE_WRITE_TIMEOUT` (`60 s`) terminates the session, marks the worker `Disconnected`, and
    moves its running task to `TaskState::Retrying` (no worker, `retry_count` 1, `failover_count` 1).
- **Integration and hygiene gates:**
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo test -p rusty_grid_master --lib --locked`
  - `cargo test -p rusty_grid_master --locked --test m4_scheduling --test master_restart_recovery`
  - `python3 tests/test_m1_challenger_hygiene.py`

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| BUG-19 | Cap QUIC session `pending_data` by message count (`256`) and bytes (`64 MiB`), add `60 s` data-lane write timeout, and update `BlobService` comment | `crates/master/src/session.rs`, `crates/master/src/server.rs`, `docs/design/0069-bounded-quic-data-lane.md`, `docs/README.md`, `CHANGELOG.md` | Unit tests for message/byte caps and write timeout pass alongside master scheduling, recovery, clippy, fmt, and doc hygiene gates | Implemented |

## Status log

- 2026-11-15: Implemented `MAX_PENDING_DATA_MESSAGES` (`256`), `MAX_PENDING_DATA_BYTES` (`64 MiB`),
  and `DATA_LANE_WRITE_TIMEOUT` (`60 s`) in `crates/master/src/session.rs`, updated the
  `BlobService` comment in `crates/master/src/server.rs`, and added unit tests (`BUG-19`).
