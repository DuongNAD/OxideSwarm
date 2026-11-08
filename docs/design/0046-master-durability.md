# 0046: Master durability and worker session resume (`FEAT-1`)

Status: Draft
Created: 2026-11-08    Updated: 2026-11-08

## Summary

Adds an append-only task journal and snapshot store (`crates/master/src/store.rs`, enabled by
`master --state-dir <DIR>` / `RUSTY_GRID_STATE_DIR` / `[master].state_dir`) that persists task
submissions, state transitions, checkpoints, terminal results, and evictions, replays them on
startup (`Scheduled` and `Running` tasks return to `Queued` without retry or failover charge), and
lets reconnecting workers resume still-running tasks via `WorkerMessage::ResumeSession { worker_id,
running }` during a bounded grace period.

## Context

1. **In-memory task queue (`crates/master/src/queue.rs:538-560`).** `TaskQueueInner` holds `tasks`,
   `ready_queue`, `delayed_retries`, `worker_assignments`, `terminal_order`, and byte accounting in
   process memory only. No journal or store exists in `crates/master` (`crates/master/src/lib.rs:3-15`).
   When a master process restarts (upgrade, host reboot, or crash), all queued tasks, running task
   assignments, mid-task checkpoints, and retained terminal results are lost (`docs/BACKLOG.md:1097-1099`).
2. **Worker registration carries no active-task list (`crates/core/src/protocol.rs:455-460`).**
   `WorkerMessage::Register` sends only `worker_id`, `capabilities`, `protocol_version`, and
   `features`. A restarted master has no way to learn which tasks a reconnecting worker is already
   executing.
3. **Workers cancel all running tasks when a session ends (`crates/worker/src/client.rs:1447-1460`).**
   Inside `WorkerClient::run`, as soon as `connect_and_run` returns (whether from a transient TCP/QUIC
   drop, a heartbeat timeout, or a master restart), line `1459` calls
   `self.cancel_all_active_tasks(&cancel_reason).await` before sleeping and reconnecting. Even a
   one-second network glitch or a fast master restart kills every running subprocess on the worker
   and forces the task to restart from zero (`docs/BACKLOG.md:1100-1101`).
4. **Phase B prerequisite (`docs/ROADMAP.md:126-138`).** Phase B requires that a master restart
   loses nothing before Phase C (agent workspaces, `docs/design/0039-agent-workspaces.md`) runs
   long multi-minute agent builds across machines.

## Goals

- **G1.** Add `crates/master/src/store.rs` with an append-only binary journal (`journal.bin`) and
  atomic snapshot compaction (`snapshot.bin`), wired to `master --state-dir <DIR>`
  (`RUSTY_GRID_STATE_DIR`, `[master].state_dir`), recording task submission, state transitions,
  mid-task checkpoints, terminal results, and terminal evictions (`docs/BACKLOG.md:1103-1104`).
- **G2.** Replay `snapshot.bin` and `journal.bin` when the master starts so that retained terminal
  results (`Completed`, `Failed`, `Cancelled`) are readable immediately, `Queued` and `Retrying`
  tasks resume their queue order, and `Scheduled` or `Running` tasks become `TaskState::Queued`
  without incrementing `retry_count` or `failover_count` (`docs/BACKLOG.md:1105`).
- **G3.** Add a worker session-resume grace period (`worker --session-resume-grace-secs`, default
  `30 s`) and a capability-gated `WorkerMessage::ResumeSession { worker_id, running }` message so a
  worker keeps active tasks running across a transient disconnect or master restart and the master
  adopts those tasks back into `TaskState::Running` without dispatching a duplicate copy
  (`docs/BACKLOG.md:1106-1107`).
- **G4.** Prove end-to-end recovery in `tests/master_restart_recovery.rs`: killing a master that
  holds one running, two queued, and one finished task and restarting it on the same `--state-dir`
  allows reading the finished result back, completes both queued tasks, and finishes the running
  task **exactly once** (`docs/BACKLOG.md:1108-1110`).

## Non-goals

- Multi-master Raft/Paxos consensus or automatic leader failover (`FEAT-6` retired `ClaimMasterRole`
  in `docs/design/0026-refuse-claim-master-role.md`).
- Persisting `WorkerRegistry` connection state across master restarts; workers re-register and
  authenticate on a fresh transport connection after a restart.
- Resuming a task subprocess across a **worker** process crash (`FEAT-2`,
  `docs/design/0031-containment-after-worker-crash.md`, ensures task trees die when the worker
  dies).

## Design

### D1: `--state-dir` configuration, directory layout, and locking

1. **CLI, environment, and config file wiring:**
   - Add `--state-dir <DIR>` to `MasterArgs` (`crates/cli/src/cli_app.rs:312`), backed by
     `RUSTY_GRID_STATE_DIR` and `MasterFileConfig::state_dir: Option<String>`
     (`crates/cli/src/config.rs:70`).
   - Add `pub state_dir: Option<PathBuf>` and `pub fn with_state_dir(mut self, dir: impl Into<PathBuf>) -> Self`
     to `ServerConfig` (`crates/master/src/server.rs:148`), defaulting to `None` so existing unit
     and integration tests remain purely in-memory unless they opt in.
   - When `state_dir` is `Some(dir)` and `--blob-dir` / `RUSTY_GRID_BLOB_DIR` / `[master].blob_dir`
     was not explicitly provided, default the master's blob store directory
     (`crates/cli/src/cli_app.rs:1404-1415` and `MasterServer::bind_full` in
     `crates/master/src/server.rs:510-525`) to `<state_dir>/blobs` so content-addressed task input
     and output blobs reside beside the task journal.
2. **On-disk layout under `<state_dir>`:**
   - `<state_dir>/store.lock`: advisory lock file held via `std::fs::File::try_lock()` for the
     lifetime of the `TaskStore` so two master processes cannot open the same state directory
     concurrently. If `try_lock()` fails with `TryLockError::WouldBlock`, `TaskStore::open` returns
     `GridError::Config` naming `<state_dir>/store.lock`.
   - `<state_dir>/journal.bin`: append-only framed event log.
   - `<state_dir>/snapshot.bin`: latest compacted point-in-time checkpoint of `TaskQueue` entries
     (written to `<state_dir>/snapshot.bin.tmp`, `sync_all()`, and atomically renamed over
     `snapshot.bin`).
3. **Directory permissions and ownership check:**
   - On Unix, `<state_dir>` is created with mode `0o700` (`DirBuilderExt::mode(0o700)`), verified
     via `std::fs::symlink_metadata` not to be a symlink and to be owned by `current_euid()`, and
     its files (`store.lock`, `journal.bin`, `snapshot.bin`) are opened with mode `0o600`.

### D2: Append-only journal and snapshot compaction (`crates/master/src/store.rs`)

1. **File header and frame envelope:**
   - Both `journal.bin` and `snapshot.bin` begin with a 12-byte header: magic `b"OXSWJRN1"` (or
     `b"OXSWSNP1"` for `snapshot.bin`) followed by version `1u32` (little-endian).
   - Every journal frame after the header has the fixed layout:
     ```text
     [payload_len: u32 LE][blake3_hash: [u8; 32]][payload: [u8; payload_len]]
     ```
     `payload_len` is bounded by `rusty_grid_core::protocol::MAX_FRAME_SIZE` (`64 MiB`,
     `crates/core/src/protocol.rs:16`). `payload` is a `bincode`-serialized `JournalRecord`.
2. **Journal record types:**
   ```rust
   #[derive(Debug, Clone, Serialize, Deserialize)]
   pub enum JournalRecord {
       TaskSubmitted {
           task: Box<Task>,
           priority: u32,
           sequence: u64,
           submitted_at_utc: u64,
       },
       TaskDispatched {
           task_id: TaskId,
           worker_id: Uuid,
           timestamp_utc: u64,
       },
       TaskRunning {
           task_id: TaskId,
           worker_id: Uuid,
           timestamp_utc: u64,
       },
       CheckpointRecorded {
           task_id: TaskId,
           checkpoint: CheckpointData,
       },
       TaskRequeued {
           task_id: TaskId,
           state: TaskState, // Queued or Retrying
           retry_count: u32,
           failover_count: u32,
           rejection_count: u32,
           avoid_workers: Vec<Uuid>,
           retry_attempt: Option<RetryAttempt>,
           retry_delay_ms: Option<u64>,
       },
       TaskAdopted {
           task_id: TaskId,
           worker_id: Uuid,
           timestamp_utc: u64,
       },
       TaskTerminal {
           task_id: TaskId,
           state: TaskState, // Completed, Failed, or Cancelled
           assigned_worker_id: Option<Uuid>,
           retry_count: u32,
           failover_count: u32,
           rejection_count: u32,
           retry_attempt: Option<RetryAttempt>,
           result: Option<TaskResult>,
           error_message: Option<String>,
           timestamp_utc: u64,
       },
       TaskEvicted {
           task_id: TaskId,
       },
   }
   ```
3. **Durability (`sync_data`) boundary:**
   - `TaskSubmitted`, `CheckpointRecorded`, and `TaskTerminal` flush and call `File::sync_data()`
     before the master sends `ClientResponse::TaskSubmitted`, `ClientResponse::TaskCancelled`, or
     wakes completion waiters, guaranteeing that any acknowledged submission, checkpoint, or
     terminal outcome is on disk before the caller observes it.
   - High-frequency intermediate transitions (`TaskDispatched`, `TaskRunning`, `TaskRequeued`,
     `TaskAdopted`, `TaskEvicted`) write and flush to the OS file buffer; if the master crashes
     mid-flight, replay restores active tasks to `Queued` anyway (see D3).
4. **Torn-tail tolerance vs. mid-file corruption:**
   - If the final record at the end of `journal.bin` has a truncated header, fewer than
     `payload_len` bytes remaining before EOF, or a BLAKE3 mismatch on the final frame (as happens
     when the process dies mid-write), `TaskStore::open` logs a `warn!`, truncates `journal.bin`
     back to the offset of that final frame, and succeeds.
   - A bad magic header, unsupported version, `payload_len > MAX_FRAME_SIZE`, or a BLAKE3 / decode
     failure on any frame **before** the final frame is treated as storage corruption and returns
     `GridError::Storage` (`io::ErrorKind::InvalidData`), refusing startup rather than silently
     dropping history.
5. **Snapshot compaction (`snapshot.bin`):**
   - When `journal.bin` exceeds `DEFAULT_COMPACT_MAX_BYTES` (`64 MiB`) or
     `DEFAULT_COMPACT_MAX_RECORDS` (`10,000` records), or on clean master shutdown, `TaskStore`
     writes a `SnapshotImage` (`next_sequence: u64` plus all non-evicted `PersistedTaskEntry`
     records in `tasks` and `terminal_order`) to `<state_dir>/snapshot.bin.tmp` with a whole-payload
     BLAKE3 digest, calls `sync_all()`, atomically renames `snapshot.bin.tmp` to `snapshot.bin`,
     and truncates `journal.bin` back to its 12-byte header.

### D3: Startup replay and adoption hold window

1. **Replay into `TaskQueue` (`crates/master/src/queue.rs`):**
   - During `MasterServer::bind_full` (`crates/master/src/server.rs:509`), if `config.state_dir` is
     `Some(dir)`, the master opens `TaskStore::open(&dir)` and replays `snapshot.bin` + `journal.bin`
     into `TaskQueue` before binding the TCP/QUIC listeners or starting the scheduler.
   - `next_sequence` is restored to `max_seen_sequence + 1` so newly submitted tasks never collide
     with recovered `QueueOrderKey` sequences (`crates/master/src/queue.rs:140-145`).
   - Tasks whose replayed state is `Completed`, `Failed`, or `Cancelled` are restored into `tasks`,
     `terminal_order`, and `terminal_sizes` (evicting oldest entries if current `--max-retained-tasks`
     or `--max-retained-mb` is smaller than when the snapshot was written).
   - Tasks whose replayed state is `Submitted`, `Queued`, or `Retrying` are placed in
     `TaskState::Queued` in `ready_queue` with their preserved `priority`, `sequence`,
     `retry_count`, `failover_count`, `rejection_count`, `avoid_workers`, `retry_history`, and
     `task.latest_checkpoint`.
   - Tasks whose replayed state was `Scheduled` or `Running` on `worker_id` transition to
     `TaskState::Queued` **without** incrementing `retry_count`, `failover_count`, or
     `rejection_count` and without appending a `RetryAttempt` (`docs/BACKLOG.md:1105`).
2. **Adoption hold window (`adoptable_by`) so running tasks finish exactly once:**
   - When a task that was `Scheduled` or `Running` on `worker_id` is replayed into `TaskState::Queued`,
     `TaskEntry` records `adoptable_by: Option<(Uuid, Instant)> = Some((worker_id, now + adoption_hold))`
     where `ServerConfig::session_resume_hold_Window` defaults to `Duration::from_secs(15)` (more
     than `4 ×` the worker's initial reconnect backoff).
   - While `Instant::now() < hold_until`:
     - `TaskQueue::queued_task_views` (`crates/master/src/queue.rs`) skips any task whose
       `adoptable_by` deadline has not yet expired, so `WorkloadScheduler::schedule_pass` will not
       hand a second copy of the task to another worker while the original worker is reconnecting.
     - If the original `worker_id` reconnects and sends `WorkerMessage::ResumeSession { worker_id, running }`
       listing `task_id`, `TaskQueue::adopt_resumed_tasks` transitions the task directly from
       `TaskState::Queued` to `TaskState::Running` on `(worker_id, new_session_id)`, clears
       `adoptable_by`, increments the worker's `active_tasks` in `WorkerRegistry`, and writes
       `JournalRecord::TaskAdopted`.
     - If `worker_id` reconnects and sends `ResumeSession` **without** `task_id` (or registers
       without `SESSION_RESUME`), `adoptable_by` is cleared immediately for all tasks that were
       waiting on `worker_id`, and `scheduler_notify.notify_one()` wakes the scheduler to dispatch
       them right away.
     - When `hold_until` expires without `worker_id` reconnecting, the scheduler's periodic pass
       (`crates/master/src/server.rs:845`) sees the task as eligible and dispatches it normally.
3. **Re-pinning blobs on replay:**
   - When `blob_store` is enabled alongside `state_dir`, startup replay re-pins `task.inputs` for
     every recovered non-terminal task and re-pins `result.outputs` for every recovered terminal
     task so the blob store sweeper (`crates/master/src/server.rs:942-944`) does not evict their
     blobs before workers or clients read them.

### D4: Worker session-resume grace period and `WorkerMessage::ResumeSession`

1. **Capability and feature negotiation (`crates/core/src/protocol.rs`):**
   - Add `pub const SESSION_RESUME: Self = Self(1 << 12);` to `ProtocolCapabilities` and include it
     in `ProtocolCapabilities::all_supported()` (`crates/core/src/protocol.rs:95-108`).
   - Note: bit `1 << 10` is reserved for `WORKSPACE_SYNC` (`docs/design/0039-agent-workspaces.md`)
     and bit `1 << 11` is reserved for `THREE_D` (`docs/design/0044-dead-code-and-connection-loop-unification.md`).
   - Add `pub const SESSION_RESUME: &str = "session-resume";` to `master_features` and include it in
     `master_features::SUPPORTED` (`crates/core/src/protocol.rs:1406-1417`) so the master announces
     `features=mobile-telemetry,session-resume[,blobs]` in `RegisterAck.message`.
2. **Wire message (`WorkerMessage::ResumeSession`):**
   - Append `ResumeSession` at the end of `WorkerMessage` (`crates/core/src/protocol.rs:520`),
     `HumanWorkerMessage` (`crates/core/src/protocol.rs:608`), `BinaryWorkerMessage`
     (`crates/core/src/protocol.rs:683`), and `crates/core/src/binary_wire.rs`:
     ```rust
     /// Sent immediately after `RegisterAck` when both worker and master support
     /// `SESSION_RESUME` and the worker still has tasks running from its previous session.
     ResumeSession {
         worker_id: Uuid,
         running: Vec<TaskId>,
     },
     ```
   - Per `docs/design/0040-protocol-versioning.md` (D1/D2), adding a wire variant to `WorkerMessage`
     bumps `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` by 1 and updates
     `crates/core/tests/data/wire_layout.txt` via `UPDATE_WIRE_LAYOUT=1`.
   - A worker sends `WorkerMessage::ResumeSession` **only** when the master's `RegisterAck`
     announces `master_features::SESSION_RESUME` (`crates/core/src/protocol.rs:1406`).
3. **Worker grace period and outbox buffering (`crates/worker/src/client.rs`):**
   - Add `pub session_resume_grace: Duration` (default `Duration::from_secs(30)`) to `WorkerConfig`
     (`crates/worker/src/client.rs:211`), configurable via `worker --session-resume-grace-secs <SECS>`,
     `RUSTY_GRID_SESSION_RESUME_GRACE_SECS`, and `[worker].session_resume_grace_secs` (`0` disables
     grace and cancels immediately on disconnect as before).
   - In `WorkerClient::run` (`crates/worker/src/client.rs:1447-1460`), when `connect_and_run` exits:
     - If `*shutdown_rx.borrow()` is true, or `current_evacuation_reason().is_some()`, or
       `self.session_resume_grace.is_zero()`, or the worker has no active tasks and no buffered
       results, call `self.cancel_all_active_tasks(&cancel_reason).await` immediately as today.
     - Otherwise (connection drop, EOF, heartbeat timeout, or master restart while tasks are
       running), keep active tasks running and start/retain a `resume_deadline = Instant::now() + self.session_resume_grace`.
     - While disconnected, any `WorkerMessage::Checkpoint` or `WorkerMessage::TaskResult` emitted by
       a still-running task onto the old session's `outbound_tx` is captured into a bounded per-worker
       `pending_resume_outbox: Mutex<VecDeque<WorkerMessage>>` (capped at `max_concurrent_tasks * 2`
       messages, retaining only the latest `Checkpoint` per `task_id` plus its final `TaskResult`).
     - During the reconnect backoff sleep (`crates/worker/src/client.rs:1515`), also `tokio::select!`
       on `tokio::time::sleep_until(resume_deadline)`: if `resume_deadline` expires before a new
       session completes `RegisterAck`, call `self.cancel_all_active_tasks("session resume grace period expired").await`
       and clear `pending_resume_outbox`.
   - Upon completing `RegisterAck` in `connect_and_run`:
     - If the master announced `master_features::SESSION_RESUME`:
       1. Collect `running: Vec<TaskId>` consisting of all currently running task IDs in
          `active_tasks` plus any task IDs that have a buffered `TaskResult` in
          `pending_resume_outbox`.
       2. If `!running.is_empty()`, send `WorkerMessage::ResumeSession { worker_id: self.worker_id, running }`
          as the first post-handshake frame before heartbeats or new dispatches.
       3. Drain `pending_resume_outbox` onto the new session's `outbound_tx` so any `Checkpoint` or
          `TaskResult` that finished while the master was restarting is delivered on the adopted
          session.
     - If the master did **not** announce `master_features::SESSION_RESUME`, cancel any carried-over
       active tasks and clear `pending_resume_outbox` before entering the session loop.
4. **Master handling of `WorkerMessage::ResumeSession` (`crates/master/src/server.rs`):**
   - When the master receives `WorkerMessage::ResumeSession { worker_id, running }` on `session_id`:
     - Verify `worker_id == registered_worker_id` (rejecting mismatched IDs as a protocol violation).
     - Call `queue.adopt_session_tasks(worker_id, session_id, &running, store.as_ref()).await`:
       - For each `task_id` in `running`:
         - If the task is in `TaskState::Queued` (or `TaskState::Scheduled` / `TaskState::Running`
           from a prior session of the same `worker_id`) and is not owned by a different live worker:
           transition it to `TaskState::Running` with `assigned_worker_id = Some(worker_id)` and
           `assigned_session = Some(session_id)`, clear `adoptable_by`, add `task_id` to
           `worker_assignments[worker_id]`, remove it from `ready_queue` / `delayed_retries`, and
           append `JournalRecord::TaskAdopted`.
         - Otherwise (the task is unknown, `Cancelled`, `Completed`, `Failed`, or already running on
           a different worker): include `task_id` in `stale_to_cancel: Vec<TaskId>`.
       - For any task in `queue` that had `adoptable_by == Some((worker_id, _))` but was **not** in
         `running`: clear `adoptable_by` so the scheduler can dispatch it immediately.
     - Update `WorkerRegistry` active task count for `(worker_id, session_id)` to match the number
       of adopted tasks, send `MasterMessage::CancelTask { task_id }` for each task in
       `stale_to_cancel`, and notify `scheduler_notify`.
5. **Handling short network drops without a master restart:**
   - When a worker's TCP/QUIC connection drops while the master stays up, `handle_worker_disconnect`
     (`crates/master/src/server.rs:2557`) unregisters the old session and `failover_session_tasks`
     re-queues its tasks. For a worker that had negotiated `ProtocolCapabilities::SESSION_RESUME`,
     `handle_session_disconnected` sets `adoptable_by = Some((worker_id, now + adoption_hold))` on
     those re-queued tasks and does not charge `failover_count` unless the hold expires without
     `worker_id` resuming them. If `worker_id` reconnects within `adoption_hold` and sends
     `ResumeSession`, the master re-adopts the tasks seamlessly and subtracts the temporary
     disconnect from failover accounting.

## Compatibility

- **Wire protocol version:** Adding `WorkerMessage::ResumeSession` bumps `PROTOCOL_VERSION_CURRENT`
  and `PROTOCOL_VERSION_MIN_COMPATIBLE` in `crates/core/src/protocol.rs:31-33` and updates
  `crates/core/tests/data/wire_layout.txt` (enforced by `crates/core/src/wire_layout_tests.rs`).
- **Feature gating:** Even within a compatible protocol version range, a worker sends
  `WorkerMessage::ResumeSession` only when `RegisterAck.message` contains
  `master_features::SESSION_RESUME` (`"session-resume"`), and a master only sets the disconnect
  adoption hold for workers that advertised `ProtocolCapabilities::SESSION_RESUME`.
- **CLI and config defaults:** `--state-dir` is optional (`None` by default in `ServerConfig`), so
  all existing CLI invocations and test harnesses continue to work unchanged.

## Security

- **Filesystem permissions and symlink defense:** `<state_dir>` is created with mode `0o700` on
  Unix, checked with `symlink_metadata` to reject symlinks and foreign UIDs, and locked with an
  exclusive advisory lock (`store.lock`) so a second process cannot corrupt or race the journal.
- **Bounded deserialization and integrity:** Every journal and snapshot frame is length-checked
  against `MAX_FRAME_SIZE` (`64 MiB`) and verified with BLAKE3 before `bincode` deserialization.
- **Session ownership enforcement:** `ResumeSession` is only accepted after the connection has
  completed the `OXS-AUTH` cluster secret handshake (`crates/master/src/server.rs`) and `Register`
  for `worker_id`. A worker cannot adopt a task that is currently assigned to a different connected
  worker; attempting to do so causes the master to reply with `MasterMessage::CancelTask` for that
  `task_id`.

## Test plan

1. **Unit tests (`crates/master/src/store.rs`):**
   - Round-trip append and replay of `TaskSubmitted`, `TaskDispatched`, `TaskRunning`,
     `CheckpointRecorded`, `TaskRequeued`, `TaskAdopted`, `TaskTerminal`, and `TaskEvicted`.
   - Torn-tail recovery: append 5 records, truncate the last 7 bytes of `journal.bin` (or flip a
     byte in the final frame's payload), reopen `TaskStore`, and verify the first 4 records replay
     cleanly and `journal.bin` is truncated to the 4th frame boundary.
   - Mid-file corruption rejection: flip a byte in the 2nd of 4 frames and verify `TaskStore::open`
     returns an error rather than silently skipping the frame.
   - Compaction (`snapshot.bin`): write records past the compaction threshold, trigger compaction,
     verify `snapshot.bin` is written and `journal.bin` is truncated to 12 bytes, append more
     records, reopen, and verify full state matches.
   - Concurrent lock rejection: opening a second `TaskStore` on the same `<state_dir>` fails while
     the first lock is held.
2. **Unit tests (`crates/master/src/queue.rs` and `crates/core/src/protocol.rs`):**
   - `WorkerMessage::ResumeSession` round-trip in both Bincode and JSON codecs, plus
     `crates/core/src/wire_layout_tests.rs` golden layout verification.
   - Replay of `Scheduled` and `Running` tasks transitions them to `Queued` with `retry_count == 0`
     and `failover_count == 0`, sets `adoptable_by`, holds them out of `queued_task_views` until
     `hold_until`, and adopts them back into `Running` when `adopt_session_tasks` is called.
3. **End-to-end integration test (`tests/master_restart_recovery.rs`, registered in `crates/master/Cargo.toml`):**
   - Start a master with `--state-dir <tmp>` and one connected worker.
   - Submit 4 tasks:
     1. `t_done`: fast task that completes before the master is killed.
     2. `t_run`: shell/command task that writes a marker counter to a file and sleeps ~2 s so it is
        actively `Running` on the worker when the master goes down.
     3. `t_q1`, `t_q2`: two tasks queued behind `t_run` (worker `max_concurrency = 1`).
   - Wait until `t_done` is `Completed`, `t_run` is `Running`, and `t_q1`, `t_q2` are `Queued`.
   - Kill/abort the first master instance while the worker stays alive inside its session-resume
     grace window.
   - Start a second master bound to the same port and `--state-dir <tmp>`.
   - Verify:
     - `get_task_result(t_done)` immediately returns the completed result from before the restart.
     - The worker reconnects, sends `ResumeSession { running: [t_run] }`, and finishes `t_run`
       with execution marker count `== 1` (ran **exactly once**, `retry_count == 0`).
     - Both `t_q1` and `t_q2` are scheduled after `t_run` finishes and complete with `exit_code == 0`.
4. **Gates:**
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --locked -- -D warnings`
   - `cargo test -p rusty_grid_core --lib --locked`
   - `cargo test -p rusty_grid_master --lib --locked`
   - `cargo test -p rusty_grid_worker --lib --locked`
   - `cargo test -p rusty_grid_cli --lib --locked`
   - `cargo test -p rusty_grid_master --test master_restart_recovery --locked`

## Work items

The implementation is split into three ordered pull requests (`F1-a` -> `F1-b` -> `F1-c`) so each
lands as a self-contained, reviewable unit.

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| F1-design | Design record `0046` for master durability and worker session resume | `docs/design/0046-master-durability.md`, `docs/README.md` | Design record reviewed and accepted by the owner | Draft |
| F1-a | Append-only task journal, snapshot compaction, `--state-dir`, and startup replay (`Scheduled`/`Running` -> `Queued` uncharged, terminal results & queued tasks restored, blob re-pinning) | `crates/master/src/store.rs`, `crates/master/src/lib.rs`, `crates/master/src/queue.rs`, `crates/master/src/server.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `README.md` | Unit tests in `crates/master/src/store.rs` and `queue.rs` pass (round-trip, torn-tail recovery, mid-file corruption error, compaction, lock exclusion, and uncharged replay) | Planned |
| F1-b | `ProtocolCapabilities::SESSION_RESUME`, `master_features::SESSION_RESUME`, `WorkerMessage::ResumeSession`, and master task adoption + `adoptable_by` hold window | `crates/core/src/protocol.rs`, `crates/core/src/binary_wire.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/master/src/queue.rs`, `crates/master/src/server.rs`, `crates/master/src/registry.rs` | Wire round-trip and golden-bytes tests pass; master unit tests prove `adoptable_by` holds replayed running tasks from duplicate dispatch, `ResumeSession` adopts them into `Running`, and unknown/cancelled tasks receive `CancelTask` | Planned |
| F1-c | Worker session-resume grace period (`--session-resume-grace-secs`), disconnected outbox buffering, and `tests/master_restart_recovery.rs` | `crates/worker/src/client.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `crates/master/Cargo.toml`, `tests/master_restart_recovery.rs`, `README.md`, `docs/design/0046-master-durability.md` | `cargo test -p rusty_grid_master --test master_restart_recovery --locked` kills a master holding 1 running, 2 queued, and 1 finished task, restarts it, reads the finished result back, finishes both queued tasks, and finishes the running task exactly once | Planned |

## Status log

- **2026-11-08:** Draft created (`0046-master-durability.md`) for `FEAT-1` per Working Rule 7.
