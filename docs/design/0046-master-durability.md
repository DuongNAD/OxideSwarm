# 0046: Master durability and worker session resume (`FEAT-1`)

Status: Draft
Created: 2026-11-08    Updated: 2026-11-08

## Summary

Adds an append-only task journal and self-describing snapshot store (`crates/master/src/store.rs`,
enabled by `master --state-dir <DIR>` / `RUSTY_GRID_STATE_DIR` / `[master].state_dir`) that persists
task submissions (with `5 ms` group commit), state transitions, checkpoints, terminal results, and
evictions, replays them on startup (`Scheduled` and `Running` tasks return to `Queued` without retry
or failover charge, or `Failed` if `Running` with `max_retries == 0` and not resumed), and lets
reconnecting workers resume still-running tasks via `WorkerMessage::ResumeSession { worker_id,
running }` within a master-advertised hold window (`--session-resume-hold-secs`).

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

- **G1.** Add `crates/master/src/store.rs` with an append-only binary journal (`journal.bin`),
  `5 ms` group commit for submissions, and atomic self-describing JSON snapshot compaction
  (`snapshot.bin`), wired to `master --state-dir <DIR>` (`RUSTY_GRID_STATE_DIR`,
  `[master].state_dir`), recording task submission, state transitions, mid-task checkpoints,
  terminal results, and terminal evictions (`docs/BACKLOG.md:1103-1104`).
- **G2.** Replay `snapshot.bin` and `journal.bin` when the master starts so that retained terminal
  results (`Completed`, `Failed`, `Cancelled`) are readable immediately, `Queued` and `Retrying`
  tasks resume their queue order, and `Scheduled` or `Running` tasks become `TaskState::Queued`
  without incrementing `retry_count` or `failover_count` (while failing any `Running` task with
  effective `max_retries == 0` that is not resumed during the hold window)
  (`docs/BACKLOG.md:1105`).
- **G3.** Make the master the single source of truth for the session-resume hold window
  (`master --session-resume-hold-secs`, default `30 s`), advertised in `RegisterAck` as
  `session-resume=<secs>`, and bound the worker's grace period by
  `hold - dead_peer_timeout - margin` so a reconnecting worker resumes still-running tasks via
  `WorkerMessage::ResumeSession { worker_id, running }` and no task ever runs concurrently on two
  workers (`docs/BACKLOG.md:1106-1107`).
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
   - Add `--session-resume-hold-secs <SECS>` to `MasterArgs` (`crates/cli/src/cli_app.rs:312`),
     backed by `RUSTY_GRID_SESSION_RESUME_HOLD_SECS`, `MasterFileConfig::session_resume_hold_secs: Option<u64>`,
     and `ServerConfig::session_resume_hold: Duration` (default `Duration::from_secs(30)`; `0`
     disables session resume).
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
   - `<state_dir>/snapshot.bin`: latest compacted point-in-time checkpoint of `TaskQueue` entries in
     self-describing JSON (written to `<state_dir>/snapshot.bin.tmp`, `sync_all()`, and atomically
     renamed over `snapshot.bin`).
3. **Directory permissions and ownership check (Unix and Windows):**
   - On Unix, `<state_dir>` is created with mode `0o700` (`DirBuilderExt::mode(0o700)`), verified
     via `std::fs::symlink_metadata` not to be a symlink and to be owned by `current_euid()`, and
     its files (`store.lock`, `journal.bin`, `snapshot.bin`) are opened with mode `0o600`.
   - On Windows, `<state_dir>` is verified via `std::fs::symlink_metadata` not to be a symlink or
     reparse point (`FileType::is_symlink`), and its DACL is restricted to the current user (owner)
     and `SYSTEM` / `Administrators` with inheritance stripped (`icacls <state_dir> /inheritance:r
     /grant:r "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F" "<current_user_sid>:(OI)(CI)F"`),
     matching the owner-only ACL pattern used in `packaging/windows/install_windows_service.ps1:805`
     for the directory holding `cluster.secret`.

### D2: Append-only journal, group commit, and self-describing snapshot compaction (`crates/master/src/store.rs`)

1. **File header (including `PROTOCOL_VERSION_CURRENT`) and frame envelope:**
   - Both `journal.bin` and `snapshot.bin` begin with a 24-byte header:
     ```text
     [magic: 8B][store_format_version: u32 LE][protocol_version: u32 LE][generation: u64 LE]
     ```
     where `magic` is `b"OXSWJRN1"` for `journal.bin` and `b"OXSWSNP1"` for `snapshot.bin`,
     `store_format_version` is `1u32`, and `protocol_version` records
     `rusty_grid_core::protocol::PROTOCOL_VERSION_CURRENT` (`crates/core/src/protocol.rs:31`) at the
     time the file was written.
   - Every journal frame after the 24-byte header has the fixed layout:
     ```text
     [payload_len: u32 LE][len_check: u32 LE][blake3_hash: [u8; 32]][payload: [u8; payload_len]]
     ```
     `payload_len` is bounded by `rusty_grid_core::protocol::MAX_FRAME_SIZE` (`64 MiB`,
     `crates/core/src/protocol.rs:16`). In `journal.bin`, `payload` is a `bincode`-serialized
     `JournalRecord`. In `snapshot.bin`, a single frame follows the 24-byte header with a `u64` length
     (`[payload_len: u64 LE][blake3][payload]`, bounded by 4 GiB) whose `payload`
     is UTF-8 JSON (`serde_json`) of `SnapshotImage` using the `is_human_readable() == true` serde
     representations (`HumanTaskSpec`, etc. in `crates/core/src/task.rs:1442-1460`), so snapshots
     remain self-describing across `PROTOCOL_VERSION_CURRENT` bumps via `#[serde(default)]`.
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
           evacuation_count: u32,
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
           evacuation_count: u32,
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
3. **Durability (`sync_data`) boundary and `5 ms` group commit:**
   - **Sync set:** `TaskSubmitted`, `TaskRequeued`, `CheckpointRecorded`, `TaskTerminal`, and
     `TaskDispatched` / `TaskRunning` (when the task's effective `max_retries == 0`, so the
     "already dispatched or started once" fact cannot be lost in a crash) are durable records that
     call `File::sync_data()`. Syncing `TaskRequeued` guarantees that `retry_count`,
     `failover_count`, `evacuation_count`, and `rejection_count` never roll backward across a crash
     and grant a task more retries than its budget.
   - **Submission gate vs. in-flight transition behaviour on storage failure:**
     - On `submit` / `try_submit_slot` / `submit_with_priority`, `TaskQueue` checks store health
       and enqueues `TaskSubmitted` before mutating `inner.tasks` or advancing `next_sequence`,
       and holds off inserting the task into `ready_queue` until the `DurableAck` resolves inside a
       spawned task (`tokio::spawn`) that the caller awaits. Because the `DurableAck` resolution and
       `ready_queue` insertion (or rollback on `Err(GridError::Storage)`) run in that spawned task,
       a concurrent scheduler pass cannot dispatch an un-synced task during the `5 ms` group-commit
       window, and dropping or aborting the caller's `submit` future mid-ack still completes the
       `ready_queue` insertion (or rollback) consistently with `journal.bin` rather than leaving a
       zombie `Queued` task outside `ready_queue`.
     - Once a task has been admitted, subsequent transitions (`record_result_from`,
       `cancel_task_detailed`, `fail_unsendable_assignment`, `requeue_task`, `mark_running_from`,
       `record_checkpoint`) commit in memory first and log a `tracing::warn!` if `append_durable`
       or `DurableAck` fails, so a degraded disk never leaks worker capacity slots, leaves task
       blobs pinned forever, or strands awaiting clients. Once the store fails, durability is gone
       until a restart; the failure reason is surfaced via `TaskStore::failure_reason()`,
       `QueueStats::store_error`, `ClientResponse::ClusterStatus.store_error` (bumping
       `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` to `5`),
       `ClusterStatusDto::store_error`, and `oxideswarm doctor` (which records a failed `Task store`
       check).
   - **`5 ms` group commit for `TaskSubmitted` and `TaskTerminal` (and concurrent durable records):**
     - A MapReduce job, an MCP burst of 1,000 task submissions, or a worker returning a burst of
       task results on a single connection must not execute one sequential `sync_data()` syscall per
       message.
     - `TaskStore` runs a dedicated writer loop fed by an `mpsc` channel of `(JournalRecord,
       Option<oneshot::Sender<GridResult<()>>>)`.
     - When the first record requiring `sync_data()` arrives, the writer appends it to the OS buffer
       and waits up to `GROUP_COMMIT_WINDOW = Duration::from_millis(5)` (or until `MAX_GROUP_BATCH = 256`
       records have accumulated) to coalesce any concurrent submissions/transitions arriving in the
       same window, calls `File::sync_data()` **once** for the entire batch, and then signals all
       waiting `oneshot::Sender` handles together.
     - On the worker session inbound path (`handle_session_task_message`), `apply_task_result_inner`
       enqueues `TaskTerminal` via `record_result_from_enqueued`, releases the worker slot and unpins
       blobs immediately, and awaits the `DurableAck` + wakes completion `waiters` in a spawned task
       so sequential `WorkerMessage::TaskResult` frames on one connection coalesce inside the same
       `5 ms` group-commit window.
   - Non-synced transitions (`TaskDispatched` and `TaskRunning` when `max_retries > 0`, `TaskAdopted`,
     `TaskEvicted`) are written to `journal.bin` without blocking on `sync_data()`.
4. **Torn-tail tolerance, mid-file corruption, and `GridError::Storage`:**
   - Add `GridError::Storage(String)` (`#[error("Storage error: {0}")]`) in
     `crates/core/src/error.rs:14`.
   - If the final record at the end of `journal.bin` has a truncated header, fewer than
     `payload_len` bytes remaining before EOF, or a BLAKE3 mismatch on the final frame (as happens
     when the process dies mid-write), `TaskStore::open` logs a `warn!`, truncates `journal.bin`
     back to the offset of that final frame, and succeeds.
   - A bad magic header, unsupported `store_format_version`, `payload_len > MAX_FRAME_SIZE`, or a
     BLAKE3 / decode failure on any frame **before** the final frame is treated as storage
     corruption and returns `GridError::Storage`, refusing startup rather than silently dropping
     history.
5. **Snapshot compaction (`snapshot.bin`) and clean-shutdown compaction:**
   - When `journal.bin` exceeds `DEFAULT_COMPACT_MAX_BYTES` (`64 MiB`) or
     `DEFAULT_COMPACT_MAX_RECORDS` (`10,000` records), or on **clean master shutdown** (`shutdown_tx`
     / `SIGINT` / `SIGTERM`), `TaskStore` writes a self-describing JSON `SnapshotImage`
     (`next_sequence: u64` plus all non-evicted `PersistedTaskEntry` records in `tasks` and
     `terminal_order`) to `<state_dir>/snapshot.bin.tmp` with the 24-byte header (`b"OXSWSNP1"`,
     `1u32`, `PROTOCOL_VERSION_CURRENT`) and frame envelope (`[payload_len u64][blake3][json_bytes]`),
     calls `sync_all()`, atomically renames `snapshot.bin.tmp` to `snapshot.bin`, and resets
     `journal.bin` to its 24-byte header (generation `g + 1` in both files) with `PROTOCOL_VERSION_CURRENT` and 0 records.

### D3: Startup replay, `max_retries == 0` enforcement, and adoption hold window

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
     `retry_count`, `failover_count`, `evacuation_count`, `rejection_count`, `avoid_workers`,
     `retry_history`, and `task.latest_checkpoint`.
   - Tasks whose replayed state was `Scheduled` or `Running` on `worker_id` transition to
     `TaskState::Queued` **without** incrementing `retry_count`, `failover_count`,
     `evacuation_count`, or `rejection_count` and without appending a `RetryAttempt`
     (`docs/BACKLOG.md:1105`), subject to the adoption hold and `max_retries == 0` rule below.
2. **Adoption hold window (`adoptable_by`) and `max_retries == 0` ("run at most once") rule:**
   - When a task that was `Scheduled` or `Running` on `worker_id` is replayed:
     - If `config.session_resume_hold > Duration::ZERO`, `TaskEntry` records
       `adoptable_by: Option<(Uuid, Instant)> = Some((worker_id, now + config.session_resume_hold))`
       (default `30 s`).
     - If the task's replayed state was `Running` (or `Scheduled`) and its effective retry limit is
       zero (`task.requirements.max_retries == Some(0)` or `retry_policy.max_retries == 0`),
       `TaskEntry` also sets `fail_if_not_adopted: bool = true`. (If `config.session_resume_hold.is_zero()`,
       such a task transitions immediately on replay to `TaskState::Failed` with reason
       `"master restarted while the task ran; not re-run because max_retries is 0"`.)
   - While `Instant::now() < hold_until`:
     - `TaskQueue::queued_task_views` (`crates/master/src/queue.rs`) skips any task whose
       `adoptable_by` deadline has not yet expired, so `WorkloadScheduler::schedule_pass` will not
       hand a second copy of the task to another worker while the original worker is reconnecting.
     - If the original `worker_id` reconnects and sends `WorkerMessage::ResumeSession { worker_id, running }`
       listing `task_id`, `TaskQueue::adopt_session_tasks` transitions the task directly from
       `TaskState::Queued` to `TaskState::Running` on `(worker_id, new_session_id)`, clears
       `adoptable_by` and `fail_if_not_adopted`, increments the worker's `active_tasks` in
       `WorkerRegistry`, and writes `JournalRecord::TaskAdopted`.
     - If `worker_id` reconnects and sends `ResumeSession` **without** `task_id` (or registers
       without `SESSION_RESUME`), or when `hold_until` expires without `worker_id` reconnecting:
       - If `fail_if_not_adopted` is `true` (effective `max_retries == 0`), the task never enters
         `ready_queue` for a second run; instead it transitions to `TaskState::Failed` with
         `error_message = Some("master restarted while the task ran; not re-run because max_retries is 0".into())`,
         syncs `JournalRecord::TaskTerminal`, and wakes any completion waiters.
       - Otherwise, `adoptable_by` is cleared and the task becomes eligible in `ready_queue` for
         the scheduler to dispatch.
3. **Store-health reporting, visibility before `fsync`, and clean-shutdown fencing (`D3.3`):**
   - **Store-health reporting:** Once the store fails, durability is gone until a restart. `TaskStore::failure_reason(&self) -> Option<String>` exposes the writer's failure reason; `TaskQueue::stats()` populates `QueueStats::store_error`, which flows into `ClientResponse::ClusterStatus.store_error` (`crates/core/src/protocol.rs`, protocol version `5`), `ClusterStatusDto::store_error` (`crates/master/src/dashboard/dto.rs`), `oxideswarm status`, MCP `cluster_status`, and `oxideswarm doctor` (`crates/cli/src/doctor.rs`, which records `Outcome::Fail` on `C1 Task store` whenever `store_error` is `Some`).
   - **Visibility and schedulability relative to `fsync`:** A task is schedulable only after its `TaskSubmitted` `DurableAck` resolves (an acknowledged `submit`), and a `Completed` result is readable in `get_task` / `get_result` immediately when `record_result_from_enqueued` commits in memory, before that result's own `fsync` (which gates `--wait` completion waiters) or a subsequent result's `fsync` completes.
   - **Clean-shutdown boundary (`store_closed`):** When `MasterServer::run` (or `MasterHandle::shutdown`) calls `close_and_compact_store` / `close_and_compact_store_sync`, `TaskQueueInner::store_closed` is set to `true` atomically under the queue write lock before writing the final `snapshot.bin`. Once `store_closed` is `true`, new submissions (`submit_with_priority`, `try_submit_slot`) and post-shutdown results (`record_result_from_enqueued`) return `Err(GridError::Storage("task store is closed (master shutting down)"))`, and post-shutdown worker session teardown (`handle_session_disconnected`, `handle_session_evacuated`) leaves in-flight task states untouched so workers still transition to `Disconnected` in `WorkerRegistry` without charging `failover_count` after the final snapshot is written.
4. **Re-pinning blobs on replay:**
   - When `blob_store` is enabled alongside `state_dir`, startup replay re-pins `task.inputs` for
     every recovered non-terminal task and re-pins `result.outputs` for every recovered terminal
     task so the blob store sweeper (`crates/master/src/server.rs:942-944`) does not evict their
     blobs before workers or clients read them.

### D4: Master-advertised hold, worker grace inequality, and `WorkerMessage::ResumeSession`

1. **Capability and feature negotiation with master-sourced hold (`crates/core/src/protocol.rs`):**
   - Add `pub const SESSION_RESUME: Self = Self(1 << 12);` to `ProtocolCapabilities` (`crates/core/src/protocol.rs:100`).
     Keep `SESSION_RESUME` out of `ProtocolCapabilities::all_supported()` in the `F1-b` wire pull
     request, and add it to `all_supported()` only in the pull request that implements master task
     adoption and worker session resume (`F1-b` master adoption / `F1-c`) so a v4 worker built
     before resume support does not cause a master to hold its tasks on disconnect.
   - Note: bit `1 << 10` is reserved for `WORKSPACES` (`docs/design/0039-agent-workspaces.md`)
     and bit `1 << 11` is reserved for `THREE_D` (`docs/design/0044-dead-code-and-connection-loop-unification.md`).
   - In `master_features` (`crates/core/src/protocol.rs:1441-1485`), add:
     - `pub const SESSION_RESUME_PREFIX: &str = "session-resume=";`
     - `pub const MAX_SESSION_RESUME_HOLD_SECS: u64 = 3_600;` (`1 hour` upper bound so bogus values
       such as `u64::MAX` are rejected before `Instant + Duration` arithmetic)
     - `pub fn session_resume_feature(hold_secs: u64) -> String` (formats `"session-resume={hold_secs}"`)
     - `pub fn parse_session_resume_hold_secs(message: Option<&str>) -> Option<u64>` (returns `None`
       when missing, malformed, or `> MAX_SESSION_RESUME_HOLD_SECS`)
     When `ServerConfig::session_resume_hold` is non-zero (`--session-resume-hold-secs`, default
     `30`), the master includes `session-resume=<secs>` in the `RegisterAck` feature announcement
     (e.g., `features=mobile-telemetry,session-resume=30,blobs`). When `--session-resume-hold-secs 0`
     is configured, the token is omitted.
2. **Wire message (`WorkerMessage::ResumeSession`) and removing `compute_intensity` on the bump:**
   - Append `ResumeSession` at the end of `WorkerMessage` (`crates/core/src/protocol.rs:534`),
     `HumanWorkerMessage` (`crates/core/src/protocol.rs:626`), and `BinaryWorkerMessage`
     (`crates/core/src/protocol.rs:705`):
     ```rust
     /// Sent immediately after `RegisterAck` when both worker and master support
     /// `SESSION_RESUME` and the worker still has tasks running or buffered from its previous session.
     ResumeSession {
         worker_id: Uuid,
         running: Vec<TaskId>,
     },
     ```
   - Per `docs/design/0040-protocol-versioning.md` (D1/D2), adding `WorkerMessage::ResumeSession`
     bumps `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` from `3` to `4` and
     updates `crates/core/tests/data/wire_layout.txt` via `UPDATE_WIRE_LAYOUT=1`.
   - Because `F1-b` performs the next required protocol version bump (`3 -> 4`), `F1-b` also removes
     the deprecated `compute_intensity: u32` field from `TaskSpec::GpuCompute`,
     `HumanTaskSpec::GpuCompute`, and `BinaryTaskSpec::GpuCompute` (`crates/core/src/task.rs:1041`,
     `:1091`, `:1149`, see `docs/design/0044-dead-code-and-connection-loop-unification.md` D1.5).
   - A worker sends `WorkerMessage::ResumeSession` **only** when the master's `RegisterAck`
     announces a non-zero `session-resume=<secs>`. On two-lane multiplexed QUIC connections,
     `WorkerMessage::ResumeSession` travels on the control stream (`STREAM_CONTROL = 0x01`,
     `LaneKind::Control` in `crates/master/src/session.rs`).
3. **Worker grace inequality, outbox buffering, and output blob upload (`crates/worker/src/client.rs`):**
   - **Why the worker's grace must be strictly shorter than the master's hold:**
     - When a silent network drop or master crash occurs at wall-clock time $t_0$, the master's
       `adoptable_by` timer starts either at master restart or when the master detects the drop,
       whereas the worker only exits `connect_and_run` after its own dead-peer liveness timeout
       $W_{\text{dead\_worker}} = \max(2\text{ s}, 4 \times \tau_{\text{worker}})$
       (`12 s` at the default $\tau_{\text{worker}} = 3\text{ s}$).
     - To guarantee that the worker kills its local task **before** the master's hold $T_{\text{hold}}$
       can expire and dispatch a second copy to another worker, the worker's post-disconnect grace
       budget $T_{\text{worker\_grace}}$ must satisfy:
       $$T_{\text{worker\_grace}} \le \max\bigl(0,\; T_{\text{hold}} - W_{\text{dead\_worker}} - T_{\text{margin}}\bigr)$$
       where $T_{\text{margin}} = 3\text{ s}$ covers clock drift and reconnect handshake latency.
     - With the defaults ($T_{\text{hold}} = 30\text{ s}$, $\tau_{\text{worker}} = 3\text{ s} \Rightarrow W_{\text{dead\_worker}} = 12\text{ s}$, $T_{\text{margin}} = 3\text{ s}$), the worker's grace after detecting the disconnect is $30 - 12 - 3 = 15\text{ s}$.
     - The worker retains `last_negotiated_hold: Option<Duration>` from the most recent `RegisterAck`.
       An optional worker flag `worker --session-resume-grace-secs <SECS>`
       (`RUSTY_GRID_SESSION_RESUME_GRACE_SECS`, `[worker].session_resume_grace_secs`,
       `WorkerConfig::session_resume_grace_cap: Option<Duration>`) **may only shorten** (clamp
       downward) the computed grace budget:
       ```rust
       let max_safe_grace = hold.saturating_sub(dead_peer_timeout).saturating_sub(RESUME_SAFETY_MARGIN);
       let effective_grace = match self.config.session_resume_grace_cap {
           Some(cap) => cap.min(max_safe_grace),
           None => max_safe_grace,
       };
       ```
       If `effective_grace.is_zero()` (for example, `session-resume` was not advertised, `--session-resume-grace-secs 0`
       was set, or `hold <= dead_peer_timeout + margin`), the worker cancels all active tasks
       immediately on disconnect.
   - **Disconnected outbox and output blobs (`pending_resume_outbox`):**
     - When `connect_and_run` exits due to a connection drop, EOF, heartbeat timeout, or master
       restart while `effective_grace > Duration::ZERO` and the worker has active tasks (and neither
       `*shutdown_rx.borrow()` nor `current_evacuation_reason()` is set), the worker keeps its
       active tasks running until `resume_deadline = Instant::now() + effective_grace`.
     - While disconnected, any `WorkerMessage::Checkpoint` or `WorkerMessage::TaskResult` produced
       by a running task is buffered in `pending_resume_outbox: Mutex<VecDeque<BufferedTaskEvent>>`
       (retaining at most the latest `Checkpoint` and the final `TaskResult` per `task_id`).
     - If a task that finished while disconnected produced output files (`output_globs`), the task
       runner stores the collected output blobs in the worker's local `BlobCache` (pinned by
       `task_id`) and attaches their `Vec<BlobFile>` metadata and local cache paths to the
       `BufferedTaskEvent::Result`.
     - During the reconnect backoff sleep (`crates/worker/src/client.rs:1515`), `WorkerClient::run`
       selects on `tokio::time::sleep_until(resume_deadline)`: if `resume_deadline` expires before
        a new session completes `RegisterAck` (or if a reconnect attempt starts before
       `resume_deadline` and only finishes `RegisterAck` after the master's `hold` has expired),
       the worker calls `self.cancel_all_active_tasks("session resume grace period expired").await`,
       unpins and clears `pending_resume_outbox`, and therefore never runs a duplicate copy alongside
       a newly scheduled worker.
   - **Post-`RegisterAck` resume sequence on the new session:**
     - If the new master session announces `session-resume=<secs>` and `Instant::now() < resume_deadline`:
       1. Collect `running: Vec<TaskId>` containing all currently running task IDs in `active_tasks`
          plus any task IDs with a buffered `TaskResult` in `pending_resume_outbox`.
       2. Send `WorkerMessage::ResumeSession { worker_id: self.worker_id, running }` as the first
          post-handshake message and read/process any immediate `MasterMessage::CancelTask` frames.
       3. For each buffered `TaskResult` whose `task_id` was not cancelled by the master and has
          non-empty `outputs: Vec<BlobFile>`, use the new session's `BlobClient` to upload any
          missing output blobs from the worker's `BlobCache` to the master's `BlobStore` **before**
          sending the `WorkerMessage::TaskResult` frame on the new session's `outbound_tx`.
       4. Flush all remaining non-cancelled buffered `Checkpoint` and `TaskResult` frames to
          `outbound_tx`.
     - Otherwise, cancel any carried-over active tasks and clear `pending_resume_outbox`.
4. **Master handling of `WorkerMessage::ResumeSession` (`crates/master/src/session.rs` and `crates/master/src/server.rs`):**
   - When the master receives `WorkerMessage::ResumeSession { worker_id, running }` on `session_id`
     (on the single TCP stream `LaneKind::Unified` or the QUIC control stream `LaneKind::Control`):
     - Verify `worker_id == registered_worker_id` (rejecting mismatched IDs as a protocol violation).
     - Call `queue.adopt_session_tasks(worker_id, session_id, &running, store.as_ref()).await`:
       - For each `task_id` in `running`:
         - If the task is in `TaskState::Queued` with `adoptable_by == Some((worker_id, hold_until))`
           where `Instant::now() < hold_until` (or still in `Scheduled`/`Running` from a superseded
           session of the same `worker_id`): transition it to `TaskState::Running` with
           `assigned_worker_id = Some(worker_id)` and `assigned_session = Some(session_id)`, clear
           `adoptable_by`, `fail_if_not_adopted`, and `pending_failover_charge`, add `task_id` to
           `worker_assignments[worker_id]`, remove it from `ready_queue` / `delayed_retries`, and
           append `JournalRecord::TaskAdopted`.
         - Otherwise (the task's `adoptable_by` hold already expired, or the task is unknown,
           `Cancelled`, `Completed`, `Failed`, or owned by a different worker): include `task_id`
           in `stale_to_cancel: Vec<TaskId>`.
       - For any task in `queue` that had `adoptable_by == Some((worker_id, _))` but was **not** in
         `running`: release its hold immediately via the deferred failover/failure rule in D3.2 and
         D4.5.
     - Update `WorkerRegistry` active task count for `(worker_id, session_id)` to match the number
       of adopted tasks, send `MasterMessage::CancelTask { task_id }` for each task in
       `stale_to_cancel`, and notify `scheduler_notify`.
5. **Handling short network drops on a master that stays up, and the cost of the hold:**
   - **Trade-off / cost:** When a `SESSION_RESUME`-capable worker disconnects while the master stays
     up, holding its in-flight tasks in `adoptable_by` for `--session-resume-hold-secs` (default
     `30 s`) means that if the worker genuinely crashed or lost power, failover redispatch of those
     tasks to another worker is delayed by `session_resume_hold` (`30 s`). Operators who want
     immediate failover on disconnect can set `--session-resume-hold-secs 0`.
   - **Exact failover-accounting state machine (`pending_failover_charge`):**
     - When a worker that negotiated `ProtocolCapabilities::SESSION_RESUME` disconnects while the
       master is running and `session_resume_hold > Duration::ZERO`,
       `TaskQueue::handle_session_disconnected` transitions the session's `Scheduled`/`Running`
       tasks to `TaskState::Queued` with `adoptable_by = Some((worker_id, now + session_resume_hold))`
       and `pending_failover_charge: bool = true` (plus `fail_if_not_adopted = true` if the task was
       `Running` with effective `max_retries == 0`), **without** incrementing `retry_count` or
       `failover_count` or appending a `RetryAttempt` yet.
     - If `worker_id` reconnects before `hold_until` and adopts `task_id` via `ResumeSession`,
       `pending_failover_charge` and `adoptable_by` are cleared; `retry_count` and `failover_count`
       stay untouched.
     - If `hold_until` expires without `worker_id` adopting `task_id` (checked by the scheduler/reaper
       via `TaskQueue::release_expired_adoption_holds(now)`) or if `worker_id` reconnects without
       listing `task_id` in `ResumeSession`:
       1. `adoptable_by` and `pending_failover_charge` are cleared.
       2. If `fail_if_not_adopted` is `true` (`Running` with `max_retries == 0`) or
          `!retry_policy.failover_budget(&entry).0`, the task transitions to `TaskState::Failed`,
          syncs `JournalRecord::TaskTerminal`, and wakes any completion waiters.
       3. Otherwise, `retry_count` and `failover_count` are incremented by `1`, a `RetryAttempt`
          audit entry is pushed to `retry_history`, `JournalRecord::TaskRequeued` is synced to disk,
          and the task becomes eligible in `ready_queue` for any worker.

## Compatibility

- **Wire protocol version:** Adding `WorkerMessage::ResumeSession` (and removing the deprecated
  `compute_intensity` field from `TaskSpec::GpuCompute`) in `F1-b` bumps `PROTOCOL_VERSION_CURRENT`
  and `PROTOCOL_VERSION_MIN_COMPATIBLE` from `3` to `4` in `crates/core/src/protocol.rs:31-33`, and
  adding `store_error: Option<String>` to `ClientResponse::ClusterStatus` in `F1-a` wiring (pull
  request #147, D3.3) bumps `PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE` from `4`
  to `5` (with `LegacyV3V4BinaryClientResponse` so a v5 client reports a clear
  `Incompatible protocol version 4` error from a v4 master) and updates
  `crates/core/tests/data/wire_layout.txt` (enforced by `crates/core/src/wire_layout_tests.rs`).
- **On-disk state across protocol version upgrades (path (a) — self-describing JSON snapshot on clean shutdown):**
  - Both `journal.bin` and `snapshot.bin` record `protocol_version: u32` (`PROTOCOL_VERSION_CURRENT`)
    in bytes `12..16` of their 24-byte file header (D2.1).
  - `snapshot.bin` is stored as UTF-8 JSON using the `Human*` serde representations (`#[serde(default)]`
    for added fields, unknown fields ignored on removal), and a clean master shutdown (`SIGTERM`,
    `SIGINT`, or `MasterHandle::shutdown`) always compacts all live and retained terminal tasks into
    `snapshot.bin` and truncates `journal.bin` to 0 records (D2.5).
  - On startup after a binary upgrade where `PROTOCOL_VERSION_CURRENT` changed:
    1. `TaskStore::open` reads `snapshot.bin` via `serde_json` even when the snapshot header's
       `protocol_version != PROTOCOL_VERSION_CURRENT`.
    2. If `journal.bin` has 0 records after its 24-byte header (the normal state after a clean stop),
       `TaskStore::open` rewrites the `snapshot.bin` and `journal.bin` headers with the new
       `PROTOCOL_VERSION_CURRENT` and startup succeeds with all tasks preserved.
    3. If `journal.bin` contains uncompacted bincode records **and** its header
       `protocol_version != PROTOCOL_VERSION_CURRENT` (meaning the old master crashed without
       compacting right before the binary was upgraded), `TaskStore::open` refuses to decode the
       incompatible bincode frames and returns a clear `GridError::Storage` message naming
       `<state_dir>/journal.bin`, the recorded `protocol_version`, and `PROTOCOL_VERSION_CURRENT`
       (and `DEPLOYMENT.md` 3.5 documents stopping the master cleanly before upgrading the binary).
- **Feature gating:** A worker sends `WorkerMessage::ResumeSession` only when `RegisterAck.message`
  contains `session-resume=<secs>` with `<secs> > 0`, and a master only sets the disconnect adoption
  hold for workers that advertised `ProtocolCapabilities::SESSION_RESUME`.
- **CLI and config defaults:** `--state-dir` is optional (`None` by default in `ServerConfig`), so
  all existing CLI invocations and test harnesses continue to work unchanged.

## Security

- **Sensitive data on disk in `<state_dir>`:**
  - `journal.bin` and `snapshot.bin` persist complete `Task` and `TaskResult` payloads in plaintext
    on disk, including task `env` variables, `stdin`, inline `staged_files`, `stdout`, and `stderr`.
    Any secret passed inside a task's environment variables, standard input, or staged files is
    written to `<state_dir>`. Users should not pass long-lived credentials in task `env` or `stdin`
    on unencrypted disks, and `<state_dir>` must be protected with the same access controls as
    `cluster.secret` (`README.md` and `docs/SECURITY.md`).
- **Filesystem permissions and symlink defense (Unix and Windows):**
  - On Unix, `<state_dir>` is created with mode `0o700`, checked with `symlink_metadata` to reject
    symlinks and foreign UIDs, and its files (`store.lock`, `journal.bin`, `snapshot.bin`) are
    created with mode `0o600`.
  - On Windows, `<state_dir>` is checked with `symlink_metadata` to reject symlinks/reparse points
    and restricted via `icacls` (`icacls <state_dir> /inheritance:r /grant:r ...`) to the current
    user SID, `SYSTEM`, and `Administrators`, matching the protection of `cluster.secret`.
  - `<state_dir>/store.lock` is held via `File::try_lock()` so a second process cannot corrupt or
    race the journal.
- **Bounded deserialization and integrity:** Every journal and snapshot frame is length-checked
  against `MAX_FRAME_SIZE` (`64 MiB`) and verified with BLAKE3 before deserialization.
- **Session ownership enforcement:** `ResumeSession` is only accepted after the connection has
  completed the `OXS-AUTH` cluster secret handshake (`crates/master/src/server.rs`) and `Register`
  for `worker_id`. A worker cannot adopt a task whose hold has expired or that is assigned to a
  different worker; attempting to do so causes the master to reply with `MasterMessage::CancelTask`
  for that `task_id`.

## Test plan

1. **Unit tests (`crates/master/src/store.rs`):**
   - Round-trip append and replay of `TaskSubmitted`, `TaskDispatched`, `TaskRunning`,
     `CheckpointRecorded`, `TaskRequeued`, `TaskAdopted`, `TaskTerminal`, and `TaskEvicted`.
   - Torn-tail recovery: append 5 records, truncate the last 7 bytes of `journal.bin` (or flip a
     byte in the final frame's payload), reopen `TaskStore`, and verify the first 4 records replay
     cleanly and `journal.bin` is truncated to the 4th frame boundary.
   - Mid-file corruption rejection: flip a byte in the 2nd of 4 frames and verify `TaskStore::open`
     returns `Err(GridError::Storage(_))` rather than silently skipping the frame.
   - Compaction (`snapshot.bin`) and cross-protocol-version upgrade test:
     1. Write records, compact to JSON `snapshot.bin` with header `protocol_version = 3` and empty
        `journal.bin`, reopen under `PROTOCOL_VERSION_CURRENT` (`4`), and verify the snapshot loads
        cleanly and updates the header version.
     2. Write non-empty `journal.bin` with header `protocol_version = 2` (simulating an uncompacted
        crash across a protocol bump), reopen, and verify `TaskStore::open` returns
        `Err(GridError::Storage(_))` naming the recorded version and `PROTOCOL_VERSION_CURRENT`.
   - Group-commit benchmark/unit test: submit 1,000 tasks concurrently to a master with
     `--state-dir`, verify all 1,000 submissions succeed and are persisted in `journal.bin`, and
     record the elapsed wall-clock time.
   - Concurrent lock rejection: opening a second `TaskStore` on the same `<state_dir>` fails while
     the first lock is held.
2. **Unit tests (`crates/master/src/queue.rs` and `crates/core/src/protocol.rs`):**
   - `WorkerMessage::ResumeSession` round-trip in both Bincode and JSON codecs, plus
     `crates/core/src/wire_layout_tests.rs` golden layout verification (with `compute_intensity`
     removed in `F1-b`).
   - Replay of `Scheduled` and `Running` tasks transitions them to `Queued` with `retry_count == 0`
     and `failover_count == 0`, sets `adoptable_by`, holds them out of `queued_task_views` until
     `hold_until`, and adopts them back into `Running` when `adopt_session_tasks` is called.
   - Replay of a `Running` task with `max_retries == 0` that nobody resumes within the hold ends in
     `TaskState::Failed` with reason `"master restarted while the task ran; not re-run because max_retries is 0"`.
3. **End-to-end integration tests (`tests/master_restart_recovery.rs`, registered in `crates/master/Cargo.toml`):**
   - **Test 1 (`test_master_restart_recovers_finished_queued_and_adopts_running_task_once`):**
     - Start a master with `--state-dir <tmp>` and one connected worker.
     - Submit 4 tasks: `t_done` (completes before kill), `t_run` (writes an execution counter to a
       marker file and runs across the restart), and `t_q1`, `t_q2` (queued behind `t_run`).
     - Kill the first master and restart a second master on the same port and `--state-dir <tmp>`.
     - Verify `get_task_result(t_done)` reads back immediately, `t_run` is adopted via
       `ResumeSession` and finishes with marker count `== 1` (ran **exactly once**), and `t_q1`,
       `t_q2` both complete.
   - **Test 2 (`test_worker_reconnecting_after_master_hold_expires_runs_task_only_once`):**
     - Configure a short master hold (`--session-resume-hold-secs 6`) with worker heartbeat `1 s`
       ($W_{\text{dead\_worker}} = 2\text{ s}$, $T_{\text{margin}} = 3\text{ s} \Rightarrow T_{\text{worker\_grace}} = 1\text{ s}$),
       delay the first worker's reconnect until after the master's `6 s` hold has expired, and
       verify via the marker file that the first worker stopped its local task before `hold` expired
       and the task ran **exactly once** on the second worker (or after reconnect).
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
| F1-design | Design record `0046` for master durability and worker session resume | `docs/design/0046-master-durability.md`, `docs/design/0044-dead-code-and-connection-loop-unification.md`, `docs/README.md` | Design record reviewed and accepted by the owner | Implemented |
| F1-a | Append-only task journal, `5 ms` group commit, self-describing JSON snapshot compaction, `GridError::Storage`, `--state-dir`, and startup replay (`Scheduled`/`Running` -> `Queued` uncharged, `Running` with `max_retries == 0` -> `Failed` when unresumed, terminal results & queued tasks restored, blob re-pinning) | `crates/core/src/error.rs`, `crates/master/src/store.rs`, `crates/master/src/lib.rs`, `crates/master/src/queue.rs`, `crates/master/src/scheduler.rs`, `crates/master/src/server.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `tests/master_restart_recovery.rs`, `README.md`, `docs/SECURITY.md`, `DEPLOYMENT.md` | Unit tests in `crates/master/src/store.rs`, `queue.rs`, and `server.rs` plus `tests/master_restart_recovery.rs` pass (round-trip, torn-tail recovery, mid-file corruption error, JSON snapshot across protocol versions, uncompacted version-mismatch error, 1,000-task group-commit timing test, lock exclusion, uncharged replay, `Running` + `max_retries == 0` unresumed failure, blob re-pinning, and end-to-end master restart recovery) | Implemented |
| F1-b wire | `ProtocolCapabilities::SESSION_RESUME` (defined, not yet advertised in `all_supported()`), `master_features` `session-resume=<secs>` with `MAX_SESSION_RESUME_HOLD_SECS = 3_600`, `WorkerMessage::ResumeSession` (accepted on `LaneKind::Unified \| LaneKind::Control`), and wire removal of deprecated `compute_intensity` (protocol bump `3 -> 4`) | `crates/core/src/protocol.rs`, `crates/core/src/task.rs`, `crates/core/src/wire_layout_tests.rs`, `crates/core/tests/data/wire_layout.txt`, `crates/master/src/session.rs`, `crates/master/src/server.rs`, `crates/worker/src/runner.rs`, `DEPLOYMENT.md`, `README.md`, `CLAUDE.md`, `docs/BACKLOG.md`, `docs/design/0044-dead-code-and-connection-loop-unification.md`, `docs/design/0046-master-durability.md` | Wire round-trip, hold-bound, and golden-bytes tests pass at protocol version 4; `SESSION_RESUME` is not advertised in `all_supported()` yet; `compute_intensity` is removed from `TaskSpec::GpuCompute` | Implemented |
| F1-b master adoption | `--session-resume-hold-secs`, advertising `SESSION_RESUME` / `session-resume=<secs>`, and master task adoption + `adoptable_by` / `pending_failover_charge` | `crates/core/src/protocol.rs`, `crates/master/src/queue.rs`, `crates/master/src/session.rs`, `crates/master/src/server.rs`, `crates/master/src/registry.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs` | Master unit tests prove `adoptable_by` holds replayed/disconnected running tasks from duplicate dispatch, `ResumeSession` adopts them into `Running`, expired holds charge deferred failovers or fail `max_retries == 0` tasks, and unknown/cancelled/expired tasks receive `CancelTask` | Planned |
| F1-c | Worker session-resume grace inequality (`hold - dead_peer_timeout - margin`, `--session-resume-grace-secs` clamp), disconnected outbox buffering with `BlobClient` output blob upload before `TaskResult`, and `tests/master_restart_recovery.rs` | `crates/worker/src/client.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/config.rs`, `crates/master/Cargo.toml`, `tests/master_restart_recovery.rs`, `README.md`, `docs/design/0046-master-durability.md` | `cargo test -p rusty_grid_master --test master_restart_recovery --locked` passes both the 4-task master restart recovery test and the expired-hold reconnect test, proving every task runs **exactly once** | Planned |

## Status log

- **2026-11-08:** Draft created (`0046-master-durability.md`) for `FEAT-1` per Working Rule 7.
- **2026-11-08:** Updated per owner review (`122-fix`): made `--session-resume-hold-secs` on the
  master the source of truth (`session-resume=<secs>` in `RegisterAck`) with worker grace bounded by
  `hold - dead_peer_timeout - margin`; recorded `PROTOCOL_VERSION_CURRENT` in file headers and made
  `snapshot.bin` self-describing JSON compacted on clean shutdown; failed unresumed `Running` tasks
  that have `max_retries == 0` and synced `TaskRequeued`; added `BlobClient` output-blob upload
  before flushing a buffered `TaskResult`; added `5 ms` group commit for submissions; documented
  plaintext task data on disk and Windows `icacls` owner-only ACLs in Security; defined deferred
  `pending_failover_charge` accounting and hold trade-off in D4.5; and scheduled the wire removal of
  deprecated `compute_intensity` in `F1-b` alongside the protocol version `3 -> 4` bump.
- **2026-11-08:** `F1-a` module landed: `crates/master/src/store.rs` (`TaskStore` journal, 5 ms group
  commit, JSON snapshot compaction, torn-tail recovery, `store.lock`) and `GridError::Storage`, not
  yet wired into `TaskQueue`, `MasterServer` or the CLI. Windows owner-only ACL is still a TODO.
- **2026-11-08:** Review fixes to the `store.rs` format (v1 is frozen with this change): 24-byte headers
  carry a `u64` generation (a journal older than the snapshot is discarded after an interrupted
  compaction); the snapshot frame has a `u64` length (4 GiB bound; `Bytes` keep the core JSON form,
  base64 needs a core serde change and is deferred); a bad journal frame is a torn tail only if no
  valid frame follows it, otherwise `open` refuses; a zero-filled tail is truncated; durable appends
  enqueue synchronously and return a `DurableAck`; the journal byte layout is recorded in
  `crates/master/tests/data/journal_layout.txt` and a change requires bumping `STORE_FORMAT_VERSION`.
- **2026-11-08:** Review round 2 (still format v1): journal frames gain `len_check: u32` (a bijective
  mix of `payload_len`). Header valid and payload past EOF is a torn tail with no scan; an invalid
  header or a BLAKE3 mismatch probes only offsets whose `len_check` passes, so classification is
  linear and a torn payload that embeds journal bytes still opens as a torn tail. `compact` takes
  the `SnapshotImage` by value and encodes it on the writer thread. Moving snapshot `Bytes` to base64
  later needs a `STORE_FORMAT_VERSION` bump (the reader takes a JSON string as raw UTF-8).
- **2026-11-08:** Implemented `F1-b wire` (PR #141): bumped `PROTOCOL_VERSION_CURRENT` and
  `PROTOCOL_VERSION_MIN_COMPATIBLE` from `3` to `4`, added `ProtocolCapabilities::SESSION_RESUME`
  (`1 << 12`, kept out of `all_supported()` until adoption and worker resume land), added
  `master_features` `session-resume=<secs>` helpers bounded by `MAX_SESSION_RESUME_HOLD_SECS = 3_600`,
  appended `WorkerMessage::ResumeSession` (routed on `LaneKind::Unified | LaneKind::Control`),
  removed `TaskSpec::GpuCompute::compute_intensity` from the wire, and regenerated `wire_layout.txt`.
- **2026-11-08:** Implemented `F1-a` wiring (PR #147): wired `TaskStore` into `TaskQueue` (journal
  emission with durable `DurableAck` awaited in a spawned task outside the queue write lock, startup
  replay of all D2.5 task states, `adoptable_by` / `fail_if_not_adopted` hold handling, input/output
  blob re-pinning, and threshold/shutdown snapshot compaction), `MasterServer`
  (`ServerConfig::with_state_dir`, default `<state_dir>/blobs` blob store, `TaskStore::open` before
  binding listeners, shutdown compaction), `oxideswarm master --state-dir` (`RUSTY_GRID_STATE_DIR`,
  `[master].state_dir`), store-health visibility (`TaskStore::failure_reason`, `QueueStats::store_error`,
  `ClientResponse::ClusterStatus.store_error` in protocol version 5 with `LegacyV3V4BinaryClientResponse`
  fallback, `ClusterStatusDto::store_error`, `#storeErrorBanner` in `dashboard.html`, and `C1 Task store`
  failure check in `oxideswarm doctor`), and `tests/master_restart_recovery.rs`.
