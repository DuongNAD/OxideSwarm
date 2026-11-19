//! Master-local bounded, append-only, display-only contribution ledger (`NET-10`, design 0096).
//!
//! Tracks compute work (`UsageKind::Task`) and model-tunnel usage (`UsageKind::ModelTunnel`,
//! `LLM-10`) using internal double-entry conservation keyed by immutable member identity
//! (`runner_member_key` / `submitter_member_key`, or `"organiser"` for v2 / local-host peers).
//! Participant display labels are resolved at display time so history never splits when labels are
//! added or changed, and self-run work (`runner == submitter` under the resolved participant) is
//! isolated into `self_run` while cross-participant conservation (`sum(ran_for_others) ==
//! sum(used_from_others)`) holds identically over `24h`, `7d`, and `all_time`.
//!
//! Per Q96 / design 0096 D4, contribution records are strictly display-only: the scheduler, queue
//! admission, fair share, and worker client never read or act on them, and ledger I/O errors are
//! quarantined / logged rather than stopping the master.
//!
//! The per-line BLAKE3 digest in `credits.journal.jsonl` (and `credits.snapshot.json`) is a
//! local integrity checksum, not a hash chain: it detects torn or accidental writes, not
//! tampering by anyone who already has write access to `--state-dir`.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::task::TaskId;
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// Snapshot file written under `--state-dir` when persistence is enabled.
pub const CREDITS_SNAPSHOT_FILE: &str = "credits.snapshot.json";
/// Append-only NDJSON journal written under `--state-dir` when persistence is enabled.
pub const CREDITS_JOURNAL_FILE: &str = "credits.journal.jsonl";
/// On-disk snapshot schema version.
pub const CREDITS_SNAPSHOT_VERSION: u32 = 1;
/// Reserved key for v2 secret peers and unauthenticated local/dashboard submissions.
pub const ORGANISER_PARTICIPANT: &str = "organiser";
/// Default cap on retained raw [`UsageRecord`] entries before compaction rolls oldest entries into
/// `archived_pairs` (`--max-credit-records`).
pub const DEFAULT_MAX_CREDIT_RECORDS: usize = 100_000;
/// Default byte budget for retained credits state (`--max-credit-bytes`, 32 MiB).
pub const DEFAULT_MAX_CREDIT_BYTES: u64 = 32 * 1024 * 1024;
/// Maximum single-task / single-tunnel duration credited (7 days, matching master submit max).
pub const MAX_CREDIT_DURATION_MS: u64 = 7 * 24 * 3600 * 1000;
/// Maximum CPU cores credited per task (matches dashboard/submit ceiling).
const MAX_CREDIT_CORES: usize = 4096;
/// 24-hour aggregation window in seconds.
pub const WINDOW_24H_SECS: u64 = 86_400;
/// 7-day aggregation window in seconds.
pub const WINDOW_7D_SECS: u64 = 604_800;

const GROUP_COMMIT_WINDOW: Duration = Duration::from_millis(5);
const MAX_GROUP_BATCH: usize = 256;
const DOMAIN_SNAPSHOT_V1: &[u8] = b"oxideswarm-credits-snapshot-v1\0";
const DOMAIN_JOURNAL_V1: &[u8] = b"oxideswarm-credits-v1\0";
const DISPLAY_ONLY_NOTE: &str =
    "Display only (Q96): contribution records never affect scheduling, admission, or quotas.";

/// Clock source for recording timestamps and computing 24h / 7d windows.
pub trait LedgerClock: Send + Sync {
    /// Returns current Unix epoch time in seconds.
    fn now_utc(&self) -> u64;
}

/// System wall-clock implementation of [`LedgerClock`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemLedgerClock;

impl LedgerClock for SystemLedgerClock {
    fn now_utc(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

/// Bounding parameters for [`CreditsLedger`]. Values below 1 are clamped to 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditsConfig {
    /// Maximum raw [`UsageRecord`] entries retained before low-water compaction (~50%).
    pub max_records: usize,
    /// Combined byte budget for `credits.snapshot.json` + `credits.journal.jsonl` (or in-memory
    /// retained estimate when no `--state-dir` is configured) before low-water compaction.
    pub max_bytes: u64,
}

impl Default for CreditsConfig {
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MAX_CREDIT_RECORDS,
            max_bytes: DEFAULT_MAX_CREDIT_BYTES,
        }
    }
}

impl CreditsConfig {
    fn sanitized(self) -> Self {
        Self {
            max_records: self.max_records.max(1),
            max_bytes: self.max_bytes.max(1),
        }
    }

    fn low_water_records(&self) -> usize {
        if self.max_records <= 1 {
            self.max_records
        } else {
            (self.max_records / 2).max(1)
        }
    }

    fn low_water_bytes(&self) -> u64 {
        if self.max_bytes <= 1 {
            self.max_bytes
        } else {
            (self.max_bytes / 2).max(1)
        }
    }
}

/// One persisted usage event shared by compute tasks (`NET-10`) and model tunnels (`LLM-10`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Monotonic master-assigned sequence starting at 1.
    pub record_id: u64,
    /// Unix epoch seconds when recorded on the master.
    pub recorded_at_utc: u64,
    /// Worker owner's member id, or `"organiser"` for v2/unauthenticated workers.
    pub runner_member_key: String,
    /// Submitter's member id, or `"organiser"` for v2/local/dashboard submissions.
    pub submitter_member_key: String,
    /// Specific workload payload.
    pub kind: UsageKind,
}

/// Payload variant inside a [`UsageRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsageKind {
    Task {
        task_id: TaskId,
        worker_id: Uuid,
        duration_ms: u64,
        cpu_cores: usize,
        gpu_reserved: bool,
        exit_code: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        verified: Option<bool>,
    },
    ModelTunnel {
        tunnel_id: Uuid,
        service: String,
        worker_id: Uuid,
        duration_ms: u64,
        bytes_to_worker: u64,
        bytes_from_worker: u64,
        close_reason: String,
    },
}

/// Input passed by the master when booking a terminal task execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskUsageInput {
    pub task_id: TaskId,
    pub worker_id: Uuid,
    pub duration_ms: u64,
    pub cpu_cores: usize,
    pub gpu_reserved: bool,
    pub exit_code: i32,
    pub verified: Option<bool>,
    pub runner_member_id: Option<String>,
    pub submitter_member_id: Option<String>,
}

/// Input for recording a completed model-tunnel session (`LLM-10`, design 0061).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelTunnelUsageInput {
    pub tunnel_id: Uuid,
    pub service: String,
    pub worker_id: Uuid,
    pub duration_ms: u64,
    pub bytes_to_worker: u64,
    pub bytes_from_worker: u64,
    pub close_reason: String,
    pub runner_member_id: Option<String>,
    pub submitter_member_id: Option<String>,
}

/// Cumulative counters for one bucket (`ran_for_others`, `used_from_others`, or `self_run`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideCounters {
    pub tasks: u128,
    pub core_ms: u128,
    pub gpu_ms: u128,
    #[serde(default)]
    pub tunnel_sessions: u128,
    #[serde(default)]
    pub tunnel_duration_ms: u128,
    #[serde(default)]
    pub tunnel_bytes: u128,
}

impl SideCounters {
    fn saturating_add_assign(&mut self, rhs: Self) {
        self.tasks = self.tasks.saturating_add(rhs.tasks);
        self.core_ms = self.core_ms.saturating_add(rhs.core_ms);
        self.gpu_ms = self.gpu_ms.saturating_add(rhs.gpu_ms);
        self.tunnel_sessions = self.tunnel_sessions.saturating_add(rhs.tunnel_sessions);
        self.tunnel_duration_ms = self
            .tunnel_duration_ms
            .saturating_add(rhs.tunnel_duration_ms);
        self.tunnel_bytes = self.tunnel_bytes.saturating_add(rhs.tunnel_bytes);
    }

    fn from_kind(kind: &UsageKind) -> Self {
        match kind {
            UsageKind::Task {
                duration_ms,
                cpu_cores,
                gpu_reserved,
                verified,
                ..
            } => {
                if *verified == Some(false) {
                    return Self::default();
                }
                let clamped_ms = (*duration_ms).min(MAX_CREDIT_DURATION_MS);
                let cores = u128::from((*cpu_cores).clamp(1, MAX_CREDIT_CORES) as u64);
                let core_ms = u128::from(clamped_ms) * cores;
                // GPU-seconds only count when the task actually reserved a GPU (`design 0094 D8`).
                let gpu_ms = if *gpu_reserved {
                    u128::from(clamped_ms)
                } else {
                    0
                };
                Self {
                    tasks: 1,
                    core_ms,
                    gpu_ms,
                    tunnel_sessions: 0,
                    tunnel_duration_ms: 0,
                    tunnel_bytes: 0,
                }
            }
            UsageKind::ModelTunnel {
                duration_ms,
                bytes_to_worker,
                bytes_from_worker,
                ..
            } => Self {
                tasks: 0,
                core_ms: 0,
                gpu_ms: 0,
                tunnel_sessions: 1,
                tunnel_duration_ms: u128::from((*duration_ms).min(MAX_CREDIT_DURATION_MS)),
                tunnel_bytes: u128::from(*bytes_to_worker)
                    .saturating_add(u128::from(*bytes_from_worker)),
            },
        }
    }

    fn is_zero(&self) -> bool {
        self.tasks == 0
            && self.core_ms == 0
            && self.gpu_ms == 0
            && self.tunnel_sessions == 0
            && self.tunnel_duration_ms == 0
            && self.tunnel_bytes == 0
    }
}

/// Internal per-row buckets across a window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WindowBuckets {
    ran_for_others: SideCounters,
    used_from_others: SideCounters,
    self_run: SideCounters,
}

/// JSON DTO for one usage bucket (`ran_for_others`, `used_from_others`, or `self_run`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CreditCountersDto {
    pub tasks: u128,
    pub core_ms: u128,
    pub core_secs: f64,
    pub gpu_ms: u128,
    pub gpu_secs: f64,
    pub tunnel_sessions: u128,
    pub tunnel_duration_ms: u128,
    pub tunnel_bytes: u128,
}

impl From<SideCounters> for CreditCountersDto {
    fn from(c: SideCounters) -> Self {
        Self {
            tasks: c.tasks,
            core_ms: c.core_ms,
            core_secs: (c.core_ms as f64) / 1000.0,
            gpu_ms: c.gpu_ms,
            gpu_secs: (c.gpu_ms as f64) / 1000.0,
            tunnel_sessions: c.tunnel_sessions,
            tunnel_duration_ms: c.tunnel_duration_ms,
            tunnel_bytes: c.tunnel_bytes,
        }
    }
}

impl CreditCountersDto {
    fn to_counters(&self) -> SideCounters {
        SideCounters {
            tasks: self.tasks,
            core_ms: self.core_ms,
            gpu_ms: self.gpu_ms,
            tunnel_sessions: self.tunnel_sessions,
            tunnel_duration_ms: self.tunnel_duration_ms,
            tunnel_bytes: self.tunnel_bytes,
        }
    }
}

/// Q96 window metrics for one participant (no signed net column).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CreditWindowDto {
    pub ran_for_others: CreditCountersDto,
    pub used_from_others: CreditCountersDto,
    pub self_run: CreditCountersDto,
}

impl From<WindowBuckets> for CreditWindowDto {
    fn from(w: WindowBuckets) -> Self {
        Self {
            ran_for_others: CreditCountersDto::from(w.ran_for_others),
            used_from_others: CreditCountersDto::from(w.used_from_others),
            self_run: CreditCountersDto::from(w.self_run),
        }
    }
}

/// Per-participant row returned by `GET /api/credits`.
///
/// For `AuthScope::Observer` callers, `member_ids` is omitted (`empty`) and `label` never reveals
/// raw member ids or machine names (showing only explicit participant labels, `"organiser"`, or
/// neutral `"participant-N"` placeholders). `AuthScope::Admin` callers also receive `member_ids`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParticipantCreditsDto {
    pub label: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member_ids: Vec<String>,
    pub last_24h: CreditWindowDto,
    pub last_7d: CreditWindowDto,
    pub all_time: CreditWindowDto,
}

/// Complete read-only ledger snapshot returned by `GET /api/credits`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditsSnapshot {
    pub display_only: bool,
    pub note: String,
    pub generated_at_utc: u64,
    pub total_records: u64,
    pub retained_records: usize,
    pub archived_records: u64,
    pub window_truncated: bool,
    pub totals_24h: CreditWindowDto,
    pub totals_7d: CreditWindowDto,
    pub totals_all_time: CreditWindowDto,
    pub participants: Vec<ParticipantCreditsDto>,
}

impl CreditsSnapshot {
    /// Empty snapshot when a router is constructed without a master handle.
    pub fn empty(generated_at_utc: u64) -> Self {
        Self {
            display_only: true,
            note: DISPLAY_ONLY_NOTE.to_string(),
            generated_at_utc,
            total_records: 0,
            retained_records: 0,
            archived_records: 0,
            window_truncated: false,
            totals_24h: CreditWindowDto::default(),
            totals_7d: CreditWindowDto::default(),
            totals_all_time: CreditWindowDto::default(),
            participants: Vec::new(),
        }
    }

    /// Verifies that across every time window (`24h`, `7d`, `all_time`), cluster-wide work run for
    /// others equals work used from others across every metric, and matches `totals_*`.
    pub fn conservation_holds(&self) -> bool {
        let windows = [
            (
                &self.totals_24h,
                self.participants
                    .iter()
                    .map(|p| &p.last_24h)
                    .collect::<Vec<_>>(),
            ),
            (
                &self.totals_7d,
                self.participants
                    .iter()
                    .map(|p| &p.last_7d)
                    .collect::<Vec<_>>(),
            ),
            (
                &self.totals_all_time,
                self.participants
                    .iter()
                    .map(|p| &p.all_time)
                    .collect::<Vec<_>>(),
            ),
        ];

        for (expected, rows) in windows {
            let mut sum_ran = SideCounters::default();
            let mut sum_used = SideCounters::default();
            let mut sum_self = SideCounters::default();
            for row in rows {
                sum_ran.saturating_add_assign(row.ran_for_others.to_counters());
                sum_used.saturating_add_assign(row.used_from_others.to_counters());
                sum_self.saturating_add_assign(row.self_run.to_counters());
            }
            if sum_ran != sum_used
                || sum_ran != expected.ran_for_others.to_counters()
                || sum_used != expected.used_from_others.to_counters()
                || sum_self != expected.self_run.to_counters()
            {
                return false;
            }
        }
        true
    }
}

/// Pairwise archived totals `[runner_member_key][submitter_member_key] -> SideCounters`,
/// preserving full cross-member detail so display-time label resolution never splits history.
type ArchivedPairsMap = BTreeMap<String, BTreeMap<String, SideCounters>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CreditsSnapshotFile {
    version: u32,
    next_record_id: u64,
    archived_records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_evicted_recorded_at_utc: Option<u64>,
    #[serde(default)]
    archived_pairs: ArchivedPairsMap,
    retained: Vec<UsageRecord>,
}

impl Default for CreditsSnapshotFile {
    fn default() -> Self {
        Self {
            version: CREDITS_SNAPSHOT_VERSION,
            next_record_id: 1,
            archived_records: 0,
            max_evicted_recorded_at_utc: None,
            archived_pairs: ArchivedPairsMap::new(),
            retained: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotHeader {
    version: u32,
    blake3: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalHeader {
    seq: u64,
    blake3: String,
}

/// Row grouping identity when resolving raw member keys to display rows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RowGroupKey {
    Organiser,
    Label(String),
    UnlabeledMember(String),
}

struct RowMeta {
    label: String,
    member_ids: BTreeSet<String>,
}

fn record_pair(
    archived_pairs: &mut ArchivedPairsMap,
    runner_key: &str,
    submitter_key: &str,
    delta: SideCounters,
) {
    if delta.is_zero() {
        return;
    }
    archived_pairs
        .entry(runner_key.to_string())
        .or_default()
        .entry(submitter_key.to_string())
        .or_default()
        .saturating_add_assign(delta);
}

#[derive(Default)]
struct CoreLedgerState {
    config: CreditsConfig,
    next_record_id: u64,
    archived_records: u64,
    max_evicted_recorded_at_utc: Option<u64>,
    archived_pairs: ArchivedPairsMap,
    retained: VecDeque<UsageRecord>,
    retained_estimated_bytes: u64,
    last_archived_record_id: Option<u64>,
}

impl CoreLedgerState {
    fn new(config: CreditsConfig) -> Self {
        Self {
            config: config.sanitized(),
            next_record_id: 1,
            archived_records: 0,
            max_evicted_recorded_at_utc: None,
            archived_pairs: ArchivedPairsMap::new(),
            retained: VecDeque::new(),
            retained_estimated_bytes: 0,
            last_archived_record_id: None,
        }
    }

    fn push_retained(&mut self, rec: UsageRecord) {
        self.next_record_id = self.next_record_id.max(rec.record_id.saturating_add(1));
        self.retained_estimated_bytes = self
            .retained_estimated_bytes
            .saturating_add(estimate_record_bytes(&rec));
        self.retained.push_back(rec);
    }

    fn fold_oldest(&mut self) -> bool {
        let Some(rec) = self.retained.pop_front() else {
            return false;
        };
        let delta = SideCounters::from_kind(&rec.kind);
        record_pair(
            &mut self.archived_pairs,
            &rec.runner_member_key,
            &rec.submitter_member_key,
            delta,
        );
        self.archived_records = self.archived_records.saturating_add(1);
        self.max_evicted_recorded_at_utc = Some(
            self.max_evicted_recorded_at_utc
                .map_or(rec.recorded_at_utc, |prev| prev.max(rec.recorded_at_utc)),
        );
        self.last_archived_record_id = Some(
            self.last_archived_record_id
                .map_or(rec.record_id, |prev| prev.max(rec.record_id)),
        );
        self.retained_estimated_bytes = self
            .retained_estimated_bytes
            .saturating_sub(estimate_record_bytes(&rec));
        true
    }

    fn apply_compaction_until(&mut self, cutoff_record_id: u64) {
        while self
            .retained
            .front()
            .is_some_and(|rec| rec.record_id <= cutoff_record_id)
        {
            if !self.fold_oldest() {
                break;
            }
        }
    }

    fn compact_in_memory_if_needed(&mut self, compaction_count: &AtomicU64) {
        if self.enforce_ram_bounds() {
            compaction_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn enforce_ram_bounds(&mut self) -> bool {
        let over_records = self.retained.len() > self.config.max_records;
        let over_bytes =
            self.retained_estimated_bytes > self.config.max_bytes && !self.retained.is_empty();
        if !over_records && !over_bytes {
            return false;
        }
        let target_records = self.config.low_water_records();
        let target_bytes = self.config.low_water_bytes();
        while self.retained.len() > target_records
            || (self.retained_estimated_bytes > target_bytes && !self.retained.is_empty())
        {
            if !self.fold_oldest() {
                break;
            }
        }
        true
    }

    fn to_snapshot_file(&self) -> CreditsSnapshotFile {
        CreditsSnapshotFile {
            version: CREDITS_SNAPSHOT_VERSION,
            next_record_id: self.next_record_id,
            archived_records: self.archived_records,
            max_evicted_recorded_at_utc: self.max_evicted_recorded_at_utc,
            archived_pairs: self.archived_pairs.clone(),
            retained: self.retained.iter().cloned().collect(),
        }
    }
}

struct PersistentDiskState {
    core: CoreLedgerState,
    snapshot_path: PathBuf,
    journal_path: PathBuf,
    journal_file: File,
    snapshot_bytes: u64,
    journal_bytes: u64,
}

impl PersistentDiskState {
    fn needs_compaction(&self) -> bool {
        if self.core.retained.len() > self.core.config.max_records {
            return true;
        }
        self.snapshot_bytes.saturating_add(self.journal_bytes) > self.core.config.max_bytes
            && (self.core.retained.len() > 1 || self.journal_bytes > 0)
    }

    fn compact(
        &mut self,
        compaction_count: &AtomicU64,
        mem: &Mutex<CoreLedgerState>,
    ) -> GridResult<()> {
        if !self.needs_compaction() {
            return Ok(());
        }
        let target_records = self.core.config.low_water_records();
        let target_bytes = self.core.config.low_water_bytes();

        while self.core.retained.len() > target_records {
            if !self.core.fold_oldest() {
                break;
            }
        }

        let mut snapshot_file = self.core.to_snapshot_file();
        let mut encoded = encode_snapshot_bytes(&snapshot_file)?;
        while (encoded.len() as u64) > target_bytes && !self.core.retained.is_empty() {
            let excess = (encoded.len() as u64).saturating_sub(target_bytes);
            let step = if excess > 4096 {
                (self.core.retained.len() / 4).max(1)
            } else {
                1
            };
            for _ in 0..step {
                if !self.core.fold_oldest() {
                    break;
                }
            }
            snapshot_file = self.core.to_snapshot_file();
            encoded = encode_snapshot_bytes(&snapshot_file)?;
        }

        refuse_symlink(&self.snapshot_path)?;
        refuse_symlink(&self.journal_path)?;
        write_private_atomic(&self.snapshot_path, &encoded)?;

        self.journal_file.set_len(0).map_err(|e| {
            GridError::Config(format!(
                "Cannot truncate credits journal '{}': {e}",
                self.journal_path.display()
            ))
        })?;
        self.journal_file.seek(SeekFrom::Start(0)).map_err(|e| {
            GridError::Config(format!(
                "Cannot rewind credits journal '{}': {e}",
                self.journal_path.display()
            ))
        })?;
        self.journal_file.sync_all().map_err(|e| {
            GridError::Config(format!(
                "Cannot sync truncated credits journal '{}': {e}",
                self.journal_path.display()
            ))
        })?;
        self.snapshot_bytes = encoded.len() as u64;
        self.journal_bytes = 0;

        compaction_count.fetch_add(1, Ordering::Relaxed);
        if let Some(cutoff_id) = self.core.last_archived_record_id {
            let mut guard = mem.lock().unwrap_or_else(|e| e.into_inner());
            guard.apply_compaction_until(cutoff_id);
        }
        Ok(())
    }
}

enum FlushReply {
    Sync(mpsc::SyncSender<GridResult<()>>),
    Async(tokio::sync::oneshot::Sender<GridResult<()>>),
}

impl FlushReply {
    fn send(self, res: GridResult<()>) {
        match self {
            FlushReply::Sync(tx) => {
                let _ = tx.send(res);
            }
            FlushReply::Async(tx) => {
                let _ = tx.send(res);
            }
        }
    }
}

enum WriterCmd {
    Append(UsageRecord),
    Flush(FlushReply),
}

struct DiskWriter {
    disk: PersistentDiskState,
    mem: Arc<Mutex<CoreLedgerState>>,
    compaction_count: Arc<AtomicU64>,
    #[cfg(test)]
    inject_fail_next_append: Arc<AtomicBool>,
}

impl DiskWriter {
    fn run(mut self, rx: mpsc::Receiver<WriterCmd>) {
        while let Some((records, flushes, disconnected)) = Self::recv_batch(&rx) {
            let write_res = self.write_and_sync_batch(&records);
            for reply in flushes {
                let res = match &write_res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(GridError::Config(e.to_string())),
                };
                reply.send(res);
            }
            if disconnected {
                break;
            }
        }
    }

    fn recv_batch(
        rx: &mpsc::Receiver<WriterCmd>,
    ) -> Option<(Vec<UsageRecord>, Vec<FlushReply>, bool)> {
        let first = rx.recv().ok()?;
        let mut records = Vec::new();
        let mut flushes = Vec::new();
        let mut disconnected = false;
        Self::push_cmd(first, &mut records, &mut flushes);

        let deadline = Instant::now() + GROUP_COMMIT_WINDOW;
        loop {
            loop {
                match rx.try_recv() {
                    Ok(cmd) => Self::push_cmd(cmd, &mut records, &mut flushes),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
            if disconnected || !flushes.is_empty() || records.len() >= MAX_GROUP_BATCH {
                break;
            }
            let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match rx.recv_timeout(rem) {
                Ok(cmd) => Self::push_cmd(cmd, &mut records, &mut flushes),
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        while let Ok(cmd) = rx.try_recv() {
            Self::push_cmd(cmd, &mut records, &mut flushes);
        }

        Some((records, flushes, disconnected))
    }

    fn push_cmd(cmd: WriterCmd, records: &mut Vec<UsageRecord>, flushes: &mut Vec<FlushReply>) {
        match cmd {
            WriterCmd::Append(rec) => records.push(rec),
            WriterCmd::Flush(reply) => flushes.push(reply),
        }
    }

    fn write_and_sync_batch(&mut self, records: &[UsageRecord]) -> GridResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut batch_bytes = Vec::with_capacity(records.len() * 256);
        for rec in records {
            batch_bytes.extend_from_slice(&encode_journal_line(rec)?);
        }

        let saved_len = self.disk.journal_bytes;
        let write_result = self.try_append_raw(&batch_bytes, saved_len);
        if let Err(ref err) = write_result {
            let _ = self.disk.journal_file.set_len(saved_len);
            let _ = self.disk.journal_file.sync_data();
            let _ = self.disk.journal_file.seek(SeekFrom::Start(saved_len));
            warn!(
                path = %self.disk.journal_path.display(),
                error = %err,
                rolled_back_to = saved_len,
                "Credits journal append failed; truncated partial bytes back to last clean offset"
            );
            self.mem
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .enforce_ram_bounds();
            return write_result;
        }

        self.disk.journal_bytes = self
            .disk
            .journal_bytes
            .saturating_add(batch_bytes.len() as u64);
        for rec in records {
            self.disk.core.push_retained(rec.clone());
        }

        // Compaction happens strictly after the journal batch is durable; if snapshot replacement
        // hits I/O trouble, log and continue so already-journaled callers never see an error.
        if let Err(err) = self.disk.compact(&self.compaction_count, &self.mem) {
            warn!(
                path = %self.disk.snapshot_path.display(),
                error = %err,
                "Credits ledger rollover compaction failed after journal commit; keeping ledger active"
            );
            self.mem
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .enforce_ram_bounds();
        }

        Ok(())
    }

    fn try_append_raw(&mut self, batch_bytes: &[u8], saved_len: u64) -> GridResult<()> {
        self.disk
            .journal_file
            .seek(SeekFrom::Start(saved_len))
            .map_err(|e| {
                GridError::Config(format!(
                    "Cannot seek credits journal '{}': {e}",
                    self.disk.journal_path.display()
                ))
            })?;
        #[cfg(test)]
        if self.inject_fail_next_append.swap(false, Ordering::SeqCst) {
            let _ = self
                .disk
                .journal_file
                .write_all(b"{\"seq\":999,\"blake3\":\"partial");
            return Err(GridError::Config(
                "Simulated credits journal append failure".into(),
            ));
        }
        self.disk.journal_file.write_all(batch_bytes).map_err(|e| {
            GridError::Config(format!(
                "Cannot append credits journal '{}': {e}",
                self.disk.journal_path.display()
            ))
        })?;
        self.disk.journal_file.sync_data().map_err(|e| {
            GridError::Config(format!(
                "Cannot sync credits journal '{}': {e}",
                self.disk.journal_path.display()
            ))
        })?;
        Ok(())
    }
}

/// Thread-safe, bounded, append-only display-only contribution ledger.
pub struct CreditsLedger {
    mem: Arc<Mutex<CoreLedgerState>>,
    clock: Arc<dyn LedgerClock>,
    writer_tx: Option<mpsc::Sender<WriterCmd>>,
    writer_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    compaction_count: Arc<AtomicU64>,
    #[cfg(test)]
    inject_fail_next_append: Arc<AtomicBool>,
}

impl Drop for CreditsLedger {
    fn drop(&mut self) {
        self.writer_tx.take();
        if let Some(handle) = self
            .writer_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

impl CreditsLedger {
    /// Creates an in-memory ledger using [`SystemLedgerClock`].
    pub fn in_memory(config: CreditsConfig) -> Self {
        Self::in_memory_with_clock(config, Arc::new(SystemLedgerClock))
    }

    /// Creates an in-memory ledger with a custom [`LedgerClock`] (for deterministic tests).
    pub fn in_memory_with_clock(config: CreditsConfig, clock: Arc<dyn LedgerClock>) -> Self {
        Self {
            mem: Arc::new(Mutex::new(CoreLedgerState::new(config))),
            clock,
            writer_tx: None,
            writer_thread: Mutex::new(None),
            compaction_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            inject_fail_next_append: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Opens or creates a durable ledger under `state_dir` (`credits.snapshot.json` and
    /// `credits.journal.jsonl`) using [`SystemLedgerClock`].
    pub fn open(state_dir: &Path, config: CreditsConfig) -> GridResult<Self> {
        Self::open_with_clock(state_dir, config, Arc::new(SystemLedgerClock))
    }

    /// Opens or creates a durable ledger under `state_dir` with a custom [`LedgerClock`].
    ///
    /// Symlinks on credit files are refused. Corrupt snapshots or mid-file corrupt journals are
    /// moved aside (`*.corrupt.<utc>`) with a warning and replaced by a clean empty ledger so a
    /// display-only ledger never stops the master from starting.
    pub fn open_with_clock(
        state_dir: &Path,
        config: CreditsConfig,
        clock: Arc<dyn LedgerClock>,
    ) -> GridResult<Self> {
        let config = config.sanitized();
        ensure_private_dir(state_dir)?;
        let snapshot_path = state_dir.join(CREDITS_SNAPSHOT_FILE);
        let journal_path = state_dir.join(CREDITS_JOURNAL_FILE);
        refuse_symlink(&snapshot_path)?;
        refuse_symlink(&journal_path)?;

        let mem = Arc::new(Mutex::new(CoreLedgerState::new(config)));
        let compaction_count = Arc::new(AtomicU64::new(0));

        let mut disk = match load_persistent_state(&snapshot_path, &journal_path, config) {
            Ok(loaded) => loaded,
            Err(err) if err.is_quarantinable() => {
                warn!(
                    dir = %state_dir.display(),
                    error = %err.err,
                    "Quarantining corrupt credits files and starting fresh empty display-only ledger"
                );
                quarantine_corrupt_files(&snapshot_path, &journal_path, clock.now_utc());
                load_persistent_state(&snapshot_path, &journal_path, config).map_err(|e| e.err)?
            }
            Err(err) => return Err(err.err),
        };

        if let Err(err) = disk.compact(&compaction_count, &mem) {
            warn!(
                dir = %state_dir.display(),
                error = %err,
                "Initial credits ledger compaction failed on startup; continuing cleanly"
            );
        }

        {
            let mut guard = mem.lock().unwrap_or_else(|e| e.into_inner());
            guard.next_record_id = disk.core.next_record_id;
            guard.archived_records = disk.core.archived_records;
            guard.max_evicted_recorded_at_utc = disk.core.max_evicted_recorded_at_utc;
            guard.archived_pairs = disk.core.archived_pairs.clone();
            guard.retained = disk.core.retained.clone();
            guard.retained_estimated_bytes = disk.core.retained_estimated_bytes;
            guard.last_archived_record_id = disk.core.last_archived_record_id;
        }

        let (tx, rx) = mpsc::channel();
        #[cfg(test)]
        let inject_fail_next_append = Arc::new(AtomicBool::new(false));
        let writer = DiskWriter {
            disk,
            mem: Arc::clone(&mem),
            compaction_count: Arc::clone(&compaction_count),
            #[cfg(test)]
            inject_fail_next_append: Arc::clone(&inject_fail_next_append),
        };
        let thread = std::thread::Builder::new()
            .name("credits-ledger-writer".into())
            .spawn(move || writer.run(rx))
            .map_err(|e| GridError::Config(format!("Cannot spawn credits writer thread: {e}")))?;

        Ok(Self {
            mem,
            clock,
            writer_tx: Some(tx),
            writer_thread: Mutex::new(Some(thread)),
            compaction_count,
            #[cfg(test)]
            inject_fail_next_append,
        })
    }

    /// Maps an optional authenticated `member_id` to an immutable ledger key (`member_id` hex or
    /// `"organiser"`).
    pub fn member_key(member_id: Option<&str>) -> String {
        member_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map_or_else(|| ORGANISER_PARTICIPANT.to_string(), str::to_string)
    }

    /// Number of rollover compactions completed since this ledger was opened or created.
    pub fn compaction_count(&self) -> u64 {
        self.compaction_count.load(Ordering::Relaxed)
    }

    /// Non-blocking booking into the in-memory ledger and batching disk writer queue.
    pub fn enqueue_task(&self, input: TaskUsageInput) -> UsageRecord {
        self.enqueue_entry(
            Self::member_key(input.runner_member_id.as_deref()),
            Self::member_key(input.submitter_member_id.as_deref()),
            UsageKind::Task {
                task_id: input.task_id,
                worker_id: input.worker_id,
                duration_ms: input.duration_ms.min(MAX_CREDIT_DURATION_MS),
                cpu_cores: input.cpu_cores.clamp(1, MAX_CREDIT_CORES),
                gpu_reserved: input.gpu_reserved,
                exit_code: input.exit_code,
                verified: input.verified,
            },
        )
    }

    /// Records a terminal task execution and waits for the batching writer to flush when
    /// persistent (blocking). Returns `Err` only if the journal write/sync itself failed before
    /// commit.
    pub fn record_task(&self, input: TaskUsageInput) -> GridResult<UsageRecord> {
        let rec = self.enqueue_task(input);
        self.flush()?;
        Ok(rec)
    }

    /// Async wrapper for [`Self::record_task`].
    pub async fn record_task_async(
        self: &Arc<Self>,
        input: TaskUsageInput,
    ) -> GridResult<UsageRecord> {
        let rec = self.enqueue_task(input);
        self.flush_async().await?;
        Ok(rec)
    }

    /// Non-blocking model-tunnel booking (`LLM-10`, design 0061).
    pub fn enqueue_model_tunnel(&self, input: ModelTunnelUsageInput) -> UsageRecord {
        self.enqueue_entry(
            Self::member_key(input.runner_member_id.as_deref()),
            Self::member_key(input.submitter_member_id.as_deref()),
            UsageKind::ModelTunnel {
                tunnel_id: input.tunnel_id,
                service: input.service,
                worker_id: input.worker_id,
                duration_ms: input.duration_ms.min(MAX_CREDIT_DURATION_MS),
                bytes_to_worker: input.bytes_to_worker,
                bytes_from_worker: input.bytes_from_worker,
                close_reason: input.close_reason,
            },
        )
    }

    /// Records a completed model-tunnel session synchronously (`LLM-10`, design 0061).
    pub fn record_model_tunnel(&self, input: ModelTunnelUsageInput) -> GridResult<UsageRecord> {
        let rec = self.enqueue_model_tunnel(input);
        self.flush()?;
        Ok(rec)
    }

    /// Waits for all previously enqueued records to be written and synced to disk (no-op in
    /// memory-only mode). Safe to call from both synchronous and async contexts.
    pub fn flush(&self) -> GridResult<()> {
        let Some(ref writer) = self.writer_tx else {
            return Ok(());
        };
        let (tx, rx) = mpsc::sync_channel(1);
        if writer.send(WriterCmd::Flush(FlushReply::Sync(tx))).is_err() {
            return Err(GridError::Config("Credits writer thread is closed".into()));
        }
        rx.recv()
            .map_err(|_| GridError::Config("Credits writer flush channel closed".into()))?
    }

    /// Async equivalent of [`Self::flush`].
    pub async fn flush_async(&self) -> GridResult<()> {
        let Some(ref writer) = self.writer_tx else {
            return Ok(());
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        if writer
            .send(WriterCmd::Flush(FlushReply::Async(tx)))
            .is_err()
        {
            return Err(GridError::Config("Credits writer thread is closed".into()));
        }
        rx.await
            .map_err(|_| GridError::Config("Credits writer flush channel closed".into()))?
    }

    #[cfg(test)]
    pub(crate) fn inject_next_append_failure(&self) {
        self.inject_fail_next_append.store(true, Ordering::SeqCst);
    }

    fn enqueue_entry(
        &self,
        runner_member_key: String,
        submitter_member_key: String,
        kind: UsageKind,
    ) -> UsageRecord {
        let recorded_at_utc = self.clock.now_utc();
        let mut guard = self.mem.lock().unwrap_or_else(|e| e.into_inner());
        let record_id = guard.next_record_id;
        guard.next_record_id = record_id.saturating_add(1);
        let rec = UsageRecord {
            record_id,
            recorded_at_utc,
            runner_member_key,
            submitter_member_key,
            kind,
        };
        guard.push_retained(rec.clone());
        if let Some(ref writer) = self.writer_tx {
            let _ = writer.send(WriterCmd::Append(rec.clone()));
        } else {
            guard.compact_in_memory_if_needed(&self.compaction_count);
        }
        rec
    }

    /// Builds a read-only admin-view snapshot (no participant labels, raw member keys shown).
    pub fn snapshot(&self) -> CreditsSnapshot {
        self.snapshot_for_display(&HashMap::new(), &HashMap::new(), true)
    }

    /// Builds a display snapshot across `24h`, `7d`, and `all_time`.
    ///
    /// - `participant_labels`: optional map of `member_id -> participant_label` (populated by ECO-9).
    /// - `member_names`: optional map of `member_id -> MemberEntry.name` (consulted ONLY when
    ///   `is_admin == true`).
    /// - `is_admin`: when `false` (`AuthScope::Observer`), omits `member_ids` and never leaks raw
    ///   member ids or machine names (showing only participant labels, `"organiser"`, or
    ///   deterministic `"participant-N"` placeholders).
    pub fn snapshot_for_display(
        &self,
        participant_labels: &HashMap<String, String>,
        member_names: &HashMap<String, String>,
        is_admin: bool,
    ) -> CreditsSnapshot {
        let now_utc = self.clock.now_utc();
        let cutoff_24h = now_utc.saturating_sub(WINDOW_24H_SECS);
        let cutoff_7d = now_utc.saturating_sub(WINDOW_7D_SECS);

        // Lock only RAM, never disk or async awaits.
        let (archived_pairs, retained, retained_len, archived_records, max_evicted) = {
            let guard = self.mem.lock().unwrap_or_else(|e| e.into_inner());
            (
                guard.archived_pairs.clone(),
                guard.retained.iter().cloned().collect::<Vec<_>>(),
                guard.retained.len(),
                guard.archived_records,
                guard.max_evicted_recorded_at_utc,
            )
        };

        let mut all_groups: BTreeMap<RowGroupKey, RowMeta> = BTreeMap::new();
        let mut all_time_map: BTreeMap<RowGroupKey, WindowBuckets> = BTreeMap::new();
        let mut w24_map: BTreeMap<RowGroupKey, WindowBuckets> = BTreeMap::new();
        let mut w7_map: BTreeMap<RowGroupKey, WindowBuckets> = BTreeMap::new();

        // Collect every known member key up front so Observer `"participant-N"` numbering is
        // completely deterministic across refreshes and independent of HashMap iteration order.
        let mut known_members = BTreeSet::new();
        for (runner_key, targets) in &archived_pairs {
            known_members.insert(runner_key.as_str());
            for submitter_key in targets.keys() {
                known_members.insert(submitter_key.as_str());
            }
        }
        for rec in &retained {
            known_members.insert(rec.runner_member_key.as_str());
            known_members.insert(rec.submitter_member_key.as_str());
        }

        let mut anon_labels: HashMap<String, String> = HashMap::new();
        let mut next_anon = 1usize;
        for key in known_members {
            let group = group_for_member_key(key, participant_labels);
            if matches!(group, RowGroupKey::UnlabeledMember(_)) && !is_admin {
                anon_labels.insert(key.to_string(), format!("participant-{next_anon}"));
                next_anon += 1;
            }
            let meta = all_groups.entry(group).or_insert_with(|| {
                let label = resolve_group_label(
                    key,
                    participant_labels,
                    member_names,
                    &anon_labels,
                    is_admin,
                );
                RowMeta {
                    label,
                    member_ids: BTreeSet::new(),
                }
            });
            if is_admin && key != ORGANISER_PARTICIPANT {
                meta.member_ids.insert(key.to_string());
            }
        }

        let book = |map: &mut BTreeMap<RowGroupKey, WindowBuckets>,
                    runner_key: &str,
                    submitter_key: &str,
                    delta: SideCounters| {
            if delta.is_zero() {
                return;
            }
            let r_group = group_for_member_key(runner_key, participant_labels);
            let s_group = group_for_member_key(submitter_key, participant_labels);
            if r_group == s_group {
                map.entry(r_group)
                    .or_default()
                    .self_run
                    .saturating_add_assign(delta);
            } else {
                map.entry(r_group)
                    .or_default()
                    .ran_for_others
                    .saturating_add_assign(delta);
                map.entry(s_group)
                    .or_default()
                    .used_from_others
                    .saturating_add_assign(delta);
            }
        };

        for (runner_key, targets) in &archived_pairs {
            for (submitter_key, &delta) in targets {
                book(&mut all_time_map, runner_key, submitter_key, delta);
            }
        }

        for rec in &retained {
            let delta = SideCounters::from_kind(&rec.kind);
            book(
                &mut all_time_map,
                &rec.runner_member_key,
                &rec.submitter_member_key,
                delta,
            );
            if rec.recorded_at_utc >= cutoff_7d {
                book(
                    &mut w7_map,
                    &rec.runner_member_key,
                    &rec.submitter_member_key,
                    delta,
                );
            }
            if rec.recorded_at_utc >= cutoff_24h {
                book(
                    &mut w24_map,
                    &rec.runner_member_key,
                    &rec.submitter_member_key,
                    delta,
                );
            }
        }

        let mut sum_24h = WindowBuckets::default();
        let mut sum_7d = WindowBuckets::default();
        let mut sum_all = WindowBuckets::default();
        let mut participants = Vec::with_capacity(all_groups.len());

        for (group, meta) in all_groups {
            let b24 = w24_map.get(&group).copied().unwrap_or_default();
            let b7 = w7_map.get(&group).copied().unwrap_or_default();
            let ball = all_time_map.get(&group).copied().unwrap_or_default();

            accumulate_window(&mut sum_24h, b24);
            accumulate_window(&mut sum_7d, b7);
            accumulate_window(&mut sum_all, ball);

            participants.push(ParticipantCreditsDto {
                label: meta.label,
                member_ids: if is_admin {
                    meta.member_ids.into_iter().collect()
                } else {
                    Vec::new()
                },
                last_24h: CreditWindowDto::from(b24),
                last_7d: CreditWindowDto::from(b7),
                all_time: CreditWindowDto::from(ball),
            });
        }

        participants.sort_by(|a, b| {
            let vol = |row: &ParticipantCreditsDto| {
                row.all_time
                    .ran_for_others
                    .core_ms
                    .saturating_add(row.all_time.used_from_others.core_ms)
                    .saturating_add(row.all_time.self_run.core_ms)
            };
            let tasks = |row: &ParticipantCreditsDto| {
                row.all_time
                    .ran_for_others
                    .tasks
                    .saturating_add(row.all_time.used_from_others.tasks)
                    .saturating_add(row.all_time.self_run.tasks)
            };
            vol(b)
                .cmp(&vol(a))
                .then_with(|| tasks(b).cmp(&tasks(a)))
                .then_with(|| a.label.cmp(&b.label))
                .then_with(|| a.member_ids.cmp(&b.member_ids))
        });

        let window_truncated = max_evicted.is_some_and(|evicted_at| evicted_at >= cutoff_7d);

        CreditsSnapshot {
            display_only: true,
            note: DISPLAY_ONLY_NOTE.to_string(),
            generated_at_utc: now_utc,
            total_records: archived_records.saturating_add(retained_len as u64),
            retained_records: retained_len,
            archived_records,
            window_truncated,
            totals_24h: CreditWindowDto::from(sum_24h),
            totals_7d: CreditWindowDto::from(sum_7d),
            totals_all_time: CreditWindowDto::from(sum_all),
            participants,
        }
    }
}

fn accumulate_window(dst: &mut WindowBuckets, src: WindowBuckets) {
    dst.ran_for_others.saturating_add_assign(src.ran_for_others);
    dst.used_from_others
        .saturating_add_assign(src.used_from_others);
    dst.self_run.saturating_add_assign(src.self_run);
}

fn group_for_member_key(key: &str, participant_labels: &HashMap<String, String>) -> RowGroupKey {
    if key == ORGANISER_PARTICIPANT {
        return RowGroupKey::Organiser;
    }
    if let Some(label) = participant_labels
        .get(key)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return RowGroupKey::Label(label.to_string());
    }
    RowGroupKey::UnlabeledMember(key.to_string())
}

fn resolve_group_label(
    member_key: &str,
    participant_labels: &HashMap<String, String>,
    member_names: &HashMap<String, String>,
    anon_labels: &HashMap<String, String>,
    is_admin: bool,
) -> String {
    if member_key == ORGANISER_PARTICIPANT {
        return ORGANISER_PARTICIPANT.to_string();
    }
    if let Some(label) = participant_labels
        .get(member_key)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return label.to_string();
    }
    if is_admin {
        if let Some(name) = member_names
            .get(member_key)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            return name.to_string();
        }
        return member_key.to_string();
    }
    anon_labels
        .get(member_key)
        .cloned()
        .unwrap_or_else(|| "participant".to_string())
}

fn estimate_record_bytes(rec: &UsageRecord) -> u64 {
    let base = 180u64
        .saturating_add(rec.runner_member_key.len() as u64)
        .saturating_add(rec.submitter_member_key.len() as u64);
    match &rec.kind {
        UsageKind::Task { .. } => base.saturating_add(80),
        UsageKind::ModelTunnel {
            service,
            close_reason,
            ..
        } => base
            .saturating_add(80)
            .saturating_add(service.len() as u64)
            .saturating_add(close_reason.len() as u64),
    }
}

fn hash_domain(domain: &[u8], payload: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(payload);
    hasher.finalize().to_hex().to_string()
}

/// Encodes `snapshot` into compact JSON wrapped in `{ "version": 1, "blake3": ..., "payload": ... }`
/// without allocating a `serde_json::Value` tree.
fn encode_snapshot_bytes(snapshot: &CreditsSnapshotFile) -> GridResult<Vec<u8>> {
    let payload = serde_json::to_vec(snapshot)
        .map_err(|e| GridError::Config(format!("Failed to serialize credits snapshot: {e}")))?;
    let digest = hash_domain(DOMAIN_SNAPSHOT_V1, &payload);
    let prefix = format!(
        "{{\"version\":{},\"blake3\":\"{digest}\",\"payload\":",
        CREDITS_SNAPSHOT_VERSION
    );
    let mut out = Vec::with_capacity(prefix.len() + payload.len() + 1);
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(&payload);
    out.push(b'}');
    Ok(out)
}

/// Encodes one journal record as a single compact JSON line without allocating `serde_json::Value`.
fn encode_journal_line(record: &UsageRecord) -> GridResult<Vec<u8>> {
    let payload = serde_json::to_vec(record)
        .map_err(|e| GridError::Config(format!("Failed to serialize credits journal line: {e}")))?;
    let digest = hash_domain(DOMAIN_JOURNAL_V1, &payload);
    let prefix = format!(
        "{{\"seq\":{},\"blake3\":\"{digest}\",\"payload\":",
        record.record_id
    );
    let mut line = Vec::with_capacity(prefix.len() + payload.len() + 2);
    line.extend_from_slice(prefix.as_bytes());
    line.extend_from_slice(&payload);
    line.extend_from_slice(b"}\n");
    Ok(line)
}

/// Splits an outer envelope `{ <header fields>, "payload": <inner> }` into its header JSON object
/// string (closing `}` restored) and the exact raw byte slice of `<inner>`, avoiding a full
/// `serde_json::Value` DOM build.
fn split_envelope(raw: &[u8]) -> Result<(String, &[u8]), String> {
    let trimmed = trim_ascii_bytes(raw);
    if trimmed.first() != Some(&b'{') || trimmed.last() != Some(&b'}') || trimmed.len() < 2 {
        return Err("envelope must be a JSON object".into());
    }
    let body = &trimmed[..trimmed.len() - 1];
    let search_end = body.len().min(192);
    let marker = b"\"payload\"";
    let key_pos = body[..search_end]
        .windows(marker.len())
        .position(|w| w == marker)
        .ok_or_else(|| "missing \"payload\" field".to_string())?;
    let comma_pos = body[..key_pos]
        .iter()
        .rposition(|&b| b == b',')
        .ok_or_else(|| "malformed envelope header before \"payload\"".to_string())?;
    let mut pos = key_pos + marker.len();
    while pos < body.len() && body[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if body.get(pos) != Some(&b':') {
        return Err("missing ':' after \"payload\"".into());
    }
    let payload = trim_ascii_bytes(&body[pos + 1..]);
    if payload.is_empty() {
        return Err("empty envelope payload".into());
    }
    let header_str = std::str::from_utf8(&body[..comma_pos])
        .map_err(|e| format!("invalid UTF-8 in header: {e}"))?;
    Ok((format!("{header_str}}}"), payload))
}

fn trim_ascii_bytes(mut s: &[u8]) -> &[u8] {
    while let Some(&b) = s.first() {
        if b.is_ascii_whitespace() {
            s = &s[1..];
        } else {
            break;
        }
    }
    while let Some(&b) = s.last() {
        if b.is_ascii_whitespace() {
            s = &s[..s.len() - 1];
        } else {
            break;
        }
    }
    s
}

fn parse_snapshot_bytes(bytes: &[u8]) -> Result<CreditsSnapshotFile, String> {
    let (header_json, payload_slice) = split_envelope(bytes)?;
    let header: SnapshotHeader =
        serde_json::from_str(&header_json).map_err(|e| format!("corrupt header: {e}"))?;
    if header.version != CREDITS_SNAPSHOT_VERSION {
        return Err(format!("unsupported snapshot version {}", header.version));
    }
    if hash_domain(DOMAIN_SNAPSHOT_V1, payload_slice) != header.blake3 {
        return Err("snapshot BLAKE3 mismatch".into());
    }
    let snapshot: CreditsSnapshotFile =
        serde_json::from_slice(payload_slice).map_err(|e| format!("invalid payload: {e}"))?;
    if snapshot.version != CREDITS_SNAPSHOT_VERSION {
        return Err(format!(
            "mismatched inner snapshot version {}",
            snapshot.version
        ));
    }
    Ok(snapshot)
}

fn parse_journal_line(raw_line: &[u8]) -> Result<UsageRecord, String> {
    let (header_json, payload_slice) = split_envelope(raw_line)?;
    let header: JournalHeader =
        serde_json::from_str(&header_json).map_err(|e| format!("invalid JSON header: {e}"))?;
    if hash_domain(DOMAIN_JOURNAL_V1, payload_slice) != header.blake3 {
        return Err("BLAKE3 mismatch".into());
    }
    let record: UsageRecord =
        serde_json::from_slice(payload_slice).map_err(|e| format!("invalid payload: {e}"))?;
    if record.record_id != header.seq {
        return Err(format!(
            "sequence mismatch: envelope {} != record {}",
            header.seq, record.record_id
        ));
    }
    Ok(record)
}

struct LoadErr {
    err: GridError,
    quarantinable: bool,
}

impl LoadErr {
    fn is_quarantinable(&self) -> bool {
        self.quarantinable
    }

    fn fatal(err: GridError) -> Self {
        Self {
            err,
            quarantinable: false,
        }
    }

    fn corrupt(msg: String) -> Self {
        Self {
            err: GridError::Config(msg),
            quarantinable: true,
        }
    }
}

fn load_persistent_state(
    snapshot_path: &Path,
    journal_path: &Path,
    config: CreditsConfig,
) -> Result<PersistentDiskState, LoadErr> {
    let (snapshot_file, snapshot_bytes) = if snapshot_path.exists() {
        let bytes = fs::read(snapshot_path).map_err(|e| {
            LoadErr::fatal(GridError::Config(format!(
                "Cannot read credits snapshot '{}': {e}",
                snapshot_path.display()
            )))
        })?;
        let file_len = bytes.len() as u64;
        let parsed = parse_snapshot_bytes(&bytes).map_err(|why| {
            LoadErr::corrupt(format!(
                "Corrupt credits snapshot '{}': {why}",
                snapshot_path.display()
            ))
        })?;
        (parsed, file_len)
    } else {
        (CreditsSnapshotFile::default(), 0)
    };

    let (journal_file, journal_records, journal_bytes) =
        open_and_replay_journal(journal_path, snapshot_file.next_record_id)?;

    let mut core = CoreLedgerState::new(config);
    core.next_record_id = snapshot_file.next_record_id.max(1);
    core.archived_records = snapshot_file.archived_records;
    core.max_evicted_recorded_at_utc = snapshot_file.max_evicted_recorded_at_utc;
    core.archived_pairs = snapshot_file.archived_pairs;
    for rec in snapshot_file.retained.into_iter().chain(journal_records) {
        core.push_retained(rec);
    }

    Ok(PersistentDiskState {
        core,
        snapshot_path: snapshot_path.to_path_buf(),
        journal_path: journal_path.to_path_buf(),
        journal_file,
        snapshot_bytes,
        journal_bytes,
    })
}

fn open_and_replay_journal(
    path: &Path,
    snapshot_next_record_id: u64,
) -> Result<(File, Vec<UsageRecord>, u64), LoadErr> {
    let mut file = open_private_journal_file(path).map_err(LoadErr::fatal)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|e| {
        LoadErr::fatal(GridError::Config(format!(
            "Cannot read credits journal '{}': {e}",
            path.display()
        )))
    })?;

    let mut records = Vec::new();
    let mut offset = 0usize;
    let mut valid_end = 0usize;
    let mut truncate_to: Option<usize> = None;

    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let Some(nl_pos) = rest.iter().position(|&b| b == b'\n') else {
            warn!(
                path = %path.display(),
                offset,
                "Truncating incomplete trailing line in credits journal"
            );
            truncate_to = Some(valid_end);
            break;
        };
        let line_slice = &rest[..nl_pos];
        let next_offset = offset + nl_pos + 1;
        if line_slice.is_empty() {
            if next_offset < bytes.len() {
                return Err(LoadErr::corrupt(format!(
                    "Unexpected blank line in middle of credits journal '{}' at offset {}",
                    path.display(),
                    offset
                )));
            }
            truncate_to = Some(valid_end);
            break;
        }
        match parse_journal_line(line_slice) {
            Ok(rec) => {
                if rec.record_id >= snapshot_next_record_id {
                    records.push(rec);
                }
                valid_end = next_offset;
                offset = next_offset;
            }
            Err(reason) => {
                if next_offset == bytes.len() {
                    warn!(
                        path = %path.display(),
                        offset,
                        error = %reason,
                        "Truncating corrupt trailing line in credits journal"
                    );
                    truncate_to = Some(valid_end);
                    break;
                }
                return Err(LoadErr::corrupt(format!(
                    "Corrupt line in middle of credits journal '{}' at offset {}: {}",
                    path.display(),
                    offset,
                    reason
                )));
            }
        }
    }

    if let Some(new_len) = truncate_to {
        file.set_len(new_len as u64).map_err(|e| {
            LoadErr::fatal(GridError::Config(format!(
                "Cannot truncate credits journal '{}': {e}",
                path.display()
            )))
        })?;
        file.sync_all().map_err(|e| {
            LoadErr::fatal(GridError::Config(format!(
                "Cannot sync truncated credits journal '{}': {e}",
                path.display()
            )))
        })?;
        valid_end = new_len;
    }

    file.seek(SeekFrom::Start(valid_end as u64)).map_err(|e| {
        LoadErr::fatal(GridError::Config(format!(
            "Cannot seek credits journal '{}': {e}",
            path.display()
        )))
    })?;
    Ok((file, records, valid_end as u64))
}

fn quarantine_corrupt_files(snapshot_path: &Path, journal_path: &Path, stamp: u64) {
    for src in [snapshot_path, journal_path] {
        if !src.exists() {
            continue;
        }
        let Some(name) = src.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let mut idx = 0u32;
        loop {
            let suffix = if idx == 0 {
                format!("{name}.corrupt.{stamp}")
            } else {
                format!("{name}.corrupt.{stamp}.{idx}")
            };
            let dest = src.with_file_name(suffix);
            if !dest.exists() {
                if let Err(e) = fs::rename(src, &dest) {
                    warn!(
                        src = %src.display(),
                        dest = %dest.display(),
                        error = %e,
                        "Failed to rename corrupt credits file; removing so fresh ledger starts"
                    );
                    let _ = fs::remove_file(src);
                }
                break;
            }
            idx = idx.saturating_add(1);
        }
    }
}

fn refuse_symlink(path: &Path) -> GridResult<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(GridError::Config(format!(
            "Refusing symlink at private credits path '{}'",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(GridError::Config(format!(
            "Cannot inspect credits path '{}': {e}",
            path.display()
        ))),
    }
}

fn ensure_private_dir(dir: &Path) -> GridResult<()> {
    refuse_symlink(dir)?;
    let existed = dir.exists();
    #[cfg(windows)]
    rusty_grid_core::private_file::create_private_dir_all(dir).map_err(|e| {
        GridError::Config(format!(
            "Cannot create credits directory '{}': {e}",
            dir.display()
        ))
    })?;
    #[cfg(not(windows))]
    {
        fs::create_dir_all(dir).map_err(|e| {
            GridError::Config(format!(
                "Cannot create credits directory '{}': {e}",
                dir.display()
            ))
        })?;
        #[cfg(unix)]
        if !existed {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
        }
        let _ = existed;
    }
    let _ = existed;
    Ok(())
}

/// Opens the journal read+write (without `append(true)`, because `set_len` during compaction and
/// tail truncation requires write permission without append-only restriction on Windows).
fn open_private_journal_file(path: &Path) -> GridResult<File> {
    refuse_symlink(path)?;
    #[cfg(windows)]
    {
        if !path.exists() {
            match rusty_grid_core::private_file::create_private_new(path) {
                Ok(f) => drop(f),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
                Err(e) => {
                    return Err(GridError::Config(format!(
                        "Cannot create private credits journal '{}': {e}",
                        path.display()
                    )));
                }
            }
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| {
                GridError::Config(format!(
                    "Cannot open credits journal '{}': {e}",
                    path.display()
                ))
            })
    }
    #[cfg(not(windows))]
    {
        let mut opts = OpenOptions::new();
        opts.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        opts.open(path).map_err(|e| {
            GridError::Config(format!(
                "Cannot open credits journal '{}': {e}",
                path.display()
            ))
        })
    }
}

fn write_private_atomic(path: &Path, data: &[u8]) -> GridResult<()> {
    #[cfg(windows)]
    {
        rusty_grid_core::private_file::write_private_replace(path, data).map_err(|e| {
            GridError::Config(format!(
                "Cannot write credits snapshot '{}': {e}",
                path.display()
            ))
        })?;
    }
    #[cfg(not(windows))]
    {
        let tmp = path.with_file_name(format!(
            ".{CREDITS_SNAPSHOT_FILE}.{}.tmp",
            std::process::id()
        ));
        let _ = fs::remove_file(&tmp);
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let res = opts.open(&tmp).and_then(|mut f| {
            f.write_all(data)?;
            f.sync_all()?;
            drop(f);
            fs::rename(&tmp, path)
        });
        if let Err(e) = res {
            let _ = fs::remove_file(&tmp);
            return Err(GridError::Config(format!(
                "Cannot write credits snapshot '{}': {e}",
                path.display()
            )));
        }
    }
    if let Some(parent) = path.parent() {
        crate::store::sync_dir(parent, "credits_snapshot_dir")?;
    }
    Ok(())
}
