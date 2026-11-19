//! Master-local bounded, append-only, display-only contribution ledger (`NET-10`, design 0096).
//!
//! Tracks compute work (`UsageKind::Task`) and model-tunnel usage (`UsageKind::ModelTunnel`,
//! `LLM-10`) using double-entry zero-sum conservation: every recorded event credits the runner
//! participant and debits the submitter participant by the exact same vector, so net across all
//! participants is identically zero over `24h`, `7d`, and `all_time`.
//!
//! Per Q96 / design 0096 D4, balances are strictly display-only: the scheduler, queue admission,
//! fair share, and worker client never read or act on credits.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
/// Reserved participant key for v2 / unauthenticated / local-host peers without a member label.
pub const ORGANISER_PARTICIPANT: &str = "organiser";
/// Default cap on retained raw [`UsageRecord`] entries before compaction rolls oldest entries into
/// `archived_totals` (`--max-credit-records`).
pub const DEFAULT_MAX_CREDIT_RECORDS: usize = 100_000;
/// Default byte budget for retained credits state (`--max-credit-bytes`, 32 MiB).
pub const DEFAULT_MAX_CREDIT_BYTES: u64 = 32 * 1024 * 1024;
/// 24-hour aggregation window in seconds.
pub const WINDOW_24H_SECS: u64 = 86_400;
/// 7-day aggregation window in seconds.
pub const WINDOW_7D_SECS: u64 = 604_800;

const DOMAIN_SNAPSHOT_V1: &[u8] = b"oxideswarm-credits-snapshot-v1\0";
const DOMAIN_JOURNAL_V1: &[u8] = b"oxideswarm-credits-v1\0";
const DISPLAY_ONLY_NOTE: &str =
    "Display only (Q96): credits never affect scheduling, admission, or quotas.";

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

/// Bounding parameters for [`CreditsLedger`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditsConfig {
    /// Maximum raw [`UsageRecord`] entries retained before rollover into `archived_totals`.
    pub max_records: usize,
    /// Combined byte budget for `credits.snapshot.json` + `credits.journal.jsonl` (or in-memory
    /// retained records when no `--state-dir` is configured).
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

/// One double-entry usage event shared by compute tasks (`NET-10`) and model tunnels (`LLM-10`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Monotonic master-assigned sequence starting at 1.
    pub record_id: u64,
    /// Unix epoch seconds when committed on the master.
    pub recorded_at_utc: u64,
    /// Runner's participant key (member id, participant label, or `"organiser"`).
    pub credited_participant: String,
    /// Submitter's participant key (member id, participant label, or `"organiser"`).
    pub debited_participant: String,
    /// Enrolled member id owning the worker when OXS-AUTH v3 is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credited_member_id: Option<String>,
    /// Enrolled member id submitting the work when OXS-AUTH v3 is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debited_member_id: Option<String>,
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
        is_gpu_executed: bool,
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

/// Input passed by the master when committing a terminal task execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskUsageInput {
    pub task_id: TaskId,
    pub worker_id: Uuid,
    pub duration_ms: u64,
    pub cpu_cores: usize,
    pub gpu_reserved: bool,
    pub is_gpu_executed: bool,
    pub exit_code: i32,
    pub verified: Option<bool>,
    pub credited_member_id: Option<String>,
    pub debited_member_id: Option<String>,
    pub credited_label: Option<String>,
    pub debited_label: Option<String>,
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
    pub credited_member_id: Option<String>,
    pub debited_member_id: Option<String>,
    pub credited_label: Option<String>,
    pub debited_label: Option<String>,
}

/// Cumulative counters on one side (`contributed` or `used`) of a double-entry balance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideCounters {
    pub tasks: u64,
    pub core_ms: u64,
    pub gpu_ms: u64,
    #[serde(default)]
    pub tunnel_sessions: u64,
    #[serde(default)]
    pub tunnel_duration_ms: u64,
    #[serde(default)]
    pub tunnel_bytes: u64,
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
                is_gpu_executed,
                verified,
                ..
            } => {
                if *verified == Some(false) {
                    return Self::default();
                }
                let cores = u64::try_from((*cpu_cores).max(1)).unwrap_or(u64::MAX);
                let core_ms = duration_ms.saturating_mul(cores);
                let gpu_ms = if *gpu_reserved || *is_gpu_executed {
                    *duration_ms
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
                tunnel_duration_ms: *duration_ms,
                tunnel_bytes: bytes_to_worker.saturating_add(*bytes_from_worker),
            },
        }
    }
}

/// Contributed vs used totals for a participant in one time window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowCounters {
    pub contributed: SideCounters,
    pub used: SideCounters,
}

impl WindowCounters {
    fn add_contributed(&mut self, delta: SideCounters) {
        self.contributed.saturating_add_assign(delta);
    }

    fn add_used(&mut self, delta: SideCounters) {
        self.used.saturating_add_assign(delta);
    }
}

fn apply_record_to_map(
    map: &mut BTreeMap<String, WindowCounters>,
    rec: &UsageRecord,
    delta: SideCounters,
) {
    map.entry(rec.credited_participant.clone())
        .or_default()
        .add_contributed(delta);
    map.entry(rec.debited_participant.clone())
        .or_default()
        .add_used(delta);
}

/// JSON DTO for one side (`contributed` or `used`) in `GET /api/credits`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditCountersDto {
    pub tasks: u64,
    pub core_ms: u64,
    pub core_secs: f64,
    pub gpu_ms: u64,
    pub gpu_secs: f64,
    pub tunnel_sessions: u64,
    pub tunnel_duration_ms: u64,
    pub tunnel_bytes: u64,
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

/// JSON DTO for signed net values (`contributed - used`), either per participant or summed across
/// all participants (which always equals zero).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CreditNetDto {
    pub tasks: i128,
    pub core_ms: i128,
    pub core_secs: f64,
    pub gpu_ms: i128,
    pub gpu_secs: f64,
    pub tunnel_sessions: i128,
    pub tunnel_duration_ms: i128,
    pub tunnel_bytes: i128,
}

impl CreditNetDto {
    fn from_pair(contributed: SideCounters, used: SideCounters) -> Self {
        let tasks = i128::from(contributed.tasks) - i128::from(used.tasks);
        let core_ms = i128::from(contributed.core_ms) - i128::from(used.core_ms);
        let gpu_ms = i128::from(contributed.gpu_ms) - i128::from(used.gpu_ms);
        let tunnel_sessions =
            i128::from(contributed.tunnel_sessions) - i128::from(used.tunnel_sessions);
        let tunnel_duration_ms =
            i128::from(contributed.tunnel_duration_ms) - i128::from(used.tunnel_duration_ms);
        let tunnel_bytes = i128::from(contributed.tunnel_bytes) - i128::from(used.tunnel_bytes);
        Self {
            tasks,
            core_ms,
            core_secs: (core_ms as f64) / 1000.0,
            gpu_ms,
            gpu_secs: (gpu_ms as f64) / 1000.0,
            tunnel_sessions,
            tunnel_duration_ms,
            tunnel_bytes,
        }
    }

    fn accumulate(&mut self, rhs: &Self) {
        self.tasks = self.tasks.saturating_add(rhs.tasks);
        self.core_ms = self.core_ms.saturating_add(rhs.core_ms);
        self.core_secs = (self.core_ms as f64) / 1000.0;
        self.gpu_ms = self.gpu_ms.saturating_add(rhs.gpu_ms);
        self.gpu_secs = (self.gpu_ms as f64) / 1000.0;
        self.tunnel_sessions = self.tunnel_sessions.saturating_add(rhs.tunnel_sessions);
        self.tunnel_duration_ms = self
            .tunnel_duration_ms
            .saturating_add(rhs.tunnel_duration_ms);
        self.tunnel_bytes = self.tunnel_bytes.saturating_add(rhs.tunnel_bytes);
    }
}

/// JSON DTO for one participant's `contributed`, `used`, and `net` counters in one time window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditWindowDto {
    pub contributed: CreditCountersDto,
    pub used: CreditCountersDto,
    pub net: CreditNetDto,
}

impl From<WindowCounters> for CreditWindowDto {
    fn from(w: WindowCounters) -> Self {
        Self {
            contributed: CreditCountersDto::from(w.contributed),
            used: CreditCountersDto::from(w.used),
            net: CreditNetDto::from_pair(w.contributed, w.used),
        }
    }
}

/// Per-participant row returned by `GET /api/credits`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParticipantCreditsDto {
    pub participant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub last_24h: CreditWindowDto,
    pub last_7d: CreditWindowDto,
    pub all_time: CreditWindowDto,
}

/// Complete read-only ledger snapshot returned by [`CreditsLedger::snapshot`] and
/// `GET /api/credits`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditsSnapshot {
    pub display_only: bool,
    pub note: String,
    pub generated_at_utc: u64,
    pub total_records: u64,
    pub retained_records: usize,
    pub archived_records: u64,
    pub window_truncated: bool,
    pub net_sum_24h: CreditNetDto,
    pub net_sum_7d: CreditNetDto,
    pub net_sum_all_time: CreditNetDto,
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
            net_sum_24h: CreditNetDto::default(),
            net_sum_7d: CreditNetDto::default(),
            net_sum_all_time: CreditNetDto::default(),
            participants: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CreditsSnapshotFile {
    version: u32,
    next_record_id: u64,
    archived_records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_evicted_recorded_at_utc: Option<u64>,
    archived_totals: BTreeMap<String, WindowCounters>,
    #[serde(default)]
    participant_member_ids: BTreeMap<String, String>,
    retained: Vec<UsageRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEnvelope {
    version: u32,
    blake3: String,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalLineEnvelope {
    seq: u64,
    blake3: String,
    payload: serde_json::Value,
}

struct PersistentState {
    snapshot_path: PathBuf,
    journal_path: PathBuf,
    journal_file: File,
    snapshot_bytes: u64,
    journal_bytes: u64,
}

struct LedgerInner {
    config: CreditsConfig,
    next_record_id: u64,
    archived_records: u64,
    max_evicted_recorded_at_utc: Option<u64>,
    archived_totals: BTreeMap<String, WindowCounters>,
    participant_member_ids: BTreeMap<String, String>,
    retained: VecDeque<UsageRecord>,
    retained_estimated_bytes: u64,
    persistent: Option<PersistentState>,
}

/// Thread-safe, bounded, append-only contribution ledger.
pub struct CreditsLedger {
    inner: Mutex<LedgerInner>,
    clock: Arc<dyn LedgerClock>,
}

impl CreditsLedger {
    /// Creates an in-memory ledger using [`SystemLedgerClock`].
    pub fn in_memory(config: CreditsConfig) -> Self {
        Self::in_memory_with_clock(config, Arc::new(SystemLedgerClock))
    }

    /// Creates an in-memory ledger with a custom [`LedgerClock`] (for deterministic tests).
    pub fn in_memory_with_clock(config: CreditsConfig, clock: Arc<dyn LedgerClock>) -> Self {
        Self {
            inner: Mutex::new(LedgerInner {
                config,
                next_record_id: 1,
                archived_records: 0,
                max_evicted_recorded_at_utc: None,
                archived_totals: BTreeMap::new(),
                participant_member_ids: BTreeMap::new(),
                retained: VecDeque::new(),
                retained_estimated_bytes: 0,
                persistent: None,
            }),
            clock,
        }
    }

    /// Opens or creates a durable ledger under `state_dir` (`credits.snapshot.json` and
    /// `credits.journal.jsonl`) using [`SystemLedgerClock`].
    pub fn open(state_dir: &Path, config: CreditsConfig) -> GridResult<Self> {
        Self::open_with_clock(state_dir, config, Arc::new(SystemLedgerClock))
    }

    /// Opens or creates a durable ledger under `state_dir` with a custom [`LedgerClock`].
    pub fn open_with_clock(
        state_dir: &Path,
        config: CreditsConfig,
        clock: Arc<dyn LedgerClock>,
    ) -> GridResult<Self> {
        ensure_private_dir(state_dir)?;
        let snapshot_path = state_dir.join(CREDITS_SNAPSHOT_FILE);
        let journal_path = state_dir.join(CREDITS_JOURNAL_FILE);
        refuse_symlink(&snapshot_path)?;
        refuse_symlink(&journal_path)?;

        let (snapshot_file, snapshot_bytes) = if snapshot_path.exists() {
            read_verified_snapshot(&snapshot_path)?
        } else {
            (
                CreditsSnapshotFile {
                    version: CREDITS_SNAPSHOT_VERSION,
                    next_record_id: 1,
                    archived_records: 0,
                    max_evicted_recorded_at_utc: None,
                    archived_totals: BTreeMap::new(),
                    participant_member_ids: BTreeMap::new(),
                    retained: Vec::new(),
                },
                0,
            )
        };

        let (journal_file, journal_records, journal_bytes) =
            open_and_replay_journal(&journal_path, snapshot_file.next_record_id)?;

        let mut retained = VecDeque::with_capacity(
            snapshot_file
                .retained
                .len()
                .saturating_add(journal_records.len()),
        );
        let mut next_record_id = snapshot_file.next_record_id.max(1);
        let mut participant_member_ids = snapshot_file.participant_member_ids;
        let mut retained_estimated_bytes = 0u64;

        for rec in snapshot_file.retained.into_iter().chain(journal_records) {
            record_member_hint(&mut participant_member_ids, &rec);
            next_record_id = next_record_id.max(rec.record_id.saturating_add(1));
            retained_estimated_bytes =
                retained_estimated_bytes.saturating_add(estimate_record_bytes(&rec));
            retained.push_back(rec);
        }

        let mut inner = LedgerInner {
            config,
            next_record_id,
            archived_records: snapshot_file.archived_records,
            max_evicted_recorded_at_utc: snapshot_file.max_evicted_recorded_at_utc,
            archived_totals: snapshot_file.archived_totals,
            participant_member_ids,
            retained,
            retained_estimated_bytes,
            persistent: Some(PersistentState {
                snapshot_path,
                journal_path,
                journal_file,
                snapshot_bytes,
                journal_bytes,
            }),
        };
        inner.enforce_bounds_if_needed()?;

        Ok(Self {
            inner: Mutex::new(inner),
            clock,
        })
    }

    /// Resolves a participant key from an optional participant label, member id, or fallback.
    pub fn resolve_participant(participant_label: Option<&str>, member_id: Option<&str>) -> String {
        if let Some(label) = participant_label.map(str::trim).filter(|s| !s.is_empty()) {
            return label.to_string();
        }
        if let Some(mid) = member_id.map(str::trim).filter(|s| !s.is_empty()) {
            return mid.to_string();
        }
        ORGANISER_PARTICIPANT.to_string()
    }

    /// Records a terminal task execution synchronously (blocking on disk sync when persistent).
    pub fn record_task(&self, input: TaskUsageInput) -> GridResult<UsageRecord> {
        let credited_participant = Self::resolve_participant(
            input.credited_label.as_deref(),
            input.credited_member_id.as_deref(),
        );
        let debited_participant = Self::resolve_participant(
            input.debited_label.as_deref(),
            input.debited_member_id.as_deref(),
        );
        self.append_entry(
            credited_participant,
            debited_participant,
            input.credited_member_id,
            input.debited_member_id,
            UsageKind::Task {
                task_id: input.task_id,
                worker_id: input.worker_id,
                duration_ms: input.duration_ms,
                cpu_cores: input.cpu_cores.max(1),
                gpu_reserved: input.gpu_reserved,
                is_gpu_executed: input.is_gpu_executed,
                exit_code: input.exit_code,
                verified: input.verified,
            },
        )
    }

    /// Async wrapper for [`Self::record_task`] that runs blocking disk appends/compaction on the
    /// Tokio blocking thread pool when backed by `--state-dir`.
    pub async fn record_task_async(
        self: &Arc<Self>,
        input: TaskUsageInput,
    ) -> GridResult<UsageRecord> {
        let is_persistent = {
            let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            guard.persistent.is_some()
        };
        if !is_persistent {
            return self.record_task(input);
        }
        let ledger = Arc::clone(self);
        tokio::task::spawn_blocking(move || ledger.record_task(input))
            .await
            .map_err(|e| GridError::Config(format!("Credits ledger task panicked: {e}")))?
    }

    /// Records a completed model-tunnel session synchronously (`LLM-10`, design 0061).
    pub fn record_model_tunnel(&self, input: ModelTunnelUsageInput) -> GridResult<UsageRecord> {
        let credited_participant = Self::resolve_participant(
            input.credited_label.as_deref(),
            input.credited_member_id.as_deref(),
        );
        let debited_participant = Self::resolve_participant(
            input.debited_label.as_deref(),
            input.debited_member_id.as_deref(),
        );
        self.append_entry(
            credited_participant,
            debited_participant,
            input.credited_member_id,
            input.debited_member_id,
            UsageKind::ModelTunnel {
                tunnel_id: input.tunnel_id,
                service: input.service,
                worker_id: input.worker_id,
                duration_ms: input.duration_ms,
                bytes_to_worker: input.bytes_to_worker,
                bytes_from_worker: input.bytes_from_worker,
                close_reason: input.close_reason,
            },
        )
    }

    /// Builds a read-only snapshot across `24h`, `7d`, and `all_time`.
    pub fn snapshot(&self) -> CreditsSnapshot {
        self.snapshot_with_display_names(&HashMap::new())
    }

    /// Builds a read-only snapshot, enriching member rows with current `display_name` values
    /// keyed by `member_id`.
    pub fn snapshot_with_display_names(
        &self,
        display_names: &HashMap<String, String>,
    ) -> CreditsSnapshot {
        let now_utc = self.clock.now_utc();
        let cutoff_24h = now_utc.saturating_sub(WINDOW_24H_SECS);
        let cutoff_7d = now_utc.saturating_sub(WINDOW_7D_SECS);

        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut all_time_map: BTreeMap<String, WindowCounters> = guard.archived_totals.clone();
        let mut w24_map: BTreeMap<String, WindowCounters> = BTreeMap::new();
        let mut w7_map: BTreeMap<String, WindowCounters> = BTreeMap::new();

        for p in all_time_map.keys() {
            w24_map.entry(p.clone()).or_default();
            w7_map.entry(p.clone()).or_default();
        }

        for rec in &guard.retained {
            let delta = SideCounters::from_kind(&rec.kind);
            apply_record_to_map(&mut all_time_map, rec, delta);
            if rec.recorded_at_utc >= cutoff_7d {
                apply_record_to_map(&mut w7_map, rec, delta);
            }
            if rec.recorded_at_utc >= cutoff_24h {
                apply_record_to_map(&mut w24_map, rec, delta);
            }
        }

        let mut net_sum_24h = CreditNetDto::default();
        let mut net_sum_7d = CreditNetDto::default();
        let mut net_sum_all_time = CreditNetDto::default();
        let mut participants = Vec::with_capacity(all_time_map.len());

        for (participant, all_time) in all_time_map {
            let last_24h =
                CreditWindowDto::from(w24_map.get(&participant).copied().unwrap_or_default());
            let last_7d =
                CreditWindowDto::from(w7_map.get(&participant).copied().unwrap_or_default());
            let all_time_dto = CreditWindowDto::from(all_time);

            net_sum_24h.accumulate(&last_24h.net);
            net_sum_7d.accumulate(&last_7d.net);
            net_sum_all_time.accumulate(&all_time_dto.net);

            let member_id = guard.participant_member_ids.get(&participant).cloned();
            let display_name = member_id
                .as_deref()
                .and_then(|id| display_names.get(id))
                .cloned()
                .or_else(|| display_names.get(&participant).cloned());

            participants.push(ParticipantCreditsDto {
                participant,
                member_id,
                display_name,
                last_24h,
                last_7d,
                all_time: all_time_dto,
            });
        }

        participants.sort_by(|a, b| {
            let volume_a = a
                .all_time
                .contributed
                .core_ms
                .saturating_add(a.all_time.used.core_ms);
            let volume_b = b
                .all_time
                .contributed
                .core_ms
                .saturating_add(b.all_time.used.core_ms);
            volume_b
                .cmp(&volume_a)
                .then_with(|| {
                    b.all_time
                        .contributed
                        .tasks
                        .saturating_add(b.all_time.used.tasks)
                        .cmp(
                            &a.all_time
                                .contributed
                                .tasks
                                .saturating_add(a.all_time.used.tasks),
                        )
                })
                .then_with(|| a.participant.cmp(&b.participant))
        });

        let retained_records = guard.retained.len();
        let archived_records = guard.archived_records;
        let window_truncated = guard
            .max_evicted_recorded_at_utc
            .is_some_and(|evicted_at| evicted_at >= cutoff_7d);

        CreditsSnapshot {
            display_only: true,
            note: DISPLAY_ONLY_NOTE.to_string(),
            generated_at_utc: now_utc,
            total_records: archived_records.saturating_add(retained_records as u64),
            retained_records,
            archived_records,
            window_truncated,
            net_sum_24h,
            net_sum_7d,
            net_sum_all_time,
            participants,
        }
    }

    fn append_entry(
        &self,
        credited_participant: String,
        debited_participant: String,
        credited_member_id: Option<String>,
        debited_member_id: Option<String>,
        kind: UsageKind,
    ) -> GridResult<UsageRecord> {
        let recorded_at_utc = self.clock.now_utc();
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let record_id = guard.next_record_id;
        let record = UsageRecord {
            record_id,
            recorded_at_utc,
            credited_participant,
            debited_participant,
            credited_member_id,
            debited_member_id,
            kind,
        };

        if let Some(ref mut p) = guard.persistent {
            let line = encode_journal_line(&record)?;
            p.journal_file.write_all(&line).map_err(|e| {
                GridError::Config(format!(
                    "Cannot append credits journal '{}': {e}",
                    p.journal_path.display()
                ))
            })?;
            p.journal_file.sync_data().map_err(|e| {
                GridError::Config(format!(
                    "Cannot sync credits journal '{}': {e}",
                    p.journal_path.display()
                ))
            })?;
            p.journal_bytes = p.journal_bytes.saturating_add(line.len() as u64);
        }

        guard.next_record_id = record_id.saturating_add(1);
        record_member_hint(&mut guard.participant_member_ids, &record);
        guard.retained_estimated_bytes = guard
            .retained_estimated_bytes
            .saturating_add(estimate_record_bytes(&record));
        guard.retained.push_back(record.clone());
        guard.enforce_bounds_if_needed()?;
        Ok(record)
    }
}

impl LedgerInner {
    fn fold_oldest(&mut self) -> bool {
        let Some(rec) = self.retained.pop_front() else {
            return false;
        };
        let delta = SideCounters::from_kind(&rec.kind);
        apply_record_to_map(&mut self.archived_totals, &rec, delta);
        self.archived_records = self.archived_records.saturating_add(1);
        self.max_evicted_recorded_at_utc = Some(
            self.max_evicted_recorded_at_utc
                .map_or(rec.recorded_at_utc, |prev| prev.max(rec.recorded_at_utc)),
        );
        self.retained_estimated_bytes = self
            .retained_estimated_bytes
            .saturating_sub(estimate_record_bytes(&rec));
        true
    }

    fn enforce_bounds_if_needed(&mut self) -> GridResult<()> {
        let max_records = self.config.max_records;
        let max_bytes = self.config.max_bytes;
        let mut need_compact = self.retained.len() > max_records;

        if let Some(ref p) = self.persistent {
            if p.snapshot_bytes.saturating_add(p.journal_bytes) > max_bytes
                && (self.retained.len() > 1 || p.journal_bytes > 0)
            {
                need_compact = true;
            }
        } else if self.retained_estimated_bytes > max_bytes && !self.retained.is_empty() {
            need_compact = true;
        }

        if !need_compact {
            return Ok(());
        }

        while self.retained.len() > max_records {
            if !self.fold_oldest() {
                break;
            }
        }

        if self.persistent.is_some() {
            // Ensure the rewritten snapshot itself respects `max_bytes` where possible, while
            // never discarding archived participant totals.
            let mut snapshot_file = self.to_snapshot_file();
            let mut encoded = encode_snapshot_bytes(&snapshot_file)?;
            while (encoded.len() as u64) > max_bytes && !self.retained.is_empty() {
                // Evict a small batch when serialized size still exceeds the budget so large
                // catches never turn O(N^2).
                let excess = (encoded.len() as u64).saturating_sub(max_bytes);
                let step = if excess > 4096 {
                    (self.retained.len() / 8).max(1)
                } else {
                    1
                };
                for _ in 0..step {
                    if !self.fold_oldest() {
                        break;
                    }
                }
                snapshot_file = self.to_snapshot_file();
                encoded = encode_snapshot_bytes(&snapshot_file)?;
            }

            let p = self.persistent.as_mut().expect("checked persistent");
            refuse_symlink(&p.snapshot_path)?;
            refuse_symlink(&p.journal_path)?;
            write_private_atomic(&p.snapshot_path, &encoded)?;
            p.journal_file.set_len(0).map_err(|e| {
                GridError::Config(format!(
                    "Cannot truncate credits journal '{}': {e}",
                    p.journal_path.display()
                ))
            })?;
            p.journal_file.seek(SeekFrom::Start(0)).map_err(|e| {
                GridError::Config(format!(
                    "Cannot rewind credits journal '{}': {e}",
                    p.journal_path.display()
                ))
            })?;
            p.journal_file.sync_all().map_err(|e| {
                GridError::Config(format!(
                    "Cannot sync truncated credits journal '{}': {e}",
                    p.journal_path.display()
                ))
            })?;
            p.snapshot_bytes = encoded.len() as u64;
            p.journal_bytes = 0;
        } else {
            while self.retained_estimated_bytes > max_bytes && !self.retained.is_empty() {
                if !self.fold_oldest() {
                    break;
                }
            }
        }

        Ok(())
    }

    fn to_snapshot_file(&self) -> CreditsSnapshotFile {
        CreditsSnapshotFile {
            version: CREDITS_SNAPSHOT_VERSION,
            next_record_id: self.next_record_id,
            archived_records: self.archived_records,
            max_evicted_recorded_at_utc: self.max_evicted_recorded_at_utc,
            archived_totals: self.archived_totals.clone(),
            participant_member_ids: self.participant_member_ids.clone(),
            retained: self.retained.iter().cloned().collect(),
        }
    }
}

fn record_member_hint(hints: &mut BTreeMap<String, String>, rec: &UsageRecord) {
    if let Some(ref mid) = rec.credited_member_id {
        hints
            .entry(rec.credited_participant.clone())
            .or_insert_with(|| mid.clone());
    }
    if let Some(ref mid) = rec.debited_member_id {
        hints
            .entry(rec.debited_participant.clone())
            .or_insert_with(|| mid.clone());
    }
}

fn estimate_record_bytes(rec: &UsageRecord) -> u64 {
    let base = 240u64
        .saturating_add(rec.credited_participant.len() as u64)
        .saturating_add(rec.debited_participant.len() as u64)
        .saturating_add(
            rec.credited_member_id
                .as_ref()
                .map_or(0, |s| s.len() as u64),
        )
        .saturating_add(rec.debited_member_id.as_ref().map_or(0, |s| s.len() as u64));
    match &rec.kind {
        UsageKind::Task { .. } => base.saturating_add(96),
        UsageKind::ModelTunnel {
            service,
            close_reason,
            ..
        } => base
            .saturating_add(96)
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

fn encode_snapshot_bytes(snapshot: &CreditsSnapshotFile) -> GridResult<Vec<u8>> {
    let payload_val = serde_json::to_value(snapshot)
        .map_err(|e| GridError::Config(format!("Failed to serialize credits snapshot: {e}")))?;
    let canonical = serde_json::to_vec(&payload_val)
        .map_err(|e| GridError::Config(format!("Failed to canonicalize credits snapshot: {e}")))?;
    let envelope = SnapshotEnvelope {
        version: CREDITS_SNAPSHOT_VERSION,
        blake3: hash_domain(DOMAIN_SNAPSHOT_V1, &canonical),
        payload: payload_val,
    };
    serde_json::to_vec_pretty(&envelope)
        .map_err(|e| GridError::Config(format!("Failed to encode credits snapshot envelope: {e}")))
}

fn read_verified_snapshot(path: &Path) -> GridResult<(CreditsSnapshotFile, u64)> {
    let bytes = fs::read(path).map_err(|e| {
        GridError::Config(format!(
            "Cannot read credits snapshot '{}': {e}",
            path.display()
        ))
    })?;
    let file_len = bytes.len() as u64;
    let envelope: SnapshotEnvelope = serde_json::from_slice(&bytes).map_err(|e| {
        GridError::Config(format!(
            "Corrupt credits snapshot '{}': {e}",
            path.display()
        ))
    })?;
    if envelope.version != CREDITS_SNAPSHOT_VERSION {
        return Err(GridError::Config(format!(
            "Unsupported credits snapshot version {} in '{}'",
            envelope.version,
            path.display()
        )));
    }
    let canonical = serde_json::to_vec(&envelope.payload).map_err(|e| {
        GridError::Config(format!(
            "Failed to canonicalize credits snapshot '{}': {e}",
            path.display()
        ))
    })?;
    let expected = hash_domain(DOMAIN_SNAPSHOT_V1, &canonical);
    if expected != envelope.blake3 {
        return Err(GridError::Config(format!(
            "Credits snapshot integrity check failed in '{}'",
            path.display()
        )));
    }
    let snapshot: CreditsSnapshotFile = serde_json::from_value(envelope.payload).map_err(|e| {
        GridError::Config(format!(
            "Invalid credits snapshot payload in '{}': {e}",
            path.display()
        ))
    })?;
    if snapshot.version != CREDITS_SNAPSHOT_VERSION {
        return Err(GridError::Config(format!(
            "Mismatched inner credits snapshot version {} in '{}'",
            snapshot.version,
            path.display()
        )));
    }
    Ok((snapshot, file_len))
}

fn encode_journal_line(record: &UsageRecord) -> GridResult<Vec<u8>> {
    let payload_val = serde_json::to_value(record)
        .map_err(|e| GridError::Config(format!("Failed to serialize credits journal line: {e}")))?;
    let canonical = serde_json::to_vec(&payload_val).map_err(|e| {
        GridError::Config(format!("Failed to canonicalize credits journal line: {e}"))
    })?;
    let envelope = JournalLineEnvelope {
        seq: record.record_id,
        blake3: hash_domain(DOMAIN_JOURNAL_V1, &canonical),
        payload: payload_val,
    };
    let mut line = serde_json::to_vec(&envelope).map_err(|e| {
        GridError::Config(format!("Failed to encode credits journal envelope: {e}"))
    })?;
    line.push(b'\n');
    Ok(line)
}

fn parse_journal_line(raw_line: &[u8]) -> Result<UsageRecord, String> {
    let envelope: JournalLineEnvelope =
        serde_json::from_slice(raw_line).map_err(|e| format!("invalid JSON: {e}"))?;
    let canonical = serde_json::to_vec(&envelope.payload)
        .map_err(|e| format!("failed to canonicalize payload: {e}"))?;
    let expected = hash_domain(DOMAIN_JOURNAL_V1, &canonical);
    if expected != envelope.blake3 {
        return Err("BLAKE3 mismatch".to_string());
    }
    let record: UsageRecord = serde_json::from_value(envelope.payload)
        .map_err(|e| format!("invalid UsageRecord payload: {e}"))?;
    if record.record_id != envelope.seq {
        return Err(format!(
            "sequence mismatch: envelope {} != record {}",
            envelope.seq, record.record_id
        ));
    }
    Ok(record)
}

fn open_and_replay_journal(
    path: &Path,
    snapshot_next_record_id: u64,
) -> GridResult<(File, Vec<UsageRecord>, u64)> {
    let mut file = open_private_journal_file(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|e| {
        GridError::Config(format!(
            "Cannot read credits journal '{}': {e}",
            path.display()
        ))
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
                return Err(GridError::Config(format!(
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
                return Err(GridError::Config(format!(
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
            GridError::Config(format!(
                "Cannot truncate credits journal '{}': {e}",
                path.display()
            ))
        })?;
        file.sync_all().map_err(|e| {
            GridError::Config(format!(
                "Cannot sync truncated credits journal '{}': {e}",
                path.display()
            ))
        })?;
        valid_end = new_len;
    }

    file.seek(SeekFrom::End(0)).map_err(|e| {
        GridError::Config(format!(
            "Cannot seek credits journal '{}': {e}",
            path.display()
        ))
    })?;
    Ok((file, records, valid_end as u64))
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
            .append(true)
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
        opts.create(true).read(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts.open(path).map_err(|e| {
            GridError::Config(format!(
                "Cannot open credits journal '{}': {e}",
                path.display()
            ))
        })?;
        Ok(file)
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
        })
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
        Ok(())
    }
}
