//! Prometheus-compatible metrics collection and exposition for the master (`ECO-4`).
//!
//! All series labels use closed, bounded enumerations (`state`, `link`) so secrets, bearer
//! tokens, task commands/inputs, worker IPs, and task IDs can never leak or cause cardinality
//! blow-up on `/metrics`.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::blob_store::BlobStoreStats;
use crate::queue::{QueueStats, TaskState};
use crate::registry::{WorkerInfo, WorkerStatus};

/// HTTP `Content-Type` header for Prometheus text exposition format 0.0.4.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Bucket upper bounds (in seconds) used by latency and fsync duration histograms.
pub const DEFAULT_LATENCY_BUCKETS_SECS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Closed set of worker transport link categories exposed as the `link` Prometheus label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerLinkKind {
    TcpLan,
    QuicDirect,
    QuicRelay,
    Other,
}

impl WorkerLinkKind {
    /// Static Prometheus label value for this link category.
    pub const fn as_label(self) -> &'static str {
        match self {
            WorkerLinkKind::TcpLan => "tcp_lan",
            WorkerLinkKind::QuicDirect => "quic_direct",
            WorkerLinkKind::QuicRelay => "quic_relay",
            WorkerLinkKind::Other => "other",
        }
    }
}

/// Classifies a worker's `link:` tags into a closed static label set.
///
/// Unrecognized or absent tags collapse to `TcpLan` (when no `link:` tag is present) or `Other`
/// so untrusted tag strings can never reach Prometheus output.
pub fn classify_worker_link(tags: &[String]) -> WorkerLinkKind {
    for tag in tags {
        if let Some(rest) = tag.strip_prefix("link:") {
            let lower = rest.to_ascii_lowercase();
            if lower.contains("relay") || lower.contains("derp") {
                return WorkerLinkKind::QuicRelay;
            }
            if lower.contains("quic") || lower.contains("direct") || lower.contains("p2p") {
                return WorkerLinkKind::QuicDirect;
            }
            if lower.contains("tcp") || lower.contains("lan") {
                return WorkerLinkKind::TcpLan;
            }
            return WorkerLinkKind::Other;
        }
    }
    WorkerLinkKind::TcpLan
}

/// Snapshot of a fixed-bucket histogram ready for rendering.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub bounds: &'static [f64],
    /// Non-cumulative per-bucket counts, length `bounds.len() + 1` (last slot is overflow).
    pub bucket_counts: Vec<u64>,
    pub count: u64,
    pub sum_secs: f64,
}

/// Lock-free fixed-bucket duration histogram stored in integer nanoseconds.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    bounds_nanos: Vec<u128>,
    /// One slot per upper bound plus one overflow (`+Inf` tail) slot.
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_nanos: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new(DEFAULT_LATENCY_BUCKETS_SECS)
    }
}

impl Histogram {
    /// Creates a lock-free histogram using the given sorted finite upper bounds in seconds.
    pub fn new(bounds: &'static [f64]) -> Self {
        let bounds_nanos = bounds
            .iter()
            .map(|&s| {
                if s <= 0.0 || !s.is_finite() {
                    0
                } else {
                    (s * 1_000_000_000.0).round() as u128
                }
            })
            .collect();
        let buckets = (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            bounds,
            bounds_nanos,
            buckets,
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }

    /// Records a single duration observation.
    pub fn observe(&self, duration: Duration) {
        let nanos = duration.as_nanos();
        let idx = self
            .bounds_nanos
            .iter()
            .position(|&bound| nanos <= bound)
            .unwrap_or(self.bounds_nanos.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        let clamped = u64::try_from(nanos).unwrap_or(u64::MAX);
        self.sum_nanos.fetch_add(clamped, Ordering::Relaxed);
    }

    /// Reads an instantaneous snapshot of buckets, count, and sum.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let bucket_counts = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        HistogramSnapshot {
            bounds: self.bounds,
            bucket_counts,
            count: self.count.load(Ordering::Relaxed),
            sum_secs: self.sum_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
        }
    }
}

const RATE_WINDOW_SECS: usize = 60;
const COUNT_MASK: u64 = 0x00FF_FFFF;
const EPOCH_SHIFT: u32 = 24;

/// Lock-free 60-second sliding-window rate tracker anchored to a monotonic creation `Instant`.
///
/// Anchoring to `started_at` avoids ever subtracting a window `Duration` from `Instant::now()`.
#[derive(Debug)]
pub struct TaskRateWindow {
    started_at: Instant,
    slots: [AtomicU64; RATE_WINDOW_SECS],
}

impl Default for TaskRateWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskRateWindow {
    /// Creates an empty 60-second sliding window anchored to `Instant::now()`.
    pub fn new() -> Self {
        Self::with_started_at(Instant::now())
    }

    /// Creates an empty window anchored to an explicit `Instant`.
    pub fn with_started_at(started_at: Instant) -> Self {
        Self {
            started_at,
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Returns the anchor `Instant` used to compute relative epochs.
    pub fn started_at(&self) -> Instant {
        self.started_at
    }

    /// Records one completed event at `Instant::now()`.
    pub fn record(&self) {
        self.record_at(Instant::now());
    }

    /// Records one completed event at `now`.
    pub fn record_at(&self, now: Instant) {
        let sec = now.saturating_duration_since(self.started_at).as_secs();
        let epoch = sec.saturating_add(1);
        let idx = (sec % RATE_WINDOW_SECS as u64) as usize;
        let slot = &self.slots[idx];

        let mut cur = slot.load(Ordering::Relaxed);
        loop {
            let cur_epoch = cur >> EPOCH_SHIFT;
            let cur_count = cur & COUNT_MASK;
            let next = if cur_epoch == epoch {
                (epoch << EPOCH_SHIFT) | cur_count.saturating_add(1).min(COUNT_MASK)
            } else if cur_epoch > epoch {
                // Concurrent thread already advanced this ring slot past `epoch`.
                break;
            } else {
                (epoch << EPOCH_SHIFT) | 1
            };
            match slot.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Returns the observed completion rate per second over the trailing 60-second window.
    pub fn rate_per_sec(&self) -> f64 {
        self.rate_per_sec_at(Instant::now())
    }

    /// Computes the trailing 60-second completion rate relative to `now`.
    pub fn rate_per_sec_at(&self, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(self.started_at);
        let cur_epoch = elapsed.as_secs().saturating_add(1);
        let mut total = 0u64;
        for slot in &self.slots {
            let val = slot.load(Ordering::Relaxed);
            if val == 0 {
                continue;
            }
            let epoch = val >> EPOCH_SHIFT;
            if epoch <= cur_epoch && cur_epoch - epoch < RATE_WINDOW_SECS as u64 {
                total = total.saturating_add(val & COUNT_MASK);
            }
        }
        let window_secs = elapsed.as_secs_f64().clamp(1.0, RATE_WINDOW_SECS as f64);
        total as f64 / window_secs
    }
}

/// Queue-level monotonic counters, throughput window, and scheduling latency histogram.
#[derive(Debug, Default)]
pub struct QueueMetrics {
    submitted_total: AtomicU64,
    completed_total: AtomicU64,
    failed_total: AtomicU64,
    cancelled_total: AtomicU64,
    completion_rate: TaskRateWindow,
    scheduling_latency: Histogram,
}

/// Point-in-time snapshot of [`QueueMetrics`].
#[derive(Debug, Clone)]
pub struct QueueMetricsSnapshot {
    pub submitted_total: u64,
    pub completed_total: u64,
    pub failed_total: u64,
    pub cancelled_total: u64,
    pub tasks_per_second: f64,
    pub scheduling_latency: HistogramSnapshot,
}

impl QueueMetrics {
    /// Creates a new `QueueMetrics` instance with zeroed counters and a fresh rate window.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the monotonic submitted-task counter.
    pub fn record_submitted(&self) {
        self.submitted_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Observes queueing latency (`Queued -> Scheduled`).
    pub fn observe_scheduling_latency(&self, latency: Duration) {
        self.scheduling_latency.observe(latency);
    }

    /// Records a task reaching a terminal state.
    pub fn record_terminal(&self, state: TaskState) {
        match state {
            TaskState::Completed => {
                self.completed_total.fetch_add(1, Ordering::Relaxed);
                self.completion_rate.record();
            }
            TaskState::Failed => {
                self.failed_total.fetch_add(1, Ordering::Relaxed);
            }
            TaskState::Cancelled => {
                self.cancelled_total.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Captures a consistent view of queue metrics for rendering.
    pub fn snapshot(&self) -> QueueMetricsSnapshot {
        QueueMetricsSnapshot {
            submitted_total: self.submitted_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            failed_total: self.failed_total.load(Ordering::Relaxed),
            cancelled_total: self.cancelled_total.load(Ordering::Relaxed),
            tasks_per_second: self.completion_rate.rate_per_sec(),
            scheduling_latency: self.scheduling_latency.snapshot(),
        }
    }
}

/// Point-in-time snapshot of durable `TaskStore` status for `/metrics`.
#[derive(Debug, Clone)]
pub struct StoreMetricsSnapshot {
    pub configured: bool,
    pub healthy: bool,
    pub journal_bytes: u64,
    pub journal_records: u64,
    pub fsync_duration: HistogramSnapshot,
}

impl Default for StoreMetricsSnapshot {
    fn default() -> Self {
        Self {
            configured: false,
            healthy: true,
            journal_bytes: 0,
            journal_records: 0,
            fsync_duration: Histogram::default().snapshot(),
        }
    }
}

/// Worker entry pre-classified by connection status and transport link kind.
#[derive(Debug, Clone, Copy)]
pub struct WorkerMetricSample {
    pub status: WorkerStatus,
    pub link: WorkerLinkKind,
}

impl WorkerMetricSample {
    /// Classifies a registry `WorkerInfo` snapshot using its tags (and optional live P2P override).
    pub fn from_worker_info(info: &WorkerInfo, p2p_link_override: Option<WorkerLinkKind>) -> Self {
        Self {
            status: info.status,
            link: p2p_link_override
                .unwrap_or_else(|| classify_worker_link(&info.capabilities.tags)),
        }
    }
}

fn write_histogram(out: &mut String, name: &str, help: &str, snap: &HistogramSnapshot) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} histogram");
    let mut cumulative = 0u64;
    for (i, &bound) in snap.bounds.iter().enumerate() {
        cumulative = cumulative.saturating_add(snap.bucket_counts.get(i).copied().unwrap_or(0));
        let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
    }
    let overflow = snap
        .bucket_counts
        .get(snap.bounds.len())
        .copied()
        .unwrap_or(0);
    cumulative = cumulative.saturating_add(overflow);
    let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
    let _ = writeln!(out, "{name}_sum {:.9}", snap.sum_secs);
    let _ = writeln!(out, "{name}_count {}", snap.count);
}

/// Renders master telemetry in Prometheus text exposition format (`0.0.4`).
pub fn render_prometheus_metrics(
    queue_stats: &QueueStats,
    queue_metrics: &QueueMetricsSnapshot,
    workers: &[WorkerMetricSample],
    blob_stats: Option<&BlobStoreStats>,
    store_metrics: &StoreMetricsSnapshot,
) -> String {
    let mut out = String::with_capacity(4096);

    // Queue depth gauges by TaskState.
    let _ = writeln!(
        out,
        "# HELP oxideswarm_queue_tasks Number of retained tasks in the queue by state."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_queue_tasks gauge");
    for (state_label, count) in [
        ("queued", queue_stats.queued),
        ("scheduled", queue_stats.scheduled),
        ("running", queue_stats.running),
        ("retrying", queue_stats.retrying),
        ("completed", queue_stats.completed),
        ("failed", queue_stats.failed),
        ("cancelled", queue_stats.cancelled),
    ] {
        let _ = writeln!(
            out,
            "oxideswarm_queue_tasks{{state=\"{state_label}\"}} {count}"
        );
    }

    let _ = writeln!(
        out,
        "# HELP oxideswarm_queue_active_bytes Estimated bytes held by active non-terminal tasks."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_queue_active_bytes gauge");
    let _ = writeln!(
        out,
        "oxideswarm_queue_active_bytes {}",
        queue_stats.active_bytes
    );

    let _ = writeln!(
        out,
        "# HELP oxideswarm_queue_max_active_bytes Configured maximum active task byte budget."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_queue_max_active_bytes gauge");
    let _ = writeln!(
        out,
        "oxideswarm_queue_max_active_bytes {}",
        queue_stats.max_active_bytes
    );

    // Monotonic task throughput and totals.
    let _ = writeln!(
        out,
        "# HELP oxideswarm_tasks_submitted_total Total tasks accepted by the master queue."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_tasks_submitted_total counter");
    let _ = writeln!(
        out,
        "oxideswarm_tasks_submitted_total {}",
        queue_metrics.submitted_total
    );

    let _ = writeln!(
        out,
        "# HELP oxideswarm_tasks_terminal_total Total tasks reaching a terminal state."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_tasks_terminal_total counter");
    for (state_label, count) in [
        ("completed", queue_metrics.completed_total),
        ("failed", queue_metrics.failed_total),
        ("cancelled", queue_metrics.cancelled_total),
    ] {
        let _ = writeln!(
            out,
            "oxideswarm_tasks_terminal_total{{state=\"{state_label}\"}} {count}"
        );
    }

    let _ = writeln!(
        out,
        "# HELP oxideswarm_tasks_per_second Completed tasks per second over a 60-second sliding window."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_tasks_per_second gauge");
    let _ = writeln!(
        out,
        "oxideswarm_tasks_per_second {:.6}",
        queue_metrics.tasks_per_second
    );

    write_histogram(
        &mut out,
        "oxideswarm_scheduling_latency_seconds",
        "Time elapsed between task queueing and scheduler assignment in seconds.",
        &queue_metrics.scheduling_latency,
    );

    // Worker counts across bounded (status, link) pairs.
    let statuses = [
        WorkerStatus::Connected,
        WorkerStatus::Busy,
        WorkerStatus::Disconnected,
    ];
    let primary_links = [
        WorkerLinkKind::TcpLan,
        WorkerLinkKind::QuicDirect,
        WorkerLinkKind::QuicRelay,
    ];
    let mut matrix = [[0u64; 4]; 3];
    let mut has_other = false;
    for w in workers {
        let s_idx = match w.status {
            WorkerStatus::Connected => 0,
            WorkerStatus::Busy => 1,
            WorkerStatus::Disconnected => 2,
        };
        let l_idx = match w.link {
            WorkerLinkKind::TcpLan => 0,
            WorkerLinkKind::QuicDirect => 1,
            WorkerLinkKind::QuicRelay => 2,
            WorkerLinkKind::Other => {
                has_other = true;
                3
            }
        };
        matrix[s_idx][l_idx] = matrix[s_idx][l_idx].saturating_add(1);
    }

    let _ = writeln!(
        out,
        "# HELP oxideswarm_workers Registered workers by operational state and link transport."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_workers gauge");
    for (s_idx, status) in statuses.iter().enumerate() {
        for (l_idx, link) in primary_links.iter().enumerate() {
            let _ = writeln!(
                out,
                "oxideswarm_workers{{state=\"{}\",link=\"{}\"}} {}",
                status.as_str(),
                link.as_label(),
                matrix[s_idx][l_idx]
            );
        }
        if has_other {
            let _ = writeln!(
                out,
                "oxideswarm_workers{{state=\"{}\",link=\"other\"}} {}",
                status.as_str(),
                matrix[s_idx][3]
            );
        }
    }

    // Blob store counters & gauges.
    let blob = blob_stats.cloned().unwrap_or_default();
    let _ = writeln!(
        out,
        "# HELP oxideswarm_blob_bytes_in_total Total blob payload bytes received by the master."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_blob_bytes_in_total counter");
    let _ = writeln!(
        out,
        "oxideswarm_blob_bytes_in_total {}",
        blob.bytes_received
    );

    let _ = writeln!(
        out,
        "# HELP oxideswarm_blob_bytes_out_total Total blob chunk bytes served by the master."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_blob_bytes_out_total counter");
    let _ = writeln!(out, "oxideswarm_blob_bytes_out_total {}", blob.bytes_served);

    let _ = writeln!(
        out,
        "# HELP oxideswarm_blob_evictions_total Total blobs evicted from the master blob store."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_blob_evictions_total counter");
    let _ = writeln!(out, "oxideswarm_blob_evictions_total {}", blob.evictions);

    let _ = writeln!(
        out,
        "# HELP oxideswarm_blob_store_bytes Current stored bytes in the master blob store."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_blob_store_bytes gauge");
    let _ = writeln!(out, "oxideswarm_blob_store_bytes {}", blob.bytes);

    let _ = writeln!(
        out,
        "# HELP oxideswarm_blob_store_max_bytes Configured capacity limit of the master blob store."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_blob_store_max_bytes gauge");
    let _ = writeln!(out, "oxideswarm_blob_store_max_bytes {}", blob.max_bytes);

    let _ = writeln!(
        out,
        "# HELP oxideswarm_blob_store_objects Number of objects currently retained in the blob store."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_blob_store_objects gauge");
    let _ = writeln!(out, "oxideswarm_blob_store_objects {}", blob.blobs);

    // Durable journal & store health metrics.
    let _ = writeln!(
        out,
        "# HELP oxideswarm_journal_bytes Current size in bytes of the active task journal."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_journal_bytes gauge");
    let _ = writeln!(
        out,
        "oxideswarm_journal_bytes {}",
        store_metrics.journal_bytes
    );

    let _ = writeln!(
        out,
        "# HELP oxideswarm_journal_records Number of records appended to the active task journal."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_journal_records gauge");
    let _ = writeln!(
        out,
        "oxideswarm_journal_records {}",
        store_metrics.journal_records
    );

    write_histogram(
        &mut out,
        "oxideswarm_journal_fsync_duration_seconds",
        "Duration of durable journal and snapshot fsync operations in seconds.",
        &store_metrics.fsync_duration,
    );

    let configured_val = i32::from(store_metrics.configured);
    let healthy_val = i32::from(queue_stats.store_error.is_none() && store_metrics.healthy);
    let _ = writeln!(
        out,
        "# HELP oxideswarm_store_configured Whether a persistent task store (--state-dir) is enabled (1) or in-memory (0)."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_store_configured gauge");
    let _ = writeln!(out, "oxideswarm_store_configured {configured_val}");

    let _ = writeln!(
        out,
        "# HELP oxideswarm_store_healthy Whether task durability is healthy (1) or has encountered a write failure (0)."
    );
    let _ = writeln!(out, "# TYPE oxideswarm_store_healthy gauge");
    let _ = writeln!(out, "oxideswarm_store_healthy {healthy_val}");

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_records_cumulative_prometheus_buckets_and_overflow() {
        let hist = Histogram::default();
        hist.observe(Duration::from_micros(400)); // <= 0.0005
        hist.observe(Duration::from_millis(5)); // <= 0.005
        hist.observe(Duration::from_secs(45)); // > 30.0 (+Inf only)

        let snap = hist.snapshot();
        assert_eq!(snap.count, 3);
        assert!(snap.sum_secs > 45.0 && snap.sum_secs < 45.1);

        let mut rendered = String::new();
        write_histogram(&mut rendered, "test_lat_seconds", "test help", &snap);
        assert!(rendered.contains("test_lat_seconds_bucket{le=\"0.0005\"} 1\n"));
        assert!(rendered.contains("test_lat_seconds_bucket{le=\"0.005\"} 2\n"));
        assert!(rendered.contains("test_lat_seconds_bucket{le=\"30\"} 2\n"));
        assert!(rendered.contains("test_lat_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(rendered.contains("test_lat_seconds_count 3\n"));
    }

    #[test]
    fn task_rate_window_rolls_over_sixty_seconds_without_instant_subtraction() {
        let start = Instant::now();
        let window = TaskRateWindow::with_started_at(start);

        // Two tasks in second 0, two more at +10s.
        window.record_at(start);
        window.record_at(start + Duration::from_millis(200));
        window.record_at(start + Duration::from_secs(10));
        window.record_at(start + Duration::from_secs(10));
        let r10 = window.rate_per_sec_at(start + Duration::from_secs(10));
        assert!((r10 - 0.4).abs() < 1e-6, "got {r10}");

        // Advance past 60s so slot 0 expires out of the 60s window, while +10s remains active.
        let t65 = start + Duration::from_secs(65);
        window.record_at(t65);
        let r65 = window.rate_per_sec_at(t65);
        // Only the two events at +10s and one event at +65s are inside [6..65] (diff < 60).
        assert!((r65 - (3.0 / 60.0)).abs() < 1e-6, "got {r65}");

        // Jump far forward to expire everything prior to the final burst.
        let t200 = start + Duration::from_secs(200);
        window.record_at(t200);
        let r200 = window.rate_per_sec_at(t200);
        assert!((r200 - (1.0 / 60.0)).abs() < 1e-6, "got {r200}");
    }

    #[test]
    fn classify_worker_link_enforces_closed_label_set() {
        assert_eq!(classify_worker_link(&[]), WorkerLinkKind::TcpLan);
        assert_eq!(
            classify_worker_link(&["link:TCP/LAN".to_string()]),
            WorkerLinkKind::TcpLan
        );
        assert_eq!(
            classify_worker_link(&["link:Direct P2P (QUIC):3.2ms".to_string()]),
            WorkerLinkKind::QuicDirect
        );
        assert_eq!(
            classify_worker_link(&["link:Relay (DERP):88.0ms".to_string()]),
            WorkerLinkKind::QuicRelay
        );
        assert_eq!(
            classify_worker_link(&["link:evil\nfake_metric 999\"label\"".to_string()]),
            WorkerLinkKind::Other
        );
    }

    #[test]
    fn render_prometheus_metrics_contains_required_families_and_health_flip() {
        let qm = QueueMetrics::new();
        qm.record_submitted();
        qm.observe_scheduling_latency(Duration::from_millis(4));
        qm.record_terminal(TaskState::Completed);
        let qsnap = qm.snapshot();

        let stats = QueueStats {
            total: 3,
            queued: 1,
            scheduled: 0,
            running: 1,
            retrying: 0,
            completed: 1,
            failed: 0,
            cancelled: 0,
            active_bytes: 512,
            max_active_bytes: 1024,
            store_error: None,
        };
        let workers = vec![
            WorkerMetricSample {
                status: WorkerStatus::Connected,
                link: WorkerLinkKind::TcpLan,
            },
            WorkerMetricSample {
                status: WorkerStatus::Busy,
                link: WorkerLinkKind::QuicRelay,
            },
        ];
        let blob = BlobStoreStats {
            blobs: 2,
            bytes: 4096,
            max_bytes: 65536,
            pinned_blobs: 1,
            blobs_received: 2,
            bytes_received: 8192,
            chunks_served: 3,
            bytes_served: 12288,
            evictions: 1,
            ..Default::default()
        };
        let mut store = StoreMetricsSnapshot {
            configured: true,
            healthy: true,
            journal_bytes: 2048,
            journal_records: 14,
            fsync_duration: Histogram::default().snapshot(),
        };

        let text = render_prometheus_metrics(&stats, &qsnap, &workers, Some(&blob), &store);
        for expected in [
            "oxideswarm_queue_tasks{state=\"queued\"} 1",
            "oxideswarm_queue_tasks{state=\"running\"} 1",
            "oxideswarm_tasks_submitted_total 1",
            "oxideswarm_tasks_terminal_total{state=\"completed\"} 1",
            "oxideswarm_tasks_per_second ",
            "oxideswarm_scheduling_latency_seconds_count 1",
            "oxideswarm_workers{state=\"connected\",link=\"tcp_lan\"} 1",
            "oxideswarm_workers{state=\"busy\",link=\"quic_relay\"} 1",
            "oxideswarm_blob_bytes_in_total 8192",
            "oxideswarm_blob_bytes_out_total 12288",
            "oxideswarm_journal_bytes 2048",
            "oxideswarm_journal_records 14",
            "oxideswarm_journal_fsync_duration_seconds_count 0",
            "oxideswarm_store_configured 1",
            "oxideswarm_store_healthy 1",
        ] {
            assert!(text.contains(expected), "missing {expected} in:\n{text}");
        }

        store.healthy = false;
        let unhealthy_text =
            render_prometheus_metrics(&stats, &qsnap, &workers, Some(&blob), &store);
        assert!(unhealthy_text.contains("oxideswarm_store_healthy 0"));
    }
}
