//! `SCALE-1` single-process simulated fleet load driver against a real `oxideswarm master`.
//!
//! Spawns an isolated `oxideswarm master` subprocess, connects `N` [`WorkerClient::simulated`]
//! worker protocol loops inside this process, drives unloaded latency probes, concurrent
//! multi-submitter bursts, a quiet heartbeat window, simultaneous blob fan-out, mass disconnect /
//! reconnect storms, and journal replay / restart timing, then emits structured JSON on stdout.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;
use sysinfo::{Pid, ProcessRefreshKind, System};
use tempfile::Builder as TempBuilder;
use tokio::sync::watch;

use rusty_grid_cli::blobs::BlobConnection;
use rusty_grid_cli::connect::connect_master;
use rusty_grid_core::blob::{BlobId, TaskInput};
use rusty_grid_core::blob_transfer::upload_blob;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_core::transport::GridStream;
use rusty_grid_master::store::TaskStore;
use rusty_grid_master::TaskQueue;
use rusty_grid_worker::{
    BackoffConfig, FakeRunnerConfig, HeartbeatTracker, LinkLossAction, SimulatedLinkConfig,
    SimulatedNetworkControl, WorkerClient, WorkerConfig, EXIT_CODE_SUCCESS,
};

#[derive(Parser, Debug)]
#[command(
    name = "scale_load_driver",
    about = "OxideSwarm SCALE-1 fleet load driver"
)]
struct Args {
    /// Path to the `oxideswarm` executable (defaults to sibling target/release/oxideswarm).
    #[arg(long)]
    master_bin: Option<PathBuf>,
    /// Parent directory for ephemeral state, blobs, and fan-out sandboxes (use NTFS on Windows).
    #[arg(long)]
    temp_root: Option<PathBuf>,
    /// Number of simulated workers to register concurrently (default 1000, quick 64).
    #[arg(long)]
    workers: Option<usize>,
    /// Number of concurrent submitter TCP connections during burst phase (default 10, quick 4).
    #[arg(long)]
    submitters: Option<usize>,
    /// Total tasks in the concurrent burst phase (default 2000, quick 160).
    #[arg(long)]
    tasks: Option<usize>,
    /// Sequential unloaded latency probes (default 32, quick 10).
    #[arg(long)]
    unloaded_probes: Option<usize>,
    /// Artificial task execution time in ms during burst phase (default 5, quick 2).
    #[arg(long)]
    fake_exec_ms: Option<u64>,
    /// Master heartbeat interval in seconds (default 3, quick 1).
    #[arg(long)]
    heartbeat_secs: Option<u64>,
    /// Quiet steady-state heartbeat sampling window in seconds (default 6, quick 2).
    #[arg(long)]
    heartbeat_window_secs: Option<u64>,
    /// Number of workers receiving simultaneous blob inputs in fan-out phase (default 100, quick 16).
    #[arg(long)]
    blob_fanout_workers: Option<usize>,
    /// Size in bytes of the deterministic input blob uploaded for fan-out (default 10 MiB, quick 1 MiB).
    #[arg(long)]
    blob_size_bytes: Option<usize>,
    /// Fixed one-way link latency in ms injected on simulated worker streams.
    #[arg(long, default_value_t = 0)]
    base_latency_ms: u64,
    /// Maximum uniform one-way FIFO jitter in ms injected on simulated worker streams.
    #[arg(long, default_value_t = 0)]
    jitter_ms: u64,
    /// Per-chunk packet loss probability (`0.0..=1.0`, models `RetransmitStall`).
    #[arg(long, default_value_t = 0.0)]
    loss_prob: f64,
    /// Initial reconnect backoff in ms for simulated workers (default 500, quick 50).
    #[arg(long)]
    reconnect_initial_ms: Option<u64>,
    /// Enable fast reduced-scale defaults for CI smoke runs.
    #[arg(long, default_value_t = false)]
    quick: bool,
}

#[derive(Debug, Clone)]
struct ResolvedConfig {
    master_bin: PathBuf,
    temp_root: PathBuf,
    workers: usize,
    submitters: usize,
    tasks: usize,
    unloaded_probes: usize,
    fake_exec_ms: u64,
    heartbeat_secs: u64,
    heartbeat_window_secs: u64,
    blob_fanout_workers: usize,
    blob_size_bytes: usize,
    base_latency_ms: u64,
    jitter_ms: u64,
    loss_prob: f64,
    reconnect_initial_ms: u64,
    quick: bool,
}

impl Args {
    fn resolve(self) -> Result<ResolvedConfig, String> {
        let q = self.quick;
        let master_bin = match self.master_bin {
            Some(p) => p,
            None => default_master_bin()?,
        };
        let temp_root = self.temp_root.unwrap_or_else(std::env::temp_dir);
        let workers = self.workers.unwrap_or(if q { 64 } else { 1000 }).max(1);
        let submitters = self.submitters.unwrap_or(if q { 4 } else { 10 }).max(1);
        let tasks = self.tasks.unwrap_or(if q { 160 } else { 2000 }).max(1);
        let unloaded_probes = self
            .unloaded_probes
            .unwrap_or(if q { 10 } else { 32 })
            .max(1);
        let fake_exec_ms = self.fake_exec_ms.unwrap_or(if q { 2 } else { 5 });
        let heartbeat_secs = self.heartbeat_secs.unwrap_or(if q { 1 } else { 3 }).max(1);
        let heartbeat_window_secs = self
            .heartbeat_window_secs
            .unwrap_or(if q { 2 } else { 6 })
            .max(1);
        let blob_fanout_workers = self
            .blob_fanout_workers
            .unwrap_or(if q { 16 } else { 100 })
            .min(workers);
        let blob_size_bytes = self
            .blob_size_bytes
            .unwrap_or(if q { 1_048_576 } else { 10_485_760 })
            .max(1);
        let reconnect_initial_ms = self
            .reconnect_initial_ms
            .unwrap_or(if q { 50 } else { 500 })
            .max(10);

        Ok(ResolvedConfig {
            master_bin,
            temp_root,
            workers,
            submitters,
            tasks,
            unloaded_probes,
            fake_exec_ms,
            heartbeat_secs,
            heartbeat_window_secs,
            blob_fanout_workers,
            blob_size_bytes,
            base_latency_ms: self.base_latency_ms,
            jitter_ms: self.jitter_ms,
            loss_prob: self.loss_prob,
            reconnect_initial_ms,
            quick: q,
        })
    }
}

fn default_master_bin() -> Result<PathBuf, String> {
    let exe_name = if cfg!(windows) {
        "oxideswarm.exe"
    } else {
        "oxideswarm"
    };
    let current = std::env::current_exe().map_err(|e| format!("current_exe failed: {e}"))?;
    let bin = current
        .parent()
        .and_then(Path::parent)
        .map(|dir| dir.join(exe_name))
        .ok_or_else(|| "cannot derive target dir from current_exe".to_string())?;
    Ok(bin)
}

#[derive(Debug, Serialize, Default, Clone)]
struct StatsSummary {
    count: usize,
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

fn compute_stats(samples: &mut [f64]) -> StatsSummary {
    if samples.is_empty() {
        return StatsSummary::default();
    }
    samples.sort_by(|a, b| a.total_cmp(b));
    let n = samples.len();
    let sum: f64 = samples.iter().sum();
    let pct = |q: f64| -> f64 {
        let idx = ((n - 1) as f64 * q).round() as usize;
        samples[idx.min(n - 1)]
    };
    StatsSummary {
        count: n,
        min: samples[0],
        p50: pct(0.50),
        p95: pct(0.95),
        p99: pct(0.99),
        max: samples[n - 1],
        mean: sum / n as f64,
    }
}

#[derive(Debug, Serialize)]
struct ScaleReport {
    config: ConfigReport,
    registration: RegistrationReport,
    unloaded: UnloadedReport,
    burst: BurstReport,
    heartbeat_quiet: HeartbeatReport,
    blob_fanout: BlobFanoutReport,
    reconnect: ReconnectReport,
    master_process: ProcessReport,
    persistence: PersistenceReport,
}

#[derive(Debug, Serialize)]
struct ConfigReport {
    workers: usize,
    submitters: usize,
    tasks: usize,
    unloaded_probes: usize,
    fake_exec_ms: u64,
    heartbeat_secs: u64,
    heartbeat_window_secs: u64,
    blob_fanout_workers: usize,
    blob_size_bytes: usize,
    base_latency_ms: u64,
    jitter_ms: u64,
    loss_prob: f64,
    reconnect_initial_ms: u64,
    quick: bool,
}

#[derive(Debug, Serialize)]
struct RegistrationReport {
    convergence_ms: f64,
    workers_registered: usize,
    total_handshakes: u64,
}

#[derive(Debug, Serialize)]
struct UnloadedReport {
    probes: usize,
    submit_to_assign_ms: StatsSummary,
    submit_to_complete_ms: StatsSummary,
}

#[derive(Debug, Serialize)]
struct BurstReport {
    tasks: usize,
    failed: usize,
    wall_ms: f64,
    tasks_per_sec: f64,
    submit_to_assign_ms: StatsSummary,
    submit_to_complete_ms: StatsSummary,
}

#[derive(Debug, Serialize)]
struct HeartbeatReport {
    window_secs: f64,
    acks_delta: u64,
    acks_per_sec: f64,
    rx_bytes_delta: u64,
    tx_bytes_delta: u64,
    rx_bytes_per_sec: f64,
    tx_bytes_per_sec: f64,
    worker_last_latency_ms: StatsSummary,
    steady_rss_bytes: u64,
    steady_cpu_pct: f64,
}

#[derive(Debug, Serialize)]
struct BlobFanoutReport {
    workers: usize,
    blob_size_bytes: usize,
    wall_ms: f64,
    rx_bytes: u64,
    tx_bytes: u64,
    throughput_mb_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct ReconnectReport {
    handshakes_before: u64,
    handshakes_after: u64,
    convergence_ms: f64,
    post_tasks: usize,
    post_failed: usize,
    loss_events: u64,
}

#[derive(Debug, Serialize)]
struct ProcessReport {
    peak_rss_bytes: u64,
    steady_rss_bytes: u64,
    peak_cpu_pct: f64,
    steady_cpu_pct: f64,
    samples: usize,
}

#[derive(Debug, Serialize)]
struct PersistenceReport {
    journal_bytes: u64,
    snapshot_bytes: u64,
    state_dir_bytes: u64,
    recovered_snapshot_tasks: usize,
    recovered_journal_records: usize,
    in_process_replay_ms: f64,
    master_restart_ready_ms: f64,
}

struct ChildGuard {
    child: Option<Child>,
    stderr_lines: Arc<Mutex<VecDeque<String>>>,
}

impl ChildGuard {
    fn spawn_master(
        cfg: &ResolvedConfig,
        secret_path: &Path,
        master_home: &Path,
        state_dir: &Path,
        blob_dir: &Path,
        port_file: &Path,
        dashboard_port_file: &Path,
    ) -> Result<Self, String> {
        let max_conns = (cfg.workers * 4 + cfg.submitters * 4 + 128).max(4096);
        let max_pending_hs = (cfg.workers + cfg.submitters + 64).max(1024);
        let max_queue = cfg.tasks.max(10_000) * 2;
        let max_retained = cfg.tasks.max(10_000) * 2;

        let mut cmd = Command::new(&cfg.master_bin);
        cmd.arg("--secret-file")
            .arg(secret_path)
            .arg("master")
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--port-file")
            .arg(port_file)
            .arg("--dashboard-port")
            .arg("0")
            .arg("--dashboard-port-file")
            .arg(dashboard_port_file)
            .arg("--state-dir")
            .arg(state_dir)
            .arg("--blob-dir")
            .arg(blob_dir)
            .arg("--heartbeat-interval")
            .arg(cfg.heartbeat_secs.to_string())
            .arg("--heartbeat-timeout")
            .arg((cfg.heartbeat_secs * 4).to_string())
            .arg("--session-resume-hold-secs")
            .arg("0")
            .arg("--max-connections")
            .arg(max_conns.to_string())
            .arg("--max-pending-handshakes")
            .arg(max_pending_hs.to_string())
            .arg("--max-queue-size")
            .arg(max_queue.to_string())
            .arg("--max-retained-tasks")
            .arg(max_retained.to_string())
            .env("HOME", master_home)
            .env("USERPROFILE", master_home)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_MASTER_KEY")
            .env_remove("RUSTY_GRID_STATE_DIR")
            .env_remove("RUSTY_GRID_BLOB_DIR")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn {}: {e}", cfg.master_bin.display()))?;
        let stderr = child.stderr.take();
        let stderr_lines = Arc::new(Mutex::new(VecDeque::with_capacity(64)));
        if let Some(pipe) = stderr {
            let lines_buf = Arc::clone(&stderr_lines);
            std::thread::spawn(move || {
                use std::io::{BufRead, BufReader};
                let reader = BufReader::new(pipe);
                for line in reader.lines().map_while(Result::ok) {
                    let mut guard = lines_buf.lock().expect("stderr_lines mutex");
                    if guard.len() >= 64 {
                        guard.pop_front();
                    }
                    guard.push_back(line);
                }
            });
        }
        Ok(Self {
            child: Some(child),
            stderr_lines,
        })
    }

    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    fn stderr_tail(&self) -> String {
        self.stderr_lines
            .lock()
            .expect("stderr_lines mutex")
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn wait_for_port_file(
        &mut self,
        port_file: &Path,
        timeout: Duration,
    ) -> Result<u16, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(child) = self.child.as_mut() {
                if let Ok(Some(status)) = child.try_wait() {
                    return Err(format!(
                        "master exited prematurely ({status}); stderr tail:\n{}",
                        self.stderr_tail()
                    ));
                }
            }
            if let Ok(raw) = std::fs::read_to_string(port_file) {
                if let Ok(port) = raw.trim().parse::<u16>() {
                    if port > 0 {
                        return Ok(port);
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for master port file; stderr tail:\n{}",
                    self.stderr_tail()
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone)]
struct ProcessSample {
    t: Instant,
    rss_bytes: u64,
    cpu_pct: f32,
}

fn spawn_process_sampler(
    pid: u32,
) -> (
    Arc<AtomicBool>,
    Arc<Mutex<Vec<ProcessSample>>>,
    std::thread::JoinHandle<()>,
) {
    let stop = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let stop_flag = Arc::clone(&stop);
    let samples_ref = Arc::clone(&samples);

    let handle = std::thread::spawn(move || {
        let mut sys = System::new();
        let sys_pid = Pid::from_u32(pid);
        let refresh = ProcessRefreshKind::new().with_memory().with_cpu();
        sys.refresh_process_specifics(sys_pid, refresh);
        while !stop_flag.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(100));
            if sys.refresh_process_specifics(sys_pid, refresh) {
                if let Some(proc) = sys.process(sys_pid) {
                    samples_ref
                        .lock()
                        .expect("samples mutex")
                        .push(ProcessSample {
                            t: Instant::now(),
                            rss_bytes: proc.memory(),
                            cpu_pct: proc.cpu_usage(),
                        });
                }
            }
        }
    });

    (stop, samples, handle)
}

fn builtin_task() -> Task {
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: "scale1".to_string(),
            iterations: 1,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 60),
    )
}

async fn submit_and_measure(
    transport: &mut MessageTransport<GridStream>,
    task: Task,
    started_times: &Arc<Mutex<HashMap<TaskId, Instant>>>,
) -> Result<(f64, Option<f64>, bool), String> {
    let task_id = task.id;
    let t_submit = Instant::now();
    transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .map_err(|e| format!("send SubmitTask failed: {e}"))?;
    match transport
        .recv_msg::<ClientResponse>()
        .await
        .map_err(|e| format!("recv TaskCompleted failed: {e}"))?
    {
        Some(ClientResponse::TaskCompleted { result, .. }) => {
            let t_complete = Instant::now();
            let complete_ms = t_complete.duration_since(t_submit).as_secs_f64() * 1000.0;
            let assign_ms = started_times
                .lock()
                .expect("started_times mutex")
                .remove(&task_id)
                .map(|started| started.saturating_duration_since(t_submit).as_secs_f64() * 1000.0);
            Ok((
                complete_ms,
                assign_ms,
                result.exit_code == EXIT_CODE_SUCCESS,
            ))
        }
        Some(other) => Err(format!("unexpected response to SubmitTask: {other:?}")),
        None => Err("master closed submitter connection".to_string()),
    }
}

async fn query_worker_count(transport: &mut MessageTransport<GridStream>) -> Result<usize, String> {
    transport
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .map_err(|e| format!("send ClusterStatus failed: {e}"))?;
    match transport
        .recv_msg::<ClientResponse>()
        .await
        .map_err(|e| format!("recv ClusterStatus failed: {e}"))?
    {
        Some(ClientResponse::ClusterStatus { workers, .. }) => Ok(workers.len()),
        Some(other) => Err(format!("unexpected response to ClusterStatus: {other:?}")),
        None => Err("master closed status connection".to_string()),
    }
}

fn build_deterministic_blob(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    let mut state: u64 = 0x5EED_5CA1_E100_0001;
    for chunk in data.chunks_mut(8) {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    data
}

fn dir_size_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if let Ok(meta) = entry.metadata() {
            if meta.is_file() {
                total = total.saturating_add(meta.len());
            } else if meta.is_dir() {
                total = total.saturating_add(dir_size_bytes(&p));
            }
        }
    }
    total
}

fn summarize_window_samples(samples: &[ProcessSample], start: Instant, end: Instant) -> (u64, f64) {
    let window: Vec<&ProcessSample> = samples
        .iter()
        .filter(|s| s.t >= start && s.t <= end)
        .collect();
    if window.is_empty() {
        return (0, 0.0);
    }
    let mut rss: Vec<u64> = window.iter().map(|s| s.rss_bytes).collect();
    rss.sort_unstable();
    let median_rss = rss[rss.len() / 2];
    let mean_cpu = window.iter().map(|s| f64::from(s.cpu_pct)).sum::<f64>() / window.len() as f64;
    (median_rss, mean_cpu)
}

async fn run_driver(cfg: ResolvedConfig) -> Result<ScaleReport, String> {
    std::fs::create_dir_all(&cfg.temp_root)
        .map_err(|e| format!("create temp_root {}: {e}", cfg.temp_root.display()))?;
    let run_dir = TempBuilder::new()
        .prefix("oxideswarm-scale1-")
        .tempdir_in(&cfg.temp_root)
        .map_err(|e| format!("tempdir_in {} failed: {e}", cfg.temp_root.display()))?;
    let root = run_dir.path();
    let master_home = root.join("master_home");
    let state_dir = root.join("state");
    let blob_dir = root.join("blobs");
    let fanout_sandboxes = root.join("fanout_sandboxes");
    let port_file = root.join("master.port");
    let dashboard_port_file = root.join("dashboard.port");
    let secret_path = root.join("cluster.secret");

    std::fs::create_dir_all(&master_home).map_err(|e| format!("mkdir master_home: {e}"))?;
    let secret = ClusterSecret::generate().map_err(|e| format!("generate secret: {e}"))?;
    secret
        .save_file(&secret_path, false)
        .map_err(|e| format!("save secret: {e}"))?;

    eprintln!(
        "[scale1] spawning master (workers={}, submitters={}, tasks={})...",
        cfg.workers, cfg.submitters, cfg.tasks
    );
    let mut master = ChildGuard::spawn_master(
        &cfg,
        &secret_path,
        &master_home,
        &state_dir,
        &blob_dir,
        &port_file,
        &dashboard_port_file,
    )?;
    let port = master
        .wait_for_port_file(&port_file, Duration::from_secs(30))
        .await?;
    let master_addr = format!("127.0.0.1:{port}");
    let master_pid = master.pid().ok_or("missing master pid")?;
    let (sampler_stop, proc_samples, sampler_thread) = spawn_process_sampler(master_pid);

    let mut status_client = connect_master(&master_addr, Some(&secret))
        .await
        .map_err(|e| format!("connect status_client: {e}"))?;

    // Phase 1: Register N simulated workers.
    let net_control = SimulatedNetworkControl::new();
    let shared_delay_nanos = Arc::new(AtomicU64::new(0));
    let started_times: Arc<Mutex<HashMap<TaskId, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let link_cfg = SimulatedLinkConfig::default()
        .with_base_latency(Duration::from_millis(cfg.base_latency_ms))
        .with_jitter(Duration::from_millis(cfg.jitter_ms))
        .with_loss_probability(cfg.loss_prob)
        .with_loss_action(LinkLossAction::RetransmitStall)
        .with_control(net_control.clone());

    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker_ids = Vec::with_capacity(cfg.workers);
    let mut heartbeat_trackers: Vec<Arc<HeartbeatTracker>> = Vec::with_capacity(cfg.workers);
    let mut worker_handles = Vec::with_capacity(cfg.workers);

    let reg_start = Instant::now();
    for i in 0..cfg.workers {
        let times_ref = Arc::clone(&started_times);
        let fake = FakeRunnerConfig::new(Duration::ZERO)
            .with_shared_delay_nanos(Arc::clone(&shared_delay_nanos))
            .with_on_task_started(move |task_id| {
                times_ref
                    .lock()
                    .expect("started_times mutex")
                    .insert(task_id, Instant::now());
            });
        let mut wcfg = WorkerConfig::simulated(
            master_addr.clone(),
            Some(format!("scale-w-{i:04}")),
            Some(4),
            fake,
            Some(link_cfg.clone()),
        )
        .with_cluster_secret(secret.clone())
        .with_max_concurrency(1)
        .with_session_resume_grace_cap(Some(Duration::ZERO));
        if i < cfg.blob_fanout_workers {
            wcfg = wcfg.with_sandbox_base_dir(fanout_sandboxes.join(format!("w-{i:03}")));
        }

        let mut client = WorkerClient::new(wcfg);
        client.set_backoff_config(
            BackoffConfig::default()
                .with_initial(Duration::from_millis(cfg.reconnect_initial_ms))
                .with_max(Duration::from_millis(
                    (cfg.reconnect_initial_ms * 8).max(500),
                )),
        );
        worker_ids.push(client.worker_id());
        heartbeat_trackers.push(Arc::clone(client.heartbeat_tracker()));
        let rx = worker_shutdown_rx.clone();
        worker_handles.push(tokio::spawn(async move { client.run(rx).await }));
    }

    let reg_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if net_control.active_sessions() == cfg.workers as u64 {
            let count = query_worker_count(&mut status_client).await?;
            if count == cfg.workers {
                break;
            }
        }
        if Instant::now() >= reg_deadline {
            return Err(format!(
                "timed out waiting for {} workers (active_sessions={})",
                cfg.workers,
                net_control.active_sessions()
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let reg_ms = reg_start.elapsed().as_secs_f64() * 1000.0;
    let total_handshakes_after_reg = net_control.total_handshakes();
    eprintln!(
        "[scale1] registered {} workers in {reg_ms:.1} ms",
        cfg.workers
    );

    // Phase 2: Sequential unloaded latency probes.
    let mut probe_transport = connect_master(&master_addr, Some(&secret))
        .await
        .map_err(|e| format!("connect probe_transport: {e}"))?;
    let mut unloaded_assign = Vec::with_capacity(cfg.unloaded_probes);
    let mut unloaded_complete = Vec::with_capacity(cfg.unloaded_probes);
    for _ in 0..cfg.unloaded_probes {
        let (c_ms, a_ms, ok) =
            submit_and_measure(&mut probe_transport, builtin_task(), &started_times).await?;
        if !ok {
            return Err("unloaded probe task failed".to_string());
        }
        unloaded_complete.push(c_ms);
        if let Some(a) = a_ms {
            unloaded_assign.push(a);
        }
    }
    drop(probe_transport);

    // Phase 3: Concurrent multi-submitter burst.
    shared_delay_nanos.store(
        Duration::from_millis(cfg.fake_exec_ms).as_nanos() as u64,
        Ordering::Release,
    );
    let mut submitter_conns = Vec::with_capacity(cfg.submitters);
    for _ in 0..cfg.submitters {
        submitter_conns.push(
            connect_master(&master_addr, Some(&secret))
                .await
                .map_err(|e| format!("connect burst submitter: {e}"))?,
        );
    }

    let base_count = cfg.tasks / cfg.submitters;
    let remainder = cfg.tasks % cfg.submitters;
    let burst_start = Instant::now();
    let mut burst_handles = Vec::with_capacity(cfg.submitters);
    for (idx, mut conn) in submitter_conns.into_iter().enumerate() {
        let n_tasks = base_count + usize::from(idx < remainder);
        let times_ref = Arc::clone(&started_times);
        burst_handles.push(tokio::spawn(async move {
            let mut assigns = Vec::with_capacity(n_tasks);
            let mut completes = Vec::with_capacity(n_tasks);
            let mut failed = 0usize;
            for _ in 0..n_tasks {
                let (c_ms, a_ms, ok) =
                    submit_and_measure(&mut conn, builtin_task(), &times_ref).await?;
                if !ok {
                    failed += 1;
                }
                completes.push(c_ms);
                if let Some(a) = a_ms {
                    assigns.push(a);
                }
            }
            Ok::<_, String>((assigns, completes, failed))
        }));
    }

    let mut burst_assign = Vec::with_capacity(cfg.tasks);
    let mut burst_complete = Vec::with_capacity(cfg.tasks);
    let mut burst_failed = 0usize;
    for h in burst_handles {
        let (a, c, f) = h.await.map_err(|e| format!("burst join error: {e}"))??;
        burst_assign.extend(a);
        burst_complete.extend(c);
        burst_failed += f;
    }
    let burst_wall_ms = burst_start.elapsed().as_secs_f64() * 1000.0;
    let tasks_per_sec = if burst_wall_ms > 0.0 {
        cfg.tasks as f64 / (burst_wall_ms / 1000.0)
    } else {
        0.0
    };
    eprintln!(
        "[scale1] burst {}/{} tasks ok in {burst_wall_ms:.1} ms ({tasks_per_sec:.1} tasks/s)",
        cfg.tasks - burst_failed,
        cfg.tasks
    );

    // Phase 4: Quiet steady-state heartbeat window.
    let hb_start = Instant::now();
    let acks_before: u64 = heartbeat_trackers
        .iter()
        .map(|t| t.total_acks.load(Ordering::Relaxed))
        .sum();
    let rx_before = net_control.rx_bytes();
    let tx_before = net_control.tx_bytes();
    tokio::time::sleep(Duration::from_secs(cfg.heartbeat_window_secs)).await;
    let hb_end = Instant::now();
    let hb_window_secs = hb_end.duration_since(hb_start).as_secs_f64();
    let acks_after: u64 = heartbeat_trackers
        .iter()
        .map(|t| t.total_acks.load(Ordering::Relaxed))
        .sum();
    let acks_delta = acks_after.saturating_sub(acks_before);
    let rx_delta = net_control.rx_bytes().saturating_sub(rx_before);
    let tx_delta = net_control.tx_bytes().saturating_sub(tx_before);
    let mut hb_latencies: Vec<f64> = heartbeat_trackers
        .iter()
        .map(|t| t.last_latency_ms.load(Ordering::Relaxed) as f64)
        .collect();

    // Phase 5: Simultaneous blob fan-out.
    shared_delay_nanos.store(0, Ordering::Release);
    let blob_data = build_deterministic_blob(cfg.blob_size_bytes);
    let blob_id = BlobId::of(&blob_data);
    let blob_conn = BlobConnection::connect(&master_addr, Some(&secret))
        .await
        .map_err(|e| format!("BlobConnection::connect failed: {e}"))?;
    upload_blob(
        &blob_conn,
        blob_id,
        blob_data.len() as u64,
        std::io::Cursor::new(&blob_data),
    )
    .await
    .map_err(|e| format!("upload_blob failed: {e}"))?;
    blob_conn.close().await;
    drop(blob_data);

    net_control.reset_counters();
    let fanout_n = cfg.blob_fanout_workers;
    let mut fanout_conns = Vec::with_capacity(fanout_n);
    for _ in 0..fanout_n {
        fanout_conns.push(
            connect_master(&master_addr, Some(&secret))
                .await
                .map_err(|e| format!("connect fanout submitter: {e}"))?,
        );
    }

    let fanout_start = Instant::now();
    let mut fanout_handles = Vec::with_capacity(fanout_n);
    for (i, mut conn) in fanout_conns.into_iter().enumerate() {
        let wid = worker_ids[i];
        let bsize = cfg.blob_size_bytes as u64;
        fanout_handles.push(tokio::spawn(async move {
            let mut task = Task::new(
                TaskSpec::command("scale1-blob", vec![]),
                TaskRequirements::generic(1, 60),
            );
            task.tags = vec![format!("require:worker={wid}")];
            task.inputs = vec![TaskInput::file("payload.bin", blob_id, bsize, false)];
            conn.send_msg(&ClientMessage::SubmitTask { task, wait: true })
                .await
                .map_err(|e| format!("fanout submit {i} failed: {e}"))?;
            match conn
                .recv_msg::<ClientResponse>()
                .await
                .map_err(|e| format!("fanout recv {i} failed: {e}"))?
            {
                Some(ClientResponse::TaskCompleted { result, .. }) => {
                    if result.exit_code != EXIT_CODE_SUCCESS {
                        return Err(format!(
                            "fanout task {i} failed (exit={}): {:?}",
                            result.exit_code, result.error
                        ));
                    }
                    Ok(())
                }
                other => Err(format!("fanout task {i} unexpected response: {other:?}")),
            }
        }));
    }
    for h in fanout_handles {
        h.await.map_err(|e| format!("fanout join error: {e}"))??;
    }
    let fanout_wall_ms = fanout_start.elapsed().as_secs_f64() * 1000.0;
    let fanout_rx_bytes = net_control.rx_bytes();
    let fanout_tx_bytes = net_control.tx_bytes();
    let fanout_total_mb = (fanout_n as f64 * cfg.blob_size_bytes as f64) / (1024.0 * 1024.0);
    let fanout_mb_per_sec = if fanout_wall_ms > 0.0 {
        fanout_total_mb / (fanout_wall_ms / 1000.0)
    } else {
        0.0
    };
    let _ = std::fs::remove_dir_all(&fanout_sandboxes);
    eprintln!(
        "[scale1] blob fan-out {fanout_n}x{} bytes in {fanout_wall_ms:.1} ms ({fanout_mb_per_sec:.2} MiB/s)",
        cfg.blob_size_bytes
    );

    // Phase 6: Mass disconnect & reconnect storm.
    let hs_before = net_control.total_handshakes();
    let recon_start = Instant::now();
    net_control.disconnect_all();

    let recon_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if net_control.total_handshakes() >= hs_before + cfg.workers as u64
            && net_control.active_sessions() == cfg.workers as u64
            && query_worker_count(&mut status_client).await? == cfg.workers
        {
            break;
        }
        if Instant::now() >= recon_deadline {
            return Err(format!(
                "timed out waiting for reconnect convergence (handshakes={}, before={}, active={})",
                net_control.total_handshakes(),
                hs_before,
                net_control.active_sessions()
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let recon_ms = recon_start.elapsed().as_secs_f64() * 1000.0;
    let hs_after = net_control.total_handshakes();

    let post_tasks = cfg.workers.min(64);
    let mut post_conn = connect_master(&master_addr, Some(&secret))
        .await
        .map_err(|e| format!("connect post_conn: {e}"))?;
    let mut post_failed = 0usize;
    for _ in 0..post_tasks {
        let (_, _, ok) = submit_and_measure(&mut post_conn, builtin_task(), &started_times).await?;
        if !ok {
            post_failed += 1;
        }
    }
    drop(post_conn);
    drop(status_client);

    // Give the TaskStore group-commit loop a brief beat to flush the final post-reconnect records.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = worker_shutdown_tx.send(true);
    for h in worker_handles {
        let _ = h.await;
    }

    // Stop Master #1 and harvest process RSS/CPU statistics.
    sampler_stop.store(true, Ordering::Release);
    master.stop();
    let _ = sampler_thread.join();

    let samples_snapshot = proc_samples.lock().expect("samples mutex").clone();
    let peak_rss = samples_snapshot
        .iter()
        .map(|s| s.rss_bytes)
        .max()
        .unwrap_or(0);
    let peak_cpu = samples_snapshot
        .iter()
        .map(|s| f64::from(s.cpu_pct))
        .fold(0.0f64, f64::max);
    let (steady_rss, steady_cpu) = summarize_window_samples(&samples_snapshot, hb_start, hb_end);

    // Phase 7: Persistence size, in-process TaskQueue replay, and restarted master readiness.
    let journal_bytes = std::fs::metadata(state_dir.join("journal.bin"))
        .map(|m| m.len())
        .unwrap_or(0);
    let snapshot_bytes = std::fs::metadata(state_dir.join("snapshot.bin"))
        .map(|m| m.len())
        .unwrap_or(0);
    let state_dir_bytes = dir_size_bytes(&state_dir);

    let replay_start = Instant::now();
    let (recovered_snapshot_tasks, recovered_journal_records) = {
        let (store, recovered) =
            TaskStore::open(&state_dir).map_err(|e| format!("TaskStore::open failed: {e}"))?;
        let snap_tasks = recovered.snapshot.as_ref().map_or(0, |s| s.tasks.len());
        let j_records = recovered.records.len();
        let queue = TaskQueue::new();
        queue
            .attach_store_and_replay(store, recovered, Duration::ZERO)
            .await
            .map_err(|e| format!("attach_store_and_replay failed: {e}"))?;
        (snap_tasks, j_records)
    };
    let in_process_replay_ms = replay_start.elapsed().as_secs_f64() * 1000.0;

    let _ = std::fs::remove_file(&port_file);
    let _ = std::fs::remove_file(&dashboard_port_file);
    let restart_start = Instant::now();
    let mut restarted_master = ChildGuard::spawn_master(
        &cfg,
        &secret_path,
        &master_home,
        &state_dir,
        &blob_dir,
        &port_file,
        &dashboard_port_file,
    )?;
    let restart_port = restarted_master
        .wait_for_port_file(&port_file, Duration::from_secs(30))
        .await?;
    let restart_addr = format!("127.0.0.1:{restart_port}");
    let mut restart_client = connect_master(&restart_addr, Some(&secret))
        .await
        .map_err(|e| format!("connect restarted master: {e}"))?;
    let _ = query_worker_count(&mut restart_client).await?;
    let master_restart_ready_ms = restart_start.elapsed().as_secs_f64() * 1000.0;
    drop(restart_client);
    restarted_master.stop();

    Ok(ScaleReport {
        config: ConfigReport {
            workers: cfg.workers,
            submitters: cfg.submitters,
            tasks: cfg.tasks,
            unloaded_probes: cfg.unloaded_probes,
            fake_exec_ms: cfg.fake_exec_ms,
            heartbeat_secs: cfg.heartbeat_secs,
            heartbeat_window_secs: cfg.heartbeat_window_secs,
            blob_fanout_workers: fanout_n,
            blob_size_bytes: cfg.blob_size_bytes,
            base_latency_ms: cfg.base_latency_ms,
            jitter_ms: cfg.jitter_ms,
            loss_prob: cfg.loss_prob,
            reconnect_initial_ms: cfg.reconnect_initial_ms,
            quick: cfg.quick,
        },
        registration: RegistrationReport {
            convergence_ms: reg_ms,
            workers_registered: cfg.workers,
            total_handshakes: total_handshakes_after_reg,
        },
        unloaded: UnloadedReport {
            probes: cfg.unloaded_probes,
            submit_to_assign_ms: compute_stats(&mut unloaded_assign),
            submit_to_complete_ms: compute_stats(&mut unloaded_complete),
        },
        burst: BurstReport {
            tasks: cfg.tasks,
            failed: burst_failed,
            wall_ms: burst_wall_ms,
            tasks_per_sec,
            submit_to_assign_ms: compute_stats(&mut burst_assign),
            submit_to_complete_ms: compute_stats(&mut burst_complete),
        },
        heartbeat_quiet: HeartbeatReport {
            window_secs: hb_window_secs,
            acks_delta,
            acks_per_sec: if hb_window_secs > 0.0 {
                acks_delta as f64 / hb_window_secs
            } else {
                0.0
            },
            rx_bytes_delta: rx_delta,
            tx_bytes_delta: tx_delta,
            rx_bytes_per_sec: if hb_window_secs > 0.0 {
                rx_delta as f64 / hb_window_secs
            } else {
                0.0
            },
            tx_bytes_per_sec: if hb_window_secs > 0.0 {
                tx_delta as f64 / hb_window_secs
            } else {
                0.0
            },
            worker_last_latency_ms: compute_stats(&mut hb_latencies),
            steady_rss_bytes: steady_rss,
            steady_cpu_pct: steady_cpu,
        },
        blob_fanout: BlobFanoutReport {
            workers: fanout_n,
            blob_size_bytes: cfg.blob_size_bytes,
            wall_ms: fanout_wall_ms,
            rx_bytes: fanout_rx_bytes,
            tx_bytes: fanout_tx_bytes,
            throughput_mb_per_sec: fanout_mb_per_sec,
        },
        reconnect: ReconnectReport {
            handshakes_before: hs_before,
            handshakes_after: hs_after,
            convergence_ms: recon_ms,
            post_tasks,
            post_failed,
            loss_events: net_control.loss_events(),
        },
        master_process: ProcessReport {
            peak_rss_bytes: peak_rss,
            steady_rss_bytes: steady_rss,
            peak_cpu_pct: peak_cpu,
            steady_cpu_pct: steady_cpu,
            samples: samples_snapshot.len(),
        },
        persistence: PersistenceReport {
            journal_bytes,
            snapshot_bytes,
            state_dir_bytes,
            recovered_snapshot_tasks,
            recovered_journal_records,
            in_process_replay_ms,
            master_restart_ready_ms,
        },
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = match Args::parse().resolve() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    match run_driver(cfg).await {
        Ok(report) => {
            let json = serde_json::to_string_pretty(&report).expect("serialize ScaleReport");
            println!("{json}");
        }
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(1);
        }
    }
}
