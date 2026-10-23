//! `oxideswarm doctor`: checks a running cluster end to end.
//!
//! Connects like every client command ([`connect_master`]), lists the workers, then runs probes
//! pinned to each worker with a `require:worker=<id>` constraint (`rusty_grid_core::placement`):
//! a built-in task in the worker process, a process start-up, a GEMM on every GPU worker, and a
//! burst of unpinned tasks to see the work spread. These are checks C1 to C6 of the design record
//! `docs/design/0001-two-node-readiness.md`.
//!
//! Probes are submitted without `wait` and collected with `GetTaskResult` on one connection, so a
//! slow worker does not hold up the others. Nothing runs on a worker but built-in tasks, `echo`
//! and the GEMM; nothing is written outside the per-task sandbox.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::net::TcpStream;
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::GridError;
use rusty_grid_core::placement::{
    listed_worker_id, tag_value, Placement, ARCH_TAG_PREFIX, LINK_TAG_PREFIX, OS_TAG_PREFIX,
    RESERVED_TAG_PREFIXES, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_core::WorkerCapabilities;

use crate::connect::{connect_master, describe_auth_error};

/// What the process probe prints; its output must contain it.
pub const PROBE_TOKEN: &str = "oxideswarm-doctor-probe";
/// Largest GEMM the simulated GPU accepts (`SIMULATOR_MAX_DIM` of the worker crate).
pub const SIMULATED_GPU_MAX_DIM: u32 = 2048;
/// Length of one task of the spread check.
pub const SPREAD_TASK_MS: u64 = 500;
/// Tasks of the spread check per connected worker, unless `spread_tasks` says otherwise.
pub const SPREAD_TASKS_PER_WORKER: usize = 4;
/// How often outstanding probes are polled.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// What a GPU result says when the product passed the CPU check.
const GPU_VERIFIED: &str = "Status: VERIFIED_OK";

/// Options of one run.
#[derive(Debug, Clone)]
pub struct DoctorOptions {
    /// C2 fails with fewer connected workers.
    pub expect_workers: usize,
    /// How long one probe (or the whole spread check) may take.
    pub probe_timeout: Duration,
    /// Matrix edge of the GPU probe (capped at [`SIMULATED_GPU_MAX_DIM`] on a simulated GPU).
    pub gpu_dim: u32,
    /// Tasks of the spread check; `None`: [`SPREAD_TASKS_PER_WORKER`] per connected worker.
    pub spread_tasks: Option<usize>,
    pub skip_gpu: bool,
    pub skip_spread: bool,
}

impl Default for DoctorOptions {
    fn default() -> Self {
        Self {
            expect_workers: 1,
            probe_timeout: Duration::from_secs(60),
            gpu_dim: 512,
            spread_tasks: None,
            skip_gpu: false,
            skip_spread: false,
        }
    }
}

/// Result of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Pass,
    Warn,
    Fail,
    Skip,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Outcome::Pass => "PASS",
            Outcome::Warn => "WARN",
            Outcome::Fail => "FAIL",
            Outcome::Skip => "SKIP",
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    /// `C1` to `C6`.
    pub id: &'static str,
    pub name: &'static str,
    /// The worker a per-worker check ran on.
    pub worker: Option<String>,
    pub outcome: Outcome,
    pub detail: String,
}

/// A worker as the report lists it.
#[derive(Debug, Clone, Serialize)]
pub struct WorkerSummary {
    pub name: String,
    /// From the master's `id:` tag; `None` with a master older than placement support.
    pub id: Option<Uuid>,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub cores: usize,
    pub ram_mb: u64,
    /// `none`, the GPU's name, or `<name> (simulated)`.
    pub gpu: String,
    pub link: Option<String>,
    pub state: Option<String>,
    /// The worker's user tags (everything without a reserved prefix).
    pub tags: Vec<String>,
}

impl WorkerSummary {
    fn of(caps: &WorkerCapabilities) -> Self {
        let value = |prefix: &str| tag_value(&caps.tags, prefix).map(str::to_string);
        let gpu = match (caps.has_gpu, caps.is_simulated_gpu) {
            (_, true) => format!(
                "{} (simulated)",
                caps.gpu_device_name.as_deref().unwrap_or("GPU")
            ),
            (true, false) => caps
                .gpu_device_name
                .clone()
                .unwrap_or_else(|| "GPU".to_string()),
            (false, false) => "none".to_string(),
        };
        Self {
            name: caps.name.clone(),
            id: listed_worker_id(caps),
            os: value(OS_TAG_PREFIX),
            arch: value(ARCH_TAG_PREFIX),
            cores: caps.cpu_cores,
            ram_mb: caps.ram_mb,
            gpu,
            link: value(LINK_TAG_PREFIX),
            state: value(STATE_TAG_PREFIX),
            tags: caps
                .tags
                .iter()
                .filter(|t| !RESERVED_TAG_PREFIXES.iter().any(|p| t.starts_with(p)))
                .cloned()
                .collect(),
        }
    }

    fn connected(&self) -> bool {
        self.state.as_deref() != Some("disconnected")
    }
}

/// Everything one run found.
#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub master: String,
    pub workers: Vec<WorkerSummary>,
    pub checks: Vec<Check>,
    /// The master refused the cluster secret, or wants one (exit code 6).
    #[serde(skip)]
    pub auth_failed: bool,
}

impl DoctorReport {
    /// True when at least one check failed.
    pub fn failed(&self) -> bool {
        self.checks.iter().any(|c| c.outcome == Outcome::Fail)
    }

    fn count(&self, outcome: Outcome) -> usize {
        self.checks.iter().filter(|c| c.outcome == outcome).count()
    }

    fn push(
        &mut self,
        id: &'static str,
        name: &'static str,
        worker: Option<&str>,
        outcome: Outcome,
        detail: impl Into<String>,
    ) {
        self.checks.push(Check {
            id,
            name,
            worker: worker.map(str::to_string),
            outcome,
            detail: detail.into(),
        });
    }

    /// The report as the terminal shows it.
    pub fn render(&self) -> String {
        let mut out = format!("OxideSwarm doctor: master {}\n", self.master);
        if !self.workers.is_empty() {
            let connected = self.workers.iter().filter(|w| w.connected()).count();
            out.push_str(&format!(
                "\nWorkers ({connected} connected, {} registered)\n",
                self.workers.len()
            ));
            out.push_str(&format!(
                "  {:<20} {:<9} {:<16} {:>5} {:>9}  {:<28} {:<10} {}\n",
                "NAME", "ID", "OS/ARCH", "CORES", "RAM MB", "GPU", "STATE", "TAGS"
            ));
            for w in &self.workers {
                let os_arch = format!(
                    "{}/{}",
                    w.os.as_deref().unwrap_or("?"),
                    w.arch.as_deref().unwrap_or("?")
                );
                out.push_str(&format!(
                    "  {:<20} {:<9} {:<16} {:>5} {:>9}  {:<28} {:<10} {}\n",
                    w.name,
                    w.id.map(|id| short_id(&id)).unwrap_or_else(|| "?".into()),
                    os_arch,
                    w.cores,
                    w.ram_mb,
                    w.gpu,
                    w.state.as_deref().unwrap_or("?"),
                    w.tags.join(","),
                ));
            }
        }
        out.push_str("\nChecks\n");
        for c in &self.checks {
            out.push_str(&format!(
                "  {}  {} {:<18} {:<20} {}\n",
                c.outcome.label(),
                c.id,
                c.name,
                c.worker.as_deref().unwrap_or(""),
                c.detail
            ));
        }
        out.push_str(&format!(
            "\nSummary: {} passed, {} warnings, {} failed, {} skipped\n",
            self.count(Outcome::Pass),
            self.count(Outcome::Warn),
            self.count(Outcome::Fail),
            self.count(Outcome::Skip),
        ));
        out
    }
}

/// The first eight characters of a worker id, as the report shows it.
fn short_id(id: &Uuid) -> String {
    id.to_string()[..8].to_string()
}

/// What happened to one submitted task.
#[derive(Debug, Clone)]
enum Collected {
    Done(Box<TaskResult>),
    /// The master refused the task or does not know it (its message).
    Refused(String),
    /// Still not finished at the deadline (its last known state).
    TimedOut(String),
}

/// What a probe checks once its task finished.
#[derive(Debug, Clone, Copy)]
enum ProbeKind {
    InProcess,
    Process,
    Gpu { dim: u32, simulated: bool },
}

impl ProbeKind {
    fn check(self) -> (&'static str, &'static str) {
        match self {
            ProbeKind::InProcess => ("C3", "In-process probe"),
            ProbeKind::Process => ("C4", "Process probe"),
            ProbeKind::Gpu { .. } => ("C5", "GPU probe"),
        }
    }
}

/// A probe pinned to one worker.
struct Probe {
    kind: ProbeKind,
    worker_name: String,
    worker_id: Uuid,
    task: Result<TaskId, String>,
}

/// One request, one reply.
async fn request(
    transport: &mut MessageTransport<TcpStream>,
    message: &ClientMessage,
) -> Result<ClientResponse, GridError> {
    transport.send_msg(message).await.map_err(GridError::from)?;
    match transport.recv_msg::<ClientResponse>().await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(GridError::ConnectionClosed),
        Err(e) => Err(GridError::from(e)),
    }
}

/// Submits `task` without waiting for it.
async fn submit(
    transport: &mut MessageTransport<TcpStream>,
    task: Task,
) -> Result<Result<TaskId, String>, GridError> {
    Ok(
        match request(transport, &ClientMessage::SubmitTask { task, wait: false }).await? {
            ClientResponse::TaskSubmitted { task_id } => Ok(task_id),
            ClientResponse::Error { message } => Err(message),
            other => Err(format!("unexpected reply {other:?}")),
        },
    )
}

/// Polls `GetTaskResult` for every task until each one finished or `timeout` passed; tasks still
/// running then are cancelled.
async fn collect(
    transport: &mut MessageTransport<TcpStream>,
    tasks: &[TaskId],
    timeout: Duration,
) -> Result<HashMap<TaskId, Collected>, GridError> {
    let deadline = Instant::now() + timeout;
    let mut done: HashMap<TaskId, Collected> = HashMap::new();
    let mut last_state: HashMap<TaskId, String> = HashMap::new();
    loop {
        for &task_id in tasks {
            if done.contains_key(&task_id) {
                continue;
            }
            match request(transport, &ClientMessage::GetTaskResult { task_id }).await? {
                ClientResponse::TaskCompleted { result, .. } => {
                    done.insert(task_id, Collected::Done(Box::new(result)));
                }
                ClientResponse::TaskStatusInfo {
                    state_name, error, ..
                } => {
                    let state = match error {
                        Some(e) => format!("{state_name}: {e}"),
                        None => state_name,
                    };
                    last_state.insert(task_id, state);
                }
                ClientResponse::Error { message } => {
                    done.insert(task_id, Collected::Refused(message));
                }
                other => {
                    done.insert(
                        task_id,
                        Collected::Refused(format!("unexpected reply {other:?}")),
                    );
                }
            }
        }
        if done.len() == tasks.len() || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    for &task_id in tasks {
        if let std::collections::hash_map::Entry::Vacant(slot) = done.entry(task_id) {
            // Best effort: a probe that outlived the run must not keep a slot.
            let _ = request(transport, &ClientMessage::CancelTask { task_id }).await;
            let state = last_state
                .remove(&task_id)
                .unwrap_or_else(|| "unknown".to_string());
            slot.insert(Collected::TimedOut(state));
        }
    }
    Ok(done)
}

/// The first line of a failed task's error, standard error or output.
fn failure_text(result: &TaskResult) -> String {
    let text = result
        .error
        .clone()
        .filter(|e| !e.trim().is_empty())
        .unwrap_or_else(|| {
            let stderr = result.stderr_str();
            if stderr.trim().is_empty() {
                result.stdout_str().into_owned()
            } else {
                stderr.into_owned()
            }
        });
    text.lines().next().unwrap_or("").trim().to_string()
}

/// The verdict of a probe from what its task returned.
fn judge(probe: &Probe, collected: Option<&Collected>, timeout: Duration) -> (Outcome, String) {
    let result = match collected {
        Some(Collected::Done(result)) => result,
        Some(Collected::Refused(message)) => {
            return (
                Outcome::Fail,
                format!("the master refused the probe: {message}"),
            )
        }
        Some(Collected::TimedOut(state)) => {
            return (
                Outcome::Fail,
                format!(
                    "no result within {}s (last state {state}); the worker may be busy or above its CPU limit",
                    timeout.as_secs()
                ),
            )
        }
        None => return (Outcome::Fail, "the probe was not collected".to_string()),
    };
    if result.worker_id != probe.worker_id {
        return (
            Outcome::Fail,
            format!(
                "ran on worker {} instead: the master ignores placement constraints (upgrade the master)",
                short_id(&result.worker_id)
            ),
        );
    }
    if result.exit_code != 0 {
        return (
            Outcome::Fail,
            format!("exit code {}: {}", result.exit_code, failure_text(result)),
        );
    }
    let ms = result.execution_time_ms;
    match probe.kind {
        ProbeKind::InProcess => (Outcome::Pass, format!("completed in {ms} ms")),
        ProbeKind::Process => {
            if result.stdout_str().contains(PROBE_TOKEN) {
                (
                    Outcome::Pass,
                    format!("started a process and read its output in {ms} ms"),
                )
            } else {
                (
                    Outcome::Fail,
                    format!(
                        "the process printed {:?} instead of the probe token",
                        result.stdout_str().trim()
                    ),
                )
            }
        }
        ProbeKind::Gpu { dim, simulated } => {
            if !result.stdout_str().contains(GPU_VERIFIED) {
                return (
                    Outcome::Fail,
                    format!(
                        "the product did not pass the CPU check: {}",
                        failure_text(result)
                    ),
                );
            }
            let gflops = 2.0 * f64::from(dim).powi(3) / (ms.max(1) as f64 * 1e6);
            let device = result.device_name.as_deref().unwrap_or("unknown device");
            (
                Outcome::Pass,
                format!(
                    "{dim}x{dim} GEMM verified in {ms} ms, {gflops:.1} GFLOP/s on {device}{}",
                    if simulated {
                        " (simulated: runs on the CPU)"
                    } else {
                        ""
                    }
                ),
            )
        }
    }
}

/// The task a probe submits, pinned to `worker_id`.
fn probe_task(
    kind: ProbeKind,
    worker_id: Uuid,
    os: Option<&str>,
    timeout: Duration,
) -> Option<Task> {
    let timeout_secs = timeout.as_secs().max(1);
    let (spec, requirements) = match kind {
        ProbeKind::InProcess => (
            TaskSpec::builtin_test("hash_compute", 10_000),
            TaskRequirements::generic(1, timeout_secs),
        ),
        ProbeKind::Process => {
            let spec = match os? {
                "windows" => {
                    TaskSpec::command("cmd", vec!["/C".into(), "echo".into(), PROBE_TOKEN.into()])
                }
                _ => TaskSpec::command("sh", vec!["-c".into(), format!("echo {PROBE_TOKEN}")]),
            };
            (spec, TaskRequirements::generic(1, timeout_secs))
        }
        ProbeKind::Gpu { dim, .. } => (
            TaskSpec::GpuCompute {
                kernel_name: "matrix_multiply".into(),
                input_data: vec![1, 2, 3, 4],
                work_group_size: 16,
                simulated_matrix_dim: dim,
                compute_intensity: 10,
            },
            TaskRequirements::gpu(timeout_secs),
        ),
    };
    let pin = Placement {
        worker: Some(worker_id.to_string()),
        ..Placement::default()
    };
    Some(Task::new(spec, requirements.with_max_retries(0)).with_placement(&pin))
}

/// Runs every check against the master at `master_addr`.
pub async fn run_doctor(
    master_addr: &str,
    secret: Option<&ClusterSecret>,
    options: &DoctorOptions,
) -> DoctorReport {
    let mut report = DoctorReport {
        master: master_addr.to_string(),
        workers: Vec::new(),
        checks: Vec::new(),
        auth_failed: false,
    };
    if let Err(e) = run_checks(master_addr, secret, options, &mut report).await {
        let detail = describe_auth_error(master_addr, &e).unwrap_or_else(|| e.to_string());
        if matches!(e, GridError::AuthFailed(_) | GridError::AuthRequired(_)) {
            report.auth_failed = true;
        }
        let id = if report.checks.is_empty() { "C1" } else { "--" };
        report.push(id, "Connection", None, Outcome::Fail, detail);
    }
    report
}

/// The checks; an error is a broken connection, which ends the run.
async fn run_checks(
    master_addr: &str,
    secret: Option<&ClusterSecret>,
    options: &DoctorOptions,
    report: &mut DoctorReport,
) -> Result<(), GridError> {
    // C1: connection, authentication and a first answer (a master that wants a secret this client
    // lacks only says so in its first reply).
    let started = Instant::now();
    let mut transport = connect_master(master_addr, secret).await?;
    let listed = match request(&mut transport, &ClientMessage::ListWorkers).await? {
        ClientResponse::WorkerList { workers } => workers,
        other => {
            return Err(GridError::UnexpectedMessage {
                expected: "WorkerList".to_string(),
                actual: format!("{other:?}"),
            })
        }
    };
    report.push(
        "C1",
        "Connect",
        None,
        Outcome::Pass,
        format!(
            "{} in {} ms",
            if secret.is_some() {
                "connected and authenticated"
            } else {
                "connected without a cluster secret"
            },
            started.elapsed().as_millis()
        ),
    );

    // C2: who is there.
    report.workers = listed.iter().map(WorkerSummary::of).collect();
    let connected: Vec<WorkerSummary> = report
        .workers
        .iter()
        .filter(|w| w.connected())
        .cloned()
        .collect();
    let mut names: Vec<String> = connected.iter().map(|w| w.name.to_lowercase()).collect();
    names.sort();
    let mut shared: Vec<String> = names
        .windows(2)
        .filter(|pair| pair[0] == pair[1])
        .map(|pair| pair[0].clone())
        .collect();
    shared.dedup();
    let (outcome, detail) = if connected.len() < options.expect_workers {
        (
            Outcome::Fail,
            format!(
                "{} connected, at least {} expected",
                connected.len(),
                options.expect_workers
            ),
        )
    } else if !shared.is_empty() {
        (
            Outcome::Warn,
            format!(
                "{} connected; several workers share the name {} (give each its own --name)",
                connected.len(),
                shared.join(", ")
            ),
        )
    } else {
        (
            Outcome::Pass,
            format!(
                "{} connected (at least {} expected)",
                connected.len(),
                options.expect_workers
            ),
        )
    };
    report.push("C2", "Workers", None, outcome, detail);

    // C3 to C5: probes pinned to each connected worker.
    let mut probes: Vec<Probe> = Vec::new();
    for w in &connected {
        let Some(worker_id) = w.id else {
            report.push(
                "C3",
                "In-process probe",
                Some(w.name.as_str()),
                Outcome::Fail,
                "the master lists workers without ids, so probes cannot be pinned (upgrade the master)",
            );
            continue;
        };
        let caps = listed
            .iter()
            .find(|c| listed_worker_id(c) == Some(worker_id))
            .expect("summaries come from the listed workers");
        let mut kinds = vec![ProbeKind::InProcess, ProbeKind::Process];
        if caps.can_execute_gpu() && !options.skip_gpu {
            let simulated = caps.is_simulated_gpu;
            let dim = if simulated {
                options.gpu_dim.min(SIMULATED_GPU_MAX_DIM)
            } else {
                options.gpu_dim
            };
            kinds.push(ProbeKind::Gpu { dim, simulated });
        }
        for kind in kinds {
            let Some(task) = probe_task(kind, worker_id, w.os.as_deref(), options.probe_timeout)
            else {
                let (id, name) = kind.check();
                report.push(
                    id,
                    name,
                    Some(w.name.as_str()),
                    Outcome::Skip,
                    "the worker reports no os: tag (older worker), so the command is unknown",
                );
                continue;
            };
            let task = submit(&mut transport, task).await?;
            probes.push(Probe {
                kind,
                worker_name: w.name.clone(),
                worker_id,
                task,
            });
        }
    }
    let submitted: Vec<TaskId> = probes
        .iter()
        .filter_map(|p| p.task.as_ref().ok().copied())
        .collect();
    let collected = collect(&mut transport, &submitted, options.probe_timeout).await?;
    for probe in &probes {
        let (id, name) = probe.kind.check();
        let (outcome, detail) = match &probe.task {
            Ok(task_id) => judge(probe, collected.get(task_id), options.probe_timeout),
            Err(message) => (
                Outcome::Fail,
                format!("the master refused the probe: {message}"),
            ),
        };
        report.push(id, name, Some(probe.worker_name.as_str()), outcome, detail);
    }
    if options.skip_gpu {
        report.push("C5", "GPU probe", None, Outcome::Skip, "--skip-gpu");
    } else if !connected.is_empty() && !listed.iter().any(|c| c.can_execute_gpu()) {
        report.push(
            "C5",
            "GPU probe",
            None,
            Outcome::Skip,
            "no worker advertises a GPU",
        );
    }

    // C6: unpinned tasks spread over the workers.
    if options.skip_spread {
        report.push("C6", "Spread", None, Outcome::Skip, "--skip-spread");
        return Ok(());
    }
    if connected.is_empty() {
        report.push("C6", "Spread", None, Outcome::Skip, "no connected worker");
        return Ok(());
    }
    let count = options
        .spread_tasks
        .unwrap_or(SPREAD_TASKS_PER_WORKER * connected.len())
        .max(1);
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(count);
    for _ in 0..count {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: "sleep".into(),
                iterations: 0,
                duration_ms: SPREAD_TASK_MS,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, options.probe_timeout.as_secs().max(1))
                .with_max_retries(0),
        );
        match submit(&mut transport, task).await? {
            Ok(task_id) => tasks.push(task_id),
            Err(message) => {
                report.push(
                    "C6",
                    "Spread",
                    None,
                    Outcome::Fail,
                    format!("the master refused a task: {message}"),
                );
                return Ok(());
            }
        }
    }
    let collected = collect(&mut transport, &tasks, options.probe_timeout).await?;
    let elapsed_ms = started.elapsed().as_millis();
    let mut per_worker: HashMap<Uuid, usize> = HashMap::new();
    let mut unfinished = 0usize;
    for task_id in &tasks {
        match collected.get(task_id) {
            Some(Collected::Done(result)) if result.exit_code == 0 => {
                *per_worker.entry(result.worker_id).or_default() += 1;
            }
            _ => unfinished += 1,
        }
    }
    let name_of = |id: &Uuid| {
        connected
            .iter()
            .find(|w| w.id == Some(*id))
            .map(|w| w.name.clone())
            .unwrap_or_else(|| short_id(id))
    };
    let mut spread: Vec<(String, usize)> =
        per_worker.iter().map(|(id, n)| (name_of(id), *n)).collect();
    spread.sort();
    let spread_text = spread
        .iter()
        .map(|(name, n)| format!("{name} {n}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (outcome, detail) = if unfinished > 0 {
        (
            Outcome::Fail,
            format!(
                "{unfinished} of {count} tasks did not complete within {}s",
                options.probe_timeout.as_secs()
            ),
        )
    } else if connected.len() >= 2 && per_worker.len() < 2 {
        (
            Outcome::Warn,
            format!(
                "all {count} tasks ran on {spread_text}; the other workers took none (busy, above their CPU limit, or kept free for GPU work)"
            ),
        )
    } else {
        (
            Outcome::Pass,
            format!(
                "{count} tasks of {SPREAD_TASK_MS} ms finished in {elapsed_ms} ms on {} worker(s): {spread_text}",
                per_worker.len()
            ),
        )
    };
    report.push("C6", "Spread", None, outcome, detail);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(name: &str, tags: &[&str]) -> WorkerCapabilities {
        WorkerCapabilities::new(name, 8, 16_384, false, false, None)
            .with_tags(tags.iter().map(|t| t.to_string()).collect())
    }

    #[test]
    fn test_worker_summary_reads_the_master_and_worker_tags() {
        let id = Uuid::new_v4();
        let summary = WorkerSummary::of(&caps(
            "MAC",
            &[
                "os:macos",
                "arch:aarch64",
                "laptop",
                "link:TCP/LAN",
                &format!("id:{id}"),
                "state:busy",
            ],
        ));
        assert_eq!(summary.id, Some(id));
        assert_eq!(summary.os.as_deref(), Some("macos"));
        assert_eq!(summary.arch.as_deref(), Some("aarch64"));
        assert_eq!(summary.link.as_deref(), Some("TCP/LAN"));
        assert_eq!(summary.state.as_deref(), Some("busy"));
        assert_eq!(summary.tags, ["laptop"], "only user tags");
        assert_eq!(summary.gpu, "none");
        assert!(summary.connected());
        assert!(!WorkerSummary::of(&caps("x", &["state:disconnected"])).connected());
    }

    #[test]
    fn test_probe_tasks_are_pinned_and_valid() {
        let id = Uuid::new_v4();
        let timeout = Duration::from_secs(30);
        for (kind, os) in [
            (ProbeKind::InProcess, None),
            (ProbeKind::Process, Some("windows")),
            (ProbeKind::Process, Some("macos")),
            (
                ProbeKind::Gpu {
                    dim: 256,
                    simulated: false,
                },
                Some("linux"),
            ),
        ] {
            let task = probe_task(kind, id, os, timeout).expect("a probe task");
            task.validate()
                .expect("probe tasks pass the master's validation");
            assert_eq!(
                task.placement().unwrap().worker.as_deref(),
                Some(id.to_string().as_str())
            );
            assert_eq!(
                task.requirements.max_retries,
                Some(0),
                "no retry hides a failure"
            );
        }
        assert!(
            probe_task(ProbeKind::Process, id, None, timeout).is_none(),
            "no os tag: no command"
        );
        match probe_task(ProbeKind::Process, id, Some("windows"), timeout)
            .unwrap()
            .spec
        {
            TaskSpec::Command { program, args, .. } => {
                assert_eq!(program, "cmd");
                assert_eq!(args, ["/C", "echo", PROBE_TOKEN]);
            }
            other => panic!("{other:?}"),
        }
    }

    fn done(worker_id: Uuid, exit_code: i32, stdout: &str) -> Collected {
        let mut result =
            TaskResult::success(worker_id, TaskId::new(), stdout.to_string(), 12, false);
        result.exit_code = exit_code;
        Collected::Done(Box::new(result))
    }

    #[test]
    fn test_judge() {
        let worker_id = Uuid::new_v4();
        let probe = |kind| Probe {
            kind,
            worker_name: "w".into(),
            worker_id,
            task: Ok(TaskId::new()),
        };
        let timeout = Duration::from_secs(5);
        let in_process = probe(ProbeKind::InProcess);
        assert_eq!(
            judge(&in_process, Some(&done(worker_id, 0, "ok")), timeout).0,
            Outcome::Pass
        );
        let (outcome, detail) = judge(&in_process, Some(&done(Uuid::new_v4(), 0, "ok")), timeout);
        assert_eq!(outcome, Outcome::Fail);
        assert!(detail.contains("ignores placement"), "{detail}");
        let (outcome, detail) = judge(&in_process, Some(&done(worker_id, 3, "boom")), timeout);
        assert_eq!(outcome, Outcome::Fail);
        assert!(
            detail.contains("exit code 3") && detail.contains("boom"),
            "{detail}"
        );
        let (outcome, detail) = judge(
            &in_process,
            Some(&Collected::TimedOut("Queued".into())),
            timeout,
        );
        assert_eq!(outcome, Outcome::Fail);
        assert!(
            detail.contains("within 5s") && detail.contains("Queued"),
            "{detail}"
        );

        let process = probe(ProbeKind::Process);
        assert_eq!(
            judge(
                &process,
                Some(&done(worker_id, 0, &format!("{PROBE_TOKEN}\r\n"))),
                timeout
            )
            .0,
            Outcome::Pass
        );
        assert_eq!(
            judge(
                &process,
                Some(&done(worker_id, 0, "something else")),
                timeout
            )
            .0,
            Outcome::Fail
        );

        let gpu = probe(ProbeKind::Gpu {
            dim: 512,
            simulated: true,
        });
        let (outcome, detail) = judge(
            &gpu,
            Some(&done(worker_id, 0, "...\nStatus: VERIFIED_OK\n")),
            timeout,
        );
        assert_eq!(outcome, Outcome::Pass);
        assert!(
            detail.contains("512x512") && detail.contains("simulated"),
            "{detail}"
        );
        assert_eq!(
            judge(
                &gpu,
                Some(&done(worker_id, 0, "Status: VERIFICATION_FAILED")),
                timeout
            )
            .0,
            Outcome::Fail
        );
    }

    #[test]
    fn test_report_rendering_and_outcome() {
        let mut report = DoctorReport {
            master: "127.0.0.1:8088".into(),
            workers: vec![WorkerSummary::of(&caps(
                "MAC",
                &["os:macos", "arch:aarch64"],
            ))],
            checks: Vec::new(),
            auth_failed: false,
        };
        report.push("C1", "Connect", None, Outcome::Pass, "connected in 3 ms");
        report.push("C6", "Spread", None, Outcome::Warn, "all on one worker");
        assert!(!report.failed());
        let text = report.render();
        assert!(
            text.contains("MAC") && text.contains("macos/aarch64"),
            "{text}"
        );
        assert!(text.contains("PASS  C1"), "{text}");
        assert!(
            text.contains("1 passed, 1 warnings, 0 failed, 0 skipped"),
            "{text}"
        );
        report.push("C3", "In-process probe", Some("MAC"), Outcome::Fail, "x");
        assert!(report.failed());
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["checks"][2]["outcome"], "fail");
        assert!(json.get("auth_failed").is_none());
    }
}
