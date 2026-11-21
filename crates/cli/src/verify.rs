//! `submit --verify <N>`: run a task again on N other workers and compare the outputs
//! (design 0089 D3, VERIFY-1).
//!
//! Everything here is client side: the replicas are ordinary tasks pinned with the existing
//! `require:worker=<id>` placement tag, and the outputs are compared by the BLAKE3 ids the
//! results already carry, so no wire format changes. Only meaningful for deterministic tasks; a
//! task whose outputs differ per run reports a mismatch.
//!
//! By default the replicas run only on workers with the operating system and CPU architecture of
//! the worker that ran the first copy (design 0089 D6, VERIFY-2): the same deterministic program
//! can give different results on two systems, which would be a false mismatch.
//! `--verify-any-system` lets any matching worker run them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use rusty_grid_core::blob::BlobFile;
use rusty_grid_core::placement::{
    listed_worker_id, normalize_arch, normalize_os, tag_value, Placement, ARCH_TAG_PREFIX,
    OS_TAG_PREFIX, REQUIRE_PREFIX, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::WorkerCapabilities;

/// Exit code of `submit --verify` when the outputs differ (1 stays for every other failure).
pub const MISMATCH_EXIT_CODE: u8 = 4;

/// The help text of the CLI flag.
pub const VERIFY_HELP: &str = "After the task completes, run it again on N other matching workers with the same operating system and CPU architecture as the first run (see --verify-any-system) and compare the declared outputs (--output) by BLAKE3; prints 'verified' or 'mismatch' (exit 4). Only for deterministic tasks: a non-deterministic task will report a mismatch";

/// The help text of `--verify-any-system`.
pub const VERIFY_ANY_SYSTEM_HELP: &str = "With --verify: let the replicas run on matching workers with another operating system or CPU architecture than the first run. By default they do not, because the same deterministic program can give different results on two systems";

/// The operating system and CPU architecture a listed worker reports in its `os:` and `arch:`
/// tags, normalized as placement does (`darwin` is `macos`, `arm64` is `aarch64`). `None` for a
/// tag the worker does not carry (an older worker) or a worker that is not listed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct System {
    pub os: Option<String>,
    pub arch: Option<String>,
}

impl System {
    /// The system a listed worker reports.
    pub fn of(caps: &WorkerCapabilities) -> Self {
        Self {
            os: tag_value(&caps.tags, OS_TAG_PREFIX).map(normalize_os),
            arch: tag_value(&caps.tags, ARCH_TAG_PREFIX).map(normalize_arch),
        }
    }

    /// The system of the listed worker with this id; empty when it is not listed.
    pub fn of_listed(workers: &[WorkerCapabilities], id: Uuid) -> Self {
        workers
            .iter()
            .find(|w| listed_worker_id(w) == Some(id))
            .map(Self::of)
            .unwrap_or_default()
    }

    /// Whether a worker on `other` runs this system. A part this one does not know is not
    /// compared, so a first worker without an `os:` tag filters on its architecture only.
    pub fn admits(&self, other: &System) -> bool {
        let same =
            |mine: &Option<String>, theirs: &Option<String>| mine.is_none() || mine == theirs;
        same(&self.os, &other.os) && same(&self.arch, &other.arch)
    }

    /// What this system does not know, for the notice; `None` when both parts are known.
    pub fn missing(&self) -> Option<&'static str> {
        match (&self.os, &self.arch) {
            (Some(_), Some(_)) => None,
            (None, Some(_)) => Some("operating system (os: tag)"),
            (Some(_), None) => Some("CPU architecture (arch: tag)"),
            (None, None) => Some("operating system and CPU architecture (os: and arch: tags)"),
        }
    }

    fn is_known(&self) -> bool {
        self.os.is_some() || self.arch.is_some()
    }
}

impl fmt::Display for System {
    /// `macos/aarch64`: the known parts, or `unknown system` when neither is.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<&str> = [self.os.as_deref(), self.arch.as_deref()]
            .into_iter()
            .flatten()
            .collect();
        if parts.is_empty() {
            f.write_str("unknown system")
        } else {
            f.write_str(&parts.join("/"))
        }
    }
}

/// A worker that ran (or will run) one copy of the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub id: Uuid,
    pub name: String,
    pub system: System,
}

/// The workers' names with their systems: `a, b, all macos/aarch64` when they share one, else
/// `a on macos/aarch64, b on windows/x86_64`.
pub fn describe_workers(workers: &[Target]) -> String {
    let names = || workers.iter().map(|w| w.name.as_str()).collect::<Vec<_>>();
    match workers.first() {
        Some(first) if workers.iter().all(|w| w.system == first.system) => {
            if first.system.is_known() {
                format!("{}, all {}", names().join(", "), first.system)
            } else {
                names().join(", ")
            }
        }
        _ => workers
            .iter()
            .map(|w| format!("{} on {}", w.name, w.system))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

/// What one worker produced.
#[derive(Debug, Clone)]
pub struct Run {
    pub worker: Target,
    pub outputs: Vec<BlobFile>,
}

/// One file whose content differs (or is missing) between workers: `(worker name, BLAKE3 hex)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub path: String,
    pub digests: Vec<(String, Option<String>)>,
}

/// The outcome of comparing the runs.
#[derive(Debug, Clone)]
pub struct Report {
    pub workers: Vec<Target>,
    pub compared: Vec<String>,
    pub differing: Vec<Difference>,
}

impl Report {
    pub fn verified(&self) -> bool {
        self.differing.is_empty()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "status": if self.verified() { "verified" } else { "mismatch" },
            "replicas": self.workers.len().saturating_sub(1),
            "workers": self.workers.iter()
                .map(|w| json!({
                    "id": w.id.to_string(),
                    "name": w.name,
                    "os": w.system.os,
                    "arch": w.system.arch,
                }))
                .collect::<Vec<_>>(),
            "compared": self.compared,
            "differing": self.differing.iter().map(|d| json!({
                "path": d.path,
                "workers": d.digests.iter()
                    .map(|(worker, hex)| json!({"worker": worker, "blake3": hex}))
                    .collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    }

    /// The lines the CLI prints.
    pub fn text(&self) -> String {
        let workers = describe_workers(&self.workers);
        if self.verified() {
            return format!(
                "verified: {} file(s) identical on {} workers ({workers})",
                self.compared.len(),
                self.workers.len(),
            );
        }
        let mut out = format!(
            "mismatch: {} of {} file(s) differ across {} workers ({workers})",
            self.differing.len(),
            self.compared.len(),
            self.workers.len(),
        );
        for d in &self.differing {
            out.push_str(&format!("\n  - {}", d.path));
            for (worker, hex) in &d.digests {
                let shown = hex
                    .as_deref()
                    .map_or("missing".to_string(), |h| h[..16.min(h.len())].to_string());
                out.push_str(&format!("\n      {worker}: {shown}"));
            }
        }
        out
    }
}

/// Checks the verify options before anything connects.
pub fn check_options(
    replicas: usize,
    verify_file: Option<&str>,
    has_outputs: bool,
    pinned_worker: bool,
) -> Result<(), String> {
    if replicas == 0 {
        return Err("--verify needs a number of replicas of at least 1".to_string());
    }
    if !has_outputs && verify_file.is_none() {
        return Err(
            "--verify compares declared outputs: give --output <file or glob> or --verify-file <file>"
                .to_string(),
        );
    }
    if pinned_worker {
        return Err(
            "--verify cannot be combined with --worker: it chooses the workers itself".to_string(),
        );
    }
    if let Some(file) = verify_file {
        rusty_grid_core::blob::validate_output_patterns(&[file.to_string()])
            .map_err(|e| format!("--verify-file: {e}"))?;
        if file.contains(['*', '?', '[']) {
            return Err("--verify-file names one file, not a pattern".to_string());
        }
    }
    Ok(())
}

/// Picks `n` connected workers other than `first` that fit the task's placement and resources.
/// Unless `any_system`, only workers on the operating system and CPU architecture that `first`
/// reports in `workers` count (a part it does not report is not compared). Idle workers come
/// first.
pub fn pick_targets(
    workers: &[WorkerCapabilities],
    placement: &Placement,
    requirements: &TaskRequirements,
    first: Uuid,
    n: usize,
    any_system: bool,
) -> Result<Vec<Target>, String> {
    let vram = requirements.effective_vram_mb();
    let wanted = (!any_system).then(|| System::of_listed(workers, first));
    let mut fits: Vec<(bool, Target)> = workers
        .iter()
        .filter(|w| tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected"))
        .filter(|w| placement.matches_listed(w))
        .filter(|w| !requirements.requires_gpu_or_vram() || w.can_execute_gpu())
        .filter(|w| w.cpu_cores >= requirements.cpu_cores && w.ram_mb >= requirements.ram_mb)
        .filter(|w| vram == 0 || w.vram_mb.is_some_and(|v| v >= vram))
        .filter_map(|w| {
            let id = listed_worker_id(w)?;
            let system = System::of(w);
            if id == first || wanted.as_ref().is_some_and(|s| !s.admits(&system)) {
                return None;
            }
            let idle = tag_value(&w.tags, STATE_TAG_PREFIX) == Some("idle");
            let name = w.name.clone();
            Some((!idle, Target { id, name, system }))
        })
        .collect();
    fits.sort_by_key(|(busy, _)| *busy);
    if fits.len() < n {
        let only = match wanted.filter(System::is_known) {
            Some(system) => format!(
                " (only workers on {system}, the system of the first run, count; --verify-any-system allows others)"
            ),
            None => String::new(),
        };
        return Err(format!(
            "--verify {n} needs {n} other workers that match the task, but only {} connected worker(s) besides the first one do{only}",
            fits.len()
        ));
    }
    Ok(fits.into_iter().take(n).map(|(_, t)| t).collect())
}

/// The same task under a new id, pinned to `worker`.
pub fn replica_task(template: &Task, worker: Uuid) -> Task {
    let mut task = template.clone();
    task.id = TaskId::new();
    task.strip_action_cache();
    let pin = format!("{REQUIRE_PREFIX}worker=");
    task.tags.retain(|t| !t.starts_with(&pin));
    task.tags.push(format!("{pin}{worker}"));
    task
}

/// Submits each replica on its own connection, waits for all and returns their outputs in the
/// order given. Any failed replica is an error naming the worker.
pub async fn run_replicas(
    jobs: Vec<(Target, MessageTransport<GridStream>, Task)>,
    timeout: Duration,
) -> Result<Vec<Run>, String> {
    let mut set = tokio::task::JoinSet::new();
    let mut runs: Vec<Option<Run>> = (0..jobs.len()).map(|_| None).collect();
    for (index, (target, mut transport, task)) in jobs.into_iter().enumerate() {
        set.spawn(async move {
            let outcome = replica(&mut transport, task, timeout).await;
            (index, target, outcome)
        });
    }
    let mut failure: Option<String> = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((index, target, Ok(outputs))) => {
                runs[index] = Some(Run {
                    worker: target,
                    outputs,
                });
            }
            Ok((_, target, Err(e))) => {
                failure.get_or_insert(format!("replica on worker {} failed: {e}", target.name));
            }
            Err(e) => {
                failure.get_or_insert(format!("replica task panicked: {e}"));
            }
        }
    }
    if let Some(f) = failure {
        return Err(f);
    }
    Ok(runs.into_iter().flatten().collect())
}

async fn replica(
    transport: &mut MessageTransport<GridStream>,
    task: Task,
    timeout: Duration,
) -> Result<Vec<BlobFile>, String> {
    transport
        .send_msg(&ClientMessage::SubmitTask { task, wait: true })
        .await
        .map_err(|e| format!("could not submit: {e}"))?;
    match tokio::time::timeout(timeout, transport.recv_msg::<ClientResponse>()).await {
        Ok(Ok(Some(ClientResponse::TaskCompleted { result, .. }))) => {
            if result.is_success() {
                Ok(result.outputs)
            } else {
                Err(format!(
                    "exit code {}{}",
                    result.exit_code,
                    result.error.map(|e| format!(": {e}")).unwrap_or_default()
                ))
            }
        }
        Ok(Ok(Some(ClientResponse::Error { message }))) => Err(message),
        Ok(Ok(Some(other))) => Err(format!("unexpected master response: {other:?}")),
        Ok(Ok(None)) => Err("master closed the connection".to_string()),
        Ok(Err(e)) => Err(format!("communication error: {e}")),
        Err(_) => Err(format!("timed out after {}s", timeout.as_secs())),
    }
}

/// What `verify_on_workers` needs to know about the first run.
pub struct Request<'a> {
    pub workers: &'a [WorkerCapabilities],
    pub template: &'a Task,
    pub placement: &'a Placement,
    pub first: &'a TaskResult,
    pub first_name: Option<String>,
    pub replicas: usize,
    pub file: Option<&'a str>,
    /// `--verify-any-system`: replicas may run on another operating system or architecture.
    pub any_system: bool,
}

/// Picks the workers, runs the replicas (one new connection from `connect` each) and compares
/// them with the first run. Shared by `submit --verify` and the MCP `submit` tool.
pub async fn verify_on_workers<C, Fut>(req: Request<'_>, connect: C) -> Result<Report, String>
where
    C: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<MessageTransport<GridStream>, String>>,
{
    let first = Target {
        id: req.first.worker_id,
        name: req
            .first_name
            .unwrap_or_else(|| req.first.worker_id.to_string()),
        system: System::of_listed(req.workers, req.first.worker_id),
    };
    if !req.any_system {
        if let Some(missing) = first.system.missing() {
            eprintln!(
                "Note: worker {} that ran the first copy does not report its {missing}; the replicas are not filtered on it",
                first.name
            );
        }
    }
    let targets = pick_targets(
        req.workers,
        req.placement,
        &req.template.requirements,
        req.first.worker_id,
        req.replicas,
        req.any_system,
    )?;
    eprintln!(
        "Verifying on {} other worker(s): {}",
        targets.len(),
        describe_workers(&targets)
    );
    let mut jobs = Vec::new();
    for target in targets {
        let transport = connect().await?;
        let task = replica_task(req.template, target.id);
        jobs.push((target, transport, task));
    }
    let timeout = Duration::from_secs(req.template.requirements.timeout_secs.saturating_add(30));
    let mut runs = vec![Run {
        worker: first,
        outputs: req.first.outputs.clone(),
    }];
    runs.extend(run_replicas(jobs, timeout).await?);
    compare(&runs, req.file)
}

/// Compares the runs (the first entry is the original run). With `file`, only that file's
/// content is compared; otherwise every declared output any run produced.
pub fn compare(runs: &[Run], file: Option<&str>) -> Result<Report, String> {
    let first = runs.first().ok_or("no runs to compare")?;
    let paths: BTreeSet<String> = match file {
        Some(f) => BTreeSet::from([f.to_string()]),
        None => runs
            .iter()
            .flat_map(|r| r.outputs.iter().map(|o| o.path.clone()))
            .collect(),
    };
    if paths.is_empty() || first.outputs.is_empty() {
        return Err(
            "the task produced no output files to compare (check the --output patterns)"
                .to_string(),
        );
    }
    let by_run: Vec<BTreeMap<&str, String>> = runs
        .iter()
        .map(|r| {
            r.outputs
                .iter()
                .map(|o| (o.path.as_str(), o.id.to_hex()))
                .collect()
        })
        .collect();
    let mut differing = Vec::new();
    for path in &paths {
        let digests: Vec<Option<&String>> = by_run.iter().map(|m| m.get(path.as_str())).collect();
        if digests.iter().any(|d| *d != digests[0]) || digests[0].is_none() {
            differing.push(Difference {
                path: path.clone(),
                digests: runs
                    .iter()
                    .zip(digests)
                    .map(|(r, d)| (r.worker.name.clone(), d.cloned()))
                    .collect(),
            });
        }
    }
    Ok(Report {
        workers: runs.iter().map(|r| r.worker.clone()).collect(),
        compared: paths.into_iter().collect(),
        differing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::blob::BlobId;

    fn run(name: &str, files: &[(&str, &[u8])]) -> Run {
        Run {
            worker: Target {
                id: Uuid::new_v4(),
                name: name.into(),
                system: System::default(),
            },
            outputs: files
                .iter()
                .map(|(p, d)| BlobFile {
                    path: (*p).into(),
                    id: BlobId::of(d),
                    size: d.len() as u64,
                    executable: false,
                })
                .collect(),
        }
    }

    #[test]
    fn identical_outputs_verify() {
        let runs = [run("a", &[("o.txt", b"x")]), run("b", &[("o.txt", b"x")])];
        let report = compare(&runs, None).unwrap();
        assert!(report.verified());
        assert!(report.text().starts_with("verified"));
        assert_eq!(report.to_json()["status"], "verified");
    }

    #[test]
    fn differing_and_missing_files_are_named() {
        let runs = [
            run("a", &[("o.txt", b"x"), ("p.txt", b"1")]),
            run("b", &[("o.txt", b"y")]),
        ];
        let report = compare(&runs, None).unwrap();
        let paths: Vec<_> = report.differing.iter().map(|d| d.path.as_str()).collect();
        assert_eq!(paths, ["o.txt", "p.txt"]);
        assert!(report.text().contains("p.txt") && report.text().contains("missing"));
    }

    #[test]
    fn verify_file_compares_only_that_file() {
        let runs = [
            run("a", &[("sums", b"s"), ("o", b"1")]),
            run("b", &[("sums", b"s"), ("o", b"2")]),
        ];
        assert!(compare(&runs, Some("sums")).unwrap().verified());
        assert!(!compare(&runs, None).unwrap().verified());
        assert!(!compare(&runs, Some("absent")).unwrap().verified());
    }

    #[test]
    fn no_outputs_is_an_error() {
        let runs = [run("a", &[]), run("b", &[])];
        assert!(compare(&runs, None).is_err());
    }

    #[test]
    fn options_are_checked() {
        assert!(check_options(0, None, true, false).is_err());
        assert!(check_options(1, None, false, false).is_err());
        assert!(check_options(1, Some("sums"), false, false).is_ok());
        assert!(check_options(1, None, true, true).is_err());
        assert!(check_options(1, Some("*.sha"), false, false).is_err());
        assert!(check_options(1, Some("../x"), false, false).is_err());
    }

    #[test]
    fn replica_is_pinned_with_a_fresh_id() {
        let template = Task::new(
            rusty_grid_core::task::TaskSpec::Command {
                program: "true".into(),
                args: vec![],
                env: Default::default(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 10),
        );
        let w = Uuid::new_v4();
        let r = replica_task(&template, w);
        assert_ne!(r.id, template.id);
        assert!(r.tags.contains(&format!("require:worker={w}")));
    }

    /// A connected idle worker as `ListWorkers` reports it, with the master's `id:` tag and the
    /// given system tags (`os:...`, `arch:...`).
    fn listed(name: &str, system_tags: &[&str]) -> (Uuid, WorkerCapabilities) {
        let id = Uuid::new_v4();
        let mut tags = vec![format!("id:{id}"), "state:idle".to_string()];
        tags.extend(system_tags.iter().map(|t| t.to_string()));
        let caps = WorkerCapabilities {
            name: name.into(),
            cpu_cores: 4,
            ram_mb: 8192,
            tags,
            ..WorkerCapabilities::default()
        };
        (id, caps)
    }

    fn pick(
        workers: &[WorkerCapabilities],
        first: Uuid,
        n: usize,
        any_system: bool,
    ) -> Result<Vec<String>, String> {
        let requirements = TaskRequirements::generic(1, 10);
        pick_targets(
            workers,
            &Placement::default(),
            &requirements,
            first,
            n,
            any_system,
        )
        .map(|targets| targets.into_iter().map(|t| t.name).collect())
    }

    /// The first worker `mac1` (macOS arm64, tagged `darwin`/`arm64`, which normalize to
    /// `macos`/`aarch64`), another Mac, a Windows x86_64 worker and a Linux arm64 worker.
    fn mixed() -> (Uuid, Vec<WorkerCapabilities>) {
        let (first, mac1) = listed("mac1", &["os:darwin", "arch:arm64"]);
        let (_, mac2) = listed("mac2", &["os:macos", "arch:aarch64"]);
        let (_, win) = listed("win", &["os:windows", "arch:x86_64"]);
        let (_, linux) = listed("linux-arm", &["os:linux", "arch:aarch64"]);
        (first, vec![mac1, mac2, win, linux])
    }

    #[test]
    fn replicas_go_to_the_same_system_by_default() {
        let (first, workers) = mixed();
        assert_eq!(pick(&workers, first, 1, false).unwrap(), ["mac2"]);
    }

    #[test]
    fn other_systems_are_refused_by_default_and_the_error_names_the_flag() {
        let (first, workers) = mixed();
        let err = pick(&workers, first, 2, false).unwrap_err();
        assert!(err.contains("needs 2 other workers"), "{err}");
        assert!(err.contains("only 1 connected worker(s)"), "{err}");
        assert!(err.contains("macos/aarch64"), "{err}");
        assert!(err.contains("--verify-any-system"), "{err}");
    }

    #[test]
    fn any_system_allows_other_systems() {
        let (first, workers) = mixed();
        let mut picked = pick(&workers, first, 3, true).unwrap();
        picked.sort();
        assert_eq!(picked, ["linux-arm", "mac2", "win"]);
        let err = pick(&workers, first, 4, true).unwrap_err();
        assert!(!err.contains("--verify-any-system"), "{err}");
    }

    #[test]
    fn a_first_worker_without_system_tags_falls_back_to_placement() {
        let (first, old) = listed("old", &[]);
        let (_, mac) = listed("mac", &["os:macos", "arch:aarch64"]);
        let (_, win) = listed("win", &["os:windows", "arch:x86_64"]);
        let workers = [old, mac, win];
        let mut picked = pick(&workers, first, 2, false).unwrap();
        picked.sort();
        assert_eq!(picked, ["mac", "win"]);
        let err = pick(&workers, first, 3, false).unwrap_err();
        assert!(!err.contains("--verify-any-system"), "{err}");
        assert_eq!(
            System::of_listed(&workers, first).missing(),
            Some("operating system and CPU architecture (os: and arch: tags)")
        );
    }

    #[test]
    fn a_missing_tag_on_the_first_worker_is_not_compared() {
        let (first, arch_only) = listed("arm-only", &["arch:aarch64"]);
        let (_, mac) = listed("mac", &["os:macos", "arch:aarch64"]);
        let (_, linux) = listed("linux-arm", &["os:linux", "arch:aarch64"]);
        let (_, win) = listed("win", &["os:windows", "arch:x86_64"]);
        let (_, untagged) = listed("untagged", &[]);
        let workers = [arch_only, mac, linux, win, untagged];
        let mut picked = pick(&workers, first, 2, false).unwrap();
        picked.sort();
        assert_eq!(picked, ["linux-arm", "mac"]);
        let err = pick(&workers, first, 3, false).unwrap_err();
        assert!(err.contains("only workers on aarch64"), "{err}");
        assert_eq!(
            System::of_listed(&workers, first).missing(),
            Some("operating system (os: tag)")
        );
    }

    #[test]
    fn the_report_names_the_systems() {
        let mut runs = [run("a", &[("o", b"1")]), run("b", &[("o", b"1")])];
        let mac = System {
            os: Some("macos".into()),
            arch: Some("aarch64".into()),
        };
        runs[0].worker.system = mac.clone();
        runs[1].worker.system = mac;
        let report = compare(&runs, None).unwrap();
        assert!(
            report.text().ends_with("(a, b, all macos/aarch64)"),
            "{}",
            report.text()
        );
        assert_eq!(report.to_json()["workers"][1]["os"], "macos");
        runs[1].worker.system = System {
            os: Some("windows".into()),
            arch: Some("x86_64".into()),
        };
        let report = compare(&runs, None).unwrap();
        assert!(
            report
                .text()
                .ends_with("(a on macos/aarch64, b on windows/x86_64)"),
            "{}",
            report.text()
        );
        // Workers that report no system are listed by name only.
        let plain = [run("a", &[("o", b"1")]), run("b", &[("o", b"1")])];
        assert!(compare(&plain, None).unwrap().text().ends_with("(a, b)"));
    }

    #[test]
    fn replica_task_strips_action_cache_extension() {
        use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
        let template = Task::new(
            TaskSpec::BuiltinTest {
                test_name: "verify-cache".into(),
                iterations: 1,
                duration_ms: 1,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 30),
        )
        .with_action_cache();
        assert!(template.is_action_cache_enabled());
        let replica = replica_task(&template, Uuid::new_v4());
        assert!(
            !replica.is_action_cache_enabled(),
            "verification replicas must strip action cache opt-in"
        );
    }
}
