//! `submit --verify <N>`: run a task again on N other workers and compare the outputs
//! (design 0089 D3, VERIFY-1).
//!
//! Everything here is client side: the replicas are ordinary tasks pinned with the existing
//! `require:worker=<id>` placement tag, and the outputs are compared by the BLAKE3 ids the
//! results already carry, so no wire format changes. Only meaningful for deterministic tasks; a
//! task whose outputs differ per run reports a mismatch.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use rusty_grid_core::blob::BlobFile;
use rusty_grid_core::placement::{
    listed_worker_id, tag_value, Placement, REQUIRE_PREFIX, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskResult};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::WorkerCapabilities;

/// Exit code of `submit --verify` when the outputs differ (1 stays for every other failure).
pub const MISMATCH_EXIT_CODE: u8 = 4;

/// The help text of the CLI flag.
pub const VERIFY_HELP: &str = "After the task completes, run it again on N other matching workers and compare the declared outputs (--output) by BLAKE3; prints 'verified' or 'mismatch' (exit 4). Only for deterministic tasks: a non-deterministic task will report a mismatch";

/// A worker that ran (or will run) one copy of the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub id: Uuid,
    pub name: String,
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
                .map(|w| json!({"id": w.id.to_string(), "name": w.name}))
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
        let names: Vec<&str> = self.workers.iter().map(|w| w.name.as_str()).collect();
        if self.verified() {
            return format!(
                "verified: {} file(s) identical on {} workers ({})",
                self.compared.len(),
                self.workers.len(),
                names.join(", ")
            );
        }
        let mut out = format!(
            "mismatch: {} of {} file(s) differ across {} workers ({})",
            self.differing.len(),
            self.compared.len(),
            self.workers.len(),
            names.join(", ")
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
/// Idle workers come first.
pub fn pick_targets(
    workers: &[WorkerCapabilities],
    placement: &Placement,
    requirements: &TaskRequirements,
    first: Uuid,
    n: usize,
) -> Result<Vec<Target>, String> {
    let vram = requirements.effective_vram_mb();
    let mut fits: Vec<(bool, Target)> = workers
        .iter()
        .filter(|w| tag_value(&w.tags, STATE_TAG_PREFIX) != Some("disconnected"))
        .filter(|w| placement.matches_listed(w))
        .filter(|w| !requirements.requires_gpu_or_vram() || w.can_execute_gpu())
        .filter(|w| w.cpu_cores >= requirements.cpu_cores && w.ram_mb >= requirements.ram_mb)
        .filter(|w| vram == 0 || w.vram_mb.is_some_and(|v| v >= vram))
        .filter_map(|w| {
            let id = listed_worker_id(w)?;
            (id != first).then(|| {
                let idle = tag_value(&w.tags, STATE_TAG_PREFIX) == Some("idle");
                (
                    !idle,
                    Target {
                        id,
                        name: w.name.clone(),
                    },
                )
            })
        })
        .collect();
    fits.sort_by_key(|(busy, _)| *busy);
    if fits.len() < n {
        return Err(format!(
            "--verify {n} needs {n} other workers that match the task, but only {} connected worker(s) besides the first one do",
            fits.len()
        ));
    }
    Ok(fits.into_iter().take(n).map(|(_, t)| t).collect())
}

/// The same task under a new id, pinned to `worker`.
pub fn replica_task(template: &Task, worker: Uuid) -> Task {
    let mut task = template.clone();
    task.id = TaskId::new();
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
}

/// Picks the workers, runs the replicas (one new connection from `connect` each) and compares
/// them with the first run. Shared by `submit --verify` and the MCP `submit` tool.
pub async fn verify_on_workers<C, Fut>(req: Request<'_>, connect: C) -> Result<Report, String>
where
    C: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<MessageTransport<GridStream>, String>>,
{
    let targets = pick_targets(
        req.workers,
        req.placement,
        &req.template.requirements,
        req.first.worker_id,
        req.replicas,
    )?;
    let mut jobs = Vec::new();
    for target in targets {
        let transport = connect().await?;
        let task = replica_task(req.template, target.id);
        jobs.push((target, transport, task));
    }
    eprintln!(
        "Verifying on {} other worker(s): {}",
        jobs.len(),
        jobs.iter()
            .map(|(t, _, _)| t.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let timeout = Duration::from_secs(req.template.requirements.timeout_secs.saturating_add(30));
    let mut runs = vec![Run {
        worker: Target {
            id: req.first.worker_id,
            name: req
                .first_name
                .unwrap_or_else(|| req.first.worker_id.to_string()),
        },
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
}
