//! In-memory Map/Reduce distributed engine coordinating parallel mapper and reducer tasks.
//!
//! Map tasks (one per partition) and, for command and shell reducers, reduce tasks (one per key)
//! are submitted to the grid together, at most `parallelism` in flight, so every device with a free
//! slot works on the job at once. Map output is merged in partition order, and the first failure
//! fails the job and cancels whatever of it is still queued or running.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use futures::stream::{self, StreamExt};

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::mapreduce::{
    GroupedKeyValue, MapFunctionSpec, MapReduceJobSpec, MapReduceResult, ReduceFunctionSpec,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};

use crate::queue::TaskQueue;
use crate::registry::WorkerRegistry;
use crate::server::{prune_closed_waiters, WaiterMap};

/// Default number of a job's tasks in flight at once (see [`MapReduceEngine::with_parallelism`]).
pub const DEFAULT_MAPREDUCE_PARALLELISM: usize = 256;

/// The line a worker appends to a captured stream it had to cut short (`read_bounded_stream` in
/// `rusty_grid_worker`). The task still exits 0, so this marker is the only sign of the loss.
pub const OUTPUT_TRUNCATED_MARKER: &str =
    "[rusty_grid: output truncated after exceeding size limit]";

/// True if `result.stdout` was cut short by the worker's capture limit.
pub fn stdout_was_truncated(result: &TaskResult) -> bool {
    let out = &result.stdout.0;
    let tail = &out[out.len().saturating_sub(OUTPUT_TRUNCATED_MARKER.len() + 8)..];
    tail.windows(OUTPUT_TRUNCATED_MARKER.len())
        .any(|w| w == OUTPUT_TRUNCATED_MARKER.as_bytes())
}

/// Shared "last sign of life" clock of one job, so a task's wait only times out when the job as a
/// whole has made no progress for the whole timeout (tasks queued behind others are not late).
struct JobProgress {
    last: std::sync::Mutex<Instant>,
}

impl JobProgress {
    fn new() -> Self {
        Self {
            last: std::sync::Mutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }

    fn last(&self) -> Instant {
        *self.last.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Cancels a submitted task if the future waiting for it is dropped first (the job failed, or the
/// client went away), so abandoned work neither sits in the queue nor keeps a worker busy.
///
/// Only armed after `queue.submit` succeeds; if dropped while still waiting for queue room, only
/// the waiter table entry is pruned.
struct AbandonGuard {
    engine: MapReduceEngine,
    task_id: rusty_grid_core::task::TaskId,
    armed: bool,
    waiter_registered: bool,
}

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        if !self.armed && !self.waiter_registered {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let task_id = self.task_id;
        if self.armed {
            let engine = self.engine.clone();
            runtime.spawn(async move {
                engine
                    .cancel_abandoned(task_id, "MapReduce job abandoned")
                    .await;
            });
        } else {
            let waiters = self.engine.waiters.clone();
            runtime.spawn(async move {
                prune_closed_waiters(&waiters, &task_id).await;
            });
        }
    }
}

/// Doubles the queue-wait backoff step up to the 1-second ceiling.
fn next_queue_wait_backoff(current: Duration) -> Duration {
    (current * 2).min(Duration::from_secs(1))
}

/// Lightweight in-memory Map/Reduce engine executing jobs across cluster workers.
#[derive(Clone)]
pub struct MapReduceEngine {
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    registry: Option<WorkerRegistry>,
    parallelism: usize,
    #[cfg(test)]
    cancel_abandoned_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl MapReduceEngine {
    /// Creates a new MapReduceEngine bound to the master task queue and completion waiters.
    pub fn new(
        queue: TaskQueue,
        scheduler_notify: Arc<tokio::sync::Notify>,
        waiters: WaiterMap,
    ) -> Self {
        Self {
            queue,
            scheduler_notify,
            waiters,
            registry: None,
            parallelism: DEFAULT_MAPREDUCE_PARALLELISM,
            #[cfg(test)]
            cancel_abandoned_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    fn cancel_abandoned_count(&self) -> usize {
        self.cancel_abandoned_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Maximum number of the job's tasks submitted and not yet finished at any time (at least 1).
    ///
    /// Keep it above the total number of slots in the cluster, otherwise devices sit idle. Tasks
    /// beyond it are only created once an earlier one finishes, which also bounds the memory held
    /// by a job with many reduce keys.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// Lets the engine tell workers to stop tasks of a failed or abandoned job. Without it such
    /// tasks are only cancelled in the queue (a task already running finishes unobserved).
    pub fn with_registry(mut self, registry: WorkerRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Executes an in-memory Map/Reduce job according to the provided specification.
    pub async fn execute(&self, spec: MapReduceJobSpec) -> GridResult<MapReduceResult> {
        let start_time = Instant::now();
        info!(job_id = %spec.job_id, name = %spec.name, "Starting Map/Reduce execution");

        if spec.input_data.is_empty() {
            return Ok(MapReduceResult {
                job_id: spec.job_id,
                status: "Completed".into(),
                output: HashMap::new(),
                map_tasks_total: 0,
                map_tasks_completed: 0,
                reduce_tasks_total: 0,
                reduce_tasks_completed: 0,
                execution_time_ms: 0,
                error: None,
            });
        }

        // Unsupported operators fail the job before anything is submitted.
        let map_script = match &spec.mapper {
            MapFunctionSpec::Builtin { operator } => Some(builtin_map_script(operator)?),
            _ => None,
        };
        if let ReduceFunctionSpec::Builtin { operator } = &spec.reducer {
            validate_builtin_reducer(operator)?;
        }

        // 1. Partition input data into chunks
        let num_partitions = spec.partition_count.max(1).min(spec.input_data.len());
        let mut chunks: Vec<Vec<String>> = vec![Vec::new(); num_partitions];
        for (i, line) in spec.input_data.iter().enumerate() {
            chunks[i % num_partitions].push(line.clone());
        }
        let chunks: Vec<Vec<String>> = chunks.into_iter().filter(|c| !c.is_empty()).collect();
        let map_tasks_total = chunks.len();
        let timeout_secs = if spec.timeout_secs == 0 {
            60
        } else {
            spec.timeout_secs
        };
        let timeout = Duration::from_secs(timeout_secs);
        let progress = JobProgress::new();

        // 2. Dispatch Map Tasks: all partitions at once (bounded by `parallelism`). They finish in
        //    any order, and the first failure fails the job at once; the pairs are kept per
        //    partition and merged in partition order below, so the result does not depend on timing.
        let mut per_partition: Vec<Vec<(String, serde_json::Value)>> =
            vec![Vec::new(); map_tasks_total];
        let mut map_tasks_completed = 0;
        {
            let mut results = stream::iter(chunks.into_iter().enumerate())
                .map(|(idx, chunk)| {
                    let task = Task::new(
                        map_task_spec(&spec.mapper, map_script, chunk.join("\n")),
                        TaskRequirements::generic(1, timeout_secs),
                    );
                    let progress = &progress;
                    async move {
                        (
                            idx,
                            self.submit_and_await(task, timeout, Some(progress)).await,
                        )
                    }
                })
                .buffer_unordered(self.parallelism);

            // An early return drops `results`, which cancels every partition still in flight.
            while let Some((idx, outcome)) = results.next().await {
                let result = outcome?;
                if result.exit_code != 0 {
                    let uses_sh = !matches!(spec.mapper, MapFunctionSpec::Command { .. });
                    return Ok(failed_result(
                        &spec,
                        start_time,
                        (map_tasks_total, map_tasks_completed),
                        (0, 0),
                        format!(
                            "Map partition {} failed with exit code {}: {}",
                            idx,
                            result.exit_code,
                            task_failure_detail(&result, uses_sh)
                        ),
                    ));
                }
                if stdout_was_truncated(&result) {
                    // The task exited 0 with part of its output cut off: counting what is left
                    // would silently under-count (and invent a key from the cut-off last line).
                    return Ok(failed_result(
                        &spec,
                        start_time,
                        (map_tasks_total, map_tasks_completed),
                        (0, 0),
                        format!(
                            "Map partition {idx} produced more output than a worker captures and \
                             it was truncated, so the result would be incomplete; use more \
                             partitions (--chunks) or have the mapper aggregate its output"
                        ),
                    ));
                }

                map_tasks_completed += 1;
                parse_map_output(&result, &mut per_partition[idx]);
            }
        }
        let intermediate_kvs = per_partition.into_iter().flatten();

        // 3. Shuffle & Group by key
        let mut grouped: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for (k, v) in intermediate_kvs {
            grouped.entry(k).or_default().push(v);
        }

        let reduce_tasks_total = grouped.len();
        let mut reduce_tasks_completed = 0;
        let mut final_output: HashMap<String, serde_json::Value> = HashMap::new();

        // 4. Reduce Phase
        match &spec.reducer {
            ReduceFunctionSpec::Builtin { operator } => {
                for (key, values) in grouped {
                    if let Some(reduced) = reduce_builtin(operator, &values)? {
                        final_output.insert(key, reduced);
                    }
                    reduce_tasks_completed += 1;
                }
            }
            ReduceFunctionSpec::ShellScript { .. } | ReduceFunctionSpec::Command { .. } => {
                let mut results = stream::iter(grouped)
                    .map(|(key, values)| {
                        let built =
                            reduce_task_spec(&spec.reducer, &key, &values).map(|task_spec| {
                                Task::new(task_spec, TaskRequirements::generic(1, timeout_secs))
                            });
                        let progress = &progress;
                        async move {
                            let outcome = match built {
                                Ok(task) => {
                                    self.submit_and_await(task, timeout, Some(progress)).await
                                }
                                Err(e) => Err(e),
                            };
                            (key, outcome)
                        }
                    })
                    .buffer_unordered(self.parallelism);

                while let Some((key, outcome)) = results.next().await {
                    let result = outcome?;
                    if result.exit_code != 0 {
                        let uses_sh =
                            matches!(spec.reducer, ReduceFunctionSpec::ShellScript { .. });
                        return Ok(failed_result(
                            &spec,
                            start_time,
                            (map_tasks_total, map_tasks_completed),
                            (reduce_tasks_total, reduce_tasks_completed),
                            format!(
                                "Reducer for key '{key}' failed: {}",
                                task_failure_detail(&result, uses_sh)
                            ),
                        ));
                    }
                    if stdout_was_truncated(&result) {
                        return Ok(failed_result(
                            &spec,
                            start_time,
                            (map_tasks_total, map_tasks_completed),
                            (reduce_tasks_total, reduce_tasks_completed),
                            format!(
                                "Reducer for key '{key}' produced more output than a worker \
                                 captures and it was truncated"
                            ),
                        ));
                    }
                    let reduced_val = serde_json::from_str(result.stdout_str().trim())
                        .unwrap_or_else(|_| {
                            serde_json::Value::String(result.stdout_str().trim().to_string())
                        });
                    reduce_tasks_completed += 1;
                    final_output.insert(key, reduced_val);
                }
            }
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        info!(
            job_id = %spec.job_id,
            map_tasks = map_tasks_completed,
            reduce_tasks = reduce_tasks_completed,
            time_ms = execution_time_ms,
            "Map/Reduce job completed successfully"
        );

        Ok(MapReduceResult {
            job_id: spec.job_id,
            status: "Completed".into(),
            output: final_output,
            map_tasks_total,
            map_tasks_completed,
            reduce_tasks_total,
            reduce_tasks_completed,
            execution_time_ms,
            error: None,
        })
    }

    /// Submits `task` and waits for its result.
    ///
    /// `timeout` is how long the wait may go without sign of life: with a job `progress` clock it
    /// restarts whenever any task of the job finishes, so a task that merely queued behind others
    /// is not timed out; without one it is a plain deadline. On timeout the task is cancelled and
    /// its waiter removed before returning; if this future is dropped instead (the job failed), a
    /// drop guard cancels the task in the background.
    async fn submit_and_await(
        &self,
        task: Task,
        timeout: Duration,
        progress: Option<&JobProgress>,
    ) -> GridResult<TaskResult> {
        let task_id = task.id;
        let mut task_slot = Some(task);
        // Declared before `rx` so that on future drop, `rx` is dropped first (closing `tx`)
        // before `AbandonGuard::drop` spawns `prune_closed_waiters`.
        let mut guard = AbandonGuard {
            engine: self.clone(),
            task_id,
            armed: false,
            waiter_registered: false,
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.waiters.write().await;
            lock.entry(task_id).or_default().push(tx);
        }
        guard.waiter_registered = true;
        let wait_start = Instant::now();
        let mut backoff = Duration::from_millis(25);
        loop {
            match self.queue.try_submit_slot(&mut task_slot).await {
                Ok(_) => {
                    guard.armed = true;
                    break;
                }
                Err(e) if !e.is_transient() => {
                    guard.waiter_registered = false;
                    drop(rx);
                    prune_closed_waiters(&self.waiters, &task_id).await;
                    return Err(e);
                }
                Err(
                    e @ (GridError::QueueByteLimitExceeded { .. } | GridError::QueueFull { .. }),
                ) => {
                    let idle_since = progress.map_or(wait_start, |p| p.last().max(wait_start));
                    let remaining = timeout.saturating_sub(idle_since.elapsed());
                    if remaining.is_zero() {
                        guard.waiter_registered = false;
                        drop(rx);
                        prune_closed_waiters(&self.waiters, &task_id).await;
                        return Err(GridError::Timeout {
                            operation: format!(
                                "MapReduce task {task_id} waiting for queue room ({e})"
                            ),
                            duration_secs: timeout.as_secs(),
                        });
                    }
                    tokio::time::sleep(remaining.min(backoff)).await;
                    backoff = next_queue_wait_backoff(backoff);
                }
                Err(e) => {
                    guard.waiter_registered = false;
                    drop(rx);
                    prune_closed_waiters(&self.waiters, &task_id).await;
                    return Err(e);
                }
            }
        }
        debug_assert!(
            task_slot.is_none(),
            "original task must be moved into the queue on submit"
        );
        self.scheduler_notify.notify_one();

        let own_start = Instant::now();
        let mut rx = rx;
        loop {
            let idle_since = progress.map_or(own_start, |p| p.last().max(own_start));
            let remaining = timeout.saturating_sub(idle_since.elapsed());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, &mut rx).await {
                Ok(Ok(result)) => {
                    guard.armed = false;
                    guard.waiter_registered = false;
                    if let Some(p) = progress {
                        p.touch();
                    }
                    return Ok(result);
                }
                Ok(Err(_)) => {
                    guard.armed = false;
                    guard.waiter_registered = false;
                    return Err(GridError::ConnectionClosed);
                }
                // Not due yet if another task of the job finished meanwhile: re-evaluate.
                Err(_) => continue,
            }
        }

        // Nobody is waiting any more: stop the abandoned task from sitting in the queue
        // (and later running for nobody) and drop its waiter entry.
        guard.armed = false;
        guard.waiter_registered = false;
        drop(rx);
        prune_closed_waiters(&self.waiters, &task_id).await;
        self.cancel_abandoned(task_id, "MapReduce wait timed out")
            .await;
        Err(GridError::Timeout {
            operation: format!("MapReduce task {task_id}"),
            duration_secs: timeout.as_secs(),
        })
    }

    /// Cancels `task_id` (telling its worker when the engine knows the registry) and removes its
    /// waiter entry.
    async fn cancel_abandoned(&self, task_id: rusty_grid_core::task::TaskId, reason: &str) {
        #[cfg(test)]
        self.cancel_abandoned_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        prune_closed_waiters(&self.waiters, &task_id).await;
        match &self.registry {
            Some(registry) => {
                if let Err(e) = crate::server::cancel_task_and_notify(
                    registry,
                    &self.queue,
                    &self.scheduler_notify,
                    &self.waiters,
                    task_id,
                    reason,
                )
                .await
                {
                    warn!(task_id = %task_id, error = %e, "Could not cancel abandoned MapReduce task");
                }
            }
            None => {
                let _ = self
                    .queue
                    .cancel_task(&task_id, Some(reason.to_string()))
                    .await;
            }
        }
    }
}

/// A failed job: the counters reached so far plus the reason.
fn failed_result(
    spec: &MapReduceJobSpec,
    start_time: Instant,
    (map_tasks_total, map_tasks_completed): (usize, usize),
    (reduce_tasks_total, reduce_tasks_completed): (usize, usize),
    error: String,
) -> MapReduceResult {
    MapReduceResult {
        job_id: spec.job_id,
        status: "Failed".into(),
        output: HashMap::new(),
        map_tasks_total,
        map_tasks_completed,
        reduce_tasks_total,
        reduce_tasks_completed,
        execution_time_ms: start_time.elapsed().as_millis() as u64,
        error: Some(error),
    }
}

fn task_failure_detail(result: &TaskResult, uses_sh: bool) -> String {
    let stderr = result.stderr_str();
    let detail = if !stderr.trim().is_empty() {
        stderr.trim()
    } else {
        result.error.as_deref().unwrap_or("unknown error")
    };
    let is_sh_spawn_error = detail.contains("Failed to spawn process 'sh'")
        || detail.contains("Failed to spawn command 'sh'")
        || result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("Process spawn error"));
    let lower_detail = detail.to_ascii_lowercase();
    let lower_err = result
        .error
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let is_not_found = lower_detail.contains("not found")
        || lower_detail.contains("no such file")
        || lower_detail.contains("cannot find the file")
        || lower_detail.contains("os error 2")
        || lower_err.contains("not found")
        || lower_err.contains("no such file")
        || lower_err.contains("cannot find the file")
        || lower_err.contains("os error 2");
    if uses_sh && is_sh_spawn_error && is_not_found {
        format!(
            "{detail} (worker does not have 'sh' on PATH; install a POSIX shell such as Git for Windows or pass --map-cmd / --reduce-cmd)"
        )
    } else {
        detail.to_string()
    }
}

/// Shell script of a built-in mapper operator.
fn builtin_map_script(operator: &str) -> GridResult<&'static str> {
    Ok(match operator {
        "word_count" => {
            r#"while IFS= read -r line || [ -n "$line" ]; do
  for word in $line; do
    printf "%s\t1\n" "$word"
  done
done"#
        }
        "line_count" => {
            r#"count=0
while IFS= read -r line || [ -n "$line" ]; do
  count=$((count + 1))
done
printf "lines\t%d\n" "$count""#
        }
        "identity" => {
            r#"while IFS= read -r line || [ -n "$line" ]; do
  printf "%s\t1\n" "$line"
done"#
        }
        other => {
            return Err(GridError::Config(format!(
                "Unsupported builtin mapper operator: {other}"
            )));
        }
    })
}

/// Task spec of one map partition (`script` is the built-in operator's script, if any).
fn map_task_spec(mapper: &MapFunctionSpec, script: Option<&str>, chunk_input: String) -> TaskSpec {
    let stdin = Some(chunk_input.into_bytes());
    match mapper {
        MapFunctionSpec::Builtin { .. } => TaskSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script.unwrap_or_default().into()],
            env: HashMap::new(),
            working_dir: None,
            stdin,
        },
        MapFunctionSpec::ShellScript { script } => TaskSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script.clone()],
            env: HashMap::new(),
            working_dir: None,
            stdin,
        },
        MapFunctionSpec::Command { program, args } => TaskSpec::Command {
            program: program.clone(),
            args: args.clone(),
            env: HashMap::new(),
            working_dir: None,
            stdin,
        },
    }
}

/// Task spec of one custom reduce task (the key and its values go in as JSON on stdin).
fn reduce_task_spec(
    reducer: &ReduceFunctionSpec,
    key: &str,
    values: &[serde_json::Value],
) -> GridResult<TaskSpec> {
    let (program, args) = match reducer {
        ReduceFunctionSpec::ShellScript { script } => {
            ("sh".to_string(), vec!["-c".to_string(), script.clone()])
        }
        ReduceFunctionSpec::Command { program, args } => (program.clone(), args.clone()),
        ReduceFunctionSpec::Builtin { operator } => {
            return Err(GridError::Config(format!(
                "Builtin reducer '{operator}' runs on the master and has no task"
            )));
        }
    };
    let grouped_kv = GroupedKeyValue {
        key: key.to_string(),
        values: values.to_vec(),
    };
    Ok(TaskSpec::Command {
        program,
        args,
        env: HashMap::new(),
        working_dir: None,
        stdin: Some(serde_json::to_vec(&grouped_kv)?),
    })
}

/// Parses mapper output lines into key-value pairs.
fn parse_map_output(result: &TaskResult, intermediate_kvs: &mut Vec<(String, serde_json::Value)>) {
    for line in result.stdout_str().lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(obj) = val.as_object() {
                if let (Some(k), Some(v)) = (obj.get("key"), obj.get("value")) {
                    intermediate_kvs.push((k.as_str().unwrap_or("").to_string(), v.clone()));
                    continue;
                }
            }
        }
        // Fallback: tab or space delimiter
        if let Some((k, v)) = trimmed.split_once('\t') {
            let json_val = serde_json::from_str(v)
                .unwrap_or_else(|_| serde_json::Value::String(v.to_string()));
            intermediate_kvs.push((k.to_string(), json_val));
        } else if let Some((k, v)) = trimmed.split_once(' ') {
            let json_val = serde_json::from_str(v)
                .unwrap_or_else(|_| serde_json::Value::String(v.to_string()));
            intermediate_kvs.push((k.to_string(), json_val));
        } else {
            intermediate_kvs.push((trimmed.to_string(), serde_json::json!(1)));
        }
    }
}

fn validate_builtin_reducer(operator: &str) -> GridResult<()> {
    match operator {
        "sum" | "count" | "max" | "min" | "concat" => Ok(()),
        other => Err(GridError::Config(format!(
            "Unsupported builtin reducer operator: {other}"
        ))),
    }
}

/// Applies a built-in reducer to the values of one key.
/// Returns `Ok(None)` when every value of the key is skipped (no usable finite numeric value
/// for `"sum"`, `"max"`, or `"min"`), so the key is omitted from the output rather than
/// reporting a bogus default (`0`, `i64::MIN`, `i64::MAX`, or `null`).
fn reduce_builtin(
    operator: &str,
    values: &[serde_json::Value],
) -> GridResult<Option<serde_json::Value>> {
    Ok(match operator {
        "sum" => {
            let mut sum_f = 0.0;
            let mut is_float = false;
            let mut sum_i: i64 = 0;
            let mut has_usable = false;
            for val in values {
                if let Some(n) = val.as_i64() {
                    sum_i = sum_i.saturating_add(n);
                    has_usable = true;
                } else if let Some(f) = val.as_f64().filter(|f| f.is_finite()) {
                    is_float = true;
                    sum_f += f;
                    has_usable = true;
                } else if let Some(s) = val.as_str() {
                    if let Ok(n) = s.parse::<i64>() {
                        sum_i = sum_i.saturating_add(n);
                        has_usable = true;
                    } else if let Ok(f) = s.parse::<f64>() {
                        if f.is_finite() {
                            is_float = true;
                            sum_f += f;
                            has_usable = true;
                        }
                    }
                }
            }
            if !has_usable {
                None
            } else if is_float {
                let total = sum_f + sum_i as f64;
                if total.is_finite() {
                    Some(serde_json::json!(total))
                } else {
                    None
                }
            } else {
                Some(serde_json::json!(sum_i))
            }
        }
        "count" => Some(serde_json::json!(values.len())),
        "max" => {
            let mut max_i: Option<i64> = None;
            let mut max_f: Option<f64> = None;
            let mut is_float = false;
            for val in values {
                if let Some(n) = val.as_i64() {
                    max_i = Some(max_i.map_or(n, |m| m.max(n)));
                    let f = n as f64;
                    max_f = Some(max_f.map_or(f, |m| m.max(f)));
                } else if let Some(f) = val.as_f64().filter(|f| f.is_finite()) {
                    is_float = true;
                    max_f = Some(max_f.map_or(f, |m| m.max(f)));
                } else if let Some(s) = val.as_str() {
                    if let Ok(n) = s.parse::<i64>() {
                        max_i = Some(max_i.map_or(n, |m| m.max(n)));
                        let f = n as f64;
                        max_f = Some(max_f.map_or(f, |m| m.max(f)));
                    } else if let Ok(f) = s.parse::<f64>() {
                        if f.is_finite() {
                            is_float = true;
                            max_f = Some(max_f.map_or(f, |m| m.max(f)));
                        }
                    }
                }
            }
            if is_float {
                max_f.map(|v| serde_json::json!(v))
            } else {
                max_i.map(|v| serde_json::json!(v))
            }
        }
        "min" => {
            let mut min_i: Option<i64> = None;
            let mut min_f: Option<f64> = None;
            let mut is_float = false;
            for val in values {
                if let Some(n) = val.as_i64() {
                    min_i = Some(min_i.map_or(n, |m| m.min(n)));
                    let f = n as f64;
                    min_f = Some(min_f.map_or(f, |m| m.min(f)));
                } else if let Some(f) = val.as_f64().filter(|f| f.is_finite()) {
                    is_float = true;
                    min_f = Some(min_f.map_or(f, |m| m.min(f)));
                } else if let Some(s) = val.as_str() {
                    if let Ok(n) = s.parse::<i64>() {
                        min_i = Some(min_i.map_or(n, |m| m.min(n)));
                        let f = n as f64;
                        min_f = Some(min_f.map_or(f, |m| m.min(f)));
                    } else if let Ok(f) = s.parse::<f64>() {
                        if f.is_finite() {
                            is_float = true;
                            min_f = Some(min_f.map_or(f, |m| m.min(f)));
                        }
                    }
                }
            }
            if is_float {
                min_f.map(|v| serde_json::json!(v))
            } else {
                min_i.map(|v| serde_json::json!(v))
            }
        }
        "concat" => {
            let parts: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            Some(serde_json::json!(parts.join(",")))
        }
        other => {
            return Err(GridError::Config(format!(
                "Unsupported builtin reducer operator: {other}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
    use std::sync::Arc;
    use tokio::sync::{Notify, RwLock};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_mapreduce_empty_input() {
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine = MapReduceEngine::new(queue, notify, waiters);

        let spec = MapReduceJobSpec::new(
            "test_empty",
            vec![],
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            ReduceFunctionSpec::Builtin {
                operator: "sum".into(),
            },
            2,
            1,
            30,
        );

        let res = engine
            .execute(spec)
            .await
            .expect("empty mapreduce succeeds");
        assert_eq!(res.status, "Completed");
        assert_eq!(res.map_tasks_total, 0);
        assert_eq!(res.reduce_tasks_total, 0);
        assert!(res.output.is_empty());
    }

    #[tokio::test]
    async fn test_mapreduce_word_count_execution() {
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine = MapReduceEngine::new(queue.clone(), notify.clone(), waiters.clone());

        // Background mock worker answering map and reduce tasks
        let q_clone = queue.clone();
        let w_clone = Arc::clone(&waiters);
        let n_clone = Arc::clone(&notify);
        let mock_worker = tokio::spawn(async move {
            for _ in 0..10 {
                n_clone.notified().await;
                let ready = q_clone.get_schedulable_tasks().await;
                for t in ready {
                    let task_id = t.id;
                    let _ = q_clone.mark_running(&task_id, Uuid::new_v4()).await;

                    // Produce simulated output based on task stdin
                    let stdout = if let TaskSpec::Command {
                        stdin: Some(bytes), ..
                    } = &t.spec
                    {
                        let text = String::from_utf8_lossy(bytes);
                        let mut out = String::new();
                        for word in text.split_whitespace() {
                            out.push_str(&format!("{word}\t1\n"));
                        }
                        out
                    } else {
                        "test\t1\n".into()
                    };

                    let res = TaskResult::success(Uuid::new_v4(), task_id, stdout, 10, false);
                    let _ = q_clone.record_result(res.clone()).await;
                    let mut lock = w_clone.write().await;
                    if let Some(senders) = lock.remove(&task_id) {
                        for tx in senders {
                            let _ = tx.send(res.clone());
                        }
                    }
                }
            }
        });

        let spec = MapReduceJobSpec::new(
            "test_wc",
            vec!["apple banana apple".into(), "banana orange apple".into()],
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            ReduceFunctionSpec::Builtin {
                operator: "sum".into(),
            },
            2,
            1,
            10,
        );

        let res = engine.execute(spec).await.expect("mapreduce execution");
        assert_eq!(res.status, "Completed");
        assert_eq!(res.output.get("apple").and_then(|v| v.as_i64()), Some(3));
        assert_eq!(res.output.get("banana").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(res.output.get("orange").and_then(|v| v.as_i64()), Some(1));

        mock_worker.abort();
    }

    #[tokio::test]
    async fn test_timed_out_task_is_cancelled_and_leaves_no_waiter_behind() {
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine = MapReduceEngine::new(queue.clone(), notify, waiters.clone());

        let task = Task::new(
            TaskSpec::command("echo", vec!["abandoned".into()]),
            TaskRequirements::generic(1, 60),
        );
        let task_id = task.id;
        // No worker exists, so nothing will ever run it and the wait must time out.
        let err = engine
            .submit_and_await(task, std::time::Duration::from_millis(50), None)
            .await
            .unwrap_err();
        assert!(matches!(err, GridError::Timeout { .. }), "{err}");

        assert!(
            waiters.read().await.get(&task_id).is_none(),
            "the timed-out wait must not leave its sender behind"
        );
        assert_eq!(
            queue.get_state(&task_id).await,
            Some(crate::queue::TaskState::Cancelled),
            "an abandoned task must not stay queued to run later for nobody"
        );
        assert!(queue.get_schedulable_tasks().await.is_empty());
    }

    // --- Step B: concurrent dispatch, bounded parallelism, loud failure on cut-off output ---

    /// What a mock worker answers one task with: `(exit_code, stdout)`.
    type Answer = (i32, String);

    /// A fake grid worker for engine tests. Polls the queue, holds every answer back until `gate`
    /// accepts the batch of queued tasks it can see, then answers each task for which `answer`
    /// returns a result (tasks it returns `None` for stay queued).
    fn spawn_mock_worker(
        queue: TaskQueue,
        waiters: WaiterMap,
        gate: impl Fn(&[Task]) -> bool + Send + 'static,
        answer: impl Fn(&Task) -> Option<Answer> + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let worker = Uuid::new_v4();
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let ready = queue.get_schedulable_tasks().await;
                if ready.is_empty() || !gate(&ready) {
                    continue;
                }
                for task in ready {
                    let Some((exit_code, stdout)) = answer(&task) else {
                        continue;
                    };
                    let task_id = task.id;
                    // Assign it for real (Queued -> Scheduled on this worker's session) so that the
                    // queue's bookkeeping, not just the waiter, sees it leave the queue.
                    if queue
                        .schedule_tasks_batch_in_sessions(&[(task_id, worker, 1)])
                        .await
                        .is_empty()
                    {
                        continue; // cancelled meanwhile
                    }
                    let result = if exit_code == 0 {
                        TaskResult::success(worker, task_id, stdout, 1, false)
                    } else {
                        TaskResult::failure(worker, task_id, exit_code, "", "boom", 1, None)
                    };
                    let _ = queue
                        .record_result_from(worker, Some(1), result.clone())
                        .await;
                    if let Some(senders) = waiters.write().await.remove(&task_id) {
                        for tx in senders {
                            let _ = tx.send(result.clone());
                        }
                    }
                }
            }
        })
    }

    /// Word-count mapper answer: `word\t1` per word of the task's stdin.
    fn word_count_answer(task: &Task) -> Option<Answer> {
        let TaskSpec::Command {
            stdin: Some(bytes), ..
        } = &task.spec
        else {
            return None;
        };
        let text = String::from_utf8_lossy(bytes);
        if text.trim_start().starts_with('{') {
            // A reduce task: stdin is {"key":..,"values":[..]}; answer with the number of values.
            let parsed: serde_json::Value = serde_json::from_str(&text).ok()?;
            let n = parsed["values"].as_array().map_or(0, Vec::len);
            return Some((0, n.to_string()));
        }
        let out: String = text
            .split_whitespace()
            .map(|word| format!("{word}\t1\n"))
            .collect();
        Some((0, out))
    }

    fn engine_fixture() -> (MapReduceEngine, TaskQueue, WaiterMap) {
        let queue = TaskQueue::new();
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine =
            MapReduceEngine::new(queue.clone(), Arc::new(Notify::new()), Arc::clone(&waiters));
        (engine, queue, waiters)
    }

    fn word_job(
        lines: Vec<String>,
        partitions: usize,
        reducer: ReduceFunctionSpec,
    ) -> MapReduceJobSpec {
        MapReduceJobSpec::new(
            "step_b",
            lines,
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            reducer,
            partitions,
            1,
            60,
        )
    }

    fn sum_reducer() -> ReduceFunctionSpec {
        ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        }
    }

    #[tokio::test]
    async fn test_all_map_partitions_are_submitted_before_any_finishes() {
        let (engine, queue, waiters) = engine_fixture();
        // The worker refuses to answer until it can see all four partitions queued together: a
        // serial engine (submit one, wait for it) never shows it more than one and would hang.
        let worker = spawn_mock_worker(
            queue.clone(),
            waiters,
            |ready| ready.len() >= 4,
            word_count_answer,
        );

        let job = word_job(
            vec!["a b".into(), "b c".into(), "c d".into(), "d a".into()],
            4,
            sum_reducer(),
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), engine.execute(job))
            .await
            .expect("map partitions must run concurrently, not one after another")
            .unwrap();
        worker.abort();

        assert_eq!(result.status, "Completed");
        assert_eq!(result.map_tasks_total, 4);
        assert_eq!(result.map_tasks_completed, 4);
        for (word, expected) in [("a", 2), ("b", 2), ("c", 2), ("d", 2)] {
            assert_eq!(result.output[word].as_i64(), Some(expected), "{word}");
        }
    }

    #[tokio::test]
    async fn test_custom_reducers_run_concurrently_one_task_per_key() {
        let (engine, queue, waiters) = engine_fixture();
        // Maps (2 partitions) go out together; the 5 reduce tasks must also be queued together.
        let worker = spawn_mock_worker(
            queue.clone(),
            waiters,
            |ready| {
                let is_reduce = matches!(
                    &ready[0].spec,
                    TaskSpec::Command { stdin: Some(b), .. } if b.starts_with(b"{")
                );
                ready.len() >= if is_reduce { 5 } else { 2 }
            },
            word_count_answer,
        );

        let job = word_job(
            vec!["a b c".into(), "d e a".into()],
            2,
            ReduceFunctionSpec::ShellScript {
                script: "cat".into(),
            },
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), engine.execute(job))
            .await
            .expect("reduce tasks must run concurrently, not one after another")
            .unwrap();
        worker.abort();

        assert_eq!(result.status, "Completed");
        assert_eq!(result.reduce_tasks_total, 5);
        assert_eq!(result.reduce_tasks_completed, 5);
        assert_eq!(result.output["a"].as_i64(), Some(2));
        assert_eq!(result.output["e"].as_i64(), Some(1));
    }

    #[tokio::test]
    async fn test_in_flight_never_exceeds_parallelism() {
        let (engine, queue, waiters) = engine_fixture();
        let engine = engine.with_parallelism(3);
        let observed_max = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&observed_max);
        let worker = spawn_mock_worker(
            queue.clone(),
            waiters,
            move |ready| {
                // The mock answers everything it sees at once, so what it sees queued together is
                // exactly what the engine had in flight.
                observed.fetch_max(ready.len(), std::sync::atomic::Ordering::SeqCst);
                true
            },
            word_count_answer,
        );

        let lines: Vec<String> = (0..20).map(|i| format!("w{i}")).collect();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            engine.execute(word_job(lines, 20, sum_reducer())),
        )
        .await
        .expect("job must finish")
        .unwrap();
        worker.abort();

        assert_eq!(result.status, "Completed");
        assert!(
            observed_max.load(std::sync::atomic::Ordering::SeqCst) <= 3,
            "at most `parallelism` tasks of the job may be queued at once, saw {}",
            observed_max.load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_a_mapper_whose_output_was_cut_off_fails_the_job_loudly() {
        let (engine, queue, waiters) = engine_fixture();
        let marker = OUTPUT_TRUNCATED_MARKER;
        let worker = spawn_mock_worker(
            queue.clone(),
            waiters,
            |_| true,
            move |task| {
                let (code, mut out) = word_count_answer(task)?;
                // The partition containing "greedy" hit the worker's capture limit: exit 0 + marker.
                if let TaskSpec::Command { stdin: Some(b), .. } = &task.spec {
                    if String::from_utf8_lossy(b).contains("greedy") {
                        out.push_str(&format!("\nappl\n{marker}\n"));
                    }
                }
                Some((code, out))
            },
        );

        let job = word_job(
            vec!["fine words".into(), "greedy words".into()],
            2,
            sum_reducer(),
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), engine.execute(job))
            .await
            .unwrap()
            .unwrap();
        worker.abort();

        assert_eq!(
            result.status, "Failed",
            "a truncated mapper must not report Completed"
        );
        let error = result.error.expect("failure reason");
        assert!(error.contains("truncated"), "{error}");
        assert!(result.output.is_empty(), "no partial counts, no bogus keys");
    }

    #[test]
    fn test_truncation_marker_matches_what_the_real_worker_appends() {
        // The detector is only as good as its match with `rusty_grid_worker::read_bounded_stream`.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let data = [b'x'; 100];
        let (cut, truncated) = rt.block_on(rusty_grid_worker::read_bounded_stream(&data[..], 10));
        assert!(truncated);
        let base = TaskResult::success(
            Uuid::new_v4(),
            rusty_grid_core::task::TaskId::new(),
            "",
            1,
            false,
        );
        let result = TaskResult {
            stdout: cut,
            ..base.clone()
        };
        assert!(stdout_was_truncated(&result));

        let (whole, truncated) =
            rt.block_on(rusty_grid_worker::read_bounded_stream(&data[..], 1000));
        assert!(!truncated);
        let result = TaskResult {
            stdout: whole,
            ..base.clone()
        };
        assert!(!stdout_was_truncated(&result));

        // The text appearing early in a large honest output is not a truncation.
        let mut honest = format!("{OUTPUT_TRUNCATED_MARKER}\n").into_bytes();
        honest.extend(vec![b'y'; 4096]);
        let result = TaskResult {
            stdout: honest.into(),
            ..base
        };
        assert!(!stdout_was_truncated(&result));
    }

    #[tokio::test]
    async fn test_first_failure_fails_the_job_and_cancels_the_partitions_still_in_flight() {
        let (engine, queue, waiters) = engine_fixture();
        // Only the partition containing "boom" is ever answered (with a failure); the other two
        // would wait for ever.
        let worker = spawn_mock_worker(
            queue.clone(),
            waiters,
            |_| true,
            |task| {
                let TaskSpec::Command { stdin: Some(b), .. } = &task.spec else {
                    return None;
                };
                String::from_utf8_lossy(b)
                    .contains("boom")
                    .then(|| (1, String::new()))
            },
        );

        let job = word_job(
            vec!["ok one".into(), "ok two".into(), "boom three".into()],
            3,
            sum_reducer(),
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), engine.execute(job))
            .await
            .expect("a failed partition must end the job at once")
            .unwrap();
        worker.abort();

        assert_eq!(result.status, "Failed");
        assert!(result.error.unwrap().contains("Map partition"));

        // The two unanswered partitions are cancelled, not left queued to run for nobody.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let stats = queue.stats().await;
            if stats.cancelled == 2 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "abandoned partitions must be cancelled, stats: {stats:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(queue.get_schedulable_tasks().await.is_empty());
    }

    #[tokio::test]
    async fn test_unsupported_operators_fail_before_anything_is_submitted() {
        let (engine, queue, _waiters) = engine_fixture();
        let bad_mapper = MapReduceJobSpec::new(
            "bad_mapper",
            vec!["x".into()],
            MapFunctionSpec::Builtin {
                operator: "nope".into(),
            },
            sum_reducer(),
            1,
            1,
            60,
        );
        assert!(matches!(
            engine.execute(bad_mapper).await,
            Err(GridError::Config(_))
        ));

        let bad_reducer = word_job(
            vec!["x".into()],
            1,
            ReduceFunctionSpec::Builtin {
                operator: "median".into(),
            },
        );
        assert!(matches!(
            engine.execute(bad_reducer).await,
            Err(GridError::Config(_))
        ));
        assert_eq!(
            queue.stats().await.total,
            0,
            "a job that cannot run must not leave map tasks behind"
        );
    }

    #[tokio::test]
    async fn test_wait_only_times_out_when_the_whole_job_stalls() {
        let (engine, _queue, waiters) = engine_fixture();
        let task = Task::new(
            TaskSpec::command("echo", vec!["late".into()]),
            TaskRequirements::generic(1, 60),
        );
        let task_id = task.id;
        let progress = Arc::new(JobProgress::new());

        // Other tasks of the job keep finishing every 50 ms (progress), while this one queues for
        // 600 ms: far longer than its 300 ms timeout.
        let ticker = {
            let progress = Arc::clone(&progress);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    progress.touch();
                }
            })
        };
        let completer = {
            let waiters = Arc::clone(&waiters);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(600)).await;
                let result = TaskResult::success(Uuid::new_v4(), task_id, "done", 1, false);
                if let Some(senders) = waiters.write().await.remove(&task_id) {
                    for tx in senders {
                        let _ = tx.send(result.clone());
                    }
                }
            })
        };

        let outcome = engine
            .submit_and_await(task, std::time::Duration::from_millis(300), Some(&progress))
            .await;
        ticker.abort();
        completer.abort();
        assert!(
            outcome.is_ok(),
            "job-level progress keeps a queued task's wait alive: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn test_mapreduce_job_waits_for_queue_room_and_completes_when_room_is_freed() {
        let queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            None,
            crate::queue::DEFAULT_MAX_TERMINAL_TASKS,
            crate::queue::DEFAULT_MAX_TERMINAL_BYTES,
            4_000,
        );
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine =
            MapReduceEngine::new(queue.clone(), Arc::new(Notify::new()), Arc::clone(&waiters));

        // 1. A task that alone exceeds the 4_000-byte cap fails at once without waiting.
        let impossible_job = MapReduceJobSpec::new(
            "too_large_alone",
            vec!["x".repeat(5_000)],
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            sum_reducer(),
            1,
            1,
            10,
        );
        let started = Instant::now();
        let err = engine.execute(impossible_job).await.unwrap_err();
        assert!(
            matches!(err, GridError::QueueByteLimitExceeded { .. }),
            "expected immediate QueueByteLimitExceeded when task alone exceeds cap, got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "must fail at once without waiting for timeout"
        );

        // 2. Fill the 4_000-byte cap with a blocker task, start the Map/Reduce job, then free
        //    the blocker after 100 ms and verify the job completes.
        let blocker = Task::new(
            TaskSpec::Command {
                program: "sleep".into(),
                args: vec!["10".into()],
                env: Default::default(),
                working_dir: None,
                stdin: Some(vec![b'b'; 2_500]),
            },
            TaskRequirements::generic(1, 60),
        );
        let blocker_id = queue.submit(blocker).await.unwrap();

        let worker = spawn_mock_worker(
            queue.clone(),
            Arc::clone(&waiters),
            |_| true,
            move |task| {
                if task.id == blocker_id {
                    return None;
                }
                word_count_answer(task)
            },
        );

        let releaser = {
            let queue = queue.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                queue
                    .cancel_task(&blocker_id, Some("free room".into()))
                    .await
                    .unwrap();
            })
        };

        let job = word_job(
            vec!["alpha beta".into(), "beta gamma".into()],
            2,
            sum_reducer(),
        );
        let result = tokio::time::timeout(Duration::from_secs(5), engine.execute(job))
            .await
            .expect("job completes after room is freed")
            .unwrap();
        releaser.abort();
        worker.abort();

        assert_eq!(result.status, "Completed");
        assert_eq!(result.output["alpha"].as_i64(), Some(1));
        assert_eq!(result.output["beta"].as_i64(), Some(2));
        assert_eq!(result.output["gamma"].as_i64(), Some(1));

        // 3. Task-count capacity: `capacity: Some(0)` fails immediately with `QueueFull { capacity: 0 }`
        //    (M2b), while `capacity: Some(1)` waits for a queued blocker to finish (M2c).
        let zero_cap_queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            Some(0),
            crate::queue::DEFAULT_MAX_TERMINAL_TASKS,
            crate::queue::DEFAULT_MAX_TERMINAL_BYTES,
            crate::queue::DEFAULT_MAX_ACTIVE_BYTES,
        );
        let zero_engine = MapReduceEngine::new(
            zero_cap_queue,
            Arc::new(Notify::new()),
            Arc::new(RwLock::new(HashMap::new())),
        );
        let zero_err = zero_engine
            .execute(word_job(vec!["alpha".into()], 1, sum_reducer()))
            .await
            .unwrap_err();
        assert!(
            matches!(zero_err, GridError::QueueFull { capacity: 0 }),
            "expected immediate QueueFull {{ capacity: 0 }}, got {zero_err:?}"
        );

        let one_cap_queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            Some(1),
            crate::queue::DEFAULT_MAX_TERMINAL_TASKS,
            crate::queue::DEFAULT_MAX_TERMINAL_BYTES,
            crate::queue::DEFAULT_MAX_ACTIVE_BYTES,
        );
        let one_waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let one_engine = MapReduceEngine::new(
            one_cap_queue.clone(),
            Arc::new(Notify::new()),
            Arc::clone(&one_waiters),
        );
        let cap_blocker = Task::new(
            TaskSpec::Command {
                program: "sleep".into(),
                args: vec!["10".into()],
                env: Default::default(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 60),
        );
        let cap_blocker_id = one_cap_queue.submit(cap_blocker).await.unwrap();
        let one_worker = spawn_mock_worker(
            one_cap_queue.clone(),
            Arc::clone(&one_waiters),
            |_| true,
            move |task| {
                if task.id == cap_blocker_id {
                    return None;
                }
                word_count_answer(task)
            },
        );
        let one_releaser = {
            let q = one_cap_queue.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(80)).await;
                q.cancel_task(&cap_blocker_id, Some("free slot".into()))
                    .await
                    .unwrap();
            })
        };
        let one_res = tokio::time::timeout(
            Duration::from_secs(5),
            one_engine.execute(word_job(vec!["alpha beta".into()], 1, sum_reducer())),
        )
        .await
        .expect("job completes after capacity slot is freed")
        .unwrap();
        one_releaser.abort();
        one_worker.abort();
        assert_eq!(one_res.status, "Completed");
        assert_eq!(one_res.output["alpha"].as_i64(), Some(1));
    }

    #[tokio::test]
    async fn test_mapreduce_job_times_out_when_queue_stays_full_and_reports_why() {
        let queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            None,
            crate::queue::DEFAULT_MAX_TERMINAL_TASKS,
            crate::queue::DEFAULT_MAX_TERMINAL_BYTES,
            4_000,
        );
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine =
            MapReduceEngine::new(queue.clone(), Arc::new(Notify::new()), Arc::clone(&waiters));

        // Keep the queue full for the entire 1-second job timeout.
        let blocker = Task::new(
            TaskSpec::Command {
                program: "sleep".into(),
                args: vec!["60".into()],
                env: Default::default(),
                working_dir: None,
                stdin: Some(vec![b'b'; 2_500]),
            },
            TaskRequirements::generic(1, 60),
        );
        let _blocker_id = queue.submit(blocker).await.unwrap();

        let job = MapReduceJobSpec::new(
            "full_queue_timeout",
            vec!["hello world".into()],
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            sum_reducer(),
            1,
            1,
            1,
        );
        let err = tokio::time::timeout(Duration::from_secs(5), engine.execute(job))
            .await
            .expect("job ends at its 1s timeout")
            .unwrap_err();
        let msg = err.to_string();
        assert!(matches!(err, GridError::Timeout { .. }), "got {err:?}");
        assert!(
            msg.contains("waiting for queue room") && msg.contains("--max-active-mb"),
            "timeout message must explain why the task could not be queued: {msg}"
        );
    }

    #[tokio::test]
    async fn test_mapreduce_resets_execution_clock_after_queue_wait() {
        // Item 1: `own_start` must be reset after `try_submit_slot` succeeds so that time
        // spent waiting for queue room does not consume the partition's execution timeout.
        let queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            Some(1),
            crate::queue::DEFAULT_MAX_TERMINAL_TASKS,
            crate::queue::DEFAULT_MAX_TERMINAL_BYTES,
            crate::queue::DEFAULT_MAX_ACTIVE_BYTES,
        );
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine =
            MapReduceEngine::new(queue.clone(), Arc::new(Notify::new()), Arc::clone(&waiters));

        // Fill the single queue slot for 700 ms.
        let blocker = Task::new(
            TaskSpec::command("sleep", vec!["10".into()]),
            TaskRequirements::generic(1, 60),
        );
        let blocker_id = queue.submit(blocker).await.unwrap();
        let q_free = queue.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            q_free
                .cancel_task(&blocker_id, Some("free room".into()))
                .await
                .unwrap();
        });

        // Partition task has a 1000 ms execution timeout (`own_timeout`), while the job-level
        // timeout is 3000 ms. Once admitted after ~700 ms of waiting, the worker takes 500 ms
        // to produce the result (~1200 ms total wall time > 1000 ms execution timeout).
        let part_task = Task::new(
            TaskSpec::command("echo", vec!["hello".into()]),
            TaskRequirements::generic(1, 1),
        );
        let part_id = part_task.id;
        let q_worker = queue.clone();
        let w_worker = Arc::clone(&waiters);
        tokio::spawn(async move {
            let worker_id = Uuid::new_v4();
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let ready = q_worker.get_schedulable_tasks().await;
                if ready.iter().any(|t| t.id == part_id) {
                    // Simulate 500 ms execution time after queue admission.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let res = TaskResult::success(worker_id, part_id, "ok", 500, false);
                    let _ = q_worker.record_result(res.clone()).await;
                    if let Some(senders) = w_worker.write().await.remove(&part_id) {
                        for tx in senders {
                            let _ = tx.send(res.clone());
                        }
                    }
                    break;
                }
            }
        });

        let progress = JobProgress::new();
        let result = engine
            .submit_and_await(part_task, Duration::from_millis(1000), Some(&progress))
            .await
            .expect("execution clock must reset after queue admission");
        assert_eq!(result.stdout_str(), "ok");
    }

    #[tokio::test]
    async fn test_mapreduce_try_submit_slot_moves_task_and_disarms_abandon_guard_while_waiting() {
        // Items 2 & 3:
        // - `try_submit_slot` checks room before taking the task out of `Option<Task>` and moves
        //   it into the queue on acceptance (`task_slot` becomes `None`).
        // - `AbandonGuard` is only armed after `try_submit_slot` succeeds; dropping a waiting
        //   `submit_and_await` future removes its waiter entry without calling `cancel_abandoned`.
        let queue = TaskQueue::with_retention_limits(
            crate::queue::RetryPolicy::default(),
            Some(1),
            crate::queue::DEFAULT_MAX_TERMINAL_TASKS,
            crate::queue::DEFAULT_MAX_TERMINAL_BYTES,
            crate::queue::DEFAULT_MAX_ACTIVE_BYTES,
        );
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let engine =
            MapReduceEngine::new(queue.clone(), Arc::new(Notify::new()), Arc::clone(&waiters));

        let blocker = Task::new(
            TaskSpec::command("sleep", vec!["10".into()]),
            TaskRequirements::generic(1, 60),
        );
        let blocker_id = queue.submit(blocker).await.unwrap();

        let waiting_task = Task::new(
            TaskSpec::command("echo", vec!["wait".into()]),
            TaskRequirements::generic(1, 60),
        );
        let waiting_id = waiting_task.id;

        // While the queue is full, `try_submit_slot` leaves `slot` intact (`Some`).
        let mut slot = Some(waiting_task.clone());
        let err = queue.try_submit_slot(&mut slot).await.unwrap_err();
        assert!(matches!(err, GridError::QueueFull { capacity: 1 }));
        assert!(
            slot.is_some(),
            "try_submit_slot must not take the task when the queue has no room"
        );

        // Start `submit_and_await` and abort it after 60 ms while it is still waiting for room:
        // the waiter entry must be cleaned up, the task must never have entered the queue, and
        // `cancel_abandoned` must NOT have been called (`armed == false`).
        let fut = engine.submit_and_await(waiting_task, Duration::from_secs(5), None);
        let _ = tokio::time::timeout(Duration::from_millis(60), fut).await;
        tokio::task::yield_now().await;
        assert!(
            waiters.read().await.get(&waiting_id).is_none(),
            "AbandonGuard must prune the waiter entry when dropped while waiting for room"
        );
        assert!(
            queue.get_state(&waiting_id).await.is_none(),
            "unsubmitted task must not be present in the queue"
        );
        assert_eq!(
            engine.cancel_abandoned_count(),
            0,
            "AbandonGuard must not call cancel_abandoned when dropped before submit succeeds"
        );

        // Once the blocker is cancelled, `try_submit_slot` moves `slot` into the queue (`None`).
        queue
            .cancel_task(&blocker_id, Some("free".into()))
            .await
            .unwrap();
        let submitted_id = queue.try_submit_slot(&mut slot).await.unwrap();
        assert_eq!(submitted_id, waiting_id);
        assert!(
            slot.is_none(),
            "try_submit_slot must take the task out of the caller's Option on acceptance"
        );
        queue
            .cancel_task(&submitted_id, Some("free again".into()))
            .await
            .unwrap();

        // Now that the queue has room, a `submit_and_await` that succeeds in submitting and is
        // then dropped while awaiting execution MUST arm `AbandonGuard` and call `cancel_abandoned`.
        let admitted_task = Task::new(
            TaskSpec::command("echo", vec!["admitted".into()]),
            TaskRequirements::generic(1, 60),
        );
        let admitted_id = admitted_task.id;
        let fut2 = engine.submit_and_await(admitted_task, Duration::from_secs(5), None);
        let _ = tokio::time::timeout(Duration::from_millis(40), fut2).await;
        tokio::task::yield_now().await;
        assert_eq!(
            engine.cancel_abandoned_count(),
            1,
            "AbandonGuard must call cancel_abandoned when dropped after submit succeeds"
        );
        assert_eq!(
            queue.get_state(&admitted_id).await,
            Some(crate::queue::TaskState::Cancelled)
        );
    }

    #[test]
    fn test_next_queue_wait_backoff_doubles_from_25ms_up_to_1s() {
        let mut b = Duration::from_millis(25);
        let mut observed = Vec::new();
        for _ in 0..8 {
            observed.push(b.as_millis());
            b = next_queue_wait_backoff(b);
        }
        assert_eq!(observed, vec![25, 50, 100, 200, 400, 800, 1000, 1000]);
    }

    #[test]
    fn test_builtin_reducer_omits_keys_whose_values_are_all_skipped() {
        let skipped = vec![
            serde_json::json!("inf"),
            serde_json::json!("-inf"),
            serde_json::json!("NaN"),
            serde_json::json!("1e999"),
            serde_json::json!("abc"),
        ];
        for op in ["sum", "max", "min"] {
            assert_eq!(
                reduce_builtin(op, &skipped).unwrap(),
                None,
                "builtin reducer '{op}' must return None when every value of a key is skipped"
            );
        }

        // A key with at least one usable value (including 0) keeps its reduced value.
        let with_zero = vec![serde_json::json!("inf"), serde_json::json!(0)];
        assert_eq!(
            reduce_builtin("sum", &with_zero).unwrap(),
            Some(serde_json::json!(0))
        );
        assert_eq!(
            reduce_builtin("max", &with_zero).unwrap(),
            Some(serde_json::json!(0))
        );
        assert_eq!(
            reduce_builtin("min", &with_zero).unwrap(),
            Some(serde_json::json!(0))
        );
    }
}
