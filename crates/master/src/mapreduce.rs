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
struct AbandonGuard {
    engine: MapReduceEngine,
    task_id: rusty_grid_core::task::TaskId,
    armed: bool,
}

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let engine = self.engine.clone();
        let task_id = self.task_id;
        runtime.spawn(async move {
            engine
                .cancel_abandoned(task_id, "MapReduce job abandoned")
                .await;
        });
    }
}

/// Lightweight in-memory Map/Reduce engine executing jobs across cluster workers.
#[derive(Clone)]
pub struct MapReduceEngine {
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    registry: Option<WorkerRegistry>,
    parallelism: usize,
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
        }
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
        let timeout = Duration::from_secs(spec.timeout_secs.max(60));
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
                        TaskRequirements::generic(1, spec.timeout_secs.max(60)),
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
                    return Ok(failed_result(
                        &spec,
                        start_time,
                        (map_tasks_total, map_tasks_completed),
                        (0, 0),
                        format!(
                            "Map partition {} failed with exit code {}: {}",
                            idx,
                            result.exit_code,
                            result.stderr_str()
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
                    final_output.insert(key, reduce_builtin(operator, &values)?);
                    reduce_tasks_completed += 1;
                }
            }
            ReduceFunctionSpec::ShellScript { .. } | ReduceFunctionSpec::Command { .. } => {
                let mut results = stream::iter(grouped)
                    .map(|(key, values)| {
                        let built =
                            reduce_task_spec(&spec.reducer, &key, &values).map(|task_spec| {
                                Task::new(
                                    task_spec,
                                    TaskRequirements::generic(1, spec.timeout_secs.max(60)),
                                )
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
                        return Ok(failed_result(
                            &spec,
                            start_time,
                            (map_tasks_total, map_tasks_completed),
                            (reduce_tasks_total, reduce_tasks_completed),
                            format!("Reducer for key '{key}' failed: {}", result.stderr_str()),
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
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.waiters.write().await;
            lock.entry(task_id).or_default().push(tx);
        }
        if let Err(e) = self.queue.submit(task).await {
            drop(rx);
            prune_closed_waiters(&self.waiters, &task_id).await;
            return Err(e);
        }
        self.scheduler_notify.notify_one();

        let mut guard = AbandonGuard {
            engine: self.clone(),
            task_id,
            armed: true,
        };
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
                    if let Some(p) = progress {
                        p.touch();
                    }
                    return Ok(result);
                }
                Ok(Err(_)) => {
                    guard.armed = false;
                    return Err(GridError::ConnectionClosed);
                }
                // Not due yet if another task of the job finished meanwhile: re-evaluate.
                Err(_) => continue,
            }
        }

        // Nobody is waiting any more: stop the abandoned task from sitting in the queue
        // (and later running for nobody) and drop its waiter entry.
        guard.armed = false;
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
fn reduce_builtin(operator: &str, values: &[serde_json::Value]) -> GridResult<serde_json::Value> {
    Ok(match operator {
        "sum" => {
            let mut sum_f = 0.0;
            let mut is_float = false;
            let mut sum_i: i64 = 0;
            for val in values {
                if let Some(n) = val.as_i64() {
                    sum_i += n;
                } else if let Some(f) = val.as_f64() {
                    is_float = true;
                    sum_f += f;
                } else if let Some(s) = val.as_str() {
                    if let Ok(n) = s.parse::<i64>() {
                        sum_i += n;
                    } else if let Ok(f) = s.parse::<f64>() {
                        is_float = true;
                        sum_f += f;
                    }
                }
            }
            if is_float {
                serde_json::json!(sum_f + sum_i as f64)
            } else {
                serde_json::json!(sum_i)
            }
        }
        "count" => serde_json::json!(values.len()),
        "max" => {
            let mut max_val = i64::MIN;
            for val in values {
                if let Some(n) = val.as_i64() {
                    max_val = max_val.max(n);
                }
            }
            serde_json::json!(max_val)
        }
        "min" => {
            let mut min_val = i64::MAX;
            for val in values {
                if let Some(n) = val.as_i64() {
                    min_val = min_val.min(n);
                }
            }
            serde_json::json!(min_val)
        }
        "concat" => {
            let parts: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            serde_json::json!(parts.join(","))
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
}
