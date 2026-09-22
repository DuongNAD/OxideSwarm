//! In-memory Map/Reduce distributed engine coordinating parallel mapper and reducer tasks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::mapreduce::{
    GroupedKeyValue, MapFunctionSpec, MapReduceJobSpec, MapReduceResult, ReduceFunctionSpec,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};

use crate::queue::TaskQueue;
use crate::server::WaiterMap;

/// Lightweight in-memory Map/Reduce engine executing jobs across cluster workers.
#[derive(Clone)]
pub struct MapReduceEngine {
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
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
        }
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

        // 1. Partition input data into chunks
        let num_partitions = spec.partition_count.max(1).min(spec.input_data.len());
        let mut chunks: Vec<Vec<String>> = vec![Vec::new(); num_partitions];
        for (i, line) in spec.input_data.iter().enumerate() {
            chunks[i % num_partitions].push(line.clone());
        }
        let chunks: Vec<Vec<String>> = chunks.into_iter().filter(|c| !c.is_empty()).collect();
        let map_tasks_total = chunks.len();

        // 2. Dispatch Map Tasks
        let mut intermediate_kvs: Vec<(String, serde_json::Value)> = Vec::new();
        let mut map_tasks_completed = 0;

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let chunk_input = chunk.join("\n");
            let task_spec = match &spec.mapper {
                MapFunctionSpec::Builtin { operator } => {
                    let script = match operator.as_str() {
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
                    };
                    TaskSpec::Command {
                        program: "sh".into(),
                        args: vec!["-c".into(), script.into()],
                        env: HashMap::new(),
                        working_dir: None,
                        stdin: Some(chunk_input.into_bytes()),
                    }
                }
                MapFunctionSpec::ShellScript { script } => TaskSpec::Command {
                    program: "sh".into(),
                    args: vec!["-c".into(), script.clone()],
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: Some(chunk_input.into_bytes()),
                },
                MapFunctionSpec::Command { program, args } => TaskSpec::Command {
                    program: program.clone(),
                    args: args.clone(),
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: Some(chunk_input.into_bytes()),
                },
            };

            let task = Task::new(
                task_spec,
                TaskRequirements::generic(1, spec.timeout_secs.max(60)),
            );

            // Submit task and await result
            let result = self.submit_and_await_task(task, spec.timeout_secs).await?;
            if result.exit_code != 0 {
                return Ok(MapReduceResult {
                    job_id: spec.job_id,
                    status: "Failed".into(),
                    output: HashMap::new(),
                    map_tasks_total,
                    map_tasks_completed,
                    reduce_tasks_total: 0,
                    reduce_tasks_completed: 0,
                    execution_time_ms: start_time.elapsed().as_millis() as u64,
                    error: Some(format!(
                        "Map partition {} failed with exit code {}: {}",
                        idx, result.exit_code, result.stderr
                    )),
                });
            }

            map_tasks_completed += 1;

            // Parse output lines into key-value pairs
            for line in result.stdout.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    if let Some(obj) = val.as_object() {
                        if let (Some(k), Some(v)) = (obj.get("key"), obj.get("value")) {
                            intermediate_kvs
                                .push((k.as_str().unwrap_or("").to_string(), v.clone()));
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

        // 3. Shuffle & Group by key
        let mut grouped: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for (k, v) in intermediate_kvs {
            grouped.entry(k).or_default().push(v);
        }

        let reduce_tasks_total = grouped.len();
        let mut reduce_tasks_completed = 0;
        let mut final_output: HashMap<String, serde_json::Value> = HashMap::new();

        // 4. Reduce Phase
        for (key, values) in grouped {
            let reduced_val = match &spec.reducer {
                ReduceFunctionSpec::Builtin { operator } => match operator.as_str() {
                    "sum" => {
                        let mut sum_f = 0.0;
                        let mut is_float = false;
                        let mut sum_i: i64 = 0;
                        for val in &values {
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
                        for val in &values {
                            if let Some(n) = val.as_i64() {
                                max_val = max_val.max(n);
                            }
                        }
                        serde_json::json!(max_val)
                    }
                    "min" => {
                        let mut min_val = i64::MAX;
                        for val in &values {
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
                },
                ReduceFunctionSpec::ShellScript { script } => {
                    let grouped_kv = GroupedKeyValue {
                        key: key.clone(),
                        values: values.clone(),
                    };
                    let stdin_bytes = serde_json::to_vec(&grouped_kv)?;
                    let task_spec = TaskSpec::Command {
                        program: "sh".into(),
                        args: vec!["-c".into(), script.clone()],
                        env: HashMap::new(),
                        working_dir: None,
                        stdin: Some(stdin_bytes),
                    };
                    let task = Task::new(
                        task_spec,
                        TaskRequirements::generic(1, spec.timeout_secs.max(60)),
                    );
                    let result = self.submit_and_await_task(task, spec.timeout_secs).await?;
                    if result.exit_code != 0 {
                        return Ok(MapReduceResult {
                            job_id: spec.job_id,
                            status: "Failed".into(),
                            output: HashMap::new(),
                            map_tasks_total,
                            map_tasks_completed,
                            reduce_tasks_total,
                            reduce_tasks_completed,
                            execution_time_ms: start_time.elapsed().as_millis() as u64,
                            error: Some(format!(
                                "Reducer for key '{key}' failed: {}",
                                result.stderr
                            )),
                        });
                    }
                    serde_json::from_str(result.stdout.trim()).unwrap_or_else(|_| {
                        serde_json::Value::String(result.stdout.trim().to_string())
                    })
                }
                ReduceFunctionSpec::Command { program, args } => {
                    let grouped_kv = GroupedKeyValue {
                        key: key.clone(),
                        values: values.clone(),
                    };
                    let stdin_bytes = serde_json::to_vec(&grouped_kv)?;
                    let task_spec = TaskSpec::Command {
                        program: program.clone(),
                        args: args.clone(),
                        env: HashMap::new(),
                        working_dir: None,
                        stdin: Some(stdin_bytes),
                    };
                    let task = Task::new(
                        task_spec,
                        TaskRequirements::generic(1, spec.timeout_secs.max(60)),
                    );
                    let result = self.submit_and_await_task(task, spec.timeout_secs).await?;
                    if result.exit_code != 0 {
                        return Ok(MapReduceResult {
                            job_id: spec.job_id,
                            status: "Failed".into(),
                            output: HashMap::new(),
                            map_tasks_total,
                            map_tasks_completed,
                            reduce_tasks_total,
                            reduce_tasks_completed,
                            execution_time_ms: start_time.elapsed().as_millis() as u64,
                            error: Some(format!(
                                "Reducer for key '{key}' failed: {}",
                                result.stderr
                            )),
                        });
                    }
                    serde_json::from_str(result.stdout.trim()).unwrap_or_else(|_| {
                        serde_json::Value::String(result.stdout.trim().to_string())
                    })
                }
            };

            reduce_tasks_completed += 1;
            final_output.insert(key, reduced_val);
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

    async fn submit_and_await_task(&self, task: Task, timeout_secs: u64) -> GridResult<TaskResult> {
        let task_id = task.id;
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.waiters.write().await;
            lock.entry(task_id).or_default().push(tx);
        }
        self.queue.submit(task).await?;
        self.scheduler_notify.notify_one();

        let timeout = std::time::Duration::from_secs(timeout_secs.max(60));
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(GridError::ConnectionClosed),
            Err(_) => Err(GridError::Timeout {
                operation: format!("MapReduce task {task_id}"),
                duration_secs: timeout.as_secs(),
            }),
        }
    }
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
}
