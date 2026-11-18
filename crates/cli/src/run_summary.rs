//! Run summary calculation and formatting for `submit --wait`, `--json`, and MCP results
//! (design 0059 D5, `ADOPT-2`).

use std::collections::BTreeSet;

use serde_json::{Map, Value};
use uuid::Uuid;

use rusty_grid_core::task::TaskResult;

/// Summary metrics for one waited CLI or MCP run, derived from client wait elapsed time and the
/// completed [`TaskResult`] values without changing the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunSummary {
    pub finished: usize,
    pub failed: usize,
    pub machines: usize,
    pub waited_ms: u64,
    pub task_time_sum_ms: u64,
}

impl RunSummary {
    /// Summarizes `results` together with client-measured wall time `waited_ms`.
    pub fn from_results(results: &[TaskResult], waited_ms: u64) -> Self {
        let mut finished = 0usize;
        let mut failed = 0usize;
        let mut task_time_sum_ms = 0u64;
        let mut distinct_workers: BTreeSet<Uuid> = BTreeSet::new();

        for result in results {
            if result.is_success() {
                finished += 1;
            } else {
                failed += 1;
            }
            task_time_sum_ms = task_time_sum_ms.saturating_add(result.execution_time_ms);
            if !result.worker_id.is_nil() {
                distinct_workers.insert(result.worker_id);
            }
        }

        Self {
            finished,
            failed,
            machines: distinct_workers.len(),
            waited_ms,
            task_time_sum_ms,
        }
    }

    /// Formats the first D5 line: `Tasks: <N> finished, <F> failed, on <M> machines`.
    pub fn tasks_line(&self) -> String {
        format!(
            "Tasks: {} finished, {} failed, on {} machines",
            self.finished, self.failed, self.machines
        )
    }

    /// Formats the second D5 line: `Waited: <W> s    Sum of task times: <S> s`.
    pub fn waited_line(&self) -> String {
        format!(
            "Waited: {:.3} s    Sum of task times: {:.3} s",
            self.waited_ms as f64 / 1000.0,
            self.task_time_sum_ms as f64 / 1000.0
        )
    }

    /// Inserts `waited_ms`, `task_time_sum_ms`, and `machines` into a JSON object map.
    pub fn insert_json_fields(&self, obj: &mut Map<String, Value>) {
        obj.insert("waited_ms".to_string(), Value::from(self.waited_ms));
        obj.insert(
            "task_time_sum_ms".to_string(),
            Value::from(self.task_time_sum_ms),
        );
        obj.insert("machines".to_string(), Value::from(self.machines));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::{Bytes, TaskId};
    use uuid::Uuid;

    fn sample_result(worker_id: Uuid, exit_code: i32, execution_time_ms: u64) -> TaskResult {
        TaskResult {
            worker_id,
            task_id: TaskId::new(),
            exit_code,
            stdout: Bytes::new(),
            stderr: Bytes::new(),
            execution_time_ms,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: Vec::new(),
            outputs: Vec::new(),
        }
    }

    #[test]
    fn test_run_summary_no_task() {
        let summary = RunSummary::from_results(&[], 0);
        assert_eq!(summary.finished, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.machines, 0);
        assert_eq!(summary.waited_ms, 0);
        assert_eq!(summary.task_time_sum_ms, 0);
        assert_eq!(
            summary.tasks_line(),
            "Tasks: 0 finished, 0 failed, on 0 machines"
        );
        assert_eq!(
            summary.waited_line(),
            "Waited: 0.000 s    Sum of task times: 0.000 s"
        );
    }

    #[test]
    fn test_run_summary_one_task() {
        let w1 = Uuid::from_u128(1);
        let results = [sample_result(w1, 0, 348)];
        let summary = RunSummary::from_results(&results, 412);
        assert_eq!(summary.finished, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.machines, 1);
        assert_eq!(summary.waited_ms, 412);
        assert_eq!(summary.task_time_sum_ms, 348);

        let tasks = summary.tasks_line();
        let waited = summary.waited_line();
        assert_eq!(tasks, "Tasks: 1 finished, 0 failed, on 1 machines");
        assert_eq!(waited, "Waited: 0.412 s    Sum of task times: 0.348 s");
        assert!(!tasks.contains('%') && !waited.contains('%'));
        assert!(!tasks.contains("your machine") && !waited.contains("your machine"));

        let mut map = Map::new();
        summary.insert_json_fields(&mut map);
        assert_eq!(map.get("waited_ms"), Some(&Value::from(412u64)));
        assert_eq!(map.get("task_time_sum_ms"), Some(&Value::from(348u64)));
        assert_eq!(map.get("machines"), Some(&Value::from(1usize)));
    }

    #[test]
    fn test_run_summary_one_failed_task() {
        let w1 = Uuid::from_u128(1);
        let results = [sample_result(w1, 2, 125)];
        let summary = RunSummary::from_results(&results, 200);
        assert_eq!(summary.finished, 0);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.machines, 1);
        assert_eq!(summary.waited_ms, 200);
        assert_eq!(summary.task_time_sum_ms, 125);
        assert_eq!(
            summary.tasks_line(),
            "Tasks: 0 finished, 1 failed, on 1 machines"
        );
        assert_eq!(
            summary.waited_line(),
            "Waited: 0.200 s    Sum of task times: 0.125 s"
        );

        let nil_results = [sample_result(Uuid::nil(), 1, 0)];
        let nil_summary = RunSummary::from_results(&nil_results, 10);
        assert_eq!(nil_summary.machines, 0);
        assert_eq!(nil_summary.failed, 1);
    }

    #[test]
    fn test_run_summary_two_tasks_on_one_worker() {
        let w1 = Uuid::from_u128(1);
        let results = [sample_result(w1, 0, 120), sample_result(w1, 0, 280)];
        let summary = RunSummary::from_results(&results, 450);
        assert_eq!(summary.finished, 2);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.machines, 1);
        assert_eq!(summary.waited_ms, 450);
        assert_eq!(summary.task_time_sum_ms, 400);
        assert_eq!(
            summary.tasks_line(),
            "Tasks: 2 finished, 0 failed, on 1 machines"
        );
        assert_eq!(
            summary.waited_line(),
            "Waited: 0.450 s    Sum of task times: 0.400 s"
        );
    }
}
