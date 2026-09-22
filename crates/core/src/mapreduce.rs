//! In-memory Map/Reduce wire types and specifications for distributed stream processing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Top-level configuration for an in-memory Map/Reduce job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MapReduceJobSpec {
    pub job_id: Uuid,
    pub name: String,
    pub input_data: Vec<String>,
    pub mapper: MapFunctionSpec,
    pub reducer: ReduceFunctionSpec,
    pub partition_count: usize,
    pub reducer_count: usize,
    pub timeout_secs: u64,
}

impl MapReduceJobSpec {
    /// Creates a new MapReduceJobSpec with generated UUID.
    pub fn new(
        name: impl Into<String>,
        input_data: Vec<String>,
        mapper: MapFunctionSpec,
        reducer: ReduceFunctionSpec,
        partition_count: usize,
        reducer_count: usize,
        timeout_secs: u64,
    ) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            name: name.into(),
            input_data,
            mapper,
            reducer,
            partition_count,
            reducer_count,
            timeout_secs,
        }
    }
}

/// Mapper execution specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum MapFunctionSpec {
    /// Command receiving data chunk via stdin, emitting JSON Lines or whitespace-separated key/value to stdout.
    Command { program: String, args: Vec<String> },
    /// Shell script receiving data chunk via stdin/environment, emitting key/value to stdout.
    ShellScript { script: String },
    /// Pre-registered in-memory operator (e.g. "word_count", "line_count", "identity").
    Builtin { operator: String },
}

/// Reducer execution specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ReduceFunctionSpec {
    /// Command receiving grouped KV JSON via stdin, emitting reduced KV to stdout.
    Command { program: String, args: Vec<String> },
    /// Shell script receiving grouped KV JSON via stdin/environment, emitting reduced KV to stdout.
    ShellScript { script: String },
    /// Pre-registered in-memory operator (e.g. "sum", "count", "max", "min").
    Builtin { operator: String },
}

/// Grouped Key-Values passed to a reducer task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GroupedKeyValue {
    pub key: String,
    pub values: Vec<serde_json::Value>,
}

/// Execution outcome of a Map/Reduce job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MapReduceResult {
    pub job_id: Uuid,
    pub status: String,
    pub output: HashMap<String, serde_json::Value>,
    pub map_tasks_total: usize,
    pub map_tasks_completed: usize,
    pub reduce_tasks_total: usize,
    pub reduce_tasks_completed: usize,
    pub execution_time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapreduce_job_spec_roundtrip() {
        let spec = MapReduceJobSpec::new(
            "word_count",
            vec!["apple banana".into(), "banana cherry".into()],
            MapFunctionSpec::Builtin {
                operator: "word_count".into(),
            },
            ReduceFunctionSpec::Builtin {
                operator: "sum".into(),
            },
            2,
            1,
            60,
        );

        let json = serde_json::to_string(&spec).expect("serialize");
        let deserialized: MapReduceJobSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(spec, deserialized);
    }
}
