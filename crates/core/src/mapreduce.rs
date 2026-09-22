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
#[derive(Debug, Clone, PartialEq)]
pub enum MapFunctionSpec {
    /// Command receiving data chunk via stdin, emitting JSON Lines or whitespace-separated key/value to stdout.
    Command { program: String, args: Vec<String> },
    /// Shell script receiving data chunk via stdin/environment, emitting key/value to stdout.
    ShellScript { script: String },
    /// Pre-registered in-memory operator (e.g. "word_count", "line_count", "identity").
    Builtin { operator: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanMapFunctionSpec {
    Command { program: String, args: Vec<String> },
    ShellScript { script: String },
    Builtin { operator: String },
}

#[derive(Serialize, Deserialize)]
enum BinaryMapFunctionSpec {
    Command { program: String, args: Vec<String> },
    ShellScript { script: String },
    Builtin { operator: String },
}

impl From<MapFunctionSpec> for HumanMapFunctionSpec {
    fn from(s: MapFunctionSpec) -> Self {
        match s {
            MapFunctionSpec::Command { program, args } => Self::Command { program, args },
            MapFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            MapFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl From<HumanMapFunctionSpec> for MapFunctionSpec {
    fn from(s: HumanMapFunctionSpec) -> Self {
        match s {
            HumanMapFunctionSpec::Command { program, args } => Self::Command { program, args },
            HumanMapFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            HumanMapFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl From<MapFunctionSpec> for BinaryMapFunctionSpec {
    fn from(s: MapFunctionSpec) -> Self {
        match s {
            MapFunctionSpec::Command { program, args } => Self::Command { program, args },
            MapFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            MapFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl From<BinaryMapFunctionSpec> for MapFunctionSpec {
    fn from(s: BinaryMapFunctionSpec) -> Self {
        match s {
            BinaryMapFunctionSpec::Command { program, args } => Self::Command { program, args },
            BinaryMapFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            BinaryMapFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl Serialize for MapFunctionSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanMapFunctionSpec::from(self.clone()).serialize(serializer)
        } else {
            BinaryMapFunctionSpec::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for MapFunctionSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanMapFunctionSpec::deserialize(deserializer).map(Into::into)
        } else {
            BinaryMapFunctionSpec::deserialize(deserializer).map(Into::into)
        }
    }
}

/// Reducer execution specification.
#[derive(Debug, Clone, PartialEq)]
pub enum ReduceFunctionSpec {
    /// Command receiving grouped KV JSON via stdin, emitting reduced KV to stdout.
    Command { program: String, args: Vec<String> },
    /// Shell script receiving grouped KV JSON via stdin/environment, emitting reduced KV to stdout.
    ShellScript { script: String },
    /// Pre-registered in-memory operator (e.g. "sum", "count", "max", "min").
    Builtin { operator: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanReduceFunctionSpec {
    Command { program: String, args: Vec<String> },
    ShellScript { script: String },
    Builtin { operator: String },
}

#[derive(Serialize, Deserialize)]
enum BinaryReduceFunctionSpec {
    Command { program: String, args: Vec<String> },
    ShellScript { script: String },
    Builtin { operator: String },
}

impl From<ReduceFunctionSpec> for HumanReduceFunctionSpec {
    fn from(s: ReduceFunctionSpec) -> Self {
        match s {
            ReduceFunctionSpec::Command { program, args } => Self::Command { program, args },
            ReduceFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            ReduceFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl From<HumanReduceFunctionSpec> for ReduceFunctionSpec {
    fn from(s: HumanReduceFunctionSpec) -> Self {
        match s {
            HumanReduceFunctionSpec::Command { program, args } => Self::Command { program, args },
            HumanReduceFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            HumanReduceFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl From<ReduceFunctionSpec> for BinaryReduceFunctionSpec {
    fn from(s: ReduceFunctionSpec) -> Self {
        match s {
            ReduceFunctionSpec::Command { program, args } => Self::Command { program, args },
            ReduceFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            ReduceFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl From<BinaryReduceFunctionSpec> for ReduceFunctionSpec {
    fn from(s: BinaryReduceFunctionSpec) -> Self {
        match s {
            BinaryReduceFunctionSpec::Command { program, args } => Self::Command { program, args },
            BinaryReduceFunctionSpec::ShellScript { script } => Self::ShellScript { script },
            BinaryReduceFunctionSpec::Builtin { operator } => Self::Builtin { operator },
        }
    }
}

impl Serialize for ReduceFunctionSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanReduceFunctionSpec::from(self.clone()).serialize(serializer)
        } else {
            BinaryReduceFunctionSpec::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ReduceFunctionSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanReduceFunctionSpec::deserialize(deserializer).map(Into::into)
        } else {
            BinaryReduceFunctionSpec::deserialize(deserializer).map(Into::into)
        }
    }
}

/// Grouped Key-Values passed to a reducer task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GroupedKeyValue {
    pub key: String,
    pub values: Vec<serde_json::Value>,
}

/// Execution outcome of a Map/Reduce job.
#[derive(Debug, Clone, PartialEq)]
pub struct MapReduceResult {
    pub job_id: Uuid,
    pub status: String,
    pub output: HashMap<String, serde_json::Value>,
    pub map_tasks_total: usize,
    pub map_tasks_completed: usize,
    pub reduce_tasks_total: usize,
    pub reduce_tasks_completed: usize,
    pub execution_time_ms: u64,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct HumanMapReduceResult {
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

#[derive(Serialize, Deserialize)]
struct BinaryMapReduceResult {
    pub job_id: Uuid,
    pub status: String,
    pub output_json: String,
    pub map_tasks_total: usize,
    pub map_tasks_completed: usize,
    pub reduce_tasks_total: usize,
    pub reduce_tasks_completed: usize,
    pub execution_time_ms: u64,
    pub error: Option<String>,
}

impl From<MapReduceResult> for HumanMapReduceResult {
    fn from(r: MapReduceResult) -> Self {
        Self {
            job_id: r.job_id,
            status: r.status,
            output: r.output,
            map_tasks_total: r.map_tasks_total,
            map_tasks_completed: r.map_tasks_completed,
            reduce_tasks_total: r.reduce_tasks_total,
            reduce_tasks_completed: r.reduce_tasks_completed,
            execution_time_ms: r.execution_time_ms,
            error: r.error,
        }
    }
}

impl From<HumanMapReduceResult> for MapReduceResult {
    fn from(h: HumanMapReduceResult) -> Self {
        Self {
            job_id: h.job_id,
            status: h.status,
            output: h.output,
            map_tasks_total: h.map_tasks_total,
            map_tasks_completed: h.map_tasks_completed,
            reduce_tasks_total: h.reduce_tasks_total,
            reduce_tasks_completed: h.reduce_tasks_completed,
            execution_time_ms: h.execution_time_ms,
            error: h.error,
        }
    }
}

impl From<MapReduceResult> for BinaryMapReduceResult {
    fn from(r: MapReduceResult) -> Self {
        Self {
            job_id: r.job_id,
            status: r.status,
            output_json: serde_json::to_string(&r.output).unwrap_or_else(|_| "{}".into()),
            map_tasks_total: r.map_tasks_total,
            map_tasks_completed: r.map_tasks_completed,
            reduce_tasks_total: r.reduce_tasks_total,
            reduce_tasks_completed: r.reduce_tasks_completed,
            execution_time_ms: r.execution_time_ms,
            error: r.error,
        }
    }
}

impl From<BinaryMapReduceResult> for MapReduceResult {
    fn from(b: BinaryMapReduceResult) -> Self {
        Self {
            job_id: b.job_id,
            status: b.status,
            output: serde_json::from_str(&b.output_json).unwrap_or_default(),
            map_tasks_total: b.map_tasks_total,
            map_tasks_completed: b.map_tasks_completed,
            reduce_tasks_total: b.reduce_tasks_total,
            reduce_tasks_completed: b.reduce_tasks_completed,
            execution_time_ms: b.execution_time_ms,
            error: b.error,
        }
    }
}

impl Serialize for MapReduceResult {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanMapReduceResult::from(self.clone()).serialize(serializer)
        } else {
            BinaryMapReduceResult::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for MapReduceResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanMapReduceResult::deserialize(deserializer).map(Into::into)
        } else {
            BinaryMapReduceResult::deserialize(deserializer).map(Into::into)
        }
    }
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

        let bin = bincode::serialize(&spec).expect("bincode serialize");
        let bin_deser: MapReduceJobSpec = bincode::deserialize(&bin).expect("bincode deserialize");
        assert_eq!(spec, bin_deser);
    }

    #[test]
    fn test_mapreduce_result_bincode_roundtrip() {
        let mut output = HashMap::new();
        output.insert("word".to_string(), serde_json::json!(42));
        output.insert("count".to_string(), serde_json::json!(100));

        let res = MapReduceResult {
            job_id: Uuid::new_v4(),
            status: "Completed".into(),
            output,
            map_tasks_total: 2,
            map_tasks_completed: 2,
            reduce_tasks_total: 1,
            reduce_tasks_completed: 1,
            execution_time_ms: 120,
            error: None,
        };

        let json = serde_json::to_string(&res).expect("serialize");
        let json_deser: MapReduceResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(res, json_deser);

        let bin = bincode::serialize(&res).expect("bincode serialize");
        let bin_deser: MapReduceResult = bincode::deserialize(&bin).expect("bincode deserialize");
        assert_eq!(res, bin_deser);
    }
}
