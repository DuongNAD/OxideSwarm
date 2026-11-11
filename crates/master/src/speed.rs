//! Per-worker task execution speed history (`docs/design/0060-speed-aware-placement.md`, D1).
//!
//! [`SpeedBook`] learns how long each worker takes for each kind of task ([`TaskKey`]) from
//! completed [`TaskResult`] frames. [`crate::scheduler::WorkloadScheduler`] consults those
//! averages during placement so batches of identical tasks wait for a clearly faster worker
//! instead of filling slow slots that would stretch total batch time.

use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;

use rusty_grid_core::task::{Task, TaskResult, TaskSpec};

/// Exponential moving average weight applied to each new sample (`0.3`).
pub const EMA_WEIGHT: f64 = 0.3;

/// Maximum number of distinct [`TaskKey`] buckets retained in [`SpeedBook`].
pub const MAX_SPEED_KEYS: usize = 4_096;

/// Maximum number of worker entries retained under a single [`TaskKey`].
pub const MAX_WORKERS_PER_KEY: usize = 64;

/// Deterministic 64-bit fingerprint of a task's kind and speed-defining parameters.
///
/// Per `0060` D1:
/// - `Command`, `ShellScript`, `RustCompilation`: task type plus program/script/crate and args/flags.
/// - `GpuCompute`: task type plus matrix edge (`simulated_matrix_dim`).
/// - `BuiltinTest`: task type plus test name, iterations, duration, and GPU flag.
/// - `Generative3D`, `AutoRig`: task type only.
///
/// Inputs, staged files, working directory, stdin, and environment variables are excluded so
/// repeated invocations over different shards share one speed history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct TaskKey(pub u64);

impl TaskKey {
    /// Computes the [`TaskKey`] of `task`.
    pub fn for_task(task: &Task) -> Self {
        Self::of(&task.spec)
    }

    /// Computes the [`TaskKey`] of `spec`.
    pub fn of(spec: &TaskSpec) -> Self {
        let mut hasher = blake3::Hasher::new();
        match spec {
            TaskSpec::Command { program, args, .. } => {
                hasher.update(&[1u8]);
                hash_str(&mut hasher, program);
                hash_str_slice(&mut hasher, args);
            }
            TaskSpec::ShellScript {
                script,
                interpreter,
                ..
            } => {
                hasher.update(&[2u8]);
                match interpreter {
                    Some(interp) => {
                        hasher.update(&[1u8]);
                        hash_str(&mut hasher, interp);
                    }
                    None => {
                        hasher.update(&[0u8]);
                    }
                }
                hash_str(&mut hasher, script);
            }
            TaskSpec::RustCompilation {
                crate_name,
                compiler_flags,
                ..
            } => {
                hasher.update(&[3u8]);
                hash_str(&mut hasher, crate_name);
                hash_str_slice(&mut hasher, compiler_flags);
            }
            TaskSpec::GpuCompute {
                simulated_matrix_dim,
                ..
            } => {
                hasher.update(&[4u8]);
                hasher.update(&simulated_matrix_dim.to_le_bytes());
            }
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                require_gpu,
                should_fail: _,
            } => {
                hasher.update(&[5u8]);
                hash_str(&mut hasher, test_name);
                hasher.update(&iterations.to_le_bytes());
                hasher.update(&duration_ms.to_le_bytes());
                hasher.update(&[u8::from(*require_gpu)]);
            }
            TaskSpec::Generative3D(_) => {
                hasher.update(&[6u8]);
            }
            TaskSpec::AutoRig(_) => {
                hasher.update(&[7u8]);
            }
        }
        let digest = hasher.finalize();
        let bytes: [u8; 8] = digest.as_bytes()[..8]
            .try_into()
            .expect("blake3 digest is 32 bytes");
        Self(u64::from_le_bytes(bytes))
    }
}

fn hash_str(hasher: &mut blake3::Hasher, s: &str) {
    hasher.update(&(s.len() as u64).to_le_bytes());
    hasher.update(s.as_bytes());
}

fn hash_str_slice(hasher: &mut blake3::Hasher, items: &[String]) {
    hasher.update(&(items.len() as u64).to_le_bytes());
    for item in items {
        hash_str(hasher, item);
    }
}

/// Measured speed summary for one `(worker_id, TaskKey)` pair.
#[derive(Debug, Clone, Copy)]
pub struct SpeedEntry {
    /// Exponential moving average of `execution_time_ms` (`EMA_WEIGHT = 0.3`).
    pub avg_ms: f64,
    /// Number of accepted successful samples folded into `avg_ms`.
    pub samples: u64,
    /// Timestamp when the most recent sample was recorded.
    pub last_sample: Instant,
    /// Monotonic insertion/update counter breaking ties on coarse `Instant` clocks.
    seq: u64,
}

#[derive(Debug, Clone)]
struct KeyBucket {
    last_used_seq: u64,
    workers: HashMap<Uuid, SpeedEntry>,
}

/// Bounded in-memory table of per-worker task execution speeds (`0060` D1).
#[derive(Debug, Clone)]
pub struct SpeedBook {
    buckets: HashMap<TaskKey, KeyBucket>,
    max_keys: usize,
    max_workers_per_key: usize,
    next_seq: u64,
}

impl Default for SpeedBook {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedBook {
    /// Creates an empty [`SpeedBook`] with the standard bounds (`4,096` keys, `64` workers/key).
    pub fn new() -> Self {
        Self::with_limits(MAX_SPEED_KEYS, MAX_WORKERS_PER_KEY)
    }

    /// Creates an empty [`SpeedBook`] with custom bounds (each clamped to at least `1`).
    pub fn with_limits(max_keys: usize, max_workers_per_key: usize) -> Self {
        Self {
            buckets: HashMap::new(),
            max_keys: max_keys.max(1),
            max_workers_per_key: max_workers_per_key.max(1),
            next_seq: 0,
        }
    }

    /// True when no speed samples are currently stored.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Number of [`TaskKey`] buckets currently retained.
    pub fn key_count(&self) -> usize {
        self.buckets.len()
    }

    /// Number of workers currently recorded under `key`.
    pub fn worker_count(&self, key: TaskKey) -> usize {
        self.buckets.get(&key).map_or(0, |b| b.workers.len())
    }

    /// Looks up the speed entry for `(worker_id, key)` without mutating LRU order.
    pub fn get(&self, worker_id: &Uuid, key: TaskKey) -> Option<&SpeedEntry> {
        self.buckets.get(&key)?.workers.get(worker_id)
    }

    /// Records a terminal [`TaskResult`] for `(worker_id, key)` when eligible under `0060` D1.
    ///
    /// Only successful results (`exit_code == 0 && error.is_none()`) with `1 <= execution_time_ms`
    /// and `execution_time_ms <= timeout_secs * 1000` are recorded. Returns `true` when the sample
    /// was accepted.
    pub fn record_result(
        &mut self,
        worker_id: Uuid,
        key: TaskKey,
        timeout_secs: u64,
        result: &TaskResult,
    ) -> bool {
        if !result.is_success() {
            return false;
        }
        self.record_sample_at(
            worker_id,
            key,
            result.execution_time_ms,
            timeout_secs,
            Instant::now(),
        )
    }

    /// Records a successful duration sample at `Instant::now()`.
    pub fn record_sample(
        &mut self,
        worker_id: Uuid,
        key: TaskKey,
        duration_ms: u64,
        timeout_secs: u64,
    ) -> bool {
        self.record_sample_at(worker_id, key, duration_ms, timeout_secs, Instant::now())
    }

    /// Records a successful duration sample with an explicit timestamp (used by deterministic unit
    /// tests and [`Self::record_result`]).
    pub fn record_sample_at(
        &mut self,
        worker_id: Uuid,
        key: TaskKey,
        duration_ms: u64,
        timeout_secs: u64,
        now: Instant,
    ) -> bool {
        let effective_timeout_secs = if timeout_secs == 0 { 60 } else { timeout_secs };
        let max_ms = effective_timeout_secs.saturating_mul(1_000);
        if duration_ms == 0 || duration_ms > max_ms {
            return false;
        }

        self.next_seq = self.next_seq.wrapping_add(1);
        let seq = self.next_seq;

        if !self.buckets.contains_key(&key) && self.buckets.len() >= self.max_keys {
            if let Some((&lru_key, _)) = self
                .buckets
                .iter()
                .min_by_key(|(&k, bucket)| (bucket.last_used_seq, k))
            {
                self.buckets.remove(&lru_key);
            }
        }

        let max_workers = self.max_workers_per_key;
        let bucket = self.buckets.entry(key).or_insert_with(|| KeyBucket {
            last_used_seq: seq,
            workers: HashMap::new(),
        });
        bucket.last_used_seq = seq;

        if !bucket.workers.contains_key(&worker_id) && bucket.workers.len() >= max_workers {
            if let Some((&oldest_worker, _)) = bucket
                .workers
                .iter()
                .min_by_key(|(&wid, entry)| (entry.last_sample, entry.seq, wid))
            {
                bucket.workers.remove(&oldest_worker);
            }
        }

        let duration = duration_ms as f64;
        bucket
            .workers
            .entry(worker_id)
            .and_modify(|entry| {
                entry.avg_ms = EMA_WEIGHT * duration + (1.0 - EMA_WEIGHT) * entry.avg_ms;
                entry.samples = entry.samples.saturating_add(1);
                entry.last_sample = now;
                entry.seq = seq;
            })
            .or_insert(SpeedEntry {
                avg_ms: duration,
                samples: 1,
                last_sample: now,
                seq,
            });
        true
    }

    /// Removes all recorded speed entries for `worker_id` (called when a disconnected worker is
    /// pruned or removed from [`crate::registry::WorkerRegistry`]).
    pub fn remove_worker(&mut self, worker_id: &Uuid) {
        self.buckets.retain(|_, bucket| {
            bucket.workers.remove(worker_id);
            !bucket.workers.is_empty()
        });
    }

    /// Removes all recorded speed entries for each worker in `worker_ids`.
    pub fn remove_workers(&mut self, worker_ids: &[Uuid]) {
        if worker_ids.is_empty() || self.buckets.is_empty() {
            return;
        }
        self.buckets.retain(|_, bucket| {
            for id in worker_ids {
                bucket.workers.remove(id);
            }
            !bucket.workers.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    use rusty_grid_core::blob::{BlobId, TaskInput};
    use rusty_grid_core::task::{
        AutoRigSpec, Generative3DSpec, StagedFile, TaskId, TaskRequirements,
    };

    #[test]
    fn task_key_hashes_type_and_defining_fields_while_ignoring_inputs_and_env() {
        let mut env_a = HashMap::new();
        env_a.insert("FOO".to_string(), "1".to_string());
        let mut env_b = HashMap::new();
        env_b.insert("FOO".to_string(), "2".to_string());
        env_b.insert("BAR".to_string(), "baz".to_string());

        let mut task_a = Task::new(
            TaskSpec::Command {
                program: "python3".into(),
                args: vec!["work.py".into(), "--shard=1".into()],
                env: env_a.clone(),
                working_dir: Some(PathBuf::from("sub_a")),
                stdin: Some(b"alpha".to_vec()),
            },
            TaskRequirements::default(),
        );
        task_a
            .staged_files
            .push(StagedFile::new("in.txt", "hello", false));
        task_a.inputs.push(TaskInput::file(
            "blob.bin",
            BlobId::of(b"blob-data"),
            9,
            false,
        ));

        let task_b = Task::new(
            TaskSpec::Command {
                program: "python3".into(),
                args: vec!["work.py".into(), "--shard=1".into()],
                env: env_b.clone(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(4, 120),
        );
        assert_eq!(TaskKey::for_task(&task_a), TaskKey::for_task(&task_b));

        // Different argument boundaries must not collide.
        let boundary_a = TaskSpec::Command {
            program: "cmd".into(),
            args: vec!["ab".into(), "c".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        let boundary_b = TaskSpec::Command {
            program: "cmd".into(),
            args: vec!["a".into(), "bc".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        assert_ne!(TaskKey::of(&boundary_a), TaskKey::of(&boundary_b));

        // ShellScript ignores env, respects script and interpreter.
        let sh_a = TaskSpec::ShellScript {
            script: "echo hi".into(),
            interpreter: Some("bash".into()),
            env: env_a,
        };
        let sh_b = TaskSpec::ShellScript {
            script: "echo hi".into(),
            interpreter: Some("bash".into()),
            env: env_b,
        };
        let sh_pwsh = TaskSpec::ShellScript {
            script: "echo hi".into(),
            interpreter: Some("pwsh".into()),
            env: HashMap::new(),
        };
        assert_eq!(TaskKey::of(&sh_a), TaskKey::of(&sh_b));
        assert_ne!(TaskKey::of(&sh_a), TaskKey::of(&sh_pwsh));

        // RustCompilation ignores source_files and target_dir, respects crate_name and flags.
        let mut src_a = HashMap::new();
        src_a.insert("src/lib.rs".into(), "pub fn a() {}".into());
        let mut src_b = HashMap::new();
        src_b.insert("src/lib.rs".into(), "pub fn b() {}".into());
        let rust_a = TaskSpec::RustCompilation {
            crate_name: "demo".into(),
            source_files: src_a,
            compiler_flags: vec!["--release".into()],
            target_dir: Some(PathBuf::from("t1")),
        };
        let rust_b = TaskSpec::RustCompilation {
            crate_name: "demo".into(),
            source_files: src_b,
            compiler_flags: vec!["--release".into()],
            target_dir: None,
        };
        let rust_debug = TaskSpec::RustCompilation {
            crate_name: "demo".into(),
            source_files: HashMap::new(),
            compiler_flags: Vec::new(),
            target_dir: None,
        };
        assert_eq!(TaskKey::of(&rust_a), TaskKey::of(&rust_b));
        assert_ne!(TaskKey::of(&rust_a), TaskKey::of(&rust_debug));

        // GpuCompute keys on matrix edge only.
        let gpu_4096_a = TaskSpec::GpuCompute {
            kernel_name: "k1".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 8,
            simulated_matrix_dim: 4096,
        };
        let gpu_4096_b = TaskSpec::GpuCompute {
            kernel_name: "k2".into(),
            input_data: vec![9],
            work_group_size: 16,
            simulated_matrix_dim: 4096,
        };
        let gpu_8192 = TaskSpec::GpuCompute {
            kernel_name: "k1".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 8,
            simulated_matrix_dim: 8192,
        };
        assert_eq!(TaskKey::of(&gpu_4096_a), TaskKey::of(&gpu_4096_b));
        assert_ne!(TaskKey::of(&gpu_4096_a), TaskKey::of(&gpu_8192));

        // Generative3D and AutoRig key on task type only.
        let g3d_a = TaskSpec::Generative3D(Generative3DSpec::new_prompt("chair"));
        let g3d_b =
            TaskSpec::Generative3D(Generative3DSpec::new_prompt("spaceship").with_quality("high"));
        let rig_a = TaskSpec::AutoRig(AutoRigSpec::new(vec![1u8, 2, 3], "biped"));
        let rig_b = TaskSpec::AutoRig(AutoRigSpec::new(vec![4u8, 5, 6], "quadruped"));
        assert_eq!(TaskKey::of(&g3d_a), TaskKey::of(&g3d_b));
        assert_eq!(TaskKey::of(&rig_a), TaskKey::of(&rig_b));
        assert_ne!(TaskKey::of(&g3d_a), TaskKey::of(&rig_a));
    }

    #[test]
    fn speed_book_ema_average_and_sample_count() {
        let mut book = SpeedBook::new();
        let worker = Uuid::new_v4();
        let key = TaskKey(42);
        let t0 = Instant::now();

        assert!(book.record_sample_at(worker, key, 100, 60, t0));
        let e1 = book.get(&worker, key).expect("entry after sample 1");
        assert!((e1.avg_ms - 100.0).abs() < 1e-9);
        assert_eq!(e1.samples, 1);
        assert_eq!(e1.last_sample, t0);

        let t1 = t0 + Duration::from_secs(1);
        assert!(book.record_sample_at(worker, key, 200, 60, t1));
        let e2 = book.get(&worker, key).expect("entry after sample 2");
        // 0.3 * 200 + 0.7 * 100 = 130
        assert!((e2.avg_ms - 130.0).abs() < 1e-9);
        assert_eq!(e2.samples, 2);
        assert_eq!(e2.last_sample, t1);

        let t2 = t1 + Duration::from_secs(1);
        assert!(book.record_sample_at(worker, key, 50, 60, t2));
        let e3 = book.get(&worker, key).expect("entry after sample 3");
        // 0.3 * 50 + 0.7 * 130 = 106
        assert!((e3.avg_ms - 106.0).abs() < 1e-9);
        assert_eq!(e3.samples, 3);
        assert_eq!(e3.last_sample, t2);
    }

    #[test]
    fn speed_book_ignores_zero_over_timeout_and_failed_results() {
        let mut book = SpeedBook::new();
        let worker = Uuid::new_v4();
        let key = TaskKey(99);
        let task_id = TaskId::new();

        // duration_ms == 0 is ignored.
        let zero = TaskResult::success(worker, task_id, "ok", 0, false);
        assert!(!book.record_result(worker, key, 10, &zero));
        assert!(book.is_empty());

        // Above timeout (10_001 ms > 10 * 1000 ms) is ignored.
        let over_timeout = TaskResult::success(worker, task_id, "ok", 10_001, false);
        assert!(!book.record_result(worker, key, 10, &over_timeout));
        assert!(book.is_empty());

        // Non-zero exit code is ignored.
        let failed = TaskResult::failure(worker, task_id, 1, "", "boom", 500, Some("err".into()));
        assert!(!book.record_result(worker, key, 10, &failed));
        assert!(book.is_empty());

        // Cancelled (130) is ignored.
        let cancelled =
            TaskResult::failure(worker, task_id, 130, "", "", 500, Some("Cancelled".into()));
        assert!(!book.record_result(worker, key, 10, &cancelled));
        assert!(book.is_empty());

        // exit_code == 0 with an error string attached is not a clean success and is ignored.
        let mut errored_zero = TaskResult::success(worker, task_id, "ok", 500, false);
        errored_zero.error = Some("missing output blob".into());
        assert!(!book.record_result(worker, key, 10, &errored_zero));
        assert!(book.is_empty());

        // Exact timeout boundary (10_000 ms == 10 * 1000 ms) is accepted.
        let at_timeout = TaskResult::success(worker, task_id, "ok", 10_000, false);
        assert!(book.record_result(worker, key, 10, &at_timeout));
        let entry = book.get(&worker, key).expect("accepted boundary sample");
        assert_eq!(entry.samples, 1);
        assert!((entry.avg_ms - 10_000.0).abs() < 1e-9);
    }

    #[test]
    fn speed_book_enforces_4096_key_lru_bound_and_64_workers_per_key() {
        let mut book = SpeedBook::new();
        let worker = Uuid::new_v4();
        let base = Instant::now();

        for i in 0..(MAX_SPEED_KEYS as u64) {
            assert!(book.record_sample_at(worker, TaskKey(i), 100, 60, base));
        }
        assert_eq!(book.key_count(), MAX_SPEED_KEYS);

        // Refresh key 0 so key 1 becomes the least-recently-used key.
        assert!(book.record_sample_at(
            worker,
            TaskKey(0),
            120,
            60,
            base + Duration::from_millis(1),
        ));

        // Inserting key 4096 evicts key 1, preserving key 0.
        let extra_key = TaskKey(MAX_SPEED_KEYS as u64);
        assert!(book.record_sample_at(worker, extra_key, 80, 60, base + Duration::from_millis(2),));
        assert_eq!(book.key_count(), MAX_SPEED_KEYS);
        assert!(book.get(&worker, TaskKey(0)).is_some());
        assert!(book.get(&worker, TaskKey(1)).is_none());
        assert!(book.get(&worker, extra_key).is_some());

        // Per-key worker cap: 64 workers retained, oldest sample evicted first.
        let shared_key = TaskKey(999_999);
        let mut workers = Vec::with_capacity(MAX_WORKERS_PER_KEY + 1);
        for idx in 0..=MAX_WORKERS_PER_KEY {
            let wid = Uuid::from_u128((idx + 1) as u128);
            workers.push(wid);
            assert!(book.record_sample_at(
                wid,
                shared_key,
                50 + idx as u64,
                60,
                base + Duration::from_millis(idx as u64),
            ));
        }
        assert_eq!(book.worker_count(shared_key), MAX_WORKERS_PER_KEY);
        assert!(
            book.get(&workers[0], shared_key).is_none(),
            "oldest worker on the key must be evicted when inserting the 65th worker"
        );
        assert!(book.get(&workers[1], shared_key).is_some());
        assert!(book
            .get(&workers[MAX_WORKERS_PER_KEY], shared_key)
            .is_some());

        // Removing a worker drops its entries and cleans up empty buckets.
        book.remove_worker(&worker);
        assert!(book.get(&worker, TaskKey(0)).is_none());
        assert_eq!(
            book.key_count(),
            1,
            "only shared_key still has remaining workers"
        );
        book.remove_workers(&workers[1..]);
        assert!(book.is_empty());
    }

    #[tokio::test]
    async fn registry_remove_and_prune_drop_worker_speed_entries() {
        use std::sync::{Arc, Mutex};
        use tokio::sync::mpsc;

        use rusty_grid_core::capabilities::WorkerCapabilities;

        use crate::registry::WorkerRegistry;

        let registry = WorkerRegistry::new();
        let book = Arc::new(Mutex::new(SpeedBook::new()));
        registry.set_speed_book(Arc::clone(&book));

        let w1 = Uuid::new_v4();
        let w2 = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(8);
        let (tx2, _rx2) = mpsc::channel(8);
        registry
            .register(
                w1,
                WorkerCapabilities::new("w1", 4, 4096, false, false, None),
                "127.0.0.1:10001".parse().unwrap(),
                tx1,
                None,
            )
            .await
            .unwrap();
        registry
            .register(
                w2,
                WorkerCapabilities::new("w2", 4, 4096, false, false, None),
                "127.0.0.1:10002".parse().unwrap(),
                tx2,
                None,
            )
            .await
            .unwrap();

        let key = TaskKey(77);
        {
            let mut sb = book.lock().unwrap();
            // Also verify timeout_secs == 0 uses the 60 s fallback like TaskRunner::execute.
            assert!(sb.record_sample(w1, key, 250, 0));
            assert!(sb.record_sample(w2, key, 900, 60));
            assert_eq!(sb.worker_count(key), 2);
        }

        // Direct removal drops w1's speed entries immediately.
        assert!(registry.remove_worker(&w1).await.is_some());
        assert!(book.lock().unwrap().get(&w1, key).is_none());
        assert!(book.lock().unwrap().get(&w2, key).is_some());

        // Sweeping w2's session and pruning its stale disconnected entry drops w2's speed entries.
        registry.mark_disconnected(&w2).await.unwrap();
        assert_eq!(registry.prune_stale_workers(Duration::ZERO).await, 1);
        assert!(book.lock().unwrap().get(&w2, key).is_none());
        assert!(book.lock().unwrap().is_empty());
    }
}
