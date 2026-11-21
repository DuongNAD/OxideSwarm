//! Content-addressed action cache keying for deterministic tasks (`CACHE-1`, design 0100).

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::extension::{Extension, ACTION_CACHE_EXTENSION_ID};
use super::{Task, TaskSpec, WorkspaceSource};
use crate::blob::{InputSource, TaskInput};
use crate::cluster_auth::{hex_decode, hex_encode};

/// Domain separator prefixing every canonical action-cache digest (`oxideswarm-action-cache-v1`).
pub const ACTION_CACHE_KEY_DOMAIN: &[u8] = b"oxideswarm-action-cache-v1";

/// 32-byte BLAKE3 key identifying a task's deterministic execution definition and declared inputs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActionCacheKey(pub [u8; 32]);

impl ActionCacheKey {
    /// Constructs a key from raw digest bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw 32-byte digest.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Formats the key as 64 lowercase hexadecimal characters.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// Parses a 64-character hexadecimal key string.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        let bytes = hex_decode(s).ok_or_else(|| "invalid hex in action cache key".to_string())?;
        if bytes.len() != 32 {
            return Err(format!(
                "action cache key must be 32 bytes (64 hex chars), got {} bytes",
                bytes.len()
            ));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl fmt::Debug for ActionCacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActionCacheKey({})", self.to_hex())
    }
}

impl fmt::Display for ActionCacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for ActionCacheKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl AsRef<[u8]> for ActionCacheKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Computes the canonical, length-prefixed BLAKE3 cache key for `task`.
///
/// Includes `task.spec` (with sorted env/source maps), sorted `inputs`, sorted `staged_files`,
/// `workspace.source`, sorted/deduped `output_globs` and `expected_outputs`, execution-affecting
/// placement (`scheduling_placement()`), `requirements.gpu_required`, `requirements.vram_mb`, and
/// any non-cache extensions.
///
/// Excludes volatile per-submission fields (`id`, `created_at_utc`, `timeout_secs`), scheduling
/// hints (`cpu_cores`, `ram_mb`, `max_retries`, `opt:repeatable`), and the `action-cache` opt-in
/// extension itself.
pub fn compute_action_cache_key(task: &Task) -> ActionCacheKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ACTION_CACHE_KEY_DOMAIN);

    hash_task_spec(&mut hasher, &task.spec);
    hash_inputs(&mut hasher, &task.inputs);
    hash_staged_files(&mut hasher, &task.staged_files);
    hash_workspace_source(
        &mut hasher,
        task.workspace.as_ref().and_then(|ws| ws.source.as_ref()),
    );
    hash_sorted_deduped_strings(&mut hasher, &task.output_globs);
    hash_sorted_deduped_strings(&mut hasher, &task.expected_outputs);
    hash_placement_and_hardware(&mut hasher, task);
    hash_non_cache_extensions(&mut hasher, &task.extensions);
    hash_non_cache_extensions(&mut hasher, &task.requirements.extensions);

    ActionCacheKey(*hasher.finalize().as_bytes())
}

fn hash_u64(hasher: &mut blake3::Hasher, val: u64) {
    hasher.update(&val.to_le_bytes());
}

fn hash_u32(hasher: &mut blake3::Hasher, val: u32) {
    hasher.update(&val.to_le_bytes());
}

fn hash_u16(hasher: &mut blake3::Hasher, val: u16) {
    hasher.update(&val.to_le_bytes());
}

fn hash_bool(hasher: &mut blake3::Hasher, val: bool) {
    hasher.update(&[u8::from(val)]);
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hash_u64(hasher, bytes.len() as u64);
    hasher.update(bytes);
}

fn hash_str(hasher: &mut blake3::Hasher, s: &str) {
    hash_bytes(hasher, s.as_bytes());
}

fn hash_opt_str(hasher: &mut blake3::Hasher, opt: Option<&str>) {
    match opt {
        Some(s) => {
            hasher.update(&[1]);
            hash_str(hasher, s);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_opt_path(hasher: &mut blake3::Hasher, opt: Option<&Path>) {
    match opt {
        Some(p) => {
            hasher.update(&[1]);
            hash_str(hasher, &p.to_string_lossy());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_opt_bytes(hasher: &mut blake3::Hasher, opt: Option<&[u8]>) {
    match opt {
        Some(b) => {
            hasher.update(&[1]);
            hash_bytes(hasher, b);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_opt_u64(hasher: &mut blake3::Hasher, opt: Option<u64>) {
    match opt {
        Some(v) => {
            hasher.update(&[1]);
            hash_u64(hasher, v);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_str_slice(hasher: &mut blake3::Hasher, items: &[String]) {
    hash_u64(hasher, items.len() as u64);
    for item in items {
        hash_str(hasher, item);
    }
}

fn hash_str_map(hasher: &mut blake3::Hasher, map: &HashMap<String, String>) {
    let mut entries: Vec<(&str, &str)> =
        map.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    entries.sort_unstable();
    hash_u64(hasher, entries.len() as u64);
    for (k, v) in entries {
        hash_str(hasher, k);
        hash_str(hasher, v);
    }
}

fn hash_sorted_deduped_strings(hasher: &mut blake3::Hasher, items: &[String]) {
    let mut sorted: Vec<&str> = items.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    hash_u64(hasher, sorted.len() as u64);
    for s in sorted {
        hash_str(hasher, s);
    }
}

fn hash_task_spec(hasher: &mut blake3::Hasher, spec: &TaskSpec) {
    match spec {
        TaskSpec::Command {
            program,
            args,
            env,
            working_dir,
            stdin,
        } => {
            hasher.update(&[0]);
            hash_str(hasher, program);
            hash_str_slice(hasher, args);
            hash_str_map(hasher, env);
            hash_opt_path(hasher, working_dir.as_deref());
            hash_opt_bytes(hasher, stdin.as_deref());
        }
        TaskSpec::ShellScript {
            script,
            interpreter,
            env,
        } => {
            hasher.update(&[1]);
            hash_str(hasher, script);
            hash_opt_str(hasher, interpreter.as_deref());
            hash_str_map(hasher, env);
        }
        TaskSpec::RustCompilation {
            crate_name,
            source_files,
            compiler_flags,
            target_dir,
        } => {
            hasher.update(&[2]);
            hash_str(hasher, crate_name);
            hash_str_map(hasher, source_files);
            hash_str_slice(hasher, compiler_flags);
            hash_opt_path(hasher, target_dir.as_deref());
        }
        TaskSpec::GpuCompute {
            kernel_name,
            input_data,
            work_group_size,
            simulated_matrix_dim,
        } => {
            hasher.update(&[3]);
            hash_str(hasher, kernel_name);
            hash_bytes(hasher, input_data);
            hash_u32(hasher, *work_group_size);
            hash_u32(hasher, *simulated_matrix_dim);
        }
        TaskSpec::BuiltinTest {
            test_name,
            iterations,
            duration_ms,
            should_fail,
            require_gpu,
        } => {
            hasher.update(&[4]);
            hash_str(hasher, test_name);
            hash_u32(hasher, *iterations);
            hash_u64(hasher, *duration_ms);
            hash_bool(hasher, *should_fail);
            hash_bool(hasher, *require_gpu);
        }
        TaskSpec::Generative3D(g) => {
            hasher.update(&[5]);
            hash_opt_str(hasher, g.prompt.as_deref());
            hash_opt_bytes(hasher, g.image_data.as_ref().map(|b| b.as_bytes()));
            hash_str(hasher, &g.quality);
            hash_opt_str(hasher, g.engine_preference.as_deref());
            hash_bool(hasher, g.export_texture);
        }
        TaskSpec::AutoRig(a) => {
            hasher.update(&[6]);
            hash_bytes(hasher, a.input_mesh.as_bytes());
            hash_str(hasher, &a.rig_preset);
            hash_str_slice(hasher, &a.embed_animations);
            hash_opt_str(hasher, a.preferred_provider.as_deref());
        }
    }
}

fn hash_inputs(hasher: &mut blake3::Hasher, inputs: &[TaskInput]) {
    #[derive(Ord, PartialOrd, Eq, PartialEq)]
    struct SortKey<'a> {
        path: &'a str,
        kind: u8,
        digest: [u8; 32],
        size: u64,
        executable: bool,
    }

    let mut canonical: Vec<SortKey<'_>> = inputs
        .iter()
        .map(|inp| match inp.source {
            InputSource::File {
                id,
                size,
                executable,
            } => SortKey {
                path: inp.path.as_str(),
                kind: 0,
                digest: id.0,
                size,
                executable,
            },
            InputSource::Tree {
                manifest,
                manifest_size,
            } => SortKey {
                path: inp.path.as_str(),
                kind: 1,
                digest: manifest.0,
                size: manifest_size,
                executable: false,
            },
        })
        .collect();
    canonical.sort_unstable();

    hash_u64(hasher, canonical.len() as u64);
    for entry in canonical {
        hash_str(hasher, entry.path);
        hasher.update(&[entry.kind]);
        hasher.update(&entry.digest);
        hash_u64(hasher, entry.size);
        hash_bool(hasher, entry.executable);
    }
}

fn hash_staged_files(hasher: &mut blake3::Hasher, staged: &[super::StagedFile]) {
    let mut entries: Vec<&super::StagedFile> = staged.iter().collect();
    entries.sort_unstable_by_key(|f| {
        (
            f.path.as_str(),
            f.executable,
            f.size,
            &f.sha256,
            f.data.as_bytes(),
        )
    });

    hash_u64(hasher, entries.len() as u64);
    for f in entries {
        hash_str(hasher, &f.path);
        hash_bool(hasher, f.executable);
        hash_u64(hasher, f.size);
        hasher.update(&f.sha256);
        hash_bytes(hasher, f.data.as_bytes());
    }
}

fn hash_workspace_source(hasher: &mut blake3::Hasher, source: Option<&WorkspaceSource>) {
    match source {
        Some(WorkspaceSource::Tree {
            manifest,
            manifest_size,
        }) => {
            hasher.update(&[1, 0]);
            hasher.update(&manifest.0);
            hash_u64(hasher, *manifest_size);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_placement_and_hardware(hasher: &mut blake3::Hasher, task: &Task) {
    match task.scheduling_placement() {
        Ok(placement) => {
            hasher.update(&[1]);
            hash_opt_str(hasher, placement.os.as_deref());
            hash_opt_str(hasher, placement.arch.as_deref());
            hash_opt_str(hasher, placement.worker.as_deref());
            hash_opt_str(hasher, placement.isolation.map(|iso| iso.as_str()));
            hash_str_slice(hasher, &placement.tools);
            hash_str_slice(hasher, &placement.tags);
            hash_u64(hasher, placement.extensions.len() as u64);
            for ext_id in &placement.extensions {
                hash_u16(hasher, *ext_id);
            }
        }
        Err(_) => {
            hasher.update(&[0]);
            let mut tags: Vec<&str> = task
                .tags
                .iter()
                .map(String::as_str)
                .filter(|t| *t != "opt:repeatable")
                .collect();
            tags.sort_unstable();
            tags.dedup();
            hash_u64(hasher, tags.len() as u64);
            for t in tags {
                hash_str(hasher, t);
            }
        }
    }

    hash_bool(hasher, task.requirements.gpu_required);
    hash_opt_u64(hasher, task.requirements.vram_mb);
}

fn hash_non_cache_extensions(hasher: &mut blake3::Hasher, extensions: &[Extension]) {
    let mut entries: Vec<(u16, bool, &[u8])> = extensions
        .iter()
        .filter(|ext| ext.id != ACTION_CACHE_EXTENSION_ID)
        .map(|ext| (ext.id, ext.critical, ext.bytes.as_slice()))
        .collect();
    entries.sort_unstable();

    hash_u64(hasher, entries.len() as u64);
    for (id, critical, bytes) in entries {
        hash_u16(hasher, id);
        hash_bool(hasher, critical);
        hash_bytes(hasher, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobId;
    use crate::placement::{IsolationLevel, Placement};
    use crate::task::{StagedFile, TaskRequirements, WorkspaceRef};

    fn sample_cached_task() -> Task {
        let mut env = HashMap::new();
        env.insert("ALPHA".into(), "1".into());
        env.insert("BETA".into(), "2".into());

        let mut task = Task::new(
            TaskSpec::Command {
                program: "python3".into(),
                args: vec!["build.py".into(), "--fast".into()],
                env,
                working_dir: Some("work".into()),
                stdin: Some(b"hello".to_vec()),
            },
            TaskRequirements::new(2, 1024, false, 120),
        );
        task.inputs
            .push(TaskInput::file("a.txt", BlobId::of(b"aaa"), 3, false));
        task.inputs
            .push(TaskInput::file("b.txt", BlobId::of(b"bbb"), 3, true));
        task.staged_files
            .push(StagedFile::new("one.txt", b"111".to_vec(), false));
        task.staged_files
            .push(StagedFile::new("two.txt", b"222".to_vec(), true));
        task.output_globs = vec!["out/*.bin".into(), "dist/**".into()];
        task.expected_outputs = vec!["out/result.bin".into()];
        task.with_action_cache()
    }

    #[test]
    fn key_hex_round_trips() {
        let key = compute_action_cache_key(&sample_cached_task());
        let hex = key.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(ActionCacheKey::from_hex(&hex).unwrap(), key);
        assert!(ActionCacheKey::from_hex("abcd").is_err());
    }

    #[test]
    fn key_ignores_volatile_fields_and_scheduling_hints() {
        let a = sample_cached_task();
        let mut b = a.clone();

        b.id = crate::task::TaskId::new();
        b.created_at_utc = a.created_at_utc.saturating_add(9999);
        b.requirements.timeout_secs = 900;
        b.requirements.cpu_cores = 16;
        b.requirements.ram_mb = 32_768;
        b.requirements.max_retries = Some(5);
        b.tags.push("opt:repeatable".into());
        b.tags.push("user-label-ignored".into());
        b.strip_action_cache();
        b.workspace = Some(WorkspaceRef::new("other-name"));

        assert_eq!(compute_action_cache_key(&a), compute_action_cache_key(&b));
    }

    #[test]
    fn key_is_independent_of_input_staged_env_and_output_order() {
        let a = sample_cached_task();
        let mut b = a.clone();

        b.inputs.reverse();
        b.staged_files.reverse();
        b.output_globs.reverse();
        b.output_globs.push("dist/**".into());
        b.expected_outputs.push("out/result.bin".into());

        assert_eq!(compute_action_cache_key(&a), compute_action_cache_key(&b));
    }

    #[test]
    fn key_changes_on_every_deterministic_dimension() {
        let base = sample_cached_task();
        let base_key = compute_action_cache_key(&base);

        // Spec command/args/env
        let mut t = base.clone();
        if let TaskSpec::Command { ref mut args, .. } = t.spec {
            args.push("--extra".into());
        }
        assert_ne!(compute_action_cache_key(&t), base_key);

        let mut t = base.clone();
        if let TaskSpec::Command { ref mut env, .. } = t.spec {
            env.insert("ALPHA".into(), "2".into());
        }
        assert_ne!(compute_action_cache_key(&t), base_key);

        // Inputs & staged files
        let mut t = base.clone();
        t.inputs[0] = TaskInput::file("a.txt", BlobId::of(b"changed"), 7, false);
        assert_ne!(compute_action_cache_key(&t), base_key);

        let mut t = base.clone();
        t.staged_files[0] = StagedFile::new("one.txt", b"changed".to_vec(), false);
        assert_ne!(compute_action_cache_key(&t), base_key);

        // Workspace source tree
        let mut t = base.clone();
        t.workspace = Some(WorkspaceRef::new("ws").with_tree(BlobId::of(b"tree"), 42));
        assert_ne!(compute_action_cache_key(&t), base_key);

        // Output globs & expected outputs
        let mut t = base.clone();
        t.output_globs.push("logs/*.txt".into());
        assert_ne!(compute_action_cache_key(&t), base_key);

        let mut t = base.clone();
        t.expected_outputs.push("out/other.bin".into());
        assert_ne!(compute_action_cache_key(&t), base_key);

        // Placement & hardware constraints
        let p = Placement {
            os: Some("linux".into()),
            ..Default::default()
        };
        let t = base.clone().with_placement(&p);
        assert_ne!(compute_action_cache_key(&t), base_key);

        let p_iso = Placement {
            isolation: Some(IsolationLevel::FsNet),
            ..Default::default()
        };
        let t = base.clone().with_placement(&p_iso);
        assert_ne!(compute_action_cache_key(&t), base_key);

        let mut t = base.clone();
        t.requirements.gpu_required = true;
        assert_ne!(compute_action_cache_key(&t), base_key);

        let mut t = base.clone();
        t.requirements.vram_mb = Some(4096);
        assert_ne!(compute_action_cache_key(&t), base_key);
    }
}
