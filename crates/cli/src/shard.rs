//! Client-side sharded test execution (`oxideswarm shard`) and Rust `--build-once` mode
//! (`AX-2`, `docs/design/0103-ax2-sharded-runs.md`).
//!
//! Orchestrates `N` parallel shard tasks over Protocol 10 (`ListWorkers`, `SubmitTask`,
//! `CancelTask`, `BlobRequest`/`BlobReply`) without changing the wire format or master store:
//! - Pre-flight `ListWorkers` filtering fails immediately without uploading blobs when 0 eligible
//!   workers match.
//! - Smooth Weighted Round-Robin (`spread_shards_swrr`) weighted by `slots:<n>` tags (falling back
//!   to `cpu_cores`) pins each shard so work spreads proportionally across eligible workers and
//!   preserves `(os, arch)` archive affinity in `--build-once` mode.
//! - Injects 1-based `OX_SHARD_INDEX` (`1..=N`) and `OX_SHARD_TOTAL` (`N`) into process tasks.
//! - In `--build-once` mode, scans the local Rust tree once with `.gitignore` awareness (skipping
//!   `.git` and top-level `target/`), uploads a clean `TreeManifest` v1 once, runs 1
//!   `cargo nextest archive` builder per distinct `(os, arch)` group, and passes the archive
//!   `BlobFile` alongside the source `TreeManifest` in each shard's `Task::inputs` (never
//!   `Task::workspace`).
//! - Includes [`junit`], a zero-dependency `std` JUnit XML parser, merger, and symlink-safe atomic
//!   writer.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use uuid::Uuid;

use rusty_grid_core::blob::{
    hash_reader, open_regular_file_nofollow, validate_blob_path, BlobFile, BlobId, OutputPattern,
    TaskInput, TreeManifest,
};
use rusty_grid_core::blob_transfer::{for_each_concurrently, missing_blobs, upload_blob};
use rusty_grid_core::placement::{
    advertised_slots, listed_worker_id, normalize_arch, normalize_os, tag_value, Placement,
    ARCH_TAG_PREFIX, OS_TAG_PREFIX, REQUIRE_PREFIX, TOOL_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{StagedFile, Task, TaskId, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::WorkerCapabilities;

use crate::blobs::{BlobConnection, UploadReport, WorkspaceLimits};
use crate::matrix::{listed_as_connected, safe_deadline, worker_matches_cell, CellStatus};
use crate::mcp::submit::{
    escaped_str_bytes, split_stream_budgets, stream_was_worker_truncated, truncate_stream_for_llm,
};
use crate::run_summary::RunSummary;

/// Hard upper bound on shard count (`--count`).
pub const HARD_MAX_SHARDS: usize = 256;

/// Default per-shard inline stdout/stderr escaped-JSON byte budget for CLI `--json`.
pub const DEFAULT_SHARD_OUTPUT_BYTES: usize = crate::matrix::DEFAULT_CELL_OUTPUT_BYTES;

/// Default task timeout in seconds for regular `oxideswarm shard`.
pub const DEFAULT_SHARD_TIMEOUT_SECS: u64 = 60;

/// Default task timeout in seconds for `oxideswarm shard --build-once` (compilation + test).
pub const DEFAULT_BUILD_ONCE_TIMEOUT_SECS: u64 = 1800;

/// Default sandbox-relative glob for JUnit XML collection when `--junit-out` is set.
pub const DEFAULT_JUNIT_GLOB: &str = "**/junit*.xml";

/// Sandbox-relative config path staged when `--build-once` + `--junit-out` are used.
pub const BUILD_ONCE_NEXTEST_CONFIG_REL: &str = ".oxideswarm-nextest.toml";

/// Sandbox-relative JUnit filename written by `cargo nextest` via `--tool-config-file`.
pub const BUILD_ONCE_NEXTEST_JUNIT_REL: &str = "oxideswarm-junit.xml";

/// Cap concurrent TCP/Noise handshakes under the master's default `max_pending_handshakes` (64).
const MAX_CONCURRENT_HANDSHAKES: usize = 32;

/// Soft worker-affinity tag prefix (`prefer:worker=<target_key>`), honored by the Master
/// scheduler while the preferred worker is connected and falling back automatically on disconnect.
pub const PREFER_WORKER_PREFIX: &str = "prefer:worker=";

/// Validates `--count` is within `1..=HARD_MAX_SHARDS`.
pub fn validate_shard_count(count: usize) -> Result<usize, String> {
    if count == 0 || count > HARD_MAX_SHARDS {
        return Err(format!(
            "--count / count must be between 1 and {HARD_MAX_SHARDS} (got {count})"
        ));
    }
    Ok(count)
}

/// Format a deterministic, filesystem-safe slug for shard `index` (`1..=total`).
pub fn shard_slug(index: usize, total: usize) -> String {
    let width = total.to_string().len().max(2);
    format!("shard-{index:0width$}")
}

/// Connected, eligible worker metadata extracted from `ListWorkers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleWorker {
    pub id: Option<Uuid>,
    pub name: String,
    pub target_key: String,
    pub slots: usize,
    pub os: String,
    pub arch: String,
    pub nextest_version: Option<String>,
    pub has_cargo_nextest: bool,
    pub has_cargo: bool,
}

fn extract_nextest_tag(tags: &[String]) -> (bool, Option<String>) {
    let mut has_plain = false;
    let mut versioned: Option<String> = None;
    for tag in tags {
        if let Some(tool) = tag.strip_prefix(TOOL_TAG_PREFIX) {
            if tool.eq_ignore_ascii_case("cargo-nextest") {
                has_plain = true;
            } else if let Some(ver) = tool.strip_prefix("cargo-nextest@") {
                if !ver.trim().is_empty() {
                    versioned = Some(ver.trim().to_string());
                }
            }
        }
    }
    (has_plain || versioned.is_some(), versioned)
}

/// Filters `workers` against `placement`, `reqs`, workspace capability, and (when `build_once` is
/// enabled) `tool:cargo-nextest*` / `tool:cargo` availability. Fails immediately when 0 eligible
/// workers match.
pub fn filter_eligible_workers(
    workers: &[WorkerCapabilities],
    placement: &Placement,
    reqs: &TaskRequirements,
    has_workspace: bool,
    build_once: bool,
) -> Result<Vec<EligibleWorker>, String> {
    let connected_count = workers.iter().filter(|w| listed_as_connected(w)).count();
    let mut eligible: Vec<EligibleWorker> = workers
        .iter()
        .filter(|w| worker_matches_cell(w, placement, reqs, has_workspace))
        .map(|w| {
            let id = listed_worker_id(w);
            let target_key = id.map(|u| u.to_string()).unwrap_or_else(|| w.name.clone());
            let slots = advertised_slots(w)
                .unwrap_or_else(|| w.cpu_cores.max(1))
                .max(1);
            let os = tag_value(&w.tags, OS_TAG_PREFIX)
                .map(normalize_os)
                .unwrap_or_else(|| "unknown".to_string());
            let arch = tag_value(&w.tags, ARCH_TAG_PREFIX)
                .map(normalize_arch)
                .unwrap_or_else(|| "unknown".to_string());
            let (has_cargo_nextest, nextest_version) = extract_nextest_tag(&w.tags);
            let has_cargo = w.tags.iter().any(|t| t.eq_ignore_ascii_case("tool:cargo"));
            EligibleWorker {
                id,
                name: w.name.clone(),
                target_key,
                slots,
                os,
                arch,
                nextest_version,
                has_cargo_nextest,
                has_cargo,
            }
        })
        .collect();

    if eligible.is_empty() {
        if connected_count == 0 {
            return Err("no connected workers are available in the cluster".to_string());
        }
        return Err(format!(
            "no connected worker matches shard placement ({placement}) and task requirements (checked {connected_count} connected worker(s))"
        ));
    }

    if build_once {
        if eligible.iter().any(|w| w.has_cargo_nextest) {
            eligible.retain(|w| w.has_cargo_nextest);
        } else if eligible.iter().any(|w| w.has_cargo) {
            eligible.retain(|w| w.has_cargo);
        }
    }

    eligible.sort_by(|a, b| {
        a.os.cmp(&b.os)
            .then_with(|| a.arch.cmp(&b.arch))
            .then_with(|| a.target_key.cmp(&b.target_key))
    });
    Ok(eligible)
}

/// Assignment of one shard (`1..=total`) to a specific eligible worker via SWRR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardAssignment {
    pub index: usize,
    pub total: usize,
    pub slug: String,
    pub worker_id: Option<Uuid>,
    pub worker_name: String,
    pub worker_target_key: String,
    pub os: String,
    pub arch: String,
    pub slots: usize,
    pub placement: Placement,
    pub preferred_worker: Option<String>,
}

/// Spreads `1..=count` shards across `eligible` workers using Smooth Weighted Round-Robin
/// weighted by each worker's `slots` (`advertised_slots`, falling back to `cpu_cores.max(1)`).
///
/// When `base_placement.worker` is unset (`None`), each assignment records a soft
/// `preferred_worker` (`chosen.target_key`) rather than hard-pinning `placement.worker`
/// (`require:worker=`), preserving proportional `slots:<n>` spreading while allowing Master
/// failover if a worker disconnects. When the caller explicitly pinned `--worker`,
/// `placement.worker` remains set and `preferred_worker` is `None`.
pub fn spread_shards_swrr(
    count: usize,
    eligible: &[EligibleWorker],
    base_placement: &Placement,
) -> Result<Vec<ShardAssignment>, String> {
    validate_shard_count(count)?;
    if eligible.is_empty() {
        return Err("cannot spread shards across zero eligible workers".to_string());
    }

    let weights: Vec<i64> = eligible.iter().map(|w| w.slots.max(1) as i64).collect();
    let total_weight: i64 = weights.iter().sum();
    let mut current_weight = vec![0i64; eligible.len()];
    let mut assignments = Vec::with_capacity(count);

    for index in 1..=count {
        let mut best_k = 0usize;
        let mut best_val = i64::MIN;
        for (k, weight) in weights.iter().enumerate() {
            current_weight[k] += *weight;
            if current_weight[k] > best_val {
                best_val = current_weight[k];
                best_k = k;
            }
        }
        current_weight[best_k] -= total_weight;

        let chosen = &eligible[best_k];
        let placement = base_placement.clone();
        let preferred_worker = if base_placement.worker.is_none() {
            Some(chosen.target_key.clone())
        } else {
            None
        };

        assignments.push(ShardAssignment {
            index,
            total: count,
            slug: shard_slug(index, count),
            worker_id: chosen.id,
            worker_name: chosen.name.clone(),
            worker_target_key: chosen.target_key.clone(),
            os: chosen.os.clone(),
            arch: chosen.arch.clone(),
            slots: chosen.slots,
            placement,
            preferred_worker,
        });
    }

    Ok(assignments)
}

/// Per-`(os, arch)` required cargo-nextest tool tag resolved in `--build-once` mode.
pub type PlatformToolMap = BTreeMap<(String, String), Option<String>>;

/// Filters `(os, arch)` worker groups in `--build-once` mode so each group uses a consistent
/// `cargo-nextest` version (selecting the version with highest aggregate slots when multiple exist)
/// before SWRR assignment.
pub fn normalize_build_once_workers(
    eligible: Vec<EligibleWorker>,
) -> Result<(Vec<EligibleWorker>, PlatformToolMap), String> {
    if eligible.is_empty() {
        return Err("no eligible workers for --build-once".to_string());
    }
    let mut by_platform: BTreeMap<(String, String), Vec<EligibleWorker>> = BTreeMap::new();
    for w in eligible {
        by_platform
            .entry((w.os.clone(), w.arch.clone()))
            .or_default()
            .push(w);
    }

    let mut filtered = Vec::new();
    let mut platform_tools: BTreeMap<(String, String), Option<String>> = BTreeMap::new();

    for ((os, arch), mut group) in by_platform {
        let mut ver_slots: BTreeMap<String, usize> = BTreeMap::new();
        for w in &group {
            if let Some(ref v) = w.nextest_version {
                *ver_slots.entry(v.clone()).or_default() += w.slots;
            }
        }

        let required_tool = if !ver_slots.is_empty() {
            let best_ver = ver_slots
                .into_iter()
                .max_by(|(v_a, s_a), (v_b, s_b)| s_a.cmp(s_b).then_with(|| v_a.cmp(v_b)))
                .map(|(v, _)| v)
                .expect("non-empty version map");
            group.retain(|w| w.nextest_version.as_deref() == Some(best_ver.as_str()));
            Some(format!("cargo-nextest@{best_ver}"))
        } else if group.iter().any(|w| w.has_cargo_nextest) {
            group.retain(|w| w.has_cargo_nextest);
            Some("cargo-nextest".to_string())
        } else if group.iter().any(|w| w.has_cargo) {
            group.retain(|w| w.has_cargo);
            Some("cargo".to_string())
        } else {
            None
        };

        platform_tools.insert((os, arch), required_tool);
        filtered.extend(group);
    }

    filtered.sort_by(|a, b| {
        a.os.cmp(&b.os)
            .then_with(|| a.arch.cmp(&b.arch))
            .then_with(|| a.target_key.cmp(&b.target_key))
    });
    Ok((filtered, platform_tools))
}

/// Validates that `source_dir` exists, is a directory, and contains `Cargo.toml`.
pub fn validate_build_source(source_dir: &Path) -> Result<(), String> {
    let meta = std::fs::symlink_metadata(source_dir)
        .map_err(|e| format!("--build-source '{}': {e}", source_dir.display()))?;
    if !meta.is_dir() {
        return Err(format!(
            "--build-source '{}': must be a directory",
            source_dir.display()
        ));
    }
    let cargo_toml = source_dir.join("Cargo.toml");
    let toml_meta = std::fs::metadata(&cargo_toml).map_err(|e| {
        format!(
            "--build-once requires a Cargo project with Cargo.toml in '{}': {e}",
            source_dir.display()
        )
    })?;
    if !toml_meta.is_file() {
        return Err(format!(
            "--build-once requires '{}' to be a regular file",
            cargo_toml.display()
        ));
    }
    Ok(())
}

/// A scanned Rust source tree for `--build-once`, packaged as a clean `TreeManifest` v1.
#[derive(Clone)]
pub struct BuildSourceScan {
    pub manifest: TreeManifest,
    pub manifest_id: BlobId,
    pub manifest_len: u64,
    pub files: usize,
    pub bytes: u64,
    pub skipped: Vec<String>,
    blobs: HashMap<BlobId, BuildLocalBlob>,
}

#[derive(Clone)]
enum BuildLocalSource {
    File(PathBuf),
    Bytes(Vec<u8>),
}

#[derive(Clone)]
struct BuildLocalBlob {
    size: u64,
    source: BuildLocalSource,
}

fn is_executable_meta(meta: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        false
    }
}

/// Walks `root` with `.gitignore` awareness, explicitly skipping `.git` and top-level `target/`,
/// hashes regular files (skipping symlinks/special files), and encodes a `TreeManifest` v1.
pub fn scan_build_source(root: &Path, limits: WorkspaceLimits) -> Result<BuildSourceScan, String> {
    validate_build_source(root)?;

    let top_target = root.join("target");
    let walker = ignore::WalkBuilder::new(root)
        .hidden(false)
        .parents(false)
        .git_global(false)
        .require_git(false)
        .follow_links(false)
        .sort_by_file_path(|a, b| a.cmp(b))
        .filter_entry(move |e| e.file_name() != ".git" && e.path() != top_target)
        .build();

    let mut files = Vec::new();
    let mut skipped = Vec::new();
    let mut blobs: HashMap<BlobId, BuildLocalBlob> = HashMap::new();
    let mut file_count = 0usize;
    let mut total_bytes = 0u64;

    for entry in walker {
        let entry = entry.map_err(|e| format!("build source '{}': {e}", root.display()))?;
        if entry.depth() == 0 {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .ok_or_else(|| format!("build source '{}': unreadable entry", path.display()))?;
        let rel_path = path
            .strip_prefix(root)
            .map_err(|e| format!("build source '{}': {e}", path.display()))?;

        let mut parts = Vec::new();
        let mut valid = true;
        for c in rel_path.components() {
            match c {
                Component::Normal(s) => match s.to_str() {
                    Some(s) => parts.push(s),
                    None => valid = false,
                },
                _ => valid = false,
            }
        }
        if !valid {
            skipped.push(format!("{} (name is not UTF-8)", path.display()));
            continue;
        }
        let rel = parts.join("/");
        if rel == "target" || rel.starts_with("target/") {
            continue;
        }
        if let Err(e) = validate_blob_path(&rel) {
            skipped.push(format!("{} ({e})", path.display()));
            continue;
        }
        if file_type.is_dir() {
            continue;
        }
        if file_type.is_symlink() {
            skipped.push(format!("{} (symbolic links are skipped)", path.display()));
            continue;
        }
        if !file_type.is_file() {
            skipped.push(format!("{} (not a regular file)", path.display()));
            continue;
        }
        if file_count >= limits.max_files {
            return Err(format!(
                "build source '{}': more than {} files; add build artifacts to .gitignore",
                root.display(),
                limits.max_files
            ));
        }
        let (mut file, meta) = open_regular_file_nofollow(path)
            .map_err(|e| format!("build source '{}': {e}", path.display()))?;
        let len = meta.len();
        if total_bytes.saturating_add(len) > limits.max_bytes {
            return Err(format!(
                "build source '{}': exceeds {} bytes total limit at '{}'",
                root.display(),
                limits.max_bytes,
                path.display()
            ));
        }
        let remaining = limits.max_bytes - total_bytes;
        let (id, size) = hash_reader((&mut file).take(remaining.saturating_add(1)))
            .map_err(|e| format!("build source '{}': {e}", path.display()))?;
        if size > remaining {
            return Err(format!(
                "build source '{}': exceeds {} bytes total limit at '{}'",
                root.display(),
                limits.max_bytes,
                path.display()
            ));
        }
        let executable = is_executable_meta(&meta);
        file_count += 1;
        total_bytes = total_bytes.saturating_add(size);
        blobs.entry(id).or_insert_with(|| BuildLocalBlob {
            size,
            source: BuildLocalSource::File(path.to_path_buf()),
        });
        files.push(BlobFile {
            path: rel,
            id,
            size,
            executable,
        });
    }

    let manifest =
        TreeManifest::new(files).map_err(|e| format!("build source '{}': {e}", root.display()))?;
    let encoded = manifest.encode();
    let manifest_id = BlobId::of(&encoded);
    let manifest_len = encoded.len() as u64;
    blobs.insert(
        manifest_id,
        BuildLocalBlob {
            size: manifest_len,
            source: BuildLocalSource::Bytes(encoded),
        },
    );

    Ok(BuildSourceScan {
        manifest,
        manifest_id,
        manifest_len,
        files: file_count,
        bytes: total_bytes,
        skipped,
        blobs,
    })
}

/// Uploads any blobs of `scan` missing from the master.
pub async fn upload_build_source(
    conn: &BlobConnection,
    scan: &BuildSourceScan,
) -> Result<UploadReport, String> {
    let ids: Vec<BlobId> = scan.blobs.keys().copied().collect();
    let missing = missing_blobs(conn, &ids).await.map_err(|e| e.to_string())?;
    let uploaded = missing.len();
    let jobs: Vec<(BlobId, BuildLocalBlob)> = missing
        .into_iter()
        .map(|id| (id, scan.blobs[&id].clone()))
        .collect();
    let bytes = for_each_concurrently(jobs, |(id, blob)| async move {
        match blob.source {
            BuildLocalSource::File(path) => {
                let (std_file, _meta) = open_regular_file_nofollow(&path)?;
                let tokio_file = tokio::fs::File::from_std(std_file);
                upload_blob(conn, id, blob.size, tokio_file).await
            }
            BuildLocalSource::Bytes(bytes) => {
                upload_blob(conn, id, blob.size, std::io::Cursor::new(bytes)).await
            }
        }
    })
    .await
    .map_err(|e| e.to_string())?;

    Ok(UploadReport {
        blobs: ids.len(),
        uploaded,
        bytes,
    })
}

/// Checks that `candidate` does not collide with any path in `manifest` or `extra_inputs` (per
/// `crates/worker/src/staging.rs:106`).
pub fn check_build_once_path_collision(
    candidate: &str,
    manifest: &TreeManifest,
    extra_inputs: &[TaskInput],
) -> Result<(), String> {
    validate_blob_path(candidate).map_err(|e| format!("build-once path '{candidate}': {e}"))?;
    if manifest.files().iter().any(|f| f.path == candidate) {
        return Err(format!(
            "build-once sandbox path '{candidate}' collides with a file in the scanned source tree"
        ));
    }
    if extra_inputs.iter().any(|i| i.path == candidate) {
        return Err(format!(
            "build-once sandbox path '{candidate}' collides with a staged --input path"
        ));
    }
    Ok(())
}

/// Picks a canonical non-colliding path in the sandbox starting from `(stem, ext)`.
pub fn choose_non_colliding_path(
    stem: &str,
    ext: &str,
    manifest: &TreeManifest,
    extra_inputs: &[TaskInput],
    reserved: &BTreeSet<String>,
) -> Result<String, String> {
    let first = if ext.is_empty() {
        stem.to_string()
    } else {
        format!("{stem}.{ext}")
    };
    if !reserved.contains(&first)
        && check_build_once_path_collision(&first, manifest, extra_inputs).is_ok()
    {
        return Ok(first);
    }
    for n in 1..=32 {
        let cand = if ext.is_empty() {
            format!("{stem}-{n}")
        } else {
            format!("{stem}-{n}.{ext}")
        };
        if !reserved.contains(&cand)
            && check_build_once_path_collision(&cand, manifest, extra_inputs).is_ok()
        {
            return Ok(cand);
        }
    }
    Err(format!(
        "could not find a non-colliding sandbox path for '{first}'"
    ))
}

/// One `(os, arch)` build-once archive group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOnceGroup {
    pub os: String,
    pub arch: String,
    pub archive_rel: String,
    pub required_tool: Option<String>,
    pub pinned_worker: Option<String>,
}

/// Plans one builder task per distinct `(os, arch)` present in `assignments`, reserving collision-free
/// archive paths and optional nextest config path.
pub fn plan_build_once_groups(
    assignments: &[ShardAssignment],
    platform_tools: &BTreeMap<(String, String), Option<String>>,
    user_pinned_worker: Option<&str>,
    manifest: &TreeManifest,
    extra_inputs: &[TaskInput],
    need_junit_config: bool,
) -> Result<(Vec<BuildOnceGroup>, Option<String>), String> {
    let mut distinct: BTreeSet<(String, String)> = BTreeSet::new();
    for a in assignments {
        distinct.insert((a.os.clone(), a.arch.clone()));
    }

    let mut reserved = BTreeSet::new();
    let config_rel = if need_junit_config {
        let cfg = choose_non_colliding_path(
            ".oxideswarm-nextest",
            "toml",
            manifest,
            extra_inputs,
            &reserved,
        )?;
        reserved.insert(cfg.clone());
        Some(cfg)
    } else {
        None
    };

    let mut groups = Vec::with_capacity(distinct.len());
    for (os, arch) in distinct {
        let safe_os: String = os
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        let safe_arch: String = arch
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        let stem = format!(".oxideswarm-build-{safe_os}-{safe_arch}");
        let archive_rel =
            choose_non_colliding_path(&stem, "tar.zst", manifest, extra_inputs, &reserved)?;
        reserved.insert(archive_rel.clone());

        let required_tool = platform_tools
            .get(&(os.clone(), arch.clone()))
            .cloned()
            .flatten();
        groups.push(BuildOnceGroup {
            os,
            arch,
            archive_rel,
            required_tool,
            pinned_worker: user_pinned_worker.map(str::to_string),
        });
    }

    Ok((groups, config_rel))
}

/// Completed `(os, arch)` archive build outcome.
#[derive(Debug, Clone)]
pub struct BuildOnceOutcome {
    pub os: String,
    pub arch: String,
    pub archive_rel: String,
    pub archive_blob: BlobFile,
    pub task_id: TaskId,
    pub worker_id: Uuid,
    pub worker_name: Option<String>,
    pub duration_ms: u64,
    pub required_tool: Option<String>,
}

impl BuildOnceOutcome {
    pub fn to_json(&self) -> Value {
        json!({
            "os": self.os,
            "arch": self.arch,
            "archive_path": self.archive_rel,
            "archive_blob_id": self.archive_blob.id.to_hex(),
            "archive_size_bytes": self.archive_blob.size,
            "task_id": self.task_id.to_string(),
            "worker_id": self.worker_id.to_string(),
            "worker_name": self.worker_name,
            "duration_ms": self.duration_ms,
        })
    }
}

/// Runs one `cargo nextest archive` builder task per distinct `(os, arch)` group concurrently.
/// If any builder fails or omits its archive output blob, cancels all remaining in-flight builders
/// and returns an error before any shard is dispatched.
#[allow(clippy::too_many_arguments)]
pub async fn execute_build_once<C, Fut>(
    groups: &[BuildOnceGroup],
    manifest_input: &TaskInput,
    extra_inputs: &[TaskInput],
    base_placement: &Placement,
    requirements: &TaskRequirements,
    env: &HashMap<String, String>,
    repeatable: bool,
    workers: &[WorkerCapabilities],
    timeout: Duration,
    external_cancel_rx: Option<watch::Receiver<bool>>,
    connect: Arc<C>,
) -> Result<BTreeMap<(String, String), BuildOnceOutcome>, String>
where
    C: Fn() -> Fut + Send + Sync + 'static,
    Fut:
        std::future::Future<Output = Result<MessageTransport<GridStream>, String>> + Send + 'static,
{
    if groups.is_empty() {
        return Ok(BTreeMap::new());
    }

    let worker_names: Arc<BTreeMap<Uuid, String>> = Arc::new(
        workers
            .iter()
            .filter_map(|w| listed_worker_id(w).map(|id| (id, w.name.clone())))
            .collect(),
    );

    let (internal_cancel_tx, internal_cancel_rx) = watch::channel(false);
    if let Some(mut ext_rx) = external_cancel_rx {
        let tx = internal_cancel_tx.clone();
        tokio::spawn(async move {
            if *ext_rx.borrow() {
                let _ = tx.send(true);
                return;
            }
            while ext_rx.changed().await.is_ok() {
                if *ext_rx.borrow() {
                    let _ = tx.send(true);
                    break;
                }
            }
        });
    }

    let handshake_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
    let deadline = safe_deadline(timeout);
    let mut set = JoinSet::new();

    for group in groups {
        let mut placement = base_placement.clone();
        if group.os != "unknown" {
            placement.os = Some(group.os.clone());
        }
        if group.arch != "unknown" {
            placement.arch = Some(group.arch.clone());
        }
        placement.worker = group.pinned_worker.clone();
        if let Some(ref req_tool) = group.required_tool {
            if !placement.tools.iter().any(|t| t == req_tool) {
                placement.tools.push(req_tool.clone());
            }
        }

        let mut inputs = Vec::with_capacity(1 + extra_inputs.len());
        inputs.push(manifest_input.clone());
        inputs.extend(extra_inputs.iter().cloned());

        let spec = TaskSpec::Command {
            program: "cargo".to_string(),
            args: vec![
                "nextest".to_string(),
                "archive".to_string(),
                "--archive-file".to_string(),
                group.archive_rel.clone(),
            ],
            env: env.clone(),
            working_dir: None,
            stdin: None,
        };
        let mut task = Task::new(spec, requirements.clone())
            .with_placement(&placement)
            .with_inputs(inputs)
            .with_output_globs(vec![group.archive_rel.clone()]);
        if repeatable {
            task.tags
                .push(rusty_grid_core::placement::OPT_REPEATABLE_TAG.to_string());
        }

        let group = group.clone();
        let connect = Arc::clone(&connect);
        let sem = Arc::clone(&handshake_sem);
        let names = Arc::clone(&worker_names);
        let mut cancel_rx = internal_cancel_rx.clone();

        set.spawn(async move {
            let task_id = task.id;
            let outcome =
                run_single_task_wait(task, timeout, deadline, &mut cancel_rx, &sem, &*connect)
                    .await;

            match outcome {
                SingleTaskOutcome::Completed(result) => {
                    if !result.is_success() {
                        let stderr_trim = result.stderr_str().trim().to_string();
                        let stdout_trim = result.stdout_str().trim().to_string();
                        let detail = if !stderr_trim.is_empty() {
                            stderr_trim
                        } else if !stdout_trim.is_empty() {
                            stdout_trim
                        } else {
                            result.error.clone().unwrap_or_default()
                        };
                        return Err(format!(
                            "build-once archive failed for ({}, {}) with exit code {}: {}",
                            group.os, group.arch, result.exit_code, detail
                        ));
                    }
                    let Some(archive_blob) = result
                        .outputs
                        .iter()
                        .find(|b| b.path == group.archive_rel)
                        .cloned()
                    else {
                        return Err(format!(
                            "build-once archive for ({}, {}) did not produce expected output '{}'",
                            group.os, group.arch, group.archive_rel
                        ));
                    };
                    if archive_blob.size == 0 {
                        return Err(format!(
                            "build-once archive '{}' for ({}, {}) is empty (0 bytes)",
                            group.archive_rel, group.os, group.arch
                        ));
                    }
                    let worker_name = names.get(&result.worker_id).cloned();
                    Ok(BuildOnceOutcome {
                        os: group.os,
                        arch: group.arch,
                        archive_rel: group.archive_rel,
                        archive_blob,
                        task_id,
                        worker_id: result.worker_id,
                        worker_name,
                        duration_ms: result.execution_time_ms,
                        required_tool: group.required_tool,
                    })
                }
                SingleTaskOutcome::Failed { error, .. } => Err(format!(
                    "build-once archive failed for ({}, {}): {error}",
                    group.os, group.arch
                )),
            }
        });
    }

    let mut out = BTreeMap::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(res)) => {
                out.insert((res.os.clone(), res.arch.clone()), res);
            }
            Ok(Err(err)) => {
                let _ = internal_cancel_tx.send(true);
                while set.join_next().await.is_some() {}
                return Err(err);
            }
            Err(join_err) => {
                let _ = internal_cancel_tx.send(true);
                while set.join_next().await.is_some() {}
                return Err(format!("build-once task panicked: {join_err}"));
            }
        }
    }

    Ok(out)
}

/// Injects `OX_SHARD_INDEX` (`1..=total`) and `OX_SHARD_TOTAL` (`total`), plus optional
/// `OXIDESWARM_BUILD_ONCE_ARCHIVE`, into process tasks, overriding conflicting user `--env` keys.
pub fn inject_shard_env(
    spec: &mut TaskSpec,
    index: usize,
    total: usize,
    build_once_archive: Option<&str>,
) {
    match spec {
        TaskSpec::Command { env, .. } | TaskSpec::ShellScript { env, .. } => {
            env.insert("OX_SHARD_INDEX".to_string(), index.to_string());
            env.insert("OX_SHARD_TOTAL".to_string(), total.to_string());
            if let Some(archive_rel) = build_once_archive {
                env.insert(
                    "OXIDESWARM_BUILD_ONCE_ARCHIVE".to_string(),
                    archive_rel.to_string(),
                );
            }
        }
        _ => {}
    }
}

/// Builds a shard `Task` from `template` and `assignment`, preserving non-`require:` tags (e.g.
/// `opt:repeatable`), injecting `OX_SHARD_INDEX`/`OX_SHARD_TOTAL`, and attaching `--build-once`
/// archive inputs and `cargo nextest run` args when `build_once_mode` is enabled.
#[allow(clippy::too_many_arguments)]
pub fn build_shard_task(
    template: &Task,
    assignment: &ShardAssignment,
    build_once_archive: Option<(&str, &BlobFile)>,
    nextest_config_file: Option<&StagedFile>,
    extra_args: &[String],
    build_once_mode: bool,
) -> Task {
    let mut task = template.clone();
    task.id = TaskId::new();
    task.tags
        .retain(|t| !t.starts_with(REQUIRE_PREFIX) && !t.starts_with(PREFER_WORKER_PREFIX));
    task.tags.extend(assignment.placement.to_task_tags());
    if assignment.placement.worker.is_none() {
        if let Some(ref pw) = assignment.preferred_worker {
            task.tags.push(format!("{PREFER_WORKER_PREFIX}{pw}"));
        }
    }

    let archive_rel_opt = build_once_archive.map(|(rel, _)| rel);
    if build_once_mode {
        if let Some((archive_rel, archive_blob)) = build_once_archive {
            debug_assert!(task.workspace.is_none());
            task.inputs.push(TaskInput::file(
                archive_rel,
                archive_blob.id,
                archive_blob.size,
                archive_blob.executable,
            ));
            let mut nextest_args = vec![
                "nextest".to_string(),
                "run".to_string(),
                "--archive-file".to_string(),
                archive_rel.to_string(),
                "--workspace-remap".to_string(),
                ".".to_string(),
                "--partition".to_string(),
                format!("slice:{}/{}", assignment.index, assignment.total),
            ];
            if let Some(cfg) = nextest_config_file {
                let has_custom_cfg = extra_args
                    .iter()
                    .any(|a| a == "--tool-config-file" || a.starts_with("--tool-config-file="));
                if !has_custom_cfg {
                    nextest_args.push("--tool-config-file".to_string());
                    nextest_args.push(format!("oxideswarm:{}", cfg.path));
                }
                if !task.staged_files.iter().any(|f| f.path == cfg.path) {
                    task.staged_files.push(cfg.clone());
                }
            }
            nextest_args.extend(extra_args.iter().cloned());
            if let TaskSpec::Command {
                ref mut program,
                ref mut args,
                ..
            } = task.spec
            {
                *program = "cargo".to_string();
                *args = nextest_args;
            }
        }
    }

    inject_shard_env(
        &mut task.spec,
        assignment.index,
        assignment.total,
        archive_rel_opt,
    );
    task
}

/// Progress notification emitted by [`execute_shards`].
#[derive(Debug, Clone)]
pub enum ShardProgressEvent {
    Submitted {
        shard_index: usize,
        task_id: TaskId,
    },
    Finished {
        shard_index: usize,
        status: CellStatus,
    },
}

/// Raw execution outcome for one shard.
#[derive(Debug, Clone)]
pub struct ShardExecution {
    pub assignment: ShardAssignment,
    pub status: CellStatus,
    pub task_id: Option<TaskId>,
    pub worker_name: Option<String>,
    pub result: Option<TaskResult>,
    pub error: Option<String>,
    pub saved_outputs: Option<Vec<PathBuf>>,
    pub saved_artifacts: Option<Vec<PathBuf>>,
    pub download_error: Option<String>,
}

enum SingleTaskOutcome {
    Completed(Box<TaskResult>),
    Failed { status: CellStatus, error: String },
}

async fn send_best_effort_cancel<C, Fut>(connect: &C, task_id: TaskId)
where
    C: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<MessageTransport<GridStream>, String>> + Send,
{
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        if let Ok(mut transport) = connect().await {
            let _ = transport
                .send_msg(&ClientMessage::CancelTask { task_id })
                .await;
            let _ = transport.recv_msg::<ClientResponse>().await;
            transport.close().await;
        }
    })
    .await;
}

async fn wait_for_cancel(cancel_rx: &mut watch::Receiver<bool>) {
    if *cancel_rx.borrow() {
        return;
    }
    while cancel_rx.changed().await.is_ok() {
        if *cancel_rx.borrow() {
            return;
        }
    }
    std::future::pending::<()>().await
}

async fn run_single_task_wait<C, Fut>(
    task: Task,
    timeout: Duration,
    deadline: tokio::time::Instant,
    cancel_rx: &mut watch::Receiver<bool>,
    sem: &Semaphore,
    connect: &C,
) -> SingleTaskOutcome
where
    C: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<MessageTransport<GridStream>, String>> + Send,
{
    let task_id = task.id;
    if *cancel_rx.borrow() {
        return SingleTaskOutcome::Failed {
            status: CellStatus::Cancelled,
            error: format!("Task {task_id} cancelled by client"),
        };
    }

    let permit = tokio::select! {
        biased;
        () = wait_for_cancel(cancel_rx) => {
            return SingleTaskOutcome::Failed {
                status: CellStatus::Cancelled,
                error: format!("Task {task_id} cancelled by client"),
            };
        }
        res = tokio::time::timeout_at(deadline, sem.acquire()) => match res {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => {
                return SingleTaskOutcome::Failed {
                    status: CellStatus::Error,
                    error: "internal handshake semaphore closed".to_string(),
                };
            }
            Err(_) => {
                return SingleTaskOutcome::Failed {
                    status: CellStatus::TimedOut,
                    error: format!("timed out after {}s", timeout.as_secs()),
                };
            }
        }
    };

    let mut transport = tokio::select! {
        biased;
        () = wait_for_cancel(cancel_rx) => {
            drop(permit);
            return SingleTaskOutcome::Failed {
                status: CellStatus::Cancelled,
                error: format!("Task {task_id} cancelled by client"),
            };
        }
        res = tokio::time::timeout_at(deadline, connect()) => match res {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => {
                drop(permit);
                return SingleTaskOutcome::Failed {
                    status: CellStatus::Error,
                    error: e,
                };
            }
            Err(_) => {
                drop(permit);
                return SingleTaskOutcome::Failed {
                    status: CellStatus::TimedOut,
                    error: format!("timed out connecting to master after {}s", timeout.as_secs()),
                };
            }
        }
    };
    drop(permit);

    if *cancel_rx.borrow() {
        transport.close().await;
        return SingleTaskOutcome::Failed {
            status: CellStatus::Cancelled,
            error: format!("Task {task_id} cancelled by client"),
        };
    }

    let msg = ClientMessage::SubmitTask { task, wait: true };
    let send_outcome = tokio::select! {
        biased;
        () = wait_for_cancel(cancel_rx) => {
            drop(transport);
            send_best_effort_cancel(connect, task_id).await;
            return SingleTaskOutcome::Failed {
                status: CellStatus::Cancelled,
                error: format!("Task {task_id} cancelled by client"),
            };
        }
        res = tokio::time::timeout_at(deadline, transport.send_msg(&msg)) => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err((CellStatus::Error, format!("could not submit shard: {err}"))),
            Err(_) => Err((
                CellStatus::TimedOut,
                format!("timed out submitting shard after {}s", timeout.as_secs()),
            )),
        }
    };
    if let Err((status, error)) = send_outcome {
        transport.close().await;
        return SingleTaskOutcome::Failed { status, error };
    }

    if *cancel_rx.borrow() {
        drop(transport);
        send_best_effort_cancel(connect, task_id).await;
        return SingleTaskOutcome::Failed {
            status: CellStatus::Cancelled,
            error: format!("Task {task_id} cancelled by client"),
        };
    }
    tokio::select! {
        biased;
        () = wait_for_cancel(cancel_rx) => {
            drop(transport);
            send_best_effort_cancel(connect, task_id).await;
            SingleTaskOutcome::Failed {
                status: CellStatus::Cancelled,
                error: format!("Task {task_id} cancelled by client"),
            }
        }
        res = transport.recv_msg::<ClientResponse>() => {
            transport.close().await;
            match res {
                Ok(Some(ClientResponse::TaskCompleted { result, .. })) => {
                    SingleTaskOutcome::Completed(Box::new(result))
                }
                Ok(Some(ClientResponse::Error { message })) => SingleTaskOutcome::Failed {
                    status: CellStatus::Error,
                    error: message,
                },
                Ok(Some(other)) => SingleTaskOutcome::Failed {
                    status: CellStatus::Error,
                    error: format!("unexpected master response: {other:?}"),
                },
                Ok(None) => SingleTaskOutcome::Failed {
                    status: CellStatus::Error,
                    error: "master closed the connection before replying".to_string(),
                },
                Err(e) => SingleTaskOutcome::Failed {
                    status: CellStatus::Error,
                    error: format!("communication error: {e}"),
                },
            }
        }
        _ = tokio::time::sleep_until(deadline) => {
            transport.close().await;
            send_best_effort_cancel(connect, task_id).await;
            SingleTaskOutcome::Failed {
                status: CellStatus::TimedOut,
                error: format!("timed out after {}s", timeout.as_secs()),
            }
        }
    }
}

fn apply_build_once_shard_placement(assignment: &mut ShardAssignment, outcome: &BuildOnceOutcome) {
    if assignment.placement.worker.is_none() {
        if assignment.placement.os.is_none() && outcome.os != "unknown" {
            assignment.placement.os = Some(outcome.os.clone());
        }
        if assignment.placement.arch.is_none() && outcome.arch != "unknown" {
            assignment.placement.arch = Some(outcome.arch.clone());
        }
        if let Some(ref req_tool) = outcome.required_tool {
            if !assignment.placement.tools.iter().any(|t| t == req_tool) {
                assignment.placement.tools.push(req_tool.clone());
            }
        }
    }
}

/// Executes all planned shards concurrently via dedicated connections, returning one
/// [`ShardExecution`] per shard in ascending shard index order (`1..=N`).
///
/// Supports `--fail-fast` (cancelling all remaining shards via `CancelTask` on the first failure)
/// and external `cancel_rx` (e.g. Ctrl-C or MCP cancellation).
#[allow(clippy::too_many_arguments)]
pub async fn execute_shards<C, Fut>(
    assignments: Vec<ShardAssignment>,
    workers: &[WorkerCapabilities],
    template: &Task,
    build_once_outcomes: Option<&BTreeMap<(String, String), BuildOnceOutcome>>,
    nextest_config_file: Option<&StagedFile>,
    extra_args: &[String],
    timeout: Duration,
    fail_fast: bool,
    external_cancel_rx: Option<watch::Receiver<bool>>,
    progress_tx: Option<mpsc::UnboundedSender<ShardProgressEvent>>,
    connect: Arc<C>,
) -> Vec<ShardExecution>
where
    C: Fn() -> Fut + Send + Sync + 'static,
    Fut:
        std::future::Future<Output = Result<MessageTransport<GridStream>, String>> + Send + 'static,
{
    let worker_names: Arc<BTreeMap<Uuid, String>> = Arc::new(
        workers
            .iter()
            .filter_map(|w| listed_worker_id(w).map(|id| (id, w.name.clone())))
            .collect(),
    );

    let total = assignments.len();
    let fallback_assignments = assignments.clone();
    let mut outcomes: Vec<Option<ShardExecution>> = vec![None; total];
    let mut spawned_meta: HashMap<tokio::task::Id, (usize, TaskId)> = HashMap::with_capacity(total);
    let handshake_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
    let deadline = safe_deadline(timeout);
    let (internal_cancel_tx, internal_cancel_rx) = watch::channel(false);

    if let Some(mut ext_rx) = external_cancel_rx {
        let tx = internal_cancel_tx.clone();
        tokio::spawn(async move {
            if *ext_rx.borrow() {
                let _ = tx.send(true);
                return;
            }
            while ext_rx.changed().await.is_ok() {
                if *ext_rx.borrow() {
                    let _ = tx.send(true);
                    break;
                }
            }
        });
    }

    let build_once_mode = build_once_outcomes.is_some();
    let mut set = JoinSet::new();

    for mut assignment in assignments {
        let bo_outcome = build_once_outcomes
            .and_then(|map| map.get(&(assignment.os.clone(), assignment.arch.clone())));
        if let Some(outcome) = bo_outcome {
            apply_build_once_shard_placement(&mut assignment, outcome);
        }
        let bo_archive = bo_outcome.map(|o| (o.archive_rel.as_str(), &o.archive_blob));
        let task = build_shard_task(
            template,
            &assignment,
            bo_archive,
            nextest_config_file,
            extra_args,
            build_once_mode,
        );
        let task_id = task.id;
        let pos = assignment.index - 1;
        let connect = Arc::clone(&connect);
        let sem = Arc::clone(&handshake_sem);
        let names = Arc::clone(&worker_names);
        let mut cancel_rx = internal_cancel_rx.clone();
        let tx = progress_tx.clone();

        let abort_handle = set.spawn(async move {
            if let Some(ref ptx) = tx {
                let _ = ptx.send(ShardProgressEvent::Submitted {
                    shard_index: assignment.index,
                    task_id,
                });
            }

            let exec = match run_single_task_wait(
                task,
                timeout,
                deadline,
                &mut cancel_rx,
                &sem,
                &*connect,
            )
            .await
            {
                SingleTaskOutcome::Completed(result) => {
                    let status = if result.is_success() {
                        CellStatus::Completed
                    } else if result.exit_code == 130
                        && result
                            .error
                            .as_deref()
                            .is_some_and(|e| e.to_ascii_lowercase().contains("cancel"))
                    {
                        CellStatus::Cancelled
                    } else {
                        CellStatus::Failed
                    };
                    let worker_name = names.get(&result.worker_id).cloned();
                    let error = result.error.clone();
                    ShardExecution {
                        assignment,
                        status,
                        task_id: Some(task_id),
                        worker_name,
                        result: Some(*result),
                        error,
                        saved_outputs: None,
                        saved_artifacts: None,
                        download_error: None,
                    }
                }
                SingleTaskOutcome::Failed { status, error } => ShardExecution {
                    assignment,
                    status,
                    task_id: Some(task_id),
                    worker_name: None,
                    result: None,
                    error: Some(error),
                    saved_outputs: None,
                    saved_artifacts: None,
                    download_error: None,
                },
            };

            if let Some(ref ptx) = tx {
                let _ = ptx.send(ShardProgressEvent::Finished {
                    shard_index: exec.assignment.index,
                    status: exec.status,
                });
            }
            exec
        });
        spawned_meta.insert(abort_handle.id(), (pos, task_id));
    }

    drop(progress_tx);
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(exec) => {
                if fail_fast
                    && matches!(
                        exec.status,
                        CellStatus::Failed | CellStatus::TimedOut | CellStatus::Error
                    )
                {
                    let _ = internal_cancel_tx.send(true);
                }
                let pos = exec.assignment.index - 1;
                outcomes[pos] = Some(exec);
            }
            Err(join_err) => {
                if fail_fast {
                    let _ = internal_cancel_tx.send(true);
                }
                if let Some((pos, task_id)) = spawned_meta.get(&join_err.id()).copied() {
                    outcomes[pos] = Some(ShardExecution {
                        assignment: fallback_assignments[pos].clone(),
                        status: CellStatus::Error,
                        task_id: Some(task_id),
                        worker_name: None,
                        result: None,
                        error: Some(format!("shard task panicked or was aborted: {join_err}")),
                        saved_outputs: None,
                        saved_artifacts: None,
                        download_error: None,
                    });
                }
            }
        }
    }

    outcomes
        .into_iter()
        .zip(fallback_assignments)
        .map(|(item, assignment)| {
            item.unwrap_or_else(|| ShardExecution {
                assignment,
                status: CellStatus::Error,
                task_id: None,
                worker_name: None,
                result: None,
                error: Some("shard task ended without outcome".to_string()),
                saved_outputs: None,
                saved_artifacts: None,
                download_error: None,
            })
        })
        .collect()
}

/// Summary of a JUnit XML merge across shards.
#[derive(Debug, Clone)]
pub struct JunitSummary {
    pub output_path: String,
    pub tests: u64,
    pub failures: u64,
    pub errors: u64,
    pub skipped: u64,
    pub time_secs: f64,
    pub suites: usize,
    pub synthesized_shards: Vec<usize>,
    pub parse_errors: Vec<String>,
}

impl JunitSummary {
    pub fn to_json(&self) -> Value {
        json!({
            "output_path": self.output_path,
            "tests": self.tests,
            "failures": self.failures,
            "errors": self.errors,
            "skipped": self.skipped,
            "time_secs": self.time_secs,
            "suites": self.suites,
            "synthesized_shards": self.synthesized_shards,
            "parse_errors": self.parse_errors,
        })
    }
}

/// Structured per-shard report entry.
#[derive(Debug, Clone)]
pub struct ShardRecord {
    pub index: usize,
    pub total: usize,
    pub slug: String,
    pub assigned_worker: String,
    pub os: String,
    pub arch: String,
    pub slots: usize,
    pub status: CellStatus,
    pub task_id: Option<String>,
    pub worker: Option<String>,
    pub worker_id: Option<String>,
    pub worker_name: Option<String>,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
    pub worker_truncated: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub stdout_captured_bytes: usize,
    pub stderr_captured_bytes: usize,
    pub captured_bytes: usize,
    pub error: Option<String>,
    pub outputs: Vec<Value>,
    pub saved_outputs: Option<Vec<String>>,
    pub saved_artifacts: Option<Vec<String>>,
}

impl ShardRecord {
    pub fn to_json(&self) -> Value {
        let mut obj = json!({
            "index": self.index,
            "total": self.total,
            "slug": self.slug,
            "assigned_worker": self.assigned_worker,
            "os": self.os,
            "arch": self.arch,
            "slots": self.slots,
            "status": self.status.as_str(),
            "task_id": self.task_id,
            "worker": self.worker,
            "worker_id": self.worker_id,
            "worker_name": self.worker_name,
            "exit_code": self.exit_code,
            "duration_ms": self.duration_ms,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "truncated": self.truncated,
            "worker_truncated": self.worker_truncated,
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
            "stdout_captured_bytes": self.stdout_captured_bytes,
            "stderr_captured_bytes": self.stderr_captured_bytes,
            "captured_bytes": self.captured_bytes,
            "error": self.error,
            "outputs": self.outputs,
        });
        if let Some(ref saved) = self.saved_outputs {
            obj["saved_outputs"] = json!(saved);
        }
        if let Some(ref saved) = self.saved_artifacts {
            obj["saved_artifacts"] = json!(saved);
        }
        obj
    }
}

/// Summary metrics for an executed shard run.
#[derive(Debug, Clone)]
pub struct ShardSummary {
    pub total_shards: usize,
    pub completed_shards: usize,
    pub failed_shards: usize,
    pub timed_out_shards: usize,
    pub error_shards: usize,
    pub cancelled_shards: usize,
    pub machines: usize,
    pub waited_ms: u64,
    pub task_time_sum_ms: u64,
}

/// Aggregated report for `oxideswarm shard` and MCP `shard`.
#[derive(Debug, Clone)]
pub struct ShardReport {
    pub shards: Vec<ShardRecord>,
    pub summary: ShardSummary,
    pub build_once: Vec<BuildOnceOutcome>,
    pub junit: Option<JunitSummary>,
    pub junit_error: Option<String>,
    pub input_upload: Option<UploadReport>,
}

impl ShardReport {
    #[allow(clippy::too_many_arguments)]
    pub fn from_executions(
        executions: Vec<ShardExecution>,
        waited_ms: u64,
        max_shard_output_bytes: usize,
        build_once: Vec<BuildOnceOutcome>,
        junit: Option<JunitSummary>,
        junit_error: Option<String>,
        input_upload: Option<UploadReport>,
    ) -> Self {
        let mut completed_shards = 0usize;
        let mut failed_shards = 0usize;
        let mut timed_out_shards = 0usize;
        let mut error_shards = 0usize;
        let mut cancelled_shards = 0usize;
        let mut results_for_summary = Vec::new();
        let mut records = Vec::with_capacity(executions.len());

        for exec in executions {
            let status = if exec.download_error.is_some() && exec.status == CellStatus::Completed {
                CellStatus::Error
            } else {
                exec.status
            };
            match status {
                CellStatus::Completed => completed_shards += 1,
                CellStatus::Failed => failed_shards += 1,
                CellStatus::Unplaced | CellStatus::Error => error_shards += 1,
                CellStatus::TimedOut => timed_out_shards += 1,
                CellStatus::Cancelled => cancelled_shards += 1,
            }

            let task_id = exec.task_id.map(|id| id.to_string());
            let saved_outputs_strs = exec.saved_outputs.as_ref().map(|paths| {
                paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
            });
            let saved_artifacts_strs = exec.saved_artifacts.as_ref().map(|paths| {
                paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
            });

            let record = if let Some(result) = exec.result {
                results_for_summary.push(result.clone());
                let worker_id_str = if result.worker_id.is_nil() {
                    None
                } else {
                    Some(result.worker_id.to_string())
                };
                let worker_display = exec.worker_name.clone().or_else(|| worker_id_str.clone());

                let stdout_raw = result.stdout.as_bytes();
                let stderr_raw = result.stderr.as_bytes();
                let stdout_worker_truncated = stream_was_worker_truncated(stdout_raw);
                let stderr_worker_truncated = stream_was_worker_truncated(stderr_raw);
                let worker_truncated = stdout_worker_truncated || stderr_worker_truncated;

                let stdout_need = escaped_str_bytes(&String::from_utf8_lossy(stdout_raw));
                let stderr_need = escaped_str_bytes(&String::from_utf8_lossy(stderr_raw));
                let (stdout_budget, stderr_budget) =
                    split_stream_budgets(stdout_need, stderr_need, max_shard_output_bytes);
                let (stdout, stdout_mcp_truncated, stdout_captured_bytes) =
                    truncate_stream_for_llm(stdout_raw, stdout_budget);
                let (stderr, stderr_mcp_truncated, stderr_captured_bytes) =
                    truncate_stream_for_llm(stderr_raw, stderr_budget);
                let stdout_truncated = stdout_mcp_truncated || stdout_worker_truncated;
                let stderr_truncated = stderr_mcp_truncated || stderr_worker_truncated;

                let mut outputs = Vec::new();
                for (i, output) in result.outputs.iter().enumerate() {
                    let mut entry = json!({
                        "path": output.path,
                        "size_bytes": output.size,
                        "blob_id": output.id.to_hex(),
                        "checksum_blake3": output.id.to_hex(),
                        "executable": output.executable,
                    });
                    if let Some(saved) = saved_outputs_strs.as_ref().and_then(|v| v.get(i)) {
                        entry["saved_path"] = Value::String(saved.clone());
                    }
                    outputs.push(entry);
                }
                for (i, artifact) in result.artifacts.iter().enumerate() {
                    let mut entry = json!({
                        "path": artifact.path,
                        "size_bytes": artifact.size,
                        "checksum_sha256": artifact.sha256_hex(),
                    });
                    if let Some(saved) = saved_artifacts_strs.as_ref().and_then(|v| v.get(i)) {
                        entry["saved_path"] = Value::String(saved.clone());
                    }
                    outputs.push(entry);
                }

                let error = exec.download_error.or(exec.error).or(result.error);

                ShardRecord {
                    index: exec.assignment.index,
                    total: exec.assignment.total,
                    slug: exec.assignment.slug,
                    assigned_worker: exec.assignment.worker_name,
                    os: exec.assignment.os,
                    arch: exec.assignment.arch,
                    slots: exec.assignment.slots,
                    status,
                    task_id,
                    worker: worker_display,
                    worker_id: worker_id_str,
                    worker_name: exec.worker_name,
                    exit_code: Some(result.exit_code),
                    duration_ms: result.execution_time_ms,
                    stdout,
                    stderr,
                    truncated: stdout_truncated || stderr_truncated,
                    worker_truncated,
                    stdout_truncated,
                    stderr_truncated,
                    stdout_captured_bytes,
                    stderr_captured_bytes,
                    captured_bytes: stdout_captured_bytes + stderr_captured_bytes,
                    error,
                    outputs,
                    saved_outputs: saved_outputs_strs,
                    saved_artifacts: saved_artifacts_strs,
                }
            } else {
                ShardRecord {
                    index: exec.assignment.index,
                    total: exec.assignment.total,
                    slug: exec.assignment.slug,
                    assigned_worker: exec.assignment.worker_name,
                    os: exec.assignment.os,
                    arch: exec.assignment.arch,
                    slots: exec.assignment.slots,
                    status,
                    task_id,
                    worker: None,
                    worker_id: None,
                    worker_name: None,
                    exit_code: None,
                    duration_ms: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                    truncated: false,
                    worker_truncated: false,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    stdout_captured_bytes: 0,
                    stderr_captured_bytes: 0,
                    captured_bytes: 0,
                    error: exec.download_error.or(exec.error),
                    outputs: Vec::new(),
                    saved_outputs: saved_outputs_strs,
                    saved_artifacts: saved_artifacts_strs,
                }
            };
            records.push(record);
        }

        let run_summary = RunSummary::from_results(&results_for_summary, waited_ms);
        let summary = ShardSummary {
            total_shards: records.len(),
            completed_shards,
            failed_shards,
            timed_out_shards,
            error_shards,
            cancelled_shards,
            machines: run_summary.machines,
            waited_ms: run_summary.waited_ms,
            task_time_sum_ms: run_summary.task_time_sum_ms,
        };

        Self {
            shards: records,
            summary,
            build_once,
            junit,
            junit_error,
            input_upload,
        }
    }

    pub fn any_failed(&self) -> bool {
        self.summary.completed_shards < self.summary.total_shards
            || self.junit_error.is_some()
            || self
                .junit
                .as_ref()
                .is_some_and(|j| !j.parse_errors.is_empty())
    }

    pub fn exit_code(&self) -> i32 {
        if !self.any_failed() {
            return 0;
        }
        for s in &self.shards {
            if s.status != CellStatus::Completed {
                if let Some(code) = s.exit_code {
                    if code != 0 {
                        return code.clamp(1, 255);
                    }
                }
            }
        }
        1
    }

    pub fn to_json(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("shard".to_string(), Value::Bool(true));
        obj.insert(
            "status".to_string(),
            Value::String(if self.any_failed() {
                "failed".to_string()
            } else {
                "completed".to_string()
            }),
        );
        obj.insert("total_shards".to_string(), json!(self.summary.total_shards));
        obj.insert(
            "completed_shards".to_string(),
            json!(self.summary.completed_shards),
        );
        obj.insert(
            "failed_shards".to_string(),
            json!(self.summary.failed_shards),
        );
        obj.insert(
            "timed_out_shards".to_string(),
            json!(self.summary.timed_out_shards),
        );
        obj.insert("error_shards".to_string(), json!(self.summary.error_shards));
        obj.insert(
            "cancelled_shards".to_string(),
            json!(self.summary.cancelled_shards),
        );
        obj.insert("machines".to_string(), json!(self.summary.machines));
        obj.insert("waited_ms".to_string(), json!(self.summary.waited_ms));
        obj.insert(
            "task_time_sum_ms".to_string(),
            json!(self.summary.task_time_sum_ms),
        );
        obj.insert(
            "shards".to_string(),
            Value::Array(self.shards.iter().map(ShardRecord::to_json).collect()),
        );
        if !self.build_once.is_empty() {
            obj.insert(
                "build_once".to_string(),
                Value::Array(
                    self.build_once
                        .iter()
                        .map(BuildOnceOutcome::to_json)
                        .collect(),
                ),
            );
        }
        if let Some(ref j) = self.junit {
            obj.insert("junit".to_string(), j.to_json());
        }
        if let Some(ref je) = self.junit_error {
            obj.insert("junit_error".to_string(), json!(je));
        }
        if let Some(ref upload) = self.input_upload {
            obj.insert("input_upload".to_string(), json!(upload));
        }
        Value::Object(obj)
    }

    pub fn text(&self) -> String {
        let mut lines = Vec::new();
        if !self.build_once.is_empty() {
            for b in &self.build_once {
                let w = b
                    .worker_name
                    .clone()
                    .unwrap_or_else(|| b.worker_id.to_string());
                lines.push(format!(
                    "Build-once ({}/{}): {} ({} bytes, worker {}, {}ms)",
                    b.os, b.arch, b.archive_rel, b.archive_blob.size, w, b.duration_ms
                ));
            }
        }
        lines.push(format!(
            "Shards: {} completed, {} failed ({} total)",
            self.summary.completed_shards,
            self.summary.total_shards - self.summary.completed_shards,
            self.summary.total_shards
        ));
        for s in &self.shards {
            let mut detail = format!(
                "  [{}/{}] {} -> {}",
                s.index,
                s.total,
                s.slug,
                s.status.as_str()
            );
            if let Some(ref w) = s.worker {
                detail.push_str(&format!(" (worker {w}"));
                if let Some(code) = s.exit_code {
                    detail.push_str(&format!(", exit {code}"));
                }
                detail.push_str(&format!(", {}ms)", s.duration_ms));
            } else if let Some(ref err) = s.error {
                detail.push_str(&format!(": {err}"));
            }
            lines.push(detail);
            if !s.stdout.trim().is_empty() {
                lines.push(format!("      stdout: {}", s.stdout.trim()));
            }
            if !s.stderr.trim().is_empty() {
                lines.push(format!("      stderr: {}", s.stderr.trim()));
            }
            if s.worker.is_some() {
                if let Some(ref err) = s.error {
                    lines.push(format!("      error: {err}"));
                }
            }
        }
        if let Some(ref j) = self.junit {
            lines.push(format!(
                "JUnit: {} -> {} tests, {} failures, {} errors, {} skipped ({:.3}s)",
                j.output_path, j.tests, j.failures, j.errors, j.skipped, j.time_secs
            ));
        }
        if let Some(ref je) = self.junit_error {
            lines.push(format!("JUnit error: {je}"));
        }
        let non_completed = self.summary.total_shards - self.summary.completed_shards;
        let run_summary = RunSummary {
            finished: self.summary.completed_shards,
            failed: non_completed,
            machines: self.summary.machines,
            waited_ms: self.summary.waited_ms,
            task_time_sum_ms: self.summary.task_time_sum_ms,
        };
        lines.push(run_summary.tasks_line());
        lines.push(run_summary.waited_line());
        lines.join("\n")
    }
}

/// Checks if `blob_path` matches the configured JUnit pattern (`junit_glob` or default).
pub fn blob_matches_junit(blob_path: &str, patterns: &[OutputPattern]) -> bool {
    patterns.iter().any(|p| p.selects(blob_path))
}

/// Zero-dependency pure-`std` JUnit XML parser, merger, and symlink-safe atomic writer.
pub mod junit {
    use std::fmt::Write as _;
    use std::path::Path;

    use crate::matrix::CellStatus;

    /// Maximum JUnit XML input size per blob (64 MiB).
    const MAX_JUNIT_XML_BYTES: usize = 64 * 1024 * 1024;
    /// Maximum XML element nesting depth.
    const MAX_XML_DEPTH: usize = 256;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum XmlNode {
        Element(XmlElement),
        Text(String),
        CData(String),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct XmlElement {
        pub name: String,
        pub attrs: Vec<(String, String)>,
        pub children: Vec<XmlNode>,
    }

    impl XmlElement {
        pub fn new(name: impl Into<String>) -> Self {
            Self {
                name: name.into(),
                attrs: Vec::new(),
                children: Vec::new(),
            }
        }

        pub fn attr(&self, key: &str) -> Option<&str> {
            self.attrs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
        }

        pub fn set_attr(&mut self, key: impl Into<String>, val: impl Into<String>) {
            let key = key.into();
            let val = val.into();
            if let Some((_, existing)) = self.attrs.iter_mut().find(|(k, _)| *k == key) {
                *existing = val;
            } else {
                self.attrs.push((key, val));
            }
        }

        pub fn child_elements(&self) -> impl Iterator<Item = &XmlElement> {
            self.children.iter().filter_map(|n| match n {
                XmlNode::Element(el) => Some(el),
                _ => None,
            })
        }
    }

    struct Parser<'a> {
        src: &'a str,
        pos: usize,
    }

    impl<'a> Parser<'a> {
        fn new(src: &'a str) -> Self {
            let trimmed = src.strip_prefix('\u{FEFF}').unwrap_or(src);
            Self {
                src: trimmed,
                pos: 0,
            }
        }

        fn rest(&self) -> &'a str {
            &self.src[self.pos..]
        }

        fn is_eof(&self) -> bool {
            self.pos >= self.src.len()
        }

        fn skip_ws(&mut self) {
            while let Some(ch) = self.rest().chars().next() {
                if ch.is_ascii_whitespace() {
                    self.pos += ch.len_utf8();
                } else {
                    break;
                }
            }
        }

        fn starts_with(&self, pat: &str) -> bool {
            self.rest().starts_with(pat)
        }

        fn advance(&mut self, n: usize) {
            self.pos += n;
        }

        fn expect_char(&mut self, expected: char) -> Result<(), String> {
            match self.rest().chars().next() {
                Some(ch) if ch == expected => {
                    self.pos += ch.len_utf8();
                    Ok(())
                }
                Some(ch) => Err(format!(
                    "expected '{expected}' at byte {}, found '{ch}'",
                    self.pos
                )),
                None => Err(format!(
                    "unexpected end of XML while expecting '{expected}'"
                )),
            }
        }

        fn skip_misc(&mut self) -> Result<(), String> {
            loop {
                self.skip_ws();
                if self.starts_with("<?") {
                    self.advance(2);
                    let Some(end) = self.rest().find("?>") else {
                        return Err("unclosed XML processing instruction '<?...?>'".to_string());
                    };
                    self.advance(end + 2);
                } else if self.starts_with("<!--") {
                    self.advance(4);
                    let Some(end) = self.rest().find("-->") else {
                        return Err("unclosed XML comment '<!--...-->'".to_string());
                    };
                    self.advance(end + 3);
                } else if self.starts_with("<!DOCTYPE") {
                    self.skip_doctype()?;
                } else {
                    break;
                }
            }
            Ok(())
        }

        fn skip_doctype(&mut self) -> Result<(), String> {
            self.advance("<!DOCTYPE".len());
            let mut bracket_depth = 0usize;
            let mut in_quote: Option<char> = None;
            while let Some(ch) = self.rest().chars().next() {
                self.pos += ch.len_utf8();
                if let Some(q) = in_quote {
                    if ch == q {
                        in_quote = None;
                    }
                    continue;
                }
                match ch {
                    '"' | '\'' => in_quote = Some(ch),
                    '[' => bracket_depth += 1,
                    ']' => bracket_depth = bracket_depth.saturating_sub(1),
                    '>' if bracket_depth == 0 => return Ok(()),
                    _ => {}
                }
            }
            Err("unclosed <!DOCTYPE declaration".to_string())
        }

        fn parse_name(&mut self) -> Result<String, String> {
            let start = self.pos;
            while let Some(ch) = self.rest().chars().next() {
                if ch.is_ascii_alphanumeric()
                    || matches!(ch, '_' | '-' | ':' | '.')
                    || (!ch.is_ascii() && !ch.is_whitespace())
                {
                    self.pos += ch.len_utf8();
                } else {
                    break;
                }
            }
            if self.pos == start {
                return Err(format!("expected XML name at byte {}", self.pos));
            }
            Ok(self.src[start..self.pos].to_string())
        }

        fn parse_attr_value(&mut self) -> Result<String, String> {
            let quote = match self.rest().chars().next() {
                Some(q @ ('"' | '\'')) => {
                    self.pos += 1;
                    q
                }
                _ => {
                    return Err(format!(
                        "expected quote starting XML attribute value at byte {}",
                        self.pos
                    ))
                }
            };
            let start = self.pos;
            while let Some(ch) = self.rest().chars().next() {
                if ch == quote {
                    let raw = &self.src[start..self.pos];
                    self.pos += 1;
                    return decode_entities(raw);
                }
                if ch == '<' {
                    return Err(format!(
                        "unescaped '<' inside XML attribute value at byte {}",
                        self.pos
                    ));
                }
                self.pos += ch.len_utf8();
            }
            Err("unclosed XML attribute value".to_string())
        }

        fn parse_element(&mut self, depth: usize) -> Result<XmlElement, String> {
            if depth > MAX_XML_DEPTH {
                return Err(format!("XML nesting exceeds maximum depth {MAX_XML_DEPTH}"));
            }
            self.expect_char('<')?;
            let name = self.parse_name()?;
            let mut el = XmlElement::new(name.clone());

            loop {
                self.skip_ws();
                if self.starts_with("/>") {
                    self.advance(2);
                    return Ok(el);
                }
                if self.starts_with(">") {
                    self.advance(1);
                    break;
                }
                let attr_name = self.parse_name()?;
                self.skip_ws();
                self.expect_char('=')?;
                self.skip_ws();
                let attr_val = self.parse_attr_value()?;
                if el.attrs.iter().any(|(k, _)| k == &attr_name) {
                    return Err(format!(
                        "duplicate XML attribute '{attr_name}' on '<{name}>'"
                    ));
                }
                el.attrs.push((attr_name, attr_val));
            }

            loop {
                if self.is_eof() {
                    return Err(format!("unclosed XML element '<{name}>'"));
                }
                if self.starts_with("</") {
                    self.advance(2);
                    let close_name = self.parse_name()?;
                    if close_name != name {
                        return Err(format!(
                            "mismatched closing tag '</{close_name}>' for '<{name}>'"
                        ));
                    }
                    self.skip_ws();
                    self.expect_char('>')?;
                    return Ok(el);
                } else if self.starts_with("<![CDATA[") {
                    self.advance("<![CDATA[".len());
                    let Some(end) = self.rest().find("]]>") else {
                        return Err("unclosed <![CDATA[...]]> section".to_string());
                    };
                    let body = self.rest()[..end].to_string();
                    self.advance(end + 3);
                    el.children.push(XmlNode::CData(body));
                } else if self.starts_with("<!--") {
                    self.advance(4);
                    let Some(end) = self.rest().find("-->") else {
                        return Err("unclosed XML comment '<!--...-->'".to_string());
                    };
                    self.advance(end + 3);
                } else if self.starts_with("<?") {
                    self.advance(2);
                    let Some(end) = self.rest().find("?>") else {
                        return Err("unclosed XML processing instruction '<?...?>'".to_string());
                    };
                    self.advance(end + 2);
                } else if self.starts_with("<") {
                    let child = self.parse_element(depth + 1)?;
                    el.children.push(XmlNode::Element(child));
                } else {
                    let start = self.pos;
                    while let Some(ch) = self.rest().chars().next() {
                        if ch == '<' {
                            break;
                        }
                        self.pos += ch.len_utf8();
                    }
                    let text = decode_entities(&self.src[start..self.pos])?;
                    el.children.push(XmlNode::Text(text));
                }
            }
        }
    }

    /// Decodes standard XML entities (`&lt;`, `&gt;`, `&amp;`, `&quot;`, `&apos;`, `&#...;`,
    /// `&#x...;`). Unknown named entities are preserved or rejected cleanly.
    pub fn decode_entities(raw: &str) -> Result<String, String> {
        if !raw.contains('&') {
            return Ok(raw.to_string());
        }
        let mut out = String::with_capacity(raw.len());
        let mut rest = raw;
        while let Some(amp) = rest.find('&') {
            out.push_str(&rest[..amp]);
            let after_amp = &rest[amp + 1..];
            let Some(semi) = after_amp.find(';') else {
                return Err("unterminated XML entity reference '&...'".to_string());
            };
            let entity = &after_amp[..semi];
            match entity {
                "lt" => out.push('<'),
                "gt" => out.push('>'),
                "amp" => out.push('&'),
                "quot" => out.push('"'),
                "apos" => out.push('\''),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                    let hex = &entity[2..];
                    let code = u32::from_str_radix(hex, 16)
                        .map_err(|_| format!("invalid hex XML entity '&#x{hex};'"))?;
                    let ch = char::from_u32(code).ok_or_else(|| {
                        format!("invalid Unicode scalar in XML entity '&{entity};'")
                    })?;
                    out.push(ch);
                }
                _ if entity.starts_with('#') => {
                    let dec = &entity[1..];
                    let code = dec
                        .parse::<u32>()
                        .map_err(|_| format!("invalid decimal XML entity '&#{dec};'"))?;
                    let ch = char::from_u32(code).ok_or_else(|| {
                        format!("invalid Unicode scalar in XML entity '&{entity};'")
                    })?;
                    out.push(ch);
                }
                _ => {
                    return Err(format!("unknown XML entity '&{entity};'"));
                }
            }
            rest = &after_amp[semi + 1..];
        }
        out.push_str(rest);
        Ok(out)
    }

    fn sanitize_xml_text(s: &str) -> String {
        s.chars()
            .filter(|&c| {
                let code = c as u32;
                code == 0x09 || code == 0x0A || code == 0x0D || code >= 0x20
            })
            .collect()
    }

    fn escape_xml_text(s: &str) -> String {
        let clean = sanitize_xml_text(s);
        let mut out = String::with_capacity(clean.len());
        for ch in clean.chars() {
            match ch {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                _ => out.push(ch),
            }
        }
        out
    }

    fn escape_xml_attr(s: &str) -> String {
        let clean = sanitize_xml_text(s);
        let mut out = String::with_capacity(clean.len());
        for ch in clean.chars() {
            match ch {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                '"' => out.push_str("&quot;"),
                '\'' => out.push_str("&apos;"),
                _ => out.push(ch),
            }
        }
        out
    }

    fn serialize_element(el: &XmlElement, indent: usize, out: &mut String) {
        for _ in 0..indent {
            out.push_str("  ");
        }
        out.push('<');
        out.push_str(&el.name);
        for (k, v) in &el.attrs {
            let _ = write!(out, " {k}=\"{}\"", escape_xml_attr(v));
        }
        if el.children.is_empty() {
            out.push_str("/>\n");
            return;
        }
        out.push('>');

        let has_element_child = el.children.iter().any(|c| matches!(c, XmlNode::Element(_)));
        if has_element_child {
            out.push('\n');
            for child in &el.children {
                match child {
                    XmlNode::Element(sub) => serialize_element(sub, indent + 1, out),
                    XmlNode::Text(txt) => {
                        let trimmed = txt.trim();
                        if !trimmed.is_empty() {
                            for _ in 0..(indent + 1) {
                                out.push_str("  ");
                            }
                            out.push_str(&escape_xml_text(trimmed));
                            out.push('\n');
                        }
                    }
                    XmlNode::CData(raw) => {
                        for _ in 0..(indent + 1) {
                            out.push_str("  ");
                        }
                        let clean = sanitize_xml_text(raw).replace("]]>", "]]]]><![CDATA[>");
                        let _ = writeln!(out, "<![CDATA[{clean}]]>");
                    }
                }
            }
            for _ in 0..indent {
                out.push_str("  ");
            }
        } else {
            for child in &el.children {
                match child {
                    XmlNode::Text(txt) => out.push_str(&escape_xml_text(txt)),
                    XmlNode::CData(raw) => {
                        let clean = sanitize_xml_text(raw).replace("]]>", "]]]]><![CDATA[>");
                        let _ = write!(out, "<![CDATA[{clean}]]>");
                    }
                    XmlNode::Element(_) => unreachable!(),
                }
            }
        }
        let _ = writeln!(out, "</{}>", el.name);
    }

    /// Serializes an [`XmlElement`] tree into `out`.
    pub fn serialize_root(root: &XmlElement, out: &mut String) {
        serialize_element(root, 0, out);
    }

    /// Parses a single root XML document.
    pub fn parse_xml(input: &str) -> Result<XmlElement, String> {
        if input.len() > MAX_JUNIT_XML_BYTES {
            return Err(format!(
                "JUnit XML exceeds maximum allowed size of {MAX_JUNIT_XML_BYTES} bytes"
            ));
        }
        let mut parser = Parser::new(input);
        parser.skip_misc()?;
        if parser.is_eof() {
            return Err("empty XML document".to_string());
        }
        let root = parser.parse_element(0)?;
        parser.skip_misc()?;
        if !parser.is_eof() {
            return Err(format!(
                "unexpected trailing content after root XML element '</{}>'",
                root.name
            ));
        }
        Ok(root)
    }

    /// Extracts `<testsuite>` elements from a parsed JUnit XML root (`<testsuites>` or `<testsuite>`).
    pub fn extract_suites(root: &XmlElement) -> Result<Vec<XmlElement>, String> {
        fn collect_from_testsuites(
            el: &XmlElement,
            suites: &mut Vec<XmlElement>,
            stray_cases: &mut Vec<XmlNode>,
        ) {
            for child in el.child_elements() {
                match child.name.as_str() {
                    "testsuite" => suites.push(child.clone()),
                    "testcase" => stray_cases.push(XmlNode::Element(child.clone())),
                    "testsuites" => collect_from_testsuites(child, suites, stray_cases),
                    _ => {}
                }
            }
        }

        match root.name.as_str() {
            "testsuite" => Ok(vec![root.clone()]),
            "testsuites" => {
                let mut suites = Vec::new();
                let mut stray_cases = Vec::new();
                collect_from_testsuites(root, &mut suites, &mut stray_cases);
                if !stray_cases.is_empty() {
                    let mut wrapper = XmlElement::new("testsuite");
                    wrapper.set_attr("name", "default");
                    wrapper.children = stray_cases;
                    suites.push(wrapper);
                }
                Ok(suites)
            }
            other => Err(format!(
                "unexpected JUnit root element '<{other}>' (expected <testsuites> or <testsuite>)"
            )),
        }
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    struct SuiteCounts {
        tests: u64,
        failures: u64,
        errors: u64,
        skipped: u64,
        time_secs: f64,
    }

    fn compute_suite_counts(suite: &XmlElement) -> SuiteCounts {
        let mut child_tests = 0u64;
        let mut child_failures = 0u64;
        let mut child_errors = 0u64;
        let mut child_skipped = 0u64;
        let mut child_time = 0.0f64;

        for child in suite.child_elements() {
            if child.name == "testcase" {
                child_tests = child_tests.saturating_add(1);
                if let Some(t) = child
                    .attr("time")
                    .and_then(|s| s.trim().parse::<f64>().ok())
                {
                    if t.is_finite() && t >= 0.0 {
                        child_time += t;
                    }
                }
                for tc_child in child.child_elements() {
                    match tc_child.name.as_str() {
                        "failure" => child_failures = child_failures.saturating_add(1),
                        "error" => child_errors = child_errors.saturating_add(1),
                        "skipped" => child_skipped = child_skipped.saturating_add(1),
                        _ => {}
                    }
                }
            } else if child.name == "testsuite" {
                let sub = compute_suite_counts(child);
                child_tests = child_tests.saturating_add(sub.tests);
                child_failures = child_failures.saturating_add(sub.failures);
                child_errors = child_errors.saturating_add(sub.errors);
                child_skipped = child_skipped.saturating_add(sub.skipped);
                child_time += sub.time_secs;
            }
        }

        let tests = suite
            .attr("tests")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(child_tests);
        let failures = suite
            .attr("failures")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(child_failures);
        let errors = suite
            .attr("errors")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(child_errors);
        let skipped = suite
            .attr("skipped")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(child_skipped);
        let time_secs = suite
            .attr("time")
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|t| t.is_finite() && *t >= 0.0)
            .unwrap_or(child_time);

        SuiteCounts {
            tests,
            failures,
            errors,
            skipped,
            time_secs,
        }
    }

    fn synthesize_shard_error_suite(
        shard_index: usize,
        case_name: &str,
        message: &str,
        body: &str,
        duration_ms: u64,
    ) -> XmlElement {
        let time_str = format!("{:.6}", duration_ms as f64 / 1000.0);
        let mut err_el = XmlElement::new("error");
        err_el.set_attr("message", message);
        err_el.set_attr("type", "ShardFailure");
        if !body.trim().is_empty() {
            err_el.children.push(XmlNode::Text(body.trim().to_string()));
        }

        let mut tc = XmlElement::new("testcase");
        tc.set_attr("name", case_name);
        tc.set_attr("classname", format!("oxideswarm.shard.{shard_index}"));
        tc.set_attr("time", time_str.clone());
        tc.children.push(XmlNode::Element(err_el));

        let mut suite = XmlElement::new("testsuite");
        suite.set_attr("name", format!("oxideswarm.shard.{shard_index}"));
        suite.set_attr("tests", "1");
        suite.set_attr("failures", "0");
        suite.set_attr("errors", "1");
        suite.set_attr("skipped", "0");
        suite.set_attr("time", time_str);
        suite.children.push(XmlNode::Element(tc));
        suite
    }

    /// One downloaded JUnit XML blob from a shard.
    #[derive(Debug, Clone)]
    pub struct ShardJunitBlob {
        pub path: String,
        pub bytes: Vec<u8>,
    }

    /// All JUnit XML blobs and metadata collected for one shard.
    #[derive(Debug, Clone)]
    pub struct ShardJunitInput {
        pub shard_index: usize,
        pub status: CellStatus,
        pub duration_ms: u64,
        pub stderr_excerpt: String,
        pub error: Option<String>,
        pub blobs: Vec<ShardJunitBlob>,
    }

    /// Result of merging per-shard JUnit XML documents.
    #[derive(Debug, Clone)]
    pub struct MergedJunit {
        pub xml: String,
        pub tests: u64,
        pub failures: u64,
        pub errors: u64,
        pub skipped: u64,
        pub time_secs: f64,
        pub suites: usize,
        pub synthesized_shards: Vec<usize>,
        pub parse_errors: Vec<String>,
    }

    /// Merges JUnit XML inputs across shards in ascending shard order.
    ///
    /// - If all shards completed (`CellStatus::Completed`) and zero JUnit XML blobs were
    ///   collected across all shards, returns `Err(...)`.
    /// - If a failed/timed-out/errored/cancelled shard produced zero JUnit XML blobs, synthesizes
    ///   an error `<testsuite>` entry for that shard.
    /// - If a JUnit XML blob is invalid UTF-8 or malformed XML, records the error in
    ///   `parse_errors` and synthesizes an error `<testsuite>` for that shard.
    pub fn merge_shard_junit_inputs(
        mut inputs: Vec<ShardJunitInput>,
        junit_glob_desc: &str,
    ) -> Result<MergedJunit, String> {
        inputs.sort_by_key(|s| s.shard_index);

        let all_completed = inputs.iter().all(|s| s.status == CellStatus::Completed);
        let total_blobs: usize = inputs.iter().map(|s| s.blobs.len()).sum();
        if all_completed && total_blobs == 0 {
            return Err(format!(
                "all {} shard(s) completed without producing any JUnit XML files matching '{junit_glob_desc}'",
                inputs.len()
            ));
        }

        let mut collected_suites: Vec<XmlElement> = Vec::new();
        let mut synthesized_shards: Vec<usize> = Vec::new();
        let mut parse_errors: Vec<String> = Vec::new();

        for shard in &inputs {
            if shard.blobs.is_empty() {
                if shard.status != CellStatus::Completed {
                    let msg = shard.error.clone().unwrap_or_else(|| {
                        format!(
                            "Shard {} {} without producing JUnit XML",
                            shard.shard_index,
                            shard.status.as_str()
                        )
                    });
                    collected_suites.push(synthesize_shard_error_suite(
                        shard.shard_index,
                        &format!("shard-{}", shard.shard_index),
                        &msg,
                        &shard.stderr_excerpt,
                        shard.duration_ms,
                    ));
                    synthesized_shards.push(shard.shard_index);
                }
                continue;
            }

            if let Some(ref err_msg) = shard.error {
                if shard.status != CellStatus::Completed {
                    collected_suites.push(synthesize_shard_error_suite(
                        shard.shard_index,
                        &format!("shard-{}", shard.shard_index),
                        err_msg,
                        &shard.stderr_excerpt,
                        shard.duration_ms,
                    ));
                    synthesized_shards.push(shard.shard_index);
                }
            }

            for blob in &shard.blobs {
                let text = match std::str::from_utf8(&blob.bytes) {
                    Ok(t) => t,
                    Err(e) => {
                        let msg = format!(
                            "shard {} JUnit file '{}' is not valid UTF-8: {e}",
                            shard.shard_index, blob.path
                        );
                        parse_errors.push(msg.clone());
                        collected_suites.push(synthesize_shard_error_suite(
                            shard.shard_index,
                            "junit-parse",
                            &msg,
                            "",
                            shard.duration_ms,
                        ));
                        synthesized_shards.push(shard.shard_index);
                        continue;
                    }
                };
                let parsed = parse_xml(text).and_then(|root| extract_suites(&root));
                match parsed {
                    Ok(suites) => {
                        collected_suites.extend(suites);
                    }
                    Err(e) => {
                        let msg = format!(
                            "shard {} malformed JUnit XML '{}': {e}",
                            shard.shard_index, blob.path
                        );
                        parse_errors.push(msg.clone());
                        collected_suites.push(synthesize_shard_error_suite(
                            shard.shard_index,
                            "junit-parse",
                            &msg,
                            "",
                            shard.duration_ms,
                        ));
                        synthesized_shards.push(shard.shard_index);
                    }
                }
            }
        }

        let mut totals = SuiteCounts::default();
        for suite in &collected_suites {
            let c = compute_suite_counts(suite);
            totals.tests = totals.tests.saturating_add(c.tests);
            totals.failures = totals.failures.saturating_add(c.failures);
            totals.errors = totals.errors.saturating_add(c.errors);
            totals.skipped = totals.skipped.saturating_add(c.skipped);
            totals.time_secs += c.time_secs;
        }

        let mut root = XmlElement::new("testsuites");
        root.set_attr("name", "oxideswarm-shard");
        root.set_attr("tests", totals.tests.to_string());
        root.set_attr("failures", totals.failures.to_string());
        root.set_attr("errors", totals.errors.to_string());
        root.set_attr("skipped", totals.skipped.to_string());
        root.set_attr("time", format!("{:.6}", totals.time_secs));
        let suite_count = collected_suites.len();
        root.children = collected_suites.into_iter().map(XmlNode::Element).collect();

        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        serialize_element(&root, 0, &mut xml);

        synthesized_shards.sort_unstable();
        synthesized_shards.dedup();

        Ok(MergedJunit {
            xml,
            tests: totals.tests,
            failures: totals.failures,
            errors: totals.errors,
            skipped: totals.skipped,
            time_secs: totals.time_secs,
            suites: suite_count,
            synthesized_shards,
            parse_errors,
        })
    }

    /// Checks that `path` and its existing ancestor directories are not symbolic links, creates
    /// missing parent directories safely, and atomically writes `xml` via a sibling temp file.
    pub fn write_junit_atomic(path: &Path, xml: &str) -> Result<(), String> {
        if path.as_os_str().is_empty() {
            return Err("--junit-out path cannot be empty".to_string());
        }
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "--junit-out '{}': path is a symbolic link, which is refused",
                    path.display()
                ));
            }
            if meta.is_dir() {
                return Err(format!(
                    "--junit-out '{}': path exists and is a directory",
                    path.display()
                ));
            }
        }
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let mut cur = std::path::PathBuf::new();
                for comp in parent.components() {
                    cur.push(comp);
                    match comp {
                        std::path::Component::Prefix(_) | std::path::Component::RootDir => continue,
                        _ => {}
                    }
                    match std::fs::symlink_metadata(&cur) {
                        Ok(meta) => {
                            let is_macos_root_alias = cfg!(target_os = "macos")
                                && matches!(cur.to_str(), Some("/var" | "/tmp" | "/etc"));
                            if meta.file_type().is_symlink() && !is_macos_root_alias {
                                return Err(format!(
                                    "--junit-out '{}': parent '{}' is a symbolic link, which is refused",
                                    path.display(),
                                    cur.display()
                                ));
                            }
                            if !(meta.is_dir() || (is_macos_root_alias && cur.is_dir())) {
                                return Err(format!(
                                    "--junit-out '{}': parent '{}' exists and is not a directory",
                                    path.display(),
                                    cur.display()
                                ));
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            std::fs::create_dir(&cur).map_err(|e| {
                                format!(
                                    "--junit-out '{}': cannot create parent directory '{}': {e}",
                                    path.display(),
                                    cur.display()
                                )
                            })?;
                        }
                        Err(e) => {
                            return Err(format!(
                                "--junit-out '{}': cannot inspect parent '{}': {e}",
                                path.display(),
                                cur.display()
                            ));
                        }
                    }
                }
            }
        }

        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("--junit-out '{}': invalid file name", path.display()))?;
        let tmp = path.with_file_name(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
        if let Err(e) = std::fs::write(&tmp, xml.as_bytes()) {
            let _ = std::fs::remove_file(&tmp);
            return Err(format!(
                "--junit-out '{}': failed writing temp file: {e}",
                path.display()
            ));
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(format!(
                "--junit-out '{}': failed renaming temp file into place: {e}",
                path.display()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::Bytes;

    fn sample_worker(
        name: &str,
        id: Uuid,
        cpu_cores: usize,
        slots: Option<usize>,
        os: &str,
        arch: &str,
        extra_tags: &[&str],
    ) -> WorkerCapabilities {
        let mut tags = vec![
            format!("id:{id}"),
            format!("os:{os}"),
            format!("arch:{arch}"),
            "state:connected".to_string(),
        ];
        if let Some(s) = slots {
            tags.push(format!("slots:{s}"));
        }
        for t in extra_tags {
            tags.push((*t).to_string());
        }
        WorkerCapabilities {
            name: name.to_string(),
            cpu_cores,
            ram_mb: 8192,
            has_gpu: false,
            gpu_device_name: None,
            vram_mb: None,
            is_simulated_gpu: false,
            has_blender: false,
            blender_version: None,
            has_cuda: false,
            mobile: None,
            tags,
        }
    }

    #[test]
    fn test_swrr_spread_respects_slots_weights_and_interleaving() {
        let w1 = sample_worker(
            "heavy",
            Uuid::from_u128(1),
            4,
            Some(6),
            "linux",
            "x86_64",
            &[],
        );
        let w2 = sample_worker(
            "light",
            Uuid::from_u128(2),
            4,
            Some(2),
            "linux",
            "x86_64",
            &[],
        );
        let eligible = filter_eligible_workers(
            &[w1, w2],
            &Placement::default(),
            &TaskRequirements::default(),
            false,
            false,
        )
        .unwrap();
        let assignments = spread_shards_swrr(8, &eligible, &Placement::default()).unwrap();
        assert_eq!(assignments.len(), 8);

        let heavy_count = assignments
            .iter()
            .filter(|a| a.worker_name == "heavy")
            .count();
        let light_count = assignments
            .iter()
            .filter(|a| a.worker_name == "light")
            .count();
        assert_eq!(heavy_count, 6);
        assert_eq!(light_count, 2);

        // Interleaving: light worker appears every 4th assignment, not clumped at the end.
        let names: Vec<&str> = assignments.iter().map(|a| a.worker_name.as_str()).collect();
        assert!(
            names[..4].contains(&"light") && names[4..].contains(&"light"),
            "SWRR must interleave light worker across rounds: {names:?}"
        );
        for (idx, a) in assignments.iter().enumerate() {
            assert_eq!(a.index, idx + 1);
            assert_eq!(a.total, 8);
            assert_eq!(a.placement.worker, None);
            assert_eq!(
                a.preferred_worker.as_deref(),
                Some(a.worker_target_key.as_str())
            );
        }
    }

    #[test]
    fn test_swrr_soft_prefer_tag_vs_explicit_worker_pin_and_build_once_affinity() {
        let w_id = Uuid::from_u128(11);
        let w = sample_worker(
            "mac-arm",
            w_id,
            4,
            Some(4),
            "macos",
            "aarch64",
            &["tool:cargo-nextest@0.9.99"],
        );
        let eligible = filter_eligible_workers(
            std::slice::from_ref(&w),
            &Placement::default(),
            &TaskRequirements::default(),
            false,
            true,
        )
        .unwrap();

        // 1) Without explicit --worker: uses soft prefer:worker= tag and clears prior require:/prefer:worker= tags.
        let mut soft_assignments = spread_shards_swrr(1, &eligible, &Placement::default()).unwrap();
        let mut soft = soft_assignments.remove(0);
        assert_eq!(soft.placement.worker, None);
        assert_eq!(
            soft.preferred_worker.as_deref(),
            Some(w_id.to_string().as_str())
        );

        let outcome = BuildOnceOutcome {
            os: "macos".to_string(),
            arch: "aarch64".to_string(),
            archive_rel: ".oxideswarm-build-macos-aarch64.tar.zst".to_string(),
            archive_blob: BlobFile {
                path: ".oxideswarm-build-macos-aarch64.tar.zst".to_string(),
                id: BlobId::of(b"archive"),
                size: 16,
                executable: false,
            },
            task_id: TaskId::new(),
            worker_id: w_id,
            worker_name: Some("mac-arm".to_string()),
            duration_ms: 10,
            required_tool: Some("cargo-nextest@0.9.99".to_string()),
        };
        apply_build_once_shard_placement(&mut soft, &outcome);
        assert_eq!(soft.placement.os.as_deref(), Some("macos"));
        assert_eq!(soft.placement.arch.as_deref(), Some("aarch64"));
        assert_eq!(soft.placement.tools, vec!["cargo-nextest@0.9.99"]);

        let mut template = Task::new(
            TaskSpec::new_command("echo", vec!["hi".into()]),
            TaskRequirements::default(),
        );
        template
            .tags
            .push("require:worker=stale-worker".to_string());
        template.tags.push("prefer:worker=stale-pref".to_string());
        template.tags.push("opt:repeatable".to_string());

        let soft_task = build_shard_task(
            &template,
            &soft,
            Some((outcome.archive_rel.as_str(), &outcome.archive_blob)),
            None,
            &[],
            true,
        );
        assert!(soft_task.validate().is_ok(), "{:?}", soft_task.tags);
        assert!(
            !soft_task
                .tags
                .iter()
                .any(|t| t.starts_with("require:worker=")),
            "soft shard must not emit require:worker=: {:?}",
            soft_task.tags
        );
        assert!(
            soft_task.tags.contains(&format!("prefer:worker={w_id}")),
            "expected soft prefer:worker= tag: {:?}",
            soft_task.tags
        );
        assert!(!soft_task
            .tags
            .contains(&"prefer:worker=stale-pref".to_string()));
        assert!(soft_task.tags.contains(&"require:os=macos".to_string()));
        assert!(soft_task.tags.contains(&"require:arch=aarch64".to_string()));
        assert!(soft_task
            .tags
            .contains(&"require:tool=cargo-nextest@0.9.99".to_string()));
        assert!(soft_task.tags.contains(&"opt:repeatable".to_string()));

        // 2) With explicit --worker: keeps require:worker= pin and omits prefer:worker=.
        let explicit_base = Placement {
            worker: Some("mac-arm".to_string()),
            ..Placement::default()
        };
        let mut hard = spread_shards_swrr(1, &eligible, &explicit_base)
            .unwrap()
            .remove(0);
        assert_eq!(hard.placement.worker.as_deref(), Some("mac-arm"));
        assert_eq!(hard.preferred_worker, None);
        apply_build_once_shard_placement(&mut hard, &outcome);
        assert_eq!(hard.placement.os, None);
        assert_eq!(hard.placement.arch, None);
        assert!(hard.placement.tools.is_empty());

        let hard_task = build_shard_task(&template, &hard, None, None, &[], false);
        assert!(hard_task
            .tags
            .contains(&"require:worker=mac-arm".to_string()));
        assert!(!hard_task
            .tags
            .iter()
            .any(|t| t.starts_with("prefer:worker=")));
    }

    #[test]
    fn test_filter_eligible_workers_rejects_disconnected_and_unmatched() {
        let mut disconnected = sample_worker(
            "offline",
            Uuid::from_u128(10),
            4,
            Some(4),
            "macos",
            "aarch64",
            &[],
        );
        disconnected.tags.retain(|t| !t.starts_with("state:"));
        disconnected.tags.push("state:disconnected".to_string());

        let err = filter_eligible_workers(
            &[disconnected],
            &Placement::default(),
            &TaskRequirements::default(),
            false,
            false,
        )
        .unwrap_err();
        assert!(err.contains("no connected workers"), "{err}");

        let online = sample_worker(
            "mac",
            Uuid::from_u128(11),
            4,
            Some(4),
            "macos",
            "aarch64",
            &[],
        );
        let placement = Placement {
            os: Some("linux".to_string()),
            ..Placement::default()
        };
        let err = filter_eligible_workers(
            &[online],
            &placement,
            &TaskRequirements::default(),
            false,
            false,
        )
        .unwrap_err();
        assert!(err.contains("no connected worker matches"), "{err}");
    }

    #[test]
    fn test_build_once_platform_grouping_and_collision_detection() {
        let w_mac_1 = sample_worker(
            "m1",
            Uuid::from_u128(1),
            4,
            Some(4),
            "macos",
            "aarch64",
            &["tool:cargo-nextest@0.9.99"],
        );
        let w_mac_old = sample_worker(
            "m2",
            Uuid::from_u128(2),
            2,
            Some(1),
            "macos",
            "aarch64",
            &["tool:cargo-nextest@0.9.98"],
        );
        let w_lin = sample_worker(
            "l1",
            Uuid::from_u128(3),
            8,
            Some(4),
            "linux",
            "x86_64",
            &["tool:cargo-nextest@0.9.99"],
        );
        let eligible = filter_eligible_workers(
            &[w_mac_1, w_mac_old, w_lin],
            &Placement::default(),
            &TaskRequirements::default(),
            false,
            true,
        )
        .unwrap();
        let (norm, platform_tools) = normalize_build_once_workers(eligible).unwrap();
        // Old 0.9.98 mac worker with fewer slots was dropped so macos/aarch64 uses consistent 0.9.99.
        assert_eq!(norm.len(), 2);
        assert_eq!(
            platform_tools.get(&("macos".to_string(), "aarch64".to_string())),
            Some(&Some("cargo-nextest@0.9.99".to_string()))
        );

        let assignments = spread_shards_swrr(4, &norm, &Placement::default()).unwrap();
        let manifest = TreeManifest::new(vec![BlobFile {
            path: ".oxideswarm-build-linux-x86_64.tar.zst".to_string(),
            id: BlobId::of(b"conflict"),
            size: 8,
            executable: false,
        }])
        .unwrap();
        assert!(check_build_once_path_collision(
            ".oxideswarm-build-linux-x86_64.tar.zst",
            &manifest,
            &[]
        )
        .is_err());

        let (groups, cfg) =
            plan_build_once_groups(&assignments, &platform_tools, None, &manifest, &[], true)
                .unwrap();
        assert_eq!(cfg.as_deref(), Some(BUILD_ONCE_NEXTEST_CONFIG_REL));
        assert_eq!(groups.len(), 2);
        let linux_group = groups.iter().find(|g| g.os == "linux").unwrap();
        // Automatically avoided colliding path by suffixing `-1`.
        assert_eq!(
            linux_group.archive_rel,
            ".oxideswarm-build-linux-x86_64-1.tar.zst"
        );
    }

    #[test]
    fn test_scan_build_source_skips_git_and_target_even_without_gitignore() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::create_dir_all(root.join("target/debug")).unwrap();
        std::fs::write(root.join("target/debug/huge.bin"), vec![0u8; 64]).unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("Cargo.toml"), "[package]\nname = \"demo\"\n").unwrap();
        std::fs::write(root.join("src/lib.rs"), "pub fn ok() {}\n").unwrap();

        let scan = scan_build_source(root, WorkspaceLimits::default()).unwrap();
        assert!(!scan.manifest.is_v2());
        let paths: Vec<&str> = scan
            .manifest
            .files()
            .iter()
            .map(|f| f.path.as_str())
            .collect();
        assert_eq!(paths, vec!["Cargo.toml", "src/lib.rs"]);
    }

    #[test]
    fn test_junit_merge_aggregates_counts_entities_cdata_and_synthesizes_missing_failure() {
        let xml1 = "\u{FEFF}<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
            <!-- comment -->\n\
            <testsuites>\n\
              <testsuite name=\"suite-a\" tests=\"2\" failures=\"0\" errors=\"0\" skipped=\"1\" time=\"0.125\">\n\
                <testcase name=\"t1\" classname=\"c\" time=\"0.100\"/>\n\
                <testcase name=\"t2\" classname=\"c\" time=\"0.025\"><skipped message=\"skip &amp; go\"/></testcase>\n\
              </testsuite>\n\
            </testsuites>";
        let xml2 = "<testsuite name=\"suite-b\" time=\"0.500\">\n\
              <testcase name=\"t3\" classname=\"c\" time=\"0.500\">\n\
                <failure message=\"boom &lt;1&gt;\"><![CDATA[line1 ]]]]><![CDATA[>\nline2]]></failure>\n\
              </testcase>\n\
            </testsuite>";

        let merged = junit::merge_shard_junit_inputs(
            vec![
                junit::ShardJunitInput {
                    shard_index: 1,
                    status: CellStatus::Completed,
                    duration_ms: 150,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "junit.xml".to_string(),
                        bytes: xml1.as_bytes().to_vec(),
                    }],
                },
                junit::ShardJunitInput {
                    shard_index: 2,
                    status: CellStatus::Failed,
                    duration_ms: 520,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "junit.xml".to_string(),
                        bytes: xml2.as_bytes().to_vec(),
                    }],
                },
                junit::ShardJunitInput {
                    shard_index: 3,
                    status: CellStatus::TimedOut,
                    duration_ms: 3000,
                    stderr_excerpt: "timed out waiting".to_string(),
                    error: None,
                    blobs: Vec::new(),
                },
            ],
            "**/junit*.xml",
        )
        .unwrap();

        assert_eq!(merged.tests, 4);
        assert_eq!(merged.failures, 1);
        assert_eq!(merged.errors, 1);
        assert_eq!(merged.skipped, 1);
        assert_eq!(merged.suites, 3);
        assert_eq!(merged.synthesized_shards, vec![3]);
        assert!(merged.parse_errors.is_empty());

        let reparsed = junit::parse_xml(&merged.xml).unwrap();
        assert_eq!(reparsed.name, "testsuites");
        assert_eq!(reparsed.attr("tests"), Some("4"));
        assert_eq!(reparsed.attr("failures"), Some("1"));
        assert_eq!(reparsed.attr("errors"), Some("1"));
        assert_eq!(reparsed.attr("skipped"), Some("1"));
        assert!(merged.xml.contains("skip &amp; go"));
        assert!(merged.xml.contains("boom &lt;1&gt;"));
        assert!(merged.xml.contains("line1 ]]"));
        assert!(merged.xml.contains(">\nline2"));

        // Direct XmlNode::CData containing literal `]]>` is split safely on serialization.
        let mut cdata_suite = junit::XmlElement::new("testsuite");
        cdata_suite.set_attr("name", "cdata-split");
        let mut tc = junit::XmlElement::new("testcase");
        tc.set_attr("name", "c1");
        let mut fail_el = junit::XmlElement::new("failure");
        fail_el
            .children
            .push(junit::XmlNode::CData("a]]>b".to_string()));
        tc.children.push(junit::XmlNode::Element(fail_el));
        cdata_suite.children.push(junit::XmlNode::Element(tc));
        let mut root = junit::XmlElement::new("testsuites");
        root.children.push(junit::XmlNode::Element(cdata_suite));
        let mut serialized = String::new();
        junit::serialize_root(&root, &mut serialized);
        assert!(serialized.contains("]]]]><![CDATA[>"));
    }

    #[test]
    fn test_junit_merge_errors_when_all_completed_with_zero_files_and_handles_malformed_xml() {
        let err = junit::merge_shard_junit_inputs(
            vec![junit::ShardJunitInput {
                shard_index: 1,
                status: CellStatus::Completed,
                duration_ms: 50,
                stderr_excerpt: String::new(),
                error: None,
                blobs: Vec::new(),
            }],
            "**/junit*.xml",
        )
        .unwrap_err();
        assert!(err.contains("without producing any JUnit XML"), "{err}");

        let merged = junit::merge_shard_junit_inputs(
            vec![junit::ShardJunitInput {
                shard_index: 1,
                status: CellStatus::Failed,
                duration_ms: 50,
                stderr_excerpt: String::new(),
                error: None,
                blobs: vec![junit::ShardJunitBlob {
                    path: "bad.xml".to_string(),
                    bytes: b"<testsuite><unclosed>".to_vec(),
                }],
            }],
            "**/junit*.xml",
        )
        .unwrap();
        assert_eq!(merged.errors, 1);
        assert_eq!(merged.parse_errors.len(), 1);
        assert_eq!(merged.synthesized_shards, vec![1]);
    }

    #[test]
    fn test_junit_merge_aggregates_nested_testsuites_without_double_counting() {
        // 1) Outer <testsuite> omits summary attributes -> recursively aggregates inner <testsuite> + direct <testcase>.
        let nested_omitted = r#"
<testsuites>
  <testsuite name="outer">
    <testcase name="direct" time="0.2"><skipped/></testcase>
    <testsuite name="inner" tests="2" failures="1" errors="0" skipped="0" time="0.3">
      <testcase name="x" time="0.1"/>
      <testcase name="y" time="0.2"><failure message="boom"/></testcase>
    </testsuite>
  </testsuite>
</testsuites>"#;
        // 2) Outer <testsuite> specifies explicit summary attributes alongside inner <testsuite> -> must NOT double-count.
        let nested_explicit = r#"
<testsuites>
  <testsuite name="outer-explicit" tests="2" failures="0" errors="1" skipped="0" time="0.5">
    <testsuite name="inner-explicit" tests="2" failures="0" errors="1" skipped="0" time="0.5">
      <testcase name="a" time="0.2"/>
      <testcase name="b" time="0.3"><error message="err"/></testcase>
    </testsuite>
  </testsuite>
</testsuites>"#;

        let merged = junit::merge_shard_junit_inputs(
            vec![
                junit::ShardJunitInput {
                    shard_index: 1,
                    status: CellStatus::Failed,
                    duration_ms: 500,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "junit.xml".to_string(),
                        bytes: nested_omitted.as_bytes().to_vec(),
                    }],
                },
                junit::ShardJunitInput {
                    shard_index: 2,
                    status: CellStatus::Failed,
                    duration_ms: 500,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "junit.xml".to_string(),
                        bytes: nested_explicit.as_bytes().to_vec(),
                    }],
                },
            ],
            "**/junit*.xml",
        )
        .unwrap();

        assert_eq!(merged.tests, 5);
        assert_eq!(merged.failures, 1);
        assert_eq!(merged.errors, 1);
        assert_eq!(merged.skipped, 1);
        assert!(
            (merged.time_secs - 1.0).abs() < 1e-6,
            "{}",
            merged.time_secs
        );
        assert_eq!(merged.suites, 2);

        let reparsed = junit::parse_xml(&merged.xml).unwrap();
        assert_eq!(reparsed.attr("tests"), Some("5"));
        assert_eq!(reparsed.attr("failures"), Some("1"));
        assert_eq!(reparsed.attr("errors"), Some("1"));
        assert_eq!(reparsed.attr("skipped"), Some("1"));
    }

    #[test]
    fn test_junit_merge_synthesizes_error_suite_on_junit_download_error() {
        let merged = junit::merge_shard_junit_inputs(
            vec![
                junit::ShardJunitInput {
                    shard_index: 1,
                    status: CellStatus::Completed,
                    duration_ms: 100,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "junit.xml".to_string(),
                        bytes: b"<testsuite name=\"s1\" tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\" time=\"0.1\"><testcase name=\"ok\"/></testsuite>".to_vec(),
                    }],
                },
                junit::ShardJunitInput {
                    shard_index: 2,
                    status: CellStatus::Error,
                    duration_ms: 120,
                    stderr_excerpt: String::new(),
                    error: Some("JUnit blob 'junit.xml' download failed: blob evicted".to_string()),
                    blobs: Vec::new(),
                },
            ],
            "**/junit*.xml",
        )
        .unwrap();

        assert_eq!(merged.tests, 2);
        assert_eq!(merged.errors, 1);
        assert_eq!(merged.synthesized_shards, vec![2]);
        assert!(merged.xml.contains("blob evicted"), "{}", merged.xml);
    }

    #[test]
    fn test_shard_report_json_and_text_include_run_summary_fields() {
        let wid = Uuid::from_u128(42);
        let assignment = ShardAssignment {
            index: 1,
            total: 1,
            slug: shard_slug(1, 1),
            worker_id: Some(wid),
            worker_name: "w1".to_string(),
            worker_target_key: wid.to_string(),
            os: "macos".to_string(),
            arch: "aarch64".to_string(),
            slots: 2,
            placement: Placement::default(),
            preferred_worker: Some(wid.to_string()),
        };
        let result = TaskResult {
            worker_id: wid,
            task_id: TaskId::new(),
            exit_code: 0,
            stdout: Bytes::from(b"ok\n".to_vec()),
            stderr: Bytes::new(),
            execution_time_ms: 250,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: Vec::new(),
            outputs: Vec::new(),
            cached: false,
        };
        let report = ShardReport::from_executions(
            vec![ShardExecution {
                assignment,
                status: CellStatus::Completed,
                task_id: Some(result.task_id),
                worker_name: Some("w1".to_string()),
                result: Some(result),
                error: None,
                saved_outputs: None,
                saved_artifacts: None,
                download_error: None,
            }],
            300,
            DEFAULT_SHARD_OUTPUT_BYTES,
            Vec::new(),
            None,
            None,
            None,
        );
        assert!(!report.any_failed());
        assert_eq!(report.exit_code(), 0);
        let json = report.to_json();
        assert_eq!(json["shard"], true);
        assert_eq!(json["status"], "completed");
        assert_eq!(json["machines"], 1);
        assert_eq!(json["waited_ms"], 300);
        assert_eq!(json["task_time_sum_ms"], 250);
        let text = report.text();
        assert!(text.contains("Tasks: 1 finished, 0 failed, on 1 machines"));
        assert!(text.contains("Waited: 0.300 s    Sum of task times: 0.250 s"));
    }

    #[cfg(unix)]
    #[test]
    fn test_write_junit_atomic_refuses_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("outside.xml");
        let link = tmp.path().join("junit.xml");
        std::fs::write(&target, "keep").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = junit::write_junit_atomic(&link, "<testsuites/>").unwrap_err();
        assert!(err.contains("symbolic link"), "{err}");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep");
    }

    #[test]
    fn test_junit_adversarial_nested_dup_attrs_doctype_entities_and_partial_completed() {
        // 1. Nested <testsuites> inside <testsuites> (including stray <testcase>)
        let nested_ts = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE testsuites SYSTEM "junit.dtd">
<testsuites>
  <testsuites name="inner-root">
    <testsuite name="inner-suite" tests="2" failures="1" errors="0" skipped="0" time="0.25">
      <testcase name="a" time="0.1"/>
      <testcase name="b" time="0.15"><failure message="boom"/></testcase>
    </testsuite>
    <testcase name="stray_case" time="0.05"/>
  </testsuites>
</testsuites>"#;
        let parsed = junit::parse_xml(nested_ts).unwrap();
        let extracted = junit::extract_suites(&parsed).unwrap();
        assert_eq!(extracted.len(), 2, "expected inner-suite + default wrapper");

        // 2. Duplicate XML attributes rejected cleanly by parse_xml and synthesized in merge
        let dup_attr_xml =
            r#"<testsuite name="dup" tests="1" tests="99"><testcase name="t1"/></testsuite>"#;
        let dup_err = junit::parse_xml(dup_attr_xml).unwrap_err();
        assert!(dup_err.contains("duplicate XML attribute"), "{dup_err}");

        // 3. Unknown entity &foo; rejected cleanly
        let bad_entity = r#"<testsuite name="ent"><testcase name="x"><failure message="&foo;"/></testcase></testsuite>"#;
        assert!(junit::parse_xml(bad_entity)
            .unwrap_err()
            .contains("unknown XML entity"));

        // 4. Duplicate <testsuite name="..."> across shards, missing/malformed counts/time, control chars & &#0;
        let s1_xml = "<testsuite name=\"shared\" tests=\"abc\" time=\"NaN\">\n\
            <testcase name=\"a\x07b\" time=\"-1\"><failure message=\"x&#0;y\"/></testcase>\n\
            </testsuite>";
        let s2_xml = "<testsuite name=\"shared\">\n\
            <testcase name=\"ok\" time=\"0.40\"/>\n\
            </testsuite>";

        let merged = junit::merge_shard_junit_inputs(
            vec![
                junit::ShardJunitInput {
                    shard_index: 1,
                    status: CellStatus::Completed,
                    duration_ms: 100,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "s1.xml".to_string(),
                        bytes: s1_xml.as_bytes().to_vec(),
                    }],
                },
                junit::ShardJunitInput {
                    shard_index: 2,
                    status: CellStatus::Completed,
                    duration_ms: 400,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "s2.xml".to_string(),
                        bytes: s2_xml.as_bytes().to_vec(),
                    }],
                },
                junit::ShardJunitInput {
                    shard_index: 3,
                    status: CellStatus::Failed,
                    duration_ms: 50,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: vec![junit::ShardJunitBlob {
                        path: "dup.xml".to_string(),
                        bytes: dup_attr_xml.as_bytes().to_vec(),
                    }],
                },
            ],
            "**/junit*.xml",
        )
        .unwrap();
        // Shard 1: 1 test, 1 failure; Shard 2: 1 test; Shard 3: synthesized 1 test, 1 error.
        assert_eq!(merged.tests, 3);
        assert_eq!(merged.failures, 1);
        assert_eq!(merged.errors, 1);
        assert_eq!(merged.suites, 3);
        assert_eq!(merged.synthesized_shards, vec![3]);
        assert_eq!(merged.parse_errors.len(), 1);
        assert!(!merged.xml.contains('\x07') && !merged.xml.contains('\0'));
        assert!(junit::parse_xml(&merged.xml).is_ok());

        // 5. Partial failure: Shard 1 Completed (0 blobs) + Shard 2 Failed (0 blobs)
        let partial = junit::merge_shard_junit_inputs(
            vec![
                junit::ShardJunitInput {
                    shard_index: 1,
                    status: CellStatus::Completed,
                    duration_ms: 20,
                    stderr_excerpt: String::new(),
                    error: None,
                    blobs: Vec::new(),
                },
                junit::ShardJunitInput {
                    shard_index: 2,
                    status: CellStatus::Failed,
                    duration_ms: 30,
                    stderr_excerpt: "boom".to_string(),
                    error: None,
                    blobs: Vec::new(),
                },
            ],
            "**/junit*.xml",
        )
        .unwrap();
        assert_eq!(partial.tests, 1);
        assert_eq!(partial.errors, 1);
        assert_eq!(partial.synthesized_shards, vec![2]);

        // 6. Nested <testsuites> via merge_shard_junit_inputs preserves inner failures
        let merged_nested = junit::merge_shard_junit_inputs(
            vec![junit::ShardJunitInput {
                shard_index: 1,
                status: CellStatus::Failed,
                duration_ms: 300,
                stderr_excerpt: String::new(),
                error: None,
                blobs: vec![junit::ShardJunitBlob {
                    path: "nested.xml".to_string(),
                    bytes: nested_ts.as_bytes().to_vec(),
                }],
            }],
            "**/junit*.xml",
        )
        .unwrap();
        assert_eq!(merged_nested.tests, 3);
        assert_eq!(merged_nested.failures, 1);
    }

    #[test]
    fn test_filter_eligible_workers_mixed_nextest_and_cargo_only_and_collisions() {
        let w_nextest = sample_worker(
            "w-nextest",
            Uuid::from_u128(101),
            4,
            Some(4),
            "macos",
            "aarch64",
            &["tool:cargo-nextest@0.9.99"],
        );
        let w_cargo_only = sample_worker(
            "w-cargo",
            Uuid::from_u128(102),
            4,
            Some(4),
            "linux",
            "x86_64",
            &["tool:cargo"],
        );
        let w_plain = sample_worker(
            "w-plain",
            Uuid::from_u128(103),
            4,
            Some(4),
            "linux",
            "x86_64",
            &[],
        );

        let eligible = filter_eligible_workers(
            &[w_nextest, w_cargo_only.clone(), w_plain.clone()],
            &Placement::default(),
            &TaskRequirements::default(),
            false,
            true,
        )
        .unwrap();
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].name, "w-nextest");

        // When only cargo-only + plain workers exist, retains cargo-only.
        let eligible_cargo = filter_eligible_workers(
            &[w_cargo_only, w_plain],
            &Placement::default(),
            &TaskRequirements::default(),
            false,
            true,
        )
        .unwrap();
        assert_eq!(eligible_cargo.len(), 1);
        assert_eq!(eligible_cargo[0].name, "w-cargo");

        // Collision checks: .oxideswarm-nextest.toml in manifest triggers `-1` suffix; Cargo.toml collision check.
        let manifest = TreeManifest::new(vec![
            BlobFile {
                path: "Cargo.toml".to_string(),
                id: BlobId::of(b"cargo"),
                size: 10,
                executable: false,
            },
            BlobFile {
                path: ".oxideswarm-nextest.toml".to_string(),
                id: BlobId::of(b"cfg"),
                size: 10,
                executable: false,
            },
        ])
        .unwrap();
        assert!(check_build_once_path_collision("Cargo.toml", &manifest, &[]).is_err());
        let (_groups, cfg) = plan_build_once_groups(
            &[ShardAssignment {
                index: 1,
                total: 1,
                slug: shard_slug(1, 1),
                worker_id: Some(Uuid::from_u128(101)),
                worker_name: "w-nextest".to_string(),
                worker_target_key: Uuid::from_u128(101).to_string(),
                os: "macos".to_string(),
                arch: "aarch64".to_string(),
                slots: 4,
                placement: Placement::default(),
                preferred_worker: None,
            }],
            &BTreeMap::new(),
            None,
            &manifest,
            &[],
            true,
        )
        .unwrap();
        assert_eq!(cfg.as_deref(), Some(".oxideswarm-nextest-1.toml"));
    }

    #[tokio::test]
    async fn test_execute_build_once_partial_group_cancel_empty_and_omitted_archive() {
        use rusty_grid_core::protocol::WireCodec;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::net::TcpListener;

        #[derive(Clone, Copy)]
        enum Mode {
            PartialFailCancelsSibling,
            ZeroByteArchive,
            OmittedArchive,
        }

        async fn run_mode(
            mode: Mode,
        ) -> (
            Result<BTreeMap<(String, String), BuildOnceOutcome>, String>,
            bool,
        ) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let cancelled_group1 = Arc::new(AtomicBool::new(false));
            let cancelled_flag = Arc::clone(&cancelled_group1);
            let cancel_notify = Arc::new(tokio::sync::Notify::new());
            let cancel_notify_srv = Arc::clone(&cancel_notify);

            let srv_handle = tokio::spawn(async move {
                let mut tasks = Vec::new();
                let expected_conns = match mode {
                    Mode::PartialFailCancelsSibling => 3, // 2 SubmitTask + 1 CancelTask
                    Mode::ZeroByteArchive | Mode::OmittedArchive => 1,
                };
                for _ in 0..expected_conns {
                    let Ok(Ok((stream, _))) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept()).await
                    else {
                        break;
                    };
                    let cf = Arc::clone(&cancelled_flag);
                    let cn = Arc::clone(&cancel_notify_srv);
                    let m = match mode {
                        Mode::PartialFailCancelsSibling => 0,
                        Mode::ZeroByteArchive => 1,
                        Mode::OmittedArchive => 2,
                    };
                    tasks.push(tokio::spawn(async move {
                        let mut transport = MessageTransport::with_codec(
                            GridStream::Tcp(stream),
                            WireCodec::Bincode,
                        );
                        let Ok(Some(msg)) = transport.recv_msg::<ClientMessage>().await else {
                            return;
                        };
                        match msg {
                            ClientMessage::SubmitTask { task, .. } => {
                                let is_linux = task.tags.iter().any(|t| t == "require:os=linux");
                                let archive_rel = match &task.spec {
                                    TaskSpec::Command { args, .. } => {
                                        args.last().cloned().unwrap_or_default()
                                    }
                                    _ => String::new(),
                                };
                                match (m, is_linux) {
                                    (0, true) => {
                                        // Group 1 (linux): wait until CancelTask arrives.
                                        let _ = tokio::time::timeout(
                                            Duration::from_secs(5),
                                            cn.notified(),
                                        )
                                        .await;
                                        let _ = transport
                                            .send_msg(&ClientResponse::TaskCompleted {
                                                task_id: task.id,
                                                result: TaskResult::failure(
                                                    Uuid::nil(),
                                                    task.id,
                                                    130,
                                                    Bytes::new(),
                                                    Bytes::new(),
                                                    5,
                                                    Some("Cancelled".into()),
                                                ),
                                            })
                                            .await;
                                    }
                                    (0, false) => {
                                        // Group 2 (macos): fail immediately.
                                        tokio::time::sleep(Duration::from_millis(30)).await;
                                        let _ = transport
                                            .send_msg(&ClientResponse::TaskCompleted {
                                                task_id: task.id,
                                                result: TaskResult::failure(
                                                    Uuid::nil(),
                                                    task.id,
                                                    2,
                                                    Bytes::new(),
                                                    Bytes::from("group2 compile failed"),
                                                    10,
                                                    Some("compile failed".into()),
                                                ),
                                            })
                                            .await;
                                    }
                                    (1, _) => {
                                        let mut res = TaskResult::success(
                                            Uuid::nil(),
                                            task.id,
                                            "ok",
                                            5,
                                            false,
                                        );
                                        res.outputs.push(BlobFile {
                                            path: archive_rel,
                                            id: BlobId::of(b""),
                                            size: 0,
                                            executable: false,
                                        });
                                        let _ = transport
                                            .send_msg(&ClientResponse::TaskCompleted {
                                                task_id: task.id,
                                                result: res,
                                            })
                                            .await;
                                    }
                                    _ => {
                                        let res = TaskResult::success(
                                            Uuid::nil(),
                                            task.id,
                                            "ok",
                                            5,
                                            false,
                                        );
                                        let _ = transport
                                            .send_msg(&ClientResponse::TaskCompleted {
                                                task_id: task.id,
                                                result: res,
                                            })
                                            .await;
                                    }
                                }
                            }
                            ClientMessage::CancelTask { task_id } => {
                                cf.store(true, Ordering::Release);
                                cn.notify_waiters();
                                let _ = transport
                                    .send_msg(&ClientResponse::TaskCancelled {
                                        task_id,
                                        success: true,
                                    })
                                    .await;
                            }
                            _ => {}
                        }
                        transport.close().await;
                    }));
                }
                for t in tasks {
                    let _ = t.await;
                }
            });

            let groups = match mode {
                Mode::PartialFailCancelsSibling => vec![
                    BuildOnceGroup {
                        os: "linux".to_string(),
                        arch: "x86_64".to_string(),
                        archive_rel: ".oxideswarm-build-linux-x86_64.tar.zst".to_string(),
                        required_tool: None,
                        pinned_worker: None,
                    },
                    BuildOnceGroup {
                        os: "macos".to_string(),
                        arch: "aarch64".to_string(),
                        archive_rel: ".oxideswarm-build-macos-aarch64.tar.zst".to_string(),
                        required_tool: None,
                        pinned_worker: None,
                    },
                ],
                Mode::ZeroByteArchive | Mode::OmittedArchive => vec![BuildOnceGroup {
                    os: "macos".to_string(),
                    arch: "aarch64".to_string(),
                    archive_rel: ".oxideswarm-build-macos-aarch64.tar.zst".to_string(),
                    required_tool: None,
                    pinned_worker: None,
                }],
            };

            let connect = Arc::new(move || {
                Box::pin(async move {
                    let stream = tokio::net::TcpStream::connect(addr)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(MessageTransport::with_codec(
                        GridStream::Tcp(stream),
                        WireCodec::Bincode,
                    ))
                })
            });

            let manifest_input = TaskInput::tree(".", BlobId::of(b"tree"), 4);
            let res = execute_build_once(
                &groups,
                &manifest_input,
                &[],
                &Placement::default(),
                &TaskRequirements::default(),
                &HashMap::new(),
                false,
                &[],
                Duration::from_secs(10),
                None,
                connect,
            )
            .await;
            let _ = tokio::time::timeout(Duration::from_secs(2), srv_handle).await;
            (res, cancelled_group1.load(Ordering::Acquire))
        }

        let t0 = std::time::Instant::now();
        let (partial_res, sibling_cancelled) = run_mode(Mode::PartialFailCancelsSibling).await;
        let elapsed = t0.elapsed();
        let partial_err = partial_res.unwrap_err();
        assert!(
            partial_err.contains("group2 compile failed"),
            "{partial_err}"
        );
        assert!(
            sibling_cancelled,
            "sibling builder must receive CancelTask on partial failure"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "partial build-once failure took {elapsed:?} (> 2s)"
        );

        let (empty_res, _) = run_mode(Mode::ZeroByteArchive).await;
        let empty_err = empty_res.unwrap_err();
        assert!(empty_err.contains("is empty (0 bytes)"), "{empty_err}");

        let (omitted_res, _) = run_mode(Mode::OmittedArchive).await;
        let omitted_err = omitted_res.unwrap_err();
        assert!(
            omitted_err.contains("did not produce expected output"),
            "{omitted_err}"
        );
    }
}
