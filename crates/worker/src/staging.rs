//! Placing a task's blob inputs into its sandbox and collecting its output files from it
//! (`Task::inputs`, `Task::output_globs`; design record
//! `docs/design/0006-content-addressed-data-plane.md`).
//!
//! Both directions treat the paths as untrusted:
//!
//! * **Inputs.** Every destination is a canonical relative path
//!   ([`rusty_grid_core::blob::validate_blob_path`]), checked again here with the rules of this
//!   platform (Windows device names, trailing dots). Files are created with `create_new`, so an
//!   input never overwrites another one or follows a link placed in the sandbox.
//! * **Outputs.** The walk never follows a symbolic link (a task could point one at the worker's
//!   secrets) and takes regular files only. What the patterns select is capped in count and total
//!   size ([`OutputLimits`]); exceeding a cap fails the task with a message naming the cap, the
//!   size found and the largest file. A pattern without wildcards that matches nothing fails a
//!   task that otherwise succeeded: a declared output that was not produced is an error.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusty_grid_core::blob::{
    join_blob_path, validate_blob_path, BlobFile, InputSource, OutputPattern, TaskInput,
    TreeManifest, MAX_MANIFEST_BYTES,
};

use crate::blob_cache::{BlobCache, CachePin};

/// Default cap on the total size of a task's output files: 1 GiB.
pub const DEFAULT_OUTPUT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
/// Default cap on the number of a task's output files.
pub const DEFAULT_MAX_OUTPUT_FILES: usize = 10_000;
/// Directory entries a collection walk may visit before it gives up.
const MAX_WALK_ENTRIES: usize = 1_000_000;

/// Caps on what output patterns may bring back from one task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLimits {
    pub max_bytes: u64,
    pub max_files: usize,
}

impl Default for OutputLimits {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_OUTPUT_LIMIT_BYTES,
            max_files: DEFAULT_MAX_OUTPUT_FILES,
        }
    }
}

/// The absolute path of the sandbox-relative `rel`, after checking it with this platform's
/// rules.
fn sandbox_path(root: &Path, rel: &str) -> Result<PathBuf, String> {
    validate_blob_path(rel)?;
    #[cfg(windows)]
    crate::sandbox::normalize_relative_path_str(rel).map_err(|e| e.to_string())?;
    let mut path = root.to_path_buf();
    path.extend(rel.split('/'));
    Ok(path)
}

/// Places every input of a task into the sandbox at `root`. All blobs must already be in
/// `cache` (the worker fetches them before the run). Returns the number of files placed.
pub fn materialize_inputs(
    root: &Path,
    inputs: &[TaskInput],
    cache: &BlobCache,
) -> Result<usize, String> {
    let mut placed = 0;
    for input in inputs {
        match &input.source {
            InputSource::File { id, executable, .. } => {
                place(root, &input.path, id, *executable, cache)?;
                placed += 1;
            }
            InputSource::Tree { manifest, .. } => {
                let bytes = cache
                    .read_verified(manifest, MAX_MANIFEST_BYTES)
                    .map_err(|e| format!("input '{}': directory manifest: {e}", input.path))?;
                let tree = TreeManifest::decode(&bytes)
                    .map_err(|e| format!("input '{}': {e}", input.path))?;
                for file in tree.files() {
                    let rel = join_blob_path(&input.path, &file.path);
                    place(root, &rel, &file.id, file.executable, cache)?;
                    placed += 1;
                }
            }
        }
    }
    Ok(placed)
}

fn place(
    root: &Path,
    rel: &str,
    id: &rusty_grid_core::blob::BlobId,
    executable: bool,
    cache: &BlobCache,
) -> Result<(), String> {
    let dest = sandbox_path(root, rel).map_err(|e| format!("input '{rel}': {e}"))?;
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("input '{rel}': cannot create its directory: {e}"))?;
    }
    cache.materialize(id, &dest, executable).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            format!("input '{rel}' collides with another input or a file the task already has")
        } else {
            format!("input '{rel}': {e}")
        }
    })
}

/// A file a pattern selected.
struct Found {
    path: PathBuf,
    size: u64,
}

/// Collects the files the `patterns` select in the sandbox at `root` into `cache` and describes
/// them as blobs, sorted by path. `process_failed` relaxes one rule: a failed task may well not
/// have produced a declared output, and that is not worth a second error.
pub fn collect_outputs(
    root: &Path,
    patterns: &[String],
    cache: &BlobCache,
    limits: OutputLimits,
    process_failed: bool,
) -> Result<Vec<BlobFile>, String> {
    collect_outputs_inner(root, patterns, cache, limits, process_failed, true, None)
}

/// Like [`collect_outputs`], and pins each file in `cache` at the moment it enters the cache so
/// a concurrent [`BlobCache::evict_to_fit`] cannot evict it before its upload finishes. If
/// collection fails partway through, the partial pin is dropped before returning.
pub fn collect_outputs_pinned(
    root: &Path,
    patterns: &[String],
    cache: &Arc<BlobCache>,
    limits: OutputLimits,
    process_failed: bool,
) -> Result<(Vec<BlobFile>, CachePin), String> {
    collect_outputs_from_dir_pinned(root, patterns, cache, limits, process_failed, true)
}

/// Like [`collect_outputs_pinned`], with `allow_move` controlling whether `cache` may move files
/// out of `root` (pass `false` for persistent workspace trees so outputs remain in place).
pub fn collect_outputs_from_dir_pinned(
    root: &Path,
    patterns: &[String],
    cache: &Arc<BlobCache>,
    limits: OutputLimits,
    process_failed: bool,
    allow_move: bool,
) -> Result<(Vec<BlobFile>, CachePin), String> {
    let mut pin = cache.pin(std::iter::empty());
    let outputs = collect_outputs_inner(
        root,
        patterns,
        cache,
        limits,
        process_failed,
        allow_move,
        Some(&mut pin),
    )?;
    Ok((outputs, pin))
}

fn collect_outputs_inner(
    root: &Path,
    patterns: &[String],
    cache: &BlobCache,
    limits: OutputLimits,
    process_failed: bool,
    allow_move: bool,
    mut pin: Option<&mut CachePin>,
) -> Result<Vec<BlobFile>, String> {
    let patterns = patterns
        .iter()
        .map(|p| OutputPattern::parse(p))
        .collect::<Result<Vec<_>, _>>()?;
    let root = root
        .canonicalize()
        .map_err(|e| format!("cannot read the task sandbox: {e}"))?;

    let mut found: BTreeMap<String, Found> = BTreeMap::new();
    let mut not_produced = Vec::new();
    let mut visited = 0usize;
    'patterns: for pattern in &patterns {
        let prefix = pattern.literal_prefix();
        let start = if prefix.is_empty() {
            root.clone()
        } else {
            match sandbox_path(&root, &prefix) {
                Ok(path) => path,
                Err(e) => return Err(format!("output '{}': {e}", pattern.as_str())),
            }
        };
        if !prefix.is_empty() {
            let parts: Vec<&str> = prefix.split('/').collect();
            let mut ancestor = root.clone();
            for part in &parts[..parts.len() - 1] {
                ancestor.push(part);
                if let Ok(meta) = std::fs::symlink_metadata(&ancestor) {
                    if meta.file_type().is_symlink() {
                        if pattern.is_literal() {
                            return Err(format!(
                                "output '{}' traverses a symbolic link (symbolic links are never returned)",
                                pattern.as_str()
                            ));
                        }
                        continue 'patterns;
                    }
                }
            }
        }
        let meta = match std::fs::symlink_metadata(&start) {
            Ok(meta) => meta,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                if pattern.is_literal() {
                    not_produced.push(pattern.as_str().to_string());
                }
                continue;
            }
            Err(e) => {
                return Err(format!("cannot read output '{}': {e}", pattern.as_str()));
            }
        };
        if meta.file_type().is_symlink() {
            if pattern.is_literal() {
                return Err(format!(
                    "output '{}' is not a regular file or directory (symbolic links are never returned)",
                    pattern.as_str()
                ));
            }
            continue;
        }
        if meta.is_file() {
            if pattern.selects(&prefix) {
                found.entry(prefix.clone()).or_insert(Found {
                    path: start,
                    size: meta.len(),
                });
            }
        } else if meta.is_dir() {
            walk(&start, &prefix, pattern, &mut found, &mut visited)?;
        } else if pattern.is_literal() {
            return Err(format!(
                "output '{}' is not a regular file or directory (symbolic links are never returned)",
                pattern.as_str()
            ));
        }
    }

    if !not_produced.is_empty() && !process_failed {
        return Err(format!(
            "the task did not produce {} (declared with --output; use a wildcard pattern for optional outputs)",
            not_produced
                .iter()
                .map(|p| format!("'{p}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let total: u64 = found.values().map(|f| f.size).sum();
    if found.len() > limits.max_files || total > limits.max_bytes {
        let (largest_path, largest) = found
            .iter()
            .max_by_key(|(_, f)| f.size)
            .map(|(p, f)| (p.as_str(), f.size))
            .unwrap_or(("", 0));
        return Err(format!(
            "the output patterns select {} files, {} in total (largest: '{largest_path}', {}); \
             this worker returns at most {} files and {} per task: narrow the patterns or raise \
             `worker --output-limit-mb`",
            found.len(),
            human_bytes(total),
            human_bytes(largest),
            limits.max_files,
            human_bytes(limits.max_bytes)
        ));
    }

    let mut outputs = Vec::with_capacity(found.len());
    for (rel, file) in found {
        let executable = is_executable(&file.path);
        let (id, size) = match pin.as_deref_mut() {
            Some(pin) => cache.insert_file_pinned(&file.path, allow_move, pin),
            None => cache.insert_file(&file.path, allow_move),
        }
        .map_err(|e| format!("output '{rel}': {e}"))?;
        outputs.push(BlobFile {
            path: rel,
            id,
            size,
            executable,
        });
    }
    Ok(outputs)
}

/// Adds the regular files below `dir` (sandbox path `rel_dir`) that `pattern` selects.
fn walk(
    dir: &Path,
    rel_dir: &str,
    pattern: &OutputPattern,
    found: &mut BTreeMap<String, Found>,
    visited: &mut usize,
) -> Result<(), String> {
    let mut stack = vec![(dir.to_path_buf(), rel_dir.to_string(), true)];
    while let Some((dir, rel_dir, dir_utf8)) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| format!("cannot read output directory '{rel_dir}': {e}"))?;
        for entry in entries {
            let entry =
                entry.map_err(|e| format!("cannot read output directory '{rel_dir}': {e}"))?;
            *visited += 1;
            if *visited > MAX_WALK_ENTRIES {
                return Err(format!(
                    "the task sandbox has more than {MAX_WALK_ENTRIES} entries below the output patterns; narrow them"
                ));
            }
            let file_type = entry
                .file_type()
                .map_err(|e| format!("cannot read output '{}': {e}", entry.path().display()))?;
            let file_name = entry.file_name();
            let name_utf8 = file_name.to_str().is_some();
            let lossy_name = file_name.to_string_lossy();
            let rel = if rel_dir.is_empty() {
                lossy_name.into_owned()
            } else {
                format!("{rel_dir}/{lossy_name}")
            };
            let entry_utf8 = dir_utf8 && name_utf8;
            if file_type.is_dir() {
                stack.push((entry.path(), rel, entry_utf8));
            } else if file_type.is_file() && pattern.selects(&rel) {
                check_selected_entry_path(&rel, entry_utf8)?;
                let size = entry
                    .metadata()
                    .map_err(|e| format!("cannot read output '{rel}': {e}"))?
                    .len();
                found.entry(rel).or_insert(Found {
                    path: entry.path(),
                    size,
                });
            }
        }
    }
    Ok(())
}

fn check_selected_entry_path(rel: &str, entry_utf8: bool) -> Result<(), String> {
    if !entry_utf8 {
        return Err(format!("output '{rel}' has a name that is not valid UTF-8"));
    }
    validate_blob_path(rel).map_err(|e| format!("output '{rel}': {e}"))
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

/// `1.5 MiB`-style sizes for messages.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = "B";
    for next in UNITS {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next;
    }
    format!("{value:.1} {unit}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::blob::{BlobId, SANDBOX_ROOT};
    use std::sync::Arc;

    fn setup() -> (tempfile::TempDir, PathBuf, Arc<BlobCache>) {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = dir.path().join("sandbox");
        std::fs::create_dir_all(&sandbox).unwrap();
        let cache = BlobCache::open(&dir.path().join("cache"), 1 << 30).unwrap();
        (dir, sandbox, cache)
    }

    fn cache_blob(cache: &BlobCache, dir: &Path, data: &[u8]) -> BlobId {
        let src = dir.join(format!("src-{}", BlobId::of(data).short()));
        std::fs::write(&src, data).unwrap();
        cache.insert_file(&src, true).unwrap().0
    }

    fn write(root: &Path, rel: &str, data: &[u8]) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }

    #[test]
    fn files_and_trees_are_placed_where_the_task_says() {
        let (dir, sandbox, cache) = setup();
        let main = cache_blob(&cache, dir.path(), b"fn main() {}");
        let readme = cache_blob(&cache, dir.path(), b"# readme");
        let tree = TreeManifest::new(vec![
            BlobFile {
                path: "src/main.rs".into(),
                id: main,
                size: 12,
                executable: false,
            },
            BlobFile {
                path: "README.md".into(),
                id: readme,
                size: 8,
                executable: false,
            },
        ])
        .unwrap();
        let encoded = tree.encode();
        let manifest = cache_blob(&cache, dir.path(), &encoded);
        let inputs = vec![
            TaskInput::tree("proj", manifest, encoded.len() as u64),
            TaskInput::tree(SANDBOX_ROOT, manifest, encoded.len() as u64),
            TaskInput::file("bin/run.sh", readme, 8, true),
        ];
        assert_eq!(materialize_inputs(&sandbox, &inputs, &cache).unwrap(), 5);
        assert_eq!(
            std::fs::read(sandbox.join("proj/src/main.rs")).unwrap(),
            b"fn main() {}"
        );
        assert_eq!(
            std::fs::read(sandbox.join("src/main.rs")).unwrap(),
            b"fn main() {}"
        );
        assert_eq!(
            std::fs::read(sandbox.join("bin/run.sh")).unwrap(),
            b"# readme"
        );
    }

    #[test]
    fn inputs_never_overwrite_each_other_or_escape() {
        let (dir, sandbox, cache) = setup();
        let id = cache_blob(&cache, dir.path(), b"x");
        let clash = vec![
            TaskInput::file("a.txt", id, 1, false),
            TaskInput::file("a.txt", id, 1, false),
        ];
        assert!(materialize_inputs(&sandbox, &clash, &cache)
            .unwrap_err()
            .contains("collides"));
        let escape = vec![TaskInput::file("sub/../../a.txt", id, 1, false)];
        assert!(materialize_inputs(&sandbox, &escape, &cache).is_err());
        assert!(!dir.path().join("a.txt").exists());
        let twice = vec![
            TaskInput::file("b.txt", id, 1, false),
            TaskInput::tree(SANDBOX_ROOT, id, 1),
        ];
        // The second input names a blob that is not a manifest.
        assert!(materialize_inputs(&sandbox, &twice, &cache)
            .unwrap_err()
            .contains("manifest"));
        let missing = vec![TaskInput::file("c.txt", BlobId::of(b"absent"), 6, false)];
        assert!(materialize_inputs(&sandbox, &missing, &cache)
            .unwrap_err()
            .contains("not in the worker's blob cache"));
    }

    #[test]
    fn patterns_collect_files_and_directories_into_the_cache() {
        let (_dir, sandbox, cache) = setup();
        write(&sandbox, "out/report.txt", b"report");
        write(&sandbox, "out/deep/data.bin", b"\x00\x01");
        write(&sandbox, "logs/a.log", b"log a");
        write(&sandbox, "logs/a.txt", b"not selected");
        write(&sandbox, "input.txt", b"not selected either");
        let patterns = vec!["out".to_string(), "logs/*.log".to_string()];
        let outputs =
            collect_outputs(&sandbox, &patterns, &cache, OutputLimits::default(), false).unwrap();
        let paths: Vec<&str> = outputs.iter().map(|o| o.path.as_str()).collect();
        assert_eq!(paths, ["logs/a.log", "out/deep/data.bin", "out/report.txt"]);
        let report = &outputs[2];
        assert_eq!(report.id, BlobId::of(b"report"));
        assert_eq!(report.size, 6);
        assert_eq!(
            cache.read_verified(&report.id, 100).unwrap(),
            b"report",
            "outputs are kept in the cache for upload"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_links_are_never_followed_out_of_the_sandbox() {
        let (dir, sandbox, cache) = setup();
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, b"cluster secret").unwrap();
        std::os::unix::fs::symlink(&secret, sandbox.join("leak.txt")).unwrap();
        std::fs::create_dir_all(sandbox.join("out")).unwrap();
        std::os::unix::fs::symlink(dir.path(), sandbox.join("out/escape")).unwrap();
        write(&sandbox, "out/real.txt", b"real");

        let outputs = collect_outputs(
            &sandbox,
            &["out".to_string(), "*.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .unwrap();
        let paths: Vec<&str> = outputs.iter().map(|o| o.path.as_str()).collect();
        assert_eq!(paths, ["out/real.txt"]);

        let err = collect_outputs(
            &sandbox,
            &["leak.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .unwrap_err();
        assert!(err.contains("symbolic links"), "{err}");
    }

    #[test]
    fn symbolic_link_in_parent_directory_of_output_pattern_is_never_followed() {
        let (dir, sandbox, cache) = setup();
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(outside.join("sub")).unwrap();
        std::fs::write(outside.join("important.txt"), b"do not steal").unwrap();
        std::fs::write(outside.join("sub/other.txt"), b"also outside").unwrap();
        write(&sandbox, "inside.txt", b"inside");

        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&outside, sandbox.join("link")).is_ok();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&outside, sandbox.join("link")).is_ok();
        if !linked {
            eprintln!("skipping: host refused to create a directory symlink");
            return;
        }

        // Literal pattern through a symlinked parent directory fails and names the pattern.
        let err = collect_outputs(
            &sandbox,
            &["link/important.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .unwrap_err();
        assert!(err.contains("link/important.txt"), "{err}");
        assert!(err.contains("symbolic link"), "{err}");
        assert_eq!(
            std::fs::read(outside.join("important.txt")).unwrap(),
            b"do not steal",
            "outside file must remain untouched"
        );

        // Wildcard pattern with a symlinked directory in its literal prefix selects nothing,
        // while '**/*.txt' still returns files inside the sandbox and nothing from outside.
        let outputs = collect_outputs(
            &sandbox,
            &["link/sub/*.txt".to_string(), "**/*.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .unwrap();
        let paths: Vec<&str> = outputs.iter().map(|o| o.path.as_str()).collect();
        assert_eq!(paths, ["inside.txt"]);
        assert_eq!(
            std::fs::read(outside.join("sub/other.txt")).unwrap(),
            b"also outside",
            "outside subfile must remain untouched"
        );
    }

    #[test]
    fn exceeding_the_caps_is_a_clear_error() {
        let (_dir, sandbox, cache) = setup();
        write(&sandbox, "big/one.bin", &[0u8; 3000]);
        write(&sandbox, "big/two.bin", &[1u8; 500]);
        let limits = OutputLimits {
            max_bytes: 2048,
            max_files: 10,
        };
        let err =
            collect_outputs(&sandbox, &["big".to_string()], &cache, limits, false).unwrap_err();
        assert!(err.contains("2 files"), "{err}");
        assert!(err.contains("big/one.bin"), "names the largest file: {err}");
        assert!(
            err.contains("--output-limit-mb"),
            "says how to fix it: {err}"
        );

        let few = OutputLimits {
            max_bytes: 1 << 30,
            max_files: 1,
        };
        assert!(collect_outputs(&sandbox, &["big".to_string()], &cache, few, false).is_err());
    }

    #[test]
    fn a_missing_literal_output_fails_only_a_successful_task() {
        let (_dir, sandbox, cache) = setup();
        let patterns = vec!["result.json".to_string(), "maybe/*.txt".to_string()];
        let err = collect_outputs(&sandbox, &patterns, &cache, OutputLimits::default(), false)
            .unwrap_err();
        assert!(err.contains("'result.json'"), "{err}");
        assert!(
            !err.contains("maybe"),
            "a wildcard may match nothing: {err}"
        );
        let outputs =
            collect_outputs(&sandbox, &patterns, &cache, OutputLimits::default(), true).unwrap();
        assert!(outputs.is_empty());

        // A pattern whose literal prefix passes through a regular file (`a/b/*.txt` where `a`
        // is a regular file) produces `NotADirectory` on `symlink_metadata(&start)`: a glob
        // must skip it just like `NotFound`, while a literal pattern reports "did not produce".
        write(&sandbox, "a", b"regular file");
        let glob_below_file = collect_outputs(
            &sandbox,
            &["a/b/*.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .expect("a glob below a regular file must not fail a successful task");
        assert!(glob_below_file.is_empty());

        let literal_err = collect_outputs(
            &sandbox,
            &["a/b/c.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .unwrap_err();
        assert!(
            literal_err.contains("did not produce 'a/b/c.txt'"),
            "{literal_err}"
        );
        assert!(collect_outputs(
            &sandbox,
            &["a/b/c.txt".to_string()],
            &cache,
            OutputLimits::default(),
            true,
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn human_sizes() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GiB");
    }

    #[cfg(unix)]
    #[test]
    fn walk_fails_on_selected_invalid_blob_path_non_utf8_name_or_unreadable_directory() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::PermissionsExt;

        let (_dir, sandbox, cache) = setup();
        std::fs::create_dir_all(sandbox.join("out")).unwrap();

        // 1. A file with ':' in its name is not a valid blob path; if selected, it must fail.
        write(&sandbox, "out/bad:colon.txt", b"colon");
        let err = collect_outputs(
            &sandbox,
            &["out/*.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        )
        .unwrap_err();
        assert!(err.contains("bad:colon.txt"), "{err}");
        std::fs::remove_file(sandbox.join("out/bad:colon.txt")).unwrap();

        // 2. A file with a non-UTF-8 name selected by the pattern must fail (APFS refuses
        //    creating non-UTF-8 filenames with EILSEQ, so test on disk when the filesystem
        //    permits it and always verify `check_selected_entry_path`).
        let non_utf8_os = OsStr::from_bytes(b"bad-\xff.txt");
        assert!(check_selected_entry_path(
            &format!("out/{}", non_utf8_os.to_string_lossy()),
            false
        )
        .unwrap_err()
        .contains("UTF-8"));
        let non_utf8 = sandbox.join("out").join(non_utf8_os);
        if std::fs::write(&non_utf8, b"bytes").is_ok() {
            let err = collect_outputs(
                &sandbox,
                &["out/*.txt".to_string()],
                &cache,
                OutputLimits::default(),
                false,
            )
            .unwrap_err();
            assert!(err.contains("UTF-8"), "{err}");
            let _ = std::fs::remove_file(&non_utf8);
        }

        // 3. A real stat error at `&start` (e.g. an unreadable ancestor directory `locked_parent`
        //    for pattern `locked_parent/sub/*.txt`) must fail collection (catches M3b).
        let locked_parent = sandbox.join("locked_parent");
        std::fs::create_dir_all(locked_parent.join("sub")).unwrap();
        std::fs::write(locked_parent.join("sub/file.txt"), b"secret").unwrap();
        std::fs::set_permissions(&locked_parent, std::fs::Permissions::from_mode(0o000)).unwrap();
        let start_res = collect_outputs(
            &sandbox,
            &["locked_parent/sub/*.txt".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        );
        std::fs::set_permissions(&locked_parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        let start_err =
            start_res.expect_err("unreadable ancestor at start path must fail collection");
        assert!(
            start_err.contains("cannot read output 'locked_parent/sub/*.txt'"),
            "{start_err}"
        );

        // 4. A directory with read permission (`0o400`) but no search/execute permission (`+x`)
        //    lets `read_dir` and `entry.file_type()` succeed on Unix while `entry.metadata()`
        //    fails with `PermissionDenied`; `walk` and `collect_outputs` must fail (catches M3d).
        let no_search = sandbox.join("out/no_search");
        std::fs::create_dir_all(&no_search).unwrap();
        std::fs::write(no_search.join("file.txt"), b"secret").unwrap();
        std::fs::set_permissions(&no_search, std::fs::Permissions::from_mode(0o400)).unwrap();
        let meta_res = collect_outputs(
            &sandbox,
            &["out/**".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        );
        std::fs::set_permissions(&no_search, std::fs::Permissions::from_mode(0o755)).unwrap();
        let meta_err =
            meta_res.expect_err("metadata failure on a selected file must fail collection");
        assert!(
            meta_err.contains("cannot read output 'out/no_search/file.txt'"),
            "{meta_err}"
        );
        std::fs::remove_dir_all(&no_search).unwrap();

        // 5. An unreadable directory (`0o000`) below a pattern must fail at `read_dir`.
        let locked = sandbox.join("out/locked");
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::write(locked.join("file.txt"), b"secret").unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let res = collect_outputs(
            &sandbox,
            &["out/**".to_string()],
            &cache,
            OutputLimits::default(),
            false,
        );
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = res.expect_err("unreadable directory must fail output collection");
        assert!(
            err.contains("cannot read output directory 'out/locked'"),
            "{err}"
        );
    }
}
