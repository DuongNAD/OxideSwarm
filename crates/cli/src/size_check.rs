//! Source file size check (`ox-mode test size`, also part of `ox-mode test lint`, design 0080).
//!
//! A Rust source file whose production part exceeds [`MAX_PRODUCTION_LINES`] fails, except files
//! on the allowlist `scripts/rust-file-size-allowlist.txt`, which may only shrink.
//!
//! The production part is the lines before the file's first `#[cfg(test)]` line (the whole file
//! when there is none). Files that hold only tests are exempt: anything under a `tests/`
//! directory, `tests.rs`, and `*_tests.rs`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusty_grid_core::mode::{ModeExecutionResult, WorkflowMode};

/// A file whose production part is longer than this fails unless it is allowlisted.
pub const MAX_PRODUCTION_LINES: usize = 3000;

/// Path of the allowlist, relative to the workspace root.
pub const ALLOWLIST_PATH: &str = "scripts/rust-file-size-allowlist.txt";

/// Files measured and reported even when within the limit, so the output shows how close they are.
const REPORT_FLOOR: usize = 2500;

const SKIPPED_DIRS: &[&str] = &["target", "node_modules"];

/// Number of lines before the first line that starts (after indentation) with `#[cfg(test)]`.
pub fn production_lines(source: &str) -> usize {
    let mut count = 0;
    for line in source.lines() {
        if line.trim_start().starts_with("#[cfg(test)]") {
            return count;
        }
        count += 1;
    }
    count
}

/// True for files that hold only tests and are therefore not production code.
pub fn is_test_only_file(rel_path: &str) -> bool {
    let name = rel_path.rsplit('/').next().unwrap_or(rel_path);
    rel_path.split('/').any(|seg| seg == "tests")
        || name == "tests.rs"
        || name.ends_with("_tests.rs")
}

/// Parses the allowlist: `path max_lines` per line, `#` starts a comment.
pub fn parse_allowlist(text: &str) -> Result<BTreeMap<String, usize>, String> {
    let mut map = BTreeMap::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let (path, max) = match parts.as_slice() {
            [p, m] => (*p, m.parse::<usize>().ok()),
            _ => ("", None),
        };
        let Some(max) = max else {
            return Err(format!(
                "{ALLOWLIST_PATH}:{}: expected `<path> <max_lines>`, got `{raw}`",
                i + 1
            ));
        };
        if max <= MAX_PRODUCTION_LINES {
            return Err(format!(
                "{ALLOWLIST_PATH}:{}: `{path}` has maximum {max}, which is not above the limit {MAX_PRODUCTION_LINES}; remove the entry",
                i + 1
            ));
        }
        if map.insert(path.to_string(), max).is_some() {
            return Err(format!(
                "{ALLOWLIST_PATH}:{}: duplicate entry `{path}`",
                i + 1
            ));
        }
    }
    Ok(map)
}

/// Outcome of comparing measured sizes with the allowlist.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SizeReport {
    /// Violations: the check fails when this is not empty.
    pub failures: Vec<String>,
    /// Entries that can be lowered or removed: reported, never a failure.
    pub warnings: Vec<String>,
}

/// Compares `(relative path, production lines)` pairs with the allowlist.
pub fn evaluate(measured: &[(String, usize)], allowlist: &BTreeMap<String, usize>) -> SizeReport {
    let mut report = SizeReport::default();
    for (path, lines) in measured {
        match allowlist.get(path) {
            None if *lines > MAX_PRODUCTION_LINES => report.failures.push(format!(
                "{path}: production part has {lines} lines, above the limit {MAX_PRODUCTION_LINES}; split the file (do not add it to the allowlist)"
            )),
            Some(max) if lines > max => report.failures.push(format!(
                "{path}: production part has {lines} lines, above its allowlisted maximum {max}; the allowlist may only shrink"
            )),
            Some(max) if lines < max => report.warnings.push(if *lines > MAX_PRODUCTION_LINES {
                format!("{path}: now {lines} lines, lower its allowlist maximum from {max} to {lines}")
            } else {
                format!("{path}: now {lines} lines, at or below the limit; remove its allowlist entry (was {max})")
            }),
            _ => {}
        }
    }
    for path in allowlist.keys() {
        if !measured.iter().any(|(p, _)| p == path) {
            report.warnings.push(format!(
                "{path}: allowlisted but not found (or test-only); remove its allowlist entry"
            ));
        }
    }
    report
}

fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, usize)>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let Ok(ty) = entry.file_type() else { continue };
        if ty.is_symlink() {
            continue;
        }
        if ty.is_dir() {
            if name.starts_with('.') || SKIPPED_DIRS.contains(&name.as_str()) {
                continue;
            }
            walk(&path, root, out);
        } else if name.ends_with(".rs") {
            let rel: PathBuf = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
            let rel = rel.to_string_lossy().replace('\\', "/");
            if is_test_only_file(&rel) {
                continue;
            }
            if let Ok(src) = fs::read_to_string(&path) {
                out.push((rel, production_lines(&src)));
            }
        }
    }
}

/// Measures every non-test-only `*.rs` file under the workspace root (skipping `target/`,
/// `node_modules/` and hidden directories).
pub fn measure_workspace(root: &Path) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

/// Runs the check for `workspace_root` and renders it as a mode result (`test size`).
pub fn run_size_check(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Test, "size");

    let allowlist = match fs::read_to_string(workspace_root.join(ALLOWLIST_PATH))
        .map_err(|e| format!("cannot read {ALLOWLIST_PATH}: {e}"))
        .and_then(|t| parse_allowlist(&t))
    {
        Ok(a) => a,
        Err(e) => {
            res.exit_code = Some(1);
            res.stderr = format!("{e}\n");
            res.duration_ms = start.elapsed().as_millis() as u64;
            return res;
        }
    };

    let mut measured = measure_workspace(workspace_root);
    measured.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let report = evaluate(&measured, &allowlist);

    let mut out = format!(
        "Production lines (before the first #[cfg(test)]); limit {MAX_PRODUCTION_LINES}, files above {REPORT_FLOOR} listed:\n"
    );
    for (path, lines) in measured.iter().filter(|(_, l)| *l > REPORT_FLOOR) {
        let note = match allowlist.get(path) {
            Some(max) => format!("allowlisted, max {max}"),
            None if *lines > MAX_PRODUCTION_LINES => "OVER LIMIT".to_string(),
            None => "ok".to_string(),
        };
        out.push_str(&format!("  {lines:>6}  {path}  ({note})\n"));
    }
    out.push_str(&format!(
        "{} files measured, {} allowlisted.\n",
        measured.len(),
        allowlist.len()
    ));
    for w in &report.warnings {
        out.push_str(&format!("warning: {w}\n"));
    }
    let mut err = String::new();
    for f in &report.failures {
        err.push_str(&format!("error: {f}\n"));
    }
    res.success = report.failures.is_empty();
    res.exit_code = Some(if res.success { 0 } else { 1 });
    res.stdout = out;
    res.stderr = err;
    res.duration_ms = start.elapsed().as_millis() as u64;
    res.with_metric("files_measured", measured.len())
        .with_metric("failures", report.failures.len())
        .with_metric("warnings", report.warnings.len())
}

/// Adds the size check as part 3 of a `test lint` result; a failure fails the lint.
pub fn fold_into_lint(mut lint: ModeExecutionResult, workspace_root: &Path) -> ModeExecutionResult {
    let size = run_size_check(workspace_root);
    lint.stdout.push_str("\n=== 3. Source File Size ===\n");
    lint.stdout.push_str(&size.stdout);
    lint.stderr.push_str(&size.stderr);
    if !size.success {
        lint.success = false;
        lint.exit_code = Some(1);
    }
    lint.with_metric("size_passed", size.success)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(path: &str, n: usize) -> (String, usize) {
        (path.to_string(), n)
    }

    #[test]
    fn production_part_stops_at_first_cfg_test() {
        assert_eq!(production_lines("a\nb\n#[cfg(test)]\nmod tests {}\nc\n"), 2);
        assert_eq!(production_lines("a\nb\n    #[cfg(test)]\nx\n"), 2);
        assert_eq!(production_lines("a\nb\nc\n"), 3);
        assert_eq!(production_lines(""), 0);
        assert_eq!(production_lines("#[cfg(test)]\nmod tests;\n"), 0);
    }

    #[test]
    fn test_only_files_are_recognised() {
        assert!(is_test_only_file("crates/core/tests/data.rs"));
        assert!(is_test_only_file("tests/e2e.rs"));
        assert!(is_test_only_file("crates/worker/src/client/tests.rs"));
        assert!(is_test_only_file(
            "crates/master/src/queue/recovery_tests.rs"
        ));
        assert!(!is_test_only_file("crates/master/src/queue.rs"));
        assert!(!is_test_only_file("crates/cli/src/contests.rs"));
    }

    #[test]
    fn allowlist_parsing_rejects_bad_lines() {
        let ok = parse_allowlist("# c\n\na.rs 3500  # note\nb.rs 4000\n").unwrap();
        assert_eq!(ok.get("a.rs"), Some(&3500));
        assert_eq!(ok.len(), 2);
        assert!(parse_allowlist("a.rs\n").is_err());
        assert!(parse_allowlist("a.rs x\n").is_err());
        assert!(parse_allowlist("a.rs 3000\n").is_err());
        assert!(parse_allowlist("a.rs 3500\na.rs 3600\n").is_err());
    }

    #[test]
    fn unlisted_file_over_limit_fails_and_at_limit_passes() {
        let none = BTreeMap::new();
        assert!(evaluate(&[m("a.rs", 3000)], &none).failures.is_empty());
        let r = evaluate(&[m("a.rs", 3001)], &none);
        assert_eq!(r.failures.len(), 1);
        assert!(r.failures[0].contains("a.rs"));
    }

    #[test]
    fn allowlisted_file_may_not_grow_but_may_shrink() {
        let allow = parse_allowlist("a.rs 3500\n").unwrap();
        assert!(evaluate(&[m("a.rs", 3500)], &allow).failures.is_empty());
        let grown = evaluate(&[m("a.rs", 3501)], &allow);
        assert_eq!(grown.failures.len(), 1);
        let shrunk = evaluate(&[m("a.rs", 3400)], &allow);
        assert!(shrunk.failures.is_empty());
        assert_eq!(shrunk.warnings.len(), 1);
        assert!(shrunk.warnings[0].contains("lower"));
        let gone = evaluate(&[m("a.rs", 2900)], &allow);
        assert!(gone.failures.is_empty());
        assert!(gone.warnings[0].contains("remove"));
    }

    #[test]
    fn stale_allowlist_entry_warns() {
        let allow = parse_allowlist("gone.rs 3500\n").unwrap();
        let r = evaluate(&[], &allow);
        assert!(r.failures.is_empty());
        assert_eq!(r.warnings.len(), 1);
    }

    #[test]
    fn workspace_allowlist_holds_for_this_checkout() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let res = run_size_check(&root);
        assert!(res.success, "{}\n{}", res.stdout, res.stderr);
    }
}
