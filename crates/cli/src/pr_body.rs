//! Pull request body and verify-table generator (design 0111, D6 / TEAM-5 groundwork).
//!
//! Formats recorded verification outputs (worker, OS, link kind, toolchain, exit result, task id,
//! and verified base commit) into a deterministic GFM Markdown pull request body with stale-base
//! warnings and failure summaries.

use serde::{Deserialize, Serialize};

/// One failing test or compiler diagnostic summary from a verify cell (design 0111, D4.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyFailureSummary {
    pub kind: String,
    pub name: String,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub line: Option<u32>,
    pub message: String,
}

/// Execution summary of one verify profile cell across a target worker/OS (design 0111, D4.3 & D6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyCellRecord {
    pub profile: String,
    pub os: String,
    pub worker: String,
    pub link: String,
    pub toolchain: String,
    pub exit: i32,
    pub secs: f64,
    pub task_id: String,
    #[serde(default)]
    pub failures: Vec<VerifyFailureSummary>,
}

/// Compact verify output produced by the verify gate for one task revision (design 0111, D4.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifyRecord {
    pub task: String,
    #[serde(default)]
    pub title: Option<String>,
    pub commit: String,
    #[serde(default)]
    pub base_commit: Option<String>,
    #[serde(default)]
    pub base_branch: Option<String>,
    pub ok: bool,
    #[serde(default)]
    pub cells: Vec<VerifyCellRecord>,
    #[serde(default)]
    pub omitted: usize,
}

/// Metadata required to render a complete pull request description for a completed task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrBodyOptions {
    pub task_id: String,
    #[serde(default)]
    pub task_title: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub base_branch: Option<String>,
    pub base_commit: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub current_base_commit: Option<String>,
}

/// Parses a single JSON-encoded [`VerifyRecord`].
pub fn parse_verify_record(json_str: &str) -> Result<VerifyRecord, String> {
    serde_json::from_str::<VerifyRecord>(json_str)
        .map_err(|e| format!("invalid verify record JSON: {e}"))
}

/// Parses either a single JSON object, a JSON array of [`VerifyRecord`]s, or newline-delimited
/// JSON objects (JSONL) into a `Vec<VerifyRecord>`.
pub fn parse_verify_records(input: &str) -> Result<Vec<VerifyRecord>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        return serde_json::from_str::<Vec<VerifyRecord>>(trimmed)
            .map_err(|e| format!("invalid verify record array JSON: {e}"));
    }
    if let Ok(single) = serde_json::from_str::<VerifyRecord>(trimmed) {
        return Ok(vec![single]);
    }

    let mut out = Vec::new();
    for (idx, line) in trimmed.lines().enumerate() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let rec = parse_verify_record(l)
            .map_err(|e| format!("invalid verify record at line {}: {e}", idx + 1))?;
        out.push(rec);
    }
    Ok(out)
}

/// Collapses newlines and control characters and escapes Markdown table pipes (`|`) so untrusted
/// output strings never break or inject rows/columns inside a Markdown table cell.
pub fn escape_md_cell(raw: &str) -> String {
    let mut cleaned = String::with_capacity(raw.len());
    let mut prev_space = false;
    for ch in raw.chars() {
        let mapped = if ch.is_control() { ' ' } else { ch };
        if mapped.is_whitespace() {
            if !prev_space && !cleaned.is_empty() {
                cleaned.push(' ');
            }
            prev_space = true;
        } else {
            if mapped == '|' {
                cleaned.push_str("\\|");
            } else {
                cleaned.push(mapped);
            }
            prev_space = false;
        }
    }
    cleaned.trim().to_string()
}

/// Formats one verify cell's result string (`PASS (...)` or `FAIL (...)`).
pub fn format_cell_result(cell: &VerifyCellRecord) -> String {
    let secs = if cell.secs.is_finite() && cell.secs >= 0.0 {
        cell.secs
    } else {
        0.0
    };
    if cell.exit == 0 && cell.failures.is_empty() {
        format!("PASS (exit 0, {secs:.1}s)")
    } else if cell.failures.is_empty() {
        format!("FAIL (exit {}, {secs:.1}s)", cell.exit)
    } else {
        format!(
            "FAIL (exit {}, {secs:.1}s, {} failure(s))",
            cell.exit,
            cell.failures.len()
        )
    }
}

fn short_sha(raw: &str) -> &str {
    let trimmed = raw.trim();
    if trimmed.len() <= 12 {
        trimmed
    } else {
        &trimmed[..12]
    }
}

fn validate_non_empty_token(raw: &str, field: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("{field} cannot be empty"));
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!(
            "{field} ('{raw}') must not contain whitespace or control characters"
        ));
    }
    Ok(trimmed.to_string())
}

/// Renders the GFM Markdown verification table for `records` (design 0111, D6.1).
pub fn render_verify_table(records: &[VerifyRecord], default_base_commit: Option<&str>) -> String {
    let has_any_cell = records.iter().any(|r| !r.cells.is_empty());
    if !has_any_cell {
        return "_(no verify cells recorded)_".to_string();
    }

    let mut lines = vec![
        "| Profile | Worker | OS | Link | Toolchain | Result | Task ID | Base commit |".to_string(),
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |".to_string(),
    ];

    for rec in records {
        let base_ref = rec
            .base_commit
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| default_base_commit.filter(|s| !s.trim().is_empty()))
            .unwrap_or("unknown");
        let base_cell = escape_md_cell(short_sha(base_ref));

        for cell in &rec.cells {
            let row = format!(
                "| {} | {} | {} | {} | `{}` | {} | `{}` | `{}` |",
                escape_md_cell(&cell.profile),
                escape_md_cell(&cell.worker),
                escape_md_cell(&cell.os),
                escape_md_cell(&cell.link),
                escape_md_cell(&cell.toolchain.replace('`', "'")),
                escape_md_cell(&format_cell_result(cell)),
                escape_md_cell(&cell.task_id.replace('`', "'")),
                base_cell.replace('`', "'"),
            );
            lines.push(row);
        }
    }

    lines.join("\n")
}

/// Renders the complete Markdown pull request body from task metadata and verify records
/// (design 0111, D6).
pub fn render_pr_body(opts: &PrBodyOptions, records: &[VerifyRecord]) -> Result<String, String> {
    let task_id = validate_non_empty_token(&opts.task_id, "task_id")?;
    let base_commit = validate_non_empty_token(&opts.base_commit, "base_commit")?;
    let base_branch = opts
        .base_branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("master");

    for rec in records {
        if rec.task.trim() != task_id {
            return Err(format!(
                "verify record task '{}' does not match PR task_id '{task_id}'",
                rec.task
            ));
        }
    }

    let mut out = String::new();
    out.push_str("## Task\n\n");
    let resolved_title = opts
        .task_title
        .as_deref()
        .or_else(|| records.iter().find_map(|r| r.title.as_deref()))
        .map(escape_md_cell)
        .filter(|t| !t.is_empty());

    match resolved_title {
        Some(title) => out.push_str(&format!("- **Task:** `{task_id}` — {title}\n")),
        None => out.push_str(&format!("- **Task:** `{task_id}`\n")),
    }
    if let Some(branch) = opts
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        out.push_str(&format!("- **Branch:** `{}`\n", escape_md_cell(branch)));
    }
    out.push_str(&format!(
        "- **Base verified:** `{}` @ `{}`\n",
        escape_md_cell(base_branch),
        escape_md_cell(short_sha(&base_commit))
    ));

    if let Some(cur_base) = opts
        .current_base_commit
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if !cur_base.eq_ignore_ascii_case(&base_commit)
            && !short_sha(cur_base).eq_ignore_ascii_case(short_sha(&base_commit))
        {
            out.push_str(&format!(
                "- **Stale base warning:** current `{}` is at `{}`, which differs from verified `{}` — re-verify on the latest base before merging.\n",
                escape_md_cell(base_branch),
                escape_md_cell(short_sha(cur_base)),
                escape_md_cell(short_sha(&base_commit))
            ));
        }
    }

    if let Some(summary) = opts
        .summary
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        out.push_str("\n## Summary\n\n");
        out.push_str(summary);
        out.push('\n');
    }

    let total_cells: usize = records.iter().map(|r| r.cells.len()).sum();
    let all_pass = !records.is_empty()
        && total_cells > 0
        && records
            .iter()
            .all(|r| r.ok && r.cells.iter().all(|c| c.exit == 0 && c.failures.is_empty()));
    let overall_status = if total_cells == 0 {
        "UNVERIFIED"
    } else if all_pass {
        "PASS"
    } else {
        "FAIL"
    };

    out.push_str("\n## Verification\n\n");
    out.push_str(&format!(
        "**Status:** `{overall_status}` ({total_cells} cell(s))\n\n"
    ));
    out.push_str(&render_verify_table(records, Some(&base_commit)));
    out.push('\n');

    let mut failure_lines = Vec::new();
    let mut omitted_total = 0usize;
    for rec in records {
        omitted_total = omitted_total.saturating_add(rec.omitted);
        for cell in &rec.cells {
            for f in &cell.failures {
                let loc = match (&f.file, f.line) {
                    (Some(file), Some(line)) => format!(" ({}:{line})", escape_md_cell(file)),
                    (Some(file), None) => format!(" ({})", escape_md_cell(file)),
                    _ => String::new(),
                };
                failure_lines.push(format!(
                    "- `{}` on `{}` [{}]: `{}`{loc} — {}",
                    escape_md_cell(&cell.profile),
                    escape_md_cell(&cell.worker),
                    escape_md_cell(&f.kind),
                    escape_md_cell(&f.name),
                    escape_md_cell(&f.message),
                ));
            }
        }
    }

    if !failure_lines.is_empty() || omitted_total > 0 {
        out.push_str("\n### Failures\n\n");
        for line in failure_lines {
            out.push_str(&line);
            out.push('\n');
        }
        if omitted_total > 0 {
            out.push_str(&format!(
                "- _({omitted_total} additional failure detail(s) omitted to stay within the compact verify budget)_\n"
            ));
        }
    }

    out.push_str(
        "\n> Note: Re-verify against the latest base branch before merging. A person reviews and merges; agents never merge.\n",
    );

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded_passing_json() -> &'static str {
        r#"{
  "task": "T-12",
  "title": "Count cancelled tasks apart from failed ones in status --json",
  "commit": "abcdef1234567890",
  "base_commit": "fc95e4c9f6ea5a23b1be6e10ac902d79baf92163",
  "base_branch": "master",
  "ok": true,
  "cells": [
    {
      "profile": "unit",
      "os": "macos",
      "worker": "mac-air",
      "link": "direct",
      "toolchain": "rustc 1.96.0 aarch64-apple-darwin",
      "exit": 0,
      "secs": 31.4,
      "task_id": "11111111-1111-1111-1111-111111111111",
      "failures": []
    },
    {
      "profile": "unit",
      "os": "windows",
      "worker": "msi",
      "link": "direct",
      "toolchain": "rustc 1.96.0 x86_64-pc-windows-gnu",
      "exit": 0,
      "secs": 48.2,
      "task_id": "22222222-2222-2222-2222-222222222222",
      "failures": []
    },
    {
      "profile": "clippy",
      "os": "linux",
      "worker": "hetzner-1",
      "link": "relay",
      "toolchain": "rustc 1.96.0 x86_64-unknown-linux-gnu",
      "exit": 0,
      "secs": 19.0,
      "task_id": "33333333-3333-3333-3333-333333333333",
      "failures": []
    }
  ],
  "omitted": 0
}"#
    }

    fn recorded_failing_json() -> &'static str {
        r#"{
  "task": "T-12",
  "commit": "1234567890abcdef",
  "ok": false,
  "cells": [
    {
      "profile": "unit",
      "os": "windows",
      "worker": "msi",
      "link": "direct",
      "toolchain": "rustc 1.96.0 x86_64-pc-windows-gnu",
      "exit": 101,
      "secs": 48.2,
      "task_id": "44444444-4444-4444-4444-444444444444",
      "failures": [
        {
          "kind": "test",
          "name": "queue::tests::cancel_counts",
          "file": "crates/master/src/queue.rs",
          "line": 812,
          "message": "assertion `left == right` failed: left: 2, right: 1\nsecond line | pipe"
        }
      ]
    }
  ],
  "omitted": 2
}"#
    }

    #[test]
    fn test_pr_body_passing_matrix_table_and_stale_base_warning() {
        let records = parse_verify_records(recorded_passing_json()).expect("parse passing json");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cells.len(), 3);

        let body = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: None,
                branch: Some("team/T-12".to_string()),
                base_branch: Some("master".to_string()),
                base_commit: "fc95e4c9f6ea5a23b1be6e10ac902d79baf92163".to_string(),
                summary: Some("Separated cancelled counts in status --json.".to_string()),
                current_base_commit: Some("ddbbf520deadbeef".to_string()),
            },
            &records,
        )
        .expect("render passing PR body");

        assert!(body.contains(
            "**Task:** `T-12` — Count cancelled tasks apart from failed ones in status --json"
        ));
        assert!(body.contains("**Status:** `PASS` (3 cell(s))"));
        assert!(body.contains(
            "| Profile | Worker | OS | Link | Toolchain | Result | Task ID | Base commit |"
        ));
        assert!(body.contains("| unit | mac-air | macos | direct | `rustc 1.96.0 aarch64-apple-darwin` | PASS (exit 0, 31.4s) | `11111111-1111-1111-1111-111111111111` | `fc95e4c9f6ea` |"));
        assert!(body.contains("| unit | msi | windows | direct | `rustc 1.96.0 x86_64-pc-windows-gnu` | PASS (exit 0, 48.2s) |"));
        assert!(body.contains("| clippy | hetzner-1 | linux | relay |"));
        assert!(body.contains("Stale base warning:"));
        assert!(body.contains("`ddbbf520dead`"));
        assert!(!body.contains("### Failures"));
    }

    #[test]
    fn test_pr_body_failing_record_escapes_markdown_and_surfaces_omitted() {
        let rec = parse_verify_record(recorded_failing_json()).expect("parse failing json");
        let body = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: Some("Header | title\ninjection".to_string()),
                branch: Some("team/T-12".to_string()),
                base_branch: None,
                base_commit: "fc95e4c9".to_string(),
                summary: None,
                current_base_commit: Some("fc95e4c9".to_string()),
            },
            &[rec],
        )
        .expect("render failing PR body");

        assert!(body.contains("**Status:** `FAIL` (1 cell(s))"));
        assert!(body.contains("FAIL (exit 101, 48.2s, 1 failure(s))"));
        assert!(body.contains("Header \\| title injection"));
        assert!(body.contains("queue::tests::cancel_counts"));
        assert!(body.contains("crates/master/src/queue.rs:812"));
        assert!(body.contains("second line \\| pipe"));
        assert!(body.contains("2 additional failure detail(s) omitted"));
        assert!(!body.contains("Stale base warning:"));
    }

    #[test]
    fn test_pr_body_parses_arrays_jsonl_and_rejects_mismatched_or_empty_inputs() {
        // Array input
        let arr_json = format!("[{}, {}]", recorded_passing_json(), recorded_failing_json());
        let arr = parse_verify_records(&arr_json).expect("parse array");
        assert_eq!(arr.len(), 2);

        // JSONL input
        let single_line_pass = recorded_passing_json().replace('\n', " ");
        let single_line_fail = recorded_failing_json().replace('\n', " ");
        let jsonl = format!("{single_line_pass}\n\n{single_line_fail}\n");
        let parsed_jsonl = parse_verify_records(&jsonl).expect("parse jsonl");
        assert_eq!(parsed_jsonl.len(), 2);

        // Invalid JSON rejected
        assert!(parse_verify_records("{bad json").is_err());

        // Mismatched task_id rejected
        let rec = parse_verify_record(recorded_passing_json()).unwrap();
        let mismatch = render_pr_body(
            &PrBodyOptions {
                task_id: "T-99".to_string(),
                task_title: None,
                branch: None,
                base_branch: None,
                base_commit: "fc95e4c9".to_string(),
                summary: None,
                current_base_commit: None,
            },
            &[rec],
        );
        assert!(mismatch.is_err());

        // Empty verify records -> UNVERIFIED (never PASS)
        let unverified = render_pr_body(
            &PrBodyOptions {
                task_id: "T-12".to_string(),
                task_title: None,
                branch: None,
                base_branch: None,
                base_commit: "fc95e4c9".to_string(),
                summary: None,
                current_base_commit: None,
            },
            &[],
        )
        .expect("empty records still render unverified body");
        assert!(unverified.contains("**Status:** `UNVERIFIED` (0 cell(s))"));
        assert!(unverified.contains("_(no verify cells recorded)_"));
    }

    #[test]
    fn test_escape_md_cell_handles_pipes_controls_and_non_finite_secs() {
        assert_eq!(escape_md_cell("a | b\r\nc\t\x07d"), "a \\| b c d");

        let nan_cell = VerifyCellRecord {
            profile: "p|1".to_string(),
            os: "linux".to_string(),
            worker: "w1\nline2".to_string(),
            link: "direct".to_string(),
            toolchain: "rustc`1.96".to_string(),
            exit: 0,
            secs: f64::NAN,
            task_id: "id|1".to_string(),
            failures: Vec::new(),
        };
        assert_eq!(format_cell_result(&nan_cell), "PASS (exit 0, 0.0s)");
        let table = render_verify_table(
            &[VerifyRecord {
                task: "T-1".to_string(),
                title: None,
                commit: "abc".to_string(),
                base_commit: None,
                base_branch: None,
                ok: true,
                cells: vec![nan_cell],
                omitted: 0,
            }],
            Some("deadbeef12345678"),
        );
        assert!(table.contains("| p\\|1 | w1 line2 | linux | direct | `rustc'1.96` | PASS (exit 0, 0.0s) | `id\\|1` | `deadbeef1234` |"));
    }
}
