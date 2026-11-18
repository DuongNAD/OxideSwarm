//! `ox-mode doc` workflow engine and interactive menu, split out of `mode_cli.rs`
//! (design 0080, MAINT-2).

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use chrono::Utc;

use rusty_grid_core::mode::{ModeExecutionResult, WorkflowMode};
use rusty_grid_core::protocol::{
    ProtocolCapabilities, MAX_FRAME_SIZE, PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE,
};

use super::{execute_single_action, render_execution_result, CLR_BOLD, CLR_RESET};

/// Resolves the Cargo target directory for `workspace_root`, honouring `CARGO_TARGET_DIR` when set.
pub fn resolve_cargo_target_dir(workspace_root: &Path) -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(val) if !val.is_empty() => {
            let p = PathBuf::from(val);
            if p.is_absolute() {
                p
            } else {
                workspace_root.join(p)
            }
        }
        _ => workspace_root.join("target"),
    }
}

/// Verifies that the generated rustdoc HTML index exists inside `target_dir` before reporting `doc build` success.
pub fn finalize_doc_build_output_with_target_dir(target_dir: &Path, res: &mut ModeExecutionResult) {
    if res.success {
        let doc_index = target_dir.join("doc/rusty_grid_core/index.html");
        if doc_index.is_file() {
            res.stdout.push_str(&format!(
                "\nDocumentation built successfully!\nHTML Index: {}\n",
                doc_index.display()
            ));
        } else {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!(
                "cargo doc exited 0, but expected HTML index does not exist: {}",
                doc_index.display()
            );
        }
    }
}

/// Verifies that the generated rustdoc HTML index exists on disk before reporting `doc build` success.
pub fn finalize_doc_build_output(workspace_root: &Path, res: &mut ModeExecutionResult) {
    let target_dir = resolve_cargo_target_dir(workspace_root);
    finalize_doc_build_output_with_target_dir(&target_dir, res);
}

/// Builds rustdoc for all workspace crates.
pub fn run_doc_build(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Doc, "build");

    let cmd_res = Command::new("cargo")
        .args(["doc", "--workspace", "--no-deps"])
        .current_dir(workspace_root)
        .output();

    res.duration_ms = start.elapsed().as_millis() as u64;

    match cmd_res {
        Ok(output) => {
            res.exit_code = output.status.code();
            res.success = output.status.success();
            res.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            res.stderr = String::from_utf8_lossy(&output.stderr).to_string();
            finalize_doc_build_output(workspace_root, &mut res);
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to spawn cargo doc: {}", e);
        }
    }
    res
}

/// Strips inline backtick code spans (`` `...` ``) from a single markdown line so links inside
/// inline code examples are not treated as document links.
fn strip_inline_code_spans(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_tick = false;
    for ch in line.chars() {
        if ch == '`' {
            in_tick = !in_tick;
        } else if !in_tick {
            out.push(ch);
        }
    }
    out
}

/// Extracts markdown link targets (`[label](target)`) from a line whose inline code spans were removed.
fn extract_markdown_link_targets(line: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            if let Some(close_bracket_rel) = line[i + 1..].find("](") {
                let paren_start = i + 1 + close_bracket_rel + 2;
                if let Some(close_paren_rel) = line[paren_start..].find(')') {
                    let target = line[paren_start..paren_start + close_paren_rel]
                        .trim()
                        .to_string();
                    if !target.is_empty() {
                        targets.push(target);
                    }
                    i = paren_start + close_paren_rel + 1;
                    continue;
                }
            }
        }
        i += 1;
    }
    targets
}

/// Verifies markdown documentation files for syntax, headings, balanced code fences, and relative links.
pub fn verify_documentation(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Doc, "verify");

    let target_docs = [
        "README.md",
        "CLAUDE.md",
        "PROJECT.md",
        "DEPLOYMENT.md",
        "ANDROID_ARCHITECTURE.md",
    ];

    let mut doc_details = Vec::new();
    let mut all_exist = true;
    let mut total_lines = 0usize;
    let mut total_bytes = 0usize;
    let mut total_links_checked = 0usize;
    let mut total_broken_links = 0usize;
    let mut issues = Vec::new();

    for doc_name in &target_docs {
        let doc_path = workspace_root.join(doc_name);
        if !doc_path.exists() {
            all_exist = false;
            issues.push(format!("Missing documentation file: {}", doc_name));
            doc_details.push(serde_json::json!({
                "file": doc_name,
                "exists": false,
                "lines": 0,
                "bytes": 0,
                "headings_count": 0,
                "links_checked": 0,
                "broken_links": 0
            }));
            continue;
        }

        match fs::read_to_string(&doc_path) {
            Ok(content) => {
                let lines_count = content.lines().count();
                let bytes_count = content.len();
                let mut in_fence = false;
                let mut headings_count = 0usize;
                let mut doc_links_checked = 0usize;
                let mut doc_broken_links = 0usize;
                let doc_parent = doc_path.parent().unwrap_or(workspace_root);

                for (line_idx, l) in content.lines().enumerate() {
                    let trimmed = l.trim_start();
                    if trimmed.starts_with("```") {
                        in_fence = !in_fence;
                        continue;
                    }
                    if in_fence {
                        continue;
                    }
                    if trimmed.starts_with('#') {
                        headings_count += 1;
                    }

                    let cleaned = strip_inline_code_spans(l);
                    for target in extract_markdown_link_targets(&cleaned) {
                        if target.starts_with('#')
                            || target.contains("://")
                            || target.starts_with("mailto:")
                        {
                            continue;
                        }
                        let rel_path = target.split('#').next().unwrap_or("").trim();
                        if rel_path.is_empty() {
                            continue;
                        }
                        doc_links_checked += 1;
                        if !doc_parent.join(rel_path).exists() {
                            doc_broken_links += 1;
                            issues.push(format!(
                                "{}:{}: Broken relative link '{}'",
                                doc_name,
                                line_idx + 1,
                                target
                            ));
                        }
                    }
                }

                total_lines += lines_count;
                total_bytes += bytes_count;
                total_links_checked += doc_links_checked;
                total_broken_links += doc_broken_links;

                if headings_count == 0 {
                    issues.push(format!("{}: No markdown headings found", doc_name));
                }

                let fence_count = content
                    .lines()
                    .filter(|l| l.trim_start().starts_with("```"))
                    .count();
                if fence_count % 2 != 0 {
                    issues.push(format!(
                        "{}: Odd number of code fence delimiters (```) detected (unclosed fence)",
                        doc_name
                    ));
                }

                doc_details.push(serde_json::json!({
                    "file": doc_name,
                    "exists": true,
                    "lines": lines_count,
                    "bytes": bytes_count,
                    "headings_count": headings_count,
                    "fences_count": fence_count,
                    "links_checked": doc_links_checked,
                    "broken_links": doc_broken_links
                }));
            }
            Err(e) => {
                all_exist = false;
                issues.push(format!("Could not read {}: {}", doc_name, e));
            }
        }
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = all_exist && issues.is_empty();
    res.exit_code = Some(if res.success { 0 } else { 1 });

    let mut stdout = format!(
        "=== Documentation & Specification Verification ===\nFiles Checked: {}\nTotal Size: {:.1} KB across {} lines\nRelative Links Checked: {} ({} broken)\nStatus: {}\n\nVerified Documents:\n",
        target_docs.len(),
        total_bytes as f64 / 1024.0,
        total_lines,
        total_links_checked,
        total_broken_links,
        if res.success {
            "[PASSED]"
        } else {
            "[ISSUES DETECTED]"
        }
    );

    for d in &doc_details {
        stdout.push_str(&format!(
            "  ● {:<24} -> {} lines, {} headings, {} links checked ({} broken), {} bytes\n",
            d["file"].as_str().unwrap_or(""),
            d["lines"],
            d["headings_count"],
            d["links_checked"],
            d["broken_links"],
            d["bytes"]
        ));
    }

    if !issues.is_empty() {
        stdout.push_str("\nIdentified Issues:\n");
        for iss in &issues {
            stdout.push_str(&format!("  [!] {}\n", iss));
        }
        res.stderr = issues.join("\n");
    }

    res.stdout = stdout;
    res = res.with_metric("total_docs", target_docs.len());
    res = res.with_metric("total_lines", total_lines);
    res = res.with_metric("total_bytes", total_bytes);
    res = res.with_metric("links_checked", total_links_checked);
    res = res.with_metric("broken_links", total_broken_links);
    res = res.with_metric("issues_count", issues.len());
    res = res.with_metric("details", doc_details);

    res
}

/// Generates a factual workspace and wire-protocol specification summary from live code constants
/// and workspace files on disk.
pub fn export_specification(workspace_root: &Path) -> ModeExecutionResult {
    let start = Instant::now();
    let mut res = ModeExecutionResult::new(WorkflowMode::Doc, "export");

    let now_str = Utc::now().to_rfc3339();
    let max_frame_mib = MAX_FRAME_SIZE / (1024 * 1024);
    let supported_caps_bits = ProtocolCapabilities::all_supported().bits();

    // Inspect workspace crates from Cargo.toml
    let mut crate_rows = Vec::new();
    let root_toml_path = workspace_root.join("Cargo.toml");
    if let Ok(root_toml_str) = fs::read_to_string(&root_toml_path) {
        if let Ok(root_toml) = root_toml_str.parse::<toml::Table>() {
            if let Some(members) = root_toml
                .get("workspace")
                .and_then(|w| w.get("members"))
                .and_then(|m| m.as_array())
            {
                for member in members {
                    if let Some(member_rel) = member.as_str() {
                        let member_toml_path = workspace_root.join(member_rel).join("Cargo.toml");
                        let pkg_name = fs::read_to_string(&member_toml_path)
                            .ok()
                            .and_then(|s| s.parse::<toml::Table>().ok())
                            .and_then(|t| {
                                t.get("package")
                                    .and_then(|p| p.get("name"))
                                    .and_then(|n| n.as_str())
                                    .map(str::to_string)
                            })
                            .unwrap_or_else(|| member_rel.to_string());
                        crate_rows.push(format!("- `{member_rel}` (`{pkg_name}`)"));
                    }
                }
            }
        }
    }

    // Inspect design records in docs/design/
    let mut design_rows = Vec::new();
    let design_dir = workspace_root.join("docs/design");
    if let Ok(entries) = fs::read_dir(&design_dir) {
        let mut paths: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|ext| ext.to_str()) == Some("md"))
            .collect();
        paths.sort();
        for p in paths {
            let file_name = p
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            let first_heading = fs::read_to_string(&p)
                .ok()
                .and_then(|c| {
                    c.lines()
                        .find(|l| l.starts_with("# "))
                        .map(|l| l.trim_start_matches("# ").trim().to_string())
                })
                .unwrap_or_else(|| file_name.clone());
            design_rows.push(format!("- `docs/design/{file_name}`: {first_heading}"));
        }
    }

    let crates_section = if crate_rows.is_empty() {
        "- (No workspace crates discovered)".to_string()
    } else {
        crate_rows.join("\n")
    };
    let designs_section = if design_rows.is_empty() {
        "- (No design records found in `docs/design/`)".to_string()
    } else {
        design_rows.join("\n")
    };

    let spec_markdown = format!(
        "# OxideSwarm Generated Workspace & Wire Specification Summary\n\
Generated At: {now_str}\n\
Workspace Root: {}\n\n\
## 1. Wire Protocol Constants (`rusty_grid_core::protocol`)\n\
- **Current Protocol Version (`PROTOCOL_VERSION_CURRENT`)**: `{PROTOCOL_VERSION_CURRENT}`\n\
- **Minimum Compatible Version (`PROTOCOL_VERSION_MIN_COMPATIBLE`)**: `{PROTOCOL_VERSION_MIN_COMPATIBLE}`\n\
- **Maximum Frame Size (`MAX_FRAME_SIZE`)**: `{MAX_FRAME_SIZE}` bytes (`{max_frame_mib} MiB`)\n\
- **Length Prefix**: 4-byte big-endian unsigned integer (`u32`), followed by a 1-byte wire-format discriminator (`0x02` Bincode default, `0x01` tagged JSON, `0x03` cluster-auth handshake, `0x04` ChaCha20-Poly1305 AEAD encrypted frame)\n\
- **Supported Capability Bits (`ProtocolCapabilities::all_supported()`)**: `0x{supported_caps_bits:04x}`\n\n\
## 2. Workspace Crates (`Cargo.toml`)\n\
{crates_section}\n\n\
## 3. Recorded Design Documents (`docs/design/`)\n\
{designs_section}\n",
        workspace_root.display()
    );

    let target_dir = workspace_root.join("target/docs");
    let target_file = target_dir.join("OXIDESWARM_SPECIFICATION.md");

    let write_ok =
        fs::create_dir_all(&target_dir).and_then(|_| fs::write(&target_file, &spec_markdown));

    res.duration_ms = start.elapsed().as_millis() as u64;

    match write_ok {
        Ok(_) => {
            res.success = true;
            res.exit_code = Some(0);
            res.stdout = format!(
                "Workspace Specification Summary generated!\nTarget File: {}\nSpecification Size: {} bytes\nGenerated At: {}\n",
                target_file.display(),
                spec_markdown.len(),
                now_str
            );
            res = res.with_metric("target_path", target_file.to_string_lossy().to_string());
            res = res.with_metric("spec_bytes", spec_markdown.len());
            res = res.with_metric("max_frame_size_bytes", MAX_FRAME_SIZE);
            res = res.with_metric("protocol_version_current", PROTOCOL_VERSION_CURRENT);
            res = res.with_metric("design_records_count", design_rows.len());
            res = res.with_metric("updated_at", now_str);
        }
        Err(e) => {
            res.success = false;
            res.exit_code = Some(1);
            res.stderr = format!("Failed to write specification: {}", e);
        }
    }

    res
}

pub(super) async fn run_doc_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 📚 DOCUMENTATION & ARCHITECTURE SPECIFICATION MODE ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Verify Markdown Documentation & Relative Links");
    println!("  [2] Export Workspace & Wire Specification Summary");
    println!("  [3] Build Source Documentation (cargo doc --workspace --no-deps)");
    println!("  [0] Back to Main Menu");
    print!("Select [0-3]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res = execute_single_action(WorkflowMode::Doc, "verify", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res = execute_single_action(WorkflowMode::Doc, "export", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res = execute_single_action(WorkflowMode::Doc, "build", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}
