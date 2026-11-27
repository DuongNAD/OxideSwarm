//! MCP `plan_snapshot` tool implementation (design 0111, TEAM-1).

use std::path::{Component, Path, PathBuf};

use chrono::Utc;
use serde_json::Value;

use super::ToolCallResult;

fn rel_path_within_cwd(cwd: &Path, canon_cwd: &Path, path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return Some(path.to_path_buf());
    }
    if let Ok(rel) = path
        .strip_prefix(cwd)
        .or_else(|_| path.strip_prefix(canon_cwd))
    {
        return Some(rel.to_path_buf());
    }
    for anc in path.ancestors().collect::<Vec<_>>().into_iter().rev() {
        if let Ok(canon_anc) = std::fs::canonicalize(anc) {
            if canon_anc == *canon_cwd {
                return path.strip_prefix(anc).ok().map(Path::to_path_buf);
            }
        }
    }
    None
}

/// Resolves an optional user-supplied relative or in-workspace path inside `cwd`, refusing `..`,
/// symlinks, and escapes outside `cwd`.
pub(crate) fn confine_mcp_path(
    cwd: &Path,
    raw: &str,
    field: &str,
    expect_dir: bool,
) -> Result<PathBuf, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("Property '{field}' cannot be empty"));
    }
    let candidate = Path::new(trimmed);
    if candidate
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(format!(
            "Property '{field}' ('{trimmed}') must not contain '..' or leave the working directory"
        ));
    }

    let canon_cwd = std::fs::canonicalize(cwd)
        .map_err(|e| format!("cannot resolve working directory '{}': {e}", cwd.display()))?;
    let rel_path = rel_path_within_cwd(cwd, &canon_cwd, candidate).ok_or_else(|| {
        format!(
            "Property '{field}' ('{trimmed}') is outside the working directory '{}'",
            cwd.display()
        )
    })?;

    let mut cur = cwd.to_path_buf();
    for comp in rel_path.components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(part) => {
                cur.push(part);
                let meta = std::fs::symlink_metadata(&cur)
                    .map_err(|e| format!("Property '{field}' ('{trimmed}'): {e}"))?;
                if meta.file_type().is_symlink() {
                    return Err(format!(
                        "Property '{field}' ('{trimmed}') is or traverses a symbolic link ('{}'), which is refused",
                        cur.display()
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "Property '{field}' ('{trimmed}') is not a relative path inside the working directory"
                ));
            }
        }
    }

    let meta = std::fs::symlink_metadata(&cur)
        .map_err(|e| format!("Property '{field}' ('{trimmed}'): {e}"))?;
    if meta.file_type().is_symlink() {
        return Err(format!(
            "Property '{field}' ('{trimmed}') is a symbolic link, which is refused"
        ));
    }
    let canon_target = std::fs::canonicalize(&cur)
        .map_err(|e| format!("Property '{field}' ('{trimmed}'): {e}"))?;
    if !canon_target.starts_with(&canon_cwd) {
        return Err(format!(
            "Property '{field}' ('{trimmed}') resolves outside the working directory"
        ));
    }
    if expect_dir && !meta.is_dir() {
        return Err(format!(
            "Property '{field}' ('{trimmed}') must be a directory"
        ));
    }
    if !expect_dir && !meta.is_file() {
        return Err(format!("Property '{field}' ('{trimmed}') must be a file"));
    }
    Ok(cur)
}

pub(super) async fn tool_plan_snapshot(args: &Value) -> ToolCallResult {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            return ToolCallResult::error(format!("cannot determine working directory: {e}"));
        }
    };
    tool_plan_snapshot_in_dir(&cwd, args)
}

pub(crate) fn tool_plan_snapshot_in_dir(cwd: &Path, args: &Value) -> ToolCallResult {
    let repo_dir = match args.get("repo_dir").and_then(Value::as_str) {
        Some(raw) => match confine_mcp_path(cwd, raw, "repo_dir", true) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(e),
        },
        None => cwd.to_path_buf(),
    };
    let plan_path = match args.get("plan_path").and_then(Value::as_str) {
        Some(raw) => match confine_mcp_path(&repo_dir, raw, "plan_path", false) {
            Ok(p) => Some(p),
            Err(e) => return ToolCallResult::error(e),
        },
        None => None,
    };
    let base_override = args.get("base").and_then(Value::as_str);

    match crate::plan::build_snapshot(&repo_dir, plan_path.as_deref(), base_override, Utc::now()) {
        Ok(snapshot) => match serde_json::to_value(&snapshot) {
            Ok(val) => ToolCallResult::ok_json(&val),
            Err(e) => ToolCallResult::error(format!("Failed to serialize plan snapshot: {e}")),
        },
        Err(e) => ToolCallResult::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{call_tool, validate_tool_arguments, McpState, ToolContent};
    use serde_json::json;

    #[test]
    fn test_plan_snapshot_confines_paths_and_refuses_symlinks() {
        let outer = tempfile::tempdir().unwrap();
        let cwd = outer.path().join("repo");
        std::fs::create_dir_all(cwd.join(".oxideswarm")).unwrap();
        std::fs::write(
            cwd.join(".oxideswarm/plan.toml"),
            "[project]\nbase = \"master\"\n\n[[task]]\nid = \"T-1\"\ntitle = \"One\"\nstatus = \"open\"\n",
        )
        .unwrap();

        let outside = outer.path().join("outside.toml");
        std::fs::write(&outside, "[project]\nbase = \"main\"\n").unwrap();

        // Valid relative plan path inside repo_dir
        let ok = tool_plan_snapshot_in_dir(&cwd, &json!({}));
        assert!(!ok.is_error, "{:?}", ok);
        let ToolContent::Text { text } = &ok.content[0];
        let body: Value = serde_json::from_str(text).unwrap();
        assert!(body["line_count"].as_u64().unwrap() <= crate::plan::MAX_SNAPSHOT_LINES as u64);
        assert!(body["text"].as_str().unwrap().contains("T-1"));

        // Refuses '..' and outside absolute paths
        let err_parent = tool_plan_snapshot_in_dir(&cwd, &json!({"plan_path": "../outside.toml"}));
        assert!(err_parent.is_error);
        let err_abs =
            tool_plan_snapshot_in_dir(&cwd, &json!({"repo_dir": outer.path().to_string_lossy()}));
        assert!(err_abs.is_error);
        assert!(validate_tool_arguments("plan_snapshot", &json!({"repo_dir": "../x"})).is_err());

        #[cfg(unix)]
        {
            let link = cwd.join("link.toml");
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            let err_link = tool_plan_snapshot_in_dir(&cwd, &json!({"plan_path": "link.toml"}));
            assert!(err_link.is_error);
        }
    }

    #[tokio::test]
    async fn test_call_tool_plan_snapshot_validation_rejects_bad_types() {
        let state = McpState::new("127.0.0.1:0".to_string(), None);
        let res = call_tool(&state, "plan_snapshot", &json!({"base": 42})).await;
        assert!(res.is_error);
        let ToolContent::Text { text } = &res.content[0];
        assert!(text.contains("must be a string"), "{text}");
    }
}
