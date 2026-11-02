//! Subprocess 3D engine runner utilities and Python environment discovery (Feature F5).

use std::path::{Path, PathBuf};

/// Converts a path to an absolute, normalized path without Windows verbatim prefix (`\\?\`).
///
/// Uses `std::path::absolute` instead of `std::fs::canonicalize` to avoid resolving
/// symbolic links in virtual environments (`<venv>/bin/python`), preserving `sys.prefix`
/// and virtualenv package discovery.
pub fn to_clean_absolute_path(path: &Path) -> Option<PathBuf> {
    if !path.is_file() {
        return None;
    }
    let abs = if let Ok(abs) = std::path::absolute(path) {
        abs
    } else if path.is_absolute() {
        path.to_path_buf()
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(path)
    } else {
        path.to_path_buf()
    };

    #[cfg(windows)]
    {
        let s = abs.to_string_lossy();
        if let Some(stripped) = s.strip_prefix(r"\\?\UNC\") {
            return Some(PathBuf::from(format!(r"\\{stripped}")));
        } else if let Some(stripped) = s.strip_prefix(r"\\?\") {
            return Some(PathBuf::from(stripped));
        }
    }
    Some(abs)
}

/// Probes whether an executable name exists on PATH, responds to `--version`,
/// and returns its clean absolute path.
fn probe_executable_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var("PATH").ok()?;
    for dir in std::env::split_paths(&path_var) {
        #[cfg(windows)]
        let exts: &[&str] = &[".exe", ".cmd", ".bat", ""];
        #[cfg(not(windows))]
        let exts: &[&str] = &[""];

        for ext in exts {
            let candidate = dir.join(format!("{name}{ext}"));
            if candidate.is_file() {
                let mut cmd = std::process::Command::new(&candidate);
                cmd.arg("--version");
                for var in crate::runner::SECRET_ENV_VARS {
                    cmd.env_remove(var);
                }
                if let Ok(output) = cmd.output() {
                    if output.status.success() {
                        return to_clean_absolute_path(&candidate);
                    }
                }
            }
        }
    }
    None
}

/// Resolves the optimal Python executable for executing local 3D models as an absolute path.
pub fn resolve_python_runtime() -> Option<PathBuf> {
    // 1. Explicit environment variable override
    if let Ok(custom) =
        std::env::var("OXIDESWARM_PYTHON").or_else(|_| std::env::var("RUSTY_GRID_PYTHON"))
    {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            let p = PathBuf::from(trimmed);
            if let Some(abs) = to_clean_absolute_path(&p) {
                return Some(abs);
            }
            if let Some(abs) = probe_executable_in_path(trimmed) {
                return Some(abs);
            }
        }
    }

    // 2. Local workspace virtual environment (.venv)
    #[cfg(windows)]
    let venv_candidates = [
        PathBuf::from(r".venv\Scripts\python.exe"),
        PathBuf::from(r"..\.venv\Scripts\python.exe"),
        PathBuf::from(r"..\..\.venv\Scripts\python.exe"),
    ];

    #[cfg(not(windows))]
    let venv_candidates = [
        PathBuf::from(".venv/bin/python"),
        PathBuf::from("../.venv/bin/python"),
        PathBuf::from("../../.venv/bin/python"),
        PathBuf::from("/usr/bin/python3"),
    ];

    for candidate in &venv_candidates {
        if let Some(abs) = to_clean_absolute_path(candidate) {
            return Some(abs);
        }
    }

    // 3. Fallback: check if standard python executable is available on PATH
    for name in &["python", "python3"] {
        if let Some(abs) = probe_executable_in_path(name) {
            return Some(abs);
        }
    }

    None
}

/// Resolves the file path to the 3D generation entrypoint script as an absolute path.
pub fn resolve_entrypoint_script() -> Option<PathBuf> {
    if let Ok(custom) = std::env::var("OXIDESWARM_GEN3D_SCRIPT") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            let p = PathBuf::from(trimmed);
            if let Some(abs) = to_clean_absolute_path(&p) {
                return Some(abs);
            }
        }
    }

    let candidates = [
        PathBuf::from("engines/generative_3d/entrypoint.py"),
        PathBuf::from("../engines/generative_3d/entrypoint.py"),
        PathBuf::from("../../engines/generative_3d/entrypoint.py"),
    ];

    for c in &candidates {
        if let Some(abs) = to_clean_absolute_path(c) {
            return Some(abs);
        }
    }

    None
}

/// Parses progress markers in stdout of format `[PROGRESS: 0.85] Running Marching Cubes...`
/// or `[PROGRESS: 85%] Running Marching Cubes...` or `[PROGRESS: 85]`
pub fn parse_progress_marker(line: &str) -> Option<(f32, String)> {
    let trimmed = line.trim();
    if trimmed.starts_with("[PROGRESS:") {
        if let Some(end_bracket) = trimmed.find(']') {
            let num_str = trimmed[10..end_bracket].trim();
            let clean_str = num_str.trim_end_matches('%').trim();
            if let Ok(mut val) = clean_str.parse::<f32>() {
                if val > 1.0 {
                    val /= 100.0;
                }
                let val = val.clamp(0.0, 1.0);
                let msg = trimmed[end_bracket + 1..].trim().to_string();
                return Some((val, msg));
            }
        }
    }
    None
}

/// Parses engine or device label markers in stdout, e.g.:
/// `[ENGINE: TripoSR-CUDA]` or `[DEVICE: TripoSR-CPU]` or `[ENGINE] MyModel`
/// or `[TripoSR] Initializing TripoSR-PyTorch-v1.0...`
pub fn parse_engine_marker(line: &str) -> Option<String> {
    let trimmed = line.trim();
    for prefix in &["[ENGINE:", "[DEVICE:"] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            if let Some(end_bracket) = rest.find(']') {
                let label = rest[..end_bracket].trim();
                if !label.is_empty() {
                    return Some(label.to_string());
                }
            }
        }
    }
    for prefix in &["[ENGINE]", "[DEVICE]"] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let label = rest.trim();
            if !label.is_empty() {
                return Some(label.to_string());
            }
        }
    }
    if let Some(rest) = trimmed.strip_prefix("[TripoSR] Initializing ") {
        let label = rest.trim_end_matches('.').trim();
        if !label.is_empty() {
            return Some(label.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_progress_marker_formats() {
        assert_eq!(
            parse_progress_marker("[PROGRESS: 0.45] Computing SDF..."),
            Some((0.45, "Computing SDF...".to_string()))
        );
        assert_eq!(
            parse_progress_marker("[PROGRESS: 45%] Marching Cubes..."),
            Some((0.45, "Marching Cubes...".to_string()))
        );
        assert_eq!(
            parse_progress_marker("[PROGRESS: 100%] Exporting GLB..."),
            Some((1.0, "Exporting GLB...".to_string()))
        );
        assert_eq!(
            parse_progress_marker("[PROGRESS: 0%] Initializing..."),
            Some((0.0, "Initializing...".to_string()))
        );
        assert_eq!(
            parse_progress_marker("[PROGRESS: 85] Texturing..."),
            Some((0.85, "Texturing...".to_string()))
        );
        assert_eq!(parse_progress_marker("Random output without marker"), None);
    }

    #[test]
    fn test_parse_engine_marker_formats() {
        assert_eq!(
            parse_engine_marker("[ENGINE: Custom-Inference-Model]"),
            Some("Custom-Inference-Model".to_string())
        );
        assert_eq!(
            parse_engine_marker("[DEVICE: TripoSR-CUDA] ready"),
            Some("TripoSR-CUDA".to_string())
        );
        assert_eq!(
            parse_engine_marker("[DEVICE: Custom-Mock-Engine-v2]"),
            Some("Custom-Mock-Engine-v2".to_string())
        );
        assert_eq!(
            parse_engine_marker("[TripoSR] Initializing TripoSR-PyTorch-v1.0..."),
            Some("TripoSR-PyTorch-v1.0".to_string())
        );
        assert_eq!(parse_engine_marker("No engine marker here"), None);
    }
}
