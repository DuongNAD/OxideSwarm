//! Subprocess 3D engine runner utilities and Python environment discovery (Feature F5).

use std::path::PathBuf;

/// Resolves the optimal Python executable for executing local 3D models.
pub fn resolve_python_runtime() -> Option<PathBuf> {
    // 1. Explicit environment variable override
    if let Ok(custom) =
        std::env::var("OXIDESWARM_PYTHON").or_else(|_| std::env::var("RUSTY_GRID_PYTHON"))
    {
        let p = PathBuf::from(custom);
        if p.is_file() {
            return Some(p);
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
        if candidate.is_file() {
            return Some(candidate.clone());
        }
    }

    // 3. Fallback: check if standard python executable is available on PATH
    if let Ok(output) = std::process::Command::new("python")
        .arg("--version")
        .output()
    {
        if output.status.success() {
            return Some(PathBuf::from("python"));
        }
    }
    if let Ok(output) = std::process::Command::new("python3")
        .arg("--version")
        .output()
    {
        if output.status.success() {
            return Some(PathBuf::from("python3"));
        }
    }

    None
}

/// Resolves the file path to the 3D generation entrypoint script.
pub fn resolve_entrypoint_script() -> Option<PathBuf> {
    if let Ok(custom) = std::env::var("OXIDESWARM_GEN3D_SCRIPT") {
        let p = PathBuf::from(custom);
        if p.is_file() {
            return Some(p);
        }
    }

    let candidates = [
        PathBuf::from("engines/generative_3d/entrypoint.py"),
        PathBuf::from("../engines/generative_3d/entrypoint.py"),
        PathBuf::from("../../engines/generative_3d/entrypoint.py"),
    ];

    for c in &candidates {
        if c.is_file() {
            return Some(c.clone());
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
}
