//! Windows path normalization, reparse-point checks, and MSVC/program discovery.

use super::WINDOWS_SPAWN_LOCK;
use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

const WINDOWS_EXEC_EXTS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];

static MSVC_ENV_CACHE: OnceLock<Option<(PathBuf, String, Vec<PathBuf>)>> = OnceLock::new();

pub(super) fn to_wide(s: &OsStr) -> Vec<u16> {
    s.encode_wide().chain(std::iter::once(0)).collect()
}

fn strip_verbatim_prefix(raw: PathBuf) -> PathBuf {
    let s = raw.to_string_lossy();
    if let Some(stripped) = s.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{stripped}"))
    } else if let Some(stripped) = s.strip_prefix(r"\\?\") {
        PathBuf::from(stripped)
    } else {
        raw
    }
}

fn lexically_resolve_dots(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                // Only pop a normal component; never pop past RootDir or Prefix.
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    out
}

/// Canonicalizes `p` (or its longest existing ancestor prefix when `p` does not exist yet) and
/// lexically resolves `.` and `..` components on any non-existent suffix so `<sandbox>\..\..`
/// cannot bypass `path_starts_with_ci` and 8.3 short names (`RUNNER~1`) are expanded on the
/// host before paths enter an AppContainer.
pub fn normalize_win_path(p: &Path) -> PathBuf {
    if let Ok(canon) = std::fs::canonicalize(p) {
        return strip_verbatim_prefix(canon);
    }
    let lexical = lexically_resolve_dots(p);
    if let Ok(canon) = std::fs::canonicalize(&lexical) {
        return strip_verbatim_prefix(canon);
    }
    let mut tail: Vec<OsString> = Vec::new();
    let mut cur: &Path = &lexical;
    while let Some(parent) = cur.parent() {
        if let Some(name) = cur.file_name() {
            tail.push(name.to_os_string());
        }
        if !parent.as_os_str().is_empty() {
            if let Ok(canon_parent) = std::fs::canonicalize(parent) {
                let mut rebuilt = strip_verbatim_prefix(canon_parent);
                for part in tail.into_iter().rev() {
                    rebuilt.push(part);
                }
                return lexically_resolve_dots(&rebuilt);
            }
        }
        if parent == cur {
            break;
        }
        cur = parent;
    }
    lexical
}

/// Computes a deterministic 8-hex-character worker instance identifier from `sandbox_base` so
/// each worker instance (and each test `Rig`) namespaces its AppContainer profiles under
/// `oxideswarm.task.<worker8>.<run_token>` and never sweeps live profiles of another worker
/// or parallel test (N3).
pub fn worker_profile_prefix(sandbox_base: &Path) -> String {
    let norm = normalize_win_path(sandbox_base);
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for unit in norm.as_os_str().encode_wide() {
        let lower = if (b'A' as u16..=b'Z' as u16).contains(&unit) {
            unit + 32
        } else {
            unit
        };
        hash ^= (lower & 0xff) as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        hash ^= (lower >> 8) as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let folded = ((hash >> 32) as u32) ^ (hash as u32);
    format!("oxideswarm.task.{folded:08x}")
}

/// Returns `true` if `path` itself is a symlink or NTFS junction (`FILE_ATTRIBUTE_REPARSE_POINT`).
/// Uses `symlink_metadata` so it never follows the reparse target.
pub fn is_reparse_point(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|m| (m.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0)
        .unwrap_or(false)
}

/// Returns `true` if `child` is lexically or canonically inside `parent` (case-insensitive on
/// Windows).
pub fn path_starts_with_ci(child: &Path, parent: &Path) -> bool {
    if child.as_os_str().is_empty() || parent.as_os_str().is_empty() {
        return false;
    }
    let c_norm = normalize_win_path(child);
    let p_norm = normalize_win_path(parent);
    let mut c_iter = c_norm.components();
    for p_comp in p_norm.components() {
        let Some(c_comp) = c_iter.next() else {
            return false;
        };
        let p_str = p_comp.as_os_str().to_string_lossy();
        let c_str = c_comp.as_os_str().to_string_lossy();
        if !p_str.eq_ignore_ascii_case(&c_str) {
            return false;
        }
    }
    true
}

/// Queries `vswhere.exe` (when installed) for Visual Studio installation paths with VC tools.
fn query_vswhere_install_paths() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for pf_var in ["ProgramFiles(x86)", "ProgramFiles"] {
        if let Some(pf) = std::env::var_os(pf_var).map(PathBuf::from) {
            candidates.push(
                pf.join("Microsoft Visual Studio")
                    .join("Installer")
                    .join("vswhere.exe"),
            );
        }
    }
    candidates.push(PathBuf::from(
        r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe",
    ));
    for vswhere in candidates {
        if !vswhere.is_file() {
            continue;
        }
        let output_res = {
            let _spawn_guard = WINDOWS_SPAWN_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::process::Command::new(&vswhere)
                .args([
                    "-latest",
                    "-products",
                    "*",
                    "-requires",
                    "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                    "-property",
                    "installationPath",
                ])
                .stdin(std::process::Stdio::null())
                .output()
        };
        let Ok(output) = output_res else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let paths: Vec<PathBuf> = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
            .collect();
        if !paths.is_empty() {
            return paths;
        }
    }
    Vec::new()
}

pub(super) fn discover_msvc_details() -> Option<(PathBuf, String, Vec<PathBuf>)> {
    MSVC_ENV_CACHE
        .get_or_init(|| {
            let mut vs_roots = query_vswhere_install_paths();
            vs_roots.push(PathBuf::from(r"C:\BuildTools"));
            for pf_var in ["ProgramFiles", "ProgramFiles(x86)"] {
                if let Some(pf) = std::env::var_os(pf_var).map(PathBuf::from) {
                    for year in ["2022", "2019"] {
                        for ed in ["Enterprise", "Professional", "Community", "BuildTools"] {
                            let candidate = pf.join("Microsoft Visual Studio").join(year).join(ed);
                            if !vs_roots.contains(&candidate) {
                                vs_roots.push(candidate);
                            }
                        }
                    }
                }
            }
            let mut msvc_ver_dir: Option<PathBuf> = None;
            let mut matched_vs_root: Option<PathBuf> = None;
            for root in vs_roots {
                let msvc_base = root.join("VC").join("Tools").join("MSVC");
                if let Ok(entries) = std::fs::read_dir(&msvc_base) {
                    let mut vers: Vec<PathBuf> = entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.is_dir())
                        .collect();
                    vers.sort();
                    if let Some(best) = vers.into_iter().rev().find(|v| {
                        v.join("bin")
                            .join("Hostx64")
                            .join("x64")
                            .join("link.exe")
                            .is_file()
                    }) {
                        msvc_ver_dir = Some(best);
                        matched_vs_root = Some(root);
                        break;
                    }
                }
            }
            let msvc_ver_dir = msvc_ver_dir?;
            let matched_vs_root = matched_vs_root?;
            let msvc_bin = msvc_ver_dir.join("bin").join("Hostx64").join("x64");
            let msvc_lib = msvc_ver_dir.join("lib").join("x64");

            let kits_root = PathBuf::from(r"C:\Program Files (x86)\Windows Kits\10");
            let kits_base = kits_root.join("Lib");
            let mut sdk_ver_dir: Option<PathBuf> = None;
            if let Ok(entries) = std::fs::read_dir(&kits_base) {
                let mut vers: Vec<PathBuf> = entries
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| p.is_dir())
                    .collect();
                vers.sort();
                sdk_ver_dir = vers.into_iter().rev().find(|v| {
                    v.join("um").join("x64").join("kernel32.lib").is_file()
                        && v.join("ucrt").join("x64").join("ucrt.lib").is_file()
                });
            }
            let sdk_ver_dir = sdk_ver_dir?;
            let ucrt_lib = sdk_ver_dir.join("ucrt").join("x64");
            let um_lib = sdk_ver_dir.join("um").join("x64");
            let lib_str = format!(
                "{};{};{}",
                msvc_lib.display(),
                ucrt_lib.display(),
                um_lib.display()
            );
            let grant_roots = vec![matched_vs_root, kits_root];
            Some((msvc_bin, lib_str, grant_roots))
        })
        .clone()
}

/// Resolves the host MSVC `bin\Hostx64\x64` directory and `LIB` search path once on Windows
/// (querying `vswhere.exe` first, then standard Visual Studio installation roots) so `rustc`
/// inside an AppContainer can locate `link.exe` and Windows SDK `.lib` files via `PATH` and
/// `LIB`.
pub fn resolve_windows_msvc_env() -> Option<(PathBuf, String)> {
    let (bin, lib, _) = discover_msvc_details()?;
    Some((bin, lib))
}

/// Resolves `program` against `working_dir` (for relative paths inside `sandbox_dir`) and
/// `search_path` on Windows. Returns `Ok(None)` if the executable does not exist or fails
/// design 0025 validation, and `Err(...)` if a relative path escapes `sandbox_dir`. Expands
/// 8.3 short names on non-reparse paths so `CreateProcessW` inside an AppContainer never walks
/// denied ancestor directories with `FindFirstFileW`.
pub(in crate::isolation) fn resolve_program(
    program: &str,
    search_path: &str,
    working_dir: &Path,
    sandbox_dir: &Path,
) -> Result<Option<String>, String> {
    let norm_file = |found: &Path| -> String {
        let abs = std::path::absolute(found).unwrap_or_else(|_| found.to_path_buf());
        if !is_reparse_point(&abs) {
            normalize_win_path(&abs).to_string_lossy().into_owned()
        } else {
            abs.to_string_lossy().into_owned()
        }
    };
    let check_with_exts = |base: &Path| -> Option<String> {
        if rusty_grid_core::capabilities::is_valid_windows_executable(base) {
            return Some(norm_file(base));
        }
        for ext in WINDOWS_EXEC_EXTS {
            let mut os = base.as_os_str().to_os_string();
            os.push(ext);
            let candidate = PathBuf::from(os);
            if rusty_grid_core::capabilities::is_valid_windows_executable(&candidate) {
                return Some(norm_file(&candidate));
            }
        }
        None
    };
    let p = Path::new(program);
    let has_sep = program.contains('/') || program.contains('\\') || program.contains(':');
    if has_sep {
        if p.is_absolute() {
            return Ok(check_with_exts(p));
        }
        if program.starts_with("/bin/") || program.starts_with("/usr/bin/") {
            for seg in program.split(['/', '\\']) {
                if seg == ".." {
                    return Err(format!(
                        "program path '{program}' escapes the task sandbox ('..' traversal is not allowed)"
                    ));
                }
            }
            if let Some(found) = check_with_exts(p) {
                return Ok(Some(found));
            }
        } else {
            let (resolved, is_valid) =
                rusty_grid_core::capabilities::resolve_relative_windows_program_in_dir(
                    program,
                    working_dir,
                    Some(sandbox_dir),
                )?;
            return Ok(if is_valid {
                Some(norm_file(&resolved))
            } else {
                None
            });
        }
    }
    let abs_cwd = if working_dir.is_absolute() {
        working_dir.to_path_buf()
    } else {
        std::path::absolute(working_dir).unwrap_or_else(|_| working_dir.to_path_buf())
    };
    let bare = p.file_name().and_then(|n| n.to_str()).unwrap_or(program);
    for dir in std::env::split_paths(search_path) {
        let base = if dir.as_os_str().is_empty() {
            abs_cwd.clone()
        } else if !dir.is_absolute() {
            abs_cwd.join(&dir)
        } else {
            dir
        };
        let direct = base.join(bare);
        if let Some(found) = check_with_exts(&direct) {
            return Ok(Some(found));
        }
    }
    Ok(None)
}
