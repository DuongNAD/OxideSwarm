//! Helpers of `launch_local_dev_cluster_with_ports`: the master's log file (TEST-43), the cleanup of
//! a failed launch, and (on Windows) the stdio inheritance fix for detached children.

use std::fs;
#[cfg(windows)]
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

/// How many lines of the master's output a failed launch quotes.
const MASTER_LOG_TAIL_LINES: usize = 40;

/// The file that receives the master's stdout and stderr (TEST-43). It is overwritten by every
/// launch, so a launch that fails can say what the master did, and a running cluster keeps its log
/// there. The name matches `.dev_cluster.*` in `.gitignore`.
pub(super) fn master_log_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(".dev_cluster.master.log")
}

/// Stdout and stderr for the master: both into `log`, or nowhere when the file cannot be created.
pub(super) fn master_log_stdio(log: &Path) -> (Stdio, Stdio) {
    match fs::File::create(log).and_then(|out| Ok((out.try_clone()?, out))) {
        Ok((out, err)) => (Stdio::from(out), Stdio::from(err)),
        Err(_) => (Stdio::null(), Stdio::null()),
    }
}

/// `reason`, then the last lines of the master's output, so a failed wait shows whether the master
/// started late, could not bind the dashboard, or printed nothing at all.
pub(super) fn with_master_log_tail(reason: String, log: &Path) -> String {
    match fs::read_to_string(log) {
        Ok(text) if !text.trim().is_empty() => {
            let lines: Vec<&str> = text.lines().collect();
            let tail = &lines[lines.len().saturating_sub(MASTER_LOG_TAIL_LINES)..];
            format!(
                "{reason}\n--- last {} lines of the master's output ({}) ---\n{}",
                tail.len(),
                log.display(),
                tail.join("\n")
            )
        }
        Ok(_) => format!("{reason}\n(the master wrote nothing to {})", log.display()),
        Err(e) => format!("{reason}\n(could not read {}: {e})", log.display()),
    }
}

pub(super) fn cleanup_failed_cluster_launch(
    children: &mut [&mut std::process::Child],
    pid_file: &Path,
    json_file: &Path,
    master_port_file: &Path,
    dash_port_file: &Path,
    isolated_home: Option<&Path>,
) {
    for child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = fs::remove_file(pid_file);
    let _ = fs::remove_file(json_file);
    let _ = fs::remove_file(master_port_file);
    let _ = fs::remove_file(dash_port_file);
    if let Some(home) = isolated_home {
        let _ = fs::remove_dir_all(home);
    }
}

#[cfg(windows)]
pub(super) fn clear_current_process_stdio_inherit_flags() {
    use std::os::windows::io::AsRawHandle;
    const HANDLE_FLAG_INHERIT: u32 = 0x0000_0001;
    extern "system" {
        fn SetHandleInformation(
            h_object: *mut std::ffi::c_void,
            dw_mask: u32,
            dw_flags: u32,
        ) -> i32;
    }
    for h in [
        io::stdin().as_raw_handle(),
        io::stdout().as_raw_handle(),
        io::stderr().as_raw_handle(),
    ] {
        if !h.is_null() {
            // SAFETY: `h` is a valid OS handle from `std::io::{stdin,stdout,stderr}`. Clearing
            // `HANDLE_FLAG_INHERIT` stops `CreateProcessW(..., bInheritHandles=TRUE, ...)` from
            // leaking `ox-mode`'s piped stdout/stderr handles into detached `rusty-grid` children.
            unsafe {
                let _ = SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}
