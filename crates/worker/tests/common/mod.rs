//! Shared helpers for `rusty_grid_worker` integration tests.

#![allow(dead_code)]

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Process-wide async mutex serialising tests that mutate process environment variables.
pub static ENV_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// RAII guard to safely set and restore multiple environment variables in async tests.
pub struct EnvLock {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    vars: Vec<(&'static str, Option<String>)>,
}

impl EnvLock {
    pub async fn lock() -> Self {
        let guard = ENV_MUTEX.lock().await;
        Self {
            _guard: guard,
            vars: Vec::new(),
        }
    }

    pub fn set(&mut self, key: &'static str, val: &str) {
        let original_val = std::env::var(key).ok();
        std::env::set_var(key, val);
        self.vars.push((key, original_val));
    }
}

impl Drop for EnvLock {
    fn drop(&mut self) {
        for (key, original_val) in self.vars.iter().rev() {
            if let Some(ref orig) = original_val {
                std::env::set_var(key, orig);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}

/// Single-key RAII environment guard backed by [`ENV_MUTEX`].
pub struct ScopedEnvVar {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    key: &'static str,
    original_val: Option<String>,
}

impl ScopedEnvVar {
    pub async fn set<V: AsRef<std::ffi::OsStr>>(key: &'static str, val: V) -> Self {
        let guard = ENV_MUTEX.lock().await;
        let original_val = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self {
            _guard: guard,
            key,
            original_val,
        }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        if let Some(ref orig) = self.original_val {
            std::env::set_var(self.key, orig);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

/// Resolves a runnable Python interpreter (`OXIDESWARM_PYTHON`, `.venv`, `python`, or `python3`).
pub fn get_python_exe() -> PathBuf {
    #[cfg(feature = "3d")]
    if let Some(py) = rusty_grid_worker::gen3d::runner::resolve_python_runtime() {
        return py;
    }
    if let Ok(env_py) = std::env::var("OXIDESWARM_PYTHON") {
        let p = PathBuf::from(env_py.trim());
        if p.exists() {
            return p;
        }
    }
    #[cfg(windows)]
    {
        PathBuf::from("python")
    }
    #[cfg(not(windows))]
    {
        PathBuf::from("python3")
    }
}

/// Resolves the host Blender binary, or returns `None` after printing a skip message.
///
/// When `RUSTY_GRID_REQUIRE_BLENDER=1` (or `true`) is set in the environment, a missing
/// Blender installation panics instead of skipping.
#[cfg(feature = "3d")]
pub fn blender_binary_or_skip(test: &str) -> Option<PathBuf> {
    if let Some(bin) = rusty_grid_worker::autorig::blender_provider::resolve_blender_binary(None) {
        Some(bin)
    } else if std::env::var("RUSTY_GRID_REQUIRE_BLENDER")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        panic!("RUSTY_GRID_REQUIRE_BLENDER=1 but {test} found no Blender executable on this host");
    } else {
        println!("{test}: skipped, no Blender executable found on this host");
        None
    }
}

/// Builds a [`std::process::Command`] placed in its own process group on Unix (`process_group(0)`).
pub fn command_in_own_process_group<S: AsRef<std::ffi::OsStr>>(
    program: S,
) -> std::process::Command {
    #[allow(unused_mut)]
    let mut cmd = std::process::Command::new(program);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd
}

/// Returns `true` if `pid` is present in `sys` and not in `ProcessStatus::Zombie`.
pub fn is_pid_alive(sys: &sysinfo::System, pid: u32) -> bool {
    sys.process(sysinfo::Pid::from(pid as usize))
        .is_some_and(|p| p.status() != sysinfo::ProcessStatus::Zombie)
}

/// Synchronously polls `pids_file` until `expected_count` distinct PIDs have been recorded.
pub fn poll_pids(pids_file: &Path, expected_count: usize, timeout: Duration) -> Vec<u32> {
    let start = Instant::now();
    let mut pids = Vec::new();
    while start.elapsed() < timeout {
        if let Ok(content) = std::fs::read_to_string(pids_file) {
            pids.clear();
            for line in content.lines() {
                if let Some(pid_str) = line.strip_prefix("PID:") {
                    if let Ok(pid) = pid_str.trim().parse::<u32>() {
                        if !pids.contains(&pid) {
                            pids.push(pid);
                        }
                    }
                }
            }
            if pids.len() >= expected_count {
                return pids;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    pids
}

/// Asynchronously polls `pids_file` until `expected_count` distinct PIDs have been recorded.
pub async fn poll_pids_async(
    pids_file: &Path,
    expected_count: usize,
    timeout: Duration,
) -> Vec<u32> {
    let start = Instant::now();
    let mut pids = Vec::new();
    while start.elapsed() < timeout {
        if let Ok(content) = tokio::fs::read_to_string(pids_file).await {
            pids.clear();
            for line in content.lines() {
                if let Some(pid_str) = line.strip_prefix("PID:") {
                    if let Ok(pid) = pid_str.trim().parse::<u32>() {
                        if !pids.contains(&pid) {
                            pids.push(pid);
                        }
                    }
                }
            }
            if pids.len() >= expected_count {
                return pids;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    pids
}

/// Verifies that all given PIDs are actively running (and not zombies) in the OS process table.
pub fn assert_all_alive(sys: &mut sysinfo::System, pids: &[u32]) {
    sys.refresh_processes();
    for &pid in pids {
        assert!(
            is_pid_alive(sys, pid),
            "Expected PID {pid} to be alive, but it was not found (or is a zombie) in system process table"
        );
    }
}

/// Synchronously polls until all given PIDs are absent (or zombie-reaped) from the OS process table.
pub fn assert_all_dead(sys: &mut sysinfo::System, pids: &[u32], timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        sys.refresh_processes();
        let any_alive = pids.iter().any(|&pid| is_pid_alive(sys, pid));
        if !any_alive {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    sys.refresh_processes();
    let survivors: Vec<u32> = pids
        .iter()
        .copied()
        .filter(|&pid| is_pid_alive(sys, pid))
        .collect();

    for &pid in &survivors {
        if let Some(p) = sys.process(sysinfo::Pid::from(pid as usize)) {
            let _ = p.kill();
        }
    }

    assert!(
        survivors.is_empty(),
        "Orphaned descendant processes survived termination: {:?}",
        survivors
    );
}

/// Asynchronously polls until all given PIDs are absent (or zombie-reaped) from the OS process table.
pub async fn assert_all_dead_async(pids: &[u32], timeout: Duration) {
    let mut sys = sysinfo::System::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        sys.refresh_processes();
        let any_alive = pids.iter().any(|&pid| is_pid_alive(&sys, pid));
        if !any_alive {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sys.refresh_processes();
    let survivors: Vec<u32> = pids
        .iter()
        .copied()
        .filter(|&pid| is_pid_alive(&sys, pid))
        .collect();

    for &pid in &survivors {
        if let Some(p) = sys.process(sysinfo::Pid::from(pid as usize)) {
            let _ = p.kill();
        }
    }

    assert!(
        survivors.is_empty(),
        "Orphaned descendant processes survived termination: {:?}",
        survivors
    );
}

/// Creates a portable fake Blender runner script in `dir` that executes `python_logic` via [`get_python_exe`].
pub fn create_fake_blender_executable(dir: &Path, python_logic: &str) -> PathBuf {
    let logic_path = dir.join("fake_blender_logic.py");
    let mut f = File::create(&logic_path).expect("Failed to create logic file");
    f.write_all(python_logic.as_bytes())
        .expect("Failed to write logic file");
    drop(f);

    let py_exe = get_python_exe();

    #[cfg(windows)]
    {
        let cmd_path = dir.join("fake_blender.cmd");
        let content = format!(
            "@echo off\r\n\"{}\" \"{}\" %*\r\n",
            py_exe.display(),
            logic_path.display()
        );
        let mut f = File::create(&cmd_path).expect("Failed to create .cmd file");
        f.write_all(content.as_bytes())
            .expect("Failed to write .cmd file");
        cmd_path
    }

    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        let sh_path = dir.join("fake_blender.sh");
        let content = format!(
            "#!/bin/sh\nexec \"{}\" \"{}\" \"$@\"\n",
            py_exe.display(),
            logic_path.display()
        );
        let mut f = File::create(&sh_path).expect("Failed to create .sh file");
        f.write_all(content.as_bytes())
            .expect("Failed to write .sh file");
        let _ = std::fs::set_permissions(&sh_path, std::fs::Permissions::from_mode(0o755));
        sh_path
    }
}
