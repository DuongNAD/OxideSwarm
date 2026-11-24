//! Process-wide macOS spawn serialization (`BUG-35`).
//!
//! On macOS (`#[cfg(target_os = "macos")]`), there is no `pipe2(O_CLOEXEC)` syscall: Rust's
//! standard library creates both its internal exec-status `output_pipe` and any `Stdio::piped()`
//! descriptors with `pipe()` followed by `fcntl(F_SETFD, FD_CLOEXEC)`. If a second thread runs
//! `fork()` or `posix_spawn()` inside that non-atomic window, the unrelated child inherits the
//! write end of those descriptors without `FD_CLOEXEC`, blocking `Command::spawn()` (waiting on
//! `output_pipe`) or `Child::wait_with_output()` (waiting for `stdout`/`stderr` EOF) until the
//! unrelated child exits.
//!
//! Linux (`pipe2(O_CLOEXEC)` atomic in the kernel) and Windows (explicit handle inheritance and
//! `WINDOWS_SPAWN_LOCK`) do not share this macOS `pipe()` + `FD_CLOEXEC` window, so all functions
//! in this module bypass the lock there and keep today's direct path.
//!
//! The mutex is held **only** across descriptor setup and `Command::spawn()` (never across
//! `wait()`, `wait_with_output()`, or output reading) so short spawns never block on child run
//! duration once `exec`/`posix_spawn` returns cleanly.

use std::io;
#[cfg(target_os = "macos")]
use std::process::Stdio;
use std::process::{Child, Command, ExitStatus, Output};

#[cfg(target_os = "macos")]
static MACOS_SPAWN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Runs `f` while holding the process-wide macOS spawn serialization lock on macOS.
///
/// On non-macOS targets (Linux uses atomic `pipe2(O_CLOEXEC)`; Windows uses handle inheritance
/// rules outside this POSIX descriptor race), this executes `f()` directly with no lock.
pub fn with_macos_spawn_lock<T>(f: impl FnOnce() -> T) -> T {
    #[cfg(target_os = "macos")]
    let _guard = MACOS_SPAWN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f()
}

/// Spawns `cmd` (`std::process::Command`) under the process-wide macOS spawn lock on macOS.
///
/// Linux and Windows keep today's direct `cmd.spawn()` path without locking.
pub fn spawn_std(cmd: &mut Command) -> io::Result<Child> {
    with_macos_spawn_lock(|| cmd.spawn())
}

/// Captures stdout and stderr of `cmd` (`std::process::Command`), holding the macOS spawn lock
/// strictly around `spawn()` and dropping it before waiting for the child or reading its pipes.
///
/// On macOS, this configures `stdin(Stdio::null())`, `stdout(Stdio::piped())`, and
/// `stderr(Stdio::piped())` to match [`Command::output()`]'s default behavior (since
/// `Command::spawn()` otherwise defaults stdin to inherit). Callers needing a custom stdin should
/// configure it on `cmd` and call [`spawn_std`] followed by `child.wait_with_output()`.
///
/// Linux and Windows keep today's direct `cmd.output()` path without locking.
pub fn output_std(cmd: &mut Command) -> io::Result<Output> {
    #[cfg(target_os = "macos")]
    {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = spawn_std(cmd)?;
        child.wait_with_output()
    }
    #[cfg(not(target_os = "macos"))]
    {
        cmd.output()
    }
}

/// Runs `cmd` (`std::process::Command`) to completion and returns its exit status, holding the
/// macOS spawn lock strictly around `spawn()` and dropping it before waiting.
///
/// Linux and Windows keep today's direct `cmd.status()` path without locking.
pub fn status_std(cmd: &mut Command) -> io::Result<ExitStatus> {
    #[cfg(target_os = "macos")]
    {
        let mut child = spawn_std(cmd)?;
        child.wait()
    }
    #[cfg(not(target_os = "macos"))]
    {
        cmd.status()
    }
}

/// Spawns `cmd` (`tokio::process::Command`) under the process-wide macOS spawn lock on macOS.
///
/// Linux and Windows keep today's direct `cmd.spawn()` path without locking.
pub fn spawn_tokio(cmd: &mut tokio::process::Command) -> io::Result<tokio::process::Child> {
    with_macos_spawn_lock(|| cmd.spawn())
}

/// Captures stdout and stderr of `cmd` (`tokio::process::Command`), holding the macOS spawn lock
/// strictly around `spawn()` and dropping it before awaiting child completion or reading pipes.
///
/// On macOS, this configures `stdin(Stdio::null())`, `stdout(Stdio::piped())`, and
/// `stderr(Stdio::piped())` to match `tokio::process::Command::output()`'s default behavior
/// (since `Command::spawn()` otherwise defaults stdin to inherit). Callers needing a custom stdin
/// should configure it on `cmd` and call [`spawn_tokio`] followed by `child.wait_with_output().await`.
///
/// Linux and Windows keep today's direct `cmd.output().await` path without locking.
pub async fn output_tokio(cmd: &mut tokio::process::Command) -> io::Result<Output> {
    #[cfg(target_os = "macos")]
    {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = spawn_tokio(cmd)?;
        child.wait_with_output().await
    }
    #[cfg(not(target_os = "macos"))]
    {
        cmd.output().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(miri, ignore = "Miri: spawns a process")]
    fn spawn_and_output_preserve_enoent_and_output() {
        let mut missing_cmd = Command::new("/nonexistent-oxideswarm-bug35-probe");
        let err = spawn_std(&mut missing_cmd).expect_err("nonexistent binary must fail spawn");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);

        let mut ok_cmd = if cfg!(windows) {
            let mut c = Command::new("cmd");
            c.args(["/C", "echo bug35-ok"]);
            c
        } else {
            let mut c = Command::new("/bin/echo");
            c.arg("bug35-ok");
            c
        };
        let out = output_std(&mut ok_cmd).expect("echo must succeed");
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("bug35-ok"));

        #[cfg(unix)]
        {
            let mut cat_cmd = Command::new("/bin/cat");
            let cat_out = output_std(&mut cat_cmd).expect("cat with default null stdin must exit");
            assert!(cat_out.status.success());
            assert!(cat_out.stdout.is_empty());
        }
    }

    #[cfg(target_os = "macos")]
    mod macos_repro {
        use super::*;
        use std::os::unix::process::CommandExt;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;
        use std::time::{Duration, Instant};

        struct ChildGuard(Vec<Child>);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                for c in &mut self.0 {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
        }

        pub(super) fn run_concurrent_gate_ordering_check(use_lock: bool, rounds: usize) -> usize {
            let mut stalled_rounds = 0usize;
            let long_threads = 4;
            let short_threads = 4;
            let long_spawns_per_thread = 25;
            let short_spawns_per_thread = 150;

            for _round in 0..rounds {
                let (gate_r, gate_w) = std::io::pipe().expect("create safe gate pipe");
                let gate_r = Arc::new(gate_r);
                let gate_w = Arc::new(Mutex::new(Some(gate_w)));
                let stop = Arc::new(AtomicBool::new(false));
                let long_children = Arc::new(Mutex::new(ChildGuard(Vec::new())));
                let short_done_count = Arc::new(AtomicUsize::new(0));
                let barrier = Arc::new(Barrier::new(long_threads + short_threads));
                let mut handles = Vec::with_capacity(long_threads + short_threads);

                for _ in 0..long_threads {
                    let b = Arc::clone(&barrier);
                    let s = Arc::clone(&stop);
                    let lc = Arc::clone(&long_children);
                    let gr = Arc::clone(&gate_r);
                    handles.push(thread::spawn(move || {
                        b.wait();
                        for _ in 0..long_spawns_per_thread {
                            if s.load(Ordering::Relaxed) {
                                break;
                            }
                            let stdin_pipe = gr.try_clone().expect("clone gate read pipe");
                            let mut cmd = Command::new("/bin/cat");
                            cmd.stdin(Stdio::from(stdin_pipe))
                                .stdout(Stdio::piped())
                                .stderr(Stdio::null());
                            cmd.process_group(0);
                            let child = if use_lock {
                                spawn_std(&mut cmd).expect("spawn gated /bin/cat")
                            } else {
                                cmd.spawn().expect("spawn gated /bin/cat")
                            };
                            lc.lock().unwrap().0.push(child);
                        }
                    }));
                }

                for _ in 0..short_threads {
                    let b = Arc::clone(&barrier);
                    let s = Arc::clone(&stop);
                    let done = Arc::clone(&short_done_count);
                    handles.push(thread::spawn(move || {
                        b.wait();
                        for _ in 0..short_spawns_per_thread {
                            if s.load(Ordering::Relaxed) {
                                break;
                            }
                            let mut cmd = Command::new("/bin/echo");
                            cmd.arg("ok").stdin(Stdio::null());
                            cmd.process_group(0);
                            let out = if use_lock {
                                output_std(&mut cmd).expect("short output_std")
                            } else {
                                cmd.output().expect("short cmd.output")
                            };
                            assert!(out.status.success());
                        }
                        done.fetch_add(1, Ordering::SeqCst);
                    }));
                }

                // Purely a deadlock-breaker guard so an actual regression fails cleanly instead of
                // wedging CI forever: every short thread must reach `short_done_count` BEFORE `gate_w`
                // is released below.
                let watchdog = Instant::now() + Duration::from_secs(60);
                while short_done_count.load(Ordering::SeqCst) < short_threads
                    && Instant::now() < watchdog
                {
                    thread::sleep(Duration::from_millis(10));
                }

                let finished_before_gate = short_done_count.load(Ordering::SeqCst) == short_threads;
                if !finished_before_gate {
                    stalled_rounds += 1;
                }

                stop.store(true, Ordering::SeqCst);
                drop(gate_w.lock().unwrap().take());
                drop(gate_r);
                for h in handles {
                    h.join().expect("join worker thread");
                }
                drop(long_children);

                if !finished_before_gate && !use_lock {
                    break;
                }
            }

            stalled_rounds
        }

        #[test]
        #[cfg_attr(miri, ignore = "Miri: spawns a process")]
        fn test_macos_concurrent_spawns_do_not_inherit_unrelated_pipes() {
            let stalled = run_concurrent_gate_ordering_check(true, 10);
            assert_eq!(
                stalled, 0,
                "short spawns must all complete before the long gate opens (observed {stalled} stalls)"
            );
        }
    }
}
