//! End-to-end tests of the cluster secret under `--isolation fs|fs+net` (design 0033, Security).
//!
//! On macOS no Seatbelt rule stops a task from reading the environment and the command line of
//! the other processes of its user (`sysctl KERN_PROCARGS2`). A secret given to the worker in
//! `OXIDESWARM_SECRET` or `--secret` would therefore be readable by every task, so a worker with
//! isolation refuses to start with one. A secret file is fine. These tests run the real
//! `rusty-grid worker` binary with a private `HOME`.
#![cfg(any(unix, windows))]
#![cfg_attr(not(target_os = "macos"), allow(dead_code, unused_imports))]

use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");
/// Exit code of an authentication failure.
const AUTH_EXIT_CODE: i32 = 6;
const SECRET: &str = "5ec12e75ec12e75ec12e75ec12e75ec12e75ec12e75ec12e75ec12e75ec12e7a";

fn worker(home: &Path, isolation: &str) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .current_dir(home)
        .env_remove("OXIDESWARM_HOME")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_ISOLATION")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .args([
            "worker",
            "--master",
            "127.0.0.1:9",
            "--cores",
            "1",
            "--isolation",
            isolation,
        ])
        .arg("--sandbox-base-dir")
        .arg(home.join("sandboxes"));
    cmd
}

/// Runs a worker that must exit by itself; returns (exit code, stderr).
fn refused(mut cmd: Command) -> (Option<i32>, String) {
    let mut child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Some(status) = child.try_wait().unwrap() {
            let mut err = String::new();
            std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut err).unwrap();
            return (status.code(), err);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("the worker did not exit: it was meant to refuse to start");
}

#[test]
fn test_isolated_worker_refuses_a_secret_from_the_environment() {
    let home = TempDir::new().unwrap();
    for level in ["fs", "fs+net"] {
        let mut cmd = worker(home.path(), level);
        cmd.env("OXIDESWARM_SECRET", SECRET);
        let (code, err) = refused(cmd);
        assert_eq!(code, Some(AUTH_EXIT_CODE), "{level}: {err}");
        assert!(
            err.contains("--secret-file") && err.contains("OXIDESWARM_SECRET_FILE"),
            "{level}: the message must name the fix: {err}"
        );
        assert!(
            !err.contains(SECRET),
            "the refusal must not print the secret"
        );
    }
}

#[test]
fn test_isolated_worker_refuses_a_secret_on_the_command_line() {
    let home = TempDir::new().unwrap();
    let mut cmd = worker(home.path(), "fs");
    cmd.args(["--secret", SECRET]);
    let (code, err) = refused(cmd);
    assert_eq!(code, Some(AUTH_EXIT_CODE), "{err}");
    assert!(err.contains("--secret-file"), "{err}");
    assert!(!err.contains(SECRET));
}

/// A running worker, killed on drop.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Compiles the reviewer's `procenv` (prints `NAME=<first 8 chars>` of one variable of a pid's
/// environment, as any process of the same user may read it). `None` without a C compiler.
#[cfg(target_os = "macos")]
fn build_procenv(dir: &Path) -> Option<std::path::PathBuf> {
    const SOURCE: &str = r####"// Prints NAME=<first 8 chars of value> for env entries of <pid> whose name equals argv[2].
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysctl.h>
#include <errno.h>
int main(int argc, char **argv) {
  if (argc < 3) return 2;
  int pid = atoi(argv[1]); const char *want = argv[2]; size_t wl = strlen(want);
  int mib[3] = {CTL_KERN, KERN_PROCARGS2, pid}; size_t sz = 0;
  int argmax_mib[2] = {CTL_KERN, KERN_ARGMAX}; int argmax = 1048576; size_t l = sizeof(argmax);
  if (sysctl(argmax_mib, 2, &argmax, &l, NULL, 0) != 0) argmax = 1048576;
  char *buf = malloc(argmax); sz = argmax;
  if (sysctl(mib, 3, buf, &sz, NULL, 0) != 0) { printf("sysctl KERN_PROCARGS2 failed: %s\n", strerror(errno)); return 1; }
  int nargs; memcpy(&nargs, buf, sizeof(nargs)); char *p = buf + sizeof(nargs), *end = buf + sz;
  p += strlen(p) + 1; while (p < end && *p == 0) p++;
  for (int i = 0; i < nargs && p < end; i++) p += strlen(p) + 1;
  int found = 0;
  while (p < end && *p) { if (strncmp(p, want, wl) == 0 && p[wl] == '=') { printf("FOUND %s=%.8s...\n", want, p + wl + 1); found = 1; } p += strlen(p) + 1; }
  if (!found) printf("read env block of pid %d, %s not present\n", pid, want);
  return 0;
}
"####;
    let src = dir.join("procenv.c");
    fs::write(&src, SOURCE).unwrap();
    let out = dir.join("procenv");
    let status = Command::new("cc")
        .arg(&src)
        .arg("-o")
        .arg(&out)
        .status()
        .ok()?;
    status.success().then_some(out)
}

#[cfg(target_os = "macos")]
fn procenv(bin: &Path, pid: u32, name: &str) -> String {
    let out = Command::new(bin)
        .arg(pid.to_string())
        .arg(name)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[cfg(target_os = "macos")]
fn wait_for_banner(log: &Path) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if fs::read_to_string(log)
            .unwrap_or_default()
            .contains("RustyGrid Worker ")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("no worker banner in {}", log.display());
}

/// With the secret in a file the worker's own environment and command line hold no secret, so
/// `KERN_PROCARGS2` has nothing to show a task; with `OXIDESWARM_SECRET` and no isolation it does
/// (the control that proves the probe works).
#[cfg(target_os = "macos")]
#[test]
fn test_procenv_finds_no_secret_in_an_isolated_worker_that_uses_a_secret_file() {
    let home = TempDir::new().unwrap();
    let Some(procenv_bin) = build_procenv(home.path()) else {
        eprintln!("skipped: no C compiler");
        return;
    };
    let secret_file = home.path().join("cluster.secret");
    fs::write(&secret_file, format!("{SECRET}\n")).unwrap();

    let log = home.path().join("control.log");
    let out = fs::File::create(&log).unwrap();
    let mut control = worker(home.path(), "off");
    control
        .env("OXIDESWARM_SECRET", SECRET)
        .stdout(out.try_clone().unwrap())
        .stderr(out);
    let control = Running(control.spawn().unwrap());
    wait_for_banner(&log);
    let found = procenv(&procenv_bin, control.0.id(), "OXIDESWARM_SECRET");
    assert!(
        found.contains("FOUND OXIDESWARM_SECRET=5ec12e75"),
        "the control must show the leak the refusal exists for: {found}"
    );
    drop(control);

    let log = home.path().join("isolated.log");
    let out = fs::File::create(&log).unwrap();
    let mut isolated = worker(home.path(), "fs");
    isolated
        .arg("--secret-file")
        .arg(&secret_file)
        .stdout(out.try_clone().unwrap())
        .stderr(out);
    let isolated = Running(isolated.spawn().unwrap());
    wait_for_banner(&log);
    let pid = isolated.0.id();
    for name in ["OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE"] {
        let found = procenv(&procenv_bin, pid, name);
        assert!(
            !found.contains("FOUND OXIDESWARM_SECRET="),
            "{name}: {found}"
        );
    }
    let args = Command::new("/bin/ps")
        .args(["-ww", "-o", "command=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    assert!(
        !String::from_utf8_lossy(&args.stdout).contains(SECRET),
        "the command line holds no secret"
    );
}
