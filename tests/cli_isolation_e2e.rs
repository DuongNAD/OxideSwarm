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
        .env_remove("RUSTY_GRID_ISOLATION_READ_DIRS")
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

#[cfg(windows)]
fn cli_for_bug21(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .current_dir(home)
        .env_remove("OXIDESWARM_HOME")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_BLOB_DIR")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_ISOLATION")
        .env_remove("RUSTY_GRID_ISOLATION_READ_DIRS")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

#[cfg(windows)]
fn compile_bug21_exe(dir: &Path, name: &str, src: &str) -> std::path::PathBuf {
    let rs = dir.join(format!("{name}.rs"));
    let exe = dir.join(format!("{name}.exe"));
    fs::write(&rs, src).expect("write helper rust source");
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let out = Command::new(rustc)
        .arg(&rs)
        .arg("-o")
        .arg(&exe)
        .output()
        .expect("run rustc");
    assert!(
        out.status.success(),
        "rustc failed for {name}: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    exe
}

#[cfg(windows)]
fn wait_for_condition(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    pred()
}

/// BUG-21 CLI end-to-end test: stages a small `.exe` via `submit --input` and executes it via
/// `./prog.exe`, `.\prog.exe`, `sub/prog.exe`, and `sub\prog.exe` under both `--isolation off` and
/// `--isolation fs`, with decoy executables in both the worker's own working directory and outside
/// the sandbox (`sb_base` / `sb_base\other_task` / parent), and verifies escaping paths are refused.
#[cfg(windows)]
#[test]
fn test_cli_submit_staged_relative_executable_under_off_and_fs_isolation() {
    let temp = TempDir::new().unwrap();
    let home = temp.path();
    let bin_dir = home.join("bins");
    fs::create_dir_all(&bin_dir).unwrap();

    let prog_exe = compile_bug21_exe(
        &bin_dir,
        "prog",
        r#"fn main() {
    let arg = std::env::args().nth(1).unwrap_or_default();
    let cwd = std::env::current_dir().unwrap();
    println!("CLI_REL_OK:{}:{}", arg, cwd.display());
}
"#,
    );
    let decoy_canary = home.join("decoy_canary.txt");
    let decoy_src = format!(
        "fn main() {{ let _ = std::fs::write(r#\"{}\"# , \"DECOY_HIT\"); println!(\"DECOY_RAN\"); }}\n",
        decoy_canary.display()
    );
    let decoy_exe = compile_bug21_exe(&bin_dir, "decoy", &decoy_src);

    let worker_cwd = home.join("worker_cwd");
    fs::create_dir_all(&worker_cwd).unwrap();
    fs::copy(&decoy_exe, worker_cwd.join("prog.exe")).unwrap();

    let port_file = home.join("master.port");
    let blob_dir = home.join("blobs");
    let master_log = home.join("master.log");
    let master_out = fs::File::create(&master_log).unwrap();
    let master_err = master_out.try_clone().unwrap();
    let mut master_cmd = cli_for_bug21(home);
    master_cmd
        .args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&port_file)
        .arg("--blob-dir")
        .arg(&blob_dir)
        .stdout(Stdio::from(master_out))
        .stderr(Stdio::from(master_err));
    let _master = Running(master_cmd.spawn().expect("spawn master"));

    let mut master_port = None;
    assert!(
        wait_for_condition(Duration::from_secs(20), || {
            master_port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|s| s.trim().parse::<u16>().ok());
            master_port.is_some()
        }),
        "master did not publish port: {}",
        fs::read_to_string(&master_log).unwrap_or_default()
    );
    let master_addr = format!("127.0.0.1:{}", master_port.unwrap());

    let input_root = format!("{}:prog.exe", prog_exe.display());
    let input_sub = format!("{}:sub/prog.exe", prog_exe.display());

    let fake_cargo = home.join("fake_cargo");
    let fake_rustup = home.join("fake_rustup");
    fs::create_dir_all(&fake_cargo).unwrap();
    fs::create_dir_all(&fake_rustup).unwrap();

    let mut levels = vec!["off"];
    if rusty_grid_worker::isolation::windows::OwnedSid::oxideswarm_toolchain().is_ok() {
        levels.push("fs");
    } else {
        assert!(
            std::env::var_os("RUSTY_GRID_REQUIRE_APPCONTAINER").is_none(),
            "AppContainer required (RUSTY_GRID_REQUIRE_APPCONTAINER) but unavailable"
        );
        eprintln!("skipped fs leg in test_cli_submit_staged_relative_executable: no AppContainer");
    }

    let outside = TempDir::new().unwrap();
    fs::copy(&decoy_exe, outside.path().join("prog.exe")).unwrap();

    for iso in levels {
        let worker_tag = format!("bug21-{iso}");
        let sb_base = outside.path().join(format!("sb_{iso}"));
        fs::create_dir_all(sb_base.join("other_task")).unwrap();
        // Decoy outside any task's sandbox dir: in sb_base/other_task for `..\other_task\prog.exe`,
        // in outside.path() for `..\..\prog.exe`, and (under `off`) directly in sb_base for
        // `..\prog.exe` (`--isolation fs` refuses non-sandbox entries directly inside sb_base).
        if iso == "off" {
            fs::copy(&decoy_exe, sb_base.join("prog.exe")).unwrap();
        }
        fs::copy(&decoy_exe, sb_base.join("other_task").join("prog.exe")).unwrap();

        let w_log = home.join(format!("worker_{iso}.log"));
        let w_out = fs::File::create(&w_log).unwrap();
        let w_err = w_out.try_clone().unwrap();
        let mut w_cmd = cli_for_bug21(home);
        w_cmd
            .current_dir(&worker_cwd)
            .env("CARGO_HOME", &fake_cargo)
            .env("RUSTUP_HOME", &fake_rustup)
            .args([
                "worker",
                "--master",
                &master_addr,
                "--cores",
                "1",
                "--isolation",
                iso,
                "--tag",
                &worker_tag,
                "--sandbox-base-dir",
            ])
            .arg(&sb_base)
            .stdout(Stdio::from(w_out))
            .stderr(Stdio::from(w_err));
        let worker_guard = Running(w_cmd.spawn().expect("spawn worker"));

        assert!(
            wait_for_condition(Duration::from_secs(20), || {
                cli_for_bug21(home)
                    .args(["workers", "--master", &master_addr, "--json"])
                    .output()
                    .ok()
                    .and_then(|o| serde_json::from_slice::<serde_json::Value>(&o.stdout).ok())
                    .and_then(|v| {
                        v.as_array().map(|arr| {
                            arr.iter().any(|w| {
                                let connected = w.get("status").and_then(|s| s.as_str())
                                    != Some("Disconnected");
                                let has_tag =
                                    w.get("tags")
                                        .and_then(|t| t.as_array())
                                        .is_some_and(|tags| {
                                            tags.iter()
                                                .any(|t| t.as_str() == Some(worker_tag.as_str()))
                                        });
                                connected && has_tag
                            })
                        })
                    })
                    .unwrap_or(false)
            }),
            "[{iso}] worker did not register: {}",
            fs::read_to_string(&w_log).unwrap_or_default()
        );

        for command in ["./prog.exe", r".\prog.exe", "sub/prog.exe", r"sub\prog.exe"] {
            let mut sub = cli_for_bug21(home);
            sub.args([
                "submit",
                "--master",
                &master_addr,
                "--wait",
                "--timeout-secs",
                "45",
                "--require-tag",
                &worker_tag,
                "--input",
                &input_root,
                "--input",
                &input_sub,
            ]);
            if iso == "fs" {
                sub.args(["--require-isolation", "fs"]);
            }
            sub.args(["--command", command, "--", "1024"]);
            let out = sub.output().expect("run submit");
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                out.status.success(),
                "[{iso}] submit --command '{command}' failed:\nstdout={stdout}\nstderr={stderr}"
            );
            assert!(
                stdout.contains("CLI_REL_OK:1024:"),
                "[{iso}] expected CLI_REL_OK:1024: for '{command}', got:\nstdout={stdout}\nstderr={stderr}"
            );
            assert!(
                !stdout.contains("DECOY_RAN") && !decoy_canary.exists(),
                "[{iso}] worker_cwd decoy must never execute for '{command}'"
            );
        }

        for escaping in [r"..\prog.exe", r"..\other_task\prog.exe", r"..\..\prog.exe"] {
            let mut esc_cmd = cli_for_bug21(home);
            esc_cmd.args([
                "submit",
                "--master",
                &master_addr,
                "--wait",
                "--timeout-secs",
                "30",
                "--require-tag",
                &worker_tag,
            ]);
            if iso == "fs" {
                esc_cmd.args(["--require-isolation", "fs"]);
            }
            esc_cmd.args(["--input", &input_root, "--command", escaping]);
            let esc_out = esc_cmd.output().expect("run escaping submit");
            assert!(
                !esc_out.status.success(),
                "[{iso}] escaping --command {escaping} must be refused"
            );
            assert!(
                !decoy_canary.exists(),
                "[{iso}] escaping --command {escaping} must never execute parent decoy"
            );
        }
        drop(worker_guard);
    }
}

#[test]
fn test_isolated_worker_refuses_broad_or_secret_isolation_read_dir() {
    let home = TempDir::new().unwrap();
    let ox_dir = home.path().join(".oxideswarm");
    let ssh_dir = home.path().join(".ssh");
    std::fs::create_dir_all(&ox_dir).unwrap();
    std::fs::create_dir_all(&ssh_dir).unwrap();
    let secret_file = ox_dir.join("cluster.secret");
    std::fs::write(&secret_file, SECRET).unwrap();

    for (label, flag_dir) in [
        ("home", home.path()),
        ("ssh", ssh_dir.as_path()),
        ("oxideswarm", ox_dir.as_path()),
    ] {
        let mut cmd = worker(home.path(), "fs");
        cmd.arg("--secret-file")
            .arg(&secret_file)
            .arg("--isolation-read-dir")
            .arg(flag_dir);
        let (code, err) = refused(cmd);
        assert_ne!(code, Some(0), "{label} must fail: {err}");
        assert_ne!(
            code,
            Some(AUTH_EXIT_CODE),
            "{label} must fail on isolation config, not auth: {err}"
        );
        assert!(
            !err.contains(SECRET),
            "{label} refusal output must not leak secret"
        );
    }

    let mut env_cmd = worker(home.path(), "fs");
    env_cmd
        .arg("--secret-file")
        .arg(&secret_file)
        .env("RUSTY_GRID_ISOLATION_READ_DIRS", &ssh_dir);
    let (code, err) = refused(env_cmd);
    assert_ne!(code, Some(0), "env ssh must fail: {err}");
    assert_ne!(
        code,
        Some(AUTH_EXIT_CODE),
        "env ssh must fail on isolation config, not auth: {err}"
    );
    assert!(
        !err.contains(SECRET),
        "env refusal output must not leak secret"
    );
}
