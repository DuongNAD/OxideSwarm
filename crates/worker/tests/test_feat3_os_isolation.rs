//! FEAT-3 (design 0033): OS-level task isolation, `--isolation off|fs|fs+net`.
//!
//! FEAT-3a ships the macOS implementation (`sandbox-exec`) and FEAT-3b the Linux one (Landlock,
//! `seccomp`, namespaces); the tests that exercise them are `#[cfg(target_os = "macos")]` and
//! `#[cfg(target_os = "linux")]`. On Windows (FEAT-3c is still to come) the tests below check the
//! other half of the contract: a worker asked for isolation it cannot enforce refuses to start
//! and never runs a task unsandboxed.
//!
//! On Linux a kernel without Landlock skips the confinement tests with a notice, unless
//! `RUSTY_GRID_REQUIRE_LANDLOCK=1` is set (CI sets it), which makes that a failure.
//!
//! Every test uses a temporary `HOME` (with a dummy `.oxideswarm/cluster.secret` and
//! `.ssh/test_key`), its own sandbox base and loopback ports; nothing touches the developer's
//! real `~/.oxideswarm`. The real Rust toolchain is only read.

#![cfg_attr(
    not(any(target_os = "macos", target_os = "linux")),
    allow(dead_code, unused_imports)
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_worker::sandbox::SandboxConfig;
use rusty_grid_worker::{
    verify_isolation_support, HostDirs, IsolationLevel, RunnerConfig, TaskRunner,
    EXIT_CODE_CANCELLED, EXIT_CODE_TIMEOUT,
};
use tempfile::TempDir;

/// A fake owner home, a sandbox base and a runner confining tasks at `level`.
struct Rig {
    home: TempDir,
    /// Holds the sandbox base when it is not inside the home.
    _outside: TempDir,
    base: PathBuf,
    runner: TaskRunner,
    host: HostDirs,
}

const SECRET: &str = "dummy-cluster-secret-feat3";
const KEY: &str = "dummy-ssh-key-feat3";
const SIBLING: &str = "dummy-sibling-sandbox-file-feat3";

impl Rig {
    /// `base_in_home`: put the sandbox base under the fake home (like `~/.oxideswarm/sandboxes`)
    /// instead of next to it.
    fn new(level: IsolationLevel, base_in_home: bool) -> Self {
        let home = tempfile::tempdir().expect("home");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::create_dir_all(home.path().join(".oxideswarm")).unwrap();
        std::fs::write(home.path().join(".oxideswarm/cluster.secret"), SECRET).unwrap();
        std::fs::create_dir_all(home.path().join(".ssh")).unwrap();
        std::fs::write(home.path().join(".ssh/test_key"), KEY).unwrap();
        let base = if base_in_home {
            home.path().join(".oxideswarm/sandboxes")
        } else {
            outside.path().join("sandboxes")
        };
        std::fs::create_dir_all(base.join("other_task")).unwrap();
        std::fs::write(base.join("other_task/secret.txt"), SIBLING).unwrap();

        // The real toolchain, read-only; only the home is the fake one.
        let real = HostDirs::detect();
        let host = HostDirs::from_home(
            home.path().to_path_buf(),
            Some(real.rustup_home),
            Some(real.cargo_home),
        );
        let config = RunnerConfig::new(SandboxConfig::new(base.clone()))
            .with_isolation(level)
            .with_host_dirs(host.clone());
        let caps = WorkerCapabilities::new("feat3", 4, 8192, false, false, None);
        let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, config);
        Self {
            home,
            _outside: outside,
            base,
            runner,
            host,
        }
    }

    /// The same rig, but the policy treats `user_home` as the owner's home (the fake home with
    /// its dummy secrets stays where it is). With the real toolchain below it, this is the
    /// layout of a real machine: `~/.cargo` and `~/.rustup` inside the denied home.
    #[cfg(target_os = "macos")]
    fn with_policy_home(mut self, level: IsolationLevel, user_home: PathBuf) -> Self {
        self.host.user_home = user_home;
        let config = RunnerConfig::new(SandboxConfig::new(self.base.clone()))
            .with_isolation(level)
            .with_host_dirs(self.host.clone());
        let caps = WorkerCapabilities::new("feat3", 4, 8192, false, false, None);
        self.runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, config);
        self
    }

    fn env(&self) -> HashMap<String, String> {
        let p = |p: &Path| p.to_string_lossy().into_owned();
        HashMap::from([
            (
                "T_SECRET".into(),
                p(&self.home.path().join(".oxideswarm/cluster.secret")),
            ),
            ("T_KEY".into(), p(&self.home.path().join(".ssh/test_key"))),
            (
                "T_SIBLING".into(),
                p(&self.base.join("other_task/secret.txt")),
            ),
            ("T_BASE".into(), p(&self.base)),
            ("T_HOME".into(), p(self.home.path())),
        ])
    }

    fn task(&self, script: &str, extra_env: &[(&str, &str)], timeout_secs: u64) -> Task {
        let mut env = self.env();
        for (k, v) in extra_env {
            env.insert((*k).into(), (*v).into());
        }
        Task::new(
            TaskSpec::Command {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), script.into()],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, timeout_secs),
        )
    }

    async fn run(&self, script: &str) -> TaskResult {
        self.runner
            .execute_task(&self.task(script, &[], 60), None, None)
            .await
    }
}

fn lines(result: &TaskResult) -> Vec<String> {
    result.stdout_str().lines().map(str::to_string).collect()
}

#[cfg(target_os = "macos")]
const FS_SCRIPT: &str = r#"
echo hello > "$RUSTY_GRID_SANDBOX_DIR/allowed.txt" && echo WRITE_OK
[ "$(cat "$RUSTY_GRID_SANDBOX_DIR/allowed.txt")" = hello ] && echo READ_OK
mkdir "$RUSTY_GRID_SANDBOX_DIR/sub" && echo MKDIR_OK
echo x > /dev/null && echo DEVNULL_OK
cat "$T_SECRET" > /dev/null 2>&1 && echo LEAK_SECRET || echo DENY_SECRET
cat "$T_KEY" > /dev/null 2>&1 && echo LEAK_KEY || echo DENY_KEY
cat "$T_SIBLING" > /dev/null 2>&1 && echo LEAK_SIBLING || echo DENY_SIBLING
ls "$T_BASE" > /dev/null 2>&1 && echo LEAK_LISTING || echo DENY_LISTING
ls "$T_HOME" > /dev/null 2>&1 && echo LEAK_HOME_LISTING || echo DENY_HOME_LISTING
( echo x > "$T_HOME/planted.txt" ) 2>/dev/null && echo LEAK_WRITE_HOME || echo DENY_WRITE_HOME
( echo x > "$T_BASE/other_task/planted.txt" ) 2>/dev/null && echo LEAK_WRITE_SIBLING || echo DENY_WRITE_SIBLING
( echo x > /private/tmp/feat3_outside_marker ) 2>/dev/null && echo LEAK_WRITE_TMP || echo DENY_WRITE_TMP
"#;

#[cfg(target_os = "macos")]
fn assert_fs_policy_holds(result: &TaskResult, what: &str) {
    assert!(
        result.is_success(),
        "{what}: the task itself must run: {:?} / {}",
        result.error,
        result.stderr_str()
    );
    let got = lines(result);
    for expected in [
        "WRITE_OK",
        "READ_OK",
        "MKDIR_OK",
        "DEVNULL_OK",
        "DENY_SECRET",
        "DENY_KEY",
        "DENY_SIBLING",
        "DENY_LISTING",
        "DENY_HOME_LISTING",
        "DENY_WRITE_HOME",
        "DENY_WRITE_SIBLING",
        "DENY_WRITE_TMP",
    ] {
        assert!(
            got.iter().any(|l| l == expected),
            "{what}: missing {expected}; output was {got:?}"
        );
    }
    assert!(
        !got.iter().any(|l| l.starts_with("LEAK_")),
        "{what}: a forbidden access succeeded: {got:?}"
    );
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;

    #[tokio::test]
    async fn test_off_leaves_the_owners_files_readable_as_a_control() {
        let rig = Rig::new(IsolationLevel::Off, false);
        let result = rig.run(FS_SCRIPT).await;
        let got = lines(&result);
        for leak in ["LEAK_SECRET", "LEAK_KEY", "LEAK_SIBLING", "LEAK_WRITE_HOME"] {
            assert!(
                got.iter().any(|l| l == leak),
                "without isolation {leak} must succeed (otherwise the tests below prove nothing): {got:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_fs_confines_a_task_to_its_sandbox() {
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig.run(FS_SCRIPT).await;
            assert_fs_policy_holds(&result, &level.to_string());
            assert!(
                !rig.home.path().join("planted.txt").exists()
                    && !rig.base.join("other_task/planted.txt").exists(),
                "nothing was planted outside the sandbox"
            );
            assert_eq!(
                std::fs::read_to_string(rig.base.join("other_task/secret.txt")).unwrap(),
                SIBLING
            );
        }
    }

    #[tokio::test]
    async fn test_fs_holds_when_the_sandbox_base_lives_inside_the_denied_home() {
        // `~/.oxideswarm/sandboxes`: the sandbox is inside the directory the policy denies, and
        // must stay usable while its siblings stay hidden.
        let rig = Rig::new(IsolationLevel::Fs, true);
        let result = rig.run(FS_SCRIPT).await;
        assert_fs_policy_holds(&result, "base inside home");
    }

    #[tokio::test]
    async fn test_environment_is_moved_into_the_sandbox_and_ipc_sockets_are_removed() {
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = rig.task(
            r#"
echo "HOME=$HOME"; echo "USERPROFILE=$USERPROFILE"; echo "TMPDIR=$TMPDIR"; echo "TEMP=$TEMP"; echo "TMP=$TMP"
echo "SANDBOX=$RUSTY_GRID_SANDBOX_DIR"
echo "RUSTUP_HOME=$RUSTUP_HOME"; echo "CARGO_HOME=$CARGO_HOME"
echo "SSH=${SSH_AUTH_SOCK-unset}"; echo "GPG=${GPG_AGENT_INFO-unset}"
echo "DBUS=${DBUS_SESSION_BUS_ADDRESS-unset}"; echo "DOCKER=${DOCKER_HOST-unset}"
echo "SECRET=${OXIDESWARM_SECRET-unset}"
echo "TOKEN=${RUSTY_GRID_RUN_TOKEN-unset}"
"#,
            &[
                ("SSH_AUTH_SOCK", "/tmp/agent.sock"),
                ("GPG_AGENT_INFO", "x"),
                ("DBUS_SESSION_BUS_ADDRESS", "unix:path=/tmp/bus"),
                ("DOCKER_HOST", "unix:///var/run/docker.sock"),
            ],
            60,
        );
        let result = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result.is_success(),
            "{:?} {}",
            result.error,
            result.stderr_str()
        );
        let got: HashMap<String, String> = lines(&result)
            .iter()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let sandbox = got["SANDBOX"].clone();
        assert!(sandbox.contains(&rig.base.file_name().unwrap().to_string_lossy().to_string()));
        for name in ["HOME", "USERPROFILE", "TMPDIR", "TEMP", "TMP"] {
            assert_eq!(got[name], sandbox, "{name}");
        }
        assert_eq!(got["RUSTUP_HOME"], rig.host.rustup_home.to_string_lossy());
        assert_eq!(got["CARGO_HOME"], rig.host.cargo_home.to_string_lossy());
        assert_ne!(
            got["TOKEN"], "unset",
            "FEAT-2's containment token must survive the environment preparation"
        );
        for name in ["SSH", "GPG", "DBUS", "DOCKER", "SECRET"] {
            assert_eq!(got[name], "unset", "{name}");
        }
    }

    #[tokio::test]
    async fn test_fs_net_refuses_tcp_connections_and_fs_allows_them() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port().to_string();
        let script = r#"/usr/bin/nc -z -w 2 127.0.0.1 "$T_PORT" && echo CONNECTED || echo REFUSED"#;

        // Control: under `fs` the very same connect works, so the refusal below is the sandbox.
        let rig = Rig::new(IsolationLevel::Fs, false);
        let result = rig
            .runner
            .execute_task(&rig.task(script, &[("T_PORT", &port)], 60), None, None)
            .await;
        assert!(
            lines(&result).contains(&"CONNECTED".to_string()),
            "{:?}",
            lines(&result)
        );
        let accepted_in_control = listener.accept().is_ok();
        assert!(
            accepted_in_control,
            "the control connect reached the listener"
        );
        while listener.accept().is_ok() {}

        let rig = Rig::new(IsolationLevel::FsNet, false);
        let result = rig
            .runner
            .execute_task(&rig.task(script, &[("T_PORT", &port)], 60), None, None)
            .await;
        assert!(
            lines(&result).contains(&"REFUSED".to_string()),
            "{:?} {}",
            lines(&result),
            result.stderr_str()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            other => panic!("the listener must see no connection from an fs+net task: {other:?}"),
        }
    }

    /// G2: `cargo build --offline` of a tiny crate works under `fs` with the preserved
    /// `RUSTUP_HOME` / `CARGO_HOME`, read-only.
    #[tokio::test]
    async fn test_cargo_build_offline_works_under_fs() {
        let cargo = HostDirs::detect().cargo_home.join("bin/cargo");
        if !cargo.exists() {
            eprintln!("skipped: no rustup cargo proxy at {}", cargo.display());
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let path = format!(
                "{}:/usr/bin:/bin",
                rig.host.cargo_home.join("bin").to_string_lossy()
            );
            let task = rig.task(
                r#"
set -e
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR
mkdir -p tiny/src
printf '[package]\nname = "tiny"\nversion = "0.1.0"\nedition = "2021"\n' > tiny/Cargo.toml
echo 'fn main() { println!("tiny-ran"); }' > tiny/src/main.rs
cd tiny
cargo build --offline 2>&1
./target/debug/tiny
"#,
                &[("PATH", &path)],
                300,
            );
            let result = rig.runner.execute_task(&task, None, None).await;
            assert!(
                result.is_success(),
                "{level}: {:?}\n{}\n{}",
                result.error,
                result.stdout_str(),
                result.stderr_str()
            );
            assert!(
                result.stdout_str().contains("tiny-ran"),
                "{}",
                result.stdout_str()
            );
        }
    }

    /// The toolchain lives inside the denied home on a real machine (`~/.cargo`, `~/.rustup`), and
    /// a dependency from the cached registry is compiled from `$CARGO_HOME/registry/src`:
    /// cargo canonicalizes those paths, which needs `stat` on the directories on the way.
    #[tokio::test]
    async fn test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home() {
        let real = HostDirs::detect();
        let cargo = real.cargo_home.join("bin/cargo");
        let Some(home) = real.cargo_home.parent().map(Path::to_path_buf) else {
            return;
        };
        // A leaf crate (no required dependencies) whose source is in the local registry.
        let cached = std::fs::read_dir(real.cargo_home.join("registry/cache"))
            .into_iter()
            .flatten()
            .flatten()
            .flat_map(|index| {
                std::fs::read_dir(index.path())
                    .into_iter()
                    .flatten()
                    .flatten()
            })
            .filter_map(|f| f.file_name().to_str().map(str::to_string))
            .filter_map(|f| f.strip_suffix(".crate").map(str::to_string))
            .find_map(|stem| {
                let (name, version) = stem.rsplit_once('-')?;
                ["cfg-if", "itoa", "bytes", "memchr", "smallvec", "once_cell"]
                    .contains(&name)
                    .then(|| (name.to_string(), version.to_string()))
            });
        let Some((name, version)) = cached else {
            eprintln!(
                "skipped: no cached leaf crate in {}",
                real.cargo_home.display()
            );
            return;
        };
        if !cargo.exists() {
            eprintln!("skipped: no rustup cargo proxy at {}", cargo.display());
            return;
        }
        let rig = Rig::new(IsolationLevel::Fs, false).with_policy_home(IsolationLevel::Fs, home);
        let path = format!(
            "{}:/usr/bin:/bin",
            rig.host.cargo_home.join("bin").to_string_lossy()
        );
        let script = format!(
            r#"
set -e
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR
mkdir -p dep/src
printf '[package]\nname = "dep"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\n{name} = "={version}"\n' > dep/Cargo.toml
echo 'fn main() {{ println!("dep-ran"); }}' > dep/src/main.rs
cd dep
cargo build --offline 2>&1
./target/debug/dep
"#
        );
        let result = rig
            .runner
            .execute_task(&rig.task(&script, &[("PATH", &path)], 300), None, None)
            .await;
        assert!(
            result.is_success(),
            "{name} {version}: {:?}\n{}\n{}",
            result.error,
            result.stdout_str(),
            result.stderr_str()
        );
        assert!(result.stdout_str().contains("dep-ran"));
    }

    /// The PID the worker tracks is the task itself (`sandbox-exec` execs in place), so the
    /// process-group kill, the timeout and the cancellation work as without isolation.
    #[tokio::test]
    async fn test_tracked_pid_is_the_task_and_cancel_and_timeout_still_kill_it() {
        fn comm_of(pid: u32) -> String {
            let out = std::process::Command::new("/bin/ps")
                .args(["-p", &pid.to_string(), "-o", "comm="])
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        }
        fn alive(pid: u32) -> bool {
            unsafe { libc::kill(pid as i32, 0) == 0 }
        }

        let rig = Rig::new(IsolationLevel::Fs, false);
        let rig = Arc::new(rig);

        // Cancellation.
        let pid = Arc::new(AtomicU32::new(0));
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<String>();
        let handle = {
            let (rig, pid) = (Arc::clone(&rig), Arc::clone(&pid));
            tokio::spawn(async move {
                let task = rig.task("exec sleep 120", &[], 120);
                rig.runner
                    .execute_task(&task, Some(pid), Some(cancel_rx))
                    .await
            })
        };
        let tracked = wait_for_pid(&pid).await;
        // The PID is recorded at spawn; `sandbox-exec` and then `sh` exec in place, so the name
        // settles on `sleep` within moments. It must never be another PID (a child of the
        // wrapper), which the single value below proves.
        let mut name = comm_of(tracked);
        for _ in 0..100 {
            if name.ends_with("sleep") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            name = comm_of(tracked);
        }
        assert!(
            name.ends_with("sleep"),
            "the tracked PID must become the task itself, not stay sandbox-exec; it is `{name}`"
        );
        cancel_tx.send("test".into()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(20), handle)
            .await
            .expect("cancellation finishes the task")
            .unwrap();
        assert_eq!(result.exit_code, EXIT_CODE_CANCELLED, "{:?}", result.error);
        assert!(wait_dead(tracked).await, "the cancelled task is gone");
        assert!(!alive(tracked));

        // Timeout, with a grandchild that must go down with the group.
        let pid = Arc::new(AtomicU32::new(0));
        let task = rig.task(
            "sleep 120 & echo $! > \"$RUSTY_GRID_SANDBOX_DIR/child.pid\"; wait",
            &[],
            2,
        );
        let started = std::time::Instant::now();
        let result = rig
            .runner
            .execute_task(&task, Some(Arc::clone(&pid)), None)
            .await;
        assert_eq!(result.exit_code, EXIT_CODE_TIMEOUT, "{:?}", result.error);
        assert!(started.elapsed() < Duration::from_secs(30));
        let tracked = pid.load(Ordering::Acquire);
        assert!(tracked > 1);
        assert!(wait_dead(tracked).await, "the timed-out task is gone");
    }

    // ---- Security review of PR 133: escapes that are not file accesses -------------------

    /// Runs `script` as a task at `level`; the owner's dummy secret path is in `$T_SECRET`.
    async fn run_at(level: IsolationLevel, script: &str, extra: &[(&str, &str)]) -> TaskResult {
        let rig = Rig::new(level, false);
        rig.runner
            .execute_task(&rig.task(script, extra, 120), None, None)
            .await
    }

    fn has(result: &TaskResult, line: &str) -> bool {
        lines(result).iter().any(|l| l == line)
    }

    /// H1: LaunchServices starts a background-only app that launchd runs unconfined. The app
    /// copies the owner's dummy secret into the sandbox. `(deny lsopen)` stops `open` and
    /// `NSWorkspace` alike.
    #[tokio::test]
    async fn test_launchservices_cannot_start_an_unconfined_helper() {
        // The helper is built by the task, so the script is the same at every level. The bundle
        // is created in the task's sandbox and launched either with `open` or with NSWorkspace.
        let script = |how: &str| {
            format!(
                r#"
APP="$RUSTY_GRID_SANDBOX_DIR/Esc.app"; mkdir -p "$APP/Contents/MacOS"
cat > "$APP/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>CFBundleExecutable</key><string>run</string><key>CFBundleIdentifier</key><string>dev.oxideswarm.feat3.esc</string><key>CFBundlePackageType</key><string>APPL</string><key>LSBackgroundOnly</key><true/></dict></plist>
PLIST
printf '#!/bin/sh\n/bin/cat "%s" > "%s/stolen.txt" 2>&1\n' "$T_SECRET" "$RUSTY_GRID_SANDBOX_DIR" > "$APP/Contents/MacOS/run"
chmod +x "$APP/Contents/MacOS/run"
{how}
i=0; while [ $i -lt 30 ] && [ ! -s "$RUSTY_GRID_SANDBOX_DIR/stolen.txt" ]; do sleep 0.2; i=$((i+1)); done
echo "STOLEN=$(cat "$RUSTY_GRID_SANDBOX_DIR/stolen.txt" 2>/dev/null)"
"#
            )
        };
        let routes = [
            ("open", script("/usr/bin/open -g -j \"$APP\"")),
            (
                "nsworkspace",
                script(
                    "/usr/bin/osascript -l JavaScript -e 'ObjC.import(\"AppKit\"); $.NSWorkspace.sharedWorkspace.openURL($.NSURL.fileURLWithPath(\"'\"$APP\"'\"))' >/dev/null 2>&1",
                ),
            ),
        ];
        for (route, script) in &routes {
            let control = run_at(IsolationLevel::Off, script, &[]).await;
            if !control.stdout_str().contains(SECRET) {
                eprintln!(
                    "skipped {route}: LaunchServices is not usable here (the control failed)"
                );
                continue;
            }
            for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
                let result = run_at(level, script, &[]).await;
                assert!(
                    !result.stdout_str().contains(SECRET),
                    "{route} at {level}: a LaunchServices helper read the denied secret: {}",
                    result.stdout_str()
                );
            }
        }
    }

    /// H2: cfprefsd reads and writes the owner's preference files on the task's behalf.
    #[tokio::test]
    async fn test_preferences_of_the_owner_are_neither_read_nor_written() {
        let domain = format!("dev.oxideswarm.feat3.read.{}", std::process::id());
        let wdomain = format!("dev.oxideswarm.feat3.write.{}", std::process::id());
        let defaults = |args: &[&str]| {
            std::process::Command::new("/usr/bin/defaults")
                .args(args)
                .output()
        };
        let cleanup = || {
            let _ = defaults(&["delete", &domain]);
            let _ = defaults(&["delete", &wdomain]);
        };
        if !defaults(&["write", &domain, "key", "owner-preference-value"])
            .is_ok_and(|o| o.status.success())
        {
            eprintln!("skipped: cannot write preferences here");
            return;
        }
        let script = format!(
            r#"
echo "READ=$(/usr/bin/defaults read {domain} key 2>/dev/null)"
/usr/bin/defaults write {wdomain} planted yes 2>/dev/null; echo "WRITE_RC=$?"
echo "READ_AFTER_WRITE=$(/usr/bin/defaults read {wdomain} planted 2>/dev/null)"
"#
        );
        let control = run_at(IsolationLevel::Off, &script, &[]).await;
        let control_reads = has(&control, "READ=owner-preference-value");
        let _ = defaults(&["delete", &wdomain]);
        if !control_reads {
            cleanup();
            eprintln!("skipped: preferences are not readable even unconfined here");
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, &script, &[]).await;
            assert!(
                !result.stdout_str().contains("owner-preference-value"),
                "{level}: read the owner's preference: {}",
                result.stdout_str()
            );
            assert!(
                !has(&result, "READ_AFTER_WRITE=yes"),
                "{level}: wrote a preference into the owner's home"
            );
            let planted =
                defaults(&["read", &wdomain, "planted"]).is_ok_and(|o| o.status.success());
            assert!(
                !planted,
                "{level}: the owner's preferences now hold the task's write"
            );
        }
        cleanup();
    }

    /// M1: the worker's environment is not inherited, only an allowlist.
    #[tokio::test]
    async fn test_worker_only_environment_variables_do_not_reach_a_confined_task() {
        // `CARGO_PKG_NAME` is set by `cargo test` in this very process and is not on the list.
        assert!(std::env::var_os("CARGO_PKG_NAME").is_some());
        let script = r#"echo "WORKER_VAR=${CARGO_PKG_NAME-unset}"; echo "PATH_SET=${PATH:+yes}"; echo "TASK_VAR=${T_BASE:+yes}"; echo "ID=${RUSTY_GRID_TASK_ID:+yes}""#;
        let control = run_at(IsolationLevel::Off, script, &[]).await;
        assert!(
            has(&control, "WORKER_VAR=rusty_grid_worker"),
            "{:?}",
            lines(&control)
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, script, &[]).await;
            for expected in ["WORKER_VAR=unset", "PATH_SET=yes", "TASK_VAR=yes", "ID=yes"] {
                assert!(
                    has(&result, expected),
                    "{level}: {expected} in {:?}",
                    lines(&result)
                );
            }
        }
    }

    /// M2: the desktop session's Unix sockets (ssh-agent, gpg-agent, docker) are unreachable.
    #[tokio::test]
    async fn test_unix_sockets_cannot_be_connected_to_under_fs() {
        // Server and client both run in the task's sandbox, so only the socket rule differs
        // between the control and the confined runs.
        let script = r#"
cd "$RUSTY_GRID_SANDBOX_DIR"
S=agent.sock   # relative: a Unix socket path is limited to about 100 bytes
/usr/bin/nc -lU "$S" >/dev/null 2>&1 &
i=0; while [ $i -lt 25 ] && [ ! -S "$S" ]; do sleep 0.2; i=$((i+1)); done
[ -S "$S" ] && echo SOCKET_UP
echo | /usr/bin/nc -U -w 2 "$S" >/dev/null 2>&1 && echo CONNECTED || echo REFUSED
kill $! 2>/dev/null
"#;
        let control = run_at(IsolationLevel::Off, script, &[]).await;
        if !(has(&control, "SOCKET_UP") && has(&control, "CONNECTED")) {
            eprintln!(
                "skipped: no Unix socket connect even unconfined: {:?}",
                lines(&control)
            );
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, script, &[]).await;
            assert!(has(&result, "REFUSED"), "{level}: {:?}", lines(&result));
        }
    }

    /// M3: the clipboard. Reads nothing of the developer's clipboard; skips when it is empty.
    #[tokio::test]
    async fn test_clipboard_is_not_readable() {
        let clip = std::process::Command::new("/usr/bin/pbpaste").output();
        let Ok(clip) = clip else { return };
        if clip.stdout.is_empty() {
            eprintln!("skipped: the clipboard holds no text, nothing to leak");
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, "/usr/bin/pbpaste 2>/dev/null; echo END", &[]).await;
            assert_eq!(
                result.stdout_str().trim(),
                "END",
                "{level}: the task read the clipboard"
            );
        }
    }

    /// M5: a task cannot signal processes that are not its own, the worker included.
    #[tokio::test]
    async fn test_a_task_cannot_signal_foreign_processes() {
        let mut victim = std::process::Command::new("/bin/sleep")
            .arg("120")
            .spawn()
            .expect("victim");
        let pid = victim.id().to_string();
        let script = r#"kill -0 "$VICTIM" 2>/dev/null && echo SIGNALLED || echo DENIED
sleep 120 & kill -STOP $! && echo OWN_CHILD_OK; kill -9 $! 2>/dev/null; wait $! 2>/dev/null; true"#;
        let control = run_at(IsolationLevel::Off, script, &[("VICTIM", &pid)]).await;
        let control_ok = has(&control, "SIGNALLED");
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, script, &[("VICTIM", &pid)]).await;
            assert!(has(&result, "DENIED"), "{level}: {:?}", lines(&result));
            assert!(
                has(&result, "OWN_CHILD_OK"),
                "{level}: a task can still signal its own children"
            );
        }
        let _ = victim.kill();
        let _ = victim.wait();
        assert!(control_ok, "the unconfined control must be able to signal");
    }

    /// M4: TIOCSTI. Runs the task from a process that has a controlling terminal (a pty made by
    /// python3 around this very test binary, see `tiocsti_inner`).
    #[tokio::test]
    async fn test_a_task_cannot_inject_keystrokes_into_the_controlling_terminal() {
        if std::process::Command::new("/usr/bin/python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: no python3 for the pty harness");
            return;
        }
        let harness = r#"
import os, pty, select, sys, time
exe, level = sys.argv[1], sys.argv[2]
pid, fd = pty.fork()
if pid == 0:
    os.environ["FEAT3_TIOCSTI_LEVEL"] = level
    os.execv(exe, [exe, "--exact", "macos::tiocsti_inner", "--ignored", "--nocapture", "--test-threads=1"])
out = b""
end = time.time() + 90
done = False
while time.time() < end:
    r, _, _ = select.select([fd], [], [], 0.3)
    if r:
        try:
            d = os.read(fd, 65536)
        except OSError:
            break
        if not d:
            break
        out += d
        continue
    # Nothing to read: stop once the child has exited.
    if done:
        break
    wpid, _ = os.waitpid(pid, os.WNOHANG)
    if wpid == pid:
        done = True
sys.stdout.write(out.decode("utf-8", "replace"))
"#;
        let exe = std::env::current_exe().unwrap();
        let mut seen = HashMap::new();
        for level in ["off", "fs", "fs+net"] {
            let out = std::process::Command::new("/usr/bin/python3")
                .args(["-c", harness, exe.to_str().unwrap(), level])
                .output()
                .expect("harness");
            let text = String::from_utf8_lossy(&out.stdout).into_owned();
            seen.insert(level, text);
        }
        let off = &seen["off"];
        if !off.contains("TIOCSTI=INJECTED") {
            eprintln!("skipped: the unconfined control could not inject either: {off}");
            return;
        }
        for level in ["fs", "fs+net"] {
            let text = &seen[level];
            assert!(text.contains("TIOCSTI=DENIED"), "{level}: {text}");
            assert!(!text.contains("TIOCSTI=INJECTED"), "{level}: {text}");
        }
    }

    /// The pty-side half of the test above; ignored unless the harness starts it.
    #[tokio::test]
    #[ignore]
    async fn tiocsti_inner() {
        let level: IsolationLevel = std::env::var("FEAT3_TIOCSTI_LEVEL")
            .unwrap_or_default()
            .parse()
            .unwrap_or_default();
        // A real attacker ignores the job-control signals a background process group gets.
        let script = r#"trap '' TTOU TTIN TSTP; /usr/bin/perl -e 'open(TTY, "+<", "/dev/tty") or do { print "TIOCSTI=NOTTY\n"; exit }; my $c = "@"; if (ioctl(TTY, 0x80017472, $c)) { print "TIOCSTI=INJECTED\n" } else { print "TIOCSTI=DENIED\n" }'"#;
        let result = run_at(level, script, &[]).await;
        println!("{}", result.stdout_str());
    }

    /// Review of the re-verification: the cluster secret file (and the config file) may live
    /// anywhere, so the worker hands their paths to the policy.
    #[tokio::test]
    async fn test_a_secret_file_outside_the_home_is_unreadable_to_a_task() {
        // The toolchain directories are readable by policy, so a file placed there is readable
        // unless it is named as protected: the strongest proof that the rule is the cause.
        let outside = tempfile::tempdir().unwrap();
        let cargo_home = outside.path().join("cargo");
        std::fs::create_dir_all(&cargo_home).unwrap();
        let secret_file = cargo_home.join("cluster.secret");
        std::fs::write(&secret_file, "DUMMY-SECRETFILE-feat3a").unwrap();
        let config_file = cargo_home.join("rusty-grid.toml");
        std::fs::write(&config_file, "secret = \"DUMMY-CONFIG-feat3a\"").unwrap();

        let run = |level: IsolationLevel, protect: bool| {
            let rig = Rig::new(level, false);
            let mut host = rig.host.clone();
            host.cargo_home = cargo_home.clone();
            let mut config = RunnerConfig::new(SandboxConfig::new(rig.base.clone()))
                .with_isolation(level)
                .with_host_dirs(host);
            if protect {
                config =
                    config.with_protected_files(vec![secret_file.clone(), config_file.clone()]);
            }
            let runner = TaskRunner::new(
                uuid::Uuid::new_v4(),
                WorkerCapabilities::new("feat3", 4, 8192, false, false, None),
                config,
            );
            let task = rig.task(
                r#"cat "$T_A" 2>&1; echo; cat "$T_B" 2>&1; echo END"#,
                &[
                    ("T_A", secret_file.to_str().unwrap()),
                    ("T_B", config_file.to_str().unwrap()),
                ],
                60,
            );
            async move {
                let result = runner.execute_task(&task, None, None).await;
                drop(rig);
                result
            }
        };
        // Unprotected, the policy itself lets the task read it (so the rule is what stops it).
        let open = run(IsolationLevel::Fs, false).await;
        assert!(
            open.stdout_str().contains("DUMMY-SECRETFILE-feat3a"),
            "{}",
            open.stdout_str()
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run(level, true).await;
            let out = result.stdout_str();
            assert!(
                !out.contains("DUMMY-SECRETFILE-feat3a") && !out.contains("DUMMY-CONFIG-feat3a"),
                "{level}: {out}"
            );
            assert!(out.contains("Operation not permitted"), "{level}: {out}");
        }
    }

    /// The per-user cache directory (`/var/folders/../C`) is denied like the temp directory.
    #[tokio::test]
    async fn test_the_user_cache_directory_is_unreadable_to_a_task() {
        let mut buf = vec![0 as libc::c_char; 1024];
        let n =
            unsafe { libc::confstr(libc::_CS_DARWIN_USER_CACHE_DIR, buf.as_mut_ptr(), buf.len()) };
        if n == 0 || n > buf.len() {
            eprintln!("skipped: no user cache dir");
            return;
        }
        let dir = PathBuf::from(
            unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
                .to_string_lossy()
                .into_owned(),
        );
        let Ok(probe) = tempfile::Builder::new()
            .prefix("feat3a-cache-probe")
            .tempfile_in(&dir)
        else {
            eprintln!("skipped: cannot write to {}", dir.display());
            return;
        };
        std::fs::write(probe.path(), "DUMMY-CACHE-feat3a").unwrap();
        let path = probe.path().to_str().unwrap().to_string();
        let script =
            r#"cat "$T_FILE" 2>&1; ls "$T_DIR" >/dev/null 2>&1 && echo LISTED || echo NOT_LISTED"#;
        let extra = [("T_FILE", path.as_str()), ("T_DIR", dir.to_str().unwrap())];
        let control = run_at(IsolationLevel::Off, script, &extra).await;
        assert!(
            control.stdout_str().contains("DUMMY-CACHE-feat3a"),
            "{}",
            control.stdout_str()
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, script, &extra).await;
            assert!(
                !result.stdout_str().contains("DUMMY-CACHE-feat3a"),
                "{level}"
            );
            assert!(has(&result, "NOT_LISTED"), "{level}: {:?}", lines(&result));
        }
    }

    async fn wait_for_pid(pid: &AtomicU32) -> u32 {
        for _ in 0..200 {
            let v = pid.load(Ordering::Acquire);
            if v > 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the runner never reported a child PID");
    }

    async fn wait_dead(pid: u32) -> bool {
        for _ in 0..100 {
            if unsafe { libc::kill(pid as i32, 0) } != 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_self_check_accepts_both_levels_on_macos() {
        let base = tempfile::tempdir().unwrap();
        for level in [
            IsolationLevel::Off,
            IsolationLevel::Fs,
            IsolationLevel::FsNet,
        ] {
            verify_isolation_support(level, base.path())
                .await
                .unwrap_or_else(|e| panic!("{level}: {e}"));
        }
    }

    #[tokio::test]
    async fn test_a_missing_program_fails_the_task_with_a_clear_message() {
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = Task::new(
            TaskSpec::command("no-such-program-feat3", vec![]),
            TaskRequirements::generic(1, 30),
        );
        let result = rig.runner.execute_task(&task, None, None).await;
        assert!(!result.is_success());
        assert!(
            result.stderr_str().contains("no-such-program-feat3")
                && result.stderr_str().contains("not found"),
            "{}",
            result.stderr_str()
        );
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::os::unix::net::UnixListener;

    /// The Landlock ABI of this kernel, or `None` (after a notice) when there is no Landlock and
    /// the environment does not demand it.
    fn landlock_abi() -> Option<u32> {
        match rusty_grid_worker::isolation::landlock_abi() {
            Ok(abi) => Some(abi),
            Err(e) => {
                assert!(
                    std::env::var_os("RUSTY_GRID_REQUIRE_LANDLOCK").is_none(),
                    "Landlock is required here (RUSTY_GRID_REQUIRE_LANDLOCK) but unavailable: {e}"
                );
                eprintln!("skipped: no Landlock on this kernel ({e})");
                None
            }
        }
    }

    macro_rules! require_landlock {
        () => {
            match landlock_abi() {
                Some(abi) => abi,
                None => return,
            }
        };
    }

    fn python3() -> bool {
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    }

    /// Runs `script` as a task at `level`.
    async fn run_at(level: IsolationLevel, script: &str, extra: &[(&str, &str)]) -> TaskResult {
        let rig = Rig::new(level, false);
        rig.runner
            .execute_task(&rig.task(script, extra, 120), None, None)
            .await
    }

    fn has(result: &TaskResult, line: &str) -> bool {
        lines(result).iter().any(|l| l == line)
    }

    const LEVELS: [IsolationLevel; 2] = [IsolationLevel::Fs, IsolationLevel::FsNet];

    /// Every line the policy decides: what must work inside the sandbox and what must not work
    /// anywhere else.
    const FS_SCRIPT_LINUX: &str = r#"
S="$RUSTY_GRID_SANDBOX_DIR"
echo hello > "$S/allowed.txt" && echo WRITE_OK
[ "$(cat "$S/allowed.txt")" = hello ] && echo READ_OK
mkdir "$S/sub" && echo MKDIR_OK
mv "$S/allowed.txt" "$S/sub/moved.txt" && [ -f "$S/sub/moved.txt" ] && echo RENAME_OK
printf '#!/bin/sh\necho x\n' > "$S/run.sh" && chmod +x "$S/run.sh" && "$S/run.sh" > /dev/null && echo EXEC_OK
echo x > /dev/null && echo DEVNULL_OK
read -r line < /proc/self/status && echo PROC_SELF_OK
cat "$T_SECRET" > /dev/null 2>&1 && echo LEAK_SECRET || echo DENY_SECRET
cat "$T_KEY" > /dev/null 2>&1 && echo LEAK_KEY || echo DENY_KEY
cat "$T_SIBLING" > /dev/null 2>&1 && echo LEAK_SIBLING || echo DENY_SIBLING
ls "$T_BASE" > /dev/null 2>&1 && echo LEAK_LISTING || echo DENY_LISTING
ls "$T_HOME" > /dev/null 2>&1 && echo LEAK_HOME_LISTING || echo DENY_HOME_LISTING
( echo x > "$T_HOME/planted.txt" ) 2>/dev/null && echo LEAK_WRITE_HOME || echo DENY_WRITE_HOME
( echo x > "$T_BASE/other_task/planted.txt" ) 2>/dev/null && echo LEAK_WRITE_SIBLING || echo DENY_WRITE_SIBLING
( echo x > "$T_OUTSIDE" ) 2>/dev/null && echo LEAK_WRITE_TMP || echo DENY_WRITE_TMP
ln -s "$T_KEY" "$S/symlink" && cat "$S/symlink" > /dev/null 2>&1 && echo LEAK_SYMLINK || echo DENY_SYMLINK
ln "$T_KEY" "$S/hardlink" 2>/dev/null && echo LEAK_HARDLINK || echo DENY_HARDLINK
cat "/proc/$T_PID/environ" > /dev/null 2>&1 && echo LEAK_ENVIRON || echo DENY_ENVIRON
cat "/proc/$T_PID/cmdline" > /dev/null 2>&1 && echo LEAK_CMDLINE || echo DENY_CMDLINE
"#;

    const ALLOWED: [&str; 7] = [
        "WRITE_OK",
        "READ_OK",
        "MKDIR_OK",
        "RENAME_OK",
        "EXEC_OK",
        "DEVNULL_OK",
        "PROC_SELF_OK",
    ];
    const DENIED: [&str; 12] = [
        "SECRET",
        "KEY",
        "SIBLING",
        "LISTING",
        "HOME_LISTING",
        "WRITE_HOME",
        "WRITE_SIBLING",
        "WRITE_TMP",
        "SYMLINK",
        "HARDLINK",
        "ENVIRON",
        "CMDLINE",
    ];

    /// The extra task environment of `FS_SCRIPT_LINUX`; the marker file is outside everything.
    fn fs_env(marker: &Path) -> Vec<(String, String)> {
        vec![
            ("T_PID".into(), std::process::id().to_string()),
            ("T_OUTSIDE".into(), marker.to_string_lossy().into_owned()),
        ]
    }

    async fn run_fs_script(rig: &Rig, marker: &Path) -> TaskResult {
        let env = fs_env(marker);
        let extra: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        rig.runner
            .execute_task(&rig.task(FS_SCRIPT_LINUX, &extra, 120), None, None)
            .await
    }

    fn marker_path(tag: &str) -> PathBuf {
        PathBuf::from(format!("/tmp/feat3b-{tag}-{}", std::process::id()))
    }

    fn assert_policy_holds(result: &TaskResult, what: &str) {
        assert!(
            result.is_success(),
            "{what}: the task itself must run: {:?} / {}",
            result.error,
            result.stderr_str()
        );
        let got = lines(result);
        for expected in ALLOWED {
            assert!(
                got.iter().any(|l| l == expected),
                "{what}: missing {expected}; output was {got:?}"
            );
        }
        for name in DENIED {
            assert!(
                got.iter().any(|l| *l == format!("DENY_{name}")),
                "{what}: DENY_{name} missing; output was {got:?}"
            );
        }
        assert!(
            !got.iter().any(|l| l.starts_with("LEAK_")),
            "{what}: a forbidden access succeeded: {got:?}"
        );
    }

    #[tokio::test]
    async fn test_off_leaves_the_owners_files_readable_as_a_control() {
        let marker = marker_path("control");
        let rig = Rig::new(IsolationLevel::Off, false);
        let result = run_fs_script(&rig, &marker).await;
        let _ = std::fs::remove_file(&marker);
        let got = lines(&result);
        for name in DENIED {
            let leak = format!("LEAK_{name}");
            assert!(
                got.contains(&leak),
                "without isolation {leak} must succeed (otherwise the tests below prove nothing): {got:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_fs_confines_a_task_to_its_sandbox() {
        require_landlock!();
        for level in LEVELS {
            let marker = marker_path(&level.to_string().replace('+', "-"));
            let rig = Rig::new(level, false);
            let result = run_fs_script(&rig, &marker).await;
            let planted = marker.exists();
            let _ = std::fs::remove_file(&marker);
            assert_policy_holds(&result, &level.to_string());
            assert!(!planted, "{level}: the task wrote outside its sandbox");
            assert!(
                !rig.home.path().join("planted.txt").exists()
                    && !rig.base.join("other_task/planted.txt").exists(),
                "nothing was planted outside the sandbox"
            );
            assert_eq!(
                std::fs::read_to_string(rig.base.join("other_task/secret.txt")).unwrap(),
                SIBLING
            );
        }
    }

    #[tokio::test]
    async fn test_fs_holds_when_the_sandbox_base_lives_inside_the_home() {
        require_landlock!();
        // `~/.oxideswarm/sandboxes`: the sandbox is inside the owner's home, which the policy
        // never grants, and must stay usable while its siblings stay hidden.
        let marker = marker_path("inhome");
        let rig = Rig::new(IsolationLevel::Fs, true);
        let result = run_fs_script(&rig, &marker).await;
        let _ = std::fs::remove_file(&marker);
        assert_policy_holds(&result, "base inside home");
    }

    #[tokio::test]
    async fn test_two_concurrent_tasks_cannot_read_each_others_sandbox() {
        require_landlock!();
        let script = r#"
cd "$RUSTY_GRID_SANDBOX_DIR"
echo "secret-of-$T_ME" > secret.txt
i=0; while [ ! -e "$T_BASE/$T_PEER/secret.txt" ] && [ $i -lt 300 ]; do sleep 0.1; i=$((i+1)); done
cat "$T_BASE/$T_PEER/secret.txt" 2>/dev/null && echo LEAK_PEER || echo DENY_PEER
ls "$T_BASE/$T_PEER" > /dev/null 2>&1 && echo LEAK_LIST || echo DENY_LIST
( echo x > "$T_BASE/$T_PEER/planted.txt" ) 2>/dev/null && echo LEAK_WRITE || echo DENY_WRITE
echo done > done.txt
i=0; while [ ! -e "$T_BASE/$T_PEER/done.txt" ] && [ $i -lt 300 ]; do sleep 0.1; i=$((i+1)); done
sleep 1   # the peer must see this task's done.txt before the sandbox goes away
"#;
        for (level, must_leak) in [
            (IsolationLevel::Off, true),
            (IsolationLevel::Fs, false),
            (IsolationLevel::FsNet, false),
        ] {
            let rig = Arc::new(Rig::new(level, false));
            let (a, b) = (rig.task(script, &[], 120), rig.task(script, &[], 120));
            let (id_a, id_b) = (a.id.to_string(), b.id.to_string());
            let with_peer = |mut task: Task, me: &str, peer: &str| {
                if let TaskSpec::Command { env, .. } = &mut task.spec {
                    env.insert("T_ME".into(), me.into());
                    env.insert("T_PEER".into(), peer.into());
                }
                task
            };
            let (a, b) = (with_peer(a, "a", &id_b), with_peer(b, "b", &id_a));
            let (ra, rb) = tokio::join!(
                rig.runner.execute_task(&a, None, None),
                rig.runner.execute_task(&b, None, None)
            );
            for (who, result) in [("a", &ra), ("b", &rb)] {
                assert!(
                    result.is_success(),
                    "{level}/{who}: {:?} {}",
                    result.error,
                    result.stderr_str()
                );
                let out = result.stdout_str();
                if must_leak {
                    assert!(
                        out.contains("LEAK_PEER") && out.contains("LEAK_LIST"),
                        "control {who}: {out}"
                    );
                } else {
                    assert!(
                        out.contains("DENY_PEER")
                            && out.contains("DENY_LIST")
                            && out.contains("DENY_WRITE")
                            && !out.contains("secret-of-")
                            && !out.contains("LEAK_"),
                        "{level}/{who} read the other task's sandbox: {out}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_environment_is_moved_into_the_sandbox_and_ipc_sockets_are_removed() {
        require_landlock!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = rig.task(
            r#"
echo "HOME=$HOME"; echo "TMPDIR=$TMPDIR"; echo "SANDBOX=$RUSTY_GRID_SANDBOX_DIR"
echo "RUSTUP_HOME=$RUSTUP_HOME"; echo "CARGO_HOME=$CARGO_HOME"
echo "SSH=${SSH_AUTH_SOCK-unset}"; echo "DBUS=${DBUS_SESSION_BUS_ADDRESS-unset}"; echo "DOCKER=${DOCKER_HOST-unset}"
echo "TOKEN=${RUSTY_GRID_RUN_TOKEN-unset}"
"#,
            &[
                ("SSH_AUTH_SOCK", "/tmp/agent.sock"),
                ("DBUS_SESSION_BUS_ADDRESS", "unix:path=/run/user/1000/bus"),
                ("DOCKER_HOST", "unix:///var/run/docker.sock"),
            ],
            60,
        );
        let result = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result.is_success(),
            "{:?} {}",
            result.error,
            result.stderr_str()
        );
        let got: HashMap<String, String> = lines(&result)
            .iter()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for name in ["HOME", "TMPDIR"] {
            assert_eq!(got[name], got["SANDBOX"], "{name}");
        }
        assert_eq!(got["RUSTUP_HOME"], rig.host.rustup_home.to_string_lossy());
        assert_eq!(got["CARGO_HOME"], rig.host.cargo_home.to_string_lossy());
        assert_ne!(
            got["TOKEN"], "unset",
            "FEAT-2's containment token must survive"
        );
        for name in ["SSH", "DBUS", "DOCKER"] {
            assert_eq!(got[name], "unset", "{name}");
        }
    }

    /// `fs+net`: no TCP connection, and the listener sees nothing; `fs` lets it through.
    #[tokio::test]
    async fn test_fs_net_refuses_tcp_connections_and_fs_allows_them() {
        require_landlock!();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port().to_string();
        let script = r#"bash -c 'exec 3<>/dev/tcp/127.0.0.1/$T_PORT' 2>/dev/null && echo CONNECTED || echo REFUSED"#;

        // Control: unconfined and under `fs` the very same connect works.
        for level in [IsolationLevel::Off, IsolationLevel::Fs] {
            let result = run_at(level, script, &[("T_PORT", &port)]).await;
            assert!(has(&result, "CONNECTED"), "{level}: {:?}", lines(&result));
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                listener.accept().is_ok(),
                "{level}: the connect reached the listener"
            );
            while listener.accept().is_ok() {}
        }

        let result = run_at(IsolationLevel::FsNet, script, &[("T_PORT", &port)]).await;
        assert!(
            has(&result, "REFUSED"),
            "{:?} {}",
            lines(&result),
            result.stderr_str()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            other => panic!("the listener must see no connection from an fs+net task: {other:?}"),
        }
    }

    /// Which socket domains a task can create, per level (seccomp, design 0033 D2).
    #[tokio::test]
    async fn test_socket_domains_are_refused_by_level() {
        require_landlock!();
        if !python3() {
            eprintln!("skipped: no python3");
            return;
        }
        let py = r#"
import socket
def t(name, fam, typ):
    try:
        socket.socket(fam, typ).close()
        print(name + "=OK")
    except OSError as e:
        print(name + "=ERR%d" % e.errno)
t("TCP4", socket.AF_INET, socket.SOCK_STREAM)
t("UDP4", socket.AF_INET, socket.SOCK_DGRAM)
t("TCP6", socket.AF_INET6, socket.SOCK_STREAM)
t("UNIX", socket.AF_UNIX, socket.SOCK_STREAM)
try:
    a, b = socket.socketpair(); a.close(); b.close(); print("PAIR=OK")
except OSError as e:
    print("PAIR=ERR%d" % e.errno)
"#;
        let script = r#"python3 -c "$T_PY""#;
        let control = run_at(IsolationLevel::Off, script, &[("T_PY", py)]).await;
        for name in ["TCP4=OK", "UDP4=OK", "UNIX=OK", "PAIR=OK"] {
            assert!(
                has(&control, name),
                "control {name}: {:?} {}",
                lines(&control),
                control.stderr_str()
            );
        }
        let control_tcp6 = has(&control, "TCP6=OK");
        let fs = run_at(IsolationLevel::Fs, script, &[("T_PY", py)]).await;
        for name in ["TCP4=OK", "UDP4=OK", "PAIR=OK", "UNIX=ERR13"] {
            assert!(
                has(&fs, name),
                "fs {name}: {:?} {}",
                lines(&fs),
                fs.stderr_str()
            );
        }
        assert_eq!(has(&fs, "TCP6=OK"), control_tcp6);
        let net = run_at(IsolationLevel::FsNet, script, &[("T_PY", py)]).await;
        for name in [
            "TCP4=ERR13",
            "UDP4=ERR13",
            "TCP6=ERR13",
            "UNIX=ERR13",
            "PAIR=OK",
        ] {
            assert!(
                has(&net, name),
                "fs+net {name}: {:?} {}",
                lines(&net),
                net.stderr_str()
            );
        }
    }

    /// Landlock does not mediate `connect` to a pathname Unix socket (ssh-agent, docker.sock,
    /// the D-Bus session bus); the socket filter does.
    #[tokio::test]
    async fn test_a_listening_unix_socket_outside_the_sandbox_cannot_be_reached() {
        require_landlock!();
        if !python3() {
            eprintln!("skipped: no python3");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.sock");
        let listener = UnixListener::bind(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let py = r#"
import os, socket
try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(os.environ["T_SOCK"])
    print("CONNECTED")
except OSError as e:
    print("REFUSED%d" % e.errno)
"#;
        let script = r#"python3 -c "$T_PY""#;
        let extra = [("T_PY", py), ("T_SOCK", path.to_str().unwrap())];
        let control = run_at(IsolationLevel::Off, script, &extra).await;
        assert!(
            has(&control, "CONNECTED"),
            "{:?} {}",
            lines(&control),
            control.stderr_str()
        );
        assert!(
            listener.accept().is_ok(),
            "the control connect reached the listener"
        );
        for level in LEVELS {
            let result = run_at(level, script, &extra).await;
            assert!(
                lines(&result).iter().any(|l| l.starts_with("REFUSED")),
                "{level}: {:?} {}",
                lines(&result),
                result.stderr_str()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            match listener.accept() {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                other => panic!("{level}: the listener must see nothing: {other:?}"),
            }
        }
    }

    /// `fs+net` enters a private user and network namespace when the host allows unprivileged
    /// user namespaces, with the task's ids mapped to themselves; where it does not (sysctl,
    /// AppArmor), seccomp alone enforces the level and every other test still holds.
    /// `RUSTY_GRID_REQUIRE_USERNS=1` (CI) turns "not entered" into a failure.
    #[tokio::test]
    async fn test_fs_net_enters_a_private_namespace_when_the_host_allows_it() {
        require_landlock!();
        if !python3() {
            eprintln!("skipped: no python3");
            return;
        }
        let py = r#"
import os, socket
print("NETNS=" + os.readlink("/proc/self/ns/net"))
print("UID=%d" % os.getuid())
print("GID=%d" % os.getgid())
try:
    socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 0).close()
    print("NETLINK=OK")
except OSError as e:
    print("NETLINK=ERR%d" % e.errno)
"#;
        // `exec` keeps the interpreter the task leader, the one process whose `/proc/self` the
        // policy allows.
        let script = r#"exec python3 -c "$T_PY""#;
        let value = |result: &TaskResult, key: &str| {
            lines(result)
                .iter()
                .find_map(|l| l.strip_prefix(&format!("{key}=")).map(str::to_string))
                .unwrap_or_else(|| {
                    panic!(
                        "{key} missing in {:?} {}",
                        lines(result),
                        result.stderr_str()
                    )
                })
        };
        let worker_netns = std::fs::read_link("/proc/self/ns/net").unwrap();
        let worker_ids = unsafe { (libc::getuid(), libc::getgid()) };
        let fs = run_at(IsolationLevel::Fs, script, &[("T_PY", py)]).await;
        assert_eq!(value(&fs, "NETNS"), worker_netns.to_string_lossy());
        let net = run_at(IsolationLevel::FsNet, script, &[("T_PY", py)]).await;
        let netns = value(&net, "NETNS");
        let require = std::env::var_os("RUSTY_GRID_REQUIRE_USERNS").is_some();
        if netns == worker_netns.to_string_lossy() {
            assert!(
                !require,
                "the task did not enter a private network namespace"
            );
            eprintln!("note: no private network namespace here, seccomp only");
            return;
        }
        let (uid, gid) = (value(&net, "UID"), value(&net, "GID"));
        if uid == worker_ids.0.to_string() && gid == worker_ids.1.to_string() {
            assert!(has(&net, "NETLINK=OK"), "{:?}", lines(&net));
        } else {
            // AppArmor (Ubuntu 24.04+) lets `unshare` succeed but strips the capabilities in the
            // new namespace, so the id map cannot be written: the ids show as the overflow id.
            assert!(!require, "ids are not mapped: uid {uid} gid {gid}");
            assert_eq!((uid.as_str(), gid.as_str()), ("65534", "65534"));
            eprintln!("note: private network namespace without an id map (restricted userns)");
        }
    }

    /// `io_uring` could create sockets behind the socket filter (`IORING_OP_SOCKET`), so its
    /// setup is refused with `ENOSYS`.
    #[tokio::test]
    async fn test_io_uring_setup_is_refused() {
        require_landlock!();
        if !python3() {
            eprintln!("skipped: no python3");
            return;
        }
        let py = r#"
import ctypes
libc = ctypes.CDLL(None, use_errno=True)
params = ctypes.create_string_buffer(120)
fd = libc.syscall(425, 4, params)
print("IOURING_OK" if fd >= 0 else "IOURING_ERR%d" % ctypes.get_errno())
"#;
        let script = r#"python3 -c "$T_PY""#;
        let control = run_at(IsolationLevel::Off, script, &[("T_PY", py)]).await;
        if !has(&control, "IOURING_OK") {
            eprintln!(
                "skipped: this kernel has no usable io_uring even unconfined: {:?}",
                lines(&control)
            );
            return;
        }
        for level in LEVELS {
            let result = run_at(level, script, &[("T_PY", py)]).await;
            assert!(
                has(&result, "IOURING_ERR38"),
                "{level}: {:?} {}",
                lines(&result),
                result.stderr_str()
            );
        }
    }

    /// `TIOCSTI` is refused before the kernel looks at the descriptor: a task with stdin on
    /// `/dev/null` gets `EPERM` (1) where the control gets `ENOTTY` (25).
    #[tokio::test]
    async fn test_tiocsti_is_refused() {
        require_landlock!();
        if !python3() {
            eprintln!("skipped: no python3");
            return;
        }
        let py = r#"
import fcntl
try:
    fcntl.ioctl(0, 0x5412, b"x")
    print("TIOCSTI_OK")
except OSError as e:
    print("TIOCSTI_ERR%d" % e.errno)
"#;
        let script = r#"python3 -c "$T_PY""#;
        let control = run_at(IsolationLevel::Off, script, &[("T_PY", py)]).await;
        assert!(
            has(&control, "TIOCSTI_ERR25"),
            "{:?} {}",
            lines(&control),
            control.stderr_str()
        );
        for level in LEVELS {
            let result = run_at(level, script, &[("T_PY", py)]).await;
            assert!(
                has(&result, "TIOCSTI_ERR1"),
                "{level}: {:?} {}",
                lines(&result),
                result.stderr_str()
            );
        }
    }

    /// A task cannot signal processes that are not its own, the worker included. Needs Landlock
    /// ABI 6 (Linux 6.12); on older kernels this is a documented gap, and the test only checks
    /// that a task can signal its own children.
    #[tokio::test]
    async fn test_a_task_cannot_signal_foreign_processes() {
        let abi = require_landlock!();
        let mut victim = std::process::Command::new("/bin/sleep")
            .arg("120")
            .spawn()
            .expect("victim");
        let pid = victim.id().to_string();
        let script = r#"kill -0 "$VICTIM" 2>/dev/null && echo SIGNALLED || echo DENIED
sleep 120 & kill -STOP $! && echo OWN_CHILD_OK; kill -9 $! 2>/dev/null; wait $! 2>/dev/null; true"#;
        let control = run_at(IsolationLevel::Off, script, &[("VICTIM", &pid)]).await;
        let control_ok = has(&control, "SIGNALLED");
        for level in LEVELS {
            let result = run_at(level, script, &[("VICTIM", &pid)]).await;
            assert!(
                has(&result, "OWN_CHILD_OK"),
                "{level}: a task can still signal its own children: {:?}",
                lines(&result)
            );
            if abi >= 6 {
                assert!(has(&result, "DENIED"), "{level}: {:?}", lines(&result));
            } else {
                eprintln!(
                    "note: Landlock ABI {abi} < 6, signals to foreign processes are not scoped"
                );
            }
        }
        let _ = victim.kill();
        let _ = victim.wait();
        assert!(control_ok, "the unconfined control must be able to signal");
    }

    /// A protected file (the cluster secret, the config file) inside a directory that the policy
    /// grants stays unreadable: the grant is cut around it.
    #[tokio::test]
    async fn test_a_secret_file_inside_a_readable_directory_is_carved_out() {
        require_landlock!();
        let outside = tempfile::tempdir().unwrap();
        let toolchain = outside.path().join("rustup");
        std::fs::create_dir_all(toolchain.join("deep")).unwrap();
        let secret_file = toolchain.join("deep/cluster.secret");
        std::fs::write(&secret_file, "DUMMY-SECRETFILE-feat3b").unwrap();
        let config_file = toolchain.join("rusty-grid.toml");
        std::fs::write(&config_file, "secret = \"DUMMY-CONFIG-feat3b\"").unwrap();
        std::fs::write(
            toolchain.join("deep/neighbour.txt"),
            "DUMMY-NEIGHBOUR-feat3b",
        )
        .unwrap();

        let run = |level: IsolationLevel, protect: bool| {
            let rig = Rig::new(level, false);
            let mut host = rig.host.clone();
            host.rustup_home = toolchain.clone();
            let mut config = RunnerConfig::new(SandboxConfig::new(rig.base.clone()))
                .with_isolation(level)
                .with_host_dirs(host);
            if protect {
                config =
                    config.with_protected_files(vec![secret_file.clone(), config_file.clone()]);
            }
            let runner = TaskRunner::new(
                uuid::Uuid::new_v4(),
                WorkerCapabilities::new("feat3", 4, 8192, false, false, None),
                config,
            );
            let task = rig.task(
                r#"cat "$T_A" 2>&1; echo; cat "$T_B" 2>&1; echo; cat "$T_C" 2>&1; echo END"#,
                &[
                    ("T_A", secret_file.to_str().unwrap()),
                    ("T_B", config_file.to_str().unwrap()),
                    (
                        "T_C",
                        toolchain.join("deep/neighbour.txt").to_str().unwrap(),
                    ),
                ],
                60,
            );
            async move {
                let result = runner.execute_task(&task, None, None).await;
                drop(rig);
                result
            }
        };
        // Unprotected, the policy itself lets the task read all of it (so the cut is the cause).
        let open = run(IsolationLevel::Fs, false).await;
        for text in [
            "DUMMY-SECRETFILE-feat3b",
            "DUMMY-CONFIG-feat3b",
            "DUMMY-NEIGHBOUR-feat3b",
        ] {
            assert!(open.stdout_str().contains(text), "{}", open.stdout_str());
        }
        for level in LEVELS {
            let result = run(level, true).await;
            let out = result.stdout_str();
            assert!(
                !out.contains("DUMMY-SECRETFILE-feat3b") && !out.contains("DUMMY-CONFIG-feat3b"),
                "{level}: {out}"
            );
            assert!(out.contains("Permission denied"), "{level}: {out}");
            assert!(
                out.contains("DUMMY-NEIGHBOUR-feat3b"),
                "{level}: the rest of the directory stays readable: {out}"
            );
        }
    }

    /// G2: `cargo build --offline` of a tiny crate works with the toolchain read-only.
    #[tokio::test]
    async fn test_cargo_build_offline_works_under_fs() {
        require_landlock!();
        let cargo = HostDirs::detect().cargo_home.join("bin/cargo");
        if !cargo.exists() {
            eprintln!("skipped: no rustup cargo proxy at {}", cargo.display());
            return;
        }
        for level in LEVELS {
            let rig = Rig::new(level, false);
            let path = format!(
                "{}:/usr/bin:/bin",
                rig.host.cargo_home.join("bin").to_string_lossy()
            );
            let task = rig.task(
                r#"
set -e
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR
mkdir -p tiny/src
printf '[package]\nname = "tiny"\nversion = "0.1.0"\nedition = "2021"\n' > tiny/Cargo.toml
echo 'fn main() { println!("tiny-ran"); }' > tiny/src/main.rs
cd tiny
cargo build --offline 2>&1
./target/debug/tiny
"#,
                &[("PATH", &path)],
                300,
            );
            let result = rig.runner.execute_task(&task, None, None).await;
            assert!(
                result.is_success(),
                "{level}: {:?}\n{}\n{}",
                result.error,
                result.stdout_str(),
                result.stderr_str()
            );
            assert!(
                result.stdout_str().contains("tiny-ran"),
                "{}",
                result.stdout_str()
            );
        }
    }

    /// A dependency from the cached registry is compiled from `$CARGO_HOME/registry/src`, which
    /// the policy only lets the task read.
    #[tokio::test]
    async fn test_cargo_build_offline_with_a_cached_dependency_under_fs() {
        require_landlock!();
        let real = HostDirs::detect();
        let cargo = real.cargo_home.join("bin/cargo");
        // A leaf crate (no required dependencies) whose source is in the local registry.
        let cached = std::fs::read_dir(real.cargo_home.join("registry/cache"))
            .into_iter()
            .flatten()
            .flatten()
            .flat_map(|index| {
                std::fs::read_dir(index.path())
                    .into_iter()
                    .flatten()
                    .flatten()
            })
            .filter_map(|f| f.file_name().to_str().map(str::to_string))
            .filter_map(|f| f.strip_suffix(".crate").map(str::to_string))
            .find_map(|stem| {
                let (name, version) = stem.rsplit_once('-')?;
                ["cfg-if", "itoa", "bytes", "memchr", "smallvec", "once_cell"]
                    .contains(&name)
                    .then(|| (name.to_string(), version.to_string()))
            });
        let Some((name, version)) = cached else {
            eprintln!(
                "skipped: no cached leaf crate in {}",
                real.cargo_home.display()
            );
            return;
        };
        if !cargo.exists() {
            eprintln!("skipped: no rustup cargo proxy at {}", cargo.display());
            return;
        }
        let rig = Rig::new(IsolationLevel::Fs, false);
        let path = format!(
            "{}:/usr/bin:/bin",
            rig.host.cargo_home.join("bin").to_string_lossy()
        );
        let script = format!(
            r#"
set -e
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR
mkdir -p dep/src
printf '[package]\nname = "dep"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\n{name} = "={version}"\n' > dep/Cargo.toml
echo 'fn main() {{ println!("dep-ran"); }}' > dep/src/main.rs
cd dep
cargo build --offline 2>&1
./target/debug/dep
"#
        );
        let result = rig
            .runner
            .execute_task(&rig.task(&script, &[("PATH", &path)], 300), None, None)
            .await;
        assert!(
            result.is_success(),
            "{name} {version}: {:?}\n{}\n{}",
            result.error,
            result.stdout_str(),
            result.stderr_str()
        );
        assert!(result.stdout_str().contains("dep-ran"));
    }

    /// The task leader is the tracked PID (the hook runs in it, there is no wrapper), and
    /// cancellation and the timeout still take the whole tree down, with the leader inside its
    /// user and network namespaces too.
    #[tokio::test]
    async fn test_tracked_pid_is_the_task_and_cancel_and_timeout_still_kill_it() {
        require_landlock!();
        fn comm_of(pid: u32) -> String {
            std::fs::read_to_string(format!("/proc/{pid}/comm"))
                .unwrap_or_default()
                .trim()
                .to_string()
        }
        fn alive(pid: u32) -> bool {
            unsafe { libc::kill(pid as i32, 0) == 0 }
        }

        let rig = Arc::new(Rig::new(IsolationLevel::Fs, false));
        let pid = Arc::new(AtomicU32::new(0));
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<String>();
        let handle = {
            let (rig, pid) = (Arc::clone(&rig), Arc::clone(&pid));
            tokio::spawn(async move {
                let task = rig.task("exec sleep 120", &[], 120);
                rig.runner
                    .execute_task(&task, Some(pid), Some(cancel_rx))
                    .await
            })
        };
        let tracked = wait_for_pid(&pid).await;
        let mut name = comm_of(tracked);
        for _ in 0..100 {
            if name == "sleep" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            name = comm_of(tracked);
        }
        assert_eq!(name, "sleep", "the tracked PID must become the task itself");
        cancel_tx.send("test".into()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(20), handle)
            .await
            .expect("cancellation finishes the task")
            .unwrap();
        assert_eq!(result.exit_code, EXIT_CODE_CANCELLED, "{:?}", result.error);
        assert!(wait_dead(tracked).await, "the cancelled task is gone");
        assert!(!alive(tracked));

        // Timeout, with a grandchild that must go down with the group.
        for level in LEVELS {
            let rig = Rig::new(level, false);
            let pid = Arc::new(AtomicU32::new(0));
            let task = rig.task(
                "sleep 120 & echo $! > \"$RUSTY_GRID_SANDBOX_DIR/child.pid\"; wait",
                &[],
                2,
            );
            let started = std::time::Instant::now();
            let result = rig
                .runner
                .execute_task(&task, Some(Arc::clone(&pid)), None)
                .await;
            assert_eq!(
                result.exit_code, EXIT_CODE_TIMEOUT,
                "{level}: {:?}",
                result.error
            );
            assert!(started.elapsed() < Duration::from_secs(30));
            let tracked = pid.load(Ordering::Acquire);
            assert!(tracked > 1);
            assert!(
                wait_dead(tracked).await,
                "{level}: the timed-out task is gone"
            );
        }
    }

    async fn wait_for_pid(pid: &AtomicU32) -> u32 {
        for _ in 0..200 {
            let v = pid.load(Ordering::Acquire);
            if v > 1 {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the runner never reported a child PID");
    }

    async fn wait_dead(pid: u32) -> bool {
        for _ in 0..100 {
            if unsafe { libc::kill(pid as i32, 0) } != 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_self_check_accepts_both_levels_on_linux() {
        require_landlock!();
        let base = tempfile::tempdir().unwrap();
        for level in [
            IsolationLevel::Off,
            IsolationLevel::Fs,
            IsolationLevel::FsNet,
        ] {
            verify_isolation_support(level, base.path())
                .await
                .unwrap_or_else(|e| panic!("{level}: {e}"));
        }
        let leftovers: Vec<_> = std::fs::read_dir(base.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "the probe cleans up after itself");
    }

    #[tokio::test]
    async fn test_a_missing_program_fails_the_task_with_a_clear_message() {
        require_landlock!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = Task::new(
            TaskSpec::command("no-such-program-feat3", vec![]),
            TaskRequirements::generic(1, 30),
        );
        let result = rig.runner.execute_task(&task, None, None).await;
        assert!(!result.is_success());
        assert!(
            result.stderr_str().contains("no-such-program-feat3")
                && result.stderr_str().contains("not found"),
            "{}",
            result.stderr_str()
        );
    }
}

/// Where there is no implementation yet (FEAT-3c: Windows) isolation is refused at start-up, and
/// a task is refused instead of running unsandboxed.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod unimplemented_platform {
    use super::*;

    #[tokio::test]
    async fn test_self_check_says_not_implemented_yet() {
        let base = tempfile::tempdir().unwrap();
        verify_isolation_support(IsolationLevel::Off, base.path())
            .await
            .expect("off needs nothing");
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let err = verify_isolation_support(level, base.path())
                .await
                .expect_err("no implementation on this platform");
            let text = err.to_string();
            assert!(
                text.contains("not implemented yet on this platform"),
                "{text}"
            );
            assert!(text.contains("FEAT-3c"), "{text}");
        }
    }

    #[tokio::test]
    async fn test_a_task_is_refused_rather_than_run_unsandboxed() {
        let rig = Rig::new(IsolationLevel::Fs, false);
        let marker = rig.home.path().join("ran.txt");
        let result = rig.run(&format!("echo ran > '{}'", marker.display())).await;
        assert!(!result.is_success());
        assert!(
            result.stderr_str().contains("not implemented yet"),
            "{}",
            result.stderr_str()
        );
        assert!(!marker.exists(), "the task must not have run at all");
    }
}
