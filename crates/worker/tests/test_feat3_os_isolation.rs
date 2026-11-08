//! FEAT-3 (design 0033): OS-level task isolation, `--isolation off|fs|fs+net`.
//!
//! FEAT-3a ships the macOS implementation (`sandbox-exec`); the tests that exercise it are
//! `#[cfg(target_os = "macos")]`. On Linux and Windows (FEAT-3b / FEAT-3c are still to come) the
//! tests below check the other half of the contract: a worker asked for isolation it cannot
//! enforce refuses to start and never runs a task unsandboxed.
//!
//! Every test uses a temporary `HOME` (with a dummy `.oxideswarm/cluster.secret` and
//! `.ssh/test_key`), its own sandbox base and loopback ports; nothing touches the developer's
//! real `~/.oxideswarm`. The real Rust toolchain is only read.

#![cfg_attr(
    not(any(target_os = "macos", windows)),
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

    #[cfg(not(windows))]
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

    #[cfg(not(windows))]
    async fn run(&self, script: &str) -> TaskResult {
        self.runner
            .execute_task(&self.task(script, &[], 60), None, None)
            .await
    }
}

fn lines(result: &TaskResult) -> Vec<String> {
    result.stdout_str().lines().map(str::to_string).collect()
}

#[cfg(not(windows))]
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

#[cfg(any(target_os = "macos", windows))]
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

#[cfg(windows)]
mod windows {
    use super::*;

    fn win_task(rig: &Rig, script: &str, extra_env: &[(&str, &str)], timeout_secs: u64) -> Task {
        let mut env = rig.env();
        for (k, v) in extra_env {
            env.insert((*k).into(), (*v).into());
        }
        Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe".into(),
                args: vec![
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-Command".into(),
                    script.into(),
                ],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, timeout_secs),
        )
    }

    fn win_cmd_task(
        rig: &Rig,
        script: &str,
        extra_env: &[(&str, &str)],
        timeout_secs: u64,
    ) -> Task {
        let mut env = rig.env();
        for (k, v) in extra_env {
            env.insert((*k).into(), (*v).into());
        }
        Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), script.into()],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, timeout_secs),
        )
    }

    const WIN_FS_SCRIPT: &str = r#"
Set-Content -LiteralPath "$env:RUSTY_GRID_SANDBOX_DIR\allowed.txt" -Value 'hello' -NoNewline
Write-Output 'WRITE_OK'
if ((Get-Content -LiteralPath "$env:RUSTY_GRID_SANDBOX_DIR\allowed.txt" -Raw) -eq 'hello') { Write-Output 'READ_OK' }
New-Item -ItemType Directory -Path "$env:RUSTY_GRID_SANDBOX_DIR\sub" -Force | Out-Null
Write-Output 'MKDIR_OK'
'x' > $null
Write-Output 'DEVNULL_OK'
try { Get-Content -LiteralPath $env:T_SECRET -ErrorAction Stop | Out-Null; Write-Output 'LEAK_SECRET' } catch { Write-Output 'DENY_SECRET' }
try { Get-Content -LiteralPath $env:T_KEY -ErrorAction Stop | Out-Null; Write-Output 'LEAK_KEY' } catch { Write-Output 'DENY_KEY' }
try { Get-Content -LiteralPath $env:T_SIBLING -ErrorAction Stop | Out-Null; Write-Output 'LEAK_SIBLING' } catch { Write-Output 'DENY_SIBLING' }
try { Get-ChildItem -LiteralPath $env:T_BASE -ErrorAction Stop | Out-Null; Write-Output 'LEAK_LISTING' } catch { Write-Output 'DENY_LISTING' }
try { Get-ChildItem -LiteralPath $env:T_HOME -ErrorAction Stop | Out-Null; Write-Output 'LEAK_HOME_LISTING' } catch { Write-Output 'DENY_HOME_LISTING' }
try { Set-Content -LiteralPath "$env:T_HOME\planted.txt" -Value 'x' -ErrorAction Stop; Write-Output 'LEAK_WRITE_HOME' } catch { Write-Output 'DENY_WRITE_HOME' }
try { Set-Content -LiteralPath "$env:T_BASE\other_task\planted.txt" -Value 'x' -ErrorAction Stop; Write-Output 'LEAK_WRITE_SIBLING' } catch { Write-Output 'DENY_WRITE_SIBLING' }
try { Set-Content -LiteralPath "$env:T_OUTSIDE\feat3_outside_marker" -Value 'x' -ErrorAction Stop; Write-Output 'LEAK_WRITE_TMP' } catch { Write-Output 'DENY_WRITE_TMP' }
"#;

    fn run_win_fs(rig: &Rig) -> Task {
        let outside = rig._outside.path().to_string_lossy().into_owned();
        win_task(rig, WIN_FS_SCRIPT, &[("T_OUTSIDE", &outside)], 60)
    }

    #[tokio::test]
    async fn test_off_leaves_the_owners_files_readable_as_a_control() {
        let rig = Rig::new(IsolationLevel::Off, false);
        let result = rig.runner.execute_task(&run_win_fs(&rig), None, None).await;
        let got = lines(&result);
        for leak in ["LEAK_SECRET", "LEAK_KEY", "LEAK_SIBLING", "LEAK_WRITE_HOME"] {
            assert!(
                got.iter().any(|l| l == leak),
                "without isolation {leak} must succeed (otherwise the tests below prove nothing): {got:?} / stderr={}",
                result.stderr_str()
            );
        }
    }

    #[tokio::test]
    async fn test_fs_confines_a_task_to_its_sandbox() {
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig.runner.execute_task(&run_win_fs(&rig), None, None).await;
            assert_fs_policy_holds(&result, &level.to_string());
            assert!(
                !rig.home.path().join("planted.txt").exists()
                    && !rig.base.join("other_task/planted.txt").exists()
                    && !rig._outside.path().join("feat3_outside_marker").exists(),
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
        let rig = Rig::new(IsolationLevel::Fs, true);
        let result = rig.runner.execute_task(&run_win_fs(&rig), None, None).await;
        assert_fs_policy_holds(&result, "base inside home");
    }

    #[tokio::test]
    async fn test_concurrent_tasks_under_fs_cannot_read_or_list_each_others_sandboxes() {
        let rig = Arc::new(Rig::new(IsolationLevel::Fs, false));
        let script = r#"
Set-Content -LiteralPath "$env:RUSTY_GRID_SANDBOX_DIR\secret.txt" -Value 'secret-from-t1'
Start-Sleep -Milliseconds 400
try { Get-Content -LiteralPath "$env:T_SIB_SANDBOX\secret.txt" -ErrorAction Stop | Out-Null; Write-Output 'LEAK_SIBLING_READ' } catch { Write-Output 'DENY_SIBLING_READ' }
try { Get-ChildItem -LiteralPath $env:T_SIB_SANDBOX -ErrorAction Stop | Out-Null; Write-Output 'LEAK_SIBLING_LIST' } catch { Write-Output 'DENY_SIBLING_LIST' }
try { Set-Content -LiteralPath "$env:T_SIB_SANDBOX\planted.txt" -Value 'x' -ErrorAction Stop; Write-Output 'LEAK_SIBLING_WRITE' } catch { Write-Output 'DENY_SIBLING_WRITE' }
Start-Sleep -Milliseconds 400
"#;
        let task1 = win_task(&rig, script, &[], 60);
        let mut task2 = task1.clone();
        task2.id = rusty_grid_core::TaskId::new();

        let sb1 = rig.base.join(task1.id.0.to_string());
        let sb2 = rig.base.join(task2.id.0.to_string());

        let mut t1 = task1;
        let mut t2 = task2;
        if let TaskSpec::Command { ref mut env, .. } = t1.spec {
            env.insert("T_SIB_SANDBOX".into(), sb2.to_string_lossy().into_owned());
        }
        if let TaskSpec::Command { ref mut env, .. } = t2.spec {
            env.insert("T_SIB_SANDBOX".into(), sb1.to_string_lossy().into_owned());
        }

        let r1 = Arc::clone(&rig);
        let r2 = Arc::clone(&rig);
        let (res1, res2) = tokio::join!(
            async move { r1.runner.execute_task(&t1, None, None).await },
            async move { r2.runner.execute_task(&t2, None, None).await },
        );

        for (label, res) in [("task1", &res1), ("task2", &res2)] {
            assert!(
                res.is_success(),
                "{label} failed: {:?} / {}",
                res.error,
                res.stderr_str()
            );
            let got = lines(res);
            for expected in [
                "DENY_SIBLING_READ",
                "DENY_SIBLING_LIST",
                "DENY_SIBLING_WRITE",
            ] {
                assert!(
                    got.iter().any(|l| l == expected),
                    "{label}: missing {expected}; got {got:?}"
                );
            }
            assert!(
                !got.iter().any(|l| l.starts_with("LEAK_")),
                "{label}: sibling sandbox access leaked: {got:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_environment_is_moved_into_the_sandbox_and_ipc_sockets_are_removed() {
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = win_task(
            &rig,
            r#"
Write-Output "HOME=$env:HOME"
Write-Output "USERPROFILE=$env:USERPROFILE"
Write-Output "TMPDIR=$env:TMPDIR"
Write-Output "TEMP=$env:TEMP"
Write-Output "TMP=$env:TMP"
Write-Output "SANDBOX=$env:RUSTY_GRID_SANDBOX_DIR"
Write-Output "RUSTUP_HOME=$env:RUSTUP_HOME"
Write-Output "CARGO_HOME=$env:CARGO_HOME"
Write-Output ("SSH=" + $(if ($null -ne $env:SSH_AUTH_SOCK) { $env:SSH_AUTH_SOCK } else { 'unset' }))
Write-Output ("GPG=" + $(if ($null -ne $env:GPG_AGENT_INFO) { $env:GPG_AGENT_INFO } else { 'unset' }))
Write-Output ("DBUS=" + $(if ($null -ne $env:DBUS_SESSION_BUS_ADDRESS) { $env:DBUS_SESSION_BUS_ADDRESS } else { 'unset' }))
Write-Output ("DOCKER=" + $(if ($null -ne $env:DOCKER_HOST) { $env:DOCKER_HOST } else { 'unset' }))
Write-Output ("SECRET=" + $(if ($null -ne $env:OXIDESWARM_SECRET) { $env:OXIDESWARM_SECRET } else { 'unset' }))
Write-Output ("TASK_ID=" + $(if ($null -ne $env:RUSTY_GRID_TASK_ID) { $env:RUSTY_GRID_TASK_ID } else { 'unset' }))
Write-Output ("RUN_TOKEN=" + $(if ($null -ne $env:RUSTY_GRID_RUN_TOKEN) { $env:RUSTY_GRID_RUN_TOKEN } else { 'unset' }))
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
        for name in ["HOME", "USERPROFILE", "TMPDIR"] {
            assert_eq!(got[name], sandbox, "{name}");
        }
        // On Windows, `CreateProcessW` (`KernelBase!BasepAppContainerEnvironmentExtension`)
        // rewrites `TEMP` and `TMP` into the per-attempt AppContainer folder
        // `...\Packages\oxideswarm.task.<run_token>\AC\Temp`.
        assert_ne!(got["RUN_TOKEN"], "unset");
        let task_moniker = format!("oxideswarm.task.{}", got["RUN_TOKEN"]);
        for name in ["TEMP", "TMP"] {
            let val = &got[name];
            assert!(
                val == &sandbox || val.contains(&task_moniker),
                "{name} must be sandbox or per-task AppContainer temp dir, got {val}"
            );
        }
        assert_eq!(got["RUSTUP_HOME"], rig.host.rustup_home.to_string_lossy());
        assert_eq!(got["CARGO_HOME"], rig.host.cargo_home.to_string_lossy());
        assert_eq!(
            got["TASK_ID"],
            task.id.to_string(),
            "worker task diagnostic vars must survive environment preparation"
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
        let tcp_script = r#"
try {
    $c = New-Object System.Net.Sockets.TcpClient
    $c.Connect('127.0.0.1', [int]$env:T_PORT)
    $c.Close()
    Write-Output 'CONNECTED'
} catch {
    Write-Output 'REFUSED'
}
"#;
        // Probes outbound TCP via WFP: under `fs` (`WinCapabilityInternetClientSid` +
        // `WinCapabilityPrivateNetworkClientServerSid`), WFP permits the outbound SYN to
        // RFC 5737 TEST-NET-1 (`192.0.2.1:80`), whereas under `fs+net` (zero capabilities),
        // WFP immediately rejects `Connect` with `WSAEACCES` (`10013` = `AccessDenied`).
        let wfp_tcp_script = r#"
try {
    $c = New-Object System.Net.Sockets.TcpClient
    $ar = $c.BeginConnect('192.0.2.1', 80, $null, $null)
    if ($ar.AsyncWaitHandle.WaitOne(150, $false)) {
        $c.EndConnect($ar)
    }
    $c.Close()
    Write-Output 'NET_ALLOWED'
} catch [System.Net.Sockets.SocketException] {
    if ($_.Exception.NativeErrorCode -eq 10013) {
        Write-Output 'NET_REFUSED'
    } else {
        Write-Output 'NET_ALLOWED'
    }
} catch {
    if ($_.Exception.InnerException -is [System.Net.Sockets.SocketException] -and $_.Exception.InnerException.NativeErrorCode -eq 10013) {
        Write-Output 'NET_REFUSED'
    } else {
        Write-Output 'NET_ALLOWED'
    }
}
"#;

        // Control 1 (`off`): loopback TCP connect reaches the listener.
        let rig = Rig::new(IsolationLevel::Off, false);
        let result = rig
            .runner
            .execute_task(
                &win_task(&rig, tcp_script, &[("T_PORT", &port)], 60),
                None,
                None,
            )
            .await;
        let accepted_in_off = listener.accept().is_ok();
        assert!(
            accepted_in_off && lines(&result).contains(&"CONNECTED".to_string()),
            "control under off must reach listener: {:?} / {}",
            lines(&result),
            result.stderr_str()
        );
        while listener.accept().is_ok() {}

        // Control 2 (`fs`): `WinCapabilityInternetClientSid` and
        // `WinCapabilityPrivateNetworkClientServerSid` allow outbound TCP in WFP.
        let rig = Rig::new(IsolationLevel::Fs, false);
        let result = rig
            .runner
            .execute_task(&win_task(&rig, wfp_tcp_script, &[], 60), None, None)
            .await;
        assert!(
            lines(&result).contains(&"NET_ALLOWED".to_string()),
            "control under fs must allow outbound TCP in WFP: {:?} / {}",
            lines(&result),
            result.stderr_str()
        );

        // Under `fs+net` (zero capabilities): both outbound TCP and loopback TCP are refused
        // by WFP (`WSAEACCES` = 10013), and the listener accepts no connection.
        let rig = Rig::new(IsolationLevel::FsNet, false);
        let wfp_res = rig
            .runner
            .execute_task(&win_task(&rig, wfp_tcp_script, &[], 60), None, None)
            .await;
        assert!(
            lines(&wfp_res).contains(&"NET_REFUSED".to_string()),
            "{:?} {}",
            lines(&wfp_res),
            wfp_res.stderr_str()
        );
        let result = rig
            .runner
            .execute_task(
                &win_task(&rig, tcp_script, &[("T_PORT", &port)], 60),
                None,
                None,
            )
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

    #[tokio::test]
    async fn test_cargo_build_offline_works_under_fs() {
        let cargo = HostDirs::detect().cargo_home.join("bin/cargo.exe");
        if !cargo.exists() {
            eprintln!("skipped: no rustup cargo proxy at {}", cargo.display());
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let sys_path = std::env::var("PATH").unwrap_or_default();
            let path = format!(
                "{};{sys_path}",
                rig.host.cargo_home.join("bin").to_string_lossy()
            );
            let task = win_cmd_task(
                &rig,
                r#"set "CARGO_TARGET_DIR=" & set "CARGO_BUILD_TARGET_DIR=" & mkdir tiny\src & (echo [package]& echo name = "tiny"& echo version = "0.1.0"& echo edition = "2021") > tiny\Cargo.toml & (echo fn main^(^) { println!^("tiny-ran"^); }) > tiny\src\main.rs & cd tiny & cargo.exe build --offline 2>&1 & target\debug\tiny.exe"#,
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

    #[tokio::test]
    async fn test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home() {
        let real = HostDirs::detect();
        let cargo = real.cargo_home.join("bin/cargo.exe");
        let Some(home) = real.cargo_home.parent().map(Path::to_path_buf) else {
            return;
        };
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
        };
        let rig = Rig::new(IsolationLevel::Fs, false).with_policy_home(IsolationLevel::Fs, home);
        let sys_path = std::env::var("PATH").unwrap_or_default();
        let path = format!(
            "{};{sys_path}",
            rig.host.cargo_home.join("bin").to_string_lossy()
        );
        let script = format!(
            r#"set "CARGO_TARGET_DIR=" & set "CARGO_BUILD_TARGET_DIR=" & mkdir dep\src & (echo [package]& echo name = "dep"& echo version = "0.1.0"& echo edition = "2021"& echo [dependencies]& echo {name} = "={version}") > dep\Cargo.toml & (echo fn main^(^) {{ println!^("dep-ran"^); }}) > dep\src\main.rs & cd dep & cargo.exe build --offline 2>&1 & target\debug\dep.exe"#
        );
        let result = rig
            .runner
            .execute_task(
                &win_cmd_task(&rig, &script, &[("PATH", &path)], 300),
                None,
                None,
            )
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

    #[tokio::test]
    async fn test_tracked_pid_is_the_task_and_cancel_and_timeout_still_kill_it() {
        let rig = Arc::new(Rig::new(IsolationLevel::Fs, false));

        // Cancellation.
        let pid = Arc::new(AtomicU32::new(0));
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<String>();
        let handle = {
            let (rig, pid) = (Arc::clone(&rig), Arc::clone(&pid));
            tokio::spawn(async move {
                let task = win_task(&rig, "Start-Sleep -Seconds 120", &[], 120);
                rig.runner
                    .execute_task(&task, Some(pid), Some(cancel_rx))
                    .await
            })
        };
        let tracked = wait_for_pid(&pid).await;
        assert!(tracked > 1);
        cancel_tx.send("test".into()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(20), handle)
            .await
            .expect("cancellation finishes the task")
            .unwrap();
        assert_eq!(result.exit_code, EXIT_CODE_CANCELLED, "{:?}", result.error);

        // Timeout.
        let pid = Arc::new(AtomicU32::new(0));
        let task = win_task(&rig, "Start-Sleep -Seconds 120", &[], 2);
        let started = std::time::Instant::now();
        let result = rig
            .runner
            .execute_task(&task, Some(Arc::clone(&pid)), None)
            .await;
        assert_eq!(result.exit_code, EXIT_CODE_TIMEOUT, "{:?}", result.error);
        assert!(started.elapsed() < Duration::from_secs(30));
        let tracked = pid.load(Ordering::Acquire);
        assert!(tracked > 1);
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

    #[tokio::test]
    async fn test_self_check_accepts_both_levels_on_windows() {
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

    #[tokio::test]
    async fn test_worker_only_environment_variables_do_not_reach_a_confined_task() {
        assert!(std::env::var_os("CARGO_PKG_NAME").is_some());
        let script = r#"
$w = if ($env:CARGO_PKG_NAME) { $env:CARGO_PKG_NAME } else { "unset" }
$p = if ($env:PATH) { "yes" } else { "no" }
$t = if ($env:T_BASE) { "yes" } else { "no" }
$i = if ($env:RUSTY_GRID_TASK_ID) { "yes" } else { "no" }
Write-Output "WORKER_VAR=$w"
Write-Output "PATH_SET=$p"
Write-Output "TASK_VAR=$t"
Write-Output "ID=$i"
"#;
        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(&win_task(&control_rig, script, &[], 60), None, None)
            .await;
        assert!(
            lines(&control).contains(&"WORKER_VAR=rusty_grid_worker".to_string()),
            "{:?}",
            lines(&control)
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_task(&rig, script, &[], 60), None, None)
                .await;
            let got = lines(&result);
            for expected in ["WORKER_VAR=unset", "PATH_SET=yes", "TASK_VAR=yes", "ID=yes"] {
                assert!(
                    got.contains(&expected.to_string()),
                    "{level}: {expected} in {got:?}"
                );
            }
        }
    }

    /// M2 on Windows: host named pipes (such as `\\.\pipe\openssh-ssh-agent` or
    /// `\\.\pipe\docker_engine`) are isolated by the per-task AppContainer named-object directory
    /// (`\Sessions\<id>\AppContainerNamedObjects\<TaskAppContainerSid>`).
    #[tokio::test]
    async fn test_host_named_pipes_cannot_be_connected_to_under_fs() {
        let pipe_name = format!(
            "oxideswarm-feat3-pipe-{}-{}",
            std::process::id(),
            rusty_grid_core::TaskId::new().0
        );
        let script = r#"
$server = New-Object System.IO.Pipes.NamedPipeServerStream($env:T_PIPE, [System.IO.Pipes.PipeDirection]::InOut, 4)
try {
    $client = New-Object System.IO.Pipes.NamedPipeClientStream('.', $env:T_PIPE, [System.IO.Pipes.PipeDirection]::InOut)
    $client.Connect(1500)
    $client.Dispose()
    Write-Output 'PIPE_CONNECTED'
} catch {
    Write-Output 'PIPE_REFUSED'
} finally {
    $server.Dispose()
}
"#;
        // First verify that a host named pipe created outside the AppContainer is reachable
        // under `off` and unreachable under `fs` and `fs+net`.
        let host_pipe_script = r#"
try {
    $client = New-Object System.IO.Pipes.NamedPipeClientStream('.', $env:T_PIPE, [System.IO.Pipes.PipeDirection]::InOut)
    $client.Connect(1500)
    $client.Dispose()
    Write-Output 'HOST_PIPE_CONNECTED'
} catch {
    Write-Output 'HOST_PIPE_REFUSED'
}
foreach ($wellKnown in @('openssh-ssh-agent', 'docker_engine')) {
    try {
        $wk = New-Object System.IO.Pipes.NamedPipeClientStream('.', $wellKnown, [System.IO.Pipes.PipeDirection]::InOut)
        $wk.Connect(200)
        $wk.Dispose()
        Write-Output "WK_CONNECTED=$wellKnown"
    } catch {
        Write-Output "WK_REFUSED=$wellKnown"
    }
}
"#;
        let mut host_server = std::process::Command::new(
            r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
        )
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!(
                "$s1 = New-Object System.IO.Pipes.NamedPipeServerStream('{pipe_name}', [System.IO.Pipes.PipeDirection]::InOut, 4); \
                 $s2 = New-Object System.IO.Pipes.NamedPipeServerStream('{pipe_name}', [System.IO.Pipes.PipeDirection]::InOut, 4); \
                 $s3 = New-Object System.IO.Pipes.NamedPipeServerStream('{pipe_name}', [System.IO.Pipes.PipeDirection]::InOut, 4); \
                 Write-Output 'READY'; Start-Sleep -Seconds 30"
            ),
        ])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn host pipe server");

        // Wait for the host pipe instances to be created.
        tokio::time::sleep(Duration::from_millis(800)).await;

        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(
                &win_task(
                    &control_rig,
                    host_pipe_script,
                    &[("T_PIPE", &pipe_name)],
                    60,
                ),
                None,
                None,
            )
            .await;
        let control_ok = lines(&control).contains(&"HOST_PIPE_CONNECTED".to_string());

        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(
                    &win_task(&rig, host_pipe_script, &[("T_PIPE", &pipe_name)], 60),
                    None,
                    None,
                )
                .await;
            assert!(
                lines(&result).contains(&"HOST_PIPE_REFUSED".to_string())
                    && lines(&result).contains(&"WK_REFUSED=openssh-ssh-agent".to_string())
                    && lines(&result).contains(&"WK_REFUSED=docker_engine".to_string()),
                "{level}: host named pipes should be unreachable from AppContainer: {:?}",
                lines(&result)
            );
            // Also verify `\\.\pipe\...` creation/connect inside AppContainer is blocked by
            // default named-pipe ACLs.
            let self_res = rig
                .runner
                .execute_task(
                    &win_task(
                        &rig,
                        script,
                        &[("T_PIPE", &format!("{pipe_name}-inner"))],
                        60,
                    ),
                    None,
                    None,
                )
                .await;
            assert!(
                !lines(&self_res).contains(&"HOST_PIPE_CONNECTED".to_string()),
                "{level}: {:?}",
                lines(&self_res)
            );
        }

        let _ = host_server.kill();
        let _ = host_server.wait();
        assert!(
            control_ok,
            "unconfined control must connect to host named pipe: {:?}",
            lines(&control)
        );
    }

    /// M5 + worker process environment and memory protection on Windows: an AppContainer task
    /// runs at Low Mandatory Integrity Level and cannot terminate, suspend, open
    /// `PROCESS_VM_READ` / `PROCESS_QUERY_INFORMATION` handles, call `ReadProcessMemory` on the
    /// worker's memory, or inject threads via `CreateRemoteThread` into Medium-IL host processes
    /// (the worker or foreign processes), while still being able to terminate its own child
    /// processes.
    #[tokio::test]
    async fn test_a_task_cannot_signal_or_inspect_foreign_processes() {
        let mut victim = std::process::Command::new(r"C:\Windows\System32\cmd.exe")
            .args(["/C", "ping -n 120 127.0.0.1 >nul"])
            .spawn()
            .expect("spawn victim");
        let victim_pid = victim.id().to_string();
        let worker_pid = std::process::id().to_string();
        let secret_bytes: Vec<u8> = b"WORKER-MEM-SECRET-feat3c".to_vec();
        let secret_addr = (secret_bytes.as_ptr() as usize).to_string();
        let secret_len = secret_bytes.len().to_string();

        let script = r#"
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public static class WinProcProbe {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr OpenProcess(uint dwDesiredAccess, bool bInheritHandle, int dwProcessId);
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool ReadProcessMemory(IntPtr hProcess, IntPtr lpBaseAddress, byte[] lpBuffer, int dwSize, out IntPtr lpNumberOfBytesRead);
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr CreateRemoteThread(IntPtr hProcess, IntPtr lpThreadAttributes, uint dwStackSize, IntPtr lpStartAddress, IntPtr lpParameter, uint dwCreationFlags, out uint lpThreadId);
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool CloseHandle(IntPtr hObject);

    public static string TryReadWorkerMemory(int pid, ulong addr, int len) {
        const uint PROCESS_VM_READ = 0x0010;
        const uint PROCESS_QUERY_INFORMATION = 0x0400;
        IntPtr h = OpenProcess(PROCESS_VM_READ | PROCESS_QUERY_INFORMATION, false, pid);
        if (h == IntPtr.Zero) return "RPM_OPEN_DENIED";
        try {
            byte[] buf = new byte[len];
            IntPtr read;
            if (!ReadProcessMemory(h, (IntPtr)addr, buf, len, out read)) return "RPM_READ_DENIED";
            return "RPM_LEAK=" + Encoding.UTF8.GetString(buf, 0, (int)read);
        } finally {
            CloseHandle(h);
        }
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern IntPtr GetModuleHandle(string lpModuleName);
    [DllImport("kernel32.dll", CharSet = CharSet.Ansi, ExactSpelling = true, SetLastError = true)]
    public static extern IntPtr GetProcAddress(IntPtr hModule, string procName);

    public static string TryCreateRemoteThread(int pid) {
        const uint PROCESS_CREATE_THREAD = 0x0002;
        const uint PROCESS_VM_OPERATION = 0x0008;
        const uint PROCESS_VM_WRITE = 0x0020;
        IntPtr h = OpenProcess(PROCESS_CREATE_THREAD | PROCESS_VM_OPERATION | PROCESS_VM_WRITE, false, pid);
        if (h == IntPtr.Zero) return "CRT_OPEN_DENIED";
        try {
            IntPtr k32 = GetModuleHandle("kernel32.dll");
            IntPtr fnAddr = GetProcAddress(k32, "GetCurrentProcessId");
            uint tid;
            IntPtr th = CreateRemoteThread(h, IntPtr.Zero, 0, fnAddr, IntPtr.Zero, 0, out tid);
            if (th == IntPtr.Zero) return "CRT_CALL_DENIED";
            CloseHandle(th);
            return "CRT_CREATED";
        } finally {
            CloseHandle(h);
        }
    }
}
"@
Write-Output ([WinProcProbe]::TryReadWorkerMemory([int]$env:WORKER_PID, [uint64]$env:SECRET_ADDR, [int]$env:SECRET_LEN))
Write-Output ("WORKER_" + [WinProcProbe]::TryCreateRemoteThread([int]$env:WORKER_PID))
Write-Output ("VICTIM_" + [WinProcProbe]::TryCreateRemoteThread([int]$env:VICTIM_PID))
try {
    $w = [System.Diagnostics.Process]::GetProcessById([int]$env:WORKER_PID)
    $h = $w.Handle
    Write-Output 'WORKER_HANDLE_OPENED'
} catch {
    Write-Output 'WORKER_HANDLE_DENIED'
}
try {
    Stop-Process -Id ([int]$env:VICTIM_PID) -Force -ErrorAction Stop
    Write-Output 'SIGNALLED'
} catch {
    Write-Output 'DENIED'
}
try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo("$env:SystemRoot\System32\cmd.exe", '/C ping -n 60 127.0.0.1 >nul')
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $own = [System.Diagnostics.Process]::Start($psi)
    $own.Kill()
    $own.WaitForExit(5000) | Out-Null
    Write-Output 'OWN_CHILD_OK'
} catch {
    Write-Output 'OWN_CHILD_FAIL'
}
"#;
        let envs = [
            ("VICTIM_PID", victim_pid.as_str()),
            ("WORKER_PID", worker_pid.as_str()),
            ("SECRET_ADDR", secret_addr.as_str()),
            ("SECRET_LEN", secret_len.as_str()),
        ];
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_task(&rig, script, &envs, 60), None, None)
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"RPM_OPEN_DENIED".to_string())
                    && !result.stdout_str().contains("WORKER-MEM-SECRET-feat3c"),
                "{level}: AppContainer must not ReadProcessMemory on worker: {got:?}"
            );
            assert!(
                got.contains(&"WORKER_CRT_OPEN_DENIED".to_string())
                    && got.contains(&"VICTIM_CRT_OPEN_DENIED".to_string()),
                "{level}: AppContainer must not open PROCESS_CREATE_THREAD / CreateRemoteThread: {got:?}"
            );
            assert!(
                got.contains(&"WORKER_HANDLE_DENIED".to_string()),
                "{level}: AppContainer must not open worker process handle: {got:?}"
            );
            assert!(
                got.contains(&"DENIED".to_string()),
                "{level}: AppContainer must not terminate foreign process: {got:?}"
            );
            assert!(
                got.contains(&"OWN_CHILD_OK".to_string()),
                "{level}: AppContainer must still be able to terminate its own child: {got:?}"
            );
        }

        // Control (`off`): can ReadProcessMemory on worker, open worker handle, and terminate victim.
        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(&win_task(&control_rig, script, &envs, 60), None, None)
            .await;
        let _ = victim.kill();
        let _ = victim.wait();
        std::hint::black_box(&secret_bytes);
        let control_got = lines(&control);
        assert!(
            control_got.contains(&"RPM_LEAK=WORKER-MEM-SECRET-feat3c".to_string())
                && control_got.contains(&"WORKER_HANDLE_OPENED".to_string())
                && control_got.contains(&"SIGNALLED".to_string()),
            "unconfined control must ReadProcessMemory on worker and terminate victim: {control_got:?}"
        );
    }

    /// M4 on Windows: console / Windows Terminal keystroke injection (`TIOCSTI` equivalent via
    /// `AttachConsole(worker_pid)` + `WriteConsoleInputW` or inherited `CONIN$`) is blocked
    /// because AppContainer tasks are spawned with `CREATE_NO_WINDOW` and run at Low Mandatory
    /// Integrity Level.
    #[tokio::test]
    async fn test_console_and_terminal_injection_is_blocked() {
        let worker_pid = std::process::id().to_string();
        let script = r#"
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class WinConsoleProbe {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool FreeConsole();
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool AttachConsole(uint dwProcessId);

    public static string TryAttachWorkerConsole(uint workerPid) {
        FreeConsole();
        if (AttachConsole(workerPid)) {
            return "ATTACH_WORKER_OK";
        }
        if (AttachConsole(0xFFFFFFFFu)) {
            return "ATTACH_PARENT_OK";
        }
        return "ATTACH_DENIED";
    }
}
"@
Write-Output ([WinConsoleProbe]::TryAttachWorkerConsole([uint32]$env:WORKER_PID))
"#;
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(
                    &win_task(&rig, script, &[("WORKER_PID", &worker_pid)], 60),
                    None,
                    None,
                )
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"ATTACH_DENIED".to_string()),
                "{level}: AppContainer must not attach to worker/parent console: {got:?}"
            );
        }
    }

    /// H2 on Windows: the owner's `HKCU\Software\...` registry keys cannot be read or written
    /// by a confined AppContainer task.
    #[tokio::test]
    async fn test_registry_of_the_owner_is_neither_read_nor_written() {
        let pid = std::process::id();
        let read_key = format!(r"HKCU\Software\OxideSwarmFeat3Read_{pid}");
        let write_key = format!(r"HKCU\Software\OxideSwarmFeat3Write_{pid}");
        let reg = |args: &[&str]| {
            std::process::Command::new(r"C:\Windows\System32\reg.exe")
                .args(args)
                .output()
        };
        let cleanup = || {
            let _ = reg(&["delete", &read_key, "/f"]);
            let _ = reg(&["delete", &write_key, "/f"]);
        };
        if !reg(&[
            "add",
            &read_key,
            "/v",
            "secret",
            "/t",
            "REG_SZ",
            "/d",
            "owner-registry-value",
            "/f",
        ])
        .is_ok_and(|o| o.status.success())
        {
            eprintln!("skipped: cannot write test registry key");
            return;
        }

        let script = format!(
            r#"
$r = & "$env:SystemRoot\System32\reg.exe" query "{read_key}" /v secret 2>$null
Write-Output "READ=$($r -join ' ')"
& "$env:SystemRoot\System32\reg.exe" add "{write_key}" /v planted /t REG_SZ /d yes /f >$null 2>$null
Write-Output "WRITE_RC=$LASTEXITCODE"
"#
        );

        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(&win_task(&control_rig, &script, &[], 60), None, None)
            .await;
        let control_reads = control.stdout_str().contains("owner-registry-value");
        let _ = reg(&["delete", &write_key, "/f"]);
        if !control_reads {
            cleanup();
            eprintln!("skipped: registry key not readable even unconfined");
            return;
        }

        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_task(&rig, &script, &[], 60), None, None)
                .await;
            assert!(
                !result.stdout_str().contains("owner-registry-value"),
                "{level}: AppContainer read owner's HKCU registry secret: {}",
                result.stdout_str()
            );
            let planted =
                reg(&["query", &write_key, "/v", "planted"]).is_ok_and(|o| o.status.success());
            assert!(
                !planted,
                "{level}: AppContainer wrote into owner's HKCU registry"
            );
        }
        cleanup();
    }

    /// M3 on Windows: Window Station DACL for `ALL APPLICATION PACKAGES` omits
    /// `WINSTA_READCLIPBOARD` and `WINSTA_ACCESSCLIPBOARD`, and AppContainer isolation prevents
    /// reading the user's clipboard (`Get-Clipboard`).
    #[tokio::test]
    async fn test_clipboard_is_not_readable() {
        let script = r#"
try {
    $c = Get-Clipboard -ErrorAction Stop
    if ($null -ne $c -and "$c".Length -gt 0) {
        Write-Output "CLIP_LEAK=$c"
    } else {
        Write-Output 'CLIP_EMPTY'
    }
} catch {
    Write-Output 'CLIP_DENIED'
}
"#;
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_task(&rig, script, &[], 60), None, None)
                .await;
            assert!(
                !result.stdout_str().contains("CLIP_LEAK="),
                "{level}: task read the clipboard: {}",
                result.stdout_str()
            );
        }
    }

    /// Design 0033 D4.1 + D4.6: the per-attempt AppContainer profile is named by the FEAT-2
    /// per-attempt run token (`oxideswarm.task.<run_token>`) so a retried task with the same
    /// `TaskId` gets a fresh profile that cannot collide, and `DeleteAppContainerProfile` deletes
    /// `%LOCALAPPDATA%\Packages\oxideswarm.task.<run_token>` when each attempt finishes.
    #[tokio::test]
    async fn test_appcontainer_profile_is_deleted_on_cleanup() {
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = win_task(
            &rig,
            r#"Write-Output "RUN_TOKEN=$env:RUSTY_GRID_RUN_TOKEN""#,
            &[],
            60,
        );
        let result1 = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result1.is_success(),
            "{:?} {}",
            result1.error,
            result1.stderr_str()
        );
        // Retry the exact same `Task` (same `TaskId`): must get a distinct per-attempt run token.
        let result2 = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result2.is_success(),
            "{:?} {}",
            result2.error,
            result2.stderr_str()
        );

        let extract_token = |res: &rusty_grid_core::TaskResult| -> String {
            lines(res)
                .iter()
                .find_map(|l| l.strip_prefix("RUN_TOKEN=").map(str::to_string))
                .expect("RUN_TOKEN line")
        };
        let token1 = extract_token(&result1);
        let token2 = extract_token(&result2);
        assert_ne!(
            token1, token2,
            "retried task with the same TaskId must get a fresh per-attempt run token"
        );

        if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA").map(PathBuf::from) {
            for token in [&token1, &token2] {
                let moniker = format!("oxideswarm.task.{token}");
                let pkg_dir = local_app_data.join("Packages").join(&moniker);
                for _ in 0..20 {
                    if !pkg_dir.exists() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                assert!(
                    !pkg_dir.exists(),
                    "DeleteAppContainerProfile must remove {} on task cleanup",
                    pkg_dir.display()
                );
            }
        }
    }

    /// Review of the re-verification: the cluster secret file (and the config file) may live
    /// anywhere (even inside a toolchain directory), so the worker hands their paths to the
    /// policy via `RunnerConfig::protected_files`.
    #[tokio::test]
    async fn test_a_secret_file_outside_the_home_is_unreadable_to_a_task() {
        let outside = tempfile::tempdir().unwrap();
        let cargo_home = outside.path().join("cargo");
        std::fs::create_dir_all(&cargo_home).unwrap();
        let secret_file = cargo_home.join("cluster.secret");
        std::fs::write(&secret_file, "DUMMY-SECRETFILE-feat3c").unwrap();
        let config_file = cargo_home.join("rusty-grid.toml");
        std::fs::write(&config_file, "secret = \"DUMMY-CONFIG-feat3c\"").unwrap();

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
            let script = r#"
try { Write-Output (Get-Content -LiteralPath $env:T_A -Raw -ErrorAction Stop) } catch { Write-Output 'DENY_A' }
try { Write-Output (Get-Content -LiteralPath $env:T_B -Raw -ErrorAction Stop) } catch { Write-Output 'DENY_B' }
"#;
            let task = win_task(
                &rig,
                script,
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

        // Unprotected, the toolchain policy itself lets the task read files in `cargo_home`
        // (proving the `protected_files` deny ACE is what blocks them below).
        let open = run(IsolationLevel::Fs, false).await;
        assert!(
            open.stdout_str().contains("DUMMY-SECRETFILE-feat3c"),
            "{}",
            open.stdout_str()
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run(level, true).await;
            let out = result.stdout_str();
            assert!(
                !out.contains("DUMMY-SECRETFILE-feat3c") && !out.contains("DUMMY-CONFIG-feat3c"),
                "{level}: {out}"
            );
            let got = lines(&result);
            assert!(
                got.contains(&"DENY_A".to_string()) && got.contains(&"DENY_B".to_string()),
                "{level}: {got:?}"
            );
        }
    }
}

/// Where there is no implementation yet (FEAT-3b: Linux) isolation is refused
/// at start-up, and a task is refused instead of running unsandboxed.
#[cfg(not(any(target_os = "macos", windows)))]
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
            assert!(text.contains("FEAT-3"), "{text}");
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
