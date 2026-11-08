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

#![cfg_attr(not(target_os = "macos"), allow(dead_code, unused_imports))]

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

/// Where there is no implementation yet (FEAT-3b: Linux, FEAT-3c: Windows) isolation is refused
/// at start-up, and a task is refused instead of running unsandboxed.
#[cfg(not(target_os = "macos"))]
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
