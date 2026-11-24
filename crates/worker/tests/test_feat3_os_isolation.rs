//! FEAT-3 (design 0033): OS-level task isolation, `--isolation off|fs|fs+net`.
//!
//! FEAT-3a ships the macOS implementation (`sandbox-exec`), FEAT-3b the Linux one (Landlock,
//! `seccomp`, namespaces), and FEAT-3c the Windows one (per-task AppContainer profile, capability
//! SIDs, and private Desktop); the tests that exercise them are `#[cfg(target_os = "macos")]`,
//! `#[cfg(target_os = "linux")]`, and `#[cfg(windows)]`.
//!
//! On Linux a kernel without Landlock skips the confinement tests with a notice, unless
//! `RUSTY_GRID_REQUIRE_LANDLOCK=1` is set (CI sets it), which makes that a failure. On Windows,
//! `RUSTY_GRID_REQUIRE_APPCONTAINER=1` (set in CI) likewise turns any missing host precondition
//! into a failure via `cannot_run`.
//!
//! `RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8=1` (set ONLY on Windows CI's Step 8
//! `cargo test --workspace` invocation after Step 7 has already executed
//! `test_feat3_os_isolation` serially with `--test-threads=1 --nocapture`) lets the existing
//! `#[cfg(windows)]` AppContainer tests return early with a printed `skipped:` notice so Step 8
//! still builds and executes the target without repeating the AppContainer run. Any newly added
//! test target or unannotated test still runs normally.
//!
//! Every test uses a temporary `HOME` / `USERPROFILE` (with a dummy `.oxideswarm/cluster.secret`
//! and `.ssh/test_key`), its own sandbox base and loopback ports; nothing touches the developer's
//! real `~/.oxideswarm`. The real Rust toolchain is only read.

#![cfg_attr(
    not(any(target_os = "macos", target_os = "linux", windows)),
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
            home.path().join(".oxideswarm").join("sandboxes")
        } else {
            outside.path().join("sandboxes")
        };
        std::fs::create_dir_all(base.join("other_task")).unwrap();
        std::fs::write(base.join("other_task").join("secret.txt"), SIBLING).unwrap();

        // On Windows, never touch the developer's real `.rustup` or `.cargo` in `Rig::new`;
        // use temporary directories under `home` by default. Tests that run `cargo build`
        // use a staged toolchain in a dedicated `TempDir`.
        #[cfg(windows)]
        let host = {
            let rustup_home = home.path().join(".rustup");
            let cargo_home = home.path().join(".cargo");
            std::fs::create_dir_all(&rustup_home).unwrap();
            std::fs::create_dir_all(&cargo_home).unwrap();
            HostDirs::from_home(
                home.path().to_path_buf(),
                Some(rustup_home),
                Some(cargo_home),
            )
        };
        #[cfg(not(windows))]
        let host = {
            let real = HostDirs::detect();
            HostDirs::from_home(
                home.path().to_path_buf(),
                Some(real.rustup_home),
                Some(real.cargo_home),
            )
        };
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
                p(&self.home.path().join(".oxideswarm").join("cluster.secret")),
            ),
            (
                "T_KEY".into(),
                p(&self.home.path().join(".ssh").join("test_key")),
            ),
            (
                "T_SIBLING".into(),
                p(&self.base.join("other_task").join("secret.txt")),
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

    #[cfg_attr(target_os = "linux", allow(dead_code))]
    #[cfg(not(windows))]
    async fn run(&self, script: &str) -> TaskResult {
        self.runner
            .execute_task(&self.task(script, &[], 60), None, None)
            .await
    }
}

fn lines(result: &TaskResult) -> Vec<String> {
    result
        .stdout_str()
        .lines()
        .map(|l| l.trim_end().to_string())
        .collect()
}

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
fn format_result_diag(result: &TaskResult) -> String {
    format!(
        "exit_code={} error={:?} elapsed_ms={} stdout={:?} stderr={:?}",
        result.exit_code,
        result.error,
        result.execution_time_ms,
        result.stdout_str(),
        result.stderr_str()
    )
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

#[cfg(any(target_os = "macos", windows))]
fn assert_fs_policy_holds(result: &TaskResult, what: &str) {
    assert!(
        result.is_success(),
        "{what}: the task itself must run (exit_code={} / {:#x}): {:?} / {}",
        result.exit_code,
        result.exit_code as u32,
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
            "{what}: missing {expected}; output was {got:?} (stderr: {})",
            result.stderr_str()
        );
    }
    assert!(
        !got.iter().any(|l| l.starts_with("LEAK_")),
        "{what}: a forbidden access succeeded: {got:?}"
    );
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn assert_workspace_confinement(level: IsolationLevel, ws_in_home: bool) {
    use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
    use rusty_grid_core::task::WorkspaceRef;

    let home = tempfile::tempdir().expect("home");
    let outside = tempfile::tempdir().expect("outside");
    std::fs::create_dir_all(home.path().join(".oxideswarm")).unwrap();
    std::fs::write(home.path().join(".oxideswarm/cluster.secret"), SECRET).unwrap();
    std::fs::create_dir_all(home.path().join(".ssh")).unwrap();
    std::fs::write(home.path().join(".ssh/test_key"), KEY).unwrap();

    let sandbox_base = outside.path().join("sandboxes");
    let ws_root = if ws_in_home {
        home.path().join(".cache/oxideswarm/workspaces")
    } else {
        outside.path().join("workspaces")
    };

    let real = HostDirs::detect();
    let host = HostDirs::from_home(
        home.path().to_path_buf(),
        Some(real.rustup_home),
        Some(real.cargo_home),
    );
    let config = RunnerConfig::new(SandboxConfig::new(sandbox_base.clone()))
        .with_isolation(level)
        .with_host_dirs(host)
        .with_workspaces(true)
        .with_workspace_root(Some(ws_root.clone()));
    let caps = WorkerCapabilities::new("feat3-ws", 4, 8192, false, false, None);
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, config);
    let cache = runner.blob_cache().expect("blob cache");

    let file_data = b"manifest-hello";
    let file_id = BlobId::of(file_data);
    let tmp_file = cache.temp_path();
    std::fs::write(&tmp_file, file_data).unwrap();
    cache
        .adopt(&tmp_file, file_id, file_data.len() as u64)
        .unwrap();
    let manifest = TreeManifest::new(vec![BlobFile {
        path: "src/in.txt".into(),
        id: file_id,
        size: file_data.len() as u64,
        executable: false,
    }])
    .unwrap();
    let m_bytes = manifest.encode();
    let m_id = BlobId::of(&m_bytes);
    let tmp_m = cache.temp_path();
    std::fs::write(&tmp_m, &m_bytes).unwrap();
    cache.adopt(&tmp_m, m_id, m_bytes.len() as u64).unwrap();
    let m_size = m_bytes.len() as u64;

    // Populate a sibling workspace `other_ws` first so `<ws_root>/other_ws/tree/secret.txt` exists.
    let seed_other = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "printf 'other-ws-secret' > secret.txt".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("other_ws").with_tree(m_id, m_size));
    let seed_res = runner.execute_task(&seed_other, None, None).await;
    assert!(
        seed_res.is_success(),
        "seeding other_ws failed: {:?} / {}",
        seed_res.error,
        seed_res.stderr_str()
    );
    let other_secret_path = ws_root.join("other_ws/tree/secret.txt");
    let other_planted_path = ws_root.join("other_ws/tree/planted.txt");
    let active_state_json = ws_root.join("active_ws/state.json");
    assert!(other_secret_path.exists());

    // Now run a task in `active_ws` and check its own tree/tmp accesses and cross-workspace /
    // host probes (direct paths, `..` traversal, symlink to another workspace, hard link to
    // another workspace's file).
    let script = r#"
[ "$(cat src/in.txt)" = "manifest-hello" ] && echo READ_TREE_OK
echo built-v1 > "$RUSTY_GRID_WORKSPACE_DIR/built.txt" && echo WRITE_TREE_OK
[ "$(cat "$RUSTY_GRID_WORKSPACE_DIR/built.txt")" = "built-v1" ] && echo READ_BUILT_OK
echo scratch > "$TMPDIR/scratch.txt" && echo WRITE_TMP_OK
[ "$(cat "$TMPDIR/scratch.txt")" = "scratch" ] && echo READ_TMP_OK
cat "$T_OTHER_SECRET" > /dev/null 2>&1 && echo LEAK_READ_OTHER_WS || echo DENY_READ_OTHER_WS
( echo pwn > "$T_OTHER_PLANTED" ) 2>/dev/null && echo LEAK_WRITE_OTHER_WS || echo DENY_WRITE_OTHER_WS
cat "$T_STATE_JSON" > /dev/null 2>&1 && echo LEAK_STATE_JSON || echo DENY_STATE_JSON
ls "$T_WS_ROOT" > /dev/null 2>&1 && echo LEAK_WS_ROOT_LIST || echo DENY_WS_ROOT_LIST
cat ../state.json > /dev/null 2>&1 && echo LEAK_DOTDOT_STATE || echo DENY_DOTDOT_STATE
cat ../../other_ws/tree/secret.txt > /dev/null 2>&1 && echo LEAK_DOTDOT_OTHER_WS || echo DENY_DOTDOT_OTHER_WS
( ln -s "$T_OTHER_SECRET" "$RUSTY_GRID_WORKSPACE_DIR/sym_other" 2>/dev/null && cat "$RUSTY_GRID_WORKSPACE_DIR/sym_other" > /dev/null 2>&1 ) && echo LEAK_SYMLINK_OTHER_WS || echo DENY_SYMLINK_OTHER_WS
rm -f "$RUSTY_GRID_WORKSPACE_DIR/sym_other"
( ln "$T_OTHER_SECRET" "$RUSTY_GRID_WORKSPACE_DIR/hard_other" 2>/dev/null && cat "$RUSTY_GRID_WORKSPACE_DIR/hard_other" > /dev/null 2>&1 ) && echo LEAK_HARDLINK_OTHER_WS || echo DENY_HARDLINK_OTHER_WS
rm -f "$RUSTY_GRID_WORKSPACE_DIR/hard_other"
cat "$T_SECRET" > /dev/null 2>&1 && echo LEAK_SECRET || echo DENY_SECRET
cat "$T_KEY" > /dev/null 2>&1 && echo LEAK_KEY || echo DENY_KEY
( echo x > "$T_HOME/planted.txt" ) 2>/dev/null && echo LEAK_WRITE_HOME || echo DENY_WRITE_HOME
"#;
    let env = HashMap::from([
        (
            "T_OTHER_SECRET".into(),
            other_secret_path.to_string_lossy().into_owned(),
        ),
        (
            "T_OTHER_PLANTED".into(),
            other_planted_path.to_string_lossy().into_owned(),
        ),
        (
            "T_STATE_JSON".into(),
            active_state_json.to_string_lossy().into_owned(),
        ),
        ("T_WS_ROOT".into(), ws_root.to_string_lossy().into_owned()),
        (
            "T_SECRET".into(),
            home.path()
                .join(".oxideswarm/cluster.secret")
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "T_KEY".into(),
            home.path()
                .join(".ssh/test_key")
                .to_string_lossy()
                .into_owned(),
        ),
        ("T_HOME".into(), home.path().to_string_lossy().into_owned()),
    ]);
    let task1 = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            env,
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("active_ws").with_tree(m_id, m_size));

    let res1 = runner.execute_task(&task1, None, None).await;
    assert!(
        res1.is_success(),
        "{level} (ws_in_home={ws_in_home}): {:?} / {}",
        res1.error,
        res1.stderr_str()
    );
    let got = lines(&res1);
    for expected in [
        "READ_TREE_OK",
        "WRITE_TREE_OK",
        "READ_BUILT_OK",
        "WRITE_TMP_OK",
        "READ_TMP_OK",
    ] {
        assert!(
            got.iter().any(|l| l == expected),
            "{level} (ws_in_home={ws_in_home}): missing {expected}; output was {got:?}"
        );
    }

    let probes = [
        "READ_OTHER_WS",
        "WRITE_OTHER_WS",
        "STATE_JSON",
        "WS_ROOT_LIST",
        "DOTDOT_STATE",
        "DOTDOT_OTHER_WS",
        "SYMLINK_OTHER_WS",
        "HARDLINK_OTHER_WS",
        "SECRET",
        "KEY",
        "WRITE_HOME",
    ];
    if level == IsolationLevel::Off {
        for probe in probes {
            let expected = format!("LEAK_{probe}");
            assert!(
                got.contains(&expected),
                "unconfined control (ws_in_home={ws_in_home}): {expected} must succeed so the confined tests prove something; output was {got:?}"
            );
        }
        return;
    }

    for probe in probes {
        let expected = format!("DENY_{probe}");
        assert!(
            got.contains(&expected),
            "{level} (ws_in_home={ws_in_home}): missing {expected}; output was {got:?}"
        );
    }
    assert!(
        !got.iter().any(|l| l.starts_with("LEAK_")),
        "{level} (ws_in_home={ws_in_home}): forbidden access succeeded: {got:?}"
    );
    assert!(!other_planted_path.exists());
    assert!(!home.path().join("planted.txt").exists());

    // Second task in `active_ws` sees `built.txt` and a fresh `$TMPDIR`.
    let task2 = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                r#"[ "$(cat built.txt)" = "built-v1" ] && [ ! -e "$TMPDIR/scratch.txt" ] && echo SECOND_OK"#
                    .into(),
            ],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("active_ws"));
    let res2 = runner.execute_task(&task2, None, None).await;
    assert!(
        res2.is_success() && res2.stdout_str().contains("SECOND_OK"),
        "{level} second task: {:?} / {} / {}",
        res2.error,
        res2.stdout_str(),
        res2.stderr_str()
    );
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn assert_workspace_tree_swap_regression(level: IsolationLevel) {
    use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
    use rusty_grid_core::task::WorkspaceRef;

    let home = tempfile::tempdir().expect("home");
    let outside = tempfile::tempdir().expect("outside");
    let victim_dir = home.path().join("victim");
    let victim_sub = victim_dir.join("victim_only");
    std::fs::create_dir_all(&victim_sub).unwrap();
    let victim_secret = victim_dir.join("secret.txt");
    let victim_planted = victim_sub.join("pwned.txt");
    std::fs::write(&victim_secret, "dummy-victim-secret").unwrap();

    let sandbox_base = outside.path().join("sandboxes");
    let ws_root = outside.path().join("workspaces");

    let real = HostDirs::detect();
    let host = HostDirs::from_home(
        home.path().to_path_buf(),
        Some(real.rustup_home),
        Some(real.cargo_home),
    );
    let config = RunnerConfig::new(SandboxConfig::new(sandbox_base))
        .with_isolation(level)
        .with_host_dirs(host)
        .with_workspaces(true)
        .with_workspace_root(Some(ws_root.clone()));
    let caps = WorkerCapabilities::new("feat3-ws-swap", 4, 8192, false, false, None);
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, config);
    let cache = runner.blob_cache().expect("blob cache");

    let file_data = b"hello";
    let file_id = BlobId::of(file_data);
    let tmp_file = cache.temp_path();
    std::fs::write(&tmp_file, file_data).unwrap();
    cache
        .adopt(&tmp_file, file_id, file_data.len() as u64)
        .unwrap();
    let manifest = TreeManifest::new(vec![BlobFile {
        path: "in.txt".into(),
        id: file_id,
        size: file_data.len() as u64,
        executable: false,
    }])
    .unwrap();
    let m_bytes = manifest.encode();
    let m_id = BlobId::of(&m_bytes);
    let tmp_m = cache.temp_path();
    std::fs::write(&tmp_m, &m_bytes).unwrap();
    cache.adopt(&tmp_m, m_id, m_bytes.len() as u64).unwrap();
    let m_size = m_bytes.len() as u64;

    // 1. Task 1 attempts `rm -rf <ws>/tree && ln -s <victim> <ws>/tree` from inside the sandbox,
    //    then Task 2 runs in the same workspace with no source.
    let swap_script = r#"
rm -rf "$RUSTY_GRID_WORKSPACE_DIR" 2>/dev/null && ln -s "$T_VICTIM" "$RUSTY_GRID_WORKSPACE_DIR" 2>/dev/null && echo SWAP_OK || echo SWAP_DENIED
"#;
    let task1 = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), swap_script.into()],
            env: HashMap::from([("T_VICTIM".into(), victim_dir.to_string_lossy().into_owned())]),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("swap_ws").with_tree(m_id, m_size));
    let res1 = runner.execute_task(&task1, None, None).await;

    let probe_victim_script = r#"
cat secret.txt 2>/dev/null && echo LEAK_VICTIM_READ
( echo pwn > victim_only/pwned.txt ) 2>/dev/null && echo LEAK_VICTIM_WRITE
echo TASK2_RAN
"#;
    let task2 = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), probe_victim_script.into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("swap_ws"));
    let res2 = runner.execute_task(&task2, None, None).await;

    let swap_denied = res1.stdout_str().contains("SWAP_DENIED");
    let task2_refused = !res2.is_success() && !res2.stdout_str().contains("TASK2_RAN");
    assert!(
        swap_denied || task2_refused,
        "{level}: either task 1's tree swap must be denied or task 2 must be refused; res1={:?}/{}, res2={:?}/{}",
        res1.error,
        res1.stdout_str(),
        res2.error,
        res2.stdout_str()
    );
    assert!(
        !res2.stdout_str().contains("LEAK_VICTIM_READ")
            && !res2.stdout_str().contains("LEAK_VICTIM_WRITE")
            && !res2.stdout_str().contains("dummy-victim-secret"),
        "{level}: task 2 touched or read the victim directory: {}",
        res2.stdout_str()
    );
    assert_eq!(
        std::fs::read_to_string(&victim_secret).unwrap(),
        "dummy-victim-secret"
    );
    assert!(!victim_planted.exists());

    // 2. Pre-run symlink check (defense in depth): even if `<ws>/tree` or `<ws>/tmp` is replaced
    //    with a symlink on disk outside the sandbox, the next workspace run (without or with a
    //    source) refuses to run, never touches the victim, and repairs the workspace.
    let seed = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "echo ok".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("tamper_ws").with_tree(m_id, m_size));
    assert!(runner.execute_task(&seed, None, None).await.is_success());

    let tamper_tree = ws_root.join("tamper_ws/tree");
    std::fs::remove_dir_all(&tamper_tree).unwrap();
    std::os::unix::fs::symlink(&victim_dir, &tamper_tree).unwrap();

    let no_source_task = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), probe_victim_script.into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("tamper_ws"));
    let no_source_res = runner.execute_task(&no_source_task, None, None).await;
    assert!(
        !no_source_res.is_success() && !no_source_res.stdout_str().contains("TASK2_RAN"),
        "{level}: task without source must be refused when tree is a symlink: {:?} / {} / {}",
        no_source_res.error,
        no_source_res.stdout_str(),
        no_source_res.stderr_str()
    );
    assert_eq!(
        std::fs::read_to_string(&victim_secret).unwrap(),
        "dummy-victim-secret"
    );
    assert!(!victim_planted.exists());
    assert!(
        !std::fs::symlink_metadata(&tamper_tree)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false),
        "tampered tree symlink must be wiped/repaired under the workspace lock"
    );

    // Also verify with a source manifest when `<ws>/tree` or `<ws>/tmp` is a symlink.
    std::fs::remove_dir_all(&tamper_tree).unwrap();
    std::os::unix::fs::symlink(&victim_dir, &tamper_tree).unwrap();
    let with_source_task = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), probe_victim_script.into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 60),
    )
    .with_workspace(WorkspaceRef::new("tamper_ws").with_tree(m_id, m_size));
    let with_source_res = runner.execute_task(&with_source_task, None, None).await;
    assert!(
        !with_source_res.is_success() && !with_source_res.stdout_str().contains("TASK2_RAN"),
        "{level}: task with source must be refused when tree is a symlink: {:?} / {}",
        with_source_res.error,
        with_source_res.stderr_str()
    );
    assert_eq!(
        std::fs::read_to_string(&victim_secret).unwrap(),
        "dummy-victim-secret"
    );
    assert!(!victim_planted.exists());
    assert!(
        !std::fs::symlink_metadata(&tamper_tree)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false),
        "tampered tree symlink must be wiped/repaired under the workspace lock"
    );

    let tamper_tmp = ws_root.join("tamper_ws/tmp");
    std::fs::remove_dir_all(&tamper_tmp).unwrap();
    std::os::unix::fs::symlink(&victim_dir, &tamper_tmp).unwrap();
    let tmp_res = runner.execute_task(&with_source_task, None, None).await;
    assert!(
        !tmp_res.is_success() && !tmp_res.stdout_str().contains("TASK2_RAN"),
        "{level}: task must be refused when tmp is a symlink: {:?} / {}",
        tmp_res.error,
        tmp_res.stderr_str()
    );
    assert_eq!(
        std::fs::read_to_string(&victim_secret).unwrap(),
        "dummy-victim-secret"
    );
    assert!(!victim_planted.exists());
    assert!(
        !std::fs::symlink_metadata(&tamper_tmp)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false),
        "tampered tmp symlink must be wiped/repaired under the workspace lock"
    );
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn assert_extra_read_only_dir_confinement(level: IsolationLevel) {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir().expect("home");
    let outside = tempfile::tempdir().expect("outside");
    let ox_dir = home.path().join(".oxideswarm");
    let ssh_dir = home.path().join(".ssh");
    let in_home_runtime = home.path().join(".pyenv").join("versions").join("3.11.8");
    let ext_runtime = outside.path().join("ext_runtime");
    let sandbox_base = outside.path().join("sandboxes");
    for dir in [
        &ox_dir,
        &ssh_dir,
        &in_home_runtime,
        &ext_runtime,
        &sandbox_base,
    ] {
        std::fs::create_dir_all(dir).unwrap();
    }

    let secret_file = ox_dir.join("cluster.secret");
    let node_key_file = ox_dir.join("node.key");
    let ssh_key_file = ssh_dir.join("test_key");
    std::fs::write(&secret_file, SECRET).unwrap();
    std::fs::write(&node_key_file, "dummy-node-key-feat3").unwrap();
    std::fs::write(&ssh_key_file, KEY).unwrap();

    let in_home_txt = in_home_runtime.join("runtime.txt");
    let ext_txt = ext_runtime.join("ext.txt");
    std::fs::write(&in_home_txt, "in-home-runtime").unwrap();
    std::fs::write(&ext_txt, "external-runtime").unwrap();

    let in_home_bin = in_home_runtime.join("hello.sh");
    std::fs::write(&in_home_bin, "#!/bin/sh\necho IN_HOME_BIN_OK\n").unwrap();
    std::fs::set_permissions(&in_home_bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let ext_bin = ext_runtime.join("ext.sh");
    std::fs::write(&ext_bin, "#!/bin/sh\necho EXT_BIN_OK\n").unwrap();
    std::fs::set_permissions(&ext_bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let real = HostDirs::detect();
    let host = HostDirs::from_home(
        home.path().to_path_buf(),
        Some(real.rustup_home),
        Some(real.cargo_home),
    )
    .with_extra_read_only_dirs(vec![in_home_runtime.clone(), ext_runtime.clone()]);
    let protected = vec![secret_file.clone(), node_key_file.clone()];

    rusty_grid_worker::isolation::verify_isolation_support_with_config(
        level,
        &sandbox_base,
        &host,
        &protected,
    )
    .await
    .expect("extra_read_only_dirs startup check must succeed");

    let cfg = RunnerConfig::new(SandboxConfig::new(sandbox_base))
        .with_isolation(level)
        .with_host_dirs(host)
        .with_protected_files(protected);
    let caps = WorkerCapabilities::new("feat3-extra-ro", 2, 4096, false, false, None);
    let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, cfg);

    let bin_task = Task::new(
        TaskSpec::Command {
            program: in_home_bin.to_string_lossy().into_owned(),
            args: Vec::new(),
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    );
    let bin_res = runner.execute_task(&bin_task, None, None).await;
    assert!(
        bin_res.is_success() && bin_res.stdout_str().contains("IN_HOME_BIN_OK"),
        "{level}: executable in allowlisted home dir must run: {:?} / {} / {}",
        bin_res.error,
        bin_res.stdout_str(),
        bin_res.stderr_str()
    );

    let ext_bin_task = Task::new(
        TaskSpec::Command {
            program: ext_bin.to_string_lossy().into_owned(),
            args: Vec::new(),
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    );
    let ext_bin_res = runner.execute_task(&ext_bin_task, None, None).await;
    assert!(
        ext_bin_res.is_success() && ext_bin_res.stdout_str().contains("EXT_BIN_OK"),
        "{level}: executable in allowlisted external dir must run: {:?} / {} / {}",
        ext_bin_res.error,
        ext_bin_res.stdout_str(),
        ext_bin_res.stderr_str()
    );

    let probe_script = r#"
set +e
[ "$(cat "$T_IN_HOME_TXT" 2>/dev/null)" = "in-home-runtime" ] && echo READ_IN_HOME_OK
[ "$(cat "$T_EXT_TXT" 2>/dev/null)" = "external-runtime" ] && echo READ_EXT_OK
if (echo pwn > "$T_IN_HOME_DIR/pwn.txt") 2>/dev/null; then echo LEAK_WRITE_IN_HOME; else echo DENY_WRITE_IN_HOME; fi
if (echo pwn > "$T_EXT_DIR/pwn.txt") 2>/dev/null; then echo LEAK_WRITE_EXT; else echo DENY_WRITE_EXT; fi
if cat "$T_SECRET" >/dev/null 2>&1; then echo LEAK_SECRET; else echo DENY_SECRET; fi
if cat "$T_NODE_KEY" >/dev/null 2>&1; then echo LEAK_NODE_KEY; else echo DENY_NODE_KEY; fi
if cat "$T_SSH_KEY" >/dev/null 2>&1; then echo LEAK_SSH_KEY; else echo DENY_SSH_KEY; fi
"#;
    let probe_task = Task::new(
        TaskSpec::Command {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), probe_script.into()],
            env: HashMap::from([
                (
                    "T_IN_HOME_TXT".into(),
                    in_home_txt.to_string_lossy().into_owned(),
                ),
                ("T_EXT_TXT".into(), ext_txt.to_string_lossy().into_owned()),
                (
                    "T_IN_HOME_DIR".into(),
                    in_home_runtime.to_string_lossy().into_owned(),
                ),
                (
                    "T_EXT_DIR".into(),
                    ext_runtime.to_string_lossy().into_owned(),
                ),
                (
                    "T_SECRET".into(),
                    secret_file.to_string_lossy().into_owned(),
                ),
                (
                    "T_NODE_KEY".into(),
                    node_key_file.to_string_lossy().into_owned(),
                ),
                (
                    "T_SSH_KEY".into(),
                    ssh_key_file.to_string_lossy().into_owned(),
                ),
            ]),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    );
    let res = runner.execute_task(&probe_task, None, None).await;
    assert!(
        res.is_success(),
        "{level} extra_read_only_dir probe task failed: {:?} / {}",
        res.error,
        res.stderr_str()
    );
    let got = lines(&res);
    for expected in [
        "READ_IN_HOME_OK",
        "READ_EXT_OK",
        "DENY_WRITE_IN_HOME",
        "DENY_WRITE_EXT",
        "DENY_SECRET",
        "DENY_NODE_KEY",
        "DENY_SSH_KEY",
    ] {
        assert!(
            got.iter().any(|l| l == expected),
            "{level}: missing {expected}; got={got:?}; stderr={}",
            res.stderr_str()
        );
    }
    assert!(
        !got.iter().any(|l| l.starts_with("LEAK_")),
        "{level}: forbidden access succeeded: {got:?}"
    );
    assert!(!in_home_runtime.join("pwn.txt").exists());
    assert!(!ext_runtime.join("pwn.txt").exists());
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
                "without isolation {leak} must succeed (otherwise the tests below prove nothing): lines={got:?} ({})",
                format_result_diag(&result)
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
    async fn test_confined_workspace_task_writes_own_tree_and_tmp_but_not_other_workspaces() {
        assert_workspace_confinement(IsolationLevel::Off, false).await;
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_workspace_confinement(level, false).await;
        }
        assert_workspace_confinement(IsolationLevel::Fs, true).await;
    }

    #[tokio::test]
    async fn test_workspace_tree_swap_is_denied_and_never_touches_victim() {
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_workspace_tree_swap_regression(level).await;
        }
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
            // SAFETY: `kill` with `sig == 0` sends no signal and touches no memory.
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
        let result = tokio::time::timeout(Duration::from_secs(60), handle)
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
        let elapsed = started.elapsed();
        eprintln!(
            "macos timeout task killed in {:.2}s (limit 60s)",
            elapsed.as_secs_f64()
        );
        assert!(
            elapsed < Duration::from_secs(60),
            "timed-out task hung for {elapsed:?}"
        );
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
            "{}",
            format_result_diag(&control)
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
        assert!(
            control_ok,
            "the unconfined control must be able to signal ({})",
            format_result_diag(&control)
        );
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
            format_result_diag(&open)
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
        // SAFETY: `buf` is a live, writable buffer of `buf.len()` `c_char` bytes.
        let n =
            unsafe { libc::confstr(libc::_CS_DARWIN_USER_CACHE_DIR, buf.as_mut_ptr(), buf.len()) };
        if n == 0 || n > buf.len() {
            eprintln!("skipped: no user cache dir");
            return;
        }
        let dir = PathBuf::from(
            // SAFETY: `0 < n <= buf.len()` above guarantees `confstr` wrote a NUL-terminated
            // string inside `buf`, which outlives this temporary `CStr`.
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
            format_result_diag(&control)
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
            // SAFETY: `kill` with `sig == 0` sends no signal and touches no memory.
            if unsafe { libc::kill(pid as i32, 0) } != 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    // ---- SEC-12: Mach/XPC lookups and hard links ------------------------------------------

    /// Waits up to `secs` for `path` to exist.
    async fn appears(path: &Path, secs: u64) -> bool {
        for _ in 0..(secs * 5) {
            if path.exists() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        path.exists()
    }

    /// Runs `script` at `level` with `$T_MARK` set to a path in a directory outside the sandbox
    /// that only an unconfined process can create. Returns whether the marker appeared.
    async fn marker_appears(level: IsolationLevel, script: &str, wait: u64) -> bool {
        let out = tempfile::tempdir().unwrap();
        let mark = out.path().join("marker");
        let bid = out
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .replace(|c: char| !c.is_ascii_alphanumeric(), "");
        let result = run_at(
            level,
            script,
            &[("T_MARK", mark.to_str().unwrap()), ("T_BID", &bid)],
        )
        .await;
        eprintln!("[{level}] task output: {}", result.stdout_str().trim());
        appears(&mark, wait).await
    }

    /// SEC-12: `launchctl submit` asks launchd to run a job outside the sandbox. The job would
    /// create a marker file that a confined task cannot create itself.
    #[tokio::test]
    async fn test_launchctl_submit_cannot_start_a_job_outside_the_sandbox() {
        let label = format!("dev.oxideswarm.feat3.sec12.{}", std::process::id());
        let script = format!(
            r#"/bin/launchctl submit -l {label} -- /usr/bin/touch "$T_MARK" 2>&1; echo "rc=$?""#
        );
        let remove = || {
            let _ = std::process::Command::new("/bin/launchctl")
                .args(["remove", &label])
                .output();
        };
        let control = marker_appears(IsolationLevel::Off, &script, 8).await;
        remove();
        if !control {
            eprintln!("skipped: launchctl submit is not usable here (the control failed)");
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let appeared = marker_appears(level, &script, 8).await;
            let listed = std::process::Command::new("/bin/launchctl")
                .args(["list", &label])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            remove();
            assert!(!appeared, "{level}: launchd ran a job outside the sandbox");
            assert!(!listed, "{level}: the task registered a launchd job");
        }
    }

    /// SEC-12: `open -a <app>` starts a background app that launchd runs unconfined; the app
    /// creates a marker outside the sandbox.
    #[tokio::test]
    async fn test_open_dash_a_cannot_start_an_unconfined_app() {
        let script = r#"
APP="$RUSTY_GRID_SANDBOX_DIR/Sec12.app"; mkdir -p "$APP/Contents/MacOS"
cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>CFBundleExecutable</key><string>run</string><key>CFBundleIdentifier</key><string>dev.oxideswarm.feat3.sec12.$T_BID</string><key>CFBundlePackageType</key><string>APPL</string><key>LSBackgroundOnly</key><true/></dict></plist>
PLIST
printf '#!/bin/sh\n/usr/bin/touch "%s"\n' "$T_MARK" > "$APP/Contents/MacOS/run"
chmod +x "$APP/Contents/MacOS/run"
/usr/bin/open -g -j -a "$APP" 2>&1; echo "rc=$?"
# The bundle must outlive the launch: the sandbox is removed when the task ends.
sleep 4
"#;
        if !marker_appears(IsolationLevel::Off, script, 10).await {
            eprintln!("skipped: open -a is not usable here (the control failed)");
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert!(
                !marker_appears(level, script, 6).await,
                "{level}: open -a started a process outside the sandbox"
            );
        }
    }

    /// SEC-12: `osascript` sends Apple events to other applications. Finder creates a folder
    /// outside the sandbox on its behalf; the task cannot create it itself.
    #[tokio::test]
    async fn test_osascript_cannot_drive_another_application() {
        let script = r#"
DIR=$(/usr/bin/dirname "$T_MARK"); NAME=$(/usr/bin/basename "$T_MARK")
/usr/bin/osascript -e "tell application \"Finder\" to make new folder at (POSIX file \"$DIR\") with properties {name:\"$NAME\"}" 2>&1
echo "rc=$?"
"#;
        let control = marker_appears(IsolationLevel::Off, script, 6).await;
        if !control {
            eprintln!("skipped: Apple events to Finder are not usable here (the control failed)");
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert!(
                !marker_appears(level, script, 4).await,
                "{level}: osascript made another application act outside the sandbox"
            );
        }
    }

    /// SEC-12: a hard link to a file of the owner's home must not make its content readable
    /// from the sandbox. The control (`off`) shows the link works without confinement.
    #[tokio::test]
    async fn test_a_hard_link_to_a_secret_does_not_expose_its_content() {
        let script = r#"
ln "$T_SECRET" "$RUSTY_GRID_SANDBOX_DIR/linked" 2>/dev/null && echo LINK_OK || echo LINK_DENIED
cat "$RUSTY_GRID_SANDBOX_DIR/linked" 2>/dev/null
echo END
"#;
        let control = run_at(IsolationLevel::Off, script, &[]).await;
        assert!(
            control.stdout_str().contains(SECRET),
            "the control must read the linked secret: {}",
            format_result_diag(&control)
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, script, &[]).await;
            eprintln!("[{level}] task output: {}", result.stdout_str().trim());
            assert!(
                !result.stdout_str().contains(SECRET),
                "{level}: the hard-linked secret became readable: {}",
                format_result_diag(&result)
            );
        }
    }

    /// SEC-12: with Mach lookups denied by default, the system C toolchain (`cc` compiling and
    /// linking through `xcrun` and `ld`) and `git` (init, add, commit, log) still work under `fs`
    /// and `fs+net`.
    #[tokio::test]
    async fn test_system_compiler_linker_and_git_work_under_fs() {
        let script = r#"
set -e
echo 'int main(void){return 42;}' > t.c
/usr/bin/cc t.c -o t
./t || [ $? -eq 42 ] && echo CC_OK
/usr/bin/git init -q repo
cd repo
echo a > a
/usr/bin/git add a
/usr/bin/git -c user.name=t -c user.email=t@example.invalid commit -q -m m
/usr/bin/git log --oneline | /usr/bin/grep -q . && echo GIT_OK
/usr/bin/git status --short
echo DONE
"#;
        if !Path::new("/usr/bin/cc").exists() || !Path::new("/usr/bin/git").exists() {
            eprintln!("skipped: no /usr/bin/cc or /usr/bin/git");
            return;
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run_at(level, script, &[]).await;
            eprintln!(
                "[{level}] stdout: {} stderr: {}",
                result.stdout_str().trim(),
                result.stderr_str().trim()
            );
            assert!(
                has(&result, "CC_OK") && has(&result, "GIT_OK") && has(&result, "DONE"),
                "{level}: {} / {}",
                result.stdout_str(),
                result.stderr_str()
            );
        }
    }

    /// SEC-12 / design 0074: Metal needs `com.apple.MTLCompilerService`, the one Mach service
    /// the profile allows. Runs the `gemm_bench` example (built with `--features gpu-wgpu`:
    /// `cargo build --release -p rusty_grid_worker --features gpu-wgpu --example gemm_bench`;
    /// found under `$RUSTY_GRID_GEMM_BENCH` or `$CARGO_TARGET_DIR/{release,debug}/examples`)
    /// as a task under `fs` and `fs+net` and expects a GPU speed-up line. Skips with a notice
    /// when the binary or a Metal adapter is missing, unless `RUSTY_GRID_REQUIRE_GPU=1`.
    #[tokio::test]
    async fn test_a_metal_task_gets_a_gpu_device_under_fs() {
        use std::os::unix::fs::PermissionsExt;
        let require = std::env::var("RUSTY_GRID_REQUIRE_GPU").as_deref() == Ok("1");
        let cannot = |why: &str| {
            assert!(!require, "RUSTY_GRID_REQUIRE_GPU=1 but: {why}");
            eprintln!("skipped: {why}");
        };
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Some(p) = std::env::var_os("RUSTY_GRID_GEMM_BENCH") {
            candidates.push(PathBuf::from(p));
        }
        if let Some(t) = std::env::var_os("CARGO_TARGET_DIR") {
            for profile in ["release", "debug"] {
                candidates.push(PathBuf::from(&t).join(profile).join("examples/gemm_bench"));
            }
        }
        let Some(bench) = candidates.into_iter().find(|p| p.is_file()) else {
            return cannot("no gemm_bench binary (build the example with --features gpu-wgpu)");
        };
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("gemm_bench");
        std::fs::copy(&bench, &exe).unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let control = std::process::Command::new(&exe)
            .arg("128")
            .output()
            .unwrap();
        let control_text = format!(
            "{}{}",
            String::from_utf8_lossy(&control.stdout),
            String::from_utf8_lossy(&control.stderr)
        );
        if !control_text.contains("GPU speed-up") {
            return cannot("no Metal adapter here (the unconfined control found no GPU engine)");
        }
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let mut rig = Rig::new(level, false);
            rig.host.extra_read_only_dirs = vec![dir.path().to_path_buf()];
            let config = RunnerConfig::new(SandboxConfig::new(rig.base.clone()))
                .with_isolation(level)
                .with_host_dirs(rig.host.clone());
            let runner = TaskRunner::new(
                uuid::Uuid::new_v4(),
                WorkerCapabilities::new("feat3", 4, 8192, false, false, None),
                config,
            );
            let task = Task::new(
                TaskSpec::Command {
                    program: exe.to_string_lossy().into_owned(),
                    args: vec!["128".into()],
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: None,
                },
                TaskRequirements::generic(1, 120),
            );
            let result = runner.execute_task(&task, None, None).await;
            let text = format!("{}{}", result.stdout_str(), result.stderr_str());
            assert!(
                text.contains("GPU speed-up"),
                "{level}: the isolated task got no GPU device: {text}"
            );
        }
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

    #[tokio::test]
    async fn test_extra_read_only_dir_inside_home_is_readable_while_secrets_stay_denied() {
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_extra_read_only_dir_confinement(level).await;
        }
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

    /// An early return of a test whose precondition is missing: a failure when CI demands the
    /// Linux tests (`RUSTY_GRID_REQUIRE_LANDLOCK`), a notice otherwise.
    fn cannot_run(why: &str) {
        assert!(
            std::env::var_os("RUSTY_GRID_REQUIRE_LANDLOCK").is_none(),
            "required here (RUSTY_GRID_REQUIRE_LANDLOCK) but cannot run: {why}"
        );
        eprintln!("skipped: {why}");
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
            cannot_run("no python3");
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
            cannot_run("no python3");
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
        let _serial = NETNS_ENV.lock().await;
        if !python3() {
            cannot_run("no python3");
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
        // SAFETY: `getuid` and `getgid` have no preconditions, touch no memory and cannot fail.
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
            assert!(has(&net, "NETLINK=ERR13"), "{:?}", lines(&net));
        } else {
            // AppArmor (Ubuntu 24.04+) lets `unshare` succeed but strips the capabilities in the
            // new namespace, so the id map cannot be written: the ids show as the overflow id.
            assert!(!require, "ids are not mapped: uid {uid} gid {gid}");
            assert_eq!((uid.as_str(), gid.as_str()), ("65534", "65534"));
            eprintln!("note: private network namespace without an id map (restricted userns)");
        }
    }

    /// Serialises the tests that depend on `RUSTY_GRID_ISOLATION_NO_USERNS` (process-wide).
    static NETNS_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Without a private network namespace (here forced with `RUSTY_GRID_ISOLATION_NO_USERNS`,
    /// like a host that forbids unprivileged user namespaces) a task shares the worker's network
    /// namespace: netlink would list the host's sockets, routes and addresses, and
    /// `/proc/self/net/tcp` its connections. `fs+net` must deny both.
    #[tokio::test]
    async fn test_fs_net_without_a_private_namespace_hides_the_hosts_sockets() {
        require_landlock!();
        let _serial = NETNS_ENV.lock().await;
        if !python3() {
            cannot_run("no python3");
            return;
        }
        let py = r#"
import os, socket
print("NETNS=" + os.readlink("/proc/self/ns/net"))
for name, proto in (("DIAG", 4), ("ROUTE", 0)):
    try:
        socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, proto).close()
        print(name + "=OK")
    except OSError as e:
        print(name + "=ERR%d" % e.errno)
try:
    open("/proc/self/net/tcp").read()
    print("PROCNET=OK")
except OSError as e:
    print("PROCNET=ERR%d" % e.errno)
"#;
        let script = r#"exec python3 -c "$T_PY""#;
        let value = |result: &TaskResult, key: &str| {
            lines(result)
                .iter()
                .find_map(|l| l.strip_prefix(&format!("{key}=")).map(str::to_string))
                .unwrap_or_default()
        };
        let control = run_at(IsolationLevel::Off, script, &[("T_PY", py)]).await;
        if !(has(&control, "DIAG=OK") && has(&control, "ROUTE=OK") && has(&control, "PROCNET=OK")) {
            cannot_run(&format!(
                "netlink or /proc/self/net unusable even unconfined: {:?}",
                lines(&control)
            ));
            return;
        }
        eprintln!("NETLINK_CONTROL_OK");
        let worker_netns = std::fs::read_link("/proc/self/ns/net").unwrap();
        // LV-4 finding: `NETNS_ENV` only serialises the two `NETNS_ENV` tests, while other
        // parallel tests in this binary call `getenv` (`require_landlock!`, `tempfile::tempdir`)
        // and spawn children without holding `NETNS_ENV`, racing glibc `setenv`/`unsetenv` on
        // `environ`. Left unchanged per LV-4 part 2 scope (Linux-only test code).
        // SAFETY: `NETNS_ENV` serialises `RUSTY_GRID_ISOLATION_NO_USERNS` readers and writers; in
        // Rust 2021 `set_var` is a safe fn, though POSIX `setenv` requires exclusive env access.
        unsafe { std::env::set_var("RUSTY_GRID_ISOLATION_NO_USERNS", "1") };
        let net = run_at(IsolationLevel::FsNet, script, &[("T_PY", py)]).await;
        // SAFETY: paired with `set_var` above while still holding `NETNS_ENV` (same `unsetenv`
        // caveat as above regarding concurrent `getenv` calls in other parallel tests).
        unsafe { std::env::remove_var("RUSTY_GRID_ISOLATION_NO_USERNS") };
        assert_eq!(
            value(&net, "NETNS"),
            worker_netns.to_string_lossy(),
            "the namespace was not entered"
        );
        for name in ["DIAG=ERR13", "ROUTE=ERR13", "PROCNET=ERR13"] {
            assert!(
                has(&net, name),
                "{name}: {:?} {}",
                lines(&net),
                net.stderr_str()
            );
        }
        eprintln!("NETLINK_CONFINED_DENIED {:?}", lines(&net));
    }

    /// The worker user's kernel keyring is out of reach (`EPERM`).
    #[tokio::test]
    async fn test_the_kernel_keyring_is_refused() {
        require_landlock!();
        if !python3() {
            cannot_run("no python3");
            return;
        }
        let py = r#"
import ctypes, platform
nr = 250 if platform.machine() == "x86_64" else 219
libc = ctypes.CDLL(None, use_errno=True)
# KEYCTL_GET_KEYRING_ID(0) of KEY_SPEC_USER_KEYRING (-4)
r = libc.syscall(nr, 0, -4, 0)
print("KEYCTL_OK" if r >= 0 else "KEYCTL_ERR%d" % ctypes.get_errno())
"#;
        let script = r#"python3 -c "$T_PY""#;
        let control = run_at(IsolationLevel::Off, script, &[("T_PY", py)]).await;
        if !has(&control, "KEYCTL_OK") {
            eprintln!(
                "skipped: keyctl unusable even unconfined: {:?}",
                lines(&control)
            );
            return;
        }
        for level in LEVELS {
            let result = run_at(level, script, &[("T_PY", py)]).await;
            assert!(has(&result, "KEYCTL_ERR1"), "{level}: {:?}", lines(&result));
        }
    }

    /// `io_uring` could create sockets behind the socket filter (`IORING_OP_SOCKET`), so its
    /// setup is refused with `ENOSYS`.
    #[tokio::test]
    async fn test_io_uring_setup_is_refused() {
        require_landlock!();
        if !python3() {
            cannot_run("no python3");
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
            cannot_run("no python3");
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
        assert!(
            control_ok,
            "the unconfined control must be able to signal ({})",
            format_result_diag(&control)
        );
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
            // SAFETY: `kill` with `sig == 0` sends no signal and touches no memory.
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
        let result = tokio::time::timeout(Duration::from_secs(60), handle)
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
            let elapsed = started.elapsed();
            eprintln!(
                "linux timeout task ({level}) killed in {:.2}s (limit 60s)",
                elapsed.as_secs_f64()
            );
            assert!(
                elapsed < Duration::from_secs(60),
                "{level}: timed-out task hung for {elapsed:?}"
            );
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
            // SAFETY: `kill` with `sig == 0` sends no signal and touches no memory.
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

    #[tokio::test]
    async fn test_confined_workspace_task_writes_own_tree_and_tmp_but_not_other_workspaces() {
        require_landlock!();
        assert_workspace_confinement(IsolationLevel::Off, false).await;
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_workspace_confinement(level, false).await;
        }
        assert_workspace_confinement(IsolationLevel::Fs, true).await;
    }

    #[tokio::test]
    async fn test_workspace_tree_swap_is_denied_and_never_touches_victim() {
        require_landlock!();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_workspace_tree_swap_regression(level).await;
        }
    }

    #[tokio::test]
    async fn test_extra_read_only_dir_inside_home_is_readable_while_secrets_stay_denied() {
        require_landlock!();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_extra_read_only_dir_confinement(level).await;
        }
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    /// A `Command` whose console child gets a console with no window (TEST-88). Run from a parent
    /// without a console (a CI service, a script that captures output), a plain `Command` opens a
    /// visible console window for every `cmd`, `powershell`, `reg` or `icacls` child.
    fn hidden_command(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
        use std::os::windows::process::CommandExt;
        let mut cmd = std::process::Command::new(program);
        cmd.creation_flags(rusty_grid_core::capabilities::WINDOWS_CREATE_NO_WINDOW);
        cmd
    }

    /// Verifies that Windows AppContainer isolation (`userenv.dll` + capability SIDs) is
    /// supported on this host, or returns `false` (after a notice) when unavailable and the
    /// environment does not demand it (`RUSTY_GRID_REQUIRE_APPCONTAINER`).
    fn appcontainer_available() -> bool {
        match rusty_grid_worker::isolation::windows::OwnedSid::oxideswarm_toolchain() {
            Ok(_) => true,
            Err(e) => {
                assert!(
                    std::env::var_os("RUSTY_GRID_REQUIRE_APPCONTAINER").is_none(),
                    "AppContainer is required here (RUSTY_GRID_REQUIRE_APPCONTAINER) but unavailable: {e}"
                );
                eprintln!("skipped: no AppContainer on this Windows host ({e})");
                false
            }
        }
    }

    macro_rules! require_appcontainer {
        () => {
            if !appcontainer_available() {
                return;
            }
        };
    }

    /// An early return of a test whose precondition is missing: a failure when CI demands the
    /// Windows tests (`RUSTY_GRID_REQUIRE_APPCONTAINER`), a notice otherwise.
    fn cannot_run(why: &str) {
        assert!(
            std::env::var_os("RUSTY_GRID_REQUIRE_APPCONTAINER").is_none(),
            "required here (RUSTY_GRID_REQUIRE_APPCONTAINER) but cannot run: {why}"
        );
        eprintln!("skipped: {why}");
    }

    /// Windows CI runs `test_feat3_os_isolation` serially in its own step (`--test-threads=1`)
    /// before `cargo test --workspace` (Step 8). Step 8 sets
    /// `RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8=1` on itself so these already-run Windows tests
    /// print a skip notice without excluding the binary or masking any newly added target or test.
    fn skip_duplicate_ci_workspace_pass() -> bool {
        if matches!(
            std::env::var("RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        ) {
            eprintln!(
                "skipped: already executed in Windows AppContainer isolation step (RUSTY_GRID_SKIP_FEAT3_OS_ISOLATION_STEP8=1)"
            );
            return true;
        }
        false
    }

    macro_rules! skip_if_duplicate_ci_pass {
        () => {
            if skip_duplicate_ci_workspace_pass() {
                return;
            }
        };
    }

    /// Generous condition-wait deadline for Windows PowerShell/AppContainer test barriers under load.
    const WIN_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

    struct ChildKillGuard(Option<std::process::Child>);

    impl Drop for ChildKillGuard {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    /// Poll `cond` every 25 ms until it holds, any `done_flag` becomes true, or `WIN_WAIT_TIMEOUT`
    /// elapses. Returns early with a diagnostic error if a test task exits before the barrier.
    async fn wait_for_barrier<F>(
        what: &str,
        done_flags: &[(&str, &AtomicBool)],
        mut cond: F,
    ) -> Result<(), String>
    where
        F: FnMut() -> bool,
    {
        let deadline = Instant::now() + WIN_WAIT_TIMEOUT;
        loop {
            if cond() {
                return Ok(());
            }
            for (label, flag) in done_flags {
                if flag.load(Ordering::Acquire) {
                    return Err(format!(
                        "{label} exited before barrier '{what}' was satisfied"
                    ));
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out after {}s waiting for barrier '{what}'",
                    WIN_WAIT_TIMEOUT.as_secs()
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn win_task(rig: &Rig, script: &str, extra_env: &[(&str, &str)], timeout_secs: u64) -> Task {
        let mut env = rig.env();
        for (k, v) in extra_env {
            env.insert((*k).into(), (*v).into());
        }
        let capped_timeout = if timeout_secs == 60 {
            35
        } else {
            timeout_secs.min(90)
        };
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
            TaskRequirements::generic(1, capped_timeout),
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
        let capped_timeout = if timeout_secs == 60 {
            35
        } else {
            timeout_secs.min(90)
        };
        Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), script.into()],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, capped_timeout),
        )
    }

    const WIN_CSHARP_HELPER: &str = r#"
function Compile-CSharp([string]$code) {
    $prov = [Microsoft.CSharp.CSharpCodeProvider]::new()
    $cp = [System.CodeDom.Compiler.CompilerParameters]::new()
    $cp.GenerateInMemory = $true
    $cp.TempFiles = [System.CodeDom.Compiler.TempFileCollection]::new($env:RUSTY_GRID_SANDBOX_DIR, $false)
    $null = $cp.ReferencedAssemblies.Add('System.dll')
    $null = $cp.ReferencedAssemblies.Add('System.Core.dll')
    $cr = $prov.CompileAssemblyFromSource($cp, [string[]]@($code))
    if ($cr.Errors.HasErrors) {
        $msgs = [System.Collections.Generic.List[string]]::new()
        foreach ($e in $cr.Errors) { $null = $msgs.Add($e.ToString()) }
        throw [string]::Join('; ', $msgs)
    }
}
"#;

    const WIN_FS_SCRIPT: &str = r#"echo TASK_STARTED&((echo hello)>%RUSTY_GRID_SANDBOX_DIR%\allowed.txt&&echo WRITE_OK)&(findstr hello %RUSTY_GRID_SANDBOX_DIR%\allowed.txt >.sink 2>.err&&echo READ_OK)&(mkdir %RUSTY_GRID_SANDBOX_DIR%\sub >.sink 2>.err&&echo MKDIR_OK)&((echo x>.sink)&&echo DEVNULL_OK)&(type %T_SECRET% >.sink 2>.err&&echo LEAK_SECRET||echo DENY_SECRET)&(type %T_KEY% >.sink 2>.err&&echo LEAK_KEY||echo DENY_KEY)&(type %T_SIBLING% >.sink 2>.err&&echo LEAK_SIBLING||echo DENY_SIBLING)&(dir /b %T_BASE% >.sink 2>.err&&echo LEAK_LISTING||echo DENY_LISTING)&(dir /b %T_HOME% >.sink 2>.err&&echo LEAK_HOME_LISTING||echo DENY_HOME_LISTING)&((echo x>%T_HOME%\planted.txt) >.sink 2>.err&if exist %T_HOME%\planted.txt (echo LEAK_WRITE_HOME) else (echo DENY_WRITE_HOME))&((echo x>%T_BASE%\other_task\planted.txt) >.sink 2>.err&if exist %T_BASE%\other_task\planted.txt (echo LEAK_WRITE_SIBLING) else (echo DENY_WRITE_SIBLING))&((echo x>%T_OUTSIDE%\feat3_outside_marker) >.sink 2>.err&if exist %T_OUTSIDE%\feat3_outside_marker (echo LEAK_WRITE_TMP) else (echo DENY_WRITE_TMP))"#;

    fn run_win_fs(rig: &Rig) -> Task {
        let outside = rig._outside.path().to_string_lossy().into_owned();
        win_cmd_task(rig, WIN_FS_SCRIPT, &[("T_OUTSIDE", &outside)], 25)
    }

    #[tokio::test]
    async fn test_off_leaves_the_owners_files_readable_as_a_control() {
        skip_if_duplicate_ci_pass!();
        let rig = Rig::new(IsolationLevel::Off, false);
        let result = rig.runner.execute_task(&run_win_fs(&rig), None, None).await;
        let got = lines(&result);
        assert!(got.contains(&"TASK_STARTED".to_string()), "{got:?}");
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
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig.runner.execute_task(&run_win_fs(&rig), None, None).await;
            assert!(
                lines(&result).contains(&"TASK_STARTED".to_string()),
                "{level}: missing TASK_STARTED: {:?}",
                lines(&result)
            );
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
        skip_if_duplicate_ci_pass!();
        let rig = Rig::new(IsolationLevel::Fs, true);
        let result = rig.runner.execute_task(&run_win_fs(&rig), None, None).await;
        assert!(
            lines(&result).contains(&"TASK_STARTED".to_string()),
            "missing TASK_STARTED: {:?}",
            lines(&result)
        );
        assert_fs_policy_holds(&result, "base inside home");
    }

    #[tokio::test]
    async fn test_concurrent_tasks_under_fs_cannot_read_or_list_each_others_sandboxes() {
        skip_if_duplicate_ci_pass!();
        let rig = Arc::new(Rig::new(IsolationLevel::Fs, false));
        let script = r#"
[Console]::Out.WriteLine('TASK_STARTED')
[System.IO.File]::WriteAllText("$env:RUSTY_GRID_SANDBOX_DIR\secret.txt", 'secret-from-t1')
$goDeadline = [DateTime]::UtcNow.AddSeconds(60)
while ([DateTime]::UtcNow -lt $goDeadline) {
    if ([System.IO.File]::Exists("$env:RUSTY_GRID_SANDBOX_DIR\go.txt")) { [Console]::Out.WriteLine('GO_SEEN'); break }
    [System.Threading.Thread]::Sleep(25)
}
try { $null = [System.IO.File]::ReadAllText("$env:T_SIB_SANDBOX\secret.txt"); [Console]::Out.WriteLine('LEAK_SIBLING_READ') } catch { [Console]::Out.WriteLine('DENY_SIBLING_READ') }
try { $null = [System.IO.Directory]::GetFileSystemEntries($env:T_SIB_SANDBOX); [Console]::Out.WriteLine('LEAK_SIBLING_LIST') } catch { [Console]::Out.WriteLine('DENY_SIBLING_LIST') }
try { [System.IO.File]::WriteAllText("$env:T_SIB_SANDBOX\planted.txt", 'x'); [Console]::Out.WriteLine('LEAK_SIBLING_WRITE') } catch { [Console]::Out.WriteLine('DENY_SIBLING_WRITE') }
[System.IO.File]::WriteAllText("$env:RUSTY_GRID_SANDBOX_DIR\done.txt", '1')
$exitDeadline = [DateTime]::UtcNow.AddSeconds(60)
while ([DateTime]::UtcNow -lt $exitDeadline) {
    if ([System.IO.File]::Exists("$env:RUSTY_GRID_SANDBOX_DIR\exit.txt")) { break }
    [System.Threading.Thread]::Sleep(25)
}
"#;
        let task1 = win_task(&rig, script, &[], 90);
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
        let sb1_b = sb1.clone();
        let sb2_b = sb2.clone();
        let t1_done = Arc::new(AtomicBool::new(false));
        let t2_done = Arc::new(AtomicBool::new(false));
        let t1_done_exec = Arc::clone(&t1_done);
        let t2_done_exec = Arc::clone(&t2_done);
        let barrier = async move {
            let release_siblings = || {
                for dir in [&sb1_b, &sb2_b] {
                    if dir.is_dir() {
                        let _ = std::fs::write(dir.join("go.txt"), "go");
                        let _ = std::fs::write(dir.join("exit.txt"), "exit");
                    }
                }
            };
            let flags = [("task1", t1_done.as_ref()), ("task2", t2_done.as_ref())];
            if let Err(err) = wait_for_barrier("both sibling secret.txt", &flags, || {
                sb1_b.join("secret.txt").is_file() && sb2_b.join("secret.txt").is_file()
            })
            .await
            {
                release_siblings();
                return Err(format!(
                    "both sibling sandbox secret.txt files must exist before either task probes the other ({}, {}): {err}",
                    sb1_b.display(),
                    sb2_b.display()
                ));
            }
            std::fs::write(sb1_b.join("go.txt"), "go")
                .map_err(|e| format!("write sb1 go.txt: {e}"))?;
            std::fs::write(sb2_b.join("go.txt"), "go")
                .map_err(|e| format!("write sb2 go.txt: {e}"))?;

            if let Err(err) = wait_for_barrier("both sibling done.txt", &flags, || {
                sb1_b.join("done.txt").is_file() && sb2_b.join("done.txt").is_file()
            })
            .await
            {
                release_siblings();
                return Err(format!(
                    "both tasks must complete their sibling probes while both sandboxes are still present: {err}"
                ));
            }
            if !(sb1_b.join("secret.txt").is_file() && sb2_b.join("secret.txt").is_file()) {
                release_siblings();
                return Err(
                    "both sibling secret.txt files must still exist when probes finish".to_string(),
                );
            }
            let _ = std::fs::write(sb1_b.join("exit.txt"), "exit");
            let _ = std::fs::write(sb2_b.join("exit.txt"), "exit");
            Ok(())
        };
        let (res1, res2, barrier_res) = tokio::join!(
            async move {
                let res = r1.runner.execute_task(&t1, None, None).await;
                t1_done_exec.store(true, Ordering::Release);
                res
            },
            async move {
                let res = r2.runner.execute_task(&t2, None, None).await;
                t2_done_exec.store(true, Ordering::Release);
                res
            },
            barrier,
        );
        if let Err(err) = barrier_res {
            panic!(
                "{err}; task1: {}; task2: {}",
                format_result_diag(&res1),
                format_result_diag(&res2)
            );
        }

        for (label, res) in [("task1", &res1), ("task2", &res2)] {
            assert!(
                res.is_success(),
                "{label} failed: {:?} / {}",
                res.error,
                res.stderr_str()
            );
            let got = lines(res);
            for expected in [
                "TASK_STARTED",
                "GO_SEEN",
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
        skip_if_duplicate_ci_pass!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = win_cmd_task(
            &rig,
            r#"echo TASK_STARTED=1& echo HOME=%HOME%& echo USERPROFILE=%USERPROFILE%& echo TMPDIR=%TMPDIR%& echo TEMP=%TEMP%& echo TMP=%TMP%& echo SANDBOX=%RUSTY_GRID_SANDBOX_DIR%& echo RUSTUP_HOME=%RUSTUP_HOME%& echo CARGO_HOME=%CARGO_HOME%& (if defined SSH_AUTH_SOCK (echo SSH=%SSH_AUTH_SOCK%) else (echo SSH=unset))& (if defined GPG_AGENT_INFO (echo GPG=%GPG_AGENT_INFO%) else (echo GPG=unset))& (if defined DBUS_SESSION_BUS_ADDRESS (echo DBUS=%DBUS_SESSION_BUS_ADDRESS%) else (echo DBUS=unset))& (if defined DOCKER_HOST (echo DOCKER=%DOCKER_HOST%) else (echo DOCKER=unset))& (if defined OXIDESWARM_SECRET (echo SECRET=%OXIDESWARM_SECRET%) else (echo SECRET=unset))& (if defined RUSTY_GRID_TASK_ID (echo TASK_ID=%RUSTY_GRID_TASK_ID%) else (echo TASK_ID=unset))& (if defined RUSTY_GRID_RUN_TOKEN (echo RUN_TOKEN=%RUSTY_GRID_RUN_TOKEN%) else (echo RUN_TOKEN=unset))"#,
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
        assert_eq!(got.get("TASK_STARTED").map(String::as_str), Some("1"));
        let sandbox = got["SANDBOX"].clone();
        assert!(sandbox.contains(&rig.base.file_name().unwrap().to_string_lossy().to_string()));
        for name in ["HOME", "USERPROFILE", "TMPDIR"] {
            assert_eq!(got[name], sandbox, "{name}");
        }
        // On Windows, `CreateProcessW` (`KernelBase!BasepAppContainerEnvironmentExtension`)
        // rewrites `TEMP` and `TMP` into the per-attempt AppContainer folder
        // `...\Packages\oxideswarm.task.<worker8>.<run_token_simple>\AC\Temp`.
        assert_ne!(got["RUN_TOKEN"], "unset");
        let prefix = rusty_grid_worker::isolation::windows::worker_profile_prefix(&rig.base);
        let token_simple: String = got["RUN_TOKEN"].chars().filter(|c| *c != '-').collect();
        let task_moniker = format!("{prefix}.{token_simple}");
        for name in ["TEMP", "TMP"] {
            let val = &got[name];
            assert!(
                val == &sandbox || val.contains(&task_moniker),
                "{name} must be sandbox or per-task AppContainer temp dir ({task_moniker}), got {val}"
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
        skip_if_duplicate_ci_pass!();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port().to_string();

        let udp_listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        udp_listener.set_nonblocking(true).unwrap();
        let udp_port = udp_listener.local_addr().unwrap().port().to_string();

        let listener6 = std::net::TcpListener::bind("[::1]:0").ok();
        if let Some(ref l6) = listener6 {
            l6.set_nonblocking(true).unwrap();
        }
        let port6 = listener6
            .as_ref()
            .map(|l| l.local_addr().unwrap().port().to_string())
            .unwrap_or_else(|| "0".to_string());

        let tcp_script = r#"
[Console]::Out.WriteLine('TASK_STARTED')
try {
    $c = [System.Net.Sockets.TcpClient]::new()
    $c.Connect('127.0.0.1', [int]$env:T_PORT)
    $c.Close()
    [Console]::Out.WriteLine('CONNECTED')
} catch {
    [Console]::Out.WriteLine('REFUSED')
}
try {
    $u = [System.Net.Sockets.UdpClient]::new()
    $b = [System.Text.Encoding]::ASCII.GetBytes('FEAT3_UDP')
    $null = $u.Send($b, $b.Length, '127.0.0.1', [int]$env:T_UDP_PORT)
    $u.Close()
} catch {}
"#;
        // M1: Probes non-loopback IPv4 TCP (`192.0.2.1:80`), outbound UDP to IP literal
        // (`192.0.2.1:53`), real DNS resolution (`example.com`), IPv6 loopback (`[::1]`) and IPv6
        // outbound (`[2001:db8::1]:80`), and UNC share access (`\\127.0.0.1\C$\Windows\win.ini`).
        let net_matrix_script = r#"
[Console]::Out.WriteLine('TASK_STARTED')
try {
    $c = [System.Net.Sockets.TcpClient]::new()
    $ar = $c.BeginConnect('192.0.2.1', 80, $null, $null)
    if ($ar.AsyncWaitHandle.WaitOne(150, $false)) {
        $c.EndConnect($ar)
    }
    $c.Close()
    [Console]::Out.WriteLine('TCP_V4_ALLOWED')
} catch [System.Net.Sockets.SocketException] {
    if ($_.Exception.NativeErrorCode -eq 10013) { [Console]::Out.WriteLine('TCP_V4_REFUSED') } else { [Console]::Out.WriteLine('TCP_V4_ALLOWED') }
} catch {
    if ($_.Exception.InnerException -is [System.Net.Sockets.SocketException] -and $_.Exception.InnerException.NativeErrorCode -eq 10013) {
        [Console]::Out.WriteLine('TCP_V4_REFUSED')
    } else {
        [Console]::Out.WriteLine('TCP_V4_ALLOWED')
    }
}

try {
    $u = [System.Net.Sockets.UdpClient]::new()
    $u.Connect('192.0.2.1', 53)
    $bytes = [System.Text.Encoding]::ASCII.GetBytes('FEAT3_MATRIX_UDP')
    $null = $u.Send($bytes, $bytes.Length)
    $u.Close()
    [Console]::Out.WriteLine('UDP_SEND_OK')
} catch [System.Net.Sockets.SocketException] {
    if ($_.Exception.NativeErrorCode -eq 10013) { [Console]::Out.WriteLine('UDP_SEND_REFUSED') } else { [Console]::Out.WriteLine("UDP_SEND_OTHER=$($_.Exception.NativeErrorCode)") }
} catch {
    if ($_.Exception.InnerException -is [System.Net.Sockets.SocketException] -and $_.Exception.InnerException.NativeErrorCode -eq 10013) {
        [Console]::Out.WriteLine('UDP_SEND_REFUSED')
    } else {
        [Console]::Out.WriteLine("UDP_SEND_OTHER=$($_.Exception.Message)")
    }
}

try {
    $ips = [System.Net.Dns]::GetHostAddresses('example.com')
    if ($ips.Count -gt 0) { [Console]::Out.WriteLine('DNS_ALLOWED') } else { [Console]::Out.WriteLine('DNS_EMPTY') }
} catch {
    [Console]::Out.WriteLine('DNS_REFUSED')
}

if ([int]$env:T_PORT6 -gt 0) {
    try {
        $c6 = [System.Net.Sockets.TcpClient]::new([System.Net.Sockets.AddressFamily]::InterNetworkV6)
        $c6.Connect('::1', [int]$env:T_PORT6)
        $c6.Close()
        [Console]::Out.WriteLine('IPV6_LOOPBACK_CONNECTED')
    } catch {
        [Console]::Out.WriteLine('IPV6_LOOPBACK_REFUSED')
    }
}

try {
    $c6o = [System.Net.Sockets.TcpClient]::new([System.Net.Sockets.AddressFamily]::InterNetworkV6)
    $ar6 = $c6o.BeginConnect('2001:db8::1', 80, $null, $null)
    if ($ar6.AsyncWaitHandle.WaitOne(150, $false)) {
        $c6o.EndConnect($ar6)
    }
    $c6o.Close()
    [Console]::Out.WriteLine('IPV6_OUTBOUND_ALLOWED')
} catch [System.Net.Sockets.SocketException] {
    if ($_.Exception.NativeErrorCode -eq 10013) { [Console]::Out.WriteLine('IPV6_OUTBOUND_REFUSED') } else { [Console]::Out.WriteLine("IPV6_OUTBOUND_OTHER=$($_.Exception.NativeErrorCode)") }
} catch {
    if ($_.Exception.InnerException -is [System.Net.Sockets.SocketException] -and $_.Exception.InnerException.NativeErrorCode -eq 10013) {
        [Console]::Out.WriteLine('IPV6_OUTBOUND_REFUSED')
    } else {
        [Console]::Out.WriteLine("IPV6_OUTBOUND_OTHER=$($_.Exception.Message)")
    }
}

try {
    $null = [System.IO.File]::ReadAllText('\\127.0.0.1\C$\Windows\win.ini')
    [Console]::Out.WriteLine('UNC_ALLOWED')
} catch {
    [Console]::Out.WriteLine('UNC_REFUSED')
}
"#;

        // Control 1 (`off`): loopback IPv4 TCP and UDP reach their listeners, and all matrix
        // probes (`TCP_V4_ALLOWED`, `UDP_SEND_OK`, `DNS_ALLOWED`, `UNC_ALLOWED`) succeed.
        let rig = Rig::new(IsolationLevel::Off, false);
        let result = rig
            .runner
            .execute_task(
                &win_task(
                    &rig,
                    tcp_script,
                    &[("T_PORT", &port), ("T_UDP_PORT", &udp_port)],
                    60,
                ),
                None,
                None,
            )
            .await;
        let accepted_in_off = listener.accept().is_ok();
        let mut udp_buf = [0u8; 32];
        let udp_received_in_off = udp_listener.recv_from(&mut udp_buf).is_ok();
        assert!(
            accepted_in_off
                && udp_received_in_off
                && lines(&result).contains(&"TASK_STARTED".to_string())
                && lines(&result).contains(&"CONNECTED".to_string()),
            "control under off must reach TCP and UDP listeners: tcp={accepted_in_off} udp={udp_received_in_off} {:?} / {}",
            lines(&result),
            result.stderr_str()
        );
        while listener.accept().is_ok() {}
        while udp_listener.recv_from(&mut udp_buf).is_ok() {}

        let matrix_envs = [("T_PORT6", port6.as_str())];

        let off_matrix = rig
            .runner
            .execute_task(
                &win_task(&rig, net_matrix_script, &matrix_envs, 60),
                None,
                None,
            )
            .await;
        let off_lines = lines(&off_matrix);
        for expected in [
            "TASK_STARTED",
            "TCP_V4_ALLOWED",
            "UDP_SEND_OK",
            "DNS_ALLOWED",
            "UNC_ALLOWED",
        ] {
            assert!(
                off_lines.contains(&expected.to_string()),
                "control under off must allow {expected}: {:?} / {}",
                off_lines,
                off_matrix.stderr_str()
            );
        }
        // Check whether the runner has an outbound IPv6 route in the `off` control; if not
        // (e.g. `IPV6_OUTBOUND_OTHER=10051` WSAENETUNREACH on runners without IPv6 routing),
        // skip outbound IPv6 assertions cleanly unless `RUSTY_GRID_REQUIRE_IPV6` is set, and
        // never count `IPV6_OUTBOUND_OTHER` as a pass.
        let ipv6_outbound_routable = if off_lines.contains(&"IPV6_OUTBOUND_ALLOWED".to_string()) {
            true
        } else {
            assert!(
                std::env::var_os("RUSTY_GRID_REQUIRE_IPV6").is_none(),
                "outbound IPv6 required (RUSTY_GRID_REQUIRE_IPV6) but off control could not connect: {off_lines:?}"
            );
            eprintln!("skipped outbound IPv6 check: host has no IPv6 route ({off_lines:?})");
            false
        };
        if let Some(ref l6) = listener6 {
            assert!(
                off_lines.contains(&"IPV6_LOOPBACK_CONNECTED".to_string()),
                "control under off must connect to IPv6 loopback: {:?}",
                off_lines
            );
            while l6.accept().is_ok() {}
        }

        // Control 2 (`fs`): `WinCapabilityInternetClientSid` and
        // `WinCapabilityPrivateNetworkClientServerSid` allow outbound IPv4/IPv6 TCP, UDP send,
        // and DNS resolution (`example.com`), while UNC (`\\127.0.0.1\C$\Windows\win.ini`) is
        // refused because AppContainer tokens omit user credentials / `enterpriseAuthentication`.
        let rig = Rig::new(IsolationLevel::Fs, false);
        let result = rig
            .runner
            .execute_task(
                &win_task(&rig, net_matrix_script, &matrix_envs, 60),
                None,
                None,
            )
            .await;
        let fs_lines = lines(&result);
        for expected in [
            "TASK_STARTED",
            "TCP_V4_ALLOWED",
            "UDP_SEND_OK",
            "DNS_ALLOWED",
            "UNC_REFUSED",
        ] {
            assert!(
                fs_lines.contains(&expected.to_string()),
                "control under fs expected {expected}: {:?} / {}",
                fs_lines,
                result.stderr_str()
            );
        }
        if ipv6_outbound_routable {
            assert!(
                fs_lines.contains(&"IPV6_OUTBOUND_ALLOWED".to_string()),
                "control under fs expected IPV6_OUTBOUND_ALLOWED: {:?} / {}",
                fs_lines,
                result.stderr_str()
            );
        }
        if let Some(ref l6) = listener6 {
            while l6.accept().is_ok() {}
        }

        // Under `fs+net` (zero network capabilities): non-loopback IPv4 TCP, loopback IPv4 TCP,
        // DNS (`example.com`), IPv6 loopback, IPv6 outbound, and UNC are all refused, and
        // loopback UDP datagrams (`127.0.0.1:<udp_port>`) are dropped by WFP so the listener
        // receives nothing.
        let rig = Rig::new(IsolationLevel::FsNet, false);
        let matrix_res = rig
            .runner
            .execute_task(
                &win_task(&rig, net_matrix_script, &matrix_envs, 60),
                None,
                None,
            )
            .await;
        let fsnet_lines = lines(&matrix_res);
        for expected in [
            "TASK_STARTED",
            "TCP_V4_REFUSED",
            "UDP_SEND_OK",
            "DNS_REFUSED",
            "UNC_REFUSED",
        ] {
            assert!(
                fsnet_lines.contains(&expected.to_string()),
                "fs+net must refuse {expected}: {:?} / {}",
                fsnet_lines,
                matrix_res.stderr_str()
            );
        }
        assert!(
            !fsnet_lines.contains(&"DNS_ALLOWED".to_string()),
            "fs+net must not resolve DNS: {:?}",
            fsnet_lines
        );
        if ipv6_outbound_routable {
            assert!(
                fsnet_lines.contains(&"IPV6_OUTBOUND_REFUSED".to_string())
                    && !fsnet_lines.contains(&"IPV6_OUTBOUND_ALLOWED".to_string())
                    && !fsnet_lines
                        .iter()
                        .any(|l| l.starts_with("IPV6_OUTBOUND_OTHER")),
                "fs+net must refuse IPv6 outbound with WSAEACCES (10013), not OTHER: {:?}",
                fsnet_lines
            );
        }
        if let Some(ref l6) = listener6 {
            assert!(
                fsnet_lines.contains(&"IPV6_LOOPBACK_REFUSED".to_string()),
                "fs+net must refuse IPv6 loopback: {:?}",
                fsnet_lines
            );
            match l6.accept() {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                other => {
                    panic!("IPv6 listener must see no connection from an fs+net task: {other:?}")
                }
            }
        }
        let result = rig
            .runner
            .execute_task(
                &win_task(
                    &rig,
                    tcp_script,
                    &[("T_PORT", &port), ("T_UDP_PORT", &udp_port)],
                    60,
                ),
                None,
                None,
            )
            .await;
        assert!(
            lines(&result).contains(&"TASK_STARTED".to_string())
                && lines(&result).contains(&"REFUSED".to_string()),
            "{:?} {}",
            lines(&result),
            result.stderr_str()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            other => panic!("the listener must see no connection from an fs+net task: {other:?}"),
        }
        match udp_listener.recv_from(&mut udp_buf) {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            other => panic!("the UDP listener must see no datagram from an fs+net task: {other:?}"),
        }
    }

    /// Stages a minimal self-contained Rust toolchain (`cargo.exe`, `rustc.exe`, compiler DLLs,
    /// target standard library `.rlib`s, and a vendored `cfg-if = "1.0.0"` crate) inside a
    /// temporary `user_home` directory so `cargo build --offline` tests never touch the
    /// developer's real `.rustup` or `.cargo` DACLs and never skip.
    struct StagedWinToolchain {
        _home_dir: TempDir,
        user_home: PathBuf,
        rustup_home: PathBuf,
        cargo_home: PathBuf,
        toolchain_bin: PathBuf,
    }

    fn copy_dir_files_recursive(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).expect("create staged dir");
        for entry in std::fs::read_dir(src).expect("read_dir").flatten() {
            let ty = entry.file_type().expect("file_type");
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if ty.is_dir() {
                copy_dir_files_recursive(&src_path, &dst_path);
            } else if ty.is_file() {
                std::fs::copy(&src_path, &dst_path).expect("copy toolchain file");
            }
        }
    }

    fn find_host_sysroot_read_only() -> PathBuf {
        // Locate the host toolchain sysroot without invoking rustup with mutated env vars and
        // without modifying any DACLs on the host toolchain.
        if let Some(path_var) = std::env::var_os("PATH") {
            for dir in std::env::split_paths(&path_var) {
                if dir.join("cargo.exe").is_file() {
                    // Check if `dir` itself is a toolchain `bin` dir (`.../toolchains/<tc>/bin`).
                    if let Some(tc_root) = dir.parent() {
                        if tc_root
                            .join("lib/rustlib/x86_64-pc-windows-msvc/lib")
                            .is_dir()
                        {
                            return tc_root.to_path_buf();
                        }
                        // Or `dir` is `<profile>/.cargo/bin`, so check `<profile>/.rustup/toolchains`.
                        if let Some(profile) = tc_root.parent() {
                            let toolchains = profile.join(".rustup/toolchains");
                            if let Ok(entries) = std::fs::read_dir(&toolchains) {
                                let mut candidates: Vec<PathBuf> = entries
                                    .flatten()
                                    .map(|e| e.path())
                                    .filter(|p| {
                                        p.join("bin/cargo.exe").is_file()
                                            && p.join("bin/rustc.exe").is_file()
                                            && p.join("lib/rustlib/x86_64-pc-windows-msvc/lib")
                                                .is_dir()
                                    })
                                    .collect();
                                candidates.sort();
                                if let Some(found) = candidates
                                    .iter()
                                    .find(|p| {
                                        p.file_name().and_then(|n| n.to_str()).is_some_and(|s| {
                                            s.starts_with("1.96") || s.starts_with("stable")
                                        })
                                    })
                                    .cloned()
                                    .or_else(|| candidates.into_iter().next())
                                {
                                    return found;
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Ok(out) = hidden_command("rustc")
            .args(["--print", "sysroot"])
            .output()
        {
            if out.status.success() {
                let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
                let p = PathBuf::from(s);
                if p.join("bin/cargo.exe").is_file() && p.join("bin/rustc.exe").is_file() {
                    return p;
                }
            }
        }
        cannot_run("could not locate host Rust sysroot from PATH or rustc --print sysroot");
        panic!("could not locate host Rust sysroot from PATH for staging temporary toolchain");
    }

    fn staged_toolchain() -> &'static StagedWinToolchain {
        static STAGED: std::sync::OnceLock<StagedWinToolchain> = std::sync::OnceLock::new();
        STAGED.get_or_init(|| {
            let sysroot = find_host_sysroot_read_only();
            let home_dir = tempfile::tempdir().expect("staged home tempdir");
            let user_home = rusty_grid_worker::isolation::windows::normalize_win_path(home_dir.path());

            // Populate dummy secrets inside `user_home` so `with_policy_home` verifies that
            // siblings of `.cargo` / `.rustup` inside `user_home` stay unreadable.
            std::fs::create_dir_all(user_home.join(".oxideswarm")).unwrap();
            std::fs::write(user_home.join(".oxideswarm").join("cluster.secret"), SECRET).unwrap();
            std::fs::create_dir_all(user_home.join(".ssh")).unwrap();
            std::fs::write(user_home.join(".ssh").join("test_key"), KEY).unwrap();

            let rustup_home = user_home.join(".rustup");
            let cargo_home = user_home.join(".cargo");
            let tc_dir = rustup_home
                .join("toolchains")
                .join("staged-x86_64-pc-windows-msvc");
            let tc_bin = tc_dir.join("bin");
            std::fs::create_dir_all(&tc_bin).unwrap();
            std::fs::create_dir_all(cargo_home.join("bin")).unwrap();
            let vendor_crate = cargo_home.join("vendor").join("cfg-if");
            std::fs::create_dir_all(vendor_crate.join("src")).unwrap();

            let staged_host = HostDirs::from_home(
                user_home.clone(),
                Some(rustup_home.clone()),
                Some(cargo_home.clone()),
            );
            // Prepare toolchain directory ACLs BEFORE copying files so newly created files under
            // `toolchains` and `cargo_home/{bin,vendor}` inherit `(I)(RX)` at creation time, and
            // call `prepare_worker_toolchains` again after copying to cover `config.toml` and
            // verify every descendant file carries `(RX)`.
            rusty_grid_worker::isolation::windows::prepare_worker_toolchains(
                &staged_host,
                &[],
                None,
            )
            .expect("pre-prepare staged toolchain directory ACLs");

            // Copy `cargo.exe`, `rustc.exe`, and required DLLs (`rustc_driver-*.dll`, `std-*.dll`)
            // using `std::fs::copy` (never hard_link, so DACLs remain independent of the host).
            for entry in std::fs::read_dir(sysroot.join("bin")).unwrap().flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.eq_ignore_ascii_case("cargo.exe")
                    || name_str.eq_ignore_ascii_case("rustc.exe")
                    || name_str.to_ascii_lowercase().ends_with(".dll")
                {
                    std::fs::copy(entry.path(), tc_bin.join(&name)).unwrap();
                }
            }
            std::fs::copy(tc_bin.join("cargo.exe"), cargo_home.join("bin").join("cargo.exe")).unwrap();

            // Copy target standard library (`lib/rustlib/x86_64-pc-windows-msvc`).
            let src_rustlib = sysroot
                .join("lib")
                .join("rustlib")
                .join("x86_64-pc-windows-msvc");
            let dst_rustlib = tc_dir
                .join("lib")
                .join("rustlib")
                .join("x86_64-pc-windows-msvc");
            copy_dir_files_recursive(&src_rustlib, &dst_rustlib);

            // Stage a vendored `cfg-if = "1.0.0"` dependency in `cargo_home/vendor/cfg-if` and
            // configure `cargo_home/config.toml` so `--offline` builds resolve it from `CARGO_HOME`.
            std::fs::write(
                vendor_crate.join("Cargo.toml"),
                "[package]\nname = \"cfg-if\"\nversion = \"1.0.0\"\nedition = \"2018\"\n",
            )
            .unwrap();
            std::fs::write(
                vendor_crate.join("src").join("lib.rs"),
                "#[macro_export]\nmacro_rules! cfg_if { ($($tt:tt)*) => {}; }\n",
            )
            .unwrap();
            std::fs::write(
                vendor_crate.join(".cargo-checksum.json"),
                "{\"files\":{},\"package\":\"baf1de4339761588bc0619e3cbc0120ee582ebb74b53b4efbf79117bd2da40fd\"}\n",
            )
            .unwrap();
            let vendor_root_toml = cargo_home
                .join("vendor")
                .to_string_lossy()
                .replace('\\', "/");
            std::fs::write(
                cargo_home.join("config.toml"),
                format!(
                    "[source.crates-io]\nreplace-with = \"vendored-sources\"\n\n[source.vendored-sources]\ndirectory = \"{vendor_root_toml}\"\n"
                ),
            )
            .unwrap();

            rusty_grid_worker::isolation::windows::prepare_worker_toolchains(
                &staged_host,
                &[],
                None,
            )
            .expect("prepare staged toolchain ACLs");

            let rustc_bin = tc_bin.join("rustc.exe");
            let cargo_bin = cargo_home.join("bin").join("cargo.exe");
            let tc_sid =
                rusty_grid_worker::isolation::windows::OwnedSid::oxideswarm_toolchain().unwrap();
            let user_sid =
                rusty_grid_worker::isolation::windows::OwnedSid::current_user().unwrap();
            for p in [&tc_bin, &rustc_bin, &cargo_bin] {
                assert!(
                    rusty_grid_worker::isolation::windows::dacl_already_grants_sid(
                        p,
                        tc_sid.as_psid(),
                        0x20,
                    ),
                    "expected oxideswarm.toolchain (RX) on {}: {}",
                    p.display(),
                    sddl_of(p)
                );
                assert!(
                    rusty_grid_worker::isolation::windows::dacl_already_grants_sid(
                        p,
                        user_sid.as_psid(),
                        0x20,
                    ),
                    "expected CurrentUser (RX) on {}: {}",
                    p.display(),
                    sddl_of(p)
                );
            }
            StagedWinToolchain {
                _home_dir: home_dir,
                user_home,
                rustup_home,
                cargo_home,
                toolchain_bin: tc_bin,
            }
        })
    }

    #[tokio::test]
    async fn test_cargo_build_offline_works_under_fs() {
        skip_if_duplicate_ci_pass!();
        let staged = staged_toolchain();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let mut rig = Rig::new(level, false);
            rig.host.rustup_home = staged.rustup_home.clone();
            rig.host.cargo_home = staged.cargo_home.clone();
            let config = RunnerConfig::new(SandboxConfig::new(rig.base.clone()))
                .with_isolation(level)
                .with_host_dirs(rig.host.clone());
            rig.runner = TaskRunner::new(
                uuid::Uuid::new_v4(),
                WorkerCapabilities::new("feat3", 4, 8192, false, false, None),
                config,
            );
            let sys_path = std::env::var("PATH").unwrap_or_default();
            let path = format!(
                "{};{};{sys_path}",
                rig.host.cargo_home.join("bin").to_string_lossy(),
                staged.toolchain_bin.to_string_lossy()
            );
            let rustc = staged
                .toolchain_bin
                .join("rustc.exe")
                .to_string_lossy()
                .into_owned();
            let task = win_cmd_task(
                &rig,
                r#"echo TASK_STARTED & rustc.exe -vV 2>&1 & set "CARGO_TARGET_DIR=" & set "CARGO_BUILD_TARGET_DIR=" & mkdir tiny\src & (echo [package]& echo name = "tiny"& echo version = "0.1.0"& echo edition = "2021") > tiny\Cargo.toml & (echo fn main^(^) { println!^("tiny-ran"^); }) > tiny\src\main.rs & cd tiny & cargo.exe build --offline 2>&1 & if exist target\debug\tiny.exe target\debug\tiny.exe"#,
                &[
                    ("PATH", &path),
                    ("RUSTC", &rustc),
                    ("RUSTUP_TOOLCHAIN", "staged-x86_64-pc-windows-msvc"),
                ],
                300,
            );
            let result = rig.runner.execute_task(&task, None, None).await;
            let got = lines(&result);
            let stdout = result.stdout_str();
            let stderr = result.stderr_str();
            if rusty_grid_worker::isolation::windows::is_process_elevated()
                && !got.contains(&"tiny-ran".to_string())
            {
                // Under an elevated split-token runner (`runneradmin` on `windows-latest`),
                // `\Sessions\<S>\DosDevices\<AdminLogonId>` is created by the kernel with
                // `TokenOwner = BUILTIN\Administrators` (`O:BA D:(A;OICI;...;;;SY)(A;OICI;...;;;BA)`)
                // and no ACE for `CurrentUser`. Because OxideSwarm never edits Object Manager
                // DACLs (`\??`) or grants broad SIDs, direct execution of staged `rustc.exe -vV`
                // and `cargo.exe` inside the denied `USERPROFILE` succeeds, while `cargo.exe`'s
                // internal `Command::output()` `CreateFileW(L"NUL")` (`\??\NUL`) for `Stdio::null()`
                // stdin is refused by the kernel with `ERROR_ACCESS_DENIED (os error 5)`.
                assert!(
                    got.contains(&"TASK_STARTED".to_string())
                        && stdout.contains("rustc 1.")
                        && stdout.contains("rustc.exe -vV` (never executed)")
                        && stdout.contains("Access is denied. (os error 5)"),
                    "{level} (elevated runner): expected staged rustc.exe -vV output and elevated \\??\\NUL refusal:\nstdout:\n{stdout}\nstderr:\n{stderr}"
                );
            } else {
                assert!(
                    result.is_success(),
                    "{level}: {:?}\n{stdout}\n{stderr}",
                    result.error,
                );
                assert!(
                    got.contains(&"TASK_STARTED".to_string())
                        && got.contains(&"tiny-ran".to_string()),
                    "stdout lines: {got:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cargo_build_offline_with_a_cached_dependency_under_a_denied_home() {
        skip_if_duplicate_ci_pass!();
        let staged = staged_toolchain();
        let mut rig = Rig::new(IsolationLevel::Fs, false);
        rig.host.user_home = staged.user_home.clone();
        rig.host.rustup_home = staged.rustup_home.clone();
        rig.host.cargo_home = staged.cargo_home.clone();
        let config = RunnerConfig::new(SandboxConfig::new(rig.base.clone()))
            .with_isolation(IsolationLevel::Fs)
            .with_host_dirs(rig.host.clone());
        rig.runner = TaskRunner::new(
            uuid::Uuid::new_v4(),
            WorkerCapabilities::new("feat3", 4, 8192, false, false, None),
            config,
        );
        let sys_path = std::env::var("PATH").unwrap_or_default();
        let path = format!(
            "{};{};{sys_path}",
            rig.host.cargo_home.join("bin").to_string_lossy(),
            staged.toolchain_bin.to_string_lossy()
        );
        let rustc = staged
            .toolchain_bin
            .join("rustc.exe")
            .to_string_lossy()
            .into_owned();
        let denied_secret = staged
            .user_home
            .join(".oxideswarm")
            .join("cluster.secret")
            .to_string_lossy()
            .into_owned();
        let script = r#"echo TASK_STARTED & rustc.exe -vV 2>&1 & set "CARGO_TARGET_DIR=" & set "CARGO_BUILD_TARGET_DIR=" & mkdir dep\src & (echo [package]& echo name = "dep"& echo version = "0.1.0"& echo edition = "2021"& echo [dependencies]& echo cfg-if = "=1.0.0") > dep\Cargo.toml & (echo [workspace]) >> dep\Cargo.toml & (echo version = 3& echo.& echo [[package]]& echo name = "cfg-if"& echo version = "1.0.0"& echo source = "registry+https://github.com/rust-lang/crates.io-index"& echo checksum = "baf1de4339761588bc0619e3cbc0120ee582ebb74b53b4efbf79117bd2da40fd"& echo.& echo [[package]]& echo name = "dep"& echo version = "0.1.0"& echo dependencies = ["cfg-if"]) > dep\Cargo.lock & (echo use cfg_if::cfg_if; fn main^(^) { cfg_if! { if #[cfg^(windows^)] {} } println!^("dep-ran"^); }) > dep\src\main.rs & cd dep & cargo.exe build --offline 2>&1 & (if exist target\debug\dep.exe target\debug\dep.exe) & (type %T_DENIED_SECRET% >..\.sink 2>..\.err && echo LEAK_DENIED_SECRET || echo DENY_DENIED_SECRET)"#;
        let result = rig
            .runner
            .execute_task(
                &win_cmd_task(
                    &rig,
                    script,
                    &[
                        ("PATH", &path),
                        ("RUSTC", &rustc),
                        ("RUSTUP_TOOLCHAIN", "staged-x86_64-pc-windows-msvc"),
                        ("T_DENIED_SECRET", &denied_secret),
                    ],
                    300,
                ),
                None,
                None,
            )
            .await;
        assert!(
            result.is_success(),
            "cfg-if 1.0.0: {:?}\n{}\n{}",
            result.error,
            result.stdout_str(),
            result.stderr_str()
        );
        let got = lines(&result);
        let stdout = result.stdout_str();
        let stderr = result.stderr_str();
        if rusty_grid_worker::isolation::windows::is_process_elevated()
            && !got.contains(&"dep-ran".to_string())
        {
            assert!(
                got.contains(&"TASK_STARTED".to_string())
                    && stdout.contains("rustc 1.")
                    && stdout.contains("rustc.exe -vV` (never executed)")
                    && stdout.contains("Access is denied. (os error 5)"),
                "cfg-if 1.0.0 (elevated runner): expected staged rustc.exe -vV output and elevated \\??\\NUL refusal:\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        } else {
            assert!(
                got.contains(&"TASK_STARTED".to_string()) && got.contains(&"dep-ran".to_string()),
                "stdout lines: {got:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            );
        }
        assert!(
            got.contains(&"DENY_DENIED_SECRET".to_string())
                && !got.contains(&"LEAK_DENIED_SECRET".to_string()),
            "sibling secret inside denied user_home must remain unreadable: {got:?}",
        );
    }

    #[tokio::test]
    async fn test_tracked_pid_is_the_task_and_cancel_and_timeout_still_kill_it() {
        skip_if_duplicate_ci_pass!();
        let rig = Arc::new(Rig::new(IsolationLevel::Fs, false));

        // Cancellation.
        let pid = Arc::new(AtomicU32::new(0));
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<String>();
        let handle = {
            let (rig, pid) = (Arc::clone(&rig), Arc::clone(&pid));
            tokio::spawn(async move {
                let task = win_task(&rig, "[System.Threading.Thread]::Sleep(120000)", &[], 120);
                rig.runner
                    .execute_task(&task, Some(pid), Some(cancel_rx))
                    .await
            })
        };
        let tracked = wait_for_pid(&pid).await;
        assert!(tracked > 1);
        cancel_tx.send("test".into()).unwrap();
        let result = tokio::time::timeout(WIN_WAIT_TIMEOUT, handle)
            .await
            .expect("cancellation finishes the task")
            .unwrap();
        assert_eq!(result.exit_code, EXIT_CODE_CANCELLED, "{:?}", result.error);

        // Timeout.
        let pid = Arc::new(AtomicU32::new(0));
        let task = win_task(&rig, "[System.Threading.Thread]::Sleep(120000)", &[], 2);
        let started = Instant::now();
        let result = rig
            .runner
            .execute_task(&task, Some(Arc::clone(&pid)), None)
            .await;
        assert_eq!(result.exit_code, EXIT_CODE_TIMEOUT, "{:?}", result.error);
        let elapsed = started.elapsed();
        eprintln!(
            "timeout task killed in {:.2}s (limit {}s)",
            elapsed.as_secs_f64(),
            WIN_WAIT_TIMEOUT.as_secs()
        );
        assert!(elapsed < WIN_WAIT_TIMEOUT);
        let tracked = pid.load(Ordering::Acquire);
        assert!(tracked > 1);
    }

    async fn wait_for_pid(pid: &AtomicU32) -> u32 {
        let deadline = Instant::now() + WIN_WAIT_TIMEOUT;
        loop {
            let v = pid.load(Ordering::Acquire);
            if v > 1 {
                return v;
            }
            if Instant::now() >= deadline {
                panic!(
                    "the runner never reported a child PID within {}s",
                    WIN_WAIT_TIMEOUT.as_secs()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn test_self_check_accepts_both_levels_on_windows() {
        skip_if_duplicate_ci_pass!();
        let base = tempfile::tempdir().unwrap();
        for level in [
            IsolationLevel::Off,
            IsolationLevel::Fs,
            IsolationLevel::FsNet,
        ] {
            tokio::time::timeout(
                WIN_WAIT_TIMEOUT,
                verify_isolation_support(level, base.path()),
            )
            .await
            .unwrap_or_else(|_| panic!("{level}: verify_isolation_support timed out"))
            .unwrap_or_else(|e| panic!("{level}: {e}"));
        }
    }

    #[tokio::test]
    async fn test_a_missing_program_fails_the_task_with_a_clear_message() {
        skip_if_duplicate_ci_pass!();
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
        skip_if_duplicate_ci_pass!();
        assert!(std::env::var_os("CARGO_PKG_NAME").is_some());
        let script = r#"echo TASK_STARTED& (if defined CARGO_PKG_NAME (echo WORKER_VAR=%CARGO_PKG_NAME%) else (echo WORKER_VAR=unset))& (if defined PATH (echo PATH_SET=yes) else (echo PATH_SET=no))& (if defined T_BASE (echo TASK_VAR=yes) else (echo TASK_VAR=no))& (if defined RUSTY_GRID_TASK_ID (echo ID=yes) else (echo ID=no))"#;
        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(&win_cmd_task(&control_rig, script, &[], 60), None, None)
            .await;
        assert!(
            lines(&control).contains(&"TASK_STARTED".to_string())
                && lines(&control).contains(&"WORKER_VAR=rusty_grid_worker".to_string()),
            "{:?}",
            lines(&control)
        );
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_cmd_task(&rig, script, &[], 60), None, None)
                .await;
            let got = lines(&result);
            for expected in [
                "TASK_STARTED",
                "WORKER_VAR=unset",
                "PATH_SET=yes",
                "TASK_VAR=yes",
                "ID=yes",
            ] {
                assert!(
                    got.contains(&expected.to_string()),
                    "{level}: {expected} in {got:?}"
                );
            }
        }
    }

    /// M2 on Windows: host named pipes (such as `\\.\pipe\openssh-ssh-agent` or
    /// `\\.\pipe\docker_engine`) are gated by their DACL, and non-pipe kernel objects (such as
    /// named events) are isolated by the per-task AppContainer named-object directory
    /// (`\Sessions\<id>\AppContainerNamedObjects\<TaskAppContainerSid>`).
    #[tokio::test]
    async fn test_host_named_pipes_cannot_be_connected_to_under_fs() {
        skip_if_duplicate_ci_pass!();
        let pipe_name = format!(
            "oxideswarm-feat3-pipe-{}-{}",
            std::process::id(),
            rusty_grid_core::TaskId::new().0
        );
        let event_name = format!(
            "oxideswarm-feat3-event-{}-{}",
            std::process::id(),
            rusty_grid_core::TaskId::new().0
        );
        let script = r#"
[Console]::Out.WriteLine('TASK_STARTED')
$server = [System.IO.Pipes.NamedPipeServerStream]::new($env:T_PIPE, [System.IO.Pipes.PipeDirection]::InOut, 4)
try {
    $client = [System.IO.Pipes.NamedPipeClientStream]::new('.', $env:T_PIPE, [System.IO.Pipes.PipeDirection]::InOut)
    $client.Connect(1500)
    $client.Dispose()
    [Console]::Out.WriteLine('PIPE_CONNECTED')
} catch {
    [Console]::Out.WriteLine('PIPE_REFUSED')
} finally {
    $server.Dispose()
}
"#;
        // First verify that a host named pipe and host named event created outside the
        // AppContainer are reachable under `off` and unreachable under `fs` and `fs+net`.
        let host_pipe_script = r#"
[Console]::Out.WriteLine('TASK_STARTED')
try {
    $client = [System.IO.Pipes.NamedPipeClientStream]::new('.', $env:T_PIPE, [System.IO.Pipes.PipeDirection]::InOut)
    $client.Connect(1500)
    $client.Dispose()
    [Console]::Out.WriteLine('HOST_PIPE_CONNECTED')
} catch {
    [Console]::Out.WriteLine('HOST_PIPE_REFUSED')
}
try {
    $ev = [System.Threading.EventWaitHandle]::OpenExisting($env:T_EVENT)
    $ev.Dispose()
    [Console]::Out.WriteLine('HOST_EVENT_OPENED')
} catch {
    [Console]::Out.WriteLine('HOST_EVENT_REFUSED')
}
foreach ($wellKnown in @('openssh-ssh-agent', 'docker_engine')) {
    try {
        $wk = [System.IO.Pipes.NamedPipeClientStream]::new('.', $wellKnown, [System.IO.Pipes.PipeDirection]::InOut)
        $wk.Connect(200)
        $wk.Dispose()
        [Console]::Out.WriteLine("WK_CONNECTED=$wellKnown")
    } catch {
        [Console]::Out.WriteLine("WK_REFUSED=$wellKnown")
    }
}
"#;
        let mut host_server_guard = ChildKillGuard(Some(
            hidden_command(
                r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
            )
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "$s1 = [System.IO.Pipes.NamedPipeServerStream]::new('{pipe_name}', [System.IO.Pipes.PipeDirection]::InOut, 4); \
                     $s2 = [System.IO.Pipes.NamedPipeServerStream]::new('{pipe_name}', [System.IO.Pipes.PipeDirection]::InOut, 4); \
                     $s3 = [System.IO.Pipes.NamedPipeServerStream]::new('{pipe_name}', [System.IO.Pipes.PipeDirection]::InOut, 4); \
                     $ev = [System.Threading.EventWaitHandle]::new($true, [System.Threading.EventResetMode]::ManualReset, '{event_name}'); \
                     [Console]::Out.WriteLine('READY'); [System.Threading.Thread]::Sleep(180000)"
                ),
            ])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn host pipe server"),
        ));

        // Wait until the host pipe and event instances print READY.
        let host_stdout = host_server_guard
            .0
            .as_mut()
            .and_then(|c| c.stdout.take())
            .expect("host_server stdout must be piped");
        let ready_res = tokio::time::timeout(
            WIN_WAIT_TIMEOUT,
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                let mut reader = BufReader::new(host_stdout);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader
                        .read_line(&mut line)
                        .map_err(|e| format!("read host_server stdout: {e}"))?;
                    if n == 0 {
                        return Err("host_server closed stdout before printing READY".to_string());
                    }
                    if line.trim() == "READY" {
                        return Ok(());
                    }
                }
            }),
        )
        .await;
        match ready_res {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => panic!("host pipe server failed before READY: {e}"),
            Ok(Err(e)) => panic!("host pipe server READY reader panicked: {e}"),
            Err(_) => panic!(
                "timed out after {}s waiting for host pipe server READY",
                WIN_WAIT_TIMEOUT.as_secs()
            ),
        }

        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(
                &win_task(
                    &control_rig,
                    host_pipe_script,
                    &[("T_PIPE", &pipe_name), ("T_EVENT", &event_name)],
                    60,
                ),
                None,
                None,
            )
            .await;
        let control_lines = lines(&control);
        let control_ok = control_lines.contains(&"TASK_STARTED".to_string())
            && control_lines.contains(&"HOST_PIPE_CONNECTED".to_string())
            && control_lines.contains(&"HOST_EVENT_OPENED".to_string());

        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(
                    &win_task(
                        &rig,
                        host_pipe_script,
                        &[("T_PIPE", &pipe_name), ("T_EVENT", &event_name)],
                        60,
                    ),
                    None,
                    None,
                )
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"TASK_STARTED".to_string())
                    && got.contains(&"HOST_PIPE_REFUSED".to_string())
                    && got.contains(&"HOST_EVENT_REFUSED".to_string())
                    && got.contains(&"WK_REFUSED=openssh-ssh-agent".to_string())
                    && got.contains(&"WK_REFUSED=docker_engine".to_string()),
                "{level}: host named pipes and events should be unreachable from AppContainer: {got:?} (stderr: {})",
                result.stderr_str()
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
            let self_got = lines(&self_res);
            assert!(
                self_got.contains(&"TASK_STARTED".to_string())
                    && self_got.contains(&"PIPE_REFUSED".to_string())
                    && !self_got.contains(&"PIPE_CONNECTED".to_string()),
                "{level}: {self_got:?} (stderr: {})",
                self_res.stderr_str()
            );
        }

        drop(host_server_guard);
        assert!(
            control_ok,
            "unconfined control must connect to host named pipe and event: {control_lines:?}"
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
        skip_if_duplicate_ci_pass!();
        // `PING.EXE` itself, not `cmd /C ping`: killing `cmd.exe` left `PING.EXE` running for up
        // to 120 s (TEST-88).
        let mut victim = hidden_command(r"C:\Windows\System32\PING.EXE")
            .args(["-n", "120", "127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn victim");
        let victim_pid = victim.id().to_string();
        let worker_pid = std::process::id().to_string();
        let secret_bytes: Vec<u8> = b"WORKER-MEM-SECRET-feat3c".to_vec();
        let secret_addr = (secret_bytes.as_ptr() as usize).to_string();
        let secret_len = secret_bytes.len().to_string();

        let script = format!(
            r#"{WIN_CSHARP_HELPER}
[Console]::Out.WriteLine('TASK_STARTED')
Compile-CSharp @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public static class WinProcProbe {{
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr OpenProcess(uint dwDesiredAccess, bool bInheritHandle, int dwProcessId);
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool ReadProcessMemory(IntPtr hProcess, IntPtr lpBaseAddress, byte[] lpBuffer, int dwSize, out IntPtr lpNumberOfBytesRead);
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr CreateRemoteThread(IntPtr hProcess, IntPtr lpThreadAttributes, uint dwStackSize, IntPtr lpStartAddress, IntPtr lpParameter, uint dwCreationFlags, out uint lpThreadId);
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool CloseHandle(IntPtr hObject);

    public static string TryReadWorkerMemory(int pid, ulong addr, int len) {{
        const uint PROCESS_VM_READ = 0x0010;
        const uint PROCESS_QUERY_INFORMATION = 0x0400;
        IntPtr h = OpenProcess(PROCESS_VM_READ | PROCESS_QUERY_INFORMATION, false, pid);
        if (h == IntPtr.Zero) return "RPM_OPEN_DENIED";
        try {{
            byte[] buf = new byte[len];
            IntPtr read;
            if (!ReadProcessMemory(h, (IntPtr)addr, buf, len, out read)) return "RPM_READ_DENIED";
            return "RPM_LEAK=" + Encoding.UTF8.GetString(buf, 0, (int)read);
        }} finally {{
            CloseHandle(h);
        }}
    }}

    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern IntPtr GetModuleHandle(string lpModuleName);
    [DllImport("kernel32.dll", CharSet = CharSet.Ansi, ExactSpelling = true, SetLastError = true)]
    public static extern IntPtr GetProcAddress(IntPtr hModule, string procName);

    public static string TryCreateRemoteThread(int pid) {{
        const uint PROCESS_CREATE_THREAD = 0x0002;
        const uint PROCESS_VM_OPERATION = 0x0008;
        const uint PROCESS_VM_WRITE = 0x0020;
        IntPtr h = OpenProcess(PROCESS_CREATE_THREAD | PROCESS_VM_OPERATION | PROCESS_VM_WRITE, false, pid);
        if (h == IntPtr.Zero) return "CRT_OPEN_DENIED";
        try {{
            IntPtr k32 = GetModuleHandle("kernel32.dll");
            IntPtr fnAddr = GetProcAddress(k32, "GetCurrentProcessId");
            uint tid;
            IntPtr th = CreateRemoteThread(h, IntPtr.Zero, 0, fnAddr, IntPtr.Zero, 0, out tid);
            if (th == IntPtr.Zero) return "CRT_CALL_DENIED";
            CloseHandle(th);
            return "CRT_CREATED";
        }} finally {{
            CloseHandle(h);
        }}
    }}
}}
"@
[Console]::Out.WriteLine(([WinProcProbe]::TryReadWorkerMemory([int]$env:WORKER_PID, [uint64]$env:SECRET_ADDR, [int]$env:SECRET_LEN)))
[Console]::Out.WriteLine(("WORKER_" + [WinProcProbe]::TryCreateRemoteThread([int]$env:WORKER_PID)))
[Console]::Out.WriteLine(("VICTIM_" + [WinProcProbe]::TryCreateRemoteThread([int]$env:VICTIM_PID)))
try {{
    $w = [System.Diagnostics.Process]::GetProcessById([int]$env:WORKER_PID)
    $h = $w.Handle
    [Console]::Out.WriteLine('WORKER_HANDLE_OPENED')
}} catch {{
    [Console]::Out.WriteLine('WORKER_HANDLE_DENIED')
}}
try {{
    [System.Diagnostics.Process]::GetProcessById([int]$env:VICTIM_PID).Kill()
    [Console]::Out.WriteLine('SIGNALLED')
}} catch {{
    [Console]::Out.WriteLine('DENIED')
}}
try {{
    $psi = [System.Diagnostics.ProcessStartInfo]::new("$env:SystemRoot\System32\cmd.exe", '/C ping -n 60 127.0.0.1')
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.CreateNoWindow = $true
    $own = [System.Diagnostics.Process]::Start($psi)
    $own.Kill()
    $null = $own.WaitForExit(5000)
    [Console]::Out.WriteLine('OWN_CHILD_OK')
}} catch {{
    [Console]::Out.WriteLine('OWN_CHILD_FAIL')
}}
"#
        );
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
                .execute_task(&win_task(&rig, &script, &envs, 60), None, None)
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"TASK_STARTED".to_string()),
                "{level}: task did not start: {got:?} (stderr: {})",
                result.stderr_str()
            );
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
            .execute_task(&win_task(&control_rig, &script, &envs, 60), None, None)
            .await;
        let _ = victim.kill();
        let _ = victim.wait();
        std::hint::black_box(&secret_bytes);
        let control_got = lines(&control);
        assert!(
            control_got.contains(&"TASK_STARTED".to_string())
                && control_got.contains(&"RPM_LEAK=WORKER-MEM-SECRET-feat3c".to_string())
                && control_got.contains(&"WORKER_HANDLE_OPENED".to_string())
                && control_got.contains(&"SIGNALLED".to_string()),
            "unconfined control must ReadProcessMemory on worker and terminate victim: {control_got:?}"
        );
    }

    /// M4 on Windows: console / Windows Terminal keystroke injection (`TIOCSTI` equivalent via
    /// `AttachConsole(pid)` + `WriteConsoleInputW` or inherited `CONIN$`) is blocked because
    /// AppContainer tasks are spawned with `CREATE_NO_WINDOW` and run at Low Mandatory Integrity
    /// Level. Includes a positive `IsolationLevel::Off` control against a helper console process
    /// spawned with `CREATE_NO_WINDOW` (a console without a window, TEST-88).
    #[tokio::test]
    async fn test_console_and_terminal_injection_is_blocked() {
        skip_if_duplicate_ci_pass!();
        // The helper needs a console of its own for `AttachConsole(helper)`. `CREATE_NO_WINDOW`
        // gives it one without a window; `CREATE_NEW_CONSOLE` opened a visible window per run, and
        // `cmd /C ping` left `PING.EXE` running after the kill (TEST-88).
        let mut console_helper = hidden_command(r"C:\Windows\System32\PING.EXE")
            .args(["-n", "120", "127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn helper console process");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let helper_pid = console_helper.id().to_string();
        let worker_pid = std::process::id().to_string();

        let script = format!(
            r#"{WIN_CSHARP_HELPER}
[Console]::Out.WriteLine('TASK_STARTED')
Compile-CSharp @"
using System;
using System.Runtime.InteropServices;
public static class WinConsoleProbe {{
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool FreeConsole();
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool AttachConsole(uint dwProcessId);

    public static string TryAttach(uint helperPid, uint workerPid) {{
        FreeConsole();
        if (AttachConsole(helperPid)) {{
            FreeConsole();
            return "ATTACH_HELPER_OK";
        }}
        if (AttachConsole(workerPid)) {{
            FreeConsole();
            return "ATTACH_WORKER_OK";
        }}
        if (AttachConsole(0xFFFFFFFFu)) {{
            FreeConsole();
            return "ATTACH_PARENT_OK";
        }}
        return "ATTACH_DENIED";
    }}
}}
"@
[Console]::Out.WriteLine(([WinConsoleProbe]::TryAttach([uint32]$env:HELPER_PID, [uint32]$env:WORKER_PID)))
"#
        );
        let envs = [
            ("HELPER_PID", helper_pid.as_str()),
            ("WORKER_PID", worker_pid.as_str()),
        ];

        // Positive control (`IsolationLevel::Off`): unconfined task at Medium IL can attach to
        // the helper's console.
        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(&win_task(&control_rig, &script, &envs, 60), None, None)
            .await;
        let control_got = lines(&control);

        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_task(&rig, &script, &envs, 60), None, None)
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"TASK_STARTED".to_string())
                    && got.contains(&"ATTACH_DENIED".to_string()),
                "{level}: AppContainer must not attach to helper/worker/parent console: {got:?} (stderr: {})",
                result.stderr_str()
            );
        }

        let _ = console_helper.kill();
        let _ = console_helper.wait();
        assert!(
            control_got.contains(&"TASK_STARTED".to_string())
                && (control_got.contains(&"ATTACH_HELPER_OK".to_string())
                    || control_got.contains(&"ATTACH_WORKER_OK".to_string())
                    || control_got.contains(&"ATTACH_PARENT_OK".to_string())),
            "positive control under off must attach to host console: {control_got:?} / {}",
            control.stderr_str()
        );
    }

    /// H2 on Windows: the owner's `HKCU\Software\...` registry keys cannot be read or written
    /// by a confined AppContainer task, while `HKLM` system keys remain readable.
    #[tokio::test]
    async fn test_registry_of_the_owner_is_neither_read_nor_written() {
        skip_if_duplicate_ci_pass!();
        let pid = std::process::id();
        let read_key = format!(r"HKCU\Software\OxideSwarmFeat3Read_{pid}");
        let write_key = format!(r"HKCU\Software\OxideSwarmFeat3Write_{pid}");
        let reg = |args: &[&str]| {
            hidden_command(r"C:\Windows\System32\reg.exe")
                .args(args)
                .output()
        };
        let cleanup = || {
            let _ = reg(&["delete", &read_key, "/f"]);
            let _ = reg(&["delete", &write_key, "/f"]);
        };
        assert!(
            reg(&[
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
            .is_ok_and(|o| o.status.success()),
            "failed to write test registry key {read_key}"
        );

        let script = format!(
            r#"echo TASK_STARTED&(reg.exe query HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion /v ProgramFilesDir >.sink 2>.err&&echo HKLM_READ_OK)&(reg.exe query {read_key} /v secret 2>.err||echo READ_DENIED)&(reg.exe add {write_key} /v planted /t REG_SZ /d yes /f >.sink 2>.err&&echo WRITE_RC=0||echo WRITE_RC=1)"#
        );

        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control = control_rig
            .runner
            .execute_task(&win_cmd_task(&control_rig, &script, &[], 60), None, None)
            .await;
        let control_lines = lines(&control);
        let control_reads = control_lines.contains(&"TASK_STARTED".to_string())
            && control_lines.contains(&"HKLM_READ_OK".to_string())
            && control.stdout_str().contains("owner-registry-value");
        let _ = reg(&["delete", &write_key, "/f"]);
        if !control_reads {
            cleanup();
            panic!(
                "unconfined control must read test registry key {read_key}: {} (stderr: {})",
                control.stdout_str(),
                control.stderr_str()
            );
        }

        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_cmd_task(&rig, &script, &[], 60), None, None)
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"TASK_STARTED".to_string())
                    && got.contains(&"HKLM_READ_OK".to_string()),
                "{level}: confined task must start and read HKLM: {got:?} (stderr: {})",
                result.stderr_str()
            );
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

    /// M3 + N7 on Windows: Window Station DACL for `oxideswarm.toolchain` omits
    /// `WINSTA_READCLIPBOARD` and `WINSTA_ACCESSCLIPBOARD`, each confined task runs on its own
    /// private Desktop (`ox_desk_<pid>_<run_token>`, not `Default`) and cannot open or switch to
    /// the host's `Default` desktop, and AppContainer isolation prevents reading the user's
    /// clipboard. Seeds a temporary test clipboard marker, verifies `Off` reads it, and verifies
    /// `Fs` and `FsNet` cannot read it, then restores the clipboard.
    #[tokio::test]
    async fn test_clipboard_is_not_readable() {
        skip_if_duplicate_ci_pass!();
        let marker = format!("feat3c-clip-marker-{}", rusty_grid_core::TaskId::new().0);
        let clip_helper = format!(
            r#"{WIN_CSHARP_HELPER}
Compile-CSharp @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public static class WinClipProbe {{
    const uint CF_UNICODETEXT = 13;
    const uint GMEM_MOVEABLE = 0x0002;
    const int UOI_NAME = 2;
    const uint DESKTOP_READOBJECTS = 0x0001;
    const uint DESKTOP_SWITCHDESKTOP = 0x0100;
    [DllImport("user32.dll", SetLastError = true)] public static extern bool OpenClipboard(IntPtr hWndNewOwner);
    [DllImport("user32.dll", SetLastError = true)] public static extern bool CloseClipboard();
    [DllImport("user32.dll", SetLastError = true)] public static extern bool EmptyClipboard();
    [DllImport("user32.dll", SetLastError = true)] public static extern IntPtr GetClipboardData(uint uFormat);
    [DllImport("user32.dll", SetLastError = true)] public static extern IntPtr SetClipboardData(uint uFormat, IntPtr hMem);
    [DllImport("user32.dll", SetLastError = true)] public static extern IntPtr GetThreadDesktop(uint dwThreadId);
    [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)] public static extern bool GetUserObjectInformationW(IntPtr hObj, int nIndex, byte[] pvInfo, uint nLength, out uint lpnLengthNeeded);
    [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)] public static extern IntPtr OpenDesktopW(string lpszDesktop, uint dwFlags, bool fInherit, uint dwDesiredAccess);
    [DllImport("user32.dll", SetLastError = true)] public static extern bool SwitchDesktop(IntPtr hDesktop);
    [DllImport("user32.dll", SetLastError = true)] public static extern bool CloseDesktop(IntPtr hDesktop);
    [DllImport("kernel32.dll")] public static extern uint GetCurrentThreadId();
    [DllImport("kernel32.dll", SetLastError = true)] public static extern IntPtr GlobalAlloc(uint uFlags, UIntPtr dwBytes);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern IntPtr GlobalLock(IntPtr hMem);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool GlobalUnlock(IntPtr hMem);

    public static string CurrentDesktopName() {{
        IntPtr hDesk = GetThreadDesktop(GetCurrentThreadId());
        if (hDesk == IntPtr.Zero) return "NONE";
        byte[] buf = new byte[512];
        uint needed;
        if (!GetUserObjectInformationW(hDesk, UOI_NAME, buf, (uint)buf.Length, out needed) || needed < 2) return "UNKNOWN";
        return Encoding.Unicode.GetString(buf, 0, (int)needed).TrimEnd('\0');
    }}

    public static bool CanOpenOrSwitchDefaultDesktop() {{
        IntPtr h = OpenDesktopW("Default", 0, false, DESKTOP_READOBJECTS | DESKTOP_SWITCHDESKTOP);
        if (h == IntPtr.Zero) return false;
        bool switched = SwitchDesktop(h);
        CloseDesktop(h);
        return true;
    }}

    public static string ReadText() {{
        for (int i = 0; i < 10; i++) {{
            if (OpenClipboard(IntPtr.Zero)) {{
                try {{
                    IntPtr h = GetClipboardData(CF_UNICODETEXT);
                    if (h == IntPtr.Zero) return "";
                    IntPtr p = GlobalLock(h);
                    if (p == IntPtr.Zero) return "";
                    try {{ return Marshal.PtrToStringUni(p) ?? ""; }}
                    finally {{ GlobalUnlock(h); }}
                }} finally {{ CloseClipboard(); }}
            }}
            System.Threading.Thread.Sleep(20);
        }}
        return "OPEN_DENIED";
    }}

    public static bool WriteText(string s) {{
        for (int i = 0; i < 10; i++) {{
            if (OpenClipboard(IntPtr.Zero)) {{
                try {{
                    EmptyClipboard();
                    if (string.IsNullOrEmpty(s)) return true;
                    int bytes = (s.Length + 1) * 2;
                    IntPtr h = GlobalAlloc(GMEM_MOVEABLE, (UIntPtr)bytes);
                    if (h == IntPtr.Zero) return false;
                    IntPtr p = GlobalLock(h);
                    Marshal.Copy(s.ToCharArray(), 0, p, s.Length);
                    Marshal.WriteInt16(p, s.Length * 2, 0);
                    GlobalUnlock(h);
                    return SetClipboardData(CF_UNICODETEXT, h) != IntPtr.Zero;
                }} finally {{ CloseClipboard(); }}
            }}
            System.Threading.Thread.Sleep(20);
        }}
        return false;
    }}
}}
"@
"#
        );
        let read_script = format!(
            r#"{clip_helper}
[Console]::Out.WriteLine('TASK_STARTED')
$desk = [WinClipProbe]::CurrentDesktopName()
if ($desk.StartsWith('ox_desk_')) {{ [Console]::Out.WriteLine('DESK_ISOLATED=yes') }} else {{ [Console]::Out.WriteLine("DESK_ISOLATED=$desk") }}
if (-not [WinClipProbe]::CanOpenOrSwitchDefaultDesktop()) {{ [Console]::Out.WriteLine('DEFAULT_DESK_DENIED=yes') }} else {{ [Console]::Out.WriteLine('DEFAULT_DESK_DENIED=no') }}
$v = [WinClipProbe]::ReadText()
if ($v -eq 'OPEN_DENIED' -or $v -eq '') {{
    [Console]::Out.WriteLine("CLIP_NONE=$v")
}} else {{
    [Console]::Out.WriteLine("CLIP_LEAK=$v")
}}
"#
        );
        let seed_and_read_script = format!(
            r#"{clip_helper}
[Console]::Out.WriteLine('TASK_STARTED')
$prev = [WinClipProbe]::ReadText()
if ($prev -eq 'OPEN_DENIED') {{ $prev = '' }}
[System.IO.File]::WriteAllText("$env:RUSTY_GRID_SANDBOX_DIR\prev_clip.txt", $prev)
$null = [WinClipProbe]::WriteText($env:T_MARKER)
$v = [WinClipProbe]::ReadText()
[Console]::Out.WriteLine("CLIP_LEAK=$v")
"#
        );

        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control_task = win_task(
            &control_rig,
            &seed_and_read_script,
            &[("T_MARKER", &marker)],
            60,
        );
        let control_sb = control_rig.base.join(control_task.id.0.to_string());
        let control = control_rig
            .runner
            .execute_task(&control_task, None, None)
            .await;
        let prev_clip =
            std::fs::read_to_string(control_sb.join("prev_clip.txt")).unwrap_or_default();

        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let result = rig
                .runner
                .execute_task(&win_task(&rig, &read_script, &[], 60), None, None)
                .await;
            let got = lines(&result);
            assert!(
                got.contains(&"TASK_STARTED".to_string())
                    && got.contains(&"DESK_ISOLATED=yes".to_string())
                    && got.contains(&"DEFAULT_DESK_DENIED=yes".to_string()),
                "{level}: task must run on private ox_desk_* desktop and be denied Default desktop: {got:?} (stderr: {})",
                result.stderr_str()
            );
            assert!(
                !result.stdout_str().contains(&marker)
                    && !result.stdout_str().contains("CLIP_LEAK="),
                "{level}: task read the clipboard marker: {}",
                result.stdout_str()
            );
        }

        // Restore previous clipboard contents.
        let restore_script = format!(
            r#"{clip_helper}
$null = [WinClipProbe]::WriteText($env:T_PREV)
"#
        );
        let _ = control_rig
            .runner
            .execute_task(
                &win_task(&control_rig, &restore_script, &[("T_PREV", &prev_clip)], 60),
                None,
                None,
            )
            .await;

        assert!(
            lines(&control).contains(&"TASK_STARTED".to_string())
                && control
                    .stdout_str()
                    .contains(&format!("CLIP_LEAK={marker}")),
            "positive control under off must read the seeded clipboard marker: {}",
            control.stdout_str()
        );
    }

    /// Design 0033 D4.1 + D4.6: the per-attempt AppContainer profile is named by the worker
    /// instance prefix and the FEAT-2 per-attempt run token
    /// (`oxideswarm.task.<worker8>.<run_token>`) so a retried task with the same `TaskId` gets a
    /// fresh profile that cannot collide, and `DeleteAppContainerProfile` deletes
    /// `%LOCALAPPDATA%\Packages\oxideswarm.task.<worker8>.<run_token>` when each attempt finishes.
    #[tokio::test]
    async fn test_appcontainer_profile_is_deleted_on_cleanup() {
        skip_if_duplicate_ci_pass!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let task = win_cmd_task(
            &rig,
            r#"echo TASK_STARTED& echo RUN_TOKEN=%RUSTY_GRID_RUN_TOKEN%& echo TASK_TEMP=%TEMP%& (if exist "%TEMP%\." (echo PROFILE_TEMP_EXISTS=1) else (echo PROFILE_TEMP_EXISTS=0))"#,
            &[],
            60,
        );
        let result1 = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result1.is_success()
                && lines(&result1).contains(&"TASK_STARTED".to_string())
                && lines(&result1).contains(&"PROFILE_TEMP_EXISTS=1".to_string()),
            "{:?} / {:?} / {}",
            result1.error,
            lines(&result1),
            result1.stderr_str()
        );
        // Retry the exact same `Task` (same `TaskId`): must get a distinct per-attempt run token.
        let result2 = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result2.is_success()
                && lines(&result2).contains(&"TASK_STARTED".to_string())
                && lines(&result2).contains(&"PROFILE_TEMP_EXISTS=1".to_string()),
            "{:?} / {:?} / {}",
            result2.error,
            lines(&result2),
            result2.stderr_str()
        );

        let extract_field = |res: &rusty_grid_core::TaskResult, key: &str| -> String {
            let prefix = format!("{key}=");
            lines(res)
                .iter()
                .find_map(|l| l.strip_prefix(&prefix).map(str::to_string))
                .unwrap_or_else(|| panic!("missing {key}= line in {:?}", lines(res)))
        };
        let token1 = extract_field(&result1, "RUN_TOKEN");
        let token2 = extract_field(&result2, "RUN_TOKEN");
        let temp1 = PathBuf::from(extract_field(&result1, "TASK_TEMP"));
        let temp2 = PathBuf::from(extract_field(&result2, "TASK_TEMP"));
        assert_ne!(
            token1, token2,
            "retried task with the same TaskId must get a fresh per-attempt run token"
        );

        let prefix = rusty_grid_worker::isolation::windows::worker_profile_prefix(&rig.base);
        if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA").map(PathBuf::from) {
            for (token, task_temp) in [(&token1, &temp1), (&token2, &temp2)] {
                let token_simple: String = token.chars().filter(|c| *c != '-').collect();
                let moniker = format!("{prefix}.{token_simple}");
                let pkg_dir = local_app_data.join("Packages").join(&moniker);
                assert!(
                    rusty_grid_worker::isolation::windows::path_starts_with_ci(task_temp, &pkg_dir),
                    "task %TEMP% ('{}') must reside inside per-attempt profile folder '{}' that existed during the task",
                    task_temp.display(),
                    pkg_dir.display()
                );
                let cleanup_deadline = Instant::now() + Duration::from_secs(30);
                while pkg_dir.exists() && Instant::now() < cleanup_deadline {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                assert!(
                    !pkg_dir.exists(),
                    "DeleteAppContainerProfile must remove {} on task cleanup",
                    pkg_dir.display()
                );
            }

            // N3: Verify `sweep_leftover_appcontainer_profiles_for_base` sweeps ONLY profiles
            // belonging to `rig.base`'s prefix (`oxideswarm.task.<worker8>.*`) and leaves
            // another worker's profile prefix untouched.
            let other_rig = Rig::new(IsolationLevel::Fs, false);
            let other_prefix =
                rusty_grid_worker::isolation::windows::worker_profile_prefix(&other_rig.base);
            assert_ne!(
                prefix, other_prefix,
                "distinct sandbox_base dirs must have distinct worker profile prefixes"
            );
            let other_dummy = local_app_data
                .join("Packages")
                .join(format!("{other_prefix}.deadbeef0000_dummy"));
            std::fs::create_dir_all(&other_dummy).unwrap();
            let swept =
                rusty_grid_worker::isolation::windows::sweep_leftover_appcontainer_profiles_for_base(
                    &rig.base,
                );
            assert!(
                other_dummy.exists(),
                "sweep for {} must NOT touch another worker's profile {} (swept={swept})",
                rig.base.display(),
                other_dummy.display()
            );
            let _ = std::fs::remove_dir_all(&other_dummy);
        }
    }

    /// Design 0033 D4.5 + Requirement 5 + N5:
    /// 1. Secret and config files located outside `%USERPROFILE%` (in a non-toolchain directory)
    ///    are readable under `IsolationLevel::Off` and unreadable under `Fs` and `FsNet`.
    /// 2. If a user configures `protected_files` (`--join-token-file`, `--admin-token-file`, or
    ///    `--config`) inside a granted toolchain directory (`CARGO_HOME` or `RUSTUP_HOME`), or
    ///    with an on-disk DACL granting read access to `S-1-15-2-1` (`ALL APPLICATION PACKAGES`),
    ///    the worker refuses to start at `verify_isolation_support_with_config`.
    #[tokio::test]
    async fn test_a_secret_file_outside_the_home_is_unreadable_to_a_task() {
        skip_if_duplicate_ci_pass!();
        let outside = tempfile::tempdir().unwrap();
        let etc_dir =
            rusty_grid_worker::isolation::windows::normalize_win_path(outside.path()).join("etc");
        std::fs::create_dir_all(&etc_dir).unwrap();
        let secret_file = etc_dir.join("cluster.secret");
        std::fs::write(&secret_file, "DUMMY-SECRETFILE-feat3c").unwrap();
        let config_file = etc_dir.join("rusty-grid.toml");
        std::fs::write(&config_file, "secret = \"DUMMY-CONFIG-feat3c\"").unwrap();

        let run = |level: IsolationLevel| {
            let rig = Rig::new(level, false);
            let config = RunnerConfig::new(SandboxConfig::new(rig.base.clone()))
                .with_isolation(level)
                .with_host_dirs(rig.host.clone())
                .with_protected_files(vec![secret_file.clone(), config_file.clone()]);
            let runner = TaskRunner::new(
                uuid::Uuid::new_v4(),
                WorkerCapabilities::new("feat3", 4, 8192, false, false, None),
                config,
            );
            let script = r#"echo TASK_STARTED&(type %T_A% 2>.err||echo DENY_A)&echo.&(type %T_B% 2>.err||echo DENY_B)"#;
            let task = win_cmd_task(
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

        // Control (`off`): readable outside the home.
        let open = run(IsolationLevel::Off).await;
        assert!(
            lines(&open).contains(&"TASK_STARTED".to_string())
                && open.stdout_str().contains("DUMMY-SECRETFILE-feat3c")
                && open.stdout_str().contains("DUMMY-CONFIG-feat3c"),
            "{}",
            open.stdout_str()
        );
        // Under `fs` and `fs+net`: unreadable because `etc_dir` has no AppContainer ACE.
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let result = run(level).await;
            let out = result.stdout_str();
            assert!(
                !out.contains("DUMMY-SECRETFILE-feat3c") && !out.contains("DUMMY-CONFIG-feat3c"),
                "{level}: {out}"
            );
            let got = lines(&result);
            assert!(
                got.contains(&"TASK_STARTED".to_string())
                    && got.contains(&"DENY_A".to_string())
                    && got.contains(&"DENY_B".to_string()),
                "{level}: {got:?} (stderr: {})",
                result.stderr_str()
            );
        }

        // Placing a protected file inside `CARGO_HOME` or `RUSTUP_HOME` is refused at start-up
        // and before task execution.
        let rig = Rig::new(IsolationLevel::Fs, false);
        let bad_secret = rig.host.cargo_home.join("cluster.secret");
        std::fs::write(&bad_secret, "BAD-LOCATION").unwrap();
        let err = rusty_grid_worker::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &rig.base,
            &rig.host,
            std::slice::from_ref(&bad_secret),
        )
        .await
        .expect_err("must refuse protected file inside CARGO_HOME");
        assert!(
            err.to_string().contains("CARGO_HOME"),
            "unexpected error: {err}"
        );

        // N5: A protected file outside the home that carries an `S-1-15-2-1:(R)` or
        // `S-1-15-2-2:(R)` ACE is refused at start-up by `verify_isolation_support_with_config`.
        for (sid_str, filename) in [
            ("S-1-15-2-1", "leaked_by_s_1_15_2_1.secret"),
            ("S-1-15-2-2", "leaked_by_s_1_15_2_2.secret"),
        ] {
            let world_ac_secret = etc_dir.join(filename);
            std::fs::write(&world_ac_secret, "LEAKED-BY-LEFTOVER-ACE").unwrap();
            let grant_status = hidden_command(r"C:\Windows\System32\icacls.exe")
                .arg(&world_ac_secret)
                .args(["/grant", &format!("*{sid_str}:(R)"), "/q"])
                .status()
                .expect("icacls grant on temp file");
            assert!(grant_status.success());
            let err_leaked = rusty_grid_worker::verify_isolation_support_with_config(
                IsolationLevel::Fs,
                &rig.base,
                &rig.host,
                std::slice::from_ref(&world_ac_secret),
            )
            .await
            .expect_err("must refuse protected file readable by AppContainer SID");
            assert!(
                err_leaked.to_string().contains(filename)
                    && err_leaked.to_string().contains(sid_str),
                "unexpected error for {sid_str} secret: {err_leaked}"
            );
        }

        // Live AppContainer probe test: if `<user_home>/.oxideswarm/cluster.secret` is readable
        // from inside the AppContainer (when `protected_files` is empty so the static slice check
        // passes and the live probe checks `cluster.secret`), `verify_isolation_support_with_config`
        // refuses to start with "is readable from inside the AppContainer".
        let home_secret = rig
            .host
            .user_home
            .join(".oxideswarm")
            .join("cluster.secret");
        for sid_grant in ["*S-1-1-0:(R)", "*S-1-15-2-1:(R)"] {
            let grant_home_secret = hidden_command(r"C:\Windows\System32\icacls.exe")
                .arg(&home_secret)
                .args(["/grant", sid_grant, "/q"])
                .status()
                .expect("icacls grant on home cluster.secret");
            assert!(grant_home_secret.success());
        }
        let err_probe = rusty_grid_worker::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &rig.base,
            &rig.host,
            &[],
        )
        .await
        .expect_err("live probe must refuse readable home cluster.secret");
        for sid_remove in ["*S-1-1-0", "*S-1-15-2-1"] {
            let _ = hidden_command(r"C:\Windows\System32\icacls.exe")
                .arg(&home_secret)
                .args(["/remove", sid_remove, "/q"])
                .status();
        }
        assert!(
            err_probe
                .to_string()
                .contains("is readable from inside the AppContainer"),
            "unexpected error from live AppContainer probe: {err_probe}"
        );
    }

    fn sddl_of(path: &Path) -> String {
        let out = hidden_command(r"C:\Windows\System32\icacls.exe")
            .arg(path)
            .output()
            .expect("icacls");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Attack Test 1 (C2): A task sets `CARGO_HOME` and `RUSTUP_HOME` in `TaskSpec::Command.env`
    /// to `<temp_home>/.ssh` and `<base>/other_task`. Verify that the DACLs on `<temp_home>/.ssh`
    /// and `<base>/other_task` are NOT modified and the task CANNOT read files inside them.
    #[tokio::test]
    async fn test_attack_task_chosen_cargo_and_rustup_home_never_modifies_dacls_or_leaks() {
        skip_if_duplicate_ci_pass!();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            rusty_grid_worker::isolation::windows::prepare_worker_toolchains(
                &rig.host,
                &[],
                Some(&rig.base),
            )
            .expect("prepare_worker_toolchains");
            let ssh_dir = rig.home.path().join(".ssh");
            let other_task_dir = rig.base.join("other_task");
            let ssh_acl_before = sddl_of(&ssh_dir);
            let other_acl_before = sddl_of(&other_task_dir);
            let tc_sid = rusty_grid_worker::isolation::windows::oxideswarm_toolchain_sid_string()
                .expect("toolchain SID string");
            assert!(!ssh_acl_before.contains(&tc_sid));
            assert!(!other_acl_before.contains(&tc_sid));

            let script = r#"echo TASK_STARTED& (type %CARGO_HOME%\test_key >.sink 2>.err && echo LEAK_SSH || echo DENY_SSH)& (type %RUSTUP_HOME%\secret.txt >.sink 2>.err && echo LEAK_OTHER || echo DENY_OTHER)"#;
            let task = win_cmd_task(
                &rig,
                script,
                &[
                    ("CARGO_HOME", ssh_dir.to_str().unwrap()),
                    ("RUSTUP_HOME", other_task_dir.to_str().unwrap()),
                ],
                60,
            );
            let result = rig.runner.execute_task(&task, None, None).await;
            assert!(
                !result.is_success(),
                "{level}: task with CARGO_HOME/RUSTUP_HOME outside sandbox must be refused"
            );
            assert!(
                result.stderr_str().contains("CARGO_HOME"),
                "{level}: expected clear CARGO_HOME error, got: {}",
                result.stderr_str()
            );
            let got = lines(&result);
            assert!(
                !got.iter().any(|l| l.starts_with("LEAK_")),
                "{level}: leaked: {got:?}"
            );
            assert_eq!(
                sddl_of(&ssh_dir),
                ssh_acl_before,
                "{level}: DACL on .ssh was modified by task-chosen CARGO_HOME"
            );
            assert_eq!(
                sddl_of(&other_task_dir),
                other_acl_before,
                "{level}: DACL on other_task was modified by task-chosen RUSTUP_HOME"
            );

            // Conversely, a task-chosen CARGO_HOME inside its own sandbox IS permitted.
            let mut allow_task = win_cmd_task(
                &rig,
                r#"echo TASK_STARTED& echo ALLOWED_CARGO=%CARGO_HOME%"#,
                &[],
                60,
            );
            let sb_cargo = rig.base.join(allow_task.id.0.to_string()).join("my_cargo");
            if let TaskSpec::Command { ref mut env, .. } = allow_task.spec {
                env.insert("CARGO_HOME".into(), sb_cargo.to_string_lossy().into_owned());
            }
            let allow_res = rig.runner.execute_task(&allow_task, None, None).await;
            let allow_got = lines(&allow_res);
            assert!(
                allow_res.is_success()
                    && allow_got.contains(&"TASK_STARTED".to_string())
                    && allow_res.stdout_str().contains("my_cargo"),
                "{level}: CARGO_HOME inside sandbox must be allowed: {:?} / {}",
                allow_res.error,
                allow_res.stderr_str()
            );
        }
    }

    /// Attack Test 2 (C2): A task sets `TaskSpec::Command.program` to an executable inside an
    /// arbitrary directory `<outside>/custom_prog_dir/myprog.exe` next to
    /// `<outside>/custom_prog_dir/secret.txt`. Verify that `<outside>/custom_prog_dir`'s DACL is
    /// NOT modified, the out-of-sandbox program path is refused by policy, and a confined task
    /// that does run (`TASK_STARTED`) cannot read `secret.txt`.
    #[tokio::test]
    async fn test_attack_task_chosen_program_path_never_modifies_dacl_or_leaks_sibling_secret() {
        skip_if_duplicate_ci_pass!();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            let rig = Rig::new(level, false);
            let custom_dir =
                rusty_grid_worker::isolation::windows::normalize_win_path(rig._outside.path())
                    .join("custom_prog_dir");
            std::fs::create_dir_all(&custom_dir).unwrap();
            let myprog = custom_dir.join("myprog.exe");
            std::fs::copy(r"C:\Windows\System32\cmd.exe", &myprog).unwrap();
            let sibling_secret = custom_dir.join("secret.txt");
            std::fs::write(&sibling_secret, "CUSTOM-PROG-SIBLING-SECRET").unwrap();

            let dacl_before = sddl_of(&custom_dir);
            let secret_dacl_before = sddl_of(&sibling_secret);

            let task = Task::new(
                TaskSpec::Command {
                    program: myprog.to_string_lossy().into_owned(),
                    args: vec![
                        "/C".into(),
                        format!("type \"{}\"", sibling_secret.display()),
                    ],
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: None,
                },
                TaskRequirements::generic(1, 30),
            );
            let result = rig.runner.execute_task(&task, None, None).await;
            assert!(
                !result.is_success()
                    && result.stderr_str().contains(
                        "lies outside the task sandbox, worker toolchain directories, and system directories readable by AppContainers"
                    ),
                "{level}: expected program-path policy refusal, got: {:?}",
                result.stderr_str()
            );
            assert!(
                !result.stdout_str().contains("CUSTOM-PROG-SIBLING-SECRET"),
                "{level}: task read sibling secret in custom_prog_dir: {}",
                result.stdout_str()
            );
            assert_eq!(
                sddl_of(&custom_dir),
                dacl_before,
                "{level}: DACL on custom_prog_dir was modified by task program path"
            );
            assert_eq!(
                sddl_of(&sibling_secret),
                secret_dacl_before,
                "{level}: DACL on secret.txt was modified by task program path"
            );

            // Also run a positive confined task on the same rig to prove confined execution works
            // (`TASK_STARTED`) and `sibling_secret` remains unreadable (`DENY_SIBLING_SECRET`).
            let probe = win_cmd_task(
                &rig,
                r#"echo TASK_STARTED& (type %T_SIBLING_SECRET% >.sink 2>.err && echo LEAK_SIBLING_SECRET || echo DENY_SIBLING_SECRET)"#,
                &[("T_SIBLING_SECRET", sibling_secret.to_str().unwrap())],
                60,
            );
            let probe_res = rig.runner.execute_task(&probe, None, None).await;
            let probe_got = lines(&probe_res);
            assert!(
                probe_res.is_success()
                    && probe_got.contains(&"TASK_STARTED".to_string())
                    && probe_got.contains(&"DENY_SIBLING_SECRET".to_string())
                    && !probe_res.stdout_str().contains("CUSTOM-PROG-SIBLING-SECRET"),
                "{level}: confined probe must start and be denied sibling_secret: {probe_got:?} (stderr: {})",
                probe_res.stderr_str()
            );
        }
    }

    /// Attack Test 3 (H1): A task creates a directory junction (`cmd /c mklink /J ...`) and
    /// attempts to create a hard link (`CreateHardLinkW`) inside its sandbox pointing to
    /// `<temp_home>/.ssh` and `<temp_home>/.oxideswarm/cluster.secret`, including across a retry.
    /// Verifies that:
    /// 1. Under `IsolationLevel::Off` (control), both the junction and the hard link are created
    ///    (`JUNCTION_EXISTS`, `HARDLINK_CREATED`) and read the secrets (`LEAK_JUNCTION`, `LEAK_HARDLINK`).
    /// 2. Under `IsolationLevel::Fs` (across 2 attempts of the same `TaskId`), the junction IS
    ///    created as a reparse point inside the sandbox (`JUNCTION_EXISTS` + host verification),
    ///    reading through the junction is denied (`DENY_JUNCTION`), `CreateHardLinkW` to the
    ///    denied secret fails with `ERROR_ACCESS_DENIED` (`HARDLINK_CREATE_DENIED=5`), and
    ///    neither `.ssh`'s nor `cluster.secret`'s DACL is modified.
    #[tokio::test]
    async fn test_attack_junctions_and_hardlinks_inside_sandbox_cannot_expose_denied_secret() {
        skip_if_duplicate_ci_pass!();
        let script = format!(
            r#"{WIN_CSHARP_HELPER}
[Console]::Out.WriteLine('TASK_STARTED')
Compile-CSharp @"
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
public static class WinLinkProbe {{
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern bool CreateHardLinkW(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateFileW(
        string lpFileName, uint dwDesiredAccess, uint dwShareMode, IntPtr lpSecurityAttributes,
        uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool DeviceIoControl(
        IntPtr hDevice, uint dwIoControlCode, byte[] lpInBuffer, int nInBufferSize,
        IntPtr lpOutBuffer, int nOutBufferSize, out int lpBytesReturned, IntPtr lpOverlapped);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr hObject);

    public static string CreateJunction(string junctionPath, string targetDir) {{
        Directory.CreateDirectory(junctionPath);
        const uint GENERIC_WRITE = 0x40000000;
        const uint OPEN_EXISTING = 3;
        const uint FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000;
        const uint FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
        const uint FSCTL_SET_REPARSE_POINT = 0x000900A4;
        const uint IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003;

        IntPtr h = CreateFileW(
            junctionPath, GENERIC_WRITE, 0, IntPtr.Zero, OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, IntPtr.Zero);
        if (h == new IntPtr(-1)) {{
            return "OPEN_ERR=" + Marshal.GetLastWin32Error();
        }}
        try {{
            string subName = @"\??\" + targetDir;
            string printName = targetDir;
            byte[] subBytes = Encoding.Unicode.GetBytes(subName);
            byte[] printBytes = Encoding.Unicode.GetBytes(printName);
            int pathBufferLen = subBytes.Length + 2 + printBytes.Length + 2;
            int dataLen = 8 + pathBufferLen;
            byte[] buf = new byte[8 + dataLen];
            BitConverter.GetBytes(IO_REPARSE_TAG_MOUNT_POINT).CopyTo(buf, 0);
            BitConverter.GetBytes((ushort)dataLen).CopyTo(buf, 4);
            BitConverter.GetBytes((ushort)0).CopyTo(buf, 6);
            BitConverter.GetBytes((ushort)0).CopyTo(buf, 8);
            BitConverter.GetBytes((ushort)subBytes.Length).CopyTo(buf, 10);
            BitConverter.GetBytes((ushort)(subBytes.Length + 2)).CopyTo(buf, 12);
            BitConverter.GetBytes((ushort)printBytes.Length).CopyTo(buf, 14);
            Array.Copy(subBytes, 0, buf, 16, subBytes.Length);
            Array.Copy(printBytes, 0, buf, 16 + subBytes.Length + 2, printBytes.Length);
            int returned;
            if (!DeviceIoControl(h, FSCTL_SET_REPARSE_POINT, buf, buf.Length, IntPtr.Zero, 0, out returned, IntPtr.Zero)) {{
                int err = Marshal.GetLastWin32Error();
                CloseHandle(h);
                h = new IntPtr(-1);
                try {{ Directory.Delete(junctionPath); }} catch {{}}
                return "IOCTL_ERR=" + err;
            }}
            return "OK";
        }} finally {{
            if (h != new IntPtr(-1)) CloseHandle(h);
        }}
    }}
}}
"@
$sb = $env:RUSTY_GRID_SANDBOX_DIR
if (-not [System.IO.Directory]::Exists("$sb\j_ssh")) {{
    $jRes = [WinLinkProbe]::CreateJunction("$sb\j_ssh", $env:T_SSH_DIR)
    [Console]::Out.WriteLine("JUNCTION_CREATE=$jRes")
}}
if (-not [System.IO.File]::Exists("$sb\hl_secret.txt")) {{
    if ([WinLinkProbe]::CreateHardLinkW("$sb\hl_secret.txt", $env:T_SECRET, [IntPtr]::Zero)) {{
        [Console]::Out.WriteLine('HARDLINK_CREATED')
    }} else {{
        $err = [System.Runtime.InteropServices.Marshal]::GetLastWin32Error()
        [Console]::Out.WriteLine("HARDLINK_CREATE_DENIED=$err")
    }}
}}
if ($env:T_WAIT_HOST_JUNCTION -eq '1') {{
    [System.IO.File]::WriteAllText("$sb\ready.txt", "1")
    $goDeadline = [DateTime]::UtcNow.AddSeconds(60)
    while ([DateTime]::UtcNow -lt $goDeadline) {{
        if ([System.IO.File]::Exists("$sb\go.txt")) {{ break }}
        [System.Threading.Thread]::Sleep(25)
    }}
}}
if ([System.IO.Directory]::Exists("$sb\j_ssh")) {{
    $attrs = [System.IO.File]::GetAttributes("$sb\j_ssh")
    if (($attrs -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {{
        [Console]::Out.WriteLine('JUNCTION_EXISTS')
    }}
}}
if ([System.IO.File]::Exists("$sb\hl_secret.txt")) {{
    [Console]::Out.WriteLine('HARDLINK_CREATED')
}}
try {{ $null = [System.IO.File]::ReadAllText("$sb\j_ssh\test_key"); [Console]::Out.WriteLine('LEAK_JUNCTION') }} catch {{ [Console]::Out.WriteLine('DENY_JUNCTION') }}
if ([System.IO.File]::Exists("$sb\hl_secret.txt")) {{
    try {{ $null = [System.IO.File]::ReadAllText("$sb\hl_secret.txt"); [Console]::Out.WriteLine('LEAK_HARDLINK') }} catch {{ [Console]::Out.WriteLine('DENY_HARDLINK') }}
}}
"#
        );

        // Positive control (`IsolationLevel::Off`): both the junction and the hard link are
        // created and both secrets can be read through them.
        let control_rig = Rig::new(IsolationLevel::Off, false);
        let control_ssh = control_rig.home.path().join(".ssh");
        let control_res = control_rig
            .runner
            .execute_task(
                &win_task(
                    &control_rig,
                    &script,
                    &[("T_SSH_DIR", control_ssh.to_str().unwrap())],
                    60,
                ),
                None,
                None,
            )
            .await;
        let control_got = lines(&control_res);
        for expected in [
            "TASK_STARTED",
            "JUNCTION_EXISTS",
            "HARDLINK_CREATED",
            "LEAK_JUNCTION",
            "LEAK_HARDLINK",
        ] {
            assert!(
                control_got.contains(&expected.to_string()),
                "control under off must produce {expected}: {control_got:?} / {}",
                control_res.stderr_str()
            );
        }

        let rig = Rig::new(IsolationLevel::Fs, false);
        let ssh_dir = rig.home.path().join(".ssh");
        let secret_file = rig.home.path().join(".oxideswarm/cluster.secret");
        let ssh_dacl_before = sddl_of(&ssh_dir);
        let secret_dacl_before = sddl_of(&secret_file);

        let task = win_task(
            &rig,
            &script,
            &[("T_SSH_DIR", ssh_dir.to_str().unwrap())],
            60,
        );

        // Attempt 1: inside the AppContainer (`IsolationLevel::Fs`), the Windows NTFS kernel
        // (`NtfsSetReparsePoint` / `NtSetInformationFile`) refuses both `FSCTL_SET_REPARSE_POINT`
        // (`JUNCTION_CREATE=IOCTL_ERR=5`) and `CreateHardLinkW` (`HARDLINK_CREATE_DENIED=5`) with
        // `ERROR_ACCESS_DENIED` (5), whereas under `IsolationLevel::Off` above both succeeded.
        let result1 = rig.runner.execute_task(&task, None, None).await;
        assert!(
            result1.is_success(),
            "attempt 1: {:?} {}",
            result1.error,
            result1.stderr_str()
        );
        let got1 = lines(&result1);
        assert!(
            got1.contains(&"TASK_STARTED".to_string())
                && got1.contains(&"JUNCTION_CREATE=IOCTL_ERR=5".to_string())
                && got1.contains(&"HARDLINK_CREATE_DENIED=5".to_string())
                && got1.contains(&"DENY_JUNCTION".to_string())
                && !got1.iter().any(|l| l.starts_with("LEAK_")),
            "attempt 1: expected IOCTL_ERR=5 and HARDLINK_CREATE_DENIED=5 inside AppContainer: {got1:?}"
        );
        assert_eq!(sddl_of(&ssh_dir), ssh_dacl_before);
        assert_eq!(sddl_of(&secret_file), secret_dacl_before);

        // Attempt 2: run the task while the host plants BOTH a real NTFS junction
        // `<sandbox_dir>\j_ssh -> <temp_home>\.ssh` AND a real NTFS hard link
        // `<sandbox_dir>\hl_secret.txt -> <temp_home>\.oxideswarm\cluster.secret` inside the
        // active sandbox directory and re-runs `prepare_worker_toolchains`, verifying that
        // `JUNCTION_EXISTS` and `HARDLINK_CREATED` are both confirmed on the host and inside the
        // task, reading both `j_ssh\test_key` and `hl_secret.txt` is denied (`DENY_JUNCTION`,
        // `DENY_HARDLINK`), and neither `.ssh`'s nor `cluster.secret`'s DACL is modified.
        let task2 = win_task(
            &rig,
            &script,
            &[
                ("T_SSH_DIR", ssh_dir.to_str().unwrap()),
                ("T_WAIT_HOST_JUNCTION", "1"),
            ],
            90,
        );
        let sb_dir2 = rig.base.join(task2.id.0.to_string());
        let ssh_dir_clone = ssh_dir.clone();
        let secret_file_clone = secret_file.clone();
        let host_clone = rig.host.clone();
        let base_clone = rig.base.clone();
        let task2_done = Arc::new(AtomicBool::new(false));
        let task2_done_exec = Arc::clone(&task2_done);
        let exec_fut = async {
            let res = rig.runner.execute_task(&task2, None, None).await;
            task2_done_exec.store(true, Ordering::Release);
            res
        };
        let plant_fut = async move {
            let ready = sb_dir2.join("ready.txt");
            if let Err(err) =
                wait_for_barrier("task2 ready.txt", &[("task2", task2_done.as_ref())], || {
                    ready.exists()
                })
                .await
            {
                return Err(format!("task2 must signal ready.txt: {err}"));
            }
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                let j_path = sb_dir2.join("j_ssh");
                let hl_path = sb_dir2.join("hl_secret.txt");
                let ps_mklink = hidden_command(
                    r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
                )
                .args([
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "New-Item -ItemType Junction -Path $env:J_LINK -Value $env:J_TARGET | Out-Null; New-Item -ItemType HardLink -Path $env:HL_LINK -Value $env:HL_TARGET | Out-Null",
                ])
                .env("J_LINK", &j_path)
                .env("J_TARGET", &ssh_dir_clone)
                .env("HL_LINK", &hl_path)
                .env("HL_TARGET", &secret_file_clone)
                .output()
                .map_err(|e| format!("create host junction and hardlink inside active sandbox: {e}"))?;
                if !ps_mklink.status.success() {
                    return Err(format!(
                        "host New-Item Junction/HardLink failed: {}",
                        String::from_utf8_lossy(&ps_mklink.stderr)
                    ));
                }
                let j_meta = std::fs::symlink_metadata(&j_path)
                    .map_err(|e| format!("junction j_ssh must exist inside sandbox on host: {e}"))?;
                {
                    use std::os::windows::fs::MetadataExt;
                    if (j_meta.file_attributes() & 0x0400) == 0 {
                        return Err("j_ssh must have FILE_ATTRIBUTE_REPARSE_POINT".to_string());
                    }
                }
                if !hl_path.is_file() {
                    return Err("hardlink hl_secret.txt must exist inside sandbox on host".to_string());
                }
                rusty_grid_worker::isolation::windows::prepare_worker_toolchains(
                    &host_clone,
                    &[],
                    Some(&base_clone),
                )
                .map_err(|e| {
                    format!("prepare_worker_toolchains with active junction and hardlink: {e}")
                })?;
                std::fs::write(sb_dir2.join("go.txt"), "1")
                    .map_err(|e| format!("write go.txt: {e}"))?;
                Ok(())
            })
            .await
            .map_err(|e| format!("plant_fut join error: {e}"))?
        };
        let (result2, plant_res) = tokio::join!(exec_fut, plant_fut);
        if let Err(err) = plant_res {
            panic!("{err}; attempt 2: {}", format_result_diag(&result2));
        }
        assert!(
            result2.is_success(),
            "attempt 2: {}",
            format_result_diag(&result2)
        );
        let got2 = lines(&result2);
        assert!(
            got2.contains(&"TASK_STARTED".to_string())
                && got2.contains(&"JUNCTION_EXISTS".to_string())
                && got2.contains(&"HARDLINK_CREATED".to_string())
                && got2.contains(&"DENY_JUNCTION".to_string())
                && got2.contains(&"DENY_HARDLINK".to_string())
                && !got2.iter().any(|l| l.starts_with("LEAK_")),
            "attempt 2: both junction and hardlink must exist and both reads must be denied: {got2:?}"
        );
        assert_eq!(
            sddl_of(&ssh_dir),
            ssh_dacl_before,
            "attempt 2: target .ssh DACL was modified via junction"
        );
        assert_eq!(
            sddl_of(&secret_file),
            secret_dacl_before,
            "attempt 2: target cluster.secret DACL was modified"
        );
    }

    /// Attack Test 4 (H2 + H3 + N4 + Requirement 6):
    /// 1. Verifies that `revoke_oxideswarm_toolchain_acls` removes the `DENY_ACCESS` ACE on
    ///    `<cargo_home>/credentials.toml` even when `credentials.toml` is NOT replaced first, and
    ///    restores the original SDDL on `sandbox_base`.
    /// 2. Creates temporary staging files inside `<cargo_home>/registry` (which inherits
    ///    `oxideswarm.toolchain:(RX)`), proves in a task that those files ARE readable before the
    ///    rename (`READABLE_SECRET_TMP`, `READABLE_CRED_TMP`), then atomically renames them via
    ///    `std::fs::rename` over `<temp_home>/.oxideswarm/cluster.secret` and
    ///    `<temp_cargo_home>/credentials.toml` while the worker is running, and verifies that the
    ///    subsequent task on the same `TaskRunner` cannot read either secret.
    #[tokio::test]
    async fn test_attack_secret_replaced_by_rename_stays_unreadable_and_revoke_cleans_up() {
        skip_if_duplicate_ci_pass!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let orig_base_sddl = rusty_grid_worker::isolation::windows::get_path_dacl_sddl(&rig.base)
            .expect("orig_base_sddl");
        let cargo_cred = rig.host.cargo_home.join("credentials.toml");
        std::fs::write(&cargo_cred, "token = \"initial-cargo-cred\"").unwrap();
        let registry_dir = rig.host.cargo_home.join("registry");
        std::fs::create_dir_all(&registry_dir).unwrap();

        let probe_script = r#"echo TASK_STARTED&(type %T_SECRET% >.sink 2>.err&&echo LEAK_CLUSTER_SECRET||echo DENY_CLUSTER_SECRET)&(type %T_CARGO_CRED% >.sink 2>.err&&echo LEAK_CARGO_CRED||echo DENY_CARGO_CRED)"#;
        let envs = [("T_CARGO_CRED", cargo_cred.to_str().unwrap())];

        // First task triggers `prepare_worker_toolchains`, placing a DENY_ACCESS ACE on `credentials.toml`.
        let res1 = rig
            .runner
            .execute_task(&win_cmd_task(&rig, probe_script, &envs, 60), None, None)
            .await;
        assert!(res1.is_success());
        assert_eq!(
            lines(&res1),
            vec!["TASK_STARTED", "DENY_CLUSTER_SECRET", "DENY_CARGO_CRED"]
        );

        let tc_sid = rusty_grid_worker::isolation::windows::oxideswarm_toolchain_sid_string()
            .expect("toolchain SID string");
        let cred_sddl_after_prepare = sddl_of(&cargo_cred);
        assert!(
            cred_sddl_after_prepare.contains(&tc_sid),
            "credentials.toml must have explicit deny ACE for {tc_sid}: {cred_sddl_after_prepare}"
        );

        // N4: Test `revoke_oxideswarm_toolchain_acls` on `credentials.toml` BEFORE any rename
        // replaces it, proving that `DeleteAce` removes `ACCESS_DENIED_ACE_TYPE` entries too, and
        // that `sandbox_base`'s original SDDL is restored.
        let manifest = rusty_grid_worker::isolation::windows::acl_grant_manifest_path(&rig.host);
        assert!(
            manifest.is_file(),
            "expected ACL grant manifest at {}",
            manifest.display()
        );
        let revoked_first =
            rusty_grid_worker::isolation::windows::revoke_oxideswarm_toolchain_acls(&manifest)
                .expect("revoke_oxideswarm_toolchain_acls before rename");
        assert!(
            revoked_first.contains(&cargo_cred),
            "revoked list must include credentials.toml: {revoked_first:?}"
        );
        for p in &revoked_first {
            let sddl = sddl_of(p);
            assert!(
                !sddl.contains(&tc_sid),
                "revoked path {} (without prior rename) still contains toolchain SID {tc_sid}: {sddl}",
                p.display()
            );
        }
        let restored_base_sddl =
            rusty_grid_worker::isolation::windows::get_path_dacl_sddl(&rig.base)
                .expect("restored_base_sddl");
        assert_eq!(
            restored_base_sddl, orig_base_sddl,
            "revoke must restore sandbox_base's original DACL SDDL"
        );
        // Also verify `restore_path_dacl_sddl` preserves a non-`AI` (`D:(...)` without
        // `SE_DACL_AUTO_INHERITED`) SDDL byte-for-byte, matching `windows-latest`.
        let non_ai_sddl = orig_base_sddl.replacen("D:AI(", "D:(", 1);
        rusty_grid_worker::isolation::windows::restore_path_dacl_sddl(&rig.base, &non_ai_sddl)
            .expect("restore non-AI SDDL");
        assert_eq!(
            rusty_grid_worker::isolation::windows::get_path_dacl_sddl(&rig.base).unwrap(),
            non_ai_sddl,
            "restore_path_dacl_sddl must preserve non-AI SDDL byte-for-byte"
        );
        rusty_grid_worker::isolation::windows::restore_path_dacl_sddl(&rig.base, &orig_base_sddl)
            .expect("restore orig_base_sddl");

        // Stage `.tmp` files inside `<cargo_home>/registry` (which receives inheritable `(OI)(CI)(RX)`
        // for `oxideswarm.toolchain`) and prove in a task BEFORE the rename that both `.tmp`
        // files ARE readable by the AppContainer task.
        let secret_tmp = registry_dir.join("cluster.secret.tmp");
        let cred_tmp = registry_dir.join("credentials.toml.tmp");
        std::fs::write(&secret_tmp, "rotated-cluster-secret-value").unwrap();
        std::fs::write(&cred_tmp, "token = \"rotated-cargo-cred-value\"").unwrap();

        let pre_rename_script = r#"echo TASK_STARTED&(findstr rotated-cluster-secret-value %T_SECRET_TMP% >.sink 2>.err&&echo READABLE_SECRET_TMP||echo UNREADABLE_SECRET_TMP)&(findstr rotated-cargo-cred-value %T_CRED_TMP% >.sink 2>.err&&echo READABLE_CRED_TMP||echo UNREADABLE_CRED_TMP)"#;
        let pre_res = rig
            .runner
            .execute_task(
                &win_cmd_task(
                    &rig,
                    pre_rename_script,
                    &[
                        ("T_SECRET_TMP", secret_tmp.to_str().unwrap()),
                        ("T_CRED_TMP", cred_tmp.to_str().unwrap()),
                    ],
                    60,
                ),
                None,
                None,
            )
            .await;
        assert!(pre_res.is_success());
        assert_eq!(
            lines(&pre_res),
            vec!["TASK_STARTED", "READABLE_SECRET_TMP", "READABLE_CRED_TMP"],
            "staging files inside <cargo_home>/registry must be readable BEFORE the rename: {:?}",
            lines(&pre_res)
        );

        // Now atomically rename those readable `.tmp` files over `<temp_home>/.oxideswarm/cluster.secret`
        // and `<temp_cargo_home>/credentials.toml` without restarting `TaskRunner`.
        let secret_path = rig.home.path().join(".oxideswarm/cluster.secret");
        std::fs::rename(&secret_tmp, &secret_path).unwrap();
        std::fs::rename(&cred_tmp, &cargo_cred).unwrap();

        // Subsequent task on the SAME running `TaskRunner` must still be denied both secrets.
        let res2 = rig
            .runner
            .execute_task(&win_cmd_task(&rig, probe_script, &envs, 60), None, None)
            .await;
        assert!(res2.is_success());
        assert_eq!(
            lines(&res2),
            vec!["TASK_STARTED", "DENY_CLUSTER_SECRET", "DENY_CARGO_CRED"],
            "rotated secrets after atomic rename from readable .tmp files must be unreadable: {:?}",
            lines(&res2)
        );
        assert!(
            !res2.stdout_str().contains("rotated-cluster-secret-value")
                && !res2.stdout_str().contains("rotated-cargo-cred-value")
        );

        let revoked_after =
            rusty_grid_worker::isolation::windows::revoke_oxideswarm_toolchain_acls(&manifest)
                .expect("revoke_oxideswarm_toolchain_acls after rename");
        assert!(!revoked_after.is_empty());
        for p in &revoked_after {
            let sddl = sddl_of(p);
            assert!(
                !sddl.contains(&tc_sid),
                "revoked path {} still contains toolchain SID {tc_sid}: {sddl}",
                p.display()
            );
        }
    }

    /// N2 + N4: Runs `windows/revoke-isolation-acls.ps1 -DryRun` with a temporary manifest and
    /// asserts that `DeriveCapabilitySidsFromName` succeeds (`kernelbase.dll`) and that `-DryRun`
    /// lists every recorded ACL grant and `SDDL|` restore entry without modifying DACLs on disk.
    #[tokio::test]
    async fn test_revoke_isolation_acls_script_dry_run_lists_recorded_grants() {
        skip_if_duplicate_ci_pass!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let cargo_cred = rig.host.cargo_home.join("credentials.toml");
        std::fs::write(&cargo_cred, "token = \"dryrun-test\"").unwrap();
        std::fs::create_dir_all(rig.host.cargo_home.join("bin")).unwrap();
        std::fs::create_dir_all(rig.host.rustup_home.join("toolchains")).unwrap();

        rusty_grid_worker::isolation::windows::prepare_worker_toolchains(
            &rig.host,
            &[],
            Some(&rig.base),
        )
        .expect("prepare_worker_toolchains");

        let manifest = rusty_grid_worker::isolation::windows::acl_grant_manifest_path(&rig.host);
        assert!(
            manifest.is_file(),
            "manifest must exist at {}",
            manifest.display()
        );
        let manifest_text = std::fs::read_to_string(&manifest).expect("read manifest");
        let mut expected_paths = Vec::new();
        let mut expected_sddl_paths = Vec::new();
        for line in manifest_text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("SDDL|") {
                if let Some((p, _sddl)) = rest.split_once('|') {
                    expected_sddl_paths.push(p.to_string());
                }
            } else {
                expected_paths.push(trimmed.to_string());
            }
        }
        assert!(
            !expected_paths.is_empty() && !expected_sddl_paths.is_empty(),
            "manifest must contain both grant paths and SDDL| restore lines: {manifest_text}"
        );

        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let script_path = repo_root.join("windows/revoke-isolation-acls.ps1");
        assert!(script_path.is_file(), "missing {}", script_path.display());

        let out = hidden_command(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&script_path)
            .arg("-DryRun")
            .arg("-ManifestPath")
            .arg(&manifest)
            .output()
            .expect("run revoke-isolation-acls.ps1 -DryRun");

        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "revoke-isolation-acls.ps1 -DryRun failed (status={}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            out.status
        );

        let tc_sid = rusty_grid_worker::isolation::windows::oxideswarm_toolchain_sid_string()
            .expect("toolchain SID string");
        assert!(
            stdout.contains(&format!("oxideswarm.toolchain capability SID: {tc_sid}")),
            "DryRun output must include derived capability SID {tc_sid}:\n{stdout}"
        );
        for p in &expected_paths {
            let expected_line = format!("[DryRun] icacls \"{p}\" /remove \"*{tc_sid}\" /c /q");
            assert!(
                stdout.contains(&expected_line),
                "DryRun output missing `{expected_line}`:\n{stdout}"
            );
        }
        for sp in &expected_sddl_paths {
            let expected_prefix = format!("[DryRun] Restore-AclSddl \"{sp}\" ");
            assert!(
                stdout.contains(&expected_prefix),
                "DryRun output missing `{expected_prefix}`:\n{stdout}"
            );
        }
    }

    async fn assert_windows_workspace_confinement(level: IsolationLevel, ws_in_home: bool) {
        use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
        use rusty_grid_core::task::WorkspaceRef;

        let home = tempfile::tempdir().expect("home");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::create_dir_all(home.path().join(".oxideswarm")).unwrap();
        std::fs::write(
            home.path().join(".oxideswarm").join("cluster.secret"),
            SECRET,
        )
        .unwrap();
        std::fs::create_dir_all(home.path().join(".ssh")).unwrap();
        std::fs::write(home.path().join(".ssh").join("test_key"), KEY).unwrap();

        let sandbox_base = outside.path().join("sandboxes");
        let ws_root = if ws_in_home {
            home.path()
                .join(".cache")
                .join("oxideswarm")
                .join("workspaces")
        } else {
            outside.path().join("workspaces")
        };

        let rustup_home = home.path().join(".rustup");
        let cargo_home = home.path().join(".cargo");
        std::fs::create_dir_all(&rustup_home).unwrap();
        std::fs::create_dir_all(&cargo_home).unwrap();
        let host = HostDirs::from_home(
            home.path().to_path_buf(),
            Some(rustup_home),
            Some(cargo_home),
        );
        let config = RunnerConfig::new(SandboxConfig::new(sandbox_base.clone()))
            .with_isolation(level)
            .with_host_dirs(host)
            .with_workspaces(true)
            .with_workspace_root(Some(ws_root.clone()));
        let caps = WorkerCapabilities::new("feat3-ws", 4, 8192, false, false, None);
        let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, config);
        let cache = runner.blob_cache().expect("blob cache");

        let file_data = b"manifest-hello";
        let file_id = BlobId::of(file_data);
        let tmp_file = cache.temp_path();
        std::fs::write(&tmp_file, file_data).unwrap();
        cache
            .adopt(&tmp_file, file_id, file_data.len() as u64)
            .unwrap();
        let manifest = TreeManifest::new(vec![BlobFile {
            path: "src/in.txt".into(),
            id: file_id,
            size: file_data.len() as u64,
            executable: false,
        }])
        .unwrap();
        let m_bytes = manifest.encode();
        let m_id = BlobId::of(&m_bytes);
        let tmp_m = cache.temp_path();
        std::fs::write(&tmp_m, &m_bytes).unwrap();
        cache.adopt(&tmp_m, m_id, m_bytes.len() as u64).unwrap();
        let m_size = m_bytes.len() as u64;

        let seed_other = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), "(echo other-ws-secret)>secret.txt".into()],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("other_ws").with_tree(m_id, m_size));
        let seed_res = runner.execute_task(&seed_other, None, None).await;
        assert!(
            seed_res.is_success(),
            "seeding other_ws failed: {:?} / {}",
            seed_res.error,
            seed_res.stderr_str()
        );
        let other_tree_path = ws_root.join("other_ws").join("tree");
        let other_secret_path = other_tree_path.join("secret.txt");
        let other_planted_path = other_tree_path.join("planted.txt");
        let active_state_json = ws_root.join("active_ws").join("state.json");
        assert!(other_secret_path.exists());

        let script = r#"
(findstr manifest-hello src\in.txt >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo READ_TREE_OK) &
((echo built-v1)>%RUSTY_GRID_WORKSPACE_DIR%\built.txt && echo WRITE_TREE_OK) &
(findstr built-v1 %RUSTY_GRID_WORKSPACE_DIR%\built.txt >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo READ_BUILT_OK) &
((echo scratch)>%TMPDIR%\scratch.txt && echo WRITE_TMP_OK) &
(findstr scratch %TMPDIR%\scratch.txt >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo READ_TMP_OK) &
(type %T_OTHER_SECRET% >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_READ_OTHER_WS || echo DENY_READ_OTHER_WS) &
((echo pwn>%T_OTHER_PLANTED%) >%TMPDIR%\.sink 2>%TMPDIR%\.err & if exist %T_OTHER_PLANTED% (echo LEAK_WRITE_OTHER_WS) else (echo DENY_WRITE_OTHER_WS)) &
(type %T_STATE_JSON% >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_STATE_JSON || echo DENY_STATE_JSON) &
(dir /b %T_WS_ROOT% >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_WS_ROOT_LIST || echo DENY_WS_ROOT_LIST) &
(type ..\state.json >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_DOTDOT_STATE || echo DENY_DOTDOT_STATE) &
(type ..\..\other_ws\tree\secret.txt >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_DOTDOT_OTHER_WS || echo DENY_DOTDOT_OTHER_WS) &
((mklink /J %RUSTY_GRID_WORKSPACE_DIR%\sym_other %T_OTHER_TREE% >%TMPDIR%\.sink 2>%TMPDIR%\.err && type %RUSTY_GRID_WORKSPACE_DIR%\sym_other\secret.txt >%TMPDIR%\.sink 2>%TMPDIR%\.err) && echo LEAK_SYMLINK_OTHER_WS || echo DENY_SYMLINK_OTHER_WS) &
(rmdir %RUSTY_GRID_WORKSPACE_DIR%\sym_other >%TMPDIR%\.sink 2>%TMPDIR%\.err) &
((mklink /H %RUSTY_GRID_WORKSPACE_DIR%\hard_other %T_OTHER_SECRET% >%TMPDIR%\.sink 2>%TMPDIR%\.err && type %RUSTY_GRID_WORKSPACE_DIR%\hard_other >%TMPDIR%\.sink 2>%TMPDIR%\.err) && echo LEAK_HARDLINK_OTHER_WS || echo DENY_HARDLINK_OTHER_WS) &
(del /f /q %RUSTY_GRID_WORKSPACE_DIR%\hard_other >%TMPDIR%\.sink 2>%TMPDIR%\.err) &
(type %T_SECRET% >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_SECRET || echo DENY_SECRET) &
(type %T_KEY% >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo LEAK_KEY || echo DENY_KEY) &
((echo x>%T_HOME%\planted.txt) >%TMPDIR%\.sink 2>%TMPDIR%\.err & if exist %T_HOME%\planted.txt (echo LEAK_WRITE_HOME) else (echo DENY_WRITE_HOME))
"#
        .replace('\n', " ");

        let env = HashMap::from([
            (
                "T_OTHER_TREE".into(),
                other_tree_path.to_string_lossy().into_owned(),
            ),
            (
                "T_OTHER_SECRET".into(),
                other_secret_path.to_string_lossy().into_owned(),
            ),
            (
                "T_OTHER_PLANTED".into(),
                other_planted_path.to_string_lossy().into_owned(),
            ),
            (
                "T_STATE_JSON".into(),
                active_state_json.to_string_lossy().into_owned(),
            ),
            ("T_WS_ROOT".into(), ws_root.to_string_lossy().into_owned()),
            (
                "T_SECRET".into(),
                home.path()
                    .join(".oxideswarm")
                    .join("cluster.secret")
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "T_KEY".into(),
                home.path()
                    .join(".ssh")
                    .join("test_key")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ("T_HOME".into(), home.path().to_string_lossy().into_owned()),
        ]);
        let task1 = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), script],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("active_ws").with_tree(m_id, m_size));

        let res1 = runner.execute_task(&task1, None, None).await;
        assert!(
            res1.is_success(),
            "{level} (ws_in_home={ws_in_home}): {:?} / {}",
            res1.error,
            res1.stderr_str()
        );
        let got = lines(&res1);
        for expected in [
            "READ_TREE_OK",
            "WRITE_TREE_OK",
            "READ_BUILT_OK",
            "WRITE_TMP_OK",
            "READ_TMP_OK",
        ] {
            assert!(
                got.iter().any(|l| l == expected),
                "{level} (ws_in_home={ws_in_home}): missing {expected}; output was {got:?}"
            );
        }

        let probes = [
            "READ_OTHER_WS",
            "WRITE_OTHER_WS",
            "STATE_JSON",
            "WS_ROOT_LIST",
            "DOTDOT_STATE",
            "DOTDOT_OTHER_WS",
            "SYMLINK_OTHER_WS",
            "HARDLINK_OTHER_WS",
            "SECRET",
            "KEY",
            "WRITE_HOME",
        ];
        if level == IsolationLevel::Off {
            for probe in probes {
                let expected = format!("LEAK_{probe}");
                assert!(
                    got.contains(&expected),
                    "unconfined control (ws_in_home={ws_in_home}): {expected} must succeed so the confined tests prove something; output was {got:?}"
                );
            }
            return;
        }

        for probe in probes {
            let expected = format!("DENY_{probe}");
            assert!(
                got.contains(&expected),
                "{level} (ws_in_home={ws_in_home}): missing {expected}; output was {got:?}"
            );
        }
        assert!(
            !got.iter().any(|l| l.starts_with("LEAK_")),
            "{level} (ws_in_home={ws_in_home}): forbidden access succeeded: {got:?}"
        );
        assert!(!other_planted_path.exists());
        assert!(!home.path().join("planted.txt").exists());

        // Second task in `active_ws` sees `built.txt` and a fresh `%TMPDIR%`.
        let task2 = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec![
                    "/C".into(),
                    r#"findstr built-v1 built.txt >%TMPDIR%\.sink 2>%TMPDIR%\.err && if not exist %TMPDIR%\scratch.txt echo SECOND_OK"#
                        .into(),
                ],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("active_ws"));
        let res2 = runner.execute_task(&task2, None, None).await;
        assert!(
            res2.is_success() && res2.stdout_str().contains("SECOND_OK"),
            "{level} second task: {:?} / {} / {}",
            res2.error,
            res2.stdout_str(),
            res2.stderr_str()
        );
    }

    async fn assert_windows_workspace_tree_swap_regression(level: IsolationLevel) {
        use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
        use rusty_grid_core::task::WorkspaceRef;

        let home = tempfile::tempdir().expect("home");
        let outside = tempfile::tempdir().expect("outside");
        let victim_dir = home.path().join("victim");
        let victim_sub = victim_dir.join("victim_only");
        std::fs::create_dir_all(&victim_sub).unwrap();
        let victim_secret = victim_dir.join("secret.txt");
        let victim_planted = victim_sub.join("pwned.txt");
        std::fs::write(&victim_secret, "dummy-victim-secret").unwrap();

        let sandbox_base = outside.path().join("sandboxes");
        let ws_root = outside.path().join("workspaces");

        let rustup_home = home.path().join(".rustup");
        let cargo_home = home.path().join(".cargo");
        std::fs::create_dir_all(&rustup_home).unwrap();
        std::fs::create_dir_all(&cargo_home).unwrap();
        let host = HostDirs::from_home(
            home.path().to_path_buf(),
            Some(rustup_home),
            Some(cargo_home),
        );
        let config = RunnerConfig::new(SandboxConfig::new(sandbox_base))
            .with_isolation(level)
            .with_host_dirs(host)
            .with_workspaces(true)
            .with_workspace_root(Some(ws_root.clone()));
        let caps = WorkerCapabilities::new("feat3-ws-swap", 4, 8192, false, false, None);
        let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, config);
        let cache = runner.blob_cache().expect("blob cache");

        let file_data = b"hello";
        let file_id = BlobId::of(file_data);
        let tmp_file = cache.temp_path();
        std::fs::write(&tmp_file, file_data).unwrap();
        cache
            .adopt(&tmp_file, file_id, file_data.len() as u64)
            .unwrap();
        let manifest = TreeManifest::new(vec![BlobFile {
            path: "in.txt".into(),
            id: file_id,
            size: file_data.len() as u64,
            executable: false,
        }])
        .unwrap();
        let m_bytes = manifest.encode();
        let m_id = BlobId::of(&m_bytes);
        let tmp_m = cache.temp_path();
        std::fs::write(&tmp_m, &m_bytes).unwrap();
        cache.adopt(&tmp_m, m_id, m_bytes.len() as u64).unwrap();
        let m_size = m_bytes.len() as u64;

        let swap_script = r#"cd /d %TMPDIR% & (rmdir /s /q %RUSTY_GRID_WORKSPACE_DIR% >%TMPDIR%\.sink 2>%TMPDIR%\.err && mklink /J %RUSTY_GRID_WORKSPACE_DIR% %T_VICTIM% >%TMPDIR%\.sink 2>%TMPDIR%\.err && echo SWAP_OK || echo SWAP_DENIED)"#;
        let task1 = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), swap_script.into()],
                env: HashMap::from([(
                    "T_VICTIM".into(),
                    victim_dir.to_string_lossy().into_owned(),
                )]),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("swap_ws").with_tree(m_id, m_size));
        let res1 = runner.execute_task(&task1, None, None).await;

        let probe_victim_script = r#"(type secret.txt 2>%TMPDIR%\.err && echo LEAK_VICTIM_READ) & ((echo pwn>victim_only\pwned.txt) 2>%TMPDIR%\.err & if exist victim_only\pwned.txt echo LEAK_VICTIM_WRITE) & echo TASK2_RAN"#;
        let task2 = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), probe_victim_script.into()],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("swap_ws"));
        let res2 = runner.execute_task(&task2, None, None).await;

        let swap_denied = res1.stdout_str().contains("SWAP_DENIED");
        let task2_refused = !res2.is_success() && !res2.stdout_str().contains("TASK2_RAN");
        assert!(
            swap_denied || task2_refused,
            "{level}: either task 1's tree swap must be denied or task 2 must be refused; res1={:?}/{}, res2={:?}/{}",
            res1.error,
            res1.stdout_str(),
            res2.error,
            res2.stdout_str()
        );
        assert!(
            !res2.stdout_str().contains("LEAK_VICTIM_READ")
                && !res2.stdout_str().contains("LEAK_VICTIM_WRITE")
                && !res2.stdout_str().contains("dummy-victim-secret"),
            "{level}: task 2 touched or read the victim directory: {}",
            res2.stdout_str()
        );
        assert_eq!(
            std::fs::read_to_string(&victim_secret).unwrap(),
            "dummy-victim-secret"
        );
        assert!(!victim_planted.exists());

        // Pre-run junction check (defense in depth): if `<ws>/tree` is replaced with a junction on
        // disk outside the sandbox, the next workspace run (without or with a source) refuses to
        // run, never touches the victim, and repairs the workspace.
        let seed = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), "echo ok".into()],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("tamper_ws").with_tree(m_id, m_size));
        assert!(runner.execute_task(&seed, None, None).await.is_success());

        let tamper_tree = ws_root.join("tamper_ws").join("tree");
        std::fs::remove_dir_all(&tamper_tree).unwrap();
        let j_status = hidden_command(r"C:\Windows\System32\cmd.exe")
            .args([
                "/C",
                "mklink",
                "/J",
                &tamper_tree.to_string_lossy(),
                &victim_dir.to_string_lossy(),
            ])
            .status()
            .expect("mklink /J");
        assert!(j_status.success());

        let no_source_task = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), probe_victim_script.into()],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("tamper_ws"));
        let no_source_res = runner.execute_task(&no_source_task, None, None).await;
        assert!(
            !no_source_res.is_success() && !no_source_res.stdout_str().contains("TASK2_RAN"),
            "{level}: task without source must be refused when tree is a junction: {:?} / {} / {}",
            no_source_res.error,
            no_source_res.stdout_str(),
            no_source_res.stderr_str()
        );
        assert_eq!(
            std::fs::read_to_string(&victim_secret).unwrap(),
            "dummy-victim-secret"
        );
        assert!(!victim_planted.exists());
        assert!(
            !rusty_grid_worker::isolation::windows::is_reparse_point(&tamper_tree),
            "tampered tree junction must be wiped/repaired under the workspace lock"
        );

        std::fs::remove_dir_all(&tamper_tree).unwrap();
        let j_status2 = hidden_command(r"C:\Windows\System32\cmd.exe")
            .args([
                "/C",
                "mklink",
                "/J",
                &tamper_tree.to_string_lossy(),
                &victim_dir.to_string_lossy(),
            ])
            .status()
            .expect("mklink /J");
        assert!(j_status2.success());
        let with_source_task = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec!["/C".into(), probe_victim_script.into()],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 35),
        )
        .with_workspace(WorkspaceRef::new("tamper_ws").with_tree(m_id, m_size));
        let with_source_res = runner.execute_task(&with_source_task, None, None).await;
        assert!(
            !with_source_res.is_success() && !with_source_res.stdout_str().contains("TASK2_RAN"),
            "{level}: task with source must be refused when tree is a junction: {:?} / {}",
            with_source_res.error,
            with_source_res.stderr_str()
        );
        assert_eq!(
            std::fs::read_to_string(&victim_secret).unwrap(),
            "dummy-victim-secret"
        );
        assert!(!victim_planted.exists());
        assert!(
            !rusty_grid_worker::isolation::windows::is_reparse_point(&tamper_tree),
            "tampered tree junction must be wiped/repaired under the workspace lock"
        );
    }

    #[tokio::test]
    async fn test_confined_workspace_task_writes_own_tree_and_tmp_but_not_other_workspaces() {
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();
        assert_windows_workspace_confinement(IsolationLevel::Off, false).await;
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_windows_workspace_confinement(level, false).await;
        }
        assert_windows_workspace_confinement(IsolationLevel::Fs, true).await;
    }

    #[tokio::test]
    async fn test_workspace_tree_swap_is_denied_and_never_touches_victim() {
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();
        for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
            assert_windows_workspace_tree_swap_regression(level).await;
        }
    }

    #[tokio::test]
    async fn test_worker_with_default_sandbox_base_inside_user_home_starts_and_runs_under_fs() {
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();
        let temp_home = tempfile::tempdir().expect("temp home");
        let rustup_home = temp_home.path().join(".rustup");
        let cargo_home = temp_home.path().join(".cargo");
        std::fs::create_dir_all(&rustup_home).unwrap();
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::fs::create_dir_all(temp_home.path().join(".oxideswarm")).unwrap();
        std::fs::write(
            temp_home.path().join(".oxideswarm").join("cluster.secret"),
            SECRET,
        )
        .unwrap();

        let temp_dir_in_home = temp_home.path().join("AppData").join("Local").join("Temp");
        std::fs::create_dir_all(&temp_dir_in_home).unwrap();
        let default_sandbox_cfg = SandboxConfig::default_under(temp_dir_in_home.clone());
        assert_eq!(
            default_sandbox_cfg.base_dir,
            temp_dir_in_home.join("rusty_grid").join("sandboxes")
        );

        let host = HostDirs::from_home(
            temp_home.path().to_path_buf(),
            Some(rustup_home),
            Some(cargo_home),
        );

        rusty_grid_worker::isolation::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &default_sandbox_cfg.base_dir,
            &host,
            &[],
        )
        .await
        .expect(
            "worker with --isolation fs and default sandbox base under <user_home>\\AppData\\Local\\Temp\\rusty_grid\\sandboxes must pass startup check",
        );

        let runner_cfg = RunnerConfig::new(default_sandbox_cfg)
            .with_isolation(IsolationLevel::Fs)
            .with_host_dirs(host.clone());
        let caps = WorkerCapabilities::new("feat3-default-sb", 2, 4096, false, false, None);
        let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_cfg);
        let task = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec![
                    "/C".into(),
                    "echo DEFAULT_SB_OK > out.txt & type out.txt".into(),
                ],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let res = runner.execute_task(&task, None, None).await;
        let manifest_path = temp_home
            .path()
            .join(".oxideswarm")
            .join("windows_acl_grants.txt");
        let _ =
            rusty_grid_worker::isolation::windows::revoke_oxideswarm_toolchain_acls(&manifest_path);
        assert!(
            res.is_success() && res.stdout_str().contains("DEFAULT_SB_OK"),
            "task under default sandbox base must succeed: {:?} / {} / {}",
            res.error,
            res.stdout_str(),
            res.stderr_str()
        );
    }

    #[tokio::test]
    async fn test_worker_with_sandbox_base_under_junction_starts_and_runs_under_fs() {
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();

        struct AclRevokeGuard(PathBuf);
        impl Drop for AclRevokeGuard {
            fn drop(&mut self) {
                let _ = rusty_grid_worker::isolation::windows::revoke_oxideswarm_toolchain_acls(
                    &self.0,
                );
            }
        }
        struct JunctionGuard(PathBuf);
        impl Drop for JunctionGuard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir(&self.0);
            }
        }
        struct SubstGuard(String);
        impl Drop for SubstGuard {
            fn drop(&mut self) {
                let _ = hidden_command(r"C:\Windows\System32\subst.exe")
                    .args([&self.0, "/D"])
                    .status();
            }
        }

        let temp_home = tempfile::tempdir().expect("temp home");
        let temp_root = tempfile::tempdir().expect("temp junction root");
        let _acl_guard = AclRevokeGuard(
            temp_home
                .path()
                .join(".oxideswarm")
                .join("windows_acl_grants.txt"),
        );

        let rustup_home = temp_home.path().join(".rustup");
        let cargo_home = temp_home.path().join(".cargo");
        std::fs::create_dir_all(&rustup_home).unwrap();
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::fs::create_dir_all(temp_home.path().join(".oxideswarm")).unwrap();
        std::fs::write(
            temp_home.path().join(".oxideswarm").join("cluster.secret"),
            SECRET,
        )
        .unwrap();

        let host = HostDirs::from_home(
            temp_home.path().to_path_buf(),
            Some(rustup_home),
            Some(cargo_home),
        );

        let real_parent = temp_root.path().join("real_parent");
        let real_sandboxes = real_parent.join("sandboxes");
        std::fs::create_dir_all(&real_sandboxes).unwrap();
        let junc_parent = temp_root.path().join("junc_parent");
        let j_status = hidden_command(r"C:\Windows\System32\cmd.exe")
            .args([
                "/C",
                "mklink",
                "/J",
                &junc_parent.to_string_lossy(),
                &real_parent.to_string_lossy(),
            ])
            .status()
            .expect("mklink /J junc_parent");
        assert!(j_status.success());
        let _junc_parent_guard = JunctionGuard(junc_parent.clone());

        // 1. Direct caller passing `sandbox_base` under an ancestor junction:
        //    `probe_with_config` normalises ancestor junctions before `protect_sandbox_base`,
        //    while `SandboxConfig::ensure_usable` still refuses ancestor reparse points (design 0021).
        let junc_sb = junc_parent.join("sandboxes");
        rusty_grid_worker::isolation::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &junc_sb,
            &host,
            &[],
        )
        .await
        .expect(
            "verify_isolation_support_with_config must accept sandbox_base under ancestor junction",
        );

        let junc_err = SandboxConfig::new(junc_sb)
            .ensure_usable()
            .await
            .expect_err("ensure_usable must still refuse an ancestor reparse point");
        assert!(junc_err.to_string().contains("reparse point"), "{junc_err}");

        // 2. End-to-end startup check + task execution on a `SUBST` drive (`SandboxConfig::ensure_usable`,
        //    `verify_isolation_support_with_config`, and `TaskRunner::execute_task`).
        let drive_letter = ['Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q']
            .into_iter()
            .find(|ch| !Path::new(&format!("{ch}:\\")).exists())
            .expect("free drive letter for SUBST test");
        let drive = format!("{drive_letter}:");
        let subst_status = hidden_command(r"C:\Windows\System32\subst.exe")
            .arg(&drive)
            .arg(&real_parent)
            .status()
            .expect("subst drive");
        assert!(subst_status.success(), "subst {drive} failed");
        let _subst_guard = SubstGuard(drive.clone());

        let subst_sb = PathBuf::from(format!("{drive}\\sandboxes_subst"));
        let subst_cfg = SandboxConfig::new(subst_sb);
        subst_cfg
            .ensure_usable()
            .await
            .expect("ensure_usable on SUBST sandbox_base");

        rusty_grid_worker::isolation::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &subst_cfg.base_dir,
            &host,
            &[],
        )
        .await
        .expect("startup self-check must accept sandbox_base on SUBST drive");

        let runner_cfg = RunnerConfig::new(subst_cfg)
            .with_isolation(IsolationLevel::Fs)
            .with_host_dirs(host.clone());
        let caps = WorkerCapabilities::new("feat3-subst-sb", 2, 4096, false, false, None);
        let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_cfg);
        let task = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec![
                    "/C".into(),
                    "echo SUBST_SB_OK > out.txt & type out.txt".into(),
                ],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let res = runner.execute_task(&task, None, None).await;
        assert!(
            res.is_success() && res.stdout_str().contains("SUBST_SB_OK"),
            "task under SUBST sandbox base must succeed: {:?} / {} / {}",
            res.error,
            res.stdout_str(),
            res.stderr_str()
        );

        // 3. A junction directly at `sandbox_base` itself must still be refused.
        let real_sb_direct = temp_root.path().join("real_sb_direct");
        std::fs::create_dir_all(&real_sb_direct).unwrap();
        let junc_sb_self = temp_root.path().join("junc_sb_self");
        let j_self_status = hidden_command(r"C:\Windows\System32\cmd.exe")
            .args([
                "/C",
                "mklink",
                "/J",
                &junc_sb_self.to_string_lossy(),
                &real_sb_direct.to_string_lossy(),
            ])
            .status()
            .expect("mklink /J junc_sb_self");
        assert!(j_self_status.success());
        let _junc_self_guard = JunctionGuard(junc_sb_self.clone());
        let direct_err = rusty_grid_worker::isolation::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &junc_sb_self,
            &host,
            &[],
        )
        .await
        .expect_err("junction directly at sandbox_base must remain refused");
        assert!(
            direct_err.to_string().contains("reparse point"),
            "{direct_err}"
        );
    }

    /// BUG-18 regression test: bare `whoami` (`TaskSpec::Command { program: "whoami", .. }`)
    /// under `--isolation fs` must resolve to `%SystemRoot%\System32\whoami.exe` rather than
    /// `C:\Program Files\Git\usr\bin\whoami.exe` (`msys-2.0.dll`, which fails with
    /// `0xC0000142` / `STATUS_DLL_INIT_FAILURE` inside a Windows AppContainer) and exit 0.
    #[tokio::test]
    async fn test_bare_whoami_succeeds_under_isolation_fs() {
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();
        let rig = Rig::new(IsolationLevel::Fs, false);
        let mut env = HashMap::new();
        env.insert(
            "PATH".to_string(),
            r"C:\Program Files\Git\usr\bin;C:\Windows\System32;C:\Windows".to_string(),
        );
        let task = Task::new(
            TaskSpec::Command {
                program: "whoami".into(),
                args: Vec::new(),
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let res = rig.runner.execute_task(&task, None, None).await;
        assert!(
            res.is_success() && !res.stdout_str().trim().is_empty(),
            "bare `whoami` under IsolationLevel::Fs must exit 0 with username output: exit_code={}, error={:?}, stdout={:?}, stderr={:?}",
            res.exit_code,
            res.error,
            res.stdout_str(),
            res.stderr_str()
        );
    }

    fn compile_tiny_windows_exe(out_dir: &Path, name: &str, src: &str) -> PathBuf {
        let rs_path = out_dir.join(format!("{name}.rs"));
        let exe_path = out_dir.join(format!("{name}.exe"));
        std::fs::write(&rs_path, src).expect("write helper rust source");
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let output = hidden_command(rustc)
            .arg(&rs_path)
            .arg("-o")
            .arg(&exe_path)
            .output()
            .expect("invoke rustc for test binary");
        assert!(
            output.status.success(),
            "rustc failed compiling {name}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        exe_path
    }

    /// BUG-21 regression test: relative staged executable paths (`./prog.exe`, `.\prog.exe`,
    /// `sub/prog.exe`, `sub\prog.exe`, `--cwd sub` + `./prog.exe`, and a batch script in a
    /// spaced subdirectory) must resolve against the task's working directory inside the sandbox
    /// under both `--isolation off` and `--isolation fs`, while `..\decoy.exe` and
    /// `../decoy.exe` must be refused without ever spawning an executable outside the sandbox.
    #[tokio::test]
    async fn test_windows_relative_staged_executables_across_isolation_modes() {
        skip_if_duplicate_ci_pass!();
        use rusty_grid_core::task::StagedFile;

        let bin_dir = tempfile::tempdir().expect("bin_dir");
        let ok_exe = compile_tiny_windows_exe(
            bin_dir.path(),
            "rel_probe",
            r#"fn main() {
    let arg = std::env::args().nth(1).unwrap_or_default();
    let cwd = std::env::current_dir().unwrap();
    println!("REL_EXE_OK|{}|{}", arg, cwd.display());
}
"#,
        );
        let decoy_exe = compile_tiny_windows_exe(
            bin_dir.path(),
            "decoy_probe",
            r#"fn main() {
    if let Ok(p) = std::env::var("BUG21_CANARY") {
        let _ = std::fs::write(p, "ESCAPED");
    }
    eprintln!("DECOY_EXECUTED");
}
"#,
        );
        let ok_bytes = std::fs::read(&ok_exe).expect("read compiled rel_probe.exe");
        let decoy_bytes = std::fs::read(&decoy_exe).expect("read compiled decoy_probe.exe");

        let mut levels = vec![IsolationLevel::Off];
        if appcontainer_available() {
            levels.push(IsolationLevel::Fs);
        }

        for level in levels {
            let rig = Rig::new(level, false);
            // Place decoys outside any task's sandbox directory:
            // - under `--isolation off`: directly in `rig.base` (for `..\decoy.exe`)
            // - under both modes: in `rig.base/other_task` (`..\other_task\decoy.exe`) and in
            //   `rig.base`'s parent (`..\..\decoy.exe`); `--isolation fs` refuses non-sandbox
            //   entries directly inside `rig.base`.
            let canary = rig.home.path().join(format!("decoy_canary_{level}.txt"));
            if level == IsolationLevel::Off {
                std::fs::write(rig.base.join("decoy.exe"), &decoy_bytes).unwrap();
            }
            std::fs::write(rig.base.join("other_task").join("decoy.exe"), &decoy_bytes).unwrap();
            std::fs::write(rig.base.parent().unwrap().join("decoy.exe"), &decoy_bytes).unwrap();

            let staged = vec![
                StagedFile::new("prog.exe", ok_bytes.clone(), true),
                StagedFile::new("sub/prog.exe", ok_bytes.clone(), true),
                StagedFile::new(
                    "dir with space/run me.bat",
                    b"@echo off\r\necho REL_BAT_OK^|%~1^|%CD%\r\n".to_vec(),
                    true,
                ),
            ];

            for (prog, working_dir, arg) in [
                ("./prog.exe", None, "1024"),
                (r".\prog.exe", None, "1024"),
                ("sub/prog.exe", None, "1024"),
                (r"sub\prog.exe", None, "1024"),
                ("./prog.exe", Some(PathBuf::from("sub")), "1024"),
                (r".\dir with space\run me.bat", None, "hello world"),
            ] {
                let is_bat = prog.ends_with(".bat");
                let task = Task::new(
                    TaskSpec::Command {
                        program: prog.into(),
                        args: vec![arg.into()],
                        env: HashMap::new(),
                        working_dir: working_dir.clone(),
                        stdin: None,
                    },
                    TaskRequirements::generic(1, 30),
                )
                .with_staged_files(staged.clone());

                let res = rig.runner.execute_task(&task, None, None).await;
                let expected_prefix = if is_bat {
                    format!("REL_BAT_OK|{arg}|")
                } else {
                    format!("REL_EXE_OK|{arg}|")
                };
                assert!(
                    res.is_success() && res.stdout_str().contains(&expected_prefix),
                    "[{level}] relative program '{prog}' (cwd={working_dir:?}) failed: exit={}, error={:?}, stdout={:?}, stderr={:?}",
                    res.exit_code,
                    res.error,
                    res.stdout_str(),
                    res.stderr_str()
                );
                if working_dir.is_some() {
                    let line = res.stdout_str();
                    assert!(
                        line.trim_end().to_ascii_lowercase().ends_with(r"\sub"),
                        "[{level}] expected child cwd inside \\sub, got: {line:?}"
                    );
                }
            }

            for escaping in [
                r"..\decoy.exe",
                "../decoy.exe",
                r"..\other_task\decoy.exe",
                "../other_task/decoy.exe",
                r"..\..\decoy.exe",
            ] {
                let mut esc_env = HashMap::new();
                esc_env.insert("BUG21_CANARY".into(), canary.to_string_lossy().into_owned());
                let esc_task = Task::new(
                    TaskSpec::Command {
                        program: escaping.into(),
                        args: Vec::new(),
                        env: esc_env,
                        working_dir: None,
                        stdin: None,
                    },
                    TaskRequirements::generic(1, 30),
                )
                .with_staged_files(staged.clone());

                let res = rig.runner.execute_task(&esc_task, None, None).await;
                assert!(
                    !res.is_success(),
                    "[{level}] escaping program '{escaping}' must fail"
                );
                let combined = format!(
                    "{} {}",
                    res.error.as_deref().unwrap_or_default(),
                    res.stderr_str()
                );
                assert!(
                    combined.contains("escapes the task sandbox"),
                    "[{level}] refusal for '{escaping}' must state sandbox escape, got: {combined}"
                );
                assert!(
                    !canary.exists(),
                    "[{level}] escaping program '{escaping}' must never execute decoy.exe"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_extra_read_only_dir_inside_home_is_readable_while_secrets_stay_denied() {
        skip_if_duplicate_ci_pass!();
        require_appcontainer!();
        let temp_home = tempfile::tempdir().expect("temp home");
        let outside = tempfile::tempdir().expect("outside");
        let ox_dir = temp_home.path().join(".oxideswarm");
        let ssh_dir = temp_home.path().join(".ssh");
        let rustup_home = temp_home.path().join(".rustup");
        let cargo_home = temp_home.path().join(".cargo");
        let in_home_runtime = temp_home
            .path()
            .join("AppData")
            .join("Local")
            .join("Programs")
            .join("Python")
            .join("Python311");
        let ext_runtime = outside.path().join("ext_runtime");
        let sandbox_base = outside.path().join("sandboxes");
        for dir in [
            &ox_dir,
            &ssh_dir,
            &rustup_home,
            &cargo_home,
            &in_home_runtime,
            &ext_runtime,
            &sandbox_base,
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }

        let secret_file = ox_dir.join("cluster.secret");
        let node_key_file = ox_dir.join("node.key");
        let ssh_key_file = ssh_dir.join("test_key");
        std::fs::write(&secret_file, SECRET).unwrap();
        std::fs::write(&node_key_file, "dummy-node-key-feat3").unwrap();
        std::fs::write(&ssh_key_file, KEY).unwrap();

        let in_home_txt = in_home_runtime.join("runtime.txt");
        let ext_txt = ext_runtime.join("ext.txt");
        std::fs::write(&in_home_txt, "in-home-runtime").unwrap();
        std::fs::write(&ext_txt, "external-runtime").unwrap();

        let copied_cmd = in_home_runtime.join("probe_cmd.exe");
        std::fs::copy(r"C:\Windows\System32\cmd.exe", &copied_cmd).unwrap();

        let host = HostDirs::from_home(
            temp_home.path().to_path_buf(),
            Some(rustup_home),
            Some(cargo_home),
        )
        .with_extra_read_only_dirs(vec![in_home_runtime.clone(), ext_runtime.clone()]);
        let protected = vec![secret_file.clone(), node_key_file.clone()];
        let manifest_path = rusty_grid_worker::isolation::acl_grant_manifest_path(&host);

        let verify_res = rusty_grid_worker::isolation::verify_isolation_support_with_config(
            IsolationLevel::Fs,
            &sandbox_base,
            &host,
            &protected,
        )
        .await;
        if let Err(err) = verify_res {
            let _ = rusty_grid_worker::isolation::windows::revoke_oxideswarm_toolchain_acls(
                &manifest_path,
            );
            panic!("extra_read_only_dirs startup check must succeed: {err}");
        }

        let manifest_body = std::fs::read_to_string(&manifest_path).unwrap_or_default();

        let runner_cfg = RunnerConfig::new(SandboxConfig::new(sandbox_base))
            .with_isolation(IsolationLevel::Fs)
            .with_host_dirs(host)
            .with_protected_files(protected);
        let caps = WorkerCapabilities::new("feat3-extra-ro", 2, 4096, false, false, None);
        let runner = TaskRunner::new(uuid::Uuid::new_v4(), caps, runner_cfg);

        let exe_task = Task::new(
            TaskSpec::Command {
                program: copied_cmd.to_string_lossy().into_owned(),
                args: vec!["/C".into(), "echo IN_HOME_EXE_OK".into()],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let exe_res = runner.execute_task(&exe_task, None, None).await;

        let probe_task = Task::new(
            TaskSpec::Command {
                program: r"C:\Windows\System32\cmd.exe".into(),
                args: vec![
                    "/C".into(),
                    r#"(findstr /L "in-home-runtime" "%T_IN_HOME_TXT%" >.sink 2>.err_ro1 && echo READ_IN_HOME_OK || type .err_ro1 1>&2) & (findstr /L "external-runtime" "%T_EXT_TXT%" >.sink 2>.err_ro2 && echo READ_EXT_OK || type .err_ro2 1>&2) & ((echo pwn>"%T_IN_HOME_DIR%\pwn.txt") >.sink 2>.err & if exist "%T_IN_HOME_DIR%\pwn.txt" (echo LEAK_WRITE_IN_HOME) else (echo DENY_WRITE_IN_HOME)) & ((echo pwn>"%T_EXT_DIR%\pwn.txt") >.sink 2>.err & if exist "%T_EXT_DIR%\pwn.txt" (echo LEAK_WRITE_EXT) else (echo DENY_WRITE_EXT)) & (type "%T_SECRET%" >.sink 2>.err && echo LEAK_SECRET || echo DENY_SECRET) & (type "%T_NODE_KEY%" >.sink 2>.err && echo LEAK_NODE_KEY || echo DENY_NODE_KEY) & (type "%T_SSH_KEY%" >.sink 2>.err && echo LEAK_SSH_KEY || echo DENY_SSH_KEY)"#.into(),
                ],
                env: HashMap::from([
                    (
                        "T_IN_HOME_TXT".into(),
                        in_home_txt.to_string_lossy().into_owned(),
                    ),
                    ("T_EXT_TXT".into(), ext_txt.to_string_lossy().into_owned()),
                    (
                        "T_IN_HOME_DIR".into(),
                        in_home_runtime.to_string_lossy().into_owned(),
                    ),
                    (
                        "T_EXT_DIR".into(),
                        ext_runtime.to_string_lossy().into_owned(),
                    ),
                    (
                        "T_SECRET".into(),
                        secret_file.to_string_lossy().into_owned(),
                    ),
                    (
                        "T_NODE_KEY".into(),
                        node_key_file.to_string_lossy().into_owned(),
                    ),
                    (
                        "T_SSH_KEY".into(),
                        ssh_key_file.to_string_lossy().into_owned(),
                    ),
                ]),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 30),
        );
        let res = runner.execute_task(&probe_task, None, None).await;
        let revoke_res =
            rusty_grid_worker::isolation::windows::revoke_oxideswarm_toolchain_acls(&manifest_path);

        assert!(
            exe_res.is_success() && exe_res.stdout_str().contains("IN_HOME_EXE_OK"),
            "executable inside allowlisted home dir must run under AppContainer: {:?} / {} / {}",
            exe_res.error,
            exe_res.stdout_str(),
            exe_res.stderr_str()
        );
        assert!(
            res.is_success(),
            "extra_read_only_dir probe task failed: {:?} / {}",
            res.error,
            res.stderr_str()
        );
        assert!(
            !res.stderr_str().contains("Access is denied"),
            "unexpected redirection failure on stderr: {}",
            res.stderr_str()
        );
        let got = lines(&res);
        for expected in [
            "READ_IN_HOME_OK",
            "READ_EXT_OK",
            "DENY_WRITE_IN_HOME",
            "DENY_WRITE_EXT",
            "DENY_SECRET",
            "DENY_NODE_KEY",
            "DENY_SSH_KEY",
        ] {
            assert!(
                got.iter().any(|l| l == expected),
                "missing {expected}; got={got:?}; stderr={}",
                res.stderr_str()
            );
        }
        assert!(
            !got.iter().any(|l| l.starts_with("LEAK_")),
            "forbidden access succeeded: {got:?}"
        );
        assert!(
            manifest_body.contains("Python311") && manifest_body.contains("ext_runtime"),
            "ACL grant manifest must record extra_read_only_dirs: {manifest_body}"
        );
        assert!(!in_home_runtime.join("pwn.txt").exists());
        assert!(!ext_runtime.join("pwn.txt").exists());
        revoke_res.expect("revoke_oxideswarm_toolchain_acls must succeed");
    }
}

/// Where there is no implementation on an unsupported OS, isolation is refused
/// at start-up, and a task is refused instead of running unsandboxed.
#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
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
