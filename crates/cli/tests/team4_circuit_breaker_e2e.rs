//! Integration tests for TEAM-4 circuit breaker, per-agent hourly verify cap,
//! blocked-task person notice, and `plan unblock` (`design 0111` D5 / T3).

#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::Command;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
use chrono::Utc;
#[cfg(unix)]
use tempfile::TempDir;
#[cfg(unix)]
use tokio::sync::watch;

#[cfg(unix)]
use rusty_grid_cli::blobs::WorkspaceLimits;
#[cfg(unix)]
use rusty_grid_cli::plan::lease::{
    claim_task_at, lookup_task_lease, unblock_task_at, ClaimOptions, UnblockOptions,
    CIRCUIT_BREAKER_THRESHOLD, DEFAULT_LEASE_TTL_MINS,
};
#[cfg(unix)]
use rusty_grid_cli::verify_gate::{run_verify_gate_with_options, VerifyGateOptions};
#[cfg(unix)]
use rusty_grid_master::reaper::ReaperConfig;
#[cfg(unix)]
use rusty_grid_master::registry::WorkerStatus;
#[cfg(unix)]
use rusty_grid_master::scheduler::SchedulerConfig;
#[cfg(unix)]
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
#[cfg(unix)]
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

#[cfg(unix)]
struct BreakerCluster {
    master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
}

#[cfg(unix)]
impl BreakerCluster {
    async fn new() -> Self {
        let blob_dir = tempfile::tempdir().expect("blob dir");
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap())
                .with_blob_store(blob_dir.path(), 64 * 1024 * 1024),
            SchedulerConfig::default().without_host_cpu_backpressure(),
            ReaperConfig::default(),
        )
        .await
        .expect("master starts");
        let addr = master.server_addr().to_string();
        Self {
            master,
            addr,
            workers: Vec::new(),
            _dirs: vec![blob_dir],
        }
    }

    fn spawn_workspace_worker(&mut self, name: &str) {
        let dir = tempfile::tempdir().expect("sandbox dir");
        let ws_root = dir.path().join("workspaces");
        std::fs::create_dir_all(&ws_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&ws_root, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let config = WorkerConfig::new(self.addr.clone())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(2)
            .with_ram_mb(2048)
            .with_sandbox_base_dir(dir.path())
            .with_workspaces(true)
            .with_workspace_root(Some(ws_root))
            .with_redirection_portal(false);
        self._dirs.push(dir);
        let mut client = WorkerClient::new(config);
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((tx, handle));
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(workers) = self.master.list_workers().await {
                if workers
                    .iter()
                    .filter(|w| w.status != WorkerStatus::Disconnected)
                    .count()
                    >= count
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("{count} workers did not register");
    }
}

#[cfg(unix)]
impl Drop for BreakerCluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

#[cfg(unix)]
fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .expect("git must run");
    assert!(
        out.status.success(),
        "git {:?} failed: stderr={}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[cfg(unix)]
struct RepoFixture {
    _tmp: TempDir,
    clone: PathBuf,
    wt_root: PathBuf,
    hub_dir: PathBuf,
}

#[cfg(unix)]
fn setup_repo_fixture() -> RepoFixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let bare = tmp.path().join("remote.git");
    let seed = tmp.path().join("seed");
    let clone = tmp.path().join("clone");
    let wt_root = tmp.path().join("worktrees");
    let hub_dir = tmp.path().join("hub");

    std::fs::create_dir_all(&bare).unwrap();
    git(&bare, &["init", "--bare", "-b", "master"]);
    std::fs::create_dir_all(&hub_dir).unwrap();

    std::fs::create_dir_all(seed.join(".oxideswarm")).unwrap();
    std::fs::create_dir_all(seed.join("src")).unwrap();
    git(&seed, &["init", "-b", "master"]);
    std::fs::write(
        seed.join(".oxideswarm/plan.toml"),
        r#"[project]
base = "master"

[profile.unit]
command = ["sh", "src/run_check.sh"]

[[task]]
id = "T-1"
title = "Circuit breaker E2E task"
status = "open"
owned_paths = ["src/**"]
verify = ["unit"]
person = "alice"

[[task]]
id = "T-CAP"
title = "Rate limit E2E task"
status = "open"
owned_paths = ["cap/**"]
verify = ["unit"]
"#,
    )
    .unwrap();
    std::fs::write(seed.join("src/run_check.sh"), "echo ok\nexit 0\n").unwrap();
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-m", "initial plan"]);

    let bare_str = bare.to_string_lossy().into_owned();
    git(&seed, &["remote", "add", "origin", &bare_str]);
    git(&seed, &["push", "-u", "origin", "master"]);
    git(tmp.path(), &["clone", &bare_str, clone.to_str().unwrap()]);

    RepoFixture {
        _tmp: tmp,
        clone,
        wt_root,
        hub_dir,
    }
}

#[cfg(unix)]
fn write_failing_script(wt: &Path, test_name: &str, panic_line: &str) {
    let script = format!(
        "cat <<'EOF'\nrunning 1 test\ntest {test_name} ... FAILED\n\nfailures:\n\n---- {test_name} stdout ----\nthread '{test_name}' panicked at src/queue.rs:42:5:\n{panic_line}\nEOF\nexit 101\n"
    );
    std::fs::write(wt.join("src/run_check.sh"), script).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn t4_circuit_breaker_trips_at_three_notifies_person_unblocks_and_resets() {
    let mut cluster = BreakerCluster::new().await;
    cluster.spawn_workspace_worker("ws-worker");
    cluster.wait_for_workers(1).await;

    let fix = setup_repo_fixture();
    let claim = claim_task_at(
        &fix.clone,
        &ClaimOptions {
            task_id: "T-1".to_string(),
            holder: "agent-test".to_string(),
            person: None,
            remote: "origin".to_string(),
            base: None,
            plan_path: None,
            branch: None,
            worktree_root: Some(fix.wt_root.clone()),
            create_worktree: true,
            ttl_minutes: DEFAULT_LEASE_TTL_MINS,
        },
        Utc::now(),
    )
    .expect("claim T-1");
    let wt = claim.worktree_path.expect("worktree created");

    let opts = || VerifyGateOptions {
        remote: "origin".to_string(),
        agent_id: Some("agent-test".to_string()),
        max_verifies_per_hour: 20,
        hub_dir: Some(fix.hub_dir.clone()),
        breaker_threshold: CIRCUIT_BREAKER_THRESHOLD,
    };

    // Failure A: establishes fingerprint 1 (`verify_failures = 1`).
    write_failing_script(&wt, "queue::tests::alpha", "alpha mismatch at 0x7fff0001:");
    let (r1, _) = run_verify_gate_with_options(
        &wt,
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        opts(),
    )
    .await
    .expect("fail 1");
    assert!(!r1.ok);
    assert!(!r1.blocked);
    let l1 = lookup_task_lease(&wt, "origin", "T-1").unwrap().unwrap();
    assert_eq!(l1.record.verify_attempts, 1);
    assert_eq!(l1.record.verify_failures, 1);
    assert!(!l1.record.blocked);

    // Different failure B resets consecutive count to 1 (does NOT trip early).
    write_failing_script(&wt, "queue::tests::beta", "beta panic at 0x7fff0002:");
    let (r_diff, _) = run_verify_gate_with_options(
        &wt,
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        opts(),
    )
    .await
    .expect("diff failure resets streak");
    assert!(!r_diff.blocked);
    let l_diff = lookup_task_lease(&wt, "origin", "T-1").unwrap().unwrap();
    assert_eq!(l_diff.record.verify_attempts, 2);
    assert_eq!(l_diff.record.verify_failures, 1);
    assert!(!l_diff.record.blocked);

    // Second consecutive failure with B (different hex pointer still normalizes to same fingerprint).
    write_failing_script(&wt, "queue::tests::beta", "beta panic at 0x7fffdead:");
    let (r2, _) = run_verify_gate_with_options(
        &wt,
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        opts(),
    )
    .await
    .expect("fail 2 of B");
    assert!(!r2.blocked);
    let l2 = lookup_task_lease(&wt, "origin", "T-1").unwrap().unwrap();
    assert_eq!(l2.record.verify_failures, 2);
    assert!(!l2.record.blocked);

    // Third consecutive failure with B trips the circuit breaker and notifies `alice`.
    write_failing_script(&wt, "queue::tests::beta", "beta panic at 0x7fffbeef:");
    let (r3, _) = run_verify_gate_with_options(
        &wt,
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        opts(),
    )
    .await
    .expect("fail 3 trips breaker");
    assert!(!r3.ok);
    assert!(r3.blocked);
    let l3 = lookup_task_lease(&wt, "origin", "T-1").unwrap().unwrap();
    assert!(l3.record.blocked);
    assert_eq!(l3.record.verify_attempts, 4);
    assert_eq!(l3.record.verify_failures, 3);

    let alice_box = fix.hub_dir.join("mailbox/to_alice");
    let entries: Vec<_> = std::fs::read_dir(&alice_box)
        .expect("alice mailbox created")
        .filter_map(Result::ok)
        .collect();
    assert_eq!(entries.len(), 1);
    let notice = std::fs::read_to_string(entries[0].path()).unwrap();
    assert!(notice.contains("oxideswarm plan unblock T-1"), "{notice}");
    assert!(
        notice.contains(l3.record.last_fingerprint.as_deref().unwrap()),
        "{notice}"
    );

    // 4. Subsequent verify while blocked is refused before running tests or incrementing attempts.
    let err_blocked = run_verify_gate_with_options(
        &wt,
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        opts(),
    )
    .await
    .unwrap_err();
    assert!(
        err_blocked.contains("oxideswarm plan unblock T-1"),
        "{err_blocked}"
    );
    let l_still = lookup_task_lease(&wt, "origin", "T-1").unwrap().unwrap();
    assert_eq!(l_still.record.verify_attempts, 4);

    // 5. Person unblocks T-1, fix lands, verify succeeds cleanly.
    let unblocked = unblock_task_at(
        &wt,
        &UnblockOptions {
            task_id: "T-1".to_string(),
            remote: "origin".to_string(),
            by: Some("alice".to_string()),
            note: Some("fixed test".to_string()),
            ttl_minutes: DEFAULT_LEASE_TTL_MINS,
        },
        Utc::now(),
    )
    .expect("unblock T-1");
    assert!(!unblocked.lease.blocked);
    assert_eq!(unblocked.lease.verify_failures, 0);
    assert_eq!(unblocked.lease.verify_attempts, 4);

    std::fs::write(wt.join("src/run_check.sh"), "echo green\nexit 0\n").unwrap();
    let (r_pass, _) = run_verify_gate_with_options(
        &wt,
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        opts(),
    )
    .await
    .expect("verify after unblock succeeds");
    assert!(r_pass.ok);
    assert!(!r_pass.blocked);
    let l_pass = lookup_task_lease(&wt, "origin", "T-1").unwrap().unwrap();
    assert_eq!(l_pass.record.verify_attempts, 5);
    assert_eq!(l_pass.record.verify_failures, 0);
    assert!(!l_pass.record.blocked);

    // 6. Per-agent hourly verify cap refuses the 3rd invocation when max_verifies_per_hour = 2.
    let cap_opts = || VerifyGateOptions {
        remote: "origin".to_string(),
        agent_id: Some("agent-capped".to_string()),
        max_verifies_per_hour: 2,
        hub_dir: None,
        breaker_threshold: CIRCUIT_BREAKER_THRESHOLD,
    };
    assert!(run_verify_gate_with_options(
        &fix.clone,
        None,
        "T-CAP",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        cap_opts(),
    )
    .await
    .is_ok());
    assert!(run_verify_gate_with_options(
        &fix.clone,
        None,
        "T-CAP",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        cap_opts(),
    )
    .await
    .is_ok());
    let cap_err = run_verify_gate_with_options(
        &fix.clone,
        None,
        "T-CAP",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
        cap_opts(),
    )
    .await
    .unwrap_err();
    assert!(cap_err.contains("rate limit exceeded"), "{cap_err}");
}
