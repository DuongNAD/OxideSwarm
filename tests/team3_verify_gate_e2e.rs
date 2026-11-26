//! Integration tests for TEAM-3 verify gate (`oxideswarm verify --task <id>` & MCP `verify`,
//! design 0111 D4 / T3).

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_cli::blobs::WorkspaceLimits;
use rusty_grid_cli::mcp::{call_tool_with_context, McpState, ToolCallContext, ToolContent};
use rusty_grid_cli::verify_gate::{
    run_verify_gate, VerifyFailureKind, MAX_VERIFY_JSON_BYTES, VERIFY_RECORD_REL_DIR,
};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

struct VerifyCluster {
    master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
}

impl VerifyCluster {
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

    fn spawn_workspace_worker(&mut self, name: &str) -> uuid::Uuid {
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
        let id = client.worker_id();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((tx, handle));
        id
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

impl Drop for VerifyCluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

fn init_test_repo(dir: &Path) {
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["init", "-b", "master"])
        .output()
        .unwrap()
        .status
        .success());
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["config", "user.email", "test@example.com"])
        .output()
        .unwrap();
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["config", "user.name", "Test"])
        .output()
        .unwrap();

    std::fs::create_dir_all(dir.join(".oxideswarm")).unwrap();
    std::fs::create_dir_all(dir.join("src")).unwrap();
    std::fs::write(
        dir.join(".oxideswarm/plan.toml"),
        r#"[project]
base = "master"

[profile.unit]
command = ["sh", "src/run_check.sh"]

[[task]]
id = "T-1"
title = "Sample check task"
status = "open"
owned_paths = ["src/**"]
verify = ["unit"]
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("src/run_check.sh"),
        "cat <<'EOF'\nrunning 1 test\ntest queue::tests::cancel_counts ... FAILED\n\nfailures:\n\n---- queue::tests::cancel_counts stdout ----\nthread 'queue::tests::cancel_counts' panicked at src/queue.rs:42:5:\nassertion `left == right` failed\n  left: 2\n right: 1\nEOF\nexit 101\n",
    )
    .unwrap();
    std::fs::write(dir.join("README.md"), "# initial\n").unwrap();

    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["add", "-A"])
        .output()
        .unwrap();
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["commit", "-m", "initial"])
        .output()
        .unwrap()
        .status
        .success());
}

#[cfg(unix)]
#[tokio::test]
async fn t3_verify_gate_failing_and_passing_task_on_loopback_cluster() {
    let mut cluster = VerifyCluster::new().await;
    let worker_id = cluster.spawn_workspace_worker("ws-worker");
    cluster.wait_for_workers(1).await;

    let repo_dir = tempfile::tempdir().expect("repo dir");
    init_test_repo(repo_dir.path());

    // 1. Failing worktree returns <= 4 KiB failure JSON with worker, link, exit=101, and parsed test failure.
    let (report, compact_json) = run_verify_gate(
        repo_dir.path(),
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
    )
    .await
    .expect("verify gate executes");

    assert!(compact_json.len() <= MAX_VERIFY_JSON_BYTES);
    assert!(!report.ok);
    assert_eq!(report.task, "T-1");
    assert_eq!(report.cells.len(), 1);
    let cell = &report.cells[0];
    assert_eq!(cell.profile, "unit");
    assert_eq!(
        cell.worker.as_deref(),
        Some("ws-worker"),
        "report={compact_json}"
    );
    assert_eq!(
        cell.worker_id.as_deref(),
        Some(worker_id.to_string().as_str())
    );
    assert_eq!(cell.link.as_deref(), Some("tcp"));
    assert_eq!(cell.exit, 101);
    assert!(cell.task_id.is_some());
    assert_eq!(cell.failures.len(), 1);
    assert_eq!(cell.failures[0].kind, VerifyFailureKind::Test);
    assert_eq!(
        cell.failures[0].name.as_deref(),
        Some("queue::tests::cancel_counts")
    );
    assert_eq!(cell.failures[0].file.as_deref(), Some("src/queue.rs"));
    assert_eq!(cell.failures[0].line, Some(42));

    let record_file = repo_dir.path().join(VERIFY_RECORD_REL_DIR).join("T-1.json");
    assert!(record_file.is_file());
    let rec_val: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&record_file).unwrap()).unwrap();
    assert_eq!(rec_val["ok"], false);
    assert!(rec_val["first_fingerprint"].is_string());

    // 2. Fix inside owned_paths (`src/run_check.sh`) without committing: worktree syncs dirty fix & passes.
    std::fs::write(
        repo_dir.path().join("src/run_check.sh"),
        "echo all green\nexit 0\n",
    )
    .unwrap();
    let (pass_report, pass_json) = run_verify_gate(
        repo_dir.path(),
        None,
        "T-1",
        &[],
        None,
        &cluster.addr,
        None,
        Duration::from_secs(30),
        WorkspaceLimits::default(),
    )
    .await
    .expect("verify gate re-runs dirty worktree");
    assert!(pass_json.len() <= MAX_VERIFY_JSON_BYTES);
    assert!(pass_report.ok);
    assert!(pass_report.commit.contains("+dirty-"));
    assert_eq!(pass_report.cells[0].exit, 0);
    assert!(pass_report.cells[0].failures.is_empty());

    // 3. MCP `verify` tool also executes cleanly via McpState against a repo subdirectory.
    let parent = repo_dir.path().parent().unwrap();
    let rel_repo = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let state = McpState::new(cluster.addr.clone(), None);
    let mut ctx = ToolCallContext::default();
    let args = serde_json::json!({
        "task": "T-1",
        "repo_dir": rel_repo,
    });
    // Temporarily invoke helper directly on `parent` so cwd confinement checks `repo_dir`.
    let orig_cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(parent).unwrap();
    let mcp_res = call_tool_with_context(&state, "verify", &args, &mut ctx).await;
    std::env::set_current_dir(orig_cwd).unwrap();
    assert!(!mcp_res.is_error, "{mcp_res:?}");
    let ToolContent::Text { text } = &mcp_res.content[0];
    let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed["ok"], true);
    assert!(text.len() <= MAX_VERIFY_JSON_BYTES);
}

#[tokio::test]
async fn t3_verify_gate_refuses_changes_outside_owned_paths() {
    let repo_dir = tempfile::tempdir().expect("repo dir");
    init_test_repo(repo_dir.path());
    std::fs::write(
        repo_dir.path().join("README.md"),
        "# modified out of scope\n",
    )
    .unwrap();

    let err = run_verify_gate(
        repo_dir.path(),
        None,
        "T-1",
        &[],
        None,
        "127.0.0.1:1",
        None,
        Duration::from_secs(10),
        WorkspaceLimits::default(),
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("outside task owned_paths") && err.contains("README.md"),
        "{err}"
    );
}
