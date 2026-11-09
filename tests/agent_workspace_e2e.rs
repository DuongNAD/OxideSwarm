//! Design 0039 W6: End-to-end integration tests for agent workspaces (`submit --workspace` and
//! `oxideswarm mcp` over stdio).
//!
//! Covers the Design 0039 Test Plan integration flow:
//! - One master and one worker with workspaces enabled on loopback, with a temporary `HOME` and
//!   workspace directory.
//! - A small crate without dependencies builds with `cargo build --offline`.
//! - A second build reuses `target/` and prints no `Compiling` line.
//! - A file deleted locally is gone from the workspace after the next sync, and `target/` is
//!   still there.
//! - The same flow runs through `oxideswarm mcp` over stdio.

use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_master::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::{WorkerClient, WorkerConfig};
use serde_json::{json, Value};
use tokio::sync::watch;
use uuid::Uuid;

fn oxideswarm_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_oxideswarm"))
}

struct HarnessCluster {
    master: MasterHandle,
    master_addr: SocketAddr,
    worker_id: Uuid,
    workspace_root: PathBuf,
    worker_shutdown_tx: watch::Sender<bool>,
    worker_join: tokio::task::JoinHandle<()>,
    _temp_dir: tempfile::TempDir,
}

impl HarnessCluster {
    async fn spawn() -> Self {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let blob_dir = temp_dir.path().join("master_blobs");
        let sandbox_dir = temp_dir.path().join("worker_sandboxes");
        let workspace_root = temp_dir.path().join("worker_workspaces");

        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = ServerConfig::new(bind_addr).with_blob_store(&blob_dir, 128 * 1024 * 1024);
        let master = MasterServer::spawn(config)
            .await
            .expect("spawn MasterServer");
        let master_addr = master.server_addr();

        let worker_id = Uuid::new_v4();
        let mut worker = WorkerClient::new(
            WorkerConfig::new(master_addr.to_string())
                .with_worker_id(worker_id)
                .with_name("w6-workspace-worker")
                .with_cores(2)
                .with_no_battery(true)
                .with_no_gpu(true)
                .with_no_blender(true)
                .with_no_cuda(true)
                .with_redirection_portal(false)
                .with_sandbox_base_dir(&sandbox_dir)
                .with_workspaces(true)
                .with_workspace_root(Some(workspace_root.clone())),
        );
        let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
        let worker_join = tokio::spawn(async move {
            let _ = worker.run(worker_shutdown_rx).await;
        });

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if master
                .registry()
                .list_workers()
                .await
                .iter()
                .any(|w| w.worker_id == worker_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        Self {
            master,
            master_addr,
            worker_id,
            workspace_root,
            worker_shutdown_tx,
            worker_join,
            _temp_dir: temp_dir,
        }
    }

    async fn shutdown(self) {
        let _ = self.worker_shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), self.worker_join).await;
        let _ = self.master.shutdown();
    }
}

fn create_sample_crate(dir: &Path) {
    let src_dir = dir.join("src");
    std::fs::create_dir_all(&src_dir).expect("create src dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "w6_sample_app"
version = "0.1.0"
edition = "2021"
"#,
    )
    .expect("write Cargo.toml");
    std::fs::write(
        src_dir.join("main.rs"),
        r#"fn main() {
    println!("hello from w6 workspace");
}
"#,
    )
    .expect("write src/main.rs");
    std::fs::write(
        src_dir.join("obsolete.rs"),
        "// file that will be deleted before the third sync\n",
    )
    .expect("write src/obsolete.rs");
}

struct McpProcess {
    child: Child,
    stdin: ChildStdin,
    stdout_reader: BufReader<ChildStdout>,
}

impl McpProcess {
    fn spawn(master_addr: SocketAddr, cwd: &Path, client_home: &Path) -> Self {
        let mut cmd = Command::new(oxideswarm_bin());
        cmd.current_dir(cwd)
            .args(["mcp", "--master", &master_addr.to_string()])
            .env("HOME", client_home)
            .env("USERPROFILE", client_home)
            .env("OXIDESWARM_HOME", client_home.join(".oxideswarm"))
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("OXIDESWARM_P2P_TICKET")
            .env_remove("OXIDESWARM_MASTER")
            .env_remove("RUSTY_GRID_MASTER_ADDR")
            .env_remove("RUSTY_GRID_MASTER")
            .env_remove("RUSTY_GRID_CONFIG")
            .env_remove("RUSTY_GRID_INSECURE")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = cmd.spawn().expect("spawn oxideswarm mcp");
        let stdin = child.stdin.take().expect("take stdin");
        let stdout = child.stdout.take().expect("take stdout");
        Self {
            child,
            stdin,
            stdout_reader: BufReader::new(stdout),
        }
    }

    fn send(&mut self, msg: &Value) {
        let line = format!("{}\n", serde_json::to_string(msg).unwrap());
        self.stdin.write_all(line.as_bytes()).expect("write stdin");
        self.stdin.flush().expect("flush stdin");
    }

    fn recv_response(&mut self) -> Value {
        loop {
            let mut line = String::new();
            let n = self
                .stdout_reader
                .read_line(&mut line)
                .expect("read stdout line");
            assert!(n > 0, "unexpected EOF from oxideswarm mcp");
            let val: Value = serde_json::from_str(line.trim()).expect("parse JSON-RPC frame");
            if val.get("id").is_some() {
                return val;
            }
        }
    }
}

impl Drop for McpProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_workspace_cargo_build_offline_incremental_and_deleted_file_sync() {
    let cluster = HarnessCluster::spawn().await;
    let temp = tempfile::tempdir().expect("create client temp dir");
    let client_home = temp.path().join("client_home");
    let cargo_home = temp.path().join("cargo_home");
    let project_dir = temp.path().join("sample_crate");
    std::fs::create_dir_all(&client_home).expect("create client_home");
    std::fs::create_dir_all(&cargo_home).expect("create cargo_home");
    create_sample_crate(&project_dir);

    let cargo_home_env = format!("CARGO_HOME={}", cargo_home.display());
    let run_submit = || {
        Command::new(oxideswarm_bin())
            .current_dir(&project_dir)
            .args([
                "submit",
                "--master",
                &cluster.master_addr.to_string(),
                "--workspace",
                "w6-cli",
                "--env",
                &cargo_home_env,
                "--env",
                "CARGO_TARGET_DIR=target",
                "--env",
                "CARGO_TERM_COLOR=never",
                "--wait",
                "--",
                "cargo",
                "build",
                "--offline",
            ])
            .env("HOME", &client_home)
            .env("USERPROFILE", &client_home)
            .env("OXIDESWARM_HOME", client_home.join(".oxideswarm"))
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("OXIDESWARM_P2P_TICKET")
            .env_remove("OXIDESWARM_MASTER")
            .env_remove("RUSTY_GRID_MASTER_ADDR")
            .env_remove("RUSTY_GRID_MASTER")
            .output()
            .expect("run oxideswarm submit --workspace")
    };

    // 1. First build compiles the crate and creates target/ in the worker's workspace tree.
    let out1 = run_submit();
    let combined1 = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out1.stdout),
        String::from_utf8_lossy(&out1.stderr)
    );
    assert!(
        out1.status.success(),
        "first workspace cargo build failed:\n{combined1}"
    );
    assert!(
        combined1.contains("Compiling") && combined1.contains("w6_sample_app"),
        "first build must print Compiling line, got:\n{combined1}"
    );

    let ws_tree = cluster.workspace_root.join("w6-cli").join("tree");
    assert!(
        ws_tree.join("target").is_dir(),
        "target/ must exist in workspace tree after first build"
    );
    assert!(
        ws_tree.join("src").join("obsolete.rs").is_file(),
        "src/obsolete.rs must exist in workspace tree after first sync"
    );

    // 2. Second build reuses target/ and prints no Compiling line.
    let out2 = run_submit();
    let combined2 = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out2.stdout),
        String::from_utf8_lossy(&out2.stderr)
    );
    assert!(
        out2.status.success(),
        "second workspace cargo build failed:\n{combined2}"
    );
    assert!(
        !combined2.contains("Compiling"),
        "second build must reuse target/ and print no Compiling line, got:\n{combined2}"
    );

    // 3. Delete src/obsolete.rs locally and run again: the deleted file is removed from the
    //    workspace tree while target/ remains intact.
    std::fs::remove_file(project_dir.join("src").join("obsolete.rs"))
        .expect("remove local src/obsolete.rs");
    let out3 = run_submit();
    let combined3 = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out3.stdout),
        String::from_utf8_lossy(&out3.stderr)
    );
    assert!(
        out3.status.success(),
        "third workspace cargo build failed:\n{combined3}"
    );
    assert!(
        !ws_tree.join("src").join("obsolete.rs").exists(),
        "deleted local file must be removed from workspace tree after sync"
    );
    assert!(
        ws_tree.join("target").is_dir(),
        "untracked target/ directory must survive sync after file deletion"
    );

    // 4. `oxideswarm workspaces --json` lists the workspace affinity.
    let ws_list = Command::new(oxideswarm_bin())
        .args([
            "workspaces",
            "--master",
            &cluster.master_addr.to_string(),
            "--json",
        ])
        .env("HOME", &client_home)
        .env("USERPROFILE", &client_home)
        .env("OXIDESWARM_HOME", client_home.join(".oxideswarm"))
        .output()
        .expect("run oxideswarm workspaces --json");
    assert!(ws_list.status.success());
    let rows: Vec<Value> =
        serde_json::from_slice(&ws_list.stdout).expect("parse workspaces --json output");
    assert!(
        rows.iter()
            .any(|r| r["name"] == "w6-cli" && r["worker"] == cluster.worker_id.to_string()),
        "expected w6-cli affinity for worker {}, got: {rows:?}",
        cluster.worker_id
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_workspace_cargo_build_offline_incremental_and_deleted_file_sync() {
    let cluster = HarnessCluster::spawn().await;
    let temp = tempfile::tempdir().expect("create client temp dir");
    let client_home = temp.path().join("client_home");
    let cargo_home = temp.path().join("cargo_home");
    let project_dir = temp.path().join("mcp_sample_crate");
    std::fs::create_dir_all(&client_home).expect("create client_home");
    std::fs::create_dir_all(&cargo_home).expect("create cargo_home");
    create_sample_crate(&project_dir);

    let mut mcp = McpProcess::spawn(cluster.master_addr, &project_dir, &client_home);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "w6-e2e", "version": "1.0" }
        }
    }));
    let init_resp = mcp.recv_response();
    assert!(
        init_resp.get("result").is_some(),
        "initialize failed: {init_resp}"
    );
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }));

    let cargo_home_str = cargo_home.to_string_lossy().to_string();
    let submit_call = |id: u64| {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": "submit",
                "arguments": {
                    "type": "command",
                    "command": "cargo",
                    "args": ["build", "--offline"],
                    "env": {
                        "CARGO_HOME": cargo_home_str,
                        "CARGO_TARGET_DIR": "target",
                        "CARGO_TERM_COLOR": "never"
                    },
                    "workspace": {
                        "name": "w6-mcp",
                        "sync_dir": "."
                    },
                    "wait": true,
                    "wait_secs": 120
                }
            }
        })
    };

    // 1. First MCP build compiles the crate.
    mcp.send(&submit_call(2));
    let resp1 = mcp.recv_response();
    assert_eq!(
        resp1["result"]["isError"],
        json!(false),
        "first MCP workspace build failed: {resp1}"
    );
    let body1: Value = serde_json::from_str(
        resp1["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP content text"),
    )
    .expect("parse MCP submit payload");
    assert_eq!(body1["exit_code"], json!(0), "body1: {body1}");
    assert_eq!(body1["workspace"]["name"], json!("w6-mcp"));
    let stderr1 = body1["stderr"].as_str().unwrap_or("");
    let stdout1 = body1["stdout"].as_str().unwrap_or("");
    assert!(
        (stderr1.contains("Compiling") && stderr1.contains("w6_sample_app"))
            || (stdout1.contains("Compiling") && stdout1.contains("w6_sample_app")),
        "first MCP build must print Compiling line, got: {body1}"
    );

    let ws_tree = cluster.workspace_root.join("w6-mcp").join("tree");
    assert!(ws_tree.join("target").is_dir());
    assert!(ws_tree.join("src").join("obsolete.rs").is_file());

    // 2. Second MCP build reuses target/ and prints no Compiling line.
    mcp.send(&submit_call(3));
    let resp2 = mcp.recv_response();
    assert_eq!(
        resp2["result"]["isError"],
        json!(false),
        "second MCP workspace build failed: {resp2}"
    );
    let body2: Value = serde_json::from_str(
        resp2["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP content text"),
    )
    .expect("parse MCP submit payload");
    assert_eq!(body2["exit_code"], json!(0), "body2: {body2}");
    assert_eq!(
        body2["workspace"]["uploaded_bytes"],
        json!(0),
        "second build with unchanged files should upload 0 new bytes: {body2}"
    );
    let stderr2 = body2["stderr"].as_str().unwrap_or("");
    let stdout2 = body2["stdout"].as_str().unwrap_or("");
    assert!(
        !stderr2.contains("Compiling") && !stdout2.contains("Compiling"),
        "second MCP build must print no Compiling line, got: {body2}"
    );

    // 3. Delete src/obsolete.rs locally and run a third MCP build: src/obsolete.rs is removed
    //    from the workspace tree and target/ is still present.
    std::fs::remove_file(project_dir.join("src").join("obsolete.rs"))
        .expect("remove local src/obsolete.rs");
    mcp.send(&submit_call(4));
    let resp3 = mcp.recv_response();
    assert_eq!(
        resp3["result"]["isError"],
        json!(false),
        "third MCP workspace build failed: {resp3}"
    );
    let body3: Value = serde_json::from_str(
        resp3["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP content text"),
    )
    .expect("parse MCP submit payload");
    assert_eq!(body3["exit_code"], json!(0), "body3: {body3}");
    assert!(
        !ws_tree.join("src").join("obsolete.rs").exists(),
        "deleted file must be gone from MCP workspace tree after sync"
    );
    assert!(
        ws_tree.join("target").is_dir(),
        "target/ must remain in MCP workspace tree after sync"
    );

    // 4. `workspaces` MCP tool lists `w6-mcp`.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "workspaces",
            "arguments": {}
        }
    }));
    let ws_resp = mcp.recv_response();
    assert_eq!(ws_resp["result"]["isError"], json!(false));
    let rows: Vec<Value> = serde_json::from_str(
        ws_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("workspaces tool text"),
    )
    .expect("parse workspaces tool payload");
    assert!(
        rows.iter()
            .any(|r| r["name"] == "w6-mcp" && r["worker"] == cluster.worker_id.to_string()),
        "expected w6-mcp in MCP workspaces tool output, got: {rows:?}"
    );

    drop(mcp);
    cluster.shutdown().await;
}
