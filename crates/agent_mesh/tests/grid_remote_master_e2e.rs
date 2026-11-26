//! End-to-end integration test for `MESH-3`:
//! Attaches `agent-mesh hub --grid-connect` to a separately spawned `oxideswarm master` +
//! `oxideswarm worker`, verifying `/api/nodes` online/offline transitions, `supported_commands`,
//! placement + tags, inline `files` + `outputs` + SHA-256 `artifacts`, `grid_result`,
//! `grid_cancel`, hub-local path refusal, and fail-closed auth.

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use agent_mesh::{AgentMeshClient, AgentMeshEnvelope};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rusty_grid_core::cluster_auth::ClusterSecret;
use tempfile::TempDir;

struct ChildGuard {
    label: &'static str,
    child: Option<Child>,
}

impl ChildGuard {
    fn new(label: &'static str, child: Child) -> Self {
        Self {
            label,
            child: Some(child),
        }
    }

    fn stop_now(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.label;
    }
}

fn spawn_drain_thread<R: Read + Send + 'static>(mut reader: R) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = reader.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
    });
}

fn locate_or_build_oxideswarm_bin() -> std::path::PathBuf {
    static BIN_PATH: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    BIN_PATH
        .get_or_init(|| {
            if let Ok(p) = std::env::var("CARGO_BIN_EXE_oxideswarm") {
                let pb = std::path::PathBuf::from(p);
                if pb.is_file() {
                    return pb;
                }
            }
            let exe =
                std::env::current_exe().expect("failed to resolve current test executable path");
            let target_profile_dir = exe
                .parent()
                .and_then(|deps| deps.parent())
                .expect("failed to resolve target/<profile> directory from current_exe");
            let bin_name = format!("oxideswarm{}", std::env::consts::EXE_SUFFIX);
            let bin_path = target_profile_dir.join(&bin_name);
            let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(|p| p.parent())
                .expect("failed to resolve workspace root from CARGO_MANIFEST_DIR");
            let mtime = |p: &std::path::Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
            let bin_fresh = mtime(&bin_path).is_some_and(|bm| {
                [
                    workspace_root.join("crates/core/src/protocol.rs"),
                    workspace_root.join("crates/cli/src/cli_app.rs"),
                    workspace_root.join("crates/master/src/store.rs"),
                ]
                .iter()
                .all(|src| mtime(src).is_some_and(|sm| bm >= sm))
            });
            let bin_usable = bin_fresh
                && Command::new(&bin_path)
                    .args(["master", "--help"])
                    .output()
                    .map(|o| o.status.success())
                    .unwrap_or(false);
            if bin_usable {
                return bin_path;
            }
            let cargo =
                std::env::var_os("CARGO").unwrap_or_else(|| std::ffi::OsString::from("cargo"));
            let mut cmd = Command::new(&cargo);
            cmd.arg("build")
                .arg("-p")
                .arg("rusty_grid_cli")
                .arg("--bin")
                .arg("oxideswarm")
                .arg("--locked")
                .current_dir(workspace_root);
            if target_profile_dir
                .file_name()
                .is_some_and(|name| name == "release")
            {
                cmd.arg("--release");
            }
            let status = cmd.status().unwrap_or_else(|e| {
                panic!(
                    "failed to invoke {:?} to build oxideswarm at {}: {e}",
                    cargo,
                    bin_path.display()
                )
            });
            assert!(
                status.success() && bin_path.is_file(),
                "failed to build or locate oxideswarm binary at {} (status: {status})",
                bin_path.display()
            );
            bin_path
        })
        .clone()
}

struct IsolatedEnv {
    _root: TempDir,
    home: std::path::PathBuf,
    tmp: std::path::PathBuf,
    fake_bin: std::path::PathBuf,
    secret_path: std::path::PathBuf,
    secret_hex: String,
}

impl IsolatedEnv {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create isolated tempdir");
        let home = root.path().join("home");
        let oxideswarm_dir = home.join(".oxideswarm");
        let tmp = root.path().join("tmp");
        let fake_bin = root.path().join("fake_bin");
        std::fs::create_dir_all(&oxideswarm_dir).unwrap();
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::create_dir_all(&fake_bin).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let pmset = fake_bin.join("pmset");
            std::fs::write(
                &pmset,
                "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\nexit 0\n",
            )
            .unwrap();
            let mut perms = std::fs::metadata(&pmset).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&pmset, perms).unwrap();
        }

        let secret = ClusterSecret::generate().expect("generate secret");
        let secret_hex = secret.to_hex();
        let secret_path = root.path().join("cluster.secret");
        secret
            .save_file(&secret_path, false)
            .expect("save secret file");

        Self {
            _root: root,
            home,
            tmp,
            fake_bin,
            secret_path,
            secret_hex,
        }
    }

    fn apply(&self, cmd: &mut Command) {
        let env_keys: Vec<std::ffi::OsString> = std::env::vars_os().map(|(k, _)| k).collect();
        for key in env_keys {
            let s = key.to_string_lossy();
            if s.starts_with("RUSTY_GRID_")
                || s.starts_with("OXIDESWARM_")
                || s.starts_with("OXIDE_")
                || s.starts_with("OX_SHARD_")
            {
                cmd.env_remove(&key);
            }
        }
        let orig_path = std::env::var_os("PATH").unwrap_or_default();
        let joined_path = std::env::join_paths(
            std::iter::once(self.fake_bin.clone()).chain(std::env::split_paths(&orig_path)),
        )
        .expect("join PATH");

        let real_home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
        if let Some(p) = std::env::var_os("RUSTUP_HOME").or_else(|| {
            real_home
                .as_ref()
                .map(|h| std::path::PathBuf::from(h).join(".rustup"))
                .filter(|d| d.is_dir())
                .map(|d| d.into_os_string())
        }) {
            cmd.env("RUSTUP_HOME", p);
        }
        if let Some(p) = std::env::var_os("CARGO_HOME").or_else(|| {
            real_home
                .as_ref()
                .map(|h| std::path::PathBuf::from(h).join(".cargo"))
                .filter(|d| d.is_dir())
                .map(|d| d.into_os_string())
        }) {
            cmd.env("CARGO_HOME", p);
        }

        cmd.env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("OXIDESWARM_HOME", self.home.join(".oxideswarm"))
            .env("TMPDIR", &self.tmp)
            .env("PATH", joined_path)
            .env("RUSTY_GRID_NO_BATTERY", "1")
            .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
            .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
            .env("RUSTY_GRID_P2P_MINIMAL", "1")
            .env("NO_COLOR", "1");
    }
}

fn wait_for_port_file(path: &std::path::Path) -> u16 {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(content) = std::fs::read_to_string(path) {
            if let Ok(port) = content.trim().parse::<u16>() {
                if port > 0 {
                    return port;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for master port file at {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn spawn_agent_mesh_hub_connect(
    env: &IsolatedEnv,
    master_addr: &str,
) -> (ChildGuard, String, String) {
    let bin = env!("CARGO_BIN_EXE_agent-mesh");
    let mut cmd = Command::new(bin);
    env.apply(&mut cmd);
    cmd.args([
        "hub",
        "--listen",
        "127.0.0.1:0",
        "--grid-connect",
        master_addr,
        "--secret-file",
        env.secret_path.to_str().unwrap(),
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn agent-mesh hub --grid-connect");
    let stdout = child.stdout.take().expect("hub stdout");
    let stderr = child.stderr.take().expect("hub stderr");
    spawn_drain_thread(stderr);

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let ws_url = loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for agent-mesh hub startup banner"
        );
        line.clear();
        let n = reader.read_line(&mut line).expect("read hub stdout");
        assert!(n > 0, "agent-mesh hub exited before printing ws URL");
        if line.contains("[HUB] OxideRelay Hub running at ") {
            if let Some(pos) = line.find("ws://") {
                break line[pos..].trim().to_string();
            }
        }
    };
    let port_str = ws_url
        .trim_start_matches("ws://127.0.0.1:")
        .trim_end_matches("/ws");
    let port: u16 = port_str.parse().expect("parse hub port");
    let http_base = format!("http://127.0.0.1:{port}");
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });

    (ChildGuard::new("agent-mesh-hub", child), ws_url, http_base)
}

async fn fetch_grid_node(http_base: &str) -> Option<serde_json::Value> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{http_base}/api/nodes"))
        .send()
        .await
        .ok()?;
    let nodes = resp.json::<Vec<serde_json::Value>>().await.ok()?;
    nodes
        .into_iter()
        .find(|n| n.get("node_id").and_then(|v| v.as_str()) == Some("grid"))
}

async fn wait_for_grid_status(http_base: &str, expected_status: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(node) = fetch_grid_node(http_base).await {
            if node.get("status").and_then(|v| v.as_str()) == Some(expected_status) {
                return node;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for /api/nodes 'grid' status == '{expected_status}'"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_worker_ready(client: &AgentMeshClient) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(payload),
            ..
        }) = client.query_grid_status(Some(5000)).await
        {
            if status == "success"
                && payload
                    .get("active_workers")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
                    >= 1
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for worker to register with external master"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn test_mesh3_remote_master_bridge_full_e2e() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let env = IsolatedEnv::new();
    let oxideswarm_bin = locate_or_build_oxideswarm_bin();
    let port_file = env._root.path().join("master.port");

    // 1. Start separate `oxideswarm master`.
    let mut master_cmd = Command::new(&oxideswarm_bin);
    env.apply(&mut master_cmd);
    master_cmd
        .args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "master",
            "--listen",
            "127.0.0.1:0",
            "--port-file",
            port_file.to_str().unwrap(),
            "--discovery-port",
            "0",
            "--max-host-cpu-pct",
            "100",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut master_child = master_cmd.spawn().expect("spawn oxideswarm master");
    spawn_drain_thread(master_child.stdout.take().unwrap());
    spawn_drain_thread(master_child.stderr.take().unwrap());
    let mut master_guard = ChildGuard::new("oxideswarm-master", master_child);

    let master_port = wait_for_port_file(&port_file);
    let master_addr = format!("127.0.0.1:{master_port}");

    // 2. Start separate `oxideswarm worker` with custom tag `mesh-tier-alpha`.
    let mut worker_cmd = Command::new(&oxideswarm_bin);
    env.apply(&mut worker_cmd);
    worker_cmd
        .args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "worker",
            "--master",
            &master_addr,
            "--name",
            "e2e-mesh-worker",
            "--tag",
            "mesh-tier-alpha",
            "--no-gpu",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut worker_child = worker_cmd.spawn().expect("spawn oxideswarm worker");
    spawn_drain_thread(worker_child.stdout.take().unwrap());
    spawn_drain_thread(worker_child.stderr.take().unwrap());
    let mut worker_guard = ChildGuard::new("oxideswarm-worker", worker_child);

    // 3. Start `agent-mesh hub --grid-connect <HOST:PORT>`.
    let (_hub_guard, hub_ws_url, hub_http_base) = spawn_agent_mesh_hub_connect(&env, &master_addr);

    // Verify /api/nodes reports "grid" online and supported_commands includes grid_result/grid_cancel.
    let grid_node = wait_for_grid_status(&hub_http_base, "online").await;
    let supported: Vec<String> = grid_node["capabilities"]["supported_commands"]
        .as_array()
        .expect("supported_commands array")
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    for required_cmd in [
        "grid_submit",
        "grid_result",
        "result",
        "grid_cancel",
        "cancel",
    ] {
        assert!(
            supported.iter().any(|c| c == required_cmd),
            "missing '{required_cmd}' in supported_commands: {supported:?}"
        );
    }

    let client = AgentMeshClient::new(&hub_ws_url, "mesh3-e2e-agent", "rust");
    client.connect().await.expect("connect AgentMeshClient");
    wait_for_worker_ready(&client).await;

    // 4. `grid_submit` with caller tags, placement, inline files (text + base64 + executable),
    //    and expected outputs -> SHA-256 verified artifacts.
    let run_sh =
        "#!/bin/sh\nset -e\nmkdir -p out\necho \"arg=$1\"\ncat inputs/raw.bin > out/artifact.bin\n";
    let raw_input = b"OxideSwarm-MESH3-Remote-Payload";
    let submit_resp = client
        .submit_grid(
            serde_json::json!({
                "program": "sh",
                "args": ["scripts/run.sh", "mesh3"],
                "tags": ["caller-trace-tag"],
                "placement": {
                    "os": std::env::consts::OS,
                    "require_tags": ["mesh-tier-alpha"]
                },
                "files": [
                    {
                        "path": "scripts/run.sh",
                        "text": run_sh,
                        "executable": true
                    },
                    {
                        "path": "inputs/raw.bin",
                        "base64": STANDARD.encode(raw_input)
                    }
                ],
                "outputs": ["out/artifact.bin"],
                "wait": true
            }),
            Some(20_000),
        )
        .await
        .expect("grid_submit response");

    match &submit_resp {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            stderr,
            ..
        } => {
            assert_eq!(status, "success", "stderr={stderr}");
            assert_eq!(*exit_code, 0, "stderr={stderr}");
            assert!(stdout.contains("arg=mesh3"), "stdout={stdout}");
        }
        other => panic!("expected CommandResponse, got {other:?}"),
    }
    let artifacts = AgentMeshClient::extract_output_artifacts(&submit_resp)
        .expect("extract and verify artifacts");
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].path, "out/artifact.bin");
    assert_eq!(artifacts[0].bytes, raw_input);

    // 5. Security: hub-local string path in `files` must be refused without submitting.
    let bad_files_resp = client
        .submit_grid(
            serde_json::json!({
                "script": "cat /etc/passwd",
                "files": ["/etc/passwd"],
                "wait": false
            }),
            Some(5_000),
        )
        .await
        .expect("bad files response");
    match bad_files_resp {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stderr,
            ..
        } => {
            assert_eq!(status, "failed");
            assert_ne!(exit_code, 0);
            assert!(
                stderr.contains("hub-local file paths are not accepted"),
                "{stderr}"
            );
        }
        other => panic!("expected CommandResponse refusal, got {other:?}"),
    }

    // 5b. Unsupported `workspace` (alone and with `files` + `outputs`) must be rejected upfront
    //     over `--grid-connect` without submitting any task.
    let queue_total_before_ws = match client
        .query_grid_status(Some(5_000))
        .await
        .expect("query_grid_status before workspace probe")
    {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            p["queue"]["total"].as_u64().expect("queue.total")
        }
        other => panic!("expected CommandResponse, got {other:?}"),
    };
    for ws_payload in [
        serde_json::json!({
            "script": "echo x",
            "workspace": "ws-1",
            "files": [{"path": "a.txt", "text": "x"}],
            "outputs": ["out.txt"],
            "wait": false
        }),
        serde_json::json!({
            "script": "echo x",
            "workspace": {"name": "ws-1"},
            "wait": false
        }),
    ] {
        let ws_resp = client
            .submit_grid(ws_payload.clone(), Some(5_000))
            .await
            .expect("workspace rejection response");
        match ws_resp {
            AgentMeshEnvelope::CommandResponse {
                status,
                exit_code,
                stderr,
                ..
            } => {
                assert_eq!(status, "failed", "payload={ws_payload}");
                assert_eq!(exit_code, 1, "payload={ws_payload}");
                assert!(
                    stderr.contains("workspace"),
                    "expected workspace error, got: {stderr}"
                );
            }
            other => panic!("expected CommandResponse refusal, got {other:?}"),
        }
    }
    let queue_total_after_ws = match client
        .query_grid_status(Some(5_000))
        .await
        .expect("query_grid_status after workspace probe")
    {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            p["queue"]["total"].as_u64().expect("queue.total")
        }
        other => panic!("expected CommandResponse, got {other:?}"),
    };
    assert_eq!(
        queue_total_before_ws, queue_total_after_ws,
        "workspace-rejected submissions must not enqueue tasks on remote master"
    );

    // 6. Async `grid_submit(wait=false)` -> `grid_result(wait=true)` with artifact extraction.
    let async_resp = client
        .submit_grid(
            serde_json::json!({
                "script": "mkdir -p out && printf 'async-artifact-data' > out/async.txt",
                "outputs": ["out/async.txt"],
                "wait": false
            }),
            Some(5_000),
        )
        .await
        .expect("async grid_submit");
    let async_task_id = match async_resp {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            p["task_id"].as_str().expect("task_id").to_string()
        }
        other => panic!("expected async CommandResponse, got {other:?}"),
    };

    let result_resp = client
        .get_grid_result(&async_task_id, true, Some(20_000))
        .await
        .expect("grid_result(wait=true)");
    match &result_resp {
        AgentMeshEnvelope::CommandResponse {
            status, exit_code, ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(*exit_code, 0);
        }
        other => panic!("expected completed CommandResponse from grid_result, got {other:?}"),
    }
    let async_artifacts =
        AgentMeshClient::extract_output_artifacts(&result_resp).expect("extract async artifact");
    assert_eq!(async_artifacts.len(), 1);
    assert_eq!(async_artifacts[0].path, "out/async.txt");
    assert_eq!(async_artifacts[0].bytes, b"async-artifact-data");

    // 7. Non-matching placement holds/times out when wait=true and can be cancelled with grid_cancel.
    let held_submit = client
        .submit_grid(
            serde_json::json!({
                "script": "echo should-never-run",
                "placement": {
                    "require_tags": ["nonexistent-tier-xyz"]
                },
                "wait": true
            }),
            Some(1_500),
        )
        .await;
    // Wait timeout on client or bridge returns either timeout error or failed CommandResponse(124).
    if let Ok(AgentMeshEnvelope::CommandResponse { status, .. }) = held_submit {
        assert_eq!(status, "failed");
    }

    let queued_unmatchable = client
        .submit_grid(
            serde_json::json!({
                "script": "echo queued-for-cancel",
                "placement": {
                    "require_tags": ["nonexistent-tier-xyz"]
                },
                "wait": false
            }),
            Some(5_000),
        )
        .await
        .expect("submit unmatchable async task");
    let cancel_task_id = match queued_unmatchable {
        AgentMeshEnvelope::CommandResponse {
            payload: Some(p), ..
        } => p["task_id"].as_str().expect("task_id").to_string(),
        other => panic!("expected submitted CommandResponse, got {other:?}"),
    };

    let cancel_resp_1 = client
        .cancel_grid_task(&cancel_task_id, Some(5_000))
        .await
        .expect("first cancel_grid_task");
    match cancel_resp_1 {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(p["cancelled"], serde_json::json!(true));
        }
        other => panic!("expected cancel CommandResponse, got {other:?}"),
    }

    // Second cancel on already-cancelled task returns cancelled: false.
    let cancel_resp_2 = client
        .cancel_grid_task(&cancel_task_id, Some(5_000))
        .await
        .expect("second cancel_grid_task");
    match cancel_resp_2 {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(p["cancelled"], serde_json::json!(false));
        }
        other => panic!("expected second cancel CommandResponse, got {other:?}"),
    }

    // 8. Stop external worker & master, then verify /api/nodes flips "grid" to "offline".
    client.close().await;
    worker_guard.stop_now();
    master_guard.stop_now();
    let offline_node = wait_for_grid_status(&hub_http_base, "offline").await;
    assert_eq!(offline_node["status"], "offline");
}

#[tokio::test]
async fn test_mesh3_hub_grid_connect_fails_fast_on_wrong_secret() {
    let env = IsolatedEnv::new();
    let oxideswarm_bin = locate_or_build_oxideswarm_bin();
    let port_file = env._root.path().join("master.port");

    let mut master_cmd = Command::new(&oxideswarm_bin);
    env.apply(&mut master_cmd);
    master_cmd
        .args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "master",
            "--listen",
            "127.0.0.1:0",
            "--port-file",
            port_file.to_str().unwrap(),
            "--discovery-port",
            "0",
            "--max-host-cpu-pct",
            "100",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut master_child = master_cmd.spawn().expect("spawn oxideswarm master");
    spawn_drain_thread(master_child.stdout.take().unwrap());
    spawn_drain_thread(master_child.stderr.take().unwrap());
    let mut master_guard = ChildGuard::new("oxideswarm-master", master_child);

    let master_port = wait_for_port_file(&port_file);
    let master_addr = format!("127.0.0.1:{master_port}");

    let wrong_secret = ClusterSecret::generate().unwrap();
    assert_ne!(wrong_secret.to_hex(), env.secret_hex);

    let mut hub_cmd = Command::new(env!("CARGO_BIN_EXE_agent-mesh"));
    env.apply(&mut hub_cmd);
    hub_cmd
        .args([
            "hub",
            "--listen",
            "127.0.0.1:0",
            "--grid-connect",
            &master_addr,
            "--secret",
            &wrong_secret.to_hex(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = hub_cmd
        .output()
        .expect("run agent-mesh hub with wrong secret");
    assert!(
        !out.status.success(),
        "agent-mesh hub --grid-connect must fail fast on wrong cluster secret"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Authentication")
            || stderr.contains("AuthFailed")
            || stderr.contains("secret"),
        "expected auth failure in stderr, got: {stderr}"
    );
    master_guard.stop_now();
}

#[cfg(unix)]
#[test]
fn test_mesh3_hub_grid_connect_exits_zero_on_sigterm() {
    let env = IsolatedEnv::new();
    let unused_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("listener local_addr").port()
    };
    let dead_master_addr = format!("127.0.0.1:{unused_port}");

    let mut hub_cmd = Command::new(env!("CARGO_BIN_EXE_agent-mesh"));
    env.apply(&mut hub_cmd);
    hub_cmd
        .args([
            "hub",
            "--listen",
            "127.0.0.1:0",
            "--grid-connect",
            &dead_master_addr,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = hub_cmd
        .spawn()
        .expect("spawn agent-mesh hub --grid-connect");
    let pid = child.id();
    let stdout = child.stdout.take().expect("hub stdout");
    let stderr = child.stderr.take().expect("hub stderr");
    spawn_drain_thread(stderr);

    let mut guard = ChildGuard::new("agent-mesh-hub-sigterm", child);

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut hub_port: Option<u16> = None;
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for agent-mesh hub startup banner"
        );
        line.clear();
        let n = reader.read_line(&mut line).expect("read hub stdout");
        assert!(
            n > 0,
            "agent-mesh hub exited before finishing startup banner"
        );
        if line.contains("[HUB] OxideRelay Hub running at ") {
            if let Some(pos) = line.find("ws://127.0.0.1:") {
                let port_str = line[pos..]
                    .trim()
                    .trim_start_matches("ws://127.0.0.1:")
                    .trim_end_matches("/ws");
                hub_port = Some(port_str.parse::<u16>().expect("parse hub port"));
            }
        }
        if line.contains("[HUB] Token authentication:") {
            break;
        }
    }
    let hub_port = hub_port.expect("hub startup banner did not report ws://127.0.0.1:<port>/ws");
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });

    // Confirm the hub socket is actually listening before sending SIGTERM.
    let listen_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", hub_port)).is_ok() {
            break;
        }
        assert!(
            Instant::now() < listen_deadline,
            "timed out waiting for hub TCP port {hub_port} to listen"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // Brief yield so main() enters wait_for_hub_shutdown_signal() after printing the banner.
    std::thread::sleep(Duration::from_millis(50));

    let kill_status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM via kill");
    assert!(kill_status.success(), "kill -TERM failed: {kill_status}");

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        let child_ref = guard.child.as_mut().expect("child present");
        match child_ref.try_wait().expect("try_wait hub child") {
            Some(status) => {
                guard.child.take();
                break status;
            }
            None => {
                if Instant::now() >= exit_deadline {
                    guard.stop_now();
                    panic!("agent-mesh hub did not exit within 5s after SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    };

    assert_eq!(
        status.code(),
        Some(0),
        "expected agent-mesh hub to exit 0 on SIGTERM, got status: {status}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_mesh3_sigterm_during_inflight_remote_master_rpcs_exits_zero() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let env = IsolatedEnv::new();
    let oxideswarm_bin = locate_or_build_oxideswarm_bin();
    let port_file = env._root.path().join("master.port");

    let mut master_cmd = Command::new(&oxideswarm_bin);
    env.apply(&mut master_cmd);
    master_cmd
        .args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "master",
            "--listen",
            "127.0.0.1:0",
            "--port-file",
            port_file.to_str().unwrap(),
            "--discovery-port",
            "0",
            "--max-host-cpu-pct",
            "100",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut master_child = master_cmd.spawn().expect("spawn oxideswarm master");
    spawn_drain_thread(master_child.stdout.take().unwrap());
    spawn_drain_thread(master_child.stderr.take().unwrap());
    let mut master_guard = ChildGuard::new("oxideswarm-master", master_child);
    let master_port = wait_for_port_file(&port_file);
    let master_addr = format!("127.0.0.1:{master_port}");

    let mut worker_cmd = Command::new(&oxideswarm_bin);
    env.apply(&mut worker_cmd);
    worker_cmd
        .args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "worker",
            "--master",
            &master_addr,
            "--name",
            "sigterm-worker",
            "--max-concurrency",
            "4",
            "--no-gpu",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut worker_child = worker_cmd.spawn().expect("spawn oxideswarm worker");
    spawn_drain_thread(worker_child.stdout.take().unwrap());
    spawn_drain_thread(worker_child.stderr.take().unwrap());
    let mut worker_guard = ChildGuard::new("oxideswarm-worker", worker_child);

    // Spawn hub via OXIDE_GRID_CONNECT and retain the Child handle so we can send SIGTERM.
    let bin = env!("CARGO_BIN_EXE_agent-mesh");
    let mut hub_cmd = Command::new(bin);
    env.apply(&mut hub_cmd);
    hub_cmd
        .env("OXIDE_GRID_CONNECT", &master_addr)
        .args([
            "hub",
            "--listen",
            "127.0.0.1:0",
            "--secret-file",
            env.secret_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut hub_child = hub_cmd.spawn().expect("spawn agent-mesh hub");
    let hub_pid = hub_child.id();
    let stdout = hub_child.stdout.take().unwrap();
    spawn_drain_thread(hub_child.stderr.take().unwrap());
    let mut hub_guard = ChildGuard::new("agent-mesh-hub", hub_child);

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let ws_url = loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for hub banner"
        );
        line.clear();
        let n = reader.read_line(&mut line).unwrap();
        assert!(n > 0, "hub exited prematurely");
        if line.contains("[HUB] OxideRelay Hub running at ") {
            if let Some(pos) = line.find("ws://") {
                break line[pos..].trim().to_string();
            }
        }
    };
    let port_str = ws_url
        .trim_start_matches("ws://127.0.0.1:")
        .trim_end_matches("/ws");
    let port: u16 = port_str.parse().unwrap();
    let http_base = format!("http://127.0.0.1:{port}");
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });

    wait_for_grid_status(&http_base, "online").await;

    let c_ctrl = AgentMeshClient::new(&ws_url, "ctrl-agent", "rust");
    c_ctrl.connect().await.expect("connect ctrl-agent");
    wait_for_worker_ready(&c_ctrl).await;

    // Submit one slow task asynchronously so we can hold an active grid_result(wait=true) RPC.
    let async_resp = c_ctrl
        .submit_grid(
            serde_json::json!({ "script": "sleep 20", "wait": false }),
            Some(5_000),
        )
        .await
        .expect("async sleep submit");
    let sleep_tid = match async_resp {
        AgentMeshEnvelope::CommandResponse {
            payload: Some(p), ..
        } => p["task_id"].as_str().unwrap().to_string(),
        other => panic!("unexpected async submit response: {other:?}"),
    };

    let c_wait_result = AgentMeshClient::new(&ws_url, "wait-result-agent", "rust");
    c_wait_result
        .connect()
        .await
        .expect("connect wait-result-agent");
    let c_wait_submit = AgentMeshClient::new(&ws_url, "wait-submit-agent", "rust");
    c_wait_submit
        .connect()
        .await
        .expect("connect wait-submit-agent");

    let h_res = tokio::spawn(async move {
        let _ = c_wait_result
            .get_grid_result(&sleep_tid, true, Some(25_000))
            .await;
    });
    let h_sub = tokio::spawn(async move {
        let _ = c_wait_submit
            .submit_grid(
                serde_json::json!({ "script": "sleep 20", "wait": true }),
                Some(25_000),
            )
            .await;
    });

    // Give both RPCs time to reach in-flight wait on the Master before sending SIGTERM.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let kill_status = Command::new("kill")
        .args(["-TERM", &hub_pid.to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(kill_status.success());

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        let child_ref = hub_guard.child.as_mut().expect("hub child present");
        match child_ref.try_wait().expect("try_wait hub") {
            Some(st) => {
                hub_guard.child.take();
                break st;
            }
            None => {
                if Instant::now() >= exit_deadline {
                    hub_guard.stop_now();
                    panic!("agent-mesh hub did not exit within 5s on SIGTERM while RPCs were in-flight");
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    };
    assert_eq!(
        status.code(),
        Some(0),
        "expected exit 0 on SIGTERM with in-flight RPCs, got {status}"
    );

    h_res.abort();
    h_sub.abort();
    c_ctrl.close().await;
    worker_guard.stop_now();
    master_guard.stop_now();
}

#[tokio::test]
async fn test_mesh3_oxide_grid_connect_env_master_restart_auth_and_edge_cases() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let env = IsolatedEnv::new();
    let oxideswarm_bin = locate_or_build_oxideswarm_bin();
    let port_file = env._root.path().join("master.port");

    let spawn_master_on = |listen_addr: &str, pf: &std::path::Path| {
        let mut cmd = Command::new(&oxideswarm_bin);
        env.apply(&mut cmd);
        cmd.args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "master",
            "--listen",
            listen_addr,
            "--port-file",
            pf.to_str().unwrap(),
            "--discovery-port",
            "0",
            "--max-host-cpu-pct",
            "100",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn master");
        spawn_drain_thread(child.stdout.take().unwrap());
        spawn_drain_thread(child.stderr.take().unwrap());
        ChildGuard::new("oxideswarm-master", child)
    };

    let mut master_guard_1 = spawn_master_on("127.0.0.1:0", &port_file);
    let master_port = wait_for_port_file(&port_file);
    let master_addr = format!("127.0.0.1:{master_port}");

    // 1. Auth fail-fast/fail-closed checks:
    //    (a) Loopback --grid-connect WITHOUT secret against authenticated Master fails fast.
    {
        let mut hub_cmd = Command::new(env!("CARGO_BIN_EXE_agent-mesh"));
        env.apply(&mut hub_cmd);
        let out = hub_cmd
            .args([
                "hub",
                "--listen",
                "127.0.0.1:0",
                "--grid-connect",
                &master_addr,
            ])
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "loopback without secret must fail against authenticated Master"
        );
    }
    //    (b) Empty OXIDESWARM_SECRET="" and OXIDESWARM_SECRET_FILE="" fail closed.
    for (var, val) in [("OXIDESWARM_SECRET", ""), ("OXIDESWARM_SECRET_FILE", "")] {
        let mut hub_cmd = Command::new(env!("CARGO_BIN_EXE_agent-mesh"));
        env.apply(&mut hub_cmd);
        let out = hub_cmd
            .env(var, val)
            .args([
                "hub",
                "--listen",
                "127.0.0.1:0",
                "--grid-connect",
                &master_addr,
            ])
            .output()
            .unwrap();
        assert!(!out.status.success(), "empty {var} must fail closed");
    }

    // 2. Spawn worker + hub using OXIDE_GRID_CONNECT env var (no --grid-connect CLI flag).
    let mut worker_cmd = Command::new(&oxideswarm_bin);
    env.apply(&mut worker_cmd);
    worker_cmd
        .args([
            "--secret-file",
            env.secret_path.to_str().unwrap(),
            "worker",
            "--master",
            &master_addr,
            "--name",
            "env-worker",
            "--no-gpu",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut worker_child = worker_cmd.spawn().expect("spawn worker");
    spawn_drain_thread(worker_child.stdout.take().unwrap());
    spawn_drain_thread(worker_child.stderr.take().unwrap());
    let mut worker_guard = ChildGuard::new("oxideswarm-worker", worker_child);

    let mut hub_cmd = Command::new(env!("CARGO_BIN_EXE_agent-mesh"));
    env.apply(&mut hub_cmd);
    hub_cmd
        .env("OXIDE_GRID_CONNECT", &master_addr)
        .args([
            "hub",
            "--listen",
            "127.0.0.1:0",
            "--secret-file",
            env.secret_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut hub_child = hub_cmd.spawn().expect("spawn hub via OXIDE_GRID_CONNECT");
    let stdout = hub_child.stdout.take().unwrap();
    spawn_drain_thread(hub_child.stderr.take().unwrap());
    let _hub_guard = ChildGuard::new("agent-mesh-hub", hub_child);

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let ws_url = loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for hub banner"
        );
        line.clear();
        let n = reader.read_line(&mut line).unwrap();
        assert!(n > 0, "hub exited prematurely");
        if line.contains("[HUB] OxideRelay Hub running at ") {
            if let Some(pos) = line.find("ws://") {
                break line[pos..].trim().to_string();
            }
        }
    };
    let port: u16 = ws_url
        .trim_start_matches("ws://127.0.0.1:")
        .trim_end_matches("/ws")
        .parse()
        .unwrap();
    let http_base = format!("http://127.0.0.1:{port}");
    std::thread::spawn(move || {
        let mut sink = String::new();
        while reader.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });

    wait_for_grid_status(&http_base, "online").await;
    let client = AgentMeshClient::new(&ws_url, "edge-agent", "rust");
    client.connect().await.expect("connect edge-agent");
    wait_for_worker_ready(&client).await;

    // 3. Edge cases:
    //    (a) Inline `files` object-map + missing expected output -> failed task result
    let missing_out_resp = client
        .submit_grid(
            serde_json::json!({
                "script": "./run.sh",
                "files": {
                    "a/b.txt": "map-hello",
                    "run.sh": { "text": "#!/bin/sh\ncat a/b.txt\n", "executable": true }
                },
                "outputs": ["missing.txt"],
                "wait": true
            }),
            Some(15_000),
        )
        .await
        .expect("missing output submit");
    match missing_out_resp {
        AgentMeshEnvelope::CommandResponse {
            status, exit_code, ..
        } => {
            assert_eq!(status, "failed");
            assert_ne!(exit_code, 0);
        }
        other => panic!("expected failed CommandResponse, got {other:?}"),
    }

    //    (b) Conflicting placement tags -> failed upfront
    let conflict_resp = client
        .submit_grid(
            serde_json::json!({
                "script": "echo hi",
                "tags": ["require:os=linux"],
                "os": "macos",
                "wait": false
            }),
            Some(5_000),
        )
        .await
        .expect("conflict submit");
    match conflict_resp {
        AgentMeshEnvelope::CommandResponse {
            status, exit_code, ..
        } => {
            assert_eq!(status, "failed");
            assert_ne!(exit_code, 0);
        }
        other => panic!("expected failed CommandResponse, got {other:?}"),
    }

    //    (c) Pending grid_status(task_id) and grid_result(wait=false)
    let pending_sub = client
        .submit_grid(
            serde_json::json!({
                "script": "echo pending-check",
                "placement": { "require_tags": ["nonexistent-pending-tag"] },
                "wait": false
            }),
            Some(5_000),
        )
        .await
        .unwrap();
    let pending_tid = match pending_sub {
        AgentMeshEnvelope::CommandResponse {
            payload: Some(p), ..
        } => p["task_id"].as_str().unwrap().to_string(),
        other => panic!("expected pending submit payload, got {other:?}"),
    };
    let st_resp = client
        .submit_grid_status(Some(&pending_tid), Some(5_000))
        .await
        .unwrap();
    match st_resp {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(p["task"]["task_id"], pending_tid);
        }
        other => panic!("expected pending grid_status, got {other:?}"),
    }
    let res_nowait = client
        .get_grid_result(&pending_tid, false, Some(5_000))
        .await
        .unwrap();
    match res_nowait {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(p["completed"], serde_json::json!(false));
        }
        other => panic!("expected pending grid_result, got {other:?}"),
    }

    //    (d) grid_cancel on unknown UUID -> cancelled: false
    let unknown_uuid = uuid::Uuid::new_v4().to_string();
    let cancel_unk = client
        .cancel_grid_task(&unknown_uuid, Some(5_000))
        .await
        .unwrap();
    match cancel_unk {
        AgentMeshEnvelope::CommandResponse {
            status,
            payload: Some(p),
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(p["cancelled"], serde_json::json!(false));
        }
        other => panic!("expected unknown cancel response, got {other:?}"),
    }

    // 4. Master stop -> "offline", grid_status(task_id) reports "Failed to retrieve task status: ...",
    //    then restart Master on SAME port -> /api/nodes "grid" recovers to "online"!
    worker_guard.stop_now();
    master_guard_1.stop_now();
    wait_for_grid_status(&http_base, "offline").await;

    let err_status = client
        .submit_grid_status(Some(&pending_tid), Some(5_000))
        .await
        .unwrap();
    match err_status {
        AgentMeshEnvelope::CommandResponse { status, stderr, .. } => {
            assert_eq!(status, "failed");
            assert!(
                stderr.starts_with("Failed to retrieve task status:"),
                "unreachable Master grid_status must not say Task not found, got: {stderr}"
            );
        }
        other => panic!("expected failed CommandResponse, got {other:?}"),
    }

    let port_file_2 = env._root.path().join("master2.port");
    let mut master_guard_2 = spawn_master_on(&master_addr, &port_file_2);
    let recovered_port = wait_for_port_file(&port_file_2);
    assert_eq!(recovered_port, master_port);
    wait_for_grid_status(&http_base, "online").await;

    client.close().await;
    master_guard_2.stop_now();
}
