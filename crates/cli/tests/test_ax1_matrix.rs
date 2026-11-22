//! End-to-end tests for `AX-1` matrix fan-out in `oxideswarm submit --matrix` and
//! `oxideswarm mcp` `submit` (`docs/design/0101-matrix-fanout.md`).
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_core::capabilities::MobileCapabilities;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{
    MasterHandle, MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE,
};
use rusty_grid_master::ReaperConfig;
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::MobileTelemetryHandle;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

struct Cluster {
    _master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
    home: TempDir,
    fake_bin: TempDir,
}

impl Cluster {
    async fn start_with_tagged_workers(specs: &[(&str, &[&str])]) -> Self {
        let store = tempfile::tempdir().unwrap();
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_discovery_port(0)
            .with_blob_store(store.path(), 64 * 1024 * 1024);
        let sched_cfg = SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure();
        let master = MasterServer::spawn_with_config(config, sched_cfg, ReaperConfig::default())
            .await
            .expect("master starts");
        let addr = master.server_addr().to_string();
        let fake_bin = setup_fake_pmset();
        let mut cluster = Self {
            _master: master,
            addr,
            workers: Vec::new(),
            _dirs: vec![store],
            home: tempfile::tempdir().unwrap(),
            fake_bin,
        };
        for (name, tags) in specs {
            cluster.add_worker(name, tags);
        }
        cluster.wait_for_workers(specs.len()).await;
        cluster
    }

    fn add_worker(&mut self, name: &str, tags: &[&str]) {
        let sandbox = tempfile::tempdir().unwrap();
        let ws_root = sandbox.path().join("workspaces");
        let mut config = WorkerConfig::new(self.addr.clone())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(2)
            .with_ram_mb(4096)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_sandbox_base_dir(sandbox.path().join("sandboxes"))
            .with_workspaces(true)
            .with_workspace_root(Some(ws_root))
            .with_mobile_telemetry(MobileTelemetryHandle::new(Some(MobileCapabilities {
                battery_pct: Some(100),
                is_charging: Some(true),
                ..MobileCapabilities::default()
            })))
            .with_redirection_portal(false);
        config.tags = tags.iter().map(|s| (*s).to_string()).collect();
        self._dirs.push(sandbox);
        let worker_name = name.to_string();
        let mut client = WorkerClient::new(config);
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            if let Err(e) = client.run(rx).await {
                eprintln!("worker {worker_name} failed: {e}");
            }
        });
        self.workers.push((tx, handle));
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let workers = self._master.list_workers().await.unwrap_or_default();
            if workers
                .iter()
                .filter(|w| w.status != WorkerStatus::Disconnected)
                .count()
                >= count
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("{count} worker(s) did not register");
    }

    fn cli(&self) -> Command {
        isolated(Command::new(BIN), self.home.path(), self.fake_bin.path())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

fn setup_fake_pmset() -> TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let pmset = dir.path().join("pmset");
    std::fs::write(&pmset, "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\n").unwrap();
    let mut perms = std::fs::metadata(&pmset).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&pmset, perms).unwrap();
    dir
}

fn isolated(mut cmd: Command, home: &Path, fake_bin: &Path) -> Command {
    let orig_path = std::env::var("PATH").unwrap_or_default();
    let path = format!("{}:{orig_path}", fake_bin.display());
    cmd.current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env("PATH", path)
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_INSECURE")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

fn json_of(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON on stdout ({e}):\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn describe(out: &Output) -> String {
    format!(
        "status {:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

struct Mcp {
    child: std::process::Child,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl Mcp {
    fn start(cluster: &Cluster) -> Self {
        let mut child = isolated(
            Command::new(BIN),
            cluster.home.path(),
            cluster.fake_bin.path(),
        )
        .args(["mcp", "--master", &cluster.addr])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mcp");
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut mcp = Self {
            child,
            stdout,
            next_id: 1,
        };
        let init = mcp.call(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "ax1-e2e", "version": "1.0"}
            }),
        );
        assert_eq!(init["result"]["protocolVersion"], "2024-11-05");
        mcp.notify("notifications/initialized", json!({}));
        mcp
    }

    fn notify(&mut self, method: &str, params: Value) {
        let line = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
        loop {
            let mut buf = String::new();
            assert!(self.stdout.read_line(&mut buf).unwrap() > 0, "mcp closed");
            let trimmed = buf.trim();
            assert!(!trimmed.contains('\n'));
            let value: Value = serde_json::from_str(trimmed)
                .unwrap_or_else(|e| panic!("invalid JSON-RPC line '{trimmed}': {e}"));
            assert_eq!(value["jsonrpc"], "2.0", "line missing jsonrpc 2.0: {value}");
            if value.get("id").and_then(Value::as_u64) == Some(id) {
                return value;
            }
        }
    }

    fn submit(&mut self, arguments: Value) -> (Value, bool) {
        let resp = self.call(
            "tools/call",
            json!({"name": "submit", "arguments": arguments}),
        );
        assert!(resp.get("error").is_none(), "unexpected RPC error: {resp}");
        let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .expect("content text");
        let body: Value = serde_json::from_str(text).unwrap_or_else(|e| {
            panic!("tool body is not JSON ({e}): {text}");
        });
        (body, is_error)
    }

    fn ping(&mut self) {
        let resp = self.call("ping", json!({}));
        assert!(resp.get("error").is_none(), "ping failed: {resp}");
        assert_eq!(resp["result"], json!({}));
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_matrix_1d_three_tagged_workers_and_human_summary() {
    let cluster = Cluster::start_with_tagged_workers(&[
        ("w-alpha", &["cell-a"]),
        ("w-beta", &["cell-b"]),
        ("w-gamma", &["cell-c"]),
    ])
    .await;

    let out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--matrix",
            "tag=cell-a,cell-b,cell-c",
            "--json",
            "--command",
            "sh",
            "--",
            "-c",
            "printf \"%s\" \"$OXIDESWARM_MATRIX_CELL\"",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", describe(&out));
    let v = json_of(&out);
    assert_eq!(v["matrix"], true);
    assert_eq!(v["status"], "completed");
    assert_eq!(v["total_cells"], 3);
    assert_eq!(v["completed_cells"], 3);
    assert_eq!(v["failed_cells"], 0);
    assert_eq!(v["unplaced_cells"], 0);
    assert_eq!(v["machines"], 3);

    let cells = v["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 3);
    assert_eq!(cells[0]["worker"], "w-alpha");
    assert_eq!(cells[0]["stdout"], "tag=cell-a");
    assert_eq!(cells[1]["worker"], "w-beta");
    assert_eq!(cells[1]["stdout"], "tag=cell-b");
    assert_eq!(cells[2]["worker"], "w-gamma");
    assert_eq!(cells[2]["stdout"], "tag=cell-c");

    // Human output without --json
    let plain = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--matrix",
            "tag=cell-a,cell-b,cell-c",
            "--command",
            "echo",
            "--",
            "hi",
        ])
        .output()
        .unwrap();
    assert!(plain.status.success(), "{}", describe(&plain));
    let stdout = String::from_utf8_lossy(&plain.stdout);
    assert!(stdout.contains("Matrix: 3 completed, 0 failed, 0 unplaced (3 total)"));
    assert!(stdout.contains("Tasks: 3 finished, 0 failed, on 3 machines"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_matrix_2d_cartesian_tags_and_env_axes() {
    let cluster = Cluster::start_with_tagged_workers(&[
        ("w-11", &["row-1", "col-1"]),
        ("w-12", &["row-1", "col-2"]),
        ("w-21", &["row-2", "col-1"]),
        ("w-22", &["row-2", "col-2"]),
    ])
    .await;

    let out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--matrix",
            "tag=row-1,row-2",
            "--matrix",
            "tag=col-1,col-2",
            "--json",
            "--command",
            "sh",
            "--",
            "-c",
            "printf \"%s\" \"$OXIDESWARM_MATRIX_CELL\"",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", describe(&out));
    let v = json_of(&out);
    assert_eq!(v["total_cells"], 4);
    assert_eq!(v["completed_cells"], 4);
    assert_eq!(v["machines"], 4);
    let cells = v["cells"].as_array().unwrap();
    let workers: Vec<&str> = cells
        .iter()
        .map(|c| c["worker"].as_str().unwrap())
        .collect();
    assert_eq!(workers, ["w-11", "w-12", "w-21", "w-22"]);

    // 2D tag x env:<VAR> Cartesian sweep
    let env_out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--matrix",
            "tag=row-1,row-2",
            "--matrix",
            "env:MODE=fast,slow",
            "--json",
            "--command",
            "sh",
            "--",
            "-c",
            "printf \"%s|%s\" \"$MODE\" \"$OXIDESWARM_MATRIX_INDEX\"",
        ])
        .output()
        .unwrap();
    assert!(env_out.status.success(), "{}", describe(&env_out));
    let ev = json_of(&env_out);
    assert_eq!(ev["total_cells"], 4);
    assert_eq!(ev["completed_cells"], 4);
    let stdouts: Vec<&str> = ev["cells"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["stdout"].as_str().unwrap())
        .collect();
    assert_eq!(stdouts, ["fast|0", "slow|1", "fast|2", "slow|3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_matrix_unplaced_cell_fails_promptly_and_skips_upload_when_all_unplaced() {
    let cluster = Cluster::start_with_tagged_workers(&[("w-a", &["cell-a"])]).await;

    let started = Instant::now();
    let out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--timeout-secs",
            "60",
            "--matrix",
            "tag=cell-a,cell-missing",
            "--json",
            "--command",
            "echo",
            "--",
            "ok",
        ])
        .output()
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "unplaced matrix cell should fail promptly via ListWorkers without waiting timeout_secs"
    );
    assert!(!out.status.success(), "{}", describe(&out));
    let v = json_of(&out);
    assert_eq!(v["status"], "failed");
    assert_eq!(v["total_cells"], 2);
    assert_eq!(v["completed_cells"], 1);
    assert_eq!(v["unplaced_cells"], 1);
    assert_eq!(v["cells"][0]["status"], "completed");
    assert_eq!(v["cells"][0]["unplaced"], false);
    assert_eq!(v["cells"][1]["status"], "unplaced");
    assert_eq!(v["cells"][1]["unplaced"], true);

    // When 0 cells are placeable, input upload is skipped completely.
    let input_path = cluster.home.path().join("payload.txt");
    std::fs::write(&input_path, b"never-upload-me").unwrap();
    let all_unplaced = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--timeout-secs",
            "60",
            "--input",
            "payload.txt",
            "--matrix",
            "tag=missing-1,missing-2",
            "--json",
            "--command",
            "cat",
            "--",
            "payload.txt",
        ])
        .output()
        .unwrap();
    assert!(
        !all_unplaced.status.success(),
        "{}",
        describe(&all_unplaced)
    );
    let av = json_of(&all_unplaced);
    assert_eq!(av["unplaced_cells"], 2);
    assert!(av.get("input_upload").is_none(), "{av}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_matrix_input_dedup_output_dir_isolation_and_workspace_sync() {
    let cluster = Cluster::start_with_tagged_workers(&[
        ("w-a", &["cell-a"]),
        ("w-b", &["cell-b"]),
        ("w-c", &["cell-c"]),
    ])
    .await;

    std::fs::write(cluster.home.path().join("shared.txt"), b"shared-payload").unwrap();
    let out_dir = cluster.home.path().join("matrix_out");

    let out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--input",
            "shared.txt",
            "--output",
            "result.txt",
            "--output-dir",
            out_dir.to_str().unwrap(),
            "--matrix",
            "tag=cell-a,cell-b,cell-c",
            "--json",
            "--type",
            "shell",
            "--command",
            "printf \"%s:%s\" \"$(cat shared.txt)\" \"$OXIDESWARM_MATRIX_CELL\" > result.txt",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", describe(&out));
    let v = json_of(&out);
    assert_eq!(v["completed_cells"], 3);
    assert_eq!(v["input_upload"]["blobs"], 1);
    assert_eq!(v["input_upload"]["uploaded"], 1);

    for (idx, tag) in ["cell-a", "cell-b", "cell-c"].iter().enumerate() {
        let cell = &v["cells"][idx];
        let saved = cell["saved_outputs"][0].as_str().unwrap();
        let content = std::fs::read_to_string(saved).unwrap();
        assert_eq!(content, format!("shared-payload:tag={tag}"));
    }

    // Workspace sync + matrix fan-out
    let ws_dir = cluster.home.path().join("ws_src");
    std::fs::create_dir_all(&ws_dir).unwrap();
    std::fs::write(ws_dir.join("hello.txt"), b"from-workspace").unwrap();
    let ws_out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--workspace",
            "ax1-ws",
            "--sync",
            ws_dir.to_str().unwrap(),
            "--matrix",
            "tag=cell-a,cell-b",
            "--json",
            "--type",
            "shell",
            "--command",
            "printf \"%s:%s\" \"$(cat hello.txt)\" \"$OXIDESWARM_MATRIX_CELL\"",
        ])
        .output()
        .unwrap();
    assert!(ws_out.status.success(), "{}", describe(&ws_out));
    let wv = json_of(&ws_out);
    assert_eq!(wv["completed_cells"], 2);
    assert_eq!(wv["workspace"]["name"], "ax1-ws");
    assert_eq!(wv["cells"][0]["stdout"], "from-workspace:tag=cell-a");
    assert_eq!(wv["cells"][1]["stdout"], "from-workspace:tag=cell-b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_matrix_partial_failure_and_invalid_flag_refusals() {
    let cluster =
        Cluster::start_with_tagged_workers(&[("w-a", &["cell-a"]), ("w-b", &["cell-b"])]).await;

    let partial = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--matrix",
            "tag=cell-a,cell-b",
            "--json",
            "--type",
            "shell",
            "--command",
            "if [ \"$OXIDESWARM_MATRIX_INDEX\" = \"1\" ]; then exit 7; fi; echo ok",
        ])
        .output()
        .unwrap();
    assert!(!partial.status.success(), "{}", describe(&partial));
    let pv = json_of(&partial);
    assert_eq!(pv["status"], "failed");
    assert_eq!(pv["completed_cells"], 1);
    assert_eq!(pv["failed_cells"], 1);
    assert_eq!(pv["cells"][0]["exit_code"], 0);
    assert_eq!(pv["cells"][1]["exit_code"], 7);

    // Offline refusals before connecting
    let run_bad = |extra: &[&str]| {
        cluster
            .cli()
            .args(["submit", "--master", "127.0.0.1:1"])
            .args(extra)
            .args(["--command", "echo", "--", "x"])
            .output()
            .unwrap()
    };
    assert!(!run_bad(&["--matrix", "bad"]).status.success());
    assert!(!run_bad(&["--matrix", "tag="]).status.success());
    assert!(
        !run_bad(&["--matrix", "tag=a,b,c", "--max-matrix-cells", "2"])
            .status
            .success()
    );
    assert!(!run_bad(&["--os", "macos", "--matrix", "os=macos,linux"])
        .status
        .success());
    assert!(
        !run_bad(&["--matrix", "tag=a", "--verify", "1", "--output", "o.txt"])
            .status
            .success()
    );
    assert!(!run_bad(&["--matrix", "tag=a", "-o", "out.glb"])
        .status
        .success());
    assert!(!run_bad(&["--matrix", "env:BAD=KEY=1"]).status.success());
    assert!(!run_bad(&["--matrix", "iso=off"]).status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_and_mcp_matrix_huge_timeout_and_delimiter_validation() {
    let cluster = Cluster::start_with_tagged_workers(&[("w-a", &["cell-a"])]).await;

    // 1. CLI --timeout-secs u64::MAX with --matrix must succeed without panic
    let cli_huge = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--timeout-secs",
            "18446744073709551615",
            "--matrix",
            "tag=cell-a",
            "--json",
            "--command",
            "echo",
            "--",
            "huge-timeout",
        ])
        .output()
        .unwrap();
    assert!(cli_huge.status.success(), "{}", describe(&cli_huge));
    let cv = json_of(&cli_huge);
    assert_eq!(cv["completed_cells"], 1);
    assert_eq!(cv["cells"][0]["stdout"], "huge-timeout\n");

    let mut mcp = Mcp::start(&cluster);

    // 2. MCP matrix with wait_secs = u64::MAX and timeout_secs = u64::MAX must succeed without panic
    let (body_huge, err_huge) = mcp.submit(json!({
        "command": "echo",
        "args": ["mcp-huge"],
        "timeout_secs": 18446744073709551615u64,
        "wait_secs": 18446744073709551615u64,
        "matrix": ["tag=cell-a"],
    }));
    assert!(!err_huge, "{body_huge}");
    assert_eq!(body_huge["completed_cells"], 1);
    assert_eq!(body_huge["cells"][0]["stdout"], "mcp-huge\n");
    mcp.ping();

    // 3. Non-matrix MCP submit with wait_secs = u64::MAX also succeeds without panic
    let (body_single, err_single) = mcp.submit(json!({
        "command": "echo",
        "args": ["single-huge"],
        "wait": true,
        "wait_secs": 18446744073709551615u64,
    }));
    assert!(!err_single, "{body_single}");
    assert_eq!(body_single["status"], "completed");
    mcp.ping();

    // 4. Unified delimiter and iso=off rejections return -32602 and keep server alive
    for bad in [
        json!(["env:BAD=KEY=1"]),
        json!({"env:BAD": ["KEY=1"]}),
        json!(["tag=a=b"]),
        json!(["iso=off"]),
        json!({"iso": ["off"]}),
    ] {
        let resp = mcp.call(
            "tools/call",
            json!({
                "name": "submit",
                "arguments": {"command": "echo", "matrix": bad}
            }),
        );
        assert_eq!(resp["error"]["code"], -32602, "expected -32602 for {resp}");
        mcp.ping();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_matrix_workflows_validation_framing_and_eof_grace() {
    let cluster = Cluster::start_with_tagged_workers(&[
        ("w-11", &["row-1", "col-1", "mcp-a"]),
        ("w-12", &["row-1", "col-2", "mcp-b"]),
        ("w-21", &["row-2", "col-1", "mcp-c"]),
        ("w-22", &["row-2", "col-2"]),
    ])
    .await;

    let mut mcp = Mcp::start(&cluster);

    // 1. tools/list exposes matrix on submit without top-level anyOf/oneOf/allOf
    let list = mcp.call("tools/list", json!({}));
    let tools = list["result"]["tools"].as_array().unwrap();
    let submit_tool = tools.iter().find(|t| t["name"] == "submit").unwrap();
    assert_eq!(
        submit_tool["inputSchema"]["properties"]["matrix"]["type"],
        "array"
    );

    // 2. 1D matrix across 3 tagged workers
    let (body_1d, err_1d) = mcp.submit(json!({
        "type": "shell",
        "script": "printf \"%s\" \"$OXIDESWARM_MATRIX_CELL\"",
        "matrix": ["tag=mcp-a,mcp-b,mcp-c"],
    }));
    assert!(!err_1d, "{body_1d}");
    assert_eq!(body_1d["total_cells"], 3);
    assert_eq!(body_1d["completed_cells"], 3);
    assert_eq!(body_1d["machines"], 3);
    assert_eq!(body_1d["cells"][0]["worker"], "w-11");
    assert_eq!(body_1d["cells"][1]["worker"], "w-12");
    assert_eq!(body_1d["cells"][2]["worker"], "w-21");

    // 3. 2D matrix across 4 tagged workers
    let (body_2d, err_2d) = mcp.submit(json!({
        "type": "shell",
        "script": "printf \"%s\" \"$OXIDESWARM_MATRIX_CELL\"",
        "matrix": ["tag=row-1,row-2", "tag=col-1,col-2"],
    }));
    assert!(!err_2d, "{body_2d}");
    assert_eq!(body_2d["total_cells"], 4);
    assert_eq!(body_2d["completed_cells"], 4);
    assert_eq!(body_2d["machines"], 4);

    // 4. Workspace + matrix
    let ws_dir = cluster.home.path().join("mcp_ws");
    std::fs::create_dir_all(&ws_dir).unwrap();
    std::fs::write(ws_dir.join("data.txt"), b"ws-matrix-data").unwrap();
    let (body_ws, err_ws) = mcp.submit(json!({
        "type": "shell",
        "script": "cat data.txt",
        "workspace": {"name": "mcp-ax1-ws", "sync_dir": "mcp_ws"},
        "matrix": ["tag=mcp-a,mcp-b"],
    }));
    assert!(!err_ws, "{body_ws}");
    assert_eq!(body_ws["completed_cells"], 2);
    assert_eq!(body_ws["workspace"]["name"], "mcp-ax1-ws");

    // 5. Unplaced cell fails promptly without hanging and keeps server alive
    let started = Instant::now();
    let (body_unplaced, err_unplaced) = mcp.submit(json!({
        "command": "echo",
        "args": ["ok"],
        "timeout_secs": 60,
        "matrix": ["tag=mcp-a,mcp-missing"],
    }));
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "MCP unplaced matrix cell hung: {body_unplaced}"
    );
    assert!(err_unplaced, "{body_unplaced}");
    assert_eq!(body_unplaced["unplaced_cells"], 1);
    assert_eq!(body_unplaced["cells"][1]["unplaced"], true);
    mcp.ping();

    // 6. Invalid matrix schemas return -32602 and keep server alive
    let too_many = (0..100)
        .map(|i| format!("x{i}"))
        .collect::<Vec<_>>()
        .join(",");
    for bad in [
        json!([]),
        json!({}),
        json!("not-valid"),
        json!([format!("tag={too_many}")]),
    ] {
        let resp = mcp.call(
            "tools/call",
            json!({
                "name": "submit",
                "arguments": {"command": "echo", "matrix": bad}
            }),
        );
        assert_eq!(resp["error"]["code"], -32602, "expected -32602 for {resp}");
        mcp.ping();
    }

    // 7. Partial failure returns isError: true and preserves exit codes
    let (body_partial, err_partial) = mcp.submit(json!({
        "type": "shell",
        "script": "if [ \"$OXIDESWARM_MATRIX_INDEX\" = \"1\" ]; then exit 9; fi; echo ok",
        "matrix": ["tag=mcp-a,mcp-b"],
    }));
    assert!(err_partial, "{body_partial}");
    assert_eq!(body_partial["cells"][0]["exit_code"], 0);
    assert_eq!(body_partial["cells"][1]["exit_code"], 9);
    mcp.ping();

    // 8. Large multi-line JSON-like stdout preserves single-line JSON-RPC 2.0 framing
    let (body_large, err_large) = mcp.submit(json!({
        "type": "shell",
        "script": "i=0; while [ $i -lt 600 ]; do printf '{\"k\":\"v\\n\"}\\n'; i=$((i+1)); done",
        "matrix": ["tag=mcp-a,mcp-b"],
    }));
    assert!(!err_large, "{body_large}");
    assert_eq!(body_large["cells"].as_array().unwrap().len(), 2);
    mcp.ping();
    drop(mcp);

    // 9. Stdin EOF grace window finishes in-flight matrix submit cleanly
    let mut eof_child = isolated(
        Command::new(BIN),
        cluster.home.path(),
        cluster.fake_bin.path(),
    )
    .args(["mcp", "--master", &cluster.addr])
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .expect("spawn eof mcp");
    {
        let mut stdin = eof_child.stdin.take().unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"eof","version":"1"}}})
        )
        .unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}})
        )
        .unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0","id":99,"method":"tools/call","params":{"name":"submit","arguments":{"command":"echo","args":["eof-ok"],"wait":true,"matrix":["tag=mcp-a,mcp-b"]}}})
        )
        .unwrap();
        stdin.flush().unwrap();
        // Close stdin immediately to exercise the 4s EOF grace window.
    }
    let eof_out = eof_child.wait_with_output().unwrap();
    assert!(eof_out.status.success(), "{}", describe(&eof_out));
    let out_str = String::from_utf8_lossy(&eof_out.stdout);
    let reply_99: Value = out_str
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|v| v.get("id").and_then(Value::as_u64) == Some(99))
        .unwrap_or_else(|| panic!("missing id 99 reply on EOF:\n{out_str}"));
    assert_eq!(reply_99["result"]["isError"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_matrix_symlink_cell_dir_fails_closed() {
    let cluster = Cluster::start_with_tagged_workers(&[("w-a", &["cell-a"])]).await;

    let out_dir = cluster.home.path().join("cli_symlink_out");
    let outside_dir = cluster.home.path().join("outside_target");
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::create_dir_all(&outside_dir).unwrap();
    std::os::unix::fs::symlink(&outside_dir, out_dir.join("cell-00-cell-a")).unwrap();

    let out = cluster
        .cli()
        .args([
            "submit",
            "--master",
            &cluster.addr,
            "--matrix",
            "tag=cell-a",
            "--type",
            "shell",
            "--output",
            "secret.txt",
            "--output-dir",
            out_dir.to_str().unwrap(),
            "--json",
            "--command",
            "echo LEAKED_VIA_CLI_SYMLINK > secret.txt",
        ])
        .output()
        .unwrap();

    assert!(
        !out.status.success(),
        "CLI matrix output into symlinked cell dir must exit non-zero:\n{}",
        describe(&out)
    );
    let v = json_of(&out);
    assert_eq!(v["status"], "failed");
    assert_eq!(v["completed_cells"], 0);
    assert_eq!(v["error_cells"], 1);
    assert_eq!(v["cells"][0]["status"], "error");
    let err = v["cells"][0]["error"].as_str().unwrap_or("");
    assert!(
        err.contains("symbolic link"),
        "expected symbolic link refusal in cell error, got: {v}"
    );
    assert_eq!(
        std::fs::read_dir(&outside_dir).unwrap().count(),
        0,
        "symlink target directory must remain completely empty"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_task_result_and_doctor_huge_timeout_do_not_panic() {
    let cluster = Cluster::start_with_tagged_workers(&[("w-a", &["cell-a"])]).await;
    let mut mcp = Mcp::start(&cluster);

    // 1. Submit matrix cell then poll task_result(wait=true, wait_secs=u64::MAX).
    let (submit_body, submit_err) = mcp.submit(json!({
        "command": "echo",
        "args": ["task-result-huge"],
        "matrix": ["tag=cell-a"],
    }));
    assert!(!submit_err, "{submit_body}");
    let task_id = submit_body["cells"][0]["task_id"]
        .as_str()
        .expect("matrix cell task_id")
        .to_string();

    let tr_resp = mcp.call(
        "tools/call",
        json!({
            "name": "task_result",
            "arguments": {
                "task_id": task_id,
                "wait": true,
                "wait_secs": u64::MAX,
            }
        }),
    );
    assert!(
        tr_resp.get("error").is_none(),
        "unexpected task_result RPC error: {tr_resp}"
    );
    assert_eq!(tr_resp["result"]["isError"], false, "{tr_resp}");
    let tr_text = tr_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("task_result text");
    let tr_body: Value = serde_json::from_str(tr_text).unwrap();
    assert_eq!(tr_body["status"], "completed");
    assert_eq!(tr_body["stdout"], "task-result-huge\n");
    mcp.ping();

    // 2. Call MCP doctor with timeout_secs=u64::MAX (exercises doctor::collect_with deadline).
    let doc_resp = mcp.call(
        "tools/call",
        json!({
            "name": "doctor",
            "arguments": {
                "timeout_secs": u64::MAX,
                "skip_gpu": true,
                "skip_spread": true,
                "skip_throughput": true,
            }
        }),
    );
    assert!(
        doc_resp.get("error").is_none(),
        "unexpected doctor RPC error: {doc_resp}"
    );
    assert_eq!(doc_resp["result"]["isError"], false, "{doc_resp}");
    let doc_text = doc_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("doctor text");
    let doc_body: Value = serde_json::from_str(doc_text).unwrap();
    assert!(
        !doc_body["checks"].as_array().unwrap().is_empty(),
        "doctor checks should not be empty: {doc_body}"
    );
    mcp.ping();
}
