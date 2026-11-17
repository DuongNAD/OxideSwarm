//! End-to-end tests of `submit --verify <N>` and the MCP `submit` option `verify`
//! (`docs/design/0089-anima-engine-first-workload.md` D3, VERIFY-1).
//!
//! A master with a blob store and several workers run in this process on 127.0.0.1:0; `submit`
//! and `mcp` are the real `oxideswarm` binary with a private `HOME`. The tasks are `sh` scripts,
//! so the file is Unix only. A "plugged in" battery reading keeps a laptop from refusing work.
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
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::MobileTelemetryHandle;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

struct Cluster {
    master: MasterHandle,
    addr: String,
    workers: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
    home: TempDir,
}

impl Cluster {
    async fn start(workers: usize) -> Self {
        let store = tempfile::tempdir().unwrap();
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_blob_store(store.path(), 64 * 1024 * 1024);
        let master = MasterServer::spawn(config).await.expect("master starts");
        let addr = master.server_addr().to_string();
        let mut cluster = Self {
            master,
            addr,
            workers: Vec::new(),
            _dirs: vec![store],
            home: tempfile::tempdir().unwrap(),
        };
        for i in 1..=workers {
            cluster.add_worker(&format!("verify-w{i}"));
        }
        cluster.wait_for_workers(workers).await;
        cluster
    }

    fn add_worker(&mut self, name: &str) {
        let sandbox = tempfile::tempdir().unwrap();
        let config = WorkerConfig::new(self.addr.clone())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(2)
            .with_ram_mb(4096)
            .with_sandbox_base_dir(sandbox.path())
            .with_mobile_telemetry(MobileTelemetryHandle::new(Some(MobileCapabilities {
                battery_pct: Some(100),
                is_charging: Some(true),
                ..MobileCapabilities::default()
            })))
            .with_redirection_portal(false);
        self._dirs.push(sandbox);
        let mut client = WorkerClient::new(config);
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((tx, handle));
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let workers = self.master.list_workers().await.unwrap_or_default();
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
        isolated(Command::new(BIN), self.home.path())
    }

    /// Runs `oxideswarm submit --master <addr> --json --timeout-secs 60 <extra> --command sh -- -c <script>`.
    fn submit(&self, extra: &[&str], script: &str) -> Output {
        self.cli()
            .args([
                "submit",
                "--master",
                &self.addr,
                "--json",
                "--timeout-secs",
                "60",
            ])
            .args(extra)
            .args(["--command", "sh", "--", "-c", script])
            .output()
            .expect("run submit")
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

fn isolated(mut cmd: Command, home: &Path) -> Command {
    cmd.current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("OXIDESWARM_HOME")
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
            "no JSON on stdout ({e}):\nstdout: {}\nstderr: {}",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_identical_outputs_on_two_workers_is_verified() {
    let cluster = Cluster::start(2).await;
    let out = cluster.submit(
        &["--verify", "1", "--output", "out.txt"],
        "printf hello > out.txt",
    );
    assert!(out.status.success(), "{}", describe(&out));
    let value = json_of(&out);
    assert_eq!(value["verify"]["status"], "verified", "{value}");
    assert_eq!(value["verify"]["workers"].as_array().unwrap().len(), 2);
    assert_eq!(value["verify"]["differing"].as_array().unwrap().len(), 0);

    // Without --json the verdict is one plain line.
    let plain = cluster
        .cli()
        .args(["submit", "--master", &cluster.addr, "--timeout-secs", "60"])
        .args(["--verify", "1", "--output", "out.txt"])
        .args(["--command", "sh", "--", "-c", "printf hello > out.txt"])
        .output()
        .unwrap();
    assert!(plain.status.success(), "{}", describe(&plain));
    assert!(
        String::from_utf8_lossy(&plain.stdout)
            .contains("verified: 1 file(s) identical on 2 workers"),
        "{}",
        describe(&plain)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_reports_a_mismatch_and_names_the_file() {
    let cluster = Cluster::start(2).await;
    // The sandbox path differs for every run, so the content cannot match.
    let out = cluster.submit(
        &[
            "--verify",
            "1",
            "--output",
            "same.txt",
            "--output",
            "where.txt",
        ],
        "printf same > same.txt; pwd > where.txt",
    );
    assert_eq!(out.status.code(), Some(4), "{}", describe(&out));
    let value = json_of(&out);
    assert_eq!(value["verify"]["status"], "mismatch", "{value}");
    let differing = value["verify"]["differing"].as_array().unwrap();
    assert_eq!(differing.len(), 1, "{value}");
    assert_eq!(differing[0]["path"], "where.txt");
    assert_eq!(differing[0]["workers"].as_array().unwrap().len(), 2);

    let plain = cluster
        .cli()
        .args(["submit", "--master", &cluster.addr, "--timeout-secs", "60"])
        .args(["--verify", "1", "--output", "where.txt"])
        .args(["--command", "sh", "--", "-c", "pwd > where.txt"])
        .output()
        .unwrap();
    assert_eq!(plain.status.code(), Some(4), "{}", describe(&plain));
    let text = String::from_utf8_lossy(&plain.stdout);
    assert!(
        text.contains("mismatch") && text.contains("where.txt"),
        "{text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_without_enough_other_workers_is_a_clear_error() {
    let cluster = Cluster::start(2).await;
    let out = cluster.submit(
        &["--verify", "2", "--output", "out.txt"],
        "printf hello > out.txt",
    );
    assert_eq!(out.status.code(), Some(1), "{}", describe(&out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("needs 2 other workers"), "{stderr}");
    assert_eq!(json_of(&out)["verify"]["status"], "error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_verify_file_compares_only_the_checksum_file() {
    let cluster = Cluster::start(2).await;
    // data.txt differs per worker, checksums.sha256 does not: --verify-file looks only at the latter,
    // and fetches it even though no --output names it.
    let script = "pwd > data.txt; printf 'abc  fixed\\n' > checksums.sha256";
    let out = cluster.submit(
        &["--verify", "1", "--verify-file", "checksums.sha256"],
        script,
    );
    assert!(out.status.success(), "{}", describe(&out));
    let value = json_of(&out);
    assert_eq!(value["verify"]["status"], "verified", "{value}");
    assert_eq!(value["verify"]["compared"], json!(["checksums.sha256"]));

    // A checksum file that differs is a mismatch naming that file.
    let out = cluster.submit(
        &["--verify", "1", "--verify-file", "checksums.sha256"],
        "pwd > checksums.sha256",
    );
    assert_eq!(out.status.code(), Some(4), "{}", describe(&out));
    assert_eq!(
        json_of(&out)["verify"]["differing"][0]["path"],
        "checksums.sha256"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_verify_options_are_refused_before_connecting() {
    let home = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        isolated(Command::new(BIN), home.path())
            // Nothing listens here: a refusal must not need a master.
            .args(["submit", "--master", "127.0.0.1:1"])
            .args(args)
            .args(["--command", "true"])
            .output()
            .unwrap()
    };
    let no_outputs = run(&["--verify", "1"]);
    assert_eq!(no_outputs.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&no_outputs.stderr).contains("compares declared outputs"));

    let zero = run(&["--verify", "0", "--output", "a"]);
    assert_eq!(zero.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&zero.stderr).contains("at least 1"));

    let pinned = run(&["--verify", "1", "--output", "a", "--worker", "w"]);
    assert_eq!(pinned.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&pinned.stderr).contains("--worker"));

    let orphan = run(&["--verify-file", "sums"]);
    assert_ne!(
        orphan.status.code(),
        Some(0),
        "--verify-file needs --verify"
    );
}

struct Mcp {
    child: std::process::Child,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl Mcp {
    fn start(cluster: &Cluster) -> Self {
        let mut child = isolated(Command::new(BIN), cluster.home.path())
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
        mcp.call(
            "initialize",
            json!({"protocolVersion": "2024-11-05", "capabilities": {},
                   "clientInfo": {"name": "verify-e2e", "version": "1"}}),
        );
        mcp
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
            let value: Value = serde_json::from_str(buf.trim()).unwrap();
            if value["id"] == id {
                return value;
            }
        }
    }

    fn submit(&mut self, arguments: Value) -> (bool, Value) {
        let reply = self.call(
            "tools/call",
            json!({"name": "submit", "arguments": arguments}),
        );
        let result = &reply["result"];
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        let body = serde_json::from_str(text).unwrap_or(Value::String(text.to_string()));
        (result["isError"].as_bool().unwrap_or(false), body)
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mcp_submit_verify_reports_verified_mismatch_and_errors() {
    let cluster = Cluster::start(2).await;
    let mut mcp = Mcp::start(&cluster);

    let tools = mcp.call("tools/list", json!({}));
    let submit = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "submit")
        .unwrap();
    assert!(submit["inputSchema"]["properties"]["verify"].is_object());
    assert!(submit["inputSchema"]["properties"]["verify_file"].is_object());

    let (is_error, body) = mcp.submit(json!({
        "command": "sh", "args": ["-c", "printf hello > out.txt"],
        "outputs": ["out.txt"], "verify": 1, "timeout_secs": 60
    }));
    assert!(!is_error, "{body}");
    assert_eq!(body["verify"]["status"], "verified", "{body}");
    assert_eq!(body["verify"]["workers"].as_array().unwrap().len(), 2);

    let (is_error, body) = mcp.submit(json!({
        "command": "sh", "args": ["-c", "pwd > out.txt"],
        "outputs": ["out.txt"], "verify": 1, "timeout_secs": 60
    }));
    assert!(is_error, "a mismatch is an error result: {body}");
    assert_eq!(body["verify"]["status"], "mismatch", "{body}");
    assert_eq!(body["verify"]["differing"][0]["path"], "out.txt");

    let (is_error, body) = mcp.submit(json!({
        "command": "sh", "args": ["-c", "printf hello > out.txt"],
        "outputs": ["out.txt"], "verify": 2, "timeout_secs": 60
    }));
    assert!(is_error, "{body}");
    assert_eq!(body["verify"]["status"], "error", "{body}");
    assert!(body["verify"]["error"]
        .as_str()
        .unwrap()
        .contains("needs 2 other workers"));

    let (is_error, body) = mcp.submit(json!({
        "command": "sh", "args": ["-c", "pwd > data.txt; printf s > sums"],
        "verify": 1, "verify_file": "sums", "timeout_secs": 60
    }));
    assert!(!is_error, "{body}");
    assert_eq!(body["verify"]["compared"], json!(["sums"]));

    let (is_error, body) = mcp.submit(json!({"command": "true", "verify": 1}));
    assert!(is_error, "verify without outputs is refused: {body}");
    assert!(
        body.as_str()
            .unwrap_or("")
            .contains("compares declared outputs"),
        "{body}"
    );
}
