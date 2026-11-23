//! Integration tests for `AX-2` sharded runs, slot spreading, `--build-once`,
//! JUnit XML merge, and MCP `shard` tool (`docs/design/0103-ax2-sharded-runs.md`).
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Output, Stdio};
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
    async fn start_with_workers(specs: &[(&str, usize, &[&str])]) -> Self {
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
        for (name, slots, tags) in specs {
            cluster.add_worker(name, *slots, tags);
        }
        if !specs.is_empty() {
            cluster.wait_for_workers(specs.len()).await;
        }
        cluster
    }

    fn add_worker(&mut self, name: &str, slots: usize, tags: &[&str]) {
        let sandbox = tempfile::tempdir().unwrap();
        let ws_root = sandbox.path().join("workspaces");
        let mut config = WorkerConfig::new(self.addr.clone())
            .with_no_battery(true)
            .with_name(name)
            .with_cores(2)
            .with_ram_mb(4096)
            .with_max_concurrency(slots)
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
        let deadline = Instant::now() + Duration::from_secs(60);
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

struct SubprocessCluster {
    _master: MasterHandle,
    addr: String,
    children: Vec<Child>,
    _dirs: Vec<TempDir>,
    home: TempDir,
    fake_bin: TempDir,
}

impl SubprocessCluster {
    async fn start_with_stub_cargo(worker_names: &[&str], fail_archive: bool) -> Self {
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
        let stub_log_dir = tempfile::tempdir().unwrap();
        install_stub_cargo(fake_bin.path(), stub_log_dir.path(), fail_archive);

        let home = tempfile::tempdir().unwrap();
        let mut children = Vec::new();
        let mut dirs = vec![store, stub_log_dir];

        for name in worker_names {
            let w_home = tempfile::tempdir().unwrap();
            let mut cmd = isolated(Command::new(BIN), w_home.path(), fake_bin.path());
            cmd.args([
                "worker",
                "--master",
                &addr,
                "--name",
                name,
                "--max-concurrency",
                "2",
                "--no-portal",
                "--no-gpu",
                "--no-blender",
                "--no-cuda",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
            let child = cmd.spawn().expect("spawn oxideswarm worker");
            children.push(child);
            dirs.push(w_home);
        }

        let sc = Self {
            _master: master,
            addr,
            children,
            _dirs: dirs,
            home,
            fake_bin,
        };
        sc.wait_for_workers(worker_names.len()).await;
        sc
    }

    async fn wait_for_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(60);
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
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("{count} subprocess worker(s) did not register");
    }

    fn cli(&self) -> Command {
        isolated(Command::new(BIN), self.home.path(), self.fake_bin.path())
    }
}

impl Drop for SubprocessCluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn setup_fake_pmset() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let pmset = dir.path().join("pmset");
    std::fs::write(&pmset, "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\n").unwrap();
    let mut perms = std::fs::metadata(&pmset).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&pmset, perms).unwrap();
    dir
}

fn install_stub_cargo(fake_bin: &Path, log_dir: &Path, fail_archive: bool) {
    let cargo = fake_bin.join("cargo");
    let fail_flag = if fail_archive { "1" } else { "0" };
    let script = format!(
        r#"#!/bin/sh
set -eu
LOG_DIR="{log_dir}"
FAIL_ARCHIVE="{fail_flag}"
if [ "${{1:-}}" = "nextest" ]; then
  shift
  sub="${{1:-}}"
  shift || true
  if [ "$sub" = "archive" ]; then
    echo "ARCHIVE_CALL" >> "$LOG_DIR/calls.log"
    if [ "$FAIL_ARCHIVE" = "1" ]; then
      echo "simulated archive build failure" >&2
      exit 2
    fi
    if [ ! -f "Cargo.toml" ] || [ ! -f "src/lib.rs" ]; then
      echo "missing source files in builder sandbox" >&2
      exit 3
    fi
    if [ -e "target/stale.bin" ]; then
      echo "stale target/ directory must not be uploaded" >&2
      exit 4
    fi
    archive_file=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --archive-file) archive_file="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    printf 'STUB_ARCHIVE_OK\n' > "$archive_file"
    exit 0
  elif [ "$sub" = "run" ]; then
    echo "RUN_CALL:$OX_SHARD_INDEX/$OX_SHARD_TOTAL" >> "$LOG_DIR/calls.log"
    archive_file=""
    tool_cfg=""
    partition=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --archive-file) archive_file="$2"; shift 2 ;;
        --tool-config-file) tool_cfg="$2"; shift 2 ;;
        --partition) partition="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    if [ ! -f "$archive_file" ] || [ "$(cat "$archive_file")" != "STUB_ARCHIVE_OK" ]; then
      echo "missing or invalid archive '$archive_file'" >&2
      exit 5
    fi
    if [ ! -f "src/lib.rs" ]; then
      echo "missing remapped source src/lib.rs" >&2
      exit 6
    fi
    echo "STUB_RUN:$partition:SHARD=$OX_SHARD_INDEX/$OX_SHARD_TOTAL"
    if [ -n "$tool_cfg" ]; then
      cat > oxideswarm-junit.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="nextest.shard.$OX_SHARD_INDEX" tests="1" failures="0" errors="0" skipped="0" time="0.010">
    <testcase name="case_$OX_SHARD_INDEX" classname="demo" time="0.010"/>
  </testsuite>
</testsuites>
EOF
    fi
    exit 0
  fi
fi
echo "unexpected stub cargo invocation: $*" >&2
exit 9
"#,
        log_dir = log_dir.display(),
        fail_flag = fail_flag
    );
    std::fs::write(&cargo, script).unwrap();
    let mut perms = std::fs::metadata(&cargo).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&cargo, perms).unwrap();
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
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Mcp {
    fn start(master_addr: &str, home: &Path, fake_bin: &Path) -> Self {
        let mut cmd = isolated(Command::new(BIN), home, fake_bin);
        cmd.args(["mcp", "--master", master_addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn oxideswarm mcp");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut mcp = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };
        let init = mcp.request(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "test_ax2_shard", "version": "1.0" }
            }),
        );
        assert!(init.get("result").is_some(), "initialize failed: {init}");
        mcp.notify("notifications/initialized", json!({}));
        mcp
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&frame).unwrap();
        writeln!(self.stdin, "{line}").unwrap();
        self.stdin.flush().unwrap();

        loop {
            let mut buf = String::new();
            let n = self.stdout.read_line(&mut buf).unwrap();
            assert!(n > 0, "MCP server closed stdout unexpectedly");
            let v: Value = serde_json::from_str(&buf)
                .unwrap_or_else(|e| panic!("invalid JSON-RPC line ({e}): {buf}"));
            if v.get("id").and_then(Value::as_u64) == Some(id) {
                return v;
            }
        }
    }

    fn notify(&mut self, method: &str, params: Value) {
        let frame = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        writeln!(self.stdin, "{}", serde_json::to_string(&frame).unwrap()).unwrap();
        self.stdin.flush().unwrap();
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_env_injection_and_slot_spreading_across_workers() {
    let cluster = Cluster::start_with_workers(&[("w-heavy", 6, &[]), ("w-light", 2, &[])]).await;

    let out = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "8",
            "--json",
            "--env",
            "OX_SHARD_INDEX=999",
            "--env",
            "OX_SHARD_TOTAL=999",
            "--shell",
            "--",
            "printf '%s/%s' \"$OX_SHARD_INDEX\" \"$OX_SHARD_TOTAL\"",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", describe(&out));

    let body = json_of(&out);
    assert_eq!(body["shard"], true);
    assert_eq!(body["status"], "completed");
    assert_eq!(body["total_shards"], 8);
    assert_eq!(body["completed_shards"], 8);
    assert_eq!(body["failed_shards"], 0);
    assert_eq!(body["machines"], 2);
    assert!(body["waited_ms"].is_u64());
    assert!(body["task_time_sum_ms"].is_u64());

    let shards = body["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 8);
    let mut heavy = 0usize;
    let mut light = 0usize;
    for (idx, shard) in shards.iter().enumerate() {
        let expected_idx = idx + 1;
        assert_eq!(shard["index"], expected_idx);
        assert_eq!(shard["total"], 8);
        assert_eq!(shard["status"], "completed");
        assert_eq!(shard["stdout"], format!("{expected_idx}/8"));
        match shard["worker"].as_str().unwrap() {
            "w-heavy" => heavy += 1,
            "w-light" => light += 1,
            other => panic!("unexpected worker: {other}"),
        }
    }
    // SWRR prefers 6:2, but the preference is soft (design 0103 D7): a shard whose preferred
    // worker has no free slot at that moment runs on the other one instead of waiting.
    println!("shards: w-heavy {heavy}, w-light {light}");
    assert_eq!(heavy + light, 8);
    assert!(heavy >= 1 && light >= 1, "both workers must take shards");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_list_workers_preflight_zero_workers_fails_fast() {
    let cluster = Cluster::start_with_workers(&[]).await;
    let t0 = Instant::now();
    let out = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "3",
            "--timeout-secs",
            "600",
            "--json",
            "--",
            "echo",
            "ok",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "{}", describe(&out));
    // The "no connected workers" error below proves the pre-flight refused the run before any
    // shard was submitted; the time is printed, not asserted (TEST-86).
    eprintln!("[timing] 0-worker preflight: {:?}", t0.elapsed());
    let body = json_of(&out);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("no connected workers"),
        "{body}"
    );

    // Also test 1 connected worker with non-matching placement constraint.
    let cluster2 = Cluster::start_with_workers(&[("w1", 2, &["role-cpu"])]).await;
    let t1 = Instant::now();
    let out2 = cluster2
        .cli()
        .args([
            "shard",
            "--master",
            &cluster2.addr,
            "--count",
            "2",
            "--timeout-secs",
            "600",
            "--require-tag",
            "role-missing",
            "--json",
            "--",
            "echo",
            "ok",
        ])
        .output()
        .unwrap();
    assert!(!out2.status.success(), "{}", describe(&out2));
    eprintln!("[timing] non-matching preflight: {:?}", t1.elapsed());
    let body2 = json_of(&out2);
    assert!(
        body2["error"]
            .as_str()
            .unwrap()
            .contains("no connected worker matches"),
        "{body2}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_partial_failure_and_fail_fast_cancels_siblings() {
    let cluster = Cluster::start_with_workers(&[("w1", 2, &[]), ("w2", 2, &[])]).await;

    // 1) --fail-fast cancels slow siblings long before their 600 s sleep ends.
    let t0 = Instant::now();
    let out_ff = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "3",
            "--fail-fast",
            "--timeout-secs",
            "900",
            "--json",
            "--shell",
            "--",
            "if [ \"$OX_SHARD_INDEX\" = \"1\" ]; then exit 7; else sleep 600; fi",
        ])
        .output()
        .unwrap();
    assert!(!out_ff.status.success(), "{}", describe(&out_ff));
    assert_eq!(out_ff.status.code(), Some(7));
    // `cancelled_shards == 2` below proves the two `sleep 600` siblings were cancelled rather than
    // run to the end; the time is printed, not asserted (TEST-86).
    eprintln!("[timing] --fail-fast: {:?}", t0.elapsed());
    let body_ff = json_of(&out_ff);
    assert_eq!(body_ff["status"], "failed");
    assert_eq!(body_ff["failed_shards"], 1);
    assert_eq!(body_ff["cancelled_shards"], 2);

    // 2) Without --fail-fast, all shards finish and partial failure exits non-zero.
    let out_all = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "3",
            "--json",
            "--shell",
            "--",
            "if [ \"$OX_SHARD_INDEX\" = \"2\" ]; then exit 5; fi; echo ok-$OX_SHARD_INDEX",
        ])
        .output()
        .unwrap();
    assert!(!out_all.status.success(), "{}", describe(&out_all));
    assert_eq!(out_all.status.code(), Some(5));
    let body_all = json_of(&out_all);
    assert_eq!(body_all["completed_shards"], 2);
    assert_eq!(body_all["failed_shards"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_output_dir_isolation_and_symlink_refusal() {
    let cluster = Cluster::start_with_workers(&[("w1", 2, &[]), ("w2", 2, &[])]).await;
    let out_dir = cluster.home.path().join("shard-outputs");

    let out = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "3",
            "--output",
            "out.txt",
            "--output-dir",
            out_dir.to_str().unwrap(),
            "--shell",
            "--",
            "printf 'shard-%s' \"$OX_SHARD_INDEX\" > out.txt",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", describe(&out));
    for i in 1..=3 {
        let p = out_dir.join(format!("shard-{i:02}")).join("out.txt");
        assert_eq!(std::fs::read_to_string(&p).unwrap(), format!("shard-{i}"));
    }

    // Symlink refusal on shard-01 output dir.
    let outside = cluster.home.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    let bad_out = cluster.home.path().join("bad-outputs");
    std::fs::create_dir_all(&bad_out).unwrap();
    std::os::unix::fs::symlink(&outside, bad_out.join("shard-01")).unwrap();

    let out_sym = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "2",
            "--output",
            "out.txt",
            "--output-dir",
            bad_out.to_str().unwrap(),
            "--json",
            "--shell",
            "--",
            "printf 'bad' > out.txt",
        ])
        .output()
        .unwrap();
    assert!(!out_sym.status.success(), "{}", describe(&out_sym));
    assert!(
        std::fs::read_dir(&outside).unwrap().next().is_none(),
        "symlink target directory must remain untouched"
    );

    // Symlink-ancestor refusal: `--output-dir <sym_parent>/nested` where `<sym_parent> -> outside_nested`.
    let outside_nested = cluster.home.path().join("outside-nested");
    std::fs::create_dir_all(&outside_nested).unwrap();
    let sym_parent = cluster.home.path().join("sym-parent");
    std::os::unix::fs::symlink(&outside_nested, &sym_parent).unwrap();
    let nested_out_dir = sym_parent.join("nested");

    let out_sym_ancestor = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "2",
            "--output",
            "leak.txt",
            "--output-dir",
            nested_out_dir.to_str().unwrap(),
            "--json",
            "--shell",
            "--",
            "echo LEAKED > leak.txt",
        ])
        .output()
        .unwrap();
    assert!(
        !out_sym_ancestor.status.success(),
        "symlinked --output-dir ancestor must be refused: {}",
        describe(&out_sym_ancestor)
    );
    let body_sym_ancestor = json_of(&out_sym_ancestor);
    assert!(
        body_sym_ancestor["error"]
            .as_str()
            .unwrap_or("")
            .contains("symbolic link"),
        "expected symlink refusal error, got: {body_sym_ancestor}"
    );
    assert!(
        std::fs::read_dir(&outside_nested).unwrap().next().is_none(),
        "outside target of symlinked --output-dir ancestor must remain empty"
    );

    // Dotdot-after-nonexistent symlink bypass refusal: `--output-dir nonexistent/../sym-dir`
    // where `nonexistent` does not exist on disk and `sym-dir -> outside_dotdot`.
    let outside_dotdot = cluster.home.path().join("outside-dotdot");
    std::fs::create_dir_all(&outside_dotdot).unwrap();
    std::os::unix::fs::symlink(&outside_dotdot, cluster.home.path().join("sym-dir")).unwrap();

    let out_dotdot = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "1",
            "--output",
            "leak.txt",
            "--output-dir",
            "nonexistent/../sym-dir",
            "--json",
            "--shell",
            "--",
            "echo LEAK > leak.txt",
        ])
        .output()
        .unwrap();
    assert!(
        !out_dotdot.status.success(),
        "nonexistent/../sym-dir --output-dir must be refused: {}",
        describe(&out_dotdot)
    );
    let body_dotdot = json_of(&out_dotdot);
    assert!(
        body_dotdot["error"].as_str().unwrap_or("").contains(".."),
        "expected '..' refusal error, got: {body_dotdot}"
    );
    assert!(
        !cluster.home.path().join("nonexistent").exists(),
        "preflight refusal must not create 'nonexistent/' on disk"
    );
    assert!(
        std::fs::read_dir(&outside_dotdot).unwrap().next().is_none(),
        "outside target of nonexistent/../sym-dir must remain empty"
    );

    // Relative multi-level non-existent `--output-dir ./rel/nested/out` works cleanly.
    let out_rel = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "1",
            "--output",
            "ok.txt",
            "--output-dir",
            "./rel/nested/out",
            "--shell",
            "--",
            "printf 'ok' > ok.txt",
        ])
        .output()
        .unwrap();
    assert!(out_rel.status.success(), "{}", describe(&out_rel));
    assert_eq!(
        std::fs::read_to_string(cluster.home.path().join("rel/nested/out/shard-01/ok.txt"))
            .unwrap(),
        "ok"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_junit_out_merges_per_shard_reports_without_cluttering_cwd() {
    let cluster = Cluster::start_with_workers(&[("w1", 2, &[]), ("w2", 2, &[])]).await;
    let junit_path = cluster.home.path().join("reports/merged-junit.xml");

    // Failing shard 2 + passing shards 1 & 3, NO --output-dir -> must NOT create ./shard-01/.
    let out = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "3",
            "--junit-out",
            junit_path.to_str().unwrap(),
            "--json",
            "--shell",
            "--",
            r#"if [ "$OX_SHARD_INDEX" = "2" ]; then
  cat > junit.xml <<EOF
<testsuite name="shard-2" tests="2" failures="1" errors="0" skipped="0" time="0.200">
  <testcase name="ok_2" classname="suite" time="0.100"/>
  <testcase name="fail_2" classname="suite" time="0.100"><failure message="bad">trace</failure></testcase>
</testsuite>
EOF
  exit 1
else
  cat > junit.xml <<EOF
<testsuite name="shard-$OX_SHARD_INDEX" tests="2" failures="0" errors="0" skipped="0" time="0.100">
  <testcase name="a_$OX_SHARD_INDEX" classname="suite" time="0.050"/>
  <testcase name="b_$OX_SHARD_INDEX" classname="suite" time="0.050"/>
</testsuite>
EOF
fi"#,
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(
        !cluster.home.path().join("shard-01").exists(),
        "CWD must not be cluttered with shard-01/ when --output-dir is omitted"
    );

    let body = json_of(&out);
    assert_eq!(body["junit"]["tests"], 6);
    assert_eq!(body["junit"]["failures"], 1);
    assert_eq!(body["junit"]["errors"], 0);
    assert_eq!(body["junit"]["suites"], 3);

    let xml_text = std::fs::read_to_string(&junit_path).unwrap();
    assert!(xml_text.contains("tests=\"6\""));
    assert!(xml_text.contains("failures=\"1\""));
    assert!(xml_text.contains("name=\"fail_2\""));

    // All-passing shards -> exit 0.
    let junit_pass = cluster.home.path().join("reports/pass-junit.xml");
    let out_pass = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "2",
            "--junit-out",
            junit_pass.to_str().unwrap(),
            "--shell",
            "--",
            r#"cat > junit.xml <<EOF
<testsuite name="s-$OX_SHARD_INDEX" tests="2" failures="0" errors="0" skipped="0" time="0.050">
  <testcase name="t1" classname="c" time="0.025"/>
  <testcase name="t2" classname="c" time="0.025"/>
</testsuite>
EOF"#,
        ])
        .output()
        .unwrap();
    assert!(out_pass.status.success(), "{}", describe(&out_pass));
    let pass_xml = std::fs::read_to_string(&junit_pass).unwrap();
    assert!(pass_xml.contains("tests=\"4\""));
    assert!(pass_xml.contains("failures=\"0\""));

    // Nested <testsuite> inside <testsuite> where outer omits summary attributes.
    let junit_nested = cluster.home.path().join("reports/nested-junit.xml");
    let out_nested = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "1",
            "--junit-out",
            junit_nested.to_str().unwrap(),
            "--json",
            "--shell",
            "--",
            r#"cat > junit.xml <<'EOF'
<testsuites><testsuite name="outer"><testsuite name="inner" tests="2" failures="1" errors="0" skipped="0" time="0.3"><testcase name="x"/><testcase name="y"><failure/></testcase></testsuite></testsuite></testsuites>
EOF"#,
        ])
        .output()
        .unwrap();
    assert!(out_nested.status.success(), "{}", describe(&out_nested));
    let nested_body = json_of(&out_nested);
    assert_eq!(nested_body["junit"]["tests"], 2, "{nested_body}");
    assert_eq!(nested_body["junit"]["failures"], 1, "{nested_body}");
    assert_eq!(nested_body["junit"]["errors"], 0, "{nested_body}");
    let nested_time = nested_body["junit"]["time_secs"].as_f64().unwrap();
    assert!(
        (nested_time - 0.3).abs() < 1e-6,
        "unexpected time_secs: {nested_time}"
    );
    let nested_xml = std::fs::read_to_string(&junit_nested).unwrap();
    assert!(nested_xml.contains("tests=\"2\""), "{nested_xml}");
    assert!(nested_xml.contains("failures=\"1\""), "{nested_xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_build_once_archive_blob_inputs_pipeline() {
    let cluster = SubprocessCluster::start_with_stub_cargo(&["sub-w1", "sub-w2"], false).await;

    let project = cluster.home.path().join("demo-crate");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(project.join("target")).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"demo\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    std::fs::write(
        project.join("src/lib.rs"),
        "pub fn sum(a: i32, b: i32) -> i32 { a + b }\n",
    )
    .unwrap();
    std::fs::write(project.join("target/stale.bin"), b"stale").unwrap();

    let junit_out = cluster.home.path().join("build-once-junit.xml");
    let out = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "4",
            "--build-once",
            "--build-source",
            project.to_str().unwrap(),
            "--junit-out",
            junit_out.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", describe(&out));

    let body = json_of(&out);
    assert_eq!(body["status"], "completed");
    assert_eq!(body["completed_shards"], 4);
    assert_eq!(body["machines"], 2);

    let bo = body["build_once"].as_array().unwrap();
    assert_eq!(
        bo.len(),
        1,
        "expected exactly 1 builder task for the (os, arch) group"
    );
    assert_eq!(bo[0]["archive_size_bytes"], 16);

    let shards = body["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 4);
    for (idx, s) in shards.iter().enumerate() {
        let m = idx + 1;
        assert!(
            s["stdout"]
                .as_str()
                .unwrap()
                .contains(&format!("STUB_RUN:slice:{m}/4:SHARD={m}/4")),
            "unexpected shard stdout: {s}"
        );
    }

    assert_eq!(body["junit"]["tests"], 4);
    assert_eq!(body["junit"]["failures"], 0);
    let xml = std::fs::read_to_string(&junit_out).unwrap();
    assert!(xml.contains("tests=\"4\""));

    // Now verify builder failure aborts BEFORE dispatching any shards.
    let fail_cluster = SubprocessCluster::start_with_stub_cargo(&["sub-fail"], true).await;
    let out_fail = fail_cluster
        .cli()
        .args([
            "shard",
            "--master",
            &fail_cluster.addr,
            "--count",
            "3",
            "--build-once",
            "--build-source",
            project.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!out_fail.status.success(), "{}", describe(&out_fail));
    let body_fail = json_of(&out_fail);
    assert!(
        body_fail["error"]
            .as_str()
            .unwrap()
            .contains("build-once archive failed"),
        "{body_fail}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_mcp_tools_list_call_and_validation() {
    let cluster = Cluster::start_with_workers(&[("w1", 2, &[]), ("w2", 2, &[])]).await;
    let mut mcp = Mcp::start(&cluster.addr, cluster.home.path(), cluster.fake_bin.path());

    let list = mcp.request("tools/list", json!({}));
    let tools = list["result"]["tools"].as_array().unwrap();
    assert!(
        tools.iter().any(|t| t["name"] == "shard"),
        "tools/list missing 'shard': {list}"
    );

    // Invalid params -> JSON-RPC -32602.
    let bad = mcp.request(
        "tools/call",
        json!({
            "name": "shard",
            "arguments": { "count": 0, "command": "echo" }
        }),
    );
    assert_eq!(bad["error"]["code"], -32602, "expected -32602: {bad}");

    // Valid MCP shard execution.
    let ok = mcp.request(
        "tools/call",
        json!({
            "name": "shard",
            "arguments": {
                "count": 3,
                "type": "shell",
                "script": "printf 'mcp-%s/%s' \"$OX_SHARD_INDEX\" \"$OX_SHARD_TOTAL\""
            }
        }),
    );
    assert_eq!(ok["result"]["isError"], false, "{ok}");
    let text = ok["result"]["content"][0]["text"].as_str().unwrap();
    let report: Value = serde_json::from_str(text).unwrap();
    assert_eq!(report["completed_shards"], 3);
    assert_eq!(report["machines"], 2);
    assert_eq!(report["shards"][0]["stdout"], "mcp-1/3");
    assert_eq!(report["shards"][2]["stdout"], "mcp-3/3");
}

#[test]
fn test_shard_offline_validation_refusals() {
    let home = tempfile::tempdir().unwrap();
    let fake_bin = setup_fake_pmset();
    let outside = home.path().join("outside-offline");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, home.path().join("sym-offline")).unwrap();

    let cases: &[&[&str]] = &[
        &["shard", "--count", "0", "--", "echo", "hi"],
        &["shard", "--count", "257", "--", "echo", "hi"],
        &["shard", "--count", "3"],
        &["shard", "--count", "2", "--workspace", "ws", "--build-once"],
        &[
            "shard",
            "--count",
            "2",
            "--workspace",
            "ws",
            "--input",
            "a.txt",
            "--",
            "echo",
        ],
        &[
            "shard",
            "--count",
            "2",
            "--build-once",
            "--build-source",
            "does-not-exist",
        ],
        &["shard", "--count", "2", "--env", "NOEQUALS", "--", "echo"],
        &[
            "shard",
            "--count",
            "1",
            "--output-dir",
            "nonexistent/../sym-offline",
            "--",
            "echo",
        ],
        &[
            "shard",
            "--count",
            "1",
            "--junit-out",
            "nonexistent/../sym-offline/out.xml",
            "--",
            "echo",
        ],
        &[
            "shard",
            "--count",
            "1",
            "--junit-out",
            "nonexistent/..",
            "--",
            "echo",
        ],
    ];
    for args in cases {
        let out = isolated(Command::new(BIN), home.path(), fake_bin.path())
            .args(*args)
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "expected offline refusal for {args:?}: {}",
            describe(&out)
        );
    }
    assert!(
        !home.path().join("nonexistent").exists(),
        "offline refusals must not create 'nonexistent/' on disk"
    );
    assert!(
        std::fs::read_dir(&outside).unwrap().next().is_none(),
        "offline refusals must leave symlink target untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_slot_spreading_with_max_retries_failover_and_worker_pin() {
    // 1) Verify `slots:`-weighted 6:2 SWRR spreading still holds when `--max-retries 2` is passed,
    //    and explicit `--worker` pins strictly to that worker.
    let spread_cluster =
        Cluster::start_with_workers(&[("w-heavy", 6, &[]), ("w-light", 2, &[])]).await;
    let spread_out = spread_cluster
        .cli()
        .args([
            "shard",
            "--master",
            &spread_cluster.addr,
            "--count",
            "8",
            "--max-retries",
            "2",
            "--json",
            "--shell",
            "--",
            "printf '%s/%s' \"$OX_SHARD_INDEX\" \"$OX_SHARD_TOTAL\"",
        ])
        .output()
        .unwrap();
    assert!(spread_out.status.success(), "{}", describe(&spread_out));
    let spread_body = json_of(&spread_out);
    assert_eq!(spread_body["completed_shards"], 8);
    let mut heavy = 0usize;
    let mut light = 0usize;
    for shard in spread_body["shards"].as_array().unwrap() {
        match shard["worker"].as_str().unwrap() {
            "w-heavy" => heavy += 1,
            "w-light" => light += 1,
            other => panic!("unexpected worker: {other}"),
        }
    }
    println!("shards with --max-retries: w-heavy {heavy}, w-light {light}");
    assert_eq!(heavy + light, 8);
    assert!(
        heavy >= 1 && light >= 1,
        "both workers must take shards (soft SWRR, design 0103 D7)"
    );

    let pin_out = spread_cluster
        .cli()
        .args([
            "shard",
            "--master",
            &spread_cluster.addr,
            "--count",
            "4",
            "--max-retries",
            "2",
            "--worker",
            "w-light",
            "--json",
            "--",
            "echo",
            "pinned",
        ])
        .output()
        .unwrap();
    assert!(pin_out.status.success(), "{}", describe(&pin_out));
    let pin_body = json_of(&pin_out);
    assert!(pin_body["shards"]
        .as_array()
        .unwrap()
        .iter()
        .all(|s| s["worker"].as_str() == Some("w-light")));
    drop(spread_cluster);

    // 2) Verify soft `prefer:worker=` failover when the sole pre-flight worker dies mid-run.
    let store = tempfile::tempdir().unwrap();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_discovery_port(0)
        .with_blob_store(store.path(), 64 * 1024 * 1024);
    let sched_cfg = SchedulerConfig::default()
        .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
        .without_host_cpu_backpressure();
    let reaper_cfg = ReaperConfig::default()
        .with_scan_interval(Duration::from_secs(1))
        .with_timeout(Duration::from_secs(3));
    let master = MasterServer::spawn_with_config(config, sched_cfg, reaper_cfg)
        .await
        .expect("master starts");
    let addr = master.server_addr().to_string();
    let fake_bin = setup_fake_pmset();
    let cli_home = tempfile::tempdir().unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    let marker = marker_dir.path().join("started_1");

    struct ChildGuard(Option<Child>);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    let w_die_home = tempfile::tempdir().unwrap();
    let mut w_die_cmd = isolated(Command::new(BIN), w_die_home.path(), fake_bin.path());
    w_die_cmd
        .args([
            "worker",
            "--master",
            &addr,
            "--name",
            "w-die-1",
            "--cores",
            "1",
            "--max-concurrency",
            "1",
            "--heartbeat-interval-secs",
            "1",
            "--no-portal",
            "--no-gpu",
            "--no-blender",
            "--no-cuda",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut w_die = ChildGuard(Some(w_die_cmd.spawn().expect("spawn w-die-1")));

    let wait_reg = Instant::now() + Duration::from_secs(60);
    loop {
        if master
            .list_workers()
            .await
            .unwrap_or_default()
            .iter()
            .any(|w| w.status != WorkerStatus::Disconnected && w.capabilities.name == "w-die-1")
        {
            break;
        }
        assert!(Instant::now() < wait_reg, "w-die-1 did not register");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    let script = format!(
        "if [ \"$OX_SHARD_INDEX\" = \"1\" ] && [ ! -f '{}' ]; then touch '{}'; sleep 20; fi; printf 'ok-%s' \"$OX_SHARD_INDEX\"",
        marker.display(),
        marker.display()
    );
    let mut shard_cmd = isolated(Command::new(BIN), cli_home.path(), fake_bin.path());
    shard_cmd
        .args([
            "shard",
            "--master",
            &addr,
            "--count",
            "3",
            "--max-retries",
            "2",
            "--timeout-secs",
            "30",
            "--json",
            "--shell",
            "--",
            &script,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let shard_child = shard_cmd.spawn().expect("spawn shard");

    let wait_marker = Instant::now() + Duration::from_secs(60);
    while !marker.exists() {
        assert!(
            Instant::now() < wait_marker,
            "shard 1 did not start on w-die-1"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // Start w-survive-2, then kill w-die-1 to force failover of shard 1 and queued shards 2 & 3.
    let w_surv_home = tempfile::tempdir().unwrap();
    let mut w_surv_cmd = isolated(Command::new(BIN), w_surv_home.path(), fake_bin.path());
    w_surv_cmd
        .args([
            "worker",
            "--master",
            &addr,
            "--name",
            "w-survive-2",
            "--cores",
            "4",
            "--max-concurrency",
            "4",
            "--heartbeat-interval-secs",
            "1",
            "--no-portal",
            "--no-gpu",
            "--no-blender",
            "--no-cuda",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut w_surv = ChildGuard(Some(w_surv_cmd.spawn().expect("spawn w-survive-2")));

    let wait_surv = Instant::now() + Duration::from_secs(60);
    loop {
        if master
            .list_workers()
            .await
            .unwrap_or_default()
            .iter()
            .any(|w| w.status != WorkerStatus::Disconnected && w.capabilities.name == "w-survive-2")
        {
            break;
        }
        assert!(Instant::now() < wait_surv, "w-survive-2 did not register");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    if let Some(mut child) = w_die.0.take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    let out = tokio::task::spawn_blocking(move || shard_child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    if let Some(mut child) = w_surv.0.take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    assert!(out.status.success(), "{}", describe(&out));
    let body = json_of(&out);
    assert_eq!(body["completed_shards"], 3, "{body}");
    assert_eq!(body["shards"][0]["stdout"], "ok-1");
    assert_eq!(body["shards"][1]["stdout"], "ok-2");
    assert_eq!(body["shards"][2]["stdout"], "ok-3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_count_1_and_256_boundaries() {
    let cluster = Cluster::start_with_workers(&[("w-256", 16, &[])]).await;

    // 1. --count 1 boundary with --output-dir
    let out_dir_1 = cluster.home.path().join("out-1");
    let out_1 = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "1",
            "--output-dir",
            out_dir_1.to_str().unwrap(),
            "--output",
            "res.txt",
            "--json",
            "--shell",
            "--",
            "printf '%s/%s' \"$OX_SHARD_INDEX\" \"$OX_SHARD_TOTAL\" > res.txt",
        ])
        .output()
        .unwrap();
    assert!(out_1.status.success(), "{}", describe(&out_1));
    let body_1 = json_of(&out_1);
    assert_eq!(body_1["completed_shards"], 1);
    assert_eq!(
        std::fs::read_to_string(out_dir_1.join("shard-01/res.txt")).unwrap(),
        "1/1"
    );

    // 2. --count 256 boundary (exercises MAX_CONCURRENT_HANDSHAKES semaphore + 3-digit slugs)
    let out_256 = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "256",
            "--timeout-secs",
            "60",
            "--json",
            "--",
            "true",
        ])
        .output()
        .unwrap();
    assert!(out_256.status.success(), "{}", describe(&out_256));
    let body_256 = json_of(&out_256);
    assert_eq!(body_256["status"], "completed");
    assert_eq!(body_256["completed_shards"], 256);
    let shards = body_256["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 256);
    assert_eq!(shards[0]["slug"], "shard-001");
    assert_eq!(shards[255]["slug"], "shard-256");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_output_without_output_dir_refuses_symlink_shard01() {
    let cluster = Cluster::start_with_workers(&[("w-sym", 2, &[])]).await;
    let outside = cluster.home.path().join("outside-target");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, cluster.home.path().join("shard-01")).unwrap();

    let out = cluster
        .cli()
        .args([
            "shard",
            "--master",
            &cluster.addr,
            "--count",
            "1",
            "--output",
            "leak.txt",
            "--json",
            "--shell",
            "--",
            "printf 'secret' > leak.txt",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "{}", describe(&out));
    let body = json_of(&out);
    assert_eq!(body["shards"][0]["status"], "error", "{body}");
    assert!(
        body["shards"][0]["error"]
            .as_str()
            .unwrap_or("")
            .contains("symbolic link"),
        "{body}"
    );
    assert!(
        std::fs::read_dir(&outside).unwrap().next().is_none(),
        "symlink target directory must remain untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_build_once_rejects_empty_and_omitted_archives_e2e() {
    for mode in ["empty", "omit"] {
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
        let log_dir = tempfile::tempdir().unwrap();
        let cargo = fake_bin.path().join("cargo");
        let script = format!(
            r#"#!/bin/sh
set -eu
LOG_DIR="{log_dir}"
MODE="{mode}"
if [ "${{1:-}}" = "nextest" ]; then
  shift
  sub="${{1:-}}"
  shift || true
  if [ "$sub" = "archive" ]; then
    echo "ARCHIVE_CALL" >> "$LOG_DIR/calls.log"
    archive_file=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --archive-file) archive_file="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    if [ "$MODE" = "empty" ]; then
      : > "$archive_file"
    fi
    exit 0
  elif [ "$sub" = "run" ]; then
    echo "RUN_CALL" >> "$LOG_DIR/calls.log"
    exit 0
  fi
fi
exit 9
"#,
            log_dir = log_dir.path().display(),
            mode = mode
        );
        std::fs::write(&cargo, script).unwrap();
        let mut perms = std::fs::metadata(&cargo).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&cargo, perms).unwrap();

        let w_home = tempfile::tempdir().unwrap();
        let mut w_cmd = isolated(Command::new(BIN), w_home.path(), fake_bin.path());
        w_cmd
            .args([
                "worker",
                "--master",
                &addr,
                "--name",
                "sub-bad-arch",
                "--max-concurrency",
                "2",
                "--no-portal",
                "--no-gpu",
                "--no-blender",
                "--no-cuda",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut w_child = w_cmd.spawn().unwrap();
        let wait_w = Instant::now() + Duration::from_secs(60);
        while Instant::now() < wait_w {
            if master
                .list_workers()
                .await
                .unwrap_or_default()
                .iter()
                .any(|w| w.status != WorkerStatus::Disconnected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        let cli_home = tempfile::tempdir().unwrap();
        let project = cli_home.path().join("crate-bad-arch");
        std::fs::create_dir_all(project.join("src")).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname = \"demo\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::write(project.join("src/lib.rs"), "pub fn f() {}\n").unwrap();

        let out = isolated(Command::new(BIN), cli_home.path(), fake_bin.path())
            .args([
                "shard",
                "--master",
                &addr,
                "--count",
                "2",
                "--build-once",
                "--build-source",
                project.to_str().unwrap(),
                "--json",
            ])
            .output()
            .unwrap();
        let _ = w_child.kill();
        let _ = w_child.wait();

        assert!(!out.status.success(), "mode={mode}: {}", describe(&out));
        let body = json_of(&out);
        let err = body["error"].as_str().unwrap_or("");
        if mode == "empty" {
            assert!(err.contains("is empty (0 bytes)"), "{body}");
        } else {
            assert!(err.contains("did not produce"), "{body}");
        }
        let calls = std::fs::read_to_string(log_dir.path().join("calls.log")).unwrap_or_default();
        assert!(
            !calls.contains("RUN_CALL"),
            "mode={mode} must abort before dispatching shards: {calls}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_mcp_adversarial_refusals() {
    let cluster = Cluster::start_with_workers(&[("w-mcp", 2, &[])]).await;
    let outside = cluster.home.path().join("outside-mcp");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("keep.xml"), "keep").unwrap();
    std::os::unix::fs::symlink(&outside, cluster.home.path().join("sym-out-dir")).unwrap();
    std::os::unix::fs::symlink(
        outside.join("keep.xml"),
        cluster.home.path().join("sym-junit.xml"),
    )
    .unwrap();
    std::fs::write(cluster.home.path().join("a.txt"), "hi").unwrap();

    let mut mcp = Mcp::start(&cluster.addr, cluster.home.path(), cluster.fake_bin.path());

    // 1. build_once + workspace -> JSON-RPC -32602
    let r_bo_ws = mcp.request(
        "tools/call",
        json!({
            "name": "shard",
            "arguments": { "count": 1, "build_once": true, "workspace": "ws" }
        }),
    );
    assert_eq!(r_bo_ws["error"]["code"], -32602, "{r_bo_ws}");

    // 2. workspace + files -> JSON-RPC -32602
    let r_ws_files = mcp.request(
        "tools/call",
        json!({
            "name": "shard",
            "arguments": {
                "count": 1,
                "workspace": "ws",
                "files": ["a.txt"],
                "command": "echo"
            }
        }),
    );
    assert_eq!(r_ws_files["error"]["code"], -32602, "{r_ws_files}");

    // 3. junit_out symlink -> refused
    let r_junit_sym = mcp.request(
        "tools/call",
        json!({
            "name": "shard",
            "arguments": {
                "count": 1,
                "junit_out": "sym-junit.xml",
                "command": "echo"
            }
        }),
    );
    assert_eq!(r_junit_sym["result"]["isError"], true, "{r_junit_sym}");
    assert_eq!(
        std::fs::read_to_string(outside.join("keep.xml")).unwrap(),
        "keep"
    );

    // 4. output_dir symlink -> refused
    let r_outdir_sym = mcp.request(
        "tools/call",
        json!({
            "name": "shard",
            "arguments": {
                "count": 1,
                "output_dir": "sym-out-dir",
                "command": "echo"
            }
        }),
    );
    assert_eq!(r_outdir_sym["result"]["isError"], true, "{r_outdir_sym}");
}
