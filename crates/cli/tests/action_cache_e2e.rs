//! End-to-end integration tests for `CACHE-1` content-addressed action cache (design 0100).

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::watch;

use rusty_grid_master::action_cache::ACTION_CACHE_FILE;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

fn cli_cmd(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_INSECURE")
        .env("NO_COLOR", "1");
    cmd
}

async fn spawn_cluster(
    action_cache_max_bytes: u64,
    state_dir: Option<&Path>,
) -> (
    MasterHandle,
    String,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let blob_dir = temp.path().join("master_blobs");
    let sandbox_dir = temp.path().join("worker_sandboxes");
    std::fs::create_dir_all(&blob_dir).expect("create blob_dir");
    std::fs::create_dir_all(&sandbox_dir).expect("create sandbox_dir");

    let mut server_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("server config")
        .with_web_ui(false)
        .with_discovery_port(0)
        .with_action_cache_max_bytes(action_cache_max_bytes);
    if let Some(dir) = state_dir {
        server_cfg = server_cfg
            .with_state_dir(dir)
            .with_session_resume_hold(Duration::ZERO);
    } else {
        server_cfg = server_cfg.with_blob_store(blob_dir, 64 * 1024 * 1024);
    }

    let master = MasterServer::spawn(server_cfg).await.expect("spawn master");
    let master_addr = master.server_addr().to_string();

    let worker_cfg = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("cache-worker")
        .with_cores(4)
        .with_ram_mb(4096)
        .with_sandbox_base_dir(&sandbox_dir);
    let mut worker = WorkerClient::new(worker_cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while master.registry().list_workers().await.is_empty() {
        assert!(Instant::now() < deadline, "worker did not register in time");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    (master, master_addr, shutdown_tx, worker_handle, temp)
}

#[cfg(unix)]
fn copy_and_count_command(exit_code: i32) -> (&'static str, String) {
    (
        "sh",
        format!("echo x >> \"$COUNTER_FILE\" && cp in.bin out.bin && exit {exit_code}"),
    )
}

#[cfg(windows)]
fn copy_and_count_command(exit_code: i32) -> (&'static str, String) {
    (
        "powershell",
        format!(
            "Add-Content -Path $env:COUNTER_FILE -Value 'x'; Copy-Item -Force in.bin out.bin; exit {exit_code}"
        ),
    )
}

fn counter_lines(path: &Path) -> usize {
    match std::fs::read_to_string(path) {
        Ok(s) => s.lines().count(),
        Err(_) => 0,
    }
}

fn submit_cached_json_with_args(
    home: &Path,
    master_addr: &str,
    input_path: &Path,
    output_dir: &Path,
    counter_path: &Path,
    exit_code: i32,
    extra_args: &[&str],
) -> Value {
    std::fs::create_dir_all(output_dir).expect("create output dir");
    let (prog, script) = copy_and_count_command(exit_code);
    let input_arg = format!("{}:in.bin", input_path.display());
    let counter_env = format!("COUNTER_FILE={}", counter_path.display());
    let out_dir_str = output_dir.display().to_string();
    let mut args = vec![
        "submit",
        "--master",
        master_addr,
        "--cache",
        "--json",
        "--input",
        &input_arg,
        "--output",
        "out.bin",
        "--output-dir",
        &out_dir_str,
        "--env",
        &counter_env,
    ];
    args.extend_from_slice(extra_args);
    #[cfg(unix)]
    args.extend_from_slice(&["--", prog, "-c", &script]);
    #[cfg(windows)]
    args.extend_from_slice(&["--", prog, "-NoProfile", "-Command", &script]);

    let out = cli_cmd(home).args(args).output().expect("run submit");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if exit_code == 0 {
        assert!(
            out.status.success(),
            "expected success for submit, status={:?}\nstdout={stdout}\nstderr={stderr}",
            out.status.code()
        );
    } else {
        assert_eq!(
            out.status.code(),
            Some(exit_code),
            "expected exit_code={exit_code}\nstdout={stdout}\nstderr={stderr}"
        );
    }
    serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("invalid JSON stdout: {e}\nstdout={stdout}\nstderr={stderr}"))
}

fn submit_cached_json(
    home: &Path,
    master_addr: &str,
    input_path: &Path,
    output_dir: &Path,
    counter_path: &Path,
    exit_code: i32,
) -> Value {
    submit_cached_json_with_args(
        home,
        master_addr,
        input_path,
        output_dir,
        counter_path,
        exit_code,
        &[],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_1_to_3_hit_input_mutation_miss_and_failed_not_cached() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) =
        spawn_cluster(64 * 1024 * 1024, None).await;
    let home = temp.path();
    let input_path = home.join("input.bin");
    let counter_path = home.join("counter.txt");

    // Scenario 1: Identical `--input` + `--cache` twice -> 2nd hits cache, skips worker, and
    // shows `Cached: yes` on both `submit` and `result`.
    std::fs::write(&input_path, b"cache-payload-v1\n").unwrap();
    let first = submit_cached_json(
        home,
        &master_addr,
        &input_path,
        &home.join("out1"),
        &counter_path,
        0,
    );
    assert_eq!(first["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 1);
    assert_eq!(
        std::fs::read(home.join("out1").join("out.bin")).unwrap(),
        b"cache-payload-v1\n"
    );

    // Run 2nd submit in human mode to verify `Cached: yes`.
    let (prog, script) = copy_and_count_command(0);
    let input_arg = format!("{}:in.bin", input_path.display());
    let counter_env = format!("COUNTER_FILE={}", counter_path.display());
    let out2_dir = home.join("out2");
    std::fs::create_dir_all(&out2_dir).unwrap();
    let out2_str = out2_dir.display().to_string();
    let second_human = cli_cmd(home)
        .args([
            "submit",
            "--master",
            &master_addr,
            "--cache",
            "--input",
            &input_arg,
            "--output",
            "out.bin",
            "--output-dir",
            &out2_str,
            "--env",
            &counter_env,
            "--",
            prog,
            #[cfg(windows)]
            "-NoProfile",
            #[cfg(windows)]
            "-Command",
            #[cfg(unix)]
            "-c",
            &script,
        ])
        .output()
        .expect("run human cached submit");
    assert!(
        second_human.status.success(),
        "human cached submit failed: {}",
        String::from_utf8_lossy(&second_human.stderr)
    );
    let second_stdout = String::from_utf8_lossy(&second_human.stdout);
    assert!(
        second_stdout.contains("Cached: yes"),
        "expected `Cached: yes` in submit stdout:\n{second_stdout}"
    );
    assert_eq!(
        counter_lines(&counter_path),
        1,
        "cached submit must not execute on worker"
    );
    assert_eq!(
        std::fs::read(out2_dir.join("out.bin")).unwrap(),
        b"cache-payload-v1\n"
    );

    let second_id = second_stdout
        .lines()
        .find_map(|line| {
            line.strip_prefix("Task ")
                .and_then(|rest| rest.split_whitespace().next())
        })
        .expect("task id line in submit stdout");
    let res_cmd = cli_cmd(home)
        .args(["result", "--master", &master_addr, second_id])
        .output()
        .expect("run oxideswarm result");
    assert!(res_cmd.status.success());
    let res_stdout = String::from_utf8_lossy(&res_cmd.stdout);
    assert!(
        res_stdout.contains("Cached: yes"),
        "expected `Cached: yes` in `oxideswarm result`:\n{res_stdout}"
    );
    let stats = master.action_cache().unwrap().stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.entries, 1);

    // Scenario 2: Mutating input content misses cache and executes updated payload on worker.
    std::fs::write(&input_path, b"cache-payload-v2\n").unwrap();
    let mutated = submit_cached_json(
        home,
        &master_addr,
        &input_path,
        &home.join("out3"),
        &counter_path,
        0,
    );
    assert_eq!(mutated["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 2);
    assert_eq!(
        std::fs::read(home.join("out3").join("out.bin")).unwrap(),
        b"cache-payload-v2\n"
    );

    // Scenario 3: Failed task (`exit 7`) is never cached; second run executes on worker again.
    let fail_1 = submit_cached_json(
        home,
        &master_addr,
        &input_path,
        &home.join("out4"),
        &counter_path,
        7,
    );
    assert_eq!(fail_1["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 3);

    let fail_2 = submit_cached_json(
        home,
        &master_addr,
        &input_path,
        &home.join("out5"),
        &counter_path,
        7,
    );
    assert_eq!(fail_2["cached"], Value::Bool(false));
    assert_eq!(
        counter_lines(&counter_path),
        4,
        "failed task must never populate action cache"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_2b_env_var_and_os_constraint_changes_miss_cache() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) =
        spawn_cluster(64 * 1024 * 1024, None).await;
    let home = temp.path();
    let input_path = home.join("dim_input.bin");
    let counter_path = home.join("dim_counter.txt");
    let payload = b"cache-dimensions-v1\n";
    std::fs::write(&input_path, payload).unwrap();

    let host_os = std::env::consts::OS;
    let cases: &[(&str, &[&str], bool, usize)] = &[
        ("d_out1", &["--env", "CACHE_DIM=one"], false, 1),
        ("d_out2", &["--env", "CACHE_DIM=one"], true, 1),
        ("d_out3", &["--env", "CACHE_DIM=two"], false, 2),
        (
            "d_out4",
            &["--env", "CACHE_DIM=two", "--os", host_os],
            false,
            3,
        ),
        (
            "d_out5",
            &["--env", "CACHE_DIM=two", "--os", host_os],
            true,
            3,
        ),
    ];
    for &(out_dir, extra_args, expected_cached, expected_runs) in cases {
        let out = home.join(out_dir);
        let resp = submit_cached_json_with_args(
            home,
            &master_addr,
            &input_path,
            &out,
            &counter_path,
            0,
            extra_args,
        );
        assert_eq!(
            resp["cached"],
            Value::Bool(expected_cached),
            "unexpected cached flag for {out_dir} ({extra_args:?}): {resp}"
        );
        assert_eq!(
            counter_lines(&counter_path),
            expected_runs,
            "unexpected worker execution count after {out_dir} ({extra_args:?})"
        );
        assert_eq!(std::fs::read(out.join("out.bin")).unwrap(), payload);
    }

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_4_and_5_cli_validation_rejections() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let input_path = home.join("in.txt");
    std::fs::write(&input_path, b"hello").unwrap();
    let input_arg = format!("{}:in.txt", input_path.display());

    // Scenario 4a: `submit --cache` without declared inputs is rejected upfront.
    let no_inputs = cli_cmd(home)
        .args(["submit", "--cache", "--wait", "--", "echo", "nope"])
        .output()
        .expect("run submit --cache without inputs");
    assert!(!no_inputs.status.success());
    let no_inputs_err = String::from_utf8_lossy(&no_inputs.stderr);
    assert!(
        no_inputs_err.contains("requires at least one declared input"),
        "unexpected stderr: {no_inputs_err}"
    );

    // Scenario 4b: `submit --cache --workspace ws --no-sync` is rejected upfront.
    let no_sync = cli_cmd(home)
        .args([
            "submit",
            "--cache",
            "--workspace",
            "ws",
            "--no-sync",
            "--wait",
            "--",
            "echo",
            "nope",
        ])
        .output()
        .expect("run submit --cache --no-sync");
    assert!(!no_sync.status.success());
    let no_sync_err = String::from_utf8_lossy(&no_sync.stderr);
    assert!(
        no_sync_err.contains("unsynced persistent workspace"),
        "unexpected stderr: {no_sync_err}"
    );

    // Scenario 5: `submit --cache --verify 1` is rejected upfront.
    let cache_verify = cli_cmd(home)
        .args([
            "submit", "--cache", "--input", &input_arg, "--output", "out.txt", "--verify", "1",
            "--", "echo", "nope",
        ])
        .output()
        .expect("run submit --cache --verify");
    assert!(!cache_verify.status.success());
    let verify_err = String::from_utf8_lossy(&cache_verify.stderr);
    assert!(
        verify_err.contains("--cache cannot be combined with --verify"),
        "unexpected stderr: {verify_err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_6_lru_eviction_unpins_and_misses_oldest() {
    // Cap action cache at 1 MiB (`1024 * 1024` bytes) while keeping blob store roomy (`64 MiB`).
    let (master, master_addr, shutdown_tx, worker_handle, temp) =
        spawn_cluster(1024 * 1024, None).await;
    let home = temp.path();
    let counter_path = home.join("evict_counter.txt");

    let blob_a = home.join("a.bin");
    let blob_b = home.join("b.bin");
    std::fs::write(&blob_a, vec![b'A'; 700 * 1024]).unwrap();
    std::fs::write(&blob_b, vec![b'B'; 700 * 1024]).unwrap();

    // 1. Insert 700 KiB entry A.
    let res_a1 = submit_cached_json(
        home,
        &master_addr,
        &blob_a,
        &home.join("out_a1"),
        &counter_path,
        0,
    );
    assert_eq!(res_a1["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 1);

    // 2. Insert 700 KiB entry B -> 1400 KiB > 1 MiB, so A is evicted.
    let res_b1 = submit_cached_json(
        home,
        &master_addr,
        &blob_b,
        &home.join("out_b1"),
        &counter_path,
        0,
    );
    assert_eq!(res_b1["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 2);
    assert_eq!(master.action_cache().unwrap().stats().entries, 1);

    // 3. Re-submit B -> still cached.
    let res_b2 = submit_cached_json(
        home,
        &master_addr,
        &blob_b,
        &home.join("out_b2"),
        &counter_path,
        0,
    );
    assert_eq!(res_b2["cached"], Value::Bool(true));
    assert_eq!(counter_lines(&counter_path), 2);

    // 4. Re-submit A -> evicted earlier, so must miss and re-execute on worker.
    let res_a2 = submit_cached_json(
        home,
        &master_addr,
        &blob_a,
        &home.join("out_a2"),
        &counter_path,
        0,
    );
    assert_eq!(res_a2["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 3);

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_7_persists_across_master_restart_with_state_dir() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let state_dir = home.join("master_state");
    let input_path = home.join("persist.bin");
    let counter_path = home.join("persist_counter.txt");
    std::fs::write(&input_path, b"survive-restart\n").unwrap();

    // Run master #1 + worker to populate cache on disk.
    {
        let (master1, addr1, shutdown_tx, worker_handle, _worker_tmp) =
            spawn_cluster(8 * 1024 * 1024, Some(&state_dir)).await;
        let first = submit_cached_json(
            home,
            &addr1,
            &input_path,
            &home.join("p_out1"),
            &counter_path,
            0,
        );
        assert_eq!(first["cached"], Value::Bool(false));
        assert_eq!(counter_lines(&counter_path), 1);
        assert!(
            state_dir.join(ACTION_CACHE_FILE).exists(),
            "state_dir must contain persisted action_cache.json"
        );

        master1.shutdown().expect("shutdown master1");
        let _ = shutdown_tx.send(true);
        worker_handle.abort();
    }

    // Wait briefly for master1 TaskStore compaction/lock release before spawning master2.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !state_dir.join("snapshot.bin").exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let lock_deadline = Instant::now() + Duration::from_secs(5);
    let master2 = loop {
        let cfg2 = ServerConfig::from_addr("127.0.0.1:0")
            .unwrap()
            .with_web_ui(false)
            .with_discovery_port(0)
            .with_state_dir(&state_dir)
            .with_session_resume_hold(Duration::ZERO)
            .with_action_cache_max_bytes(8 * 1024 * 1024);
        match MasterServer::spawn(cfg2).await {
            Ok(m) => break m,
            Err(_) if Instant::now() < lock_deadline => {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            Err(e) => panic!("failed to restart master2 on state_dir: {e}"),
        }
    };
    let addr2 = master2.server_addr().to_string();

    // Master #2 has NO workers at all: identical `--cache` submit must still hit immediately.
    let after_restart = submit_cached_json(
        home,
        &addr2,
        &input_path,
        &home.join("p_out2"),
        &counter_path,
        0,
    );
    assert_eq!(after_restart["cached"], Value::Bool(true));
    assert_eq!(counter_lines(&counter_path), 1);
    assert_eq!(
        std::fs::read(home.join("p_out2").join("out.bin")).unwrap(),
        b"survive-restart\n"
    );

    let _ = master2.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_8_action_cache_zero_bytes_disables_caching() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) = spawn_cluster(0, None).await;
    assert!(
        master.action_cache().is_none(),
        "action_cache_max_bytes = 0 must disable master ActionCache"
    );
    let home = temp.path();
    let input_path = home.join("disabled.bin");
    let counter_path = home.join("disabled_counter.txt");
    std::fs::write(&input_path, b"no-cache\n").unwrap();

    let first = submit_cached_json(
        home,
        &master_addr,
        &input_path,
        &home.join("d_out1"),
        &counter_path,
        0,
    );
    let second = submit_cached_json(
        home,
        &master_addr,
        &input_path,
        &home.join("d_out2"),
        &counter_path,
        0,
    );
    assert_eq!(first["cached"], Value::Bool(false));
    assert_eq!(second["cached"], Value::Bool(false));
    assert_eq!(counter_lines(&counter_path), 2);

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}

struct McpChild {
    child: Child,
    stdout: BufReader<std::process::ChildStdout>,
}

impl McpChild {
    fn spawn(home: &Path, master_addr: &str) -> Self {
        let mut child = cli_cmd(home)
            .args(["mcp", "--master", master_addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn oxideswarm mcp");
        let stdout = BufReader::new(child.stdout.take().expect("mcp stdout"));
        Self { child, stdout }
    }

    fn send(&mut self, msg: &Value) {
        let stdin = self.child.stdin.as_mut().expect("mcp stdin");
        let line = serde_json::to_string(msg).unwrap();
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
    }

    fn recv(&mut self) -> Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read mcp line");
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("invalid JSON from MCP: {e}\n{line}"))
    }
}

impl Drop for McpChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_9_mcp_submit_cache_hit_and_verify_conflict() {
    let (master, master_addr, shutdown_tx, worker_handle, temp) =
        spawn_cluster(64 * 1024 * 1024, None).await;
    let home = temp.path();
    std::fs::write(home.join("mcp_in.txt"), b"from-mcp\n").unwrap();

    let mut mcp = McpChild::spawn(home, &master_addr);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "action-cache-test", "version": "1.0" }
        }
    }));
    let init = mcp.recv();
    assert_eq!(init["id"], 1);
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }));

    #[cfg(unix)]
    let (prog, prog_args) = ("cat", json!(["mcp_in.txt"]));
    #[cfg(windows)]
    let (prog, prog_args) = ("cmd", json!(["/C", "type mcp_in.txt"]));

    // 1st MCP call with cache: true -> miss (`cached: false`).
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": prog,
                "args": prog_args,
                "files": ["mcp_in.txt:mcp_in.txt"],
                "cache": true,
                "wait": true
            }
        }
    }));
    let r1 = mcp.recv();
    let r1_body: Value =
        serde_json::from_str(r1["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(r1_body["cached"], Value::Bool(false), "r1={r1_body}");
    assert_eq!(r1_body["stdout"], "from-mcp\n");

    // 2nd identical MCP call with cache: true -> hit (`cached: true`).
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": prog,
                "args": prog_args,
                "files": ["mcp_in.txt:mcp_in.txt"],
                "cache": true,
                "wait": true
            }
        }
    }));
    let r2 = mcp.recv();
    let r2_body: Value =
        serde_json::from_str(r2["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(r2_body["cached"], Value::Bool(true), "r2={r2_body}");
    assert_eq!(r2_body["stdout"], "from-mcp\n");
    assert_eq!(master.action_cache().unwrap().stats().hits, 1);

    // Combining cache + verify in MCP `submit` must be rejected.
    mcp.send(&json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "submit",
            "arguments": {
                "type": "command",
                "command": prog,
                "args": prog_args,
                "files": ["mcp_in.txt:mcp_in.txt"],
                "outputs": ["out.bin"],
                "cache": true,
                "verify": 1
            }
        }
    }));
    let bad = mcp.recv();
    assert!(
        bad.get("error").is_some() || bad["result"]["isError"] == Value::Bool(true),
        "expected error when combining cache and verify in MCP submit: {bad}"
    );

    let _ = shutdown_tx.send(true);
    worker_handle.abort();
    let _ = master.shutdown();
}
