//! Real-binary CLI E2E tests for FEAT-5 (`oxideswarm worker` `SIGKILL` & `SIGTERM` drain).
//!
//! Uses isolated `TempDir` `HOME` + fake `pmset` on `PATH` + loopback `127.0.0.1:0` so host
//! config and battery state never interfere.

#![cfg(unix)]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE};
use rusty_grid_master::ReaperConfig;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn spawn(mut cmd: Command) -> Self {
        Self(Some(cmd.spawn().expect("spawn child")))
    }

    fn id(&self) -> u32 {
        self.0.as_ref().expect("child").id()
    }

    fn kill_and_wait(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn wait_exit(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let c = self.0.as_mut().expect("child");
        let start = Instant::now();
        loop {
            if let Some(status) = c.try_wait().expect("try_wait") {
                self.0 = None;
                return status;
            }
            assert!(
                start.elapsed() < timeout,
                "child did not exit within {timeout:?}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn setup_fake_pmset() -> TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let pmset = dir.path().join("pmset");
    std::fs::write(&pmset, "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\n").unwrap();
    std::fs::set_permissions(&pmset, std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

fn preserve_toolchain_env(cmd: &mut Command) {
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
}

fn clean_cmd(home: &Path, path_prefix: &Path) -> Command {
    let orig_path = std::env::var_os("PATH").unwrap_or_default();
    let mut joined = std::ffi::OsString::from(path_prefix.as_os_str());
    joined.push(":");
    joined.push(&orig_path);

    let mut cmd = Command::new(BIN);
    preserve_toolchain_env(&mut cmd);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env("PATH", joined)
        .env("RUSTY_GRID_NO_BATTERY", "1");
    for var in [
        "RUSTY_GRID_MASTER_ADDR",
        "RUSTY_GRID_MASTER",
        "OXIDESWARM_MASTER",
        "OXIDESWARM_SECRET",
        "OXIDESWARM_SECRET_FILE",
        "OXIDE_MESH_TOKEN",
    ] {
        cmd.env_remove(var);
    }
    cmd
}

fn spawn_worker(
    home: &Path,
    fake_bin: &Path,
    master_addr: &str,
    name: &str,
    sandbox_dir: &Path,
) -> ChildGuard {
    let mut cmd = clean_cmd(home, fake_bin);
    cmd.args([
        "worker",
        "--master",
        master_addr,
        "--name",
        name,
        "--cores",
        "2",
        "--ram-mb",
        "4096",
        "--no-gpu",
        "--no-blender",
        "--no-cuda",
        "--no-portal",
        "--sandbox-base-dir",
        &sandbox_dir.to_string_lossy(),
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    ChildGuard::spawn(cmd)
}

async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

#[tokio::test]
async fn test_feat5_cli_worker_sigkill_and_sigterm_drain_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_bin = setup_fake_pmset();
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let w1_sb = tmp.path().join("w1_sb");
    let w2_sb = tmp.path().join("w2_sb");
    let out_dir = tmp.path().join("collected_outputs");
    std::fs::create_dir_all(&out_dir).unwrap();
    let step1_count = tmp.path().join("step1.count");

    let blob_dir = tmp.path().join("master_blobs");
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_discovery_port(0)
            .with_max_retries(3)
            .with_blob_store(&blob_dir, 64 * 1024 * 1024),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let addr = master.server_addr().to_string();

    // 1. SIGKILL test via real `oxideswarm worker` + `oxideswarm submit --output '**/*'`
    let mut w1 = spawn_worker(&home, fake_bin.path(), &addr, "cli-w1", &w1_sb);
    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| !ws.is_empty())
                    .unwrap_or(false)
            }
        )
        .await,
        "cli-w1 must register"
    );

    let script1 = format!(
        "if [ -f \"$OXIDESWARM_CHECKPOINT_DIR/step1.ok\" ]; then echo \"RESUMED_OK\"; echo \"done\" > final.txt; exit 0; fi; echo 1 >> '{}'; echo \"cp_v1\" > \"$OXIDESWARM_CHECKPOINT_DIR/step1.ok\"; sleep 20",
        step1_count.display()
    );
    let addr_sub = addr.clone();
    let home_sub = home.clone();
    let fake_sub = fake_bin.path().to_path_buf();
    let out_sub = out_dir.clone();
    let submit_handle = tokio::task::spawn_blocking(move || {
        let mut cmd = clean_cmd(&home_sub, &fake_sub);
        cmd.args([
            "submit",
            "--master",
            &addr_sub,
            "--wait",
            "--output",
            "**/*",
            "--output-dir",
            &out_sub.to_string_lossy(),
            "--",
            "sh",
            "-c",
            &script1,
        ]);
        cmd.output().expect("run submit")
    });

    // Wait for periodic OXCP1 checkpoint to hit Master queue.
    let task_id = {
        let mut found = None;
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            let tasks = master.queue().list_tasks().await;
            if let Some(t) = tasks.first() {
                if master
                    .get_checkpoint(&t.task_id)
                    .await
                    .is_some_and(|cp| cp.delta_state.as_slice().starts_with(b"OXCP1\x01"))
                {
                    found = Some(t.task_id);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        found.expect("periodic OXCP1 checkpoint must reach Master before SIGKILL")
    };

    // Real SIGKILL on cli-w1.
    w1.kill_and_wait();

    // Start cli-w2 to resume and finish.
    let mut w2 = spawn_worker(&home, fake_bin.path(), &addr, "cli-w2", &w2_sb);
    let output = tokio::time::timeout(Duration::from_secs(20), submit_handle)
        .await
        .expect("submit must complete after failover")
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "submit failed: stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.contains("RESUMED_OK"), "stdout={stdout}");
    assert_eq!(
        std::fs::read_to_string(&step1_count)
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(out_dir.join("final.txt").is_file());
    assert!(!out_dir.join(".oxideswarm_checkpoint").exists());
    assert!(master.get_checkpoint(&task_id).await.is_some());

    // 2. Real SIGTERM graceful drain test on cli-w2.
    let started_flag = tmp.path().join("sigterm_started.flag");
    let script2 = format!(
        "if [ -f \"$OXIDESWARM_CHECKPOINT_DIR/term.ok\" ]; then cat \"$OXIDESWARM_CHECKPOINT_DIR/term.ok\"; echo \"TERM_RESUMED\"; exit 0; fi; trap 'echo \"sigterm_flushed\" > \"$OXIDESWARM_CHECKPOINT_DIR/term.ok\"; exit 0' TERM; echo 1 > '{}'; while :; do sleep 0.05; done",
        started_flag.display()
    );
    let addr_sub2 = addr.clone();
    let home_sub2 = home.clone();
    let fake_sub2 = fake_bin.path().to_path_buf();
    let submit2_handle = tokio::task::spawn_blocking(move || {
        let mut cmd = clean_cmd(&home_sub2, &fake_sub2);
        cmd.args([
            "submit", "--master", &addr_sub2, "--wait", "--", "sh", "-c", &script2,
        ]);
        cmd.output().expect("run submit 2")
    });

    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(25), || {
            let p = started_flag.clone();
            async move { p.exists() }
        })
        .await,
        "task 2 must start on cli-w2"
    );

    // Send real SIGTERM to cli-w2.
    let pid = w2.id().to_string();
    let kill_status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("send SIGTERM");
    assert!(kill_status.success());
    let exit_status = w2.wait_exit(Duration::from_secs(10));
    assert!(
        exit_status.success(),
        "worker SIGTERM exit: {exit_status:?}"
    );

    // Start cli-w3 to resume from the SIGTERM-flushed checkpoint.
    let w3_sb = tmp.path().join("w3_sb");
    let mut w3 = spawn_worker(&home, fake_bin.path(), &addr, "cli-w3", &w3_sb);
    let out2 = tokio::time::timeout(Duration::from_secs(20), submit2_handle)
        .await
        .expect("submit 2 must complete")
        .unwrap();
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    let stderr2 = String::from_utf8_lossy(&out2.stderr);
    assert!(
        out2.status.success(),
        "submit 2 failed: stdout={stdout2} stderr={stderr2}"
    );
    assert!(stdout2.contains("sigterm_flushed"), "stdout2={stdout2}");
    assert!(stdout2.contains("TERM_RESUMED"), "stdout2={stdout2}");

    w3.kill_and_wait();
    let _ = master.shutdown();
}

#[tokio::test]
async fn test_feat5_cli_mixed_case_and_dot_slash_output_excludes_checkpoint_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_bin = setup_fake_pmset();
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let w_sb = tmp.path().join("w_sb");
    std::fs::create_dir_all(&w_sb).unwrap();

    let isolation_arg = if rusty_grid_worker::verify_isolation_support(
        rusty_grid_worker::IsolationLevel::Fs,
        &w_sb,
    )
    .await
    .is_ok()
    {
        "fs"
    } else {
        "off"
    };

    let blob_dir = tmp.path().join("master_blobs");
    let master = MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_discovery_port(0)
            .with_blob_store(&blob_dir, 64 * 1024 * 1024),
        SchedulerConfig::default()
            .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
            .without_host_cpu_backpressure(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawn");
    let addr = master.server_addr().to_string();

    let mut w_cmd = clean_cmd(&home, fake_bin.path());
    w_cmd
        .args([
            "worker",
            "--master",
            &addr,
            "--name",
            "cli-case-w",
            "--cores",
            "2",
            "--ram-mb",
            "4096",
            "--no-gpu",
            "--no-blender",
            "--no-cuda",
            "--no-portal",
            "--isolation",
            isolation_arg,
            "--sandbox-base-dir",
            &w_sb.to_string_lossy(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut worker = ChildGuard::spawn(w_cmd);

    assert!(
        wait_for(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|ws| !ws.is_empty())
                    .unwrap_or(false)
            }
        )
        .await,
        "cli-case-w must register"
    );

    // 1. Mixed-case, Unicode case-folded, and `./`-normalized wildcards must NOT leak checkpoint files.
    let out_wild = tmp.path().join("out_wild");
    std::fs::create_dir_all(&out_wild).unwrap();
    let addr_1 = addr.clone();
    let home_1 = home.clone();
    let fake_1 = fake_bin.path().to_path_buf();
    let out_wild_1 = out_wild.clone();
    let res1 = tokio::task::spawn_blocking(move || {
        let mut cmd = clean_cmd(&home_1, &fake_1);
        cmd.args([
            "submit",
            "--master",
            &addr_1,
            "--wait",
            "--output",
            ".OXIDESWARM_CHECKPOINT/*",
            "--output",
            "./.Oxideswarm_Checkpoint/**",
            "--output",
            ".oxideswarm_chec\u{212a}point/*",
            "--output",
            ".oxide\u{017f}warm_checkpoint/*",
            "--output",
            "out.txt",
            "--output-dir",
            &out_wild_1.to_string_lossy(),
            "--",
            "sh",
            "-c",
            "echo TOP_SECRET > \"$OXIDESWARM_CHECKPOINT_DIR/secret_cp.bin\"; mkdir -p \"$OXIDESWARM_CHECKPOINT_DIR/sub\"; echo NESTED > \"$OXIDESWARM_CHECKPOINT_DIR/sub/nested.bin\"; echo ok > out.txt",
        ]);
        cmd.output().expect("run submit 1")
    })
    .await
    .unwrap();
    let stdout1 = String::from_utf8_lossy(&res1.stdout);
    let stderr1 = String::from_utf8_lossy(&res1.stderr);
    assert!(
        res1.status.success(),
        "wildcard submit failed: stdout={stdout1} stderr={stderr1}"
    );
    assert!(out_wild.join("out.txt").is_file());
    assert_eq!(
        std::fs::read_dir(&out_wild).unwrap().count(),
        1,
        "only out.txt may be collected into {out_wild:?}"
    );

    // 2. Literal mixed-case, Unicode case-folded, & `./`-normalized `--output` targeting checkpoint dir must fail.
    for literal in [
        ".OXIDESWARM_CHECKPOINT/secret_cp.bin",
        "./.Oxideswarm_Checkpoint/secret_cp.bin",
        ".oxideswarm_chec\u{212a}point/secret_cp.bin",
        "./.oxideswarm_chec\u{212a}point/secret_cp.bin",
        ".oxide\u{017f}warm_checkpoint/secret_cp.bin",
    ] {
        let addr_2 = addr.clone();
        let home_2 = home.clone();
        let fake_2 = fake_bin.path().to_path_buf();
        let lit = literal.to_string();
        let res2 = tokio::task::spawn_blocking(move || {
            let mut cmd = clean_cmd(&home_2, &fake_2);
            cmd.args([
                "submit",
                "--master",
                &addr_2,
                "--wait",
                "--output",
                &lit,
                "--",
                "sh",
                "-c",
                "echo TOP_SECRET > \"$OXIDESWARM_CHECKPOINT_DIR/secret_cp.bin\"",
            ]);
            cmd.output().expect("run submit 2")
        })
        .await
        .unwrap();
        let stdout2 = String::from_utf8_lossy(&res2.stdout);
        let stderr2 = String::from_utf8_lossy(&res2.stderr);
        assert!(
            !res2.status.success(),
            "literal '{literal}' must fail, got rc=0 stdout={stdout2} stderr={stderr2}"
        );
        let combined = format!("{stdout2}\n{stderr2}");
        assert!(
            combined.contains("reserved checkpoint directory"),
            "literal '{literal}' must mention reserved checkpoint directory, got: {combined}"
        );
    }

    worker.kill_and_wait();
    let _ = master.shutdown();
}
