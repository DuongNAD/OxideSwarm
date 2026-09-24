//! Adversarial empirical challenge tests for OxideSwarm Agent Mesh
//! Tests command execution safety, timeout process termination,
//! non-zero exit codes, stderr capture, and high-throughput bidirectional data bursts.

use agent_mesh::{
    compute_sha256, verify_sha256, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub,
    CommandExecutor,
};
use std::fs;
use std::time::Duration;
use sysinfo::{Pid, System};

#[tokio::test]
async fn test_rust_executor_non_zero_exit_and_stderr() {
    let script_path = std::env::current_dir().unwrap().join(format!("test_fail_{}.py", uuid::Uuid::new_v4()));
    let script_content = r#"
import sys
sys.stdout.write("stdout_marker_123\n")
sys.stdout.flush()
sys.stderr.write("stderr_error_fatal_456\n")
sys.stderr.flush()
sys.exit(42)
"#;
    fs::write(&script_path, script_content).expect("Failed to write test script");

    let cmd_str = format!("python {}", script_path.file_name().unwrap().to_str().unwrap());
    let res = CommandExecutor::execute(
        "shell_exec",
        &serde_json::json!({"cmd": cmd_str}),
        Some(5000),
    )
    .await;

    let _ = fs::remove_file(&script_path);

    println!("Rust Executor Non-Zero Exit Result: {:?}", res);
    assert_eq!(res.status, "failed");
    assert_eq!(res.exit_code, 42);
    assert!(res.stderr.contains("stderr_error_fatal_456"));
    assert!(res.stdout.contains("stdout_marker_123"));
}

#[tokio::test]
async fn test_rust_executor_timeout_process_survival() {
    let pid_file = std::env::current_dir().unwrap().join(format!("timeout_pid_{}.txt", uuid::Uuid::new_v4()));
    let script_path = std::env::current_dir().unwrap().join(format!("test_hang_{}.py", uuid::Uuid::new_v4()));

    let script_content = format!(
        r#"
import os, sys, time
pid = os.getpid()
with open("{}", "w") as f:
    f.write(str(pid))
sys.stdout.write("started_pid_{}\n")
sys.stdout.flush()
time.sleep(10)
"#,
        pid_file.file_name().unwrap().to_str().unwrap(),
        "marker"
    );
    fs::write(&script_path, script_content).expect("Failed to write test script");

    let cmd_str = format!("python {}", script_path.file_name().unwrap().to_str().unwrap());
    println!("Executing command with timeout 1000ms: {}", cmd_str);

    // Timeout of 1000 ms on a 10-second sleep
    let res = CommandExecutor::execute(
        "shell_exec",
        &serde_json::json!({"cmd": cmd_str}),
        Some(1000),
    )
    .await;

    println!("Timeout Result: {:?}", res);
    assert_eq!(res.status, "timeout");
    assert_eq!(res.exit_code, 124);

    // Read the child PID that was spawned
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut child_pid_opt: Option<u32> = None;
    if pid_file.exists() {
        if let Ok(content) = fs::read_to_string(&pid_file) {
            if let Ok(pid) = content.trim().parse::<u32>() {
                child_pid_opt = Some(pid);
            }
        }
    }

    if let Some(pid) = child_pid_opt {
        println!("Checking if child PID {} is still running...", pid);
        let mut sys = System::new_all();
        sys.refresh_all();
        let is_running = sys.process(Pid::from(pid as usize)).is_some();

        if is_running {
            println!(
                "CRITICAL DEFECT CONFIRMED: Subprocess PID {} (python.exe) is STILL RUNNING after CommandExecutor timeout!",
                pid
            );
            // Clean up the hanging process
            if let Some(proc) = sys.process(Pid::from(pid as usize)) {
                proc.kill();
            }
        } else {
            println!("SUCCESS: Subprocess PID {} was cleanly killed!", pid);
        }

        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(&script_path);

        assert!(
            !is_running,
            "DEFECT FOUND: Subprocess with PID {} was NOT killed on timeout! It survived in OS.",
            pid
        );
    } else {
        println!("Warning: PID file was not written before timeout.");
        let _ = fs::remove_file(&script_path);
    }
}

#[tokio::test]
async fn test_rust_executor_direct_hung_command_kill() {
    #[cfg(target_os = "windows")]
    let (cmd, args) = (
        "powershell",
        serde_json::json!({
            "args": ["-NoProfile", "-Command", "Start-Sleep -Seconds 10"]
        }),
    );

    #[cfg(not(target_os = "windows"))]
    let (cmd, args) = ("sleep", serde_json::json!(["10"]));

    let t0 = std::time::Instant::now();
    let res = CommandExecutor::execute(cmd, &args, Some(1000)).await;
    let elapsed = t0.elapsed();

    println!("Direct Hung Command Timeout Result: {:?}", res);
    assert_eq!(res.status, "timeout");
    assert_eq!(res.exit_code, 124);
    assert!(res.stderr.contains("1000 ms"));
    assert!(elapsed.as_millis() < 4000, "Direct command was not terminated upon timeout! Took {:?}", elapsed);
}

#[tokio::test]
async fn test_rust_bidirectional_burst_stress() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    let node1 = AgentMeshClient::new(&hub_url, "rust-stress-win-1", "windows");
    let node3 = AgentMeshClient::new(&hub_url, "rust-stress-and-3", "android");

    node1.connect().await.expect("node1 connect");
    node3.connect().await.expect("node3 connect");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 1. Forward 64 KB
    let mut payload_64k = vec![0u8; 65536];
    for (i, b) in payload_64k.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }
    let expected_fwd_hash = compute_sha256(&payload_64k);

    let (_, fwd_hash) = node1.send_data_payload("rust-stress-and-3", &payload_64k).await.expect("send forward");
    assert_eq!(fwd_hash, expected_fwd_hash);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let rec_node3 = node3.received_data_payloads().await;
    assert_eq!(rec_node3.len(), 1);
    if let AgentMeshEnvelope::DataPayload { data, checksum_sha256, .. } = &rec_node3[0] {
        let rec_bytes: Vec<u8> = data.as_ref().unwrap().chars().map(|c| c as u8).collect();
        assert_eq!(rec_bytes.len(), 65536);
        assert!(verify_sha256(&rec_bytes, checksum_sha256.as_ref().unwrap()));
        assert_eq!(checksum_sha256.as_ref().unwrap(), &expected_fwd_hash);
    }

    // 2. Reverse 64 KB
    let mut payload_rev = vec![0u8; 65536];
    for (i, b) in payload_rev.iter_mut().enumerate() {
        *b = ((i + 137) % 256) as u8;
    }
    let expected_rev_hash = compute_sha256(&payload_rev);

    let (_, rev_hash) = node3.send_data_payload("rust-stress-win-1", &payload_rev).await.expect("send reverse");
    assert_eq!(rev_hash, expected_rev_hash);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let rec_node1 = node1.received_data_payloads().await;
    assert_eq!(rec_node1.len(), 1);
    if let AgentMeshEnvelope::DataPayload { data, checksum_sha256, .. } = &rec_node1[0] {
        let rec_bytes: Vec<u8> = data.as_ref().unwrap().chars().map(|c| c as u8).collect();
        assert_eq!(rec_bytes.len(), 65536);
        assert!(verify_sha256(&rec_bytes, checksum_sha256.as_ref().unwrap()));
        assert_eq!(checksum_sha256.as_ref().unwrap(), &expected_rev_hash);
    }

    // 3. High throughput burst: 100 packets (50 fwd, 50 rev)
    for i in 0..50 {
        let fwd_chunk = format!("burst-fwd-{}-{}", i, i * 7).into_bytes();
        let _ = node1.send_data_payload("rust-stress-and-3", &fwd_chunk).await;

        let rev_chunk = format!("burst-rev-{}-{}", i, i * 13).into_bytes();
        let _ = node3.send_data_payload("rust-stress-win-1", &rev_chunk).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let total_node3 = node3.received_data_payloads().await;
    let total_node1 = node1.received_data_payloads().await;

    assert_eq!(total_node3.len(), 1 + 50, "Forward burst packet loss!");
    assert_eq!(total_node1.len(), 1 + 50, "Reverse burst packet loss!");

    node1.close().await;
    node3.close().await;
    hub.stop().await;
}
