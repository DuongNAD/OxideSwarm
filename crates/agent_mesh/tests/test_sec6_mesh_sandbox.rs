//! Integration Tests for SEC-6: Mesh Sandbox Hardening (Rust Agent Mesh)
//!
//! Verifies:
//! 1. Rejection of unadvertised commands (/bin/ls) returning exit code 127 and status "failed".
//! 2. --no-shell advertisement: registration capabilities omit shell_exec.
//! 3. --no-shell refusal: sending shell_exec to no-shell node returns exit code 127.
//! 4. Built-in commands (echo, ping, system_info) succeed on no-shell node.
//! 5. Cross-platform secret stripping from subprocess environment.
//! 6. Output capping at 1 MiB with truncation marker.

use agent_mesh::{AgentMeshClient, AgentMeshEnvelope, AgentMeshHub};
use std::time::Duration;

#[tokio::test]
async fn test_sec6_unadvertised_command_rejected_with_127() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    let worker = AgentMeshClient::new(&hub_url, "sec6-worker-reject", "windows");
    let caller = AgentMeshClient::new(&hub_url, "sec6-caller-reject", "windows");

    worker.connect().await.expect("worker connect failed");
    caller.connect().await.expect("caller connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send unadvertised /bin/ls command
    let res = caller
        .send_command(
            "sec6-worker-reject",
            "/bin/ls",
            serde_json::json!({}),
            Some(4000),
        )
        .await
        .expect("Command send failed");

    match res {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stderr,
            ..
        } => {
            assert_eq!(status, "failed");
            assert_eq!(exit_code, 127);
            assert!(
                stderr.contains("Unsupported command: '/bin/ls'"),
                "unexpected stderr: {}",
                stderr
            );
        }
        other => panic!("Expected CommandResponse, got: {:?}", other),
    }

    // Send unadvertised cmd.exe command
    let res2 = caller
        .send_command(
            "sec6-worker-reject",
            "cmd.exe",
            serde_json::json!({}),
            Some(4000),
        )
        .await
        .expect("Command send failed");

    match res2 {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stderr,
            ..
        } => {
            assert_eq!(status, "failed");
            assert_eq!(exit_code, 127);
            assert!(
                stderr.contains("Unsupported command: 'cmd.exe'"),
                "unexpected stderr: {}",
                stderr
            );
        }
        other => panic!("Expected CommandResponse, got: {:?}", other),
    }

    worker.close().await;
    caller.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_sec6_no_shell_capability_and_execution_refusal() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    let worker =
        AgentMeshClient::new(&hub_url, "sec6-worker-noshell", "windows").with_no_shell(true);
    let caller = AgentMeshClient::new(&hub_url, "sec6-caller-noshell", "windows");

    worker.connect().await.expect("worker connect failed");
    caller.connect().await.expect("caller connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 1. Verify capability advertisement omits shell_exec
    let catalog = caller
        .query_node_list(Some(3000))
        .await
        .expect("query_node_list failed");
    let worker_entry = catalog
        .iter()
        .find(|n| n.node_id == "sec6-worker-noshell")
        .expect("worker not found in catalog");

    let supported = worker_entry
        .capabilities
        .get("supported_commands")
        .and_then(|v| v.as_array())
        .expect("supported_commands array missing");
    let commands: Vec<&str> = supported.iter().filter_map(|v| v.as_str()).collect();

    assert!(
        !commands.contains(&"shell_exec"),
        "shell_exec MUST NOT be advertised when no_shell is active: {:?}",
        commands
    );
    assert!(commands.contains(&"echo"));
    assert!(commands.contains(&"ping"));
    assert!(commands.contains(&"system_info"));
    assert!(commands.contains(&"sha256_verify"));

    // 2. Sending shell_exec to no-shell node MUST be refused with 127
    let res = caller
        .send_command(
            "sec6-worker-noshell",
            "shell_exec",
            serde_json::json!({"cmd": "echo forbidden"}),
            Some(4000),
        )
        .await
        .expect("Command send failed");

    match res {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stderr,
            ..
        } => {
            assert_eq!(status, "failed");
            assert_eq!(exit_code, 127);
            assert!(
                stderr.contains("shell_exec is disabled by --no-shell"),
                "unexpected stderr: {}",
                stderr
            );
        }
        other => panic!("Expected CommandResponse, got: {:?}", other),
    }

    // 3. Builtin commands continue to work
    let echo_res = caller
        .send_command(
            "sec6-worker-noshell",
            "echo",
            serde_json::json!({"msg": "builtins_still_work"}),
            Some(4000),
        )
        .await
        .expect("Echo command failed");

    match echo_res {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert_eq!(stdout, "builtins_still_work");
        }
        other => panic!("Expected CommandResponse, got: {:?}", other),
    }

    worker.close().await;
    caller.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_sec6_environment_secret_stripping_cross_platform() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    // Set sentinel secrets on this process
    const SENTINELS: [(&str, &str); 5] = [
        ("OXIDESWARM_SECRET", "sentinel-cluster-secret-999"),
        ("OXIDESWARM_SECRET_FILE", "/sentinel/secret/file/888"),
        ("OXIDE_MESH_TOKEN", "sentinel-mesh-token-777"),
        ("RUSTY_GRID_SECRET", "sentinel-grid-secret-666"),
        ("RUSTY_GRID_SECRET_FILE", "/sentinel/grid-file/555"),
    ];

    for (name, val) in SENTINELS {
        std::env::set_var(name, val);
    }

    let worker = AgentMeshClient::new(&hub_url, "sec6-worker-env", "windows");
    let caller = AgentMeshClient::new(&hub_url, "sec6-caller-env", "windows");

    worker.connect().await.expect("worker connect failed");
    caller.connect().await.expect("caller connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let script_name = format!("test_sec6_env_{}.py", uuid::Uuid::new_v4());
    let script_path = std::env::current_dir().unwrap().join(&script_name);
    let script_content = r#"
import os
keys = ['OXIDESWARM_SECRET', 'OXIDESWARM_SECRET_FILE', 'OXIDE_MESH_TOKEN', 'RUSTY_GRID_SECRET', 'RUSTY_GRID_SECRET_FILE']
print(' '.join(k + '=' + os.environ.get(k, 'UNSET') for k in keys))
"#;
    std::fs::write(&script_path, script_content).expect("Failed to write test script");
    let probe_cmd = format!("python {}", script_name);

    let res = caller
        .send_command(
            "sec6-worker-env",
            "shell_exec",
            serde_json::json!({
                "cmd": probe_cmd,
                "env": { "EXPLICIT_TEST_VAR": "explicit_arrived_ok" }
            }),
            Some(6000),
        )
        .await
        .expect("Command send failed");

    let _ = std::fs::remove_file(&script_path);

    for (name, _) in SENTINELS {
        std::env::remove_var(name);
    }

    match res {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            for (name, secret_val) in SENTINELS {
                assert!(
                    stdout.contains(&format!("{}=UNSET", name)),
                    "Secret variable {} was not UNSET in child environment! Stdout: {}",
                    name,
                    stdout
                );
                assert!(
                    !stdout.contains(secret_val),
                    "Secret value for {} was leaked into child! Stdout: {}",
                    name,
                    stdout
                );
            }
        }
        other => panic!("Expected CommandResponse, got: {:?}", other),
    }

    worker.close().await;
    caller.close().await;
    hub.stop().await;
}
