//! Adversarial empirical challenge tests for OxideSwarm Agent Mesh (Features F20 & F21)
//! Author: Challenger M4.2 (Empirical Challenger)
//!
//! Stress-tests:
//! 1. Cryptographic SHA-256 Tamper Detection:
//!    - 1-bit flip at byte 0, middle byte, and final byte
//!    - Altered checksums and corrupted base64 payloads
//!    - Verification strictly rejects tampered payloads in Rust AgentMeshClient
//! 2. Binary Data Parser Fuzzing in AgentGridBridge:
//!    - Hex strings (0x..., odd-length, uppercase, invalid chars)
//!    - Raw JSON arrays (valid bytes, negative, out-of-range, mixed)
//!    - Malformed Base64, whitespace, nulls, emojis, and non-existent paths
//!    - Asserts 100% panic-free resilience across all adversarial inputs
//! 3. High-Concurrency Agent Mesh Burst:
//!    - 10 concurrent agents submitting 3D generation and auto-rig tasks simultaneously
//!    - Exact correlation_id routing verification across all concurrent channels
//!    - WebSocket DeliveryAck ("target_forwarded") and CommandResponse sequencing

use agent_mesh::bridge::AgentGridBridge;
use agent_mesh::{
    compute_sha256, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub,
};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use uuid::Uuid;

fn create_minimal_valid_glb() -> Vec<u8> {
    let json_bytes = br#"{"asset":{"version":"2.0"},"scenes":[{"nodes":[0]}],"nodes":[{"name":"RootNode"}]}"#;
    let pad = (4 - (json_bytes.len() % 4)) % 4;
    let mut padded_json = json_bytes.to_vec();
    padded_json.extend(vec![b' '; pad]);

    let chunk0_len = padded_json.len() as u32;
    let total_len = 12 + 8 + chunk0_len;

    let mut glb = Vec::new();
    glb.extend_from_slice(b"glTF");
    glb.extend_from_slice(&2u32.to_le_bytes());
    glb.extend_from_slice(&total_len.to_le_bytes());
    glb.extend_from_slice(&chunk0_len.to_le_bytes());
    glb.extend_from_slice(&0x4E4F534Au32.to_le_bytes());
    glb.extend_from_slice(&padded_json);
    glb
}

fn create_humanoid_obj() -> Vec<u8> {
    let mut s = String::new();
    s.push_str("# Humanoid OBJ\n");
    let boxes = [
        (-0.20f32, 0.50f32, 0.0f32, 0.08f32, 0.50f32, 0.08f32),
        (0.20, 0.50, 0.0, 0.08, 0.50, 0.08),
        (0.0, 1.30, 0.0, 0.22, 0.30, 0.12),
        (-0.38, 1.35, 0.0, 0.14, 0.06, 0.06),
        (0.38, 1.35, 0.0, 0.14, 0.06, 0.06),
        (0.0, 1.80, 0.0, 0.12, 0.20, 0.12),
    ];
    let mut base = 1usize;
    for (cx, cy, cz, hx, hy, hz) in boxes {
        for &dx in &[-hx, hx] {
            for &dy in &[-hy, hy] {
                for &dz in &[-hz, hz] {
                    s.push_str(&format!("v {:.4} {:.4} {:.4}\n", cx + dx, cy + dy, cz + dz));
                }
            }
        }
        let faces = [
            (base, base + 2, base + 1),
            (base + 1, base + 2, base + 3),
            (base + 4, base + 5, base + 6),
            (base + 5, base + 7, base + 6),
            (base, base + 1, base + 4),
            (base + 1, base + 5, base + 4),
            (base + 2, base + 6, base + 3),
            (base + 3, base + 6, base + 7),
            (base, base + 4, base + 2),
            (base + 2, base + 4, base + 6),
            (base + 1, base + 3, base + 5),
            (base + 3, base + 7, base + 5),
        ];
        for f in faces {
            s.push_str(&format!("f {} {} {}\n", f.0, f.1, f.2));
        }
        base += 8;
    }
    s.into_bytes()
}

fn make_command_response(
    status: &str,
    exit_code: i32,
    payload: Option<serde_json::Value>,
    corr_id: &str,
) -> AgentMeshEnvelope {
    AgentMeshEnvelope::CommandResponse {
        version: "1.0".into(),
        correlation_id: corr_id.into(),
        from: "grid".into(),
        to: "agent-1".into(),
        command: Some("grid_3d_generate".into()),
        status: status.into(),
        exit_code,
        stdout: String::new(),
        stderr: String::new(),
        execution_duration_ms: 42,
        payload,
        error: None,
        timestamp: 1000,
    }
}

// =========================================================================
// 1. Cryptographic SHA-256 Tamper Detection Suite
// =========================================================================

#[test]
fn test_adv_sha256_single_bit_flip_tampering_rust() {
    let original_bytes = create_minimal_valid_glb();
    let original_sha = compute_sha256(&original_bytes);
    let original_b64 = STANDARD.encode(&original_bytes);

    // Untampered baseline check
    let valid_envelope = make_command_response(
        "success",
        0,
        Some(serde_json::json!({
            "asset_sha256": original_sha,
            "asset_base64": original_b64,
            "asset_bytes_len": original_bytes.len(),
        })),
        "corr-base",
    );

    let extracted = AgentMeshClient::extract_asset_bytes(&valid_envelope)
        .expect("Untampered asset extraction must succeed");
    assert_eq!(extracted, original_bytes);

    // Attack 1: Single bit flip at byte 0 (LSB)
    let mut tampered_0 = original_bytes.clone();
    tampered_0[0] ^= 0x01;
    let b64_tampered_0 = STANDARD.encode(&tampered_0);
    let env_tampered_0 = make_command_response(
        "success",
        0,
        Some(serde_json::json!({
            "asset_sha256": original_sha,
            "asset_base64": b64_tampered_0,
        })),
        "corr-attack-0",
    );
    let res_0 = AgentMeshClient::extract_asset_bytes(&env_tampered_0);
    assert!(res_0.is_err(), "Single bit flip at byte 0 must fail SHA-256");
    assert!(res_0.unwrap_err().to_string().contains("Asset SHA-256 verification failed"));

    // Attack 2: Single bit flip at middle byte
    let mid_idx = original_bytes.len() / 2;
    let mut tampered_mid = original_bytes.clone();
    tampered_mid[mid_idx] ^= 0x10;
    let b64_tampered_mid = STANDARD.encode(&tampered_mid);
    let env_tampered_mid = make_command_response(
        "success",
        0,
        Some(serde_json::json!({
            "asset_sha256": original_sha,
            "asset_base64": b64_tampered_mid,
        })),
        "corr-attack-mid",
    );
    let res_mid = AgentMeshClient::extract_asset_bytes(&env_tampered_mid);
    assert!(res_mid.is_err(), "Single bit flip at middle byte must fail SHA-256");
    assert!(res_mid.unwrap_err().to_string().contains("Asset SHA-256 verification failed"));

    // Attack 3: Single bit flip at final byte (MSB)
    let last_idx = original_bytes.len() - 1;
    let mut tampered_last = original_bytes.clone();
    tampered_last[last_idx] ^= 0x80;
    let b64_tampered_last = STANDARD.encode(&tampered_last);
    let env_tampered_last = make_command_response(
        "success",
        0,
        Some(serde_json::json!({
            "asset_sha256": original_sha,
            "asset_base64": b64_tampered_last,
        })),
        "corr-attack-last",
    );
    let res_last = AgentMeshClient::extract_asset_bytes(&env_tampered_last);
    assert!(res_last.is_err(), "Single bit flip at last byte must fail SHA-256");
    assert!(res_last.unwrap_err().to_string().contains("Asset SHA-256 verification failed"));

    // Attack 4: Altered SHA-256 checksum string
    let mut altered_sha = original_sha.clone();
    altered_sha.replace_range(0..1, if altered_sha.starts_with('a') { "b" } else { "a" });
    let env_altered_sha = make_command_response(
        "success",
        0,
        Some(serde_json::json!({
            "asset_sha256": altered_sha,
            "asset_base64": original_b64,
        })),
        "corr-attack-sha",
    );
    let res_sha = AgentMeshClient::extract_asset_bytes(&env_altered_sha);
    assert!(res_sha.is_err(), "Altered SHA-256 string must be rejected");

    // Attack 5: Corrupted non-base64 characters in asset_base64
    let env_corrupt_b64 = make_command_response(
        "success",
        0,
        Some(serde_json::json!({
            "asset_sha256": original_sha,
            "asset_base64": "!@#$%^&*()_not_base64!",
        })),
        "corr-corrupt-b64",
    );
    let res_corrupt = AgentMeshClient::extract_asset_bytes(&env_corrupt_b64);
    assert!(res_corrupt.is_err(), "Corrupted base64 must return error");

    // Attack 6: Response status is "failed"
    let env_failed = make_command_response(
        "failed",
        1,
        Some(serde_json::json!({
            "asset_sha256": original_sha,
            "asset_base64": original_b64,
        })),
        "corr-failed",
    );
    let res_failed = AgentMeshClient::extract_asset_bytes(&env_failed);
    assert!(res_failed.is_err(), "Status 'failed' must be rejected");
}

// =========================================================================
// 2. Binary Data Parser Fuzzing Suite (AgentGridBridge)
// =========================================================================

#[tokio::test]
async fn test_adv_bridge_binary_data_parser_fuzzing() {
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer spawn");
    let bridge = AgentGridBridge::new(master.clone());

    let test_temp = TempDir::new().unwrap();
    let existing_file = test_temp.path().join("sample_asset.glb");
    std::fs::write(&existing_file, create_minimal_valid_glb()).unwrap();

    let adversarial_inputs: Vec<serde_json::Value> = vec![
        serde_json::json!("0x12345678"),
        serde_json::json!("0x12345"),      // odd-length hex
        serde_json::json!("0XDEADBEEF"),   // uppercase hex prefix
        serde_json::json!("0xZZZZ"),       // invalid hex characters
        serde_json::json!("0x"),           // empty hex prefix
        serde_json::json!("0X"),
        serde_json::json!("aGVsbG8gd29ybGQ="), // valid base64
        serde_json::json!("!@#$%^&*()_+~`"),  // invalid base64 string
        serde_json::json!("aGVsbG8==="),      // corrupted padding
        serde_json::json!([0, 1, 2, 127, 255]), // raw byte array
        serde_json::json!([-5, 300, 1000000]), // negative & overflow values
        serde_json::json!(["string", "in", "array"]),
        serde_json::json!([]),                // empty array
        serde_json::json!(""),                // empty string
        serde_json::json!("   \r\n\t  "),     // whitespace
        serde_json::json!("null\0byte\0str"), // embedded null
        serde_json::json!("🤖📦🔥✨🚀"),       // unicode / emoji string
        serde_json::json!("non_existent_file_98765.glb"), // non-existent file
        serde_json::json!(existing_file.to_string_lossy().to_string()), // existing file path
        serde_json::json!(123456),             // integer
        serde_json::json!(true),               // boolean
        serde_json::Value::Null,              // null
        serde_json::json!({"nested": "object"}), // object
    ];

    for (idx, input_val) in adversarial_inputs.into_iter().enumerate() {
        // 1. Fuzz grid_3d_generate image parameter
        let req_3d = AgentMeshEnvelope::CommandRequest {
            from: "fuzz-agent".into(),
            to: "grid".into(),
            command: "grid_3d_generate".into(),
            args: serde_json::json!({
                "image": input_val.clone(),
                "prompt": serde_json::Value::Null,
                "quality": "fast",
            }),
            payload: None,
            timeout_ms: Some(20),
            require_ack: false,
            correlation_id: format!("fuzz-3d-corr-{idx}"),
            version: "1.0".into(),
            timestamp: 2000 + idx as u64,
        };

        let res_3d = bridge.handle_command(&req_3d).await;
        // Verify response is valid envelope and bridge NEVER panicked
        assert_eq!(res_3d.correlation_id(), format!("fuzz-3d-corr-{idx}"));

        // 2. Fuzz grid_auto_rig input_mesh parameter
        let req_rig = AgentMeshEnvelope::CommandRequest {
            from: "fuzz-agent".into(),
            to: "grid".into(),
            command: "grid_auto_rig".into(),
            args: serde_json::json!({
                "input_mesh": input_val,
                "rig_preset": "biped",
            }),
            payload: None,
            timeout_ms: Some(20),
            require_ack: false,
            correlation_id: format!("fuzz-rig-corr-{idx}"),
            version: "1.0".into(),
            timestamp: 3000 + idx as u64,
        };

        let res_rig = bridge.handle_command(&req_rig).await;
        assert_eq!(res_rig.correlation_id(), format!("fuzz-rig-corr-{idx}"));
    }

    let _ = master.shutdown();
}

// =========================================================================
// 3. WebSocket DeliveryAck and Correlation Routing Suite
// =========================================================================

#[tokio::test]
async fn test_adv_websocket_delivery_ack_and_correlation_routing() {
    // 1. Spawn Master
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer spawn");
    let master_addr = master.server_addr().to_string();

    // 2. Spawn Hub with Bridge
    let hub = AgentMeshHub::new();
    let bridge = Arc::new(AgentGridBridge::new(master.clone()));
    hub.attach_bridge(bridge).await;
    let hub_port = hub.start("127.0.0.1:0").await.expect("Hub start");
    let hub_url = format!("ws://127.0.0.1:{}/ws", hub_port);

    // 3. Spawn Worker
    let worker_temp = TempDir::new().unwrap();
    let worker_cfg = WorkerConfig::new(master_addr)
        .with_name("worker-burst-1")
        .with_cores(4)
        .with_ram_mb(8192)
        .with_sandbox_base_dir(worker_temp.path());
    let mut worker = WorkerClient::new(worker_cfg);
    let (tx_w, rx_w) = watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(rx_w).await;
    });

    tokio::time::sleep(Duration::from_millis(250)).await;

    // 4. Connect via raw WebSocket tunnel to test wire message sequencing
    let (ws_stream, _) = connect_async(&hub_url)
        .await
        .expect("WebSocket connection to Hub failed");
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Register node
    let reg_env = AgentMeshEnvelope::NodeRegistration {
        version: "1.0".into(),
        correlation_id: "wire-reg-1".into(),
        from: "adv-wire-agent".into(),
        to: "hub".into(),
        platform: "windows".into(),
        hostname: "wire-box".into(),
        capabilities: serde_json::json!({}),
        timestamp: 1000,
    };
    ws_tx
        .send(WsMessage::Text(reg_env.to_json_string().unwrap()))
        .await
        .expect("Send registration failed");

    // Receive NodeRegistrationAck
    let reg_ack_msg = ws_rx.next().await.unwrap().unwrap();
    let reg_ack: AgentMeshEnvelope =
        AgentMeshEnvelope::from_json_str(&reg_ack_msg.to_text().unwrap()).unwrap();
    assert!(matches!(reg_ack, AgentMeshEnvelope::NodeRegistrationAck { .. }));

    // Send 3D generation request with require_ack: true
    let target_corr_id = format!("corr-wire-3d-{}", Uuid::new_v4());
    let req_3d = AgentMeshEnvelope::CommandRequest {
        from: "adv-wire-agent".into(),
        to: "grid".into(),
        command: "grid_3d_generate".into(),
        args: serde_json::json!({
            "prompt": "wireframe polygon",
            "quality": "fast",
            "engine": "sdf",
        }),
        payload: None,
        timeout_ms: Some(30000),
        require_ack: true,
        correlation_id: target_corr_id.clone(),
        version: "1.0".into(),
        timestamp: 5000,
    };
    ws_tx
        .send(WsMessage::Text(req_3d.to_json_string().unwrap()))
        .await
        .expect("Send 3D command failed");

    // 1st message MUST be DeliveryAck { status: "target_forwarded" } with matching correlation_id
    let ack_msg = ws_rx.next().await.unwrap().unwrap();
    let ack_env = AgentMeshEnvelope::from_json_str(&ack_msg.to_text().unwrap()).unwrap();
    match ack_env {
        AgentMeshEnvelope::DeliveryAck {
            status,
            correlation_id,
            from,
            to,
            ..
        } => {
            assert_eq!(status, "target_forwarded", "Immediate ACK status must be 'target_forwarded'");
            assert_eq!(correlation_id, target_corr_id, "DeliveryAck correlation_id mismatch");
            assert_eq!(from, "hub");
            assert_eq!(to, "adv-wire-agent");
        }
        other => panic!("Expected DeliveryAck, got {:?}", other),
    }

    // 2nd message MUST be CommandResponse with matching correlation_id and asset payload
    let resp_msg = ws_rx.next().await.unwrap().unwrap();
    let resp_env = AgentMeshEnvelope::from_json_str(&resp_msg.to_text().unwrap()).unwrap();
    match resp_env {
        AgentMeshEnvelope::CommandResponse {
            ref status,
            ref exit_code,
            ref correlation_id,
            ref from,
            ref to,
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(*exit_code, 0);
            assert_eq!(correlation_id, &target_corr_id, "CommandResponse correlation_id mismatch");
            assert_eq!(from, "grid");
            assert_eq!(to, "adv-wire-agent");

            let asset = AgentMeshClient::extract_asset_bytes(&resp_env)
                .expect("SHA-256 verification must pass on returned asset");
            assert!(asset.len() >= 20);
            assert_eq!(&asset[0..4], b"glTF");
        }
        other => panic!("Expected CommandResponse, got {:?}", other),
    }

    // Cleanup
    let _ = tx_w.send(true);
    let _ = worker_handle.await;
    let _ = hub.stop().await;
    let _ = master.shutdown();
}

// =========================================================================
// 4. High-Concurrency Agent Mesh Burst Suite (10 Concurrent Clients)
// =========================================================================

#[tokio::test]
async fn test_adv_high_concurrency_mesh_burst() {
    // 1. Spawn Master
    let master = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("MasterServer spawn");
    let master_addr = master.server_addr().to_string();

    // 2. Spawn Hub with Bridge
    let hub = AgentMeshHub::new();
    let bridge = Arc::new(AgentGridBridge::new(master.clone()));
    hub.attach_bridge(bridge).await;
    let hub_port = hub.start("127.0.0.1:0").await.expect("Hub start");
    let hub_url = format!("ws://127.0.0.1:{}/ws", hub_port);

    // 3. Spawn 2 Grid Workers for parallel throughput
    let mut worker_senders = Vec::new();
    let mut worker_handles = Vec::new();
    for i in 0..2 {
        let temp = TempDir::new().unwrap();
        let cfg = WorkerConfig::new(master_addr.clone())
            .with_name(&format!("burst-worker-{i}"))
            .with_cores(4)
            .with_ram_mb(8192)
            .with_sandbox_base_dir(temp.path());
        let mut worker = WorkerClient::new(cfg);
        let (tx, rx) = watch::channel(false);
        worker_senders.push(tx);
        worker_handles.push(tokio::spawn(async move {
            let _ = worker.run(rx).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    // 4. Spawn 10 concurrent AgentMeshClient instances
    let num_clients = 10;
    let mut client_tasks = Vec::new();
    let humanoid_mesh = Arc::new(create_humanoid_obj());

    for i in 0..num_clients {
        let client_url = hub_url.clone();
        let mesh_clone = humanoid_mesh.clone();
        let client_id = format!("burst-agent-{i}");

        client_tasks.push(tokio::spawn(async move {
            let client = AgentMeshClient::new(&client_url, &client_id, "rust-rt");
            client.connect().await.expect("Client connect failed");

            // Alternating task: even clients submit 3D generate, odd clients submit AutoRig
            let resp = if i % 2 == 0 {
                let prompt = format!("burst crystalline structure {i}");
                client
                    .generate_3d_from_prompt(&prompt, Some("fast"), Some("sdf"), Some(30000))
                    .await
                    .expect("Concurrent generate_3d_from_prompt failed")
            } else {
                client
                    .auto_rig(&mesh_clone, Some("biped"), None, Some("builtin"), Some(30000))
                    .await
                    .expect("Concurrent auto_rig failed")
            };

            match resp {
                AgentMeshEnvelope::CommandResponse {
                    ref status,
                    ref exit_code,
                    ref to,
                    ref from,
                    ..
                } => {
                    assert_eq!(status, "success", "Client {client_id} task failed: {:?}", resp);
                    assert_eq!(*exit_code, 0);
                    assert_eq!(to, &client_id, "Response routed to wrong agent! Expected {client_id}, got {to}");
                    assert_eq!(from, "grid");

                    let asset = AgentMeshClient::extract_asset_bytes(&resp)
                        .expect("Asset cryptographic verification failed");
                    assert!(asset.len() >= 20);
                    assert_eq!(&asset[0..4], b"glTF");
                }
                other => panic!("Client {client_id} expected CommandResponse, got {:?}", other),
            }

            client.close().await;
        }));
    }

    // Wait for all 10 concurrent requests to resolve
    for (idx, task) in client_tasks.into_iter().enumerate() {
        task.await.unwrap_or_else(|e| panic!("Client task {idx} panicked: {e}"));
    }

    // Cleanup
    for tx in worker_senders {
        let _ = tx.send(true);
    }
    for handle in worker_handles {
        let _ = handle.await;
    }
    let _ = hub.stop().await;
    let _ = master.shutdown();
}
