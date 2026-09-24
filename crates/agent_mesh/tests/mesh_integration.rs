//! Integration Tests for OxideSwarm Agent Mesh
//!
//! Verifies:
//! 1. 3-Node local simulation and targeted command routing (Node 1 -> Node 3)
//! 2. Target isolation (Node 2 non-interference)
//! 3. Real OS shell subprocess execution
//! 4. Bidirectional 64 KB data payload transmission with bit-for-bit SHA-256 verification
//! 5. High-throughput concurrent burst stress test (50 packets with zero data loss)
//! 6. Negative routing with DeliveryNack generation on missing target
//! 7. REST Observability endpoints (/api/status, /api/nodes, /api/health)

use agent_mesh::{
    compute_sha256, verify_sha256, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub,
};
use std::time::Duration;

#[tokio::test]
async fn test_three_node_simulation_and_routing() {
    // 1. Start Hub on ephemeral port
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    // 2. Connect 3 simulated heterogeneous nodes
    let node1 = AgentMeshClient::new(&hub_url, "sim-win-1", "windows");
    let node2 = AgentMeshClient::new(&hub_url, "sim-mac-2", "macos");
    let node3 = AgentMeshClient::new(&hub_url, "sim-android-3", "android");

    node1.connect().await.expect("Node 1 failed to connect");
    node2.connect().await.expect("Node 2 failed to connect");
    node3.connect().await.expect("Node 3 failed to connect");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 3. Verify Active Directory Catalog
    let nodes = node1.query_node_list(Some(3000)).await.expect("Catalog query failed");
    let node_ids: Vec<String> = nodes.iter().map(|n| n.node_id.clone()).collect();
    assert!(node_ids.contains(&"sim-win-1".to_string()));
    assert!(node_ids.contains(&"sim-mac-2".to_string()));
    assert!(node_ids.contains(&"sim-android-3".to_string()));

    // 4. Targeted Command Routing: Node 1 -> Node 3
    let res = node1
        .send_command(
            "sim-android-3",
            "echo",
            serde_json::json!({"message": "OxideSwarm Verified Command Flow"}),
            Some(4000),
        )
        .await
        .expect("Command routing failed");

    if let AgentMeshEnvelope::CommandResponse {
        status,
        exit_code,
        stdout,
        from,
        to,
        ..
    } = res
    {
        assert_eq!(status, "success");
        assert_eq!(exit_code, 0);
        assert_eq!(stdout, "OxideSwarm Verified Command Flow");
        assert_eq!(from, "sim-android-3");
        assert_eq!(to, "sim-win-1");
    } else {
        panic!("Expected CommandResponse, got {:?}", res);
    }

    // 5. Verify Node 2 Isolation (Zero cross-talk)
    assert_eq!(node3.executed_commands_count(), 1);
    assert_eq!(node2.executed_commands_count(), 0);
    assert_eq!(node1.executed_commands_count(), 0);

    node1.close().await;
    node2.close().await;
    node3.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_real_subprocess_execution() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    let sender = AgentMeshClient::new(&hub_url, "sender-node", "windows");
    let runner = AgentMeshClient::new(&hub_url, "runner-node", "linux");

    sender.connect().await.expect("Sender failed to connect");
    runner.connect().await.expect("Runner failed to connect");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let res = sender
        .send_command(
            "runner-node",
            "shell_exec",
            serde_json::json!({"cmd": "echo Subprocess_Execution_OK"}),
            Some(5000),
        )
        .await
        .expect("Command failed");

    if let AgentMeshEnvelope::CommandResponse {
        status,
        exit_code,
        stdout,
        ..
    } = res
    {
        assert_eq!(status, "success");
        assert_eq!(exit_code, 0);
        assert!(stdout.contains("Subprocess_Execution_OK"));
    } else {
        panic!("Expected CommandResponse, got {:?}", res);
    }

    sender.close().await;
    runner.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_bidirectional_data_payload_lossless() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    let node1 = AgentMeshClient::new(&hub_url, "data-node-1", "macos");
    let node3 = AgentMeshClient::new(&hub_url, "data-node-3", "android");

    node1.connect().await.expect("Node 1 failed to connect");
    node3.connect().await.expect("Node 3 failed to connect");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A. Forward: 64 KB (65,536 bytes) from Node 1 -> Node 3
    let mut payload_1 = vec![0u8; 65536];
    for (i, b) in payload_1.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }
    let expected_hash_1 = compute_sha256(&payload_1);

    let (_corr_1, hash_1) = node1
        .send_data_payload("data-node-3", &payload_1)
        .await
        .expect("Forward payload failed");
    assert_eq!(hash_1, expected_hash_1);

    tokio::time::sleep(Duration::from_millis(250)).await;

    let rec_node3 = node3.received_data_payloads().await;
    assert_eq!(rec_node3.len(), 1, "Node 3 did not receive payload");

    if let AgentMeshEnvelope::DataPayload {
        data,
        checksum_sha256,
        ..
    } = &rec_node3[0]
    {
        let rec_bytes: Vec<u8> = data.as_ref().unwrap().chars().map(|c| c as u8).collect();
        assert_eq!(rec_bytes.len(), 65536);
        assert!(verify_sha256(&rec_bytes, checksum_sha256.as_ref().unwrap()));
        assert_eq!(checksum_sha256.as_ref().unwrap(), &expected_hash_1);
    } else {
        panic!("Expected DataPayload");
    }

    // B. Reverse: 64 KB from Node 3 -> Node 1
    let mut payload_2 = vec![0u8; 65536];
    for (i, b) in payload_2.iter_mut().enumerate() {
        *b = ((i + 73) % 256) as u8;
    }
    let expected_hash_2 = compute_sha256(&payload_2);

    let (_corr_2, hash_2) = node3
        .send_data_payload("data-node-1", &payload_2)
        .await
        .expect("Reverse payload failed");
    assert_eq!(hash_2, expected_hash_2);

    tokio::time::sleep(Duration::from_millis(250)).await;

    let rec_node1 = node1.received_data_payloads().await;
    assert_eq!(rec_node1.len(), 1, "Node 1 did not receive reverse payload");

    if let AgentMeshEnvelope::DataPayload {
        data,
        checksum_sha256,
        ..
    } = &rec_node1[0]
    {
        let rec_bytes: Vec<u8> = data.as_ref().unwrap().chars().map(|c| c as u8).collect();
        assert_eq!(rec_bytes.len(), 65536);
        assert!(verify_sha256(&rec_bytes, checksum_sha256.as_ref().unwrap()));
        assert_eq!(checksum_sha256.as_ref().unwrap(), &expected_hash_2);
    } else {
        panic!("Expected reverse DataPayload");
    }

    // C. Concurrent Burst Stress Test (50 packets)
    for i in 0..50 {
        let burst_data = format!("burst-test-data-{}-payload", i).into_bytes();
        let _ = node1.send_data_payload("data-node-3", &burst_data).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let total_node3 = node3.received_data_payloads().await;
    assert_eq!(
        total_node3.len(),
        1 + 50,
        "Data loss detected in burst test!"
    );

    node1.close().await;
    node3.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_negative_routing_delivery_nack() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);

    let client = AgentMeshClient::new(&hub_url, "active-node", "windows");
    client.connect().await.expect("Connect failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let res = client
        .send_command("ghost-node-404", "ping", serde_json::Value::Null, Some(2000))
        .await
        .expect("Command send should complete");

    if let AgentMeshEnvelope::DeliveryNack {
        error_code, reason, ..
    } = res
    {
        assert_eq!(error_code, "ERR_NODE_NOT_FOUND");
        assert!(reason.contains("ghost-node-404"));
    } else {
        panic!("Expected DeliveryNack, got {:?}", res);
    }

    client.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_rest_observability_endpoints() {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);
    let base_http = format!("http://127.0.0.1:{}", port);

    let client = AgentMeshClient::new(&hub_url, "rest-test-node", "ubuntu");
    client.connect().await.expect("Connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let http_client = reqwest::Client::new();

    // 1. Test /api/health
    let health_res = http_client
        .get(format!("{}/api/health", base_http))
        .send()
        .await
        .expect("Health GET failed");
    assert_eq!(health_res.status(), 200);

    // 2. Test /api/nodes
    let nodes_res = http_client
        .get(format!("{}/api/nodes", base_http))
        .send()
        .await
        .expect("Nodes GET failed");
    assert_eq!(nodes_res.status(), 200);
    let nodes_json: serde_json::Value = nodes_res.json().await.expect("JSON parse failed");
    let nodes_arr = nodes_json.as_array().expect("Expected JSON array");
    assert_eq!(nodes_arr.len(), 1);
    assert_eq!(nodes_arr[0]["node_id"], "rest-test-node");

    // 3. Test /api/status
    let status_res = http_client
        .get(format!("{}/api/status", base_http))
        .send()
        .await
        .expect("Status GET failed");
    assert_eq!(status_res.status(), 200);
    let status_json: serde_json::Value = status_res.json().await.expect("Status JSON failed");
    assert_eq!(status_json["status"], "online");
    assert_eq!(status_json["connected_nodes"], 1);

    client.close().await;
    hub.stop().await;
}
