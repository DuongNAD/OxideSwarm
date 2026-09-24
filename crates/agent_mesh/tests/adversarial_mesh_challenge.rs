//! Adversarial Challenge Test Suite for OxideSwarm Agent Mesh
//!
//! Author: Challenger 1 (teamwork_preview_challenger)
//!
//! Rigorous stress-testing of communication mesh protocol, hub relay, and command routing:
//! 1. Verified Node 1 -> Node 3 command routing and response accuracy
//! 2. Strict node isolation: Node 2 NEVER receives or executes commands addressed to Node 3
//! 3. Negative routing: Nonexistent node IDs trigger DeliveryNack
//! 4. High-frequency rapid node churn (registration / deregistration)
//! 5. High-throughput message bursts and large payload boundary testing

use agent_mesh::{
    compute_sha256, verify_sha256, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Helper to spin up a hub on an ephemeral port
async fn spawn_test_hub() -> (AgentMeshHub, String) {
    let hub = AgentMeshHub::new();
    let port = hub.start("127.0.0.1:0").await.expect("Failed to start test hub");
    let hub_url = format!("ws://127.0.0.1:{}/ws", port);
    (hub, hub_url)
}

#[tokio::test]
async fn test_adv_node1_to_node3_command_flow() {
    let (hub, hub_url) = spawn_test_hub().await;

    let node1 = AgentMeshClient::new(&hub_url, "adv-node-win-1", "windows");
    let node2 = AgentMeshClient::new(&hub_url, "adv-node-mac-2", "macos");
    let node3 = AgentMeshClient::new(&hub_url, "adv-node-android-3", "android");

    node1.connect().await.expect("Node 1 failed to connect");
    node2.connect().await.expect("Node 2 failed to connect");
    node3.connect().await.expect("Node 3 failed to connect");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify all 3 registered in catalog
    let catalog = node1.query_node_list(Some(3000)).await.expect("Catalog query failed");
    assert_eq!(catalog.len(), 3, "Expected 3 nodes in catalog, found {}", catalog.len());

    // 1. Echo Command
    let echo_res = node1
        .send_command(
            "adv-node-android-3",
            "echo",
            serde_json::json!({"message": "Adversarial Verification 2026"}),
            Some(4000),
        )
        .await
        .expect("Echo command failed");

    match echo_res {
        AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            from,
            to,
            ..
        } => {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert_eq!(stdout, "Adversarial Verification 2026");
            assert_eq!(from, "adv-node-android-3");
            assert_eq!(to, "adv-node-win-1");
        }
        other => panic!("Expected CommandResponse, got {:?}", other),
    }

    // 2. Ping Command
    let ping_res = node1
        .send_command("adv-node-android-3", "ping", serde_json::Value::Null, Some(3000))
        .await
        .expect("Ping command failed");

    match ping_res {
        AgentMeshEnvelope::CommandResponse { status, stdout, .. } => {
            assert_eq!(status, "success");
            assert_eq!(stdout, "pong");
        }
        other => panic!("Expected CommandResponse for ping, got {:?}", other),
    }

    // 3. Subprocess execution
    let shell_res = node1
        .send_command(
            "adv-node-android-3",
            "shell_exec",
            serde_json::json!({"cmd": "echo ADV_SUBPROCESS_OK"}),
            Some(5000),
        )
        .await
        .expect("Subprocess command failed");

    match shell_res {
        AgentMeshEnvelope::CommandResponse { status, exit_code, stdout, .. } => {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert!(stdout.contains("ADV_SUBPROCESS_OK"), "Unexpected stdout: {}", stdout);
        }
        other => panic!("Expected CommandResponse for shell_exec, got {:?}", other),
    }

    // Strict isolation verification
    assert_eq!(node3.executed_commands_count(), 3, "Node 3 should have executed 3 commands");
    assert_eq!(node2.executed_commands_count(), 0, "Node 2 isolation breached! Count > 0");
    assert_eq!(node1.executed_commands_count(), 0, "Node 1 isolation breached! Count > 0");

    node1.close().await;
    node2.close().await;
    node3.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_adv_strict_node_isolation_under_heavy_concurrency() {
    let (hub, hub_url) = spawn_test_hub().await;

    let node1 = Arc::new(AgentMeshClient::new(&hub_url, "sender-1", "windows"));
    let node2 = Arc::new(AgentMeshClient::new(&hub_url, "isolated-2", "macos"));
    let node3 = Arc::new(AgentMeshClient::new(&hub_url, "target-3", "android"));

    node1.connect().await.expect("Sender 1 connect failed");
    node2.connect().await.expect("Isolated 2 connect failed");
    node3.connect().await.expect("Target 3 connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let iterations = 50;
    let mut handles = Vec::new();

    // Launch concurrent command requests from node 1 to node 3
    for i in 0..iterations {
        let n1 = node1.clone();
        handles.push(tokio::spawn(async move {
            let msg = format!("concurrent-test-payload-{}", i);
            let res = n1
                .send_command(
                    "target-3",
                    "echo",
                    serde_json::json!({ "message": msg }),
                    Some(6000),
                )
                .await;
            res
        }));
    }

    let mut successful_responses = 0;
    for h in handles {
        let res = h.await.expect("Task join failed").expect("Command send failed");
        if let AgentMeshEnvelope::CommandResponse { status, exit_code, .. } = res {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            successful_responses += 1;
        } else {
            panic!("Unexpected envelope: {:?}", res);
        }
    }

    assert_eq!(successful_responses, iterations);

    // Wait slightly to ensure all background tasks have processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // STRICT ISOLATION ASSERTIONS
    assert_eq!(
        node3.executed_commands_count(),
        iterations,
        "Node 3 did not execute expected number of commands"
    );
    assert_eq!(
        node2.executed_commands_count(),
        0,
        "CRITICAL ISOLATION BREACH: Node 2 executed {} commands meant for Node 3!",
        node2.executed_commands_count()
    );
    assert_eq!(
        node1.executed_commands_count(),
        0,
        "Node 1 executed unexpected commands"
    );
    assert_eq!(
        node2.received_data_payloads().await.len(),
        0,
        "CRITICAL ISOLATION BREACH: Node 2 received data packets meant for other nodes!"
    );

    node1.close().await;
    node2.close().await;
    node3.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_adv_negative_routing_delivery_nack() {
    let (hub, hub_url) = spawn_test_hub().await;

    let client = AgentMeshClient::new(&hub_url, "adv-caller", "linux");
    client.connect().await.expect("Client connect failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test 1: Nonexistent ASCII ID
    let res1 = client
        .send_command("ghost-node-404", "ping", serde_json::Value::Null, Some(3000))
        .await
        .expect("Command send failed");

    match res1 {
        AgentMeshEnvelope::DeliveryNack { error_code, reason, to, from, .. } => {
            assert_eq!(error_code, "ERR_NODE_NOT_FOUND");
            assert_eq!(to, "adv-caller");
            assert_eq!(from, "hub");
            assert!(reason.contains("ghost-node-404"));
        }
        other => panic!("Expected DeliveryNack for nonexistent node, got {:?}", other),
    }

    // Test 2: Unicode Nonexistent ID
    let res2 = client
        .send_command("nonexistent-🚀-node", "echo", serde_json::json!({"message": "test"}), Some(3000))
        .await
        .expect("Command send failed");

    match res2 {
        AgentMeshEnvelope::DeliveryNack { error_code, reason, .. } => {
            assert_eq!(error_code, "ERR_NODE_NOT_FOUND");
            assert!(reason.contains("nonexistent-🚀-node"));
        }
        other => panic!("Expected DeliveryNack for unicode target, got {:?}", other),
    }

    // Test 3: Data payload to nonexistent node
    let (corr_id, _hash) = client
        .send_data_payload("ghost-target-999", b"sample payload")
        .await
        .expect("Failed to send data payload");

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Hub should have delivered NACK to client
    assert!(!corr_id.is_empty());

    client.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_adv_rapid_node_churn() {
    let (hub, hub_url) = spawn_test_hub().await;

    let base_node = AgentMeshClient::new(&hub_url, "stable-anchor", "windows");
    base_node.connect().await.expect("Base anchor failed to connect");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Initial catalog should have 1 node
    let cat_init = base_node.query_node_list(Some(3000)).await.expect("Init query failed");
    assert_eq!(cat_init.len(), 1);

    // Rapidly connect and disconnect 30 nodes sequentially
    for i in 0..30 {
        let ephemeral_id = format!("churn-node-seq-{}", i);
        let ephemeral = AgentMeshClient::new(&hub_url, &ephemeral_id, "linux");
        ephemeral.connect().await.expect("Ephemeral connect failed");
        // Ensure registered
        tokio::time::sleep(Duration::from_millis(15)).await;
        ephemeral.close().await;
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Rapidly connect 10 nodes concurrently, then disconnect concurrently
    let concurrent_count = 10;
    let mut churn_clients = Vec::new();
    for i in 0..concurrent_count {
        let ephemeral_id = format!("churn-node-par-{}", i);
        let ephemeral = Arc::new(AgentMeshClient::new(&hub_url, &ephemeral_id, "android"));
        churn_clients.push(ephemeral);
    }

    let mut connect_handles = Vec::new();
    for c in &churn_clients {
        let client_clone = c.clone();
        connect_handles.push(tokio::spawn(async move {
            client_clone.connect().await
        }));
    }

    for h in connect_handles {
        let res = h.await.expect("Connect task panicked");
        assert!(res.is_ok(), "Concurrent connect failed: {:?}", res);
    }

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify catalog size: anchor + 10 = 11 nodes
    let cat_churn = base_node.query_node_list(Some(3000)).await.expect("Churn query failed");
    assert_eq!(
        cat_churn.len(),
        1 + concurrent_count,
        "Catalog size mismatch during concurrent churn"
    );

    // Disconnect all 10 concurrent nodes
    for c in &churn_clients {
        c.close().await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Final catalog should return to just the anchor node
    let cat_final = base_node.query_node_list(Some(3000)).await.expect("Final query failed");
    assert_eq!(
        cat_final.len(),
        1,
        "Catalog failed to prune disconnected nodes: found {} nodes",
        cat_final.len()
    );
    assert_eq!(cat_final[0].node_id, "stable-anchor");

    base_node.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_adv_rapid_message_burst() {
    let (hub, hub_url) = spawn_test_hub().await;

    let node1 = Arc::new(AgentMeshClient::new(&hub_url, "burst-node-1", "macos"));
    let node2 = Arc::new(AgentMeshClient::new(&hub_url, "isolated-spy", "windows"));
    let node3 = Arc::new(AgentMeshClient::new(&hub_url, "burst-node-3", "ubuntu"));

    node1.connect().await.expect("Node 1 connect failed");
    node2.connect().await.expect("Node 2 connect failed");
    node3.connect().await.expect("Node 3 connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let _burst_packets = 100; // 50 forward + 50 reverse
    let counter_fwd = Arc::new(AtomicUsize::new(0));
    let counter_rev = Arc::new(AtomicUsize::new(0));

    let mut send_handles = Vec::new();

    // 50 forward packets (Node 1 -> Node 3)
    for i in 0..50 {
        let n1 = node1.clone();
        let cf = counter_fwd.clone();
        send_handles.push(tokio::spawn(async move {
            let data = format!("BURST-FWD-{}-{}", i, "X".repeat(512)).into_bytes();
            let (_corr, hash) = n1.send_data_payload("burst-node-3", &data).await.expect("Send failed");
            cf.fetch_add(1, Ordering::SeqCst);
            hash
        }));
    }

    // 50 reverse packets (Node 3 -> Node 1)
    for i in 0..50 {
        let n3 = node3.clone();
        let cr = counter_rev.clone();
        send_handles.push(tokio::spawn(async move {
            let data = format!("BURST-REV-{}-{}", i, "Y".repeat(512)).into_bytes();
            let (_corr, hash) = n3.send_data_payload("burst-node-1", &data).await.expect("Send failed");
            cr.fetch_add(1, Ordering::SeqCst);
            hash
        }));
    }

    for h in send_handles {
        h.await.expect("Send task join failed");
    }

    assert_eq!(counter_fwd.load(Ordering::SeqCst), 50);
    assert_eq!(counter_rev.load(Ordering::SeqCst), 50);

    // Wait for delivery
    tokio::time::sleep(Duration::from_millis(400)).await;

    let rec3 = node3.received_data_payloads().await;
    let rec1 = node1.received_data_payloads().await;
    let rec2 = node2.received_data_payloads().await;

    assert_eq!(rec3.len(), 50, "Node 3 did not receive all 50 forward packets (got {})", rec3.len());
    assert_eq!(rec1.len(), 50, "Node 1 did not receive all 50 reverse packets (got {})", rec1.len());
    assert_eq!(
        rec2.len(),
        0,
        "ISOLATION VIOLATION: Intermediary Node 2 intercepted {} packets!",
        rec2.len()
    );

    node1.close().await;
    node2.close().await;
    node3.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_adv_large_payload_boundaries() {
    let (hub, hub_url) = spawn_test_hub().await;

    let sender = AgentMeshClient::new(&hub_url, "sender-node", "windows");
    let receiver = AgentMeshClient::new(&hub_url, "receiver-node", "android");

    sender.connect().await.expect("Sender connect failed");
    receiver.connect().await.expect("Receiver connect failed");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let test_sizes = vec![0, 1024, 65536, 131072, 262144]; // 0B, 1KB, 64KB, 128KB, 256KB

    for size in test_sizes {
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let expected_hash = compute_sha256(&payload);

        let (_corr, sent_hash) = sender
            .send_data_payload("receiver-node", &payload)
            .await
            .expect("Payload send failed");
        assert_eq!(sent_hash, expected_hash);

        tokio::time::sleep(Duration::from_millis(150)).await;

        let received_list = receiver.received_data_payloads().await;
        let last_rec = received_list.last().expect("No payload received");

        if let AgentMeshEnvelope::DataPayload {
            data,
            checksum_sha256,
            chunk_size_bytes,
            ..
        } = last_rec
        {
            assert_eq!(*chunk_size_bytes, size, "Chunk size mismatch for size {}", size);
            let rec_bytes: Vec<u8> = data.as_ref().unwrap().chars().map(|c| c as u8).collect();
            assert_eq!(rec_bytes.len(), size, "Received byte count mismatch for size {}", size);
            assert_eq!(
                checksum_sha256.as_deref(),
                Some(expected_hash.as_str()),
                "SHA-256 hash mismatch for size {}",
                size
            );
            assert!(
                verify_sha256(&rec_bytes, &expected_hash),
                "verify_sha256 failed for size {}",
                size
            );
        } else {
            panic!("Expected DataPayload envelope");
        }
    }

    sender.close().await;
    receiver.close().await;
    hub.stop().await;
}
