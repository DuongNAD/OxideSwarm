//! Adversarial test harness for OxideSwarm Mobile Coordinator & Socket Teardown Resilience.
//!
//! Evaluates failure modes, concurrency bounds, corrupt frame handling,
//! pre-sleep evacuation latency, and connection tracking invariants under real TCP sockets.

use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use uuid::Uuid;

use oxideworker::{
    get_master_worker_count_impl, is_master_running_impl, start_master_impl, stop_master_impl,
};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};

static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Helper to connect a synthetic worker, register it, and return its MessageTransport.
async fn connect_and_register(
    addr: &str,
    worker_id: Uuid,
    name: &str,
) -> MessageTransport<TcpStream> {
    let stream = TcpStream::connect(addr)
        .await
        .expect("Synthetic worker should connect to mobile coordinator");
    let mut transport = MessageTransport::new(stream);

    let caps = WorkerCapabilities::new(name, 4, 2048, false, false, None);
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities: caps,
    };
    transport
        .send_msg(&reg_msg)
        .await
        .expect("Send Register must succeed");

    let ack_msg = transport
        .recv_msg::<MasterMessage>()
        .await
        .expect("Receive RegisterAck must succeed")
        .expect("Connection should not be closed");

    match ack_msg {
        MasterMessage::RegisterAck {
            accepted,
            worker_id: ack_id,
            ..
        } => {
            assert!(accepted, "Registration should be accepted");
            assert_eq!(ack_id, worker_id, "Worker ID should match");
        }
        other => panic!("Expected RegisterAck, got: {other:?}"),
    }

    transport
}

#[tokio::test]
async fn test_adversarial_double_decrement_on_evacuation() {
    let _guard = TEST_MUTEX.lock().await;
    stop_master_impl();

    let lan_addr = start_master_impl(0, 0, false).expect("Mobile master coordinator should start");
    assert!(is_master_running_impl());
    assert_eq!(get_master_worker_count_impl(), 0);

    // 1. Connect Worker 1
    let id_1 = Uuid::new_v4();
    let mut worker_1 = connect_and_register(&lan_addr, id_1, "worker-node-1").await;

    // 2. Connect Worker 2
    let id_2 = Uuid::new_v4();
    let mut worker_2 = connect_and_register(&lan_addr, id_2, "worker-node-2").await;

    // Settle registration
    tokio::time::sleep(Duration::from_millis(60)).await;
    let initial_count = get_master_worker_count_impl();
    assert_eq!(
        initial_count, 2,
        "Mobile coordinator must record 2 connected workers, got: {initial_count}"
    );

    // 3. Worker 1 initiates zero-latency pre-sleep evacuation
    let disconnect_msg = WorkerMessage::Disconnecting {
        worker_id: id_1,
        reason: "BATTERY_CRITICAL".to_string(),
    };
    worker_1
        .send_msg(&disconnect_msg)
        .await
        .expect("Send Disconnecting must succeed");

    let ack = worker_1
        .recv_msg::<MasterMessage>()
        .await
        .expect("Receive Shutdown ACK must succeed")
        .expect("Message should not be EOF");

    match ack {
        MasterMessage::Shutdown { reason, .. } => {
            assert!(
                reason.contains("pre-sleep evacuation"),
                "Expected pre-sleep evacuation ack, got: {reason}"
            );
        }
        other => panic!("Expected Shutdown ack, got: {other:?}"),
    }

    // Drop Worker 1 transport
    drop(worker_1);

    // Settle disconnection accounting
    tokio::time::sleep(Duration::from_millis(60)).await;

    // 4. CRITICAL INVARIANT: Worker 2 is STILL CONNECTED and sending heartbeats!
    let hb_msg = WorkerMessage::Heartbeat {
        worker_id: id_2,
        timestamp: 999999,
        active_tasks: 0,
        cpu_usage_pct: 10.0,
        ram_available_mb: 4096,
    };
    worker_2
        .send_msg(&hb_msg)
        .await
        .expect("Worker 2 should still be able to send heartbeat");

    let hb_ack = worker_2
        .recv_msg::<MasterMessage>()
        .await
        .expect("Worker 2 must receive heartbeat ack")
        .expect("Worker 2 connection must not be closed");

    match hb_ack {
        MasterMessage::HeartbeatAck { timestamp } => assert_eq!(timestamp, 999999),
        other => panic!("Expected HeartbeatAck, got: {other:?}"),
    }

    let remaining_count = get_master_worker_count_impl();
    println!("DEBUG: remaining_count after worker 1 evacuated: {remaining_count}");

    // If double-decrement bug exists, remaining_count will be 0 instead of 1!
    assert_eq!(
        remaining_count, 1,
        "INVARIANT VIOLATION: Worker 2 is still actively connected and responding, but coordinator reports {remaining_count} workers (Double-decrement bug detected!)"
    );

    stop_master_impl();
}

#[tokio::test]
async fn test_pre_sleep_evacuation_latency_sub_50ms() {
    let _guard = TEST_MUTEX.lock().await;
    stop_master_impl();

    let lan_addr = start_master_impl(0, 0, false).expect("Mobile master coordinator should start");
    let id = Uuid::new_v4();
    let mut worker = connect_and_register(&lan_addr, id, "latency-worker").await;

    // Measure pre-sleep evacuation round-trip time
    let disconnect_msg = WorkerMessage::Disconnecting {
        worker_id: id,
        reason: "SCREEN_OFF_DOZE".to_string(),
    };

    let start_instant = Instant::now();
    worker
        .send_msg(&disconnect_msg)
        .await
        .expect("Send Disconnecting");
    let ack = worker
        .recv_msg::<MasterMessage>()
        .await
        .expect("Recv")
        .expect("Not EOF");
    let elapsed = start_instant.elapsed();

    println!("Empirical pre-sleep evacuation latency: {:?}", elapsed);
    assert!(
        elapsed < Duration::from_millis(50),
        "Pre-sleep evacuation roundtrip took {:?}, exceeding 50ms requirement",
        elapsed
    );

    match ack {
        MasterMessage::Shutdown { .. } => {}
        other => panic!("Expected Shutdown ack, got: {other:?}"),
    }

    stop_master_impl();
}

#[tokio::test]
async fn test_abrupt_socket_drop_teardown() {
    let _guard = TEST_MUTEX.lock().await;
    stop_master_impl();

    let lan_addr = start_master_impl(0, 0, false).expect("Mobile master coordinator should start");
    let id = Uuid::new_v4();
    let worker = connect_and_register(&lan_addr, id, "abrupt-drop-worker").await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 1);

    // Abruptly drop the TCP connection (simulating cellular drop / crash)
    drop(worker);

    // Coordinator should detect EOF and decrement count within 150ms
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        get_master_worker_count_impl(),
        0,
        "Abrupt socket drop must decrement connected worker count"
    );

    stop_master_impl();
}

#[tokio::test]
async fn test_rapid_coordinator_start_stop_cycles() {
    let _guard = TEST_MUTEX.lock().await;

    for i in 0..10 {
        let addr = start_master_impl(0, 0, false)
            .unwrap_or_else(|e| panic!("Iteration {i}: Start master failed: {e}"));
        assert!(is_master_running_impl());
        assert!(!addr.is_empty());

        let stopped = stop_master_impl();
        assert!(stopped, "Iteration {i}: Stop master must return true");
        assert!(!is_master_running_impl());
    }
}

#[tokio::test]
async fn test_malformed_and_corrupt_frames_resilience() {
    let _guard = TEST_MUTEX.lock().await;
    stop_master_impl();

    let lan_addr = start_master_impl(0, 0, false).expect("Start master");

    // Connect raw TCP socket and inject garbage
    let mut raw_stream = TcpStream::connect(&lan_addr).await.expect("Connect raw");

    // Write invalid frame length prefix + garbage payload
    let garbage = [0xFF, 0xFF, 0x00, 0x01, 0xDE, 0xAD, 0xBE, 0xEF];
    let write_res = raw_stream.write_all(&garbage).await;
    assert!(write_res.is_ok());

    // Allow coordinator to process and drop
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify coordinator is still healthy and accepting valid connections
    let valid_id = Uuid::new_v4();
    let valid_worker = connect_and_register(&lan_addr, valid_id, "healthy-worker").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 1);

    drop(valid_worker);
    stop_master_impl();
}

#[tokio::test]
async fn test_duplicate_registration_on_same_socket() {
    let _guard = TEST_MUTEX.lock().await;
    stop_master_impl();

    let lan_addr = start_master_impl(0, 0, false).expect("Start master");
    let stream = TcpStream::connect(&lan_addr).await.expect("Connect");
    let mut transport = MessageTransport::new(stream);

    let id = Uuid::new_v4();
    let caps = WorkerCapabilities::new("dup-node", 2, 1024, false, false, None);

    // First Register
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: id,
            capabilities: caps.clone(),
        })
        .await
        .expect("Send 1");
    let _ack1 = transport.recv_msg::<MasterMessage>().await.expect("Recv 1");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 1);

    // Second Register on same connection
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: id,
            capabilities: caps,
        })
        .await
        .expect("Send 2");
    let _ack2 = transport.recv_msg::<MasterMessage>().await.expect("Recv 2");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let count_after_dup = get_master_worker_count_impl();
    println!("DEBUG: count_after_dup = {count_after_dup}");
    assert_eq!(
        count_after_dup, 1,
        "Duplicate registration on the same socket must be idempotent"
    );

    drop(transport);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let count_after_drop = get_master_worker_count_impl();
    println!("DEBUG: count_after_drop = {count_after_drop}");
    assert_eq!(
        count_after_drop, 0,
        "Dropping socket after duplicate registration must reduce count to 0"
    );

    stop_master_impl();
}
