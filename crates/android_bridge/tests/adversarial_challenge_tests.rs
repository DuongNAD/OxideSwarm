//! Adversarial Challenge Test Harness for OxideSwarm Android Bridge (`oxideworker`).
//!
//! Specifically stress-tests:
//! 1. Boundary telemetry inputs (0%, 100%, negative values, extreme out-of-bounds, rapid toggling, concurrent access).
//! 2. P2P ticket parsing with empty strings, whitespace, malformed JSON, LAN strings, and testing compatibility with real `iroh::EndpointAddr`.
//! 3. Rapid sequential master/worker start/stop cycles, double-start/stop idempotency, port conflicts, and deadlock detection.

use std::time::Duration;
use uuid::Uuid;

use oxideworker::{
    get_master_worker_count_impl, get_telemetry_impl, get_worker_status_impl,
    is_master_running_impl, is_running_impl, start_master_impl, start_worker_impl,
    stop_master_impl, stop_worker_impl, update_telemetry_detailed_impl, update_telemetry_impl,
};
use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};

static ADVERSARIAL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ==============================================================================
// 1. Boundary Telemetry Testing
// ==============================================================================

#[test]
fn test_adversarial_telemetry_boundaries() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    // 1. Nominal boundary: 0% battery
    assert!(update_telemetry_impl(0, false, false));
    let t_zero = get_telemetry_impl();
    assert_eq!(t_zero.battery_pct, Some(0), "0% battery must be accepted as Some(0)");
    assert_eq!(t_zero.is_charging, Some(false));
    assert!(!t_zero.thermal_throttled);

    // 2. Nominal boundary: 100% battery
    assert!(update_telemetry_impl(100, true, false));
    let t_full = get_telemetry_impl();
    assert_eq!(t_full.battery_pct, Some(100), "100% battery must be accepted as Some(100)");
    assert_eq!(t_full.is_charging, Some(true));
    assert!(!t_full.thermal_throttled);

    // 3. Negative out-of-bounds: -1, -50, i32::MIN
    for neg_val in [-1, -50, i32::MIN] {
        assert!(update_telemetry_impl(neg_val, false, true));
        let t_neg = get_telemetry_impl();
        assert_eq!(
            t_neg.battery_pct, None,
            "Negative battery level {neg_val} must sanitize to None"
        );
        assert!(t_neg.thermal_throttled);
    }

    // 4. Positive out-of-bounds: 101, 200, i32::MAX
    for pos_val in [101, 200, i32::MAX] {
        assert!(update_telemetry_impl(pos_val, true, false));
        let t_pos = get_telemetry_impl();
        assert_eq!(
            t_pos.battery_pct, None,
            "Excessive battery level {pos_val} must sanitize to None"
        );
    }

    // 5. Rapid thermal throttling and charging state toggling
    for i in 0..100 {
        let is_charging = i % 2 == 0;
        let thermal_throttled = i % 3 == 0;
        let pct = i % 101;
        assert!(update_telemetry_impl(pct, is_charging, thermal_throttled));
        let cur = get_telemetry_impl();
        assert_eq!(cur.battery_pct, Some(pct as u8));
        assert_eq!(cur.is_charging, Some(is_charging));
        assert_eq!(cur.thermal_throttled, thermal_throttled);
    }

    // 6. Adversarial edge cases for update_telemetry_detailed_impl
    // 6a. Extreme temperature values, NaN, +/-Infinity
    let temp_edge_cases = [
        (Some(f32::NAN), None, "NaN temperature must sanitize to None"),
        (Some(f32::INFINITY), None, "Positive infinity must sanitize to None"),
        (Some(f32::NEG_INFINITY), None, "Negative infinity must sanitize to None"),
        (Some(-50.001), None, "Below -50.0C lower bound must sanitize to None"),
        (Some(-1000.0), None, "Extreme negative temperature must sanitize to None"),
        (Some(120.001), None, "Above 120.0C upper bound must sanitize to None"),
        (Some(1000.0), None, "Extreme positive temperature must sanitize to None"),
        (Some(-50.0), Some(-50.0), "-50.0C exact lower bound must be accepted"),
        (Some(120.0), Some(120.0), "120.0C exact upper bound must be accepted"),
        (Some(0.0), Some(0.0), "0.0C nominal must be accepted"),
        (Some(37.5), Some(37.5), "37.5C normal operating temperature must be accepted"),
        (None, None, "None temperature must remain None"),
    ];

    for (input_temp, expected_temp, desc) in temp_edge_cases {
        assert!(update_telemetry_detailed_impl(
            75,
            true,
            false,
            input_temp,
            Some("wifi".to_string())
        ));
        let cur = get_telemetry_impl();
        assert_eq!(cur.battery_temperature, expected_temp, "{desc}");
        assert_eq!(cur.battery_temp_c(), expected_temp, "{desc} (via battery_temp_c)");
    }

    // 6b. Empty, whitespace, sentinel ("unknown", "none"), and valid network types
    let net_edge_cases = [
        (Some("".to_string()), None, "Empty string network type must sanitize to None"),
        (Some("   ".to_string()), None, "Whitespace string network type must sanitize to None"),
        (Some(" \t\r\n ".to_string()), None, "Whitespace with tabs/newlines must sanitize to None"),
        (Some("unknown".to_string()), None, "'unknown' sentinel must sanitize to None"),
        (Some("UNKNOWN".to_string()), None, "'UNKNOWN' uppercase sentinel must sanitize to None"),
        (Some("  Unknown  ".to_string()), None, "Padded 'Unknown' sentinel must sanitize to None"),
        (Some("none".to_string()), None, "'none' sentinel must sanitize to None"),
        (Some("NONE".to_string()), None, "'NONE' uppercase sentinel must sanitize to None"),
        (Some("  None  ".to_string()), None, "Padded 'None' sentinel must sanitize to None"),
        (Some("wifi".to_string()), Some("wifi".to_string()), "'wifi' must be accepted"),
        (Some("  WIFI  ".to_string()), Some("wifi".to_string()), "'  WIFI  ' must be trimmed and lowercased"),
        (Some("CELLULAR".to_string()), Some("cellular".to_string()), "'CELLULAR' must be lowercased"),
        (Some("Ethernet".to_string()), Some("ethernet".to_string()), "'Ethernet' must be lowercased"),
        (Some("5G-NR".to_string()), Some("5g-nr".to_string()), "'5G-NR' must be lowercased"),
        (None, None, "None network type must remain None"),
    ];

    for (input_net, expected_net, desc) in net_edge_cases {
        assert!(update_telemetry_detailed_impl(
            50,
            false,
            false,
            Some(35.0),
            input_net
        ));
        let cur = get_telemetry_impl();
        assert_eq!(cur.network_type, expected_net, "{desc}");
    }

    // 6c. JSON output serialization under edge cases
    assert!(update_telemetry_detailed_impl(
        -1,
        false,
        true,
        Some(f32::NAN),
        Some("  ".to_string())
    ));
    let status_str = get_worker_status_impl();
    let status_val: serde_json::Value = serde_json::from_str(&status_str)
        .expect("Worker status must always parse as valid JSON");
    assert_eq!(status_val["telemetry"]["battery_pct"], serde_json::Value::Null);
    assert_eq!(status_val["telemetry"]["battery_temperature"], serde_json::Value::Null);
    assert_eq!(status_val["telemetry"]["network_type"], serde_json::Value::Null);
    assert_eq!(status_val["telemetry"]["thermal_throttled"], true);
}

#[test]
fn test_adversarial_telemetry_concurrent_flood() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    // Concurrently flood update_telemetry_impl and get_worker_status_impl from 16 threads
    let threads: Vec<_> = (0..16)
        .map(|tid| {
            std::thread::spawn(move || {
                for i in 0..200 {
                    let pct = (tid * 10 + i) % 101;
                    let charging = (i + tid) % 2 == 0;
                    let throttled = (i + tid) % 3 == 0;
                    update_telemetry_impl(pct, charging, throttled);
                    let _ = get_worker_status_impl();
                    let _ = get_telemetry_impl();
                }
            })
        })
        .collect();

    for handle in threads {
        handle.join().expect("Concurrent telemetry thread should not panic");
    }

    // After flood, verify state is healthy and queryable
    let status = get_worker_status_impl();
    assert!(status.contains("telemetry"));
    assert!(status.contains("battery_pct"));
}

// ==============================================================================
// 2. P2P Ticket Parsing Edge Cases & Master Ticket Validation
// ==============================================================================

#[test]
fn test_adversarial_p2p_ticket_parsing_edge_cases() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    // 1. Empty string
    let res_empty = rusty_grid_core::transport::parse_p2p_ticket("");
    assert!(res_empty.is_err(), "Empty ticket string must fail to parse");

    // 2. Whitespace only
    let res_ws = rusty_grid_core::transport::parse_p2p_ticket("   \t\r\n   ");
    assert!(res_ws.is_err(), "Whitespace ticket string must fail to parse");

    // 3. Plain IP/port LAN address (not a P2P ticket)
    let res_lan = rusty_grid_core::transport::parse_p2p_ticket("127.0.0.1:8088");
    assert!(res_lan.is_err(), "Raw LAN address must not parse as P2P ticket");

    // 4. Malformed JSON
    let res_malformed = rusty_grid_core::transport::parse_p2p_ticket("{\"id\": 1234, incomplete");
    assert!(res_malformed.is_err(), "Malformed JSON must fail to parse");

    // 5. Random binary/ASCII garbage
    let res_garbage = rusty_grid_core::transport::parse_p2p_ticket("!@#$%^&*()_+-=~`[]{}|;:,.<>?");
    assert!(res_garbage.is_err(), "Garbage string must fail to parse");
}

#[test]
fn test_adversarial_master_generated_ticket_compatibility() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_master_impl();

    // Start master with P2P enabled
    let ticket = start_master_impl(0, 0, true).expect("Master should start with P2P enabled");
    assert!(!ticket.is_empty(), "Master ticket must not be empty");

    // EMPIRICAL VERIFICATION: Does the ticket generated by start_master_impl
    // parse as a valid iroh::EndpointAddr using rusty_grid_core::transport::parse_p2p_ticket?
    let parse_result = rusty_grid_core::transport::parse_p2p_ticket(&ticket);
    println!("Empirical Check: Master P2P ticket content: {}", ticket);
    println!("Empirical Check: parse_p2p_ticket result: {:?}", parse_result.as_ref().map(|_| "SUCCESS").map_err(|e| e.to_string()));

    // Record whether parse succeeds or fails
    // In start_master_impl:
    // serde_json::json!({
    //     "id": node_id,
    //     "addrs": [
    //         { "type": "relay", "url": "https://relay.n0.iroh.link./" },
    //         { "type": "ip", "addr": format!("0.0.0.0:{}", actual_port) }
    //     ]
    // })
    // We observe the empirical result:
    match &parse_result {
        Ok(addr) => {
            println!("Ticket successfully parsed into iroh::EndpointAddr: {:?}", addr);
        }
        Err(e) => {
            println!("Ticket failed parsing: {}", e);
        }
    }
    assert!(
        parse_result.is_ok(),
        "Master generated P2P ticket must successfully parse into iroh::EndpointAddr: {:?}",
        parse_result.err()
    );

    assert!(stop_master_impl());
}

#[test]
fn test_adversarial_worker_p2p_ticket_sanitization_and_lan_fallback() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_worker_impl();

    // 1. Whitespace ticket should be sanitized and filtered to None
    let started = start_worker_impl(
        "127.0.0.1:59999".to_string(),
        Some("   \t  ".to_string()),
        "SanitizeTest".to_string(),
        2,
        1024,
        false,
        5,
    );
    assert!(started, "Worker should accept sanitized config");
    let status_json = get_worker_status_impl();
    assert!(status_json.contains("status"));

    assert!(stop_worker_impl());
    assert!(!is_running_impl());

    // 2. Completely malformed ticket
    let started_malformed = start_worker_impl(
        "127.0.0.1:59998".to_string(),
        Some("INVALID_P2P_TICKET_PAYLOAD_NOT_JSON".to_string()),
        "MalformedTest".to_string(),
        2,
        1024,
        false,
        5,
    );
    assert!(started_malformed, "Worker should initialize in-process supervisor");

    // Give supervisor loop brief moment to attempt connection and fail gracefully with backoff
    std::thread::sleep(Duration::from_millis(50));

    // Worker supervisor must cleanly shut down without panic or hang
    assert!(stop_worker_impl(), "Worker with malformed ticket must stop cleanly");
    assert!(!is_running_impl());
}

// ==============================================================================
// 3. Rapid Sequential Master/Worker Start/Stop Cycles & Idempotency
// ==============================================================================

#[test]
fn test_adversarial_master_rapid_start_stop_cycles() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_master_impl();

    // 25 rapid start and stop cycles on dynamic port
    for i in 0..25 {
        let ticket = start_master_impl(0, 0, false)
            .unwrap_or_else(|e| panic!("Cycle {i}: Master failed to start: {e}"));
        assert!(!ticket.is_empty(), "Cycle {i}: ticket must not be empty");
        assert!(is_master_running_impl(), "Cycle {i}: master must report running");

        let stopped = stop_master_impl();
        assert!(stopped, "Cycle {i}: stop_master_impl must return true");
        assert!(!is_master_running_impl(), "Cycle {i}: master must report not running");
    }
}

#[test]
fn test_adversarial_worker_rapid_start_stop_cycles() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_worker_impl();

    // 15 rapid start and stop cycles
    for i in 0..15 {
        let started = start_worker_impl(
            "127.0.0.1:58888".to_string(),
            None,
            format!("RapidWorker-{i}"),
            2,
            1024,
            false,
            5,
        );
        assert!(started, "Cycle {i}: Worker failed to start");
        assert!(is_running_impl(), "Cycle {i}: Worker must report running");

        let stopped = stop_worker_impl();
        assert!(stopped, "Cycle {i}: stop_worker_impl must succeed");
        assert!(!is_running_impl(), "Cycle {i}: Worker must report stopped");
    }
}

#[test]
fn test_adversarial_double_start_and_double_stop_idempotency() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_master_impl();
    stop_worker_impl();

    // 1. Double start master
    let t1 = start_master_impl(0, 0, false).expect("First start_master should succeed");
    let t2 = start_master_impl(0, 0, false).expect("Second start_master should be idempotent");
    assert_eq!(t1, t2, "Second start_master call must return identical ticket");
    assert!(is_master_running_impl());

    // 2. Double stop master
    assert!(stop_master_impl(), "First stop_master should return true");
    assert!(stop_master_impl(), "Second stop_master should be idempotent true");
    assert!(!is_master_running_impl());

    // 3. Double start worker
    assert!(start_worker_impl(
        "127.0.0.1:57777".to_string(),
        None,
        "IdempotentWorker".to_string(),
        2,
        1024,
        false,
        5
    ));
    // Second start should return true without spawning duplicate thread
    assert!(start_worker_impl(
        "127.0.0.1:57777".to_string(),
        None,
        "IdempotentWorker".to_string(),
        2,
        1024,
        false,
        5
    ));
    assert!(is_running_impl());

    // 4. Double stop worker
    assert!(stop_worker_impl(), "First stop_worker should return true");
    assert!(stop_worker_impl(), "Second stop_worker should be idempotent true");
    assert!(!is_running_impl());
}

#[test]
fn test_adversarial_port_conflict_handling() {
    let _guard = ADVERSARIAL_TEST_LOCK.blocking_lock();

    stop_master_impl();

    // Occupy a TCP port on 0.0.0.0 intentionally (exact interface that start_master_impl binds)
    let std_listener = std::net::TcpListener::bind("0.0.0.0:0").expect("Must bind temp listener");
    let occupied_port = std_listener.local_addr().expect("Must get local_addr").port() as i32;

    // Attempt to start master coordinator on the occupied port
    let start_result = start_master_impl(occupied_port, 0, false);
    assert!(
        start_result.is_err(),
        "start_master_impl must return Err when port {occupied_port} on 0.0.0.0 is already occupied"
    );

    // Master must not be in running state
    assert!(!is_master_running_impl(), "Master must not report running on bind failure");

    // Free the port
    drop(std_listener);

    // Subsequent start on dynamic port must still succeed cleanly
    let recovery_ticket = start_master_impl(0, 0, false).expect("Master should recover and bind dynamic port");
    assert!(!recovery_ticket.is_empty());
    assert!(is_master_running_impl());

    assert!(stop_master_impl());
}

#[tokio::test]
async fn test_adversarial_coordinator_concurrent_worker_stress() {
    let _guard = ADVERSARIAL_TEST_LOCK.lock().await;

    stop_master_impl();

    // Start master coordinator on dynamic port
    let addr = start_master_impl(0, 0, false).expect("Master coordinator should start");

    // Launch 8 concurrent synthetic workers connecting, registering, pinging, and disconnecting
    let mut tasks = Vec::new();
    for i in 0..8 {
        let master_addr = addr.clone();
        tasks.push(tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(&master_addr)
                .await
                .expect("Synthetic worker should connect");
            let mut transport = MessageTransport::new(stream);
            let worker_id = Uuid::new_v4();

            // 1. Register
            let caps = WorkerCapabilities::new(format!("stress-worker-{i}"), 2, 1024, false, false, None);
            transport
                .send_msg(&WorkerMessage::Register { worker_id, capabilities: caps })
                .await
                .expect("Send Register should succeed");

            let ack = transport.recv_msg::<MasterMessage>().await.expect("Recv ack").expect("Not EOF");
            match ack {
                MasterMessage::RegisterAck { accepted, .. } => assert!(accepted),
                other => panic!("Unexpected ack: {other:?}"),
            }

            // 2. Several rapid heartbeats
            for seq in 0..5 {
                let hb = WorkerMessage::Heartbeat {
                    worker_id,
                    timestamp: 1000 + seq,
                    active_tasks: 0,
                    cpu_usage_pct: 5.0,
                    ram_available_mb: 1024,
                };
                transport.send_msg(&hb).await.expect("Send hb");
                let hb_ack = transport.recv_msg::<MasterMessage>().await.expect("Recv hb_ack").expect("Not EOF");
                match hb_ack {
                    MasterMessage::HeartbeatAck { timestamp } => assert_eq!(timestamp, 1000 + seq),
                    other => panic!("Unexpected hb ack: {other:?}"),
                }
            }

            // 3. Pre-sleep evacuation disconnect
            let disc = WorkerMessage::Disconnecting {
                worker_id,
                reason: "BATTERY_DRAIN".to_string(),
            };
            transport.send_msg(&disc).await.expect("Send disc");
            let disc_ack = transport.recv_msg::<MasterMessage>().await.expect("Recv disc ack").expect("Not EOF");
            match disc_ack {
                MasterMessage::Shutdown { reason, .. } => assert!(reason.contains("pre-sleep evacuation")),
                other => panic!("Unexpected disc ack: {other:?}"),
            }
        }));
    }

    for task in tasks {
        task.await.expect("Synthetic worker task should complete without panic");
    }

    // Give coordinator accept loop brief moment to settle worker count
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(get_master_worker_count_impl(), 0, "All 8 workers should have evacuated");

    assert!(stop_master_impl());
}

#[tokio::test]
async fn test_adversarial_worker_connect_via_master_p2p_ticket() {
    let _guard = ADVERSARIAL_TEST_LOCK.lock().await;

    stop_master_impl();
    stop_worker_impl();

    let ticket = start_master_impl(0, 0, true).expect("Master should start");
    
    // Start worker using this ticket
    let started = start_worker_impl(
        "127.0.0.1:0".to_string(),
        Some(ticket.clone()),
        "P2PWorker".to_string(),
        2,
        1024,
        false,
        2,
    );
    assert!(started, "Worker supervisor should spawn");

    // Settle connection (poll up to 5000ms)
    let mut connected = false;
    for _ in 0..100 {
        if get_master_worker_count_impl() >= 1 {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let worker_count = get_master_worker_count_impl();
    println!("Master connected workers count using genuine P2P ticket: {}", worker_count);
    assert!(
        connected,
        "Worker must successfully connect and register via master genuine P2P ticket, count: {worker_count}"
    );

    assert!(stop_worker_impl());
    assert!(stop_master_impl());
}

#[tokio::test]
async fn test_adversarial_dirty_counter_leak_across_master_restarts() {
    let _guard = ADVERSARIAL_TEST_LOCK.lock().await;

    stop_master_impl();

    let addr1 = start_master_impl(0, 0, false).expect("Start master 1");
    let stream = tokio::net::TcpStream::connect(&addr1).await.expect("Connect");
    let mut transport = MessageTransport::new(stream);
    let worker_id = Uuid::new_v4();
    let caps = WorkerCapabilities::new("leak-worker", 2, 1024, false, false, None);
    transport
        .send_msg(&WorkerMessage::Register { worker_id, capabilities: caps })
        .await
        .expect("Send Register");

    let _ = transport.recv_msg::<MasterMessage>().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 1);

    // Stop master while worker connection socket is still held
    assert!(stop_master_impl());
    drop(transport);

    // Now start master 2 fresh
    let _addr2 = start_master_impl(0, 0, false).expect("Start master 2");

    // EMPIRICAL CHECK: On a fresh master restart, connected_workers MUST be 0!
    let count_after_restart = get_master_worker_count_impl();
    println!("EMPIRICAL CHECK: Master worker count after restart: {}", count_after_restart);

    // If MasterState does not reset connected_workers on start/stop, this leaks dirty state!
    assert_eq!(
        count_after_restart, 0,
        "BUG DETECTED: Master coordinator retained dirty connected_workers count ({count_after_restart}) across server restart!"
    );

    assert!(stop_master_impl());
}

#[tokio::test]
async fn test_adversarial_pre_sleep_evacuation_double_decrement() {
    let _guard = ADVERSARIAL_TEST_LOCK.lock().await;

    stop_master_impl();

    let addr = start_master_impl(0, 0, false).expect("Start master");

    // Connect Worker 1
    let stream1 = tokio::net::TcpStream::connect(&addr).await.expect("Connect w1");
    let mut transport1 = MessageTransport::new(stream1);
    let id1 = Uuid::new_v4();
    transport1
        .send_msg(&WorkerMessage::Register {
            worker_id: id1,
            capabilities: WorkerCapabilities::new("worker-1", 2, 1024, false, false, None),
        })
        .await
        .expect("Send Register 1");
    let _ = transport1.recv_msg::<MasterMessage>().await;

    // Connect Worker 2
    let stream2 = tokio::net::TcpStream::connect(&addr).await.expect("Connect w2");
    let mut transport2 = MessageTransport::new(stream2);
    let id2 = Uuid::new_v4();
    transport2
        .send_msg(&WorkerMessage::Register {
            worker_id: id2,
            capabilities: WorkerCapabilities::new("worker-2", 2, 1024, false, false, None),
        })
        .await
        .expect("Send Register 2");
    let _ = transport2.recv_msg::<MasterMessage>().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(get_master_worker_count_impl(), 2, "Expected 2 connected workers initially");

    // Worker 1 executes pre-sleep evacuation
    transport1
        .send_msg(&WorkerMessage::Disconnecting {
            worker_id: id1,
            reason: "LOW_BATTERY".to_string(),
        })
        .await
        .expect("Send Disconnecting");
    let _ = transport1.recv_msg::<MasterMessage>().await;

    // Drop worker 1 socket
    drop(transport1);
    tokio::time::sleep(Duration::from_millis(80)).await;

    // EMPIRICAL BUG CHECK:
    // Worker 2 is STILL CONNECTED and HEALTHY!
    let count = get_master_worker_count_impl();
    println!("EMPIRICAL CHECK: Worker count after 1 of 2 workers evacuated: {}", count);

    // If double-decrement bug exists in lib.rs lines 495 & 517, count is 0 instead of 1!
    let is_double_decrement_bug = count == 0;
    if is_double_decrement_bug {
        println!("CRITICAL EMPIRICAL FINDING: Double-decrement bug confirmed! Count dropped to 0 while worker 2 remains connected.");
    }

    // Verify worker 2 is still alive and pinging
    transport2
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id: id2,
            timestamp: 12345,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 1024,
        })
        .await
        .expect("Worker 2 heartbeat send");
    let hb_ack = transport2.recv_msg::<MasterMessage>().await.expect("Worker 2 recv hb ack").expect("Not EOF");
    match hb_ack {
        MasterMessage::HeartbeatAck { timestamp } => assert_eq!(timestamp, 12345),
        other => panic!("Unexpected ack for worker 2: {other:?}"),
    }

    assert_eq!(
        count, 1,
        "Double-decrement bug: Remaining connected worker count should be 1, but got {count}!"
    );

    assert!(stop_master_impl());
}



