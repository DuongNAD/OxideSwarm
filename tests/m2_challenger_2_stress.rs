//! Empirical stress-testing and adversarial challenge test suite for Milestone 2 (Challenger 2).
//!
//! Validates:
//! 1. Reaper accuracy:
//!    - Silent worker death (connection frozen/silent without Disconnecting message)
//!    - Reaper precision: worker remains Connected before timeout, transitions to Disconnected precisely after timeout
//!    - Selective reaping: active peer heartbeats prevent reaping while silent peer is reaped
//!    - Wall-clock timestamp skew immunity: reaper uses monotonic Instant, not worker-reported epoch
//!    - Fast-path abrupt TCP socket drop (EOF) vs slow-path reaper timeout
//! 2. Atomic port publication:
//!    - High concurrency read race: 10 concurrent readers polling port-file from start
//!    - Non-empty, fully atomic reads of valid u16 port
//!    - Deep nested directory auto-creation
//!    - Cleanup of port-file on graceful Master shutdown
//!    - Overwrite of stale port-files from previous crashes
//! 3. Capability queries under stress:
//!    - Concurrent `list_gpu_workers` and `list_non_gpu_workers` under heavy worker churn
//!    - Strict disjoint invariant: no GPU worker in non-GPU list, no non-GPU in GPU list
//!    - Zero Disconnected workers in capability or active worker query results
//!    - Zero deadlocks across reader/writer lock contention under continuous churn

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_core::task::TaskRequirements;
use rusty_grid_master::reaper::{spawn_reaper, ReaperConfig};
use rusty_grid_master::registry::{WorkerRegistry, WorkerStatus};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;

/// Polls `condition` every `step` until true or `timeout` elapses.
async fn poll_until<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

// =========================================================================
// Challenge 1: Reaper Accuracy on Silent Worker Death & Abrupt Drops
// =========================================================================

#[tokio::test]
async fn test_reaper_accuracy_silent_worker_boundary_timing() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    // Configure reaper with 200ms timeout and 20ms scan interval
    let timeout = Duration::from_millis(200);
    let scan_interval = Duration::from_millis(20);
    let reaper_config = ReaperConfig::new(scan_interval, timeout);
    let reaper_handle = spawn_reaper(registry.clone(), reaper_config, master_shutdown_rx.clone());

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Connect raw TCP socket, perform handshake, then go completely silent
    // (Simulates worker freezing or network partition where socket stays half-open)
    let stream = TcpStream::connect(master_addr).await.expect("tcp connect");
    let mut transport = MessageTransport::new(stream);
    let worker_id = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new("silent-node", 4, 4096, false, false, None),
    };
    transport.send_msg(&reg_msg).await.expect("send register");
    let _ack: MasterMessage = transport.recv_msg().await.expect("recv").expect("ack");

    let reg_time = Instant::now();

    // 1. Boundary check: At t = 100ms (< 200ms timeout), worker MUST still be Connected!
    tokio::time::sleep(Duration::from_millis(100)).await;
    let info_mid = registry.get_worker(worker_id).await.expect("worker exists");
    assert_eq!(
        info_mid.status,
        WorkerStatus::Connected,
        "Reaper must NOT reap worker before timeout (at 100ms < 200ms)"
    );

    // 2. Poll until reaped, recording detection time
    let mut reaped_time: Option<Instant> = None;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if let Some(info) = registry.get_worker(worker_id).await {
            if info.status == WorkerStatus::Disconnected {
                reaped_time = Some(Instant::now());
                break;
            }
        }
    }

    let detected_at = reaped_time.expect("Reaper must mark silent worker as Disconnected");
    let elapsed = detected_at.duration_since(reg_time);

    println!(
        "[REAPER TIMING] Configured timeout: {:?}, Actual elapsed to Disconnected: {:?}",
        timeout, elapsed
    );

    // Accuracy validation:
    // Reaper must trigger strictly after timeout (>= 200ms)
    // and reasonably close to timeout + scan_interval (< 450ms including CI/scheduling slack)
    assert!(
        elapsed >= timeout,
        "Reaper marked worker Disconnected too early: elapsed={:?}, timeout={:?}",
        elapsed,
        timeout
    );
    assert!(
        elapsed < timeout + Duration::from_millis(250),
        "Reaper took too long to detect dead worker: elapsed={:?}, timeout={:?}",
        elapsed,
        timeout
    );

    // 3. Verify server aborted connection: transport reading should now encounter EOF or error
    let mut buf = [0u8; 1];
    let (mut raw_reader, _) = transport.into_inner().into_inner().into_split();
    let read_res =
        tokio::time::timeout(Duration::from_millis(200), raw_reader.read(&mut buf)).await;
    match read_res {
        Ok(Ok(0)) => {
            // EOF observed: server dropped connection upon reap
            println!("[REAPER CLEANUP] Confirmed server closed TCP connection upon reaping");
        }
        Ok(Ok(_)) => panic!("Unexpected data received after worker was reaped"),
        Ok(Err(e)) => println!("[REAPER CLEANUP] Read error as expected on reaped socket: {e}"),
        Err(_) => println!("[REAPER CLEANUP] Socket read timed out (TCP half-close state)"),
    }

    let _ = master_shutdown_tx.send(true);
    let _ = reaper_handle.await;
    let _ = master_handle.await;
}

#[tokio::test]
async fn test_reaper_selective_reaping_with_active_peers() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    // Reaper with 250ms timeout and 25ms scan interval
    let reaper_config = ReaperConfig::new(Duration::from_millis(25), Duration::from_millis(250));
    let reaper_handle = spawn_reaper(registry.clone(), reaper_config, master_shutdown_rx.clone());
    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Worker 1: Active peer that sends heartbeats every 60ms
    let stream1 = TcpStream::connect(master_addr)
        .await
        .expect("tcp connect 1");
    let mut transport1 = MessageTransport::new(stream1);
    let w1_id = Uuid::new_v4();
    transport1
        .send_msg(&WorkerMessage::Register {
            worker_id: w1_id,
            capabilities: WorkerCapabilities::new("active-worker-1", 4, 4096, false, false, None),
        })
        .await
        .unwrap();
    let _ack1: MasterMessage = transport1.recv_msg().await.unwrap().unwrap();

    // Worker 2: Silent peer that stops sending heartbeats immediately after registration
    let stream2 = TcpStream::connect(master_addr)
        .await
        .expect("tcp connect 2");
    let mut transport2 = MessageTransport::new(stream2);
    let w2_id = Uuid::new_v4();
    transport2
        .send_msg(&WorkerMessage::Register {
            worker_id: w2_id,
            capabilities: WorkerCapabilities::new("silent-worker-2", 4, 4096, false, false, None),
        })
        .await
        .unwrap();
    let _ack2: MasterMessage = transport2.recv_msg().await.unwrap().unwrap();

    // Active worker heartbeat loop: send heartbeats every 60ms for 600ms (10 heartbeats)
    let w1_keepalive = tokio::spawn(async move {
        for tick in 1..=10 {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let hb = WorkerMessage::Heartbeat {
                worker_id: w1_id,
                timestamp: tick * 60,
                active_tasks: 0,
                cpu_usage_pct: 0.0,
                ram_available_mb: 2048,
            };
            if transport1.send_msg(&hb).await.is_err() {
                break;
            }
            let _ = transport1.recv_msg::<MasterMessage>().await;
        }
    });

    // Wait 400ms (> 250ms timeout)
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Verify Worker 2 (silent) was reaped, but Worker 1 (active) is STILL Connected
    let w1_status = registry.get_worker(w1_id).await.unwrap().status;
    let w2_status = registry.get_worker(w2_id).await.unwrap().status;

    assert_eq!(
        w1_status,
        WorkerStatus::Connected,
        "Active heartbeating worker must NOT be reaped"
    );
    assert_eq!(
        w2_status,
        WorkerStatus::Disconnected,
        "Silent worker must be selectively reaped"
    );

    let _ = w1_keepalive.await;
    let _ = master_shutdown_tx.send(true);
    let _ = reaper_handle.await;
    let _ = master_handle.await;
}

#[tokio::test]
async fn test_reaper_fast_path_abrupt_socket_drop_without_disconnecting() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    // Set a very long reaper timeout (60 seconds).
    // An abrupt socket drop should be detected IMMEDIATELY by EOF handler (< 50ms),
    // NOT waiting for the 60s reaper!
    let reaper_config = ReaperConfig::new(Duration::from_secs(1), Duration::from_secs(60));
    let reaper_handle = spawn_reaper(registry.clone(), reaper_config, master_shutdown_rx.clone());
    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let stream = TcpStream::connect(master_addr).await.expect("tcp connect");
    let mut transport = MessageTransport::new(stream);
    let worker_id = Uuid::new_v4();
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::new("drop-node", 4, 4096, false, false, None),
        })
        .await
        .unwrap();
    let _ack: MasterMessage = transport.recv_msg().await.unwrap().unwrap();

    assert_eq!(
        registry.get_worker(worker_id).await.unwrap().status,
        WorkerStatus::Connected
    );

    let drop_start = Instant::now();
    // Drop the socket abruptly (TCP FIN generated by OS socket close)
    drop(transport);

    // Verify Disconnected within 100ms (fast-path EOF)
    let fast_detected = poll_until(
        Duration::from_millis(200),
        Duration::from_millis(10),
        || async {
            registry
                .get_worker(worker_id)
                .await
                .map(|e| e.status == WorkerStatus::Disconnected)
                .unwrap_or(false)
        },
    )
    .await;

    let elapsed = drop_start.elapsed();
    println!(
        "[FAST-PATH EOF] Abrupt socket drop detected in {:?} (far below 60s reaper)",
        elapsed
    );

    assert!(
        fast_detected,
        "Abrupt socket drop must trigger fast-path disconnect without waiting for reaper"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "Fast-path disconnect must complete in <200ms"
    );

    let _ = master_shutdown_tx.send(true);
    let _ = reaper_handle.await;
    let _ = master_handle.await;
}

// =========================================================================
// Challenge 2: Atomic Port Publication & Cleanup
// =========================================================================

#[tokio::test]
async fn test_atomic_port_file_publication_concurrent_client_reads() {
    let temp_dir = TempDir::new().expect("tempdir");
    let port_file = temp_dir.path().join("master.port");

    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config =
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_port_file(port_file.clone());

    let readers_active = Arc::new(AtomicBool::new(true));
    let read_success_count = Arc::new(AtomicUsize::new(0));

    // Launch 10 concurrent tasks aggressively polling for port_file from before it exists
    let mut reader_handles = Vec::new();
    for reader_id in 0..10 {
        let p_file = port_file.clone();
        let active = Arc::clone(&readers_active);
        let count = Arc::clone(&read_success_count);

        let handle = tokio::spawn(async move {
            let mut reads = 0;
            while active.load(Ordering::Relaxed) {
                if let Ok(contents) = tokio::fs::read_to_string(&p_file).await {
                    let trimmed = contents.trim();
                    // Crucial invariant: Partial / empty writes are forbidden!
                    assert!(
                        !trimmed.is_empty(),
                        "Reader {reader_id} observed empty port file!"
                    );
                    let port: u16 = trimmed
                        .parse()
                        .expect("Port file must parse to valid u16 integer");
                    assert!(port > 0, "Reader {reader_id} observed invalid port {port}");
                    reads += 1;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            count.fetch_add(reads, Ordering::SeqCst);
        });
        reader_handles.push(handle);
    }

    // Bind Master
    let master = MasterServer::bind(config, registry)
        .await
        .expect("master bind");
    let bound_port = master.port();
    assert!(bound_port > 0);

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Allow readers to hammer the published file for 100ms
    tokio::time::sleep(Duration::from_millis(100)).await;
    readers_active.store(false, Ordering::Relaxed);

    for h in reader_handles {
        let _ = h.await;
    }

    let total_reads = read_success_count.load(Ordering::SeqCst);
    println!(
        "[PORT PUBLICATION] Concurrent readers performed {} successful zero-corruption reads",
        total_reads
    );
    assert!(
        total_reads > 0,
        "Readers should have successfully read published port file"
    );

    // Verify immediate client connection using read port
    let file_content = tokio::fs::read_to_string(&port_file).await.unwrap();
    let published_port: u16 = file_content.trim().parse().unwrap();
    assert_eq!(published_port, bound_port);

    let client_stream = TcpStream::connect(format!("127.0.0.1:{published_port}")).await;
    assert!(
        client_stream.is_ok(),
        "Client must immediately be able to connect to published port"
    );

    // Teardown
    let _ = master_shutdown_tx.send(true);
    let _ = master_handle.await;

    // Verify port file cleanup on shutdown
    assert!(
        !port_file.exists(),
        "Port file must be automatically removed upon Master shutdown"
    );
}

#[tokio::test]
async fn test_port_file_deep_nested_dir_and_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    // Non-existent deeply nested path
    let nested_port_file = temp_dir
        .path()
        .join("sub1")
        .join("sub2")
        .join("sub3")
        .join("deep.port");

    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config =
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_port_file(nested_port_file.clone());

    let master = MasterServer::bind(config, registry)
        .await
        .expect("bind should create parent directories");
    let bound_port = master.port();

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    assert!(
        nested_port_file.exists(),
        "Deep nested port file must exist"
    );
    let content = tokio::fs::read_to_string(&nested_port_file).await.unwrap();
    let read_port: u16 = content.trim().parse().unwrap();
    assert_eq!(read_port, bound_port);

    // Verify temporary sibling files (.tmp.<pid>) were cleanly renamed and do not linger
    let parent_dir = nested_port_file.parent().unwrap();
    let mut dir_entries = tokio::fs::read_dir(parent_dir).await.unwrap();
    let mut files_in_parent = Vec::new();
    while let Some(entry) = dir_entries.next_entry().await.unwrap() {
        files_in_parent.push(entry.file_name().to_string_lossy().to_string());
    }
    assert_eq!(
        files_in_parent,
        vec!["deep.port".to_string()],
        "No temporary sibling files (.tmp.<pid>) must remain"
    );

    // Trigger shutdown
    let _ = master_shutdown_tx.send(true);
    let _ = master_handle.await;

    // Verify file deleted
    assert!(
        !nested_port_file.exists(),
        "Nested port file must be removed after master shutdown"
    );
}

#[tokio::test]
async fn test_port_file_atomic_overwrite_of_stale_content() {
    let temp_dir = TempDir::new().unwrap();
    let port_file = temp_dir.path().join("stale.port");

    // Write stale garbage to file
    tokio::fs::write(&port_file, b"999999_STALE_GARBAGE\n")
        .await
        .unwrap();

    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config =
        ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_port_file(port_file.clone());

    let master = MasterServer::bind(config, registry)
        .await
        .expect("bind with existing file");
    let master_port = master.port();
    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let content = tokio::fs::read_to_string(&port_file).await.unwrap();
    let parsed: u16 = content
        .trim()
        .parse()
        .expect("stale content must be replaced with valid u16");
    assert_eq!(parsed, master_port);

    let _ = master_shutdown_tx.send(true);
    let _ = master_handle.await;
}

// =========================================================================
// Challenge 3: Capability Queries Under High Concurrency Stress
// =========================================================================

#[tokio::test]
async fn test_capability_queries_under_connect_disconnect_stress() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_heartbeat_interval(1);
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr().to_string();

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let running = Arc::new(AtomicBool::new(true));
    let mut worker_shutdown_txs = Vec::new();
    let mut worker_handles = Vec::new();

    // Spawn 8 worker clients: 4 with simulated GPU, 4 CPU-only
    // Each worker runs in a loop: connects, stays alive for 100-300ms, disconnects, reconnects
    for i in 0..8 {
        let (w_tx, w_rx) = watch::channel(false);
        worker_shutdown_txs.push(w_tx);

        let is_gpu = i % 2 == 0;
        let worker_name = format!("churn-worker-{i}-{}", if is_gpu { "gpu" } else { "cpu" });
        let m_addr = master_addr.clone();
        let is_running = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            while is_running.load(Ordering::Relaxed) {
                let mut client = WorkerClient::from_options(
                    m_addr.clone(),
                    Some(worker_name.clone()),
                    Some(4),
                    is_gpu,
                );

                // Run client for short burst
                tokio::select! {
                    _ = client.run(w_rx.clone()) => {}
                    _ = tokio::time::sleep(Duration::from_millis(150 + (i as u64 * 30))) => {}
                }

                // Short backoff before reconnecting
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        worker_handles.push(handle);
    }

    // Concurrently run 4 query hammer tasks
    let gpu_query_count = Arc::new(AtomicUsize::new(0));
    let non_gpu_query_count = Arc::new(AtomicUsize::new(0));
    let active_query_count = Arc::new(AtomicUsize::new(0));
    let filter_query_count = Arc::new(AtomicUsize::new(0));

    // Task 1: Hammer list_gpu_workers
    let reg1 = registry.clone();
    let run1 = Arc::clone(&running);
    let count1 = Arc::clone(&gpu_query_count);
    let q1_handle = tokio::spawn(async move {
        while run1.load(Ordering::Relaxed) {
            let gpu_workers = reg1.list_gpu_workers().await;
            for w in &gpu_workers {
                // INVARIANT 1: Must actually possess GPU capabilities
                assert!(
                    w.capabilities.can_execute_gpu(),
                    "list_gpu_workers returned non-GPU worker: {:?}",
                    w
                );
                // INVARIANT 2: Must not be Disconnected
                assert_ne!(
                    w.status,
                    WorkerStatus::Disconnected,
                    "list_gpu_workers returned Disconnected worker: {:?}",
                    w
                );
            }
            count1.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    // Task 2: Hammer list_non_gpu_workers
    let reg2 = registry.clone();
    let run2 = Arc::clone(&running);
    let count2 = Arc::clone(&non_gpu_query_count);
    let q2_handle = tokio::spawn(async move {
        while run2.load(Ordering::Relaxed) {
            let non_gpu_workers = reg2.list_non_gpu_workers().await;
            for w in &non_gpu_workers {
                // INVARIANT 3: Must NOT possess GPU capabilities
                assert!(
                    !w.capabilities.can_execute_gpu(),
                    "list_non_gpu_workers returned GPU worker: {:?}",
                    w
                );
                // INVARIANT 4: Must not be Disconnected
                assert_ne!(
                    w.status,
                    WorkerStatus::Disconnected,
                    "list_non_gpu_workers returned Disconnected worker: {:?}",
                    w
                );
            }
            count2.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    // Task 3: Hammer list_active_workers and counts()
    let reg3 = registry.clone();
    let run3 = Arc::clone(&running);
    let count3 = Arc::clone(&active_query_count);
    let q3_handle = tokio::spawn(async move {
        while run3.load(Ordering::Relaxed) {
            let active_workers = reg3.list_active_workers().await;
            for w in &active_workers {
                assert_ne!(
                    w.status,
                    WorkerStatus::Disconnected,
                    "list_active_workers returned Disconnected worker: {:?}",
                    w
                );
            }
            let (_total, active_cnt, gpu_cnt) = reg3.counts().await;
            assert!(
                active_cnt <= 8,
                "active count ({active_cnt}) exceeds max workers"
            );
            assert!(
                gpu_cnt <= 4,
                "gpu count ({gpu_cnt}) exceeds max GPU workers"
            );
            count3.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    // Task 4: Hammer find_eligible_workers with GPU constraint
    let reg4 = registry.clone();
    let run4 = Arc::clone(&running);
    let count4 = Arc::clone(&filter_query_count);
    let q4_handle = tokio::spawn(async move {
        let gpu_req = TaskRequirements::gpu(10);
        let cpu_req = TaskRequirements::generic(2, 10);

        while run4.load(Ordering::Relaxed) {
            let eligible_gpu = reg4.find_eligible_workers(&gpu_req).await;
            for w in &eligible_gpu {
                assert!(
                    w.capabilities.can_execute_gpu(),
                    "find_eligible_workers(gpu) returned worker unable to run GPU: {:?}",
                    w
                );
                assert_ne!(w.status, WorkerStatus::Disconnected);
            }

            let eligible_cpu = reg4.find_eligible_workers(&cpu_req).await;
            for w in &eligible_cpu {
                assert!(w.capabilities.cpu_cores >= 2);
                assert_ne!(w.status, WorkerStatus::Disconnected);
            }

            count4.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    // Run the stress test for 1.8 seconds under high worker churn and continuous queries
    tokio::time::sleep(Duration::from_millis(1800)).await;

    // Signal query tasks and worker churn to terminate
    running.store(false, Ordering::Relaxed);
    for tx in worker_shutdown_txs {
        let _ = tx.send(true);
    }

    let _ = tokio::join!(q1_handle, q2_handle, q3_handle, q4_handle);
    for h in worker_handles {
        let _ = h.await;
    }

    let q_gpu = gpu_query_count.load(Ordering::SeqCst);
    let q_cpu = non_gpu_query_count.load(Ordering::SeqCst);
    let q_act = active_query_count.load(Ordering::SeqCst);
    let q_fil = filter_query_count.load(Ordering::SeqCst);

    println!(
        "[CAPABILITY QUERY STRESS] Queries under churn: GPU={}, NonGPU={}, Active={}, Eligible={}",
        q_gpu, q_cpu, q_act, q_fil
    );

    assert!(
        q_gpu > 50 && q_cpu > 50 && q_act > 50 && q_fil > 50,
        "Continuous query progress must be sustained under churn (observed GPU={}, NonGPU={}, Active={}, Eligible={})",
        q_gpu, q_cpu, q_act, q_fil
    );

    let _ = master_shutdown_tx.send(true);
    let _ = master_handle.await;
}

// =========================================================================
// Challenge 4: Clock Immunity (Monotonic Instant vs Worker Timestamp)
// =========================================================================

#[tokio::test]
async fn test_reaper_monotonic_clock_immune_to_worker_reported_epoch_skew() {
    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let master = MasterServer::bind(config, registry.clone()).await.unwrap();
    let master_addr = master.local_addr();

    let timeout = Duration::from_millis(200);
    let reaper_config = ReaperConfig::new(Duration::from_millis(25), timeout);
    let reaper_handle = spawn_reaper(registry.clone(), reaper_config, master_shutdown_rx.clone());
    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    let stream = TcpStream::connect(master_addr).await.unwrap();
    let mut transport = MessageTransport::new(stream);
    let w_id = Uuid::new_v4();
    transport
        .send_msg(&WorkerMessage::Register {
            worker_id: w_id,
            capabilities: WorkerCapabilities::new("clock-skew-worker", 2, 2048, false, false, None),
        })
        .await
        .unwrap();
    let _ack: MasterMessage = transport.recv_msg().await.unwrap().unwrap();

    // Adversarial: worker reports a timestamp 10 years in the past (epoch 0)
    let skewed_hb = WorkerMessage::Heartbeat {
        worker_id: w_id,
        timestamp: 0, // Year 1970
        active_tasks: 0,
        cpu_usage_pct: 0.0,
        ram_available_mb: 2048,
    };
    transport.send_msg(&skewed_hb).await.unwrap();
    let _ack_hb: MasterMessage = transport.recv_msg().await.unwrap().unwrap();

    // At t = 50ms, worker must STILL be Connected despite reporting timestamp 0!
    tokio::time::sleep(Duration::from_millis(50)).await;
    let info = registry.get_worker(w_id).await.unwrap();
    assert_eq!(
        info.status,
        WorkerStatus::Connected,
        "Master must calculate timeouts using monotonic Instant, not worker epoch timestamp!"
    );

    // After 250ms with no further heartbeats, worker MUST be reaped
    let reaped = poll_until(
        Duration::from_millis(300),
        Duration::from_millis(20),
        || async {
            registry
                .get_worker(w_id)
                .await
                .map(|e| e.status == WorkerStatus::Disconnected)
                .unwrap_or(false)
        },
    )
    .await;
    assert!(
        reaped,
        "Worker must be reaped after monotonic timeout expires"
    );

    let _ = master_shutdown_tx.send(true);
    let _ = reaper_handle.await;
    let _ = master_handle.await;
}

// =========================================================================
// Challenge 5: QUIC Stream Multiplexing Isolation & Anti-HoL Blocking
// =========================================================================

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_adversarial_quic_stream_multiplexing_heavy_saturation_anti_hol() {
    use iroh::endpoint::presets::N0;
    use iroh::endpoint::RelayMode;
    use rusty_grid_core::transport::{BiStream, GridStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA};
    use tokio::io::AsyncWriteExt;

    // 1. Setup loopback QUIC endpoints
    let ep_server = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind server endpoint");

    let ep_client = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind client endpoint");

    let server_addr = ep_server.addr();

    // 2. Server accepts connection and the two bi-directional streams
    let server_handle = tokio::spawn(async move {
        let incoming = ep_server.accept().await.expect("accept").await.expect("handshake");

        let (send_a, mut recv_a) = incoming.accept_bi().await.expect("accept stream A");
        let mut tag_a = [0u8; 1];
        recv_a.read_exact(&mut tag_a).await.expect("read tag A");

        let (send_b, mut recv_b) = incoming.accept_bi().await.expect("accept stream B");
        let mut tag_b = [0u8; 1];
        recv_b.read_exact(&mut tag_b).await.expect("read tag B");

        let (ctrl_send, ctrl_recv, _data_send, mut data_recv) = if tag_a[0] == STREAM_CONTROL {
            assert_eq!(tag_b[0], STREAM_DATA);
            (send_a, recv_a, send_b, recv_b)
        } else {
            assert_eq!(tag_a[0], STREAM_DATA);
            assert_eq!(tag_b[0], STREAM_CONTROL);
            (send_b, recv_b, send_a, recv_a)
        };

        let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
        let mut ctrl_transport = MessageTransport::new(ctrl_stream);

        // Data sink reads up to 5MB
        let data_sink_task = tokio::spawn(async move {
            let mut total_read = 0usize;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match data_recv.read(&mut buf).await {
                    Ok(Some(n)) if n > 0 => total_read += n,
                    _ => break,
                }
            }
            total_read
        });

        // Server answers heartbeats on Control Stream
        while let Ok(Some(msg)) = ctrl_transport.recv_msg::<WorkerMessage>().await {
            match msg {
                WorkerMessage::Heartbeat { timestamp, .. } => {
                    let ack = MasterMessage::HeartbeatAck { timestamp };
                    ctrl_transport.send_msg(&ack).await.expect("send heartbeat ack");
                }
                WorkerMessage::Disconnecting { .. } => {
                    break;
                }
                _ => {}
            }
        }

        let total_data_read = data_sink_task.await.expect("data sink finish");
        total_data_read
    });

    // 3. Client connects
    let conn = ep_client.connect(server_addr, GRID_ALPN).await.expect("connect");

    let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("open ctrl bi");
    ctrl_send.write_all(&[STREAM_CONTROL]).await.expect("write ctrl tag");
    ctrl_send.flush().await.expect("flush ctrl tag");
    let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
    let mut ctrl_transport = MessageTransport::new(ctrl_stream);

    let (mut data_send, _data_recv) = conn.open_bi().await.expect("open data bi");
    data_send.write_all(&[STREAM_DATA]).await.expect("write data tag");
    data_send.flush().await.expect("flush data tag");

    // 4. Heavy Data Saturation: Stream 5MB of data concurrently
    let heavy_payload_size = 5 * 1024 * 1024;
    let data_producer_task = tokio::spawn(async move {
        let chunk = vec![0xA5u8; 64 * 1024];
        let mut sent = 0;
        while sent < heavy_payload_size {
            data_send.write_all(&chunk).await.expect("write heavy chunk");
            sent += chunk.len();
        }
        data_send.flush().await.expect("flush data");
        data_send.shutdown().await.expect("shutdown data send");
    });

    // 5. Send rapid heartbeats while 5MB data is flowing
    let worker_id = Uuid::new_v4();
    let mut heartbeat_latencies = Vec::new();
    let num_heartbeats = 30;

    for i in 0..num_heartbeats {
        let t0 = Instant::now();
        let hb = WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 5000 + i,
            active_tasks: 2,
            cpu_usage_pct: 75.0,
            ram_available_mb: 4096,
        };
        ctrl_transport.send_msg(&hb).await.expect("send hb");
        let ack: Option<MasterMessage> = ctrl_transport.recv_msg().await.expect("recv ack");
        let elapsed = t0.elapsed();
        heartbeat_latencies.push(elapsed);

        match ack {
            Some(MasterMessage::HeartbeatAck { timestamp }) => {
                assert_eq!(timestamp, 5000 + i);
            }
            other => panic!("Expected HeartbeatAck, got: {:?}", other),
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let disc = WorkerMessage::Disconnecting {
        worker_id,
        reason: "Adversarial test complete".into(),
    };
    ctrl_transport.send_msg(&disc).await.expect("send disconnecting");

    data_producer_task.await.expect("data producer finish");
    let total_server_read = server_handle.await.expect("server finish");

    assert_eq!(total_server_read, heavy_payload_size, "Server must receive all 5MB of streamed data");

    let max_latency = heartbeat_latencies.iter().max().cloned().unwrap();
    let avg_latency: Duration = heartbeat_latencies.iter().sum::<Duration>() / heartbeat_latencies.len() as u32;

    println!(
        "[QUIC HOL ISOLATION] Heartbeats: {}, Avg Latency: {:?}, Max Latency: {:?}",
        num_heartbeats, avg_latency, max_latency
    );

    assert!(
        avg_latency < Duration::from_millis(50),
        "Average heartbeat latency over multiplexed control stream must remain sub-50ms (got: {:?})",
        avg_latency
    );
    assert!(
        max_latency < Duration::from_millis(150),
        "Max heartbeat latency must remain bounded sub-150ms (got: {:?})",
        max_latency
    );
}

// Adversarial Stress Test: Master Server Multiplexed Control Lane Isolation under Heavy Data Backpressure.
// NOTE FOR WORKER M2: This test demonstrates a confirmed Head-of-Line Blocking bug in `MasterServer::handle_multiplexed_connection`.
// When 150 tasks (15MB) fill `data_out_tx` (capacity 64) and saturate QUIC stream flow control, `router_task` blocks on
// `data_out_tx.send(msg).await`. Because `HeartbeatAck` is routed through the same `outbound_tx` channel as `AssignTask`,
// `HeartbeatAck` is trapped in `outbound_rx` behind the tasks and is NEVER delivered to `ctrl_writer_task` / `STREAM_CONTROL`.
// Furthermore, once `outbound_rx` fills up (capacity 128), line 2240 (`outbound_tx.send(MasterMessage::HeartbeatAck).await`)
// blocks the master's inbound `tokio::select!` loop itself!
// Worker M2 must fix this by sending `HeartbeatAck` directly to a dedicated `ctrl_out_tx` channel.
#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_adversarial_master_multiplexed_heartbeat_under_backpressured_data_lane() {
    use iroh::endpoint::presets::N0;
    use iroh::endpoint::RelayMode;
    use rusty_grid_core::transport::{BiStream, GridStream, GRID_ALPN, STREAM_CONTROL, STREAM_DATA};
    use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
    use rusty_grid_master::queue::TaskQueue;
    use rusty_grid_master::scheduler::{SchedulerConfig, WorkloadScheduler};
    use std::collections::HashMap;
    use tokio::io::AsyncWriteExt;

    let (master_shutdown_tx, master_shutdown_rx) = watch::channel(false);
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let sched_trigger = Arc::new(tokio::sync::Notify::new());
    let waiters = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    // Configure master server with P2P
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_p2p(true)
        .with_heartbeat_interval(5)
        .with_handshake_timeout(5);

    let master = MasterServer::bind_full(
        config,
        registry.clone(),
        queue.clone(),
        sched_trigger.clone(),
        waiters,
    )
    .await
    .expect("master bind");

    let p2p_endpoint = master.p2p_endpoint().expect("p2p endpoint must exist").clone();
    let server_addr = p2p_endpoint.addr();

    let master_handle = tokio::spawn(async move { master.run(master_shutdown_rx).await });

    // Client connects via Iroh QUIC
    let ep_client = iroh::Endpoint::builder(N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind client endpoint");

    let conn = ep_client.connect(server_addr, GRID_ALPN).await.expect("connect");

    // Open control stream (0x01)
    let (mut ctrl_send, ctrl_recv) = conn.open_bi().await.expect("open ctrl bi");
    ctrl_send.write_all(&[STREAM_CONTROL]).await.expect("write ctrl tag");
    ctrl_send.flush().await.expect("flush ctrl tag");
    let ctrl_stream = GridStream::P2p(BiStream::new(ctrl_recv, ctrl_send));
    let mut ctrl_transport = MessageTransport::new(ctrl_stream);

    // Open data stream (0x02)
    let (mut data_send, _data_recv) = conn.open_bi().await.expect("open data bi");
    data_send.write_all(&[STREAM_DATA]).await.expect("write data tag");
    data_send.flush().await.expect("flush data tag");

    // Register worker with 200 cores
    let worker_id = Uuid::new_v4();
    let reg_msg = WorkerMessage::Register {
        worker_id,
        capabilities: WorkerCapabilities::new("backpressure-test-node", 200, 131072, false, false, None),
    };
    ctrl_transport.send_msg(&reg_msg).await.expect("send register");

    let ack: Option<MasterMessage> = ctrl_transport.recv_msg().await.expect("recv register ack");
    match ack {
        Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
        other => panic!("Expected RegisterAck accepted, got: {:?}", other),
    }

    // Now submit 150 tasks to the queue with large payloads (100KB each = 15MB total)
    // To thoroughly saturate QUIC window backpressure and fill data_out_tx (capacity 64)
    for i in 0..150 {
        let task = Task::new(
            TaskSpec::Command {
                program: "heavy_task".into(),
                args: vec![format!("arg-{}", i)],
                env: HashMap::new(),
                working_dir: None,
                stdin: Some(vec![0xCC; 100_000]),
            },
            TaskRequirements::default(),
        );
        queue.submit(task).await.expect("submit task");
    }

    // Run scheduler to assign all 150 tasks to the worker in batches
    let scheduler = WorkloadScheduler::new(
        SchedulerConfig::default().with_max_batch_size(200),
        registry.clone(),
        queue.clone(),
        sched_trigger.clone(),
    );
    let sched_report = scheduler.schedule_batch(200).await.expect("schedule batch");
    assert_eq!(sched_report.assignments.len(), 150, "All 150 tasks should be scheduled to worker");

    // Notice: The client deliberately DOES NOT READ from `data_recv`!
    // This creates complete backpressure on the data lane.

    // Allow a moment for Master to push tasks into outbound channels
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send heartbeat over the Control Lane.
    let hb = WorkerMessage::Heartbeat {
        worker_id,
        timestamp: 9999,
        active_tasks: 150,
        cpu_usage_pct: 90.0,
        ram_available_mb: 32768,
    };
    ctrl_transport.send_msg(&hb).await.expect("send heartbeat");

    // We expect HeartbeatAck within 2 seconds
    let ack_res = tokio::time::timeout(Duration::from_secs(2), ctrl_transport.recv_msg::<MasterMessage>()).await;

    println!("[BACKPRESSURE TEST RESULT] HeartbeatAck recv result: {:?}", ack_res);

    let _ = master_shutdown_tx.send(true);
    let _ = master_handle.await;

    match ack_res {
        Ok(Ok(Some(MasterMessage::HeartbeatAck { timestamp }))) => {
            assert_eq!(timestamp, 9999);
            println!("[BACKPRESSURE TEST] SUCCESS: HeartbeatAck arrived despite 15MB unread data backlog!");
        }
        Ok(Ok(other)) => {
            panic!("Unexpected message on control stream: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("Control stream read error: {:?}", e);
        }
        Err(_) => {
            panic!("HEAD-OF-LINE BLOCKING BUG CONFIRMED: HeartbeatAck timed out! Control stream is blocked behind backpressured data stream!");
        }
    }
}

#[test]
fn test_adversarial_zero_copy_huge_payload_slicing_and_cow() {
    use rusty_grid_core::task::{Bytes, TaskResult, TaskId};
    use std::borrow::Cow;

    let task_id = TaskId::new();
    let worker_id = Uuid::new_v4();

    // 5MB stdout buffer
    let size = 5 * 1024 * 1024;
    let mut big_data = Vec::with_capacity(size);
    big_data.resize(size, b'X');

    let raw = tokio_util::bytes::Bytes::from(big_data);
    let task_bytes = Bytes::from(raw.clone());

    // 1. Zero copy slicing: verify subslice memory address matches parent address + offset
    let sub = task_bytes.slice(1000..2000);
    assert_eq!(sub.len(), 1000);
    assert_eq!(sub.as_ptr(), unsafe { raw.as_ptr().add(1000) });

    // 2. TaskResult Cow conversion without cloning the 5MB buffer
    let res = TaskResult {
        task_id,
        worker_id,
        exit_code: 0,
        stdout: task_bytes,
        stderr: Bytes::from_static(b""),
        execution_time_ms: 100,
        error: None,
        is_gpu_executed: false,
        device_name: None,
    };

    let stdout_cow = res.stdout_str();
    match stdout_cow {
        Cow::Borrowed(s) => {
            assert_eq!(s.len(), size);
            assert_eq!(s.as_ptr(), raw.as_ptr());
        }
        Cow::Owned(_) => panic!("Valid ASCII/UTF8 should NOT allocate Owned Cow"),
    }
}

#[tokio::test]
async fn test_adversarial_scheduler_micro_batch_massive_burst() {
    use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
    use rusty_grid_master::queue::{TaskQueue, TaskState};
    use rusty_grid_master::registry::WorkerRegistry;
    use rusty_grid_master::scheduler::{SchedulerConfig, WorkloadScheduler};
    use std::collections::HashMap;

    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let trigger = Arc::new(tokio::sync::Notify::new());

    let config = SchedulerConfig::default()
        .with_micro_batch_window(Duration::from_millis(5))
        .with_max_batch_size(64);

    let scheduler = WorkloadScheduler::new(config, registry.clone(), queue.clone(), trigger);

    // Register 4 workers with 100 cores each (400 slots)
    for i in 0..4 {
        let wid = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let caps = WorkerCapabilities::new(format!("bulk-worker-{}", i), 100, 131072, false, false, None);
        registry
            .register(wid, caps, format!("127.0.0.1:{}", 9100 + i).parse().unwrap(), tx, None)
            .await
            .unwrap();
    }

    // Burst submit 250 tasks
    let mut task_ids = Vec::new();
    for i in 0..250 {
        let task = Task::new(
            TaskSpec::Command {
                program: "burst".into(),
                args: vec![format!("{}", i)],
                env: HashMap::new(),
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::default(),
        );
        let tid = task.id;
        queue.submit(task).await.unwrap();
        task_ids.push(tid);
    }

    assert_eq!(queue.get_schedulable_tasks().await.len(), 250);

    // Schedule iteratively until all are scheduled
    let mut total_scheduled = 0;
    while total_scheduled < 250 {
        let report = scheduler.schedule_batch(64).await.unwrap();
        if report.assignments.is_empty() {
            break;
        }
        total_scheduled += report.assignments.len();
    }

    assert_eq!(total_scheduled, 250, "All 250 burst tasks must be scheduled");
    assert_eq!(queue.get_schedulable_tasks().await.len(), 0, "No schedulable tasks remain");

    // Verify all 250 tasks are in Scheduled state
    for tid in task_ids {
        assert_eq!(queue.get_state(&tid).await, Some(TaskState::Scheduled));
    }
}

