//! Integration tests for `SCALE-1` simulated worker execution, network shaping, and
//! 100-worker fleet burst & reconnect convergence.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_master::{MasterServer, ServerConfig};
use rusty_grid_worker::{
    BackoffConfig, FakeRunnerConfig, LinkLossAction, RunnerConfig, SandboxConfig,
    SimulatedLinkConfig, SimulatedLinkStream, SimulatedNetworkControl, TaskRunner, WorkerClient,
    EXIT_CODE_CANCELLED, EXIT_CODE_SUCCESS, EXIT_CODE_TIMEOUT,
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

fn builtin_task(should_fail: bool, timeout_secs: u64) -> Task {
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: "scale1-sim".to_string(),
            iterations: 1,
            duration_ms: 0,
            should_fail,
            require_gpu: false,
        },
        TaskRequirements::generic(1, timeout_secs),
    )
}

#[tokio::test]
async fn fake_runner_handles_delay_callbacks_cancellation_timeout_and_zero_disk_io() {
    let tmp = TempDir::new().expect("tempdir");
    let absent_sandbox = tmp.path().join("never_created_sandboxes");

    let shared_delay_nanos = Arc::new(AtomicU64::new(Duration::from_millis(55).as_nanos() as u64));
    let started_ids: Arc<Mutex<Vec<TaskId>>> = Arc::new(Mutex::new(Vec::new()));
    let started_ref = Arc::clone(&started_ids);

    let fake = FakeRunnerConfig::new(Duration::from_millis(5))
        .with_shared_delay_nanos(Arc::clone(&shared_delay_nanos))
        .with_stdout(b"sim-stdout\n".to_vec())
        .with_on_task_started(move |task_id| {
            started_ref.lock().expect("started_ids").push(task_id);
        });

    let mut sandbox_cfg = SandboxConfig::default();
    sandbox_cfg.base_dir = absent_sandbox.clone();
    let runner_cfg = RunnerConfig::new(sandbox_cfg).with_fake_runner(Some(fake));
    let caps = WorkerCapabilities::detect(Some("fake-runner-unit".into()), Some(4), false);
    let runner = TaskRunner::new(Uuid::new_v4(), caps, runner_cfg);

    // 1. Normal delayed run triggers on_task_started and avoids creating any sandbox directory.
    let task_ok = builtin_task(false, 30);
    let t0 = Instant::now();
    let res_ok = runner
        .execute_task_with_checkpoint(&task_ok, None, None, None, None)
        .await;
    let elapsed = t0.elapsed();
    assert_eq!(res_ok.exit_code, EXIT_CODE_SUCCESS);
    assert_eq!(res_ok.stdout, "sim-stdout\n");
    assert!(
        elapsed >= Duration::from_millis(40),
        "expected >=40ms simulated delay, got {elapsed:?}"
    );
    assert_eq!(*started_ids.lock().expect("started_ids"), vec![task_ok.id]);
    assert!(
        !absent_sandbox.exists(),
        "fake runner must not create sandbox directories for inputless tasks"
    );

    // 2. Dynamic delay update + immediate cancellation returns EXIT_CODE_CANCELLED.
    shared_delay_nanos.store(Duration::from_secs(5).as_nanos() as u64, Ordering::Relaxed);
    let task_cancel = builtin_task(false, 30);
    let (cancel_tx, cancel_rx) = oneshot::channel::<String>();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let _ = cancel_tx.send("stop-test".to_string());
    });
    let res_cancel = runner
        .execute_task_with_checkpoint(&task_cancel, None, None, None, Some(cancel_rx))
        .await;
    assert_eq!(res_cancel.exit_code, EXIT_CODE_CANCELLED);
    assert!(
        res_cancel
            .error
            .as_deref()
            .is_some_and(|e| e.contains("Cancelled: stop-test")),
        "unexpected cancel error: {:?}",
        res_cancel.error
    );

    // 3. Task timeout while fake delay exceeds timeout_secs returns EXIT_CODE_TIMEOUT.
    let task_timeout = builtin_task(false, 1);
    let res_timeout = runner
        .execute_task_with_checkpoint(&task_timeout, None, None, None, None)
        .await;
    assert_eq!(res_timeout.exit_code, EXIT_CODE_TIMEOUT);
    assert!(!absent_sandbox.exists());
}

#[tokio::test]
async fn simulated_link_applies_latency_fifo_jitter_and_impairments() {
    // 1. One-way latency + jitter must preserve strict FIFO byte/frame order.
    let net = SimulatedNetworkControl::new();
    let (client_raw, mut server_raw) = tokio::io::duplex(64 * 1024);
    let link_cfg = SimulatedLinkConfig::default()
        .with_base_latency(Duration::from_millis(25))
        .with_jitter(Duration::from_millis(20))
        .with_seed(42)
        .with_control(net.clone());
    let mut wrapped = SimulatedLinkStream::wrap(client_raw, link_cfg);

    let frames: Vec<[u8; 8]> = (0u64..10).map(|i| i.to_be_bytes()).collect();
    let writer = tokio::spawn(async move {
        for frame in &frames {
            wrapped.write_all(frame).await.expect("write frame");
        }
        wrapped.flush().await.expect("flush wrapped");
        wrapped
    });

    let t0 = Instant::now();
    let mut received = Vec::with_capacity(80);
    let mut buf = [0u8; 80];
    while received.len() < 80 {
        let n = server_raw.read(&mut buf).await.expect("read server_raw");
        assert!(n > 0, "unexpected early EOF");
        received.extend_from_slice(&buf[..n]);
    }
    let elapsed = t0.elapsed();
    let _wrapped = writer.await.expect("writer join");
    assert!(
        elapsed >= Duration::from_millis(20),
        "expected >=20ms base latency, got {elapsed:?}"
    );
    for (idx, chunk) in received.chunks_exact(8).enumerate() {
        let val = u64::from_be_bytes(chunk.try_into().expect("8 bytes"));
        assert_eq!(val, idx as u64, "FIFO order broken at index {idx}");
    }
    assert_eq!(net.tx_bytes(), 80);

    // 2. RetransmitStall injects stall_delay and increments loss_events without losing bytes.
    let net_stall = SimulatedNetworkControl::new();
    let (client_raw, mut server_raw) = tokio::io::duplex(16 * 1024);
    let stall_cfg = SimulatedLinkConfig::default()
        .with_loss_probability(1.0)
        .with_loss_action(LinkLossAction::RetransmitStall)
        .with_stall_delay(Duration::from_millis(45))
        .with_control(net_stall.clone());
    let mut wrapped = SimulatedLinkStream::wrap(client_raw, stall_cfg);

    let t0 = Instant::now();
    let payload = b"retransmit-ok";
    wrapped.write_all(payload).await.expect("write stall");
    wrapped.flush().await.expect("flush stall");
    let mut got = vec![0u8; payload.len()];
    server_raw.read_exact(&mut got).await.expect("read stall");
    assert_eq!(&got, payload);
    assert!(t0.elapsed() >= Duration::from_millis(35));
    assert!(net_stall.loss_events() >= 1);

    // 3. DropConnection terminates the wrapped stream on loss.
    let net_drop = SimulatedNetworkControl::new();
    let (client_raw, _server_raw) = tokio::io::duplex(16 * 1024);
    let drop_cfg = SimulatedLinkConfig::default()
        .with_loss_probability(1.0)
        .with_loss_action(LinkLossAction::DropConnection)
        .with_control(net_drop.clone());
    let mut wrapped_drop = SimulatedLinkStream::wrap(client_raw, drop_cfg);
    let _ = wrapped_drop.write_all(b"boom").await;
    let flush_res = wrapped_drop.flush().await;
    assert_eq!(
        flush_res.unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
    assert!(net_drop.loss_events() >= 1);

    // 4. disconnect_all() immediately fails an idle blocked reader.
    let net_ctrl = SimulatedNetworkControl::new();
    let (client_raw, _server_raw) = tokio::io::duplex(16 * 1024);
    let mut wrapped_idle = SimulatedLinkStream::wrap(
        client_raw,
        SimulatedLinkConfig::default().with_control(net_ctrl.clone()),
    );
    let disconnect_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        net_ctrl.disconnect_all();
    });
    let mut read_buf = [0u8; 16];
    let read_res = tokio::time::timeout(Duration::from_secs(2), wrapped_idle.read(&mut read_buf))
        .await
        .expect("disconnect_all did not wake reader promptly");
    assert_eq!(read_res.unwrap_err().kind(), io::ErrorKind::ConnectionReset);
    disconnect_task.await.expect("disconnect task");
}

#[tokio::test]
async fn hundred_simulated_workers_execute_burst_survive_mass_disconnect_and_recover() {
    const WORKERS: usize = 100;
    const SUBMITTERS: usize = 4;
    const TASKS_PER_SUBMITTER: usize = 50;

    let server_cfg = ServerConfig::from_addr("127.0.0.1:0")
        .expect("valid loopback addr")
        .with_web_ui(false)
        .with_implicit_local_dashboard(false)
        .with_discovery(false)
        .with_heartbeat_interval(1)
        .with_max_connections(512)
        .with_max_pending_handshakes(256)
        .with_max_terminal_tasks(1000)
        .with_session_resume_hold(Duration::ZERO);
    let master = MasterServer::spawn(server_cfg).await.expect("spawn master");
    let master_addr = master.server_addr();
    let master_addr_str = master_addr.to_string();

    let net = SimulatedNetworkControl::new();
    let started_count = Arc::new(AtomicUsize::new(0));
    let link_cfg = SimulatedLinkConfig::default().with_control(net.clone());

    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker_handles = Vec::with_capacity(WORKERS);
    for i in 0..WORKERS {
        let counter = Arc::clone(&started_count);
        let fake =
            FakeRunnerConfig::new(Duration::from_millis(5)).with_on_task_started(move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        let mut client = WorkerClient::simulated(
            master_addr_str.clone(),
            Some(format!("sim-{i:03}")),
            Some(4),
            fake,
            Some(link_cfg.clone()),
        );
        client.set_backoff_config(
            BackoffConfig::default()
                .with_initial(Duration::from_millis(50))
                .with_max(Duration::from_millis(250)),
        );
        let rx = worker_shutdown_rx.clone();
        worker_handles.push(tokio::spawn(async move { client.run(rx).await }));
    }

    // 1. Wait for all 100 workers to register cleanly.
    let reg_deadline = Instant::now() + Duration::from_secs(20);
    let mut query = MessageTransport::new(
        TcpStream::connect(master_addr)
            .await
            .expect("connect status client"),
    );
    loop {
        if net.active_sessions() == WORKERS as u64 {
            query
                .send_msg(&ClientMessage::ClusterStatus)
                .await
                .expect("send status");
            if let Ok(Some(ClientResponse::ClusterStatus { workers, .. })) =
                query.recv_msg::<ClientResponse>().await
            {
                if workers.len() == WORKERS {
                    break;
                }
            }
        }
        assert!(
            Instant::now() < reg_deadline,
            "timed out waiting for 100 simulated workers (active_sessions={})",
            net.active_sessions()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 2. Submit 200 concurrent tasks across 4 submitters.
    let mut submitter_handles = Vec::with_capacity(SUBMITTERS);
    for (s_idx, _) in (0..SUBMITTERS).enumerate() {
        let addr = master_addr;
        submitter_handles.push(tokio::spawn(async move {
            let mut transport =
                MessageTransport::new(TcpStream::connect(addr).await.expect("connect submitter"));
            for t_idx in 0..TASKS_PER_SUBMITTER {
                let task = builtin_task(false, 30);
                transport
                    .send_msg(&ClientMessage::SubmitTask { task, wait: true })
                    .await
                    .expect("submit");
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    transport.recv_msg::<ClientResponse>(),
                )
                .await
                {
                    Ok(Ok(Some(ClientResponse::TaskCompleted { result, .. }))) => {
                        assert_eq!(result.exit_code, EXIT_CODE_SUCCESS);
                    }
                    other => panic!("submitter {s_idx} task {t_idx} failed/timed out: {other:?}"),
                }
            }
        }));
    }
    for h in submitter_handles {
        if let Err(e) = h.await {
            query.send_msg(&ClientMessage::ClusterStatus).await.ok();
            let status = query.recv_msg::<ClientResponse>().await;
            panic!(
                "submitter burst failed: {e:?}; started={}, net={:?}, status={status:?}",
                started_count.load(Ordering::Relaxed),
                net.snapshot()
            );
        }
    }
    assert_eq!(
        started_count.load(Ordering::Relaxed),
        SUBMITTERS * TASKS_PER_SUBMITTER
    );
    assert!(net.rx_bytes() > 0 && net.tx_bytes() > 0);

    // 3. Inject fleet-wide disconnect and verify full reconnection convergence.
    let hs_before = net.total_handshakes();
    assert!(hs_before >= WORKERS as u64);
    let reconnected = Arc::new(AtomicBool::new(false));
    net.disconnect_all();

    let recon_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if net.total_handshakes() >= hs_before + WORKERS as u64
            && net.active_sessions() == WORKERS as u64
        {
            query
                .send_msg(&ClientMessage::ClusterStatus)
                .await
                .expect("send status post-disconnect");
            if let Ok(Some(ClientResponse::ClusterStatus { workers, .. })) =
                query.recv_msg::<ClientResponse>().await
            {
                if workers.len() == WORKERS {
                    reconnected.store(true, Ordering::Release);
                    break;
                }
            }
        }
        assert!(
            Instant::now() < recon_deadline,
            "timed out waiting for reconnect convergence (handshakes={}, before={}, active={})",
            net.total_handshakes(),
            hs_before,
            net.active_sessions()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(reconnected.load(Ordering::Acquire));

    // 4. Verify 50 post-reconnect tasks complete cleanly.
    let mut post_handles = Vec::with_capacity(5);
    for _ in 0..5 {
        let addr = master_addr;
        post_handles.push(tokio::spawn(async move {
            let mut transport =
                MessageTransport::new(TcpStream::connect(addr).await.expect("connect post"));
            for _ in 0..10 {
                let task = builtin_task(false, 30);
                transport
                    .send_msg(&ClientMessage::SubmitTask { task, wait: true })
                    .await
                    .expect("submit post");
                match tokio::time::timeout(
                    Duration::from_secs(15),
                    transport.recv_msg::<ClientResponse>(),
                )
                .await
                .expect("post submitter recv timeout")
                .expect("recv post")
                .expect("post response present")
                {
                    ClientResponse::TaskCompleted { result, .. } => {
                        assert_eq!(result.exit_code, EXIT_CODE_SUCCESS);
                    }
                    other => panic!("unexpected post response: {other:?}"),
                }
            }
        }));
    }
    for h in post_handles {
        h.await.expect("post-reconnect submitter");
    }

    let _ = worker_shutdown_tx.send(true);
    for h in worker_handles {
        tokio::time::timeout(Duration::from_secs(15), h)
            .await
            .expect("worker shutdown timeout")
            .expect("worker task join")
            .expect("worker run result");
    }
    master.shutdown().expect("shutdown master");
}
