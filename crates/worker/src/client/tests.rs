use super::*;
use rusty_grid_core::protocol::PROTOCOL_VERSION_MIN_COMPATIBLE;
use rusty_grid_core::task::TaskRequirements;

#[test]
fn test_worker_config_builder() {
    let id = Uuid::new_v4();
    let config = WorkerConfig::new("127.0.0.1:8088")
        .with_worker_id(id)
        .with_name("test-node")
        .with_cores(16)
        .with_simulate_gpu(true)
        .with_handshake_timeout(Duration::from_secs(10))
        .with_sandbox_base_dir(PathBuf::from("/tmp/custom_sb"))
        .with_keep_sandboxes(true)
        .with_max_output_bytes(5 * 1024 * 1024)
        .with_tags(vec!["fast".into(), "gpu".into()]);

    assert_eq!(config.master_addr, "127.0.0.1:8088");
    assert_eq!(config.worker_id, Some(id));
    assert_eq!(config.name.as_deref(), Some("test-node"));
    assert_eq!(config.cores, Some(16));
    assert!(config.simulate_gpu);
    assert_eq!(
        config.sandbox_base_dir,
        Some(PathBuf::from("/tmp/custom_sb"))
    );
    assert!(config.keep_sandboxes);
    assert_eq!(config.max_output_bytes, 5 * 1024 * 1024);
    assert_eq!(config.tags.len(), 2);
}

// ---- GPU advertising: `--gpu` is a request, only an engine that works is a claim ----------
// (These read `RUSTY_GRID_ENABLE_PHYSICAL_GPU` through the policy; run them without it set.)

fn gpu_test_client(config: WorkerConfig) -> WorkerClient {
    WorkerClient::new(
        config
            .with_name("gpu-policy-test")
            .with_cores(2)
            .with_no_battery(true),
    )
}

#[test]
fn plain_worker_advertises_no_gpu_and_does_not_look_for_one() {
    // The library default: a test cluster on a laptop must not grow GPU workers, even in a
    // build that has the wgpu engine and a GPU in the machine.
    let client = gpu_test_client(WorkerConfig::new("127.0.0.1:9999"));
    assert!(!client.capabilities().can_execute_gpu());
    assert!(client.capabilities().gpu_device_name.is_none());
    assert!(client.gpu_notices().is_empty());
}

#[test]
fn gpu_flag_advertises_a_gpu_only_if_this_binary_can_use_one() {
    let client = gpu_test_client(WorkerConfig::new("127.0.0.1:9999").with_gpu(true));
    let caps = client.capabilities();
    assert!(!caps.is_simulated_gpu, "--gpu never means the simulator");
    assert_eq!(caps.has_gpu, caps.can_execute_gpu());
    if !crate::gpu::WGPU_COMPILED {
        // Used to be: has_gpu = true, name "Physical GPU (Manual Override)", tasks run on the CPU.
        assert!(!caps.has_gpu);
        assert!(caps.gpu_device_name.is_none());
        let notices = client.gpu_notices();
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert_eq!(notices[0].level, NoticeLevel::Warn);
        assert!(
            notices[0].message.contains("--features gpu-wgpu"),
            "{}",
            notices[0].message
        );
    } else if caps.has_gpu {
        let name = caps.gpu_device_name.as_deref().expect("the adapter's name");
        assert_ne!(name, "Physical GPU (Manual Override)");
        assert!(client.gpu_notices()[0].message.contains(name));
    } else {
        assert_eq!(client.gpu_notices()[0].level, NoticeLevel::Warn);
    }
}

#[test]
fn gpu_autodetect_follows_the_same_rule_and_is_quiet_when_nothing_was_insisted_on() {
    let client = gpu_test_client(WorkerConfig::new("127.0.0.1:9999").with_gpu_autodetect(true));
    let caps = client.capabilities();
    assert!(!caps.is_simulated_gpu);
    assert_eq!(caps.has_gpu, caps.can_execute_gpu());
    if !crate::gpu::WGPU_COMPILED {
        assert!(!caps.has_gpu);
    }
    // Without --gpu nothing is a warning: it just says what happened.
    assert!(client
        .gpu_notices()
        .iter()
        .all(|n| n.level == NoticeLevel::Info));
    assert_eq!(client.gpu_notices().len(), 1);
}

#[test]
fn no_gpu_and_simulate_gpu_are_unaffected_by_the_engine() {
    let none = gpu_test_client(
        WorkerConfig::new("127.0.0.1:9999")
            .with_gpu(true)
            .with_gpu_autodetect(true)
            .with_no_gpu(true),
    );
    assert!(!none.capabilities().can_execute_gpu());

    let simulated = gpu_test_client(
        WorkerConfig::new("127.0.0.1:9999")
            .with_simulate_gpu(true)
            .with_gpu_autodetect(true),
    );
    let caps = simulated.capabilities();
    assert!(caps.has_gpu && caps.is_simulated_gpu);
    assert_eq!(
        caps.gpu_device_name.as_deref(),
        Some("Simulated Virtual GPU (Matrix Engine)")
    );
    assert!(simulated.gpu_notices()[0].message.contains("SIMULATED"));
}

#[test]
fn test_worker_config_cluster_secret_and_connect_timeout() {
    let default = WorkerConfig::new("127.0.0.1:8088");
    assert!(default.cluster_secret.is_none());
    assert_eq!(default.connect_timeout, DEFAULT_CONNECT_TIMEOUT);

    let secret = ClusterSecret::from_bytes(&[7u8; 32]).unwrap();
    let config = WorkerConfig::new("127.0.0.1:8088")
        .with_cluster_secret(secret.clone())
        .with_connect_timeout(Duration::from_secs(2));
    assert_eq!(config.cluster_secret.as_ref(), Some(&secret));
    assert_eq!(config.connect_timeout, Duration::from_secs(2));
    // Debug output (e.g. in logs) never reveals the secret.
    assert!(!format!("{config:?}").contains(&secret.to_hex()));
}

#[tokio::test]
async fn test_shutdown_requested_resolves_on_flag_or_dropped_sender_only() {
    // Pending while the flag is false and the sender is alive.
    let (tx, mut rx) = watch::channel(false);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), shutdown_requested(&mut rx))
            .await
            .is_err()
    );

    // Resolves when the flag flips ...
    let waiter = tokio::spawn(async move {
        shutdown_requested(&mut rx).await;
    });
    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("flag must wake the waiter")
        .unwrap();

    // ... when it was already set before waiting, and when the sender is gone.
    let (tx, mut rx) = watch::channel(true);
    shutdown_requested(&mut rx).await;
    drop(tx);
    let (tx, mut rx) = watch::channel(false);
    drop(tx);
    shutdown_requested(&mut rx).await;
}

#[test]
fn test_worker_client_initialization_preserves_id_and_capabilities() {
    let id = Uuid::new_v4();
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_worker_id(id)
        .with_name("gpu-worker")
        .with_cores(8)
        .with_simulate_gpu(true)
        .with_no_battery(true);

    let client = WorkerClient::new(config);
    assert_eq!(client.worker_id(), id);
    assert_eq!(client.capabilities().cpu_cores, 8);
    assert!(client.capabilities().can_execute_gpu());
    assert!(client.capabilities().is_simulated_gpu);
    assert!(client.capabilities().mobile.is_none());
    assert_eq!(client.concurrency_semaphore().available_permits(), 8);
}

#[test]
fn test_worker_config_no_battery_disables_host_battery_probe() {
    #[derive(Default)]
    struct WarnVisitor(String);
    impl tracing::field::Visit for WarnVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.0.push_str(value);
            }
        }
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                use std::fmt::Write;
                let _ = write!(&mut self.0, "{value:?}");
            }
        }
    }
    struct WarnCollector {
        warnings: Arc<std::sync::Mutex<Vec<String>>>,
        thread_id: std::thread::ThreadId,
    }
    impl tracing::Subscriber for WarnCollector {
        fn register_callsite(
            &self,
            metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            if *metadata.level() <= tracing::Level::WARN {
                tracing::subscriber::Interest::always()
            } else {
                tracing::subscriber::Interest::never()
            }
        }
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::WARN
        }
        fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
            Some(tracing::metadata::LevelFilter::WARN)
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN
                && std::thread::current().id() == self.thread_id
            {
                let mut v = WarnVisitor::default();
                event.record(&mut v);
                self.warnings.lock().unwrap().push(v.0);
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    let warnings: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let thread_id = std::thread::current().id();
    let dispatch = tracing::Dispatch::new(WarnCollector {
        warnings: warnings.clone(),
        thread_id,
    });
    // Keep a second `Dispatch` alive in `tracing_core`'s global `LOCKED_DISPATCHERS` registry so
    // `DISPATCHERS.has_just_one` is `false`. Otherwise, in `tracing-core 0.1.x`, if another test
    // thread hits the `no_battery` callsite for the first time while only one scoped `Dispatch`
    // exists, `DefaultCallsite::register` uses `Rebuilder::JustOne` (`dispatcher::get_default` on
    // that other thread, which is `NoSubscriber`) and caches `Interest::never()`.
    let _second_dispatch = tracing::Dispatch::new(WarnCollector {
        warnings: warnings.clone(),
        thread_id,
    });

    tracing::dispatcher::with_default(&dispatch, || {
        rusty_grid_core::capabilities::with_test_battery_probe((Some(10), Some(false)), || {
            tracing::callsite::rebuild_interest_cache();
            let default_cfg = WorkerConfig::new("127.0.0.1:9999");
            assert!(!default_cfg.no_battery);
            let default_client = WorkerClient::new(default_cfg);
            assert!(
                    !warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w.contains("Host battery and thermal protection is disabled (no_battery=true)")),
                    "without with_no_battery(true), WorkerClient::new must not emit the no_battery warning"
                );
            assert_eq!(
                    default_client
                        .capabilities()
                        .mobile
                        .as_ref()
                        .and_then(|m| m.battery_pct),
                    Some(10),
                    "without with_no_battery(true), WorkerClient::new must observe the 10 % battery probe"
                );
            assert_eq!(
                    default_client
                        .mobile_telemetry()
                        .snapshot()
                        .and_then(|s| s.battery_pct),
                    Some(10),
                    "without with_no_battery(true), a 10 % discharging battery probe must populate mobile_telemetry()"
                );

            let from_opts_client =
                WorkerClient::from_options("127.0.0.1:9999", Some("opts".into()), Some(2), false);
            assert_eq!(
                from_opts_client
                    .capabilities()
                    .mobile
                    .as_ref()
                    .and_then(|m| m.battery_pct),
                Some(10),
                "WorkerClient::from_options must keep host battery protection enabled"
            );

            warnings.lock().unwrap().clear();
            tracing::callsite::rebuild_interest_cache();
            let no_bat_cfg = WorkerConfig::new("127.0.0.1:9999").with_no_battery(true);
            assert!(no_bat_cfg.no_battery);
            let client = WorkerClient::new(no_bat_cfg);
            assert!(
                    warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w.contains("Host battery and thermal protection is disabled (no_battery=true)")),
                    "with_no_battery(true) must emit a tracing::warn! when WorkerClient::new initializes, got: {:?}",
                    *warnings.lock().unwrap()
                );
            assert!(
                    client.capabilities().mobile.is_none(),
                    "with_no_battery(true) must suppress host battery detection even when the battery probe reports 10 %"
                );
            assert!(
                    client.mobile_telemetry().snapshot().is_none(),
                    "with_no_battery(true) must leave mobile telemetry handle empty unless fed externally"
                );

            let testing_client =
                WorkerClient::for_testing("127.0.0.1:9999", Some("test".into()), Some(2), false);
            assert!(
                testing_client.capabilities().mobile.is_none(),
                "WorkerClient::for_testing must suppress host battery detection"
            );

            warnings.lock().unwrap().clear();
            tracing::callsite::rebuild_interest_cache();
            let _ = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true));
            assert!(
                    !warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w.contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                    "without with_p2p_minimal_preset(true), WorkerClient::new must not emit the p2p_minimal_preset warning"
                );

            warnings.lock().unwrap().clear();
            tracing::callsite::rebuild_interest_cache();
            let min_cfg = WorkerConfig::new("127.0.0.1:9999")
                .with_no_battery(true)
                .with_p2p_minimal_preset(true);
            assert!(min_cfg.p2p_minimal_preset);
            let _ = WorkerClient::new(min_cfg);
            assert!(
                    warnings
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|w| w.contains("P2P minimal preset is enabled (p2p_minimal_preset=true)")),
                    "with_p2p_minimal_preset(true) must emit a tracing::warn! when WorkerClient::new initializes, got: {:?}",
                    *warnings.lock().unwrap()
                );
        });
    });

    let client_source = include_str!("../client.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    assert!(
            !client_source
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .any(|l| l.contains("RUSTY_GRID_P2P_MINIMAL")),
            "crates/worker/src/client.rs library code must not read RUSTY_GRID_P2P_MINIMAL from the environment"
        );
}

/// Design 0021: a task is refused before anything touches the sandbox base when its private
/// root was replaced (a link stands in for another user's directory), so the reclaim of a
/// leftover sandbox never runs behind the link.
#[cfg(unix)]
#[tokio::test]
async fn test_assign_refuses_a_task_before_touching_a_replaced_sandbox_root() {
    let temp = tempfile::tempdir().unwrap();
    let temp_str = temp.path().to_str().unwrap().to_string();
    let sandbox_cfg = crate::sandbox::SandboxConfig::from_env(
        |name| (name == "TMPDIR").then(|| temp_str.clone()),
        PathBuf::from("/unused/platform/tmp"),
    );
    let root = sandbox_cfg.base_dir.parent().unwrap().to_path_buf();
    let elsewhere = temp.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &root).unwrap();

    let mut client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_cores(2)
            .with_no_battery(true),
    );
    client.sandbox_base_dir = sandbox_cfg.base_dir.clone();
    client.runner = Arc::new(TaskRunner::new(
        client.worker_id(),
        client.capabilities().clone(),
        crate::runner::RunnerConfig::new(sandbox_cfg),
    ));

    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("echo", vec!["x".into()]),
        TaskRequirements::generic(1, 10),
    );
    // A leftover sandbox of this task id, reached through the link.
    let leftover = elsewhere.join("sandboxes").join(task.id.to_string());
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("keep"), b"x").unwrap();

    let (tx, mut rx) = mpsc::channel(16);
    client.handle_assign_task(task.clone(), tx).await;
    match rx.recv().await.expect("a result for the refused task") {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            error,
            ..
        } => {
            assert_eq!(task_id, task.id);
            assert_eq!(exit_code, EXIT_CODE_GENERAL_ERROR);
            let error = error.unwrap_or_default();
            assert!(error.contains("symbolic link"), "{error}");
        }
        other => panic!("expected the refusal as a TaskResult, got {other:?}"),
    }
    assert!(
        leftover.join("keep").exists(),
        "the reclaim must not run behind a replaced root"
    );
}

#[tokio::test]
async fn test_worker_client_handle_assign_and_cancel_task() {
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(4)
        .with_no_battery(true);
    let client = WorkerClient::new(config);
    let (tx, mut rx) = mpsc::channel(16);

    // Sleep for 60s
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 30),
    );
    let task_id = task.id;

    client.handle_assign_task(task, tx).await;

    // Verify task progress message emitted
    let progress = rx.recv().await.expect("progress message");
    match progress {
        WorkerMessage::TaskProgress {
            task_id: pid,
            status,
            ..
        } => {
            assert_eq!(pid, task_id);
            assert_eq!(status, TaskStatus::Running);
        }
        other => panic!("Expected TaskProgress, got {other:?}"),
    }

    // Cancel the task
    client
        .handle_cancel_task(task_id, Some("test cancellation".into()))
        .await;

    // Verify task result emitted with cancelled exit code
    let result = rx.recv().await.expect("result message");
    match result {
        WorkerMessage::TaskResult {
            task_id: rid,
            exit_code,
            error,
            ..
        } => {
            assert_eq!(rid, task_id);
            assert_eq!(exit_code, crate::runner::EXIT_CODE_CANCELLED);
            assert_eq!(exit_code, 130);
            assert!(error.unwrap().contains("test cancellation"));
        }
        other => panic!("Expected TaskResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_assign_task_that_was_cancelled_before_it_arrived_is_refused_not_run() {
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(4)
        .with_no_battery(true);
    let client = WorkerClient::new(config);
    let (tx, mut rx) = mpsc::channel(16);
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["60".into()]),
        TaskRequirements::generic(1, 30),
    );
    let task_id = task.id;

    // The Master's CancelTask overtakes the task's AssignTask.
    client
        .handle_cancel_task(task_id, Some("client cancelled".into()))
        .await;
    client.handle_assign_task(task, tx.clone()).await;

    match rx.recv().await.expect("a cancelled result, not progress") {
        WorkerMessage::TaskResult {
            task_id: rid,
            exit_code,
            ..
        } => {
            assert_eq!(rid, task_id);
            assert_eq!(exit_code, crate::runner::EXIT_CODE_CANCELLED);
        }
        other => panic!("Expected a cancelled TaskResult, got {other:?}"),
    }
    assert!(
        client.active_task_table.read().await.is_empty(),
        "the cancelled task must not have been started"
    );

    // The memory is per cancel: a later, legitimate assignment of the same id runs.
    let again = Task::new(
        rusty_grid_core::task::TaskSpec::command("true", vec![]),
        TaskRequirements::generic(1, 30),
    );
    let mut again = again;
    again.id = task_id;
    client.handle_assign_task(again, tx).await;
    match rx.recv().await.expect("progress of the second run") {
        WorkerMessage::TaskProgress { task_id: pid, .. } => assert_eq!(pid, task_id),
        other => panic!("Expected TaskProgress, got {other:?}"),
    }
}

#[tokio::test]
async fn test_worker_client_concurrency_bounded_by_cores() {
    let config = WorkerConfig::new("127.0.0.1:9999")
        .with_cores(2)
        .with_no_battery(true);
    let client = WorkerClient::new(config);
    assert_eq!(client.concurrency_semaphore().available_permits(), 2);
    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);

    let (tx, mut rx) = mpsc::channel(16);

    // Spawn 2 tasks that take permits
    let t1 = Task::new(
        rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(1, 10),
    );
    let t2 = Task::new(
        rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(1, 10),
    );

    client.handle_assign_task(t1, tx.clone()).await;
    client.handle_assign_task(t2, tx.clone()).await;

    // Drain messages
    let mut progress_count = 0;
    let mut result_count = 0;

    for _ in 0..4 {
        match rx.recv().await.expect("message") {
            WorkerMessage::TaskProgress { .. } => progress_count += 1,
            WorkerMessage::TaskResult { .. } => result_count += 1,
            _ => {}
        }
    }

    assert_eq!(progress_count, 2);
    assert_eq!(result_count, 2);

    // Give a moment for ActiveTaskGuard drop tasks to execute
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.concurrency_semaphore().available_permits(), 2);
    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);
}

#[tokio::test]
async fn test_worker_client_mobile_battery_rejection() {
    let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true));
    // Inject low battery mobile capabilities. The gate reads the live handle, not the
    // snapshot detected at startup, so that is where a platform app (or the poller) writes.
    client
        .mobile_telemetry()
        .set(Some(rusty_grid_core::capabilities::MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 3".into(),
            battery_pct: Some(10),    // Under 15%
            is_charging: Some(false), // Discharging
            thermal_throttled: false,
        }));

    let (tx, mut rx) = mpsc::channel(16);
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(1, 10),
    );
    let task_id = task.id;

    client.handle_assign_task(task, tx).await;

    let msg = rx.recv().await.expect("result message");
    match msg {
        WorkerMessage::TaskResult {
            task_id: rid,
            exit_code,
            error,
            ..
        } => {
            assert_eq!(rid, task_id);
            assert_eq!(exit_code, 1);
            assert!(error.unwrap().to_lowercase().contains("battery"));
        }
        other => panic!("Expected TaskResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_worker_client_backoff_reset_on_register_ack() {
    let (client_io, master_io) = tokio::io::duplex(4096);
    let config = WorkerConfig::new("127.0.0.1:9999").with_no_battery(true);
    let client = WorkerClient::new(config);
    let worker_id = client.worker_id();

    // Simulate master receiving Register and responding with RegisterAck(accepted: true)
    tokio::spawn(async move {
        let mut master_transport = MessageTransport::new(master_io);
        let msg = master_transport
            .recv_msg::<WorkerMessage>()
            .await
            .unwrap()
            .unwrap();
        match msg {
            WorkerMessage::Register { worker_id: wid, .. } => {
                assert_eq!(wid, worker_id);
                master_transport
                    .send_msg(&MasterMessage::RegisterAck {
                        accepted: true,
                        worker_id,
                        heartbeat_interval_secs: 3,
                        message: None,
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::all_supported(),
                    })
                    .await
                    .unwrap();
            }
            other => panic!("Expected Register, got {other:?}"),
        }
    });

    let mut client_transport = MessageTransport::new(client_io);
    let mut backoff = ExponentialBackoff::new(BackoffConfig::default());
    // Simulate previous reconnection failures incrementing backoff attempt
    backoff.next_delay();
    backoff.next_delay();
    assert_eq!(backoff.attempt(), 2);

    let res = client.perform_handshake(&mut client_transport).await;
    assert!(res.is_ok());
    backoff.reset();
    assert_eq!(backoff.attempt(), 0);
}

#[tokio::test]
async fn test_worker_refuses_register_ack_with_protocol_version_1() {
    let (client_io, master_io) = tokio::io::duplex(4096);
    let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true));
    let worker_id = client.worker_id();

    tokio::spawn(async move {
        let mut master_transport = MessageTransport::new(master_io);
        let _ = master_transport
            .recv_msg::<WorkerMessage>()
            .await
            .unwrap()
            .unwrap();
        master_transport
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: 3,
                message: None,
                protocol_version: 1,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
    });

    let mut client_transport = MessageTransport::new(client_io);
    let err = client
        .perform_handshake(&mut client_transport)
        .await
        .expect_err("worker must refuse a version 1 RegisterAck");
    let msg = err.to_string();
    assert!(
        msg.contains("1") && msg.contains(&PROTOCOL_VERSION_MIN_COMPATIBLE.to_string()),
        "{msg}"
    );

    // Also verify that a future Master protocol version (> PROTOCOL_VERSION_CURRENT) is refused.
    let (client_io2, server_io2) = tokio::io::duplex(4096);
    let future_ver = PROTOCOL_VERSION_CURRENT + 5;
    tokio::spawn(async move {
        let mut server_transport = MessageTransport::new(server_io2);
        let _ = server_transport
            .recv_msg::<WorkerMessage>()
            .await
            .unwrap()
            .unwrap();
        server_transport
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: 3,
                message: None,
                protocol_version: future_ver,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
    });
    let mut client_transport2 = MessageTransport::new(client_io2);
    let err2 = client
        .perform_handshake(&mut client_transport2)
        .await
        .expect_err("worker must refuse a higher incompatible RegisterAck version");
    let msg2 = err2.to_string();
    assert!(
        msg2.contains(&future_ver.to_string())
            && msg2.contains(&PROTOCOL_VERSION_CURRENT.to_string()),
        "{msg2}"
    );
}

#[tokio::test]
async fn test_register_ack_feature_bits_gate_worker_capabilities() {
    use rusty_grid_core::blob::{BlobId, InputSource, TaskInput};
    use rusty_grid_core::task::{StagedFile, TaskRequirements, TaskSpec};

    let config = WorkerConfig::new("127.0.0.1:9999").with_no_battery(true);
    let client = WorkerClient::new(config);
    let worker_id = client.worker_id();

    // Master accepts registration and announces mobile-telemetry and blobs in `message`,
    // but clears FILE_STAGING, BLOB_TRANSFER, MOBILE_TELEMETRY, and CHECKPOINT_DELTA in `features`.
    let ack_features = ProtocolCapabilities::AEAD_ENCRYPTION;
    let (client_io, server_io) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut server_transport = MessageTransport::new(server_io);
        let _ = server_transport
            .recv_msg::<WorkerMessage>()
            .await
            .unwrap()
            .unwrap();
        server_transport
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: 3,
                message: Some("features=mobile-telemetry,blobs".into()),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ack_features,
            })
            .await
            .unwrap();
    });

    let mut client_transport = MessageTransport::new(client_io);
    client
        .perform_handshake(&mut client_transport)
        .await
        .expect("handshake should succeed");

    assert_eq!(client.negotiated_features(), ack_features);
    assert!(
        !client.master_takes_mobile_telemetry.load(Ordering::Acquire),
        "MOBILE_TELEMETRY must stay disabled when RegisterAck.features omits it"
    );
    assert!(
        !client.master_has_blobs.load(Ordering::Acquire),
        "BLOB_TRANSFER must stay disabled when RegisterAck.features omits it"
    );

    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);

    // 1. A task with staged_files must be refused when FILE_STAGING is not in negotiated_features.
    let mut staged_task = Task::new(
        TaskSpec::command("echo", vec!["hi".into()]),
        TaskRequirements::generic(1, 10),
    );
    staged_task.staged_files.push(StagedFile {
        path: "input.txt".into(),
        size: 2,
        sha256: [0u8; 32],
        executable: false,
        data: rusty_grid_core::task::Bytes::from_static(b"hi"),
    });
    client
        .handle_assign_task(staged_task.clone(), outbound_tx.clone())
        .await;
    match outbound_rx
        .recv()
        .await
        .expect("TaskResult for staged_task")
    {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            stderr,
            ..
        } => {
            assert_eq!(task_id, staged_task.id);
            assert_ne!(exit_code, 0);
            let stderr_str = String::from_utf8_lossy(stderr.as_ref());
            assert!(
                stderr_str.contains("FILE_STAGING"),
                "expected FILE_STAGING refusal, got: {stderr_str}"
            );
        }
        other => panic!("expected TaskResult, got {other:?}"),
    }

    // 2. A task with blob inputs must be refused when BLOB_TRANSFER is not in negotiated_features.
    let mut blob_task = Task::new(
        TaskSpec::command("echo", vec!["hi".into()]),
        TaskRequirements::generic(1, 10),
    );
    blob_task.inputs.push(TaskInput {
        path: "blob.bin".into(),
        source: InputSource::File {
            id: BlobId([1u8; 32]),
            size: 4,
            executable: false,
        },
    });
    client
        .handle_assign_task(blob_task.clone(), outbound_tx)
        .await;
    match outbound_rx.recv().await.expect("TaskResult for blob_task") {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            stderr,
            ..
        } => {
            assert_eq!(task_id, blob_task.id);
            assert_ne!(exit_code, 0);
            let stderr_str = String::from_utf8_lossy(stderr.as_ref());
            assert!(
                stderr_str.contains("BLOB_TRANSFER"),
                "expected BLOB_TRANSFER refusal, got: {stderr_str}"
            );
        }
        other => panic!("expected TaskResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_worker_redirection_portal() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Hold port 18080 (if not already held by another process) so the test proves it never
    // depends on a fixed port being free.
    let _occupied_18080 = tokio::net::TcpListener::bind("127.0.0.1:18080").await.ok();

    let (addr, shutdown_tx) = portal_fixture(
        PortalTarget::from("192.0.2.20:8088"),
        PORTAL_MAX_CONNECTIONS,
    )
    .await;

    // 1. Test browser navigation / redirect
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("307 Temporary Redirect"));
    assert!(resp.contains("Location: http://192.0.2.20:8080"));

    // 2. Test /api/status probe
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("200 OK"));
    assert!(resp.contains("\"role\":\"WORKER_PORTAL\""));
    assert!(resp.contains("http://192.0.2.20:8080"));

    let _ = shutdown_tx.send(true);
}

// ------------------------------------------------------------------------------------------
// Session lifecycle: task cleanup, duplicate ids, stale sandboxes
// ------------------------------------------------------------------------------------------

/// True while a process with `pid` exists (signal 0 only probes).
#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Waits for the `Running` progress message of `task_id`.
async fn wait_running(rx: &mut mpsc::Receiver<WorkerMessage>, task_id: TaskId) {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskProgress {
                task_id: id,
                status: TaskStatus::Running,
                ..
            })) if id == task_id => return,
            Ok(Some(_)) => {}
            other => panic!("task {task_id} never started running: {other:?}"),
        }
    }
}

fn test_client(dir: &std::path::Path, cores: usize) -> WorkerClient {
    WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_cores(cores)
            .with_sandbox_base_dir(dir)
            .with_no_battery(true),
    )
}

#[tokio::test]
async fn test_reassigned_task_id_stops_the_old_run_silently_and_the_new_run_owns_the_slot() {
    let dir = tempfile::tempdir().unwrap();
    let client = test_client(dir.path(), 2);
    let (tx, mut rx) = mpsc::channel(32);

    let mut task = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 60),
    );
    let task_id = task.id;
    client.handle_assign_task(task.clone(), tx.clone()).await;
    wait_running(&mut rx, task_id).await;

    // The same id again (a re-dispatch): the old run must be gone before the new one starts,
    // otherwise both fight over `<sandbox>/<task id>`.
    task.spec = rusty_grid_core::task::TaskSpec::command("echo", vec!["second".into()]);
    client.handle_assign_task(task.clone(), tx.clone()).await;

    let mut results = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(800), rx.recv()).await {
        if let WorkerMessage::TaskResult {
            exit_code,
            stdout,
            error,
            ..
        } = msg
        {
            results.push((
                exit_code,
                String::from_utf8_lossy(&stdout.0).to_string(),
                error,
            ));
        }
    }
    assert_eq!(
            results.len(),
            1,
            "exactly one result: the new run's. The superseded run must not report a cancellation (the Master would record the task as cancelled): {results:?}"
        );
    assert_eq!(
        results[0].0, 0,
        "the re-dispatch must run, not fail: {results:?}"
    );
    assert!(results[0].1.contains("second"));

    // The old run's cleanup cannot have removed anything of the new run, and both are gone now.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(client.active_task_table().read().await.is_empty());
    assert_eq!(client.concurrency_semaphore().available_permits(), 2);
}

#[tokio::test]
async fn test_cleanup_of_an_old_run_never_removes_the_entry_of_a_newer_one() {
    let table: ActiveTaskTable = Arc::new(RwLock::new(HashMap::new()));
    let task_id = TaskId::new();
    let parked = tokio::spawn(std::future::pending::<()>());
    table.write().await.insert(
        task_id,
        TaskExecutionHandle {
            task_id,
            cancel_tx: None,
            abort_handle: parked.abort_handle(),
            child_pid: Arc::new(AtomicU32::new(0)),
            started_at_epoch_sec: 0,
            attempt: 2,
            suppress_result: Arc::new(AtomicBool::new(false)),
            join: None,
        },
    );

    // The first run of the id (attempt 1) finally ends and its guard cleans up after it.
    let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    drop(ActiveTaskGuard {
        task_id,
        attempt: 1,
        heartbeat_tracker: Arc::new(HeartbeatTracker::new(Arc::new(AtomicUsize::new(1)))),
        active_task_table: Arc::clone(&table),
        runner: None,
        _permit: permit,
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
            table.read().await.get(&task_id).map(|h| h.attempt),
            Some(2),
            "the old run's cleanup removed the new run's entry (which would also drop its cancel handle)"
        );

    // The run that owns the entry does remove it.
    remove_active(&table, task_id, 2).await;
    assert!(table.read().await.is_empty());
    parked.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn test_cancel_all_active_tasks_kills_processes_and_sends_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let client = test_client(dir.path(), 2);
    let (tx, mut rx) = mpsc::channel(32);

    let mut ids = Vec::new();
    let mut pids = Vec::new();
    for _ in 0..2 {
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
            TaskRequirements::generic(1, 60),
        );
        ids.push(task.id);
        client.handle_assign_task(task, tx.clone()).await;
    }
    for id in &ids {
        wait_running(&mut rx, *id).await;
    }
    for id in &ids {
        // The `Running` report precedes the spawn of the child by a moment.
        let mut pid = 0;
        for _ in 0..100 {
            pid = client.active_task_table().read().await[id]
                .child_pid
                .load(Ordering::Acquire);
            if pid != 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_ne!(pid, 0, "child process never started");
        assert!(process_alive(pid));
        pids.push(pid);
    }

    let cancelled = client
        .cancel_all_active_tasks("test: connection lost")
        .await;
    assert_eq!(cancelled, 2);
    assert!(client.active_task_table().read().await.is_empty());
    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        2,
        "permits released"
    );
    assert_eq!(client.heartbeat_tracker().active_tasks(), 0);
    for pid in pids {
        assert!(
            !process_alive(pid),
            "child {pid} of a cancelled task must be dead, not orphaned"
        );
    }
    // Their results could only go to the dead session: nothing is sent.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err(),
        "no result may be sent for a task the worker cancelled itself"
    );
    // And with nothing running the call is a cheap no-op.
    assert_eq!(client.cancel_all_active_tasks("again").await, 0);
}

#[tokio::test]
async fn test_cancel_all_active_tasks_also_stops_tasks_still_queued_for_a_permit() {
    let dir = tempfile::tempdir().unwrap();
    let client = test_client(dir.path(), 1);
    let (tx, mut rx) = mpsc::channel(32);
    let running = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 60),
    );
    let queued = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 60),
    );
    let running_id = running.id;
    client.handle_assign_task(running, tx.clone()).await;
    wait_running(&mut rx, running_id).await;
    client.handle_assign_task(queued, tx.clone()).await;
    assert_eq!(client.active_task_table().read().await.len(), 2);

    assert_eq!(client.cancel_all_active_tasks("test").await, 2);
    assert!(client.active_task_table().read().await.is_empty());
    assert_eq!(client.concurrency_semaphore().available_permits(), 1);
    assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn test_handle_cancel_task_stops_task_waiting_for_a_permit_and_reports_130() {
    let dir = tempfile::tempdir().unwrap();
    let client = test_client(dir.path(), 1);
    let (tx, mut rx) = mpsc::channel(32);

    let running = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["0.5".into()]),
        TaskRequirements::generic(1, 60),
    );
    let queued = Task::new(
        rusty_grid_core::task::TaskSpec::command("sleep", vec!["30".into()]),
        TaskRequirements::generic(1, 60),
    );
    let running_id = running.id;
    let queued_id = queued.id;

    client.handle_assign_task(running, tx.clone()).await;
    wait_running(&mut rx, running_id).await;

    // Assign second task while the single permit is held by `running`.
    client.handle_assign_task(queued, tx.clone()).await;
    assert_eq!(client.active_task_table().read().await.len(), 2);

    // Cancel the permit-waiting task via `handle_cancel_task`.
    client
        .handle_cancel_task(queued_id, Some("cancelled while waiting for permit".into()))
        .await;

    let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("cancelled permit-waiting task must report promptly")
        .expect("channel open");
    match msg {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            error,
            ..
        } => {
            assert_eq!(task_id, queued_id);
            assert_eq!(exit_code, EXIT_CODE_CANCELLED);
            assert!(
                error.as_deref().unwrap_or("").contains("Cancelled"),
                "expected Cancelled error message, got: {error:?}"
            );
        }
        other => panic!("expected TaskResult for cancelled queued task, got {other:?}"),
    }

    // Wait for `running` to complete naturally, then verify permit is released and a third task runs.
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskResult {
                task_id, exit_code, ..
            })) if task_id == running_id => {
                assert_eq!(exit_code, 0);
                break;
            }
            Ok(Some(_)) => {}
            other => panic!("running task did not finish: {other:?}"),
        }
    }

    let follow_up = Task::new(
        rusty_grid_core::task::TaskSpec::command("echo", vec!["permit_ok".into()]),
        TaskRequirements::generic(1, 30),
    );
    let follow_id = follow_up.id;
    client.handle_assign_task(follow_up, tx).await;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskResult {
                task_id,
                exit_code,
                stdout,
                ..
            })) if task_id == follow_id => {
                assert_eq!(exit_code, 0);
                assert!(String::from_utf8_lossy(&stdout.0).contains("permit_ok"));
                break;
            }
            Ok(Some(_)) => {}
            other => panic!("follow_up task did not finish: {other:?}"),
        }
    }
    assert_eq!(client.concurrency_semaphore().available_permits(), 1);
}

#[tokio::test]
async fn test_a_leftover_sandbox_directory_does_not_fail_the_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let client = test_client(dir.path(), 2);
    let (tx, mut rx) = mpsc::channel(16);
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("echo", vec!["fresh".into()]),
        TaskRequirements::generic(1, 30),
    );
    // What a worker killed mid-task (power loss, Android low-memory kill) leaves behind.
    let leftover = dir.path().join(task.id.to_string());
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("half_written.txt"), "stale").unwrap();

    client.handle_assign_task(task.clone(), tx).await;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(WorkerMessage::TaskResult {
                exit_code,
                stdout,
                error,
                ..
            })) => {
                assert_eq!(
                        exit_code, 0,
                        "a stale sandbox directory must not turn into a terminal task failure: {error:?}"
                    );
                assert!(String::from_utf8_lossy(&stdout.0).contains("fresh"));
                break;
            }
            Ok(Some(_)) => {}
            other => panic!("no result: {other:?}"),
        }
    }
    assert!(!leftover.join("half_written.txt").exists());
}

#[tokio::test]
async fn test_with_keep_sandboxes_a_leftover_directory_is_moved_aside_not_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_cores(1)
            .with_sandbox_base_dir(dir.path())
            .with_keep_sandboxes(true)
            .with_no_battery(true),
    );
    let (tx, mut rx) = mpsc::channel(16);
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::command("echo", vec!["again".into()]),
        TaskRequirements::generic(1, 30),
    );
    let leftover = dir.path().join(task.id.to_string());
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("kept.txt"), "debug data").unwrap();

    client.handle_assign_task(task.clone(), tx).await;
    loop {
        if let Ok(Some(WorkerMessage::TaskResult { exit_code, .. })) =
            tokio::time::timeout(Duration::from_secs(5), rx.recv()).await
        {
            assert_eq!(exit_code, 0);
            break;
        }
    }
    let moved: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".stale-"))
        .collect();
    assert_eq!(moved.len(), 1, "the earlier attempt's data is preserved");
    assert!(moved[0].path().join("kept.txt").exists());
}

// ------------------------------------------------------------------------------------------
// Lane writer: control traffic is never queued behind task data
// ------------------------------------------------------------------------------------------

fn big_result(worker_id: Uuid, bytes: usize) -> WorkerMessage {
    WorkerMessage::from(TaskResult::success(
        worker_id,
        TaskId::new(),
        "x".repeat(bytes),
        1,
        false,
    ))
}

struct LaneHarness {
    control_tx: mpsc::Sender<WorkerMessage>,
    data_tx: mpsc::Sender<WorkerMessage>,
    closing_tx: watch::Sender<bool>,
    events_rx: mpsc::Receiver<Inbound>,
    peer: MessageReader<tokio::io::DuplexStream>,
    task: JoinHandle<()>,
}

fn spawn_lane(pipe_capacity: usize) -> LaneHarness {
    let (a, b) = tokio::io::duplex(pipe_capacity);
    let (control_tx, control_rx) = mpsc::channel(8);
    let (data_tx, data_rx) = mpsc::channel(8);
    let (events_tx, events_rx) = mpsc::channel(8);
    let (closing_tx, closing_rx) = watch::channel(false);
    let task = tokio::spawn(lane_writer(
        MessageWriter::new(a),
        control_rx,
        data_rx,
        "test",
        closing_rx,
        events_tx,
    ));
    LaneHarness {
        control_tx,
        data_tx,
        closing_tx,
        events_rx,
        peer: MessageReader::new(b),
        task,
    }
}

#[tokio::test]
async fn test_lane_writer_sends_control_messages_ahead_of_queued_data() {
    let mut lane = spawn_lane(1024);
    let worker_id = Uuid::new_v4();
    // `big` blocks mid-write on the tiny pipe (nobody reads yet); `small` waits behind it.
    lane.data_tx
        .send(big_result(worker_id, 200_000))
        .await
        .unwrap();
    lane.data_tx.send(big_result(worker_id, 10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    // A heartbeat that arrives while `big` is on the wire ...
    lane.control_tx
        .send(WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 1,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 1,
        })
        .await
        .unwrap();

    // ... goes out right after it, ahead of the data message that was queued earlier.
    let mut order = Vec::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(
            Duration::from_secs(5),
            lane.peer.recv_msg::<WorkerMessage>(),
        )
        .await
        .expect("frame in time")
        .unwrap()
        .expect("frame");
        order.push(match msg {
            WorkerMessage::Heartbeat { .. } => "heartbeat".to_string(),
            WorkerMessage::TaskResult { stdout, .. } => format!("result:{}", stdout.0.len()),
            other => panic!("unexpected {other:?}"),
        });
    }
    assert_eq!(order, ["result:200000", "heartbeat", "result:10"]);
    lane.task.abort();
}

#[tokio::test]
async fn test_lane_writer_reports_a_write_failure() {
    let mut lane = spawn_lane(1024);
    // The peer vanished: the next write must surface as an event, not be silently dropped.
    drop(std::mem::replace(
        &mut lane.peer,
        MessageReader::new(tokio::io::duplex(1).1),
    ));
    lane.control_tx
        .send(WorkerMessage::Disconnecting {
            worker_id: Uuid::new_v4(),
            reason: "x".into(),
        })
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(2), lane.events_rx.recv()).await {
        Ok(Some(Inbound::Failed("test", _))) => {}
        other => panic!(
            "expected a write failure event, got {:?}",
            other.map(|o| o.is_some())
        ),
    }
    lane.task.await.unwrap();
}

#[tokio::test]
async fn test_lane_writer_flushes_pending_control_messages_before_closing() {
    let mut lane = spawn_lane(64 * 1024);
    let worker_id = Uuid::new_v4();
    lane.control_tx
        .send(WorkerMessage::Disconnecting {
            worker_id,
            reason: "farewell".into(),
        })
        .await
        .unwrap();
    lane.closing_tx.send(true).unwrap();
    // Data queued after the close request is not started.
    let _ = lane.data_tx.try_send(big_result(worker_id, 10));

    let msg = lane
        .peer
        .recv_msg::<WorkerMessage>()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(msg, WorkerMessage::Disconnecting { .. }));
    tokio::time::timeout(Duration::from_secs(2), lane.task)
        .await
        .expect("the writer exits once the control queue is flushed")
        .unwrap();
}

#[tokio::test]
async fn test_lane_writer_fails_only_oversized_task_result_and_keeps_session_alive() {
    let mut lane = spawn_lane(64 * 1024);
    let worker_id = Uuid::new_v4();
    let oversized_id = TaskId::new();
    let normal_id = TaskId::new();

    lane.data_tx
        .send(WorkerMessage::from(TaskResult::success(
            worker_id,
            oversized_id,
            "x".repeat(rusty_grid_core::protocol::MAX_FRAME_SIZE),
            12,
            false,
        )))
        .await
        .unwrap();
    lane.data_tx
        .send(WorkerMessage::from(TaskResult::success(
            worker_id, normal_id, "ok", 5, false,
        )))
        .await
        .unwrap();

    let first = tokio::time::timeout(
        Duration::from_secs(5),
        lane.peer.recv_msg::<WorkerMessage>(),
    )
    .await
    .expect("oversized fallback frame in time")
    .unwrap()
    .expect("first frame");
    match first {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            stdout,
            stderr,
            error,
            ..
        } => {
            assert_eq!(task_id, oversized_id);
            assert_ne!(exit_code, 0);
            assert!(stdout.0.is_empty());
            let err_text = error.as_deref().unwrap_or_default();
            let stderr_text = String::from_utf8_lossy(&stderr.0);
            assert!(
                err_text.contains("exceeds") && err_text.contains("dropped stdout"),
                "unexpected error: {error:?}"
            );
            assert!(
                stderr_text.contains("exceeds") && stderr_text.contains("dropped stdout"),
                "unexpected stderr: {stderr_text}"
            );
        }
        other => panic!("expected failed TaskResult for oversized task, got {other:?}"),
    }

    let second = tokio::time::timeout(
        Duration::from_secs(5),
        lane.peer.recv_msg::<WorkerMessage>(),
    )
    .await
    .expect("subsequent frame in time")
    .unwrap()
    .expect("second frame");
    match second {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            stdout,
            ..
        } => {
            assert_eq!(task_id, normal_id);
            assert_eq!(exit_code, 0);
            assert_eq!(stdout.0.as_ref(), b"ok");
        }
        other => panic!("expected normal TaskResult for second task, got {other:?}"),
    }
    assert!(
        lane.events_rx.try_recv().is_err(),
        "oversized TaskResult must not report a lane failure"
    );
    lane.task.abort();
}

#[tokio::test]
async fn test_lane_writer_oversized_checkpoint_is_dropped_and_keeps_lane_alive() {
    let mut lane = spawn_lane(1024 * 1024);
    let worker_id = Uuid::new_v4();
    let cp_task_id = TaskId::new();
    let normal_id = TaskId::new();

    lane.data_tx
        .send(WorkerMessage::Checkpoint {
            task_id: cp_task_id,
            sequence: 7,
            delta_state: rusty_grid_core::task::Bytes::from(vec![
                    b'c';
                    rusty_grid_core::protocol::MAX_FRAME_SIZE
                ]),
        })
        .await
        .unwrap();
    lane.data_tx
        .send(WorkerMessage::from(TaskResult::success(
            worker_id, normal_id, "after-cp", 3, false,
        )))
        .await
        .unwrap();

    let next = tokio::time::timeout(
        Duration::from_secs(5),
        lane.peer.recv_msg::<WorkerMessage>(),
    )
    .await
    .expect("subsequent frame in time after oversized checkpoint")
    .unwrap()
    .expect("frame after dropped checkpoint");
    match next {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            stdout,
            ..
        } => {
            assert_eq!(task_id, normal_id);
            assert_eq!(exit_code, 0);
            assert_eq!(stdout.0.as_ref(), b"after-cp");
        }
        other => panic!("expected normal TaskResult after dropped checkpoint, got {other:?}"),
    }
    assert!(
        lane.events_rx.try_recv().is_err(),
        "oversized Checkpoint must not report a lane failure"
    );
    lane.task.abort();
}

#[test]
fn test_rewrite_oversized_task_result_drops_in_order_and_keeps_what_fits() {
    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();
    let max = rusty_grid_core::protocol::MAX_FRAME_SIZE;
    let sample_output = rusty_grid_core::blob::BlobFile {
        path: "out/report.json".into(),
        id: rusty_grid_core::blob::BlobId::of(b"report"),
        size: 6,
        executable: false,
    };
    let sample_artifact =
        rusty_grid_core::task::OutputArtifact::new("inline.txt", b"small-artifact".to_vec());

    // 1. 70 MiB asset_bytes: drops only asset_bytes, keeping artifacts, stdout, stderr,
    //    outputs, device_name, non-zero exit_code, and original error text.
    let mut r1 = TaskResult::failure(
        worker_id,
        task_id,
        7,
        "engine stdout",
        "engine stderr",
        42,
        Some("ORIGINAL ENGINE ERROR".to_string()),
    );
    r1.device_name = Some("long-device-".repeat(30));
    r1.asset_bytes = Some(rusty_grid_core::task::Bytes::from(vec![
        0u8;
        70 * 1024 * 1024
    ]));
    r1.artifacts = vec![sample_artifact.clone()];
    r1.outputs = vec![sample_output.clone()];

    match rewrite_oversized_task_result(
        r1.clone(),
        70 * 1024 * 1024 + 500,
        max,
        WireCodec::Bincode,
        false,
    ) {
        WorkerMessage::TaskResult {
            exit_code,
            stdout,
            stderr,
            device_name,
            error,
            asset_bytes,
            artifacts,
            outputs,
            ..
        } => {
            assert_eq!(exit_code, 7, "non-zero exit code must be kept");
            assert!(asset_bytes.is_none());
            assert_eq!(artifacts, vec![sample_artifact]);
            assert_eq!(outputs, vec![sample_output.clone()]);
            assert_eq!(device_name, r1.device_name);
            assert_eq!(stdout.0.as_ref(), b"engine stdout");
            let stderr_str = String::from_utf8_lossy(&stderr.0);
            assert!(stderr_str.contains("engine stderr"), "{stderr_str}");
            assert!(stderr_str.contains("dropped asset_bytes"), "{stderr_str}");
            let err_str = error.unwrap();
            assert!(
                err_str.contains("ORIGINAL ENGINE ERROR"),
                "original error text must be preserved: {err_str}"
            );
            assert!(err_str.contains("dropped asset_bytes"), "{err_str}");
            assert!(!err_str.contains("artifacts"), "{err_str}");
        }
        other => panic!("unexpected {other:?}"),
    }

    // 2. Oversized artifacts (no asset_bytes): drops artifacts, keeping stdout, stderr, outputs.
    let mut r2 = TaskResult::success(worker_id, task_id, "stdout kept", 10, false);
    r2.stderr = rusty_grid_core::task::Bytes::from_static(b"stderr kept");
    r2.artifacts = vec![rusty_grid_core::task::OutputArtifact::new(
        "huge.bin",
        vec![0u8; 70 * 1024 * 1024],
    )];
    r2.outputs = vec![sample_output];

    match rewrite_oversized_task_result(r2, 70 * 1024 * 1024 + 500, max, WireCodec::Bincode, false)
    {
        WorkerMessage::TaskResult {
            exit_code,
            stdout,
            stderr,
            error,
            artifacts,
            outputs,
            ..
        } => {
            assert_eq!(exit_code, EXIT_CODE_GENERAL_ERROR);
            assert!(artifacts.is_empty());
            assert_eq!(outputs.len(), 1);
            assert_eq!(stdout.0.as_ref(), b"stdout kept");
            let stderr_str = String::from_utf8_lossy(&stderr.0);
            assert!(stderr_str.contains("stderr kept"), "{stderr_str}");
            assert!(stderr_str.contains("dropped artifacts"), "{stderr_str}");
            assert!(error.unwrap().contains("dropped artifacts"));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn test_encrypted_lane_writer_accounts_for_full_25_byte_aead_overhead() {
    use rusty_grid_core::protocol::{
        serialize_message, AeadReaderSession, AeadWriterSession, AEAD_RECORD_OVERHEAD,
        MAX_FRAME_SIZE,
    };

    let key = [0x42u8; 32];
    let (a, b) = tokio::io::duplex(256 * 1024);
    let mut writer = MessageWriter::new(a);
    writer.enable_encryption(AeadWriterSession::new(&key));
    let mut peer = MessageReader::new(b);
    peer.enable_encryption(AeadReaderSession::new(&key));

    let (_control_tx, control_rx) = mpsc::channel(8);
    let (data_tx, data_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let (_closing_tx, closing_rx) = watch::channel(false);
    let lane_task = tokio::spawn(lane_writer(
        writer, control_rx, data_rx, "enc-test", closing_rx, events_tx,
    ));

    let worker_id = Uuid::new_v4();
    let oversized_id = TaskId::new();
    let normal_id = TaskId::new();

    // Measure the base serialized size of `build(true, true)` with empty stdout and an
    // 8-digit `size` (matching the ~67_110_000-byte oversized frame) so we can size `stdout`
    // to place the `build(true, true)` plaintext frame at `MAX_FRAME_SIZE - 20` bytes
    // (inside the 8-byte window `MAX_FRAME_SIZE - 25 < len <= MAX_FRAME_SIZE - 17`).
    let mut probe = TaskResult::success(worker_id, oversized_id, "", 5, false);
    probe.asset_bytes = Some(rusty_grid_core::task::Bytes::from(vec![0u8; 1024]));
    let base_rewritten =
        rewrite_oversized_task_result(probe, 67_110_000, MAX_FRAME_SIZE, WireCodec::Bincode, false);
    let base_len = serialize_message(&base_rewritten, WireCodec::Bincode)
        .unwrap()
        .len();
    let target_plaintext_len = MAX_FRAME_SIZE - 20;
    assert!(
        target_plaintext_len + AEAD_RECORD_OVERHEAD > MAX_FRAME_SIZE
            && target_plaintext_len + 17 <= MAX_FRAME_SIZE
    );
    let stdout_len = target_plaintext_len - base_len;

    let mut window_result = TaskResult::success(
        worker_id,
        oversized_id,
        String::from_utf8(vec![b's'; stdout_len]).unwrap(),
        5,
        false,
    );
    window_result.asset_bytes = Some(rusty_grid_core::task::Bytes::from(vec![0u8; 1024]));

    data_tx
        .send(WorkerMessage::from(window_result))
        .await
        .unwrap();
    data_tx
        .send(WorkerMessage::from(TaskResult::success(
            worker_id,
            normal_id,
            "still-alive",
            2,
            false,
        )))
        .await
        .unwrap();

    let first = tokio::time::timeout(Duration::from_secs(5), peer.recv_msg::<WorkerMessage>())
        .await
        .expect("rewritten frame arrives on encrypted lane")
        .expect("encrypted frame must not fail AEAD limit")
        .expect("first message");
    match first {
        WorkerMessage::TaskResult {
            task_id,
            exit_code,
            stdout,
            error,
            ..
        } => {
            assert_eq!(task_id, oversized_id);
            assert_ne!(exit_code, 0);
            assert!(
                stdout.0.is_empty(),
                "frame at MAX_FRAME_SIZE - 20 must drop stdout on an encrypted lane"
            );
            let err_str = error.unwrap_or_default();
            assert!(err_str.contains("dropped asset_bytes, stdout"), "{err_str}");
        }
        other => panic!("expected rewritten TaskResult, got {other:?}"),
    }

    let second = tokio::time::timeout(Duration::from_secs(5), peer.recv_msg::<WorkerMessage>())
        .await
        .expect("second frame arrives on encrypted lane")
        .unwrap()
        .expect("second message");
    match second {
        WorkerMessage::TaskResult {
            task_id, stdout, ..
        } => {
            assert_eq!(task_id, normal_id);
            assert_eq!(stdout.0.as_ref(), b"still-alive");
        }
        other => panic!("expected normal second TaskResult, got {other:?}"),
    }
    assert!(
        events_rx.try_recv().is_err(),
        "encrypted lane must not emit Inbound::Failed"
    );
    lane_task.abort();
}

#[tokio::test]
async fn test_oversized_task_result_fails_only_its_task_while_other_tasks_on_worker_finish() {
    let dir = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mut client = WorkerClient::new(
        WorkerConfig::new(addr.to_string())
            .with_cores(2)
            .with_sandbox_base_dir(dir.path())
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let (stop_tx, stop_rx) = watch::channel(false);
    let worker = tokio::spawn(async move { client.run(stop_rx).await });

    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("worker connects")
        .unwrap();
    let mut master = MessageTransport::new(stream);
    let Some(WorkerMessage::Register { worker_id, .. }) =
        master.recv_msg::<WorkerMessage>().await.unwrap()
    else {
        panic!("expected Register");
    };
    master
        .send_msg(&MasterMessage::RegisterAck {
            accepted: true,
            worker_id,
            heartbeat_interval_secs: 5,
            message: None,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();

    let oversized_task = Task::new(
        rusty_grid_core::task::TaskSpec::BuiltinTest {
            test_name: "oversized_asset".into(),
            iterations: 1,
            duration_ms: 20,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let oversized_id = oversized_task.id;

    let sibling_task = Task::new(
        rusty_grid_core::task::TaskSpec::BuiltinTest {
            test_name: "sleep".into(),
            iterations: 1,
            duration_ms: 200,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::generic(1, 10),
    );
    let sibling_id = sibling_task.id;

    master
        .send_msg(&MasterMessage::AssignTask {
            task: oversized_task,
        })
        .await
        .unwrap();
    master
        .send_msg(&MasterMessage::AssignTask { task: sibling_task })
        .await
        .unwrap();

    let mut arrival_order = Vec::new();
    let mut oversized_outcome = None;
    let mut sibling_outcome = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while oversized_outcome.is_none() || sibling_outcome.is_none() {
        match tokio::time::timeout_at(deadline, master.recv_msg::<WorkerMessage>())
            .await
            .expect("both tasks finish on the same session")
            .expect("stream stays open without protocol error")
        {
            Some(WorkerMessage::TaskResult {
                task_id,
                exit_code,
                stdout,
                stderr,
                error,
                asset_bytes,
                ..
            }) => {
                arrival_order.push(task_id);
                if task_id == oversized_id {
                    oversized_outcome = Some((exit_code, stdout, stderr, error, asset_bytes));
                } else if task_id == sibling_id {
                    sibling_outcome = Some((exit_code, error));
                }
            }
            Some(_) => {}
            None => panic!("worker disconnected instead of keeping the session alive"),
        }
    }

    assert_eq!(
        arrival_order,
        vec![oversized_id, sibling_id],
        "oversized result must arrive first before the slower sibling task finishes"
    );

    let (oversized_code, oversized_stdout, oversized_stderr, oversized_err, oversized_asset) =
        oversized_outcome.unwrap();
    assert_ne!(oversized_code, 0, "oversized task must fail");
    assert!(oversized_asset.is_none(), "70 MiB asset must be dropped");
    assert_eq!(
        String::from_utf8_lossy(&oversized_stdout.0),
        "engine stdout line\n",
        "engine stdout must be preserved when only asset_bytes is dropped"
    );
    let stderr_str = String::from_utf8_lossy(&oversized_stderr.0);
    assert!(
        stderr_str.contains("engine stderr line") && stderr_str.contains("dropped asset_bytes"),
        "stderr must contain engine stderr and the drop reason: {stderr_str}"
    );
    let err_str = oversized_err.as_deref().unwrap_or_default();
    assert!(
        err_str.contains("exceeds") && err_str.contains("dropped asset_bytes"),
        "unexpected oversized error: {oversized_err:?}"
    );

    let (sibling_code, sibling_err) = sibling_outcome.unwrap();
    assert_eq!(
        sibling_code, 0,
        "sibling task on the same worker must finish successfully (err: {sibling_err:?})"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
}

// ------------------------------------------------------------------------------------------
// Registration, identity, timeouts
// ------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_register_carries_the_live_mobile_state_not_the_startup_snapshot() {
    let (client_io, master_io) = tokio::io::duplex(4096);
    let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true));
    let worker_id = client.worker_id();
    // The phone has drained since the worker started.
    client
        .mobile_telemetry()
        .set(Some(rusty_grid_core::capabilities::MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Tensor".into(),
            battery_pct: Some(6),
            is_charging: Some(false),
            thermal_throttled: true,
        }));

    let master = tokio::spawn(async move {
        let mut master_transport = MessageTransport::new(master_io);
        let msg = master_transport
            .recv_msg::<WorkerMessage>()
            .await
            .unwrap()
            .unwrap();
        master_transport
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: 3,
                message: None,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        msg
    });

    let mut transport = MessageTransport::new(client_io);
    client
        .perform_handshake(&mut transport)
        .await
        .expect("handshake");
    match master.await.unwrap() {
        WorkerMessage::Register { capabilities, .. } => {
            let mobile = capabilities.mobile.expect("mobile state in Register");
            assert_eq!(mobile.battery_pct, Some(6));
            assert_eq!(mobile.is_charging, Some(false));
            assert!(mobile.thermal_throttled);
        }
        other => panic!("expected Register, got {other:?}"),
    }
}

/// Runs the worker against a protocol-level Master that accepts it with `ack_message`, changes
/// the device's battery reading, and returns the names of the non-heartbeat messages the
/// Master receives while the worker has time to report it.
async fn messages_after_a_battery_change(ack_message: Option<String>) -> Vec<&'static str> {
    let dir = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // A fixed starting reading: an empty handle would take this host's own battery (or none),
    // and with it the outcome of the test.
    let handle =
        MobileTelemetryHandle::new(Some(rusty_grid_core::capabilities::MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 3".into(),
            battery_pct: Some(80),
            is_charging: Some(true),
            thermal_throttled: false,
        }));
    let mut client = WorkerClient::new(
        WorkerConfig::new(addr.to_string())
            .with_cores(1)
            .with_mobile_telemetry(handle.clone())
            .with_sandbox_base_dir(dir.path())
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let (stop_tx, stop_rx) = watch::channel(false);
    let worker = tokio::spawn(async move { client.run(stop_rx).await });

    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("the worker connects")
        .unwrap();
    let mut master = MessageTransport::new(stream);
    let Some(WorkerMessage::Register { worker_id, .. }) =
        master.recv_msg::<WorkerMessage>().await.unwrap()
    else {
        panic!("expected Register");
    };
    master
        .send_msg(&MasterMessage::RegisterAck {
            accepted: true,
            worker_id,
            heartbeat_interval_secs: 1,
            message: ack_message,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    // A mobile worker heartbeats on its adaptive keepalive (a minute on Wi-Fi), so a heartbeat
    // cannot pace this test. The answer to a task it cannot take proves the session loop is
    // running, and so has read the battery already: what follows is a change.
    let probe = Task::new(
        rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(64, 10),
    );
    let probe_id = probe.id;
    master
        .send_msg(&MasterMessage::AssignTask { task: probe })
        .await
        .unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), master.recv_msg::<WorkerMessage>())
            .await
            .expect("an answer to the probe task")
        {
            Ok(Some(WorkerMessage::TaskResult { task_id, .. })) if task_id == probe_id => break,
            Ok(Some(_)) => {}
            other => panic!("worker went away before answering the probe task: {other:?}"),
        }
    }
    handle.update(MobileLiveStatus {
        battery_pct: Some(42),
        is_charging: Some(false),
        thermal_throttled: false,
        ..Default::default()
    });

    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + MOBILE_TELEMETRY_CHECK_INTERVAL * 2;
    while let Ok(Ok(Some(msg))) =
        tokio::time::timeout_at(deadline, master.recv_msg::<WorkerMessage>()).await
    {
        if !matches!(msg, WorkerMessage::Heartbeat { .. }) {
            seen.push(msg.variant_name());
        }
    }
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
    seen
}

#[tokio::test]
async fn test_battery_changes_are_reported_to_a_master_that_announced_it_reads_them() {
    let announcement = master_features::announcement(master_features::SUPPORTED);
    let seen = messages_after_a_battery_change(Some(announcement)).await;
    assert!(seen.contains(&"MobileTelemetry"), "{seen:?}");
}

#[tokio::test]
async fn test_battery_changes_are_not_sent_to_a_master_that_did_not_announce_it_reads_them() {
    // A Master built before WorkerMessage::MobileTelemetry cannot decode it and would drop
    // the worker (and fail its tasks over) on every battery tick; it sends no announcement.
    let seen = messages_after_a_battery_change(None).await;
    assert!(!seen.contains(&"MobileTelemetry"), "{seen:?}");
}

#[test]
fn test_external_telemetry_handle_is_shared_with_the_client() {
    let handle = MobileTelemetryHandle::default();
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_mobile_telemetry(handle.clone())
            .with_no_battery(true),
    );
    handle.update(MobileLiveStatus {
        battery_pct: Some(33),
        is_charging: Some(true),
        thermal_throttled: false,
        ..Default::default()
    });
    assert_eq!(
        client.mobile_telemetry().live_status().unwrap().battery_pct,
        Some(33)
    );
    assert!(client.mobile_fed_externally);
}

#[test]
fn test_worker_id_is_persisted_only_when_asked_to() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker_id");

    // Default: a fresh id every time, and nothing is written.
    let a =
        WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true)).worker_id();
    let b =
        WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true)).worker_id();
    assert_ne!(a, b);

    // With a file: the same id across "restarts".
    let first = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_worker_id_file(&path)
            .with_no_battery(true),
    );
    let second = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_worker_id_file(&path)
            .with_no_battery(true),
    );
    assert_eq!(first.worker_id(), second.worker_id());
    assert_eq!(
        std::fs::read_to_string(&path).unwrap().trim(),
        first.worker_id().to_string()
    );

    // An explicit id still wins over the file, and leaves it alone.
    let explicit = Uuid::new_v4();
    let third = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_worker_id_file(&path)
            .with_worker_id(explicit)
            .with_no_battery(true),
    );
    assert_eq!(third.worker_id(), explicit);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap().trim(),
        first.worker_id().to_string()
    );
}

#[test]
fn test_unusable_worker_id_file_falls_back_to_a_fresh_id() {
    let dir = tempfile::tempdir().unwrap();
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, "a file, not a directory").unwrap();
    let path = blocker.join("worker_id");
    let a = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_worker_id_file(&path)
            .with_no_battery(true),
    );
    let b = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_worker_id_file(&path)
            .with_no_battery(true),
    );
    assert_ne!(
        a.worker_id(),
        b.worker_id(),
        "no persistence, but the worker still starts"
    );
}

#[test]
fn test_dead_peer_timeout_scales_with_the_heartbeat_interval() {
    let client = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true));
    assert_eq!(
        client.dead_peer_timeout(Duration::from_secs(3)),
        Duration::from_secs(12)
    );
    assert_eq!(
        client.dead_peer_timeout(Duration::from_millis(100)),
        MIN_DEAD_PEER_TIMEOUT,
        "never twitchier than the floor"
    );
    let tuned = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_dead_peer_timeout(Duration::from_millis(700))
            .with_no_battery(true),
    );
    assert_eq!(
        tuned.dead_peer_timeout(Duration::from_secs(3)),
        Duration::from_millis(700)
    );
}

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_run_refuses_an_unparsable_p2p_ticket_instead_of_retrying_forever() {
    let mut client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9")
            .with_p2p_ticket("this is not a ticket")
            .with_p2p_minimal_preset(true)
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let (_tx, rx) = watch::channel(false);
    let result = tokio::time::timeout(Duration::from_secs(5), client.run(rx))
        .await
        .expect("a configuration error must end run() at once, not loop");
    assert!(matches!(result, Err(GridError::Config(_))), "{result:?}");
}

#[tokio::test]
async fn test_run_refuses_to_start_when_task_sandboxes_cannot_be_created() {
    let dir = tempfile::tempdir().unwrap();
    // A regular file where the base directory should be: not creatable, whoever runs the test.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let mut client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9")
            .with_sandbox_base_dir(blocker.join("sandboxes"))
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let (_tx, rx) = watch::channel(false);
    let result = tokio::time::timeout(Duration::from_secs(5), client.run(rx))
        .await
        .expect("an unusable sandbox directory must end run() at once, not loop");
    match result {
        Err(GridError::Config(msg)) => {
            assert!(
                msg.contains("not writable") && msg.contains("blocker"),
                "{msg}"
            );
            assert!(
                msg.contains("--sandbox-base-dir"),
                "the fix must be in the message: {msg}"
            );
        }
        other => panic!("expected a Config error, got {other:?}"),
    }
}

#[test]
fn test_worker_advertises_its_isolation_level_and_refuses_a_spoofed_tag() {
    for level in [
        IsolationLevel::Off,
        IsolationLevel::Fs,
        IsolationLevel::FsNet,
    ] {
        let client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9")
                .with_isolation(level)
                .with_redirection_portal(false)
                .with_no_battery(true),
        );
        let tags = client.capabilities().tags.clone();
        assert!(tags.contains(&format!("iso:{level}")), "{tags:?}");
        assert_eq!(tags.iter().filter(|t| t.starts_with("iso:")).count(), 1);
        assert_eq!(client.runner().config().isolation, level);
    }
    assert!(rusty_grid_core::placement::validate_user_tag("iso:fs+net").is_err());
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
#[tokio::test]
async fn test_run_refuses_isolation_this_platform_cannot_enforce() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9")
            .with_sandbox_base_dir(dir.path().join("sandboxes"))
            .with_isolation(IsolationLevel::Fs)
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let (_tx, rx) = watch::channel(false);
    let result = tokio::time::timeout(Duration::from_secs(5), client.run(rx))
        .await
        .expect("an impossible isolation level must end run() at once");
    match result {
        Err(GridError::Config(msg)) => {
            assert!(
                msg.contains("not implemented yet on this platform"),
                "{msg}"
            )
        }
        other => panic!("expected a Config error, got {other:?}"),
    }
}

#[test]
fn test_worker_advertises_system_tags_then_user_tags() {
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9")
            .with_max_concurrency(3)
            .with_tags(vec![
                "laptop".into(),
                format!("os:{}", std::env::consts::OS),
                "slots:99".into(),
                "tool:spoofed".into(),
            ])
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let mut expected = vec![
        format!("os:{}", std::env::consts::OS),
        format!("arch:{}", std::env::consts::ARCH),
        "slots:3".to_string(),
        "iso:off".to_string(),
    ];
    expected.extend(rusty_grid_core::capabilities::detect_host_tools());
    expected.push("laptop".to_string());
    assert_eq!(
            client.capabilities().tags,
            expected,
            "system tags, slots:<n>, iso:<level> and probed tool:<name> first, a user tag equal to one of them or starting with slots:/tool: is not repeated"
        );
}

#[tokio::test]
async fn test_run_refuses_a_reserved_or_malformed_user_tag() {
    for bad in ["os:windows", "link:fake", "tool:cargo", "two words"] {
        let mut client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9")
                .with_tags(vec![bad.into()])
                .with_redirection_portal(false)
                .with_no_battery(true),
        );
        let (_tx, rx) = watch::channel(false);
        let result = tokio::time::timeout(Duration::from_secs(5), client.run(rx))
            .await
            .expect("an invalid tag must end run() at once, not loop");
        match result {
            Err(GridError::Config(msg)) => {
                assert!(
                    msg.contains("Invalid worker tag") && msg.contains(bad),
                    "{msg}"
                )
            }
            other => panic!("{bad}: expected a Config error, got {other:?}"),
        }
    }
}

#[test]
fn test_default_max_concurrency_leaves_a_core_free() {
    assert_eq!(default_max_concurrency(0), 1);
    assert_eq!(default_max_concurrency(1), 1);
    assert_eq!(default_max_concurrency(2), 1);
    assert_eq!(default_max_concurrency(8), 7);
    assert_eq!(default_max_concurrency(16), 15);
}

#[test]
fn test_concurrency_permits_default_to_cores_minus_one_unless_configured() {
    // Detected cores: headroom.
    let detected = WorkerClient::new(WorkerConfig::new("127.0.0.1:9999").with_no_battery(true));
    assert_eq!(
        detected.concurrency_semaphore().available_permits(),
        default_max_concurrency(detected.capabilities().cpu_cores)
    );
    // Cores the owner named are offered in full; an explicit limit always wins.
    let named = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_cores(6)
            .with_no_battery(true),
    );
    assert_eq!(named.concurrency_semaphore().available_permits(), 6);
    let limited = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_cores(6)
            .with_max_concurrency(2)
            .with_no_battery(true),
    );
    assert_eq!(limited.concurrency_semaphore().available_permits(), 2);
    let all = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_max_concurrency(64)
            .with_no_battery(true),
    );
    assert_eq!(all.concurrency_semaphore().available_permits(), 64);
}

#[test]
fn test_process_priority_reaches_the_runner() {
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_process_priority(ProcessPriority::Idle)
            .with_no_battery(true),
    );
    assert_eq!(
        client.runner().config().process_priority,
        ProcessPriority::Idle
    );
}

// ------------------------------------------------------------------------------------------
// Standby portal: validated URLs only, no hard-coded hosts, bounded requests
// ------------------------------------------------------------------------------------------

fn beacon(cluster: &str, ui: &str) -> rusty_grid_core::discovery::MasterBeacon {
    rusty_grid_core::discovery::MasterBeacon::new(cluster, ui, "h", 0)
}

fn remote_visitor() -> IpAddr {
    "192.168.50.7".parse().unwrap()
}

#[test]
fn test_portal_resolution_never_invents_a_host() {
    // Master on this very machine (loopback) and a visitor from the LAN: nowhere useful to
    // send them. (This used to fall back to two hard-coded 192.168.1.x addresses.)
    let b = beacon("127.0.0.1:8088", "http://127.0.0.1:8080");
    assert_eq!(portal_master_from_beacon(&b, remote_visitor()), None);
    // A loopback visitor may be sent to loopback.
    assert_eq!(
        portal_master_from_beacon(&b, "127.0.0.1".parse().unwrap()),
        Some(("http://127.0.0.1:8080".into(), "127.0.0.1:8088".into()))
    );
    // Dashboard on loopback but the cluster address on the LAN: use the cluster host.
    let b = beacon("10.1.2.3:8088", "http://127.0.0.1:8080");
    assert_eq!(
        portal_master_from_beacon(&b, remote_visitor()),
        Some(("http://10.1.2.3:8080".into(), "10.1.2.3:8088".into()))
    );
    // A proper LAN dashboard URL is kept as advertised.
    let b = beacon("10.1.2.3:8088", "http://10.1.2.3:9090");
    assert_eq!(
        portal_master_from_beacon(&b, remote_visitor()),
        Some(("http://10.1.2.3:9090".into(), "10.1.2.3:8088".into()))
    );
    // IPv6 hosts are bracketed.
    let b = beacon("[fd00::5]:8088", "http://127.0.0.1:8080");
    assert_eq!(
        portal_master_from_beacon(&b, remote_visitor()),
        Some(("http://[fd00::5]:8080".into(), "[fd00::5]:8088".into()))
    );
}

#[test]
fn test_portal_rejects_hostile_beacon_fields() {
    let visitor = remote_visitor();
    // An invalid dashboard URL is replaced by one derived from a valid cluster address...
    for ui in [
        "javascript:alert(1)",
        "http://10.1.2.3:8080\r\nSet-Cookie: pwn=1",
        "http://10.1.2.3\"><script>alert(1)</script>",
        "//evil.example",
        "http://user@10.1.2.3",
    ] {
        let b = beacon("10.1.2.3:8088", ui);
        assert_eq!(
            portal_master_from_beacon(&b, visitor),
            Some(("http://10.1.2.3:8080".into(), "10.1.2.3:8088".into())),
            "ui {ui:?}"
        );
    }
    // An *empty* dashboard URL is not a malformed one: the Master has no dashboard to offer,
    // so the visitor stays on the standby page instead of being sent to a guessed address.
    // (This used to be derived from the cluster host like the cases above.)
    let b = beacon("10.1.2.3:8088", "");
    assert_eq!(
        portal_master_from_beacon(&b, visitor),
        Some((String::new(), "10.1.2.3:8088".into()))
    );
    // (and a master on loopback tells a visitor from another machine nothing useful)
    let b = beacon("127.0.0.1:8088", "");
    assert_eq!(portal_master_from_beacon(&b, visitor), None);
    // ...and a hostile cluster address drops the beacon altogether.
    for cluster in [
        "10.1.2.3:8088\r\nX-Evil: 1",
        "evil\"host:8088",
        "<script>:8088",
        "10.1.2.3",
        "auto",
    ] {
        let b = beacon(cluster, "http://10.1.2.3:8080");
        assert_eq!(
            portal_master_from_beacon(&b, visitor),
            None,
            "cluster {cluster:?}"
        );
    }
}

#[test]
fn test_portal_responses_never_reflect_unvalidated_text() {
    for hostile in [
        "http://10.0.0.5:8080\r\nSet-Cookie: pwn=1",
        "javascript:alert(document.cookie)",
        "http://10.0.0.5\"><script>alert(1)</script>",
        "http://10.0.0.5';alert(1);//",
    ] {
        let response = portal_redirect_response(hostile, "10.0.0.5:8088\r\nX-Evil: 1");
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "falls back to the standby page: {hostile:?}"
        );
        assert!(!response.contains("Set-Cookie"));
        assert!(!response.contains("X-Evil"));
        assert!(!response.contains("alert(document.cookie)"));
        assert!(!response.contains("Location:"));
    }
    // A valid origin with a hostile cluster address: still redirected, header omitted.
    let response = portal_redirect_response("http://10.0.0.5:8080", "10.0.0.5:8088\r\nX-Evil: 1");
    assert!(response.starts_with("HTTP/1.1 307 Temporary Redirect"));
    assert!(response.contains("Location: http://10.0.0.5:8080\r\n"));
    assert!(!response.contains("X-Evil"));
    assert!(!response.contains("X-OxideSwarm-Master"));
    // Only the response head can carry headers: exactly one blank line ends it.
    let head = response.split("\r\n\r\n").next().unwrap();
    assert!(!head.contains("<script>"));
}

#[test]
fn test_portal_has_no_hard_coded_hosts_and_no_cors_wildcard() {
    let standby = portal_standby_response();
    assert!(
        !standby.contains("192.168."),
        "the standby page must not probe fixed hosts"
    );
    assert!(
        standby.contains("fetch('/api/status'"),
        "it asks its own origin instead"
    );
    assert!(standby.contains("probeCluster()"));
    assert!(!standby
        .to_ascii_lowercase()
        .contains("access-control-allow-origin"));

    let status = portal_status_response(None);
    assert!(!status.contains("192.168."));
    assert!(!status.contains("candidate_nodes"));
    assert!(status.contains("\"status\":\"searching_master\""));
    let status = portal_status_response(Some(&(
        "http://10.1.2.3:8080".into(),
        "10.1.2.3:8088".into(),
    )));
    assert!(status.contains("\"redirect_url\":\"http://10.1.2.3:8080\""));

    // The source of the portal itself carries no site-specific address either.
    let source = include_str!("../client.rs");
    let code = source.split("#[cfg(test)]").next().unwrap();
    assert!(
        !code
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .any(|l| l.contains("192.168.1.")),
        "client.rs must not hard-code LAN hosts"
    );
}

#[test]
fn test_status_request_detection_is_exact() {
    assert!(is_status_request("GET /api/status HTTP/1.1"));
    assert!(is_status_request("GET /api/status?x=1 HTTP/1.1"));
    assert!(!is_status_request("GET /api/statusfoo HTTP/1.1"));
    assert!(!is_status_request("GET / HTTP/1.1"));
    assert!(!is_status_request("GET"));
    assert!(!is_status_request(""));
}

async fn portal_fixture(
    target: PortalTarget,
    max_connections: usize,
) -> (std::net::SocketAddr, watch::Sender<bool>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(run_portal(
        listener,
        target,
        None,
        shutdown_rx,
        max_connections,
    ));
    (addr, shutdown_tx)
}

async fn portal_get(addr: std::net::SocketAddr, request: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("portal answered in time")
        .expect("portal closed the connection cleanly (no reset)");
    String::from_utf8_lossy(&response).to_string()
}

#[tokio::test]
async fn test_portal_serves_the_validated_redirect_end_to_end() {
    let state = Arc::new(RwLock::new(Some(beacon(
        "10.1.2.3:8088\r\nX-Evil: 1",
        "http://10.1.2.3:8080",
    ))));
    let (addr, shutdown) = portal_fixture(PortalTarget::Dynamic(Arc::clone(&state)), 8).await;

    // Hostile cluster address: no redirect, a standby page, and nothing injected.
    let response = portal_get(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(!response.contains("X-Evil"));

    *state.write().await = Some(beacon("10.1.2.3:8088", "http://10.1.2.3:8080"));
    let response = portal_get(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 307 Temporary Redirect"));
    assert!(response.contains("Location: http://10.1.2.3:8080\r\n"));
    assert!(response.contains("X-OxideSwarm-Master: 10.1.2.3:8088\r\n"));

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_portal_shows_the_standby_page_for_a_master_that_has_no_reachable_dashboard() {
    // A master whose dashboard listens on loopback advertises no dashboard URL.
    let state = Arc::new(RwLock::new(Some(beacon("10.1.2.3:8088", ""))));
    let (addr, shutdown) = portal_fixture(PortalTarget::Dynamic(Arc::clone(&state)), 8).await;
    let response = portal_get(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "{}",
        &response[..response.len().min(60)]
    );
    assert!(
        !response.contains("Location:"),
        "no redirect to a guessed address"
    );
    // The status API still names the master that is being followed.
    let status = portal_get(addr, b"GET /api/status HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert!(
        status.contains("\"status\":\"connected_to_master\""),
        "{status}"
    );
    assert!(
        status.contains("\"master_cluster_addr\":\"10.1.2.3:8088\""),
        "{status}"
    );
    assert!(status.contains("\"redirect_url\":null"), "{status}");
    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_portal_answers_requests_with_large_headers_without_a_reset() {
    let (addr, shutdown) = portal_fixture(PortalTarget::from("10.1.2.3:8088"), 8).await;
    // A browser with a big cookie jar sends far more than the first 1 KiB read of the old code.
    let mut request = b"GET / HTTP/1.1\r\nHost: x\r\nCookie: ".to_vec();
    request.extend_from_slice(&[b'a'; 6000]);
    request.extend_from_slice(b"\r\n\r\n");
    let response = portal_get(addr, &request).await;
    assert!(
        response.starts_with("HTTP/1.1 307 Temporary Redirect"),
        "{}",
        &response[..response.len().min(80)]
    );
    assert!(response.contains("Location: http://10.1.2.3:8080"));
    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_portal_request_reader_gives_up_on_a_silent_client() {
    let (mut client, mut server) = tokio::io::duplex(64);
    // A client that connects and sends nothing must not hold a task and a socket forever.
    let started = std::time::Instant::now();
    let line = read_portal_request_line_with(&mut server, Duration::from_millis(150)).await;
    assert!(line.is_none());
    assert!(started.elapsed() < Duration::from_secs(2));
    // Headers that arrive in pieces are still assembled.
    use tokio::io::AsyncWriteExt;
    let reader = tokio::spawn(async move {
        read_portal_request_line_with(&mut server, Duration::from_secs(2)).await
    });
    client.write_all(b"GET /api/sta").await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    client
        .write_all(b"tus HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(
        reader.await.unwrap().as_deref(),
        Some("GET /api/status HTTP/1.1")
    );
}

#[tokio::test]
async fn test_portal_caps_concurrent_connections() {
    use tokio::io::AsyncReadExt;
    let (addr, shutdown) = portal_fixture(PortalTarget::from("10.1.2.3:8088"), 2).await;
    // Two clients that connect and say nothing occupy both slots...
    let _hold1 = TcpStream::connect(addr).await.unwrap();
    let _hold2 = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // ...so a third is dropped at once instead of becoming another idle task.
    let mut third = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(1), third.read(&mut buf))
        .await
        .expect("the surplus connection is closed promptly");
    assert!(matches!(n, Ok(0) | Err(_)));
    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_cancel_during_output_upload_reports_result_and_decrements_master_active_tasks() {
    use rusty_grid_master::server::{MasterServer, ServerConfig};

    let master_dir = tempfile::tempdir().unwrap();
    let worker_dir = tempfile::tempdir().unwrap();
    let master = MasterServer::spawn(
        ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_blob_store(master_dir.path(), 16 * 1024 * 1024)
            .with_heartbeat_interval(30),
    )
    .await
    .expect("spawn master");

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let master_addr = master.server_addr();

    let (upload_started_tx, upload_started_rx) = oneshot::channel::<()>();
    let (release_upload_tx, release_upload_rx) = oneshot::channel::<()>();
    let observed_results: Arc<std::sync::Mutex<Vec<i32>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_clone = Arc::clone(&observed_results);

    let proxy_task = tokio::spawn(async move {
        let (worker_stream, _) = proxy_listener.accept().await.unwrap();
        let master_stream = TcpStream::connect(master_addr).await.unwrap();
        let (mut w_writer, mut w_reader) = MessageTransport::new(worker_stream).split();
        let (mut m_writer, mut m_reader) = MessageTransport::new(master_stream).split();

        let m_to_w = tokio::spawn(async move {
            while let Ok(Some(msg)) = m_reader.recv_msg::<MasterMessage>().await {
                if w_writer.send_msg(&msg).await.is_err() {
                    break;
                }
            }
        });

        let mut upload_started_tx = Some(upload_started_tx);
        let mut release_upload_rx = Some(release_upload_rx);
        while let Ok(Some(msg)) = w_reader.recv_msg::<WorkerMessage>().await {
            if matches!(msg, WorkerMessage::BlobRequest { .. }) {
                if let Some(tx) = upload_started_tx.take() {
                    let _ = tx.send(());
                }
                if let Some(rx) = release_upload_rx.take() {
                    // Hold the BlobRequest until the test sees the cancelled TaskResult,
                    // proving that `upload_task_outputs` was interrupted by `tokio::select!`
                    // rather than finishing first.
                    let delayed = msg;
                    let mut rx = rx;
                    loop {
                        tokio::select! {
                            _ = &mut rx => {
                                let _ = m_writer.send_msg(&delayed).await;
                                break;
                            }
                            next_opt = w_reader.recv_msg::<WorkerMessage>() => {
                                if let Ok(Some(next_msg)) = next_opt {
                                    let is_result = if let WorkerMessage::TaskResult { exit_code, .. } = &next_msg {
                                        observed_clone.lock().unwrap().push(*exit_code);
                                        true
                                    } else {
                                        false
                                    };
                                    if m_writer.send_msg(&next_msg).await.is_err() {
                                        break;
                                    }
                                    if is_result {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                    continue;
                }
            }
            if let WorkerMessage::TaskResult { exit_code, .. } = &msg {
                observed_clone.lock().unwrap().push(*exit_code);
            }
            if m_writer.send_msg(&msg).await.is_err() {
                break;
            }
        }
        m_to_w.abort();
    });

    let mut client = WorkerClient::new(
        WorkerConfig::new(proxy_addr.to_string())
            .with_cores(1)
            .with_sandbox_base_dir(worker_dir.path())
            .with_redirection_portal(false)
            .with_no_battery(true),
    );
    let (stop_tx, stop_rx) = watch::channel(false);
    let worker_task = tokio::spawn(async move { client.run(stop_rx).await });

    let reg_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if master
            .list_workers()
            .await
            .is_ok_and(|workers| workers.len() == 1)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < reg_deadline,
            "worker did not register with master in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Case 1: Cancel arrives while `upload_task_outputs` is blocked on `BlobRequest`.
    // The proxy holds `BlobRequest` until the worker sends `TaskResult`, so no fixed sleep is
    // needed and removing the upload `tokio::select!` makes `upload_task_outputs` hang.
    let mut task = Task::new(
        rusty_grid_core::task::TaskSpec::shell_script("echo upload-payload > out.txt"),
        TaskRequirements::generic(1, 10),
    );
    task.output_globs = vec!["out.txt".into()];
    let task_id = master.submit_task(task).await.expect("submit task");

    tokio::time::timeout(Duration::from_secs(5), upload_started_rx)
        .await
        .expect("task must reach upload_task_outputs and send BlobRequest")
        .unwrap();

    assert_eq!(
        master.list_workers().await.unwrap()[0].active_tasks,
        1,
        "worker active_tasks on master must be 1 while uploading outputs"
    );

    master.cancel_task(task_id).await.expect("cancel task");

    let finish_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < finish_deadline {
        let got_result = !observed_results.lock().unwrap().is_empty();
        let active_zero = master
            .list_workers()
            .await
            .is_ok_and(|w| w.first().is_some_and(|info| info.active_tasks == 0));
        if got_result && active_zero {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = release_upload_tx.send(());

    let results = observed_results.lock().unwrap().clone();
    assert_eq!(
        results,
        vec![EXIT_CODE_CANCELLED],
        "master must receive exactly one cancelled TaskResult when cancelled during output upload"
    );
    let workers = master.list_workers().await.unwrap();
    assert_eq!(
            workers[0].active_tasks, 0,
            "worker active_tasks on the master must return to 0 after cancellation during output upload"
        );

    // Case 2: Cancel arrives after the child process has exited (exit 0) while
    // `execute_process_supervised` is draining pipes held open by a background child.
    // `exec_cancel_tx.send` succeeds because `exec_cancel_rx` is still in scope, so keeping
    // `cancelled_reason` across `exec_fut` is what turns the post-exit run into 130.
    let marker_path = worker_dir.path().join("child_exited.marker");
    #[cfg(unix)]
    let task2 = Task::new(
        rusty_grid_core::task::TaskSpec::shell_script(format!(
            "sleep 5 &\necho done > '{}'\nexit 0",
            marker_path.display()
        )),
        TaskRequirements::generic(1, 10),
    );
    #[cfg(windows)]
    let task2 = {
        let mut env = std::collections::HashMap::new();
        env.insert(
            "MARKER_PATH".into(),
            marker_path.to_string_lossy().to_string(),
        );
        let mut t = Task::new(
            rusty_grid_core::task::TaskSpec::Command {
                program: "cmd.exe".into(),
                args: vec!["/C".into(), "drain.cmd".into()],
                env,
                working_dir: None,
                stdin: None,
            },
            TaskRequirements::generic(1, 10),
        );
        t.staged_files
                .push(rusty_grid_core::task::StagedFile::new(
                    "drain.cmd",
                    rusty_grid_core::task::Bytes::from_static(
                        b"@echo off\r\nstart /B ping -n 6 127.0.0.1\r\necho done > \"%MARKER_PATH%\"\r\nexit /B 0\r\n",
                    ),
                    true,
                ));
        t
    };
    let task2_id = master.submit_task(task2).await.expect("submit task 2");

    let marker_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !marker_path.exists() {
        assert!(
            tokio::time::Instant::now() < marker_deadline,
            "task 2 did not write exit marker in time"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    master.cancel_task(task2_id).await.expect("cancel task 2");

    let finish2_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < finish2_deadline {
        let got_two = observed_results.lock().unwrap().len() >= 2;
        let active_zero = master
            .list_workers()
            .await
            .is_ok_and(|w| w.first().is_some_and(|info| info.active_tasks == 0));
        if got_two && active_zero {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let results2 = observed_results.lock().unwrap().clone();
    assert_eq!(
        results2,
        vec![EXIT_CODE_CANCELLED, EXIT_CODE_CANCELLED],
        "worker must report 130 when cancelled while draining pipes after child exit"
    );
    let workers2 = master.list_workers().await.unwrap();
    assert_eq!(
        workers2[0].active_tasks, 0,
        "worker active_tasks on the master must return to 0 after second cancellation"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_task).await;
    proxy_task.abort();
    let _ = master.shutdown();
}

#[cfg(unix)]
#[test]
fn test_worker_client_with_hung_blender_path_reports_no_blender_within_timeout() {
    use std::os::unix::fs::PermissionsExt;

    let dir =
        std::env::temp_dir().join(format!("oxideswarm_worker_hung_blender_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let pid_file = dir.join("hung.pid");
    let script = dir.join("blender");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&script).unwrap();
        file.write_all(
            format!(
                "#!/bin/sh\nsleep 60 &\necho $! > '{}'\nwait\n",
                pid_file.display()
            )
            .as_bytes(),
        )
        .unwrap();
        file.sync_all().unwrap();
    }
    let mut perms = std::fs::metadata(&script).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).unwrap();

    let started = std::time::Instant::now();
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_name("hung-blender-worker")
            .with_cores(2)
            .with_no_gpu(true)
            .with_no_cuda(true)
            .with_no_battery(true)
            .with_blender_path(script.to_string_lossy().to_string()),
    );
    let elapsed = started.elapsed();
    if let Ok(raw) = std::fs::read_to_string(&pid_file) {
        let pid: u32 = raw.trim().parse().expect("valid hung grandchild pid");
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while process_alive(pid) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !process_alive(pid),
            "hung Blender probe grandchild {pid} survived WorkerClient::new"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);

    assert!(!client.capabilities().has_blender);
    assert!(client.capabilities().blender_version.is_none());
    assert!(
        elapsed < Duration::from_secs(6),
        "WorkerClient::new took too long ({elapsed:?}) when blender_path hung"
    );
}

#[tokio::test]
async fn test_worker_client_with_blender_path_plumbs_override_to_runner() {
    let dir = std::env::temp_dir().join(format!(
        "oxideswarm_worker_blender_plumbing_{}",
        Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let marker_file = dir.join("runner_invoked_blender.marker");
    #[cfg(windows)]
    let script = dir.join("fake_blender.cmd");
    #[cfg(not(windows))]
    let script = dir.join("fake_blender.sh");

    {
        use std::io::Write;
        let mut file = std::fs::File::create(&script).unwrap();
        #[cfg(windows)]
            let body = format!(
                "@echo off\r\necho %* | findstr /C:\"--version\" >nul\r\nif not errorlevel 1 (\r\n    echo Blender 4.5.4 LTS\r\n    exit /b 0\r\n)\r\necho invoked > \"{}\"\r\nexit /b 42\r\n",
                marker_file.display()
            );
        #[cfg(not(windows))]
            let body = format!(
                "#!/bin/sh\ncase \"$*\" in\n  *--version*)\n    echo \"Blender 4.5.4 LTS\"\n    exit 0\n    ;;\nesac\necho invoked > '{}'\nexit 42\n",
                marker_file.display()
            );
        file.write_all(body.as_bytes()).unwrap();
        file.sync_all().unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).unwrap();
    }

    let script_str = script.to_string_lossy().to_string();
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_name("blender-plumbing-worker")
            .with_cores(2)
            .with_no_gpu(true)
            .with_no_cuda(true)
            .with_no_battery(true)
            .with_sandbox_base_dir(dir.join("sandboxes"))
            .with_blender_path(script_str.clone()),
    );

    assert!(client.capabilities().has_blender);
    assert_eq!(
        client.capabilities().blender_version.as_deref(),
        Some("Blender 4.5.4 LTS")
    );
    assert_eq!(
        client.runner.config().blender_path.as_deref(),
        Some(script_str.as_str()),
        "WorkerClient::new must forward WorkerConfig::blender_path to RunnerConfig"
    );

    #[cfg(feature = "3d")]
    {
        let (glb_bytes, _) = crate::gen3d::generate_procedural_3d(
            &rusty_grid_core::task::Generative3DSpec::new_prompt("test biped")
                .with_quality("fast")
                .with_engine("sdf"),
        )
        .expect("procedural 3d mesh");
        let spec = rusty_grid_core::task::AutoRigSpec::new(
            rusty_grid_core::task::Bytes::from(glb_bytes),
            "biped",
        )
        .with_provider("blender");
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::AutoRig(spec),
            rusty_grid_core::task::TaskRequirements::generic(1, 30),
        );
        let result = client
            .runner
            .execute_task_with_checkpoint(&task, None, None, None, None)
            .await;
        assert!(
                marker_file.exists(),
                "TaskRunner constructed by WorkerClient must invoke the configured blender_path override"
            );
        assert!(
            result.is_success(),
            "AutoRig should fall back cleanly after fake Blender exits 42: {:?}",
            result.error
        );
    }
    #[cfg(not(feature = "3d"))]
    let _ = &marker_file;

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_compute_effective_resume_grace_and_capability_toggle() {
    let dead_peer = Duration::from_secs(12);
    // None or <= dead_peer + 3s (15s) hold -> zero grace.
    assert_eq!(
        compute_effective_resume_grace(None, dead_peer, None),
        Duration::ZERO
    );
    assert_eq!(
        compute_effective_resume_grace(Some(Duration::from_secs(15)), dead_peer, None),
        Duration::ZERO
    );
    // 30s hold, 12s dead_peer, 3s margin -> 30 - 12 - 3 = 15s.
    assert_eq!(
        compute_effective_resume_grace(Some(Duration::from_secs(30)), dead_peer, None),
        Duration::from_secs(15)
    );
    // Operator cap may only shorten (clamp downward) the computed grace budget.
    assert_eq!(
        compute_effective_resume_grace(
            Some(Duration::from_secs(30)),
            dead_peer,
            Some(Duration::from_secs(25)),
        ),
        Duration::from_secs(15)
    );
    assert_eq!(
        compute_effective_resume_grace(
            Some(Duration::from_secs(30)),
            dead_peer,
            Some(Duration::from_secs(10)),
        ),
        Duration::from_secs(10)
    );
    assert_eq!(
        compute_effective_resume_grace(
            Some(Duration::from_secs(30)),
            dead_peer,
            Some(Duration::ZERO),
        ),
        Duration::ZERO
    );
    // Sub-second computed grace (< 1s) is treated as zero.
    assert_eq!(
        compute_effective_resume_grace(Some(Duration::from_millis(15_500)), dead_peer, None,),
        Duration::ZERO
    );

    let default_client = WorkerClient::for_testing("127.0.0.1:9999", None, Some(1), false);
    assert!(default_client
        .advertised_features()
        .contains(ProtocolCapabilities::SESSION_RESUME));

    // L2: when ADAPTIVE_KEEPALIVE is negotiated on a mobile worker whose live keepalive is
    // 30s (dead_peer = 120s), a 30s master hold yields 0s grace instead of 15s.
    default_client
        .last_negotiated_hold_secs
        .store(30, Ordering::Release);
    default_client
        .last_heartbeat_interval_ms
        .store(3_000, Ordering::Release);
    default_client.negotiated_features.store(
        ProtocolCapabilities::all_supported().bits(),
        Ordering::Release,
    );
    assert_eq!(
        default_client.effective_resume_grace(),
        Duration::from_secs(15)
    );
    default_client.mobile_telemetry().update(MobileLiveStatus {
        battery_pct: Some(80),
        is_charging: Some(true),
        thermal_throttled: false,
        network_type: Some("cellular".into()),
        ..Default::default()
    });
    assert_eq!(default_client.effective_resume_grace(), Duration::ZERO);

    let disabled_client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_no_battery(true)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_redirection_portal(false)
            .with_session_resume_grace_cap(Some(Duration::ZERO)),
    );
    assert!(!disabled_client
        .advertised_features()
        .contains(ProtocolCapabilities::SESSION_RESUME));
}

#[tokio::test]
async fn test_outbox_retains_result_across_aborted_flush_until_result_ack() {
    let client = WorkerClient::for_testing("127.0.0.1:9999", None, Some(1), false);
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(1, 32),
    );
    let task_id = task.id;
    let res = TaskResult::success(client.worker_id(), task_id, "ok", 5, false);
    let token = master_features::result_ack_token(&res);
    {
        let mut outbox = client.pending_resume_outbox.lock().unwrap();
        outbox.push_result(BufferedTaskResult::new(task, res, None, false));
    }
    assert_eq!(client.resumable_task_ids().await, vec![task_id]);

    // Simulating `run_session`'s non-destructive clone of `outbox.results` followed by an
    // aborted `_outbox_flush_guard`: the buffered result remains in the outbox for the next
    // reconnect until `RESULT_ACK_REASON` arrives.
    let cloned = client.pending_resume_outbox.lock().unwrap().results.clone();
    assert_eq!(cloned.len(), 1);
    assert_eq!(client.resumable_task_ids().await, vec![task_id]);

    client
        .handle_cancel_task(
            task_id,
            Some(master_features::format_result_ack_reason(token)),
        )
        .await;
    assert!(client.resumable_task_ids().await.is_empty());
    assert!(
        !client.cancelled_unseen.lock().unwrap().contains(&task_id),
        "RESULT_ACK_REASON must not record task_id in cancelled_unseen"
    );
}

#[tokio::test]
async fn test_stale_attempt_result_ack_preserves_retry_outbox_and_output_pins() {
    let client = WorkerClient::for_testing("127.0.0.1:9999", None, Some(1), false);
    let task = Task::new(
        rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
        TaskRequirements::generic(1, 32),
    );
    let task_id = task.id;
    let attempt1_res = TaskResult::failure(
        client.worker_id(),
        task_id,
        1,
        "",
        "boom",
        5,
        Some("boom".into()),
    );
    let token1 = master_features::result_ack_token(&attempt1_res);
    let attempt2_res = TaskResult::success(client.worker_id(), task_id, "ok-2", 7, false);
    let token2 = master_features::result_ack_token(&attempt2_res);
    assert_ne!(token1, token2);

    // Attempt 2 has buffered its result in the outbox AND holds an active output pin in the
    // runner's BlobCache when attempt 1's delayed acknowledgement arrives.
    {
        let mut outbox = client.pending_resume_outbox.lock().unwrap();
        outbox.push_result(BufferedTaskResult::new(
            task.clone(),
            attempt2_res,
            None,
            true,
        ));
    }
    let cache = client.runner.blob_cache().unwrap();
    let fake_blob_id = rusty_grid_core::blob::BlobId::of(b"retry-attempt-2-output");
    cache.keep_task_outputs(task_id, cache.pin([fake_blob_id]));

    // Master's delayed ack for attempt 1 arrives: must NOT evict attempt 2's buffered result
    // and must NOT release attempt 2's output pin in `BlobCache`.
    client
        .handle_cancel_task(
            task_id,
            Some(master_features::format_result_ack_reason(token1)),
        )
        .await;
    assert_eq!(client.resumable_task_ids().await, vec![task_id]);
    assert!(
        cache.take_task_outputs(task_id).is_some(),
        "attempt 1 result-ack must not release attempt 2's task_outputs pin"
    );

    // Now attempt 2's ack arrives and clears the buffered result.
    client
        .handle_cancel_task(
            task_id,
            Some(master_features::format_result_ack_reason(token2)),
        )
        .await;
    assert!(client.resumable_task_ids().await.is_empty());
}

#[test]
fn test_pending_resume_outbox_enforces_byte_budget() {
    let mut outbox = PendingResumeOutbox::with_capacity(16);
    outbox.max_bytes = 2_048;

    let worker_id = Uuid::new_v4();
    let mut ids = Vec::new();
    for i in 0..5 {
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::builtin_test("sleep", 0),
            TaskRequirements::generic(1, 32),
        );
        ids.push(task.id);
        let payload = format!("chunk-{i}-{}", "x".repeat(600));
        let res = TaskResult::success(worker_id, task.id, payload, 1, false);
        outbox.push_result(BufferedTaskResult::new(task, res, None, false));
    }

    assert!(
        outbox.total_bytes <= outbox.max_bytes,
        "total_bytes {} exceeded max_bytes {}",
        outbox.total_bytes,
        outbox.max_bytes
    );
    assert!(outbox.results.len() < 5);
    assert_eq!(outbox.results.last().unwrap().result.task_id, ids[4]);
}

#[tokio::test]
async fn test_workspaces_capability_advertised_only_when_enabled() {
    assert!(!ProtocolCapabilities::all_supported().contains(ProtocolCapabilities::WORKSPACES));
    assert!(!advertised_protocol_capabilities().contains(ProtocolCapabilities::WORKSPACES));

    let default_client = WorkerClient::for_testing("127.0.0.1:9999", None, Some(1), false);
    assert!(!default_client
        .advertised_features()
        .contains(ProtocolCapabilities::WORKSPACES));

    let ws_client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_no_battery(true)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_redirection_portal(false)
            .with_workspaces(true),
    );
    assert!(ws_client
        .advertised_features()
        .contains(ProtocolCapabilities::WORKSPACES));

    // Verify Register wire message with and without --workspaces over an in-memory duplex stream.
    for (enable_ws, client) in [(false, &default_client), (true, &ws_client)] {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client_tx = MessageTransport::new(client_io);
        let mut server_tx = MessageTransport::new(server_io);
        let wid = client.worker_id();
        let server_task = tokio::spawn(async move {
            let msg = server_tx
                .recv_msg::<WorkerMessage>()
                .await
                .unwrap()
                .unwrap();
            let features = match msg {
                WorkerMessage::Register { features, .. } => features,
                other => panic!("expected Register, got {other:?}"),
            };
            server_tx
                .send_msg(&MasterMessage::RegisterAck {
                    accepted: true,
                    worker_id: wid,
                    heartbeat_interval_secs: 3,
                    message: None,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features,
                })
                .await
                .unwrap();
            features
        });
        client.perform_handshake(&mut client_tx).await.unwrap();
        let sent_features = server_task.await.unwrap();
        assert_eq!(
            sent_features.contains(ProtocolCapabilities::WORKSPACES),
            enable_ws,
            "enable_ws={enable_ws}"
        );
        assert_eq!(
            client
                .negotiated_features()
                .contains(ProtocolCapabilities::WORKSPACES),
            enable_ws,
            "enable_ws={enable_ws}"
        );
    }
}

#[tokio::test]
async fn test_worker_client_workspace_startup_validation_and_blob_fetch_execution() {
    use rusty_grid_core::blob::{BlobFile, BlobId, TreeManifest};
    use rusty_grid_core::blob_transfer::{BlobReply, BlobRequest};
    use rusty_grid_core::task::{TaskRequirements, TaskSpec, WorkspaceRef};

    let tmp = tempfile::tempdir().expect("tempdir");
    let sandbox_dir = tmp.path().join("sandboxes");
    let ws_root = tmp.path().join("workspaces");

    // 1. If workspace_root is a symlink, `WorkerClient::run` refuses to start at startup.
    #[cfg(unix)]
    {
        let real_target = tmp.path().join("real_ws_target");
        std::fs::create_dir_all(&real_target).unwrap();
        let symlink_root = tmp.path().join("symlink_ws_root");
        std::os::unix::fs::symlink(&real_target, &symlink_root).unwrap();

        let mut bad_client = WorkerClient::new(
            WorkerConfig::new("127.0.0.1:9999")
                .with_no_battery(true)
                .with_no_gpu(true)
                .with_no_blender(true)
                .with_no_cuda(true)
                .with_redirection_portal(false)
                .with_sandbox_base_dir(&sandbox_dir)
                .with_workspaces(true)
                .with_workspace_root(Some(symlink_root)),
        );
        let (_tx, rx) = watch::channel(false);
        let err = bad_client
            .run(rx)
            .await
            .expect_err("must reject symlink workspace root");
        match err {
            GridError::Config(msg) => {
                assert!(msg.contains("Workspace store cannot be opened"), "{msg}");
            }
            other => panic!("expected GridError::Config, got {other:?}"),
        }
    }

    // 2. Worker without `--workspaces` refuses an assigned workspace task.
    let disabled_client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_no_battery(true)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_redirection_portal(false)
            .with_sandbox_base_dir(&sandbox_dir),
    );
    let (tx_dis, mut rx_dis) = mpsc::channel(16);
    let ws_task_dis = Task::new(
        TaskSpec::shell_script("echo should-not-run"),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("ws1"));
    disabled_client
        .handle_assign_task(ws_task_dis, tx_dis)
        .await;
    match rx_dis.recv().await.expect("rejection frame") {
        WorkerMessage::TaskResult {
            exit_code, stderr, ..
        } => {
            assert_eq!(exit_code, EXIT_CODE_GENERAL_ERROR);
            let stderr_str = String::from_utf8_lossy(stderr.as_bytes());
            assert!(stderr_str.contains("WORKSPACES"), "{stderr_str}");
        }
        other => panic!("expected TaskResult failure, got {other:?}"),
    }

    // 3. Worker with `--workspaces` fetches missing manifest + file blobs via BlobClient,
    // syncs the workspace, runs task 1, and preserves build output for task 2.
    let ws_client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_no_battery(true)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_redirection_portal(false)
            .with_sandbox_base_dir(&sandbox_dir)
            .with_workspaces(true)
            .with_workspace_root(Some(ws_root.clone())),
    );

    let file_bytes = b"hello-from-remote-blob".to_vec();
    let file_id = BlobId::of(&file_bytes);
    let manifest = TreeManifest::new(vec![BlobFile {
        path: "input.txt".into(),
        id: file_id,
        size: file_bytes.len() as u64,
        executable: false,
    }])
    .unwrap();
    let manifest_bytes = manifest.encode();
    let manifest_id = BlobId::of(&manifest_bytes);
    let blobs: HashMap<BlobId, Vec<u8>> =
        HashMap::from([(file_id, file_bytes), (manifest_id, manifest_bytes.clone())]);

    let (out_tx, out_rx) = mpsc::channel::<WorkerMessage>(32);
    let blob_client = BlobClient::new(out_tx.clone());
    *ws_client.blob_client.write().unwrap() = Some(Arc::clone(&blob_client));

    let responder_blobs = blobs.clone();
    let responder_bc = Arc::clone(&blob_client);
    let collect_result = |mut rx: mpsc::Receiver<WorkerMessage>| async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                WorkerMessage::BlobRequest {
                    request_id,
                    request:
                        BlobRequest::Read {
                            id,
                            offset,
                            max_len,
                        },
                } => {
                    let bytes = responder_blobs.get(&id).expect("known blob");
                    let start = offset as usize;
                    let end = (start + max_len as usize).min(bytes.len());
                    responder_bc.deliver(
                        request_id,
                        BlobReply::Chunk {
                            id,
                            total_size: bytes.len() as u64,
                            offset,
                            data: rusty_grid_core::task::Bytes::from(bytes[start..end].to_vec()),
                        },
                    );
                }
                WorkerMessage::TaskResult {
                    exit_code,
                    stdout,
                    stderr,
                    error,
                    ..
                } => {
                    return (exit_code, stdout, stderr, error);
                }
                _ => {}
            }
        }
        panic!("channel closed before TaskResult");
    };

    let task1 = Task::new(
        TaskSpec::shell_script("cat input.txt && printf ':built-1' >> build.out && cat build.out"),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("ws1").with_tree(manifest_id, manifest_bytes.len() as u64));
    let assign_fut = ws_client.handle_assign_task(task1, out_tx.clone());
    let ((), (exit1, stdout1, stderr1, err1)) = tokio::join!(assign_fut, collect_result(out_rx));
    assert_eq!(
        exit1,
        0,
        "task 1 failed: {err1:?} / {}",
        String::from_utf8_lossy(stderr1.as_bytes())
    );
    assert!(String::from_utf8_lossy(stdout1.as_bytes()).contains("hello-from-remote-blob:built-1"));

    let (out_tx2, mut out_rx2) = mpsc::channel::<WorkerMessage>(32);
    let task2 = Task::new(
        TaskSpec::shell_script("cat build.out"),
        TaskRequirements::generic(1, 30),
    )
    .with_workspace(WorkspaceRef::new("ws1"));
    ws_client.handle_assign_task(task2, out_tx2).await;
    let mut res2 = None;
    while let Some(msg) = out_rx2.recv().await {
        if let WorkerMessage::TaskResult {
            exit_code,
            stdout,
            stderr,
            error,
            ..
        } = msg
        {
            res2 = Some((exit_code, stdout, stderr, error));
            break;
        }
    }
    let (exit2, stdout2, stderr2, err2) = res2.expect("task 2 result");
    assert_eq!(
        exit2,
        0,
        "task 2 failed: {err2:?} / {}",
        String::from_utf8_lossy(stderr2.as_bytes())
    );
    assert_eq!(
        String::from_utf8_lossy(stdout2.as_bytes()).trim(),
        ":built-1"
    );
}

#[tokio::test]
async fn test_perform_handshake_master_public_key_without_node_key_returns_config_error() {
    let master_kp = NodeKeypair::generate().unwrap();
    let mut config =
        WorkerConfig::new("127.0.0.1:8088").with_master_public_key(*master_kp.public_key());
    config.node_key = None;
    let client = WorkerClient::new(config);
    let (a, _b) = tokio::io::duplex(4096);
    let mut transport = MessageTransport::new(a);
    let err = client
        .perform_handshake(&mut transport)
        .await
        .expect_err("master_public_key without node_key must return GridError::Config");
    assert!(matches!(err, GridError::Config(_)), "got: {err:?}");
    assert!(
        err.to_string().contains("with_v3_auth"),
        "unexpected error message: {err}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn test_windows_worker_task_running_junk_exe_fails_fast_without_error_dialog() {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetErrorMode() -> u32;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let client = WorkerClient::new(
        WorkerConfig::new("127.0.0.1:9999")
            .with_name("win-error-mode-worker")
            .with_cores(2)
            .with_no_gpu(true)
            .with_no_blender(true)
            .with_no_cuda(true)
            .with_no_battery(true)
            .with_redirection_portal(false)
            .with_sandbox_base_dir(dir.path().join("sandboxes")),
    );

    let mode = unsafe { GetErrorMode() };
    let expected_bits = rusty_grid_core::capabilities::WINDOWS_SUPPRESS_ERROR_DIALOGS_MODE;
    assert_eq!(
            mode & expected_bits,
            expected_bits,
            "WorkerClient::new must set SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX (0x8003), got 0x{mode:04x}"
        );

    let junk_non_pe = dir.path().join("fake_blender_file.exe");
    std::fs::write(&junk_non_pe, b"not-a-real-blender").expect("write junk non-PE exe");
    let junk_non_pe_str = junk_non_pe.to_string_lossy().to_string();

    let junk_mz = dir.path().join("fake_16bit_stub.exe");
    std::fs::write(&junk_mz, b"MZ\x00\x00not-a-valid-64bit-pe-executable")
        .expect("write junk MZ exe");
    let junk_mz_str = junk_mz.to_string_lossy().to_string();

    // 1. Direct task execution of junk-byte .exe files (with --isolation off).
    for exe_path in [&junk_non_pe_str, &junk_mz_str] {
        let task = Task::new(
            rusty_grid_core::task::TaskSpec::command(exe_path.clone(), vec![]),
            TaskRequirements::generic(1, 5),
        );
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            client.runner.execute_task(&task, None, None),
        )
        .await
        .expect("junk .exe task must fail fast without hanging on a modal dialog");
        assert!(
            !res.is_success() && res.exit_code != 0,
            "junk .exe ({exe_path}) must fail with non-zero exit code, got: {res:?}"
        );
    }

    // 2. Child process (`cmd.exe /C <junk.exe>`) inherits the worker's error mode and fails
    //    fast with a non-zero exit code instead of blocking on a 16-bit/64-bit modal dialog.
    for exe_path in [&junk_non_pe_str, &junk_mz_str] {
        let child_task = Task::new(
            rusty_grid_core::task::TaskSpec::command(
                "cmd.exe",
                vec!["/C".to_string(), exe_path.clone()],
            ),
            TaskRequirements::generic(1, 5),
        );
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            client.runner.execute_task(&child_task, None, None),
        )
        .await
        .expect("child process running junk .exe must fail fast without hanging");
        assert!(
            !res.is_success() && res.exit_code != 0,
            "child running junk .exe ({exe_path}) must fail with non-zero exit code, got: {res:?}"
        );
    }
}

#[tokio::test]
async fn test_loopback_targeting_worker_portal_binds_loopback_not_all_interfaces() {
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().expect("tempdir");
    let make_worker = |master_addr: &str| {
        WorkerClient::new(
            WorkerConfig::new(master_addr)
                .with_no_gpu(true)
                .with_no_blender(true)
                .with_no_cuda(true)
                .with_no_battery(true)
                .with_sandbox_base_dir(dir.path().join("sandboxes")),
        )
    };

    // 1. Loopback master addresses resolve to 127.0.0.1:<port>, not 0.0.0.0:<port>.
    for loopback_master in ["127.0.0.1:8088", "[::1]:8088", "localhost:8088"] {
        let worker = make_worker(loopback_master);
        let bind_addr =
            portal_bind_addr_for_target(8080, &PortalTarget::from(worker.master_beacon.clone()))
                .await;
        assert_eq!(
                bind_addr.ip(),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                "worker targeting loopback master {loopback_master} must bind portal to 127.0.0.1, got {bind_addr}"
            );
        let static_bind =
            portal_bind_addr_for_target(8080, &PortalTarget::from(loopback_master)).await;
        assert_eq!(static_bind.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    // 2. Non-loopback and auto-discovery targets keep binding 0.0.0.0:<port>.
    for lan_master in ["192.0.2.10:8088", "auto", "auto:8089"] {
        let worker = make_worker(lan_master);
        let bind_addr =
            portal_bind_addr_for_target(8080, &PortalTarget::from(worker.master_beacon.clone()))
                .await;
        assert_eq!(
                bind_addr.ip(),
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                "worker targeting non-loopback master {lan_master} must keep binding 0.0.0.0, got {bind_addr}"
            );
    }

    // 3. Spawn a live portal for a loopback-targeting worker and prove it listens on 127.0.0.1
    //    and does not occupy the port on non-loopback interfaces.
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let portal_port = reserve.local_addr().unwrap().port();
    drop(reserve);

    let worker = make_worker("127.0.0.1:18088");
    let target = PortalTarget::from(worker.master_beacon.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let portal_handle = tokio::spawn(async move {
        spawn_worker_redirection_portal_with_secret(portal_port, target, None, shutdown_rx).await;
    });

    let mut connected = None;
    for _ in 0..30 {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", portal_port)).await {
            connected = Some(stream);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut stream = connected.expect("portal should listen on 127.0.0.1");
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("write status request");
    let mut buf = vec![0u8; 2048];
    let n = stream.read(&mut buf).await.expect("read status response");
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("200 OK"), "{resp}");
    assert!(
        resp.contains("\"master_cluster_addr\":\"127.0.0.1:18088\""),
        "{resp}"
    );

    let lan_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
    if !lan_ip.is_loopback() {
        let lan_bind = std::net::TcpListener::bind((lan_ip, portal_port));
        assert!(
                lan_bind.is_ok(),
                "loopback-targeting worker portal must not bind 0.0.0.0:{portal_port} (binding {lan_ip}:{portal_port} failed: {:?})",
                lan_bind.err()
            );
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), portal_handle).await;
}

#[test]
fn test_advertised_tags_drops_tool_cargo_on_elevated_windows_worker_with_isolation() {
    let tool_tags = vec![
        "tool:cargo".to_string(),
        "tool:rustc".to_string(),
        "tool:git".to_string(),
    ];
    let user_tags = vec!["custom-tag".to_string()];

    // Non-elevated with isolation Fs or FsNet keeps tool:cargo.
    for level in [
        IsolationLevel::Off,
        IsolationLevel::Fs,
        IsolationLevel::FsNet,
    ] {
        let tags = advertised_tags(&user_tags, 4, level, &tool_tags, false);
        assert!(
            tags.iter().any(|t| t == "tool:cargo"),
            "non-elevated worker with isolation {level} must keep tool:cargo: {tags:?}"
        );
        assert!(tags.iter().any(|t| t == "tool:rustc"));
        assert!(tags.iter().any(|t| t == "tool:git"));
    }

    // Elevated with IsolationLevel::Off keeps tool:cargo.
    let off_elevated = advertised_tags(&user_tags, 4, IsolationLevel::Off, &tool_tags, true);
    assert!(
        off_elevated.iter().any(|t| t == "tool:cargo"),
        "elevated worker with --isolation off must keep tool:cargo: {off_elevated:?}"
    );

    // Elevated with IsolationLevel::Fs or IsolationLevel::FsNet drops tool:cargo while keeping
    // tool:rustc, tool:git, iso:<level>, slots:4, and custom user tags.
    for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
        let tags = advertised_tags(&user_tags, 4, level, &tool_tags, true);
        assert!(
            !tags.iter().any(|t| t == "tool:cargo"),
            "elevated worker with --isolation {level} must drop tool:cargo: {tags:?}"
        );
        assert!(
            tags.iter().any(|t| t == "tool:rustc"),
            "elevated worker with --isolation {level} must retain tool:rustc: {tags:?}"
        );
        assert!(
            tags.iter().any(|t| t == "tool:git"),
            "elevated worker with --isolation {level} must retain tool:git: {tags:?}"
        );
        assert!(tags.iter().any(|t| t == &level.tag()));
        assert!(tags.iter().any(|t| t == "slots:4"));
        assert!(tags.iter().any(|t| t == "custom-tag"));
    }
}
