use std::path::{Path, PathBuf};

use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::placement::STATE_TAG_PREFIX;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::WorkerCapabilities;
use rusty_grid_master::ServerConfig;

use crate::config::SecretText;
use crate::connect::connect_master;

use super::mapreduce::run_mapreduce;
use super::master::{
    apply_max_queue_size, build_master_scheduler_config, resolve_fail_unfulfillable_tasks,
    resolve_max_batch_size_with_env, resolve_scheduler_policy, resolve_speed_hold_factor,
    run_master,
};
use super::submit::{
    any_connected_worker_satisfies_vram, build_task_spec, run_submit, run_submit_with_secret,
};
use super::tasks::run_status;
use super::tests::{parse, submit_args};
use super::worker::{resolve_isolation_read_dirs, resolve_worker_target, run_worker};
use super::*;

/// Runs on its own thread with a 16 MiB stack: this test drives `run_master`, `run_worker`
/// and the client commands in one future, which overflowed the default 2 MiB test-thread
/// stack in Linux debug builds.
#[test]
fn test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket() {
    std::thread::Builder::new()
        .name("sched2-big-stack".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(
                    sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket(),
                )
        })
        .expect("spawn the test thread")
        .join()
        .expect("the test thread panicked");
}

async fn sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket() {
    use crate::config::{MasterFileConfig, SubmitFileConfig, WorkerFileConfig};
    use rusty_grid_master::scheduler::SchedulingPolicy;

    let test_secret = ClusterSecret::from_bytes(&[0x22; 32]).unwrap();
    let secret_opts = SecretOpts {
        value: Some(crate::config::SecretText(test_secret.to_hex())),
        file: None,
    };

    // 1. Unit checks for helpers:
    //    - `apply_max_queue_size` rejects 0 and honors flag > config.
    let base = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    assert_eq!(
        apply_max_queue_size(base.clone(), None, None)
            .unwrap()
            .max_queue_size,
        None
    );
    assert_eq!(
        apply_max_queue_size(base.clone(), None, Some(5))
            .unwrap()
            .max_queue_size,
        Some(5)
    );
    assert_eq!(
        apply_max_queue_size(base.clone(), Some(2), Some(5))
            .unwrap()
            .max_queue_size,
        Some(2)
    );
    assert!(apply_max_queue_size(base, Some(0), None).is_err());

    //    - `resolve_scheduler_policy` and `build_master_scheduler_config` parse known policies,
    //      carry `policy` into `SchedulerConfig`, and reject unknown ones.
    assert_eq!(
        resolve_scheduler_policy(None, None).unwrap(),
        SchedulingPolicy::WeightedLeastLoaded
    );
    assert_eq!(
        resolve_scheduler_policy(None, Some("least-loaded".into())).unwrap(),
        SchedulingPolicy::LeastLoaded
    );
    assert_eq!(
        resolve_scheduler_policy(Some("round-robin".into()), Some("least-loaded".into())).unwrap(),
        SchedulingPolicy::RoundRobin
    );
    assert!(resolve_scheduler_policy(Some("bogus-policy".into()), None).is_err());

    let built_sched_cfg = build_master_scheduler_config(
        Some("round-robin".into()),
        Some("least-loaded".into()),
        false,
        72.5,
        true,
        Some("2.0".into()),
        Some(1.8),
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        built_sched_cfg.policy,
        SchedulingPolicy::RoundRobin,
        "SchedulerConfig built by run_master must carry the parsed scheduler_policy"
    );
    assert!(!built_sched_cfg.preserve_gpu_for_gpu_tasks);
    assert_eq!(built_sched_cfg.max_host_cpu_pct, 72.5);
    assert!(built_sched_cfg.fail_unfulfillable_tasks);
    assert_eq!(built_sched_cfg.speed_hold_factor, 2.0);
    assert_eq!(
        built_sched_cfg.max_batch_size,
        rusty_grid_master::DEFAULT_MASTER_MAX_BATCH_SIZE
    );
    assert_eq!(
        resolve_max_batch_size_with_env(None, None, None).unwrap(),
        1024
    );
    assert_eq!(
        resolve_max_batch_size_with_env(None, Some(""), Some(256)).unwrap(),
        256
    );
    assert_eq!(
        resolve_max_batch_size_with_env(None, Some("512"), Some(256)).unwrap(),
        512
    );
    assert_eq!(
        resolve_max_batch_size_with_env(Some("2048".into()), Some("512"), Some(256)).unwrap(),
        2048
    );
    assert!(resolve_max_batch_size_with_env(Some("0".into()), None, None).is_err());
    assert!(resolve_max_batch_size_with_env(Some("nope".into()), None, None).is_err());
    assert!(resolve_max_batch_size_with_env(None, Some("0"), None).is_err());
    assert!(resolve_max_batch_size_with_env(None, Some("-4"), None).is_err());
    assert!(resolve_max_batch_size_with_env(None, None, Some(0)).is_err());
    assert_eq!(
        build_master_scheduler_config(
            None,
            None,
            true,
            85.0,
            false,
            None,
            None,
            Some("2048".into()),
            Some(256),
        )
        .unwrap()
        .max_batch_size,
        2048
    );
    assert!(build_master_scheduler_config(
        Some("bogus-policy".into()),
        None,
        true,
        85.0,
        false,
        None,
        None,
        None,
        None,
    )
    .is_err());
    assert!(build_master_scheduler_config(
        None,
        None,
        true,
        85.0,
        false,
        Some("1.0".into()),
        None,
        None,
        None,
    )
    .is_err());
    assert!(build_master_scheduler_config(
        None,
        None,
        true,
        85.0,
        false,
        None,
        None,
        Some("0".into()),
        None,
    )
    .is_err());
    assert!(build_master_scheduler_config(
        None,
        None,
        true,
        85.0,
        false,
        None,
        None,
        Some("abc".into()),
        None,
    )
    .is_err());

    //    - `resolve_worker_target`: explicit `--master` beats config `p2p_ticket`.
    let cfg_with_bad_ticket = WorkerFileConfig {
        master: Some("198.51.100.9:8088".into()),
        p2p_ticket: Some("not-a-valid-p2p-ticket".into()),
        ..Default::default()
    };
    let (addr_from_cfg, ticket_from_cfg) =
        resolve_worker_target(None, None, Some(&cfg_with_bad_ticket));
    assert_eq!(addr_from_cfg, "198.51.100.9:8088");
    assert_eq!(
        ticket_from_cfg.as_deref(),
        Some("not-a-valid-p2p-ticket"),
        "without --master, config p2p_ticket is used"
    );
    let (addr_from_cli, ticket_when_cli_master) = resolve_worker_target(
        Some("127.0.0.1:18088".into()),
        None,
        Some(&cfg_with_bad_ticket),
    );
    assert_eq!(addr_from_cli, "127.0.0.1:18088");
    assert_eq!(
        ticket_when_cli_master, None,
        "explicit --master must beat config p2p_ticket"
    );

    // 2. Startup validation: `--max-queue-size 0`, `--scheduler-policy bogus`, and
    //    `--wire-codec bogus` (on master and worker) fail immediately with `ExitCode::FAILURE`.
    //    Even on an exposed bind (`--listen 0.0.0.0:0`), a bad `--wire-codec` is rejected
    //    before secret setup so a fresh `OXIDESWARM_HOME` remains empty.
    let fresh_home = tempfile::tempdir().unwrap();
    let prev_home = std::env::var("OXIDESWARM_HOME").ok();
    std::env::set_var("OXIDESWARM_HOME", fresh_home.path());
    let bad_exposed_cli = Cli::try_parse_from([
        "oxideswarm",
        "master",
        "--listen",
        "0.0.0.0:0",
        "--wire-codec",
        "xml",
    ])
    .unwrap();
    let Commands::Master(bad_exposed_args) = bad_exposed_cli.command else {
        panic!("expected Master");
    };
    let bad_exposed_code = tokio::time::timeout(
        Duration::from_secs(5),
        run_master(bad_exposed_args, None, SecretOpts::default()),
    )
    .await
    .expect("bad exposed master --wire-codec must exit immediately");
    match prev_home {
        Some(v) => std::env::set_var("OXIDESWARM_HOME", v),
        None => std::env::remove_var("OXIDESWARM_HOME"),
    }
    assert_eq!(bad_exposed_code, ExitCode::FAILURE);
    assert!(
        std::fs::read_dir(fresh_home.path())
            .unwrap()
            .next()
            .is_none(),
        "fresh OXIDESWARM_HOME must stay empty when --wire-codec validation fails"
    );

    for bad_master_argv in [
        vec![
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
            "--max-queue-size",
            "0",
        ],
        vec![
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
            "--scheduler-policy",
            "not-a-policy",
        ],
        vec![
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
            "--wire-codec",
            "xml",
        ],
        vec![
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
            "--speed-hold-factor",
            "1.0",
        ],
        vec![
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
            "--speed-hold-factor",
            "-1",
        ],
    ] {
        let cli = Cli::try_parse_from(&bad_master_argv).unwrap();
        let Commands::Master(m_args) = cli.command else {
            panic!("expected Master");
        };
        let code = tokio::time::timeout(
            Duration::from_secs(5),
            run_master(m_args, None, secret_opts.clone()),
        )
        .await
        .expect("bad master flag must exit immediately");
        assert_eq!(
            code,
            ExitCode::FAILURE,
            "expected FAILURE for {bad_master_argv:?}"
        );
    }

    let bad_worker_cli = Cli::try_parse_from([
        "oxideswarm",
        "worker",
        "--master",
        "127.0.0.1:8088",
        "--wire-codec",
        "protobuf",
    ])
    .unwrap();
    let Commands::Worker(bad_w_args) = bad_worker_cli.command else {
        panic!("expected Worker");
    };
    let bad_w_code = tokio::time::timeout(
        Duration::from_secs(5),
        run_worker(bad_w_args, None, secret_opts.clone()),
    )
    .await
    .expect("bad worker --wire-codec must exit immediately");
    assert_eq!(bad_w_code, ExitCode::FAILURE);

    // Client commands also reject an invalid `RUSTY_GRID_WIRE_CODEC` instead of silently ignoring it.
    std::env::set_var("RUSTY_GRID_WIRE_CODEC", "xml");
    let bad_client_err = connect_master("127.0.0.1:8088", Some(&test_secret))
        .await
        .err()
        .expect("client connect_master must reject invalid RUSTY_GRID_WIRE_CODEC");
    assert!(
        matches!(bad_client_err, GridError::Config(ref msg) if msg.contains("RUSTY_GRID_WIRE_CODEC")),
        "expected GridError::Config for bad RUSTY_GRID_WIRE_CODEC, got: {bad_client_err:?}"
    );
    std::env::remove_var("RUSTY_GRID_WIRE_CODEC");

    // 3. `--max-queue-size 2` rejects the 3rd submit, and `RUSTY_GRID_WIRE_CODEC=json`
    //    takes effect on the master when `--wire-codec` is omitted on the CLI.
    //    Also test `[submit] timeout_secs` via `run_submit_with_secret` and `run_mapreduce`.
    let tmp_blob = tempfile::tempdir().unwrap();
    let probe_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let master_addr = probe_listener.local_addr().unwrap();
    drop(probe_listener);
    let master_addr_str = master_addr.to_string();
    let blob_dir_str = tmp_blob.path().to_str().unwrap().to_string();

    std::env::set_var("RUSTY_GRID_WIRE_CODEC", "json");
    let resolved_env_codec = resolve_wire_codec(None, Some("bincode".into())).unwrap();
    assert_eq!(
        resolved_env_codec,
        WireCodec::Json,
        "RUSTY_GRID_WIRE_CODEC=json must beat config file and default"
    );
    assert_eq!(
        resolve_wire_codec(Some("bincode".into()), Some("json".into())).unwrap(),
        WireCodec::Bincode,
        "--wire-codec CLI flag must beat RUSTY_GRID_WIRE_CODEC env var and config file"
    );

    let master_cli = Cli::try_parse_from([
        "oxideswarm",
        "master",
        "--listen",
        &master_addr_str,
        "--web-ui-addr",
        "127.0.0.1:0",
        "--max-queue-size",
        "2",
        "--scheduler-policy",
        "round-robin",
        "--state-dir",
        &blob_dir_str,
        "--blob-dir",
        &blob_dir_str,
    ])
    .unwrap();
    let Commands::Master(master_args) = master_cli.command else {
        panic!("expected Master");
    };
    let master_task = tokio::spawn(run_master(master_args, None, secret_opts.clone()));

    let mut bound = false;
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&master_addr).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(bound, "master bound and accepted probe connection");
    // Clear `RUSTY_GRID_WIRE_CODEC` BEFORE connecting the client so the client speaks
    // `WireCodec::Bincode` (0x02). A master configured with `RUSTY_GRID_WIRE_CODEC=json`
    // must still reply in `WireCodec::Json` (0x01) instead of mirroring the client's bincode.
    std::env::remove_var("RUSTY_GRID_WIRE_CODEC");

    let mut transport = connect_master(&master_addr_str, Some(&test_secret))
        .await
        .expect("bincode client connected to json master");
    assert_eq!(
        transport.codec(),
        WireCodec::Bincode,
        "client transport must send bincode after RUSTY_GRID_WIRE_CODEC is cleared"
    );
    transport
        .send_msg(&ClientMessage::ClusterStatus)
        .await
        .unwrap();
    let (reply, detected_codec) = transport
        .recv_msg_with_codec::<ClientResponse>()
        .await
        .unwrap()
        .expect("master sent a ClusterStatus reply");
    assert!(matches!(reply, ClientResponse::ClusterStatus { .. }));
    assert_eq!(
        detected_codec,
        WireCodec::Json,
        "master started with RUSTY_GRID_WIRE_CODEC=json must emit WireCodec::Json (0x01) even to a bincode client"
    );

    // Submit 1st task (without `--timeout-secs`, with `[submit] timeout_secs = 42` in ConfigFile):
    let submit_cfg_file = ConfigFile {
        master: Some(MasterFileConfig::default()),
        submit: Some(SubmitFileConfig {
            timeout_secs: Some(42),
            ..Default::default()
        }),
        ..Default::default()
    };
    let sub1 = submit_args(&["--master", &master_addr_str, "--", "echo", "one"]);
    assert_eq!(
        run_submit_with_secret(sub1, Some(submit_cfg_file.clone()), secret_opts.clone()).await,
        ExitCode::SUCCESS,
        "1st submit must succeed under --max-queue-size 2"
    );

    // Submit 2nd task (with explicit `--timeout-secs 15`, which overrides `[submit] timeout_secs = 42`):
    let sub2 = submit_args(&[
        "--master",
        &master_addr_str,
        "--timeout-secs",
        "15",
        "--",
        "echo",
        "two",
    ]);
    assert_eq!(
        run_submit_with_secret(sub2, Some(submit_cfg_file.clone()), secret_opts.clone()).await,
        ExitCode::SUCCESS,
        "2nd submit must succeed under --max-queue-size 2"
    );

    // Submit 3rd task: must be rejected because `--max-queue-size 2` is full!
    let sub3 = submit_args(&["--master", &master_addr_str, "--", "echo", "three"]);
    assert_eq!(
        run_submit_with_secret(sub3, Some(submit_cfg_file), secret_opts.clone()).await,
        ExitCode::FAILURE,
        "3rd submit must fail when --max-queue-size 2 is full"
    );

    // Also verify via direct transport that the error message is `QueueFull` with actionable guidance.
    let third_direct = exchange(
        &mut transport,
        &master_addr_str,
        true,
        &ClientMessage::SubmitTask {
            task: Task::new(
                TaskSpec::command("echo", vec!["three".into()]),
                TaskRequirements::generic(1, 30),
            ),
            wait: false,
        },
        "submit failed",
    )
    .await
    .unwrap();
    match third_direct {
        ClientResponse::Error { message } => {
            assert!(
                message.contains(
                    "Task queue is full (2 queued or running tasks; wait, or raise --max-queue-size)"
                ),
                "expected actionable QueueFull (2) error message, got: {message}"
            );
        }
        other => panic!("expected ClientResponse::Error for 3rd submit, got {other:?}"),
    }

    master_task.abort();

    // 4. Verify `[submit] timeout_secs` sets `timeout_secs` on the wire for both `submit`
    //    and `mapreduce` when `--timeout-secs` is omitted on the CLI, and yields when
    //    `--timeout-secs` is explicitly passed.
    let capture_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let capture_addr = capture_listener.local_addr().unwrap().to_string();
    let server_secret = test_secret.clone();
    let capture_server = tokio::spawn(async move {
        let mut timeouts = Vec::new();
        for _ in 0..4 {
            let (stream, _) = capture_listener.accept().await.unwrap();
            let mut t = MessageTransport::new(stream);
            let session_key =
                rusty_grid_core::cluster_auth::server_handshake(&mut t, &server_secret)
                    .await
                    .unwrap();
            t.enable_encryption(
                &session_key,
                rusty_grid_core::protocol::EndpointRole::Server,
            );
            while let Some(in_msg) = t
                .recv_msg::<rusty_grid_core::protocol::InboundMessage>()
                .await
                .unwrap()
            {
                match in_msg {
                    rusty_grid_core::protocol::InboundMessage::Client(ClientMessage::Hello) => {
                        t.send_msg(&ClientResponse::Hello {
                            min_compatible_version:
                                rusty_grid_core::protocol::PROTOCOL_VERSION_MIN_COMPATIBLE,
                        })
                        .await
                        .unwrap();
                    }
                    rusty_grid_core::protocol::InboundMessage::Client(
                        ClientMessage::ClusterStatus,
                    ) => {
                        t.send_msg(&ClientResponse::ClusterStatus {
                            total_tasks: 0,
                            pending_tasks: 0,
                            running_tasks: 0,
                            completed_tasks: 0,
                            failed_tasks: 0,
                            active_bytes: 0,
                            max_active_bytes: 0,
                            store_error: None,
                            workers: vec![],
                        })
                        .await
                        .unwrap();
                    }
                    rusty_grid_core::protocol::InboundMessage::Client(
                        ClientMessage::SubmitTask { task, .. },
                    ) => {
                        timeouts.push(task.requirements.timeout_secs);
                        t.send_msg(&ClientResponse::TaskSubmitted { task_id: task.id })
                            .await
                            .unwrap();
                        break;
                    }
                    rusty_grid_core::protocol::InboundMessage::Client(
                        ClientMessage::SubmitMapReduce { job },
                    ) => {
                        timeouts.push(job.timeout_secs);
                        t.send_msg(&ClientResponse::MapReduceCompleted {
                            result: rusty_grid_core::mapreduce::MapReduceResult {
                                job_id: job.job_id,
                                status: "Completed".into(),
                                output: std::collections::HashMap::new(),
                                map_tasks_total: 1,
                                map_tasks_completed: 1,
                                reduce_tasks_total: 1,
                                reduce_tasks_completed: 1,
                                execution_time_ms: 1,
                                error: None,
                            },
                        })
                        .await
                        .unwrap();
                        break;
                    }
                    other => panic!("expected SubmitTask or SubmitMapReduce, got {other:?}"),
                }
            }
        }
        timeouts
    });

    let cfg_timeout_42 = ConfigFile {
        submit: Some(SubmitFileConfig {
            timeout_secs: Some(42),
            ..Default::default()
        }),
        ..Default::default()
    };
    let s_default = submit_args(&["--master", &capture_addr, "--", "echo", "from-cfg"]);
    assert_eq!(
        run_submit_with_secret(s_default, Some(cfg_timeout_42.clone()), secret_opts.clone()).await,
        ExitCode::SUCCESS
    );
    let s_override = submit_args(&[
        "--master",
        &capture_addr,
        "--timeout-secs",
        "15",
        "--",
        "echo",
        "from-cli",
    ]);
    assert_eq!(
        run_submit_with_secret(
            s_override,
            Some(cfg_timeout_42.clone()),
            secret_opts.clone()
        )
        .await,
        ExitCode::SUCCESS
    );

    let mr_default_cli = Cli::try_parse_from([
        "oxideswarm",
        "mapreduce",
        "--master",
        &capture_addr,
        "--input",
        "hello world",
        "--chunks",
        "1",
    ])
    .unwrap();
    let Commands::Mapreduce(mr_default_args) = mr_default_cli.command else {
        panic!("expected Mapreduce");
    };
    assert_eq!(
        run_mapreduce(
            mr_default_args,
            Some(cfg_timeout_42.clone()),
            secret_opts.clone()
        )
        .await,
        ExitCode::SUCCESS
    );

    let mr_override_cli = Cli::try_parse_from([
        "oxideswarm",
        "mapreduce",
        "--master",
        &capture_addr,
        "--input",
        "hello world",
        "--chunks",
        "1",
        "--timeout-secs",
        "15",
    ])
    .unwrap();
    let Commands::Mapreduce(mr_override_args) = mr_override_cli.command else {
        panic!("expected Mapreduce");
    };
    assert_eq!(
        run_mapreduce(mr_override_args, Some(cfg_timeout_42), secret_opts.clone()).await,
        ExitCode::SUCCESS
    );

    let captured_timeouts = capture_server.await.unwrap();
    assert_eq!(
        captured_timeouts,
        vec![42, 15, 42, 15],
        "[submit].timeout_secs must set 42 for both submit and mapreduce when --timeout-secs is omitted and yield to 15 when --timeout-secs 15 is passed"
    );

    // 5. Source-prefixed error strings for `resolve_wire_codec` and `resolve_scheduler_policy`:
    //    CLI flag -> `--wire-codec: ...` / `--scheduler-policy: ...`
    //    Env var  -> `RUSTY_GRID_WIRE_CODEC: ...` / `RUSTY_GRID_SCHEDULER_POLICY: ...`
    //    Config   -> `wire_codec: ...` / `scheduler_policy: ...`
    std::env::remove_var("RUSTY_GRID_WIRE_CODEC");
    std::env::remove_var("RUSTY_GRID_SCHEDULER_POLICY");
    assert!(
        resolve_wire_codec(Some("xml".into()), None)
            .unwrap_err()
            .starts_with("--wire-codec: "),
        "CLI wire codec error must start with '--wire-codec: '"
    );
    assert!(
        resolve_wire_codec(None, Some("xml".into()))
            .unwrap_err()
            .starts_with("wire_codec: "),
        "Config wire codec error must start with 'wire_codec: '"
    );
    std::env::set_var("RUSTY_GRID_WIRE_CODEC", "xml");
    assert!(
        resolve_wire_codec(None, Some("json".into()))
            .unwrap_err()
            .starts_with("RUSTY_GRID_WIRE_CODEC: "),
        "Env wire codec error must start with 'RUSTY_GRID_WIRE_CODEC: '"
    );
    std::env::remove_var("RUSTY_GRID_WIRE_CODEC");

    assert!(
        resolve_scheduler_policy(Some("bogus".into()), None)
            .unwrap_err()
            .starts_with("--scheduler-policy: "),
        "CLI scheduler policy error must start with '--scheduler-policy: '"
    );
    assert!(
        resolve_scheduler_policy(None, Some("bogus".into()))
            .unwrap_err()
            .starts_with("scheduler_policy: "),
        "Config scheduler policy error must start with 'scheduler_policy: '"
    );
    std::env::set_var("RUSTY_GRID_SCHEDULER_POLICY", "bogus");
    assert!(
        resolve_scheduler_policy(None, Some("round-robin".into()))
            .unwrap_err()
            .starts_with("RUSTY_GRID_SCHEDULER_POLICY: "),
        "Env scheduler policy error must start with 'RUSTY_GRID_SCHEDULER_POLICY: '"
    );
    std::env::remove_var("RUSTY_GRID_SCHEDULER_POLICY");

    // Precedence for `fail_unfulfillable_tasks` via `resolve_fail_unfulfillable_tasks`:
    // CLI `--fail-unfulfillable-tasks` > `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS` > `[master].fail_unfulfillable_tasks` > false
    std::env::remove_var("RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS");

    let parsed_fail_true_cfg: ConfigFile = toml::from_str(
        r#"
[master]
fail_unfulfillable_tasks = true
"#,
    )
    .unwrap();
    let parsed_fail_false_cfg: ConfigFile = toml::from_str(
        r#"
[master]
fail_unfulfillable_tasks = false
"#,
    )
    .unwrap();
    assert!(!resolve_fail_unfulfillable_tasks(None, None));
    assert!(resolve_fail_unfulfillable_tasks(
        None,
        parsed_fail_true_cfg.master.as_ref()
    ));
    std::env::set_var("RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS", "1");
    assert!(
        resolve_fail_unfulfillable_tasks(None, parsed_fail_false_cfg.master.as_ref()),
        "RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS=1 must override config false"
    );
    std::env::set_var("RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS", "0");
    assert!(
        !resolve_fail_unfulfillable_tasks(None, parsed_fail_true_cfg.master.as_ref()),
        "RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS=0 must override config true"
    );
    assert!(
        resolve_fail_unfulfillable_tasks(Some(true), parsed_fail_false_cfg.master.as_ref()),
        "--fail-unfulfillable-tasks CLI flag must override env 0"
    );
    std::env::remove_var("RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS");

    // Precedence & validation for `speed_hold_factor`:
    // CLI `--speed-hold-factor` > `RUSTY_GRID_SPEED_HOLD_FACTOR` > `[master].speed_hold_factor` > 1.5
    std::env::remove_var("RUSTY_GRID_SPEED_HOLD_FACTOR");
    let parsed_hold_cfg: ConfigFile = toml::from_str(
        r#"
[master]
speed_hold_factor = 2.25
"#,
    )
    .unwrap();
    assert_eq!(resolve_speed_hold_factor(None, None).unwrap(), 1.5);
    assert_eq!(
        resolve_speed_hold_factor(
            None,
            parsed_hold_cfg.master.as_ref().unwrap().speed_hold_factor
        )
        .unwrap(),
        2.25
    );
    std::env::set_var("RUSTY_GRID_SPEED_HOLD_FACTOR", "0");
    assert_eq!(
        resolve_speed_hold_factor(
            None,
            parsed_hold_cfg.master.as_ref().unwrap().speed_hold_factor
        )
        .unwrap(),
        0.0,
        "RUSTY_GRID_SPEED_HOLD_FACTOR=0 must override config 2.25 and disable D2"
    );
    assert_eq!(
        resolve_speed_hold_factor(
            Some("1.75".into()),
            parsed_hold_cfg.master.as_ref().unwrap().speed_hold_factor
        )
        .unwrap(),
        1.75,
        "--speed-hold-factor CLI flag must override env and config"
    );
    for bad in ["1.0", "0.5", "-1", "nan", "inf", "not-a-float"] {
        assert!(
            resolve_speed_hold_factor(Some(bad.into()), None)
                .unwrap_err()
                .starts_with("--speed-hold-factor: "),
            "CLI error for '{bad}' must start with '--speed-hold-factor: '"
        );
        std::env::set_var("RUSTY_GRID_SPEED_HOLD_FACTOR", bad);
        assert!(
            resolve_speed_hold_factor(None, None)
                .unwrap_err()
                .starts_with("RUSTY_GRID_SPEED_HOLD_FACTOR: "),
            "Env error for '{bad}' must start with 'RUSTY_GRID_SPEED_HOLD_FACTOR: '"
        );
    }
    std::env::remove_var("RUSTY_GRID_SPEED_HOLD_FACTOR");
    for bad_cfg in [1.0, 0.5, -1.0, f64::NAN, f64::INFINITY] {
        assert!(
            resolve_speed_hold_factor(None, Some(bad_cfg))
                .unwrap_err()
                .starts_with("speed_hold_factor: "),
            "Config error for '{bad_cfg}' must start with 'speed_hold_factor: '"
        );
    }

    // 6. Live `run_master` verification of `sched_config`:
    //    - `--scheduler-policy round-robin` rotates two 1-core task submissions across two
    //      connected workers (`rr-w1` with 8 cores at 0% CPU and `rr-w2` with 2 cores at 50% CPU)
    //      that `SchedulerConfig::default()` (`WeightedLeastLoaded`) would both assign to `rr-w1`.
    //    - `[master].fail_unfulfillable_tasks = true` (from parsed TOML `FileConfig`, without
    //      passing `--fail-unfulfillable-tasks` on the CLI) immediately fails a task whose
    //      placement constraints no registered worker can satisfy (`--require-tag nothing --wait`),
    //      waking `--wait` with `ExitCode::FAILURE` and
    //      `"No registered worker matches the placement constraints (tag=nothing)"`.
    let rr_blob = tempfile::tempdir().unwrap();
    let rr_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rr_addr = rr_probe.local_addr().unwrap();
    drop(rr_probe);
    let rr_addr_str = rr_addr.to_string();
    let rr_blob_str = rr_blob.path().to_str().unwrap().to_string();

    let rr_master_cli = Cli::try_parse_from([
        "oxideswarm",
        "master",
        "--listen",
        &rr_addr_str,
        "--web-ui-addr",
        "127.0.0.1:0",
        "--scheduler-policy",
        "round-robin",
        "--state-dir",
        &rr_blob_str,
        "--blob-dir",
        &rr_blob_str,
    ])
    .unwrap();
    let Commands::Master(rr_master_args) = rr_master_cli.command else {
        panic!("expected Master");
    };
    let rr_secret_opts = secret_opts;
    let rr_master_task = tokio::spawn(run_master(
        rr_master_args,
        Some(parsed_fail_true_cfg),
        rr_secret_opts.clone(),
    ));

    let mut rr_bound = false;
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&rr_addr).await.is_ok() {
            rr_bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(rr_bound, "round-robin master bound");

    // Connect two simulated workers over TCP to `rr_addr_str`:
    // - `rr-w1`: 8 cores (`slots:8`), 0% CPU usage -> weighted ratio 0/8=0.0 on task 1, 1/8=0.125 on task 2.
    // - `rr-w2`: 2 cores (`slots:2`), 50% CPU usage -> weighted ratio 0/2 + 0.5*0.50 = 0.250 on both tasks.
    // Under `WeightedLeastLoaded` (`SchedulerConfig::default()`), both tasks go to `rr-w1`;
    // under `RoundRobin`, one goes to `rr-w1` and one goes to `rr-w2`.
    let mut worker_transports = Vec::new();
    for (w_name, cores, cpu_pct) in [("rr-w1", 8usize, 0.0f32), ("rr-w2", 2usize, 50.0f32)] {
        let stream = tokio::net::TcpStream::connect(&rr_addr_str).await.unwrap();
        let mut wt = MessageTransport::new(stream);
        let session_key = rusty_grid_core::cluster_auth::client_handshake(&mut wt, &test_secret)
            .await
            .unwrap();
        wt.enable_encryption(
            &session_key,
            rusty_grid_core::protocol::EndpointRole::Client,
        );
        let w_id = uuid::Uuid::new_v4();
        let mut caps = rusty_grid_core::capabilities::WorkerCapabilities::new(
            w_name, cores, 8192, false, false, None,
        );
        caps.tags.push(format!("slots:{cores}"));
        wt.send_msg(&rusty_grid_core::protocol::WorkerMessage::Register {
            worker_id: w_id,
            capabilities: caps,
            protocol_version: rusty_grid_core::protocol::PROTOCOL_VERSION_CURRENT,
            features: rusty_grid_core::protocol::ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
        let ack = wt
            .recv_msg::<rusty_grid_core::protocol::MasterMessage>()
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            ack,
            rusty_grid_core::protocol::MasterMessage::RegisterAck { accepted: true, .. }
        ));
        if cpu_pct > 0.0 {
            wt.send_msg(&rusty_grid_core::protocol::WorkerMessage::Heartbeat {
                worker_id: w_id,
                timestamp: 1,
                active_tasks: 0,
                cpu_usage_pct: cpu_pct,
                ram_available_mb: 8192,
            })
            .await
            .unwrap();
            let hb_ack = wt
                .recv_msg::<rusty_grid_core::protocol::MasterMessage>()
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                hb_ack,
                rusty_grid_core::protocol::MasterMessage::HeartbeatAck { .. }
            ));
        }
        worker_transports.push(wt);
    }

    // 6a. Submit an unplaceable task (`--require-tag nothing --wait`): must fail immediately!
    let unfulfillable_sub = submit_args(&[
        "--master",
        &rr_addr_str,
        "--require-tag",
        "nothing",
        "--wait",
        "--",
        "echo",
        "impossible",
    ]);
    let unfulfillable_code = tokio::time::timeout(
        Duration::from_secs(5),
        run_submit_with_secret(unfulfillable_sub, None, rr_secret_opts.clone()),
    )
    .await
    .expect("--fail-unfulfillable-tasks must wake --wait immediately instead of hanging");
    assert_eq!(
        unfulfillable_code,
        ExitCode::FAILURE,
        "unplaceable task with --wait must exit with FAILURE when --fail-unfulfillable-tasks is active"
    );

    // Also check via direct client transport that a submitted unfulfillable task's `TaskCompleted`
    // response carries the exact mismatch reason.
    let mut rr_client = connect_master(&rr_addr_str, Some(&test_secret))
        .await
        .unwrap();
    let bad_task = Task::new(
        TaskSpec::command("echo", vec!["nope".into()]),
        TaskRequirements::generic(1, 30),
    )
    .with_tags(vec!["require:tag=nothing".to_string()]);
    let bad_direct = exchange(
        &mut rr_client,
        &rr_addr_str,
        true,
        &ClientMessage::SubmitTask {
            task: bad_task,
            wait: true,
        },
        "submit failed",
    )
    .await
    .unwrap();
    match bad_direct {
        ClientResponse::TaskCompleted { result, .. } => {
            assert_eq!(result.exit_code, 1);
            assert_eq!(
                result.error.as_deref(),
                Some("No registered worker matches the placement constraints (tag=nothing)")
            );
        }
        other => panic!(
            "expected ClientResponse::TaskCompleted for unfulfillable wait=true, got {other:?}"
        ),
    }

    // 6b. Submit 2 placeable tasks and verify `round-robin` assigns one to `rr-w1` and one to `rr-w2`.
    let [mut wt1, mut wt2] = <[_; 2]>::try_from(worker_transports).ok().unwrap();

    for label in ["rr-1", "rr-2"] {
        let sub_rr = submit_args(&["--master", &rr_addr_str, "--", "echo", label]);
        assert_eq!(
            run_submit_with_secret(sub_rr, None, rr_secret_opts.clone()).await,
            ExitCode::SUCCESS
        );
    }

    let got_wt1 = tokio::time::timeout(
        Duration::from_secs(3),
        wt1.recv_msg::<rusty_grid_core::protocol::MasterMessage>(),
    )
    .await
    .expect("wt1 must receive one AssignTask under round-robin")
    .unwrap();
    assert!(matches!(
        got_wt1,
        Some(rusty_grid_core::protocol::MasterMessage::AssignTask { .. })
    ));

    let got_wt2 = tokio::time::timeout(
        Duration::from_secs(3),
        wt2.recv_msg::<rusty_grid_core::protocol::MasterMessage>(),
    )
    .await
    .expect("wt2 must receive one AssignTask under round-robin (WeightedLeastLoaded would send both to wt1)")
    .unwrap();
    assert!(matches!(
        got_wt2,
        Some(rusty_grid_core::protocol::MasterMessage::AssignTask { .. })
    ));

    rr_master_task.abort();
}

#[tokio::test]
async fn test_submit_compile_builds_crate_with_local_dependency_and_avoids_worker_without_cargo() {
    use rusty_grid_core::capabilities::MobileCapabilities;
    use rusty_grid_master::reaper::ReaperConfig;
    use rusty_grid_master::registry::WorkerStatus;
    use rusty_grid_master::scheduler::SchedulerConfig;
    use rusty_grid_master::server::{MasterServer, ServerConfig};
    use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
    use rusty_grid_worker::MobileTelemetryHandle;
    use tokio::sync::watch;

    let temp = tempfile::tempdir().unwrap();
    let secret = rusty_grid_core::cluster_auth::ClusterSecret::generate().unwrap();
    let secret_opts = SecretOpts {
        value: Some(SecretText(secret.to_hex())),
        file: None,
    };

    let master_blob_dir = temp.path().join("master-blobs");
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(false)
        .with_cluster_secret(secret.clone())
        .with_blob_store(&master_blob_dir, 256 * 1024 * 1024);
    let master = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master starts");
    let master_addr = master.server_addr().to_string();

    let plugged_in = || {
        MobileTelemetryHandle::new(Some(MobileCapabilities {
            battery_pct: Some(100),
            is_charging: Some(true),
            ..MobileCapabilities::default()
        }))
    };

    // Worker 1: does NOT advertise `tool:cargo`.
    let w1_sandbox = temp.path().join("w1-sandbox");
    let w1_config = WorkerConfig::new(master_addr.clone())
        .with_cluster_secret(secret.clone())
        .with_no_battery(true)
        .with_name("worker-no-cargo")
        .with_cores(2)
        .with_ram_mb(2048)
        .with_sandbox_base_dir(&w1_sandbox)
        .with_mobile_telemetry(plugged_in())
        .with_redirection_portal(false);
    let w1_client = WorkerClient::new(w1_config);
    let mut w1_caps = w1_client.capabilities().clone();
    w1_caps.tags.retain(|t| t != "tool:cargo");
    let mut w1_client = w1_client.with_capabilities(w1_caps);
    let w1_id = w1_client.worker_id();
    let (w1_stop_tx, w1_stop_rx) = watch::channel(false);
    let w1_handle = tokio::spawn(async move {
        let _ = w1_client.run(w1_stop_rx).await;
    });

    // Worker 2: advertises `tool:cargo` and `tool:rustc` (probed from host at start-up).
    let w2_sandbox = temp.path().join("w2-sandbox");
    let w2_config = WorkerConfig::new(master_addr.clone())
        .with_cluster_secret(secret.clone())
        .with_no_battery(true)
        .with_name("worker-with-cargo")
        .with_cores(2)
        .with_ram_mb(2048)
        .with_sandbox_base_dir(&w2_sandbox)
        .with_mobile_telemetry(plugged_in())
        .with_redirection_portal(false);
    let mut w2_client = WorkerClient::new(w2_config);
    assert!(
        w2_client
            .capabilities()
            .tags
            .contains(&"tool:cargo".to_string()),
        "host worker must advertise tool:cargo"
    );
    let w2_id = w2_client.worker_id();
    let (w2_stop_tx, w2_stop_rx) = watch::channel(false);
    let w2_handle = tokio::spawn(async move {
        let _ = w2_client.run(w2_stop_rx).await;
    });

    // Wait until both workers are registered on the master.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let workers = master.list_workers().await.unwrap_or_default();
        let connected = workers
            .iter()
            .filter(|w| w.status != WorkerStatus::Disconnected)
            .count();
        if connected >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for both workers to register"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Create a crate with a local path dependency (passed via `--source` + `--input`).
    let crate_dir = temp.path().join("app_crate");
    std::fs::create_dir_all(crate_dir.join("src")).unwrap();
    std::fs::write(
        crate_dir.join("Cargo.toml"),
        r#"[package]
name = "feat10_cli_bin"
version = "0.1.0"
edition = "2021"

[dependencies]
local_math = { path = "local_math" }
"#,
    )
    .unwrap();
    std::fs::write(
        crate_dir.join("src/main.rs"),
        "fn main() { println!(\"sum={}\", local_math::add(19, 23)); }\n",
    )
    .unwrap();

    let dep_dir = temp.path().join("local_math");
    std::fs::create_dir_all(dep_dir.join("src")).unwrap();
    std::fs::write(
        dep_dir.join("Cargo.toml"),
        r#"[package]
name = "local_math"
version = "0.1.0"
edition = "2021"
"#,
    )
    .unwrap();
    std::fs::write(
        dep_dir.join("src/lib.rs"),
        "pub fn add(a: u32, b: u32) -> u32 { a + b }\n",
    )
    .unwrap();

    let client_out_dir = temp.path().join("client-out");
    let input_spec = format!("{}:local_math", dep_dir.display());
    // Note: no explicit `--require-tool cargo` flag is passed; `submit --type compile`
    // automatically adds `require:tool=cargo` because `Cargo.toml` is in `source_files`.
    let s_args = submit_args(&[
        "--master",
        &master_addr,
        "--type",
        "compile",
        "--source",
        crate_dir.to_str().unwrap(),
        "--input",
        &input_spec,
        "--output-dir",
        client_out_dir.to_str().unwrap(),
    ]);

    let exit = run_submit_with_secret(s_args, None, secret_opts).await;
    assert_eq!(
        exit,
        ExitCode::SUCCESS,
        "submit --type compile must succeed"
    );

    // Verify the task ran on Worker 2 (`worker-with-cargo`) and avoided Worker 1 (`worker-no-cargo`).
    let tasks = master.queue().list_tasks().await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].assigned_worker_id, Some(w2_id));
    assert_ne!(tasks[0].assigned_worker_id, Some(w1_id));

    // Verify the compiled binary reached the client output directory and runs!
    let bin_name = if cfg!(windows) {
        "feat10_cli_bin.exe"
    } else {
        "feat10_cli_bin"
    };
    let downloaded_bin = client_out_dir.join("target").join(bin_name);
    assert!(
        downloaded_bin.is_file(),
        "expected compiled binary at {}",
        downloaded_bin.display()
    );
    let run_out = std::process::Command::new(&downloaded_bin)
        .output()
        .expect("run downloaded binary");
    assert!(run_out.status.success(), "{run_out:?}");
    assert_eq!(String::from_utf8_lossy(&run_out.stdout).trim(), "sum=42");

    let _ = w1_stop_tx.send(true);
    let _ = w2_stop_tx.send(true);
    let _ = w1_handle.await;
    let _ = w2_handle.await;
    let _ = master.shutdown();
}

#[test]
fn test_submit_workspace_flag_parsing_and_conflicts() {
    let parsed = submit_args(&[
        "--workspace",
        "oxideswarm",
        "--sync",
        "crates/core",
        "--cwd",
        "src",
        "--workspace-max-files",
        "500",
        "--workspace-max-mb",
        "64",
        "--command",
        "cargo",
        "--",
        "test",
    ]);
    assert_eq!(parsed.workspace.as_deref(), Some("oxideswarm"));
    assert_eq!(parsed.sync.as_deref(), Some(Path::new("crates/core")));
    assert!(!parsed.no_sync);
    assert_eq!(parsed.cwd.as_deref(), Some("src"));
    assert_eq!(parsed.workspace_max_files, Some(500));
    assert_eq!(parsed.workspace_max_mb, Some(64));

    let no_sync = submit_args(&[
        "--workspace",
        "oxideswarm",
        "--no-sync",
        "--cwd",
        "crates/core",
        "--command",
        "cargo",
    ]);
    assert_eq!(no_sync.workspace.as_deref(), Some("oxideswarm"));
    assert!(no_sync.no_sync);
    assert!(no_sync.sync.is_none());
    assert_eq!(no_sync.cwd.as_deref(), Some("crates/core"));

    // Validation and conflicts enforced by `build_task_spec`:
    let spec = build_task_spec(&parsed).expect("valid workspace command spec");
    assert!(matches!(
        spec,
        TaskSpec::Command {
            working_dir: Some(ref wd),
            ..
        } if wd == Path::new("src")
    ));

    assert!(
        build_task_spec(&submit_args(&[
            "--workspace",
            "oxideswarm",
            "--input",
            "Cargo.toml",
            "--command",
            "cargo",
        ]))
        .unwrap_err()
        .contains("--workspace cannot be combined with --input"),
        "--workspace and --input must conflict"
    );
    assert!(
        build_task_spec(&submit_args(&[
            "--workspace",
            "oxideswarm",
            "--sync",
            ".",
            "--no-sync",
            "--command",
            "cargo",
        ]))
        .unwrap_err()
        .contains("--sync cannot be combined with --no-sync"),
        "--sync and --no-sync must conflict"
    );
    assert!(
        build_task_spec(&submit_args(&["--cwd", "src", "--command", "cargo"]))
            .unwrap_err()
            .contains("--cwd requires --workspace"),
        "--cwd requires --workspace"
    );
    assert!(
        build_task_spec(&submit_args(&["--sync", ".", "--command", "cargo"]))
            .unwrap_err()
            .contains("--sync requires --workspace"),
        "--sync requires --workspace"
    );
    assert!(
        build_task_spec(&submit_args(&["--no-sync", "--command", "cargo"]))
            .unwrap_err()
            .contains("--no-sync requires --workspace"),
        "--no-sync requires --workspace"
    );
}

#[tokio::test]
async fn test_submit_workspace_max_files_and_max_mb_enforced_before_connect() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), vec![b'a'; 1024]).unwrap();
    std::fs::write(dir.path().join("b.txt"), vec![b'b'; 1024]).unwrap();

    // Exceeds `--workspace-max-files 1`: fails in `scan_workspace_source` before connecting.
    let too_many_files = submit_args(&[
        "--master",
        "127.0.0.1:1",
        "--workspace",
        "oxideswarm",
        "--sync",
        dir.path().to_str().unwrap(),
        "--workspace-max-files",
        "1",
        "--command",
        "echo",
    ]);
    let code = run_submit(too_many_files, None).await;
    assert_eq!(code, ExitCode::FAILURE);

    // Write a >1 MiB file so total size exceeds `--workspace-max-mb 1` (1 MiB): fails in
    // `scan_workspace_source` before connecting.
    std::fs::write(
        dir.path().join("big.bin"),
        vec![b'x'; (MIB as usize) + 1024],
    )
    .unwrap();
    let too_many_bytes = submit_args(&[
        "--master",
        "127.0.0.1:1",
        "--workspace",
        "oxideswarm",
        "--sync",
        dir.path().to_str().unwrap(),
        "--workspace-max-mb",
        "1",
        "--command",
        "echo",
    ]);
    let code = run_submit(too_many_bytes, None).await;
    assert_eq!(code, ExitCode::FAILURE);
}

#[test]
fn test_relay_url_flag_parses_and_precedence_holds_across_roles() {
    let id = Uuid::new_v4().to_string();
    let relay_of = |args: &[&str]| match parse(args).command {
        Commands::Master(a) => a.relay_url,
        Commands::Worker(a) => a.relay_url,
        Commands::Submit(a) => a.relay_url,
        Commands::Status(a) => a.relay_url,
        Commands::Cancel(a) => a.relay_url,
        Commands::Result(a) => a.relay_url,
        Commands::Workers(a) => a.relay_url,
        Commands::Workspaces(a) => a.relay_url,
        Commands::Mapreduce(a) => a.relay_url,
        Commands::Doctor(a) => a.relay_url,
        other => panic!("unexpected {other:?}"),
    };

    for with_flag in [
        vec!["master", "--relay-url", "https://relay.example.com"],
        vec!["worker", "--relay-url", "https://relay.example.com"],
        vec![
            "submit",
            "--relay-url",
            "https://relay.example.com",
            "--",
            "true",
        ],
        vec!["status", "--relay-url", "https://relay.example.com"],
        vec!["cancel", &id, "--relay-url", "https://relay.example.com"],
        vec!["result", &id, "--relay-url", "https://relay.example.com"],
        vec!["workers", "--relay-url", "https://relay.example.com"],
        vec!["workspaces", "--relay-url", "https://relay.example.com"],
        vec![
            "mapreduce",
            "--input",
            "x",
            "--relay-url",
            "https://relay.example.com",
        ],
        vec!["doctor", "--relay-url", "https://relay.example.com"],
    ] {
        let refs: Vec<&str> = with_flag.iter().map(AsRef::as_ref).collect();
        assert_eq!(
            relay_of(&refs).as_deref(),
            Some("https://relay.example.com"),
            "failed for {with_flag:?}"
        );
    }

    // Default when unset: None -> RelayMode::Default.
    assert_eq!(
        resolve_relay_url_from(None, None, None, false).unwrap(),
        None
    );

    // Precedence: CLI flag > RUSTY_GRID_RELAY_URL env > config file > default (None).
    let cfg: ConfigFile = toml::from_str(
        r#"
[master]
relay_url = "https://cfg-master.relay.example.com"

[worker]
relay_url = "https://cfg-worker.relay.example.com"

[client]
relay_url = "https://cfg-client.relay.example.com"
"#,
    )
    .unwrap();

    for (role_cfg, expected_cfg) in [
        (
            cfg.master_relay_url(),
            "https://cfg-master.relay.example.com",
        ),
        (
            cfg.worker_relay_url(),
            "https://cfg-worker.relay.example.com",
        ),
        (
            cfg.submit_relay_url(),
            "https://cfg-client.relay.example.com",
        ),
        (
            cfg.status_relay_url(),
            "https://cfg-client.relay.example.com",
        ),
    ] {
        // Tier 3: config file wins over default (None).
        let from_cfg = resolve_relay_url_from(None, None, role_cfg.clone(), false)
            .unwrap()
            .unwrap();
        assert_eq!(from_cfg, expected_cfg);

        // Tier 2: RUSTY_GRID_RELAY_URL beats config file.
        let from_env = resolve_relay_url_from(
            None,
            Some("https://env.relay.example.com:8443"),
            role_cfg.clone(),
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(from_env, "https://env.relay.example.com:8443");

        // Tier 1: --relay-url CLI flag beats both env and config file.
        let from_cli = resolve_relay_url_from(
            Some("https://cli.relay.example.com:9443".into()),
            Some("https://env.relay.example.com:8443"),
            role_cfg,
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(from_cli, "https://cli.relay.example.com:9443");

        // Resolved URL builds a custom iroh RelayMap containing that exact RelayUrl.
        let expected_relay_url = rusty_grid_core::transport::validate_relay_url(&from_cli).unwrap();
        let map = rusty_grid_core::transport::build_custom_relay_map(&from_cli).unwrap();
        assert!(map.contains(&expected_relay_url));
        let urls: Vec<_> = map.urls();
        assert_eq!(urls, vec![expected_relay_url]);
    }

    // Invalid URLs (http://, credentials, garbage) are refused at every tier with the source name.
    for bad in [
        "http://relay.example.com",
        "https://user:secretpass@relay.example.com",
        "not-a-valid-url",
    ] {
        let cli_err = resolve_relay_url_from(Some(bad.into()), None, None, false).unwrap_err();
        assert!(
            cli_err.starts_with("--relay-url: "),
            "expected '--relay-url: ', got: {cli_err}"
        );
        assert!(!cli_err.contains("secretpass"));

        let env_err = resolve_relay_url_from(None, Some(bad), None, false).unwrap_err();
        assert!(
            env_err.starts_with("RUSTY_GRID_RELAY_URL: "),
            "expected 'RUSTY_GRID_RELAY_URL: ', got: {env_err}"
        );
        assert!(!env_err.contains("secretpass"));

        let cfg_err = resolve_relay_url_from(None, None, Some(bad.into()), false).unwrap_err();
        assert!(
            cfg_err.starts_with("relay_url: "),
            "expected 'relay_url: ', got: {cfg_err}"
        );
        assert!(!cfg_err.contains("secretpass"));
    }

    // Combining --relay-url with minimal/no-relay test mode is a clear error.
    let minimal_err =
        resolve_relay_url_from(Some("https://relay.example.com".into()), None, None, true)
            .unwrap_err();
    assert!(
        minimal_err.starts_with("--relay-url: ") && minimal_err.contains("minimal"),
        "got: {minimal_err}"
    );
}

#[tokio::test]
async fn test_run_master_worker_and_client_refuse_invalid_relay_url_before_network() {
    let secret = ClusterSecret::from_bytes(&[0x33; 32]).unwrap();
    let secret_opts = SecretOpts {
        value: Some(SecretText(secret.to_hex())),
        file: None,
    };

    let tmp_state = tempfile::tempdir().unwrap();
    let state_dir_str = tmp_state.path().to_str().unwrap().to_string();
    for bad in [
        "http://relay.example.com",
        "https://user:secret@relay.example.com",
        "garbage-url",
    ] {
        let master_cli = Cli::try_parse_from([
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
            "--state-dir",
            &state_dir_str,
            "--relay-url",
            bad,
        ])
        .unwrap();
        let Commands::Master(m_args) = master_cli.command else {
            panic!("expected Master");
        };
        assert_eq!(
            run_master(m_args, None, secret_opts.clone()).await,
            ExitCode::FAILURE
        );

        let worker_cli = Cli::try_parse_from([
            "oxideswarm",
            "worker",
            "--master",
            "127.0.0.1:1",
            "--relay-url",
            bad,
        ])
        .unwrap();
        let Commands::Worker(w_args) = worker_cli.command else {
            panic!("expected Worker");
        };
        assert_eq!(
            run_worker(w_args, None, secret_opts.clone()).await,
            ExitCode::FAILURE
        );

        let sub_args = submit_args(&["--master", "127.0.0.1:1", "--relay-url", bad]);
        assert_eq!(
            run_submit_with_secret(sub_args, None, secret_opts.clone()).await,
            ExitCode::FAILURE
        );

        let status_cli = Cli::try_parse_from([
            "oxideswarm",
            "status",
            "--master",
            "127.0.0.1:1",
            "--relay-url",
            bad,
        ])
        .unwrap();
        let Commands::Status(st_args) = status_cli.command else {
            panic!("expected Status");
        };
        assert_eq!(
            run_status(st_args, None, secret_opts.clone()).await,
            ExitCode::FAILURE
        );
    }
}

#[tokio::test]
async fn test_worker_workspace_options_parsing_precedence_and_validation() {
    // 1. Default values when nothing is set.
    let cli_default = Cli::try_parse_from(["oxideswarm", "worker"]).unwrap();
    let Commands::Worker(w_default) = cli_default.command else {
        panic!("expected Worker");
    };
    assert!(!w_default.workspaces);
    assert!(w_default.workspace_root.is_none());
    assert!(w_default.workspace_limit_mb.is_none());
    assert!(w_default.max_workspaces.is_none());

    // 2. CLI flags parse cleanly.
    let cli_flags = Cli::try_parse_from([
        "oxideswarm",
        "worker",
        "--workspaces",
        "--workspace-root",
        "/tmp/custom-ws",
        "--workspace-limit-mb",
        "4096",
        "--max-workspaces",
        "12",
    ])
    .unwrap();
    let Commands::Worker(w_flags) = cli_flags.command else {
        panic!("expected Worker");
    };
    assert!(w_flags.workspaces);
    assert_eq!(
        w_flags.workspace_root.as_deref(),
        Some(Path::new("/tmp/custom-ws"))
    );
    assert_eq!(w_flags.workspace_limit_mb, Some(4096));
    assert_eq!(w_flags.max_workspaces, Some(12));

    // 3. Config file TOML deserialization for [worker] workspace keys.
    let parsed_cfg: ConfigFile = toml::from_str(
        r#"
[worker]
workspaces = true
workspace_root = "/var/tmp/ox-ws"
workspace_limit_mb = 10240
max_workspaces = 4
"#,
    )
    .expect("parse TOML");
    let w_cfg = parsed_cfg.worker.as_ref().expect("worker section");
    assert_eq!(w_cfg.workspaces, Some(true));
    assert_eq!(w_cfg.workspace_root.as_deref(), Some("/var/tmp/ox-ws"));
    assert_eq!(w_cfg.workspace_limit_mb, Some(10240));
    assert_eq!(w_cfg.max_workspaces, Some(4));

    // 4. Zero limits are rejected before network connect.
    let secret = ClusterSecret::from_bytes(&[0x44; 32]).unwrap();
    let secret_opts = SecretOpts {
        value: Some(SecretText(secret.to_hex())),
        file: None,
    };
    for bad_args in [
        vec![
            "oxideswarm",
            "worker",
            "--master",
            "127.0.0.1:1",
            "--workspace-limit-mb",
            "0",
        ],
        vec![
            "oxideswarm",
            "worker",
            "--master",
            "127.0.0.1:1",
            "--max-workspaces",
            "0",
        ],
    ] {
        let cli = Cli::try_parse_from(bad_args).unwrap();
        let Commands::Worker(args) = cli.command else {
            panic!("expected Worker");
        };
        assert_eq!(
            run_worker(args, None, secret_opts.clone()).await,
            ExitCode::FAILURE
        );
    }
}

#[test]
fn test_resolve_auth_mode_precedence_and_cli_flags() {
    use rusty_grid_master::server::AuthMode;

    assert_eq!(
        resolve_auth_mode_from(None, None, None).unwrap(),
        AuthMode::Both
    );
    assert_eq!(
        resolve_auth_mode_from(None, None, Some("secret".into())).unwrap(),
        AuthMode::Secret
    );
    assert_eq!(
        resolve_auth_mode_from(None, Some("members"), Some("secret".into())).unwrap(),
        AuthMode::Members
    );
    assert_eq!(
        resolve_auth_mode_from(Some("both".into()), Some("members"), Some("secret".into()))
            .unwrap(),
        AuthMode::Both
    );
    assert!(resolve_auth_mode_from(Some("bogus".into()), None, None).is_err());

    let cli = parse(&["master", "--auth", "members"]);
    match cli.command {
        Commands::Master(m) => assert_eq!(m.auth.as_deref(), Some("members")),
        other => panic!("expected Master, got {other:?}"),
    }

    let hex = "11".repeat(32);
    let cli_w = parse(&["worker", "--master-key", &hex]);
    assert_eq!(cli_w.master_key.as_deref(), Some(hex.as_str()));
}

#[test]
fn test_submit_vram_mb_flag_and_preflight_helper() {
    let with_vram = submit_args(&["--vram-mb", "8192", "--", "python", "train.py"]);
    assert_eq!(with_vram.vram_mb, Some(8192));

    let with_alias = submit_args(&["--vram", "4096", "--", "echo", "ok"]);
    assert_eq!(with_alias.vram_mb, Some(4096));

    let with_zero = submit_args(&["--vram-mb", "0", "--", "echo", "ok"]);
    assert_eq!(with_zero.vram_mb, Some(0));

    let without_vram = submit_args(&["--", "echo", "ok"]);
    assert_eq!(without_vram.vram_mb, None);

    let w_8g =
        WorkerCapabilities::new("gpu-8g", 4, 8192, true, true, None).with_vram_mb(Some(8192));
    let w_4g =
        WorkerCapabilities::new("gpu-4g", 4, 8192, true, true, None).with_vram_mb(Some(4096));
    let w_unknown = WorkerCapabilities::new("gpu-sim", 4, 8192, true, false, None);
    let mut w_disconnected =
        WorkerCapabilities::new("gpu-16g-off", 4, 8192, true, true, None).with_vram_mb(Some(16384));
    w_disconnected
        .tags
        .push(format!("{STATE_TAG_PREFIX}disconnected"));

    assert!(any_connected_worker_satisfies_vram(
        &[w_4g.clone(), w_8g.clone()],
        6144
    ));
    assert!(!any_connected_worker_satisfies_vram(
        &[w_4g, w_unknown, w_disconnected],
        6144
    ));
}

#[test]
fn test_worker_isolation_read_dir_flags_and_precedence() {
    let cli_dirs = match parse(&[
        "worker",
        "--isolation-read-dir",
        "/opt/cuda",
        "--allow-read-dir",
        "/opt/python311",
        "--isolation-read-dir",
        "/opt/cuda",
    ])
    .command
    {
        Commands::Worker(a) => a.isolation_read_dirs,
        other => panic!("expected Worker, got {other:?}"),
    };
    assert_eq!(
        cli_dirs,
        vec![
            PathBuf::from("/opt/cuda"),
            PathBuf::from("/opt/python311"),
            PathBuf::from("/opt/cuda"),
        ]
    );

    let sep = if cfg!(windows) { ";" } else { ":" };
    let env_raw = std::ffi::OsString::from(format!("/env/one{sep} {sep}/env/two{sep}/env/one"));
    let cfg = vec![
        "/cfg/one".to_string(),
        "  ".to_string(),
        "/cfg/two".to_string(),
    ];

    assert_eq!(
        resolve_isolation_read_dirs(&cli_dirs, Some(env_raw.as_os_str()), Some(&cfg)),
        vec![PathBuf::from("/opt/cuda"), PathBuf::from("/opt/python311")],
        "CLI --isolation-read-dir must override env and config"
    );
    assert_eq!(
        resolve_isolation_read_dirs(&[], Some(env_raw.as_os_str()), Some(&cfg)),
        vec![PathBuf::from("/env/one"), PathBuf::from("/env/two")],
        "RUSTY_GRID_ISOLATION_READ_DIRS must override config"
    );
    assert_eq!(
        resolve_isolation_read_dirs(&[], Some(std::ffi::OsStr::new("")), Some(&cfg)),
        vec![PathBuf::from("/cfg/one"), PathBuf::from("/cfg/two")],
        "empty env falls back to [worker].isolation_read_dirs"
    );
    assert!(resolve_isolation_read_dirs(&[], None, None).is_empty());
}
