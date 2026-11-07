//! Integration tests for OxideSwarm Workflow Modes & Presets (`MODE-1`).

use rusty_grid_cli::mode::{
    execute_mode_action, export_specification, find_workspace_root, generate_research_report,
    get_mode_descriptors, launch_local_dev_cluster_with_ports,
    probe_cluster_health_with_fallback_ports, probe_cluster_health_with_secret,
    run_distributed_benchmark, run_hardware_profile, run_micro_benchmarks,
    run_mode_cli_with_secret, run_watch_check, stop_local_dev_cluster,
    stop_local_dev_cluster_with_signaller, verify_documentation, ModeCliArgs, WorkflowMode,
    DEFAULT_FALLBACK_STATUS_PORTS,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::protocol::MAX_FRAME_SIZE;
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::server::{MasterServer, ServerConfig};

#[test]
fn test_workflow_modes_enum_and_descriptors() {
    let modes = WorkflowMode::all();
    assert_eq!(modes.len(), 4);

    let descriptors = get_mode_descriptors();
    assert_eq!(descriptors.len(), 4);

    for desc in descriptors {
        assert!(!desc.id.is_empty());
        assert!(!desc.name.is_empty());
        assert!(!desc.description.is_empty());
        assert!(!desc.actions.is_empty());
        for act in desc.actions {
            assert!(!act.id.is_empty());
            assert!(!act.name.is_empty());
            assert!(!act.description.is_empty());
        }
    }
}

#[test]
fn test_workflow_modes_aliases() {
    assert_eq!(
        WorkflowMode::from_str_loose("test"),
        Some(WorkflowMode::Test)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("tests"),
        Some(WorkflowMode::Test)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("verify"),
        Some(WorkflowMode::Test)
    );
    assert_eq!(WorkflowMode::from_str_loose("qa"), Some(WorkflowMode::Test));

    assert_eq!(WorkflowMode::from_str_loose("dev"), Some(WorkflowMode::Dev));
    assert_eq!(
        WorkflowMode::from_str_loose("develop"),
        Some(WorkflowMode::Dev)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("cluster"),
        Some(WorkflowMode::Dev)
    );

    assert_eq!(WorkflowMode::from_str_loose("doc"), Some(WorkflowMode::Doc));
    assert_eq!(
        WorkflowMode::from_str_loose("docs"),
        Some(WorkflowMode::Doc)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("spec"),
        Some(WorkflowMode::Doc)
    );

    assert_eq!(
        WorkflowMode::from_str_loose("research"),
        Some(WorkflowMode::Research)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("bench"),
        Some(WorkflowMode::Research)
    );
    assert_eq!(
        WorkflowMode::from_str_loose("profile"),
        Some(WorkflowMode::Research)
    );

    assert_eq!(WorkflowMode::from_str_loose("unknown_invalid_xyz"), None);
}

#[test]
fn test_doc_mode_verify_and_export() {
    let root = find_workspace_root();
    assert!(
        root.join("Cargo.toml").exists(),
        "Root must have Cargo.toml"
    );

    // 1. Verify docs on real workspace
    let verify_res = verify_documentation(&root);
    assert!(
        verify_res.success,
        "Doc verification should pass: {:?}\n{}",
        verify_res.stderr, verify_res.stdout
    );
    assert!(verify_res
        .stdout
        .contains("Documentation & Specification Verification"));
    assert_eq!(verify_res.metrics["total_docs"], 5);
    assert!(
        verify_res.metrics["links_checked"].as_u64().unwrap_or(0) > 0,
        "verify_documentation must check relative links in markdown files"
    );
    assert_eq!(verify_res.metrics["broken_links"], 0);

    // 2. Verify broken relative link is detected and fails
    let temp = tempfile::tempdir().expect("tempdir");
    for doc_name in [
        "README.md",
        "CLAUDE.md",
        "PROJECT.md",
        "DEPLOYMENT.md",
        "ANDROID_ARCHITECTURE.md",
    ] {
        std::fs::write(
            temp.path().join(doc_name),
            format!("# {doc_name}\n\nValid text.\n"),
        )
        .expect("write doc");
    }
    std::fs::write(
        temp.path().join("README.md"),
        "# README\n\nSee [missing file](docs/does_not_exist.md) for details.\n",
    )
    .expect("write broken link README");
    let broken_res = verify_documentation(temp.path());
    assert!(
        !broken_res.success,
        "verify_documentation must fail when a relative link target is missing"
    );
    assert_eq!(broken_res.exit_code, Some(1));
    assert_eq!(broken_res.metrics["broken_links"], 1);
    assert!(broken_res.stderr.contains("does_not_exist.md"));

    // 3. Export spec and verify generated facts match real protocol constants
    let export_res = export_specification(&root);
    assert!(export_res.success, "Spec export should pass");
    let target_spec = root.join("target/docs/OXIDESWARM_SPECIFICATION.md");
    assert!(target_spec.exists(), "Exported spec file should exist");
    let spec_text = std::fs::read_to_string(&target_spec).expect("read exported spec");
    assert!(
        spec_text.contains(&MAX_FRAME_SIZE.to_string()),
        "Exported spec must report real MAX_FRAME_SIZE ({MAX_FRAME_SIZE})"
    );
    assert!(
        !spec_text.contains("16,777,216") && !spec_text.contains("max 16MB"),
        "Exported spec must not contain false 16 MB frame limit claims"
    );
    assert!(
        !spec_text.contains("[Payload: JSON]"),
        "Exported spec must not claim JSON-only wire framing"
    );
}

#[test]
fn test_research_mode_profile_and_bench() {
    // 1. Hardware profile
    let profile = run_hardware_profile();
    assert!(profile.success);
    assert!(profile.metrics.contains_key("cpu_cores"));
    assert!(profile.metrics.contains_key("ram_total_mb"));
    assert!(profile.metrics["cpu_cores"].as_u64().unwrap_or(0) > 0);

    // 2. Micro benchmarks (real Task JSON serde + Bincode wire codec roundtrip)
    let bench = run_micro_benchmarks();
    assert!(bench.success);
    assert!(bench.metrics.contains_key("serde_ops_per_sec"));
    assert!(bench.metrics.contains_key("codec_ops_per_sec"));
    assert_eq!(bench.metrics["codec_wire_discriminator"], 2);
    assert!(bench.metrics["codec_frame_bytes"].as_u64().unwrap_or(0) > 16);
    assert_eq!(
        bench.metrics["codec_decoded_task_id"],
        bench.metrics["expected_task_id"]
    );
    assert_eq!(
        bench.metrics["codec_verified_iters"], 10_000,
        "every iteration of the Bincode benchmark loop must verify a unique roundtrip"
    );
    assert!(bench.stdout.contains("Bincode Wire Codec Roundtrip"));
    let serde_ops = bench.metrics["serde_ops_per_sec"].as_u64().unwrap_or(0);
    assert!(
        serde_ops > 100,
        "Serde ops/sec should be > 100, got: {}",
        serde_ops
    );
    let codec_ops = bench.metrics["codec_ops_per_sec"].as_u64().unwrap_or(0);
    assert!(
        codec_ops > 100,
        "Codec ops/sec should be > 100, got: {}",
        codec_ops
    );

    let root = find_workspace_root();
    let mode_cli_src = std::fs::read_to_string(root.join("crates/cli/src/mode_cli.rs"))
        .expect("read crates/cli/src/mode_cli.rs");
    let fn_start = mode_cli_src
        .find("pub fn run_micro_benchmarks()")
        .expect("find run_micro_benchmarks");
    let fn_body = &mode_cli_src[fn_start..fn_start + 4000];
    let t2_start_pos = fn_body
        .find("let t2_start = Instant::now();")
        .expect("find t2_start timer");
    let t2_end_pos = fn_body
        .find("let t2_dur = t2_start.elapsed();")
        .expect("find t2_dur timer");
    let before_t2 = &fn_body[..t2_start_pos];
    let timed_t2 = &fn_body[t2_start_pos..t2_end_pos];
    assert!(
        !before_t2.contains("serialize_message(") && !before_t2.contains("deserialize_message("),
        "serialize_message / deserialize_message must not be called outside the timed t2 loop"
    );
    let loop_pos = timed_t2
        .find("for i in 0..codec_iters")
        .expect("find codec_iters loop inside timed region");
    let loop_body = &timed_t2[loop_pos..];
    assert!(
        loop_body.contains("serialize_message(") && loop_body.contains("deserialize_message("),
        "run_micro_benchmarks must call serialize_message + deserialize_message inside the timed for-loop"
    );
    assert!(
        !fn_body.contains("extend_from_slice") && !fn_body.contains("copy_from_slice"),
        "run_micro_benchmarks must not use a fake byte-copy loop"
    );
}

#[test]
fn test_research_mode_distributed_and_report() {
    let root = find_workspace_root();

    // 1. Distributed synthetic simulation benchmark
    let dist_res = run_distributed_benchmark(&root);
    assert!(dist_res.success, "Distributed benchmark should succeed");
    assert_eq!(dist_res.metrics["simulation_only"], true);
    assert!(dist_res.metrics.contains_key("throughput_tasks_per_sec"));
    assert!(dist_res.metrics.contains_key("latency_p50_us"));
    assert!(dist_res.metrics.contains_key("latency_p90_us"));
    assert!(dist_res.metrics.contains_key("latency_p99_us"));
    assert!(dist_res.metrics.contains_key("worker_allocations"));

    let allocs = dist_res.metrics["worker_allocations"]
        .as_object()
        .expect("worker_allocations object");
    for key in allocs.keys() {
        assert!(
            matches!(
                key.as_str(),
                "sim-cpu-node-a" | "sim-cpu-node-b" | "sim-gpu-node" | "sim-mobile-node"
            ),
            "unexpected synthetic worker profile name: {key}"
        );
    }

    let throughput = dist_res.metrics["throughput_tasks_per_sec"]
        .as_f64()
        .unwrap_or(0.0);
    assert!(
        throughput > 0.0,
        "Throughput should be positive, got: {}",
        throughput
    );

    // 2. Full research report aggregation
    let report_res = generate_research_report(&root);
    assert!(
        report_res.success,
        "Research report generation should succeed"
    );
    assert!(report_res.metrics.contains_key("hardware"));
    assert!(report_res.metrics.contains_key("micro_benchmarks"));
    assert!(report_res.metrics.contains_key("distributed_simulation"));

    let report_path = root.join("target/research_report.json");
    assert!(
        report_path.exists(),
        "target/research_report.json should exist"
    );
    let report_json = std::fs::read_to_string(&report_path).expect("read research_report.json");

    for text in [&dist_res.stdout, &report_res.stdout, &report_json] {
        let lower = text.to_ascii_lowercase();
        assert!(!text.contains("OPTIMAL"), "output must not print OPTIMAL");
        assert!(
            !lower.contains("rtx") && !lower.contains("4090") && !lower.contains("worker-gpu"),
            "output must not print invented RTX / worker-gpu-rtx4090 hardware or node names"
        );
        assert!(
            !lower.contains("snapdragon"),
            "output must not print invented Snapdragon hardware"
        );
        assert!(
            !text.contains("mac-orchestrator")
                && !text.contains("linux-worker-cpu")
                && !text.contains("android-mobile-edge"),
            "output must not print invented node names"
        );
    }

    // 3. Research report must fail when target directory cannot be created (e.g. target is a file)
    let temp = tempfile::tempdir().expect("tempdir");
    std::fs::write(temp.path().join("target"), b"not a directory").expect("write target file");
    let bad_report = generate_research_report(temp.path());
    assert!(
        !bad_report.success,
        "generate_research_report must fail when target is a regular file"
    );
    assert_eq!(bad_report.exit_code, Some(1));
}

#[test]
fn test_dev_mode_watch_and_aliases() {
    let root = find_workspace_root();

    // 1. Test watch check
    let watch_res = run_watch_check(&root);
    assert!(watch_res.success, "Watch check should succeed");
    assert!(watch_res.metrics.contains_key("watchable_files"));
    assert!(watch_res.metrics.contains_key("watchable_dirs"));

    // 2. Test execute_mode_action with aliases
    let reload_res = execute_mode_action(WorkflowMode::Dev, "reload", &root);
    assert!(reload_res.success);

    let update_res = execute_mode_action(WorkflowMode::Doc, "update", &root);
    assert!(update_res.success);
}

#[test]
fn test_unknown_and_non_runnable_actions_fail() {
    let root = find_workspace_root();
    let res = execute_mode_action(WorkflowMode::Test, "non_existent_action", &root);
    assert!(!res.success);
    assert!(res.stderr.contains("Unknown action"));

    let cluster_res = execute_mode_action(WorkflowMode::Dev, "cluster", &root);
    assert!(
        !cluster_res.success,
        "dev cluster must fail in non-CLI execute_mode_action path"
    );
    assert_eq!(cluster_res.exit_code, Some(1));

    let stop_res = execute_mode_action(WorkflowMode::Dev, "stop", &root);
    assert!(
        !stop_res.success,
        "dev stop must fail in non-CLI execute_mode_action path"
    );
    assert_eq!(stop_res.exit_code, Some(1));
}

#[test]
fn test_core_has_no_cargo_invocation() {
    let root = find_workspace_root();
    let core_src_dir = root.join("crates/core/src");
    for entry in std::fs::read_dir(&core_src_dir).expect("read crates/core/src") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let content = std::fs::read_to_string(&path).expect("read rs file");
            assert!(
                !content.contains("Command::new(\"cargo\")"),
                "{} must not contain Command::new(\"cargo\")",
                path.display()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_status_against_protected_master_exits_nonzero() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
        allowed_origins: Vec::new(),
    }
    .with_token("secret-admin-token", "admin", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);
    let master = MasterServer::spawn(config)
        .await
        .expect("start protected master");
    let dash_port = master.dashboard_addr().expect("dashboard addr").port();

    let temp = tempfile::tempdir().expect("tempdir");
    let temp_home = temp.path().join("home");
    std::fs::create_dir_all(&temp_home).expect("create temp_home");
    std::fs::write(
        temp.path().join(".dev_cluster.json"),
        serde_json::json!({ "web_ui_port": dash_port }).to_string(),
    )
    .expect("write .dev_cluster.json");

    // 1. Direct engine probe against protected master returns failure with HTTP 401
    let probe_res = probe_cluster_health_with_secret(Some(dash_port), temp.path(), None);
    assert!(
        !probe_res.success,
        "dev status against protected master (401) must fail"
    );
    assert_eq!(probe_res.exit_code, Some(1));
    assert_eq!(probe_res.metrics["http_status"], 401);
    assert!(
        probe_res.stderr.contains("401"),
        "expected HTTP 401 in stderr, got: {}",
        probe_res.stderr
    );

    // 2. CLI runner exits non-zero against protected master
    let exit_code = run_mode_cli_with_secret(
        ModeCliArgs {
            mode: Some("dev".to_string()),
            action: Some("status".to_string()),
            action_opt: None,
            iterations: 1,
            port: Some(dash_port),
            json: true,
            interactive: false,
        },
        temp.path(),
        None,
    )
    .await;
    assert_ne!(
        exit_code, 0,
        "run_mode_cli dev status against protected master must return non-zero exit code"
    );

    // 3. Binary invocation (`ox-mode dev status --port <dash_port>`) exits non-zero
    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));
    let out = std::process::Command::new(&ox_mode_bin)
        .current_dir(temp.path())
        .env("HOME", &temp_home)
        .env("USERPROFILE", &temp_home)
        .env_remove("CARGO_MANIFEST_DIR")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .args(["dev", "status", "--port", &dash_port.to_string(), "--json"])
        .output()
        .expect("spawn ox-mode dev status");
    assert!(
        !out.status.success(),
        "ox-mode dev status against protected master must exit non-zero"
    );

    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_status_closed_port_fails_while_another_master_is_up() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: true,
        loopback_bypass: true,
        tokens: Default::default(),
        allowed_origins: Vec::new(),
    };
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);
    let master = MasterServer::spawn(config).await.expect("start dev master");
    let dash_port = master.dashboard_addr().expect("dashboard addr").port();

    let temp = tempfile::tempdir().expect("tempdir");
    let temp_home = temp.path().join("home");
    std::fs::create_dir_all(&temp_home).expect("create temp_home");
    std::fs::write(
        temp.path().join(".dev_cluster.json"),
        serde_json::json!({ "web_ui_port": dash_port }).to_string(),
    )
    .expect("write .dev_cluster.json");

    // Without explicit --port, fallback to .dev_cluster.json succeeds against dash_port
    let default_res = probe_cluster_health_with_secret(None, temp.path(), None);
    assert!(
        default_res.success,
        "probe without explicit port should succeed via .dev_cluster.json: {}",
        default_res.stderr
    );
    assert_eq!(
        default_res.metrics["endpoint"],
        format!("http://127.0.0.1:{dash_port}/api/status")
    );

    // Pick a closed port
    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        listener.local_addr().expect("local_addr").port()
    };

    // With explicit --port <closed_port>, must probe ONLY closed_port and fail (no fallback to .dev_cluster.json)
    let explicit_res = probe_cluster_health_with_secret(Some(closed_port), temp.path(), None);
    assert!(
        !explicit_res.success,
        "explicit --port on closed port must fail even when another master is listed in .dev_cluster.json"
    );
    assert_eq!(explicit_res.exit_code, Some(1));

    let exit_code = run_mode_cli_with_secret(
        ModeCliArgs {
            mode: Some("dev".to_string()),
            action: Some("status".to_string()),
            action_opt: None,
            iterations: 1,
            port: Some(closed_port),
            json: true,
            interactive: false,
        },
        temp.path(),
        None,
    )
    .await;
    assert_ne!(
        exit_code, 0,
        "run_mode_cli with closed --port must return non-zero while another master is up"
    );

    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));
    let out = std::process::Command::new(&ox_mode_bin)
        .current_dir(temp.path())
        .env("HOME", &temp_home)
        .env("USERPROFILE", &temp_home)
        .env_remove("CARGO_MANIFEST_DIR")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .args([
            "dev",
            "status",
            "--port",
            &closed_port.to_string(),
            "--json",
        ])
        .output()
        .expect("spawn ox-mode dev status --port closed");
    assert!(
        !out.status.success(),
        "ox-mode dev status --port <closed> must exit non-zero while another master is up"
    );

    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_status_succeeds_against_cluster_with_secret() {
    let secret = ClusterSecret::generate().expect("generate secret");
    let temp = tempfile::tempdir().expect("tempdir");
    let temp_home = temp.path().join("home");
    std::fs::create_dir_all(&temp_home).expect("create temp_home");
    let secret_path = temp.path().join("cluster.secret");
    secret
        .save_file(&secret_path, true)
        .expect("save secret file");

    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
        allowed_origins: Vec::new(),
    };
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_cluster_secret(secret.clone())
        .with_auth_config(auth_config);
    let master = MasterServer::spawn(config)
        .await
        .expect("start secret-protected master");
    let dash_port = master.dashboard_addr().expect("dashboard addr").port();

    // 1. Without secret, status fails with HTTP 401
    let no_secret_res = probe_cluster_health_with_secret(Some(dash_port), temp.path(), None);
    assert!(
        !no_secret_res.success,
        "probe without secret against secret-protected master must fail"
    );
    assert_eq!(no_secret_res.metrics["http_status"], 401);

    // 2. With cluster secret, status derives dashboard_admin_token(&secret) and succeeds (200 OK)
    let with_secret_res =
        probe_cluster_health_with_secret(Some(dash_port), temp.path(), Some(&secret));
    assert!(
        with_secret_res.success,
        "probe with cluster secret must succeed against secret-protected master: {}",
        with_secret_res.stderr
    );
    assert_eq!(with_secret_res.metrics["http_status"], 200);

    // 3. Binary invocation with OXIDESWARM_SECRET_FILE succeeds and exits 0
    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));
    let out = std::process::Command::new(&ox_mode_bin)
        .current_dir(temp.path())
        .env("HOME", &temp_home)
        .env("USERPROFILE", &temp_home)
        .env_remove("CARGO_MANIFEST_DIR")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("RUSTY_GRID_CONFIG")
        .env("OXIDESWARM_SECRET_FILE", &secret_path)
        .args(["dev", "status", "--port", &dash_port.to_string(), "--json"])
        .output()
        .expect("spawn ox-mode dev status with secret");
    assert!(
        out.status.success(),
        "ox-mode dev status with OXIDESWARM_SECRET_FILE must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("parse ox-mode status json");
    assert_eq!(json["success"], true);

    let _ = master.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_status_never_sends_token_to_guessed_ports_or_3000() {
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    // 1. Default fallback port list must be only [8080] (port 3000 and grid port 8088 are never guessed).
    assert_eq!(DEFAULT_FALLBACK_STATUS_PORTS, &[8080]);
    assert!(!DEFAULT_FALLBACK_STATUS_PORTS.contains(&3000));
    assert!(!DEFAULT_FALLBACK_STATUS_PORTS.contains(&8088));

    let secret = ClusterSecret::generate().expect("generate secret");
    let admin_token = rusty_grid_master::dashboard_admin_token(&secret);
    let temp = tempfile::tempdir().expect("tempdir");
    let temp_home = temp.path().join("home");
    let oxide_dir = temp_home.join(".oxideswarm");
    std::fs::create_dir_all(&oxide_dir).expect("create .oxideswarm");
    let secret_path = oxide_dir.join("cluster.secret");
    secret
        .save_file(&secret_path, true)
        .expect("save secret file");

    // 2. Test the fallback port rule deterministically on an ephemeral port:
    //    A fake OxideSwarm-realm listener (`WWW-Authenticate: Bearer realm="OxideSwarm"`) on the
    //    unconfigured fallback port must receive the unauthenticated `GET /api/status` probe,
    //    and `dev status` must NEVER retry or send `Authorization: Bearer <token>` to it.
    let fallback_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind fallback listener");
    let fallback_port = fallback_listener.local_addr().expect("local_addr").port();
    let _ = fallback_listener.set_nonblocking(true);
    let captured_fallback = Arc::new(Mutex::new(Vec::<String>::new()));
    let cap_fb = Arc::clone(&captured_fallback);
    let fb_handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
        while std::time::Instant::now() < deadline {
            if let Ok((mut stream, _)) = fallback_listener.accept() {
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let _ = stream.write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer realm=\"OxideSwarm\"\r\nContent-Length: 0\r\n\r\n",
                );
                let mut list = cap_fb.lock().unwrap();
                list.push(req);
                if list.len() >= 2 {
                    // Wait briefly in case a buggy implementation attempts a retry with Authorization.
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
            } else {
                std::thread::sleep(std::time::Duration::from_millis(15));
            }
        }
    });

    let fb_res = probe_cluster_health_with_fallback_ports(
        None,
        temp.path(),
        Some(&secret),
        &[fallback_port],
    );
    assert!(!fb_res.success);
    assert_eq!(fb_res.metrics["http_status"], 401);

    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));
    let out_fb = std::process::Command::new(&ox_mode_bin)
        .current_dir(temp.path())
        .env("HOME", &temp_home)
        .env("USERPROFILE", &temp_home)
        .env("OXIDESWARM_SECRET_FILE", &secret_path)
        .env_remove("CARGO_MANIFEST_DIR")
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_DASHBOARD_PORT")
        .args(["dev", "status", "--json"])
        .output()
        .expect("spawn ox-mode dev status without --port");
    assert!(!out_fb.status.success());

    let _ = fb_handle.join();
    let reqs_fb = captured_fallback.lock().unwrap();
    assert_eq!(
        reqs_fb.len(),
        1,
        "fallback listener must receive exactly 1 unauthenticated probe (no auth retry), got {} requests",
        reqs_fb.len()
    );
    assert!(
        reqs_fb
            .iter()
            .all(|r| !r.contains(&admin_token) && !r.contains("Authorization:")),
        "fallback port must never receive Authorization Bearer token even when replying with OxideSwarm 401 challenge"
    );

    // 3. Even on an explicit `--port` or `.dev_cluster.json` port, a fake server that responds
    //    200 OK or 401 without `WWW-Authenticate: Bearer realm="OxideSwarm"` must never receive the token.
    let fake_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake listener on ephemeral port");
    let fake_port = fake_listener.local_addr().expect("local_addr").port();
    let captured_fake = Arc::new(Mutex::new(Vec::<String>::new()));
    let cap_fake_clone = Arc::clone(&captured_fake);
    let fake_handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = fake_listener.accept() {
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).into_owned();
            cap_fake_clone.lock().unwrap().push(req);
            let _ = stream.write_all(
                b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer realm=\"OtherService\"\r\nContent-Length: 0\r\n\r\n",
            );
        }
    });

    let fake_res = probe_cluster_health_with_secret(Some(fake_port), temp.path(), Some(&secret));
    assert!(!fake_res.success);
    let _ = fake_handle.join();
    let fake_reqs = captured_fake.lock().unwrap();
    assert_eq!(fake_reqs.len(), 1);
    assert!(
        !fake_reqs[0].contains(&admin_token) && !fake_reqs[0].contains("Authorization:"),
        "initial probe and non-OxideSwarm 401 must never receive Authorization Bearer token"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_cluster_never_sends_token_when_web_ui_port_is_held() {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let temp = tempfile::tempdir().expect("tempdir");
    let secret = ClusterSecret::generate().expect("generate secret");
    let admin_token = rusty_grid_master::dashboard_admin_token(&secret);
    let isolated_secret_dir = temp.path().join(".dev_cluster_home").join(".oxideswarm");
    std::fs::create_dir_all(&isolated_secret_dir).expect("create isolated .oxideswarm");
    secret
        .save_file(&isolated_secret_dir.join("cluster.secret"), true)
        .expect("save secret in isolated home");

    let free_master_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral master port");
        l.local_addr().expect("local_addr").port()
    };

    let web_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind held web_ui_port listener");
    let held_web_port = web_listener.local_addr().expect("local_addr").port();
    let _ = web_listener.set_nonblocking(true);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop_flag);
    let captured = Arc::new(Mutex::new(Vec::<String>::new()));
    let cap_clone = Arc::clone(&captured);

    let listener_handle = std::thread::spawn(move || {
        while !stop_clone.load(Ordering::Relaxed) {
            if let Ok((mut stream, _)) = web_listener.accept() {
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                cap_clone.lock().unwrap().push(req);
                let _ = stream.write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer realm=\"OxideSwarm\"\r\nContent-Length: 0\r\n\r\n",
                );
            } else {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    });

    let res =
        launch_local_dev_cluster_with_ports(temp.path(), free_master_port, held_web_port).await;
    stop_flag.store(true, Ordering::Relaxed);
    let _ = listener_handle.join();
    let _ = stop_local_dev_cluster(temp.path()).await;

    assert!(
        !res.success,
        "launch_local_dev_cluster_with_ports must fail when web_ui_port is held by another listener"
    );
    assert_eq!(res.exit_code, Some(1));
    assert!(
        res.stderr.contains("did not appear within 8s"),
        "expected failure message to state that the dashboard port file did not appear within 8s, got: {}",
        res.stderr
    );

    let reqs = captured.lock().unwrap();
    assert!(
        reqs.iter()
            .all(|r| !r.contains("Authorization:") && !r.contains(&admin_token)),
        "fake listener holding web_ui_port must never receive Authorization Bearer token, got: {:?}",
        *reqs
    );
    assert!(
        reqs.is_empty(),
        "dev cluster must not poll web_ui_port before --dashboard-port-file confirms the master bound it, got {} requests",
        reqs.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_stop_skips_unrelated_and_stale_pids_and_reports_honestly() {
    let temp = tempfile::tempdir().expect("tempdir");

    // Spawn an unrelated background process using stock OS utilities (`sleep` on Unix, `ping` on Windows)
    // so stock macOS machines without `python` on PATH can run this test.
    #[cfg(unix)]
    let mut unrelated = std::process::Command::new("sleep")
        .arg("60")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn unrelated sleep process");
    #[cfg(windows)]
    let mut unrelated = std::process::Command::new("ping")
        .args(["-n", "60", "127.0.0.1"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn unrelated ping process");
    let unrelated_pid = unrelated.id();
    let self_pid = std::process::id();

    std::fs::write(
        temp.path().join(".dev_cluster.pids"),
        format!("master:{unrelated_pid}\nworker_cpu:0\nworker_gpu:1\nworker_self:{self_pid}\n"),
    )
    .expect("write .dev_cluster.pids");
    std::fs::write(
        temp.path().join(".dev_cluster.json"),
        serde_json::json!({
            "master_pid": 4_294_967_297u64,
            "pids": [format!("master:{unrelated_pid}")]
        })
        .to_string(),
    )
    .expect("write .dev_cluster.json");

    let stop_res = stop_local_dev_cluster(temp.path()).await;

    // Unrelated process must still be alive (`try_wait() == Ok(None)`)
    let child_status = unrelated.try_wait().expect("try_wait on unrelated process");
    let _ = unrelated.kill();
    let _ = unrelated.wait();

    assert!(
        child_status.is_none(),
        "unrelated process (PID {unrelated_pid}) must survive `dev stop`, but exited with {child_status:?}"
    );
    assert!(stop_res.success);
    assert_eq!(stop_res.metrics["signalled_count"], 0);
    assert_eq!(stop_res.metrics["exited_count"], 0);
    assert_eq!(stop_res.metrics["signal_failed_count"], 0);
    assert_eq!(
        stop_res.metrics["skipped_count"], 5,
        "expected 5 invalid/stale/unrelated entries skipped (unrelated, 0, 1, self_pid, u64 overflow), got: {:?}",
        stop_res.metrics
    );
    assert!(
        stop_res.stdout.contains("0 signalled, 0 exited")
            && stop_res
                .stdout
                .contains("stale/unrelated PID entry(ies) skipped"),
        "expected honest stop summary reporting skipped stale entries, got: {}",
        stop_res.stdout
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_cluster_in_process_launch_then_stop_and_signal_failure_handling() {
    let master_tcp_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral master tcp port");
        l.local_addr().expect("local_addr").port()
    };
    let web_ui_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral web ui port");
        l.local_addr().expect("local_addr").port()
    };

    let temp = tempfile::tempdir().expect("tempdir");
    let launch_res =
        launch_local_dev_cluster_with_ports(temp.path(), master_tcp_port, web_ui_port).await;
    assert!(
        launch_res.success,
        "in-process dev cluster launch must succeed, got stderr: {}",
        launch_res.stderr
    );

    let pid_file = temp.path().join(".dev_cluster.pids");
    let json_file = temp.path().join(".dev_cluster.json");
    assert!(
        pid_file.exists(),
        ".dev_cluster.pids must exist after launch"
    );
    assert!(
        json_file.exists(),
        ".dev_cluster.json must exist after launch"
    );

    let json_str = std::fs::read_to_string(&json_file).expect("read .dev_cluster.json");
    let json_val: serde_json::Value =
        serde_json::from_str(&json_str).expect("parse .dev_cluster.json");
    let start_times = json_val
        .get("pid_start_times")
        .and_then(|v| v.as_object())
        .expect("pid_start_times map in .dev_cluster.json");
    assert_eq!(
        start_times.len(),
        3,
        "all 3 spawned cluster children must have start_time recorded in .dev_cluster.json"
    );

    // 1. If a verified process cannot be signalled, `stop` must count it as failed, return
    //    `success == false` (`exit_code == Some(1)`), and preserve the tracking files.
    let fail_stop = stop_local_dev_cluster_with_signaller(temp.path(), |_, _| false).await;
    assert!(
        !fail_stop.success,
        "stop must fail when verified cluster processes cannot be signalled"
    );
    assert_eq!(fail_stop.exit_code, Some(1));
    assert_eq!(fail_stop.metrics["signal_failed_count"], 3);
    assert_eq!(fail_stop.metrics["signalled_count"], 0);
    assert!(
        pid_file.exists() && json_file.exists(),
        "tracking files must be preserved when stop fails to signal verified processes"
    );

    // 2. Now run real `stop_local_dev_cluster` in the SAME process that launched the cluster:
    //    must reap/detect exit for all 3 children, report 3 signalled, 3 exited, and remove tracking files.
    let stop_res = stop_local_dev_cluster(temp.path()).await;
    assert!(
        stop_res.success,
        "in-process stop_local_dev_cluster must succeed, got stdout={} stderr={}",
        stop_res.stdout, stop_res.stderr
    );
    assert_eq!(stop_res.exit_code, Some(0));
    assert_eq!(stop_res.metrics["signalled_count"], 3);
    assert_eq!(stop_res.metrics["exited_count"], 3);
    assert_eq!(stop_res.metrics["signal_failed_count"], 0);
    assert!(
        stop_res.stdout.contains("3 signalled, 3 exited"),
        "expected '3 signalled, 3 exited' in stdout, got: {}",
        stop_res.stdout
    );
    assert!(
        !pid_file.exists() && !json_file.exists(),
        "tracking files must be deleted after clean stop"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_cluster_fails_when_port_is_held() {
    let held_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind held tcp port for master");
    let held_port = held_listener.local_addr().expect("local_addr").port();
    let free_web_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral web port");
        l.local_addr().expect("local_addr").port()
    };

    let temp = tempfile::tempdir().expect("tempdir");
    let res = launch_local_dev_cluster_with_ports(temp.path(), held_port, free_web_port).await;
    let json_exists_after_launch = temp.path().join(".dev_cluster.json").exists();
    let pids_exist_after_launch = temp.path().join(".dev_cluster.pids").exists();
    // Always run cleanup in case a regression left child processes behind.
    let _ = stop_local_dev_cluster(temp.path()).await;
    drop(held_listener);

    assert!(
        !res.success,
        "launch_local_dev_cluster_with_ports must fail when the master TCP port is already held"
    );
    assert_eq!(res.exit_code, Some(1));
    assert!(
        !json_exists_after_launch && !pids_exist_after_launch,
        "failed dev cluster launch must clean up .dev_cluster.json and .dev_cluster.pids"
    );
}

#[test]
fn test_cli_binary_mode_subcommand() {
    let root = find_workspace_root();
    let ox_mode_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_ox-mode"));
    let temp = tempfile::tempdir().expect("tempdir");

    let output = std::process::Command::new(&ox_mode_bin)
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .args(["doc", "verify", "--json"])
        .current_dir(&root)
        .output()
        .expect("Failed to execute ox-mode binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    assert_eq!(json["mode"], "doc");
    assert_eq!(json["action"], "verify");
    assert_eq!(json["success"], true);
}

fn resolve_functional_bash() -> Option<std::path::PathBuf> {
    #[cfg(windows)]
    let candidates = {
        let mut candidates = vec![
            std::path::PathBuf::from(r"C:\Program Files\Git\bin\bash.exe"),
            std::path::PathBuf::from(r"C:\Program Files\Git\usr\bin\bash.exe"),
            std::path::PathBuf::from(r"C:\Program Files (x86)\Git\bin\bash.exe"),
        ];
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            candidates
                .push(std::path::PathBuf::from(local_app_data).join(r"Programs\Git\bin\bash.exe"));
        }
        candidates.push(std::path::PathBuf::from("bash"));
        candidates
    };
    #[cfg(not(windows))]
    let candidates = vec![std::path::PathBuf::from("bash")];

    for cand in candidates {
        if let Ok(output) = std::process::Command::new(&cand)
            .args(["-c", "exit 0"])
            .output()
        {
            if output.status.success() {
                return Some(cand);
            }
        }
    }

    None
}

#[test]
fn test_bash_script_invocation() {
    let bash_bin = match resolve_functional_bash() {
        Some(bin) => bin,
        None => {
            eprintln!("Skipping test_bash_script_invocation: No functional bash interpreter found on host");
            return;
        }
    };

    let root = find_workspace_root();
    let script = root.join("scripts/mode.sh");
    if !script.exists() {
        return;
    }
    let temp = tempfile::tempdir().expect("tempdir");

    let output = std::process::Command::new(&bash_bin)
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_CONFIG")
        .arg(&script)
        .args(["doc", "verify", "--json"])
        .current_dir(&root)
        .output()
        .expect("Failed to execute scripts/mode.sh");

    assert!(
        output.status.success(),
        "scripts/mode.sh doc verify should succeed: {:?}",
        output
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("Script output should be valid JSON");
    assert_eq!(json["mode"], "doc");
    assert_eq!(json["action"], "verify");
    assert_eq!(json["success"], true);
}
