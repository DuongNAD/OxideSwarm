use super::mapreduce::{read_mapreduce_script, split_command_line, MAX_MAPREDUCE_SCRIPT_BYTES};
use super::master::{apply_max_active_mb, apply_web_ui_addr, dashboard_banner, run_master};
use super::submit::{
    apply_default_spec_placement, build_placement, build_task_spec, read_task_input,
    run_submit_with_secret, MAX_TASK_INPUT_BYTES,
};
use super::tasks::{
    parse_worker_link, parse_worker_speed, parse_worker_speed_str, worker_is_connected,
    worker_state,
};
use super::worker::{resolve_isolation, run_worker, ISOLATION_ENV};
use super::*;
use crate::config::SecretText;
use crate::connect::connect_master;
use crate::secret_cmd::SecretCliArgs;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::placement::{IsolationLevel, Placement};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::WorkerCapabilities;
use rusty_grid_master::ServerConfig;
use rusty_grid_worker::ProcessPriority;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(super) fn parse(args: &[&str]) -> Cli {
    let mut argv = vec!["oxideswarm"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv).expect("arguments must parse")
}

/// The bound on how long a test waits for an in-process master to publish its port and accept a
/// client (TEST-49, TEST-51). It is a limit, not a window: each wait ends as soon as its condition
/// holds, and a loaded runner can take far longer than a second to build the master's state.
pub(super) const MASTER_START_BOUND: Duration = Duration::from_secs(30);

/// Returns `127.0.0.1:<port>` once `run_master`, started with `--listen 127.0.0.1:0 --port-file
/// <port_file>`, has written the port it bound (TEST-51). The master picks its own port, so no
/// other test in this binary can take it first, as one could when the test bound a probe
/// listener, dropped it and started the master on its port. Panics with `what` and what it
/// waited for when `master` ends first or the bound passes.
pub(super) async fn wait_for_master_port(
    port_file: &Path,
    master: &tokio::task::JoinHandle<ExitCode>,
    what: &str,
) -> String {
    let deadline = std::time::Instant::now() + MASTER_START_BOUND;
    loop {
        let port = std::fs::read_to_string(port_file)
            .ok()
            .and_then(|text| text.trim().parse::<u16>().ok())
            .filter(|port| *port != 0);
        if let Some(port) = port {
            return format!("127.0.0.1:{port}");
        }
        assert!(
            !master.is_finished(),
            "{what}: run_master ended before it wrote its port to {}",
            port_file.display()
        );
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: run_master did not write its port to {} within {} s",
            port_file.display(),
            MASTER_START_BOUND.as_secs()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Connects to the in-process master at `addr`, trying again until `connect_master` succeeds
/// (TEST-51). Panics with `what` and the last error when `MASTER_START_BOUND` passes first.
pub(super) async fn connect_master_within_bound(
    addr: &str,
    secret: &ClusterSecret,
    what: &str,
) -> MessageTransport<GridStream> {
    let deadline = std::time::Instant::now() + MASTER_START_BOUND;
    loop {
        match connect_master(addr, Some(secret)).await {
            Ok(transport) => return transport,
            Err(e) if std::time::Instant::now() >= deadline => panic!(
                "{what}: connect_master to {addr} did not succeed within {} s, last error: {e}",
                MASTER_START_BOUND.as_secs()
            ),
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

pub(super) fn submit_args(args: &[&str]) -> SubmitArgs {
    match parse(&[&["submit"], args].concat()).command {
        Commands::Submit(a) => a,
        other => panic!("expected submit, got {other:?}"),
    }
}

pub(super) fn command_of(spec: Result<TaskSpec, String>) -> (String, Vec<String>) {
    match spec.expect("the spec must build") {
        TaskSpec::Command { program, args, .. } => (program, args),
        other => panic!("expected Command, got {other:?}"),
    }
}

#[test]
fn test_submit_trailing_command_without_args_runs_with_no_args() {
    // Regression: `submit -- nproc` used to run `nproc "Hello from RustyGrid"`.
    for single in ["nproc", "uname", "hostname", "nvidia-smi"] {
        let (program, args) = command_of(build_task_spec(&submit_args(&["--", single])));
        assert_eq!(program, single);
        assert!(
            args.is_empty(),
            "{single} must get no arguments, got {args:?}"
        );
    }
}

#[test]
fn test_submit_trailing_command_keeps_its_arguments() {
    let (program, args) = command_of(build_task_spec(&submit_args(&[
        "--", "echo", "hi", "there",
    ])));
    assert_eq!(program, "echo");
    assert_eq!(args, ["hi", "there"]);
}

#[test]
fn test_submit_command_flag_takes_trailing_args_verbatim() {
    let (program, args) = command_of(build_task_spec(&submit_args(&[
        "--command",
        "sha256sum",
        "--",
        "a.bin",
        "b.bin",
    ])));
    assert_eq!(program, "sha256sum");
    assert_eq!(args, ["a.bin", "b.bin"]);

    let (program, args) = command_of(build_task_spec(&submit_args(&["--command", "whoami"])));
    assert_eq!(program, "whoami");
    assert!(args.is_empty());
}

#[test]
fn test_submit_demo_greeting_only_for_a_completely_empty_command_line() {
    let (program, args) = command_of(build_task_spec(&submit_args(&[])));
    assert_eq!(program, "echo");
    assert_eq!(args, ["Hello from RustyGrid"]);
}

#[test]
fn test_cancel_and_result_cli_flags_parse() {
    let id = Uuid::new_v4();
    let id_str = id.to_string();
    match parse(&["cancel", &id_str, "--json", "--master", "127.0.0.1:8088"]).command {
        Commands::Cancel(c) => {
            assert_eq!(c.task_id, id);
            assert!(c.json);
            assert_eq!(c.master.as_deref(), Some("127.0.0.1:8088"));
        }
        other => panic!("expected Cancel, got {other:?}"),
    }
    match parse(&["result", &id_str, "--wait", "--json", "--output-dir", "out"]).command {
        Commands::Result(r) => {
            assert_eq!(r.task_id, id);
            assert!(r.wait);
            assert!(r.json);
            assert_eq!(r.output_dir.as_deref(), Some(Path::new("out")));
        }
        other => panic!("expected Result, got {other:?}"),
    }
}

#[test]
fn test_submit_shell_type_forwards_trailing_args() {
    let script_of = |spec: Result<TaskSpec, String>| match spec.expect("the spec must build") {
        TaskSpec::ShellScript { script, .. } => script,
        other => panic!("expected ShellScript, got {other:?}"),
    };
    assert_eq!(
        script_of(build_task_spec(&submit_args(&[
            "--type",
            "shell",
            "--command",
            "ls",
            "--",
            "-la",
            "/tmp"
        ]))),
        "ls -la /tmp"
    );
    assert_eq!(
        script_of(build_task_spec(&submit_args(&[
            "--type", "shell", "--", "echo", "hi"
        ]))),
        "echo hi"
    );
    assert_eq!(
        script_of(build_task_spec(&submit_args(&["--type", "shell"]))),
        "echo 'Hello from shell task'"
    );
}

#[test]
fn test_submit_placement_flags_become_require_tags() {
    let placement = build_placement(&submit_args(&[
        "--os",
        "Mac",
        "--arch",
        "arm64",
        "--worker",
        "MAC-M5",
        "--require-tag",
        "laptop",
        "--require-tag",
        "fast",
        "--",
        "true",
    ]))
    .unwrap();
    assert_eq!(placement.os.as_deref(), Some("macos"));
    assert_eq!(placement.arch.as_deref(), Some("aarch64"));
    assert_eq!(placement.worker.as_deref(), Some("MAC-M5"));
    assert_eq!(placement.tags, ["fast", "laptop"]);
    let task = Task::new(
        TaskSpec::builtin_test("hash_compute", 1),
        TaskRequirements::generic(1, 5),
    )
    .with_placement(&placement);
    assert_eq!(
        task.placement().unwrap(),
        placement,
        "the master reads back what the CLI meant"
    );

    assert!(build_placement(&submit_args(&["--", "true"]))
        .unwrap()
        .is_empty());
    let err =
        build_placement(&submit_args(&["--require-tag", "os:macos", "--", "true"])).unwrap_err();
    assert!(err.contains("only the worker or the master"), "{err}");
}

#[test]
fn test_submit_require_isolation_becomes_a_placement_constraint() {
    for (flag, level) in [
        ("fs", IsolationLevel::Fs),
        ("fs+net", IsolationLevel::FsNet),
    ] {
        let placement =
            build_placement(&submit_args(&["--require-isolation", flag, "--", "true"])).unwrap();
        assert_eq!(placement.isolation, Some(level));
        assert_eq!(placement.to_task_tags(), [format!("require:iso={flag}")]);
    }
    for bad in ["off", "strict", ""] {
        let err =
            build_placement(&submit_args(&["--require-isolation", bad, "--", "true"])).unwrap_err();
        assert!(err.contains("--require-isolation"), "{bad}: {err}");
    }
}

#[test]
fn test_isolation_precedence_is_cli_then_env_then_config_then_off() {
    let key = "RUSTY_GRID_TEST_ISOLATION_PRECEDENCE";
    std::env::remove_var(key);
    let resolve = |cli: Option<&str>, file: Option<&str>| {
        resolve_isolation(cli.map(str::to_string), &[key], file.map(str::to_string))
    };
    assert_eq!(resolve(None, None), Ok(IsolationLevel::Off));
    assert_eq!(resolve(None, Some("fs")), Ok(IsolationLevel::Fs));
    std::env::set_var(key, "fs+net");
    assert_eq!(resolve(None, Some("fs")), Ok(IsolationLevel::FsNet));
    assert_eq!(resolve(Some("off"), Some("fs")), Ok(IsolationLevel::Off));
    std::env::set_var(key, "bogus");
    let err = resolve(None, Some("fs")).unwrap_err();
    std::env::remove_var(key);
    assert!(err.contains("unknown isolation level 'bogus'"), "{err}");
    assert_eq!(ISOLATION_ENV, "RUSTY_GRID_ISOLATION");
}

#[test]
fn test_worker_isolation_flag_is_kept_as_text_for_run_worker_to_validate() {
    let isolation_of = |args: &[&str]| match parse(args).command {
        Commands::Worker(a) => a.isolation,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(isolation_of(&["worker"]), None);
    assert_eq!(
        isolation_of(&["worker", "--isolation", "fs+net"]).as_deref(),
        Some("fs+net")
    );
}

#[test]
fn test_submit_stdin_file_and_env_reach_the_command_spec() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.txt");
    std::fs::write(&input, b"line 1\nline 2\n").unwrap();
    let spec = build_task_spec(&submit_args(&[
        "--stdin-file",
        input.to_str().unwrap(),
        "--env",
        "MODE=fast",
        "--env",
        "EMPTY=",
        "--env",
        "EQ=a=b",
        "--command",
        "wc",
        "--",
        "-l",
    ]))
    .unwrap();
    match spec {
        TaskSpec::Command {
            program,
            args,
            env,
            stdin,
            ..
        } => {
            assert_eq!(program, "wc");
            assert_eq!(args, ["-l"]);
            assert_eq!(stdin.as_deref(), Some(&b"line 1\nline 2\n"[..]));
            assert_eq!(env.get("MODE").map(String::as_str), Some("fast"));
            assert_eq!(env.get("EMPTY").map(String::as_str), Some(""));
            assert_eq!(env.get("EQ").map(String::as_str), Some("a=b"));
        }
        other => panic!("expected Command, got {other:?}"),
    }

    let err = build_task_spec(&submit_args(&["--env", "=x", "--", "true"])).unwrap_err();
    assert!(err.contains("expected KEY=VALUE"), "{err}");
    let missing = dir.path().join("missing.txt");
    let err = build_task_spec(&submit_args(&[
        "--stdin-file",
        missing.to_str().unwrap(),
        "--",
        "cat",
    ]))
    .unwrap_err();
    assert!(err.starts_with("--stdin-file"), "{err}");
}

#[test]
fn test_submit_script_file_and_interpreter_reach_the_shell_spec() {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("job.py");
    std::fs::write(&script, "print('hi')\n").unwrap();
    let path = script.to_str().unwrap();
    match build_task_spec(&submit_args(&[
        "--type",
        "shell",
        "--script-file",
        path,
        "--interpreter",
        "python3",
    ]))
    .unwrap()
    {
        TaskSpec::ShellScript {
            script,
            interpreter,
            ..
        } => {
            assert_eq!(script, "print('hi')\n");
            assert_eq!(interpreter.as_deref(), Some("python3"));
        }
        other => panic!("expected ShellScript, got {other:?}"),
    }

    let err = build_task_spec(&submit_args(&[
        "--type",
        "shell",
        "--script-file",
        path,
        "--",
        "extra",
    ]))
    .unwrap_err();
    assert!(err.contains("cannot be combined"), "{err}");
    std::fs::write(&script, [0xff, 0xfe, 0x00]).unwrap();
    let err =
        build_task_spec(&submit_args(&["--type", "shell", "--script-file", path])).unwrap_err();
    assert!(err.contains("not UTF-8"), "{err}");
}

#[test]
fn test_submit_gpu_size_flags() {
    let size_of = |args: &[&str]| match build_task_spec(&submit_args(args)).unwrap() {
        TaskSpec::GpuCompute {
            simulated_matrix_dim,
            work_group_size,
            ..
        } => (simulated_matrix_dim, work_group_size),
        other => panic!("expected GpuCompute, got {other:?}"),
    };
    assert_eq!(size_of(&["--type", "gpu"]), (64, 16), "defaults unchanged");
    assert_eq!(
        size_of(&[
            "--type",
            "gpu",
            "--matrix-dim",
            "1024",
            "--work-group-size",
            "8"
        ]),
        (1024, 8)
    );
    assert!(build_task_spec(&submit_args(&["--type", "gpu", "--matrix-dim", "0"])).is_err());
}

#[test]
fn test_submit_refuses_flags_of_another_task_type_and_compile() {
    for (args, needle) in [
        (
            &["--type", "shell", "--stdin-file", "x", "--", "cat"][..],
            "--stdin-file applies to command",
        ),
        (
            &["--script-file", "x", "--", "true"],
            "--script-file applies to shell",
        ),
        (
            &["--interpreter", "python3", "--", "true"],
            "--interpreter applies to shell",
        ),
        (
            &["--type", "gpu", "--env", "A=1"],
            "--env applies to command and shell",
        ),
        (
            &["--matrix-dim", "128", "--", "true"],
            "--matrix-dim applies to gpu",
        ),
        (
            &["--type", "shell", "--work-group-size", "8", "--", "ls"],
            "--work-group-size applies to gpu",
        ),
        (
            &["--type", "compile", "--command", "demo"],
            "compile tasks require --source",
        ),
        (
            &["--source", "main.rs", "--", "true"],
            "--source applies to compile",
        ),
    ] {
        let err = build_task_spec(&submit_args(args)).unwrap_err();
        assert!(err.contains(needle), "{args:?}: {err}");
    }
}

#[test]
fn test_submit_input_files_above_the_limit_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.bin");
    std::fs::File::create(&big)
        .unwrap()
        .set_len(MAX_TASK_INPUT_BYTES + 1)
        .unwrap();
    let err = read_task_input(&big, "--stdin-file").unwrap_err();
    assert!(err.contains("32 MiB"), "{err}");
}

#[test]
fn test_default_log_filter_keeps_wgpu_quiet_and_parses() {
    for level in ["debug", "info", "error"] {
        let filter = default_log_filter(level);
        assert!(filter.starts_with(level), "{filter}");
        assert!(filter.contains("wgpu_core=warn"), "{filter}");
        tracing_subscriber::EnvFilter::try_new(&filter).expect("a valid filter");
    }
}

#[test]
fn test_doctor_flags_parse_with_documented_defaults() {
    match parse(&["doctor"]).command {
        Commands::Doctor(a) => {
            assert_eq!(a.expect_workers, 1);
            assert_eq!(a.probe_timeout_secs, 60);
            assert_eq!(a.gpu_dim, 512);
            assert_eq!(a.spread_tasks, None);
            assert!(!a.skip_gpu && !a.skip_spread && !a.skip_throughput && !a.json);
        }
        other => panic!("expected doctor, got {other:?}"),
    }
    match parse(&[
        "doctor",
        "--master",
        "10.0.0.7",
        "--expect-workers",
        "2",
        "--spread-tasks",
        "16",
        "--skip-gpu",
        "--skip-throughput",
        "--json",
    ])
    .command
    {
        Commands::Doctor(a) => {
            assert_eq!(a.master.as_deref(), Some("10.0.0.7"));
            assert_eq!(a.expect_workers, 2);
            assert_eq!(a.spread_tasks, Some(16));
            assert!(a.skip_gpu && a.skip_throughput && a.json);
        }
        other => panic!("expected doctor, got {other:?}"),
    }
}

#[test]
fn test_worker_tag_flag_is_repeatable() {
    match parse(&["worker", "--tag", "laptop", "--tag", "fast"]).command {
        Commands::Worker(a) => {
            assert_eq!(a.tags, ["laptop", "fast"]);
            assert!(!a.no_portal, "the portal stays on unless asked");
        }
        other => panic!("expected worker, got {other:?}"),
    }
    match parse(&["worker", "--no-portal"]).command {
        Commands::Worker(a) => assert!(a.no_portal),
        other => panic!("expected worker, got {other:?}"),
    }
}

#[test]
fn test_secret_flags_are_global_and_work_on_every_command() {
    for args in [
        &["master", "--secret-file", "/s"][..],
        &["worker", "--secret-file", "/s"],
        &["submit", "--secret-file", "/s", "--", "true"],
        &["status", "--secret-file", "/s"],
        &["workers", "--secret-file", "/s"],
        &["mapreduce", "--input", "x", "--secret-file", "/s"],
        &["doctor", "--secret-file", "/s"],
        // Also accepted before the subcommand.
        &["--secret-file", "/s", "status"],
    ] {
        let cli = parse(args);
        assert_eq!(cli.secret_file, Some(PathBuf::from("/s")), "args {args:?}");
    }

    let cli = parse(&["worker", "--secret", "abcd"]);
    assert_eq!(cli.secret.as_ref().map(SecretText::as_str), Some("abcd"));
    assert!(
        !format!("{cli:?}").contains("abcd"),
        "the secret must not appear in Debug output"
    );
}

#[test]
fn test_secret_subcommands_parse() {
    match parse(&["secret", "generate", "--out", "/tmp/s", "--force"]).command {
        Commands::Secret(SecretCliArgs {
            action: crate::secret_cmd::SecretAction::Generate(g),
        }) => {
            assert_eq!(g.out, Some(PathBuf::from("/tmp/s")));
            assert!(g.force);
        }
        other => panic!("unexpected {other:?}"),
    }
    match parse(&["secret", "show", "--dashboard-token"]).command {
        Commands::Secret(SecretCliArgs {
            action: crate::secret_cmd::SecretAction::Show(s),
        }) => assert!(s.dashboard_token),
        other => panic!("unexpected {other:?}"),
    }
    assert!(Cli::try_parse_from(["oxideswarm", "secret"]).is_err());
}

#[test]
fn test_master_insecure_flag_parses() {
    match parse(&["master", "--listen", "0.0.0.0:8088", "--insecure"]).command {
        Commands::Master(m) => assert!(m.insecure),
        other => panic!("unexpected {other:?}"),
    }
    match parse(&["master"]).command {
        Commands::Master(m) => assert!(!m.insecure),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn test_worker_task_priority_flag_parses_into_a_known_priority() {
    let priority_of = |args: &[&str]| match parse(args).command {
        Commands::Worker(a) => a.task_priority,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(
        priority_of(&["worker"]),
        None,
        "unset: the library default applies"
    );
    let given = priority_of(&["worker", "--task-priority", "idle"]).unwrap();
    assert_eq!(given.parse::<ProcessPriority>(), Ok(ProcessPriority::Idle));
    let misspelt = priority_of(&["worker", "--task-priority", "realtime"]).unwrap();
    assert!(
        misspelt.parse::<ProcessPriority>().is_err(),
        "run_worker must refuse this instead of running tasks at full priority"
    );
}

#[test]
fn test_peer_insecure_flag_parses_on_worker_and_every_client_command() {
    let insecure_of = |args: &[&str]| match parse(args).command {
        Commands::Worker(a) => a.insecure,
        Commands::Submit(a) => a.insecure,
        Commands::Status(a) => a.insecure,
        Commands::Workers(a) => a.insecure,
        Commands::Mapreduce(a) => a.insecure,
        Commands::Doctor(a) => a.insecure,
        Commands::Mcp(a) => a.insecure,
        other => panic!("unexpected {other:?}"),
    };
    for with_flag in [
        &["worker", "--master", "auto", "--insecure"][..],
        &["submit", "--insecure", "--", "true"],
        &["status", "--insecure"],
        &["workers", "--insecure"],
        &["mapreduce", "--input", "x", "--insecure"],
        &["doctor", "--insecure"],
        &["mcp", "--insecure"],
    ] {
        assert!(insecure_of(with_flag), "{with_flag:?}");
    }
    // Off unless asked for.
    for without_flag in [
        &["worker"][..],
        &["submit", "--", "true"],
        &["status"],
        &["workers"],
        &["mapreduce", "--input", "x"],
        &["doctor"],
        &["mcp"],
    ] {
        assert!(!insecure_of(without_flag), "{without_flag:?}");
    }
}

#[test]
fn test_peer_insecure_resolution_is_flag_then_config_and_defaults_to_off() {
    // RUSTY_GRID_INSECURE is deliberately not touched here (process-global); the end-to-end
    // test covers the environment tier.
    if std::env::var_os("RUSTY_GRID_INSECURE").is_some() {
        return;
    }
    assert!(!peer_insecure(false, None));
    assert!(!peer_insecure(false, Some(false)));
    assert!(peer_insecure(false, Some(true)));
    assert!(peer_insecure(true, None));
    assert!(peer_insecure(true, Some(false)));
}

#[test]
fn test_insecure_config_keys_exist_for_worker_submit_and_status() {
    let parsed: ConfigFile = toml::from_str(
        r#"
[worker]
insecure = true
[submit]
insecure = true
[status]
insecure = false
"#,
    )
    .unwrap();
    assert_eq!(parsed.worker.unwrap().insecure, Some(true));
    assert_eq!(parsed.submit.unwrap().insecure, Some(true));
    assert_eq!(parsed.status.unwrap().insecure, Some(false));
}

#[test]
fn test_parse_worker_link_direct_p2p() {
    let caps = WorkerCapabilities::new("worker-1", 4, 4096, false, false, None)
        .with_tags(vec!["link:Direct P2P (QUIC):18ms".to_string()]);
    let link = parse_worker_link(&caps);
    assert_eq!(link.as_deref(), Some("Link: Direct P2P (QUIC) | RTT: 18ms"));
}

#[test]
fn test_parse_worker_link_relay_derp() {
    let caps = WorkerCapabilities::new("worker-2", 4, 4096, false, false, None)
        .with_tags(vec!["link:Relay (DERP):120ms".to_string()]);
    let link = parse_worker_link(&caps);
    assert_eq!(link.as_deref(), Some("Link: Relay (DERP) | RTT: 120ms"));
}

#[test]
fn test_worker_state_counts_only_connected_workers() {
    let connected = WorkerCapabilities::new("a", 4, 4096, false, false, None)
        .with_tags(vec!["state:connected".to_string()]);
    let gone = WorkerCapabilities::new("b", 4, 4096, false, false, None)
        .with_tags(vec!["state:disconnected".to_string()]);
    let untagged = WorkerCapabilities::new("c", 4, 4096, false, false, None);
    assert_eq!(worker_state(&connected), Some("connected"));
    assert_eq!(worker_state(&gone), Some("disconnected"));
    assert_eq!(worker_state(&untagged), None);
    assert!(worker_is_connected(&connected));
    assert!(!worker_is_connected(&gone));
    assert!(worker_is_connected(&untagged));
}

#[test]
fn test_parse_worker_link_default_tcp() {
    let caps = WorkerCapabilities::new("worker-3", 4, 4096, false, false, None);
    let link = parse_worker_link(&caps);
    assert_eq!(link.as_deref(), Some("Link: TCP/LAN"));
}

#[test]
fn test_dashboard_address_is_loopback_unless_web_ui_addr_names_another() {
    // Whatever the grid listens on, no flag means the loopback default.
    for listen in [
        "127.0.0.1:8088",
        "0.0.0.0:8088",
        "192.168.1.5:8088",
        "[::]:8088",
    ] {
        let config = ServerConfig::from_addr(listen).unwrap();
        let config = apply_web_ui_addr(config, None).unwrap();
        assert!(
            config.web_ui_addr.ip().is_loopback(),
            "listen {listen} must not move the dashboard: {}",
            config.web_ui_addr
        );
    }

    // The flag is the explicit opt-in, taken exactly as given.
    let config = ServerConfig::from_addr("0.0.0.0:8088").unwrap();
    let config = apply_web_ui_addr(config, Some("0.0.0.0:9090")).unwrap();
    assert_eq!(config.web_ui_addr, "0.0.0.0:9090".parse().unwrap());
    assert!(config.enable_web_ui);

    // Garbage is an error, not a silent fallback.
    let config = ServerConfig::from_addr("127.0.0.1:8088").unwrap();
    assert!(apply_web_ui_addr(config, Some("not an address")).is_err());
}

#[test]
fn test_dashboard_banner_tells_the_truth_about_access() {
    use rusty_grid_master::auth::DevBypass;
    let addr: std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let text = |bypass, secret| dashboard_banner(addr, bypass, secret).join("\n");

    assert!(text(DevBypass::Implicit, false).contains("this machine only, no login"));
    assert!(text(DevBypass::Explicit, false).contains("--dev is ON"));
    assert!(text(DevBypass::RefusedNonLoopback, false).contains("ignored"));
    assert!(text(DevBypass::Off, true).contains("secret show --dashboard-token"));
    let locked = text(DevBypass::Off, false);
    assert!(locked.contains("token is required") && !locked.contains("no login"));
    // Open access is never announced together with a secret-derived login hint.
    assert!(!text(DevBypass::Implicit, false).contains("secret show"));
}

#[test]
fn test_master_max_active_mb_flag_and_config_parse() {
    match parse(&["master", "--max-active-mb", "512"]).command {
        Commands::Master(a) => assert_eq!(a.max_active_mb, Some(512)),
        other => panic!("unexpected {other:?}"),
    }
    let parsed: ConfigFile = toml::from_str(
        r#"
[master]
max_active_mb = 768
"#,
    )
    .unwrap();
    assert_eq!(parsed.master.unwrap().max_active_mb, Some(768));

    let base = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    let default_cfg = apply_max_active_mb(base.clone(), None, None).unwrap();
    assert_eq!(
        default_cfg.max_active_bytes,
        rusty_grid_master::DEFAULT_MAX_ACTIVE_BYTES
    );
    let from_config = apply_max_active_mb(base.clone(), None, Some(768)).unwrap();
    assert_eq!(from_config.max_active_bytes, 768 * 1024 * 1024);
    let from_flag = apply_max_active_mb(base.clone(), Some(512), Some(768)).unwrap();
    assert_eq!(from_flag.max_active_bytes, 512 * 1024 * 1024);
    let err = apply_max_active_mb(base, Some(0), None).unwrap_err();
    assert!(err.contains("at least 1 MiB"), "unexpected error: {err}");
}

#[tokio::test]
async fn test_master_with_larger_max_active_bytes_accepts_what_default_refuses_and_reports_bytes() {
    let _env_guard = crate::test_env_lock::lock_async().await;
    // 1. `--max-active-mb 0` is refused at startup with `ExitCode::FAILURE`.
    let tmp_blob = tempfile::tempdir().unwrap();
    let blob_dir_str = tmp_blob.path().to_str().unwrap().to_string();
    let zero_cli = Cli::try_parse_from([
        "oxideswarm",
        "master",
        "--listen",
        "127.0.0.1:0",
        "--state-dir",
        &blob_dir_str,
        "--max-active-mb",
        "0",
    ])
    .unwrap();
    let Commands::Master(zero_args) = zero_cli.command else {
        panic!("expected Master subcommand");
    };
    let test_secret = ClusterSecret::from_bytes(&[0x11; 32]).unwrap();
    let secret_opts = SecretOpts {
        value: Some(crate::config::SecretText(test_secret.to_hex())),
        file: None,
    };
    let zero_exit = tokio::time::timeout(
        Duration::from_secs(5),
        run_master(zero_args, None, secret_opts.clone()),
    )
    .await
    .expect("run_master --max-active-mb 0 must exit immediately instead of serving");
    assert_eq!(zero_exit, ExitCode::FAILURE);

    // 2. `run_master` wires `--max-active-mb` into `ServerConfig::with_max_active_bytes`:
    //    starting `run_master` with `--max-active-mb 1` accepts the first 600 KiB task,
    //    reports `active_bytes` and `max_active_bytes` in `ClusterStatus`, and refuses
    //    the second 600 KiB task with `task requires ... bytes`, `bytes in use`,
    //    `limit 1048576 bytes`, and `--max-active-mb`.
    //    The master picks its own port and writes it to a port file (TEST-51).
    let port_dir = tempfile::tempdir().unwrap();
    let port_file = port_dir.path().join("master.port");
    let port_file_str = port_file.to_str().unwrap().to_string();
    let cli = Cli::try_parse_from([
        "oxideswarm",
        "master",
        "--listen",
        "127.0.0.1:0",
        "--port-file",
        &port_file_str,
        "--web-ui-addr",
        "127.0.0.1:0",
        "--max-active-mb",
        "1",
        "--state-dir",
        &blob_dir_str,
        "--blob-dir",
        &blob_dir_str,
    ])
    .unwrap();
    let Commands::Master(master_args) = cli.command else {
        panic!("expected Master subcommand");
    };
    let master_task = tokio::spawn(run_master(master_args, None, secret_opts));

    let run_master_addr_str = wait_for_master_port(
        &port_file,
        &master_task,
        "run_master bound and accepted connection",
    )
    .await;
    let mut transport = connect_master_within_bound(
        &run_master_addr_str,
        &test_secret,
        "run_master bound and accepted connection",
    )
    .await;
    let make_600k_task = |i: u8| {
        let mut task = Task::new(
            TaskSpec::Command {
                program: "echo".into(),
                args: vec![i.to_string()],
                env: Default::default(),
                working_dir: None,
                stdin: Some(vec![i; 600 * 1024]),
            },
            TaskRequirements::generic(1, 30),
        );
        task.id = rusty_grid_core::task::TaskId::new();
        task
    };
    let first_resp = exchange(
        &mut transport,
        &run_master_addr_str,
        false,
        &ClientMessage::SubmitTask {
            task: make_600k_task(1),
            wait: false,
        },
        "submit failed",
    )
    .await
    .expect("first 600 KiB task fits in 1 MiB cap");
    assert!(matches!(first_resp, ClientResponse::TaskSubmitted { .. }));

    // Verify ClusterStatus includes active_bytes and max_active_bytes.
    let status_resp = exchange(
        &mut transport,
        &run_master_addr_str,
        false,
        &ClientMessage::ClusterStatus,
        "status failed",
    )
    .await
    .expect("cluster status succeeded");
    match status_resp {
        ClientResponse::ClusterStatus {
            active_bytes,
            max_active_bytes,
            ..
        } => {
            assert!(active_bytes >= 600 * 1024, "active_bytes={active_bytes}");
            assert_eq!(max_active_bytes, 1024 * 1024);
        }
        other => panic!("expected ClusterStatus, got {other:?}"),
    }

    transport
        .send_msg(&ClientMessage::SubmitTask {
            task: make_600k_task(2),
            wait: false,
        })
        .await
        .unwrap();
    match transport.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::Error { message }) => {
            assert!(
                message.contains("task requires"),
                "error must state task requires N bytes, got: {message}"
            );
            assert!(
                message.contains("bytes in use"),
                "error must state bytes in use, got: {message}"
            );
            assert!(
                message.contains("limit 1048576 bytes"),
                "run_master --max-active-mb 1 must enforce 1 MiB limit, got: {message}"
            );
            assert!(message.contains("--max-active-mb"), "{message}");
        }
        other => panic!("expected ClientResponse::Error on second 600 KiB task, got {other:?}"),
    }
    master_task.abort();
}

#[test]
fn test_read_mapreduce_script_names_map_cmd_and_reduce_cmd_flags_and_split_unc_paths() {
    let dir = tempfile::tempdir().unwrap();
    let huge = dir.path().join("huge.sh");
    std::fs::write(&huge, "x".repeat(MAX_MAPREDUCE_SCRIPT_BYTES + 1)).unwrap();

    let map_err = read_mapreduce_script("mapper", "--map-cmd", huge.to_str().unwrap()).unwrap_err();
    assert!(
        map_err.contains("use --map-cmd"),
        "expected '--map-cmd' in mapper size error, got: {map_err}"
    );
    assert!(!map_err.contains("--mapper-cmd"), "{map_err}");

    let reduce_err =
        read_mapreduce_script("reducer", "--reduce-cmd", huge.to_str().unwrap()).unwrap_err();
    assert!(
        reduce_err.contains("use --reduce-cmd"),
        "expected '--reduce-cmd' in reducer size error, got: {reduce_err}"
    );
    assert!(!reduce_err.contains("--reducer-cmd"), "{reduce_err}");

    // Outside single quotes, `\\` is always one backslash everywhere:
    let (prog, args) =
        split_command_line(r#"prog "\\r\\n" "\\d+\\.\\d+" --regexp=\\d+\\.\\d+"#).unwrap();
    assert_eq!(prog, "prog");
    assert_eq!(args, vec![r"\r\n", r"\d+\.\d+", r"--regexp=\d+\.\d+"]);

    let (printf_prog, printf_args) = split_command_line(r#"printf "\\t%s\\n""#).unwrap();
    assert_eq!(printf_prog, "printf");
    assert_eq!(printf_args, vec![r"\t%s\n"]);

    let (ps_prog, ps_args) = split_command_line(r"psexec \\host").unwrap();
    assert_eq!(ps_prog, "psexec");
    assert_eq!(ps_args, vec![r"\host"]);

    let (sp_prog, sp_args) = split_command_line(r"prog \\ next\x").unwrap();
    assert_eq!(sp_prog, "prog");
    assert_eq!(sp_args, vec![r"\", r"next\x"]);

    // UNC paths use single quotes or doubled backslashes (`'\\srv\share\m.exe'` or
    // `\\\\srv\\share\\m.exe`):
    let (unc_prog, unc_args) =
        split_command_line(r"'\\srv\share\m.exe' '\\srv\share\arg.txt'").unwrap();
    assert_eq!(unc_prog, r"\\srv\share\m.exe");
    assert_eq!(unc_args, vec![r"\\srv\share\arg.txt"]);

    let (esc_prog, esc_args) = split_command_line(r"\\\\srv\\share\\m.exe tr -d \\r").unwrap();
    assert_eq!(esc_prog, r"\\srv\share\m.exe");
    assert_eq!(esc_args, vec!["tr", "-d", r"\r"]);
}

/// Verifies that every long flag and alias in `Cli::command()` appears inside the
/// `docs/reference.md` reference section (or summary table row) for its own subcommand.
#[test]
fn test_every_cli_long_flag_is_documented_in_reference() {
    use clap::CommandFactory;

    fn collect_long_flags(cmd: &clap::Command, path: &str, out: &mut Vec<(String, String, bool)>) {
        for arg in cmd.get_arguments() {
            let is_global_or_builtin = path == "oxideswarm"
                || arg.is_global_set()
                || matches!(arg.get_long(), Some("help" | "version"));
            if let Some(long) = arg.get_long() {
                out.push((path.to_string(), format!("--{long}"), is_global_or_builtin));
            }
            if let Some(aliases) = arg.get_all_aliases() {
                for alias in aliases {
                    out.push((path.to_string(), format!("--{alias}"), is_global_or_builtin));
                }
            }
        }
        for sub in cmd.get_subcommands() {
            if sub.get_name() == "help" {
                continue;
            }
            let sub_path = format!("{path} {}", sub.get_name());
            collect_long_flags(sub, &sub_path, out);
        }
    }

    fn section_between<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
        let s = text
            .find(start)
            .unwrap_or_else(|| panic!("missing heading in docs/reference.md: {start}"));
        let rest = &text[s..];
        let e = rest
            .find(end)
            .unwrap_or_else(|| panic!("missing heading in docs/reference.md after {start}: {end}"));
        &rest[..e]
    }

    let reference = include_str!("../../../../docs/reference.md");
    let global_sec = section_between(
        reference,
        "#### Global Flags",
        "#### `oxideswarm master` Flags",
    );
    let master_sec = section_between(
        reference,
        "#### `oxideswarm master` Flags",
        "#### `oxideswarm worker` Flags",
    );
    let worker_sec = section_between(
        reference,
        "#### `oxideswarm worker` Flags",
        "#### `oxideswarm submit` Flags",
    );
    let submit_sec = section_between(
        reference,
        "#### `oxideswarm submit` Flags",
        "#### Inspection, MapReduce, Doctor, Secret, Mode & MCP Subcommands",
    );
    let subcmd_table_sec = section_between(
        reference,
        "#### Inspection, MapReduce, Doctor, Secret, Mode & MCP Subcommands",
        "#### `agent-mesh` CLI Subcommands",
    );

    let subcommand_scope = |sub: &str, is_global: bool| -> &str {
        if is_global || sub == "oxideswarm" || sub == "oxideswarm secret" {
            return global_sec;
        }
        match sub {
            "oxideswarm master" => master_sec,
            "oxideswarm worker" => worker_sec,
            "oxideswarm submit" => submit_sec,
            other => {
                let row_prefix = format!("| `{other}");
                subcmd_table_sec
                    .lines()
                    .find(|line| line.starts_with(&row_prefix))
                    .unwrap_or_else(|| {
                        panic!("missing subcommand row in docs/reference.md for `{other}`")
                    })
            }
        }
    };

    let cmd = Cli::command();
    let mut flags = Vec::new();
    collect_long_flags(&cmd, "oxideswarm", &mut flags);
    assert!(!flags.is_empty(), "Cli::command() must expose long flags");

    let missing: Vec<String> = flags
        .into_iter()
        .filter(|(sub, flag, is_global)| {
            let scope = subcommand_scope(sub, *is_global);
            !scope.contains(flag.as_str())
        })
        .map(|(sub, flag, _)| format!("{sub}: {flag}"))
        .collect();

    assert!(
        missing.is_empty(),
        "Every CLI long flag and alias in Cli::command() must be documented in its subcommand section of docs/reference.md; missing:\n{}",
        missing.join("\n")
    );
}

#[tokio::test]
async fn test_run_cli_registers_mode_runner_hook() {
    let _env_guard = crate::test_env_lock::lock_async().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let cfg_path = temp.path().join("rusty-grid.toml");
    std::fs::write(&cfg_path, "").expect("write empty config");
    let cli = Cli::try_parse_from([
        "oxideswarm",
        "--config",
        cfg_path.to_str().expect("cfg_path utf8"),
        "--secret",
        "0101010101010101010101010101010101010101010101010101010101010101",
        "secret",
        "show",
    ])
    .unwrap();
    let code = run_cli_with(cli).await;
    assert_eq!(code, ExitCode::SUCCESS);
    let res = rusty_grid_core::mode::dispatch_mode_action(
        rusty_grid_core::mode::WorkflowMode::Research,
        "profile",
        std::path::Path::new("."),
    );
    assert!(
        res.success,
        "run_cli must register the mode runner hook before dispatching subcommands: {}",
        res.stderr
    );
}

#[test]
fn test_resolve_no_battery_only_accepts_rusty_grid_no_battery_env_and_not_config_or_aliases() {
    let _env_guard = crate::test_env_lock::lock_blocking();
    let _no_bat_clear = crate::test_env_lock::ScopedEnvVar::remove("RUSTY_GRID_NO_BATTERY");
    let _alias_oxide_clear = crate::test_env_lock::ScopedEnvVar::remove("OXIDE_SWARM_NO_BATTERY");
    let _alias_disable_clear =
        crate::test_env_lock::ScopedEnvVar::remove("RUSTY_GRID_DISABLE_BATTERY");

    assert!(!resolve_no_battery());

    let _alias_oxide_set = crate::test_env_lock::ScopedEnvVar::set("OXIDE_SWARM_NO_BATTERY", "yes");
    let _alias_disable_set =
        crate::test_env_lock::ScopedEnvVar::set("RUSTY_GRID_DISABLE_BATTERY", "on");
    assert!(
        !resolve_no_battery(),
        "undocumented alias env vars must not disable battery protection"
    );

    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("rusty-grid.toml");
    std::fs::write(
        &cfg_path,
        "[worker]\nno_battery = true\n[worker.hardware]\nno_battery = true\n",
    )
    .unwrap();
    let loaded = crate::config::load_config_file(Some(&cfg_path)).unwrap();
    assert!(
        loaded.as_ref().and_then(|c| c.worker.as_ref()).is_some(),
        "config file with unknown no_battery keys must still load"
    );

    // Pass an explicit cluster secret via Tier 1 (`SecretOpts::value`) so `run_worker` never
    // consults `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, or `OXIDESWARM_HOME`
    // (`~/.oxideswarm/cluster.secret`), avoiding process-wide environment mutations.
    let secret = ClusterSecret::from_bytes(&[0x55; 32]).unwrap();
    let secret_opts = SecretOpts {
        value: Some(SecretText(secret.to_hex())),
        file: None,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rusty_grid_core::capabilities::with_test_battery_probe((Some(10), Some(false)), || {
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let master_addr = listener.local_addr().unwrap().to_string();

            let accept_register = || async {
                let (stream, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .expect("run_worker must connect to local listener")
                        .unwrap();
                let mut transport = MessageTransport::new(stream);
                let session_key = tokio::time::timeout(
                    Duration::from_secs(5),
                    rusty_grid_core::cluster_auth::server_handshake(&mut transport, &secret),
                )
                .await
                .expect("run_worker must complete OXS-AUTH handshake in time")
                .expect("OXS-AUTH handshake must succeed");
                transport.enable_encryption(
                    &session_key,
                    rusty_grid_core::protocol::EndpointRole::Server,
                );
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    transport.recv_msg::<rusty_grid_core::protocol::WorkerMessage>(),
                )
                .await
                .expect("run_worker must send Register")
                .unwrap()
                {
                    Some(rusty_grid_core::protocol::WorkerMessage::Register {
                        capabilities,
                        ..
                    }) => capabilities,
                    other => panic!("expected WorkerMessage::Register, got {other:?}"),
                }
            };

            let make_worker_args = |sandbox_sub: &str| {
                let sb = dir.path().join(sandbox_sub);
                let cli = Cli::try_parse_from([
                    "oxideswarm",
                    "worker",
                    "--master",
                    &master_addr,
                    "--no-gpu",
                    "--no-cuda",
                    "--no-blender",
                    "--no-portal",
                    "--sandbox-base-dir",
                    sb.to_str().unwrap(),
                ])
                .unwrap();
                let Commands::Worker(w_args) = cli.command else {
                    panic!("expected Worker");
                };
                w_args
            };

            // 1. With `no_battery = true` in `rusty-grid.toml` (and alias env vars set), `run_worker`
            //    must still probe the 10% simulated battery and advertise `battery_pct == Some(10)`.
            let w_task1 = tokio::spawn(run_worker(
                make_worker_args("sb1"),
                loaded.clone(),
                secret_opts.clone(),
            ));
            let caps_from_cfg = accept_register().await;
            w_task1.abort();
            assert_eq!(
                caps_from_cfg.mobile.as_ref().and_then(|m| m.battery_pct),
                Some(10),
                "no_battery = true in rusty-grid.toml must not disable battery probing in run_worker"
            );

            // 2. With `RUSTY_GRID_NO_BATTERY=1` in the environment, `run_worker` disables battery probing.
            let _no_bat_set = crate::test_env_lock::ScopedEnvVar::set("RUSTY_GRID_NO_BATTERY", "1");
            assert!(
                resolve_no_battery(),
                "RUSTY_GRID_NO_BATTERY=1 must disable host battery probing in the CLI layer"
            );
            let w_task2 = tokio::spawn(run_worker(
                make_worker_args("sb2"),
                loaded,
                secret_opts,
            ));
            let caps_from_env = accept_register().await;
            w_task2.abort();
            assert!(
                caps_from_env.mobile.is_none(),
                "RUSTY_GRID_NO_BATTERY=1 must suppress host battery probing in run_worker"
            );
        });
    });
}

#[test]
fn test_resolve_p2p_minimal_only_accepts_rusty_grid_p2p_minimal_env() {
    assert!(!resolve_p2p_minimal_from(None));

    for disabled in ["0", "false", "FALSE", "no", "off", "", "   ", "2"] {
        assert!(
            !resolve_p2p_minimal_from(Some(disabled)),
            "RUSTY_GRID_P2P_MINIMAL={disabled:?} must resolve to false"
        );
    }

    for enabled in [
        "1", "true", "TRUE", "yes", "YES", "on", "ON", " 1 ", " true ",
    ] {
        assert!(
            resolve_p2p_minimal_from(Some(enabled)),
            "RUSTY_GRID_P2P_MINIMAL={enabled:?} must resolve to true"
        );
    }
}

#[tokio::test]
async fn test_v3_submit_against_fake_v2_master_never_sends_submit_task_and_leaves_no_marker() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let _env_guard = crate::test_env_lock::lock_async().await;
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("marker");
    let marker_for_srv = marker.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_str = listener.local_addr().unwrap().to_string();

    let saw_submit_task = Arc::new(AtomicBool::new(false));
    let saw_submit_srv = Arc::clone(&saw_submit_task);
    let frames_received = Arc::new(AtomicUsize::new(0));
    let frames_srv = Arc::clone(&frames_received);

    let srv_handle = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut t = MessageTransport::new(stream);
            while let Ok(Some(raw_frame)) = t.recv_raw_frame().await {
                frames_srv.fetch_add(1, Ordering::SeqCst);
                if raw_frame.len() >= 5
                    && raw_frame[0] == rusty_grid_core::protocol::WIRE_FORMAT_BINCODE
                {
                    let tag = u32::from_le_bytes(raw_frame[1..5].try_into().unwrap());
                    if tag == 0 {
                        // Variant 0 of BinaryClientMessage is SubmitTask: simulate a v2 master
                        // that ignores the trailing protocol_version and executes the command.
                        saw_submit_srv.store(true, Ordering::SeqCst);
                        let _ = std::fs::write(&marker_for_srv, b"ran on v2 master");
                        break;
                    }
                    if tag == 3 {
                        // Variant 3 is ClusterStatus: reply with an unversioned v2
                        // BinaryClientResponse::ClusterStatus (tag 4 + 7 u64 fields + 0-len vec).
                        let mut v2_resp = vec![rusty_grid_core::protocol::WIRE_FORMAT_BINCODE];
                        v2_resp.extend_from_slice(&4u32.to_le_bytes());
                        for _ in 0..8 {
                            v2_resp.extend_from_slice(&0u64.to_le_bytes());
                        }
                        let _ = t.send_raw_frame(v2_resp.into()).await;
                    }
                }
            }
        }
    });

    let secret_opts = SecretOpts {
        value: None,
        file: None,
    };
    let marker_str = marker.to_string_lossy().to_string();
    let s_args = submit_args(&[
        "--master",
        &addr_str,
        "--wait",
        "--command",
        "touch",
        "--",
        &marker_str,
    ]);
    let exit = run_submit_with_secret(s_args, None, secret_opts).await;

    let _ = srv_handle.await;

    assert_eq!(
        exit,
        ExitCode::FAILURE,
        "v3 submit against v2 master must exit 1"
    );
    assert!(
        !saw_submit_task.load(Ordering::SeqCst),
        "fake v2 master must never receive a SubmitTask frame"
    );
    assert!(
        !marker.exists(),
        "mixed v3 CLI -> v2 master run must leave no marker file at {}",
        marker.display()
    );
    assert_eq!(
        frames_received.load(Ordering::SeqCst),
        1,
        "only the pre-flight ClusterStatus probe should be sent before refusing the v2 master"
    );
}

#[test]
fn test_master_state_dir_flag_and_env_precedence() {
    let cli = Cli::try_parse_from(["oxideswarm", "master", "--state-dir", "/tmp/cli-state"])
        .expect("parse master --state-dir");
    let Commands::Master(m_args) = cli.command else {
        panic!("expected Master");
    };
    assert_eq!(
        m_args.state_dir.as_deref(),
        Some(std::path::Path::new("/tmp/cli-state"))
    );

    let resolved_cli = resolve_state_dir_value(
        Some(PathBuf::from("/tmp/cli-state")),
        Some("/tmp/env-state"),
        Some("/tmp/cfg-state"),
    );
    let resolved_env =
        resolve_state_dir_value(None, Some("/tmp/env-state"), Some("/tmp/cfg-state"));
    let resolved_cfg = resolve_state_dir_value(None, Some("   "), Some("/tmp/cfg-state"));
    let resolved_none = resolve_state_dir_value(None, None, None);

    assert_eq!(
        resolved_cli.as_deref(),
        Some(std::path::Path::new("/tmp/cli-state"))
    );
    assert_eq!(
        resolved_env.as_deref(),
        Some(std::path::Path::new("/tmp/env-state"))
    );
    assert_eq!(
        resolved_cfg.as_deref(),
        Some(std::path::Path::new("/tmp/cfg-state"))
    );
    assert_eq!(resolved_none, None);

    let cli_hold =
        Cli::try_parse_from(["oxideswarm", "master", "--session-resume-hold-secs", "45"])
            .expect("parse master --session-resume-hold-secs");
    let Commands::Master(m_hold_args) = cli_hold.command else {
        panic!("expected Master");
    };
    assert_eq!(m_hold_args.session_resume_hold_secs, Some(45));

    let cli_grace =
        Cli::try_parse_from(["oxideswarm", "worker", "--session-resume-grace-secs", "15"])
            .expect("parse worker --session-resume-grace-secs");
    let Commands::Worker(w_grace_args) = cli_grace.command else {
        panic!("expected Worker");
    };
    assert_eq!(w_grace_args.session_resume_grace_secs, Some(15));
}

#[test]
fn test_submit_require_tool_flag_parses_and_builds_placement() {
    let placement = build_placement(&submit_args(&[
        "--require-tool",
        "Cargo",
        "--require-tool",
        "rustc",
        "--",
        "true",
    ]))
    .unwrap();
    assert_eq!(placement.tools, ["cargo", "rustc"]);
    assert_eq!(
        placement.to_task_tags(),
        ["require:tool=cargo", "require:tool=rustc"]
    );

    let err =
        build_placement(&submit_args(&["--require-tool", "bad:tool", "--", "true"])).unwrap_err();
    assert!(err.contains("require:tool"), "{err}");

    let tag_err =
        build_placement(&submit_args(&["--require-tag", "tool:cargo", "--", "true"])).unwrap_err();
    assert!(
        tag_err.contains("only the worker or the master"),
        "{tag_err}"
    );

    // `submit --type compile` automatically adds `require:tool=cargo` when `Cargo.toml` is
    // present and `require:tool=rustc` otherwise, unless `--require-tool` was already set.
    let mut cargo_sources = HashMap::new();
    cargo_sources.insert("Cargo.toml".into(), "[package]\nname = \"a\"\n".into());
    cargo_sources.insert("src/main.rs".into(), "fn main() {}\n".into());
    let cargo_spec = TaskSpec::rust_compilation("a", cargo_sources.clone(), vec![]);
    let mut default_cargo_placement = Placement::default();
    apply_default_spec_placement(&cargo_spec, &mut default_cargo_placement);
    assert_eq!(
        default_cargo_placement.to_task_tags(),
        ["require:tool=cargo"]
    );

    let mut rustc_sources = HashMap::new();
    rustc_sources.insert("src/lib.rs".into(), "pub fn f() {}\n".into());
    let rustc_spec = TaskSpec::rust_compilation("b", rustc_sources, vec![]);
    let mut default_rustc_placement = Placement::default();
    apply_default_spec_placement(&rustc_spec, &mut default_rustc_placement);
    assert_eq!(
        default_rustc_placement.to_task_tags(),
        ["require:tool=rustc"]
    );

    let mut explicit_placement =
        build_placement(&submit_args(&["--require-tool", "git", "--", "true"])).unwrap();
    apply_default_spec_placement(&cargo_spec, &mut explicit_placement);
    assert_eq!(explicit_placement.to_task_tags(), ["require:tool=git"]);
}

#[test]
fn test_submit_repeatable_flag_and_reserved_opt_speed_tags() {
    let default_args = submit_args(&["--", "echo", "hi"]);
    assert!(!default_args.repeatable);

    let rep_args = submit_args(&["--repeatable", "--", "echo", "hi"]);
    assert!(rep_args.repeatable);

    let mut caps = WorkerCapabilities::new("w1", 4, 4096, false, false, None)
        .with_tags(vec!["os:linux".into(), "speed:1.25".into()]);
    assert_eq!(parse_worker_speed_str(&caps), Some("1.25"));
    assert_eq!(parse_worker_speed(&caps), Some(1.25));

    caps.tags = vec!["speed:invalid".into()];
    assert_eq!(parse_worker_speed(&caps), None);

    assert!(rusty_grid_core::placement::validate_user_tag("opt:repeatable").is_err());
    assert!(rusty_grid_core::placement::validate_user_tag("speed:1.0").is_err());
    assert!(build_placement(&submit_args(&[
        "--require-tag",
        "opt:repeatable",
        "--",
        "true"
    ]))
    .is_err());
    assert!(build_placement(&submit_args(&["--require-tag", "speed:1.0", "--", "true"])).is_err());
}

#[tokio::test]
async fn test_cli_env_lock_serializes_wire_codec_mutation_and_restores_on_panic() {
    let _env_guard = crate::test_env_lock::lock_async().await;
    let _baseline_clear = crate::test_env_lock::ScopedEnvVar::remove("RUSTY_GRID_WIRE_CODEC");

    // 1. Panicking while holding ScopedEnvVar restores the previous value on unwind.
    let panic_res = std::panic::catch_unwind(|| {
        let _xml = crate::test_env_lock::ScopedEnvVar::set("RUSTY_GRID_WIRE_CODEC", "xml");
        panic!("simulated panic while env var overridden");
    });
    assert!(panic_res.is_err());
    assert_eq!(
        std::env::var_os("RUSTY_GRID_WIRE_CODEC"),
        None,
        "ScopedEnvVar::drop must restore RUSTY_GRID_WIRE_CODEC on unwind"
    );
    drop(_baseline_clear);
    drop(_env_guard);

    // 2. Concurrent tasks contending on test_env_lock::lock_async() never observe "xml".
    for _ in 0..20 {
        let mutator = tokio::spawn(async {
            let _lock = crate::test_env_lock::lock_async().await;
            let _xml = crate::test_env_lock::ScopedEnvVar::set("RUSTY_GRID_WIRE_CODEC", "xml");
            tokio::task::yield_now().await;
        });
        let reader = tokio::spawn(async {
            let _lock = crate::test_env_lock::lock_async().await;
            assert!(
                resolve_wire_codec(None, None).is_ok(),
                "locked sibling must never observe transient invalid RUSTY_GRID_WIRE_CODEC=xml"
            );
        });
        let (r1, r2) = tokio::join!(mutator, reader);
        r1.unwrap();
        r2.unwrap();
    }
}

#[test]
fn test_plan_cli_subcommands_parse_cleanly() {
    let check_cli = Cli::try_parse_from([
        "oxideswarm",
        "plan",
        "check",
        "--plan",
        "custom.toml",
        "--json",
    ])
    .expect("parse plan check");
    let Commands::Plan(check_args) = check_cli.command else {
        panic!("expected Plan");
    };
    let crate::plan::PlanSubcommand::Check { plan, json } = check_args.command else {
        panic!("expected Check");
    };
    assert_eq!(plan.as_deref(), Some(std::path::Path::new("custom.toml")));
    assert!(json);

    let status_cli = Cli::try_parse_from([
        "oxideswarm",
        "plan",
        "status",
        "--repo",
        ".",
        "--base",
        "master",
        "--json",
    ])
    .expect("parse plan status");
    let Commands::Plan(status_args) = status_cli.command else {
        panic!("expected Plan");
    };
    let crate::plan::PlanSubcommand::Status {
        plan,
        repo,
        base,
        json,
    } = status_args.command
    else {
        panic!("expected Status");
    };
    assert!(plan.is_none());
    assert_eq!(repo.as_deref(), Some(std::path::Path::new(".")));
    assert_eq!(base.as_deref(), Some("master"));
    assert!(json);
}
