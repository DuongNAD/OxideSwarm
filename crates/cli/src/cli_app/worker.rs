//! `oxideswarm worker` target/isolation resolution and execution loop.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use tokio::sync::watch;

use rusty_grid_core::placement::{validate_user_tag, IsolationLevel};
use rusty_grid_worker::control::{
    parse_pause_until, send_control_request, ControlEndpoint, ControlRequest, ControlResponse,
    LimitPersistHook, WorkerLimitsConfig,
};
use rusty_grid_worker::{NoticeLevel, ProcessPriority, WorkerClient, WorkerConfig};

use crate::config::{
    resolve_bool, resolve_opt_field, resolve_opt_string, resolve_opt_u64, resolve_opt_usize,
    resolve_u64, resolve_usize, ConfigFile, SecretSection,
};
use crate::connect::{normalize_master_addr, AUTH_EXIT_CODE, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::{
    announce_peer_secret, peer_insecure, PeerRole, PeerSecretOutcome, SecretOpts,
};

use super::{
    resolve_no_battery, resolve_p2p_minimal, resolve_relay_url, resolve_wire_codec,
    wait_for_worker_shutdown_signal, WorkerArgs, WorkerControlSubcommand, WorkerLimitsSubcommand,
    MIB,
};

/// Resolves `(master_addr, p2p_ticket)` from explicit CLI, env and config values without reading
/// the process environment.
pub(super) fn resolve_worker_target_from(
    cli_master: Option<&str>,
    cli_ticket: Option<&str>,
    env_master: Option<(&str, &str)>,
    env_ticket: Option<&str>,
    worker_cfg: Option<&crate::config::WorkerFileConfig>,
) -> Result<(String, Option<String>), String> {
    let clean = |s: Option<&str>| {
        s.map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    };
    let cli_master = clean(cli_master);
    let cli_ticket = clean(cli_ticket);
    let env_master = env_master.and_then(|(var, val)| clean(Some(val)).map(|v| (var, v)));
    let env_ticket = clean(env_ticket);
    let cfg_master = clean(worker_cfg.and_then(|w| w.master.as_deref()));
    let cfg_ticket = clean(worker_cfg.and_then(|w| w.p2p_ticket.as_deref()));

    let p2p_ticket = if cli_ticket.is_some() {
        cli_ticket
    } else if cli_master.is_some() {
        None
    } else if env_ticket.is_some() {
        env_ticket
    } else if env_master.is_some() {
        None
    } else {
        cfg_ticket
    };

    let (source, master_raw) = if let Some(ref m) = cli_master {
        ("--master", m.clone())
    } else if let Some((var, ref m)) = env_master {
        (var, m.clone())
    } else if let Some(ref m) = cfg_master {
        ("[worker].master", m.clone())
    } else if p2p_ticket.is_some() {
        ("p2p-ticket", String::new())
    } else {
        ("default", DEFAULT_MASTER_ADDR.to_string())
    };
    if crate::connect::is_p2p_ticket(&master_raw) {
        return Err(match source {
            "--master" => {
                "error: --master: this looks like a P2P ticket; use --p2p-ticket <TICKET> instead"
                    .into()
            }
            "[worker].master" => {
                "error: [worker].master: this looks like a P2P ticket; use --p2p-ticket <TICKET> or [worker].p2p_ticket instead"
                    .into()
            }
            var => format!(
                "error: {var}: this looks like a P2P ticket; use --p2p-ticket <TICKET> or RUSTY_GRID_P2P_TICKET instead"
            ),
        });
    }
    let master_addr = normalize_master_addr(&master_raw);

    Ok((master_addr, p2p_ticket))
}

fn peer_announce_target(master_addr: &str, p2p_ticket: Option<&str>) -> String {
    if !master_addr.trim().is_empty() {
        return master_addr.to_string();
    }
    match p2p_ticket {
        Some(ticket) => match rusty_grid_core::transport::parse_p2p_ticket(ticket) {
            Ok(addr) => format!("iroh://{}", addr.id),
            Err(_) => "P2P ticket".to_string(),
        },
        None => String::new(),
    }
}

/// Resolves `(master_addr, p2p_ticket)` for `oxideswarm worker` across CLI > Env > Config > Default,
/// ensuring that an explicit `--master` (or `RUSTY_GRID_MASTER_ADDR` / `RUSTY_GRID_MASTER`) is not
/// overridden by a lower-tier `p2p_ticket` (such as `[worker] p2p_ticket` in the config file).
pub(super) fn resolve_worker_target(
    cli_master: Option<String>,
    cli_ticket: Option<String>,
    worker_cfg: Option<&crate::config::WorkerFileConfig>,
) -> Result<(String, Option<String>), String> {
    let env_master = ["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"]
        .into_iter()
        .find_map(|name| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .map(|v| (name, v))
        });
    let env_ticket = resolve_opt_string(None, &["RUSTY_GRID_P2P_TICKET"], None);
    resolve_worker_target_from(
        cli_master.as_deref(),
        cli_ticket.as_deref(),
        env_master.as_ref().map(|(k, v)| (*k, v.as_str())),
        env_ticket.as_deref(),
        worker_cfg,
    )
}

pub(super) async fn run_worker(
    args: WorkerArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    rusty_grid_core::capabilities::suppress_windows_error_dialogs();
    if let Some(action) = args.action.clone() {
        return run_worker_control_action(
            action,
            args.control_socket.as_deref(),
            args.name.as_deref(),
        )
        .await;
    }
    let rendezvous_url =
        match crate::connect::resolve_and_validate_rendezvous_url(args.rendezvous.as_deref()) {
            Ok(url) => url,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
    let worker_cfg = config_file.as_ref().and_then(|c| c.worker.clone());
    let hw_cfg = worker_cfg.as_ref().and_then(|w| w.hardware.clone());

    let p2p_minimal = resolve_p2p_minimal();
    let relay_url = match resolve_relay_url(
        args.relay_url,
        config_file.as_ref().and_then(|c| c.worker_relay_url()),
        p2p_minimal,
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let (master_addr, p2p_ticket) =
        match resolve_worker_target(args.master, args.p2p_ticket, worker_cfg.as_ref()) {
            Ok(resolved) => resolved,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };

    let v3_auth = match secret_opts.resolve_v3_auth(config_file.as_ref(), SecretSection::Worker) {
        Ok(v3) => v3,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let cluster_secret = match secret_opts.resolve(config_file.as_ref(), SecretSection::Worker) {
        Ok(secret) => secret,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    // Fail closed: a worker obeys whatever its master sends, so without a credential it may only
    // follow a master on this machine. Remote, `auto`-discovered and P2P-ticket masters need a
    // cluster credential (the cluster secret or a pinned v3 master key) unless --insecure is given.
    let insecure = peer_insecure(args.insecure, worker_cfg.as_ref().and_then(|w| w.insecure));
    let peer_outcome = match crate::secret_cmd::decide_peer_credential(
        cluster_secret.is_some() || v3_auth.is_some(),
        &master_addr,
        p2p_ticket.is_some(),
        insecure,
        PeerRole::Worker,
    ) {
        Ok(outcome) => outcome,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(AUTH_EXIT_CODE);
        }
    };
    let announce_target = peer_announce_target(&master_addr, p2p_ticket.as_deref());
    announce_peer_secret(peer_outcome, PeerRole::Worker, &announce_target);

    let name = resolve_opt_string(
        args.name,
        &["RUSTY_GRID_WORKER_NAME", "RUSTY_GRID_NAME"],
        worker_cfg.as_ref().and_then(|w| w.name.clone()),
    );

    let tags = resolve_opt_field(
        (!args.tags.is_empty()).then_some(args.tags),
        &["RUSTY_GRID_WORKER_TAGS"],
        worker_cfg.as_ref().and_then(|w| w.tags.clone()),
        |list| {
            let tags: Vec<String> = list
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string)
                .collect();
            (!tags.is_empty()).then_some(tags)
        },
    )
    .unwrap_or_default();
    if let Some(e) = tags.iter().find_map(|t| validate_user_tag(t).err()) {
        eprintln!("--tag: {e}");
        return ExitCode::FAILURE;
    }

    let cores = resolve_opt_usize(
        args.cores,
        &["RUSTY_GRID_CORES", "RUSTY_GRID_CPU_CORES"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.cores)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.cores)),
    );

    let ram_mb = resolve_opt_u64(
        args.ram_mb,
        &["RUSTY_GRID_RAM_MB", "RUSTY_GRID_OVERRIDE_RAM_MB"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.ram_mb)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.ram_mb)),
    );

    let gpu = resolve_opt_field(
        if args.gpu { Some(true) } else { None },
        &["RUSTY_GRID_GPU", "RUSTY_GRID_ENABLE_GPU"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.gpu)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.gpu)),
        |s| s.parse().ok(),
    );

    let no_gpu = resolve_bool(
        if args.no_gpu { Some(true) } else { None },
        &["RUSTY_GRID_NO_GPU"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.no_gpu)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.no_gpu)),
        false,
    );

    let simulate_gpu = resolve_bool(
        if args.simulate_gpu { Some(true) } else { None },
        &["RUSTY_GRID_SIMULATE_GPU"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.simulate_gpu)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.simulate_gpu)),
        false,
    );

    let gpu_name = resolve_opt_string(
        args.gpu_name,
        &["RUSTY_GRID_GPU_NAME"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.gpu_name.clone())
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.gpu_name.clone())),
    );

    let blender = resolve_opt_field(
        if args.blender { Some(true) } else { None },
        &["RUSTY_GRID_BLENDER", "RUSTY_GRID_ENABLE_BLENDER"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.blender)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.blender)),
        |s| s.parse().ok(),
    );

    let no_blender = resolve_bool(
        if args.no_blender { Some(true) } else { None },
        &["RUSTY_GRID_NO_BLENDER", "RUSTY_GRID_DISABLE_BLENDER"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.no_blender)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.no_blender)),
        false,
    );

    let blender_path = resolve_opt_string(
        args.blender_path,
        &["RUSTY_GRID_BLENDER_PATH"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.blender_path.clone())
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.blender_path.clone())),
    );

    let blender_version = resolve_opt_string(
        args.blender_version,
        &["RUSTY_GRID_BLENDER_VERSION"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.blender_version.clone())
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.blender_version.clone())),
    );

    let vram_mb = resolve_opt_u64(
        args.vram_mb,
        &["RUSTY_GRID_VRAM_MB", "RUSTY_GRID_GPU_VRAM_MB"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.vram_mb)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.vram_mb)),
    );

    let cuda = resolve_opt_field(
        if args.cuda { Some(true) } else { None },
        &["RUSTY_GRID_CUDA", "RUSTY_GRID_ENABLE_CUDA"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.cuda)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.cuda)),
        |s| s.parse().ok(),
    );

    let no_cuda = resolve_bool(
        if args.no_cuda { Some(true) } else { None },
        &["RUSTY_GRID_NO_CUDA", "RUSTY_GRID_DISABLE_CUDA"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.no_cuda)
            .or_else(|| hw_cfg.as_ref().and_then(|h| h.no_cuda)),
        false,
    );

    let no_battery = resolve_no_battery();

    let max_concurrency = resolve_opt_usize(
        args.max_concurrency,
        &[
            "RUSTY_GRID_MAX_CONCURRENCY",
            "RUSTY_GRID_MAX_CONCURRENT_TASKS",
        ],
        worker_cfg.as_ref().and_then(|w| w.max_concurrency),
    );

    let heartbeat_interval = resolve_u64(
        args.heartbeat_interval_secs,
        &["RUSTY_GRID_HEARTBEAT_INTERVAL_SECS"],
        worker_cfg.as_ref().and_then(|w| w.heartbeat_interval_secs),
        3,
    );

    let session_resume_grace_secs = resolve_opt_u64(
        args.session_resume_grace_secs,
        &["RUSTY_GRID_SESSION_RESUME_GRACE_SECS"],
        worker_cfg
            .as_ref()
            .and_then(|w| w.session_resume_grace_secs),
    );

    let sandbox_base_dir = resolve_opt_string(
        args.sandbox_base_dir.map(|p| p.display().to_string()),
        &["RUSTY_GRID_SANDBOX_BASE_DIR"],
        worker_cfg.as_ref().and_then(|w| w.sandbox_base_dir.clone()),
    );

    // A misspelt priority must not silently run tasks at full priority on the owner's machine.
    let task_priority =
        match resolve_opt_string(args.task_priority, &["RUSTY_GRID_TASK_PRIORITY"], None)
            .map(|text| text.parse::<ProcessPriority>())
        {
            Some(Ok(priority)) => Some(priority),
            Some(Err(e)) => {
                eprintln!("--task-priority: {e}");
                return ExitCode::FAILURE;
            }
            None => None,
        };

    // A misspelt level must not silently run tasks unconfined on the owner's machine.
    let isolation = match resolve_isolation(
        args.isolation,
        &[ISOLATION_ENV],
        worker_cfg.as_ref().and_then(|w| w.isolation.clone()),
    ) {
        Ok(level) => level,
        Err(e) => {
            eprintln!("--isolation: {e}");
            return ExitCode::FAILURE;
        }
    };
    let isolation_read_dirs = resolve_isolation_read_dirs(
        &args.isolation_read_dirs,
        std::env::var_os(rusty_grid_worker::ISOLATION_READ_DIRS_ENV).as_deref(),
        worker_cfg
            .as_ref()
            .and_then(|w| w.isolation_read_dirs.as_deref()),
    );
    if isolation == IsolationLevel::Off && !isolation_read_dirs.is_empty() {
        eprintln!(
            "Warning: --isolation-read-dir is configured while --isolation is off; extra read directories apply only when --isolation fs or fs+net is active"
        );
        tracing::warn!(
            count = isolation_read_dirs.len(),
            "--isolation-read-dir has no effect while --isolation is off"
        );
    }
    // A task can read the environment and argv of this user's other processes on macOS, so a
    // secret passed that way would defeat the isolation it is meant to enforce.
    if isolation != IsolationLevel::Off {
        let exposed = crate::secret_cmd::secret_exposed_to_tasks(&secret_opts, |name| {
            std::env::var_os(name).map(|v| v.to_string_lossy().into_owned())
        });
        if let Some(channel) = exposed {
            eprintln!("{}", crate::secret_cmd::exposed_secret_error(channel));
            return ExitCode::from(AUTH_EXIT_CODE);
        }
    }

    let keep_sandboxes = resolve_bool(
        if args.keep_sandboxes {
            Some(true)
        } else {
            None
        },
        &["RUSTY_GRID_KEEP_SANDBOXES"],
        worker_cfg.as_ref().and_then(|w| w.keep_sandboxes),
        false,
    );

    let no_portal = resolve_bool(
        args.no_portal.then_some(true),
        &["RUSTY_GRID_NO_PORTAL"],
        worker_cfg.as_ref().and_then(|w| w.no_portal),
        false,
    );

    // Decided here, before `name` moves into the config: see `identity::persistence_wanted`.
    let keep_worker_id = rusty_grid_worker::identity::persistence_wanted(
        name.as_deref(),
        std::env::var_os(rusty_grid_worker::identity::WORKER_ID_FILE_ENV).as_deref(),
    );

    let mut config = WorkerConfig::new(master_addr);
    if let Some((node_key, master_pub)) = v3_auth {
        config = config.with_v3_auth(node_key, master_pub);
    }
    if let Some(secret) = cluster_secret.as_ref() {
        config = config.with_cluster_secret(secret.clone());
    }
    if let Some(ticket) = p2p_ticket {
        config = config.with_p2p_ticket(ticket);
    }
    if p2p_minimal {
        config = config.with_p2p_minimal_preset(true);
    }
    config = config
        .with_relay_url(relay_url)
        .with_rendezvous_url(rendezvous_url);
    if let Some(n) = name {
        config = config.with_name(n);
    }
    config = config.with_tags(tags);
    if let Some(c) = cores {
        config = config.with_cores(c);
    }
    if let Some(r) = ram_mb {
        config = config.with_ram_mb(r);
    }
    if let Some(g) = gpu {
        config = config.with_gpu(g);
    }
    if no_gpu {
        config = config.with_no_gpu(true);
    }
    // Look for a real GPU unless told not to (a build without the wgpu engine says so below).
    config = config.with_gpu_autodetect(!no_gpu);
    if simulate_gpu {
        config = config.with_simulate_gpu(true);
    }
    if let Some(gn) = gpu_name {
        config = config.with_gpu_name(gn);
    }
    if let Some(b) = blender {
        config = config.with_blender(b);
    }
    if no_blender {
        config = config.with_no_blender(true);
    }
    if let Some(bp) = blender_path {
        config = config.with_blender_path(bp);
    }
    if let Some(bv) = blender_version {
        config = config.with_blender_version(bv);
    }
    if let Some(vram) = vram_mb {
        config = config.with_vram_mb(vram);
    }
    if let Some(c) = cuda {
        config = config.with_cuda(c);
    }
    if no_cuda {
        config = config.with_no_cuda(true);
    }
    if no_battery {
        config = config.with_no_battery(true);
    }
    if let Some(mc) = max_concurrency {
        config = config.with_max_concurrency(mc);
    }
    if let Some(sbd) = sandbox_base_dir {
        config = config.with_sandbox_base_dir(PathBuf::from(sbd));
    }
    if let Some(priority) = task_priority {
        config = config.with_process_priority(priority);
    }
    config = config
        .with_isolation(isolation)
        .with_isolation_read_dirs(isolation_read_dirs);
    if isolation != IsolationLevel::Off {
        config = config.with_protected_files(crate::secret_cmd::secret_files_to_protect(
            &secret_opts,
            config_file.as_ref(),
            |name| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned()),
        ));
    }
    config = config.with_keep_sandboxes(keep_sandboxes);
    let blob_cache_mb = resolve_u64(
        args.blob_cache_mb,
        &["RUSTY_GRID_BLOB_CACHE_MB"],
        worker_cfg.as_ref().and_then(|w| w.blob_cache_mb),
        rusty_grid_worker::DEFAULT_BLOB_CACHE_MAX_BYTES / MIB,
    );
    let output_limit_mb = resolve_u64(
        args.output_limit_mb,
        &["RUSTY_GRID_OUTPUT_LIMIT_MB"],
        worker_cfg.as_ref().and_then(|w| w.output_limit_mb),
        rusty_grid_worker::DEFAULT_OUTPUT_LIMIT_BYTES / MIB,
    );
    let workspaces = resolve_bool(
        args.workspaces.then_some(true),
        &["RUSTY_GRID_WORKSPACES"],
        worker_cfg.as_ref().and_then(|w| w.workspaces),
        false,
    );
    let workspace_root = resolve_opt_string(
        args.workspace_root.map(|p| p.display().to_string()),
        &["RUSTY_GRID_WORKSPACE_ROOT"],
        worker_cfg.as_ref().and_then(|w| w.workspace_root.clone()),
    )
    .map(PathBuf::from);
    let workspace_limit_mb = resolve_u64(
        args.workspace_limit_mb,
        &["RUSTY_GRID_WORKSPACE_LIMIT_MB"],
        worker_cfg.as_ref().and_then(|w| w.workspace_limit_mb),
        rusty_grid_worker::DEFAULT_WORKSPACE_LIMIT_MB,
    );
    if workspace_limit_mb == 0 {
        eprintln!("--workspace-limit-mb / workspace_limit_mb must be at least 1 MiB (got 0)");
        return ExitCode::FAILURE;
    }
    let max_workspaces = resolve_usize(
        args.max_workspaces,
        &["RUSTY_GRID_MAX_WORKSPACES"],
        worker_cfg.as_ref().and_then(|w| w.max_workspaces),
        rusty_grid_worker::DEFAULT_MAX_WORKSPACES,
    );
    if max_workspaces == 0 {
        eprintln!("--max-workspaces / max_workspaces must be at least 1 (got 0)");
        return ExitCode::FAILURE;
    }
    config = config
        .with_blob_cache_bytes(blob_cache_mb.saturating_mul(MIB))
        .with_output_limit_bytes(output_limit_mb.saturating_mul(MIB))
        .with_workspaces(workspaces)
        .with_workspace_root(workspace_root)
        .with_workspace_limit_mb(workspace_limit_mb)
        .with_max_workspaces(max_workspaces);
    config = config.with_redirection_portal(!no_portal);
    // A restarted worker is the same machine, not a new one: keep its id (see `identity`), when it
    // knows which machine it is. Otherwise every unnamed worker under this HOME would share one
    // id and the master would keep closing the other's connection.
    if keep_worker_id {
        config = config.with_persistent_worker_id();
    }
    config.default_heartbeat_interval = Duration::from_secs(heartbeat_interval);
    if let Some(secs) = session_resume_grace_secs {
        config = config.with_session_resume_grace_cap(Some(Duration::from_secs(secs)));
    }

    let wire_codec = match resolve_wire_codec(
        args.wire_codec,
        worker_cfg.as_ref().and_then(|w| w.wire_codec.clone()),
    ) {
        Ok(codec) => codec,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    config = config.with_wire_codec(wire_codec);

    let config_write_path = resolve_config_write_path(config_file.as_ref());
    let mut initial_limits = config_write_path
        .as_deref()
        .and_then(|p| load_worker_limits_from_toml(p).ok())
        .unwrap_or_default();
    let no_gpu_tasks_override = resolve_bool(
        args.no_gpu_tasks.then_some(true),
        &["RUSTY_GRID_NO_GPU_TASKS"],
        None,
        initial_limits.no_gpu_tasks,
    );
    initial_limits.no_gpu_tasks = no_gpu_tasks_override;
    let _hold_ends_tasks = args.hold_ends_tasks;

    let control_socket_override = resolve_opt_string(
        args.control_socket.map(|p| p.display().to_string()),
        &["RUSTY_GRID_CONTROL_SOCKET", "OXIDESWARM_CONTROL_SOCKET"],
        None,
    )
    .map(PathBuf::from);
    let control_endpoint =
        ControlEndpoint::resolve(control_socket_override.as_deref(), config.name.as_deref());
    let persist_hook = config_write_path.clone().map(|path| {
        LimitPersistHook(std::sync::Arc::new(move |limits: &WorkerLimitsConfig| {
            persist_worker_limits_to_toml(&path, limits).map(|()| Some(path.clone()))
        }))
    });
    config = config
        .with_control_endpoint(control_endpoint)
        .with_initial_limits(initial_limits)
        .with_control_config_path(config_write_path)
        .with_limit_persist_hook(persist_hook);

    let mut client = WorkerClient::new(config);
    if let Err(e) = client.validate_isolation_config() {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }
    println!(
        "RustyGrid Worker {} initializing [Name: {}, Cores: {}, RAM: {}MB, GPU: {}, Simulated: {}]",
        client.worker_id(),
        client.capabilities().name,
        client.capabilities().cpu_cores,
        client.capabilities().ram_mb,
        client.capabilities().has_gpu,
        client.capabilities().is_simulated_gpu,
    );
    println!("Tags: {}", client.capabilities().tags.join(", "));
    for notice in client.gpu_notices() {
        match notice.level {
            NoticeLevel::Info => println!("GPU: {}", notice.message),
            NoticeLevel::Warn => eprintln!("Warning: {}", notice.message),
        }
    }
    if no_battery {
        eprintln!(
            "Warning: host battery and thermal protection is disabled (RUSTY_GRID_NO_BATTERY); this worker will accept tasks regardless of battery level or thermal state"
        );
    }
    match peer_outcome {
        PeerSecretOutcome::Authenticated => println!("Cluster authentication: enabled"),
        PeerSecretOutcome::OpenLocalOnly => println!(
            "Cluster authentication: off (the master is on this machine; a master that requires a secret will refuse this worker)"
        ),
        PeerSecretOutcome::OpenInsecure => println!(
            "Cluster authentication: OFF (--insecure; the master is not authenticated)"
        ),
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_worker_shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let outcome = client.run(shutdown_rx).await;
    // `run` has cancelled the running tasks, whose process trees die with them; this is the net
    // under that, for a task that did not stop in time.
    rusty_grid_worker::terminate_all_task_process_trees();
    match outcome {
        Ok(()) => {
            println!("RustyGrid Worker finished cleanly.");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("RustyGrid Worker exited with error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Environment variable that selects the worker's isolation level (`--isolation`).
pub(super) const ISOLATION_ENV: &str = "RUSTY_GRID_ISOLATION";

/// `--isolation` > environment > config file `[worker].isolation` > `off`.
pub(super) fn resolve_isolation(
    cli: Option<String>,
    env_names: &[&str],
    file: Option<String>,
) -> Result<IsolationLevel, String> {
    match resolve_opt_string(cli, env_names, file) {
        Some(text) => text.parse(),
        None => Ok(IsolationLevel::Off),
    }
}

/// `--isolation-read-dir` > `RUSTY_GRID_ISOLATION_READ_DIRS` (`split_paths`) > `[worker].isolation_read_dirs`.
pub(super) fn resolve_isolation_read_dirs(
    cli_dirs: &[PathBuf],
    env_val: Option<&std::ffi::OsStr>,
    cfg_dirs: Option<&[String]>,
) -> Vec<PathBuf> {
    fn push_unique(out: &mut Vec<PathBuf>, p: PathBuf) {
        if !out.contains(&p) {
            out.push(p);
        }
    }

    let mut out = Vec::new();
    if !cli_dirs.is_empty() {
        for dir in cli_dirs {
            push_unique(&mut out, dir.clone());
        }
        return out;
    }
    if let Some(raw_env) = env_val.filter(|v| !v.is_empty()) {
        for dir in std::env::split_paths(raw_env) {
            let trimmed = dir.to_string_lossy();
            if !dir.as_os_str().is_empty() && !trimmed.trim().is_empty() {
                push_unique(&mut out, dir);
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    if let Some(cfg) = cfg_dirs {
        for raw in cfg {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                push_unique(&mut out, PathBuf::from(trimmed));
            }
        }
    }
    out
}

fn resolve_control_endpoint_for_cli(
    flag_socket: Option<&std::path::Path>,
    parent_socket: Option<&std::path::Path>,
    flag_name: Option<&str>,
    parent_name: Option<&str>,
) -> Result<ControlEndpoint, String> {
    let env_socket = resolve_opt_string(
        None,
        &["RUSTY_GRID_CONTROL_SOCKET", "OXIDESWARM_CONTROL_SOCKET"],
        None,
    )
    .map(PathBuf::from);
    let socket = flag_socket.or(parent_socket).or(env_socket.as_deref());
    let name = flag_name
        .or(parent_name)
        .map(str::to_string)
        .or_else(|| resolve_opt_string(None, &["RUSTY_GRID_WORKER_NAME", "RUSTY_GRID_NAME"], None));
    ControlEndpoint::resolve(socket, name.as_deref()).ok_or_else(|| {
        "cannot determine default worker control socket (set --control-socket or HOME)".to_string()
    })
}

pub(crate) async fn run_worker_control_action(
    action: WorkerControlSubcommand,
    parent_socket: Option<&std::path::Path>,
    parent_name: Option<&str>,
) -> ExitCode {
    let (socket_flag, name_flag, json, req) = match action {
        WorkerControlSubcommand::Pause(p) => {
            let until_epoch_ms = match p.until.as_deref() {
                Some(spec) => match parse_pause_until(spec, chrono::Utc::now()) {
                    Ok(ms) => Some(ms),
                    Err(e) => {
                        eprintln!("worker pause: {e}");
                        return ExitCode::FAILURE;
                    }
                },
                None => None,
            };
            (
                p.control_socket,
                p.name,
                p.json,
                ControlRequest::Pause {
                    until_epoch_ms,
                    reason: p.reason,
                },
            )
        }
        WorkerControlSubcommand::Resume(t) => {
            (t.control_socket, t.name, t.json, ControlRequest::Resume)
        }
        WorkerControlSubcommand::Status(t) => {
            (t.control_socket, t.name, t.json, ControlRequest::Status)
        }
        WorkerControlSubcommand::Limits(l) => match l.op {
            WorkerLimitsSubcommand::Get(t) => {
                (t.control_socket, t.name, t.json, ControlRequest::LimitsGet)
            }
            WorkerLimitsSubcommand::Set {
                key,
                value,
                control_socket,
                name,
                json,
            } => (
                control_socket,
                name,
                json,
                ControlRequest::LimitsSet { key, value },
            ),
        },
    };

    let endpoint = match resolve_control_endpoint_for_cli(
        socket_flag.as_deref(),
        parent_socket,
        name_flag.as_deref(),
        parent_name,
    ) {
        Ok(ep) => ep,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let resp = match send_control_request(&endpoint, &req).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("worker control ({}): {e}", endpoint.display_target());
            return ExitCode::FAILURE;
        }
    };

    if json {
        match serde_json::to_string_pretty(&resp) {
            Ok(text) => println!("{text}"),
            Err(e) => {
                eprintln!("failed to encode response: {e}");
                return ExitCode::FAILURE;
            }
        }
        return if matches!(resp, ControlResponse::Error { .. }) {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        };
    }

    match resp {
        ControlResponse::Status { snapshot } => {
            println!(
                "Worker {} ({}): state={}{}",
                snapshot.worker_name,
                snapshot.worker_id,
                snapshot.state.as_str(),
                snapshot
                    .reason
                    .as_deref()
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default()
            );
            println!(
                "  active_tasks={}, no_gpu_tasks={}",
                snapshot.active_tasks, snapshot.no_gpu_tasks
            );
            if let Some(ms) = snapshot.paused_until_epoch_ms {
                println!("  paused_until_epoch_ms={ms}");
            }
            ExitCode::SUCCESS
        }
        ControlResponse::Ok { message, snapshot } => {
            println!(
                "{message} (state={}, active_tasks={})",
                snapshot.state.as_str(),
                snapshot.active_tasks
            );
            ExitCode::SUCCESS
        }
        ControlResponse::Limits {
            limits,
            config_path,
        } => {
            println!("Worker contributor limits:");
            println!("  local_max_cpu_pct = {:?}", limits.local_max_cpu_pct);
            println!("  hours = {:?}", limits.hours);
            println!(
                "  max_task_hours_per_day = {:?}",
                limits.max_task_hours_per_day
            );
            println!("  no_tasks_on_battery = {:?}", limits.no_tasks_on_battery);
            println!("  no_tasks_on_metered = {:?}", limits.no_tasks_on_metered);
            println!("  no_gpu_tasks = {}", limits.no_gpu_tasks);
            if let Some(p) = config_path {
                println!("  config = {p}");
            }
            ExitCode::SUCCESS
        }
        ControlResponse::Error { message } => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

pub(crate) fn resolve_config_write_path(config_file: Option<&ConfigFile>) -> Option<PathBuf> {
    resolve_config_write_path_with(
        config_file,
        std::env::var_os("RUSTY_GRID_CONFIG").as_deref(),
        crate::config::default_oxideswarm_dir(),
    )
}

fn resolve_config_write_path_with(
    config_file: Option<&ConfigFile>,
    env_config: Option<&std::ffi::OsStr>,
    oxideswarm_dir: Option<PathBuf>,
) -> Option<PathBuf> {
    if let Some(p) = config_file.and_then(|c| c.source_path.0.as_ref()) {
        return Some(p.clone());
    }
    if let Some(env_path) = env_config.filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(env_path));
    }
    oxideswarm_dir.map(|dir| dir.join("rusty-grid.toml"))
}

pub(crate) fn load_worker_limits_from_toml(
    path: &std::path::Path,
) -> Result<WorkerLimitsConfig, String> {
    let raw = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WorkerLimitsConfig::default());
        }
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let doc: toml_edit::DocumentMut = raw
        .parse()
        .map_err(|e| format!("parse {}: {e}", path.display()))?;
    let Some(worker_tbl) = doc.get("worker").and_then(|item| item.as_table_like()) else {
        return Ok(WorkerLimitsConfig::default());
    };
    Ok(WorkerLimitsConfig {
        local_max_cpu_pct: worker_tbl
            .get("local_max_cpu_pct")
            .and_then(|v| v.as_integer())
            .and_then(|n| u8::try_from(n).ok())
            .filter(|n| (1..=100).contains(n)),
        hours: worker_tbl
            .get("hours")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        max_task_hours_per_day: worker_tbl
            .get("max_task_hours_per_day")
            .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
            .filter(|f| f.is_finite() && *f > 0.0),
        no_tasks_on_battery: worker_tbl
            .get("no_tasks_on_battery")
            .and_then(|v| v.as_bool()),
        no_tasks_on_metered: worker_tbl
            .get("no_tasks_on_metered")
            .and_then(|v| v.as_bool()),
        no_gpu_tasks: worker_tbl
            .get("no_gpu_tasks")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    })
}

pub(crate) fn persist_worker_limits_to_toml(
    path: &std::path::Path,
    limits: &WorkerLimitsConfig,
) -> Result<(), String> {
    let existing = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let mut doc: toml_edit::DocumentMut = if existing.trim().is_empty() {
        toml_edit::DocumentMut::new()
    } else {
        existing
            .parse()
            .map_err(|e| format!("parse {}: {e}", path.display()))?
    };

    if !doc.as_table().contains_key("worker") {
        doc["worker"] = toml_edit::Item::Table(toml_edit::Table::new());
    }
    let worker_tbl = doc["worker"]
        .as_table_mut()
        .ok_or_else(|| format!("{} has non-table [worker] entry", path.display()))?;

    match limits.local_max_cpu_pct {
        Some(pct) => {
            worker_tbl["local_max_cpu_pct"] = toml_edit::value(i64::from(pct));
        }
        None => {
            worker_tbl.remove("local_max_cpu_pct");
        }
    }
    match &limits.hours {
        Some(h) => {
            worker_tbl["hours"] = toml_edit::value(h.as_str());
        }
        None => {
            worker_tbl.remove("hours");
        }
    }
    match limits.max_task_hours_per_day {
        Some(hrs) => {
            worker_tbl["max_task_hours_per_day"] = toml_edit::value(hrs);
        }
        None => {
            worker_tbl.remove("max_task_hours_per_day");
        }
    }
    match limits.no_tasks_on_battery {
        Some(b) => {
            worker_tbl["no_tasks_on_battery"] = toml_edit::value(b);
        }
        None => {
            worker_tbl.remove("no_tasks_on_battery");
        }
    }
    match limits.no_tasks_on_metered {
        Some(b) => {
            worker_tbl["no_tasks_on_metered"] = toml_edit::value(b);
        }
        None => {
            worker_tbl.remove("no_tasks_on_metered");
        }
    }
    if limits.no_gpu_tasks {
        worker_tbl["no_gpu_tasks"] = toml_edit::value(true);
    } else {
        worker_tbl.remove("no_gpu_tasks");
    }

    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create dir {}: {e}", parent.display()))?;
    }
    let tmp = path.with_extension(format!("toml.tmp.{}", uuid::Uuid::new_v4()));
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|e| format!("create temp config {}: {e}", tmp.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
        f.write_all(doc.to_string().as_bytes())
            .map_err(|e| format!("write temp config {}: {e}", tmp.display()))?;
        f.sync_all()
            .map_err(|e| format!("sync temp config {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("replace config {}: {e}", path.display())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_app::{Cli, Commands};
    use crate::config::WorkerFileConfig;
    use crate::secret_cmd::SecretOpts;
    use clap::Parser;

    fn generated_p2p_ticket() -> String {
        let secret_key = rusty_grid_core::transport::iroh::SecretKey::generate();
        let addr = rusty_grid_core::transport::iroh::EndpointAddr::from(secret_key.public());
        rusty_grid_core::transport::serialize_p2p_ticket(&addr)
            .expect("serialize generated EndpointAddr")
    }

    #[test]
    fn worker_master_refuses_p2p_ticket_and_points_to_p2p_ticket_flag() {
        let ticket = generated_p2p_ticket();

        for (label, outcome) in [
            (
                "--master",
                resolve_worker_target_from(Some(&ticket), None, None, None, None),
            ),
            (
                "RUSTY_GRID_MASTER_ADDR",
                resolve_worker_target_from(
                    None,
                    None,
                    Some(("RUSTY_GRID_MASTER_ADDR", &ticket)),
                    None,
                    None,
                ),
            ),
            (
                "[worker].master",
                resolve_worker_target_from(
                    None,
                    None,
                    None,
                    None,
                    Some(&WorkerFileConfig {
                        master: Some(ticket.clone()),
                        ..Default::default()
                    }),
                ),
            ),
        ] {
            let err = outcome.unwrap_err();
            assert!(
                err.contains("this looks like a P2P ticket; use --p2p-ticket"),
                "{label}: unexpected error text: {err}"
            );
            assert!(
                !err.contains(&ticket),
                "{label}: error must not echo the raw ticket: {err}"
            );
        }

        let (master_addr, p2p_ticket) =
            resolve_worker_target_from(None, Some(&ticket), None, None, None)
                .expect("--p2p-ticket alone succeeds");
        assert_eq!(master_addr, "");
        assert_eq!(p2p_ticket.as_deref(), Some(ticket.as_str()));
    }

    #[test]
    fn bug40_ticket_only_has_no_tcp_fallback_while_explicit_master_keeps_it() {
        let ticket = generated_p2p_ticket();

        // 1. Ticket alone via CLI, env, or config: no default 127.0.0.1:8088 TCP fallback.
        for (label, resolved) in [
            (
                "--p2p-ticket",
                resolve_worker_target_from(None, Some(&ticket), None, None, None),
            ),
            (
                "RUSTY_GRID_P2P_TICKET",
                resolve_worker_target_from(None, None, None, Some(&ticket), None),
            ),
            (
                "[worker].p2p_ticket",
                resolve_worker_target_from(
                    None,
                    None,
                    None,
                    None,
                    Some(&WorkerFileConfig {
                        p2p_ticket: Some(ticket.clone()),
                        ..Default::default()
                    }),
                ),
            ),
        ] {
            let (master_addr, p2p_ticket) = resolved.unwrap_or_else(|e| panic!("{label}: {e}"));
            assert_eq!(
                master_addr, "",
                "{label}: ticket-only worker must not fall back to TCP {DEFAULT_MASTER_ADDR}"
            );
            assert_eq!(p2p_ticket.as_deref(), Some(ticket.as_str()));
        }

        // 2. Explicit master + ticket (Android / explicit fallback path): keeps normalized TCP address.
        let (cli_addr, cli_tkt) =
            resolve_worker_target_from(Some("192.168.1.10"), Some(&ticket), None, None, None)
                .expect("explicit --master + --p2p-ticket");
        assert_eq!(cli_addr, "192.168.1.10:8088");
        assert_eq!(cli_tkt.as_deref(), Some(ticket.as_str()));

        let (env_addr, env_tkt) = resolve_worker_target_from(
            None,
            None,
            Some(("RUSTY_GRID_MASTER", "10.0.0.5:9088")),
            Some(&ticket),
            None,
        )
        .expect("explicit env master + env ticket");
        assert_eq!(env_addr, "10.0.0.5:9088");
        assert_eq!(env_tkt.as_deref(), Some(ticket.as_str()));

        let (cfg_addr, cfg_tkt) = resolve_worker_target_from(
            None,
            None,
            None,
            None,
            Some(&WorkerFileConfig {
                master: Some("198.51.100.9:8088".into()),
                p2p_ticket: Some(ticket.clone()),
                ..Default::default()
            }),
        )
        .expect("explicit [worker].master + [worker].p2p_ticket");
        assert_eq!(cfg_addr, "198.51.100.9:8088");
        assert_eq!(cfg_tkt.as_deref(), Some(ticket.as_str()));

        // 3. Neither master nor ticket: keeps DEFAULT_MASTER_ADDR.
        let (default_addr, no_tkt) =
            resolve_worker_target_from(None, None, None, None, None).expect("default target");
        assert_eq!(default_addr, DEFAULT_MASTER_ADDR);
        assert_eq!(no_tkt, None);
    }

    #[tokio::test]
    async fn run_worker_exits_failure_when_master_is_a_p2p_ticket() {
        let ticket = generated_p2p_ticket();
        let cli = Cli::try_parse_from([
            "oxideswarm",
            "worker",
            "--master",
            &ticket,
            "--no-gpu",
            "--no-portal",
            "--insecure",
        ])
        .expect("parse worker CLI");
        let Commands::Worker(args) = cli.command else {
            panic!("expected Worker command");
        };
        let code = tokio::time::timeout(
            Duration::from_secs(5),
            run_worker(args, None, SecretOpts::default()),
        )
        .await
        .expect("worker --master <ticket> must fail fast without attempting TCP");
        assert_eq!(code, ExitCode::FAILURE);
    }

    #[test]
    fn worker_control_subcommands_parse_and_toml_persistence_preserves_comments() {
        for argv in [
            vec![
                "oxideswarm",
                "worker",
                "pause",
                "--until",
                "2h",
                "--reason",
                "call",
            ],
            vec!["oxideswarm", "worker", "resume", "--name", "box1"],
            vec!["oxideswarm", "worker", "status", "--json"],
            vec!["oxideswarm", "worker", "limits", "get"],
            vec![
                "oxideswarm",
                "worker",
                "limits",
                "set",
                "no_gpu_tasks",
                "true",
            ],
        ] {
            let cli = Cli::try_parse_from(argv).expect("parse worker control CLI");
            assert!(matches!(cli.command, Commands::Worker(_)));
        }

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("rusty-grid.toml");
        std::fs::write(
            &cfg_path,
            "# contributor config\n[worker]\nname = \"mac-box\"\n",
        )
        .unwrap();

        let limits = WorkerLimitsConfig {
            local_max_cpu_pct: Some(40),
            hours: Some("18:00-08:00".into()),
            max_task_hours_per_day: Some(4.5),
            no_tasks_on_battery: Some(true),
            no_tasks_on_metered: Some(true),
            no_gpu_tasks: true,
        };
        persist_worker_limits_to_toml(&cfg_path, &limits).unwrap();
        let raw = std::fs::read_to_string(&cfg_path).unwrap();
        assert!(raw.contains("# contributor config"));
        assert!(raw.contains("name = \"mac-box\""));
        let loaded = load_worker_limits_from_toml(&cfg_path).unwrap();
        assert_eq!(loaded, limits);
    }

    #[test]
    fn resolve_config_write_path_honours_oxideswarm_home() {
        use std::ffi::OsStr;

        let mut cfg = ConfigFile::default();
        cfg.source_path.0 = Some(PathBuf::from("/explicit/rusty-grid.toml"));

        assert_eq!(
            resolve_config_write_path_with(
                Some(&cfg),
                Some(OsStr::new("/env/rusty-grid.toml")),
                Some(PathBuf::from("/custom/ox")),
            ),
            Some(PathBuf::from("/explicit/rusty-grid.toml"))
        );
        assert_eq!(
            resolve_config_write_path_with(
                None,
                Some(OsStr::new("/env/rusty-grid.toml")),
                Some(PathBuf::from("/custom/ox")),
            ),
            Some(PathBuf::from("/env/rusty-grid.toml"))
        );
        assert_eq!(
            resolve_config_write_path_with(
                None,
                Some(OsStr::new("")),
                Some(PathBuf::from("/custom/ox")),
            ),
            Some(PathBuf::from("/custom/ox/rusty-grid.toml"))
        );
    }
}
