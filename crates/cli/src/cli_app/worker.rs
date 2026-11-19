//! `oxideswarm worker` target/isolation resolution and execution loop.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use rusty_grid_core::placement::{validate_user_tag, IsolationLevel};
use rusty_grid_worker::{
    default_control_file_path, is_pause_file_present, remove_pause_file, validate_daily_task_hours,
    validate_max_host_cpu_pct, write_pause_file, AllowHoursSchedule, ContributorControlsConfig,
    NoticeLevel, ProcessPriority, WorkerClient, WorkerConfig,
};

use crate::config::{
    resolve_bool, resolve_opt_field, resolve_opt_string, resolve_opt_u64, resolve_opt_usize,
    resolve_u64, resolve_usize, ConfigFile, SecretSection, WorkerFileConfig,
};
use crate::connect::{normalize_master_addr, AUTH_EXIT_CODE, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::{
    announce_peer_secret, peer_insecure, PeerRole, PeerSecretOutcome, SecretOpts,
};

use super::{
    resolve_no_battery, resolve_p2p_minimal, resolve_relay_url, resolve_wire_codec,
    wait_for_worker_shutdown_signal, WorkerAction, WorkerArgs, MIB,
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

    let (source, master_raw) = if let Some(ref m) = cli_master {
        ("--master", m.clone())
    } else if let Some((var, ref m)) = env_master {
        (var, m.clone())
    } else if let Some(ref m) = cfg_master {
        ("[worker].master", m.clone())
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

    Ok((master_addr, p2p_ticket))
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
    let worker_cfg = config_file.as_ref().and_then(|c| c.worker.clone());
    if let Some(action) = args.action {
        return run_worker_control_action(
            action,
            args.name,
            args.control_file,
            worker_cfg.as_ref(),
        );
    }
    let rendezvous_url =
        match crate::connect::resolve_and_validate_rendezvous_url(args.rendezvous.as_deref()) {
            Ok(url) => url,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
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
    announce_peer_secret(peer_outcome, PeerRole::Worker, &master_addr);

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
    if let Some(ref n) = name {
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
        args.wire_codec.clone(),
        worker_cfg.as_ref().and_then(|w| w.wire_codec.clone()),
    ) {
        Ok(codec) => codec,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    config = config.with_wire_codec(wire_codec);

    let controls_cfg = match resolve_contributor_controls_from(
        ContributorControlInputs {
            max_host_cpu_pct: args.max_host_cpu_pct,
            allow_hours: args.allow_hours.as_deref(),
            daily_task_hours: args.daily_task_hours,
            pause_on_battery: args.pause_on_battery,
            pause_on_metered: args.pause_on_metered,
            control_file: args.control_file.as_deref(),
        },
        worker_cfg.as_ref(),
        name.as_deref(),
        |var| std::env::var(var).ok(),
    ) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    config = config.with_contributor_controls(controls_cfg);

    let mut client = WorkerClient::new(config);
    if client.controls().config().pause_on_battery || client.controls().config().pause_on_metered {
        let ctrl = Arc::clone(client.controls());
        let _ = tokio::task::spawn_blocking(move || ctrl.refresh_os_probes_blocking()).await;
    }
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
    if let Some(line) = format_controls_summary(client.controls().config()) {
        println!("{line}");
    }
    for diag in client.controls().startup_diagnostics() {
        eprintln!("Warning: {diag}");
        tracing::warn!("{diag}");
    }
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

fn parse_env_bool(var: &str, raw: &str) -> Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("{var}: invalid boolean value {other:?}")),
    }
}

fn resolve_control_file_path_from(
    cli_path: Option<PathBuf>,
    worker_name: Option<&str>,
    worker_cfg: Option<&WorkerFileConfig>,
    get_env: impl Fn(&str) -> Option<String>,
) -> Option<PathBuf> {
    if let Some(p) = cli_path {
        return Some(p);
    }
    if let Some(env_p) = get_env("RUSTY_GRID_CONTROL_FILE") {
        let trimmed = env_p.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }
    if let Some(cfg_p) = worker_cfg.and_then(|w| w.control_file.as_deref()) {
        let trimmed = cfg_p.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }
    default_control_file_path(worker_name)
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ContributorControlInputs<'a> {
    pub(super) max_host_cpu_pct: Option<f32>,
    pub(super) allow_hours: Option<&'a str>,
    pub(super) daily_task_hours: Option<f64>,
    pub(super) pause_on_battery: Option<bool>,
    pub(super) pause_on_metered: Option<bool>,
    pub(super) control_file: Option<&'a std::path::Path>,
}

pub(super) fn resolve_contributor_controls_from(
    cli: ContributorControlInputs<'_>,
    worker_cfg: Option<&WorkerFileConfig>,
    resolved_name: Option<&str>,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<ContributorControlsConfig, String> {
    let max_host_cpu_pct = if let Some(v) = cli.max_host_cpu_pct {
        Some(validate_max_host_cpu_pct(v)?)
    } else if let Some(raw) =
        get_env("RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT").filter(|s| !s.trim().is_empty())
    {
        let parsed: f32 = raw.trim().parse().map_err(|_| {
            format!(
                "RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT: invalid float {:?}",
                raw.trim()
            )
        })?;
        Some(validate_max_host_cpu_pct(parsed)?)
    } else if let Some(v) = worker_cfg.and_then(|w| w.max_host_cpu_pct) {
        Some(validate_max_host_cpu_pct(v)?)
    } else {
        None
    };

    let allow_hours_spec = cli
        .allow_hours
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            get_env("RUSTY_GRID_ALLOW_HOURS")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            worker_cfg
                .and_then(|w| w.allow_hours.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });
    let allow_hours = match allow_hours_spec {
        Some(spec) => Some(AllowHoursSchedule::parse(&spec)?),
        None => None,
    };

    let daily_task_hours = if let Some(v) = cli.daily_task_hours {
        Some(validate_daily_task_hours(v)?)
    } else if let Some(raw) =
        get_env("RUSTY_GRID_DAILY_TASK_HOURS").filter(|s| !s.trim().is_empty())
    {
        let parsed: f64 = raw.trim().parse().map_err(|_| {
            format!(
                "RUSTY_GRID_DAILY_TASK_HOURS: invalid number {:?}",
                raw.trim()
            )
        })?;
        Some(validate_daily_task_hours(parsed)?)
    } else if let Some(v) = worker_cfg.and_then(|w| w.daily_task_hours) {
        Some(validate_daily_task_hours(v)?)
    } else {
        None
    };

    let pause_on_battery = if let Some(v) = cli.pause_on_battery {
        v
    } else if let Some(raw) =
        get_env("RUSTY_GRID_PAUSE_ON_BATTERY").filter(|s| !s.trim().is_empty())
    {
        parse_env_bool("RUSTY_GRID_PAUSE_ON_BATTERY", &raw)?
    } else {
        worker_cfg.and_then(|w| w.pause_on_battery).unwrap_or(false)
    };

    let pause_on_metered = if let Some(v) = cli.pause_on_metered {
        v
    } else if let Some(raw) =
        get_env("RUSTY_GRID_PAUSE_ON_METERED").filter(|s| !s.trim().is_empty())
    {
        parse_env_bool("RUSTY_GRID_PAUSE_ON_METERED", &raw)?
    } else {
        worker_cfg.and_then(|w| w.pause_on_metered).unwrap_or(false)
    };

    let control_file = resolve_control_file_path_from(
        cli.control_file.map(PathBuf::from),
        resolved_name,
        worker_cfg,
        &get_env,
    );

    let cfg = ContributorControlsConfig {
        max_host_cpu_pct,
        allow_hours,
        daily_task_hours,
        pause_on_battery,
        pause_on_metered,
        control_file,
    };
    cfg.validate()?;
    Ok(cfg)
}

fn format_controls_summary(cfg: &ContributorControlsConfig) -> Option<String> {
    if !cfg.is_any_limit_configured() && cfg.control_file.is_none() {
        return None;
    }
    let mut parts = Vec::new();
    if let Some(pct) = cfg.max_host_cpu_pct {
        parts.push(format!("max_host_cpu={pct:.1}%"));
    }
    if let Some(ref sched) = cfg.allow_hours {
        parts.push(format!("allow_hours=\"{}\"", sched.raw()));
    }
    if let Some(hours) = cfg.daily_task_hours {
        parts.push(format!("daily_task_hours={hours:.2}h"));
    }
    if cfg.pause_on_battery {
        parts.push("pause_on_battery=on".to_string());
    }
    if cfg.pause_on_metered {
        parts.push("pause_on_metered=on".to_string());
    }
    if let Some(ref path) = cfg.control_file {
        parts.push(format!("control_file={}", path.display()));
    }
    Some(format!("Contributor controls: {}", parts.join(", ")))
}

fn warn_if_no_watcher(path: &std::path::Path) -> Option<u32> {
    match rusty_grid_worker::check_watcher_status(path) {
        Ok(rusty_grid_worker::WatcherStatus::Watching { pid }) => Some(pid),
        Ok(rusty_grid_worker::WatcherStatus::Stale { pid }) => {
            eprintln!(
                "Warning: no live worker is watching {} (stale watcher PID {pid}). If the worker runs with --name or as a service/daemon under another account, pass --name or --control-file pointing to its control file.",
                path.display()
            );
            None
        }
        Ok(rusty_grid_worker::WatcherStatus::NotWatching) => {
            eprintln!(
                "Warning: no running worker is watching {}. If the worker runs with --name or as a service/daemon under another account, pass --name or --control-file pointing to its control file.",
                path.display()
            );
            None
        }
        Err(e) => {
            eprintln!(
                "Warning: could not inspect watcher file for {}: {e}",
                path.display()
            );
            None
        }
    }
}

fn run_worker_control_action(
    action: WorkerAction,
    top_name: Option<String>,
    top_control_file: Option<PathBuf>,
    worker_cfg: Option<&WorkerFileConfig>,
) -> ExitCode {
    let (ctrl_name, ctrl_file, op) = match action {
        WorkerAction::Pause(c) => (c.name, c.control_file, "pause"),
        WorkerAction::Resume(c) => (c.name, c.control_file, "resume"),
        WorkerAction::Status(c) => (c.name, c.control_file, "status"),
    };
    let effective_name = resolve_opt_string(
        ctrl_name.or(top_name),
        &["RUSTY_GRID_WORKER_NAME", "RUSTY_GRID_NAME"],
        worker_cfg.and_then(|w| w.name.clone()),
    );
    let Some(path) = resolve_control_file_path_from(
        ctrl_file.or(top_control_file),
        effective_name.as_deref(),
        worker_cfg,
        |var| std::env::var(var).ok(),
    ) else {
        eprintln!(
            "error: cannot determine pause control file path (no home directory; pass --control-file PATH or set RUSTY_GRID_CONTROL_FILE)"
        );
        return ExitCode::FAILURE;
    };

    match op {
        "pause" => match write_pause_file(&path) {
            Ok(()) => {
                println!("Worker paused (control file: {}).", path.display());
                warn_if_no_watcher(&path);
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!(
                    "error: failed to write pause control file {}: {e}",
                    path.display()
                );
                ExitCode::FAILURE
            }
        },
        "resume" => match remove_pause_file(&path) {
            Ok(true) => {
                println!("Worker resumed (removed control file: {}).", path.display());
                ExitCode::SUCCESS
            }
            Ok(false) => {
                println!(
                    "Worker is already active (no control file at {}).",
                    path.display()
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!(
                    "error: failed to remove pause control file {}: {e}",
                    path.display()
                );
                ExitCode::FAILURE
            }
        },
        _ => match is_pause_file_present(&path) {
            Ok(true) => {
                if let Some(pid) = warn_if_no_watcher(&path) {
                    println!(
                        "Worker status: paused (control file: {}, watched by worker PID {pid})",
                        path.display()
                    );
                } else {
                    println!("Worker status: paused (control file: {})", path.display());
                }
                ExitCode::SUCCESS
            }
            Ok(false) => {
                if let Some(pid) = warn_if_no_watcher(&path) {
                    println!(
                        "Worker status: active (control file: {}, watched by worker PID {pid})",
                        path.display()
                    );
                } else {
                    println!("Worker status: active (control file: {})", path.display());
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!(
                    "error: failed to inspect pause control file {}: {e}",
                    path.display()
                );
                ExitCode::FAILURE
            }
        },
    }
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
                .expect("--p2p-ticket with default master succeeds");
        assert_eq!(master_addr, DEFAULT_MASTER_ADDR);
        assert_eq!(p2p_ticket.as_deref(), Some(ticket.as_str()));
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
    fn resolve_contributor_controls_honors_cli_env_config_precedence_and_rejects_invalid() {
        let cfg = WorkerFileConfig {
            max_host_cpu_pct: Some(50.0),
            allow_hours: Some("Mon-Fri 09:00-17:00".to_string()),
            daily_task_hours: Some(2.0),
            pause_on_battery: Some(false),
            pause_on_metered: Some(true),
            control_file: Some("/cfg/pause".to_string()),
            ..Default::default()
        };

        // Env overrides [worker] config.
        let env_map = |k: &str| match k {
            "RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT" => Some("65".to_string()),
            "RUSTY_GRID_ALLOW_HOURS" => Some("Daily 00:00-24:00".to_string()),
            "RUSTY_GRID_DAILY_TASK_HOURS" => Some("4.5".to_string()),
            "RUSTY_GRID_PAUSE_ON_BATTERY" => Some("1".to_string()),
            "RUSTY_GRID_PAUSE_ON_METERED" => Some("0".to_string()),
            "RUSTY_GRID_CONTROL_FILE" => Some("/env/pause".to_string()),
            _ => None,
        };
        let resolved_env = resolve_contributor_controls_from(
            ContributorControlInputs::default(),
            Some(&cfg),
            Some("w1"),
            env_map,
        )
        .unwrap();
        assert_eq!(resolved_env.max_host_cpu_pct, Some(65.0));
        assert_eq!(
            resolved_env.allow_hours.as_ref().unwrap().raw(),
            "Daily 00:00-24:00"
        );
        assert_eq!(resolved_env.daily_task_hours, Some(4.5));
        assert!(resolved_env.pause_on_battery);
        assert!(!resolved_env.pause_on_metered);
        assert_eq!(resolved_env.control_file, Some(PathBuf::from("/env/pause")));

        // CLI overrides env and config.
        let cli_ctrl_path = PathBuf::from("/cli/pause");
        let resolved_cli = resolve_contributor_controls_from(
            ContributorControlInputs {
                max_host_cpu_pct: Some(80.0),
                allow_hours: Some("Sat-Sun 10:00-18:00"),
                daily_task_hours: Some(1.5),
                pause_on_battery: Some(true),
                pause_on_metered: Some(true),
                control_file: Some(&cli_ctrl_path),
            },
            Some(&cfg),
            Some("w1"),
            env_map,
        )
        .unwrap();
        assert_eq!(resolved_cli.max_host_cpu_pct, Some(80.0));
        assert_eq!(
            resolved_cli.allow_hours.as_ref().unwrap().raw(),
            "Sat-Sun 10:00-18:00"
        );
        assert_eq!(resolved_cli.daily_task_hours, Some(1.5));
        assert!(resolved_cli.pause_on_battery);
        assert!(resolved_cli.pause_on_metered);
        assert_eq!(resolved_cli.control_file, Some(cli_ctrl_path));

        // Explicit `--pause-on-battery=false` / `--pause-on-metered=false` override env & config `true`.
        let parsed_false = Cli::try_parse_from([
            "oxideswarm",
            "worker",
            "--pause-on-battery=false",
            "--pause-on-metered=false",
        ])
        .unwrap();
        let Commands::Worker(false_args) = parsed_false.command else {
            panic!("expected Worker");
        };
        assert_eq!(false_args.pause_on_battery, Some(false));
        assert_eq!(false_args.pause_on_metered, Some(false));
        let overridden_false = resolve_contributor_controls_from(
            ContributorControlInputs {
                pause_on_battery: false_args.pause_on_battery,
                pause_on_metered: false_args.pause_on_metered,
                ..Default::default()
            },
            Some(&cfg),
            Some("w1"),
            |k| match k {
                "RUSTY_GRID_PAUSE_ON_BATTERY" | "RUSTY_GRID_PAUSE_ON_METERED" => {
                    Some("true".to_string())
                }
                _ => None,
            },
        )
        .unwrap();
        assert!(!overridden_false.pause_on_battery);
        assert!(!overridden_false.pause_on_metered);

        // Bare `--pause-on-battery` still sets `Some(true)` and leaves subcommand positional args intact.
        let parsed_true =
            Cli::try_parse_from(["oxideswarm", "worker", "--pause-on-battery"]).unwrap();
        let Commands::Worker(true_args) = parsed_true.command else {
            panic!("expected Worker");
        };
        assert_eq!(true_args.pause_on_battery, Some(true));
        assert_eq!(true_args.pause_on_metered, None);

        // Invalid values fail loudly.
        assert!(resolve_contributor_controls_from(
            ContributorControlInputs::default(),
            None,
            None,
            |k| (k == "RUSTY_GRID_WORKER_MAX_HOST_CPU_PCT").then(|| "bad".to_string()),
        )
        .is_err());
        assert!(resolve_contributor_controls_from(
            ContributorControlInputs::default(),
            None,
            None,
            |k| (k == "RUSTY_GRID_DAILY_TASK_HOURS").then(|| "0".to_string()),
        )
        .is_err());
    }

    #[tokio::test]
    async fn worker_pause_resume_status_subcommands_toggle_control_file() {
        let dir = tempfile::tempdir().unwrap();
        let ctrl_path = dir.path().join("custom_pause");
        let ctrl_str = ctrl_path.display().to_string();

        for argv in [
            vec![
                "oxideswarm",
                "worker",
                "status",
                "--control-file",
                &ctrl_str,
            ],
            vec!["oxideswarm", "worker", "pause", "--control-file", &ctrl_str],
            vec![
                "oxideswarm",
                "worker",
                "status",
                "--control-file",
                &ctrl_str,
            ],
            vec![
                "oxideswarm",
                "worker",
                "resume",
                "--control-file",
                &ctrl_str,
            ],
            vec![
                "oxideswarm",
                "worker",
                "status",
                "--control-file",
                &ctrl_str,
            ],
        ] {
            let cli = Cli::try_parse_from(&argv).expect("parse worker control subcommand");
            let Commands::Worker(args) = cli.command else {
                panic!("expected Worker");
            };
            let code = run_worker(args, None, SecretOpts::default()).await;
            assert_eq!(code, ExitCode::SUCCESS);
        }
        assert!(!ctrl_path.exists());

        // Also check pause leaves file on disk until resume.
        let cli =
            Cli::try_parse_from(["oxideswarm", "worker", "pause", "--control-file", &ctrl_str])
                .unwrap();
        let Commands::Worker(args) = cli.command else {
            panic!("expected Worker");
        };
        assert_eq!(
            run_worker(args, None, SecretOpts::default()).await,
            ExitCode::SUCCESS
        );
        assert!(ctrl_path.exists());
        assert_eq!(warn_if_no_watcher(&ctrl_path), None);

        rusty_grid_worker::write_watch_file(&ctrl_path).unwrap();
        assert_eq!(warn_if_no_watcher(&ctrl_path), Some(std::process::id()));
        assert!(rusty_grid_worker::remove_watch_file_if_owned(&ctrl_path).unwrap());
    }
}
