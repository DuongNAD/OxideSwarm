//! `oxideswarm master` configuration helpers and server entrypoint.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use tracing::info;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_master::{MasterServer, ReaperConfig, SchedulerConfig, ServerConfig};

use crate::config::{
    resolve_bool, resolve_f32, resolve_opt_string, resolve_opt_u16, resolve_opt_usize,
    resolve_string, resolve_u32, resolve_u64, resolve_usize, ConfigFile, MasterFileConfig,
    SecretSection,
};
use crate::connect::DEFAULT_MASTER_ADDR;
use crate::secret_cmd::{announce_master_secret, decide_master_secret, SecretOpts};

use super::{
    resolve_auth_mode, resolve_p2p_minimal, resolve_relay_url, resolve_sourced_string,
    resolve_state_dir_value, resolve_wire_codec, wait_for_worker_shutdown_signal, MasterArgs, MIB,
};

/// Resolve `--max-active-mb` / `RUSTY_GRID_MAX_ACTIVE_MB` / `[master].max_active_mb`
/// and apply it to `ServerConfig`, refusing `0` because a 0-byte active cap rejects
/// every task.
pub(super) fn apply_max_active_mb(
    config: ServerConfig,
    flag: Option<u64>,
    cfg_val: Option<u64>,
) -> Result<ServerConfig, String> {
    let max_active_mb = resolve_u64(
        flag,
        &["RUSTY_GRID_MAX_ACTIVE_MB"],
        cfg_val,
        (rusty_grid_master::DEFAULT_MAX_ACTIVE_BYTES / (1024 * 1024)) as u64,
    );
    if max_active_mb == 0 {
        return Err("--max-active-mb / max_active_mb must be at least 1 MiB (got 0)".to_string());
    }
    let bytes = usize::try_from(max_active_mb.saturating_mul(1024 * 1024)).unwrap_or(usize::MAX);
    Ok(config.with_max_active_bytes(bytes))
}

/// Resolve `--max-queue-size` / `RUSTY_GRID_MAX_QUEUE_SIZE` / `[master].max_queue_size`
/// and apply it to `ServerConfig`, refusing `0` because a 0-task cap rejects every task.
pub(super) fn apply_max_queue_size(
    config: ServerConfig,
    flag: Option<usize>,
    cfg_val: Option<usize>,
) -> Result<ServerConfig, String> {
    let max_queue_size = resolve_opt_usize(flag, &["RUSTY_GRID_MAX_QUEUE_SIZE"], cfg_val);
    if max_queue_size == Some(0) {
        return Err("--max-queue-size / max_queue_size must be at least 1 (got 0)".to_string());
    }
    Ok(config.with_max_queue_size(max_queue_size))
}

/// Resolve `--scheduler-policy` / `RUSTY_GRID_SCHEDULER_POLICY` / `[master].scheduler_policy`,
/// returning an error prefixed with the source name on unknown policy names.
pub(super) fn resolve_scheduler_policy(
    flag: Option<String>,
    cfg_val: Option<String>,
) -> Result<rusty_grid_master::scheduler::SchedulingPolicy, String> {
    match resolve_sourced_string(
        "--scheduler-policy",
        flag,
        "RUSTY_GRID_SCHEDULER_POLICY",
        "scheduler_policy",
        cfg_val,
    ) {
        Some((source, raw)) => raw.parse().map_err(|e| format!("{source}: {e}")),
        None => Ok(rusty_grid_master::scheduler::SchedulingPolicy::default()),
    }
}

/// Resolve `--fail-unfulfillable-tasks` / `RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS` /
/// `[master].fail_unfulfillable_tasks` (default `false`).
pub(super) fn resolve_fail_unfulfillable_tasks(
    flag: Option<bool>,
    master_cfg: Option<&MasterFileConfig>,
) -> bool {
    resolve_bool(
        flag,
        &["RUSTY_GRID_FAIL_UNFULFILLABLE_TASKS"],
        master_cfg.and_then(|m| m.fail_unfulfillable_tasks),
        false,
    )
}

/// Resolve `speed_hold_factor` with precedence CLI `--speed-hold-factor`, then
/// `RUSTY_GRID_SPEED_HOLD_FACTOR`, then `[master].speed_hold_factor`, then
/// `SchedulerConfig::default().speed_hold_factor` (`1.5`), validating `0.0` (off) or finite `> 1.0`.
pub(super) fn resolve_speed_hold_factor(
    flag: Option<String>,
    cfg_val: Option<f64>,
) -> Result<f64, String> {
    if let Some(raw) = flag {
        let parsed: f64 = raw
            .trim()
            .parse()
            .map_err(|_| format!("--speed-hold-factor: invalid factor '{raw}'"))?;
        return SchedulerConfig::validate_speed_hold_factor(parsed)
            .map_err(|e| format!("--speed-hold-factor: {e}"));
    }
    if let Ok(raw) = std::env::var("RUSTY_GRID_SPEED_HOLD_FACTOR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let parsed: f64 = trimmed
                .parse()
                .map_err(|_| format!("RUSTY_GRID_SPEED_HOLD_FACTOR: invalid factor '{trimmed}'"))?;
            return SchedulerConfig::validate_speed_hold_factor(parsed)
                .map_err(|e| format!("RUSTY_GRID_SPEED_HOLD_FACTOR: {e}"));
        }
    }
    if let Some(cfg) = cfg_val {
        return SchedulerConfig::validate_speed_hold_factor(cfg)
            .map_err(|e| format!("speed_hold_factor: {e}"));
    }
    Ok(SchedulerConfig::default().speed_hold_factor)
}

fn parse_positive_usize(source: &str, raw: &str) -> Result<usize, String> {
    let trimmed = raw.trim();
    let parsed: usize = trimmed
        .parse()
        .map_err(|_| format!("{source}: invalid positive integer '{trimmed}'"))?;
    if parsed == 0 {
        return Err(format!("{source}: must be at least 1"));
    }
    Ok(parsed)
}

/// Resolve `max_batch_size` with precedence CLI `--max-batch-size`, then an explicit
/// `RUSTY_GRID_MAX_BATCH_SIZE` env value, then `[master].max_batch_size`, then
/// `DEFAULT_MASTER_MAX_BATCH_SIZE` (`1024`), rejecting `0` or non-integer values.
pub(super) fn resolve_max_batch_size_with_env(
    flag: Option<String>,
    env_val: Option<&str>,
    cfg_val: Option<usize>,
) -> Result<usize, String> {
    if let Some(raw) = flag {
        return parse_positive_usize("--max-batch-size", &raw);
    }
    if let Some(raw) = env_val {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return parse_positive_usize("RUSTY_GRID_MAX_BATCH_SIZE", trimmed);
        }
    }
    if let Some(cfg) = cfg_val {
        if cfg == 0 {
            return Err("max_batch_size: must be at least 1".to_string());
        }
        return Ok(cfg);
    }
    Ok(rusty_grid_master::DEFAULT_MASTER_MAX_BATCH_SIZE)
}

/// Resolve `max_batch_size` from CLI flag, `RUSTY_GRID_MAX_BATCH_SIZE`, or `[master].max_batch_size`.
fn resolve_max_batch_size(flag: Option<String>, cfg_val: Option<usize>) -> Result<usize, String> {
    let env_val = std::env::var("RUSTY_GRID_MAX_BATCH_SIZE").ok();
    resolve_max_batch_size_with_env(flag, env_val.as_deref(), cfg_val)
}

/// Build the `SchedulerConfig` for `oxideswarm master`, resolving `--scheduler-policy` /
/// `RUSTY_GRID_SCHEDULER_POLICY` / `[master].scheduler_policy`, `--speed-hold-factor` /
/// `RUSTY_GRID_SPEED_HOLD_FACTOR` / `[master].speed_hold_factor`, and `--max-batch-size` /
/// `RUSTY_GRID_MAX_BATCH_SIZE` / `[master].max_batch_size`, and carrying `preserve_gpu`,
/// `max_host_cpu`, and `fail_unfulfillable_tasks`.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_master_scheduler_config(
    flag: Option<String>,
    cfg_val: Option<String>,
    preserve_gpu: bool,
    max_host_cpu: f32,
    fail_unfulfillable_tasks: bool,
    speed_hold_factor_flag: Option<String>,
    speed_hold_factor_cfg: Option<f64>,
    max_batch_size_flag: Option<String>,
    max_batch_size_cfg: Option<usize>,
) -> Result<SchedulerConfig, String> {
    let scheduler_policy = resolve_scheduler_policy(flag, cfg_val)?;
    let speed_hold_factor =
        resolve_speed_hold_factor(speed_hold_factor_flag, speed_hold_factor_cfg)?;
    let max_batch_size = resolve_max_batch_size(max_batch_size_flag, max_batch_size_cfg)?;
    Ok(SchedulerConfig {
        policy: scheduler_policy,
        preserve_gpu_for_gpu_tasks: preserve_gpu,
        max_host_cpu_pct: max_host_cpu,
        fail_unfulfillable_tasks,
        speed_hold_factor,
        max_batch_size,
        ..Default::default()
    })
}

/// Runs the master. The body's future is large (the server, the auth context and the P2P
/// endpoint), so it is boxed: a caller on a default 2 MiB thread stack, such as a
/// `#[tokio::test]`, would otherwise overflow it in a debug build.
#[cfg(any(not(feature = "otel"), test))]
pub(super) async fn run_master(
    args: MasterArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    Box::pin(run_master_inner(
        args,
        config_file,
        secret_opts,
        #[cfg(feature = "otel")]
        None,
    ))
    .await
}

#[cfg(feature = "otel")]
pub(super) async fn run_master_with_otel(
    args: MasterArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
    otel_handle: Option<rusty_grid_master::otel::OtlpExporterHandle>,
) -> ExitCode {
    Box::pin(run_master_inner(
        args,
        config_file,
        secret_opts,
        otel_handle,
    ))
    .await
}

async fn run_master_inner(
    args: MasterArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
    #[cfg(feature = "otel")] otel_handle: Option<rusty_grid_master::otel::OtlpExporterHandle>,
) -> ExitCode {
    let master_cfg = config_file.as_ref().and_then(|c| c.master.clone());

    let rendezvous_url =
        match crate::connect::resolve_and_validate_rendezvous_url(args.rendezvous.as_deref()) {
            Ok(url) => url,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };

    // Loopback by default: a bare `master` works with a bare `worker` on the same machine, and
    // reaching other machines is an explicit `--listen 0.0.0.0:8088` that brings in the secret policy.
    let listen_addr_str = resolve_string(
        args.listen,
        &["RUSTY_GRID_MASTER_LISTEN", "RUSTY_GRID_LISTEN"],
        master_cfg.as_ref().and_then(|m| m.listen.clone()),
        DEFAULT_MASTER_ADDR,
    );

    let port_file = resolve_opt_string(
        args.port_file.map(|p| p.display().to_string()),
        &["RUSTY_GRID_PORT_FILE"],
        master_cfg.as_ref().and_then(|m| m.port_file.clone()),
    );

    let heartbeat_interval = resolve_u64(
        args.heartbeat_interval_secs,
        &["RUSTY_GRID_HEARTBEAT_INTERVAL_SECS"],
        master_cfg.as_ref().and_then(|m| m.heartbeat_interval_secs),
        3,
    );

    let heartbeat_timeout = resolve_u64(
        args.heartbeat_timeout_secs,
        &["RUSTY_GRID_HEARTBEAT_TIMEOUT_SECS"],
        master_cfg.as_ref().and_then(|m| m.heartbeat_timeout_secs),
        10,
    );

    let reaper_interval = resolve_u64(
        args.reaper_interval_secs,
        &["RUSTY_GRID_REAPER_INTERVAL_SECS"],
        master_cfg.as_ref().and_then(|m| m.reaper_interval_secs),
        1,
    );

    let max_host_cpu = resolve_f32(
        args.max_host_cpu_pct,
        &["RUSTY_GRID_MAX_HOST_CPU_PCT"],
        master_cfg.as_ref().and_then(|m| m.max_host_cpu_pct),
        85.0,
    );

    let preserve_gpu = resolve_bool(
        args.preserve_gpu,
        &["RUSTY_GRID_PRESERVE_GPU"],
        master_cfg.as_ref().and_then(|m| m.preserve_gpu),
        true,
    );

    let fail_unfulfillable_tasks =
        resolve_fail_unfulfillable_tasks(args.fail_unfulfillable_tasks, master_cfg.as_ref());

    let p2p_ticket_file = resolve_opt_string(
        args.p2p_ticket_file.map(|p| p.display().to_string()),
        &["RUSTY_GRID_P2P_TICKET_FILE"],
        master_cfg.as_ref().and_then(|m| m.p2p_ticket_file.clone()),
    );

    let ephemeral_key = resolve_bool(
        if args.ephemeral_key { Some(true) } else { None },
        &["RUSTY_GRID_EPHEMERAL_KEY"],
        None,
        false,
    );

    let mut p2p_key_file = resolve_opt_string(
        args.p2p_key_file.map(|p| p.display().to_string()),
        &["RUSTY_GRID_P2P_KEY_FILE"],
        master_cfg.as_ref().and_then(|m| m.p2p_key_file.clone()),
    );

    let mut enable_p2p = resolve_bool(
        if args.p2p { Some(true) } else { None },
        &["RUSTY_GRID_P2P"],
        master_cfg.as_ref().and_then(|m| m.p2p),
        false,
    );

    if p2p_key_file.is_some() || p2p_ticket_file.is_some() {
        enable_p2p = true;
    }

    if enable_p2p && p2p_key_file.is_none() && !ephemeral_key {
        if let Some(def_key) = rusty_grid_master::server::default_master_key_file() {
            p2p_key_file = Some(def_key.display().to_string());
        }
    }

    let mut server_config = match ServerConfig::from_addr(&listen_addr_str) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Invalid listen address '{listen_addr_str}': {e}");
            return ExitCode::FAILURE;
        }
    };

    let default_retry_max = resolve_u32(
        args.default_retry_max,
        &["RUSTY_GRID_DEFAULT_RETRY_MAX", "RUSTY_GRID_MAX_RETRIES"],
        master_cfg.as_ref().and_then(|m| m.default_retry_max),
        3,
    );

    let p2p_minimal = resolve_p2p_minimal();
    let relay_url = match resolve_relay_url(
        args.relay_url,
        config_file.as_ref().and_then(|c| c.master_relay_url()),
        p2p_minimal,
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    if let Some(pf) = port_file {
        server_config = server_config.with_port_file(pf);
    }
    server_config = server_config
        .with_heartbeat_interval(heartbeat_interval)
        .with_p2p(enable_p2p)
        .with_p2p_minimal_preset(p2p_minimal)
        .with_relay_url(relay_url)
        .with_ephemeral_key(ephemeral_key)
        .with_max_retries(default_retry_max);

    if let Some(ref tf) = p2p_ticket_file {
        server_config = server_config.with_p2p_ticket_file(tf);
    }

    if let Some(r_url) = rendezvous_url {
        server_config = server_config.with_rendezvous_url(r_url);
    }

    server_config = match apply_web_ui_addr(server_config, args.web_ui_addr.as_deref()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let max_retained_tasks = resolve_usize(
        args.max_retained_tasks,
        &["RUSTY_GRID_MAX_RETAINED_TASKS"],
        master_cfg.as_ref().and_then(|m| m.max_retained_tasks),
        rusty_grid_master::queue::DEFAULT_MAX_TERMINAL_TASKS,
    );
    let max_retained_mb = resolve_u64(
        args.max_retained_mb,
        &["RUSTY_GRID_MAX_RETAINED_MB"],
        master_cfg.as_ref().and_then(|m| m.max_retained_mb),
        (rusty_grid_master::queue::DEFAULT_MAX_TERMINAL_BYTES / (1024 * 1024)) as u64,
    );
    let mapreduce_parallelism = resolve_usize(
        args.mapreduce_parallelism,
        &["RUSTY_GRID_MAPREDUCE_PARALLELISM"],
        master_cfg.as_ref().and_then(|m| m.mapreduce_parallelism),
        rusty_grid_master::mapreduce::DEFAULT_MAPREDUCE_PARALLELISM,
    );
    let max_connections = resolve_usize(
        args.max_connections,
        &["RUSTY_GRID_MAX_CONNECTIONS"],
        master_cfg.as_ref().and_then(|m| m.max_connections),
        rusty_grid_master::server::DEFAULT_MAX_CONNECTIONS,
    );
    let max_pending_handshakes = resolve_usize(
        args.max_pending_handshakes,
        &["RUSTY_GRID_MAX_PENDING_HANDSHAKES"],
        master_cfg.as_ref().and_then(|m| m.max_pending_handshakes),
        rusty_grid_master::server::DEFAULT_MAX_PENDING_HANDSHAKES,
    );
    server_config = match apply_max_active_mb(
        server_config,
        args.max_active_mb,
        master_cfg.as_ref().and_then(|m| m.max_active_mb),
    ) {
        Ok(cfg) => cfg,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };
    server_config = match apply_max_queue_size(
        server_config,
        args.max_queue_size,
        master_cfg.as_ref().and_then(|m| m.max_queue_size),
    ) {
        Ok(cfg) => cfg,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };
    let sched_config = match build_master_scheduler_config(
        args.scheduler_policy,
        master_cfg.as_ref().and_then(|m| m.scheduler_policy.clone()),
        preserve_gpu,
        max_host_cpu,
        fail_unfulfillable_tasks,
        args.speed_hold_factor,
        master_cfg.as_ref().and_then(|m| m.speed_hold_factor),
        args.max_batch_size,
        master_cfg.as_ref().and_then(|m| m.max_batch_size),
    ) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let wire_codec = match resolve_wire_codec(
        args.wire_codec,
        master_cfg.as_ref().and_then(|w| w.wire_codec.clone()),
    ) {
        Ok(codec) => codec,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    server_config = server_config
        .with_wire_codec(wire_codec)
        .with_max_terminal_tasks(max_retained_tasks)
        .with_max_terminal_bytes(
            usize::try_from(max_retained_mb.saturating_mul(1024 * 1024)).unwrap_or(usize::MAX),
        )
        .with_mapreduce_parallelism(mapreduce_parallelism)
        .with_max_connections(max_connections)
        .with_max_pending_handshakes(max_pending_handshakes);

    let state_dir = resolve_state_dir_value(
        args.state_dir,
        std::env::var("RUSTY_GRID_STATE_DIR").ok().as_deref(),
        master_cfg.as_ref().and_then(|m| m.state_dir.as_deref()),
    );
    if let Some(ref dir) = state_dir {
        // Before anything is written there (`master_noise.key` and `members.json` come next): a
        // volume that keeps no file permissions (exFAT, FAT32) would expose the master's keys,
        // and the next start would refuse `members.json` (BUG-24, design 0085).
        if let Err(msg) =
            rusty_grid_core::private_file::ensure_volume_keeps_permissions(dir, "state directory")
        {
            eprintln!("Refusing to start the master: {msg}");
            return ExitCode::FAILURE;
        }
        server_config = server_config.with_state_dir(dir);
    }
    let requested_hold_secs = resolve_u64(
        args.session_resume_hold_secs,
        &["RUSTY_GRID_SESSION_RESUME_HOLD_SECS"],
        master_cfg.as_ref().and_then(|m| m.session_resume_hold_secs),
        rusty_grid_master::DEFAULT_SESSION_RESUME_HOLD_SECS,
    );
    let max_hold_secs = rusty_grid_core::protocol::master_features::MAX_SESSION_RESUME_HOLD_SECS;
    if requested_hold_secs > max_hold_secs {
        tracing::warn!(
            requested_secs = requested_hold_secs,
            max_secs = max_hold_secs,
            "Requested --session-resume-hold-secs exceeds maximum; clamping to {max_hold_secs}s"
        );
    }
    let session_resume_hold_secs = requested_hold_secs.min(max_hold_secs);
    server_config =
        server_config.with_session_resume_hold(Duration::from_secs(session_resume_hold_secs));

    // Task input and output files live in a bounded, content-addressed store on disk.
    let blob_store_mb = resolve_u64(
        args.blob_store_mb,
        &["RUSTY_GRID_BLOB_STORE_MB"],
        master_cfg.as_ref().and_then(|m| m.blob_store_mb),
        rusty_grid_master::DEFAULT_BLOB_STORE_MAX_BYTES / MIB,
    );
    let blob_dir = resolve_opt_string(
        args.blob_dir.map(|p| p.display().to_string()),
        &["RUSTY_GRID_BLOB_DIR"],
        master_cfg.as_ref().and_then(|m| m.blob_dir.clone()),
    )
    .map(PathBuf::from)
    .or_else(|| state_dir.as_ref().map(|dir| dir.join("blobs")))
    .or_else(|| crate::config::default_oxideswarm_dir().map(|dir| dir.join("blobs")));
    match &blob_dir {
        Some(dir) => {
            server_config =
                server_config.with_blob_store(dir, blob_store_mb.saturating_mul(MIB));
        }
        None => eprintln!(
            "Warning: no home directory to keep the blob store in; tasks with input or output files will be refused (pass --blob-dir DIR)"
        ),
    }

    // Cluster secret policy: a master reachable beyond this machine (non-loopback listener, or
    // the P2P listener, since an iroh ticket is an address and not a credential) must
    // authenticate its peers. Resolve the secret, generate the default file if nothing resolves,
    // or honour the explicit --insecure escape hatch.
    let auth_mode =
        match resolve_auth_mode(args.auth, master_cfg.as_ref().and_then(|m| m.auth.clone())) {
            Ok(mode) => mode,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
    let insecure = resolve_bool(
        if args.insecure { Some(true) } else { None },
        &["RUSTY_GRID_INSECURE"],
        master_cfg.as_ref().and_then(|m| m.insecure),
        false,
    );
    let secret_outcome = match secret_opts
        .resolve(config_file.as_ref(), SecretSection::Master)
        .and_then(|resolved| {
            let exposed = (!server_config.bind_addr.ip().is_loopback() || enable_p2p)
                && auth_mode.allows_secret();
            decide_master_secret(resolved, exposed, insecure, ClusterSecret::default_path())
        }) {
        Ok(outcome) => outcome,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    // A bare master (no secret, no token) with a loopback dashboard gets local access without a
    // login; the library enforces that this never applies to a non-loopback dashboard. A
    // members-only master reachable beyond loopback has no secret either, but it is not bare:
    // local Admin there could mint invites, so only an explicit `--dev` opens its dashboard.
    let cluster_exposed = !server_config.bind_addr.ip().is_loopback() || enable_p2p;
    server_config =
        server_config.with_implicit_local_dashboard(!cluster_exposed || auth_mode.allows_secret());
    let in_unisolated_unit_test =
        cfg!(test) && state_dir.is_none() && std::env::var_os("OXIDESWARM_HOME").is_none();
    let disable_v3_disk = in_unisolated_unit_test
        || (ephemeral_key && state_dir.is_none())
        || !auth_mode.allows_members();
    let members = if disable_v3_disk {
        rusty_grid_master::server::MemberStore::new()
    } else {
        match rusty_grid_master::server::MemberStore::load_from_dir(state_dir.as_deref()) {
            Ok(store) => store,
            Err(e) => {
                eprintln!("Failed to load members.json: {e}");
                return ExitCode::FAILURE;
            }
        }
    };
    let key_path = if disable_v3_disk {
        None
    } else {
        rusty_grid_core::noise_auth::master_noise_key_path(state_dir.as_deref())
    };
    let members_path = if disable_v3_disk {
        None
    } else {
        rusty_grid_master::server::MemberStore::resolve_path(state_dir.as_deref())
    };
    let local_node_key_path = if disable_v3_disk {
        None
    } else if cfg!(test) && std::env::var_os("OXIDESWARM_HOME").is_none() {
        state_dir.as_deref().map(|d| d.join("node.key"))
    } else {
        rusty_grid_core::noise_auth::default_node_key_path()
    };
    let existing_v3_files = key_path.as_ref().is_some_and(|p| p.is_file())
        || members_path.as_ref().is_some_and(|p| p.is_file());
    // OXS-AUTH v3 files (`master_noise.key`, `node.key`, `members.json`) are persisted at startup
    // only when the master admits members (`--auth both` or `--auth members`) and one of: a state
    // directory was given, `master_noise.key` or `members.json` already exists on disk, members
    // are already present, `--auth members` is used, or the master uses the default
    // `~/.oxideswarm/cluster.secret` under `--auth both`. With `--auth secret`, no v3 files are
    // ever loaded or written; under `--auth both` with an explicit `--secret` / `--secret-file`,
    // `--insecure`, or bare loopback without existing v3 files, `master_noise.key`, `members.json`,
    // and `local-host` are persisted on demand only if `CreateInvite` is called.
    let has_explicit_secret =
        secret_opts.has_explicit_secret(config_file.as_ref(), SecretSection::Master);
    let uses_default_secret = matches!(
        secret_outcome,
        crate::secret_cmd::MasterSecretOutcome::Generated { .. }
    ) || (matches!(
        secret_outcome,
        crate::secret_cmd::MasterSecretOutcome::Configured(_)
    ) && !has_explicit_secret);
    let persist_noise_key = !disable_v3_disk
        && (state_dir.is_some()
            || existing_v3_files
            || !members.is_empty()
            || matches!(auth_mode, rusty_grid_master::server::AuthMode::Members)
            || uses_default_secret);
    if !persist_noise_key {
        members.set_save_path(None);
    }
    let master_noise_key = if !persist_noise_key {
        match rusty_grid_core::noise_auth::NodeKeypair::generate() {
            Ok(kp) => kp,
            Err(e) => {
                eprintln!("Failed to generate master_noise.key: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if let Some(ref kp_path) = key_path {
        match rusty_grid_core::noise_auth::NodeKeypair::load_or_generate_file(kp_path) {
            Ok((kp, _)) => kp,
            Err(e) => {
                eprintln!("Failed to load or generate {}: {e}", kp_path.display());
                return ExitCode::FAILURE;
            }
        }
    } else {
        match rusty_grid_core::noise_auth::NodeKeypair::generate() {
            Ok(kp) => kp,
            Err(e) => {
                eprintln!("Failed to generate master_noise.key: {e}");
                return ExitCode::FAILURE;
            }
        }
    };
    if persist_noise_key && auth_mode.allows_members() {
        if let Some(ref node_key_path) = local_node_key_path {
            match rusty_grid_core::noise_auth::NodeKeypair::load_or_generate_file(node_key_path) {
                Ok((host_node_kp, _)) => {
                    if let Err(e) = members.ensure_local_host_admin(
                        *host_node_kp.public_key(),
                        "local-host",
                        true,
                    ) {
                        eprintln!("Failed to enrol local master host in members.json: {e}");
                        return ExitCode::FAILURE;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Failed to load or generate {}: {e}",
                        node_key_path.display()
                    );
                    return ExitCode::FAILURE;
                }
            }
        }
    }
    server_config = server_config
        .with_auth_mode(auth_mode)
        .with_members(members)
        .with_master_noise_key(master_noise_key)
        .with_master_noise_key_path(key_path)
        .with_local_node_key_path(local_node_key_path);
    if auth_mode == rusty_grid_master::server::AuthMode::Members {
        println!("Cluster authentication: members only (enrolled machines via OXS-AUTH v3)");
    } else {
        announce_master_secret(&secret_outcome, &listen_addr_str, insecure);
    }
    if let Some(secret) = secret_outcome.secret() {
        server_config = server_config.with_cluster_secret(secret.clone());
    }

    if let Some(ref kf) = p2p_key_file {
        server_config = server_config.with_p2p_key_file(kf);
    }

    let dashboard_port = resolve_opt_u16(
        args.dashboard_port,
        &["RUSTY_GRID_DASHBOARD_PORT", "OXIDE_SWARM_DASHBOARD_PORT"],
        master_cfg.as_ref().and_then(|m| m.dashboard_port),
    );

    let dashboard_port_file = resolve_opt_string(
        args.dashboard_port_file.map(|p| p.display().to_string()),
        &[
            "RUSTY_GRID_DASHBOARD_PORT_FILE",
            "OXIDE_SWARM_DASHBOARD_PORT_FILE",
        ],
        master_cfg
            .as_ref()
            .and_then(|m| m.dashboard_port_file.clone()),
    );

    // `--web-ui-addr` (a flag, so it outranks the environment and the config file) names the whole
    // dashboard address; a `dashboard_port` from RUSTY_GRID_DASHBOARD_PORT or the config file
    // would otherwise take over and quietly put the dashboard back on loopback.
    match (dashboard_port, args.web_ui_addr.is_some()) {
        (Some(dp), true) => eprintln!(
            "Note: ignoring dashboard_port {dp} from the environment or the config file: --web-ui-addr names the dashboard address"
        ),
        (Some(dp), false) => server_config = server_config.with_dashboard_port(dp),
        (None, _) => {}
    }
    if let Some(ref dpf) = dashboard_port_file {
        server_config = server_config.with_dashboard_port_file(dpf);
    }
    let discovery_port = resolve_opt_u16(
        args.discovery_port,
        &["RUSTY_GRID_DISCOVERY_PORT"],
        master_cfg.as_ref().and_then(|m| m.discovery_port),
    );
    server_config = apply_discovery_port(server_config, discovery_port);
    if args.dev {
        server_config = server_config.with_dev_mode(true);
    }
    for token_spec in &args.auth_tokens {
        let parts: Vec<&str> = token_spec.split(':').collect();
        if parts.len() >= 3 {
            let token = parts[0];
            let name = parts[1];
            let scopes: Vec<rusty_grid_master::auth::AuthScope> =
                parts[2].split(',').filter_map(|s| s.parse().ok()).collect();
            server_config.auth_config.add_token(token, name, &scopes);
        }
    }

    let reaper_config = ReaperConfig::new(
        Duration::from_secs(reaper_interval),
        Duration::from_secs(heartbeat_timeout),
    );

    info!(
        bind_addr = %server_config.bind_addr,
        enable_p2p = enable_p2p,
        "Starting RustyGrid Master"
    );

    let handle =
        match MasterServer::spawn_with_config(server_config, sched_config, reaper_config).await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Failed to start master server: {e}");
                return ExitCode::FAILURE;
            }
        };

    println!("RustyGrid Master started on {}", handle.server_addr());
    if let Some(pub_key) = handle.master_noise_public_key() {
        println!(
            "Master v3 public key: {} (fingerprint: {})",
            pub_key.to_hex(),
            pub_key.fingerprint()
        );
    }
    if let Some(dash_addr) = handle.dashboard_addr() {
        println!("Embedded Dashboard available at http://{}", dash_addr);
        for line in dashboard_banner(
            dash_addr,
            handle.dashboard_dev_bypass(),
            secret_outcome.secret().is_some(),
        ) {
            println!("{line}");
        }
        if !dash_addr.ip().to_canonical().is_loopback() {
            eprintln!(
                "WARNING: the dashboard is listening on {dash_addr}, which is not loopback, and it speaks plain HTTP:\n\
                 \x20        tokens and task output cross the network unencrypted. Keep it on 127.0.0.1\n\
                 \x20        (the default) and reach it through an SSH tunnel or a VPN instead."
            );
        }
    }

    if enable_p2p {
        if let Some(ref tf) = p2p_ticket_file {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Ok(ticket) = tokio::fs::read_to_string(tf).await {
                println!("P2P Connection Ticket: {}", ticket.trim());
            }
        }
    }

    // Await termination signal (SIGINT or SIGTERM on Unix)
    wait_for_worker_shutdown_signal().await;
    println!("\nRustyGrid Master shutting down...");
    let _ = handle.shutdown();
    #[cfg(feature = "otel")]
    if let Some(otel) = otel_handle {
        otel.shutdown().await;
    }
    ExitCode::SUCCESS
}

/// Applies `--web-ui-addr` to the master's configuration.
///
/// Without the flag the dashboard keeps `ServerConfig`'s default, `127.0.0.1:8080`, whatever the
/// grid listens on: a master that listens on `0.0.0.0` for workers must not publish its dashboard
/// (plain HTTP) to the network as a side effect. Naming an address is the explicit opt-in.
pub(super) fn apply_web_ui_addr(
    config: ServerConfig,
    web_ui_addr: Option<&str>,
) -> Result<ServerConfig, String> {
    match web_ui_addr {
        None => Ok(config),
        Some(addr) => addr
            .parse()
            .map(|parsed| config.with_web_ui(true).with_web_ui_addr(parsed))
            .map_err(|e| format!("Invalid web UI address '{addr}': {e}")),
    }
}

/// Startup lines describing how the dashboard authenticates, from the resolved dev-bypass state.
pub(super) fn dashboard_banner(
    dash_addr: std::net::SocketAddr,
    bypass: rusty_grid_master::auth::DevBypass,
    has_secret: bool,
) -> Vec<String> {
    use rusty_grid_master::auth::DevBypass;
    let mut lines = Vec::new();
    match bypass {
        DevBypass::Implicit => lines.push(
            "Dashboard access: this machine only, no login (no cluster secret and no --auth-token \
             are configured). Set either one to require a token."
                .to_string(),
        ),
        DevBypass::Explicit => lines.push(
            "Dashboard access: --dev is ON, so requests from this machine are trusted without a login."
                .to_string(),
        ),
        DevBypass::RefusedNonLoopback => lines.push(format!(
            "Dashboard access: --dev / OXIDE_DEV_MODE ignored, the dashboard is not on a loopback address ({dash_addr}); a token is required."
        )),
        DevBypass::Off => {
            if has_secret {
                lines.push(
                    "Dashboard login token: run `oxideswarm secret show --dashboard-token`"
                        .to_string(),
                );
            } else {
                lines.push(
                    "Dashboard access: a token is required (--auth-token); --dev allows this machine without one."
                        .to_string(),
                );
            }
        }
    }
    if has_secret && matches!(bypass, DevBypass::Explicit | DevBypass::RefusedNonLoopback) {
        lines.push(
            "Dashboard login token: run `oxideswarm secret show --dashboard-token`".to_string(),
        );
    }
    lines
}

/// Applies the resolved `--discovery-port`: `None` keeps the default beacon port (8089), `0` turns
/// the beacon off, anything else moves it. A set `RUSTY_GRID_DISABLE_DISCOVERY` still wins (it
/// is read in `ServerConfig::new`, and a port never turns the beacon back on).
fn apply_discovery_port(config: ServerConfig, port: Option<u16>) -> ServerConfig {
    match port {
        None => config,
        Some(0) => config.with_discovery(false),
        Some(p) => config.with_discovery_port(p),
    }
}

#[cfg(test)]
mod discovery_port_tests {
    use super::*;

    fn base() -> ServerConfig {
        ServerConfig::new("127.0.0.1:18088".parse().unwrap()).with_discovery(true)
    }

    #[test]
    fn unset_keeps_the_default_beacon_port() {
        let c = apply_discovery_port(base(), None);
        assert_eq!(c.discovery_port, rusty_grid_core::DEFAULT_DISCOVERY_PORT);
        assert!(c.enable_discovery);
    }

    #[test]
    fn a_port_moves_the_beacon() {
        let c = apply_discovery_port(base(), Some(18089));
        assert_eq!(c.discovery_port, 18089);
        assert!(c.enable_discovery);
    }

    #[test]
    fn zero_turns_the_beacon_off() {
        let c = apply_discovery_port(base(), Some(0));
        assert!(!c.enable_discovery);
    }

    #[test]
    fn a_port_does_not_turn_a_disabled_beacon_back_on() {
        let c = apply_discovery_port(base().with_discovery(false), Some(18089));
        assert!(!c.enable_discovery);
    }
}
