//! Unified CLI binary for `rusty_grid`.
//!
//! Provides the primary command-line interface for orchestrating the cluster:
//! - `rusty-grid master`: Starts coordinator server with TCP/P2P listener.
//! - `rusty-grid worker`: Starts compute agent with hardware auto-detection & overrides.
//! - `rusty-grid submit`: Submits generic, GPU, or compilation workloads to the grid.
//! - `rusty-grid status`: Queries cluster metrics, task lifecycle state, or worker details.
//! - `rusty-grid workers`: Inspects active worker nodes advertising capabilities.
//! - `rusty-grid mapreduce`: Orchestrates in-memory distributed map-reduce jobs.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport, WireCodec};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_master::{MasterServer, ReaperConfig, SchedulerConfig, ServerConfig};
use rusty_grid_worker::{WorkerClient, WorkerConfig};

use crate::config::{
    self, load_config_file, resolve_bool, resolve_f32, resolve_opt_field, resolve_opt_string,
    resolve_opt_u16, resolve_opt_u64, resolve_opt_usize, resolve_string, resolve_u32, resolve_u64,
};
use crate::mode_cli;

#[derive(Parser, Debug)]
#[command(
    name = "rusty-grid",
    about = "Distributed computing framework for general, compilation, and GPU workloads",
    version
)]
pub struct Cli {
    #[arg(
        global = true,
        short = 'v',
        long = "verbose",
        help = "Increase logging verbosity"
    )]
    pub verbose: bool,

    #[arg(
        global = true,
        short = 'q',
        long = "quiet",
        help = "Suppress non-essential output"
    )]
    pub quiet: bool,

    #[arg(
        global = true,
        long = "config",
        help = "Path to configuration file (TOML or JSON)"
    )]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start master coordinator node
    Master(MasterArgs),

    /// Start worker compute node
    Worker(WorkerArgs),

    /// Submit a task to the grid cluster
    Submit(SubmitArgs),

    /// Query cluster status or task execution state
    Status(StatusArgs),

    /// List active worker nodes in the grid
    Workers(WorkersArgs),

    /// Execute a distributed Map/Reduce job
    Mapreduce(MapReduceArgs),

    /// Workflow modes and presets (test, dev, doc, research)
    Mode(mode_cli::ModeCliArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct MasterArgs {
    #[arg(
        long,
        help = "Socket address to bind TCP listener to (e.g. 127.0.0.1:8080 or 127.0.0.1:0)"
    )]
    pub listen: Option<String>,

    #[arg(
        long,
        alias = "portfile",
        help = "File path where bound port number is written"
    )]
    pub port_file: Option<PathBuf>,

    #[arg(
        long,
        alias = "heartbeat-interval",
        help = "Heartbeat interval advertised to workers in seconds"
    )]
    pub heartbeat_interval_secs: Option<u64>,

    #[arg(
        long,
        alias = "heartbeat-timeout",
        help = "Timeout in seconds before marking unacknowledged worker disconnected"
    )]
    pub heartbeat_timeout_secs: Option<u64>,

    #[arg(
        long,
        alias = "reaper-interval",
        help = "Interval in seconds between dead-worker reaper sweeps"
    )]
    pub reaper_interval_secs: Option<u64>,

    #[arg(long, help = "Maximum queue capacity")]
    pub max_queue_size: Option<usize>,

    #[arg(long, help = "Maximum execution retries for failed tasks")]
    pub default_retry_max: Option<u32>,

    #[arg(long, help = "Scheduler policy (weighted, least-loaded, round-robin)")]
    pub scheduler_policy: Option<String>,

    #[arg(
        long,
        help = "Host CPU usage % threshold for worker anti-stuttering backpressure"
    )]
    pub max_host_cpu_pct: Option<f32>,

    #[arg(long, help = "Preserve GPU-capable workers for GPU-specific workloads")]
    pub preserve_gpu: Option<bool>,

    #[arg(long, help = "Enable native P2P QUIC endpoint for NAT traversal")]
    pub p2p: bool,

    #[arg(long, help = "File path where P2P connection ticket string is written")]
    pub p2p_ticket_file: Option<PathBuf>,

    #[arg(
        long,
        help = "Address to bind HTTP Web UI Dashboard listener (e.g. 127.0.0.1:3000)"
    )]
    pub web_ui_addr: Option<String>,

    #[arg(
        long,
        alias = "key-file",
        value_name = "PATH",
        help = "File path to persist/load P2P node secret key for stable ticket generation"
    )]
    pub p2p_key_file: Option<PathBuf>,

    #[arg(
        long,
        help = "Do not persist P2P secret key to default ~/.oxideswarm/master_key.bin; generate ephemeral key"
    )]
    pub ephemeral_key: bool,

    #[arg(
        long,
        default_value = "bincode",
        help = "Wire format codec: bincode or json (default: bincode)"
    )]
    pub wire_codec: String,

    #[arg(
        long = "dashboard-port",
        value_name = "PORT",
        help = "Enable embedded web observability dashboard HTTP server on specified port (use 0 for ephemeral)"
    )]
    pub dashboard_port: Option<u16>,

    #[arg(
        long = "dashboard-port-file",
        value_name = "PATH",
        help = "File path where bound dashboard port number is written (useful for ephemeral port 0)"
    )]
    pub dashboard_port_file: Option<PathBuf>,
}

#[derive(Parser, Debug, Clone)]
pub struct WorkerArgs {
    #[arg(
        long,
        help = "Address of the Master node to connect to (e.g. 127.0.0.1:8080)"
    )]
    pub master: Option<String>,

    #[arg(long, help = "P2P connection ticket string for NAT traversal")]
    pub p2p_ticket: Option<String>,

    #[arg(long, help = "Human-readable worker node name override")]
    pub name: Option<String>,

    #[arg(long, alias = "cpu-cores", help = "Override advertised CPU core count")]
    pub cores: Option<usize>,

    #[arg(
        long,
        alias = "override-ram-mb",
        alias = "ram",
        help = "Override advertised RAM in Megabytes"
    )]
    pub ram_mb: Option<u64>,

    #[arg(long, help = "Explicitly advertise physical GPU presence")]
    pub gpu: bool,

    #[arg(long, help = "Explicitly disable GPU advertising")]
    pub no_gpu: bool,

    #[arg(long, help = "Advertise simulated GPU compute capabilities")]
    pub simulate_gpu: bool,

    #[arg(long, help = "Human-readable GPU device model string")]
    pub gpu_name: Option<String>,

    #[arg(
        long,
        alias = "max-concurrent-tasks",
        help = "Maximum concurrent tasks executing simultaneously"
    )]
    pub max_concurrency: Option<usize>,

    #[arg(
        long,
        alias = "heartbeat-interval",
        help = "Heartbeat interval in seconds"
    )]
    pub heartbeat_interval_secs: Option<u64>,

    #[arg(long, help = "Base directory for task scratch sandboxes")]
    pub sandbox_base_dir: Option<PathBuf>,

    #[arg(long, help = "Retain task sandbox directories after execution")]
    pub keep_sandboxes: bool,

    #[arg(
        long,
        default_value = "bincode",
        help = "Wire format codec: bincode or json (default: bincode)"
    )]
    pub wire_codec: String,
}

#[derive(Parser, Debug, Clone)]
pub struct SubmitArgs {
    #[arg(long, help = "Address of Master coordinator")]
    pub master: Option<String>,

    #[arg(
        long = "type",
        alias = "task-type",
        default_value = "generic",
        help = "Task type: generic, gpu, compile, shell, command"
    )]
    pub task_type: String,

    #[arg(long, help = "Command executable name to run")]
    pub command: Option<String>,

    #[arg(long, default_value = "1", help = "Minimum CPU cores required")]
    pub cores: usize,

    #[arg(
        long,
        alias = "ram",
        default_value = "0",
        help = "Minimum RAM in MB required"
    )]
    pub ram_mb: u64,

    #[arg(long, help = "Require GPU worker node")]
    pub gpu: bool,

    #[arg(long, default_value = "60", help = "Task execution timeout in seconds")]
    pub timeout_secs: u64,

    #[arg(long, help = "Maximum execution retries for this task")]
    pub max_retries: Option<u32>,

    #[arg(long, help = "Wait synchronously for task completion and emit result")]
    pub wait: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(last = true, help = "Command arguments (trailing args after --)")]
    pub args: Vec<String>,
}

#[derive(Parser, Debug, Clone)]
pub struct StatusArgs {
    #[arg(long, help = "Address of Master coordinator")]
    pub master: Option<String>,

    #[arg(long, alias = "task", help = "Query status of a specific task ID")]
    pub task_id: Option<Uuid>,

    #[arg(long, help = "Display registered worker nodes in the grid")]
    pub workers: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct WorkersArgs {
    #[arg(long, help = "Address of Master coordinator")]
    pub master: Option<String>,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct MapReduceArgs {
    #[arg(long, help = "Address of Master coordinator")]
    pub master: Option<String>,

    #[arg(long, required = true, help = "Input data files or text datasets")]
    pub input: Vec<String>,

    #[arg(
        long,
        default_value = "word_count",
        help = "Mapper name (e.g. word_count, uppercase) or script path"
    )]
    pub mapper: String,

    #[arg(
        long,
        default_value = "sum",
        help = "Reducer name (e.g. sum, count) or script path"
    )]
    pub reducer: String,

    #[arg(
        long,
        default_value = "4",
        help = "Number of input chunks to partition dataset into"
    )]
    pub chunks: usize,

    #[arg(
        long,
        default_value = "60",
        help = "Timeout in seconds for Map/Reduce execution"
    )]
    pub timeout_secs: u64,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,
}

pub async fn run_cli() -> ExitCode {
    let cli = Cli::parse();

    // Initialize tracing subscriber if not already set
    let log_filter = if cli.verbose {
        "debug"
    } else if cli.quiet {
        "error"
    } else {
        "info"
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_filter)),
        )
        .try_init();

    let config_file = match load_config_file(cli.config.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error: {e}");
            return ExitCode::FAILURE;
        }
    };

    match cli.command {
        Commands::Master(args) => run_master(args, config_file).await,
        Commands::Worker(args) => run_worker(args, config_file).await,
        Commands::Submit(args) => run_submit(args, config_file).await,
        Commands::Status(args) => run_status(args, config_file).await,
        Commands::Workers(args) => run_workers(args, config_file).await,
        Commands::Mapreduce(args) => run_mapreduce(args, config_file).await,
        Commands::Mode(args) => {
            let code = mode_cli::run_mode_cli(args).await;
            if code == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(code as u8)
            }
        }
    }
}

async fn run_master(args: MasterArgs, config_file: Option<config::ConfigFile>) -> ExitCode {
    let master_cfg = config_file.as_ref().and_then(|c| c.master.clone());

    let listen_addr_str = resolve_string(
        args.listen,
        &["RUSTY_GRID_MASTER_LISTEN", "RUSTY_GRID_LISTEN"],
        master_cfg.as_ref().and_then(|m| m.listen.clone()),
        "127.0.0.1:0",
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

    if let Some(pf) = port_file {
        server_config = server_config.with_port_file(pf);
    }
    server_config = server_config
        .with_heartbeat_interval(heartbeat_interval)
        .with_p2p(enable_p2p)
        .with_ephemeral_key(ephemeral_key)
        .with_max_retries(default_retry_max);

    if let Some(ref tf) = p2p_ticket_file {
        server_config = server_config.with_p2p_ticket_file(tf);
    }

    if let Some(ref w_addr) = args.web_ui_addr {
        if let Ok(parsed) = w_addr.parse() {
            server_config = server_config.with_web_ui(true).with_web_ui_addr(parsed);
        }
    }

    if let Some(ref kf) = p2p_key_file {
        server_config = server_config.with_p2p_key_file(kf);
    }

    let wire_codec_str = resolve_string(
        Some(args.wire_codec),
        &["RUSTY_GRID_WIRE_CODEC"],
        master_cfg.as_ref().and_then(|m| m.wire_codec.clone()),
        "bincode",
    );
    let wire_codec: WireCodec = wire_codec_str.parse().unwrap_or_default();
    server_config = server_config.with_wire_codec(wire_codec);

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

    if let Some(dp) = dashboard_port {
        server_config = server_config.with_dashboard_port(dp);
    }
    if let Some(ref dpf) = dashboard_port_file {
        server_config = server_config.with_dashboard_port_file(dpf);
    }

    let sched_config = SchedulerConfig {
        preserve_gpu_for_gpu_tasks: preserve_gpu,
        max_host_cpu_pct: max_host_cpu,
        ..Default::default()
    };

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
    if let Some(dash_addr) = handle.dashboard_addr() {
        println!("Embedded Dashboard available at http://{}", dash_addr);
    }

    if enable_p2p {
        if let Some(ref tf) = p2p_ticket_file {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Ok(ticket) = tokio::fs::read_to_string(tf).await {
                println!("P2P Connection Ticket: {}", ticket.trim());
            }
        }
    }

    // Await termination signal
    let _ = tokio::signal::ctrl_c().await;
    println!("\nRustyGrid Master shutting down...");
    let _ = handle.shutdown();
    ExitCode::SUCCESS
}

async fn run_worker(args: WorkerArgs, config_file: Option<config::ConfigFile>) -> ExitCode {
    let worker_cfg = config_file.as_ref().and_then(|c| c.worker.clone());
    let hw_cfg = worker_cfg.as_ref().and_then(|w| w.hardware.clone());

    let master_addr = resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        worker_cfg.as_ref().and_then(|w| w.master.clone()),
        "127.0.0.1:8080",
    );

    let p2p_ticket = resolve_opt_string(
        args.p2p_ticket,
        &["RUSTY_GRID_P2P_TICKET"],
        worker_cfg.as_ref().and_then(|w| w.p2p_ticket.clone()),
    );

    let name = resolve_opt_string(
        args.name,
        &["RUSTY_GRID_WORKER_NAME", "RUSTY_GRID_NAME"],
        worker_cfg.as_ref().and_then(|w| w.name.clone()),
    );

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

    let sandbox_base_dir = resolve_opt_string(
        args.sandbox_base_dir.map(|p| p.display().to_string()),
        &["RUSTY_GRID_SANDBOX_BASE_DIR"],
        worker_cfg.as_ref().and_then(|w| w.sandbox_base_dir.clone()),
    );

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

    let mut config = WorkerConfig::new(master_addr);
    if let Some(ticket) = p2p_ticket {
        config = config.with_p2p_ticket(ticket);
    }
    if let Some(n) = name {
        config = config.with_name(n);
    }
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
    if simulate_gpu {
        config = config.with_simulate_gpu(true);
    }
    if let Some(gn) = gpu_name {
        config = config.with_gpu_name(gn);
    }
    if let Some(mc) = max_concurrency {
        config = config.with_max_concurrency(mc);
    }
    if let Some(sbd) = sandbox_base_dir {
        config = config.with_sandbox_base_dir(PathBuf::from(sbd));
    }
    config = config.with_keep_sandboxes(keep_sandboxes);
    config.default_heartbeat_interval = Duration::from_secs(heartbeat_interval);

    let wire_codec_str = resolve_string(
        Some(args.wire_codec),
        &["RUSTY_GRID_WIRE_CODEC"],
        worker_cfg.as_ref().and_then(|w| w.wire_codec.clone()),
        "bincode",
    );
    let wire_codec: WireCodec = wire_codec_str.parse().unwrap_or_default();
    config = config.with_wire_codec(wire_codec);

    let mut client = WorkerClient::new(config);
    println!(
        "RustyGrid Worker {} initializing [Name: {}, Cores: {}, RAM: {}MB, GPU: {}, Simulated: {}]",
        client.worker_id(),
        client.capabilities().name,
        client.capabilities().cpu_cores,
        client.capabilities().ram_mb,
        client.capabilities().has_gpu,
        client.capabilities().is_simulated_gpu,
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });

    match client.run(shutdown_rx).await {
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

async fn resolve_master_addr(addr: String) -> String {
    if addr.eq_ignore_ascii_case("auto") || addr.starts_with("auto:") {
        let disc_port = if let Some(port_str) = addr.strip_prefix("auto:") {
            port_str
                .parse::<u16>()
                .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
        } else {
            rusty_grid_core::DEFAULT_DISCOVERY_PORT
        };
        info!(
            port = disc_port,
            "Master address set to 'auto'; performing LAN UDP discovery probe..."
        );
        match rusty_grid_core::discovery::discover_master(Duration::from_secs(3), disc_port).await {
            Some(beacon) => {
                info!(
                    master = %beacon.cluster_addr,
                    hostname = %beacon.hostname,
                    "Discovered active Master via UDP LAN beacon"
                );
                beacon.cluster_addr
            }
            None => {
                eprintln!("Auto-discovery timed out; falling back to 127.0.0.1:8088");
                "127.0.0.1:8088".to_string()
            }
        }
    } else {
        addr
    }
}

async fn run_submit(args: SubmitArgs, config_file: Option<config::ConfigFile>) -> ExitCode {
    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        submit_cfg.as_ref().and_then(|s| s.master.clone()),
        "127.0.0.1:8080",
    );
    let master_addr = resolve_master_addr(master_addr).await;

    let stream = match TcpStream::connect(&master_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to Master at {master_addr}: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut transport = MessageTransport::new(stream);

    let is_gpu = args.gpu || args.task_type.eq_ignore_ascii_case("gpu");
    let spec = match args.task_type.to_lowercase().as_str() {
        "gpu" => TaskSpec::GpuCompute {
            kernel_name: args
                .command
                .clone()
                .unwrap_or_else(|| "matrix_multiply".into()),
            input_data: vec![1, 2, 3, 4],
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 10,
        },
        "compile" => TaskSpec::RustCompilation {
            crate_name: args
                .command
                .clone()
                .unwrap_or_else(|| "simulated_crate".into()),
            source_files: HashMap::new(),
            compiler_flags: if !args.args.is_empty() {
                args.args.clone()
            } else {
                vec!["build".into()]
            },
            target_dir: None,
        },
        "shell" => TaskSpec::new_shell_script(args.command.clone().unwrap_or_else(|| {
            if !args.args.is_empty() {
                args.args.join(" ")
            } else {
                "echo 'Hello from shell task'".into()
            }
        })),
        _ => {
            let cmd = args.command.clone().unwrap_or_else(|| {
                if !args.args.is_empty() {
                    args.args[0].clone()
                } else {
                    "echo".into()
                }
            });
            let cmd_args = if args.command.is_some() {
                args.args.clone()
            } else if args.args.len() > 1 {
                args.args[1..].to_vec()
            } else {
                vec!["Hello from RustyGrid".into()]
            };
            TaskSpec::new_command(cmd, cmd_args)
        }
    };

    let requirements = TaskRequirements {
        cpu_cores: args.cores,
        ram_mb: args.ram_mb,
        gpu_required: is_gpu,
        timeout_secs: args.timeout_secs,
        max_retries: args.max_retries,
    };

    let task = Task::new(spec, requirements);
    let submit_msg = ClientMessage::SubmitTask {
        task,
        wait: args.wait,
    };

    if let Err(e) = transport.send_msg(&submit_msg).await {
        eprintln!("Failed to send task to Master: {e}");
        return ExitCode::FAILURE;
    }

    match transport.recv_msg::<ClientResponse>().await {
        Ok(Some(ClientResponse::TaskSubmitted { task_id })) => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "submitted",
                        "task_id": task_id.to_string()
                    })
                );
            } else {
                println!("Task submitted successfully. Task ID: {task_id}");
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::TaskCompleted { task_id, result })) => {
            if args.json {
                println!("{}", serde_json::to_string_pretty(&result).unwrap());
            } else {
                println!("Task {task_id} completed successfully.");
                println!("Exit Code: {}", result.exit_code);
                println!("Execution Time: {}ms", result.execution_time_ms);
                if !result.stdout.is_empty() {
                    println!("Stdout:\n{}", result.stdout.trim());
                }
                if !result.stderr.is_empty() {
                    eprintln!("Stderr:\n{}", result.stderr.trim());
                }
            }
            if result.exit_code == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(result.exit_code.min(255) as u8)
            }
        }
        Ok(Some(ClientResponse::Error { message })) => {
            if args.json {
                println!("{}", serde_json::json!({"error": message}));
            } else {
                eprintln!("Master error: {message}");
            }
            ExitCode::FAILURE
        }
        Ok(other) => {
            eprintln!("Unexpected master response: {other:?}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("Communication error with Master: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run_status(args: StatusArgs, config_file: Option<config::ConfigFile>) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        status_cfg.as_ref().and_then(|s| s.master.clone()),
        "127.0.0.1:8080",
    );
    let master_addr = resolve_master_addr(master_addr).await;

    let stream = match TcpStream::connect(&master_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to Master at {master_addr}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut transport = MessageTransport::new(stream);

    if let Some(task_uuid) = args.task_id {
        let task_id = TaskId(task_uuid);
        let msg = ClientMessage::GetTaskStatus { task_id };
        if let Err(e) = transport.send_msg(&msg).await {
            eprintln!("Failed to query task status: {e}");
            return ExitCode::FAILURE;
        }
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            })) => {
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "assigned_worker": assigned_worker,
                            "error": error
                        })
                    );
                } else {
                    println!("Task ID:        {task_id}");
                    println!("Status:         {:?}", status);
                    println!("State:          {state_name}");
                    if let Some(w) = assigned_worker {
                        println!("Worker:         {w}");
                    }
                    if let Some(err) = error {
                        println!("Error:          {err}");
                    }
                }
                ExitCode::SUCCESS
            }
            Ok(Some(ClientResponse::Error { message })) => {
                eprintln!("Error: {message}");
                ExitCode::FAILURE
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                ExitCode::FAILURE
            }
        }
    } else if args.workers {
        let msg = ClientMessage::ListWorkers;
        if let Err(e) = transport.send_msg(&msg).await {
            eprintln!("Failed to list workers: {e}");
            return ExitCode::FAILURE;
        }
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::WorkerList { workers })) => {
                if args.json {
                    let enriched_workers: Vec<serde_json::Value> = workers
                        .iter()
                        .map(|w| {
                            let link = parse_worker_link(w);
                            let mut val = serde_json::to_value(w).unwrap_or_default();
                            if let Some(l) = link {
                                if let Some(obj) = val.as_object_mut() {
                                    obj.insert("link".to_string(), serde_json::Value::String(l));
                                }
                            }
                            val
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&enriched_workers).unwrap());
                } else {
                    println!("Connected Workers ({}):", workers.len());
                    for w in &workers {
                        println!(
                            "- Worker [{}] (Cores: {}, RAM: {}MB, GPU: {}, Simulated: {})",
                            w.name, w.cpu_cores, w.ram_mb, w.has_gpu, w.is_simulated_gpu,
                        );
                        if let Some(link) = parse_worker_link(w) {
                            println!("  └─ {}", link);
                        }
                    }
                }
                ExitCode::SUCCESS
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                ExitCode::FAILURE
            }
        }
    } else {
        let msg = ClientMessage::ClusterStatus;
        if let Err(e) = transport.send_msg(&msg).await {
            eprintln!("Failed to get cluster status: {e}");
            return ExitCode::FAILURE;
        }
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            })) => {
                if args.json {
                    let enriched_workers: Vec<serde_json::Value> = workers
                        .iter()
                        .map(|w| {
                            let link = parse_worker_link(w);
                            let mut val = serde_json::to_value(w).unwrap_or_default();
                            if let Some(l) = link {
                                if let Some(obj) = val.as_object_mut() {
                                    obj.insert("link".to_string(), serde_json::Value::String(l));
                                }
                            }
                            val
                        })
                        .collect();
                    println!(
                        "{}",
                        serde_json::json!({
                            "total_tasks": total_tasks,
                            "pending_tasks": pending_tasks,
                            "running_tasks": running_tasks,
                            "completed_tasks": completed_tasks,
                            "failed_tasks": failed_tasks,
                            "worker_count": workers.len(),
                            "workers": enriched_workers
                        })
                    );
                } else {
                    println!("=== RustyGrid Cluster Status ===");
                    println!("Connected Workers: {}", workers.len());
                    println!("Total Tasks:      {}", total_tasks);
                    println!("Pending Tasks:    {}", pending_tasks);
                    println!("Running Tasks:    {}", running_tasks);
                    println!("Completed Tasks:  {}", completed_tasks);
                    println!("Failed Tasks:     {}", failed_tasks);
                    if !workers.is_empty() {
                        println!("\nWorkers:");
                        for w in &workers {
                            println!(
                                "  • [{}] (Cores: {}, RAM: {}MB, GPU: {})",
                                w.name, w.cpu_cores, w.ram_mb, w.has_gpu
                            );
                            if let Some(link) = parse_worker_link(w) {
                                println!("    └─ {}", link);
                            }
                        }
                    }
                }
                ExitCode::SUCCESS
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                ExitCode::FAILURE
            }
        }
    }
}

fn parse_worker_link(w: &rusty_grid_core::WorkerCapabilities) -> Option<String> {
    for tag in &w.tags {
        if let Some(rest) = tag.strip_prefix("link:") {
            if let Some((kind, rtt)) = rest.split_once(':') {
                return Some(format!("Link: {} | RTT: {}", kind, rtt));
            } else {
                return Some(format!("Link: {}", rest));
            }
        }
    }
    Some("Link: TCP/LAN".to_string())
}

async fn run_workers(args: WorkersArgs, config_file: Option<config::ConfigFile>) -> ExitCode {
    let status_args = StatusArgs {
        master: args.master,
        task_id: None,
        workers: true,
        json: args.json,
    };
    run_status(status_args, config_file).await
}

async fn run_mapreduce(args: MapReduceArgs, config_file: Option<config::ConfigFile>) -> ExitCode {
    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        submit_cfg.as_ref().and_then(|s| s.master.clone()),
        "127.0.0.1:8080",
    );
    let master_addr = resolve_master_addr(master_addr).await;

    let stream = match TcpStream::connect(&master_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to Master at {master_addr}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut transport = MessageTransport::new(stream);

    // Read input data from files or treat arguments directly as data
    let mut chunks: Vec<String> = Vec::new();
    for inp in &args.input {
        let p = Path::new(inp);
        if p.exists() {
            match std::fs::read_to_string(p) {
                Ok(content) => {
                    // Split file content into N roughly equal line chunks
                    let lines: Vec<&str> = content.lines().collect();
                    if lines.is_empty() {
                        chunks.push(String::new());
                    } else {
                        let chunk_size = lines.len().div_ceil(args.chunks);
                        for chunk_slice in lines.chunks(chunk_size.max(1)) {
                            chunks.push(chunk_slice.join("\n"));
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read input file '{}': {e}", p.display());
                    return ExitCode::FAILURE;
                }
            }
        } else {
            chunks.push(inp.clone());
        }
    }

    let mapper_spec = if args.mapper.contains('/')
        || args.mapper.contains('\\')
        || Path::new(&args.mapper).exists()
    {
        MapFunctionSpec::ShellScript {
            script: args.mapper,
        }
    } else {
        MapFunctionSpec::Builtin {
            operator: args.mapper,
        }
    };

    let reducer_spec = if args.reducer.contains('/')
        || args.reducer.contains('\\')
        || Path::new(&args.reducer).exists()
    {
        ReduceFunctionSpec::ShellScript {
            script: args.reducer,
        }
    } else {
        ReduceFunctionSpec::Builtin {
            operator: args.reducer,
        }
    };

    let job = MapReduceJobSpec::new(
        "cli_mapreduce_job",
        chunks,
        mapper_spec,
        reducer_spec,
        args.chunks,
        1,
        args.timeout_secs,
    );

    let msg = ClientMessage::SubmitMapReduce { job };
    if let Err(e) = transport.send_msg(&msg).await {
        eprintln!("Failed to dispatch MapReduce job to Master: {e}");
        return ExitCode::FAILURE;
    }

    match transport.recv_msg::<ClientResponse>().await {
        Ok(Some(ClientResponse::MapReduceCompleted { result })) => {
            if args.json {
                println!("{}", serde_json::to_string_pretty(&result).unwrap());
            } else {
                println!("=== MapReduce Job Completed ===");
                println!("Job ID:           {}", result.job_id);
                println!("Status:           {}", result.status);
                println!(
                    "Map Tasks:        {}/{}",
                    result.map_tasks_completed, result.map_tasks_total
                );
                println!(
                    "Reduce Tasks:     {}/{}",
                    result.reduce_tasks_completed, result.reduce_tasks_total
                );
                println!("Execution Time:   {}ms", result.execution_time_ms);
                println!(
                    "\nAggregated Results ({} unique keys):",
                    result.output.len()
                );
                for (key, val) in &result.output {
                    println!("  {key}: {val}");
                }
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::Error { message })) => {
            eprintln!("MapReduce execution error: {message}");
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected Master response: {other:?}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::WorkerCapabilities;

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
    fn test_parse_worker_link_default_tcp() {
        let caps = WorkerCapabilities::new("worker-3", 4, 4096, false, false, None);
        let link = parse_worker_link(&caps);
        assert_eq!(link.as_deref(), Some("Link: TCP/LAN"));
    }
}

