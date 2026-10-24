//! Unified CLI binary for `rusty_grid`.
//!
//! Provides the primary command-line interface for orchestrating the cluster:
//! - `rusty-grid master`: Starts coordinator server with TCP/P2P listener.
//! - `rusty-grid worker`: Starts compute agent with hardware auto-detection & overrides.
//! - `rusty-grid submit`: Submits generic, GPU, or compilation workloads to the grid.
//! - `rusty-grid status`: Queries cluster metrics, task lifecycle state, or worker details.
//! - `rusty-grid workers`: Inspects active worker nodes advertising capabilities.
//! - `rusty-grid mapreduce`: Orchestrates in-memory distributed map-reduce jobs.
//! - `rusty-grid doctor`: Checks a running cluster end to end ([`crate::doctor`]).
//! - `rusty-grid secret`: Generates / shows the shared cluster secret.
//!
//! Every command that joins or drives a grid accepts the cluster secret (`--secret-file`,
//! `--secret`, `OXIDESWARM_SECRET[_FILE]`, config file, `~/.oxideswarm/cluster.secret`); see
//! [`crate::secret_cmd`]. Ports: the Master's grid port is 8088 (default for `--master` and
//! `--listen`), the dashboard is 8080.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::GridError;
use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::placement::{
    listed_worker_id, tag_value, validate_user_tag, Placement, REQUIRE_PREFIX, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport, WireCodec};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_core::WorkerCapabilities;
use rusty_grid_master::{MasterServer, ReaperConfig, SchedulerConfig, ServerConfig};
use rusty_grid_worker::{NoticeLevel, ProcessPriority, WorkerClient, WorkerConfig};

use crate::config::{
    load_config_file, resolve_bool, resolve_f32, resolve_opt_field, resolve_opt_string,
    resolve_opt_u16, resolve_opt_u64, resolve_opt_usize, resolve_string, resolve_u32, resolve_u64,
    resolve_usize, ConfigFile, SecretSection, SecretText,
};
use crate::connect::{
    connect_master, normalize_master_addr, report_master_failure, AUTH_EXIT_CODE,
    DEFAULT_MASTER_ADDR,
};
use crate::doctor::{run_doctor, DoctorOptions};
use crate::mode_cli;
use crate::secret_cmd::{
    announce_master_secret, announce_peer_secret, decide_master_secret, decide_peer_secret,
    run_secret, PeerRole, PeerSecretOutcome, SecretCliArgs, SecretOpts,
};

/// Help text of `--insecure` on the commands that connect to a master (worker and clients).
const INSECURE_PEER_HELP: &str = "Continue WITHOUT a cluster secret although the master is remote, found by 'auto' discovery, or reached through a P2P ticket. The master is then not authenticated: a rogue host can run commands on a worker or receive what a client submits. Without this flag such a connection is refused when no secret is configured.";

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

    #[arg(
        global = true,
        long = "secret-file",
        value_name = "PATH",
        help = "File holding the cluster secret (hex); default: OXIDESWARM_SECRET[_FILE], then ~/.oxideswarm/cluster.secret"
    )]
    pub secret_file: Option<PathBuf>,

    #[arg(
        global = true,
        long = "secret",
        value_name = "HEX",
        help = "Cluster secret as hex (discouraged: visible in the process list; prefer --secret-file)"
    )]
    pub secret: Option<SecretText>,

    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    /// The secret-related flags as one value.
    pub fn secret_opts(&self) -> SecretOpts {
        SecretOpts {
            value: self.secret.clone(),
            file: self.secret_file.clone(),
        }
    }
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

    /// Check a running cluster end to end: connection, workers, pinned probes, GPUs, spreading
    Doctor(DoctorArgs),

    /// Generate or show the shared cluster secret
    Secret(SecretCliArgs),

    /// Workflow modes and presets (test, dev, doc, research)
    Mode(mode_cli::ModeCliArgs),

    /// Model Context Protocol (MCP) server for external AI coding agents (Claude Desktop, Cline, Cursor)
    Mcp(crate::mcp::McpArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct MasterArgs {
    #[arg(
        long,
        help = "Socket address of the grid listener for workers and clients (default 127.0.0.1:8088; use 0.0.0.0:8088 to accept other machines, 127.0.0.1:0 for an ephemeral port)"
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
        conflicts_with = "dashboard_port",
        help = "Address to bind the HTTP Web UI Dashboard listener (default: 127.0.0.1:8080, this machine only). \
                A non-loopback address (e.g. 0.0.0.0:8080) exposes it over PLAIN HTTP to that network: use a tunnel/VPN. \
                It also picks the port, so it cannot be combined with --dashboard-port"
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

    #[arg(
        long = "dev",
        help = "Enable local development mode (bypasses auth for loopback requests)"
    )]
    pub dev: bool,

    #[arg(
        long = "auth-token",
        value_name = "TOKEN:NAME:SCOPES",
        help = "Register an API token with name and comma-separated scopes (e.g. secret123:admin:admin or obs123:monitor:observer)"
    )]
    pub auth_tokens: Vec<String>,

    #[arg(
        long,
        help = "Allow a master reachable beyond this machine (non-loopback --listen, or --p2p) to run WITHOUT a cluster secret. Anyone who can reach it can run commands on every worker."
    )]
    pub insecure: bool,

    #[arg(
        long,
        value_name = "N",
        help = "Finished tasks (with payload and output) the master retains, oldest evicted first (default 10000)"
    )]
    pub max_retained_tasks: Option<usize>,

    #[arg(
        long,
        value_name = "MB",
        help = "Megabytes of finished-task payload and output the master retains, oldest evicted first (default 256)"
    )]
    pub max_retained_mb: Option<u64>,

    #[arg(
        long,
        value_name = "N",
        help = "Tasks of one mapreduce job in flight at once; keep it above the cluster's total slots (default 256)"
    )]
    pub mapreduce_parallelism: Option<usize>,

    #[arg(
        long,
        value_name = "N",
        help = "Connections the grid port keeps open at once; further peers are closed (default 1024)"
    )]
    pub max_connections: Option<usize>,

    #[arg(
        long,
        value_name = "N",
        help = "Connections allowed in the pre-authentication handshake at once (default 64)"
    )]
    pub max_pending_handshakes: Option<usize>,

    #[arg(
        long,
        value_name = "URL",
        help = "URL of rendezvous registry for cross-subnet discovery (or set OXIDESWARM_RENDEZVOUS)"
    )]
    pub rendezvous: Option<String>,
}

#[derive(Parser, Debug, Clone)]
pub struct WorkerArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088, the dashboard is on :8080); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(long, help = "P2P connection ticket string for NAT traversal")]
    pub p2p_ticket: Option<String>,

    #[arg(
        long,
        help = "Human-readable worker node name override. A named worker also keeps the same worker id across restarts (~/.oxideswarm/worker_id.<NAME>, or the file in RUSTY_GRID_WORKER_ID_FILE): give every worker that shares a home directory its own name. Without a name the id is new at each start"
    )]
    pub name: Option<String>,

    #[arg(
        long = "tag",
        value_name = "TAG",
        help = "User tag advertised to the master, matched by `submit --require-tag` (repeatable; env RUSTY_GRID_WORKER_TAGS, comma-separated). The prefixes os:, arch:, link:, id: and state: are reserved"
    )]
    pub tags: Vec<String>,

    #[arg(long, alias = "cpu-cores", help = "Override advertised CPU core count")]
    pub cores: Option<usize>,

    #[arg(
        long,
        alias = "override-ram-mb",
        alias = "ram",
        help = "Override advertised RAM in Megabytes"
    )]
    pub ram_mb: Option<u64>,

    #[arg(
        long,
        help = "Insist on a real GPU. A GPU is advertised only if this binary was built with `--features gpu-wgpu` and an adapter starts (it is looked for automatically unless --no-gpu); otherwise none is advertised and the reason is printed"
    )]
    pub gpu: bool,

    #[arg(long, help = "Never advertise a GPU (also skips looking for one)")]
    pub no_gpu: bool,

    #[arg(
        long,
        help = "TEST MODE: advertise a simulated GPU whose tasks run on the CPU and are labelled as simulated"
    )]
    pub simulate_gpu: bool,

    #[arg(long, help = "Human-readable GPU device model string")]
    pub gpu_name: Option<String>,

    #[arg(long, help = "Explicitly advertise Blender capability")]
    pub blender: bool,

    #[arg(long, help = "Explicitly disable Blender capability")]
    pub no_blender: bool,

    #[arg(long, help = "Path to Blender executable")]
    pub blender_path: Option<String>,

    #[arg(long, help = "Blender version string override")]
    pub blender_version: Option<String>,

    #[arg(long, help = "GPU VRAM in Megabytes override")]
    pub vram_mb: Option<u64>,

    #[arg(long, help = "Explicitly advertise CUDA capability")]
    pub cuda: bool,

    #[arg(long, help = "Explicitly disable CUDA capability")]
    pub no_cuda: bool,

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

    #[arg(
        long,
        help = "Base directory for task scratch sandboxes (default: RUSTY_GRID_SANDBOX_BASE_DIR, then TMPDIR, then the platform temp dir; must be writable)"
    )]
    pub sandbox_base_dir: Option<PathBuf>,

    #[arg(
        long,
        help = "CPU priority of task processes: normal, below-normal (default) or idle; env RUSTY_GRID_TASK_PRIORITY"
    )]
    pub task_priority: Option<String>,

    #[arg(long, help = "Retain task sandbox directories after execution")]
    pub keep_sandboxes: bool,

    #[arg(
        long,
        help = "Do not serve the standby portal (HTTP on 0.0.0.0:8080 that sends a browser to the master's dashboard); env RUSTY_GRID_NO_PORTAL. Useful for a second worker on one machine"
    )]
    pub no_portal: bool,

    #[arg(
        long,
        default_value = "bincode",
        help = "Wire format codec: bincode or json (default: bincode)"
    )]
    pub wire_codec: String,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,

    #[arg(
        long,
        value_name = "URL",
        help = "URL of rendezvous registry for cross-subnet discovery (or set OXIDESWARM_RENDEZVOUS)"
    )]
    pub rendezvous: Option<String>,
}

#[derive(Parser, Debug, Clone, Default)]
pub struct SubmitArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(
        long = "type",
        alias = "task-type",
        default_value = "generic",
        help = "Task type: command (default, also `generic`), shell, gpu, 3d, or autorig. `compile` needs source files, which only the agent-mesh bridge sends"
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

    // =========================================================================
    // 3D Generation Flags (Feature F19)
    // =========================================================================
    #[arg(
        long,
        help = "Text prompt for 3D mesh generation (e.g. 'a low-poly chest')"
    )]
    pub prompt: Option<String>,

    #[arg(
        long,
        value_name = "IMAGE_PATH",
        help = "Input image file path (PNG, JPEG, WebP) for image-to-3D mesh reconstruction"
    )]
    pub image: Option<PathBuf>,

    #[arg(
        long,
        value_name = "QUALITY",
        help = "3D generation quality preset: fast, standard, high (default: standard)"
    )]
    pub quality: Option<String>,

    #[arg(
        long,
        value_name = "ENGINE",
        help = "Preferred 3D generation engine: sdf, triposr, auto (default: auto)"
    )]
    pub engine: Option<String>,

    #[arg(
        long = "export-texture",
        action = clap::ArgAction::SetTrue,
        help = "Bake and export PBR UV texture maps into output glTF asset (default: true)"
    )]
    pub export_texture: bool,

    #[arg(
        long = "no-export-texture",
        action = clap::ArgAction::SetTrue,
        help = "Disable texture map generation and export untextured mesh"
    )]
    pub no_export_texture: bool,

    // =========================================================================
    // Automated Rigging & Animation Flags (Feature F19)
    // =========================================================================
    #[arg(
        long = "input-mesh",
        value_name = "MESH_PATH",
        help = "Input 3D mesh file path (glTF/GLB or Wavefront OBJ) for automated rigging"
    )]
    pub input_mesh: Option<PathBuf>,

    #[arg(
        long = "rig-preset",
        value_name = "PRESET",
        help = "Target skeletal armature preset: biped, quadruped, generic (default: biped)"
    )]
    pub rig_preset: Option<String>,

    #[arg(
        long = "embed-animations",
        value_name = "ANIMATIONS",
        value_delimiter = ',',
        help = "Comma-separated list of animation clips to embed (e.g. idle,walk,turnaround)"
    )]
    pub embed_animations: Vec<String>,

    #[arg(
        long = "preferred-provider",
        value_name = "PROVIDER",
        help = "Preferred auto-rigging provider: builtin, blender (default: builtin)"
    )]
    pub preferred_provider: Option<String>,

    // =========================================================================
    // Asset Export Flag (Feature F19)
    // =========================================================================
    #[arg(
        short = 'o',
        long = "output-asset",
        alias = "glb-output",
        value_name = "OUTPUT_PATH",
        help = "Destination file path to write generated or rigged glTF 2.0 binary asset (.glb)"
    )]
    pub output: Option<PathBuf>,

    #[arg(
        long,
        value_name = "OS",
        help = "Run only on a worker with this operating system: macos, windows, linux, android (aliases: mac, darwin, osx, win)"
    )]
    pub os: Option<String>,

    #[arg(
        long,
        value_name = "ARCH",
        help = "Run only on a worker with this CPU architecture: x86_64, aarch64 (aliases: amd64, x64, arm64)"
    )]
    pub arch: Option<String>,

    #[arg(
        long,
        value_name = "NAME|ID",
        help = "Run only on this worker: its name (ignoring case) or its id, as `workers --json` lists them"
    )]
    pub worker: Option<String>,

    #[arg(
        long = "require-tag",
        value_name = "TAG",
        help = "Run only on a worker that carries this user tag (`worker --tag`); repeatable, all must match"
    )]
    pub require_tags: Vec<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Command tasks: send this local file as the program's standard input (at most 32 MiB)"
    )]
    pub stdin_file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Shell tasks: send this local file as the script (at most 32 MiB), instead of --command or words after --"
    )]
    pub script_file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PROG",
        help = "Shell tasks: run the script with PROG (for example python3) instead of sh or cmd.exe"
    )]
    pub interpreter: Option<String>,

    #[arg(
        long = "env",
        value_name = "KEY=VALUE",
        help = "Command and shell tasks: environment variable of the task process (repeatable)"
    )]
    pub env: Vec<String>,

    #[arg(
        long,
        value_name = "N",
        help = "GPU tasks: edge of the square matrices to multiply (default 64; a real GPU accepts up to 8192 within its limits, the simulated GPU 2048)"
    )]
    pub matrix_dim: Option<u32>,

    #[arg(
        long,
        value_name = "N",
        help = "GPU tasks: edge of a workgroup tile (default 16)"
    )]
    pub work_group_size: Option<u32>,

    #[arg(
        long = "file",
        value_name = "LOCAL[:SANDBOX]",
        help = "Stage a local file into the task sandbox (repeatable, at most 32 MiB total). Syntax: PATH or LOCAL_PATH:SANDBOX_PATH"
    )]
    pub files: Vec<String>,

    #[arg(
        long = "output",
        alias = "expected-output",
        value_name = "PATH",
        help = "Declare an output artifact path to retrieve from the sandbox upon task completion (repeatable)"
    )]
    pub outputs: Vec<String>,

    #[arg(
        long = "output-dir",
        value_name = "DIR",
        help = "Local directory where retrieved output artifacts are written upon task completion (default: current directory)"
    )]
    pub output_dir: Option<PathBuf>,

    #[arg(last = true, help = "Command arguments (trailing args after --)")]
    pub args: Vec<String>,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct StatusArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(long, alias = "task", help = "Query status of a specific task ID")]
    pub task_id: Option<Uuid>,

    #[arg(long, help = "Display registered worker nodes in the grid")]
    pub workers: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct WorkersArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct MapReduceArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
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

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct DoctorArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(
        long,
        value_name = "N",
        default_value = "1",
        help = "Fail unless at least N workers are connected"
    )]
    pub expect_workers: usize,

    #[arg(
        long,
        value_name = "SECS",
        default_value = "60",
        help = "How long each probe, and the spread check as a whole, may take"
    )]
    pub probe_timeout_secs: u64,

    #[arg(
        long,
        value_name = "N",
        default_value = "512",
        help = "Matrix edge of the GEMM probe on GPU workers (at most 2048 on a simulated GPU)"
    )]
    pub gpu_dim: u32,

    #[arg(
        long,
        value_name = "N",
        help = "Unpinned 500 ms tasks of the spread check (default: 4 per connected worker)"
    )]
    pub spread_tasks: Option<usize>,

    #[arg(long, help = "Leave out the GPU probe (C5)")]
    pub skip_gpu: bool,

    #[arg(long, help = "Leave out the spread check (C6)")]
    pub skip_spread: bool,

    #[arg(long, help = "Print the report as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

pub async fn run_cli() -> ExitCode {
    let cli = Cli::parse();

    // Initialize tracing subscriber if not already set
    let log_level = if cli.verbose {
        "debug"
    } else if cli.quiet {
        "error"
    } else {
        "info"
    };
    let builder = tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_filter(log_level))),
    );
    let _ = if matches!(cli.command, Commands::Mcp(_)) {
        builder.with_writer(std::io::stderr).try_init()
    } else {
        builder.try_init()
    };

    let config_file = match load_config_file(cli.config.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let secret_opts = cli.secret_opts();
    match cli.command {
        Commands::Master(args) => run_master(args, config_file, secret_opts).await,
        Commands::Worker(args) => run_worker(args, config_file, secret_opts).await,
        Commands::Submit(args) => run_submit_with_secret(args, config_file, secret_opts).await,
        Commands::Status(args) => run_status(args, config_file, secret_opts).await,
        Commands::Workers(args) => run_workers(args, config_file, secret_opts).await,
        Commands::Mapreduce(args) => run_mapreduce(args, config_file, secret_opts).await,
        Commands::Doctor(args) => run_doctor_command(args, config_file, secret_opts).await,
        Commands::Secret(args) => run_secret(args, &secret_opts, config_file.as_ref()),
        Commands::Mode(args) => {
            let code = mode_cli::run_mode_cli(args).await;
            if code == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(code as u8)
            }
        }
        Commands::Mcp(args) => crate::mcp::run_mcp(args, config_file, secret_opts).await,
    }
}

async fn run_master(
    args: MasterArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let master_cfg = config_file.as_ref().and_then(|c| c.master.clone());

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

    if let Some(r_url) = args.rendezvous {
        server_config = server_config.with_rendezvous_url(r_url);
    }

    server_config = match apply_web_ui_addr(server_config, args.web_ui_addr.as_deref()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };
    // A bare master (no secret, no token) with a loopback dashboard gets local access without a
    // login; the library enforces that this never applies to a non-loopback dashboard.
    server_config = server_config.with_implicit_local_dashboard(true);

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
    server_config = server_config
        .with_max_terminal_tasks(max_retained_tasks)
        .with_max_terminal_bytes(
            usize::try_from(max_retained_mb.saturating_mul(1024 * 1024)).unwrap_or(usize::MAX),
        )
        .with_mapreduce_parallelism(mapreduce_parallelism)
        .with_max_connections(max_connections)
        .with_max_pending_handshakes(max_pending_handshakes);

    // Cluster secret policy: a master reachable beyond this machine (non-loopback listener, or
    // the P2P listener, since an iroh ticket is an address and not a credential) must
    // authenticate its peers. Resolve the secret, generate the default file if nothing resolves,
    // or honour the explicit --insecure escape hatch.
    let insecure = resolve_bool(
        if args.insecure { Some(true) } else { None },
        &["RUSTY_GRID_INSECURE"],
        master_cfg.as_ref().and_then(|m| m.insecure),
        false,
    );
    let secret_outcome = match secret_opts
        .resolve(config_file.as_ref(), SecretSection::Master)
        .and_then(|resolved| {
            let exposed = !server_config.bind_addr.ip().is_loopback() || enable_p2p;
            decide_master_secret(resolved, exposed, insecure, ClusterSecret::default_path())
        }) {
        Ok(outcome) => outcome,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    announce_master_secret(&secret_outcome, &listen_addr_str, insecure);
    if let Some(secret) = secret_outcome.secret() {
        server_config = server_config.with_cluster_secret(secret.clone());
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

    // Await termination signal
    let _ = tokio::signal::ctrl_c().await;
    println!("\nRustyGrid Master shutting down...");
    let _ = handle.shutdown();
    ExitCode::SUCCESS
}

/// Applies `--web-ui-addr` to the master's configuration.
///
/// Without the flag the dashboard keeps `ServerConfig`'s default, `127.0.0.1:8080`, whatever the
/// grid listens on: a master that listens on `0.0.0.0` for workers must not publish its dashboard
/// (plain HTTP) to the network as a side effect. Naming an address is the explicit opt-in.
fn apply_web_ui_addr(
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
fn dashboard_banner(
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

async fn run_worker(
    args: WorkerArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let worker_cfg = config_file.as_ref().and_then(|c| c.worker.clone());
    let hw_cfg = worker_cfg.as_ref().and_then(|w| w.hardware.clone());

    let master_addr = normalize_master_addr(&resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        worker_cfg.as_ref().and_then(|w| w.master.clone()),
        DEFAULT_MASTER_ADDR,
    ));

    if let Some(ref r_url) = args.rendezvous {
        std::env::set_var(rusty_grid_core::discovery::RENDEZVOUS_ENV, r_url);
    }

    let cluster_secret = match secret_opts.resolve(config_file.as_ref(), SecretSection::Worker) {
        Ok(secret) => secret,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let p2p_ticket = resolve_opt_string(
        args.p2p_ticket,
        &["RUSTY_GRID_P2P_TICKET"],
        worker_cfg.as_ref().and_then(|w| w.p2p_ticket.clone()),
    );

    // Fail closed: a worker obeys whatever its master sends, so without a secret it may only
    // follow a master on this machine. Remote, `auto`-discovered and P2P-ticket masters need the
    // cluster secret (to prove the master is genuine) unless --insecure is given explicitly.
    let insecure = peer_insecure(args.insecure, worker_cfg.as_ref().and_then(|w| w.insecure));
    let peer_outcome = match decide_peer_secret(
        cluster_secret.as_ref(),
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
    if let Some(secret) = cluster_secret.as_ref() {
        config = config.with_cluster_secret(secret.clone());
    }
    if let Some(ticket) = p2p_ticket {
        config = config.with_p2p_ticket(ticket);
    }
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
    if let Some(mc) = max_concurrency {
        config = config.with_max_concurrency(mc);
    }
    if let Some(sbd) = sandbox_base_dir {
        config = config.with_sandbox_base_dir(PathBuf::from(sbd));
    }
    if let Some(priority) = task_priority {
        config = config.with_process_priority(priority);
    }
    config = config.with_keep_sandboxes(keep_sandboxes);
    config = config.with_redirection_portal(!no_portal);
    // A restarted worker is the same machine, not a new one: keep its id (see `identity`), when it
    // knows which machine it is. Otherwise every unnamed worker under this HOME would share one
    // id and the master would keep closing the other's connection.
    if keep_worker_id {
        config = config.with_persistent_worker_id();
    }
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
    println!("Tags: {}", client.capabilities().tags.join(", "));
    for notice in client.gpu_notices() {
        match notice.level {
            NoticeLevel::Info => println!("GPU: {}", notice.message),
            NoticeLevel::Warn => eprintln!("Warning: {}", notice.message),
        }
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

/// Resolves when the worker is asked to stop: Ctrl-C, and the signals a service manager, `kill`
/// or the platform's stop mechanism send (SIGTERM; on Windows the console-close, break and
/// system-shutdown events). Without them a plain `kill` ends the worker on the spot and leaves
/// every running task process behind. SIGHUP is deliberately not handled, so `nohup` still works.
async fn wait_for_worker_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                match terminate.as_mut() {
                    Some(sigterm) => {
                        sigterm.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
    }

    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_close, ctrl_shutdown};
        let mut brk = ctrl_break().ok();
        let mut close = ctrl_close().ok();
        let mut shutdown = ctrl_shutdown().ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                match brk.as_mut() {
                    Some(event) => {
                        event.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
            _ = async {
                match close.as_mut() {
                    Some(event) => {
                        event.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
            _ = async {
                match shutdown.as_mut() {
                    Some(event) => {
                        event.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Sends one request on an authenticated connection and reads the single reply, printing a
/// clear message and returning the exit code on any failure (including an authentication
/// rejection from a Master that requires a secret this client does not have).
async fn exchange(
    transport: &mut MessageTransport<TcpStream>,
    master_addr: &str,
    had_secret: bool,
    request: &ClientMessage,
    send_failure: &str,
) -> Result<ClientResponse, ExitCode> {
    if let Err(e) = transport.send_msg(request).await {
        return Err(report_master_failure(
            master_addr,
            had_secret,
            send_failure,
            &GridError::from(e),
        ));
    }
    match transport.recv_msg::<ClientResponse>().await {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(report_master_failure(
            master_addr,
            had_secret,
            "Master closed the connection before replying",
            &GridError::ConnectionClosed,
        )),
        Err(e) => Err(report_master_failure(
            master_addr,
            had_secret,
            "Communication error with Master",
            &GridError::from(e),
        )),
    }
}

/// Resolves `--insecure` for a worker or client command: the flag, then `RUSTY_GRID_INSECURE`,
/// then the command's config-file key, else off.
fn peer_insecure(flag: bool, config_value: Option<bool>) -> bool {
    resolve_bool(
        if flag { Some(true) } else { None },
        &["RUSTY_GRID_INSECURE"],
        config_value,
        false,
    )
}

/// Resolves the cluster secret for a client command and applies the fail-closed policy for
/// `master_addr` (see [`decide_peer_secret`]), printing the error on failure: a configuration
/// problem exits with 1, a refusal to talk to an unauthenticated remote / `auto` master without
/// `--insecure` with the authentication exit code (6).
fn client_secret(
    secret_opts: &SecretOpts,
    config_file: Option<&ConfigFile>,
    section: SecretSection,
    master_addr: &str,
    insecure: bool,
) -> Result<Option<ClusterSecret>, ExitCode> {
    let secret = secret_opts.resolve(config_file, section).map_err(|e| {
        eprintln!("{e}");
        ExitCode::FAILURE
    })?;
    match decide_peer_secret(
        secret.as_ref(),
        master_addr,
        false,
        insecure,
        PeerRole::Client,
    ) {
        Ok(outcome) => {
            announce_peer_secret(outcome, PeerRole::Client, master_addr);
            Ok(secret)
        }
        Err(e) => {
            eprintln!("{e}");
            Err(ExitCode::from(AUTH_EXIT_CODE))
        }
    }
}

/// Largest file `submit --stdin-file` / `--script-file` sends: well below the 64 MiB frame limit
/// (`MAX_FRAME_SIZE`), which also has to carry the rest of the task.
const MAX_TASK_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// Validates that a binary buffer has a well-formed glTF 2.0 binary (`.glb`) header and JSON chunk 0.
pub fn validate_glb_header(raw: &[u8]) -> Result<(), String> {
    if raw.len() < 20 {
        return Err(format!(
            "Asset buffer too small ({} bytes < 20 bytes minimum for glTF 2.0 binary)",
            raw.len()
        ));
    }
    if &raw[0..4] != b"glTF" {
        return Err(format!(
            "Invalid glTF magic number: expected b\"glTF\", got {:?}",
            &raw[0..4]
        ));
    }
    let version = u32::from_le_bytes(raw[4..8].try_into().unwrap());
    if version != 2 {
        return Err(format!(
            "Unsupported glTF binary version: {version} (expected 2)"
        ));
    }
    let total_length = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
    if total_length > raw.len() || total_length < 20 {
        return Err(format!(
            "Corrupted glTF binary: header length ({total_length}) exceeds buffer ({})",
            raw.len()
        ));
    }
    let chunk0_length = u32::from_le_bytes(raw[12..16].try_into().unwrap()) as usize;
    if 20usize.saturating_add(chunk0_length) > raw.len() {
        return Err(format!(
            "Corrupted glTF chunk 0: length ({chunk0_length}) exceeds buffer ({})",
            raw.len()
        ));
    }
    let chunk0_type = u32::from_le_bytes(raw[16..20].try_into().unwrap());
    if chunk0_type != 0x4E4F534A {
        return Err(format!(
            "Invalid glTF chunk 0 type: 0x{chunk0_type:08X} (expected 0x4E4F534A 'JSON')"
        ));
    }
    let json_slice = &raw[20..20 + chunk0_length];
    let json_str = std::str::from_utf8(json_slice)
        .map_err(|e| format!("glTF chunk 0 is not valid UTF-8: {e}"))?;
    let json_val: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| format!("Failed to parse glTF chunk 0 JSON: {e}"))?;
    let asset_ver = json_val
        .get("asset")
        .and_then(|a| a.get("version"))
        .and_then(|v| v.as_str());
    if asset_ver != Some("2.0") {
        return Err(format!(
            "Invalid glTF asset version: {:?} (expected Some(\"2.0\"))",
            asset_ver
        ));
    }
    Ok(())
}

/// Builds the task spec `submit` sends from its flags, reading `--stdin-file` / `--script-file`;
/// an error names the flag at fault. Nothing is sent before this succeeds.
///
/// * `--command PROG [-- ARGS...]`: run `PROG` with exactly `ARGS` (possibly none).
/// * `-- PROG [ARGS...]` without `--command`: the first trailing token is the program.
/// * neither: the default demo `echo "Hello from RustyGrid"`.
///
/// An explicit program with no arguments runs with no arguments; the demo greeting is only a
/// default for a completely empty command line.
fn build_task_spec(args: &SubmitArgs) -> Result<TaskSpec, String> {
    let mut kind = args.task_type.to_lowercase();
    if (kind == "generic" || kind == "command" || kind.is_empty())
        && (args.prompt.is_some() || args.image.is_some())
    {
        kind = "3d".to_string();
    } else if (kind == "generic" || kind == "command" || kind.is_empty())
        && args.input_mesh.is_some()
    {
        kind = "autorig".to_string();
    }
    let is_command = !matches!(
        kind.as_str(),
        "gpu"
            | "compile"
            | "shell"
            | "3d"
            | "gen3d"
            | "3d_generate"
            | "generative_3d"
            | "autorig"
            | "auto_rig"
            | "rig"
    );
    let only_for = |flag: &str, kinds: &str| {
        Err(format!(
            "{flag} applies to {kinds} tasks, not to `--type {}`",
            args.task_type
        ))
    };
    if args.stdin_file.is_some() && !is_command {
        return only_for("--stdin-file", "command");
    }
    if kind != "shell" {
        if args.script_file.is_some() {
            return only_for("--script-file", "shell");
        }
        if args.interpreter.is_some() {
            return only_for("--interpreter", "shell");
        }
    }
    if !args.env.is_empty() && !is_command && kind != "shell" {
        return only_for("--env", "command and shell");
    }
    if kind != "gpu" {
        if args.matrix_dim.is_some() {
            return only_for("--matrix-dim", "gpu");
        }
        if args.work_group_size.is_some() {
            return only_for("--work-group-size", "gpu");
        }
    }
    let env = parse_env_pairs(&args.env)?;

    match kind.as_str() {
        "3d" | "gen3d" | "3d_generate" | "generative_3d" => {
            let prompt = match &args.prompt {
                Some(p) => {
                    let trimmed = p.trim();
                    if trimmed.is_empty() {
                        return Err("--prompt cannot be empty or whitespace".to_string());
                    }
                    Some(trimmed.to_string())
                }
                None => None,
            };
            let image_data = match &args.image {
                Some(img_path) => {
                    let meta = std::fs::metadata(img_path)
                        .map_err(|e| format!("--image {}: {e}", img_path.display()))?;
                    if !meta.is_file() {
                        return Err(format!(
                            "--image {}: path is not a regular file",
                            img_path.display()
                        ));
                    }
                    if meta.len() == 0 {
                        return Err(format!(
                            "--image {}: file is empty (0 bytes)",
                            img_path.display()
                        ));
                    }
                    let bytes = read_task_input(img_path, "--image")?;
                    Some(rusty_grid_core::task::Bytes::from(bytes))
                }
                None => None,
            };
            if prompt.is_none() && image_data.is_none() {
                return Err(
                    "3D generation requires either --prompt or --image".to_string(),
                );
            }
            let quality = args
                .quality
                .clone()
                .unwrap_or_else(|| "standard".to_string())
                .to_lowercase();
            let engine_preference = args
                .engine
                .as_ref()
                .map(|e| e.to_lowercase())
                .filter(|e| e != "auto");
            let export_texture = if args.no_export_texture {
                false
            } else if args.export_texture {
                true
            } else {
                true
            };
            let spec = TaskSpec::Generative3D(rusty_grid_core::task::Generative3DSpec {
                prompt,
                image_data,
                quality,
                engine_preference,
                export_texture,
            });
            spec.validate()?;
            Ok(spec)
        }
        "autorig" | "auto_rig" | "rig" => {
            let mesh_path = args
                .input_mesh
                .as_ref()
                .ok_or_else(|| "Auto-rigging requires --input-mesh <PATH>".to_string())?;
            let meta = std::fs::metadata(mesh_path)
                .map_err(|e| format!("--input-mesh {}: {e}", mesh_path.display()))?;
            if !meta.is_file() {
                return Err(format!(
                    "--input-mesh {}: path is not a regular file",
                    mesh_path.display()
                ));
            }
            if meta.len() == 0 {
                return Err(format!(
                    "--input-mesh {}: file is empty (0 bytes)",
                    mesh_path.display()
                ));
            }
            let bytes = read_task_input(mesh_path, "--input-mesh")?;
            let rig_preset = args
                .rig_preset
                .clone()
                .unwrap_or_else(|| "biped".to_string());
            let spec = TaskSpec::AutoRig(rusty_grid_core::task::AutoRigSpec {
                input_mesh: rusty_grid_core::task::Bytes::from(bytes),
                rig_preset,
                embed_animations: args.embed_animations.clone(),
                preferred_provider: args.preferred_provider.clone(),
            });
            spec.validate()?;
            Ok(spec)
        }
        "gpu" => {
            let dim = args.matrix_dim.unwrap_or(64);
            let tile = args.work_group_size.unwrap_or(16);
            if dim == 0 || tile == 0 {
                return Err("--matrix-dim and --work-group-size must be at least 1".to_string());
            }
            Ok(TaskSpec::GpuCompute {
                kernel_name: args
                    .command
                    .clone()
                    .unwrap_or_else(|| "matrix_multiply".into()),
                input_data: vec![1, 2, 3, 4],
                work_group_size: tile,
                simulated_matrix_dim: dim,
                compute_intensity: 10,
            })
        }
        "compile" => Err(
            "compile tasks need the crate's source files, which `submit` cannot send; they are \
             submitted through the agent-mesh bridge (`grid_compile`)"
                .to_string(),
        ),
        "shell" => {
            let script = match &args.script_file {
                Some(path) => {
                    if args.command.is_some() || !args.args.is_empty() {
                        return Err(
                            "--script-file cannot be combined with --command or words after --"
                                .to_string(),
                        );
                    }
                    String::from_utf8(read_task_input(path, "--script-file")?).map_err(|_| {
                        format!(
                            "--script-file {}: the file is not UTF-8 text",
                            path.display()
                        )
                    })?
                }
                None => match (&args.command, args.args.is_empty()) {
                    // `--command 'ls' -- -la`: the trailing words extend the script instead of
                    // being dropped.
                    (Some(script), _) => std::iter::once(script.as_str())
                        .chain(args.args.iter().map(String::as_str))
                        .collect::<Vec<_>>()
                        .join(" "),
                    (None, false) => args.args.join(" "),
                    (None, true) => "echo 'Hello from shell task'".into(),
                },
            };
            Ok(TaskSpec::ShellScript {
                script,
                interpreter: args.interpreter.clone(),
                env,
            })
        }
        _ => {
            let (program, cmd_args) = match (&args.command, args.args.split_first()) {
                (Some(program), _) => (program.clone(), args.args.clone()),
                (None, Some((program, rest))) => (program.clone(), rest.to_vec()),
                (None, None) => ("echo".to_string(), vec!["Hello from RustyGrid".to_string()]),
            };
            let stdin = match &args.stdin_file {
                Some(path) => Some(read_task_input(path, "--stdin-file")?),
                None => None,
            };
            Ok(TaskSpec::Command {
                program,
                args: cmd_args,
                env,
                working_dir: None,
                stdin,
            })
        }
    }
}

/// Reads a file `submit` sends with the task, refusing one above [`MAX_TASK_INPUT_BYTES`].
fn read_task_input(path: &Path, flag: &str) -> Result<Vec<u8>, String> {
    let len = std::fs::metadata(path)
        .map_err(|e| format!("{flag} {}: {e}", path.display()))?
        .len();
    if len > MAX_TASK_INPUT_BYTES {
        return Err(format!(
            "{flag} {}: {len} bytes, more than the {} MiB a task may carry",
            path.display(),
            MAX_TASK_INPUT_BYTES / (1024 * 1024)
        ));
    }
    std::fs::read(path).map_err(|e| format!("{flag} {}: {e}", path.display()))
}

/// `--env KEY=VALUE` pairs as a map; the first `=` ends the key, which must not be empty.
fn parse_env_pairs(pairs: &[String]) -> Result<HashMap<String, String>, String> {
    let mut env = HashMap::new();
    for pair in pairs {
        match pair.split_once('=') {
            Some((key, value)) if !key.is_empty() => {
                env.insert(key.to_string(), value.to_string());
            }
            _ => return Err(format!("--env {pair}: expected KEY=VALUE")),
        }
    }
    Ok(env)
}

/// The placement constraints of `submit --os / --arch / --worker / --require-tag`, checked and
/// normalised exactly as the master will read them.
fn build_placement(args: &SubmitArgs) -> Result<Placement, String> {
    let mut tags = Vec::new();
    if let Some(os) = &args.os {
        tags.push(format!("{REQUIRE_PREFIX}os={os}"));
    }
    if let Some(arch) = &args.arch {
        tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
    }
    if let Some(worker) = &args.worker {
        tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
    }
    for tag in &args.require_tags {
        tags.push(format!("{REQUIRE_PREFIX}tag={tag}"));
    }
    Placement::from_task_tags(&tags)
}

/// The master's worker list, each entry tagged by the master with `id:` / `state:` / `link:`
/// (see `rusty_grid_core::placement`). `None` when the master did not answer with a list.
async fn fetch_workers(
    transport: &mut MessageTransport<TcpStream>,
    master_addr: &str,
    had_secret: bool,
) -> Option<Vec<WorkerCapabilities>> {
    match exchange(
        transport,
        master_addr,
        had_secret,
        &ClientMessage::ListWorkers,
        "Failed to list the workers",
    )
    .await
    {
        Ok(ClientResponse::WorkerList { workers }) => Some(workers),
        _ => None,
    }
}

/// Whether a listed worker is connected (the master marks the others `state:disconnected`).
fn listed_as_connected(caps: &WorkerCapabilities) -> bool {
    tag_value(&caps.tags, STATE_TAG_PREFIX) != Some("disconnected")
}

/// The name of the listed worker with this id, if the list has it.
fn listed_worker_name(workers: &[WorkerCapabilities], worker_id: Uuid) -> Option<String> {
    workers
        .iter()
        .find(|w| listed_worker_id(w) == Some(worker_id))
        .map(|w| w.name.clone())
}

/// The log filter when `RUST_LOG` is not set: `level` for everything, except the wgpu crates, which
/// log every device poll at INFO (dozens of lines per GPU task) and stay at WARN.
fn default_log_filter(level: &str) -> String {
    format!("{level},wgpu_core=warn,wgpu_hal=warn,naga=warn")
}

/// `doctor`: resolves the master and the secret like `submit`, runs [`crate::doctor`] and maps the
/// report to an exit code (0 all passed, 1 a check failed, 6 authentication failed).
async fn run_doctor_command(
    args: DoctorArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master.clone(),
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        submit_cfg.as_ref().and_then(|s| s.master.clone()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &master_addr,
        peer_insecure(args.insecure, submit_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let options = DoctorOptions {
        expect_workers: args.expect_workers,
        probe_timeout: Duration::from_secs(args.probe_timeout_secs.max(1)),
        gpu_dim: args.gpu_dim.max(1),
        spread_tasks: args.spread_tasks,
        skip_gpu: args.skip_gpu,
        skip_spread: args.skip_spread,
    };
    let report = run_doctor(&master_addr, secret.as_ref(), &options).await;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).unwrap_or_default()
        );
    } else {
        print!("{}", report.render());
    }
    if report.auth_failed {
        ExitCode::from(AUTH_EXIT_CODE)
    } else if report.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Public entry point for programmatic/test submission with default secret options.
pub async fn run_submit(args: SubmitArgs, config_file: Option<ConfigFile>) -> ExitCode {
    run_submit_with_secret(args, config_file, SecretOpts::default()).await
}

async fn run_submit_with_secret(
    args: SubmitArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    // Everything wrong with the arguments is reported before connecting.
    let (spec, placement) = match build_task_spec(&args).and_then(|spec| {
        let placement = build_placement(&args)?;
        Ok((spec, placement))
    }) {
        Ok(built) => built,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let is_asset_task = matches!(
        spec,
        TaskSpec::Generative3D(_) | TaskSpec::AutoRig(_)
    );

    let staged_files = if !args.files.is_empty() {
        match crate::connect::prepare_staged_files(&args.files) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        Vec::new()
    };

    let expected_outputs = if !args.outputs.is_empty() && !is_asset_task {
        match crate::connect::validate_output_paths(&args.outputs) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        Vec::new()
    };

    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master.clone(),
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        submit_cfg.as_ref().and_then(|s| s.master.clone()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &master_addr,
        peer_insecure(args.insecure, submit_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            )
        }
    };

    let had_secret = secret.is_some();
    // A constrained task nobody can take would wait silently; say so (it is still submitted, a
    // matching worker may connect later).
    if !placement.is_empty() {
        if let Some(workers) = fetch_workers(&mut transport, &master_addr, had_secret).await {
            if !workers
                .iter()
                .any(|w| listed_as_connected(w) && placement.matches_listed(w))
            {
                eprintln!(
                    "Warning: no connected worker matches the placement constraints ({placement}); the task waits in the queue until one does"
                );
            }
        }
    }

    let is_gpu = args.gpu || args.task_type.eq_ignore_ascii_case("gpu");
    let requirements = TaskRequirements {
        cpu_cores: args.cores.max(1),
        ram_mb: args.ram_mb,
        gpu_required: is_gpu,
        timeout_secs: args.timeout_secs.max(1),
        max_retries: args.max_retries,
    };

    let mut task = Task::new(spec, requirements).with_placement(&placement);
    task.staged_files = staged_files;
    task.expected_outputs = expected_outputs.clone();
    let wait = args.wait || args.output.is_some() || !expected_outputs.is_empty();
    let submit_msg = ClientMessage::SubmitTask { task, wait };

    let response = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &submit_msg,
        "Failed to send task to Master",
    )
    .await
    {
        Ok(response) => response,
        Err(code) => return code,
    };

    match response {
        ClientResponse::TaskSubmitted { task_id } => {
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
        ClientResponse::TaskCompleted { task_id, result } => {
            let worker_name = fetch_workers(&mut transport, &master_addr, had_secret)
                .await
                .and_then(|workers| listed_worker_name(&workers, result.worker_id));

            let asset_output_path = args.output.clone().or_else(|| {
                if is_asset_task && args.outputs.len() == 1 {
                    Some(PathBuf::from(&args.outputs[0]))
                } else {
                    None
                }
            });

            if let (Some(ref out_path), Some(ref asset_bytes)) =
                (&asset_output_path, &result.asset_bytes)
            {
                if let Err(e) = validate_glb_header(asset_bytes.as_slice()) {
                    eprintln!("Asset validation failed before writing to disk: {e}");
                    return ExitCode::FAILURE;
                }
                if let Some(parent) = out_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            eprintln!(
                                "Failed to create output directory '{}': {e}",
                                parent.display()
                            );
                            return ExitCode::FAILURE;
                        }
                    }
                }
                if let Err(e) = std::fs::write(out_path, asset_bytes.as_slice()) {
                    eprintln!("Failed to write asset to '{}': {e}", out_path.display());
                    return ExitCode::FAILURE;
                }
            }

            let output_dir = args.output_dir.as_deref().unwrap_or(Path::new("."));
            let saved_artifacts = if !result.artifacts.is_empty() {
                match crate::connect::save_output_artifacts(&result.artifacts, output_dir) {
                    Ok(saved) => Some(saved),
                    Err(e) => {
                        eprintln!("Failed to save output artifacts: {e}");
                        None
                    }
                }
            } else {
                None
            };

            if args.json {
                let mut value = serde_json::to_value(&result).unwrap_or_default();
                if let Some(obj) = value.as_object_mut() {
                    obj.insert("worker_name".to_string(), serde_json::json!(worker_name));
                    if let Some(ref saved) = saved_artifacts {
                        let saved_strs: Vec<String> =
                            saved.iter().map(|p| p.display().to_string()).collect();
                        obj.insert("saved_artifacts".to_string(), serde_json::json!(saved_strs));
                    }
                }
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            } else {
                println!("Task {task_id} completed successfully.");
                match &worker_name {
                    Some(name) => println!("Worker: {name} ({})", result.worker_id),
                    None => println!("Worker: {}", result.worker_id),
                }
                println!("Exit Code: {}", result.exit_code);
                println!("Execution Time: {}ms", result.execution_time_ms);
                if let Some(ref saved) = saved_artifacts {
                    println!(
                        "Artifacts ({} saved to {}):",
                        saved.len(),
                        output_dir.display()
                    );
                    for (artifact, path) in result.artifacts.iter().zip(saved.iter()) {
                        println!(
                            "  - {} ({} bytes, sha256: {}) -> {}",
                            artifact.path,
                            artifact.size,
                            artifact.sha256_hex(),
                            path.display()
                        );
                    }
                }
                if !result.stdout.is_empty() {
                    println!("Stdout:\n{}", result.stdout_str().trim());
                }
                if !result.stderr.is_empty() {
                    eprintln!("Stderr:\n{}", result.stderr_str().trim());
                }
            }
            if result.exit_code == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(result.exit_code.min(255) as u8)
            }
        }
        ClientResponse::Error { message } => {
            if args.json {
                println!("{}", serde_json::json!({"error": message}));
            } else {
                eprintln!("Master error: {message}");
            }
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected master response: {other:?}");
            ExitCode::FAILURE
        }
    }
}

async fn run_status(
    args: StatusArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        status_cfg.as_ref().and_then(|s| s.master.clone()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Status,
        &master_addr,
        peer_insecure(args.insecure, status_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            )
        }
    };
    let had_secret = secret.is_some();

    if let Some(task_uuid) = args.task_id {
        let task_id = TaskId(task_uuid);
        let msg = ClientMessage::GetTaskStatus { task_id };
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to query task status",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };
        match response {
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => {
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
            ClientResponse::Error { message } => {
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
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to list workers",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };
        match response {
            ClientResponse::WorkerList { workers } => {
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
                        serde_json::to_string_pretty(&enriched_workers).unwrap()
                    );
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
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to get cluster status",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };
        match response {
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                workers,
            } => {
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

async fn run_workers(
    args: WorkersArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_args = StatusArgs {
        master: args.master,
        task_id: None,
        workers: true,
        json: args.json,
        insecure: args.insecure,
    };
    run_status(status_args, config_file, secret_opts).await
}

async fn run_mapreduce(
    args: MapReduceArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if args.chunks == 0 {
        eprintln!("--chunks must be at least 1");
        return ExitCode::FAILURE;
    }
    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master,
        &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"],
        submit_cfg.as_ref().and_then(|s| s.master.clone()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &master_addr,
        peer_insecure(args.insecure, submit_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            )
        }
    };

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
    let response = match exchange(
        &mut transport,
        &master_addr,
        secret.is_some(),
        &msg,
        "Failed to dispatch MapReduce job to Master",
    )
    .await
    {
        Ok(response) => response,
        Err(code) => return code,
    };

    match response {
        ClientResponse::MapReduceCompleted { result } => {
            // The master answers a job that failed (a mapper exited non-zero, output was cut off,
            // a reducer failed) with a result whose status is not "Completed": report it as a failure.
            let succeeded = result.status == "Completed";
            if args.json {
                println!("{}", serde_json::to_string_pretty(&result).unwrap());
            } else if !succeeded {
                eprintln!("=== MapReduce Job FAILED ===");
                eprintln!("Job ID:           {}", result.job_id);
                eprintln!("Status:           {}", result.status);
                eprintln!(
                    "Map Tasks:        {}/{}",
                    result.map_tasks_completed, result.map_tasks_total
                );
                eprintln!(
                    "Reduce Tasks:     {}/{}",
                    result.reduce_tasks_completed, result.reduce_tasks_total
                );
                if let Some(error) = &result.error {
                    eprintln!("Error:            {error}");
                }
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
            if succeeded {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        ClientResponse::Error { message } => {
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
    use crate::secret_cmd::SecretCliArgs;
    use rusty_grid_core::WorkerCapabilities;

    fn parse(args: &[&str]) -> Cli {
        let mut argv = vec!["oxideswarm"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv).expect("arguments must parse")
    }

    fn submit_args(args: &[&str]) -> SubmitArgs {
        match parse(&[&["submit"], args].concat()).command {
            Commands::Submit(a) => a,
            other => panic!("expected submit, got {other:?}"),
        }
    }

    fn command_of(spec: Result<TaskSpec, String>) -> (String, Vec<String>) {
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
        let err = build_placement(&submit_args(&["--require-tag", "os:macos", "--", "true"]))
            .unwrap_err();
        assert!(err.contains("only the worker or the master"), "{err}");
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
                "agent-mesh bridge",
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
                assert!(!a.skip_gpu && !a.skip_spread && !a.json);
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
            "--json",
        ])
        .command
        {
            Commands::Doctor(a) => {
                assert_eq!(a.master.as_deref(), Some("10.0.0.7"));
                assert_eq!(a.expect_workers, 2);
                assert_eq!(a.spread_tasks, Some(16));
                assert!(a.skip_gpu && a.json);
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
}
