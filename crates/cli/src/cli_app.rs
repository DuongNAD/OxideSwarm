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
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::GridError;
use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::placement::{
    listed_supports_workspaces, listed_worker_id, tag_value, validate_user_tag, IsolationLevel,
    Placement, REQUIRE_PREFIX, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport, WireCodec};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::WorkerCapabilities;
use rusty_grid_master::{MasterServer, ReaperConfig, SchedulerConfig, ServerConfig};
use rusty_grid_worker::{NoticeLevel, ProcessPriority, WorkerClient, WorkerConfig};

use crate::config::{
    load_config_file, resolve_bool, resolve_f32, resolve_opt_field, resolve_opt_string,
    resolve_opt_u16, resolve_opt_u64, resolve_opt_usize, resolve_string, resolve_u32, resolve_u64,
    resolve_usize, ConfigFile, MasterFileConfig, SecretSection, SecretText, CLIENT_MASTER_ENV_VARS,
};
use crate::connect::{
    connect_master, normalize_master_addr, report_master_failure, AUTH_EXIT_CODE,
    DEFAULT_MASTER_ADDR,
};
use crate::doctor::{run_doctor, DoctorOptions};
use crate::mode_cli;
use crate::secret_cmd::{
    announce_master_secret, announce_peer_secret, client_secret, decide_master_secret,
    peer_insecure, run_secret, PeerRole, PeerSecretOutcome, SecretCliArgs, SecretOpts,
};

/// Bytes in a MiB, for the `*-mb` options.
const MIB: u64 = 1024 * 1024;

/// Help text of `--insecure` on the commands that connect to a master (worker and clients).
const INSECURE_PEER_HELP: &str = "Continue WITHOUT a cluster secret although the master is remote, found by 'auto' discovery, or reached through a P2P ticket. The master is then not authenticated: a rogue host can run commands on a worker or receive what a client submits. Without this flag such a connection is refused when no secret is configured.";

/// Help text of `--relay-url` across master, worker, and client commands.
const RELAY_URL_HELP: &str = "Custom iroh relay URL (https://...) to use instead of n0's public relays (env RUSTY_GRID_RELAY_URL, config relay_url). Cannot be combined with RUSTY_GRID_P2P_MINIMAL.";

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

    #[arg(
        global = true,
        long = "master-key",
        value_name = "HEX",
        help = "Pinned Master OXS-AUTH v3 X25519 public key as 64 hex characters (env RUSTY_GRID_MASTER_KEY, config master_key)"
    )]
    pub master_key: Option<String>,

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
#[allow(clippy::large_enum_variant)]
pub enum Commands {
    /// Start master coordinator node
    Master(MasterArgs),

    /// Start worker compute node
    Worker(WorkerArgs),

    /// Submit a task to the grid cluster
    Submit(SubmitArgs),

    /// Query cluster status or task execution state
    Status(StatusArgs),

    /// Cancel a queued or running task by ID
    Cancel(CancelArgs),

    /// Fetch the result and outputs of a task by ID
    Result(ResultArgs),

    /// List active worker nodes in the grid
    Workers(WorkersArgs),

    /// List the master's workspace affinity map (does not list workspaces stored on workers)
    Workspaces(WorkspacesArgs),

    /// Execute a distributed Map/Reduce job
    Mapreduce(MapReduceArgs),

    /// Check a running cluster end to end: connection, workers, pinned probes, GPUs, spreading
    Doctor(DoctorArgs),

    /// Generate or show the shared cluster secret, or create a join token for another machine
    Secret(SecretCliArgs),

    /// Join this machine to a cluster with a join token read from standard input
    Join(crate::join::JoinArgs),

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

    #[arg(long, help = "Maximum queued or running tasks allowed at once")]
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

    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        help = "Immediately fail tasks whose capabilities or placement constraints no registered worker can satisfy"
    )]
    pub fail_unfulfillable_tasks: Option<bool>,

    #[arg(long, help = "Enable native P2P QUIC endpoint for NAT traversal")]
    pub p2p: bool,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

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

    #[arg(long, help = "Wire format codec: bincode or json (default: bincode)")]
    pub wire_codec: Option<String>,

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
        long = "auth",
        value_name = "MODE",
        help = "Peer authentication mode: secret (OXS-AUTH v2 shared secret only), members (OXS-AUTH v3 enrolled machines only), or both (default; env RUSTY_GRID_AUTH, config [master].auth)"
    )]
    pub auth: Option<String>,

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
        value_name = "MB",
        help = "Megabytes of active (queued, scheduled, running, retrying) task payloads the master queue holds at once (default 256; env RUSTY_GRID_MAX_ACTIVE_MB)"
    )]
    pub max_active_mb: Option<u64>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Directory of the blob store that keeps task input and output files (default ~/.oxideswarm/blobs, or <STATE_DIR>/blobs when --state-dir is set; env RUSTY_GRID_BLOB_DIR)"
    )]
    pub blob_dir: Option<PathBuf>,

    #[arg(
        long,
        value_name = "MB",
        help = "Megabytes the blob store may hold before it evicts the least recently used files no queued or running task needs (default 4096; env RUSTY_GRID_BLOB_STORE_MB)"
    )]
    pub blob_store_mb: Option<u64>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Directory for durable task queue persistence (store.lock, snapshot.bin, journal.bin; env RUSTY_GRID_STATE_DIR)"
    )]
    pub state_dir: Option<PathBuf>,

    #[arg(
        long,
        value_name = "SECS",
        help = "Seconds the master holds a disconnected session-resume worker's tasks before requeuing or failing them (CLI default 30 on every master, even without --state-dir; 0 disables the hold for immediate failover; max 3600; env RUSTY_GRID_SESSION_RESUME_HOLD_SECS)"
    )]
    pub session_resume_hold_secs: Option<u64>,

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

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(
        long,
        help = "Human-readable worker node name override. A named worker also keeps the same worker id across restarts (~/.oxideswarm/worker_id.<NAME>, or the file in RUSTY_GRID_WORKER_ID_FILE): give every worker that shares a home directory its own name. Without a name the id is new at each start"
    )]
    pub name: Option<String>,

    #[arg(
        long = "tag",
        value_name = "TAG",
        help = "User tag advertised to the master, matched by `submit --require-tag` (repeatable; env RUSTY_GRID_WORKER_TAGS, comma-separated). The prefixes os:, arch:, slots:, link:, id:, state:, iso:, tool: and feat: are reserved"
    )]
    pub tags: Vec<String>,

    #[arg(
        long,
        value_name = "LEVEL",
        help = "Confine task processes at the OS level: off (default), fs (a task writes only its own sandbox and cannot read the owner's home, other sandboxes or the blob cache) or fs+net (fs plus no network sockets). The worker refuses to start where the level cannot be enforced (macOS only for now); advertised as the iso:<level> tag; env RUSTY_GRID_ISOLATION, config [worker].isolation"
    )]
    pub isolation: Option<String>,

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
        value_name = "SECS",
        help = "Optional cap in seconds on how long the worker keeps running tasks alive across a disconnect (0 disables session resume; default uses the master's advertised window bounded by the dead-peer inequality; env RUSTY_GRID_SESSION_RESUME_GRACE_SECS)"
    )]
    pub session_resume_grace_secs: Option<u64>,

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
        value_name = "MB",
        help = "Megabytes of task input and output files the worker caches in <sandbox base dir>/cache/blobs, least recently used evicted first (default 2048; env RUSTY_GRID_BLOB_CACHE_MB)"
    )]
    pub blob_cache_mb: Option<u64>,

    #[arg(
        long,
        value_name = "MB",
        help = "Megabytes of output files (submit --output) one task may bring back; more fails the task (default 1024; env RUSTY_GRID_OUTPUT_LIMIT_MB)"
    )]
    pub output_limit_mb: Option<u64>,

    #[arg(
        long,
        help = "Enable persistent named workspaces on this worker (off by default; opens the workspace store at start-up and advertises ProtocolCapabilities::WORKSPACES / feat:workspaces; env RUSTY_GRID_WORKSPACES, config [worker].workspaces)"
    )]
    pub workspaces: bool,

    #[arg(
        long,
        value_name = "PATH",
        help = "Root directory for persistent named workspaces (default: <cache dir>/oxideswarm/workspaces; env RUSTY_GRID_WORKSPACE_ROOT, config [worker].workspace_root)"
    )]
    pub workspace_root: Option<PathBuf>,

    #[arg(
        long,
        value_name = "N",
        help = "Per-workspace disk quota in MiB; exceeding it prints a warning on task stderr and marks the workspace first to evict (default 20480; env RUSTY_GRID_WORKSPACE_LIMIT_MB, config [worker].workspace_limit_mb)"
    )]
    pub workspace_limit_mb: Option<u64>,

    #[arg(
        long,
        value_name = "N",
        help = "Maximum number of persistent workspaces kept on disk before idle LRU eviction (default 8; env RUSTY_GRID_MAX_WORKSPACES, config [worker].max_workspaces)"
    )]
    pub max_workspaces: Option<usize>,

    #[arg(
        long,
        help = "Do not serve the standby portal (HTTP on 0.0.0.0:8080 that sends a browser to the master's dashboard); env RUSTY_GRID_NO_PORTAL. Useful for a second worker on one machine"
    )]
    pub no_portal: bool,

    #[arg(long, help = "Wire format codec: bincode or json (default: bincode)")]
    pub wire_codec: Option<String>,

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

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(
        long = "type",
        alias = "task-type",
        default_value = "generic",
        help = "Task type: command (default, also `generic`), shell, compile, gpu, 3d, or autorig"
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

    #[arg(long, help = "Task execution timeout in seconds (default: 60)")]
    pub timeout_secs: Option<u64>,

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
        help = "Preferred 3D generation engine: auto, sdf, triposr (default: auto)"
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
        long = "require-isolation",
        value_name = "LEVEL",
        help = "Run only on a worker confined at least at this level: fs or fs+net (`worker --isolation`)"
    )]
    pub require_isolation: Option<String>,

    #[arg(
        long = "require-tag",
        value_name = "TAG",
        help = "Run only on a worker that carries this user tag (`worker --tag`); repeatable, all must match"
    )]
    pub require_tags: Vec<String>,

    #[arg(
        long = "require-tool",
        value_name = "TOOL",
        help = "Run only on a worker that advertises this host tool (`tool:<name>`, e.g. cargo, rustc, git); repeatable, all must match"
    )]
    pub require_tools: Vec<String>,

    #[arg(
        long = "source",
        visible_alias = "source-dir",
        value_name = "PATH[:SANDBOX]",
        help = "Compile tasks: Rust source file or Cargo crate directory to compile (repeatable)"
    )]
    pub sources: Vec<String>,

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
        long = "input",
        visible_alias = "file",
        value_name = "LOCAL[:SANDBOX]",
        help = "Command, shell and compile tasks: place a local file or directory in the task sandbox (repeatable; LOCAL:. puts a directory's content at the sandbox root). Content is deduplicated: files the master already holds are not sent again"
    )]
    pub inputs: Vec<String>,

    #[arg(
        long = "output",
        alias = "expected-output",
        value_name = "PATTERN",
        help = "Bring back the sandbox files this pattern selects after the run (repeatable): a path, a directory (all files below it), or a pattern with * ? and ** such as 'target/release/*' or '**/*.log'. Implies --wait"
    )]
    pub outputs: Vec<String>,

    #[arg(
        long = "output-dir",
        value_name = "DIR",
        help = "Local directory the output files are written to, at their sandbox paths (default: current directory)"
    )]
    pub output_dir: Option<PathBuf>,

    #[arg(
        long = "workspace",
        value_name = "NAME",
        help = "Command and shell tasks: run in the named persistent workspace on a worker; syncs the current directory before the run unless --sync or --no-sync is given"
    )]
    pub workspace: Option<String>,

    #[arg(
        long = "sync",
        value_name = "DIR",
        help = "Workspace tasks: sync DIR into the workspace before the run instead of the current directory"
    )]
    pub sync: Option<PathBuf>,

    #[arg(
        long = "no-sync",
        help = "Workspace tasks: run in the workspace as it is, without syncing a local directory"
    )]
    pub no_sync: bool,

    #[arg(
        long = "cwd",
        value_name = "REL",
        help = "Workspace command tasks: working directory inside the workspace for --command"
    )]
    pub cwd: Option<String>,

    #[arg(
        long = "workspace-max-files",
        value_name = "N",
        help = "Workspace tasks: maximum files one workspace sync may scan (default: 100000)"
    )]
    pub workspace_max_files: Option<usize>,

    #[arg(
        long = "workspace-max-mb",
        value_name = "MB",
        help = "Workspace tasks: maximum megabytes one workspace sync may scan (default: 2048)"
    )]
    pub workspace_max_mb: Option<u64>,

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

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

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
pub struct CancelArgs {
    #[arg(value_name = "ID", help = "Task UUID to cancel")]
    pub task_id: Uuid,

    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct ResultArgs {
    #[arg(value_name = "ID", help = "Task UUID whose result to fetch")]
    pub task_id: Uuid,

    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(
        short,
        long,
        help = "Wait (poll) until the task reaches a terminal state"
    )]
    pub wait: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(
        long = "output-dir",
        value_name = "DIR",
        help = "Local directory the output files are written to, at their sandbox paths (default: current directory)"
    )]
    pub output_dir: Option<PathBuf>,

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

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct WorkspacesArgs {
    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN; a P2P ticket dials over QUIC"
    )]
    pub master: Option<String>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

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

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(long, required = true, help = "Input data files or text datasets")]
    pub input: Vec<String>,

    #[arg(
        long,
        default_value = "word_count",
        conflicts_with = "map_cmd",
        help = "Mapper built-in name (word_count, line_count, identity) or path to a local shell script"
    )]
    pub mapper: String,

    #[arg(
        long,
        conflicts_with = "mapper",
        help = "Command line to run as the mapper (receives chunk on stdin, writes key/value lines to stdout; quote UNC paths with single quotes or doubled backslashes, e.g. '\\\\srv\\share\\m.exe' or \\\\\\\\srv\\\\share\\\\m.exe)"
    )]
    pub map_cmd: Option<String>,

    #[arg(
        long,
        default_value = "sum",
        conflicts_with = "reduce_cmd",
        help = "Reducer built-in name (sum, count, max, min, concat) or path to a local shell script"
    )]
    pub reducer: String,

    #[arg(
        long,
        conflicts_with = "reducer",
        help = "Command line to run as the reducer (receives grouped JSON on stdin, writes reduced value to stdout; quote UNC paths with single quotes or doubled backslashes, e.g. '\\\\srv\\share\\r.exe' or \\\\\\\\srv\\\\share\\\\r.exe)"
    )]
    pub reduce_cmd: Option<String>,

    #[arg(
        long,
        default_value = "4",
        help = "Number of input chunks to partition dataset into"
    )]
    pub chunks: usize,

    #[arg(
        long,
        help = "Timeout in seconds for Map/Reduce execution (default: 60)"
    )]
    pub timeout_secs: Option<u64>,

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

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

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

    #[arg(
        long,
        help = "Leave out the blob throughput probe (C8), which moves an 8 MiB blob to every worker"
    )]
    pub skip_throughput: bool,

    #[arg(long, help = "Print the report as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

pub async fn run_cli() -> ExitCode {
    run_cli_with(Cli::parse()).await
}

pub async fn run_cli_with(cli: Cli) -> ExitCode {
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
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    let is_mcp = matches!(cli.command, Commands::Mcp(_));
    let _ = if is_mcp {
        builder.with_writer(std::io::stderr).try_init()
    } else {
        builder
            .with_writer(
                std::io::stderr
                    .with_max_level(tracing::Level::WARN)
                    .or_else(std::io::stdout),
            )
            .try_init()
    };

    let config_file = match if is_mcp {
        crate::config::load_mcp_config_file(cli.config.as_deref())
    } else {
        load_config_file(cli.config.as_deref())
    } {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let secret_opts = cli.secret_opts();
    crate::connect::set_cli_master_key_flag(cli.master_key.clone());
    rusty_grid_core::mode::set_mode_runner_hook(crate::mode_cli::execute_mode_action);
    match cli.command {
        Commands::Master(args) => run_master(args, config_file, secret_opts).await,
        Commands::Worker(args) => run_worker(args, config_file, secret_opts).await,
        Commands::Submit(args) => run_submit_with_secret(args, config_file, secret_opts).await,
        Commands::Status(args) => run_status(args, config_file, secret_opts).await,
        Commands::Cancel(args) => run_cancel(args, config_file, secret_opts).await,
        Commands::Result(args) => run_result(args, config_file, secret_opts).await,
        Commands::Workers(args) => run_workers(args, config_file, secret_opts).await,
        Commands::Workspaces(args) => run_workspaces(args, config_file, secret_opts).await,
        Commands::Mapreduce(args) => run_mapreduce(args, config_file, secret_opts).await,
        Commands::Doctor(args) => run_doctor_command(args, config_file, secret_opts).await,
        Commands::Secret(args) => run_secret(args, &secret_opts, config_file.as_ref()),
        Commands::Join(args) => {
            crate::join::run_join(args, &secret_opts, config_file.as_ref()).await
        }
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

/// Resolve `--max-active-mb` / `RUSTY_GRID_MAX_ACTIVE_MB` / `[master].max_active_mb`
/// and apply it to `ServerConfig`, refusing `0` because a 0-byte active cap rejects
/// every task.
fn apply_max_active_mb(
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
fn apply_max_queue_size(
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

/// Resolve a string option across CLI > Env > Config while keeping the name of the source
/// (`flag_label`, `env_name`, or `cfg_label`) that supplied the value.
fn resolve_sourced_string<'a>(
    flag_label: &'a str,
    flag: Option<String>,
    env_name: &'a str,
    cfg_label: &'a str,
    cfg_val: Option<String>,
) -> Option<(&'a str, String)> {
    if let Some(v) = flag {
        return Some((flag_label, v));
    }
    if let Ok(val_str) = std::env::var(env_name) {
        let trimmed = val_str.trim();
        if !trimmed.is_empty() {
            return Some((env_name, trimmed.to_string()));
        }
    }
    cfg_val.map(|v| (cfg_label, v))
}

/// Resolve `--scheduler-policy` / `RUSTY_GRID_SCHEDULER_POLICY` / `[master].scheduler_policy`,
/// returning an error prefixed with the source name on unknown policy names.
fn resolve_scheduler_policy(
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
fn resolve_fail_unfulfillable_tasks(
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

/// Build the `SchedulerConfig` for `oxideswarm master`, resolving `--scheduler-policy` /
/// `RUSTY_GRID_SCHEDULER_POLICY` / `[master].scheduler_policy` and carrying `preserve_gpu`,
/// `max_host_cpu`, and `fail_unfulfillable_tasks`.
fn build_master_scheduler_config(
    flag: Option<String>,
    cfg_val: Option<String>,
    preserve_gpu: bool,
    max_host_cpu: f32,
    fail_unfulfillable_tasks: bool,
) -> Result<SchedulerConfig, String> {
    let scheduler_policy = resolve_scheduler_policy(flag, cfg_val)?;
    Ok(SchedulerConfig {
        policy: scheduler_policy,
        preserve_gpu_for_gpu_tasks: preserve_gpu,
        max_host_cpu_pct: max_host_cpu,
        fail_unfulfillable_tasks,
        ..Default::default()
    })
}

/// Resolve `--wire-codec` / `RUSTY_GRID_WIRE_CODEC` / config `wire_codec`,
/// returning an error prefixed with the source name on unknown codec names.
fn resolve_wire_codec(flag: Option<String>, cfg_val: Option<String>) -> Result<WireCodec, String> {
    match resolve_sourced_string(
        "--wire-codec",
        flag,
        "RUSTY_GRID_WIRE_CODEC",
        "wire_codec",
        cfg_val,
    ) {
        Some((source, raw)) => raw.parse().map_err(|e| format!("{source}: {e}")),
        None => Ok(WireCodec::Bincode),
    }
}

/// Environment variable selecting the Master's peer authentication mode (`secret|members|both`).
pub const AUTH_MODE_ENV: &str = "RUSTY_GRID_AUTH";

/// Resolve `master --auth` > `RUSTY_GRID_AUTH` > `[master].auth` > `AuthMode::Both` without
/// touching the process environment.
pub(crate) fn resolve_auth_mode_from(
    cli_val: Option<String>,
    env_val: Option<&str>,
    cfg_val: Option<String>,
) -> Result<rusty_grid_master::server::AuthMode, String> {
    let sourced = if let Some(v) = cli_val {
        Some(("--auth", v))
    } else if let Some(v) = env_val.map(str::trim).filter(|s| !s.is_empty()) {
        Some((AUTH_MODE_ENV, v.to_string()))
    } else {
        cfg_val
            .map(|v| v.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(|v| ("auth", v))
    };
    match sourced {
        Some((source, raw)) => raw.parse().map_err(|e| format!("{source}: {e}")),
        None => Ok(rusty_grid_master::server::AuthMode::Both),
    }
}

fn resolve_auth_mode(
    flag: Option<String>,
    cfg_val: Option<String>,
) -> Result<rusty_grid_master::server::AuthMode, String> {
    let env_val = std::env::var(AUTH_MODE_ENV).ok();
    resolve_auth_mode_from(flag, env_val.as_deref(), cfg_val)
}

/// Resolves `--relay-url` > `RUSTY_GRID_RELAY_URL` > config `relay_url` > `None` without touching
/// the global process environment, validates the URL as an `https://` `iroh::RelayUrl` without
/// embedded credentials, and refuses combining a custom relay URL with `p2p_minimal` mode.
pub(crate) fn resolve_relay_url_from(
    cli_val: Option<String>,
    env_val: Option<&str>,
    cfg_val: Option<String>,
    p2p_minimal: bool,
) -> Result<Option<String>, String> {
    let sourced = if let Some(v) = cli_val {
        Some(("--relay-url", v))
    } else if let Some(v) = env_val.map(str::trim).filter(|s| !s.is_empty()) {
        Some((rusty_grid_core::transport::RELAY_URL_ENV, v.to_string()))
    } else {
        cfg_val
            .map(|v| v.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(|v| ("relay_url", v))
    };
    let Some((source, raw)) = sourced else {
        return Ok(None);
    };
    let trimmed = raw.trim().to_string();
    rusty_grid_core::transport::resolve_p2p_relay_mode(p2p_minimal, Some(&trimmed))
        .map_err(|e| format!("{source}: {e}"))?;
    Ok(Some(trimmed))
}

/// Resolves `--relay-url` > `RUSTY_GRID_RELAY_URL` > config `relay_url` > `None`, validating
/// the URL and refusing conflicts with `p2p_minimal`.
pub(crate) fn resolve_relay_url(
    cli_val: Option<String>,
    cfg_val: Option<String>,
    p2p_minimal: bool,
) -> Result<Option<String>, String> {
    let env_val = std::env::var(rusty_grid_core::transport::RELAY_URL_ENV).ok();
    resolve_relay_url_from(cli_val, env_val.as_deref(), cfg_val, p2p_minimal)
}

/// Resolves `(master_addr, p2p_ticket)` for `oxideswarm worker` across CLI > Env > Config > Default,
/// ensuring that an explicit `--master` (or `RUSTY_GRID_MASTER_ADDR` / `RUSTY_GRID_MASTER`) is not
/// overridden by a lower-tier `p2p_ticket` (such as `[worker] p2p_ticket` in the config file).
fn resolve_worker_target(
    cli_master: Option<String>,
    cli_ticket: Option<String>,
    worker_cfg: Option<&crate::config::WorkerFileConfig>,
) -> (String, Option<String>) {
    let env_master =
        resolve_opt_string(None, &["RUSTY_GRID_MASTER_ADDR", "RUSTY_GRID_MASTER"], None);
    let env_ticket = resolve_opt_string(None, &["RUSTY_GRID_P2P_TICKET"], None);
    let cfg_master = worker_cfg
        .and_then(|w| w.master.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let cfg_ticket = worker_cfg
        .and_then(|w| w.p2p_ticket.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let cli_master = cli_master
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let cli_ticket = cli_ticket
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let master_raw = cli_master
        .clone()
        .or_else(|| env_master.clone())
        .or(cfg_master)
        .unwrap_or_else(|| DEFAULT_MASTER_ADDR.to_string());
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

    (master_addr, p2p_ticket)
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
    let members = match rusty_grid_master::server::MemberStore::load_from_dir(state_dir.as_deref())
    {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Failed to load members.json: {e}");
            return ExitCode::FAILURE;
        }
    };
    let master_noise_key = if ephemeral_key && state_dir.is_none() {
        match rusty_grid_core::noise_auth::NodeKeypair::generate() {
            Ok(kp) => kp,
            Err(e) => {
                eprintln!("Failed to generate master_noise.key: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if let Some(key_path) =
        rusty_grid_core::noise_auth::master_noise_key_path(state_dir.as_deref())
    {
        match rusty_grid_core::noise_auth::NodeKeypair::load_or_generate_file(&key_path) {
            Ok((kp, _)) => kp,
            Err(e) => {
                eprintln!("Failed to load or generate {}: {e}", key_path.display());
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
    server_config = server_config
        .with_auth_mode(auth_mode)
        .with_members(members)
        .with_master_noise_key(master_noise_key);

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
    ExitCode::SUCCESS
}

/// Resolves `--state-dir` / `RUSTY_GRID_STATE_DIR` / `master.state_dir` without mutating the
/// process environment.
fn resolve_state_dir_value(
    cli_val: Option<PathBuf>,
    env_val: Option<&str>,
    cfg_val: Option<&str>,
) -> Option<PathBuf> {
    if let Some(path) = cli_val {
        let trimmed = path.display().to_string();
        if !trimmed.trim().is_empty() {
            return Some(path);
        }
    }
    if let Some(env_s) = env_val.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(env_s));
    }
    if let Some(cfg_s) = cfg_val.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(cfg_s));
    }
    None
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
        resolve_worker_target(args.master, args.p2p_ticket, worker_cfg.as_ref());

    if let Some(ref r_url) = args.rendezvous {
        std::env::set_var(rusty_grid_core::discovery::RENDEZVOUS_ENV, r_url);
    }

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
    config = config.with_relay_url(relay_url);
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
    config = config.with_isolation(isolation);
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

/// Reads `RUSTY_GRID_NO_BATTERY` from the process environment (and nowhere else: config files and
/// legacy alias names cannot disable battery/thermal protection).
pub(crate) fn resolve_no_battery() -> bool {
    resolve_bool(None, &["RUSTY_GRID_NO_BATTERY"], None, false)
}

/// Parses a `RUSTY_GRID_P2P_MINIMAL` environment value (`1`, `true`, `yes`, or `on`,
/// case-insensitive). Config files and alias names cannot disable DERP relay / address lookup.
pub(crate) fn resolve_p2p_minimal_from(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Reads `RUSTY_GRID_P2P_MINIMAL` (`1`, `true`, `yes`, or `on`, case-insensitive) from the process
/// environment (and nowhere else: config files and alias names cannot disable DERP relay / address
/// lookup).
pub(crate) fn resolve_p2p_minimal() -> bool {
    resolve_p2p_minimal_from(std::env::var("RUSTY_GRID_P2P_MINIMAL").ok().as_deref())
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
    transport: &mut MessageTransport<GridStream>,
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

/// Validates and normalizes `--cwd` / `workspace.cwd` relative to the workspace root.
/// `"."` (or `"./"`, `".\\"`) resolves to `None` (the workspace root itself).
pub(crate) fn normalize_workspace_cwd(raw: &str) -> Result<Option<PathBuf>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("Working directory inside workspace cannot be empty".to_string());
    }
    let slash_norm = trimmed.replace('\\', "/");
    if slash_norm == "." || slash_norm.trim_matches('/') == "." {
        return Ok(None);
    }
    let validated = rusty_grid_core::task::validate_relative_sandbox_path(trimmed)?;
    Ok(Some(PathBuf::from(validated)))
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
pub fn build_task_spec(args: &SubmitArgs) -> Result<TaskSpec, String> {
    let mut kind = args.task_type.to_lowercase().replace('-', "_");
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
            | "builtin_test"
            | "builtintest"
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
    let is_compile = kind == "compile";
    if !args.sources.is_empty() && !is_compile {
        return only_for("--source", "compile");
    }
    if !args.inputs.is_empty() && !is_command && kind != "shell" && !is_compile {
        return only_for("--input (or --file)", "command and shell");
    }
    if !args.outputs.is_empty() && !is_command && kind != "shell" && !is_compile {
        if matches!(
            kind.as_str(),
            "3d" | "gen3d" | "3d_generate" | "generative_3d" | "autorig" | "auto_rig" | "rig"
        ) {
            let target = if args.task_type == "generic" || args.task_type.is_empty() {
                "3d or rigging".to_string()
            } else {
                args.task_type.clone()
            };
            return Err(format!(
                "--output applies to command and shell tasks, not to {target} tasks (use --output-asset or -o for 3D/rigging assets)"
            ));
        }
        return only_for("--output", "command and shell");
    }
    if args.output_dir.is_some() && !is_command && kind != "shell" && !is_compile {
        return only_for("--output-dir", "command and shell");
    }
    if args.output_dir.is_some() && args.outputs.is_empty() && !is_compile {
        return Err("--output-dir requires at least one --output pattern".to_string());
    }
    if args.sync.is_some() && args.workspace.is_none() {
        return Err("--sync requires --workspace <NAME>".to_string());
    }
    if args.no_sync && args.workspace.is_none() {
        return Err("--no-sync requires --workspace <NAME>".to_string());
    }
    if args.sync.is_some() && args.no_sync {
        return Err("--sync cannot be combined with --no-sync".to_string());
    }
    if args.cwd.is_some() && args.workspace.is_none() {
        return Err("--cwd requires --workspace <NAME>".to_string());
    }
    if args.cwd.is_some() && !is_command {
        return only_for("--cwd", "command");
    }
    if args.workspace_max_files == Some(0) {
        return Err("--workspace-max-files must be at least 1 (got 0)".to_string());
    }
    if args.workspace_max_mb == Some(0) {
        return Err("--workspace-max-mb must be at least 1 MiB (got 0)".to_string());
    }
    if let Some(ref ws_name) = args.workspace {
        if !is_command && kind != "shell" {
            return only_for("--workspace", "command and shell");
        }
        rusty_grid_core::task::validate_workspace_name(ws_name)?;
        if !args.inputs.is_empty() {
            return Err(
                "--workspace cannot be combined with --input (or --file); sync the files into the workspace instead"
                    .to_string(),
            );
        }
    }
    let working_dir = match &args.cwd {
        Some(raw_cwd) => normalize_workspace_cwd(raw_cwd).map_err(|e| format!("--cwd: {e}"))?,
        None => None,
    };
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
                return Err("3D generation requires either --prompt or --image".to_string());
            }
            let quality = args
                .quality
                .clone()
                .unwrap_or_else(|| "standard".to_string())
                .to_lowercase();
            let engine_preference = args
                .engine
                .as_deref()
                .map(str::parse::<rusty_grid_core::task::Engine3D>)
                .transpose()?
                .map(|e| e.as_str().to_string());
            let _ = args.export_texture;
            let export_texture = !args.no_export_texture;
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
            Ok(TaskSpec::gpu_compute_with_params(
                args.command
                    .clone()
                    .unwrap_or_else(|| "matrix_multiply".into()),
                vec![1, 2, 3, 4],
                tile,
                dim,
            ))
        }
        "compile" => {
            let (crate_name, source_files) = collect_compile_sources(args)?;
            let spec = TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags: args.args.clone(),
                target_dir: None,
            };
            spec.validate()?;
            Ok(spec)
        }
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
                working_dir,
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

/// Collects Rust source files and crate metadata from `--source` (or `--input` when `--source`
/// is omitted) for `submit --type compile`.
fn collect_compile_sources(args: &SubmitArgs) -> Result<(String, HashMap<String, String>), String> {
    let (raw_specs, flag_name) = if !args.sources.is_empty() {
        (&args.sources, "--source")
    } else if !args.inputs.is_empty() {
        (&args.inputs, "--input")
    } else {
        return Err(
            "compile tasks require --source <PATH> (a Rust source file or Cargo crate directory)"
                .to_string(),
        );
    };

    let mut source_files = HashMap::new();
    let mut first_local_path: Option<PathBuf> = None;

    for raw in raw_specs {
        let (local_path, explicit_dest) =
            crate::blobs::parse_input_spec(raw).map_err(|e| e.replace("--input", flag_name))?;
        if first_local_path.is_none() {
            first_local_path = Some(local_path.clone());
        }
        let meta = std::fs::metadata(&local_path)
            .map_err(|e| format!("{flag_name} {}: {e}", local_path.display()))?;
        if meta.is_dir() {
            let prefix = match explicit_dest {
                Some(dest) => {
                    let parts: Vec<&str> = dest
                        .split('/')
                        .filter(|p| !p.is_empty() && *p != ".")
                        .collect();
                    if parts.is_empty() {
                        String::new()
                    } else {
                        let joined = parts.join("/");
                        rusty_grid_core::blob::validate_blob_path(&joined)
                            .map_err(|e| format!("{flag_name}: sandbox path '{dest}': {e}"))?;
                        joined
                    }
                }
                None => String::new(),
            };
            collect_compile_dir(
                &local_path,
                &local_path,
                &prefix,
                flag_name,
                &mut source_files,
            )?;
        } else if meta.is_file() {
            let sandbox_rel = match explicit_dest {
                Some(dest) => {
                    let parts: Vec<&str> = dest
                        .split('/')
                        .filter(|p| !p.is_empty() && *p != ".")
                        .collect();
                    if parts.is_empty() {
                        return Err(format!(
                            "{flag_name}: '{dest}' is the sandbox root; a file needs a file name"
                        ));
                    }
                    let joined = parts.join("/");
                    rusty_grid_core::blob::validate_blob_path(&joined)
                        .map_err(|e| format!("{flag_name}: sandbox path '{dest}': {e}"))?;
                    joined
                }
                None => default_compile_file_dest(&local_path, flag_name)?,
            };
            let bytes = read_task_input(&local_path, flag_name)?;
            let text = String::from_utf8(bytes).map_err(|_| {
                format!(
                    "{flag_name} {}: the file is not UTF-8 text",
                    local_path.display()
                )
            })?;
            source_files.insert(sandbox_rel, text);
        } else {
            return Err(format!(
                "{flag_name} {}: path is neither a regular file nor a directory",
                local_path.display()
            ));
        }
    }

    if source_files.is_empty() {
        return Err(format!("{flag_name}: no source files found"));
    }

    let crate_name = if let Some(cmd) = &args.command {
        let trimmed = cmd.trim();
        if trimmed.is_empty() {
            "crate".to_string()
        } else {
            trimmed.to_string()
        }
    } else if let Some(manifest) = source_files.get("Cargo.toml") {
        parse_cargo_package_name(manifest).unwrap_or_else(|| "crate".to_string())
    } else if let Some(path) = first_local_path {
        path.file_stem()
            .or_else(|| path.file_name())
            .and_then(|s| s.to_str())
            .map(|s| s.replace('-', "_"))
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "crate".to_string())
    } else {
        "crate".to_string()
    };

    Ok((crate_name, source_files))
}

fn collect_compile_dir(
    root: &Path,
    current: &Path,
    prefix: &str,
    flag_name: &str,
    out: &mut HashMap<String, String>,
) -> Result<(), String> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(current)
        .map_err(|e| format!("{flag_name} {}: {e}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{flag_name} {}: {e}", current.display()))?;
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let file_type = entry
            .file_type()
            .map_err(|e| format!("{flag_name} {}: {e}", entry.path().display()))?;
        if file_type.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if file_type.is_dir() {
            if name_str == ".git" || name_str == "target" {
                continue;
            }
            collect_compile_dir(root, &entry.path(), prefix, flag_name, out)?;
        } else if file_type.is_file() {
            let rel = entry
                .path()
                .strip_prefix(root)
                .map_err(|e| format!("{flag_name} {}: {e}", entry.path().display()))?
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => s.to_str(),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/");
            if rel.is_empty() {
                continue;
            }
            let sandbox_rel = if prefix.is_empty() {
                rel
            } else {
                format!("{prefix}/{rel}")
            };
            if rusty_grid_core::blob::validate_blob_path(&sandbox_rel).is_err() {
                continue;
            }
            let bytes = read_task_input(&entry.path(), flag_name)?;
            let text = String::from_utf8(bytes).map_err(|_| {
                format!(
                    "{flag_name} {}: the file is not UTF-8 text",
                    entry.path().display()
                )
            })?;
            out.insert(sandbox_rel, text);
        }
    }
    Ok(())
}

fn default_compile_file_dest(local: &Path, flag_name: &str) -> Result<String, String> {
    let mut parts = Vec::new();
    let mut relative = true;
    for component in local.components() {
        match component {
            std::path::Component::Normal(part) => match part.to_str() {
                Some(s) => parts.push(s.to_string()),
                None => relative = false,
            },
            std::path::Component::CurDir => {}
            _ => relative = false,
        }
    }
    if relative && !parts.is_empty() {
        let joined = parts.join("/");
        if rusty_grid_core::blob::validate_blob_path(&joined).is_ok() {
            return Ok(joined);
        }
    }
    if let (Some(file_name), Some(parent)) =
        (local.file_name().and_then(|n| n.to_str()), local.parent())
    {
        if parent.file_name().and_then(|n| n.to_str()) == Some("src") {
            let candidate = format!("src/{file_name}");
            if rusty_grid_core::blob::validate_blob_path(&candidate).is_ok() {
                return Ok(candidate);
            }
        }
        if rusty_grid_core::blob::validate_blob_path(file_name).is_ok() {
            return Ok(file_name.to_string());
        }
    }
    Err(format!(
        "{flag_name} '{}': cannot derive a sandbox path; name one with PATH:SANDBOX_PATH",
        local.display()
    ))
}

fn parse_cargo_package_name(manifest: &str) -> Option<String> {
    let parsed: toml::Value = toml::from_str(manifest).ok()?;
    parsed
        .get("package")?
        .get("name")?
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
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

/// Environment variable that selects the worker's isolation level (`--isolation`).
const ISOLATION_ENV: &str = "RUSTY_GRID_ISOLATION";

/// `--isolation` > environment > config file `[worker].isolation` > `off`.
fn resolve_isolation(
    cli: Option<String>,
    env_names: &[&str],
    file: Option<String>,
) -> Result<IsolationLevel, String> {
    match resolve_opt_string(cli, env_names, file) {
        Some(text) => text.parse(),
        None => Ok(IsolationLevel::Off),
    }
}

/// The placement constraints of `submit --os / --arch / --worker / --require-isolation / --require-tag / --require-tool`, checked and
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
    if let Some(level) = &args.require_isolation {
        let level = IsolationLevel::parse_required(level)
            .map_err(|e| format!("--require-isolation: {e}"))?;
        tags.push(format!("{REQUIRE_PREFIX}iso={level}"));
    }
    for tag in &args.require_tags {
        tags.push(format!("{REQUIRE_PREFIX}tag={tag}"));
    }
    for tool in &args.require_tools {
        tags.push(format!("{REQUIRE_PREFIX}tool={tool}"));
    }
    Placement::from_task_tags(&tags)
}

/// For `TaskSpec::RustCompilation`, automatically adds `require:tool=cargo` when `source_files`
/// contains `Cargo.toml` (or `require:tool=rustc` otherwise), unless the caller already specified
/// a `--require-tool` requirement.
fn apply_default_spec_placement(spec: &TaskSpec, placement: &mut Placement) {
    if let TaskSpec::RustCompilation { source_files, .. } = spec {
        if placement.tools.is_empty() {
            let tool = if source_files.contains_key("Cargo.toml") {
                "cargo"
            } else {
                "rustc"
            };
            placement.tools.push(tool.to_string());
        }
    }
}
/// (see `rusty_grid_core::placement`). `None` when the master did not answer with a list.
async fn fetch_workers(
    transport: &mut MessageTransport<GridStream>,
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
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master.clone(),
        CLIENT_MASTER_ENV_VARS,
        submit_cfg.as_ref().and_then(|s| s.target_master()),
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
        skip_throughput: args.skip_throughput,
    };
    let report = crate::connect::with_client_relay_url(
        relay_url,
        run_doctor(&master_addr, secret.as_ref(), &options),
    )
    .await;
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
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_submit_with_secret_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_submit_with_secret_inner(
    args: SubmitArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    // Everything wrong with the arguments is reported before connecting.
    let (spec, placement) = match build_task_spec(&args).and_then(|spec| {
        let mut placement = build_placement(&args)?;
        apply_default_spec_placement(&spec, &mut placement);
        Ok((spec, placement))
    }) {
        Ok(built) => built,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    // Inputs are hashed (and directories listed) before connecting, so a typo in a path never
    // reaches the master. When `--type compile` was given without `--source`, `args.inputs` was
    // already loaded into `TaskSpec::RustCompilation.source_files`.
    let is_compile = matches!(spec, TaskSpec::RustCompilation { .. });
    let inputs = if is_compile && args.sources.is_empty() {
        crate::blobs::PreparedInputs::default()
    } else {
        match crate::blobs::scan_inputs(&args.inputs) {
            Ok(inputs) => inputs,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    };
    for skipped in &inputs.skipped {
        eprintln!("Warning: --input skips {skipped}");
    }

    let (workspace_ref, workspace_scan) = match &args.workspace {
        Some(ws_name) => {
            if args.no_sync {
                (
                    Some(rusty_grid_core::task::WorkspaceRef::new(ws_name.clone())),
                    None,
                )
            } else {
                let limits = crate::blobs::WorkspaceLimits {
                    max_files: args
                        .workspace_max_files
                        .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_FILES),
                    max_bytes: args
                        .workspace_max_mb
                        .map(|mb| mb.saturating_mul(MIB))
                        .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_BYTES),
                };
                let sync_dir = args.sync.as_deref().unwrap_or(Path::new("."));
                let scan = match crate::blobs::scan_workspace_source(sync_dir, limits) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("{e}");
                        return ExitCode::FAILURE;
                    }
                };
                for skipped in &scan.prepared.skipped {
                    eprintln!("Warning: --workspace skips {skipped}");
                }
                let ws_ref = rusty_grid_core::task::WorkspaceRef::new(ws_name.clone())
                    .with_tree(scan.manifest_id, scan.manifest_len);
                (Some(ws_ref), Some(scan))
            }
        }
        None => {
            if args.workspace_max_files.is_some() || args.workspace_max_mb.is_some() {
                eprintln!(
                    "Warning: --workspace-max-files and --workspace-max-mb have no effect without --workspace"
                );
            }
            (None, None)
        }
    };

    let output_globs = if !args.outputs.is_empty() {
        if let Err(e) = rusty_grid_core::blob::validate_output_patterns(&args.outputs) {
            eprintln!("--output: {e}");
            return ExitCode::FAILURE;
        }
        args.outputs.clone()
    } else {
        Vec::new()
    };
    if (!inputs.inputs.is_empty() || !output_globs.is_empty())
        && !matches!(
            spec,
            TaskSpec::Command { .. }
                | TaskSpec::ShellScript { .. }
                | TaskSpec::RustCompilation { .. }
        )
    {
        eprintln!(
            "--input and --output work with command and shell tasks, not --type {}",
            args.task_type
        );
        return ExitCode::FAILURE;
    }

    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master.clone(),
        CLIENT_MASTER_ENV_VARS,
        submit_cfg.as_ref().and_then(|s| s.target_master()),
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
    // A constrained or workspace task nobody can take would wait silently; say so (it is still
    // submitted, a matching worker may connect later).
    if !placement.is_empty() || workspace_ref.is_some() {
        if let Some(workers) = fetch_workers(&mut transport, &master_addr, had_secret).await {
            if workspace_ref.is_some()
                && !workers
                    .iter()
                    .any(|w| listed_as_connected(w) && listed_supports_workspaces(w))
            {
                eprintln!(
                    "Warning: no connected worker supports workspaces yet; the task will wait"
                );
            }
            if !placement.is_empty()
                && !workers
                    .iter()
                    .any(|w| listed_as_connected(w) && placement.matches_listed(w))
            {
                eprintln!(
                    "Warning: no connected worker matches the placement constraints ({placement}); the task waits in the queue until one does"
                );
            }
        }
    }

    let is_gpu = args.gpu || args.task_type.eq_ignore_ascii_case("gpu") || spec.requires_gpu();
    let default_timeout_secs = if args.workspace.is_some() { 1800 } else { 60 };
    let timeout_secs = resolve_u64(
        args.timeout_secs,
        &[],
        submit_cfg.as_ref().and_then(|s| s.timeout_secs),
        default_timeout_secs,
    )
    .max(1);
    let requirements = TaskRequirements {
        cpu_cores: args.cores.max(1),
        ram_mb: args.ram_mb,
        gpu_required: is_gpu,
        timeout_secs,
        max_retries: args.max_retries,
    };

    // Only the input blobs the master lacks are sent, on a connection of their own.
    let upload = if let Some(ref scan) = workspace_scan {
        let uploaded =
            match crate::blobs::BlobConnection::connect(&master_addr, secret.as_ref()).await {
                Ok(connection) => crate::blobs::upload_inputs(&connection, &scan.prepared).await,
                Err(e) => Err(e.to_string()),
            };
        match uploaded {
            Ok(report) => {
                eprintln!(
                    "Workspace '{}': {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                    args.workspace.as_deref().unwrap_or(""),
                    scan.prepared.files,
                    rusty_grid_worker::staging::human_bytes(scan.prepared.bytes),
                    report.uploaded,
                    report.blobs,
                    rusty_grid_worker::staging::human_bytes(report.bytes),
                    report.blobs - report.uploaded
                );
                Some(report)
            }
            Err(e) => {
                eprintln!("Uploading the workspace files failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if inputs.inputs.is_empty() {
        None
    } else {
        let uploaded =
            match crate::blobs::BlobConnection::connect(&master_addr, secret.as_ref()).await {
                Ok(connection) => crate::blobs::upload_inputs(&connection, &inputs).await,
                Err(e) => Err(e.to_string()),
            };
        match uploaded {
            Ok(report) => {
                eprintln!(
                    "Inputs: {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                    inputs.files,
                    rusty_grid_worker::staging::human_bytes(inputs.bytes),
                    report.uploaded,
                    report.blobs,
                    rusty_grid_worker::staging::human_bytes(report.bytes),
                    report.blobs - report.uploaded
                );
                Some(report)
            }
            Err(e) => {
                eprintln!("Uploading the input files failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    let mut task = Task::new(spec, requirements)
        .with_placement(&placement)
        .with_inputs(inputs.inputs)
        .with_output_globs(output_globs.clone());
    if let Some(ws) = workspace_ref {
        task = task.with_workspace(ws);
    }
    let task_id = task.id;
    let wait = args.wait || args.output.is_some() || !output_globs.is_empty() || is_compile;
    let submit_msg = ClientMessage::SubmitTask { task, wait };

    let response = if wait {
        if let Err(e) = transport.send_msg(&submit_msg).await {
            return report_master_failure(
                &master_addr,
                had_secret,
                "Failed to send task to Master",
                &GridError::from(e),
            );
        }
        match tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            transport.recv_msg::<ClientResponse>(),
        )
        .await
        {
            Ok(Ok(Some(response))) => response,
            Ok(Ok(None)) => {
                return report_master_failure(
                    &master_addr,
                    had_secret,
                    "Master closed the connection before replying",
                    &GridError::ConnectionClosed,
                );
            }
            Ok(Err(e)) => {
                return report_master_failure(
                    &master_addr,
                    had_secret,
                    "Communication error with Master",
                    &GridError::from(e),
                );
            }
            Err(_) => {
                let msg = format!(
                    "Timed out after {timeout_secs}s waiting for task {task_id} to complete"
                );
                eprintln!("{msg}");
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "status": "timeout",
                            "task_id": task_id.to_string(),
                            "error": msg,
                        })
                    );
                }
                return ExitCode::FAILURE;
            }
        }
    } else {
        match exchange(
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
        }
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

            let asset_output_path = args.output.clone();

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
            let mut download_failed = false;
            let saved_outputs = if result.outputs.is_empty() {
                None
            } else {
                let downloaded = match crate::blobs::BlobConnection::connect(
                    &master_addr,
                    secret.as_ref(),
                )
                .await
                {
                    Ok(connection) => {
                        crate::blobs::download_outputs(&connection, &result.outputs, output_dir)
                            .await
                    }
                    Err(e) => Err(e.to_string()),
                };
                match downloaded {
                    Ok(saved) => {
                        if is_compile && result.asset_bytes.is_none() {
                            if let (Some(ref out_path), Some(first)) =
                                (&asset_output_path, saved.first())
                            {
                                if let Some(parent) = out_path.parent() {
                                    if !parent.as_os_str().is_empty() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                }
                                if let Err(e) = std::fs::copy(first, out_path) {
                                    eprintln!(
                                        "Failed to write compiled output to '{}': {e}",
                                        out_path.display()
                                    );
                                    download_failed = true;
                                }
                            }
                        }
                        Some(saved)
                    }
                    Err(e) => {
                        eprintln!("Downloading the output files failed: {e}");
                        download_failed = true;
                        None
                    }
                }
            };
            let mut artifact_save_error: Option<String> = None;
            let saved_artifacts = if !result.artifacts.is_empty() {
                match crate::connect::save_output_artifacts(&result.artifacts, output_dir) {
                    Ok(saved) => Some(saved),
                    Err(e) => {
                        artifact_save_error = Some(e);
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
                    if let Some(ref err) = artifact_save_error {
                        obj.insert("artifact_save_error".to_string(), serde_json::json!(err));
                    }
                    if let Some(ref saved) = saved_outputs {
                        let saved_strs: Vec<String> =
                            saved.iter().map(|p| p.display().to_string()).collect();
                        obj.insert("saved_outputs".to_string(), serde_json::json!(saved_strs));
                    }
                    if let Some(report) = upload {
                        obj.insert("input_upload".to_string(), serde_json::json!(report));
                    }
                }
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            } else {
                if result.is_success() {
                    println!("Task {task_id} completed successfully.");
                } else {
                    println!("Task {task_id} failed.");
                }
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
                if let Some(ref saved) = saved_outputs {
                    println!(
                        "Output files ({} saved to {}):",
                        saved.len(),
                        output_dir.display()
                    );
                    for (output, path) in result.outputs.iter().zip(saved.iter()) {
                        println!(
                            "  - {} ({} bytes, blake3: {}) -> {}",
                            output.path,
                            output.size,
                            output.id.short(),
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
                if let Some(ref error) = result.error {
                    eprintln!("Error: {error}");
                }
            }
            if let Some(err) = artifact_save_error {
                eprintln!("Failed to save output artifacts: {err}");
                return ExitCode::FAILURE;
            }
            if download_failed {
                ExitCode::FAILURE
            } else if result.is_success() {
                ExitCode::SUCCESS
            } else if result.exit_code != 0 {
                ExitCode::from(result.exit_code.min(255) as u8)
            } else {
                ExitCode::FAILURE
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
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_status_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_status_inner(
    args: StatusArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
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
                    let connected = workers.iter().filter(|w| worker_is_connected(w)).count();
                    println!(
                        "Workers ({} connected, {} registered):",
                        connected,
                        workers.len()
                    );
                    for w in &workers {
                        let state = match worker_state(w) {
                            Some(state) if state != "connected" => format!(" [{state}]"),
                            _ => String::new(),
                        };
                        println!(
                            "- Worker [{}] (Cores: {}, RAM: {}MB, GPU: {}, Simulated: {}){}",
                            w.name, w.cpu_cores, w.ram_mb, w.has_gpu, w.is_simulated_gpu, state,
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
                active_bytes,
                max_active_bytes,
                store_error,
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
                    let mut out = serde_json::json!({
                        "total_tasks": total_tasks,
                        "pending_tasks": pending_tasks,
                        "running_tasks": running_tasks,
                        "completed_tasks": completed_tasks,
                        "failed_tasks": failed_tasks,
                        "active_bytes": active_bytes,
                        "max_active_bytes": max_active_bytes,
                        "worker_count": workers.len(),
                        "workers": enriched_workers
                    });
                    if let Some(ref err) = store_error {
                        out["store_error"] = serde_json::Value::String(err.clone());
                    }
                    println!("{}", out);
                } else {
                    println!("=== RustyGrid Cluster Status ===");
                    println!(
                        "Connected Workers: {} ({} registered)",
                        workers.iter().filter(|w| worker_is_connected(w)).count(),
                        workers.len()
                    );
                    println!("Total Tasks:      {}", total_tasks);
                    println!("Pending Tasks:    {}", pending_tasks);
                    println!("Running Tasks:    {}", running_tasks);
                    println!("Completed Tasks:  {}", completed_tasks);
                    println!("Failed Tasks:     {}", failed_tasks);
                    println!(
                        "Active Budget:    {} / {} bytes ({:.2} / {:.2} MiB)",
                        active_bytes,
                        max_active_bytes,
                        active_bytes as f64 / (1024.0 * 1024.0),
                        max_active_bytes as f64 / (1024.0 * 1024.0),
                    );
                    if let Some(ref err) = store_error {
                        println!(
                            "Task Store Error: {} (durability is gone until restart)",
                            err
                        );
                    }
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

pub async fn run_cancel(
    args: CancelArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_cancel_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_cancel_inner(
    args: CancelArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
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
    let task_id = TaskId(args.task_id);
    let msg = ClientMessage::CancelTask { task_id };
    let response = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &msg,
        "Failed to cancel task",
    )
    .await
    {
        Ok(response) => response,
        Err(code) => return code,
    };

    match response {
        ClientResponse::TaskCancelled { task_id, success } => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "task_id": task_id.to_string(),
                        "cancelled": success,
                    })
                );
            } else if success {
                println!("Task {task_id} cancelled.");
            } else {
                eprintln!("Task {task_id} could not be cancelled (not found or already finished).");
            }
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        ClientResponse::Error { message } => {
            if args.json {
                println!("{}", serde_json::json!({ "error": message }));
            } else {
                eprintln!("Error: {message}");
            }
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected response: {other:?}");
            ExitCode::FAILURE
        }
    }
}

pub async fn run_result(
    args: ResultArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_result_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_result_inner(
    args: ResultArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
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
    let task_id = TaskId(args.task_id);
    let msg = ClientMessage::GetTaskResult { task_id };

    loop {
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to fetch task result",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };

        match response {
            ClientResponse::TaskCompleted { task_id, result } => {
                let output_dir = args.output_dir.as_deref().unwrap_or(Path::new("."));
                let mut output_download_error: Option<String> = None;
                let saved_outputs = if result.outputs.is_empty() {
                    None
                } else {
                    let downloaded =
                        match crate::blobs::BlobConnection::connect(&master_addr, secret.as_ref())
                            .await
                        {
                            Ok(connection) => {
                                crate::blobs::download_outputs(
                                    &connection,
                                    &result.outputs,
                                    output_dir,
                                )
                                .await
                            }
                            Err(e) => Err(e.to_string()),
                        };
                    match downloaded {
                        Ok(saved) => Some(saved),
                        Err(e) => {
                            output_download_error = Some(e);
                            None
                        }
                    }
                };
                let mut artifact_save_error: Option<String> = None;
                let saved_artifacts = if !result.artifacts.is_empty() {
                    match crate::connect::save_output_artifacts(&result.artifacts, output_dir) {
                        Ok(saved) => Some(saved),
                        Err(e) => {
                            artifact_save_error = Some(e);
                            None
                        }
                    }
                } else {
                    None
                };

                if args.json {
                    let mut value = serde_json::to_value(&result).unwrap_or_default();
                    if let Some(obj) = value.as_object_mut() {
                        if let Some(ref saved) = saved_artifacts {
                            let saved_strs: Vec<String> =
                                saved.iter().map(|p| p.display().to_string()).collect();
                            obj.insert(
                                "saved_artifacts".to_string(),
                                serde_json::json!(saved_strs),
                            );
                        }
                        if let Some(ref err) = artifact_save_error {
                            obj.insert("artifact_save_error".to_string(), serde_json::json!(err));
                        }
                        if let Some(ref saved) = saved_outputs {
                            let saved_strs: Vec<String> =
                                saved.iter().map(|p| p.display().to_string()).collect();
                            obj.insert("saved_outputs".to_string(), serde_json::json!(saved_strs));
                        }
                        if let Some(ref err) = output_download_error {
                            obj.insert("output_download_error".to_string(), serde_json::json!(err));
                        }
                    }
                    println!("{}", serde_json::to_string_pretty(&value).unwrap());
                } else {
                    if result.is_success() {
                        println!("Task {task_id} completed successfully.");
                    } else if result.exit_code == 130 {
                        println!("Task {task_id} was cancelled.");
                    } else {
                        println!("Task {task_id} failed.");
                    }
                    if !result.worker_id.is_nil() {
                        println!("Worker: {}", result.worker_id);
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
                    if let Some(ref saved) = saved_outputs {
                        println!(
                            "Output files ({} saved to {}):",
                            saved.len(),
                            output_dir.display()
                        );
                        for (output, path) in result.outputs.iter().zip(saved.iter()) {
                            println!(
                                "  - {} ({} bytes, blake3: {}) -> {}",
                                output.path,
                                output.size,
                                output.id.short(),
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
                    if let Some(ref err) = result.error {
                        eprintln!("Error: {err}");
                    }
                }
                if let Some(err) = output_download_error {
                    eprintln!("Downloading the output files failed: {err}");
                    return ExitCode::FAILURE;
                }
                if let Some(err) = artifact_save_error {
                    eprintln!("Failed to save output artifacts: {err}");
                    return ExitCode::FAILURE;
                }
                return if result.is_success() {
                    ExitCode::SUCCESS
                } else {
                    let code = result.exit_code.clamp(1, 255) as u8;
                    ExitCode::from(if code == 3 { 1 } else { code })
                };
            }
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => {
                let is_terminal = matches!(
                    status,
                    rusty_grid_core::TaskStatus::Completed
                        | rusty_grid_core::TaskStatus::Failed
                        | rusty_grid_core::TaskStatus::Cancelled
                );
                if args.wait && !is_terminal {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "assigned_worker": assigned_worker,
                            "error": error,
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
                        eprintln!("Error:          {err}");
                    }
                }
                return if status == rusty_grid_core::TaskStatus::Completed {
                    ExitCode::SUCCESS
                } else if !is_terminal {
                    ExitCode::from(3)
                } else {
                    ExitCode::FAILURE
                };
            }
            ClientResponse::Error { message } => {
                if args.json {
                    println!("{}", serde_json::json!({ "error": message }));
                } else {
                    eprintln!("Error: {message}");
                }
                return ExitCode::FAILURE;
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                return ExitCode::FAILURE;
            }
        }
    }
}

/// The `state:` tag the master adds to each worker in `ListWorkers` and `ClusterStatus`
/// (`connected`, `disconnected`, ...), or `None` when the master sent none.
fn worker_state(w: &rusty_grid_core::WorkerCapabilities) -> Option<&str> {
    w.tags.iter().find_map(|tag| tag.strip_prefix("state:"))
}

/// Whether a listed worker is connected. A worker without a `state:` tag counts as connected,
/// which is how such a listing was meant before the master added the tag.
fn worker_is_connected(w: &rusty_grid_core::WorkerCapabilities) -> bool {
    worker_state(w).is_none_or(|state| state == "connected")
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
        relay_url: args.relay_url,
        task_id: None,
        workers: true,
        json: args.json,
        insecure: args.insecure,
    };
    run_status(status_args, config_file, secret_opts).await
}

pub async fn run_workspaces(
    args: WorkspacesArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_workspaces_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_workspaces_inner(
    args: WorkspacesArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
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

    let response = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &ClientMessage::ListWorkspaces,
        "Failed to list workspaces",
    )
    .await
    {
        Ok(response) => response,
        Err(code) => return code,
    };

    match response {
        ClientResponse::Workspaces { workspaces } => {
            if args.json {
                println!("{}", serde_json::to_string_pretty(&workspaces).unwrap());
            } else {
                println!("Workspaces ({}):", workspaces.len());
                for ws in &workspaces {
                    println!(
                        "- {} -> worker {} (last_used_utc: {})",
                        ws.name, ws.worker, ws.last_used_utc
                    );
                }
            }
            ExitCode::SUCCESS
        }
        ClientResponse::Error { message } => {
            if args.json {
                println!("{}", serde_json::json!({ "error": message }));
            } else {
                eprintln!("Error: {message}");
            }
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected response: {other:?}");
            ExitCode::FAILURE
        }
    }
}

const MAX_MAPREDUCE_SCRIPT_BYTES: usize = 64 * 1024;

fn is_builtin_mapper(s: &str) -> bool {
    matches!(s, "word_count" | "line_count" | "identity")
}

fn is_builtin_reducer(s: &str) -> bool {
    matches!(s, "sum" | "count" | "max" | "min" | "concat")
}

fn looks_like_script_path(s: &str) -> bool {
    s.contains('/') || s.contains('\\') || s.ends_with(".sh")
}

fn read_mapreduce_script(kind: &str, cmd_flag: &str, path_str: &str) -> Result<String, String> {
    let path = Path::new(path_str);
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("Failed to read {kind} script '{path_str}': {e}"))?;
    if meta.len() > MAX_MAPREDUCE_SCRIPT_BYTES as u64 {
        return Err(format!(
            "Failed to read {kind} script '{path_str}': script size ({} bytes) exceeds maximum inline MapReduce script size ({} bytes); use {cmd_flag} or stage the script onto workers",
            meta.len(),
            MAX_MAPREDUCE_SCRIPT_BYTES
        ));
    }
    let script = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {kind} script '{path_str}': {e}"))?;
    if script.len() > MAX_MAPREDUCE_SCRIPT_BYTES {
        return Err(format!(
            "Failed to read {kind} script '{path_str}': script size ({} bytes) exceeds maximum inline MapReduce script size ({} bytes); use {cmd_flag} or stage the script onto workers",
            script.len(),
            MAX_MAPREDUCE_SCRIPT_BYTES
        ));
    }
    Ok(script)
}

fn split_command_line(cmd: &str) -> Result<(String, Vec<String>), String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut token_started = false;
    let mut chars = cmd.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' if !in_single => {
                let should_escape = if in_double {
                    matches!(chars.peek(), Some('"' | '\\'))
                } else {
                    chars.peek().is_some_and(|next| {
                        *next == '"' || *next == '\'' || *next == '\\' || next.is_whitespace()
                    })
                };
                if should_escape {
                    if let Some(escaped_ch) = chars.next() {
                        current.push(escaped_ch);
                    }
                } else {
                    current.push('\\');
                }
                token_started = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
                token_started = true;
            }
            '"' if !in_single => {
                in_double = !in_double;
                token_started = true;
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if token_started {
                    tokens.push(std::mem::take(&mut current));
                    token_started = false;
                }
            }
            c => {
                current.push(c);
                token_started = true;
            }
        }
    }
    if in_single || in_double {
        return Err("unclosed quote in command string".to_string());
    }
    if token_started {
        tokens.push(current);
    }
    let mut iter = tokens.into_iter();
    let program = iter
        .next()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| "command cannot be empty".to_string())?;
    Ok((program, iter.collect()))
}

async fn run_mapreduce(
    args: MapReduceArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_mapreduce_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_mapreduce_inner(
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
        CLIENT_MASTER_ENV_VARS,
        submit_cfg.as_ref().and_then(|s| s.target_master()),
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

    let mapper_spec = if let Some(ref cmd) = args.map_cmd {
        match split_command_line(cmd) {
            Ok((program, cmd_args)) => MapFunctionSpec::Command {
                program,
                args: cmd_args,
            },
            Err(e) => {
                eprintln!("Invalid --map-cmd '{cmd}': {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if is_builtin_mapper(&args.mapper) {
        MapFunctionSpec::Builtin {
            operator: args.mapper,
        }
    } else if looks_like_script_path(&args.mapper) {
        match read_mapreduce_script("mapper", "--map-cmd", &args.mapper) {
            Ok(script) => MapFunctionSpec::ShellScript { script },
            Err(msg) => {
                eprintln!("{msg}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        MapFunctionSpec::Builtin {
            operator: args.mapper,
        }
    };

    let reducer_spec = if let Some(ref cmd) = args.reduce_cmd {
        match split_command_line(cmd) {
            Ok((program, cmd_args)) => ReduceFunctionSpec::Command {
                program,
                args: cmd_args,
            },
            Err(e) => {
                eprintln!("Invalid --reduce-cmd '{cmd}': {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if is_builtin_reducer(&args.reducer) {
        ReduceFunctionSpec::Builtin {
            operator: args.reducer,
        }
    } else if looks_like_script_path(&args.reducer) {
        match read_mapreduce_script("reducer", "--reduce-cmd", &args.reducer) {
            Ok(script) => ReduceFunctionSpec::ShellScript { script },
            Err(msg) => {
                eprintln!("{msg}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        ReduceFunctionSpec::Builtin {
            operator: args.reducer,
        }
    };

    let timeout_secs = resolve_u64(
        args.timeout_secs,
        &[],
        submit_cfg.as_ref().and_then(|s| s.timeout_secs),
        60,
    )
    .max(1);
    let job = MapReduceJobSpec::new(
        "cli_mapreduce_job",
        chunks,
        mapper_spec,
        reducer_spec,
        args.chunks,
        1,
        timeout_secs,
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
        let err = build_placement(&submit_args(&["--require-tag", "os:macos", "--", "true"]))
            .unwrap_err();
        assert!(err.contains("only the worker or the master"), "{err}");
    }

    #[test]
    fn test_submit_require_isolation_becomes_a_placement_constraint() {
        for (flag, level) in [
            ("fs", IsolationLevel::Fs),
            ("fs+net", IsolationLevel::FsNet),
        ] {
            let placement =
                build_placement(&submit_args(&["--require-isolation", flag, "--", "true"]))
                    .unwrap();
            assert_eq!(placement.isolation, Some(level));
            assert_eq!(placement.to_task_tags(), [format!("require:iso={flag}")]);
        }
        for bad in ["off", "strict", ""] {
            let err = build_placement(&submit_args(&["--require-isolation", bad, "--", "true"]))
                .unwrap_err();
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
    async fn test_master_with_larger_max_active_bytes_accepts_what_default_refuses_and_reports_bytes(
    ) {
        // 1. `--max-active-mb 0` is refused at startup with `ExitCode::FAILURE`.
        let zero_cli = Cli::try_parse_from([
            "oxideswarm",
            "master",
            "--listen",
            "127.0.0.1:0",
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
        let tmp_blob = tempfile::tempdir().unwrap();
        let probe_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let run_master_addr = probe_listener.local_addr().unwrap();
        drop(probe_listener);
        let run_master_addr_str = run_master_addr.to_string();
        let blob_dir_str = tmp_blob.path().to_str().unwrap().to_string();
        let cli = Cli::try_parse_from([
            "oxideswarm",
            "master",
            "--listen",
            &run_master_addr_str,
            "--web-ui-addr",
            "127.0.0.1:0",
            "--max-active-mb",
            "1",
            "--blob-dir",
            &blob_dir_str,
        ])
        .unwrap();
        let Commands::Master(master_args) = cli.command else {
            panic!("expected Master subcommand");
        };
        let master_task = tokio::spawn(run_master(master_args, None, secret_opts));

        let mut connected = None;
        for _ in 0..50 {
            if let Ok(t) = connect_master(&run_master_addr_str, Some(&test_secret)).await {
                connected = Some(t);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut transport = connected.expect("run_master bound and accepted connection");
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

        let map_err =
            read_mapreduce_script("mapper", "--map-cmd", huge.to_str().unwrap()).unwrap_err();
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
    /// `README.md` reference section (or summary table row) for its own subcommand.
    #[test]
    fn test_every_cli_long_flag_is_documented_in_readme() {
        use clap::CommandFactory;

        fn collect_long_flags(
            cmd: &clap::Command,
            path: &str,
            out: &mut Vec<(String, String, bool)>,
        ) {
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
                .unwrap_or_else(|| panic!("missing heading in README.md: {start}"));
            let rest = &text[s..];
            let e = rest
                .find(end)
                .unwrap_or_else(|| panic!("missing heading in README.md after {start}: {end}"));
            &rest[..e]
        }

        let readme = include_str!("../../../README.md");
        let global_sec = section_between(
            readme,
            "#### Global Flags",
            "#### `oxideswarm master` Flags",
        );
        let master_sec = section_between(
            readme,
            "#### `oxideswarm master` Flags",
            "#### `oxideswarm worker` Flags",
        );
        let worker_sec = section_between(
            readme,
            "#### `oxideswarm worker` Flags",
            "#### `oxideswarm submit` Flags",
        );
        let submit_sec = section_between(
            readme,
            "#### `oxideswarm submit` Flags",
            "#### Inspection, MapReduce, Doctor, Secret, Mode & MCP Subcommands",
        );
        let subcmd_table_sec = section_between(
            readme,
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
                            panic!("missing subcommand row in README.md for `{other}`")
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
            "Every CLI long flag and alias in Cli::command() must be documented in its subcommand section of README.md; missing:\n{}",
            missing.join("\n")
        );
    }

    #[tokio::test]
    async fn test_sched2_max_queue_size_scheduler_policy_wire_codec_timeout_and_p2p_ticket() {
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
            resolve_scheduler_policy(Some("round-robin".into()), Some("least-loaded".into()))
                .unwrap(),
            SchedulingPolicy::RoundRobin
        );
        assert!(resolve_scheduler_policy(Some("bogus-policy".into()), None).is_err());

        let built_sched_cfg = build_master_scheduler_config(
            Some("round-robin".into()),
            Some("least-loaded".into()),
            false,
            72.5,
            true,
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
        assert!(build_master_scheduler_config(
            Some("bogus-policy".into()),
            None,
            true,
            85.0,
            false
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
            run_submit_with_secret(s_default, Some(cfg_timeout_42.clone()), secret_opts.clone())
                .await,
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
            let session_key =
                rusty_grid_core::cluster_auth::client_handshake(&mut wt, &test_secret)
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
    async fn test_run_cli_registers_mode_runner_hook() {
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
        std::env::remove_var("RUSTY_GRID_NO_BATTERY");
        std::env::remove_var("OXIDE_SWARM_NO_BATTERY");
        std::env::remove_var("RUSTY_GRID_DISABLE_BATTERY");

        assert!(!resolve_no_battery());

        std::env::set_var("OXIDE_SWARM_NO_BATTERY", "yes");
        std::env::set_var("RUSTY_GRID_DISABLE_BATTERY", "on");
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
                std::env::set_var("RUSTY_GRID_NO_BATTERY", "1");
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

        std::env::remove_var("RUSTY_GRID_NO_BATTERY");
        std::env::remove_var("OXIDE_SWARM_NO_BATTERY");
        std::env::remove_var("RUSTY_GRID_DISABLE_BATTERY");
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

        let err = build_placement(&submit_args(&["--require-tool", "bad:tool", "--", "true"]))
            .unwrap_err();
        assert!(err.contains("require:tool"), "{err}");

        let tag_err = build_placement(&submit_args(&["--require-tag", "tool:cargo", "--", "true"]))
            .unwrap_err();
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

    #[tokio::test]
    async fn test_submit_compile_builds_crate_with_local_dependency_and_avoids_worker_without_cargo(
    ) {
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
            let expected_relay_url =
                rusty_grid_core::transport::validate_relay_url(&from_cli).unwrap();
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
}
