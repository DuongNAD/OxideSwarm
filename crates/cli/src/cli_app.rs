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

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use uuid::Uuid;

use rusty_grid_core::error::GridError;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport, WireCodec};
use rusty_grid_core::transport::GridStream;

use crate::config::{
    load_config_file, resolve_bool, resolve_string, ConfigFile, SecretSection, SecretText,
    CLIENT_MASTER_ENV_VARS,
};
use crate::connect::{report_master_failure, AUTH_EXIT_CODE, DEFAULT_MASTER_ADDR};
use crate::doctor::{run_doctor, DoctorOptions};
use crate::mode_cli;
use crate::secret_cmd::{client_secret, peer_insecure, run_secret, SecretCliArgs, SecretOpts};

mod mapreduce;
mod master;
mod shard;
mod submit;
mod tasks;
mod worker;
mod workspaces;

pub use shard::{run_shard, ShardArgs};
pub(crate) use submit::normalize_workspace_cwd;
pub use submit::{build_task_spec, run_submit, validate_glb_header};
pub use tasks::{run_cancel, run_result};
pub use workspaces::run_workspaces;

/// Bytes in a MiB, for the `*-mb` options.
pub(super) const MIB: u64 = 1024 * 1024;

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

    /// Run a command or test suite across N parallel shards
    Shard(ShardArgs),

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

    /// Join this machine to a cluster with a join token or invite read from standard input
    Join(crate::join::JoinArgs),

    /// Create, list, or revoke OXS-AUTH v3 machine enrollment invites
    Invite(crate::members_cmd::InviteArgs),

    /// Create encrypted USB setup kits (`oxideswarm usb create <DIR>`)
    Usb(crate::usb_kit::UsbCliArgs),

    /// List, approve, reject, revoke, or rename enrolled cluster members
    Members(crate::members_cmd::MembersCliArgs),

    /// Workflow modes and presets (test, dev, doc, research)
    Mode(mode_cli::ModeCliArgs),

    /// Model Context Protocol (MCP) server for external AI coding agents (Claude Desktop, Cline, Cursor)
    Mcp(crate::mcp::McpArgs),

    /// Standalone HTTP rendezvous registry for cross-subnet discovery
    Rendezvous(crate::rendezvous_cmd::RendezvousArgs),
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

    #[arg(
        long,
        value_name = "FACTOR",
        allow_hyphen_values = true,
        help = "Speed-aware placement hold-back factor (> 1.0, or 0 to disable; default: 1.5)"
    )]
    pub speed_hold_factor: Option<String>,

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
        long = "discovery-port",
        value_name = "PORT",
        help = "UDP port of the master's discovery beacon (default 8089; 0 turns the beacon off). \
                A worker or client finds it only with the same port: --master auto:PORT"
    )]
    pub discovery_port: Option<u16>,

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
        value_name = "N",
        help = "Maximum tasks placed in a single scheduling pass (default 1024; env RUSTY_GRID_MAX_BATCH_SIZE)"
    )]
    pub max_batch_size: Option<String>,

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
        help = "Address of the Master's grid port (default 127.0.0.1:8088, the dashboard is on :8080); a bare host gets :8088; 'auto' discovers it on the LAN; for a P2P ticket use --p2p-ticket"
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
        help = "User tag advertised to the master, matched by `submit --require-tag` (repeatable; env RUSTY_GRID_WORKER_TAGS, comma-separated). The prefixes os:, arch:, slots:, link:, id:, state:, iso:, tool:, feat:, ext:, proto:, opt: and speed: are reserved"
    )]
    pub tags: Vec<String>,

    #[arg(
        long,
        value_name = "LEVEL",
        help = "Confine task processes at the OS level: off (default), fs (a task writes only its own sandbox and cannot read the owner's home, other sandboxes or the blob cache) or fs+net (fs plus no network sockets). The worker refuses to start where the level cannot be enforced (for example Linux without Landlock); supported on macOS, Linux and Windows; advertised as the iso:<level> tag; env RUSTY_GRID_ISOLATION, config [worker].isolation"
    )]
    pub isolation: Option<String>,

    #[arg(
        long = "isolation-read-dir",
        alias = "allow-read-dir",
        value_name = "DIR",
        help = "Additional read-only directory accessible to isolated tasks (repeatable; e.g. a Python, CUDA, or Blender runtime); refuses broad roots, secrets, and symlinks; env RUSTY_GRID_ISOLATION_READ_DIRS, config [worker].isolation_read_dirs"
    )]
    pub isolation_read_dirs: Vec<PathBuf>,

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

    #[arg(
        long,
        alias = "vram",
        help = "Minimum GPU VRAM in MB required (implies --gpu when > 0)"
    )]
    pub vram_mb: Option<u64>,

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

    #[arg(long, value_name = "N", help = crate::verify::VERIFY_HELP)]
    pub verify: Option<usize>,

    #[arg(
        long = "verify-file",
        value_name = "PATH",
        requires = "verify",
        help = "With --verify: compare only this sandbox file (a checksum file the task writes, e.g. checksums.sha256) byte for byte instead of every declared output; it is fetched even without --output"
    )]
    pub verify_file: Option<String>,

    #[arg(
        long = "verify-any-system",
        requires = "verify",
        help = crate::verify::VERIFY_ANY_SYSTEM_HELP
    )]
    pub verify_any_system: bool,

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

    #[arg(
        long,
        help = "Mark this task as safe to re-run speculatively on another worker if a running attempt becomes a straggler"
    )]
    pub repeatable: bool,

    #[arg(
        long = "matrix",
        value_name = "KEY=V1,V2",
        help = crate::matrix::MATRIX_HELP
    )]
    pub matrix: Vec<String>,

    #[arg(
        long = "max-matrix-cells",
        value_name = "N",
        requires = "matrix",
        help = crate::matrix::MAX_MATRIX_CELLS_HELP
    )]
    pub max_matrix_cells: Option<usize>,

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

fn init_default_fmt_subscriber(env_filter: tracing_subscriber::EnvFilter, info_to_stdout: bool) {
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    let builder = tracing_subscriber::fmt().with_env_filter(env_filter);
    let _ = if !info_to_stdout {
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
}

pub async fn run_cli() -> ExitCode {
    run_cli_with(Cli::parse()).await
}

pub async fn run_cli_with(cli: Cli) -> ExitCode {
    #[cfg(feature = "otel")]
    let (otel_layer, otel_handle) = if matches!(cli.command, Commands::Master(_)) {
        match rusty_grid_master::otel::OtlpConfig::from_env() {
            Ok(Some(cfg)) => {
                let (layer, handle) = rusty_grid_master::otel::start_otlp_exporter(cfg);
                (Some(layer), Some(handle))
            }
            Ok(None) => (None, None),
            Err(e) => {
                eprintln!("Configuration error: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        (None, None)
    };

    // Initialize tracing subscriber if not already set
    let log_level = if cli.verbose {
        "debug"
    } else if cli.quiet {
        "error"
    } else {
        "info"
    };
    #[allow(unused_mut)]
    let mut env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_filter(log_level)));
    #[cfg(feature = "otel")]
    if otel_layer.is_some() {
        env_filter = env_filter.add_directive(
            "oxideswarm::otel=info"
                .parse()
                .expect("static tracing directive is valid"),
        );
    }
    let info_to_stdout = matches!(
        cli.command,
        Commands::Master(_) | Commands::Worker(_) | Commands::Rendezvous(_) | Commands::Mode(_)
    );
    #[cfg(feature = "otel")]
    if let Some(otel_layer) = otel_layer {
        use tracing_subscriber::fmt::writer::MakeWriterExt;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        let _ = if !info_to_stdout {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
                .with(otel_layer)
                .try_init()
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    tracing_subscriber::fmt::layer().with_writer(
                        std::io::stderr
                            .with_max_level(tracing::Level::WARN)
                            .or_else(std::io::stdout),
                    ),
                )
                .with(otel_layer)
                .try_init()
        };
    } else {
        init_default_fmt_subscriber(env_filter, info_to_stdout);
    }
    #[cfg(not(feature = "otel"))]
    init_default_fmt_subscriber(env_filter, info_to_stdout);

    let config_file = match load_config_file(cli.config.as_deref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let secret_opts = cli.secret_opts();
    crate::connect::set_cli_master_key_flag(cli.master_key.clone());
    rusty_grid_core::mode::set_mode_runner_hook(crate::mode_cli::execute_mode_action);
    let exit_code = match cli.command {
        #[cfg(feature = "otel")]
        Commands::Master(args) => {
            master::run_master_with_otel(args, config_file, secret_opts, otel_handle).await
        }
        #[cfg(not(feature = "otel"))]
        Commands::Master(args) => master::run_master(args, config_file, secret_opts).await,
        Commands::Worker(args) => worker::run_worker(args, config_file, secret_opts).await,
        Commands::Submit(args) => {
            submit::run_submit_with_secret(args, config_file, secret_opts).await
        }
        Commands::Shard(args) => shard::run_shard_with_secret(args, config_file, secret_opts).await,
        Commands::Status(args) => tasks::run_status(args, config_file, secret_opts).await,
        Commands::Cancel(args) => run_cancel(args, config_file, secret_opts).await,
        Commands::Result(args) => run_result(args, config_file, secret_opts).await,
        Commands::Workers(args) => tasks::run_workers(args, config_file, secret_opts).await,
        Commands::Workspaces(args) => run_workspaces(args, config_file, secret_opts).await,
        Commands::Mapreduce(args) => mapreduce::run_mapreduce(args, config_file, secret_opts).await,
        Commands::Doctor(args) => run_doctor_command(args, config_file, secret_opts).await,
        Commands::Secret(args) => run_secret(args, &secret_opts, config_file.as_ref()),
        Commands::Join(args) => {
            crate::join::run_join(args, &secret_opts, config_file.as_ref()).await
        }
        Commands::Invite(args) => {
            crate::members_cmd::run_invite(args, config_file, secret_opts).await
        }
        Commands::Usb(args) => crate::usb_kit::run_usb(args, config_file, secret_opts).await,
        Commands::Members(args) => {
            crate::members_cmd::run_members(args, config_file, secret_opts).await
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
        Commands::Rendezvous(args) => {
            crate::rendezvous_cmd::run_rendezvous(args, secret_opts).await
        }
    };
    rusty_grid_core::transport::drain_pending_p2p_client_closes().await;
    exit_code
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

/// Resolve `--wire-codec` / `RUSTY_GRID_WIRE_CODEC` / config `wire_codec`,
/// returning an error prefixed with the source name on unknown codec names.
pub(super) fn resolve_wire_codec(
    flag: Option<String>,
    cfg_val: Option<String>,
) -> Result<WireCodec, String> {
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

pub(super) fn resolve_auth_mode(
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

/// Resolves `--state-dir` / `RUSTY_GRID_STATE_DIR` / `master.state_dir` without mutating the
/// process environment.
pub(super) fn resolve_state_dir_value(
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
pub(super) async fn wait_for_worker_shutdown_signal() {
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
pub(super) async fn exchange(
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

#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod tests;
