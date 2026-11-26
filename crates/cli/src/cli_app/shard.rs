//! `oxideswarm shard` CLI arguments, offline validation, and orchestration (`AX-2`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::watch;

use rusty_grid_core::blob::{validate_output_patterns, OutputPattern, TaskInput, SANDBOX_ROOT};
use rusty_grid_core::blob_transfer::download_blob;
use rusty_grid_core::placement::{IsolationLevel, Placement, OPT_REPEATABLE_TAG, REQUIRE_PREFIX};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::{StagedFile, Task, TaskRequirements, TaskSpec, WorkspaceRef};

use crate::blobs::{
    download_outputs, scan_inputs, scan_workspace_source, upload_inputs, BlobConnection,
    PreparedInputs, UploadReport, WorkspaceLimits,
};
use crate::config::{
    resolve_string, resolve_u64, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS,
};
use crate::connect::{
    client_p2p_minimal_requested, connect_master, report_master_failure, save_output_artifacts,
    with_client_relay_url, DEFAULT_MASTER_ADDR,
};
use crate::matrix::{resolve_run_wait_secs, safe_deadline, CellStatus};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};
use crate::shard::{
    blob_matches_junit, check_build_once_path_collision, execute_build_once, execute_shards,
    filter_eligible_workers, junit, normalize_build_once_workers, plan_build_once_groups,
    scan_build_source, spread_shards_swrr, upload_build_source, validate_build_source,
    validate_shard_count, BuildOnceOutcome, ShardReport, BUILD_ONCE_NEXTEST_JUNIT_REL,
    DEFAULT_BUILD_ONCE_TIMEOUT_SECS, DEFAULT_JUNIT_GLOB, DEFAULT_SHARD_OUTPUT_BYTES,
    DEFAULT_SHARD_TIMEOUT_SECS,
};

use super::{
    exchange, normalize_workspace_cwd, resolve_relay_url, INSECURE_PEER_HELP, MIB, RELAY_URL_HELP,
};

const MAX_TASK_INPUT_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Parser, Debug, Clone, Default)]
pub struct ShardArgs {
    #[arg(
        long,
        value_name = "N",
        help = "Number of shards to run (1..=256); injects 1-based OX_SHARD_INDEX and OX_SHARD_TOTAL"
    )]
    pub count: usize,

    #[arg(
        long = "build-once",
        help = "Build once per (os, arch) with cargo nextest archive and distribute the archive via Task::inputs"
    )]
    pub build_once: bool,

    #[arg(
        long = "build-source",
        value_name = "DIR",
        help = "Local Rust project directory to archive in --build-once mode (default: .)"
    )]
    pub build_source: Option<PathBuf>,

    #[arg(
        long = "junit-out",
        value_name = "PATH",
        help = "Merge per-shard JUnit XML reports into PATH"
    )]
    pub junit_out: Option<PathBuf>,

    #[arg(
        long = "junit-glob",
        value_name = "PATTERN",
        requires = "junit_out",
        help = "Sandbox-relative glob selecting per-shard JUnit XML files (default: **/junit*.xml)"
    )]
    pub junit_glob: Option<String>,

    #[arg(
        long = "output-dir",
        value_name = "DIR",
        help = "Local directory where each shard's outputs are saved under <DIR>/<shard_slug>/"
    )]
    pub output_dir: Option<PathBuf>,

    #[arg(
        long = "fail-fast",
        help = "Cancel remaining in-flight shards via CancelTask as soon as any shard fails"
    )]
    pub fail_fast: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(
        long,
        help = "Address of the Master's grid port (default 127.0.0.1:8088); a bare host gets :8088; 'auto' discovers it on the LAN"
    )]
    pub master: Option<String>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,

    #[arg(
        long = "type",
        alias = "task-type",
        default_value = "command",
        help = "Task type: command (default, also `generic`) or shell"
    )]
    pub task_type: String,

    #[arg(
        long,
        help = "Run the shard command through a shell (sh / cmd.exe), shorthand for --type shell"
    )]
    pub shell: bool,

    #[arg(
        long,
        help = "Command executable (or shell command string when --shell / --type shell)"
    )]
    pub command: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Shell tasks: send this local file as the script (at most 32 MiB)"
    )]
    pub script_file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PROG",
        help = "Shell tasks: run the script with PROG instead of sh or cmd.exe"
    )]
    pub interpreter: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Command tasks: send this local file as standard input (at most 32 MiB)"
    )]
    pub stdin_file: Option<PathBuf>,

    #[arg(
        long,
        default_value = "1",
        help = "Minimum CPU cores required per shard"
    )]
    pub cores: usize,

    #[arg(
        long,
        alias = "ram",
        default_value = "0",
        help = "Minimum RAM in MB required per shard"
    )]
    pub ram_mb: u64,

    #[arg(
        long,
        alias = "vram",
        help = "Minimum GPU VRAM in MB required per shard (implies --gpu when > 0)"
    )]
    pub vram_mb: Option<u64>,

    #[arg(long, help = "Require GPU worker node")]
    pub gpu: bool,

    #[arg(
        long,
        help = "Per-shard execution timeout in seconds (default: 300, or 1800 with --build-once / --workspace)"
    )]
    pub timeout_secs: Option<u64>,

    #[arg(
        long,
        value_name = "SECS",
        help = "Whole-run client wait budget in seconds (default: tasks * timeout_secs + 5)"
    )]
    pub wait_secs: Option<u64>,

    #[arg(long, help = "Maximum execution retries per shard")]
    pub max_retries: Option<u32>,

    #[arg(
        long,
        help = "Mark each shard task as safe for speculative straggler re-run"
    )]
    pub repeatable: bool,

    #[arg(
        long,
        value_name = "OS",
        help = "Run only on workers with this OS: macos, windows, linux, android"
    )]
    pub os: Option<String>,

    #[arg(
        long,
        value_name = "ARCH",
        help = "Run only on workers with this CPU arch: x86_64, aarch64"
    )]
    pub arch: Option<String>,

    #[arg(
        long,
        value_name = "NAME|ID",
        help = "Run only on this worker (name or UUID)"
    )]
    pub worker: Option<String>,

    #[arg(
        long = "require-isolation",
        value_name = "LEVEL",
        help = "Run only on workers confined at least at this level: fs or fs+net"
    )]
    pub require_isolation: Option<String>,

    #[arg(
        long = "require-tag",
        value_name = "TAG",
        help = "Run only on workers carrying this user tag (repeatable)"
    )]
    pub require_tags: Vec<String>,

    #[arg(
        long = "require-tool",
        value_name = "TOOL",
        help = "Run only on workers advertising this host tool (repeatable)"
    )]
    pub require_tools: Vec<String>,

    #[arg(
        long = "env",
        value_name = "KEY=VALUE",
        help = "Environment variable for each shard task (repeatable)"
    )]
    pub env: Vec<String>,

    #[arg(
        long = "input",
        visible_alias = "file",
        value_name = "LOCAL[:SANDBOX]",
        help = "Stage a local file or directory into each shard sandbox (repeatable)"
    )]
    pub inputs: Vec<String>,

    #[arg(
        long = "output",
        alias = "expected-output",
        value_name = "PATTERN",
        help = "Bring back sandbox files matching PATTERN from each shard (repeatable)"
    )]
    pub outputs: Vec<String>,

    #[arg(
        long = "workspace",
        value_name = "NAME",
        help = "Run in a persistent single-worker workspace (incompatible with --build-once, --input, and multi-worker sharding)"
    )]
    pub workspace: Option<String>,

    #[arg(
        long = "sync",
        value_name = "DIR",
        help = "Workspace tasks: sync DIR into the workspace before running"
    )]
    pub sync: Option<PathBuf>,

    #[arg(
        long = "no-sync",
        help = "Workspace tasks: run without syncing a local directory"
    )]
    pub no_sync: bool,

    #[arg(
        long = "cwd",
        value_name = "REL",
        help = "Workspace command tasks: relative working directory inside the workspace"
    )]
    pub cwd: Option<String>,

    #[arg(
        long = "workspace-max-files",
        value_name = "N",
        help = "Maximum files one workspace or --build-once source scan may include (default: 100000)"
    )]
    pub workspace_max_files: Option<usize>,

    #[arg(
        long = "workspace-max-mb",
        value_name = "MB",
        help = "Maximum megabytes one workspace or --build-once source scan may include (default: 2048)"
    )]
    pub workspace_max_mb: Option<u64>,

    #[arg(
        last = true,
        help = "Command and arguments (or extra cargo nextest run arguments in --build-once mode)"
    )]
    pub args: Vec<String>,
}

fn read_bounded_file(path: &Path, flag: &str) -> Result<Vec<u8>, String> {
    let len = std::fs::metadata(path)
        .map_err(|e| format!("{flag} '{}': {e}", path.display()))?
        .len();
    if len > MAX_TASK_INPUT_BYTES {
        return Err(format!(
            "{flag} '{}': {len} bytes exceeds the {} MiB limit",
            path.display(),
            MAX_TASK_INPUT_BYTES / MIB
        ));
    }
    std::fs::read(path).map_err(|e| format!("{flag} '{}': {e}", path.display()))
}

fn parse_env_pairs(pairs: &[String]) -> Result<HashMap<String, String>, String> {
    let mut env = HashMap::new();
    for pair in pairs {
        match pair.split_once('=') {
            Some((key, value)) if !key.is_empty() => {
                env.insert(key.to_string(), value.to_string());
            }
            _ => return Err(format!("--env '{pair}': expected KEY=VALUE")),
        }
    }
    Ok(env)
}

fn build_shard_placement(args: &ShardArgs) -> Result<Placement, String> {
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

fn is_macos_root_alias_path(cur: &Path) -> bool {
    cfg!(target_os = "macos") && matches!(cur.to_str(), Some("/var" | "/tmp" | "/etc"))
}

fn validate_junit_out_path(path: &Path) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err("--junit-out cannot be empty".to_string());
    }
    if path.file_name().and_then(|n| n.to_str()).is_none() {
        return Err(format!(
            "--junit-out '{}': invalid file name",
            path.display()
        ));
    }
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if meta.file_type().is_symlink() {
            return Err(format!(
                "--junit-out '{}': path is a symbolic link, which is refused",
                path.display()
            ));
        }
        if meta.is_dir() {
            return Err(format!(
                "--junit-out '{}': path exists and is a directory",
                path.display()
            ));
        }
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let mut cur = PathBuf::new();
            let mut saw_missing = false;
            for comp in parent.components() {
                cur.push(comp);
                if matches!(
                    comp,
                    std::path::Component::Prefix(_) | std::path::Component::RootDir
                ) {
                    continue;
                }
                if saw_missing && matches!(comp, std::path::Component::ParentDir) {
                    return Err(format!(
                        "--junit-out '{}': parent path contains '..' after a missing component ('{}'), which is refused",
                        path.display(),
                        cur.display()
                    ));
                }
                match std::fs::symlink_metadata(&cur) {
                    Ok(meta) => {
                        let is_macos_root_alias = is_macos_root_alias_path(&cur);
                        if meta.file_type().is_symlink() && !is_macos_root_alias {
                            return Err(format!(
                                "--junit-out '{}': parent '{}' is a symbolic link, which is refused",
                                path.display(),
                                cur.display()
                            ));
                        }
                        if !(meta.is_dir() || (is_macos_root_alias && cur.is_dir())) {
                            return Err(format!(
                                "--junit-out '{}': parent '{}' exists and is not a directory",
                                path.display(),
                                cur.display()
                            ));
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        saw_missing = true;
                    }
                    Err(e) => {
                        return Err(format!(
                            "--junit-out '{}': cannot inspect parent '{}': {e}",
                            path.display(),
                            cur.display()
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_output_dir_path(path: &Path) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err("--output-dir cannot be empty".to_string());
    }
    let mut cur = PathBuf::new();
    let mut saw_missing = false;
    for comp in path.components() {
        cur.push(comp);
        if matches!(
            comp,
            std::path::Component::Prefix(_) | std::path::Component::RootDir
        ) {
            continue;
        }
        if saw_missing && matches!(comp, std::path::Component::ParentDir) {
            return Err(format!(
                "--output-dir '{}': path contains '..' after a missing component ('{}'), which is refused",
                path.display(),
                cur.display()
            ));
        }
        match std::fs::symlink_metadata(&cur) {
            Ok(meta) => {
                let is_macos_root_alias = is_macos_root_alias_path(&cur);
                if meta.file_type().is_symlink() && !is_macos_root_alias {
                    return Err(format!(
                        "--output-dir '{}': path component '{}' is a symbolic link, which is refused",
                        path.display(),
                        cur.display()
                    ));
                }
                if !(meta.is_dir() || (is_macos_root_alias && cur.is_dir())) {
                    return Err(format!(
                        "--output-dir '{}': path component '{}' exists and is not a directory",
                        path.display(),
                        cur.display()
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                saw_missing = true;
            }
            Err(e) => {
                return Err(format!(
                    "--output-dir '{}': cannot inspect path component '{}': {e}",
                    path.display(),
                    cur.display()
                ));
            }
        }
    }
    Ok(())
}

fn ensure_non_symlink_dir_segment(
    cur: &Path,
    label: &str,
    display_path: &Path,
) -> Result<(), String> {
    loop {
        match std::fs::symlink_metadata(cur) {
            Ok(meta) => {
                let is_macos_root_alias = is_macos_root_alias_path(cur);
                if meta.file_type().is_symlink() && !is_macos_root_alias {
                    return Err(format!(
                        "{label} '{}': path component '{}' is a symbolic link, which is refused",
                        display_path.display(),
                        cur.display()
                    ));
                }
                if !(meta.is_dir() || (is_macos_root_alias && cur.is_dir())) {
                    return Err(format!(
                        "{label} '{}': path component '{}' exists and is not a directory",
                        display_path.display(),
                        cur.display()
                    ));
                }
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => match std::fs::create_dir(cur) {
                Ok(()) => {
                    let meta = std::fs::symlink_metadata(cur).map_err(|e| {
                        format!(
                            "{label} '{}': cannot inspect created directory '{}': {e}",
                            display_path.display(),
                            cur.display()
                        )
                    })?;
                    if meta.file_type().is_symlink() || !meta.is_dir() {
                        return Err(format!(
                            "{label} '{}': '{}' is not a regular directory",
                            display_path.display(),
                            cur.display()
                        ));
                    }
                    return Ok(());
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(format!(
                        "{label} '{}': cannot create directory '{}': {e}",
                        display_path.display(),
                        cur.display()
                    ));
                }
            },
            Err(e) => {
                return Err(format!(
                    "{label} '{}': cannot inspect path component '{}': {e}",
                    display_path.display(),
                    cur.display()
                ));
            }
        }
    }
}

fn ensure_non_symlink_shard_output_dir(
    configured_out_dir: Option<&Path>,
    slug: &str,
) -> Result<PathBuf, String> {
    if let Some(out_dir) = configured_out_dir {
        validate_output_dir_path(out_dir)?;
    }
    let base_out_dir = configured_out_dir.unwrap_or(Path::new("."));
    let label = if configured_out_dir.is_some() {
        "--output-dir"
    } else {
        "shard output directory"
    };
    let mut cur = PathBuf::new();
    let mut saw_missing = false;
    for comp in base_out_dir.components() {
        cur.push(comp);
        if matches!(
            comp,
            std::path::Component::Prefix(_) | std::path::Component::RootDir
        ) {
            continue;
        }
        if saw_missing && matches!(comp, std::path::Component::ParentDir) {
            return Err(format!(
                "{label} '{}': path contains '..' after a missing component ('{}'), which is refused",
                base_out_dir.display(),
                cur.display()
            ));
        }
        if matches!(
            std::fs::symlink_metadata(&cur),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound
        ) {
            saw_missing = true;
        }
        ensure_non_symlink_dir_segment(&cur, label, base_out_dir)?;
    }
    cur.push(slug);
    ensure_non_symlink_dir_segment(&cur, label, base_out_dir)?;
    Ok(cur)
}

fn build_shard_spec(args: &ShardArgs, env: HashMap<String, String>) -> Result<TaskSpec, String> {
    let raw_type = args.task_type.trim().to_ascii_lowercase();
    let effective_type = if args.shell {
        if raw_type != "command" && raw_type != "generic" && raw_type != "shell" {
            return Err(format!(
                "--shell cannot be combined with --type {}",
                args.task_type
            ));
        }
        "shell"
    } else {
        match raw_type.as_str() {
            "command" | "generic" => "command",
            "shell" => "shell",
            other => {
                return Err(format!(
                    "--type '{other}' is not supported by `oxideswarm shard` (expected command or shell)"
                ));
            }
        }
    };

    if args.build_once {
        if effective_type != "command" {
            return Err("--build-once cannot be combined with --shell or --type shell".to_string());
        }
        if args.command.is_some() {
            return Err(
                "--build-once runs `cargo nextest archive` and `cargo nextest run` automatically; pass extra nextest run arguments after `--` instead of `--command`"
                    .to_string(),
            );
        }
        if args.script_file.is_some() || args.interpreter.is_some() || args.stdin_file.is_some() {
            return Err(
                "--build-once cannot be combined with --script-file, --interpreter, or --stdin-file"
                    .to_string(),
            );
        }
        return Ok(TaskSpec::Command {
            program: "cargo".to_string(),
            args: Vec::new(),
            env,
            working_dir: None,
            stdin: None,
        });
    }

    if args.build_source.is_some() {
        return Err("--build-source requires --build-once".to_string());
    }

    if effective_type == "shell" {
        if args.stdin_file.is_some() {
            return Err(
                "--stdin-file is only supported for command tasks, not shell tasks".to_string(),
            );
        }
        if args.cwd.is_some() {
            return Err("--cwd is only supported for command tasks, not shell tasks".to_string());
        }
        let script = match &args.script_file {
            Some(path) => {
                if args.command.is_some() || !args.args.is_empty() {
                    return Err(
                        "--script-file cannot be combined with --command or trailing words after --"
                            .to_string(),
                    );
                }
                let bytes = read_bounded_file(path, "--script-file")?;
                String::from_utf8(bytes).map_err(|_| {
                    format!(
                        "--script-file '{}': file is not valid UTF-8",
                        path.display()
                    )
                })?
            }
            None => match (&args.command, args.args.is_empty()) {
                (Some(cmd), _) => std::iter::once(cmd.as_str())
                    .chain(args.args.iter().map(String::as_str))
                    .collect::<Vec<_>>()
                    .join(" "),
                (None, false) => args.args.join(" "),
                (None, true) => {
                    return Err(
                        "shard shell task requires a command (--command, --script-file, or trailing words after --)"
                            .to_string(),
                    );
                }
            },
        };
        if script.trim().is_empty() {
            return Err("shard shell script cannot be empty".to_string());
        }
        return Ok(TaskSpec::ShellScript {
            script,
            interpreter: args.interpreter.clone(),
            env,
        });
    }

    if args.script_file.is_some() || args.interpreter.is_some() {
        return Err("--script-file and --interpreter require --shell or --type shell".to_string());
    }

    let (program, cmd_args) = match (&args.command, args.args.split_first()) {
        (Some(prog), _) if !prog.trim().is_empty() => (prog.clone(), args.args.clone()),
        (Some(_), _) => return Err("--command cannot be empty".to_string()),
        (None, Some((prog, rest))) if !prog.trim().is_empty() => (prog.clone(), rest.to_vec()),
        (None, _) => {
            return Err(
                "shard requires a command (--command PROG or trailing `-- PROG [ARGS...]`, or --build-once)"
                    .to_string(),
            );
        }
    };

    let working_dir = match &args.cwd {
        Some(raw) => {
            if args.workspace.is_none() {
                return Err("--cwd requires --workspace".to_string());
            }
            normalize_workspace_cwd(raw)?
        }
        None => None,
    };

    let stdin = match &args.stdin_file {
        Some(path) => Some(read_bounded_file(path, "--stdin-file")?),
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

fn emit_cli_error(json_mode: bool, message: &str) -> ExitCode {
    if json_mode {
        println!("{}", serde_json::json!({ "error": message }));
    } else {
        eprintln!("{message}");
    }
    ExitCode::FAILURE
}

/// Public entry point for programmatic/test shard execution with default secret options.
pub async fn run_shard(args: ShardArgs, config_file: Option<ConfigFile>) -> ExitCode {
    run_shard_with_secret(args, config_file, SecretOpts::default()).await
}

pub(super) async fn run_shard_with_secret(
    args: ShardArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
        client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => return emit_cli_error(args.json, &e),
    };
    with_client_relay_url(
        relay_url,
        run_shard_with_secret_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_shard_with_secret_inner(
    args: ShardArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if let Err(e) = validate_shard_count(args.count) {
        return emit_cli_error(args.json, &e);
    }

    if let Some(ref ws_name) = args.workspace {
        if let Err(e) = rusty_grid_core::task::validate_workspace_name(ws_name) {
            return emit_cli_error(args.json, &format!("--workspace: {e}"));
        }
        if args.build_once {
            return emit_cli_error(
                args.json,
                "--workspace cannot be combined with --build-once (workloads.rs forbids combining Task::workspace with Task::inputs)",
            );
        }
        if !args.inputs.is_empty() {
            return emit_cli_error(
                args.json,
                "--workspace cannot be combined with --input (workloads.rs forbids combining Task::workspace with Task::inputs)",
            );
        }
        if args.no_sync && args.sync.is_some() {
            return emit_cli_error(args.json, "--no-sync cannot be combined with --sync");
        }
    } else if args.sync.is_some() || args.no_sync || args.cwd.is_some() {
        return emit_cli_error(
            args.json,
            "--sync, --no-sync, and --cwd require --workspace",
        );
    }

    let env = match parse_env_pairs(&args.env) {
        Ok(e) => e,
        Err(e) => return emit_cli_error(args.json, &e),
    };
    let placement = match build_shard_placement(&args) {
        Ok(p) => p,
        Err(e) => return emit_cli_error(args.json, &e),
    };
    let spec = match build_shard_spec(&args, env.clone()) {
        Ok(s) => s,
        Err(e) => return emit_cli_error(args.json, &e),
    };

    if args.build_once {
        let source_dir = args
            .build_source
            .as_deref()
            .unwrap_or_else(|| Path::new("."));
        if let Err(e) = validate_build_source(source_dir) {
            return emit_cli_error(args.json, &e);
        }
    }

    if let Some(ref junit_out) = args.junit_out {
        if let Err(e) = validate_junit_out_path(junit_out) {
            return emit_cli_error(args.json, &e);
        }
    }
    if let Some(ref out_dir) = args.output_dir {
        if let Err(e) = validate_output_dir_path(out_dir) {
            return emit_cli_error(args.json, &e);
        }
    }

    let user_output_globs = if !args.outputs.is_empty() {
        if let Err(e) = validate_output_patterns(&args.outputs) {
            return emit_cli_error(args.json, &format!("--output: {e}"));
        }
        args.outputs.clone()
    } else {
        Vec::new()
    };

    let junit_patterns: Vec<OutputPattern> = if args.junit_out.is_some() {
        if let Some(ref pat) = args.junit_glob {
            match OutputPattern::parse(pat) {
                Ok(parsed) => vec![parsed],
                Err(e) => return emit_cli_error(args.json, &format!("--junit-glob: {e}")),
            }
        } else if args.build_once {
            vec![
                OutputPattern::parse(&format!("**/{BUILD_ONCE_NEXTEST_JUNIT_REL}"))
                    .expect("static pattern is valid"),
                OutputPattern::parse(DEFAULT_JUNIT_GLOB).expect("static pattern is valid"),
            ]
        } else {
            vec![OutputPattern::parse(DEFAULT_JUNIT_GLOB).expect("static pattern is valid")]
        }
    } else {
        Vec::new()
    };

    let mut effective_output_globs = user_output_globs.clone();
    for pat in &junit_patterns {
        let s = pat.as_str().to_string();
        if !effective_output_globs.contains(&s) {
            effective_output_globs.push(s);
        }
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

    let vram_mb = args.vram_mb.filter(|&v| v > 0);
    let is_gpu = args.gpu || vram_mb.is_some();
    let default_timeout = if args.build_once || args.workspace.is_some() {
        DEFAULT_BUILD_ONCE_TIMEOUT_SECS
    } else {
        DEFAULT_SHARD_TIMEOUT_SECS
    };
    let timeout_secs = resolve_u64(
        args.timeout_secs,
        &[],
        submit_cfg.as_ref().and_then(|s| s.timeout_secs),
        default_timeout,
    )
    .max(1);
    let requirements = TaskRequirements {
        cpu_cores: args.cores.max(1),
        ram_mb: args.ram_mb,
        gpu_required: is_gpu,
        timeout_secs,
        max_retries: args.max_retries,
        vram_mb,
        extensions: Vec::new(),
    };

    let started = std::time::Instant::now();
    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            );
        }
    };

    let rpc_timeout = Duration::from_secs(requirements.timeout_secs.clamp(2, 10));
    let workers = match tokio::time::timeout(
        rpc_timeout,
        exchange(
            &mut transport,
            &master_addr,
            secret.is_some(),
            &ClientMessage::ListWorkers,
            "Failed to list workers",
        ),
    )
    .await
    {
        Ok(Ok(ClientResponse::WorkerList { workers })) => workers,
        Ok(Ok(other)) => {
            transport.close().await;
            return emit_cli_error(
                args.json,
                &format!("Unexpected response to ListWorkers: {other:?}"),
            );
        }
        Ok(Err(code)) => {
            transport.close().await;
            return code;
        }
        Err(_) => {
            transport.close().await;
            return emit_cli_error(
                args.json,
                &format!(
                    "Timed out after {}s listing workers from Master",
                    rpc_timeout.as_secs()
                ),
            );
        }
    };
    transport.close().await;

    let eligible = match filter_eligible_workers(
        &workers,
        &placement,
        &requirements,
        args.workspace.is_some(),
        args.build_once,
    ) {
        Ok(e) => e,
        Err(e) => return emit_cli_error(args.json, &e),
    };

    if args.workspace.is_some() && eligible.len() > 1 {
        return emit_cli_error(
            args.json,
            &format!(
                "--workspace '{}' cannot shard across {} eligible workers because workspaces hold an exclusive single-worker lock; pass --worker <NAME|ID> to target 1 worker, or use --input / --build-once",
                args.workspace.as_deref().unwrap_or(""),
                eligible.len()
            ),
        );
    }

    let (eligible_for_swrr, platform_tools) = if args.build_once {
        match normalize_build_once_workers(eligible) {
            Ok(pair) => pair,
            Err(e) => return emit_cli_error(args.json, &e),
        }
    } else {
        (eligible, Default::default())
    };

    let assignments = match spread_shards_swrr(args.count, &eligible_for_swrr, &placement) {
        Ok(a) => a,
        Err(e) => return emit_cli_error(args.json, &e),
    };

    // Scan and upload inputs only after ListWorkers preflight confirms eligible workers exist.
    let extra_inputs: PreparedInputs = if args.inputs.is_empty() {
        PreparedInputs::default()
    } else {
        match scan_inputs(&args.inputs) {
            Ok(p) => p,
            Err(e) => return emit_cli_error(args.json, &e),
        }
    };
    for skipped in &extra_inputs.skipped {
        eprintln!("Warning: --input skips {skipped}");
    }

    let workspace_limits = WorkspaceLimits {
        max_files: args
            .workspace_max_files
            .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_FILES),
        max_bytes: args
            .workspace_max_mb
            .map(|mb| mb.saturating_mul(MIB))
            .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_BYTES),
    };

    let mut total_upload: Option<UploadReport> = None;
    let mut build_source_scan = None;
    if args.build_once {
        let source_dir = args
            .build_source
            .as_deref()
            .unwrap_or_else(|| Path::new("."));
        let scan = match scan_build_source(source_dir, workspace_limits) {
            Ok(s) => s,
            Err(e) => return emit_cli_error(args.json, &e),
        };
        for skipped in &scan.skipped {
            eprintln!("Warning: --build-source skips {skipped}");
        }
        // Check collisions with extra --input paths.
        for file in scan.manifest.files() {
            if extra_inputs.inputs.iter().any(|i| i.path == file.path) {
                return emit_cli_error(
                    args.json,
                    &format!(
                        "--input '{}' collides with scanned source file '{}' (staging.rs:106)",
                        file.path, file.path
                    ),
                );
            }
        }
        match BlobConnection::connect(&master_addr, secret.as_ref()).await {
            Ok(conn) => {
                let res = upload_build_source(&conn, &scan).await;
                conn.close().await;
                match res {
                    Ok(report) => {
                        eprintln!(
                            "Build source: {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                            scan.files,
                            rusty_grid_worker::staging::human_bytes(scan.bytes),
                            report.uploaded,
                            report.blobs,
                            rusty_grid_worker::staging::human_bytes(report.bytes),
                            report.blobs - report.uploaded
                        );
                        total_upload = Some(report);
                    }
                    Err(e) => {
                        return emit_cli_error(
                            args.json,
                            &format!("Uploading build source failed: {e}"),
                        );
                    }
                }
            }
            Err(e) => {
                return emit_cli_error(
                    args.json,
                    &format!("Connecting to Master for build-source upload failed: {e}"),
                );
            }
        }
        build_source_scan = Some(scan);
    }

    if !extra_inputs.inputs.is_empty() {
        match BlobConnection::connect(&master_addr, secret.as_ref()).await {
            Ok(conn) => {
                let res = upload_inputs(&conn, &extra_inputs).await;
                conn.close().await;
                match res {
                    Ok(report) => {
                        eprintln!(
                            "Inputs: {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                            extra_inputs.files,
                            rusty_grid_worker::staging::human_bytes(extra_inputs.bytes),
                            report.uploaded,
                            report.blobs,
                            rusty_grid_worker::staging::human_bytes(report.bytes),
                            report.blobs - report.uploaded
                        );
                        total_upload = Some(match total_upload {
                            Some(prev) => UploadReport {
                                blobs: prev.blobs + report.blobs,
                                uploaded: prev.uploaded + report.uploaded,
                                bytes: prev.bytes + report.bytes,
                            },
                            None => report,
                        });
                    }
                    Err(e) => {
                        return emit_cli_error(
                            args.json,
                            &format!("Uploading --input files failed: {e}"),
                        );
                    }
                }
            }
            Err(e) => {
                return emit_cli_error(
                    args.json,
                    &format!("Connecting to Master for input upload failed: {e}"),
                );
            }
        }
    }

    let workspace_ref = match &args.workspace {
        Some(ws_name) => {
            if args.no_sync {
                Some(WorkspaceRef::new(ws_name.clone()))
            } else {
                let sync_dir = args.sync.as_deref().unwrap_or_else(|| Path::new("."));
                let scan = match scan_workspace_source(sync_dir, workspace_limits) {
                    Ok(s) => s,
                    Err(e) => return emit_cli_error(args.json, &e),
                };
                match BlobConnection::connect(&master_addr, secret.as_ref()).await {
                    Ok(conn) => {
                        let res = upload_inputs(&conn, &scan.prepared).await;
                        conn.close().await;
                        match res {
                            Ok(report) => {
                                total_upload = Some(report);
                            }
                            Err(e) => {
                                return emit_cli_error(
                                    args.json,
                                    &format!("Uploading workspace files failed: {e}"),
                                );
                            }
                        }
                    }
                    Err(e) => {
                        return emit_cli_error(
                            args.json,
                            &format!("Connecting to Master for workspace upload failed: {e}"),
                        );
                    }
                }
                Some(
                    WorkspaceRef::new(ws_name.clone())
                        .with_tree(scan.manifest_id, scan.manifest_len),
                )
            }
        }
        None => None,
    };

    let (ctrl_c_tx, ctrl_c_rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = ctrl_c_tx.send(true);
        }
    });

    let addr_arc = Arc::new(master_addr.clone());
    let secret_arc = Arc::new(secret.clone());
    let connect = Arc::new(move || {
        let addr = Arc::clone(&addr_arc);
        let sec = Arc::clone(&secret_arc);
        async move {
            connect_master(&addr, sec.as_ref().as_ref())
                .await
                .map_err(|e| format!("could not connect to the master: {e}"))
        }
    });

    let mut template_inputs = extra_inputs.inputs.clone();
    let mut build_once_map = None;
    let mut build_once_list: Vec<BuildOnceOutcome> = Vec::new();
    let mut nextest_config_staged: Option<StagedFile> = None;
    let mut run_deadline: Option<tokio::time::Instant> = None;

    if let Some(ref scan) = build_source_scan {
        let manifest_input = TaskInput::tree(SANDBOX_ROOT, scan.manifest_id, scan.manifest_len);
        let (groups, config_rel_opt) = match plan_build_once_groups(
            &assignments,
            &platform_tools,
            args.worker.as_deref(),
            &scan.manifest,
            &extra_inputs.inputs,
            args.junit_out.is_some(),
        ) {
            Ok(res) => res,
            Err(e) => return emit_cli_error(args.json, &e),
        };
        if let Some(ref cfg_rel) = config_rel_opt {
            if let Err(e) =
                check_build_once_path_collision(cfg_rel, &scan.manifest, &extra_inputs.inputs)
            {
                return emit_cli_error(args.json, &e);
            }
            let cfg_toml =
                format!("[profile.default.junit]\npath = \"{BUILD_ONCE_NEXTEST_JUNIT_REL}\"\n");
            nextest_config_staged = Some(StagedFile::new(cfg_rel, cfg_toml.into_bytes(), false));
        }

        let run_wait_secs = resolve_run_wait_secs(
            args.wait_secs,
            groups.len() + assignments.len(),
            requirements.timeout_secs,
        );
        let shared_deadline = safe_deadline(Duration::from_secs(run_wait_secs));
        run_deadline = Some(shared_deadline);
        let build_once_budget =
            shared_deadline.saturating_duration_since(tokio::time::Instant::now());

        let outcomes = match execute_build_once(
            &groups,
            &manifest_input,
            &extra_inputs.inputs,
            &placement,
            &requirements,
            &env,
            args.repeatable,
            &workers,
            build_once_budget,
            Some(ctrl_c_rx.clone()),
            Arc::clone(&connect),
        )
        .await
        {
            Ok(map) => map,
            Err(e) => return emit_cli_error(args.json, &e),
        };

        build_once_list = outcomes.values().cloned().collect();
        build_once_map = Some(outcomes);
        template_inputs.insert(0, manifest_input);
    }

    let mut template = Task::new(spec, requirements.clone())
        .with_placement(&placement)
        .with_inputs(template_inputs)
        .with_output_globs(effective_output_globs);
    if args.repeatable {
        template.tags.push(OPT_REPEATABLE_TAG.to_string());
    }
    if let Some(ws) = workspace_ref {
        template = template.with_workspace(ws);
    }

    let shard_wait_budget = match run_deadline {
        Some(dl) => dl.saturating_duration_since(tokio::time::Instant::now()),
        None => Duration::from_secs(resolve_run_wait_secs(
            args.wait_secs,
            assignments.len(),
            requirements.timeout_secs,
        )),
    };
    let mut executions = execute_shards(
        assignments,
        &workers,
        &template,
        build_once_map.as_ref(),
        nextest_config_staged.as_ref(),
        &args.args,
        shard_wait_budget,
        args.fail_fast,
        Some(ctrl_c_rx),
        None,
        connect,
    )
    .await;

    let download_to_disk = args.output_dir.is_some() || !user_output_globs.is_empty();
    let any_outputs = executions
        .iter()
        .any(|e| e.result.as_ref().is_some_and(|r| !r.outputs.is_empty()));

    let mut junit_inputs = Vec::with_capacity(executions.len());
    if any_outputs && (download_to_disk || args.junit_out.is_some()) {
        match BlobConnection::connect(&master_addr, secret.as_ref()).await {
            Ok(conn) => {
                for exec in &mut executions {
                    let Some(ref result) = exec.result else {
                        continue;
                    };
                    if download_to_disk && !result.outputs.is_empty() {
                        match ensure_non_symlink_shard_output_dir(
                            args.output_dir.as_deref(),
                            &exec.assignment.slug,
                        ) {
                            Ok(shard_dir) => {
                                match download_outputs(&conn, &result.outputs, &shard_dir).await {
                                    Ok(paths) => exec.saved_outputs = Some(paths),
                                    Err(e) => {
                                        exec.download_error =
                                            Some(format!("output download failed: {e}"));
                                    }
                                }
                            }
                            Err(e) => {
                                exec.download_error = Some(e);
                            }
                        }
                    }

                    if args.junit_out.is_some() {
                        let mut blobs = Vec::new();
                        let mut junit_dl_error: Option<String> = None;
                        for output in &result.outputs {
                            if blob_matches_junit(&output.path, &junit_patterns) {
                                let mut buf = Vec::new();
                                match download_blob(&conn, output.id, Some(output.size), &mut buf)
                                    .await
                                {
                                    Ok(_) => blobs.push(junit::ShardJunitBlob {
                                        path: output.path.clone(),
                                        bytes: buf,
                                    }),
                                    Err(e) => {
                                        let msg = format!(
                                            "JUnit blob '{}' download failed: {e}",
                                            output.path
                                        );
                                        junit_dl_error.get_or_insert_with(|| msg.clone());
                                        exec.download_error.get_or_insert(msg);
                                    }
                                }
                            }
                        }
                        let (junit_status, junit_error) = if junit_dl_error.is_some() {
                            (CellStatus::Error, exec.download_error.clone())
                        } else {
                            (exec.status, None)
                        };
                        junit_inputs.push(junit::ShardJunitInput {
                            shard_index: exec.assignment.index,
                            status: junit_status,
                            duration_ms: result.execution_time_ms,
                            stderr_excerpt: result.stderr_str().trim().to_string(),
                            error: junit_error,
                            blobs,
                        });
                    }
                }
                conn.close().await;
            }
            Err(e) => {
                for exec in &mut executions {
                    if exec.result.as_ref().is_some_and(|r| !r.outputs.is_empty()) {
                        exec.download_error =
                            Some(format!("failed to connect for output download: {e}"));
                    }
                    if args.junit_out.is_some() && exec.result.is_some() {
                        let msg = format!("failed to connect for JUnit download: {e}");
                        junit_inputs.push(junit::ShardJunitInput {
                            shard_index: exec.assignment.index,
                            status: CellStatus::Error,
                            duration_ms: exec.result.as_ref().map_or(0, |r| r.execution_time_ms),
                            stderr_excerpt: msg.clone(),
                            error: Some(msg),
                            blobs: Vec::new(),
                        });
                    }
                }
            }
        }
    }

    if args.junit_out.is_some() {
        for exec in &executions {
            if let Some(ref r) = exec.result {
                if !any_outputs {
                    junit_inputs.push(junit::ShardJunitInput {
                        shard_index: exec.assignment.index,
                        status: exec.status,
                        duration_ms: r.execution_time_ms,
                        stderr_excerpt: r.stderr_str().trim().to_string(),
                        error: exec.error.clone(),
                        blobs: Vec::new(),
                    });
                }
            } else {
                junit_inputs.push(junit::ShardJunitInput {
                    shard_index: exec.assignment.index,
                    status: exec.status,
                    duration_ms: 0,
                    stderr_excerpt: exec.error.clone().unwrap_or_default(),
                    error: exec.error.clone(),
                    blobs: Vec::new(),
                });
            }
        }
    }

    if download_to_disk {
        for exec in &mut executions {
            let Some(ref result) = exec.result else {
                continue;
            };
            if !result.artifacts.is_empty() {
                match ensure_non_symlink_shard_output_dir(
                    args.output_dir.as_deref(),
                    &exec.assignment.slug,
                ) {
                    Ok(shard_dir) => match save_output_artifacts(&result.artifacts, &shard_dir) {
                        Ok(paths) => exec.saved_artifacts = Some(paths),
                        Err(e) => {
                            exec.download_error
                                .get_or_insert_with(|| format!("artifact save failed: {e}"));
                        }
                    },
                    Err(e) => {
                        exec.download_error.get_or_insert(e);
                    }
                }
            }
        }
    }

    let (junit_summary, junit_error) = if let Some(ref junit_path) = args.junit_out {
        let glob_desc = junit_patterns
            .iter()
            .map(OutputPattern::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        match junit::merge_shard_junit_inputs(junit_inputs, &glob_desc) {
            Ok(merged) => match junit::write_junit_atomic(junit_path, &merged.xml) {
                Ok(()) => (
                    Some(crate::shard::JunitSummary {
                        output_path: junit_path.display().to_string(),
                        tests: merged.tests,
                        failures: merged.failures,
                        errors: merged.errors,
                        skipped: merged.skipped,
                        time_secs: merged.time_secs,
                        suites: merged.suites,
                        synthesized_shards: merged.synthesized_shards,
                        parse_errors: merged.parse_errors,
                    }),
                    None,
                ),
                Err(e) => (None, Some(e)),
            },
            Err(e) => (None, Some(e)),
        }
    } else {
        (None, None)
    };

    let waited_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let report = ShardReport::from_executions(
        executions,
        waited_ms,
        DEFAULT_SHARD_OUTPUT_BYTES,
        build_once_list,
        junit_summary,
        junit_error,
        total_upload,
    );

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report.to_json()).unwrap()
        );
    } else {
        println!("{}", report.text());
    }

    let code = report.exit_code();
    if code == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(code as u8)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn test_validate_paths_reject_parent_dir_after_missing_component() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let sym = tmp.path().join("sym_dir");
        std::os::unix::fs::symlink(&outside, &sym).unwrap();

        let bad_out = tmp.path().join("nonexistent/../sym_dir");
        let err_out = validate_output_dir_path(&bad_out).unwrap_err();
        assert!(err_out.contains(".."), "unexpected error: {err_out}");
        assert!(!tmp.path().join("nonexistent").exists());

        let bad_junit = tmp.path().join("nonexistent/../sym_dir/out.xml");
        let err_junit = validate_junit_out_path(&bad_junit).unwrap_err();
        assert!(err_junit.contains(".."), "unexpected error: {err_junit}");

        let nested_missing = tmp.path().join("a/b/c");
        validate_output_dir_path(&nested_missing).unwrap();

        let created = ensure_non_symlink_shard_output_dir(Some(&nested_missing), "shard-01")
            .expect("step-by-step creation succeeds");
        assert!(created.is_dir());
        assert_eq!(created, nested_missing.join("shard-01"));

        // Pre-existing directory works identically (AlreadyExists/existing path).
        let created_again =
            ensure_non_symlink_shard_output_dir(Some(&nested_missing), "shard-01").unwrap();
        assert_eq!(created_again, created);

        // Symlinked cell directory is refused.
        let bad_cell_base = tmp.path().join("cell_base");
        std::fs::create_dir_all(&bad_cell_base).unwrap();
        std::os::unix::fs::symlink(&outside, bad_cell_base.join("shard-01")).unwrap();
        let err_cell =
            ensure_non_symlink_shard_output_dir(Some(&bad_cell_base), "shard-01").unwrap_err();
        assert!(err_cell.contains("symbolic link"), "{err_cell}");
    }
}
