//! Binary entrypoint for `oxideswarm-tray` (`TRAY-1`, `TRAY-4`).

#![deny(unsafe_code)]

use clap::Parser;
use oxideswarm_tray::control::{default_log_dir, ControlClient};
use oxideswarm_tray::fullscreen::DEFAULT_RESUME_DELAY;
use oxideswarm_tray::platform::{run_app, TrayAppConfig};
use oxideswarm_tray::state::{derive_view, ControlPollState, SupervisorPhase};
use oxideswarm_tray::supervisor::SupervisorConfig;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "oxideswarm-tray",
    version,
    about = "OxideSwarm contributor tray app (starts and watches the background worker)"
)]
struct Cli {
    /// Path to the `oxideswarm` executable (defaults to sibling `oxideswarm` binary).
    #[arg(long, value_name = "PATH")]
    worker_bin: Option<PathBuf>,

    /// Attach to an already-running worker instead of starting and supervising one.
    #[arg(long)]
    no_spawn: bool,

    /// Worker instance name (`--name`), shared between the tray control poll and worker child.
    #[arg(long, value_name = "NAME")]
    name: Option<String>,

    /// Explicit worker control pipe name or Unix domain socket path.
    #[arg(long, value_name = "PATH_OR_PIPE")]
    control_socket: Option<PathBuf>,

    /// Directory opened by "Open logs" and used for supervised worker logs.
    #[arg(long, value_name = "DIR")]
    log_dir: Option<PathBuf>,

    /// Worker status poll interval in milliseconds (default: 2000 ms per design 0112).
    #[arg(long, default_value_t = 2000)]
    poll_interval_ms: u64,

    /// Disable automatic pause while a full-screen application runs (TRAY-4).
    #[arg(long)]
    no_fullscreen_pause: bool,

    /// Seconds to wait after a full-screen program closes before resuming (default: 60 s).
    #[arg(long, default_value_t = DEFAULT_RESUME_DELAY.as_secs())]
    fullscreen_resume_delay_secs: u64,

    /// Query the running worker once and print status (useful on headless Linux), then exit.
    #[arg(long)]
    status: bool,

    /// Print `--status` output as JSON.
    #[arg(long)]
    json: bool,

    /// Extra arguments forwarded verbatim to `oxideswarm worker ...`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    worker_args: Vec<String>,
}

fn resolve_default_worker_bin() -> PathBuf {
    let exe_name = if cfg!(windows) {
        "oxideswarm.exe"
    } else {
        "oxideswarm"
    };
    if let Ok(self_exe) = std::env::current_exe() {
        if let Some(dir) = self_exe.parent() {
            let sibling = dir.join(exe_name);
            if sibling.is_file() {
                return sibling;
            }
        }
    }
    PathBuf::from(exe_name)
}

fn forward_control_flags(
    args: &mut Vec<String>,
    name: Option<&str>,
    control_socket: Option<&Path>,
) {
    if let Some(worker_name) = name {
        let has_name = args
            .iter()
            .any(|a| a == "--name" || a.starts_with("--name="));
        if !has_name {
            args.push("--name".to_string());
            args.push(worker_name.to_string());
        }
    }
    if let Some(sock) = control_socket {
        let has_sock = args
            .iter()
            .any(|a| a == "--control-socket" || a.starts_with("--control-socket="));
        if !has_sock {
            args.push("--control-socket".to_string());
            args.push(sock.to_string_lossy().into_owned());
        }
    }
}

fn run_status_once(client: Option<&ControlClient>, json_out: bool) -> ExitCode {
    let Some(client) = client else {
        eprintln!("could not resolve local worker control endpoint");
        return ExitCode::from(1);
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to initialize async runtime: {err}");
            return ExitCode::from(1);
        }
    };
    match rt.block_on(client.poll_status()) {
        Ok(snap) => {
            if json_out {
                match serde_json::to_string_pretty(&snap) {
                    Ok(body) => println!("{body}"),
                    Err(err) => {
                        eprintln!("failed to serialize status JSON: {err}");
                        return ExitCode::from(1);
                    }
                }
            } else {
                let view = derive_view(
                    &ControlPollState::Connected(Box::new(snap)),
                    &SupervisorPhase::External,
                    false,
                    false,
                );
                println!("{}", view.status_line);
                println!("Endpoint: {}", client.display_target());
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            if json_out {
                let err_json = serde_json::json!({
                    "error": err,
                    "endpoint": client.display_target(),
                });
                println!("{err_json}");
            } else {
                eprintln!(
                    "worker control unavailable at {}: {err}",
                    client.display_target()
                );
            }
            ExitCode::from(1)
        }
    }
}

fn main() -> ExitCode {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();

    let cli = Cli::parse();
    let client = ControlClient::resolve(cli.control_socket.as_deref(), cli.name.as_deref());

    if cli.status {
        return run_status_once(client.as_ref(), cli.json);
    }

    let log_dir = cli.log_dir.unwrap_or_else(default_log_dir);
    let supervisor = if cli.no_spawn {
        None
    } else {
        let bin = cli.worker_bin.unwrap_or_else(resolve_default_worker_bin);
        let mut args = cli.worker_args;
        forward_control_flags(
            &mut args,
            cli.name.as_deref(),
            cli.control_socket.as_deref(),
        );
        let mut cfg = SupervisorConfig::new(bin, args);
        cfg.log_file = Some(log_dir.join("worker.log"));
        Some(cfg)
    };

    let app_cfg = TrayAppConfig {
        supervisor,
        control_endpoint: client,
        log_dir,
        poll_interval: Duration::from_millis(cli.poll_interval_ms.max(200)),
        fullscreen_guard_enabled: !cli.no_fullscreen_pause,
        fullscreen_resume_delay: Duration::from_secs(cli.fullscreen_resume_delay_secs),
    };

    match run_app(app_cfg) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("oxideswarm-tray failed: {err}");
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_control_flags_appends_only_when_missing() {
        let mut args = vec!["--isolation".to_string(), "fs".to_string()];
        forward_control_flags(&mut args, Some("rig-1"), Some(Path::new("pipe-a")));
        assert_eq!(
            args,
            vec![
                "--isolation",
                "fs",
                "--name",
                "rig-1",
                "--control-socket",
                "pipe-a"
            ]
        );

        let mut existing = vec!["--name=already".to_string()];
        forward_control_flags(&mut existing, Some("rig-1"), None);
        assert_eq!(existing, vec!["--name=already"]);
    }
}
