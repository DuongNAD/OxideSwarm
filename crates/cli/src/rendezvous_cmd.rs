//! Standalone HTTP Rendezvous Registry CLI (`oxideswarm rendezvous`, `FEAT-7`).

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args;
use rusty_grid_master::{
    remove_port_file, spawn_rendezvous_server, write_port_file, KeyedRendezvousConfig,
    DEFAULT_MAX_LEASES, DEFAULT_MAX_TTL_SECS,
};

use crate::config::SecretSection;
use crate::secret_cmd::SecretOpts;

#[derive(Args, Debug, Clone)]
pub struct RendezvousArgs {
    /// Socket address for the standalone HTTP rendezvous registry server
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:8090",
        env = "OXIDESWARM_RENDEZVOUS_LISTEN",
        help = "Socket address for the standalone HTTP rendezvous registry server"
    )]
    pub listen: String,

    /// File path where bound port number is written
    #[arg(
        long,
        help = "File path where bound port number is written (useful for ephemeral port 0)"
    )]
    pub port_file: Option<PathBuf>,

    /// Maximum number of distinct cluster leases kept concurrently in memory
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_LEASES,
        help = "Maximum number of concurrent cluster leases kept in memory"
    )]
    pub max_leases: usize,

    /// Maximum TTL (in seconds) accepted on registration
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_TTL_SECS,
        help = "Maximum lease TTL (in seconds) accepted on registration"
    )]
    pub max_ttl_secs: u64,

    /// Allow unsigned leases in the default slot (unsafe on non-loopback without --insecure)
    #[arg(
        long,
        help = "Allow unsigned leases in the default slot (unsafe on non-loopback without --insecure)"
    )]
    pub allow_unsigned: bool,

    /// Acknowledge allowing unsigned leases on a non-loopback bind address
    #[arg(
        long,
        help = "Allow --allow-unsigned on a non-loopback --listen address"
    )]
    pub insecure: bool,
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

/// Runs `oxideswarm rendezvous` (`FEAT-7`).
pub async fn run_rendezvous(args: RendezvousArgs, secret_opts: SecretOpts) -> ExitCode {
    if args.max_leases == 0 {
        eprintln!("--max-leases must be >= 1");
        return ExitCode::FAILURE;
    }
    if args.max_ttl_secs == 0 {
        eprintln!("--max-ttl-secs must be >= 1");
        return ExitCode::FAILURE;
    }

    let listen_addr: SocketAddr = match args.listen.trim().parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Invalid --listen address '{}': {e}", args.listen);
            return ExitCode::FAILURE;
        }
    };

    if args.allow_unsigned && !listen_addr.ip().is_loopback() && !args.insecure {
        eprintln!(
            "Refusing to run `oxideswarm rendezvous --allow-unsigned` on non-loopback address {listen_addr} without --insecure. Unsigned leases let any reachable host overwrite the default discovery slot."
        );
        return ExitCode::FAILURE;
    }

    // Only pin a ClusterSecret when explicitly requested via `--secret` or `--secret-file`.
    // Never auto-load `~/.oxideswarm/cluster.secret` here so a standalone registry can serve
    // multiple independent clusters via self-authenticating Ed25519 `key_signature` leases.
    let cluster_secret = if secret_opts.value.is_some() || secret_opts.file.is_some() {
        match secret_opts.resolve_with(None, SecretSection::Master, |_| None, None) {
            Ok(secret) => secret,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    let config = KeyedRendezvousConfig {
        max_leases: args.max_leases,
        max_ttl_secs: args.max_ttl_secs,
        allow_unsigned: args.allow_unsigned,
        cluster_secret,
    };

    let handle = match spawn_rendezvous_server(listen_addr, config).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("Failed to start rendezvous server on {listen_addr}: {e}");
            return ExitCode::FAILURE;
        }
    };

    let local_addr = handle.local_addr();
    if let Some(ref port_path) = args.port_file {
        if let Err(e) = write_port_file(port_path, local_addr.port()).await {
            eprintln!(
                "Failed to write rendezvous port file {}: {e}",
                port_path.display()
            );
            handle.shutdown().await;
            return ExitCode::FAILURE;
        }
    }

    println!("OxideSwarm Rendezvous registry listening on http://{local_addr}");
    let _ = std::io::stdout().flush();

    wait_for_shutdown_signal().await;

    if let Some(ref port_path) = args.port_file {
        remove_port_file(port_path).await;
    }
    handle.shutdown().await;
    ExitCode::SUCCESS
}
