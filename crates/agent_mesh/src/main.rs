//! OxideSwarm Cross-Platform Coding Agent CLI (`agent-mesh`)
//!
//! Provides CLI subcommands to start the WebSocket Relay Hub, run a node client,
//! query active node inventory, and route commands/payloads across nodes.

use agent_mesh::{AgentGridBridge, AgentMeshClient, AgentMeshEnvelope, AgentMeshHub, HubConfig};
use clap::{Parser, Subcommand};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(
    name = "agent-mesh",
    about = "OxideSwarm Cross-Platform Coding Agent Communication Framework",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the OxideRelay WebSocket Hub server
    Hub {
        /// Address and port to bind the hub server to. A non-loopback address (e.g. 0.0.0.0:8088)
        /// needs --token, or --insecure to accept unauthenticated peers.
        #[arg(short, long, default_value = "127.0.0.1:8088")]
        listen: String,

        /// Shared token every node must present to register (visible in `ps`; prefer the
        /// OXIDE_MESH_TOKEN environment variable)
        #[arg(long, env = "OXIDE_MESH_TOKEN", hide_env_values = true)]
        token: Option<String>,

        /// Allow a non-loopback --listen address without a token. Anyone who can reach the port
        /// can then run commands on every node.
        #[arg(long)]
        insecure: bool,

        /// Address on which to run an embedded Master Grid runtime (e.g. 127.0.0.1:8089 or 0.0.0.0:8089).
        /// When set, the hub embeds MasterServer, attaches AgentGridBridge, and routes grid_* envelopes end-to-end.
        #[arg(long, env = "OXIDE_GRID_MASTER", num_args = 0..=1, default_missing_value = "127.0.0.1:0")]
        grid_master: Option<String>,

        /// Shared cluster secret for Master Grid authentication (hex string or path to secret file)
        #[arg(long, env = "OXIDESWARM_SECRET", hide_env_values = true)]
        secret: Option<String>,
    },

    /// Run an agent node client connected to the Hub
    #[command(alias = "agent")]
    Node {
        /// WebSocket URL of the OxideRelay Hub
        #[arg(long, default_value = "ws://127.0.0.1:8088/ws")]
        hub: String,

        /// Unique node identifier (defaults to node-<os>-<hostname>)
        #[arg(long)]
        id: Option<String>,

        /// Platform tag (e.g., windows, macos, ubuntu, android)
        #[arg(long)]
        platform: Option<String>,

        /// Hub token (visible in `ps`; prefer the OXIDE_MESH_TOKEN environment variable)
        #[arg(long, env = "OXIDE_MESH_TOKEN", hide_env_values = true)]
        token: Option<String>,
    },

    /// Send a structured command to a specific node
    Send {
        /// WebSocket URL of the OxideRelay Hub
        #[arg(long, default_value = "ws://127.0.0.1:8088/ws")]
        hub: String,

        /// Sender node identifier
        #[arg(long, default_value = "cli-sender")]
        from: String,

        /// Target node identifier
        #[arg(long)]
        to: String,

        /// Command name (e.g., echo, shell_exec, ping, system_info)
        #[arg(short, long, default_value = "echo")]
        command: String,

        /// JSON arguments string for the command
        #[arg(short, long, default_value = "{}")]
        args: String,

        /// Execution timeout in milliseconds
        #[arg(long, default_value = "15000")]
        timeout_ms: u64,

        /// Hub token (visible in `ps`; prefer the OXIDE_MESH_TOKEN environment variable)
        #[arg(long, env = "OXIDE_MESH_TOKEN", hide_env_values = true)]
        token: Option<String>,
    },

    /// Query the catalog of active nodes connected to the Hub
    List {
        /// WebSocket URL of the OxideRelay Hub
        #[arg(long, default_value = "ws://127.0.0.1:8088/ws")]
        hub: String,

        /// Sender node identifier
        #[arg(long, default_value = "cli-inspector")]
        from: String,

        /// Hub token (visible in `ps`; prefer the OXIDE_MESH_TOKEN environment variable)
        #[arg(long, env = "OXIDE_MESH_TOKEN", hide_env_values = true)]
        token: Option<String>,
    },

    /// Send a data payload to a target node with SHA-256 validation
    Data {
        /// WebSocket URL of the OxideRelay Hub
        #[arg(long, default_value = "ws://127.0.0.1:8088/ws")]
        hub: String,

        /// Sender node identifier
        #[arg(long, default_value = "cli-sender")]
        from: String,

        /// Target node identifier
        #[arg(long)]
        to: String,

        /// Raw data string to transmit
        #[arg(long)]
        payload: String,

        /// Hub token (visible in `ps`; prefer the OXIDE_MESH_TOKEN environment variable)
        #[arg(long, env = "OXIDE_MESH_TOKEN", hide_env_values = true)]
        token: Option<String>,
    },
}

/// An unset or empty token means "no token" for a client (an empty OXIDE_MESH_TOKEN is how a shell
/// unsets a variable it cannot remove).
fn client_token(token: Option<String>) -> Option<String> {
    token.filter(|t| !t.is_empty())
}

/// Builds a client for `hub_url`, presenting `token` if there is one.
fn build_client(
    hub_url: &str,
    node_id: &str,
    platform: &str,
    token: Option<String>,
) -> AgentMeshClient {
    let client = AgentMeshClient::new(hub_url, node_id, platform);
    match client_token(token) {
        Some(t) => client.with_token(t),
        None => client,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let cli = Cli::parse();

    match cli.command {
        Commands::Hub {
            listen,
            token,
            insecure,
            grid_master,
            secret,
        } => {
            if token.as_deref() == Some("") {
                return Err("--token / OXIDE_MESH_TOKEN is set but empty; an empty token would not protect the hub".into());
            }
            if token.as_deref().is_some_and(|t| t.len() < 16) {
                eprintln!("[HUB] WARNING: the hub token is shorter than 16 characters; use a long random one (e.g. `openssl rand -hex 24`).");
            }
            let authenticated = token.is_some();

            info!("Starting OxideRelay Hub on {}", listen);
            let mut config = HubConfig::default().with_insecure_bind(insecure);
            if let Some(t) = token {
                config = config.with_auth_token(t);
            }
            let hub = AgentMeshHub::with_config(config);

            let master_handle_opt = if let Some(ref master_spec) = grid_master {
                let spec = master_spec.trim();
                let bind_addr: SocketAddr =
                    if spec.is_empty() || spec == "auto" || spec == "default" {
                        "127.0.0.1:0".parse().unwrap()
                    } else if spec.contains(':') {
                        spec.parse().map_err(|e| {
                            format!("Invalid grid master bind address '{spec}': {e}")
                        })?
                    } else {
                        format!("127.0.0.1:{spec}")
                            .parse()
                            .map_err(|e| format!("Invalid grid master port '{spec}': {e}"))?
                    };

                let mut server_config = ServerConfig::new(bind_addr);
                let env_secret = std::env::var("OXIDESWARM_SECRET").ok();
                let secret_opt = secret.as_deref().or(env_secret.as_deref());
                if let Some(sec_str) = secret_opt.map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    let cs = ClusterSecret::from_hex(sec_str).or_else(|_| {
                        ClusterSecret::load_file(&std::path::PathBuf::from(sec_str))
                    })?;
                    server_config = server_config.with_cluster_secret(cs);
                }

                let master_handle = MasterServer::spawn(server_config).await?;
                let master_actual_addr = master_handle.server_addr();
                let bridge = Arc::new(AgentGridBridge::new(master_handle.clone()));
                hub.attach_bridge(bridge).await;
                println!(
                    "[HUB] Embedded Master Grid runtime active on {}",
                    master_actual_addr
                );
                Some(master_handle)
            } else {
                None
            };

            // Refuses a non-loopback address without a token unless --insecure was given.
            let port = hub.start(&listen).await?;
            let host = listen.rsplit_once(':').map_or(listen.as_str(), |(h, _)| h);
            println!("[HUB] OxideRelay Hub running at ws://{}:{}/ws", host, port);
            println!("[HUB] REST Status API: http://{}:{}/api/status", host, port);
            println!(
                "[HUB] REST Nodes Catalog: http://{}:{}/api/nodes",
                host, port
            );
            if authenticated {
                println!("[HUB] Token authentication: ENABLED (nodes need --token / OXIDE_MESH_TOKEN). The token is sent in clear text: use a TLS proxy (wss://) on untrusted networks.");
            } else if insecure {
                eprintln!("[HUB] WARNING: running WITHOUT authentication (--insecure). Anyone who can reach {} can run commands on every node.", listen);
            } else {
                println!("[HUB] Token authentication: disabled (loopback only)");
            }

            // Wait for Ctrl+C
            tokio::signal::ctrl_c().await?;
            println!("\n[HUB] Shutdown signal received. Stopping hub...");
            hub.stop().await;
            if let Some(handle) = master_handle_opt {
                let _ = handle.shutdown();
            }
        }

        Commands::Node {
            hub,
            id,
            platform,
            token,
        } => {
            let host_os = std::env::consts::OS.to_string();
            let hostname = sysinfo::System::host_name().unwrap_or_else(|| "device".to_string());
            let node_id = id.unwrap_or_else(|| format!("node-{}-{}", host_os, hostname));
            let plat = platform.unwrap_or(host_os);

            println!("========================================================");
            println!("   OxideSwarm Cross-Platform Agent Node Client          ");
            println!("========================================================");
            println!("Node ID:  {}", node_id);
            println!("Hub URL:  {}", hub);
            println!("Platform: {}", plat);
            println!();

            let client = build_client(&hub, &node_id, &plat, token);
            client.run_daemon().await;
        }

        Commands::Send {
            hub,
            from,
            to,
            command,
            args,
            timeout_ms,
            token,
        } => {
            let parsed_args: serde_json::Value = match serde_json::from_str(&args) {
                Ok(v) => v,
                Err(_) => serde_json::json!({ "cmd": args }),
            };

            let client = build_client(&hub, &from, std::env::consts::OS, token);
            client.connect().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;

            println!("[SEND] Routing command '{}' to '{}'...", command, to);
            match client
                .send_command(&to, &command, parsed_args, Some(timeout_ms))
                .await
            {
                Ok(resp) => match resp {
                    AgentMeshEnvelope::CommandResponse {
                        status,
                        exit_code,
                        stdout,
                        stderr,
                        execution_duration_ms,
                        ..
                    } => {
                        println!("Status:   {}", status);
                        println!("Exit Code: {}", exit_code);
                        println!("Duration: {} ms", execution_duration_ms);
                        if !stdout.is_empty() {
                            println!("\n--- STDOUT ---\n{}", stdout);
                        }
                        if !stderr.is_empty() {
                            println!("\n--- STDERR ---\n{}", stderr);
                        }
                    }
                    AgentMeshEnvelope::DeliveryNack {
                        error_code, reason, ..
                    } => {
                        error!(
                            "[NACK] Command delivery failed: {} - {}",
                            error_code, reason
                        );
                        std::process::exit(1);
                    }
                    other => {
                        println!("[RESPONSE] Received envelope: {:?}", other);
                    }
                },
                Err(e) => {
                    error!("[ERROR] Failed to execute command: {}", e);
                    std::process::exit(1);
                }
            }
            client.close().await;
        }

        Commands::List { hub, from, token } => {
            let client = build_client(&hub, &from, std::env::consts::OS, token);
            client.connect().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;

            let nodes = client.query_node_list(Some(5000)).await?;
            println!("Connected Nodes ({}):", nodes.len());
            for n in nodes {
                println!(
                    " - {} | Platform: {} | Host: {} | Status: {}",
                    n.node_id, n.platform, n.hostname, n.status
                );
            }
            client.close().await;
        }

        Commands::Data {
            hub,
            from,
            to,
            payload,
            token,
        } => {
            let client = build_client(&hub, &from, std::env::consts::OS, token);
            client.connect().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;

            println!("[DATA] Transmitting {} bytes to '{}'...", payload.len(), to);
            let (corr_id, hash) = client.send_data_payload(&to, payload.as_bytes()).await?;
            println!("[DATA] Transmission sent! Correlation ID: {}", corr_id);
            println!("[DATA] SHA-256 Checksum: {}", hash);

            // Wait brief moment for ACK
            tokio::time::sleep(Duration::from_millis(200)).await;
            client.close().await;
        }
    }

    Ok(())
}
