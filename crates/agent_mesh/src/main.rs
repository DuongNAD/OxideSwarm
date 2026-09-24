//! OxideSwarm Cross-Platform Coding Agent CLI (`agent-mesh`)
//!
//! Provides CLI subcommands to start the WebSocket Relay Hub, run a node client,
//! query active node inventory, and route commands/payloads across nodes.

use agent_mesh::{AgentMeshClient, AgentMeshEnvelope, AgentMeshHub};
use clap::{Parser, Subcommand};
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
        /// Address and port to bind the hub server to
        #[arg(short, long, default_value = "0.0.0.0:8088")]
        listen: String,
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
    },

    /// Query the catalog of active nodes connected to the Hub
    List {
        /// WebSocket URL of the OxideRelay Hub
        #[arg(long, default_value = "ws://127.0.0.1:8088/ws")]
        hub: String,

        /// Sender node identifier
        #[arg(long, default_value = "cli-inspector")]
        from: String,
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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let cli = Cli::parse();

    match cli.command {
        Commands::Hub { listen } => {
            info!("Starting OxideRelay Hub on {}", listen);
            let hub = AgentMeshHub::new();
            let port = hub.start(&listen).await?;
            println!("[HUB] OxideRelay Hub running at ws://{}:{}/ws", listen.split(':').next().unwrap_or("0.0.0.0"), port);
            println!("[HUB] REST Status API: http://{}:{}/api/status", listen.split(':').next().unwrap_or("0.0.0.0"), port);
            println!("[HUB] REST Nodes Catalog: http://{}:{}/api/nodes", listen.split(':').next().unwrap_or("0.0.0.0"), port);

            // Wait for Ctrl+C
            tokio::signal::ctrl_c().await?;
            println!("\n[HUB] Shutdown signal received. Stopping hub...");
            hub.stop().await;
        }

        Commands::Node { hub, id, platform } => {
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

            let client = AgentMeshClient::new(&hub, &node_id, &plat);
            client.run_daemon().await;
        }

        Commands::Send {
            hub,
            from,
            to,
            command,
            args,
            timeout_ms,
        } => {
            let parsed_args: serde_json::Value = match serde_json::from_str(&args) {
                Ok(v) => v,
                Err(_) => serde_json::json!({ "cmd": args }),
            };

            let client = AgentMeshClient::new(&hub, &from, std::env::consts::OS);
            client.connect().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;

            println!("[SEND] Routing command '{}' to '{}'...", command, to);
            match client.send_command(&to, &command, parsed_args, Some(timeout_ms)).await {
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
                    AgentMeshEnvelope::DeliveryNack { error_code, reason, .. } => {
                        error!("[NACK] Command delivery failed: {} - {}", error_code, reason);
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

        Commands::List { hub, from } => {
            let client = AgentMeshClient::new(&hub, &from, std::env::consts::OS);
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
        } => {
            let client = AgentMeshClient::new(&hub, &from, std::env::consts::OS);
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
