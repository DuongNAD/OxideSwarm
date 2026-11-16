//! `oxideswarm workspaces` command implementation.

use std::process::ExitCode;

use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

use crate::config::{resolve_string, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS};
use crate::connect::{connect_master, report_master_failure, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

use super::{exchange, resolve_relay_url, WorkspacesArgs};

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
