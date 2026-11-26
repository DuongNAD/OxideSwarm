//! Grid mailbox CLI commands (`oxideswarm mail send|fetch|ack`, alias `inbox`;
//! `TEAM-6`, `docs/design/0111-team-plan-leases-and-verify-gate.md` D8).

use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

use rusty_grid_core::blob::{
    hash_reader, open_regular_file_nofollow, validate_blob_path, BlobFile,
};
use rusty_grid_core::blob_transfer::{missing_blobs, upload_blob};
use rusty_grid_core::mail::{
    validate_mail_consumer, validate_mail_send, MailAttachment, MailMessage, MAX_MAIL_BODY_BYTES,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::WorkerCapabilities;

use crate::blobs::{download_outputs, BlobConnection};
use crate::cli_app::{exchange, resolve_relay_url};
use crate::config::{resolve_string, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS};
use crate::connect::{
    connect_master, report_master_failure, with_client_relay_url, DEFAULT_MASTER_ADDR,
};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

const RELAY_URL_HELP: &str = "Custom iroh relay URL (https://...) to use instead of n0's public relays (env RUSTY_GRID_RELAY_URL, config relay_url).";
const INSECURE_HELP: &str = "Continue WITHOUT a cluster credential although the master is remote, found by 'auto' discovery, or reached through a P2P ticket.";

#[derive(Parser, Debug, Clone)]
pub struct MailArgs {
    #[command(subcommand)]
    pub command: MailCommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum MailCommand {
    /// Send a durable message (with optional blob attachments) to recipients or topics
    Send(MailSendArgs),
    /// Fetch unread messages for a consumer and optionally save attachments
    #[command(visible_alias = "inbox")]
    Fetch(MailFetchArgs),
    /// Advance a consumer's durable acknowledgment cursor up to a message ID
    Ack(MailAckArgs),
}

#[derive(Args, Debug, Clone)]
pub struct MailSendArgs {
    #[arg(
        long,
        value_name = "ADDR|auto|TICKET",
        help = "Master address, 'auto', or P2P ticket"
    )]
    pub master: Option<String>,
    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,
    #[arg(
        long,
        required = true,
        value_name = "RECIPIENT",
        help = "Recipient (agent id, member/worker name, or topic:<name>; repeatable)"
    )]
    pub to: Vec<String>,
    #[arg(
        long,
        default_value = "",
        value_name = "TOPIC",
        help = "Short message topic label"
    )]
    pub topic: String,
    #[arg(
        long,
        default_value = "",
        value_name = "TEXT",
        help = "Inline UTF-8 message body (up to 64 KiB)"
    )]
    pub body: String,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read message body UTF-8 text from a regular file (conflicts with non-empty --body)"
    )]
    pub body_file: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Attach a local regular file as a content-addressed blob (repeatable)"
    )]
    pub file: Vec<PathBuf>,
    #[arg(long, help = "Print structured JSON to stdout")]
    pub json: bool,
    #[arg(long, help = INSECURE_HELP)]
    pub insecure: bool,
}

#[derive(Args, Debug, Clone)]
pub struct MailFetchArgs {
    #[arg(
        long,
        value_name = "ADDR|auto|TICKET",
        help = "Master address, 'auto', or P2P ticket"
    )]
    pub master: Option<String>,
    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,
    #[arg(
        long,
        required = true,
        value_name = "ID",
        help = "Consumer identity whose cursor filters already-acknowledged mail"
    )]
    pub consumer: String,
    #[arg(
        long,
        value_name = "TOPIC",
        help = "Additional subscribed topic or topic:<name> filter (repeatable)"
    )]
    pub topic: Vec<String>,
    #[arg(
        long,
        default_value_t = 0,
        value_name = "ID",
        help = "Return messages with ID strictly greater than max(after, ack cursor)"
    )]
    pub after: u64,
    #[arg(
        long,
        value_name = "DIR",
        help = "Download attachments into DIR without traversing symlinks or junctions"
    )]
    pub save: Option<PathBuf>,
    #[arg(long, help = "Print structured JSON to stdout")]
    pub json: bool,
    #[arg(long, help = INSECURE_HELP)]
    pub insecure: bool,
}

#[derive(Args, Debug, Clone)]
pub struct MailAckArgs {
    #[arg(
        long,
        value_name = "ADDR|auto|TICKET",
        help = "Master address, 'auto', or P2P ticket"
    )]
    pub master: Option<String>,
    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,
    #[arg(
        long,
        required = true,
        value_name = "ID",
        help = "Consumer identity whose acknowledgment cursor should advance"
    )]
    pub consumer: String,
    #[arg(
        long,
        required = true,
        value_name = "ID",
        help = "Highest message ID to acknowledge"
    )]
    pub id: u64,
    #[arg(long, help = "Print structured JSON to stdout")]
    pub json: bool,
    #[arg(long, help = INSECURE_HELP)]
    pub insecure: bool,
}

#[derive(Serialize)]
struct MailSendJson<'a> {
    id: u64,
    to: &'a [String],
    topic: &'a str,
    attachments: &'a [MailAttachment],
    recipient_links: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct MailFetchJson<'a> {
    cursor: u64,
    messages: &'a [MailMessage],
    saved_files: Vec<String>,
}

/// Top-level entrypoint for `oxideswarm mail`.
pub async fn run_mail(
    args: MailArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let (flag_relay, insecure_flag) = match &args.command {
        MailCommand::Send(a) => (a.relay_url.clone(), a.insecure),
        MailCommand::Fetch(a) => (a.relay_url.clone(), a.insecure),
        MailCommand::Ack(a) => (a.relay_url.clone(), a.insecure),
    };
    let relay_url = match resolve_relay_url(
        flag_relay,
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let _ = insecure_flag;
    with_client_relay_url(relay_url, async move {
        match args.command {
            MailCommand::Send(send) => run_mail_send(send, config_file, secret_opts).await,
            MailCommand::Fetch(fetch) => run_mail_fetch(fetch, config_file, secret_opts).await,
            MailCommand::Ack(ack) => run_mail_ack(ack, config_file, secret_opts).await,
        }
    })
    .await
}

fn is_symlink_or_reparse(meta: &std::fs::Metadata) -> bool {
    if meta.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        if (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
            return true;
        }
    }
    false
}

fn is_unix_system_alias_ancestor(prefix: &Path, full_dir: &Path) -> bool {
    #[cfg(unix)]
    {
        prefix != full_dir && matches!(prefix.to_str(), Some("/tmp" | "/var" | "/etc"))
    }
    #[cfg(not(unix))]
    {
        let _ = (prefix, full_dir);
        false
    }
}

/// Validates `--save <DIR>` without traversing symbolic links or Windows junctions, creating the
/// final directory if it does not yet exist.
pub fn validate_safe_save_dir(dir: &Path) -> Result<(), String> {
    let mut prefix = PathBuf::new();
    for component in dir.components() {
        prefix.push(component.as_os_str());
        if matches!(component, Component::Prefix(_) | Component::RootDir)
            || is_unix_system_alias_ancestor(&prefix, dir)
        {
            continue;
        }
        match std::fs::symlink_metadata(&prefix) {
            Ok(meta) => {
                if is_symlink_or_reparse(&meta) {
                    return Err(format!(
                        "refusing --save directory '{}': '{}' is a symbolic link or reparse point",
                        dir.display(),
                        prefix.display()
                    ));
                }
                if !meta.is_dir() {
                    return Err(format!(
                        "refusing --save directory '{}': '{}' exists and is not a directory",
                        dir.display(),
                        prefix.display()
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => {
                return Err(format!(
                    "cannot inspect --save directory '{}': {e}",
                    prefix.display()
                ));
            }
        }
    }
    std::fs::create_dir_all(dir)
        .map_err(|e| format!("cannot create --save directory '{}': {e}", dir.display()))?;
    let meta = std::fs::symlink_metadata(dir)
        .map_err(|e| format!("cannot inspect --save directory '{}': {e}", dir.display()))?;
    if is_symlink_or_reparse(&meta) {
        return Err(format!(
            "refusing --save directory '{}': target is a symbolic link or reparse point",
            dir.display()
        ));
    }
    if !meta.is_dir() {
        return Err(format!(
            "refusing --save directory '{}': not a directory",
            dir.display()
        ));
    }
    Ok(())
}

fn resolve_body(body: &str, body_file: Option<&Path>) -> Result<String, String> {
    match body_file {
        Some(path) => {
            if !body.is_empty() {
                return Err("cannot combine non-empty --body with --body-file".to_string());
            }
            let (mut file, meta) = open_regular_file_nofollow(path)
                .map_err(|e| format!("cannot open --body-file '{}': {e}", path.display()))?;
            if meta.len() as usize > MAX_MAIL_BODY_BYTES {
                return Err(format!(
                    "--body-file '{}' is {} bytes, above the {MAX_MAIL_BODY_BYTES} byte limit",
                    path.display(),
                    meta.len()
                ));
            }
            let mut text = String::new();
            use std::io::Read;
            file.read_to_string(&mut text)
                .map_err(|e| format!("cannot read --body-file '{}': {e}", path.display()))?;
            Ok(text)
        }
        None => Ok(body.to_string()),
    }
}

struct PreparedAttachment {
    attachment: MailAttachment,
    path: PathBuf,
}

fn prepare_attachments(files: &[PathBuf]) -> Result<Vec<PreparedAttachment>, String> {
    let mut prepared = Vec::with_capacity(files.len());
    let mut seen_names = HashSet::with_capacity(files.len());
    for path in files {
        let name_str = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("attachment path '{}' has no UTF-8 filename", path.display()))?;
        validate_blob_path(name_str)
            .map_err(|e| format!("invalid attachment filename '{}': {e}", path.display()))?;
        if !seen_names.insert(name_str.to_string()) {
            return Err(format!(
                "duplicate attachment filename '{name_str}'; pass unique filenames"
            ));
        }
        let (mut file, _meta) = open_regular_file_nofollow(path)
            .map_err(|e| format!("cannot open attachment '{}': {e}", path.display()))?;
        let (blob_id, size) = hash_reader(&mut file)
            .map_err(|e| format!("cannot hash attachment '{}': {e}", path.display()))?;
        prepared.push(PreparedAttachment {
            attachment: MailAttachment {
                name: name_str.to_string(),
                blob_id,
                size,
            },
            path: path.clone(),
        });
    }
    Ok(prepared)
}

fn worker_link_for_recipient(recipient: &str, workers: &[WorkerCapabilities]) -> String {
    let stripped = recipient.strip_prefix("worker:").unwrap_or(recipient);
    for w in workers {
        let id_tag = w.tags.iter().find_map(|t| t.strip_prefix("id:"));
        if w.name == stripped || id_tag == Some(stripped) {
            return w
                .tags
                .iter()
                .find_map(|t| t.strip_prefix("link:"))
                .unwrap_or("TCP/LAN")
                .to_string();
        }
    }
    if recipient.starts_with("topic:") {
        "topic (any subscriber)".to_string()
    } else {
        "offline/unmatched".to_string()
    }
}

async fn resolve_connection_params(
    master_flag: Option<String>,
    insecure_flag: bool,
    config_file: Option<&ConfigFile>,
    secret_opts: &SecretOpts,
) -> Result<(String, Option<rusty_grid_core::ClusterSecret>), ExitCode> {
    let status_cfg = config_file.and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        master_flag,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = client_secret(
        secret_opts,
        config_file,
        SecretSection::Status,
        &master_addr,
        peer_insecure(insecure_flag, status_cfg.as_ref().and_then(|s| s.insecure)),
    )?;
    Ok((master_addr, secret))
}

async fn run_mail_send(
    args: MailSendArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let body = match resolve_body(&args.body, args.body_file.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let prepared = match prepare_attachments(&args.file) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let attachments: Vec<MailAttachment> = prepared.iter().map(|p| p.attachment.clone()).collect();
    if let Err(e) = validate_mail_send(&args.to, &args.topic, &body, &attachments) {
        eprintln!("Error: {e}");
        return ExitCode::FAILURE;
    }

    let (master_addr, secret) = match resolve_connection_params(
        args.master,
        args.insecure,
        config_file.as_ref(),
        &secret_opts,
    )
    .await
    {
        Ok(v) => v,
        Err(code) => return code,
    };
    let had_secret = secret.is_some();

    if !prepared.is_empty() {
        let blob_conn = match BlobConnection::connect(&master_addr, secret.as_ref()).await {
            Ok(c) => c,
            Err(e) => {
                return report_master_failure(
                    &master_addr,
                    had_secret,
                    "Failed to connect to Master for attachment upload",
                    &e,
                );
            }
        };
        let ids: Vec<_> = prepared.iter().map(|p| p.attachment.blob_id).collect();
        let missing: HashSet<_> = match missing_blobs(&blob_conn, &ids).await {
            Ok(m) => m.into_iter().collect(),
            Err(e) => {
                blob_conn.close().await;
                eprintln!("Error querying missing attachment blobs: {e}");
                return ExitCode::FAILURE;
            }
        };
        for item in &prepared {
            if !missing.contains(&item.attachment.blob_id) {
                continue;
            }
            let (std_file, _meta) = match open_regular_file_nofollow(&item.path) {
                Ok(f) => f,
                Err(e) => {
                    blob_conn.close().await;
                    eprintln!("Error opening '{}': {e}", item.path.display());
                    return ExitCode::FAILURE;
                }
            };
            let async_file = tokio::fs::File::from_std(std_file);
            if let Err(e) = upload_blob(
                &blob_conn,
                item.attachment.blob_id,
                item.attachment.size,
                async_file,
            )
            .await
            {
                blob_conn.close().await;
                eprintln!("Error uploading '{}': {e}", item.path.display());
                return ExitCode::FAILURE;
            }
        }
        blob_conn.close().await;
    }

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                had_secret,
                "Failed to connect to Master",
                &e,
            );
        }
    };

    // Design 0111 D8.5: query recipients' link kinds when sending attachments.
    let mut recipient_links = BTreeMap::new();
    if !attachments.is_empty() {
        if let Ok(ClientResponse::WorkerList { workers }) = exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &ClientMessage::ListWorkers,
            "Failed to inspect recipient links",
        )
        .await
        {
            for r in &args.to {
                recipient_links.insert(r.clone(), worker_link_for_recipient(r, &workers));
            }
        }
    }

    let req = ClientMessage::MailSend {
        to: args.to.clone(),
        topic: args.topic.clone(),
        body,
        attachments: attachments.clone(),
    };
    let resp = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &req,
        "Failed to send mail",
    )
    .await
    {
        Ok(r) => r,
        Err(code) => {
            transport.close().await;
            return code;
        }
    };
    transport.close().await;

    match resp {
        ClientResponse::MailSent { id } => {
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string(&MailSendJson {
                        id,
                        to: &args.to,
                        topic: &args.topic,
                        attachments: &attachments,
                        recipient_links,
                    })
                    .unwrap()
                );
            } else {
                println!("Sent mail #{id} to {}", args.to.join(", "));
                if !attachments.is_empty() {
                    let total: u64 = attachments.iter().map(|a| a.size).sum();
                    println!(
                        "Attachments: {} file(s), {} bytes total",
                        attachments.len(),
                        total
                    );
                    for (r, link) in &recipient_links {
                        println!("  recipient {r}: link={link}");
                    }
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
}

async fn run_mail_fetch(
    args: MailFetchArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if let Err(e) = validate_mail_consumer(&args.consumer, &args.topic) {
        eprintln!("Error: {e}");
        return ExitCode::FAILURE;
    }
    if let Some(ref save_dir) = args.save {
        if let Err(e) = validate_safe_save_dir(save_dir) {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    }

    let (master_addr, secret) = match resolve_connection_params(
        args.master,
        args.insecure,
        config_file.as_ref(),
        &secret_opts,
    )
    .await
    {
        Ok(v) => v,
        Err(code) => return code,
    };
    let had_secret = secret.is_some();

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                had_secret,
                "Failed to connect to Master",
                &e,
            );
        }
    };
    let req = ClientMessage::MailFetch {
        consumer: args.consumer.clone(),
        topics: args.topic.clone(),
        after: args.after,
    };
    let resp = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &req,
        "Failed to fetch mail",
    )
    .await
    {
        Ok(r) => r,
        Err(code) => {
            transport.close().await;
            return code;
        }
    };
    transport.close().await;

    let (messages, cursor) = match resp {
        ClientResponse::MailMessages { messages, cursor } => (messages, cursor),
        ClientResponse::Error { message } => {
            eprintln!("Error: {message}");
            return ExitCode::FAILURE;
        }
        other => {
            eprintln!("Unexpected response: {other:?}");
            return ExitCode::FAILURE;
        }
    };

    let mut saved_files = Vec::new();
    if let Some(ref save_dir) = args.save {
        let mut used_paths = HashSet::new();
        let mut to_download = Vec::new();
        for m in &messages {
            for att in &m.attachments {
                let rel = if used_paths.insert(att.name.clone()) {
                    att.name.clone()
                } else {
                    let prefixed = format!("{}_{}", m.id, att.name);
                    used_paths.insert(prefixed.clone());
                    prefixed
                };
                to_download.push(BlobFile {
                    path: rel,
                    id: att.blob_id,
                    size: att.size,
                    executable: false,
                });
            }
        }
        if !to_download.is_empty() {
            let blob_conn = match BlobConnection::connect(&master_addr, secret.as_ref()).await {
                Ok(c) => c,
                Err(e) => {
                    return report_master_failure(
                        &master_addr,
                        had_secret,
                        "Failed to connect to Master for attachment download",
                        &e,
                    );
                }
            };
            let res = download_outputs(&blob_conn, &to_download, save_dir).await;
            blob_conn.close().await;
            match res {
                Ok(paths) => {
                    saved_files = paths.into_iter().map(|p| p.display().to_string()).collect();
                }
                Err(e) => {
                    eprintln!("Error downloading mail attachments: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string(&MailFetchJson {
                cursor,
                messages: &messages,
                saved_files,
            })
            .unwrap()
        );
    } else if messages.is_empty() {
        println!("No unread mail (cursor={cursor})");
    } else {
        for m in &messages {
            println!(
                "#{} from={} to={} topic={} attachments={}",
                m.id,
                m.from,
                m.to.join(","),
                m.topic,
                m.attachments.len()
            );
            if !m.body.is_empty() {
                println!(
                    "--- BEGIN MAIL DATA (untrusted) ---\n{}\n--- END MAIL DATA ---",
                    m.body
                );
            }
        }
        for path in saved_files {
            println!("Saved attachment: {path}");
        }
    }
    ExitCode::SUCCESS
}

async fn run_mail_ack(
    args: MailAckArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if let Err(e) = validate_mail_consumer(&args.consumer, &[]) {
        eprintln!("Error: {e}");
        return ExitCode::FAILURE;
    }
    let (master_addr, secret) = match resolve_connection_params(
        args.master,
        args.insecure,
        config_file.as_ref(),
        &secret_opts,
    )
    .await
    {
        Ok(v) => v,
        Err(code) => return code,
    };
    let had_secret = secret.is_some();

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                had_secret,
                "Failed to connect to Master",
                &e,
            );
        }
    };
    let req = ClientMessage::MailAck {
        consumer: args.consumer.clone(),
        up_to_id: args.id,
    };
    let resp = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &req,
        "Failed to acknowledge mail",
    )
    .await
    {
        Ok(r) => r,
        Err(code) => {
            transport.close().await;
            return code;
        }
    };
    transport.close().await;

    match resp {
        ClientResponse::MailAcked { cursor } => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({ "consumer": args.consumer, "cursor": cursor })
                );
            } else {
                println!("Acknowledged mail through #{cursor} for {}", args.consumer);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_app::Cli;

    #[test]
    fn cli_parses_mail_send_fetch_inbox_and_ack() {
        let send = Cli::try_parse_from([
            "oxideswarm",
            "mail",
            "send",
            "--to",
            "agent-win-2",
            "--topic",
            "done",
            "--body",
            "ok",
            "--json",
        ])
        .unwrap();
        assert!(matches!(
            send.command,
            crate::cli_app::Commands::Mail(MailArgs {
                command: MailCommand::Send(_)
            })
        ));

        let inbox = Cli::try_parse_from([
            "oxideswarm",
            "mail",
            "inbox",
            "--consumer",
            "agent-win-2",
            "--save",
            "./out",
        ])
        .unwrap();
        assert!(matches!(
            inbox.command,
            crate::cli_app::Commands::Mail(MailArgs {
                command: MailCommand::Fetch(_)
            })
        ));

        let ack = Cli::try_parse_from([
            "oxideswarm",
            "mail",
            "ack",
            "--consumer",
            "agent-win-2",
            "--id",
            "7",
        ])
        .unwrap();
        assert!(matches!(
            ack.command,
            crate::cli_app::Commands::Mail(MailArgs {
                command: MailCommand::Ack(_)
            })
        ));
    }

    #[test]
    fn resolve_body_rejects_oversized_file_and_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("big.txt");
        std::fs::write(&big, "x".repeat(MAX_MAIL_BODY_BYTES + 1)).unwrap();
        assert!(resolve_body("", Some(&big)).is_err());

        let small = dir.path().join("small.txt");
        std::fs::write(&small, "hi").unwrap();
        assert!(resolve_body("inline", Some(&small)).is_err());
        assert_eq!(resolve_body("", Some(&small)).unwrap(), "hi");
    }

    #[test]
    fn validate_safe_save_dir_refuses_regular_file_and_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not_a_dir");
        std::fs::write(&file, b"abc").unwrap();
        assert!(validate_safe_save_dir(&file).is_err());

        let real_dir = dir.path().join("real");
        std::fs::create_dir_all(&real_dir).unwrap();
        let link = dir.path().join("link");
        #[cfg(unix)]
        let linked = std::os::unix::fs::symlink(&real_dir, &link).is_ok();
        #[cfg(windows)]
        let linked = std::os::windows::fs::symlink_dir(&real_dir, &link).is_ok()
            || std::process::Command::new("cmd")
                .args([
                    "/C",
                    "mklink",
                    "/J",
                    link.to_str().unwrap(),
                    real_dir.to_str().unwrap(),
                ])
                .output()
                .is_ok_and(|o| o.status.success());
        if linked {
            assert!(validate_safe_save_dir(&link).is_err());
            assert!(validate_safe_save_dir(&link.join("nested")).is_err());
        }

        let nested_ok = dir.path().join("nested").join("save_out");
        assert!(validate_safe_save_dir(&nested_ok).is_ok());
        assert!(nested_ok.is_dir());
    }
}
