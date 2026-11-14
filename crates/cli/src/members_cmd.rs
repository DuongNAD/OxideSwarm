//! CLI commands and invite token encoding for OXS-AUTH v3 machine enrollment (`ENROLL-1b`,
//! design record `docs/design/0051-machine-identity-and-invites.md`).
//!
//! - `oxideswarm invite`: creates, lists, or revokes time-bounded, use-bounded enrollment invites
//!   (`oxs1-invite:<base64url payload>.<check>`).
//! - `oxideswarm members`: lists enrolled members, views pending enrollment requests (with their
//!   6-digit pairing check and shared-invite contention count), approves or rejects pending
//!   requests, revokes members, and renames members.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{
    invite_payload_check_hex, InviteSummary, MemberEntry, MemberRole, PendingEnrollmentInfo,
    PublicKey, RedactedBytes32, RedactedString,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

use crate::config::{resolve_string, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS};
use crate::connect::{
    connect_master, is_p2p_ticket, normalize_master_addr, report_master_failure,
    with_client_relay_url, DEFAULT_MASTER_ADDR,
};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

/// Every invite token starts with this prefix.
pub const INVITE_PREFIX: &str = "oxs1-invite:";

/// Bytes of SHA-256 kept in the `<check>` suffix (8 bytes = 16 hex characters).
const CHECK_BYTES: usize = 8;

/// Longest address or ticket field allowed inside an invite payload.
const MAX_FIELD_LEN: usize = 2048;

/// Help text of `--insecure` on client commands.
const INSECURE_PEER_HELP: &str = "Continue WITHOUT a cluster credential although the master is remote, found by 'auto' discovery, or reached through a P2P ticket.";

/// Help text of `--relay-url`.
const RELAY_URL_HELP: &str = "Custom iroh relay URL (https://...) to use instead of n0's public relays (env RUSTY_GRID_RELAY_URL, config relay_url). Cannot be combined with RUSTY_GRID_P2P_MINIMAL.";

/// Decoded contents of an `oxs1-invite:<base64url payload>.<check>` invite string.
#[derive(Clone, PartialEq, Eq)]
pub struct InviteToken {
    /// Address of the master's grid port (`host:port`), if the invite carries one.
    pub master: Option<String>,
    /// P2P connection ticket of the master, if the invite carries one.
    pub ticket: Option<String>,
    /// Master's static OXS-AUTH v3 X25519 public key.
    pub master_key: PublicKey,
    /// 16-byte random invite identifier (32 lowercase hex chars).
    pub invite_id: String,
    /// 32-byte random invite secret used to compute the `ENROLL` proof.
    pub invite_secret: RedactedBytes32,
    /// Unix timestamp (seconds) when the invite expires.
    pub expires_at: u64,
    /// Roles granted to a machine that redeems this invite.
    pub roles: Vec<MemberRole>,
    /// Whether an admin must approve the enrollment request before it completes.
    pub require_approval: bool,
}

impl std::fmt::Debug for InviteToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InviteToken")
            .field("master", &self.master)
            .field("ticket", &self.ticket.as_ref().map(|_| "<present>"))
            .field("master_key", &self.master_key)
            .field("invite_id", &self.invite_id)
            .field("invite_secret", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field("roles", &self.roles)
            .field("require_approval", &self.require_approval)
            .finish()
    }
}

fn default_true() -> bool {
    true
}

#[derive(Serialize, Deserialize)]
struct InvitePayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    m: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    t: Option<String>,
    mk: String,
    id: String,
    s: String,
    exp: u64,
    r: Vec<MemberRole>,
    #[serde(default = "default_true")]
    ap: bool,
}

/// Why an invite string was refused. Never contains the invite text or secret.
#[derive(Debug, PartialEq, Eq)]
pub enum InviteTokenError {
    NotAnInvite,
    Corrupt,
    Invalid(&'static str),
}

impl std::fmt::Display for InviteTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnInvite => {
                write!(f, "this is not an invite (it must start with {INVITE_PREFIX})")
            }
            Self::Corrupt => f.write_str(
                "the invite is damaged: its checksum failed (a character was changed, lost or added while copying). Ask the cluster admin for a fresh `oxideswarm invite` and copy it whole",
            ),
            Self::Invalid(why) => write!(f, "the invite is invalid: {why}"),
        }
    }
}

pub(crate) fn field_ok(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_FIELD_LEN && !value.chars().any(char::is_whitespace)
}

pub(crate) fn sanitize_terminal_text(raw: &str) -> String {
    raw.chars()
        .filter(|c| !rusty_grid_core::noise_auth::is_control_or_format_char(*c))
        .collect()
}

impl InviteToken {
    /// Encodes the invite as `oxs1-invite:<base64url payload>.<check>`.
    pub fn encode(&self) -> String {
        let payload = InvitePayload {
            m: self.master.clone(),
            t: self.ticket.clone(),
            mk: self.master_key.to_hex(),
            id: self.invite_id.clone(),
            s: self.invite_secret.to_hex(),
            exp: self.expires_at,
            r: self.roles.clone(),
            ap: self.require_approval,
        };
        let json = serde_json::to_vec(&payload).expect("InvitePayload serializes to JSON");
        let text = URL_SAFE_NO_PAD.encode(json);
        let check = invite_payload_check_hex(&text);
        format!("{INVITE_PREFIX}{text}.{check}")
    }

    /// Parses and verifies the `<check>` suffix of an invite string.
    pub fn decode(token: &str) -> Result<Self, InviteTokenError> {
        let body = token
            .trim()
            .strip_prefix(INVITE_PREFIX)
            .ok_or(InviteTokenError::NotAnInvite)?;
        let (text, check) = body.rsplit_once('.').ok_or(InviteTokenError::Corrupt)?;
        if text.is_empty() || check.len() != CHECK_BYTES * 2 {
            return Err(InviteTokenError::Corrupt);
        }
        let expected = invite_payload_check_hex(text);
        let same = expected.len() == check.len()
            && expected
                .bytes()
                .zip(check.bytes())
                .fold(0u8, |acc, (a, b)| {
                    acc | (a.to_ascii_lowercase() ^ b.to_ascii_lowercase())
                })
                == 0;
        if !same {
            return Err(InviteTokenError::Corrupt);
        }
        let json = URL_SAFE_NO_PAD
            .decode(text)
            .map_err(|_| InviteTokenError::Corrupt)?;
        let payload: InvitePayload =
            serde_json::from_slice(&json).map_err(|_| InviteTokenError::Corrupt)?;

        let master_key = PublicKey::from_hex(&payload.mk)
            .map_err(|_| InviteTokenError::Invalid("master key is malformed"))?;
        let invite_secret = RedactedBytes32::from_hex(&payload.s)
            .ok_or(InviteTokenError::Invalid("invite secret is malformed"))?;
        if payload.id.len() != 32 || !payload.id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(InviteTokenError::Invalid("invite id is malformed"));
        }
        if payload.r.is_empty() {
            return Err(InviteTokenError::Invalid("invite grants no roles"));
        }
        if payload.m.is_none() && payload.t.is_none() {
            return Err(InviteTokenError::Invalid(
                "it has neither a master address nor a P2P ticket",
            ));
        }
        if payload.m.as_deref().is_some_and(|m| !field_ok(m)) {
            return Err(InviteTokenError::Invalid("the master address is malformed"));
        }
        if payload.t.as_deref().is_some_and(|t| !field_ok(t)) {
            return Err(InviteTokenError::Invalid("the P2P ticket is malformed"));
        }

        Ok(Self {
            master: payload.m,
            ticket: payload.t,
            master_key,
            invite_id: payload.id.to_ascii_lowercase(),
            invite_secret,
            expires_at: payload.exp,
            roles: payload.r,
            require_approval: payload.ap,
        })
    }

    /// Returns `true` if `now_secs >= self.expires_at`.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        now_secs >= self.expires_at
    }

    /// Returns `true` if the invite is expired according to the system clock.
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.is_expired_at(now)
    }
}

/// Parses a human duration string (`"24h"`, `"30m"`, `"3600s"`, `"7d"`, or bare seconds `"3600"`).
pub fn parse_duration_secs(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("duration cannot be empty".into());
    }
    let (num_str, multiplier) = if let Some(n) = trimmed
        .strip_suffix('s')
        .or_else(|| trimmed.strip_suffix('S'))
    {
        (n.trim(), 1u64)
    } else if let Some(n) = trimmed
        .strip_suffix('m')
        .or_else(|| trimmed.strip_suffix('M'))
    {
        (n.trim(), 60u64)
    } else if let Some(n) = trimmed
        .strip_suffix('h')
        .or_else(|| trimmed.strip_suffix('H'))
    {
        (n.trim(), 3600u64)
    } else if let Some(n) = trimmed
        .strip_suffix('d')
        .or_else(|| trimmed.strip_suffix('D'))
    {
        (n.trim(), 86_400u64)
    } else {
        (trimmed, 1u64)
    };
    let value: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration '{raw}' (expected e.g. 24h, 30m, 3600s, or 7d)"))?;
    let secs = value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("duration '{raw}' overflows u64 seconds"))?;
    if secs == 0 {
        return Err("duration must be at least 1s".into());
    }
    Ok(secs)
}

/// Parses a comma-separated list of [`MemberRole`] values (e.g. `"worker,client"`).
pub fn parse_roles_csv(raw: &str) -> Result<Vec<MemberRole>, String> {
    let mut roles = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let role: MemberRole = trimmed.parse()?;
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    if roles.is_empty() {
        return Err("at least one role must be specified (worker, client, or admin)".into());
    }
    Ok(roles)
}

pub fn format_roles(roles: &[MemberRole]) -> String {
    roles
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

// ---------------------------------------------------------------------------------------------
// `oxideswarm invite`
// ---------------------------------------------------------------------------------------------

/// Create, list, or revoke enrollment invites (`oxs1-invite:...`).
#[derive(Parser, Debug, Clone)]
pub struct InviteArgs {
    #[arg(
        long,
        value_name = "ROLES",
        default_value = "worker",
        help = "Comma-separated roles granted by this invite: worker, client, admin (default: worker)"
    )]
    pub roles: String,

    #[arg(
        long,
        value_name = "N",
        default_value = "1",
        help = "Number of enrollments this invite permits before it is exhausted (default: 1; admin invites always allow 1)"
    )]
    pub uses: u32,

    #[arg(
        long,
        value_name = "DURATION",
        default_value = "24h",
        help = "How long until the invite expires (e.g. 24h, 30m, 3600s, 7d; default: 24h)"
    )]
    pub expires: String,

    #[arg(
        long,
        help = "Enrol machines immediately without waiting for `oxideswarm members approve` (ignored for admin invites)"
    )]
    pub no_approval: bool,

    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address other machines use to reach this master's grid port (also used to connect to the master)"
    )]
    pub master: Option<String>,

    #[arg(
        long,
        value_name = "TICKET",
        conflicts_with = "p2p_ticket_file",
        help = "Also embed this P2P connection ticket in the invite"
    )]
    pub p2p_ticket: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Also embed the P2P ticket stored in this file (default: the master's active P2P ticket, if any)"
    )]
    pub p2p_ticket_file: Option<PathBuf>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(
        long,
        conflicts_with = "revoke",
        help = "List active and past invites stored on the master instead of creating one"
    )]
    pub list: bool,

    #[arg(
        long,
        value_name = "ID",
        help = "Revoke an existing invite by its ID (or unique ID prefix)"
    )]
    pub revoke: Option<String>,

    #[arg(
        long,
        conflicts_with_all = ["list", "revoke", "no_approval", "uses", "json"],
        help = "Pair interactively with a new machine on the LAN using a short one-time code instead of printing an invite string"
    )]
    pub code: bool,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

fn cfg_client_master(config_file: Option<&ConfigFile>) -> Option<String> {
    config_file.and_then(|c| {
        c.submit
            .as_ref()
            .and_then(|s| s.master.clone())
            .or_else(|| c.status.as_ref().and_then(|s| s.master.clone()))
            .or_else(|| c.worker.as_ref().and_then(|w| w.master.clone()))
    })
}

pub(crate) fn cfg_client_insecure(config_file: Option<&ConfigFile>) -> Option<bool> {
    config_file.and_then(|c| {
        c.submit
            .as_ref()
            .and_then(|s| s.insecure)
            .or_else(|| c.status.as_ref().and_then(|s| s.insecure))
    })
}

pub(crate) fn resolve_admin_connect_addr(
    cli_master: Option<String>,
    config_file: Option<&ConfigFile>,
) -> String {
    let cfg_master = cfg_client_master(config_file).or_else(|| {
        config_file
            .and_then(|c| c.master.as_ref())
            .and_then(|m| m.listen.as_deref())
            .and_then(|l| l.parse::<std::net::SocketAddr>().ok())
            .filter(|a| !a.ip().is_unspecified() && a.port() != 0)
            .map(|a| a.to_string())
    });
    resolve_string(
        cli_master,
        CLIENT_MASTER_ENV_VARS,
        cfg_master,
        DEFAULT_MASTER_ADDR,
    )
}

/// Resolves and validates the local `--master`, `--p2p-ticket`, and `--p2p-ticket-file` inputs
/// before connecting to the master so invalid files or malformed addresses never create an
/// orphaned invite on the master.
pub(crate) fn resolve_local_invite_targets(
    args: &InviteArgs,
    config_file: Option<&ConfigFile>,
) -> GridResult<(Option<String>, Option<String>)> {
    let master_cfg = config_file.and_then(|c| c.master.as_ref());

    let explicit_ticket = match (&args.p2p_ticket, &args.p2p_ticket_file) {
        (Some(t), _) => {
            let trimmed = t.trim().to_string();
            if !field_ok(&trimmed) {
                return Err(GridError::Config(
                    "--p2p-ticket is empty or malformed".into(),
                ));
            }
            Some(trimmed)
        }
        (None, Some(file)) => {
            let content = std::fs::read_to_string(file).map_err(|e| {
                GridError::Config(format!(
                    "cannot read P2P ticket file '{}': {e}",
                    file.display()
                ))
            })?;
            let trimmed = content.trim().to_string();
            if !field_ok(&trimmed) {
                return Err(GridError::Config(format!(
                    "P2P ticket file '{}' is empty or malformed",
                    file.display()
                )));
            }
            Some(trimmed)
        }
        (None, None) => master_cfg
            .and_then(|m| m.p2p_ticket_file.as_deref())
            .and_then(|f| std::fs::read_to_string(f).ok())
            .map(|t| t.trim().to_string())
            .filter(|t| field_ok(t)),
    };

    let master_from_arg = args
        .master
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let (master, ticket) = match master_from_arg {
        Some(m) if is_p2p_ticket(m) => (None, explicit_ticket.or_else(|| Some(m.to_string()))),
        Some(m) => {
            if m.eq_ignore_ascii_case("auto") || m.to_ascii_lowercase().starts_with("auto:") {
                return Err(GridError::Config(
                    "--master in `oxideswarm invite` must be a concrete HOST:PORT or P2P ticket, not 'auto'".into(),
                ));
            }
            let norm = normalize_master_addr(m);
            if !field_ok(&norm) {
                return Err(GridError::Config("--master address is malformed".into()));
            }
            (Some(norm), explicit_ticket)
        }
        None => {
            let env_master = CLIENT_MASTER_ENV_VARS.iter().find_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
            });
            if let Some(em) = env_master {
                if is_p2p_ticket(&em) {
                    (None, explicit_ticket.or(Some(em)))
                } else if em.eq_ignore_ascii_case("auto")
                    || em.to_ascii_lowercase().starts_with("auto:")
                {
                    (None, explicit_ticket)
                } else {
                    (Some(normalize_master_addr(&em)), explicit_ticket)
                }
            } else {
                let cfg_listen = master_cfg
                    .and_then(|m| m.listen.as_deref())
                    .and_then(|l| l.parse::<std::net::SocketAddr>().ok())
                    .filter(|a| !a.ip().is_unspecified() && a.port() != 0)
                    .map(|a| a.to_string())
                    .or_else(|| {
                        cfg_client_master(config_file)
                            .filter(|m: &String| {
                                !m.eq_ignore_ascii_case("auto") && !is_p2p_ticket(m)
                            })
                            .map(|m: String| normalize_master_addr(&m))
                    });
                (cfg_listen, explicit_ticket)
            }
        }
    };

    if let Some(ref m) = master {
        if !field_ok(m) {
            return Err(GridError::Config("master address is malformed".into()));
        }
    }
    if let Some(ref t) = ticket {
        if !field_ok(t) {
            return Err(GridError::Config("P2P ticket is malformed".into()));
        }
    }
    Ok((master, ticket))
}

pub(crate) fn finalize_invite_targets(
    local_master: Option<String>,
    local_ticket: Option<String>,
    master_reported_ticket: Option<String>,
) -> GridResult<(Option<String>, Option<String>)> {
    let ticket = local_ticket.or_else(|| {
        master_reported_ticket
            .map(|t| t.trim().to_string())
            .filter(|t| field_ok(t))
    });
    if local_master.is_none() && ticket.is_none() {
        return Err(GridError::Config(
            "no address to put in the invite: pass --master HOST:PORT (the address other machines use to reach this master, for example 192.168.1.5:8088), or --p2p-ticket-file".into(),
        ));
    }
    Ok((local_master, ticket))
}

/// Runs `oxideswarm invite`.
pub async fn run_invite(
    args: InviteArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let p2p_minimal = crate::cli_app::resolve_p2p_minimal();
    let relay_url = match crate::cli_app::resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
        p2p_minimal,
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    with_client_relay_url(relay_url, async move {
        run_invite_inner(args, config_file, secret_opts).await
    })
    .await
}

async fn run_invite_inner(
    args: InviteArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let connect_addr = resolve_admin_connect_addr(args.master.clone(), config_file.as_ref());
    let insecure = peer_insecure(args.insecure, cfg_client_insecure(config_file.as_ref()));
    let cluster_secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &connect_addr,
        insecure,
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };
    let had_credential = cluster_secret.is_some()
        || crate::connect::active_client_v3_auth()
            .ok()
            .flatten()
            .is_some();

    if args.code {
        return crate::pair::run_invite_code(args, config_file, secret_opts).await;
    }

    if args.list {
        let mut transport = match connect_master(&connect_addr, cluster_secret.as_ref()).await {
            Ok(t) => t,
            Err(e) => {
                return report_master_failure(
                    &connect_addr,
                    had_credential,
                    "Failed to list invites",
                    &e,
                )
            }
        };
        if let Err(e) = transport.send_msg(&ClientMessage::ListInvites).await {
            eprintln!("Failed to send ListInvites request: {e}");
            return ExitCode::FAILURE;
        }
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::InvitesList { invites })) => {
                render_invites_list(&invites, args.json);
                return ExitCode::SUCCESS;
            }
            Ok(Some(ClientResponse::Error { message })) => {
                eprintln!("Error: {message}");
                return ExitCode::FAILURE;
            }
            Ok(other) => {
                eprintln!("Unexpected response from Master: {other:?}");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("Failed to read response from Master: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    if let Some(ref invite_id) = args.revoke {
        let mut transport = match connect_master(&connect_addr, cluster_secret.as_ref()).await {
            Ok(t) => t,
            Err(e) => {
                return report_master_failure(
                    &connect_addr,
                    had_credential,
                    "Failed to revoke invite",
                    &e,
                )
            }
        };
        if let Err(e) = transport
            .send_msg(&ClientMessage::RevokeInvite {
                invite_id: invite_id.clone(),
            })
            .await
        {
            eprintln!("Failed to send RevokeInvite request: {e}");
            return ExitCode::FAILURE;
        }
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::InviteRevoked { invite_id })) => {
                if args.json {
                    println!("{}", serde_json::json!({ "revoked": invite_id }));
                } else {
                    println!("Revoked invite {invite_id}");
                }
                return ExitCode::SUCCESS;
            }
            Ok(Some(ClientResponse::Error { message })) => {
                eprintln!("Error: {message}");
                return ExitCode::FAILURE;
            }
            Ok(other) => {
                eprintln!("Unexpected response from Master: {other:?}");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("Failed to read response from Master: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    let roles = match parse_roles_csv(&args.roles) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("--roles: {e}");
            return ExitCode::FAILURE;
        }
    };
    if args.uses == 0 {
        eprintln!("--uses: must be at least 1");
        return ExitCode::FAILURE;
    }
    let expires_in_secs = match parse_duration_secs(&args.expires) {
        Ok(secs) => secs,
        Err(e) => {
            eprintln!("--expires: {e}");
            return ExitCode::FAILURE;
        }
    };
    let grants_admin = roles.contains(&MemberRole::Admin);
    if grants_admin && args.no_approval {
        eprintln!("Note: --no-approval ignored because an invite that grants `admin` always requires approval.");
    }
    if grants_admin && args.uses > 1 {
        eprintln!(
            "Note: --uses clamped to 1 because an invite that grants `admin` has a single use."
        );
    }
    let effective_uses = if grants_admin { 1 } else { args.uses };
    let require_approval = if grants_admin {
        true
    } else {
        !args.no_approval
    };

    let (local_master, local_ticket) =
        match resolve_local_invite_targets(&args, config_file.as_ref()) {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };

    let mut transport = match connect_master(&connect_addr, cluster_secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &connect_addr,
                had_credential,
                "Failed to create invite",
                &e,
            )
        }
    };
    if let Err(e) = transport
        .send_msg(&ClientMessage::CreateInvite {
            roles: roles.clone(),
            max_uses: effective_uses,
            expires_in_secs,
            require_approval,
        })
        .await
    {
        eprintln!("Failed to send CreateInvite request: {e}");
        return ExitCode::FAILURE;
    }

    let (invite_id, invite_secret, expires_at, master_key, p2p_ticket) =
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                expires_at,
                master_key,
                p2p_ticket,
                ..
            })) => (invite_id, invite_secret, expires_at, master_key, p2p_ticket),
            Ok(Some(ClientResponse::Error { message })) => {
                eprintln!("Error: {message}");
                return ExitCode::FAILURE;
            }
            Ok(other) => {
                eprintln!("Unexpected response from Master: {other:?}");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("Failed to read response from Master: {e}");
                return ExitCode::FAILURE;
            }
        };

    let (master, ticket) = match finalize_invite_targets(local_master, local_ticket, p2p_ticket) {
        Ok(targets) => targets,
        Err(e) => {
            let _ = transport
                .send_msg(&ClientMessage::RevokeInvite {
                    invite_id: invite_id.clone(),
                })
                .await;
            let _ = transport.recv_msg::<ClientResponse>().await;
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let token = InviteToken {
        master,
        ticket,
        master_key,
        invite_id: invite_id.clone(),
        invite_secret,
        expires_at,
        roles: roles.clone(),
        require_approval,
    };

    if !require_approval {
        eprintln!(
            "WARNING: --no-approval: anyone who obtains this invite before it expires can enrol without your confirmation. Send it only over a channel you trust."
        );
    }
    eprintln!(
        "Invite {} created (roles: {}, uses: {}, approval required: {}). On the new machine run: oxideswarm join  (invite on standard input)",
        invite_id,
        format_roles(&roles),
        effective_uses,
        require_approval
    );

    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "invite": token.encode(),
                "invite_id": invite_id,
                "expires_at": expires_at,
                "roles": roles,
                "uses": effective_uses,
                "require_approval": require_approval,
            })
        );
    } else {
        println!("{}", token.encode());
    }
    ExitCode::SUCCESS
}

fn render_invites_list(invites: &[InviteSummary], json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(invites).unwrap_or_else(|_| "[]".into())
        );
        return;
    }
    if invites.is_empty() {
        println!("No invites.");
        return;
    }
    for inv in invites {
        let status = if inv.revoked {
            "revoked"
        } else if inv.uses_remaining == 0 {
            "exhausted"
        } else {
            "active"
        };
        println!(
            "{}  roles={}  uses={}/{}  expires={}  approval={}  status={}",
            inv.invite_id,
            format_roles(&inv.roles),
            inv.uses_remaining,
            inv.max_uses,
            inv.expires_at,
            inv.require_approval,
            status
        );
    }
}

// ---------------------------------------------------------------------------------------------
// `oxideswarm members`
// ---------------------------------------------------------------------------------------------

/// Manage enrolled cluster members and pending enrollment requests.
#[derive(Parser, Debug, Clone)]
pub struct MembersCliArgs {
    #[command(subcommand)]
    pub action: MembersAction,
}

#[derive(Subcommand, Debug, Clone)]
pub enum MembersAction {
    /// List enrolled cluster members
    List(MembersCommonArgs),
    /// List pending enrollment requests awaiting approval
    Pending(MembersCommonArgs),
    /// Approve a pending enrollment request by its ID (or unique prefix) and 6-digit pairing code
    Approve(MembersApproveArgs),
    /// Reject a pending enrollment request by its ID (or unique prefix)
    Reject(MembersTargetArgs),
    /// Revoke an enrolled member by name or ID
    Revoke(MembersTargetArgs),
    /// Rename an enrolled member
    Rename(MembersRenameArgs),
    /// Release a stopped worker's ID claim so an enrolled member can register with that worker_id
    ReleaseWorker(MembersReleaseWorkerArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct MembersCommonArgs {
    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address of the Master's grid port (default 127.0.0.1:8088)"
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
pub struct MembersApproveArgs {
    /// Pending enrollment ID (or unique hex prefix)
    pub target: String,

    #[arg(
        long,
        value_name = "NNNNNN",
        help = "6-digit pairing code shown on the joining machine"
    )]
    pub code: String,

    #[arg(
        long,
        value_name = "XXXX-XXXX-XXXX-XXXX",
        help = "Optional 16-hex-character master key fingerprint shown on the joining machine"
    )]
    pub fingerprint: Option<String>,

    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address of the Master's grid port (default 127.0.0.1:8088)"
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
pub struct MembersTargetArgs {
    /// Pending ID, member ID, or member name
    pub target: String,

    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address of the Master's grid port (default 127.0.0.1:8088)"
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
pub struct MembersRenameArgs {
    /// Member ID or current member name
    pub target: String,
    /// New human-readable member name
    pub new_name: String,

    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address of the Master's grid port (default 127.0.0.1:8088)"
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
pub struct MembersReleaseWorkerArgs {
    /// Worker UUID to release (must not be currently connected)
    pub worker_id: uuid::Uuid,

    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address of the Master's grid port (default 127.0.0.1:8088)"
    )]
    pub master: Option<String>,

    #[arg(long, value_name = "URL", help = RELAY_URL_HELP)]
    pub relay_url: Option<String>,

    #[arg(long, help = "Format output as JSON")]
    pub json: bool,

    #[arg(long, help = INSECURE_PEER_HELP)]
    pub insecure: bool,
}

impl MembersAction {
    fn common_opts(&self) -> (Option<String>, Option<String>, bool, bool) {
        match self {
            Self::List(a) | Self::Pending(a) => {
                (a.master.clone(), a.relay_url.clone(), a.json, a.insecure)
            }
            Self::Approve(a) => (a.master.clone(), a.relay_url.clone(), a.json, a.insecure),
            Self::Reject(a) | Self::Revoke(a) => {
                (a.master.clone(), a.relay_url.clone(), a.json, a.insecure)
            }
            Self::Rename(a) => (a.master.clone(), a.relay_url.clone(), a.json, a.insecure),
            Self::ReleaseWorker(a) => (a.master.clone(), a.relay_url.clone(), a.json, a.insecure),
        }
    }
}

/// Runs `oxideswarm members ...`.
pub async fn run_members(
    args: MembersCliArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let (_, cli_relay, _, _) = args.action.common_opts();
    let p2p_minimal = crate::cli_app::resolve_p2p_minimal();
    let relay_url = match crate::cli_app::resolve_relay_url(
        cli_relay,
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        p2p_minimal,
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    with_client_relay_url(relay_url, async move {
        run_members_inner(args, config_file, secret_opts).await
    })
    .await
}

async fn run_members_inner(
    args: MembersCliArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if let MembersAction::Approve(ref a) = args.action {
        let clean_code: String = a.code.trim().chars().filter(|c| *c != '-').collect();
        if clean_code.len() != 6 || !clean_code.bytes().all(|b| b.is_ascii_digit()) {
            eprintln!(
                "--code: expected the 6-digit pairing code shown on the joining machine (for example --code 123456)"
            );
            return ExitCode::FAILURE;
        }
        if let Some(ref fp) = a.fingerprint {
            let clean_fp: String = fp.trim().chars().filter(|c| *c != '-').collect();
            if clean_fp.len() != 16 || !clean_fp.bytes().all(|b| b.is_ascii_hexdigit()) {
                eprintln!(
                    "--fingerprint: expected the 16-hex-character master key fingerprint shown on the joining machine (for example --fingerprint 1234-5678-9abc-def0)"
                );
                return ExitCode::FAILURE;
            }
        }
    }

    let (cli_master, _, json, insecure_flag) = args.action.common_opts();
    let connect_addr = resolve_admin_connect_addr(cli_master, config_file.as_ref());
    let insecure = peer_insecure(insecure_flag, cfg_client_insecure(config_file.as_ref()));
    let cluster_secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Status,
        &connect_addr,
        insecure,
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };
    let had_credential = cluster_secret.is_some()
        || crate::connect::active_client_v3_auth()
            .ok()
            .flatten()
            .is_some();

    let mut transport = match connect_master(&connect_addr, cluster_secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &connect_addr,
                had_credential,
                "Failed to connect to Master",
                &e,
            )
        }
    };

    if let MembersAction::Approve(ref a) = args.action {
        if let Some(ref fp) = a.fingerprint {
            let expected_clean: String = fp
                .trim()
                .chars()
                .filter(|c| *c != '-')
                .collect::<String>()
                .to_ascii_lowercase();
            if let Err(e) = transport
                .send_msg(&ClientMessage::ListPendingEnrollments)
                .await
            {
                eprintln!("Failed to query pending enrollments: {e}");
                return ExitCode::FAILURE;
            }
            match transport.recv_msg::<ClientResponse>().await {
                Ok(Some(ClientResponse::PendingEnrollmentsList { pending })) => {
                    let target_lower = a.target.trim().to_ascii_lowercase();
                    let matches: Vec<_> = pending
                        .iter()
                        .filter(|p| {
                            let pid = p.pending_id.to_ascii_lowercase();
                            pid == target_lower
                                || (target_lower.len() >= 4
                                    && target_lower.chars().all(|c| c.is_ascii_hexdigit())
                                    && pid.starts_with(&target_lower))
                        })
                        .collect();
                    let Some(matched) = (if matches.len() == 1 {
                        Some(matches[0])
                    } else {
                        pending
                            .iter()
                            .find(|p| p.pending_id.eq_ignore_ascii_case(&target_lower))
                    }) else {
                        if matches.len() > 1 {
                            eprintln!(
                                "Error: ambiguous pending enrollment prefix '{}'",
                                sanitize_terminal_text(&a.target)
                            );
                        } else {
                            eprintln!(
                                "Error: no pending enrollment '{}'",
                                sanitize_terminal_text(&a.target)
                            );
                        }
                        return ExitCode::FAILURE;
                    };
                    let actual_clean: String = matched
                        .master_fingerprint
                        .chars()
                        .filter(|c| *c != '-')
                        .collect::<String>()
                        .to_ascii_lowercase();
                    if actual_clean != expected_clean {
                        eprintln!(
                            "Error: master key fingerprint mismatch (joining machine showed {}, master has {})",
                            sanitize_terminal_text(fp.trim()),
                            sanitize_terminal_text(&matched.master_fingerprint)
                        );
                        return ExitCode::FAILURE;
                    }
                }
                Ok(Some(ClientResponse::Error { message })) => {
                    eprintln!("Error: {}", sanitize_terminal_text(&message));
                    return ExitCode::FAILURE;
                }
                Ok(other) => {
                    eprintln!("Unexpected response from Master: {other:?}");
                    return ExitCode::FAILURE;
                }
                Err(e) => {
                    eprintln!("Failed to read response from Master: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    let req = match &args.action {
        MembersAction::List(_) => ClientMessage::ListMembers,
        MembersAction::Pending(_) => ClientMessage::ListPendingEnrollments,
        MembersAction::Approve(a) => {
            let clean_code: String = a.code.trim().chars().filter(|c| *c != '-').collect();
            let pairing_payload = match a.fingerprint {
                Some(ref fp) => format!("{clean_code}@{}", fp.trim()),
                None => clean_code,
            };
            ClientMessage::ApproveEnrollment {
                pending_id: a.target.clone(),
                pairing_code: RedactedString(pairing_payload),
            }
        }
        MembersAction::Reject(a) => ClientMessage::RejectEnrollment {
            pending_id: a.target.clone(),
        },
        MembersAction::Revoke(a) => ClientMessage::RevokeMember {
            target: a.target.clone(),
        },
        MembersAction::Rename(a) => ClientMessage::RenameMember {
            target: a.target.clone(),
            new_name: a.new_name.clone(),
        },
        MembersAction::ReleaseWorker(a) => ClientMessage::ReleaseWorkerId {
            worker_id: a.worker_id,
        },
    };

    if let Err(e) = transport.send_msg(&req).await {
        eprintln!("Failed to send members request: {e}");
        return ExitCode::FAILURE;
    }

    match transport.recv_msg::<ClientResponse>().await {
        Ok(Some(ClientResponse::MembersList { members })) => {
            render_members_list(&members, json);
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::PendingEnrollmentsList { pending })) => {
            render_pending_list(&pending, json);
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::EnrollmentApproved {
            pending_id,
            member_id,
            name,
            roles,
        })) => {
            let safe_name = sanitize_terminal_text(&name);
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "pending_id": pending_id,
                        "member_id": member_id,
                        "name": safe_name,
                        "roles": roles,
                    })
                );
            } else {
                println!(
                    "Approved member {safe_name} ({member_id}) with roles: {}",
                    format_roles(&roles)
                );
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::EnrollmentRejected { pending_id })) => {
            if json {
                println!("{}", serde_json::json!({ "rejected": pending_id }));
            } else {
                println!("Rejected pending enrollment {pending_id}");
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::MemberRevoked {
            member_id,
            disconnected_sessions,
        })) => {
            eprintln!(
                "Note: if the master runs with `--auth both` and the revoked machine also holds the shared cluster secret, it can still connect via v2 until you switch the master to `--auth members` or rotate the secret with `oxideswarm secret generate --force`."
            );
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "revoked_id": member_id,
                        "disconnected_sessions": disconnected_sessions,
                    })
                );
            } else {
                println!(
                    "Revoked member {member_id} (disconnected {disconnected_sessions} session(s))"
                );
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::MemberRenamed { member_id, name })) => {
            let safe_name = sanitize_terminal_text(&name);
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "member_id": member_id,
                        "name": safe_name,
                    })
                );
            } else {
                println!("Renamed member {member_id} to {safe_name}");
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::WorkerIdReleased { worker_id })) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "released_worker_id": worker_id,
                    })
                );
            } else {
                println!("Released worker_id claim {worker_id}");
            }
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::Error { message })) => {
            eprintln!("Error: {}", sanitize_terminal_text(&message));
            ExitCode::FAILURE
        }
        Ok(other) => {
            eprintln!("Unexpected response from Master: {other:?}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("Failed to read response from Master: {e}");
            ExitCode::FAILURE
        }
    }
}

fn render_members_list(members: &[MemberEntry], json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(members).unwrap_or_else(|_| "[]".into())
        );
        return;
    }
    if members.is_empty() {
        println!("No enrolled members.");
        return;
    }
    for m in members {
        let enrolled_at = m
            .enrolled_at
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "-".into());
        let last_seen = m
            .last_seen
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "never".into());
        let invite = sanitize_terminal_text(m.via_invite.as_deref().unwrap_or("-"));
        let safe_name = sanitize_terminal_text(&m.name);
        println!(
            "{}  name={}  roles={}  fingerprint={}  enrolled_at={}  last_seen={}  invite={}",
            m.id,
            safe_name,
            format_roles(&m.roles),
            m.public_key.fingerprint(),
            enrolled_at,
            last_seen,
            invite
        );
    }
}

fn render_pending_list(pending: &[PendingEnrollmentInfo], json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(pending).unwrap_or_else(|_| "[]".into())
        );
        return;
    }
    if pending.is_empty() {
        println!("No pending enrollment requests.");
        return;
    }
    for p in pending {
        let same_invite_count = pending
            .iter()
            .filter(|other| other.invite_id == p.invite_id)
            .count();
        let contention = if same_invite_count > 1 {
            format!(
                "  [{same_invite_count} requests for invite {}]",
                sanitize_terminal_text(&p.invite_id)
            )
        } else {
            String::new()
        };
        println!(
            "{}  from={}  name={}  os={}/{}  member_id={}  roles={}  invite={}{}",
            sanitize_terminal_text(&p.pending_id),
            sanitize_terminal_text(&p.remote_addr),
            sanitize_terminal_text(&p.name),
            sanitize_terminal_text(&p.os),
            sanitize_terminal_text(&p.arch),
            sanitize_terminal_text(&p.member_id),
            format_roles(&p.roles),
            sanitize_terminal_text(&p.invite_id),
            contention
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::noise_auth::NodeKeypair;

    fn sample_invite() -> InviteToken {
        let kp = NodeKeypair::generate().unwrap();
        InviteToken {
            master: Some("192.168.1.10:8088".into()),
            ticket: Some("node1ticketexample".into()),
            master_key: *kp.public_key(),
            invite_id: "0123456789abcdef0123456789abcdef".into(),
            invite_secret: RedactedBytes32([0x5a; 32]),
            expires_at: 1_900_000_000,
            roles: vec![MemberRole::Worker, MemberRole::Client],
            require_approval: true,
        }
    }

    #[test]
    fn invite_token_round_trip_and_redaction() {
        let inv = sample_invite();
        let encoded = inv.encode();
        assert!(encoded.starts_with(INVITE_PREFIX));
        let dbg = format!("{inv:?}");
        assert!(!dbg.contains("5a5a5a5a"));
        assert!(!dbg.contains("node1ticketexample"));

        let decoded = InviteToken::decode(&format!("  {encoded}\n")).unwrap();
        assert_eq!(decoded, inv);
        assert!(!decoded.is_expired_at(1_899_999_999));
        assert!(decoded.is_expired_at(1_900_000_000));
        assert!(decoded.is_expired_at(1_900_000_001));
    }

    #[test]
    fn invite_token_rejects_every_single_character_corruption() {
        let encoded = sample_invite().encode();
        let prefix_len = INVITE_PREFIX.len();
        for i in 0..encoded.len() {
            let mut bytes = encoded.clone().into_bytes();
            bytes[i] = if bytes[i] == b'a' { b'b' } else { b'a' };
            let mutated = String::from_utf8(bytes).unwrap();
            let res = InviteToken::decode(&mutated);
            if i < prefix_len {
                assert_eq!(res.err(), Some(InviteTokenError::NotAnInvite), "pos {i}");
            } else {
                assert!(res.is_err(), "mutation at pos {i} was accepted");
            }
        }
        assert!(InviteToken::decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(InviteToken::decode(&format!("{encoded}0")).is_err());
    }

    #[test]
    fn duration_and_roles_parsing() {
        assert_eq!(parse_duration_secs("24h").unwrap(), 86_400);
        assert_eq!(parse_duration_secs("30m").unwrap(), 1_800);
        assert_eq!(parse_duration_secs("45s").unwrap(), 45);
        assert_eq!(parse_duration_secs("2d").unwrap(), 172_800);
        assert_eq!(parse_duration_secs("120").unwrap(), 120);
        assert!(parse_duration_secs("0s").is_err());
        assert!(parse_duration_secs("").is_err());
        assert!(parse_duration_secs("abc").is_err());

        assert_eq!(
            parse_roles_csv("worker, client, worker").unwrap(),
            vec![MemberRole::Worker, MemberRole::Client]
        );
        assert!(parse_roles_csv("").is_err());
        assert!(parse_roles_csv("superadmin").is_err());
    }

    #[test]
    fn invite_args_code_flag_conflicts() {
        let ok = InviteArgs::try_parse_from(["invite", "--code"]).unwrap();
        assert!(ok.code);
        assert!(InviteArgs::try_parse_from(["invite", "--code", "--list"]).is_err());
        assert!(InviteArgs::try_parse_from(["invite", "--code", "--revoke", "abc"]).is_err());
        assert!(InviteArgs::try_parse_from(["invite", "--code", "--no-approval"]).is_err());
        assert!(InviteArgs::try_parse_from(["invite", "--code", "--uses", "2"]).is_err());
        assert!(InviteArgs::try_parse_from(["invite", "--code", "--json"]).is_err());
    }
}
