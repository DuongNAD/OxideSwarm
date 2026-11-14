//! LAN short-code pairing (`JOIN-2`, design record `docs/design/0052-short-code-pairing.md`).
//!
//! Runs a password-authenticated key exchange (SPAKE2 over Ed25519) between a one-shot
//! `oxideswarm invite --code` process on an already-enrolled admin machine and
//! `oxideswarm join --code` on the joining machine. Once both sides prove knowledge of the same
//! two words, the inviter sends an encrypted single-use approval-required [`InviteToken`] and
//! monitors `ListPendingEnrollments` to approve the enrollment interactively.

use std::collections::HashSet;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::hkdf::{KeyType, Salt, HKDF_SHA256};
use ring::hmac;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::discovery::get_subnet_broadcast_ip;
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{PendingEnrollmentInfo, PublicKey, RedactedString};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use serde::{Deserialize, Serialize};
use spake2::{Ed25519Group, Identity, Password, Spake2};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::Instant;

use crate::config::{ConfigFile, SecretSection};
use crate::connect::{
    active_client_v3_auth, connect_master, normalize_master_addr, report_master_failure,
    DEFAULT_MASTER_PORT,
};
use crate::join::{parse_yes_no_confirmation, run_join_invite_with_pair, JoinArgs};
use crate::members_cmd::{
    cfg_client_insecure, finalize_invite_targets, format_roles, parse_duration_secs,
    parse_roles_csv, resolve_admin_connect_addr, resolve_local_invite_targets,
    sanitize_terminal_text, InviteArgs, InviteToken,
};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};
use crate::wordlist::PairCode;

/// Dedicated UDP port on which `invite --code` answers short-code discovery queries (`design 0052` D2).
pub const PAIR_UDP_PORT: u16 = 8087;
/// Current short-code pairing wire protocol version (`v` field).
pub const PAIR_PROTOCOL_VERSION: u8 = 1;
/// Hard limit per framed TCP message during short-code pairing (`design 0052` D4).
pub const MAX_PAIR_MESSAGE_BYTES: usize = 4096;
/// Maximum time allowed for each network read/write step (`design 0052` D3).
pub const PAIR_STEP_TIMEOUT: Duration = Duration::from_secs(5);
/// Maximum time `join --code` waits for a matching UDP reply (`design 0052` D2).
pub const PAIR_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum lifetime of one `invite --code` session (`design 0052` D4).
pub const PAIR_TOTAL_TIMEOUT: Duration = Duration::from_secs(600);

/// Exact burn message required by `docs/design/0052-short-code-pairing.md` D3.
pub const PAIR_BURN_MESSAGE: &str =
    "the code was used by someone who did not know it; nothing was shared; make a new code";

/// Internal test override: bind host for pairing TCP/UDP listeners (e.g. `127.0.0.1` in tests).
const ENV_PAIR_BIND_HOST: &str = "RUSTY_GRID_PAIR_BIND_HOST";
/// Internal test override: UDP port for short-code discovery (`0` = ephemeral on inviter).
const ENV_PAIR_UDP_PORT: &str = "RUSTY_GRID_PAIR_UDP_PORT";

const SPAKE2_IDENTITY_JOINER: &[u8] = b"oxs-pair-joiner";
const SPAKE2_IDENTITY_INVITER: &[u8] = b"oxs-pair-inviter";
const CONFIRM_LABEL_JOINER: &[u8] = b"oxs-pair-confirm-joiner";
const CONFIRM_LABEL_INVITER: &[u8] = b"oxs-pair-confirm-inviter";
const HKDF_SALT: &[u8] = b"oxs-pair-v1-aead-salt";
const HKDF_INFO_I2J: &[u8] = b"oxs-pair-v1-key-i2j";
const HKDF_INFO_J2I: &[u8] = b"oxs-pair-v1-key-j2i";
const AAD_INVITE_V1: &[u8] = b"oxs-pair-v1-invite";
const AAD_PENDING_V1: &[u8] = b"oxs-pair-v1-pending";

const DISCOVERY_KIND_QUERY: &str = "oxs-pair-query";
const DISCOVERY_KIND_REPLY: &str = "oxs-pair";
const DISCOVERY_QUERY_INTERVAL: Duration = Duration::from_secs(1);
const PENDING_NOTICE_TIMEOUT: Duration = Duration::from_secs(30);
const PENDING_POLL_INTERVAL: Duration = Duration::from_millis(100);
const PENDING_POLL_TIMEOUT: Duration = Duration::from_secs(3);

/// Pending enrollment details sent back over the SPAKE2-authenticated channel from joiner to inviter
/// (`docs/design/0052-short-code-pairing.md` D3 step 5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairPendingNotice {
    pub pending_id: String,
    pub pairing_code: String,
    pub master_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairDiscoveryQuery {
    kind: String,
    v: u8,
    nameplate: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairDiscoveryReply {
    kind: String,
    v: u8,
    nameplate: u16,
    port: u16,
}

struct KeyLen32;

impl KeyType for KeyLen32 {
    fn len(&self) -> usize {
        32
    }
}

#[derive(Clone)]
struct PairSessionKeys {
    hmac_key: hmac::Key,
    k_i2j: [u8; 32],
    k_j2i: [u8; 32],
}

#[derive(Debug)]
enum FrameIoError {
    Closed,
    Timeout,
    Oversized(usize),
    Malformed(&'static str),
    Io(std::io::Error),
}

impl std::fmt::Display for FrameIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("peer closed pairing connection"),
            Self::Timeout => f.write_str("pairing step timed out"),
            Self::Oversized(len) => write!(
                f,
                "pairing frame size {len} exceeds maximum {MAX_PAIR_MESSAGE_BYTES} bytes"
            ),
            Self::Malformed(why) => write!(f, "malformed pairing frame: {why}"),
            Self::Io(e) => write!(f, "pairing I/O error: {e}"),
        }
    }
}

#[derive(Debug)]
enum InviterExchangeOutcome {
    /// Pre-`msg_i` peer stalled, closed early, or sent invalid SPAKE2 data; session did NOT burn.
    StallOrMalformed(String),
    /// Peer triggered `msg_i` and failed to confirm; invite MUST be revoked/burned immediately.
    Burned,
    /// Mutual confirmation succeeded; invite delivered and `PairPendingNotice` received.
    Paired(PairPendingNotice),
}

/// Normalizes IPv4-mapped IPv6 addresses (`::ffff:192.168.1.5`) back to `IpAddr::V4`.
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    }
}

fn pair_bind_ip() -> Result<IpAddr, String> {
    match std::env::var(ENV_PAIR_BIND_HOST) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            } else {
                trimmed
                    .parse::<IpAddr>()
                    .map_err(|e| format!("{ENV_PAIR_BIND_HOST} '{trimmed}' is invalid: {e}"))
            }
        }
        Err(_) => Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
    }
}

fn pair_udp_port() -> Result<u16, String> {
    match std::env::var(ENV_PAIR_UDP_PORT) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(PAIR_UDP_PORT)
            } else {
                trimmed
                    .parse::<u16>()
                    .map_err(|e| format!("{ENV_PAIR_UDP_PORT} '{trimmed}' is invalid: {e}"))
            }
        }
        Err(_) => Ok(PAIR_UDP_PORT),
    }
}

fn parse_pair_host(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().any(char::is_whitespace) {
        return Err("--pair-host must be HOST:PORT (for example 192.168.1.5:51234)".into());
    }
    if let Ok(sa) = trimmed.parse::<SocketAddr>() {
        if sa.port() == 0 {
            return Err("--pair-host port must be non-zero".into());
        }
        return Ok(sa.to_string());
    }
    let Some((host, port_str)) = trimmed.rsplit_once(':') else {
        return Err("--pair-host must include :PORT (for example 192.168.1.5:51234)".into());
    };
    if host.is_empty() || host.contains(':') {
        return Err(
            "--pair-host must be HOST:PORT (wrap IPv6 addresses in brackets: [::1]:PORT)".into(),
        );
    }
    let port: u16 = port_str
        .parse()
        .map_err(|_| "--pair-host port must be a valid TCP port (1..=65535)".to_string())?;
    if port == 0 {
        return Err("--pair-host port must be non-zero".into());
    }
    Ok(format!("{host}:{port}"))
}

fn aead_nonce(seq: u64) -> Nonce {
    let mut bytes = [0u8; 12];
    bytes[4..12].copy_from_slice(&seq.to_be_bytes());
    Nonce::assume_unique_for_key(bytes)
}

fn derive_session_keys(key_k: &[u8]) -> GridResult<PairSessionKeys> {
    let prk = Salt::new(HKDF_SHA256, HKDF_SALT).extract(key_k);
    let mut k_i2j = [0u8; 32];
    let mut k_j2i = [0u8; 32];

    let okm_i2j = prk
        .expand(&[HKDF_INFO_I2J], KeyLen32)
        .map_err(|_| GridError::Config("HKDF expand failed for pairing i2j key".into()))?;
    okm_i2j
        .fill(&mut k_i2j)
        .map_err(|_| GridError::Config("HKDF fill failed for pairing i2j key".into()))?;

    let okm_j2i = prk
        .expand(&[HKDF_INFO_J2I], KeyLen32)
        .map_err(|_| GridError::Config("HKDF expand failed for pairing j2i key".into()))?;
    okm_j2i
        .fill(&mut k_j2i)
        .map_err(|_| GridError::Config("HKDF fill failed for pairing j2i key".into()))?;

    Ok(PairSessionKeys {
        hmac_key: hmac::Key::new(hmac::HMAC_SHA256, key_k),
        k_i2j,
        k_j2i,
    })
}

fn seal_aead(key_bytes: &[u8; 32], seq: u64, aad: &[u8], plaintext: &[u8]) -> GridResult<Vec<u8>> {
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, key_bytes)
        .map_err(|_| GridError::Config("failed to initialize ChaCha20-Poly1305 key".into()))?;
    let key = LessSafeKey::new(unbound);
    let mut buf = plaintext.to_vec();
    key.seal_in_place_append_tag(aead_nonce(seq), Aad::from(aad), &mut buf)
        .map_err(|_| GridError::Config("AEAD encryption failed during pairing".into()))?;
    if buf.len() > MAX_PAIR_MESSAGE_BYTES {
        return Err(GridError::Config(format!(
            "pairing message ({} bytes) exceeds 4096-byte limit",
            buf.len()
        )));
    }
    Ok(buf)
}

fn open_aead<'a>(
    key_bytes: &[u8; 32],
    seq: u64,
    aad: &[u8],
    ciphertext: &'a mut [u8],
) -> GridResult<&'a [u8]> {
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, key_bytes)
        .map_err(|_| GridError::Config("failed to initialize ChaCha20-Poly1305 key".into()))?;
    let key = LessSafeKey::new(unbound);
    key.open_in_place(aead_nonce(seq), Aad::from(aad), ciphertext)
        .map(|slice| &*slice)
        .map_err(|_| GridError::Config("AEAD authentication failed during pairing".into()))
}

async fn send_frame_with_timeout(
    stream: &mut TcpStream,
    data: &[u8],
    step_timeout: Duration,
) -> Result<(), FrameIoError> {
    if data.is_empty() {
        return Err(FrameIoError::Malformed("empty pairing frame"));
    }
    if data.len() > MAX_PAIR_MESSAGE_BYTES {
        return Err(FrameIoError::Oversized(data.len()));
    }
    let len_bytes = (data.len() as u32).to_be_bytes();
    tokio::time::timeout(step_timeout, async {
        stream.write_all(&len_bytes).await?;
        stream.write_all(data).await?;
        stream.flush().await?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|_| FrameIoError::Timeout)?
    .map_err(|e| match e.kind() {
        std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionAborted
        | std::io::ErrorKind::UnexpectedEof => FrameIoError::Closed,
        _ => FrameIoError::Io(e),
    })
}

async fn recv_frame_with_timeout(
    stream: &mut TcpStream,
    step_timeout: Duration,
) -> Result<Vec<u8>, FrameIoError> {
    tokio::time::timeout(step_timeout, async {
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            return Err(classify_read_err(e));
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 {
            return Err(FrameIoError::Malformed("zero-length pairing frame"));
        }
        if len > MAX_PAIR_MESSAGE_BYTES {
            return Err(FrameIoError::Oversized(len));
        }
        let mut payload = vec![0u8; len];
        if let Err(e) = stream.read_exact(&mut payload).await {
            return Err(classify_read_err(e));
        }
        Ok(payload)
    })
    .await
    .map_err(|_| FrameIoError::Timeout)?
}

fn classify_read_err(err: std::io::Error) -> FrameIoError {
    match err.kind() {
        std::io::ErrorKind::UnexpectedEof
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionAborted => FrameIoError::Closed,
        _ => FrameIoError::Io(err),
    }
}

fn resolve_master_port(
    local_master: Option<&str>,
    config_file: Option<&ConfigFile>,
    connect_addr: &str,
) -> u16 {
    let parse_port = |s: &str| {
        normalize_master_addr(s)
            .parse::<SocketAddr>()
            .ok()
            .map(|sa| sa.port())
            .filter(|&p| p != 0)
            .or_else(|| {
                s.rsplit_once(':')
                    .and_then(|(_, p)| p.parse::<u16>().ok())
                    .filter(|&p| p != 0)
            })
    };
    local_master
        .and_then(parse_port)
        .or_else(|| {
            config_file
                .and_then(|c| c.master.as_ref())
                .and_then(|m| m.listen.as_deref())
                .and_then(|l| l.parse::<SocketAddr>().ok())
                .map(|sa| sa.port())
                .filter(|&p| p != 0)
        })
        .or_else(|| parse_port(connect_addr))
        .unwrap_or(DEFAULT_MASTER_PORT)
}

/// Computes the reachable `(master, ticket)` targets to encode inside the `InviteToken` delivered
/// to the LAN joiner over the authenticated TCP pairing session.
fn resolve_pair_invite_targets(
    cli_master: Option<&str>,
    local_master: Option<String>,
    local_ticket: Option<String>,
    p2p_ticket: Option<String>,
    config_file: Option<&ConfigFile>,
    connect_addr: &str,
    tcp_local_ip: IpAddr,
) -> GridResult<(Option<String>, Option<String>)> {
    let tcp_local_ip = normalize_ip(tcp_local_ip);
    let port = resolve_master_port(local_master.as_deref(), config_file, connect_addr);

    let adjusted_master = match (cli_master, local_master) {
        // Explicit `--master <HOST:PORT>` from CLI: keep concrete LAN/DNS host verbatim,
        // substituting only `0.0.0.0`/`::` or loopback when the joiner arrived over a non-loopback NIC.
        (Some(_), Some(m)) => {
            if let Ok(sa) = m.parse::<SocketAddr>() {
                let ip = normalize_ip(sa.ip());
                if ip.is_unspecified() || (ip.is_loopback() && !tcp_local_ip.is_loopback()) {
                    Some(SocketAddr::new(tcp_local_ip, sa.port()).to_string())
                } else {
                    Some(m)
                }
            } else {
                Some(m)
            }
        }
        // Admin passed `--master <p2p-ticket>` -> keep `(None, ticket)`.
        (Some(_), None) => None,
        // No `--master` flag: prefer non-loopback configured host if set, otherwise use the NIC IP
        // of the pairing TCP socket itself paired with the master port.
        (None, Some(m)) => {
            let is_loopback_or_unspecified = if let Ok(sa) = m.parse::<SocketAddr>() {
                let ip = normalize_ip(sa.ip());
                ip.is_unspecified() || ip.is_loopback()
            } else {
                m.to_ascii_lowercase().starts_with("localhost:")
            };
            if is_loopback_or_unspecified {
                Some(SocketAddr::new(tcp_local_ip, port).to_string())
            } else {
                Some(m)
            }
        }
        (None, None) => {
            if tcp_local_ip.is_unspecified() {
                None
            } else {
                Some(SocketAddr::new(tcp_local_ip, port).to_string())
            }
        }
    };

    finalize_invite_targets(adjusted_master, local_ticket, p2p_ticket)
}

fn format_pairing_check(digits: &str) -> String {
    let clean: String = digits.chars().filter(|c| c.is_ascii_digit()).collect();
    if clean.len() == 6 {
        format!("{} {}", &clean[..3], &clean[3..])
    } else {
        sanitize_terminal_text(digits)
    }
}

/// Core inviter-side SPAKE2 exchange + burn enforcement + encrypted `InviteToken` /
/// `PairPendingNotice` exchange over an already-accepted TCP connection.
#[allow(clippy::too_many_arguments)]
async fn run_inviter_session_on_stream<F, Fut>(
    stream: &mut TcpStream,
    code: &PairCode,
    step_timeout: Duration,
    pending_notice_timeout: Duration,
    build_invite_token: F,
) -> InviterExchangeOutcome
where
    F: FnOnce(IpAddr) -> Fut,
    Fut: std::future::Future<Output = GridResult<String>>,
{
    let local_ip = match stream.local_addr() {
        Ok(addr) => normalize_ip(addr.ip()),
        Err(e) => {
            return InviterExchangeOutcome::StallOrMalformed(format!(
                "cannot read local socket address: {e}"
            ))
        }
    };

    // Step 1 (pre-msg_i): read joiner's SPAKE2 message and verify group element BEFORE sending
    // `msg_i`. Stalls or malformed probes here do NOT burn the short code.
    let msg_j = match recv_frame_with_timeout(stream, step_timeout).await {
        Ok(bytes) => bytes,
        Err(e) => return InviterExchangeOutcome::StallOrMalformed(e.to_string()),
    };

    let pw = Password::new(code.spake2_password());
    let (state, msg_i) = Spake2::<Ed25519Group>::start_b(
        &pw,
        &Identity::new(SPAKE2_IDENTITY_JOINER),
        &Identity::new(SPAKE2_IDENTITY_INVITER),
    );
    let key_k = match state.finish(&msg_j) {
        Ok(k) => k,
        Err(e) => {
            return InviterExchangeOutcome::StallOrMalformed(format!(
                "invalid initial SPAKE2 probe: {e:?}"
            ))
        }
    };
    let keys = match derive_session_keys(&key_k) {
        Ok(k) => k,
        Err(e) => return InviterExchangeOutcome::StallOrMalformed(e.to_string()),
    };

    // Step 2: send `msg_i`. From this point on, the burn rule is armed: any failure to obtain a
    // valid joiner confirmation MUST burn (revoke) the underlying invite and exit with failure.
    if send_frame_with_timeout(stream, &msg_i, step_timeout)
        .await
        .is_err()
    {
        return InviterExchangeOutcome::Burned;
    }

    let confirm_j = match recv_frame_with_timeout(stream, step_timeout).await {
        Ok(bytes) => bytes,
        Err(_) => return InviterExchangeOutcome::Burned,
    };

    if hmac::verify(&keys.hmac_key, CONFIRM_LABEL_JOINER, &confirm_j).is_err() {
        return InviterExchangeOutcome::Burned;
    }

    // Step 3: joiner proved knowledge of the password; send inviter confirmation and sealed invite.
    let confirm_i = hmac::sign(&keys.hmac_key, CONFIRM_LABEL_INVITER);
    if send_frame_with_timeout(stream, confirm_i.as_ref(), step_timeout)
        .await
        .is_err()
    {
        return InviterExchangeOutcome::Burned;
    }

    let invite_encoded = match build_invite_token(local_ip).await {
        Ok(s) => RedactedString(s),
        Err(_) => return InviterExchangeOutcome::Burned,
    };

    let Ok(sealed_invite) = seal_aead(&keys.k_i2j, 0, AAD_INVITE_V1, invite_encoded.0.as_bytes())
    else {
        return InviterExchangeOutcome::Burned;
    };
    drop(invite_encoded);

    if send_frame_with_timeout(stream, &sealed_invite, step_timeout)
        .await
        .is_err()
    {
        return InviterExchangeOutcome::Burned;
    }

    // Step 4: receive encrypted `PairPendingNotice` once the joiner submits `EnrollWithInvite`.
    let mut sealed_pending = match recv_frame_with_timeout(stream, pending_notice_timeout).await {
        Ok(bytes) => bytes,
        Err(_) => return InviterExchangeOutcome::Burned,
    };
    let Ok(plain_pending) = open_aead(&keys.k_j2i, 1, AAD_PENDING_V1, &mut sealed_pending) else {
        return InviterExchangeOutcome::Burned;
    };
    let Ok(notice) = serde_json::from_slice::<PairPendingNotice>(plain_pending) else {
        return InviterExchangeOutcome::Burned;
    };

    InviterExchangeOutcome::Paired(notice)
}

/// Core joiner-side SPAKE2 exchange over TCP. Returns `(invite_token_string, k_j2i, tcp_stream)`
/// so the caller can deliver the encrypted `PairPendingNotice` once `EnrollPending` returns.
async fn run_joiner_exchange_on_stream(
    mut stream: TcpStream,
    code: &PairCode,
    step_timeout: Duration,
) -> GridResult<(RedactedString, [u8; 32], TcpStream)> {
    let pw = Password::new(code.spake2_password());
    let (state, msg_j) = Spake2::<Ed25519Group>::start_a(
        &pw,
        &Identity::new(SPAKE2_IDENTITY_JOINER),
        &Identity::new(SPAKE2_IDENTITY_INVITER),
    );

    send_frame_with_timeout(&mut stream, &msg_j, step_timeout)
        .await
        .map_err(|e| GridError::Config(format!("failed sending SPAKE2 handshake: {e}")))?;

    let msg_i = recv_frame_with_timeout(&mut stream, step_timeout)
        .await
        .map_err(|e| GridError::Config(format!("failed receiving inviter SPAKE2 reply: {e}")))?;

    let key_k = state
        .finish(&msg_i)
        .map_err(|e| GridError::Config(format!("SPAKE2 key agreement failed: {e:?}")))?;
    let keys = derive_session_keys(&key_k)?;

    // Joiner confirms first (`design 0052` D3 step 2).
    let confirm_j = hmac::sign(&keys.hmac_key, CONFIRM_LABEL_JOINER);
    send_frame_with_timeout(&mut stream, confirm_j.as_ref(), step_timeout)
        .await
        .map_err(|e| GridError::Config(format!("failed sending pairing confirmation: {e}")))?;

    let confirm_i = recv_frame_with_timeout(&mut stream, step_timeout)
        .await
        .map_err(|e| match e {
            FrameIoError::Closed | FrameIoError::Timeout => GridError::Config(
                "pairing code rejected or closed by inviter (check that the two words match)"
                    .into(),
            ),
            other => GridError::Config(format!("failed receiving inviter confirmation: {other}")),
        })?;

    hmac::verify(&keys.hmac_key, CONFIRM_LABEL_INVITER, &confirm_i).map_err(|_| {
        GridError::Config(
            "pairing confirmation from inviter did not match (possible wrong code or tampered channel)"
                .into(),
        )
    })?;

    let mut sealed_invite = recv_frame_with_timeout(&mut stream, step_timeout)
        .await
        .map_err(|e| GridError::Config(format!("failed receiving encrypted invite: {e}")))?;

    let plain_invite = open_aead(&keys.k_i2j, 0, AAD_INVITE_V1, &mut sealed_invite)?;
    let invite_text = std::str::from_utf8(plain_invite)
        .map_err(|_| GridError::Config("decrypted invite is not valid UTF-8".into()))?
        .to_string();

    Ok((RedactedString(invite_text), keys.k_j2i, stream))
}

async fn discover_inviter_tcp_addr(
    code: &PairCode,
    pair_host: Option<&str>,
    udp_port: u16,
    bind_host: IpAddr,
    discovery_timeout: Duration,
) -> GridResult<String> {
    if let Some(explicit) = pair_host {
        return parse_pair_host(explicit).map_err(GridError::Config);
    }

    let loopback_only = bind_host.is_loopback();
    let client_bind = if loopback_only {
        SocketAddr::new(bind_host, 0)
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    };
    let sock = UdpSocket::bind(client_bind).await.map_err(|e| {
        GridError::Config(format!(
            "cannot bind UDP discovery socket on {client_bind}: {e}"
        ))
    })?;
    if !loopback_only {
        let _ = sock.set_broadcast(true);
    }

    let targets = build_discovery_targets(bind_host, udp_port);
    let query = serde_json::to_vec(&PairDiscoveryQuery {
        kind: DISCOVERY_KIND_QUERY.to_string(),
        v: PAIR_PROTOCOL_VERSION,
        nameplate: code.nameplate,
    })
    .map_err(|e| GridError::Config(format!("cannot serialize discovery query: {e}")))?;

    let deadline = Instant::now() + discovery_timeout;
    let mut recv_buf = [0u8; MAX_PAIR_MESSAGE_BYTES];

    loop {
        for target in &targets {
            let _ = sock.send_to(&query, target).await;
        }

        let next_tick = Instant::now() + DISCOVERY_QUERY_INTERVAL;
        let wait_until = next_tick.min(deadline);

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(GridError::Config(format!(
                    "no inviter answered for pairing code nameplate {} on UDP port {udp_port} within {}s (pass --pair-host HOST:PORT if broadcasts are blocked)",
                    code.nameplate,
                    discovery_timeout.as_secs()
                )));
            }
            if now >= wait_until {
                break;
            }
            let remaining = wait_until - now;
            match tokio::time::timeout(remaining, sock.recv_from(&mut recv_buf)).await {
                Ok(Ok((n, src))) => {
                    if let Some(reply) =
                        serde_json::from_slice::<PairDiscoveryReply>(&recv_buf[..n])
                            .ok()
                            .filter(|r| {
                                r.kind == DISCOVERY_KIND_REPLY
                                    && r.v == PAIR_PROTOCOL_VERSION
                                    && r.nameplate == code.nameplate
                                    && r.port != 0
                            })
                    {
                        let peer_ip = normalize_ip(src.ip());
                        return Ok(SocketAddr::new(peer_ip, reply.port).to_string());
                    }
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
    }
}

fn build_discovery_targets(bind_host: IpAddr, udp_port: u16) -> Vec<SocketAddr> {
    if bind_host.is_loopback() {
        return vec![SocketAddr::new(bind_host, udp_port)];
    }
    let candidates = [
        IpAddr::V4(Ipv4Addr::BROADCAST),
        get_subnet_broadcast_ip(),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
    ];
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for ip in candidates {
        if seen.insert(ip) {
            out.push(SocketAddr::new(ip, udp_port));
        }
    }
    out
}

async fn handle_udp_discovery_packet(
    sock: &UdpSocket,
    packet: &[u8],
    peer: SocketAddr,
    nameplate: u16,
    tcp_port: u16,
) {
    let Ok(query) = serde_json::from_slice::<PairDiscoveryQuery>(packet) else {
        return;
    };
    if query.kind != DISCOVERY_KIND_QUERY
        || query.v != PAIR_PROTOCOL_VERSION
        || query.nameplate != nameplate
    {
        return;
    }
    if let Ok(reply) = serde_json::to_vec(&PairDiscoveryReply {
        kind: DISCOVERY_KIND_REPLY.to_string(),
        v: PAIR_PROTOCOL_VERSION,
        nameplate,
        port: tcp_port,
    }) {
        let _ = sock.send_to(&reply, peer).await;
    }
}

async fn revoke_invite_best_effort(
    connect_addr: &str,
    cluster_secret: Option<&ClusterSecret>,
    invite_id: &str,
) {
    let Ok(mut transport) = connect_master(connect_addr, cluster_secret).await else {
        return;
    };
    if transport
        .send_msg(&ClientMessage::RevokeInvite {
            invite_id: invite_id.to_string(),
        })
        .await
        .is_ok()
    {
        let _ = transport.recv_msg::<ClientResponse>().await;
    }
}

async fn find_pending_enrollment(
    connect_addr: &str,
    cluster_secret: Option<&ClusterSecret>,
    pending_id: &str,
    invite_id: &str,
    master_fp: &str,
) -> GridResult<PendingEnrollmentInfo> {
    let deadline = Instant::now() + PENDING_POLL_TIMEOUT;
    let mut transport = connect_master(connect_addr, cluster_secret).await?;

    loop {
        transport
            .send_msg(&ClientMessage::ListPendingEnrollments)
            .await?;
        match transport.recv_msg::<ClientResponse>().await? {
            Some(ClientResponse::PendingEnrollmentsList { pending }) => {
                if let Some(found) = pending.into_iter().find(|p| p.pending_id == pending_id) {
                    if found.invite_id != invite_id {
                        return Err(GridError::Config(format!(
                            "pending enrollment {pending_id} belongs to a different invite"
                        )));
                    }
                    if found.master_fingerprint != master_fp {
                        return Err(GridError::Config(
                            "pending enrollment master fingerprint mismatch".into(),
                        ));
                    }
                    return Ok(found);
                }
            }
            Some(ClientResponse::Error { message }) => {
                return Err(GridError::Config(message));
            }
            other => {
                return Err(GridError::Config(format!(
                    "unexpected response querying pending enrollments: {other:?}"
                )));
            }
        }

        if Instant::now() >= deadline {
            return Err(GridError::Config(format!(
                "pending enrollment {pending_id} did not appear on the master"
            )));
        }
        tokio::time::sleep(PENDING_POLL_INTERVAL).await;
    }
}

/// Runs `oxideswarm invite --code` (`JOIN-2`).
pub async fn run_invite_code(
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
    let had_credential =
        cluster_secret.is_some() || active_client_v3_auth().ok().flatten().is_some();

    let roles = match parse_roles_csv(&args.roles) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("--roles: {e}");
            return ExitCode::FAILURE;
        }
    };
    let requested_expires = match parse_duration_secs(&args.expires) {
        Ok(secs) => secs,
        Err(e) => {
            eprintln!("--expires: {e}");
            return ExitCode::FAILURE;
        }
    };
    let max_secs = PAIR_TOTAL_TIMEOUT.as_secs();
    let expires_in_secs = if requested_expires > max_secs {
        if args.expires.trim() != "24h" {
            eprintln!("Note: --expires capped to 10m ({max_secs}s) for --code pairing.");
        }
        max_secs
    } else {
        requested_expires
    };

    let (local_master, local_ticket) =
        match resolve_local_invite_targets(&args, config_file.as_ref()) {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };

    let bind_ip = match pair_bind_ip() {
        Ok(ip) => ip,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let udp_port = match pair_udp_port() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let udp_override_active = std::env::var(ENV_PAIR_UDP_PORT)
        .ok()
        .is_some_and(|v| !v.trim().is_empty());

    // Bind both pairing sockets before creating an invite on the master so port conflicts (e.g.
    // another active `invite --code` holding UDP 8087) never leave an orphaned invite behind.
    let udp_bind_addr = SocketAddr::new(bind_ip, udp_port);
    let udp_sock = match UdpSocket::bind(udp_bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "Cannot bind pairing UDP listener on {udp_bind_addr}: {e}. Only one `invite --code` can listen on UDP port {PAIR_UDP_PORT} at a time."
            );
            return ExitCode::FAILURE;
        }
    };
    let actual_udp_addr = match udp_sock.local_addr() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Cannot inspect pairing UDP listener address: {e}");
            return ExitCode::FAILURE;
        }
    };

    let tcp_bind_addr = SocketAddr::new(bind_ip, 0);
    let tcp_listener = match TcpListener::bind(tcp_bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Cannot bind pairing TCP listener on {tcp_bind_addr}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let tcp_addr = match tcp_listener.local_addr() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Cannot inspect pairing TCP listener address: {e}");
            return ExitCode::FAILURE;
        }
    };

    let code = match PairCode::generate() {
        Ok(c) => c,
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
                "Failed to create pairing invite",
                &e,
            )
        }
    };
    if let Err(e) = transport
        .send_msg(&ClientMessage::CreateInvite {
            roles: roles.clone(),
            max_uses: 1,
            expires_in_secs,
            require_approval: true,
        })
        .await
    {
        eprintln!("Failed to send CreateInvite request: {e}");
        return ExitCode::FAILURE;
    }

    let (invite_id, invite_secret, expires_at, master_key, p2p_ticket): (
        String,
        rusty_grid_core::noise_auth::RedactedBytes32,
        u64,
        PublicKey,
        Option<String>,
    ) = match transport.recv_msg::<ClientResponse>().await {
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
    drop(transport);

    let hint_host = if !tcp_addr.ip().is_unspecified() {
        normalize_ip(tcp_addr.ip())
    } else {
        rusty_grid_core::discovery::get_local_ip_or_loopback()
    };
    let pair_host_hint = SocketAddr::new(hint_host, tcp_addr.port());

    println!(
        "Invite {invite_id} ready (roles: {}, expires in {expires_in_secs}s).",
        format_roles(&roles)
    );
    println!("Pairing code: {code}");
    println!("On the new machine run: oxideswarm join --code {code}");
    if udp_override_active {
        println!(
            "Fallback if broadcasts are blocked: oxideswarm join --code {code} --pair-host {pair_host_hint}  (udp {actual_udp_addr})"
        );
    } else {
        println!(
            "Fallback if broadcasts are blocked: oxideswarm join --code {code} --pair-host {pair_host_hint}"
        );
    }
    let _ = std::io::stdout().flush();

    let total_deadline = Instant::now() + Duration::from_secs(expires_in_secs);
    let cli_master_flag = args.master.clone();
    let cfg_ref = config_file.as_ref();
    let mut udp_buf = [0u8; MAX_PAIR_MESSAGE_BYTES];

    let pending_notice = loop {
        let now = Instant::now();
        if now >= total_deadline {
            revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
            eprintln!("Pairing code expired after {expires_in_secs}s; invite revoked.");
            return ExitCode::FAILURE;
        }

        tokio::select! {
            ctrl_c = tokio::signal::ctrl_c() => {
                let _ = ctrl_c;
                revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
                eprintln!("Cancelled; invite revoked.");
                return ExitCode::FAILURE;
            }
            _ = tokio::time::sleep_until(total_deadline) => {
                revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
                eprintln!("Pairing code expired after {expires_in_secs}s; invite revoked.");
                return ExitCode::FAILURE;
            }
            recv_res = udp_sock.recv_from(&mut udp_buf) => {
                if let Ok((n, peer)) = recv_res {
                    handle_udp_discovery_packet(
                        &udp_sock,
                        &udp_buf[..n],
                        peer,
                        code.nameplate,
                        tcp_addr.port(),
                    ).await;
                }
            }
            accept_res = tcp_listener.accept() => {
                let Ok((mut stream, _peer_addr)) = accept_res else {
                    continue;
                };
                let now_after_accept = Instant::now();
                if now_after_accept >= total_deadline {
                    revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
                    eprintln!("Pairing code expired; invite revoked.");
                    return ExitCode::FAILURE;
                }
                let remaining_total = total_deadline - now_after_accept;
                let notice_timeout = PENDING_NOTICE_TIMEOUT.min(remaining_total);

                let lm = local_master.clone();
                let lt = local_ticket.clone();
                let pt = p2p_ticket.clone();
                let cm = cli_master_flag.clone();
                let iid = invite_id.clone();
                let isec = invite_secret;
                let mk = master_key;
                let rls = roles.clone();
                let ca = connect_addr.clone();

                let outcome = run_inviter_session_on_stream(
                    &mut stream,
                    &code,
                    PAIR_STEP_TIMEOUT,
                    notice_timeout,
                    move |local_ip| async move {
                        let (master, ticket) = resolve_pair_invite_targets(
                            cm.as_deref(),
                            lm,
                            lt,
                            pt,
                            cfg_ref,
                            &ca,
                            local_ip,
                        )?;
                        let token = InviteToken {
                            master,
                            ticket,
                            master_key: mk,
                            invite_id: iid,
                            invite_secret: isec,
                            expires_at,
                            roles: rls,
                            require_approval: true,
                        };
                        Ok(token.encode())
                    },
                )
                .await;

                match outcome {
                    InviterExchangeOutcome::StallOrMalformed(_reason) => {
                        // Single slot held for <= 5 s before `msg_i` was ever sent; keep waiting.
                        continue;
                    }
                    InviterExchangeOutcome::Burned => {
                        revoke_invite_best_effort(
                            &connect_addr,
                            cluster_secret.as_ref(),
                            &invite_id,
                        )
                        .await;
                        eprintln!("{PAIR_BURN_MESSAGE}");
                        return ExitCode::FAILURE;
                    }
                    InviterExchangeOutcome::Paired(notice) => break notice,
                }
            }
        }
    };

    drop(udp_sock);
    drop(tcp_listener);

    let master_fp = master_key.fingerprint();
    if pending_notice.master_fingerprint != master_fp {
        revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
        eprintln!("Joiner reported mismatched master fingerprint; invite revoked.");
        return ExitCode::FAILURE;
    }

    let pending_info = match find_pending_enrollment(
        &connect_addr,
        cluster_secret.as_ref(),
        &pending_notice.pending_id,
        &invite_id,
        &master_fp,
    )
    .await
    {
        Ok(info) => info,
        Err(e) => {
            revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
            eprintln!("Failed to verify pending enrollment on master: {e}");
            return ExitCode::FAILURE;
        }
    };

    let safe_name = sanitize_terminal_text(&pending_info.name);
    let safe_os = sanitize_terminal_text(&pending_info.os);
    let safe_arch = sanitize_terminal_text(&pending_info.arch);
    let safe_addr = sanitize_terminal_text(&pending_info.remote_addr);
    let pairing_check = format_pairing_check(&pending_notice.pairing_code);

    print!(
        "{safe_name} ({safe_os}/{safe_arch}, {safe_addr}) wants to join. Pairing check {pairing_check}. Approve? [y/N] "
    );
    let _ = std::io::stdout().flush();

    let (line_tx, line_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let res = std::io::stdin().read_line(&mut line).map(|n| (n, line));
        let _ = line_tx.send(res);
    });

    let approved_by_user = tokio::select! {
        res = line_rx => {
            match res {
                Ok(Ok((n, line))) if n > 0 => parse_yes_no_confirmation(&line),
                _ => false,
            }
        }
        _ = tokio::time::sleep_until(total_deadline) => {
            eprintln!("\nApproval prompt timed out.");
            false
        }
    };

    if !approved_by_user {
        if let Ok(mut t) = connect_master(&connect_addr, cluster_secret.as_ref()).await {
            let _ = t
                .send_msg(&ClientMessage::RejectEnrollment {
                    pending_id: pending_info.pending_id.clone(),
                })
                .await;
            let _ = t.recv_msg::<ClientResponse>().await;
        }
        revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
        eprintln!("Enrollment declined; invite revoked.");
        return ExitCode::FAILURE;
    }

    let clean_digits: String = pending_notice
        .pairing_code
        .trim()
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    let pairing_payload = format!("{clean_digits}@{master_fp}");

    let mut approve_transport = match connect_master(&connect_addr, cluster_secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
            return report_master_failure(
                &connect_addr,
                had_credential,
                "Failed to connect to approve enrollment",
                &e,
            );
        }
    };

    if let Err(e) = approve_transport
        .send_msg(&ClientMessage::ApproveEnrollment {
            pending_id: pending_info.pending_id.clone(),
            pairing_code: RedactedString(pairing_payload),
        })
        .await
    {
        eprintln!("Failed to send ApproveEnrollment request: {e}");
        return ExitCode::FAILURE;
    }

    match approve_transport.recv_msg::<ClientResponse>().await {
        Ok(Some(ClientResponse::EnrollmentApproved {
            member_id,
            name,
            roles,
            ..
        })) => {
            println!(
                "Approved {} ({}, roles: {}).",
                sanitize_terminal_text(&name),
                sanitize_terminal_text(&member_id),
                format_roles(&roles)
            );
            ExitCode::SUCCESS
        }
        Ok(Some(ClientResponse::Error { message })) => {
            revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
            eprintln!(
                "Enrollment approval failed: {}",
                sanitize_terminal_text(&message)
            );
            ExitCode::FAILURE
        }
        Ok(other) => {
            revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
            eprintln!("Unexpected response from Master: {other:?}");
            ExitCode::FAILURE
        }
        Err(e) => {
            revoke_invite_best_effort(&connect_addr, cluster_secret.as_ref(), &invite_id).await;
            eprintln!("Failed to read approval response from Master: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Runs `oxideswarm join --code <CODE> [--pair-host HOST:PORT]` (`JOIN-2`).
pub async fn run_join_code(
    args: JoinArgs,
    secret_opts: &SecretOpts,
    config: Option<&ConfigFile>,
) -> ExitCode {
    let raw_code = match args.code.as_deref() {
        Some(c) => c,
        None => {
            eprintln!("--code is required for short-code pairing");
            return ExitCode::FAILURE;
        }
    };
    let code = match PairCode::parse(raw_code) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let bind_host = match pair_bind_ip() {
        Ok(ip) => ip,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let udp_port = match pair_udp_port() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let tcp_target = match discover_inviter_tcp_addr(
        &code,
        args.pair_host.as_deref(),
        udp_port,
        bind_host,
        PAIR_DISCOVERY_TIMEOUT,
    )
    .await
    {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let stream =
        match tokio::time::timeout(PAIR_STEP_TIMEOUT, TcpStream::connect(&tcp_target)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                eprintln!("Cannot connect to pairing listener at {tcp_target}: {e}");
                return ExitCode::FAILURE;
            }
            Err(_) => {
                eprintln!("Timed out connecting to pairing listener at {tcp_target}");
                return ExitCode::FAILURE;
            }
        };

    let (invite_text, k_j2i, mut pair_stream) =
        match run_joiner_exchange_on_stream(stream, &code, PAIR_STEP_TIMEOUT).await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };

    let (notice_tx, notice_rx) = tokio::sync::oneshot::channel::<PairPendingNotice>();
    let notice_task = tokio::spawn(async move {
        if let Ok(notice) = notice_rx.await {
            if let Ok(json) = serde_json::to_vec(&notice) {
                if let Ok(sealed) = seal_aead(&k_j2i, 1, AAD_PENDING_V1, &json) {
                    let _ =
                        send_frame_with_timeout(&mut pair_stream, &sealed, PAIR_STEP_TIMEOUT).await;
                }
            }
        }
        let _ = pair_stream.shutdown().await;
    });

    let exit_code = run_join_invite_with_pair(
        args,
        secret_opts,
        config,
        invite_text.0,
        true,
        Some(notice_tx),
    )
    .await;

    let _ = tokio::time::timeout(Duration::from_millis(500), notice_task).await;
    exit_code
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::noise_auth::{MemberRole, NodeKeypair, RedactedBytes32};

    fn sample_invite_string() -> String {
        let mk = *NodeKeypair::generate().unwrap().public_key();
        InviteToken {
            master: Some("192.168.1.5:8088".into()),
            ticket: None,
            master_key: mk,
            invite_id: "0123456789abcdef0123456789abcdef".into(),
            invite_secret: RedactedBytes32([0x5a; 32]),
            expires_at: 4_102_444_800,
            roles: vec![MemberRole::Worker],
            require_approval: true,
        }
        .encode()
    }

    #[tokio::test]
    async fn spake2_and_aead_roundtrip_delivers_invite_and_pending_notice() {
        let code = PairCode::parse("42-rocket-violin").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let invite_str = sample_invite_string();
        let invite_clone = invite_str.clone();

        let inviter_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            run_inviter_session_on_stream(
                &mut stream,
                &code,
                Duration::from_secs(2),
                Duration::from_secs(2),
                move |_ip| async move { Ok(invite_clone) },
            )
            .await
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let (received_invite, k_j2i, mut pair_stream) =
            run_joiner_exchange_on_stream(stream, &code, Duration::from_secs(2))
                .await
                .unwrap();
        assert_eq!(received_invite.0, invite_str);

        let expected_notice = PairPendingNotice {
            pending_id: "pend-abc".into(),
            pairing_code: "418302".into(),
            master_fingerprint: "SHA256:testfp".into(),
        };
        let json = serde_json::to_vec(&expected_notice).unwrap();
        let sealed = seal_aead(&k_j2i, 1, AAD_PENDING_V1, &json).unwrap();
        send_frame_with_timeout(&mut pair_stream, &sealed, Duration::from_secs(2))
            .await
            .unwrap();

        match inviter_task.await.unwrap() {
            InviterExchangeOutcome::Paired(notice) => assert_eq!(notice, expected_notice),
            other => panic!("expected Paired, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn burn_rule_triggers_on_wrong_mac_and_never_sends_confirm_inviter() {
        let inviter_code = PairCode::parse("7-rocket-violin").unwrap();
        let wrong_code = PairCode::parse("7-abandon-zoo").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let inviter_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            run_inviter_session_on_stream(
                &mut stream,
                &inviter_code,
                Duration::from_secs(2),
                Duration::from_secs(2),
                move |_| async move { unreachable!("must not build invite on failed MAC") },
            )
            .await
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let err = run_joiner_exchange_on_stream(stream, &wrong_code, Duration::from_secs(2))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("rejected or closed"),
            "unexpected joiner error: {msg}"
        );

        assert!(matches!(
            inviter_task.await.unwrap(),
            InviterExchangeOutcome::Burned
        ));
    }

    #[tokio::test]
    async fn burn_rule_triggers_on_post_msg_i_timeout() {
        let code = PairCode::parse("15-castle-dolphin").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let inviter_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            run_inviter_session_on_stream(
                &mut stream,
                &code,
                Duration::from_millis(120),
                Duration::from_millis(120),
                move |_| async move { unreachable!() },
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let pw = Password::new(code.spake2_password());
        let (_state, msg_j) = Spake2::<Ed25519Group>::start_a(
            &pw,
            &Identity::new(SPAKE2_IDENTITY_JOINER),
            &Identity::new(SPAKE2_IDENTITY_INVITER),
        );
        send_frame_with_timeout(&mut client, &msg_j, Duration::from_secs(1))
            .await
            .unwrap();
        // Read `msg_i`, then intentionally stall without sending `confirm_joiner`.
        let _msg_i = recv_frame_with_timeout(&mut client, Duration::from_secs(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert!(matches!(
            inviter_task.await.unwrap(),
            InviterExchangeOutcome::Burned
        ));
    }

    #[tokio::test]
    async fn burn_rule_triggers_on_post_msg_i_closed_connection() {
        let code = PairCode::parse("19-galaxy-harbor").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let inviter_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            run_inviter_session_on_stream(
                &mut stream,
                &code,
                Duration::from_secs(2),
                Duration::from_secs(2),
                move |_| async move { unreachable!() },
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let pw = Password::new(code.spake2_password());
        let (_state, msg_j) = Spake2::<Ed25519Group>::start_a(
            &pw,
            &Identity::new(SPAKE2_IDENTITY_JOINER),
            &Identity::new(SPAKE2_IDENTITY_INVITER),
        );
        send_frame_with_timeout(&mut client, &msg_j, Duration::from_secs(1))
            .await
            .unwrap();
        let _msg_i = recv_frame_with_timeout(&mut client, Duration::from_secs(1))
            .await
            .unwrap();
        drop(client);

        assert!(matches!(
            inviter_task.await.unwrap(),
            InviterExchangeOutcome::Burned
        ));
    }

    #[tokio::test]
    async fn pre_msg_j_stall_does_not_burn_and_subsequent_valid_joiner_succeeds() {
        let code = PairCode::parse("88-lemon-anchor").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let invite_str = sample_invite_string();
        let invite_clone = invite_str.clone();

        let inviter_task = tokio::spawn(async move {
            // 1. Stalling client connects and never sends `msg_j`.
            let (mut s1, _) = listener.accept().await.unwrap();
            let first = run_inviter_session_on_stream(
                &mut s1,
                &code,
                Duration::from_millis(80),
                Duration::from_secs(1),
                move |_| async move { unreachable!() },
            )
            .await;
            assert!(
                matches!(first, InviterExchangeOutcome::StallOrMalformed(_)),
                "stall before msg_j must not burn: {first:?}"
            );
            drop(s1);

            // 2. Genuine joiner connects next on the same listener and succeeds.
            let (mut s2, _) = listener.accept().await.unwrap();
            run_inviter_session_on_stream(
                &mut s2,
                &code,
                Duration::from_secs(2),
                Duration::from_secs(2),
                move |_ip| async move { Ok(invite_clone) },
            )
            .await
        });

        // Client 1 stalls without sending anything.
        let _staller = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;

        // Client 2 completes genuine pairing.
        let s2 = TcpStream::connect(addr).await.unwrap();
        let (received, k_j2i, mut pair_stream) =
            run_joiner_exchange_on_stream(s2, &code, Duration::from_secs(2))
                .await
                .unwrap();
        assert_eq!(received.0, invite_str);

        let notice = PairPendingNotice {
            pending_id: "pend-after-stall".into(),
            pairing_code: "123456".into(),
            master_fingerprint: "SHA256:ok".into(),
        };
        let sealed = seal_aead(
            &k_j2i,
            1,
            AAD_PENDING_V1,
            &serde_json::to_vec(&notice).unwrap(),
        )
        .unwrap();
        send_frame_with_timeout(&mut pair_stream, &sealed, Duration::from_secs(2))
            .await
            .unwrap();

        match inviter_task.await.unwrap() {
            InviterExchangeOutcome::Paired(got) => assert_eq!(got, notice),
            other => panic!("expected Paired after harmless stall, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversized_frame_over_4096_bytes_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            recv_frame_with_timeout(&mut s, Duration::from_secs(1)).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let hdr = ((MAX_PAIR_MESSAGE_BYTES + 1) as u32).to_be_bytes();
        client.write_all(&hdr).await.unwrap();
        client.flush().await.unwrap();

        let res = server.await.unwrap();
        assert!(matches!(res, Err(FrameIoError::Oversized(4097))));
    }

    #[tokio::test]
    async fn udp_discovery_ignores_other_nameplate_and_resolves_matching_reply() {
        let inviter_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_port = inviter_udp.local_addr().unwrap().port();
        let expected_tcp_port = 45_123u16;
        let code = PairCode::parse("77-rocket-violin").unwrap();

        let responder = tokio::spawn(async move {
            let mut buf = [0u8; MAX_PAIR_MESSAGE_BYTES];
            loop {
                let Ok((n, peer)) = inviter_udp.recv_from(&mut buf).await else {
                    break;
                };
                if let Ok(q) = serde_json::from_slice::<PairDiscoveryQuery>(&buf[..n]) {
                    if q.nameplate == 77 {
                        // Send a bogus other-nameplate reply first, then the matching reply.
                        let wrong = serde_json::to_vec(&PairDiscoveryReply {
                            kind: DISCOVERY_KIND_REPLY.into(),
                            v: PAIR_PROTOCOL_VERSION,
                            nameplate: 99,
                            port: 11_111,
                        })
                        .unwrap();
                        let _ = inviter_udp.send_to(&wrong, peer).await;
                        handle_udp_discovery_packet(
                            &inviter_udp,
                            &buf[..n],
                            peer,
                            77,
                            expected_tcp_port,
                        )
                        .await;
                        break;
                    }
                }
            }
        });

        let resolved = discover_inviter_tcp_addr(
            &code,
            None,
            udp_port,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        responder.await.unwrap();
        assert_eq!(resolved, format!("127.0.0.1:{expected_tcp_port}"));
    }

    #[test]
    fn parse_pair_host_and_target_resolution_cases() {
        assert_eq!(
            parse_pair_host("192.168.1.5:51234").unwrap(),
            "192.168.1.5:51234"
        );
        assert_eq!(parse_pair_host("[::1]:51234").unwrap(), "[::1]:51234");
        assert_eq!(
            parse_pair_host("studio.local:51234").unwrap(),
            "studio.local:51234"
        );
        assert!(parse_pair_host("192.168.1.5").is_err());
        assert!(parse_pair_host("192.168.1.5:0").is_err());
        assert!(parse_pair_host("192.168.1.5:9000\nevil").is_err());

        // Loopback local master substituted with LAN TCP IP when joiner arrived over LAN.
        let (m, _) = resolve_pair_invite_targets(
            None,
            Some("127.0.0.1:18088".into()),
            None,
            None,
            None,
            "127.0.0.1:18088",
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 22)),
        )
        .unwrap();
        assert_eq!(m.as_deref(), Some("192.168.1.22:18088"));

        // Explicit LAN `--master` kept untouched even if local TCP IP differs.
        let (m2, _) = resolve_pair_invite_targets(
            Some("10.0.0.9:8088"),
            Some("10.0.0.9:8088".into()),
            None,
            None,
            None,
            "10.0.0.9:8088",
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 22)),
        )
        .unwrap();
        assert_eq!(m2.as_deref(), Some("10.0.0.9:8088"));

        assert_eq!(format_pairing_check("418302"), "418 302");
    }
}
