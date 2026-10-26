//! Shared-secret cluster authentication ("OXS-AUTH v1").
//!
//! Only holders of the cluster secret may drive or join the grid, and workers only obey masters
//! that prove they hold it. The module provides:
//!
//! * [`ClusterSecret`]: the secret itself (generation, hex/file persistence, resolution from
//!   flags/environment/default file, HMAC key derivation). It never prints or logs its value.
//! * [`client_handshake`] / [`server_handshake`]: a mutual challenge-response that runs on a
//!   [`MessageTransport`] before any `Register`/`ClientMessage` is exchanged, for TCP and for
//!   QUIC (iroh) streams alike.
//!
//! # Wire format
//!
//! Handshake frames use the normal 4-byte big-endian length-delimited framing. Their payload is
//! `[WIRE_FORMAT_AUTH = 0x03][kind][body]`:
//!
//! | kind | name      | body                                                              |
//! |------|-----------|-------------------------------------------------------------------|
//! | 0x01 | HELLO     | `version u8 (=1)`, `client_nonce[32]`                             |
//! | 0x02 | CHALLENGE | `server_nonce[32]`, `server_mac[32]`                              |
//! | 0x03 | PROOF     | `client_mac[32]`                                                  |
//! | 0x04 | OK        | empty                                                             |
//! | 0x05 | REJECT    | UTF-8 reason, at most 128 bytes, generic text only                |
//!
//! `server_mac = HMAC(secret, "oxs1-srv" || client_nonce || server_nonce)` and
//! `client_mac = HMAC(secret, "oxs1-cli" || server_nonce || client_nonce)`. Both MACs bind both
//! nonces and a direction label, so a MAC cannot be replayed on another connection or reflected
//! back in the other direction. The client verifies `server_mac` first and aborts without
//! revealing anything if it is wrong (the peer is not our master). All MAC comparisons go through
//! `ring::hmac::verify`, which is constant time.
//!
//! # What this does and does not give you
//!
//! The handshake authenticates both peers. It does **not** encrypt or integrity-protect the
//! traffic that follows: over plain TCP an on-path attacker can still read the stream or hijack
//! an already authenticated connection. Over untrusted networks use the P2P/QUIC transport
//! (iroh, already TLS) or tunnel TCP. Both handshake functions return a per-connection session
//! key (`HMAC(secret, "oxs1-key" || client_nonce || server_nonce)`) so an AEAD record layer can
//! be added later without changing the handshake; nothing uses it yet.
//!
//! # Caller duties
//!
//! * Wrap the handshake in the connection's handshake timeout (default 5 s); the functions
//!   themselves wait for the peer indefinitely.
//! * On `Err` from [`server_handshake`] log the peer address plus the error text at `warn`
//!   (the error text never contains MACs or the secret) and drop the connection.
//! * A server that has **no** secret cannot call [`server_handshake`]. If its first typed read
//!   fails with `ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)` the peer sent a HELLO, and
//!   the server should answer with `send_reject(t, REJECT_NO_SECRET)` and close.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{GridError, GridResult};
use crate::protocol::{MessageTransport, ProtocolError};

pub use crate::protocol::WIRE_FORMAT_AUTH;

/// Handshake protocol version carried in HELLO.
pub const AUTH_VERSION: u8 = 1;
/// Minimum accepted secret length in bytes (32 hex characters).
pub const MIN_SECRET_BYTES: usize = 16;
/// Length in bytes of secrets produced by [`ClusterSecret::generate`].
pub const GENERATED_SECRET_BYTES: usize = 32;
/// Maximum accepted secret length in bytes, to bound what a malformed file can make us hold.
pub const MAX_SECRET_BYTES: usize = 256;
/// Length of the random nonces exchanged in the handshake.
pub const NONCE_LEN: usize = 32;
/// Length of an HMAC-SHA256 tag.
pub const MAC_LEN: usize = 32;
/// Largest frame accepted from a peer that has not authenticated yet. Auth frames are < 100 bytes.
pub const PRE_AUTH_MAX_FRAME: usize = 1024;
/// Longest REJECT reason that is sent or accepted.
pub const MAX_REJECT_REASON: usize = 128;
/// Pause before a failed server-side handshake is answered, to blunt online brute force.
/// It is applied inside [`server_handshake`] (before the REJECT frame is sent), so callers
/// that run one task per connection need no extra delay.
pub const FAILURE_DELAY: Duration = Duration::from_millis(250);
/// How long a best-effort REJECT write may take before it is abandoned.
const REJECT_SEND_TIMEOUT: Duration = Duration::from_secs(1);

/// Environment variable holding the secret as hex.
pub const ENV_SECRET: &str = "OXIDESWARM_SECRET";
/// Environment variable holding the path of a secret file.
pub const ENV_SECRET_FILE: &str = "OXIDESWARM_SECRET_FILE";

/// Generic REJECT reasons. Peers only ever see these exact strings.
pub const REJECT_AUTH_FAILED: &str = "authentication failed";
pub const REJECT_AUTH_REQUIRED: &str = "authentication required";
pub const REJECT_NO_SECRET: &str = "server has no secret configured";
pub const REJECT_UNSUPPORTED_VERSION: &str = "unsupported auth version";

/// Actionable text of [`GridError::AuthRequired`] when a master demands the secret.
pub const AUTH_REQUIRED_HINT: &str = "master requires the cluster secret; copy ~/.oxideswarm/cluster.secret from the master or set OXIDESWARM_SECRET";

const KIND_HELLO: u8 = 0x01;
const KIND_CHALLENGE: u8 = 0x02;
const KIND_PROOF: u8 = 0x03;
const KIND_OK: u8 = 0x04;
const KIND_REJECT: u8 = 0x05;

const LABEL_SERVER: &[u8] = b"oxs1-srv";
const LABEL_CLIENT: &[u8] = b"oxs1-cli";
const LABEL_KEY: &[u8] = b"oxs1-key";

/// File name of the default secret file inside the OxideSwarm home directory.
const DEFAULT_SECRET_FILE_NAME: &str = "cluster.secret";

struct SecretInner {
    bytes: Vec<u8>,
    key: hmac::Key,
}

/// Shared cluster secret (at least 16 bytes). Cheap to clone; never prints its value.
#[derive(Clone)]
pub struct ClusterSecret(Arc<SecretInner>);

impl std::fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterSecret(<redacted>)")
    }
}

impl PartialEq for ClusterSecret {
    fn eq(&self, other: &Self) -> bool {
        // Equal secrets MAC a fixed label identically; the comparison itself is `hmac::verify`.
        let theirs = other.mac(&[b"oxs1-eq"]);
        self.verify_mac(&[b"oxs1-eq"], &theirs)
    }
}

impl Eq for ClusterSecret {}

impl ClusterSecret {
    /// Builds a secret from raw bytes (16..=256 bytes).
    pub fn from_bytes(bytes: &[u8]) -> GridResult<Self> {
        if bytes.len() < MIN_SECRET_BYTES {
            return Err(GridError::Config(format!(
                "cluster secret is too short: need at least {MIN_SECRET_BYTES} bytes ({} hex characters)",
                MIN_SECRET_BYTES * 2
            )));
        }
        if bytes.len() > MAX_SECRET_BYTES {
            return Err(GridError::Config(format!(
                "cluster secret is too long: at most {MAX_SECRET_BYTES} bytes are supported"
            )));
        }
        Ok(Self(Arc::new(SecretInner {
            bytes: bytes.to_vec(),
            key: hmac::Key::new(hmac::HMAC_SHA256, bytes),
        })))
    }

    /// Generates a fresh 32-byte secret from the operating system CSPRNG.
    pub fn generate() -> GridResult<Self> {
        let mut bytes = [0u8; GENERATED_SECRET_BYTES];
        random_bytes(&mut bytes)?;
        Self::from_bytes(&bytes)
    }

    /// Parses the hex text form (surrounding whitespace is ignored, case-insensitive).
    /// Error messages never echo the input.
    pub fn from_hex(text: &str) -> GridResult<Self> {
        let text = text.trim();
        let bytes = hex_decode(text).ok_or_else(|| {
            GridError::Config(
                "cluster secret must be an even number of hexadecimal characters".to_string(),
            )
        })?;
        Self::from_bytes(&bytes)
    }

    /// Lowercase hex text form, as stored in secret files and exported to other machines.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0.bytes)
    }

    /// Reads a secret file: the first non-empty line that is not a `#` comment, as hex.
    ///
    /// On Unix a warning is logged when the file is readable by group or others.
    pub fn load_file(path: &Path) -> GridResult<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            GridError::Config(format!(
                "cannot read cluster secret file '{}': {e}",
                path.display()
            ))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                if meta.permissions().mode() & 0o077 != 0 {
                    tracing::warn!(
                        path = %path.display(),
                        "cluster secret file is accessible by other users; run `chmod 600` on it"
                    );
                }
            }
        }
        let line = text
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .ok_or_else(|| {
                GridError::Config(format!("cluster secret file '{}' is empty", path.display()))
            })?;
        Self::from_hex(line).map_err(|e| {
            GridError::Config(format!(
                "cluster secret file '{}' is invalid: {}",
                path.display(),
                config_detail(&e)
            ))
        })
    }

    /// Writes the secret to `path` as a single hex line, creating missing parent directories.
    ///
    /// The file is created with mode 0600 on Unix (best effort on Windows, where it inherits
    /// the ACL of its directory). With `overwrite == false` an existing file is never touched
    /// and an error is returned; with `true` it is replaced atomically via a private temp file.
    pub fn save_file(&self, path: &Path, overwrite: bool) -> GridResult<()> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            let existed = parent.exists();
            std::fs::create_dir_all(parent).map_err(|e| {
                GridError::Config(format!(
                    "cannot create directory '{}': {e}",
                    parent.display()
                ))
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if !existed {
                    let _ =
                        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
                }
            }
            #[cfg(not(unix))]
            let _ = existed;
        }

        let line = format!("{}\n", self.to_hex());
        if !overwrite {
            return write_private_new(path, line.as_bytes()).map_err(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    GridError::Config(format!(
                        "refusing to overwrite existing file '{}' (use --force to replace it)",
                        path.display()
                    ))
                } else {
                    GridError::Config(format!(
                        "cannot write cluster secret file '{}': {e}",
                        path.display()
                    ))
                }
            });
        }

        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| DEFAULT_SECRET_FILE_NAME.to_string());
        let tmp = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        write_private_new(&tmp, line.as_bytes())
            .and_then(|()| std::fs::rename(&tmp, path))
            .map_err(|e| {
                let _ = std::fs::remove_file(&tmp);
                GridError::Config(format!(
                    "cannot write cluster secret file '{}': {e}",
                    path.display()
                ))
            })
    }

    /// Loads the secret stored at `path`, generating and saving a new one when the file does
    /// not exist. Returns the secret and whether it was created by this call.
    pub fn load_or_generate_file(path: &Path) -> GridResult<(Self, bool)> {
        if path.exists() {
            return Self::load_file(path).map(|s| (s, false));
        }
        let secret = Self::generate()?;
        match secret.save_file(path, false) {
            Ok(()) => Ok((secret, true)),
            // Another process created it between the check and the write: use theirs.
            Err(_) if path.exists() => Self::load_file(path).map(|s| (s, false)),
            Err(e) => Err(e),
        }
    }

    /// Default secret file: `<home>/.oxideswarm/cluster.secret` (home is `HOME`, or
    /// `USERPROFILE` on Windows).
    pub fn default_path() -> Option<PathBuf> {
        if let Some(custom) = std::env::var_os("OXIDESWARM_HOME").filter(|s| !s.is_empty()) {
            return Some(PathBuf::from(custom).join(DEFAULT_SECRET_FILE_NAME));
        }
        #[cfg(windows)]
        let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"));
        #[cfg(not(windows))]
        let home = std::env::var_os("HOME");
        home.filter(|h| !h.is_empty()).map(|h| {
            PathBuf::from(h)
                .join(".oxideswarm")
                .join(DEFAULT_SECRET_FILE_NAME)
        })
    }

    /// Resolves the secret for a CLI process. Precedence: explicit value, explicit file,
    /// `OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, the default file if it exists, else `None`.
    ///
    /// A source that is present but invalid is an error, never a silent fall-through to the next
    /// source (that would quietly turn an authenticated cluster into an open one). That includes
    /// a *blank* `--secret`, `OXIDESWARM_SECRET` or `OXIDESWARM_SECRET_FILE`: an empty shell
    /// expansion (`--secret "$MISSING"`, a vault that injected nothing) must not silently switch
    /// authentication off.
    pub fn resolve(
        explicit_value: Option<&str>,
        explicit_file: Option<&Path>,
    ) -> GridResult<Option<Self>> {
        Self::resolve_with(
            explicit_value,
            explicit_file,
            // `var_os` + lossy: a non-Unicode value is still "present" and then fails as invalid
            // hex instead of reading as "not set".
            |name| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned()),
            Self::default_path(),
        )
    }

    /// [`ClusterSecret::resolve`] with the environment and the default file location injected.
    pub fn resolve_with(
        explicit_value: Option<&str>,
        explicit_file: Option<&Path>,
        env: impl Fn(&str) -> Option<String>,
        default_file: Option<PathBuf>,
    ) -> GridResult<Option<Self>> {
        // A source that is given but blank is a configuration mistake, not "no secret".
        let blank = |source: &str| {
            GridError::Config(format!(
                "{source} is set but empty; give a valid cluster secret or remove it (an empty value never turns cluster authentication off)"
            ))
        };

        if let Some(value) = explicit_value {
            if value.trim().is_empty() {
                return Err(blank("--secret"));
            }
            return Self::from_hex(value)
                .map(Some)
                .map_err(|e| GridError::Config(format!("--secret: {}", config_detail(&e))));
        }
        if let Some(path) = explicit_file {
            if path.as_os_str().is_empty() {
                return Err(blank("--secret-file"));
            }
            return Self::load_file(path).map(Some);
        }
        if let Some(value) = env(ENV_SECRET) {
            if value.trim().is_empty() {
                return Err(blank(ENV_SECRET));
            }
            return Self::from_hex(&value)
                .map(Some)
                .map_err(|e| GridError::Config(format!("{ENV_SECRET}: {}", config_detail(&e))));
        }
        if let Some(path) = env(ENV_SECRET_FILE) {
            if path.trim().is_empty() {
                return Err(blank(ENV_SECRET_FILE));
            }
            return Self::load_file(Path::new(path.trim())).map(Some);
        }
        if let Some(path) = default_file {
            if path.is_file() {
                return Self::load_file(&path).map(Some);
            }
        }
        Ok(None)
    }

    /// Derives a 32-byte sub-secret: `HMAC-SHA256(secret, label)`. Used for e.g. the dashboard
    /// admin token so the raw secret never leaves the cluster-protocol layer.
    pub fn derive(&self, label: &str) -> [u8; MAC_LEN] {
        self.mac(&[label.as_bytes()])
    }

    /// [`ClusterSecret::derive`] rendered as lowercase hex.
    pub fn derive_hex(&self, label: &str) -> String {
        hex_encode(&self.derive(label))
    }

    /// HMAC-SHA256 over the concatenation of `parts`.
    pub(crate) fn mac(&self, parts: &[&[u8]]) -> [u8; MAC_LEN] {
        let tag = hmac::sign(&self.0.key, &concat(parts));
        let mut out = [0u8; MAC_LEN];
        out.copy_from_slice(tag.as_ref());
        out
    }

    /// Constant-time check (via `ring::hmac::verify`) that `tag` is the HMAC of `parts`.
    /// A tag of the wrong length simply fails.
    pub(crate) fn verify_mac(&self, parts: &[&[u8]], tag: &[u8]) -> bool {
        hmac::verify(&self.0.key, &concat(parts), tag).is_ok()
    }
}

fn concat(parts: &[&[u8]]) -> Vec<u8> {
    let mut data = Vec::with_capacity(parts.iter().map(|p| p.len()).sum());
    for part in parts {
        data.extend_from_slice(part);
    }
    data
}

/// Message of a `GridError::Config`, without the "Configuration error:" prefix.
fn config_detail(e: &GridError) -> String {
    match e {
        GridError::Config(msg) => msg.clone(),
        other => other.to_string(),
    }
}

/// Creates `path` exclusively (mode 0600 on Unix) and writes `data` to it.
fn write_private_new(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    file.write_all(data)?;
    file.sync_all()
}

/// Fills `buf` from the operating system CSPRNG.
pub(crate) fn random_bytes(buf: &mut [u8]) -> GridResult<()> {
    SystemRandom::new()
        .fill(buf)
        .map_err(|_| GridError::Io(io::Error::other("system random number generator failure")))
}

fn random_nonce() -> GridResult<[u8; NONCE_LEN]> {
    let mut nonce = [0u8; NONCE_LEN];
    random_bytes(&mut nonce)?;
    Ok(nonce)
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

#[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
pub(crate) fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let raw = text.as_bytes();
    if raw.len() & 1 == 1 {
        return None;
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    raw.chunks_exact(2)
        .map(|pair| Some((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------------------------

fn auth_frame(kind: u8, parts: &[&[u8]]) -> Bytes {
    let mut buf = Vec::with_capacity(2 + parts.iter().map(|p| p.len()).sum::<usize>());
    buf.push(WIRE_FORMAT_AUTH);
    buf.push(kind);
    for part in parts {
        buf.extend_from_slice(part);
    }
    Bytes::from(buf)
}

/// Splits `[0x03][kind][body]` into `(kind, body)`.
fn split_auth_frame(frame: &[u8]) -> Option<(u8, &[u8])> {
    match frame {
        [WIRE_FORMAT_AUTH, kind, body @ ..] => Some((*kind, body)),
        _ => None,
    }
}

/// Reads one pre-authentication frame (capped at [`PRE_AUTH_MAX_FRAME`]).
async fn recv_auth_frame<T>(t: &mut MessageTransport<T>) -> Result<Option<Bytes>, ProtocolError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    t.recv_raw_frame_max(PRE_AUTH_MAX_FRAME).await
}

/// Sends a REJECT frame carrying `reason` (truncated to [`MAX_REJECT_REASON`] bytes).
///
/// Used by [`server_handshake`] and by servers without a secret that receive a HELLO.
pub async fn send_reject<T>(t: &mut MessageTransport<T>, reason: &str) -> GridResult<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut end = reason.len().min(MAX_REJECT_REASON);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    t.send_raw_frame(auth_frame(KIND_REJECT, &[&reason.as_bytes()[..end]]))
        .await?;
    Ok(())
}

/// Fails a server-side handshake: waits [`FAILURE_DELAY`], sends a generic REJECT best effort,
/// and returns the internal `detail` as an [`GridError::AuthFailed`] for the caller to log.
async fn reject_peer<T>(
    t: &mut MessageTransport<T>,
    reason: &'static str,
    detail: impl Into<String>,
) -> GridError
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let detail = detail.into();
    tracing::debug!(reason, detail = %detail, "rejecting unauthenticated peer");
    tokio::time::sleep(FAILURE_DELAY).await;
    let _ = tokio::time::timeout(REJECT_SEND_TIMEOUT, send_reject(t, reason)).await;
    GridError::AuthFailed(detail)
}

/// Turns a REJECT body from a peer into an error, sanitising the untrusted text.
fn error_from_reject(body: &[u8]) -> GridError {
    let body = &body[..body.len().min(MAX_REJECT_REASON)];
    let reason: String = String::from_utf8_lossy(body)
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .collect();
    match reason.as_str() {
        REJECT_AUTH_REQUIRED => GridError::AuthRequired(AUTH_REQUIRED_HINT.to_string()),
        REJECT_NO_SECRET => GridError::AuthFailed(
            "the master has no cluster secret configured but this node does; start the master with \
             the same secret"
                .to_string(),
        ),
        REJECT_AUTH_FAILED => GridError::AuthFailed(
            "the master rejected the cluster secret (authentication failed); check that both sides \
             use the same secret"
                .to_string(),
        ),
        REJECT_UNSUPPORTED_VERSION => GridError::AuthFailed(
            "the master does not support this authentication protocol version".to_string(),
        ),
        other => GridError::AuthFailed(format!("the master rejected authentication: {other}")),
    }
}

/// Maps a transport error seen while reading a handshake frame.
fn handshake_read_error(e: ProtocolError) -> GridError {
    match e {
        ProtocolError::FrameTooLarge { max, .. } => GridError::AuthFailed(format!(
            "peer sent a handshake frame larger than {max} bytes"
        )),
        other => other.into(),
    }
}

/// Runs the client side of the handshake. On success returns the session key.
///
/// Errors: `AuthFailed` when the master is not ours (wrong `server_mac`) or rejects us,
/// `AuthRequired` when the master says authentication is required, `HandshakeFailed` when the
/// connection is closed before the master answers.
pub async fn client_handshake<T>(
    t: &mut MessageTransport<T>,
    secret: &ClusterSecret,
) -> GridResult<[u8; MAC_LEN]>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let client_nonce = random_nonce()?;
    t.send_raw_frame(auth_frame(KIND_HELLO, &[&[AUTH_VERSION], &client_nonce]))
        .await?;

    let frame = recv_auth_frame(t)
        .await
        .map_err(handshake_read_error)?
        .ok_or_else(|| {
            GridError::HandshakeFailed(
                "master closed the connection during cluster authentication (it may not be \
                 configured with a cluster secret)"
                    .to_string(),
            )
        })?;
    let (kind, body) = split_auth_frame(&frame).ok_or_else(|| {
        GridError::AuthFailed(
            "peer answered with a non-authentication frame; it does not speak the cluster \
             authentication protocol"
                .to_string(),
        )
    })?;
    match kind {
        KIND_CHALLENGE => {}
        KIND_REJECT => return Err(error_from_reject(body)),
        _ => {
            return Err(GridError::AuthFailed(
                "unexpected frame during cluster authentication".to_string(),
            ))
        }
    }
    if body.len() != NONCE_LEN + MAC_LEN {
        return Err(GridError::AuthFailed(
            "malformed authentication challenge".to_string(),
        ));
    }
    let (server_nonce, server_mac) = body.split_at(NONCE_LEN);

    // The master must prove it holds the secret before we reveal anything about ours.
    if !secret.verify_mac(&[LABEL_SERVER, &client_nonce, server_nonce], server_mac) {
        return Err(GridError::AuthFailed(
            "the master did not prove knowledge of the cluster secret (wrong secret, or this is \
             not our master); refusing to continue"
                .to_string(),
        ));
    }

    let client_mac = secret.mac(&[LABEL_CLIENT, server_nonce, &client_nonce]);
    t.send_raw_frame(auth_frame(KIND_PROOF, &[&client_mac]))
        .await?;

    let frame = recv_auth_frame(t)
        .await
        .map_err(handshake_read_error)?
        .ok_or_else(|| {
            GridError::AuthFailed(
                "master closed the connection after the proof; the cluster secret was probably \
                 rejected"
                    .to_string(),
            )
        })?;
    match split_auth_frame(&frame) {
        Some((KIND_OK, [])) => Ok(secret.mac(&[LABEL_KEY, &client_nonce, server_nonce])),
        Some((KIND_REJECT, body)) => Err(error_from_reject(body)),
        _ => Err(GridError::AuthFailed(
            "unexpected frame during cluster authentication".to_string(),
        )),
    }
}

/// Runs the server side of the handshake. On success returns the session key.
///
/// The first frame must be a HELLO of version 1. Every failure waits [`FAILURE_DELAY`], sends a
/// generic REJECT ("authentication failed", "authentication required", "unsupported auth
/// version") and returns `Err(GridError::AuthFailed(detail))`; `detail` is for the caller's
/// `warn` log and never contains MACs or secret material. If the peer disappears the plain
/// `ConnectionClosed`/`Io` error is returned without a REJECT. Frames are limited to
/// [`PRE_AUTH_MAX_FRAME`] bytes until this returns `Ok`; the transport's normal limit is in force
/// again afterwards.
pub async fn server_handshake<T>(
    t: &mut MessageTransport<T>,
    secret: &ClusterSecret,
) -> GridResult<[u8; MAC_LEN]>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let hello = match recv_auth_frame(t).await {
        Ok(Some(frame)) => frame,
        Ok(None) => return Err(GridError::ConnectionClosed),
        Err(ProtocolError::FrameTooLarge { max, .. }) => {
            return Err(reject_peer(
                t,
                REJECT_AUTH_REQUIRED,
                format!("first frame exceeds the {max} byte pre-authentication limit"),
            )
            .await)
        }
        Err(e) => return Err(e.into()),
    };

    let body = match split_auth_frame(&hello) {
        Some((KIND_HELLO, body)) => body,
        _ => {
            return Err(reject_peer(
                t,
                REJECT_AUTH_REQUIRED,
                "first frame was not an authentication HELLO",
            )
            .await)
        }
    };
    match body.first() {
        Some(&AUTH_VERSION) => {}
        Some(version) => {
            return Err(reject_peer(
                t,
                REJECT_UNSUPPORTED_VERSION,
                format!("peer requested unsupported auth version {version}"),
            )
            .await)
        }
        None => return Err(reject_peer(t, REJECT_AUTH_FAILED, "empty HELLO").await),
    }
    if body.len() != 1 + NONCE_LEN {
        return Err(reject_peer(t, REJECT_AUTH_FAILED, "malformed HELLO").await);
    }
    let client_nonce = &body[1..];

    let server_nonce = random_nonce()?;
    let server_mac = secret.mac(&[LABEL_SERVER, client_nonce, &server_nonce]);
    t.send_raw_frame(auth_frame(KIND_CHALLENGE, &[&server_nonce, &server_mac]))
        .await?;

    let proof = match recv_auth_frame(t).await {
        Ok(Some(frame)) => frame,
        Ok(None) => return Err(GridError::ConnectionClosed),
        Err(ProtocolError::FrameTooLarge { max, .. }) => {
            return Err(reject_peer(
                t,
                REJECT_AUTH_FAILED,
                format!("proof frame exceeds the {max} byte pre-authentication limit"),
            )
            .await)
        }
        Err(e) => return Err(e.into()),
    };
    let client_mac = match split_auth_frame(&proof) {
        Some((KIND_PROOF, mac)) if mac.len() == MAC_LEN => mac,
        _ => return Err(reject_peer(t, REJECT_AUTH_FAILED, "malformed PROOF").await),
    };
    if !secret.verify_mac(&[LABEL_CLIENT, &server_nonce, client_nonce], client_mac) {
        return Err(reject_peer(
            t,
            REJECT_AUTH_FAILED,
            "invalid proof (the peer does not hold the cluster secret)",
        )
        .await);
    }

    t.send_raw_frame(auth_frame(KIND_OK, &[])).await?;
    Ok(secret.mac(&[LABEL_KEY, client_nonce, &server_nonce]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{MasterMessage, WorkerMessage};
    use tokio::io::DuplexStream;
    use uuid::Uuid;

    type Duplex = MessageTransport<DuplexStream>;

    fn pair() -> (Duplex, Duplex) {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (MessageTransport::new(a), MessageTransport::new(b))
    }

    fn secret(seed: u8) -> ClusterSecret {
        ClusterSecret::from_bytes(&[seed; 32]).unwrap()
    }

    fn hello(version: u8, nonce: &[u8]) -> Bytes {
        auth_frame(KIND_HELLO, &[&[version], nonce])
    }

    /// Reads the REJECT reason the peer sent, panicking on anything else.
    async fn expect_reject(t: &mut Duplex) -> String {
        let frame = t.recv_raw_frame().await.unwrap().expect("a REJECT frame");
        match split_auth_frame(&frame) {
            Some((KIND_REJECT, reason)) => String::from_utf8(reason.to_vec()).unwrap(),
            other => panic!("expected REJECT, got {other:?}"),
        }
    }

    fn heartbeat() -> WorkerMessage {
        WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 7,
            active_tasks: 0,
            cpu_usage_pct: 1.0,
            ram_available_mb: 2,
        }
    }

    // --- ClusterSecret ---------------------------------------------------------------------

    #[test]
    fn secret_hex_roundtrip_and_validation() {
        let s = ClusterSecret::generate().unwrap();
        let hex = s.to_hex();
        assert_eq!(hex.len(), GENERATED_SECRET_BYTES * 2);
        assert!(hex
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_eq!(ClusterSecret::from_hex(&hex).unwrap(), s);
        // Whitespace/newlines are trimmed and upper case is accepted.
        assert_eq!(
            ClusterSecret::from_hex(&format!("  {}\n", hex.to_uppercase())).unwrap(),
            s
        );

        assert!(ClusterSecret::from_hex("").is_err());
        assert!(ClusterSecret::from_hex("abcd").is_err(), "too short");
        assert!(
            ClusterSecret::from_hex(&"a".repeat(31)).is_err(),
            "odd length"
        );
        assert!(
            ClusterSecret::from_hex(&"zz".repeat(16)).is_err(),
            "not hex"
        );
        assert!(ClusterSecret::from_hex(&"ab".repeat(MAX_SECRET_BYTES + 1)).is_err());
        assert!(ClusterSecret::from_hex(&"ab".repeat(MIN_SECRET_BYTES)).is_ok());
        assert!(ClusterSecret::from_bytes(&[1u8; MIN_SECRET_BYTES - 1]).is_err());

        // Error text must not leak the rejected input.
        let err = ClusterSecret::from_hex("not-the-secret-but-looks-like-one!!").unwrap_err();
        assert!(!err.to_string().contains("not-the-secret"));
    }

    #[test]
    fn secret_debug_is_redacted_and_generation_is_random() {
        let s = ClusterSecret::generate().unwrap();
        let dbg = format!("{s:?}");
        assert_eq!(dbg, "ClusterSecret(<redacted>)");
        assert!(!dbg.contains(&s.to_hex()));
        assert_ne!(s, ClusterSecret::generate().unwrap());
    }

    #[test]
    fn derive_is_hmac_of_label_and_label_separated() {
        let s = secret(9);
        let key = hmac::Key::new(hmac::HMAC_SHA256, &[9u8; 32]);
        let expected = hmac::sign(&key, b"oxs1-dashboard-admin");
        assert_eq!(
            s.derive("oxs1-dashboard-admin").as_slice(),
            expected.as_ref()
        );
        assert_eq!(
            s.derive_hex("oxs1-dashboard-admin"),
            hex_encode(expected.as_ref())
        );
        assert_ne!(s.derive("a"), s.derive("b"));
        assert_ne!(s.derive("a"), secret(8).derive("a"));
    }

    #[test]
    fn secret_file_roundtrip_permissions_and_overwrite_rules() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("cluster.secret");
        let s = ClusterSecret::generate().unwrap();

        s.save_file(&path, false).unwrap();
        assert_eq!(ClusterSecret::load_file(&path).unwrap(), s);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            format!("{}\n", s.to_hex())
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "secret file must be private");
        }

        // Never overwrite without being told to.
        let other = ClusterSecret::generate().unwrap();
        let err = other.save_file(&path, false).unwrap_err();
        assert!(err.to_string().contains("refusing to overwrite"));
        assert_eq!(ClusterSecret::load_file(&path).unwrap(), s);

        // Forced overwrite replaces it, stays private and leaves no temp file behind.
        other.save_file(&path, true).unwrap();
        assert_eq!(ClusterSecret::load_file(&path).unwrap(), other);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers.len(), 1, "unexpected files: {leftovers:?}");
    }

    #[test]
    fn secret_file_parsing_and_load_or_generate() {
        let dir = tempfile::tempdir().unwrap();
        let s = ClusterSecret::generate().unwrap();

        let commented = dir.path().join("commented.secret");
        std::fs::write(
            &commented,
            format!("# cluster secret\n\n  {}  \n", s.to_hex()),
        )
        .unwrap();
        assert_eq!(ClusterSecret::load_file(&commented).unwrap(), s);

        let empty = dir.path().join("empty.secret");
        std::fs::write(&empty, "\n# only a comment\n").unwrap();
        assert!(ClusterSecret::load_file(&empty).is_err());

        let garbage = dir.path().join("garbage.secret");
        std::fs::write(&garbage, "this is definitely not hex\n").unwrap();
        let err = ClusterSecret::load_file(&garbage).unwrap_err().to_string();
        assert!(err.contains("garbage.secret") && !err.contains("definitely"));

        assert!(ClusterSecret::load_file(&dir.path().join("missing")).is_err());

        let auto = dir.path().join("auto").join("cluster.secret");
        let (created, was_created) = ClusterSecret::load_or_generate_file(&auto).unwrap();
        assert!(was_created);
        let (loaded, was_created) = ClusterSecret::load_or_generate_file(&auto).unwrap();
        assert!(!was_created);
        assert_eq!(created, loaded);
    }

    #[test]
    fn resolve_precedence() {
        let dir = tempfile::tempdir().unwrap();
        let explicit = secret(1);
        let from_file = secret(2);
        let from_env = secret(3);
        let from_env_file = secret(4);
        let from_default = secret(5);

        let explicit_file = dir.path().join("explicit");
        from_file.save_file(&explicit_file, false).unwrap();
        let env_file = dir.path().join("env");
        from_env_file.save_file(&env_file, false).unwrap();
        let default_file = dir.path().join("default");
        from_default.save_file(&default_file, false).unwrap();

        let env_hex = from_env.to_hex();
        let env_file_str = env_file.to_string_lossy().into_owned();
        let env = move |name: &str| match name {
            ENV_SECRET => Some(env_hex.clone()),
            ENV_SECRET_FILE => Some(env_file_str.clone()),
            _ => None,
        };
        let no_env = |_: &str| None;
        let hex = explicit.to_hex();
        let resolve =
            |v: Option<&str>,
             f: Option<&Path>,
             e: &dyn Fn(&str) -> Option<String>,
             d: Option<PathBuf>| { ClusterSecret::resolve_with(v, f, e, d).unwrap() };

        // explicit value > explicit file > env value > env file > default file > None
        assert_eq!(
            resolve(
                Some(&hex),
                Some(&explicit_file),
                &env,
                Some(default_file.clone())
            ),
            Some(explicit.clone())
        );
        assert_eq!(
            resolve(None, Some(&explicit_file), &env, Some(default_file.clone())),
            Some(from_file.clone())
        );
        assert_eq!(
            resolve(None, None, &env, Some(default_file.clone())),
            Some(from_env.clone())
        );
        let only_env_file = {
            let p = env_file.to_string_lossy().into_owned();
            move |name: &str| (name == ENV_SECRET_FILE).then(|| p.clone())
        };
        assert_eq!(
            resolve(None, None, &only_env_file, Some(default_file.clone())),
            Some(from_env_file)
        );
        assert_eq!(
            resolve(None, None, &no_env, Some(default_file.clone())),
            Some(from_default)
        );
        assert_eq!(
            resolve(None, None, &no_env, Some(dir.path().join("absent"))),
            None
        );
        assert_eq!(resolve(None, None, &no_env, None), None);
    }

    #[test]
    fn resolve_treats_a_blank_source_as_a_configuration_error() {
        // An empty expansion (`--secret "$MISSING"`, a vault that injected nothing) must never
        // silently turn authentication off, nor fall through to a weaker source.
        let dir = tempfile::tempdir().unwrap();
        let default_file = dir.path().join("default");
        secret(6).save_file(&default_file, false).unwrap();
        let no_env = |_: &str| None;
        let env_of = |name: &'static str, value: &str| {
            let value = value.to_string();
            move |n: &str| (n == name).then(|| value.clone())
        };

        for blank in ["", "  ", "\t\n"] {
            let err =
                ClusterSecret::resolve_with(Some(blank), None, no_env, Some(default_file.clone()))
                    .expect_err("blank --secret");
            assert!(
                err.to_string().contains("--secret is set but empty"),
                "{err}"
            );
        }
        let err = ClusterSecret::resolve_with(None, Some(Path::new("")), no_env, None)
            .expect_err("blank --secret-file");
        assert!(err.to_string().contains("--secret-file"), "{err}");

        for blank in ["", "   "] {
            let err = ClusterSecret::resolve_with(
                None,
                None,
                env_of(ENV_SECRET, blank),
                Some(default_file.clone()),
            )
            .expect_err("blank OXIDESWARM_SECRET");
            assert!(err.to_string().contains(ENV_SECRET), "{err}");
            let err = ClusterSecret::resolve_with(
                None,
                None,
                env_of(ENV_SECRET_FILE, blank),
                Some(default_file.clone()),
            )
            .expect_err("blank OXIDESWARM_SECRET_FILE");
            assert!(err.to_string().contains(ENV_SECRET_FILE), "{err}");
        }

        // The message names the source but never echoes a value, and an unset source is still
        // simply "not given".
        assert!(ClusterSecret::resolve_with(None, None, no_env, None)
            .unwrap()
            .is_none());
        assert!(
            ClusterSecret::resolve_with(None, None, no_env, Some(default_file))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn resolve_never_falls_through_from_a_broken_source() {
        let dir = tempfile::tempdir().unwrap();
        let good = secret(6);
        let default_file = dir.path().join("default");
        good.save_file(&default_file, false).unwrap();
        let no_env = |_: &str| None;

        assert!(ClusterSecret::resolve_with(
            Some("nothex"),
            None,
            no_env,
            Some(default_file.clone())
        )
        .is_err());
        assert!(ClusterSecret::resolve_with(
            None,
            Some(&dir.path().join("nope")),
            no_env,
            Some(default_file.clone())
        )
        .is_err());
        let bad_env = |name: &str| (name == ENV_SECRET).then(|| "zz".to_string());
        assert!(
            ClusterSecret::resolve_with(None, None, bad_env, Some(default_file.clone())).is_err()
        );
        let broken_default = dir.path().join("broken");
        std::fs::write(&broken_default, "xyz\n").unwrap();
        assert!(ClusterSecret::resolve_with(None, None, no_env, Some(broken_default)).is_err());
    }

    #[test]
    fn default_path_lives_under_home() {
        if let Some(path) = ClusterSecret::default_path() {
            assert!(
                path.ends_with(".oxideswarm/cluster.secret")
                    || path.ends_with(".oxideswarm\\cluster.secret")
            );
        }
    }

    #[test]
    fn verify_mac_is_exact_and_rejects_every_single_bit_flip() {
        // verify_mac delegates to ring::hmac::verify (constant time); check its accept/reject
        // surface: only the exact tag passes, whatever the position of the difference.
        let s = secret(7);
        let parts: [&[u8]; 2] = [b"label", b"data"];
        let tag = s.mac(&parts);
        assert!(s.verify_mac(&parts, &tag));
        for byte in 0..MAC_LEN {
            for bit in 0..8 {
                let mut bad = tag;
                bad[byte] ^= 1 << bit;
                assert!(
                    !s.verify_mac(&parts, &bad),
                    "flip of byte {byte} bit {bit} accepted"
                );
            }
        }
        assert!(!s.verify_mac(&parts, &tag[..MAC_LEN - 1]));
        assert!(!s.verify_mac(&parts, &[tag.as_slice(), &[0]].concat()));
        assert!(!s.verify_mac(&parts, &[]));
        assert!(!s.verify_mac(&[b"label", b"datb"], &tag));
        assert!(!secret(8).verify_mac(&parts, &tag));
    }

    #[test]
    fn hex_helpers() {
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
        assert_eq!(hex_decode("000FA5ff"), Some(vec![0x00, 0x0f, 0xa5, 0xff]));
        assert_eq!(hex_decode("0"), None);
        assert_eq!(hex_decode("0g"), None);
        assert_eq!(hex_decode(""), Some(vec![]));
        // Multi-byte characters must not panic or be accepted.
        assert_eq!(hex_decode("é0"), None);
    }

    // --- Handshake -------------------------------------------------------------------------

    #[tokio::test]
    async fn handshake_succeeds_and_normal_protocol_continues() {
        let s = secret(1);
        let (mut client, mut server) = pair();
        let (c, srv) = tokio::join!(
            client_handshake(&mut client, &s),
            server_handshake(&mut server, &s)
        );
        let (ckey, skey) = (c.unwrap(), srv.unwrap());
        assert_eq!(ckey, skey, "both sides must derive the same session key");
        assert_ne!(ckey, [0u8; 32]);

        // After OK the regular typed protocol runs unchanged on the same transport.
        let hb = heartbeat();
        client.send_msg(&hb).await.unwrap();
        assert_eq!(server.recv_msg::<WorkerMessage>().await.unwrap(), Some(hb));

        // The pre-auth 1 KiB cap must be gone: a large frame now flows.
        client
            .send_raw_frame(Bytes::from(vec![0xAB; 50_000]))
            .await
            .unwrap();
        assert_eq!(
            server.recv_raw_frame().await.unwrap().unwrap().len(),
            50_000
        );
    }

    #[tokio::test]
    async fn session_keys_differ_per_connection() {
        let s = secret(1);
        let mut keys = Vec::new();
        for _ in 0..2 {
            let (mut client, mut server) = pair();
            let (c, srv) = tokio::join!(
                client_handshake(&mut client, &s),
                server_handshake(&mut server, &s)
            );
            keys.push((c.unwrap(), srv.unwrap()));
        }
        assert_ne!(keys[0].0, keys[1].0, "fresh nonces must give a fresh key");
    }

    #[tokio::test]
    async fn client_with_wrong_secret_rejects_server_and_server_sees_close() {
        let (mut client, mut server) = pair();
        let good = secret(1);
        let bad = secret(2);
        let srv = tokio::spawn(async move { server_handshake(&mut server, &good).await });
        let err = client_handshake(&mut client, &bad).await.unwrap_err();
        assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");
        drop(client);
        let err = srv.await.unwrap().unwrap_err();
        assert!(matches!(err, GridError::ConnectionClosed), "got {err:?}");
    }

    #[tokio::test]
    async fn server_rejects_proof_computed_with_wrong_secret() {
        // A client that skips the server check and answers with a PROOF from another secret.
        let (mut client, mut server) = pair();
        let good = secret(1);
        let bad = secret(2);
        let srv = tokio::spawn(async move { server_handshake(&mut server, &good).await });

        let nonce = [5u8; NONCE_LEN];
        client
            .send_raw_frame(hello(AUTH_VERSION, &nonce))
            .await
            .unwrap();
        let challenge = client.recv_raw_frame().await.unwrap().unwrap();
        let (kind, body) = split_auth_frame(&challenge).unwrap();
        assert_eq!(kind, KIND_CHALLENGE);
        let server_nonce = &body[..NONCE_LEN];
        let forged = bad.mac(&[LABEL_CLIENT, server_nonce, &nonce]);
        client
            .send_raw_frame(auth_frame(KIND_PROOF, &[&forged]))
            .await
            .unwrap();

        assert_eq!(expect_reject(&mut client).await, REJECT_AUTH_FAILED);
        let err = srv.await.unwrap().unwrap_err();
        assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn reflected_server_mac_is_not_a_valid_proof() {
        // Direction labels: the server's own MAC must not be accepted as the client's PROOF.
        let (mut client, mut server) = pair();
        let s = secret(1);
        let srv = tokio::spawn(async move { server_handshake(&mut server, &s).await });

        client
            .send_raw_frame(hello(AUTH_VERSION, &[1u8; NONCE_LEN]))
            .await
            .unwrap();
        let challenge = client.recv_raw_frame().await.unwrap().unwrap();
        let (_, body) = split_auth_frame(&challenge).unwrap();
        let server_mac = &body[NONCE_LEN..];
        client
            .send_raw_frame(auth_frame(KIND_PROOF, &[server_mac]))
            .await
            .unwrap();

        assert_eq!(expect_reject(&mut client).await, REJECT_AUTH_FAILED);
        assert!(srv.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn client_without_secret_is_told_authentication_is_required() {
        let (mut client, mut server) = pair();
        let s = secret(1);
        let srv = tokio::spawn(async move { server_handshake(&mut server, &s).await });

        // A secret-less client simply starts the normal protocol.
        client.send_msg(&heartbeat()).await.unwrap();
        // Reading a typed reply yields the actionable AuthRequired error.
        let err: GridError = client.recv_msg::<MasterMessage>().await.unwrap_err().into();
        match err {
            GridError::AuthRequired(msg) => assert!(msg.contains("cluster secret"), "{msg}"),
            other => panic!("expected AuthRequired, got {other:?}"),
        }
        let server_err = srv.await.unwrap().unwrap_err();
        assert!(
            matches!(server_err, GridError::AuthFailed(_)),
            "got {server_err:?}"
        );
    }

    #[tokio::test]
    async fn reject_reason_for_secretless_client_is_generic() {
        let (mut client, mut server) = pair();
        let s = secret(1);
        let srv = tokio::spawn(async move { server_handshake(&mut server, &s).await });
        client.send_msg(&heartbeat()).await.unwrap();
        assert_eq!(expect_reject(&mut client).await, REJECT_AUTH_REQUIRED);
        assert!(srv.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn client_with_secret_against_server_without_secret() {
        let (mut client, mut server) = pair();
        let s = secret(1);
        // A server with no secret sees an undecodable first frame and answers REJECT.
        let srv = tokio::spawn(async move {
            let err = server
                .recv_msg::<crate::protocol::InboundMessage>()
                .await
                .unwrap_err();
            assert!(
                matches!(err, ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH)),
                "got {err:?}"
            );
            send_reject(&mut server, REJECT_NO_SECRET).await.unwrap();
        });
        let err = client_handshake(&mut client, &s).await.unwrap_err();
        match err {
            GridError::AuthFailed(msg) => assert!(msg.contains("no cluster secret"), "{msg}"),
            other => panic!("expected AuthFailed, got {other:?}"),
        }
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn client_handshake_fails_cleanly_when_peer_closes_or_is_not_auth_aware() {
        let s = secret(1);
        // Peer closes immediately (e.g. an old master that cannot decode the HELLO).
        let (mut client, server) = pair();
        drop(server);
        let err = client_handshake(&mut client, &s).await;
        assert!(err.is_err());

        // Peer answers with a typed (non-auth) frame.
        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            let _ = server.recv_raw_frame().await;
            server.send_msg(&heartbeat()).await.unwrap();
        });
        let err = client_handshake(&mut client, &s).await.unwrap_err();
        assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");
        srv.await.unwrap();

        // Peer closes right after receiving the HELLO.
        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            let _ = server.recv_raw_frame().await;
        });
        let err = client_handshake(&mut client, &s).await.unwrap_err();
        assert!(matches!(err, GridError::HandshakeFailed(_)), "got {err:?}");
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn rogue_server_with_wrong_secret_is_rejected_before_any_proof() {
        let (mut client, mut rogue) = pair();
        let ours = secret(1);
        let theirs = secret(2);
        let rogue_task = tokio::spawn(async move {
            // Perfectly well-formed CHALLENGE, but MACed with the wrong secret.
            let hello = rogue.recv_raw_frame().await.unwrap().unwrap();
            let (_, body) = split_auth_frame(&hello).unwrap();
            let client_nonce = &body[1..];
            let server_nonce = [9u8; NONCE_LEN];
            let mac = theirs.mac(&[LABEL_SERVER, client_nonce, &server_nonce]);
            rogue
                .send_raw_frame(auth_frame(KIND_CHALLENGE, &[&server_nonce, &mac]))
                .await
                .unwrap();
            // The client must walk away without sending a PROOF.
            rogue.recv_raw_frame().await
        });
        let err = client_handshake(&mut client, &ours).await.unwrap_err();
        match err {
            GridError::AuthFailed(msg) => assert!(msg.contains("did not prove"), "{msg}"),
            other => panic!("expected AuthFailed, got {other:?}"),
        }
        drop(client);
        let next = rogue_task.await.unwrap().unwrap();
        assert!(
            next.is_none(),
            "client must not send a PROOF to a rogue server"
        );
    }

    #[tokio::test]
    async fn replayed_challenge_from_an_earlier_session_is_rejected() {
        let s = secret(1);
        // Record a genuine CHALLENGE (valid MAC) for client nonce A.
        let (mut recorder, mut server) = pair();
        let srv_secret = s.clone();
        let _srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        recorder
            .send_raw_frame(hello(AUTH_VERSION, &[0xAA; NONCE_LEN]))
            .await
            .unwrap();
        let recorded = recorder.recv_raw_frame().await.unwrap().unwrap();

        // Replay it at a real client, whose nonce is different.
        let (mut client, mut replayer) = pair();
        let replay = tokio::spawn(async move {
            let _ = replayer.recv_raw_frame().await;
            replayer.send_raw_frame(recorded).await.unwrap();
            replayer.recv_raw_frame().await
        });
        let err = client_handshake(&mut client, &s).await.unwrap_err();
        assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");
        drop(client);
        assert!(replay.await.unwrap().unwrap().is_none());
    }

    #[tokio::test]
    async fn replayed_proof_from_an_earlier_session_is_rejected() {
        let s = secret(1);
        let nonce = [0x11u8; NONCE_LEN];

        // Session 1: a legitimate client; capture its PROOF through a recording relay.
        let (mut client, mut relay_c) = pair();
        let (mut relay_s, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        let relay = tokio::spawn(async move {
            let h = relay_c.recv_raw_frame().await.unwrap().unwrap();
            relay_s.send_raw_frame(h).await.unwrap();
            let c = relay_s.recv_raw_frame().await.unwrap().unwrap();
            relay_c.send_raw_frame(c).await.unwrap();
            let proof = relay_c.recv_raw_frame().await.unwrap().unwrap();
            relay_s.send_raw_frame(proof.clone()).await.unwrap();
            let ok = relay_s.recv_raw_frame().await.unwrap().unwrap();
            relay_c.send_raw_frame(ok).await.unwrap();
            proof
        });
        client_handshake(&mut client, &s).await.unwrap();
        srv.await.unwrap().unwrap();
        let captured_proof = relay.await.unwrap();

        // Session 2: a fresh server nonce makes the captured PROOF worthless.
        let (mut attacker, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        attacker
            .send_raw_frame(hello(AUTH_VERSION, &nonce))
            .await
            .unwrap();
        let _challenge = attacker.recv_raw_frame().await.unwrap().unwrap();
        attacker.send_raw_frame(captured_proof).await.unwrap();
        assert_eq!(expect_reject(&mut attacker).await, REJECT_AUTH_FAILED);
        assert!(srv.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn version_mismatch_is_rejected_on_both_sides() {
        let s = secret(1);

        // Server side: HELLO with a future version.
        let (mut client, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        client
            .send_raw_frame(hello(AUTH_VERSION + 1, &[3u8; NONCE_LEN]))
            .await
            .unwrap();
        assert_eq!(expect_reject(&mut client).await, REJECT_UNSUPPORTED_VERSION);
        assert!(srv.await.unwrap().is_err());

        // Client side: a server that does not support our version.
        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            let _ = server.recv_raw_frame().await;
            send_reject(&mut server, REJECT_UNSUPPORTED_VERSION)
                .await
                .unwrap();
        });
        let err = client_handshake(&mut client, &s).await.unwrap_err();
        assert!(
            matches!(err, GridError::AuthFailed(ref m) if m.contains("version")),
            "got {err:?}"
        );
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_first_frames_are_rejected_without_panics() {
        let s = secret(1);
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty frame", vec![]),
            ("only the tag", vec![WIRE_FORMAT_AUTH]),
            (
                "tag + hello kind, no body",
                vec![WIRE_FORMAT_AUTH, KIND_HELLO],
            ),
            (
                "hello without nonce",
                vec![WIRE_FORMAT_AUTH, KIND_HELLO, AUTH_VERSION],
            ),
            (
                "hello with short nonce",
                [
                    vec![WIRE_FORMAT_AUTH, KIND_HELLO, AUTH_VERSION],
                    vec![1u8; 31],
                ]
                .concat(),
            ),
            (
                "hello with long nonce",
                [
                    vec![WIRE_FORMAT_AUTH, KIND_HELLO, AUTH_VERSION],
                    vec![1u8; 33],
                ]
                .concat(),
            ),
            ("unknown kind", vec![WIRE_FORMAT_AUTH, 0x7f, 1, 2, 3]),
            (
                "proof as first frame",
                [vec![WIRE_FORMAT_AUTH, KIND_PROOF], vec![0u8; 32]].concat(),
            ),
            ("ok as first frame", vec![WIRE_FORMAT_AUTH, KIND_OK]),
            ("bincode message", vec![0x02, 0, 0, 0, 0, 1, 2, 3]),
            ("json message", b"{\"type\":\"Register\"}".to_vec()),
            (
                "random garbage",
                (0..200u32).map(|i| (i * 37 % 251) as u8).collect(),
            ),
        ];
        for (name, payload) in cases {
            let (mut client, mut server) = pair();
            let srv_secret = s.clone();
            let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
            client.send_raw_frame(Bytes::from(payload)).await.unwrap();
            let reason = expect_reject(&mut client).await;
            assert!(
                reason == REJECT_AUTH_REQUIRED || reason == REJECT_AUTH_FAILED,
                "{name}: unexpected reason {reason:?}"
            );
            assert!(reason.len() <= MAX_REJECT_REASON);
            assert!(srv.await.unwrap().is_err(), "{name}: handshake must fail");
        }
    }

    #[tokio::test]
    async fn malformed_proof_frames_are_rejected() {
        let s = secret(1);
        let cases: Vec<(&str, Vec<u8>)> = vec![
            (
                "short proof",
                [vec![WIRE_FORMAT_AUTH, KIND_PROOF], vec![0u8; 31]].concat(),
            ),
            (
                "long proof",
                [vec![WIRE_FORMAT_AUTH, KIND_PROOF], vec![0u8; 33]].concat(),
            ),
            ("empty proof", vec![WIRE_FORMAT_AUTH, KIND_PROOF]),
            (
                "hello instead of proof",
                hello(AUTH_VERSION, &[1u8; NONCE_LEN]).to_vec(),
            ),
            (
                "reject instead of proof",
                vec![WIRE_FORMAT_AUTH, KIND_REJECT, b'x'],
            ),
            ("typed message instead of proof", vec![0x02, 9, 9, 9]),
        ];
        for (name, payload) in cases {
            let (mut client, mut server) = pair();
            let srv_secret = s.clone();
            let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
            client
                .send_raw_frame(hello(AUTH_VERSION, &[1u8; NONCE_LEN]))
                .await
                .unwrap();
            let challenge = client.recv_raw_frame().await.unwrap().unwrap();
            assert_eq!(split_auth_frame(&challenge).unwrap().0, KIND_CHALLENGE);
            client.send_raw_frame(Bytes::from(payload)).await.unwrap();
            assert_eq!(
                expect_reject(&mut client).await,
                REJECT_AUTH_FAILED,
                "{name}"
            );
            assert!(srv.await.unwrap().is_err(), "{name}");
        }
    }

    #[tokio::test]
    async fn malformed_challenges_make_the_client_fail() {
        let s = secret(1);
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty challenge", vec![WIRE_FORMAT_AUTH, KIND_CHALLENGE]),
            (
                "short challenge",
                [vec![WIRE_FORMAT_AUTH, KIND_CHALLENGE], vec![0u8; 63]].concat(),
            ),
            (
                "long challenge",
                [vec![WIRE_FORMAT_AUTH, KIND_CHALLENGE], vec![0u8; 65]].concat(),
            ),
            ("ok instead of challenge", vec![WIRE_FORMAT_AUTH, KIND_OK]),
            ("garbage kind", vec![WIRE_FORMAT_AUTH, 0xEE, 1, 2]),
            ("not an auth frame", vec![0x02, 1, 2, 3]),
            (
                "reject with binary reason",
                vec![WIRE_FORMAT_AUTH, KIND_REJECT, 0xFF, 0x00, 0x1b],
            ),
        ];
        for (name, payload) in cases {
            let (mut client, mut server) = pair();
            let srv = tokio::spawn(async move {
                let _ = server.recv_raw_frame().await;
                server.send_raw_frame(Bytes::from(payload)).await.unwrap();
            });
            let err = client_handshake(&mut client, &s).await.unwrap_err();
            assert!(
                matches!(err, GridError::AuthFailed(_)),
                "{name}: got {err:?}"
            );
            srv.await.unwrap();
        }
    }

    #[tokio::test]
    async fn reject_text_from_the_peer_is_sanitised() {
        let s = secret(1);
        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            let _ = server.recv_raw_frame().await;
            let mut body = b"evil\nINFO forged log line\x1b[31m".to_vec();
            body.resize(body.len() + 500, b'A');
            server
                .send_raw_frame(auth_frame(KIND_REJECT, &[&body]))
                .await
                .unwrap();
        });
        let err = client_handshake(&mut client, &s).await.unwrap_err();
        let text = err.to_string();
        assert!(!text.contains('\n') && !text.contains('\x1b'), "{text:?}");
        assert!(
            text.len() < 400,
            "reason must be length-bounded, got {}",
            text.len()
        );
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_pre_auth_frame_is_rejected_without_buffering_it() {
        let s = secret(1);
        let (mut raw, b) = tokio::io::duplex(64 * 1024);
        let mut server = MessageTransport::new(b);
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move {
            let res = server_handshake(&mut server, &srv_secret).await;
            (res, server)
        });
        // Header announces 16 MiB but only a few bytes follow: a naive reader would wait for
        // (and allocate) the whole frame.
        use tokio::io::AsyncWriteExt;
        raw.write_all(&(16u32 * 1024 * 1024).to_be_bytes())
            .await
            .unwrap();
        raw.write_all(&[WIRE_FORMAT_AUTH, KIND_HELLO, 1, 2, 3])
            .await
            .unwrap();

        let (res, _server) = tokio::time::timeout(Duration::from_secs(5), srv)
            .await
            .expect("server must not wait for the oversized payload")
            .unwrap();
        match res.unwrap_err() {
            GridError::AuthFailed(msg) => {
                assert!(msg.contains("pre-authentication limit"), "{msg}")
            }
            other => panic!("expected AuthFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversized_frame_just_above_the_cap_is_rejected_and_at_the_cap_is_parsed() {
        let s = secret(1);
        // PRE_AUTH_MAX_FRAME + 1 bytes of otherwise valid-looking prefix.
        let mut big = vec![WIRE_FORMAT_AUTH, KIND_HELLO, AUTH_VERSION];
        big.resize(PRE_AUTH_MAX_FRAME + 1, 0);
        let (mut client, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        client.send_raw_frame(Bytes::from(big)).await.unwrap();
        assert_eq!(expect_reject(&mut client).await, REJECT_AUTH_REQUIRED);
        assert!(srv.await.unwrap().is_err());

        // Exactly at the cap the frame is read and judged on content (malformed HELLO here).
        let mut at_cap = vec![WIRE_FORMAT_AUTH, KIND_HELLO, AUTH_VERSION];
        at_cap.resize(PRE_AUTH_MAX_FRAME, 0);
        let (mut client, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        client.send_raw_frame(Bytes::from(at_cap)).await.unwrap();
        assert_eq!(expect_reject(&mut client).await, REJECT_AUTH_FAILED);
        assert!(srv.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn truncated_streams_end_the_handshake_with_connection_closed() {
        let s = secret(1);

        // Peer connects and says nothing.
        let (client, mut server) = pair();
        drop(client);
        let err = server_handshake(&mut server, &s).await.unwrap_err();
        assert!(matches!(err, GridError::ConnectionClosed), "got {err:?}");

        // Peer dies in the middle of a frame (header promises 35 bytes, 5 arrive).
        let (a, b) = tokio::io::duplex(4096);
        let mut server = MessageTransport::new(b);
        let mut raw = a;
        use tokio::io::AsyncWriteExt;
        raw.write_all(&35u32.to_be_bytes()).await.unwrap();
        raw.write_all(&[WIRE_FORMAT_AUTH, KIND_HELLO, 1, 2, 3])
            .await
            .unwrap();
        drop(raw);
        let err = server_handshake(&mut server, &s).await.unwrap_err();
        assert!(
            matches!(err, GridError::ConnectionClosed | GridError::Io(_)),
            "got {err:?}"
        );

        // Peer vanishes after the CHALLENGE, before the PROOF.
        let (mut client, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        client
            .send_raw_frame(hello(AUTH_VERSION, &[2u8; NONCE_LEN]))
            .await
            .unwrap();
        let _ = client.recv_raw_frame().await.unwrap().unwrap();
        drop(client);
        let err = srv.await.unwrap().unwrap_err();
        assert!(matches!(err, GridError::ConnectionClosed), "got {err:?}");
    }

    #[tokio::test]
    async fn failed_handshake_is_delayed_to_blunt_brute_force() {
        let s = secret(1);
        let (mut client, mut server) = pair();
        let srv_secret = s.clone();
        let srv = tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
        let started = std::time::Instant::now();
        client.send_msg(&heartbeat()).await.unwrap();
        let _ = expect_reject(&mut client).await;
        assert!(
            started.elapsed() >= FAILURE_DELAY - Duration::from_millis(20),
            "REJECT must not be sent before the failure delay elapsed ({:?})",
            started.elapsed()
        );
        assert!(srv.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn concurrent_handshakes_do_not_interfere() {
        let s = secret(4);
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let s = s.clone();
            tasks.push(tokio::spawn(async move {
                let (mut client, mut server) = pair();
                let srv_secret = s.clone();
                let srv =
                    tokio::spawn(async move { server_handshake(&mut server, &srv_secret).await });
                let ckey = client_handshake(&mut client, &s).await.unwrap();
                assert_eq!(ckey, srv.await.unwrap().unwrap());
                ckey
            }));
        }
        let mut keys = Vec::new();
        for t in tasks {
            keys.push(t.await.unwrap());
        }
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 16, "every connection needs its own session key");
    }
}
