//! Per-machine X25519 identity keys and the OXS-AUTH v3 (`Noise_IK_25519_ChaChaPoly_BLAKE2s`)
//! handshake (design record `docs/design/0051-machine-identity-and-invites.md`, ENROLL-1a).
//!
//! # Protocol (OXS-AUTH v3)
//!
//! All pre-authentication frames use `WIRE_FORMAT_AUTH` (`0x03`) and are capped at
//! [`PRE_AUTH_MAX_FRAME`] (1 KiB).
//!
//! ```text
//!   Client (Initiator, static key s_i)            Master (Responder, static key s_r)
//!   ----------------------------------            ----------------------------------
//!   1. [0x03][0x01 HELLO][0x03 (v3)][proto_ver: u32 BE][purpose ("member" | "enroll")]
//!      ----------------------------------------->
//!                                                 verify is_protocol_version_compatible(proto_ver)
//!                                                 verify purpose
//!   2. [0x03][0x06 NOISE_MSG1][Noise IK -> e, es, s, ss]
//!      (prologue = exact HELLO frame bytes)
//!      ----------------------------------------->
//!                                                 Noise IK read_message(msg1)
//!                                                 extract remote static key s_i
//!                                                 verify s_i is an admitted member
//!                                                 [0x03][0x07 NOISE_MSG2][Noise IK <- e, ee, se]
//!      <-----------------------------------------
//!   Noise Split() -> (k_i2r, k_r2i)               Noise Split() -> (k_i2r, k_r2i)
//!   session_key = HMAC-SHA256("oxs3-split-session-key", k_i2r || k_r2i)
//!   enable_encryption(session_key, Client)        enable_encryption(session_key, Server)
//!   3. First 0x04 AEAD transport record --------> decrypts under fresh Split() keys ->
//!                                                 peer is now treated as authenticated
//! ```
//!
//! Because Noise `IK` message 1 (`-> e, es, s, ss`) does not depend on the responder's ephemeral
//! key `e_r`, an attacker who recorded a genuine message 1 can replay it. However, the responder's
//! fresh `e_r` in message 2 mixes `ee = DH(e_i, e_r)` and `se = DH(s_i, e_r)` into the chaining
//! key before `Split()`, so a replayed message 1 cannot produce a valid `0x04` transport record.
//! The master therefore treats a v3 peer as authenticated **only after** its first encrypted
//! transport record decrypts ([`v3_server_confirm_first_record`]).

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use ring::{digest, hmac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::cluster_auth::{
    auth_frame, config_detail, error_from_reject, handshake_read_error, hex_decode, hex_encode,
    random_bytes, recv_auth_frame, reject_peer, send_reject, split_auth_frame, write_private_new,
    AUTH_VERSION_V3, KIND_HELLO, KIND_REJECT, MAC_LEN, PRE_AUTH_MAX_FRAME, REJECT_AUTH_FAILED,
    REJECT_AUTH_REQUIRED, REJECT_WRONG_MASTER_KEY,
};
use crate::error::{GridError, GridResult};
use crate::protocol::{
    format_incompatible_version_error, is_protocol_version_compatible, EndpointRole,
    MessageTransport, ProtocolError, WireCodec, PROTOCOL_VERSION_CURRENT,
};

/// Length in bytes of an X25519 private or public key.
pub const KEY_LEN: usize = 32;

/// Number of leading lowercase hex characters of `SHA-256(public_key)` used as a member ID.
pub const MEMBER_ID_HEX_LEN: usize = 16;

/// Noise handshake pattern for OXS-AUTH v3.
pub const NOISE_PATTERN: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";

/// Default file name for a node's X25519 private key inside `~/.oxideswarm/`.
pub const NODE_KEY_FILE_NAME: &str = "node.key";

/// Default file name for the master's X25519 private key inside `~/.oxideswarm/` or `--state-dir`.
pub const MASTER_NOISE_KEY_FILE_NAME: &str = "master_noise.key";

/// Environment variable holding the pinned master v3 public key as 64 hex characters.
pub const ENV_MASTER_KEY: &str = "RUSTY_GRID_MASTER_KEY";

/// Pre-auth frame kind for Noise IK message 1 (`-> e, es, s, ss`).
pub(crate) const KIND_NOISE_MSG1: u8 = 0x06;

/// Pre-auth frame kind for Noise IK message 2 (`<- e, ee, se`).
pub(crate) const KIND_NOISE_MSG2: u8 = 0x07;

/// Maximum frame size (64 KiB) accepted for the first encrypted OXS-AUTH v3 transport record
/// before the peer has proven liveness on the `Split()` session key.
pub const V3_FIRST_RECORD_MAX_FRAME: usize = 64 * 1024;

/// Domain-separation label used to derive the 32-byte record-layer `session_key` from the two raw
/// directional `Split()` keys `(k_i2r, k_r2i)` produced by `snow`.
const LABEL_V3_SESSION_KEY: &[u8] = b"oxs3-split-session-key";

/// Formats 16 lowercase hex characters into 4 hyphen-separated groups (`xxxx-xxxx-xxxx-xxxx`).
pub fn format_fingerprint(member_id: &str) -> String {
    debug_assert_eq!(member_id.len(), MEMBER_ID_HEX_LEN);
    format!(
        "{}-{}-{}-{}",
        &member_id[0..4],
        &member_id[4..8],
        &member_id[8..12],
        &member_id[12..16]
    )
}

/// Computes the 16-hex-character member ID (`first 16 hex chars of SHA-256(public_key)`).
pub fn compute_member_id(public_key: &[u8; KEY_LEN]) -> String {
    let digest = digest::digest(&digest::SHA256, public_key);
    hex_encode(&digest.as_ref()[..8])
}

/// Computes the human-readable fingerprint (`xxxx-xxxx-xxxx-xxxx`) of `public_key`.
pub fn compute_fingerprint(public_key: &[u8; KEY_LEN]) -> String {
    format_fingerprint(&compute_member_id(public_key))
}

/// An X25519 public key (32 bytes). Safe to log, serialize, and display.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublicKey([u8; KEY_LEN]);

impl PublicKey {
    /// Wraps a 32-byte X25519 public key.
    pub const fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// Parses a 64-character hex string (ignoring surrounding whitespace) into a [`PublicKey`].
    pub fn from_hex(text: &str) -> GridResult<Self> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(GridError::Config("public key is empty".into()));
        }
        let bytes = hex_decode(trimmed).ok_or_else(|| {
            GridError::Config("public key must be 64 hexadecimal characters (0-9, a-f)".to_string())
        })?;
        if bytes.len() != KEY_LEN {
            return Err(GridError::Config(format!(
                "public key must be {KEY_LEN} bytes ({} hex characters), got {} bytes",
                KEY_LEN * 2,
                bytes.len()
            )));
        }
        let mut arr = [0u8; KEY_LEN];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Raw 32-byte public key slice.
    pub const fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// Lowercase 64-character hex representation.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// First 16 lowercase hex characters of `SHA-256(public_key)`.
    pub fn member_id(&self) -> String {
        compute_member_id(&self.0)
    }

    /// Human-readable fingerprint (`xxxx-xxxx-xxxx-xxxx`).
    pub fn fingerprint(&self) -> String {
        compute_fingerprint(&self.0)
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({})", self.to_hex())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for PublicKey {
    type Err = GridError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for PublicKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

struct KeypairInner {
    private: [u8; KEY_LEN],
    public: PublicKey,
}

impl Drop for KeypairInner {
    fn drop(&mut self) {
        for b in &mut self.private {
            // SAFETY: `b` is a valid mutable byte in `self.private`, live for this loop body.
            unsafe { std::ptr::write_volatile(b, 0) };
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
}

/// Static X25519 key pair (`node.key` or `master_noise.key`).
///
/// Cheap to clone (`Arc` inside); `Debug` redacts the private key and `Drop` zeroes it.
#[derive(Clone)]
pub struct NodeKeypair(Arc<KeypairInner>);

impl fmt::Debug for NodeKeypair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeKeypair(<redacted>)")
    }
}

impl PartialEq for NodeKeypair {
    fn eq(&self, other: &Self) -> bool {
        let k1 = hmac::Key::new(hmac::HMAC_SHA256, &self.0.private);
        let k2 = hmac::Key::new(hmac::HMAC_SHA256, &other.0.private);
        let tag2 = hmac::sign(&k2, b"oxs3-eq");
        hmac::verify(&k1, b"oxs3-eq", tag2.as_ref()).is_ok() && self.0.public == other.0.public
    }
}

impl Eq for NodeKeypair {}

fn derive_x25519_public(private: &[u8; KEY_LEN]) -> GridResult<PublicKey> {
    use snow::resolvers::{CryptoResolver, DefaultResolver};
    let mut dh = DefaultResolver
        .resolve_dh(&snow::params::DHChoice::Curve25519)
        .ok_or_else(|| GridError::Config("Curve25519 DH resolver unavailable".into()))?;
    dh.set(private);
    let mut pub_bytes = [0u8; KEY_LEN];
    pub_bytes.copy_from_slice(&dh.pubkey()[..KEY_LEN]);
    Ok(PublicKey(pub_bytes))
}

impl NodeKeypair {
    /// Generates a fresh 32-byte X25519 key pair from the OS CSPRNG.
    pub fn generate() -> GridResult<Self> {
        let mut priv_bytes = [0u8; KEY_LEN];
        random_bytes(&mut priv_bytes)?;
        Self::from_private_bytes(priv_bytes)
    }

    /// Constructs a [`NodeKeypair`] from a 32-byte X25519 private key and derives its public key.
    pub fn from_private_bytes(private: [u8; KEY_LEN]) -> GridResult<Self> {
        let public = derive_x25519_public(&private)?;
        Ok(Self(Arc::new(KeypairInner { private, public })))
    }

    /// Parses a 64-character hex string (ignoring surrounding whitespace) as a private key.
    pub fn from_private_hex(text: &str) -> GridResult<Self> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(GridError::Config("private key is empty".into()));
        }
        let bytes = hex_decode(trimmed).ok_or_else(|| {
            GridError::Config(
                "private key must be 64 hexadecimal characters (0-9, a-f)".to_string(),
            )
        })?;
        if bytes.len() != KEY_LEN {
            return Err(GridError::Config(format!(
                "private key must be {KEY_LEN} bytes ({} hex characters), got {} bytes",
                KEY_LEN * 2,
                bytes.len()
            )));
        }
        let mut arr = [0u8; KEY_LEN];
        arr.copy_from_slice(&bytes);
        Self::from_private_bytes(arr)
    }

    /// Returns the public key.
    pub fn public_key(&self) -> &PublicKey {
        &self.0.public
    }

    /// Returns the public key encoded as 64 lowercase hex characters.
    pub fn public_key_hex(&self) -> String {
        self.0.public.to_hex()
    }

    /// Returns the 16-hex-character member ID (`first 16 hex chars of SHA-256(public_key)`).
    pub fn member_id(&self) -> String {
        self.0.public.member_id()
    }

    /// Returns the human-readable fingerprint (`xxxx-xxxx-xxxx-xxxx`) of the public key.
    pub fn fingerprint(&self) -> String {
        self.0.public.fingerprint()
    }

    /// Raw 32-byte private key for `snow` handshake construction.
    pub(crate) fn private_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0.private
    }

    /// Encodes the private key as 64 lowercase hex characters for saving to a private key file.
    fn private_hex(&self) -> String {
        hex_encode(&self.0.private)
    }

    /// Reads a key pair from `path` (accepts either a 64-char hex line or raw 32 bytes).
    ///
    /// Warns if the file is readable by other local users.
    pub fn load_file(path: &Path) -> GridResult<Self> {
        let raw = std::fs::read(path).map_err(|e| {
            GridError::Config(format!("cannot read key file '{}': {e}", path.display()))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    tracing::warn!(
                        path = %path.display(),
                        mode = format!("{mode:04o}"),
                        "key file is readable by other local users; run: chmod 600 '{}'",
                        path.display()
                    );
                }
            }
        }
        #[cfg(windows)]
        crate::private_file::warn_if_readable_by_others(path, "identity key file");

        if raw.len() == KEY_LEN {
            let mut arr = [0u8; KEY_LEN];
            arr.copy_from_slice(&raw);
            return Self::from_private_bytes(arr);
        }

        let text = std::str::from_utf8(&raw).map_err(|_| {
            GridError::Config(format!(
                "key file '{}' is invalid: expected 64 hex characters or 32 raw bytes",
                path.display()
            ))
        })?;
        let line = text
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .ok_or_else(|| GridError::Config(format!("key file '{}' is empty", path.display())))?;
        Self::from_private_hex(line).map_err(|e| {
            GridError::Config(format!(
                "key file '{}' is invalid: {}",
                path.display(),
                config_detail(&e)
            ))
        })
    }

    /// Writes the private key to `path` as a private file (mode `0600` on Unix, protected
    /// owner/SYSTEM/Administrators DACL on Windows via `private_file.rs`).
    pub fn save_file(&self, path: &Path, overwrite: bool) -> GridResult<()> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            let existed = parent.exists();
            #[cfg(windows)]
            let created = crate::private_file::create_private_dir_all(parent);
            #[cfg(not(windows))]
            let created = std::fs::create_dir_all(parent);
            created.map_err(|e| {
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

        let line = format!("{}\n", self.private_hex());
        if !overwrite {
            return write_private_new(path, line.as_bytes()).map_err(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    GridError::Config(format!(
                        "refusing to overwrite existing key file '{}'",
                        path.display()
                    ))
                } else {
                    GridError::Config(format!("cannot write key file '{}': {e}", path.display()))
                }
            });
        }

        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| NODE_KEY_FILE_NAME.to_string());
        let tmp = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        write_private_new(&tmp, line.as_bytes())
            .and_then(|()| std::fs::rename(&tmp, path))
            .map_err(|e| {
                let _ = std::fs::remove_file(&tmp);
                GridError::Config(format!("cannot write key file '{}': {e}", path.display()))
            })
    }

    /// Loads the key pair at `path`, generating and saving a new private file when missing.
    /// Returns `(keypair, created_now)`.
    pub fn load_or_generate_file(path: &Path) -> GridResult<(Self, bool)> {
        if path.exists() {
            return Self::load_file(path).map(|k| (k, false));
        }
        let key = Self::generate()?;
        match key.save_file(path, false) {
            Ok(()) => Ok((key, true)),
            Err(_) if path.exists() => Self::load_file(path).map(|k| (k, false)),
            Err(e) => Err(e),
        }
    }
}

/// Default `~/.oxideswarm` directory (`OXIDESWARM_HOME` if set, otherwise `<HOME>/.oxideswarm`).
pub fn default_oxideswarm_dir() -> Option<PathBuf> {
    if let Some(custom) = std::env::var_os("OXIDESWARM_HOME").filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(custom));
    }
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"));
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME");
    home.filter(|h| !h.is_empty())
        .map(|h| PathBuf::from(h).join(".oxideswarm"))
}

/// Default path for the machine's node key: `~/.oxideswarm/node.key`.
pub fn default_node_key_path() -> Option<PathBuf> {
    default_oxideswarm_dir().map(|d| d.join(NODE_KEY_FILE_NAME))
}

/// Path for `node.key`, preferring `state_dir` when given, otherwise `~/.oxideswarm/node.key`.
pub fn node_key_path(state_dir: Option<&Path>) -> Option<PathBuf> {
    state_dir
        .map(|d| d.join(NODE_KEY_FILE_NAME))
        .or_else(default_node_key_path)
}

/// Path for `master_noise.key`, preferring `state_dir` when given, otherwise `~/.oxideswarm/master_noise.key`.
pub fn master_noise_key_path(state_dir: Option<&Path>) -> Option<PathBuf> {
    state_dir
        .map(|d| d.join(MASTER_NOISE_KEY_FILE_NAME))
        .or_else(|| default_oxideswarm_dir().map(|d| d.join(MASTER_NOISE_KEY_FILE_NAME)))
}

// ---------------------------------------------------------------------------------------------
// OXS-AUTH v3 Handshake (Noise_IK_25519_ChaChaPoly_BLAKE2s)
// ---------------------------------------------------------------------------------------------

/// Purpose carried in the cleartext v3 `HELLO` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakePurpose {
    /// Normal connection from an enrolled cluster member (`"member"`).
    Member,
    /// Enrollment connection carrying an invite proof (`"enroll"`, used in ENROLL-1b).
    Enroll,
}

impl HandshakePurpose {
    /// Wire string representation (`"member"` or `"enroll"`).
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Enroll => "enroll",
        }
    }

    fn from_wire_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"member" | [0x01] => Some(Self::Member),
            b"enroll" | [0x02] => Some(Self::Enroll),
            _ => None,
        }
    }
}

/// Builds the cleartext v3 `HELLO` frame (`[0x03, KIND_HELLO, 0x03, proto_ver_be4, purpose...]`).
///
/// The exact bytes of this frame are also passed to `snow::Builder::prologue` on both sides.
pub fn build_v3_hello_frame(protocol_version: u32, purpose: HandshakePurpose) -> Bytes {
    let ver_bytes = protocol_version.to_be_bytes();
    auth_frame(
        KIND_HELLO,
        &[&[AUTH_VERSION_V3], &ver_bytes, purpose.as_str().as_bytes()],
    )
}

/// Parses the body (after `[0x03, KIND_HELLO]`) of a v3 `HELLO` frame into
/// `(peer_protocol_version, purpose)`.
pub fn parse_v3_hello_body(body: &[u8]) -> Option<(u32, HandshakePurpose)> {
    if body.first() != Some(&AUTH_VERSION_V3) || body.len() < 1 + 4 + 1 {
        return None;
    }
    let proto_ver = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
    let purpose = HandshakePurpose::from_wire_bytes(&body[5..])?;
    Some((proto_ver, purpose))
}

/// Derives the 32-byte record-layer `session_key` from snow's two raw `Split()` directional keys
/// `(k_i2r, k_r2i)` using `HMAC-SHA256(key = "oxs3-split-session-key", k_i2r || k_r2i)`.
fn derive_v3_session_key(hs: &mut snow::HandshakeState) -> [u8; MAC_LEN] {
    let (mut k1, mut k2) = hs.dangerously_get_raw_split();
    let key = hmac::Key::new(hmac::HMAC_SHA256, LABEL_V3_SESSION_KEY);
    let mut ctx = hmac::Context::with_key(&key);
    ctx.update(&k1);
    ctx.update(&k2);
    let tag = ctx.sign();
    let mut out = [0u8; MAC_LEN];
    out.copy_from_slice(tag.as_ref());
    for b in k1.iter_mut().chain(k2.iter_mut()) {
        // SAFETY: `b` is a valid mutable stack byte.
        unsafe { std::ptr::write_volatile(b, 0) };
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    out
}

/// Runs the client (initiator) side of the OXS-AUTH v3 handshake using
/// [`PROTOCOL_VERSION_CURRENT`].
///
/// On success returns the 32-byte session key for
/// `MessageTransport::enable_encryption(&session_key, EndpointRole::Client)`.
pub async fn v3_client_handshake<T>(
    t: &mut MessageTransport<T>,
    node_key: &NodeKeypair,
    pinned_master_key: &PublicKey,
    purpose: HandshakePurpose,
) -> GridResult<[u8; MAC_LEN]>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    v3_client_handshake_with_version(
        t,
        node_key,
        pinned_master_key,
        purpose,
        PROTOCOL_VERSION_CURRENT,
    )
    .await
}

/// Like [`v3_client_handshake`], with an explicit `protocol_version` (used by version-mismatch
/// unit tests).
pub async fn v3_client_handshake_with_version<T>(
    t: &mut MessageTransport<T>,
    node_key: &NodeKeypair,
    pinned_master_key: &PublicKey,
    purpose: HandshakePurpose,
    protocol_version: u32,
) -> GridResult<[u8; MAC_LEN]>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let hello_frame = build_v3_hello_frame(protocol_version, purpose);
    let params: snow::params::NoiseParams = NOISE_PATTERN
        .parse()
        .map_err(|e| GridError::Config(format!("invalid noise pattern: {e}")))?;
    let mut hs = snow::Builder::new(params)
        .local_private_key(node_key.private_bytes())
        .and_then(|b| b.remote_public_key(pinned_master_key.as_bytes()))
        .and_then(|b| b.prologue(&hello_frame))
        .and_then(|b| b.build_initiator())
        .map_err(|e| GridError::AuthFailed(format!("failed to initialize v3 handshake: {e}")))?;

    let mut msg1_buf = [0u8; PRE_AUTH_MAX_FRAME];
    let msg1_len = hs
        .write_message(&[], &mut msg1_buf)
        .map_err(|e| GridError::AuthFailed(format!("failed to build Noise IK message 1: {e}")))?;

    t.send_raw_frame(hello_frame).await?;
    if let Err(send_err) = t
        .send_raw_frame(auth_frame(KIND_NOISE_MSG1, &[&msg1_buf[..msg1_len]]))
        .await
    {
        if let Ok(Some(frame)) = recv_auth_frame(t).await {
            if let Some((KIND_REJECT, body)) = split_auth_frame(&frame) {
                return Err(error_from_reject(body));
            }
        }
        return Err(send_err.into());
    }

    let reply = recv_auth_frame(t)
        .await
        .map_err(handshake_read_error)?
        .ok_or_else(|| {
            GridError::HandshakeFailed(
                "master closed the connection during v3 authentication".to_string(),
            )
        })?;
    let (kind, body) = split_auth_frame(&reply).ok_or_else(|| {
        GridError::AuthFailed(
            "peer answered with a non-authentication frame; it does not speak OXS-AUTH v3"
                .to_string(),
        )
    })?;
    match kind {
        KIND_NOISE_MSG2 => {}
        KIND_REJECT => return Err(error_from_reject(body)),
        _ => {
            return Err(GridError::AuthFailed(
                "unexpected frame during OXS-AUTH v3 handshake".to_string(),
            ))
        }
    }

    let mut payload_buf = [0u8; PRE_AUTH_MAX_FRAME];
    hs.read_message(body, &mut payload_buf).map_err(|_| {
        GridError::AuthFailed(
            "the master did not prove knowledge of the pinned --master-key (public key mismatch)"
                .to_string(),
        )
    })?;

    Ok(derive_v3_session_key(&mut hs))
}

/// Server-side result after completing the two Noise `IK` handshake messages, **before** the
/// peer's first encrypted `0x04` transport record has been decrypted.
///
/// Per D2 step 4, a peer is only treated as authenticated once its first encrypted transport
/// record decrypts under `session_key` (because `IK` message 1 can be replayed).
pub struct PendingV3Session {
    session_key: [u8; MAC_LEN],
    peer_public_key: PublicKey,
    purpose: HandshakePurpose,
}

impl fmt::Debug for PendingV3Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingV3Session")
            .field("session_key", &"<redacted>")
            .field("peer_public_key", &self.peer_public_key)
            .field("purpose", &self.purpose)
            .finish()
    }
}

impl Drop for PendingV3Session {
    fn drop(&mut self) {
        for b in &mut self.session_key {
            // SAFETY: `b` is a valid mutable byte in `self.session_key`.
            unsafe { std::ptr::write_volatile(b, 0) };
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
}

impl PendingV3Session {
    /// The peer's static X25519 public key from Noise `IK` message 1.
    pub fn peer_public_key(&self) -> &PublicKey {
        &self.peer_public_key
    }

    /// The purpose declared in the peer's `HELLO`.
    pub fn purpose(&self) -> HandshakePurpose {
        self.purpose
    }

    /// The derived 32-byte record-layer session key.
    pub fn session_key(&self) -> &[u8; MAC_LEN] {
        &self.session_key
    }
}

/// A v3 peer whose Noise `IK` handshake **and** first encrypted `0x04` transport record have both
/// been verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedV3Peer {
    pub public_key: PublicKey,
    pub member_id: String,
    pub purpose: HandshakePurpose,
}

/// Runs the server (responder) side of the OXS-AUTH v3 handshake from the start of the stream.
pub async fn v3_server_handshake<T, F>(
    t: &mut MessageTransport<T>,
    master_key: &NodeKeypair,
    is_admitted_member: F,
) -> GridResult<PendingV3Session>
where
    T: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(&PublicKey) -> bool,
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
    v3_server_handshake_from_hello(t, &hello, master_key, is_admitted_member).await
}

/// Completes the server (responder) side of the OXS-AUTH v3 handshake given an already-read
/// `hello_frame`.
///
/// 1. Validates the cleartext `HELLO` (`auth_version == 3`, protocol version compatibility via
///    design 0040's [`is_protocol_version_compatible`], and `purpose == Member`).
/// 2. Reads Noise `IK` message 1 (capped at [`PRE_AUTH_MAX_FRAME`]) with `prologue = hello_frame`.
/// 3. Checks the peer's static public key against `is_admitted_member`.
/// 4. Sends Noise `IK` message 2 and derives the record-layer key from `Split()`.
pub async fn v3_server_handshake_from_hello<T, F>(
    t: &mut MessageTransport<T>,
    hello_frame: &[u8],
    master_key: &NodeKeypair,
    is_admitted_member: F,
) -> GridResult<PendingV3Session>
where
    T: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(&PublicKey) -> bool,
{
    if hello_frame.len() > PRE_AUTH_MAX_FRAME {
        return Err(reject_peer(
            t,
            REJECT_AUTH_REQUIRED,
            format!("first frame exceeds the {PRE_AUTH_MAX_FRAME} byte pre-authentication limit"),
        )
        .await);
    }
    let body = match split_auth_frame(hello_frame) {
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
    let Some((peer_protocol_version, purpose)) = parse_v3_hello_body(body) else {
        return Err(reject_peer(t, REJECT_AUTH_FAILED, "malformed v3 HELLO").await);
    };

    // Design 0040 / 0051 D2 step 1: check protocol version compatibility first, before any crypto.
    if !is_protocol_version_compatible(peer_protocol_version) {
        let message = format_incompatible_version_error("peer", peer_protocol_version);
        let _ = send_reject(t, &message).await;
        return Err(GridError::IncompatibleVersion(message));
    }

    if purpose != HandshakePurpose::Member {
        return Err(reject_peer(
            t,
            REJECT_AUTH_FAILED,
            format!(
                "unsupported v3 handshake purpose '{}' (only 'member' is supported in ENROLL-1a)",
                purpose.as_str()
            ),
        )
        .await);
    }

    let msg1_frame = match recv_auth_frame(t).await {
        Ok(Some(frame)) => frame,
        Ok(None) => return Err(GridError::ConnectionClosed),
        Err(ProtocolError::FrameTooLarge { max, .. }) => {
            return Err(reject_peer(
                t,
                REJECT_AUTH_FAILED,
                format!("Noise IK message 1 exceeds the {max} byte pre-authentication limit"),
            )
            .await)
        }
        Err(e) => return Err(e.into()),
    };
    let msg1_bytes = match split_auth_frame(&msg1_frame) {
        Some((KIND_NOISE_MSG1, bytes)) if !bytes.is_empty() => bytes,
        _ => {
            return Err(
                reject_peer(t, REJECT_AUTH_FAILED, "expected Noise IK message 1 frame").await,
            )
        }
    };

    let params: snow::params::NoiseParams = NOISE_PATTERN
        .parse()
        .map_err(|e| GridError::Config(format!("invalid noise pattern: {e}")))?;
    let mut hs = snow::Builder::new(params)
        .local_private_key(master_key.private_bytes())
        .and_then(|b| b.prologue(hello_frame))
        .and_then(|b| b.build_responder())
        .map_err(|e| GridError::AuthFailed(format!("failed to initialize v3 responder: {e}")))?;

    let mut payload_buf = [0u8; PRE_AUTH_MAX_FRAME];
    if hs.read_message(msg1_bytes, &mut payload_buf).is_err() {
        return Err(reject_peer(
            t,
            REJECT_WRONG_MASTER_KEY,
            "Noise IK message 1 decryption failed (peer's pinned master key does not match this master)",
        )
        .await);
    }

    let remote_static = hs.get_remote_static().ok_or_else(|| {
        GridError::AuthFailed("Noise IK message 1 did not provide a remote static key".into())
    })?;
    if remote_static.len() != KEY_LEN {
        return Err(reject_peer(t, REJECT_AUTH_FAILED, "invalid remote static key length").await);
    }
    let mut peer_pub_bytes = [0u8; KEY_LEN];
    peer_pub_bytes.copy_from_slice(remote_static);
    let peer_public_key = PublicKey::from_bytes(peer_pub_bytes);

    if !is_admitted_member(&peer_public_key) {
        return Err(reject_peer(
            t,
            REJECT_AUTH_FAILED,
            format!(
                "peer member {} (public key {}) is not in members.json",
                peer_public_key.member_id(),
                peer_public_key.to_hex()
            ),
        )
        .await);
    }

    let mut msg2_buf = [0u8; PRE_AUTH_MAX_FRAME];
    let msg2_len = hs
        .write_message(&[], &mut msg2_buf)
        .map_err(|e| GridError::AuthFailed(format!("failed to build Noise IK message 2: {e}")))?;
    t.send_raw_frame(auth_frame(KIND_NOISE_MSG2, &[&msg2_buf[..msg2_len]]))
        .await?;

    let session_key = derive_v3_session_key(&mut hs);
    Ok(PendingV3Session {
        session_key,
        peer_public_key,
        purpose,
    })
}

/// Enables record-layer encryption on `t` with `pending`'s `Split()` key, reads and decrypts the
/// peer's first `0x04` transport record, and only then returns the confirmed
/// [`AuthenticatedV3Peer`].
///
/// This enforces D2 step 4: a replayed Noise `IK` message 1 cannot decrypt or authenticate any
/// transport record after the responder's fresh ephemeral key in message 2 is mixed into `Split()`.
pub async fn v3_server_confirm_first_record<T, M>(
    t: &mut MessageTransport<T>,
    pending: PendingV3Session,
) -> GridResult<(AuthenticatedV3Peer, M, WireCodec)>
where
    T: AsyncRead + AsyncWrite + Unpin,
    M: DeserializeOwned + 'static,
{
    t.enable_encryption(pending.session_key(), EndpointRole::Server);
    match t
        .recv_msg_with_codec_max::<M>(V3_FIRST_RECORD_MAX_FRAME)
        .await
    {
        Ok(Some((msg, codec))) => {
            let auth_peer = AuthenticatedV3Peer {
                member_id: pending.peer_public_key.member_id(),
                public_key: pending.peer_public_key,
                purpose: pending.purpose,
            };
            Ok((auth_peer, msg, codec))
        }
        Ok(None) => Err(GridError::ConnectionClosed),
        Err(e) => Err(e.into()),
    }
}

/// Runs [`v3_server_handshake`] followed by [`v3_server_confirm_first_record`], returning an
/// [`AuthenticatedV3Peer`] only after the peer's first encrypted transport record decrypts.
pub async fn v3_server_accept_authenticated<T, M, F>(
    t: &mut MessageTransport<T>,
    master_key: &NodeKeypair,
    is_admitted_member: F,
) -> GridResult<(AuthenticatedV3Peer, M, WireCodec)>
where
    T: AsyncRead + AsyncWrite + Unpin,
    M: DeserializeOwned + 'static,
    F: FnOnce(&PublicKey) -> bool,
{
    let pending = v3_server_handshake(t, master_key, is_admitted_member).await?;
    v3_server_confirm_first_record(t, pending).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{MasterMessage, WorkerMessage, WIRE_FORMAT_AUTH, WIRE_FORMAT_ENCRYPTED};
    use tokio::io::DuplexStream;
    use uuid::Uuid;

    type Duplex = MessageTransport<DuplexStream>;

    fn pair() -> (Duplex, Duplex) {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (MessageTransport::new(a), MessageTransport::new(b))
    }

    fn keypair(seed: u8) -> NodeKeypair {
        let mut priv_bytes = [0u8; KEY_LEN];
        for (i, b) in priv_bytes.iter_mut().enumerate() {
            *b = seed.wrapping_mul(31).wrapping_add(i as u8);
        }
        NodeKeypair::from_private_bytes(priv_bytes).unwrap()
    }

    fn sample_heartbeat() -> WorkerMessage {
        WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 42,
            active_tasks: 1,
            cpu_usage_pct: 12.5,
            ram_available_mb: 1024,
        }
    }

    #[test]
    fn keypair_debug_redacts_private_key_and_computes_member_id_and_fingerprint() {
        let kp = keypair(7);
        let priv_hex = kp.private_hex();
        let pub_hex = kp.public_key_hex();
        assert_eq!(priv_hex.len(), 64);
        assert_eq!(pub_hex.len(), 64);
        assert_ne!(priv_hex, pub_hex);

        let dbg = format!("{kp:?}");
        assert_eq!(dbg, "NodeKeypair(<redacted>)");
        assert!(!dbg.contains(&priv_hex));

        let member_id = kp.member_id();
        assert_eq!(member_id.len(), 16);
        assert!(member_id.chars().all(|c| c.is_ascii_hexdigit()));
        let fp = kp.fingerprint();
        assert_eq!(fp.len(), 19);
        assert_eq!(fp.replace('-', ""), member_id);
    }

    #[test]
    fn keypair_file_save_and_load_preserves_private_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("node.key");
        let (created, was_new) = NodeKeypair::load_or_generate_file(&path).unwrap();
        assert!(was_new);
        let (reloaded, was_new2) = NodeKeypair::load_or_generate_file(&path).unwrap();
        assert!(!was_new2);
        assert_eq!(created, reloaded);
        assert_eq!(created.public_key(), reloaded.public_key());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "node.key must be mode 0600");
        }
    }

    #[tokio::test]
    async fn v3_round_trip_and_record_layer_works_both_directions_after_split() {
        let master_kp = keypair(1);
        let worker_kp = keypair(2);
        let allowed_pub = *worker_kp.public_key();
        let master_pub = *master_kp.public_key();

        let (mut client, mut server) = pair();
        let (client_res, server_res) = tokio::join!(
            v3_client_handshake(
                &mut client,
                &worker_kp,
                &master_pub,
                HandshakePurpose::Member
            ),
            v3_server_handshake(&mut server, &master_kp, |pk| *pk == allowed_pub)
        );
        let client_session_key = client_res.expect("client v3 handshake");
        let pending = server_res.expect("server v3 handshake");
        assert_eq!(pending.peer_public_key(), &allowed_pub);
        assert_eq!(&client_session_key, pending.session_key());

        let pending_dbg = format!("{pending:?}");
        assert!(
            !pending_dbg.contains(&hex_encode(&client_session_key)),
            "PendingV3Session Debug must redact session_key: {pending_dbg}"
        );

        client.enable_encryption(&client_session_key, EndpointRole::Client);

        // Client -> Server first encrypted record (confirms authentication) + Server -> Client reply
        let hb = sample_heartbeat();
        let hb_clone = hb.clone();
        let client_io = async move {
            client.send_msg(&hb_clone).await.unwrap();
            let reply: MasterMessage = client.recv_msg().await.unwrap().unwrap();
            reply
        };
        let server_io = async move {
            let (peer, msg, _codec): (AuthenticatedV3Peer, WorkerMessage, WireCodec) =
                v3_server_confirm_first_record(&mut server, pending)
                    .await
                    .expect("first transport record must decrypt");
            assert_eq!(peer.public_key, allowed_pub);
            assert_eq!(peer.member_id, allowed_pub.member_id());
            assert_eq!(msg, hb);
            let ack = MasterMessage::HeartbeatAck { timestamp: 99 };
            server.send_msg(&ack).await.unwrap();
            ack
        };
        let (got_reply, sent_ack) = tokio::join!(client_io, server_io);
        assert_eq!(got_reply, sent_ack);
    }

    #[tokio::test]
    async fn replayed_ik_message_1_never_becomes_an_authenticated_session() {
        let master_kp = keypair(10);
        let worker_kp = keypair(11);
        let allowed_pub = *worker_kp.public_key();
        let master_pub = *master_kp.public_key();

        // 1. Run a genuine session through a recording proxy (or inspect raw frames) to capture
        // the exact cleartext HELLO, Noise IK message 1, and the first encrypted 0x04 record.
        let (mut real_client, mut proxy_c) = pair();
        let (mut proxy_s, mut real_server) = pair();

        let worker_kp_clone = worker_kp.clone();
        let client_task = tokio::spawn(async move {
            let key = v3_client_handshake(
                &mut real_client,
                &worker_kp_clone,
                &master_pub,
                HandshakePurpose::Member,
            )
            .await
            .unwrap();
            real_client.enable_encryption(&key, EndpointRole::Client);
            real_client.send_msg(&sample_heartbeat()).await.unwrap();
        });

        let master_kp_clone = master_kp.clone();
        let server_task = tokio::spawn(async move {
            v3_server_accept_authenticated::<_, WorkerMessage, _>(
                &mut real_server,
                &master_kp_clone,
                |pk| *pk == allowed_pub,
            )
            .await
            .unwrap()
        });

        let captured_hello = proxy_c.recv_raw_frame().await.unwrap().unwrap();
        let captured_msg1 = proxy_c.recv_raw_frame().await.unwrap().unwrap();
        proxy_s
            .send_raw_frame(captured_hello.clone())
            .await
            .unwrap();
        proxy_s.send_raw_frame(captured_msg1.clone()).await.unwrap();

        let captured_msg2 = proxy_s.recv_raw_frame().await.unwrap().unwrap();
        proxy_c.send_raw_frame(captured_msg2).await.unwrap();

        let captured_encrypted_record = proxy_c.recv_raw_frame().await.unwrap().unwrap();
        assert_eq!(captured_encrypted_record[0], WIRE_FORMAT_ENCRYPTED);
        proxy_s
            .send_raw_frame(captured_encrypted_record.clone())
            .await
            .unwrap();

        client_task.await.unwrap();
        let (verified, _, _) = server_task.await.unwrap();
        assert_eq!(verified.public_key, allowed_pub);

        // 2. Replay (captured_hello, captured_msg1, captured_encrypted_record) on a new connection.
        // Because the responder generates a fresh ephemeral key in message 2, the Split() keys
        // differ and captured_encrypted_record MUST fail AEAD decryption.
        let (mut attacker, mut server2) = pair();
        let master_kp2 = master_kp.clone();
        let srv2 = tokio::spawn(async move {
            v3_server_accept_authenticated::<_, WorkerMessage, _>(&mut server2, &master_kp2, |pk| {
                *pk == allowed_pub
            })
            .await
        });

        attacker
            .send_raw_frame(captured_hello.clone())
            .await
            .unwrap();
        attacker
            .send_raw_frame(captured_msg1.clone())
            .await
            .unwrap();
        let _msg2 = attacker.recv_raw_frame().await.unwrap().unwrap();
        attacker
            .send_raw_frame(captured_encrypted_record)
            .await
            .unwrap();

        let replay_res = srv2.await.unwrap();
        assert!(
            replay_res.is_err(),
            "replayed IK message 1 + replayed record must never become an authenticated session: {replay_res:?}"
        );

        // 3. Replay (captured_hello, captured_msg1) and then close the connection without a valid record.
        let (mut attacker_eof, mut server3) = pair();
        let master_kp3 = master_kp.clone();
        let srv3 = tokio::spawn(async move {
            v3_server_accept_authenticated::<_, WorkerMessage, _>(&mut server3, &master_kp3, |pk| {
                *pk == allowed_pub
            })
            .await
        });
        attacker_eof.send_raw_frame(captured_hello).await.unwrap();
        attacker_eof.send_raw_frame(captured_msg1).await.unwrap();
        let _msg2 = attacker_eof.recv_raw_frame().await.unwrap().unwrap();
        drop(attacker_eof);

        let eof_res = srv3.await.unwrap();
        assert!(
            matches!(eof_res, Err(GridError::ConnectionClosed)),
            "replayed IK message 1 followed by EOF must not authenticate: {eof_res:?}"
        );
    }

    #[tokio::test]
    async fn wrong_pinned_master_key_is_refused() {
        let real_master_kp = keypair(20);
        let wrong_master_kp = keypair(21);
        let worker_kp = keypair(22);
        let allowed_pub = *worker_kp.public_key();
        let wrong_master_pub = *wrong_master_kp.public_key();

        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            v3_server_handshake(&mut server, &real_master_kp, |pk| *pk == allowed_pub).await
        });
        let client_err = v3_client_handshake(
            &mut client,
            &worker_kp,
            &wrong_master_pub,
            HandshakePurpose::Member,
        )
        .await
        .expect_err("client with wrong pinned master key must be refused");

        let srv_err = srv
            .await
            .unwrap()
            .expect_err("server must refuse message 1 encrypted to wrong master key");
        assert!(
            matches!(client_err, GridError::AuthFailed(_)),
            "{client_err:?}"
        );
        assert!(
            client_err.to_string().contains("master-key")
                || client_err.to_string().contains("public key"),
            "error must clearly mention master key mismatch: {client_err}"
        );
        assert!(matches!(srv_err, GridError::AuthFailed(_)), "{srv_err:?}");
    }

    #[tokio::test]
    async fn non_member_is_refused() {
        let master_kp = keypair(30);
        let enrolled_kp = keypair(31);
        let stranger_kp = keypair(32);
        let enrolled_pub = *enrolled_kp.public_key();
        let master_pub = *master_kp.public_key();

        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            v3_server_handshake(&mut server, &master_kp, |pk| *pk == enrolled_pub).await
        });
        let client_err = v3_client_handshake(
            &mut client,
            &stranger_kp,
            &master_pub,
            HandshakePurpose::Member,
        )
        .await
        .expect_err("non-member must be refused");
        let srv_err = srv
            .await
            .unwrap()
            .expect_err("server must refuse non-member");
        assert!(
            matches!(client_err, GridError::AuthFailed(_)),
            "{client_err:?}"
        );
        assert!(matches!(srv_err, GridError::AuthFailed(_)), "{srv_err:?}");
    }

    #[tokio::test]
    async fn pre_auth_frame_over_1_kib_is_refused() {
        let master_kp = keypair(40);
        let worker_kp = keypair(41);
        let allowed_pub = *worker_kp.public_key();

        // 1. Oversized first frame (HELLO > 1 KiB)
        let (mut client1, mut server1) = pair();
        let master_kp1 = master_kp.clone();
        let srv1 = tokio::spawn(async move {
            v3_server_handshake(&mut server1, &master_kp1, |pk| *pk == allowed_pub).await
        });
        let oversized = Bytes::from(vec![WIRE_FORMAT_AUTH; PRE_AUTH_MAX_FRAME + 1]);
        client1.send_raw_frame(oversized).await.unwrap();
        let err1 = srv1
            .await
            .unwrap()
            .expect_err("oversized HELLO must be refused");
        assert!(matches!(err1, GridError::AuthFailed(_)), "{err1:?}");
        assert!(
            err1.to_string().contains(&PRE_AUTH_MAX_FRAME.to_string()),
            "{err1}"
        );

        // 2. Valid HELLO followed by oversized IK message 1 (> 1 KiB)
        let (mut client2, mut server2) = pair();
        let master_kp2 = master_kp.clone();
        let srv2 = tokio::spawn(async move {
            v3_server_handshake(&mut server2, &master_kp2, |pk| *pk == allowed_pub).await
        });
        let hello = build_v3_hello_frame(PROTOCOL_VERSION_CURRENT, HandshakePurpose::Member);
        client2.send_raw_frame(hello).await.unwrap();
        let oversized_msg1 = Bytes::from(vec![WIRE_FORMAT_AUTH; PRE_AUTH_MAX_FRAME + 1]);
        client2.send_raw_frame(oversized_msg1).await.unwrap();
        let err2 = srv2
            .await
            .unwrap()
            .expect_err("oversized IK message 1 must be refused");
        assert!(matches!(err2, GridError::AuthFailed(_)), "{err2:?}");
        assert!(
            err2.to_string().contains(&PRE_AUTH_MAX_FRAME.to_string()),
            "{err2}"
        );
    }

    #[tokio::test]
    async fn protocol_version_mismatch_is_reported_explicitly() {
        let master_kp = keypair(50);
        let worker_kp = keypair(51);
        let allowed_pub = *worker_kp.public_key();
        let master_pub = *master_kp.public_key();

        let (mut client, mut server) = pair();
        let srv = tokio::spawn(async move {
            v3_server_handshake(&mut server, &master_kp, |pk| *pk == allowed_pub).await
        });
        let incompatible_ver = PROTOCOL_VERSION_CURRENT + 10;
        let client_err = v3_client_handshake_with_version(
            &mut client,
            &worker_kp,
            &master_pub,
            HandshakePurpose::Member,
            incompatible_ver,
        )
        .await
        .expect_err("incompatible protocol version must fail");
        let srv_err = srv
            .await
            .unwrap()
            .expect_err("server must refuse incompatible protocol version");

        match &client_err {
            GridError::IncompatibleVersion(msg) => {
                assert!(
                    msg.contains(&format!("Incompatible protocol version {incompatible_ver}")),
                    "{msg}"
                );
            }
            other => panic!("expected GridError::IncompatibleVersion on client, got {other:?}"),
        }
        match &srv_err {
            GridError::IncompatibleVersion(msg) => {
                assert!(
                    msg.contains(&format!("Incompatible protocol version {incompatible_ver}")),
                    "{msg}"
                );
            }
            other => panic!("expected GridError::IncompatibleVersion on server, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversized_first_record_after_valid_or_replayed_msg1_is_refused_without_allocation() {
        use tokio::io::AsyncWriteExt;

        let master_kp = keypair(60);
        let worker_kp = keypair(61);
        let allowed_pub = *worker_kp.public_key();

        let (mut raw_client, raw_server) = tokio::io::duplex(64 * 1024);
        let mut server = MessageTransport::new(raw_server);

        let hello = build_v3_hello_frame(PROTOCOL_VERSION_CURRENT, HandshakePurpose::Member);
        let params: snow::params::NoiseParams = NOISE_PATTERN.parse().unwrap();
        let mut init_hs = snow::Builder::new(params)
            .local_private_key(worker_kp.private_bytes())
            .and_then(|b| b.remote_public_key(master_kp.public_key().as_bytes()))
            .and_then(|b| b.prologue(&hello))
            .and_then(|b| b.build_initiator())
            .unwrap();
        let mut msg1_buf = [0u8; PRE_AUTH_MAX_FRAME];
        let msg1_len = init_hs.write_message(&[], &mut msg1_buf).unwrap();
        let msg1 = auth_frame(KIND_NOISE_MSG1, &[&msg1_buf[..msg1_len]]);

        // Write length-prefixed HELLO and valid/replayed IK message 1, followed ONLY by a 4-byte
        // length header announcing a 1 MiB first encrypted record (without sending 1 MiB of data).
        for frame in [&hello, &msg1] {
            raw_client
                .write_all(&(frame.len() as u32).to_be_bytes())
                .await
                .unwrap();
            raw_client.write_all(frame).await.unwrap();
        }
        let announced_len: u32 = 1024 * 1024;
        raw_client
            .write_all(&announced_len.to_be_bytes())
            .await
            .unwrap();
        raw_client.flush().await.unwrap();

        let pending = v3_server_handshake(&mut server, &master_kp, |pk| *pk == allowed_pub)
            .await
            .expect("valid/replayed msg1 reaches pending confirmation");

        let confirm_err = v3_server_confirm_first_record::<_, WorkerMessage>(&mut server, pending)
            .await
            .expect_err("oversized first encrypted frame must be refused immediately");

        assert!(
            confirm_err
                .to_string()
                .contains(&V3_FIRST_RECORD_MAX_FRAME.to_string()),
            "expected FrameTooLarge with max {V3_FIRST_RECORD_MAX_FRAME}, got: {confirm_err}"
        );
        assert!(
            server.read_buffer_capacity() < V3_FIRST_RECORD_MAX_FRAME,
            "server read buffer must not allocate the announced 1 MiB frame (capacity = {})",
            server.read_buffer_capacity()
        );

        // Once the first valid encrypted record decrypts and the peer is authenticated, the normal
        // MAX_FRAME_SIZE limit is restored so larger records (> 64 KiB) succeed.
        let (c2, s2) = tokio::io::duplex(512 * 1024);
        let mut client2 = MessageTransport::new(c2);
        let mut server2 = MessageTransport::new(s2);
        let master_pub = *master_kp.public_key();
        let master_kp2 = master_kp.clone();
        let srv_task = tokio::spawn(async move {
            let (peer, first, _): (_, WorkerMessage, _) =
                v3_server_accept_authenticated(&mut server2, &master_kp2, |pk| *pk == allowed_pub)
                    .await
                    .unwrap();
            let second = server2
                .recv_msg::<WorkerMessage>()
                .await
                .unwrap()
                .expect("second larger record must succeed after auth confirmation");
            (peer, first, second)
        });
        let sk = v3_client_handshake(
            &mut client2,
            &worker_kp,
            &master_pub,
            HandshakePurpose::Member,
        )
        .await
        .unwrap();
        client2.enable_encryption(&sk, EndpointRole::Client);
        client2.send_msg(&sample_heartbeat()).await.unwrap();
        let large_msg = WorkerMessage::TaskResult {
            worker_id: Uuid::nil(),
            task_id: crate::task::TaskId(Uuid::nil()),
            exit_code: 0,
            stdout: crate::task::Bytes(Bytes::from(vec![
                b'x';
                V3_FIRST_RECORD_MAX_FRAME + 16 * 1024
            ])),
            stderr: crate::task::Bytes(Bytes::new()),
            execution_time_ms: 1,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: vec![],
            outputs: vec![],
        };
        client2.send_msg(&large_msg).await.unwrap();
        let (peer, _, second) = srv_task.await.unwrap();
        assert_eq!(peer.public_key, allowed_pub);
        match second {
            WorkerMessage::TaskResult { stdout, .. } => {
                assert_eq!(stdout.len(), V3_FIRST_RECORD_MAX_FRAME + 16 * 1024);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
