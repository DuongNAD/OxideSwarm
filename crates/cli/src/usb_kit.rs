//! USB setup kit creation and encrypted `invite.oxs` handling (design 0053, USB-1a).
//!
//! Creates a self-contained setup kit in `<DIR>/OxideSwarm` holding:
//! - encrypted single-kit multi-use invite (`invite.oxs`: Argon2id + ChaCha20-Poly1305 AEAD);
//! - non-secret kit metadata (`kit.json`);
//! - release archives verified against `SHA256SUMS` (local `--archives` or HTTPS `--download`);
//! - installers, uninstallers, and updaters for macOS, Windows, and Linux.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, Metadata, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use argon2::{Algorithm, Argon2, Params, Version};
use clap::{Args, Subcommand};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::digest::{Context as DigestContext, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use url::Url;
use zeroize::{Zeroize, Zeroizing};

use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::MemberRole;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

use crate::config::{ConfigFile, SecretSection};
use crate::connect::{connect_master, report_master_failure, with_client_relay_url};
use crate::members_cmd::{
    cfg_client_insecure, finalize_invite_targets, format_roles, parse_duration_secs,
    parse_roles_csv, resolve_admin_connect_addr, resolve_local_invite_targets, InviteArgs,
    InviteToken,
};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

/// Magic prefix identifying `invite.oxs` format version 1.
pub const INVITE_OXS_MAGIC: &[u8; 8] = b"OXSINV01";
/// Fixed binary header size (magic + u32 m_kib + u32 t + u32 p + 16B salt + 12B nonce).
pub const INVITE_OXS_HEADER_LEN: usize = 48;
/// Default Argon2id memory parameter (64 MiB in KiB).
pub const INVITE_OXS_DEFAULT_M_KIB: u32 = 65_536;
/// Default Argon2id iterations.
pub const INVITE_OXS_DEFAULT_T_COST: u32 = 3;
/// Default Argon2id parallelism lanes.
pub const INVITE_OXS_DEFAULT_P_COST: u32 = 1;
/// Minimum accepted Argon2id memory parameter in KiB.
pub const INVITE_OXS_MIN_M_KIB: u32 = 8;
/// Maximum accepted Argon2id memory parameter in KiB (256 MiB, design 0053 D1.4).
pub const INVITE_OXS_MAX_M_KIB: u32 = 262_144;
/// Maximum accepted Argon2id passes (design 0053 D1.4).
pub const INVITE_OXS_MAX_T_COST: u32 = 10;
/// Maximum accepted Argon2id lanes (design 0053 D1.4).
pub const INVITE_OXS_MAX_P_COST: u32 = 4;
/// Maximum allowed `invite.oxs` file length in bytes.
pub const INVITE_OXS_MAX_FILE_BYTES: usize = 16_384;
/// Poly1305 authentication tag length.
const AEAD_TAG_LEN: usize = 16;

/// Maximum invite lifetime allowed for USB setup kits (30 days in seconds).
pub const USB_MAX_EXPIRES_SECS: u64 = 30 * 86_400;
/// Maximum invite lifetime allowed when `--no-approval` is set (48 hours in seconds).
pub const USB_NO_APPROVAL_MAX_EXPIRES_SECS: u64 = 48 * 3600;
/// Minimum user-supplied passphrase length (`--passphrase-stdin`).
pub const MIN_USER_PASSPHRASE_CHARS: usize = 16;
/// Maximum stdin passphrase length accepted in bytes.
const MAX_PASSPHRASE_STDIN_BYTES: usize = 1024;
/// Default GitHub release download base URL prefix.
const DEFAULT_RELEASE_BASE_PREFIX: &str =
    "https://github.com/DuongNAD/OxideSwarm/releases/download";
/// Optional env var overriding the HTTPS base URL in tests (`--download` or online `SHA256SUMS`).
const ONLINE_DOWNLOAD_BASE_ENV: &str = "OXIDESWARM_USB_DOWNLOAD_BASE_URL";
/// Optional env var supplying an explicit HTTPS URL for online `SHA256SUMS` verification (`"off"` disables).
const ONLINE_SUMS_URL_ENV: &str = "OXIDESWARM_USB_ONLINE_SUMS_URL";
/// Maximum downloaded `SHA256SUMS` size (1 MiB).
const MAX_SUMS_DOWNLOAD_BYTES: usize = 1024 * 1024;
/// Maximum downloaded archive size (256 MiB).
const MAX_ARCHIVE_DOWNLOAD_BYTES: usize = 256 * 1024 * 1024;
/// Maximum HTTP redirect hops followed for HTTPS downloads.
const MAX_HTTPS_REDIRECTS: usize = 5;

/// Base release archive suffixes bundled onto every setup kit.
const BASE_TARGETS: &[&str] = &[
    "aarch64-apple-darwin.tar.gz",
    "x86_64-apple-darwin.tar.gz",
    "x86_64-pc-windows-msvc.zip",
    "x86_64-unknown-linux-gnu.tar.gz",
];

struct EmbeddedAsset {
    dest_name: &'static str,
    #[cfg_attr(not(unix), allow(dead_code))]
    executable: bool,
    bytes: &'static [u8],
}

const EMBEDDED_ASSETS: &[EmbeddedAsset] = &[
    EmbeddedAsset {
        dest_name: "install.sh",
        executable: true,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/install.sh"
        )),
    },
    EmbeddedAsset {
        dest_name: "uninstall.sh",
        executable: true,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/uninstall.sh"
        )),
    },
    EmbeddedAsset {
        dest_name: "install.ps1",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/install.ps1"
        )),
    },
    EmbeddedAsset {
        dest_name: "uninstall.ps1",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/uninstall.ps1"
        )),
    },
    EmbeddedAsset {
        dest_name: "Install OxideSwarm.command",
        executable: true,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/Install OxideSwarm.command"
        )),
    },
    EmbeddedAsset {
        dest_name: "Uninstall OxideSwarm.command",
        executable: true,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/Uninstall OxideSwarm.command"
        )),
    },
    EmbeddedAsset {
        dest_name: "Update OxideSwarm.command",
        executable: true,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/Update OxideSwarm.command"
        )),
    },
    EmbeddedAsset {
        dest_name: "Install OxideSwarm.cmd",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/Install OxideSwarm.cmd"
        )),
    },
    EmbeddedAsset {
        dest_name: "Uninstall OxideSwarm.cmd",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/Uninstall OxideSwarm.cmd"
        )),
    },
    EmbeddedAsset {
        dest_name: "Update OxideSwarm.cmd",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/Update OxideSwarm.cmd"
        )),
    },
    EmbeddedAsset {
        dest_name: "revoke-isolation-acls.ps1",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../windows/revoke-isolation-acls.ps1"
        )),
    },
    EmbeddedAsset {
        dest_name: "README.txt",
        executable: false,
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/usb/KIT_README.txt"
        )),
    },
];

/// CLI arguments for `oxideswarm usb`.
#[derive(Args, Debug)]
pub struct UsbCliArgs {
    #[command(subcommand)]
    pub command: UsbCommand,
}

/// Subcommands under `oxideswarm usb`.
#[derive(Subcommand, Debug)]
pub enum UsbCommand {
    /// Write an encrypted USB setup kit into `<DIR>/OxideSwarm`
    Create(UsbCreateArgs),
}

/// Arguments for `oxideswarm usb create <DIR>`.
#[derive(Args, Debug)]
pub struct UsbCreateArgs {
    #[arg(
        value_name = "DIR",
        help = "Destination folder (for example the USB mount point); writes `<DIR>/OxideSwarm`"
    )]
    pub dir: PathBuf,

    #[arg(
        long,
        alias = "uses",
        value_name = "N",
        default_value_t = 1,
        help = "Maximum number of machines that can enroll with this kit (default: 1)"
    )]
    pub machines: u32,

    #[arg(
        long,
        value_name = "DURATION",
        help = "How long until the kit invite expires, up to 30d (default: 7d, or 48h with --no-approval)"
    )]
    pub expires: Option<String>,

    #[arg(
        long,
        value_name = "ROLES",
        default_value = "worker",
        help = "Comma-separated roles granted by this kit (worker or worker,client; admin is refused)"
    )]
    pub roles: String,

    #[arg(
        long,
        help = "Enable shared-folder workspaces in the kit installers when Cargo is present"
    )]
    pub workspaces: bool,

    #[arg(long, help = "Also bundle the `-gpu` release archives on the stick")]
    pub gpu: bool,

    #[arg(
        long,
        alias = "from-dir",
        value_name = "DIR",
        conflicts_with = "download",
        help = "Read release archives and SHA256SUMS from this local folder instead of downloading"
    )]
    pub archives: Option<PathBuf>,

    #[arg(
        long,
        help = "Download release archives and SHA256SUMS from the matching GitHub release over HTTPS"
    )]
    pub download: bool,

    #[arg(
        long = "release",
        value_name = "VERSION",
        help = "Release version to package in the kit (default: running binary version)"
    )]
    pub release: Option<String>,

    #[arg(
        long,
        help = "Do not add automatic login startup when installing from this kit"
    )]
    pub no_autostart: bool,

    #[arg(
        long,
        value_name = "MODE",
        value_parser = ["off", "fs"],
        help = "Default worker filesystem isolation mode recorded on the kit (`off` or `fs`)"
    )]
    pub isolation: Option<String>,

    #[arg(
        long,
        help = "Allow machines with the stick and passphrase to enroll without per-machine approval (warns; caps expiry at 48h)"
    )]
    pub no_approval: bool,

    #[arg(
        long,
        help = "Read the encryption passphrase from standard input instead of generating 4 words (at least 16 characters)"
    )]
    pub passphrase_stdin: bool,

    #[arg(long, help = "Replace an existing `<DIR>/OxideSwarm` folder")]
    pub replace: bool,

    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address target machines use to reach the master's grid port (also used for admin RPC)"
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
        help = "Also embed the P2P ticket stored in this file (default: master's active P2P ticket, if any)"
    )]
    pub p2p_ticket_file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "URL",
        help = "HTTPS URL of a custom iroh relay server for P2P connections"
    )]
    pub relay_url: Option<String>,

    #[arg(
        long,
        help = "Allow connecting to a remote master without a cluster secret or OXS-AUTH v3 key"
    )]
    pub insecure: bool,
}

/// Validated `invite.oxs` header metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InviteOxsHeader {
    pub m_cost_kib: u32,
    pub t_cost: u32,
    pub p_cost: u32,
    pub salt: [u8; 16],
    pub nonce: [u8; 12],
}

/// Non-secret kit metadata written to `<DIR>/OxideSwarm/kit.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KitMetadata {
    pub kit_format: u32,
    pub version: String,
    pub created_at: u64,
    pub expires_at: u64,
    pub machines: u32,
    pub roles: Vec<String>,
    pub workspaces: bool,
    pub gpu: bool,
    pub require_approval: bool,
    pub invite_id: String,
}

/// Entrypoint for `oxideswarm usb ...`.
pub async fn run_usb(
    args: UsbCliArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    match args.command {
        UsbCommand::Create(create_args) => {
            run_usb_create(create_args, config_file, secret_opts).await
        }
    }
}

/// Parses and validates `invite.oxs` size, magic, and Argon2id parameter bounds BEFORE any KDF work.
pub fn validate_invite_oxs_header(bytes: &[u8]) -> GridResult<InviteOxsHeader> {
    if bytes.len() > INVITE_OXS_MAX_FILE_BYTES {
        return Err(GridError::Config(format!(
            "invite.oxs exceeds maximum allowed size ({} bytes > {INVITE_OXS_MAX_FILE_BYTES})",
            bytes.len()
        )));
    }
    if bytes.len() <= INVITE_OXS_HEADER_LEN + AEAD_TAG_LEN {
        return Err(GridError::Config(
            "invite.oxs is truncated or too short to be valid".into(),
        ));
    }
    if &bytes[..8] != INVITE_OXS_MAGIC {
        return Err(GridError::Config(
            "invite.oxs has an unrecognized header (expected OXSINV01)".into(),
        ));
    }
    let m_cost_kib = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
    let t_cost = u32::from_le_bytes(bytes[12..16].try_into().expect("4 bytes"));
    let p_cost = u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes"));
    if !(INVITE_OXS_MIN_M_KIB..=INVITE_OXS_MAX_M_KIB).contains(&m_cost_kib) {
        return Err(GridError::Config(format!(
            "invite.oxs Argon2id memory cost {m_cost_kib} KiB is outside safe bounds ({INVITE_OXS_MIN_M_KIB}..={INVITE_OXS_MAX_M_KIB})"
        )));
    }
    if !(1..=INVITE_OXS_MAX_T_COST).contains(&t_cost) {
        return Err(GridError::Config(format!(
            "invite.oxs Argon2id time cost {t_cost} is outside safe bounds (1..={INVITE_OXS_MAX_T_COST})"
        )));
    }
    if !(1..=INVITE_OXS_MAX_P_COST).contains(&p_cost) {
        return Err(GridError::Config(format!(
            "invite.oxs Argon2id parallelism {p_cost} is outside safe bounds (1..={INVITE_OXS_MAX_P_COST})"
        )));
    }
    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 12];
    salt.copy_from_slice(&bytes[20..36]);
    nonce.copy_from_slice(&bytes[36..48]);
    Ok(InviteOxsHeader {
        m_cost_kib,
        t_cost,
        p_cost,
        salt,
        nonce,
    })
}

fn derive_invite_oxs_key(
    passphrase: &str,
    header: &InviteOxsHeader,
) -> GridResult<Zeroizing<[u8; 32]>> {
    let params = Params::new(header.m_cost_kib, header.t_cost, header.p_cost, Some(32))
        .map_err(|e| GridError::AuthFailed(format!("invalid Argon2id parameters: {e}")))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = Zeroizing::new([0u8; 32]);
    argon2
        .hash_password_into(passphrase.as_bytes(), &header.salt, &mut *key)
        .map_err(|e| GridError::AuthFailed(format!("Argon2id key derivation failed: {e}")))?;
    Ok(key)
}

/// Encrypts an `oxs1-invite:...` string with Argon2id (64 MiB, t=3, p=1) + ChaCha20-Poly1305 AEAD.
pub fn encrypt_invite_oxs(plaintext_invite: &str, passphrase: &str) -> GridResult<Vec<u8>> {
    encrypt_invite_oxs_with_params(
        plaintext_invite,
        passphrase,
        INVITE_OXS_DEFAULT_M_KIB,
        INVITE_OXS_DEFAULT_T_COST,
        INVITE_OXS_DEFAULT_P_COST,
    )
}

pub(crate) fn encrypt_invite_oxs_with_params(
    plaintext_invite: &str,
    passphrase: &str,
    m_cost_kib: u32,
    t_cost: u32,
    p_cost: u32,
) -> GridResult<Vec<u8>> {
    if plaintext_invite.is_empty() {
        return Err(GridError::Config("invite payload cannot be empty".into()));
    }
    if !(INVITE_OXS_MIN_M_KIB..=INVITE_OXS_MAX_M_KIB).contains(&m_cost_kib)
        || !(1..=INVITE_OXS_MAX_T_COST).contains(&t_cost)
        || !(1..=INVITE_OXS_MAX_P_COST).contains(&p_cost)
    {
        return Err(GridError::Config(
            "Argon2id parameters outside allowed bounds".into(),
        ));
    }

    let rng = SystemRandom::new();
    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 12];
    rng.fill(&mut salt)
        .map_err(|_| GridError::AuthFailed("OS randomness failure for invite.oxs salt".into()))?;
    rng.fill(&mut nonce)
        .map_err(|_| GridError::AuthFailed("OS randomness failure for invite.oxs nonce".into()))?;

    let header = InviteOxsHeader {
        m_cost_kib,
        t_cost,
        p_cost,
        salt,
        nonce,
    };
    let mut header_bytes = Vec::with_capacity(INVITE_OXS_HEADER_LEN);
    header_bytes.extend_from_slice(INVITE_OXS_MAGIC);
    header_bytes.extend_from_slice(&m_cost_kib.to_le_bytes());
    header_bytes.extend_from_slice(&t_cost.to_le_bytes());
    header_bytes.extend_from_slice(&p_cost.to_le_bytes());
    header_bytes.extend_from_slice(&salt);
    header_bytes.extend_from_slice(&nonce);
    debug_assert_eq!(header_bytes.len(), INVITE_OXS_HEADER_LEN);

    let key_bytes = derive_invite_oxs_key(passphrase, &header)?;
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, &*key_bytes)
        .map_err(|_| GridError::AuthFailed("failed to initialize ChaCha20-Poly1305 key".into()))?;
    let aead_key = LessSafeKey::new(unbound);

    let mut payload = plaintext_invite.as_bytes().to_vec();
    if aead_key
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(&header_bytes[..]),
            &mut payload,
        )
        .is_err()
    {
        payload.zeroize();
        return Err(GridError::AuthFailed(
            "failed to encrypt invite.oxs payload".into(),
        ));
    }

    let mut out = Vec::with_capacity(header_bytes.len() + payload.len());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(&payload);
    if out.len() > INVITE_OXS_MAX_FILE_BYTES {
        return Err(GridError::Config(
            "encrypted invite.oxs exceeds maximum size".into(),
        ));
    }
    Ok(out)
}

/// Validates, derives the Argon2id key, and decrypts `invite.oxs` in memory.
pub fn decrypt_invite_oxs(bytes: &[u8], passphrase: &str) -> GridResult<Zeroizing<String>> {
    let header = validate_invite_oxs_header(bytes)?;
    let key_bytes = derive_invite_oxs_key(passphrase, &header)?;
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, &*key_bytes)
        .map_err(|_| GridError::AuthFailed("failed to initialize ChaCha20-Poly1305 key".into()))?;
    let aead_key = LessSafeKey::new(unbound);

    let (header_bytes, ciphertext) = bytes.split_at(INVITE_OXS_HEADER_LEN);
    let mut buffer = Zeroizing::new(ciphertext.to_vec());
    let plain_len = {
        let plain = aead_key
            .open_in_place(
                Nonce::assume_unique_for_key(header.nonce),
                Aad::from(header_bytes),
                &mut buffer,
            )
            .map_err(|_| {
                GridError::AuthFailed(
                    "wrong passphrase or corrupted invite.oxs (authentication failed)".into(),
                )
            })?;
        plain.len()
    };
    buffer.truncate(plain_len);
    let text = String::from_utf8(buffer.to_vec()).map_err(|_| {
        GridError::AuthFailed("decrypted invite.oxs payload is not valid UTF-8".into())
    })?;
    Ok(Zeroizing::new(text))
}

/// Generates a 4-word BIP-0039 passphrase joined by hyphens (~44 bits of entropy).
pub fn generate_kit_passphrase() -> GridResult<Zeroizing<String>> {
    let words = crate::wordlist::random_words::<4>()?;
    Ok(Zeroizing::new(words.join("-")))
}

/// Validates and normalizes a user-supplied passphrase read from `--passphrase-stdin`.
pub fn parse_user_passphrase<R: Read>(mut reader: R) -> GridResult<Zeroizing<String>> {
    let mut raw = Zeroizing::new(Vec::with_capacity(128));
    let mut limited = (&mut reader).take((MAX_PASSPHRASE_STDIN_BYTES + 1) as u64);
    limited
        .read_to_end(&mut raw)
        .map_err(|e| GridError::Config(format!("failed to read passphrase from stdin: {e}")))?;
    if raw.len() > MAX_PASSPHRASE_STDIN_BYTES {
        return Err(GridError::Config(format!(
            "passphrase on stdin exceeds {MAX_PASSPHRASE_STDIN_BYTES} bytes"
        )));
    }
    let text = Zeroizing::new(String::from_utf8(raw.to_vec()).map_err(|_| {
        GridError::Config("passphrase read from standard input is not valid UTF-8".into())
    })?);
    let line = text
        .lines()
        .next()
        .unwrap_or("")
        .trim_end_matches(&['\r', '\n'][..]);
    if text
        .lines()
        .nth(1)
        .is_some_and(|extra| !extra.trim().is_empty())
    {
        return Err(GridError::Config(
            "stdin passed extra non-empty lines alongside the passphrase".into(),
        ));
    }
    if line.chars().count() < MIN_USER_PASSPHRASE_CHARS {
        return Err(GridError::Config(format!(
            "passphrase must be at least {MIN_USER_PASSPHRASE_CHARS} characters"
        )));
    }
    if line.chars().any(|c| c.is_control()) {
        return Err(GridError::Config(
            "passphrase contains disallowed control characters".into(),
        ));
    }
    Ok(Zeroizing::new(line.to_string()))
}

fn validate_release_version(raw: &str) -> GridResult<String> {
    let trimmed = raw.trim().trim_start_matches('v');
    if trimmed.is_empty()
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+'))
        || trimmed.contains("..")
    {
        return Err(GridError::Config(format!(
            "invalid release version string: '{raw}'"
        )));
    }
    Ok(trimmed.to_string())
}

/// Returns the list of archive filenames bundled for `version` (with optional `-gpu` builds).
pub fn archive_filenames(version: &str, include_gpu: bool) -> Vec<String> {
    let mut names = Vec::with_capacity(BASE_TARGETS.len() * if include_gpu { 2 } else { 1 });
    for target in BASE_TARGETS {
        names.push(format!("oxideswarm-{version}-{target}"));
    }
    if include_gpu {
        for target in BASE_TARGETS {
            let gpu_target = if let Some(stem) = target.strip_suffix(".tar.gz") {
                format!("{stem}-gpu.tar.gz")
            } else if let Some(stem) = target.strip_suffix(".zip") {
                format!("{stem}-gpu.zip")
            } else {
                format!("{target}-gpu")
            };
            names.push(format!("oxideswarm-{version}-{gpu_target}"));
        }
    }
    names
}

/// Parses standard `sha256sum` / `shasum -a 256` output (`<hex64>  <filename>` or `*<filename>`).
pub fn parse_sha256sums(sums_text: &str) -> GridResult<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for (idx, raw_line) in sums_text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let hash = parts.next().unwrap_or("");
        let rest = parts.next().unwrap_or("").trim_start();
        let file = rest.strip_prefix('*').unwrap_or(rest);
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) || file.is_empty() {
            return Err(GridError::Config(format!(
                "malformed SHA256SUMS line {}",
                idx + 1
            )));
        }
        if file.contains('/') || file.contains('\\') || file.contains("..") {
            return Err(GridError::Config(format!(
                "disallowed path in SHA256SUMS line {}: '{file}'",
                idx + 1
            )));
        }
        map.insert(file.to_string(), hash.to_ascii_lowercase());
    }
    if map.is_empty() {
        return Err(GridError::Config("SHA256SUMS contains no entries".into()));
    }
    Ok(map)
}

fn sha256_file_hex(path: &Path) -> GridResult<String> {
    let meta = fs::symlink_metadata(path).map_err(|e| {
        GridError::Io(std::io::Error::new(
            e.kind(),
            format!("cannot inspect '{}': {e}", path.display()),
        ))
    })?;
    if is_symlink_or_reparse(&meta) {
        return Err(GridError::Config(format!(
            "refusing symbolic link or junction at '{}'",
            path.display()
        )));
    }
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut ctx = DigestContext::new(&SHA256);
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
    }
    let digest = ctx.finish();
    let mut hex = String::with_capacity(64);
    for b in digest.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{b:02x}");
    }
    Ok(hex)
}

/// Verifies every required release archive inside `archives_dir` against `sums_text`.
pub fn verify_archives_against_sums(
    archives_dir: &Path,
    sums_text: &str,
    required_files: &[String],
) -> GridResult<Vec<(String, PathBuf)>> {
    let expected = parse_sha256sums(sums_text)?;
    let mut verified = Vec::with_capacity(required_files.len());
    for name in required_files {
        let want = expected
            .get(name)
            .ok_or_else(|| GridError::Config(format!("{name} is not listed in SHA256SUMS")))?;
        let path = archives_dir.join(name);
        if !path.exists() {
            return Err(GridError::Config(format!(
                "missing release archive '{}'",
                path.display()
            )));
        }
        let actual = sha256_file_hex(&path)?;
        if &actual != want {
            return Err(GridError::AuthFailed(format!(
                "SHA-256 checksum mismatch for {name}: expected {want}, got {actual}"
            )));
        }
        verified.push((name.clone(), path));
    }
    Ok(verified)
}

/// Verifies that local `SHA256SUMS` matches an online `SHA256SUMS` document for `required_files`.
pub fn verify_sums_against_online(
    local_sums: &str,
    online_sums: &str,
    required_files: &[String],
) -> GridResult<()> {
    let local = parse_sha256sums(local_sums)?;
    let online = parse_sha256sums(online_sums)?;
    for name in required_files {
        let l = local
            .get(name)
            .ok_or_else(|| GridError::Config(format!("{name} is missing from local SHA256SUMS")))?;
        let o = online.get(name).ok_or_else(|| {
            GridError::Config(format!("{name} is missing from online SHA256SUMS"))
        })?;
        if l != o {
            return Err(GridError::AuthFailed(format!(
                "SHA256SUMS mismatch vs online release for {name}: local {l}, online {o}"
            )));
        }
    }
    Ok(())
}

fn is_symlink_or_reparse(meta: &Metadata) -> bool {
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

fn ensure_no_reparse_in_tree(dir: &Path) -> GridResult<()> {
    let meta = fs::symlink_metadata(dir)?;
    if is_symlink_or_reparse(&meta) {
        return Err(GridError::Config(format!(
            "refusing symbolic link or reparse point at '{}'",
            dir.display()
        )));
    }
    if meta.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let cmeta = fs::symlink_metadata(&path)?;
            if is_symlink_or_reparse(&cmeta) {
                return Err(GridError::Config(format!(
                    "refusing symbolic link or reparse point inside kit folder: '{}'",
                    path.display()
                )));
            }
            if cmeta.is_dir() {
                ensure_no_reparse_in_tree(&path)?;
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct KitDestinationPlan {
    kit_dir: PathBuf,
    replace_existing: bool,
}

fn validate_kit_destination(dest: &Path, replace: bool) -> GridResult<KitDestinationPlan> {
    let dest_meta = fs::symlink_metadata(dest).map_err(|e| {
        GridError::Config(format!(
            "destination '{}' is not accessible: {e}",
            dest.display()
        ))
    })?;
    if is_symlink_or_reparse(&dest_meta) {
        return Err(GridError::Config(format!(
            "refusing destination '{}': path is a symbolic link or reparse point",
            dest.display()
        )));
    }
    if !dest_meta.is_dir() {
        return Err(GridError::Config(format!(
            "destination '{}' is not a directory",
            dest.display()
        )));
    }

    let kit_dir = dest.join("OxideSwarm");
    let replace_existing = match fs::symlink_metadata(&kit_dir) {
        Ok(meta) => {
            if is_symlink_or_reparse(&meta) {
                return Err(GridError::Config(format!(
                    "refusing '{}': path is a symbolic link or reparse point",
                    kit_dir.display()
                )));
            }
            if !meta.is_dir() {
                return Err(GridError::Config(format!(
                    "'{}' already exists and is not a directory",
                    kit_dir.display()
                )));
            }
            let is_empty = fs::read_dir(&kit_dir)?.next().is_none();
            if is_empty {
                true
            } else if !replace {
                return Err(GridError::Config(format!(
                    "'{}' already exists and is not empty (pass --replace to overwrite)",
                    kit_dir.display()
                )));
            } else {
                if !kit_dir.join("install.sh").is_file() && !kit_dir.join("kit.json").is_file() {
                    return Err(GridError::Config(format!(
                        "'{}' does not look like an OxideSwarm kit; refusing --replace",
                        kit_dir.display()
                    )));
                }
                ensure_no_reparse_in_tree(&kit_dir)?;
                true
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(GridError::Io(e)),
    };

    Ok(KitDestinationPlan {
        kit_dir,
        replace_existing,
    })
}

struct VerifiedArchives {
    sums_text: String,
    files: Vec<(String, PathBuf)>,
    _temp_hold: Option<TempCleanupDir>,
}

struct TempCleanupDir {
    path: PathBuf,
}

impl TempCleanupDir {
    fn create(parent: &Path, prefix: &str) -> GridResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        for attempt in 0..32u32 {
            let candidate = parent.join(format!(
                ".{prefix}.{}.{}.{}",
                std::process::id(),
                nanos,
                attempt
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700));
                    }
                    return Ok(Self { path: candidate });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(GridError::Io(e)),
            }
        }
        Err(GridError::Config(
            "failed to allocate unique temporary staging directory".into(),
        ))
    }
}

impl Drop for TempCleanupDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn validate_https_url(raw: &str) -> GridResult<Url> {
    let parsed = Url::parse(raw.trim())
        .map_err(|e| GridError::Config(format!("invalid HTTPS URL '{raw}': {e}")))?;
    if parsed.scheme() != "https" {
        return Err(GridError::Config(format!(
            "refusing non-HTTPS URL '{raw}': HTTPS is required"
        )));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(GridError::Config(
            "HTTPS URL must not embed credentials".into(),
        ));
    }
    if parsed.host_str().is_none() {
        return Err(GridError::Config(format!("HTTPS URL '{raw}' has no host")));
    }
    Ok(parsed)
}

fn build_tls_connector() -> GridResult<TlsConnector> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let cert_res = rustls_native_certs::load_native_certs();
    let mut store = RootCertStore::empty();
    for cert in cert_res.certs {
        let _ = store.add(cert);
    }
    if store.is_empty() {
        let err_detail = cert_res
            .errors
            .into_iter()
            .next()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no native CA roots found".to_string());
        return Err(GridError::AuthFailed(format!(
            "failed to load system TLS root certificates: {err_detail}"
        )));
    }
    let config = ClientConfig::builder()
        .with_root_certificates(store)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

async fn https_get_bytes(
    url_str: &str,
    max_bytes: usize,
    timeout: Duration,
) -> GridResult<Vec<u8>> {
    let connector = build_tls_connector()?;
    let mut current = validate_https_url(url_str)?;

    for _hop in 0..=MAX_HTTPS_REDIRECTS {
        let host = current
            .host_str()
            .ok_or_else(|| GridError::Config("URL missing host".into()))?
            .to_string();
        let port = current.port_or_known_default().unwrap_or(443);
        let host_header = if host.contains(':') {
            if current.port().is_some() {
                format!("[{host}]:{port}")
            } else {
                format!("[{host}]")
            }
        } else if current.port().is_some() {
            format!("{host}:{port}")
        } else {
            host.clone()
        };
        let path_query = match current.query() {
            Some(q) => format!("{}?{q}", current.path()),
            None => current.path().to_string(),
        };
        let path_query = if path_query.is_empty() {
            "/".to_string()
        } else {
            path_query
        };

        let server_name = ServerName::try_from(host.clone())
            .map_err(|_| GridError::Config(format!("invalid TLS server name '{host}'")))?;

        let response = tokio::time::timeout(timeout, async {
            let tcp = TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|e| GridError::ConnectionFailed(format!("TCP connect to {host}:{port}: {e}")))?;
            let mut tls = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| GridError::AuthFailed(format!("TLS handshake with {host}:{port}: {e}")))?;

            let req = format!(
                "GET {path_query} HTTP/1.1\r\nHost: {host_header}\r\nUser-Agent: oxideswarm-cli/{}\r\nAccept: */*\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
                env!("CARGO_PKG_VERSION")
            );
            tls.write_all(req.as_bytes()).await?;
            tls.flush().await?;

            // Read headers up to 64 KiB.
            let mut head_buf = Vec::with_capacity(4096);
            let mut chunk = [0u8; 4096];
            let header_end;
            loop {
                if let Some(pos) = head_buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    header_end = pos;
                    break;
                }
                if head_buf.len() >= 64 * 1024 {
                    return Err(GridError::Framing("HTTP response headers too large".into()));
                }
                let n = tls.read(&mut chunk).await?;
                if n == 0 {
                    return Err(GridError::Framing(
                        "connection closed before HTTP headers completed".into(),
                    ));
                }
                head_buf.extend_from_slice(&chunk[..n]);
            }

            let leftover = head_buf.split_off(header_end + 4);
            let head_str = std::str::from_utf8(&head_buf[..header_end])
                .map_err(|_| GridError::Framing("HTTP headers are not UTF-8".into()))?;
            let mut lines = head_str.split("\r\n");
            let status_line = lines.next().unwrap_or("");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    GridError::Framing(format!("malformed HTTP status line: '{status_line}'"))
                })?;

            let mut headers: HashMap<String, String> = HashMap::new();
            for line in lines {
                if let Some((k, v)) = line.split_once(':') {
                    headers
                        .entry(k.trim().to_ascii_lowercase())
                        .and_modify(|existing| {
                            existing.push_str(", ");
                            existing.push_str(v.trim());
                        })
                        .or_insert_with(|| v.trim().to_string());
                }
            }

            if matches!(status_code, 301 | 302 | 303 | 307 | 308) {
                let location = headers.get("location").ok_or_else(|| {
                    GridError::Framing(format!(
                        "HTTP {status_code} redirect without Location header"
                    ))
                })?;
                return Ok::<Result<Vec<u8>, String>, GridError>(Err(location.clone()));
            }
            if status_code != 200 {
                return Err(GridError::ConnectionFailed(format!(
                    "HTTPS GET {current} returned HTTP {status_code}"
                )));
            }

            let is_chunked = headers
                .get("transfer-encoding")
                .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
            let content_length = headers
                .get("content-length")
                .map(|v| {
                    v.parse::<usize>().map_err(|_| {
                        GridError::Framing(format!("invalid Content-Length '{v}'"))
                    })
                })
                .transpose()?;

            if let Some(len) = content_length {
                if len > max_bytes {
                    return Err(GridError::Config(format!(
                        "download size {len} bytes exceeds limit ({max_bytes} bytes)"
                    )));
                }
            }

            let body = if is_chunked {
                read_chunked_body(tls, leftover, max_bytes).await?
            } else {
                let mut body = Vec::with_capacity(content_length.unwrap_or(leftover.len()));
                if leftover.len() > max_bytes {
                    return Err(GridError::Config(format!(
                        "download exceeds limit ({max_bytes} bytes)"
                    )));
                }
                body.extend_from_slice(&leftover);
                loop {
                    let n = tls.read(&mut chunk).await?;
                    if n == 0 {
                        break;
                    }
                    if body.len() + n > max_bytes {
                        return Err(GridError::Config(format!(
                            "download exceeds limit ({max_bytes} bytes)"
                        )));
                    }
                    body.extend_from_slice(&chunk[..n]);
                }
                if let Some(expected) = content_length {
                    if body.len() != expected {
                        return Err(GridError::Framing(format!(
                            "truncated download from {current}: got {} bytes, expected {expected}",
                            body.len()
                        )));
                    }
                }
                body
            };

            Ok::<Result<Vec<u8>, String>, GridError>(Ok(body))
        })
        .await
        .map_err(|_| GridError::Timeout {
            operation: format!("HTTPS GET {current}"),
            duration_secs: timeout.as_secs(),
        })??;

        match response {
            Ok(body) => return Ok(body),
            Err(location) => {
                let next = current.join(&location).map_err(|e| {
                    GridError::Framing(format!("invalid redirect Location '{location}': {e}"))
                })?;
                current = validate_https_url(next.as_str())?;
            }
        }
    }

    Err(GridError::ConnectionFailed(format!(
        "exceeded {MAX_HTTPS_REDIRECTS} HTTPS redirects fetching {url_str}"
    )))
}

async fn read_chunked_body<R: AsyncReadExt + Unpin>(
    mut stream: R,
    initial: Vec<u8>,
    max_bytes: usize,
) -> GridResult<Vec<u8>> {
    let mut raw = initial;
    let mut body = Vec::new();
    let mut tmp = [0u8; 4096];

    loop {
        let line_end = loop {
            if let Some(pos) = raw.windows(2).position(|w| w == b"\r\n") {
                break pos;
            }
            if raw.len() > 4096 {
                return Err(GridError::Framing("HTTP chunk header line too long".into()));
            }
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(GridError::Framing(
                    "unexpected EOF in chunked HTTP response".into(),
                ));
            }
            raw.extend_from_slice(&tmp[..n]);
        };

        let size_line = std::str::from_utf8(&raw[..line_end])
            .map_err(|_| GridError::Framing("invalid UTF-8 in chunk size line".into()))?;
        let hex_part = size_line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex_part, 16)
            .map_err(|_| GridError::Framing(format!("malformed HTTP chunk size '{hex_part}'")))?;
        raw.drain(..line_end + 2);

        if chunk_size == 0 {
            return Ok(body);
        }
        if body.len() + chunk_size > max_bytes {
            return Err(GridError::Config(format!(
                "chunked download exceeds limit ({max_bytes} bytes)"
            )));
        }
        while raw.len() < chunk_size + 2 {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(GridError::Framing(
                    "unexpected EOF inside HTTP chunk payload".into(),
                ));
            }
            raw.extend_from_slice(&tmp[..n]);
        }
        body.extend_from_slice(&raw[..chunk_size]);
        if &raw[chunk_size..chunk_size + 2] != b"\r\n" {
            return Err(GridError::Framing("malformed HTTP chunk terminator".into()));
        }
        raw.drain(..chunk_size + 2);
    }
}

async fn prepare_verified_archives(
    args: &UsbCreateArgs,
    release: &str,
) -> GridResult<VerifiedArchives> {
    let required = archive_filenames(release, args.gpu);

    if let Some(ref archives_dir) = args.archives {
        let dir_meta = fs::symlink_metadata(archives_dir).map_err(|e| {
            GridError::Config(format!(
                "cannot inspect --archives folder '{}': {e}",
                archives_dir.display()
            ))
        })?;
        if is_symlink_or_reparse(&dir_meta) || !dir_meta.is_dir() {
            return Err(GridError::Config(format!(
                "--archives path '{}' must be a real directory (not a symbolic link)",
                archives_dir.display()
            )));
        }
        let sums_path = archives_dir.join("SHA256SUMS");
        let sums_meta = fs::symlink_metadata(&sums_path).map_err(|e| {
            GridError::Config(format!(
                "missing or unreadable '{}': {e}",
                sums_path.display()
            ))
        })?;
        if is_symlink_or_reparse(&sums_meta) || !sums_meta.is_file() {
            return Err(GridError::Config(format!(
                "'{}' must be a regular file",
                sums_path.display()
            )));
        }
        let sums_text = fs::read_to_string(&sums_path)?;
        let files = verify_archives_against_sums(archives_dir, &sums_text, &required)?;

        if let Ok(online_url) = std::env::var(ONLINE_SUMS_URL_ENV) {
            let trimmed = online_url.trim();
            if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("off") {
                let bytes =
                    https_get_bytes(trimmed, MAX_SUMS_DOWNLOAD_BYTES, Duration::from_secs(30))
                        .await?;
                let online_text = String::from_utf8(bytes).map_err(|_| {
                    GridError::Framing("online SHA256SUMS is not valid UTF-8".into())
                })?;
                verify_sums_against_online(&sums_text, &online_text, &required)?;
            }
        }

        return Ok(VerifiedArchives {
            sums_text,
            files,
            _temp_hold: None,
        });
    }

    if !args.download {
        return Err(GridError::Config(
            "pass either --archives <DIR> or --download".into(),
        ));
    }

    let base_url = match std::env::var(ONLINE_DOWNLOAD_BASE_ENV) {
        Ok(v) if !v.trim().is_empty() => v.trim().trim_end_matches('/').to_string(),
        _ => format!("{DEFAULT_RELEASE_BASE_PREFIX}/v{release}"),
    };
    let _ = validate_https_url(&base_url)?;

    let sums_url = match std::env::var(ONLINE_SUMS_URL_ENV) {
        Ok(v) if !v.trim().is_empty() && !v.trim().eq_ignore_ascii_case("off") => {
            validate_https_url(v.trim())?;
            v.trim().to_string()
        }
        _ => format!("{base_url}/SHA256SUMS"),
    };

    eprintln!("Downloading release SHA256SUMS from {sums_url} ...");
    let sums_bytes =
        https_get_bytes(&sums_url, MAX_SUMS_DOWNLOAD_BYTES, Duration::from_secs(30)).await?;
    let sums_text = String::from_utf8(sums_bytes)
        .map_err(|_| GridError::Framing("downloaded SHA256SUMS is not valid UTF-8".into()))?;
    let _ = parse_sha256sums(&sums_text)?;

    let staging = TempCleanupDir::create(&std::env::temp_dir(), "oxideswarm-dl")?;
    for name in &required {
        let archive_url = format!("{base_url}/{name}");
        eprintln!("Downloading {name} ...");
        let bytes = https_get_bytes(
            &archive_url,
            MAX_ARCHIVE_DOWNLOAD_BYTES,
            Duration::from_secs(300),
        )
        .await?;
        let dest = staging.path.join(name);
        fs::write(&dest, &bytes)?;
    }

    let files = verify_archives_against_sums(&staging.path, &sums_text, &required)?;
    Ok(VerifiedArchives {
        sums_text,
        files,
        _temp_hold: Some(staging),
    })
}

fn write_private_file(path: &Path, bytes: &[u8]) -> GridResult<()> {
    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

fn copy_file_contents_only(src: &Path, dst: &Path) -> GridResult<()> {
    let mut reader = OpenOptions::new().read(true).open(src)?;
    let mut writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;
    std::io::copy(&mut reader, &mut writer)?;
    writer.sync_all()?;
    Ok(())
}

struct KitWriteSpec<'a> {
    plan: &'a KitDestinationPlan,
    archives: &'a VerifiedArchives,
    invite_oxs: &'a [u8],
    metadata: &'a KitMetadata,
    no_autostart: bool,
    isolation: Option<&'a str>,
}

fn write_kit_to_destination(spec: &KitWriteSpec<'_>) -> GridResult<()> {
    let parent_dir = spec
        .plan
        .kit_dir
        .parent()
        .ok_or_else(|| GridError::Config("kit path has no parent directory".into()))?;
    let stage = TempCleanupDir::create(parent_dir, "OxideSwarm.staging")?;
    let archives_dir = stage.path.join("archives");
    fs::create_dir(&archives_dir)?;

    for asset in EMBEDDED_ASSETS {
        let dest = stage.path.join(asset.dest_name);
        fs::write(&dest, asset.bytes)?;
        #[cfg(unix)]
        if asset.executable {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&dest, fs::Permissions::from_mode(0o755))?;
        }
    }

    fs::write(
        archives_dir.join("SHA256SUMS"),
        spec.archives.sums_text.as_bytes(),
    )?;
    for (name, src_path) in &spec.archives.files {
        copy_file_contents_only(src_path, &archives_dir.join(name))?;
    }

    write_private_file(&stage.path.join("invite.oxs"), spec.invite_oxs)?;
    let meta_json = serde_json::to_vec_pretty(spec.metadata)
        .map_err(|e| GridError::Config(format!("failed to serialize kit.json: {e}")))?;
    fs::write(stage.path.join("kit.json"), meta_json)?;

    if spec.no_autostart {
        fs::write(
            stage.path.join("no-autostart"),
            b"The installers on this kit add no start at login.\n",
        )?;
    }
    if let Some(iso) = spec.isolation {
        fs::write(stage.path.join("isolation"), format!("{iso}\n"))?;
    }

    // Re-check destination before moving staged kit into place.
    if spec.plan.replace_existing {
        match fs::symlink_metadata(&spec.plan.kit_dir) {
            Ok(meta) => {
                if is_symlink_or_reparse(&meta) {
                    return Err(GridError::Config(format!(
                        "refusing to replace symbolic link '{}'",
                        spec.plan.kit_dir.display()
                    )));
                }
                ensure_no_reparse_in_tree(&spec.plan.kit_dir)?;
                fs::remove_dir_all(&spec.plan.kit_dir)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(GridError::Io(e)),
        }
    }

    fs::rename(&stage.path, &spec.plan.kit_dir)?;
    Ok(())
}

fn validate_usb_policy(args: &UsbCreateArgs) -> GridResult<(Vec<MemberRole>, u64)> {
    let roles = parse_roles_csv(&args.roles).map_err(GridError::Config)?;
    if roles.contains(&MemberRole::Admin) {
        return Err(GridError::Config(
            "USB setup kits cannot grant the `admin` role (run `oxideswarm invite --roles admin` directly if needed)".into(),
        ));
    }
    if args.machines == 0 {
        return Err(GridError::Config("--machines must be at least 1".into()));
    }
    let default_exp = if args.no_approval { "48h" } else { "7d" };
    let exp_str = args.expires.as_deref().unwrap_or(default_exp);
    let expires_in_secs = parse_duration_secs(exp_str).map_err(GridError::Config)?;
    if expires_in_secs == 0 {
        return Err(GridError::Config(
            "--expires must be greater than zero".into(),
        ));
    }
    if expires_in_secs > USB_MAX_EXPIRES_SECS {
        return Err(GridError::Config(
            "USB kit invite expiration cannot exceed 30d".into(),
        ));
    }
    if args.no_approval && expires_in_secs > USB_NO_APPROVAL_MAX_EXPIRES_SECS {
        return Err(GridError::Config(
            "USB kit invite with --no-approval cannot exceed 48h".into(),
        ));
    }
    Ok((roles, expires_in_secs))
}

async fn run_usb_create(
    args: UsbCreateArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let (roles, expires_in_secs) = match validate_usb_policy(&args) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };
    if args.no_approval {
        eprintln!(
            "WARNING: --no-approval lets anyone with this USB stick and passphrase enroll a machine without Master confirmation (expiry capped at <= 48h)."
        );
    }

    let release = match validate_release_version(
        args.release.as_deref().unwrap_or(env!("CARGO_PKG_VERSION")),
    ) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let dest_plan = match validate_kit_destination(&args.dir, args.replace) {
        Ok(plan) => plan,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let (passphrase, generated_passphrase) = if args.passphrase_stdin {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            eprintln!("Error: --passphrase-stdin requires piping the passphrase on standard input");
            return ExitCode::FAILURE;
        }
        match parse_user_passphrase(std::io::stdin().lock()) {
            Ok(p) => (p, false),
            Err(e) => {
                eprintln!("Error: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        match generate_kit_passphrase() {
            Ok(p) => (p, true),
            Err(e) => {
                eprintln!("Error: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    let p2p_minimal = crate::cli_app::resolve_p2p_minimal();
    let relay_url = match crate::cli_app::resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
        p2p_minimal,
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let helper_invite_args = InviteArgs {
        roles: args.roles.clone(),
        uses: args.machines,
        expires: args
            .expires
            .clone()
            .unwrap_or_else(|| if args.no_approval { "48h" } else { "7d" }.to_string()),
        no_approval: args.no_approval,
        master: args.master.clone(),
        p2p_ticket: args.p2p_ticket.clone(),
        p2p_ticket_file: args.p2p_ticket_file.clone(),
        relay_url: args.relay_url.clone(),
        list: false,
        revoke: None,
        code: false,
        json: false,
        insecure: args.insecure,
    };

    let (local_master, local_ticket) =
        match resolve_local_invite_targets(&helper_invite_args, config_file.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Error: {e}");
                return ExitCode::FAILURE;
            }
        };

    let archives = match prepare_verified_archives(&args, &release).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let connect_addr = resolve_admin_connect_addr(args.master.clone(), config_file.as_ref());
    let insecure = peer_insecure(args.insecure, cfg_client_insecure(config_file.as_ref()));
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &connect_addr,
        insecure,
    ) {
        Ok(s) => s,
        Err(code) => return code,
    };

    with_client_relay_url(relay_url, async move {
        let mut transport = match connect_master(&connect_addr, secret.as_ref()).await {
            Ok(t) => t,
            Err(e) => {
                return report_master_failure(&connect_addr, secret.is_some(), "usb create", &e);
            }
        };

        if let Err(e) = transport
            .send_msg(&ClientMessage::CreateInvite {
                roles: roles.clone(),
                max_uses: args.machines,
                expires_in_secs,
                require_approval: !args.no_approval,
            })
            .await
        {
            eprintln!("Failed to send CreateInvite request to Master ({connect_addr}): {e}");
            return ExitCode::FAILURE;
        }

        let created = match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::InviteCreated {
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
            })) => (
                invite_id,
                invite_secret,
                roles,
                max_uses,
                expires_at,
                require_approval,
                master_key,
                p2p_ticket,
            ),
            Ok(Some(ClientResponse::Error { message, .. })) => {
                eprintln!("Master rejected invite creation: {message}");
                return ExitCode::FAILURE;
            }
            Ok(Some(other)) => {
                eprintln!("Unexpected response from Master while creating invite: {other:?}");
                return ExitCode::FAILURE;
            }
            Ok(None) => {
                eprintln!("Master closed connection before answering CreateInvite");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!(
                    "Failed to receive CreateInvite response from Master ({connect_addr}): {e}"
                );
                return ExitCode::FAILURE;
            }
        };

        let (
            invite_id,
            invite_secret,
            granted_roles,
            max_uses,
            expires_at,
            require_approval,
            master_key,
            master_ticket,
        ) = created;

        let cleanup_revoke =
            |id: String, mut tr: rusty_grid_core::protocol::MessageTransport<_>| async move {
                let _ = tr
                    .send_msg(&ClientMessage::RevokeInvite { invite_id: id })
                    .await;
                let _ = tr.recv_msg::<ClientResponse>().await;
            };

        let (master, ticket) =
            match finalize_invite_targets(local_master, local_ticket, master_ticket) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Error: {e}");
                    cleanup_revoke(invite_id, transport).await;
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
            roles: granted_roles.clone(),
            require_approval,
        };
        let encoded = Zeroizing::new(token.encode());
        let invite_oxs = match encrypt_invite_oxs(&encoded, &passphrase) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("Error encrypting invite.oxs: {e}");
                cleanup_revoke(invite_id, transport).await;
                return ExitCode::FAILURE;
            }
        };

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let role_names: Vec<String> = granted_roles
            .iter()
            .map(|r| match r {
                MemberRole::Worker => "worker".to_string(),
                MemberRole::Client => "client".to_string(),
                MemberRole::Admin => "admin".to_string(),
            })
            .collect();
        let metadata = KitMetadata {
            kit_format: 1,
            version: release.clone(),
            created_at,
            expires_at,
            machines: max_uses,
            roles: role_names,
            workspaces: args.workspaces,
            gpu: args.gpu,
            require_approval,
            invite_id: invite_id.clone(),
        };

        let write_spec = KitWriteSpec {
            plan: &dest_plan,
            archives: &archives,
            invite_oxs: &invite_oxs,
            metadata: &metadata,
            no_autostart: args.no_autostart,
            isolation: args.isolation.as_deref(),
        };
        if let Err(e) = write_kit_to_destination(&write_spec) {
            eprintln!(
                "Error writing USB kit to '{}': {e}",
                dest_plan.kit_dir.display()
            );
            cleanup_revoke(invite_id, transport).await;
            return ExitCode::FAILURE;
        }

        println!("USB setup kit written to {}", dest_plan.kit_dir.display());
        println!("  version:    {release}");
        println!("  invite id:  {invite_id}");
        println!("  machines:   {max_uses}");
        println!("  roles:      {}", format_roles(&granted_roles));
        println!(
            "  approval:   {}",
            if require_approval {
                "required per machine"
            } else {
                "disabled (--no-approval)"
            }
        );
        println!(
            "  workspaces: {}",
            if args.workspaces {
                "enabled when Cargo is available"
            } else {
                "disabled"
            }
        );
        println!(
            "  gpu:        {}",
            if args.gpu { "included" } else { "not included" }
        );
        if generated_passphrase {
            println!();
            println!("Kit passphrase (write this down; NOT saved on the stick):");
            println!("  {}", &*passphrase);
        } else {
            println!("  passphrase: set from stdin (not printed and not saved on the stick)");
        }
        ExitCode::SUCCESS
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_app::Cli;
    use clap::Parser;
    use ring::digest;

    const SAMPLE_TOKEN: &str = "oxs1-invite:eyJtIjoiMTI3LjAuMC4xOjgwODgiLCJ0IjpudWxsLCJtayI6ImFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWEiLCJpZCI6IjAxMjM0NTY3ODlhYmNkZWYwMTIzNDU2Nzg5YWJjZGVmIiwicyI6ImJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmIiLCJleHAiOjIwMDAwMDAwMDAsInIiOlsid29ya2VyIl0sImFwIjp0cnVlfQ.0123456789abcdef";
    const SAMPLE_PASS: &str = "abandon ability able about";

    fn fast_encrypt(plain: &str, pass: &str) -> Vec<u8> {
        encrypt_invite_oxs_with_params(plain, pass, 64, 1, 1).expect("fast encrypt")
    }

    fn hex_sha256(bytes: &[u8]) -> String {
        let d = digest::digest(&digest::SHA256, bytes);
        let mut s = String::with_capacity(64);
        for b in d.as_ref() {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{b:02x}");
        }
        s
    }

    #[test]
    fn invite_oxs_default_params_round_trip() {
        let encrypted = encrypt_invite_oxs(SAMPLE_TOKEN, SAMPLE_PASS).expect("encrypt default");
        let header = validate_invite_oxs_header(&encrypted).expect("header valid");
        assert_eq!(header.m_cost_kib, INVITE_OXS_DEFAULT_M_KIB);
        assert_eq!(header.t_cost, INVITE_OXS_DEFAULT_T_COST);
        assert_eq!(header.p_cost, INVITE_OXS_DEFAULT_P_COST);
        let decrypted = decrypt_invite_oxs(&encrypted, SAMPLE_PASS).expect("decrypt default");
        assert_eq!(&*decrypted, SAMPLE_TOKEN);
    }

    #[test]
    fn invite_oxs_rejects_wrong_passphrase_truncation_and_bit_flips() {
        let encrypted = fast_encrypt(SAMPLE_TOKEN, SAMPLE_PASS);

        let wrong =
            decrypt_invite_oxs(&encrypted, "wrong-passphrase-words").expect_err("wrong passphrase");
        assert!(matches!(wrong, GridError::AuthFailed(_)), "{wrong:?}");
        assert!(!format!("{wrong:?}").contains(SAMPLE_PASS));
        assert!(!format!("{wrong:?}").contains(SAMPLE_TOKEN));

        for len in [0, 10, 47, 48, 64] {
            let truncated = &encrypted[..len];
            assert!(
                decrypt_invite_oxs(truncated, SAMPLE_PASS).is_err(),
                "len={len} should fail"
            );
        }

        // Flip ciphertext/tag byte.
        let mut bad_body = encrypted.clone();
        let last = bad_body.len() - 1;
        bad_body[last] ^= 0x01;
        assert!(decrypt_invite_oxs(&bad_body, SAMPLE_PASS).is_err());

        // Flip non-KDF header byte (salt byte 20 / nonce byte 36): AAD mismatch.
        let mut bad_salt = encrypted.clone();
        bad_salt[20] ^= 0x80;
        assert!(decrypt_invite_oxs(&bad_salt, SAMPLE_PASS).is_err());

        let mut bad_nonce = encrypted.clone();
        bad_nonce[36] ^= 0x40;
        assert!(decrypt_invite_oxs(&bad_nonce, SAMPLE_PASS).is_err());
    }

    #[test]
    fn invite_oxs_rejects_out_of_bounds_argon2_headers_before_kdf() {
        let base = fast_encrypt(SAMPLE_TOKEN, SAMPLE_PASS);

        // Bad magic
        let mut bad_magic = base.clone();
        bad_magic[7] = b'2';
        assert!(validate_invite_oxs_header(&bad_magic).is_err());

        // Oversize file
        let mut huge = base.clone();
        huge.resize(INVITE_OXS_MAX_FILE_BYTES + 1, 0u8);
        assert!(validate_invite_oxs_header(&huge).is_err());

        // Memory > 256 MiB (e.g. 2 GiB) or < 8 KiB: rejected in O(1) without allocating GB.
        for bad_m in [0u32, 7, INVITE_OXS_MAX_M_KIB + 1, 2_097_152] {
            let mut blob = base.clone();
            blob[8..12].copy_from_slice(&bad_m.to_le_bytes());
            assert!(validate_invite_oxs_header(&blob).is_err(), "m={bad_m}");
        }

        // Passes == 0 or > 10
        for bad_t in [0u32, INVITE_OXS_MAX_T_COST + 1, 1_000_000] {
            let mut blob = base.clone();
            blob[12..16].copy_from_slice(&bad_t.to_le_bytes());
            assert!(validate_invite_oxs_header(&blob).is_err(), "t={bad_t}");
        }

        // Parallelism == 0 or > 4
        for bad_p in [0u32, INVITE_OXS_MAX_P_COST + 1, 64] {
            let mut blob = base.clone();
            blob[16..20].copy_from_slice(&bad_p.to_le_bytes());
            assert!(validate_invite_oxs_header(&blob).is_err(), "p={bad_p}");
        }
    }

    #[test]
    fn passphrase_generation_and_stdin_validation() {
        let gen = generate_kit_passphrase().expect("4 words");
        let words: Vec<&str> = gen.split('-').collect();
        assert_eq!(words.len(), 4);
        assert!(words.iter().all(|w| crate::wordlist::contains_word(w)));

        assert!(parse_user_passphrase("too short".as_bytes()).is_err());
        assert!(parse_user_passphrase("123456789012345".as_bytes()).is_err());
        assert!(parse_user_passphrase("sixteen-chars-ok\nextra-line".as_bytes()).is_err());
        assert!(parse_user_passphrase("has\x00null-character-here".as_bytes()).is_err());

        let ok = parse_user_passphrase("correct horse battery staple\r\n".as_bytes())
            .expect("valid user passphrase");
        assert_eq!(&*ok, "correct horse battery staple");
    }

    #[test]
    fn usb_policy_and_cli_flags_enforce_limits() {
        let parse_create = |extra: &[&str]| -> Result<UsbCreateArgs, clap::Error> {
            let mut argv = vec!["oxideswarm", "usb", "create", "/tmp/stick"];
            argv.extend_from_slice(extra);
            Cli::try_parse_from(argv).map(|c| match c.command {
                crate::cli_app::Commands::Usb(UsbCliArgs {
                    command: UsbCommand::Create(a),
                }) => a,
                other => unreachable!("unexpected command: {other:?}"),
            })
        };

        // Default flags parse cleanly.
        let a = parse_create(&["--machines", "5", "--download"]).expect("parse create");
        assert_eq!(a.machines, 5);
        assert!(validate_usb_policy(&a).is_ok());

        // Refuses admin role.
        let a_admin = parse_create(&["--roles", "worker,admin", "--download"]).expect("parse");
        assert!(validate_usb_policy(&a_admin).is_err());

        // Refuses > 30d expiry.
        let a_31d = parse_create(&["--expires", "31d", "--download"]).expect("parse");
        assert!(validate_usb_policy(&a_31d).is_err());

        // --no-approval defaults to 48h (ok) and refuses > 48h.
        let a_no_app = parse_create(&["--no-approval", "--download"]).expect("parse");
        let (_, exp) = validate_usb_policy(&a_no_app).expect("48h default allowed");
        assert_eq!(exp, 48 * 3600);

        let a_no_app_7d =
            parse_create(&["--no-approval", "--expires", "7d", "--download"]).expect("parse");
        assert!(validate_usb_policy(&a_no_app_7d).is_err());

        // --archives conflicts with --download.
        assert!(parse_create(&["--archives", "/tmp/a", "--download"]).is_err());
    }

    #[test]
    fn verify_archives_against_sums_and_stubbed_online_sums() {
        let tmp = std::env::temp_dir().join(format!("ox_usb_sum_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).expect("mkdir");

        let required = archive_filenames("0.1.2", false);
        let mut sums = String::new();
        for (idx, name) in required.iter().enumerate() {
            let content = format!("fake archive #{idx} for {name}").into_bytes();
            let digest = hex_sha256(&content);
            fs::write(tmp.join(name), &content).expect("write archive");
            sums.push_str(&format!("{digest}  {name}\n"));
        }

        let verified = verify_archives_against_sums(&tmp, &sums, &required).expect("valid sums");
        assert_eq!(verified.len(), required.len());

        // Stubbed identical online sums pass.
        verify_sums_against_online(&sums, &sums, &required).expect("online match");

        // Stubbed mismatched online sums are refused.
        let bad_online = sums.replacen("0", "1", 1);
        let corrupted_online = if bad_online == sums {
            sums.replacen("a", "b", 1)
        } else {
            bad_online
        };
        assert!(verify_sums_against_online(&sums, &corrupted_online, &required).is_err());

        // Corrupted local archive fails.
        fs::write(tmp.join(&required[0]), b"tampered").expect("tamper");
        assert!(verify_archives_against_sums(&tmp, &sums, &required).is_err());

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn validate_kit_destination_refuses_symlinks_and_non_empty_without_replace() {
        let tmp = std::env::temp_dir().join(format!("ox_usb_dest_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).expect("mkdir");

        // Empty destination works.
        let plan = validate_kit_destination(&tmp, false).expect("clean dest");
        assert!(!plan.replace_existing);

        // Non-empty non-kit dir refused even with --replace.
        let kit_dir = tmp.join("OxideSwarm");
        fs::create_dir_all(&kit_dir).expect("mkdir kit");
        fs::write(kit_dir.join("random.txt"), b"x").expect("write");
        assert!(validate_kit_destination(&tmp, false).is_err());
        assert!(validate_kit_destination(&tmp, true).is_err());

        // Kit dir with install.sh allowed only with --replace.
        fs::write(kit_dir.join("install.sh"), b"#!/bin/bash\n").expect("write");
        assert!(validate_kit_destination(&tmp, false).is_err());
        let plan_replace = validate_kit_destination(&tmp, true).expect("replace kit");
        assert!(plan_replace.replace_existing);

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn https_url_validation_rejects_plain_http_and_embedded_creds() {
        assert!(validate_https_url("http://example.com/SHA256SUMS").is_err());
        assert!(validate_https_url("https://user:pass@example.com/SHA256SUMS").is_err());
        assert!(validate_https_url("https://github.com/DuongNAD/OxideSwarm/releases").is_ok());
    }

    #[test]
    fn invite_oxs_argon2_0_5_3_raw_key_and_file_vectors() {
        fn decode_hex(hex: &str) -> Vec<u8> {
            assert_eq!(hex.len() % 2, 0);
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
                .collect()
        }

        let txt = include_str!("../tests/data/invite_argon2_0_5_3.txt");
        let mut vectors = BTreeMap::new();
        for line in txt.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let (k, v) = trimmed.split_once(' ').expect("key value line");
            vectors.insert(k, v);
        }

        let passphrase = vectors["passphrase"];
        let plaintext = vectors["plaintext"];

        let oxs_fast = include_bytes!("../tests/data/invite_argon2_0_5_3.oxs");
        assert_eq!(
            oxs_fast.as_slice(),
            decode_hex(vectors["oxs_64_2_2"]).as_slice()
        );
        let header_fast = validate_invite_oxs_header(oxs_fast).expect("64/2/2 header");
        let key_fast = derive_invite_oxs_key(passphrase, &header_fast).expect("64/2/2 key");
        assert_eq!(
            key_fast.as_slice(),
            decode_hex(vectors["key_64_2_2"]).as_slice()
        );
        assert_eq!(
            &*decrypt_invite_oxs(oxs_fast, passphrase).expect("64/2/2 decrypt"),
            plaintext
        );

        let oxs_default = decode_hex(vectors["oxs_65536_3_1"]);
        let header_default = validate_invite_oxs_header(&oxs_default).expect("65536/3/1 header");
        let key_default =
            derive_invite_oxs_key(passphrase, &header_default).expect("65536/3/1 key");
        assert_eq!(
            key_default.as_slice(),
            decode_hex(vectors["key_65536_3_1"]).as_slice()
        );
        assert_eq!(
            &*decrypt_invite_oxs(&oxs_default, passphrase).expect("65536/3/1 decrypt"),
            plaintext
        );
    }
}
