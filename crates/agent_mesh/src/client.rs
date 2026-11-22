//! Native Rust WebSocket Agent Mesh Client
//!
//! Provides connection management, node registration, periodic heartbeats,
//! remote command execution, data transmission, and request/response correlation.

use crate::executor::CommandExecutor;
use crate::protocol::{
    compute_sha256, verify_sha256, AgentMeshEnvelope, CorrelationId, NodeDescriptor,
    DEFAULT_DATA_CHUNK_BYTES, ERR_DUPLICATE_NODE_ID, ERR_INVALID_NODE_ID, ERR_PAYLOAD_CORRUPT,
    ERR_PERSIST_FAILED, ERR_UNAUTHORIZED,
};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_tungstenite::{
    connect_async_tls_with_config, tungstenite::protocol::Message as WsMessage, Connector,
};
use tracing::{info, warn};

/// Host part of a `ws://` hub URL, or `None` for any other scheme (`wss://` is encrypted).
fn cleartext_ws_host(hub_url: &str) -> Option<String> {
    let url = hub_url.trim();
    let (scheme, rest) = url.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("ws") {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or("");
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or("")
    } else {
        authority.split(':').next().unwrap_or("")
    };
    Some(host.to_ascii_lowercase())
}

/// True when a node that holds a hub token would send it in clear text to a host that is not on
/// loopback: the URL is `ws://` and the host is neither `localhost`, nor a loopback IP address.
/// The token itself is never part of this check (SEC-15, design 0079).
pub fn token_sent_in_clear_to_remote_host(hub_url: &str, has_token: bool) -> bool {
    if !has_token {
        return false;
    }
    match cleartext_ws_host(hub_url) {
        None => false,
        Some(host) => {
            let loopback = host == "localhost"
                || host.ends_with(".localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .map(|ip| ip.is_loopback())
                    .unwrap_or(false);
            !loopback
        }
    }
}

/// How long `connect` waits for the hub to accept or reject the registration before it carries on
/// (a hub that never answers is treated like the pre-token hubs, which did not wait either).
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause between reconnect attempts after the hub rejected the credentials or the node id: such a
/// rejection cannot succeed on its own, so do not hammer the hub.
const REJECTED_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Returns `true` when a WebSocket connection error is caused by transient loopback ephemeral
/// port or non-paged socket buffer exhaustion under heavy parallel test/connection load
/// (e.g. Windows `WSAENOBUFS` `10055` or `WSAEADDRINUSE` `10048`).
fn is_transient_ws_connect_error(err: &tokio_tungstenite::tungstenite::Error) -> bool {
    if let tokio_tungstenite::tungstenite::Error::Io(io_err) = err {
        if matches!(io_err.raw_os_error(), Some(10055 | 10048 | 10053 | 10054)) {
            return true;
        }
        if matches!(
            io_err.kind(),
            std::io::ErrorKind::AddrInUse
                | std::io::ErrorKind::OutOfMemory
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::Interrupted
        ) {
            return true;
        }
    }
    false
}

/// Verified output artifact extracted from a `grid_submit` / `grid_result` `CommandResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedArtifact {
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub bytes: Vec<u8>,
}

/// The hub answered the registration with a NACK (bad or missing token, duplicate node id, ...).
#[derive(Debug, Clone)]
pub struct RegistrationRejected {
    pub error_code: String,
    pub reason: String,
}

impl fmt::Display for RegistrationRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "hub rejected the registration: {} - {}",
            self.error_code, self.reason
        )
    }
}

impl std::error::Error for RegistrationRejected {}

/// Parses one or more X.509 certificates from PEM (`-----BEGIN CERTIFICATE-----`, including
/// `openssl x509 -text` or `openssl pkcs12` `Bag Attributes` preambles and optional UTF-8 BOM)
/// or raw DER bytes.
pub(crate) fn parse_certificates_pem_or_der(
    raw: &[u8],
) -> Result<Vec<CertificateDer<'static>>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;

    let raw_no_bom = raw.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(raw);
    let trimmed = raw_no_bom
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .map_or(&[][..], |idx| &raw_no_bom[idx..]);
    if trimmed.is_empty() {
        return Err("TLS CA certificate data is empty".into());
    }
    if raw_no_bom.windows(10).any(|w| w == b"-----BEGIN") {
        let mut certs = Vec::new();
        for item in CertificateDer::pem_slice_iter(raw_no_bom) {
            let cert = item.map_err(|e| format!("invalid TLS CA PEM certificate: {e}"))?;
            certs.push(cert);
        }
        if certs.is_empty() {
            return Err("no -----BEGIN CERTIFICATE----- block found in TLS CA PEM".into());
        }
        Ok(certs)
    } else {
        Ok(vec![CertificateDer::from(raw.to_vec())])
    }
}

/// Default maximum number of received `DataPayload` envelopes retained in memory per client.
pub const DEFAULT_MAX_RECEIVED_PAYLOADS: usize = 256;
/// Default maximum total payload bytes retained in memory in `received_payloads` (64 MiB).
pub const DEFAULT_MAX_RECEIVED_BYTES: usize = 64 * 1024 * 1024;

/// Maximum concurrent in-flight multi-chunk reassemblies tracked per client connection.
const MAX_PENDING_ASSEMBLIES: usize = 64;
/// Maximum number of chunks accepted for a single multi-chunk transfer.
const MAX_CHUNKS_PER_TRANSFER: u32 = 1024;
/// Maximum reassembled payload size accepted by a client (64 MiB).
const MAX_ASSEMBLED_BYTES: usize = 64 * 1024 * 1024;
/// Maximum aggregate payload bytes buffered across all in-flight reassemblies (128 MiB).
const MAX_TOTAL_REASSEMBLER_BYTES: usize = 128 * 1024 * 1024;
/// Time-to-live for incomplete multi-chunk transfers before eviction.
const CHUNK_ASSEMBLY_TTL: Duration = Duration::from_secs(60);

/// Delivery acknowledgement receipt returned by `AgentMeshClient::send_data_payload_with_ack`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDeliveryReceipt {
    pub correlation_id: String,
    pub sha256: String,
    pub total_chunks: u32,
    pub responder: String,
    pub status: String,
}

#[derive(Debug)]
struct BoundedPayloadBuffer {
    items: VecDeque<AgentMeshEnvelope>,
    total_bytes: usize,
    max_items: usize,
    max_bytes: usize,
}

impl BoundedPayloadBuffer {
    fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            items: VecDeque::new(),
            total_bytes: 0,
            max_items,
            max_bytes,
        }
    }

    fn payload_bytes(env: &AgentMeshEnvelope) -> usize {
        match env {
            AgentMeshEnvelope::DataPayload {
                data,
                chunk_size_bytes,
                payload,
                ..
            } => {
                let d_len = data.as_ref().map_or(0, String::len).max(*chunk_size_bytes);
                let p_len = payload.as_ref().map_or(0, |v| v.to_string().len());
                d_len.saturating_add(p_len)
            }
            _ => 0,
        }
    }

    fn push(&mut self, env: AgentMeshEnvelope) {
        if self.max_items == 0 || self.max_bytes == 0 {
            return;
        }
        let sz = Self::payload_bytes(&env);
        if sz > self.max_bytes {
            return;
        }
        while self.items.len() >= self.max_items
            || (self.total_bytes.saturating_add(sz) > self.max_bytes && !self.items.is_empty())
        {
            if let Some(evicted) = self.items.pop_front() {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(Self::payload_bytes(&evicted));
            } else {
                break;
            }
        }
        self.total_bytes = self.total_bytes.saturating_add(sz);
        self.items.push_back(env);
    }
}

struct ReassembledPayload {
    bytes: Vec<u8>,
    expected_total_sha256: Option<String>,
}

struct ChunkAssembly {
    total_chunks: u32,
    expected_total_sha256: Option<String>,
    chunks: HashMap<u32, Vec<u8>>,
    total_bytes: usize,
    updated_at: Instant,
    seq_order: u64,
}

struct ChunkReassembler {
    inflight: HashMap<(String, String), ChunkAssembly>,
    total_inflight_bytes: usize,
    next_seq_order: u64,
    max_pending_assemblies: usize,
    max_assembled_bytes: usize,
    max_total_bytes: usize,
    ttl: Duration,
}

impl ChunkReassembler {
    fn new() -> Self {
        Self::with_limits(
            MAX_PENDING_ASSEMBLIES,
            MAX_ASSEMBLED_BYTES,
            MAX_TOTAL_REASSEMBLER_BYTES,
        )
    }

    fn with_limits(
        max_pending_assemblies: usize,
        max_assembled_bytes: usize,
        max_total_bytes: usize,
    ) -> Self {
        Self {
            inflight: HashMap::new(),
            total_inflight_bytes: 0,
            next_seq_order: 0,
            max_pending_assemblies,
            max_assembled_bytes,
            max_total_bytes,
            ttl: CHUNK_ASSEMBLY_TTL,
        }
    }

    #[cfg(test)]
    fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    fn remove_assembly(&mut self, key: &(String, String)) -> Option<ChunkAssembly> {
        let removed = self.inflight.remove(key)?;
        self.total_inflight_bytes = self
            .total_inflight_bytes
            .saturating_sub(removed.total_bytes);
        Some(removed)
    }

    fn evict_oldest_fifo(&mut self, exclude_key: Option<&(String, String)>) -> bool {
        let oldest_key = self
            .inflight
            .iter()
            .filter(|(k, _)| exclude_key != Some(*k))
            .min_by_key(|(_, v)| v.seq_order)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest_key {
            self.remove_assembly(&k);
            true
        } else {
            false
        }
    }

    fn prune_expired(&mut self, now: Instant) {
        let ttl = self.ttl;
        let mut removed_bytes = 0usize;
        self.inflight.retain(|_, asm| {
            let keep = now.saturating_duration_since(asm.updated_at) <= ttl;
            if !keep {
                removed_bytes = removed_bytes.saturating_add(asm.total_bytes);
            }
            keep
        });
        self.total_inflight_bytes = self.total_inflight_bytes.saturating_sub(removed_bytes);
    }

    fn insert_chunk(
        &mut self,
        from: &str,
        corr_id: &str,
        seq: u32,
        total_chunks: u32,
        expected_total_sha256: Option<&str>,
        bytes: Vec<u8>,
    ) -> Result<Option<ReassembledPayload>, String> {
        let now = Instant::now();
        self.prune_expired(now);

        if bytes.len() > self.max_assembled_bytes || bytes.len() > self.max_total_bytes {
            let key = (from.to_string(), corr_id.to_string());
            self.remove_assembly(&key);
            return Err(format!(
                "Chunk size {} exceeds cap of {} bytes",
                bytes.len(),
                self.max_assembled_bytes.min(self.max_total_bytes)
            ));
        }

        let key = (from.to_string(), corr_id.to_string());
        if !self.inflight.contains_key(&key) {
            while self.inflight.len() >= self.max_pending_assemblies
                || self.total_inflight_bytes.saturating_add(bytes.len()) > self.max_total_bytes
            {
                if !self.evict_oldest_fifo(None) {
                    break;
                }
            }
            let seq_order = self.next_seq_order;
            self.next_seq_order = self.next_seq_order.saturating_add(1);
            self.inflight.insert(
                key.clone(),
                ChunkAssembly {
                    total_chunks,
                    expected_total_sha256: expected_total_sha256.map(str::to_string),
                    chunks: HashMap::new(),
                    total_bytes: 0,
                    updated_at: now,
                    seq_order,
                },
            );
        }

        let (expected_chunks, prev_len, new_total_bytes, seq_mismatch, sha_mismatch, set_sha) = {
            let asm = self
                .inflight
                .get(&key)
                .expect("assembly inserted immediately above");

            let seq_mismatch = asm.total_chunks != total_chunks;
            let mut sha_mismatch = false;
            let mut set_sha = false;
            if let Some(new_sha) = expected_total_sha256 {
                if let Some(ref existing_sha) = asm.expected_total_sha256 {
                    if !existing_sha.eq_ignore_ascii_case(new_sha) {
                        sha_mismatch = true;
                    }
                } else {
                    set_sha = true;
                }
            }
            let prev_len = asm.chunks.get(&seq).map_or(0, Vec::len);
            let new_total_bytes = asm
                .total_bytes
                .saturating_sub(prev_len)
                .saturating_add(bytes.len());
            (
                asm.total_chunks,
                prev_len,
                new_total_bytes,
                seq_mismatch,
                sha_mismatch,
                set_sha,
            )
        };

        if seq_mismatch {
            self.remove_assembly(&key);
            return Err(format!(
                "Inconsistent total_chunks in multi-chunk transfer (expected {expected_chunks}, got {total_chunks})"
            ));
        }

        if sha_mismatch {
            self.remove_assembly(&key);
            return Err("Inconsistent total_sha256 in multi-chunk transfer".to_string());
        }

        if new_total_bytes > self.max_assembled_bytes {
            self.remove_assembly(&key);
            return Err(format!(
                "Assembled payload size {new_total_bytes} exceeds cap of {} bytes",
                self.max_assembled_bytes
            ));
        }

        // Evict oldest *other* in-flight assemblies FIFO before exceeding aggregate cap.
        while self
            .total_inflight_bytes
            .saturating_sub(prev_len)
            .saturating_add(bytes.len())
            > self.max_total_bytes
        {
            if !self.evict_oldest_fifo(Some(&key)) {
                self.remove_assembly(&key);
                return Err(format!(
                    "Aggregate reassembler buffer cap of {} bytes exceeded",
                    self.max_total_bytes
                ));
            }
        }

        let new_bytes_len = bytes.len();
        let is_complete = {
            let asm = self
                .inflight
                .get_mut(&key)
                .expect("assembly retained while evicting others");
            if set_sha {
                asm.expected_total_sha256 = expected_total_sha256.map(str::to_string);
            }
            asm.total_bytes = new_total_bytes;
            asm.updated_at = now;
            asm.chunks.insert(seq, bytes);
            asm.chunks.len() >= total_chunks as usize
        };
        self.total_inflight_bytes = self
            .total_inflight_bytes
            .saturating_sub(prev_len)
            .saturating_add(new_bytes_len);

        if !is_complete {
            return Ok(None);
        }

        let mut completed = self
            .remove_assembly(&key)
            .expect("assembly present before completion");
        let mut assembled = Vec::with_capacity(completed.total_bytes);
        for idx in 0..total_chunks {
            let Some(part) = completed.chunks.remove(&idx) else {
                return Err(format!("Missing chunk {idx} during reassembly"));
            };
            assembled.extend_from_slice(&part);
        }
        Ok(Some(ReassembledPayload {
            bytes: assembled,
            expected_total_sha256: completed.expected_total_sha256,
        }))
    }
}

struct ActiveCommandGuard(Arc<AtomicUsize>);

impl Drop for ActiveCommandGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Validates and canonicalizes an `--inbox-dir` path before accepting incoming payloads.
///
/// Rejects empty paths, symlinks, and non-directory entries (such as regular files). Creates the
/// directory if it does not yet exist, then returns its canonical path.
pub fn validate_inbox_dir(
    path: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if path.as_os_str().is_empty() {
        return Err("inbox directory path must not be empty".into());
    }
    // Strip trailing path separators (and trailing `/.`) while preserving root `/` so POSIX
    // `lstat` inspects the symlink itself rather than dereferencing `/path/to/symlink/`.
    let inspect_path = path.components().as_path();
    if inspect_path.as_os_str().is_empty() {
        return Err("inbox directory path must not be empty".into());
    }
    match std::fs::symlink_metadata(inspect_path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(
                    format!("inbox directory '{}' must not be a symlink", path.display()).into(),
                );
            }
            if !meta.is_dir() {
                return Err(format!(
                    "inbox directory '{}' exists and is not a directory",
                    path.display()
                )
                .into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(inspect_path).map_err(|err| {
                format!(
                    "failed to create inbox directory '{}': {err}",
                    path.display()
                )
            })?;
        }
        Err(e) => {
            return Err(format!(
                "failed to inspect inbox directory '{}': {e}",
                path.display()
            )
            .into());
        }
    }

    let canonical = std::fs::canonicalize(inspect_path).map_err(|e| {
        format!(
            "failed to canonicalize inbox directory '{}': {e}",
            path.display()
        )
    })?;
    let canon_meta = std::fs::symlink_metadata(&canonical).map_err(|e| {
        format!(
            "failed to inspect canonical inbox directory '{}': {e}",
            canonical.display()
        )
    })?;
    if canon_meta.file_type().is_symlink() || !canon_meta.is_dir() {
        return Err(format!(
            "canonical inbox directory '{}' is not a valid directory",
            canonical.display()
        )
        .into());
    }
    Ok(canonical)
}

fn sanitize_filename_component(raw: &str, max_len: usize) -> String {
    let filtered: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(max_len)
        .collect();
    let collapsed = filtered.replace("..", "_");
    let trimmed = collapsed.trim_matches('.');
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "payload".to_string()
    } else {
        trimmed.to_string()
    }
}

fn persist_payload_atomic(
    inbox_dir: &Path,
    corr_id: &str,
    timestamp_ms: u64,
    metadata: Option<&HashMap<String, String>>,
    bytes: &[u8],
) -> Result<PathBuf, String> {
    let dir_meta = std::fs::symlink_metadata(inbox_dir).map_err(|e| {
        format!(
            "inbox directory '{}' inaccessible: {e}",
            inbox_dir.display()
        )
    })?;
    if dir_meta.file_type().is_symlink() || !dir_meta.is_dir() {
        return Err(format!(
            "inbox directory '{}' is not a real directory",
            inbox_dir.display()
        ));
    }

    let safe_corr = sanitize_filename_component(corr_id, 64);
    let suffix_hint = metadata
        .and_then(|m| m.get("filename"))
        .map(|f| sanitize_filename_component(f, 48));

    let base_name = match suffix_hint {
        Some(ref hint) => format!("{timestamp_ms}_{safe_corr}_{hint}.bin"),
        None => format!("{timestamp_ms}_{safe_corr}.bin"),
    };

    let mut final_name = base_name;
    let mut final_path = inbox_dir.join(&final_name);
    if std::fs::symlink_metadata(&final_path).is_ok() {
        let uniq = &uuid::Uuid::new_v4().to_string()[..8];
        final_name = match suffix_hint {
            Some(ref hint) => format!("{timestamp_ms}_{safe_corr}_{hint}_{uniq}.bin"),
            None => format!("{timestamp_ms}_{safe_corr}_{uniq}.bin"),
        };
        final_path = inbox_dir.join(&final_name);
    }

    let mut comps = Path::new(&final_name).components();
    if !matches!(comps.next(), Some(Component::Normal(_)))
        || comps.next().is_some()
        || final_path.parent() != Some(inbox_dir)
    {
        return Err("resolved inbox filename failed path-traversal check".to_string());
    }

    if let Ok(existing_meta) = std::fs::symlink_metadata(&final_path) {
        if existing_meta.file_type().is_symlink() {
            return Err("refusing to overwrite symlink in inbox directory".to_string());
        }
    }

    let tmp_name = format!(".tmp_{}.part", uuid::Uuid::new_v4());
    let tmp_path = inbox_dir.join(&tmp_name);
    if tmp_path.parent() != Some(inbox_dir) {
        return Err("resolved temp filename escaped inbox directory".to_string());
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .map_err(|e| format!("failed to create temp inbox file: {e}"))?;

    let write_res = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    })();
    drop(file);

    if let Err(e) = write_res {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(format!("failed to write inbox file: {e}"));
    }

    if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(format!("failed to finalize inbox file: {e}"));
    }

    Ok(final_path)
}

pub struct AgentMeshClient {
    hub_url: String,
    node_id: String,
    platform: String,
    hostname: String,
    token: Option<String>,
    tls_ca_bytes: Option<Vec<u8>>,
    tls_ca_file: Option<PathBuf>,
    inbox_dir: Option<PathBuf>,
    tx: Arc<RwLock<Option<mpsc::UnboundedSender<WsMessage>>>>,
    pending: Arc<RwLock<HashMap<CorrelationId, oneshot::Sender<AgentMeshEnvelope>>>>,
    received_payloads: Arc<RwLock<BoundedPayloadBuffer>>,
    executed_count: Arc<AtomicUsize>,
    active_commands: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
    no_shell: bool,
}

impl AgentMeshClient {
    pub fn new(
        hub_url: impl Into<String>,
        node_id: impl Into<String>,
        platform: impl Into<String>,
    ) -> Self {
        let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown-host".to_string());
        Self {
            hub_url: hub_url.into(),
            node_id: node_id.into(),
            platform: platform.into(),
            hostname,
            token: None,
            tls_ca_bytes: None,
            tls_ca_file: None,
            inbox_dir: None,
            tx: Arc::new(RwLock::new(None)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            received_payloads: Arc::new(RwLock::new(BoundedPayloadBuffer::new(
                DEFAULT_MAX_RECEIVED_PAYLOADS,
                DEFAULT_MAX_RECEIVED_BYTES,
            ))),
            executed_count: Arc::new(AtomicUsize::new(0)),
            active_commands: Arc::new(AtomicUsize::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            no_shell: false,
        }
    }

    /// Configures an inbox directory where verified incoming `DataPayload` bytes are written
    /// atomically before sending `DeliveryAck`.
    pub fn with_inbox_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.inbox_dir = Some(dir.into());
        self
    }

    /// Configures the maximum number of received `DataPayload` envelopes retained in memory.
    pub fn with_max_received_payloads(self, max_items: usize) -> Self {
        if let Ok(mut buf) = self.received_payloads.try_write() {
            buf.max_items = max_items;
        }
        self
    }

    /// Configures the maximum total bytes retained in `received_payloads` in memory.
    pub fn with_max_received_bytes(self, max_bytes: usize) -> Self {
        if let Ok(mut buf) = self.received_payloads.try_write() {
            buf.max_bytes = max_bytes;
        }
        self
    }

    /// Validates the configured `--inbox-dir` (if any) and returns its canonical path.
    pub fn validate_inbox_config(
        &self,
    ) -> Result<Option<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
        match self.inbox_dir {
            Some(ref dir) => validate_inbox_dir(dir).map(Some),
            None => Ok(None),
        }
    }

    /// Sets the hub token sent with the registration (the CLI feeds `--token` / `OXIDE_MESH_TOKEN`
    /// here). Needed for any hub started with a token.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Adds a custom trusted root CA certificate (PEM or DER bytes) for `wss://` connections
    /// behind a private TLS-terminating proxy.
    pub fn with_tls_ca_pem(mut self, pem_or_der: impl Into<Vec<u8>>) -> Self {
        self.tls_ca_bytes = Some(pem_or_der.into());
        self
    }

    /// Adds a custom trusted root CA certificate file (PEM or DER) for `wss://` connections
    /// behind a private TLS-terminating proxy (`--tls-ca` / `OXIDE_MESH_TLS_CA`).
    pub fn with_tls_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_ca_file = Some(path.into());
        self
    }

    /// Disables shell command execution on this client node.
    pub fn with_no_shell(mut self, no_shell: bool) -> Self {
        self.no_shell = no_shell;
        self
    }

    pub fn is_no_shell(&self) -> bool {
        self.no_shell
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn executed_commands_count(&self) -> usize {
        self.executed_count.load(Ordering::SeqCst)
    }

    pub fn active_commands_count(&self) -> usize {
        self.active_commands.load(Ordering::SeqCst)
    }

    pub async fn received_data_payloads(&self) -> Vec<AgentMeshEnvelope> {
        self.received_payloads
            .read()
            .await
            .items
            .iter()
            .cloned()
            .collect()
    }

    /// Validates any configured custom TLS CA certificate file or bytes and verifies that the TLS
    /// `Connector` can be constructed for `wss://` URLs. On plain `ws://` URLs, still validates
    /// `--tls-ca` so a missing or malformed CA file fails fast, logs a warning if a custom CA was
    /// passed on `ws://`, and returns `Ok(())`.
    pub fn validate_tls_config(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.build_tls_connector().map(|_| ())
    }

    fn build_tls_connector(
        &self,
    ) -> Result<Option<Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let is_wss = self
            .hub_url
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("wss://");

        let mut custom_store = tokio_rustls::rustls::RootCertStore::empty();
        let mut custom_certs = Vec::new();

        if let Some(ref ca_path) = self.tls_ca_file {
            let file_bytes = std::fs::read(ca_path).map_err(|e| {
                format!(
                    "failed to read TLS CA certificate file '{}': {e}",
                    ca_path.display()
                )
            })?;
            for cert in parse_certificates_pem_or_der(&file_bytes)? {
                custom_store.add(cert.clone()).map_err(|e| {
                    format!("invalid TLS CA certificate in '{}': {e}", ca_path.display())
                })?;
                custom_certs.push(cert);
            }
        }

        if let Some(ref ca_bytes) = self.tls_ca_bytes {
            for cert in parse_certificates_pem_or_der(ca_bytes)? {
                custom_store
                    .add(cert.clone())
                    .map_err(|e| format!("invalid custom TLS CA certificate: {e}"))?;
                custom_certs.push(cert);
            }
        }

        if !is_wss {
            if self.tls_ca_file.is_some() || self.tls_ca_bytes.is_some() {
                warn!(
                    "--tls-ca / OXIDE_MESH_TLS_CA is set, but hub URL '{}' uses plain ws://; custom TLS CA certificate is ignored on unencrypted connections",
                    self.hub_url
                );
            }
            return Ok(None);
        }

        let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        if !native.errors.is_empty() {
            warn!(
                "native root CA certificate loading warnings: {:?}",
                native.errors
            );
        }
        root_store.add_parsable_certificates(native.certs);
        for cert in custom_certs {
            root_store
                .add(cert)
                .map_err(|e| format!("invalid custom TLS CA certificate: {e}"))?;
        }

        if root_store.is_empty() {
            return Err("no root CA certificates available for wss:// connection".into());
        }

        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let client_config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(Some(Connector::Rustls(Arc::new(client_config))))
    }

    /// Establishes the WebSocket connection to the Hub and spawns the read/write message pump.
    pub async fn connect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let validated_inbox_dir = self.validate_inbox_config()?;
        info!("Connecting to OxideRelay Hub at {}...", self.hub_url);
        if token_sent_in_clear_to_remote_host(&self.hub_url, self.token.is_some()) {
            warn!(
                hub_url = %self.hub_url,
                "The hub token is sent over plain ws:// to a host that is not on loopback: anyone on the path can read it. Use wss:// behind a TLS proxy, a VPN or an SSH tunnel."
            );
        }
        let mut attempt = 0u32;
        let (ws_stream, _) = loop {
            let connector = self.build_tls_connector()?;
            match connect_async_tls_with_config(&self.hub_url, None, false, connector).await {
                Ok(pair) => break pair,
                Err(e) if attempt < 5 && is_transient_ws_connect_error(&e) => {
                    attempt += 1;
                    let delay = Duration::from_millis(50 * (1u64 << (attempt - 1)));
                    warn!(
                        hub_url = %self.hub_url,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "Transient socket error connecting to OxideRelay Hub; retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(Box::new(e)),
            }
        };
        let (mut ws_write, mut ws_read) = ws_stream.split();

        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<WsMessage>();
        *self.tx.write().await = Some(outbound_tx.clone());
        self.running.store(true, Ordering::SeqCst);

        // Spawn outbound writer task
        let running_clone = self.running.clone();
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if ws_write.send(msg).await.is_err() {
                    break;
                }
            }
            running_clone.store(false, Ordering::SeqCst);
        });

        // Send registration message
        let reg_corr_id = uuid::Uuid::new_v4().to_string();
        let (reg_tx, reg_rx) = oneshot::channel::<AgentMeshEnvelope>();
        let supported_commands = if self.no_shell {
            vec!["echo", "ping", "system_info", "sha256_verify"]
        } else {
            vec!["echo", "ping", "system_info", "shell_exec", "sha256_verify"]
        };

        let reg = AgentMeshEnvelope::NodeRegistration {
            version: "1.0".to_string(),
            correlation_id: reg_corr_id.clone(),
            from: self.node_id.clone(),
            to: "hub".to_string(),
            platform: self.platform.clone(),
            hostname: self.hostname.clone(),
            capabilities: serde_json::json!({
                "os": self.platform,
                "arch": std::env::consts::ARCH,
                "supported_commands": supported_commands,
            }),
            token: self.token.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        if let Ok(reg_json) = reg.to_json_string() {
            let _ = outbound_tx.send(WsMessage::Text(reg_json));
        }

        // Spawn inbound reader task
        let client_node_id = self.node_id.clone();
        let pending_map = self.pending.clone();
        let received_list = self.received_payloads.clone();
        let exec_counter = self.executed_count.clone();
        let active_counter = self.active_commands.clone();
        let out_tx = outbound_tx.clone();
        let running_flag = self.running.clone();
        let no_shell = self.no_shell;
        let inbox_dir_opt = validated_inbox_dir;
        let mut reg_waiter = Some(reg_tx);

        tokio::spawn(async move {
            let mut reassembler = ChunkReassembler::new();
            while let Some(msg_res) = ws_read.next().await {
                let msg = match msg_res {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("WebSocket read error: {}", e);
                        break;
                    }
                };

                match msg {
                    WsMessage::Text(raw_text) => {
                        let envelope = match AgentMeshEnvelope::from_json_str(&raw_text) {
                            Ok(env) => env,
                            Err(e) => {
                                warn!("Failed to parse incoming envelope: {}", e);
                                continue;
                            }
                        };

                        let target = envelope.to_node();
                        if target != client_node_id && target != "all" && target != "hub" {
                            // Message targeted to another node; ignore to prevent cross-talk
                            continue;
                        }

                        let corr_id = envelope.correlation_id().to_string();

                        // The hub's verdict on our registration is handed to `connect` directly.
                        if corr_id == reg_corr_id
                            && matches!(
                                envelope,
                                AgentMeshEnvelope::NodeRegistrationAck { .. }
                                    | AgentMeshEnvelope::DeliveryNack { .. }
                            )
                        {
                            if let Some(waiter) = reg_waiter.take() {
                                let _ = waiter.send(envelope);
                            }
                            continue;
                        }

                        #[allow(clippy::collapsible_match)]
                        match envelope {
                            AgentMeshEnvelope::CommandRequest {
                                ref from,
                                ref command,
                                ref args,
                                timeout_ms,
                                ..
                            } => {
                                exec_counter.fetch_add(1, Ordering::SeqCst);
                                active_counter.fetch_add(1, Ordering::SeqCst);
                                let active_guard = ActiveCommandGuard(active_counter.clone());
                                let sender = from.clone();
                                let cmd = command.clone();
                                let cmd_args = args.clone();
                                let node_id = client_node_id.clone();
                                let reply_tx = out_tx.clone();
                                let request_corr_id = corr_id.clone();

                                tokio::spawn(async move {
                                    let _guard = active_guard;
                                    info!(
                                        "[{}] Executing command '{}' from '{}'",
                                        node_id, cmd, sender
                                    );
                                    let exec_res = CommandExecutor::execute_guarded(
                                        &cmd, &cmd_args, timeout_ms, no_shell,
                                    )
                                    .await;

                                    let resp = AgentMeshEnvelope::new_command_response(
                                        node_id,
                                        sender,
                                        request_corr_id,
                                        Some(cmd),
                                        exec_res.status,
                                        exec_res.exit_code,
                                        exec_res.stdout,
                                        exec_res.stderr,
                                        exec_res.duration_ms,
                                    );

                                    if let Ok(json_str) = resp.to_json_string() {
                                        let _ = reply_tx.send(WsMessage::Text(json_str));
                                    }
                                });
                            }

                            AgentMeshEnvelope::DataPayload {
                                ref version,
                                ref from,
                                ref to,
                                ref payload_type,
                                sequence_number,
                                total_chunks,
                                ref data,
                                ref checksum_sha256,
                                ref payload,
                                ref metadata,
                                timestamp,
                                ..
                            } => {
                                let send_nack = |code: &str, reason: String| {
                                    let nack = AgentMeshEnvelope::new_delivery_nack(
                                        client_node_id.clone(),
                                        from,
                                        corr_id.clone(),
                                        code,
                                        reason,
                                    );
                                    if let Ok(json_str) = nack.to_json_string() {
                                        let _ = out_tx.send(WsMessage::Text(json_str));
                                    }
                                };

                                // 1. Strictly decode payload bytes (no silent char truncation).
                                let raw_bytes: Vec<u8> = if let Some(ref d) = data {
                                    let mut decoded = Vec::with_capacity(d.len());
                                    let mut bad_char = false;
                                    for c in d.chars() {
                                        match u8::try_from(u32::from(c)) {
                                            Ok(b) => decoded.push(b),
                                            Err(_) => {
                                                bad_char = true;
                                                break;
                                            }
                                        }
                                    }
                                    if bad_char {
                                        send_nack(
                                            ERR_PAYLOAD_CORRUPT,
                                            "Data payload contains non-Latin-1 code points"
                                                .to_string(),
                                        );
                                        continue;
                                    }
                                    decoded
                                } else if let Some(ref p) = payload {
                                    match serde_json::to_vec(p) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            send_nack(
                                                ERR_PAYLOAD_CORRUPT,
                                                format!("Failed to encode JSON payload: {e}"),
                                            );
                                            continue;
                                        }
                                    }
                                } else {
                                    send_nack(
                                        ERR_PAYLOAD_CORRUPT,
                                        "Missing data payload".to_string(),
                                    );
                                    continue;
                                };

                                // 2. Require and verify SHA-256 checksum before buffering/persisting.
                                let Some(ref claimed_sha) = checksum_sha256 else {
                                    send_nack(
                                        ERR_PAYLOAD_CORRUPT,
                                        "Missing SHA-256 checksum".to_string(),
                                    );
                                    continue;
                                };
                                if !verify_sha256(&raw_bytes, claimed_sha) {
                                    send_nack(
                                        ERR_PAYLOAD_CORRUPT,
                                        "SHA-256 checksum mismatch".to_string(),
                                    );
                                    continue;
                                }

                                // 3. Validate chunk sequence metadata.
                                let effective_total = if total_chunks == 0 && sequence_number == 0 {
                                    1
                                } else {
                                    total_chunks
                                };
                                if effective_total == 0
                                    || sequence_number >= effective_total
                                    || effective_total > MAX_CHUNKS_PER_TRANSFER
                                {
                                    send_nack(
                                        ERR_PAYLOAD_CORRUPT,
                                        format!(
                                            "Invalid chunk metadata: sequence_number={sequence_number}, total_chunks={total_chunks}"
                                        ),
                                    );
                                    continue;
                                }

                                let total_sha_hint = metadata
                                    .as_ref()
                                    .and_then(|m| m.get("total_sha256"))
                                    .map(String::as_str);

                                // 4. Reassemble multi-chunk transfers if needed.
                                let (final_bytes, completed_envelope) = if effective_total == 1 {
                                    if let Some(expected_total) = total_sha_hint {
                                        if !verify_sha256(&raw_bytes, expected_total) {
                                            send_nack(
                                                ERR_PAYLOAD_CORRUPT,
                                                "Payload total_sha256 mismatch".to_string(),
                                            );
                                            continue;
                                        }
                                    }
                                    (raw_bytes, envelope.clone())
                                } else {
                                    match reassembler.insert_chunk(
                                        from,
                                        &corr_id,
                                        sequence_number,
                                        effective_total,
                                        total_sha_hint,
                                        raw_bytes,
                                    ) {
                                        Ok(None) => continue,
                                        Ok(Some(ReassembledPayload {
                                            bytes: assembled,
                                            expected_total_sha256: expected_total,
                                        })) => {
                                            if let Some(ref total_sha) = expected_total {
                                                if !verify_sha256(&assembled, total_sha) {
                                                    send_nack(
                                                        ERR_PAYLOAD_CORRUPT,
                                                        "Assembled payload SHA-256 mismatch"
                                                            .to_string(),
                                                    );
                                                    continue;
                                                }
                                            }
                                            let assembled_hash = compute_sha256(&assembled);
                                            let assembled_text: String =
                                                assembled.iter().map(|&b| b as char).collect();
                                            let assembled_env = AgentMeshEnvelope::DataPayload {
                                                version: version.clone(),
                                                correlation_id: corr_id.clone(),
                                                from: from.clone(),
                                                to: to.clone(),
                                                payload_type: payload_type.clone(),
                                                sequence_number: 0,
                                                total_chunks: effective_total,
                                                chunk_size_bytes: assembled.len(),
                                                data: Some(assembled_text),
                                                checksum_sha256: Some(assembled_hash),
                                                payload: None,
                                                metadata: metadata.clone(),
                                                timestamp,
                                            };
                                            (assembled, assembled_env)
                                        }
                                        Err(reason) => {
                                            send_nack(ERR_PAYLOAD_CORRUPT, reason);
                                            continue;
                                        }
                                    }
                                };

                                // 5. Persist atomically to --inbox-dir if configured BEFORE ACK.
                                if let Some(ref dir) = inbox_dir_opt {
                                    let dir_clone = dir.clone();
                                    let corr_clone = corr_id.clone();
                                    let meta_clone = metadata.clone();
                                    let bytes_clone = final_bytes;
                                    let persist_res = tokio::task::spawn_blocking(move || {
                                        persist_payload_atomic(
                                            &dir_clone,
                                            &corr_clone,
                                            timestamp,
                                            meta_clone.as_ref(),
                                            &bytes_clone,
                                        )
                                    })
                                    .await;

                                    match persist_res {
                                        Ok(Ok(_saved_path)) => {}
                                        Ok(Err(err_msg)) => {
                                            send_nack(ERR_PERSIST_FAILED, err_msg);
                                            continue;
                                        }
                                        Err(join_err) => {
                                            send_nack(
                                                ERR_PERSIST_FAILED,
                                                format!(
                                                    "Inbox persistence task failed: {join_err}"
                                                ),
                                            );
                                            continue;
                                        }
                                    }
                                }

                                // 6. Buffer in bounded memory queue & send DeliveryAck.
                                // Only DeliveryAck/DeliveryNack should resolve `pending_map`
                                // (so self-targeted `from == to` waits for the verified ACK).
                                received_list.write().await.push(completed_envelope);

                                let ack = AgentMeshEnvelope::new_delivery_ack(
                                    client_node_id.clone(),
                                    from,
                                    corr_id.clone(),
                                    "data_received_verified",
                                );
                                if let Ok(json_str) = ack.to_json_string() {
                                    let _ = out_tx.send(WsMessage::Text(json_str));
                                }
                            }

                            AgentMeshEnvelope::DeliveryAck { ref status, .. } => {
                                if status != "target_forwarded" {
                                    if let Some(sender_channel) =
                                        pending_map.write().await.remove(&corr_id)
                                    {
                                        let _ = sender_channel.send(envelope);
                                    }
                                }
                            }

                            AgentMeshEnvelope::CommandResponse { .. }
                            | AgentMeshEnvelope::DeliveryNack { .. }
                            | AgentMeshEnvelope::NodeListResponse { .. }
                            | AgentMeshEnvelope::NodeRegistrationAck { .. } => {
                                if let Some(sender_channel) =
                                    pending_map.write().await.remove(&corr_id)
                                {
                                    let _ = sender_channel.send(envelope);
                                }
                            }

                            _ => {}
                        }
                    }
                    WsMessage::Ping(p) => {
                        let _ = out_tx.send(WsMessage::Pong(p));
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
            running_flag.store(false, Ordering::SeqCst);
            pending_map.write().await.clear();
        });

        match tokio::time::timeout(REGISTRATION_TIMEOUT, reg_rx).await {
            Ok(Ok(AgentMeshEnvelope::DeliveryNack {
                error_code, reason, ..
            })) => {
                self.close().await;
                Err(Box::new(RegistrationRejected { error_code, reason }))
            }
            Ok(Ok(_accepted)) => Ok(()),
            Ok(Err(_)) => {
                self.close().await;
                Err("hub closed the connection before accepting the registration".into())
            }
            Err(_) => {
                warn!(
                    "Hub did not answer the registration within {:?}; continuing",
                    REGISTRATION_TIMEOUT
                );
                Ok(())
            }
        }
    }

    /// Sends a structured command to a target node and awaits the result.
    pub async fn send_command(
        &self,
        target_id: &str,
        command: &str,
        args: Value,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let corr_id = uuid::Uuid::new_v4().to_string();
        let (rx_tx, rx_fut) = oneshot::channel::<AgentMeshEnvelope>();
        self.pending.write().await.insert(corr_id.clone(), rx_tx);

        let timeout_val = timeout_ms.unwrap_or(15000);
        let req = AgentMeshEnvelope::CommandRequest {
            version: "1.0".to_string(),
            correlation_id: corr_id.clone(),
            from: self.node_id.clone(),
            to: target_id.to_string(),
            command: command.to_string(),
            args,
            payload: None,
            timeout_ms: Some(timeout_val),
            require_ack: true,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.send_envelope(req).await?;

        // Await response or timeout
        let res = tokio::time::timeout(Duration::from_millis(timeout_val + 2000), rx_fut).await??;
        Ok(res)
    }

    /// Transmits a data payload (with SHA-256 verification) to a target node without waiting
    /// for a delivery acknowledgement. Automatically chunks payloads above `DEFAULT_DATA_CHUNK_BYTES`.
    pub async fn send_data_payload(
        &self,
        target_id: &str,
        data_bytes: &[u8],
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        let (envelopes, corr_id, hash) = AgentMeshEnvelope::new_data_payload_chunks(
            &self.node_id,
            target_id,
            data_bytes,
            DEFAULT_DATA_CHUNK_BYTES,
        );
        for env in envelopes {
            self.send_envelope(env).await?;
        }
        Ok((corr_id, hash))
    }

    /// Transmits a data payload (chunking automatically above `DEFAULT_DATA_CHUNK_BYTES`) and
    /// awaits consumer verification (`DeliveryAck`) or hub/consumer rejection (`DeliveryNack`).
    pub async fn send_data_payload_with_ack(
        &self,
        target_id: &str,
        data_bytes: &[u8],
        timeout_ms: Option<u64>,
    ) -> Result<DataDeliveryReceipt, Box<dyn std::error::Error + Send + Sync>> {
        self.send_data_payload_chunked_with_ack(
            target_id,
            data_bytes,
            DEFAULT_DATA_CHUNK_BYTES,
            timeout_ms,
        )
        .await
    }

    /// Transmits a data payload using an explicit chunk size limit and awaits consumer
    /// verification (`DeliveryAck`) or hub/consumer rejection (`DeliveryNack`).
    pub async fn send_data_payload_chunked_with_ack(
        &self,
        target_id: &str,
        data_bytes: &[u8],
        max_chunk_bytes: usize,
        timeout_ms: Option<u64>,
    ) -> Result<DataDeliveryReceipt, Box<dyn std::error::Error + Send + Sync>> {
        let (envelopes, corr_id, sha256) = AgentMeshEnvelope::new_data_payload_chunks(
            &self.node_id,
            target_id,
            data_bytes,
            max_chunk_bytes,
        );
        let total_chunks = envelopes.len() as u32;

        let (rx_tx, rx_fut) = oneshot::channel::<AgentMeshEnvelope>();
        self.pending.write().await.insert(corr_id.clone(), rx_tx);

        for env in envelopes {
            if let Err(e) = self.send_envelope(env).await {
                self.pending.write().await.remove(&corr_id);
                return Err(e);
            }
        }

        let wait_ms = timeout_ms.unwrap_or(10_000).max(1);
        match tokio::time::timeout(Duration::from_millis(wait_ms), rx_fut).await {
            Ok(Ok(AgentMeshEnvelope::DeliveryAck { from, status, .. })) => {
                Ok(DataDeliveryReceipt {
                    correlation_id: corr_id,
                    sha256,
                    total_chunks,
                    responder: from,
                    status,
                })
            }
            Ok(Ok(AgentMeshEnvelope::DeliveryNack {
                error_code, reason, ..
            })) => Err(format!("Data delivery rejected ({error_code}): {reason}").into()),
            Ok(Ok(other)) => {
                Err(format!("Unexpected delivery response: {}", other.msg_type()).into())
            }
            Ok(Err(_)) => {
                Err("Connection closed while awaiting data delivery acknowledgement".into())
            }
            Err(_) => {
                self.pending.write().await.remove(&corr_id);
                Err(format!(
                    "Timed out waiting for data delivery acknowledgement after {wait_ms} ms"
                )
                .into())
            }
        }
    }

    /// Queries the Hub for the list of online nodes.
    pub async fn query_node_list(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<NodeDescriptor>, Box<dyn std::error::Error + Send + Sync>> {
        let corr_id = uuid::Uuid::new_v4().to_string();
        let (rx_tx, rx_fut) = oneshot::channel::<AgentMeshEnvelope>();
        self.pending.write().await.insert(corr_id.clone(), rx_tx);

        let req = AgentMeshEnvelope::NodeList {
            version: "1.0".to_string(),
            correlation_id: corr_id,
            from: self.node_id.clone(),
            to: "hub".to_string(),
            filter_platform: None,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.send_envelope(req).await?;

        let timeout_val = timeout_ms.unwrap_or(5000);
        let res = tokio::time::timeout(Duration::from_millis(timeout_val), rx_fut).await??;

        if let AgentMeshEnvelope::NodeListResponse { nodes, .. } = res {
            Ok(nodes)
        } else {
            Err("Expected NodeListResponse envelope".into())
        }
    }

    /// Submits a GPU/matrix compute job to the OxideSwarm Grid via the Agent-Grid Bridge.
    pub async fn submit_grid_compute(
        &self,
        kernel_name: &str,
        matrix_dim: u32,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let args = serde_json::json!({
            "kernel_name": kernel_name,
            "kernel": kernel_name,
            "matrix_dim": matrix_dim,
            "simulated_matrix_dim": matrix_dim,
        });
        self.send_command("grid", "grid_compute", args, timeout_ms)
            .await
    }

    /// Submits a distributed Rust compilation job to the OxideSwarm Grid via the Agent-Grid Bridge.
    pub async fn submit_grid_compilation<I>(
        &self,
        crate_name: &str,
        source_files: I,
        compiler_flags: Vec<String>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let files: HashMap<String, String> = source_files.into_iter().collect();
        let args = serde_json::json!({
            "crate_name": crate_name,
            "source_files": files,
            "compiler_flags": compiler_flags,
        });
        self.send_command("grid", "grid_compile", args, timeout_ms.or(Some(30000)))
            .await
    }

    /// Convenience alias for `submit_grid_compilation`.
    pub async fn submit_grid_compile(
        &self,
        crate_name: &str,
        files: HashMap<String, String>,
        flags: Vec<String>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.submit_grid_compilation(crate_name, files, flags, timeout_ms)
            .await
    }

    /// Queries cluster status and scheduler queue metrics from the Master Grid scheduler.
    pub async fn query_grid_status(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.send_command(
            "grid",
            "grid_status",
            serde_json::json!({}),
            timeout_ms.or(Some(5000)),
        )
        .await
    }

    /// Queries grid status or specific task info.
    pub async fn submit_grid_status(
        &self,
        task_id: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let mut args = serde_json::json!({});
        if let Some(id) = task_id {
            args["task_id"] = serde_json::json!(id);
        }
        self.send_command("grid", "grid_status", args, timeout_ms.or(Some(5000)))
            .await
    }

    /// Submits a distributed 3D generative task to the OxideSwarm Grid.
    pub async fn generate_3d(
        &self,
        prompt: Option<&str>,
        image_bytes: Option<&[u8]>,
        quality: Option<&str>,
        engine: Option<&str>,
        export_texture: Option<bool>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        let mut args = serde_json::json!({
            "quality": quality.unwrap_or("standard"),
            "engine": engine.unwrap_or("auto"),
            "export_texture": export_texture.unwrap_or(true),
        });

        if let Some(p) = prompt {
            args["prompt"] = serde_json::json!(p);
        }
        if let Some(img) = image_bytes {
            args["image"] = serde_json::json!(STANDARD.encode(img));
        }

        self.send_command("grid", "grid_3d_generate", args, timeout_ms.or(Some(60000)))
            .await
    }

    /// Convenience method to generate 3D asset from a text prompt.
    pub async fn generate_3d_from_prompt(
        &self,
        prompt: &str,
        quality: Option<&str>,
        engine: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.generate_3d(Some(prompt), None, quality, engine, None, timeout_ms)
            .await
    }

    /// Convenience method to generate 3D asset from an image.
    pub async fn generate_3d_from_image(
        &self,
        image_bytes: &[u8],
        quality: Option<&str>,
        engine: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.generate_3d(None, Some(image_bytes), quality, engine, None, timeout_ms)
            .await
    }

    /// Submits an AutoRig task to the OxideSwarm Grid.
    pub async fn auto_rig(
        &self,
        input_mesh: &[u8],
        rig_preset: Option<&str>,
        embed_animations: Option<Vec<String>>,
        preferred_provider: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        let mut args = serde_json::json!({
            "input_mesh": STANDARD.encode(input_mesh),
            "rig_preset": rig_preset.unwrap_or("biped"),
            "embed_animations": embed_animations.unwrap_or_default(),
        });
        if let Some(p) = preferred_provider {
            args["preferred_provider"] = serde_json::json!(p);
        }

        self.send_command("grid", "grid_auto_rig", args, timeout_ms.or(Some(60000)))
            .await
    }

    /// Extracts and cryptographically verifies binary asset bytes from a CommandResponse envelope.
    pub fn extract_asset_bytes(
        envelope: &AgentMeshEnvelope,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        match envelope {
            AgentMeshEnvelope::CommandResponse {
                payload: Some(payload),
                status,
                ..
            } => {
                if status != "success" {
                    return Err(format!(
                        "Command response status is '{status}', expected 'success'"
                    )
                    .into());
                }
                let b64_str = payload
                    .get("asset_base64")
                    .and_then(|v| v.as_str())
                    .ok_or("Payload does not contain 'asset_base64'")?;
                let bytes = STANDARD.decode(b64_str)?;
                if let Some(expected_sha) = payload.get("asset_sha256").and_then(|v| v.as_str()) {
                    if !verify_sha256(&bytes, expected_sha) {
                        return Err(format!(
                            "Asset SHA-256 verification failed: expected {expected_sha}, got {}",
                            compute_sha256(&bytes)
                        )
                        .into());
                    }
                }
                Ok(bytes)
            }
            _ => Err("Envelope is not a CommandResponse with payload".into()),
        }
    }

    /// Submits a general task (`grid_submit`) to the OxideSwarm Grid via the Agent-Grid Bridge.
    pub async fn submit_grid(
        &self,
        args: Value,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.send_command("grid", "grid_submit", args, timeout_ms)
            .await
    }

    /// Queries or waits for the result of a previously submitted Grid task (`grid_result`).
    pub async fn get_grid_result(
        &self,
        task_id: &str,
        wait: bool,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let args = serde_json::json!({
            "task_id": task_id,
            "wait": wait,
        });
        self.send_command("grid", "grid_result", args, timeout_ms)
            .await
    }

    /// Cancels a Grid task by ID (`grid_cancel`).
    pub async fn cancel_grid_task(
        &self,
        task_id: &str,
        timeout_ms: Option<u64>,
    ) -> Result<AgentMeshEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let args = serde_json::json!({
            "task_id": task_id,
        });
        self.send_command("grid", "grid_cancel", args, timeout_ms.or(Some(5000)))
            .await
    }

    /// Extracts and SHA-256-verifies output artifacts from a `grid_submit` or `grid_result`
    /// `CommandResponse` envelope.
    pub fn extract_output_artifacts(
        envelope: &AgentMeshEnvelope,
    ) -> Result<Vec<ExtractedArtifact>, Box<dyn std::error::Error + Send + Sync>> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        match envelope {
            AgentMeshEnvelope::CommandResponse {
                payload: Some(payload),
                status,
                ..
            } => {
                if status != "success" {
                    return Err(format!(
                        "Command response status is '{status}', expected 'success'"
                    )
                    .into());
                }
                let Some(arr) = payload.get("artifacts").and_then(|v| v.as_array()) else {
                    return Ok(Vec::new());
                };
                let mut out = Vec::with_capacity(arr.len());
                for item in arr {
                    let path = item
                        .get("path")
                        .and_then(|v| v.as_str())
                        .ok_or("Artifact entry missing 'path'")?
                        .to_string();
                    let b64 = item
                        .get("data_base64")
                        .and_then(|v| v.as_str())
                        .ok_or("Artifact entry missing 'data_base64'")?;
                    let bytes = STANDARD.decode(b64)?;
                    let sha256 = item
                        .get("sha256")
                        .and_then(|v| v.as_str())
                        .ok_or("Artifact entry missing 'sha256'")?
                        .to_string();
                    if !verify_sha256(&bytes, &sha256) {
                        return Err(format!(
                            "Artifact '{path}' SHA-256 verification failed: expected {sha256}, got {}",
                            compute_sha256(&bytes)
                        )
                        .into());
                    }
                    let declared_size = item
                        .get("size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(bytes.len() as u64);
                    if declared_size != bytes.len() as u64 {
                        return Err(format!(
                            "Artifact '{path}' size mismatch: declared {declared_size}, decoded {}",
                            bytes.len()
                        )
                        .into());
                    }
                    out.push(ExtractedArtifact {
                        path,
                        size: declared_size,
                        sha256,
                        bytes,
                    });
                }
                Ok(out)
            }
            _ => Err("Envelope is not a CommandResponse with payload".into()),
        }
    }

    pub async fn send_envelope(
        &self,
        envelope: AgentMeshEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let json_str = envelope.to_json_string()?;
        let tx_guard = self.tx.read().await;
        if let Some(ref tx) = *tx_guard {
            tx.send(WsMessage::Text(json_str))
                .map_err(|e| format!("Failed to send outbound message: {}", e))?;
            Ok(())
        } else {
            Err("Client is not connected".into())
        }
    }

    pub async fn close(&self) {
        self.running.store(false, Ordering::SeqCst);
        let mut tx_guard = self.tx.write().await;
        if let Some(tx) = tx_guard.take() {
            let _ = tx.send(WsMessage::Close(None));
        }
    }

    pub fn is_connected(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Runs a long-lived agent node loop with automatic reconnection.
    /// Validates TLS and inbox directory configuration upfront before entering the reconnect loop.
    pub async fn run_daemon(&self) {
        if let Err(e) = self.validate_tls_config() {
            tracing::error!("Invalid TLS configuration: {e}");
            return;
        }
        if let Err(e) = self.validate_inbox_config() {
            tracing::error!("Invalid inbox directory configuration: {e}");
            return;
        }
        let mut sys = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::new()
                .with_cpu(sysinfo::CpuRefreshKind::new().with_cpu_usage())
                .with_memory(sysinfo::MemoryRefreshKind::new().with_ram()),
        );
        loop {
            if let Err(e) = self.connect().await {
                let delay = match e.downcast_ref::<RegistrationRejected>() {
                    Some(rej)
                        if rej.error_code == ERR_UNAUTHORIZED
                            || rej.error_code == ERR_INVALID_NODE_ID =>
                    {
                        tracing::error!(
                            "{}. Fix the token / node id; retrying in {}s.",
                            rej,
                            REJECTED_RETRY_DELAY.as_secs()
                        );
                        REJECTED_RETRY_DELAY
                    }
                    Some(rej) if rej.error_code == ERR_DUPLICATE_NODE_ID => {
                        // The previous session of this node may simply not have timed out yet.
                        warn!("{}. Retrying in 10s...", rej);
                        Duration::from_secs(10)
                    }
                    _ => {
                        warn!("Connection to hub failed: {}. Reconnecting in 3s...", e);
                        Duration::from_secs(3)
                    }
                };
                tokio::time::sleep(delay).await;
                continue;
            }

            // Heartbeat ticker loop
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            while self.is_connected() {
                interval.tick().await;
                sys.refresh_cpu_specifics(sysinfo::CpuRefreshKind::new().with_cpu_usage());
                sys.refresh_memory_specifics(sysinfo::MemoryRefreshKind::new().with_ram());
                let cpu_usage_pct = Some(sys.global_cpu_info().cpu_usage());
                let avail_bytes = sys.available_memory();
                let mem_bytes = if avail_bytes > 0 {
                    avail_bytes
                } else {
                    sys.free_memory()
                };
                let memory_free_mb = Some(mem_bytes / (1024 * 1024));
                let active_commands = Some(self.active_commands.load(Ordering::SeqCst));

                let hb = AgentMeshEnvelope::Heartbeat {
                    version: "1.0".to_string(),
                    correlation_id: uuid::Uuid::new_v4().to_string(),
                    from: self.node_id.clone(),
                    to: "hub".to_string(),
                    cpu_usage_pct,
                    memory_free_mb,
                    active_commands,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                };
                if let Err(e) = self.send_envelope(hb).await {
                    warn!("Failed to send heartbeat: {}", e);
                    break;
                }
            }

            warn!("Connection lost. Reconnecting in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::AgentMeshHub;

    #[test]
    fn test_cleartext_token_warning_predicate() {
        let warns = |url: &str, tok: bool| token_sent_in_clear_to_remote_host(url, tok);
        // Loopback hosts never warn.
        assert!(!warns("ws://127.0.0.1:8088/ws", true));
        assert!(!warns("ws://localhost:8088/ws", true));
        assert!(!warns("ws://LOCALHOST/ws", true));
        assert!(!warns("ws://[::1]:8088/ws", true));
        assert!(!warns("ws://127.5.5.5:1/ws", true));
        // No token, nothing to leak.
        assert!(!warns("ws://192.0.2.1:8088/ws", false));
        // wss:// is encrypted.
        assert!(!warns("wss://hub.example.com/ws", true));
        // A remote ws:// host with a token warns, userinfo tricks included.
        assert!(warns("ws://192.0.2.1:8088/ws", true));
        assert!(warns("ws://hub.example.com/ws", true));
        assert!(warns("ws://[2001:db8::1]:8088/ws", true));
        assert!(warns("ws://127.0.0.1@192.0.2.1:8088/ws", true));
        assert!(warns("ws://localhost.example.com/ws", true));
    }

    #[tokio::test]
    async fn test_client_connect_and_node_list() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let client1 = AgentMeshClient::new(&hub_url, "test-client-1", "test-os");
        client1.connect().await.expect("Client 1 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let nodes = client1
            .query_node_list(Some(3000))
            .await
            .expect("Query failed");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, "test-client-1");

        client1.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_client_send_command_e2e() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let node1 = AgentMeshClient::new(&hub_url, "node-sender", "windows");
        let node2 = AgentMeshClient::new(&hub_url, "node-executor", "linux");

        node1.connect().await.expect("Node 1 failed to connect");
        node2.connect().await.expect("Node 2 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let res = node1
            .send_command(
                "node-executor",
                "echo",
                serde_json::json!({"message": "Hello Rust Agent Mesh"}),
                Some(3000),
            )
            .await
            .expect("Command send failed");

        if let AgentMeshEnvelope::CommandResponse {
            status,
            exit_code,
            stdout,
            from,
            to,
            ..
        } = res
        {
            assert_eq!(status, "success");
            assert_eq!(exit_code, 0);
            assert_eq!(stdout, "Hello Rust Agent Mesh");
            assert_eq!(from, "node-executor");
            assert_eq!(to, "node-sender");
        } else {
            panic!("Expected CommandResponse, got {:?}", res);
        }

        assert_eq!(node2.executed_commands_count(), 1);
        assert_eq!(node1.executed_commands_count(), 0);

        node1.close().await;
        node2.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_client_send_data_payload_e2e() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let node1 = AgentMeshClient::new(&hub_url, "node-data-src", "macos");
        let node2 = AgentMeshClient::new(&hub_url, "node-data-dst", "android");

        node1.connect().await.expect("Node 1 failed to connect");
        node2.connect().await.expect("Node 2 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let sample_data = b"Arbitrary binary data payload 1234567890 \x00\x01\x02\xFF";
        let (_corr_id, hash) = node1
            .send_data_payload("node-data-dst", sample_data)
            .await
            .expect("Failed to send data payload");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let received = node2.received_data_payloads().await;
        assert_eq!(received.len(), 1);
        if let AgentMeshEnvelope::DataPayload {
            checksum_sha256,
            from,
            to,
            ..
        } = &received[0]
        {
            assert_eq!(from, "node-data-src");
            assert_eq!(to, "node-data-dst");
            assert_eq!(checksum_sha256.as_deref(), Some(hash.as_str()));
        } else {
            panic!("Expected DataPayload, got {:?}", received[0]);
        }

        node1.close().await;
        node2.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_client_send_unknown_target_nack() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let node1 = AgentMeshClient::new(&hub_url, "node-caller", "windows");
        node1.connect().await.expect("Node 1 failed to connect");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let res = node1
            .send_command(
                "non-existent-target",
                "ping",
                serde_json::Value::Null,
                Some(2000),
            )
            .await
            .expect("Send command should resolve with NACK");

        if let AgentMeshEnvelope::DeliveryNack {
            error_code, reason, ..
        } = res
        {
            assert_eq!(error_code, "ERR_NODE_NOT_FOUND");
            assert!(reason.contains("non-existent-target"));
        } else {
            panic!("Expected DeliveryNack, got {:?}", res);
        }

        node1.close().await;
        hub.stop().await;
    }

    // 2-tier test PKI (Root CA, Unrelated Root CA, Leaf for DNS:localhost/IP:127.0.0.1,
    // and Leaf for DNS:other.example, all valid 2020-2050).
    const TEST_CA_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBeDCCAR6gAwIBAgICEAEwCgYIKoZIzj0EAwIwIjEgMB4GA1UEAwwXT3hpZGVT\n\
d2FybSBUZXN0IFJvb3QgQ0EwIBcNMjAwMTAxMDAwMDAwWhgPMjA1MDAxMDEwMDAw\n\
MDBaMCIxIDAeBgNVBAMMF094aWRlU3dhcm0gVGVzdCBSb290IENBMFkwEwYHKoZI\n\
zj0CAQYIKoZIzj0DAQcDQgAEGUtFHooy4iOGsqRgnZunQ9MHT0uRFbUKc26wWFZI\n\
pgvSEAcdziYIQ3c4xWIX6P8+SAeY1kbM53gqZVtttwdUi6NCMEAwDwYDVR0TAQH/\n\
BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAQYwHQYDVR0OBBYEFPqANY0HG1TFirqsk75t\n\
xFmt6Qq0MAoGCCqGSM49BAMCA0gAMEUCIQD6tryK4FO3UIcRBe89jJeURHjK47g4\n\
IecdN7BXxcZRjAIgV1SJEA2qws9UGjHFKzCKVhBFVvrICNkBkCwemuhh0p4=\n\
-----END CERTIFICATE-----\n";

    const UNRELATED_CA_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBdTCCARygAwIBAgICEAIwCgYIKoZIzj0EAwIwITEfMB0GA1UEAwwWVW5yZWxh\n\
dGVkIFRlc3QgUm9vdCBDQTAgFw0yMDAxMDEwMDAwMDBaGA8yMDUwMDEwMTAwMDAw\n\
MFowITEfMB0GA1UEAwwWVW5yZWxhdGVkIFRlc3QgUm9vdCBDQTBZMBMGByqGSM49\n\
AgEGCCqGSM49AwEHA0IABEJENyvaaTJJ+wj80GNARb11mRAYHzyBNoCA5TwUdovn\n\
npmoe5JqLwvrn9mZbTY3JATsnMNcTNSP4yQ1lwnaM7WjQjBAMA8GA1UdEwEB/wQF\n\
MAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQWBBQ1sj9RCdtIHLtRXTvHH9lH\n\
h6n8ETAKBggqhkjOPQQDAgNHADBEAiBjoToetl4mcQH5nOBqBb75kBIOkn1dDCXY\n\
BPHGmMoyVgIgU0eI1PBP0EJpRumb05JLsaVUmdgk0+iTW84ptstqHeA=\n\
-----END CERTIFICATE-----\n";

    const TEST_LEAF_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBujCCAWGgAwIBAgICIAEwCgYIKoZIzj0EAwIwIjEgMB4GA1UEAwwXT3hpZGVT\n\
d2FybSBUZXN0IFJvb3QgQ0EwIBcNMjAwMTAxMDAwMDAwWhgPMjA1MDAxMDEwMDAw\n\
MDBaMBQxEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEH\n\
A0IABG3SNG+bewyyonB/In/F16clQ7U7S9lwQ4F+lNDg/vGDyxSFQ3r1myX/nC6B\n\
zv3aI4Fzj7LwkYWF1Kostha9J56jgZIwgY8wDAYDVR0TAQH/BAIwADAOBgNVHQ8B\n\
Af8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwGgYDVR0RBBMwEYIJbG9jYWxo\n\
b3N0hwR/AAABMB0GA1UdDgQWBBTdPmrPjNfEA3YH48JbE1SVQ55QBjAfBgNVHSME\n\
GDAWgBT6gDWNBxtUxYq6rJO+bcRZrekKtDAKBggqhkjOPQQDAgNHADBEAiBP9/V3\n\
nwSALyDVgCxHY1OeBXqyiVe5uUdpkP39YCGUFgIgCPLX6cec8I6YEqNHQUkto4yh\n\
R10F8AraCk7v/U/6jxc=\n\
-----END CERTIFICATE-----\n";

    const TEST_LEAF_KEY_PKCS8_DER: &[u8] = &[
        0x30, 0x81, 0x87, 0x02, 0x01, 0x00, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d,
        0x02, 0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x04, 0x6d, 0x30,
        0x6b, 0x02, 0x01, 0x01, 0x04, 0x20, 0x0d, 0x8e, 0x78, 0x26, 0x47, 0x17, 0x8c, 0x4b, 0xb5,
        0x53, 0x34, 0xf6, 0x47, 0x8c, 0x92, 0xde, 0xc0, 0x39, 0xfa, 0x80, 0xec, 0xa8, 0xec, 0xac,
        0x3e, 0x2c, 0x84, 0xdf, 0x3a, 0xc2, 0x9e, 0xaa, 0xa1, 0x44, 0x03, 0x42, 0x00, 0x04, 0x6d,
        0xd2, 0x34, 0x6f, 0x9b, 0x7b, 0x0c, 0xb2, 0xa2, 0x70, 0x7f, 0x22, 0x7f, 0xc5, 0xd7, 0xa7,
        0x25, 0x43, 0xb5, 0x3b, 0x4b, 0xd9, 0x70, 0x43, 0x81, 0x7e, 0x94, 0xd0, 0xe0, 0xfe, 0xf1,
        0x83, 0xcb, 0x14, 0x85, 0x43, 0x7a, 0xf5, 0x9b, 0x25, 0xff, 0x9c, 0x2e, 0x81, 0xce, 0xfd,
        0xda, 0x23, 0x81, 0x73, 0x8f, 0xb2, 0xf0, 0x91, 0x85, 0x85, 0xd4, 0xaa, 0x2c, 0xb6, 0x16,
        0xbd, 0x27, 0x9e,
    ];

    const WRONG_HOST_LEAF_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBvTCCAWOgAwIBAgICIAIwCgYIKoZIzj0EAwIwIjEgMB4GA1UEAwwXT3hpZGVT\n\
d2FybSBUZXN0IFJvb3QgQ0EwIBcNMjAwMTAxMDAwMDAwWhgPMjA1MDAxMDEwMDAw\n\
MDBaMBgxFjAUBgNVBAMMDW90aGVyLmV4YW1wbGUwWTATBgcqhkjOPQIBBggqhkjO\n\
PQMBBwNCAAQv1XJ99ZJVKfRXNsMkXsInIupAhpwPFKTVrH5qGI5e5ELdPI9TzOX1\n\
ZuifU616x5BIZw6K1j9XzuVurFrycuoDo4GQMIGNMAwGA1UdEwEB/wQCMAAwDgYD\n\
VR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBgGA1UdEQQRMA+CDW90\n\
aGVyLmV4YW1wbGUwHQYDVR0OBBYEFKPOW7YgYyba9WaS5i7/aDaO/wAlMB8GA1Ud\n\
IwQYMBaAFPqANY0HG1TFirqsk75txFmt6Qq0MAoGCCqGSM49BAMCA0gAMEUCIQD7\n\
Awqiz2dEoo0u/ek78WOGb2h+PVsKTUeFAnQc7q7C7wIgNOPgPgYWqKkIuGBeQk5C\n\
2ijXI6Qm3gvttry7sLFf9ZU=\n\
-----END CERTIFICATE-----\n";

    const WRONG_HOST_LEAF_KEY_PKCS8_DER: &[u8] = &[
        0x30, 0x81, 0x87, 0x02, 0x01, 0x00, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d,
        0x02, 0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x04, 0x6d, 0x30,
        0x6b, 0x02, 0x01, 0x01, 0x04, 0x20, 0x4e, 0x6d, 0x15, 0x24, 0x61, 0x4f, 0x20, 0x8d, 0x7e,
        0xa4, 0x3b, 0x6e, 0x78, 0xe0, 0xe8, 0xe5, 0xa1, 0xf3, 0xd8, 0xdb, 0xbf, 0x95, 0xc3, 0xe7,
        0x9b, 0x81, 0xb3, 0xcb, 0x63, 0xa0, 0x05, 0x1a, 0xa1, 0x44, 0x03, 0x42, 0x00, 0x04, 0x2f,
        0xd5, 0x72, 0x7d, 0xf5, 0x92, 0x55, 0x29, 0xf4, 0x57, 0x36, 0xc3, 0x24, 0x5e, 0xc2, 0x27,
        0x22, 0xea, 0x40, 0x86, 0x9c, 0x0f, 0x14, 0xa4, 0xd5, 0xac, 0x7e, 0x6a, 0x18, 0x8e, 0x5e,
        0xe4, 0x42, 0xdd, 0x3c, 0x8f, 0x53, 0xcc, 0xe5, 0xf5, 0x66, 0xe8, 0x9f, 0x53, 0xad, 0x7a,
        0xc7, 0x90, 0x48, 0x67, 0x0e, 0x8a, 0xd6, 0x3f, 0x57, 0xce, 0xe5, 0x6e, 0xac, 0x5a, 0xf2,
        0x72, 0xea, 0x03,
    ];

    #[test]
    fn test_parse_certificates_pem_variants_bom_text_pkcs12_der_and_bundle() {
        let mut store = tokio_rustls::rustls::RootCertStore::empty();

        // 1. `openssl x509 -text` output before -----BEGIN CERTIFICATE-----
        let x509_text_pem = format!(
            "Certificate:\n    Data:\n        Version: 3 (0x2)\n        Subject: CN = OxideSwarm Test Root CA\n{TEST_CA_CERT_PEM}"
        );
        let certs_text = parse_certificates_pem_or_der(x509_text_pem.as_bytes())
            .expect("openssl x509 -text PEM must load");
        assert_eq!(certs_text.len(), 1);
        store
            .add(certs_text[0].clone())
            .expect("x509 -text cert must be valid DER");

        // 2. `openssl pkcs12` "Bag Attributes" output before -----BEGIN CERTIFICATE-----
        let pkcs12_pem = format!(
            "Bag Attributes\n    friendlyName: OxideSwarm Test Root CA\n    localKeyID: 01 02 03 04\nsubject=CN = OxideSwarm Test Root CA\nissuer=CN = OxideSwarm Test Root CA\n{TEST_CA_CERT_PEM}"
        );
        let certs_pkcs12 = parse_certificates_pem_or_der(pkcs12_pem.as_bytes())
            .expect("openssl pkcs12 Bag Attributes PEM must load");
        assert_eq!(certs_pkcs12.len(), 1);
        assert_eq!(certs_pkcs12[0].as_ref(), certs_text[0].as_ref());

        // 3. UTF-8 BOM + CRLF line endings (PowerShell 5.1 `-Encoding UTF8`)
        let bom_crlf_pem = format!("\u{feff}{}", TEST_CA_CERT_PEM.replace('\n', "\r\n"));
        let certs_bom = parse_certificates_pem_or_der(bom_crlf_pem.as_bytes())
            .expect("UTF-8 BOM + CRLF PEM must load");
        assert_eq!(certs_bom.len(), 1);
        assert_eq!(certs_bom[0].as_ref(), certs_text[0].as_ref());

        // 4. Raw DER bytes
        let certs_der = parse_certificates_pem_or_der(certs_text[0].as_ref())
            .expect("raw DER certificate must load");
        assert_eq!(certs_der.len(), 1);
        assert_eq!(certs_der[0].as_ref(), certs_text[0].as_ref());

        // 5. Multi-certificate PEM bundle (unrelated CA first, target CA second)
        let bundle_pem = format!("{UNRELATED_CA_CERT_PEM}\n{TEST_CA_CERT_PEM}");
        let certs_bundle = parse_certificates_pem_or_der(bundle_pem.as_bytes())
            .expect("2-certificate PEM bundle must load");
        assert_eq!(
            certs_bundle.len(),
            2,
            "all certificates in a PEM bundle must be loaded"
        );
        assert_eq!(certs_bundle[1].as_ref(), certs_text[0].as_ref());
    }

    #[tokio::test]
    async fn test_client_wss_connect_via_tls_terminating_proxy() {
        use crate::hub::HubConfig;
        use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
        use tokio_rustls::TlsAcceptor;

        let hub_token = "mesh-wss-proxy-secret-token-0123456789";
        let hub = AgentMeshHub::with_config(HubConfig::default().with_auth_token(hub_token));
        let hub_port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");

        let leaf_certs = parse_certificates_pem_or_der(TEST_LEAF_CERT_PEM.as_bytes()).unwrap();
        let leaf_key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(TEST_LEAF_KEY_PKCS8_DER.to_vec()));
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let server_config =
            tokio_rustls::rustls::ServerConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(leaf_certs, leaf_key)
                .expect("valid test server TLS config");
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind TLS proxy");
        let proxy_port = proxy_listener.local_addr().unwrap().port();
        let proxy_task = tokio::spawn(async move {
            while let Ok((inbound, _)) = proxy_listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut tls_stream) = acceptor.accept(inbound).await {
                        if let Ok(mut upstream) =
                            tokio::net::TcpStream::connect(("127.0.0.1", hub_port)).await
                        {
                            let _ =
                                tokio::io::copy_bidirectional(&mut tls_stream, &mut upstream).await;
                        }
                    }
                });
            }
        });

        // Dial 127.0.0.1 matching the listener address.
        let wss_url = format!("wss://127.0.0.1:{proxy_port}/ws");

        // 1. Without trusting the proxy CA, TLS certificate verification must reject with UnknownIssuer.
        let untrusted_client =
            AgentMeshClient::new(&wss_url, "untrusted-node", "windows").with_token(hub_token);
        let untrusted_err = untrusted_client
            .connect()
            .await
            .expect_err("untrusted TLS proxy cert must be rejected");
        let untrusted_msg = format!("{untrusted_err:?} {untrusted_err}");
        assert!(
            untrusted_msg.contains("UnknownIssuer"),
            "expected UnknownIssuer for untrusted CA, got: {untrusted_msg}"
        );

        // 2. An unreadable / missing CA file must fail closed on both wss:// and ws://.
        let tmp_dir = tempfile::tempdir().unwrap();
        let missing_ca_path = tmp_dir.path().join("does-not-exist-ca.pem");
        let missing_ca_err = AgentMeshClient::new(&wss_url, "missing-ca-node", "windows")
            .with_token(hub_token)
            .with_tls_ca_file(&missing_ca_path)
            .connect()
            .await
            .expect_err("missing TLS CA file must fail");
        assert!(
            missing_ca_err
                .to_string()
                .contains("failed to read TLS CA certificate file"),
            "expected missing CA file error, got: {missing_ca_err}"
        );

        // 3. A leaf certificate valid only for `other.example` dialled as `localhost` must be rejected with NotValidForName.
        let wrong_leaf_certs =
            parse_certificates_pem_or_der(WRONG_HOST_LEAF_CERT_PEM.as_bytes()).unwrap();
        let wrong_leaf_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            WRONG_HOST_LEAF_KEY_PKCS8_DER.to_vec(),
        ));
        let wrong_server_config =
            tokio_rustls::rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(wrong_leaf_certs, wrong_leaf_key)
                .expect("valid wrong-host TLS server config");
        let wrong_acceptor = TlsAcceptor::from(Arc::new(wrong_server_config));
        let wrong_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind wrong-host TLS proxy");
        let wrong_port = wrong_listener.local_addr().unwrap().port();
        let wrong_proxy_task = tokio::spawn(async move {
            while let Ok((inbound, _)) = wrong_listener.accept().await {
                let wrong_acceptor = wrong_acceptor.clone();
                tokio::spawn(async move {
                    let _ = wrong_acceptor.accept(inbound).await;
                });
            }
        });

        let wrong_host_client = AgentMeshClient::new(
            format!("wss://localhost:{wrong_port}/ws"),
            "wrong-host-node",
            "windows",
        )
        .with_token(hub_token)
        .with_tls_ca_pem(TEST_CA_CERT_PEM.as_bytes().to_vec());
        let name_err = wrong_host_client
            .connect()
            .await
            .expect_err("leaf for other.example dialled as localhost must fail");
        let name_err_msg = format!("{name_err:?} {name_err}");
        assert!(
            name_err_msg.contains("NotValidForName"),
            "expected NotValidForName when dialling localhost against other.example cert, got: {name_err_msg}"
        );
        wrong_proxy_task.abort();

        // 4. Write a 2-cert PEM bundle with UNRELATED_CA_CERT_PEM first and TEST_CA_CERT_PEM second.
        //    node2 trusts only this bundle file, so keeping only the first PEM cert or ignoring the file fails node2.
        let ca_bundle_file = tmp_dir.path().join("mesh-proxy-ca-bundle.pem");
        std::fs::write(
            &ca_bundle_file,
            format!("{UNRELATED_CA_CERT_PEM}\n{TEST_CA_CERT_PEM}"),
        )
        .unwrap();

        let node1 = AgentMeshClient::new(&wss_url, "wss-sender", "windows")
            .with_token(hub_token)
            .with_tls_ca_pem(TEST_CA_CERT_PEM.as_bytes().to_vec());
        let node2 = AgentMeshClient::new(&wss_url, "wss-executor", "linux")
            .with_token(hub_token)
            .with_tls_ca_file(&ca_bundle_file);

        node1
            .connect()
            .await
            .expect("node1 wss:// connection must succeed");
        node2
            .connect()
            .await
            .expect("node2 wss:// connection with 2-cert bundle (CA second) must succeed");

        let nodes = node1
            .query_node_list(Some(3000))
            .await
            .expect("query_node_list over wss:// must succeed");
        assert_eq!(nodes.len(), 2);

        let res = node1
            .send_command(
                "wss-executor",
                "echo",
                serde_json::json!({"message": "Hello over TLS wss://"}),
                Some(3000),
            )
            .await
            .expect("send_command over wss:// must succeed");
        match res {
            AgentMeshEnvelope::CommandResponse {
                status,
                exit_code,
                stdout,
                ..
            } => {
                assert_eq!(status, "success");
                assert_eq!(exit_code, 0);
                assert_eq!(stdout, "Hello over TLS wss://");
            }
            other => panic!("Expected CommandResponse over wss://, got {other:?}"),
        }

        node1.close().await;
        node2.close().await;
        proxy_task.abort();
        hub.stop().await;
    }

    #[test]
    fn test_is_transient_ws_connect_error_classifies_wsaenobufs_and_addr_in_use() {
        let wsaenobufs =
            tokio_tungstenite::tungstenite::Error::Io(std::io::Error::from_raw_os_error(10055));
        assert!(is_transient_ws_connect_error(&wsaenobufs));

        let wsaeaddrinuse =
            tokio_tungstenite::tungstenite::Error::Io(std::io::Error::from_raw_os_error(10048));
        assert!(is_transient_ws_connect_error(&wsaeaddrinuse));

        let oom = tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "no buffer space",
        ));
        assert!(is_transient_ws_connect_error(&oom));

        let refused = tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert!(!is_transient_ws_connect_error(&refused));
    }

    #[test]
    fn test_validate_inbox_dir_rejects_file_empty_and_symlink() {
        assert!(validate_inbox_dir(Path::new("")).is_err());

        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("not-a-dir.txt");
        std::fs::write(&file_path, b"hello").unwrap();
        let file_err = validate_inbox_dir(&file_path).expect_err("regular file must be rejected");
        assert!(
            file_err.to_string().contains("not a directory"),
            "{file_err}"
        );

        #[cfg(unix)]
        {
            let real_dir = tmp.path().join("real");
            std::fs::create_dir(&real_dir).unwrap();
            let link_dir = tmp.path().join("link_inbox");
            std::os::unix::fs::symlink(&real_dir, &link_dir).unwrap();
            let link_err = validate_inbox_dir(&link_dir).expect_err("symlink must be rejected");
            assert!(link_err.to_string().contains("symlink"), "{link_err}");

            // Trailing path separator(s) on a symlink must still be rejected as a symlink.
            let trailing_link = PathBuf::from(format!("{}/", link_dir.display()));
            let trailing_err = validate_inbox_dir(&trailing_link)
                .expect_err("trailing-slash symlink must be rejected");
            assert!(
                trailing_err.to_string().contains("symlink"),
                "{trailing_err}"
            );

            let multi_trailing_link = PathBuf::from(format!("{}///", link_dir.display()));
            let multi_err = validate_inbox_dir(&multi_trailing_link)
                .expect_err("multi-trailing-slash symlink must be rejected");
            assert!(multi_err.to_string().contains("symlink"), "{multi_err}");

            // Broken symlink with trailing slash must also be rejected as a symlink.
            let broken_link = tmp.path().join("broken_link");
            std::os::unix::fs::symlink(tmp.path().join("does-not-exist"), &broken_link).unwrap();
            let broken_trailing = PathBuf::from(format!("{}/", broken_link.display()));
            let broken_err = validate_inbox_dir(&broken_trailing)
                .expect_err("broken trailing-slash symlink must be rejected");
            assert!(broken_err.to_string().contains("symlink"), "{broken_err}");
        }

        // Root `/` normalization keeps `/` intact.
        assert_eq!(Path::new("/").components().as_path(), Path::new("/"));

        let new_dir = tmp.path().join("nested").join("inbox");
        let validated = validate_inbox_dir(&new_dir).expect("new dir should be created");
        assert!(validated.is_dir());
    }

    #[test]
    fn test_persist_payload_atomic_traversal_binary_and_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = validate_inbox_dir(tmp.path().join("inbox").as_path()).unwrap();

        let mut meta = HashMap::new();
        meta.insert("filename".to_string(), "../../etc/passwd".to_string());
        let binary = b"\x00\x01\x80\xffhello\xfe";
        let saved_bin =
            persist_payload_atomic(&inbox, "../evil/corr", 12345, Some(&meta), binary).unwrap();
        assert_eq!(saved_bin.parent(), Some(inbox.as_path()));
        assert_eq!(std::fs::read(&saved_bin).unwrap(), binary);

        let saved_empty = persist_payload_atomic(&inbox, "empty-corr", 12346, None, b"").unwrap();
        assert_eq!(saved_empty.parent(), Some(inbox.as_path()));
        assert_eq!(std::fs::read(&saved_empty).unwrap(), b"");

        // Ensure no hidden .tmp_* files remain
        let entries: Vec<_> = std::fs::read_dir(&inbox)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(entries.len(), 2);
        for e in entries {
            assert!(!e.file_name().to_string_lossy().starts_with(".tmp_"));
        }
    }

    #[tokio::test]
    async fn test_bounded_received_payloads_cap() {
        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let sender = AgentMeshClient::new(&hub_url, "bounded-sender", "macos");
        let receiver = AgentMeshClient::new(&hub_url, "bounded-receiver", "linux")
            .with_max_received_payloads(5);

        sender.connect().await.unwrap();
        receiver.connect().await.unwrap();

        for i in 0..12u32 {
            let msg = format!("pkt-{i:02}");
            let receipt = sender
                .send_data_payload_with_ack("bounded-receiver", msg.as_bytes(), Some(3000))
                .await
                .expect("send with ack");
            assert_eq!(receipt.status, "data_received_verified");
        }

        let buffered = receiver.received_data_payloads().await;
        assert_eq!(buffered.len(), 5);
        let payloads: Vec<String> = buffered
            .into_iter()
            .map(|e| match e {
                AgentMeshEnvelope::DataPayload { data, .. } => data.unwrap(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            payloads,
            vec!["pkt-07", "pkt-08", "pkt-09", "pkt-10", "pkt-11"]
        );

        sender.close().await;
        receiver.close().await;
        hub.stop().await;
    }

    #[tokio::test]
    async fn test_corrupt_and_multi_chunk_data_delivery() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = validate_inbox_dir(&tmp.path().join("inbox")).unwrap();

        let hub = AgentMeshHub::new();
        let port = hub.start("127.0.0.1:0").await.expect("Hub failed to start");
        let hub_url = format!("ws://127.0.0.1:{}/ws", port);

        let consumer = AgentMeshClient::new(&hub_url, "consumer", "macos").with_inbox_dir(&inbox);
        let sender = AgentMeshClient::new(&hub_url, "sender", "linux");
        consumer.connect().await.unwrap();
        sender.connect().await.unwrap();

        // 1. Corrupted checksum must NACK and write nothing to inbox or received_payloads
        let mut bad_env = AgentMeshEnvelope::new_data_payload("sender", "consumer", b"tampered").0;
        let bad_corr = bad_env.correlation_id().to_string();
        if let AgentMeshEnvelope::DataPayload {
            ref mut checksum_sha256,
            ..
        } = bad_env
        {
            *checksum_sha256 = Some("0".repeat(64));
        }
        let (tx, rx) = oneshot::channel();
        sender.pending.write().await.insert(bad_corr.clone(), tx);
        sender.send_envelope(bad_env).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .unwrap()
            .unwrap();
        match reply {
            AgentMeshEnvelope::DeliveryNack { error_code, .. } => {
                assert_eq!(error_code, ERR_PAYLOAD_CORRUPT);
            }
            other => panic!("Expected DeliveryNack, got {other:?}"),
        }
        assert!(consumer.received_data_payloads().await.is_empty());
        assert_eq!(std::fs::read_dir(&inbox).unwrap().count(), 0);

        // 2. Non-Latin-1 code point must NACK and write nothing
        let non_latin1 = serde_json::json!({
            "type": "DataPayload",
            "correlation_id": "bad-unicode",
            "from": "sender",
            "to": "consumer",
            "data": "hello-\u{1F680}",
            "checksum_sha256": compute_sha256("hello-\u{1F680}".as_bytes()),
        });
        let (tx2, rx2) = oneshot::channel();
        sender
            .pending
            .write()
            .await
            .insert("bad-unicode".to_string(), tx2);
        sender
            .send_envelope(AgentMeshEnvelope::from_json_str(&non_latin1.to_string()).unwrap())
            .await
            .unwrap();
        let reply2 = tokio::time::timeout(Duration::from_secs(3), rx2)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reply2,
            AgentMeshEnvelope::DeliveryNack { ref error_code, .. } if error_code == ERR_PAYLOAD_CORRUPT
        ));
        assert_eq!(std::fs::read_dir(&inbox).unwrap().count(), 0);

        // 3. Multi-chunk out-of-order reassembly + atomic inbox write
        let big_payload: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        let (mut chunks, multi_corr, multi_sha) =
            AgentMeshEnvelope::new_data_payload_chunks("sender", "consumer", &big_payload, 1024);
        assert_eq!(chunks.len(), 5);
        chunks.reverse(); // deliver chunks out-of-order [4, 3, 2, 1, 0]
        let (tx3, rx3) = oneshot::channel();
        sender.pending.write().await.insert(multi_corr.clone(), tx3);
        for chunk in chunks {
            sender.send_envelope(chunk).await.unwrap();
        }
        let reply3 = tokio::time::timeout(Duration::from_secs(5), rx3)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reply3,
            AgentMeshEnvelope::DeliveryAck { ref status, .. } if status == "data_received_verified"
        ));
        let files: Vec<_> = std::fs::read_dir(&inbox)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(files.len(), 1);
        let disk_bytes = std::fs::read(files[0].path()).unwrap();
        assert_eq!(disk_bytes, big_payload);
        assert_eq!(compute_sha256(&disk_bytes), multi_sha);

        // 4. send_data_payload_chunked_with_ack against missing target fails immediately
        let err = sender
            .send_data_payload_chunked_with_ack("missing-node", b"abc", 2, Some(3000))
            .await
            .expect_err("missing target must fail");
        assert!(err.to_string().contains("ERR_NODE_NOT_FOUND"), "{err}");

        // 5. Self-delivery (from == to) resolves with DeliveryAck instead of DataPayload.
        let self_receipt = consumer
            .send_data_payload_with_ack("consumer", b"self-loop-payload", Some(3000))
            .await
            .expect("self-targeted send_data_payload_with_ack must succeed");
        assert_eq!(self_receipt.responder, "consumer");
        assert_eq!(self_receipt.status, "data_received_verified");
        assert_eq!(self_receipt.sha256, compute_sha256(b"self-loop-payload"));
        assert_eq!(std::fs::read_dir(&inbox).unwrap().count(), 2);

        sender.close().await;
        consumer.close().await;
        hub.stop().await;
    }

    #[test]
    fn test_chunk_reassembler_aggregate_byte_cap_evicts_oldest_fifo() {
        // Allow up to 10 assemblies, 100 bytes per assembly, and 120 bytes aggregate across all.
        let mut reassembler = ChunkReassembler::with_limits(10, 100, 120);

        // Insert incomplete 50-byte chunk 0 for transfers "t1" and "t2" (total = 100 <= 120).
        assert!(reassembler
            .insert_chunk("peer", "t1", 0, 2, None, vec![b'1'; 50])
            .unwrap()
            .is_none());
        assert!(reassembler
            .insert_chunk("peer", "t2", 0, 2, None, vec![b'2'; 50])
            .unwrap()
            .is_none());
        assert_eq!(reassembler.inflight.len(), 2);
        assert_eq!(reassembler.total_inflight_bytes, 100);

        // Inserting "t3" (50 bytes) would hit 150 > 120, so oldest "t1" must be evicted FIFO.
        assert!(reassembler
            .insert_chunk("peer", "t3", 0, 2, None, vec![b'3'; 50])
            .unwrap()
            .is_none());
        assert!(!reassembler
            .inflight
            .contains_key(&("peer".to_string(), "t1".to_string())));
        assert!(reassembler
            .inflight
            .contains_key(&("peer".to_string(), "t2".to_string())));
        assert!(reassembler
            .inflight
            .contains_key(&("peer".to_string(), "t3".to_string())));
        assert_eq!(reassembler.total_inflight_bytes, 100);

        // Adding chunk 1 (40 bytes) to "t3" would reach 140 > 120, so oldest remaining "t2"
        // is evicted FIFO and "t3" completes cleanly, returning total_inflight_bytes to 0.
        let assembled = reassembler
            .insert_chunk("peer", "t3", 1, 2, None, vec![b'4'; 40])
            .unwrap()
            .expect("t3 should complete after second chunk");
        assert_eq!(assembled.bytes.len(), 90);
        assert!(reassembler.inflight.is_empty());
        assert_eq!(reassembler.total_inflight_bytes, 0);
    }

    #[test]
    fn test_chunk_reassembler_prune_expired_ttl() {
        let mut reassembler = ChunkReassembler::new().with_ttl(Duration::from_millis(20));

        // Insert incomplete chunk 0 for "stale" (32 bytes).
        assert!(reassembler
            .insert_chunk("peer", "stale", 0, 2, None, vec![b's'; 32])
            .unwrap()
            .is_none());
        assert_eq!(reassembler.inflight.len(), 1);
        assert_eq!(reassembler.total_inflight_bytes, 32);

        // Before TTL elapses, prune_expired must retain the assembly.
        reassembler.prune_expired(Instant::now());
        assert_eq!(reassembler.inflight.len(), 1);
        assert_eq!(reassembler.total_inflight_bytes, 32);

        // Sleep past the 20ms TTL, then insert a fresh chunk; insert_chunk calls prune_expired
        // and must drop the expired "stale" assembly and reclaim its 32 bytes.
        std::thread::sleep(Duration::from_millis(35));
        assert!(reassembler
            .insert_chunk("peer", "fresh", 0, 2, None, vec![b'f'; 16])
            .unwrap()
            .is_none());
        assert!(!reassembler
            .inflight
            .contains_key(&("peer".to_string(), "stale".to_string())));
        assert!(reassembler
            .inflight
            .contains_key(&("peer".to_string(), "fresh".to_string())));
        assert_eq!(reassembler.inflight.len(), 1);
        assert_eq!(reassembler.total_inflight_bytes, 16);

        // Sending chunk 1 for "stale" after expiry starts a new assembly rather than completing
        // with the expired chunk 0.
        assert!(reassembler
            .insert_chunk("peer", "stale", 1, 2, None, vec![b'x'; 8])
            .unwrap()
            .is_none());
        assert_eq!(reassembler.total_inflight_bytes, 24);

        // Direct prune_expired with a timestamp past TTL clears everything and zeroes byte count.
        let future = Instant::now()
            .checked_add(Duration::from_millis(35))
            .unwrap();
        reassembler.prune_expired(future);
        assert!(reassembler.inflight.is_empty());
        assert_eq!(reassembler.total_inflight_bytes, 0);
    }

    #[test]
    fn test_extract_output_artifacts_verifies_sha256_and_detects_tampering() {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        let raw = b"verified-artifact-bytes";
        let sha = compute_sha256(raw);
        let mut ok_env = AgentMeshEnvelope::new_command_response(
            "grid",
            "client",
            "c1",
            Some("grid_submit".to_string()),
            "success",
            0,
            "",
            "",
            5,
        );
        if let AgentMeshEnvelope::CommandResponse {
            ref mut payload, ..
        } = ok_env
        {
            *payload = Some(serde_json::json!({
                "artifacts": [{
                    "path": "out/file.bin",
                    "size": raw.len(),
                    "sha256": sha,
                    "data_base64": STANDARD.encode(raw),
                }]
            }));
        }

        let extracted = AgentMeshClient::extract_output_artifacts(&ok_env).unwrap();
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0].path, "out/file.bin");
        assert_eq!(extracted[0].bytes, raw);

        // Tampered sha256 must fail verification.
        if let AgentMeshEnvelope::CommandResponse {
            ref mut payload, ..
        } = ok_env
        {
            *payload = Some(serde_json::json!({
                "artifacts": [{
                    "path": "out/file.bin",
                    "size": raw.len(),
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "data_base64": STANDARD.encode(raw),
                }]
            }));
        }
        let err = AgentMeshClient::extract_output_artifacts(&ok_env)
            .expect_err("tampered SHA-256 must be rejected");
        assert!(
            err.to_string().contains("SHA-256 verification failed"),
            "{err}"
        );
    }
}
