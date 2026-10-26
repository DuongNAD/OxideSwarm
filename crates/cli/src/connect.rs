//! Shared client plumbing for talking to a Master: address defaults and normalisation, LAN
//! auto-discovery, and the one authenticated connection helper, [`connect_master`], that every
//! client command (`submit`, `status`, `workers`, `mapreduce`, and later the MCP server) goes
//! through.
//!
//! Ports: the Master's grid port is **8088** (workers and clients connect there); the dashboard /
//! REST / WebSocket port is 8080 and is never a valid `--master` target.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use tokio::net::TcpStream;
use tracing::info;

use rusty_grid_core::cluster_auth::{client_handshake, ClusterSecret};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::protocol::{EndpointRole, MessageTransport};
use rusty_grid_core::task::{
    validate_relative_sandbox_path, OutputArtifact, StagedFile, MAX_AGGREGATE_STAGED_BYTES,
    MAX_STAGED_FILE_BYTES,
};
use rusty_grid_core::transport::configure_tcp_stream;

/// TCP port of the Master's grid listener (workers and clients).
pub const DEFAULT_MASTER_PORT: u16 = 8088;
/// Where `worker`, `submit`, `status`, `workers` and `mapreduce` look for a Master by default.
pub const DEFAULT_MASTER_ADDR: &str = "127.0.0.1:8088";
/// TCP port of the Master's dashboard / REST API (default when `--listen` is the default).
pub const DEFAULT_DASHBOARD_PORT: u16 = 8080;
/// How long a client waits for the TCP connection to the Master.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a client waits for the cluster authentication handshake to finish.
pub const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a client waits for LAN auto-discovery (`--master auto`).
pub const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
/// Process exit code for an authentication failure (`GridError::exit_code` of the auth errors).
pub const AUTH_EXIT_CODE: u8 = 6;

fn is_auto(addr: &str) -> bool {
    addr.eq_ignore_ascii_case("auto") || addr.to_ascii_lowercase().starts_with("auto:")
}

/// Whether a worker or client must hold a cluster secret to talk to `master_addr`.
///
/// True for everything except a master on this very machine: `auto` (UDP discovery picks whoever
/// answers first), any P2P ticket (`via_p2p_ticket`: a ticket only addresses a node, it proves
/// nothing about who runs it), an empty address (the worker treats it as `auto`), and any host
/// that is not a loopback IP or `localhost` / `*.localhost`. Host names are judged by name only,
/// never resolved: a name that merely happens to point at 127.0.0.1 still needs a secret.
///
/// Without a secret the peer cannot tell the genuine master from a rogue one: a worker would obey
/// whatever commands the rogue sends, and a client would hand it the command, environment and
/// stdin of every submitted task. The CLI therefore refuses to continue without a secret in these
/// cases unless `--insecure` is given (see `crate::secret_cmd::decide_peer_secret`).
pub fn master_requires_secret(master_addr: &str, via_p2p_ticket: bool) -> bool {
    if via_p2p_ticket {
        return true;
    }
    let addr = normalize_master_addr(master_addr);
    if addr.is_empty() || is_auto(&addr) {
        return true;
    }
    if let Ok(socket) = addr.parse::<SocketAddr>() {
        return !socket.ip().is_loopback();
    }
    let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
        .trim_end_matches('.');
    if let Ok(ip) = host.parse::<IpAddr>() {
        return !ip.is_loopback();
    }
    let host = host.to_ascii_lowercase();
    !(host == "localhost" || host.ends_with(".localhost"))
}

/// Completes a `--master` value: a bare host or IP gets the default grid port (`192.168.1.5` ->
/// `192.168.1.5:8088`, `::1` -> `[::1]:8088`). `host:port`, `[v6]:port`, and the discovery
/// keywords `auto` / `auto:<udp port>` are returned unchanged (trimmed), as is an empty string.
pub fn normalize_master_addr(addr: &str) -> String {
    let addr = addr.trim();
    if addr.is_empty() || is_auto(addr) || addr.parse::<SocketAddr>().is_ok() {
        return addr.to_string();
    }
    if let Ok(ip) = addr.parse::<IpAddr>() {
        return SocketAddr::new(ip, DEFAULT_MASTER_PORT).to_string();
    }
    if addr.starts_with('[') && addr.ends_with(']') {
        return format!("{addr}:{DEFAULT_MASTER_PORT}");
    }
    if addr.contains(':') {
        // `host:port`, or something unparsable that the connect call will report as such.
        return addr.to_string();
    }
    format!("{addr}:{DEFAULT_MASTER_PORT}")
}

/// Turns a `--master` value into a connectable `host:port`: runs LAN discovery for `auto` /
/// `auto:<udp port>` (authenticated when a `secret` is given, so only masters holding it are
/// accepted) and appends [`DEFAULT_MASTER_PORT`] to bare hosts.
///
/// Discovery that finds nothing is an error. There is deliberately no silent fallback to a
/// guessed address: it would hide that no (authentic) master answered.
pub async fn resolve_master_addr(addr: &str, secret: Option<&ClusterSecret>) -> GridResult<String> {
    let addr = addr.trim();
    if !is_auto(addr) {
        return Ok(normalize_master_addr(addr));
    }

    let disc_port = addr
        .split_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT);
    info!(
        port = disc_port,
        authenticated = secret.is_some(),
        "Master address set to 'auto'; performing LAN UDP discovery probe..."
    );
    match rusty_grid_core::discovery::discover_master_with_secret(
        DISCOVERY_TIMEOUT,
        disc_port,
        secret,
    )
    .await
    {
        Some(beacon) => {
            info!(
                master = %beacon.cluster_addr,
                hostname = %beacon.hostname,
                "Discovered active Master via UDP LAN beacon"
            );
            Ok(beacon.cluster_addr)
        }
        None => Err(GridError::ConnectionFailed(format!(
            "Auto-discovery found no Master on UDP port {disc_port}{}; start a Master or pass --master HOST:{DEFAULT_MASTER_PORT}",
            if secret.is_some() {
                " (only Masters holding the same cluster secret are accepted)"
            } else {
                ""
            }
        ))),
    }
}

/// Connects to the Master at `addr` and, when a `secret` is given, authenticates with it.
///
/// This is the single entry point for client commands: it resolves `auto` and bare hosts
/// ([`resolve_master_addr`]), connects with a timeout, applies the grid's TCP options
/// (`configure_tcp_stream`: nodelay + keepalive), runs the cluster handshake
/// (`rusty_grid_core::cluster_auth::client_handshake`) and returns the transport, ready for
/// `ClientMessage`s.
///
/// Errors: `ConnectionFailed("Failed to connect to Master at ...")` when the socket cannot be
/// opened, `AuthFailed` / `AuthRequired` when the Master rejects the secret or the Master does not
/// prove it holds the same one, `Timeout` when the handshake stalls. Print them with
/// [`report_master_failure`].
///
/// Without a secret the Master is only told apart from an open one on the first reply: a Master
/// that requires the secret answers the first request with an authentication rejection, which
/// the transport reports as `GridError::AuthRequired`.
pub async fn connect_master(
    addr: &str,
    secret: Option<&ClusterSecret>,
) -> GridResult<MessageTransport<TcpStream>> {
    let addr = resolve_master_addr(addr, secret).await?;

    let stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            return Err(GridError::ConnectionFailed(format!(
                "Failed to connect to Master at {addr}: {e}"
            )))
        }
        Err(_) => {
            return Err(GridError::ConnectionFailed(format!(
                "Failed to connect to Master at {addr}: timed out after {}s",
                CONNECT_TIMEOUT.as_secs()
            )))
        }
    };
    // Best effort: a failure here only costs latency / dead-peer detection.
    let _ = configure_tcp_stream(&stream);

    let mut transport = MessageTransport::new(stream);
    if let Some(secret) = secret {
        let session_key =
            tokio::time::timeout(AUTH_TIMEOUT, client_handshake(&mut transport, secret))
                .await
                .map_err(|_| GridError::Timeout {
                    operation: "cluster_authentication".into(),
                    duration_secs: AUTH_TIMEOUT.as_secs(),
                })??;
        transport.enable_encryption(&session_key, EndpointRole::Client);
    }
    Ok(transport)
}

/// One-line, actionable text for an authentication error from `master_addr`, or `None` for any
/// other error.
pub fn describe_auth_error(master_addr: &str, err: &GridError) -> Option<String> {
    match err {
        GridError::AuthRequired(detail) => Some(format!(
            "Authentication required: {detail} (Master: {master_addr}; you can also pass --secret-file PATH)"
        )),
        GridError::AuthFailed(detail) => Some(format!(
            "Authentication failed: {detail} (Master: {master_addr}). Master and client must use the same cluster secret: --secret-file PATH, OXIDESWARM_SECRET, or ~/.oxideswarm/cluster.secret"
        )),
        _ => None,
    }
}

/// Prints `err` for the user and returns the process exit code: [`AUTH_EXIT_CODE`] for
/// authentication errors (with the actionable text of [`describe_auth_error`]), 1 otherwise.
///
/// `what` is the prefix for errors that carry no message of their own ("Failed to send task to
/// Master"). When the client had no secret and the connection simply dies, the Master may well be
/// rejecting an unauthenticated peer, so a hint is added.
pub fn report_master_failure(
    master_addr: &str,
    had_secret: bool,
    what: &str,
    err: &GridError,
) -> ExitCode {
    if let Some(text) = describe_auth_error(master_addr, err) {
        eprintln!("{text}");
        return ExitCode::from(AUTH_EXIT_CODE);
    }
    match err {
        GridError::ConnectionFailed(message) => eprintln!("{message}"),
        GridError::ConnectionClosed | GridError::Io(_) if !had_secret => {
            eprintln!(
                "{what}: {err}\nHint: if the Master at {master_addr} requires a cluster secret, pass --secret-file PATH or set OXIDESWARM_SECRET (or copy ~/.oxideswarm/cluster.secret from the Master machine)"
            );
        }
        other => eprintln!("{what}: {other}"),
    }
    ExitCode::FAILURE
}

/// Parses a `--file <LOCAL[:SANDBOX]>` specification, handling Windows drive letters (e.g., `C:\...`).
pub fn parse_file_spec(spec: &str) -> Result<(PathBuf, String), String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("Staged file specification cannot be empty".to_string());
    }

    // Determine split point, respecting Windows drive letter at index 1 (e.g. "C:\...")
    let split_pos = if let Some(first_colon) = spec.find(':') {
        let is_drive = first_colon == 1 && spec.as_bytes()[0].is_ascii_alphabetic();
        if is_drive {
            // Search for separator colon after drive prefix
            spec[2..].find(':').map(|idx| idx + 2)
        } else {
            Some(first_colon)
        }
    } else {
        None
    };

    let (local_str, sandbox_str) = match split_pos {
        Some(pos) => {
            let local = spec[..pos].trim();
            let sandbox = spec[pos + 1..].trim();
            if local.is_empty() {
                return Err(format!("--file '{spec}': local path cannot be empty"));
            }
            if sandbox.is_empty() {
                return Err(format!(
                    "--file '{spec}': sandbox destination path cannot be empty"
                ));
            }
            (local, Some(sandbox))
        }
        None => (spec, None),
    };

    let local_path = PathBuf::from(local_str);
    let sandbox_path = match sandbox_str {
        Some(s) => validate_relative_sandbox_path(s)?,
        None => {
            // Default sandbox path: if local path is already a safe relative path, preserve it;
            // otherwise (absolute path or starts with ..), default to filename.
            let path_str = local_path
                .to_str()
                .ok_or_else(|| format!("--file '{spec}': path contains non-UTF8 characters"))?;
            match validate_relative_sandbox_path(path_str) {
                Ok(norm) => norm,
                Err(_) => {
                    let file_name =
                        local_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .ok_or_else(|| {
                                format!(
                                    "--file '{spec}': cannot determine file name for sandbox path"
                                )
                            })?;
                    validate_relative_sandbox_path(file_name)?
                }
            }
        }
    };

    Ok((local_path, sandbox_path))
}

/// Pre-flight validation and reading of staged files on the client side before submission.
pub fn prepare_staged_files(specs: &[String]) -> Result<Vec<StagedFile>, String> {
    let mut staged_files = Vec::with_capacity(specs.len());
    let mut seen_paths = std::collections::HashSet::new();
    let mut total_bytes: u64 = 0;

    for spec in specs {
        let (local_path, sandbox_rel_path) = parse_file_spec(spec)?;
        if !seen_paths.insert(sandbox_rel_path.clone()) {
            return Err(format!(
                "Duplicate sandbox path detected: '{sandbox_rel_path}' is mapped multiple times"
            ));
        }

        let meta = std::fs::metadata(&local_path)
            .map_err(|e| format!("--file {}: cannot read metadata: {e}", local_path.display()))?;
        if !meta.is_file() {
            return Err(format!(
                "--file {}: target is not a regular file",
                local_path.display()
            ));
        }
        let size = meta.len();
        if size > MAX_STAGED_FILE_BYTES as u64 {
            return Err(format!(
                "--file {}: file size ({size} bytes) exceeds maximum limit of {} MiB",
                local_path.display(),
                MAX_STAGED_FILE_BYTES / (1024 * 1024)
            ));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > MAX_AGGREGATE_STAGED_BYTES as u64 {
            return Err(format!(
                "Aggregate staged files size ({total_bytes} bytes) exceeds maximum limit of {} MiB",
                MAX_AGGREGATE_STAGED_BYTES / (1024 * 1024)
            ));
        }

        let content = std::fs::read(&local_path)
            .map_err(|e| format!("--file {}: failed to read file: {e}", local_path.display()))?;

        #[cfg(unix)]
        let executable = {
            use std::os::unix::fs::PermissionsExt;
            meta.permissions().mode() & 0o111 != 0
        };
        #[cfg(not(unix))]
        let executable = false;

        let staged = StagedFile::new(sandbox_rel_path, content, executable);
        staged_files.push(staged);
    }

    Ok(staged_files)
}

/// Validates declared expected output paths against path traversal and DOS device attacks.
pub fn validate_output_paths(paths: &[String]) -> Result<Vec<String>, String> {
    let mut validated = Vec::with_capacity(paths.len());
    for p in paths {
        let clean = validate_relative_sandbox_path(p)
            .map_err(|e| format!("--output '{p}': illegal output path: {e}"))?;
        validated.push(clean);
    }
    Ok(validated)
}

/// Writes harvested output artifacts to the specified client-side destination directory,
/// verifying SHA-256 checksums and preventing directory traversal escapes.
pub fn save_output_artifacts(
    artifacts: &[OutputArtifact],
    output_dir: &Path,
) -> Result<Vec<PathBuf>, String> {
    std::fs::create_dir_all(output_dir).map_err(|e| {
        format!(
            "Failed to create output directory {}: {e}",
            output_dir.display()
        )
    })?;

    let canonical_dest = output_dir
        .canonicalize()
        .map_err(|e| format!("Failed to canonicalize output directory: {e}"))?;

    let mut written_paths = Vec::with_capacity(artifacts.len());

    for artifact in artifacts {
        let clean_rel = validate_relative_sandbox_path(&artifact.path)
            .map_err(|e| format!("Invalid artifact path '{}': {e}", artifact.path))?;
        let target_path = output_dir.join(&clean_rel);

        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory {}: {e}", parent.display()))?;
        }

        if !artifact.verify_checksum() {
            return Err(format!(
                "Artifact checksum verification failed for '{}'",
                artifact.path
            ));
        }

        std::fs::write(&target_path, artifact.data.as_bytes())
            .map_err(|e| format!("Failed to write artifact {}: {e}", target_path.display()))?;

        if let Ok(canon_target) = target_path.canonicalize() {
            if !canon_target.starts_with(&canonical_dest) {
                let _ = std::fs::remove_file(&target_path);
                return Err(format!(
                    "Security violation: artifact path '{}' escaped output directory",
                    artifact.path
                ));
            }
        }

        written_paths.push(target_path);
    }

    Ok(written_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::cluster_auth::server_handshake;
    use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
    use tokio::net::TcpListener;

    fn secret(seed: u8) -> ClusterSecret {
        ClusterSecret::from_bytes(&[seed; 32]).unwrap()
    }

    #[test]
    fn test_defaults_use_the_grid_port_not_the_dashboard_port() {
        assert_eq!(DEFAULT_MASTER_ADDR, "127.0.0.1:8088");
        assert_eq!(DEFAULT_MASTER_PORT, 8088);
        assert_ne!(DEFAULT_MASTER_PORT, DEFAULT_DASHBOARD_PORT);
        assert!(DEFAULT_MASTER_ADDR.ends_with(&DEFAULT_MASTER_PORT.to_string()));
    }

    #[test]
    fn test_normalize_master_addr_appends_the_default_port_to_bare_hosts() {
        for (input, expected) in [
            ("192.168.1.5", "192.168.1.5:8088"),
            ("  192.168.1.5  ", "192.168.1.5:8088"),
            ("mac-studio.local", "mac-studio.local:8088"),
            ("localhost", "localhost:8088"),
            ("::1", "[::1]:8088"),
            ("[::1]", "[::1]:8088"),
            // Already complete: untouched.
            ("192.168.1.5:9000", "192.168.1.5:9000"),
            ("mac-studio.local:9000", "mac-studio.local:9000"),
            ("[::1]:9000", "[::1]:9000"),
            // Discovery keywords and empty stay as they are.
            ("auto", "auto"),
            ("AUTO", "AUTO"),
            ("auto:8090", "auto:8090"),
            ("", ""),
        ] {
            assert_eq!(normalize_master_addr(input), expected, "input {input:?}");
        }
    }

    #[test]
    fn test_only_a_master_on_this_machine_may_be_reached_without_a_secret() {
        // This machine: loopback IPs and localhost names, with or without a port.
        for local in [
            "127.0.0.1",
            "127.0.0.1:8088",
            "127.5.5.5:9000",
            "[::1]:8088",
            "::1",
            "localhost",
            "LOCALHOST:8088",
            "localhost.:8088",
            "dev.localhost:8088",
        ] {
            assert!(!master_requires_secret(local, false), "{local:?}");
        }
        // Everything else needs one: LAN/WAN hosts, names, discovery, unspecified, empty.
        for remote in [
            "192.168.1.5",
            "192.168.1.5:8088",
            "10.0.0.7:1",
            "[fd00::5]:8088",
            "mac-studio.local:8088",
            "example.com",
            "localhost.example.com:8088", // not a localhost name
            "notlocalhost:8088",
            "0.0.0.0:8088", // not a loopback address
            "auto",
            "AUTO",
            "auto:8090",
            "",
            "  ",
        ] {
            assert!(master_requires_secret(remote, false), "{remote:?}");
        }
        // A P2P ticket always needs one, even next to a loopback --master.
        assert!(master_requires_secret("127.0.0.1:8088", true));
        assert!(master_requires_secret(DEFAULT_MASTER_ADDR, true));
    }

    #[tokio::test]
    async fn test_resolve_master_addr_passes_plain_addresses_through_without_discovery() {
        assert_eq!(
            resolve_master_addr("10.0.0.7", None).await.unwrap(),
            "10.0.0.7:8088"
        );
        assert_eq!(
            resolve_master_addr("10.0.0.7:1234", Some(&secret(1)))
                .await
                .unwrap(),
            "10.0.0.7:1234"
        );
    }

    #[test]
    fn test_describe_auth_error_is_actionable_and_only_for_auth_errors() {
        let required =
            GridError::AuthRequired(rusty_grid_core::cluster_auth::AUTH_REQUIRED_HINT.to_string());
        let text = describe_auth_error("10.0.0.7:8088", &required).unwrap();
        assert!(text.starts_with("Authentication required"), "{text}");
        assert!(text.contains("OXIDESWARM_SECRET"), "{text}");
        assert!(text.contains("--secret-file"), "{text}");
        assert!(text.contains("10.0.0.7:8088"), "{text}");

        let failed = GridError::AuthFailed("the master rejected the cluster secret".into());
        let text = describe_auth_error("10.0.0.7:8088", &failed).unwrap();
        assert!(text.starts_with("Authentication failed"), "{text}");
        assert!(text.contains("same cluster secret"), "{text}");

        assert!(describe_auth_error("x", &GridError::ConnectionClosed).is_none());
    }

    #[tokio::test]
    async fn test_connect_master_reports_unreachable_master() {
        // Port 1 is never listening.
        let err = connect_master("127.0.0.1:1", None)
            .await
            .err()
            .expect("nothing listens on port 1");
        match err {
            GridError::ConnectionFailed(msg) => {
                assert!(
                    msg.starts_with("Failed to connect to Master at 127.0.0.1:1"),
                    "{msg}"
                );
            }
            other => panic!("expected ConnectionFailed, got {other:?}"),
        }
    }

    /// A master that authenticates with `secret`, then answers one `ClusterStatus` request.
    async fn serve_one_client(secret: ClusterSecret) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let secret = secret.clone();
                tokio::spawn(async move {
                    let mut t = MessageTransport::new(stream);
                    let session_key = match server_handshake(&mut t, &secret).await {
                        Ok(k) => k,
                        Err(_) => return,
                    };
                    t.enable_encryption(&session_key, EndpointRole::Server);
                    if let Ok(Some(ClientMessage::ClusterStatus)) =
                        t.recv_msg::<ClientMessage>().await
                    {
                        let _ = t
                            .send_msg(&ClientResponse::ClusterStatus {
                                total_tasks: 0,
                                pending_tasks: 0,
                                running_tasks: 0,
                                completed_tasks: 0,
                                failed_tasks: 0,
                                workers: vec![],
                            })
                            .await;
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_connect_master_authenticates_with_the_shared_secret() {
        let addr = serve_one_client(secret(2)).await;
        let mut transport = connect_master(&addr.to_string(), Some(&secret(2)))
            .await
            .expect("matching secret must connect");
        transport
            .send_msg(&ClientMessage::ClusterStatus)
            .await
            .unwrap();
        let reply = transport.recv_msg::<ClientResponse>().await.unwrap();
        assert!(matches!(reply, Some(ClientResponse::ClusterStatus { .. })));
    }

    #[tokio::test]
    async fn test_connect_master_rejects_a_master_with_another_secret() {
        let addr = serve_one_client(secret(3)).await;
        let err = connect_master(&addr.to_string(), Some(&secret(4)))
            .await
            .err()
            .expect("a master holding another secret must be refused");
        assert!(matches!(err, GridError::AuthFailed(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn test_connect_master_without_secret_learns_about_auth_on_first_reply() {
        let addr = serve_one_client(secret(5)).await;
        let mut transport = connect_master(&addr.to_string(), None)
            .await
            .expect("plain TCP connect succeeds; the rejection arrives with the first reply");
        transport
            .send_msg(&ClientMessage::ClusterStatus)
            .await
            .unwrap();
        let err: GridError = transport
            .recv_msg::<ClientResponse>()
            .await
            .map(|_| ())
            .map_err(GridError::from)
            .expect_err("an unauthenticated request must not be answered");
        assert!(matches!(err, GridError::AuthRequired(_)), "got {err:?}");
    }
}
