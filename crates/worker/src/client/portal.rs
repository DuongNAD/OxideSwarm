//! Worker standby HTTP redirection portal.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::net::TcpStream;
use tokio::sync::{watch, Semaphore};
use tracing::{debug, info};

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::discovery::{is_valid_host, sanitize_host_port, sanitize_http_origin};

/// Target coordinator reference for the worker standby redirection portal.
#[derive(Clone)]
pub enum PortalTarget {
    Static(String),
    Dynamic(Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>),
}

impl From<String> for PortalTarget {
    fn from(s: String) -> Self {
        PortalTarget::Static(s)
    }
}

impl From<&str> for PortalTarget {
    fn from(s: &str) -> Self {
        PortalTarget::Static(s.to_string())
    }
}

impl From<Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>>
    for PortalTarget
{
    fn from(a: Arc<tokio::sync::RwLock<Option<rusty_grid_core::discovery::MasterBeacon>>>) -> Self {
        PortalTarget::Dynamic(a)
    }
}

/// Port of the Master's dashboard, which the portal sends visitors to.
const PORTAL_DASHBOARD_PORT: u16 = 8080;
/// The portal reads at most this much of a request (the request line and headers).
const PORTAL_MAX_REQUEST_BYTES: usize = 8192;
/// A client that has not sent its headers after this long is dropped.
const PORTAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
/// After answering, how long the portal drains the rest of the request so that closing the socket
/// does not turn into a TCP reset that wipes the response.
const PORTAL_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
/// Simultaneous portal connections; the rest are dropped instead of piling up tasks and sockets.
pub(super) const PORTAL_MAX_CONNECTIONS: usize = 64;

/// Host part of a `host[:port]` / `[v6][:port]` address, if it is a plain valid host.
pub(super) fn portal_host_of(addr: &str) -> Option<String> {
    let host = if let Some(bracketed) = addr.strip_prefix('[') {
        bracketed.split_once(']')?.0
    } else {
        addr.split(':').next().unwrap_or_default()
    };
    is_valid_host(host).then(|| host.to_string())
}

/// `host` as it is written inside a URL authority (IPv6 literals in brackets).
pub(super) fn url_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

/// True for hosts that only mean something on the machine they are read on.
fn is_local_only_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("auto")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback() || ip.is_unspecified())
}

/// True when `addr` targets a loopback master (`127.x.x.x`, `[::1]`, or `localhost`).
pub(crate) fn is_loopback_master_addr(addr: &str) -> bool {
    let Some(host) = portal_host_of(addr.trim()) else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.to_canonical().is_loopback())
}

/// Returns the socket address the standby redirection portal should bind for `target`.
///
/// When the worker targets a loopback master, we bind `127.0.0.1:<portal_port>` rather than
/// `0.0.0.0:<portal_port>` (and rather than skipping the portal altogether):
/// - A visitor on another machine cannot reach a loopback master anyway (and
///   [`resolve_portal_master`] already refuses to serve a loopback redirect to non-loopback
///   clients), so binding all interfaces only triggers a Windows Firewall prompt ("allow public
///   and private networks?") when a worker or test connects to `127.0.0.1`.
/// - Binding `127.0.0.1` instead of skipping the portal preserves local `/api/status` probing,
///   local browser redirects on the same host, and tests that exercise the portal against a
///   loopback master.
pub(super) async fn portal_bind_addr_for_target(
    portal_port: u16,
    target: &PortalTarget,
) -> std::net::SocketAddr {
    let is_loopback = match target {
        PortalTarget::Static(addr) => is_loopback_master_addr(addr),
        PortalTarget::Dynamic(beacon_state) => beacon_state
            .read()
            .await
            .as_ref()
            .is_some_and(|beacon| is_loopback_master_addr(&beacon.cluster_addr)),
    };
    let bind_ip = if is_loopback {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    };
    std::net::SocketAddr::new(bind_ip, portal_port)
}

/// Host of a validated `scheme://host[:port]` origin, without brackets.
fn origin_host(origin: &str) -> Option<String> {
    portal_host_of(origin.split_once("://")?.1)
}

/// Turns a beacon into the `(dashboard origin, cluster address)` the portal reports, or `None`
/// when there is nothing safe and useful to send this visitor to. The dashboard origin is empty
/// when the Master has no dashboard that is reachable from the network.
///
/// Both strings come from a UDP datagram or a config file and are validated here (strict
/// `http(s)://host[:port]` and `host:port`), so they are safe to put into headers, HTML and JS.
/// A dashboard on loopback means nothing to a visitor on another machine; in that case the
/// cluster host is used instead, unless that is loopback as well (the Master runs on this very
/// machine, so the portal has nowhere better to point than itself).
pub(super) fn portal_master_from_beacon(
    beacon: &rusty_grid_core::discovery::MasterBeacon,
    client_ip: IpAddr,
) -> Option<(String, String)> {
    let cluster = sanitize_host_port(&beacon.cluster_addr)?;
    let cluster_host = portal_host_of(&cluster)?;
    // An empty dashboard URL is the Master saying that no dashboard is reachable from the network
    // (it listens on loopback only, or is off). Guessing `http://<cluster host>:8080` would send
    // the visitor to a dead address: report the Master (empty dashboard) and let the portal keep
    // its standby page.
    if beacon.web_ui_url.trim().is_empty() {
        if is_local_only_host(&cluster_host) && !client_ip.is_loopback() {
            return None;
        }
        return Some((String::new(), cluster));
    }
    let advertised = sanitize_http_origin(&beacon.web_ui_url);
    let ui = match advertised {
        Some(ui)
            if client_ip.is_loopback()
                || !origin_host(&ui).is_some_and(|h| is_local_only_host(&h)) =>
        {
            ui
        }
        _ => {
            if is_local_only_host(&cluster_host) && !client_ip.is_loopback() {
                return None;
            }
            format!(
                "http://{}:{}",
                url_host(&cluster_host),
                PORTAL_DASHBOARD_PORT
            )
        }
    };
    Some((ui, cluster))
}

async fn resolve_portal_master(
    target: &PortalTarget,
    secret: Option<&ClusterSecret>,
    client_ip: IpAddr,
) -> Option<(String, String)> {
    match target {
        PortalTarget::Dynamic(beacon_state) => {
            let guard = beacon_state.read().await;
            guard
                .as_ref()
                .and_then(|beacon| portal_master_from_beacon(beacon, client_ip))
        }
        PortalTarget::Static(addr) => {
            if addr.is_empty() || addr.eq_ignore_ascii_case("auto") || addr.starts_with("auto:") {
                let port = if let Some(port_str) = addr.strip_prefix("auto:") {
                    port_str
                        .parse::<u16>()
                        .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
                } else {
                    rusty_grid_core::DEFAULT_DISCOVERY_PORT
                };
                let beacon = rusty_grid_core::discovery::discover_master_with_secret(
                    Duration::from_millis(350),
                    port,
                    secret,
                )
                .await?;
                portal_master_from_beacon(&beacon, client_ip)
            } else {
                let cluster = sanitize_host_port(addr)
                    .or_else(|| is_valid_host(addr).then(|| format!("{addr}:8088")))?;
                let host = portal_host_of(&cluster)?;
                // The operator configured this address, but a loopback one is only meaningful to
                // a visitor on this machine.
                if is_local_only_host(&host) && !client_ip.is_loopback() {
                    return None;
                }
                Some((
                    format!("http://{}:{}", url_host(&host), PORTAL_DASHBOARD_PORT),
                    cluster,
                ))
            }
        }
    }
}

/// Derives the local user's master dashboard URL for tray status snapshots (`TRAY-1`, `0112` D1.2).
pub(super) fn derive_local_dashboard_url(
    beacon: Option<&rusty_grid_core::discovery::MasterBeacon>,
    remote_desc: &str,
) -> Option<String> {
    if let Some(b) = beacon {
        return portal_master_from_beacon(b, IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
            .and_then(|(ui, _cluster)| (!ui.trim().is_empty()).then_some(ui));
    }
    let trimmed = remote_desc.trim();
    if trimmed.is_empty() || trimmed.starts_with("iroh://") || trimmed.eq_ignore_ascii_case("auto")
    {
        return None;
    }
    let host = portal_host_of(trimmed)?;
    sanitize_http_origin(&format!(
        "http://{}:{}",
        url_host(&host),
        PORTAL_DASHBOARD_PORT
    ))
}

fn worker_portal_standby_html() -> String {
    r#"<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>OxideSwarm Standby Node</title>
  <style>
    body { background:#090d16; color:#e2e8f0; font-family:-apple-system,BlinkMacSystemFont,sans-serif; text-align:center; padding:40px 16px; margin:0; }
    .card { max-width:480px; margin:0 auto; background:#111827; border:1px solid #1f293d; border-radius:12px; padding:24px; box-shadow:0 4px 20px rgba(0,0,0,0.5); }
    .title { font-size:18px; font-weight:700; color:#38bdf8; margin-bottom:12px; }
    .desc { font-size:14px; color:#94a3b8; line-height:1.6; margin-bottom:20px; }
    .spinner { display:inline-block; width:36px; height:36px; border:3px solid rgba(56,189,248,0.2); border-top-color:#38bdf8; border-radius:50%; animation:spin 1s linear infinite; margin-bottom:16px; }
    @keyframes spin { 100% { transform:rotate(360deg); } }
    .btn { display:inline-block; background:#0284c7; color:#fff; padding:10px 20px; border-radius:6px; text-decoration:none; font-weight:600; font-size:13px; cursor:pointer; border:none; }
    .status { font-size:12px; color:#f59e0b; margin-top:14px; font-family:monospace; }
  </style>
</head>
<body>
  <div class="card">
    <div class="spinner"></div>
    <div class="title">OxideSwarm Standby Node (Worker)</div>
    <div class="desc">This device is operating as a <strong>Worker</strong>. Automatically discovering an active Master on the LAN...</div>
    <button class="btn" onclick="probeCluster()">Retry Connection Now</button>
    <div class="status" id="scanStatus">Scanning cluster...</div>
  </div>
  <script>
    // Asks this worker (same origin) which Master it is connected to; the answer is validated
    // server-side, and checked again here before the browser is sent anywhere.
    const safeOrigin = /^https?:\/\/[A-Za-z0-9._\-]+(:[0-9]{1,5})?$|^https?:\/\/\[[0-9A-Fa-f:.]+\](:[0-9]{1,5})?$/;
    async function probeCluster() {
      const s = document.getElementById('scanStatus');
      if (s) s.textContent = 'Checking cluster nodes...';
      try {
        const res = await fetch('/api/status', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          const url = data && data.redirect_url;
          if (typeof url === 'string' && safeOrigin.test(url)) {
            if (s) s.textContent = 'Master found at ' + url + '! Redirecting...';
            window.location.replace(url);
            return;
          }
        }
      } catch (_) {}
      if (s) s.textContent = 'No Master detected yet. Retrying in 2 seconds...';
    }
    probeCluster();
    setInterval(probeCluster, 2000);
  </script>
</body>
</html>"#
    .to_string()
}

/// Wraps `body` in a complete `Connection: close` HTTP response. The portal never needs
/// cross-origin access (its standby page only talks to its own origin), so it sends no CORS header.
fn portal_http_response(
    status: &str,
    content_type: &str,
    extra_headers: &str,
    body: &str,
) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{extra_headers}X-Content-Type-Options: nosniff\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The JSON answer to `GET /api/status`.
pub(super) fn portal_status_response(master: Option<&(String, String)>) -> String {
    let body = match master {
        // A known Master without a dashboard to send the visitor to.
        Some((web_ui, cluster)) if web_ui.is_empty() => serde_json::json!({
            "role": "WORKER_PORTAL",
            "status": "connected_to_master",
            "master_cluster_addr": cluster,
            "master_web_ui": null,
            "redirect_url": null,
        }),
        Some((web_ui, cluster)) => serde_json::json!({
            "role": "WORKER_PORTAL",
            "status": "connected_to_master",
            "master_cluster_addr": cluster,
            "master_web_ui": web_ui,
            "redirect_url": web_ui,
        }),
        None => serde_json::json!({
            "role": "WORKER_PORTAL",
            "status": "searching_master",
            "master_cluster_addr": null,
            "master_web_ui": null,
            "redirect_url": null,
        }),
    };
    portal_http_response("200 OK", "application/json", "", &body.to_string())
}

/// The redirect to the Master, or the standby page if either value fails validation. Re-validates
/// its inputs, so it is safe whatever the caller passes.
pub(super) fn portal_redirect_response(web_ui_url: &str, cluster_addr: &str) -> String {
    let Some(web_ui) = sanitize_http_origin(web_ui_url) else {
        return portal_standby_response();
    };
    // The cluster address is informational; a value that fails validation is simply left out.
    let master_header = sanitize_host_port(cluster_addr)
        .map(|c| format!("X-OxideSwarm-Master: {c}\r\n"))
        .unwrap_or_default();
    // `web_ui` is `scheme://host[:port]` over [A-Za-z0-9._:-] and brackets: nothing in it can
    // end a header line, an HTML attribute or a JavaScript string.
    let html_body = format!(
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"0; url={0}\"><title>OxideSwarm Auto-Redirect</title></head><body style=\"font-family:-apple-system,BlinkMacSystemFont,sans-serif;text-align:center;padding:50px;background:#090d16;color:#e2e8f0;\"><h2>OxideSwarm Node (Worker)</h2><p>This device is operating as a Worker. Redirecting to Master at <a style=\"color:#38bdf8;\" href=\"{0}\">{0}</a>...</p><script>window.location.replace(\"{0}\");</script></body></html>",
        web_ui
    );
    portal_http_response(
        "307 Temporary Redirect",
        "text/html; charset=utf-8",
        &format!("Location: {web_ui}\r\n{master_header}"),
        &html_body,
    )
}

pub(super) fn portal_standby_response() -> String {
    portal_http_response(
        "200 OK",
        "text/html; charset=utf-8",
        "",
        &worker_portal_standby_html(),
    )
}

/// Reads the request line (and headers) of a portal visitor: up to the blank line, at most
/// [`PORTAL_MAX_REQUEST_BYTES`], within [`PORTAL_REQUEST_TIMEOUT`]. Returns the request line.
pub(super) async fn read_portal_request_line<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Option<String> {
    read_portal_request_line_with(stream, PORTAL_REQUEST_TIMEOUT).await
}

pub(super) async fn read_portal_request_line_with<S: AsyncRead + Unpin>(
    stream: &mut S,
    timeout: Duration,
) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let read_head = async {
        let mut head = Vec::with_capacity(512);
        let mut chunk = [0u8; 1024];
        loop {
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            head.extend_from_slice(&chunk[..n]);
            let complete =
                head.windows(4).any(|w| w == b"\r\n\r\n") || head.windows(2).any(|w| w == b"\n\n");
            if complete || head.len() >= PORTAL_MAX_REQUEST_BYTES {
                break;
            }
        }
        head.truncate(PORTAL_MAX_REQUEST_BYTES);
        Some(head)
    };
    let head = tokio::time::timeout(timeout, read_head).await.ok()??;
    let text = String::from_utf8_lossy(&head);
    let line = text.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_string())
}

/// True if the request line asks for `/api/status` (with or without a query string).
pub(super) fn is_status_request(request_line: &str) -> bool {
    let mut parts = request_line.split_whitespace();
    let _method = parts.next();
    let Some(target) = parts.next() else {
        return false;
    };
    let path = target.split(['?', '#']).next().unwrap_or_default();
    path == "/api/status"
}

async fn handle_portal_connection(
    mut stream: TcpStream,
    client_ip: IpAddr,
    target: PortalTarget,
    secret: Option<ClusterSecret>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Some(request_line) = read_portal_request_line(&mut stream).await else {
        return;
    };
    let master = resolve_portal_master(&target, secret.as_ref(), client_ip).await;
    let response = if is_status_request(&request_line) {
        portal_status_response(master.as_ref())
    } else {
        match master {
            Some((web_ui_url, cluster_addr)) => {
                portal_redirect_response(&web_ui_url, &cluster_addr)
            }
            None => portal_standby_response(),
        }
    };
    let _ = stream.write_all(response.as_bytes()).await;
    // Half-close, then swallow whatever else the client sent: closing a socket that still has
    // unread data resets the connection and can discard the response before the client reads it.
    let _ = stream.shutdown().await;
    let mut sink = [0u8; 1024];
    let _ = tokio::time::timeout(PORTAL_DRAIN_TIMEOUT, async {
        while matches!(stream.read(&mut sink).await, Ok(n) if n > 0) {}
    })
    .await;
}

/// Spawns a lightweight HTTP portal on `portal_port` that redirects incoming browser visits
/// (e.g. from smartphones or bookmarks) to the active Master node.
pub async fn spawn_worker_redirection_portal(
    portal_port: u16,
    target: impl Into<PortalTarget>,
    shutdown_rx: watch::Receiver<bool>,
) {
    spawn_worker_redirection_portal_with_secret(portal_port, target, None, shutdown_rx).await;
}

/// Like [`spawn_worker_redirection_portal`], with the cluster secret to use when the portal has to
/// discover the Master itself (`auto` target): only a Master holding the secret is believed.
pub async fn spawn_worker_redirection_portal_with_secret(
    portal_port: u16,
    target: impl Into<PortalTarget>,
    secret: Option<ClusterSecret>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let target = target.into();
    let bind_addr = portal_bind_addr_for_target(portal_port, &target).await;
    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            debug!(
                bind_addr = %bind_addr,
                port = portal_port,
                error = %e,
                "Worker standby redirection portal not started (port 8080 likely bound by Master or other service)"
            );
            return;
        }
    };

    info!(
        bind_addr = %bind_addr,
        portal_port = portal_port,
        "Worker standby HTTP redirection portal active"
    );

    run_portal(
        listener,
        target,
        secret,
        shutdown_rx,
        PORTAL_MAX_CONNECTIONS,
    )
    .await;
}

/// Serves the portal on an already bound listener until shutdown, with at most
/// `max_connections` requests in flight.
pub(super) async fn run_portal(
    listener: tokio::net::TcpListener,
    target: PortalTarget,
    secret: Option<ClusterSecret>,
    mut shutdown_rx: watch::Receiver<bool>,
    max_connections: usize,
) {
    let slots = Arc::new(Semaphore::new(max_connections));
    loop {
        tokio::select! {
            res = listener.accept() => {
                if let Ok((stream, client_addr)) = res {
                    // Over the cap the connection is simply dropped.
                    let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                        continue;
                    };
                    let target = target.clone();
                    let secret = secret.clone();
                    tokio::spawn(async move {
                        handle_portal_connection(stream, client_addr.ip(), target, secret).await;
                        drop(slot);
                    });
                }
            }
            res = shutdown_rx.changed() => {
                if (res.is_ok() && *shutdown_rx.borrow()) || res.is_err() {
                    break;
                }
            }
        }
    }
}
