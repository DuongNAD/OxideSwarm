//! Master Node LAN Discovery Protocol & Beacon Utilities.
//!
//! Provides zero-configuration LAN discovery for OxideSwarm Master nodes
//! using UDP broadcast beacons and active discovery probes.
//!
//! # Authenticated discovery
//!
//! A client that holds the cluster secret sends `OX_DISCOVER:<32 hex chars>` (a random 16-byte
//! probe nonce); a master that holds the secret answers with a beacon whose `mac` is
//! `HMAC(secret, "oxs1-beacon" || probe_nonce || cluster_addr || web_ui_url)` (the two
//! variable-length fields are length-prefixed). The client discards beacons whose MAC is missing
//! or wrong, so a LAN host that does not know the secret cannot make workers follow it, and
//! recorded beacons cannot be replayed because the nonce is fresh per discovery call. Plain
//! `OX_DISCOVER` probes and unsigned beacons keep working between peers that have no secret.
//! Unsolicited periodic broadcasts cannot carry a probe-bound MAC, so secret holders ignore them.
//!
//! Which address a client acts on depends on whether the beacon is signed:
//!
//! * **Signed beacon (MAC verified):** the advertised `cluster_addr` is covered by the MAC, so it
//!   is authoritative and is used as signed (see [`accept_verified_beacon`]). It must *not* be
//!   re-addressed to the UDP source IP: the MAC does not cover the source, and the probe nonce
//!   travels in clear on the broadcast domain, so any LAN host can obtain a validly signed beacon
//!   from the real master and forward it from its own address. Rewriting to the source IP would
//!   turn that harmless relay into "dial the relaying host" and let it sit in the middle of the
//!   (unencrypted) TCP session.
//! * **Unsigned beacon (peers without a secret only):** the self-reported IP is never trusted
//!   blindly; the client uses the UDP *source* IP plus the advertised port (see
//!   [`normalize_beacon_source`]).
//!
//! The TCP handshake in [`crate::cluster_auth`] remains the actual authority: discovery only
//! decides where to knock.
//!
//! # Fallback candidates
//!
//! Nothing is hard-coded: besides broadcast, subnet broadcast and loopback, a probe is unicast to
//! the hosts listed in the `OXIDESWARM_DISCOVERY_CANDIDATES` environment variable (comma or space
//! separated IPs / host names), for networks whose access point blocks broadcasts.
//!
//! If UDP broadcast fails, discovery executes an authenticated 3-tier fallback hierarchy:
//! 1. Tier 1: Fast UDP broadcast and unicast active probe (port 8089) with fresh nonce.
//! 2. Tier 2: Authenticated HTTP probe (`GET /api/discovery/probe?nonce=<hex>`) across candidates,
//!    the default gateway, and loopback, verified with HMAC-SHA256 against the probe nonce.
//! 3. Tier 3: Authenticated Rendezvous Registry service (`OXIDESWARM_RENDEZVOUS` or
//!    `--rendezvous`) querying a signed Master lease.

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::cluster_auth::{hex_decode, hex_encode, random_bytes, ClusterSecret};

/// Default UDP port for OxideSwarm discovery beacons and queries.
pub const DEFAULT_DISCOVERY_PORT: u16 = 8089;

/// Magic payload sent by clients to actively probe for Master nodes.
pub const DISCOVERY_MAGIC_REQUEST: &[u8] = b"OX_DISCOVER";

/// Prefix of an authenticated probe: `OX_DISCOVER:` followed by the hex-encoded probe nonce.
pub const DISCOVERY_AUTH_PROBE_PREFIX: &[u8] = b"OX_DISCOVER:";

/// Length in bytes of the random nonce carried by an authenticated probe.
pub const DISCOVERY_PROBE_NONCE_LEN: usize = 16;

/// Domain-separation label of the beacon MAC.
const BEACON_MAC_LABEL: &[u8] = b"oxs1-beacon";

/// Service identifier to distinguish OxideSwarm beacons from other traffic.
pub const DISCOVERY_SERVICE_NAME: &str = "oxideswarm-master";

/// Current discovery protocol version.
pub const DISCOVERY_PROTOCOL_VERSION: u32 = 1;

/// Payload broadcast by Master node to advertise its cluster and web endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MasterBeacon {
    /// Service signature, must match `DISCOVERY_SERVICE_NAME`
    pub service: String,
    /// Protocol version
    pub version: u32,
    /// Cluster TCP coordinator address, e.g. "192.168.1.144:8088"
    pub cluster_addr: String,
    /// Web UI Dashboard HTTP URL, e.g. "http://192.168.1.144:8080"
    pub web_ui_url: String,
    /// Hostname of the Master node
    pub hostname: String,
    /// Current number of connected workers
    pub worker_count: usize,
    /// Timestamp (epoch seconds)
    pub timestamp: u64,
    /// Hex HMAC proving the sender holds the cluster secret (answers to authenticated probes only).
    /// Absent in beacons from secret-less masters and in unsolicited broadcasts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
}

impl MasterBeacon {
    /// Creates a new MasterBeacon with current timestamp.
    pub fn new(
        cluster_addr: impl Into<String>,
        web_ui_url: impl Into<String>,
        hostname: impl Into<String>,
        worker_count: usize,
    ) -> Self {
        Self {
            service: DISCOVERY_SERVICE_NAME.to_string(),
            version: DISCOVERY_PROTOCOL_VERSION,
            cluster_addr: cluster_addr.into(),
            web_ui_url: web_ui_url.into(),
            hostname: hostname.into(),
            worker_count,
            timestamp: chrono::Utc::now().timestamp() as u64,
            mac: None,
        }
    }

    /// Signs the connection-relevant fields for the probe identified by `probe_nonce`.
    pub fn sign(&mut self, secret: &ClusterSecret, probe_nonce: &[u8]) {
        let tag = secret.mac(&[&self.mac_input(probe_nonce)]);
        self.mac = Some(hex_encode(&tag));
    }

    /// Returns true if `mac` is a valid signature (checked in constant time) by `secret` for the
    /// probe identified by `probe_nonce`. A missing or malformed `mac` is simply invalid.
    pub fn verify(&self, secret: &ClusterSecret, probe_nonce: &[u8]) -> bool {
        match self.mac.as_deref().and_then(hex_decode) {
            Some(tag) => secret.verify_mac(&[&self.mac_input(probe_nonce)], &tag),
            None => false,
        }
    }

    /// `label || (len || field)*` over the probe nonce and the fields a client acts on. The length
    /// prefixes keep the variable-length fields from sliding into each other.
    fn mac_input(&self, probe_nonce: &[u8]) -> Vec<u8> {
        let fields = [
            probe_nonce,
            self.cluster_addr.as_bytes(),
            self.web_ui_url.as_bytes(),
        ];
        let mut buf = Vec::with_capacity(
            BEACON_MAC_LABEL.len() + fields.iter().map(|f| f.len() + 4).sum::<usize>(),
        );
        buf.extend_from_slice(BEACON_MAC_LABEL);
        for field in fields {
            buf.extend_from_slice(&(field.len() as u32).to_be_bytes());
            buf.extend_from_slice(field);
        }
        buf
    }

    /// Serializes beacon to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Parses JSON bytes into a MasterBeacon, validating service signature.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let beacon: Self = serde_json::from_slice(bytes).ok()?;
        if beacon.service == DISCOVERY_SERVICE_NAME {
            Some(beacon)
        } else {
            None
        }
    }
}

/// Domain separation label for rendezvous lease MAC.
pub const RENDEZVOUS_MAC_LABEL: &[u8] = b"oxs1-rendezvous";

/// Environment variable specifying rendezvous registry URL.
pub const RENDEZVOUS_ENV: &str = "OXIDESWARM_RENDEZVOUS";

/// Master Registration Payload published to Rendezvous Registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RendezvousLease {
    pub service: String,
    pub version: u32,
    pub cluster_addr: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub web_ui_url: String,
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_ticket: Option<String>,
    pub worker_count: usize,
    pub timestamp: u64,
    pub ttl_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl RendezvousLease {
    pub fn new(
        cluster_addr: impl Into<String>,
        web_ui_url: impl Into<String>,
        hostname: impl Into<String>,
        p2p_ticket: Option<String>,
        worker_count: usize,
        ttl_secs: u64,
    ) -> Self {
        Self {
            service: DISCOVERY_SERVICE_NAME.to_string(),
            version: DISCOVERY_PROTOCOL_VERSION,
            cluster_addr: cluster_addr.into(),
            web_ui_url: web_ui_url.into(),
            hostname: hostname.into(),
            p2p_ticket,
            worker_count,
            timestamp: chrono::Utc::now().timestamp() as u64,
            ttl_secs,
            signature: None,
        }
    }

    pub fn sign(&mut self, secret: &ClusterSecret) {
        let tag = secret.mac(&[&self.mac_input()]);
        self.signature = Some(hex_encode(&tag));
    }

    pub fn verify(&self, secret: &ClusterSecret, max_skew_secs: u64) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        if now + max_skew_secs < self.timestamp {
            return false;
        }
        if now
            > self
                .timestamp
                .saturating_add(self.ttl_secs)
                .saturating_add(max_skew_secs)
        {
            return false;
        }
        match self.signature.as_deref().and_then(hex_decode) {
            Some(tag) => secret.verify_mac(&[&self.mac_input()], &tag),
            None => false,
        }
    }

    fn mac_input(&self) -> Vec<u8> {
        let ts_str = self.timestamp.to_string();
        let ttl_str = self.ttl_secs.to_string();
        let fields = [
            self.cluster_addr.as_bytes(),
            self.web_ui_url.as_bytes(),
            self.hostname.as_bytes(),
            ts_str.as_bytes(),
            ttl_str.as_bytes(),
        ];
        let mut buf = Vec::with_capacity(
            RENDEZVOUS_MAC_LABEL.len() + fields.iter().map(|f| f.len() + 4).sum::<usize>(),
        );
        buf.extend_from_slice(RENDEZVOUS_MAC_LABEL);
        for field in fields {
            buf.extend_from_slice(&(field.len() as u32).to_be_bytes());
            buf.extend_from_slice(field);
        }
        buf
    }

    pub fn to_beacon(&self) -> MasterBeacon {
        let mut beacon = MasterBeacon::new(
            &self.cluster_addr,
            &self.web_ui_url,
            &self.hostname,
            self.worker_count,
        );
        beacon.timestamp = self.timestamp;
        beacon
    }
}

/// A parsed UDP discovery probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryProbe {
    /// Legacy `OX_DISCOVER`: the sender has no cluster secret.
    Plain,
    /// `OX_DISCOVER:<hex nonce>`: the sender holds the secret and wants a signed beacon.
    Authenticated {
        /// Fresh random nonce the beacon MAC must bind.
        nonce: [u8; DISCOVERY_PROBE_NONCE_LEN],
    },
}

/// Builds the datagram of an authenticated probe carrying `nonce`.
pub fn build_auth_probe(nonce: &[u8; DISCOVERY_PROBE_NONCE_LEN]) -> Vec<u8> {
    let mut probe = DISCOVERY_AUTH_PROBE_PREFIX.to_vec();
    probe.extend_from_slice(hex_encode(nonce).as_bytes());
    probe
}

/// Parses a datagram received by a master. Returns `None` for anything that is not a probe.
pub fn parse_probe(datagram: &[u8]) -> Option<DiscoveryProbe> {
    if datagram == DISCOVERY_MAGIC_REQUEST {
        return Some(DiscoveryProbe::Plain);
    }
    let hex = datagram.strip_prefix(DISCOVERY_AUTH_PROBE_PREFIX)?;
    if hex.len() != DISCOVERY_PROBE_NONCE_LEN * 2 {
        return None;
    }
    let bytes = hex_decode(std::str::from_utf8(hex).ok()?)?;
    let nonce = <[u8; DISCOVERY_PROBE_NONCE_LEN]>::try_from(bytes.as_slice()).ok()?;
    Some(DiscoveryProbe::Authenticated { nonce })
}

/// Master-side responder: given an incoming datagram, returns the bytes to send back, or `None`
/// if the datagram is not a discovery probe.
///
/// With a `secret` and an authenticated probe the beacon is signed for that probe's nonce; in
/// every other case it is sent unsigned (secret-holding clients ignore unsigned beacons, so a
/// master without the secret cannot be followed by them).
pub fn beacon_reply(
    datagram: &[u8],
    mut beacon: MasterBeacon,
    secret: Option<&ClusterSecret>,
) -> Option<Vec<u8>> {
    match (parse_probe(datagram)?, secret) {
        (DiscoveryProbe::Authenticated { nonce }, Some(secret)) => beacon.sign(secret, &nonce),
        _ => beacon.mac = None,
    }
    beacon.to_bytes().ok()
}

/// Splits an advertised `host:port` (`[v6]:port` accepted) into its parts.
fn split_host_port(addr: &str) -> Option<(&str, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port: u16 = port.parse().ok().filter(|p| *p != 0)?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    (!host.is_empty()).then_some((host, port))
}

/// True for an IP literal or a plain DNS name (letters, digits, `-`, `_` and `.`), i.e. the only
/// host forms that are safe to put into an HTTP header, an HTML attribute or a JS string verbatim.
pub fn is_valid_host(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    if host.parse::<IpAddr>().is_ok() {
        return true;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

/// Validates a `host:port` (`[v6]:port` accepted) and returns it in canonical form, or `None` if
/// the host is not an IP literal / plain DNS name or the port is missing or zero.
pub fn sanitize_host_port(addr: &str) -> Option<String> {
    let (host, port) = split_host_port(addr)?;
    if !is_valid_host(host) {
        return None;
    }
    Some(match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => format!("[{v6}]:{port}"),
        _ => format!("{host}:{port}"),
    })
}

/// Validates an `http(s)://host[:port]` origin and returns it in canonical form
/// (`scheme://host[:port]`, lower-case scheme, no trailing slash), or `None` for anything else:
/// other schemes (`javascript:`, `data:`), user info, paths, queries, fragments, whitespace and
/// control characters. Whatever passes can be reflected into a `Location` header, an HTML
/// attribute or a JavaScript string without further escaping.
pub fn sanitize_http_origin(url: &str) -> Option<String> {
    let lower_prefix = |p: &str| {
        url.get(..p.len())
            .is_some_and(|s| s.eq_ignore_ascii_case(p))
    };
    let (scheme, rest) = if lower_prefix("https://") {
        ("https", &url["https://".len()..])
    } else if lower_prefix("http://") {
        ("http", &url["http://".len()..])
    } else {
        return None;
    };
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let (inner, tail) = bracketed.split_once(']')?;
        inner.parse::<std::net::Ipv6Addr>().ok()?;
        let port = match tail {
            "" => None,
            t => Some(t.strip_prefix(':')?),
        };
        (format!("[{inner}]"), port)
    } else {
        match authority.split_once(':') {
            Some((host, port)) => (host.to_string(), Some(port)),
            None => (authority.to_string(), None),
        }
    };
    let bare_host = host.trim_start_matches('[').trim_end_matches(']');
    if !is_valid_host(bare_host) {
        return None;
    }
    match port {
        None => Some(format!("{scheme}://{host}")),
        Some(p) => {
            let n: u16 = p
                .bytes()
                .all(|b| b.is_ascii_digit())
                .then(|| p.parse().ok())
                .flatten()
                .filter(|n| *n != 0)?;
            Some(format!("{scheme}://{host}:{n}"))
        }
    }
}

/// Checks the `host:port` a beacon advertises against the address it arrived from and returns the
/// advertised `(host, port)`, or `None` if the beacon must be ignored: a loopback/unspecified/
/// `localhost` host from a non-loopback sender (the sender is pointing us at ourselves), a
/// multicast or broadcast address, or a missing/zero port.
fn check_advertised_addr(cluster_addr: &str, from: SocketAddr) -> Option<(&str, u16)> {
    let (host, port) = split_host_port(cluster_addr)?;
    if !is_valid_host(host) {
        return None;
    }
    let sender_is_local = from.ip().is_loopback();

    if let Ok(ip) = host.parse::<IpAddr>() {
        let bogus = ip.is_multicast()
            || matches!(ip, IpAddr::V4(v4) if v4.is_broadcast())
            || (!sender_is_local && (ip.is_loopback() || ip.is_unspecified()));
        if bogus {
            return None;
        }
    } else if !sender_is_local && host.eq_ignore_ascii_case("localhost") {
        return None;
    }
    Some((host, port))
}

/// Decides which address to connect to for an **unsigned** beacon that arrived from `from`, or
/// `None` if the beacon must be ignored. Only for peers that hold no cluster secret; a beacon
/// whose MAC verified goes through [`accept_verified_beacon`] instead.
///
/// The returned beacon's `cluster_addr` is the datagram's **source IP** joined with the
/// **advertised port**, never the self-reported IP, so a LAN host cannot redirect workers to a
/// third machine by lying inside the payload. Beacons are dropped when the advertised host is
/// unusable (see [`check_advertised_addr`]).
pub fn normalize_beacon_source(mut beacon: MasterBeacon, from: SocketAddr) -> Option<MasterBeacon> {
    let (_, port) = check_advertised_addr(&beacon.cluster_addr, from)?;
    beacon.cluster_addr = SocketAddr::new(from.ip(), port).to_string();
    Some(beacon)
}

/// Decides whether to act on a beacon whose MAC has **already been verified**
/// ([`MasterBeacon::verify`]) and returns it with the signed `cluster_addr` untouched, or `None`
/// if the advertised address is unusable (same checks as [`normalize_beacon_source`]).
///
/// The address is deliberately *not* replaced by the UDP source IP: the MAC binds the advertised
/// address but not the datagram's source, and the probe nonce is broadcast in clear, so a LAN
/// attacker can fetch a genuine signed beacon for a victim's nonce and forward it from its own
/// IP. Following the source IP would then dial the attacker, who could relay the TCP stream to
/// the real master and read or alter it (TCP mode authenticates but does not encrypt). Following
/// the signed address sends the victim to the real master whoever relayed the datagram.
pub fn accept_verified_beacon(beacon: MasterBeacon, from: SocketAddr) -> Option<MasterBeacon> {
    check_advertised_addr(&beacon.cluster_addr, from)?;
    Some(beacon)
}

/// Discovers the local machine's primary non-loopback IPv4 address
/// using OS routing table resolution without generating network traffic.
pub fn get_local_ip() -> Option<IpAddr> {
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        for target in &[
            "8.8.8.8:80",
            "192.168.1.1:80",
            "192.168.1.254:80",
            "10.0.0.1:80",
            "172.16.0.1:80",
        ] {
            if socket.connect(target).is_ok() {
                if let Ok(local_addr) = socket.local_addr() {
                    let ip = local_addr.ip();
                    if !ip.is_loopback() && !ip.is_unspecified() {
                        return Some(ip);
                    }
                }
            }
        }
    }
    None
}

/// Returns the detected local IP or falls back to loopback (127.0.0.1).
pub fn get_local_ip_or_loopback() -> IpAddr {
    get_local_ip().unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
}

/// Returns the directed IPv4 subnet broadcast address (e.g. 192.168.1.255)
/// based on the local machine's detected IP address, or standard 255.255.255.255.
pub fn get_subnet_broadcast_ip() -> IpAddr {
    if let Some(IpAddr::V4(ipv4)) = get_local_ip() {
        let o = ipv4.octets();
        IpAddr::V4(Ipv4Addr::new(o[0], o[1], o[2], 255))
    } else {
        IpAddr::V4(Ipv4Addr::BROADCAST)
    }
}

/// Derives the standard default gateway IP candidate (e.g. x.x.x.1) for the local subnet.
pub fn get_default_gateway_candidate() -> Option<IpAddr> {
    if let Some(IpAddr::V4(ipv4)) = get_local_ip() {
        let o = ipv4.octets();
        Some(IpAddr::V4(Ipv4Addr::new(o[0], o[1], o[2], 1)))
    } else {
        None
    }
}

/// Environment variable listing extra hosts to probe (comma or whitespace separated IPs / host
/// names), for networks where broadcast does not reach the master.
pub const DISCOVERY_CANDIDATES_ENV: &str = "OXIDESWARM_DISCOVERY_CANDIDATES";

/// Upper bound on configured candidates, so a typo cannot turn a probe into a scan.
const MAX_CONFIGURED_CANDIDATES: usize = 16;

/// Parses a candidate list, silently dropping entries that are not a valid IP / host name / host:port.
pub fn parse_candidate_hosts(list: &str) -> Vec<String> {
    list.split(|c: char| c == ',' || c.is_whitespace())
        .filter(|h| {
            if is_valid_host(h) {
                true
            } else if let Some((host, _port)) = split_host_port(h) {
                is_valid_host(host)
            } else {
                false
            }
        })
        .take(MAX_CONFIGURED_CANDIDATES)
        .map(str::to_string)
        .collect()
}

/// Candidate hosts configured through [`DISCOVERY_CANDIDATES_ENV`] (empty when unset).
pub fn configured_candidate_hosts() -> Vec<String> {
    std::env::var(DISCOVERY_CANDIDATES_ENV)
        .or_else(|_| std::env::var("RUSTY_GRID_DISCOVERY_CANDIDATES"))
        .map(|v| parse_candidate_hosts(&v))
        .unwrap_or_default()
}

/// Probes an individual candidate node via HTTP GET /api/status.
pub async fn probe_http_candidate(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Option<MasterBeacon> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    // `host` ends up in the request line's Host header and in the beacon we hand back.
    if !is_valid_host(host) {
        return None;
    }
    let addr_str = format!("{}:{}", host, port);
    let connect_future = TcpStream::connect(&addr_str);
    let mut stream = tokio::time::timeout(timeout, connect_future)
        .await
        .ok()?
        .ok()?;
    let _ = stream.set_nodelay(true);

    let req = format!(
        "GET /api/status HTTP/1.1\r\nHost: {}\r\nUser-Agent: OxideSwarm-Discovery/1.0\r\nConnection: close\r\n\r\n",
        host
    );
    tokio::time::timeout(timeout, stream.write_all(req.as_bytes()))
        .await
        .ok()?
        .ok()?;

    let mut buf = Vec::with_capacity(2048);
    let mut temp = [0u8; 1024];
    let read_future = async {
        while let Ok(n) = stream.read(&mut temp).await {
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&temp[..n]);
            if buf.len() > 16384 {
                break;
            }
            if let Some(idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                if serde_json::from_slice::<serde_json::Value>(&buf[idx + 4..]).is_ok() {
                    break;
                }
            }
        }
    };
    let _ = tokio::time::timeout(timeout, read_future).await;

    let resp_str = String::from_utf8_lossy(&buf);
    let json_start = resp_str.find("\r\n\r\n").map(|i| i + 4)?;
    let json: serde_json::Value = serde_json::from_str(&resp_str[json_start..]).ok()?;

    // Case 1: Node is an active Master
    if let Some(master_obj) = json.get("master") {
        if master_obj.get("role").and_then(|r| r.as_str()) == Some("MASTER") {
            let host_name = master_obj
                .get("host")
                .and_then(|h| h.as_str())
                .unwrap_or("OxideMaster");
            let worker_count = json
                .get("workers")
                .and_then(|w| w.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            return Some(MasterBeacon::new(
                format!("{}:8088", host),
                format!("http://{}:{}", host, port),
                host_name,
                worker_count,
            ));
        }
    }

    // Case 2: Node is a Worker Redirection Portal pointing to Master. The portal's answer is
    // untrusted text: only a strictly valid http(s) origin / host:port is carried over.
    if json.get("role").and_then(|r| r.as_str()) == Some("WORKER_PORTAL") {
        if let Some(origin) = json
            .get("redirect_url")
            .and_then(|u| u.as_str())
            .and_then(sanitize_http_origin)
        {
            let target_host = origin
                .split_once("://")
                .map(|(_, authority)| authority)
                .and_then(|authority| {
                    authority
                        .strip_prefix('[')
                        .and_then(|v6| v6.split_once(']').map(|(inner, _)| inner))
                        .or_else(|| authority.split(':').next())
                })
                .unwrap_or_default()
                .to_string();
            let is_local = target_host.eq_ignore_ascii_case("localhost")
                || target_host
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback() || ip.is_unspecified());
            if !target_host.is_empty() && !is_local {
                let cluster_addr = json
                    .get("master_cluster_addr")
                    .and_then(|a| a.as_str())
                    .and_then(sanitize_host_port)
                    .unwrap_or_else(|| format!("{}:8088", target_host));
                return Some(MasterBeacon::new(
                    cluster_addr,
                    origin,
                    "DiscoveredViaPortal",
                    1,
                ));
            }
        }
    }

    None
}

/// Probes an individual candidate node via HTTP, using GET /api/discovery/probe?nonce=<hex>
/// when an authenticating `secret` is provided, or falling back to unauthenticated /api/status.
pub async fn probe_http_candidate_with_secret(
    host: &str,
    port: u16,
    timeout: Duration,
    secret: Option<&ClusterSecret>,
) -> Option<MasterBeacon> {
    let Some(secret) = secret else {
        return probe_http_candidate(host, port, timeout).await;
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    if !is_valid_host(host) {
        return None;
    }
    let addr_str = format!("{}:{}", host, port);
    let connect_future = TcpStream::connect(&addr_str);
    let mut stream = tokio::time::timeout(timeout, connect_future)
        .await
        .ok()?
        .ok()?;
    let _ = stream.set_nodelay(true);

    let resolved_peer = stream.peer_addr().ok()?;

    let mut nonce = [0u8; DISCOVERY_PROBE_NONCE_LEN];
    random_bytes(&mut nonce).ok()?;
    let nonce_hex = hex_encode(&nonce);

    let req = format!(
        "GET /api/discovery/probe?nonce={} HTTP/1.1\r\nHost: {}\r\nUser-Agent: OxideSwarm-Discovery/1.0\r\nConnection: close\r\n\r\n",
        nonce_hex, host
    );
    tokio::time::timeout(timeout, stream.write_all(req.as_bytes()))
        .await
        .ok()?
        .ok()?;

    let mut buf = Vec::with_capacity(2048);
    let mut temp = [0u8; 1024];
    let read_future = async {
        while let Ok(n) = stream.read(&mut temp).await {
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&temp[..n]);
            if buf.len() > 16384 {
                break;
            }
            if let Some(idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                if serde_json::from_slice::<MasterBeacon>(&buf[idx + 4..]).is_ok() {
                    break;
                }
            }
        }
    };
    let _ = tokio::time::timeout(timeout, read_future).await;

    let resp_str = String::from_utf8_lossy(&buf);
    if !resp_str.starts_with("HTTP/1.1 200") && !resp_str.starts_with("HTTP/1.0 200") {
        return None;
    }
    let json_start = resp_str.find("\r\n\r\n").map(|i| i + 4)?;
    let beacon: MasterBeacon = serde_json::from_str(&resp_str[json_start..]).ok()?;

    if !beacon.verify(secret, &nonce) {
        tracing::debug!(
            host,
            "HTTP probe response failed HMAC-SHA256 nonce verification"
        );
        return None;
    }

    accept_verified_beacon(beacon, resolved_peer)
}

/// Probes the local machine and the configured candidate hosts
/// ([`DISCOVERY_CANDIDATES_ENV`]) in parallel via HTTP, verifying the beacon with `secret` if given.
pub async fn probe_http_candidates_with_secret(
    timeout: Duration,
    web_ui_port: u16,
    secret: Option<&ClusterSecret>,
) -> Option<MasterBeacon> {
    let mut candidates = configured_candidate_hosts();
    if let Some(gw) = get_default_gateway_candidate() {
        candidates.push(gw.to_string());
    }
    candidates.push("127.0.0.1".to_string());
    candidates.sort();
    candidates.dedup();

    let mut set = tokio::task::JoinSet::new();
    for target in candidates {
        let t = timeout;
        let (host, p) = if let Some((h, port_num)) = split_host_port(&target) {
            (h.to_string(), port_num)
        } else {
            (target.clone(), web_ui_port)
        };
        let sec = secret.cloned();
        set.spawn(async move { probe_http_candidate_with_secret(&host, p, t, sec.as_ref()).await });
    }

    while let Some(res) = set.join_next().await {
        if let Ok(Some(beacon)) = res {
            return Some(beacon);
        }
    }

    None
}

/// Probes the local machine and the configured candidate hosts
/// ([`DISCOVERY_CANDIDATES_ENV`]) in parallel via HTTP.
pub async fn probe_http_candidates(timeout: Duration, web_ui_port: u16) -> Option<MasterBeacon> {
    probe_http_candidates_with_secret(timeout, web_ui_port, None).await
}

/// Parses an HTTP/HTTPS URL into (host, port, path_prefix).
fn parse_http_url(url_str: &str) -> Option<(String, u16, String)> {
    let stripped = url_str
        .trim()
        .strip_prefix("http://")
        .or_else(|| url_str.trim().strip_prefix("https://"))?;
    let (authority, path_prefix) = match stripped.split_once('/') {
        Some((auth, path)) => (auth, format!("/{}", path.trim_end_matches('/'))),
        None => (stripped, String::new()),
    };
    let (host, port) = if authority.starts_with('[') {
        let closing = authority.find(']')?;
        let host = &authority[1..closing];
        let rest = &authority[closing + 1..];
        let port = if let Some(p) = rest.strip_prefix(':') {
            p.parse::<u16>().ok()?
        } else {
            80
        };
        (host.to_string(), port)
    } else if let Some((h, p)) = authority.split_once(':') {
        (h.to_string(), p.parse::<u16>().ok()?)
    } else {
        (authority.to_string(), 80)
    };
    Some((host, port, path_prefix))
}

/// Discovers an active Master node by querying a signed rendezvous lease from the specified URL.
pub async fn discover_master_rendezvous(
    rendezvous_url: &str,
    secret: Option<&ClusterSecret>,
    timeout: Duration,
) -> Option<MasterBeacon> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let (host, port, path_prefix) = parse_http_url(rendezvous_url)?;
    let addr_str = format!("{}:{}", host, port);
    let connect_future = TcpStream::connect(&addr_str);
    let mut stream = tokio::time::timeout(timeout, connect_future)
        .await
        .ok()?
        .ok()?;
    let _ = stream.set_nodelay(true);

    let path = format!(
        "{}/api/rendezvous/master",
        path_prefix.trim_end_matches('/')
    );
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: OxideSwarm-Discovery/1.0\r\nConnection: close\r\n\r\n",
        path, host
    );
    tokio::time::timeout(timeout, stream.write_all(req.as_bytes()))
        .await
        .ok()?
        .ok()?;

    let mut buf = Vec::with_capacity(2048);
    let mut temp = [0u8; 1024];
    let read_future = async {
        while let Ok(n) = stream.read(&mut temp).await {
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&temp[..n]);
            if buf.len() > 16384 {
                break;
            }
            if let Some(idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                if serde_json::from_slice::<RendezvousLease>(&buf[idx + 4..]).is_ok() {
                    break;
                }
            }
        }
    };
    let _ = tokio::time::timeout(timeout, read_future).await;

    let resp_str = String::from_utf8_lossy(&buf);
    if !resp_str.starts_with("HTTP/1.1 200") && !resp_str.starts_with("HTTP/1.0 200") {
        return None;
    }
    let json_start = resp_str.find("\r\n\r\n").map(|i| i + 4)?;
    let lease: RendezvousLease = serde_json::from_str(&resp_str[json_start..]).ok()?;

    if let Some(secret) = secret {
        if !lease.verify(secret, 30) {
            tracing::debug!("Rendezvous lease failed HMAC-SHA256 signature verification");
            return None;
        }
    }

    Some(lease.to_beacon())
}

/// Registers a master's signed RendezvousLease with the rendezvous registry at `rendezvous_url`.
pub async fn register_rendezvous_lease(
    rendezvous_url: &str,
    lease: &RendezvousLease,
) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let (host, port, path_prefix) = parse_http_url(rendezvous_url)
        .ok_or_else(|| format!("Invalid rendezvous URL: {rendezvous_url}"))?;
    let addr_str = format!("{}:{}", host, port);
    let timeout = Duration::from_secs(5);
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&addr_str))
        .await
        .map_err(|_| "Connection to rendezvous registry timed out".to_string())?
        .map_err(|e| format!("Failed to connect to rendezvous registry at {addr_str}: {e}"))?;
    let _ = stream.set_nodelay(true);

    let path = format!(
        "{}/api/rendezvous/register",
        path_prefix.trim_end_matches('/')
    );
    let body = serde_json::to_string(lease)
        .map_err(|e| format!("Failed to serialize rendezvous lease: {e}"))?;
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path,
        host,
        body.len(),
        body
    );
    tokio::time::timeout(timeout, stream.write_all(req.as_bytes()))
        .await
        .map_err(|_| "Writing to rendezvous registry timed out".to_string())?
        .map_err(|e| format!("Failed to write to rendezvous registry: {e}"))?;

    let mut buf = Vec::with_capacity(1024);
    let mut temp = [0u8; 512];
    let _ = tokio::time::timeout(timeout, async {
        while let Ok(n) = stream.read(&mut temp).await {
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&temp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
    })
    .await;

    let resp_str = String::from_utf8_lossy(&buf);
    if resp_str.starts_with("HTTP/1.1 200") || resp_str.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        Err(format!(
            "Rendezvous registry returned non-200 response: {resp_str}"
        ))
    }
}

/// Discovers an active Master node using UDP broadcast and unicast probing (no cluster secret:
/// any beacon is accepted). Prefer [`discover_master_udp_with_secret`] when a secret exists.
pub async fn discover_master_udp(timeout: Duration, port: u16) -> Option<MasterBeacon> {
    discover_master_udp_with_secret(timeout, port, None).await
}

/// UDP discovery that authenticates the master when a `secret` is supplied.
///
/// With a secret, probes carry a fresh nonce and only beacons whose MAC verifies for that nonce
/// are accepted (unsigned/old/forged beacons are ignored); the returned `cluster_addr` is the
/// signed one (see [`accept_verified_beacon`]), so relaying a genuine beacon from another host
/// cannot redirect the caller. Without a secret, plain probes are sent and every well-formed
/// beacon is accepted, as before, with `cluster_addr` set to the beacon's UDP source IP plus its
/// advertised port (see [`normalize_beacon_source`]).
pub async fn discover_master_udp_with_secret(
    timeout: Duration,
    port: u16,
    secret: Option<&ClusterSecret>,
) -> Option<MasterBeacon> {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.ok()?;
    let _ = socket.set_broadcast(true);

    let broadcast_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), port);
    let subnet_broadcast = SocketAddr::new(get_subnet_broadcast_ip(), port);
    let loopback_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    // Target global broadcast, subnet-directed broadcast, loopback, and any configured candidate
    // hosts (unicast reaches a master behind an access point that drops broadcasts).
    let mut candidate_targets = vec![broadcast_target, subnet_broadcast, loopback_target];
    candidate_targets.extend(
        configured_candidate_hosts()
            .iter()
            .filter_map(|h| h.parse::<IpAddr>().ok())
            .map(|ip| SocketAddr::new(ip, port)),
    );

    // One nonce per discovery call: every probe below re-sends it, and only beacons bound to it
    // are accepted, so a beacon recorded from an earlier discovery is worthless.
    let mut nonce = [0u8; DISCOVERY_PROBE_NONCE_LEN];
    let probe = match secret {
        Some(_) => {
            random_bytes(&mut nonce).ok()?;
            build_auth_probe(&nonce)
        }
        None => DISCOVERY_MAGIC_REQUEST.to_vec(),
    };

    let mut buf = [0u8; 2048];
    let start = tokio::time::Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(300));

    loop {
        if start.elapsed() >= timeout {
            break;
        }

        tokio::select! {
            _ = interval.tick() => {
                for target in &candidate_targets {
                    let _ = socket.send_to(&probe, target).await;
                }
            }
            res = socket.recv_from(&mut buf) => {
                if let Ok((len, from)) = res {
                    let Some(beacon) = MasterBeacon::from_bytes(&buf[..len]) else { continue };
                    let accepted = match secret {
                        Some(secret) => {
                            if !beacon.verify(secret, &nonce) {
                                tracing::debug!(%from, "ignoring discovery beacon with missing or invalid MAC");
                                continue;
                            }
                            accept_verified_beacon(beacon, from)
                        }
                        None => normalize_beacon_source(beacon, from),
                    };
                    match accepted {
                        Some(beacon) => return Some(beacon),
                        None => tracing::debug!(%from, "ignoring discovery beacon with unusable address"),
                    }
                }
            }
        }
    }

    None
}

/// Multi-tier Master discovery without a cluster secret (any beacon accepted).
/// See [`discover_master_with_secret`].
pub async fn discover_master(timeout: Duration, port: u16) -> Option<MasterBeacon> {
    discover_master_with_secret(timeout, port, None).await
}

/// Multi-tier Master discovery:
/// 1. Fast UDP broadcast + active probe (sub-5ms on LAN), authenticated when `secret` is given.
/// 2. If UDP fails (e.g. Wi-Fi AP isolation, cross-subnet), a fast parallel authenticated HTTP
///    probe (`GET /api/discovery/probe?nonce=<hex>`) against the local machine, default gateway,
///    and configured candidate hosts, verified via HMAC-SHA256 signature against the nonce.
/// 3. If candidates fail, an authenticated Rendezvous Registry lookup ([`RENDEZVOUS_ENV`])
///    querying a signed [`RendezvousLease`].
///
/// The TCP/QUIC handshake in [`crate::cluster_auth`] remains the final authority either way.
pub async fn discover_master_with_secret(
    timeout: Duration,
    port: u16,
    secret: Option<&ClusterSecret>,
) -> Option<MasterBeacon> {
    discover_master_tiered(timeout, port, secret, DEFAULT_WEB_UI_PORT).await
}

/// Port of the master's dashboard, probed by the HTTP fallback.
const DEFAULT_WEB_UI_PORT: u16 = 8080;

pub async fn discover_master_tiered(
    timeout: Duration,
    port: u16,
    secret: Option<&ClusterSecret>,
    web_ui_port: u16,
) -> Option<MasterBeacon> {
    let udp_timeout = timeout.min(Duration::from_millis(1500));
    if let Some(beacon) = discover_master_udp_with_secret(udp_timeout, port, secret).await {
        return Some(beacon);
    }

    let http_timeout = timeout
        .saturating_sub(udp_timeout)
        .max(Duration::from_millis(600));

    if let Some(beacon) = probe_http_candidates_with_secret(http_timeout, web_ui_port, secret).await
    {
        return Some(beacon);
    }

    // Tier 3: Rendezvous Registry Fallback (OXIDESWARM_RENDEZVOUS or RUSTY_GRID_RENDEZVOUS)
    let rendezvous_url = std::env::var(RENDEZVOUS_ENV)
        .or_else(|_| std::env::var("RUSTY_GRID_RENDEZVOUS"))
        .ok()
        .filter(|s| !s.trim().is_empty());
    if let Some(url) = rendezvous_url {
        if let Some(beacon) = discover_master_rendezvous(&url, secret, http_timeout).await {
            return Some(beacon);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_beacon_serialization_roundtrip() {
        let beacon = MasterBeacon::new(
            "192.168.1.144:8088",
            "http://192.168.1.144:8080",
            "MacBook-Pro",
            2,
        );
        let bytes = beacon.to_bytes().expect("Serialization must succeed");
        let parsed = MasterBeacon::from_bytes(&bytes).expect("Deserialization must succeed");

        assert_eq!(parsed.service, DISCOVERY_SERVICE_NAME);
        assert_eq!(parsed.cluster_addr, "192.168.1.144:8088");
        assert_eq!(parsed.web_ui_url, "http://192.168.1.144:8080");
        assert_eq!(parsed.hostname, "MacBook-Pro");
        assert_eq!(parsed.worker_count, 2);
    }

    #[test]
    fn test_beacon_invalid_signature() {
        let invalid_json = serde_json::json!({
            "service": "other-service",
            "version": 1,
            "cluster_addr": "127.0.0.1:8088",
            "web_ui_url": "http://127.0.0.1:8080",
            "hostname": "test",
            "worker_count": 0,
            "timestamp": 123456
        });
        let bytes = serde_json::to_vec(&invalid_json).unwrap();
        assert!(MasterBeacon::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_local_ip_detection() {
        let ip = get_local_ip_or_loopback();
        assert!(!ip.is_unspecified());
    }

    #[tokio::test]
    async fn test_udp_discovery_query_response() {
        // Bind an ephemeral UDP socket as a mock Master responder
        let server_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Server socket bind");
        let port = server_socket.local_addr().unwrap().port();

        let beacon = MasterBeacon::new(
            format!("127.0.0.1:{}", port + 10),
            format!("http://127.0.0.1:{}", port + 2),
            "Test-Master",
            1,
        );
        let beacon_bytes = beacon.to_bytes().unwrap();

        // Spawn mock responder
        let responder_task = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            if let Ok((len, from)) = server_socket.recv_from(&mut buf).await {
                if &buf[..len] == DISCOVERY_MAGIC_REQUEST {
                    let _ = server_socket.send_to(&beacon_bytes, from).await;
                }
            }
        });

        // Client discovers using ephemeral port
        let discovered = discover_master(Duration::from_millis(800), port).await;
        assert!(discovered.is_some(), "Client should discover Master beacon");
        let d = discovered.unwrap();
        assert_eq!(d.hostname, "Test-Master");
        assert_eq!(d.cluster_addr, format!("127.0.0.1:{}", port + 10));

        let _ = responder_task.await;
    }

    // --- Authenticated discovery ------------------------------------------------------------

    fn test_secret(seed: u8) -> ClusterSecret {
        ClusterSecret::from_bytes(&[seed; 32]).unwrap()
    }

    fn sample_beacon() -> MasterBeacon {
        MasterBeacon::new(
            "192.168.1.50:8088",
            "http://192.168.1.50:8080",
            "Master-Host",
            3,
        )
    }

    #[test]
    fn test_beacon_mac_sign_and_verify() {
        let secret = test_secret(1);
        let nonce = [7u8; DISCOVERY_PROBE_NONCE_LEN];
        let mut beacon = sample_beacon();
        assert!(
            !beacon.verify(&secret, &nonce),
            "unsigned beacon must not verify"
        );

        beacon.sign(&secret, &nonce);
        assert_eq!(beacon.mac.as_ref().unwrap().len(), 64);
        assert!(beacon.verify(&secret, &nonce));

        // Wrong secret / wrong probe nonce (replay of a recorded beacon).
        assert!(!beacon.verify(&test_secret(2), &nonce));
        assert!(!beacon.verify(&secret, &[8u8; DISCOVERY_PROBE_NONCE_LEN]));

        // Every field a client acts on is covered; cosmetic fields are not.
        let mut tampered = beacon.clone();
        tampered.cluster_addr = "192.168.1.66:8088".into();
        assert!(!tampered.verify(&secret, &nonce));
        let mut tampered = beacon.clone();
        tampered.web_ui_url = "http://evil.example".into();
        assert!(!tampered.verify(&secret, &nonce));
        let mut moved = beacon.clone();
        moved.cluster_addr = "192.168.1.50:808".into();
        moved.web_ui_url = "8http://192.168.1.50:8080".into();
        assert!(
            !moved.verify(&secret, &nonce),
            "field boundaries must be unambiguous"
        );
        let mut cosmetic = beacon.clone();
        cosmetic.worker_count = 99;
        assert!(cosmetic.verify(&secret, &nonce));

        // Malformed MAC text never verifies and never panics.
        for bad in ["", "zz", "abc", &"0".repeat(64), &"0".repeat(66)] {
            let mut b = beacon.clone();
            b.mac = Some(bad.to_string());
            assert!(!b.verify(&secret, &nonce), "mac {bad:?}");
        }
    }

    #[test]
    fn test_beacon_json_compat_with_and_without_mac() {
        // A beacon without a MAC serialises exactly as before (no "mac" key)...
        let plain = sample_beacon();
        let json: serde_json::Value = serde_json::from_slice(&plain.to_bytes().unwrap()).unwrap();
        assert!(json.get("mac").is_none());

        // ...and legacy JSON without the field still parses.
        let legacy = serde_json::json!({
            "service": DISCOVERY_SERVICE_NAME, "version": 1, "cluster_addr": "10.0.0.2:8088",
            "web_ui_url": "http://10.0.0.2:8080", "hostname": "old", "worker_count": 0, "timestamp": 1
        });
        let parsed = MasterBeacon::from_bytes(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert_eq!(parsed.mac, None);

        // A signed beacon round-trips with its MAC.
        let mut signed = sample_beacon();
        signed.sign(&test_secret(1), &[1u8; DISCOVERY_PROBE_NONCE_LEN]);
        let back = MasterBeacon::from_bytes(&signed.to_bytes().unwrap()).unwrap();
        assert_eq!(back.mac, signed.mac);
    }

    #[test]
    fn test_probe_build_and_parse() {
        assert_eq!(
            parse_probe(DISCOVERY_MAGIC_REQUEST),
            Some(DiscoveryProbe::Plain)
        );

        let nonce = [0xABu8; DISCOVERY_PROBE_NONCE_LEN];
        let probe = build_auth_probe(&nonce);
        assert_eq!(&probe[..12], b"OX_DISCOVER:");
        assert_eq!(probe.len(), 12 + 32);
        assert_eq!(
            parse_probe(&probe),
            Some(DiscoveryProbe::Authenticated { nonce })
        );

        for bad in [
            &b""[..],
            b"OX_DISCOVER ",
            b"OX_DISCOVER:",
            b"OX_DISCOVER:abcd",
            b"OX_DISCOVER:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            b"OX_DISCOVER:00000000000000000000000000000000ff",
            b"ox_discover",
            b"INVALID_QUERY_XYZ",
            b"OX_DISCOVER:\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8\xf7\xf6\xf5\xf4\xf3\xf2\xf1\xf0",
        ] {
            assert_eq!(parse_probe(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn test_beacon_reply_signs_only_authenticated_probes_with_a_secret() {
        let secret = test_secret(1);
        let nonce = [5u8; DISCOVERY_PROBE_NONCE_LEN];
        let reply_beacon = |datagram: &[u8], secret: Option<&ClusterSecret>| {
            beacon_reply(datagram, sample_beacon(), secret)
                .map(|bytes| MasterBeacon::from_bytes(&bytes).unwrap())
        };

        let signed = reply_beacon(&build_auth_probe(&nonce), Some(&secret)).unwrap();
        assert!(signed.verify(&secret, &nonce));
        assert!(!signed.verify(&secret, &[6u8; DISCOVERY_PROBE_NONCE_LEN]));

        // Plain probe to a master with a secret: answered, but unsigned.
        assert_eq!(
            reply_beacon(DISCOVERY_MAGIC_REQUEST, Some(&secret))
                .unwrap()
                .mac,
            None
        );
        // Authenticated probe to a master without a secret: unsigned.
        assert_eq!(
            reply_beacon(&build_auth_probe(&nonce), None).unwrap().mac,
            None
        );
        // Plain probe, no secret: legacy behaviour.
        assert_eq!(
            reply_beacon(DISCOVERY_MAGIC_REQUEST, None).unwrap().mac,
            None
        );
        // Noise is ignored.
        assert!(reply_beacon(b"INVALID_QUERY_XYZ", Some(&secret)).is_none());
        // A caller-supplied stale MAC is never forwarded on an unsigned reply.
        let mut stale = sample_beacon();
        stale.mac = Some("00".repeat(32));
        let bytes = beacon_reply(DISCOVERY_MAGIC_REQUEST, stale, None).unwrap();
        assert_eq!(MasterBeacon::from_bytes(&bytes).unwrap().mac, None);
    }

    fn lan(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 8089)
    }

    fn beacon_advertising(addr: &str) -> MasterBeacon {
        MasterBeacon::new(addr, "http://x:8080", "h", 0)
    }

    #[test]
    fn test_normalize_prefers_udp_source_ip_with_advertised_port() {
        // Lying about the IP does not redirect us: source IP + advertised port wins.
        let b = normalize_beacon_source(beacon_advertising("10.9.9.9:9000"), lan("192.168.1.50"))
            .unwrap();
        assert_eq!(b.cluster_addr, "192.168.1.50:9000");

        // Hostnames are replaced by the source IP too.
        let b = normalize_beacon_source(
            beacon_advertising("mymaster.local:8088"),
            lan("192.168.1.50"),
        )
        .unwrap();
        assert_eq!(b.cluster_addr, "192.168.1.50:8088");

        // IPv6 literal advertised, IPv4 sender.
        let b = normalize_beacon_source(beacon_advertising("[fd00::5]:8088"), lan("192.168.1.50"))
            .unwrap();
        assert_eq!(b.cluster_addr, "192.168.1.50:8088");

        // Local master replying from loopback while advertising its LAN address.
        let b = normalize_beacon_source(beacon_advertising("192.168.1.144:8088"), lan("127.0.0.1"))
            .unwrap();
        assert_eq!(b.cluster_addr, "127.0.0.1:8088");
    }

    #[test]
    fn test_normalize_rejects_unusable_advertised_addresses() {
        let remote = lan("192.168.1.50");
        for bad in [
            "127.0.0.1:8088", // points the worker at itself
            "127.5.5.5:8088",
            "[::1]:8088",
            "0.0.0.0:8088",
            "[::]:8088",
            "localhost:8088",
            "LOCALHOST:8088",
            "224.0.0.1:8088", // multicast
            "255.255.255.255:8088",
            "192.168.1.50:0", // no port
            "192.168.1.50",
            "192.168.1.50:",
            "192.168.1.50:99999",
            "192.168.1.50:http",
            ":8088",
            "",
        ] {
            assert!(
                normalize_beacon_source(beacon_advertising(bad), remote).is_none(),
                "advertised {bad:?} from a remote sender must be rejected"
            );
        }
        // Loopback sender may advertise loopback/localhost (single-machine setups).
        for ok in ["127.0.0.1:8088", "localhost:8088", "0.0.0.0:8088"] {
            let b = normalize_beacon_source(beacon_advertising(ok), lan("127.0.0.1")).unwrap();
            assert_eq!(b.cluster_addr, "127.0.0.1:8088");
        }
        // Multicast/broadcast are bogus even from loopback.
        assert!(
            normalize_beacon_source(beacon_advertising("224.0.0.1:8088"), lan("127.0.0.1"))
                .is_none()
        );
    }

    /// Spawns a UDP responder on 127.0.0.1 that answers every datagram with `respond(datagram)`.
    async fn spawn_responder(
        respond: impl Fn(&[u8]) -> Option<Vec<u8>> + Send + 'static,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok((len, from)) = socket.recv_from(&mut buf).await {
                if let Some(reply) = respond(&buf[..len]) {
                    let _ = socket.send_to(&reply, from).await;
                }
            }
        });
        (port, task)
    }

    #[tokio::test]
    async fn test_authenticated_discovery_accepts_signed_beacon_and_keeps_the_signed_address() {
        let secret = test_secret(1);
        let master_secret = secret.clone();
        let (port, responder) = spawn_responder(move |dg| {
            // The signed address is authoritative: it is the master's own claim, covered by the
            // MAC, and must not be replaced by the UDP source IP (here 127.0.0.1).
            let beacon = MasterBeacon::new(
                "10.99.99.99:5555",
                "http://10.99.99.99:8080",
                "Auth-Master",
                2,
            );
            beacon_reply(dg, beacon, Some(&master_secret))
        })
        .await;

        let found =
            discover_master_udp_with_secret(Duration::from_millis(1500), port, Some(&secret))
                .await
                .expect("signed beacon must be accepted");
        assert_eq!(found.hostname, "Auth-Master");
        assert_eq!(found.cluster_addr, "10.99.99.99:5555");
        responder.abort();
    }

    #[test]
    fn test_verified_beacon_keeps_the_signed_address_whoever_relays_it() {
        // A genuine beacon for a victim's nonce, forwarded by a different LAN host: the victim
        // must still be pointed at the master's signed address, never at the relaying host.
        let secret = test_secret(1);
        let nonce = [9u8; DISCOVERY_PROBE_NONCE_LEN];
        let mut beacon = sample_beacon(); // 192.168.1.50:8088
        beacon.sign(&secret, &nonce);
        assert!(beacon.verify(&secret, &nonce));

        let relayed = accept_verified_beacon(beacon.clone(), lan("192.168.1.66")).unwrap();
        assert_eq!(relayed.cluster_addr, "192.168.1.50:8088");
        assert!(
            relayed.verify(&secret, &nonce),
            "the beacon must come back untouched, MAC included"
        );

        // The unsigned path (peers without a secret) is the only one that follows the source.
        let followed = normalize_beacon_source(beacon, lan("192.168.1.66")).unwrap();
        assert_eq!(followed.cluster_addr, "192.168.1.66:8088");
        assert!(
            !followed.verify(&secret, &nonce),
            "rewriting the address invalidates the MAC, which is why verified beacons are not rewritten"
        );
    }

    #[test]
    fn test_verified_beacon_still_rejects_unusable_advertised_addresses() {
        let remote = lan("192.168.1.50");
        for bad in [
            "127.0.0.1:8088", // a relay pointing the victim at itself
            "[::1]:8088",
            "0.0.0.0:8088",
            "localhost:8088",
            "224.0.0.1:8088",
            "255.255.255.255:8088",
            "192.168.1.50:0",
            "192.168.1.50",
            "",
        ] {
            assert!(
                accept_verified_beacon(beacon_advertising(bad), remote).is_none(),
                "advertised {bad:?} from a remote sender must be rejected"
            );
        }
        // A local (loopback) sender may advertise loopback, or its LAN address: kept as signed.
        for ok in ["127.0.0.1:8088", "localhost:8088", "192.168.1.144:8088"] {
            let b = accept_verified_beacon(beacon_advertising(ok), lan("127.0.0.1")).unwrap();
            assert_eq!(b.cluster_addr, ok);
        }
        // A hostname advertised by a remote master is kept as signed too.
        let b = accept_verified_beacon(beacon_advertising("mymaster.local:8088"), remote).unwrap();
        assert_eq!(b.cluster_addr, "mymaster.local:8088");
    }

    #[tokio::test]
    async fn test_authenticated_discovery_rejects_wrong_secret_unsigned_and_replayed_beacons() {
        let ours = test_secret(1);

        // Master with a different secret.
        let other = test_secret(2);
        let (port, responder) = spawn_responder(move |dg| {
            beacon_reply(
                dg,
                MasterBeacon::new("127.0.0.1:5555", "http://127.0.0.1:1", "Rogue", 0),
                Some(&other),
            )
        })
        .await;
        assert!(
            discover_master_udp_with_secret(Duration::from_millis(700), port, Some(&ours))
                .await
                .is_none()
        );
        responder.abort();

        // Old/secret-less master answering with unsigned beacons.
        let (port, responder) = spawn_responder(|dg| {
            beacon_reply(
                dg,
                MasterBeacon::new("127.0.0.1:5555", "http://127.0.0.1:1", "Old", 0),
                None,
            )
        })
        .await;
        assert!(
            discover_master_udp_with_secret(Duration::from_millis(700), port, Some(&ours))
                .await
                .is_none()
        );
        responder.abort();

        // Attacker replaying a genuine signed beacon recorded for a previous probe nonce.
        let mut recorded = MasterBeacon::new("127.0.0.1:5555", "http://127.0.0.1:1", "Replay", 0);
        recorded.sign(&ours, &[0x42; DISCOVERY_PROBE_NONCE_LEN]);
        let replay_bytes = recorded.to_bytes().unwrap();
        let (port, responder) =
            spawn_responder(move |dg| parse_probe(dg).map(|_| replay_bytes.clone())).await;
        assert!(
            discover_master_udp_with_secret(Duration::from_millis(700), port, Some(&ours))
                .await
                .is_none()
        );
        responder.abort();

        // Control: a correctly signed beacon from a loopback sender advertising loopback (the
        // single-machine setup) is still accepted. (The remote-sender rejection of loopback
        // addresses is covered by test_normalize_rejects_unusable_advertised_addresses.)
        let secret = ours.clone();
        let (port, responder) = spawn_responder(move |dg| {
            beacon_reply(
                dg,
                MasterBeacon::new("127.0.0.1:5555", "http://127.0.0.1:1", "Local", 0),
                Some(&secret),
            )
        })
        .await;
        assert!(
            discover_master_udp_with_secret(Duration::from_millis(1500), port, Some(&ours))
                .await
                .is_some()
        );
        responder.abort();
    }

    #[tokio::test]
    async fn test_unauthenticated_discovery_still_accepts_signed_and_unsigned_beacons() {
        // A client without a secret sends the legacy probe and accepts whatever answers.
        let secret = test_secret(1);
        let (port, responder) = spawn_responder(move |dg| {
            beacon_reply(
                dg,
                MasterBeacon::new("127.0.0.1:5555", "http://127.0.0.1:1", "Open", 0),
                Some(&secret),
            )
        })
        .await;
        let found = discover_master_udp(Duration::from_millis(1500), port)
            .await
            .unwrap();
        assert_eq!(found.hostname, "Open");
        assert_eq!(found.mac, None, "plain probes get unsigned beacons");
        responder.abort();
    }

    #[tokio::test]
    async fn test_probe_http_candidate_active_master() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut req_buf = [0u8; 1024];
                let _ = stream.read(&mut req_buf).await;

                let json = serde_json::json!({
                    "master": {
                        "host": "MockMasterNode",
                        "role": "MASTER",
                        "description": "P2P Coordinator"
                    },
                    "workers": []
                })
                .to_string();

                let resp = format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    json.len(),
                    json
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.flush().await;
                let _ = stream.shutdown().await;
            }
        });

        let beacon = probe_http_candidate("127.0.0.1", port, Duration::from_millis(500)).await;
        assert!(beacon.is_some());
        let b = beacon.unwrap();
        assert_eq!(b.hostname, "MockMasterNode");
        assert_eq!(b.cluster_addr, "127.0.0.1:8088");
        assert_eq!(b.web_ui_url, format!("http://127.0.0.1:{}", port));
    }

    // --- Hard-coded candidates, HTTP fallback policy, strict URL / host validation ----------

    /// Serves `body` as the JSON of every `GET /api/status` on a fresh loopback port.
    async fn spawn_status_server(body: serde_json::Value) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let json = body.to_string();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut req = [0u8; 1024];
                let _ = stream.read(&mut req).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    json.len(),
                    json
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        port
    }

    /// A UDP port nothing listens on (bound and released), so UDP discovery finds nobody.
    async fn silent_udp_port() -> u16 {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn test_secret_holder_never_accepts_the_unauthenticated_http_fallback() {
        let web_port = spawn_status_server(serde_json::json!({
            "master": {"host": "ImpostorMaster", "role": "MASTER"},
            "workers": []
        }))
        .await;
        let udp_port = silent_udp_port().await;

        // Without a secret the HTTP fallback still finds a dashboard on loopback...
        let open = discover_master_tiered(Duration::from_millis(2500), udp_port, None, web_port)
            .await
            .expect("no-secret discovery keeps the HTTP fallback");
        assert_eq!(open.hostname, "ImpostorMaster");

        // ...but a secret holder cannot verify what an HTTP server says, so it gets nothing.
        let secret = test_secret(3);
        let strict = discover_master_tiered(
            Duration::from_millis(2500),
            udp_port,
            Some(&secret),
            web_port,
        )
        .await;
        assert!(
            strict.is_none(),
            "an unsigned HTTP answer must not be accepted when a cluster secret is configured, got {strict:?}"
        );
    }

    #[test]
    fn test_no_cluster_candidates_are_hard_coded() {
        // The module no longer ships site-specific addresses: with nothing configured the only
        // HTTP candidate is the local machine.
        let source = include_str!("discovery.rs");
        let code = source.split("#[cfg(test)]").next().unwrap();
        for ip in ["192.168.1.144", "192.168.1.123"] {
            // The module docs mention example beacons; only executable code matters.
            let in_code = code
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .any(|l| l.contains(ip));
            assert!(!in_code, "{ip} must not appear in discovery code");
        }
        assert_eq!(
            parse_candidate_hosts("10.0.0.5, nas.local  [::1] bad!host,;evil 10.0.0.6"),
            vec!["10.0.0.5", "nas.local", "10.0.0.6"]
        );
        let many: String = (0..40).map(|i| format!("10.0.0.{i},")).collect();
        assert_eq!(
            parse_candidate_hosts(&many).len(),
            MAX_CONFIGURED_CANDIDATES
        );
    }

    #[test]
    fn test_sanitize_http_origin_accepts_only_plain_http_origins() {
        for (input, expected) in [
            ("http://10.0.0.5:8080", "http://10.0.0.5:8080"),
            ("HTTP://Master.local", "http://Master.local"),
            ("https://master.example:443/", "https://master.example:443"),
            ("http://[fd00::5]:8080", "http://[fd00::5]:8080"),
            ("http://[::1]", "http://[::1]"),
            ("http://my_pc-1", "http://my_pc-1"),
        ] {
            assert_eq!(
                sanitize_http_origin(input).as_deref(),
                Some(expected),
                "{input:?}"
            );
        }
        for bad in [
            "",
            "javascript:alert(1)",
            "data:text/html,<script>alert(1)</script>",
            "ftp://10.0.0.5",
            "//10.0.0.5:8080",
            "10.0.0.5:8080",
            "http://",
            "http://:8080",
            "http://10.0.0.5:0",
            "http://10.0.0.5:99999",
            "http://10.0.0.5:80a",
            "http://10.0.0.5:8080/path",
            "http://10.0.0.5:8080/?q=1",
            "http://10.0.0.5:8080#frag",
            "http://user@10.0.0.5",
            "http://10.0.0.5\r\nSet-Cookie: x=1",
            "http://10.0.0.5\"><script>alert(1)</script>",
            "http://10.0.0.5' onmouseover='x",
            "http://evil.example\\@10.0.0.5",
            "http://exa mple.com",
            "http://[not-an-ip]:80",
            "http://[fe80::1%eth0]",
            "http://-bad.example",
            "http://a..b",
        ] {
            assert_eq!(sanitize_http_origin(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn test_sanitize_host_port_accepts_only_plain_host_port() {
        assert_eq!(
            sanitize_host_port("10.0.0.5:8088").as_deref(),
            Some("10.0.0.5:8088")
        );
        assert_eq!(
            sanitize_host_port("master.local:8088").as_deref(),
            Some("master.local:8088")
        );
        assert_eq!(
            sanitize_host_port("[fd00::5]:8088").as_deref(),
            Some("[fd00::5]:8088")
        );
        for bad in [
            "",
            "10.0.0.5",
            "10.0.0.5:0",
            ":8088",
            "a b:8088",
            "10.0.0.5:8088\r\nX: y",
            "evil\"host:8088",
            "<script>:8088",
        ] {
            assert_eq!(sanitize_host_port(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn test_beacon_with_a_hostile_cluster_addr_is_ignored() {
        let remote = lan("192.168.1.50");
        for bad in [
            "evil\r\nLocation: http://x:8088",
            "host with space:8088",
            "a\"b:8088",
            "<img src=x>:8088",
        ] {
            assert!(
                normalize_beacon_source(beacon_advertising(bad), remote).is_none(),
                "{bad:?}"
            );
            assert!(
                accept_verified_beacon(beacon_advertising(bad), remote).is_none(),
                "{bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_http_probe_does_not_follow_hostile_portal_redirects() {
        for redirect in [
            "javascript:alert(1)",
            "http://10.0.0.5/\"><script>alert(1)</script>",
            "http://10.0.0.5\r\nX-Injected: 1",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
        ] {
            let port = spawn_status_server(serde_json::json!({
                "role": "WORKER_PORTAL",
                "redirect_url": redirect,
                "master_cluster_addr": "10.0.0.5:8088"
            }))
            .await;
            let beacon = probe_http_candidate("127.0.0.1", port, Duration::from_millis(600)).await;
            assert!(
                beacon.is_none(),
                "redirect {redirect:?} must not become a beacon: {beacon:?}"
            );
        }

        // A well-formed portal answer is followed, with the validated forms of both fields.
        let port = spawn_status_server(serde_json::json!({
            "role": "WORKER_PORTAL",
            "redirect_url": "http://10.0.0.5:8080",
            "master_cluster_addr": "10.0.0.5:8088"
        }))
        .await;
        let beacon = probe_http_candidate("127.0.0.1", port, Duration::from_millis(600))
            .await
            .expect("valid portal answer");
        assert_eq!(beacon.web_ui_url, "http://10.0.0.5:8080");
        assert_eq!(beacon.cluster_addr, "10.0.0.5:8088");

        // A hostile master_cluster_addr is dropped in favour of host:8088 derived from the origin.
        let port = spawn_status_server(serde_json::json!({
            "role": "WORKER_PORTAL",
            "redirect_url": "http://10.0.0.5:8080",
            "master_cluster_addr": "10.0.0.5:8088\r\nX: y"
        }))
        .await;
        let beacon = probe_http_candidate("127.0.0.1", port, Duration::from_millis(600))
            .await
            .expect("valid redirect");
        assert_eq!(beacon.cluster_addr, "10.0.0.5:8088");
    }

    #[test]
    fn test_rendezvous_lease_signing_verification_and_tampering() {
        let secret = ClusterSecret::generate().unwrap();
        let other_secret = ClusterSecret::generate().unwrap();

        let mut lease = RendezvousLease::new(
            "192.168.1.100:8088",
            "http://192.168.1.100:8080",
            "host-alpha",
            Some("p2p-ticket-xyz".to_string()),
            3,
            60,
        );
        lease.sign(&secret);

        // Verification with correct secret and within TTL succeeds
        assert!(lease.verify(&secret, 30));

        // Verification with wrong secret fails
        assert!(!lease.verify(&other_secret, 30));

        // Tampering with cluster_addr invalidates signature
        let mut tampered = lease.clone();
        tampered.cluster_addr = "192.168.1.200:8088".to_string();
        assert!(!tampered.verify(&secret, 30));

        // Tampering with hostname invalidates signature
        let mut tampered_host = lease.clone();
        tampered_host.hostname = "host-evil".to_string();
        assert!(!tampered_host.verify(&secret, 30));

        // Expiry check
        let mut expired_lease = lease.clone();
        expired_lease.timestamp = chrono::Utc::now().timestamp() as u64 - 1000;
        expired_lease.sign(&secret);
        assert!(!expired_lease.verify(&secret, 30));

        // Conversion to beacon
        let beacon = lease.to_beacon();
        assert_eq!(beacon.cluster_addr, "192.168.1.100:8088");
        assert_eq!(beacon.web_ui_url, "http://192.168.1.100:8080");
        assert_eq!(beacon.hostname, "host-alpha");
        assert_eq!(beacon.worker_count, 3);
    }

    #[test]
    fn test_candidate_hosts_with_ports() {
        let candidates = parse_candidate_hosts("127.0.0.1:8080, 10.0.0.1:9090  node1.lan:8088");
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0], "127.0.0.1:8080");
        assert_eq!(candidates[1], "10.0.0.1:9090");
        assert_eq!(candidates[2], "node1.lan:8088");
    }
}
