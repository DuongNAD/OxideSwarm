//! Master Node LAN Discovery Protocol & Beacon Utilities.
//!
//! Provides zero-configuration LAN discovery for OxideSwarm Master nodes
//! using UDP broadcast beacons and active discovery probes.

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Default UDP port for OxideSwarm discovery beacons and queries.
pub const DEFAULT_DISCOVERY_PORT: u16 = 8089;

/// Magic payload sent by clients to actively probe for Master nodes.
pub const DISCOVERY_MAGIC_REQUEST: &[u8] = b"OX_DISCOVER";

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
        }
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

/// Well-known candidate nodes in the OxideSwarm cluster (Mac & Case PC).
pub const KNOWN_CLUSTER_CANDIDATE_IPS: &[&str] = &["192.168.1.144", "192.168.1.123"];

/// Probes an individual candidate node via HTTP GET /api/status.
pub async fn probe_http_candidate(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Option<MasterBeacon> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

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

    // Case 2: Node is a Worker Redirection Portal pointing to Master
    if json.get("role").and_then(|r| r.as_str()) == Some("WORKER_PORTAL") {
        if let Some(redirect_url) = json.get("redirect_url").and_then(|u| u.as_str()) {
            if !redirect_url.is_empty()
                && !redirect_url.contains("127.0.0.1")
                && !redirect_url.contains("localhost")
            {
                if let Some(stripped) = redirect_url.strip_prefix("http://") {
                    let target_host = stripped.split(':').next().unwrap_or(stripped);
                    let cluster_addr = json
                        .get("master_cluster_addr")
                        .and_then(|a| a.as_str())
                        .filter(|a| !a.is_empty() && *a != "auto")
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| format!("{}:8088", target_host));
                    return Some(MasterBeacon::new(
                        cluster_addr,
                        redirect_url,
                        "DiscoveredViaPortal",
                        1,
                    ));
                }
            }
        }
    }

    None
}

/// Probes known cluster candidate nodes in parallel via HTTP.
pub async fn probe_http_candidates(timeout: Duration, web_ui_port: u16) -> Option<MasterBeacon> {
    let mut candidates: Vec<String> = KNOWN_CLUSTER_CANDIDATE_IPS
        .iter()
        .map(|s| s.to_string())
        .collect();

    candidates.push("127.0.0.1".to_string());

    let mut set = tokio::task::JoinSet::new();
    for host in candidates {
        let t = timeout;
        let p = web_ui_port;
        set.spawn(async move { probe_http_candidate(&host, p, t).await });
    }

    while let Some(res) = set.join_next().await {
        if let Ok(Some(beacon)) = res {
            return Some(beacon);
        }
    }

    None
}

/// Discovers an active Master node using UDP broadcast and unicast probing.
pub async fn discover_master_udp(timeout: Duration, port: u16) -> Option<MasterBeacon> {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.ok()?;
    let _ = socket.set_broadcast(true);

    let broadcast_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), port);
    let subnet_broadcast = SocketAddr::new(get_subnet_broadcast_ip(), port);
    let loopback_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    // Target global broadcast, subnet-directed broadcast, loopback, and known cluster candidate nodes
    let candidate_targets = [
        broadcast_target,
        subnet_broadcast,
        loopback_target,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 144)), port),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 123)), port),
    ];

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
                    let _ = socket.send_to(DISCOVERY_MAGIC_REQUEST, target).await;
                }
            }
            res = socket.recv_from(&mut buf) => {
                if let Ok((len, _from)) = res {
                    if let Some(beacon) = MasterBeacon::from_bytes(&buf[..len]) {
                        return Some(beacon);
                    }
                }
            }
        }
    }

    None
}

/// Multi-tier Master discovery:
/// 1. Fast UDP broadcast + active probe (sub-5ms on LAN).
/// 2. If UDP fails (e.g. Wi-Fi AP isolation), fast parallel HTTP fallback to cluster candidates.
pub async fn discover_master(timeout: Duration, port: u16) -> Option<MasterBeacon> {
    let udp_timeout = timeout.min(Duration::from_millis(1500));
    if let Some(beacon) = discover_master_udp(udp_timeout, port).await {
        return Some(beacon);
    }

    let http_timeout = timeout
        .saturating_sub(udp_timeout)
        .max(Duration::from_millis(600));
    probe_http_candidates(http_timeout, 8080).await
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
}
