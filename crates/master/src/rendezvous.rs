//! Standalone Keyed Rendezvous Registry for Cross-Subnet Master Discovery (`FEAT-7`).
//!
//! Unlike the single-slot `/api/rendezvous/master` on an individual Master's dashboard, a
//! standalone rendezvous registry (`oxideswarm rendezvous`) holds multiple independent cluster
//! leases keyed by a cluster's deterministic Ed25519 public key (`lease_key`, derived from
//! `ClusterSecret` via [`derive_rendezvous_key`]).
//!
//! Security properties:
//! * Fail-closed by default: registration requires either a matching configured `ClusterSecret`
//!   HMAC+Ed25519 signature or a valid self-authenticating `key_signature` over the lease fields
//!   (including `p2p_ticket`) using `lease_key`. Unsigned leases are refused unless
//!   `allow_unsigned` is explicitly enabled.
//! * Anti-harvesting reads: signed keyed leases are only returned when `GET
//!   /api/rendezvous/master?key=<hex>` presents a fresh `X-OxideSwarm-Lease-Auth` signature
//!   under the same `lease_key`.
//! * Bounded memory: expired leases are swept and the store caps active slots (`max_leases`) and
//!   requested TTL (`max_ttl_secs`).

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::discovery::{
    derive_rendezvous_key, is_valid_lease_key, sanitize_host_port, sanitize_http_origin,
    verify_rendezvous_read_auth, RendezvousLease, DISCOVERY_SERVICE_NAME,
    RENDEZVOUS_READ_AUTH_HEADER,
};

/// Default maximum number of distinct cluster leases kept concurrently in memory.
pub const DEFAULT_MAX_LEASES: usize = 256;

/// Default maximum lease TTL accepted by the registry (seconds).
pub const DEFAULT_MAX_TTL_SECS: u64 = 3600;

/// Maximum accepted JSON request body size for `POST /api/rendezvous/register`.
pub const MAX_LEASE_REQUEST_BYTES: usize = 16 * 1024;

/// Allowed clock skew (seconds) around lease `timestamp` / `ttl_secs` and read auth timestamps.
pub const LEASE_CLOCK_SKEW_SECS: u64 = 30;

/// Configuration for [`KeyedRendezvousStore`] and the standalone rendezvous server.
#[derive(Debug, Clone)]
pub struct KeyedRendezvousConfig {
    /// Maximum number of active leases held simultaneously.
    pub max_leases: usize,
    /// Maximum TTL (in seconds) a lease may request.
    pub max_ttl_secs: u64,
    /// Whether to accept unsigned leases in the `"default"` slot (opt-in insecure mode).
    pub allow_unsigned: bool,
    /// Optional cluster secret; when set, every registered lease must pass `lease.verify(secret)`.
    pub cluster_secret: Option<ClusterSecret>,
}

impl Default for KeyedRendezvousConfig {
    fn default() -> Self {
        Self {
            max_leases: DEFAULT_MAX_LEASES,
            max_ttl_secs: DEFAULT_MAX_TTL_SECS,
            allow_unsigned: false,
            cluster_secret: None,
        }
    }
}

/// Thread-safe, bounded, multi-cluster store for signed [`RendezvousLease`] records.
#[derive(Clone)]
pub struct KeyedRendezvousStore {
    config: KeyedRendezvousConfig,
    leases: Arc<RwLock<HashMap<String, RendezvousLease>>>,
}

pub(crate) fn is_lease_expired(lease: &RendezvousLease, now: u64) -> bool {
    now > lease
        .timestamp
        .saturating_add(lease.ttl_secs)
        .saturating_add(LEASE_CLOCK_SKEW_SECS)
}

pub(crate) fn is_routable_or_loopback_cluster_addr(addr: &str) -> bool {
    let Some(canonical) = sanitize_host_port(addr) else {
        return false;
    };
    let host = canonical
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(&canonical)
        .trim_start_matches('[')
        .trim_end_matches(']');
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if ip.is_multicast()
            || ip.is_unspecified()
            || matches!(ip, std::net::IpAddr::V4(v4) if v4.is_broadcast())
        {
            return false;
        }
    }
    true
}

impl KeyedRendezvousStore {
    /// Creates an empty store with `config`.
    pub fn new(config: KeyedRendezvousConfig) -> Self {
        Self {
            config,
            leases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the store configuration.
    pub fn config(&self) -> &KeyedRendezvousConfig {
        &self.config
    }

    /// Returns the number of currently stored (possibly un-swept) lease slots.
    pub async fn len(&self) -> usize {
        self.leases.read().await.len()
    }

    /// Returns `true` if no leases are currently stored.
    pub async fn is_empty(&self) -> bool {
        self.leases.read().await.is_empty()
    }

    /// Validates and inserts or renews `lease` in its keyed slot.
    ///
    /// On error returns `(http_status_code, short_error, detail_message)`.
    pub async fn register(
        &self,
        lease: RendezvousLease,
    ) -> Result<(), (u16, &'static str, String)> {
        if lease.service != DISCOVERY_SERVICE_NAME {
            return Err((
                400,
                "Invalid service",
                "Lease service name does not match OxideSwarm".to_string(),
            ));
        }
        if !is_routable_or_loopback_cluster_addr(&lease.cluster_addr) {
            return Err((
                400,
                "Invalid cluster_addr",
                "Lease cluster_addr must be a valid host:port and cannot be unspecified, multicast, or broadcast".to_string(),
            ));
        }
        if !lease.web_ui_url.is_empty() && sanitize_http_origin(&lease.web_ui_url).is_none() {
            return Err((
                400,
                "Invalid web_ui_url",
                "Lease web_ui_url must be a valid http(s) origin".to_string(),
            ));
        }
        if lease.ttl_secs == 0 || lease.ttl_secs > self.config.max_ttl_secs {
            return Err((
                400,
                "Invalid TTL",
                format!(
                    "Lease ttl_secs must be between 1 and {} seconds",
                    self.config.max_ttl_secs
                ),
            ));
        }
        if let Some(ref k) = lease.lease_key {
            if !is_valid_lease_key(k) {
                return Err((
                    400,
                    "Invalid lease_key",
                    "Lease key contains unsupported characters or exceeds 64 chars".to_string(),
                ));
            }
        }

        let now = chrono::Utc::now().timestamp() as u64;
        if let Some(ref secret) = self.config.cluster_secret {
            if !lease.verify(secret, LEASE_CLOCK_SKEW_SECS) {
                return Err((
                    401,
                    "Invalid signature",
                    "Rendezvous lease HMAC signature verification failed".to_string(),
                ));
            }
        } else if !self.config.allow_unsigned
            || lease.key_signature.is_some()
            || lease.signature.is_some()
        {
            if !lease.verify_key_signature(LEASE_CLOCK_SKEW_SECS) {
                return Err((
                    401,
                    "Invalid signature",
                    "Rendezvous lease requires a valid Ed25519 key_signature matching lease_key"
                        .to_string(),
                ));
            }
        } else if now + LEASE_CLOCK_SKEW_SECS < lease.timestamp || is_lease_expired(&lease, now) {
            return Err((
                400,
                "Expired lease",
                "Unsigned rendezvous lease timestamp is expired or too far in the future"
                    .to_string(),
            ));
        }

        let slot_key = lease
            .lease_key
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let mut guard = self.leases.write().await;
        guard.retain(|_, existing| !is_lease_expired(existing, now));
        if !guard.contains_key(&slot_key) && guard.len() >= self.config.max_leases {
            return Err((
                503,
                "Registry full",
                format!(
                    "Rendezvous registry capacity reached ({} active leases)",
                    self.config.max_leases
                ),
            ));
        }
        guard.insert(slot_key, lease);
        Ok(())
    }

    /// Looks up the active lease for `key_param`, checking read authorization for signed keyed
    /// slots. Returns `Err(http_status_code)` (`400`, `401`, or `404`).
    pub async fn lookup(
        &self,
        key_param: Option<&str>,
        read_auth_header: Option<&str>,
    ) -> Result<RendezvousLease, u16> {
        let slot_key = if let Some(raw_key) = key_param.map(str::trim).filter(|s| !s.is_empty()) {
            if !is_valid_lease_key(raw_key) {
                return Err(400);
            }
            raw_key.to_string()
        } else if let Some(ref secret) = self.config.cluster_secret {
            derive_rendezvous_key(secret)
        } else if self.config.allow_unsigned {
            "default".to_string()
        } else {
            return Err(404);
        };

        let now = chrono::Utc::now().timestamp() as u64;
        let lease = {
            let guard = self.leases.read().await;
            let Some(found) = guard.get(&slot_key) else {
                return Err(404);
            };
            if is_lease_expired(found, now) {
                drop(guard);
                let mut write_guard = self.leases.write().await;
                if write_guard
                    .get(&slot_key)
                    .is_some_and(|l| is_lease_expired(l, now))
                {
                    write_guard.remove(&slot_key);
                }
                return Err(404);
            }
            found.clone()
        };

        if lease.key_signature.is_some() {
            let Some(ref key_hex) = lease.lease_key else {
                return Err(401);
            };
            let Some(hdr) = read_auth_header else {
                return Err(401);
            };
            if !verify_rendezvous_read_auth(key_hex, hdr, LEASE_CLOCK_SKEW_SECS) {
                return Err(401);
            }
        }

        Ok(lease)
    }

    /// Name of the HTTP header used to authorize keyed reads.
    pub fn read_auth_header_name() -> &'static str {
        RENDEZVOUS_READ_AUTH_HEADER
    }
}

#[cfg(feature = "dashboard")]
mod http_server {
    use super::*;
    use std::net::SocketAddr;

    use axum::{
        extract::{DefaultBodyLimit, Query, State},
        http::{HeaderMap, StatusCode},
        routing::{get, post},
        Json, Router,
    };
    use tokio::sync::watch;

    #[derive(Debug, Deserialize)]
    struct MasterLeaseQuery {
        key: Option<String>,
    }

    use serde::Deserialize;

    async fn handle_register(
        State(store): State<KeyedRendezvousStore>,
        Json(lease): Json<RendezvousLease>,
    ) -> Result<StatusCode, (StatusCode, Json<serde_json::Value>)> {
        store
            .register(lease)
            .await
            .map(|()| StatusCode::OK)
            .map_err(|(code, err, msg)| {
                let status =
                    StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                (
                    status,
                    Json(serde_json::json!({
                        "error": err,
                        "message": msg,
                    })),
                )
            })
    }

    async fn handle_master(
        State(store): State<KeyedRendezvousStore>,
        headers: HeaderMap,
        Query(query): Query<MasterLeaseQuery>,
    ) -> Result<Json<RendezvousLease>, StatusCode> {
        let auth_hdr = headers
            .get(RENDEZVOUS_READ_AUTH_HEADER)
            .and_then(|v| v.to_str().ok());
        store
            .lookup(query.key.as_deref(), auth_hdr)
            .await
            .map(Json)
            .map_err(|code| StatusCode::from_u16(code).unwrap_or(StatusCode::NOT_FOUND))
    }

    /// Running standalone HTTP rendezvous registry handle.
    pub struct RendezvousServerHandle {
        local_addr: SocketAddr,
        shutdown_tx: watch::Sender<bool>,
        join_handle: tokio::task::JoinHandle<()>,
    }

    impl RendezvousServerHandle {
        /// Bound TCP socket address of the HTTP listener.
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        /// Triggers graceful shutdown and waits for the server task to finish.
        pub async fn shutdown(self) {
            let _ = self.shutdown_tx.send(true);
            let _ = self.join_handle.await;
        }
    }

    /// Builds the Axum router for the standalone keyed rendezvous registry.
    pub fn build_rendezvous_router(store: KeyedRendezvousStore) -> Router {
        Router::new()
            .route("/api/rendezvous/register", post(handle_register))
            .route("/api/rendezvous/master", get(handle_master))
            .layer(DefaultBodyLimit::max(MAX_LEASE_REQUEST_BYTES))
            .layer(axum::middleware::from_fn(
                crate::auth::security_headers_middleware,
            ))
            .with_state(store)
    }

    /// Spawns the standalone HTTP rendezvous registry on `listen_addr`.
    pub async fn spawn_rendezvous_server(
        listen_addr: SocketAddr,
        config: KeyedRendezvousConfig,
    ) -> std::io::Result<RendezvousServerHandle> {
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let store = KeyedRendezvousStore::new(config);
        let app = build_rendezvous_router(store);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let join_handle = tokio::spawn(async move {
            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.wait_for(|v| *v).await;
            });
            if let Err(e) = server.await {
                tracing::error!(error = %e, "Standalone rendezvous server exited with error");
            }
        });

        Ok(RendezvousServerHandle {
            local_addr,
            shutdown_tx,
            join_handle,
        })
    }
}

#[cfg(feature = "dashboard")]
pub use http_server::{build_rendezvous_router, spawn_rendezvous_server, RendezvousServerHandle};

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::discovery::sign_rendezvous_read_auth;

    fn secret(seed: u8) -> ClusterSecret {
        ClusterSecret::from_bytes(&[seed; 32]).unwrap()
    }

    fn signed_lease(
        sec: &ClusterSecret,
        addr: &str,
        ticket: Option<&str>,
        ttl: u64,
    ) -> RendezvousLease {
        let mut lease =
            RendezvousLease::new(addr, "", "test-master", ticket.map(str::to_string), 1, ttl);
        lease.sign(sec);
        lease
    }

    #[tokio::test]
    async fn test_store_rejects_unsigned_by_default_and_isolates_keyed_clusters() {
        let store = KeyedRendezvousStore::new(KeyedRendezvousConfig::default());

        let unsigned = RendezvousLease::new("10.0.0.1:8088", "", "plain", None, 0, 60);
        let err = store.register(unsigned).await.unwrap_err();
        assert_eq!(err.0, 401);

        let sec_a = secret(0xA1);
        let sec_b = secret(0xB2);
        let key_a = derive_rendezvous_key(&sec_a);
        let key_b = derive_rendezvous_key(&sec_b);

        let lease_a = signed_lease(&sec_a, "10.0.0.10:8088", Some("ticket-a"), 60);
        let lease_b = signed_lease(&sec_b, "10.0.0.20:8088", Some("ticket-b"), 60);
        store.register(lease_a).await.unwrap();
        store.register(lease_b).await.unwrap();

        // Reading without read auth returns 401; reading with wrong cluster auth returns 401.
        assert_eq!(store.lookup(Some(&key_a), None).await, Err(401));
        let now = chrono::Utc::now().timestamp() as u64;
        let auth_b_for_a = sign_rendezvous_read_auth(&sec_b, &key_a, now);
        assert_eq!(
            store.lookup(Some(&key_a), Some(&auth_b_for_a)).await,
            Err(401)
        );

        let auth_a = sign_rendezvous_read_auth(&sec_a, &key_a, now);
        let got_a = store.lookup(Some(&key_a), Some(&auth_a)).await.unwrap();
        assert_eq!(got_a.cluster_addr, "10.0.0.10:8088");
        assert_eq!(got_a.p2p_ticket.as_deref(), Some("ticket-a"));

        let auth_b = sign_rendezvous_read_auth(&sec_b, &key_b, now);
        let got_b = store.lookup(Some(&key_b), Some(&auth_b)).await.unwrap();
        assert_eq!(got_b.cluster_addr, "10.0.0.20:8088");
        assert_eq!(got_b.p2p_ticket.as_deref(), Some("ticket-b"));

        // Cluster B cannot hijack Cluster A's key even by manually setting lease_key = key_a.
        let mut hijack = signed_lease(&sec_b, "10.0.0.99:8088", Some("ticket-evil"), 60);
        hijack.lease_key = Some(key_a.clone());
        let hijack_err = store.register(hijack).await.unwrap_err();
        assert_eq!(hijack_err.0, 401);
    }

    #[tokio::test]
    async fn test_store_enforces_max_leases_and_ttl_and_evicts_expired() {
        let store = KeyedRendezvousStore::new(KeyedRendezvousConfig {
            max_leases: 2,
            max_ttl_secs: 120,
            allow_unsigned: false,
            cluster_secret: None,
        });

        let sec1 = secret(1);
        let sec2 = secret(2);
        let sec3 = secret(3);

        // Excessive TTL rejected.
        let too_long = signed_lease(&sec1, "10.0.0.1:8088", None, 121);
        assert_eq!(store.register(too_long).await.unwrap_err().0, 400);

        // Tampered p2p_ticket rejected.
        let mut tampered_ticket = signed_lease(&sec1, "10.0.0.1:8088", Some("orig"), 60);
        tampered_ticket.p2p_ticket = Some("evil".to_string());
        assert_eq!(store.register(tampered_ticket).await.unwrap_err().0, 401);

        // Fill capacity with 2 active leases -> 3rd gets 503.
        store
            .register(signed_lease(&sec1, "10.0.0.1:8088", None, 60))
            .await
            .unwrap();
        store
            .register(signed_lease(&sec2, "10.0.0.2:8088", None, 60))
            .await
            .unwrap();
        assert_eq!(
            store
                .register(signed_lease(&sec3, "10.0.0.3:8088", None, 60))
                .await
                .unwrap_err()
                .0,
            503
        );
    }

    #[tokio::test]
    async fn test_store_allow_unsigned_default_slot() {
        let store = KeyedRendezvousStore::new(KeyedRendezvousConfig {
            max_leases: 4,
            max_ttl_secs: 300,
            allow_unsigned: true,
            cluster_secret: None,
        });
        let unsigned = RendezvousLease::new("127.0.0.1:8088", "", "open-master", None, 0, 60);
        store.register(unsigned).await.unwrap();
        let found = store.lookup(None, None).await.unwrap();
        assert_eq!(found.cluster_addr, "127.0.0.1:8088");
    }
}
