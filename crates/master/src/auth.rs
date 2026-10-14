//! Zero-Trust Authentication and Role-Based Access Control (RBAC) for OxideSwarm Master.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use rusty_grid_core::ClusterSecret;
use serde::{Deserialize, Serialize};

/// Label under which the dashboard Admin token is derived from the cluster secret.
pub const DASHBOARD_ADMIN_TOKEN_LABEL: &str = "oxs1-dashboard-admin";

/// Dashboard Admin token derived from the cluster secret: `hex(HMAC-SHA256(secret, label))`.
///
/// The master registers it automatically when it has a secret, and `oxideswarm secret show
/// --dashboard-token` prints it, so the raw secret itself never has to be typed into a browser.
pub fn dashboard_admin_token(secret: &ClusterSecret) -> String {
    secret.derive_hex(DASHBOARD_ADMIN_TOKEN_LABEL)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthScope {
    Observer = 1,
    Worker = 2,
    Submitter = 3,
    Admin = 4,
}

impl AuthScope {
    /// Determines whether the current scope satisfies the required scope.
    pub fn allows(&self, required: AuthScope) -> bool {
        match required {
            AuthScope::Observer => true, // All authenticated scopes can observe
            AuthScope::Worker => matches!(self, AuthScope::Worker | AuthScope::Admin),
            AuthScope::Submitter => matches!(self, AuthScope::Submitter | AuthScope::Admin),
            AuthScope::Admin => matches!(self, AuthScope::Admin),
        }
    }
}

impl FromStr for AuthScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "observer" => Ok(AuthScope::Observer),
            "worker" => Ok(AuthScope::Worker),
            "submitter" => Ok(AuthScope::Submitter),
            "admin" => Ok(AuthScope::Admin),
            other => Err(format!("Unknown AuthScope: {other}")),
        }
    }
}

impl fmt::Display for AuthScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthScope::Observer => write!(f, "observer"),
            AuthScope::Worker => write!(f, "worker"),
            AuthScope::Submitter => write!(f, "submitter"),
            AuthScope::Admin => write!(f, "admin"),
        }
    }
}

/// Metadata and permissions associated with a static or dynamic API token.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiToken {
    pub token: String,
    pub name: String,
    pub scopes: HashSet<AuthScope>,
}

impl fmt::Debug for ApiToken {
    // The token is a credential: it must never end up in logs through `{:?}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiToken")
            .field("token", &"<redacted>")
            .field("name", &self.name)
            .field("scopes", &self.scopes)
            .finish()
    }
}

/// Master RBAC authentication configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthConfig {
    /// Whether authentication middleware is actively enforced.
    pub enabled: bool,
    /// Whether development mode is active (enables the loopback automatic Admin bypass).
    ///
    /// Off by default. Turned on explicitly with `--dev`, `OXIDE_DEV_MODE=1|true` or
    /// [`AuthConfig::dev`]; even then the bypass applies only to genuinely local, same-origin
    /// requests (see `dev_bypass_allowed`).
    pub dev_mode: bool,
    /// Whether requests originating from loopback (127.0.0.1 / ::1) bypass auth when no token is
    /// passed. Only consulted when `dev_mode` is also on.
    pub loopback_bypass: bool,
    /// Registered valid tokens and their associated scopes.
    pub tokens: HashMap<String, ApiToken>,
    /// Exact browser origins (e.g. `https://ops.example.com`) that may call the API cross-origin.
    /// Empty (the default) means same-origin only; `*` is never honoured.
    pub allowed_origins: Vec<String>,
}

impl fmt::Debug for AuthConfig {
    // Token strings are the keys of `tokens`: print only how many exist and their names.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&str> = self.tokens.values().map(|t| t.name.as_str()).collect();
        names.sort_unstable();
        f.debug_struct("AuthConfig")
            .field("enabled", &self.enabled)
            .field("dev_mode", &self.dev_mode)
            .field("loopback_bypass", &self.loopback_bypass)
            .field(
                "tokens",
                &format_args!("{} registered {:?}", names.len(), names),
            )
            .field("allowed_origins", &self.allowed_origins)
            .finish()
    }
}

/// `OXIDE_DEV_MODE` semantics: only an explicit `1` or `true` (any case) enables dev mode.
pub fn dev_mode_from_env(value: Option<&str>) -> bool {
    matches!(value, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

impl Default for AuthConfig {
    /// Secure by default: authentication enforced and no loopback bypass, unless the operator
    /// explicitly exported `OXIDE_DEV_MODE=1|true`.
    fn default() -> Self {
        Self {
            enabled: true,
            dev_mode: dev_mode_from_env(std::env::var("OXIDE_DEV_MODE").ok().as_deref()),
            loopback_bypass: true,
            tokens: HashMap::new(),
            allowed_origins: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// Creates a fresh default AuthConfig.
    pub fn new() -> Self {
        Self::default()
    }

    /// Development configuration: unauthenticated Admin for local same-origin requests.
    /// For tests and local experiments only; never use it for a reachable master.
    pub fn dev() -> Self {
        Self {
            enabled: true,
            dev_mode: true,
            loopback_bypass: true,
            tokens: HashMap::new(),
            allowed_origins: Vec::new(),
        }
    }

    /// Builder helper to register an API token with granted scopes.
    pub fn with_token(
        mut self,
        token: impl Into<String>,
        name: impl Into<String>,
        scopes: &[AuthScope],
    ) -> Self {
        self.add_token(token, name, scopes);
        self
    }

    /// Adds a token to an existing AuthConfig.
    pub fn add_token(
        &mut self,
        token: impl Into<String>,
        name: impl Into<String>,
        scopes: &[AuthScope],
    ) {
        let t_str = token.into();
        self.tokens.insert(
            t_str.clone(),
            ApiToken {
                token: t_str,
                name: name.into(),
                scopes: scopes.iter().copied().collect(),
            },
        );
    }

    /// Registers the Admin token derived from the cluster secret ([`dashboard_admin_token`]).
    pub fn with_cluster_secret(mut self, secret: &ClusterSecret) -> Self {
        self.add_token(
            dashboard_admin_token(secret),
            "cluster-secret",
            &[AuthScope::Admin],
        );
        self
    }

    /// Allows `origin` (exact match, e.g. `https://ops.example.com`) to call the API cross-origin.
    pub fn with_allowed_origin(mut self, origin: impl Into<String>) -> Self {
        self.allowed_origins.push(origin.into());
        self
    }
}

/// Whether unauthenticated requests from this machine are trusted as a local developer, once the
/// dashboard listener's address is known (see [`resolve_dashboard_auth`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevBypass {
    /// Off: every request needs a token.
    Off,
    /// On because `--dev` / `OXIDE_DEV_MODE` asked for it, on a loopback listener.
    Explicit,
    /// On automatically: the master has no cluster secret and no token, and the dashboard
    /// listener is loopback-only, so the local dashboard works without any setup.
    Implicit,
    /// `--dev` / `OXIDE_DEV_MODE` was requested but the listener is not bound to a loopback
    /// address, so the request was refused: anyone who can reach that interface would be Admin.
    RefusedNonLoopback,
}

/// Decides the dashboard's final [`AuthConfig`] from the address its listener is actually bound to.
///
/// * Listener on a non-loopback address: the dev bypass is switched **off**, whatever was
///   requested (`DevBypass::RefusedNonLoopback` reports a refused request).
/// * Listener on a loopback address: an explicit `dev_mode` stays on. Otherwise, when
///   `implicit_local` is set, there is no cluster secret and no token, and `loopback_bypass` is
///   allowed, dev mode is enabled automatically (`DevBypass::Implicit`). Everything else stays
///   locked.
///
/// Even when on, the bypass still only applies to loopback peers with loopback `Host`/`Origin`,
/// no proxy headers and no cross-site fetch metadata (see `dev_bypass_allowed`).
pub fn resolve_dashboard_auth(
    mut auth: AuthConfig,
    has_secret: bool,
    implicit_local: bool,
    listener: SocketAddr,
) -> (AuthConfig, DevBypass) {
    if !auth.enabled {
        return (auth, DevBypass::Off);
    }
    if !listener.ip().to_canonical().is_loopback() {
        let refused = auth.dev_mode;
        auth.dev_mode = false;
        let state = if refused {
            DevBypass::RefusedNonLoopback
        } else {
            DevBypass::Off
        };
        return (auth, state);
    }
    if auth.dev_mode {
        let state = if auth.loopback_bypass {
            DevBypass::Explicit
        } else {
            DevBypass::Off
        };
        return (auth, state);
    }
    if implicit_local && !has_secret && auth.tokens.is_empty() && auth.loopback_bypass {
        auth.dev_mode = true;
        return (auth, DevBypass::Implicit);
    }
    (auth, DevBypass::Off)
}

/// Looks `presented` up among the registered tokens without leaking, through timing, how much of
/// it was right.
///
/// Each registered token is reduced to a fixed-length HMAC-SHA256 tag and the presented token is
/// checked against that tag with `ring::hmac::verify` (constant-time comparison). Every registered
/// token is visited, so neither a matching prefix nor the position of the match shows in the
/// response time. The key is a public constant: it only normalises the lengths, the tokens
/// themselves are the secrets.
pub fn find_token<'a>(config: &'a AuthConfig, presented: &str) -> Option<&'a ApiToken> {
    use ring::hmac;

    let key = hmac::Key::new(hmac::HMAC_SHA256, b"oxs1-dashboard-token-compare");
    let mut found = None;
    for (stored, entry) in &config.tokens {
        let stored_tag = hmac::sign(&key, stored.as_bytes());
        if hmac::verify(&key, presented.as_bytes(), stored_tag.as_ref()).is_ok() {
            found = Some(entry);
        }
    }
    found
}

/// Caller identity information stored in request extensions after successful authentication.
#[derive(Debug, Clone)]
pub struct AuthIdentity {
    pub name: String,
    pub scopes: HashSet<AuthScope>,
    pub is_bypassed: bool,
}

impl AuthIdentity {
    pub fn has_scope(&self, required: AuthScope) -> bool {
        self.scopes.iter().any(|s| s.allows(required))
    }
}

#[cfg(feature = "dashboard")]
pub use middleware::*;

#[cfg(feature = "dashboard")]
mod middleware {
    use super::*;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Arc;

    use axum::{
        extract::{ConnectInfo, Request, State},
        http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
        middleware::Next,
        response::{IntoResponse, Response},
        Json,
    };
    use tracing::{debug, warn};

    /// Extracts token from `Authorization: Bearer <token>`, `X-API-Key: <key>`, or URL query `?token=<token>`.
    ///
    /// The query form exists because browsers cannot set headers on WebSocket/EventSource
    /// requests; the dashboard sends `Referrer-Policy: no-referrer` so it does not leak.
    pub fn extract_credential(headers: &HeaderMap, query: Option<&str>) -> Option<String> {
        if let Some(auth_val) = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        {
            if let Some(token) = auth_val
                .strip_prefix("Bearer ")
                .or_else(|| auth_val.strip_prefix("bearer "))
            {
                let trimmed = token.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }

        if let Some(key_val) = headers
            .get("X-API-Key")
            .or_else(|| headers.get("x-api-key"))
            .and_then(|v| v.to_str().ok())
        {
            let trimmed = key_val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }

        if let Some(q) = query {
            for pair in q.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    if k == "token" || k == "api_key" {
                        let trimmed = v.trim();
                        if !trimmed.is_empty() {
                            return Some(trimmed.to_string());
                        }
                    }
                }
            }
        }

        None
    }

    /// Determines the required `AuthScope` for a given HTTP method and route path.
    pub fn route_required_scope(method: &Method, path: &str) -> Option<AuthScope> {
        match (method, path) {
            (&Method::GET, "/") => None, // Public SPA HTML (static, contains no cluster data)
            (&Method::GET, p)
                if p.starts_with("/api/status")
                    || p.starts_with("/api/workers")
                    || p.starts_with("/api/tasks")
                    || p.starts_with("/ws")
                    || p.starts_with("/api/stream")
                    || p.starts_with("/api/modes") =>
            {
                Some(AuthScope::Observer)
            }
            (&Method::POST, p) if p.starts_with("/api/tasks") || p.starts_with("/api/chat") => {
                Some(AuthScope::Submitter)
            }
            (&Method::POST, p) if p.starts_with("/api/modes/run") => Some(AuthScope::Admin),
            (&Method::GET, _) => Some(AuthScope::Observer),
            _ => Some(AuthScope::Admin),
        }
    }

    /// Host part of an authority (`host`, `host:port`, `[v6]`, `[v6]:port`).
    fn authority_host(authority: &str) -> &str {
        match authority.strip_prefix('[') {
            Some(rest) => rest.split(']').next().unwrap_or(""),
            None => authority
                .rsplit_once(':')
                .map(|(host, _port)| host)
                .unwrap_or(authority),
        }
    }

    /// True for `localhost` and for any loopback IP literal (including IPv4-mapped `::ffff:127.x`).
    fn is_loopback_host(host: &str) -> bool {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .map(|ip| ip.to_canonical().is_loopback())
                .unwrap_or(false)
    }

    /// True when any header suggests the request travelled through a proxy: `X-Forwarded-*`,
    /// `Forwarded` or `X-Real-IP`. A proxy on the same machine makes every client look like
    /// loopback, so such requests never get the dev bypass.
    fn has_forwarding_headers(headers: &HeaderMap) -> bool {
        headers.keys().any(|name| {
            let name = name.as_str();
            name.starts_with("x-forwarded-") || name == "forwarded" || name == "x-real-ip"
        })
    }

    /// True if `origin` is the plain-HTTP origin of exactly the authority the request was sent to
    /// and that authority is a loopback host, i.e. the page that issued the request was served by
    /// this very listener on a loopback address.
    fn is_same_loopback_origin(origin: &str, host_header: &str) -> bool {
        let Some(authority) = origin
            .get(..7)
            .filter(|scheme| scheme.eq_ignore_ascii_case("http://"))
            .map(|_| &origin[7..])
        else {
            return false;
        };
        if authority.is_empty() || authority.contains(['/', '?', '#', '@', ' ']) {
            return false;
        }
        is_loopback_host(authority_host(authority)) && authority.eq_ignore_ascii_case(host_header)
    }

    /// Decides whether an unauthenticated request may be treated as a local developer (Admin).
    ///
    /// Every condition must hold, otherwise the reason it failed is returned:
    /// dev mode and `loopback_bypass` are on; the TCP peer is a loopback address (a missing peer
    /// address fails closed); the `Host` header names a loopback host (defeats DNS rebinding); no
    /// proxy-forwarding header is present; and `Origin`, when present, is the same loopback origin
    /// (defeats drive-by requests from any website the developer happens to visit). Browsers that
    /// send `Sec-Fetch-Site` must additionally report `same-origin` or `none`.
    pub fn dev_bypass_allowed(
        config: &AuthConfig,
        peer: Option<SocketAddr>,
        headers: &HeaderMap,
        uri: &Uri,
    ) -> Result<(), &'static str> {
        if !(config.dev_mode && config.loopback_bypass) {
            return Err("dev mode is not enabled");
        }
        let Some(peer) = peer else {
            return Err("peer address unknown");
        };
        if !peer.ip().to_canonical().is_loopback() {
            return Err("peer is not a loopback address");
        }
        let host = headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .or_else(|| uri.authority().map(|a| a.as_str()));
        let Some(host) = host else {
            return Err("missing Host header");
        };
        if !is_loopback_host(authority_host(host)) {
            return Err("Host header is not a loopback name");
        }
        if has_forwarding_headers(headers) {
            return Err("proxy forwarding headers present");
        }
        let mut origins = headers.get_all(header::ORIGIN).iter();
        if let Some(origin) = origins.next() {
            if origins.next().is_some() {
                return Err("multiple Origin headers");
            }
            match origin.to_str() {
                Ok(origin) if is_same_loopback_origin(origin, host) => {}
                _ => return Err("cross-origin request"),
            }
        }
        if let Some(site) = headers.get("sec-fetch-site") {
            if !matches!(site.to_str(), Ok("same-origin") | Ok("none")) {
                return Err("browser marks the request as cross-site");
            }
        }
        Ok(())
    }

    fn unauthorized(message: &'static str, www_authenticate: &'static str) -> Response {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, www_authenticate)],
            Json(serde_json::json!({
                "error": "Unauthorized",
                "message": message,
            })),
        )
            .into_response()
    }

    /// Axum middleware function validating credentials and enforcing RBAC.
    ///
    /// Every route of the dashboard router sits behind this layer (REST, `/ws`, `/api/stream`,
    /// SSE, POSTs and the fallback); the only exempt request is `GET /` (the static SPA shell).
    pub async fn rbac_auth_middleware(
        State(config): State<Arc<AuthConfig>>,
        mut req: Request,
        next: Next,
    ) -> Response {
        if !config.enabled {
            return next.run(req).await;
        }

        let path = req.uri().path().to_string();
        let required_scope = match route_required_scope(req.method(), &path) {
            Some(scope) => scope,
            None => return next.run(req).await,
        };

        let peer_addr = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0);

        let Some(token_str) = extract_credential(req.headers(), req.uri().query()) else {
            // Dev bypass: only when no credential was presented at all.
            match dev_bypass_allowed(&config, peer_addr, req.headers(), req.uri()) {
                Ok(()) => {
                    debug!(
                        peer = ?peer_addr,
                        path = %path,
                        "Loopback dev-mode authentication bypass active (Admin granted)"
                    );
                    req.extensions_mut().insert(AuthIdentity {
                        name: "loopback-dev".to_string(),
                        scopes: [AuthScope::Admin].into_iter().collect(),
                        is_bypassed: true,
                    });
                    return next.run(req).await;
                }
                Err(reason) => {
                    debug!(
                        peer = ?peer_addr,
                        path = %path,
                        reason,
                        "Unauthorized request: missing authentication credentials"
                    );
                }
            }
            return unauthorized(
                "Missing authentication credential (Authorization Bearer, X-API-Key, or ?token=)",
                "Bearer realm=\"OxideSwarm\"",
            );
        };

        // Validate token against registered tokens (constant time; the token is never logged)
        if let Some(registered) = find_token(&config, &token_str) {
            let identity = AuthIdentity {
                name: registered.name.clone(),
                scopes: registered.scopes.clone(),
                is_bypassed: false,
            };

            if identity.has_scope(required_scope) {
                req.extensions_mut().insert(identity);
                return next.run(req).await;
            }
            warn!(
                name = %registered.name,
                token_scopes = ?registered.scopes,
                required = ?required_scope,
                "Forbidden request: insufficient permissions"
            );
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "Forbidden",
                    "message": format!("Token lacks required scope: {:?}", required_scope),
                    "required_scope": format!("{:?}", required_scope),
                })),
            )
                .into_response();
        }

        warn!(
            peer = ?peer_addr,
            path = %path,
            "Unauthorized request: invalid authentication token"
        );
        unauthorized(
            "Invalid Bearer token or API key",
            "Bearer realm=\"OxideSwarm\", error=\"invalid_token\"",
        )
    }

    /// Adds the standard hardening headers to every response, including 401/403/404 and the
    /// WebSocket upgrade: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` (the
    /// dashboard may not be framed) and `Referrer-Policy: no-referrer` (query-string tokens used by
    /// WebSocket/SSE clients must not leak through `Referer`).
    pub async fn security_headers_middleware(req: Request, next: Next) -> Response {
        let mut resp = next.run(req).await;
        let headers = resp.headers_mut();
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
        headers.insert(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        );
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_scope_hierarchy() {
        assert!(AuthScope::Admin.allows(AuthScope::Admin));
        assert!(AuthScope::Admin.allows(AuthScope::Submitter));
        assert!(AuthScope::Admin.allows(AuthScope::Worker));
        assert!(AuthScope::Admin.allows(AuthScope::Observer));

        assert!(!AuthScope::Submitter.allows(AuthScope::Admin));
        assert!(AuthScope::Submitter.allows(AuthScope::Submitter));
        assert!(!AuthScope::Submitter.allows(AuthScope::Worker));
        assert!(AuthScope::Submitter.allows(AuthScope::Observer));

        assert!(!AuthScope::Worker.allows(AuthScope::Admin));
        assert!(!AuthScope::Worker.allows(AuthScope::Submitter));
        assert!(AuthScope::Worker.allows(AuthScope::Worker));
        assert!(AuthScope::Worker.allows(AuthScope::Observer));

        assert!(!AuthScope::Observer.allows(AuthScope::Admin));
        assert!(!AuthScope::Observer.allows(AuthScope::Submitter));
        assert!(!AuthScope::Observer.allows(AuthScope::Worker));
        assert!(AuthScope::Observer.allows(AuthScope::Observer));
    }

    #[test]
    fn test_auth_scope_parsing() {
        assert_eq!("admin".parse::<AuthScope>().unwrap(), AuthScope::Admin);
        assert_eq!(
            "submitter".parse::<AuthScope>().unwrap(),
            AuthScope::Submitter
        );
        assert_eq!("worker".parse::<AuthScope>().unwrap(), AuthScope::Worker);
        assert_eq!(
            "observer".parse::<AuthScope>().unwrap(),
            AuthScope::Observer
        );
        assert!("unknown".parse::<AuthScope>().is_err());
    }

    #[test]
    fn test_auth_config_builder() {
        let config = AuthConfig::new()
            .with_token("token-obs", "observer-app", &[AuthScope::Observer])
            .with_token("token-adm", "admin-cli", &[AuthScope::Admin]);

        assert_eq!(config.tokens.len(), 2);
        let obs = config.tokens.get("token-obs").unwrap();
        assert_eq!(obs.name, "observer-app");
        assert!(obs.scopes.contains(&AuthScope::Observer));

        let adm = config.tokens.get("token-adm").unwrap();
        assert_eq!(adm.name, "admin-cli");
        assert!(adm.scopes.contains(&AuthScope::Admin));
    }

    #[cfg(feature = "dashboard")]
    #[test]
    fn test_extract_credential_sources() {
        use axum::http::HeaderMap;

        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer test-bearer-123".parse().unwrap());
        assert_eq!(
            extract_credential(&headers, None),
            Some("test-bearer-123".to_string())
        );

        let mut headers_api_key = HeaderMap::new();
        headers_api_key.insert("X-API-Key", "test-api-key-456".parse().unwrap());
        assert_eq!(
            extract_credential(&headers_api_key, None),
            Some("test-api-key-456".to_string())
        );

        let empty_headers = HeaderMap::new();
        assert_eq!(
            extract_credential(&empty_headers, Some("token=query-token-789&foo=bar")),
            Some("query-token-789".to_string())
        );
        assert_eq!(
            extract_credential(&empty_headers, Some("foo=bar&api_key=query-api-key-012")),
            Some("query-api-key-012".to_string())
        );
        assert_eq!(extract_credential(&empty_headers, None), None);
    }

    #[test]
    fn test_default_auth_config_is_secure() {
        // Only an explicit OXIDE_DEV_MODE=1|true may turn the bypass on.
        assert!(!dev_mode_from_env(None));
        assert!(!dev_mode_from_env(Some("")));
        assert!(!dev_mode_from_env(Some("0")));
        assert!(!dev_mode_from_env(Some("false")));
        assert!(!dev_mode_from_env(Some("yes")));
        assert!(dev_mode_from_env(Some("1")));
        assert!(dev_mode_from_env(Some("true")));
        assert!(dev_mode_from_env(Some("TRUE")));

        let default = AuthConfig::default();
        assert!(default.enabled);
        assert_eq!(
            default.dev_mode,
            dev_mode_from_env(std::env::var("OXIDE_DEV_MODE").ok().as_deref())
        );
        assert!(default.tokens.is_empty());
        assert!(default.allowed_origins.is_empty());
    }

    #[test]
    fn test_dev_helper_opts_in_explicitly() {
        let dev = AuthConfig::dev();
        assert!(dev.enabled && dev.dev_mode && dev.loopback_bypass);
        assert!(dev.tokens.is_empty());
    }

    #[test]
    fn test_auth_config_debug_never_prints_tokens() {
        let config =
            AuthConfig::dev().with_token("super-secret-token-value", "ci-bot", &[AuthScope::Admin]);
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-token-value"), "{rendered}");
        assert!(rendered.contains("ci-bot"));
        let token = config.tokens.values().next().unwrap();
        assert!(!format!("{token:?}").contains("super-secret-token-value"));
    }

    #[test]
    fn test_find_token_matches_exactly() {
        let config = AuthConfig::new()
            .with_token("token-aaaa", "a", &[AuthScope::Observer])
            .with_token("token-aaaa-longer", "b", &[AuthScope::Admin]);
        assert_eq!(find_token(&config, "token-aaaa").unwrap().name, "a");
        assert_eq!(find_token(&config, "token-aaaa-longer").unwrap().name, "b");
        assert!(find_token(&config, "token-aaa").is_none(), "prefix");
        assert!(find_token(&config, "token-aaaa-longe").is_none(), "prefix");
        assert!(
            find_token(&config, "token-aaaa-longer!").is_none(),
            "extension"
        );
        assert!(find_token(&config, "TOKEN-AAAA").is_none(), "case");
        assert!(find_token(&config, "").is_none());
        assert!(find_token(&AuthConfig::new(), "token-aaaa").is_none());
    }

    #[test]
    fn test_dashboard_admin_token_is_derived_not_the_secret() {
        let secret = ClusterSecret::from_bytes(&[7u8; 32]).unwrap();
        let other = ClusterSecret::from_bytes(&[8u8; 32]).unwrap();
        let token = dashboard_admin_token(&secret);

        assert_eq!(token.len(), 64);
        assert!(token.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(token, dashboard_admin_token(&secret), "deterministic");
        assert_ne!(token, dashboard_admin_token(&other));
        assert_ne!(
            token,
            secret.to_hex(),
            "the raw secret must never be the token"
        );

        let config = AuthConfig::new().with_cluster_secret(&secret);
        let entry = find_token(&config, &token).expect("derived token registered");
        assert!(entry.scopes.contains(&AuthScope::Admin));
    }

    #[cfg(feature = "dashboard")]
    mod dashboard_gate {
        use super::*;
        use axum::http::{HeaderMap, HeaderValue, Method, Uri};
        use std::net::SocketAddr;

        fn peer(addr: &str) -> Option<SocketAddr> {
            Some(addr.parse().unwrap())
        }

        fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
            let mut map = HeaderMap::new();
            for (name, value) in pairs {
                map.append(*name, HeaderValue::from_static(value));
            }
            map
        }

        fn uri() -> Uri {
            "/api/tasks".parse().unwrap()
        }

        fn allowed(
            config: &AuthConfig,
            peer_addr: Option<SocketAddr>,
            pairs: &[(&'static str, &'static str)],
        ) -> Result<(), &'static str> {
            dev_bypass_allowed(config, peer_addr, &headers(pairs), &uri())
        }

        #[test]
        fn test_dev_bypass_requires_dev_mode_and_loopback_bypass_flags() {
            let ok = [("host", "127.0.0.1:8080")];
            assert!(allowed(&AuthConfig::dev(), peer("127.0.0.1:5000"), &ok).is_ok());

            let mut no_dev = AuthConfig::dev();
            no_dev.dev_mode = false;
            assert!(allowed(&no_dev, peer("127.0.0.1:5000"), &ok).is_err());

            let mut no_loopback = AuthConfig::dev();
            no_loopback.loopback_bypass = false;
            assert!(allowed(&no_loopback, peer("127.0.0.1:5000"), &ok).is_err());
        }

        #[test]
        fn test_dev_bypass_requires_loopback_peer_and_fails_closed_without_one() {
            let ok = [("host", "localhost:8080")];
            let dev = AuthConfig::dev();
            assert!(allowed(&dev, peer("127.0.0.1:1"), &ok).is_ok());
            assert!(allowed(&dev, peer("127.8.9.10:1"), &ok).is_ok());
            assert!(allowed(&dev, peer("[::1]:1"), &ok).is_ok());
            assert!(
                allowed(&dev, peer("[::ffff:127.0.0.1]:1"), &ok).is_ok(),
                "IPv4-mapped loopback on a dual-stack socket is still loopback"
            );
            assert!(allowed(&dev, peer("192.168.1.20:1"), &ok).is_err());
            assert!(allowed(&dev, peer("[2001:db8::1]:1"), &ok).is_err());
            assert!(
                allowed(&dev, None, &ok).is_err(),
                "no ConnectInfo must not mean 'trusted'"
            );
        }

        #[test]
        fn test_dev_bypass_requires_loopback_host_header() {
            let dev = AuthConfig::dev();
            let p = peer("127.0.0.1:1");
            for good in [
                "localhost",
                "LOCALHOST:8080",
                "127.0.0.1",
                "127.0.0.1:8080",
                "[::1]:8080",
                "[::1]",
            ] {
                let h = [("host", good)];
                assert!(allowed(&dev, p, &h).is_ok(), "{good} should be accepted");
            }
            // DNS rebinding: the attacker's name resolves to 127.0.0.1 but the Host stays theirs.
            for bad in [
                "evil.example",
                "evil.example:8080",
                "localhost.evil.example",
                "127.0.0.1.evil.example",
                "192.168.1.5:8080",
                "0.0.0.0:8080",
                "user@127.0.0.1",
                "",
            ] {
                let h = [("host", bad)];
                assert!(allowed(&dev, p, &h).is_err(), "{bad:?} must be refused");
            }
            assert!(allowed(&dev, p, &[]).is_err(), "missing Host header");
        }

        #[test]
        fn test_dev_bypass_refused_behind_a_proxy() {
            let dev = AuthConfig::dev();
            let p = peer("127.0.0.1:1");
            for header in [
                "x-forwarded-for",
                "x-forwarded-host",
                "x-forwarded-proto",
                "forwarded",
                "x-real-ip",
            ] {
                let mut map = headers(&[("host", "127.0.0.1:8080")]);
                map.insert(header, HeaderValue::from_static("203.0.113.9"));
                assert!(
                    dev_bypass_allowed(&dev, p, &map, &uri()).is_err(),
                    "{header} must disable the bypass"
                );
            }
        }

        #[test]
        fn test_dev_bypass_origin_must_be_absent_or_the_same_loopback_origin() {
            let dev = AuthConfig::dev();
            let p = peer("127.0.0.1:1");
            let host = ("host", "127.0.0.1:8080");

            assert!(
                allowed(&dev, p, &[host]).is_ok(),
                "no Origin (curl, native clients)"
            );
            assert!(allowed(&dev, p, &[host, ("origin", "http://127.0.0.1:8080")]).is_ok());
            assert!(allowed(
                &dev,
                p,
                &[
                    ("host", "localhost:8080"),
                    ("origin", "http://localhost:8080")
                ]
            )
            .is_ok());

            for bad in [
                "https://evil.example",
                "http://evil.example",
                "http://127.0.0.1:9999",  // another local app on another port
                "http://localhost:8080",  // different name than the Host header
                "https://127.0.0.1:8080", // this listener has no TLS
                "http://127.0.0.1:8080/", // not a bare origin
                "http://127.0.0.1:8080@evil.example",
                "null",
                "",
            ] {
                assert!(
                    allowed(&dev, p, &[host, ("origin", bad)]).is_err(),
                    "Origin {bad:?} must be refused"
                );
            }

            // Two Origin headers are never legitimate.
            let mut map = headers(&[("host", "127.0.0.1:8080")]);
            map.append("origin", HeaderValue::from_static("http://127.0.0.1:8080"));
            map.append("origin", HeaderValue::from_static("http://127.0.0.1:8080"));
            assert!(dev_bypass_allowed(&dev, p, &map, &uri()).is_err());
        }

        #[test]
        fn test_dev_bypass_honours_fetch_metadata_when_the_browser_sends_it() {
            let dev = AuthConfig::dev();
            let p = peer("127.0.0.1:1");
            let host = ("host", "127.0.0.1:8080");
            assert!(allowed(&dev, p, &[host, ("sec-fetch-site", "same-origin")]).is_ok());
            assert!(allowed(&dev, p, &[host, ("sec-fetch-site", "none")]).is_ok());
            assert!(allowed(&dev, p, &[host, ("sec-fetch-site", "cross-site")]).is_err());
            assert!(allowed(&dev, p, &[host, ("sec-fetch-site", "same-site")]).is_err());
        }

        #[test]
        fn test_route_required_scope_covers_every_dashboard_route() {
            // Only the static SPA shell is public.
            assert_eq!(route_required_scope(&Method::GET, "/"), None);
            for path in [
                "/api/status",
                "/api/workers",
                "/api/tasks",
                "/api/tasks/00000000-0000-0000-0000-000000000000",
                "/ws",
                "/api/stream",
                "/api/stream/sse",
                "/api/modes",
                "/api/modes/status",
                "/anything/else",
            ] {
                assert_eq!(
                    route_required_scope(&Method::GET, path),
                    Some(AuthScope::Observer),
                    "GET {path}"
                );
            }
            assert_eq!(
                route_required_scope(&Method::POST, "/api/tasks"),
                Some(AuthScope::Submitter)
            );
            assert_eq!(
                route_required_scope(&Method::POST, "/api/chat"),
                Some(AuthScope::Submitter)
            );
            assert_eq!(
                route_required_scope(&Method::POST, "/api/modes/run"),
                Some(AuthScope::Admin)
            );
            // Anything unexpected needs Admin rather than slipping through.
            for method in [
                Method::DELETE,
                Method::PUT,
                Method::PATCH,
                Method::OPTIONS,
                Method::HEAD,
            ] {
                assert_eq!(
                    route_required_scope(&method, "/api/tasks"),
                    Some(AuthScope::Admin),
                    "{method}"
                );
            }
            assert_eq!(
                route_required_scope(&Method::POST, "/"),
                Some(AuthScope::Admin)
            );
        }
    }

    // --- Step B: dashboard dev-bypass policy ---

    fn loopback() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    fn lan() -> SocketAddr {
        "192.168.1.20:8080".parse().unwrap()
    }

    fn strict_auth() -> AuthConfig {
        AuthConfig {
            dev_mode: false,
            ..AuthConfig::default()
        }
    }

    #[test]
    fn test_a_bare_loopback_master_gets_the_local_bypass_only_when_asked_to() {
        // No secret, no token, loopback listener, implicit access requested: on.
        let (auth, state) = resolve_dashboard_auth(strict_auth(), false, true, loopback());
        assert!(auth.dev_mode);
        assert_eq!(state, DevBypass::Implicit);

        // The library default (implicit access not requested) stays locked.
        let (auth, state) = resolve_dashboard_auth(strict_auth(), false, false, loopback());
        assert!(!auth.dev_mode);
        assert_eq!(state, DevBypass::Off);

        // IPv6 loopback and IPv4-mapped loopback count as loopback.
        for addr in ["[::1]:8080", "[::ffff:127.0.0.1]:8080"] {
            let (_, state) =
                resolve_dashboard_auth(strict_auth(), false, true, addr.parse().unwrap());
            assert_eq!(state, DevBypass::Implicit, "{addr}");
        }
    }

    #[test]
    fn test_any_credential_configuration_switches_the_implicit_bypass_off() {
        // A cluster secret.
        let (auth, state) = resolve_dashboard_auth(strict_auth(), true, true, loopback());
        assert!(!auth.dev_mode);
        assert_eq!(state, DevBypass::Off);
        // An explicit token.
        let with_token = strict_auth().with_token("t", "ops", &[AuthScope::Admin]);
        let (auth, state) = resolve_dashboard_auth(with_token, false, true, loopback());
        assert!(!auth.dev_mode);
        assert_eq!(state, DevBypass::Off);
        // loopback_bypass = false forbids it as well.
        let mut no_bypass = strict_auth();
        no_bypass.loopback_bypass = false;
        let (auth, state) = resolve_dashboard_auth(no_bypass, false, true, loopback());
        assert!(!auth.dev_mode);
        assert_eq!(state, DevBypass::Off);
        // And auth switched off entirely is left alone.
        let mut disabled = strict_auth();
        disabled.enabled = false;
        let (auth, state) = resolve_dashboard_auth(disabled, false, true, loopback());
        assert!(!auth.enabled && !auth.dev_mode);
        assert_eq!(state, DevBypass::Off);
    }

    #[test]
    fn test_explicit_dev_mode_needs_a_loopback_listener_whatever_else_is_configured() {
        // --dev on a loopback listener: honoured, even with a secret or tokens around.
        for (has_secret, tokens) in [(false, false), (true, false), (false, true)] {
            let mut auth = AuthConfig::dev();
            if tokens {
                auth.add_token("t", "ops", &[AuthScope::Admin]);
            }
            let (auth, state) = resolve_dashboard_auth(auth, has_secret, false, loopback());
            assert!(auth.dev_mode);
            assert_eq!(state, DevBypass::Explicit);
        }

        // --dev on a listener that is not loopback: refused, for every address that is not loopback.
        for addr in [
            lan(),
            "0.0.0.0:8080".parse().unwrap(),
            "[::]:8080".parse().unwrap(),
        ] {
            for implicit in [false, true] {
                let (auth, state) =
                    resolve_dashboard_auth(AuthConfig::dev(), false, implicit, addr);
                assert!(!auth.dev_mode, "{addr} implicit={implicit}");
                assert_eq!(state, DevBypass::RefusedNonLoopback);
            }
            // No request for dev mode, nothing refused, and implicit access does not apply either.
            let (auth, state) = resolve_dashboard_auth(strict_auth(), false, true, addr);
            assert!(!auth.dev_mode);
            assert_eq!(state, DevBypass::Off);
        }
    }
}
