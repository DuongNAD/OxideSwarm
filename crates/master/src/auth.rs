//! Zero-Trust Authentication and Role-Based Access Control (RBAC) for OxideSwarm Master.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiToken {
    pub token: String,
    pub name: String,
    pub scopes: HashSet<AuthScope>,
}

/// Master RBAC authentication configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthConfig {
    /// Whether authentication middleware is actively enforced.
    pub enabled: bool,
    /// Whether development mode is active (enables loopback automatic Admin bypass).
    pub dev_mode: bool,
    /// Whether requests originating from loopback (127.0.0.1 / ::1) bypass auth when no token is passed.
    pub loopback_bypass: bool,
    /// Registered valid tokens and their associated scopes.
    pub tokens: HashMap<String, ApiToken>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        let dev_mode = std::env::var("OXIDE_DEV_MODE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        Self {
            enabled: true,
            dev_mode,
            loopback_bypass: true,
            tokens: HashMap::new(),
        }
    }
}

impl AuthConfig {
    /// Creates a fresh default AuthConfig.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder helper to register an API token with granted scopes.
    pub fn with_token(
        mut self,
        token: impl Into<String>,
        name: impl Into<String>,
        scopes: &[AuthScope],
    ) -> Self {
        let t_str = token.into();
        self.tokens.insert(
            t_str.clone(),
            ApiToken {
                token: t_str,
                name: name.into(),
                scopes: scopes.iter().copied().collect(),
            },
        );
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
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::{
        extract::{ConnectInfo, Request, State},
        http::{header, HeaderMap, Method, StatusCode},
        middleware::Next,
        response::{IntoResponse, Response},
        Json,
    };
    use tracing::{debug, warn};

    /// Extracts token from `Authorization: Bearer <token>`, `X-API-Key: <key>`, or URL query `?token=<token>`.
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
            (&Method::GET, "/") => None, // Public SPA HTML
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

    /// Axum middleware function validating credentials and enforcing RBAC.
    pub async fn rbac_auth_middleware(
        State(config): State<Arc<AuthConfig>>,
        mut req: Request,
        next: Next,
    ) -> Response {
        if !config.enabled {
            return next.run(req).await;
        }

        let path = req.uri().path();
        let method = req.method();

        let required_scope = match route_required_scope(method, path) {
            Some(scope) => scope,
            None => return next.run(req).await,
        };

        // Extract credential
        let extracted_token = extract_credential(req.headers(), req.uri().query());

        // Check peer address for loopback
        let peer_addr = req.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0);
        let peer_is_loopback = peer_addr.map(|a| a.ip().is_loopback()).unwrap_or(true);

        // Loopback / Dev mode bypass if no credential was explicitly provided
        if extracted_token.is_none() && config.loopback_bypass && config.dev_mode && peer_is_loopback {
            debug!(
                peer = ?peer_addr,
                path = %path,
                "Loopback dev-mode authentication bypass active (Admin granted)"
            );
            let bypass_id = AuthIdentity {
                name: "loopback-dev".to_string(),
                scopes: [AuthScope::Admin].into_iter().collect(),
                is_bypassed: true,
            };
            req.extensions_mut().insert(bypass_id);
            return next.run(req).await;
        }

        // If no token was extracted, reject with 401 Unauthorized
        let Some(token_str) = extracted_token else {
            warn!(
                peer = ?peer_addr,
                path = %path,
                "Unauthorized request: missing authentication credentials"
            );
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer realm=\"OxideSwarm\"")],
                Json(serde_json::json!({
                    "error": "Unauthorized",
                    "message": "Missing authentication credential (Authorization Bearer, X-API-Key, or ?token=)"
                })),
            )
                .into_response();
        };

        // Validate token against registered tokens
        if let Some(registered) = config.tokens.get(&token_str) {
            let identity = AuthIdentity {
                name: registered.name.clone(),
                scopes: registered.scopes.clone(),
                is_bypassed: false,
            };

            if identity.has_scope(required_scope) {
                req.extensions_mut().insert(identity);
                return next.run(req).await;
            } else {
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
        }

        warn!(
            token = %token_str,
            "Unauthorized request: invalid authentication token"
        );
        (
            StatusCode::UNAUTHORIZED,
            [(
                header::WWW_AUTHENTICATE,
                "Bearer realm=\"OxideSwarm\", error=\"invalid_token\"",
            )],
            Json(serde_json::json!({
                "error": "Unauthorized",
                "message": "Invalid Bearer token or API key"
            })),
        )
            .into_response()
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
        assert_eq!("submitter".parse::<AuthScope>().unwrap(), AuthScope::Submitter);
        assert_eq!("worker".parse::<AuthScope>().unwrap(), AuthScope::Worker);
        assert_eq!("observer".parse::<AuthScope>().unwrap(), AuthScope::Observer);
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
}
