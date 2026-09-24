//! Integration test suite for Zero-Trust RBAC security and role permissions (R5).
//!
//! Validates:
//! 1. `test_rbac_loopback_dev_bypass`:
//!    - Loopback requests without credentials in dev mode automatically receive Admin permissions.
//! 2. `test_rbac_strict_mode_unauthorized`:
//!    - With dev_mode=false and loopback_bypass=false, unauthenticated requests receive 401 Unauthorized.
//! 3. `test_rbac_invalid_token`:
//!    - Malformed or unrecognized tokens receive 401 Unauthorized across headers and query params.
//! 4. `test_rbac_scope_enforcement`:
//!    - Observer scope can read /api/status, /api/workers, /api/tasks, and connect to /ws, but gets 403 Forbidden on POST /api/tasks.
//!    - Submitter scope can read metrics and POST /api/tasks, but gets 403 Forbidden on POST /api/modes/run.
//!    - Admin scope can access all endpoints.
//! 5. `test_rbac_multi_channel_credential_extraction`:
//!    - Token extraction works interchangeably via Authorization Bearer, X-API-Key, and ?token= query parameter.

use std::time::Duration;
use futures::StreamExt;
use reqwest::{header, StatusCode};
use tokio_tungstenite::connect_async;

use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::server::{MasterServer, ServerConfig};

/// Test 1: Loopback `--dev` mode bypass grants Admin scope to local unauthenticated requests.
#[tokio::test]
async fn test_rbac_loopback_dev_bypass() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: true,
        loopback_bypass: true,
        tokens: Default::default(),
    };

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // 1. GET / (public HTML) succeeds
    let resp = client.get(&base_url).send().await.expect("GET /");
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. GET /api/status succeeds via loopback bypass
    let resp = client
        .get(format!("{}/api/status", base_url))
        .send()
        .await
        .expect("GET /api/status");
    assert_eq!(resp.status(), StatusCode::OK);

    // 3. POST /api/tasks succeeds via loopback bypass (Admin auto-granted)
    let resp = client
        .post(format!("{}/api/tasks", base_url))
        .json(&serde_json::json!({ "command": "echo 'dev bypass test'" }))
        .send()
        .await
        .expect("POST /api/tasks");
    assert_eq!(resp.status(), StatusCode::OK);

    // 4. WebSocket upgrade succeeds via loopback bypass
    let ws_url = format!("ws://{}/ws", dash_addr);
    let (mut ws_stream, resp) = connect_async(&ws_url).await.expect("connect ws");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    let _ = ws_stream.close(None).await;

    let _ = master.shutdown();
}

/// Test 2: Strict production mode rejects unauthenticated requests with 401 Unauthorized.
#[tokio::test]
async fn test_rbac_strict_mode_unauthorized() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("valid-admin-token", "root-admin", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // 1. GET / is always public
    let resp = client.get(&base_url).send().await.expect("GET /");
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. GET /api/status without token -> 401 Unauthorized
    let resp = client
        .get(format!("{}/api/status", base_url))
        .send()
        .await
        .expect("GET /api/status");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
        "Bearer realm=\"OxideSwarm\""
    );

    // 3. POST /api/tasks without token -> 401 Unauthorized
    let resp = client
        .post(format!("{}/api/tasks", base_url))
        .json(&serde_json::json!({ "command": "echo test" }))
        .send()
        .await
        .expect("POST /api/tasks");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 4. GET /api/workers without token -> 401 Unauthorized
    let resp = client
        .get(format!("{}/api/workers", base_url))
        .send()
        .await
        .expect("GET /api/workers");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 5. Supplying valid admin token succeeds
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer valid-admin-token")
        .send()
        .await
        .expect("GET /api/status with token");
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = master.shutdown();
}

/// Test 3: Invalid tokens are rejected with 401 Unauthorized across all extraction vectors.
#[tokio::test]
async fn test_rbac_invalid_token() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("real-token", "real-admin", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Case A: Invalid Authorization Bearer
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer bogus-token-123")
        .send()
        .await
        .expect("GET with bad bearer");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Case B: Invalid X-API-Key
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header("X-API-Key", "bogus-api-key-456")
        .send()
        .await
        .expect("GET with bad api key");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Case C: Invalid URL Query Token
    let resp = client
        .get(format!("{}/api/status?token=bogus-query-token-789", base_url))
        .send()
        .await
        .expect("GET with bad query token");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let _ = master.shutdown();
}

/// Test 4: Scope hierarchy and enforcement: Observer vs Submitter vs Admin.
#[tokio::test]
async fn test_rbac_scope_enforcement() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("token-obs", "observer-client", &[AuthScope::Observer])
    .with_token("token-sub", "submitter-client", &[AuthScope::Submitter])
    .with_token("token-adm", "admin-client", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // --- Observer Scope ---
    // Can read /api/status
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer token-obs")
        .send()
        .await
        .expect("observer status");
    assert_eq!(resp.status(), StatusCode::OK);

    // Can read /api/workers
    let resp = client
        .get(format!("{}/api/workers", base_url))
        .header(header::AUTHORIZATION, "Bearer token-obs")
        .send()
        .await
        .expect("observer workers");
    assert_eq!(resp.status(), StatusCode::OK);

    // Cannot POST /api/tasks -> 403 Forbidden
    let resp = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-obs")
        .json(&serde_json::json!({ "command": "echo test" }))
        .send()
        .await
        .expect("observer submit task");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Cannot POST /api/modes/run -> 403 Forbidden
    let resp = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-obs")
        .json(&serde_json::json!({ "mode": "doc" }))
        .send()
        .await
        .expect("observer run mode");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Can connect to WebSocket /ws?token=token-obs
    let ws_url = format!("ws://{}/ws?token=token-obs", dash_addr);
    let (mut ws_stream, resp) = connect_async(&ws_url).await.expect("observer connect ws");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    let _ = ws_stream.close(None).await;

    // --- Submitter Scope ---
    // Can read /api/status (Submitter inherits Observer)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer token-sub")
        .send()
        .await
        .expect("submitter status");
    assert_eq!(resp.status(), StatusCode::OK);

    // Can POST /api/tasks
    let resp = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-sub")
        .json(&serde_json::json!({ "command": "echo 'submitter task'" }))
        .send()
        .await
        .expect("submitter submit task");
    assert_eq!(resp.status(), StatusCode::OK);

    // Cannot POST /api/modes/run -> 403 Forbidden
    let resp = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-sub")
        .json(&serde_json::json!({ "mode": "doc" }))
        .send()
        .await
        .expect("submitter run mode");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // --- Admin Scope ---
    // Can read /api/status
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer token-adm")
        .send()
        .await
        .expect("admin status");
    assert_eq!(resp.status(), StatusCode::OK);

    // Can POST /api/tasks
    let resp = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-adm")
        .json(&serde_json::json!({ "command": "echo 'admin task'" }))
        .send()
        .await
        .expect("admin submit task");
    assert_eq!(resp.status(), StatusCode::OK);

    // Can POST /api/modes/run (authenticated, returns 200 OK or 400 Bad Request on mode validation, not 401/403)
    let resp = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-adm")
        .json(&serde_json::json!({ "mode": "doc", "action": "verify" }))
        .send()
        .await
        .expect("admin run mode");
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);

    let _ = master.shutdown();
}

/// Test 5: Credential extraction supports Bearer, X-API-Key, and query parameter ?token=.
#[tokio::test]
async fn test_rbac_multi_channel_credential_extraction() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("shared-secret-key", "integration-client", &[AuthScope::Submitter]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Channel 1: Authorization: Bearer <token>
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer shared-secret-key")
        .send()
        .await
        .expect("Bearer auth");
    assert_eq!(resp.status(), StatusCode::OK);

    // Channel 2: X-API-Key: <token>
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header("X-API-Key", "shared-secret-key")
        .send()
        .await
        .expect("X-API-Key auth");
    assert_eq!(resp.status(), StatusCode::OK);

    // Channel 3: ?token=<token>
    let resp = client
        .get(format!("{}/api/status?token=shared-secret-key", base_url))
        .send()
        .await
        .expect("?token= auth");
    assert_eq!(resp.status(), StatusCode::OK);

    // Channel 4: ?api_key=<token>
    let resp = client
        .get(format!("{}/api/status?api_key=shared-secret-key", base_url))
        .send()
        .await
        .expect("?api_key= auth");
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = master.shutdown();
}

/// Adversarial Test 1: Malformed tokens, invalid schemes, and empty prefixes rejected with 401.
#[tokio::test]
async fn test_adversarial_malformed_tokens_and_empty_prefixes() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("valid-admin-token", "root-admin", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // 1. Authorization: Bearer (no space, empty)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 2. Authorization: Bearer   (space, but empty token)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer ")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 3. Authorization: bearer    (lowercase, multiple spaces)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "bearer    ")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 4. Authorization: Basic dXNlcjpwYXNz (unsupported scheme)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Basic dXNlcjpwYXNz")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 5. Authorization: Token 12345
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Token 12345")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 6. X-API-Key: "" (empty)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header("X-API-Key", "")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 7. X-API-Key: "   " (whitespace)
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header("X-API-Key", "   ")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 8. ?token= (empty query param)
    let resp = client
        .get(format!("{}/api/status?token=", base_url))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 9. ?token=   (whitespace query param)
    let resp = client
        .get(format!("{}/api/status?token=%20%20%20", base_url))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 10. ?api_key= (empty query param)
    let resp = client
        .get(format!("{}/api/status?api_key=", base_url))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 11. Valid token with padding whitespace trimmed
    let resp = client
        .get(format!("{}/api/status", base_url))
        .header(header::AUTHORIZATION, "Bearer   valid-admin-token   ")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::OK);

    // 12. Valid token in multi-param query string
    let resp = client
        .get(format!("{}/api/status?foo=1&token=valid-admin-token&bar=2", base_url))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::OK);

    // 13. Valid api_key in multi-param query string
    let resp = client
        .get(format!("{}/api/status?alpha=beta&api_key=valid-admin-token", base_url))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = master.shutdown();
}

/// Adversarial Test 2: Non-UUID and nonexistent task paths strictly return 404 NOT FOUND.
#[tokio::test]
async fn test_adversarial_non_uuid_task_routes_strict_404() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("admin-key", "admin", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // 1. Completely non-UUID string
    let resp = client
        .get(format!("{}/api/tasks/not-a-uuid", base_url))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 2. Pure digits
    let resp = client
        .get(format!("{}/api/tasks/1234567890", base_url))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 3. Invalid characters
    let resp = client
        .get(format!("{}/api/tasks/invalid!@#$%", base_url))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 4. Valid UUID format but nonexistent task in queue
    let nonexistent_uuid = uuid::Uuid::nil().to_string();
    let resp = client
        .get(format!("{}/api/tasks/{}", base_url, nonexistent_uuid))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 5. Another random UUID nonexistent in queue
    let random_uuid = uuid::Uuid::new_v4().to_string();
    let resp = client
        .get(format!("{}/api/tasks/{}", base_url, random_uuid))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 6. Non-hex characters inside UUID-like format
    let resp = client
        .get(format!("{}/api/tasks/g0000000-0000-0000-0000-000000000000", base_url))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 7. Submit a real task and verify GET /api/tasks/:task_id returns 200 OK with correct schema
    let submit_resp = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .json(&serde_json::json!({ "command": "echo 'empirical test'" }))
        .send()
        .await
        .expect("submit");
    assert_eq!(submit_resp.status(), StatusCode::OK);
    let submit_json: serde_json::Value = submit_resp.json().await.expect("json");
    let task_id_str = submit_json["task_id"].as_str().expect("task_id str");

    let get_resp = client
        .get(format!("{}/api/tasks/{}", base_url, task_id_str))
        .header(header::AUTHORIZATION, "Bearer admin-key")
        .send()
        .await
        .expect("get task");
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_json: serde_json::Value = get_resp.json().await.expect("json");
    assert_eq!(get_json["id"], task_id_str);
    assert!(get_json.get("state").is_some());
    assert!(get_json.get("stdout").is_some());
    assert!(get_json.get("stderr").is_some());

    let _ = master.shutdown();
}

/// Adversarial Test 3: Scope permission matrix enforcement across all scopes.
#[tokio::test]
async fn test_adversarial_scope_escalation_matrix() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("token-obs", "obs-user", &[AuthScope::Observer])
    .with_token("token-wrk", "wrk-user", &[AuthScope::Worker])
    .with_token("token-sub", "sub-user", &[AuthScope::Submitter])
    .with_token("token-adm", "adm-user", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Matrix check: POST /api/tasks requires Submitter or Admin
    // Observer -> 403
    let res = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-obs")
        .json(&serde_json::json!({ "command": "echo 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Worker -> 403 (Worker scope cannot submit tasks)
    let res = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-wrk")
        .json(&serde_json::json!({ "command": "echo 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Submitter -> 200
    let res = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-sub")
        .json(&serde_json::json!({ "command": "echo 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Admin -> 200
    let res = client
        .post(format!("{}/api/tasks", base_url))
        .header(header::AUTHORIZATION, "Bearer token-adm")
        .json(&serde_json::json!({ "command": "echo 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Matrix check: POST /api/modes/run requires Admin
    // Observer -> 403
    let res = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-obs")
        .json(&serde_json::json!({ "mode": "doc" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Worker -> 403
    let res = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-wrk")
        .json(&serde_json::json!({ "mode": "doc" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Submitter -> 403
    let res = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-sub")
        .json(&serde_json::json!({ "mode": "doc" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Admin -> not 401/403
    let res = client
        .post(format!("{}/api/modes/run", base_url))
        .header(header::AUTHORIZATION, "Bearer token-adm")
        .json(&serde_json::json!({ "mode": "doc" }))
        .send()
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);

    // Matrix check: SSE /api/stream/sse requires Observer
    // Unauthenticated -> 401
    let res = client.get(format!("{}/api/stream/sse", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Observer -> 200 OK
    let res = client.get(format!("{}/api/stream/sse?token=token-obs", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let _ = master.shutdown();
}

/// Adversarial Test 4: Loopback bypass dev toggle permutations and credential priority.
#[tokio::test]
async fn test_adversarial_loopback_bypass_dev_toggle_permutations() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Permutation A: dev_mode=false, loopback_bypass=true -> Loopback MUST be rejected (401)
    {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_dashboard_port(0)
            .with_auth_config(AuthConfig {
                enabled: true,
                dev_mode: false,
                loopback_bypass: true,
                tokens: Default::default(),
            });
        let master = MasterServer::spawn(config).await.expect("spawn");
        let base_url = format!("http://{}", master.dashboard_addr().unwrap());
        let res = client.get(format!("{}/api/status", base_url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "dev_mode=false must reject unauth loopback");
        let _ = master.shutdown();
    }

    // Permutation B: dev_mode=true, loopback_bypass=false -> Loopback MUST be rejected (401)
    {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_dashboard_port(0)
            .with_auth_config(AuthConfig {
                enabled: true,
                dev_mode: true,
                loopback_bypass: false,
                tokens: Default::default(),
            });
        let master = MasterServer::spawn(config).await.expect("spawn");
        let base_url = format!("http://{}", master.dashboard_addr().unwrap());
        let res = client.get(format!("{}/api/status", base_url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "loopback_bypass=false must reject unauth loopback");
        let _ = master.shutdown();
    }

    // Permutation C: dev_mode=true, loopback_bypass=true with explicit INVALID token
    // MUST return 401, not bypassed!
    {
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_dashboard_port(0)
            .with_auth_config(AuthConfig {
                enabled: true,
                dev_mode: true,
                loopback_bypass: true,
                tokens: Default::default(),
            });
        let master = MasterServer::spawn(config).await.expect("spawn");
        let base_url = format!("http://{}", master.dashboard_addr().unwrap());
        let res = client
            .get(format!("{}/api/status", base_url))
            .header(header::AUTHORIZATION, "Bearer invalid-token-on-loopback")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "explicit invalid token must not be bypassed");
        let _ = master.shutdown();
    }

    // Permutation D: dev_mode=true, loopback_bypass=true with explicit OBSERVER token
    // accessing POST /api/tasks MUST return 403, not escalated to Admin!
    {
        let auth_config = AuthConfig {
            enabled: true,
            dev_mode: true,
            loopback_bypass: true,
            tokens: Default::default(),
        }
        .with_token("explicit-obs", "obs", &[AuthScope::Observer]);

        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
            .with_dashboard_port(0)
            .with_auth_config(auth_config);
        let master = MasterServer::spawn(config).await.expect("spawn");
        let base_url = format!("http://{}", master.dashboard_addr().unwrap());
        let res = client
            .post(format!("{}/api/tasks", base_url))
            .header(header::AUTHORIZATION, "Bearer explicit-obs")
            .json(&serde_json::json!({ "command": "echo 1" }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN, "explicit observer token must not escalate to admin");
        let _ = master.shutdown();
    }
}

/// Adversarial Test 5: High-concurrency telemetry streaming burst across /ws, /api/stream, and /api/stream/sse.
#[tokio::test]
async fn test_adversarial_high_concurrency_stream_burst() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
    }
    .with_token("stream-token", "stream-tester", &[AuthScope::Admin]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{}", dash_addr);

    const CONCURRENT_CLIENTS: usize = 10;
    let mut ws_handles = Vec::new();

    // 1. Spawn concurrent WebSocket clients to /ws
    for _ in 0..CONCURRENT_CLIENTS {
        let ws_url = format!("ws://{}/ws?token=stream-token", dash_addr);
        let handle = tokio::spawn(async move {
            let (mut ws_stream, resp) = connect_async(&ws_url).await.expect("connect ws");
            assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
            let msg = ws_stream.next().await.expect("snapshot msg").expect("frame");
            assert!(msg.is_text());
            ws_stream
        });
        ws_handles.push(handle);
    }

    // 2. Spawn concurrent WebSocket clients to /api/stream
    for _ in 0..CONCURRENT_CLIENTS {
        let ws_url = format!("ws://{}/api/stream?token=stream-token", dash_addr);
        let handle = tokio::spawn(async move {
            let (mut ws_stream, resp) = connect_async(&ws_url).await.expect("connect stream");
            assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
            let msg = ws_stream.next().await.expect("snapshot msg").expect("frame");
            assert!(msg.is_text());
            ws_stream
        });
        ws_handles.push(handle);
    }

    // 3. Spawn concurrent SSE clients to /api/stream/sse
    let mut sse_handles = Vec::new();
    for _ in 0..CONCURRENT_CLIENTS {
        let sse_url = format!("{}/api/stream/sse?token=stream-token", base_url);
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::builder().build().unwrap();
            let mut res = client.get(&sse_url).send().await.expect("connect sse");
            assert_eq!(res.status(), StatusCode::OK);
            let chunk = res.chunk().await.expect("read chunk").expect("first chunk");
            assert!(!chunk.is_empty());
        });
        sse_handles.push(handle);
    }

    // Wait for all SSE clients to connect and read first chunk
    for h in sse_handles {
        h.await.expect("sse join");
    }

    // Submit 5 tasks while clients are connected
    let client = reqwest::Client::new();
    for i in 0..5 {
        let resp = client
            .post(format!("{}/api/tasks", base_url))
            .header(header::AUTHORIZATION, "Bearer stream-token")
            .json(&serde_json::json!({ "command": format!("echo 'burst {i}'") }))
            .send()
            .await
            .expect("submit task");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Gracefully close all WebSocket streams
    for h in ws_handles {
        let mut ws = h.await.expect("ws join");
        let _ = ws.close(None).await;
    }

    let _ = master.shutdown();
}

