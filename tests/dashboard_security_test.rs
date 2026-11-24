//! Dashboard hardening tests (OXS-AUTH design, section 4 and 5).
//!
//! Validates, against a real `MasterServer` dashboard listener:
//! 1. Deny by default: with no credentials configured every route (REST, `/ws`, `/api/stream`,
//!    SSE, POST/PUT/DELETE, unknown paths) answers 401 and nothing is queued; only the static SPA
//!    shell (`GET /`) and the discovery/rendezvous routes (`GET /api/discovery/probe`,
//!    `GET /api/rendezvous/master`, `POST /api/rendezvous/register`) are public, and anonymous
//!    reads of `GET /api/rendezvous/master` redact `p2p_ticket`.
//! 2. A browser on another origin cannot drive a dev-mode dashboard: an `Origin: https://evil.example`
//!    POST is denied even with dev mode on, as are spoofed `Host` headers (DNS rebinding), proxy
//!    forwarding headers and cross-site fetch metadata, while genuine local requests still work.
//! 3. No permissive CORS: cross-origin reads get no `Access-Control-Allow-*` headers unless the origin
//!    is explicitly allow-listed (and `*` is never honoured).
//! 4. Security headers on every kind of response.
//! 5. The Admin token derived from the cluster secret works, the raw secret does not.
//! 6. Listener-address policy: a bare loopback master (no secret, no token) gets a usable local
//!    dashboard, any secret or token switches that off, the dev bypass is never honoured on a
//!    dashboard bound to a non-loopback address, and the dashboard stays on loopback unless it is
//!    told otherwise.

use std::net::SocketAddr;
use std::time::Duration;

use reqwest::{Method, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;

use rusty_grid_core::noise_auth::{compute_enroll_proof, MemberRole, NodeKeypair};
use rusty_grid_core::ClusterSecret;
use rusty_grid_master::auth::{dashboard_admin_token, AuthConfig, AuthScope, DevBypass};
use rusty_grid_master::server::{AuthMode, MasterHandle, MasterServer, MemberStore, ServerConfig};

const NIL_TASK_ID: &str = "00000000-0000-0000-0000-000000000000";

async fn spawn_dashboard(
    auth: AuthConfig,
    secret: Option<ClusterSecret>,
) -> (MasterHandle, SocketAddr) {
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth);
    if let Some(secret) = secret {
        config = config.with_cluster_secret(secret);
    }
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let addr = master.dashboard_addr().expect("dashboard addr");
    (master, addr)
}

/// Authentication enforced, no dev bypass regardless of the process environment.
fn strict() -> AuthConfig {
    AuthConfig {
        dev_mode: false,
        ..AuthConfig::default()
    }
}

fn http_client() -> reqwest::Client {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}

async fn task_count(master: &MasterHandle) -> usize {
    master.queue_stats().await.expect("queue stats").total
}

/// Status line and headers of a raw HTTP/1.1 response (the body is not read).
struct RawResponse {
    status: u16,
    headers: Vec<(String, String)>,
}

impl RawResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Sends a hand-written HTTP/1.1 request so the test controls `Host`, `Origin` and friends exactly.
async fn raw_http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> RawResponse {
    let mut request = format!("{method} {path} HTTP/1.1\r\n");
    if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("host")) {
        request.push_str(&format!("Host: {addr}\r\n"));
    }
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }

    let mut stream = TcpStream::connect(addr).await.expect("connect dashboard");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");

    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk))
            .await
            .expect("response head timed out")
            .expect("read response");
        assert!(
            n > 0,
            "connection closed before a full response head arrived"
        );
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status line");
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    RawResponse { status, headers }
}

/// POST /api/tasks with the given extra headers; returns the HTTP status.
async fn post_task(addr: SocketAddr, extra: &[(&str, &str)]) -> u16 {
    raw_http(
        addr,
        "POST",
        "/api/tasks",
        extra,
        Some(r#"{"command":"echo from-test"}"#),
    )
    .await
    .status
}

// ---------------------------------------------------------------------------------------------
// 1. Deny by default
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_every_route_is_denied_without_a_token_and_nothing_runs() {
    // Dev mode off and no tokens: nobody can authenticate at all.
    let (master, addr) = spawn_dashboard(strict(), None).await;
    let base = format!("http://{addr}");
    let client = http_client();

    let routes: Vec<(Method, String)> = vec![
        (Method::GET, "/api/status".into()),
        (Method::GET, "/api/workers".into()),
        (Method::GET, "/api/tasks".into()),
        (Method::GET, format!("/api/tasks/{NIL_TASK_ID}")),
        (Method::GET, "/api/modes".into()),
        (Method::GET, "/api/modes/status".into()),
        (Method::GET, "/api/members".into()),
        (Method::GET, "/api/invites".into()),
        (Method::POST, "/api/invites".into()),
        (Method::DELETE, "/api/invites/abc".into()),
        (Method::POST, "/api/members/abc/approve".into()),
        (Method::POST, "/api/members/abc/reject".into()),
        (Method::POST, "/api/members/abc/revoke".into()),
        (Method::POST, "/api/members/abc/rename".into()),
        (Method::GET, "/api/stream/sse".into()),
        (Method::GET, "/ws".into()),
        (Method::GET, "/api/stream".into()),
        (Method::HEAD, "/api/status".into()),
        (Method::POST, "/api/tasks".into()),
        (Method::POST, "/api/modes/run".into()),
        (Method::PUT, "/api/tasks".into()),
        (Method::PATCH, "/api/tasks".into()),
        (Method::DELETE, "/api/tasks".into()),
        (Method::DELETE, format!("/api/tasks/{NIL_TASK_ID}")),
        (Method::POST, "/".into()),
        (Method::GET, "/no/such/route".into()),
    ];
    for (method, path) in routes {
        let resp = client
            .request(method.clone(), format!("{base}{path}"))
            .header("content-type", "application/json")
            .body(r#"{"command":"touch /tmp/oxs-should-never-run","mode":"doc"}"#)
            .send()
            .await
            .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path} must require authentication"
        );
        assert!(
            resp.headers().contains_key("www-authenticate"),
            "{method} {path}: 401 must carry WWW-Authenticate"
        );
    }

    // WebSocket upgrades are refused during the HTTP handshake, before any frame is exchanged.
    for path in ["/ws", "/api/stream"] {
        match connect_async(format!("ws://{addr}{path}")).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status(), 401, "WebSocket {path} must be refused");
            }
            other => {
                panic!("WebSocket {path} without a token must fail with HTTP 401, got {other:?}")
            }
        }
    }

    // A wrong token is no better than none, in every transport.
    for suffix in ["token=wrong", "api_key=wrong"] {
        let resp = client
            .get(format!("{base}/api/status?{suffix}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    assert_eq!(
        task_count(&master).await,
        0,
        "no unauthorized request may create a task"
    );

    // CORS preflights cannot carry credentials, so the CORS layer answers OPTIONS itself: an empty
    // 200 that grants nothing (no data, no Access-Control-* permission) and reaches no handler.
    let options = raw_http(addr, "OPTIONS", "/api/tasks", &[], None).await;
    assert_eq!(options.status, 200);
    assert!(options
        .headers
        .iter()
        .all(|(k, _)| !k.to_ascii_lowercase().starts_with("access-control-")));

    // The static SPA shell is the single public resource and still gets the hardening headers.
    let resp = client.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["x-content-type-options"], "nosniff");

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_registered_token_reaches_every_route_class() {
    let auth = strict().with_token("admin-token-for-test", "tester", &[AuthScope::Admin]);
    let (master, addr) = spawn_dashboard(auth, None).await;
    let base = format!("http://{addr}");
    let client = http_client();
    let bearer = "Bearer admin-token-for-test";

    for path in ["/api/status", "/api/workers", "/api/tasks", "/api/modes"] {
        let resp = client
            .get(format!("{base}{path}"))
            .header("authorization", bearer)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET {path}");
    }

    let resp = client
        .post(format!("{base}/api/tasks"))
        .header("authorization", bearer)
        .json(&serde_json::json!({ "command": "echo authorized" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(task_count(&master).await, 1);

    // Removed POST /api/chat route returns 404 Not Found even with an Admin token (SEC-14).
    let resp = client
        .post(format!("{base}/api/chat"))
        .header("authorization", bearer)
        .json(&serde_json::json!({ "message": "hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "POST /api/chat with Admin token must return 404"
    );

    // Browsers cannot set headers on WebSocket/SSE requests, so those take ?token=.
    let (mut ws, resp) = connect_async(format!("ws://{addr}/ws?token=admin-token-for-test"))
        .await
        .expect("authorized websocket");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    let _ = ws.close(None).await;

    let resp = client
        .get(format!("{base}/api/stream/sse?token=admin-token-for-test"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// 2. Dev mode is limited to genuinely local, same-origin requests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_cross_origin_post_is_denied_even_in_dev_mode() {
    // Dev mode is explicitly on: a local developer gets Admin without a token...
    let (master, addr) = spawn_dashboard(AuthConfig::dev(), None).await;
    let port = addr.port();
    let same_origin = format!("http://{addr}");

    assert_eq!(
        post_task(addr, &[]).await,
        200,
        "control: local request without Origin"
    );
    assert_eq!(
        post_task(addr, &[("Origin", same_origin.as_str())]).await,
        200,
        "control: the dashboard's own page (same origin)"
    );
    assert_eq!(task_count(&master).await, 2);

    // ...but any website the developer visits must not be able to ride on that.
    let evil_origin = [("Origin", "https://evil.example")];
    assert_eq!(
        post_task(addr, &evil_origin).await,
        401,
        "a POST from https://evil.example must be denied in dev mode"
    );
    let attacks: Vec<(&str, Vec<(&str, String)>)> = vec![
        (
            "http evil origin",
            vec![("Origin", "http://evil.example".into())],
        ),
        ("null origin", vec![("Origin", "null".into())]),
        (
            "another local app on another port",
            vec![(
                "Origin",
                format!("http://127.0.0.1:{}", port.wrapping_add(1)),
            )],
        ),
        (
            "localhost spelling differs from the Host header",
            vec![("Origin", format!("http://localhost:{port}"))],
        ),
        (
            "DNS rebinding: attacker Host name",
            vec![("Host", format!("evil.example:{port}"))],
        ),
        (
            "DNS rebinding with matching attacker Origin",
            vec![
                ("Host", format!("evil.example:{port}")),
                ("Origin", format!("http://evil.example:{port}")),
            ],
        ),
        (
            "behind a reverse proxy (X-Forwarded-For)",
            vec![("X-Forwarded-For", "203.0.113.9".into())],
        ),
        (
            "behind a reverse proxy (Forwarded)",
            vec![("Forwarded", "for=203.0.113.9".into())],
        ),
        (
            "browser says cross-site",
            vec![("Sec-Fetch-Site", "cross-site".into())],
        ),
        (
            "invalid explicit token is not rescued by the bypass",
            vec![("Authorization", "Bearer not-a-real-token".into())],
        ),
    ];
    for (name, headers) in attacks {
        let borrowed: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        assert_eq!(post_task(addr, &borrowed).await, 401, "{name}");
    }
    assert_eq!(
        task_count(&master).await,
        2,
        "none of the rejected requests may have queued a task"
    );

    // Reads and the streaming endpoints obey the same rule.
    for path in ["/api/status", "/api/workers", "/api/tasks"] {
        assert_eq!(
            raw_http(addr, "GET", path, &[], None).await.status,
            200,
            "control {path}"
        );
        assert_eq!(
            raw_http(addr, "GET", path, &evil_origin, None).await.status,
            401,
            "cross-origin GET {path}"
        );
    }
    let sse = raw_http(addr, "GET", "/api/stream/sse", &[], None).await;
    assert_eq!(sse.status, 200);
    assert_eq!(
        raw_http(addr, "GET", "/api/stream/sse", &evil_origin, None)
            .await
            .status,
        401,
        "cross-origin SSE"
    );
    let ws_headers = |origin: Option<&'static str>| {
        let mut h = vec![
            ("Upgrade", "websocket"),
            ("Connection", "Upgrade"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ("Sec-WebSocket-Version", "13"),
        ];
        if let Some(origin) = origin {
            h.push(("Origin", origin));
        }
        h
    };
    let ws_local = raw_http(addr, "GET", "/ws", &ws_headers(None), None).await;
    assert_eq!(
        ws_local.status, 101,
        "control: local WebSocket without Origin"
    );
    let ws_evil = raw_http(
        addr,
        "GET",
        "/ws",
        &ws_headers(Some("https://evil.example")),
        None,
    )
    .await;
    assert_eq!(ws_evil.status, 401, "cross-site WebSocket hijack attempt");

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_loopback_bypass_needs_the_dev_flag() {
    // Same genuine local request as the dev-mode control above, but dev mode is off (the default).
    let (master, addr) = spawn_dashboard(strict(), None).await;
    assert_eq!(post_task(addr, &[]).await, 401);
    assert_eq!(
        raw_http(addr, "GET", "/api/status", &[], None).await.status,
        401
    );
    assert_eq!(task_count(&master).await, 0);
    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// 3. No permissive CORS
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_cors_is_same_origin_only_unless_explicitly_allow_listed() {
    let auth = strict()
        .with_token("cors-token", "tester", &[AuthScope::Admin])
        .with_allowed_origin("https://ops.example.com")
        .with_allowed_origin("*"); // must be ignored
    let (master, addr) = spawn_dashboard(auth, None).await;
    let auth_header = ("Authorization", "Bearer cors-token");

    let preflight = |origin: &'static str| {
        let headers = vec![
            ("Origin", origin),
            ("Access-Control-Request-Method", "POST"),
            (
                "Access-Control-Request-Headers",
                "authorization,content-type",
            ),
        ];
        async move { raw_http(addr, "OPTIONS", "/api/tasks", &headers, None).await }
    };

    // Allow-listed origin: preflight succeeds without credentials and names exactly that origin.
    let ok = preflight("https://ops.example.com").await;
    assert_eq!(
        ok.header("access-control-allow-origin"),
        Some("https://ops.example.com")
    );
    assert!(ok.header("access-control-allow-methods").is_some());
    assert!(
        ok.header("access-control-allow-credentials").is_none(),
        "credentials are bearer tokens, never ambient"
    );

    // Anyone else gets no CORS permission at all, and `*` was ignored.
    for origin in ["https://evil.example", "http://ops.example.com", "null"] {
        let denied = preflight(origin).await;
        assert!(
            denied.header("access-control-allow-origin").is_none(),
            "preflight from {origin} must not be granted"
        );
    }

    // Actual requests: the response only becomes readable for the allow-listed origin.
    let allowed = raw_http(
        addr,
        "GET",
        "/api/status",
        &[auth_header, ("Origin", "https://ops.example.com")],
        None,
    )
    .await;
    assert_eq!(allowed.status, 200);
    assert_eq!(
        allowed.header("access-control-allow-origin"),
        Some("https://ops.example.com")
    );
    let foreign = raw_http(
        addr,
        "GET",
        "/api/status",
        &[auth_header, ("Origin", "https://evil.example")],
        None,
    )
    .await;
    assert!(foreign.header("access-control-allow-origin").is_none());
    let public = raw_http(
        addr,
        "GET",
        "/",
        &[("Origin", "https://evil.example")],
        None,
    )
    .await;
    assert!(public.header("access-control-allow-origin").is_none());

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_default_dashboard_never_emits_cors_permission() {
    let auth = strict().with_token("t", "tester", &[AuthScope::Admin]);
    let (master, addr) = spawn_dashboard(auth, None).await;
    let origin = ("Origin", "https://evil.example");

    let preflight = raw_http(
        addr,
        "OPTIONS",
        "/api/tasks",
        &[
            origin,
            ("Access-Control-Request-Method", "POST"),
            ("Access-Control-Request-Headers", "content-type"),
        ],
        None,
    )
    .await;
    assert!(
        preflight
            .headers
            .iter()
            .all(|(k, _)| !k.to_ascii_lowercase().starts_with("access-control-")),
        "without an allow-list no Access-Control-* header may be emitted: {:?}",
        preflight.headers
    );

    let resp = raw_http(
        addr,
        "GET",
        "/api/status",
        &[origin, ("Authorization", "Bearer t")],
        None,
    )
    .await;
    assert_eq!(resp.status, 200);
    assert!(resp.header("access-control-allow-origin").is_none());

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// 4. Security headers
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_security_headers_on_every_kind_of_response() {
    let auth = strict()
        .with_token("obs", "observer", &[AuthScope::Observer])
        .with_token("adm", "admin", &[AuthScope::Admin]);
    let (master, addr) = spawn_dashboard(auth, None).await;

    // (method, path, request headers, expected status)
    type Case<'a> = (&'a str, &'a str, Vec<(&'a str, &'a str)>, u16);
    let cases: Vec<Case> = vec![
        ("GET", "/", vec![], 200),
        (
            "GET",
            "/api/status",
            vec![("Authorization", "Bearer adm")],
            200,
        ),
        ("GET", "/api/status", vec![], 401),
        (
            "POST",
            "/api/tasks",
            vec![("Authorization", "Bearer obs")],
            403,
        ),
        (
            "GET",
            "/no/such/route",
            vec![("Authorization", "Bearer adm")],
            404,
        ),
    ];
    for (method, path, headers, expected_status) in cases {
        let resp = raw_http(
            addr,
            method,
            path,
            &headers,
            (method == "POST").then_some("{}"),
        )
        .await;
        assert_eq!(resp.status, expected_status, "{method} {path}");
        assert_eq!(
            resp.header("x-content-type-options"),
            Some("nosniff"),
            "{method} {path}"
        );
        assert_eq!(
            resp.header("x-frame-options"),
            Some("DENY"),
            "{method} {path}"
        );
        assert_eq!(
            resp.header("referrer-policy"),
            Some("no-referrer"),
            "{method} {path}"
        );
    }

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// 5. Dashboard token derived from the cluster secret
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_secret_yields_a_derived_dashboard_admin_token() {
    let secret = ClusterSecret::from_bytes(&[0x42; 32]).unwrap();
    let derived = dashboard_admin_token(&secret);
    let (master, addr) = spawn_dashboard(strict(), Some(secret.clone())).await;
    let client = http_client();
    let base = format!("http://{addr}");

    // The derived token is an Admin credential...
    let resp = client
        .post(format!("{base}/api/tasks"))
        .bearer_auth(&derived)
        .json(&serde_json::json!({ "command": "echo derived" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(task_count(&master).await, 1);

    // ...the raw secret is not (it never leaves the cluster-protocol layer)...
    for credential in [secret.to_hex(), "0".repeat(64), derived[..63].to_string()] {
        let resp = client
            .get(format!("{base}/api/status"))
            .bearer_auth(credential)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ...and a master with a different secret derives a different token.
    let other = ClusterSecret::from_bytes(&[0x43; 32]).unwrap();
    assert_ne!(derived, dashboard_admin_token(&other));
    let (master2, addr2) = spawn_dashboard(strict(), Some(other)).await;
    let resp = client
        .get(format!("http://{addr2}/api/status"))
        .bearer_auth(&derived)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let _ = master.shutdown();
    let _ = master2.shutdown();
}

// ---------------------------------------------------------------------------------------------
// 6. Listener-address policy
// ---------------------------------------------------------------------------------------------

/// A loopback master the way the CLI starts it: implicit local access requested, no `--dev`.
async fn spawn_bare_master(
    auth: AuthConfig,
    secret: Option<ClusterSecret>,
) -> (MasterHandle, SocketAddr) {
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_implicit_local_dashboard(true)
        .with_auth_config(auth);
    if let Some(secret) = secret {
        config = config.with_cluster_secret(secret);
    }
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let addr = master.dashboard_addr().expect("dashboard addr");
    (master, addr)
}

#[tokio::test]
async fn test_a_bare_loopback_master_has_a_usable_local_dashboard() {
    let (master, addr) = spawn_bare_master(strict(), None).await;
    assert_eq!(master.dashboard_dev_bypass(), DevBypass::Implicit);
    assert!(addr.ip().is_loopback());

    let client = http_client();
    let base = format!("http://{addr}");
    for path in ["/api/status", "/api/tasks", "/api/workers"] {
        let resp = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "GET {path} needs no login on a bare loopback master"
        );
    }
    assert_eq!(post_task(addr, &[]).await, 200);
    assert_eq!(task_count(&master).await, 1);

    // Removed POST /api/chat route returns 404 Not Found without a token on a local dashboard (SEC-14).
    let resp = client
        .post(format!("{base}/api/chat"))
        .json(&serde_json::json!({ "message": "hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "POST /api/chat without token must return 404"
    );

    // The implicit bypass is the dev bypass: every browser-origin check still applies.
    assert_eq!(
        post_task(addr, &[("Origin", "https://evil.example")]).await,
        401
    );
    assert_eq!(post_task(addr, &[("Host", "evil.example")]).await, 401);
    assert_eq!(
        post_task(addr, &[("X-Forwarded-For", "203.0.113.9")]).await,
        401
    );
    assert_eq!(
        post_task(addr, &[("Sec-Fetch-Site", "cross-site")]).await,
        401
    );
    assert_eq!(
        task_count(&master).await,
        1,
        "none of the refused requests may have run anything"
    );

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_a_secret_or_a_token_switches_the_implicit_local_access_off() {
    let client = http_client();

    let secret = ClusterSecret::from_bytes(&[0x51; 32]).unwrap();
    let (with_secret, addr) = spawn_bare_master(strict(), Some(secret.clone())).await;
    assert_eq!(with_secret.dashboard_dev_bypass(), DevBypass::Off);
    let resp = client
        .get(format!("http://{addr}/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = client
        .get(format!("http://{addr}/api/status"))
        .bearer_auth(dashboard_admin_token(&secret))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let tokened = strict().with_token("ops-token", "ops", &[AuthScope::Observer]);
    let (with_token, addr) = spawn_bare_master(tokened, None).await;
    assert_eq!(with_token.dashboard_dev_bypass(), DevBypass::Off);
    let resp = client
        .get(format!("http://{addr}/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // The library default does not turn it on by itself either.
    let (library_default, addr) = spawn_dashboard(strict(), None).await;
    assert_eq!(library_default.dashboard_dev_bypass(), DevBypass::Off);
    let resp = client
        .get(format!("http://{addr}/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let _ = with_secret.shutdown();
    let _ = with_token.shutdown();
    let _ = library_default.shutdown();
}

#[tokio::test]
async fn test_dev_bypass_is_never_honoured_on_a_non_loopback_dashboard() {
    // The dashboard listens on every interface (an explicit choice), and dev mode is on.
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_bind_ip("0.0.0.0".parse().unwrap())
        .with_dashboard_port(0)
        .with_implicit_local_dashboard(true)
        .with_auth_config(AuthConfig::dev().with_token("ops-token", "ops", &[AuthScope::Admin]));
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let bound = master.dashboard_addr().expect("dashboard addr");
    assert!(bound.ip().is_unspecified(), "{bound}");
    assert_eq!(master.dashboard_dev_bypass(), DevBypass::RefusedNonLoopback);

    // A perfectly local, same-origin, header-clean request is still just a stranger.
    let local = SocketAddr::from(([127, 0, 0, 1], bound.port()));
    assert_eq!(post_task(local, &[]).await, 401);
    let resp = http_client()
        .get(format!("http://{local}/api/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(task_count(&master).await, 0);

    // A real credential still works there.
    let resp = http_client()
        .get(format!("http://{local}/api/status"))
        .bearer_auth("ops-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_the_dashboard_stays_on_loopback_when_the_grid_listens_on_a_lan_address() {
    let lan_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
    if lan_ip.is_loopback() {
        eprintln!("no non-loopback interface on this machine; nothing to check");
        return;
    }
    let config = ServerConfig::new(SocketAddr::new(lan_ip, 0)).with_dashboard_port(0);
    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dashboard = master.dashboard_addr().expect("dashboard addr");
    assert!(
        dashboard.ip().is_loopback(),
        "the grid listens on {lan_ip}, but the dashboard must not follow it there: {dashboard}"
    );

    // Naming the interface is how it gets published, and then it is not loopback.
    let explicit = ServerConfig::new(SocketAddr::new(lan_ip, 0))
        .with_dashboard_bind_ip(lan_ip)
        .with_dashboard_port(0);
    let master2 = MasterServer::spawn(explicit).await.expect("spawn master");
    assert_eq!(master2.dashboard_addr().unwrap().ip(), lan_ip);

    let _ = master.shutdown();
    let _ = master2.shutdown();
}

#[tokio::test]
async fn test_anonymous_rendezvous_lease_response_has_no_ticket() {
    let secret = ClusterSecret::from_bytes(&[0x42; 32]).unwrap();
    let admin_token = dashboard_admin_token(&secret);
    let auth = strict().with_cluster_secret(&secret);
    let (master, addr) = spawn_dashboard(auth, Some(secret.clone())).await;
    let base = format!("http://{addr}");
    let client = http_client();

    let mut lease = rusty_grid_core::discovery::RendezvousLease::new(
        master.server_addr().to_string(),
        base.clone(),
        "sec7-master",
        Some("iroh-ticket-should-be-redacted".to_string()),
        1,
        120,
    );
    lease.sign(&secret);

    let reg = client
        .post(format!("{base}/api/rendezvous/register"))
        .json(&lease)
        .send()
        .await
        .expect("register lease");
    assert_eq!(reg.status(), StatusCode::OK);

    // Anonymous read: succeeds (public route) and verifies, but p2p_ticket is redacted.
    let anon_resp = client
        .get(format!("{base}/api/rendezvous/master"))
        .send()
        .await
        .expect("anonymous get lease");
    assert_eq!(anon_resp.status(), StatusCode::OK);
    let anon_body = anon_resp.text().await.expect("anonymous lease body");
    assert!(
        !anon_body.contains("p2p_ticket") && !anon_body.contains("iroh-ticket-should-be-redacted"),
        "anonymous lease JSON must not expose p2p_ticket: {anon_body}"
    );
    let parsed_anon: rusty_grid_core::discovery::RendezvousLease =
        serde_json::from_str(&anon_body).expect("deserialize anonymous lease");
    assert_eq!(parsed_anon.p2p_ticket, None);
    assert!(
        parsed_anon.verify(&secret, 30),
        "redacting p2p_ticket must keep the lease HMAC valid"
    );

    // Request with a wrong token is treated as unauthenticated: p2p_ticket stays redacted.
    let wrong_resp = client
        .get(format!("{base}/api/rendezvous/master"))
        .bearer_auth("wrong-dashboard-token")
        .send()
        .await
        .expect("wrong-token get lease");
    assert_eq!(wrong_resp.status(), StatusCode::OK);
    let wrong_body = wrong_resp.text().await.expect("wrong-token lease body");
    assert!(
        !wrong_body.contains("p2p_ticket")
            && !wrong_body.contains("iroh-ticket-should-be-redacted"),
        "wrong-token lease JSON must not expose p2p_ticket: {wrong_body}"
    );
    let parsed_wrong: rusty_grid_core::discovery::RendezvousLease =
        serde_json::from_str(&wrong_body).expect("deserialize wrong-token lease");
    assert_eq!(parsed_wrong.p2p_ticket, None);
    assert!(
        parsed_wrong.verify(&secret, 30),
        "redacting p2p_ticket for wrong-token caller must keep the lease HMAC valid"
    );

    // Authenticated read with a valid token keeps p2p_ticket.
    let auth_resp = client
        .get(format!("{base}/api/rendezvous/master"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("authenticated get lease");
    assert_eq!(auth_resp.status(), StatusCode::OK);
    let parsed_auth: rusty_grid_core::discovery::RendezvousLease = auth_resp
        .json()
        .await
        .expect("deserialize authenticated lease");
    assert_eq!(
        parsed_auth.p2p_ticket.as_deref(),
        Some("iroh-ticket-should-be-redacted")
    );

    let _ = master.shutdown();
}

// ---------------------------------------------------------------------------------------------
// 7. Member & Invite Administration RBAC, CORS DELETE, Secret Redaction & XSS Safety (ENROLL-1d)
// ---------------------------------------------------------------------------------------------

async fn spawn_v3_security_dashboard(
    auth: AuthConfig,
    secret: Option<ClusterSecret>,
) -> (tempfile::TempDir, MasterHandle, SocketAddr) {
    let dir = tempfile::tempdir().expect("tempdir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let master_kp = NodeKeypair::generate().unwrap();
    let members = MemberStore::load_from_dir(Some(dir.path())).unwrap();
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_state_dir(dir.path().to_path_buf())
        .with_auth_mode(AuthMode::Members)
        .with_ephemeral_key(false)
        .with_master_noise_key(master_kp)
        .with_members(members)
        .with_dashboard_port(0)
        .with_auth_config(auth);
    config.enable_discovery = false;
    #[cfg(feature = "p2p")]
    {
        config.enable_p2p = false;
    }
    if let Some(secret) = secret {
        config = config.with_cluster_secret(secret);
    }
    let master = MasterServer::spawn(config).await.expect("spawn v3 master");
    let addr = master.dashboard_addr().expect("dashboard addr");
    (dir, master, addr)
}

#[tokio::test]
async fn test_members_and_invites_require_admin_scope_and_never_leak_secrets() {
    let secret = ClusterSecret::from_bytes(&[0x71; 32]).unwrap();
    let admin_token = dashboard_admin_token(&secret);
    let auth = strict()
        .with_token("obs-tok", "obs", &[AuthScope::Observer])
        .with_token("sub-tok", "sub", &[AuthScope::Submitter])
        .with_token("wrk-tok", "wrk", &[AuthScope::Worker]);
    let (_dir, master, addr) = spawn_v3_security_dashboard(auth, Some(secret.clone())).await;
    let base = format!("http://{addr}");
    let client = http_client();

    let member_routes = [
        (Method::GET, "/api/members", "{}"),
        (Method::GET, "/api/invites", "{}"),
        (Method::POST, "/api/invites", r#"{"roles":["worker"]}"#),
        (Method::DELETE, "/api/invites/nonexistent", "{}"),
        (
            Method::POST,
            "/api/members/nonexistent/approve",
            r#"{"pairing_code":"123456"}"#,
        ),
        (Method::POST, "/api/members/nonexistent/reject", "{}"),
        (Method::POST, "/api/members/nonexistent/revoke", "{}"),
        (
            Method::POST,
            "/api/members/nonexistent/rename",
            r#"{"name":"newname"}"#,
        ),
    ];

    // Observer, Submitter, and Worker tokens are all denied with 403 Forbidden (never fall through
    // GET catch-all to Observer!).
    for tok in ["obs-tok", "sub-tok", "wrk-tok"] {
        for (method, path, body) in &member_routes {
            let resp = client
                .request(method.clone(), format!("{base}{path}"))
                .bearer_auth(tok)
                .header("content-type", "application/json")
                .body(*body)
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "{tok} {method} {path} must return 403"
            );
            assert_eq!(resp.headers()["x-content-type-options"], "nosniff");
            assert_eq!(resp.headers()["x-frame-options"], "DENY");
            assert_eq!(resp.headers()["referrer-policy"], "no-referrer");
        }
    }

    // Raw cluster secret is rejected with 401 Unauthorized.
    let raw_secret_resp = client
        .get(format!("{base}/api/members"))
        .bearer_auth(secret.to_hex())
        .send()
        .await
        .unwrap();
    assert_eq!(raw_secret_resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        raw_secret_resp.headers()["x-content-type-options"],
        "nosniff"
    );

    // Derived Admin token succeeds (200) on /api/invites and /api/members, and 404 keeps security
    // headers.
    let create_inv = client
        .post(format!("{base}/api/invites"))
        .bearer_auth(&admin_token)
        .json(&serde_json::json!({ "roles": ["worker"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_inv.status(), StatusCode::OK);
    assert_eq!(create_inv.headers()["x-content-type-options"], "nosniff");
    assert_eq!(create_inv.headers()["x-frame-options"], "DENY");
    assert_eq!(create_inv.headers()["referrer-policy"], "no-referrer");
    let created: serde_json::Value = create_inv.json().await.unwrap();
    let inv_id = created["invite_id"].as_str().unwrap().to_string();
    assert!(created["invite"]
        .as_str()
        .unwrap()
        .starts_with("oxs1-invite:"));

    let get_inv = client
        .get(format!("{base}/api/invites"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(get_inv.status(), StatusCode::OK);
    let get_inv_body = get_inv.text().await.unwrap();
    assert!(!get_inv_body.contains("invite_secret"));
    assert!(!get_inv_body.contains("oxs1-invite:"));

    let get_mem = client
        .get(format!("{base}/api/members"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(get_mem.status(), StatusCode::OK);
    assert_eq!(get_mem.headers()["x-content-type-options"], "nosniff");
    let get_mem_body = get_mem.text().await.unwrap();
    assert!(!get_mem_body.contains("pairing_code"));
    assert!(!get_mem_body.contains("invite_secret"));

    let del_inv = client
        .delete(format!("{base}/api/invites/{inv_id}"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(del_inv.status(), StatusCode::OK);

    let missing_del = client
        .delete(format!("{base}/api/invites/deadbeef"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(missing_del.status(), StatusCode::NOT_FOUND);
    assert_eq!(missing_del.headers()["x-content-type-options"], "nosniff");
    assert_eq!(missing_del.headers()["x-frame-options"], "DENY");
    assert_eq!(missing_del.headers()["referrer-policy"], "no-referrer");

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_members_and_invites_cross_origin_dev_bypass_and_cors_delete() {
    // 1. In dev mode, cross-origin/spoofed-Host/proxy/cross-site requests to member/invite
    //    endpoints are denied with 401.
    let (_dir_dev, dev_master, dev_addr) =
        spawn_v3_security_dashboard(AuthConfig::dev(), None).await;
    let port = dev_addr.port();
    let bad_headers: Vec<[(&str, String); 1]> = vec![
        [("Origin", "https://evil.example".to_string())],
        [("Host", format!("evil.example:{port}"))],
        [("X-Forwarded-For", "203.0.113.9".to_string())],
        [("Sec-Fetch-Site", "cross-site".to_string())],
    ];
    for hdrs in &bad_headers {
        let borrowed: Vec<(&str, &str)> = hdrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
        for (method, path, body) in [
            ("GET", "/api/members", None),
            ("POST", "/api/invites", Some("{}")),
            ("DELETE", "/api/invites/abc", None),
            ("POST", "/api/members/abc/revoke", Some("{}")),
        ] {
            let resp = raw_http(dev_addr, method, path, &borrowed, body).await;
            assert_eq!(
                resp.status, 401,
                "{method} {path} with {:?} must be denied",
                hdrs
            );
        }
    }
    let _ = dev_master.shutdown();

    // 2. Allow-listed CORS origin gets Access-Control-Allow-Methods including DELETE on preflight;
    //    evil origin gets none.
    let cors_auth = strict()
        .with_token("cors-adm", "admin", &[AuthScope::Admin])
        .with_allowed_origin("https://ops.example.com");
    let (_dir_cors, cors_master, cors_addr) = spawn_v3_security_dashboard(cors_auth, None).await;

    let ok_preflight = raw_http(
        cors_addr,
        "OPTIONS",
        "/api/invites/abc",
        &[
            ("Origin", "https://ops.example.com"),
            ("Access-Control-Request-Method", "DELETE"),
            ("Access-Control-Request-Headers", "authorization"),
        ],
        None,
    )
    .await;
    assert_eq!(ok_preflight.status, 200);
    assert_eq!(
        ok_preflight.header("access-control-allow-origin"),
        Some("https://ops.example.com")
    );
    let allow_methods = ok_preflight
        .header("access-control-allow-methods")
        .unwrap_or("");
    assert!(
        allow_methods.to_ascii_uppercase().contains("DELETE"),
        "CORS preflight must allow DELETE: {allow_methods:?}"
    );

    let evil_preflight = raw_http(
        cors_addr,
        "OPTIONS",
        "/api/invites/abc",
        &[
            ("Origin", "https://evil.example"),
            ("Access-Control-Request-Method", "DELETE"),
        ],
        None,
    )
    .await;
    assert!(evil_preflight
        .header("access-control-allow-origin")
        .is_none());

    let _ = cors_master.shutdown();
}

#[tokio::test]
async fn test_member_xss_payload_roundtrip_and_dashboard_html_safety() {
    let auth = strict().with_token("adm", "admin", &[AuthScope::Admin]);
    let (_dir, master, addr) = spawn_v3_security_dashboard(auth, None).await;
    let client = http_client();
    let base = format!("http://{addr}");

    let kp = NodeKeypair::generate().unwrap();
    let xss_name = "<img src=x onerror=alert(1)>";
    let inv = master
        .members()
        .create_invite(vec![MemberRole::Worker], 1, 3600, false)
        .unwrap();
    let h = [0x33u8; 32];
    let proof = compute_enroll_proof(&inv.invite_secret.0, &h);
    master
        .members()
        .handle_enroll_request(
            *kp.public_key(),
            &h,
            "1111-2222-3333-4444",
            "127.0.0.1:44000",
            &inv.invite_id,
            &proof,
            xss_name.to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
        )
        .unwrap();

    let resp = client
        .get(format!("{base}/api/members"))
        .bearer_auth("adm")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["members"][0]["name"], xss_name);

    // Served SPA HTML must use safe escaping + setHtml + delegated data-action/data-id only.
    let html = client
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("function setHtml("));
    assert!(html.contains("function escapeHtml("));
    assert!(html.contains("data-action=\"revoke-member\""));
    assert!(html.contains("data-action=\"approve-pending\""));
    assert_eq!(html.matches(".innerHTML").count(), 1);

    let _ = master.shutdown();
}
