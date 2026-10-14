//! Dashboard hardening tests (OXS-AUTH design, section 4 and 5).
//!
//! Validates, against a real `MasterServer` dashboard listener:
//! 1. Deny by default: with no credentials configured every route (REST, `/ws`, `/api/stream`,
//!    SSE, POST/PUT/DELETE, unknown paths) answers 401 and nothing is queued; only the static SPA
//!    shell (`GET /`) is public.
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

use rusty_grid_core::ClusterSecret;
use rusty_grid_master::auth::{dashboard_admin_token, AuthConfig, AuthScope, DevBypass};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};

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
        (Method::GET, "/api/stream/sse".into()),
        (Method::GET, "/ws".into()),
        (Method::GET, "/api/stream".into()),
        (Method::HEAD, "/api/status".into()),
        (Method::POST, "/api/tasks".into()),
        (Method::POST, "/api/chat".into()),
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
            .body(r#"{"command":"touch /tmp/oxs-should-never-run","message":"hi","mode":"doc"}"#)
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
            Some("{}").filter(|_| method == "POST"),
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
