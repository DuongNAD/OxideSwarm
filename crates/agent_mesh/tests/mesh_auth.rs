//! Access-control tests for the Agent Mesh hub ("OXS-AUTH v1", section 4):
//! token authentication, `from` enforcement, duplicate node ids, per-socket cleanup, bounded
//! outbound queues, the browser-Origin check and the non-loopback bind policy.
//!
//! Attacks are mounted over real WebSockets; wherever a command could run, the test uses a
//! side-effect file (or the node's executed-command counter) to prove that nothing ran.

use agent_mesh::{
    AgentMeshClient, AgentMeshEnvelope, AgentMeshHub, HubConfig, RegistrationRejected,
    ERR_DUPLICATE_NODE_ID, ERR_INVALID_NODE_ID, ERR_NOT_REGISTERED, ERR_SENDER_MISMATCH,
    ERR_UNAUTHORIZED,
};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const TOKEN: &str = "mesh-test-token-0123456789abcdef";

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn start_hub(config: HubConfig) -> (AgentMeshHub, String, u16) {
    let hub = AgentMeshHub::with_config(config);
    let port = hub.start("127.0.0.1:0").await.expect("hub failed to start");
    (hub, format!("ws://127.0.0.1:{port}/ws"), port)
}

fn token_config() -> HubConfig {
    HubConfig::default().with_auth_token(TOKEN)
}

fn client(url: &str, id: &str) -> AgentMeshClient {
    AgentMeshClient::new(url, id, "test-os")
}

fn authed_client(url: &str, id: &str) -> AgentMeshClient {
    client(url, id).with_token(TOKEN)
}

/// What a raw socket saw next.
#[derive(Debug)]
enum Frame {
    Json(Value),
    Closed,
    Silence,
}

/// A hand-driven WebSocket peer, so tests can send things the real client never would.
struct Raw {
    ws: Ws,
}

impl Raw {
    async fn connect(url: &str) -> Raw {
        let (ws, _) = connect_async(url).await.expect("raw websocket connect failed");
        Raw { ws }
    }

    async fn send(&mut self, value: Value) {
        // A peer the hub already hung up on may fail to write; that is not what the test checks.
        let _ = self.ws.send(Message::Text(value.to_string())).await;
    }

    async fn next(&mut self, within: Duration) -> Frame {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(left, self.ws.next()).await {
                Err(_) => return Frame::Silence,
                Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => return Frame::Closed,
                Ok(Some(Ok(Message::Text(text)))) => {
                    return Frame::Json(serde_json::from_str(&text).expect("hub sent invalid JSON"))
                }
                Ok(Some(Ok(_))) => continue, // ping / pong / binary
            }
        }
    }

    async fn expect_json(&mut self) -> Value {
        match self.next(Duration::from_secs(5)).await {
            Frame::Json(v) => v,
            other => panic!("expected a JSON frame, got {other:?}"),
        }
    }

    async fn expect_closed(&mut self) {
        // Discard whatever the hub flushed before the close frame.
        loop {
            match self.next(Duration::from_secs(5)).await {
                Frame::Closed => return,
                Frame::Json(_) => continue,
                Frame::Silence => panic!("the hub did not close the connection"),
            }
        }
    }

    /// Registers `id` and returns the hub's answer (ack or NACK).
    async fn register(&mut self, id: &str, token: Option<&str>) -> Value {
        let mut reg = json!({
            "version": "1.0",
            "correlation_id": uuid::Uuid::new_v4().to_string(),
            "type": "NodeRegistration",
            "from": id,
            "to": "hub",
            "platform": "raw",
            "hostname": "raw-host",
        });
        if let Some(t) = token {
            reg["token"] = json!(t);
        }
        self.send(reg).await;
        self.expect_json().await
    }

    async fn registered(url: &str, id: &str, token: Option<&str>) -> Raw {
        let mut raw = Raw::connect(url).await;
        let answer = raw.register(id, token).await;
        assert_eq!(answer["type"], "NodeRegistrationAck", "registration of {id} failed: {answer}");
        raw
    }
}

fn command(from: &str, to: &str, cmd: &str, args: Value) -> Value {
    json!({
        "version": "1.0",
        "correlation_id": uuid::Uuid::new_v4().to_string(),
        "type": "CommandRequest",
        "from": from,
        "to": to,
        "command": cmd,
        "args": args,
        "require_ack": true,
    })
}

fn touch_command(marker: &Path) -> Value {
    json!({ "cmd": format!("touch '{}'", marker.display()) })
}

fn rejection(err: Box<dyn std::error::Error + Send + Sync>) -> RegistrationRejected {
    err.downcast_ref::<RegistrationRejected>()
        .unwrap_or_else(|| panic!("expected RegistrationRejected, got: {err}"))
        .clone()
}

async fn wait_until<F, Fut>(what: &str, within: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn node_ids(hub: &AgentMeshHub) -> Vec<String> {
    let mut ids: Vec<String> = hub.state().nodes.read().await.keys().cloned().collect();
    ids.sort();
    ids
}

fn marker_path(dir: &tempfile::TempDir, name: &str) -> PathBuf {
    dir.path().join(name)
}

// =========================================================================
// Authentication
// =========================================================================

#[tokio::test]
async fn test_registration_without_or_with_wrong_token_is_rejected() {
    let (hub, url, _) = start_hub(token_config()).await;

    let no_token = rejection(client(&url, "intruder").connect().await.expect_err("no token must be rejected"));
    assert_eq!(no_token.error_code, ERR_UNAUTHORIZED);

    let wrong = rejection(
        client(&url, "intruder")
            .with_token("not-the-token")
            .connect()
            .await
            .expect_err("wrong token must be rejected"),
    );
    assert_eq!(wrong.error_code, ERR_UNAUTHORIZED);
    // The reason must not help an attacker, and above all must not echo the real token.
    assert!(!no_token.reason.contains(TOKEN) && !wrong.reason.contains(TOKEN));

    // Both rejections are final: the socket is closed and nothing was registered.
    let mut raw = Raw::connect(&url).await;
    let answer = raw.register("intruder", Some("still-wrong")).await;
    assert_eq!(answer["type"], "DeliveryNack");
    assert_eq!(answer["error_code"], ERR_UNAUTHORIZED);
    raw.expect_closed().await;
    assert!(node_ids(&hub).await.is_empty(), "a rejected node must not appear in the registry");

    // The right token works.
    let ok = authed_client(&url, "legit");
    ok.connect().await.expect("matching token must be accepted");
    assert_eq!(node_ids(&hub).await, vec!["legit".to_string()]);

    ok.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_unauthenticated_socket_cannot_run_commands_on_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let (hub, url, _) = start_hub(token_config()).await;

    let victim = authed_client(&url, "victim");
    victim.connect().await.expect("victim connect");

    // 1. Never registers, just fires a shell command at the victim.
    let marker = marker_path(&dir, "never-registered");
    let mut raw = Raw::connect(&url).await;
    raw.send(command("attacker", "victim", "shell_exec", touch_command(&marker))).await;
    let answer = raw.expect_json().await;
    assert_eq!(answer["type"], "DeliveryNack");
    assert_eq!(answer["error_code"], ERR_NOT_REGISTERED);
    raw.expect_closed().await;

    // 2. Registers with a wrong token and pipelines the command right behind it.
    let marker2 = marker_path(&dir, "wrong-token-pipelined");
    let mut raw = Raw::connect(&url).await;
    raw.send(json!({
        "version": "1.0", "correlation_id": "r1", "type": "NodeRegistration",
        "from": "attacker", "to": "hub", "token": "guess"
    }))
    .await;
    raw.send(command("attacker", "victim", "shell_exec", touch_command(&marker2))).await;
    raw.expect_closed().await;

    // 3. Spoofs the victim's own identity without any registration.
    let marker3 = marker_path(&dir, "spoofed-victim");
    let mut raw = Raw::connect(&url).await;
    raw.send(command("victim", "victim", "shell_exec", touch_command(&marker3))).await;
    raw.expect_closed().await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    for m in [&marker, &marker2, &marker3] {
        assert!(!m.exists(), "an unauthenticated command ran: {}", m.display());
    }
    assert_eq!(victim.executed_commands_count(), 0, "the victim executed an unauthenticated command");
    assert_eq!(node_ids(&hub).await, vec!["victim".to_string()]);

    victim.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_pre_registration_frames_are_constrained() {
    let mut config = token_config();
    config.registration_timeout = Duration::from_millis(400);
    let (hub, url, _) = start_hub(config).await;

    // A socket that stays silent is dropped.
    let mut silent = Raw::connect(&url).await;
    silent.expect_closed().await;

    // So is one whose first frame is garbage...
    let mut garbage = Raw::connect(&url).await;
    garbage.ws.send(Message::Text("this is not json".into())).await.unwrap();
    garbage.expect_closed().await;

    // ...or absurdly large before it proved anything.
    let mut oversized = Raw::connect(&url).await;
    let _ = oversized
        .ws
        .send(Message::Text(json!({ "type": "NodeRegistration", "from": "big", "pad": "x".repeat(200_000) }).to_string()))
        .await;
    oversized.expect_closed().await;

    assert!(node_ids(&hub).await.is_empty());
    hub.stop().await;
}

#[tokio::test]
async fn test_token_less_legacy_registration_still_works_on_a_loopback_hub_without_token() {
    let (hub, url, _) = start_hub(HubConfig::default()).await;

    // Exactly what a pre-token client sends: no `token` member at all.
    let mut raw = Raw::connect(&url).await;
    raw.send(json!({"type": "NodeRegistration", "from": "old-timer", "platform": "linux"})).await;
    let answer = raw.expect_json().await;
    assert_eq!(answer["type"], "NodeRegistrationAck");
    assert_eq!(answer["status"], "accepted");

    // A token sent to a hub that does not need one is simply ignored.
    let tokened = client(&url, "has-token").with_token("whatever");
    tokened.connect().await.expect("a hub without a token accepts any registration");

    tokened.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_rest_endpoints_need_the_token_and_send_no_cors_headers() {
    let (hub, _url, port) = start_hub(token_config()).await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    for path in ["/api/nodes", "/api/status"] {
        let anon = http.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(anon.status(), 401, "{path} must require the token");
        let wrong = http.get(format!("{base}{path}")).bearer_auth("nope").send().await.unwrap();
        assert_eq!(wrong.status(), 401);
        let ok = http.get(format!("{base}{path}")).bearer_auth(TOKEN).send().await.unwrap();
        assert_eq!(ok.status(), 200);
    }
    // Liveness probes stay open.
    for path in ["/api/health", "/health"] {
        assert_eq!(http.get(format!("{base}{path}")).send().await.unwrap().status(), 200);
    }

    // No permissive CORS: a page on another origin must not be allowed to read any response.
    for path in ["/api/health", "/api/nodes"] {
        let resp = http
            .get(format!("{base}{path}"))
            .bearer_auth(TOKEN)
            .header("Origin", "https://evil.example")
            .send()
            .await
            .unwrap();
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "{path} answers cross-origin readers"
        );
    }
    let preflight = http
        .request(reqwest::Method::OPTIONS, format!("{base}/api/nodes"))
        .header("Origin", "https://evil.example")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert!(preflight.headers().get("access-control-allow-origin").is_none());

    hub.stop().await;
}

// =========================================================================
// Browser Origin check
// =========================================================================

#[allow(clippy::result_large_err)]
async fn connect_with_origin(url: &str, origin: &str) -> Result<Ws, WsError> {
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert("Origin", HeaderValue::from_str(origin).unwrap());
    connect_async(request).await.map(|(ws, _)| ws)
}

#[tokio::test]
async fn test_foreign_browser_origin_is_refused_on_upgrade() {
    for config in [HubConfig::default(), token_config()] {
        let (hub, url, _) = start_hub(config).await;

        for evil in ["https://evil.example", "http://192.168.1.50:8080", "null", "http://localhost.evil.example"] {
            match connect_with_origin(&url, evil).await {
                Err(WsError::Http(resp)) => assert_eq!(resp.status(), 403, "Origin {evil:?}"),
                Err(other) => panic!("Origin {evil:?}: unexpected error {other}"),
                Ok(_) => panic!("Origin {evil:?} was allowed to open a socket"),
            }
        }
        assert!(node_ids(&hub).await.is_empty());

        // Loopback origins (a locally served tool) and native clients without Origin still work.
        for ok in ["http://localhost:5173", "http://127.0.0.1:3000", "http://[::1]:8080"] {
            connect_with_origin(&url, ok)
                .await
                .unwrap_or_else(|e| panic!("loopback Origin {ok:?} must be allowed: {e}"));
        }
        Raw::connect(&url).await;

        hub.stop().await;
    }
}

// =========================================================================
// Identity: duplicate ids, forged `from`, per-socket cleanup
// =========================================================================

#[tokio::test]
async fn test_duplicate_node_id_is_nacked_and_the_original_keeps_its_route() {
    let (hub, url, _) = start_hub(HubConfig::default()).await;

    let original = client(&url, "dup-node");
    original.connect().await.expect("first registration");
    let caller = client(&url, "dup-caller");
    caller.connect().await.expect("caller");

    let usurper = client(&url, "dup-node");
    let rej = rejection(usurper.connect().await.expect_err("second registration of a live id must fail"));
    assert_eq!(rej.error_code, ERR_DUPLICATE_NODE_ID);
    assert!(rej.reason.contains("dup-node"));

    // The usurper's socket was dropped; that must not evict the original (the old bug deleted the
    // newer session when the older socket closed, and the other way round).
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(node_ids(&hub).await, vec!["dup-caller".to_string(), "dup-node".to_string()]);

    let res = caller
        .send_command("dup-node", "echo", json!({"message": "still mine"}), Some(3000))
        .await
        .expect("command to the original");
    match res {
        AgentMeshEnvelope::CommandResponse { from, stdout, .. } => {
            assert_eq!(from, "dup-node");
            assert_eq!(stdout, "still mine");
        }
        other => panic!("expected CommandResponse, got {other:?}"),
    }
    assert_eq!(original.executed_commands_count(), 1);
    assert_eq!(usurper.executed_commands_count(), 0);

    // Once the owner is gone the id is free again.
    original.close().await;
    wait_until("dup-node to be released", Duration::from_secs(3), || async {
        !node_ids(&hub).await.contains(&"dup-node".to_string())
    })
    .await;
    let successor = client(&url, "dup-node");
    successor.connect().await.expect("a released id can be registered again");

    successor.close().await;
    caller.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_duplicate_registration_on_a_raw_socket_and_idempotent_reregistration() {
    let (hub, url, _) = start_hub(HubConfig::default()).await;

    let mut owner = Raw::registered(&url, "twice", None).await;
    // The owner may repeat its own registration (reconnect logic that is not sure it succeeded).
    let again = owner.register("twice", None).await;
    assert_eq!(again["type"], "NodeRegistrationAck");

    let mut other = Raw::connect(&url).await;
    let nack = other.register("twice", None).await;
    assert_eq!(nack["type"], "DeliveryNack");
    assert_eq!(nack["error_code"], ERR_DUPLICATE_NODE_ID);
    other.expect_closed().await;

    assert_eq!(node_ids(&hub).await, vec!["twice".to_string()]);
    hub.stop().await;
}

#[tokio::test]
async fn test_forged_from_is_rejected_for_every_envelope_type() {
    let dir = tempfile::tempdir().unwrap();
    let (hub, url, _) = start_hub(token_config()).await;

    let alice = authed_client(&url, "alice");
    let bob = authed_client(&url, "bob");
    alice.connect().await.unwrap();
    bob.connect().await.unwrap();
    let mut mallory = Raw::registered(&url, "mallory", Some(TOKEN)).await;

    // Impersonating alice to make bob run a command.
    let marker = marker_path(&dir, "forged-command");
    mallory.send(command("alice", "bob", "shell_exec", touch_command(&marker))).await;
    let nack = mallory.expect_json().await;
    assert_eq!(nack["type"], "DeliveryNack");
    assert_eq!(nack["error_code"], ERR_SENDER_MISMATCH);
    assert_eq!(nack["to"], "mallory", "the NACK must reach the real sender, not the forged identity");

    // Every other envelope type is checked the same way.
    let corr = "forged-corr";
    let forgeries = [
        json!({"type": "Heartbeat", "correlation_id": corr, "from": "alice", "to": "hub"}),
        json!({"type": "NodeList", "correlation_id": corr, "from": "alice", "to": "hub"}),
        json!({"type": "Broadcast", "correlation_id": corr, "from": "alice", "to": "all", "event": "pwn"}),
        json!({"type": "DataPayload", "correlation_id": corr, "from": "alice", "to": "bob", "data": "x"}),
        json!({"type": "CommandResponse", "correlation_id": corr, "from": "bob", "to": "alice",
               "status": "success", "exit_code": 0, "stdout": "forged"}),
        json!({"type": "DeliveryAck", "correlation_id": corr, "from": "bob", "to": "alice", "status": "ok"}),
        json!({"type": "DeliveryNack", "correlation_id": corr, "from": "hub", "to": "alice",
               "error_code": "ERR_NODE_NOT_FOUND", "reason": "forged"}),
    ];
    for forged in forgeries {
        let kind = forged["type"].as_str().unwrap().to_string();
        mallory.send(forged).await;
        let answer = mallory.expect_json().await;
        assert_eq!(answer["error_code"], ERR_SENDER_MISMATCH, "forged {kind} was not rejected: {answer}");
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!marker.exists(), "a forged command was executed");
    assert_eq!(bob.executed_commands_count(), 0);
    assert!(bob.received_data_payloads().await.is_empty(), "a forged data payload reached bob");

    // Mallory may still use her own identity normally.
    mallory.send(command("mallory", "bob", "echo", json!({"message": "honest"}))).await;
    let forwarded = mallory.expect_json().await;
    assert_eq!(forwarded["type"], "DeliveryAck");
    wait_until("bob to run the honest command", Duration::from_secs(3), || async {
        bob.executed_commands_count() == 1
    })
    .await;

    alice.close().await;
    bob.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_forged_response_cannot_satisfy_someone_elses_pending_request() {
    let (hub, url, _) = start_hub(HubConfig::default()).await;

    let alice = std::sync::Arc::new(client(&url, "alice"));
    alice.connect().await.unwrap();
    let mut slow = Raw::registered(&url, "slow-node", None).await;
    let mut mallory = Raw::registered(&url, "mallory", None).await;

    let pending = {
        let alice = alice.clone();
        tokio::spawn(async move {
            alice.send_command("slow-node", "echo", json!({"message": "ignored"}), Some(5000)).await
        })
    };

    // The slow node receives alice's request (skipping the hub's "target_forwarded" ack to alice).
    let request = slow.expect_json().await;
    assert_eq!(request["type"], "CommandRequest");
    let corr = request["correlation_id"].as_str().unwrap().to_string();

    // Mallory learned the correlation id somehow and answers in the slow node's name.
    mallory
        .send(json!({
            "type": "CommandResponse", "correlation_id": corr, "from": "slow-node", "to": "alice",
            "status": "success", "exit_code": 0, "stdout": "FORGED"
        }))
        .await;
    let nack = mallory.expect_json().await;
    assert_eq!(nack["error_code"], ERR_SENDER_MISMATCH);

    // The genuine answer still resolves the request, with the genuine content.
    slow.send(json!({
        "type": "CommandResponse", "correlation_id": corr, "from": "slow-node", "to": "alice",
        "status": "success", "exit_code": 0, "stdout": "genuine"
    }))
    .await;
    match pending.await.unwrap().expect("alice's request") {
        AgentMeshEnvelope::CommandResponse { stdout, from, .. } => {
            assert_eq!(from, "slow-node");
            assert_eq!(stdout, "genuine");
        }
        other => panic!("expected the genuine CommandResponse, got {other:?}"),
    }

    alice.close().await;
    hub.stop().await;
}

#[tokio::test]
async fn test_reserved_and_malformed_node_ids_cannot_be_registered() {
    let (hub, url, _) = start_hub(token_config()).await;

    for bad in ["hub", "grid", "grid-master", "all", "", "line\nbreak", &"n".repeat(500)] {
        let mut raw = Raw::connect(&url).await;
        let answer = raw.register(bad, Some(TOKEN)).await;
        assert_eq!(answer["type"], "DeliveryNack", "id {bad:?} was accepted");
        assert_eq!(answer["error_code"], ERR_INVALID_NODE_ID, "id {bad:?}");
        raw.expect_closed().await;
    }
    assert!(node_ids(&hub).await.is_empty());
    hub.stop().await;
}

#[tokio::test]
async fn test_every_id_of_a_socket_is_released_when_it_disconnects() {
    let (hub, url, _) = start_hub(HubConfig::default()).await;
    let probe = client(&url, "probe");
    probe.connect().await.unwrap();

    // One socket legitimately serving three node ids.
    let mut multi = Raw::registered(&url, "multi-a", None).await;
    for id in ["multi-b", "multi-c"] {
        let ack = multi.register(id, None).await;
        assert_eq!(ack["type"], "NodeRegistrationAck", "{ack}");
    }
    assert_eq!(
        node_ids(&hub).await,
        vec!["multi-a", "multi-b", "multi-c", "probe"].into_iter().map(String::from).collect::<Vec<_>>()
    );

    // Each of its ids can speak, and only its own.
    multi.send(json!({"type": "Heartbeat", "from": "multi-c", "to": "hub", "correlation_id": "hb"})).await;
    assert_eq!(multi.expect_json().await["type"], "HeartbeatAck");

    drop(multi);
    wait_until("all ids of the socket to disappear", Duration::from_secs(5), || async {
        node_ids(&hub).await == vec!["probe".to_string()]
    })
    .await;

    // No phantom entries: a request to a released id is answered with a NACK, not swallowed.
    for id in ["multi-a", "multi-b", "multi-c"] {
        let res = probe.send_command(id, "ping", Value::Null, Some(2000)).await.expect("NACK expected");
        match res {
            AgentMeshEnvelope::DeliveryNack { error_code, .. } => assert_eq!(error_code, "ERR_NODE_NOT_FOUND"),
            other => panic!("{id}: expected DeliveryNack, got {other:?}"),
        }
    }

    probe.close().await;
    hub.stop().await;
}

// =========================================================================
// Liveness and bounded queues
// =========================================================================

#[tokio::test]
async fn test_half_open_node_is_dropped_so_its_id_can_be_reclaimed() {
    let config = HubConfig {
        ping_interval: Duration::from_millis(100),
        idle_timeout: Duration::from_millis(1500),
        ..Default::default()
    };
    let (hub, url, _) = start_hub(config).await;

    // "ghost" registers and then never reads again, so it never answers the hub's pings: this is
    // what a node that lost power or its network looks like to the hub.
    let ghost = Raw::registered(&url, "ghost", None).await;

    // While the ghost still looks alive its id is protected...
    let rej = rejection(client(&url, "ghost").connect().await.expect_err("live id must be protected"));
    assert_eq!(rej.error_code, ERR_DUPLICATE_NODE_ID);

    // ...but it does not hold the id forever.
    wait_until("the silent node to be dropped", Duration::from_secs(8), || async {
        node_ids(&hub).await.is_empty()
    })
    .await;
    let reborn = client(&url, "ghost");
    reborn.connect().await.expect("the id must be claimable after the ghost timed out");

    // A healthy node answers pings, so being idle for longer than the timeout does not drop it.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(node_ids(&hub).await, vec!["ghost".to_string()]);
    let peer = client(&url, "peer");
    peer.connect().await.unwrap();
    let res = peer.send_command("ghost", "ping", Value::Null, Some(3000)).await.unwrap();
    assert!(matches!(res, AgentMeshEnvelope::CommandResponse { .. }), "{res:?}");

    drop(ghost);
    reborn.close().await;
    peer.close().await;
    hub.stop().await;
}

#[tokio::test]
#[allow(clippy::manual_is_multiple_of)]
async fn test_slow_consumer_is_disconnected_instead_of_buffering_forever() {
    let config = HubConfig {
        max_queued_messages: 8,
        max_queued_bytes: 256 * 1024,
        ..Default::default()
    };
    let (hub, url, _) = start_hub(config).await;

    let healthy = client(&url, "healthy");
    healthy.connect().await.unwrap();
    // The victim registers and then stops reading entirely.
    let victim = Raw::registered(&url, "victim", None).await;
    let sender = client(&url, "flooder");
    sender.connect().await.unwrap();

    let chunk = vec![0x41u8; 100 * 1024];
    let mut sent = 0usize;
    let victim_gone = |hub: &AgentMeshHub| {
        let state = hub.state();
        async move { !state.nodes.read().await.contains_key("victim") }
    };
    while !victim_gone(&hub).await {
        assert!(sent < 2000, "the hub kept queueing {sent} x 100 KiB for a peer that reads nothing");
        sender.send_data_payload("victim", &chunk).await.expect("send");
        sent += 1;
        if sent % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    println!("victim disconnected after {sent} x 100 KiB sends (queue cap: 8 messages / 256 KiB)");

    // Everyone else is unaffected, and the dead peer's id answers with a NACK from now on.
    let res = sender.send_command("healthy", "ping", Value::Null, Some(3000)).await.unwrap();
    assert!(matches!(res, AgentMeshEnvelope::CommandResponse { .. }), "{res:?}");
    let res = sender.send_command("victim", "ping", Value::Null, Some(3000)).await.unwrap();
    assert!(matches!(res, AgentMeshEnvelope::DeliveryNack { .. }), "{res:?}");
    assert!(node_ids(&hub).await.contains(&"flooder".to_string()));

    drop(victim);
    sender.close().await;
    healthy.close().await;
    hub.stop().await;
}

// =========================================================================
// Bind policy (library + `agent-mesh hub` binary)
// =========================================================================

#[tokio::test]
async fn test_hub_refuses_public_bind_without_token() {
    let hub = AgentMeshHub::new();
    let err = hub.start("0.0.0.0:0").await.expect_err("public bind without token must fail");
    assert!(err.to_string().contains("token"), "{err}");

    let with_token = AgentMeshHub::with_config(token_config());
    with_token.start("0.0.0.0:0").await.expect("token allows a public bind");
    with_token.stop().await;

    let insecure = AgentMeshHub::with_config(HubConfig::default().with_insecure_bind(true));
    insecure.start("0.0.0.0:0").await.expect("explicit opt-out allows a public bind");
    insecure.stop().await;
}

mod cli {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::{Child, Command};

    const BIN: &str = env!("CARGO_BIN_EXE_agent-mesh");

    fn base(args: &[&str]) -> Command {
        let mut cmd = Command::new(BIN);
        cmd.args(args).env_remove("OXIDE_MESH_TOKEN").kill_on_drop(true);
        cmd
    }

    /// Starts `agent-mesh hub ...` and returns it with the `ws://` URL it reported.
    async fn spawn_hub(args: &[&str], env_token: Option<&str>) -> (Child, String) {
        let mut cmd = base(args);
        if let Some(t) = env_token {
            cmd.env("OXIDE_MESH_TOKEN", t);
        }
        let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().expect("spawn agent-mesh hub");
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let url = tokio::time::timeout(Duration::from_secs(15), async {
            while let Some(line) = lines.next_line().await.expect("hub stdout") {
                if let Some(rest) = line.split("running at ").nth(1) {
                    return rest.trim().to_string();
                }
            }
            panic!("the hub exited before reporting its address");
        })
        .await
        .expect("hub did not start in time");
        // Keep draining so the hub never blocks on a full stdout pipe.
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        (child, url)
    }

    #[tokio::test]
    async fn test_hub_binary_refuses_public_bind_without_token() {
        let out = base(&["hub", "--listen", "0.0.0.0:0"])
            .stdin(Stdio::null())
            .output()
            .await
            .expect("run agent-mesh");
        assert!(!out.status.success(), "a public, unauthenticated hub must not start");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("--token") && stderr.contains("--insecure"), "unhelpful error: {stderr}");
    }

    #[tokio::test]
    async fn test_hub_binary_insecure_flag_allows_public_bind() {
        let (mut hub, url) = spawn_hub(&["hub", "--listen", "0.0.0.0:0", "--insecure"], None).await;
        assert!(url.starts_with("ws://"), "{url}");
        let _ = hub.kill().await;
    }

    #[tokio::test]
    async fn test_hub_binary_rejects_an_empty_token() {
        let out = base(&["hub", "--listen", "127.0.0.1:0", "--token", ""])
            .stdin(Stdio::null())
            .output()
            .await
            .expect("run agent-mesh");
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("empty"));
    }

    #[tokio::test]
    async fn test_cli_subcommands_take_the_token_from_flag_or_environment() {
        // The hub itself is configured from OXIDE_MESH_TOKEN.
        let (mut hub, url) = spawn_hub(&["hub", "--listen", "127.0.0.1:0"], Some("cli-secret-token-0123456789")).await;

        // No token: refused with an actionable message.
        let out = base(&["list", "--hub", &url]).stdin(Stdio::null()).output().await.unwrap();
        assert!(!out.status.success(), "list without a token must fail");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("ERR_UNAUTHORIZED"), "unhelpful error: {stderr}");

        // Wrong token via the flag.
        let out = base(&["list", "--hub", &url, "--token", "wrong"]).stdin(Stdio::null()).output().await.unwrap();
        assert!(!out.status.success());

        // Right token via the flag.
        let out = base(&["list", "--hub", &url, "--token", "cli-secret-token-0123456789"])
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        assert!(String::from_utf8_lossy(&out.stdout).contains("Connected Nodes"));

        // Right token via the environment.
        let out = base(&["list", "--hub", &url])
            .env("OXIDE_MESH_TOKEN", "cli-secret-token-0123456789")
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

        let _ = hub.kill().await;
    }
}
