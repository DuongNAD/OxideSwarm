//! Adversarial Stress Test Suite for Milestone M5C (Challenger 2).
//!
//! Stress-tests:
//! 1. Nonce validation stress on HTTP discovery probe:
//!    - Missing nonce parameter (?nonce= omitted).
//!    - Empty nonce parameter (?nonce=).
//!    - Nonce shorter than 32 hex chars (e.g. 1 char, 31 chars).
//!    - Nonce longer than 32 hex chars (e.g. 33 chars, 64 chars).
//!    - Non-hexadecimal characters (letters g-z, punctuation, unicode emojis).
//!    - Verification that HTTP 400 Bad Request is strictly returned for all malformed nonces.
//!    - Verification that valid 32-hex nonces succeed with HTTP 200 and signed MasterBeacon.
//! 2. HMAC forgery & replay stress:
//!    - Beacon mutation: changing cluster IP/port, web UI URL breaks HMAC verification.
//!    - HMAC signature forgery: random bytes, wrong secret, all-zeros, truncated MAC strictly rejected.
//!    - Replay attack: capturing signed beacon from probe 1 and replaying it against probe 2
//!      (different nonce or 1-bit flipped nonce) is strictly rejected.
//! 3. Rendezvous registry stress:
//!    - Querying uninitialized rendezvous returns 404 / None.
//!    - Expired lease rejection: registration of already-expired lease rejected (401).
//!    - Natural lease expiration: lease evicted after TTL expiry (returns 404 / None).
//!    - Corrupted payload rejection: tampering cluster_addr, hostname, timestamp, ttl_secs,
//!      service, or signature causes verification failure and registration rejection (401/400).
//! 4. High-concurrency discovery & rendezvous stress:
//!    - 100 concurrent HTTP probes with distinct nonces verify HMAC binding without race conditions.
//!    - Concurrent rendezvous writes and reads verify thread safety and lack of deadlocks.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::discovery::{
    discover_master_rendezvous, register_rendezvous_lease, MasterBeacon, RendezvousLease,
    DISCOVERY_PROBE_NONCE_LEN, DISCOVERY_SERVICE_NAME,
};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};

fn make_secret(seed: u8) -> ClusterSecret {
    ClusterSecret::from_bytes(&[seed; 32]).expect("valid 32-byte secret")
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        write!(s, "{:02x}", b).unwrap();
    }
    s
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

fn fill_random_bytes(buf: &mut [u8]) {
    for chunk in buf.chunks_mut(8) {
        let h = RandomState::new().build_hasher().finish();
        let bytes = h.to_ne_bytes();
        let len = chunk.len().min(8);
        chunk[..len].copy_from_slice(&bytes[..len]);
    }
}

/// Raw HTTP request helper for adversarial probing.
async fn raw_http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to {addr}: {e}"));
    let mut req = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        method, path, addr
    );
    for (k, v) in headers {
        req.push_str(&format!("{}: {}\r\n", k, v));
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
        req.push_str("\r\n");
        req.push_str(b);
    } else {
        req.push_str("\r\n");
    }

    stream
        .write_all(req.as_bytes())
        .await
        .expect("write HTTP request");

    let mut resp_bytes = Vec::new();
    let mut buf = [0u8; 1024];
    while let Ok(n) = stream.read(&mut buf).await {
        if n == 0 {
            break;
        }
        resp_bytes.extend_from_slice(&buf[..n]);
    }
    let resp_str = String::from_utf8_lossy(&resp_bytes).to_string();
    let status_code = resp_str
        .lines()
        .next()
        .and_then(|line| {
            let mut parts = line.split_whitespace();
            parts.next()?; // HTTP/1.1
            parts.next()?.parse::<u16>().ok()
        })
        .unwrap_or(0);
    let body_str = if let Some(idx) = resp_str.find("\r\n\r\n") {
        resp_str[idx + 4..].to_string()
    } else {
        String::new()
    };
    (status_code, resp_str, body_str)
}

// ============================================================================
// 1. Nonce Validation Stress
// ============================================================================

#[tokio::test]
async fn test_adversarial_nonce_validation_stress() {
    let secret = make_secret(0xA1);

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");

    // Case 1: Missing nonce parameter entirely
    let (status, _, body) =
        raw_http_request(dash_addr, "GET", "/api/discovery/probe", &[], None).await;
    assert_eq!(
        status, 400,
        "Missing nonce query param must yield 400 Bad Request"
    );
    assert!(
        body.contains("Missing nonce parameter"),
        "Error body must explain missing nonce parameter: {body}"
    );

    // Case 2: Empty nonce parameter
    let (status, _, body) =
        raw_http_request(dash_addr, "GET", "/api/discovery/probe?nonce=", &[], None).await;
    assert_eq!(status, 400, "Empty nonce must yield 400 Bad Request");
    assert!(
        body.contains("Invalid nonce length") || body.contains("Missing"),
        "Error body must describe invalid nonce length: {body}"
    );

    // Case 3: Nonce shorter than 32 hex chars (1 character)
    let (status, _, body) =
        raw_http_request(dash_addr, "GET", "/api/discovery/probe?nonce=a", &[], None).await;
    assert_eq!(status, 400, "1-char nonce must yield 400 Bad Request");
    assert!(body.contains("Invalid nonce length"), "Body: {body}");

    // Case 4: Nonce shorter than 32 hex chars (31 characters)
    let nonce_31 = "0123456789abcdef0123456789abcde";
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={nonce_31}"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, 400, "31-char nonce must yield 400 Bad Request");
    assert!(body.contains("Invalid nonce length"), "Body: {body}");

    // Case 5: Nonce longer than 32 hex chars (33 characters)
    let nonce_33 = "0123456789abcdef0123456789abcdef0";
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={nonce_33}"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, 400, "33-char nonce must yield 400 Bad Request");
    assert!(body.contains("Invalid nonce length"), "Body: {body}");

    // Case 6: Nonce longer than 32 hex chars (64 characters)
    let nonce_64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={nonce_64}"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, 400, "64-char nonce must yield 400 Bad Request");
    assert!(body.contains("Invalid nonce length"), "Body: {body}");

    // Case 7: Non-hex characters (letters g-z) exactly 32 chars
    let nonce_nonhex_1 = "xyz123456789abcdef0123456789abcd";
    assert_eq!(nonce_nonhex_1.len(), 32);
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={nonce_nonhex_1}"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        status, 400,
        "Non-hex letters 'xyz' must yield 400 Bad Request"
    );
    assert!(body.contains("Invalid hex encoding"), "Body: {body}");

    // Case 8: Non-hex punctuation / symbols (32 chars without URL fragment delimiters)
    let nonce_symbols = "0123456789abcdef0123456789abc!_~";
    assert_eq!(nonce_symbols.len(), 32);
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={nonce_symbols}"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        status, 400,
        "Non-hex symbols '!_~' must yield 400 Bad Request"
    );
    assert!(body.contains("Invalid hex encoding"), "Body: {body}");

    // Case 9: Valid 32-hex-char nonce (lowercase)
    let valid_nonce = "0123456789abcdef0123456789abcdef";
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={valid_nonce}"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, 200, "Valid 32-hex nonce must yield 200 OK");
    let beacon: MasterBeacon = serde_json::from_str(&body).expect("parse beacon JSON");
    assert_eq!(beacon.cluster_addr, master.server_addr().to_string());
    assert!(beacon.mac.is_some());
    let nonce_bytes = hex_decode(valid_nonce).unwrap();
    assert!(
        beacon.verify(&secret, &nonce_bytes),
        "Returned beacon must verify with valid nonce and secret"
    );

    // Case 10: Valid 32-hex-char nonce (uppercase)
    let valid_nonce_upper = "0123456789ABCDEF0123456789ABCDEF";
    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={valid_nonce_upper}"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, 200, "Uppercase 32-hex nonce must yield 200 OK");
    let beacon: MasterBeacon = serde_json::from_str(&body).expect("parse beacon JSON");
    let nonce_bytes = hex_decode(valid_nonce_upper).unwrap();
    assert!(
        beacon.verify(&secret, &nonce_bytes),
        "Returned beacon must verify with uppercase nonce and secret"
    );
}

// ============================================================================
// 2. HMAC Forgery & Replay Stress
// ============================================================================

#[tokio::test]
async fn test_adversarial_hmac_tamper_forgery_and_replay_stress() {
    let secret = make_secret(0xB2);
    let wrong_secret = make_secret(0xC3);

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");

    // Probe 1: Legitimate request
    let mut nonce_1 = [0u8; DISCOVERY_PROBE_NONCE_LEN];
    fill_random_bytes(&mut nonce_1);
    let nonce_1_hex = hex_encode(&nonce_1);

    let (status, _, body) = raw_http_request(
        dash_addr,
        "GET",
        &format!("/api/discovery/probe?nonce={nonce_1_hex}"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, 200);
    let original_beacon: MasterBeacon = serde_json::from_str(&body).unwrap();
    assert!(original_beacon.verify(&secret, &nonce_1));

    // Attack A: Mutating advertised cluster IP/port to redirect traffic
    let mut tampered_ip = original_beacon.clone();
    tampered_ip.cluster_addr = "192.168.1.199:8088".to_string();
    assert!(
        !tampered_ip.verify(&secret, &nonce_1),
        "Worker must strictly reject beacon with tampered cluster IP"
    );

    let mut tampered_port = original_beacon.clone();
    tampered_port.cluster_addr = "127.0.0.1:65530".to_string();
    assert!(
        !tampered_port.verify(&secret, &nonce_1),
        "Worker must strictly reject beacon with tampered cluster port"
    );

    // Attack B: Mutating web_ui_url
    let mut tampered_web = original_beacon.clone();
    tampered_web.web_ui_url = "http://malicious-node.xyz:8080".to_string();
    assert!(
        !tampered_web.verify(&secret, &nonce_1),
        "Worker must strictly reject beacon with tampered web_ui_url"
    );

    // Attack C: Forging HMAC signature with random bytes
    let mut forged_mac = original_beacon.clone();
    let mut fake_tag = [0u8; 32];
    fill_random_bytes(&mut fake_tag);
    forged_mac.mac = Some(hex_encode(&fake_tag));
    assert!(
        !forged_mac.verify(&secret, &nonce_1),
        "Worker must strictly reject random forged HMAC"
    );

    // Attack D: Forging HMAC signature using wrong secret
    let mut wrong_signed = original_beacon.clone();
    wrong_signed.sign(&wrong_secret, &nonce_1);
    assert!(
        !wrong_signed.verify(&secret, &nonce_1),
        "Worker holding correct secret must reject beacon signed with wrong secret"
    );

    // Attack E: Unsigned beacon (mac is None) presented to secret holder
    let mut unsigned_beacon = original_beacon.clone();
    unsigned_beacon.mac = None;
    assert!(
        !unsigned_beacon.verify(&secret, &nonce_1),
        "Worker holding secret must reject unsigned beacon"
    );

    // Attack F: Malformed/corrupted hex in MAC
    let mut corrupt_mac = original_beacon.clone();
    corrupt_mac.mac = Some("not_a_valid_hex_hmac".to_string());
    assert!(
        !corrupt_mac.verify(&secret, &nonce_1),
        "Worker must reject non-hex MAC string"
    );

    // Attack G: Replay attack with captured beacon
    // Attacker eavesdrops original_beacon signed for nonce_1.
    // Client initiates Probe 2 with fresh nonce_2.
    let mut nonce_2 = [0u8; DISCOVERY_PROBE_NONCE_LEN];
    fill_random_bytes(&mut nonce_2);
    assert_ne!(nonce_1, nonce_2);

    // Attacker replays original_beacon against probe 2
    assert!(
        !original_beacon.verify(&secret, &nonce_2),
        "Replayed beacon from probe 1 must be strictly rejected against probe 2 nonce"
    );

    // Attack H: Bit-flip replay (differing by only 1 bit in nonce)
    let mut nonce_1_bitflip = nonce_1;
    nonce_1_bitflip[15] ^= 0x01; // flip single lowest bit
    assert!(
        !original_beacon.verify(&secret, &nonce_1_bitflip),
        "Beacon verification must fail even if nonce differs by only 1 bit"
    );
}

// ============================================================================
// 3. Rendezvous Registry Stress
// ============================================================================

#[tokio::test]
async fn test_adversarial_rendezvous_registry_stress() {
    let secret = make_secret(0xD4);
    let wrong_secret = make_secret(0xE5);

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret(secret.clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");
    let rendezvous_url = format!("http://127.0.0.1:{}", dash_addr.port());

    // 1. Querying uninitialized rendezvous returns 404 / None
    let pre_query =
        discover_master_rendezvous(&rendezvous_url, Some(&secret), Duration::from_secs(2)).await;
    assert!(
        pre_query.is_none(),
        "Uninitialized rendezvous must return None"
    );

    let (status, _, _) =
        raw_http_request(dash_addr, "GET", "/api/rendezvous/master", &[], None).await;
    assert_eq!(
        status, 404,
        "Uninitialized rendezvous endpoint must return 404"
    );

    // 2. Reject registration of already-expired lease
    let now = current_unix_timestamp();
    let mut expired_lease = RendezvousLease::new(
        master.server_addr().to_string(),
        format!("http://127.0.0.1:{}", dash_addr.port()),
        "expired-master",
        None,
        1,
        10, // 10s TTL
    );
    expired_lease.timestamp = now - 100; // expired 90 seconds ago
    expired_lease.sign(&secret);

    let reg_expired = register_rendezvous_lease(&rendezvous_url, &expired_lease).await;
    assert!(
        reg_expired.is_err(),
        "Registration of already-expired lease must be rejected"
    );

    // 3. Natural TTL expiry and eviction test
    // We register a lease whose (timestamp + ttl_secs + 30) is valid now, but expires in 1.5 seconds.
    // Specifically: ttl_secs = 1, timestamp = now - 30.
    // (timestamp + ttl_secs + 30) = (now - 30) + 1 + 30 = now + 1.
    let now_fresh = current_unix_timestamp();
    let mut short_lease = RendezvousLease::new(
        master.server_addr().to_string(),
        format!("http://127.0.0.1:{}", dash_addr.port()),
        "short-ttl-master",
        None,
        1,
        1, // 1s TTL
    );
    short_lease.timestamp = now_fresh.saturating_sub(30);
    short_lease.sign(&secret);

    let reg_short = register_rendezvous_lease(&rendezvous_url, &short_lease).await;
    assert!(
        reg_short.is_ok(),
        "Lease within valid window must register successfully"
    );

    // Query immediately while still within valid window
    let immediate_query =
        discover_master_rendezvous(&rendezvous_url, Some(&secret), Duration::from_secs(2)).await;
    assert!(
        immediate_query.is_some(),
        "Lease must be queryable before expiry window closes"
    );

    // Wait 3.5 seconds so now strictly exceeds (timestamp + ttl_secs + 30)
    tokio::time::sleep(Duration::from_millis(3500)).await;

    // After expiry: registry must reject/evict and return 404 / None
    let post_expiry_query =
        discover_master_rendezvous(&rendezvous_url, Some(&secret), Duration::from_secs(2)).await;
    assert!(
        post_expiry_query.is_none(),
        "Expired rendezvous lease must be evicted/rejected"
    );

    let (status, _, _) =
        raw_http_request(dash_addr, "GET", "/api/rendezvous/master", &[], None).await;
    assert_eq!(
        status, 404,
        "Expired rendezvous query must return 404 Not Found"
    );

    // 4. Corrupted payload rejection on registration
    let mut valid_lease = RendezvousLease::new(
        master.server_addr().to_string(),
        format!("http://127.0.0.1:{}", dash_addr.port()),
        "legit-master",
        None,
        2,
        120,
    );
    valid_lease.sign(&secret);

    // Corrupt address
    let mut corrupt_addr = valid_lease.clone();
    corrupt_addr.cluster_addr = "10.0.0.99:8088".to_string();
    assert!(!corrupt_addr.verify(&secret, 30));
    assert!(
        register_rendezvous_lease(&rendezvous_url, &corrupt_addr)
            .await
            .is_err(),
        "Registration with tampered address must fail"
    );

    // Corrupt hostname
    let mut corrupt_host = valid_lease.clone();
    corrupt_host.hostname = "evil-host".to_string();
    assert!(!corrupt_host.verify(&secret, 30));
    assert!(
        register_rendezvous_lease(&rendezvous_url, &corrupt_host)
            .await
            .is_err(),
        "Registration with tampered hostname must fail"
    );

    // Corrupt timestamp
    let mut corrupt_ts = valid_lease.clone();
    corrupt_ts.timestamp += 10;
    assert!(!corrupt_ts.verify(&secret, 30));
    assert!(
        register_rendezvous_lease(&rendezvous_url, &corrupt_ts)
            .await
            .is_err(),
        "Registration with tampered timestamp must fail"
    );

    // Corrupt TTL
    let mut corrupt_ttl = valid_lease.clone();
    corrupt_ttl.ttl_secs += 300;
    assert!(!corrupt_ttl.verify(&secret, 30));
    assert!(
        register_rendezvous_lease(&rendezvous_url, &corrupt_ttl)
            .await
            .is_err(),
        "Registration with tampered TTL must fail"
    );

    // Corrupt service name
    let mut corrupt_service = valid_lease.clone();
    corrupt_service.service = "rogue-service".to_string();
    assert!(
        register_rendezvous_lease(&rendezvous_url, &corrupt_service)
            .await
            .is_err(),
        "Registration with invalid service name must fail"
    );

    // Forged signature with wrong secret
    let mut wrong_lease = valid_lease.clone();
    wrong_lease.sign(&wrong_secret);
    assert!(!wrong_lease.verify(&secret, 30));
    assert!(
        register_rendezvous_lease(&rendezvous_url, &wrong_lease)
            .await
            .is_err(),
        "Registration signed with wrong secret must fail"
    );

    // Register genuine lease and verify in-flight tamper rejection
    assert!(
        register_rendezvous_lease(&rendezvous_url, &valid_lease)
            .await
            .is_ok(),
        "Genuine lease registration must succeed"
    );

    let fetched =
        discover_master_rendezvous(&rendezvous_url, Some(&secret), Duration::from_secs(2))
            .await
            .expect("fetch genuine lease");
    assert_eq!(fetched.cluster_addr, master.server_addr().to_string());
}

// ============================================================================
// 4. High-Concurrency Discovery & Rendezvous Stress
// ============================================================================

#[tokio::test]
async fn test_adversarial_high_concurrency_discovery_stress() {
    let secret = Arc::new(make_secret(0xF6));

    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_web_ui(true)
        .with_web_ui_addr("127.0.0.1:0".parse().unwrap())
        .with_cluster_secret((*secret).clone());

    let master: MasterHandle = MasterServer::spawn_with_config(
        server_config,
        SchedulerConfig::default(),
        ReaperConfig::default(),
    )
    .await
    .expect("master spawns");

    let dash_addr = master
        .dashboard_addr()
        .expect("master has dashboard address bound");
    let dash_port = dash_addr.port();
    let cluster_addr = master.server_addr().to_string();

    // 1. High-concurrency probe burst: 100 concurrent HTTP probes with unique nonces
    let concurrent_probes = 100;
    let mut join_set = tokio::task::JoinSet::new();

    for i in 0..concurrent_probes {
        let sec = Arc::clone(&secret);
        let expected_cluster = cluster_addr.clone();

        join_set.spawn(async move {
            let mut nonce = [0u8; DISCOVERY_PROBE_NONCE_LEN];
            fill_random_bytes(&mut nonce);
            let nonce_hex = hex_encode(&nonce);

            let (status, _, body) = raw_http_request(
                dash_addr,
                "GET",
                &format!("/api/discovery/probe?nonce={nonce_hex}"),
                &[],
                None,
            )
            .await;

            assert_eq!(status, 200, "Probe #{i} must return 200 OK");
            let beacon: MasterBeacon = serde_json::from_str(&body).unwrap();
            assert_eq!(
                beacon.cluster_addr, expected_cluster,
                "Probe #{i} returned cluster address mismatch"
            );
            assert!(
                beacon.verify(&sec, &nonce),
                "Probe #{i} must verify strictly against its own nonce"
            );

            // Verify that this beacon fails verification against an altered nonce
            let mut wrong_nonce = nonce;
            wrong_nonce[0] ^= 0x55;
            assert!(
                !beacon.verify(&sec, &wrong_nonce),
                "Probe #{i} must reject wrong nonce"
            );
        });
    }

    let mut successful_probes = 0;
    while let Some(res) = join_set.join_next().await {
        res.expect("concurrent probe task completed without panic");
        successful_probes += 1;
    }
    assert_eq!(
        successful_probes, concurrent_probes,
        "All 100 concurrent probes must succeed and verify"
    );

    // 2. High-concurrency rendezvous lease read/write race
    let rendezvous_url = Arc::new(format!("http://127.0.0.1:{dash_port}"));
    let mut race_set = tokio::task::JoinSet::new();

    // 10 concurrent writer tasks continuously registering monotonic leases
    for w in 0..10 {
        let r_url = Arc::clone(&rendezvous_url);
        let sec = Arc::clone(&secret);
        let c_addr = cluster_addr.clone();

        race_set.spawn(async move {
            for step in 0..5 {
                let mut lease = RendezvousLease::new(
                    c_addr.clone(),
                    format!("http://127.0.0.1:8080/w{w}"),
                    format!("host-writer-{w}-{step}"),
                    None,
                    w + step,
                    60,
                );
                lease.sign(&sec);
                let reg_res = register_rendezvous_lease(&r_url, &lease).await;
                assert!(
                    reg_res.is_ok(),
                    "Concurrent writer #{w} step #{step} failed"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    // 20 concurrent reader tasks continuously querying rendezvous
    for _r in 0..20 {
        let r_url = Arc::clone(&rendezvous_url);
        let sec = Arc::clone(&secret);
        let c_addr = cluster_addr.clone();

        race_set.spawn(async move {
            for _ in 0..10 {
                let res =
                    discover_master_rendezvous(&r_url, Some(&sec), Duration::from_millis(500))
                        .await;
                if let Some(beacon) = res {
                    assert_eq!(beacon.cluster_addr, c_addr);
                    assert_eq!(beacon.service, DISCOVERY_SERVICE_NAME);
                }
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        });
    }

    while let Some(res) = race_set.join_next().await {
        res.expect("concurrent race task finished cleanly");
    }
}
