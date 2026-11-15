//! Integration tests for `ECO-4` Observability:
//! 1. Prometheus `/metrics` Observer RBAC enforcement (401/403/200).
//! 2. Live master + worker + blob store + journal `/metrics` scrape, values, secret/input
//!    redaction, and `oxideswarm_store_healthy` flip on write failure.
//! 3. Opt-in `--features otel` OTLP/HTTP JSON trace export across all 5 task lifecycle spans
//!    (`submit`, `place`, `assign`, `run`, `result`), deterministic `task_id` trace/parent IDs,
//!    and secret/command redaction.
//! 4. OTLP endpoint validation refusing embedded credentials without echoing secrets.

use std::time::Duration;

use reqwest::{header, StatusCode};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::task::TaskId;
use rusty_grid_master::auth::{AuthConfig, AuthScope};
use rusty_grid_master::metrics::PROMETHEUS_CONTENT_TYPE;
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use tokio::sync::watch;
use uuid::Uuid;

async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

fn spawn_test_worker(
    master_addr: String,
    name: &str,
    cluster_secret: ClusterSecret,
    worker_id_path: std::path::PathBuf,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let config = WorkerConfig::new(master_addr)
        .with_name(name)
        .with_cluster_secret(cluster_secret)
        .with_worker_id_file(worker_id_path)
        .with_cores(2)
        .with_ram_mb(4096)
        .with_no_battery(true)
        .with_no_gpu(true)
        .with_no_blender(true)
        .with_no_cuda(true)
        .with_redirection_portal(false);
    let mut worker = WorkerClient::new(config);
    let handle = tokio::spawn(async move {
        let _ = worker.run(shutdown_rx).await;
    });
    (shutdown_tx, handle)
}

async fn wait_for_connected_worker(master: &MasterHandle) {
    let ready = wait_for(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            master
                .list_workers()
                .await
                .is_ok_and(|ws| ws.iter().any(|w| w.status == WorkerStatus::Connected))
        },
    )
    .await;
    assert!(ready, "worker did not register within timeout");
}

fn parse_metric_value(body: &str, metric_line_prefix: &str) -> Option<f64> {
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix(metric_line_prefix) {
            if rest.starts_with(' ') || rest.starts_with('\t') {
                return rest.trim().parse::<f64>().ok();
            }
        }
    }
    None
}

#[tokio::test]
async fn test_metrics_requires_observer_scope_and_rejects_unauthenticated() {
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
        allowed_origins: Vec::new(),
    }
    .with_token("obs-token-123", "observer-only", &[AuthScope::Observer]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{dash_addr}");

    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // 1. Unauthenticated /metrics -> 401
    let resp = client
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .expect("unauth GET /metrics");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 2. Invalid bearer token -> 401
    let resp = client
        .get(format!("{base_url}/metrics"))
        .bearer_auth("wrong-token")
        .send()
        .await
        .expect("wrong token GET /metrics");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 3. Observer bearer token -> 200 Prometheus text exposition
    let resp = client
        .get(format!("{base_url}/metrics"))
        .bearer_auth("obs-token-123")
        .send()
        .await
        .expect("observer GET /metrics");
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(content_type, PROMETHEUS_CONTENT_TYPE);
    let body = resp.text().await.expect("metrics body");
    assert!(body.contains("oxideswarm_queue_tasks{state=\"queued\"}"));

    // 4. Observer remains denied on mutating POST /api/tasks -> 403
    let resp = client
        .post(format!("{base_url}/api/tasks"))
        .bearer_auth("obs-token-123")
        .json(&serde_json::json!({ "command": "echo forbidden" }))
        .send()
        .await
        .expect("observer POST /api/tasks");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let _ = master.shutdown();
}

#[tokio::test]
async fn test_metrics_scrape_live_master_worker_blob_journal_and_redaction() {
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let blob_dir = tempfile::tempdir().expect("blob tempdir");
    let worker_dir = tempfile::tempdir().expect("worker tempdir");

    let secret = ClusterSecret::from_bytes(&[0x5Au8; 32]).expect("cluster secret");
    let secret_hex = secret.to_hex();

    let observer_token = "obs-secret-token";
    let submitter_token = "sub-secret-token";
    let auth_config = AuthConfig {
        enabled: true,
        dev_mode: false,
        loopback_bypass: false,
        tokens: Default::default(),
        allowed_origins: Vec::new(),
    }
    .with_token(observer_token, "observer-1", &[AuthScope::Observer])
    .with_token(submitter_token, "submitter-1", &[AuthScope::Submitter]);

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_auth_config(auth_config)
        .with_cluster_secret(secret.clone())
        .with_state_dir(state_dir.path())
        .with_blob_store(blob_dir.path(), 16 * 1024 * 1024);

    let master = MasterServer::spawn(config).await.expect("spawn master");
    let dash_addr = master.dashboard_addr().expect("dashboard addr");
    let base_url = format!("http://{dash_addr}");

    // Seed blob metrics on the live BlobStore.
    let blob_store = master.blob_store().expect("blob store configured");
    let _blob_id = blob_store
        .put(&vec![0xABu8; 4096])
        .await
        .expect("put blob into master blob store");

    let (worker_tx, worker_handle) = spawn_test_worker(
        master.server_addr().to_string(),
        "obs-worker-1",
        secret,
        worker_dir.path().join("worker.id"),
    );
    wait_for_connected_worker(&master).await;

    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    // Submit a task containing a sensitive command string via POST /api/tasks.
    let secret_cmd = "echo SECRET_TASK_INPUT_42";
    let submit_resp = client
        .post(format!("{base_url}/api/tasks"))
        .bearer_auth(submitter_token)
        .json(&serde_json::json!({ "command": secret_cmd }))
        .send()
        .await
        .expect("POST /api/tasks");
    assert_eq!(submit_resp.status(), StatusCode::OK);
    let submit_json: serde_json::Value = submit_resp.json().await.expect("submit json");
    let task_id_str = submit_json["task_id"]
        .as_str()
        .expect("task_id field in response")
        .to_string();
    let task_uuid = Uuid::parse_str(&task_id_str).expect("valid UUID task_id");

    let result = master
        .wait_task(TaskId(task_uuid), Some(Duration::from_secs(15)))
        .await
        .expect("wait for completed task");
    assert_eq!(result.exit_code, 0, "task failed: {:?}", result.error);

    let metrics_resp = client
        .get(format!("{base_url}/metrics"))
        .bearer_auth(observer_token)
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(metrics_resp.status(), StatusCode::OK);
    let body = metrics_resp.text().await.expect("metrics response text");

    // Queue & throughput series
    for state in [
        "queued",
        "scheduled",
        "running",
        "retrying",
        "completed",
        "failed",
        "cancelled",
    ] {
        assert!(
            body.contains(&format!("oxideswarm_queue_tasks{{state=\"{state}\"}}")),
            "missing state gauge {state} in:\n{body}"
        );
    }
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_queue_tasks{state=\"completed\"}"),
        Some(1.0)
    );
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_tasks_submitted_total"),
        Some(1.0)
    );
    assert_eq!(
        parse_metric_value(
            &body,
            "oxideswarm_tasks_terminal_total{state=\"completed\"}"
        ),
        Some(1.0)
    );
    assert!(
        parse_metric_value(&body, "oxideswarm_tasks_per_second").is_some_and(|v| v > 0.0),
        "expected positive oxideswarm_tasks_per_second in:\n{body}"
    );
    assert!(body.contains("oxideswarm_scheduling_latency_seconds_bucket{"));
    assert!(body.contains("oxideswarm_scheduling_latency_seconds_sum "));
    assert!(
        parse_metric_value(&body, "oxideswarm_scheduling_latency_seconds_count")
            .is_some_and(|v| v >= 1.0)
    );

    // Worker series
    assert_eq!(
        parse_metric_value(
            &body,
            "oxideswarm_workers{state=\"connected\",link=\"tcp_lan\"}"
        ),
        Some(1.0)
    );

    // Blob series
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_blob_bytes_in_total"),
        Some(4096.0)
    );
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_blob_store_bytes"),
        Some(4096.0)
    );
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_blob_store_objects"),
        Some(1.0)
    );
    assert!(body.contains("oxideswarm_blob_bytes_out_total "));
    assert!(body.contains("oxideswarm_blob_evictions_total "));
    assert!(body.contains("oxideswarm_blob_store_max_bytes "));

    // Durable journal & store health series
    assert!(parse_metric_value(&body, "oxideswarm_journal_bytes").is_some_and(|v| v > 0.0));
    assert!(parse_metric_value(&body, "oxideswarm_journal_records").is_some_and(|v| v > 0.0));
    assert!(
        parse_metric_value(&body, "oxideswarm_journal_fsync_duration_seconds_count")
            .is_some_and(|v| v >= 1.0)
    );
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_store_configured"),
        Some(1.0)
    );
    assert_eq!(
        parse_metric_value(&body, "oxideswarm_store_healthy"),
        Some(1.0)
    );

    // Secret / task input / high-cardinality identifier redaction
    assert!(!body.contains(&secret_hex), "metrics leaked cluster secret");
    assert!(
        !body.contains(observer_token),
        "metrics leaked observer token"
    );
    assert!(
        !body.contains(submitter_token),
        "metrics leaked submitter token"
    );
    assert!(
        !body.contains("SECRET_TASK_INPUT_42"),
        "metrics leaked task command"
    );
    assert!(!body.contains(&task_id_str), "metrics leaked task ID label");

    // Flip durability health via test hook and ensure 0 is reflected without leaking error strings.
    #[cfg(feature = "test-hooks")]
    {
        master.queue().fail_store_for_test("disk full").await;
        let degraded_body = client
            .get(format!("{base_url}/metrics"))
            .bearer_auth(observer_token)
            .send()
            .await
            .expect("GET /metrics after failure")
            .text()
            .await
            .expect("degraded metrics text");
        assert_eq!(
            parse_metric_value(&degraded_body, "oxideswarm_store_healthy"),
            Some(0.0)
        );
        assert!(
            !degraded_body.contains("disk full"),
            "store error reason must not leak into /metrics"
        );
    }

    let _ = worker_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), worker_handle).await;
    let _ = master.shutdown();
}

#[cfg(feature = "otel")]
async fn spawn_mock_otlp_collector() -> (
    std::net::SocketAddr,
    std::sync::Arc<tokio::sync::Mutex<Vec<String>>>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock OTLP collector");
    let addr = listener.local_addr().expect("collector local addr");
    let payloads = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let payloads_ref = std::sync::Arc::clone(&payloads);
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop_rx.changed() => break,
                accepted = listener.accept() => {
                    let Ok((mut stream, _)) = accepted else { break; };
                    let payloads_ref = std::sync::Arc::clone(&payloads_ref);
                    tokio::spawn(async move {
                        let mut buf = Vec::with_capacity(4096);
                        let mut chunk = [0u8; 2048];
                        let header_end = loop {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => {
                                    buf.extend_from_slice(&chunk[..n]);
                                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                        break pos + 4;
                                    }
                                    if buf.len() > 64 * 1024 {
                                        return;
                                    }
                                }
                            }
                        };
                        let headers_str = String::from_utf8_lossy(&buf[..header_end]).to_string();
                        let mut lines = headers_str.lines();
                        let req_line = lines.next().unwrap_or("");
                        assert!(
                            req_line.starts_with("POST /v1/traces "),
                            "unexpected OTLP request target: {req_line}"
                        );
                        let mut content_length = 0usize;
                        for line in lines {
                            if let Some((k, v)) = line.split_once(':') {
                                if k.eq_ignore_ascii_case("content-length") {
                                    content_length = v.trim().parse().unwrap_or(0);
                                }
                            }
                        }
                        while buf.len() < header_end + content_length {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        }
                        let body = String::from_utf8_lossy(
                            &buf[header_end..header_end + content_length],
                        )
                        .to_string();
                        payloads_ref.lock().await.push(body);
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                            )
                            .await;
                        let _ = stream.flush().await;
                    });
                }
            }
        }
    });

    (addr, payloads, stop_tx, handle)
}

#[cfg(feature = "otel")]
#[tokio::test]
async fn test_otel_exporter_lifecycle_spans_trace_id_and_secret_redaction() {
    use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
    use rusty_grid_master::otel::{
        encode_hex, root_span_id_for_task, start_otlp_exporter, trace_id_for_task, OtlpConfig,
    };
    use std::collections::HashSet;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let (collector_addr, payloads, stop_collector, collector_handle) =
        spawn_mock_otlp_collector().await;
    let otlp_cfg = OtlpConfig::parse_base_endpoint(
        &format!("http://{collector_addr}"),
        "oxideswarm-test-master",
    )
    .expect("valid OTLP config");
    let (otel_layer, otel_handle) = start_otlp_exporter(otlp_cfg);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            "warn,oxideswarm::otel=info",
        ))
        .with(otel_layer)
        .try_init()
        .expect("install global tracing subscriber once for test binary");

    let state_dir = tempfile::tempdir().expect("state tempdir");
    let worker_dir = tempfile::tempdir().expect("worker tempdir");
    let secret = ClusterSecret::from_bytes(&[0x7Cu8; 32]).expect("cluster secret");
    let secret_hex = secret.to_hex();

    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_dashboard_port(0)
        .with_cluster_secret(secret.clone())
        .with_state_dir(state_dir.path());
    let master = MasterServer::spawn(config).await.expect("spawn master");

    let (worker_tx, worker_handle) = spawn_test_worker(
        master.server_addr().to_string(),
        "obs-worker-1",
        secret,
        worker_dir.path().join("worker.id"),
    );
    wait_for_connected_worker(&master).await;

    let task = Task::new(
        TaskSpec::new_shell_script("echo TOP_SECRET_OTEL_CMD_999"),
        TaskRequirements::generic(1, 10),
    );
    let task_id = master.submit_task(task).await.expect("submit task");
    let res = master
        .wait_task(task_id, Some(Duration::from_secs(15)))
        .await
        .expect("wait task completion");
    assert_eq!(res.exit_code, 0);

    let _ = worker_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), worker_handle).await;
    let _ = master.shutdown();

    otel_handle.shutdown().await;
    let _ = stop_collector.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), collector_handle).await;

    let expected_trace_id = encode_hex(&trace_id_for_task(task_id.0));
    let expected_parent_span_id = encode_hex(&root_span_id_for_task(task_id.0));
    let expected_task_id = task_id.to_string();

    let raw_bodies = payloads.lock().await;
    assert!(
        !raw_bodies.is_empty(),
        "expected mock OTLP collector to receive span batches"
    );

    let mut seen_names = HashSet::new();
    for raw in raw_bodies.iter() {
        assert!(
            !raw.contains("TOP_SECRET_OTEL_CMD_999"),
            "OTLP span leaked task command: {raw}"
        );
        assert!(
            !raw.contains(&secret_hex),
            "OTLP span leaked cluster secret"
        );
        let parsed: serde_json::Value = serde_json::from_str(raw).expect("valid OTLP JSON");
        let resource_spans = parsed["resourceSpans"]
            .as_array()
            .expect("resourceSpans array");
        for rs in resource_spans {
            for ss in rs["scopeSpans"].as_array().expect("scopeSpans array") {
                for span in ss["spans"].as_array().expect("spans array") {
                    let attrs = span["attributes"].as_array().expect("attributes array");
                    let matches_task = attrs.iter().any(|a| {
                        a["key"].as_str() == Some("task_id")
                            && a["value"]["stringValue"].as_str() == Some(&expected_task_id)
                    });
                    if matches_task {
                        let name = span["name"].as_str().expect("span name").to_string();
                        assert_eq!(
                            span["traceId"].as_str(),
                            Some(expected_trace_id.as_str()),
                            "unexpected traceId on span {name}"
                        );
                        assert_eq!(
                            span["parentSpanId"].as_str(),
                            Some(expected_parent_span_id.as_str()),
                            "unexpected parentSpanId on span {name}"
                        );
                        seen_names.insert(name);
                    }
                }
            }
        }
    }

    for required in ["submit", "place", "assign", "run", "result"] {
        assert!(
            seen_names.contains(required),
            "missing lifecycle span '{required}' in {seen_names:?}"
        );
    }
}

#[cfg(feature = "otel")]
#[test]
fn test_otel_invalid_and_credentialed_endpoints_fail_cleanly() {
    use rusty_grid_master::otel::OtlpConfig;
    use std::collections::HashMap;

    for raw in [
        "http://admin:ultra_secret_pw@127.0.0.1:4318",
        "ftp://127.0.0.1:4318",
        "http:///v1/traces",
        "not-a-valid-url",
    ] {
        let env: HashMap<&str, &str> = [("OTEL_EXPORTER_OTLP_ENDPOINT", raw)].into_iter().collect();
        let err = OtlpConfig::from_env_with(|k| env.get(k).map(|s| (*s).to_string()))
            .expect_err("invalid or credentialed OTLP endpoint must error");
        assert!(
            !err.contains("ultra_secret_pw") && !err.contains("admin:"),
            "OTLP config error leaked credentials: {err}"
        );
    }

    let valid_env: HashMap<&str, &str> = [
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
        ("OTEL_SERVICE_NAME", "obs-unit"),
    ]
    .into_iter()
    .collect();
    let cfg = OtlpConfig::from_env_with(|k| valid_env.get(k).map(|s| (*s).to_string()))
        .expect("valid config")
        .expect("present config");
    let dbg = format!("{cfg:?}");
    assert!(!dbg.contains("ultra_secret_pw"));
}
