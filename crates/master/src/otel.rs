//! Opt-in OpenTelemetry OTLP/HTTP JSON trace exporter for task lifecycle spans (`ECO-4`).
//!
//! Activated only when compiled with `--features otel` and configured via
//! `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT`.
//!
//! Privacy / security invariant:
//! - Only spans targeting `oxideswarm::otel` with lifecycle names (`submit`, `place`, `assign`,
//!   `run`, `result`) are exported.
//! - Only a closed attribute allowlist (`task_id`, `worker_id`, `priority`, `requires_gpu`,
//!   `verdict`, `outcome`, `exit_code`, `attempt`) is recorded; task commands, arguments,
//!   environment variables, cluster secrets, bearer tokens, and payloads are never captured.
//! - Endpoint validation rejects embedded credentials and never echoes URL credentials in errors.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;
use url::Url;
use uuid::Uuid;

/// Tracing target matched by [`OtlpTracingLayer`].
pub const OTEL_SPAN_TARGET: &str = "oxideswarm::otel";

const ALLOWED_SPAN_NAMES: &[&str] = &["submit", "place", "assign", "run", "result"];
const CHANNEL_CAPACITY: usize = 2048;
const MAX_BATCH_SPANS: usize = 64;
const BATCH_FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const EXPORT_HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const FLUSH_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

static NEXT_SPAN_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Derives a deterministic, non-zero 16-byte OpenTelemetry trace ID from `task_id`.
pub fn trace_id_for_task(task_id: Uuid) -> [u8; 16] {
    let derived = blake3::derive_key("oxideswarm.otel.trace_id.v1", task_id.as_bytes());
    let mut id = [0u8; 16];
    id.copy_from_slice(&derived[..16]);
    if id.iter().all(|&b| b == 0) {
        id[15] = 1;
    }
    id
}

/// Derives a deterministic, non-zero 8-byte root parent span ID from `task_id`.
pub fn root_span_id_for_task(task_id: Uuid) -> [u8; 8] {
    let derived = blake3::derive_key("oxideswarm.otel.root_span_id.v1", task_id.as_bytes());
    let mut id = [0u8; 8];
    id.copy_from_slice(&derived[..8]);
    if id.iter().all(|&b| b == 0) {
        id[7] = 1;
    }
    id
}

/// Formats arbitrary bytes as lowercase hexadecimal.
pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn next_span_id() -> [u8; 8] {
    let raw = NEXT_SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nz = NonZeroU64::new(raw).map_or(1, NonZeroU64::get);
    nz.to_be_bytes()
}

fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// Validated OTLP/HTTP JSON trace exporter configuration.
#[derive(Clone)]
pub struct OtlpConfig {
    endpoint: Url,
    service_name: String,
}

impl fmt::Debug for OtlpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpConfig")
            .field("scheme", &self.endpoint.scheme())
            .field("host", &self.endpoint.host_str())
            .field("port", &self.endpoint.port_or_known_default())
            .field("path", &self.endpoint.path())
            .field("service_name", &self.service_name)
            .finish()
    }
}

impl OtlpConfig {
    /// Parses an OTLP traces endpoint URL without appending `/v1/traces`.
    pub fn parse_traces_endpoint(
        raw_url: &str,
        service_name: impl Into<String>,
    ) -> Result<Self, String> {
        Self::parse_endpoint(raw_url, false, service_name)
    }

    /// Parses a base OTLP endpoint URL (`OTEL_EXPORTER_OTLP_ENDPOINT`) and ensures `/v1/traces`
    /// is appended when absent.
    pub fn parse_base_endpoint(
        raw_url: &str,
        service_name: impl Into<String>,
    ) -> Result<Self, String> {
        Self::parse_endpoint(raw_url, true, service_name)
    }

    fn parse_endpoint(
        raw_url: &str,
        append_traces_path: bool,
        service_name: impl Into<String>,
    ) -> Result<Self, String> {
        let trimmed = raw_url.trim();
        if trimmed.is_empty() {
            return Err("OTLP endpoint URL must not be empty".to_string());
        }
        let mut parsed = Url::parse(trimmed).map_err(|_| {
            "invalid OTLP endpoint URL (expected http:// or https:// URL)".to_string()
        })?;

        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err("OTLP endpoint URL must not contain embedded credentials".to_string());
        }
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(format!(
                "unsupported OTLP endpoint scheme '{}' (only http and https are supported)",
                parsed.scheme()
            ));
        }
        if parsed.host_str().is_none_or(|h| h.is_empty())
            || trimmed
                .split_once("://")
                .is_some_and(|(_, after)| after.starts_with('/'))
        {
            return Err("OTLP endpoint URL must include a host".to_string());
        }

        if append_traces_path {
            let current_path = parsed.path().trim_end_matches('/');
            if !current_path.ends_with("/v1/traces") {
                let new_path = if current_path.is_empty() {
                    "/v1/traces".to_string()
                } else {
                    format!("{current_path}/v1/traces")
                };
                parsed.set_path(&new_path);
            }
        }

        let raw_service = service_name.into();
        let cleaned_service = raw_service.trim();
        let service_name = if cleaned_service.is_empty() {
            "oxideswarm-master".to_string()
        } else {
            cleaned_service.chars().take(128).collect()
        };

        Ok(Self {
            endpoint: parsed,
            service_name,
        })
    }

    /// Resolves OTLP configuration from standard OpenTelemetry environment variables:
    /// `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`.
    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_env_with(|key| std::env::var(key).ok())
    }

    /// Resolves OTLP configuration using a caller-supplied environment lookup function.
    pub fn from_env_with<F>(lookup: F) -> Result<Option<Self>, String>
    where
        F: Fn(&str) -> Option<String>,
    {
        let service_name =
            lookup("OTEL_SERVICE_NAME").unwrap_or_else(|| "oxideswarm-master".to_string());

        if let Some(traces_url) = lookup("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
            if !traces_url.trim().is_empty() {
                return Self::parse_traces_endpoint(&traces_url, service_name).map(Some);
            }
        }
        if let Some(base_url) = lookup("OTEL_EXPORTER_OTLP_ENDPOINT") {
            if !base_url.trim().is_empty() {
                return Self::parse_base_endpoint(&base_url, service_name).map(Some);
            }
        }
        Ok(None)
    }

    /// Resolved target URI path + query sent in HTTP/1.1 requests.
    pub fn target_path_and_query(&self) -> String {
        let path = if self.endpoint.path().is_empty() {
            "/"
        } else {
            self.endpoint.path()
        };
        match self.endpoint.query() {
            Some(q) => format!("{path}?{q}"),
            None => path.to_string(),
        }
    }

    /// Validated `service.name` resource attribute.
    pub fn service_name(&self) -> &str {
        &self.service_name
    }
}

#[derive(Debug, Clone, Serialize)]
enum OtlpAttrValue {
    #[serde(rename = "stringValue")]
    String(String),
    #[serde(rename = "intValue")]
    Int(String),
    #[serde(rename = "boolValue")]
    Bool(bool),
}

#[derive(Debug, Clone, Serialize)]
struct OtlpAttribute {
    key: &'static str,
    value: OtlpAttrValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpStatus {
    code: u8,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: &'static str,
    kind: u8,
    start_time_unix_nano: String,
    end_time_unix_nano: String,
    attributes: Vec<OtlpAttribute>,
    status: OtlpStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResource {
    attributes: Vec<OtlpAttribute>,
}

#[derive(Debug, Serialize)]
struct OtlpScope {
    name: &'static str,
    version: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpScopeSpans {
    scope: OtlpScope,
    spans: Vec<OtlpSpan>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResourceSpans {
    resource: OtlpResource,
    scope_spans: Vec<OtlpScopeSpans>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportTraceServiceRequest {
    resource_spans: Vec<OtlpResourceSpans>,
}

#[derive(Debug, Clone)]
struct CapturedSpanData {
    name: &'static str,
    span_id: [u8; 8],
    start_nanos: u64,
    task_id: Option<String>,
    worker_id: Option<String>,
    priority: Option<i64>,
    requires_gpu: Option<bool>,
    verdict: Option<String>,
    outcome: Option<String>,
    exit_code: Option<i64>,
    attempt: Option<i64>,
}

impl CapturedSpanData {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            span_id: next_span_id(),
            start_nanos: unix_nanos_now(),
            task_id: None,
            worker_id: None,
            priority: None,
            requires_gpu: None,
            verdict: None,
            outcome: None,
            exit_code: None,
            attempt: None,
        }
    }

    fn apply_field_str(&mut self, key: &str, value: &str) {
        let bounded: String = value.chars().take(128).collect();
        match key {
            "task_id" => self.task_id = Some(bounded),
            "worker_id" => self.worker_id = Some(bounded),
            "verdict" => self.verdict = Some(bounded),
            "outcome" => self.outcome = Some(bounded),
            _ => {}
        }
    }

    fn into_otlp_span(self) -> Option<OtlpSpan> {
        let task_raw = self.task_id?;
        let (trace_id, parent_span_id) = match Uuid::parse_str(task_raw.trim()) {
            Ok(uuid) => (trace_id_for_task(uuid), root_span_id_for_task(uuid)),
            Err(_) => {
                let derived =
                    blake3::derive_key("oxideswarm.otel.trace_id.v1", task_raw.as_bytes());
                let mut t = [0u8; 16];
                t.copy_from_slice(&derived[..16]);
                if t.iter().all(|&b| b == 0) {
                    t[15] = 1;
                }
                let mut p = [0u8; 8];
                p.copy_from_slice(&derived[16..24]);
                if p.iter().all(|&b| b == 0) {
                    p[7] = 1;
                }
                (t, p)
            }
        };

        let end_nanos = unix_nanos_now().max(self.start_nanos.saturating_add(1));
        let mut attrs = Vec::with_capacity(8);
        attrs.push(OtlpAttribute {
            key: "task_id",
            value: OtlpAttrValue::String(task_raw),
        });
        if let Some(worker_id) = self.worker_id {
            attrs.push(OtlpAttribute {
                key: "worker_id",
                value: OtlpAttrValue::String(worker_id),
            });
        }
        if let Some(priority) = self.priority {
            attrs.push(OtlpAttribute {
                key: "priority",
                value: OtlpAttrValue::Int(priority.to_string()),
            });
        }
        if let Some(requires_gpu) = self.requires_gpu {
            attrs.push(OtlpAttribute {
                key: "requires_gpu",
                value: OtlpAttrValue::Bool(requires_gpu),
            });
        }
        if let Some(ref verdict) = self.verdict {
            attrs.push(OtlpAttribute {
                key: "verdict",
                value: OtlpAttrValue::String(verdict.clone()),
            });
        }
        let is_error = matches!(
            self.outcome.as_deref(),
            Some("failed" | "unfulfillable" | "dispatch_failed")
        );
        if let Some(outcome) = self.outcome {
            attrs.push(OtlpAttribute {
                key: "outcome",
                value: OtlpAttrValue::String(outcome),
            });
        }
        if let Some(exit_code) = self.exit_code {
            attrs.push(OtlpAttribute {
                key: "exit_code",
                value: OtlpAttrValue::Int(exit_code.to_string()),
            });
        }
        if let Some(attempt) = self.attempt {
            attrs.push(OtlpAttribute {
                key: "attempt",
                value: OtlpAttrValue::Int(attempt.to_string()),
            });
        }

        Some(OtlpSpan {
            trace_id: encode_hex(&trace_id),
            span_id: encode_hex(&self.span_id),
            parent_span_id: encode_hex(&parent_span_id),
            name: self.name,
            kind: 1, // SPAN_KIND_INTERNAL
            start_time_unix_nano: self.start_nanos.to_string(),
            end_time_unix_nano: end_nanos.to_string(),
            attributes: attrs,
            status: OtlpStatus {
                code: if is_error { 2 } else { 1 },
            },
        })
    }
}

struct AllowlistVisitor<'a>(&'a mut CapturedSpanData);

impl Visit for AllowlistVisitor<'_> {
    fn record_i64(&mut self, field: &Field, value: i64) {
        match field.name() {
            "priority" => self.0.priority = Some(value),
            "exit_code" => self.0.exit_code = Some(value),
            "attempt" => self.0.attempt = Some(value),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let clamped = i64::try_from(value).unwrap_or(i64::MAX);
        self.record_i64(field, clamped);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "requires_gpu" {
            self.0.requires_gpu = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.apply_field_str(field.name(), value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if matches!(
            field.name(),
            "task_id" | "worker_id" | "verdict" | "outcome"
        ) {
            let formatted = format!("{value:?}");
            let trimmed = formatted.trim_matches('"');
            self.0.apply_field_str(field.name(), trimmed);
        }
    }
}

enum ExporterCmd {
    Span(OtlpSpan),
    Flush(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

/// Tracing [`Layer`] capturing allowlisted `oxideswarm::otel` lifecycle spans cleanly.
#[derive(Clone)]
pub struct OtlpTracingLayer {
    tx: mpsc::Sender<ExporterCmd>,
    dropped_spans: Arc<AtomicU64>,
}

impl OtlpTracingLayer {
    /// Returns the number of spans dropped due to a full export channel.
    pub fn dropped_spans(&self) -> u64 {
        self.dropped_spans.load(Ordering::Relaxed)
    }

    fn is_captured_metadata(meta: &Metadata<'_>) -> Option<&'static str> {
        if meta.target() != OTEL_SPAN_TARGET {
            return None;
        }
        ALLOWED_SPAN_NAMES
            .iter()
            .copied()
            .find(|&allowed| allowed == meta.name())
    }
}

impl<S> Layer<S> for OtlpTracingLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(static_name) = Self::is_captured_metadata(attrs.metadata()) else {
            return;
        };
        let mut data = CapturedSpanData::new(static_name);
        attrs.record(&mut AllowlistVisitor(&mut data));
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(data);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut exts = span.extensions_mut();
            if let Some(data) = exts.get_mut::<CapturedSpanData>() {
                values.record(&mut AllowlistVisitor(data));
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(data) = span.extensions_mut().remove::<CapturedSpanData>() else {
            return;
        };
        let Some(otlp_span) = data.into_otlp_span() else {
            return;
        };
        if self.tx.try_send(ExporterCmd::Span(otlp_span)).is_err() {
            self.dropped_spans.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Handle controlling flush and clean shutdown of the background OTLP exporter task.
pub struct OtlpExporterHandle {
    tx: mpsc::Sender<ExporterCmd>,
    dropped_spans: Arc<AtomicU64>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl OtlpExporterHandle {
    /// Flushes any queued spans to the configured OTLP collector and waits up to 5 seconds.
    pub async fn flush(&self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(ExporterCmd::Flush(ack_tx)).await.is_ok() {
            let _ = tokio::time::timeout(FLUSH_WAIT_TIMEOUT, ack_rx).await;
        }
    }

    /// Flushes queued spans and stops the background exporter task.
    pub async fn shutdown(mut self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(ExporterCmd::Shutdown(ack_tx)).await.is_ok() {
            let _ = tokio::time::timeout(FLUSH_WAIT_TIMEOUT, ack_rx).await;
        }
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
    }

    /// Returns total spans dropped due to a full bounded export queue.
    pub fn dropped_spans(&self) -> u64 {
        self.dropped_spans.load(Ordering::Relaxed)
    }
}

impl Drop for OtlpExporterHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Spawns the background OTLP/HTTP JSON exporter task on the active Tokio runtime.
pub fn start_otlp_exporter(config: OtlpConfig) -> (OtlpTracingLayer, OtlpExporterHandle) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let dropped_spans = Arc::new(AtomicU64::new(0));
    let task = tokio::spawn(run_exporter_loop(config, rx));
    let layer = OtlpTracingLayer {
        tx: tx.clone(),
        dropped_spans: Arc::clone(&dropped_spans),
    };
    let handle = OtlpExporterHandle {
        tx,
        dropped_spans,
        task: Some(task),
    };
    (layer, handle)
}

async fn run_exporter_loop(config: OtlpConfig, mut rx: mpsc::Receiver<ExporterCmd>) {
    let tls_connector = if config.endpoint.scheme() == "https" {
        match build_tls_connector() {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load TLS root certificates for OTLP exporter");
                None
            }
        }
    } else {
        None
    };

    let mut buffer: Vec<OtlpSpan> = Vec::with_capacity(MAX_BATCH_SPANS);
    loop {
        let flush_sleep = tokio::time::sleep(BATCH_FLUSH_INTERVAL);
        tokio::pin!(flush_sleep);

        enum Action {
            FlushAck(oneshot::Sender<()>),
            ShutdownAck(oneshot::Sender<()>),
            Closed,
            Tick,
        }

        let action = loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(ExporterCmd::Span(span)) => {
                        buffer.push(span);
                        if buffer.len() >= MAX_BATCH_SPANS {
                            flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
                        }
                    }
                    Some(ExporterCmd::Flush(ack)) => break Action::FlushAck(ack),
                    Some(ExporterCmd::Shutdown(ack)) => break Action::ShutdownAck(ack),
                    None => break Action::Closed,
                },
                _ = &mut flush_sleep, if !buffer.is_empty() => {
                    break Action::Tick;
                }
            }
        };

        // Drain any additional buffered spans before sending.
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                ExporterCmd::Span(span) => buffer.push(span),
                ExporterCmd::Flush(ack) => {
                    flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
                    let _ = ack.send(());
                }
                ExporterCmd::Shutdown(ack) => {
                    flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
                    let _ = ack.send(());
                    return;
                }
            }
        }

        match action {
            Action::Tick => {
                flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
            }
            Action::FlushAck(ack) => {
                flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
                let _ = ack.send(());
            }
            Action::ShutdownAck(ack) => {
                flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
                let _ = ack.send(());
                return;
            }
            Action::Closed => {
                flush_buffer(&config, tls_connector.as_ref(), &mut buffer).await;
                return;
            }
        }
    }
}

async fn flush_buffer(
    config: &OtlpConfig,
    tls: Option<&tokio_rustls::TlsConnector>,
    buffer: &mut Vec<OtlpSpan>,
) {
    while !buffer.is_empty() {
        let take = buffer.len().min(MAX_BATCH_SPANS);
        let spans: Vec<OtlpSpan> = buffer.drain(..take).collect();
        if let Err(err) = send_otlp_batch(config, tls, spans).await {
            tracing::warn!(error = %err, "Failed to deliver OTLP trace batch");
        }
    }
}

fn build_tls_connector() -> Result<tokio_rustls::TlsConnector, String> {
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    for cert in loaded.certs {
        let _ = roots.add(cert);
    }
    let tls_config = tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("TLS protocol config error: {e}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(tls_config)))
}

async fn send_otlp_batch(
    config: &OtlpConfig,
    tls: Option<&tokio_rustls::TlsConnector>,
    spans: Vec<OtlpSpan>,
) -> Result<(), String> {
    let payload = ExportTraceServiceRequest {
        resource_spans: vec![OtlpResourceSpans {
            resource: OtlpResource {
                attributes: vec![OtlpAttribute {
                    key: "service.name",
                    value: OtlpAttrValue::String(config.service_name.clone()),
                }],
            },
            scope_spans: vec![OtlpScopeSpans {
                scope: OtlpScope {
                    name: "oxideswarm.master",
                    version: env!("CARGO_PKG_VERSION"),
                },
                spans,
            }],
        }],
    };
    let body = serde_json::to_vec(&payload).map_err(|e| format!("OTLP JSON encode error: {e}"))?;

    let host = config
        .endpoint
        .host_str()
        .ok_or_else(|| "missing OTLP host".to_string())?;
    let port = config
        .endpoint
        .port_or_known_default()
        .ok_or_else(|| "missing OTLP port".to_string())?;
    let connect_host = host.trim_start_matches('[').trim_end_matches(']');
    let addr = format!("{connect_host}:{port}");
    let host_header = match config.endpoint.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    };
    let target = config.target_path_and_query();

    let mut request = Vec::with_capacity(256 + body.len());
    let headers = format!(
        "POST {target} HTTP/1.1\r\n\
         Host: {host_header}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    request.extend_from_slice(headers.as_bytes());
    request.extend_from_slice(&body);

    let fut = async {
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("OTLP connect error: {e}"))?;
        if config.endpoint.scheme() == "https" {
            let connector = tls.ok_or_else(|| "TLS connector unavailable".to_string())?;
            let server_name =
                tokio_rustls::rustls::pki_types::ServerName::try_from(connect_host.to_string())
                    .map_err(|_| "invalid OTLP TLS server name".to_string())?;
            let mut tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| format!("OTLP TLS handshake error: {e}"))?;
            write_and_check_http_status(&mut tls_stream, &request).await
        } else {
            let mut tcp = tcp;
            write_and_check_http_status(&mut tcp, &request).await
        }
    };

    tokio::time::timeout(EXPORT_HTTP_TIMEOUT, fut)
        .await
        .map_err(|_| "OTLP export HTTP request timed out".to_string())?
}

async fn write_and_check_http_status<S>(stream: &mut S, request: &[u8]) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    stream
        .write_all(request)
        .await
        .map_err(|e| format!("OTLP write error: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("OTLP flush error: {e}"))?;

    let mut buf = [0u8; 512];
    let n = stream
        .read(&mut buf)
        .await
        .map_err(|e| format!("OTLP read error: {e}"))?;
    let status_line = std::str::from_utf8(&buf[..n])
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("");
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if (200..300).contains(&code) {
        Ok(())
    } else {
        Err(format!("OTLP endpoint returned HTTP status {code}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn deterministic_trace_and_root_span_ids_are_non_zero_and_stable() {
        let task_id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let t1 = trace_id_for_task(task_id);
        let t2 = trace_id_for_task(task_id);
        let s1 = root_span_id_for_task(task_id);

        assert_eq!(t1, t2);
        assert_ne!(t1, [0u8; 16]);
        assert_ne!(s1, [0u8; 8]);
        assert_ne!(t1, trace_id_for_task(other));
    }

    #[test]
    fn otlp_config_resolves_endpoints_and_rejects_bad_urls_without_leaking_secrets() {
        let env1: HashMap<&str, &str> = [
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
            ("OTEL_SERVICE_NAME", "custom-master"),
        ]
        .into_iter()
        .collect();
        let cfg1 = OtlpConfig::from_env_with(|k| env1.get(k).map(|s| (*s).to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(cfg1.target_path_and_query(), "/v1/traces");
        assert_eq!(cfg1.service_name(), "custom-master");

        let env2: HashMap<&str, &str> = [(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "https://collector.example:4318/custom/path",
        )]
        .into_iter()
        .collect();
        let cfg2 = OtlpConfig::from_env_with(|k| env2.get(k).map(|s| (*s).to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(cfg2.target_path_and_query(), "/custom/path");

        for bad in [
            "http://user:supersecret@127.0.0.1:4318",
            "ftp://127.0.0.1:4318/v1/traces",
            "not-a-url",
            "http:///v1/traces",
        ] {
            let err = OtlpConfig::parse_base_endpoint(bad, "test").unwrap_err();
            assert!(
                !err.contains("supersecret") && !err.contains("user:"),
                "error echoed credential: {err}"
            );
        }
    }

    #[test]
    fn allowlist_visitor_drops_secrets_commands_and_unknown_fields() {
        let tid = Uuid::new_v4();
        let mut data = CapturedSpanData::new("submit");
        data.apply_field_str("task_id", &tid.to_string());
        data.apply_field_str("command", "echo SECRET_TOKEN_123");
        data.apply_field_str("cluster_secret", "top-secret-hex");
        data.apply_field_str("outcome", "accepted");

        let span = data.into_otlp_span().unwrap();
        let json = serde_json::to_string(&span).unwrap();
        assert!(json.contains("accepted"));
        assert!(!json.contains("SECRET_TOKEN_123"));
        assert!(!json.contains("top-secret-hex"));
        assert!(!json.contains("cluster_secret"));
    }
}
