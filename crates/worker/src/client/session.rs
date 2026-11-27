//! Worker transport connection, authentication handshake, and active session loop.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
#[cfg(feature = "p2p")]
use tracing::error;
use tracing::{debug, info, warn};

use rusty_grid_core::capabilities::MobileLiveStatus;
use rusty_grid_core::cluster_auth::{client_handshake, WIRE_FORMAT_AUTH};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{v3_client_handshake, HandshakePurpose};
use rusty_grid_core::protocol::{
    format_incompatible_version_error, is_master_echo_acceptable, master_features, EndpointRole,
    MasterMessage, MessageReader, MessageTransport, MessageWriter, ProtocolCapabilities,
    ProtocolError, WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{TaskId, TaskResult};

use crate::backoff::ExponentialBackoff;
use crate::blob_client::{upload_task_outputs, BlobClient};
use crate::heartbeat::{current_epoch_secs, spawn_adaptive_heartbeat_task, IoClock, StampedStream};
use crate::runner::EXIT_CODE_GENERAL_ERROR;

use super::tasks::BufferedTaskResult;
use super::{
    current_evacuation_reason, AbortOnDrop, WorkerClient, MOBILE_TELEMETRY_CHECK_INTERVAL,
};

/// How long a closing session lets its writers flush the farewell message.
const FAREWELL_TIMEOUT: Duration = Duration::from_secs(1);

/// Capacity of the per-session control queue (heartbeats, telemetry, farewell).
const CONTROL_QUEUE_CAPACITY: usize = 16;

/// Capacity of the per-session data queue (task progress, results, checkpoints).
const DATA_QUEUE_CAPACITY: usize = 64;

/// After this many consecutive failed P2P dials the log escalates to an error: the ticket is
/// almost certainly stale (the Master restarted with a new identity), not momentarily unlucky.
#[cfg(feature = "p2p")]
const P2P_STALE_ERROR_AFTER: u32 = 3;

/// Resolves when shutdown was requested (the flag is `true`) or the sender was dropped.
/// Cancel-safe, so it can sit in a `select!` next to any connect/handshake future.
pub(super) async fn shutdown_requested(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Connection stream mode to Master: single stream (direct TCP) or 2-lane QUIC multiplexed streams (iroh P2P).
pub enum WorkerStreamMode {
    Single(rusty_grid_core::transport::GridStream),
    #[cfg(feature = "p2p")]
    Multiplexed {
        control: rusty_grid_core::transport::GridStream,
        data: rusty_grid_core::transport::GridStream,
    },
}

/// What the lane tasks of a session report to its loop.
pub(super) enum Inbound {
    /// A decoded frame from the Master (boxed: a `Task` makes it much larger than the other events).
    Message(Box<MasterMessage>),
    /// The Master closed the lane (EOF).
    Closed(&'static str),
    /// Reading from or writing to the lane failed.
    Failed(&'static str, ProtocolError),
}

/// A receiver whose sender is gone: a writer lane that does not serve this queue.
fn closed_queue() -> mpsc::Receiver<WorkerMessage> {
    let (_tx, rx) = mpsc::channel(1);
    rx
}

/// Truncates `text` to at most `max_bytes` on a UTF-8 character boundary.
fn truncate_utf8_prefix(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Rewrites an oversized `TaskResult` into a failed `WorkerMessage::TaskResult` that fits within
/// `MAX_FRAME_SIZE` by dropping `asset_bytes` first, then `artifacts`, and then `stdout` and
/// `stderr`, stopping as soon as the serialized frame fits.
pub(super) fn rewrite_oversized_task_result(
    result: TaskResult,
    size: usize,
    max: usize,
    codec: WireCodec,
    encrypted: bool,
) -> WorkerMessage {
    const MAX_PRESERVED_ERROR_BYTES: usize = 4096;

    let exit_code = if result.exit_code != 0 {
        result.exit_code
    } else {
        EXIT_CODE_GENERAL_ERROR
    };
    let overhead = if encrypted {
        rusty_grid_core::protocol::AEAD_RECORD_OVERHEAD
    } else {
        0
    };
    let fits = |msg: &WorkerMessage| {
        rusty_grid_core::protocol::serialize_message(msg, codec)
            .is_ok_and(|bytes| bytes.len().saturating_add(overhead) <= max)
    };

    let build = |keep_artifacts: bool, keep_streams: bool| -> WorkerMessage {
        let mut dropped = Vec::new();
        if result.asset_bytes.is_some() {
            dropped.push("asset_bytes");
        }
        if !keep_artifacts && !result.artifacts.is_empty() {
            dropped.push("artifacts");
        }
        if !keep_streams {
            if !result.stdout.is_empty() {
                dropped.push("stdout");
            }
            if !result.stderr.is_empty() {
                dropped.push("stderr");
            }
        }
        if dropped.is_empty() {
            dropped.push("stdout, stderr");
        }
        let reason = format!(
            "Task result size ({size} bytes) exceeds maximum frame limit ({max} bytes); dropped {}",
            dropped.join(", ")
        );
        let combined_error = match result.error.as_deref().filter(|s| !s.is_empty()) {
            Some(orig) => {
                let prefix = truncate_utf8_prefix(orig, MAX_PRESERVED_ERROR_BYTES);
                format!("{prefix}; {reason}")
            }
            None => reason.clone(),
        };
        let (stdout, stderr) = if keep_streams {
            let mut err_bytes = result.stderr.0.to_vec();
            if !err_bytes.is_empty() && !err_bytes.ends_with(b"\n") {
                err_bytes.push(b'\n');
            }
            err_bytes.extend_from_slice(reason.as_bytes());
            (
                result.stdout.clone(),
                rusty_grid_core::task::Bytes::from(err_bytes),
            )
        } else {
            (
                rusty_grid_core::task::Bytes::new(),
                rusty_grid_core::task::Bytes::from(reason),
            )
        };
        let mut candidate = TaskResult::failure(
            result.worker_id,
            result.task_id,
            exit_code,
            stdout,
            stderr,
            result.execution_time_ms,
            Some(combined_error),
        );
        candidate.is_gpu_executed = result.is_gpu_executed;
        candidate.device_name = result.device_name.clone();
        if keep_artifacts {
            candidate.artifacts = result.artifacts.clone();
        }
        candidate.outputs = result.outputs.clone();
        WorkerMessage::from(candidate)
    };

    if result.asset_bytes.is_some() {
        let candidate = build(true, true);
        if fits(&candidate) {
            return candidate;
        }
    }
    if !result.artifacts.is_empty() {
        let candidate = build(false, true);
        if fits(&candidate) {
            return candidate;
        }
    }
    build(false, false)
}

/// Writes queued messages to one lane until told to close or until a write fails.
///
/// The control queue (heartbeats, telemetry, the farewell) is always drained before the next data
/// message (progress, results, checkpoints) is started, so a burst of results can never push a
/// heartbeat back by more than the one frame already on the wire. An oversized `TaskResult` is
/// rewritten into a failure that fits (`rewrite_oversized_task_result`) and an oversized
/// `Checkpoint` is dropped with a warning; any other write failure is reported through `events`.
/// On `closing` the writer first flushes the control queue and then returns.
pub(super) async fn lane_writer<W: AsyncWrite + Unpin>(
    mut writer: MessageWriter<W>,
    mut control_rx: mpsc::Receiver<WorkerMessage>,
    mut data_rx: mpsc::Receiver<WorkerMessage>,
    lane: &'static str,
    mut closing: watch::Receiver<bool>,
    events: mpsc::Sender<Inbound>,
) {
    loop {
        let msg = match control_rx.try_recv() {
            Ok(msg) => msg,
            Err(_) => {
                if *closing.borrow() {
                    return;
                }
                tokio::select! {
                    biased;
                    Some(msg) = control_rx.recv() => msg,
                    changed = closing.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue;
                    }
                    Some(msg) = data_rx.recv() => msg,
                    else => return,
                }
            }
        };
        if let Err(e) = writer.send_msg(&msg).await {
            if let ProtocolError::FrameTooLarge { size, max } = e {
                match msg {
                    WorkerMessage::TaskResult {
                        worker_id,
                        task_id,
                        exit_code,
                        stdout,
                        stderr,
                        execution_time_ms,
                        is_gpu_executed,
                        device_name,
                        error,
                        asset_bytes,
                        artifacts,
                        outputs,
                    } => {
                        warn!(
                            %worker_id,
                            %task_id,
                            size,
                            max,
                            "Task result exceeds maximum frame size; failing only this task"
                        );
                        let fallback = rewrite_oversized_task_result(
                            TaskResult {
                                worker_id,
                                task_id,
                                exit_code,
                                stdout,
                                stderr,
                                execution_time_ms,
                                is_gpu_executed,
                                device_name,
                                error,
                                asset_bytes,
                                artifacts,
                                outputs,
                            },
                            size,
                            max,
                            writer.codec(),
                            writer.is_encrypted(),
                        );
                        if let Err(fallback_err) = writer.send_msg(&fallback).await {
                            let _ = events.send(Inbound::Failed(lane, fallback_err)).await;
                            return;
                        }
                        continue;
                    }
                    WorkerMessage::Checkpoint {
                        task_id, sequence, ..
                    } => {
                        warn!(
                            %task_id,
                            sequence,
                            size,
                            max,
                            "Checkpoint exceeds maximum frame size; dropping oversized checkpoint"
                        );
                        continue;
                    }
                    _ => {}
                }
            }
            let _ = events.send(Inbound::Failed(lane, e)).await;
            return;
        }
    }
}

/// Reads frames from one lane and forwards them to the session loop, in their own task so that
/// the loop never has to cancel a half-finished read.
pub(super) fn spawn_lane_reader<R: AsyncRead + Unpin + Send + 'static>(
    mut reader: MessageReader<R>,
    lane: &'static str,
    events: mpsc::Sender<Inbound>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let event = match reader.recv_msg::<MasterMessage>().await {
                Ok(Some(msg)) => Inbound::Message(Box::new(msg)),
                Ok(None) => Inbound::Closed(lane),
                Err(e) => Inbound::Failed(lane, e),
            };
            let last = !matches!(event, Inbound::Message(_));
            if events.send(event).await.is_err() || last {
                return;
            }
        }
    })
}

/// The tasks and queues of one registered connection.
pub(super) struct Session {
    pub(super) control_tx: mpsc::Sender<WorkerMessage>,
    pub(super) data_tx: mpsc::Sender<WorkerMessage>,
    pub(super) control_rx: Option<mpsc::Receiver<WorkerMessage>>,
    pub(super) data_rx: Option<mpsc::Receiver<WorkerMessage>>,
    pub(super) inbound_tx: mpsc::Sender<Inbound>,
    pub(super) inbound_rx: mpsc::Receiver<Inbound>,
    pub(super) closing_tx: watch::Sender<bool>,
    pub(super) writers: Vec<JoinHandle<()>>,
    pub(super) readers: Vec<JoinHandle<()>>,
    pub(super) clock: Arc<IoClock>,
}

impl Session {
    pub(super) fn new(clock: Arc<IoClock>) -> Self {
        let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
        let (data_tx, data_rx) = mpsc::channel(DATA_QUEUE_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let (closing_tx, _) = watch::channel(false);
        Self {
            control_tx,
            data_tx,
            control_rx: Some(control_rx),
            data_rx: Some(data_rx),
            inbound_tx,
            inbound_rx,
            closing_tx,
            writers: Vec::new(),
            readers: Vec::new(),
            clock,
        }
    }

    /// Starts the reader and writer tasks of one lane. `control` / `data` say which outbound
    /// queues this lane serves: a single TCP stream serves both, a QUIC session one each.
    pub(super) fn attach_lane<W, R>(
        &mut self,
        writer: MessageWriter<W>,
        reader: MessageReader<R>,
        lane: &'static str,
        control: bool,
        data: bool,
    ) where
        W: AsyncWrite + Unpin + Send + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        let control_rx = control
            .then(|| self.control_rx.take())
            .flatten()
            .unwrap_or_else(closed_queue);
        let data_rx = data
            .then(|| self.data_rx.take())
            .flatten()
            .unwrap_or_else(closed_queue);
        self.writers.push(tokio::spawn(lane_writer(
            writer,
            control_rx,
            data_rx,
            lane,
            self.closing_tx.subscribe(),
            self.inbound_tx.clone(),
        )));
        self.readers
            .push(spawn_lane_reader(reader, lane, self.inbound_tx.clone()));
    }

    pub(super) fn stop_lanes(&self) {
        for task in self.writers.iter().chain(self.readers.iter()) {
            task.abort();
        }
    }

    /// Ends the session: queues `farewell`, lets the writers flush the control queue (bounded),
    /// then stops every lane task, which closes the sockets.
    pub(super) async fn close(mut self, farewell: Option<WorkerMessage>) {
        if let Some(msg) = farewell {
            let _ = self.control_tx.try_send(msg);
        }
        let _ = self.closing_tx.send(true);
        let flush = async {
            for writer in &mut self.writers {
                let _ = writer.await;
            }
        };
        let _ = tokio::time::timeout(FAREWELL_TIMEOUT, flush).await;
        self.stop_lanes();
    }
}

/// The lane tasks are spawned, so they would outlive a dropped `run()` future (a caller aborting
/// the worker task, a panic) and keep the socket open: the Master would never see the connection
/// end. Dropping the session therefore stops them, which closes the socket exactly as dropping
/// the connection did when the lanes were polled inside the future.
impl Drop for Session {
    fn drop(&mut self) {
        self.stop_lanes();
    }
}

impl WorkerClient {
    /// Evaluates active D1 contributor limits (optionally syncing updated `limits set` config)
    /// and updates [`ControlHandle::set_held_with_detail`].
    pub(super) async fn sync_and_evaluate_limits(&self, sync_from_control: bool) {
        if sync_from_control {
            let latest = crate::limits::WorkerLimits::from_control_config(&self.control.limits());
            if let Ok(mut eval) = self.limits_evaluator.lock() {
                if let Err(err) = eval.update_limits(latest) {
                    warn!(error = %err, "Failed to apply updated contributor limits");
                }
            }
        }

        let (need_battery, need_metered, need_cpu) = {
            let Ok(eval) = self.limits_evaluator.lock() else {
                return;
            };
            let lim = eval.limits();
            (
                lim.no_tasks_on_battery,
                lim.no_tasks_on_metered,
                lim.local_max_cpu_pct.is_some(),
            )
        };

        let battery = if need_battery {
            if let Some(probe) = self.battery_probe.as_ref().map(Arc::clone) {
                tokio::task::spawn_blocking(move || probe.probe_battery())
                    .await
                    .unwrap_or(crate::limits::BatteryStatus::Unknown)
            } else {
                crate::limits::BatteryStatus::Unknown
            }
        } else {
            crate::limits::BatteryStatus::Unknown
        };

        let metered = if need_metered {
            let probe = Arc::clone(&self.metered_probe);
            tokio::task::spawn_blocking(move || probe.probe_metered())
                .await
                .unwrap_or(crate::limits::MeteredStatus::Unknown)
        } else {
            crate::limits::MeteredStatus::Unknown
        };

        let host_cpu_pct = if need_cpu {
            self.cpu_probe.probe_cpu_pct()
        } else {
            None
        };

        let sample = crate::limits::LimitSample {
            now: std::time::Instant::now(),
            local_now: self.local_clock.local_now(),
            host_cpu_pct,
            battery,
            metered,
        };
        let eval_res = {
            let Ok(mut eval) = self.limits_evaluator.lock() else {
                return;
            };
            eval.evaluate(sample)
        };
        self.control.set_held_with_detail(
            eval_res.held,
            eval_res.reason,
            eval_res.local_detail,
            eval_res.hand_back_running,
        );
    }

    async fn should_reconnect_after_hold_clears(&self, session: &Session) -> bool {
        !self.control.is_paused_or_held()
            && self.active_task_table.read().await.is_empty()
            && session.data_tx.capacity() == session.data_tx.max_capacity()
            && self.control.take_session_reconnect_needed()
    }

    /// True when a plain TCP address (or `auto` discovery) is configured next to a P2P ticket.
    #[cfg(feature = "p2p")]
    pub(super) fn has_tcp_fallback(&self) -> bool {
        !self.config.master_addr.trim().is_empty()
    }

    /// Establishes an underlying stream to the Master (either P2P QUIC via ticket, or direct TCP).
    ///
    /// With a ticket the P2P dial comes first; if it fails and a TCP address is configured too
    /// (the Android app passes both) the worker falls back to TCP instead of retrying a stale
    /// ticket forever: after the Master restarts with a new identity the old ticket never works
    /// again, while its LAN address still does.
    pub(super) async fn establish_stream(&self) -> GridResult<(WorkerStreamMode, String)> {
        #[cfg(feature = "p2p")]
        if self.config.p2p_ticket.is_some() {
            return match self.establish_p2p().await {
                Ok(established) => {
                    self.p2p_failures.store(0, Ordering::Relaxed);
                    Ok(established)
                }
                Err(e) => {
                    let failures = self.p2p_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if failures >= P2P_STALE_ERROR_AFTER {
                        error!(
                            worker_id = %self.worker_id,
                            failures,
                            error = %e,
                            "The P2P ticket keeps failing; the Master probably restarted with a new identity (copy its current ticket)"
                        );
                    }
                    if !self.has_tcp_fallback() {
                        return Err(e);
                    }
                    warn!(
                        error = %e,
                        master = %self.config.master_addr,
                        "P2P connect failed; falling back to the Master's TCP address"
                    );
                    self.establish_tcp().await
                }
            };
        }
        self.establish_tcp().await
    }

    /// Dials the Master over iroh QUIC with the configured ticket.
    #[cfg(feature = "p2p")]
    pub(super) async fn establish_p2p(&self) -> GridResult<(WorkerStreamMode, String)> {
        let ticket_str = self.config.p2p_ticket.as_deref().unwrap_or_default();
        self.establish_p2p_with_ticket(ticket_str).await
    }

    /// Dials the Master over iroh QUIC using an explicit P2P ticket string.
    #[cfg(feature = "p2p")]
    pub(super) async fn establish_p2p_with_ticket(
        &self,
        ticket_str: &str,
    ) -> GridResult<(WorkerStreamMode, String)> {
        let node_addr = rusty_grid_core::transport::parse_p2p_ticket(ticket_str)
            .map_err(|e| GridError::Config(format!("Invalid P2P ticket: {e}")))?;
        info!("Connecting to Master via P2P NAT Traversal (iroh QUIC)...");

        let mut ep_guard = self.p2p_endpoint.lock().await;
        let (endpoint, is_cached) = match ep_guard.as_ref() {
            Some(ep) => (ep.clone(), true),
            None => {
                let ep = rusty_grid_core::transport::bind_p2p_endpoint_with_relay(
                    self.config.p2p_minimal_preset,
                    self.config.p2p_relay_url.as_deref(),
                )
                .await?;
                *ep_guard = Some(ep.clone());
                (ep, false)
            }
        };
        drop(ep_guard);

        let connect_timeout = self.config.connect_timeout;
        let reset_cached_endpoint = || async {
            if is_cached {
                let mut guard = self.p2p_endpoint.lock().await;
                *guard = None;
            }
        };
        let conn = match rusty_grid_core::transport::dial_p2p_endpoint(
            &endpoint,
            node_addr,
            connect_timeout,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                reset_cached_endpoint().await;
                return Err(e);
            }
        };

        // Open the two lanes: Control / Telemetry (Tag 0x01) and Data / Workload (Tag 0x02).
        // A peer that accepts the connection but never completes the stream setup must not
        // be able to park the worker here forever.
        let open_lanes = async {
            let (ctrl_send, ctrl_recv) = rusty_grid_core::transport::open_p2p_tagged_stream(
                &conn,
                rusty_grid_core::transport::STREAM_CONTROL,
                "control",
            )
            .await?;
            let (data_send, data_recv) = rusty_grid_core::transport::open_p2p_tagged_stream(
                &conn,
                rusty_grid_core::transport::STREAM_DATA,
                "data",
            )
            .await?;
            GridResult::Ok((ctrl_send, ctrl_recv, data_send, data_recv))
        };
        let (ctrl_send, ctrl_recv, data_send, data_recv) =
            tokio::time::timeout(connect_timeout, open_lanes)
                .await
                .map_err(|_| GridError::Timeout {
                    operation: "p2p_open_streams".into(),
                    duration_secs: connect_timeout.as_secs(),
                })??;

        let ctrl_bi = rusty_grid_core::transport::BiStream::new(ctrl_recv, ctrl_send);
        let data_bi = rusty_grid_core::transport::BiStream::new(data_recv, data_send);
        let path_info = rusty_grid_core::transport::inspect_connection_paths(&conn);
        if let Some(ref info) = path_info {
            info!(
                connection_type = %info.connection_type,
                is_relay = info.is_relay,
                is_ip = info.is_ip,
                rtt_ms = info.rtt_ms,
                remote = %info.remote_addr,
                "P2P transport path active (QUIC 2-lane multiplexed)"
            );
        }
        {
            let mut conn_guard = self.p2p_conn.write().await;
            *conn_guard = Some(conn.clone());
        }

        Ok((
            WorkerStreamMode::Multiplexed {
                control: rusty_grid_core::transport::GridStream::P2p(ctrl_bi),
                data: rusty_grid_core::transport::GridStream::P2p(data_bi),
            },
            format!("iroh://{:?}", conn.remote_id()),
        ))
    }

    /// Connects over plain TCP, discovering the Master first when the address is `auto`.
    pub(super) async fn establish_tcp(&self) -> GridResult<(WorkerStreamMode, String)> {
        let mut target_addr = self.config.master_addr.clone();
        #[cfg(feature = "p2p")]
        let mut discovered_p2p_ticket: Option<String> = None;
        if target_addr.is_empty()
            || target_addr.eq_ignore_ascii_case("auto")
            || target_addr.starts_with("auto:")
        {
            let disc_port = if let Some(port_str) = target_addr.strip_prefix("auto:") {
                port_str
                    .parse::<u16>()
                    .unwrap_or(rusty_grid_core::DEFAULT_DISCOVERY_PORT)
            } else {
                rusty_grid_core::DEFAULT_DISCOVERY_PORT
            };
            info!(
                port = disc_port,
                authenticated = self.config.cluster_secret.is_some(),
                "Master address set to 'auto'; probing local network via UDP discovery..."
            );
            // With a cluster secret only beacons signed for this probe are accepted, so a LAN
            // host that does not know the secret cannot steer the worker to itself.
            match rusty_grid_core::discovery::discover_master_with_secret_and_rendezvous(
                Duration::from_secs(3),
                disc_port,
                self.config.cluster_secret.as_ref(),
                self.config.rendezvous_url.as_deref(),
            )
            .await
            {
                Some(beacon) => {
                    info!(
                        master = %beacon.cluster_addr,
                        hostname = %beacon.hostname,
                        "Discovered active Master via discovery"
                    );
                    #[cfg(feature = "p2p")]
                    {
                        discovered_p2p_ticket = beacon.p2p_ticket.clone();
                    }
                    *self.master_beacon.write().await = Some(beacon.clone());
                    target_addr = beacon.cluster_addr;
                }
                None => {
                    // No silent fallback to a guessed address: an unanswered probe means no
                    // (authentic) master is reachable, and the supervisor retries with backoff.
                    return Err(GridError::ConnectionFailed(format!(
                        "auto-discovery found no Master on UDP port {disc_port}{}; start a Master, or set --master HOST:8088",
                        if self.config.cluster_secret.is_some() {
                            " (only Masters holding the same cluster secret are accepted)"
                        } else {
                            ""
                        }
                    )));
                }
            }
        }

        let connect_timeout = self.config.connect_timeout;
        let stream = match tokio::time::timeout(connect_timeout, TcpStream::connect(&target_addr))
            .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(tcp_err)) => {
                #[cfg(feature = "p2p")]
                if let Some(ticket) = discovered_p2p_ticket.as_deref().filter(|t| !t.is_empty()) {
                    info!(
                        tcp_addr = %target_addr,
                        error = %tcp_err,
                        "TCP connect to discovered Master failed; falling back to discovered P2P ticket"
                    );
                    return self.establish_p2p_with_ticket(ticket).await;
                }
                return Err(GridError::Io(tcp_err));
            }
            Err(_) => {
                #[cfg(feature = "p2p")]
                if let Some(ticket) = discovered_p2p_ticket.as_deref().filter(|t| !t.is_empty()) {
                    info!(
                        tcp_addr = %target_addr,
                        timeout_secs = connect_timeout.as_secs(),
                        "TCP connect to discovered Master timed out; falling back to discovered P2P ticket"
                    );
                    return self.establish_p2p_with_ticket(ticket).await;
                }
                return Err(GridError::Timeout {
                    operation: format!("tcp_connect {target_addr}"),
                    duration_secs: connect_timeout.as_secs(),
                });
            }
        };
        // TCP_NODELAY + keepalive: a Master that vanishes without a FIN (Wi-Fi drop, suspended
        // laptop) is noticed in about a minute instead of the OS default of hours.
        if let Err(e) = rusty_grid_core::transport::configure_tcp_stream(&stream) {
            debug!(error = %e, "Could not apply TCP nodelay/keepalive options to the Master socket");
        }
        let remote = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| target_addr.clone());

        if let Ok(peer) = stream.peer_addr() {
            let mut lock = self.master_beacon.write().await;
            if lock.is_none() {
                let peer_ip = peer.ip();
                *lock = Some(rusty_grid_core::discovery::MasterBeacon::new(
                    target_addr.clone(),
                    format!("http://{}:8080", peer_ip),
                    "MasterNode",
                    1,
                ));
            }
        }
        Ok((
            WorkerStreamMode::Single(rusty_grid_core::transport::GridStream::Tcp(stream)),
            remote,
        ))
    }

    /// Conducts a single connection lifecycle: TCP/P2P connect, registration handshake,
    /// heartbeat task spawning, and bidirectional message processing.
    pub(super) async fn connect_and_run(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        backoff: &mut ExponentialBackoff,
    ) -> GridResult<()> {
        // Discovery, TCP connect / P2P dial and the handshake are all raced against shutdown (and
        // bounded by `connect_timeout` / `handshake_timeout`), so stopping the worker never has to
        // wait for an unreachable Master.
        let mut deadline = self.resume_deadline;
        let established = {
            let establish_fut = self.establish_stream();
            tokio::pin!(establish_fut);
            loop {
                let resume_wait = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = shutdown_requested(shutdown_rx) => break None,
                    _ = resume_wait => {
                        deadline = None;
                        self.expire_resume_window("session-resume grace window expired").await;
                    }
                    res = &mut establish_fut => break Some(res),
                }
            }
        };
        self.resume_deadline = deadline;
        let Some(established) = established else {
            info!(worker_id = %self.worker_id, "Shutdown requested while connecting to Master");
            return Ok(());
        };
        let (stream_mode, remote_desc) = established?;
        info!(master = %remote_desc, worker_id = %self.worker_id, "Connected to Master; initiating handshake");

        let clock = IoClock::new();
        match stream_mode {
            WorkerStreamMode::Single(stream) => {
                if let Some(link_cfg) = self.config.simulated_link.clone() {
                    let wrapped = crate::sim::SimulatedLinkStream::wrap(stream, link_cfg);
                    self.run_single_stream_session(wrapped, clock, backoff, shutdown_rx)
                        .await
                } else {
                    self.run_single_stream_session(stream, clock, backoff, shutdown_rx)
                        .await
                }
            }
            #[cfg(feature = "p2p")]
            WorkerStreamMode::Multiplexed { control, data } => {
                let mut ctrl_transport = MessageTransport::with_codec(
                    StampedStream::new(control, Arc::clone(&clock)),
                    self.config.wire_codec,
                );
                let data_transport = MessageTransport::with_codec(
                    StampedStream::new(data, Arc::clone(&clock)),
                    self.config.wire_codec,
                );

                // Cluster authentication + handshake over the dedicated control stream (Tag 0x01)
                let mut deadline = self.resume_deadline;
                let handshake = {
                    let hs_fut = self.perform_handshake(&mut ctrl_transport);
                    tokio::pin!(hs_fut);
                    loop {
                        let resume_wait = async {
                            match deadline {
                                Some(d) => tokio::time::sleep_until(d).await,
                                None => std::future::pending::<()>().await,
                            }
                        };
                        tokio::select! {
                            biased;
                            _ = shutdown_requested(shutdown_rx) => break None,
                            _ = resume_wait => {
                                deadline = None;
                                self.expire_resume_window("session-resume grace window expired").await;
                            }
                            res = &mut hs_fut => break Some(res),
                        }
                    }
                };
                self.resume_deadline = deadline;
                let heartbeat_interval = match handshake {
                    Some(Ok(interval)) => interval,
                    outcome => {
                        // The QUIC connection stays cached in `p2p_conn` only while it is in use.
                        *self.p2p_conn.write().await = None;
                        return match outcome {
                            Some(Err(e)) => Err(e),
                            _ => {
                                info!(worker_id = %self.worker_id, "Shutdown requested during handshake");
                                Ok(())
                            }
                        };
                    }
                };
                backoff.reset();
                info!(worker_id = %self.worker_id, "Registration accepted over QUIC control stream; backoff counter reset");

                // Control traffic (heartbeats, telemetry) and bulk data (results) each get their own
                // stream *and* their own writer task, so a result upload cannot hold a heartbeat back.
                let (ctrl_writer, ctrl_reader) = ctrl_transport.split();
                let (data_writer, data_reader) = data_transport.split();
                let mut session = Session::new(clock);
                session.attach_lane(ctrl_writer, ctrl_reader, "control", true, false);
                session.attach_lane(data_writer, data_reader, "data", false, true);
                self.run_session(session, heartbeat_interval, shutdown_rx)
                    .await
            }
        }
    }

    pub(super) async fn run_single_stream_session<
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    >(
        &mut self,
        stream: S,
        clock: Arc<IoClock>,
        backoff: &mut ExponentialBackoff,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> GridResult<()> {
        let mut transport = MessageTransport::with_codec(
            StampedStream::new(stream, Arc::clone(&clock)),
            self.config.wire_codec,
        );

        // Cluster authentication (when a secret is configured) + atomic registration handshake
        let mut deadline = self.resume_deadline;
        let handshake = {
            let hs_fut = self.perform_handshake(&mut transport);
            tokio::pin!(hs_fut);
            loop {
                let resume_wait = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = shutdown_requested(shutdown_rx) => break None,
                    _ = resume_wait => {
                        deadline = None;
                        self.expire_resume_window("session-resume grace window expired").await;
                    }
                    res = &mut hs_fut => break Some(res),
                }
            }
        };
        self.resume_deadline = deadline;
        let Some(handshake) = handshake else {
            info!(worker_id = %self.worker_id, "Shutdown requested during handshake");
            return Ok(());
        };
        let heartbeat_interval = handshake?;
        let _sim_session = self
            .config
            .simulated_link
            .as_ref()
            .and_then(|cfg| cfg.control.as_ref())
            .map(crate::sim::SimulatedNetworkControl::enter_session);
        backoff.reset();
        info!(worker_id = %self.worker_id, "Registration accepted; backoff counter reset");

        // Split transport into independent reader and writer; one stream serves both queues.
        let (writer, reader) = transport.split();
        let mut session = Session::new(clock);
        session.attach_lane(writer, reader, "master", true, true);
        self.run_session(session, heartbeat_interval, shutdown_rx)
            .await
    }

    /// Returns the intersection of `ProtocolCapabilities` supported by this worker and
    /// acknowledged by the Master in the most recent `RegisterAck`.
    pub fn negotiated_features(&self) -> ProtocolCapabilities {
        ProtocolCapabilities::from_bits_truncate(self.negotiated_features.load(Ordering::Acquire))
    }

    /// The event loop of one registered connection: reacts to the Master's frames, watches for a
    /// silently dead peer, reports live mobile telemetry, and obeys the local shutdown signal.
    /// Returns `Ok(())` only for the local shutdown; every other way out is an error the
    /// supervisor answers by reconnecting.
    pub(super) async fn run_session(
        &mut self,
        mut session: Session,
        heartbeat_interval: Duration,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> GridResult<()> {
        let worker_id = self.worker_id;
        if self.current_session_hold_secs.load(Ordering::Acquire) == 0 {
            self.expire_resume_window("reconnected to a Master with session-resume disabled")
                .await;
        }
        // Blob requests ride the data queue: the QUIC data stream, never the control stream.
        // Snapshot the outbox and publish `blob_client` + `session_data_tx` while holding the
        // outbox lock before clearing `resume_window_active` so a task finishing right as the
        // session starts never sees both `session_data_tx == None` and `resume_window_active == false`.
        let blob_client = self
            .master_has_blobs
            .load(Ordering::Acquire)
            .then(|| BlobClient::new(session.data_tx.clone()));
        let (buffered_checkpoints, buffered_results) = {
            let mut outbox = self
                .pending_resume_outbox
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            outbox.cancelled.clear();
            let cps: Vec<(TaskId, WorkerMessage)> = outbox.checkpoints.drain().collect();
            let res: Vec<BufferedTaskResult> = outbox.results.clone();
            *self.blob_client.write().unwrap_or_else(|e| e.into_inner()) = blob_client.clone();
            *self
                .session_data_tx
                .write()
                .unwrap_or_else(|e| e.into_inner()) = Some(session.data_tx.clone());
            self.resume_window_active.store(false, Ordering::Release);
            (cps, res)
        };
        self.resume_deadline = None;

        let adaptive_keepalive = self
            .negotiated_features()
            .contains(ProtocolCapabilities::ADAPTIVE_KEEPALIVE);
        let heartbeat_handle = spawn_adaptive_heartbeat_task(
            worker_id,
            heartbeat_interval,
            session.control_tx.clone(),
            Arc::clone(&self.heartbeat_tracker),
            adaptive_keepalive.then(|| self.mobile.clone()),
        );

        let mut dead_after =
            self.dead_peer_timeout(self.current_keepalive_interval(heartbeat_interval));
        let mut watchdog = tokio::time::interval(
            (dead_after / 8).clamp(Duration::from_millis(25), Duration::from_secs(1)),
        );
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut telemetry = if self.initial_telemetry_delay > Duration::ZERO {
            tokio::time::interval_at(
                tokio::time::Instant::now() + self.initial_telemetry_delay,
                MOBILE_TELEMETRY_CHECK_INTERVAL,
            )
        } else {
            tokio::time::interval(MOBILE_TELEMETRY_CHECK_INTERVAL)
        };
        telemetry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `Register` carries only `MobileCapabilities` (no `network_type`, `battery_temperature`,
        // or tiers), so send one `MobileTelemetry` right after `RegisterAck` when the Master
        // announced `mobile-telemetry` (unless a test configured `initial_telemetry_delay`).
        let mut last_reported: Option<MobileLiveStatus> = None;
        // Only to a Master that said it can read the report (an older one would drop us for it).
        let report_telemetry = self.master_takes_mobile_telemetry.load(Ordering::Acquire);
        if report_telemetry && self.initial_telemetry_delay.is_zero() {
            if let Some(status) = self.mobile.live_status() {
                let report = WorkerMessage::MobileTelemetry {
                    worker_id,
                    battery_pct: status.battery_pct,
                    is_charging: status.is_charging,
                    thermal_throttled: status.thermal_throttled,
                    battery_temperature: status.battery_temperature,
                    network_type: status.network_type.clone(),
                    battery_tier: status.battery_tier,
                    thermal_tier: status.thermal_tier,
                };
                if session.control_tx.try_send(report).is_ok() {
                    last_reported = Some(status);
                }
            }
        }
        self.control.clear_session_pause_rejection();
        self.sync_and_evaluate_limits(true).await;
        let mut control_rx = self.control.subscribe();
        let mut pause_expiry_tick = tokio::time::interval(Duration::from_secs(1));
        pause_expiry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut limit_eval_tick = tokio::time::interval(
            self.config
                .limit_eval_interval
                .max(Duration::from_millis(50)),
        );
        limit_eval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut farewell: Option<WorkerMessage> = None;
        let _outbox_flush_guard = (!buffered_checkpoints.is_empty()
            || !buffered_results.is_empty())
        .then(|| {
            let data_tx = session.data_tx.clone();
            let flush_blob_client = blob_client.clone();
            let flush_runner = Arc::clone(&self.runner);
            let flush_outbox = Arc::clone(&self.pending_resume_outbox);
            AbortOnDrop(tokio::spawn(async move {
                for (tid, cp_msg) in buffered_checkpoints {
                    let cancelled = flush_outbox
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .cancelled
                        .contains(&tid);
                    if !cancelled && data_tx.send(cp_msg).await.is_err() {
                        return;
                    }
                }
                for item in buffered_results {
                    let still_pending = {
                        let outbox = flush_outbox.lock().unwrap_or_else(|e| e.into_inner());
                        !outbox.cancelled.contains(&item.task.id)
                            && outbox.results.iter().any(|r| {
                                r.result.task_id == item.task.id && r.ack_token == item.ack_token
                            })
                    };
                    if !still_pending {
                        continue;
                    }

                    let mut result = item.result.clone();
                    if !result.outputs.is_empty() {
                        match flush_runner.ensure_blob_cache().await {
                            Ok(cache) => {
                                upload_task_outputs(
                                    &item.task,
                                    &mut result,
                                    flush_blob_client.as_deref(),
                                    &cache,
                                )
                                .await;
                            }
                            Err(e) => {
                                result.outputs.clear();
                                result.error = Some(e);
                            }
                        }
                    }

                    if !item.result.outputs.is_empty()
                        && flush_blob_client
                            .as_ref()
                            .is_some_and(|client| client.is_closed())
                    {
                        return;
                    }

                    let current_token = {
                        let mut outbox = flush_outbox.lock().unwrap_or_else(|e| e.into_inner());
                        outbox.update_result(item.task.id, item.ack_token, result.clone())
                    };
                    let still_pending = {
                        let outbox = flush_outbox.lock().unwrap_or_else(|e| e.into_inner());
                        !outbox.cancelled.contains(&item.task.id)
                            && outbox.results.iter().any(|r| {
                                r.result.task_id == item.task.id && r.ack_token == current_token
                            })
                    };
                    if !still_pending {
                        continue;
                    }

                    if data_tx.send(WorkerMessage::from(result)).await.is_err() {
                        return;
                    }
                }
            }))
        });

        let outcome: GridResult<()> = loop {
            tokio::select! {
                event = session.inbound_rx.recv() => {
                    match event {
                        Some(Inbound::Message(msg)) => match *msg {
                            MasterMessage::HeartbeatAck { timestamp } => {
                                self.heartbeat_tracker.record_ack(timestamp, current_epoch_secs());
                            }
                            MasterMessage::Shutdown { reason, .. } => {
                                // The Master is going away (not the worker): keep `farewell = None`
                                // so we close cleanly without sending `WorkerMessage::Disconnecting`,
                                // preserving any `SESSION_RESUME` tasks/outbox without triggering
                                // evacuation on the master.
                                warn!(reason = %reason, "Master announced its shutdown; disconnecting and waiting for it to return");
                                self.master_shutdown_announced = true;
                                break Err(GridError::ConnectionClosed);
                            }
                            MasterMessage::AssignTask { task } => {
                                debug!(task_id = %task.id, "AssignTask received; dispatching runner");
                                let checkpoint = task.latest_checkpoint.clone();
                                self.handle_assign_task_with_checkpoint(task, checkpoint, session.data_tx.clone()).await;
                            }
                            MasterMessage::AssignTaskWithCheckpoint { task, latest_checkpoint } => {
                                debug!(task_id = %task.id, "AssignTaskWithCheckpoint received; dispatching runner");
                                let checkpoint = latest_checkpoint.or_else(|| task.latest_checkpoint.clone());
                                self.handle_assign_task_with_checkpoint(task, checkpoint, session.data_tx.clone()).await;
                            }
                            MasterMessage::CancelTask { task_id, reason } => {
                                debug!(task_id = %task_id, reason = ?reason, "CancelTask received; aborting task");
                                self.handle_cancel_task(task_id, reason).await;
                            }
                            MasterMessage::RegisterAck { .. } => {
                                warn!("Unexpected RegisterAck during active connection");
                            }
                            MasterMessage::RoleHandoverAck { accepted, reason, .. } => {
                                info!(accepted, reason = ?reason, "Received RoleHandoverAck from master");
                            }
                            MasterMessage::UnknownVariant { tag, ref raw_bytes } => {
                                warn!(tag, bytes_len = raw_bytes.len(), "Ignoring unknown MasterMessage variant");
                            }
                            MasterMessage::BlobReply { request_id, reply } => {
                                let delivered = blob_client
                                    .as_ref()
                                    .is_some_and(|client| client.deliver(request_id, reply));
                                if !delivered {
                                    debug!(request_id, "Blob reply for a request nobody waits for");
                                }
                            }
                        },
                        Some(Inbound::Closed(lane)) => {
                            info!(lane, "Master closed the connection (EOF)");
                            break Err(GridError::ConnectionClosed);
                        }
                        Some(Inbound::Failed(lane, e)) => {
                            warn!(lane, error = %e, "Connection error");
                            break Err(e.into());
                        }
                        None => break Err(GridError::ConnectionClosed),
                    }
                }

                // Dead-peer watchdog: nothing received for `dead_after` (and no frame upload making
                // progress) means the Master is gone even though the socket still accepts writes.
                _ = watchdog.tick() => {
                    dead_after = dead_after
                        .max(self.dead_peer_timeout(self.current_keepalive_interval(heartbeat_interval)));
                    let silence = session.clock.silence();
                    if silence > dead_after {
                        warn!(
                            silent_secs = silence.as_secs_f32(),
                            limit_secs = dead_after.as_secs_f32(),
                            "Master has been silent for too long; dropping the connection to reconnect"
                        );
                        break Err(GridError::HeartbeatTimeout {
                            worker_id,
                            last_seen_secs: silence.as_secs(),
                        });
                    }
                }

                // Live battery / thermal state: tell the Master whenever it changed.
                _ = telemetry.tick(), if report_telemetry => {
                    let current = self.mobile.live_status();
                    if current != last_reported {
                        if let Some(ref status) = current {
                            let report = WorkerMessage::MobileTelemetry {
                                worker_id,
                                battery_pct: status.battery_pct,
                                is_charging: status.is_charging,
                                thermal_throttled: status.thermal_throttled,
                                battery_temperature: status.battery_temperature,
                                network_type: status.network_type.clone(),
                                battery_tier: status.battery_tier,
                                thermal_tier: status.thermal_tier,
                            };
                            if session.control_tx.try_send(report).is_ok() {
                                last_reported = current;
                            }
                        }
                    }
                }

                // Local pause / resume control transitions (`design 0113` D2/D3).
                ev = control_rx.recv() => {
                    match ev {
                        Ok(crate::control::ControlEvent::PauseHandBack { reason }) => {
                            self.hand_back_running_tasks_on_pause(&reason, &session.data_tx)
                                .await;
                        }
                        Ok(crate::control::ControlEvent::LimitsUpdated) => {
                            self.sync_and_evaluate_limits(true).await;
                            if self.should_reconnect_after_hold_clears(&session).await {
                                info!(
                                    worker_id = %worker_id,
                                    "Worker limits updated and hold cleared; closing session to re-register immediately"
                                );
                                self.resume_reconnect_pending.store(true, Ordering::Release);
                                break Err(GridError::ConnectionClosed);
                            }
                        }
                        Ok(crate::control::ControlEvent::ResumeReconnect) => {
                            if self.should_reconnect_after_hold_clears(&session).await {
                                info!(
                                    worker_id = %worker_id,
                                    "Worker resumed while held back; closing session to re-register immediately"
                                );
                                self.resume_reconnect_pending.store(true, Ordering::Release);
                                break Err(GridError::ConnectionClosed);
                            }
                        }
                        Err(
                            tokio::sync::broadcast::error::RecvError::Lagged(_)
                            | tokio::sync::broadcast::error::RecvError::Closed,
                        ) => {}
                    }
                }

                _ = limit_eval_tick.tick() => {
                    self.sync_and_evaluate_limits(false).await;
                    if self.should_reconnect_after_hold_clears(&session).await {
                        info!(
                            worker_id = %worker_id,
                            "Contributor limit hold cleared; closing session to re-register immediately"
                        );
                        self.resume_reconnect_pending.store(true, Ordering::Release);
                        break Err(GridError::ConnectionClosed);
                    }
                }

                _ = self.limits_reeval_notify.notified() => {
                    self.sync_and_evaluate_limits(false).await;
                    if self.should_reconnect_after_hold_clears(&session).await {
                        info!(
                            worker_id = %worker_id,
                            "Completed task idle check cleared hold; closing session to re-register immediately"
                        );
                        self.resume_reconnect_pending.store(true, Ordering::Release);
                        break Err(GridError::ConnectionClosed);
                    }
                }

                _ = pause_expiry_tick.tick() => {
                    let _ = self.control.check_pause_expiry();
                    if self.should_reconnect_after_hold_clears(&session).await {
                        info!(
                            worker_id = %worker_id,
                            "Worker pause expired while held back; closing session to re-register immediately"
                        );
                        self.resume_reconnect_pending.store(true, Ordering::Release);
                        break Err(GridError::ConnectionClosed);
                    }
                }

                // External shutdown signal
                res = shutdown_rx.changed() => {
                    if res.is_ok() && *shutdown_rx.borrow() {
                        let evac_reason = current_evacuation_reason();
                        if evac_reason.is_none() {
                            let drained = self
                                .drain_active_tasks_for_shutdown(Duration::from_secs(2))
                                .await;
                            if drained > 0 {
                                let flush_deadline =
                                    tokio::time::Instant::now() + Duration::from_millis(500);
                                while session.data_tx.capacity() < session.data_tx.max_capacity()
                                    && !session.data_tx.is_closed()
                                    && tokio::time::Instant::now() < flush_deadline
                                {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            }
                        }
                        let reason = evac_reason
                            .unwrap_or_else(|| "Graceful worker shutdown".to_string());
                        info!(worker_id = %worker_id, reason = %reason, "Worker shutdown initiated; notifying Master");
                        farewell = Some(WorkerMessage::Disconnecting {
                            worker_id,
                            reason,
                        });
                        break Ok(());
                    } else if res.is_err() {
                        break Ok(());
                    }
                }
            }
        };

        let grace = self.effective_resume_grace();
        if outcome.is_err()
            && !*shutdown_rx.borrow()
            && current_evacuation_reason().is_none()
            && !self.resume_reconnect_pending.load(Ordering::Acquire)
            && grace > Duration::ZERO
        {
            self.resume_deadline = Some(tokio::time::Instant::now() + grace);
            self.resume_window_active.store(true, Ordering::Release);
        } else {
            self.resume_deadline = None;
            self.resume_window_active.store(false, Ordering::Release);
        }
        *self
            .session_data_tx
            .write()
            .unwrap_or_else(|e| e.into_inner()) = None;

        // Cleanly terminate heartbeat task and the lanes
        heartbeat_handle.stop().await;
        if let Some(client) = self
            .blob_client
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            client.close();
        }
        session.close(farewell).await;
        #[cfg(feature = "p2p")]
        {
            let mut conn_guard = self.p2p_conn.write().await;
            *conn_guard = None;
        }

        outcome
    }

    /// Executes the cluster authentication (when a secret is configured) and the atomic
    /// registration handshake over the transport.
    ///
    /// With a secret, `client_handshake` runs first, so nothing about this worker is sent to a
    /// Master that cannot prove it holds the same secret. Without one, a Master that demands
    /// authentication answers the `Register` with a rejection that surfaces as
    /// `GridError::AuthRequired`. `AuthFailed`/`AuthRequired` are permanent for a given secret:
    /// the supervisor in [`WorkerClient::run`] backs off for at least [`super::AUTH_FAILURE_MIN_BACKOFF`].
    ///
    /// The capabilities in `Register` carry the *current* battery / thermal state, not the
    /// snapshot taken at startup.
    pub async fn perform_handshake<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        transport: &mut MessageTransport<S>,
    ) -> GridResult<Duration> {
        if let Some(master_pub) = self.config.master_public_key.as_ref() {
            let node_key = self.config.node_key.as_ref().ok_or_else(|| {
                GridError::Config(
                    "master_public_key is set on WorkerConfig without node_key; pass both via WorkerConfig::with_v3_auth".into(),
                )
            })?;
            let session_key = tokio::time::timeout(
                self.config.handshake_timeout,
                v3_client_handshake(transport, node_key, master_pub, HandshakePurpose::Member),
            )
            .await
            .map_err(|_| GridError::Timeout {
                operation: "cluster_authentication".into(),
                duration_secs: self.config.handshake_timeout.as_secs(),
            })??;
            debug!(
                worker_id = %self.worker_id,
                member_id = %node_key.member_id(),
                "Master authenticated via OXS-AUTH v3 (Noise IK)"
            );
            transport.enable_encryption(&session_key, EndpointRole::Client);
        } else if let Some(secret) = self.config.cluster_secret.as_ref() {
            // Upgrade transport to AEAD record layer upon successful authentication.
            let session_key = tokio::time::timeout(
                self.config.handshake_timeout,
                client_handshake(transport, secret),
            )
            .await
            .map_err(|_| GridError::Timeout {
                operation: "cluster_authentication".into(),
                duration_secs: self.config.handshake_timeout.as_secs(),
            })??;
            debug!(worker_id = %self.worker_id, "Master proved knowledge of the cluster secret");
            transport.enable_encryption(&session_key, EndpointRole::Client);
        }

        // Whatever the previous connection's Master could do says nothing about this one.
        self.master_takes_mobile_telemetry
            .store(false, Ordering::Release);
        self.master_has_blobs.store(false, Ordering::Release);
        self.current_session_hold_secs.store(0, Ordering::Release);
        self.negotiated_features.store(0, Ordering::Release);
        let advertised = self.advertised_features();
        let mut capabilities = self.capabilities.clone();
        capabilities.mobile = self.mobile.snapshot();
        let register_msg = WorkerMessage::Register {
            worker_id: self.worker_id,
            capabilities,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: advertised,
        };

        transport.send_msg(&register_msg).await.map_err(|e| {
            GridError::HandshakeFailed(format!("Failed to send Register message: {e}"))
        })?;

        debug!(worker_id = %self.worker_id, "WorkerMessage::Register dispatched; awaiting RegisterAck");

        let maybe_ack = tokio::time::timeout(
            self.config.handshake_timeout,
            transport.recv_msg::<MasterMessage>(),
        )
        .await
        .map_err(|_| GridError::Timeout {
            operation: "registration_handshake".into(),
            duration_secs: self.config.handshake_timeout.as_secs(),
        })?
        .map_err(|e| match e {
            // A Master that requires the cluster secret answers our un-authenticated Register
            // with an authentication REJECT frame; keep that actionable error intact.
            ProtocolError::InvalidFormatTag(WIRE_FORMAT_AUTH) => GridError::from(e),
            e => GridError::HandshakeFailed(format!("Transport error awaiting RegisterAck: {e}")),
        })?
        .ok_or(GridError::ConnectionClosed)?;

        match maybe_ack {
            MasterMessage::RegisterAck {
                accepted,
                worker_id: ack_wid,
                heartbeat_interval_secs,
                message,
                protocol_version,
                features,
            } => {
                if !accepted {
                    let reason =
                        message.unwrap_or_else(|| "Registration rejected by master".into());
                    warn!(worker_id = %self.worker_id, reason = %reason, "Master rejected registration");
                    return Err(GridError::RegistrationRejected(reason));
                }

                // Only an echo of the own version (design 0083 D5): the window protects a newer
                // master from older peers, never the reverse.
                if !is_master_echo_acceptable(protocol_version) {
                    return Err(GridError::HandshakeFailed(
                        format_incompatible_version_error("Master", protocol_version),
                    ));
                }

                if ack_wid != self.worker_id {
                    return Err(GridError::HandshakeFailed(format!(
                        "Worker ID mismatch in RegisterAck: expected {}, got {}",
                        self.worker_id, ack_wid
                    )));
                }

                let interval = if heartbeat_interval_secs > 0 {
                    Duration::from_secs(heartbeat_interval_secs)
                } else {
                    self.config.default_heartbeat_interval
                };

                let negotiated = features & advertised;
                self.negotiated_features
                    .store(negotiated.bits(), Ordering::Release);

                let takes_telemetry = negotiated.contains(ProtocolCapabilities::MOBILE_TELEMETRY)
                    && master_features::announced(
                        message.as_deref(),
                        master_features::MOBILE_TELEMETRY,
                    );
                self.master_takes_mobile_telemetry
                    .store(takes_telemetry, Ordering::Release);
                let has_blobs = negotiated.contains(ProtocolCapabilities::BLOB_TRANSFER)
                    && master_features::announced(message.as_deref(), master_features::BLOBS);
                self.master_has_blobs.store(has_blobs, Ordering::Release);

                let hold_secs = if negotiated.contains(ProtocolCapabilities::SESSION_RESUME) {
                    master_features::parse_session_resume_hold_secs(message.as_deref()).unwrap_or(0)
                } else {
                    0
                };
                self.current_session_hold_secs
                    .store(hold_secs, Ordering::Release);
                self.last_negotiated_hold_secs
                    .store(hold_secs, Ordering::Release);
                self.last_heartbeat_interval_ms
                    .store(interval.as_millis() as u64, Ordering::Release);

                if !takes_telemetry && self.mobile.live_status().is_some() {
                    info!(
                        worker_id = %self.worker_id,
                        "This Master does not announce live battery/thermal reports (older build?); sending them only at registration"
                    );
                }

                if hold_secs > 0 {
                    if self
                        .resume_deadline
                        .is_some_and(|d| tokio::time::Instant::now() >= d)
                    {
                        self.expire_resume_window("session-resume grace window expired")
                            .await;
                    }
                    let running = self.resumable_task_ids().await;
                    let resume_msg = WorkerMessage::ResumeSession {
                        worker_id: self.worker_id,
                        running,
                    };
                    transport.send_msg(&resume_msg).await.map_err(|e| {
                        GridError::HandshakeFailed(format!(
                            "Failed to send ResumeSession message: {e}"
                        ))
                    })?;
                }

                info!(
                    worker_id = %self.worker_id,
                    heartbeat_interval_secs = interval.as_secs(),
                    "Worker successfully registered with Master"
                );

                Ok(interval)
            }
            unexpected => Err(GridError::UnexpectedMessage {
                expected: "RegisterAck".into(),
                actual: unexpected.variant_name().into(),
            }),
        }
    }
}
