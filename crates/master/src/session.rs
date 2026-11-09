//! Unified worker session state machine shared by single-stream (TCP) and two-lane multiplexed
//! (QUIC / iroh) connections (`docs/design/0044-dead-code-and-connection-loop-unification.md`, D4).
//!
//! Both transports run the same sequence:
//! 1. [`PreAuthSession`]: cluster-secret authentication and first-frame watchdog on the primary
//!    stream, releasing `handshake_permit` immediately after authentication and draining refusal
//!    frames on multiplexed connections (`BUG-17`).
//! 2. [`WorkerSession::register`]: `WorkerMessage::Register` validation, `ClaimMasterRole`
//!    refusal, `WorkerRegistry::register_session`, `RxClock` attachment, superseded-session
//!    failover (`BUG-18`), `P2pConnGuard` registration, and `RegisterAck` delivery.
//! 3. [`ActiveLanes`]: transport splitting and background outbound writer / router task setup
//!    (single writer for TCP; head-of-line-free control + data writers for QUIC).
//! 4. [`WorkerSession::run`]: one `tokio::select!` event loop over `lanes.recv_next()` and
//!    `shutdown_rx.changed()`, enforcing lane-specific message acceptance (`LaneKind`) and
//!    unified disconnect / evacuation cleanup.

#[cfg(feature = "p2p")]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit};
use tokio::task::AbortHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use rusty_grid_core::capabilities::MobileLiveStatus;
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::protocol::{
    format_incompatible_version_error, is_protocol_version_compatible, InboundMessage,
    MessageReader, ProtocolCapabilities, ProtocolError, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::{
    GridError, GridResult, MasterMessage, MessageTransport, WireCodec, WorkerMessage,
};

use crate::blob_store::BlobStore;
use crate::liveness::{RxClock, StampedStream};
use crate::queue::TaskQueue;
use crate::registry::{HeartbeatOutcome, WorkerRegistry};
#[cfg(feature = "dashboard")]
use crate::server::DashboardTx;
#[cfg(feature = "p2p")]
use crate::server::P2pConnGuard;
use crate::server::{
    apply_mobile_telemetry, authenticate_and_read_first_message, drain_refusal_stream,
    failover_superseded_session, feature_announcement, handle_client_connection,
    handle_oversized_master_message, handle_session_task_message,
    handle_worker_disconnect_with_broadcast, handle_worker_evacuate_with_broadcast,
    reject_master_role_claim, resolve_terminal_waiters, send_refusal_and_drain,
    serve_worker_blob_request, AbortOnDrop, BlobService, MasterAuthContext, WaiterMap,
};

/// Which inbound lane delivered a [`WorkerMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneKind {
    /// Single-stream TCP connection (accepts all `WorkerMessage` variants).
    Unified,
    /// Multiplexed QUIC control stream (`Heartbeat`, `Disconnecting`, `MobileTelemetry`,
    /// `ClaimMasterRole`, `BlobRequest`).
    Control,
    /// Multiplexed QUIC data stream (`TaskProgress`, `TaskResult`, `Checkpoint`, `BlobRequest`).
    Data,
}

impl LaneKind {
    fn eof_reason(self) -> &'static str {
        match self {
            Self::Unified => "TCP connection EOF",
            Self::Control => "Control stream EOF",
            Self::Data => "Data stream EOF",
        }
    }

    fn error_reason(self, err: &ProtocolError) -> String {
        match self {
            Self::Unified => format!("Inbound socket error: {err}"),
            Self::Control => format!("Control stream socket error: {err}"),
            Self::Data => format!("Data stream socket error: {err}"),
        }
    }

    fn cleanup_reason(self) -> &'static str {
        match self {
            Self::Unified => "Connection terminated",
            Self::Control | Self::Data => "Multiplexed connection terminated",
        }
    }
}

/// Pre-registration transport state before authentication and `Register` completion.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PreAuthSession<
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    D: AsyncRead + AsyncWrite + Unpin + Send + 'static = tokio::io::Empty,
> {
    Single {
        transport: MessageTransport<StampedStream<C>>,
        remote_addr: SocketAddr,
        rx_clock: Arc<RxClock>,
        mapreduce_parallelism: usize,
        #[cfg(feature = "p2p")]
        p2p_conn: Option<iroh::endpoint::Connection>,
        #[cfg(feature = "p2p")]
        p2p_conns: Option<Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>>,
    },
    Multiplexed {
        ctrl_transport: MessageTransport<StampedStream<C>>,
        data_transport: MessageTransport<StampedStream<D>>,
        remote_addr: SocketAddr,
        rx_clock: Arc<RxClock>,
        #[cfg(feature = "p2p")]
        p2p_conn: Option<iroh::endpoint::Connection>,
        #[cfg(feature = "p2p")]
        p2p_conns: Option<Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>>,
    },
}

impl<C: AsyncRead + AsyncWrite + Unpin + Send + 'static> PreAuthSession<C, tokio::io::Empty> {
    pub(crate) fn new_single(
        stream: C,
        remote_addr: SocketAddr,
        wire_codec: WireCodec,
        mapreduce_parallelism: usize,
        #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
        #[cfg(feature = "p2p")] p2p_conns: Option<
            Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
        >,
    ) -> Self {
        let rx_clock = RxClock::new();
        let transport =
            MessageTransport::with_codec(StampedStream::new(stream, rx_clock.clone()), wire_codec);
        Self::Single {
            transport,
            remote_addr,
            rx_clock,
            mapreduce_parallelism,
            #[cfg(feature = "p2p")]
            p2p_conn,
            #[cfg(feature = "p2p")]
            p2p_conns,
        }
    }
}

impl<
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    > PreAuthSession<C, D>
{
    pub(crate) fn new_multiplexed(
        ctrl_stream: C,
        data_stream: D,
        remote_addr: SocketAddr,
        wire_codec: WireCodec,
        #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
        #[cfg(feature = "p2p")] p2p_conns: Option<
            Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
        >,
    ) -> Self {
        let rx_clock = RxClock::new();
        let ctrl_transport = MessageTransport::with_codec(
            StampedStream::new(ctrl_stream, rx_clock.clone()),
            wire_codec,
        );
        let data_transport = MessageTransport::with_codec(
            StampedStream::new(data_stream, rx_clock.clone()),
            wire_codec,
        );
        Self::Multiplexed {
            ctrl_transport,
            data_transport,
            remote_addr,
            rx_clock,
            #[cfg(feature = "p2p")]
            p2p_conn,
            #[cfg(feature = "p2p")]
            p2p_conns,
        }
    }

    fn remote_addr(&self) -> SocketAddr {
        match self {
            Self::Single { remote_addr, .. } | Self::Multiplexed { remote_addr, .. } => {
                *remote_addr
            }
        }
    }

    fn rx_clock(&self) -> &Arc<RxClock> {
        match self {
            Self::Single { rx_clock, .. } | Self::Multiplexed { rx_clock, .. } => rx_clock,
        }
    }

    fn is_multiplexed(&self) -> bool {
        matches!(self, Self::Multiplexed { .. })
    }

    #[cfg(feature = "p2p")]
    fn p2p_conn(&self) -> Option<&iroh::endpoint::Connection> {
        match self {
            Self::Single { p2p_conn, .. } | Self::Multiplexed { p2p_conn, .. } => p2p_conn.as_ref(),
        }
    }

    #[cfg(feature = "p2p")]
    fn p2p_conns(
        &self,
    ) -> Option<&Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>> {
        match self {
            Self::Single { p2p_conns, .. } | Self::Multiplexed { p2p_conns, .. } => {
                p2p_conns.as_ref()
            }
        }
    }

    async fn send_handshake_refusal(&mut self, msg: &MasterMessage) {
        match self {
            Self::Single { transport, .. } => {
                let _ = transport.send_msg(msg).await;
            }
            Self::Multiplexed {
                ctrl_transport,
                #[cfg(feature = "p2p")]
                p2p_conn,
                ..
            } => {
                send_refusal_and_drain(
                    ctrl_transport,
                    msg,
                    #[cfg(feature = "p2p")]
                    p2p_conn.as_ref(),
                )
                .await;
            }
        }
    }

    async fn send_register_ack(&mut self, ack: &MasterMessage) -> Result<(), ProtocolError> {
        match self {
            Self::Single { transport, .. } => transport.send_msg(ack).await,
            Self::Multiplexed { ctrl_transport, .. } => ctrl_transport.send_msg(ack).await,
        }
    }
}

/// Active inbound readers and background outbound writers for a registered worker session.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ActiveLanes<
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
> {
    Single {
        reader: MessageReader<ReadHalf<StampedStream<C>>>,
        ctrl_reply_tx: mpsc::Sender<MasterMessage>,
        _writer_guard: AbortOnDrop,
    },
    Multiplexed {
        ctrl_reader: MessageReader<ReadHalf<StampedStream<C>>>,
        data_reader: MessageReader<ReadHalf<StampedStream<D>>>,
        ctrl_reply_tx: mpsc::Sender<MasterMessage>,
        _router_guard: AbortOnDrop,
        _ctrl_writer_guard: AbortOnDrop,
        _data_writer_guard: AbortOnDrop,
    },
}

impl<
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    > ActiveLanes<C, D>
{
    fn default_lane_kind(&self) -> LaneKind {
        match self {
            Self::Single { .. } => LaneKind::Unified,
            Self::Multiplexed { .. } => LaneKind::Control,
        }
    }

    fn ctrl_reply_tx(&self) -> &mpsc::Sender<MasterMessage> {
        match self {
            Self::Single { ctrl_reply_tx, .. } | Self::Multiplexed { ctrl_reply_tx, .. } => {
                ctrl_reply_tx
            }
        }
    }

    async fn recv_next(
        &mut self,
    ) -> Result<(LaneKind, Option<WorkerMessage>), (LaneKind, ProtocolError)> {
        match self {
            Self::Single { reader, .. } => reader
                .recv_msg::<WorkerMessage>()
                .await
                .map(|msg| (LaneKind::Unified, msg))
                .map_err(|e| (LaneKind::Unified, e)),
            Self::Multiplexed {
                ctrl_reader,
                data_reader,
                ..
            } => {
                tokio::select! {
                    ctrl_res = ctrl_reader.recv_msg::<WorkerMessage>() => {
                        ctrl_res
                            .map(|msg| (LaneKind::Control, msg))
                            .map_err(|e| (LaneKind::Control, e))
                    }
                    data_res = data_reader.recv_msg::<WorkerMessage>() => {
                        data_res
                            .map(|msg| (LaneKind::Data, msg))
                            .map_err(|e| (LaneKind::Data, e))
                    }
                }
            }
        }
    }
}

/// Active state machine for a registered worker connection.
pub(crate) struct WorkerSession<
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    D: AsyncRead + AsyncWrite + Unpin + Send + 'static = tokio::io::Empty,
> {
    worker_id: Uuid,
    session_id: u64,
    negotiated_features: ProtocolCapabilities,
    lanes: ActiveLanes<C, D>,
    outbound_tx: mpsc::Sender<MasterMessage>,
    blob_service: Option<BlobService>,
    registry: WorkerRegistry,
    queue: TaskQueue,
    scheduler_notify: Arc<tokio::sync::Notify>,
    waiters: WaiterMap,
    blob_store: Option<Arc<BlobStore>>,
    shutdown_rx: watch::Receiver<bool>,
    #[cfg(feature = "dashboard")]
    broadcast_tx: Option<DashboardTx>,
    #[cfg(feature = "p2p")]
    p2p_guard: Option<P2pConnGuard>,
}

impl<
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    > WorkerSession<C, D>
{
    /// Authenticates `pre_auth`, dispatches client connections on single-stream transports, or
    /// registers the worker and drives its session loop until disconnect or shutdown.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_connection(
        mut pre_auth: PreAuthSession<C, D>,
        registry: WorkerRegistry,
        queue: TaskQueue,
        scheduler_notify: Arc<tokio::sync::Notify>,
        waiters: WaiterMap,
        heartbeat_interval_secs: u64,
        handshake_timeout: Duration,
        shutdown_rx: watch::Receiver<bool>,
        abort_handle: Option<AbortHandle>,
        wire_codec: WireCodec,
        cluster_secret: Option<ClusterSecret>,
        auth_ctx: MasterAuthContext,
        handshake_permit: Option<OwnedSemaphorePermit>,
        blob_store: Option<Arc<BlobStore>>,
        #[cfg(feature = "dashboard")] broadcast_tx: Option<DashboardTx>,
    ) -> GridResult<()> {
        // 1. Cluster authentication + handshake watchdog (Slowloris defense)
        let first_res = match &mut pre_auth {
            PreAuthSession::Single {
                transport,
                remote_addr,
                #[cfg(feature = "p2p")]
                p2p_conn,
                ..
            } => {
                #[cfg(feature = "p2p")]
                let peer_label = match p2p_conn {
                    Some(ref conn) => format!("{remote_addr} (iroh node {})", conn.remote_id()),
                    None => remote_addr.to_string(),
                };
                #[cfg(not(feature = "p2p"))]
                let peer_label = remote_addr.to_string();
                authenticate_and_read_first_message(
                    transport,
                    &peer_label,
                    cluster_secret.as_ref(),
                    &auth_ctx,
                    handshake_timeout,
                    "connection",
                )
                .await
            }
            PreAuthSession::Multiplexed {
                ctrl_transport,
                remote_addr,
                #[cfg(feature = "p2p")]
                p2p_conn,
                ..
            } => {
                #[cfg(feature = "p2p")]
                let peer_label = match p2p_conn {
                    Some(ref conn) => format!("{remote_addr} (iroh node {})", conn.remote_id()),
                    None => remote_addr.to_string(),
                };
                #[cfg(not(feature = "p2p"))]
                let peer_label = remote_addr.to_string();
                authenticate_and_read_first_message(
                    ctrl_transport,
                    &peer_label,
                    cluster_secret.as_ref(),
                    &auth_ctx,
                    handshake_timeout,
                    "control stream",
                )
                .await
            }
        };
        // Authenticated (or rejected): release the pre-auth handshake slot immediately.
        drop(handshake_permit);

        let (first_msg, detected_codec, v3_peer) = match first_res {
            Ok(triple) => triple,
            Err(e) => {
                if matches!(e, GridError::AuthFailed(_) | GridError::AuthRequired(_)) {
                    match pre_auth {
                        PreAuthSession::Single {
                            ref mut transport,
                            #[cfg(feature = "p2p")]
                            ref p2p_conn,
                            ..
                        } if {
                            #[cfg(feature = "p2p")]
                            {
                                p2p_conn.is_some()
                            }
                            #[cfg(not(feature = "p2p"))]
                            {
                                false
                            }
                        } =>
                        {
                            drain_refusal_stream(
                                transport,
                                #[cfg(feature = "p2p")]
                                p2p_conn.as_ref(),
                            )
                            .await;
                        }
                        PreAuthSession::Multiplexed {
                            ref mut ctrl_transport,
                            #[cfg(feature = "p2p")]
                            ref p2p_conn,
                            ..
                        } => {
                            drain_refusal_stream(
                                ctrl_transport,
                                #[cfg(feature = "p2p")]
                                p2p_conn.as_ref(),
                            )
                            .await;
                        }
                        _ => {}
                    }
                }
                return Err(e);
            }
        };

        if wire_codec == WireCodec::Bincode {
            match &mut pre_auth {
                PreAuthSession::Single { transport, .. } => {
                    transport.set_outbound_codec(detected_codec);
                }
                PreAuthSession::Multiplexed {
                    ctrl_transport,
                    data_transport,
                    ..
                } => {
                    ctrl_transport.set_outbound_codec(detected_codec);
                    data_transport.set_outbound_codec(detected_codec);
                }
            }
        }

        let worker_msg = match first_msg {
            InboundMessage::Client(client_msg) => match pre_auth {
                PreAuthSession::Single {
                    transport,
                    mapreduce_parallelism,
                    #[cfg(feature = "p2p")]
                        p2p_conn: _p2p_conn,
                    #[cfg(feature = "p2p")]
                    p2p_conns,
                    ..
                } => {
                    return handle_client_connection(
                        client_msg,
                        transport,
                        registry,
                        queue,
                        scheduler_notify,
                        waiters,
                        mapreduce_parallelism,
                        blob_store,
                        auth_ctx,
                        v3_peer,
                        #[cfg(feature = "dashboard")]
                        broadcast_tx,
                        #[cfg(feature = "p2p")]
                        p2p_conns,
                    )
                    .await;
                }
                PreAuthSession::Multiplexed { .. } => {
                    return Err(GridError::HandshakeFailed(
                        "Client connection not supported over multiplexed P2P worker transport"
                            .into(),
                    ));
                }
            },
            InboundMessage::Worker(wm) => wm,
        };

        let Some(mut session) = Self::register(
            pre_auth,
            worker_msg,
            registry,
            queue,
            scheduler_notify,
            waiters,
            heartbeat_interval_secs,
            shutdown_rx,
            abort_handle,
            blob_store,
            #[cfg(feature = "dashboard")]
            broadcast_tx,
        )
        .await?
        else {
            return Ok(());
        };

        session.run().await
    }

    #[allow(clippy::too_many_arguments)]
    async fn register(
        mut pre_auth: PreAuthSession<C, D>,
        worker_msg: WorkerMessage,
        registry: WorkerRegistry,
        queue: TaskQueue,
        scheduler_notify: Arc<tokio::sync::Notify>,
        waiters: WaiterMap,
        heartbeat_interval_secs: u64,
        shutdown_rx: watch::Receiver<bool>,
        abort_handle: Option<AbortHandle>,
        blob_store: Option<Arc<BlobStore>>,
        #[cfg(feature = "dashboard")] broadcast_tx: Option<DashboardTx>,
    ) -> GridResult<Option<Self>> {
        // 2. Validate Register message
        let (worker_id, capabilities, worker_features) = match worker_msg {
            WorkerMessage::Register {
                worker_id,
                capabilities,
                protocol_version,
                features,
            } => {
                if !is_protocol_version_compatible(protocol_version) {
                    let reason = format_incompatible_version_error("worker", protocol_version);
                    let reject_ack = MasterMessage::RegisterAck {
                        accepted: false,
                        worker_id,
                        heartbeat_interval_secs: 0,
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::empty(),
                        message: Some(reason.clone()),
                    };
                    pre_auth.send_handshake_refusal(&reject_ack).await;
                    return Err(GridError::RegistrationRejected(reason));
                }
                if capabilities.cpu_cores == 0 {
                    let reject_ack = MasterMessage::RegisterAck {
                        accepted: false,
                        worker_id,
                        heartbeat_interval_secs: 0,
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::empty(),
                        message: Some("Invalid capability: cpu_cores must be > 0".into()),
                    };
                    pre_auth.send_handshake_refusal(&reject_ack).await;
                    return Err(GridError::RegistrationRejected(
                        "cpu_cores must be > 0".into(),
                    ));
                }
                (worker_id, capabilities, features)
            }
            WorkerMessage::ClaimMasterRole { ticket } => {
                let ack = reject_master_role_claim(&ticket);
                pre_auth.send_handshake_refusal(&ack).await;
                return Ok(None);
            }
            other => {
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id: Uuid::nil(),
                    heartbeat_interval_secs: 0,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::empty(),
                    message: Some(format!(
                        "Expected Register message, received {}",
                        other.variant_name()
                    )),
                };
                pre_auth.send_handshake_refusal(&reject_ack).await;
                return Err(GridError::HandshakeFailed(format!(
                    "Expected Register message, received {}",
                    other.variant_name()
                )));
            }
        };

        let remote_addr = pre_auth.remote_addr();
        let rx_clock = pre_auth.rx_clock().clone();
        let is_multiplexed = pre_auth.is_multiplexed();

        // 3. Register in WorkerRegistry
        let negotiated_features = worker_features
            & (ProtocolCapabilities::all_supported() | ProtocolCapabilities::WORKSPACES);
        let worker_supports_resume =
            negotiated_features.contains(ProtocolCapabilities::SESSION_RESUME);

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<MasterMessage>(128);
        let registration = registry
            .register_session(
                worker_id,
                capabilities,
                negotiated_features,
                remote_addr,
                outbound_tx.clone(),
                abort_handle,
            )
            .await?;
        queue.set_worker_session_resume(worker_id, worker_supports_resume);
        let session_id = registration.session_id;
        queue.set_worker_session_resume_connected(worker_id, session_id, worker_supports_resume);
        registry
            .set_rx_clock(&worker_id, session_id, rx_clock)
            .await;

        // Fail over the superseded session's tasks; `failover_superseded_session` spawns a task
        // and awaits it so the sweep finishes even if this handler is aborted (`BUG-18`).
        if let Some(old_session) = registration.superseded_session {
            failover_superseded_session(
                &queue,
                &scheduler_notify,
                &waiters,
                worker_id,
                old_session,
                session_id,
                #[cfg(feature = "dashboard")]
                broadcast_tx.as_ref(),
            )
            .await;
        }
        if !worker_supports_resume {
            let outcome = queue.adopt_session_tasks(worker_id, session_id, &[]).await;
            if !outcome.newly_failed.is_empty() {
                if let Some(ref bstore) = blob_store {
                    for tid in &outcome.newly_failed {
                        bstore.unpin_task(tid).await;
                    }
                }
                resolve_terminal_waiters(&queue, &waiters, &outcome.newly_failed, Some(worker_id))
                    .await;
            }
        }

        #[cfg(feature = "p2p")]
        let p2p_guard = match (pre_auth.p2p_conn(), pre_auth.p2p_conns()) {
            (Some(conn), Some(conns)) => {
                conns.write().await.insert(worker_id, conn.clone());
                let path_info = rusty_grid_core::transport::inspect_connection_paths(conn);
                info!(
                    worker_id = %worker_id,
                    path_info = ?path_info,
                    "Tracked P2P connection path for registered worker"
                );
                Some(P2pConnGuard::new(
                    conn.clone(),
                    Arc::clone(conns),
                    worker_id,
                ))
            }
            _ => None,
        };

        // 4. Send RegisterAck
        let ack = MasterMessage::RegisterAck {
            accepted: true,
            worker_id,
            heartbeat_interval_secs,
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features: negotiated_features,
            message: Some(feature_announcement(
                blob_store.as_ref(),
                queue.session_resume_hold_secs(),
            )),
        };
        if let Err(e) = pre_auth.send_register_ack(&ack).await {
            handle_worker_disconnect_with_broadcast(
                &registry,
                &queue,
                &scheduler_notify,
                Some(&waiters),
                &worker_id,
                Some(session_id),
                "RegisterAck delivery failed",
                true,
                #[cfg(feature = "dashboard")]
                broadcast_tx.as_ref(),
            )
            .await;
            return Err(e.into());
        }

        #[cfg(feature = "dashboard")]
        if let Some(ref b_tx) = broadcast_tx {
            if let Some(info) = registry.get_worker(worker_id).await {
                let _ = b_tx
                    .send(crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(info));
            }
        }

        if is_multiplexed {
            info!(
                worker_id = %worker_id,
                session_id = session_id,
                remote = %remote_addr,
                "Worker successfully registered with master (QUIC 2-lane multiplexed)"
            );
        } else {
            info!(
                worker_id = %worker_id,
                session_id = session_id,
                remote = %remote_addr,
                "Worker successfully registered with master"
            );
        }

        // 5. Split transport(s) and spawn outbound writer task(s)
        let lanes = match pre_auth {
            PreAuthSession::Single { transport, .. } => {
                let (mut writer, reader) = transport.split();
                let writer_registry = registry.clone();
                let writer_queue = queue.clone();
                let writer_sched_notify = Arc::clone(&scheduler_notify);
                let writer_waiters = Arc::clone(&waiters);
                let writer_blob_store = blob_store.clone();
                #[cfg(feature = "dashboard")]
                let writer_broadcast_tx = broadcast_tx.clone();
                let writer_task = tokio::spawn(async move {
                    while let Some(msg) = outbound_rx.recv().await {
                        match writer.send_msg(&msg).await {
                            Ok(()) => {}
                            Err(ProtocolError::FrameTooLarge { size, max }) => {
                                handle_oversized_master_message(
                                    msg,
                                    size,
                                    max,
                                    &writer_registry,
                                    &writer_queue,
                                    &writer_sched_notify,
                                    &writer_waiters,
                                    worker_id,
                                    session_id,
                                    writer_blob_store.as_ref(),
                                    #[cfg(feature = "dashboard")]
                                    writer_broadcast_tx.as_ref(),
                                )
                                .await;
                            }
                            Err(e) => {
                                debug!(error = %e, "Outbound message write failed; terminating writer task");
                                break;
                            }
                        }
                    }
                });
                ActiveLanes::Single {
                    reader,
                    ctrl_reply_tx: outbound_tx.clone(),
                    _writer_guard: AbortOnDrop(writer_task.abort_handle()),
                }
            }
            PreAuthSession::Multiplexed {
                ctrl_transport,
                data_transport,
                ..
            } => {
                let (mut ctrl_writer, ctrl_reader) = ctrl_transport.split();
                let (mut data_writer, data_reader) = data_transport.split();

                let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<MasterMessage>(64);
                let (data_out_tx, mut data_out_rx) = mpsc::channel::<MasterMessage>(64);
                let ctrl_reply_tx = ctrl_out_tx.clone();

                let router_task = tokio::spawn(async move {
                    let mut pending_data: std::collections::VecDeque<MasterMessage> =
                        std::collections::VecDeque::new();

                    loop {
                        if pending_data.is_empty() {
                            match outbound_rx.recv().await {
                                Some(msg) => match msg {
                                    MasterMessage::AssignTask { .. }
                                    | MasterMessage::AssignTaskWithCheckpoint { .. }
                                    | MasterMessage::CancelTask { .. }
                                    | MasterMessage::BlobReply { .. } => {
                                        match data_out_tx.try_send(msg) {
                                            Ok(()) => {}
                                            Err(tokio::sync::mpsc::error::TrySendError::Full(
                                                data_msg,
                                            )) => {
                                                pending_data.push_back(data_msg);
                                            }
                                            Err(
                                                tokio::sync::mpsc::error::TrySendError::Closed(_),
                                            ) => break,
                                        }
                                    }
                                    ctrl_msg => {
                                        if ctrl_out_tx.send(ctrl_msg).await.is_err() {
                                            break;
                                        }
                                    }
                                },
                                None => break,
                            }
                        } else {
                            tokio::select! {
                                send_permit = data_out_tx.reserve() => {
                                    match send_permit {
                                        Ok(permit) => {
                                            if let Some(msg) = pending_data.pop_front() {
                                                permit.send(msg);
                                            }
                                        }
                                        Err(_) => break,
                                    }
                                }
                                msg_opt = outbound_rx.recv() => {
                                    match msg_opt {
                                        Some(msg) => match msg {
                                            MasterMessage::AssignTask { .. }
                                            | MasterMessage::AssignTaskWithCheckpoint { .. }
                                            | MasterMessage::CancelTask { .. }
                                            | MasterMessage::BlobReply { .. } => {
                                                pending_data.push_back(msg);
                                            }
                                            ctrl_msg => {
                                                if ctrl_out_tx.send(ctrl_msg).await.is_err() {
                                                    break;
                                                }
                                            }
                                        },
                                        None => {
                                            while let Some(msg) = pending_data.pop_front() {
                                                if data_out_tx.send(msg).await.is_err() {
                                                    break;
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                });

                let ctrl_writer_task = tokio::spawn(async move {
                    while let Some(msg) = ctrl_out_rx.recv().await {
                        match ctrl_writer.send_msg(&msg).await {
                            Ok(()) => {}
                            Err(ProtocolError::FrameTooLarge { size, max }) => {
                                warn!(
                                    worker_id = %worker_id,
                                    session_id = session_id,
                                    variant = %msg.variant_name(),
                                    size = size,
                                    max = max,
                                    "Control frame exceeds frame limit; dropping frame without closing session"
                                );
                            }
                            Err(e) => {
                                debug!(error = %e, "Control writer send error; terminating");
                                break;
                            }
                        }
                    }
                });

                let data_registry = registry.clone();
                let data_queue = queue.clone();
                let data_sched_notify = Arc::clone(&scheduler_notify);
                let data_waiters = Arc::clone(&waiters);
                let data_blob_store = blob_store.clone();
                #[cfg(feature = "dashboard")]
                let data_broadcast_tx = broadcast_tx.clone();
                let data_writer_task = tokio::spawn(async move {
                    while let Some(msg) = data_out_rx.recv().await {
                        match data_writer.send_msg(&msg).await {
                            Ok(()) => {}
                            Err(ProtocolError::FrameTooLarge { size, max }) => {
                                handle_oversized_master_message(
                                    msg,
                                    size,
                                    max,
                                    &data_registry,
                                    &data_queue,
                                    &data_sched_notify,
                                    &data_waiters,
                                    worker_id,
                                    session_id,
                                    data_blob_store.as_ref(),
                                    #[cfg(feature = "dashboard")]
                                    data_broadcast_tx.as_ref(),
                                )
                                .await;
                            }
                            Err(e) => {
                                debug!(error = %e, "Data writer send error; terminating");
                                break;
                            }
                        }
                    }
                });

                ActiveLanes::Multiplexed {
                    ctrl_reader,
                    data_reader,
                    ctrl_reply_tx,
                    _router_guard: AbortOnDrop(router_task.abort_handle()),
                    _ctrl_writer_guard: AbortOnDrop(ctrl_writer_task.abort_handle()),
                    _data_writer_guard: AbortOnDrop(data_writer_task.abort_handle()),
                }
            }
        };

        let blob_service = blob_store
            .clone()
            .map(|store| BlobService::spawn(store, outbound_tx.clone()));

        let sched_notify = scheduler_notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sched_notify.notify_one();
        });

        Ok(Some(Self {
            worker_id,
            session_id,
            negotiated_features,
            lanes,
            outbound_tx,
            blob_service,
            registry,
            queue,
            scheduler_notify,
            waiters,
            blob_store,
            shutdown_rx,
            #[cfg(feature = "dashboard")]
            broadcast_tx,
            #[cfg(feature = "p2p")]
            p2p_guard,
        }))
    }

    async fn run(&mut self) -> GridResult<()> {
        let default_lane = self.lanes.default_lane_kind();
        loop {
            tokio::select! {
                inbound = self.lanes.recv_next() => {
                    match inbound {
                        Ok((lane, Some(msg))) => {
                            if !self.handle_inbound_message(lane, msg).await {
                                break;
                            }
                        }
                        Ok((lane, None)) => {
                            let reason = lane.eof_reason();
                            info!(worker_id = %self.worker_id, ?lane, "{reason}");
                            handle_worker_disconnect_with_broadcast(
                                &self.registry,
                                &self.queue,
                                &self.scheduler_notify,
                                Some(&self.waiters),
                                &self.worker_id,
                                Some(self.session_id),
                                reason,
                                false,
                                #[cfg(feature = "dashboard")]
                                self.broadcast_tx.as_ref(),
                            )
                            .await;
                            break;
                        }
                        Err((lane, e)) => {
                            let reason = lane.error_reason(&e);
                            warn!(worker_id = %self.worker_id, ?lane, error = %e, "Worker stream read error");
                            handle_worker_disconnect_with_broadcast(
                                &self.registry,
                                &self.queue,
                                &self.scheduler_notify,
                                Some(&self.waiters),
                                &self.worker_id,
                                Some(self.session_id),
                                &reason,
                                false,
                                #[cfg(feature = "dashboard")]
                                self.broadcast_tx.as_ref(),
                            )
                            .await;
                            break;
                        }
                    }
                }
                res = self.shutdown_rx.changed() => {
                    if res.is_ok() && *self.shutdown_rx.borrow() {
                        info!(worker_id = %self.worker_id, "Master shutting down; notifying worker");
                        let _ = self
                            .lanes
                            .ctrl_reply_tx()
                            .send(MasterMessage::Shutdown {
                                reason: "Master server shutting down".into(),
                                grace_period_secs: Some(5),
                            })
                            .await;
                        break;
                    } else if res.is_err() {
                        break;
                    }
                }
            }
        }

        handle_worker_disconnect_with_broadcast(
            &self.registry,
            &self.queue,
            &self.scheduler_notify,
            Some(&self.waiters),
            &self.worker_id,
            Some(self.session_id),
            default_lane.cleanup_reason(),
            false,
            #[cfg(feature = "dashboard")]
            self.broadcast_tx.as_ref(),
        )
        .await;

        #[cfg(feature = "p2p")]
        if let Some(ref guard) = self.p2p_guard {
            guard.unlist().await;
        }

        Ok(())
    }

    /// Processes one inbound [`WorkerMessage`] from `lane`. Returns `false` if the session loop
    /// should exit (`Disconnecting`).
    async fn handle_inbound_message(&mut self, lane: LaneKind, msg: WorkerMessage) -> bool {
        let worker_id = self.worker_id;
        let session_id = self.session_id;

        match (lane, msg) {
            (
                LaneKind::Unified | LaneKind::Control,
                WorkerMessage::Heartbeat {
                    worker_id: id,
                    timestamp,
                    active_tasks,
                    cpu_usage_pct,
                    ram_available_mb,
                },
            ) => {
                if id == worker_id {
                    let heartbeat = self
                        .registry
                        .record_heartbeat_session(
                            &worker_id,
                            active_tasks,
                            timestamp,
                            cpu_usage_pct,
                            ram_available_mb,
                            Some(session_id),
                        )
                        .await;
                    if let Ok(HeartbeatOutcome::Revived) = heartbeat {
                        self.scheduler_notify.notify_one();
                        #[cfg(feature = "dashboard")]
                        if let Some(ref b_tx) = self.broadcast_tx {
                            if let Some(info) = self.registry.get_worker(worker_id).await {
                                let _ = b_tx.send(
                                    crate::dashboard::dto::DashboardStreamMessage::WorkerRegistered(
                                        info,
                                    ),
                                );
                            }
                        }
                    }
                    let _ = self
                        .lanes
                        .ctrl_reply_tx()
                        .send(MasterMessage::HeartbeatAck { timestamp })
                        .await;
                    #[cfg(feature = "dashboard")]
                    if let Some(ref b_tx) = self.broadcast_tx {
                        let _ = b_tx.send(
                            crate::dashboard::dto::DashboardStreamMessage::WorkerHeartbeat {
                                worker_id: id,
                                cpu_usage_pct,
                                ram_available_mb,
                                active_tasks,
                                timestamp,
                            },
                        );
                    }
                } else {
                    warn!(expected = %worker_id, got = %id, "Heartbeat worker_id mismatch");
                }
                true
            }
            (
                LaneKind::Unified | LaneKind::Control,
                WorkerMessage::Disconnecting {
                    worker_id: id,
                    reason,
                },
            ) => {
                info!(worker_id = %id, reason = %reason, "Worker announced graceful disconnection");
                handle_worker_evacuate_with_broadcast(
                    &self.registry,
                    &self.queue,
                    &self.scheduler_notify,
                    Some(&self.waiters),
                    &worker_id,
                    Some(session_id),
                    &format!("Worker graceful disconnect: {reason}"),
                    #[cfg(feature = "dashboard")]
                    self.broadcast_tx.as_ref(),
                )
                .await;
                false
            }
            (
                LaneKind::Unified | LaneKind::Control,
                WorkerMessage::MobileTelemetry {
                    worker_id: claimed,
                    battery_pct,
                    is_charging,
                    thermal_throttled,
                    battery_temperature,
                    network_type,
                    battery_tier,
                    thermal_tier,
                },
            ) => {
                apply_mobile_telemetry(
                    &self.registry,
                    &self.scheduler_notify,
                    worker_id,
                    session_id,
                    claimed,
                    MobileLiveStatus {
                        battery_pct,
                        is_charging,
                        thermal_throttled,
                        battery_temperature,
                        network_type,
                        battery_tier,
                        thermal_tier,
                    },
                )
                .await;
                true
            }
            (LaneKind::Unified | LaneKind::Control, WorkerMessage::ClaimMasterRole { ticket }) => {
                let ack = reject_master_role_claim(&ticket);
                let _ = self.lanes.ctrl_reply_tx().send(ack).await;
                true
            }
            (
                LaneKind::Unified | LaneKind::Data,
                msg @ (WorkerMessage::TaskProgress { .. }
                | WorkerMessage::TaskResult { .. }
                | WorkerMessage::Checkpoint { .. }),
            ) => {
                handle_session_task_message(
                    msg,
                    &self.registry,
                    &self.queue,
                    &self.scheduler_notify,
                    &self.waiters,
                    worker_id,
                    session_id,
                    self.blob_store.as_ref(),
                    #[cfg(feature = "dashboard")]
                    self.broadcast_tx.as_ref(),
                )
                .await;
                true
            }
            (
                _,
                WorkerMessage::BlobRequest {
                    request_id,
                    request,
                },
            ) => {
                serve_worker_blob_request(
                    self.blob_service.as_ref(),
                    &self.outbound_tx,
                    request_id,
                    request,
                )
                .await;
                true
            }
            (LaneKind::Unified, WorkerMessage::Register { .. }) => {
                warn!(worker_id = %worker_id, "Ignoring unexpected Register message on active connection");
                true
            }
            (
                LaneKind::Unified | LaneKind::Control,
                WorkerMessage::ResumeSession {
                    worker_id: claimed,
                    running,
                },
            ) => {
                if !self
                    .negotiated_features
                    .contains(ProtocolCapabilities::SESSION_RESUME)
                {
                    warn!(
                        worker_id = %worker_id,
                        session_id = session_id,
                        "Refusing ResumeSession from a session that did not negotiate SESSION_RESUME"
                    );
                    handle_worker_disconnect_with_broadcast(
                        &self.registry,
                        &self.queue,
                        &self.scheduler_notify,
                        Some(&self.waiters),
                        &worker_id,
                        Some(session_id),
                        "ResumeSession sent without negotiated SESSION_RESUME feature",
                        false,
                        #[cfg(feature = "dashboard")]
                        self.broadcast_tx.as_ref(),
                    )
                    .await;
                    return false;
                }
                if claimed != worker_id {
                    warn!(
                        expected = %worker_id,
                        got = %claimed,
                        "ResumeSession worker_id mismatch; disconnecting session"
                    );
                    handle_worker_disconnect_with_broadcast(
                        &self.registry,
                        &self.queue,
                        &self.scheduler_notify,
                        Some(&self.waiters),
                        &worker_id,
                        Some(session_id),
                        "ResumeSession worker_id mismatch",
                        false,
                        #[cfg(feature = "dashboard")]
                        self.broadcast_tx.as_ref(),
                    )
                    .await;
                    return false;
                }
                let outcome = self
                    .queue
                    .adopt_session_tasks(worker_id, session_id, &running)
                    .await;
                if !outcome.adopted.is_empty() {
                    self.registry
                        .add_adopted_tasks(&worker_id, session_id, outcome.adopted.len())
                        .await;
                    info!(
                        worker_id = %worker_id,
                        session_id = session_id,
                        adopted = ?outcome.adopted,
                        "Worker resumed session and adopted in-flight task(s)"
                    );
                }
                for stale_id in outcome.stale_to_cancel {
                    let _ = self
                        .outbound_tx
                        .send(MasterMessage::CancelTask {
                            task_id: stale_id,
                            reason: Some("Task is no longer assigned to this worker".into()),
                        })
                        .await;
                }
                if !outcome.newly_failed.is_empty() {
                    if let Some(ref bstore) = self.blob_store {
                        for tid in &outcome.newly_failed {
                            bstore.unpin_task(tid).await;
                        }
                    }
                    resolve_terminal_waiters(
                        &self.queue,
                        &self.waiters,
                        &outcome.newly_failed,
                        Some(worker_id),
                    )
                    .await;
                }
                #[cfg(feature = "dashboard")]
                if let Some(ref b_tx) = self.broadcast_tx {
                    for tid in outcome.adopted.iter().chain(outcome.newly_failed.iter()) {
                        if let Some(info) = self.queue.get_task(tid).await {
                            let _ = b_tx.send(
                                crate::dashboard::dto::DashboardStreamMessage::TaskUpdated(info),
                            );
                        }
                    }
                    let _ = b_tx.send(crate::dashboard::dto::DashboardStreamMessage::StatsUpdated(
                        self.queue.stats().await,
                    ));
                }
                self.scheduler_notify.notify_one();
                true
            }
            (LaneKind::Unified, WorkerMessage::UnknownVariant { tag, ref raw_bytes }) => {
                warn!(
                    worker_id = %worker_id,
                    tag,
                    raw_bytes_len = raw_bytes.len(),
                    "Ignoring unrecognized WorkerMessage variant from future protocol version"
                );
                true
            }
            (LaneKind::Control, other) => {
                warn!(
                    worker_id = %worker_id,
                    message = %other.variant_name(),
                    "Unexpected message on control stream"
                );
                true
            }
            (LaneKind::Data, other) => {
                warn!(
                    worker_id = %worker_id,
                    message = %other.variant_name(),
                    "Unexpected message on data stream"
                );
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    use rusty_grid_core::arbitrage::{MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS};
    use rusty_grid_core::blob::BlobId;
    use rusty_grid_core::blob_transfer::{BlobReply, BlobRequest};
    use rusty_grid_core::capabilities::{
        BatteryTier, MobileCapabilities, ThermalTier, WorkerCapabilities,
    };
    use rusty_grid_core::task::{Bytes, Task, TaskRequirements, TaskSpec, TaskStatus};

    use crate::blob_store::{BlobStore, BlobStoreConfig, DEFAULT_BLOB_STORE_MAX_BYTES};
    use crate::queue::TaskState;

    #[allow(clippy::too_many_arguments)]
    fn spawn_test_multiplexed_session(
        ctrl_master: tokio::io::DuplexStream,
        data_master: tokio::io::DuplexStream,
        registry: WorkerRegistry,
        queue: TaskQueue,
        scheduler_notify: Arc<tokio::sync::Notify>,
        waiters: WaiterMap,
        stop_rx: watch::Receiver<bool>,
        blob_store: Option<Arc<BlobStore>>,
    ) -> tokio::task::JoinHandle<GridResult<()>> {
        tokio::spawn(async move {
            let pre_auth = PreAuthSession::new_multiplexed(
                ctrl_master,
                data_master,
                SocketAddr::from(([127, 0, 0, 1], 55_000)),
                WireCodec::Bincode,
                #[cfg(feature = "p2p")]
                None,
                #[cfg(feature = "p2p")]
                None,
            );
            WorkerSession::run_connection(
                pre_auth,
                registry,
                queue,
                scheduler_notify,
                waiters,
                10,
                Duration::from_secs(5),
                stop_rx,
                None,
                WireCodec::Bincode,
                None,
                MasterAuthContext::default(),
                None,
                blob_store,
                #[cfg(feature = "dashboard")]
                None,
            )
            .await
        })
    }

    #[tokio::test]
    async fn test_multiplexed_blob_reply_is_routed_to_data_lane() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(&BlobStoreConfig::new(
            temp_dir.path().join("blobs"),
            DEFAULT_BLOB_STORE_MAX_BYTES,
        ))
        .unwrap();
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let (stop_tx, stop_rx) = watch::channel(false);

        // Use a small 64-byte duplex buffer on the data lane so Phase 2 can deterministically
        // backpressure `data_out_tx` and exercise the `!pending_data.is_empty()` branch too.
        let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
        let (d_master, d_worker) = tokio::io::duplex(64);
        let session_task = spawn_test_multiplexed_session(
            c_master,
            d_master,
            registry.clone(),
            queue.clone(),
            Arc::clone(&scheduler_notify),
            Arc::clone(&waiters),
            stop_rx,
            Some(Arc::clone(&store)),
        );

        let mut mux_ctrl = MessageTransport::new(c_worker);
        let mut mux_data = MessageTransport::new(d_worker);
        let worker_id = Uuid::new_v4();
        mux_ctrl
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::new(
                    "mux-blob-worker",
                    2,
                    4096,
                    false,
                    false,
                    None,
                ),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        match mux_ctrl.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected RegisterAck, got {other:?}"),
        }

        // Phase 1 (`pending_data.is_empty()` branch in `router_task`):
        // Send a `BlobRequest` followed by a control-lane `Heartbeat`. `BlobReply` must arrive on
        // the data lane (`mux_data`) while `HeartbeatAck` arrives on the control lane (`mux_ctrl`).
        let probe_id_1 = BlobId::of(b"blob-on-data-lane-1");
        mux_data
            .send_msg(&WorkerMessage::BlobRequest {
                request_id: 101,
                request: BlobRequest::Missing {
                    ids: vec![probe_id_1],
                },
            })
            .await
            .unwrap();

        let data_reply =
            tokio::time::timeout(Duration::from_secs(2), mux_data.recv_msg::<MasterMessage>())
                .await
                .expect(
                    "BlobReply (request_id=101) must arrive on the data lane without timing out",
                )
                .expect("data lane decode must succeed")
                .expect("data lane must stay open");
        assert_eq!(
            data_reply,
            MasterMessage::BlobReply {
                request_id: 101,
                reply: BlobReply::Missing {
                    ids: vec![probe_id_1],
                },
            }
        );

        mux_ctrl
            .send_msg(&WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 1_001,
                active_tasks: 0,
                cpu_usage_pct: 10.0,
                ram_available_mb: 4096,
            })
            .await
            .unwrap();
        let ctrl_reply =
            tokio::time::timeout(Duration::from_secs(2), mux_ctrl.recv_msg::<MasterMessage>())
                .await
                .expect("HeartbeatAck must arrive on the control lane")
                .expect("control lane decode must succeed")
                .expect("control lane must stay open");
        assert_eq!(ctrl_reply, MasterMessage::HeartbeatAck { timestamp: 1_001 });

        // Phase 2 (`!pending_data.is_empty()` branch in `router_task`):
        // Stop reading `mux_data` and flood 80 `AssignTask` frames so the 64-byte duplex buffer
        // and 64-slot `data_out_tx` channel fill up and overflow into `pending_data`.
        let flood_count = 80usize;
        let mut expected_task_ids = Vec::with_capacity(flood_count);
        for i in 0..flood_count {
            let task = Task::new(
                TaskSpec::Command {
                    program: "echo".into(),
                    args: vec![format!("blob-flood-{i}"), "p".repeat(1024)],
                    env: HashMap::new(),
                    working_dir: None,
                    stdin: None,
                },
                TaskRequirements::generic(1, 64),
            );
            expected_task_ids.push(task.id);
            registry
                .send_to_worker(&worker_id, MasterMessage::AssignTask { task })
                .await
                .unwrap();
        }
        // Give `router_task` a brief turn to drain `outbound_rx` into `pending_data`.
        tokio::time::sleep(Duration::from_millis(25)).await;

        let probe_id_2 = BlobId::of(b"blob-on-data-lane-2");
        mux_data
            .send_msg(&WorkerMessage::BlobRequest {
                request_id: 202,
                request: BlobRequest::Missing {
                    ids: vec![probe_id_2],
                },
            })
            .await
            .unwrap();
        // Wait briefly so `BlobService` enqueues `BlobReply(202)` onto `outbound_rx` while
        // `pending_data` is still non-empty.
        tokio::time::sleep(Duration::from_millis(25)).await;

        // A control-lane heartbeat sent now must still receive `HeartbeatAck` on `mux_ctrl`
        // (and never `BlobReply(202)`).
        mux_ctrl
            .send_msg(&WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 2_002,
                active_tasks: 0,
                cpu_usage_pct: 15.0,
                ram_available_mb: 4096,
            })
            .await
            .unwrap();
        let ctrl_reply_2 =
            tokio::time::timeout(Duration::from_secs(2), mux_ctrl.recv_msg::<MasterMessage>())
                .await
                .expect(
                    "HeartbeatAck must arrive on the control lane while data lane is backpressured",
                )
                .expect("control lane decode must succeed")
                .expect("control lane must stay open");
        assert_eq!(
            ctrl_reply_2,
            MasterMessage::HeartbeatAck { timestamp: 2_002 },
            "control lane must receive HeartbeatAck, never a misrouted BlobReply"
        );

        // Now drain the 80 `AssignTask` messages and the trailing `BlobReply(202)` from `mux_data`.
        for expected_id in expected_task_ids {
            let msg =
                tokio::time::timeout(Duration::from_secs(2), mux_data.recv_msg::<MasterMessage>())
                    .await
                    .expect("queued AssignTask must drain from data lane")
                    .expect("data lane decode must succeed")
                    .expect("data lane must stay open");
            match msg {
                MasterMessage::AssignTask { task } => assert_eq!(task.id, expected_id),
                other => panic!("expected AssignTask({expected_id}), got {other:?}"),
            }
        }
        let data_reply_2 = tokio::time::timeout(
            Duration::from_secs(2),
            mux_data.recv_msg::<MasterMessage>(),
        )
        .await
        .expect("BlobReply (request_id=202) queued while pending_data was non-empty must arrive on data lane")
        .expect("data lane decode must succeed")
        .expect("data lane must stay open");
        assert_eq!(
            data_reply_2,
            MasterMessage::BlobReply {
                request_id: 202,
                reply: BlobReply::Missing {
                    ids: vec![probe_id_2],
                },
            }
        );

        let _ = stop_tx.send(true);
        session_task.abort();
    }

    #[tokio::test]
    async fn test_multiplexed_inbound_lane_separation_ignores_wrong_lane_messages() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let (stop_tx, stop_rx) = watch::channel(false);

        let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
        let (d_master, d_worker) = tokio::io::duplex(64 * 1024);
        let session_task = spawn_test_multiplexed_session(
            c_master,
            d_master,
            registry.clone(),
            queue.clone(),
            Arc::clone(&scheduler_notify),
            Arc::clone(&waiters),
            stop_rx,
            None,
        );

        let mut mux_ctrl = MessageTransport::new(c_worker);
        let mut mux_data = MessageTransport::new(d_worker);
        let worker_id = Uuid::new_v4();
        let mut caps = WorkerCapabilities::new("mux-lane-sep-worker", 2, 4096, false, false, None);
        caps.mobile = Some(MobileCapabilities {
            soc_model: "Pixel-Test".into(),
            os_version: "15".into(),
            battery_pct: Some(90),
            is_charging: Some(true),
            thermal_throttled: false,
        });
        mux_ctrl
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: caps,
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        match mux_ctrl.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected RegisterAck, got {other:?}"),
        }

        let before = registry.get_worker(worker_id).await.unwrap();
        assert_eq!(before.active_tasks, 0);
        assert_eq!(before.cpu_usage_pct, 0.0);
        assert_ne!(before.last_heartbeat_timestamp, 999_999);
        assert_eq!(
            before
                .capabilities
                .mobile
                .as_ref()
                .and_then(|m| m.battery_pct),
            Some(90)
        );

        // 1. Send control-only messages (`Heartbeat`, `MobileTelemetry`, `ClaimMasterRole`,
        //    `Disconnecting`) on the DATA lane, followed by a `BlobRequest` barrier on the data
        //    lane so we know the session loop has already read and processed all four messages.
        let secret = ClusterSecret::generate().unwrap();
        let ticket = MasterLeaseTicket::issue(
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            5_000,
            1_000,
            Duration::from_millis(LEASE_VALIDITY_DURATION_MS),
            &secret,
        );
        mux_data
            .send_msg(&WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 999_999,
                active_tasks: 7,
                cpu_usage_pct: 88.0,
                ram_available_mb: 512,
            })
            .await
            .unwrap();
        mux_data
            .send_msg(&WorkerMessage::MobileTelemetry {
                worker_id,
                battery_pct: Some(15),
                is_charging: Some(false),
                thermal_throttled: true,
                battery_temperature: Some(46.0),
                network_type: Some("cellular".into()),
                battery_tier: Some(BatteryTier::Critical),
                thermal_tier: Some(ThermalTier::Critical),
            })
            .await
            .unwrap();
        mux_data
            .send_msg(&WorkerMessage::ClaimMasterRole { ticket })
            .await
            .unwrap();
        mux_data
            .send_msg(&WorkerMessage::Disconnecting {
                worker_id,
                reason: "wrong-lane disconnect".into(),
            })
            .await
            .unwrap();
        mux_data
            .send_msg(&WorkerMessage::BlobRequest {
                request_id: 1,
                request: BlobRequest::Missing { ids: vec![] },
            })
            .await
            .unwrap();

        let barrier_reply = tokio::time::timeout(
            Duration::from_secs(2),
            mux_data.recv_msg::<MasterMessage>(),
        )
        .await
        .expect("data-lane BlobRequest barrier must be answered; Disconnecting on data lane must not close session")
        .expect("data lane decode must succeed")
        .expect("data lane must stay open");
        assert!(matches!(
            barrier_reply,
            MasterMessage::BlobReply { request_id: 1, .. }
        ));

        let after_wrong_data = registry
            .get_worker(worker_id)
            .await
            .expect("worker must remain registered after wrong-lane Disconnecting on data stream");
        assert_eq!(
            after_wrong_data.active_tasks, 0,
            "Heartbeat on data lane must not update active_tasks"
        );
        assert_eq!(
            after_wrong_data.cpu_usage_pct, 0.0,
            "Heartbeat on data lane must not update cpu_usage_pct"
        );
        assert_eq!(
            after_wrong_data.last_heartbeat_timestamp, before.last_heartbeat_timestamp,
            "Heartbeat on data lane must not update last_heartbeat_timestamp"
        );
        assert_eq!(
            after_wrong_data
                .capabilities
                .mobile
                .as_ref()
                .and_then(|m| m.battery_pct),
            Some(90),
            "MobileTelemetry on data lane must not update mobile capabilities"
        );

        // No `HeartbeatAck` (for timestamp 999_999) or `RoleHandoverAck` may have been sent on `mux_ctrl`.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                mux_ctrl.recv_msg::<MasterMessage>(),
            )
            .await
            .is_err(),
            "Heartbeat and ClaimMasterRole on data lane must not produce any reply on the control lane"
        );

        // 2. Send data-only messages (`TaskProgress`, `Checkpoint`, `TaskResult`) on the CONTROL
        //    lane, followed by a valid `Heartbeat` barrier on the control lane.
        let session_id = after_wrong_data.session_id;
        let task = Task::new(
            TaskSpec::command("echo", vec!["lane-check".into()]),
            TaskRequirements::generic(1, 64),
        );
        let task_id = queue.submit(task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, session_id)])
            .await;

        mux_ctrl
            .send_msg(&WorkerMessage::TaskProgress {
                worker_id,
                task_id,
                status: TaskStatus::Running,
            })
            .await
            .unwrap();
        mux_ctrl
            .send_msg(&WorkerMessage::Checkpoint {
                task_id,
                sequence: 1,
                delta_state: Bytes::from_static(b"wrong-lane-cp"),
            })
            .await
            .unwrap();
        mux_ctrl
            .send_msg(&WorkerMessage::TaskResult {
                worker_id,
                task_id,
                exit_code: 0,
                stdout: Bytes::from_static(b"wrong-lane-result"),
                stderr: Bytes::new(),
                execution_time_ms: 5,
                is_gpu_executed: false,
                device_name: None,
                error: None,
                asset_bytes: None,
                artifacts: vec![],
                outputs: vec![],
            })
            .await
            .unwrap();
        mux_ctrl
            .send_msg(&WorkerMessage::Heartbeat {
                worker_id,
                timestamp: 123_456,
                active_tasks: 1,
                cpu_usage_pct: 42.0,
                ram_available_mb: 2048,
            })
            .await
            .unwrap();

        let ctrl_ack =
            tokio::time::timeout(Duration::from_secs(2), mux_ctrl.recv_msg::<MasterMessage>())
                .await
                .expect("valid control-lane Heartbeat must receive HeartbeatAck")
                .expect("control lane decode must succeed")
                .expect("control lane must stay open");
        assert_eq!(ctrl_ack, MasterMessage::HeartbeatAck { timestamp: 123_456 });

        let task_info = queue.get_task(&task_id).await.unwrap();
        assert_eq!(
            task_info.state,
            TaskState::Scheduled,
            "TaskProgress/TaskResult on control lane must be ignored and leave task in Scheduled state"
        );
        assert!(
            queue.get_result(&task_id).await.is_none(),
            "TaskResult on control lane must not record a result"
        );

        let _ = stop_tx.send(true);
        session_task.abort();
    }

    #[tokio::test]
    async fn test_register_call_site_supersede_sweep_survives_session_abort() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let (stop_tx, stop_rx) = watch::channel(false);
        let worker_id = Uuid::new_v4();

        // 1. Register session 1 via `WorkerSession::run_connection` and put a running task on it.
        let (c1_master, c1_worker) = tokio::io::duplex(64 * 1024);
        let (d1_master, _d1_worker) = tokio::io::duplex(64 * 1024);
        let session1_task = spawn_test_multiplexed_session(
            c1_master,
            d1_master,
            registry.clone(),
            queue.clone(),
            Arc::clone(&scheduler_notify),
            Arc::clone(&waiters),
            stop_rx.clone(),
            None,
        );
        let mut ctrl1 = MessageTransport::new(c1_worker);
        ctrl1
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::new(
                    "supersede-worker",
                    2,
                    4096,
                    false,
                    false,
                    None,
                ),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        match ctrl1.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected session 1 RegisterAck, got {other:?}"),
        }
        assert_eq!(
            registry.get_worker(worker_id).await.map(|w| w.session_id),
            Some(1)
        );

        let task = Task::new(
            TaskSpec::command("echo", vec!["supersede-call-site".into()]),
            TaskRequirements::generic(1, 30),
        );
        let task_id = queue.submit(task).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, 1)])
            .await;
        queue
            .mark_running_from(worker_id, Some(1), &task_id)
            .await
            .unwrap();

        // 2. Hold the queue write lock, then start session 2's `WorkerSession::run_connection`
        //    so it registers session 2 in `WorkerRegistry` and parks at the `session.rs`
        //    `failover_superseded_session` call site waiting for the queue write lock.
        let lock = queue.hold_write_lock_for_test().await;
        let (c2_master, c2_worker) = tokio::io::duplex(64 * 1024);
        let (d2_master, _d2_worker) = tokio::io::duplex(64 * 1024);
        let session2_task = spawn_test_multiplexed_session(
            c2_master,
            d2_master,
            registry.clone(),
            queue.clone(),
            Arc::clone(&scheduler_notify),
            Arc::clone(&waiters),
            stop_rx,
            None,
        );
        let mut ctrl2 = MessageTransport::new(c2_worker);
        ctrl2
            .send_msg(&WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::new(
                    "supersede-worker",
                    2,
                    4096,
                    false,
                    false,
                    None,
                ),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while registry.get_worker(worker_id).await.map(|w| w.session_id) != Some(2) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "session 2 did not reach WorkerRegistry::register_session"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let session 2 enter `failover_superseded_session` and spawn its inner sweep task.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Abort session 2's connection handler while it is parked in `failover_superseded_session`,
        // then release the queue write lock. Session 1's task must still fail over to `Queued`.
        session2_task.abort();
        assert!(session2_task.await.unwrap_err().is_cancelled());
        drop(lock);

        let failover_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while queue.get_state(&task_id).await != Some(TaskState::Queued) {
            assert!(
                tokio::time::Instant::now() < failover_deadline,
                "session 1's task stayed in {:?} after session 2 was aborted mid-sweep",
                queue.get_state(&task_id).await
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(queue.get_task(&task_id).await.unwrap().retry_count, 1);

        let _ = stop_tx.send(true);
        session1_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rv158_probe_quic_buffered_result_races_resume_session() {
        let mut lost = 0usize;
        let mut completed = 0usize;
        let rounds = 60usize;
        for _ in 0..rounds {
            let registry = WorkerRegistry::new();
            let queue = TaskQueue::new();
            queue.set_session_resume_hold(Duration::from_secs(30)).await;
            let scheduler_notify = Arc::new(tokio::sync::Notify::new());
            let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
            let (_stop_tx, stop_rx) = watch::channel(false);
            let worker_id = Uuid::new_v4();
            let register = || WorkerMessage::Register {
                worker_id,
                capabilities: WorkerCapabilities::new("rv158", 2, 4096, false, false, None),
                protocol_version: PROTOCOL_VERSION_CURRENT,
                features: ProtocolCapabilities::all_supported(),
            };

            let (c1m, c1w) = tokio::io::duplex(64 * 1024);
            let (d1m, d1w) = tokio::io::duplex(64 * 1024);
            let s1 = spawn_test_multiplexed_session(
                c1m,
                d1m,
                registry.clone(),
                queue.clone(),
                Arc::clone(&scheduler_notify),
                Arc::clone(&waiters),
                stop_rx.clone(),
                None,
            );
            let mut ctrl1 = MessageTransport::new(c1w);
            ctrl1.send_msg(&register()).await.unwrap();
            match ctrl1.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
                other => panic!("expected RegisterAck, got {other:?}"),
            }
            let sid1 = registry.get_worker(worker_id).await.unwrap().session_id;
            let task = Task::new(
                TaskSpec::command("echo", vec!["rv158".into()]),
                TaskRequirements::generic(1, 64),
            );
            let task_id = queue.submit(task).await.unwrap();
            queue
                .schedule_tasks_batch_in_sessions(&[(task_id, worker_id, sid1)])
                .await;
            queue
                .mark_running_from(worker_id, Some(sid1), &task_id)
                .await
                .unwrap();
            drop(ctrl1);
            drop(d1w);
            let _ = tokio::time::timeout(Duration::from_secs(2), s1).await;
            let held = queue.get_task(&task_id).await.unwrap();
            assert_eq!(
                held.state,
                TaskState::Queued,
                "task must be held after drop"
            );

            let (c2m, c2w) = tokio::io::duplex(64 * 1024);
            let (d2m, d2w) = tokio::io::duplex(64 * 1024);
            let s2 = spawn_test_multiplexed_session(
                c2m,
                d2m,
                registry.clone(),
                queue.clone(),
                Arc::clone(&scheduler_notify),
                Arc::clone(&waiters),
                stop_rx.clone(),
                None,
            );
            let mut ctrl2 = MessageTransport::new(c2w);
            let mut data2 = MessageTransport::new(d2w);
            ctrl2.send_msg(&register()).await.unwrap();
            match ctrl2.recv_msg::<MasterMessage>().await.unwrap() {
                Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
                other => panic!("expected RegisterAck, got {other:?}"),
            }
            ctrl2
                .send_msg(&WorkerMessage::ResumeSession {
                    worker_id,
                    running: vec![task_id],
                })
                .await
                .unwrap();
            data2
                .send_msg(&WorkerMessage::TaskResult {
                    worker_id,
                    task_id,
                    exit_code: 0,
                    stdout: Bytes::from_static(b"done-while-disconnected"),
                    stderr: Bytes::new(),
                    execution_time_ms: 5,
                    is_gpu_executed: false,
                    device_name: None,
                    error: None,
                    asset_bytes: None,
                    artifacts: vec![],
                    outputs: vec![],
                })
                .await
                .unwrap();

            let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
            let mut state = queue.get_task(&task_id).await.unwrap().state;
            while state != TaskState::Completed && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
                state = queue.get_task(&task_id).await.unwrap().state;
            }
            match state {
                TaskState::Completed => completed += 1,
                TaskState::Running => lost += 1,
                other => panic!("unexpected state {other:?}"),
            }
            drop(ctrl2);
            drop(data2);
            s2.abort();
        }
        eprintln!(
            "RV158 PROBE P1: rounds={rounds} completed={completed} stuck_running_result_lost={lost}"
        );
        assert_eq!(
            lost, 0,
            "a buffered TaskResult flushed after ResumeSession was dropped as stale in {lost}/{rounds} rounds; the adopted task stays Running forever"
        );
    }

    #[tokio::test]
    async fn test_resume_session_rejected_when_capability_not_negotiated() {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        queue.set_session_resume_hold(Duration::from_secs(30)).await;
        let scheduler_notify = Arc::new(tokio::sync::Notify::new());
        let waiters: WaiterMap = Arc::new(RwLock::new(HashMap::new()));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let worker_id = Uuid::new_v4();

        let (c_master, c_worker) = tokio::io::duplex(64 * 1024);
        let (d_master, _d_worker) = tokio::io::duplex(64 * 1024);
        let session_task = spawn_test_multiplexed_session(
            c_master,
            d_master,
            registry.clone(),
            queue.clone(),
            Arc::clone(&scheduler_notify),
            Arc::clone(&waiters),
            stop_rx,
            None,
        );
        let mut ctrl = MessageTransport::new(c_worker);
        let mut features = ProtocolCapabilities::all_supported();
        features.remove(ProtocolCapabilities::SESSION_RESUME);
        ctrl.send_msg(&WorkerMessage::Register {
            worker_id,
            capabilities: WorkerCapabilities::new("no-resume", 2, 4096, false, false, None),
            protocol_version: PROTOCOL_VERSION_CURRENT,
            features,
        })
        .await
        .unwrap();
        match ctrl.recv_msg::<MasterMessage>().await.unwrap() {
            Some(MasterMessage::RegisterAck { accepted: true, .. }) => {}
            other => panic!("expected RegisterAck, got {other:?}"),
        }
        ctrl.send_msg(&WorkerMessage::ResumeSession {
            worker_id,
            running: vec![],
        })
        .await
        .unwrap();

        let res = tokio::time::timeout(Duration::from_secs(2), session_task)
            .await
            .expect("session must terminate when ResumeSession is sent without SESSION_RESUME")
            .unwrap();
        assert!(res.is_ok());
        assert_eq!(
            registry.get_worker(worker_id).await.unwrap().status,
            crate::registry::WorkerStatus::Disconnected
        );
    }
}
