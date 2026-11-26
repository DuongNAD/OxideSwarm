//! Connection authentication (v2 and v3), refusal draining, connection dispatch, and rendezvous.

#[cfg(feature = "p2p")]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{watch, OwnedSemaphorePermit};
use tokio::task::AbortHandle;
use tracing::{debug, warn};
#[cfg(feature = "p2p")]
use uuid::Uuid;

use rusty_grid_core::cluster_auth::{
    peek_hello_version, recv_auth_frame, reject_peer, send_reject, server_handshake_from_hello,
    ClusterSecret, AUTH_VERSION, AUTH_VERSION_V3, REJECT_AUTH_FAILED, REJECT_AUTH_REQUIRED,
    REJECT_MEMBERS_ONLY, REJECT_NO_SECRET, WIRE_FORMAT_AUTH,
};
use rusty_grid_core::noise_auth::{
    v3_server_confirm_first_record, v3_server_handshake_from_hello, AuthenticatedV3Peer,
    HandshakePurpose, NodeKeypair, PublicKey,
};
use rusty_grid_core::protocol::{
    format_incompatible_version_error, is_protocol_version_compatible, ClientMessage,
    ClientResponse, EndpointRole, InboundMessage, ProtocolCapabilities, ProtocolError,
    PROTOCOL_VERSION_CURRENT, WIRE_FORMAT_JSON,
};
use rusty_grid_core::{
    GridError, GridResult, MasterMessage, MessageTransport, WireCodec, WorkerMessage,
};

use crate::blob_store::BlobStore;
use crate::member_connections::MemberConnections;
use crate::queue::TaskQueue;
use crate::registry::WorkerRegistry;

use super::client_and_members::ENROLL_CONFIRM_TIMEOUT_SECS;
use super::members::{self, AuthMode, MemberStore};
use super::WaiterMap;

#[derive(Clone)]
pub(crate) struct MasterAuthContext {
    pub auth_mode: AuthMode,
    pub master_noise_key: Option<NodeKeypair>,
    pub master_noise_key_path: Option<PathBuf>,
    pub local_node_key_path: Option<PathBuf>,
    pub ephemeral_without_state_dir: bool,
    pub members: Arc<MemberStore>,
    /// Live connections of each v3 member, closed together when the member is revoked (design
    /// 0051 D5). One registry per master, shared by its TCP and QUIC accept loops.
    pub member_connections: MemberConnections,
    pub p2p_ticket: Option<String>,
    pub enroll_confirm_timeout: Duration,
    pub mailbox: Arc<crate::mail::Mailbox>,
}

impl Default for MasterAuthContext {
    fn default() -> Self {
        Self {
            auth_mode: AuthMode::default(),
            master_noise_key: None,
            master_noise_key_path: None,
            local_node_key_path: None,
            ephemeral_without_state_dir: false,
            members: Arc::default(),
            member_connections: MemberConnections::default(),
            p2p_ticket: None,
            enroll_confirm_timeout: Duration::from_secs(ENROLL_CONFIRM_TIMEOUT_SECS),
            mailbox: Arc::new(crate::mail::Mailbox::in_memory_default()),
        }
    }
}

impl MasterAuthContext {
    /// Ensures `master_noise.key` is persisted on disk and the local host `node.key` is enrolled as
    /// `local-host` (`admin`) before `CreateInvite` hands out the master's v3 public key.
    pub fn ensure_v3_key_persisted(&self) -> GridResult<PublicKey> {
        let Some(ref master_key) = self.master_noise_key else {
            return Err(GridError::Config(
                "Master has no OXS-AUTH v3 key (master_noise.key) configured".into(),
            ));
        };
        if self.ephemeral_without_state_dir
            || (self.members.has_save_path() && self.master_noise_key_path.is_none())
        {
            return Err(GridError::Config(
                "cannot create an invite when the master is running with --ephemeral-key and no --state-dir".into(),
            ));
        }
        if let Some(ref key_path) = self.master_noise_key_path {
            if key_path.is_file() {
                let existing = NodeKeypair::load_file(key_path)?;
                if existing.public_key() != master_key.public_key() {
                    return Err(GridError::Config(format!(
                        "existing master_noise.key at '{}' has a different public key ({}) than the running master ({}); restart the master so it uses the key on disk",
                        key_path.display(),
                        existing.public_key().fingerprint(),
                        master_key.public_key().fingerprint(),
                    )));
                }
            } else {
                master_key.save_file(key_path, false)?;
            }
            if !self.members.has_save_path() {
                if let Some(dir) = key_path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    self.members
                        .set_save_path(Some(dir.join(members::MEMBERS_FILE_NAME)));
                }
            }
        }
        if let Some(ref node_key_path) = self.local_node_key_path {
            let (host_node_kp, _) = NodeKeypair::load_or_generate_file(node_key_path)?;
            self.members
                .ensure_local_host_admin(*host_node_kp.public_key(), "local-host", true)?;
        }
        Ok(*master_key.public_key())
    }
}

pub(super) fn check_v3_client_message_role(
    auth_ctx: &MasterAuthContext,
    public_key: &PublicKey,
    msg: &ClientMessage,
) -> Result<(), (&'static str, String)> {
    let Some(member) = auth_ctx.members.get(public_key) else {
        return Err((
            "Refusing client message from un-enrolled or revoked v3 member",
            "member is no longer enrolled".to_string(),
        ));
    };
    match msg {
        ClientMessage::Hello
        | ClientMessage::ClusterStatus
        | ClientMessage::ListWorkers
        | ClientMessage::ListWorkspaces => {
            if member.has_any_role() {
                Ok(())
            } else {
                Err((
                    "Refusing client connection from v3 member with no roles",
                    "member has no roles".to_string(),
                ))
            }
        }
        ClientMessage::SubmitTask { .. }
        | ClientMessage::GetTaskStatus { .. }
        | ClientMessage::CancelTask { .. }
        | ClientMessage::SubmitMapReduce { .. }
        | ClientMessage::GetTaskResult { .. }
        | ClientMessage::BlobRequest { .. }
        | ClientMessage::MailSend { .. }
        | ClientMessage::MailFetch { .. }
        | ClientMessage::MailAck { .. }
        | ClientMessage::MailWait { .. } => {
            if member.has_client_role() {
                Ok(())
            } else {
                Err((
                    "Refusing client connection from v3 member without the client role",
                    "member does not have the client role".to_string(),
                ))
            }
        }
        ClientMessage::CreateInvite { .. }
        | ClientMessage::ListInvites
        | ClientMessage::RevokeInvite { .. }
        | ClientMessage::ListMembers
        | ClientMessage::ListPendingEnrollments
        | ClientMessage::ApproveEnrollment { .. }
        | ClientMessage::RejectEnrollment { .. }
        | ClientMessage::RevokeMember { .. }
        | ClientMessage::RenameMember { .. }
        | ClientMessage::ReleaseWorkerId { .. } => {
            if member.has_admin_role() {
                Ok(())
            } else {
                Err((
                    "Refusing admin command from v3 member without the admin role",
                    "member does not have the admin role".to_string(),
                ))
            }
        }
        ClientMessage::Enroll { .. } => Err((
            "Refusing ClientMessage::Enroll on a member session",
            "ClientMessage::Enroll requires an enroll handshake".to_string(),
        )),
    }
}

async fn run_v3_auth_from_hello<S>(
    transport: &mut MessageTransport<S>,
    peer: &str,
    lane: &str,
    hello_frame: &[u8],
    auth_ctx: &MasterAuthContext,
    deadline: tokio::time::Instant,
    handshake_timeout: Duration,
) -> GridResult<(InboundMessage, WireCodec, Option<AuthenticatedV3Peer>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let timed_out = || GridError::Timeout {
        operation: "handshake".into(),
        duration_secs: handshake_timeout.as_secs(),
    };
    if !auth_ctx.auth_mode.allows_members() {
        let err = tokio::time::timeout_at(
            deadline,
            reject_peer(
                transport,
                REJECT_AUTH_FAILED,
                "peer attempted OXS-AUTH v3 on a master configured with --auth secret",
            ),
        )
        .await
        .unwrap_or_else(|_| timed_out());
        warn!(remote = %peer, lane, error = %err, "Cluster authentication failed; dropping connection");
        return Err(err);
    }
    let Some(ref master_key) = auth_ctx.master_noise_key else {
        let err = tokio::time::timeout_at(
            deadline,
            reject_peer(
                transport,
                REJECT_AUTH_FAILED,
                "peer attempted OXS-AUTH v3 but master has no master_noise.key configured",
            ),
        )
        .await
        .unwrap_or_else(|_| timed_out());
        warn!(remote = %peer, lane, error = %err, "Cluster authentication failed; dropping connection");
        return Err(err);
    };
    let members = Arc::clone(&auth_ctx.members);
    let pending = match tokio::time::timeout_at(
        deadline,
        v3_server_handshake_from_hello(transport, hello_frame, master_key, |pk| {
            members.contains(pk)
        }),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e @ (GridError::ConnectionClosed | GridError::Io(_)))) => {
            debug!(remote = %peer, lane, error = %e, "Peer disconnected during OXS-AUTH v3 handshake");
            return Err(e);
        }
        Ok(Err(e)) => {
            warn!(remote = %peer, lane, error = %e, "OXS-AUTH v3 handshake failed; dropping connection");
            return Err(e);
        }
        Err(_) => {
            warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "OXS-AUTH v3 handshake timed out; dropping connection");
            return Err(timed_out());
        }
    };

    let (mut auth_peer, first_msg, codec) = match tokio::time::timeout_at(
        deadline,
        v3_server_confirm_first_record::<_, InboundMessage>(transport, pending),
    )
    .await
    {
        Ok(Ok(res)) => res,
        Ok(Err(GridError::IncompatibleVersion(message))) => {
            warn!(
                remote = %peer,
                lane,
                error = %message,
                "Refusing v3 client connection with incompatible protocol version"
            );
            if let Some(detected) = transport.last_detected_codec() {
                transport.set_outbound_codec(detected);
            }
            let err_resp = ClientResponse::Error {
                message: message.clone(),
            };
            let _ =
                tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, transport.send_msg(&err_resp)).await;
            return Err(GridError::IncompatibleVersion(message));
        }
        Ok(Err(e @ (GridError::ConnectionClosed | GridError::Io(_)))) => {
            debug!(remote = %peer, lane, error = %e, "Peer closed connection before first encrypted v3 record");
            return Err(e);
        }
        Ok(Err(e)) => {
            warn!(remote = %peer, lane, error = %e, "First encrypted v3 record failed to decrypt or decode; dropping connection");
            return Err(e);
        }
        Err(_) => {
            warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Handshake watchdog timed out waiting for first v3 record");
            return Err(timed_out());
        }
    };

    auth_peer.remote_addr = peer.to_string();

    debug!(
        remote = %peer,
        lane,
        member_id = %auth_peer.member_id,
        purpose = ?auth_peer.purpose,
        "Peer authenticated with OXS-AUTH v3 (Noise IK)"
    );

    if auth_peer.purpose == HandshakePurpose::Enroll {
        if !matches!(
            first_msg,
            InboundMessage::Client(ClientMessage::Enroll { .. })
        ) {
            let reason = "enroll handshake may only send ClientMessage::Enroll".to_string();
            warn!(
                remote = %peer,
                lane,
                member_id = %auth_peer.member_id,
                "Refusing non-Enroll first message on OXS-AUTH v3 enroll handshake"
            );
            return Err(GridError::AuthFailed(reason));
        }
        return Ok((first_msg, codec, Some(auth_peer)));
    }

    auth_ctx.members.touch_last_seen(&auth_peer.public_key);

    match &first_msg {
        InboundMessage::Worker(WorkerMessage::Register {
            worker_id,
            capabilities,
            protocol_version,
            ..
        }) => {
            let has_worker_role = auth_ctx
                .members
                .get(&auth_peer.public_key)
                .is_some_and(|m| m.has_worker_role());
            if !has_worker_role {
                let reason = "member does not have the worker role".to_string();
                warn!(
                    remote = %peer,
                    lane,
                    member_id = %auth_peer.member_id,
                    worker_id = %worker_id,
                    "Refusing Register from v3 member without the worker role"
                );
                if codec == WireCodec::Json {
                    transport.set_outbound_codec(WireCodec::Json);
                }
                let reject_ack = MasterMessage::RegisterAck {
                    accepted: false,
                    worker_id: *worker_id,
                    heartbeat_interval_secs: 0,
                    protocol_version: PROTOCOL_VERSION_CURRENT,
                    features: ProtocolCapabilities::empty(),
                    message: Some(reason.clone()),
                };
                let _ =
                    tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, transport.send_msg(&reject_ack))
                        .await;
                return Err(GridError::AuthFailed(reason));
            }
            if is_protocol_version_compatible(*protocol_version) && capabilities.cpu_cores > 0 {
                if let Err(err) = auth_ctx
                    .members
                    .check_worker_id_owner(*worker_id, Some(&auth_peer.member_id))
                {
                    let reason = err.to_string();
                    warn!(
                        remote = %peer,
                        lane,
                        member_id = %auth_peer.member_id,
                        worker_id = %worker_id,
                        error = %reason,
                        "Refusing Register from v3 member for worker_id owned by another member"
                    );
                    if codec == WireCodec::Json {
                        transport.set_outbound_codec(WireCodec::Json);
                    }
                    let reject_ack = MasterMessage::RegisterAck {
                        accepted: false,
                        worker_id: *worker_id,
                        heartbeat_interval_secs: 0,
                        protocol_version: PROTOCOL_VERSION_CURRENT,
                        features: ProtocolCapabilities::empty(),
                        message: Some(reason),
                    };
                    let _ = tokio::time::timeout(
                        REFUSAL_DRAIN_TIMEOUT,
                        transport.send_msg(&reject_ack),
                    )
                    .await;
                    return Err(err);
                }
            }
        }
        InboundMessage::Worker(_) => {
            let has_worker_role = auth_ctx
                .members
                .get(&auth_peer.public_key)
                .is_some_and(|m| m.has_worker_role());
            if !has_worker_role {
                let reason = "member does not have the worker role".to_string();
                warn!(
                    remote = %peer,
                    lane,
                    member_id = %auth_peer.member_id,
                    "Refusing worker connection from v3 member without the worker role"
                );
                return Err(GridError::AuthFailed(reason));
            }
        }
        InboundMessage::Client(client_msg) => {
            if let Err((log_msg, wire_reason)) =
                check_v3_client_message_role(auth_ctx, &auth_peer.public_key, client_msg)
            {
                warn!(
                    remote = %peer,
                    lane,
                    member_id = %auth_peer.member_id,
                    "{log_msg}"
                );
                if codec == WireCodec::Json {
                    transport.set_outbound_codec(WireCodec::Json);
                }
                let err_resp = ClientResponse::Error {
                    message: format!("Authentication failed: {wire_reason}"),
                };
                let _ = tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, transport.send_msg(&err_resp))
                    .await;
                return Err(GridError::AuthFailed(wire_reason));
            }
        }
    }

    Ok((first_msg, codec, Some(auth_peer)))
}

/// Authenticates a freshly accepted peer and reads its first protocol message, all inside one
/// `handshake_timeout` window.
///
/// With a `cluster_secret` or enrolled `members`, the OXS-AUTH v2 or v3 handshake runs first and
/// only an authenticated peer gets to send a first `InboundMessage`; until then frames are capped
/// at 1 KiB. Failures are logged with the peer address and the (secret-free) reason, and the
/// connection is dropped, so no task is created and no worker registered.
///
/// Without a secret or required members, the first message is read as before, except that a peer
/// that opens with a v2 authentication HELLO is told `REJECT("server has no secret configured")`
/// instead of being silently dropped, so a misconfigured worker gets an actionable error.
///
/// `lane` only labels the log lines ("connection" for TCP, "control stream" for QUIC).
pub(crate) async fn authenticate_and_read_first_message<S>(
    transport: &mut MessageTransport<S>,
    peer: &str,
    cluster_secret: Option<&ClusterSecret>,
    auth_ctx: &MasterAuthContext,
    handshake_timeout: Duration,
    lane: &str,
) -> GridResult<(InboundMessage, WireCodec, Option<AuthenticatedV3Peer>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + handshake_timeout;
    let timed_out = || GridError::Timeout {
        operation: "handshake".into(),
        duration_secs: handshake_timeout.as_secs(),
    };
    let effective_secret = if auth_ctx.auth_mode.allows_secret() {
        cluster_secret
    } else {
        None
    };
    let requires_auth = effective_secret.is_some()
        || auth_ctx.auth_mode == AuthMode::Members
        || (auth_ctx.auth_mode.allows_members() && auth_ctx.members.has_non_host_members());

    if requires_auth {
        let hello_frame = match tokio::time::timeout_at(deadline, recv_auth_frame(transport)).await
        {
            Ok(Ok(Some(frame))) => frame,
            Ok(Ok(None)) => {
                debug!(remote = %peer, lane, "Peer disconnected during cluster authentication");
                return Err(GridError::ConnectionClosed);
            }
            Ok(Err(ProtocolError::FrameTooLarge { max, .. })) => {
                let err = tokio::time::timeout_at(
                    deadline,
                    reject_peer(
                        transport,
                        REJECT_AUTH_REQUIRED,
                        format!("first frame exceeds the {max} byte pre-authentication limit"),
                    ),
                )
                .await
                .unwrap_or_else(|_| timed_out());
                warn!(remote = %peer, lane, error = %err, "Cluster authentication failed; dropping connection");
                return Err(err);
            }
            Ok(Err(e)) => {
                let err: GridError = e.into();
                if matches!(err, GridError::ConnectionClosed | GridError::Io(_)) {
                    debug!(remote = %peer, lane, error = %err, "Peer disconnected during cluster authentication");
                } else {
                    warn!(remote = %peer, lane, error = %err, "Cluster authentication failed; dropping connection");
                }
                return Err(err);
            }
            Err(_) => {
                warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Cluster authentication timed out; dropping connection");
                return Err(timed_out());
            }
        };

        match peek_hello_version(&hello_frame) {
            Some(AUTH_VERSION_V3) => {
                return run_v3_auth_from_hello(
                    transport,
                    peer,
                    lane,
                    &hello_frame,
                    auth_ctx,
                    deadline,
                    handshake_timeout,
                )
                .await;
            }
            Some(AUTH_VERSION) if auth_ctx.auth_mode == AuthMode::Members => {
                let err = tokio::time::timeout_at(
                    deadline,
                    reject_peer(
                        transport,
                        REJECT_MEMBERS_ONLY,
                        "peer attempted OXS-AUTH v2 on a master configured with --auth members",
                    ),
                )
                .await
                .unwrap_or_else(|_| timed_out());
                warn!(remote = %peer, lane, error = %err, "Cluster authentication failed; dropping connection");
                return Err(err);
            }
            Some(AUTH_VERSION) if effective_secret.is_none() => {
                warn!(remote = %peer, lane, "Peer attempted cluster authentication but this master has no cluster secret configured; rejecting");
                let _ = tokio::time::timeout(
                    REFUSAL_DRAIN_TIMEOUT,
                    send_reject(transport, REJECT_NO_SECRET),
                )
                .await;
                return Err(GridError::AuthFailed(
                    "peer requested cluster authentication but this master has no secret configured"
                        .into(),
                ));
            }
            _ => {
                if let Some(secret) = effective_secret {
                    match tokio::time::timeout_at(
                        deadline,
                        server_handshake_from_hello(transport, &hello_frame, secret),
                    )
                    .await
                    {
                        Ok(Ok(session_key)) => {
                            debug!(remote = %peer, lane, "Peer authenticated with the cluster secret");
                            transport.enable_encryption(&session_key, EndpointRole::Server);
                        }
                        Ok(Err(e @ (GridError::ConnectionClosed | GridError::Io(_)))) => {
                            debug!(remote = %peer, lane, error = %e, "Peer disconnected during cluster authentication");
                            return Err(e);
                        }
                        Ok(Err(e)) => {
                            warn!(remote = %peer, lane, error = %e, "Cluster authentication failed; dropping connection");
                            return Err(e);
                        }
                        Err(_) => {
                            warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Cluster authentication timed out; dropping connection");
                            return Err(timed_out());
                        }
                    }
                } else {
                    let err = tokio::time::timeout_at(
                        deadline,
                        reject_peer(
                            transport,
                            REJECT_AUTH_REQUIRED,
                            "first frame was not an authentication HELLO",
                        ),
                    )
                    .await
                    .unwrap_or_else(|_| timed_out());
                    warn!(remote = %peer, lane, error = %err, "Cluster authentication failed; dropping connection");
                    return Err(err);
                }
            }
        }

        match tokio::time::timeout_at(deadline, transport.recv_msg_with_codec::<InboundMessage>())
            .await
        {
            Ok(Ok(Some((first, codec)))) => Ok((first, codec, None)),
            Ok(Ok(None)) => {
                debug!(remote = %peer, lane, "Peer closed connection before handshake");
                Err(GridError::ConnectionClosed)
            }
            Ok(Err(ProtocolError::IncompatibleVersion { peer_version, .. })) => {
                let message = format_incompatible_version_error("client", peer_version);
                warn!(
                    remote = %peer,
                    lane,
                    peer_version,
                    error = %message,
                    "Refusing client connection with incompatible protocol version"
                );
                if let Some(detected) = transport.last_detected_codec() {
                    transport.set_outbound_codec(detected);
                }
                let err_resp = ClientResponse::Error {
                    message: message.clone(),
                };
                let _ = tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, transport.send_msg(&err_resp))
                    .await;
                Err(GridError::IncompatibleVersion(message))
            }
            Ok(Err(e)) => {
                warn!(remote = %peer, lane, error = %e, "Protocol error during handshake");
                Err(e.into())
            }
            Err(_) => {
                warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Handshake watchdog timed out");
                Err(timed_out())
            }
        }
    } else {
        let raw_frame = match tokio::time::timeout_at(deadline, transport.recv_raw_frame()).await {
            Ok(Ok(Some(frame))) => frame,
            Ok(Ok(None)) => {
                debug!(remote = %peer, lane, "Peer closed connection before handshake");
                return Err(GridError::ConnectionClosed);
            }
            Ok(Err(e)) => {
                warn!(remote = %peer, lane, error = %e, "Protocol error during handshake");
                return Err(e.into());
            }
            Err(_) => {
                warn!(remote = %peer, lane, timeout_secs = handshake_timeout.as_secs(), "Handshake watchdog timed out");
                return Err(timed_out());
            }
        };

        if raw_frame.first() == Some(&WIRE_FORMAT_AUTH) {
            if peek_hello_version(&raw_frame) == Some(AUTH_VERSION_V3) {
                return run_v3_auth_from_hello(
                    transport,
                    peer,
                    lane,
                    &raw_frame,
                    auth_ctx,
                    deadline,
                    handshake_timeout,
                )
                .await;
            }
            warn!(remote = %peer, lane, "Peer attempted cluster authentication but this master has no cluster secret configured; rejecting");
            let _ = tokio::time::timeout(
                REFUSAL_DRAIN_TIMEOUT,
                send_reject(transport, REJECT_NO_SECRET),
            )
            .await;
            return Err(GridError::AuthFailed(
                "peer requested cluster authentication but this master has no secret configured"
                    .into(),
            ));
        }

        match rusty_grid_core::protocol::deserialize_message_with_version::<InboundMessage>(
            &raw_frame,
        ) {
            Ok((first, codec, version)) => {
                if let Some(version) = version {
                    transport.note_peer_version(version);
                }
                Ok((first, codec, None))
            }
            Err(ProtocolError::IncompatibleVersion { peer_version, .. }) => {
                let message = format_incompatible_version_error("client", peer_version);
                warn!(
                    remote = %peer,
                    lane,
                    peer_version,
                    error = %message,
                    "Refusing client connection with incompatible protocol version"
                );
                if matches!(
                    raw_frame.first().copied(),
                    Some(WIRE_FORMAT_JSON | b'{' | b' ' | b'\t' | b'\r' | b'\n')
                ) {
                    transport.set_outbound_codec(WireCodec::Json);
                }
                let err_resp = ClientResponse::Error {
                    message: message.clone(),
                };
                let _ = tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, transport.send_msg(&err_resp))
                    .await;
                Err(GridError::IncompatibleVersion(message))
            }
            Err(e) => {
                warn!(remote = %peer, lane, error = %e, "Protocol error during handshake");
                Err(e.into())
            }
        }
    }
}

/// Aborts a spawned helper task when the connection handler that owns it ends, including when the
/// handler itself is aborted by the reaper or by the worker re-registering.
pub(crate) struct AbortOnDrop(pub(crate) tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Owns the QUIC side of a registered P2P worker: closes the connection when the handler ends and
/// unlists it from the active-connection map.
///
/// Aborting the handler task (the reaper, `unregister`, a re-registration) only drops its own
/// clone of the connection; the map holds another, so without an explicit close the worker would
/// stay connected to a master that has long written it off and never get the chance to reconnect.
#[cfg(feature = "p2p")]
pub(crate) struct P2pConnGuard {
    conn: iroh::endpoint::Connection,
    conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    worker_id: Uuid,
}

#[cfg(feature = "p2p")]
impl P2pConnGuard {
    pub(crate) fn new(
        conn: iroh::endpoint::Connection,
        conns: Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
        worker_id: Uuid,
    ) -> Self {
        Self {
            conn,
            conns,
            worker_id,
        }
    }

    /// Removes this guard's connection from the map, unless the same worker has since
    /// re-registered over a newer connection that took the slot.
    pub(crate) async fn unlist(&self) {
        let mut guard = self.conns.write().await;
        if guard
            .get(&self.worker_id)
            .is_some_and(|c| c.stable_id() == self.conn.stable_id())
        {
            guard.remove(&self.worker_id);
        }
    }
}

#[cfg(feature = "p2p")]
impl Drop for P2pConnGuard {
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"session ended by master");
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let conns = Arc::clone(&self.conns);
            let worker_id = self.worker_id;
            let stable_id = self.conn.stable_id();
            runtime.spawn(async move {
                let mut guard = conns.write().await;
                if guard
                    .get(&worker_id)
                    .is_some_and(|c| c.stable_id() == stable_id)
                {
                    guard.remove(&worker_id);
                }
            });
        }
    }
}

pub(crate) fn reject_master_role_claim(
    ticket: &rusty_grid_core::arbitrage::MasterLeaseTicket,
) -> MasterMessage {
    warn!(
        claimant_master_id = %ticket.master_id,
        authority_score = ticket.authority_score,
        "Refusing ClaimMasterRole: automatic master failover is not supported"
    );
    MasterMessage::RoleHandoverAck {
        accepted: false,
        reason: Some(
            "Automatic master failover is not supported; use manual role switching".into(),
        ),
        task_queue_snapshot: Vec::new(),
        connected_workers: Vec::new(),
    }
}

/// Maximum time the master spends writing a handshake refusal, finishing the control send stream,
/// and waiting for the peer to close the QUIC connection before dropping it (`BUG-17`).
pub(super) const REFUSAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

async fn finish_and_wait_for_peer_close<T: AsyncRead + AsyncWrite + Unpin>(
    transport: &mut MessageTransport<T>,
    #[cfg(feature = "p2p")] p2p_conn: Option<&iroh::endpoint::Connection>,
) {
    let _ = tokio::io::AsyncWriteExt::shutdown(transport.get_mut()).await;
    #[cfg(feature = "p2p")]
    if let Some(conn) = p2p_conn {
        conn.closed().await;
    }
}

/// Finishes the control send stream after an authentication `REJECT` frame has been written and
/// waits up to [`REFUSAL_DRAIN_TIMEOUT`] for the peer to close the QUIC connection (`BUG-17`).
pub(crate) async fn drain_refusal_stream<T: AsyncRead + AsyncWrite + Unpin>(
    transport: &mut MessageTransport<T>,
    #[cfg(feature = "p2p")] p2p_conn: Option<&iroh::endpoint::Connection>,
) {
    let _ = tokio::time::timeout(
        REFUSAL_DRAIN_TIMEOUT,
        finish_and_wait_for_peer_close(
            transport,
            #[cfg(feature = "p2p")]
            p2p_conn,
        ),
    )
    .await;
}

/// Sends a handshake refusal message (`RegisterAck { accepted: false, .. }` or
/// `RoleHandoverAck { accepted: false, .. }`), finishes the control send stream, and waits up to
/// [`REFUSAL_DRAIN_TIMEOUT`] (`1 s`) in total for the peer to close before dropping the QUIC
/// connection handle (`BUG-17`).
pub(crate) async fn send_refusal_and_drain<T: AsyncRead + AsyncWrite + Unpin>(
    transport: &mut MessageTransport<T>,
    msg: &MasterMessage,
    #[cfg(feature = "p2p")] p2p_conn: Option<&iroh::endpoint::Connection>,
) {
    let _ = tokio::time::timeout(REFUSAL_DRAIN_TIMEOUT, async {
        if transport.send_msg(msg).await.is_ok() {
            finish_and_wait_for_peer_close(
                transport,
                #[cfg(feature = "p2p")]
                p2p_conn,
            )
            .await;
        }
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: S,
    remote_addr: SocketAddr,
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
    mapreduce_parallelism: usize,
    blob_store: Option<Arc<BlobStore>>,
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let pre_auth = crate::session::PreAuthSession::new_single(
        stream,
        remote_addr,
        wire_codec,
        mapreduce_parallelism,
        #[cfg(feature = "p2p")]
        p2p_conn,
        #[cfg(feature = "p2p")]
        p2p_conns,
    );
    crate::session::WorkerSession::run_connection(
        pre_auth,
        registry,
        queue,
        scheduler_notify,
        waiters,
        heartbeat_interval_secs,
        handshake_timeout,
        shutdown_rx,
        abort_handle,
        wire_codec,
        cluster_secret,
        auth_ctx,
        handshake_permit,
        blob_store,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "p2p"), allow(dead_code))]
pub(super) async fn handle_multiplexed_connection<
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
>(
    ctrl_stream: C,
    data_stream: D,
    remote_addr: SocketAddr,
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
    #[cfg(feature = "dashboard")] broadcast_tx: Option<
        tokio::sync::broadcast::Sender<crate::dashboard::dto::DashboardStreamMessage>,
    >,
    #[cfg(feature = "p2p")] p2p_conn: Option<iroh::endpoint::Connection>,
    #[cfg(feature = "p2p")] p2p_conns: Option<
        Arc<tokio::sync::RwLock<HashMap<Uuid, iroh::endpoint::Connection>>>,
    >,
) -> GridResult<()> {
    let pre_auth = crate::session::PreAuthSession::new_multiplexed(
        ctrl_stream,
        data_stream,
        remote_addr,
        wire_codec,
        #[cfg(feature = "p2p")]
        p2p_conn,
        #[cfg(feature = "p2p")]
        p2p_conns,
    );
    crate::session::WorkerSession::run_connection(
        pre_auth,
        registry,
        queue,
        scheduler_notify,
        waiters,
        heartbeat_interval_secs,
        handshake_timeout,
        shutdown_rx,
        abort_handle,
        wire_codec,
        cluster_secret,
        auth_ctx,
        handshake_permit,
        blob_store,
        #[cfg(feature = "dashboard")]
        broadcast_tx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn spawn_master_rendezvous_registration(
    rendezvous_url: String,
    cluster_addr: String,
    web_ui_url: String,
    hostname: String,
    p2p_ticket: Option<String>,
    registry: WorkerRegistry,
    cluster_secret: Option<ClusterSecret>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    if let Err(e) = rusty_grid_core::discovery::parse_rendezvous_url(&rendezvous_url) {
        tracing::error!(
            url = %rendezvous_url,
            error = %e,
            "Invalid Master rendezvous URL; disabling rendezvous registration"
        );
        return;
    }

    let cluster_addr = if let Ok(parsed) = cluster_addr.parse::<std::net::SocketAddr>() {
        if parsed.ip().is_unspecified() {
            let local_ip = rusty_grid_core::discovery::get_local_ip_or_loopback();
            format!("{}:{}", local_ip, parsed.port())
        } else {
            cluster_addr
        }
    } else {
        cluster_addr
    };

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let workers_count = registry.list_workers().await.len();
        let mut lease = rusty_grid_core::discovery::RendezvousLease::new(
            &cluster_addr,
            &web_ui_url,
            &hostname,
            p2p_ticket.clone(),
            workers_count,
            120,
        );
        if let Some(ref secret) = cluster_secret {
            lease.sign(secret);
        }
        let retry_delay = match rusty_grid_core::discovery::register_rendezvous_lease(
            &rendezvous_url,
            &lease,
        )
        .await
        {
            Ok(()) => {
                debug!(url = %rendezvous_url, "Successfully renewed Master lease in rendezvous registry");
                Duration::from_secs(30)
            }
            Err(e) => {
                warn!(url = %rendezvous_url, error = %e, "Failed to register Master lease with rendezvous registry");
                Duration::from_secs(2)
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(retry_delay) => {}
            res = shutdown_rx.changed() => {
                if (res.is_ok() && *shutdown_rx.borrow()) || res.is_err() {
                    break;
                }
            }
        }
    }
}
