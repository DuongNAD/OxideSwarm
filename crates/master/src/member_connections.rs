//! Live connections of each enrolled member, so that revoking a member closes every lane it has
//! at once (design 0051 D5, revocation step 1).
//!
//! Every connection that authenticated with OXS-AUTH v3 as a member registers here, right after
//! its handshake and before it handles its first message, with the member id and a
//! [`CancellationToken`]:
//!
//! - worker sessions over TCP, and over iroh QUIC, where the `STREAM_CONTROL` and `STREAM_DATA`
//!   lanes belong to one connection and share one token;
//! - client connections over TCP and over a P2P `STREAM_CLIENT` stream. `BlobConnection`s and MCP
//!   clients are client connections of this kind.
//!
//! The connection handler selects on its token next to its own work and ends, dropping its
//! streams, as soon as the token is cancelled. The [`MemberConnectionGuard`] it holds unregisters
//! the connection when the handler ends for any reason.
//!
//! No connection slips between a revocation and its teardown: revocation first commits the
//! member's removal and then calls [`MemberConnections::close_member`], while a connection first
//! registers and then checks that its key is still a member. Both steps take the registry lock, so
//! a connection registered after the close sees the removal, and one registered before it is
//! cancelled.
//!
//! Cancellation is cooperative: a cancelled handler stops at its next await, not at once, and may
//! finish the step it is in. A worker registration is therefore guarded on its own: the worker-id
//! claim refuses a key that is no longer a member, under the member store lock that orders it
//! against the revocation (`MemberStore::claim_worker_id_evicting_deferred`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio_util::sync::CancellationToken;

/// The kind of connection a member holds, for logs and for the answer to `members revoke`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MemberLane {
    /// A worker session over one TCP stream.
    WorkerTcp,
    /// A worker session over iroh QUIC: the control and data lanes of one connection.
    WorkerQuic,
    /// A client connection over TCP: CLI commands, `BlobConnection`, MCP.
    ClientTcp,
    /// A client connection over an iroh `STREAM_CLIENT` stream.
    ClientP2p,
}

impl MemberLane {
    /// Classifies a connection by its transport and its first message.
    pub(crate) const fn classify(is_worker: bool, over_quic: bool) -> Self {
        match (is_worker, over_quic) {
            (true, false) => Self::WorkerTcp,
            (true, true) => Self::WorkerQuic,
            (false, false) => Self::ClientTcp,
            (false, true) => Self::ClientP2p,
        }
    }

    /// Short label used in log lines.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::WorkerTcp => "worker (TCP)",
            Self::WorkerQuic => "worker (QUIC control and data lanes)",
            Self::ClientTcp => "client (TCP)",
            Self::ClientP2p => "client (P2P STREAM_CLIENT)",
        }
    }

    /// Stable machine-readable lane identifier for HTTP/JSON APIs.
    #[allow(dead_code)]
    pub(crate) const fn api_name(self) -> &'static str {
        match self {
            Self::WorkerTcp => "worker_tcp",
            Self::WorkerQuic => "worker_quic",
            Self::ClientTcp => "client_tcp",
            Self::ClientP2p => "client_p2p",
        }
    }

    /// Returns `true` for a worker session.
    pub(crate) const fn is_worker(self) -> bool {
        matches!(self, Self::WorkerTcp | Self::WorkerQuic)
    }
}

/// Identifies one registered connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ConnectionId(u64);

impl ConnectionId {
    /// Numeric identifier of this connection registration.
    #[allow(dead_code)]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Read-only snapshot of an active member connection (never exposes `CancellationToken`).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ActiveMemberConnection {
    pub connection_id: u64,
    pub member_id: String,
    pub lane: &'static str,
}

struct Entry {
    lane: MemberLane,
    token: CancellationToken,
}

#[derive(Default)]
struct Inner {
    next_id: u64,
    by_member: HashMap<String, HashMap<ConnectionId, Entry>>,
}

/// Registry of the live connections of every enrolled member (see the module docs).
#[derive(Clone, Default)]
pub(crate) struct MemberConnections {
    inner: Arc<Mutex<Inner>>,
}

impl MemberConnections {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers a connection of `member_id` on `lane`. The returned guard unregisters it on drop.
    pub(crate) fn register(&self, member_id: &str, lane: MemberLane) -> MemberConnectionGuard {
        let token = CancellationToken::new();
        let id = {
            let mut inner = self.lock();
            let id = ConnectionId(inner.next_id);
            inner.next_id = inner.next_id.wrapping_add(1);
            inner
                .by_member
                .entry(member_id.to_string())
                .or_default()
                .insert(
                    id,
                    Entry {
                        lane,
                        token: token.clone(),
                    },
                );
            id
        };
        MemberConnectionGuard {
            registry: self.clone(),
            member_id: member_id.to_string(),
            id,
            lane,
            token,
        }
    }

    /// Closes every registered connection of `member_id` at once, except `keep` (the connection
    /// that asked for the revocation, closed by its handler once the answer is sent). Returns the
    /// lanes of the connections it closed.
    pub(crate) fn close_member(
        &self,
        member_id: &str,
        keep: Option<ConnectionId>,
    ) -> Vec<MemberLane> {
        let closed: Vec<Entry> = {
            let mut inner = self.lock();
            let Some(conns) = inner.by_member.get_mut(member_id) else {
                return Vec::new();
            };
            let ids: Vec<ConnectionId> = conns
                .keys()
                .copied()
                .filter(|id| Some(*id) != keep)
                .collect();
            let closed = ids.iter().filter_map(|id| conns.remove(id)).collect();
            if conns.is_empty() {
                inner.by_member.remove(member_id);
            }
            closed
        };
        closed
            .into_iter()
            .map(|entry| {
                entry.token.cancel();
                entry.lane
            })
            .collect()
    }

    /// Closes one connection of `member_id`. Returns `false` if it was not registered.
    pub(crate) fn close_connection(&self, member_id: &str, id: ConnectionId) -> bool {
        let entry = {
            let mut inner = self.lock();
            let Some(conns) = inner.by_member.get_mut(member_id) else {
                return false;
            };
            let entry = conns.remove(&id);
            if conns.is_empty() {
                inner.by_member.remove(member_id);
            }
            entry
        };
        match entry {
            Some(entry) => {
                entry.token.cancel();
                true
            }
            None => false,
        }
    }

    /// Returns `true` if `id` is a registered connection of `member_id`.
    pub(crate) fn contains(&self, member_id: &str, id: ConnectionId) -> bool {
        self.lock()
            .by_member
            .get(member_id)
            .is_some_and(|conns| conns.contains_key(&id))
    }

    /// Lists all active registered member connections sorted deterministically by
    /// `(member_id, connection_id)`.
    #[allow(dead_code)]
    pub(crate) fn list_active(&self) -> Vec<ActiveMemberConnection> {
        let inner = self.lock();
        let mut active = Vec::new();
        for (member_id, conns) in &inner.by_member {
            for (&id, entry) in conns {
                active.push(ActiveMemberConnection {
                    connection_id: id.as_u64(),
                    member_id: member_id.clone(),
                    lane: entry.lane.api_name(),
                });
            }
        }
        active.sort_by(|a, b| {
            a.member_id
                .cmp(&b.member_id)
                .then_with(|| a.connection_id.cmp(&b.connection_id))
        });
        active
    }

    /// Number of registered connections of `member_id`.
    #[cfg(test)]
    pub(crate) fn count(&self, member_id: &str) -> usize {
        self.lock().by_member.get(member_id).map_or(0, HashMap::len)
    }

    fn unregister(&self, member_id: &str, id: ConnectionId) {
        let mut inner = self.lock();
        if let Some(conns) = inner.by_member.get_mut(member_id) {
            conns.remove(&id);
            if conns.is_empty() {
                inner.by_member.remove(member_id);
            }
        }
    }
}

/// A registered member connection. Dropping it unregisters the connection.
pub(crate) struct MemberConnectionGuard {
    registry: MemberConnections,
    member_id: String,
    id: ConnectionId,
    lane: MemberLane,
    token: CancellationToken,
}

impl MemberConnectionGuard {
    /// This connection's id in the registry.
    pub(crate) fn id(&self) -> ConnectionId {
        self.id
    }

    /// The member this connection authenticated as.
    pub(crate) fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The kind of connection.
    pub(crate) fn lane(&self) -> MemberLane {
        self.lane
    }

    /// Resolves once the connection has been closed through the registry.
    pub(crate) async fn closed(&self) {
        self.token.cancelled().await;
    }

    /// Returns `true` once the connection has been closed through the registry.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.token.is_cancelled()
    }
}

impl Drop for MemberConnectionGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.member_id, self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_counts_and_drop_unregisters() {
        let reg = MemberConnections::default();
        let a = reg.register("member-a", MemberLane::WorkerTcp);
        let b = reg.register("member-a", MemberLane::ClientTcp);
        assert_ne!(a.id(), b.id());
        assert_eq!(reg.count("member-a"), 2);
        assert!(reg.contains("member-a", a.id()));
        assert_eq!(a.member_id(), "member-a");
        assert_eq!(b.lane(), MemberLane::ClientTcp);

        let a_id = a.id();
        drop(a);
        assert_eq!(reg.count("member-a"), 1);
        assert!(!reg.contains("member-a", a_id));
        drop(b);
        assert_eq!(reg.count("member-a"), 0);
        assert!(
            reg.lock().by_member.is_empty(),
            "no empty member entry left"
        );
    }

    #[tokio::test]
    async fn close_member_cancels_every_lane_and_leaves_other_members_alone() {
        let reg = MemberConnections::default();
        let worker_tcp = reg.register("revoked", MemberLane::WorkerTcp);
        let worker_quic = reg.register("revoked", MemberLane::WorkerQuic);
        let client_tcp = reg.register("revoked", MemberLane::ClientTcp);
        let client_p2p = reg.register("revoked", MemberLane::ClientP2p);
        let other = reg.register("kept", MemberLane::ClientTcp);

        let mut closed = reg.close_member("revoked", None);
        closed.sort_by_key(|lane| lane.as_str());
        let mut expected = vec![
            MemberLane::WorkerTcp,
            MemberLane::WorkerQuic,
            MemberLane::ClientTcp,
            MemberLane::ClientP2p,
        ];
        expected.sort_by_key(|lane| lane.as_str());
        assert_eq!(closed, expected);

        for guard in [&worker_tcp, &worker_quic, &client_tcp, &client_p2p] {
            assert!(
                guard.is_closed(),
                "{} must be closed",
                guard.lane().as_str()
            );
            // Resolves at once for a closed connection.
            guard.closed().await;
        }
        assert_eq!(reg.count("revoked"), 0);
        assert!(!other.is_closed(), "another member's connection stays open");
        assert_eq!(reg.count("kept"), 1);

        // Dropping a guard that was already closed through the registry is harmless.
        drop(worker_tcp);
        assert_eq!(reg.count("kept"), 1);
        assert!(reg.close_member("revoked", None).is_empty());
    }

    #[test]
    fn close_member_keeps_the_requesting_connection_until_closed_explicitly() {
        let reg = MemberConnections::default();
        let own = reg.register("self-revoking", MemberLane::ClientTcp);
        let worker = reg.register("self-revoking", MemberLane::WorkerTcp);

        let closed = reg.close_member("self-revoking", Some(own.id()));
        assert_eq!(closed, vec![MemberLane::WorkerTcp]);
        assert!(worker.is_closed());
        assert!(!own.is_closed());
        assert!(reg.contains("self-revoking", own.id()));

        assert!(reg.close_connection("self-revoking", own.id()));
        assert!(own.is_closed());
        assert!(!reg.close_connection("self-revoking", own.id()));
        assert_eq!(reg.count("self-revoking"), 0);
    }

    #[test]
    fn a_connection_registered_after_close_member_is_not_closed() {
        // Revocation removes the member before it closes the registry; a connection registered
        // afterwards is refused by its own membership check, not by the registry.
        let reg = MemberConnections::default();
        assert!(reg.close_member("late", None).is_empty());
        let late = reg.register("late", MemberLane::ClientP2p);
        assert!(!late.is_closed());
        assert_eq!(reg.count("late"), 1);
    }

    #[test]
    fn classify_maps_transport_and_role() {
        assert_eq!(MemberLane::classify(true, false), MemberLane::WorkerTcp);
        assert_eq!(MemberLane::classify(true, true), MemberLane::WorkerQuic);
        assert_eq!(MemberLane::classify(false, false), MemberLane::ClientTcp);
        assert_eq!(MemberLane::classify(false, true), MemberLane::ClientP2p);
        assert!(MemberLane::WorkerQuic.is_worker());
        assert!(!MemberLane::ClientP2p.is_worker());
        assert_eq!(MemberLane::WorkerTcp.api_name(), "worker_tcp");
        assert_eq!(MemberLane::WorkerQuic.api_name(), "worker_quic");
        assert_eq!(MemberLane::ClientTcp.api_name(), "client_tcp");
        assert_eq!(MemberLane::ClientP2p.api_name(), "client_p2p");
    }

    #[test]
    fn list_active_returns_sorted_snapshots_and_clears_on_drop() {
        let reg = MemberConnections::default();
        let b1 = reg.register("member-b", MemberLane::ClientP2p);
        let a1 = reg.register("member-a", MemberLane::WorkerTcp);
        let a2 = reg.register("member-a", MemberLane::ClientTcp);
        assert_eq!(
            reg.list_active(),
            vec![
                ActiveMemberConnection {
                    connection_id: a1.id().as_u64(),
                    member_id: "member-a".to_string(),
                    lane: "worker_tcp",
                },
                ActiveMemberConnection {
                    connection_id: a2.id().as_u64(),
                    member_id: "member-a".to_string(),
                    lane: "client_tcp",
                },
                ActiveMemberConnection {
                    connection_id: b1.id().as_u64(),
                    member_id: "member-b".to_string(),
                    lane: "client_p2p",
                },
            ]
        );
        drop(a1);
        drop(a2);
        drop(b1);
        assert!(reg.list_active().is_empty());
    }
}
