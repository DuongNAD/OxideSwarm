//! Master-side durable grid mailbox with bounded retention and long-poll notifications
//! (`TEAM-6`, `docs/design/0111-team-plan-leases-and-verify-gate.md` D8).
//!
//! Locking discipline: `self.state` (`std::sync::Mutex`) is NEVER held across an `.await`.
//! Journal durability acks and `BlobStore::unpin_mail` calls always run after dropping the lock.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusty_grid_core::blob::BlobId;
use rusty_grid_core::error::GridResult;
use rusty_grid_core::mail::{
    mail_direct_recipients, mail_matches_consumer, mail_message_bytes, validate_mail_consumer,
    validate_mail_send, MailAttachment, MailMessage, MAX_MAIL_WAIT_TIMEOUT_SECS,
};
use tokio::sync::Notify;
use tracing::warn;

use crate::blob_store::BlobStore;
use crate::mail_store::{MailJournalRecord, MailSnapshotImage, MailStore};

/// Default maximum number of messages retained in the master mailbox.
pub const DEFAULT_MAX_MAIL_COUNT: usize = 4_096;
/// Default maximum retained mailbox footprint in bytes (16 MiB).
pub const DEFAULT_MAX_MAIL_BYTES: usize = 16 * 1024 * 1024;
/// Compact the mail journal automatically after this many uncompacted records.
const AUTO_COMPACT_RECORDS: usize = 512;

struct MailboxState {
    next_id: u64,
    messages: VecDeque<MailMessage>,
    cursors: HashMap<String, u64>,
    total_bytes: usize,
    journal_records_since_compact: usize,
    max_count: usize,
    max_bytes: usize,
    store: Option<MailStore>,
}

impl MailboxState {
    fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            next_id: 1,
            messages: VecDeque::new(),
            cursors: HashMap::new(),
            total_bytes: 0,
            journal_records_since_compact: 0,
            max_count: max_count.max(1),
            max_bytes: max_bytes.max(1),
            store: None,
        }
    }

    fn is_acknowledged_for_eviction(&self, msg: &MailMessage) -> bool {
        let mut has_direct = false;
        for recipient in mail_direct_recipients(msg) {
            has_direct = true;
            if self.cursors.get(recipient).copied().unwrap_or(0) < msg.id {
                return false;
            }
        }
        if has_direct {
            return true;
        }
        // Topic-only messages count as acknowledged once at least one matching consumer has acked
        // at or past `msg.id` (still subject to capacity eviction in Pass 2 if unconsumed).
        self.cursors.values().any(|&c| c >= msg.id)
    }

    fn is_all_direct_acked(&self, msg: &MailMessage) -> bool {
        let mut has_direct = false;
        for recipient in mail_direct_recipients(msg) {
            has_direct = true;
            if self.cursors.get(recipient).copied().unwrap_or(0) < msg.id {
                return false;
            }
        }
        has_direct
    }

    fn evict_over_budget(&mut self, protect_id: u64) -> Vec<u64> {
        let mut evicted = Vec::new();
        // Pass 1: evict oldest acknowledged messages first while over count or byte cap.
        loop {
            let over = self.messages.len() > self.max_count || self.total_bytes > self.max_bytes;
            if !over {
                break;
            }
            let pos = self
                .messages
                .iter()
                .position(|m| m.id != protect_id && self.is_acknowledged_for_eviction(m));
            let Some(idx) = pos else {
                break;
            };
            if let Some(removed) = self.messages.remove(idx) {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(mail_message_bytes(&removed));
                evicted.push(removed.id);
            }
        }
        // Pass 2: if still over capacity, evict oldest remaining (protecting just-sent if alone).
        while self.messages.len() > self.max_count || self.total_bytes > self.max_bytes {
            let pos = self.messages.iter().position(|m| m.id != protect_id);
            let Some(idx) = pos else {
                break;
            };
            if let Some(removed) = self.messages.remove(idx) {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(mail_message_bytes(&removed));
                evicted.push(removed.id);
            }
        }
        evicted
    }

    fn snapshot_image(&self) -> MailSnapshotImage {
        let cursors: BTreeMap<String, u64> =
            self.cursors.iter().map(|(k, v)| (k.clone(), *v)).collect();
        MailSnapshotImage {
            next_id: self.next_id,
            messages: self.messages.iter().cloned().collect(),
            cursors,
        }
    }

    fn apply_recovered_record(&mut self, rec: MailJournalRecord) {
        match rec {
            MailJournalRecord::Send { message } => {
                self.next_id = self.next_id.max(message.id.saturating_add(1));
                self.total_bytes = self
                    .total_bytes
                    .saturating_add(mail_message_bytes(&message));
                self.messages.push_back(message);
                self.journal_records_since_compact += 1;
            }
            MailJournalRecord::Ack { consumer, up_to_id } => {
                let entry = self.cursors.entry(consumer).or_insert(0);
                *entry = (*entry).max(up_to_id);
                self.journal_records_since_compact += 1;
            }
            MailJournalRecord::Evict { ids } => {
                if !ids.is_empty() {
                    let evicted: HashSet<u64> = ids.into_iter().collect();
                    let mut retained = VecDeque::with_capacity(self.messages.len());
                    let mut bytes = 0usize;
                    for m in self.messages.drain(..) {
                        if !evicted.contains(&m.id) {
                            bytes = bytes.saturating_add(mail_message_bytes(&m));
                            retained.push_back(m);
                        }
                    }
                    self.messages = retained;
                    self.total_bytes = bytes;
                }
                self.journal_records_since_compact += 1;
            }
        }
    }
}

/// Thread-safe durable grid mailbox owned by the Master server.
pub struct Mailbox {
    state: Mutex<MailboxState>,
    notify: Arc<Notify>,
    blob_store: Option<Arc<BlobStore>>,
}

impl Mailbox {
    /// Creates an in-memory mailbox without disk persistence or blob pinning (for unit tests).
    pub fn in_memory_default() -> Self {
        Self {
            state: Mutex::new(MailboxState::new(
                DEFAULT_MAX_MAIL_COUNT,
                DEFAULT_MAX_MAIL_BYTES,
            )),
            notify: Arc::new(Notify::new()),
            blob_store: None,
        }
    }

    /// Opens the mailbox, replaying persisted snapshot + journal from `state_dir` (when set) and
    /// restoring attachment pins on `blob_store`.
    pub fn open(
        state_dir: Option<&Path>,
        blob_store: Option<Arc<BlobStore>>,
        max_count: usize,
        max_bytes: usize,
    ) -> GridResult<Self> {
        let mut state = MailboxState::new(max_count, max_bytes);
        if let Some(dir) = state_dir {
            let (s, recovered) = MailStore::open(dir)?;
            if let Some(snap) = recovered.snapshot {
                state.next_id = snap.next_id.max(1);
                for (k, v) in snap.cursors {
                    state.cursors.insert(k, v);
                }
                for m in snap.messages {
                    state.next_id = state.next_id.max(m.id.saturating_add(1));
                    state.total_bytes = state.total_bytes.saturating_add(mail_message_bytes(&m));
                    state.messages.push_back(m);
                }
            }
            for rec in recovered.records {
                state.apply_recovered_record(rec);
            }
            state.store = Some(s);
        }

        if let Some(ref bstore) = blob_store {
            for msg in &state.messages {
                if msg.attachments.is_empty() || state.is_all_direct_acked(msg) {
                    continue;
                }
                let ids: Vec<BlobId> = msg.attachments.iter().map(|a| a.blob_id).collect();
                let missing = bstore.restore_mail_pins(msg.id, ids);
                if !missing.is_empty() {
                    warn!(
                        mail_id = msg.id,
                        missing_count = missing.len(),
                        "Recovered mail message references missing attachment blob(s)"
                    );
                }
            }
        }

        Ok(Self {
            state: Mutex::new(state),
            notify: Arc::new(Notify::new()),
            blob_store,
        })
    }

    /// Compacts any uncompacted journal records into `mail_snapshot.bin`, closes the store writer
    /// thread synchronously, and releases `mail_store.lock`.
    pub fn close_and_compact_store_sync(&self) {
        let (store, image) = {
            let mut st = self.state.lock().unwrap();
            let store = st.store.take();
            let image = if store.is_some() && st.journal_records_since_compact > 0 {
                st.journal_records_since_compact = 0;
                Some(st.snapshot_image())
            } else {
                None
            };
            (store, image)
        };
        if let Some(store) = store {
            store.compact_and_close(image);
        }
    }

    /// Validates, pins attachments, persists, evicts over-budget mail, and wakes waiters.
    pub async fn send(
        &self,
        from: String,
        to: Vec<String>,
        topic: String,
        body: String,
        attachments: Vec<MailAttachment>,
    ) -> Result<u64, String> {
        validate_mail_send(&to, &topic, &body, &attachments)?;

        let msg_id = {
            let mut st = self.state.lock().unwrap();
            let id = st.next_id;
            st.next_id = st.next_id.saturating_add(1);
            id
        };

        let had_attachments = !attachments.is_empty();
        if had_attachments {
            let Some(ref bstore) = self.blob_store else {
                return Err("master has no blob store configured for mail attachments".to_string());
            };
            let pairs: Vec<(BlobId, u64)> =
                attachments.iter().map(|a| (a.blob_id, a.size)).collect();
            bstore.pin_mail_checked(msg_id, &pairs)?;
        }

        let created_at_utc = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let message = MailMessage {
            id: msg_id,
            from,
            to,
            topic,
            body,
            attachments,
            created_at_utc,
        };

        let (evicted, acks, persist_err) = {
            let mut st = self.state.lock().unwrap();
            let rollback = st.store.is_some().then(|| {
                (
                    st.messages.clone(),
                    st.total_bytes,
                    st.journal_records_since_compact,
                )
            });
            st.total_bytes = st.total_bytes.saturating_add(mail_message_bytes(&message));
            st.messages.push_back(message.clone());
            st.journal_records_since_compact += 1;
            let evicted = st.evict_over_budget(msg_id);
            if !evicted.is_empty() {
                st.journal_records_since_compact += 1;
            }
            let image_if_compact =
                (st.journal_records_since_compact >= AUTO_COMPACT_RECORDS).then(|| {
                    st.journal_records_since_compact = 0;
                    st.snapshot_image()
                });
            let mut acks = Vec::new();
            let mut persist_err = None;
            if let Some(ref store) = st.store {
                match store.append_durable(&MailJournalRecord::Send { message }) {
                    Ok(ack) => acks.push(ack),
                    Err(e) => persist_err = Some(e.to_string()),
                }
                if persist_err.is_none() && !evicted.is_empty() {
                    match store.append_durable(&MailJournalRecord::Evict {
                        ids: evicted.clone(),
                    }) {
                        Ok(ack) => acks.push(ack),
                        Err(e) => persist_err = Some(e.to_string()),
                    }
                }
                if persist_err.is_none() {
                    if let Some(image) = image_if_compact {
                        if let Ok(ack) = store.compact(image) {
                            acks.push(ack);
                        }
                    }
                }
            }
            if persist_err.is_some() {
                if let Some((msgs, bytes, recs)) = rollback {
                    st.messages = msgs;
                    st.total_bytes = bytes;
                    st.journal_records_since_compact = recs;
                }
            }
            (evicted, acks, persist_err)
        };

        if let Some(err) = persist_err {
            if had_attachments {
                if let Some(ref bstore) = self.blob_store {
                    bstore.unpin_mail(msg_id).await;
                }
            }
            return Err(err);
        }

        for ack in acks {
            if let Err(e) = ack.await {
                {
                    let mut st = self.state.lock().unwrap();
                    if let Some(pos) = st.messages.iter().position(|m| m.id == msg_id) {
                        if let Some(removed) = st.messages.remove(pos) {
                            st.total_bytes =
                                st.total_bytes.saturating_sub(mail_message_bytes(&removed));
                        }
                    }
                }
                if had_attachments {
                    if let Some(ref bstore) = self.blob_store {
                        bstore.unpin_mail(msg_id).await;
                    }
                }
                return Err(e.to_string());
            }
        }

        if let Some(ref bstore) = self.blob_store {
            for id in evicted {
                bstore.unpin_mail(id).await;
            }
        }

        self.notify.notify_waiters();
        Ok(msg_id)
    }

    /// Returns all retained messages matching `consumer` / `topics` after `max(after, cursor)`.
    pub fn fetch(
        &self,
        consumer: &str,
        topics: &[String],
        after: u64,
    ) -> Result<(Vec<MailMessage>, u64), String> {
        validate_mail_consumer(consumer, topics)?;
        let st = self.state.lock().unwrap();
        let cursor = st.cursors.get(consumer).copied().unwrap_or(0);
        let floor = after.max(cursor);
        let messages = st
            .messages
            .iter()
            .filter(|m| m.id > floor && mail_matches_consumer(m, consumer, topics))
            .cloned()
            .collect();
        Ok((messages, cursor))
    }

    /// Advances `consumer`'s ack cursor monotonically and unpins attachments whose direct
    /// recipients have all acknowledged.
    pub async fn ack(&self, consumer: String, up_to_id: u64) -> Result<u64, String> {
        validate_mail_consumer(&consumer, &[])?;

        let (cursor, unpin_ids, ack) = {
            let mut st = self.state.lock().unwrap();
            let latest_id = st.next_id.saturating_sub(1);
            if up_to_id > latest_id {
                return Err(format!(
                    "cannot acknowledge future message #{up_to_id} (latest message is #{latest_id})"
                ));
            }
            let prev = st.cursors.get(&consumer).copied().unwrap_or(0);
            let next = prev.max(up_to_id);
            let mut unpin_ids = Vec::new();
            let ack = if next > prev {
                st.cursors.insert(consumer.clone(), next);
                st.journal_records_since_compact += 1;
                for m in &st.messages {
                    if !m.attachments.is_empty() && st.is_all_direct_acked(m) {
                        unpin_ids.push(m.id);
                    }
                }
                if let Some(ref store) = st.store {
                    let durable = store
                        .append_durable(&MailJournalRecord::Ack {
                            consumer,
                            up_to_id: next,
                        })
                        .map_err(|e| e.to_string())?;
                    Some(durable)
                } else {
                    None
                }
            } else {
                None
            };
            (next, unpin_ids, ack)
        };

        if let Some(durable) = ack {
            if let Err(e) = durable.await {
                return Err(e.to_string());
            }
        }
        if let Some(ref bstore) = self.blob_store {
            for id in unpin_ids {
                bstore.unpin_mail(id).await;
            }
        }
        Ok(cursor)
    }

    /// Long-polls up to `timeout_secs` (clamped to [`MAX_MAIL_WAIT_TIMEOUT_SECS`]) for matching mail.
    pub async fn wait(
        &self,
        consumer: &str,
        topics: &[String],
        after: u64,
        timeout_secs: u64,
    ) -> Result<(Vec<MailMessage>, u64, bool), String> {
        validate_mail_consumer(consumer, topics)?;
        let clamped = timeout_secs.min(MAX_MAIL_WAIT_TIMEOUT_SECS);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(clamped);

        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let (messages, cursor) = self.fetch(consumer, topics, after)?;
            if !messages.is_empty() {
                return Ok((messages, cursor, false));
            }
            if clamped == 0 || tokio::time::Instant::now() >= deadline {
                return Ok((Vec::new(), cursor, true));
            }

            tokio::select! {
                _ = &mut notified => continue,
                _ = tokio::time::sleep_until(deadline) => {
                    let (messages, cursor) = self.fetch(consumer, topics, after)?;
                    let timed_out = messages.is_empty();
                    return Ok((messages, cursor, timed_out));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn eviction_prefers_oldest_acknowledged_before_unacknowledged() {
        let mailbox = Arc::new(Mailbox::open(None, None, 16, 1024).unwrap());
        let id1 = mailbox
            .send(
                "s".into(),
                vec!["a".into()],
                "t".into(),
                "x".repeat(450),
                vec![],
            )
            .await
            .unwrap();
        mailbox.ack("a".into(), id1).await.unwrap();
        let id2 = mailbox
            .send(
                "s".into(),
                vec!["b".into()],
                "t".into(),
                "y".repeat(450),
                vec![],
            )
            .await
            .unwrap();
        let id3 = mailbox
            .send(
                "s".into(),
                vec!["b".into()],
                "t".into(),
                "z".repeat(450),
                vec![],
            )
            .await
            .unwrap();

        let (msgs, _) = mailbox.fetch("b", &["t".into()], 0).unwrap();
        let ids: Vec<u64> = msgs.into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![id2, id3]);
    }

    #[tokio::test]
    async fn wait_wakes_immediately_when_matching_mail_arrives() {
        let mailbox = Arc::new(Mailbox::in_memory_default());
        let waiter_mb = Arc::clone(&mailbox);
        let waiter = tokio::spawn(async move {
            waiter_mb
                .wait("agent-win-2", &["topic:wake".into()], 0, 10)
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let sent_id = mailbox
            .send(
                "agent-mac".into(),
                vec!["topic:wake".into()],
                "wake".into(),
                "go".into(),
                vec![],
            )
            .await
            .unwrap();
        let (msgs, _cursor, timed_out) = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(!timed_out);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, sent_id);
    }

    #[tokio::test]
    async fn ack_rejects_future_message_id_without_advancing_cursor() {
        let mailbox = Mailbox::in_memory_default();

        // When no messages exist yet, acking any id > 0 fails and leaves cursor at 0.
        assert!(mailbox.ack("agent-win-2".into(), 1).await.is_err());
        assert!(mailbox.ack("agent-win-2".into(), u64::MAX).await.is_err());
        assert_eq!(mailbox.ack("agent-win-2".into(), 0).await.unwrap(), 0);

        let id1 = mailbox
            .send(
                "agent-mac".into(),
                vec!["agent-win-2".into()],
                "ping".into(),
                "hello".into(),
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(id1, 1);

        // Acking past the latest issued id must fail and leave #1 unacknowledged.
        assert!(mailbox.ack("agent-win-2".into(), 2).await.is_err());
        assert!(mailbox.ack("agent-win-2".into(), u64::MAX).await.is_err());

        let (msgs, cursor) = mailbox.fetch("agent-win-2", &[], 0).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, id1);

        // Valid ack up to id1 succeeds and advances the cursor.
        assert_eq!(mailbox.ack("agent-win-2".into(), id1).await.unwrap(), id1);
        let (msgs_after, cursor_after) = mailbox.fetch("agent-win-2", &[], 0).unwrap();
        assert_eq!(cursor_after, id1);
        assert!(msgs_after.is_empty());
    }
}
