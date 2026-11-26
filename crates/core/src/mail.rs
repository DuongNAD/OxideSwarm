//! Grid mailbox types and validation shared across wire protocol, master, and CLI
//! (`docs/design/0111-team-plan-leases-and-verify-gate.md`, D8 / TEAM-6).
//!
//! No field in this module may carry `#[serde(skip_serializing_if)]`: every struct here is
//! encoded inside `bincode` `ClientMessage` / `ClientResponse` frames and the durable mail
//! journal.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::blob::{validate_blob_path, BlobId};

/// Maximum UTF-8 byte length of a single mail message body (64 KiB, design 0111 D8.1).
pub const MAX_MAIL_BODY_BYTES: usize = 64 * 1024;
/// Maximum number of attachments on a single mail message.
pub const MAX_MAIL_ATTACHMENTS: usize = 32;
/// Maximum number of recipients in `ClientMessage::MailSend::to`.
pub const MAX_MAIL_RECIPIENTS: usize = 64;
/// Maximum UTF-8 byte length of a topic string.
pub const MAX_MAIL_TOPIC_BYTES: usize = 256;
/// Maximum UTF-8 byte length of an address / consumer identifier.
pub const MAX_MAIL_ADDRESS_BYTES: usize = 128;
/// Maximum server-side long-poll duration accepted for `ClientMessage::MailWait`.
pub const MAX_MAIL_WAIT_TIMEOUT_SECS: u64 = 300;
/// Maximum number of topic filters passed to `MailFetch` / `MailWait`.
pub const MAX_MAIL_TOPICS_FILTER: usize = 64;

/// A content-addressed attachment referenced by a mailbox message (uploaded via `BlobRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailAttachment {
    /// Relative portable file name/path validated with [`validate_blob_path`].
    pub name: String,
    /// BLAKE3 digest of the attachment blob stored in the master `BlobStore`.
    pub blob_id: BlobId,
    /// Expected size in bytes.
    pub size: u64,
}

/// One durable message stored in the master grid mailbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailMessage {
    /// Monotonically increasing master-assigned message ID (1-based).
    pub id: u64,
    /// Sender identity (authenticated member name/ID or `"local"` on unauthenticated loopback).
    pub from: String,
    /// Addressed recipients (agent IDs, member/worker names, or `"topic:<name>"`).
    pub to: Vec<String>,
    /// Short classification topic (e.g. `"verify.done"` or `"lease.expired"`).
    pub topic: String,
    /// Message text (up to [`MAX_MAIL_BODY_BYTES`] UTF-8 bytes).
    pub body: String,
    /// Content-addressed attachments pinned in the master `BlobStore` while retained/unacked.
    pub attachments: Vec<MailAttachment>,
    /// Creation timestamp (seconds since Unix epoch, UTC).
    pub created_at_utc: u64,
}

fn validate_label(
    kind: &str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), String> {
    if !allow_empty && value.trim().is_empty() {
        return Err(format!("mail {kind} cannot be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!(
            "mail {kind} is {} bytes, above the {max_bytes} byte limit",
            value.len()
        ));
    }
    if value.chars().any(|c| c.is_control()) {
        return Err(format!("mail {kind} contains control characters"));
    }
    Ok(())
}

/// Validates a `MailSend` payload before accepting or pinning attachments.
pub fn validate_mail_send(
    to: &[String],
    topic: &str,
    body: &str,
    attachments: &[MailAttachment],
) -> Result<(), String> {
    if to.is_empty() {
        return Err("mail must specify at least one recipient in `to`".to_string());
    }
    if to.len() > MAX_MAIL_RECIPIENTS {
        return Err(format!(
            "mail may name at most {MAX_MAIL_RECIPIENTS} recipients, got {}",
            to.len()
        ));
    }
    for recipient in to {
        validate_label("recipient", recipient, MAX_MAIL_ADDRESS_BYTES, false)?;
    }
    validate_label("topic", topic, MAX_MAIL_TOPIC_BYTES, true)?;
    if body.len() > MAX_MAIL_BODY_BYTES {
        return Err(format!(
            "mail body is {} bytes, above the {MAX_MAIL_BODY_BYTES} byte (64 KiB) limit",
            body.len()
        ));
    }
    if attachments.len() > MAX_MAIL_ATTACHMENTS {
        return Err(format!(
            "mail may include at most {MAX_MAIL_ATTACHMENTS} attachments, got {}",
            attachments.len()
        ));
    }
    let mut seen_names = HashSet::with_capacity(attachments.len());
    for att in attachments {
        validate_blob_path(&att.name)
            .map_err(|e| format!("invalid mail attachment name '{}': {e}", att.name))?;
        if !seen_names.insert(att.name.as_str()) {
            return Err(format!(
                "duplicate mail attachment name '{}' in message",
                att.name
            ));
        }
    }
    Ok(())
}

/// Validates consumer and topic filters for `MailFetch`, `MailWait`, or `MailAck`.
pub fn validate_mail_consumer(consumer: &str, topics: &[String]) -> Result<(), String> {
    validate_label("consumer", consumer, MAX_MAIL_ADDRESS_BYTES, false)?;
    if topics.len() > MAX_MAIL_TOPICS_FILTER {
        return Err(format!(
            "mail topic filter may name at most {MAX_MAIL_TOPICS_FILTER} entries, got {}",
            topics.len()
        ));
    }
    for topic in topics {
        validate_label("topic filter", topic, MAX_MAIL_TOPIC_BYTES, false)?;
    }
    Ok(())
}

/// Returns `true` when `msg` matches `consumer` directly or through any subscribed `topics`.
pub fn mail_matches_consumer(msg: &MailMessage, consumer: &str, topics: &[String]) -> bool {
    if msg.to.iter().any(|r| r == consumer) {
        return true;
    }
    for sub in topics {
        if msg.to.iter().any(|r| r == sub) {
            return true;
        }
        if !msg.topic.is_empty() && msg.topic == *sub {
            return true;
        }
        if let Some(stripped) = sub.strip_prefix("topic:") {
            if !msg.topic.is_empty() && msg.topic == stripped {
                return true;
            }
        } else if msg.to.iter().any(|r| {
            r.strip_prefix("topic:")
                .is_some_and(|prefixed| prefixed == sub)
        }) {
            return true;
        }
    }
    false
}

/// Iterates over explicit non-topic recipients (`to` entries not prefixed with `"topic:"`).
pub fn mail_direct_recipients(msg: &MailMessage) -> impl Iterator<Item = &str> {
    msg.to
        .iter()
        .map(String::as_str)
        .filter(|r| !r.starts_with("topic:"))
}

/// Retained content byte footprint used to enforce the master mailbox byte bound.
pub fn mail_message_bytes(msg: &MailMessage) -> usize {
    let to_bytes: usize = msg.to.iter().map(String::len).sum();
    let att_bytes: usize = msg.attachments.iter().map(|a| a.name.len() + 40).sum();
    msg.from
        .len()
        .saturating_add(msg.topic.len())
        .saturating_add(msg.body.len())
        .saturating_add(to_bytes)
        .saturating_add(att_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_msg(to: &[&str], topic: &str) -> MailMessage {
        MailMessage {
            id: 1,
            from: "agent-mac".into(),
            to: to.iter().map(|s| (*s).to_string()).collect(),
            topic: topic.into(),
            body: "hello".into(),
            attachments: Vec::new(),
            created_at_utc: 1_700_000_000,
        }
    }

    #[test]
    fn matches_direct_and_topic_subscriptions() {
        let direct = sample_msg(&["agent-win-2"], "note");
        assert!(mail_matches_consumer(&direct, "agent-win-2", &[]));
        assert!(!mail_matches_consumer(&direct, "agent-mac", &[]));
        assert!(mail_matches_consumer(&direct, "observer", &["note".into()]));

        let topic_addressed = sample_msg(&["topic:team-6"], "verify.done");
        assert!(mail_matches_consumer(
            &topic_addressed,
            "agent-win",
            &["topic:team-6".into()]
        ));
        assert!(mail_matches_consumer(
            &topic_addressed,
            "agent-win",
            &["team-6".into()]
        ));
        assert!(mail_matches_consumer(
            &topic_addressed,
            "agent-win",
            &["topic:verify.done".into()]
        ));
        assert!(!mail_matches_consumer(
            &topic_addressed,
            "agent-win",
            &["topic:other".into()]
        ));
    }

    #[test]
    fn validation_rejects_oversized_body_empty_to_and_bad_attachment_paths() {
        let big = "x".repeat(MAX_MAIL_BODY_BYTES + 1);
        assert!(validate_mail_send(&["c1".into()], "t", &big, &[]).is_err());
        assert!(validate_mail_send(&[], "t", "ok", &[]).is_err());
        let bad_att = MailAttachment {
            name: "../escape.bin".into(),
            blob_id: BlobId::of(b"x"),
            size: 1,
        };
        assert!(validate_mail_send(&["c1".into()], "t", "ok", &[bad_att]).is_err());
        assert!(validate_mail_consumer("", &[]).is_err());
        assert!(validate_mail_consumer("c1", &[String::new()]).is_err());
    }
}
