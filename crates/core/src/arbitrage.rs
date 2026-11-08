//! Cryptographic master lease tickets (§5.3).

use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

use crate::cluster_auth::ClusterSecret;

/// Master lease ticket validity duration in milliseconds (30 seconds).
pub const LEASE_VALIDITY_DURATION_MS: u64 = 30_000;

/// Domain separation label for lease HMAC generation.
pub const LEASE_HMAC_LABEL: &[u8] = b"oxs1-master-lease";

/// Cryptographically signed, time-bounded ticket proving coordinator authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MasterLeaseTicket {
    /// Target cluster identifier.
    pub cluster_id: Uuid,
    /// Node UUID of the designated master coordinator.
    pub master_id: Uuid,
    /// Monotonically increasing lease sequence number.
    pub lease_sequence: u64,
    /// Authority score calculated at ticket generation.
    pub authority_score: i32,
    /// Creation timestamp in milliseconds since Unix epoch.
    pub issued_at_epoch_ms: u64,
    /// Expiration timestamp in milliseconds since Unix epoch.
    pub expires_at_epoch_ms: u64,
    /// 32-byte HMAC-SHA256 signature generated with the shared cluster secret.
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseValidationError {
    #[error("Master lease ticket expired at epoch {expired_at}ms, current time is {current}ms")]
    Expired { expired_at: u64, current: u64 },
    #[error("Invalid lease time range (expires_at <= issued_at)")]
    InvalidTimeRange,
    #[error("Invalid signature length: expected 32 bytes, got {0}")]
    InvalidSignatureLength(usize),
    #[error("Lease ticket signature mismatch against cluster secret")]
    SignatureMismatch,
    #[error("Authority score {0} is below minimum election threshold (2500)")]
    ScoreBelowThreshold(i32),
}

impl MasterLeaseTicket {
    /// Constructs a fresh unsigned lease ticket valid for 30 seconds.
    pub fn new(
        cluster_id: Uuid,
        master_id: Uuid,
        lease_sequence: u64,
        authority_score: i32,
        now_epoch_ms: u64,
    ) -> Self {
        Self {
            cluster_id,
            master_id,
            lease_sequence,
            authority_score,
            issued_at_epoch_ms: now_epoch_ms,
            expires_at_epoch_ms: now_epoch_ms + LEASE_VALIDITY_DURATION_MS,
            signature: Vec::new(),
        }
    }

    /// Issues and signs a fresh lease ticket with custom validity.
    pub fn issue(
        cluster_id: Uuid,
        master_id: Uuid,
        lease_sequence: u64,
        authority_score: i32,
        now_epoch_ms: u64,
        validity: Duration,
        secret: &ClusterSecret,
    ) -> Self {
        let mut ticket = Self {
            cluster_id,
            master_id,
            lease_sequence,
            authority_score,
            issued_at_epoch_ms: now_epoch_ms,
            expires_at_epoch_ms: now_epoch_ms + validity.as_millis() as u64,
            signature: Vec::new(),
        };
        ticket.sign_with_secret(secret);
        ticket
    }

    /// Serializes canonical 60-byte binary payload for HMAC computation.
    /// Layout: [cluster_id: 16B][master_id: 16B][seq: 8B BE][score: 4B BE][issued: 8B BE][expires: 8B BE]
    pub fn canonical_bytes(&self) -> [u8; 60] {
        let mut buf = [0u8; 60];
        buf[0..16].copy_from_slice(self.cluster_id.as_bytes());
        buf[16..32].copy_from_slice(self.master_id.as_bytes());
        buf[32..40].copy_from_slice(&self.lease_sequence.to_be_bytes());
        buf[40..44].copy_from_slice(&self.authority_score.to_be_bytes());
        buf[44..52].copy_from_slice(&self.issued_at_epoch_ms.to_be_bytes());
        buf[52..60].copy_from_slice(&self.expires_at_epoch_ms.to_be_bytes());
        buf
    }

    /// Signs this ticket with the cluster secret using HMAC-SHA256.
    pub fn sign_with_secret(&mut self, secret: &ClusterSecret) {
        let canonical = self.canonical_bytes();
        let tag = secret.mac(&[LEASE_HMAC_LABEL, &canonical]);
        self.signature = tag.to_vec();
    }

    /// Validates ticket expiry, time window, and cryptographic HMAC signature.
    pub fn verify(
        &self,
        secret: Option<&ClusterSecret>,
        current_epoch_ms: u64,
    ) -> Result<(), LeaseValidationError> {
        // 1. Time validity checks
        if current_epoch_ms > self.expires_at_epoch_ms {
            return Err(LeaseValidationError::Expired {
                expired_at: self.expires_at_epoch_ms,
                current: current_epoch_ms,
            });
        }
        if self.expires_at_epoch_ms <= self.issued_at_epoch_ms {
            return Err(LeaseValidationError::InvalidTimeRange);
        }

        // 2. Cryptographic signature verification
        if let Some(sec) = secret {
            if self.signature.len() != 32 {
                return Err(LeaseValidationError::InvalidSignatureLength(
                    self.signature.len(),
                ));
            }
            let canonical = self.canonical_bytes();
            if !sec.verify_mac(&[LEASE_HMAC_LABEL, &canonical], &self.signature) {
                return Err(LeaseValidationError::SignatureMismatch);
            }
        }

        Ok(())
    }

    /// Convenience predicate returning true if ticket is valid and unexpired.
    pub fn is_valid(&self, secret: &ClusterSecret, current_epoch_ms: u64) -> bool {
        self.verify(Some(secret), current_epoch_ms).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_master_lease_ticket_signing_and_verification() {
        let secret = ClusterSecret::generate().unwrap();
        let cluster_id = Uuid::new_v4();
        let master_id = Uuid::new_v4();
        let now = 1_000_000;

        let mut ticket = MasterLeaseTicket::new(cluster_id, master_id, 1, 8500, now);
        ticket.sign_with_secret(&secret);

        // Valid signature & unexpired
        assert!(ticket.verify(Some(&secret), now + 10_000).is_ok());

        // Expired ticket rejected
        assert!(matches!(
            ticket.verify(Some(&secret), now + 35_000),
            Err(LeaseValidationError::Expired { .. })
        ));

        // Tampered payload rejected
        let mut tampered = ticket.clone();
        tampered.authority_score = 9999;
        assert_eq!(
            tampered.verify(Some(&secret), now + 5_000),
            Err(LeaseValidationError::SignatureMismatch)
        );
    }
}
