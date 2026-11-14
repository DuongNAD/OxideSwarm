//! Per-submitter Weighted Round-Robin (WRR) fair-share queue ordering (`design 0050` D2.4).
//!
//! Each queued task belongs to a [`SubmitterId`] (by default the client TCP/QUIC connection UUID
//! assigned on accept, or [`SubmitterId::Default`] for internal callers without a session id). When
//! two or more submitters have queued work simultaneously, [`FairShareQueues`] yields tasks across
//! active submitters in weighted round-robin order while preserving `(Reverse(priority), sequence)`
//! order within each submitter's own `BTreeSet<QueueOrderKey>`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::Bound;

use uuid::Uuid;

use crate::queue::QueueOrderKey;

/// Identifies who submitted a queued task for scheduling fair-share accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum SubmitterId {
    /// Internal / unattributed callers (`submit`, `submit_with_priority`).
    #[default]
    Default,
    /// Per-client connection identity (or explicit caller token).
    Client(Uuid),
}

#[derive(Debug, Default)]
struct SubmitterQueue {
    weight: u32,
    keys: BTreeSet<QueueOrderKey>,
    in_ring: bool,
}

/// Cursor returned by [`FairShareQueues::walk`] so multi-page scheduler scans resume without
/// repeating already-inspected tasks across pages.
#[derive(Debug, Clone, Default)]
pub(crate) struct FairWalkCursor {
    last_yielded: HashMap<SubmitterId, QueueOrderKey>,
    ring: VecDeque<SubmitterId>,
    head_remaining: u32,
    started: bool,
}

/// Per-submitter fair-share index layered alongside `TaskQueueInner::ready_queue`.
#[derive(Debug, Default)]
pub(crate) struct FairShareQueues {
    submitters: HashMap<SubmitterId, SubmitterQueue>,
    active_ring: VecDeque<SubmitterId>,
    head_remaining: u32,
}

impl FairShareQueues {
    /// Sets the WRR quantum weight (clamped to `>= 1`) for `submitter`.
    pub(crate) fn set_weight(&mut self, submitter: SubmitterId, weight: u32) {
        let clamped = weight.max(1);
        let entry = self
            .submitters
            .entry(submitter)
            .or_insert_with(|| SubmitterQueue {
                weight: clamped,
                keys: BTreeSet::new(),
                in_ring: false,
            });
        entry.weight = clamped;
        if self.active_ring.front() == Some(&submitter) {
            self.head_remaining = self.head_remaining.min(clamped).max(1);
        }
    }

    #[inline]
    pub(crate) fn active_submitter_count(&self) -> usize {
        self.active_ring.len()
    }

    #[inline]
    fn weight_of(&self, submitter: SubmitterId) -> u32 {
        self.submitters
            .get(&submitter)
            .map_or(1, |s| s.weight.max(1))
    }

    /// Inserts `key` under `submitter`.
    pub(crate) fn insert(&mut self, submitter: SubmitterId, key: QueueOrderKey) {
        let entry = self
            .submitters
            .entry(submitter)
            .or_insert_with(|| SubmitterQueue {
                weight: 1,
                keys: BTreeSet::new(),
                in_ring: false,
            });
        entry.keys.insert(key);
        if !entry.in_ring {
            entry.in_ring = true;
            let was_empty = self.active_ring.is_empty();
            let weight = entry.weight.max(1);
            self.active_ring.push_back(submitter);
            if was_empty {
                self.head_remaining = weight;
            }
        }
    }

    /// Removes `key` from `submitter`'s ready set (falling back to scanning all submitters if
    /// needed), dropping the submitter from `active_ring` if its set becomes empty.
    pub(crate) fn remove(&mut self, submitter: SubmitterId, key: &QueueOrderKey) -> bool {
        if Self::remove_from_submitter(
            &mut self.submitters,
            &mut self.active_ring,
            &mut self.head_remaining,
            submitter,
            key,
        ) {
            return true;
        }
        let other_ids: Vec<SubmitterId> = self
            .submitters
            .iter()
            .filter_map(|(&id, sq)| (id != submitter && sq.keys.contains(key)).then_some(id))
            .collect();
        let mut removed = false;
        for id in other_ids {
            if Self::remove_from_submitter(
                &mut self.submitters,
                &mut self.active_ring,
                &mut self.head_remaining,
                id,
                key,
            ) {
                removed = true;
            }
        }
        removed
    }

    fn remove_from_submitter(
        submitters: &mut HashMap<SubmitterId, SubmitterQueue>,
        active_ring: &mut VecDeque<SubmitterId>,
        head_remaining: &mut u32,
        submitter: SubmitterId,
        key: &QueueOrderKey,
    ) -> bool {
        let Some(entry) = submitters.get_mut(&submitter) else {
            return false;
        };
        if !entry.keys.remove(key) {
            return false;
        }
        if entry.keys.is_empty() && entry.in_ring {
            entry.in_ring = false;
            let was_front = active_ring.front() == Some(&submitter);
            active_ring.retain(|&s| s != submitter);
            if was_front {
                *head_remaining = active_ring
                    .front()
                    .and_then(|next| submitters.get(next))
                    .map_or(0, |s| s.weight.max(1));
            }
        }
        true
    }

    /// Rebuilds the entire fair-share structure cleanly (used on replay/clear).
    pub(crate) fn clear_entries_preserving_weights(&mut self) {
        for s in self.submitters.values_mut() {
            s.keys.clear();
            s.in_ring = false;
        }
        self.active_ring.clear();
        self.head_remaining = 0;
    }

    /// Advances the persistent WRR ring when a task belonging to `submitter` was scheduled.
    pub(crate) fn on_scheduled(&mut self, submitter: SubmitterId) {
        if self.active_ring.is_empty() {
            self.head_remaining = 0;
            return;
        }
        if self.active_ring.front() == Some(&submitter) {
            if self.head_remaining > 1 {
                self.head_remaining -= 1;
            } else {
                if self.active_ring.len() > 1 {
                    if let Some(front) = self.active_ring.pop_front() {
                        self.active_ring.push_back(front);
                    }
                }
                self.head_remaining = self
                    .active_ring
                    .front()
                    .map_or(0, |&next| self.weight_of(next));
            }
        }
    }

    /// Walks up to `limit` queued `(QueueOrderKey, SubmitterId)` pairs across active submitters
    /// in weighted round-robin order without mutating `active_ring`. Updates `cursor` so
    /// subsequent pages continue seamlessly until all submitters are exhausted.
    pub(crate) fn walk(
        &self,
        cursor: &mut FairWalkCursor,
        limit: usize,
    ) -> Vec<(QueueOrderKey, SubmitterId)> {
        if limit == 0 || self.active_ring.is_empty() {
            return Vec::new();
        }
        if !cursor.started {
            cursor.started = true;
            cursor.ring = self.active_ring.clone();
            cursor.head_remaining = self.head_remaining.max(1);
        }
        if cursor.ring.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(limit.min(512));

        while out.len() < limit && !cursor.ring.is_empty() {
            let sub = *cursor.ring.front().unwrap();
            let next_key = self.submitters.get(&sub).and_then(|sq| {
                let start = match cursor.last_yielded.get(&sub) {
                    Some(prev) => Bound::Excluded(*prev),
                    None => Bound::Unbounded,
                };
                sq.keys.range((start, Bound::Unbounded)).next().copied()
            });

            match next_key {
                Some(key) => {
                    out.push((key, sub));
                    cursor.last_yielded.insert(sub, key);
                    cursor.head_remaining = cursor.head_remaining.saturating_sub(1);
                    if cursor.head_remaining == 0 {
                        cursor.ring.pop_front();
                        cursor.ring.push_back(sub);
                        cursor.head_remaining =
                            cursor.ring.front().map_or(1, |&next| self.weight_of(next));
                    }
                }
                None => {
                    cursor.ring.pop_front();
                    cursor.head_remaining =
                        cursor.ring.front().map_or(0, |&next| self.weight_of(next));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::task::TaskId;
    use std::cmp::Reverse;

    fn key(priority: u32, seq: u64) -> QueueOrderKey {
        QueueOrderKey {
            priority_rev: Reverse(priority),
            sequence: seq,
            task_id: TaskId::new(),
        }
    }

    #[test]
    fn equal_weights_interleave_across_submitters() {
        let mut fq = FairShareQueues::default();
        let a = SubmitterId::Client(Uuid::new_v4());
        let b = SubmitterId::Client(Uuid::new_v4());

        for seq in 0..4 {
            fq.insert(a, key(0, seq));
        }
        for seq in 4..6 {
            fq.insert(b, key(0, seq));
        }

        let mut cursor = FairWalkCursor::default();
        let walked: Vec<(u64, SubmitterId)> = fq
            .walk(&mut cursor, 16)
            .into_iter()
            .map(|(k, s)| (k.sequence, s))
            .collect();
        assert_eq!(
            walked,
            vec![(0, a), (4, b), (1, a), (5, b), (2, a), (3, a),]
        );
    }

    #[test]
    fn weighted_round_robin_honors_weights_and_pagination() {
        let mut fq = FairShareQueues::default();
        let a = SubmitterId::Client(Uuid::new_v4());
        let b = SubmitterId::Client(Uuid::new_v4());
        fq.set_weight(a, 3);
        fq.set_weight(b, 1);

        for seq in 0..5 {
            fq.insert(a, key(0, seq));
        }
        for seq in 5..8 {
            fq.insert(b, key(0, seq));
        }

        let mut cursor = FairWalkCursor::default();
        let p1: Vec<(u64, SubmitterId)> = fq
            .walk(&mut cursor, 3)
            .into_iter()
            .map(|(k, s)| (k.sequence, s))
            .collect();
        let p2: Vec<(u64, SubmitterId)> = fq
            .walk(&mut cursor, 3)
            .into_iter()
            .map(|(k, s)| (k.sequence, s))
            .collect();
        let p3: Vec<(u64, SubmitterId)> = fq
            .walk(&mut cursor, 10)
            .into_iter()
            .map(|(k, s)| (k.sequence, s))
            .collect();

        let mut combined = Vec::new();
        combined.extend(p1);
        combined.extend(p2);
        combined.extend(p3);
        assert_eq!(
            combined,
            vec![
                (0, a),
                (1, a),
                (2, a),
                (5, b),
                (3, a),
                (4, a),
                (6, b),
                (7, b),
            ]
        );
    }
}
