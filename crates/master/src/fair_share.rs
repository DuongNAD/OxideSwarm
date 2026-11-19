//! Per-submitter Weighted Round-Robin (WRR) fair-share queue ordering (`design 0050` D2.4,
//! `design 0097` D3).
//!
//! Each queued task belongs to a [`SubmitterId`] (resolved to [`SubmitterId::Participant`] for
//! ECO-9 participant-keyed fair share, [`SubmitterId::Client`] for per-connection tests, or
//! [`SubmitterId::Default`] for internal callers without attribution). [`FairShareQueues`] yields
//! tasks across active non-excluded submitters in priority-tiered WRR order while preserving
//! `(Reverse(priority), sequence)` order within each submitter's own `BTreeSet<QueueOrderKey>`.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Bound;

use uuid::Uuid;

use crate::queue::QueueOrderKey;

/// Identifies who submitted a queued task for scheduling fair-share accounting.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum SubmitterId {
    /// Internal / unattributed callers (`submit`, `submit_with_priority`).
    #[default]
    Default,
    /// Per-client connection identity (used by legacy/direct connection fair-share tests).
    Client(Uuid),
    /// Participant identity grouping all connections and keys of one participant (`ECO-9`, design 0097).
    Participant(String),
}

#[derive(Debug, Default)]
struct SubmitterQueue {
    weight: u32,
    remaining: u32,
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
            .entry(submitter.clone())
            .or_insert_with(|| SubmitterQueue {
                weight: clamped,
                remaining: clamped,
                keys: BTreeSet::new(),
                in_ring: false,
            });
        entry.weight = clamped;
        entry.remaining = entry.remaining.min(clamped).max(1);
        if self.active_ring.front() == Some(&submitter) {
            self.head_remaining = entry.remaining;
        }
    }

    #[inline]
    pub(crate) fn active_submitter_count(&self) -> usize {
        self.active_ring.len()
    }

    #[inline]
    fn weight_of(&self, submitter: &SubmitterId) -> u32 {
        self.submitters
            .get(submitter)
            .map_or(1, |s| s.weight.max(1))
    }

    #[inline]
    fn remaining_of(&self, submitter: &SubmitterId) -> u32 {
        self.submitters
            .get(submitter)
            .map_or(1, |s| s.remaining.max(1))
    }

    /// Inserts `key` under `submitter`.
    pub(crate) fn insert(&mut self, submitter: SubmitterId, key: QueueOrderKey) {
        let entry = self
            .submitters
            .entry(submitter.clone())
            .or_insert_with(|| SubmitterQueue {
                weight: 1,
                remaining: 1,
                keys: BTreeSet::new(),
                in_ring: false,
            });
        entry.keys.insert(key);
        if !entry.in_ring {
            entry.in_ring = true;
            entry.remaining = entry.weight.max(1);
            let was_empty = self.active_ring.is_empty();
            let remaining = entry.remaining;
            self.active_ring.push_back(submitter);
            if was_empty {
                self.head_remaining = remaining;
            }
        }
    }

    /// Removes `key` from `submitter`'s ready set (falling back to scanning all submitters if
    /// needed), dropping the submitter from `active_ring` if its set becomes empty.
    pub(crate) fn remove(&mut self, submitter: &SubmitterId, key: &QueueOrderKey) -> bool {
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
            .filter(|&(id, sq)| id != submitter && sq.keys.contains(key))
            .map(|(id, _)| id.clone())
            .collect();
        let mut removed = false;
        for id in other_ids {
            if Self::remove_from_submitter(
                &mut self.submitters,
                &mut self.active_ring,
                &mut self.head_remaining,
                &id,
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
        submitter: &SubmitterId,
        key: &QueueOrderKey,
    ) -> bool {
        let Some(entry) = submitters.get_mut(submitter) else {
            return false;
        };
        if !entry.keys.remove(key) {
            return false;
        }
        if entry.keys.is_empty() && entry.in_ring {
            entry.in_ring = false;
            entry.remaining = entry.weight.max(1);
            let was_front = active_ring.front() == Some(submitter);
            active_ring.retain(|s| s != submitter);
            if was_front {
                *head_remaining = active_ring
                    .front()
                    .and_then(|next| submitters.get(next))
                    .map_or(0, |s| s.remaining.max(1));
            }
        }
        true
    }

    /// Rebuilds the entire fair-share structure cleanly (used on replay/clear).
    pub(crate) fn clear_entries_preserving_weights(&mut self) {
        for s in self.submitters.values_mut() {
            s.keys.clear();
            s.in_ring = false;
            s.remaining = s.weight.max(1);
        }
        self.active_ring.clear();
        self.head_remaining = 0;
    }

    /// Advances the persistent WRR ring when a task belonging to `submitter` was scheduled, even
    /// when another excluded or lower-priority submitter sits at the front of `active_ring`.
    pub(crate) fn on_scheduled(&mut self, submitter: &SubmitterId) {
        if self.active_ring.is_empty() {
            self.head_remaining = 0;
            return;
        }
        if let Some(pos) = self.active_ring.iter().position(|s| s == submitter) {
            if let Some(entry) = self.submitters.get_mut(submitter) {
                if entry.remaining > 1 {
                    entry.remaining -= 1;
                } else {
                    entry.remaining = entry.weight.max(1);
                    if self.active_ring.len() > 1 {
                        if let Some(moved) = self.active_ring.remove(pos) {
                            self.active_ring.push_back(moved);
                        }
                    }
                }
            }
        }
        self.head_remaining = self
            .active_ring
            .front()
            .map_or(0, |front| self.remaining_of(front));
    }

    fn next_key_for(
        &self,
        sub: &SubmitterId,
        last_yielded: &HashMap<SubmitterId, QueueOrderKey>,
    ) -> Option<QueueOrderKey> {
        self.submitters.get(sub).and_then(|sq| {
            let start = match last_yielded.get(sub) {
                Some(prev) => Bound::Excluded(*prev),
                None => Bound::Unbounded,
            };
            sq.keys.range((start, Bound::Unbounded)).next().copied()
        })
    }

    /// Walks up to `limit` queued `(QueueOrderKey, SubmitterId)` pairs across active submitters
    /// in weighted round-robin order without mutating `active_ring`.
    #[cfg(test)]
    pub(crate) fn walk(
        &self,
        cursor: &mut FairWalkCursor,
        limit: usize,
    ) -> Vec<(QueueOrderKey, SubmitterId)> {
        self.walk_excluding(cursor, limit, &HashSet::new())
    }

    /// Walks up to `limit` queued `(QueueOrderKey, SubmitterId)` pairs across active submitters
    /// not in `excluded`, serving higher-priority work first with weighted round-robin within each
    /// priority tier (`ECO-9`, design 0097 D3).
    pub(crate) fn walk_excluding(
        &self,
        cursor: &mut FairWalkCursor,
        limit: usize,
        excluded: &HashSet<SubmitterId>,
    ) -> Vec<(QueueOrderKey, SubmitterId)> {
        if limit == 0 || self.active_ring.is_empty() {
            return Vec::new();
        }
        if !cursor.started {
            cursor.started = true;
            cursor.ring = self
                .active_ring
                .iter()
                .filter(|s| !excluded.contains(*s))
                .cloned()
                .collect();
            cursor.head_remaining = cursor
                .ring
                .front()
                .map_or(1, |front| self.remaining_of(front));
        } else if !excluded.is_empty() {
            let old_front = cursor.ring.front().cloned();
            cursor.ring.retain(|s| !excluded.contains(s));
            if cursor.ring.front() != old_front.as_ref() {
                cursor.head_remaining =
                    cursor.ring.front().map_or(1, |front| self.weight_of(front));
            }
        }
        if cursor.ring.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(limit.min(512));

        while out.len() < limit && !cursor.ring.is_empty() {
            // Drop any submitter whose queued keys are now exhausted for this pass.
            let mut max_priority: Option<u32> = None;
            let mut empty_subs = false;
            for sub in &cursor.ring {
                match self.next_key_for(sub, &cursor.last_yielded) {
                    Some(k) => {
                        let p = k.priority_rev.0;
                        max_priority = Some(max_priority.map_or(p, |cur| cur.max(p)));
                    }
                    None => empty_subs = true,
                }
            }
            if empty_subs {
                let old_front = cursor.ring.front().cloned();
                let last_yielded = &cursor.last_yielded;
                cursor
                    .ring
                    .retain(|sub| self.next_key_for(sub, last_yielded).is_some());
                if cursor.ring.front() != old_front.as_ref() {
                    cursor.head_remaining =
                        cursor.ring.front().map_or(1, |front| self.weight_of(front));
                }
            }
            let Some(target_priority) = max_priority else {
                break;
            };
            if cursor.ring.is_empty() {
                break;
            }

            let sub = cursor.ring.front().unwrap().clone();
            let Some(key) = self.next_key_for(&sub, &cursor.last_yielded) else {
                continue;
            };
            if key.priority_rev.0 < target_priority {
                cursor.ring.pop_front();
                cursor.ring.push_back(sub);
                cursor.head_remaining = cursor.ring.front().map_or(1, |next| self.weight_of(next));
                continue;
            }

            out.push((key, sub.clone()));
            cursor.last_yielded.insert(sub.clone(), key);
            cursor.head_remaining = cursor.head_remaining.saturating_sub(1);
            if cursor.head_remaining == 0 {
                cursor.ring.pop_front();
                cursor.ring.push_back(sub);
                cursor.head_remaining = cursor.ring.front().map_or(1, |next| self.weight_of(next));
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
            fq.insert(a.clone(), key(0, seq));
        }
        for seq in 4..6 {
            fq.insert(b.clone(), key(0, seq));
        }

        let mut cursor = FairWalkCursor::default();
        let walked: Vec<(u64, SubmitterId)> = fq
            .walk(&mut cursor, 16)
            .into_iter()
            .map(|(k, s)| (k.sequence, s))
            .collect();
        assert_eq!(
            walked,
            vec![
                (0, a.clone()),
                (4, b.clone()),
                (1, a.clone()),
                (5, b),
                (2, a.clone()),
                (3, a),
            ]
        );
    }

    #[test]
    fn weighted_round_robin_honors_weights_and_pagination() {
        let mut fq = FairShareQueues::default();
        let a = SubmitterId::Client(Uuid::new_v4());
        let b = SubmitterId::Client(Uuid::new_v4());
        fq.set_weight(a.clone(), 3);
        fq.set_weight(b.clone(), 1);

        for seq in 0..5 {
            fq.insert(a.clone(), key(0, seq));
        }
        for seq in 5..8 {
            fq.insert(b.clone(), key(0, seq));
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
                (0, a.clone()),
                (1, a.clone()),
                (2, a.clone()),
                (5, b.clone()),
                (3, a.clone()),
                (4, a),
                (6, b.clone()),
                (7, b),
            ]
        );
    }

    #[test]
    fn participant_priority_tiers_and_exclusion_avoid_starvation() {
        let mut fq = FairShareQueues::default();
        let high = SubmitterId::Participant("high-team".into());
        let normal = SubmitterId::Participant("normal-team".into());
        let low = SubmitterId::Participant("low-team".into());
        fq.set_weight(high.clone(), 2);
        fq.set_weight(normal.clone(), 2);
        fq.set_weight(low.clone(), 1);

        fq.insert(low.clone(), key(0, 1));
        fq.insert(normal.clone(), key(50, 2));
        fq.insert(normal.clone(), key(50, 3));
        fq.insert(high.clone(), key(100, 4));
        fq.insert(high.clone(), key(100, 5));

        // When none excluded, high yields first, then normal, then low.
        let mut cursor = FairWalkCursor::default();
        let seqs: Vec<u64> = fq
            .walk(&mut cursor, 10)
            .into_iter()
            .map(|(k, _)| k.sequence)
            .collect();
        assert_eq!(seqs, vec![4, 5, 2, 3, 1]);

        // Excluding capped `high` immediately yields `normal` then `low` without returning `high`.
        let mut excluded = HashSet::new();
        excluded.insert(high);
        let mut cursor_ex = FairWalkCursor::default();
        let seqs_ex: Vec<(u64, SubmitterId)> = fq
            .walk_excluding(&mut cursor_ex, 10, &excluded)
            .into_iter()
            .map(|(k, s)| (k.sequence, s))
            .collect();
        assert_eq!(seqs_ex, vec![(2, normal.clone()), (3, normal), (1, low)]);
    }

    #[test]
    fn non_front_on_scheduled_rotates_even_when_front_is_excluded() {
        let mut fq = FairShareQueues::default();
        let capped_front = SubmitterId::Participant("capped".into());
        let a = SubmitterId::Participant("alice".into());
        let b = SubmitterId::Participant("bob".into());

        // `capped` enters first so it sits at front of `active_ring`.
        fq.insert(capped_front.clone(), key(100, 1));
        for seq in 2..6 {
            fq.insert(a.clone(), key(50, seq));
        }
        for seq in 6..10 {
            fq.insert(b.clone(), key(50, seq));
        }

        let mut excluded = HashSet::new();
        excluded.insert(capped_front);

        let mut picked = Vec::new();
        for _ in 0..4 {
            let mut cursor = FairWalkCursor::default();
            let (k, s) = fq.walk_excluding(&mut cursor, 1, &excluded).pop().unwrap();
            assert!(fq.remove(&s, &k));
            fq.on_scheduled(&s);
            picked.push(s);
        }
        assert_eq!(picked, vec![a.clone(), b.clone(), a, b]);
    }
}
