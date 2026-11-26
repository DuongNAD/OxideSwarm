//! Bitset index over worker capabilities, placement constraints, and free slots.
//!
//! `WorkerIndex` replaces repeated `O(W)` linear scans over worker metadata inside a scheduling
//! pass (`scheduler::Engine::place`) with `O(W / 64)` bitwise intersections over bitsets, plus
//! `O(W)` initial construction once per pass. `RegisteredIndex` builds the same attribute index
//! over all registered workers (including `Disconnected` / cooling-down workers) for
//! `fail_unfulfillable_tasks` checks.

use std::collections::{HashMap, HashSet};

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::placement::{
    normalize_arch, normalize_os, IsolationLevel, ARCH_TAG_PREFIX, EXT_TAG_PREFIX, OS_TAG_PREFIX,
    TOOL_TAG_PREFIX,
};
use rusty_grid_core::protocol::ProtocolCapabilities;
use rusty_grid_core::task::TaskRequirements;
use rusty_grid_core::Placement;
use uuid::Uuid;

use crate::registry::{WorkerInfo, WorkerStatus};

/// Compact dense bitset backed by `u64` words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BitSet {
    words: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// Creates an empty bitset sized for `len` elements.
    pub(crate) fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            len,
        }
    }

    /// Creates a bitset with all indices in `0..len` set.
    pub(crate) fn filled(len: usize) -> Self {
        let mut words = vec![u64::MAX; len.div_ceil(64)];
        let rem = len % 64;
        if rem != 0 {
            if let Some(last) = words.last_mut() {
                *last = (1u64 << rem) - 1;
            }
        }
        Self { words, len }
    }

    #[inline]
    pub(crate) fn set(&mut self, idx: usize) {
        if idx < self.len {
            self.words[idx >> 6] |= 1u64 << (idx & 63);
        }
    }

    #[inline]
    pub(crate) fn clear(&mut self, idx: usize) {
        if idx < self.len {
            self.words[idx >> 6] &= !(1u64 << (idx & 63));
        }
    }

    #[inline]
    pub(crate) fn contains(&self, idx: usize) -> bool {
        idx < self.len && (self.words[idx >> 6] & (1u64 << (idx & 63))) != 0
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    #[inline]
    pub(crate) fn and_assign(&mut self, other: &Self) {
        for (dst, &src) in self.words.iter_mut().zip(other.words.iter()) {
            *dst &= src;
        }
        if self.words.len() > other.words.len() {
            for dst in &mut self.words[other.words.len()..] {
                *dst = 0;
            }
        }
    }

    #[inline]
    pub(crate) fn and_not_assign(&mut self, other: &Self) {
        for (dst, &src) in self.words.iter_mut().zip(other.words.iter()) {
            *dst &= !src;
        }
    }

    #[inline]
    pub(crate) fn or_assign(&mut self, other: &Self) {
        for (dst, &src) in self.words.iter_mut().zip(other.words.iter()) {
            *dst |= src;
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn any_intersect(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(other.words.iter())
            .any(|(&a, &b)| (a & b) != 0)
    }

    #[inline]
    pub(crate) fn iter_ones(&self) -> BitSetOnes<'_> {
        BitSetOnes {
            words: &self.words,
            word_idx: 0,
            current: self.words.first().copied().unwrap_or(0),
        }
    }
}

/// Iterator yielding set bit indices in ascending order.
pub(crate) struct BitSetOnes<'a> {
    words: &'a [u64],
    word_idx: usize,
    current: u64,
}

impl Iterator for BitSetOnes<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        while self.current == 0 {
            self.word_idx += 1;
            if self.word_idx >= self.words.len() {
                return None;
            }
            self.current = self.words[self.word_idx];
        }
        let tz = self.current.trailing_zeros() as usize;
        self.current &= self.current - 1;
        Some((self.word_idx << 6) + tz)
    }
}

/// Precomputed per-seat environmental/hardware flags that stay invariant over one pass.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SeatStaticMeta {
    pub(crate) host_busy: bool,
    pub(crate) is_gpu: bool,
    pub(crate) is_real_gpu: bool,
    pub(crate) initial_load: usize,
}

const KNOWN_FEATURES: ProtocolCapabilities = ProtocolCapabilities(
    ProtocolCapabilities::FILE_STAGING.0
        | ProtocolCapabilities::BLOB_TRANSFER.0
        | ProtocolCapabilities::THREE_D.0
        | ProtocolCapabilities::WORKSPACES.0
        | ProtocolCapabilities::TREE_V2.0,
);

/// Capability and feature requirements passed into worker bitset filtering (`3D-2`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CapRequirements {
    pub(crate) requires_gpu: bool,
    pub(crate) requires_blender: bool,
    pub(crate) required_features: ProtocolCapabilities,
}

/// Attribute bitset index over any slice of `WorkerInfo`.
pub(crate) struct WorkerAttrIndex {
    all: BitSet,
    connected: BitSet,
    gpu: BitSet,
    blender: BitSet,
    by_os: HashMap<String, BitSet>,
    by_arch: HashMap<String, BitSet>,
    by_worker_id: HashMap<Uuid, BitSet>,
    by_name_lower: HashMap<String, BitSet>,
    iso_fs_or_higher: BitSet,
    iso_fs_net: BitSet,
    by_tool: HashMap<String, BitSet>,
    by_tag: HashMap<String, BitSet>,
    feat_file_staging: BitSet,
    feat_blob_transfer: BitSet,
    feat_three_d: BitSet,
    feat_workspaces: BitSet,
    feat_tree_v2: BitSet,
}

impl WorkerAttrIndex {
    /// Builds an attribute index over `workers`.
    pub(crate) fn build(workers: &[WorkerInfo]) -> Self {
        let len = workers.len();
        let all = BitSet::filled(len);
        let mut connected = BitSet::new(len);
        let mut gpu = BitSet::new(len);
        let mut blender = BitSet::new(len);
        let mut by_os: HashMap<String, BitSet> = HashMap::new();
        let mut by_arch: HashMap<String, BitSet> = HashMap::new();
        let mut by_worker_id: HashMap<Uuid, BitSet> = HashMap::with_capacity(len);
        let mut by_name_lower: HashMap<String, BitSet> = HashMap::with_capacity(len);
        let mut iso_fs_or_higher = BitSet::new(len);
        let mut iso_fs_net = BitSet::new(len);
        let mut by_tool: HashMap<String, BitSet> = HashMap::new();
        let mut by_tag: HashMap<String, BitSet> = HashMap::new();
        let mut feat_file_staging = BitSet::new(len);
        let mut feat_blob_transfer = BitSet::new(len);
        let mut feat_three_d = BitSet::new(len);
        let mut feat_workspaces = BitSet::new(len);
        let mut feat_tree_v2 = BitSet::new(len);

        let mut seen_os: HashSet<String> = HashSet::new();
        let mut seen_arch: HashSet<String> = HashSet::new();
        let mut seen_tools: HashSet<String> = HashSet::new();
        let mut seen_tags: HashSet<String> = HashSet::new();

        for (idx, worker) in workers.iter().enumerate() {
            if worker.status != WorkerStatus::Disconnected && !worker.paused {
                connected.set(idx);
            }
            if worker.capabilities.can_execute_gpu() && !worker.gpu_paused {
                gpu.set(idx);
            }
            if worker.capabilities.has_blender {
                blender.set(idx);
            }
            by_worker_id
                .entry(worker.worker_id)
                .or_insert_with(|| BitSet::new(len))
                .set(idx);
            by_name_lower
                .entry(worker.capabilities.name.to_ascii_lowercase())
                .or_insert_with(|| BitSet::new(len))
                .set(idx);

            let iso = IsolationLevel::from_tags(&worker.capabilities.tags);
            if iso >= IsolationLevel::Fs {
                iso_fs_or_higher.set(idx);
            }
            if iso >= IsolationLevel::FsNet {
                iso_fs_net.set(idx);
            }

            seen_os.clear();
            seen_arch.clear();
            seen_tools.clear();
            seen_tags.clear();
            for tag in &worker.capabilities.tags {
                if let Some(os) = tag.strip_prefix(OS_TAG_PREFIX) {
                    let norm = normalize_os(os);
                    if seen_os.insert(norm.clone()) {
                        by_os
                            .entry(norm)
                            .or_insert_with(|| BitSet::new(len))
                            .set(idx);
                    }
                } else if let Some(arch) = tag.strip_prefix(ARCH_TAG_PREFIX) {
                    let norm = normalize_arch(arch);
                    if seen_arch.insert(norm.clone()) {
                        by_arch
                            .entry(norm)
                            .or_insert_with(|| BitSet::new(len))
                            .set(idx);
                    }
                } else if let Some(tool) = tag.strip_prefix(TOOL_TAG_PREFIX) {
                    let norm = tool.to_ascii_lowercase();
                    if seen_tools.insert(norm.clone()) {
                        by_tool
                            .entry(norm)
                            .or_insert_with(|| BitSet::new(len))
                            .set(idx);
                    }
                }
                if seen_tags.insert(tag.clone()) {
                    by_tag
                        .entry(tag.clone())
                        .or_insert_with(|| BitSet::new(len))
                        .set(idx);
                }
            }

            if worker.features.contains(ProtocolCapabilities::FILE_STAGING) {
                feat_file_staging.set(idx);
            }
            if worker
                .features
                .contains(ProtocolCapabilities::BLOB_TRANSFER)
            {
                feat_blob_transfer.set(idx);
            }
            if worker.features.contains(ProtocolCapabilities::THREE_D) {
                feat_three_d.set(idx);
            }
            if worker.features.contains(ProtocolCapabilities::WORKSPACES) {
                feat_workspaces.set(idx);
            }
            if worker.features.contains(ProtocolCapabilities::TREE_V2) {
                feat_tree_v2.set(idx);
            }
        }

        Self {
            all,
            connected,
            gpu,
            blender,
            by_os,
            by_arch,
            by_worker_id,
            by_name_lower,
            iso_fs_or_higher,
            iso_fs_net,
            by_tool,
            by_tag,
            feat_file_staging,
            feat_blob_transfer,
            feat_three_d,
            feat_workspaces,
            feat_tree_v2,
        }
    }

    /// Narrows `mask` in place by `placement` constraints, returning `false` immediately if no
    /// worker can match.
    pub(crate) fn apply_placement(&self, mask: &mut BitSet, placement: &Placement) -> bool {
        if let Some(ref os) = placement.os {
            let Some(m) = self.by_os.get(os) else {
                return false;
            };
            mask.and_assign(m);
            if mask.is_empty() {
                return false;
            }
        }
        if let Some(ref arch) = placement.arch {
            let Some(m) = self.by_arch.get(arch) else {
                return false;
            };
            mask.and_assign(m);
            if mask.is_empty() {
                return false;
            }
        }
        if let Some(ref worker) = placement.worker {
            let name_lower = worker.to_ascii_lowercase();
            let by_name = self.by_name_lower.get(&name_lower);
            let by_id = worker
                .parse::<Uuid>()
                .ok()
                .and_then(|id| self.by_worker_id.get(&id));
            match (by_id, by_name) {
                (Some(a), Some(b)) => {
                    let mut combined = a.clone();
                    combined.or_assign(b);
                    mask.and_assign(&combined);
                }
                (Some(m), None) | (None, Some(m)) => {
                    mask.and_assign(m);
                }
                (None, None) => return false,
            }
            if mask.is_empty() {
                return false;
            }
        }
        if let Some(iso) = placement.isolation {
            match iso {
                IsolationLevel::Off => {}
                IsolationLevel::Fs => {
                    mask.and_assign(&self.iso_fs_or_higher);
                    if mask.is_empty() {
                        return false;
                    }
                }
                IsolationLevel::FsNet => {
                    mask.and_assign(&self.iso_fs_net);
                    if mask.is_empty() {
                        return false;
                    }
                }
            }
        }
        for tool in &placement.tools {
            let Some(m) = self.by_tool.get(tool) else {
                return false;
            };
            mask.and_assign(m);
            if mask.is_empty() {
                return false;
            }
        }
        for id in &placement.extensions {
            let Some(m) = self.by_tag.get(&format!("{EXT_TAG_PREFIX}{id}")) else {
                return false;
            };
            mask.and_assign(m);
            if mask.is_empty() {
                return false;
            }
        }
        for tag in &placement.tags {
            let Some(m) = self.by_tag.get(tag) else {
                return false;
            };
            mask.and_assign(m);
            if mask.is_empty() {
                return false;
            }
        }
        true
    }

    /// Narrows `mask` in place by GPU, Blender, & protocol features; returns `false` if empty.
    pub(crate) fn apply_caps_and_features(&self, mask: &mut BitSet, caps: CapRequirements) -> bool {
        if caps.requires_gpu {
            mask.and_assign(&self.gpu);
            if mask.is_empty() {
                return false;
            }
        }
        if caps.requires_blender {
            mask.and_assign(&self.blender);
            if mask.is_empty() {
                return false;
            }
        }
        if caps
            .required_features
            .contains(ProtocolCapabilities::FILE_STAGING)
        {
            mask.and_assign(&self.feat_file_staging);
            if mask.is_empty() {
                return false;
            }
        }
        if caps
            .required_features
            .contains(ProtocolCapabilities::BLOB_TRANSFER)
        {
            mask.and_assign(&self.feat_blob_transfer);
            if mask.is_empty() {
                return false;
            }
        }
        if caps
            .required_features
            .contains(ProtocolCapabilities::THREE_D)
        {
            mask.and_assign(&self.feat_three_d);
            if mask.is_empty() {
                return false;
            }
        }
        if caps
            .required_features
            .contains(ProtocolCapabilities::WORKSPACES)
        {
            mask.and_assign(&self.feat_workspaces);
            if mask.is_empty() {
                return false;
            }
        }
        if caps
            .required_features
            .contains(ProtocolCapabilities::TREE_V2)
        {
            mask.and_assign(&self.feat_tree_v2);
            if mask.is_empty() {
                return false;
            }
        }
        true
    }

    #[inline]
    pub(crate) fn any_has_blender(&self) -> bool {
        !self.blender.is_empty()
    }

    #[inline]
    pub(crate) fn any_matches_placement(&self, placement: &Placement) -> bool {
        if placement.is_empty() {
            return !self.all.is_empty();
        }
        let mut mask = self.all.clone();
        self.apply_placement(&mut mask, placement)
    }

    /// Returns whether any indexed GPU worker satisfies `requirements`, `req_vram`,
    /// `requires_blender`, and `placement`.
    #[inline]
    pub(crate) fn any_gpu_fits(
        &self,
        workers: &[WorkerInfo],
        requirements: &TaskRequirements,
        req_vram: u64,
        requires_blender: bool,
        placement: &Placement,
    ) -> bool {
        if self.gpu.is_empty() {
            return false;
        }
        let mut mask = self.gpu.clone();
        if requires_blender {
            mask.and_assign(&self.blender);
            if mask.is_empty() {
                return false;
            }
        }
        if !placement.is_empty() && !self.apply_placement(&mut mask, placement) {
            return false;
        }
        mask.iter_ones().any(|idx| {
            statically_fits_caps_with_vram(requirements, req_vram, &workers[idx].capabilities)
        })
    }

    #[inline]
    pub(crate) fn any_has_features(
        &self,
        workers: &[WorkerInfo],
        required_features: ProtocolCapabilities,
    ) -> bool {
        if required_features.is_empty() {
            return !workers.is_empty();
        }
        let mut mask = self.all.clone();
        let caps = CapRequirements {
            requires_gpu: false,
            requires_blender: false,
            required_features,
        };
        if !self.apply_caps_and_features(&mut mask, caps) {
            return false;
        }
        if (required_features.0 & !KNOWN_FEATURES.0) == 0 {
            return true;
        }
        mask.iter_ones()
            .any(|idx| workers[idx].features.contains(required_features))
    }
}

/// Bitset index maintained during one scheduling pass over `Engine::seats`.
pub(crate) struct WorkerIndex {
    attrs: WorkerAttrIndex,
    meta: Vec<SeatStaticMeta>,
    /// Seats that are connected, not host-CPU-throttled, not reserved, and currently have > 0
    /// remaining slots.
    free_slots_mask: BitSet,
    /// Seats that are connected, not host-CPU-throttled, and not reserved.
    unblocked_mask: BitSet,
    /// Connected seats permitted to enter `matching` (`!(host_busy && seat.initial_load == 0)`).
    matching_base_mask: BitSet,
    /// Per-(multi_core, heavy) mobile/environmental eligibility bitsets:
    /// index = `(multi_core as usize) | ((heavy as usize) << 1)`.
    mobile_allow: [BitSet; 4],
}

impl WorkerIndex {
    /// Constructs a `WorkerIndex` aligned 1:1 with `workers` / `Engine::seats`.
    pub(crate) fn build(
        workers: &[WorkerInfo],
        meta: Vec<SeatStaticMeta>,
        remaining_slots: &[usize],
        mobile_flags: &[[bool; 4]],
    ) -> Self {
        let len = workers.len();
        let attrs = WorkerAttrIndex::build(workers);
        let mut free_slots_mask = BitSet::new(len);
        let mut unblocked_mask = BitSet::new(len);
        let mut matching_base_mask = BitSet::new(len);
        let mut mobile_allow = [
            BitSet::new(len),
            BitSet::new(len),
            BitSet::new(len),
            BitSet::new(len),
        ];

        for (idx, m) in meta.iter().enumerate() {
            let connected = attrs.connected.contains(idx);
            if connected {
                if !m.host_busy {
                    unblocked_mask.set(idx);
                    if remaining_slots[idx] > 0 {
                        free_slots_mask.set(idx);
                    }
                    matching_base_mask.set(idx);
                } else if m.initial_load > 0 {
                    matching_base_mask.set(idx);
                }
            }
            let mf = mobile_flags[idx];
            for bucket in 0..4 {
                if mf[bucket] {
                    mobile_allow[bucket].set(idx);
                }
            }
        }

        Self {
            attrs,
            meta,
            free_slots_mask,
            unblocked_mask,
            matching_base_mask,
            mobile_allow,
        }
    }

    #[inline]
    pub(crate) fn attrs(&self) -> &WorkerAttrIndex {
        &self.attrs
    }

    #[inline]
    pub(crate) fn meta(&self, idx: usize) -> &SeatStaticMeta {
        &self.meta[idx]
    }

    #[inline]
    pub(crate) fn set_seat_reserved(&mut self, idx: usize) {
        self.free_slots_mask.clear(idx);
        self.unblocked_mask.clear(idx);
    }

    #[inline]
    pub(crate) fn sync_seat_free(&mut self, idx: usize, has_free: bool, reserved: bool) {
        if reserved || !has_free || self.meta[idx].host_busy || !self.attrs.connected.contains(idx)
        {
            self.free_slots_mask.clear(idx);
        } else {
            self.free_slots_mask.set(idx);
        }
    }

    #[inline]
    fn clear_avoided(&self, mask: &mut BitSet, avoided: &[Uuid]) {
        for wid in avoided {
            if let Some(m) = self.attrs.by_worker_id.get(wid) {
                mask.and_not_assign(m);
            }
        }
    }

    #[inline]
    fn mobile_bucket(cpu_cores: usize, heavy: bool) -> usize {
        ((cpu_cores > 1) as usize) | ((heavy as usize) << 1)
    }

    /// Computes a mask of seats eligible for `found` (connected, not host-busy, not reserved,
    /// remaining slots > 0, satisfying placement, GPU/Blender/required features, mobile/environmental
    /// constraints, and not in `avoided`).
    pub(crate) fn free_candidate_mask(
        &self,
        placement: &Placement,
        caps: CapRequirements,
        cpu_cores: usize,
        heavy: bool,
        avoided: &[Uuid],
    ) -> Option<BitSet> {
        let mut mask = self.free_slots_mask.clone();
        if mask.is_empty() {
            return None;
        }
        let bucket = Self::mobile_bucket(cpu_cores, heavy);
        mask.and_assign(&self.mobile_allow[bucket]);
        if mask.is_empty() {
            return None;
        }
        if !self.attrs.apply_caps_and_features(&mut mask, caps) {
            return None;
        }
        if !placement.is_empty() && !self.attrs.apply_placement(&mut mask, placement) {
            return None;
        }
        if !avoided.is_empty() {
            self.clear_avoided(&mut mask, avoided);
            if mask.is_empty() {
                return None;
            }
        }
        Some(mask)
    }

    /// Computes a mask of unblocked seats (connected, not host-busy, not reserved) matching
    /// static placement, GPU/Blender/required features, mobile rules, and `!avoided`, regardless
    /// of whether slots are currently free.
    pub(crate) fn unblocked_static_mask(
        &self,
        placement: &Placement,
        caps: CapRequirements,
        cpu_cores: usize,
        heavy: bool,
        avoided: &[Uuid],
    ) -> Option<BitSet> {
        let mut mask = self.unblocked_mask.clone();
        if mask.is_empty() {
            return None;
        }
        let bucket = Self::mobile_bucket(cpu_cores, heavy);
        mask.and_assign(&self.mobile_allow[bucket]);
        if mask.is_empty() {
            return None;
        }
        if !self.attrs.apply_caps_and_features(&mut mask, caps) {
            return None;
        }
        if !placement.is_empty() && !self.attrs.apply_placement(&mut mask, placement) {
            return None;
        }
        if !avoided.is_empty() {
            self.clear_avoided(&mut mask, avoided);
            if mask.is_empty() {
                return None;
            }
        }
        Some(mask)
    }

    /// Computes a mask for all connected seats permitted into `matching` (`!(host_busy && load==0)`)
    /// matching static placement, GPU/Blender/required features, mobile rules, and `!avoided`.
    pub(crate) fn matching_static_mask(
        &self,
        placement: &Placement,
        caps: CapRequirements,
        cpu_cores: usize,
        heavy: bool,
        avoided: &[Uuid],
    ) -> Option<BitSet> {
        let mut mask = self.matching_base_mask.clone();
        if mask.is_empty() {
            return None;
        }
        let bucket = Self::mobile_bucket(cpu_cores, heavy);
        mask.and_assign(&self.mobile_allow[bucket]);
        if mask.is_empty() {
            return None;
        }
        if !self.attrs.apply_caps_and_features(&mut mask, caps) {
            return None;
        }
        if !placement.is_empty() && !self.attrs.apply_placement(&mut mask, placement) {
            return None;
        }
        if !avoided.is_empty() {
            self.clear_avoided(&mut mask, avoided);
            if mask.is_empty() {
                return None;
            }
        }
        Some(mask)
    }
}

/// Attribute index over `Engine::registered` (including disconnected / excluded workers)
/// used by `fail_unfulfillable_tasks` checks.
pub(crate) struct RegisteredIndex {
    attrs: WorkerAttrIndex,
}

impl RegisteredIndex {
    pub(crate) fn build(registered: &[WorkerInfo]) -> Self {
        Self {
            attrs: WorkerAttrIndex::build(registered),
        }
    }

    #[inline]
    pub(crate) fn any_has_blender(&self) -> bool {
        self.attrs.any_has_blender()
    }

    #[inline]
    pub(crate) fn any_has_features(
        &self,
        registered: &[WorkerInfo],
        required_features: ProtocolCapabilities,
    ) -> bool {
        self.attrs.any_has_features(registered, required_features)
    }

    #[inline]
    pub(crate) fn any_matches_placement(&self, placement: &Placement) -> bool {
        self.attrs.any_matches_placement(placement)
    }

    /// Returns whether at least one registered worker satisfies `requirements`, `req_vram`,
    /// `placement`, and `caps`.
    pub(crate) fn any_fits_all(
        &self,
        registered: &[WorkerInfo],
        requirements: &TaskRequirements,
        req_vram: u64,
        placement: &Placement,
        caps: CapRequirements,
    ) -> bool {
        if registered.is_empty() {
            return false;
        }
        let mut mask = self.attrs.all.clone();
        if !self.attrs.apply_caps_and_features(&mut mask, caps) {
            return false;
        }
        if !placement.is_empty() && !self.attrs.apply_placement(&mut mask, placement) {
            return false;
        }
        mask.iter_ones().any(|idx| {
            let w = &registered[idx];
            statically_fits_caps_with_vram(requirements, req_vram, &w.capabilities)
                && w.features.contains(caps.required_features)
        })
    }
}

#[inline]
pub(crate) fn statically_fits_caps_with_vram(
    req: &TaskRequirements,
    req_vram: u64,
    caps: &WorkerCapabilities,
) -> bool {
    (req.cpu_cores == 0 || caps.cpu_cores >= req.cpu_cores)
        && (req.ram_mb == 0 || caps.ram_mb >= req.ram_mb)
        && (req_vram == 0 || caps.vram_mb.is_some_and(|v| v >= req_vram))
}

#[cfg(test)]
#[inline]
fn statically_fits_caps(req: &TaskRequirements, caps: &WorkerCapabilities) -> bool {
    statically_fits_caps_with_vram(req, req.effective_vram_mb(), caps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_grid_core::capabilities::WorkerCapabilities;

    #[allow(clippy::too_many_arguments)]
    fn make_worker(
        id: Uuid,
        name: &str,
        cores: usize,
        ram_mb: u64,
        gpu: bool,
        simulated_gpu: bool,
        tags: Vec<String>,
        features: ProtocolCapabilities,
    ) -> WorkerInfo {
        let mut caps = WorkerCapabilities::new(name, cores, ram_mb, gpu, simulated_gpu, None);
        caps.tags = tags;
        let mut info = WorkerInfo::new(
            id,
            1,
            caps,
            crate::registry::WorkerStatus::Connected,
            0,
            0,
            0,
            "127.0.0.1:0".into(),
        );
        info.features = features;
        info
    }

    #[test]
    fn bitset_ops_and_iteration() {
        let mut a = BitSet::new(130);
        a.set(0);
        a.set(63);
        a.set(64);
        a.set(129);
        assert_eq!(a.iter_ones().collect::<Vec<_>>(), vec![0, 63, 64, 129]);

        let mut b = BitSet::filled(130);
        b.clear(63);
        b.clear(64);
        a.and_assign(&b);
        assert_eq!(a.iter_ones().collect::<Vec<_>>(), vec![0, 129]);

        let mut c = BitSet::new(130);
        c.set(129);
        assert!(a.any_intersect(&c));
        a.and_not_assign(&c);
        assert_eq!(a.iter_ones().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn placement_index_matches_linear_scan() {
        let id_mac = Uuid::new_v4();
        let id_win = Uuid::new_v4();
        let id_lin = Uuid::new_v4();
        let workers = vec![
            make_worker(
                id_mac,
                "mac-gpu",
                8,
                16384,
                true,
                false,
                vec![
                    "os:darwin".into(),
                    "arch:arm64".into(),
                    "iso:fs".into(),
                    "tool:Cargo".into(),
                    "fast".into(),
                ],
                ProtocolCapabilities::FILE_STAGING | ProtocolCapabilities::BLOB_TRANSFER,
            ),
            make_worker(
                id_win,
                "win-gpu",
                16,
                32768,
                true,
                false,
                vec![
                    "os:windows".into(),
                    "arch:amd64".into(),
                    "iso:fs+net".into(),
                    "tool:git".into(),
                    "gpu-node".into(),
                ],
                ProtocolCapabilities::FILE_STAGING,
            ),
            make_worker(
                id_lin,
                "lin-cpu",
                4,
                8192,
                false,
                false,
                vec![
                    "os:linux".into(),
                    "arch:x86_64".into(),
                    "tool:cargo".into(),
                    "fast".into(),
                ],
                ProtocolCapabilities::empty(),
            ),
        ];
        let meta = workers
            .iter()
            .map(|w| SeatStaticMeta {
                host_busy: false,
                is_gpu: w.capabilities.can_execute_gpu(),
                is_real_gpu: w.capabilities.has_gpu && !w.capabilities.is_simulated_gpu,
                initial_load: 0,
            })
            .collect();
        let remaining = vec![8, 16, 4];
        let mobile = vec![[true; 4]; 3];
        let index = WorkerIndex::build(&workers, meta, &remaining, &mobile);

        let placements = vec![
            Placement::default(),
            Placement::from_task_tags(&["require:os=macos".into()]).unwrap(),
            Placement::from_task_tags(&["require:arch=aarch64".into()]).unwrap(),
            Placement::from_task_tags(&["require:worker=WIN-GPU".into()]).unwrap(),
            Placement::from_task_tags(&[format!("require:worker={id_lin}")]).unwrap(),
            Placement::from_task_tags(&["require:iso=fs".into()]).unwrap(),
            Placement::from_task_tags(&["require:iso=fs+net".into()]).unwrap(),
            Placement::from_task_tags(&["require:tool=cargo".into()]).unwrap(),
            Placement::from_task_tags(&["require:tag=fast".into()]).unwrap(),
        ];

        for p in &placements {
            for &gpu in &[false, true] {
                for feat in [
                    ProtocolCapabilities::empty(),
                    ProtocolCapabilities::FILE_STAGING,
                    ProtocolCapabilities::BLOB_TRANSFER,
                    ProtocolCapabilities::WORKSPACES,
                    ProtocolCapabilities::TREE_V2,
                    ProtocolCapabilities::THREE_D | ProtocolCapabilities::WORKSPACES,
                ] {
                    let expected: Vec<usize> = workers
                        .iter()
                        .enumerate()
                        .filter(|(_, w)| {
                            p.matches(w.worker_id, &w.capabilities.name, &w.capabilities.tags)
                                && (!gpu || w.capabilities.can_execute_gpu())
                                && w.features.contains(feat)
                        })
                        .map(|(i, _)| i)
                        .collect();
                    let caps = CapRequirements {
                        requires_gpu: gpu,
                        requires_blender: false,
                        required_features: feat,
                    };
                    let actual: Vec<usize> = index
                        .free_candidate_mask(p, caps, 1, false, &[])
                        .map_or_else(Vec::new, |m| m.iter_ones().collect());
                    assert_eq!(actual, expected, "mismatch for {p} gpu={gpu} feat={feat:?}");
                }
            }
        }

        let reg_index = RegisteredIndex::build(&workers);
        assert!(reg_index.any_has_features(&workers, ProtocolCapabilities::FILE_STAGING));
        assert!(!reg_index.any_has_features(&workers, ProtocolCapabilities::WORKSPACES));
    }

    #[test]
    fn tree_v2_feature_indexed_in_worker_and_registered_indices() {
        let w_v1 = make_worker(
            Uuid::new_v4(),
            "w-v1",
            4,
            8192,
            false,
            false,
            Vec::new(),
            ProtocolCapabilities::WORKSPACES | ProtocolCapabilities::FILE_STAGING,
        );
        let w_v2 = make_worker(
            Uuid::new_v4(),
            "w-v2",
            4,
            8192,
            false,
            false,
            Vec::new(),
            ProtocolCapabilities::WORKSPACES
                | ProtocolCapabilities::FILE_STAGING
                | ProtocolCapabilities::TREE_V2,
        );
        let workers = vec![w_v1, w_v2];
        let meta = vec![
            SeatStaticMeta {
                host_busy: false,
                is_gpu: false,
                is_real_gpu: false,
                initial_load: 0,
            },
            SeatStaticMeta {
                host_busy: false,
                is_gpu: false,
                is_real_gpu: false,
                initial_load: 0,
            },
        ];
        let index = WorkerIndex::build(&workers, meta, &[4, 4], &[[true; 4]; 2]);

        let tree_v2_only: Vec<usize> = index
            .free_candidate_mask(
                &Placement::default(),
                CapRequirements {
                    requires_gpu: false,
                    requires_blender: false,
                    required_features: ProtocolCapabilities::TREE_V2,
                },
                1,
                false,
                &[],
            )
            .map_or_else(Vec::new, |m| m.iter_ones().collect());
        assert_eq!(tree_v2_only, vec![1]);

        let ws_plus_v2: Vec<usize> = index
            .free_candidate_mask(
                &Placement::default(),
                CapRequirements {
                    requires_gpu: false,
                    requires_blender: false,
                    required_features: ProtocolCapabilities::WORKSPACES
                        | ProtocolCapabilities::TREE_V2,
                },
                1,
                false,
                &[],
            )
            .map_or_else(Vec::new, |m| m.iter_ones().collect());
        assert_eq!(ws_plus_v2, vec![1]);

        let reg_v1_only = RegisteredIndex::build(&workers[..1]);
        assert!(!reg_v1_only.any_has_features(&workers[..1], ProtocolCapabilities::TREE_V2));

        let reg_both = RegisteredIndex::build(&workers);
        assert!(reg_both.any_has_features(&workers, ProtocolCapabilities::TREE_V2));
    }

    #[test]
    fn statically_fits_caps_enforces_explicit_vram_mb() {
        let caps_8g =
            WorkerCapabilities::new("gpu-8g", 8, 16384, true, true, None).with_vram_mb(Some(8192));
        let caps_4g =
            WorkerCapabilities::new("gpu-4g", 8, 16384, true, true, None).with_vram_mb(Some(4096));
        let caps_unknown = WorkerCapabilities::new("gpu-sim", 8, 16384, true, false, None);

        let req_6g = TaskRequirements::new(1, 512, true, 60).with_vram_mb(6144);
        assert!(statically_fits_caps(&req_6g, &caps_8g));
        assert!(!statically_fits_caps(&req_6g, &caps_4g));
        assert!(!statically_fits_caps(&req_6g, &caps_unknown));

        let req_any_gpu = TaskRequirements::gpu(60);
        assert!(statically_fits_caps(&req_any_gpu, &caps_8g));
        assert!(statically_fits_caps(&req_any_gpu, &caps_unknown));
    }
}
