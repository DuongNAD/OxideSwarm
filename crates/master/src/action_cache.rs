//! Master-side content-addressed action cache (`CACHE-1`, design 0100).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusty_grid_core::blob::BlobId;
use rusty_grid_core::task::{ActionCacheKey, TaskResult};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::blob_store::BlobStore;

/// Default maximum total footprint of cached action outputs + result payloads (`512 MiB`).
pub const DEFAULT_ACTION_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;
/// Relative filename under `--state-dir` for durable action cache metadata.
pub const ACTION_CACHE_FILE: &str = "action_cache.json";

const ACTION_CACHE_PERSIST_VERSION: u32 = 1;

/// Configuration for [`ActionCache`].
#[derive(Debug, Clone)]
pub struct ActionCacheConfig {
    /// Maximum total bytes of retained cache entries (`0` disables caching).
    pub max_bytes: u64,
    /// Optional path (typically `<state_dir>/action_cache.json`) for durable persistence.
    pub persist_path: Option<PathBuf>,
}

impl Default for ActionCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_ACTION_CACHE_MAX_BYTES,
            persist_path: None,
        }
    }
}

/// Point-in-time counters and occupancy of the action cache.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActionCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: u64,
    pub max_bytes: u64,
}

#[derive(Clone)]
struct CacheEntry {
    result: TaskResult,
    size_bytes: u64,
    tick: u64,
    last_used_unix_secs: u64,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<ActionCacheKey, CacheEntry>,
    lru: BTreeMap<u64, ActionCacheKey>,
    tick: u64,
    total_bytes: u64,
}

impl CacheState {
    fn remove_key(&mut self, key: &ActionCacheKey) -> Option<CacheEntry> {
        let entry = self.entries.remove(key)?;
        self.lru.remove(&entry.tick);
        self.total_bytes = self.total_bytes.saturating_sub(entry.size_bytes);
        Some(entry)
    }

    fn touch(&mut self, key: &ActionCacheKey, now_unix: u64) {
        self.tick += 1;
        let next_tick = self.tick;
        if let Some(entry) = self.entries.get_mut(key) {
            self.lru.remove(&entry.tick);
            entry.tick = next_tick;
            entry.last_used_unix_secs = now_unix;
            self.lru.insert(next_tick, *key);
        }
    }

    fn evict_oldest(&mut self) -> Option<ActionCacheKey> {
        let oldest_tick = *self.lru.keys().next()?;
        let key = self.lru.remove(&oldest_tick)?;
        if let Some(entry) = self.entries.remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(entry.size_bytes);
        }
        Some(key)
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedActionCache {
    version: u32,
    entries: Vec<PersistedCacheEntry>,
}

#[derive(Serialize, Deserialize)]
struct PersistedCacheEntry {
    key_hex: String,
    last_used_unix_secs: u64,
    result: TaskResult,
}

/// Thread-safe, LRU-bounded content-addressed cache of deterministic successful task results.
pub struct ActionCache {
    max_bytes: u64,
    persist_path: Option<PathBuf>,
    blob_store: Option<Arc<BlobStore>>,
    state: Mutex<CacheState>,
    persist_lock: Mutex<()>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ActionCache {
    /// Creates (and, when `config.persist_path` exists, reloads) an [`ActionCache`].
    ///
    /// Persisted entries whose output blobs are missing from `blob_store` (or that exceed
    /// `max_bytes`) are pruned cleanly on load.
    pub fn open(config: ActionCacheConfig, blob_store: Option<Arc<BlobStore>>) -> Arc<Self> {
        let cache = Arc::new(Self {
            max_bytes: config.max_bytes,
            persist_path: config.persist_path.clone(),
            blob_store,
            state: Mutex::new(CacheState::default()),
            persist_lock: Mutex::new(()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        });

        if cache.max_bytes > 0 {
            cache.load_from_disk();
        }
        cache
    }

    /// Looks up `key`, returning a cached [`TaskResult`] with `cached: true` if present and all
    /// referenced output blobs still exist in [`BlobStore`].
    pub fn lookup(&self, key: &ActionCacheKey) -> Option<TaskResult> {
        if self.max_bytes == 0 {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let now_unix = Utc::now().timestamp().max(0) as u64;
        enum ProbeOutcome {
            Miss,
            Hit(Box<TaskResult>),
            Stale,
        }

        let probe = {
            let mut st = self.lock_state();
            match st.entries.get(key) {
                None => ProbeOutcome::Miss,
                Some(entry) => {
                    let outputs_valid = if entry.result.outputs.is_empty() {
                        true
                    } else if let Some(ref store) = self.blob_store {
                        entry
                            .result
                            .outputs
                            .iter()
                            .all(|file| store.contains(&file.id))
                    } else {
                        false
                    };

                    if outputs_valid {
                        st.touch(key, now_unix);
                        let mut res = st.entries[key].result.clone();
                        res.cached = true;
                        ProbeOutcome::Hit(Box::new(res))
                    } else {
                        st.remove_key(key);
                        ProbeOutcome::Stale
                    }
                }
            }
        };

        match probe {
            ProbeOutcome::Miss => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            ProbeOutcome::Hit(res) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(*res)
            }
            ProbeOutcome::Stale => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                if let Some(ref store) = self.blob_store {
                    if store.unpin_cache_entry_sync(&key.0) {
                        store.spawn_evict_if_over_limit();
                    }
                }
                self.persist_current();
                None
            }
        }
    }

    /// Inserts a terminal successful `result` under `key`, pinning any output blobs and evicting
    /// oldest LRU entries if `max_bytes` would be exceeded.
    ///
    /// Returns `false` if caching is disabled, `result` failed, `result` exceeds `max_bytes`, or
    /// any output blob cannot be pinned in [`BlobStore`].
    pub fn insert(&self, key: ActionCacheKey, result: &TaskResult) -> bool {
        if self.max_bytes == 0 || !result.is_success() {
            return false;
        }

        let size_bytes = entry_bytes(result);
        if size_bytes > self.max_bytes {
            debug!(
                key = %key,
                size_bytes,
                max_bytes = self.max_bytes,
                "Action cache skipping oversized result"
            );
            return false;
        }

        let output_ids = unique_output_ids(result);
        if !output_ids.is_empty() {
            let Some(ref store) = self.blob_store else {
                return false;
            };
            if !store.pin_cache_entry(key.0, &output_ids) {
                debug!(
                    key = %key,
                    "Action cache skipped entry because an output blob could not be pinned"
                );
                return false;
            }
        } else if let Some(ref store) = self.blob_store {
            let _ = store.unpin_cache_entry_sync(&key.0);
        }

        let mut normalized = result.clone();
        normalized.cached = false;
        let now_unix = Utc::now().timestamp().max(0) as u64;

        let (evicted, snapshot) = {
            let _persist_guard = self.lock_persist();
            let mut evicted = Vec::new();
            let snapshot = {
                let mut st = self.lock_state();
                if st.entries.contains_key(&key) {
                    st.remove_key(&key);
                }
                while st.total_bytes.saturating_add(size_bytes) > self.max_bytes {
                    match st.evict_oldest() {
                        Some(old_key) => evicted.push(old_key),
                        None => break,
                    }
                }
                st.tick += 1;
                let tick = st.tick;
                st.entries.insert(
                    key,
                    CacheEntry {
                        result: normalized,
                        size_bytes,
                        tick,
                        last_used_unix_secs: now_unix,
                    },
                );
                st.lru.insert(tick, key);
                st.total_bytes = st.total_bytes.saturating_add(size_bytes);
                self.snapshot_for_disk(&st)
            };
            (evicted, snapshot)
        };

        self.unpin_and_evict_keys(&evicted);
        if let Some(ref snap) = snapshot {
            self.write_snapshot_under_persist_lock(snap);
        }
        true
    }

    /// Removes `key` from the cache, releases any output blob pins, and updates disk state.
    pub fn invalidate(&self, key: &ActionCacheKey) {
        let (removed, snapshot) = {
            let _persist_guard = self.lock_persist();
            let (removed, snap) = {
                let mut st = self.lock_state();
                let removed = st.remove_key(key).is_some();
                let snap = if removed {
                    self.snapshot_for_disk(&st)
                } else {
                    None
                };
                (removed, snap)
            };
            (removed, snap)
        };
        if removed {
            if let Some(ref store) = self.blob_store {
                if store.unpin_cache_entry_sync(&key.0) {
                    store.spawn_evict_if_over_limit();
                }
            }
            if let Some(ref snap) = snapshot {
                self.write_snapshot_under_persist_lock(snap);
            }
        }
    }

    /// Returns point-in-time action cache statistics.
    pub fn stats(&self) -> ActionCacheStats {
        let st = self.lock_state();
        ActionCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: st.entries.len(),
            bytes: st.total_bytes,
            max_bytes: self.max_bytes,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lock_persist(&self) -> std::sync::MutexGuard<'_, ()> {
        self.persist_lock.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn unpin_and_evict_keys(&self, keys: &[ActionCacheKey]) {
        if keys.is_empty() {
            return;
        }
        if let Some(ref store) = self.blob_store {
            let mut need_evict = false;
            for key in keys {
                if store.unpin_cache_entry_sync(&key.0) {
                    need_evict = true;
                }
            }
            if need_evict {
                store.spawn_evict_if_over_limit();
            }
        }
    }

    fn load_from_disk(&self) {
        let Some(ref path) = self.persist_path else {
            return;
        };
        if !path.exists() {
            return;
        }
        let raw = match fs::read_to_string(path) {
            Ok(data) => data,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "Failed to read persisted action cache; starting empty");
                return;
            }
        };
        let mut parsed: PersistedActionCache = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "Corrupt persisted action cache; starting empty");
                return;
            }
        };
        if parsed.version != ACTION_CACHE_PERSIST_VERSION {
            warn!(
                path = %path.display(),
                version = parsed.version,
                "Unsupported action cache version; starting empty"
            );
            return;
        }

        // Oldest last_used first so LRU order matches pre-restart recency.
        parsed.entries.sort_by_key(|e| e.last_used_unix_secs);

        let mut dirty = false;
        let mut evicted = Vec::new();
        let snapshot = {
            let _persist_guard = self.lock_persist();
            let mut st = self.lock_state();

            for item in parsed.entries {
                let Ok(key) = ActionCacheKey::from_hex(&item.key_hex) else {
                    dirty = true;
                    continue;
                };
                if !item.result.is_success() {
                    dirty = true;
                    continue;
                }
                let size_bytes = entry_bytes(&item.result);
                if size_bytes > self.max_bytes {
                    dirty = true;
                    continue;
                }

                let output_ids = unique_output_ids(&item.result);
                if !output_ids.is_empty() {
                    let Some(ref store) = self.blob_store else {
                        dirty = true;
                        continue;
                    };
                    if !store.pin_cache_entry(key.0, &output_ids) {
                        dirty = true;
                        continue;
                    }
                }

                if st.entries.contains_key(&key) {
                    st.remove_key(&key);
                    dirty = true;
                }
                while st.total_bytes.saturating_add(size_bytes) > self.max_bytes {
                    match st.evict_oldest() {
                        Some(old_key) => {
                            evicted.push(old_key);
                            dirty = true;
                        }
                        None => break,
                    }
                }

                let mut normalized = item.result;
                normalized.cached = false;
                st.tick += 1;
                let tick = st.tick;
                st.entries.insert(
                    key,
                    CacheEntry {
                        result: normalized,
                        size_bytes,
                        tick,
                        last_used_unix_secs: item.last_used_unix_secs,
                    },
                );
                st.lru.insert(tick, key);
                st.total_bytes = st.total_bytes.saturating_add(size_bytes);
            }

            if dirty {
                self.snapshot_for_disk(&st)
            } else {
                None
            }
        };

        self.unpin_and_evict_keys(&evicted);
        if let Some(ref snap) = snapshot {
            self.write_snapshot_under_persist_lock(snap);
        }
    }

    fn snapshot_for_disk(&self, st: &CacheState) -> Option<PersistedActionCache> {
        self.persist_path.as_ref()?;
        let entries = st
            .lru
            .values()
            .filter_map(|key| {
                st.entries.get(key).map(|entry| PersistedCacheEntry {
                    key_hex: key.to_hex(),
                    last_used_unix_secs: entry.last_used_unix_secs,
                    result: entry.result.clone(),
                })
            })
            .collect();
        Some(PersistedActionCache {
            version: ACTION_CACHE_PERSIST_VERSION,
            entries,
        })
    }

    fn persist_current(&self) {
        if self.persist_path.is_none() {
            return;
        }
        let snapshot = {
            let st = self.lock_state();
            self.snapshot_for_disk(&st)
        };
        if let Some(ref snap) = snapshot {
            self.write_snapshot_under_persist_lock(snap);
        }
    }

    fn write_snapshot_under_persist_lock(&self, snapshot: &PersistedActionCache) {
        let Some(ref path) = self.persist_path else {
            return;
        };
        let _persist_guard = self.lock_persist();
        if let Err(e) = write_json_atomic(path, snapshot) {
            warn!(path = %path.display(), error = %e, "Failed to persist action cache");
        }
    }
}

fn entry_bytes(result: &TaskResult) -> u64 {
    let mut seen = HashSet::new();
    let mut total = 0u64;
    for out in &result.outputs {
        if seen.insert(out.id) {
            total = total.saturating_add(out.size);
        }
    }
    total = total
        .saturating_add(result.stdout.len() as u64)
        .saturating_add(result.stderr.len() as u64)
        .saturating_add(result.asset_bytes.as_ref().map_or(0, |b| b.len() as u64));
    for art in &result.artifacts {
        total = total.saturating_add(art.data.len() as u64);
    }
    total.max(1)
}

fn unique_output_ids(result: &TaskResult) -> Vec<BlobId> {
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(result.outputs.len());
    for out in &result.outputs {
        if seen.insert(out.id) {
            ids.push(out.id);
        }
    }
    ids
}

fn write_json_atomic(path: &Path, snapshot: &PersistedActionCache) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec(snapshot)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let tmp = path.with_extension("json.tmp");
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob_store::BlobStoreConfig;
    use rusty_grid_core::blob::BlobFile;
    use rusty_grid_core::task::TaskId;
    use uuid::Uuid;

    fn blob_store_in(dir: &Path, max_bytes: u64) -> Arc<BlobStore> {
        let cfg = BlobStoreConfig::new(dir, max_bytes);
        BlobStore::open(&cfg).unwrap()
    }

    #[tokio::test]
    async fn hit_miss_counters_and_failures_are_never_cached() {
        let cache = ActionCache::open(ActionCacheConfig::default(), None);
        let worker = Uuid::new_v4();
        let tid1 = TaskId::new();
        let key1 = ActionCacheKey([1u8; 32]);

        assert!(cache.lookup(&key1).is_none());
        assert_eq!(cache.stats().misses, 1);

        let failed = TaskResult::failure(worker, tid1, 1, "bad", "err", 5, Some("boom".into()));
        assert!(!cache.insert(key1, &failed));
        assert_eq!(cache.stats().entries, 0);

        let ok = TaskResult::success(worker, tid1, "good", 5, false);
        assert!(cache.insert(key1, &ok));
        assert_eq!(cache.stats().entries, 1);

        let hit = cache.lookup(&key1).expect("cache hit");
        assert!(hit.cached);
        assert_eq!(hit.stdout.as_bytes(), b"good");
        assert_eq!(cache.stats().hits, 1);
    }

    #[tokio::test]
    async fn zero_max_bytes_disables_caching_and_oversized_entry_is_skipped() {
        let disabled = ActionCache::open(
            ActionCacheConfig {
                max_bytes: 0,
                persist_path: None,
            },
            None,
        );
        let key = ActionCacheKey([2u8; 32]);
        let res = TaskResult::success(Uuid::new_v4(), TaskId::new(), "hi", 1, false);
        assert!(!disabled.insert(key, &res));
        assert!(disabled.lookup(&key).is_none());

        let tiny = ActionCache::open(
            ActionCacheConfig {
                max_bytes: 4,
                persist_path: None,
            },
            None,
        );
        let big = TaskResult::success(Uuid::new_v4(), TaskId::new(), "0123456789", 1, false);
        assert!(!tiny.insert(key, &big));
        assert_eq!(tiny.stats().entries, 0);
    }

    #[tokio::test]
    async fn lru_eviction_releases_blob_pins_and_persisted_cache_prunes_missing_blobs() {
        let tmp = tempfile::tempdir().unwrap();
        let persist_path = tmp.path().join(ACTION_CACHE_FILE);
        let store = blob_store_in(&tmp.path().join("blobs"), 4096);

        let cache = ActionCache::open(
            ActionCacheConfig {
                max_bytes: 100,
                persist_path: Some(persist_path.clone()),
            },
            Some(Arc::clone(&store)),
        );

        let b1 = store.put(&[b'1'; 60]).await.unwrap();
        let b2 = store.put(&[b'2'; 60]).await.unwrap();
        let k1 = ActionCacheKey([11u8; 32]);
        let k2 = ActionCacheKey([22u8; 32]);

        let r1 =
            TaskResult::success(Uuid::new_v4(), TaskId::new(), "", 1, false).with_outputs(vec![
                BlobFile {
                    path: "one.bin".into(),
                    id: b1,
                    size: 60,
                    executable: false,
                },
            ]);
        let r2 =
            TaskResult::success(Uuid::new_v4(), TaskId::new(), "", 1, false).with_outputs(vec![
                BlobFile {
                    path: "two.bin".into(),
                    id: b2,
                    size: 60,
                    executable: false,
                },
            ]);

        assert!(cache.insert(k1, &r1));
        assert!(cache.insert(k2, &r2));

        // 60 + 60 > 100 -> oldest entry k1 is evicted cleanly and k2 stays.
        assert!(cache.lookup(&k1).is_none());
        assert!(cache.lookup(&k2).is_some());
        assert_eq!(cache.stats().entries, 1);

        // Reopen with same state file + blob store -> k2 survives restart.
        drop(cache);
        let reopened = ActionCache::open(
            ActionCacheConfig {
                max_bytes: 100,
                persist_path: Some(persist_path.clone()),
            },
            Some(Arc::clone(&store)),
        );
        assert!(reopened.lookup(&k2).is_some());
        drop(reopened);

        // Reopen with an empty blob store -> stale persisted entry with missing blob is pruned.
        let empty_store = blob_store_in(&tmp.path().join("blobs-empty"), 4096);
        let pruned = ActionCache::open(
            ActionCacheConfig {
                max_bytes: 100,
                persist_path: Some(persist_path),
            },
            Some(empty_store),
        );
        assert_eq!(pruned.stats().entries, 0);
        assert!(pruned.lookup(&k2).is_none());
    }
}
