//! The task extension slot (design 0082): `Extension`, the registry of ids, the limits and the
//! receiver rules, re-exported from [`crate::task`].

use super::{Task, TaskId};
use serde::{Deserialize, Serialize};

/// Most extensions in one list (`Task::extensions` or `TaskRequirements::extensions`).
pub const MAX_EXTENSIONS_PER_LIST: usize = 16;
/// Most bytes in the value of one extension.
pub const MAX_EXTENSION_BYTES: usize = 64 * 1024;
/// Most extension value bytes in one task, both lists together.
pub const MAX_TASK_EXTENSION_BYTES: usize = 256 * 1024;
/// First id of the experimental range (`0x8000..=0xFFFF`): no release build assigns one.
pub const EXPERIMENTAL_EXTENSION_ID_MIN: u16 = 0x8000;

/// One optional field of a task that an older build need not understand (design 0082 D1).
///
/// A receiver that does not know `id` ignores a non-critical extension (and keeps it when it
/// relays the task) and refuses a task with a critical one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extension {
    /// Registry id ([`EXTENSION_REGISTRY`]); `0` is invalid.
    pub id: u16,
    /// `true`: a receiver that does not know `id` must refuse the task.
    pub critical: bool,
    /// The value; its layout belongs to the registry entry of `id`.
    #[serde(with = "extension_bytes")]
    pub bytes: Vec<u8>,
}

/// Serde helper for [`Extension::bytes`]: lowercase hex in JSON, the plain `Vec<u8>` layout
/// (length, then the bytes) in bincode.
pub mod extension_bytes {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&crate::cluster_auth::hex_encode(bytes))
        } else {
            serializer.serialize_bytes(bytes)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        struct HexVisitor;
        impl serde::de::Visitor<'_> for HexVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a hexadecimal string")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                crate::cluster_auth::hex_decode(v)
                    .ok_or_else(|| E::custom("extension bytes are not valid hexadecimal"))
            }
        }
        struct BytesVisitor;
        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a byte array")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(v.to_vec())
            }

            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(v)
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(HexVisitor)
        } else {
            deserializer.deserialize_byte_buf(BytesVisitor)
        }
    }
}

/// Which extension list an id belongs to (design 0082 D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionList {
    /// [`Task::extensions`]: what the worker does with the task.
    Task,
    /// [`TaskRequirements::extensions`]: what the scheduler matches on.
    Requirements,
}

impl ExtensionList {
    pub fn as_str(self) -> &'static str {
        match self {
            ExtensionList::Task => "Task",
            ExtensionList::Requirements => "Requirements",
        }
    }
}

/// Who reads an extension (design 0082 D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionScope {
    /// Only the master's scheduler or admission reads it; the master strips it from the copy of
    /// the task it sends to a worker.
    Master,
    /// Only the worker reads it: a worker must advertise `ext:<id>` to receive the task.
    Worker,
    /// Both read it.
    Both,
}

impl ExtensionScope {
    /// True when a worker must understand the extension.
    pub fn needs_worker(self) -> bool {
        matches!(self, ExtensionScope::Worker | ExtensionScope::Both)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ExtensionScope::Master => "Master",
            ExtensionScope::Worker => "Worker",
            ExtensionScope::Both => "Both",
        }
    }
}

/// One entry of the extension registry (design 0082 D3).
#[derive(Debug, Clone, Copy)]
pub struct ExtensionInfo {
    /// `0` is invalid; `1..=0x7FFF` are assigned by a design record, in order;
    /// `0x8000..=0xFFFF` are experimental and used by tests.
    pub id: u16,
    /// Short lowercase name for logs and error messages.
    pub name: &'static str,
    /// The list the id belongs to.
    pub list: ExtensionList,
    /// The value `critical` must carry (anything that restricts where or how a task runs is
    /// critical; a hint is not).
    pub critical: bool,
    /// Who reads the extension.
    pub scope: ExtensionScope,
    /// Checks that `bytes` hold a valid value of this extension.
    pub decode: fn(&[u8]) -> Result<(), String>,
}

/// The extensions this build knows. Empty until the first feature that needs one; the wire layout
/// test records this table. The first real id comes with its design record.
pub const EXTENSION_REGISTRY: &[ExtensionInfo] = &[];

/// Test-only registry (experimental id range), visible to this crate's tests and, through the
/// `test-extensions` feature, to the dev-dependencies of the master and worker crates.
#[cfg(any(test, feature = "test-extensions"))]
pub const TEST_EXTENSION_REGISTRY: &[ExtensionInfo] = &[
    ExtensionInfo {
        id: 0x8001,
        name: "test-hint",
        list: ExtensionList::Task,
        critical: false,
        scope: ExtensionScope::Worker,
        decode: test_decode,
    },
    ExtensionInfo {
        id: 0x8002,
        name: "test-worker-limit",
        list: ExtensionList::Task,
        critical: true,
        scope: ExtensionScope::Worker,
        decode: test_decode,
    },
    ExtensionInfo {
        id: 0x8003,
        name: "test-master-limit",
        list: ExtensionList::Task,
        critical: true,
        scope: ExtensionScope::Master,
        decode: test_decode,
    },
    ExtensionInfo {
        id: 0x8004,
        name: "test-requirement",
        list: ExtensionList::Requirements,
        critical: true,
        scope: ExtensionScope::Both,
        decode: test_decode,
    },
];

/// Test decoder: any value but a single `0xFF` byte is valid.
#[cfg(any(test, feature = "test-extensions"))]
fn test_decode(bytes: &[u8]) -> Result<(), String> {
    if bytes == [0xFF] {
        Err("the value 0xFF is invalid".to_string())
    } else {
        Ok(())
    }
}

/// The registry entry of `id`, if this build knows it.
pub fn extension_info(id: u16) -> Option<&'static ExtensionInfo> {
    let found = EXTENSION_REGISTRY.iter().find(|e| e.id == id);
    #[cfg(any(test, feature = "test-extensions"))]
    let found = found.or_else(|| TEST_EXTENSION_REGISTRY.iter().find(|e| e.id == id));
    found
}

/// The worker tags (`ext:<id>`, decimal) for every extension a worker of this build understands:
/// those whose scope needs a worker (design 0082 D4).
pub fn worker_extension_tags() -> Vec<String> {
    let all = EXTENSION_REGISTRY.iter();
    #[cfg(any(test, feature = "test-extensions"))]
    let all = all.chain(TEST_EXTENSION_REGISTRY.iter());
    let mut ids: Vec<u16> = all
        .filter(|e| e.scope.needs_worker())
        .map(|e| e.id)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids.into_iter()
        .map(|id| format!("{}{id}", crate::placement::EXT_TAG_PREFIX))
        .collect()
}

/// Checks the two lists of task `task_id` as `enforcer` (`"master"` or `"worker"`, for messages).
///
/// Order (design 0082 D2): bounds and ids first, so a malformed list never reaches the
/// per-extension rules; then, per extension, known ids against the registry (list, `critical`,
/// value) and unknown critical ids refused. An unknown non-critical id is accepted untouched.
fn check_extension_lists(
    task_id: TaskId,
    task_list: &[Extension],
    requirement_list: &[Extension],
    enforcer: &str,
) -> Result<(), String> {
    let lists = [
        (ExtensionList::Task, task_list),
        (ExtensionList::Requirements, requirement_list),
    ];
    let mut total: usize = 0;
    for (list, items) in lists {
        if items.len() > MAX_EXTENSIONS_PER_LIST {
            return Err(format!(
                "Task {task_id}: {} extensions in the {} list; the limit is {MAX_EXTENSIONS_PER_LIST}",
                items.len(),
                list.as_str()
            ));
        }
        let mut seen: Vec<u16> = Vec::with_capacity(items.len());
        for ext in items {
            if ext.id == 0 {
                return Err(format!("Task {task_id}: extension id 0 is invalid"));
            }
            if ext.bytes.len() > MAX_EXTENSION_BYTES {
                return Err(format!(
                    "Task {task_id}: extension {} is {} bytes long; the limit is {MAX_EXTENSION_BYTES}",
                    ext.id,
                    ext.bytes.len()
                ));
            }
            if seen.contains(&ext.id) {
                return Err(format!(
                    "Task {task_id}: extension {} appears more than once in the {} list",
                    ext.id,
                    list.as_str()
                ));
            }
            seen.push(ext.id);
            total += ext.bytes.len();
        }
    }
    if total > MAX_TASK_EXTENSION_BYTES {
        return Err(format!(
            "Task {task_id}: the extensions hold {total} bytes; the limit is {MAX_TASK_EXTENSION_BYTES}"
        ));
    }
    for (list, items) in lists {
        for ext in items {
            match extension_info(ext.id) {
                Some(info) => {
                    if info.list != list {
                        return Err(format!(
                            "Task {task_id}: extension {} ({}) belongs in the {} list, not the {} list",
                            ext.id,
                            info.name,
                            info.list.as_str(),
                            list.as_str()
                        ));
                    }
                    if info.critical != ext.critical {
                        return Err(format!(
                            "Task {task_id}: extension {} ({}) must have critical = {}",
                            ext.id, info.name, info.critical
                        ));
                    }
                    (info.decode)(&ext.bytes).map_err(|e| {
                        format!(
                            "Task {task_id}: extension {} ({}) is invalid: {e}",
                            ext.id, info.name
                        )
                    })?;
                }
                None if ext.critical => {
                    return Err(format!(
                        "Task {task_id} needs extension {}, which this {enforcer} does not support",
                        ext.id
                    ));
                }
                None => {}
            }
        }
    }
    Ok(())
}

impl Task {
    /// Every extension of the task, `Task` list first.
    pub fn all_extensions(&self) -> impl Iterator<Item = &Extension> {
        self.extensions
            .iter()
            .chain(self.requirements.extensions.iter())
    }

    /// Checks the extension lists as `enforcer` (`"master"` at submit, `"worker"` at assignment;
    /// design 0082 D2).
    pub fn validate_extensions(&self, enforcer: &str) -> Result<(), String> {
        check_extension_lists(
            self.id,
            &self.extensions,
            &self.requirements.extensions,
            enforcer,
        )
    }

    /// The ids of the known critical extensions that a worker must understand (scope `Worker` or
    /// `Both`), sorted and without duplicates. The scheduler places the task only on workers that
    /// advertise `ext:<id>` for each (design 0082 D4).
    pub fn required_extension_ids(&self) -> Vec<u16> {
        let mut ids: Vec<u16> = self
            .all_extensions()
            .filter(|e| e.critical)
            .filter(|e| extension_info(e.id).is_some_and(|i| i.scope.needs_worker()))
            .map(|e| e.id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The constraints the scheduler places this task by: the `require:` tags
    /// ([`Task::placement`]) plus an `ext:<id>` requirement for each id of
    /// [`Task::required_extension_ids`] (design 0082 D4).
    pub fn scheduling_placement(&self) -> Result<crate::placement::Placement, String> {
        let mut placement = self.placement()?;
        placement.require_extensions(&self.required_extension_ids());
        Ok(placement)
    }

    /// Removes the extensions whose scope is `Master` (the worker cannot enforce them and would
    /// refuse a critical one as unknown); the master keeps the stored task whole.
    pub fn strip_master_extensions(&mut self) {
        let keep = |e: &Extension| {
            !extension_info(e.id).is_some_and(|i| i.scope == ExtensionScope::Master)
        };
        self.extensions.retain(keep);
        self.requirements.extensions.retain(keep);
    }

    /// True when this task must never be sent to a peer speaking protocol `version`: it carries a
    /// critical `Worker`/`Both` extension (the receiver must enforce it) and the peer is below the
    /// extension protocol, so `for_peer` would strip the restriction (design 0083, defense in
    /// depth: placement already keeps such a task off a v9 worker).
    pub fn withheld_from_peer(&self, version: u32) -> bool {
        version < EXTENSION_PROTOCOL_VERSION && !self.required_extension_ids().is_empty()
    }

    /// The copy of this task a peer speaking protocol `version` may receive (design 0083 D3).
    ///
    /// `Master`-scope extensions are removed. For `version` below 10 (the layout before the
    /// extension slot) both extension lists are cleared: a v9 worker never advertises `ext:<id>`,
    /// so a task with a critical `Worker`/`Both` extension is never placed on it, and what is left
    /// is non-critical, which is defined as ignorable. The stored task keeps every extension; only
    /// this copy for the wire changes.
    pub fn for_peer(&self, version: u32) -> Task {
        let mut copy = self.clone();
        copy.strip_master_extensions();
        if version < EXTENSION_PROTOCOL_VERSION {
            copy.extensions.clear();
            copy.requirements.extensions.clear();
        }
        copy
    }
}

/// The first protocol version whose `Task` and `TaskRequirements` carry `extensions`.
pub const EXTENSION_PROTOCOL_VERSION: u32 = 10;
