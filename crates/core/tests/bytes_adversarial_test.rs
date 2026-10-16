//! Adversarial Stress Verification for `Bytes` in `crates/core/src/task.rs`.
//!
//! Specifically validates behavior without `impl Borrow<str>`:
//! 1. Hashing & HashMap lookup:
//!    - `Bytes` as HashMap key with arbitrary binary and UTF-8 payloads.
//!    - Key lookup using `Bytes` instances.
//!    - Key lookup using `&[u8]` via `Borrow<[u8]>`.
//!    - Hash consistency invariant: `k1 == k2 => hash(k1) == hash(k2)`.
//!    - Sliced buffers: hashing reflects sliced view, not underlying buffer.
//! 2. `PartialEq` semantics:
//!    - `Bytes == Bytes`, `Bytes == &str`, `&str == Bytes`, `Bytes == String`, `String == Bytes`.
//!    - Non-UTF-8 byte exactness: invalid UTF-8 bytes are NEVER equal to any str.
//!    - Partial UTF-8 valid prefix does NOT compare equal to the prefix str.
//! 3. Serde roundtrips:
//!    - JSON serialization and deserialization for UTF-8 strings and arbitrary binary.
//!    - Bincode serialization and deserialization.
//!    - Roundtripping through `TaskResult` and complex container structures.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use rusty_grid_core::task::Bytes;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[test]
fn test_bytes_hash_equality_invariant() {
    // k1 == k2 MUST imply hash(k1) == hash(k2)
    let b1 = Bytes::from("identical_content");
    let b2 = Bytes::copy_from_slice(b"identical_content");
    assert_eq!(b1, b2);
    assert_eq!(calculate_hash(&b1), calculate_hash(&b2));

    // Arbitrary binary payloads
    let bin1 = Bytes::from(vec![0x00, 0xFF, 0xFE, 0xAA, 0x55]);
    let bin2 = Bytes::copy_from_slice(&[0x00, 0xFF, 0xFE, 0xAA, 0x55]);
    assert_eq!(bin1, bin2);
    assert_eq!(calculate_hash(&bin1), calculate_hash(&bin2));

    // Empty payloads
    let empty1 = Bytes::new();
    let empty2 = Bytes::from("");
    let empty3 = Bytes::from(Vec::<u8>::new());
    assert_eq!(empty1, empty2);
    assert_eq!(empty2, empty3);
    assert_eq!(calculate_hash(&empty1), calculate_hash(&empty2));
    assert_eq!(calculate_hash(&empty2), calculate_hash(&empty3));
}

#[test]
fn test_bytes_hash_distinguishes_distinct_payloads() {
    let mut hashes = HashSet::new();
    let test_cases = [
        Bytes::from(""),
        Bytes::from("a"),
        Bytes::from("b"),
        Bytes::from("ab"),
        Bytes::from("ba"),
        Bytes::from(vec![0x00]),
        Bytes::from(vec![0x00, 0x00]),
        Bytes::from(vec![0xFF]),
        Bytes::from(vec![0xFF, 0xFE]),
        Bytes::from(vec![0xFE, 0xFF]),
        Bytes::from("long prefix text that is quite extensive"),
        Bytes::from("long prefix text that is quite extensive!"),
    ];

    for case in &test_cases {
        hashes.insert(calculate_hash(case));
    }

    // Hash collisions are statistically improbable across these 12 distinct inputs
    assert_eq!(
        hashes.len(),
        test_cases.len(),
        "Hash function should produce distinct values for distinct inputs"
    );
}

#[test]
fn test_bytes_hashmap_and_hashset_operations() {
    let mut map: HashMap<Bytes, usize> = HashMap::new();

    let keys = [
        Bytes::from("command_stdout"),
        Bytes::from("command_stderr"),
        Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        Bytes::from(vec![0x80, 0x81, 0x82]),
        Bytes::from(""),
    ];

    for (i, key) in keys.iter().enumerate() {
        map.insert(key.clone(), i);
    }

    assert_eq!(map.len(), keys.len());

    // 1. Lookup using exact Bytes keys
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(map.get(key), Some(&i));
        assert!(map.contains_key(key));
    }

    // 2. Lookup using &[u8] via Borrow<[u8]>
    assert_eq!(map.get(b"command_stdout".as_slice()), Some(&0));
    assert_eq!(map.get(b"command_stderr".as_slice()), Some(&1));
    assert_eq!(map.get([0xDE, 0xAD, 0xBE, 0xEF].as_slice()), Some(&2));
    assert_eq!(map.get([0x80, 0x81, 0x82].as_slice()), Some(&3));
    assert_eq!(map.get(b"".as_slice()), Some(&4));
    assert_eq!(map.get(b"non_existent".as_slice()), None);

    // 3. Remove by &[u8]
    assert_eq!(map.remove(b"command_stdout".as_slice()), Some(0));
    assert_eq!(map.len(), keys.len() - 1);
    assert_eq!(map.get(b"command_stdout".as_slice()), None);
}

#[test]
fn test_bytes_sliced_buffer_hashing_and_equality() {
    let full = Bytes::from("0123456789abcdef");
    let slice1 = full.slice(0..10);
    let slice2 = full.slice(10..16);

    let expected1 = Bytes::from("0123456789");
    let expected2 = Bytes::from("abcdef");

    assert_eq!(slice1, expected1);
    assert_eq!(slice2, expected2);
    assert_ne!(slice1, slice2);

    assert_eq!(calculate_hash(&slice1), calculate_hash(&expected1));
    assert_eq!(calculate_hash(&slice2), calculate_hash(&expected2));
}

#[test]
fn test_bytes_partial_eq_adversarial_matrix() {
    // 1. Equal UTF-8
    let b = Bytes::from("hello world");
    assert_eq!(b, "hello world");
    assert_eq!("hello world", b);
    assert_eq!(b, String::from("hello world"));
    assert_eq!(String::from("hello world"), b);

    // 2. Non-equal UTF-8
    assert_ne!(b, "hello");
    assert_ne!("hello", b);
    assert_ne!(b, "hello world!");
    assert_ne!("hello world!", b);

    // 3. Invalid UTF-8 must NEVER equal any string (even empty string or invalid-char replacement)
    let invalid = Bytes::from(vec![0xFF, 0xFE, 0xFD]);
    assert_ne!(invalid, "");
    assert_ne!("", invalid);
    assert_ne!(invalid, "\u{FFFD}\u{FFFD}\u{FFFD}");
    assert_ne!("\u{FFFD}\u{FFFD}\u{FFFD}", invalid);

    // 4. Truncated multibyte UTF-8 sequence
    let mut broken_emoji = b"valid prefix ".to_vec();
    broken_emoji.extend_from_slice(&"\u{1F980}".as_bytes()[..2]); // truncated crab emoji
    let b_broken = Bytes::from(broken_emoji);

    assert_ne!(b_broken, "valid prefix ");
    assert_ne!("valid prefix ", b_broken);
    assert_ne!(b_broken, "valid prefix \u{1F980}");

    // 5. Embedded null bytes
    let b_null = Bytes::from(vec![b'a', 0x00, b'b']);
    assert_eq!(b_null, "a\0b");
    assert_ne!(b_null, "ab");
    assert_ne!(b_null, "a");
}

#[test]
fn test_bytes_serde_json_human_readable_roundtrips() {
    // 1. Clean UTF-8 string -> serializes as JSON string
    let original_utf8 = Bytes::from("standard utf8 string with emojis \u{1F980}\u{2728}");
    let json_utf8 = serde_json::to_string(&original_utf8).expect("serialize utf8 json");
    assert!(json_utf8.starts_with('"'));
    let roundtripped_utf8: Bytes =
        serde_json::from_str(&json_utf8).expect("deserialize utf8 json");
    assert_eq!(original_utf8, roundtripped_utf8);
    assert_eq!(original_utf8.as_bytes(), roundtripped_utf8.as_bytes());

    // 2. Raw binary non-UTF8 -> serializes as byte array in JSON
    let original_raw = Bytes::from(vec![0x00, 0x80, 0xFF, 0xFE, 0x42]);
    let json_raw = serde_json::to_string(&original_raw).expect("serialize raw json");
    let roundtripped_raw: Bytes = serde_json::from_str(&json_raw).expect("deserialize raw json");
    assert_eq!(original_raw, roundtripped_raw);
    assert_eq!(original_raw.as_bytes(), roundtripped_raw.as_bytes());

    // 3. Deserialization from JSON byte array [10, 20, 30]
    let json_seq = "[10, 20, 30]";
    let from_seq: Bytes = serde_json::from_str(json_seq).expect("deserialize seq");
    assert_eq!(from_seq.as_bytes(), &[10, 20, 30]);

    // 4. Empty string and empty array
    let empty_str_json = "\"\"";
    let from_empty_str: Bytes = serde_json::from_str(empty_str_json).unwrap();
    assert_eq!(from_empty_str.len(), 0);

    let empty_seq_json = "[]";
    let from_empty_seq: Bytes = serde_json::from_str(empty_seq_json).unwrap();
    assert_eq!(from_empty_seq.len(), 0);
}

#[test]
fn test_bytes_bincode_binary_roundtrips() {
    // Test a matrix of payloads: empty, short ASCII, unicode, non-UTF8 binary, large payload
    let payloads = vec![
        Bytes::new(),
        Bytes::from("hello bincode"),
        Bytes::from("🦀 OxideSwarm Distributed Grid 🚀"),
        Bytes::from(vec![0x00, 0x01, 0x02, 0xFE, 0xFF]),
        Bytes::from(vec![0xAA; 16384]), // 16KB payload
    ];

    for (idx, original) in payloads.into_iter().enumerate() {
        let encoded = bincode::serialize(&original)
            .unwrap_or_else(|e| panic!("Failed to bincode serialize payload #{idx}: {e}"));
        let decoded: Bytes = bincode::deserialize(&encoded)
            .unwrap_or_else(|e| panic!("Failed to bincode deserialize payload #{idx}: {e}"));
        assert_eq!(
            original, decoded,
            "Bincode roundtrip mismatch for payload #{idx}"
        );
        assert_eq!(
            original.as_bytes(),
            decoded.as_bytes(),
            "Raw byte mismatch for payload #{idx}"
        );
    }
}

#[test]
fn test_bytes_inside_task_result_roundtrip() {
    use rusty_grid_core::task::{TaskId, TaskResult};
    use uuid::Uuid;

    let worker_id = Uuid::new_v4();
    let task_id = TaskId::new();
    let stdout_bytes = Bytes::from(vec![b'O', b'K', 0xFF, 0xFE, b'!']);
    let stderr_bytes = Bytes::from("warnings encountered");

    let mut result = TaskResult::success(worker_id, task_id, stdout_bytes.clone(), 42, false);
    result.stderr = stderr_bytes.clone();

    // JSON roundtrip
    let json = serde_json::to_string(&result).expect("TaskResult JSON serialize");
    let result_from_json: TaskResult =
        serde_json::from_str(&json).expect("TaskResult JSON deserialize");
    assert_eq!(result.stdout, result_from_json.stdout);
    assert_eq!(result.stderr, result_from_json.stderr);
    assert_eq!(result.exit_code, result_from_json.exit_code);

    // Bincode roundtrip
    let bin = bincode::serialize(&result).expect("TaskResult Bincode serialize");
    let result_from_bin: TaskResult =
        bincode::deserialize(&bin).expect("TaskResult Bincode deserialize");
    assert_eq!(result.stdout, result_from_bin.stdout);
    assert_eq!(result.stderr, result_from_bin.stderr);
    assert_eq!(result.exit_code, result_from_bin.exit_code);
}
