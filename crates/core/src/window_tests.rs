//! Cross-version tests of the compatibility window (design 0083 D7).
//!
//! `tests/data/wire_layout_previous.txt` is the `wire_layout.txt` of the latest release that speaks
//! `PROTOCOL_VERSION_CURRENT - 1` (v0.3.0, protocol 9), copied from its tag. It is the oracle:
//! the bytes a v9 peer really sent and really read, so no encoder written here can drift from
//! them. The stamp of a `ClientMessage` / `ClientResponse` entry is its last four bytes; the
//! current file records it as `0`, the previous one still holds the stamp 9.

use std::collections::BTreeMap;
use std::path::Path;

use crate::protocol::{
    deserialize_message, deserialize_message_as_v9, deserialize_message_with_version,
    serialize_message_for_peer, ClientMessage, ClientResponse, MasterMessage, WireCodec,
    WorkerMessage, PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE, WIRE_FORMAT_BINCODE,
};
use crate::task::Task;
use crate::wire_layout_tests::{
    client_messages, client_responses, current_wire_layout, layout_differences, master_messages,
    parse_wire_layout_from, to_hex, verify_previous_layout_file, verify_window_range,
    worker_messages, PREVIOUS_WIRE_LAYOUT_FILE, RECORDED_STAMP,
};

/// The version the previous file records.
const PREVIOUS: u32 = 9;

/// Entries of the current layout that differ from the previous release's, with the reason. A
/// fourth entry that changes without being listed fails the test, and so does a listed one that no
/// longer differs (design 0083 D7.3).
const DIFFERS_FROM_PREVIOUS: &[(&str, &str)] = &[
    (
        "ClientMessage::SubmitTask",
        "carries a Task, which gained the `extensions` lists in protocol 10",
    ),
    (
        "MasterMessage::AssignTask",
        "carries a Task, which gained the `extensions` lists in protocol 10",
    ),
    (
        "MasterMessage::AssignTaskWithCheckpoint",
        "carries a Task, which gained the `extensions` lists in protocol 10",
    ),
    (
        "MasterMessage::RoleHandoverAck",
        "holds a Vec<Task>, and Task gained the `extensions` lists in protocol 10",
    ),
];

fn previous_text() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(PREVIOUS_WIRE_LAYOUT_FILE);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn previous_layout() -> BTreeMap<String, String> {
    parse_wire_layout_from(&previous_text(), PREVIOUS_WIRE_LAYOUT_FILE)
}

/// `0x02` plus the recorded bytes: the frame a peer sent.
fn frame_of(hex: &str) -> Vec<u8> {
    let mut frame = vec![WIRE_FORMAT_BINCODE];
    frame.extend(
        (0..hex.len() / 2).map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap()),
    );
    frame
}

/// The recorded hex without its trailing four-byte version stamp.
fn without_stamp(hex: &str) -> &str {
    &hex[..hex.len() - 8]
}

fn is_stamped(key: &str) -> bool {
    key.starts_with("ClientMessage::") || key.starts_with("ClientResponse::")
}

/// A canonical sample as a v9 peer would have received it: no extensions.
fn clear_extensions(task: &mut Task) {
    task.extensions.clear();
    task.requirements.extensions.clear();
}

#[test]
fn previous_file_is_the_release_the_window_reaches_back_to() {
    let text = previous_text();
    verify_previous_layout_file(
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        Some(&text),
    )
    .unwrap();
    assert_eq!(PROTOCOL_VERSION_MIN_COMPATIBLE, PREVIOUS);
}

/// D7.1: every message a v9 peer sends is read by the current decoder, and what comes out is the
/// canonical sample (with no extensions for the `Task` carrier).
#[test]
fn peer_to_master_previous_bytes_decode_to_the_canonical_samples() {
    let previous = previous_layout();
    let mut checked = 0;

    for (variant, expected) in worker_messages() {
        let key = format!("WorkerMessage::{variant}");
        let Some(hex) = previous.get(&key) else {
            continue;
        };
        let (decoded, codec) = deserialize_message::<WorkerMessage>(&frame_of(hex))
            .unwrap_or_else(|e| panic!("{key}: {e}"));
        assert_eq!(codec, WireCodec::Bincode);
        assert_eq!(decoded, expected, "{key}");
        checked += 1;
    }

    for (variant, mut expected) in client_messages() {
        let key = format!("ClientMessage::{variant}");
        let hex = previous
            .get(&key)
            .unwrap_or_else(|| panic!("{key} is not in the previous file"));
        let (decoded, codec, stamp) =
            deserialize_message_with_version::<ClientMessage>(&frame_of(hex))
                .unwrap_or_else(|e| panic!("{key}: {e}"));
        assert_eq!(codec, WireCodec::Bincode);
        assert_eq!(stamp, Some(PREVIOUS), "{key}: the stamp of the v9 peer");
        if let ClientMessage::SubmitTask { task, .. } = &mut expected {
            clear_extensions(task);
        }
        assert_eq!(decoded, expected, "{key}");
        checked += 1;
    }
    assert!(checked > 20, "only {checked} previous entries were decoded");
}

/// D7.2: what the master sends a v9 peer is exactly what v0.3.0 recorded, byte for byte.
#[test]
fn master_to_peer_current_samples_encode_as_the_previous_bytes() {
    let previous = previous_layout();
    let mut checked = 0;

    for (variant, msg) in master_messages() {
        let key = format!("MasterMessage::{variant}");
        let Some(recorded) = previous.get(&key) else {
            continue;
        };
        let frame = serialize_message_for_peer(&msg, WireCodec::Bincode, PREVIOUS).unwrap();
        let mut expected = recorded.clone();
        if let MasterMessage::RegisterAck {
            accepted: true,
            protocol_version,
            ..
        } = &msg
        {
            // The sample's `protocol_version` is a marker; the ack echoes the peer's version.
            let marker = to_hex(&protocol_version.to_le_bytes());
            assert_eq!(
                expected.matches(&marker).count(),
                1,
                "{key}: version marker"
            );
            expected = expected.replace(&marker, &to_hex(&PREVIOUS.to_le_bytes()));
        }
        assert_eq!(to_hex(&frame[1..]), expected, "{key}");

        // The frozen v9 reader reads it back as the message with its task for a v9 peer.
        let (read, _) = deserialize_message_as_v9::<MasterMessage>(&frame).unwrap();
        let mut want = msg.clone();
        match &mut want {
            MasterMessage::RegisterAck {
                accepted: true,
                protocol_version,
                ..
            } => *protocol_version = PREVIOUS,
            MasterMessage::AssignTask { task }
            | MasterMessage::AssignTaskWithCheckpoint { task, .. } => {
                *task = task.for_peer(PREVIOUS)
            }
            MasterMessage::RoleHandoverAck {
                task_queue_snapshot,
                ..
            } => {
                for t in task_queue_snapshot {
                    *t = t.for_peer(PREVIOUS)
                }
            }
            _ => {}
        }
        assert_eq!(read, want, "{key}: the v9 reader");
        checked += 1;
    }

    for (variant, resp) in client_responses() {
        let key = format!("ClientResponse::{variant}");
        let recorded = previous
            .get(&key)
            .unwrap_or_else(|| panic!("{key} is not in the previous file"));
        let frame = serialize_message_for_peer(&resp, WireCodec::Bincode, PREVIOUS).unwrap();
        let mut expected = recorded.clone();
        if matches!(resp, ClientResponse::Error { .. }) {
            // `Error` keeps stamping the current version, which a v9 peer reads without a check.
            expected = format!(
                "{}{}",
                without_stamp(&expected),
                to_hex(&PROTOCOL_VERSION_CURRENT.to_le_bytes())
            );
        }
        assert_eq!(to_hex(&frame[1..]), expected, "{key}");
        let (read, _) = deserialize_message_as_v9::<ClientResponse>(&frame)
            .unwrap_or_else(|e| panic!("{key}: the v9 reader: {e}"));
        assert_eq!(read, resp, "{key}: the v9 reader");
        checked += 1;
    }
    assert!(checked > 25, "only {checked} previous entries were encoded");
}

/// D7.3: the entries that differ are the ones listed, and only those.
#[test]
fn only_the_listed_entries_differ_from_the_previous_release() {
    let previous = previous_layout();
    // The stamps are normalised: the previous file records 9, the current one 0.
    let normalise = |layout: &BTreeMap<String, String>| -> BTreeMap<String, String> {
        layout
            .iter()
            .map(|(k, v)| {
                let v = if is_stamped(k) {
                    without_stamp(v).to_string()
                } else {
                    v.clone()
                };
                (k.clone(), v)
            })
            .collect()
    };
    for (key, hex) in &previous {
        if is_stamped(key) {
            assert!(
                hex.ends_with(&to_hex(&PREVIOUS.to_le_bytes())),
                "{key}: the previous file stamps {PREVIOUS}"
            );
        }
    }
    let current = current_wire_layout();
    for (key, hex) in &current {
        if is_stamped(key) {
            assert!(
                hex.ends_with(&to_hex(&RECORDED_STAMP.to_le_bytes())),
                "{key}: the current file records the stamp {RECORDED_STAMP}"
            );
        }
    }
    let current_n = normalise(&current);
    let previous_n = normalise(&previous);
    // `layout_differences` prints `Enum::Variant: ...`; keep `Enum::Variant`.
    let mut differing: Vec<String> = layout_differences(&previous_n, &current_n)
        .iter()
        .map(|d| d.split(": ").next().unwrap().to_string())
        .collect();
    differing.sort();
    let mut listed: Vec<String> = DIFFERS_FROM_PREVIOUS
        .iter()
        .map(|(k, _)| k.to_string())
        .collect();
    listed.sort();
    assert_eq!(
        differing, listed,
        "the entries that differ from {PREVIOUS_WIRE_LAYOUT_FILE} must be exactly the \
         DIFFERS_FROM_PREVIOUS list: a new difference needs a reason there, a listed entry that \
         no longer differs must go"
    );
}

/// D1.4 / D7.4: the window rule as unit tests.
#[test]
fn window_rule_and_previous_file_rule() {
    let previous = previous_text();
    let closed_current = format!("# protocol_version: {PREVIOUS}\n");

    // Window open (MIN = CURRENT - 1) with the previous file: fine.
    verify_previous_layout_file(10, 9, Some(&previous)).unwrap();
    // Window open without a previous file: refused, and the message says what to copy.
    let err = verify_previous_layout_file(10, 9, None).unwrap_err();
    assert!(err.contains("git show"), "{err}");
    // Window closed with a previous file left over: refused.
    let err = verify_previous_layout_file(10, 10, Some(&previous)).unwrap_err();
    assert!(err.contains("must be deleted"), "{err}");
    // Window closed without one: fine.
    verify_previous_layout_file(10, 10, None).unwrap();
    // A previous file for the wrong version: refused.
    assert!(verify_previous_layout_file(11, 10, Some(&closed_current)).is_err());
    // More than one version back: refused.
    assert!(verify_window_range(11, 9).is_err());
    assert!(verify_previous_layout_file(11, 9, Some(&previous)).is_err());
    // MIN above CURRENT: refused.
    assert!(verify_window_range(10, 11).is_err());
}
