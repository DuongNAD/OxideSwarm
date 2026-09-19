//! Empirical validation of error propagation, mapping, exit codes, and transient classifications.
//!
//! Verifies:
//! 1. All std::io::Error kinds mapped to GridError
//! 2. All serde_json::Error categories mapped to GridError
//! 3. All ProtocolError variants mapped to GridError
//! 4. Identification of classification anomalies (e.g. BrokenPipe and UnexpectedEof)

use rusty_grid_core::error::GridError;
use rusty_grid_core::protocol::{ProtocolError, WorkerMessage};
use std::io;

#[test]
fn test_all_io_error_kinds_mapping_and_exit_codes() {
    let test_kinds = [
        (io::ErrorKind::NotFound, false),
        (io::ErrorKind::PermissionDenied, false),
        (io::ErrorKind::ConnectionRefused, false),
        (io::ErrorKind::ConnectionReset, true),
        (io::ErrorKind::ConnectionAborted, true),
        (io::ErrorKind::BrokenPipe, false), // EMPIRICAL ANOMALY: BrokenPipe should be transient in network RPC
        (io::ErrorKind::TimedOut, true),
        (io::ErrorKind::WouldBlock, true),
        (io::ErrorKind::Interrupted, true),
        (io::ErrorKind::UnexpectedEof, false), // EMPIRICAL ANOMALY: UnexpectedEof is false here, but true in ConnectionClosed
        (io::ErrorKind::Other, false),
    ];

    for (kind, expected_transient) in test_kinds {
        let raw_io = io::Error::new(kind, format!("test {kind:?}"));
        let grid_err: GridError = raw_io.into();

        // 1. Variant must be GridError::Io
        assert!(
            matches!(&grid_err, GridError::Io(_)),
            "Expected GridError::Io for kind {kind:?}"
        );

        // 2. Stable error code must be IO_ERROR
        assert_eq!(
            grid_err.error_code(),
            "IO_ERROR",
            "Error code mismatch for {kind:?}"
        );

        // 3. Exit code must be 3 for all I/O errors
        assert_eq!(
            grid_err.exit_code(),
            3,
            "Exit code mismatch for {kind:?}: expected 3, got {}",
            grid_err.exit_code()
        );

        // 4. Transient classification
        assert_eq!(
            grid_err.is_transient(),
            expected_transient,
            "Transient classification mismatch for kind {kind:?}"
        );
    }
}

#[test]
fn test_all_serde_json_error_categories_mapping() {
    let bad_payloads = [
        ("syntax error", "not a json string"),
        ("truncated json", "{\"type\": \"Register\", \"worker_id\":"),
        ("missing fields", "{\"type\": \"Register\"}"),
        (
            "type mismatch",
            "{\"type\": \"Register\", \"worker_id\": 12345}",
        ),
        ("invalid enum variant", "{\"type\": \"UnknownVariant\"}"),
    ];

    for (desc, payload) in bad_payloads {
        let parse_res = serde_json::from_str::<WorkerMessage>(payload);
        assert!(parse_res.is_err(), "Payload '{desc}' should have failed");
        let json_err = parse_res.unwrap_err();

        // Direct conversion
        let grid_err: GridError = json_err.into();

        assert!(
            matches!(&grid_err, GridError::Serialization(_)),
            "Expected Serialization variant for {desc}"
        );
        assert_eq!(
            grid_err.error_code(),
            "SERIALIZATION_ERROR",
            "Error code mismatch for {desc}"
        );
        assert_eq!(
            grid_err.exit_code(),
            1,
            "Exit code mismatch for {desc}: expected 1, got {}",
            grid_err.exit_code()
        );
        assert!(
            !grid_err.is_transient(),
            "Serialization error should not be transient for {desc}"
        );
    }
}

#[test]
fn test_protocol_error_to_grid_error_exhaustive_mapping() {
    // 1. ProtocolError::Io
    let io_e = ProtocolError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
    let g_io: GridError = io_e.into();
    assert_eq!(g_io.error_code(), "IO_ERROR");
    assert_eq!(g_io.exit_code(), 3);
    assert!(g_io.is_transient());

    // 2. ProtocolError::Json
    let json_err = serde_json::from_str::<WorkerMessage>("{").unwrap_err();
    let p_json = ProtocolError::Json(json_err);
    let g_json: GridError = p_json.into();
    assert_eq!(g_json.error_code(), "SERIALIZATION_ERROR");
    assert_eq!(g_json.exit_code(), 1);
    assert!(!g_json.is_transient());

    // 3. ProtocolError::FrameTooLarge
    let p_ftl = ProtocolError::FrameTooLarge {
        size: 70_000_000,
        max: 64_000_000,
    };
    let g_ftl: GridError = p_ftl.into();
    assert_eq!(g_ftl.error_code(), "FRAME_TOO_LARGE");
    assert_eq!(g_ftl.exit_code(), 1);
    assert!(!g_ftl.is_transient());

    // 4. ProtocolError::UnexpectedEof
    let p_ueof = ProtocolError::UnexpectedEof;
    let g_ueof: GridError = p_ueof.into();
    assert_eq!(g_ueof.error_code(), "CONNECTION_CLOSED");
    assert_eq!(g_ueof.exit_code(), 3);
    assert!(g_ueof.is_transient());

    // 5. ProtocolError::ConnectionClosed
    let p_cc = ProtocolError::ConnectionClosed;
    let g_cc: GridError = p_cc.into();
    assert_eq!(g_cc.error_code(), "CONNECTION_CLOSED");
    assert_eq!(g_cc.exit_code(), 3);
    assert!(g_cc.is_transient());

    // 6. ProtocolError::Violation
    let p_v = ProtocolError::Violation("invalid sequence number".into());
    let g_v: GridError = p_v.into();
    assert_eq!(g_v.error_code(), "FRAMING_ERROR");
    assert_eq!(g_v.exit_code(), 1);
    assert!(!g_v.is_transient());
}

#[test]
fn test_transient_classification_discrepancies() {
    // DISCREPANCY 1: UnexpectedEof classification
    let direct_io_eof: GridError = io::Error::new(io::ErrorKind::UnexpectedEof, "eof").into();
    let proto_eof: GridError = ProtocolError::UnexpectedEof.into();

    println!(
        "Direct io::ErrorKind::UnexpectedEof is_transient: {}",
        direct_io_eof.is_transient()
    );
    println!(
        "ProtocolError::UnexpectedEof is_transient:        {}",
        proto_eof.is_transient()
    );

    assert!(
        !direct_io_eof.is_transient(),
        "Direct IO UnexpectedEof currently maps to false"
    );
    assert!(
        proto_eof.is_transient(),
        "Protocol UnexpectedEof maps to ConnectionClosed (true)"
    );

    // DISCREPANCY 2: BrokenPipe on socket write vs ConnectionReset on socket read
    let reset_err: GridError = io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
    let pipe_err: GridError = io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe").into();

    assert!(
        reset_err.is_transient(),
        "ConnectionReset is marked transient"
    );
    assert!(
        !pipe_err.is_transient(),
        "CRITICAL: BrokenPipe is currently NOT marked transient despite being a standard peer disconnect on write"
    );

    // DISCREPANCY 3: ConnectionRefused during initial cluster startup
    let ref_err: GridError = io::Error::new(io::ErrorKind::ConnectionRefused, "refused").into();
    assert!(
        !ref_err.is_transient(),
        "CRITICAL: ConnectionRefused is currently NOT marked transient, impacting worker startup retry"
    );
}
