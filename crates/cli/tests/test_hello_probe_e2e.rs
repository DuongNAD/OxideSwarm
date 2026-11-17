//! The `Hello` version probe of `connect_master` and the clear version errors in both directions
//! (design 0039 W1, protocol version 6). No cluster secret: loopback, private ports.

use rusty_grid_cli::connect::connect_master;
use rusty_grid_core::protocol::{
    serialize_message, ClientMessage, ClientResponse, PROTOCOL_VERSION_CURRENT,
    PROTOCOL_VERSION_MIN_COMPATIBLE,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::{GridError, MessageTransport, WireCodec};
use rusty_grid_master::server::{MasterServer, ServerConfig};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_master() -> (
    rusty_grid_master::server::MasterHandle,
    std::net::SocketAddr,
) {
    let server = MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("spawn master");
    let addr = server.server_addr();
    (server, addr)
}

fn submit_task_frame(version: u32) -> Vec<u8> {
    let task = Task::new(
        TaskSpec::new_command("echo", vec!["hi".into()]),
        TaskRequirements::default(),
    );
    let msg = ClientMessage::SubmitTask {
        task: task.clone(),
        wait: false,
    };
    let mut raw = serialize_message(&msg, WireCodec::Bincode)
        .unwrap()
        .to_vec();
    let len = raw.len();
    let vram_tag_offset = {
        let mut task_with_vram = task.clone();
        task_with_vram.requirements.vram_mb = Some(0x1122_3344_5566_7788);
        let with_vram = serialize_message(
            &ClientMessage::SubmitTask {
                task: task_with_vram,
                wait: false,
            },
            WireCodec::Bincode,
        )
        .unwrap();
        raw.iter()
            .zip(with_vram.iter())
            .position(|(a, b)| a != b)
            .expect("vram_mb byte offset")
    };
    if version < 10 {
        // Clients before v10 sent no `extensions` lists (an empty list is an 8-byte length):
        // `Task::extensions` sits before `wait` and the version, `TaskRequirements::extensions`
        // right after `vram_mb` (one byte, `None`).
        assert_eq!(&raw[len - 13..len - 5], &[0u8; 8]);
        raw.drain(len - 13..len - 5);
        assert_eq!(&raw[vram_tag_offset + 1..vram_tag_offset + 9], &[0u8; 8]);
        raw.drain(vram_tag_offset + 1..vram_tag_offset + 9);
    }
    let len = raw.len();
    if version < 6 {
        // A v5 client sent no `workspace` (a one-byte `None` before `wait` and the version).
        assert_eq!(raw[len - 6], 0);
        raw.remove(len - 6);
    }
    if version < 9 {
        // Clients before v9 sent no `vram_mb` (a one-byte `None` in `TaskRequirements`).
        assert_eq!(raw[vram_tag_offset], 0);
        raw.remove(vram_tag_offset);
    }
    let len = raw.len();
    raw[len - 4..].copy_from_slice(&version.to_le_bytes());
    raw
}

fn cluster_status_frame(version: u32) -> Vec<u8> {
    let mut raw = serialize_message(&ClientMessage::ClusterStatus, WireCodec::Bincode)
        .unwrap()
        .to_vec();
    let len = raw.len();
    raw[len - 4..].copy_from_slice(&version.to_le_bytes());
    raw
}

fn hello_frame(version: u32) -> Vec<u8> {
    let mut raw = serialize_message(&ClientMessage::Hello, WireCodec::Bincode)
        .unwrap()
        .to_vec();
    let len = raw.len();
    raw[len - 4..].copy_from_slice(&version.to_le_bytes());
    raw
}

async fn send_raw_expect_error(addr: std::net::SocketAddr, frame: Vec<u8>) -> String {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut t = MessageTransport::with_codec(stream, WireCodec::Bincode);
    t.send_raw_frame(frame.into()).await.unwrap();
    match t
        .recv_msg::<ClientResponse>()
        .await
        .expect("a v6 master must answer a v5 client with a decodable error")
        .expect("an error frame, not a closed connection")
    {
        ClientResponse::Error { message } => message,
        other => panic!("expected ClientResponse::Error, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_master_probes_with_hello_and_the_master_answers_it() {
    let (server, addr) = spawn_master().await;
    let mut t = connect_master(&addr.to_string(), None)
        .await
        .expect("connect_master against a v6 master");
    // The probe left the connection clean: the next request gets its own answer.
    t.send_msg(&ClientMessage::Hello).await.unwrap();
    match t.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::Hello {
            min_compatible_version,
        }) => assert_eq!(min_compatible_version, PROTOCOL_VERSION_MIN_COMPATIBLE),
        other => panic!("expected Hello, got {other:?}"),
    }
    t.send_msg(&ClientMessage::ClusterStatus).await.unwrap();
    assert!(matches!(
        t.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::ClusterStatus { .. })
    ));
    let _ = server.shutdown();
}

#[tokio::test]
async fn a_v5_client_gets_a_clear_version_error_from_a_v6_master() {
    let (server, addr) = spawn_master().await;
    // The v5 probe (ClusterStatus) and the v5 SubmitTask, whose Task has no `workspace`.
    for frame in [cluster_status_frame(5), submit_task_frame(5)] {
        let message = send_raw_expect_error(addr, frame).await;
        assert!(
            message.contains("Incompatible protocol version 5")
                && message.contains(&format!("{PROTOCOL_VERSION_CURRENT}")),
            "{message}"
        );
    }
    // A current SubmitTask is not mistaken for a v5 one: it reaches the queue (and is accepted).
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut t = MessageTransport::with_codec(stream, WireCodec::Bincode);
    t.send_raw_frame(submit_task_frame(PROTOCOL_VERSION_CURRENT).into())
        .await
        .unwrap();
    assert!(matches!(
        t.recv_msg::<ClientResponse>().await.unwrap(),
        Some(ClientResponse::TaskSubmitted { .. })
    ));
    let _ = server.shutdown();
}

/// A v6 client against a v5 master: the master cannot decode `Hello` and drops the connection,
/// which `connect_master` reports as a closed connection (the caller prints the "older or newer
/// build" hint with protocol version 6).
#[tokio::test]
async fn a_v6_client_against_a_master_that_cannot_decode_hello_sees_a_closed_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            use tokio::io::AsyncReadExt;
            let mut buf = [0u8; 64];
            let _ = stream.read(&mut buf).await;
            drop(stream);
        }
    });
    let err = connect_master(&addr.to_string(), None)
        .await
        .err()
        .expect("a master that drops the probe must fail the connect");
    assert!(
        matches!(err, GridError::ConnectionClosed | GridError::Io(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn a_v6_client_gets_a_clear_version_error_from_a_v7_master() {
    let (server, addr) = spawn_master().await;
    for frame in [
        hello_frame(6),
        cluster_status_frame(6),
        submit_task_frame(6),
    ] {
        let message = send_raw_expect_error(addr, frame).await;
        assert!(
            message.contains("Incompatible protocol version 6")
                && message.contains(&format!("{PROTOCOL_VERSION_CURRENT}")),
            "{message}"
        );
    }
    let _ = server.shutdown();
}

#[tokio::test]
async fn a_v8_client_gets_a_clear_version_error_from_a_v9_master() {
    let (server, addr) = spawn_master().await;
    let message = send_raw_expect_error(addr, submit_task_frame(8)).await;
    assert!(
        message.contains("Incompatible protocol version 8")
            && message.contains(&format!("{PROTOCOL_VERSION_CURRENT}")),
        "{message}"
    );
    let _ = server.shutdown();
}

/// A master that answers the probe with a reply stamped with another version is refused with
/// `GridError::IncompatibleVersion`, and so is one that sends the error text itself.
#[tokio::test]
async fn a_probe_reply_from_another_version_is_an_incompatible_version_error() {
    for stamped in [PROTOCOL_VERSION_CURRENT - 1, PROTOCOL_VERSION_CURRENT + 1] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut t = MessageTransport::with_codec(stream, WireCodec::Bincode);
            let _ = t.recv_msg::<ClientMessage>().await;
            let mut raw = serialize_message(
                &ClientResponse::Hello {
                    min_compatible_version: stamped,
                },
                WireCodec::Bincode,
            )
            .unwrap()
            .to_vec();
            let len = raw.len();
            raw[len - 4..].copy_from_slice(&stamped.to_le_bytes());
            t.send_raw_frame(raw.into()).await.unwrap();
            let _ = t.recv_msg::<ClientMessage>().await;
        });
        let err = connect_master(&addr.to_string(), None)
            .await
            .err()
            .expect("another protocol version must be refused");
        match err {
            GridError::IncompatibleVersion(message) => assert!(
                message.contains(&format!("Incompatible protocol version {stamped}")),
                "{message}"
            ),
            other => panic!("expected IncompatibleVersion, got {other:?}"),
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut t = MessageTransport::with_codec(stream, WireCodec::Bincode);
        let _ = t.recv_msg::<ClientMessage>().await;
        let _ = t
            .send_msg(&ClientResponse::Error {
                message: "Incompatible protocol version 7 from client (supported: 6..=6)".into(),
            })
            .await;
    });
    let err = connect_master(&addr.to_string(), None)
        .await
        .err()
        .expect("an error reply must fail the connect");
    assert!(
        matches!(err, GridError::IncompatibleVersion(_)),
        "got {err:?}"
    );
}

/// A master that answers the probe with bytes a v6 client cannot decode (what an older master's
/// reply to an unknown request, or a half-written frame, looks like) gets the upgrade hint, and
/// only the probe is reworded: the error is a `ConnectionFailed` that names protocol version 6.
#[tokio::test]
async fn an_undecodable_probe_reply_carries_the_upgrade_hint() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 64];
                let _ = stream.read(&mut buf).await;
                // A complete frame (length 9, bincode tag) whose body is no ClientResponse.
                let mut frame = 9u32.to_be_bytes().to_vec();
                frame.push(0x02);
                frame.extend_from_slice(&[0xff; 8]);
                let _ = stream.write_all(&frame).await;
                let _ = stream.flush().await;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            });
        }
    });
    let err = connect_master(&addr.to_string(), None)
        .await
        .err()
        .expect("an undecodable reply must fail the connect");
    match err {
        GridError::ConnectionFailed(message) => {
            assert!(
                message.contains("older or newer build")
                    && message.contains(&format!("protocol version {PROTOCOL_VERSION_CURRENT}")),
                "{message}"
            );
        }
        other => panic!("expected ConnectionFailed with the hint, got {other:?}"),
    }
}

/// A master that dies in the middle of the reply frame is an I/O error of the probe, which
/// `report_master_failure` already answers with the same hint.
#[tokio::test]
async fn a_probe_reply_cut_off_mid_frame_is_a_closed_or_io_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        let _ = stream.read(&mut buf).await;
        let _ = stream.write_all(&[0, 0, 0, 40, 0x02, 1, 2]).await;
        let _ = stream.flush().await;
    });
    let err = connect_master(&addr.to_string(), None)
        .await
        .err()
        .expect("a truncated reply must fail the connect");
    assert!(
        matches!(err, GridError::ConnectionClosed | GridError::Io(_)),
        "got {err:?}"
    );
}

/// Design 0083 D2/D3: a `Hello` stamped with the previous version (what a v9 client sends) is
/// answered by a v10 master, and the reply is stamped with the client's version so that the v9
/// client's own range check accepts it. A v10 client would refuse that reply (it takes only an
/// echo of its own version), which is what the next test shows from the other side.
#[tokio::test]
async fn a_previous_version_hello_is_answered_with_the_clients_own_stamp() {
    let (server, addr) = spawn_master().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut t = MessageTransport::with_codec(stream, WireCodec::Bincode);
    t.send_raw_frame(hello_frame(PROTOCOL_VERSION_CURRENT - 1).into())
        .await
        .unwrap();
    let raw = t
        .recv_raw_frame()
        .await
        .unwrap()
        .expect("the master answers a previous-version Hello");
    let len = raw.len();
    assert_eq!(
        raw[len - 4..],
        (PROTOCOL_VERSION_CURRENT - 1).to_le_bytes(),
        "the reply carries the client's version"
    );
    // The window's own bound: the number `Hello` reports.
    assert_eq!(
        raw[len - 8..len - 4],
        PROTOCOL_VERSION_MIN_COMPATIBLE.to_le_bytes()
    );
    assert!(
        rusty_grid_core::protocol::deserialize_message::<ClientResponse>(&raw).is_err(),
        "a v10 reader takes only its own stamp"
    );

    // A client below the window is still refused with the version error.
    let message =
        send_raw_expect_error(addr, hello_frame(PROTOCOL_VERSION_MIN_COMPATIBLE - 1)).await;
    assert!(
        message.contains(&format!(
            "Incompatible protocol version {}",
            PROTOCOL_VERSION_MIN_COMPATIBLE - 1
        )),
        "{message}"
    );
    let _ = server.shutdown();
}
