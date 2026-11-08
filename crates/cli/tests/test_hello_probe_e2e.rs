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
    let msg = ClientMessage::SubmitTask { task, wait: false };
    let mut raw = serialize_message(&msg, WireCodec::Bincode)
        .unwrap()
        .to_vec();
    let len = raw.len();
    if version < 6 {
        // A v5 client sent no `workspace` (a one-byte `None` before `wait` and the version).
        assert_eq!(raw[len - 6], 0);
        raw.remove(len - 6);
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

/// A master that answers the probe with a reply stamped with another version is refused with
/// `GridError::IncompatibleVersion`, and so is one that sends the error text itself.
#[tokio::test]
async fn a_probe_reply_from_another_version_is_an_incompatible_version_error() {
    for stamped in [5u32, 7] {
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
                message: "Incompatible protocol version 5 from client (supported: 6..=6)".into(),
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
