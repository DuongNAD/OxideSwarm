//! Empirical stress tests for async stream properties:
//! 1. Duplex channel splitting
//! 2. Concurrent read/write on separate tokio tasks
//! 3. Real TCP socket streaming and half-close handling
//! 4. Abrupt disconnect handling mid-header, mid-payload, and post-close

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rusty_grid_core::error::GridError;
use rusty_grid_core::protocol::{
    MasterMessage, MessageReader, MessageTransport, MessageWriter, ProtocolError, WorkerMessage,
};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_core::WorkerCapabilities;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

fn sample_task(name: &str) -> Task {
    Task::new(
        TaskSpec::Command {
            program: "echo".into(),
            args: vec![name.into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        },
        TaskRequirements::generic(1, 30),
    )
}

#[tokio::test]
async fn test_duplex_split_separate_tasks_concurrent_io() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (mut client_tx, mut client_rx) = MessageTransport::new(client_io).split();
    let (mut server_tx, mut server_rx) = MessageTransport::new(server_io).split();

    let count = 500usize;
    let worker_id = Uuid::new_v4();

    // Spawn Client Sender Task
    let client_send_handle = tokio::spawn(async move {
        for i in 0..count {
            let hb = WorkerMessage::Heartbeat {
                worker_id,
                timestamp: i as u64,
                active_tasks: i % 4,
                cpu_usage_pct: 0.0,
                ram_available_mb: 0,
            };
            client_tx.send_msg(&hb).await.expect("Client send failed");
        }
    });

    // Spawn Server Receiver Task
    let server_recv_count = Arc::new(AtomicUsize::new(0));
    let server_recv_count_clone = server_recv_count.clone();
    let server_recv_handle = tokio::spawn(async move {
        for expected_i in 0..count {
            match server_rx.recv_msg::<WorkerMessage>().await {
                Ok(Some(WorkerMessage::Heartbeat {
                    timestamp,
                    worker_id: wid,
                    ..
                })) => {
                    assert_eq!(wid, worker_id);
                    assert_eq!(timestamp, expected_i as u64);
                    server_recv_count_clone.fetch_add(1, Ordering::SeqCst);
                }
                other => panic!("Expected Heartbeat {expected_i}, got: {other:?}"),
            }
        }
    });

    // Spawn Server Sender Task
    let server_send_handle = tokio::spawn(async move {
        for i in 0..count {
            let ack = MasterMessage::HeartbeatAck {
                timestamp: i as u64,
            };
            server_tx.send_msg(&ack).await.expect("Server send failed");
        }
    });

    // Spawn Client Receiver Task
    let client_recv_count = Arc::new(AtomicUsize::new(0));
    let client_recv_count_clone = client_recv_count.clone();
    let client_recv_handle = tokio::spawn(async move {
        for expected_i in 0..count {
            match client_rx.recv_msg::<MasterMessage>().await {
                Ok(Some(MasterMessage::HeartbeatAck { timestamp })) => {
                    assert_eq!(timestamp, expected_i as u64);
                    client_recv_count_clone.fetch_add(1, Ordering::SeqCst);
                }
                other => panic!("Expected HeartbeatAck {expected_i}, got: {other:?}"),
            }
        }
    });

    // Await all 4 concurrent tasks
    let (res_cs, res_sr, res_ss, res_cr) = tokio::join!(
        client_send_handle,
        server_recv_handle,
        server_send_handle,
        client_recv_handle
    );
    res_cs.expect("client sender panicked");
    res_sr.expect("server receiver panicked");
    res_ss.expect("server sender panicked");
    res_cr.expect("client receiver panicked");

    assert_eq!(server_recv_count.load(Ordering::SeqCst), count);
    assert_eq!(client_recv_count.load(Ordering::SeqCst), count);
}

#[tokio::test]
async fn test_tcp_real_socket_split_concurrent_bidirectional() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr: SocketAddr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.unwrap();
        let (mut server_tx, mut server_rx) = MessageTransport::new(server_stream).split();

        // 1. Receive Register message
        let reg_msg: Option<WorkerMessage> = server_rx.recv_msg().await.unwrap();
        let (wid, caps) = match reg_msg {
            Some(WorkerMessage::Register {
                worker_id,
                capabilities,
            }) => (worker_id, capabilities),
            other => panic!("Expected Register, got {other:?}"),
        };
        assert_eq!(caps.name, "tcp-worker");

        // 2. Send RegisterAck
        server_tx
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id: wid,
                heartbeat_interval_secs: 2,
                message: Some("Registered successfully".into()),
            })
            .await
            .unwrap();

        // 3. Bidirectional burst: Server sends 200 tasks while reading 200 progress updates
        let server_tx_handle = tokio::spawn(async move {
            for i in 0..200 {
                let task = sample_task(&format!("task-{i}"));
                server_tx
                    .send_msg(&MasterMessage::AssignTask { task })
                    .await
                    .unwrap();
            }
        });

        let mut progress_count = 0;
        while progress_count < 200 {
            let msg: Option<WorkerMessage> = server_rx.recv_msg().await.unwrap();
            if let Some(WorkerMessage::TaskProgress { status, .. }) = msg {
                assert_eq!(status, TaskStatus::Running);
                progress_count += 1;
            } else {
                panic!("Expected TaskProgress, got: {msg:?}");
            }
        }

        server_tx_handle.await.unwrap();
        progress_count
    });

    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let (mut client_tx, mut client_rx) = MessageTransport::new(client_stream).split();

    let wid = Uuid::new_v4();
    client_tx
        .send_msg(&WorkerMessage::Register {
            worker_id: wid,
            capabilities: WorkerCapabilities::new("tcp-worker", 8, 16384, false, false, None),
        })
        .await
        .unwrap();

    let ack: Option<MasterMessage> = client_rx.recv_msg().await.unwrap();
    assert!(matches!(
        ack,
        Some(MasterMessage::RegisterAck { accepted: true, .. })
    ));

    // Client receives tasks and sends progress
    let client_recv_handle = tokio::spawn(async move {
        let mut tasks_received = 0;
        for _ in 0..200 {
            let msg: Option<MasterMessage> = client_rx.recv_msg().await.unwrap();
            if let Some(MasterMessage::AssignTask { task }) = msg {
                tasks_received += 1;
                client_tx
                    .send_msg(&WorkerMessage::TaskProgress {
                        worker_id: wid,
                        task_id: task.id,
                        status: TaskStatus::Running,
                    })
                    .await
                    .unwrap();
            } else {
                panic!("Expected AssignTask, got {msg:?}");
            }
        }
        tasks_received
    });

    let client_tasks = client_recv_handle.await.unwrap();
    let server_progress = server_task.await.unwrap();

    assert_eq!(client_tasks, 200);
    assert_eq!(server_progress, 200);
}

#[tokio::test]
async fn test_tcp_half_close_handling() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.unwrap();
        let (mut server_tx, mut server_rx) = MessageTransport::new(server_stream).split();

        // 1. Read message sent by client before half-close
        let msg1: Option<WorkerMessage> = server_rx.recv_msg().await.unwrap();
        assert!(matches!(msg1, Some(WorkerMessage::Heartbeat { .. })));

        // 2. Read next message -> Client shut down writing, so should be Ok(None)
        let msg2: Option<WorkerMessage> = server_rx.recv_msg().await.unwrap();
        assert!(
            msg2.is_none(),
            "Server must read Ok(None) after client half-close"
        );

        // 3. Server sends a message back to client (half-duplex still active)
        server_tx
            .send_msg(&MasterMessage::HeartbeatAck { timestamp: 9999 })
            .await
            .expect("Server write should succeed even after client write half-close");

        // 4. Server explicitly closes write
        // drop server_tx
        drop(server_tx);
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let (mut client_read_raw, mut client_write_raw) = tokio::io::split(client_stream);

    let mut client_writer = MessageWriter::new(&mut client_write_raw);
    let mut client_reader = MessageReader::new(&mut client_read_raw);

    // 1. Client writes heartbeat
    client_writer
        .send_msg(&WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: 1234,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        })
        .await
        .unwrap();

    // 2. Client shuts down write side
    client_write_raw.shutdown().await.unwrap();

    // 3. Client receives server's response on read side
    let ack: Option<MasterMessage> = client_reader.recv_msg().await.unwrap();
    assert_eq!(
        ack,
        Some(MasterMessage::HeartbeatAck { timestamp: 9999 }),
        "Client should receive response after client's write half is shut down"
    );

    // 4. Client reads EOF from server after server closes
    let eof: Option<MasterMessage> = client_reader.recv_msg().await.unwrap();
    assert!(
        eof.is_none(),
        "Client reader should receive Ok(None) on final EOF"
    );

    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_abrupt_disconnect_mid_length_prefix() {
    let (mut client_io, server_io) = tokio::io::duplex(1024);
    let mut server = MessageTransport::new(server_io);

    // Write only 2 bytes of the 4-byte big-endian length prefix
    client_io.write_all(&[0x00, 0x00]).await.unwrap();
    // Abruptly drop client
    drop(client_io);

    let res: Result<Option<WorkerMessage>, ProtocolError> = server.recv_msg().await;
    // EMPIRICAL FINDING:
    // LengthDelimitedCodec::decode_eof returns io::ErrorKind::Other with "bytes remaining on stream".
    // It does NOT return io::ErrorKind::UnexpectedEof!
    // As a consequence, recv_msg falls through to ProtocolError::Io, NOT ProtocolError::UnexpectedEof!
    match res {
        Err(ProtocolError::Io(ref e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::Other);
            assert!(e.to_string().contains("bytes remaining on stream"));
            let grid_err: GridError = res.unwrap_err().into();
            assert_eq!(grid_err.error_code(), "IO_ERROR");
            // Under current implementation, ErrorKind::Other is NOT considered transient:
            assert!(
                !grid_err.is_transient(),
                "CRITICAL: Truncated stream on disconnect is classified as non-transient IO_ERROR"
            );
        }
        Err(ProtocolError::UnexpectedEof) => {
            // If implementation were to recognize "bytes remaining on stream", it would reach here
            let grid_err: GridError = ProtocolError::UnexpectedEof.into();
            assert_eq!(grid_err.error_code(), "CONNECTION_CLOSED");
            assert!(grid_err.is_transient());
        }
        other => panic!("Unexpected result on truncated header: {other:?}"),
    }
}

#[tokio::test]
async fn test_abrupt_disconnect_mid_payload() {
    let (mut client_io, server_io) = tokio::io::duplex(1024);
    let mut server = MessageTransport::new(server_io);

    // Write 4-byte length prefix: 100 bytes payload
    let length_prefix = 100u32.to_be_bytes();
    client_io.write_all(&length_prefix).await.unwrap();

    // Write only 20 bytes of payload
    client_io.write_all(&[b'x'; 20]).await.unwrap();
    // Drop client mid-payload
    drop(client_io);

    let res: Result<Option<WorkerMessage>, ProtocolError> = server.recv_msg().await;
    // EMPIRICAL FINDING:
    // Truncated payload triggers LengthDelimitedCodec::decode_eof with ErrorKind::Other ("bytes remaining on stream")
    match res {
        Err(ProtocolError::Io(ref e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::Other);
            assert!(e.to_string().contains("bytes remaining on stream"));
            let grid_err: GridError = res.unwrap_err().into();
            assert_eq!(grid_err.error_code(), "IO_ERROR");
            assert!(!grid_err.is_transient());
        }
        Err(ProtocolError::UnexpectedEof) => {
            let grid_err: GridError = ProtocolError::UnexpectedEof.into();
            assert_eq!(grid_err.error_code(), "CONNECTION_CLOSED");
            assert!(grid_err.is_transient());
        }
        other => panic!("Unexpected result on truncated payload: {other:?}"),
    }
}

#[tokio::test]
async fn test_write_to_closed_peer_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.unwrap();
        // Immediately close server socket
        drop(server_stream);
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    server_handle.await.unwrap();

    // Wait a brief moment to ensure TCP FIN/RST is delivered
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client_writer = MessageWriter::new(client_stream);

    // Write messages until failure
    let mut error_encountered = None;
    for i in 0..100 {
        let msg = WorkerMessage::Heartbeat {
            worker_id: Uuid::new_v4(),
            timestamp: i,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 0,
        };
        if let Err(e) = client_writer.send_msg(&msg).await {
            error_encountered = Some(e);
            break;
        }
    }

    assert!(
        error_encountered.is_some(),
        "Writing to closed peer must eventually fail"
    );
    let proto_err = error_encountered.unwrap();
    let grid_err: GridError = proto_err.into();

    println!(
        "Observed error when writing to closed socket: {:?}",
        grid_err
    );
    println!("Error code: {}", grid_err.error_code());
    println!("Exit code: {}", grid_err.exit_code());
    println!("Is transient: {}", grid_err.is_transient());

    // Both BrokenPipe and ConnectionReset can occur depending on OS timing
    match &grid_err {
        GridError::Io(io_err) => {
            let kind = io_err.kind();
            assert!(
                kind == std::io::ErrorKind::BrokenPipe
                    || kind == std::io::ErrorKind::ConnectionReset
                    || kind == std::io::ErrorKind::ConnectionAborted
                    || kind == std::io::ErrorKind::UnexpectedEof,
                "Unexpected IO kind: {kind:?}"
            );
        }
        GridError::ConnectionClosed => {}
        other => panic!("Unexpected GridError variant: {other:?}"),
    }
}
