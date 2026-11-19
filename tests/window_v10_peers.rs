//! A v11 master with peers one protocol version back (design 0083, design 0098).
//!
//! The v10 peers here speak the v10 layouts that `crates/core/tests/data/wire_layout_previous.txt`
//! (the layout file of release v0.4.4) pins: `deserialize_message_as_v10` is the frozen v10 reader,
//! `serialize_v10_client_message` the v10 writer, and both are checked against the recorded v0.4.4
//! bytes by `crates/core/src/window_tests.rs`. `ClientResponse::ClusterStatus` is the one variant
//! whose wire layout changed in protocol 11 (`cancelled_tasks`, BUG-22); for a v10 peer the master
//! folds `cancelled_tasks` back into `failed_tasks` and omits the new field. The real released
//! binaries run in `scripts/release_skew.py`.
//!
//! Tests: a v10 worker registers and runs a task; critical worker extensions still require `ext:`;
//! a v10 worker cannot forge `proto:11`; failover resume (`AssignTaskWithCheckpoint`); a v10 client
//! submits, polls, lists, and queries `ClusterStatus` (both codecs); `ClusterStatus` with 1 failed
//! and 1 cancelled task reports `(1, 1)` to v11 and `(2, 0)` to v10; v9 peers are refused outside
//! the window.

use std::time::Duration;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::placement::PROTO_TAG_PREFIX;
use rusty_grid_core::protocol::{
    deserialize_message, deserialize_message_as_v10, serialize_message,
    serialize_v10_client_message, ClientMessage, ClientResponse, MasterMessage, MessageTransport,
    ProtocolCapabilities, WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
    PROTOCOL_VERSION_MIN_COMPATIBLE,
};
use rusty_grid_core::task::{
    Bytes, Extension, Task, TaskId, TaskRequirements, TaskResult, TaskSpec,
};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

const V10: u32 = 10;
const CODECS: [WireCodec; 2] = [WireCodec::Bincode, WireCodec::Json];

/// A test extension of the `Task` list that a worker may ignore (`0x8001`, non-critical).
fn hint() -> Extension {
    Extension {
        id: 0x8001,
        critical: false,
        bytes: vec![0x01, 0x02],
    }
}

/// A critical extension the worker must understand (`0x8002`, scope `Worker`).
fn worker_limit() -> Extension {
    Extension {
        id: 0x8002,
        critical: true,
        bytes: vec![0x03],
    }
}

/// A critical extension only the master enforces (`0x8003`, scope `Master`).
fn master_limit() -> Extension {
    Extension {
        id: 0x8003,
        critical: true,
        bytes: vec![0x04],
    }
}

async fn spawn_master() -> MasterHandle {
    MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("master starts")
}

fn echo_task(extensions: Vec<Extension>) -> Task {
    let mut task = Task::new(
        TaskSpec::command("echo", vec!["window".into()]),
        TaskRequirements::generic(1, 10),
    );
    task.extensions = extensions;
    task
}

/// A worker that speaks protocol 10 over a raw transport.
struct V10Worker {
    transport: MessageTransport<TcpStream>,
    id: Uuid,
    codec: WireCodec,
}

impl V10Worker {
    async fn register(master: &MasterHandle, name: &str, codec: WireCodec) -> Self {
        Self::register_with_tags(master, name, Vec::new(), codec).await
    }

    async fn register_with_tags(
        master: &MasterHandle,
        name: &str,
        tags: Vec<String>,
        codec: WireCodec,
    ) -> Self {
        let stream = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut transport = MessageTransport::with_codec(stream, codec);
        let id = Uuid::new_v4();
        let mut caps = WorkerCapabilities::new(name, 4, 8_192, false, false, None);
        caps.tags = tags;
        transport
            .send_msg(&WorkerMessage::Register {
                worker_id: id,
                capabilities: caps,
                protocol_version: V10,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        let mut worker = Self {
            transport,
            id,
            codec,
        };
        match worker.next_message(Duration::from_secs(5)).await {
            Some(MasterMessage::RegisterAck {
                accepted,
                protocol_version,
                ..
            }) => {
                assert!(accepted, "the v11 master accepts a v10 worker");
                assert_eq!(protocol_version, V10, "the ack echoes the worker's version");
            }
            other => panic!("expected RegisterAck, got {other:?}"),
        }
        worker
    }

    /// The next message the master sends, read the way a v10 worker reads it.
    async fn next_message(&mut self, timeout: Duration) -> Option<MasterMessage> {
        let frame = tokio::time::timeout(timeout, self.transport.recv_raw_frame())
            .await
            .ok()?
            .expect("read")?;
        let (msg, codec) = deserialize_message_as_v10::<MasterMessage>(&frame)
            .unwrap_or_else(|e| panic!("a v10 worker cannot read a frame of the master: {e}"));
        assert_eq!(codec, self.codec);
        Some(msg)
    }

    /// The next `AssignTask` within `timeout`, skipping other messages.
    async fn next_assigned_task(&mut self, timeout: Duration) -> Option<(Task, Vec<u8>)> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.checked_duration_since(tokio::time::Instant::now())?;
            let frame = tokio::time::timeout(left, self.transport.recv_raw_frame())
                .await
                .ok()?
                .expect("read")?;
            let (msg, _) = deserialize_message_as_v10::<MasterMessage>(&frame).unwrap();
            if let MasterMessage::AssignTask { task } = msg {
                return Some((task, frame.to_vec()));
            }
        }
    }

    async fn finish(&mut self, task_id: TaskId, exit_code: i32) {
        let result = if exit_code == 0 {
            TaskResult::success(self.id, task_id, "window\n", 1, false)
        } else {
            TaskResult::failure(self.id, task_id, exit_code, "", "failed", 1, None)
        };
        self.transport
            .send_msg(&WorkerMessage::from(result))
            .await
            .unwrap();
    }
}

/// A v10 worker registers with a v11 master and runs a task. `Master`-scope extensions are
/// stripped from its copy while non-critical ones stay; the stored task keeps both; the worker is
/// listed as `proto:10`.
#[tokio::test]
async fn v10_worker_registers_and_runs_a_task() {
    for codec in CODECS {
        let master = spawn_master().await;
        let mut worker = V10Worker::register(&master, "v10-worker", codec).await;

        let listed = master.list_workers().await.unwrap();
        let info = listed.iter().find(|w| w.worker_id == worker.id).unwrap();
        assert!(
            info.capabilities
                .tags
                .contains(&format!("{PROTO_TAG_PREFIX}{V10}")),
            "{codec:?}: tags {:?}",
            info.capabilities.tags
        );

        let task = echo_task(vec![hint(), master_limit()]);
        let task_id = master.submit_task(task.clone()).await.unwrap();

        let (assigned, _frame) = worker
            .next_assigned_task(Duration::from_secs(5))
            .await
            .unwrap_or_else(|| panic!("{codec:?}: the v10 worker got no task"));
        assert_eq!(assigned.id, task_id);
        assert_eq!(
            assigned.extensions,
            vec![hint()],
            "{codec:?}: Master-scope extension is stripped while non-critical stays"
        );

        let stored = master.queue().get_task_spec(&task_id).await.unwrap();
        assert_eq!(stored.extensions, vec![hint(), master_limit()]);

        worker.finish(task_id, 0).await;
        let result = master
            .wait_task(task_id, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.worker_id, worker.id);
        let _ = master.shutdown();
    }
}

/// A v10 worker without `ext:<id>` does not receive a task carrying a critical `Worker`
/// extension; a v10 worker that carries `ext:<id>` does.
#[tokio::test]
async fn critical_worker_extension_requires_ext_tag_on_v10_worker() {
    let master = spawn_master().await;
    let mut plain_v10 = V10Worker::register(&master, "v10-plain", WireCodec::Bincode).await;

    let task_id = master
        .submit_task(echo_task(vec![worker_limit()]))
        .await
        .unwrap();
    assert!(
        plain_v10
            .next_assigned_task(Duration::from_millis(1_200))
            .await
            .is_none(),
        "a task with a critical Worker extension must not reach a worker without ext:<id>"
    );
    assert_eq!(
        master.queue().get_state(&task_id).await,
        Some(TaskState::Queued)
    );

    let mut tagged_v10 = V10Worker::register_with_tags(
        &master,
        "v10-tagged",
        vec!["ext:32770".into()],
        WireCodec::Bincode,
    )
    .await;
    let (assigned, _) = tagged_v10
        .next_assigned_task(Duration::from_secs(5))
        .await
        .expect("v10 worker with ext:32770 receives the critical extension task");
    assert_eq!(assigned.id, task_id);
    assert_eq!(assigned.extensions, vec![worker_limit()]);
    tagged_v10.finish(task_id, 0).await;
    let result = master
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(result.worker_id, tagged_v10.id);
    assert_eq!(result.exit_code, 0);
    let _ = master.shutdown();
}

/// A v10 worker cannot forge a `proto:11` tag; the master drops worker-supplied `proto:*` tags
/// and records `proto:10` alongside user tags.
#[tokio::test]
async fn v10_worker_cannot_forge_proto_tag() {
    let master = spawn_master().await;
    let v10 = V10Worker::register_with_tags(
        &master,
        "forger",
        vec!["proto:11".into(), "zone:lab".into()],
        WireCodec::Bincode,
    )
    .await;
    let listed = master.list_workers().await.unwrap();
    let tags = &listed
        .iter()
        .find(|w| w.worker_id == v10.id)
        .unwrap()
        .capabilities
        .tags;
    assert!(tags.contains(&"zone:lab".to_string()), "{tags:?}");
    let protos: Vec<_> = tags.iter().filter(|t| t.starts_with("proto:")).collect();
    assert_eq!(
        protos,
        vec![&format!("{PROTO_TAG_PREFIX}{V10}")],
        "{tags:?}"
    );
    let _ = master.shutdown();
}

/// A task resumed from a checkpoint reaches a v10 worker as `AssignTaskWithCheckpoint`.
#[tokio::test]
async fn v10_worker_gets_assign_task_with_checkpoint_after_failover() {
    let master = spawn_master().await;
    let mut first = V10Worker::register(&master, "v10-first", WireCodec::Bincode).await;
    let task_id = master.submit_task(echo_task(vec![hint()])).await.unwrap();
    first
        .next_assigned_task(Duration::from_secs(5))
        .await
        .expect("first v10 worker gets the task");
    first
        .transport
        .send_msg(&WorkerMessage::Checkpoint {
            task_id,
            sequence: 1,
            delta_state: Bytes::from_static(b"state"),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(first);

    let mut second = V10Worker::register(&master, "v10-second", WireCodec::Bincode).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let left = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .expect("AssignTaskWithCheckpoint must arrive");
        let Some(msg) = second.next_message(left).await else {
            panic!("no AssignTaskWithCheckpoint arrived");
        };
        if let MasterMessage::AssignTaskWithCheckpoint {
            task,
            latest_checkpoint,
        } = msg
        {
            assert_eq!(task.id, task_id);
            assert_eq!(task.extensions, vec![hint()]);
            assert_eq!(
                latest_checkpoint.expect("the checkpoint").delta_state,
                Bytes::from_static(b"state")
            );
            break;
        }
    }
    let _ = master.shutdown();
}

async fn v10_call(
    client: &mut MessageTransport<TcpStream>,
    codec: WireCodec,
    msg: ClientMessage,
) -> ClientResponse {
    let frame = serialize_v10_client_message(&msg, codec).unwrap();
    client.send_raw_frame(frame).await.unwrap();
    let reply = client.recv_raw_frame().await.unwrap().expect("a reply");
    let (resp, _) = deserialize_message_as_v10::<ClientResponse>(&reply)
        .unwrap_or_else(|e| panic!("a v10 client cannot read the reply: {e}"));
    if !matches!(resp, ClientResponse::Error { .. }) {
        // Stamped 10: the v11 reader (which takes only its own version) refuses it.
        assert!(
            deserialize_message::<ClientResponse>(&reply).is_err(),
            "the reply to a v10 client must be stamped 10"
        );
    }
    resp
}

/// A v10 client: `Hello`, `SubmitTask` (waiting), `GetTaskResult`, `GetTaskStatus`, `ListWorkers`,
/// and `ClusterStatus` against a v11 master. Every reply is stamped 10 and decodes with the frozen
/// v10 reader, not with the v11 one; the worker listing shows `proto:11`.
#[tokio::test]
async fn v10_client_round_trip() {
    for codec in CODECS {
        let master = spawn_master().await;
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut worker = WorkerClient::for_testing(
            master.server_addr().to_string(),
            Some("v11-worker".into()),
            Some(2),
            false,
        );
        tokio::spawn(async move {
            let _ = worker.run(stop_rx).await;
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while master.list_workers().await.unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the v11 worker registers");

        let stream = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut client = MessageTransport::with_codec(stream, codec);

        match v10_call(&mut client, codec, ClientMessage::Hello).await {
            ClientResponse::Hello {
                min_compatible_version,
            } => assert_eq!(min_compatible_version, V10),
            other => panic!("expected Hello, got {other:?}"),
        }

        let task = echo_task(Vec::new());
        let task_id = task.id;
        match v10_call(
            &mut client,
            codec,
            ClientMessage::SubmitTask { task, wait: true },
        )
        .await
        {
            ClientResponse::TaskCompleted {
                task_id: id,
                result,
            } => {
                assert_eq!(id, task_id);
                assert_eq!(result.exit_code, 0);
            }
            other => panic!("expected TaskCompleted, got {other:?}"),
        }
        match v10_call(&mut client, codec, ClientMessage::GetTaskResult { task_id }).await {
            ClientResponse::TaskCompleted { result, .. } => assert_eq!(result.exit_code, 0),
            other => panic!("expected TaskCompleted, got {other:?}"),
        }
        match v10_call(&mut client, codec, ClientMessage::GetTaskStatus { task_id }).await {
            ClientResponse::TaskStatusInfo { task_id: id, .. } => assert_eq!(id, task_id),
            other => panic!("expected TaskStatusInfo, got {other:?}"),
        }
        match v10_call(&mut client, codec, ClientMessage::ListWorkers).await {
            ClientResponse::WorkerList { workers } => {
                assert_eq!(workers.len(), 1);
                assert!(
                    workers[0]
                        .tags
                        .contains(&format!("{PROTO_TAG_PREFIX}{PROTOCOL_VERSION_CURRENT}")),
                    "{:?}",
                    workers[0].tags
                );
            }
            other => panic!("expected WorkerList, got {other:?}"),
        }
        match v10_call(&mut client, codec, ClientMessage::ClusterStatus).await {
            ClientResponse::ClusterStatus {
                completed_tasks,
                cancelled_tasks,
                workers,
                ..
            } => {
                assert_eq!(completed_tasks, 1);
                assert_eq!(cancelled_tasks, 0);
                assert_eq!(workers.len(), 1);
            }
            other => panic!("expected ClusterStatus, got {other:?}"),
        }

        let _ = stop_tx.send(true);
        let _ = master.shutdown();
    }
}

/// BUG-22 across protocol versions: with 1 failed task and 1 cancelled task on a v11 master,
/// a v11 client sees `(failed_tasks: 1, cancelled_tasks: 1)` while a v10 client decodes a v10
/// `ClusterStatus` stamped 10 and sees `(failed_tasks: 2)` with `cancelled_tasks` absent.
#[tokio::test]
async fn cluster_status_with_failed_and_cancelled_splits_for_v11_and_folds_for_v10() {
    let master = spawn_master().await;
    let mut worker = V10Worker::register(&master, "v10-worker", WireCodec::Bincode).await;

    // 1. One failed task (exit code 2).
    let failed_id = master.submit_task(echo_task(Vec::new())).await.unwrap();
    let (assigned, _) = worker
        .next_assigned_task(Duration::from_secs(5))
        .await
        .expect("worker receives failing task");
    assert_eq!(assigned.id, failed_id);
    worker.finish(failed_id, 2).await;
    let failed_res = master
        .wait_task(failed_id, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(failed_res.exit_code, 2);

    // 2. One cancelled task (unrunnable GPU task cancelled while queued).
    let cancelled_task = Task::new(
        TaskSpec::command("echo", vec!["never".into()]),
        TaskRequirements::gpu(10),
    );
    let cancelled_id = master.submit_task(cancelled_task).await.unwrap();
    master.cancel_task(cancelled_id).await.unwrap();
    assert_eq!(
        master.queue().get_state(&cancelled_id).await,
        Some(TaskState::Cancelled)
    );

    for codec in CODECS {
        // v11 client sees 1 and 1.
        let stream_v11 = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut client_v11 = MessageTransport::with_codec(stream_v11, codec);
        client_v11
            .send_msg(&ClientMessage::ClusterStatus)
            .await
            .unwrap();
        match client_v11
            .recv_msg::<ClientResponse>()
            .await
            .unwrap()
            .expect("v11 ClusterStatus")
        {
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                cancelled_tasks,
                ..
            } => {
                assert_eq!(total_tasks, 2, "{codec:?}");
                assert_eq!(pending_tasks, 0, "{codec:?}");
                assert_eq!(running_tasks, 0, "{codec:?}");
                assert_eq!(completed_tasks, 0, "{codec:?}");
                assert_eq!(failed_tasks, 1, "{codec:?}: BUG-22 failed count");
                assert_eq!(cancelled_tasks, 1, "{codec:?}: BUG-22 cancelled count");
            }
            other => panic!("expected ClusterStatus, got {other:?}"),
        }

        // v10 client sees combined failed_tasks = 2 with v10 wire shape and stamp 10.
        let stream_v10 = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut client_v10 = MessageTransport::with_codec(stream_v10, codec);
        match v10_call(&mut client_v10, codec, ClientMessage::ClusterStatus).await {
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                cancelled_tasks,
                ..
            } => {
                assert_eq!(total_tasks, 2, "{codec:?}");
                assert_eq!(pending_tasks, 0, "{codec:?}");
                assert_eq!(running_tasks, 0, "{codec:?}");
                assert_eq!(completed_tasks, 0, "{codec:?}");
                assert_eq!(
                    failed_tasks, 2,
                    "{codec:?}: v10 sees failed + cancelled combined"
                );
                assert_eq!(cancelled_tasks, 0, "{codec:?}");
            }
            other => panic!("expected ClusterStatus, got {other:?}"),
        }
    }

    let _ = master.shutdown();
}

/// Peers below `PROTOCOL_VERSION_MIN_COMPATIBLE` (10) — such as protocol 9 — are refused by a v11
/// master with a clear version error naming `10..=11`.
#[tokio::test]
async fn v9_peers_are_refused_outside_window() {
    let master = spawn_master().await;

    // 1. v9 worker.
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut worker = MessageTransport::new(stream);
    worker
        .send_msg(&WorkerMessage::Register {
            worker_id: Uuid::new_v4(),
            capabilities: WorkerCapabilities::new("v9-worker", 2, 4_096, false, false, None),
            protocol_version: 9,
            features: ProtocolCapabilities::all_supported(),
        })
        .await
        .unwrap();
    match worker.recv_msg::<MasterMessage>().await.unwrap() {
        Some(MasterMessage::RegisterAck {
            accepted: false,
            message: Some(msg),
            ..
        }) => {
            assert!(
                msg.contains("Incompatible protocol version 9")
                    && msg.contains(&format!(
                        "{PROTOCOL_VERSION_MIN_COMPATIBLE}..={PROTOCOL_VERSION_CURRENT}"
                    )),
                "unexpected v9 worker refusal: {msg}"
            );
        }
        other => panic!("expected rejected RegisterAck, got {other:?}"),
    }

    // 2. v9 client Hello frame.
    let mut hello = serialize_message(&ClientMessage::Hello, WireCodec::Bincode)
        .unwrap()
        .to_vec();
    let len = hello.len();
    hello[len - 4..].copy_from_slice(&9u32.to_le_bytes());

    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);
    client.send_raw_frame(hello.into()).await.unwrap();
    match client.recv_msg::<ClientResponse>().await.unwrap() {
        Some(ClientResponse::Error { message }) => {
            assert!(
                message.contains("Incompatible protocol version 9")
                    && message.contains(&format!(
                        "{PROTOCOL_VERSION_MIN_COMPATIBLE}..={PROTOCOL_VERSION_CURRENT}"
                    )),
                "unexpected v9 client refusal: {message}"
            );
        }
        other => panic!("expected ClientResponse::Error, got {other:?}"),
    }

    let _ = master.shutdown();
}
