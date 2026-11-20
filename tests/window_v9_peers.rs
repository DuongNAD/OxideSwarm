//! A v10 master with peers one protocol version back (design 0083, PROTO-4).
//!
//! The v9 peers here speak the v9 layouts that `crates/core/tests/data/wire_layout_previous.txt`
//! (the layout file of release v0.3.0) pins: `deserialize_message_as_v9` is the frozen v9 reader,
//! `serialize_v9_client_message` the v9 writer, and both are checked against the recorded v0.3.0
//! bytes by `crates/core/src/window_tests.rs`. Messages whose layout did not change between 9 and
//! 10 (`Register`, `Heartbeat`, `TaskResult`, ...) are encoded with the current encoder, which is
//! the same bytes. The real v0.3.0 binaries run in `scripts/release_skew.py`.
//!
//! Tests: a v9 worker registers and runs a task; a critical `Worker` extension never reaches a v9
//! worker; a v9 client submits, polls and lists; both codecs.

use std::time::Duration;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::placement::PROTO_TAG_PREFIX;
use rusty_grid_core::protocol::{
    deserialize_message, deserialize_message_as_v9, serialize_message, serialize_v9_client_message,
    ClientMessage, ClientResponse, MasterMessage, MessageTransport, ProtocolCapabilities,
    WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_core::task::{
    Bytes, Extension, Task, TaskId, TaskRequirements, TaskResult, TaskSpec,
};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::scheduler::SchedulerConfig;
use rusty_grid_master::server::{
    MasterHandle, MasterServer, ServerConfig, DEFAULT_MASTER_MAX_BATCH_SIZE,
};
use rusty_grid_master::ReaperConfig;
use rusty_grid_worker::client::WorkerClient;
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

const V9: u32 = 9;
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
    let sched_cfg = SchedulerConfig::default()
        .with_max_batch_size(DEFAULT_MASTER_MAX_BATCH_SIZE)
        .without_host_cpu_backpressure();
    MasterServer::spawn_with_config(
        ServerConfig::new("127.0.0.1:0".parse().unwrap()),
        sched_cfg,
        ReaperConfig::default(),
    )
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

/// A worker that speaks protocol 9 over a raw transport.
struct V9Worker {
    transport: MessageTransport<TcpStream>,
    id: Uuid,
    codec: WireCodec,
}

impl V9Worker {
    async fn register(master: &MasterHandle, name: &str, codec: WireCodec) -> Self {
        let stream = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut transport = MessageTransport::with_codec(stream, codec);
        let id = Uuid::new_v4();
        // `Register` has the same layout in 9 and 10; the version field says 9.
        transport
            .send_msg(&WorkerMessage::Register {
                worker_id: id,
                capabilities: WorkerCapabilities::new(name, 4, 8_192, false, false, None),
                protocol_version: V9,
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
                assert!(accepted, "the v10 master accepts a v9 worker");
                // The frozen v9 reader already requires 9; spell it out.
                assert_eq!(protocol_version, V9, "the ack echoes the worker's version");
            }
            other => panic!("expected RegisterAck, got {other:?}"),
        }
        worker
    }

    /// The next message the master sends, read the way a v9 worker reads it.
    async fn next_message(&mut self, timeout: Duration) -> Option<MasterMessage> {
        let frame = tokio::time::timeout(timeout, self.transport.recv_raw_frame())
            .await
            .ok()?
            .expect("read")?;
        let (msg, codec) = deserialize_message_as_v9::<MasterMessage>(&frame)
            .unwrap_or_else(|e| panic!("a v9 worker cannot read a frame of the master: {e}"));
        assert_eq!(codec, self.codec);
        // The raw frame is the v9 layout: a task-carrying message does not parse as v10.
        if matches!(
            msg,
            MasterMessage::AssignTask { .. } | MasterMessage::AssignTaskWithCheckpoint { .. }
        ) && codec == WireCodec::Bincode
        {
            assert!(
                deserialize_message::<MasterMessage>(&frame).is_err(),
                "AssignTask for a v9 worker carries the v9 task layout"
            );
        }
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
            let (msg, _) = deserialize_message_as_v9::<MasterMessage>(&frame).unwrap();
            if let MasterMessage::AssignTask { task } = msg {
                return Some((task, frame.to_vec()));
            }
        }
    }

    async fn complete(&mut self, task_id: TaskId) {
        let result = TaskResult {
            worker_id: self.id,
            task_id,
            exit_code: 0,
            stdout: Bytes::from_static(b"window\n"),
            stderr: Bytes::new(),
            execution_time_ms: 1,
            is_gpu_executed: false,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: Vec::new(),
            outputs: Vec::new(),
        };
        // `TaskResult` has the same layout in 9 and 10.
        self.transport
            .send_msg(&WorkerMessage::from(result))
            .await
            .unwrap();
    }
}

/// A v9 worker registers with a v10 master and runs a task. The copy it receives has neither
/// extension list; the stored task keeps them; the worker is listed as `proto:9`.
#[tokio::test]
async fn v9_worker_registers_and_runs_a_task() {
    for codec in CODECS {
        let master = spawn_master().await;
        let mut worker = V9Worker::register(&master, "v9-worker", codec).await;

        let listed = master.list_workers().await.unwrap();
        let info = listed.iter().find(|w| w.worker_id == worker.id).unwrap();
        assert!(
            info.capabilities
                .tags
                .contains(&format!("{PROTO_TAG_PREFIX}{V9}")),
            "{codec:?}: tags {:?}",
            info.capabilities.tags
        );

        // A non-critical extension and one the master enforces itself.
        let task = echo_task(vec![hint(), master_limit()]);
        let task_id = master.submit_task(task.clone()).await.unwrap();

        let (assigned, frame) = worker
            .next_assigned_task(Duration::from_secs(5))
            .await
            .unwrap_or_else(|| panic!("{codec:?}: the v9 worker got no task"));
        assert_eq!(assigned.id, task_id);
        assert!(
            assigned.extensions.is_empty() && assigned.requirements.extensions.is_empty(),
            "{codec:?}: a v9 worker's copy has no extensions: {assigned:?}"
        );
        // Everything else is the task as submitted.
        let mut expected = task.clone();
        expected.extensions.clear();
        assert_eq!(assigned.spec, expected.spec);
        assert_eq!(assigned.requirements, expected.requirements);
        assert_eq!(assigned.tags, expected.tags);
        if codec == WireCodec::Bincode {
            // The v9 frame is exactly the two empty list prefixes (8 bytes each) shorter than the
            // same task in the v10 layout.
            let v10 =
                serialize_message(&MasterMessage::AssignTask { task: assigned }, codec).unwrap();
            assert_eq!(
                v10.len(),
                frame.len() + 16,
                "v9 layout drops two list prefixes"
            );
        }

        // The stored task keeps every extension.
        let stored = master.queue().get_task_spec(&task_id).await.unwrap();
        assert_eq!(stored.extensions, vec![hint(), master_limit()]);

        worker.complete(task_id).await;
        let result = master
            .wait_task(task_id, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.worker_id, worker.id);
        let _ = master.shutdown();
    }
}

/// A critical extension the worker must understand is never placed on a v9 worker (it has no
/// `ext:` tags); it waits, and runs when a v10 worker that carries the tag joins.
#[tokio::test]
async fn critical_worker_extension_never_reaches_a_v9_worker() {
    let master = spawn_master().await;
    let mut v9 = V9Worker::register(&master, "v9-worker", WireCodec::Bincode).await;

    let task_id = master
        .submit_task(echo_task(vec![worker_limit()]))
        .await
        .unwrap();
    assert!(
        v9.next_assigned_task(Duration::from_millis(1_200))
            .await
            .is_none(),
        "a task with a critical Worker extension must not reach a v9 worker"
    );
    assert_eq!(
        master.queue().get_state(&task_id).await,
        Some(TaskState::Queued)
    );

    // A v10 worker advertises `ext:32770` and takes it.
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut v10 = WorkerClient::for_testing(
        master.server_addr().to_string(),
        Some("v10-worker".into()),
        Some(2),
        false,
    );
    let v10_id = v10.worker_id();
    tokio::spawn(async move {
        let _ = v10.run(stop_rx).await;
    });
    let result = master
        .wait_task(task_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(result.worker_id, v10_id);
    assert_eq!(result.exit_code, 0);
    let _ = stop_tx.send(true);
    let _ = master.shutdown();
}

/// A v9 client: `Hello`, `SubmitTask` (waiting), `GetTaskResult`, `GetTaskStatus`, `ListWorkers`
/// and `ClusterStatus` against a v10 master. Every reply is stamped 9 and decodes with the frozen
/// v9 reader, not with the v10 one; the listing shows `proto:<n>`.
#[tokio::test]
async fn v9_client_round_trip() {
    for codec in CODECS {
        let master = spawn_master().await;
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut worker = WorkerClient::for_testing(
            master.server_addr().to_string(),
            Some("v10-worker".into()),
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
        .expect("the v10 worker registers");

        let stream = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut client = MessageTransport::with_codec(stream, codec);

        async fn call(
            client: &mut MessageTransport<TcpStream>,
            codec: WireCodec,
            msg: ClientMessage,
        ) -> ClientResponse {
            let frame = serialize_v9_client_message(&msg, codec).unwrap();
            client.send_raw_frame(frame).await.unwrap();
            let reply = client.recv_raw_frame().await.unwrap().expect("a reply");
            let (resp, _) = deserialize_message_as_v9::<ClientResponse>(&reply)
                .unwrap_or_else(|e| panic!("a v9 client cannot read the reply: {e}"));
            if !matches!(resp, ClientResponse::Error { .. }) {
                // Stamped 9: the v10 reader (which takes only its own version) refuses it.
                assert!(
                    deserialize_message::<ClientResponse>(&reply).is_err(),
                    "the reply to a v9 client is stamped 9"
                );
            }
            resp
        }

        match call(&mut client, codec, ClientMessage::Hello).await {
            ClientResponse::Hello {
                min_compatible_version,
            } => assert_eq!(min_compatible_version, V9),
            other => panic!("expected Hello, got {other:?}"),
        }

        // A task with extensions cannot come from a v9 client; the task has none.
        let task = echo_task(Vec::new());
        let task_id = task.id;
        match call(
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
        match call(&mut client, codec, ClientMessage::GetTaskResult { task_id }).await {
            ClientResponse::TaskCompleted { result, .. } => assert_eq!(result.exit_code, 0),
            other => panic!("expected TaskCompleted, got {other:?}"),
        }
        match call(&mut client, codec, ClientMessage::GetTaskStatus { task_id }).await {
            ClientResponse::TaskStatusInfo { task_id: id, .. } => assert_eq!(id, task_id),
            other => panic!("expected TaskStatusInfo, got {other:?}"),
        }
        match call(&mut client, codec, ClientMessage::ListWorkers).await {
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
        match call(&mut client, codec, ClientMessage::ClusterStatus).await {
            ClientResponse::ClusterStatus {
                completed_tasks,
                workers,
                ..
            } => {
                assert_eq!(completed_tasks, 1);
                assert_eq!(workers.len(), 1);
            }
            other => panic!("expected ClusterStatus, got {other:?}"),
        }

        let _ = stop_tx.send(true);
        let _ = master.shutdown();
    }
}

/// A v9 client's `SubmitTask` with `created_at_utc = 0` (a v9 frame that also parses as v10)
/// is a v9 task: it runs, and the task has no extensions.
#[tokio::test]
async fn v9_client_submit_with_created_at_zero_is_a_v9_task() {
    let master = spawn_master().await;
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut worker = WorkerClient::for_testing(
        master.server_addr().to_string(),
        Some("v10-worker".into()),
        Some(2),
        false,
    );
    tokio::spawn(async move {
        let _ = worker.run(stop_rx).await;
    });
    let stream = TcpStream::connect(master.server_addr()).await.unwrap();
    let mut client = MessageTransport::new(stream);
    let mut task = echo_task(Vec::new());
    task.created_at_utc = 0;
    let task_id = task.id;
    let frame = serialize_v9_client_message(
        &ClientMessage::SubmitTask { task, wait: true },
        WireCodec::Bincode,
    )
    .unwrap();
    client.send_raw_frame(frame).await.unwrap();
    let reply = client.recv_raw_frame().await.unwrap().expect("a reply");
    match deserialize_message_as_v9::<ClientResponse>(&reply)
        .unwrap()
        .0
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
    let _ = stop_tx.send(true);
    let _ = master.shutdown();
}

impl V9Worker {
    /// Like `register`, with tags the worker supplies itself.
    async fn register_with_tags(master: &MasterHandle, name: &str, tags: Vec<String>) -> Self {
        let stream = TcpStream::connect(master.server_addr()).await.unwrap();
        let mut transport = MessageTransport::with_codec(stream, WireCodec::Bincode);
        let id = Uuid::new_v4();
        let mut caps = WorkerCapabilities::new(name, 4, 8_192, false, false, None);
        caps.tags = tags;
        transport
            .send_msg(&WorkerMessage::Register {
                worker_id: id,
                capabilities: caps,
                protocol_version: V9,
                features: ProtocolCapabilities::all_supported(),
            })
            .await
            .unwrap();
        let mut worker = Self {
            transport,
            id,
            codec: WireCodec::Bincode,
        };
        assert!(matches!(
            worker.next_message(Duration::from_secs(5)).await,
            Some(MasterMessage::RegisterAck { accepted: true, .. })
        ));
        worker
    }
}

/// M1: a v9 worker does not reserve `ext:` / `proto:`, so it may register them. The master drops
/// `ext:*` from a worker below protocol 10 and `proto:*` from every worker (it adds its own), so a
/// task with a critical Worker extension still never reaches the v9 worker.
#[tokio::test]
async fn v9_worker_cannot_forge_ext_or_proto_tags() {
    let master = spawn_master().await;
    let mut v9 = V9Worker::register_with_tags(
        &master,
        "forger",
        vec!["ext:32770".into(), "proto:10".into(), "zone:lab".into()],
    )
    .await;
    let listed = master.list_workers().await.unwrap();
    let tags = &listed
        .iter()
        .find(|w| w.worker_id == v9.id)
        .unwrap()
        .capabilities
        .tags;
    assert!(tags.contains(&"zone:lab".to_string()), "{tags:?}");
    assert!(!tags.iter().any(|t| t.starts_with("ext:")), "{tags:?}");
    let protos: Vec<_> = tags.iter().filter(|t| t.starts_with("proto:")).collect();
    assert_eq!(protos, vec![&format!("{PROTO_TAG_PREFIX}{V9}")], "{tags:?}");

    let task_id = master
        .submit_task(echo_task(vec![worker_limit()]))
        .await
        .unwrap();
    assert!(
        v9.next_assigned_task(Duration::from_millis(1_200))
            .await
            .is_none(),
        "a forged ext: tag must not attract a critical task"
    );
    assert_eq!(
        master.queue().get_state(&task_id).await,
        Some(TaskState::Queued)
    );
    let _ = master.shutdown();
}

/// S3: a task resumed from a checkpoint reaches a v9 worker as `AssignTaskWithCheckpoint` in the
/// v9 layout: a first v9 worker takes it, reports a checkpoint and disconnects; a second one gets
/// the resume message without extension lists.
#[tokio::test]
async fn v9_worker_gets_assign_task_with_checkpoint_after_failover() {
    let master = spawn_master().await;
    let mut first = V9Worker::register(&master, "v9-first", WireCodec::Bincode).await;
    let task_id = master.submit_task(echo_task(vec![hint()])).await.unwrap();
    first
        .next_assigned_task(Duration::from_secs(5))
        .await
        .expect("first v9 worker gets the task");
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

    let mut second = V9Worker::register(&master, "v9-second", WireCodec::Bincode).await;
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
            assert!(task.extensions.is_empty() && task.requirements.extensions.is_empty());
            assert_eq!(
                latest_checkpoint.expect("the checkpoint").delta_state,
                Bytes::from_static(b"state")
            );
            break;
        }
    }
    let _ = master.shutdown();
}
