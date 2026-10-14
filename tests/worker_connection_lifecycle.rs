//! Worker connection lifecycle against scripted and real masters: what happens to running tasks,
//! heartbeats and identity when the connection to the Master drops, stalls, or is shut down.
//!
//! The worker is the system under test. Most tests drive it with a scripted TCP "master" that
//! speaks the real protocol (registration, assignments, cancellations) and then misbehaves in one
//! specific way; the telemetry and identity tests use a real `MasterServer`.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::{MobileCapabilities, MobileLiveStatus};
use rusty_grid_core::protocol::{deserialize_message, MasterMessage, MessageTransport, WorkerMessage};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec, TaskStatus};
use rusty_grid_core::GridResult;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::backoff::BackoffConfig;
use rusty_grid_worker::client::{ActiveTaskTable, WorkerClient, WorkerConfig};
use rusty_grid_worker::heartbeat::MobileTelemetryHandle;

const WAIT: Duration = Duration::from_secs(20);

// ------------------------------------------------------------------------------------------
// Harness
// ------------------------------------------------------------------------------------------

/// A TCP listener that plays the Master for exactly as long as a test script wants.
struct ScriptedMaster {
    listener: TcpListener,
    addr: SocketAddr,
}

struct Conn {
    transport: MessageTransport<TcpStream>,
    worker_id: Uuid,
}

impl ScriptedMaster {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        Self { listener, addr }
    }

    /// Accepts the next worker connection, reads its `Register` and acknowledges it.
    async fn accept(&self, heartbeat_secs: u64) -> Conn {
        let (stream, _) = tokio::time::timeout(WAIT, self.listener.accept())
            .await
            .expect("the worker connected in time")
            .unwrap();
        let mut transport = MessageTransport::new(stream);
        let register = tokio::time::timeout(Duration::from_secs(5), transport.recv_msg::<WorkerMessage>())
            .await
            .expect("Register in time")
            .expect("Register decodes")
            .expect("Register present");
        let WorkerMessage::Register { worker_id, .. } = register else {
            panic!("first message must be Register, got {register:?}");
        };
        transport
            .send_msg(&MasterMessage::RegisterAck {
                accepted: true,
                worker_id,
                heartbeat_interval_secs: heartbeat_secs,
                message: None,
            })
            .await
            .unwrap();
        Conn { transport, worker_id }
    }

    /// True if no worker connection arrives within `window`.
    async fn quiet_for(&self, window: Duration) -> bool {
        tokio::time::timeout(window, self.listener.accept()).await.is_err()
    }
}

impl Conn {
    async fn send(&mut self, msg: MasterMessage) {
        self.transport.send_msg(&msg).await.unwrap();
    }

    /// The next message other than a heartbeat, or `None` on EOF, error or timeout.
    async fn next(&mut self, within: Duration) -> Option<WorkerMessage> {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.checked_duration_since(Instant::now())?;
            match tokio::time::timeout(left, self.transport.recv_msg::<WorkerMessage>()).await {
                Ok(Ok(Some(WorkerMessage::Heartbeat { .. }))) => continue,
                Ok(Ok(Some(msg))) => return Some(msg),
                _ => return None,
            }
        }
    }

    async fn wait_for(
        &mut self,
        within: Duration,
        pred: impl Fn(&WorkerMessage) -> bool,
    ) -> Option<WorkerMessage> {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.checked_duration_since(Instant::now())?;
            let msg = self.next(left).await?;
            if pred(&msg) {
                return Some(msg);
            }
        }
    }
}

struct Worker {
    shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<GridResult<()>>,
    table: ActiveTaskTable,
    worker_id: Uuid,
    _sandbox: tempfile::TempDir,
}

fn start_worker(addr: SocketAddr, tweak: impl FnOnce(WorkerConfig) -> WorkerConfig) -> Worker {
    let sandbox = tempfile::tempdir().unwrap();
    let config = tweak(
        WorkerConfig::new(addr.to_string())
            .with_name("lifecycle-worker")
            .with_cores(4)
            .with_redirection_portal(false)
            .with_sandbox_base_dir(sandbox.path()),
    );
    let mut client = WorkerClient::new(config);
    client.set_backoff_config(BackoffConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(300),
        2.0,
        0.0,
    ));
    let table = std::sync::Arc::clone(client.active_task_table());
    let worker_id = client.worker_id();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move { client.run(shutdown_rx).await });
    Worker {
        shutdown_tx,
        handle,
        table,
        worker_id,
        _sandbox: sandbox,
    }
}

impl Worker {
    fn running(&self) -> bool {
        !self.handle.is_finished()
    }

    /// Requests shutdown and waits for `run()` to return.
    async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(15), self.handle)
            .await
            .expect("worker did not stop after the shutdown signal")
            .expect("worker task panicked")
            .expect("run() returned an error");
    }
}

/// True while a process with `pid` exists.
fn process_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn read_pid(path: &Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(pid) = std::fs::read_to_string(path)
            .ok()
            .and_then(|t| t.trim().parse::<u32>().ok())
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{} never received a pid", path.display());
}

async fn wait_dead(pid: u32, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if !process_alive(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    !process_alive(pid)
}

/// A task that records the pid of its long-lived process in `pid_file`.
fn sleeper(pid_file: &Path) -> Task {
    Task::new(
        TaskSpec::command(
            "sh",
            vec![
                "-c".into(),
                format!("echo $$ > {}; exec sleep 60", pid_file.display()),
            ],
        ),
        TaskRequirements::generic(1, 120),
    )
}

fn echo_task(id: TaskId, text: &str) -> Task {
    let mut task = Task::new(
        TaskSpec::command("echo", vec![text.into()]),
        TaskRequirements::generic(1, 30),
    );
    task.id = id;
    task
}

/// A task whose stdout is `bytes` zero bytes: a big result frame.
fn big_output_task(bytes: usize) -> Task {
    Task::new(
        TaskSpec::command("head", vec!["-c".into(), bytes.to_string(), "/dev/zero".into()]),
        TaskRequirements::generic(1, 60),
    )
}

fn is_result_of(msg: &WorkerMessage, id: TaskId) -> bool {
    matches!(msg, WorkerMessage::TaskResult { task_id, .. } if *task_id == id)
}

// ------------------------------------------------------------------------------------------
// Master shutdown
// ------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_master_shutdown_frame_makes_the_worker_wait_and_reconnect() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| c);

    let mut conn = master.accept(1).await;
    let sent_at = Instant::now();
    conn.send(MasterMessage::Shutdown {
        reason: "master restarting".into(),
        grace_period_secs: None,
    })
    .await;

    // The worker says goodbye...
    let farewell = conn
        .wait_for(Duration::from_secs(5), |m| matches!(m, WorkerMessage::Disconnecting { .. }))
        .await;
    assert!(farewell.is_some(), "the worker announces its departure");
    drop(conn);

    // ...and keeps running: it comes back as the same worker, promptly (a restarted master is
    // the common case; the existing fast-reconnect expectation is a few seconds at most).
    let again = master.accept(1).await;
    assert_eq!(again.worker_id, worker.worker_id);
    assert!(
        sent_at.elapsed() < Duration::from_secs(3),
        "reconnected only after {:?}",
        sent_at.elapsed()
    );
    assert!(worker.running(), "a Master Shutdown frame must not end the worker");

    // Only the local signal ends it.
    drop(again);
    worker.stop().await;
}

// ------------------------------------------------------------------------------------------
// Connection loss with work in flight
// ------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_connection_loss_cancels_running_tasks_and_a_redispatch_of_the_same_id_runs() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| c);
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");

    let mut conn = master.accept(1).await;
    let task = sleeper(&pid_file);
    let task_id = task.id;
    conn.send(MasterMessage::AssignTask { task }).await;
    conn.wait_for(WAIT, |m| matches!(m, WorkerMessage::TaskProgress { status: TaskStatus::Running, .. }))
        .await
        .expect("task started");
    let pid = read_pid(&pid_file).await;
    assert!(process_alive(pid));

    // The connection drops (Wi-Fi blip). The Master will requeue the task; nothing the worker
    // could still send for it would be accepted.
    drop(conn);

    assert!(
        wait_dead(pid, Duration::from_secs(10)).await,
        "the orphaned process {pid} must be killed when the session ends, not left running"
    );

    // The worker reconnects and the Master hands it the SAME task id again: it must just work
    // (it used to fail with "sandbox already exists", a terminal failure for the task).
    let mut conn = master.accept(1).await;
    conn.send(MasterMessage::AssignTask {
        task: echo_task(task_id, "second attempt"),
    })
    .await;
    let result = conn
        .wait_for(WAIT, |m| is_result_of(m, task_id))
        .await
        .expect("a result for the re-dispatched task");
    match result {
        WorkerMessage::TaskResult {
            exit_code, stdout, error, ..
        } => {
            assert_eq!(exit_code, 0, "re-dispatch failed: {error:?}");
            assert!(String::from_utf8_lossy(&stdout.0).contains("second attempt"));
        }
        other => panic!("{other:?}"),
    }
    // No stray cancellation of the old run may follow it (the Master would record the task as cancelled).
    assert!(
        conn.wait_for(Duration::from_millis(500), |m| is_result_of(m, task_id)).await.is_none(),
        "exactly one result per delivered task"
    );
    assert!(worker.table.read().await.is_empty());

    drop(conn);
    worker.stop().await;
}

#[tokio::test]
async fn test_master_shutdown_also_cancels_running_tasks() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| c);
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");

    let mut conn = master.accept(1).await;
    conn.send(MasterMessage::AssignTask { task: sleeper(&pid_file) }).await;
    let pid = read_pid(&pid_file).await;

    conn.send(MasterMessage::Shutdown { reason: "bye".into(), grace_period_secs: None }).await;
    assert!(wait_dead(pid, Duration::from_secs(10)).await, "process {pid} survived the Master's shutdown");
    drop(conn);
    worker.stop().await;
}

#[tokio::test]
async fn test_aborting_the_worker_task_closes_its_connection_to_the_master() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| c);
    let mut conn = master.accept(1).await;
    // Wait until the session is really up (its first heartbeat), so the abort lands on a worker
    // that has reader/writer tasks running, not on one still reading the RegisterAck.
    let heartbeat = tokio::time::timeout(Duration::from_secs(5), async {
        while let Ok(Some(msg)) = conn.transport.recv_msg::<WorkerMessage>().await {
            if matches!(msg, WorkerMessage::Heartbeat { .. }) {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(heartbeat, Ok(true), "the session never started");

    // A crash as the tests (and an embedding app) simulate it: the task running `run()` is
    // aborted. The connection's reader/writer tasks must not outlive it, or the Master would
    // never see the worker go.
    worker.handle.abort();
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Ok(Some(_)) = conn.transport.recv_msg::<WorkerMessage>().await {}
    })
    .await;
    assert!(ended.is_ok(), "the socket stayed open after the worker task was aborted");
}

// ------------------------------------------------------------------------------------------
// Dead peer detection
// ------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_silent_master_is_detected_and_the_worker_reconnects() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| c.with_dead_peer_timeout(Duration::from_millis(800)));

    // Registers, then the "master" goes dark: socket open, no reads, no acks, no FIN. Writes
    // into it keep succeeding, so only the watchdog can notice.
    let silent = master.accept(1).await;
    let started = Instant::now();
    let _second = master.accept(1).await;
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the worker must give up on a mute master within seconds, took {:?}",
        started.elapsed()
    );
    assert!(worker.running());
    drop(silent);
    worker.stop().await;
}

#[tokio::test]
async fn test_a_healthy_master_that_acks_heartbeats_is_never_dropped() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| c.with_dead_peer_timeout(Duration::from_millis(1800)));
    let mut conn = master.accept(1).await;

    // Several multiples of the dead-peer timeout, acknowledging every heartbeat like a real master.
    let until = Instant::now() + Duration::from_secs(6);
    let mut acked = 0;
    while Instant::now() < until {
        let left = until.saturating_duration_since(Instant::now());
        if let Ok(Ok(Some(WorkerMessage::Heartbeat { timestamp, .. }))) =
            tokio::time::timeout(left, conn.transport.recv_msg::<WorkerMessage>()).await
        {
            conn.send(MasterMessage::HeartbeatAck { timestamp }).await;
            acked += 1;
        }
    }
    assert!(acked >= 3, "heartbeats keep flowing ({acked})");
    assert!(master.quiet_for(Duration::from_millis(200)).await, "no reconnect while the master answers");
    assert!(worker.running());
    drop(conn);
    worker.stop().await;
}

// ------------------------------------------------------------------------------------------
// Large results versus control traffic
// ------------------------------------------------------------------------------------------

const BIG: usize = 40_000_000;

#[tokio::test]
async fn test_worker_keeps_servicing_the_master_while_a_large_result_cannot_be_sent() {
    let master = ScriptedMaster::start().await;
    // A generous dead-peer timeout keeps this test about servicing messages, not about the watchdog.
    let worker = start_worker(master.addr, |c| {
        c.with_dead_peer_timeout(Duration::from_secs(30))
            .with_max_output_bytes(BIG + 1024)
    });
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");

    let mut conn = master.accept(1).await;
    let victim = sleeper(&pid_file);
    let victim_id = victim.id;
    conn.send(MasterMessage::AssignTask { task: victim }).await;
    conn.wait_for(WAIT, |m| matches!(m, WorkerMessage::TaskProgress { status: TaskStatus::Running, .. }))
        .await
        .expect("victim running");
    let pid = read_pid(&pid_file).await;

    // A 40 MB result the master refuses to read: the worker's upload stalls in the middle of the frame.
    let big = big_output_task(BIG);
    let big_id = big.id;
    conn.send(MasterMessage::AssignTask { task: big }).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // While it is stuck, the worker must still act on what the master tells it (it used to sit
    // inside the blocked write and never look at the socket again).
    conn.send(MasterMessage::CancelTask {
        task_id: victim_id,
        reason: Some("cancelled during an upload".into()),
    })
    .await;
    assert!(
        wait_dead(pid, Duration::from_secs(5)).await,
        "CancelTask was not acted on while a large result was being written"
    );

    // Now the master reads again: the big result arrives intact, then the cancellation result.
    let result = conn
        .wait_for(WAIT, |m| is_result_of(m, big_id))
        .await
        .expect("the large result is delivered");
    if let WorkerMessage::TaskResult { stdout, exit_code, .. } = result {
        assert_eq!(exit_code, 0);
        assert_eq!(stdout.0.len(), BIG);
    }
    let cancelled = conn
        .wait_for(WAIT, |m| is_result_of(m, victim_id))
        .await
        .expect("the cancellation result");
    assert!(matches!(cancelled, WorkerMessage::TaskResult { exit_code: 130, .. }));
    assert!(worker.running());
    drop(conn);
    worker.stop().await;
}

/// Reads frames from a raw socket in small, paced chunks: a slow uplink as seen from the master.
struct SlowFrames {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl SlowFrames {
    async fn next(&mut self, chunk: usize, pause: Duration) -> Option<WorkerMessage> {
        loop {
            if self.buf.len() >= 4 {
                let len = u32::from_be_bytes(self.buf[..4].try_into().unwrap()) as usize;
                if self.buf.len() >= 4 + len {
                    let frame: Vec<u8> = self.buf.drain(..4 + len).skip(4).collect();
                    let (msg, _) = deserialize_message::<WorkerMessage>(&frame).ok()?;
                    return Some(msg);
                }
            }
            let mut tmp = vec![0u8; chunk];
            let n = tokio::time::timeout(Duration::from_secs(10), self.stream.read(&mut tmp))
                .await
                .ok()?
                .ok()?;
            if n == 0 {
                return None;
            }
            self.buf.extend_from_slice(&tmp[..n]);
            tokio::time::sleep(pause).await;
        }
    }
}

#[tokio::test]
async fn test_a_slow_but_progressing_upload_is_not_mistaken_for_a_dead_master() {
    let master = ScriptedMaster::start().await;
    // The dead-peer timeout is far shorter than the upload: only its progress keeps the link alive.
    let worker = start_worker(master.addr, |c| {
        c.with_dead_peer_timeout(Duration::from_millis(1200))
            .with_max_output_bytes(BIG + 1024)
    });
    let conn = master.accept(1).await;
    let Conn { mut transport, .. } = conn;
    let big = big_output_task(BIG);
    let big_id = big.id;
    transport.send_msg(&MasterMessage::AssignTask { task: big }).await.unwrap();

    // Switch to paced raw reads (13 MB/s): about 3 s for the part that does not fit the socket buffers.
    let parts = transport.into_inner().into_parts();
    let mut slow = SlowFrames {
        stream: parts.io,
        buf: parts.read_buf.to_vec(),
    };
    let started = Instant::now();
    let mut delivered = None;
    while let Some(msg) = slow.next(128 * 1024, Duration::from_millis(10)).await {
        if is_result_of(&msg, big_id) {
            delivered = Some(msg);
            break;
        }
    }
    let elapsed = started.elapsed();
    match delivered {
        Some(WorkerMessage::TaskResult { stdout, exit_code, .. }) => {
            assert_eq!(exit_code, 0);
            assert_eq!(stdout.0.len(), BIG);
        }
        other => panic!("the result was never delivered (upload took {elapsed:?}): {other:?}"),
    }
    assert!(
        elapsed > Duration::from_millis(1500),
        "the test must outlast the dead-peer timeout to mean anything (took {elapsed:?})"
    );
    assert!(
        master.quiet_for(Duration::from_millis(300)).await,
        "the worker dropped and re-established the connection in the middle of a live upload"
    );
    assert!(worker.running());
    worker.stop().await;
}

#[tokio::test]
async fn test_a_stalled_upload_to_a_master_that_stopped_reading_is_detected() {
    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| {
        c.with_dead_peer_timeout(Duration::from_millis(1000))
            .with_max_output_bytes(BIG + 1024)
    });
    let mut conn = master.accept(1).await;
    conn.send(MasterMessage::AssignTask { task: big_output_task(BIG) }).await;

    // The master never reads again: the upload fills the socket buffers and stops making progress.
    let started = Instant::now();
    let second = master.accept(1).await;
    assert_eq!(second.worker_id, worker.worker_id);
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "a stuck upload must be given up on, took {:?}",
        started.elapsed()
    );
    drop(conn);
    drop(second);
    worker.stop().await;
}

// ------------------------------------------------------------------------------------------
// Live mobile telemetry reaches the Master
// ------------------------------------------------------------------------------------------

async fn spawn_real_master() -> MasterHandle {
    MasterServer::spawn(ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_handshake_timeout(2))
        .await
        .expect("spawn master")
}

async fn wait_until<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    check().await
}

fn phone(battery_pct: u8, charging: bool) -> MobileCapabilities {
    MobileCapabilities {
        os_version: "Android 14".into(),
        soc_model: "Snapdragon 8 Gen 3".into(),
        battery_pct: Some(battery_pct),
        is_charging: Some(charging),
        thermal_throttled: false,
    }
}

#[tokio::test]
async fn test_live_battery_and_thermal_updates_reach_the_masters_scheduler_view() {
    let master = spawn_real_master().await;
    let handle = MobileTelemetryHandle::new(Some(phone(90, true)));
    let worker = start_worker(master.server_addr(), |c| c.with_mobile_telemetry(handle.clone()));

    let registered = wait_until(Duration::from_secs(10), || async {
        master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
    })
    .await;
    assert!(registered, "worker registered");
    let mobile_of = |master: &MasterHandle| {
        let registry = master.registry().clone();
        async move {
            registry
                .list_all_workers()
                .await
                .into_iter()
                .next()
                .and_then(|w| w.capabilities.mobile)
        }
    };
    assert_eq!(mobile_of(&master).await.unwrap().battery_pct, Some(90), "Register carried the initial state");

    // The phone is unplugged, drains and overheats after registration: the Android app writes the
    // new readings into the shared handle, and the master must see them without a re-registration.
    handle.update(MobileLiveStatus {
        battery_pct: Some(4),
        is_charging: Some(false),
        thermal_throttled: true,
    });
    let updated = wait_until(Duration::from_secs(10), || async {
        mobile_of(&master)
            .await
            .is_some_and(|m| m.battery_pct == Some(4) && m.thermal_throttled && m.is_charging == Some(false))
    })
    .await;
    assert!(updated, "the master still sees the registration-time snapshot");
    // Static descriptors from the registration survive.
    assert_eq!(mobile_of(&master).await.unwrap().soc_model, "Snapdragon 8 Gen 3");

    // Plugged in and cooled down again.
    handle.update(MobileLiveStatus {
        battery_pct: Some(5),
        is_charging: Some(true),
        thermal_throttled: false,
    });
    assert!(
        wait_until(Duration::from_secs(10), || async {
            mobile_of(&master)
                .await
                .is_some_and(|m| m.is_charging == Some(true) && !m.thermal_throttled)
        })
        .await
    );

    worker.stop().await;
    let _ = master.shutdown();
}

// ------------------------------------------------------------------------------------------
// Persistent identity
// ------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_a_restarted_worker_keeps_its_identity_at_the_master() {
    let master = spawn_real_master().await;
    let home = tempfile::tempdir().unwrap();
    let id_file = home.path().join("worker_id");

    let first = start_worker(master.server_addr(), |c| c.with_worker_id_file(&id_file));
    assert!(
        wait_until(Duration::from_secs(10), || async {
            master.list_workers().await.map(|w| w.len() == 1).unwrap_or(false)
        })
        .await
    );
    let first_id = first.worker_id;
    first.stop().await;

    // "Reboot": a new process (a new WorkerClient) on the same machine.
    let second = start_worker(master.server_addr(), |c| c.with_worker_id_file(&id_file));
    assert_eq!(second.worker_id, first_id, "same machine, same identity");
    assert!(
        wait_until(Duration::from_secs(10), || async {
            master
                .list_workers()
                .await
                .map(|w| w.iter().any(|i| i.worker_id == first_id && i.status != rusty_grid_master::registry::WorkerStatus::Disconnected))
                .unwrap_or(false)
        })
        .await
    );
    let all = master.list_workers().await.unwrap();
    assert_eq!(all.len(), 1, "no ghost entry of the previous run: {all:?}");

    second.stop().await;
    let _ = master.shutdown();
}

// ------------------------------------------------------------------------------------------
// P2P ticket with a TCP fallback
// ------------------------------------------------------------------------------------------

#[cfg(feature = "p2p")]
#[tokio::test]
async fn test_a_stale_p2p_ticket_falls_back_to_the_tcp_address() {
    // A syntactically valid ticket for a node that no longer exists (the master "restarted with a
    // new identity"): bind a throwaway endpoint, take its address, and close it again.
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![rusty_grid_core::transport::GRID_ALPN.to_vec()])
        .bind()
        .await
        .expect("local iroh endpoint");
    let stale_ticket = rusty_grid_core::transport::serialize_p2p_ticket(&endpoint.addr()).unwrap();
    endpoint.close().await;

    let master = ScriptedMaster::start().await;
    let worker = start_worker(master.addr, |c| {
        c.with_p2p_ticket(stale_ticket).with_connect_timeout(Duration::from_secs(2))
    });

    // Without the fallback the worker would redial the dead ticket forever and never show up here.
    let conn = master.accept(1).await;
    assert_eq!(conn.worker_id, worker.worker_id);
    assert!(worker.running());
    drop(conn);
    worker.stop().await;
}
