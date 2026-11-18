//! End-to-end tests of member revocation across every connection lane (`ENROLL-1c`, design record
//! `docs/design/0051-machine-identity-and-invites.md` D5, "Revocation").
//!
//! A loopback master holds two enrolled members, "revoked" and "kept", each with the `worker` and
//! `client` roles. After `oxideswarm members revoke <revoked>`:
//! 1. every connection of the revoked member ends at once: its worker session (TCP, or the two
//!    QUIC lanes), a `submit --wait` that is still waiting, a client connection that holds a
//!    `GetTaskResult` answer, a `BlobConnection`, and P2P `STREAM_CLIENT` connections;
//! 2. the task that ran on the revoked member's worker is requeued at once and runs on the kept
//!    member's worker, although the master holds disconnected workers' tasks for 3,600 s;
//! 3. the revoked member's next OXS-AUTH v3 handshake is refused;
//! 4. the kept member's connections and worker stay up.
//!
//! Every wait is a condition with a 30 s bound; no test sleeps for a fixed time.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_cli::blobs::BlobConnection;
use rusty_grid_cli::connect::{connect_master, with_client_v3_auth};
use rusty_grid_core::blob::BlobId;
use rusty_grid_core::blob_transfer::{BlobChannel, BlobError, BlobRequest};
use rusty_grid_core::noise_auth::{NodeKeypair, PublicKey};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::{MessageTransport, Task, TaskId, TaskRequirements, TaskSpec, TaskStatus};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

/// Bound of every wait in these tests.
const BOUND: Duration = Duration::from_secs(30);

/// The master holds a disconnected worker's tasks this long; a revocation must not wait for it.
const RESUME_HOLD_SECS: &str = "3600";

fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .current_dir(home)
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("RUSTY_GRID_MASTER_KEY")
        .env_remove("RUSTY_GRID_AUTH")
        .env_remove("RUSTY_GRID_CONFIG")
        .env_remove("RUSTY_GRID_MASTER")
        .env_remove("RUSTY_GRID_MASTER_ADDR")
        .env_remove("RUSTY_GRID_LISTEN")
        .env_remove("RUSTY_GRID_MASTER_LISTEN")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_P2P")
        .env_remove("RUSTY_GRID_P2P_TICKET")
        .env_remove("RUSTY_GRID_SESSION_RESUME_HOLD_SECS")
        .env_remove("RUST_LOG")
        .env("RUSTY_GRID_P2P_MINIMAL", "1")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

fn with(home: &Path, args: &[&str]) -> Command {
    let mut cmd = cli(home);
    cmd.args(args);
    cmd
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn run_cmd(mut cmd: Command) -> Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    cmd.output().expect("run rusty-grid")
}

fn wait_for(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    condition()
}

/// A background process whose combined output goes to a log file.
struct Service {
    child: Child,
    log: PathBuf,
}

impl Service {
    fn spawn(mut cmd: Command, log: PathBuf) -> Self {
        let out = fs::File::create(&log).expect("create log");
        let err = out.try_clone().expect("clone log handle");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        let child = cmd.spawn().expect("spawn service");
        Self { child, log }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn exit_status(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().expect("try_wait")
    }

    /// Kills the process and waits for it to exit (a second call is harmless).
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
    }
}

/// An enrolled machine: a private home with its own `node.key`.
struct Member {
    home: TempDir,
    kp: NodeKeypair,
}

impl Member {
    fn new() -> Self {
        let home = TempDir::new().unwrap();
        let kp = NodeKeypair::generate().unwrap();
        kp.save_file(&home.path().join(".oxideswarm").join("node.key"), false)
            .unwrap();
        Self { home, kp }
    }

    fn id(&self) -> String {
        self.kp.member_id()
    }
}

/// A master started on loopback with `members.json` listing `members` as `worker,client`.
///
/// Fields drop in declaration order, so the processes come before `home` (TEST-33): the master
/// and its workers are stopped before the master's home is deleted, and `Drop` stops the workers
/// before the master.
struct Cluster {
    workers: Vec<Service>,
    master: Service,
    addr: String,
    ticket: Option<String>,
    master_pub: PublicKey,
    home: TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            worker.stop();
        }
        self.master.stop();
    }
}

impl Cluster {
    fn start(members: &[(&str, &Member)], p2p: bool) -> Self {
        let home = TempDir::new().unwrap();
        let entries: Vec<serde_json::Value> = members
            .iter()
            .map(|(name, m)| {
                serde_json::json!({
                    "id": m.id(),
                    "name": name,
                    "public_key": m.kp.public_key().to_hex(),
                    "roles": ["worker", "client"],
                })
            })
            .collect();
        let raw = serde_json::to_string_pretty(&serde_json::json!({
            "version": 1,
            "members": entries,
        }))
        .unwrap();
        let members_path = home.path().join(".oxideswarm").join("members.json");
        fs::create_dir_all(members_path.parent().unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::write(&members_path, &raw).unwrap();
            fs::set_permissions(&members_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        #[cfg(windows)]
        rusty_grid_core::private_file::write_private_replace(&members_path, raw.as_bytes())
            .unwrap();

        let port_file = home.path().join("master.port");
        let ticket_file = home.path().join("master.ticket");
        let mut cmd = cli(home.path());
        cmd.args(["master", "--listen", "127.0.0.1:0", "--port-file"])
            .arg(&port_file)
            .args(["--session-resume-hold-secs", RESUME_HOLD_SECS]);
        if p2p {
            cmd.arg("--p2p").arg("--p2p-ticket-file").arg(&ticket_file);
        }
        let master = Service::spawn(cmd, home.path().join("master.log"));

        let mut port = None;
        let mut ticket = None;
        assert!(
            wait_for(BOUND, || {
                port = fs::read_to_string(&port_file)
                    .ok()
                    .and_then(|p| p.trim().parse::<u16>().ok());
                ticket = fs::read_to_string(&ticket_file)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                port.is_some()
                    && (!p2p || ticket.is_some())
                    && master.log().contains("Master v3 public key:")
            }),
            "master did not start; log:\n{}",
            master.log()
        );
        let master_pub =
            *NodeKeypair::load_file(&home.path().join(".oxideswarm").join("master_noise.key"))
                .unwrap()
                .public_key();
        Self {
            workers: Vec::new(),
            master,
            addr: format!("127.0.0.1:{}", port.unwrap()),
            ticket,
            master_pub,
            home,
        }
    }

    fn master_key_hex(&self) -> String {
        self.master_pub.to_hex()
    }

    /// Starts a worker of `member` named `name`, over TCP or over QUIC with the P2P ticket.
    fn spawn_worker(&mut self, member: &Member, name: &str, over_p2p: bool) -> usize {
        let (flag, target) = if over_p2p {
            ("--p2p-ticket", self.ticket.clone().expect("P2P ticket"))
        } else {
            ("--master", self.addr.clone())
        };
        let mut cmd = cli(member.home.path());
        cmd.args([
            "worker",
            "--name",
            name,
            flag,
            &target,
            "--master-key",
            &self.master_key_hex(),
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(member.home.path().join("sandbox"));
        let log = member.home.path().join(format!("{name}.log"));
        self.workers.push(Service::spawn(cmd, log));
        self.workers.len() - 1
    }

    fn logs(&self) -> String {
        let mut all = format!("master log:\n{}\n", self.master.log());
        for w in &self.workers {
            all.push_str(&format!("{}:\n{}\n", w.log.display(), w.log()));
        }
        all
    }

    /// Runs `oxideswarm members revoke <member>` on the master's host (its own admin key).
    fn revoke(&self, member: &Member) -> String {
        let out = run_cmd(with(
            self.home.path(),
            &["members", "revoke", &member.id(), "--master", &self.addr],
        ));
        assert!(
            out.status.success(),
            "members revoke failed:\n{}\n{}\n{}",
            text(&out.stdout),
            text(&out.stderr),
            self.logs()
        );
        text(&out.stdout)
    }
}

/// An authenticated client connection of `member` to `target`, opened from this process.
fn connect_as(
    rt: &Runtime,
    member: &Member,
    master_pub: PublicKey,
    target: &str,
) -> MessageTransport<GridStream> {
    rt.block_on(with_client_v3_auth(
        Some((member.kp.clone(), master_pub)),
        connect_master(target, None),
    ))
    .expect("member connects")
}

fn request(
    rt: &Runtime,
    conn: &mut MessageTransport<GridStream>,
    msg: &ClientMessage,
) -> ClientResponse {
    rt.block_on(async {
        conn.send_msg(msg).await.expect("send request");
        tokio::time::timeout(BOUND, conn.recv_msg::<ClientResponse>())
            .await
            .expect("answer within the bound")
            .expect("readable answer")
            .expect("connection still open")
    })
}

/// Waits (up to the bound) for the master to end `conn` without having been sent anything.
fn closed_by_master(rt: &Runtime, conn: &mut MessageTransport<GridStream>) -> bool {
    rt.block_on(async {
        match tokio::time::timeout(BOUND, conn.recv_msg::<ClientResponse>()).await {
            Ok(Ok(None)) | Ok(Err(_)) => true,
            Ok(Ok(Some(other))) => panic!("unexpected message on an idle connection: {other:?}"),
            Err(_) => false,
        }
    })
}

/// The id of the live worker named `name`, if the master lists one.
fn live_worker_id(
    rt: &Runtime,
    conn: &mut MessageTransport<GridStream>,
    name: &str,
) -> Option<(Uuid, Vec<String>)> {
    let ClientResponse::WorkerList { workers } = request(rt, conn, &ClientMessage::ListWorkers)
    else {
        panic!("ListWorkers must answer WorkerList");
    };
    workers.into_iter().find_map(|w| {
        let live = w
            .tags
            .iter()
            .any(|t| t == "state:connected" || t == "state:busy");
        let id = w
            .tags
            .iter()
            .find_map(|t| t.strip_prefix("id:"))
            .and_then(|s| s.parse().ok());
        match id {
            Some(id) if live && w.name == name => Some((id, w.tags)),
            _ => None,
        }
    })
}

fn task_status(
    rt: &Runtime,
    conn: &mut MessageTransport<GridStream>,
    task_id: TaskId,
) -> (TaskStatus, Option<Uuid>) {
    match request(rt, conn, &ClientMessage::GetTaskStatus { task_id }) {
        ClientResponse::TaskStatusInfo {
            status,
            assigned_worker,
            ..
        } => (status, assigned_worker),
        other => panic!("GetTaskStatus answered {other:?}"),
    }
}

fn total_tasks(rt: &Runtime, conn: &mut MessageTransport<GridStream>) -> usize {
    match request(rt, conn, &ClientMessage::ClusterStatus) {
        ClientResponse::ClusterStatus { total_tasks, .. } => total_tasks,
        other => panic!("ClusterStatus answered {other:?}"),
    }
}

/// A task that runs (sleeps) far longer than the test.
fn long_task() -> Task {
    Task::new(
        TaskSpec::BuiltinTest {
            test_name: "sleep".into(),
            iterations: 1,
            duration_ms: 3_600_000,
            should_fail: false,
            require_gpu: false,
        },
        TaskRequirements::new(1, 64, false, 7_200),
    )
}

/// The master's log line for a closed client connection of `member` on `lane`.
fn closed_line(member: &Member, lane: &str) -> String {
    format!(
        "Closed the connection of a revoked member member_id={} lane=\"{lane}\"",
        member.id()
    )
}

/// The master's log line for a revoked member's worker taken out of service. Its session may end
/// through the registry (which aborts the handler) before its own close branch runs, so this line,
/// not the close line, is what every worker lane is sure to leave.
fn revoked_worker_line(worker_id: Uuid) -> String {
    format!(
        "Worker of a revoked member disconnected; requeuing its tasks without the resume hold \
         worker_id={worker_id}"
    )
}

fn probe_request() -> BlobRequest {
    BlobRequest::Missing {
        ids: vec![BlobId::of(b"cli_revoke_e2e probe")],
    }
}

#[test]
fn revoke_closes_every_tcp_lane_of_a_member_and_requeues_its_workers_task_at_once() {
    let rt = Runtime::new().unwrap();
    let revoked = Member::new();
    let kept = Member::new();
    let mut cluster = Cluster::start(&[("revoked", &revoked), ("kept", &kept)], false);
    let addr = cluster.addr.clone();
    let master_pub = cluster.master_pub;
    let master_key_hex = cluster.master_key_hex();

    // The kept member's client connection stays open for the whole test.
    let mut kept_conn = connect_as(&rt, &kept, master_pub, &addr);

    // 1. The revoked member's worker runs a long task.
    cluster.spawn_worker(&revoked, "worker-revoked", false);
    let mut revoked_wid = None;
    assert!(
        wait_for(BOUND, || {
            revoked_wid = live_worker_id(&rt, &mut kept_conn, "worker-revoked").map(|w| w.0);
            revoked_wid.is_some()
        }),
        "the revoked member's worker did not register:\n{}",
        cluster.logs()
    );
    let revoked_wid = revoked_wid.unwrap();
    let task = long_task();
    let task_id = task.id;
    match request(
        &rt,
        &mut kept_conn,
        &ClientMessage::SubmitTask { task, wait: false },
    ) {
        ClientResponse::TaskSubmitted { task_id: id } => assert_eq!(id, task_id),
        other => panic!("SubmitTask answered {other:?}"),
    }
    assert!(
        wait_for(BOUND, || task_status(&rt, &mut kept_conn, task_id)
            == (TaskStatus::Running, Some(revoked_wid))),
        "the task did not start on the revoked member's worker:\n{}",
        cluster.logs()
    );

    // 2. The kept member's worker joins once the task runs elsewhere.
    cluster.spawn_worker(&kept, "worker-kept", false);
    let mut kept_wid = None;
    assert!(
        wait_for(BOUND, || {
            kept_wid = live_worker_id(&rt, &mut kept_conn, "worker-kept").map(|w| w.0);
            kept_wid.is_some()
        }),
        "the kept member's worker did not register:\n{}",
        cluster.logs()
    );
    let kept_wid = kept_wid.unwrap();

    // 3. The revoked member's client lanes: a `submit --wait` for a task no worker matches, a
    //    connection holding a `GetTaskResult` answer, and a blob connection.
    let mut waiting_submit = Service::spawn(
        with(
            revoked.home.path(),
            &[
                "submit",
                "--master",
                &addr,
                "--master-key",
                &master_key_hex,
                "--worker",
                "no-such-worker",
                "--timeout-secs",
                "3600",
                "--wait",
                "--command",
                "echo",
                "--",
                "never-runs",
            ],
        ),
        revoked.home.path().join("submit-wait.log"),
    );
    assert!(
        wait_for(BOUND, || total_tasks(&rt, &mut kept_conn) == 2),
        "the revoked member's `submit --wait` did not reach the master:\n{}\n{}",
        waiting_submit.log(),
        cluster.logs()
    );
    let mut holding_conn = connect_as(&rt, &revoked, master_pub, &addr);
    match request(
        &rt,
        &mut holding_conn,
        &ClientMessage::GetTaskResult { task_id },
    ) {
        ClientResponse::TaskStatusInfo { .. } => {}
        other => panic!("GetTaskResult answered {other:?}"),
    }
    let blob_conn = rt
        .block_on(with_client_v3_auth(
            Some((revoked.kp.clone(), master_pub)),
            BlobConnection::connect(&addr, None),
        ))
        .expect("blob connection");
    rt.block_on(blob_conn.call(probe_request()))
        .expect("a blob request is answered before the revocation");
    assert!(
        waiting_submit.exit_status().is_none(),
        "`submit --wait` must still be waiting before the revocation:\n{}",
        waiting_submit.log()
    );

    // 4. Revoke: the worker session, `submit --wait`, the held connection and the blob
    //    connection are the member's four connections.
    let stdout = cluster.revoke(&revoked);
    assert!(
        stdout.contains(&format!(
            "Revoked member {} (disconnected 4 session(s))",
            revoked.id()
        )),
        "unexpected revoke output: {stdout}\n{}",
        cluster.logs()
    );

    // Every lane of the revoked member ends.
    assert!(
        closed_by_master(&rt, &mut holding_conn),
        "the connection holding a GetTaskResult answer stayed open:\n{}",
        cluster.logs()
    );
    match rt.block_on(blob_conn.call(probe_request())) {
        Err(BlobError::Transport(_)) => {}
        other => panic!("the blob connection must be closed by the master, got {other:?}"),
    }
    let mut exit = None;
    assert!(
        wait_for(BOUND, || {
            exit = waiting_submit.exit_status();
            exit.is_some()
        }),
        "`submit --wait` kept waiting after the revocation:\n{}",
        waiting_submit.log()
    );
    assert!(!exit.unwrap().success());
    assert!(
        waiting_submit
            .log()
            .contains("Master closed the connection before replying"),
        "`submit --wait` must end because the master closed it:\n{}",
        waiting_submit.log()
    );
    assert!(
        wait_for(BOUND, || {
            let log = cluster.master.log();
            log.contains(&closed_line(&revoked, "client (TCP)"))
                && log.contains(&revoked_worker_line(revoked_wid))
        }),
        "master log must record the closed client and worker lanes:\n{}",
        cluster.logs()
    );

    // The task is requeued at once (no 3,600 s hold) and runs on the kept member's worker.
    assert!(
        wait_for(BOUND, || task_status(&rt, &mut kept_conn, task_id)
            == (TaskStatus::Running, Some(kept_wid))),
        "the revoked worker's task was not requeued onto the kept worker:\n{}",
        cluster.logs()
    );

    // The revoked key is refused at its next handshake: the worker's reconnect and a new client.
    let refusal = format!("peer member {} (public key", revoked.id());
    assert!(
        wait_for(BOUND, || cluster.master.log().contains(&refusal)),
        "the revoked worker's reconnect was not refused:\n{}",
        cluster.logs()
    );
    let status = run_cmd(with(
        revoked.home.path(),
        &["status", "--master", &addr, "--master-key", &master_key_hex],
    ));
    assert_eq!(
        status.status.code(),
        Some(6),
        "a revoked member's client must fail authentication:\n{}\n{}",
        text(&status.stdout),
        text(&status.stderr)
    );
    assert!(live_worker_id(&rt, &mut kept_conn, "worker-revoked").is_none());

    // The kept member keeps its connection (answering above) and its worker.
    assert_eq!(
        live_worker_id(&rt, &mut kept_conn, "worker-kept").map(|w| w.0),
        Some(kept_wid)
    );
    let kept_blob = rt
        .block_on(with_client_v3_auth(
            Some((kept.kp.clone(), master_pub)),
            BlobConnection::connect(&addr, None),
        ))
        .expect("the kept member still connects");
    rt.block_on(kept_blob.call(probe_request()))
        .expect("the kept member's blob requests are answered");
}

#[test]
fn revoke_closes_quic_worker_lanes_and_p2p_client_streams() {
    rusty_grid_cli::connect::set_client_p2p_minimal_preset_for_testing(true);
    let rt = Runtime::new().unwrap();
    let revoked = Member::new();
    let kept = Member::new();
    let mut cluster = Cluster::start(&[("revoked", &revoked), ("kept", &kept)], true);
    let ticket = cluster.ticket.clone().expect("P2P ticket");
    let master_pub = cluster.master_pub;

    // Both members talk to the master over iroh `STREAM_CLIENT` streams.
    let mut kept_conn = connect_as(&rt, &kept, master_pub, &ticket);

    // The revoked member's worker connects over QUIC (control and data lanes).
    cluster.spawn_worker(&revoked, "worker-revoked-p2p", true);
    // The master lists the QUIC link once it tracks the connection, right after registration.
    let mut listed = None;
    assert!(
        wait_for(BOUND, || {
            listed = live_worker_id(&rt, &mut kept_conn, "worker-revoked-p2p");
            listed
                .as_ref()
                .is_some_and(|(_, tags)| !tags.iter().any(|t| t == "link:TCP/LAN"))
        }),
        "the revoked member's worker did not register over QUIC: {listed:?}\n{}",
        cluster.logs()
    );
    let (revoked_wid, _) = listed.unwrap();

    let mut revoked_conn = connect_as(&rt, &revoked, master_pub, &ticket);
    match request(&rt, &mut revoked_conn, &ClientMessage::ClusterStatus) {
        ClientResponse::ClusterStatus { .. } => {}
        other => panic!("ClusterStatus answered {other:?}"),
    }

    let stdout = cluster.revoke(&revoked);
    assert!(
        stdout.contains(&format!(
            "Revoked member {} (disconnected 2 session(s))",
            revoked.id()
        )),
        "unexpected revoke output: {stdout}\n{}",
        cluster.logs()
    );
    assert!(
        closed_by_master(&rt, &mut revoked_conn),
        "the revoked member's P2P client stream stayed open:\n{}",
        cluster.logs()
    );
    assert!(
        wait_for(BOUND, || {
            let log = cluster.master.log();
            log.contains(&closed_line(&revoked, "client (P2P STREAM_CLIENT)"))
                && log.contains(&revoked_worker_line(revoked_wid))
        }),
        "master log must record the closed P2P client stream and QUIC worker:\n{}",
        cluster.logs()
    );
    // The worker reconnects only once its QUIC connection ended, and that reconnect is refused.
    let refusal = format!("peer member {} (public key", revoked.id());
    assert!(
        wait_for(BOUND, || cluster.master.log().contains(&refusal)),
        "the revoked P2P worker's reconnect was not refused:\n{}",
        cluster.logs()
    );
    assert!(live_worker_id(&rt, &mut kept_conn, "worker-revoked-p2p").is_none());

    // The kept member's P2P stream still answers.
    match request(&rt, &mut kept_conn, &ClientMessage::ClusterStatus) {
        ClientResponse::ClusterStatus { .. } => {}
        other => panic!("ClusterStatus answered {other:?}"),
    }
}
