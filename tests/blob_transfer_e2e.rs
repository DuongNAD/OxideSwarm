//! End-to-end tests of content-addressed task inputs and output files
//! (`docs/design/0006-content-addressed-data-plane.md`).
//!
//! * An input directory goes from a client to a worker through the master's blob store, the
//!   worker's command reads it and writes output files, and the client gets them back byte for
//!   byte.
//! * Submitting the same inputs again uploads nothing and the worker fetches nothing.
//! * A `RustCompilation` task returns the library it compiled.
//! * A master without a blob store refuses tasks with files instead of losing them.
//! * The same round trip through the real `rusty-grid` binary (`submit --input/--output`), with
//!   a private `HOME`, port files and ephemeral ports.
//!
//! The shell-based tests run on Unix; the in-process ones bind 127.0.0.1:0 and keep every
//! directory in a temp dir, so nothing touches the developer's `~/.oxideswarm`. Workers run in
//! this process with a "plugged in" battery reading: a laptop on low battery would otherwise
//! (rightly) refuse the tasks and the tests would wait forever.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::sync::watch;

use rusty_grid_cli::blobs::{download_outputs, scan_inputs, upload_inputs, BlobConnection};
use rusty_grid_core::capabilities::MobileCapabilities;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::{Task, TaskRequirements, TaskResult, TaskSpec};
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::MobileTelemetryHandle;

/// A battery reading that never makes the worker refuse work.
fn plugged_in() -> MobileTelemetryHandle {
    MobileTelemetryHandle::new(Some(MobileCapabilities {
        battery_pct: Some(100),
        is_charging: Some(true),
        ..MobileCapabilities::default()
    }))
}

/// Starts a worker for the master at `addr` in this process; stop it with the sender.
fn spawn_worker(addr: &str, sandbox: &Path) -> (watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let worker_config = WorkerConfig::new(addr.to_string())
        .with_name("blob-e2e-worker")
        .with_cores(2)
        .with_ram_mb(4096)
        .with_sandbox_base_dir(sandbox)
        .with_mobile_telemetry(plugged_in())
        .with_redirection_portal(false);
    let mut client = WorkerClient::new(worker_config);
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = client.run(stop_rx).await;
    });
    (stop_tx, handle)
}

/// A master with a blob store and one worker, in this process.
struct Cluster {
    master: MasterHandle,
    addr: String,
    worker: Option<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    async fn start(with_blob_store: bool) -> Self {
        let store_dir = tempfile::tempdir().unwrap();
        let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
        if with_blob_store {
            config = config.with_blob_store(store_dir.path(), 256 * 1024 * 1024);
        }
        Self::start_with_config(config, store_dir).await
    }

    async fn start_with_config(config: ServerConfig, store_dir: TempDir) -> Self {
        let master = MasterServer::spawn(config).await.expect("master starts");
        let addr = master.server_addr().to_string();

        let sandbox_dir = tempfile::tempdir().unwrap();
        let worker = spawn_worker(&addr, sandbox_dir.path());
        let cluster = Self {
            master,
            addr,
            worker: Some(worker),
            _dirs: vec![store_dir, sandbox_dir],
        };
        cluster.wait_for_worker().await;
        cluster
    }

    async fn wait_for_worker(&self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            let workers = self.master.list_workers().await.unwrap_or_default();
            if workers
                .iter()
                .any(|w| w.status != WorkerStatus::Disconnected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("the worker did not register");
    }

    /// Submits `task` and waits for its result.
    async fn run(&self, task: Task) -> Result<TaskResult, String> {
        let mut transport = rusty_grid_cli::connect_master(&self.addr, None)
            .await
            .expect("connect");
        transport
            .send_msg(&ClientMessage::SubmitTask { task, wait: true })
            .await
            .expect("send");
        let reply = tokio::time::timeout(
            Duration::from_secs(60),
            transport.recv_msg::<ClientResponse>(),
        )
        .await
        .expect("task finishes in time")
        .expect("read reply");
        match reply {
            Some(ClientResponse::TaskCompleted { result, .. }) => Ok(result),
            Some(ClientResponse::Error { message }) => Err(message),
            other => panic!("unexpected reply {other:?}"),
        }
    }

    #[cfg(unix)]
    fn store_stats(&self) -> rusty_grid_master::BlobStoreStats {
        self.master.blob_store().expect("blob store").stats()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if let Some((stop, handle)) = self.worker.take() {
            let _ = stop.send(true);
            handle.abort();
        }
        let _ = self.master.shutdown();
    }
}

/// Deterministic bytes that do not compress or repeat.
#[cfg(unix)]
fn noise(len: usize, mut state: u64) -> Vec<u8> {
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// The input project: nested text files, an executable script and a file larger than one
/// transfer chunk (4 MiB).
#[cfg(unix)]
fn write_project(root: &Path) -> HashMap<&'static str, Vec<u8>> {
    let files: HashMap<&'static str, Vec<u8>> = HashMap::from([
        ("hello.txt", b"hello from the client\n".to_vec()),
        ("nested/deeper/data.csv", b"id,value\n1,42\n2,58\n".to_vec()),
        ("big.bin", noise(5 * 1024 * 1024 + 123, 0x5eed)),
        ("tool.sh", b"#!/bin/sh\necho tool-ran\n".to_vec()),
    ]);
    for (rel, data) in &files {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, data).unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(root.join("tool.sh"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
    }
    files
}

/// Reads the project, writes `out/` from it.
#[cfg(unix)]
const PROCESS_SCRIPT: &str = "set -e; mkdir -p out; \
     cat project/hello.txt project/nested/deeper/data.csv > out/combined.txt; \
     cp project/big.bin out/big-copy.bin; \
     ./project/tool.sh > out/tool-output.txt";

#[cfg(unix)]
fn processing_task(inputs: Vec<rusty_grid_core::blob::TaskInput>) -> Task {
    Task::new(
        TaskSpec::command("sh", vec!["-c".into(), PROCESS_SCRIPT.into()]),
        TaskRequirements::generic(1, 60),
    )
    .with_inputs(inputs)
    .with_output_glob("out")
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_input_directory_round_trip_and_second_submit_sends_nothing() {
    let cluster = Cluster::start(true).await;
    let local = tempfile::tempdir().unwrap();
    let project = local.path().join("project");
    let files = write_project(&project);

    // 1. First submission: every blob is uploaded once, the worker fetches them.
    let prepared = scan_inputs(&[format!("{}:project", project.display())]).unwrap();
    assert_eq!(prepared.files, 4);
    let connection = BlobConnection::connect(&cluster.addr, None).await.unwrap();
    let report = upload_inputs(&connection, &prepared).await.unwrap();
    assert_eq!(
        report.uploaded, report.blobs,
        "nothing was on the master yet"
    );
    assert_eq!(report.blobs, 5, "four files and the directory manifest");
    assert!(report.bytes > 5 * 1024 * 1024);

    let result = cluster
        .run(processing_task(prepared.inputs.clone()))
        .await
        .unwrap();
    assert!(
        result.is_success(),
        "{:?} {}",
        result.error,
        result.stderr_str()
    );
    let paths: Vec<&str> = result.outputs.iter().map(|o| o.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "out/big-copy.bin",
            "out/combined.txt",
            "out/tool-output.txt"
        ]
    );

    let download_dir = local.path().join("downloaded");
    let saved = download_outputs(&connection, &result.outputs, &download_dir)
        .await
        .unwrap();
    assert_eq!(saved.len(), 3);
    let combined = [
        files["hello.txt"].clone(),
        files["nested/deeper/data.csv"].clone(),
    ]
    .concat();
    assert_eq!(
        std::fs::read(download_dir.join("out/combined.txt")).unwrap(),
        combined
    );
    assert_eq!(
        std::fs::read(download_dir.join("out/big-copy.bin")).unwrap(),
        files["big.bin"],
        "a multi-chunk file arrives byte-identical"
    );
    assert_eq!(
        std::fs::read(download_dir.join("out/tool-output.txt")).unwrap(),
        b"tool-ran\n",
        "the executable bit survives the trip"
    );

    // 2. Same inputs again: the client uploads nothing, the worker fetches nothing, and the
    //    outputs (unchanged content) are not uploaded again either.
    let before = cluster.store_stats();
    let again = scan_inputs(&[format!("{}:project", project.display())]).unwrap();
    assert_eq!(
        again.inputs, prepared.inputs,
        "unchanged files keep their ids"
    );
    let report = upload_inputs(&connection, &again).await.unwrap();
    assert_eq!(report.uploaded, 0, "no blob is sent twice");
    assert_eq!(report.bytes, 0);

    let second = cluster.run(processing_task(again.inputs)).await.unwrap();
    assert!(second.is_success(), "{:?}", second.error);
    let after = cluster.store_stats();
    assert_eq!(
        after.chunks_served, before.chunks_served,
        "the worker's cache served every input"
    );
    assert_eq!(
        after.blobs_received, before.blobs_received,
        "identical outputs are not uploaded again"
    );
    assert_eq!(
        second.outputs.iter().map(|o| o.id).collect::<Vec<_>>(),
        result.outputs.iter().map(|o| o.id).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rust_compilation_returns_the_library_it_built() {
    if std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipping: rustc is not on PATH");
        return;
    }
    let cluster = Cluster::start(true).await;
    let mut sources = HashMap::new();
    sources.insert(
        "src/lib.rs".to_string(),
        "pub fn answer() -> u32 { 42 }".to_string(),
    );
    let task = Task::new(
        TaskSpec::rust_compilation("blob_e2e_lib", sources, vec![]),
        TaskRequirements::generic(1, 120),
    );
    let result = cluster.run(task).await.unwrap();
    assert!(
        result.is_success(),
        "{:?} {}",
        result.error,
        result.stderr_str()
    );
    let rlib = result
        .outputs
        .iter()
        .find(|o| o.path == "target/libblob_e2e_lib.rlib")
        .unwrap_or_else(|| panic!("no rlib among {:?}", result.outputs));
    assert!(rlib.size > 0);

    let local = tempfile::tempdir().unwrap();
    let connection = BlobConnection::connect(&cluster.addr, None).await.unwrap();
    download_outputs(&connection, std::slice::from_ref(rlib), local.path())
        .await
        .unwrap();
    let bytes = std::fs::read(local.path().join("target/libblob_e2e_lib.rlib")).unwrap();
    assert_eq!(bytes.len() as u64, rlib.size);
    assert!(bytes.starts_with(b"!<arch>\n"), "an rlib is an ar archive");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_master_without_a_blob_store_refuses_tasks_with_files() {
    let cluster = Cluster::start(false).await;
    let task = Task::new(
        TaskSpec::command("echo", vec!["x".into()]),
        TaskRequirements::generic(1, 10),
    )
    .with_output_glob("out.txt");
    let err = cluster.run(task).await.unwrap_err();
    assert!(err.contains("no blob store"), "{err}");

    // Blob requests get a clear refusal instead of a dropped connection.
    let connection = BlobConnection::connect(&cluster.addr, None).await.unwrap();
    let prepared = {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.txt"), b"x").unwrap();
        scan_inputs(&[format!("{}:f.txt", dir.path().join("f.txt").display())]).unwrap()
    };
    let err = upload_inputs(&connection, &prepared).await.unwrap_err();
    assert!(err.contains("no blob store"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_inputs_that_were_never_uploaded_are_refused_at_submission() {
    let cluster = Cluster::start(true).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("data.txt"), b"never uploaded").unwrap();
    let prepared = scan_inputs(&[format!("{}", dir.path().join("data.txt").display())]).unwrap();
    let task = Task::new(
        TaskSpec::command("cat", vec!["data.txt".into()]),
        TaskRequirements::generic(1, 10),
    )
    .with_inputs(prepared.inputs);
    let err = cluster.run(task).await.unwrap_err();
    assert!(err.contains("not on the master"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_output_patterns_never_follow_symlinked_parent_directories_or_steal_external_files() {
    let cluster = Cluster::start(true).await;
    let outside = tempfile::tempdir().unwrap();
    let sub = outside.path().join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(outside.path().join("important.txt"), b"precious").unwrap();
    std::fs::write(sub.join("secret.txt"), b"sub-secret").unwrap();

    #[cfg(unix)]
    let can_symlink = {
        let probe = outside.path().join("probe_link");
        let ok = std::os::unix::fs::symlink(&sub, &probe).is_ok();
        let _ = std::fs::remove_file(&probe);
        ok
    };
    #[cfg(windows)]
    let can_symlink = {
        let probe = outside.path().join("probe_link");
        let ok = std::os::windows::fs::symlink_dir(&sub, &probe).is_ok();
        let _ = std::fs::remove_dir(&probe);
        ok
    };
    if !can_symlink {
        eprintln!("skipping: host refused to create a directory symlink");
        return;
    }

    #[cfg(unix)]
    let make_link_task = |outputs: Vec<&str>| {
        let script = format!(
            "ln -s '{}' link && printf 'sandbox-ok' > keep.txt",
            outside.path().display()
        );
        let mut task = Task::new(
            TaskSpec::command("sh", vec!["-c".into(), script]),
            TaskRequirements::generic(1, 30),
        );
        for pat in outputs {
            task = task.with_output_glob(pat);
        }
        task
    };
    #[cfg(windows)]
    let make_link_task = |outputs: Vec<&str>| {
        let script = format!(
            "mklink /D link \"{}\" && echo sandbox-ok>keep.txt",
            outside.path().display()
        );
        let mut task = Task::new(
            TaskSpec::command("cmd", vec!["/C".into(), script]),
            TaskRequirements::generic(1, 30),
        );
        for pat in outputs {
            task = task.with_output_glob(pat);
        }
        task
    };

    // 1. Literal output through a symlinked directory fails and names the pattern.
    let res_literal = cluster
        .run(make_link_task(vec!["link/important.txt"]))
        .await
        .unwrap();
    assert!(
        !res_literal.is_success(),
        "literal output through symlinked directory must fail"
    );
    let err_msg = res_literal.error.as_deref().unwrap_or("");
    assert!(
        err_msg.contains("link/important.txt"),
        "expected error naming pattern, got: {err_msg}"
    );
    assert_eq!(
        std::fs::read(outside.path().join("important.txt")).unwrap(),
        b"precious",
        "external file must not be moved or modified"
    );

    // 2. Wildcard through symlinked prefix selects nothing, while '**/*.txt' returns sandbox files only.
    let res_glob = cluster
        .run(make_link_task(vec!["link/sub/*.txt", "**/*.txt"]))
        .await
        .unwrap();
    assert!(res_glob.is_success(), "{:?}", res_glob.error);
    let paths: Vec<&str> = res_glob.outputs.iter().map(|o| o.path.as_str()).collect();
    assert_eq!(paths, ["keep.txt"]);
    assert_eq!(
        std::fs::read(sub.join("secret.txt")).unwrap(),
        b"sub-secret",
        "external subdirectory file must not be moved or modified"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_output_blobs_stay_pinned_for_delayed_client_download() {
    let store_dir = tempfile::tempdir().unwrap();
    // 3 MiB blob store bound: two 2 MiB outputs cannot both fit without eviction unless pinned.
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_blob_store(store_dir.path(), 3 * 1024 * 1024);
    let cluster = Cluster::start_with_config(config, store_dir).await;

    let write_2mib_task = |name: &str, byte: u8| -> (Task, Vec<u8>) {
        let data = vec![byte; 2 * 1024 * 1024];
        let shared_src = cluster._dirs[0].path().join(format!("src_{name}"));
        std::fs::write(&shared_src, &data).unwrap();
        #[cfg(unix)]
        let spec = TaskSpec::command(
            "cp",
            vec![shared_src.to_string_lossy().into_owned(), name.to_string()],
        );
        #[cfg(windows)]
        let spec = TaskSpec::command(
            "cmd",
            vec![
                "/C".into(),
                format!("copy /Y \"{}\" \"{}\"", shared_src.display(), name),
            ],
        );
        (
            Task::new(spec, TaskRequirements::generic(1, 30)).with_output_glob(name),
            data,
        )
    };

    let (task_a, expected_a) = write_2mib_task("a.bin", 0x41);
    let result_a = cluster.run(task_a).await.unwrap();
    assert!(result_a.is_success(), "{:?}", result_a.error);
    assert_eq!(result_a.outputs.len(), 1);

    // Before client A downloads `a.bin`, task B runs and stores a 2 MiB output `b.bin`.
    let (task_b, _expected_b) = write_2mib_task("b.bin", 0x42);
    let result_b = cluster.run(task_b).await.unwrap();
    assert!(result_b.is_success(), "{:?}", result_b.error);

    // Client A now resumes and downloads `a.bin`.
    let out_dir = tempfile::tempdir().unwrap();
    let connection = BlobConnection::connect(&cluster.addr, None).await.unwrap();
    download_outputs(&connection, &result_a.outputs, out_dir.path())
        .await
        .expect("task A output must remain pinned until client A downloads it");
    assert_eq!(
        std::fs::read(out_dir.path().join("a.bin")).unwrap(),
        expected_a
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_output_blobs_can_be_evicted_after_output_pin_retention_expires() {
    let store_dir = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap());
    config.blob_store = Some(
        rusty_grid_master::BlobStoreConfig::new(store_dir.path(), 3 * 1024 * 1024)
            .with_output_pin_retention(Duration::from_millis(100)),
    );
    let cluster = Cluster::start_with_config(config, store_dir).await;

    let write_2mib_task = |name: &str, byte: u8| -> Task {
        let data = vec![byte; 2 * 1024 * 1024];
        let shared_src = cluster._dirs[0].path().join(format!("src_{name}"));
        std::fs::write(&shared_src, &data).unwrap();
        #[cfg(unix)]
        let spec = TaskSpec::command(
            "cp",
            vec![shared_src.to_string_lossy().into_owned(), name.to_string()],
        );
        #[cfg(windows)]
        let spec = TaskSpec::command(
            "cmd",
            vec![
                "/C".into(),
                format!("copy /Y \"{}\" \"{}\"", shared_src.display(), name),
            ],
        );
        Task::new(spec, TaskRequirements::generic(1, 30)).with_output_glob(name)
    };

    let result_a = cluster
        .run(write_2mib_task("a.bin", 0x51))
        .await
        .unwrap();
    assert!(result_a.is_success(), "{:?}", result_a.error);
    let a_id = result_a.outputs[0].id;
    let store = cluster.master.blob_store().expect("blob store");
    assert!(store.contains(&a_id));
    assert_eq!(store.stats().pinned_blobs, 1);

    // Wait for `output_pin_retention` (100 ms) and the sweeper to release the output pin.
    let deadline = Instant::now() + Duration::from_secs(3);
    while store.stats().pinned_blobs > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        store.stats().pinned_blobs,
        0,
        "sweeper must release expired output pins"
    );

    // Now storing another 2 MiB output evicts `a.bin` to fit the 3 MiB bound.
    let result_b = cluster
        .run(write_2mib_task("b.bin", 0x52))
        .await
        .unwrap();
    assert!(result_b.is_success(), "{:?}", result_b.error);
    assert!(
        !store.contains(&a_id),
        "expired output blob must be evictable once its pin ends"
    );
    assert!(store.stats().evictions >= 1);
}

// ---------------------------------------------------------------------------------------------
// The real binary: `rusty-grid master` and `rusty-grid submit` as separate processes (the worker
// runs in this process, see the module docs)
// ---------------------------------------------------------------------------------------------

#[cfg(unix)]
mod cli {
    use super::*;
    use std::process::{Child, Command, Stdio};

    const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

    fn cli(home: &Path) -> Command {
        let mut cmd = Command::new(BIN);
        cmd.env("HOME", home)
            .env("USERPROFILE", home)
            .env_remove("OXIDESWARM_HOME")
            .current_dir(home)
            .env_remove("OXIDESWARM_SECRET")
            .env_remove("OXIDESWARM_SECRET_FILE")
            .env_remove("RUSTY_GRID_CONFIG")
            .env_remove("RUSTY_GRID_MASTER")
            .env_remove("RUSTY_GRID_MASTER_ADDR")
            .env_remove("RUSTY_GRID_BLOB_DIR")
            .env_remove("RUSTY_GRID_INSECURE")
            .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
            .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
            .env("NO_COLOR", "1")
            .stdin(Stdio::null());
        cmd
    }

    struct Service(Child);

    impl Drop for Service {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn spawn(mut cmd: Command, log: &Path) -> Service {
        let out = std::fs::File::create(log).unwrap();
        let err = out.try_clone().unwrap();
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        Service(cmd.spawn().expect("spawn rusty-grid"))
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

    fn submit(home: &Path, master: &str, project: &Path, out_dir: &Path) -> serde_json::Value {
        let output = cli(home)
            .args([
                "submit",
                "--master",
                master,
                "--json",
                "--timeout-secs",
                "60",
            ])
            .arg("--input")
            .arg(format!("{}:project", project.display()))
            .args(["--output", "out/*.txt", "--output", "out/big-copy.bin"])
            .arg("--output-dir")
            .arg(out_dir)
            .args(["--command", "sh", "--", "-c", PROCESS_SCRIPT])
            .output()
            .expect("run submit");
        assert!(
            output.status.success(),
            "submit failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).expect("submit --json prints JSON")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cli_submit_input_and_output_round_trip_with_dedup() {
        let home = tempfile::tempdir().unwrap();
        let home = home.path();
        let port_file = home.join("master.port");
        let blob_dir = home.join("blobs");
        let master_log = home.join("master.log");
        let _master = spawn(
            {
                let mut cmd = cli(home);
                cmd.args(["master", "--listen", "127.0.0.1:0", "--port-file"])
                    .arg(&port_file)
                    .arg("--blob-dir")
                    .arg(&blob_dir);
                cmd
            },
            &master_log,
        );
        let mut port = None;
        assert!(
            wait_for(Duration::from_secs(20), || {
                port = std::fs::read_to_string(&port_file)
                    .ok()
                    .and_then(|p| p.trim().parse::<u16>().ok());
                port.is_some()
            }),
            "master did not publish its port: {}",
            std::fs::read_to_string(&master_log).unwrap_or_default()
        );
        let master = format!("127.0.0.1:{}", port.unwrap());
        let (stop_worker, worker) = spawn_worker(&master, &home.join("sandboxes"));
        assert!(
            wait_for(Duration::from_secs(20), || {
                cli(home)
                    .args(["workers", "--master", &master, "--json"])
                    .output()
                    .ok()
                    .and_then(|o| serde_json::from_slice::<serde_json::Value>(&o.stdout).ok())
                    .and_then(|v| v.as_array().map(|a| !a.is_empty()))
                    .unwrap_or(false)
            }),
            "worker did not register"
        );

        let project = home.join("project-src");
        let files = write_project(&project);

        let out1 = home.join("out1");
        let first = submit(home, &master, &project, &out1);
        assert_eq!(first["exit_code"], 0, "{first}");
        assert_eq!(first["input_upload"]["uploaded"], 5, "{first}");
        assert_eq!(
            std::fs::read(out1.join("out/big-copy.bin")).unwrap(),
            files["big.bin"]
        );
        assert_eq!(
            std::fs::read(out1.join("out/tool-output.txt")).unwrap(),
            b"tool-ran\n"
        );
        assert_eq!(first["saved_outputs"].as_array().map(Vec::len), Some(3));
        assert!(
            blob_dir.join("objects").is_dir(),
            "the store lives where --blob-dir says"
        );
        assert!(
            !home.join(".oxideswarm").join("blobs").exists(),
            "--blob-dir replaces the default location"
        );

        let out2 = home.join("out2");
        let second = submit(home, &master, &project, &out2);
        assert_eq!(second["exit_code"], 0, "{second}");
        assert_eq!(
            second["input_upload"]["uploaded"], 0,
            "unchanged inputs are not sent again: {second}"
        );
        assert_eq!(second["input_upload"]["bytes"], 0);
        assert_eq!(
            std::fs::read(out2.join("out/combined.txt")).unwrap(),
            std::fs::read(out1.join("out/combined.txt")).unwrap()
        );
        let _ = stop_worker.send(true);
        worker.abort();
    }
}
