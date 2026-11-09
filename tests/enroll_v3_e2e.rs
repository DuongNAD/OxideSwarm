//! End-to-end integration test for OXS-AUTH v3 per-machine Noise `IK` authentication and
//! `members.json` coexistence (`ENROLL-1a`, design record
//! `docs/design/0051-machine-identity-and-invites.md`).
//!
//! Verifies:
//! 1. Loopback master with temp `HOME` and `members.json` holding worker A's public key prints its
//!    v3 public key and fingerprint at start.
//! 2. Worker A (OXS-AUTH v3 via `--master-key` + `node.key`) and Worker B (OXS-AUTH v2 shared
//!    secret) both register with the master and each executes a pinned task.
//! 3. Worker C (OXS-AUTH v3 with a `node.key` not in `members.json`) is refused and never
//!    registers.
//! 4. With `master --auth members`, Worker B (OXS-AUTH v2 shared secret) is refused with the
//!    distinct reason `"this master admits enrolled machines only; join with an invite"`, while
//!    Worker A (OXS-AUTH v3 enrolled member) is admitted.
//! 5. No private key or cluster secret ever appears in any process output or log.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_core::noise_auth::NodeKeypair;
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("OXIDESWARM_HOME")
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

#[derive(Default)]
struct Seen(Vec<String>);

impl Seen {
    fn run(&mut self, mut cmd: Command) -> Output {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let out = cmd.output().expect("run rusty-grid");
        self.0.push(text(&out.stdout));
        self.0.push(text(&out.stderr));
        out
    }
}

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
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

#[cfg(unix)]
fn assert_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "{} must be mode 600", path.display());
}

#[cfg(windows)]
fn assert_private(path: &Path) {
    use rusty_grid_core::private_file::{current_user_sid, open_grants, read_dacl};
    let (aces, protected) = read_dacl(path).expect("read DACL");
    assert!(protected, "the DACL must not inherit entries");
    let user = current_user_sid().expect("user sid");
    assert!(
        open_grants(&aces, &user).is_empty(),
        "only the user, SYSTEM and Administrators may have access: {aces:?}"
    );
}

#[test]
fn test_enroll_v3_and_v2_coexistence_and_members_only_mode() {
    let master_home = TempDir::new().unwrap();
    let worker_a_home = TempDir::new().unwrap();
    let worker_b_home = TempDir::new().unwrap();
    let worker_c_home = TempDir::new().unwrap();
    let mut seen = Seen::default();

    // 1. Generate node keypairs for Worker A (enrolled) and Worker C (un-enrolled).
    let worker_a_kp = NodeKeypair::generate().unwrap();
    let worker_a_key_path = worker_a_home.path().join(".oxideswarm").join("node.key");
    worker_a_kp.save_file(&worker_a_key_path, false).unwrap();
    assert_private(&worker_a_key_path);

    let worker_c_kp = NodeKeypair::generate().unwrap();
    let worker_c_key_path = worker_c_home.path().join(".oxideswarm").join("node.key");
    worker_c_kp.save_file(&worker_c_key_path, false).unwrap();
    assert_private(&worker_c_key_path);

    // 2. Prepare Master's home: generate cluster.secret (for Worker B) and write members.json
    //    holding Worker A's public key. Let Master generate master_noise.key on startup and verify
    //    it is saved as a private file and printed to stdout.
    let out = seen.run(with(master_home.path(), &["secret", "generate"]));
    assert!(out.status.success(), "{}", text(&out.stderr));
    let master_secret_file = master_home
        .path()
        .join(".oxideswarm")
        .join("cluster.secret");
    let secret_hex = fs::read_to_string(&master_secret_file)
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(secret_hex.len(), 64);

    // Copy the shared secret to Worker B's home.
    let worker_b_secret_dir = worker_b_home.path().join(".oxideswarm");
    fs::create_dir_all(&worker_b_secret_dir).unwrap();
    let worker_b_secret_file = worker_b_secret_dir.join("cluster.secret");
    rusty_grid_core::cluster_auth::ClusterSecret::from_hex(&secret_hex)
        .unwrap()
        .save_file(&worker_b_secret_file, true)
        .unwrap();
    assert_private(&worker_b_secret_file);

    // Write members.json in Master's ~/.oxideswarm/ with Worker A enrolled as worker + client.
    let members_json = serde_json::json!({
        "version": 1,
        "members": [
            {
                "id": worker_a_kp.member_id(),
                "name": "worker-a",
                "public_key": worker_a_kp.public_key().to_hex(),
                "roles": ["worker", "client"]
            }
        ]
    });
    let members_path = master_home.path().join(".oxideswarm").join("members.json");
    fs::write(
        &members_path,
        serde_json::to_string_pretty(&members_json).unwrap(),
    )
    .unwrap();

    // 3. Start Master (default --auth both) on 127.0.0.1:0.
    let port_file = master_home.path().join("master.port");
    let mut master_cmd = cli(master_home.path());
    master_cmd
        .args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&port_file);
    let master = Service::spawn(master_cmd, master_home.path().join("master.log"));

    let mut port = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port.is_some() && master.log().contains("Master v3 public key:")
        }),
        "master did not start and print its v3 public key; log:\n{}",
        master.log()
    );
    let addr = format!("127.0.0.1:{}", port.unwrap());

    let master_noise_path = master_home
        .path()
        .join(".oxideswarm")
        .join("master_noise.key");
    assert!(
        master_noise_path.is_file(),
        "master must create ~/.oxideswarm/master_noise.key"
    );
    assert_private(&master_noise_path);
    let master_kp = NodeKeypair::load_file(&master_noise_path).unwrap();
    let master_pub_hex = master_kp.public_key().to_hex();
    let master_fp = master_kp.fingerprint();
    assert!(
        master.log().contains(&format!(
            "Master v3 public key: {master_pub_hex} (fingerprint: {master_fp})"
        )),
        "master startup output must contain v3 public key and fingerprint:\n{}",
        master.log()
    );

    // 4. Start Worker A (v3: --master-key, no cluster.secret) and Worker B (v2: cluster.secret).
    let mut worker_a_cmd = cli(worker_a_home.path());
    worker_a_cmd
        .args([
            "worker",
            "--name",
            "worker-a",
            "--master",
            &addr,
            "--master-key",
            &master_pub_hex,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(worker_a_home.path().join("sandbox"));
    let worker_a = Service::spawn(worker_a_cmd, worker_a_home.path().join("worker-a.log"));

    let mut worker_b_cmd = cli(worker_b_home.path());
    worker_b_cmd
        .args([
            "worker",
            "--name",
            "worker-b",
            "--master",
            &addr,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(worker_b_home.path().join("sandbox"));
    let worker_b = Service::spawn(worker_b_cmd, worker_b_home.path().join("worker-b.log"));

    // Wait for both Worker A and Worker B to register.
    assert!(
        wait_for(Duration::from_secs(20), || {
            let out = seen.run(with(
                worker_a_home.path(),
                &[
                    "status",
                    "--workers",
                    "--json",
                    "--master",
                    &addr,
                    "--master-key",
                    &master_pub_hex,
                ],
            ));
            if !out.status.success() {
                return false;
            }
            let stdout = text(&out.stdout);
            stdout.contains("\"worker-a\"") && stdout.contains("\"worker-b\"")
        }),
        "worker-a and worker-b did not both register:\nmaster log:\n{}\nworker-a log:\n{}\nworker-b log:\n{}",
        master.log(),
        worker_a.log(),
        worker_b.log()
    );

    // 5. Each worker runs a task pinned to it.
    let sub_a = seen.run(with(
        worker_a_home.path(),
        &[
            "submit",
            "--master",
            &addr,
            "--master-key",
            &master_pub_hex,
            "--worker",
            "worker-a",
            "--wait",
            "--json",
            "--",
            "echo",
            "ran-on-worker-a",
        ],
    ));
    assert!(
        sub_a.status.success(),
        "submit to worker-a failed:\n{}\n{}",
        text(&sub_a.stdout),
        text(&sub_a.stderr)
    );
    assert!(
        text(&sub_a.stdout).contains("ran-on-worker-a"),
        "expected task output from worker-a, got: {}",
        text(&sub_a.stdout)
    );

    let sub_b = seen.run(with(
        worker_b_home.path(),
        &[
            "submit",
            "--master",
            &addr,
            "--worker",
            "worker-b",
            "--wait",
            "--json",
            "--",
            "echo",
            "ran-on-worker-b",
        ],
    ));
    assert!(
        sub_b.status.success(),
        "submit to worker-b failed:\n{}\n{}",
        text(&sub_b.stdout),
        text(&sub_b.stderr)
    );
    assert!(
        text(&sub_b.stdout).contains("ran-on-worker-b"),
        "expected task output from worker-b, got: {}",
        text(&sub_b.stdout)
    );

    // 6. Worker C (key not in members.json) is refused.
    let mut worker_c_cmd = cli(worker_c_home.path());
    worker_c_cmd
        .args([
            "worker",
            "--name",
            "worker-c",
            "--master",
            &addr,
            "--master-key",
            &master_pub_hex,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(worker_c_home.path().join("sandbox"));
    let worker_c = Service::spawn(worker_c_cmd, worker_c_home.path().join("worker-c.log"));

    assert!(
        wait_for(Duration::from_secs(15), || {
            let log = worker_c.log();
            log.contains("not an enrolled member of this cluster")
                || log.contains("Authentication failed")
        }),
        "worker-c was not refused; worker-c log:\n{}\nmaster log:\n{}",
        worker_c.log(),
        master.log()
    );

    let status_out = seen.run(with(
        worker_a_home.path(),
        &[
            "status",
            "--workers",
            "--json",
            "--master",
            &addr,
            "--master-key",
            &master_pub_hex,
        ],
    ));
    assert!(
        !text(&status_out.stdout).contains("worker-c"),
        "un-enrolled worker-c must never appear in registered workers: {}",
        text(&status_out.stdout)
    );

    drop(worker_c);
    drop(worker_b);
    drop(worker_a);
    drop(master);

    // 7. Start a master with `--auth members` and verify Worker B (v2 shared secret) is refused
    //    with the distinct reason "this master admits enrolled machines only; join with an invite",
    //    while Worker A (v3 enrolled member) is admitted.
    let port_file_2 = master_home.path().join("master2.port");
    let mut master2_cmd = cli(master_home.path());
    master2_cmd
        .args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--auth",
            "members",
            "--port-file",
        ])
        .arg(&port_file_2);
    let master2 = Service::spawn(master2_cmd, master_home.path().join("master2.log"));

    let mut port2 = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port2 = fs::read_to_string(&port_file_2)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port2.is_some()
        }),
        "master2 did not publish port; log:\n{}",
        master2.log()
    );
    let addr2 = format!("127.0.0.1:{}", port2.unwrap());

    let mut worker_b2_cmd = cli(worker_b_home.path());
    worker_b2_cmd
        .args([
            "worker",
            "--name",
            "worker-b",
            "--master",
            &addr2,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(worker_b_home.path().join("sandbox2"));
    let worker_b2 = Service::spawn(worker_b2_cmd, worker_b_home.path().join("worker-b2.log"));

    assert!(
        wait_for(Duration::from_secs(15), || {
            worker_b2
                .log()
                .contains("this master admits enrolled machines only; join with an invite")
        }),
        "worker-b was not refused with the members-only reason; worker-b2 log:\n{}\nmaster2 log:\n{}",
        worker_b2.log(),
        master2.log()
    );

    // Also verify a v2 CLI client against `--auth members` gets exit code 6 and the distinct reason.
    let v2_status = seen.run(with(worker_b_home.path(), &["status", "--master", &addr2]));
    assert_eq!(v2_status.status.code(), Some(6));
    assert!(
        text(&v2_status.stderr)
            .contains("this master admits enrolled machines only; join with an invite"),
        "expected members-only rejection in stderr, got: {}",
        text(&v2_status.stderr)
    );

    // Meanwhile, enrolled v3 client succeeds against `--auth members`.
    let v3_status = seen.run(with(
        worker_a_home.path(),
        &[
            "status",
            "--master",
            &addr2,
            "--master-key",
            &master_pub_hex,
        ],
    ));
    assert!(
        v3_status.status.success(),
        "enrolled v3 client failed against --auth members:\n{}",
        text(&v3_status.stderr)
    );

    // 8. Ensure no secret or private key material ever appeared in any output or log.
    let master_priv_hex = fs::read_to_string(&master_noise_path)
        .unwrap()
        .trim()
        .to_string();
    let worker_a_priv_hex = fs::read_to_string(&worker_a_key_path)
        .unwrap()
        .trim()
        .to_string();
    let worker_c_priv_hex = fs::read_to_string(&worker_c_key_path)
        .unwrap()
        .trim()
        .to_string();

    for output in seen.0.iter().chain([&master2.log(), &worker_b2.log()]) {
        for sensitive in [
            &secret_hex,
            &master_priv_hex,
            &worker_a_priv_hex,
            &worker_c_priv_hex,
        ] {
            assert!(
                !output.contains(sensitive),
                "sensitive key or secret leaked into output:\n{output}"
            );
        }
    }
}
