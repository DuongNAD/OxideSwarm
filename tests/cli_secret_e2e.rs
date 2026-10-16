//! End-to-end tests of the CLI's cluster secret handling, driving the real `rusty-grid` binary
//! (master, worker, submit, workers, status, secret) as separate processes.
//!
//! Every spawned process gets a private `HOME` / `USERPROFILE` and working directory, so nothing
//! here reads or writes the developer's real `~/.oxideswarm` (secret, config, master key), and no
//! `OXIDESWARM_*` / `RUSTY_GRID_*` variable from the test environment leaks in.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

/// Makes port-file and log names unique within one test (e.g. across a master restart).
static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A clean command environment rooted in `home`.
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
        .env_remove("RUSTY_GRID_LISTEN")
        .env_remove("RUSTY_GRID_MASTER_LISTEN")
        .env_remove("RUSTY_GRID_INSECURE")
        .env_remove("RUSTY_GRID_P2P")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

fn run(mut cmd: Command) -> Output {
    cmd.output().expect("run rusty-grid")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A long-running child (master / worker) whose output goes to a log file. Killed on drop.
struct Service {
    child: Child,
    log: PathBuf,
}

impl Service {
    fn spawn(mut cmd: Command, log: PathBuf) -> Self {
        let out = fs::File::create(&log).expect("create log");
        let err = out.try_clone().expect("clone log handle");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        let child = cmd.spawn().expect("spawn rusty-grid service");
        Self { child, log }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Polls the log until it contains `needle`.
    #[allow(dead_code)]
    fn wait_for_log(&self, needle: &str, timeout: Duration) -> bool {
        wait_for(timeout, || self.log().contains(needle))
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

/// Everything a test needs about one running master.
struct Master {
    service: Service,
    addr: String,
}

/// Starts `rusty-grid master --listen <listen> --port-file ...` with extra `args` in `home`.
fn start_master(home: &Path, listen: &str, args: &[&str]) -> Master {
    let id = SEQ.fetch_add(1, Ordering::SeqCst);
    let port_file = home.join(format!("master-{id}.port"));
    let mut cmd = cli(home);
    cmd.args(["master", "--listen", listen, "--port-file"])
        .arg(&port_file)
        .args(args);
    let service = Service::spawn(cmd, home.join(format!("master-{id}.log")));
    let mut port = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port.is_some()
        }),
        "master did not publish its port; log:\n{}",
        service.log()
    );
    Master {
        service,
        addr: format!("127.0.0.1:{}", port.unwrap()),
    }
}

fn start_worker(home: &Path, master: &str, name: &str, args: &[&str]) -> Service {
    let sandbox = home.join(format!("sandbox-{name}"));
    let mut cmd = cli(home);
    cmd.args(["worker", "--master", master, "--name", name, "--cores", "1"])
        .arg("--sandbox-base-dir")
        .arg(&sandbox)
        .args(args);
    Service::spawn(cmd, home.join(format!("worker-{name}.log")))
}

/// Number of workers `rusty-grid workers` reports, or `None` if the command failed.
fn worker_count(home: &Path, master: &str, args: &[&str]) -> Option<usize> {
    let mut cmd = cli(home);
    cmd.args(["workers", "--master", master, "--json"])
        .args(args);
    let out = run(cmd);
    if !out.status.success() {
        return None;
    }
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    Some(json.as_array()?.len())
}

fn generate_secret(home: &Path, path: &Path) {
    let mut cmd = cli(home);
    cmd.args(["secret", "generate", "--out"]).arg(path);
    let out = run(cmd);
    assert!(
        out.status.success(),
        "secret generate failed: {}",
        text(&out.stderr)
    );
}

fn tempdir() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

// ---------------------------------------------------------------------------------------------
// A shared secret: master + worker + submit/workers/status all work
// ---------------------------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn test_master_worker_and_submit_sharing_a_secret_succeed() {
    let home = tempdir();
    let secret = home.path().join("cluster.secret");
    generate_secret(home.path(), &secret);
    let secret_arg = secret.display().to_string();

    let master = start_master(home.path(), "127.0.0.1:0", &["--secret-file", &secret_arg]);
    assert!(
        master
            .service
            .log()
            .contains("Cluster authentication: enabled"),
        "master log:\n{}",
        master.service.log()
    );
    let worker = start_worker(
        home.path(),
        &master.addr,
        "e2e-worker",
        &["--secret-file", &secret_arg],
    );

    assert!(
        wait_for(Duration::from_secs(30), || {
            worker_count(home.path(), &master.addr, &["--secret-file", &secret_arg]) == Some(1)
        }),
        "worker never registered.\nworker log:\n{}\nmaster log:\n{}",
        worker.log(),
        master.service.log()
    );

    // submit --wait runs the command on the worker and prints its output.
    let mut cmd = cli(home.path());
    cmd.args([
        "submit",
        "--master",
        &master.addr,
        "--secret-file",
        &secret_arg,
    ])
    .args(["--wait", "--", "echo", "cli-e2e-ok"]);
    let out = run(cmd);
    assert!(out.status.success(), "submit failed: {}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("cli-e2e-ok"),
        "{}",
        text(&out.stdout)
    );

    // A command with no further arguments runs with no arguments (it used to receive
    // "Hello from RustyGrid" and `uname` rejects the extra operand).
    let mut cmd = cli(home.path());
    cmd.args([
        "submit",
        "--master",
        &master.addr,
        "--secret-file",
        &secret_arg,
    ])
    .args(["--wait", "--", "uname"]);
    let out = run(cmd);
    assert!(
        out.status.success(),
        "`submit -- uname` must run uname without arguments: stdout {} stderr {}",
        text(&out.stdout),
        text(&out.stderr)
    );

    // The secret may also come from the environment, like every client command allows.
    let mut cmd = cli(home.path());
    cmd.env("OXIDESWARM_SECRET_FILE", &secret)
        .args(["status", "--master", &master.addr, "--json"]);
    let out = run(cmd);
    assert!(out.status.success(), "status failed: {}", text(&out.stderr));
    let status: serde_json::Value = serde_json::from_slice(&out.stdout).expect("status json");
    assert_eq!(status["worker_count"], 1);
}

// ---------------------------------------------------------------------------------------------
// Mismatched or missing secrets: clear failure, nothing registers or runs
// ---------------------------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn test_mismatched_and_missing_secrets_fail_with_clear_messages() {
    let home = tempdir();
    let right = home.path().join("right.secret");
    let wrong = home.path().join("wrong.secret");
    generate_secret(home.path(), &right);
    generate_secret(home.path(), &wrong);
    let (right_arg, wrong_arg) = (right.display().to_string(), wrong.display().to_string());

    let master = start_master(home.path(), "127.0.0.1:0", &["--secret-file", &right_arg]);
    let marker = home.path().join("must-not-exist");

    // submit with the wrong secret: refused before any task is sent, exit code 6.
    let mut cmd = cli(home.path());
    cmd.args([
        "submit",
        "--master",
        &master.addr,
        "--secret-file",
        &wrong_arg,
    ])
    .args(["--", "touch"])
    .arg(&marker);
    let out = run(cmd);
    assert_eq!(out.status.code(), Some(6), "stderr: {}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("Authentication failed"), "{stderr}");
    assert!(stderr.contains("same cluster secret"), "{stderr}");

    // submit with no secret at all: told what to do about it.
    let mut cmd = cli(home.path());
    cmd.args(["submit", "--master", &master.addr, "--", "touch"])
        .arg(&marker);
    let out = run(cmd);
    assert_eq!(out.status.code(), Some(6), "stderr: {}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("Authentication required"), "{stderr}");
    assert!(stderr.contains("OXIDESWARM_SECRET"), "{stderr}");
    assert!(stderr.contains("--secret-file"), "{stderr}");

    // The read-only client commands behave the same way.
    for sub in [vec!["workers"], vec!["status"], vec!["status", "--workers"]] {
        let mut cmd = cli(home.path());
        cmd.args(sub.clone())
            .args(["--master", &master.addr, "--secret-file", &wrong_arg]);
        let out = run(cmd);
        assert_eq!(out.status.code(), Some(6), "{sub:?}: {}", text(&out.stderr));
    }
    let mut cmd = cli(home.path());
    cmd.args(["mapreduce", "--input", "a b a", "--master", &master.addr]);
    let out = run(cmd);
    assert_eq!(out.status.code(), Some(6), "{}", text(&out.stderr));

    // Workers with the wrong / no secret log one actionable error and never register.
    let wrong_worker = start_worker(
        home.path(),
        &master.addr,
        "wrong-secret",
        &["--secret-file", &wrong_arg],
    );
    let no_secret_worker = start_worker(home.path(), &master.addr, "no-secret", &[]);
    for worker in [&wrong_worker, &no_secret_worker] {
        assert!(
            worker.wait_for_log(
                "Cluster authentication with the Master failed",
                Duration::from_secs(20)
            ),
            "worker log:\n{}",
            worker.log()
        );
    }
    assert!(wrong_worker.log().contains("Authentication failed"));
    assert!(no_secret_worker.log().contains("Authentication required"));
    // 30 s of backoff: exactly one failure line each, not a hot loop.
    std::thread::sleep(Duration::from_millis(1500));
    for worker in [&wrong_worker, &no_secret_worker] {
        assert_eq!(
            worker
                .log()
                .matches("Cluster authentication with the Master failed")
                .count(),
            1,
            "worker retried too fast:\n{}",
            worker.log()
        );
    }

    // The legitimate view of the cluster: nobody registered, nothing ran.
    assert_eq!(
        worker_count(home.path(), &master.addr, &["--secret-file", &right_arg]),
        Some(0)
    );
    assert!(!marker.exists());
}

// ---------------------------------------------------------------------------------------------
// `secret generate` / `secret show`
// ---------------------------------------------------------------------------------------------

#[test]
fn test_secret_generate_and_show_subcommands() {
    let home = tempdir();

    // `show` without any secret fails and says how to create one.
    let out = run({
        let mut c = cli(home.path());
        c.args(["secret", "show"]);
        c
    });
    assert!(!out.status.success());
    assert!(
        text(&out.stderr).contains("secret generate"),
        "{}",
        text(&out.stderr)
    );

    // `generate` without --out writes the default file inside HOME (never the real one).
    let out = run({
        let mut c = cli(home.path());
        c.args(["secret", "generate"]);
        c
    });
    assert!(out.status.success(), "{}", text(&out.stderr));
    let default_file = home.path().join(".oxideswarm").join("cluster.secret");
    assert!(default_file.is_file(), "default secret file not created");
    let stdout = text(&out.stdout);
    assert!(
        stdout.contains(&default_file.display().to_string()),
        "{stdout}"
    );
    assert!(stdout.contains("--secret-file"), "{stdout}");
    let generated = fs::read_to_string(&default_file).unwrap();
    assert!(
        !stdout.contains(generated.trim()),
        "`generate` must not print the secret itself"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&default_file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    // A second generate refuses to overwrite; --force replaces.
    let refused = run({
        let mut c = cli(home.path());
        c.args(["secret", "generate"]);
        c
    });
    assert!(!refused.status.success());
    assert!(
        text(&refused.stderr).contains("--force"),
        "{}",
        text(&refused.stderr)
    );
    assert_eq!(fs::read_to_string(&default_file).unwrap(), generated);
    let forced = run({
        let mut c = cli(home.path());
        c.args(["secret", "generate", "--force"]);
        c
    });
    assert!(forced.status.success());
    assert_ne!(fs::read_to_string(&default_file).unwrap(), generated);

    // `show` prints just the hex secret; `--dashboard-token` a different derived value.
    let secret = text(
        &run({
            let mut c = cli(home.path());
            c.args(["secret", "show"]);
            c
        })
        .stdout,
    );
    let secret = secret.trim();
    assert_eq!(secret, fs::read_to_string(&default_file).unwrap().trim());
    let token = text(
        &run({
            let mut c = cli(home.path());
            c.args(["secret", "show", "--dashboard-token"]);
            c
        })
        .stdout,
    );
    let token = token.trim();
    assert_eq!(token.len(), 64);
    assert_ne!(token, secret);

    // Flags and environment outrank the default file.
    let other = home.path().join("other.secret");
    generate_secret(home.path(), &other);
    let other_hex = fs::read_to_string(&other).unwrap();
    let shown = text(
        &run({
            let mut c = cli(home.path());
            c.args(["secret", "show", "--secret-file"]).arg(&other);
            c
        })
        .stdout,
    );
    assert_eq!(shown.trim(), other_hex.trim());
    let shown = text(
        &run({
            let mut c = cli(home.path());
            c.env("OXIDESWARM_SECRET", other_hex.trim())
                .args(["secret", "show"]);
            c
        })
        .stdout,
    );
    assert_eq!(shown.trim(), other_hex.trim());
}

// ---------------------------------------------------------------------------------------------
// Master policy: loopback open, exposed generates, --insecure warns
// ---------------------------------------------------------------------------------------------

#[test]
fn test_loopback_master_runs_without_a_secret_and_creates_no_files() {
    let home = tempdir();
    let master = start_master(home.path(), "127.0.0.1:0", &[]);
    assert!(
        !home.path().join(".oxideswarm").exists(),
        "a loopback master must not create ~/.oxideswarm"
    );
    assert!(!master.service.log().contains("Cluster authentication"));
    assert_eq!(worker_count(home.path(), &master.addr, &[]), Some(0));
}

#[test]
fn test_exposed_master_generates_the_default_secret_and_enforces_it() {
    let home = tempdir();
    let master = start_master(home.path(), "0.0.0.0:0", &[]);
    let secret_file = home.path().join(".oxideswarm").join("cluster.secret");
    assert!(
        secret_file.is_file(),
        "default secret file must be generated"
    );

    // The path and the copy instruction are printed once.
    let log = master.service.log();
    assert!(log.contains(&secret_file.display().to_string()), "{log}");
    assert!(log.contains("To let other machines join"), "{log}");
    assert!(
        !log.contains(fs::read_to_string(&secret_file).unwrap().trim()),
        "the master must never print the secret"
    );

    // Same HOME => the default file is picked up automatically by clients.
    assert_eq!(worker_count(home.path(), &master.addr, &[]), Some(0));

    // A machine without the file is refused with the actionable message.
    let stranger = tempdir();
    let out = run({
        let mut c = cli(stranger.path());
        c.args(["workers", "--master", &master.addr]);
        c
    });
    assert_eq!(out.status.code(), Some(6), "{}", text(&out.stderr));
    assert!(text(&out.stderr).contains("Authentication required"));

    // Copying the file over (the documented one-liner) is enough.
    let copied = stranger.path().join(".oxideswarm");
    fs::create_dir_all(&copied).unwrap();
    fs::copy(&secret_file, copied.join("cluster.secret")).unwrap();
    assert_eq!(worker_count(stranger.path(), &master.addr, &[]), Some(0));

    // A restart reuses the same secret instead of generating another.
    let before = fs::read_to_string(&secret_file).unwrap();
    drop(master);
    let master = start_master(home.path(), "0.0.0.0:0", &[]);
    assert_eq!(fs::read_to_string(&secret_file).unwrap(), before);
    assert!(!master
        .service
        .log()
        .contains("Generated a new cluster secret"));
}

#[test]
fn test_insecure_flag_allows_an_exposed_master_without_a_secret_but_warns_loudly() {
    let home = tempdir();
    let master = start_master(home.path(), "0.0.0.0:0", &["--insecure"]);
    let log = master.service.log();
    assert!(log.contains("WARNING: --insecure"), "{log}");
    assert!(log.contains("WITHOUT a cluster secret"), "{log}");
    assert!(
        !home.path().join(".oxideswarm").exists(),
        "--insecure must not generate a secret"
    );
    assert_eq!(worker_count(home.path(), &master.addr, &[]), Some(0));
}

#[test]
fn test_invalid_listen_address_fails_before_any_secret_is_created() {
    let home = tempdir();
    let out = run({
        let mut c = cli(home.path());
        c.args(["master", "--listen", "not-an-address"]);
        c
    });
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("Invalid listen address"));
    assert!(!home.path().join(".oxideswarm").exists());
}

#[test]
fn test_missing_explicit_secret_file_is_reported_with_the_fix() {
    let home = tempdir();
    let missing = home.path().join("nope.secret");
    let out = run({
        let mut c = cli(home.path());
        c.args(["workers", "--master", "127.0.0.1:1", "--secret-file"])
            .arg(&missing);
        c
    });
    assert!(!out.status.success());
    let stderr = text(&out.stderr);
    assert!(stderr.contains("does not exist"), "{stderr}");
    assert!(stderr.contains("secret generate --out"), "{stderr}");
}

// ---------------------------------------------------------------------------------------------
// Worker / client policy: no secret => refuse remote, `auto` and P2P-ticket masters
// ---------------------------------------------------------------------------------------------

/// Runs `cmd` to completion, killing it (and failing) if it outlives `timeout`: a policy
/// regression would leave a worker or a discovery loop running forever.
fn run_bounded(mut cmd: Command, timeout: Duration) -> Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn rusty-grid");
    let exited = wait_for(timeout, || matches!(child.try_wait(), Ok(Some(_))));
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
        panic!("command did not exit within {timeout:?}; it should have been refused up front");
    }
    child.wait_with_output().expect("collect output")
}

/// Every command that connects to a master, pointed at `master` (no secret, no `--insecure`).
fn peer_commands(master: &str, marker: &Path) -> Vec<(&'static str, Vec<String>)> {
    let to_strings = |parts: &[&str]| parts.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    vec![
        ("worker", to_strings(&["worker", "--master", master])),
        (
            "submit",
            [
                to_strings(&["submit", "--master", master, "--", "touch"]),
                vec![marker.display().to_string()],
            ]
            .concat(),
        ),
        ("status", to_strings(&["status", "--master", master])),
        ("workers", to_strings(&["workers", "--master", master])),
        (
            "mapreduce",
            to_strings(&["mapreduce", "--input", "a b a", "--master", master]),
        ),
    ]
}

#[test]
fn test_peer_commands_refuse_remote_and_auto_masters_without_a_secret() {
    let home = tempdir();
    let marker = home.path().join("must-not-exist");

    // 192.0.2.1 is TEST-NET-1 (never routed): reaching it would hang, so the refusal must come
    // before any connection attempt. `auto` must not even start a discovery probe.
    for master in [
        "192.0.2.1:8088",
        "192.0.2.1",
        "example.invalid",
        "auto",
        "auto:18089",
    ] {
        for (name, args) in peer_commands(master, &marker) {
            let mut cmd = cli(home.path());
            cmd.args(&args);
            let out = run_bounded(cmd, Duration::from_secs(20));
            let stderr = text(&out.stderr);
            assert_eq!(
                out.status.code(),
                Some(6),
                "{name} --master {master}: stderr: {stderr}"
            );
            assert!(
                stderr.contains("refusing to connect"),
                "{name} --master {master}: {stderr}"
            );
            assert!(stderr.contains("--insecure"), "{name}: {stderr}");
            assert!(stderr.contains("--secret-file"), "{name}: {stderr}");
            assert!(
                !stderr.contains("Registering") && !stderr.contains("Discovered"),
                "{name} --master {master} went on to talk to the network: {stderr}"
            );
        }
    }
    assert!(!marker.exists(), "no task may have been sent anywhere");
    assert!(
        !home.path().join(".oxideswarm").exists(),
        "clients and workers must not generate a secret"
    );
}

#[test]
fn test_p2p_ticket_requires_a_secret_even_next_to_a_loopback_master() {
    let home = tempdir();
    let mut cmd = cli(home.path());
    cmd.args([
        "worker",
        "--master",
        "127.0.0.1:8088",
        "--p2p-ticket",
        "not-a-real-ticket",
    ]);
    let out = run_bounded(cmd, Duration::from_secs(20));
    let stderr = text(&out.stderr);
    assert_eq!(out.status.code(), Some(6), "{stderr}");
    assert!(stderr.contains("P2P ticket"), "{stderr}");
}

#[test]
#[allow(clippy::type_complexity)]
fn test_blank_secret_is_a_configuration_error_not_an_open_cluster() {
    let home = tempdir();
    // Even towards a loopback master (which would otherwise run open) a blank secret source is a
    // mistake to report, never a silent "no secret".
    let blank_sources: [(&str, Box<dyn Fn(&mut Command)>); 3] = [
        (
            "--secret is set but empty",
            Box::new(|c: &mut Command| {
                c.args(["--secret", ""]);
            }),
        ),
        (
            "OXIDESWARM_SECRET is set but empty",
            Box::new(|c: &mut Command| {
                c.env("OXIDESWARM_SECRET", "");
            }),
        ),
        (
            "OXIDESWARM_SECRET_FILE is set but empty",
            Box::new(|c: &mut Command| {
                c.env("OXIDESWARM_SECRET_FILE", "  ");
            }),
        ),
    ];
    for (expected, apply) in &blank_sources {
        for sub in [vec!["workers"], vec!["worker"]] {
            let mut cmd = cli(home.path());
            cmd.args(&sub).args(["--master", "127.0.0.1:1"]);
            apply(&mut cmd);
            let out = run_bounded(cmd, Duration::from_secs(20));
            let stderr = text(&out.stderr);
            assert_eq!(out.status.code(), Some(1), "{sub:?} {expected}: {stderr}");
            assert!(stderr.contains(expected), "{sub:?}: {stderr}");
            assert!(
                !stderr.contains("Failed to connect"),
                "{sub:?} must fail before connecting: {stderr}"
            );
        }
    }
}

#[test]
fn test_insecure_opt_in_lets_a_worker_and_clients_use_a_remote_master_without_a_secret() {
    // Needs a LAN address to play "remote"; offline machines have nothing to test here.
    let Some(lan_ip) = rusty_grid_core::discovery::get_local_ip() else {
        eprintln!("skipping: this machine has no non-loopback address");
        return;
    };
    let home = tempdir();
    let master = start_master(home.path(), "0.0.0.0:0", &["--insecure"]);
    let port = master.addr.rsplit(':').next().unwrap();
    let remote = format!("{lan_ip}:{port}");

    // Without the opt-in the same address is refused up front.
    let mut cmd = cli(home.path());
    cmd.args(["workers", "--master", &remote]);
    let out = run_bounded(cmd, Duration::from_secs(20));
    assert_eq!(out.status.code(), Some(6), "{}", text(&out.stderr));

    // --insecure and RUSTY_GRID_INSECURE both opt in, with a warning on stderr (stdout stays
    // clean for --json consumers).
    let mut cmd = cli(home.path());
    cmd.args(["workers", "--master", &remote, "--json", "--insecure"]);
    let out = run_bounded(cmd, Duration::from_secs(20));
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("WARNING: --insecure"),
        "{}",
        text(&out.stderr)
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&out.stdout).is_ok());

    let mut cmd = cli(home.path());
    cmd.env("RUSTY_GRID_INSECURE", "1")
        .args(["status", "--master", &remote]);
    let out = run_bounded(cmd, Duration::from_secs(20));
    assert!(out.status.success(), "{}", text(&out.stderr));

    // A worker registers once it has opted in, and says plainly that authentication is off.
    let worker = start_worker(home.path(), &remote, "insecure-optin", &["--insecure"]);
    assert!(
        wait_for(Duration::from_secs(20), || {
            worker_count(home.path(), &remote, &["--insecure"]) == Some(1)
        }),
        "worker never registered.\nworker log:\n{}",
        worker.log()
    );
    let log = worker.log();
    assert!(log.contains("WARNING: --insecure"), "{log}");
    assert!(log.contains("Cluster authentication: OFF"), "{log}");
}
