//! End-to-end tests for LAN short-code pairing (`oxideswarm invite --code` and
//! `oxideswarm join --code`, design record `docs/design/0052-short-code-pairing.md`).
//!
//! Runs completely offline in isolated temp `HOME` / `USERPROFILE` / `OXIDESWARM_HOME` directories
//! with loopback listeners (`RUSTY_GRID_PAIR_BIND_HOST=127.0.0.1`, ephemeral UDP `0` and TCP `0`).

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("OXIDESWARM_MASTER")
        .env_remove("OXIDESWARM_KEY")
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
        .env_remove("RUSTY_GRID_PAIR_BIND_HOST")
        .env_remove("RUSTY_GRID_PAIR_UDP_PORT")
        .env("RUSTY_GRID_P2P_MINIMAL", "1")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .stdin(Stdio::null());
    cmd
}

fn with(home: &Path, args: &[&str]) -> Command {
    let mut c = cli(home);
    c.args(args);
    c
}

fn run_cmd(mut cmd: Command) -> Output {
    cmd.output().expect("spawn oxideswarm CLI")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn wait_for(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Long-running child process (master / worker) that logs to a file and terminates on drop.
struct Service {
    child: Option<Child>,
    log_path: PathBuf,
}

impl Service {
    fn spawn(mut cmd: Command, log_path: PathBuf) -> Self {
        let out = fs::File::create(&log_path).expect("create log file");
        let err = out.try_clone().expect("clone log file");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        let child = cmd.spawn().expect("spawn background service");
        Self {
            child: Some(child),
            log_path,
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Interactive child process with non-blocking chunked stdout/stderr capture and writable stdin.
struct StreamingChild {
    child: Option<Child>,
    stdin: Option<std::process::ChildStdin>,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    out_thread: Option<JoinHandle<()>>,
    err_thread: Option<JoinHandle<()>>,
}

impl StreamingChild {
    fn spawn(mut cmd: Command) -> Self {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn interactive child");
        let stdin = child.stdin.take();
        let stdout_pipe = child.stdout.take().expect("stdout pipe");
        let stderr_pipe = child.stderr.take().expect("stderr pipe");

        let stdout = Arc::new(Mutex::new(String::new()));
        let stderr = Arc::new(Mutex::new(String::new()));

        let out_acc = Arc::clone(&stdout);
        let out_thread = std::thread::spawn(move || read_pipe_chunks(stdout_pipe, out_acc));
        let err_acc = Arc::clone(&stderr);
        let err_thread = std::thread::spawn(move || read_pipe_chunks(stderr_pipe, err_acc));

        Self {
            child: Some(child),
            stdin,
            stdout,
            stderr,
            out_thread: Some(out_thread),
            err_thread: Some(err_thread),
        }
    }

    fn stdout_snapshot(&self) -> String {
        self.stdout.lock().expect("stdout lock").clone()
    }

    fn stderr_snapshot(&self) -> String {
        self.stderr.lock().expect("stderr lock").clone()
    }

    fn write_line(&mut self, line: &str) {
        if let Some(ref mut stdin) = self.stdin {
            writeln!(stdin, "{line}").expect("write line to child stdin");
            stdin.flush().expect("flush child stdin");
        }
    }

    fn wait_exit(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(ref mut child) = self.child {
                if let Some(status) = child.try_wait().expect("try_wait child") {
                    self.stdin.take();
                    if let Some(h) = self.out_thread.take() {
                        let _ = h.join();
                    }
                    if let Some(h) = self.err_thread.take() {
                        let _ = h.join();
                    }
                    self.child.take();
                    return status;
                }
            }
            if Instant::now() >= deadline {
                panic!(
                    "child timed out after {timeout:?}\nstdout:\n{}\nstderr:\n{}",
                    self.stdout_snapshot(),
                    self.stderr_snapshot()
                );
            }
            std::thread::sleep(Duration::from_millis(40));
        }
    }
}

impl Drop for StreamingChild {
    fn drop(&mut self) {
        self.stdin.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(h) = self.out_thread.take() {
            let _ = h.join();
        }
        if let Some(h) = self.err_thread.take() {
            let _ = h.join();
        }
    }
}

fn read_pipe_chunks<R: Read>(mut reader: R, acc: Arc<Mutex<String>>) {
    let mut buf = [0u8; 1024];
    loop {
        match reader.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let chunk = String::from_utf8_lossy(&buf[..n]);
                acc.lock().expect("pipe acc lock").push_str(&chunk);
            }
        }
    }
}

fn start_loopback_master(home: &Path) -> (Service, String) {
    let gen = run_cmd(with(home, &["secret", "generate"]));
    assert!(
        gen.status.success(),
        "secret generate failed: {}",
        text(&gen.stderr)
    );
    start_master_with_args(home, &[])
}

fn start_loopback_members_only_master(home: &Path) -> (Service, String) {
    start_master_with_args(home, &["--auth", "members"])
}

fn start_master_with_args(home: &Path, extra_args: &[&str]) -> (Service, String) {
    let port_file = home.join("master.port");
    let mut cmd = cli(home);
    cmd.args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&port_file)
        .args(extra_args);
    let master = Service::spawn(cmd, home.join("master.log"));

    let mut port = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port.is_some() && master.log().contains("Master v3 public key:")
        }),
        "master did not become ready:\n{}",
        master.log()
    );
    let addr = format!("127.0.0.1:{}", port.unwrap());
    (master, addr)
}

fn extract_after(haystack: &str, prefix: &str) -> Option<String> {
    let start = haystack.find(prefix)? + prefix.len();
    haystack[start..]
        .split_whitespace()
        .next()
        .map(str::to_string)
}

fn extract_udp_port(haystack: &str) -> Option<u16> {
    let start = haystack.find("(udp ")? + "(udp ".len();
    let end = haystack[start..].find(')')?;
    let addr = haystack[start..start + end].trim();
    addr.rsplit_once(':')?.1.parse::<u16>().ok()
}

fn extract_pairing_check_digits(haystack: &str) -> Option<String> {
    let marker = "Pairing check ";
    let start = haystack.find(marker)? + marker.len();
    let rest = &haystack[start..];
    let digits: String = rest
        .chars()
        .take(7)
        .filter(|c| c.is_ascii_digit())
        .collect();
    (digits.len() == 6).then_some(digits)
}

#[test]
fn invite_code_and_join_code_enrol_worker_through_stdin() {
    let master_home = TempDir::new().unwrap();
    let joiner_home = TempDir::new().unwrap();
    let (master, master_addr) = start_loopback_master(master_home.path());

    let mut inv_cmd = with(
        master_home.path(),
        &["invite", "--code", "--master", &master_addr],
    );
    inv_cmd
        .env("RUSTY_GRID_PAIR_BIND_HOST", "127.0.0.1")
        .env("RUSTY_GRID_PAIR_UDP_PORT", "0");
    let mut inviter = StreamingChild::spawn(inv_cmd);

    let mut code = None;
    let mut pair_host = None;
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = inviter.stdout_snapshot();
            code = extract_after(&out, "Pairing code: ");
            pair_host = extract_after(&out, "--pair-host ");
            code.is_some() && pair_host.is_some()
        }),
        "invite --code did not print pairing code & --pair-host\nstdout:\n{}\nstderr:\n{}",
        inviter.stdout_snapshot(),
        inviter.stderr_snapshot()
    );
    let code = code.unwrap();
    let pair_host = pair_host.unwrap();

    let mut join_cmd = with(
        joiner_home.path(),
        &["join", "--code", &code, "--pair-host", &pair_host],
    );
    join_cmd
        .env("RUSTY_GRID_PAIR_BIND_HOST", "127.0.0.1")
        .env("RUSTY_GRID_WORKER_NAME", "paired-worker");
    let mut joiner = StreamingChild::spawn(join_cmd);

    let mut inviter_digits = None;
    let mut joiner_digits = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            let inv_out = inviter.stdout_snapshot();
            let join_out = joiner.stdout_snapshot();
            if inv_out.contains("wants to join. Pairing check ") && inv_out.contains("Approve? [y/N]")
            {
                inviter_digits = extract_pairing_check_digits(&inv_out);
            }
            if join_out.contains("Confirm that the cluster admin's master shows") {
                joiner_digits = extract_after(&join_out, "Pairing code: ")
                    .map(|s| s.chars().filter(|c| c.is_ascii_digit()).collect());
            }
            inviter_digits.is_some() && joiner_digits.is_some()
        }),
        "prompts did not appear\ninviter stdout:\n{}\ninviter stderr:\n{}\njoiner stdout:\n{}\njoiner stderr:\n{}",
        inviter.stdout_snapshot(),
        inviter.stderr_snapshot(),
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );
    assert_eq!(
        inviter_digits, joiner_digits,
        "pairing check digits must match across both terminals"
    );

    // Confirm on both sides in order (inviter approves, joiner confirms).
    inviter.write_line("y");
    joiner.write_line("y");

    let inv_status = inviter.wait_exit(Duration::from_secs(15));
    let join_status = joiner.wait_exit(Duration::from_secs(15));
    assert!(
        inv_status.success(),
        "inviter failed: stdout:\n{}\nstderr:\n{}",
        inviter.stdout_snapshot(),
        inviter.stderr_snapshot()
    );
    assert!(
        join_status.success(),
        "joiner failed: stdout:\n{}\nstderr:\n{}",
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );

    assert!(
        joiner_home
            .path()
            .join(".oxideswarm")
            .join("node.key")
            .is_file(),
        "joiner node.key should be saved"
    );
    assert!(
        !joiner_home
            .path()
            .join(".oxideswarm")
            .join("cluster.secret")
            .exists(),
        "short-code pairing uses machine identity invites and must not copy cluster.secret"
    );

    // Start the paired worker using its saved config and run one task.
    let mut worker_cmd = cli(joiner_home.path());
    worker_cmd
        .args([
            "worker",
            "--name",
            "paired-worker",
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(joiner_home.path().join("sandbox"));
    let worker = Service::spawn(worker_cmd, joiner_home.path().join("worker.log"));

    assert!(
        wait_for(Duration::from_secs(20), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--json", "--master", &master_addr],
            ));
            out.status.success() && text(&out.stdout).contains("\"paired-worker\"")
        }),
        "paired-worker did not register\nmaster log:\n{}\nworker log:\n{}",
        master.log(),
        worker.log()
    );

    let submit_out = run_cmd(with(
        master_home.path(),
        &[
            "submit",
            "--master",
            &master_addr,
            "--worker",
            "paired-worker",
            "--wait",
            "--json",
            "--",
            "echo",
            "paired-ok",
        ],
    ));
    assert!(
        submit_out.status.success() && text(&submit_out.stdout).contains("paired-ok"),
        "submit via paired-worker failed:\nstdout:\n{}\nstderr:\n{}",
        text(&submit_out.stdout),
        text(&submit_out.stderr)
    );
}

#[test]
fn wrong_code_burns_invite_and_writes_nothing_on_joiner() {
    let master_home = TempDir::new().unwrap();
    let joiner_home = TempDir::new().unwrap();
    let (_master, master_addr) = start_loopback_master(master_home.path());

    let mut inv_cmd = with(
        master_home.path(),
        &["invite", "--code", "--master", &master_addr],
    );
    inv_cmd
        .env("RUSTY_GRID_PAIR_BIND_HOST", "127.0.0.1")
        .env("RUSTY_GRID_PAIR_UDP_PORT", "0");
    let mut inviter = StreamingChild::spawn(inv_cmd);

    let mut code = None;
    let mut pair_host = None;
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = inviter.stdout_snapshot();
            code = extract_after(&out, "Pairing code: ");
            pair_host = extract_after(&out, "--pair-host ");
            code.is_some() && pair_host.is_some()
        }),
        "invite --code did not start\nstdout:\n{}\nstderr:\n{}",
        inviter.stdout_snapshot(),
        inviter.stderr_snapshot()
    );
    let real_code = code.unwrap();
    let pair_host = pair_host.unwrap();
    let (nameplate, words) = real_code.split_once('-').expect("valid code format");
    let wrong_code = if words == "abandon-zoo" {
        format!("{nameplate}-ability-zebra")
    } else {
        format!("{nameplate}-abandon-zoo")
    };

    let mut join_cmd = with(
        joiner_home.path(),
        &["join", "--code", &wrong_code, "--pair-host", &pair_host],
    );
    join_cmd.env("RUSTY_GRID_PAIR_BIND_HOST", "127.0.0.1");
    let join_out = run_cmd(join_cmd);
    assert!(!join_out.status.success(), "wrong code must fail on joiner");
    assert!(
        !joiner_home.path().join(".oxideswarm").exists(),
        "failed wrong-code join must write nothing"
    );

    let inv_status = inviter.wait_exit(Duration::from_secs(15));
    assert!(
        !inv_status.success(),
        "inviter must exit with failure after burning code"
    );
    let inv_err = inviter.stderr_snapshot();
    assert!(
        inv_err.contains(
            "the code was used by someone who did not know it; nothing was shared; make a new code"
        ),
        "missing expected burn message in inviter stderr:\n{inv_err}"
    );

    // Confirm the underlying invite was revoked on the master.
    let list_out = run_cmd(with(
        master_home.path(),
        &["invite", "--list", "--json", "--master", &master_addr],
    ));
    assert!(list_out.status.success());
    let invites: serde_json::Value = serde_json::from_slice(&list_out.stdout).unwrap();
    let first = invites
        .as_array()
        .and_then(|a| a.first())
        .expect("one created invite in list");
    assert_eq!(
        first["revoked"].as_bool(),
        Some(true),
        "burned invite must be revoked on master: {invites}"
    );
}

#[test]
fn udp_discovery_pairs_on_loopback_without_pair_host() {
    let master_home = TempDir::new().unwrap();
    let joiner_home = TempDir::new().unwrap();
    let (_master, master_addr) = start_loopback_members_only_master(master_home.path());

    let mut inv_cmd = with(
        master_home.path(),
        &["invite", "--code", "--master", &master_addr],
    );
    inv_cmd
        .env("RUSTY_GRID_PAIR_BIND_HOST", "127.0.0.1")
        .env("RUSTY_GRID_PAIR_UDP_PORT", "0");
    let mut inviter = StreamingChild::spawn(inv_cmd);

    let mut code = None;
    let mut udp_port = None;
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = inviter.stdout_snapshot();
            code = extract_after(&out, "Pairing code: ");
            udp_port = extract_udp_port(&out);
            code.is_some() && udp_port.is_some()
        }),
        "invite --code did not report code + ephemeral udp port\nstdout:\n{}\nstderr:\n{}",
        inviter.stdout_snapshot(),
        inviter.stderr_snapshot()
    );
    let code = code.unwrap();
    let udp_port = udp_port.unwrap().to_string();

    // Notice: NO `--pair-host` passed here; discovery happens solely over UDP on loopback.
    let mut join_cmd = with(joiner_home.path(), &["join", "--code", &code]);
    join_cmd
        .env("RUSTY_GRID_PAIR_BIND_HOST", "127.0.0.1")
        .env("RUSTY_GRID_PAIR_UDP_PORT", &udp_port)
        .env("RUSTY_GRID_WORKER_NAME", "udp-discovered-worker");
    let mut joiner = StreamingChild::spawn(join_cmd);

    assert!(
        wait_for(Duration::from_secs(20), || {
            inviter.stdout_snapshot().contains("Approve? [y/N]")
                && joiner
                    .stdout_snapshot()
                    .contains("Confirm that the cluster admin's master shows")
        }),
        "UDP-discovered pairing prompts did not appear\ninviter:\n{}\n{}\njoiner:\n{}\n{}",
        inviter.stdout_snapshot(),
        inviter.stderr_snapshot(),
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );

    inviter.write_line("y");
    joiner.write_line("y");

    assert!(inviter.wait_exit(Duration::from_secs(15)).success());
    assert!(joiner.wait_exit(Duration::from_secs(15)).success());

    let members_out = run_cmd(with(
        master_home.path(),
        &["members", "list", "--json", "--master", &master_addr],
    ));
    assert!(
        members_out.status.success()
            && text(&members_out.stdout).contains("\"udp-discovered-worker\""),
        "UDP-paired worker missing from members list: {}",
        text(&members_out.stdout)
    );
}

#[test]
fn join_refuses_positional_secret_with_or_without_code_without_echo() {
    let home = TempDir::new().unwrap();

    for (args, secret) in [
        (vec!["join", "SUPER_SECRET_ARG_1"], "SUPER_SECRET_ARG_1"),
        (
            vec!["join", "--code", "7-rocket-violin", "SUPER_SECRET_ARG_2"],
            "SUPER_SECRET_ARG_2",
        ),
        (
            vec![
                "join",
                "--code",
                "oxs1-invite:SUPER_SECRET_ARG_3.0123456789abcdef0123456789abcdef",
            ],
            "SUPER_SECRET_ARG_3",
        ),
    ] {
        let out = run_cmd(with(home.path(), &args));
        assert!(!out.status.success(), "expected failure for args {args:?}");
        let combined = format!("{}\n{}", text(&out.stdout), text(&out.stderr));
        assert!(
            !combined.contains(secret),
            "CLI must never echo secret argument back:\n{combined}"
        );
    }

    let missing_code = run_cmd(with(
        home.path(),
        &["join", "--pair-host", "127.0.0.1:9000"],
    ));
    assert!(!missing_code.status.success());
}
