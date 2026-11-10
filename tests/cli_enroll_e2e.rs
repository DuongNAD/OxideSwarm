//! End-to-end CLI integration tests for OXS-AUTH v3 invites, enrollment, pairing check,
//! pending request lifecycle, worker-id ownership, roles, P2P ticket invites, and host admin
//! enrolment (`ENROLL-1b`, design record `docs/design/0051-machine-identity-and-invites.md`).

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_cli::members_cmd::InviteToken;
use rusty_grid_core::noise_auth::{MemberRole, NodeKeypair, RedactedBytes32};
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");

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

fn run_with_stdin(mut cmd: Command, input: &str) -> Output {
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn rusty-grid");
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(input.as_bytes());
    }
    child.wait_with_output().expect("wait rusty-grid")
}

/// Drives an interactive `oxideswarm join` session using a real Unix pseudo-terminal on `stdin`
/// so the invite token is entered at the hidden prompt first, and the `[y/N]` confirmation is
/// entered on the terminal *only after* `join` has printed the pairing code and master fingerprint.
#[cfg(unix)]
struct InteractiveJoin {
    pty_master: fs::File,
    child: Child,
    stdout_buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    stderr_buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    stdout_thread: Option<std::thread::JoinHandle<()>>,
    stderr_thread: Option<std::thread::JoinHandle<()>>,
}

#[cfg(unix)]
impl InteractiveJoin {
    fn spawn(mut cmd: Command, invite: &str) -> Self {
        use std::io::Read;
        use std::os::fd::FromRawFd;

        let (mut master_fd, mut slave_fd) = (0, 0);
        // SAFETY: `openpty` initializes both file descriptors on return code 0.
        let rc = unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, 0, "openpty failed");
        // SAFETY: both descriptors are valid and uniquely owned from here on.
        let (mut pty_master, pty_slave) = unsafe {
            (
                fs::File::from_raw_fd(master_fd),
                fs::File::from_raw_fd(slave_fd),
            )
        };

        cmd.stdin(Stdio::from(pty_slave))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn interactive join");
        let mut child_stdout = child.stdout.take().expect("child stdout");
        let mut child_stderr = child.stderr.take().expect("child stderr");

        let stdout_buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let stderr_buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let stdout_clone = stdout_buf.clone();
        let stderr_clone = stderr_buf.clone();

        let stdout_thread = std::thread::spawn(move || {
            let mut chunk = [0u8; 512];
            while let Ok(n) = child_stdout.read(&mut chunk) {
                if n == 0 {
                    break;
                }
                stdout_clone.lock().unwrap().extend_from_slice(&chunk[..n]);
            }
        });
        let stderr_thread = std::thread::spawn(move || {
            let mut chunk = [0u8; 512];
            while let Ok(n) = child_stderr.read(&mut chunk) {
                if n == 0 {
                    break;
                }
                stderr_clone.lock().unwrap().extend_from_slice(&chunk[..n]);
            }
        });

        let sess = Self {
            pty_master: pty_master.try_clone().expect("clone pty_master"),
            child,
            stdout_buf,
            stderr_buf,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
        };

        assert!(
            wait_for(Duration::from_secs(10), || {
                sess.stderr().contains("Paste the join token")
            }),
            "join did not prompt for the token; stderr:\n{}",
            sess.stderr()
        );
        pty_master
            .write_all(format!("{invite}\n").as_bytes())
            .expect("write invite to pty");
        pty_master.flush().expect("flush pty");
        sess
    }

    fn stdout(&self) -> String {
        text(&self.stdout_buf.lock().unwrap())
    }

    fn stderr(&self) -> String {
        text(&self.stderr_buf.lock().unwrap())
    }

    fn wait_for_pairing_prompt(&self, timeout: Duration) -> bool {
        wait_for(timeout, || {
            self.stdout().contains(
                "Confirm that the cluster admin's master shows this pairing code and fingerprint [y/N]:",
            )
        })
    }

    fn confirm(&mut self, answer: &str) {
        assert!(
            self.wait_for_pairing_prompt(Duration::from_secs(15)),
            "join did not print pairing confirmation prompt before confirm({answer:?});\nstdout:\n{}\nstderr:\n{}",
            self.stdout(),
            self.stderr()
        );
        self.pty_master
            .write_all(format!("{answer}\n").as_bytes())
            .expect("write confirmation to pty");
        self.pty_master.flush().expect("flush pty");
    }

    fn wait_output(mut self, timeout: Duration) -> Output {
        let start = Instant::now();
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("try_wait join") {
                break status;
            }
            assert!(
                start.elapsed() < timeout,
                "join did not exit within {timeout:?};\nstdout:\n{}\nstderr:\n{}",
                self.stdout(),
                self.stderr()
            );
            std::thread::sleep(Duration::from_millis(25));
        };
        if let Some(h) = self.stdout_thread.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stderr_thread.take() {
            let _ = h.join();
        }
        Output {
            status,
            stdout: self.stdout_buf.lock().unwrap().clone(),
            stderr: self.stderr_buf.lock().unwrap().clone(),
        }
    }

    fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(unix)]
impl Drop for InteractiveJoin {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
fn test_cli_enroll_invite_join_approve_roles_contention_and_host_admin() {
    let master_home = TempDir::new().unwrap();
    let worker_1_home = TempDir::new().unwrap();
    let worker_2_home = TempDir::new().unwrap();
    let contender_a_home = TempDir::new().unwrap();
    let contender_b_home = TempDir::new().unwrap();
    let drop_home = TempDir::new().unwrap();
    let expired_home = TempDir::new().unwrap();
    let declined_home = TempDir::new().unwrap();
    let v2_worker_home = TempDir::new().unwrap();

    // 1. Generate default cluster.secret in `master_home` (for v2 coexistence later) and start
    //    Master (default `--auth both`). Verify it auto-enrols the host's own `node.key` as an
    //    `admin` member in `members.json`.
    let gen_secret = run_cmd(with(master_home.path(), &["secret", "generate"]));
    assert!(gen_secret.status.success(), "{}", text(&gen_secret.stderr));
    let secret_file = master_home
        .path()
        .join(".oxideswarm")
        .join("cluster.secret");
    let v2_secret_dir = v2_worker_home.path().join(".oxideswarm");
    fs::create_dir_all(&v2_secret_dir).unwrap();
    fs::copy(&secret_file, v2_secret_dir.join("cluster.secret")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            v2_secret_dir.join("cluster.secret"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }

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
        "master did not start; log:\n{}",
        master.log()
    );
    let addr = format!("127.0.0.1:{}", port.unwrap());

    let host_node_key_path = master_home.path().join(".oxideswarm").join("node.key");
    let master_noise_path = master_home
        .path()
        .join(".oxideswarm")
        .join("master_noise.key");
    let members_path = master_home.path().join(".oxideswarm").join("members.json");
    assert!(host_node_key_path.is_file(), "master host node.key created");
    assert!(
        master_noise_path.is_file(),
        "master_noise.key created on startup"
    );
    assert!(members_path.is_file(), "members.json created on startup");
    assert_private(&host_node_key_path);
    assert_private(&master_noise_path);
    assert_private(&members_path);

    let master_kp = NodeKeypair::load_file(&master_noise_path).unwrap();
    let master_fp = master_kp.fingerprint();

    // Verify `oxideswarm members list` on the master's host works via the auto-enrolled host admin key.
    let list_out = run_cmd(with(
        master_home.path(),
        &["members", "list", "--master", &addr, "--json"],
    ));
    assert!(
        list_out.status.success(),
        "members list failed: {}",
        text(&list_out.stderr)
    );
    assert!(
        text(&list_out.stdout).contains("\"local-host\""),
        "members list must include auto-enrolled local-host admin: {}",
        text(&list_out.stdout)
    );

    // 2. Expired invite and declined confirmation write nothing.
    let expired_token = InviteToken {
        master: Some(addr.clone()),
        ticket: None,
        master_key: *master_kp.public_key(),
        invite_id: "00112233445566778899aabbccddeeff".into(),
        invite_secret: RedactedBytes32([0x11; 32]),
        expires_at: 1, // expired in 1970
        roles: vec![MemberRole::Worker],
        require_approval: true,
    };
    let exp_out = run_with_stdin(
        with(expired_home.path(), &["join"]),
        &format!("{}\n", expired_token.encode()),
    );
    assert!(
        !exp_out.status.success(),
        "expired invite must fail: {}",
        text(&exp_out.stdout)
    );
    assert!(
        !expired_home.path().join(".oxideswarm").exists(),
        "expired invite must write nothing to ~/.oxideswarm"
    );

    // Master-side expired invite refusal (Item 16): create a 1s invite on the master, wait for it
    // to expire on the master, and redeem a copy whose client-side `expires_at` is still in the
    // future (simulating clock skew) so `join` reaches the master and the master rejects it.
    let short_inv_out = run_cmd(with(
        master_home.path(),
        &["invite", "--master", &addr, "--expires", "1s"],
    ));
    assert!(short_inv_out.status.success());
    let mut skewed_expired =
        InviteToken::decode(text(&short_inv_out.stdout).trim()).expect("decode short invite");
    skewed_expired.expires_at += 3600;
    std::thread::sleep(Duration::from_millis(1200));
    let master_exp_out = run_with_stdin(
        with(expired_home.path(), &["join"]),
        &format!("{}\n", skewed_expired.encode()),
    );
    assert!(
        !master_exp_out.status.success(),
        "master-side expired invite must fail"
    );
    assert!(
        text(&master_exp_out.stderr).contains("invalid, expired, or revoked invite"),
        "expected master-side expired invite error, got: {}",
        text(&master_exp_out.stderr)
    );
    assert!(!expired_home.path().join(".oxideswarm").exists());

    // Create a valid 1-use worker invite and decline the [y/N] confirmation.
    let inv_decline_out = run_cmd(with(master_home.path(), &["invite", "--master", &addr]));
    assert!(
        inv_decline_out.status.success(),
        "invite failed: {}",
        text(&inv_decline_out.stderr)
    );
    let inv_decline = text(&inv_decline_out.stdout).trim().to_string();

    // Item 9: piping both the invite and `y` on stdin (`printf '%s\ny\n' "$INVITE" | oxideswarm join`)
    // is rejected as invalid input before connecting or writing anything.
    let prestuffed_out = run_with_stdin(
        with(declined_home.path(), &["join"]),
        &format!("{inv_decline}\ny\n"),
    );
    assert!(
        !prestuffed_out.status.success(),
        "pre-stuffed multi-line piped stdin must be rejected"
    );
    assert!(!declined_home.path().join(".oxideswarm").exists());

    let mut dec_join = InteractiveJoin::spawn(with(declined_home.path(), &["join"]), &inv_decline);
    dec_join.confirm("n");
    let dec_out = dec_join.wait_output(Duration::from_secs(10));
    assert!(
        !dec_out.status.success(),
        "declining confirmation must fail"
    );
    assert!(
        !declined_home.path().join(".oxideswarm").exists(),
        "declined confirmation must write nothing to ~/.oxideswarm"
    );

    // 3. A pending request whose connection closes takes no use:
    //    `declined_home` connected with `inv_decline`, entered pending state, and closed its
    //    connection when `n` was entered. Wait for pending list to clear and verify `inv_decline`
    //    still has `uses_remaining == 1`.
    assert!(
        wait_for(Duration::from_secs(10), || {
            let out = run_cmd(with(
                master_home.path(),
                &["members", "pending", "--master", &addr, "--json"],
            ));
            out.status.success() && text(&out.stdout).trim() == "[]"
        }),
        "pending list did not clear after peer disconnect"
    );
    let inv_list_after_drop = run_cmd(with(
        master_home.path(),
        &["invite", "--list", "--master", &addr, "--json"],
    ));
    assert!(inv_list_after_drop.status.success());
    let inv_list_json: serde_json::Value =
        serde_json::from_slice(&inv_list_after_drop.stdout).unwrap();
    let decoded_decline = InviteToken::decode(&inv_decline).unwrap();
    let decline_entry = inv_list_json
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["invite_id"].as_str() == Some(&decoded_decline.invite_id))
        .expect("invite present in list");
    assert_eq!(
        decline_entry["uses_remaining"].as_u64(),
        Some(1),
        "disconnected pending request must not consume an invite use: {decline_entry}"
    );

    // Also test killing a live `join` process while it is waiting in `pending`:
    let mut drop_join = InteractiveJoin::spawn(with(drop_home.path(), &["join"]), &inv_decline);
    assert!(
        wait_for(Duration::from_secs(10), || {
            let out = run_cmd(with(
                master_home.path(),
                &["members", "pending", "--master", &addr, "--json"],
            ));
            if !out.status.success() {
                return false;
            }
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
            v.as_array().is_some_and(|arr| !arr.is_empty())
        }),
        "drop_child did not appear in members pending"
    );
    drop_join.kill_and_wait();
    assert!(
        wait_for(Duration::from_secs(10), || {
            let out = run_cmd(with(
                master_home.path(),
                &["members", "pending", "--master", &addr, "--json"],
            ));
            out.status.success() && text(&out.stdout).trim() == "[]"
        }),
        "killed pending join did not clear from members pending"
    );
    assert!(
        !drop_home.path().join(".oxideswarm").exists(),
        "aborted join must write nothing"
    );

    // 4. Invite, join, approve, confirm, and the worker runs a task (reusing `inv_decline` to
    //    prove its 1 use is still intact!).
    let mut join_1_cmd = with(worker_1_home.path(), &["join"]);
    join_1_cmd.env("RUSTY_GRID_WORKER_NAME", "worker-one");
    let mut join_1 = InteractiveJoin::spawn(join_1_cmd, &inv_decline);

    // Wait for `worker-one` to appear in `members pending`, then approve it.
    let mut pending_1_id = String::new();
    let mut pending_1_code = String::new();
    let mut pending_1_fp = String::new();
    let mut pending_1_remote = String::new();
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home.path(),
                &["members", "pending", "--master", &addr, "--json"],
            ));
            if !out.status.success() {
                return false;
            }
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
            if let Some(item) = v.as_array().and_then(|a| a.first()) {
                pending_1_id = item["pending_id"].as_str().unwrap_or_default().to_string();
                pending_1_code = item["pairing_code"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                pending_1_fp = item["master_fingerprint"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                pending_1_remote = item["remote_addr"].as_str().unwrap_or_default().to_string();
                !pending_1_id.is_empty()
            } else {
                false
            }
        }),
        "worker-one did not appear in members pending"
    );
    assert_eq!(pending_1_code.len(), 6, "6-digit pairing code");
    assert_eq!(pending_1_fp, master_fp, "pending entry carries master_fingerprint");
    assert!(
        pending_1_remote.starts_with("127.0.0.1:"),
        "pending entry carries remote_addr: {pending_1_remote}"
    );

    // Item 3: wrong `--code` is refused and leaves the pending request and invite use intact.
    let wrong_code = if pending_1_code == "000000" {
        "999999"
    } else {
        "000000"
    };
    let bad_approve = run_cmd(with(
        master_home.path(),
        &[
            "members",
            "approve",
            &pending_1_id,
            "--code",
            wrong_code,
            "--master",
            &addr,
        ],
    ));
    assert!(
        !bad_approve.status.success(),
        "members approve with wrong --code must fail"
    );
    assert!(
        text(&bad_approve.stderr).contains("pairing code mismatch"),
        "expected pairing code mismatch error, got: {}",
        text(&bad_approve.stderr)
    );

    let approve_1 = run_cmd(with(
        master_home.path(),
        &[
            "members",
            "approve",
            &pending_1_id,
            "--code",
            &pending_1_code,
            "--master",
            &addr,
        ],
    ));
    assert!(
        approve_1.status.success(),
        "members approve failed: {}",
        text(&approve_1.stderr)
    );

    join_1.confirm("y");
    let join_1_out = join_1.wait_output(Duration::from_secs(20));
    assert!(
        join_1_out.status.success(),
        "join_1 failed:\nstdout:\n{}\nstderr:\n{}",
        text(&join_1_out.stdout),
        text(&join_1_out.stderr)
    );
    let join_1_stdout = text(&join_1_out.stdout);
    assert!(
        join_1_stdout.contains(&master_fp),
        "join output must show master fingerprint {master_fp}:\n{join_1_stdout}"
    );
    assert!(
        join_1_stdout.contains(&pending_1_code),
        "join output must show the same 6-digit pairing code {pending_1_code}:\n{join_1_stdout}"
    );
    assert!(
        join_1_stdout.contains("code execution on this machine"),
        "join output must show the G3 code-execution warning:\n{join_1_stdout}"
    );

    let worker_1_node_key = worker_1_home.path().join(".oxideswarm").join("node.key");
    let worker_1_cfg = worker_1_home
        .path()
        .join(".oxideswarm")
        .join("rusty-grid.toml");
    assert!(worker_1_node_key.is_file());
    assert!(worker_1_cfg.is_file());
    assert_private(&worker_1_node_key);

    // Start `worker-one` using only its saved config (`~/.oxideswarm/rusty-grid.toml` + `node.key`).
    let mut w1_cmd = cli(worker_1_home.path());
    w1_cmd
        .args([
            "worker",
            "--name",
            "worker-one",
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(worker_1_home.path().join("sandbox"));
    let w1_service = Service::spawn(w1_cmd, worker_1_home.path().join("worker-one.log"));

    assert!(
        wait_for(Duration::from_secs(20), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--json", "--master", &addr],
            ));
            out.status.success() && text(&out.stdout).contains("\"worker-one\"")
        }),
        "worker-one did not register:\nmaster log:\n{}\nworker-one log:\n{}",
        master.log(),
        w1_service.log()
    );

    // Submit a task from the master host and verify `worker-one` executes it.
    let task_out = run_cmd(with(
        master_home.path(),
        &[
            "submit",
            "--master",
            &addr,
            "--worker",
            "worker-one",
            "--wait",
            "--json",
            "--",
            "echo",
            "hello-from-enrolled-worker-one",
        ],
    ));
    assert!(
        task_out.status.success(),
        "submit failed:\n{}\n{}",
        text(&task_out.stdout),
        text(&task_out.stderr)
    );
    assert!(text(&task_out.stdout).contains("hello-from-enrolled-worker-one"));

    // 5. A worker-only member cannot submit tasks (`SubmitTask` requires `client`).
    let w1_submit = run_cmd(with(
        worker_1_home.path(),
        &["submit", "--wait", "--", "echo", "forbidden-submit"],
    ));
    assert!(
        !w1_submit.status.success(),
        "worker-only member must not be able to submit tasks"
    );
    assert!(
        text(&w1_submit.stderr).contains("member does not have the client role"),
        "expected client-role error, got: {}",
        text(&w1_submit.stderr)
    );

    // 6. Two requests for a one-use invite: rejecting one and approving the other takes one use.
    let shared_inv_out = run_cmd(with(
        master_home.path(),
        &["invite", "--master", &addr, "--uses", "1"],
    ));
    assert!(shared_inv_out.status.success());
    let shared_inv = text(&shared_inv_out.stdout).trim().to_string();
    let shared_inv_id = InviteToken::decode(&shared_inv).unwrap().invite_id;

    let mut join_a_cmd = with(contender_a_home.path(), &["join", "--no-doctor"]);
    join_a_cmd.env("RUSTY_GRID_WORKER_NAME", "contender-a");
    let join_a = InteractiveJoin::spawn(join_a_cmd, &shared_inv);

    let mut join_b_cmd = with(contender_b_home.path(), &["join", "--no-doctor"]);
    join_b_cmd.env("RUSTY_GRID_WORKER_NAME", "contender-b");
    let mut join_b = InteractiveJoin::spawn(join_b_cmd, &shared_inv);

    let mut pending_a_id = String::new();
    let mut pending_b_id = String::new();
    let mut pending_b_code = String::new();
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home.path(),
                &["members", "pending", "--master", &addr, "--json"],
            ));
            if !out.status.success() {
                return false;
            }
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
            let Some(arr) = v.as_array() else {
                return false;
            };
            for item in arr {
                match item["name"].as_str() {
                    Some("contender-a") => {
                        pending_a_id = item["pending_id"].as_str().unwrap_or_default().to_string();
                    }
                    Some("contender-b") => {
                        pending_b_id = item["pending_id"].as_str().unwrap_or_default().to_string();
                        pending_b_code = item["pairing_code"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                    }
                    _ => {}
                }
            }
            !pending_a_id.is_empty() && !pending_b_id.is_empty()
        }),
        "both contenders did not appear in members pending"
    );
    assert!(
        join_a.wait_for_pairing_prompt(Duration::from_secs(10)),
        "contender-a did not print pairing prompt"
    );

    // Human `members pending` output marks the contention (`[2 requests for invite ...]`).
    let pending_human = run_cmd(with(
        master_home.path(),
        &["members", "pending", "--master", &addr],
    ));
    assert!(pending_human.status.success());
    assert!(
        text(&pending_human.stdout).contains("2 requests for invite"),
        "expected contention marker in pending output, got:\n{}",
        text(&pending_human.stdout)
    );

    // Reject contender-a while its [y/N] prompt is still unanswered; verify it exits immediately
    // without hanging for Enter (Item 9), then approve contender-b with `--code`.
    let rej_a = run_cmd(with(
        master_home.path(),
        &["members", "reject", &pending_a_id, "--master", &addr],
    ));
    assert!(rej_a.status.success(), "{}", text(&rej_a.stderr));

    let out_a = join_a.wait_output(Duration::from_secs(5));
    assert!(
        !out_a.status.success(),
        "rejected contender-a must exit non-zero"
    );
    assert!(
        !contender_a_home.path().join(".oxideswarm").exists(),
        "rejected contender-a must write nothing to ~/.oxideswarm"
    );

    let app_b = run_cmd(with(
        master_home.path(),
        &[
            "members",
            "approve",
            &pending_b_id,
            "--code",
            &pending_b_code,
            "--master",
            &addr,
        ],
    ));
    assert!(app_b.status.success(), "{}", text(&app_b.stderr));

    join_b.confirm("y");
    let out_b = join_b.wait_output(Duration::from_secs(15));
    assert!(
        out_b.status.success(),
        "approved contender-b must succeed:\n{}\n{}",
        text(&out_b.stdout),
        text(&out_b.stderr)
    );
    assert!(contender_b_home
        .path()
        .join(".oxideswarm")
        .join("node.key")
        .is_file());

    // Verify the 1-use invite now has `uses_remaining == 0`.
    let inv_list_after = run_cmd(with(
        master_home.path(),
        &["invite", "--list", "--master", &addr, "--json"],
    ));
    let inv_list_after_json: serde_json::Value =
        serde_json::from_slice(&inv_list_after.stdout).unwrap();
    let shared_entry = inv_list_after_json
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["invite_id"].as_str() == Some(&shared_inv_id))
        .unwrap();
    assert_eq!(shared_entry["uses_remaining"].as_u64(), Some(0));

    // 7. A member cannot register another member's worker_id.
    //    Enrol `worker_2_home` with `--no-approval` and attempt to register using `worker-one`'s
    //    persisted worker_id file.
    let w1_id_file = worker_1_home
        .path()
        .join(".oxideswarm")
        .join("worker_id.worker-one");
    assert!(
        w1_id_file.is_file(),
        "worker-one persisted its worker_id file"
    );

    let inv_w2_out = run_cmd(with(
        master_home.path(),
        &["invite", "--master", &addr, "--no-approval"],
    ));
    assert!(inv_w2_out.status.success());
    let inv_w2 = text(&inv_w2_out.stdout).trim().to_string();
    let mut join_w2_sess =
        InteractiveJoin::spawn(with(worker_2_home.path(), &["join", "--no-doctor"]), &inv_w2);
    join_w2_sess.confirm("y");
    let join_w2 = join_w2_sess.wait_output(Duration::from_secs(15));
    assert!(
        join_w2.status.success(),
        "worker_2 join failed:\n{}\n{}",
        text(&join_w2.stdout),
        text(&join_w2.stderr)
    );

    let mut w2_impersonate_cmd = cli(worker_2_home.path());
    w2_impersonate_cmd
        .env("RUSTY_GRID_WORKER_ID_FILE", &w1_id_file)
        .args([
            "worker",
            "--name",
            "worker-two-impostor",
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(worker_2_home.path().join("sandbox"));
    let w2_impersonate = Service::spawn(
        w2_impersonate_cmd,
        worker_2_home.path().join("worker-two.log"),
    );

    assert!(
        wait_for(Duration::from_secs(15), || {
            w2_impersonate.log().contains("is owned by another member")
                || master.log().contains(
                    "Refusing v3 worker registration for worker_id owned by another member",
                )
        }),
        "master did not refuse worker_id takeover:\nmaster log:\n{}\nworker-two log:\n{}",
        master.log(),
        w2_impersonate.log()
    );
    drop(w2_impersonate);

    // 8. A v2 secret worker and a v3 member worker run side by side under `--auth both`,
    //    and `--auth members` refuses the v2 worker with the distinct reason while the
    //    master's host CLI stays admin.
    let mut w_v2_cmd = cli(v2_worker_home.path());
    w_v2_cmd
        .args([
            "worker",
            "--name",
            "worker-v2",
            "--master",
            &addr,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(v2_worker_home.path().join("sandbox"));
    let w_v2 = Service::spawn(w_v2_cmd, v2_worker_home.path().join("w-v2.log"));

    assert!(
        wait_for(Duration::from_secs(20), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--json", "--master", &addr],
            ));
            out.status.success()
                && text(&out.stdout).contains("\"worker-one\"")
                && text(&out.stdout).contains("\"worker-v2\"")
        }),
        "v3 worker-one and v2 worker-v2 did not both register:\nmaster:\n{}\nw1:\n{}\nw_v2:\n{}",
        master.log(),
        w1_service.log(),
        w_v2.log()
    );

    drop(w_v2);
    drop(w1_service);
    drop(master);

    // Now start master with `--auth members` (while `cluster.secret` still exists in `master_home`!).
    let port_file_m = master_home.path().join("master-members.port");
    let mut master_mem_cmd = cli(master_home.path());
    master_mem_cmd
        .args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--auth",
            "members",
            "--port-file",
        ])
        .arg(&port_file_m);
    let master_mem = Service::spawn(
        master_mem_cmd,
        master_home.path().join("master-members.log"),
    );
    let mut port_m = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port_m = fs::read_to_string(&port_file_m)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port_m.is_some()
        }),
        "master_mem did not start:\n{}",
        master_mem.log()
    );
    let addr_m = format!("127.0.0.1:{}", port_m.unwrap());

    // v2 worker is refused with the distinct reason.
    let mut w_v2_m_cmd = cli(v2_worker_home.path());
    w_v2_m_cmd
        .args([
            "worker",
            "--name",
            "worker-v2",
            "--master",
            &addr_m,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(v2_worker_home.path().join("sandbox-m"));
    let w_v2_m = Service::spawn(w_v2_m_cmd, v2_worker_home.path().join("w-v2-m.log"));
    assert!(
        wait_for(Duration::from_secs(15), || {
            w_v2_m
                .log()
                .contains("this master admits enrolled machines only; join with an invite")
        }),
        "v2 worker was not refused with members-only reason:\n{}",
        w_v2_m.log()
    );

    // Master's host CLI stays admin under `--auth members` even though `cluster.secret` exists in `~/.oxideswarm`!
    let host_members_out = run_cmd(with(
        master_home.path(),
        &["members", "list", "--master", &addr_m, "--json"],
    ));
    assert!(
        host_members_out.status.success(),
        "master host CLI must stay admin under --auth members:\nstdout:\n{}\nstderr:\n{}",
        text(&host_members_out.stdout),
        text(&host_members_out.stderr)
    );
    assert!(text(&host_members_out.stdout).contains("\"worker-one\""));
}

#[test]
fn test_cli_enroll_over_p2p_ticket_minimal_preset() {
    let master_home = TempDir::new().unwrap();
    let joiner_home = TempDir::new().unwrap();

    let port_file = master_home.path().join("master.port");
    let ticket_file = master_home.path().join("master.ticket");
    let mut master_cmd = cli(master_home.path());
    master_cmd
        .args(["master", "--listen", "127.0.0.1:0", "--p2p", "--port-file"])
        .arg(&port_file)
        .arg("--p2p-ticket-file")
        .arg(&ticket_file);
    let master = Service::spawn(master_cmd, master_home.path().join("master-p2p.log"));

    let mut ticket = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            ticket = fs::read_to_string(&ticket_file)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            ticket.is_some()
        }),
        "master did not write P2P ticket file:\n{}",
        master.log()
    );
    let ticket = ticket.unwrap();

    // Create a P2P-only invite (`--master <ticket>`) with `--roles worker,client --no-approval`.
    let inv_out = run_cmd(with(
        master_home.path(),
        &[
            "invite",
            "--master",
            &ticket,
            "--roles",
            "worker,client",
            "--no-approval",
        ],
    ));
    assert!(
        inv_out.status.success(),
        "invite over P2P ticket failed:\n{}\n{}",
        text(&inv_out.stdout),
        text(&inv_out.stderr)
    );
    let invite_str = text(&inv_out.stdout).trim().to_string();
    let decoded = InviteToken::decode(&invite_str).unwrap();
    assert!(decoded.master.is_none(), "P2P-only invite has master=None");
    assert_eq!(decoded.ticket.as_deref(), Some(ticket.as_str()));

    // Redeem the P2P invite via `oxideswarm join --no-doctor`.
    let mut join_sess =
        InteractiveJoin::spawn(with(joiner_home.path(), &["join", "--no-doctor"]), &invite_str);
    join_sess.confirm("y");
    let join_out = join_sess.wait_output(Duration::from_secs(20));
    assert!(
        join_out.status.success(),
        "P2P join failed:\nstdout:\n{}\nstderr:\n{}",
        text(&join_out.stdout),
        text(&join_out.stderr)
    );

    let cfg_path = joiner_home
        .path()
        .join(".oxideswarm")
        .join("rusty-grid.toml");
    assert!(cfg_path.is_file());
    let cfg_text = fs::read_to_string(&cfg_path).unwrap();
    assert!(cfg_text.contains("p2p_ticket"));
    assert!(cfg_text.contains("master_key"));

    // Verify the P2P-enrolled machine can query `status` over P2P using its saved config.
    let status_out = run_cmd(with(joiner_home.path(), &["status", "--json"]));
    assert!(
        status_out.status.success(),
        "status over P2P after join failed:\n{}\n{}",
        text(&status_out.stdout),
        text(&status_out.stderr)
    );
}

fn start_master(home: &Path, extra_args: &[&str]) -> (Service, String) {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let port_file = home.join(format!("master-{id}.port"));
    let log_file = home.join(format!("master-{id}.log"));
    let mut master_cmd = cli(home);
    master_cmd
        .args(extra_args)
        .args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&port_file);
    let master = Service::spawn(master_cmd, log_file);
    let mut port = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port.is_some()
        }),
        "master did not start; log:\n{}",
        master.log()
    );
    (master, format!("127.0.0.1:{}", port.unwrap()))
}

#[test]
fn test_secret_only_setups_repro_a_and_b_and_secret_file_invite_persistence() {
    // Repro A: `'secret generate'`, `'master'`, then `'worker --master 127.0.0.1:PORT'` in the
    // same home. The worker must authenticate via v2 `cluster.secret` (not `local-host` admin-only
    // v3) and register cleanly.
    let home_b = TempDir::new().unwrap();
    let gen_out = run_cmd(with(home_b.path(), &["secret", "generate"]));
    assert!(gen_out.status.success());

    let (master_b, addr_b) = start_master(home_b.path(), &[]);
    let mut worker_b_cmd = cli(home_b.path());
    worker_b_cmd
        .args([
            "worker",
            "--name",
            "same-home-worker",
            "--master",
            &addr_b,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(home_b.path().join("sandbox-b"));
    let worker_b = Service::spawn(worker_b_cmd, home_b.path().join("worker-b.log"));

    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                home_b.path(),
                &["workers", "--master", &addr_b, "--json"],
            ));
            out.status.success() && text(&out.stdout).contains("\"same-home-worker\"")
        }),
        "Repro A failed: worker in master's home did not register:\n{}",
        worker_b.log()
    );
    drop(worker_b);
    drop(master_b);

    // Repro B: `home_b` once ran a master (so `home_b/.oxideswarm` has `node.key`,
    // `master_noise.key` for B, `members.json`, and `cluster.secret`). Now `home_b` connects
    // `workers` and `worker` to another master (`home_a`) that shares the same `cluster.secret`.
    let home_a = TempDir::new().unwrap();
    let secret_bytes = fs::read(home_b.path().join(".oxideswarm").join("cluster.secret")).unwrap();
    let secret_file_a = home_a.path().join("shared.secret");
    fs::write(&secret_file_a, &secret_bytes).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&secret_file_a, fs::Permissions::from_mode(0o600)).unwrap();
    }

    // Start Master A with `--secret-file` (and no `--state-dir` or `~/.oxideswarm/cluster.secret`).
    let secret_file_str = secret_file_a.to_str().unwrap();
    let (master_a, addr_a) =
        start_master(home_a.path(), &["--secret-file", secret_file_str]);

    let workers_from_b = run_cmd(with(
        home_b.path(),
        &["workers", "--master", &addr_a, "--json"],
    ));
    assert!(
        workers_from_b.status.success(),
        "Repro B failed: `workers` on host B rejected by Master A:\n{}\n{}",
        text(&workers_from_b.stdout),
        text(&workers_from_b.stderr)
    );

    let mut worker_b_to_a_cmd = cli(home_b.path());
    worker_b_to_a_cmd
        .args([
            "worker",
            "--name",
            "host-b-worker",
            "--master",
            &addr_a,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(home_b.path().join("sandbox-b-to-a"));
    let worker_b_to_a = Service::spawn(worker_b_to_a_cmd, home_b.path().join("worker-b-to-a.log"));
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                home_b.path(),
                &["workers", "--master", &addr_a, "--json"],
            ));
            out.status.success() && text(&out.stdout).contains("\"host-b-worker\"")
        }),
        "Repro B failed: `worker` on host B did not register with Master A:\n{}",
        worker_b_to_a.log()
    );
    drop(worker_b_to_a);

    // Item 2: Master A (started with `--secret-file` and no `--state-dir`) creates an invite,
    // a member joins, Master A restarts with `--secret-file`, and the member still connects with
    // the same pinned `master_key`.
    let inv_out = run_cmd(with(
        home_a.path(),
        &[
            "--secret-file",
            secret_file_str,
            "invite",
            "--master",
            &addr_a,
            "--roles",
            "worker,client",
            "--no-approval",
        ],
    ));
    assert!(
        inv_out.status.success(),
        "invite with --secret-file failed:\n{}\n{}",
        text(&inv_out.stdout),
        text(&inv_out.stderr)
    );
    let invite_str = text(&inv_out.stdout).trim().to_string();

    let member_home = TempDir::new().unwrap();
    let mut join_cmd = with(member_home.path(), &["join", "--no-doctor"]);
    join_cmd.env("RUSTY_GRID_WORKER_NAME", "secret-file-member");
    let mut join_sess = InteractiveJoin::spawn(join_cmd, &invite_str);
    join_sess.confirm("y");
    let join_out = join_sess.wait_output(Duration::from_secs(20));
    assert!(
        join_out.status.success(),
        "join failed:\n{}\n{}",
        text(&join_out.stdout),
        text(&join_out.stderr)
    );

    // Restart Master A with `--secret-file` on a new port; the persisted `master_noise.key` and
    // `members.json` must be loaded so `member_home` connects with its pinned `master_key`.
    drop(master_a);
    let (_master_a2, addr_a2) =
        start_master(home_a.path(), &["--secret-file", secret_file_str]);

    let status_after_restart = run_cmd(with(
        member_home.path(),
        &["status", "--master", &addr_a2, "--json"],
    ));
    assert!(
        status_after_restart.status.success(),
        "Item 2 regression: enrolled member failed to connect after master restart with --secret-file:\n{}\n{}",
        text(&status_after_restart.stdout),
        text(&status_after_restart.stderr)
    );
}
