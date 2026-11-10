//! End-to-end CLI integration tests for OXS-AUTH v3 invites, enrollment, pairing check,
//! pending request lifecycle, worker-id ownership, roles, P2P ticket invites, and host admin
//! enrolment (`ENROLL-1b`, design record `docs/design/0051-machine-identity-and-invites.md`).

use std::fs;
#[cfg(unix)]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use rusty_grid_cli::members_cmd::InviteToken;
#[cfg(unix)]
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

#[cfg(unix)]
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
            let out = self.stdout();
            out.contains(
                "Confirm that the cluster admin's master shows this pairing code and fingerprint [y/N]:",
            ) || out.contains("matches the cluster you intend to join [y/N]:")
        })
    }

    fn pairing_code(&self) -> String {
        assert!(
            self.wait_for_pairing_prompt(Duration::from_secs(15)),
            "join did not print pairing code before pairing_code();\nstdout:\n{}\nstderr:\n{}",
            self.stdout(),
            self.stderr()
        );
        let out = self.stdout();
        let after = out
            .split_once("Pairing code: ")
            .expect("stdout contains Pairing code:")
            .1;
        let code: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        assert_eq!(code.len(), 6, "expected 6-digit pairing code in:\n{out}");
        code
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

#[cfg(unix)]
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
    let master_exp_out = InteractiveJoin::spawn(
        with(expired_home.path(), &["join"]),
        &skewed_expired.encode(),
    )
    .wait_output(Duration::from_secs(10));
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

    // Item 9 & N12: piping both the invite and `y` on stdin (`printf '%s\ny\n' "$INVITE" | oxideswarm join`)
    // is rejected with a specific multi-line error before connecting or writing anything.
    let prestuffed_out = run_with_stdin(
        with(declined_home.path(), &["join"]),
        &format!("{inv_decline}\ny\n"),
    );
    assert!(
        !prestuffed_out.status.success(),
        "pre-stuffed multi-line piped stdin must be rejected"
    );
    assert!(
        text(&prestuffed_out.stderr).contains("multiple lines"),
        "expected multi-line input error (N12), got: {}",
        text(&prestuffed_out.stderr)
    );
    assert!(!declined_home.path().join(".oxideswarm").exists());

    // N3(a): Declining `[y/N]` on a 1-use `--no-approval` invite neither enrolls the machine nor
    // consumes the invite's use on the master.
    let inv_no_app_dec_out = run_cmd(with(
        master_home.path(),
        &["invite", "--master", &addr, "--no-approval", "--uses", "1"],
    ));
    assert!(inv_no_app_dec_out.status.success());
    let inv_no_app_dec = text(&inv_no_app_dec_out.stdout).trim().to_string();
    let inv_no_app_dec_id = InviteToken::decode(&inv_no_app_dec).unwrap().invite_id;

    let mut dec_no_app =
        InteractiveJoin::spawn(with(declined_home.path(), &["join"]), &inv_no_app_dec);
    dec_no_app.confirm("n");
    let dec_no_app_out = dec_no_app.wait_output(Duration::from_secs(10));
    assert!(
        !dec_no_app_out.status.success(),
        "declining confirmation on --no-approval invite must fail"
    );
    assert!(!declined_home.path().join(".oxideswarm").exists());

    let members_after_no_app_dec = run_cmd(with(
        master_home.path(),
        &["members", "list", "--master", &addr, "--json"],
    ));
    let members_json_no_app: serde_json::Value =
        serde_json::from_slice(&members_after_no_app_dec.stdout).unwrap();
    assert_eq!(
        members_json_no_app.as_array().unwrap().len(),
        1,
        "N3 regression: declining --no-approval invite must not enroll the member on the master: {members_json_no_app}"
    );
    let inv_list_no_app = run_cmd(with(
        master_home.path(),
        &["invite", "--list", "--master", &addr, "--json"],
    ));
    let inv_list_no_app_json: serde_json::Value =
        serde_json::from_slice(&inv_list_no_app.stdout).unwrap();
    let no_app_entry = inv_list_no_app_json
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["invite_id"].as_str() == Some(&inv_no_app_dec_id))
        .expect("--no-approval invite must still be active");
    assert_eq!(
        no_app_entry["uses_remaining"].as_u64(),
        Some(1),
        "N3 regression: declining --no-approval invite must leave uses_remaining == 1"
    );

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

    // N3(b): Admin runs `members approve` FIRST while the joining machine is at `[y/N]`, and the
    // joining machine THEN declines `n`. The master must roll back the enrollment and restore
    // `uses_remaining == 1` on `inv_decline`.
    let mut dec_after_approve =
        InteractiveJoin::spawn(with(declined_home.path(), &["join"]), &inv_decline);
    let dec_after_code = dec_after_approve.pairing_code();
    let mut dec_pending_id = String::new();
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
            if let Some(item) = v.as_array().and_then(|a| a.first()) {
                dec_pending_id = item["pending_id"].as_str().unwrap_or_default().to_string();
                !dec_pending_id.is_empty()
            } else {
                false
            }
        }),
        "dec_after_approve did not appear in members pending"
    );
    let approve_then_decline = run_cmd(with(
        master_home.path(),
        &[
            "members",
            "approve",
            &dec_pending_id,
            "--code",
            &dec_after_code,
            "--fingerprint",
            &master_fp,
            "--master",
            &addr,
        ],
    ));
    assert!(
        approve_then_decline.status.success(),
        "approve before decline failed: {}",
        text(&approve_then_decline.stderr)
    );
    std::thread::sleep(Duration::from_millis(150));
    dec_after_approve.confirm("n");
    let dec_after_out = dec_after_approve.wait_output(Duration::from_secs(10));
    assert!(
        !dec_after_out.status.success(),
        "declining after approval must fail"
    );
    assert!(!declined_home.path().join(".oxideswarm").exists());

    let members_after_rollback = run_cmd(with(
        master_home.path(),
        &["members", "list", "--master", &addr, "--json"],
    ));
    let members_rollback_json: serde_json::Value =
        serde_json::from_slice(&members_after_rollback.stdout).unwrap();
    assert_eq!(
        members_rollback_json.as_array().unwrap().len(),
        1,
        "N3 regression: declining after admin approval must roll back the member on the master: {members_rollback_json}"
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
    let pending_1_code = join_1.pairing_code();

    // Wait for `worker-one` to appear in `members pending`, then approve it.
    // N4: `members pending` (both `--json` and human output) must NOT expose `pairing_code`!
    let mut pending_1_id = String::new();
    let mut pending_1_fp = String::new();
    let mut pending_1_remote = String::new();
    let mut pending_1_raw_json = String::new();
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home.path(),
                &["members", "pending", "--master", &addr, "--json"],
            ));
            if !out.status.success() {
                return false;
            }
            pending_1_raw_json = text(&out.stdout);
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
            if let Some(item) = v.as_array().and_then(|a| a.first()) {
                pending_1_id = item["pending_id"].as_str().unwrap_or_default().to_string();
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
    assert!(
        !pending_1_raw_json.contains("pairing_code"),
        "N4 regression: members pending --json must not expose pairing_code:\n{pending_1_raw_json}"
    );
    let pending_1_human = run_cmd(with(
        master_home.path(),
        &["members", "pending", "--master", &addr],
    ));
    assert!(
        !text(&pending_1_human.stdout).contains("code="),
        "N4 regression: members pending human output must not expose pairing code:\n{}",
        text(&pending_1_human.stdout)
    );
    assert_eq!(pending_1_code.len(), 6, "6-digit pairing code");
    assert_eq!(
        pending_1_fp, master_fp,
        "pending entry carries master_fingerprint"
    );
    assert!(
        pending_1_remote.starts_with("127.0.0.1:"),
        "pending entry carries remote_addr: {pending_1_remote}"
    );

    // Item 3 & N4: wrong `--code` or wrong `--fingerprint` is refused and leaves the pending
    // request and invite use intact.
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

    let wrong_fp = if master_fp == "0000-0000-0000-0000" {
        "ffff-ffff-ffff-ffff"
    } else {
        "0000-0000-0000-0000"
    };
    let bad_fp_approve = run_cmd(with(
        master_home.path(),
        &[
            "members",
            "approve",
            &pending_1_id,
            "--code",
            &pending_1_code,
            "--fingerprint",
            wrong_fp,
            "--master",
            &addr,
        ],
    ));
    assert!(
        !bad_fp_approve.status.success(),
        "members approve with wrong --fingerprint must fail"
    );
    assert!(
        text(&bad_fp_approve.stderr).contains("master key fingerprint mismatch"),
        "expected fingerprint mismatch error, got: {}",
        text(&bad_fp_approve.stderr)
    );

    let approve_1 = run_cmd(with(
        master_home.path(),
        &[
            "members",
            "approve",
            &pending_1_id,
            "--code",
            &pending_1_code,
            "--fingerprint",
            &master_fp,
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
    let pending_b_code = join_b.pairing_code();

    let mut pending_a_id = String::new();
    let mut pending_b_id = String::new();
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
    let mut join_w2_sess = InteractiveJoin::spawn(
        with(worker_2_home.path(), &["join", "--no-doctor"]),
        &inv_w2,
    );
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

#[cfg(unix)]
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
    let mut join_sess = InteractiveJoin::spawn(
        with(joiner_home.path(), &["join", "--no-doctor"]),
        &invite_str,
    );
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
        .args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&port_file)
        .args(extra_args);
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

    // N10: Create an invite on `master_b` so `home_b/.oxideswarm` genuinely holds `node.key`,
    // `master_noise.key`, and `members.json` (in addition to `cluster.secret`) before Repro B
    // runs `workers` and `worker` from `home_b` against `master_a`.
    let inv_b_out = run_cmd(with(
        home_b.path(),
        &["invite", "--master", &addr_b, "--no-approval"],
    ));
    assert!(
        inv_b_out.status.success(),
        "invite on master_b failed:\n{}\n{}",
        text(&inv_b_out.stdout),
        text(&inv_b_out.stderr)
    );
    let home_b_oxs = home_b.path().join(".oxideswarm");
    assert!(home_b_oxs.join("node.key").is_file(), "home_b node.key");
    assert!(
        home_b_oxs.join("master_noise.key").is_file(),
        "home_b master_noise.key"
    );
    assert!(
        home_b_oxs.join("members.json").is_file(),
        "home_b members.json"
    );
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
    let (master_a, addr_a) = start_master(home_a.path(), &["--secret-file", secret_file_str]);

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

    // N1(b): Before `CreateInvite` is called, a `--secret-file` master must NOT have created
    // `members.json`, `master_noise.key`, or `node.key` when a v2 worker registered.
    assert!(
        !home_a.path().join(".oxideswarm").join("members.json").exists(),
        "N1(b) regression: v2 Register on --secret-file master created members.json before invite"
    );
    assert!(
        !home_a
            .path()
            .join(".oxideswarm")
            .join("master_noise.key")
            .exists(),
        "N1(b) regression: master_noise.key created before invite"
    );
    assert!(
        !home_a.path().join(".oxideswarm").join("node.key").exists(),
        "N1(b) regression: node.key created before invite"
    );

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
    let decoded_before = InviteToken::decode(&invite_str).expect("decode invite before restart");

    let master_a_noise = home_a.path().join(".oxideswarm").join("master_noise.key");
    let master_a_members = home_a.path().join(".oxideswarm").join("members.json");
    assert!(
        master_a_noise.is_file(),
        "master_noise.key persisted by invite"
    );
    assert!(
        master_a_members.is_file(),
        "members.json persisted by invite"
    );
    assert_private(&master_a_noise);
    assert_private(&master_a_members);

    #[cfg(unix)]
    let member_home = {
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
        member_home
    };

    // Restart Master A with `--secret-file` on a new port; the persisted `master_noise.key` and
    // `members.json` must be loaded so its `master_key` stays stable and `member_home` connects
    // with its pinned `master_key`.
    drop(master_a);
    let (_master_a2, addr_a2) = start_master(home_a.path(), &["--secret-file", secret_file_str]);

    let inv_after_out = run_cmd(with(
        home_a.path(),
        &[
            "--secret-file",
            secret_file_str,
            "invite",
            "--master",
            &addr_a2,
            "--no-approval",
        ],
    ));
    assert!(
        inv_after_out.status.success(),
        "invite after master restart failed:\n{}\n{}",
        text(&inv_after_out.stdout),
        text(&inv_after_out.stderr)
    );
    let decoded_after = InviteToken::decode(text(&inv_after_out.stdout).trim())
        .expect("decode invite after restart");
    assert_eq!(
        decoded_after.master_key, decoded_before.master_key,
        "Item 2 regression: master_key changed across master restart with --secret-file"
    );

    #[cfg(unix)]
    {
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
}

#[cfg(unix)]
#[test]
fn test_round3_two_unnamed_workers_slow_no_approval_confirm_and_paste_no_autoconfirm() {
    use std::io::Write;

    let master_home = TempDir::new().unwrap();
    assert!(run_cmd(with(master_home.path(), &["secret", "generate"]))
        .status
        .success());
    let (_master, addr) = start_master(master_home.path(), &[]);

    let inv_out = run_cmd(with(
        master_home.path(),
        &["invite", "--master", &addr, "--no-approval", "--uses", "5"],
    ));
    assert!(inv_out.status.success(), "{}", text(&inv_out.stderr));
    let invite_str = text(&inv_out.stdout).trim().to_string();

    // 1. Item 4: Pasting `<invite>\ny\n` in a single write at the hidden token prompt must NOT
    //    auto-confirm the `[y/N]` prompt.
    let paste_home = TempDir::new().unwrap();
    let mut paste_sess = InteractiveJoin::spawn(
        with(paste_home.path(), &["join", "--no-doctor"]),
        &format!("{invite_str}\ny"),
    );
    assert!(
        paste_sess.wait_for_pairing_prompt(Duration::from_secs(10)),
        "paste_sess did not reach [y/N] prompt:\nstdout:\n{}\nstderr:\n{}",
        paste_sess.stdout(),
        paste_sess.stderr()
    );
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        paste_sess.child.try_wait().unwrap().is_none(),
        "Item 4 regression: pasting `<invite>\\ny\\n` auto-confirmed [y/N] without waiting for user input!"
    );
    paste_sess
        .pty_master
        .write_all(b"n\n")
        .expect("decline after paste");
    let _ = paste_sess.pty_master.flush();
    let paste_out = paste_sess.wait_output(Duration::from_secs(10));
    assert!(!paste_out.status.success(), "declined join must fail");
    assert!(!paste_home.path().join(".oxideswarm").exists());

    // 2. Item 3: Answering `y` after 8 seconds on a `--no-approval` invite (> 5 s handshake
    //    watchdog) must succeed because `ClientMessage::Hello` -> `ClientResponse::Hello`
    //    completes before the `[y/N]` prompt.
    let joiner_home = TempDir::new().unwrap();
    let mut slow_sess = InteractiveJoin::spawn(
        with(joiner_home.path(), &["join", "--no-doctor"]),
        &invite_str,
    );
    assert!(
        slow_sess.wait_for_pairing_prompt(Duration::from_secs(10)),
        "slow_sess did not reach [y/N] prompt"
    );
    std::thread::sleep(Duration::from_secs(8));
    slow_sess.confirm("y");
    let slow_out = slow_sess.wait_output(Duration::from_secs(15));
    assert!(
        slow_out.status.success(),
        "Item 3 regression: answering `y` after 8s on --no-approval invite failed:\nstdout:\n{}\nstderr:\n{}",
        text(&slow_out.stdout),
        text(&slow_out.stderr)
    );
    assert!(
        !joiner_home
            .path()
            .join(".oxideswarm")
            .join("worker_id")
            .exists(),
        "Item 2 regression: `join` must not write an unnamed ~/.oxideswarm/worker_id file"
    );

    // 3. Item 2: Two unnamed workers in `joiner_home` (no `--name`, no `RUSTY_GRID_WORKER_NAME`)
    //    get distinct ephemeral UUIDs and both stay registered simultaneously without superseding
    //    each other.
    let mut u1_cmd = cli(joiner_home.path());
    u1_cmd
        .args([
            "worker",
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(joiner_home.path().join("sb1"));
    let u1 = Service::spawn(u1_cmd, joiner_home.path().join("u1.log"));

    let mut u2_cmd = cli(joiner_home.path());
    u2_cmd
        .args([
            "worker",
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(joiner_home.path().join("sb2"));
    let u2 = Service::spawn(u2_cmd, joiner_home.path().join("u2.log"));

    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--master", &addr, "--json"],
            ));
            if !out.status.success() {
                return false;
            }
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
            v.as_array().is_some_and(|arr| arr.len() == 2)
        }),
        "Item 2 regression: two unnamed workers in the same HOME did not both stay registered:\nu1:\n{}\nu2:\n{}",
        u1.log(),
        u2.log()
    );
    assert!(
        !joiner_home
            .path()
            .join(".oxideswarm")
            .join("worker_id")
            .exists(),
        "Item 2 regression: unnamed `worker` must not write ~/.oxideswarm/worker_id"
    );
}

#[cfg(unix)]
#[test]
fn test_n1_readonly_home_secret_only_master_and_n2_d6_named_worker_v2_to_v3_and_p2p_reclaim() {
    use std::os::unix::fs::PermissionsExt;

    let work_dir = TempDir::new().unwrap();
    let secret_path = work_dir.path().join("cluster.secret");
    fs::write(
        &secret_path,
        b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
    )
    .unwrap();
    fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600)).unwrap();
    let secret_str = secret_path.to_str().unwrap();

    // -------------------------------------------------------------------------
    // 1. N1(a)/(b): Secret-only master (`--auth secret --secret-file X`) with a
    //    read-only HOME / OXIDESWARM_HOME (0o500) accepts v2 workers, runs a
    //    submitted task, and creates no `members.json`, `master_noise.key`, or
    //    `node.key`.
    // -------------------------------------------------------------------------
    let ro_home = TempDir::new().unwrap();
    let ro_oxs = ro_home.path().join(".oxideswarm");
    fs::create_dir_all(&ro_oxs).unwrap();
    fs::set_permissions(&ro_oxs, fs::Permissions::from_mode(0o500)).unwrap();
    fs::set_permissions(ro_home.path(), fs::Permissions::from_mode(0o500)).unwrap();

    let port_file_ro = work_dir.path().join("ro-master.port");
    let log_file_ro = work_dir.path().join("ro-master.log");
    let mut ro_master_cmd = cli(ro_home.path());
    ro_master_cmd
        .args([
            "--secret-file",
            secret_str,
            "master",
            "--listen",
            "127.0.0.1:0",
            "--auth",
            "secret",
            "--port-file",
        ])
        .arg(&port_file_ro);
    let ro_master = Service::spawn(ro_master_cmd, log_file_ro);
    let mut ro_port = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            ro_port = fs::read_to_string(&port_file_ro)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            ro_port.is_some()
        }),
        "read-only HOME secret-only master did not start:\n{}",
        ro_master.log()
    );
    let ro_addr = format!("127.0.0.1:{}", ro_port.unwrap());

    let v2_worker_home = TempDir::new().unwrap();
    let mut ro_worker_cmd = cli(v2_worker_home.path());
    ro_worker_cmd
        .args([
            "--secret-file",
            secret_str,
            "worker",
            "--name",
            "ro-v2-worker",
            "--master",
            &ro_addr,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(work_dir.path().join("sb-ro"));
    let ro_worker = Service::spawn(ro_worker_cmd, work_dir.path().join("ro-worker.log"));
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                v2_worker_home.path(),
                &[
                    "--secret-file",
                    secret_str,
                    "workers",
                    "--master",
                    &ro_addr,
                    "--json",
                ],
            ));
            out.status.success() && text(&out.stdout).contains("\"ro-v2-worker\"")
        }),
        "N1(a) regression: v2 worker rejected by secret-only master with read-only HOME:\nmaster:\n{}\nworker:\n{}",
        ro_master.log(),
        ro_worker.log()
    );
    let submit_out = run_cmd(with(
        v2_worker_home.path(),
        &[
            "--secret-file",
            secret_str,
            "submit",
            "--master",
            &ro_addr,
            "--wait",
            "--",
            "echo",
            "ro-ok",
        ],
    ));
    assert!(
        submit_out.status.success() && text(&submit_out.stdout).contains("ro-ok"),
        "task on secret-only master failed:\n{}\n{}",
        text(&submit_out.stdout),
        text(&submit_out.stderr)
    );
    assert!(!ro_oxs.join("members.json").exists());
    assert!(!ro_oxs.join("master_noise.key").exists());
    assert!(!ro_oxs.join("node.key").exists());
    drop(ro_worker);
    drop(ro_master);
    // Restore permissions so TempDir cleanup succeeds.
    let _ = fs::set_permissions(ro_home.path(), fs::Permissions::from_mode(0o700));
    let _ = fs::set_permissions(&ro_oxs, fs::Permissions::from_mode(0o700));

    // -------------------------------------------------------------------------
    // 2. N2 (D6 end-to-end) + P2P v2 reclaim:
    //    Start a master with `--p2p` (`--auth both`), register two named v2
    //    workers (`gpu1` and `gpu2`), stop both, then join the worker machine
    //    with a `--no-approval` invite.
    //    - In `--auth both`: `gpu1` is refused with the `oxideswarm members
    //      release-worker <id>` hint; after running `release-worker`, `gpu1`
    //      registers as the enrolled v3 member with the same persisted
    //      `worker_id.gpu1`.
    //    - Over P2P (`--master <ticket>`): a v2 worker with the cluster secret
    //      reclaims a non-live v3 worker_id over QUIC/P2P.
    //    - In `--auth members`: restarting the master with `--auth members`
    //      lets `gpu2` register as the enrolled v3 member automatically without
    //      needing `release-worker`.
    // -------------------------------------------------------------------------
    let master_home = TempDir::new().unwrap();
    let gen_out = run_cmd(with(master_home.path(), &["secret", "generate"]));
    assert!(gen_out.status.success());

    let port_file = master_home.path().join("master.port");
    let ticket_file = master_home.path().join("master.ticket");
    let mut master_cmd = cli(master_home.path());
    master_cmd
        .args(["master", "--listen", "127.0.0.1:0", "--p2p", "--port-file"])
        .arg(&port_file)
        .arg("--p2p-ticket-file")
        .arg(&ticket_file);
    let master = Service::spawn(master_cmd, master_home.path().join("master.log"));
    let mut port = None;
    let mut ticket = None;
    assert!(
        wait_for(Duration::from_secs(20), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            ticket = fs::read_to_string(&ticket_file)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            port.is_some() && ticket.is_some()
        }),
        "master did not start:\n{}",
        master.log()
    );
    let addr = format!("127.0.0.1:{}", port.unwrap());
    let ticket = ticket.unwrap();

    let machine_home = TempDir::new().unwrap();
    let machine_oxs = machine_home.path().join(".oxideswarm");
    fs::create_dir_all(&machine_oxs).unwrap();
    fs::set_permissions(&machine_oxs, fs::Permissions::from_mode(0o700)).unwrap();
    let master_secret_path = master_home.path().join(".oxideswarm").join("cluster.secret");
    let machine_secret_file = work_dir.path().join("machine-v2.secret");
    fs::copy(&master_secret_path, &machine_secret_file).unwrap();
    fs::set_permissions(&machine_secret_file, fs::Permissions::from_mode(0o600)).unwrap();
    let machine_secret_str = machine_secret_file.to_str().unwrap();

    // Register `gpu1` and `gpu2` as v2 workers using `--secret-file`.
    for name in ["gpu1", "gpu2"] {
        let mut w_cmd = cli(machine_home.path());
        w_cmd
            .args([
                "--secret-file",
                machine_secret_str,
                "worker",
                "--name",
                name,
                "--master",
                &addr,
                "--cores",
                "1",
                "--no-portal",
                "--sandbox-base-dir",
            ])
            .arg(work_dir.path().join(format!("sb-v2-{name}")));
        let w = Service::spawn(w_cmd, work_dir.path().join(format!("w-v2-{name}.log")));
        assert!(
            wait_for(Duration::from_secs(15), || {
                let out = run_cmd(with(
                    master_home.path(),
                    &["workers", "--master", &addr, "--json"],
                ));
                out.status.success() && text(&out.stdout).contains(&format!("\"{name}\""))
            }),
            "v2 worker {name} did not register:\n{}",
            w.log()
        );
        drop(w);
    }

    let gpu1_id = fs::read_to_string(machine_oxs.join("worker_id.gpu1"))
        .unwrap()
        .trim()
        .to_string();
    let gpu2_id = fs::read_to_string(machine_oxs.join("worker_id.gpu2"))
        .unwrap()
        .trim()
        .to_string();

    // Machine joins via `--no-approval` invite; `worker_id.gpu1` and `worker_id.gpu2` are kept.
    let inv_out = run_cmd(with(
        master_home.path(),
        &["invite", "--master", &addr, "--no-approval"],
    ));
    assert!(inv_out.status.success());
    let invite_str = text(&inv_out.stdout).trim().to_string();

    let mut join_sess = InteractiveJoin::spawn(
        with(machine_home.path(), &["join", "--no-doctor"]),
        &invite_str,
    );
    join_sess.confirm("y");
    let join_out = join_sess.wait_output(Duration::from_secs(20));
    assert!(
        join_out.status.success(),
        "join failed:\n{}\n{}",
        text(&join_out.stdout),
        text(&join_out.stderr)
    );
    assert_eq!(
        fs::read_to_string(machine_oxs.join("worker_id.gpu1"))
            .unwrap()
            .trim(),
        gpu1_id
    );

    // Under `--auth both`, restarting `worker --name gpu1` as v3 member is refused with the
    // `oxideswarm members release-worker <gpu1_id>` hint.
    let mut gpu1_v3_refused_cmd = cli(machine_home.path());
    gpu1_v3_refused_cmd
        .args([
            "worker",
            "--name",
            "gpu1",
            "--master",
            &addr,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(work_dir.path().join("sb-v3-gpu1-refused"));
    let gpu1_v3_refused = Service::spawn(
        gpu1_v3_refused_cmd,
        work_dir.path().join("w-v3-gpu1-refused.log"),
    );
    assert!(
        wait_for(Duration::from_secs(15), || {
            gpu1_v3_refused
                .log()
                .contains(&format!("oxideswarm members release-worker {gpu1_id}"))
        }),
        "expected release-worker hint in worker log:\n{}",
        gpu1_v3_refused.log()
    );
    drop(gpu1_v3_refused);

    // Admin releases `gpu1_id` via `oxideswarm members release-worker <gpu1_id>`.
    let release_out = run_cmd(with(
        master_home.path(),
        &["members", "release-worker", &gpu1_id, "--master", &addr],
    ));
    assert!(
        release_out.status.success(),
        "release-worker failed:\n{}\n{}",
        text(&release_out.stdout),
        text(&release_out.stderr)
    );
    assert!(text(&release_out.stdout).contains(&gpu1_id));

    // Now `worker --name gpu1` registers as the enrolled v3 member with the same `gpu1_id`!
    let mut gpu1_v3_ok_cmd = cli(machine_home.path());
    gpu1_v3_ok_cmd
        .args([
            "worker",
            "--name",
            "gpu1",
            "--master",
            &addr,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(work_dir.path().join("sb-v3-gpu1-ok"));
    let gpu1_v3_ok = Service::spawn(gpu1_v3_ok_cmd, work_dir.path().join("w-v3-gpu1-ok.log"));
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--master", &addr, "--json"],
            ));
            out.status.success()
                && text(&out.stdout).contains("\"gpu1\"")
                && text(&out.stdout).contains(&gpu1_id)
        }),
        "gpu1 did not register as v3 member after release-worker:\n{}",
        gpu1_v3_ok.log()
    );
    drop(gpu1_v3_ok);

    // Verify a v2 worker over P2P (`--master <ticket>`) can reclaim `gpu1_id` once `gpu1_v3_ok`
    // is stopped!
    let mut gpu1_p2p_v2_cmd = cli(v2_worker_home.path());
    fs::create_dir_all(v2_worker_home.path().join(".oxideswarm")).unwrap();
    fs::write(
        v2_worker_home
            .path()
            .join(".oxideswarm")
            .join("worker_id.gpu1"),
        format!("{gpu1_id}\n"),
    )
    .unwrap();
    gpu1_p2p_v2_cmd
        .args([
            "--secret-file",
            machine_secret_str,
            "worker",
            "--name",
            "gpu1",
            "--master",
            &ticket,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(work_dir.path().join("sb-p2p-v2-reclaim"));
    let gpu1_p2p_v2 = Service::spawn(
        gpu1_p2p_v2_cmd,
        work_dir.path().join("w-p2p-v2-reclaim.log"),
    );
    assert!(
        wait_for(Duration::from_secs(20), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--master", &addr, "--json"],
            ));
            out.status.success()
                && text(&out.stdout).contains("\"gpu1\"")
                && text(&out.stdout).contains(&gpu1_id)
        }),
        "v2 worker over P2P ticket did not reclaim gpu1_id:\n{}",
        gpu1_p2p_v2.log()
    );
    drop(gpu1_p2p_v2);
    drop(master);

    // Restart the master with `--auth members`; `gpu2` (which is still in `v2_worker_seq` in
    // `members.json`) must be allowed to register as the enrolled v3 member automatically without
    // needing `release-worker`.
    let (_master_mem, addr_mem) = start_master(master_home.path(), &["--auth", "members"]);
    let mut gpu2_v3_cmd = cli(machine_home.path());
    gpu2_v3_cmd
        .args([
            "worker",
            "--name",
            "gpu2",
            "--master",
            &addr_mem,
            "--cores",
            "1",
            "--no-portal",
            "--sandbox-base-dir",
        ])
        .arg(work_dir.path().join("sb-v3-gpu2-members"));
    let gpu2_v3 = Service::spawn(gpu2_v3_cmd, work_dir.path().join("w-v3-gpu2-members.log"));
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home.path(),
                &["workers", "--master", &addr_mem, "--json"],
            ));
            out.status.success()
                && text(&out.stdout).contains("\"gpu2\"")
                && text(&out.stdout).contains(&gpu2_id)
        }),
        "N2 regression: under --auth members, v3 member gpu2 could not register its persisted worker_id:\n{}",
        gpu2_v3.log()
    );
}
