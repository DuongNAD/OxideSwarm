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
        &format!("{}\ny\n", expired_token.encode()),
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

    // Create a valid 1-use worker invite and decline the [y/N] confirmation.
    let inv_decline_out = run_cmd(with(master_home.path(), &["invite", "--master", &addr]));
    assert!(
        inv_decline_out.status.success(),
        "invite failed: {}",
        text(&inv_decline_out.stderr)
    );
    let inv_decline = text(&inv_decline_out.stdout).trim().to_string();
    let dec_out = run_with_stdin(
        with(declined_home.path(), &["join"]),
        &format!("{inv_decline}\nn\n"),
    );
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
    let mut drop_join_cmd = with(drop_home.path(), &["join"]);
    drop_join_cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut drop_child = drop_join_cmd.spawn().unwrap();
    {
        let stdin = drop_child.stdin.as_mut().unwrap();
        stdin
            .write_all(format!("{inv_decline}\n").as_bytes())
            .unwrap();
        stdin.flush().unwrap();
    }
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
    let _ = drop_child.kill();
    let _ = drop_child.wait();
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
    join_1_cmd
        .env("RUSTY_GRID_WORKER_NAME", "worker-one")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut join_1_child = join_1_cmd.spawn().unwrap();
    {
        let mut stdin = join_1_child.stdin.take().unwrap();
        stdin
            .write_all(format!("{inv_decline}\ny\n").as_bytes())
            .unwrap();
    }

    // Wait for `worker-one` to appear in `members pending`, then approve it.
    let mut pending_1_id = String::new();
    let mut pending_1_code = String::new();
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
                !pending_1_id.is_empty()
            } else {
                false
            }
        }),
        "worker-one did not appear in members pending"
    );
    assert_eq!(pending_1_code.len(), 6, "6-digit pairing code");

    let approve_1 = run_cmd(with(
        master_home.path(),
        &["members", "approve", &pending_1_id, "--master", &addr],
    ));
    assert!(
        approve_1.status.success(),
        "members approve failed: {}",
        text(&approve_1.stderr)
    );

    let join_1_out = join_1_child.wait_with_output().unwrap();
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
    join_a_cmd
        .env("RUSTY_GRID_WORKER_NAME", "contender-a")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut join_a_child = join_a_cmd.spawn().unwrap();
    join_a_child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{shared_inv}\ny\n").as_bytes())
        .unwrap();

    let mut join_b_cmd = with(contender_b_home.path(), &["join", "--no-doctor"]);
    join_b_cmd
        .env("RUSTY_GRID_WORKER_NAME", "contender-b")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut join_b_child = join_b_cmd.spawn().unwrap();
    join_b_child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{shared_inv}\ny\n").as_bytes())
        .unwrap();

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

    // Reject contender-a, approve contender-b.
    let rej_a = run_cmd(with(
        master_home.path(),
        &["members", "reject", &pending_a_id, "--master", &addr],
    ));
    assert!(rej_a.status.success(), "{}", text(&rej_a.stderr));

    let out_a = join_a_child.wait_with_output().unwrap();
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
        &["members", "approve", &pending_b_id, "--master", &addr],
    ));
    assert!(app_b.status.success(), "{}", text(&app_b.stderr));

    let out_b = join_b_child.wait_with_output().unwrap();
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
    let join_w2 = run_with_stdin(
        with(worker_2_home.path(), &["join", "--no-doctor"]),
        &format!("{inv_w2}\ny\n"),
    );
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
    let join_out = run_with_stdin(
        with(joiner_home.path(), &["join", "--no-doctor"]),
        &format!("{invite_str}\ny\n"),
    );
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
