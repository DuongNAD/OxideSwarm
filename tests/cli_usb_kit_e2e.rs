//! End-to-end tests for USB setup kits (`oxideswarm usb create` and
//! `oxideswarm join --invite-file`, design record `docs/design/0053-usb-setup-kit.md`).
//!
//! Every process runs with isolated `HOME` / `USERPROFILE` / `OXIDESWARM_HOME` directories
//! (never the host user's `~/.oxideswarm`) and a loopback master bound on `127.0.0.1:0`.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ring::digest::{Context, SHA256};
use rusty_grid_cli::usb_kit::archive_filenames;
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

fn preserve_toolchain_env(cmd: &mut Command) {
    let real_home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    if let Some(p) = std::env::var_os("RUSTUP_HOME").or_else(|| {
        real_home
            .as_ref()
            .map(|h| PathBuf::from(h).join(".rustup"))
            .filter(|d| d.is_dir())
            .map(|d| d.into_os_string())
    }) {
        cmd.env("RUSTUP_HOME", p);
    }
    if let Some(p) = std::env::var_os("CARGO_HOME").or_else(|| {
        real_home
            .as_ref()
            .map(|h| PathBuf::from(h).join(".cargo"))
            .filter(|d| d.is_dir())
            .map(|d| d.into_os_string())
    }) {
        cmd.env("CARGO_HOME", p);
    }
}

fn cli(home: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    preserve_toolchain_env(&mut cmd);
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("OXIDESWARM_HOME", home.join(".oxideswarm"))
        .current_dir(home)
        .env_remove("OXIDESWARM_SECRET")
        .env_remove("OXIDESWARM_SECRET_FILE")
        .env_remove("OXIDESWARM_USB_ONLINE_SUMS_URL")
        .env_remove("OXIDESWARM_USB_DOWNLOAD_BASE_URL")
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

fn run_cmd(mut cmd: Command) -> Output {
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run oxideswarm")
}

fn run_cmd_with_stdin(mut cmd: Command, stdin_bytes: &[u8]) -> Output {
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn oxideswarm");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin_bytes)
        .expect("write stdin");
    child.wait_with_output().expect("wait oxideswarm")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

struct Service {
    child: Child,
    log_path: PathBuf,
}

impl Service {
    fn spawn(mut cmd: Command, log_path: PathBuf) -> Self {
        let out = fs::File::create(&log_path).expect("create service log");
        let err = out.try_clone().expect("clone service log handle");
        cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        let child = cmd.spawn().expect("spawn service");
        Self { child, log_path }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct StreamingChild {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl StreamingChild {
    fn spawn(mut cmd: Command) -> Self {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn interactive oxideswarm");
        let stdin = child.stdin.take();
        let out_pipe = child.stdout.take().expect("child stdout");
        let err_pipe = child.stderr.take().expect("child stderr");
        let stdout = Arc::new(Mutex::new(String::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let out_acc = Arc::clone(&stdout);
        let err_acc = Arc::clone(&stderr);
        let stdout_thread = Some(std::thread::spawn(move || {
            read_pipe_chunks(out_pipe, out_acc);
        }));
        let stderr_thread = Some(std::thread::spawn(move || {
            read_pipe_chunks(err_pipe, err_acc);
        }));
        Self {
            child,
            stdin,
            stdout,
            stderr,
            stdout_thread,
            stderr_thread,
        }
    }

    fn stdout_snapshot(&self) -> String {
        self.stdout.lock().unwrap().clone()
    }

    fn stderr_snapshot(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }

    fn write_line(&mut self, line: &str) {
        if let Some(ref mut stdin) = self.stdin {
            writeln!(stdin, "{line}").expect("write stdin line");
            stdin.flush().expect("flush stdin line");
        }
    }

    fn has_exited(&mut self) -> bool {
        self.child.try_wait().expect("try_wait child").is_some()
    }

    fn wait(mut self, timeout: Duration) -> ExitStatus {
        let start = Instant::now();
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("try_wait child") {
                break status;
            }
            if start.elapsed() >= timeout {
                let _ = self.child.kill();
                let status = self.child.wait().expect("wait killed child");
                panic!(
                    "interactive child timed out after {timeout:?} (status={status})\nstdout:\n{}\nstderr:\n{}",
                    self.stdout_snapshot(),
                    self.stderr_snapshot()
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        if let Some(t) = self.stdout_thread.take() {
            let _ = t.join();
        }
        if let Some(t) = self.stderr_thread.take() {
            let _ = t.join();
        }
        status
    }
}

impl Drop for StreamingChild {
    fn drop(&mut self) {
        let _ = self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(t) = self.stdout_thread.take() {
            let _ = t.join();
        }
        if let Some(t) = self.stderr_thread.take() {
            let _ = t.join();
        }
    }
}

fn read_pipe_chunks<R: Read>(mut reader: R, acc: Arc<Mutex<String>>) {
    let mut buf = [0u8; 512];
    loop {
        match reader.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let chunk = String::from_utf8_lossy(&buf[..n]);
                acc.lock().unwrap().push_str(&chunk);
            }
        }
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

fn start_loopback_master(home: &Path) -> (Service, String) {
    let port_file = home.join("master.port");
    let mut master_cmd = cli(home);
    master_cmd
        .args([
            "master",
            "--listen",
            "127.0.0.1:0",
            "--auth",
            "members",
            "--port-file",
        ])
        .arg(&port_file);
    let master = Service::spawn(master_cmd, home.join("master.log"));
    let mut port = None;
    assert!(
        wait_for(Duration::from_secs(25), || {
            port = fs::read_to_string(&port_file)
                .ok()
                .and_then(|p| p.trim().parse::<u16>().ok());
            port.is_some() && master.log().contains("Master v3 public key:")
        }),
        "master did not initialize v3 + port file; log:\n{}",
        master.log()
    );
    let addr = format!("127.0.0.1:{}", port.unwrap());
    (master, addr)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut ctx = Context::new(&SHA256);
    ctx.update(bytes);
    let digest = ctx.finish();
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn create_dummy_archives(dir: &Path, version: &str) -> Vec<String> {
    fs::create_dir_all(dir).expect("create archives dir");
    let names = archive_filenames(version, false);
    let mut sums = String::new();
    for name in &names {
        let payload = format!("dummy-archive-payload-{name}\n").into_bytes();
        let hex = sha256_hex(&payload);
        fs::write(dir.join(name), &payload).expect("write dummy archive");
        sums.push_str(&format!("{hex}  {name}\n"));
    }
    fs::write(dir.join("SHA256SUMS"), &sums).expect("write SHA256SUMS");
    names
}

fn extract_after(haystack: &str, prefix: &str) -> Option<String> {
    let idx = haystack.find(prefix)?;
    let rest = &haystack[idx + prefix.len()..];
    Some(rest.lines().next().unwrap_or_default().trim().to_string())
}

fn extract_kit_passphrase(stdout: &str) -> String {
    let mut lines = stdout.lines();
    while let Some(line) = lines.next() {
        if line.contains("Kit passphrase (write this down") {
            if let Some(next) = lines.next() {
                let phrase = next.trim().to_string();
                assert!(!phrase.is_empty(), "kit passphrase line was empty");
                return phrase;
            }
        }
    }
    panic!("missing kit passphrase in `usb create` output:\n{stdout}");
}

fn assert_dir_absent_needle(dir: &Path, needle: &[u8], label: &str) {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        for entry in fs::read_dir(&cur).expect("read stick dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if entry.file_type().expect("file type").is_dir() {
                stack.push(path);
            } else {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                assert_ne!(
                    name,
                    "cluster.secret",
                    "USB stick must never contain cluster.secret ({})",
                    path.display()
                );
                let data = fs::read(&path).expect("read stick file");
                assert!(
                    !data.windows(needle.len()).any(|w| w == needle),
                    "{label} leaked into USB kit file {}",
                    path.display()
                );
            }
        }
    }
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

fn enroll_via_usb_kit(
    master_home: &Path,
    master_addr: &str,
    invite_oxs: &Path,
    passphrase: &str,
    invite_id: &str,
    joiner_home: &Path,
    label: &str,
) {
    let invite_str = invite_oxs.to_string_lossy().into_owned();
    let mut joiner = StreamingChild::spawn(with(
        joiner_home,
        &["join", "--invite-file", &invite_str, "--no-doctor"],
    ));

    assert!(
        wait_for(Duration::from_secs(15), || {
            joiner.stderr_snapshot().contains("USB kit passphrase:") || joiner.has_exited()
        }),
        "[{label}] join did not prompt for USB kit passphrase\nstdout:\n{}\nstderr:\n{}",
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );
    assert!(
        !joiner.has_exited(),
        "[{label}] join exited unexpectedly before passphrase\nstdout:\n{}\nstderr:\n{}",
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );

    joiner.write_line(passphrase);

    assert!(
        wait_for(Duration::from_secs(20), || {
            let out = joiner.stdout_snapshot();
            (out.contains("Pairing code: ") && out.contains("Confirm that")) || joiner.has_exited()
        }),
        "[{label}] join did not print pairing code & confirmation\nstdout:\n{}\nstderr:\n{}",
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );
    assert!(
        !joiner.has_exited(),
        "[{label}] join exited prematurely after passphrase\nstdout:\n{}\nstderr:\n{}",
        joiner.stdout_snapshot(),
        joiner.stderr_snapshot()
    );

    let join_out = joiner.stdout_snapshot();
    let digits: String = extract_after(&join_out, "Pairing code: ")
        .expect("pairing code on joiner stdout")
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    assert_eq!(
        digits.len(),
        6,
        "[{label}] invalid 6-digit code in:\n{join_out}"
    );

    let mut pending_id: Option<String> = None;
    assert!(
        wait_for(Duration::from_secs(15), || {
            let out = run_cmd(with(
                master_home,
                &["members", "pending", "--master", master_addr, "--json"],
            ));
            if !out.status.success() {
                return false;
            }
            let Ok(list) = serde_json::from_slice::<Vec<serde_json::Value>>(&out.stdout) else {
                return false;
            };
            pending_id = list.iter().find_map(|item| {
                (item.get("invite_id").and_then(|v| v.as_str()) == Some(invite_id))
                    .then(|| {
                        item.get("pending_id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string)
                    })
                    .flatten()
            });
            pending_id.is_some()
        }),
        "[{label}] pending enrollment for invite {invite_id} never appeared"
    );

    let approve_out = run_cmd(with(
        master_home,
        &[
            "members",
            "approve",
            pending_id.as_deref().unwrap(),
            "--code",
            &digits,
            "--master",
            master_addr,
        ],
    ));
    assert!(
        approve_out.status.success(),
        "[{label}] `members approve` failed:\nstdout:\n{}\nstderr:\n{}",
        text(&approve_out.stdout),
        text(&approve_out.stderr)
    );

    joiner.write_line("y");

    let status = joiner.wait(Duration::from_secs(20));
    assert!(status.success(), "[{label}] join failed with {status}");

    let ox_dir = joiner_home.join(".oxideswarm");
    let node_key = ox_dir.join("node.key");
    let config_toml = ox_dir.join("rusty-grid.toml");
    assert!(node_key.is_file(), "[{label}] missing node.key");
    assert_private(&node_key);
    assert!(config_toml.is_file(), "[{label}] missing rusty-grid.toml");
    let cfg_text = fs::read_to_string(&config_toml).expect("read rusty-grid.toml");
    assert!(
        cfg_text.contains("master_key"),
        "[{label}] rusty-grid.toml missing master_key:\n{cfg_text}"
    );
    assert!(
        !ox_dir.join("cluster.secret").exists(),
        "[{label}] v3 USB join must never copy cluster.secret"
    );
}

#[test]
fn usb_kit_create_multi_use_approval_and_exhaustion_e2e() {
    let master_home = TempDir::new().unwrap();
    let archives_dir = TempDir::new().unwrap();
    let stick_dir = TempDir::new().unwrap();
    let bad_pass_home = TempDir::new().unwrap();
    let joiner1_home = TempDir::new().unwrap();
    let joiner2_home = TempDir::new().unwrap();
    let joiner3_home = TempDir::new().unwrap();

    let (_master, master_addr) = start_loopback_master(master_home.path());
    let expected_archives = create_dummy_archives(archives_dir.path(), env!("CARGO_PKG_VERSION"));

    let stick_str = stick_dir.path().to_string_lossy().into_owned();
    let archives_str = archives_dir.path().to_string_lossy().into_owned();
    let create_out = run_cmd(with(
        master_home.path(),
        &[
            "usb",
            "create",
            &stick_str,
            "--master",
            &master_addr,
            "--machines",
            "2",
            "--roles",
            "worker",
            "--archives",
            &archives_str,
        ],
    ));
    assert!(
        create_out.status.success(),
        "`usb create` failed:\nstdout:\n{}\nstderr:\n{}",
        text(&create_out.stdout),
        text(&create_out.stderr)
    );

    let stdout = text(&create_out.stdout);
    let invite_id = extract_after(&stdout, "invite id:").expect("invite id in output");
    assert_eq!(
        invite_id.len(),
        32,
        "expected 32-hex invite id: {invite_id}"
    );
    let passphrase = extract_kit_passphrase(&stdout);

    // Verify full USB stick layout under <dir>/OxideSwarm.
    let kit_dir = stick_dir.path().join("OxideSwarm");
    let stick = kit_dir.as_path();
    for expected in [
        "invite.oxs",
        "kit.json",
        "README.txt",
        "install.sh",
        "uninstall.sh",
        "install.ps1",
        "uninstall.ps1",
        "Install OxideSwarm.cmd",
        "Uninstall OxideSwarm.cmd",
        "Update OxideSwarm.cmd",
        "Install OxideSwarm.command",
        "Uninstall OxideSwarm.command",
        "Update OxideSwarm.command",
        "revoke-isolation-acls.ps1",
        "archives/SHA256SUMS",
    ] {
        assert!(
            stick.join(expected).is_file(),
            "missing kit file {expected}"
        );
    }
    for name in &expected_archives {
        assert!(
            stick.join("archives").join(name).is_file(),
            "missing archived target {name}"
        );
    }

    // Security invariants on stick files.
    assert_dir_absent_needle(stick, passphrase.as_bytes(), "kit passphrase");
    assert_dir_absent_needle(stick, b"oxs1-invite:", "plaintext invite token");
    let kit_json: serde_json::Value =
        serde_json::from_slice(&fs::read(stick.join("kit.json")).unwrap()).expect("parse kit.json");
    assert_eq!(kit_json["kit_format"], 1);
    assert_eq!(kit_json["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(kit_json["machines"], 2);
    assert_eq!(kit_json["roles"], serde_json::json!(["worker"]));
    assert_eq!(kit_json["workspaces"], false);
    assert_eq!(kit_json["gpu"], false);
    assert_eq!(kit_json["require_approval"], true);
    assert_eq!(kit_json["invite_id"], invite_id);

    // Wrong passphrase must fail before talking to master and create nothing.
    let invite_oxs = stick.join("invite.oxs");
    let invite_str = invite_oxs.to_string_lossy().into_owned();
    let bad_out = run_cmd_with_stdin(
        with(
            bad_pass_home.path(),
            &["join", "--invite-file", &invite_str, "--no-doctor"],
        ),
        b"wrong passphrase that must never unlock\n",
    );
    assert!(
        !bad_out.status.success(),
        "wrong passphrase unexpectedly unlocked invite.oxs"
    );
    assert!(
        !bad_pass_home
            .path()
            .join(".oxideswarm")
            .join("node.key")
            .exists(),
        "wrong passphrase must not write node.key"
    );

    // Machines 1 and 2 enroll cleanly with approval.
    enroll_via_usb_kit(
        master_home.path(),
        &master_addr,
        &invite_oxs,
        &passphrase,
        &invite_id,
        joiner1_home.path(),
        "machine-1",
    );
    enroll_via_usb_kit(
        master_home.path(),
        &master_addr,
        &invite_oxs,
        &passphrase,
        &invite_id,
        joiner2_home.path(),
        "machine-2",
    );

    // Confirm both joined members appear with distinct member_ids on master.
    let members_out = run_cmd(with(
        master_home.path(),
        &["members", "list", "--master", &master_addr, "--json"],
    ));
    assert!(
        members_out.status.success(),
        "`members list` failed:\n{}",
        text(&members_out.stderr)
    );
    let members: Vec<serde_json::Value> =
        serde_json::from_slice(&members_out.stdout).expect("parse members list json");
    let via_this_kit: Vec<_> = members
        .iter()
        .filter(|m| m.get("via_invite").and_then(|v| v.as_str()) == Some(invite_id.as_str()))
        .collect();
    assert_eq!(
        via_this_kit.len(),
        2,
        "expected exactly 2 members enrolled via USB kit invite, got {members:#?}"
    );
    assert!(
        via_this_kit[0]["id"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "member entry missing non-empty id: {via_this_kit:#?}"
    );
    assert_ne!(
        via_this_kit[0]["id"], via_this_kit[1]["id"],
        "two enrolled machines must receive distinct member ids"
    );

    // Machine 3 (N+1th) must be rejected because max_uses=2 is exhausted.
    let mut j3 = StreamingChild::spawn(with(
        joiner3_home.path(),
        &["join", "--invite-file", &invite_str, "--no-doctor"],
    ));
    assert!(
        wait_for(Duration::from_secs(15), || {
            j3.stderr_snapshot().contains("USB kit passphrase:") || j3.has_exited()
        }),
        "machine-3 join never reached passphrase prompt"
    );
    j3.write_line(&passphrase);
    let j3_status = j3.wait(Duration::from_secs(15));
    assert!(
        !j3_status.success(),
        "N+1th machine unexpectedly succeeded against max_uses=2 kit"
    );
    assert!(
        !joiner3_home
            .path()
            .join(".oxideswarm")
            .join("node.key")
            .exists(),
        "rejected N+1th machine must not persist node.key"
    );
}

#[test]
fn usb_kit_revoked_invite_rejected_e2e() {
    let master_home = TempDir::new().unwrap();
    let archives_dir = TempDir::new().unwrap();
    let stick_dir = TempDir::new().unwrap();
    let joiner_home = TempDir::new().unwrap();

    let (_master, master_addr) = start_loopback_master(master_home.path());
    create_dummy_archives(archives_dir.path(), env!("CARGO_PKG_VERSION"));

    let custom_pass = "correct horse battery staple";
    let stick_str = stick_dir.path().to_string_lossy().into_owned();
    let archives_str = archives_dir.path().to_string_lossy().into_owned();
    let create_out = run_cmd_with_stdin(
        with(
            master_home.path(),
            &[
                "usb",
                "create",
                &stick_str,
                "--master",
                &master_addr,
                "--machines",
                "1",
                "--roles",
                "client",
                "--archives",
                &archives_str,
                "--passphrase-stdin",
            ],
        ),
        format!("{custom_pass}\n").as_bytes(),
    );
    assert!(
        create_out.status.success(),
        "`usb create --passphrase-stdin` failed:\nstdout:\n{}\nstderr:\n{}",
        text(&create_out.stdout),
        text(&create_out.stderr)
    );
    let stdout = text(&create_out.stdout);
    assert!(
        stdout.contains("passphrase: set from stdin"),
        "expected `--passphrase-stdin` confirmation in:\n{stdout}"
    );
    assert!(
        !stdout.contains(custom_pass),
        "`--passphrase-stdin` must not echo the user passphrase"
    );
    let kit_dir = stick_dir.path().join("OxideSwarm");
    assert!(kit_dir.join("invite.oxs").is_file(), "missing invite.oxs");
    assert_dir_absent_needle(&kit_dir, custom_pass.as_bytes(), "user passphrase");

    let invite_id = extract_after(&stdout, "invite id:").expect("invite id in output");
    let revoke_out = run_cmd(with(
        master_home.path(),
        &["invite", "--revoke", &invite_id, "--master", &master_addr],
    ));
    assert!(
        revoke_out.status.success(),
        "`invite --revoke` failed:\nstdout:\n{}\nstderr:\n{}",
        text(&revoke_out.stdout),
        text(&revoke_out.stderr)
    );

    let invite_str = kit_dir.join("invite.oxs").to_string_lossy().into_owned();
    let join_out = run_cmd_with_stdin(
        with(
            joiner_home.path(),
            &["join", "--invite-file", &invite_str, "--no-doctor"],
        ),
        format!("{custom_pass}\n").as_bytes(),
    );
    assert!(
        !join_out.status.success(),
        "revoked USB kit invite unexpectedly succeeded"
    );
    assert!(
        !joiner_home
            .path()
            .join(".oxideswarm")
            .join("node.key")
            .exists(),
        "revoked USB kit must not persist node.key"
    );
}
