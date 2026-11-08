//! End-to-end test of joining a machine with one token (JOIN-1, design record
//! `docs/design/0048-join-token.md`), driving the real `rusty-grid` binary as separate processes.
//!
//! Two temporary homes stand for two machines. Every process gets its own `HOME` / `USERPROFILE`
//! through `Command::env` (never the developer's `~/.oxideswarm`), and the master listens on
//! `127.0.0.1:0`. The secrets are generated here, so the test can assert that they appear in no
//! output of any process.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rusty-grid");
const PREFIX: &str = "oxs1-join:";

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
        .env_remove("RUSTY_GRID_P2P_TICKET")
        .env("RUSTY_GRID_P2P_MINIMAL", "1")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    cmd
}

/// Every stdout and stderr this test saw, to check that no secret is among them.
#[derive(Default)]
struct Seen(Vec<String>);

impl Seen {
    fn run(&mut self, mut cmd: Command, stdin: Option<&str>) -> Output {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let out = match stdin {
            None => cmd.output().expect("run rusty-grid"),
            Some(input) => {
                cmd.stdin(Stdio::piped());
                let mut child = cmd.spawn().expect("spawn rusty-grid");
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(input.as_bytes())
                    .unwrap();
                child.wait_with_output().unwrap()
            }
        };
        self.0
            .push(String::from_utf8_lossy(&out.stderr).into_owned());
        out
    }

    /// Like `run`, but the stdout of `secret join-token` is the token itself, so it is not kept.
    fn run_token(&mut self, cmd: Command) -> Output {
        let mut cmd = cmd;
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let out = cmd.output().expect("run rusty-grid");
        self.0
            .push(String::from_utf8_lossy(&out.stderr).into_owned());
        out
    }

    fn keep_stdout(&mut self, out: &Output) {
        self.0
            .push(String::from_utf8_lossy(&out.stdout).into_owned());
    }
}

fn with(home: &Path, args: &[&str]) -> Command {
    let mut cmd = cli(home);
    cmd.args(args);
    cmd
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
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

fn secret_file(home: &Path) -> PathBuf {
    home.join(".oxideswarm").join("cluster.secret")
}

fn read_secret(home: &Path) -> String {
    fs::read_to_string(secret_file(home))
        .expect("secret file")
        .trim()
        .to_string()
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
fn test_a_machine_joins_with_one_token_and_the_cluster_passes_the_doctor() {
    let master_home = TempDir::new().unwrap();
    let worker_home = TempDir::new().unwrap();
    let mut seen = Seen::default();

    // The master machine: a secret in its home, a loopback master.
    let out = seen.run(
        {
            let mut c = cli(master_home.path());
            c.args(["secret", "generate"]);
            c
        },
        None,
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    seen.keep_stdout(&out);
    let secret = read_secret(master_home.path());
    assert_eq!(secret.len(), 64);

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
            port.is_some()
        }),
        "master did not publish its port; log:\n{}",
        master.log()
    );
    let addr = format!("127.0.0.1:{}", port.unwrap());

    // A token needs an address: a wildcard or missing one is refused, nothing is printed.
    let out = seen.run_token(with(master_home.path(), &["secret", "join-token"]));
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "no token without an address");

    // Create the token: stdout is the token alone, stderr warns that it is sensitive.
    let out = seen.run_token(with(
        master_home.path(),
        &["secret", "join-token", "--master", &addr],
    ));
    assert!(out.status.success(), "{}", text(&out.stderr));
    let token = text(&out.stdout).trim().to_string();
    assert!(token.starts_with(PREFIX), "token format");
    assert_eq!(text(&out.stdout).lines().count(), 1);
    assert!(text(&out.stderr).to_lowercase().contains("secret"));
    assert!(
        !token.contains(&secret),
        "the secret is not in the token as plain hex"
    );

    // The worker machine joins with the token on standard input.
    assert!(!secret_file(worker_home.path()).exists());
    let out = seen.run(with(worker_home.path(), &["join"]), Some(&token));
    seen.keep_stdout(&out);
    assert!(
        out.status.success(),
        "join failed:\n{}\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    assert_eq!(read_secret(worker_home.path()), secret);
    assert_private(&secret_file(worker_home.path()));
    let config =
        fs::read_to_string(worker_home.path().join(".oxideswarm/rusty-grid.toml")).unwrap();
    assert!(config.contains(&addr), "the address is saved: {config}");
    assert!(!config.contains(&secret), "no secret in the config file");
    assert!(
        text(&out.stdout).contains("C1"),
        "doctor ran: {}",
        text(&out.stdout)
    );

    // Joining again with the same token is harmless.
    let out = seen.run(with(worker_home.path(), &["join"]), Some(&token));
    seen.keep_stdout(&out);
    assert!(out.status.success(), "{}", text(&out.stderr));

    // The worker starts without --master or secret flags: both come from the join.
    let mut worker_cmd = cli(worker_home.path());
    worker_cmd
        .args([
            "worker",
            "--name",
            "joined",
            "--cores",
            "1",
            "--sandbox-base-dir",
        ])
        .arg(worker_home.path().join("sandbox"));
    let worker = Service::spawn(worker_cmd, worker_home.path().join("worker.log"));

    let mut doctor_ok = false;
    let ok = wait_for(Duration::from_secs(40), || {
        let out = seen.run(
            with(
                worker_home.path(),
                &[
                    "doctor",
                    "--skip-gpu",
                    "--skip-throughput",
                    "--expect-workers",
                    "1",
                ],
            ),
            None,
        );
        seen.keep_stdout(&out);
        doctor_ok = out.status.success();
        doctor_ok
    });
    assert!(
        ok,
        "doctor did not pass; worker log:\n{}\nmaster log:\n{}",
        worker.log(),
        master.log()
    );

    // Nothing printed the secret, in any process.
    for output in seen.0.iter().chain([&worker.log(), &master.log()]) {
        assert!(
            !output.contains(&secret),
            "the secret was printed:\n{output}"
        );
        assert!(!output.contains(&token), "the token was printed");
    }
}

#[test]
fn test_bad_tokens_and_a_different_existing_secret_are_refused() {
    let master_home = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();
    let mut seen = Seen::default();

    let out = seen.run(with(master_home.path(), &["secret", "generate"]), None);
    assert!(out.status.success());
    let secret = read_secret(master_home.path());
    // 127.0.0.1:9 refuses connections; the doctor is not what this test is about.
    let out = seen.run_token(with(
        master_home.path(),
        &["secret", "join-token", "--master", "127.0.0.1:9"],
    ));
    assert!(out.status.success());
    let token = text(&out.stdout).trim().to_string();

    // One changed byte (in the payload, and in the tag): refused, nothing written.
    for index in [PREFIX.len() + 3, token.len() - 2] {
        let mut bytes = token.clone().into_bytes();
        bytes[index] = if bytes[index] == b'A' { b'B' } else { b'A' };
        let changed = String::from_utf8(bytes).unwrap();
        let out = seen.run(with(home.path(), &["join", "--no-doctor"]), Some(&changed));
        seen.keep_stdout(&out);
        assert!(
            !out.status.success(),
            "a changed byte at {index} was accepted"
        );
        assert!(text(&out.stderr).contains("integrity") || text(&out.stderr).contains("damaged"));
        assert!(!secret_file(home.path()).exists());
        assert!(!home.path().join(".oxideswarm/rusty-grid.toml").exists());
        assert!(
            !text(&out.stderr).contains(&changed),
            "the token is not echoed"
        );
    }

    // Not a token at all.
    let out = seen.run(with(home.path(), &["join"]), Some("hello\n"));
    assert!(!out.status.success());
    // Empty input.
    let out = seen.run(with(home.path(), &["join"]), Some(""));
    assert!(!out.status.success());

    // A token as an argument is refused, and not echoed back.
    let out = seen.run(with(home.path(), &["join", &token]), Some(&token));
    seen.keep_stdout(&out);
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("argument"));
    assert!(!text(&out.stderr).contains(&token));
    assert!(
        !secret_file(home.path()).exists(),
        "a refused join writes nothing"
    );

    // A different existing secret is not replaced without the flag.
    fs::create_dir_all(home.path().join(".oxideswarm")).unwrap();
    let other = "cd".repeat(32);
    let out = seen.run(
        {
            let mut c = cli(home.path());
            c.args(["secret", "generate", "--force"]);
            c
        },
        None,
    );
    assert!(out.status.success());
    let existing = read_secret(home.path());
    assert_ne!(existing, secret);
    assert_ne!(existing, other);
    let out = seen.run(with(home.path(), &["join", "--no-doctor"]), Some(&token));
    seen.keep_stdout(&out);
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("--replace-secret"));
    assert_eq!(
        read_secret(home.path()),
        existing,
        "the old secret is untouched"
    );
    assert!(!home.path().join(".oxideswarm/rusty-grid.toml").exists());

    // With the flag it is replaced, at mode 600.
    let out = seen.run(
        with(home.path(), &["join", "--no-doctor", "--replace-secret"]),
        Some(&token),
    );
    seen.keep_stdout(&out);
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert_eq!(read_secret(home.path()), secret);
    assert_private(&secret_file(home.path()));
    assert!(
        fs::read_to_string(home.path().join(".oxideswarm/rusty-grid.toml"))
            .unwrap()
            .contains("127.0.0.1:9")
    );

    for output in &seen.0 {
        assert!(
            !output.contains(&secret),
            "the secret was printed:\n{output}"
        );
        assert!(
            !output.contains(&existing),
            "the old secret was printed:\n{output}"
        );
    }
}

#[test]
fn test_an_existing_config_is_kept_backed_up_and_not_repointed_without_the_flag() {
    let master_home = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();
    let mut seen = Seen::default();
    let out = seen.run(with(master_home.path(), &["secret", "generate"]), None);
    assert!(out.status.success());
    let secret = read_secret(master_home.path());
    let out = seen.run_token(with(
        master_home.path(),
        &["secret", "join-token", "--master", "127.0.0.1:9"],
    ));
    let token = text(&out.stdout).trim().to_string();

    let config_path = home.path().join(".oxideswarm/rusty-grid.toml");
    fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    let old = "# my own notes\n[worker]\nname = \"box\" # keep\nmaster = \"10.9.9.9:8088\"\n";
    fs::write(&config_path, old).unwrap();

    // A different master is already configured: refused, nothing written, not even the secret.
    let out = seen.run(with(home.path(), &["join", "--no-doctor"]), Some(&token));
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("--replace-master"));
    assert_eq!(fs::read_to_string(&config_path).unwrap(), old);
    assert!(!secret_file(home.path()).exists());

    // With the flag: comments and other keys survive, the keys are listed, a private backup exists.
    let out = seen.run(
        with(home.path(), &["join", "--no-doctor", "--replace-master"]),
        Some(&token),
    );
    seen.keep_stdout(&out);
    assert!(out.status.success(), "{}", text(&out.stderr));
    let new = fs::read_to_string(&config_path).unwrap();
    assert!(new.contains("# my own notes") && new.contains("name = \"box\" # keep"));
    assert!(new.contains("127.0.0.1:9") && !new.contains("10.9.9.9"));
    let err = text(&out.stderr);
    assert!(err.contains("[worker].master") && err.contains("[submit].master"));
    assert!(
        !err.contains("127.0.0.1:9"),
        "values are not listed, only keys"
    );
    let backups: Vec<PathBuf> = fs::read_dir(config_path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().contains("rusty-grid.toml.bak-"))
        .collect();
    assert_eq!(backups.len(), 1);
    assert_eq!(fs::read_to_string(&backups[0]).unwrap(), old);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&backups[0]).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    // A config file that is a symbolic link is never edited.
    #[cfg(unix)]
    {
        let other = TempDir::new().unwrap();
        let real = other.path().join("real.toml");
        fs::write(&real, "# real\n").unwrap();
        let link_home = TempDir::new().unwrap();
        fs::create_dir_all(link_home.path().join(".oxideswarm")).unwrap();
        std::os::unix::fs::symlink(&real, link_home.path().join(".oxideswarm/rusty-grid.toml"))
            .unwrap();
        let out = seen.run(
            with(link_home.path(), &["join", "--no-doctor"]),
            Some(&token),
        );
        assert!(!out.status.success());
        assert!(text(&out.stderr).contains("symbolic link"));
        assert_eq!(fs::read_to_string(&real).unwrap(), "# real\n");
        assert!(!secret_file(link_home.path()).exists());
    }

    for output in &seen.0 {
        assert!(
            !output.contains(&secret),
            "the secret was printed:\n{output}"
        );
    }
}
