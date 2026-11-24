//! Opaque-box Tier-5 adversarial hardening E2E suite for OxideSwarm (M5 Phase 2).
//! Exercises compiled `oxideswarm` / `rusty-grid` binaries, raw wire JSON framing, HTTP/1.1
//! dashboard APIs, and Unix PTY `join` races with zero internal Rust library imports.
//!
//! The PTY and signal scenarios are `#[cfg(unix)]`; on Windows the helpers they use are unused.
#![cfg_attr(not(unix), allow(unused_imports, dead_code))]

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;

const BIN_OX: &str = env!("CARGO_BIN_EXE_oxideswarm");
const BIN_RG: &str = env!("CARGO_BIN_EXE_rusty-grid");
const HANG_TO: Duration = Duration::from_secs(60);
static SEQ: AtomicUsize = AtomicUsize::new(0);

fn scrub_env(c: &mut Command) {
    for (k, _) in std::env::vars_os() {
        let u = k.to_string_lossy().to_ascii_uppercase();
        if u.starts_with("RUSTY_GRID_")
            || u.starts_with("OXIDESWARM_")
            || u.starts_with("OXIDE_")
            || u == "CARGO_MANIFEST_DIR"
            || u == "GITHUB_STEP_SUMMARY"
        {
            c.env_remove(&k);
        }
    }
}

fn home() -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let fake_bin = dir.path().join(".fake_bin");
    fs::create_dir_all(&fake_bin).unwrap();
    let pmset = fake_bin.join("pmset");
    fs::write(&pmset, "#!/bin/sh\necho \"Now drawing from 'AC Power'\"\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&pmset, fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}

fn txt(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn cmd_for(bin: &str, h: &Path) -> Command {
    let mut c = Command::new(bin);
    scrub_env(&mut c);
    let path_env = std::env::var_os("PATH")
        .map(|p| format!("{}:{}", h.join(".fake_bin").display(), p.to_string_lossy()))
        .unwrap_or_else(|| h.join(".fake_bin").display().to_string());
    c.current_dir(h)
        .env("HOME", h)
        .env("USERPROFILE", h)
        .env("OXIDESWARM_HOME", h.join(".oxideswarm"))
        .env("PATH", path_env)
        .env("NO_COLOR", "1")
        .env("RUSTY_GRID_DISABLE_DISCOVERY", "1")
        .env("RUSTY_GRID_DISABLE_WEB_UI", "1")
        .env("RUSTY_GRID_P2P_MINIMAL", "1")
        .env("RUSTY_GRID_NO_BATTERY", "1")
        .stdin(Stdio::null());
    c
}

fn wait_until(to: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < to {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    pred()
}

fn run_to(mut c: Command, to: Duration) -> Output {
    c.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut ch = c.spawn().expect("spawn");
    let (mut op, mut ep) = (ch.stdout.take().unwrap(), ch.stderr.take().unwrap());
    let oh = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = op.read_to_end(&mut b);
        b
    });
    let eh = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = ep.read_to_end(&mut b);
        b
    });
    let start = Instant::now();
    let status = loop {
        match ch.try_wait().unwrap() {
            Some(s) => break s,
            None if start.elapsed() < to => std::thread::sleep(Duration::from_millis(25)),
            None => {
                let _ = ch.kill();
                let _ = ch.wait();
                panic!("timeout {to:?}\n{}", txt(&eh.join().unwrap_or_default()));
            }
        }
    };
    Output {
        status,
        stdout: oh.join().unwrap_or_default(),
        stderr: eh.join().unwrap_or_default(),
    }
}

fn run_bin(bin: &str, h: &Path, args: &[&str]) -> Output {
    let mut c = cmd_for(bin, h);
    c.args(args);
    run_to(c, HANG_TO)
}

fn run_json(bin: &str, h: &Path, args: &[&str]) -> Value {
    let o = run_bin(bin, h, args);
    assert!(
        o.status.success(),
        "{args:?} ({:?}): {}",
        o.status.code(),
        txt(&o.stderr)
    );
    serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("json ({e}): {}", txt(&o.stdout)))
}

fn sub_sh(bin: &str, h: &Path, g: &str, sh: &str, extra: &[&str], to: Duration) -> Output {
    let mut c = cmd_for(bin, h);
    c.args(["submit", "--master", g, "--type", "shell", "--command", sh])
        .args(extra);
    run_to(c, to)
}

fn arr(v: &Value) -> &[Value] {
    v.as_array().map(Vec::as_slice).unwrap_or(&[])
}

fn has_tag(w: &Value, tag: &str) -> bool {
    arr(&w["tags"]).iter().any(|t| t == tag)
}

struct Service {
    child: Child,
    log: PathBuf,
}

impl Service {
    fn spawn(mut c: Command, log: PathBuf) -> Self {
        let f = fs::File::create(&log).unwrap();
        c.stdout(Stdio::from(f.try_clone().unwrap()))
            .stderr(Stdio::from(f));
        Self {
            child: c.spawn().expect("spawn service"),
            log,
        }
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

struct MasterGuard {
    svc: Service,
    grid_addr: SocketAddr,
    dash_addr: Option<SocketAddr>,
}

impl MasterGuard {
    fn g(&self) -> String {
        self.grid_addr.to_string()
    }
    fn d(&self) -> SocketAddr {
        self.dash_addr.expect("no dashboard")
    }
}

fn start_m(
    h: &Path,
    bin: &str,
    dash: bool,
    cpu100: bool,
    args: &[&str],
    env: &[(&str, &str)],
) -> MasterGuard {
    let id = SEQ.fetch_add(1, Ordering::SeqCst);
    let (pf, df) = (
        h.join(format!("m-{id}.port")),
        h.join(format!("d-{id}.port")),
    );
    let mut c = cmd_for(bin, h);
    c.args(["master", "--listen", "127.0.0.1:0", "--port-file"])
        .arg(&pf);
    if cpu100 {
        c.args(["--max-host-cpu-pct", "100"]);
    }
    if dash {
        c.env_remove("RUSTY_GRID_DISABLE_WEB_UI");
        c.args(["--dashboard-port", "0", "--dashboard-port-file"])
            .arg(&df);
    }
    c.args(args);
    for (k, v) in env {
        c.env(k, v);
    }
    let svc = Service::spawn(c, h.join(format!("m-{id}.log")));
    let rp = |p: &Path| {
        fs::read_to_string(p)
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
    };
    assert!(
        wait_until(HANG_TO, || {
            rp(&pf).is_some() && (!dash || rp(&df).is_some())
        }),
        "master log:\n{}",
        svc.log()
    );
    MasterGuard {
        grid_addr: SocketAddr::from(([127, 0, 0, 1], rp(&pf).unwrap())),
        dash_addr: dash.then(|| SocketAddr::from(([127, 0, 0, 1], rp(&df).unwrap()))),
        svc,
    }
}

fn start_w(h: &Path, bin: &str, g: &str, name: &str, extra: &[&str]) -> Service {
    let id = SEQ.fetch_add(1, Ordering::SeqCst);
    let sb = h.join(format!("sb-{name}-{id}"));
    fs::create_dir_all(&sb).unwrap();
    let mut c = cmd_for(bin, h);
    c.args([
        "worker",
        "--master",
        g,
        "--name",
        name,
        "--no-gpu",
        "--no-cuda",
        "--no-blender",
        "--no-portal",
        "--sandbox-base-dir",
    ])
    .arg(&sb);
    if !extra.contains(&"--cores") {
        c.args(["--cores", "2"]);
    }
    c.args(extra);
    let svc = Service::spawn(c, h.join(format!("w-{name}-{id}.log")));
    let ok = wait_until(HANG_TO, || {
        let o = run_bin(bin, h, &["workers", "--master", g, "--json"]);
        o.status.success()
            && serde_json::from_slice::<Value>(&o.stdout)
                .ok()
                .is_some_and(|v| {
                    arr(&v)
                        .iter()
                        .any(|w| w["name"] == name && has_tag(w, "state:connected"))
                })
    });
    assert!(ok, "worker {name} timeout; log:\n{}", svc.log());
    svc
}

struct HttpResp {
    status: u16,
    body: String,
}

impl HttpResp {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("json ({e}): {}", self.body))
    }
}

fn unchunk(raw: &[u8]) -> Vec<u8> {
    let (mut out, mut p) = (Vec::new(), 0);
    while p < raw.len() {
        let Some(nl) = raw[p..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let hx = std::str::from_utf8(&raw[p..p + nl])
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let Ok(n) = usize::from_str_radix(hx, 16) else {
            break;
        };
        p += nl + 2;
        if n == 0 {
            break;
        }
        let e = (p + n).min(raw.len());
        out.extend_from_slice(&raw[p..e]);
        p = e + 2;
    }
    out
}

fn http_req(
    addr: SocketAddr,
    m: &str,
    path: &str,
    hdrs: &[(&str, &str)],
    body: Option<&str>,
) -> HttpResp {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(10)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    let mut r = format!("{m} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    let mut ct = false;
    for (k, v) in hdrs {
        if k.eq_ignore_ascii_case("content-type") {
            ct = true;
        }
        r.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        if !ct {
            r.push_str("Content-Type: application/json\r\n");
        }
        r.push_str(&format!("Content-Length: {}\r\n\r\n{b}", b.len()));
    } else {
        r.push_str("\r\n");
    }
    s.write_all(r.as_bytes()).unwrap();
    s.flush().unwrap();
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    let sp = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&raw[..sp]);
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = BTreeMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let rb = &raw[sp + 4..];
    let bb = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
    {
        unchunk(rb)
    } else {
        rb.to_vec()
    };
    HttpResp {
        status,
        body: String::from_utf8_lossy(&bb).into_owned(),
    }
}

fn hcall(addr: SocketAddr, m: &str, path: &str, tok: Option<&str>, body: Option<&str>) -> HttpResp {
    if let Some(t) = tok {
        let b = format!("Bearer {t}");
        http_req(addr, m, path, &[("Authorization", &b)], body)
    } else {
        http_req(addr, m, path, &[], body)
    }
}

fn pump_buf(mut r: impl Read + Send + 'static, dst: Arc<Mutex<Vec<u8>>>) {
    std::thread::spawn(move || {
        let mut b = [0u8; 4096];
        while let Ok(n) = r.read(&mut b) {
            if n == 0 {
                break;
            }
            dst.lock().unwrap().extend_from_slice(&b[..n]);
        }
    });
}

#[cfg(unix)]
struct PtyJoin {
    pty: fs::File,
    child: Child,
    out: Arc<Mutex<Vec<u8>>>,
    err: Arc<Mutex<Vec<u8>>>,
}

#[cfg(unix)]
impl PtyJoin {
    fn spawn_raw(h: &Path) -> Self {
        use std::os::fd::FromRawFd;
        let (mut m, mut s) = (0, 0);
        // SAFETY: `m` and `s` are valid writable `c_int` locals; null optional pointers are permitted.
        let r = unsafe {
            libc::openpty(
                &mut m,
                &mut s,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(r, 0);
        // SAFETY: `openpty` returned two freshly opened distinct file descriptors.
        let (pty, slave) = unsafe { (fs::File::from_raw_fd(m), fs::File::from_raw_fd(s)) };
        let mut c = cmd_for(BIN_OX, h);
        c.args(["join", "--no-doctor"])
            .stdin(Stdio::from(slave))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = c.spawn().unwrap();
        let (out, err) = (
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
        );
        pump_buf(child.stdout.take().unwrap(), Arc::clone(&out));
        pump_buf(child.stderr.take().unwrap(), Arc::clone(&err));
        assert!(wait_until(HANG_TO, || {
            txt(&err.lock().unwrap()).contains("Paste the join token")
        }));
        Self {
            pty,
            child,
            out,
            err,
        }
    }

    fn spawn(h: &Path, inv: &str) -> Self {
        let mut p = Self::spawn_raw(h);
        p.send_line(inv);
        p
    }

    fn send_line(&mut self, line: &str) {
        writeln!(self.pty, "{line}").unwrap();
        let _ = self.pty.flush();
    }

    fn code_and_pending_id(&self) -> (String, String) {
        assert!(
            wait_until(HANG_TO, || {
                let s = txt(&self.out.lock().unwrap());
                s.contains("Pairing code: ") && s.contains("Pending enrollment ID: ")
            }),
            "out={}\nerr={}",
            txt(&self.out.lock().unwrap()),
            txt(&self.err.lock().unwrap())
        );
        let s = txt(&self.out.lock().unwrap());
        let code: String = s
            .split_once("Pairing code: ")
            .unwrap()
            .1
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        let pid: String = s
            .split_once("Pending enrollment ID: ")
            .unwrap()
            .1
            .chars()
            .take_while(|c| c.is_ascii_hexdigit())
            .collect();
        (code, pid)
    }

    fn yes(&mut self) {
        assert!(wait_until(HANG_TO, || {
            txt(&self.out.lock().unwrap()).contains("[y/N]:")
        }));
        writeln!(self.pty, "y").unwrap();
        let _ = self.pty.flush();
    }

    fn yes_if_prompted(&mut self) {
        let _ = wait_until(HANG_TO, || {
            self.child.try_wait().ok().flatten().is_some()
                || txt(&self.out.lock().unwrap()).contains("[y/N]:")
        });
        if txt(&self.out.lock().unwrap()).contains("[y/N]:") {
            let _ = writeln!(self.pty, "y");
            let _ = self.pty.flush();
        }
    }

    fn wait_exit(mut self) -> (std::process::ExitStatus, String, String) {
        let start = Instant::now();
        let st = loop {
            match self.child.try_wait().unwrap() {
                Some(s) => break s,
                None if start.elapsed() < HANG_TO => std::thread::sleep(Duration::from_millis(25)),
                None => panic!(
                    "join child exit timeout\nout={}\nerr={}",
                    txt(&self.out.lock().unwrap()),
                    txt(&self.err.lock().unwrap())
                ),
            }
        };
        std::thread::sleep(Duration::from_millis(50));
        let out = txt(&self.out.lock().unwrap());
        let err = txt(&self.err.lock().unwrap());
        (st, out, err)
    }
}

#[test]
fn tier5_rusty_grid_alias_repeatable_and_workers_json_speed_parity() {
    let h_m = home();
    let h_f = home();
    let h_s = home();
    fs::write(h_s.path().join("is_slow"), "1").unwrap();
    let m = start_m(h_m.path(), BIN_RG, false, true, &[], &[]);
    let _w1 = start_w(
        h_f.path(),
        BIN_RG,
        &m.g(),
        "rg-fast",
        &["--max-concurrency", "1", "--tag", "tier-fast"],
    );
    let _w2 = start_w(
        h_s.path(),
        BIN_RG,
        &m.g(),
        "rg-slow",
        &["--max-concurrency", "1", "--tag", "tier-slow"],
    );

    // Warm up SpeedBook on the SAME TaskKey across both workers via rusty-grid --repeatable.
    let shared_sh = "if [ -f \"$HOME/is_slow\" ]; then sleep 0.25; fi; echo rg-speed-ok";
    for _ in 0..3 {
        let out_f = sub_sh(
            BIN_RG,
            h_m.path(),
            &m.g(),
            shared_sh,
            &[
                "--repeatable",
                "--require-tag",
                "tier-fast",
                "--wait",
                "--json",
            ],
            HANG_TO,
        );
        assert!(out_f.status.success(), "{}", txt(&out_f.stderr));
        let out_s = sub_sh(
            BIN_RG,
            h_m.path(),
            &m.g(),
            shared_sh,
            &[
                "--repeatable",
                "--require-tag",
                "tier-slow",
                "--wait",
                "--json",
            ],
            HANG_TO,
        );
        assert!(out_s.status.success(), "{}", txt(&out_s.stderr));
    }

    let rg_workers = run_json(
        BIN_RG,
        h_m.path(),
        &["workers", "--master", &m.g(), "--json"],
    );
    let ox_workers = run_json(
        BIN_OX,
        h_m.path(),
        &["workers", "--master", &m.g(), "--json"],
    );
    for label in ["rg-fast", "rg-slow"] {
        let rg_w = arr(&rg_workers)
            .iter()
            .find(|w| w["name"] == label)
            .unwrap();
        let ox_w = arr(&ox_workers)
            .iter()
            .find(|w| w["name"] == label)
            .unwrap();
        let rg_speed = arr(&rg_w["tags"])
            .iter()
            .find_map(|t| t.as_str()?.strip_prefix("speed:"));
        let ox_speed = arr(&ox_w["tags"])
            .iter()
            .find_map(|t| t.as_str()?.strip_prefix("speed:"));
        assert!(
            rg_speed.is_some() && rg_speed == ox_speed,
            "expected matching speed:<factor> on {label}: rg={rg_w} vs ox={ox_w}"
        );
    }
}

#[test]
fn tier5_e2e_20_slot_cluster_allows_two_concurrent_speculative_and_workspace_exclusion() {
    let h = home();
    let h_f = home();
    let h_s1 = home();
    let h_s2 = home();
    let h_s3 = home();
    let h_s4 = home();
    let m = start_m(
        h.path(),
        BIN_OX,
        false,
        true,
        &["--reaper-interval-secs", "1", "--speed-hold-factor", "0"],
        &[],
    );
    // 20 total slots: fast (16) + 4x slow (1 each) => cap = 20/10 = 2.
    let _wf = start_w(
        h_f.path(),
        BIN_OX,
        &m.g(),
        "w20-fast",
        &[
            "--cores",
            "16",
            "--max-concurrency",
            "16",
            "--workspaces",
            "--tag",
            "tier-fast",
        ],
    );
    let _ws1 = start_w(
        h_s1.path(),
        BIN_OX,
        &m.g(),
        "w20-slow1",
        &[
            "--cores",
            "1",
            "--max-concurrency",
            "1",
            "--workspaces",
            "--tag",
            "tier-slow",
        ],
    );
    let _ws2 = start_w(
        h_s2.path(),
        BIN_OX,
        &m.g(),
        "w20-slow2",
        &[
            "--cores",
            "1",
            "--max-concurrency",
            "1",
            "--workspaces",
            "--tag",
            "tier-slow",
        ],
    );
    let _ws3 = start_w(
        h_s3.path(),
        BIN_OX,
        &m.g(),
        "w20-slow3",
        &[
            "--cores",
            "1",
            "--max-concurrency",
            "1",
            "--workspaces",
            "--tag",
            "tier-slow",
        ],
    );
    let _ws4 = start_w(
        h_s4.path(),
        BIN_OX,
        &m.g(),
        "w20-slow4",
        &[
            "--cores",
            "1",
            "--max-concurrency",
            "1",
            "--workspaces",
            "--tag",
            "tier-slow",
        ],
    );

    // Prime fast worker on shared TaskKey (`sh step.sh`) via `--input <path>:step.sh`.
    let prime_sh = h.path().join("prime_step.sh");
    fs::write(&prime_sh, "echo prime-ok\n").unwrap();
    let prime_arg = format!("{}:step.sh", prime_sh.display());
    for _ in 0..3 {
        let o = sub_sh(
            BIN_OX,
            h.path(),
            &m.g(),
            "sh step.sh",
            &[
                "--require-tag",
                "tier-fast",
                "--input",
                &prime_arg,
                "--wait",
            ],
            HANG_TO,
        );
        assert!(o.status.success(), "{}", txt(&o.stderr));
    }

    // Occupy all 16 cores of w20-fast so the slow tasks land on slow1/slow2/slow3.
    let blocker_out = sub_sh(
        BIN_OX,
        h.path(),
        &m.g(),
        "sleep 60",
        &["--cores", "16", "--require-tag", "tier-fast", "--json"],
        HANG_TO,
    );
    assert!(blocker_out.status.success(), "{}", txt(&blocker_out.stderr));
    let blocker_tid = serde_json::from_slice::<Value>(&blocker_out.stdout).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(wait_until(HANG_TO, || {
        let o = run_bin(
            BIN_OX,
            h.path(),
            &[
                "status",
                "--master",
                &m.g(),
                "--task-id",
                &blocker_tid,
                "--json",
            ],
        );
        serde_json::from_slice::<Value>(&o.stdout)
            .ok()
            .is_some_and(|v| v["status"] == "Running")
    }));

    // Workspace+repeatable straggler must NEVER receive a speculative copy (runs >10s floor).
    let ws_dir = h.path().join("ws-src");
    fs::create_dir_all(&ws_dir).unwrap();
    fs::write(ws_dir.join("hello.txt"), "ws-data").unwrap();
    let ws_cnt_file = h.path().join("ws_count.log");
    let ws_cmd = format!(
        "echo x >> '{}' ; sleep 12 ; cat hello.txt",
        ws_cnt_file.display()
    );
    let ws_out = sub_sh(
        BIN_OX,
        h.path(),
        &m.g(),
        &ws_cmd,
        &[
            "--workspace",
            "t5-ws",
            "--sync",
            ws_dir.to_str().unwrap(),
            "--repeatable",
            "--require-tag",
            "tier-slow",
            "--json",
        ],
        HANG_TO,
    );
    assert!(ws_out.status.success(), "{}", txt(&ws_out.stderr));
    let ws_tid = serde_json::from_slice::<Value>(&ws_out.stdout).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Submit 3 repeatable straggler tasks sharing the primed `sh step.sh` TaskKey.
    let spec_dir = h.path().join("spec_events");
    fs::create_dir_all(&spec_dir).unwrap();
    let prim_dir = h.path().join("prim_flags");
    fs::create_dir_all(&prim_dir).unwrap();
    let release_flag = spec_dir.join("release");

    let mut tids = Vec::new();
    for idx in 1..=3 {
        let script_path = h.path().join(format!("slow_{idx}.sh"));
        let prim_flag = prim_dir.join(format!("p{idx}.flag"));
        let seen_flag = spec_dir.join(format!("s{idx}.seen"));
        fs::write(
            &script_path,
            format!(
                "if [ ! -f '{}' ]; then\n  touch '{}'\n  sleep 30\nelse\n  touch '{}'\n  i=0\n  while [ ! -f '{}' ] && [ $i -lt 300 ]; do sleep 0.05; i=$((i+1)); done\n  echo rescued-{idx}\nfi\n",
                prim_flag.display(),
                prim_flag.display(),
                seen_flag.display(),
                release_flag.display(),
            ),
        )
        .unwrap();
        let in_arg = format!("{}:step.sh", script_path.display());
        let o = sub_sh(
            BIN_OX,
            h.path(),
            &m.g(),
            "sh step.sh",
            &["--repeatable", "--input", &in_arg, "--json"],
            HANG_TO,
        );
        assert!(o.status.success(), "{}", txt(&o.stderr));
        let tid = serde_json::from_slice::<Value>(&o.stdout).unwrap()["task_id"]
            .as_str()
            .unwrap()
            .to_string();
        tids.push(tid);
    }

    // Wait until all 3 repeatable tasks + ws_rep are Running on the slow workers.
    for tid in tids.iter().chain(std::iter::once(&ws_tid)) {
        assert!(
            wait_until(Duration::from_secs(20), || {
                let o = run_bin(
                    BIN_OX,
                    h.path(),
                    &["status", "--master", &m.g(), "--task-id", tid, "--json"],
                );
                serde_json::from_slice::<Value>(&o.stdout)
                    .ok()
                    .is_some_and(|v| v["status"] == "Running")
            }),
            "task {tid} never started on slow workers; status={}; workers={}; master_log={}",
            txt(&run_bin(
                BIN_OX,
                h.path(),
                &["status", "--master", &m.g(), "--task-id", tid, "--json"]
            )
            .stdout),
            txt(&run_bin(BIN_OX, h.path(), &["workers", "--master", &m.g(), "--json"]).stdout),
            m.svc.log()
        );
    }

    // Cancel the fast worker blocker -> w20-fast becomes idle and eligible for speculative copies.
    let cancel_o = run_bin(
        BIN_OX,
        h.path(),
        &["cancel", &blocker_tid, "--master", &m.g(), "--json"],
    );
    assert!(cancel_o.status.success(), "{}", txt(&cancel_o.stderr));

    let count_seen = || {
        fs::read_dir(&spec_dir)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("seen"))
            .count()
    };

    // Wait for 2 speculative copies to start concurrently on w20-fast (after 10s straggler floor).
    assert!(
        wait_until(HANG_TO, || count_seen() >= 2),
        "expected 2 concurrent speculative copies on 20-slot cluster, saw {}",
        count_seen()
    );
    // Hold > 1 reaper tick to verify the 3rd repeatable straggler is capped while 2 are in flight.
    std::thread::sleep(Duration::from_millis(1600));
    assert_eq!(
        count_seen(),
        2,
        "10% cap on 20 slots must allow exactly 2 concurrent speculative copies"
    );

    // Release the 2 speculative copies so they complete and free slots for the 3rd straggler.
    fs::write(&release_flag, "go").unwrap();
    for tid in &tids {
        let res = run_json(
            BIN_OX,
            h.path(),
            &["result", tid, "--master", &m.g(), "--wait", "--json"],
        );
        assert_eq!(res["exit_code"], 0, "task {tid}: {res}");
    }
    assert_eq!(
        count_seen(),
        3,
        "after slots freed, the 3rd repeatable straggler must also be speculatively rescued"
    );

    let ws_res = run_json(
        BIN_OX,
        h.path(),
        &["result", &ws_tid, "--master", &m.g(), "--wait", "--json"],
    );
    assert_eq!(ws_res["exit_code"], 0);
    let ws_runs = fs::read_to_string(&ws_cnt_file)
        .unwrap_or_default()
        .lines()
        .count();
    assert_eq!(
        ws_runs, 1,
        "workspace+repeatable task must run exactly once (no speculative copy), got {ws_runs}"
    );
}

#[test]
fn tier5_e2e_sched5_live_recovery_without_restart_and_multi_transition_antiflood() {
    let h = home();
    let sd = h.path().join("state");
    fs::create_dir_all(&sd).unwrap();
    let m = start_m(
        h.path(),
        BIN_OX,
        false,
        false,
        &["--state-dir", sd.to_str().unwrap()],
        &[("RUSTY_GRID_MAX_HOST_CPU_PCT", "-1")],
    );
    let _w1 = start_w(
        h.path(),
        BIN_OX,
        &m.g(),
        "sat-w1",
        &["--max-concurrency", "1", "--tag", "w-one"],
    );

    // Submit queued task while sat-w1 is host-CPU saturated -> warning logged once.
    let sub1 = sub_sh(
        BIN_OX,
        h.path(),
        &m.g(),
        "echo recover-ok",
        &["--json"],
        HANG_TO,
    );
    assert!(sub1.status.success(), "{}", txt(&sub1.stderr));
    let tid1 = serde_json::from_slice::<Value>(&sub1.stdout).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();

    assert!(wait_until(HANG_TO, || {
        m.svc.log().to_ascii_lowercase().contains("saturated")
    }));
    std::thread::sleep(Duration::from_millis(600));
    let count_1 = m
        .svc
        .log()
        .lines()
        .filter(|l| l.to_ascii_lowercase().contains("saturated"))
        .count();
    assert_eq!(count_1, 1, "anti-flood failed on w1: {}", m.svc.log());

    // Add second saturated worker -> busy-worker set changes {w1} -> {w1, w2} and logs once more.
    let _w2 = start_w(
        h.path(),
        BIN_OX,
        &m.g(),
        "sat-w2",
        &["--max-concurrency", "1", "--tag", "w-two"],
    );
    assert!(
        wait_until(HANG_TO, || {
            m.svc
                .log()
                .lines()
                .filter(|l| l.to_ascii_lowercase().contains("saturated"))
                .count()
                >= 2
        }),
        "expected second saturated log on busy-worker-set change; log:\n{}",
        m.svc.log()
    );
    std::thread::sleep(Duration::from_millis(500));
    let count_2 = m
        .svc
        .log()
        .lines()
        .filter(|l| l.to_ascii_lowercase().contains("saturated"))
        .count();
    assert_eq!(count_2, 2, "must emit once per busy-worker set change");

    // Shut down master cleanly and restart with normal host-CPU threshold -> persisted task drains.
    drop(_w2);
    drop(_w1);
    drop(m);
    let m2 = start_m(
        h.path(),
        BIN_OX,
        false,
        true,
        &["--state-dir", sd.to_str().unwrap()],
        &[],
    );
    let _w_rec = start_w(
        h.path(),
        BIN_OX,
        &m2.g(),
        "rec-w",
        &["--max-concurrency", "1"],
    );
    let res = run_json(
        BIN_OX,
        h.path(),
        &["result", &tid1, "--master", &m2.g(), "--wait", "--json"],
    );
    assert_eq!(res["exit_code"], 0);
}

#[test]
fn tier5_e2e_wire_register_spoof_rejected_end_to_end() {
    let h = home();
    let m = start_m(h.path(), BIN_OX, true, true, &[], &[]);

    let real_wid = "259fb4c9-c5d1-493f-a72e-72616c7962de";
    let victim_wid = "00000000-0000-0000-0000-000000000001";
    let reg = json!({
        "type": "Register",
        "worker_id": real_wid,
        "capabilities": {
            "name": "spoof-e2e-worker",
            "cpu_cores": 2,
            "ram_mb": 2048,
            "has_gpu": false,
            "is_simulated_gpu": false,
            "gpu_device_name": null,
            "tags": [
                "opt:spoof",
                "speed:0.01",
                "proto:99",
                "state:disconnected",
                format!("id:{victim_wid}"),
                "link:Relay (DERP):999.0ms",
                "feat:workspaces",
                "user:legit"
            ],
            "has_blender": false,
            "blender_version": null,
            "vram_mb": null,
            "has_cuda": false
        },
        "protocol_version": 10,
        "features": 0
    });
    let payload = serde_json::to_vec(&reg).unwrap();
    let frame_len = (1 + payload.len()) as u32;

    let mut stream = TcpStream::connect_timeout(&m.grid_addr, Duration::from_secs(10)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(&frame_len.to_be_bytes()).unwrap();
    stream.write_all(&[0x01]).unwrap();
    stream.write_all(&payload).unwrap();
    stream.flush().unwrap();

    // Read RegisterAck frame.
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let ack_len = u32::from_be_bytes(len_buf) as usize;
    let mut ack_buf = vec![0u8; ack_len];
    stream.read_exact(&mut ack_buf).unwrap();
    let ack_json: Value = serde_json::from_slice(&ack_buf[1..]).unwrap();
    assert_eq!(ack_json["accepted"], true);

    // Inspect workers --json, status, doctor --json, and /api/status while socket stays open.
    let workers = run_json(BIN_OX, h.path(), &["workers", "--master", &m.g(), "--json"]);
    let w = arr(&workers)
        .iter()
        .find(|w| w["name"] == "spoof-e2e-worker")
        .expect("spoof worker listed");
    assert_eq!(w["link"], "Link: TCP/LAN");
    assert!(has_tag(w, "user:legit"));
    assert!(has_tag(w, "state:connected"));
    assert!(has_tag(w, &format!("id:{real_wid}")));
    assert!(!has_tag(w, "state:disconnected"));
    assert!(!has_tag(w, &format!("id:{victim_wid}")));
    assert!(!has_tag(w, "link:Relay (DERP):999.0ms"));
    assert!(!has_tag(w, "feat:workspaces"));
    assert!(!has_tag(w, "opt:spoof"));
    assert!(!has_tag(w, "speed:0.01"));
    assert!(!has_tag(w, "proto:99"));

    let st_out = run_bin(BIN_OX, h.path(), &["status", "--master", &m.g()]);
    assert!(st_out.status.success());
    let st_txt = txt(&st_out.stdout);
    assert!(st_txt.contains("Connected Workers: 1"), "{st_txt}");
    assert!(!st_txt.contains("Relay (DERP)"), "{st_txt}");

    let doc_out = run_bin(
        BIN_OX,
        h.path(),
        &[
            "doctor",
            "--master",
            &m.g(),
            "--probe-timeout-secs",
            "2",
            "--skip-gpu",
            "--skip-spread",
            "--skip-throughput",
            "--json",
        ],
    );
    let doc: Value = serde_json::from_slice(&doc_out.stdout)
        .unwrap_or_else(|e| panic!("doctor json ({e}): {}", txt(&doc_out.stdout)));
    let c2 = arr(&doc["checks"])
        .iter()
        .find(|c| c["name"] == "Workers")
        .expect("C2 Workers check");
    assert_eq!(c2["outcome"], "pass", "doctor={doc}");
    let dw = arr(&doc["workers"])
        .iter()
        .find(|w| w["name"] == "spoof-e2e-worker")
        .unwrap();
    assert_eq!(dw["id"], real_wid);
    assert_eq!(dw["state"], "connected");
    assert_eq!(dw["link"], "TCP/LAN");

    let dash_status = hcall(m.d(), "GET", "/api/status", None, None);
    assert_eq!(dash_status.status, 200);
    let ds = dash_status.json();
    let dsw = arr(&ds["workers"])
        .iter()
        .find(|w| w["name"] == "spoof-e2e-worker")
        .unwrap();
    assert_eq!(dsw["is_relayed"], false);
    assert_ne!(dsw["interconnect_type"], "Relay (DERP)");
}

#[cfg(unix)]
#[test]
fn tier5_approve_validation_and_hyphenated_code_fingerprint() {
    let h_m = home();
    let h_j = home();
    let m = start_m(
        h_m.path(),
        BIN_OX,
        true,
        true,
        &["--auth", "both", "--auth-token", "adm-tok:adm:admin"],
        &[],
    );

    let inv_resp = hcall(
        m.d(),
        "POST",
        "/api/invites",
        Some("adm-tok"),
        Some(r#"{"roles":["worker"],"max_uses":1,"require_approval":true}"#),
    );
    assert_eq!(inv_resp.status, 200, "{}", inv_resp.body);
    let inv_json = inv_resp.json();
    let inv_tok = inv_json["invite"].as_str().unwrap().to_string();
    let fp = inv_json["master_fingerprint"].as_str().unwrap().to_string();

    let mut pty = PtyJoin::spawn(h_j.path(), &inv_tok);
    let (code, pid) = pty.code_and_pending_id();
    pty.yes();

    // Validation boundary errors -> 400
    let wrong_code = if code == "000000" { "111111" } else { "000000" };
    let r_bad_code = hcall(
        m.d(),
        "POST",
        &format!("/api/members/{pid}/approve"),
        Some("adm-tok"),
        Some(&format!(r#"{{"pairing_code":"{wrong_code}"}}"#)),
    );
    assert_eq!(r_bad_code.status, 400);

    let r_bad_fp = hcall(
        m.d(),
        "POST",
        &format!("/api/members/{pid}/approve"),
        Some("adm-tok"),
        Some(&format!(
            r#"{{"pairing_code":"{code}","fingerprint":"0000000000000000"}}"#
        )),
    );
    assert_eq!(r_bad_fp.status, 400);

    let r_mal_fp = hcall(
        m.d(),
        "POST",
        &format!("/api/members/{pid}/approve"),
        Some("adm-tok"),
        Some(&format!(
            r#"{{"pairing_code":"{code}","fingerprint":"xyz"}}"#
        )),
    );
    assert_eq!(r_mal_fp.status, 400);

    let r_unknown_field = hcall(
        m.d(),
        "POST",
        &format!("/api/members/{pid}/approve"),
        Some("adm-tok"),
        Some(&format!(r#"{{"pairing_code":"{code}","extra":1}}"#)),
    );
    assert_eq!(r_unknown_field.status, 400);

    let auth_hdr = "Bearer adm-tok";
    let r_plain = http_req(
        m.d(),
        "POST",
        &format!("/api/members/{pid}/approve"),
        &[("Authorization", auth_hdr), ("Content-Type", "text/plain")],
        Some(&format!(r#"{{"pairing_code":"{code}"}}"#)),
    );
    assert_eq!(r_plain.status, 400);

    // Hyphenated XXX-XXX pairing code + uppercase hyphenated fingerprint -> 200
    let hyph_code = format!("{}-{}", &code[..3], &code[3..]);
    let raw_fp: String = fp.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    let hyph_fp = format!(
        "{}-{}-{}-{}",
        &raw_fp[..4],
        &raw_fp[4..8],
        &raw_fp[8..12],
        &raw_fp[12..16]
    )
    .to_ascii_uppercase();
    let r_ok = hcall(
        m.d(),
        "POST",
        &format!("/api/members/{pid}/approve"),
        Some("adm-tok"),
        Some(&format!(
            r#"{{"pairing_code":"{hyph_code}","fingerprint":"{hyph_fp}"}}"#
        )),
    );
    assert_eq!(r_ok.status, 200, "{}", r_ok.body);

    let (st, _out, err) = pty.wait_exit();
    assert!(st.success(), "join failed: {err}");

    // Subsequent approve/reject on already-approved pending ID -> 404.
    assert_eq!(
        hcall(
            m.d(),
            "POST",
            &format!("/api/members/{pid}/reject"),
            Some("adm-tok"),
            Some("{}"),
        )
        .status,
        404
    );
    assert_eq!(
        hcall(
            m.d(),
            "POST",
            &format!("/api/members/{pid}/approve"),
            Some("adm-tok"),
            Some(&format!(r#"{{"pairing_code":"{code}"}}"#)),
        )
        .status,
        404
    );
}

#[cfg(unix)]
#[test]
fn tier5_concurrent_join_max_uses_one_race() {
    let h_m = home();
    let m = start_m(
        h_m.path(),
        BIN_OX,
        true,
        true,
        &["--auth", "both", "--auth-token", "adm-tok:adm:admin"],
        &[],
    );
    let inv_resp = hcall(
        m.d(),
        "POST",
        "/api/invites",
        Some("adm-tok"),
        Some(r#"{"roles":["worker"],"max_uses":1,"require_approval":false}"#),
    );
    assert_eq!(inv_resp.status, 200);
    let inv_tok = inv_resp.json()["invite"].as_str().unwrap().to_string();

    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let b = Arc::clone(&barrier);
        let tok = inv_tok.clone();
        handles.push(std::thread::spawn(move || {
            let h_j = home();
            let mut pty = PtyJoin::spawn_raw(h_j.path());
            b.wait();
            pty.send_line(&tok);
            pty.yes_if_prompted();
            let (st, _out, err) = pty.wait_exit();
            (st.code().unwrap_or(-1), err)
        }));
    }
    let results: Vec<(i32, String)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wins = results.iter().filter(|(c, _)| *c == 0).count();
    let losses = results.iter().filter(|(c, _)| *c != 0).count();
    assert_eq!(wins, 1, "expected exactly 1 winner: {results:?}");
    assert_eq!(losses, 3, "expected 3 losers: {results:?}");

    let members = hcall(m.d(), "GET", "/api/members", Some("adm-tok"), None).json();
    let non_local = arr(&members["members"])
        .iter()
        .filter(|m| m["name"] != "local-host")
        .count();
    assert_eq!(non_local, 1, "members: {members}");
}

#[cfg(unix)]
#[test]
fn tier5_concurrent_pending_approve_race_and_approve_vs_reject() {
    let h_m = home();
    let m = start_m(
        h_m.path(),
        BIN_OX,
        true,
        true,
        &["--auth", "both", "--auth-token", "adm-tok:adm:admin"],
        &[],
    );

    // 1. Two concurrent pending joins approved simultaneously against max_uses=1 -> one 200, one 400.
    let inv_1 = hcall(
        m.d(),
        "POST",
        "/api/invites",
        Some("adm-tok"),
        Some(r#"{"roles":["worker"],"max_uses":1,"require_approval":true}"#),
    )
    .json()["invite"]
        .as_str()
        .unwrap()
        .to_string();

    let h1 = home();
    let h2 = home();
    let mut p1 = PtyJoin::spawn(h1.path(), &inv_1);
    let mut p2 = PtyJoin::spawn(h2.path(), &inv_1);
    let (c1, pid1) = p1.code_and_pending_id();
    let (c2, pid2) = p2.code_and_pending_id();
    p1.yes();
    p2.yes();

    let dash = m.d();
    let bar = Arc::new(Barrier::new(2));
    let t1 = {
        let b = Arc::clone(&bar);
        std::thread::spawn(move || {
            b.wait();
            hcall(
                dash,
                "POST",
                &format!("/api/members/{pid1}/approve"),
                Some("adm-tok"),
                Some(&format!(r#"{{"pairing_code":"{c1}"}}"#)),
            )
            .status
        })
    };
    let t2 = {
        let b = Arc::clone(&bar);
        std::thread::spawn(move || {
            b.wait();
            hcall(
                dash,
                "POST",
                &format!("/api/members/{pid2}/approve"),
                Some("adm-tok"),
                Some(&format!(r#"{{"pairing_code":"{c2}"}}"#)),
            )
            .status
        })
    };
    let mut statuses = vec![t1.join().unwrap(), t2.join().unwrap()];
    statuses.sort();
    assert_eq!(statuses, vec![200, 400]);

    // 2. Concurrent approve vs reject on the same pending enrollment -> mutually exclusive 200 + 404.
    let inv_2 = hcall(
        m.d(),
        "POST",
        "/api/invites",
        Some("adm-tok"),
        Some(r#"{"roles":["worker"],"max_uses":2,"require_approval":true}"#),
    )
    .json();
    let inv_2_id = inv_2["invite_id"].as_str().unwrap().to_string();
    let inv_2_tok = inv_2["invite"].as_str().unwrap().to_string();
    let h3 = home();
    let mut p3 = PtyJoin::spawn(h3.path(), &inv_2_tok);
    let (c3, pid3) = p3.code_and_pending_id();
    p3.yes();

    let bar2 = Arc::new(Barrier::new(2));
    let ta = {
        let b = Arc::clone(&bar2);
        let pid = pid3.clone();
        std::thread::spawn(move || {
            b.wait();
            hcall(
                dash,
                "POST",
                &format!("/api/members/{pid}/approve"),
                Some("adm-tok"),
                Some(&format!(r#"{{"pairing_code":"{c3}"}}"#)),
            )
            .status
        })
    };
    let tr = {
        let b = Arc::clone(&bar2);
        let pid = pid3.clone();
        std::thread::spawn(move || {
            b.wait();
            hcall(
                dash,
                "POST",
                &format!("/api/members/{pid}/reject"),
                Some("adm-tok"),
                Some("{}"),
            )
            .status
        })
    };
    let mut ar_statuses = vec![ta.join().unwrap(), tr.join().unwrap()];
    ar_statuses.sort();
    assert_eq!(ar_statuses, vec![200, 404]);

    // 3. DELETE /api/invites/{id} invalidates pending enrollment for that invite -> approve returns 404.
    let h4 = home();
    let mut p4 = PtyJoin::spawn(h4.path(), &inv_2_tok);
    let (c4, pid4) = p4.code_and_pending_id();
    p4.yes();
    assert_eq!(
        hcall(
            dash,
            "DELETE",
            &format!("/api/invites/{inv_2_id}"),
            Some("adm-tok"),
            None,
        )
        .status,
        200
    );
    assert_eq!(
        hcall(
            dash,
            "POST",
            &format!("/api/members/{pid4}/approve"),
            Some("adm-tok"),
            Some(&format!(r#"{{"pairing_code":"{c4}"}}"#)),
        )
        .status,
        404
    );
}

#[cfg(unix)]
#[test]
fn tier5_expired_invite_rejected_and_duplicate_rename_concurrency() {
    let h_m = home();
    let m = start_m(
        h_m.path(),
        BIN_OX,
        true,
        true,
        &["--auth", "both", "--auth-token", "adm-tok:adm:admin"],
        &[],
    );

    // 1. Expired invite fails cleanly and writes no member key.
    let exp_inv = hcall(
        m.d(),
        "POST",
        "/api/invites",
        Some("adm-tok"),
        Some(r#"{"roles":["worker"],"expires_in_secs":1,"require_approval":false}"#),
    )
    .json()["invite"]
        .as_str()
        .unwrap()
        .to_string();
    std::thread::sleep(Duration::from_millis(1400));

    let h_exp = home();
    let mut p_exp = PtyJoin::spawn(h_exp.path(), &exp_inv);
    p_exp.yes_if_prompted();
    let (st_exp, _out_exp, _err_exp) = p_exp.wait_exit();
    assert!(!st_exp.success());
    assert!(!h_exp.path().join(".oxideswarm/member.key").exists());

    // 2. Enroll two workers, rename B -> A's name, verify ambiguous 400 and concurrent revoke/rename.
    let inv = hcall(
        m.d(),
        "POST",
        "/api/invites",
        Some("adm-tok"),
        Some(r#"{"roles":["worker"],"max_uses":2,"require_approval":false}"#),
    )
    .json()["invite"]
        .as_str()
        .unwrap()
        .to_string();
    let h_a = home();
    let h_b = home();
    let mut p_a = PtyJoin::spawn(h_a.path(), &inv);
    p_a.yes();
    assert!(p_a.wait_exit().0.success());
    let mut p_b = PtyJoin::spawn(h_b.path(), &inv);
    p_b.yes();
    assert!(p_b.wait_exit().0.success());

    let members = hcall(m.d(), "GET", "/api/members", Some("adm-tok"), None).json();
    let non_local: Vec<&Value> = arr(&members["members"])
        .iter()
        .filter(|e| e["name"] != "local-host")
        .collect();
    assert_eq!(non_local.len(), 2);
    let id_a = non_local[0]["id"].as_str().unwrap().to_string();
    let id_b = non_local[1]["id"].as_str().unwrap().to_string();

    assert_eq!(
        hcall(
            m.d(),
            "POST",
            &format!("/api/members/{id_a}/rename"),
            Some("adm-tok"),
            Some(r#"{"name":"shared-alpha"}"#),
        )
        .status,
        200
    );
    assert_eq!(
        hcall(
            m.d(),
            "POST",
            &format!("/api/members/{id_b}/rename"),
            Some("adm-tok"),
            Some(r#"{"name":"shared-alpha"}"#),
        )
        .status,
        200
    );
    assert_eq!(
        hcall(
            m.d(),
            "POST",
            "/api/members/shared-alpha/revoke",
            Some("adm-tok"),
            Some("{}"),
        )
        .status,
        400
    );

    let dash = m.d();
    let bar = Arc::new(Barrier::new(2));
    let id_b1 = id_b.clone();
    let id_b2 = id_b.clone();
    let t_rev = {
        let b = Arc::clone(&bar);
        std::thread::spawn(move || {
            b.wait();
            hcall(
                dash,
                "POST",
                &format!("/api/members/{id_b1}/revoke"),
                Some("adm-tok"),
                Some("{}"),
            )
            .status
        })
    };
    let t_ren = {
        let b = Arc::clone(&bar);
        std::thread::spawn(move || {
            b.wait();
            hcall(
                dash,
                "POST",
                &format!("/api/members/{id_b2}/rename"),
                Some("adm-tok"),
                Some(r#"{"name":"beta-raced"}"#),
            )
            .status
        })
    };
    assert_eq!(t_rev.join().unwrap(), 200);
    let ren_status = t_ren.join().unwrap();
    assert!(ren_status == 200 || ren_status == 404, "{ren_status}");
    assert_eq!(
        hcall(
            m.d(),
            "POST",
            &format!("/api/members/{id_b}/revoke"),
            Some("adm-tok"),
            Some("{}"),
        )
        .status,
        404
    );
}
