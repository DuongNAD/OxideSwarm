//! Opaque-box 4-tier E2E test suite for OxideSwarm (F1-F7, Tiers 1-4).
//! Exercises compiled binaries (`oxideswarm`, `rusty-grid`, `ox-mode`), HTTP/1.1 + JSON
//! dashboard APIs, stdio JSON-RPC (`mcp`), lockfile/doc invariants, and process exit codes
//! with zero internal Rust library imports and zero `Cargo.toml` modifications.
//!
//! Several scenarios need Unix process control (SIGTERM, `#!/bin/sh` fakes) and keep their body
//! under `#[cfg(unix)]`; on Windows the helpers they use are then unused.
#![cfg_attr(not(unix), allow(unused_imports, unused_variables, dead_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;

const BIN_OX: &str = env!("CARGO_BIN_EXE_oxideswarm");
const BIN_RG: &str = env!("CARGO_BIN_EXE_rusty-grid");
const BIN_MODE: &str = env!("CARGO_BIN_EXE_ox-mode");
const HANG_TO: Duration = Duration::from_secs(60);
const STRAG_TO: Duration = Duration::from_secs(90);
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

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}
fn home() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}
fn txt(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

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

fn cmd_for(bin: &str, h: &Path) -> Command {
    let mut c = Command::new(bin);
    scrub_env(&mut c);
    preserve_toolchain_env(&mut c);
    c.current_dir(h)
        .env("HOME", h)
        .env("USERPROFILE", h)
        .env("OXIDESWARM_HOME", h.join(".oxideswarm"))
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

fn ox(h: &Path, args: &[&str]) -> Output {
    let mut c = cmd_for(BIN_OX, h);
    c.args(args);
    run_to(c, HANG_TO)
}
fn ox_ok(h: &Path, args: &[&str]) {
    let o = ox(h, args);
    assert!(o.status.success(), "{args:?}: {}", txt(&o.stderr));
}
fn ox_code(h: &Path, args: &[&str]) -> Option<i32> {
    ox(h, args).status.code()
}
fn oxj(h: &Path, args: &[&str]) -> Value {
    let o = ox(h, args);
    assert!(
        o.status.success(),
        "{args:?} ({:?}): {}",
        o.status.code(),
        txt(&o.stderr)
    );
    serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("json ({e}): {}", txt(&o.stdout)))
}
fn sub_ok(h: &Path, g: &str, prog: &str, arg: &str) -> Value {
    oxj(
        h,
        &["submit", "--master", g, "--wait", "--json", "--", prog, arg],
    )
}
fn doc_ok(h: &Path, g: &str) -> Value {
    oxj(
        h,
        &[
            "doctor",
            "--master",
            g,
            "--skip-gpu",
            "--skip-throughput",
            "--json",
        ],
    )
}
fn arr(v: &Value) -> &[Value] {
    v.as_array().map(Vec::as_slice).unwrap_or(&[])
}
fn str_f<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}
fn has_tag(w: &Value, tag: &str) -> bool {
    arr(&w["tags"]).iter().any(|t| t == tag)
}
fn has_tag_pfx(w: &Value, pfx: &str) -> bool {
    arr(&w["tags"])
        .iter()
        .any(|t| t.as_str().is_some_and(|s| s.starts_with(pfx)))
}
fn mode_run(h: &Path, args: &[&str]) -> Output {
    let mut c = cmd_for(BIN_MODE, h);
    c.current_dir(root()).args(args);
    run_to(c, HANG_TO)
}
fn py_in(h: &Path, dir: &Path, extra_env: &[(&str, &Path)], args: &[&str]) -> Output {
    let mut c = Command::new("python3");
    scrub_env(&mut c);
    preserve_toolchain_env(&mut c);
    c.current_dir(dir).env("HOME", h).env("TMPDIR", h);
    for (k, v) in extra_env {
        c.env(k, v);
    }
    c.args(args);
    run_to(c, STRAG_TO)
}
fn py(h: &Path, args: &[&str]) -> Output {
    py_in(h, &root(), &[], args)
}
fn py_ok(h: &Path, args: &[&str]) {
    let o = py(h, args);
    assert!(o.status.success(), "{}\n{}", txt(&o.stdout), txt(&o.stderr));
}
fn sub_sh(h: &Path, g: &str, sh: &str, extra: &[&str], to: Duration) -> Output {
    let mut c = cmd_for(BIN_OX, h);
    c.args(["submit", "--master", g, "--type", "shell", "--command", sh])
        .args(extra);
    run_to(c, to)
}
fn sub_sh_j(h: &Path, g: &str, sh: &str, extra: &[&str], to: Duration) -> Value {
    let mut a = extra.to_vec();
    a.extend(["--wait", "--json"]);
    let o = sub_sh(h, g, sh, &a, to);
    assert!(o.status.success(), "{}\n{}", txt(&o.stdout), txt(&o.stderr));
    jv(&o)
}
fn sub_tid(h: &Path, g: &str, sh: &str, extra: &[&str]) -> String {
    let mut a = extra.to_vec();
    a.push("--json");
    let o = sub_sh(h, g, sh, &a, HANG_TO);
    assert!(o.status.success(), "{}", txt(&o.stderr));
    jv(&o)["task_id"].as_str().unwrap().to_string()
}
fn ox_tid(h: &Path, args: &[&str]) -> String {
    oxj(h, args)["task_id"].as_str().unwrap().to_string()
}
fn io_sub(h: &Path, g: &str, cmd: &str, inf: &str, outf: &str, extra: &[&str]) -> String {
    let od = h.join(format!("od_{}", SEQ.fetch_add(1, Ordering::SeqCst)));
    fs::create_dir_all(&od).unwrap();
    let ods = od.to_string_lossy().into_owned();
    let mut ex = vec![
        "--input",
        inf,
        "--output",
        outf,
        "--output-dir",
        &ods,
        "--json",
    ];
    ex.extend_from_slice(extra);
    let o = sub_sh(h, g, cmd, &ex, HANG_TO);
    assert!(
        o.status.success() && jv(&o)["saved_outputs"].is_array(),
        "{}",
        txt(&o.stderr)
    );
    fs::read_to_string(od.join(outf)).unwrap()
}
fn block_fast(h: &Path, g: &str, secs: u32) {
    let tid = sub_tid(
        h,
        g,
        &format!("sleep {secs}"),
        &["--require-tag", "tier-fast"],
    );
    wait_run(h, g, &tid);
}
fn assert_slow_cancelled(h: &Path, g: &str, sh: &Path, marker: &str) {
    let a = ["--require-tag", "tier-slow", "--wait"];
    assert!(sub_sh(h, g, "echo slow-free", &a, HANG_TO).status.success());
    assert!(!sh.join(marker).exists(), "slow sibling must be cancelled");
}
fn res_wait(h: &Path, g: &str, tid: &str) -> Value {
    oxj(h, &["result", tid, "--master", g, "--wait", "--json"])
}
fn task_st(h: &Path, g: &str, tid: &str) -> Value {
    oxj(h, &["status", "--master", g, "--task-id", tid, "--json"])
}
fn cancel_tid(h: &Path, g: &str, tid: &str) {
    assert_eq!(
        oxj(h, &["cancel", tid, "--master", g, "--json"])["cancelled"],
        true
    );
}
fn prime_sh(h: &Path, g: &str, tag: &str, sh: &str, inp: Option<&str>) {
    for _ in 0..2 {
        let mut ex = vec!["--require-tag", tag, "--wait"];
        if let Some(i) = inp {
            ex.extend(["--input", i]);
        }
        assert!(sub_sh(h, g, sh, &ex, HANG_TO).status.success());
    }
}
fn wait_run(h: &Path, g: &str, tid: &str) {
    let ok = wait_until(HANG_TO, || {
        let o = ox(h, &["status", "--master", g, "--task-id", tid, "--json"]);
        serde_json::from_slice::<Value>(&o.stdout)
            .ok()
            .is_some_and(|v| v["status"] == "Running")
    });
    assert!(ok, "task {tid} never entered Running");
}

fn wait_child(ch: &mut Child) -> std::process::ExitStatus {
    let start = Instant::now();
    while start.elapsed() < HANG_TO {
        if let Some(st) = ch.try_wait().unwrap() {
            return st;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("child exit timeout");
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
    fn wait_log_ci(&self, needle: &str) -> String {
        let low = needle.to_ascii_lowercase();
        wait_until(HANG_TO, || self.log().to_ascii_lowercase().contains(&low));
        let l = self.log();
        assert!(
            l.to_ascii_lowercase().contains(&low),
            "missing {needle:?} in log:\n{l}"
        );
        l
    }
    #[cfg(unix)]
    fn sigterm(&self) {
        let pid = self.child.id() as libc::pid_t;
        // SAFETY: `self.child` is a live child process owned by this `Service` guard.
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
    }
    fn wait_exit(&mut self) -> std::process::ExitStatus {
        wait_child(&mut self.child)
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
        wait_until(HANG_TO, || rp(&pf).is_some()
            && (!dash || rp(&df).is_some())),
        "master log:\n{}",
        svc.log()
    );
    MasterGuard {
        grid_addr: SocketAddr::from(([127, 0, 0, 1], rp(&pf).unwrap())),
        dash_addr: dash.then(|| SocketAddr::from(([127, 0, 0, 1], rp(&df).unwrap()))),
        svc,
    }
}
fn m_ok(h: &Path) -> MasterGuard {
    start_m(h, BIN_OX, false, true, &[], &[])
}
fn m_dash(h: &Path, args: &[&str]) -> MasterGuard {
    start_m(h, BIN_OX, true, true, args, &[])
}
fn m_auth(h: &Path) -> MasterGuard {
    m_dash(
        h,
        &["--auth", "members", "--auth-token", "adm-tok:adm:admin"],
    )
}

fn start_w(h: &Path, bin: &str, g: &str, name: &str, extra: &[&str]) -> Service {
    let id = SEQ.fetch_add(1, Ordering::SeqCst);
    let sb = h.join(format!("sb-{name}-{id}"));
    fs::create_dir_all(&sb).unwrap();
    let mut c = cmd_for(bin, h);
    let base = [
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
    ];
    c.args(base).arg(&sb);
    if !extra.contains(&"--cores") {
        c.args(["--cores", "2"]);
    }
    c.args(extra);
    let svc = Service::spawn(c, h.join(format!("w-{name}-{id}.log")));
    let sf_arg = extra
        .windows(2)
        .find_map(|w| (w[0] == "--secret-file").then_some(w[1]));
    let ok = wait_until(HANG_TO, || {
        let mut q = cmd_for(bin, h);
        q.args(["workers", "--master", g, "--json"]);
        if let Some(sf) = sf_arg {
            q.args(["--secret-file", sf]);
        }
        let o = run_to(q, HANG_TO);
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
fn w_ok(h: &Path, g: &str, name: &str) -> Service {
    start_w(h, BIN_OX, g, name, &[])
}
fn w_tag(h: &Path, g: &str, name: &str, cores: &str, conc: &str, tag: &str) -> Service {
    start_w(
        h,
        BIN_OX,
        g,
        name,
        &["--cores", cores, "--max-concurrency", conc, "--tag", tag],
    )
}
fn w_pair(h1: &Path, h2: &Path, g: &str, conc1: bool) -> (Service, Service) {
    let fa: &[&str] = if conc1 {
        &["--max-concurrency", "1", "--tag", "tier-fast"]
    } else {
        &["--tag", "tier-fast"]
    };
    let sa: &[&str] = if conc1 {
        &["--max-concurrency", "1", "--tag", "tier-slow"]
    } else {
        &["--tag", "tier-slow"]
    };
    (
        start_w(h1, BIN_OX, g, "w-fast", fa),
        start_w(h2, BIN_OX, g, "w-slow", sa),
    )
}

struct HttpResp {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}
impl HttpResp {
    fn hdr(&self, k: &str) -> Option<&str> {
        self.headers
            .get(&k.to_ascii_lowercase())
            .map(String::as_str)
    }
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
        headers,
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
fn hg(a: SocketAddr, p: &str, t: Option<&str>) -> HttpResp {
    hcall(a, "GET", p, t, None)
}
fn hg_j(a: SocketAddr, p: &str, t: Option<&str>) -> Value {
    let r = hg(a, p, t);
    assert_eq!(r.status, 200, "{p}: {}", r.body);
    r.json()
}
fn hp(a: SocketAddr, p: &str, t: Option<&str>, b: &str) -> HttpResp {
    hcall(a, "POST", p, t, Some(b))
}
fn hp_ok(a: SocketAddr, p: &str, t: Option<&str>, b: &str) {
    let r = hp(a, p, t, b);
    assert_eq!(r.status, 200, "{p}: {}", r.body);
}
fn jv(o: &Output) -> Value {
    serde_json::from_slice(&o.stdout).unwrap()
}
fn inv_tok(d: SocketAddr, roles: &str, uses: u32) -> String {
    let b = format!(r#"{{"roles":[{roles}],"max_uses":{uses}}}"#);
    hp(d, "/api/invites", Some("adm-tok"), &b).json()["invite"]
        .as_str()
        .unwrap()
        .to_string()
}
fn wid(h: &Path, g: &str, name: &str) -> String {
    let wk = oxj(h, &["workers", "--master", g, "--json"]);
    let w = arr(&wk).iter().find(|w| w["name"] == name).unwrap();
    arr(&w["tags"])
        .iter()
        .find_map(|t| t.as_str()?.strip_prefix("id:"))
        .unwrap()
        .to_string()
}
fn parse_speed_val(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| {
        let s = v.as_str()?.trim();
        s.strip_prefix("speed:")
            .unwrap_or(s)
            .trim_end_matches('x')
            .parse()
            .ok()
    })
}
fn worker_speed(w: &Value) -> Option<f64> {
    w.get("speed")
        .and_then(parse_speed_val)
        .or_else(|| w.get("speed_factor").and_then(parse_speed_val))
        .or_else(|| {
            let ts = w
                .get("tags")
                .or_else(|| w.get("capabilities").and_then(|c| c.get("tags")))?;
            arr(ts)
                .iter()
                .find_map(|x| x.as_str()?.strip_prefix("speed:")?.parse().ok())
        })
}
fn dash_sp(st: &Value, aw: &Value, name: &str) -> f64 {
    arr(&st["workers"])
        .iter()
        .find(|w| w["name"] == name)
        .and_then(worker_speed)
        .or_else(|| {
            arr(aw)
                .iter()
                .find(|w| w["capabilities"]["name"] == name)
                .and_then(worker_speed)
        })
        .unwrap_or_else(|| panic!("missing dash speed for {name}: st={st} aw={aw}"))
}
fn bp_m(h: &Path, dash: bool, extra: &[&str]) -> MasterGuard {
    start_m(
        h,
        BIN_OX,
        dash,
        false,
        extra,
        &[("RUSTY_GRID_MAX_HOST_CPU_PCT", "-1")],
    )
}
fn assert_bp_recover(h: &Path, m1: MasterGuard, w1: Service, sds: &str, tid: &str, needle: &str) {
    assert_eq!(task_st(h, &m1.g(), tid)["status"], "Queued");
    assert!(
        hg_j(m1.d(), "/api/status", None)["tasks"]["queued"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    drop(w1);
    stop_m(m1, None);
    let m2 = start_m(h, BIN_OX, true, true, &["--state-dir", sds], &[]);
    let _w2 = w_ok(h, &m2.g(), "bp-rec");
    let r = res_wait(h, &m2.g(), tid);
    assert!(r["exit_code"] == 0 && str_f(&r, "stdout").contains(needle));
    assert_eq!(task_st(h, &m2.g(), tid)["status"], "Completed");
    assert_eq!(hg_j(m2.d(), "/api/status", None)["tasks"]["queued"], 0);
}
fn prime_pair(h: &Path, g: &str, sh: &str) {
    fs::write(h.join("mode_fast"), "0").unwrap();
    fs::write(h.join("mode_slow"), "1").unwrap();
    prime_sh(h, g, "tier-fast", sh, Some("mode_fast:mode.txt"));
    prime_sh(h, g, "tier-slow", sh, Some("mode_slow:mode.txt"));
}
fn stop_m(m: MasterGuard, _extra: Option<&Path>) {
    #[cfg(unix)]
    {
        let mut m = m;
        m.svc.sigterm();
        assert!(m.svc.wait_exit().success());
    }
    #[cfg(not(unix))]
    let _ = m;
}

struct McpProc {
    child: Child,
    stdin: Option<ChildStdin>,
    rx: mpsc::Receiver<Value>,
    log: PathBuf,
}
impl McpProc {
    fn spawn(h: &Path, g: &str) -> Self {
        let log = h.join(format!("mcp-{}.log", SEQ.fetch_add(1, Ordering::SeqCst)));
        let mut c = cmd_for(BIN_OX, h);
        c.args(["mcp", "--master", g])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(fs::File::create(&log).unwrap()));
        let mut child = c.spawn().unwrap();
        let (stdin, stdout) = (child.stdin.take().unwrap(), child.stdout.take().unwrap());
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for l in BufReader::new(stdout).lines().map_while(Result::ok) {
                if !l.trim().is_empty() && tx.send(serde_json::from_str(l.trim()).unwrap()).is_err()
                {
                    break;
                }
            }
        });
        Self {
            child,
            stdin: Some(stdin),
            rx,
            log,
        }
    }
    fn send_raw(&mut self, l: &str) {
        let s = self.stdin.as_mut().unwrap();
        writeln!(s, "{l}").unwrap();
        s.flush().unwrap();
    }
    fn send(&mut self, v: &Value) {
        self.send_raw(&serde_json::to_string(v).unwrap());
    }
    fn recv(&self) -> Value {
        self.rx.recv_timeout(HANG_TO).unwrap_or_else(|e| {
            panic!(
                "MCP timeout ({e}): {}",
                fs::read_to_string(&self.log).unwrap_or_default()
            )
        })
    }
    fn init(&mut self) -> Value {
        self.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}));
        let r = self.recv();
        self.send(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        r
    }
    fn call(&mut self, id: u64, name: &str, args: Value) -> Value {
        self.send(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}}));
        self.recv()
    }
    fn close(mut self) -> std::process::ExitStatus {
        self.stdin.take();
        wait_child(&mut self.child)
    }
}
impl Drop for McpProc {
    fn drop(&mut self) {
        self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(unix)]
fn pump_buf(mut r: impl Read + Send + 'static, buf: Arc<Mutex<Vec<u8>>>) {
    std::thread::spawn(move || {
        let mut b = [0u8; 512];
        while let Ok(n) = r.read(&mut b) {
            if n == 0 {
                break;
            }
            buf.lock().unwrap().extend_from_slice(&b[..n]);
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
    fn spawn(h: &Path, inv: &str) -> Self {
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
        let (mut pty, slave) = unsafe { (fs::File::from_raw_fd(m), fs::File::from_raw_fd(s)) };
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
        assert!(wait_until(HANG_TO, || txt(&err.lock().unwrap())
            .contains("Paste the join token")));
        writeln!(pty, "{inv}").unwrap();
        pty.flush().unwrap();
        Self {
            pty,
            child,
            out,
            err,
        }
    }
    fn code(&self) -> String {
        assert!(
            wait_until(HANG_TO, || txt(&self.out.lock().unwrap())
                .contains("Pairing code: ")),
            "{}",
            txt(&self.err.lock().unwrap())
        );
        txt(&self.out.lock().unwrap())
            .split_once("Pairing code: ")
            .unwrap()
            .1
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect()
    }
    fn yes(&mut self) {
        assert!(wait_until(HANG_TO, || txt(&self.out.lock().unwrap())
            .contains("[y/N]:")));
        writeln!(self.pty, "y").unwrap();
        let _ = self.pty.flush();
    }
    fn wait_exit(mut self, expect_ok: bool) {
        let mut st = None;
        let done = wait_until(HANG_TO, || {
            st = self.child.try_wait().unwrap();
            st.is_some()
        });
        assert!(
            done,
            "join exit timeout: {}",
            txt(&self.err.lock().unwrap())
        );
        assert_eq!(
            st.unwrap().success(),
            expect_ok,
            "out:\n{}\nerr:\n{}",
            txt(&self.out.lock().unwrap()),
            txt(&self.err.lock().unwrap())
        );
    }
}
#[cfg(unix)]
impl Drop for PtyJoin {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(unix)]
fn pending_id(d: SocketAddr, tok: &str) -> String {
    let (u, t) = ("/api/members", Some(tok));
    assert!(wait_until(HANG_TO, || !arr(&hg_j(d, u, t)["pending"]).is_empty()));
    hg_j(d, u, t)["pending"][0]["pending_id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[cfg(unix)]
fn join_approve(d: SocketAddr, tok: &str, inv: &str) -> (TempDir, String) {
    let u = "/api/members";
    let mems = hg_j(d, u, Some(tok));
    let before: BTreeSet<String> = arr(&mems["members"])
        .iter()
        .filter_map(|m| m["id"].as_str().map(str::to_string))
        .collect();
    let jh = home();
    let mut p = PtyJoin::spawn(jh.path(), inv);
    let (code, pid) = (p.code(), pending_id(d, tok));
    hp_ok(
        d,
        &format!("{u}/{pid}/approve"),
        Some(tok),
        &format!(r#"{{"pairing_code":"{code}"}}"#),
    );
    p.yes();
    p.wait_exit(true);
    let mut mid = None;
    assert!(wait_until(HANG_TO, || {
        let mj = hg_j(d, u, Some(tok));
        mid = arr(&mj["members"])
            .iter()
            .filter_map(|m| m["id"].as_str())
            .find(|id| !before.contains(*id))
            .map(str::to_string);
        mid.is_some()
    }));
    (jh, mid.unwrap())
}

#[cfg(unix)]
fn assert_revoke_requeue(
    h: &Path,
    m: &MasterGuard,
    rev: (&Path, &str, &str),
    stay: (&Path, &str),
    tag: &str,
) {
    let ((d, g), (jh, mid, rw), (sh, sw)) = ((m.d(), m.g()), rev, stay);
    fs::write(jh.join("is_revoked"), "1").unwrap();
    let cmd = format!("if [ -f \"$HOME/is_revoked\" ]; then sleep 60; fi; echo {tag}");
    let tid = sub_tid(h, &g, &cmd, &[]);
    wait_run(h, &g, &tid);
    let _ws = w_ok(sh, &g, sw);
    let sid = wid(h, &g, sw);
    hp_ok(
        d,
        &format!("/api/members/{mid}/revoke"),
        Some("adm-tok"),
        "{}",
    );
    assert!(wait_until(HANG_TO, || {
        let mj = hg_j(d, "/api/members", Some("adm-tok"));
        let gm = !arr(&mj["members"]).iter().any(|x| x["id"] == mid)
            && !arr(&mj["active_connections"])
                .iter()
                .any(|c| c["member_id"] == mid || c["id"] == mid);
        let wks = oxj(h, &["workers", "--master", &g, "--json"]);
        let gw = !arr(&wks).iter().any(|w| {
            w["name"] == rw && (has_tag(w, "state:connected") || has_tag(w, "state:busy"))
        });
        gm && gw
    }));
    let r = res_wait(h, &g, &tid);
    assert!(
        r["exit_code"] == 0 && r["worker_id"] == sid && str_f(&r, "stdout").contains(tag),
        "r={r}"
    );
}

fn assert_bad_ox(h: &Path, cases: &[(&[&str], &str)]) {
    for (a, needle) in cases {
        let o = ox(h, a);
        assert!(
            !o.status.success() && txt(&o.stderr).contains(needle),
            "{a:?}: {}",
            txt(&o.stderr)
        );
    }
}

fn semver(v: &str) -> (u64, u64, u64) {
    let mut p = v.split(['-', '+']).next().unwrap_or(v).split('.');
    (
        p.next().and_then(|s| s.parse().ok()).unwrap_or(0),
        p.next().and_then(|s| s.parse().ok()).unwrap_or(0),
        p.next().and_then(|s| s.parse().ok()).unwrap_or(0),
    )
}

fn lock_pkgs(p: &Path) -> Vec<(String, String, Option<String>)> {
    let doc: toml::Value = toml::from_str(&fs::read_to_string(p).unwrap()).unwrap();
    doc["package"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["name"].as_str().unwrap_or("").into(),
                p["version"].as_str().unwrap_or("").into(),
                p.get("source")
                    .and_then(toml::Value::as_str)
                    .map(str::to_string),
            )
        })
        .collect()
}

fn backlog_rows(s: &str) -> Vec<(String, String, String)> {
    let (mut out, mut on) = (Vec::new(), false);
    for l in s.lines().map(str::trim) {
        if l.starts_with("| ID | Item | Priority | Size |") {
            on = true;
            continue;
        }
        if !on || l.starts_with("|---") {
            continue;
        }
        if !l.starts_with('|') {
            break;
        }
        let c: Vec<&str> = l.split('|').map(str::trim).collect();
        if c.len() >= 9 {
            out.push((c[1].into(), c[7].into(), c[8].into()));
        }
    }
    out
}

fn const_u32(src: &str, name: &str) -> Option<u32> {
    src.lines().find_map(|l| {
        l.contains(name)
            .then(|| {
                l.split('=')
                    .nth(1)?
                    .trim()
                    .trim_end_matches(';')
                    .trim()
                    .parse()
                    .ok()
            })
            .flatten()
    })
}

fn gen_sec(h: &Path) -> String {
    assert!(ox(h, &["secret", "generate"]).status.success());
    txt(&ox(h, &["secret", "show", "--dashboard-token"]).stdout)
        .trim()
        .to_string()
}

const SKEW_SELF: &[&str] = &[
    "scripts/release_skew.py",
    BIN_OX,
    BIN_OX,
    "--timeout",
    "30",
    "--release-protocol-rs",
    "crates/core/src/protocol.rs",
    "--pr-protocol-rs",
    "crates/core/src/protocol.rs",
];

// ============================================================================
// F1: Cluster Lifecycle & Task Execution (6 Tier 1 + 6 Tier 2)
// ============================================================================

#[test]
fn f1_t1_master_worker_submit_wait_json() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f1-w1");
    let j = sub_ok(h.path(), &m.g(), "echo", "hello-f1");
    assert!(j["exit_code"] == 0 && j["worker_name"] == "f1-w1" && j["machines"] == 1);
    assert!(str_f(&j, "stdout").contains("hello-f1"));
}

#[test]
fn f1_t1_status_and_workers_json_surface() {
    let h = home();
    let m = m_ok(h.path());
    let g = m.g();
    let (_w1, _w2) = (w_ok(h.path(), &g, "f1-a"), w_ok(h.path(), &g, "f1-b"));
    let sub = sub_ok(h.path(), &g, "echo", "ok");
    let tid = sub["task_id"].as_str().unwrap();
    let st = oxj(h.path(), &["status", "--master", &g, "--json"]);
    assert!(st["worker_count"] == 2 && st["completed_tasks"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(task_st(h.path(), &g, tid)["status"], "Completed");
    let wk = oxj(h.path(), &["workers", "--master", &g, "--json"]);
    assert_eq!(arr(&wk).len(), 2);
    for w in arr(&wk) {
        assert!(!str_f(w, "link").is_empty() && has_tag_pfx(w, "os:") && has_tag_pfx(w, "slots:"));
    }
}

#[test]
fn f1_t1_cancel_running_task_and_result() {
    let h = home();
    let m = m_ok(h.path());
    let g = m.g();
    let _w = w_ok(h.path(), &g, "f1-cw");
    let tid = sub_tid(
        h.path(),
        &g,
        "import time; time.sleep(60)",
        &["--interpreter", "python3"],
    );
    wait_run(h.path(), &g, &tid);
    cancel_tid(h.path(), &g, &tid);
    assert!(!ox(
        h.path(),
        &["result", &tid, "--master", &g, "--wait", "--json"]
    )
    .status
    .success());
}

#[test]
fn f1_t1_result_wait_and_output_artifact() {
    let h = home();
    let p = h.path();
    let m = m_ok(p);
    let g = m.g();
    let _w = w_ok(p, &g, "f1-io");
    fs::write(p.join("in.txt"), "opaque-payload").unwrap();
    assert_eq!(
        io_sub(
            p,
            &g,
            "tr a-z A-Z < in.txt > out.txt",
            "in.txt:in.txt",
            "out.txt",
            &[]
        ),
        "OPAQUE-PAYLOAD"
    );
    let tid = sub_tid(p, &g, "sleep 0.2; echo async-res-ok", &[]);
    let r = res_wait(p, &g, &tid);
    assert!(r["exit_code"] == 0 && str_f(&r, "stdout").contains("async-res-ok"));
}

#[test]
fn f1_t1_secret_generate_show_and_authenticated_cluster() {
    let h = home();
    let tok = gen_sec(h.path());
    let sec = txt(&ox(h.path(), &["secret", "show"]).stdout)
        .trim()
        .to_string();
    assert!(tok.len() == 64 && sec.len() == 64 && sec != tok);
    let jt = ox(
        h.path(),
        &["secret", "join-token", "--master", "127.0.0.1:8088"],
    );
    assert!(jt.status.success() && txt(&jt.stdout).contains("oxs1-join:"));
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f1-aw");
    assert_eq!(sub_ok(h.path(), &m.g(), "echo", "ok")["exit_code"], 0);
}

#[test]
fn f1_t1_p2p_ephemeral_key_secret_file_master_and_doctor() {
    let h = home();
    let p = h.path();
    let (sf, tf) = (p.join("p2p.secret"), p.join("p2p.ticket"));
    let (sfs, tfs) = (
        sf.to_str().unwrap().to_string(),
        tf.to_str().unwrap().to_string(),
    );
    ox_ok(p, &["secret", "generate", "--out", &sfs]);
    let args = [
        "--p2p",
        "--ephemeral-key",
        "--secret-file",
        &sfs,
        "--p2p-ticket-file",
        &tfs,
    ];
    let m = start_m(p, BIN_OX, false, true, &args, &[]);
    let g = m.g();
    assert!(wait_until(HANG_TO, || fs::read_to_string(&tf)
        .is_ok_and(|s| !s.trim().is_empty())));
    let w = start_w(p, BIN_OX, &g, "f1-p2p", &["--secret-file", &sfs]);
    let da = [
        "--secret-file",
        &sfs,
        "doctor",
        "--master",
        &g,
        "--skip-gpu",
        "--skip-throughput",
        "--json",
    ];
    assert!(!arr(&oxj(p, &da)["checks"]).is_empty());
    drop(w);
    stop_m(m, Some(&tf));
}

#[test]
fn f1_t2_worker_rejects_p2p_ticket_in_master_flag() {
    let h = home();
    let o = ox(h.path(), &["worker", "--master", r#"{"id":"fake"}"#]);
    assert!(o.status.code() == Some(1) && txt(&o.stderr).contains("--p2p-ticket"));
    let o2 = ox(
        h.path(),
        &["worker", "--master", "127.0.0.1:8088", "--tag", "os:custom"],
    );
    assert!(!o2.status.success() && txt(&o2.stderr).contains("os:"));
}

#[test]
fn f1_t2_worker_rejects_isolation_fs_with_exposed_secret() {
    let h = home();
    let sec = "a".repeat(64);
    let o = ox(
        h.path(),
        &[
            "--secret",
            &sec,
            "worker",
            "--master",
            "127.0.0.1:8088",
            "--isolation",
            "fs",
        ],
    );
    assert_eq!(o.status.code(), Some(6));
}

#[test]
fn f1_t2_cancel_unknown_uuid_exits_1() {
    let h = home();
    let m = m_ok(h.path());
    let o = ox(
        h.path(),
        &[
            "cancel",
            "00000000-0000-0000-0000-000000000000",
            "--master",
            &m.g(),
            "--json",
        ],
    );
    assert!(o.status.code() == Some(1) && jv(&o)["cancelled"] == false);
}

#[test]
fn f1_t2_result_non_terminal_exits_3_and_unknown_exits_1() {
    let h = home();
    let p = h.path();
    let m = m_ok(p);
    let g = m.g();
    let tid = ox_tid(p, &["submit", "--master", &g, "--json", "--", "echo", "q"]);
    assert_eq!(
        ox_code(p, &["result", &tid, "--master", &g, "--json"]),
        Some(3)
    );
    let u = "00000000-0000-0000-0000-000000000099";
    assert_eq!(
        ox_code(p, &["result", u, "--master", &g, "--json"]),
        Some(1)
    );
}

#[test]
fn f1_t2_bad_secret_and_unreachable_master_exit_6_and_1() {
    let (ha, hb) = (home(), home());
    let _ = (gen_sec(ha.path()), gen_sec(hb.path()));
    let m = m_ok(ha.path());
    assert_eq!(
        ox_code(hb.path(), &["status", "--master", &m.g(), "--json"]),
        Some(6)
    );
    assert_eq!(
        ox_code(ha.path(), &["status", "--master", "127.0.0.1:1"]),
        Some(1)
    );
}

#[test]
fn f1_t2_nonzero_task_exit_code_propagation() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f1-fw");
    let a = [
        "submit",
        "--master",
        &m.g(),
        "--wait",
        "--json",
        "--",
        "sh",
        "-c",
        "exit 42",
    ];
    let o = ox(h.path(), &a);
    assert!(o.status.code() == Some(42) && jv(&o)["exit_code"] == 42);
}

// ============================================================================
// F2: Security, Advisory & Doc/Citation Hygiene (6 Tier 1 + 5 Tier 2)
// ============================================================================

#[test]
fn f2_t1_cargo_lock_and_fuzz_lock_rustix_patched() {
    for rel in ["Cargo.lock", "fuzz/Cargo.lock"] {
        let pkgs = lock_pkgs(&root().join(rel));
        let r: Vec<_> = pkgs.iter().filter(|(n, _, _)| n == "rustix").collect();
        assert!(!r.is_empty(), "{rel} missing rustix");
        for (_, v, _) in r {
            assert!(semver(v) >= (1, 1, 6), "{rel} rustix {v}");
        }
    }
}

#[test]
fn f2_t1_deny_toml_allows_only_crates_io_and_no_git() {
    let d: toml::Value =
        toml::from_str(&fs::read_to_string(root().join("deny.toml")).unwrap()).unwrap();
    let s = &d["sources"];
    assert_eq!(s["unknown-registry"].as_str(), Some("deny"));
    assert_eq!(s["unknown-git"].as_str(), Some("deny"));
    assert!(s["allow-git"].as_array().unwrap().is_empty());
    let exp = format!(
        "registry+{}",
        s["allow-registry"].as_array().unwrap()[0].as_str().unwrap()
    );
    for rel in ["Cargo.lock", "fuzz/Cargo.lock"] {
        for (n, v, src) in lock_pkgs(&root().join(rel)) {
            if let Some(src) = src {
                assert!(
                    !src.starts_with("git+") && src == exp,
                    "bad {rel} {n} {v}: {src}"
                );
            }
        }
    }
}

#[test]
fn f2_t1_feat9_d6_benchmark_only_in_project_and_readme() {
    for rel in ["PROJECT.md", "README.md"] {
        let body = fs::read_to_string(root().join(rel)).unwrap();
        assert!(
            body.contains("benchmark only") && body.contains("Command"),
            "{rel} FEAT-9 D6"
        );
    }
}

#[test]
fn f2_t1_backlog_header_recount_matches_rows() {
    // The newest recount line of the Index header must match the rows; the number of rows itself
    // grows with every new item, so it is not pinned.
    let b = fs::read_to_string(root().join("docs/BACKLOG.md")).unwrap();
    let rows = backlog_rows(&b);
    assert!(rows.len() >= 247, "BACKLOG Index lost rows: {}", rows.len());
    let cnt = |st: &str| rows.iter().filter(|r| r.1 == st).count();
    let open = |ln: &str| {
        rows.iter()
            .filter(|r| r.1 != "Implemented" && r.2 == ln)
            .count()
    };
    let want = format!(
        "{} items, {} Implemented, {} Partial and {} Planned; {} open items are In and {} are Out",
        rows.len(),
        cnt("Implemented"),
        cnt("Partial"),
        cnt("Planned"),
        open("In"),
        open("Out")
    );
    assert!(b
        .lines()
        .rfind(|l| l.contains("Recounted on") && l.contains("items,"))
        .unwrap()
        .contains(&want));
}

#[test]
fn f2_t1_python_hygiene_suite_passes() {
    let h = home();
    py_ok(h.path(), &["tests/test_m1_challenger_hygiene.py"]);
}

#[test]
fn f2_t1_citation_cff_and_readme_how_to_cite() {
    let cff = fs::read_to_string(root().join("CITATION.cff")).expect("CITATION.cff");
    let cargo: toml::Value =
        toml::from_str(&fs::read_to_string(root().join("Cargo.toml")).unwrap()).unwrap();
    let ver = cargo["workspace"]["package"]["version"].as_str().unwrap();
    assert!(cff.contains("cff-version: 1.2.0"));
    for k in [
        "cff-version:",
        "message:",
        "title:",
        "abstract:",
        "authors:",
        "version:",
        "date-released:",
        "license:",
        "repository-code:",
        "keywords:",
    ] {
        assert!(cff.contains(k), "missing {k}");
    }
    assert!(
        cff.contains(&format!("version: {ver}")) || cff.contains(&format!("version: \"{ver}\""))
    );
    let rd = fs::read_to_string(root().join("README.md")).unwrap();
    assert!(rd.find("How to cite").unwrap() < rd.find("## License").unwrap());
}

#[test]
fn f2_t2_six_legacy_files_absent_and_no_crlf_sh() {
    for rel in [
        "ORIGINAL_REQUEST.md",
        "TEST_READY.md",
        "TEST_INFRA.md",
        "upgrade_and_optimization_plan.md",
        "docs/WHAT_WAS_DONE.md",
        "scripts/sync_network/SUCCESS_CONFIRMED.md",
    ] {
        assert!(!root().join(rel).exists(), "legacy exists: {rel}");
    }
    let mut stack = vec![root().join("scripts"), root().join("tests")];
    while let Some(d) = stack.pop() {
        if !d.exists() {
            continue;
        }
        for e in fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some("sh") {
                assert!(
                    !fs::read(&p).unwrap().contains(&b'\r'),
                    "CR in {}",
                    p.display()
                );
            }
        }
    }
}

#[test]
fn f2_t2_backlog_no_duplicate_ids_and_valid_lines() {
    let b = fs::read_to_string(root().join("docs/BACKLOG.md")).unwrap();
    let mut seen = BTreeSet::new();
    for (id, st, ln) in backlog_rows(&b) {
        assert!(seen.insert(id.clone()), "dup {id}");
        assert!(
            (st == "Implemented" && (ln.is_empty() || (id == "TEST-56" && ln == "In")))
                || (st != "Implemented" && (ln == "In" || ln == "Out"))
        );
    }
}

#[test]
fn f2_t2_ox_mode_size_allowlist_empty_and_under_3000() {
    let allow = fs::read_to_string(root().join("scripts/rust-file-size-allowlist.txt")).unwrap();
    assert!(allow
        .lines()
        .map(str::trim)
        .all(|l| l.is_empty() || l.starts_with('#')));
    let h = home();
    let o = mode_run(h.path(), &["test", "size", "--json"]);
    assert!(o.status.success() && jv(&o)["success"] == true);
}

#[test]
fn f2_t2_ox_mode_size_rejects_oversized_rust_file() {
    let h = home();
    let p = h.path();
    fs::create_dir_all(p.join("crates/fake/src")).unwrap();
    fs::create_dir_all(p.join("scripts")).unwrap();
    fs::write(p.join("Cargo.toml"), "").unwrap();
    fs::write(p.join("scripts/rust-file-size-allowlist.txt"), "").unwrap();
    fs::write(p.join("crates/fake/src/lib.rs"), "fn x() {}\n".repeat(3001)).unwrap();
    let mut c = cmd_for(BIN_MODE, p);
    c.args(["test", "size", "--json"]);
    let o = run_to(c, HANG_TO);
    let j = jv(&o);
    assert!(
        o.status.code() == Some(1)
            && j["success"] == false
            && j["metrics"]["failures"] == 1
            && str_f(&j, "stderr").contains("crates/fake/src/lib.rs")
    );
}

#[test]
fn f2_t2_citation_cff_rejects_stale_version() {
    let h = home();
    let p = h.path();
    fs::create_dir_all(p.join("tests")).unwrap();
    let rel = "tests/test_m1_challenger_hygiene.py";
    fs::copy(root().join(rel), p.join(rel)).unwrap();
    fs::copy(root().join("Cargo.toml"), p.join("Cargo.toml")).unwrap();
    fs::write(p.join("CITATION.cff"), "cff-version: 1.2.0\nmessage: m\ntitle: OxideSwarm\nabstract: a\nauthors:\n  - name: The OxideSwarm Authors\nversion: \"0.0.1\"\ndate-released: 2026-01-01\nlicense:\n  - MIT\n  - Apache-2.0\nrepository-code: https://example.invalid\nkeywords:\n  - grid\n").unwrap();
    let o = py_in(
        p,
        p,
        &[],
        &[
            rel,
            "TestMilestone1EmpiricalStress.test_citation_cff_matches_cargo_workspace",
        ],
    );
    assert!(
        !o.status.success()
            && txt(&o.stderr).contains("CITATION.cff version must match Cargo.toml"),
        "{}",
        txt(&o.stderr)
    );
}

// ============================================================================
// F3: Protocol Skew & Compatibility Tooling (6 Tier 1 + 6 Tier 2)
// ============================================================================

#[test]
fn f3_t1_release_skew_help_lists_flags() {
    let h = home();
    let o = py(h.path(), &["scripts/release_skew.py", "--help"]);
    let s = txt(&o.stdout);
    assert!(
        o.status.success()
            && s.contains("--timeout")
            && s.contains("--release-protocol-rs")
            && s.contains("--pr-protocol-rs")
    );
}

#[test]
fn f3_t1_release_skew_python_unit_suite_passes() {
    let h = home();
    py_ok(h.path(), &["tests/test_release_skew.py"]);
}

#[test]
fn f3_t1_release_skew_self_pair_works() {
    let h = home();
    let o = py(h.path(), SKEW_SELF);
    assert!(o.status.success(), "{}\n{}", txt(&o.stdout), txt(&o.stderr));
    assert_eq!(
        txt(&o.stdout)
            .lines()
            .filter(|l| l.contains("| works |"))
            .count(),
        3
    );
}

#[test]
fn f3_t1_release_skew_writes_github_step_summary() {
    let h = home();
    let sum = h.path().join("summary.md");
    assert!(py_in(
        h.path(),
        &root(),
        &[("GITHUB_STEP_SUMMARY", &sum)],
        SKEW_SELF
    )
    .status
    .success());
    let b = fs::read_to_string(&sum).unwrap();
    assert!(b.contains("### Release skew") && b.contains("| Master |") && b.contains("| works |"));
}

#[test]
fn f3_t1_protocol_rs_defines_9_to_10_window() {
    let p = fs::read_to_string(root().join("crates/core/src/protocol.rs")).unwrap();
    assert_eq!(const_u32(&p, "PROTOCOL_VERSION_CURRENT"), Some(10));
    assert_eq!(const_u32(&p, "PROTOCOL_VERSION_MIN_COMPATIBLE"), Some(9));
}

#[test]
fn f3_t1_ci_workflow_pins_v030_and_newest_release() {
    let ci = fs::read_to_string(root().join(".github/workflows/ci.yml")).unwrap();
    assert!(
        ci.contains("release-skew:")
            && ci.contains("v0.3.0")
            && ci.contains("gh release view")
            && ci.contains("release_skew.py")
    );
}

#[test]
fn f3_t2_release_skew_rejects_bogus_flag() {
    let h = home();
    assert_eq!(
        py(h.path(), &["scripts/release_skew.py", "--bogus-flag"])
            .status
            .code(),
        Some(2)
    );
}

// release_skew.py and its fake `#!/bin/sh` binaries run on ubuntu-latest only.
#[cfg(unix)]
#[test]
fn f3_t2_release_skew_rejects_non_executable_bin() {
    let h = home();
    let f = h.path().join("not_exec");
    fs::write(&f, "plain").unwrap();
    let o = py(
        h.path(),
        &["scripts/release_skew.py", f.to_str().unwrap(), BIN_OX],
    );
    assert!(o.status.code() == Some(2) && txt(&o.stderr).contains("is not an executable file"));
}

#[test]
fn f3_t2_release_skew_rejects_missing_or_invalid_protocol_rs() {
    let h = home();
    let bad = h.path().join("bad.rs");
    fs::write(&bad, "pub const X: u32 = 1;\n").unwrap();
    for p in [h.path().join("missing.rs"), bad] {
        let a = [
            "scripts/release_skew.py",
            BIN_OX,
            BIN_OX,
            "--release-protocol-rs",
            p.to_str().unwrap(),
        ];
        let o = py(h.path(), &a);
        assert!(
            o.status.code() == Some(2)
                && txt(&o.stderr).contains("cannot read the protocol constants")
        );
    }
}

// release_skew.py and its fake `#!/bin/sh` binaries run on ubuntu-latest only.
#[cfg(unix)]
#[test]
fn f3_t2_release_skew_detects_broken_binary() {
    let h = home();
    let bad = h.path().join("broken_worker.sh");
    fs::write(
        &bad,
        format!("#!/bin/sh\n[ \"$1\" = master ] && exec \"{BIN_OX}\" \"$@\"\nexit 1\n"),
    )
    .unwrap();
    fs::set_permissions(&bad, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let args = [
        "scripts/release_skew.py",
        bad.to_str().unwrap(),
        BIN_OX,
        "--timeout",
        "5",
        "--release-protocol-rs",
        "crates/core/src/protocol.rs",
        "--pr-protocol-rs",
        "crates/core/src/protocol.rs",
    ];
    let o = py(h.path(), &args);
    assert!(
        o.status.code() == Some(1)
            && format!("{}\n{}", txt(&o.stdout), txt(&o.stderr)).contains("broken")
    );
}

#[test]
fn f3_t2_version_error_format_and_classifier() {
    let p = fs::read_to_string(root().join("crates/core/src/protocol.rs")).unwrap();
    assert!(p.contains("Incompatible protocol version") && p.contains("supported:"));
    let s = "import sys; sys.path.insert(0, 'scripts'); import release_skew as rs; assert rs.expected_verdicts((9, 9), (10, 9)) == [rs.REFUSED, rs.WORKS, rs.WORKS]; v, ok, vers, _ = rs.classify(1, '', 1, '', 'Incompatible protocol version 10 from worker (supported: 9..=9)'); assert v == rs.REFUSED and ok and vers == (10, 9); vb, okb, _, _ = rs.classify(1, '', 1, '', 'crash'); assert vb == rs.BROKEN and not okb";
    let h = home();
    py_ok(h.path(), &["-c", s]);
}

#[test]
fn f3_t2_proto5_p9_p10_asymmetry_in_ci_expectations() {
    let ci = fs::read_to_string(root().join(".github/workflows/ci.yml")).unwrap();
    assert!(ci.contains("v0.3.0") && ci.contains("release-skew"));
    let h = home();
    let s = "import sys; sys.path.insert(0, 'scripts'); import release_skew as rs; assert rs.expected_verdicts((9, 9), (10, 9)) == [rs.REFUSED, rs.WORKS, rs.WORKS]";
    py_ok(h.path(), &["-c", s]);
}

// ============================================================================
// F4: Dashboard Status, Auth, Members & Invites HTTP/JSON API (7 T1 + 6 T2)
// ============================================================================

#[test]
fn f4_t1_dashboard_root_html_and_security_headers() {
    let h = home();
    let _ = gen_sec(h.path());
    let m = m_dash(h.path(), &[]);
    let r = hcall(m.d(), "GET", "/", None, None);
    assert!(r.status == 200 && r.body.contains("OxideSwarm"));
    assert_eq!(r.hdr("x-content-type-options"), Some("nosniff"));
    assert_eq!(r.hdr("x-frame-options"), Some("DENY"));
    assert_eq!(r.hdr("referrer-policy"), Some("no-referrer"));
}

#[test]
fn f4_t1_dashboard_status_workers_tasks_metrics() {
    let h = home();
    let tok = gen_sec(h.path());
    let m = m_dash(h.path(), &[]);
    let (g, d, t) = (m.g(), m.d(), Some(tok.as_str()));
    let _w = w_ok(h.path(), &g, "f4-w1");
    assert_eq!(sub_ok(h.path(), &g, "echo", "f4")["exit_code"], 0);
    assert!(
        hg_j(d, "/api/status", t)["tasks"]["completed"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert_eq!(arr(&hg_j(d, "/api/workers", t)).len(), 1);
    assert!(!arr(&hg_j(d, "/api/tasks", t)).is_empty());
    let met = hg(d, "/metrics", t);
    assert!(met.status == 200 && (met.body.contains("# TYPE") || met.body.contains("rusty_grid")));
}

#[test]
fn f4_t1_dashboard_post_task_and_poll_completion() {
    let h = home();
    let m = m_dash(h.path(), &["--auth-token", "sub-tok:sub:submitter"]);
    let (d, t) = (m.d(), Some("sub-tok"));
    let _w = w_ok(h.path(), &m.g(), "f4-pw");
    let post = hp(d, "/api/tasks", t, r#"{"command":"echo dash-post-ok"}"#);
    assert_eq!(post.status, 200);
    let tid = post.json()["task_id"].as_str().unwrap().to_string();
    assert!(wait_until(HANG_TO, || {
        let r = hg(d, &format!("/api/tasks/{tid}"), t);
        r.status == 200
            && r.json()["state"] == "completed"
            && str_f(&r.json(), "stdout").contains("dash-post-ok")
    }));
}

#[test]
fn f4_t1_dashboard_auth_mechanisms_and_derived_token() {
    let h = home();
    let tok = gen_sec(h.path());
    let m = m_dash(h.path(), &[]);
    let d = m.d();
    assert_eq!(hg(d, "/api/status", Some(&tok)).status, 200);
    assert_eq!(
        http_req(d, "GET", "/api/status", &[("X-API-Key", &tok)], None).status,
        200
    );
    assert_eq!(hg(d, &format!("/api/status?token={tok}"), None).status, 200);
}

#[test]
fn f4_t1_dashboard_members_list_pending_connections() {
    let h = home();
    let m = m_auth(h.path());
    let j = hg_j(m.d(), "/api/members", Some("adm-tok"));
    assert!(
        j["members"].is_array() && j["pending"].is_array() && j["active_connections"].is_array()
    );
}

#[test]
fn f4_t1_dashboard_invites_create_list_delete() {
    let h = home();
    let m = m_auth(h.path());
    let (d, t) = (m.d(), Some("adm-tok"));
    let cr = hp(d, "/api/invites", t, r#"{"roles":["worker"],"max_uses":1}"#);
    assert!(cr.status == 200 || cr.status == 201);
    let cj = cr.json();
    let iid = cj["invite_id"].as_str().unwrap();
    assert!(str_f(&cj, "invite").starts_with("oxs1-invite:"));
    let has_inv = |v: &Value, rev: bool| {
        arr(&v["invites"])
            .iter()
            .any(|i| i["invite_id"] == iid && i["revoked"] == rev)
    };
    let ls = hg(d, "/api/invites", t);
    assert!(
        ls.status == 200
            && !ls.body.contains("invite_secret")
            && !ls.body.contains("oxs1-invite:")
            && has_inv(&ls.json(), false)
    );
    let dl = hcall(d, "DELETE", &format!("/api/invites/{iid}"), t, None);
    assert!(dl.status == 200 || dl.status == 204);
    assert!(has_inv(&hg_j(d, "/api/invites", t), true));
}

#[test]
fn f4_t1_dashboard_members_approve_rename_reject_revoke_and_ui() {
    let h = home();
    let m = m_auth(h.path());
    let (d, t, u) = (m.d(), Some("adm-tok"), "/api/members");
    let html = hg(d, "/", None);
    assert!(html.status == 200 && html.body.contains(u) && html.body.contains("Members"));
    let rdoc = fs::read_to_string(root().join("docs/reference.md")).unwrap();
    let gdoc = fs::read_to_string(root().join("docs/guides/add-a-machine.md")).unwrap();
    let sdoc = fs::read_to_string(root().join("docs/SECURITY.md")).unwrap();
    assert!(
        !rdoc.contains("dashboard members page remain planned")
            && gdoc.contains("invite")
            && sdoc.contains("members revoke")
    );
    #[cfg(unix)]
    {
        let inv = inv_tok(d, r#""worker""#, 2);
        let (_jh1, mid) = join_approve(d, "adm-tok", &inv);
        hp_ok(
            d,
            &format!("{u}/{mid}/rename"),
            t,
            r#"{"name":"renamed-node"}"#,
        );
        let mems = hg_j(d, u, t);
        assert_eq!(
            arr(&mems["members"])
                .iter()
                .find(|x| x["id"] == mid)
                .unwrap()["name"],
            "renamed-node"
        );
        hp_ok(d, &format!("{u}/{mid}/revoke"), t, "{}");
        assert!(!arr(&hg_j(d, u, t)["members"])
            .iter()
            .any(|x| x["id"] == mid));
        let jh2 = home();
        let p2 = PtyJoin::spawn(jh2.path(), &inv);
        let pid2 = pending_id(d, "adm-tok");
        hp_ok(d, &format!("{u}/{pid2}/reject"), t, "{}");
        p2.wait_exit(false);
        let mj = hg_j(d, u, t);
        assert!(
            !arr(&mj["pending"]).iter().any(|p| p["pending_id"] == pid2)
                && arr(&mj["members"]).len() == 1
                && !jh2.path().join(".oxideswarm").exists()
        );
    }
}

#[test]
fn f4_t2_dashboard_missing_and_bad_token_401() {
    let h = home();
    let _ = gen_sec(h.path());
    let m = m_dash(h.path(), &[]);
    for (meth, p, b) in [
        ("GET", "/api/status", None),
        ("GET", "/api/workers", None),
        ("POST", "/api/tasks", Some(r#"{"command":"echo hi"}"#)),
    ] {
        let r = hcall(m.d(), meth, p, None, b);
        assert!(r.status == 401 && r.hdr("www-authenticate").is_some());
        assert_eq!(hcall(m.d(), meth, p, Some("bad"), b).status, 401);
    }
}

#[test]
fn f4_t2_dashboard_rbac_403_observer_and_submitter() {
    let h = home();
    let m = m_dash(
        h.path(),
        &[
            "--auth-token",
            "obs-tok:obs:observer",
            "--auth-token",
            "sub-tok:sub:submitter",
        ],
    );
    let d = m.d();
    let r1 = hp(d, "/api/tasks", Some("obs-tok"), r#"{"command":"echo x"}"#);
    assert!(r1.status == 403 && r1.json()["required_scope"] == "Submitter");
    let r2 = hp(
        d,
        "/api/modes/run",
        Some("sub-tok"),
        r#"{"mode":"test","action":"size"}"#,
    );
    assert!(r2.status == 403 && r2.json()["required_scope"] == "Admin");
}

#[test]
fn f4_t2_dashboard_loopback_dev_bypass_blocks_proxy_and_cors() {
    let h = home();
    let m = m_dash(h.path(), &[]);
    let d = m.d();
    assert_eq!(hg(d, "/api/status", None).status, 200);
    for kv in [
        ("X-Forwarded-For", "10.0.0.1"),
        ("Origin", "https://evil.example"),
        ("Sec-Fetch-Site", "cross-site"),
    ] {
        assert_eq!(http_req(d, "GET", "/api/status", &[kv], None).status, 401);
    }
}

#[test]
fn f4_t2_dashboard_post_tasks_validation_400_and_get_404() {
    let h = home();
    let m = m_dash(h.path(), &[]);
    let d = m.d();
    for bad in [
        r#"{"command":"x","gpu":true}"#,
        r#"{"command":"  "}"#,
        r#"{"command":"x","cores":0}"#,
        r#"{"command":"x","timeout_secs":0}"#,
    ] {
        assert_eq!(hp(d, "/api/tasks", None, bad).status, 400);
    }
    assert_eq!(hg(d, "/api/tasks/not-a-uuid", None).status, 404);
    assert_eq!(
        hg(d, "/api/tasks/00000000-0000-0000-0000-000000000001", None).status,
        404
    );
}

#[test]
fn f4_t2_dashboard_members_invites_require_admin_403() {
    let h = home();
    let a = [
        "--auth",
        "members",
        "--auth-token",
        "obs-tok:obs:observer",
        "--auth-token",
        "sub-tok:sub:submitter",
    ];
    let m = m_dash(h.path(), &a);
    let d = m.d();
    for t in ["obs-tok", "sub-tok"] {
        for (m, p, b) in [
            ("GET", "/api/members", None),
            ("GET", "/api/invites", None),
            ("POST", "/api/invites", Some("{}")),
            ("DELETE", "/api/invites/none", None),
            (
                "POST",
                "/api/members/none/approve",
                Some(r#"{"pairing_code":"000000"}"#),
            ),
            ("POST", "/api/members/none/revoke", Some("{}")),
        ] {
            assert_eq!(hcall(d, m, p, Some(t), b).status, 403);
        }
    }
}

#[test]
fn f4_t2_dashboard_approve_wrong_pairing_code_rejected() {
    let h = home();
    let m = m_auth(h.path());
    #[cfg(unix)]
    {
        let d = m.d();
        let inv = inv_tok(d, r#""worker""#, 1);
        let jh = home();
        let p = PtyJoin::spawn(jh.path(), &inv);
        let wrong = if p.code() == "000000" {
            "111111"
        } else {
            "000000"
        };
        let pid = pending_id(d, "adm-tok");
        let b = format!(r#"{{"pairing_code":"{wrong}"}}"#);
        let r = hp(
            d,
            &format!("/api/members/{pid}/approve"),
            Some("adm-tok"),
            &b,
        );
        assert!(r.status == 400 || r.status == 403);
        let mj = hg_j(d, "/api/members", Some("adm-tok"));
        assert_eq!(arr(&mj["members"]).len(), 1);
    }
}

// ============================================================================
// F5: Scheduler Repeatable Straggler Re-Run (SCHED-4c) (5 T1 + 6 T2)
// ============================================================================

#[test]
fn f5_t1_submit_repeatable_succeeds() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f5-w1");
    let j = oxj(
        h.path(),
        &[
            "submit",
            "--master",
            &m.g(),
            "--repeatable",
            "--wait",
            "--json",
            "--",
            "echo",
            "rep-ok",
        ],
    );
    assert_eq!(j["exit_code"], 0);
}

#[test]
fn f5_t1_repeatable_straggler_rescued_by_fast_worker() {
    let (h, fh, sh) = (home(), home(), home());
    fs::write(sh.path().join("is_slow"), "1").unwrap();
    let m = m_ok(h.path());
    let g = m.g();
    let (_wf, _ws) = w_pair(fh.path(), sh.path(), &g, true);
    let sh_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 25 && touch \"$HOME/slow_done\"; fi; echo rescued-fast";
    prime_sh(h.path(), &g, "tier-fast", sh_s, None);
    block_fast(h.path(), &g, 3);
    let j = sub_sh_j(h.path(), &g, sh_s, &["--repeatable"], STRAG_TO);
    assert!(j["worker_name"] == "w-fast" && str_f(&j, "stdout").contains("rescued-fast"));
    assert_slow_cancelled(h.path(), &g, sh.path(), "slow_done");
}

#[test]
fn f5_t1_repeatable_sibling_failure_tolerated() {
    let (h, fh, sh) = (home(), home(), home());
    fs::write(sh.path().join("is_slow"), "1").unwrap();
    let m = m_ok(h.path());
    let g = m.g();
    let (_wf, _ws) = w_pair(fh.path(), sh.path(), &g, true);
    let sh_s =
        "if [ -f \"$HOME/is_slow\" ]; then sleep 12; exit 2; else sleep 3; echo sibling-ok; fi";
    prime_sh(h.path(), &g, "tier-fast", sh_s, None);
    block_fast(h.path(), &g, 3);
    let j = sub_sh_j(h.path(), &g, sh_s, &["--repeatable"], STRAG_TO);
    assert!(
        j["worker_name"] == "w-fast" && str_f(&j, "stdout").contains("sibling-ok"),
        "{j}"
    );
}

#[test]
fn f5_t1_non_repeatable_control_never_duplicated() {
    let (h, fh, sh) = (home(), home(), home());
    fs::write(sh.path().join("is_slow"), "1").unwrap();
    let m = m_ok(h.path());
    let g = m.g();
    let (_wf, _ws) = w_pair(fh.path(), sh.path(), &g, true);
    let sh_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 12; echo slow-ran; else echo fast-ran; fi";
    prime_sh(h.path(), &g, "tier-fast", sh_s, None);
    block_fast(h.path(), &g, 3);
    assert_eq!(
        sub_sh_j(h.path(), &g, sh_s, &[], STRAG_TO)["worker_name"],
        "w-slow"
    );
}

#[test]
fn f5_t1_mcp_submit_repeatable_supported() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f5-mcp");
    let mut mcp = McpProc::spawn(h.path(), &m.g());
    let _ = mcp.init();
    let r = mcp.call(
        2,
        "submit",
        json!({"command":"echo","args":["mcp-rep"],"repeatable":true,"wait":true}),
    );
    assert!(r["error"].is_null() && r["result"]["isError"] != true);
    assert!(mcp.close().success());
}

#[test]
fn f5_t2_repeatable_10pct_cluster_slot_copy_cap() {
    let (h, fh, sh1, sh2) = (home(), home(), home(), home());
    let p = h.path();
    fs::write(sh1.path().join("is_slow"), "1").unwrap();
    fs::write(sh2.path().join("is_slow"), "1").unwrap();
    let m = m_ok(p);
    let g = m.g();
    let _wf = w_tag(fh.path(), &g, "w-fast", "8", "8", "tier-fast");
    let _ws1 = w_tag(sh1.path(), &g, "w-slow1", "1", "1", "tier-slow");
    let _ws2 = w_tag(sh2.path(), &g, "w-slow2", "1", "1", "tier-slow");
    let fid = wid(p, &g, "w-fast");
    let sh_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 30; else if ! mkdir \"$HOME/run.lock\" 2>/dev/null; then touch \"$HOME/overlap\"; fi; sleep 2; rmdir \"$HOME/run.lock\" 2>/dev/null; fi; echo cap-ok";
    prime_sh(p, &g, "tier-fast", sh_s, None);
    let btid = sub_tid(
        p,
        &g,
        "sleep 12",
        &["--cores", "8", "--require-tag", "tier-fast"],
    );
    wait_run(p, &g, &btid);
    let tids: Vec<String> = (0..2)
        .map(|_| {
            let t = sub_tid(p, &g, sh_s, &["--repeatable"]);
            wait_run(p, &g, &t);
            t
        })
        .collect();
    for tid in &tids {
        let r = res_wait(p, &g, tid);
        assert!(
            r["exit_code"] == 0 && r["worker_id"] == fid,
            "tid {tid}: {r}"
        );
    }
    assert!(!fh.path().join("overlap").exists(), "10% copy cap violated");
}

#[test]
fn f5_t2_worker_rejects_reserved_opt_repeatable_tag() {
    let h = home();
    let a = [
        "worker",
        "--master",
        "127.0.0.1:8088",
        "--tag",
        "opt:repeatable",
    ];
    assert_bad_ox(h.path(), &[(&a, "opt:")]);
}

#[test]
fn f5_t2_submit_rejects_require_tag_opt_repeatable() {
    let h = home();
    let a = [
        "submit",
        "--require-tag",
        "opt:repeatable",
        "--",
        "echo",
        "hi",
    ];
    assert_bad_ox(h.path(), &[(&a, "opt:")]);
}

#[test]
fn f5_t2_repeatable_failing_task_no_sibling_fails_cleanly() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f5-fw");
    let a = [
        "submit",
        "--master",
        &m.g(),
        "--repeatable",
        "--wait",
        "--json",
        "--",
        "sh",
        "-c",
        "exit 7",
    ];
    assert_eq!(ox(h.path(), &a).status.code(), Some(7));
}

#[test]
fn f5_t2_repeatable_under_10s_never_duplicated() {
    let (h, fh, sh) = (home(), home(), home());
    fs::write(sh.path().join("is_slow"), "1").unwrap();
    let m = m_ok(h.path());
    let g = m.g();
    let (_wf, _ws) = w_pair(fh.path(), sh.path(), &g, true);
    let sh_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 4; echo slow-4s; else echo fast; fi";
    prime_sh(h.path(), &g, "tier-fast", sh_s, None);
    block_fast(h.path(), &g, 2);
    assert_eq!(
        sub_sh_j(h.path(), &g, sh_s, &["--repeatable"], HANG_TO)["worker_name"],
        "w-slow"
    );
}

#[test]
fn f5_t2_cancel_repeatable_task_and_no_idle_worker_no_copy() {
    let (h, fh, sh) = (home(), home(), home());
    fs::write(sh.path().join("is_slow"), "1").unwrap();
    let m = m_ok(h.path());
    let g = m.g();
    let (_wf, _ws) = w_pair(fh.path(), sh.path(), &g, true);
    let sh_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 25; fi; echo no-idle";
    prime_sh(h.path(), &g, "tier-fast", sh_s, None);
    block_fast(h.path(), &g, 25);
    let tid = sub_tid(h.path(), &g, sh_s, &["--repeatable"]);
    wait_run(h.path(), &g, &tid);
    std::thread::sleep(Duration::from_secs(12));
    assert_eq!(task_st(h.path(), &g, &tid)["status"], "Running");
    cancel_tid(h.path(), &g, &tid);
    assert_eq!(task_st(h.path(), &g, &tid)["status"], "Cancelled");
}

// ============================================================================
// F6: Scheduler Speed Surface & Host-CPU Backpressure (6 T1 + 5 T2)
// ============================================================================

#[test]
fn f6_t1_speed_hold_factor_zero_and_2_5_accepted() {
    for f in ["0", "2.5"] {
        let h = home();
        let m = start_m(
            h.path(),
            BIN_OX,
            false,
            true,
            &["--speed-hold-factor", f],
            &[],
        );
        let _w = w_ok(h.path(), &m.g(), "f6-w");
        assert_eq!(sub_ok(h.path(), &m.g(), "echo", f)["exit_code"], 0);
    }
}

#[test]
fn f6_t1_speed_aware_batch_steering_m1() {
    let h = home();
    let p = h.path();
    let m = start_m(p, BIN_OX, false, true, &["--speed-hold-factor", "1.5"], &[]);
    let g = m.g();
    let (_wf, _ws) = w_pair(p, p, &g, true);
    let sh_s = "if [ \"$(cat mode.txt)\" = 1 ]; then sleep 2.0; fi; echo steer";
    prime_pair(p, &g, sh_s);
    let ids: Vec<String> = (0..4)
        .map(|_| sub_tid(p, &g, sh_s, &["--input", "mode_fast:mode.txt"]))
        .collect();
    let fid = wid(p, &g, "w-fast");
    let fast_n = ids
        .iter()
        .filter(|id| res_wait(p, &g, id)["worker_id"] == fid)
        .count();
    assert!(fast_n >= 3, "expected w-fast >= 3/4, got {fast_n}");
}

#[test]
fn f6_t1_workers_json_shows_speed_tag() {
    let h = home();
    let p = h.path();
    let m = m_ok(p);
    let g = m.g();
    let (_wf, _ws) = w_pair(p, p, &g, false);
    prime_pair(
        p,
        &g,
        "if [ \"$(cat mode.txt)\" = 1 ]; then sleep 0.6; fi; echo sp",
    );
    let wk = oxj(p, &["workers", "--master", &g, "--json"]);
    let wf = arr(&wk).iter().find(|w| w["name"] == "w-fast").unwrap();
    let ws = arr(&wk).iter().find(|w| w["name"] == "w-slow").unwrap();
    assert!(has_tag_pfx(wf, "speed:") && worker_speed(wf).is_some_and(|s| (s - 1.0).abs() < 0.05));
    assert!(has_tag_pfx(ws, "speed:") && worker_speed(ws).is_some_and(|s| s > 1.2));
}

#[test]
fn f6_t1_dashboard_exposes_worker_speed() {
    let h = home();
    let p = h.path();
    let m = m_dash(p, &[]);
    let (g, d) = (m.g(), m.d());
    let (_wf, _ws) = w_pair(p, p, &g, false);
    prime_pair(
        p,
        &g,
        "if [ \"$(cat mode.txt)\" = 1 ]; then sleep 0.6; fi; echo sp",
    );
    let html = hg(d, "/", None);
    assert!(html.status == 200 && html.body.to_ascii_lowercase().contains("speed"));
    let (st, aw) = (hg_j(d, "/api/status", None), hg_j(d, "/api/workers", None));
    let (fs, ss) = (dash_sp(&st, &aw, "w-fast"), dash_sp(&st, &aw, "w-slow"));
    assert!((fs - 1.0).abs() < 0.05 && ss > 1.2, "fast={fs} slow={ss}");
}

#[test]
fn f6_t1_sched5_design_record_and_reference_present() {
    assert!(fs::read_dir(root().join("docs/design"))
        .unwrap()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().contains("backpressure")));
    let r = fs::read_to_string(root().join("docs/reference.md")).unwrap();
    assert!(
        r.contains("AllEligibleWorkersSaturated")
            || r.to_ascii_lowercase().contains("backpressure")
    );
}

#[test]
fn f6_t1_sched5_backpressure_holdback_and_recovery() {
    let h = home();
    let p = h.path();
    let sds = p.join("f6_sd").to_string_lossy().into_owned();
    let m = bp_m(p, true, &["--state-dir", &sds]);
    let w = w_ok(p, &m.g(), "f6-sat");
    let tid = ox_tid(
        p,
        &["submit", "--master", &m.g(), "--json", "--", "echo", "held"],
    );
    let _ = m.svc.wait_log_ci("saturated");
    assert_bp_recover(p, m, w, &sds, &tid, "held");
}

#[test]
fn f6_t2_speed_hold_factor_invalid_values_rejected() {
    let h = home();
    for bad in ["1.0", "0.5", "-1", "nan", "inf"] {
        assert_bad_ox(
            h.path(),
            &[(
                &[
                    "master",
                    "--listen",
                    "127.0.0.1:0",
                    "--speed-hold-factor",
                    bad,
                ],
                "--speed-hold-factor",
            )],
        );
    }
}

#[test]
fn f6_t2_max_host_cpu_pct_100_allows_dispatch() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f6-c100");
    ox_ok(
        h.path(),
        &[
            "submit",
            "--master",
            &m.g(),
            "--wait",
            "--",
            "echo",
            "cpu100",
        ],
    );
}

#[test]
fn f6_t2_worker_rejects_reserved_speed_tag() {
    let h = home();
    let o = ox(
        h.path(),
        &["worker", "--master", "127.0.0.1:8088", "--tag", "speed:1.0"],
    );
    assert!(!o.status.success() && txt(&o.stderr).contains("speed:"));
}

#[test]
fn f6_t2_sched5_disconnected_and_gpu_mismatch_excluded() {
    let h = home();
    let p = h.path();
    let m = bp_m(p, false, &[]);
    let g = m.g();
    let w = w_ok(p, &g, "f6-disc");
    drop(w);
    assert!(wait_until(HANG_TO, || txt(&ox(
        p,
        &["workers", "--master", &g, "--json"]
    )
    .stdout)
    .contains("state:disconnected")));
    let tid = ox_tid(p, &["submit", "--master", &g, "--json", "--", "echo", "d"]);
    std::thread::sleep(Duration::from_secs(1));
    assert!(!m.svc.log().to_ascii_lowercase().contains("saturated"));
    cancel_tid(p, &g, &tid);
    let _w2 = w_ok(p, &g, "f6-cpu");
    assert!(ox(p, &["submit", "--master", &g, "--gpu", "--json"])
        .status
        .success());
    std::thread::sleep(Duration::from_secs(2));
    assert!(!m.svc.log().to_ascii_lowercase().contains("saturated"));
    let st = oxj(p, &["status", "--master", &g, "--json"]);
    assert!(st["pending_tasks"].as_u64().unwrap_or(0) >= 1 && st["running_tasks"] == 0);
}

#[test]
fn f6_t2_sched5_anti_flood_once_per_busy_set_change() {
    let h = home();
    let m = bp_m(h.path(), false, &[]);
    let _w = w_ok(h.path(), &m.g(), "f6-fl");
    let tid = ox_tid(
        h.path(),
        &["submit", "--master", &m.g(), "--json", "--", "echo", "hold"],
    );
    let _ = m.svc.wait_log_ci("saturated");
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(
        m.svc
            .log()
            .lines()
            .filter(|l| l.to_ascii_lowercase().contains("saturated"))
            .count(),
        1
    );
    assert_eq!(task_st(h.path(), &m.g(), &tid)["status"], "Queued");
}

// ============================================================================
// F7: CLI Diagnostics, MCP & Binary/Size Guardrails (6 T1 + 6 T2)
// ============================================================================

#[test]
fn f7_t1_ox_mode_test_size_passes() {
    let h = home();
    let o = mode_run(h.path(), &["test", "size", "--json"]);
    assert!(o.status.success() && jv(&o)["success"] == true);
}

#[test]
fn f7_t1_ox_mode_doc_verify_and_research_profile_json() {
    let h = home();
    for (m, a) in [("doc", "verify"), ("research", "profile")] {
        let o = mode_run(h.path(), &[m, a, "--json"]);
        let j = jv(&o);
        assert!(o.status.success() && j["success"] == true && j["action"] == a);
    }
}

#[test]
fn f7_t1_mcp_initialize_list_and_call_tools() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f7-mcp");
    let mut mcp = McpProc::spawn(h.path(), &m.g());
    assert_eq!(mcp.init()["result"]["protocolVersion"], "2024-11-05");
    mcp.send(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}));
    let tools = mcp.recv()["result"]["tools"].as_array().cloned().unwrap();
    for n in [
        "status",
        "workers",
        "submit",
        "task_result",
        "cancel",
        "doctor",
        "workspaces",
    ] {
        assert!(tools.iter().any(|t| t["name"] == n), "missing tool {n}");
    }
    assert_ne!(mcp.call(3, "workers", json!({}))["result"]["isError"], true);
    let sr = mcp.call(
        4,
        "submit",
        json!({"command":"echo","args":["mcp-ok"],"wait":true}),
    );
    assert!(
        sr["result"]["isError"] != true
            && str_f(&sr["result"]["content"][0], "text").contains("mcp-ok")
    );
    assert_ne!(mcp.call(5, "status", json!({}))["result"]["isError"], true);
    assert!(mcp.close().success());
}

#[test]
fn f7_t1_rusty_grid_binary_alias_parity() {
    let h = home();
    let p = h.path();
    let mut rv = cmd_for(BIN_RG, p);
    rv.arg("--version");
    assert_eq!(
        txt(&ox(p, &["--version"]).stdout),
        txt(&run_to(rv, HANG_TO).stdout)
    );
    let m = start_m(p, BIN_RG, false, true, &[], &[]);
    let _w = start_w(p, BIN_RG, &m.g(), "rg-w", &[]);
    let mut sub = cmd_for(BIN_RG, p);
    sub.args([
        "submit",
        "--master",
        &m.g(),
        "--wait",
        "--json",
        "--",
        "echo",
        "rg-ok",
    ]);
    let o = run_to(sub, HANG_TO);
    assert!(o.status.success() && jv(&o)["exit_code"] == 0);
}

#[test]
fn f7_t1_doctor_json_against_live_cluster() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f7-doc");
    let d = doc_ok(h.path(), &m.g());
    let cks = arr(&d["checks"]);
    for id in ["C3", "C4", "C6", "C7"] {
        assert!(cks.iter().any(|c| c["id"] == id), "missing {id}");
    }
    assert!(!cks.iter().any(|c| c["outcome"] == "fail"));
}

#[test]
fn f7_t1_invite_and_members_cli_lifecycle() {
    let h = home();
    let p = h.path();
    let _ = gen_sec(p);
    let m = m_ok(p);
    let g = m.g();
    let inv = oxj(p, &["invite", "--master", &g, "--json"]);
    let iid = inv["invite_id"].as_str().unwrap();
    assert!(
        str_f(&inv, "invite").starts_with("oxs1-invite:")
            && arr(&inv["roles"]).iter().any(|x| x == "worker")
    );
    let ls = oxj(p, &["invite", "--list", "--master", &g, "--json"]);
    assert!(arr(&ls)
        .iter()
        .any(|i| i["invite_id"] == iid && i["revoked"] == false));
    assert_eq!(
        oxj(p, &["invite", "--revoke", iid, "--master", &g, "--json"])["revoked"],
        iid
    );
    let mems = oxj(p, &["members", "list", "--master", &g, "--json"]);
    assert!(arr(&mems).iter().any(|x| x["name"] == "local-host"));
    assert_eq!(
        oxj(p, &["members", "pending", "--master", &g, "--json"]),
        json!([])
    );
}

#[test]
fn f7_t2_ox_mode_unknown_mode_and_action_exit_1() {
    let h = home();
    assert_eq!(mode_run(h.path(), &["bogus_mode"]).status.code(), Some(1));
    let o = mode_run(h.path(), &["test", "bogus_action", "--json"]);
    assert!(o.status.code() == Some(1) && jv(&o)["success"] == false);
}

#[test]
fn f7_t2_mcp_jsonrpc_parse_method_and_param_errors() {
    let h = home();
    let mut mcp = McpProc::spawn(h.path(), "127.0.0.1:1");
    mcp.send_raw("{not valid json");
    assert_eq!(mcp.recv()["error"]["code"], -32700);
    mcp.send(&json!({"jsonrpc":"2.0","id":2,"method":"unknown/method"}));
    assert_eq!(mcp.recv()["error"]["code"], -32601);
    mcp.send(&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{}}));
    assert_eq!(mcp.recv()["error"]["code"], -32602);
    assert!(mcp.close().success());
}

#[test]
fn f7_t2_submit_invalid_flag_combinations_exit_nonzero() {
    let h = home();
    assert_bad_ox(
        h.path(),
        &[
            (&["submit", "--type", "compile"], "--source"),
            (&["submit", "--type", "3d"], "--prompt"),
            (
                &["submit", "--type", "shell", "--stdin-file", "/dev/null"],
                "--stdin-file",
            ),
            (&["submit", "--output-dir", "."], "--output-dir"),
            (
                &["submit", "--workspace", "ws", "--workspace-max-files", "0"],
                "--workspace-max-files",
            ),
        ],
    );
}

#[test]
fn f7_t2_invite_and_members_invalid_args_rejected() {
    let (h, hb) = (home(), home());
    assert_bad_ox(
        h.path(),
        &[
            (&["invite", "--roles", "superadmin"], "--roles:"),
            (&["invite", "--uses", "0"], "--uses:"),
            (&["invite", "--expires", "0s"], "--expires:"),
            (&["members", "approve", "abc", "--code", "123"], "--code:"),
        ],
    );
    let _ = gen_sec(h.path());
    let m = m_ok(h.path());
    let o = ox(hb.path(), &["invite", "--master", &m.g()]);
    assert!(!o.status.success() && txt(&o.stderr).contains("Authentication"));
}

#[test]
fn f7_t2_secret_generate_refuses_overwrite_without_force() {
    let h = home();
    let p = h.path();
    ox_ok(p, &["secret", "generate"]);
    assert!(!ox(p, &["secret", "generate"]).status.success());
    ox_ok(p, &["secret", "generate", "--force"]);
}

#[test]
fn f7_t2_doctor_expect_workers_99_exits_1() {
    let h = home();
    let m = m_ok(h.path());
    let _w = w_ok(h.path(), &m.g(), "f7-d1");
    let a = [
        "doctor",
        "--master",
        &m.g(),
        "--expect-workers",
        "99",
        "--skip-gpu",
        "--skip-spread",
        "--skip-throughput",
        "--json",
    ];
    let o = ox(h.path(), &a);
    assert!(
        o.status.code() == Some(1)
            && arr(&jv(&o)["checks"])
                .iter()
                .any(|c| c["outcome"] == "fail")
    );
}

// ============================================================================
// Tier 3: Pairwise Cross-Feature Combinations (9 tests)
// ============================================================================

#[test]
fn t3_f2_f7_hygiene_and_ox_mode_size_joint_gate() {
    let h = home();
    py_ok(h.path(), &["tests/test_m1_challenger_hygiene.py"]);
    assert!(mode_run(h.path(), &["test", "size", "--json"])
        .status
        .success());
    assert!(lock_pkgs(&root().join("Cargo.lock"))
        .into_iter()
        .filter(|(n, _, _)| n == "rustix")
        .all(|(_, v, _)| semver(&v) >= (1, 1, 6)));
}

#[test]
fn t3_f1_f4_auth_cli_submit_and_dashboard_visibility() {
    let h = home();
    let p = h.path();
    let adm = gen_sec(p);
    let m = m_dash(
        p,
        &[
            "--auth-token",
            "sub-tok:sub:submitter",
            "--auth-token",
            "obs-tok:obs:observer",
        ],
    );
    let (g, d) = (m.g(), m.d());
    let _w = w_ok(p, &g, "t3-w");
    assert_eq!(sub_ok(p, &g, "echo", "cli-t3")["exit_code"], 0);
    let post = hp(
        d,
        "/api/tasks",
        Some("sub-tok"),
        r#"{"command":"echo api-t3"}"#,
    );
    assert_eq!(post.status, 200);
    let tid = post.json()["task_id"].as_str().unwrap().to_string();
    let u = format!("/api/tasks/{tid}");
    assert!(wait_until(HANG_TO, || hg_j(d, &u, Some("obs-tok"))
        ["state"]
        == "completed"));
    assert!(
        hg_j(d, "/api/status", Some(&adm))["tasks"]["completed"]
            .as_u64()
            .unwrap_or(0)
            >= 2
    );
}

#[test]
fn t3_f1_f3_skew_self_check_against_live_cluster_flags() {
    let h = home();
    py_ok(h.path(), SKEW_SELF);
}

#[test]
fn t3_f1_f7_mcp_and_cli_cross_control_with_cancel() {
    let h = home();
    let p = h.path();
    let m = m_ok(p);
    let g = m.g();
    let _w = w_ok(p, &g, "t3-mcp");
    let mut mcp = McpProc::spawn(p, &g);
    let _ = mcp.init();
    let sub = mcp.call(2, "submit", json!({"type":"shell","interpreter":"python3","command":"import time; time.sleep(60)","wait":false}));
    let tid = serde_json::from_str::<Value>(sub["result"]["content"][0]["text"].as_str().unwrap())
        .unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_run(p, &g, &tid);
    cancel_tid(p, &g, &tid);
    assert!(mcp.call(3, "task_result", json!({"task_id":tid,"wait":true}))["error"].is_null());
    assert!(mcp.close().success());
}

#[test]
fn t3_f6_f1_speed_steering_with_placement_tag_override() {
    let h = home();
    let p = h.path();
    let m = start_m(p, BIN_OX, false, true, &["--speed-hold-factor", "1.5"], &[]);
    let g = m.g();
    let (_wf, _ws) = w_pair(p, p, &g, false);
    let sh_s = "if [ \"$(cat mode.txt)\" = 1 ]; then sleep 0.6; fi; echo override-ok";
    prime_pair(p, &g, sh_s);
    let ex = [
        "--require-tag",
        "tier-slow",
        "--input",
        "mode_fast:mode.txt",
        "--wait",
        "--json",
    ];
    let o = sub_sh(p, &g, sh_s, &ex, HANG_TO);
    assert!(o.status.success() && jv(&o)["worker_name"] == "w-slow");
}

#[test]
fn t3_f4_f1_enroll_via_dashboard_then_revoke_requeues_task() {
    let h = home();
    let m = m_auth(h.path());
    let (g, d) = (m.g(), m.d());
    #[cfg(unix)]
    {
        let inv = inv_tok(d, r#""worker""#, 2);
        let (hh, _mh) = join_approve(d, "adm-tok", &inv);
        let (jh, mr) = join_approve(d, "adm-tok", &inv);
        let ew = w_ok(jh.path(), &g, "revoked-w");
        assert_revoke_requeue(
            h.path(),
            &m,
            (jh.path(), &mr, "revoked-w"),
            (hh.path(), "healthy-w"),
            "requeued",
        );
        drop(ew);
    }
}

#[test]
fn t3_f1_f5_repeatable_with_staged_inputs_and_outputs() {
    let h = home();
    let p = h.path();
    let m = m_ok(p);
    let _w = w_ok(p, &m.g(), "t3-rio");
    fs::write(p.join("src.txt"), "rep-data").unwrap();
    assert_eq!(
        io_sub(
            p,
            &m.g(),
            "cp src.txt out.txt",
            "src.txt:src.txt",
            "out.txt",
            &["--repeatable"]
        ),
        "rep-data"
    );
}

#[test]
fn t3_f4_f6_dashboard_and_cli_speed_surface_consistency() {
    let h = home();
    let p = h.path();
    let m = m_dash(p, &[]);
    let (g, d) = (m.g(), m.d());
    let (_wf, _ws) = w_pair(p, p, &g, false);
    prime_pair(
        p,
        &g,
        "if [ \"$(cat mode.txt)\" = 1 ]; then sleep 0.6; fi; echo sp",
    );
    let cli_w = oxj(p, &["workers", "--master", &g, "--json"]);
    let (st, aw) = (hg_j(d, "/api/status", None), hg_j(d, "/api/workers", None));
    for name in ["w-fast", "w-slow"] {
        let cw = arr(&cli_w).iter().find(|w| w["name"] == name).unwrap();
        let c_sp = arr(&cw["tags"])
            .iter()
            .find_map(|t| t.as_str()?.strip_prefix("speed:")?.parse::<f64>().ok())
            .unwrap_or_else(|| panic!("missing CLI speed for {name}: {cw}"));
        let (cid, d_sp) = (wid(p, &g, name), dash_sp(&st, &aw, name));
        assert!((c_sp - d_sp).abs() < 0.15, "{name}: cli={c_sp} dash={d_sp}");
        if name == "w-fast" {
            assert!((c_sp - 1.0).abs() < 0.05 && (d_sp - 1.0).abs() < 0.05);
        } else {
            assert!(c_sp > 1.2 && d_sp > 1.2);
        }
        assert!(arr(&aw)
            .iter()
            .any(|w| w["worker_id"] == cid && w["capabilities"]["name"] == name));
    }
}

#[test]
fn t3_f2_f3_m2_citation_and_skew_ci_contract() {
    let h = home();
    assert!(root().join("CITATION.cff").is_file());
    let ci = fs::read_to_string(root().join(".github/workflows/ci.yml")).unwrap();
    assert!(ci.contains("v0.3.0") && ci.contains("release-skew"));
    py_ok(h.path(), &["tests/test_m1_challenger_hygiene.py"]);
    py_ok(h.path(), &["tests/test_release_skew.py"]);
}

// ============================================================================
// Tier 4: Realistic Multi-Feature E2E Scenarios (6 scenarios)
// ============================================================================

#[test]
fn s1_local_cluster_task_and_dashboard_telemetry_e2e() {
    let h = home();
    let p = h.path();
    let tok = gen_sec(p);
    let m = m_dash(p, &["--speed-hold-factor", "1.5"]);
    let (g, d) = (m.g(), m.d());
    let (_w1, _w2) = (w_ok(p, &g, "s1-w1"), w_ok(p, &g, "s1-w2"));
    assert_eq!(
        arr(&oxj(p, &["workers", "--master", &g, "--json"])).len(),
        2
    );
    fs::write(p.join("payload.txt"), "s1-e2e-data").unwrap();
    assert_eq!(
        io_sub(
            p,
            &g,
            "cp payload.txt result.txt",
            "payload.txt:payload.txt",
            "result.txt",
            &["--wait"]
        ),
        "s1-e2e-data"
    );
    let st = hg_j(d, "/api/status", Some(&tok));
    assert!(arr(&st["workers"]).len() == 2 && st["tasks"]["completed"].as_u64().unwrap_or(0) >= 1);
    assert!(!arr(&hg_j(d, "/api/tasks", Some(&tok))).is_empty());
    assert!(!arr(&doc_ok(p, &g)["checks"]).is_empty());
    drop(_w1);
    drop(_w2);
    stop_m(m, None);
}

#[test]
fn s2_dashboard_member_enrollment_approval_and_revocation_e2e() {
    let h = home();
    let m = m_auth(h.path());
    let (g, d) = (m.g(), m.d());
    #[cfg(unix)]
    {
        let (sh, _ms) = join_approve(d, "adm-tok", &inv_tok(d, r#""worker""#, 1));
        let (jh, mj) = join_approve(d, "adm-tok", &inv_tok(d, r#""worker","client""#, 1));
        let wj = w_ok(jh.path(), &g, "s2-join");
        ox_ok(
            jh.path(),
            &["submit", "--master", &g, "--wait", "--", "echo", "s2-ok"],
        );
        assert_revoke_requeue(
            h.path(),
            &m,
            (jh.path(), &mj, "s2-join"),
            (sh.path(), "s2-stay"),
            "s2-requeued",
        );
        assert_eq!(
            ox(jh.path(), &["status", "--master", &g]).status.code(),
            Some(6)
        );
        drop(wj);
    }
}

#[test]
fn s3_heterogeneous_speed_and_repeatable_straggler_pipeline_e2e() {
    let (h, fh, sh) = (home(), home(), home());
    let p = h.path();
    fs::write(sh.path().join("is_slow"), "1").unwrap();
    let m = m_dash(p, &["--speed-hold-factor", "1.5"]);
    let g = m.g();
    let (_wf, _ws) = w_pair(fh.path(), sh.path(), &g, true);
    let sp_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 1.0; fi; echo s3-done";
    prime_sh(p, &g, "tier-fast", sp_s, None);
    prime_sh(p, &g, "tier-slow", sp_s, None);
    let wk = oxj(p, &["workers", "--master", &g, "--json"]);
    let wf = arr(&wk).iter().find(|w| w["name"] == "w-fast").unwrap();
    let ws = arr(&wk).iter().find(|w| w["name"] == "w-slow").unwrap();
    assert!(has_tag_pfx(wf, "speed:") && worker_speed(wf).is_some_and(|s| (s - 1.0).abs() < 0.05));
    assert!(has_tag_pfx(ws, "speed:") && worker_speed(ws).is_some_and(|s| s > 1.2));
    let rep_s = "if [ -f \"$HOME/is_slow\" ]; then sleep 25 && touch \"$HOME/s3_slow_done\"; fi; echo s3-rescued";
    prime_sh(p, &g, "tier-fast", rep_s, None);
    block_fast(p, &g, 3);
    let j = sub_sh_j(p, &g, rep_s, &["--repeatable"], STRAG_TO);
    assert!(j["worker_name"] == "w-fast" && str_f(&j, "stdout").contains("s3-rescued"));
    assert_slow_cancelled(p, &g, sh.path(), "s3_slow_done");
}

#[test]
fn s4_cpu_backpressure_saturation_recovery_and_overload_e2e() {
    let h = home();
    let p = h.path();
    let sds = p.join("s4_sd").to_string_lossy().into_owned();
    let m = bp_m(p, true, &["--state-dir", &sds]);
    let w = w_ok(p, &m.g(), "s4-w");
    assert_eq!(
        hp(m.d(), "/api/tasks", None, r#"{"command":""}"#).status,
        400
    );
    let tid = ox_tid(
        p,
        &[
            "submit",
            "--master",
            &m.g(),
            "--json",
            "--",
            "echo",
            "s4-held",
        ],
    );
    let _ = m.svc.wait_log_ci("saturated");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        m.svc
            .log()
            .lines()
            .filter(|l| l.to_ascii_lowercase().contains("saturated"))
            .count(),
        1
    );
    assert_bp_recover(p, m, w, &sds, &tid, "s4-held");
}

#[test]
fn s5a_release_readiness_hygiene_and_skew_baseline_e2e() {
    let h = home();
    let p = h.path();
    py_ok(p, &["tests/test_m1_challenger_hygiene.py"]);
    py_ok(p, &["tests/test_release_skew.py"]);
    let sum = p.join("s5a.md");
    let so = py_in(p, &root(), &[("GITHUB_STEP_SUMMARY", &sum)], SKEW_SELF);
    assert!(
        so.status.success()
            && fs::read_to_string(&sum)
                .unwrap()
                .contains("### Release skew")
    );
    assert!(mode_run(p, &["test", "size", "--json"]).status.success());
    let backlog = fs::read_to_string(root().join("docs/BACKLOG.md")).unwrap();
    let rows = backlog_rows(&backlog).len();
    assert!(
        backlog
            .lines()
            .rfind(|l| l.contains("Recounted on") && l.contains("items,"))
            .is_some_and(|l| l.contains(&format!("{rows} items,"))),
        "the newest BACKLOG recount line must name {rows} items"
    );
    for rel in ["Cargo.lock", "fuzz/Cargo.lock"] {
        assert!(lock_pkgs(&root().join(rel))
            .into_iter()
            .filter(|(n, _, _)| n == "rustix")
            .all(|(_, v, _)| semver(&v) >= (1, 1, 6)));
    }
    assert!(fs::read_to_string(root().join("README.md"))
        .unwrap()
        .contains("benchmark only"));
}

#[test]
fn s5b_release_readiness_m2_citation_and_skew_pin_e2e() {
    let cff = fs::read_to_string(root().join("CITATION.cff")).unwrap();
    let cargo = fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let version = cargo
        .split("[workspace.package]")
        .nth(1)
        .and_then(|sec| sec.lines().find(|l| l.trim_start().starts_with("version")))
        .and_then(|l| l.split('"').nth(1))
        .expect("[workspace.package] version in Cargo.toml");
    assert!(
        cff.contains("cff-version: 1.2.0") && cff.contains(&format!("version: \"{version}\"")),
        "CITATION.cff must name the workspace version {version}"
    );
    let ci = fs::read_to_string(root().join(".github/workflows/ci.yml")).unwrap();
    assert!(ci.contains("v0.3.0") && ci.contains("release-skew"));
}
