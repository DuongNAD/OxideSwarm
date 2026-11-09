#!/usr/bin/env python3
"""
benchmarks/grid/bench_grid.py - benchmarks of the OxideSwarm grid (BENCH-1 a, b and c).

  (a) no-op task throughput and submit-to-result latency with 1..4 workers (loopback);
  (b) a first and a repeated upload of the same input tree (bytes sent, time, loopback);
  (c) end-to-end offload of `cargo test` in a named workspace, cold and warm, against a two-machine
      cluster (`--offload-master <ADDR_OR_TICKET>`).

Standard library only. Drives the built `oxideswarm` binary; starts its own master and workers on
127.0.0.1 with ephemeral ports for (a) and (b), gives every process a private HOME/USERPROFILE (the
real ~/.oxideswarm is never read or written unless `--secret-file` explicitly names a file to copy),
stops spawned processes by their PIDs, and removes its temporary directory.
Read benchmarks/grid/README.md for what each number means and what it does not show.

  python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm
  python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm --quick   # smoke run
  python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm \\
      --offload-master <HOST:PORT_OR_TICKET> --secret-file ~/.oxideswarm/cluster.secret
"""

import argparse
import concurrent.futures
import datetime
import json
import os
import platform
import random
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
IS_WINDOWS = os.name == "nt"
# The no-op task: exits 0 at once, on every platform.
NOOP = ["cmd", "/c", "exit 0"] if IS_WINDOWS else ["true"]


# ---------------------------------------------------------------- statistics (pure, unit-tested)

def percentile(values, q):
    """Nearest-rank percentile (q in 0..100) of a non-empty list."""
    if not values:
        raise ValueError("percentile of an empty list")
    s = sorted(values)
    rank = max(1, -(-len(s) * q // 100))  # ceil(len * q / 100)
    return s[int(rank) - 1]


def summarize(values):
    """Median, min, max of a list of per-run figures."""
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "runs": len(values),
    }


UPLOAD_RE = re.compile(
    r"Inputs: (\d+) files \(([^)]*)\); sent (\d+) of (\d+) blobs \(([^)]*)\), (\d+) already on the master"
)


def parse_inputs_line(text):
    """Parse the `Inputs: ...` line `submit --input` prints on stderr; None when absent."""
    m = UPLOAD_RE.search(text)
    if not m:
        return None
    return {
        "files": int(m.group(1)),
        "tree_size": m.group(2),
        "blobs_sent": int(m.group(3)),
        "blobs_total": int(m.group(4)),
        "bytes_sent_reported": m.group(5),
        "blobs_already_on_master": int(m.group(6)),
    }


WORKSPACE_RE = re.compile(
    r"Workspace '([^']*)': (\d+) files \(([^)]*)\); sent (\d+) of (\d+) blobs \(([^)]*)\), (\d+) already on the master"
)

EXEC_TIME_RE = re.compile(r"Execution Time:\s*(\d+)ms")


def parse_workspace_line(text):
    """Parse the `Workspace '<name>': ...` line `submit --workspace` prints on stderr; None when absent."""
    m = WORKSPACE_RE.search(text)
    if not m:
        return None
    return {
        "workspace": m.group(1),
        "files": int(m.group(2)),
        "tree_size": m.group(3),
        "blobs_sent": int(m.group(4)),
        "blobs_total": int(m.group(5)),
        "bytes_sent_reported": m.group(6),
        "blobs_already_on_master": int(m.group(7)),
    }


def parse_execution_time_ms(text):
    """Parse `Execution Time: <N>ms` from `submit --wait` stdout; None when absent."""
    m = EXEC_TIME_RE.search(text)
    return int(m.group(1)) if m else None


def sanitize_worker_caps(worker):
    """Extract non-identifying hardware/link facts from a `workers --json` entry (never `name` or `id`)."""
    tags = worker.get("tags") or []
    tag_map = {}
    tools = []
    features = []
    for t in tags:
        if not isinstance(t, str) or ":" not in t:
            continue
        k, v = t.split(":", 1)
        if k == "tool":
            tools.append(v)
        elif k == "feat":
            features.append(v)
        elif k in ("os", "arch", "link", "iso", "state"):
            tag_map[k] = v
    return {
        "os": tag_map.get("os", "unknown"),
        "arch": tag_map.get("arch", "unknown"),
        "link": worker.get("link") or tag_map.get("link", "unknown"),
        "isolation": tag_map.get("iso", "off"),
        "cpu_cores": worker.get("cpu_cores"),
        "ram_mb": worker.get("ram_mb"),
        "has_gpu": worker.get("has_gpu"),
        "is_simulated_gpu": worker.get("is_simulated_gpu"),
        "tools": sorted(tools),
        "features": sorted(features),
    }


def summarize_c_runs(c_runs):
    """Aggregate cold and warm workspace offload runs into median/min/max summaries."""
    summary = {}
    for phase in ("cold", "warm"):
        phase_rows = [r[phase] for r in c_runs if phase in r]
        if not phase_rows:
            continue
        entry = {
            "wall_s": summarize([p["wall_s"] for p in phase_rows]),
            "blobs_sent": summarize([p["blobs_sent"] for p in phase_rows]),
            "blobs_total": summarize([p["blobs_total"] for p in phase_rows]),
        }
        exec_ms = [p["worker_execution_time_ms"] for p in phase_rows if p.get("worker_execution_time_ms") is not None]
        if exec_ms:
            entry["worker_execution_time_ms"] = summarize(exec_ms)
        if phase == "warm":
            entry["all_reused_target"] = all(bool(p.get("reused_target")) for p in phase_rows)
        summary[phase] = entry
    return summary


def dir_bytes(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


# ---------------------------------------------------------------- cluster plumbing

class Cluster:
    """One master plus k workers, every process with its own private HOME."""

    def __init__(self, binary, workdir, workers):
        self.binary = binary
        self.workdir = workdir
        self.procs = []
        self.addr = None
        self.blob_dir = os.path.join(workdir, "blobs")
        self.empty_config = os.path.join(workdir, "empty.toml")
        with open(self.empty_config, "w") as f:
            f.write("")
        self.n_workers = workers
        self.env = self._env(os.path.join(workdir, "home-client"))

    def _env(self, home):
        os.makedirs(home, exist_ok=True)
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("RUSTY_GRID_")
            and k not in ("OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE", "OXIDE_DEV_MODE",
                          "OXIDE_SWARM_DASHBOARD_PORT", "OXIDE_SWARM_DASHBOARD_PORT_FILE")
        }
        env.update(HOME=home, USERPROFILE=home, RUSTY_GRID_CONFIG=self.empty_config,
                   RUSTY_GRID_NO_BATTERY="1")
        return env

    def _spawn(self, name, args):
        log = open(os.path.join(self.workdir, name + ".log"), "wb")
        kw = {} if IS_WINDOWS else {"start_new_session": True}
        p = subprocess.Popen([self.binary] + args, stdout=log, stderr=subprocess.STDOUT,
                             env=self._env(os.path.join(self.workdir, "home-" + name)), **kw)
        self.procs.append((name, p, log))
        return p

    def start(self):
        port_file = os.path.join(self.workdir, "master.port")
        dash_file = os.path.join(self.workdir, "dash.port")
        master = self._spawn("master", [
            "master", "--listen", "127.0.0.1:0", "--port-file", port_file,
            "--dashboard-port", "0", "--dashboard-port-file", dash_file,
            "--blob-dir", self.blob_dir])
        deadline = time.time() + 30
        while time.time() < deadline:
            if master.poll() is not None:
                raise SystemExit("master exited early; see " + os.path.join(self.workdir, "master.log"))
            if os.path.exists(port_file) and os.path.getsize(port_file) > 0:
                txt = open(port_file).read().strip()
                if txt:
                    self.addr = txt if ":" in txt else "127.0.0.1:" + txt
                    break
            time.sleep(0.05)
        else:
            raise SystemExit("master did not publish its port within 30 s (is another process blocking?)")
        for i in range(self.n_workers):
            self._spawn("worker%d" % i, ["worker", "--master", self.addr, "--name", "w%d" % i,
                                          "--no-portal", "--no-gpu"])
        deadline = time.time() + 60
        while time.time() < deadline:
            r = self.run(["workers", "--json"])
            if r.returncode == 0:
                try:
                    if len(json.loads(r.stdout)) >= self.n_workers:
                        return
                except ValueError:
                    pass
            for name, p, _ in self.procs:
                if p.poll() is not None:
                    raise SystemExit("%s exited early; see its log in %s" % (name, self.workdir))
            time.sleep(0.1)
        raise SystemExit("workers did not register within 60 s")

    def run(self, args, timeout=120):
        return subprocess.run([self.binary] + args[:1] + ["--master", self.addr] + args[1:],
                              capture_output=True, text=True, env=self.env, timeout=timeout)

    def stop(self):
        for _name, p, _log in reversed(self.procs):
            if p.poll() is None:
                p.terminate()
        for _name, p, log in self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            log.close()
        self.procs = []


def timed(cluster, args):
    t0 = time.perf_counter()
    r = cluster.run(args)
    return time.perf_counter() - t0, r


def noop_submit(cluster):
    dt, r = timed(cluster, ["submit", "--command", NOOP[0], "--wait", "--"] + NOOP[1:])
    if r.returncode != 0:
        raise SystemExit("no-op task failed (exit %d): %s%s" % (r.returncode, r.stdout, r.stderr))
    return dt


def floor_call(cluster):
    dt, r = timed(cluster, ["status"])
    if r.returncode != 0:
        raise SystemExit("status failed: %s%s" % (r.stdout, r.stderr))
    return dt


def ms(v):
    return round(v * 1000.0, 3)


# ---------------------------------------------------------------- (a) throughput and latency

def measure_a(binary, k, n_lat, n_thr, conc, tmp):
    wd = tempfile.mkdtemp(prefix="a%d-" % k, dir=tmp)
    c = Cluster(binary, wd, k)
    try:
        c.start()
        for _ in range(5):  # warm up connections, worker caches and page cache
            noop_submit(c)
        floor = [floor_call(c) for _ in range(n_lat)]
        lat = [noop_submit(c) for _ in range(n_lat)]
        with concurrent.futures.ThreadPoolExecutor(conc) as ex:
            t0 = time.perf_counter()
            list(ex.map(lambda _i: noop_submit(c), range(n_thr)))
            wall = time.perf_counter() - t0
            t1 = time.perf_counter()
            list(ex.map(lambda _i: floor_call(c), range(n_thr)))
            floor_wall = time.perf_counter() - t1
        return {
            "workers": k,
            "latency_ms": {"p50": ms(percentile(lat, 50)), "p99": ms(percentile(lat, 99)),
                           "min": ms(min(lat)), "max": ms(max(lat)), "samples": len(lat)},
            "floor_latency_ms": {"p50": ms(percentile(floor, 50)), "p99": ms(percentile(floor, 99)),
                                 "samples": len(floor)},
            "throughput_tasks_per_s": round(n_thr / wall, 2),
            "floor_throughput_calls_per_s": round(n_thr / floor_wall, 2),
            "throughput_tasks": n_thr,
            "throughput_wall_s": round(wall, 3),
            "client_concurrency": conc,
        }
    finally:
        c.stop()


# ---------------------------------------------------------------- (b) first and repeated upload

def make_tree(root, files, seed, max_size):
    rng = random.Random(seed)
    os.makedirs(root, exist_ok=True)
    total = 0
    for i in range(files):
        d = os.path.join(root, "d%02d" % (i % 10))
        os.makedirs(d, exist_ok=True)
        size = rng.randint(1, max_size)
        with open(os.path.join(d, "f%04d.bin" % i), "wb") as f:
            f.write(rng.randbytes(size) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(size)))
        total += size
    return total


def submit_tree(c, tree):
    dt, r = timed(c, ["submit", "--input", tree, "--command", NOOP[0], "--wait", "--"] + NOOP[1:])
    if r.returncode != 0:
        raise SystemExit("input submit failed (exit %d): %s%s" % (r.returncode, r.stdout, r.stderr))
    parsed = parse_inputs_line(r.stderr)
    if parsed is None:
        raise SystemExit("no `Inputs:` line in submit output: " + r.stderr)
    return dt, parsed


def measure_b(binary, files, max_size, seed, tmp):
    wd = tempfile.mkdtemp(prefix="b-", dir=tmp)
    tree = os.path.join(wd, "tree")
    total = make_tree(tree, files, seed, max_size)
    c = Cluster(binary, wd, 1)
    try:
        c.start()
        noop_submit(c)  # warm up
        before = dir_bytes(c.blob_dir)
        t_first, p_first = submit_tree(c, tree)
        stored_first = dir_bytes(c.blob_dir) - before
        t_rep, p_rep = submit_tree(c, tree)
        stored_rep = dir_bytes(c.blob_dir) - before - stored_first
        victim = os.path.join(tree, "d00", "f0000.bin")
        with open(victim, "ab") as f:
            f.write(b"x")
        t_one, p_one = submit_tree(c, tree)
        stored_one = dir_bytes(c.blob_dir) - before - stored_first - stored_rep
        step = lambda t, p, stored: dict(wall_s=round(t, 4), master_blob_store_growth_bytes=stored, **p)
        return {
            "tree_files": files,
            "tree_bytes": total,
            "seed": seed,
            "first": step(t_first, p_first, stored_first),
            "repeat_same": step(t_rep, p_rep, stored_rep),
            "repeat_one_file_changed": step(t_one, p_one, stored_one),
        }
    finally:
        c.stop()


# ---------------------------------------------------------------- (c) two-machine workspace offload

def _client_env(home, empty_config, secret_file=None):
    os.makedirs(home, exist_ok=True)
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("RUSTY_GRID_")
        and k not in ("OXIDESWARM_SECRET_FILE", "OXIDE_DEV_MODE", "OXIDESWARM_MASTER",
                      "OXIDESWARM_P2P_TICKET", "OXIDE_SWARM_DASHBOARD_PORT",
                      "OXIDE_SWARM_DASHBOARD_PORT_FILE")
    }
    env.update(
        HOME=home,
        USERPROFILE=home,
        OXIDESWARM_HOME=os.path.join(home, ".oxideswarm"),
        RUSTY_GRID_CONFIG=empty_config,
    )
    if secret_file:
        env["OXIDESWARM_SECRET_FILE"] = os.path.abspath(secret_file)
    return env


def _run_offload_step(binary, master_addr, crate_dir, ws_name, cargo_args, env, insecure=False, timeout=900):
    cmd = [binary, "submit", "--master", master_addr]
    if insecure:
        cmd.append("--insecure")
    cmd += [
        "--workspace", ws_name,
        "--env", "CARGO_TERM_COLOR=never",
        "--wait",
        "--", "cargo",
    ] + cargo_args
    t0 = time.perf_counter()
    r = subprocess.run(cmd, cwd=crate_dir, capture_output=True, text=True, env=env, timeout=timeout)
    wall = time.perf_counter() - t0
    if r.returncode != 0:
        raise SystemExit(
            "workspace offload failed (exit %d) in %s:\n%s%s"
            % (r.returncode, ws_name, r.stdout, r.stderr)
        )
    parsed = parse_workspace_line(r.stderr)
    if parsed is None:
        raise SystemExit("no `Workspace '...'` line in submit output: " + r.stderr)
    combined = (r.stdout or "") + "\n" + (r.stderr or "")
    exec_ms = parse_execution_time_ms(r.stdout or "")
    return {
        "wall_s": round(wall, 4),
        "worker_execution_time_ms": exec_ms,
        "reused_target": "Compiling " not in combined,
        **parsed,
    }


def query_remote_workers(binary, master_addr, env, insecure=False):
    cmd = [binary, "workers", "--master", master_addr, "--json"]
    if insecure:
        cmd.append("--insecure")
    r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=60)
    if r.returncode != 0:
        raise SystemExit("workers --json failed (exit %d): %s%s" % (r.returncode, r.stdout, r.stderr))
    raw = json.loads(r.stdout)
    return [sanitize_worker_caps(w) for w in raw if isinstance(w, dict)]


def measure_c(binary, master_addr, crate_dir, ws_prefix, cargo_args, run_idx, tmp, secret_file=None, insecure=False):
    wd = tempfile.mkdtemp(prefix="c%d-" % run_idx, dir=tmp)
    empty_config = os.path.join(wd, "empty.toml")
    with open(empty_config, "w") as f:
        f.write("")
    env = _client_env(os.path.join(wd, "home-client"), empty_config, secret_file=secret_file)
    workers = query_remote_workers(binary, master_addr, env, insecure=insecure)
    suffix = "%06x" % random.SystemRandom().randint(0, 0xFFFFFF)
    ws_name = "%s-r%d-%s" % (_slug(ws_prefix)[:20] or "bench1c", run_idx, suffix)
    cold = _run_offload_step(binary, master_addr, crate_dir, ws_name, cargo_args, env, insecure=insecure)
    warm = _run_offload_step(binary, master_addr, crate_dir, ws_name, cargo_args, env, insecure=insecure)
    return {
        "workspace": ws_name,
        "cargo_args": cargo_args,
        "workers_snapshot": workers,
        "cold": cold,
        "warm": warm,
    }


# ---------------------------------------------------------------- environment record

def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return ""


def hardware():
    info = {"cpu_model": platform.processor() or platform.machine(), "cores": os.cpu_count(), "ram_bytes": None}
    sysname = platform.system()
    if sysname == "Darwin":
        info["cpu_model"] = sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or info["cpu_model"]
        mem = sh(["sysctl", "-n", "hw.memsize"])
        info["ram_bytes"] = int(mem) if mem.isdigit() else None
    elif sysname == "Linux":
        try:
            for line in open("/proc/cpuinfo"):
                if line.startswith("model name"):
                    info["cpu_model"] = line.split(":", 1)[1].strip()
                    break
            for line in open("/proc/meminfo"):
                if line.startswith("MemTotal"):
                    info["ram_bytes"] = int(line.split()[1]) * 1024
                    break
        except OSError:
            pass
    elif sysname == "Windows":
        out = sh(["powershell", "-NoProfile", "-Command",
                  "(Get-CimInstance Win32_Processor).Name; (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"])
        lines = [l for l in out.splitlines() if l.strip()]
        if len(lines) >= 2 and lines[-1].strip().isdigit():
            info["cpu_model"], info["ram_bytes"] = lines[0].strip(), int(lines[-1])
    return info


def power_state():
    if platform.system() == "Darwin":
        return sh(["pmset", "-g", "batt"]).splitlines()[0:2]
    if platform.system() == "Linux":
        base = "/sys/class/power_supply"
        try:
            return {n: open(os.path.join(base, n, "status")).read().strip()
                    for n in os.listdir(base) if os.path.exists(os.path.join(base, n, "status"))} or "no battery found"
        except OSError:
            return "unknown"
    return "unknown (not recorded on this platform)"


def os_info():
    """Generic OS facts only. Never the host name, the user name or a path."""
    return {"system": platform.system(), "release": platform.release(), "version": platform.version(),
            "machine": platform.machine()}


def _slug(text):
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-") or "unknown"


def result_path(directory, when, os_facts, hw):
    """<UTC date>-<os>-<arch>-<cpu short>.json, with a numeric suffix if it exists. No host name."""
    system = {"darwin": "macos"}.get(os_facts["system"].lower(), os_facts["system"].lower())
    cpu = _slug(re.sub(r"\(R\)|\(TM\)|CPU|Processor|@.*$", " ", hw.get("cpu_model") or ""))[:30].strip("-")
    base = "%s-%s-%s-%s" % (when.strftime("%Y-%m-%d"), _slug(system), _slug(os_facts["machine"]), cpu or "cpu")
    path, n = os.path.join(directory, base + ".json"), 1
    while os.path.exists(path):
        n += 1
        path = os.path.join(directory, "%s-%d.json" % (base, n))
    return path


def git_info():
    commit = sh(["git", "-C", REPO, "rev-parse", "HEAD"])
    dirty = sh(["git", "-C", REPO, "status", "--porcelain", "--", ":!benchmarks/grid/results"])
    return {"commit": commit or "unknown", "dirty_tree": bool(dirty)}


def load_avg():
    try:
        return [round(x, 2) for x in os.getloadavg()]
    except (AttributeError, OSError):
        return None


def build_profile(binary):
    p = binary.replace("\\", "/").lower()
    return "release" if "/release/" in p else "debug" if "/debug/" in p else "unknown"


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    default_bin = os.path.join(REPO, "target", "release", "oxideswarm" + (".exe" if IS_WINDOWS else ""))
    ap.add_argument("--bin", default=default_bin, help="oxideswarm binary (default: target/release/oxideswarm)")
    ap.add_argument("--runs", type=int, default=3, help="repetitions of every measurement (default 3)")
    ap.add_argument("--latency-samples", type=int, default=100, help="sequential tasks per run for p50/p99")
    ap.add_argument("--throughput-tasks", type=int, default=300, help="tasks per throughput run")
    ap.add_argument("--concurrency", type=int, default=16, help="client processes in flight (default 16)")
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--files", type=int, default=500, help="files in the (b) input tree")
    ap.add_argument("--max-file-bytes", type=int, default=65536)
    ap.add_argument("--seed", type=int, default=20261109)
    ap.add_argument("--quick", action="store_true", help="small smoke run: 1 run, 1 and 2 workers, tiny sizes")
    ap.add_argument("--only", choices=("ab", "c", "all"),
                    help="which benchmarks to run (default: 'c' if --offload-master is set, else 'ab')")
    ap.add_argument("--offload-master",
                    help="master host:port or P2P ticket for (c) two-machine workspace cargo test offload")
    ap.add_argument("--offload-dir", default=REPO,
                    help="crate/workspace directory to sync for (c) (default: repository root)")
    ap.add_argument("--offload-cargo-args", default="test -p rusty_grid_core --lib --offline",
                    help="cargo arguments for (c) (default: 'test -p rusty_grid_core --lib --offline')")
    ap.add_argument("--offload-workspace-prefix", default="bench1c",
                    help="workspace name prefix for (c) (default: bench1c)")
    ap.add_argument("--secret-file",
                    help="cluster.secret path for authenticated two-machine (c) runs")
    ap.add_argument("--insecure", action="store_true",
                    help="pass --insecure to client commands in (c)")
    ap.add_argument("--out", help="results file (default benchmarks/grid/results/<date>-<os>-<arch>-<cpu>.json)")
    ap.add_argument("--no-write", action="store_true", help="print the record instead of writing it")
    ap.add_argument("--label", default="", help="free-text note stored in the record")
    args = ap.parse_args()

    mode = args.only or ("c" if args.offload_master else "ab")
    if mode in ("c", "all") and not args.offload_master:
        raise SystemExit("--offload-master <HOST:PORT_OR_TICKET> is required when running benchmark (c)")

    if args.quick:
        args.runs, args.latency_samples, args.throughput_tasks = 1, 10, 20
        args.concurrency, args.max_workers, args.files, args.max_file_bytes = 4, 2, 20, 2048
    binary = os.path.abspath(args.bin)
    if not os.path.isfile(binary):
        raise SystemExit("binary not found: %s (build it: cargo build --release --bin oxideswarm)" % binary)

    offload_dir = os.path.abspath(args.offload_dir)
    cargo_args = [tok for tok in args.offload_cargo_args.split() if tok]
    tmp = tempfile.mkdtemp(prefix="oxbench-")
    started = datetime.datetime.now(datetime.timezone.utc)
    load_before = load_avg()
    try:
        a_runs = {k: [] for k in range(1, args.max_workers + 1)} if mode in ("ab", "all") else {}
        b_runs = []
        c_runs = []
        # Interleave the repetitions so slow drift (other processes, thermals) spreads over all rows.
        for run in range(args.runs):
            if mode in ("ab", "all"):
                for k in a_runs:
                    print("[a] run %d/%d, %d worker(s)" % (run + 1, args.runs, k), flush=True)
                    a_runs[k].append(measure_a(binary, k, args.latency_samples, args.throughput_tasks,
                                               args.concurrency, tmp))
                print("[b] run %d/%d" % (run + 1, args.runs), flush=True)
                b_runs.append(measure_b(binary, args.files, args.max_file_bytes, args.seed, tmp))
            if mode in ("c", "all"):
                print("[c] run %d/%d (cold + warm workspace offload)" % (run + 1, args.runs), flush=True)
                c_runs.append(measure_c(
                    binary, args.offload_master, offload_dir, args.offload_workspace_prefix,
                    cargo_args, run + 1, tmp, secret_file=args.secret_file, insecure=args.insecure
                ))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    a_summary = []
    for k, runs in a_runs.items():
        a_summary.append({
            "workers": k,
            "throughput_tasks_per_s": summarize([r["throughput_tasks_per_s"] for r in runs]),
            "floor_throughput_calls_per_s": summarize([r["floor_throughput_calls_per_s"] for r in runs]),
            "latency_p50_ms": summarize([r["latency_ms"]["p50"] for r in runs]),
            "latency_p99_ms": summarize([r["latency_ms"]["p99"] for r in runs]),
            "floor_latency_p50_ms": summarize([r["floor_latency_ms"]["p50"] for r in runs]),
            "floor_latency_p99_ms": summarize([r["floor_latency_ms"]["p99"] for r in runs]),
        })
    b_summary = {}
    if b_runs:
        for key in ("first", "repeat_same", "repeat_one_file_changed"):
            b_summary[key] = {
                "wall_s": summarize([r[key]["wall_s"] for r in b_runs]),
                "blobs_sent": summarize([r[key]["blobs_sent"] for r in b_runs]),
                "master_blob_store_growth_bytes": summarize([r[key]["master_blob_store_growth_bytes"] for r in b_runs]),
            }
    c_summary = summarize_c_runs(c_runs) if c_runs else {}

    version = sh([binary, "--version"])
    bench_title = {
        "ab": "BENCH-1 (a) task overhead and (b) repeated upload",
        "c": "BENCH-1 (c) two-machine workspace cargo test offload",
        "all": "BENCH-1 (a) task overhead, (b) repeated upload, and (c) workspace offload",
    }[mode]
    network_desc = (
        "two-machine cluster via client -> master -> workspace worker"
        if mode == "c"
        else "loopback (127.0.0.1), master and workers on this machine"
    )
    record = {
        "benchmark": bench_title,
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": git_info(),
        "binary": {"version": version, "build_profile": build_profile(binary)},
        "hardware": hardware(),
        "os": os_info(),
        "network": network_desc,
        "power": power_state(),
        "load_average_before": load_before,
        "load_average_after": load_avg(),
        "python": platform.python_version(),
        "note": args.label,
        "parameters": {k: getattr(args, k) for k in ("runs", "latency_samples", "throughput_tasks",
                                                       "concurrency", "max_workers", "files",
                                                       "max_file_bytes", "seed", "quick")},
    }
    if a_summary:
        record["a_summary"] = a_summary
    if b_summary:
        record["b_summary"] = b_summary
    if c_summary:
        record["c_summary"] = c_summary
        record["remote_workers"] = c_runs[0].get("workers_snapshot", [])
        record["offload_cargo_args"] = cargo_args
    raw_block = {}
    if a_runs:
        raw_block["a"] = [r for runs in a_runs.values() for r in runs]
    if b_runs:
        raw_block["b"] = b_runs
    if c_runs:
        raw_block["c"] = c_runs
    record["raw"] = raw_block

    text = json.dumps(record, indent=2)
    if args.no_write:
        print(text)
        return
    out = args.out
    if not out:
        out = result_path(os.path.join(HERE, "results"), started, record["os"], record["hardware"])
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w") as f:
        f.write(text + "\n")
    print("wrote", out)
    for row in a_summary:
        print("(a) %d worker(s): %.1f tasks/s (client floor %.1f/s), p50 %.1f ms (floor %.1f), p99 %.1f ms" % (
            row["workers"], row["throughput_tasks_per_s"]["median"], row["floor_throughput_calls_per_s"]["median"],
            row["latency_p50_ms"]["median"], row["floor_latency_p50_ms"]["median"], row["latency_p99_ms"]["median"]))
    for key, row in b_summary.items():
        print("(b) %-24s %.3f s, blobs sent %s, store growth %s B" % (
            key, row["wall_s"]["median"], row["blobs_sent"]["median"], row["master_blob_store_growth_bytes"]["median"]))
    for phase, row in c_summary.items():
        exec_str = (" (worker %s ms)" % row["worker_execution_time_ms"]["median"]
                    if "worker_execution_time_ms" in row else "")
        reused = (", reused_target=%s" % row["all_reused_target"]) if "all_reused_target" in row else ""
        print("(c) %-24s %.3f s%s, blobs sent %s/%s%s" % (
            phase, row["wall_s"]["median"], exec_str,
            row["blobs_sent"]["median"], row["blobs_total"]["median"], reused))


if __name__ == "__main__":
    main()
