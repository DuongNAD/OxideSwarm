#!/usr/bin/env python3
"""
benchmarks/grid/bench_grid.py - loopback benchmarks of the OxideSwarm grid (BENCH-1 a and b).

  (a) no-op task throughput and submit-to-result latency with 1..4 workers;
  (b) a first and a repeated upload of the same input tree (bytes sent, time).

Standard library only. Drives the built `oxideswarm` binary; starts its own master and workers on
127.0.0.1 with ephemeral ports, a private HOME/USERPROFILE per process (the real ~/.oxideswarm is
never read or written), stops them by the PIDs it started, and removes its temporary directory.
Read benchmarks/grid/README.md for what each number means and what it does not show.

  python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm
  python3 benchmarks/grid/bench_grid.py --bin target/release/oxideswarm --quick   # smoke run
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
    ap.add_argument("--out", help="results file (default benchmarks/grid/results/<date>-<os>-<arch>-<cpu>.json)")
    ap.add_argument("--no-write", action="store_true", help="print the record instead of writing it")
    ap.add_argument("--label", default="", help="free-text note stored in the record")
    args = ap.parse_args()

    if args.quick:
        args.runs, args.latency_samples, args.throughput_tasks = 1, 10, 20
        args.concurrency, args.max_workers, args.files, args.max_file_bytes = 4, 2, 20, 2048
    binary = os.path.abspath(args.bin)
    if not os.path.isfile(binary):
        raise SystemExit("binary not found: %s (build it: cargo build --release --bin oxideswarm)" % binary)

    tmp = tempfile.mkdtemp(prefix="oxbench-")
    started = datetime.datetime.now(datetime.timezone.utc)
    load_before = load_avg()
    try:
        a_runs = {k: [] for k in range(1, args.max_workers + 1)}
        b_runs = []
        # Interleave the repetitions so slow drift (other processes, thermals) spreads over all rows.
        for run in range(args.runs):
            for k in a_runs:
                print("[a] run %d/%d, %d worker(s)" % (run + 1, args.runs, k), flush=True)
                a_runs[k].append(measure_a(binary, k, args.latency_samples, args.throughput_tasks,
                                           args.concurrency, tmp))
            print("[b] run %d/%d" % (run + 1, args.runs), flush=True)
            b_runs.append(measure_b(binary, args.files, args.max_file_bytes, args.seed, tmp))
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
    for key in ("first", "repeat_same", "repeat_one_file_changed"):
        b_summary[key] = {
            "wall_s": summarize([r[key]["wall_s"] for r in b_runs]),
            "blobs_sent": summarize([r[key]["blobs_sent"] for r in b_runs]),
            "master_blob_store_growth_bytes": summarize([r[key]["master_blob_store_growth_bytes"] for r in b_runs]),
        }

    version = sh([binary, "--version"])
    record = {
        "benchmark": "BENCH-1 (a) task overhead and (b) repeated upload",
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": git_info(),
        "binary": {"version": version, "build_profile": build_profile(binary)},
        "hardware": hardware(),
        "os": os_info(),
        "network": "loopback (127.0.0.1), master and workers on this machine",
        "power": power_state(),
        "load_average_before": load_before,
        "load_average_after": load_avg(),
        "python": platform.python_version(),
        "note": args.label,
        "parameters": {k: getattr(args, k) for k in ("runs", "latency_samples", "throughput_tasks",
                                                       "concurrency", "max_workers", "files",
                                                       "max_file_bytes", "seed", "quick")},
        "a_summary": a_summary,
        "b_summary": b_summary,
        "raw": {"a": [r for runs in a_runs.values() for r in runs], "b": b_runs},
    }
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


if __name__ == "__main__":
    main()
