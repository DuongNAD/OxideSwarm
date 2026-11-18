#!/usr/bin/env python3
"""
benchmarks/sweep/bench_sweep.py - the sweep harness of design 0093 (BENCH-2).

Runs one list of tasks two ways and records both under the method of design 0093 D1:

  local  the tasks run on this machine without OxideSwarm, one process per task, at 1, half and all
         logical cores; the best of the three is this machine's baseline;
  grid   the same tasks run through a cluster with `oxideswarm submit --json --wait`, either a loopback
         cluster this script starts (private HOME/USERPROFILE, ephemeral ports, beacon off, stopped by
         process handle) or an existing master given with --master (and --secret-file).

It reports throughput, speedup against the fastest machine alone and parallel efficiency against the
sum of each machine's best local throughput (pass the local records of the other machines with
--local-results), compares every grid checksum with the local checksum of the same task, and writes one
JSON record to benchmarks/sweep/results/<UTC date>-<slug>.json. Standard library only. Read
benchmarks/sweep/README.md for the method and for what a number does not show.

  python3 benchmarks/sweep/bench_sweep.py --quick --no-write                 # smoke run, loopback
  python3 benchmarks/sweep/bench_sweep.py --plan plan.json --tasks 200 --task-seconds 10
  python3 benchmarks/sweep/bench_sweep.py --plan plan.json --master HOST:PORT --secret-file run.secret \\
      --local-results results/<date>-local-mac.json --worker-python python3
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
WORKLOAD = os.path.join(HERE, "workload_cpu.py")
IS_WINDOWS = os.name == "nt"
RECORD_FORMAT = 1
W1_RE = re.compile(r"W1 seed=(\d+) units=(\d+) checksum=([0-9a-f]{64})")
IPV4_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")


# ---------------------------------------------------------------- pure helpers (unit-tested)

def summarize(values):
    """Median, min, max and count of a non-empty list of per-run figures."""
    if not values:
        raise ValueError("summarize of an empty list")
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "runs": len(values)}


def parallelism_levels(cores):
    """1, half and all logical cores (design 0093 D1 rule 2), without repeats."""
    cores = max(1, int(cores or 1))
    return sorted({1, max(1, cores // 2), cores})


def task_seeds(count, base=0):
    """The seeds of a sweep of `count` tasks: base, base+1, ..."""
    if count < 1:
        raise ValueError("a sweep needs at least one task")
    return list(range(base, base + count))


def parse_w1_line(text):
    """The (seed, units, checksum) of the W1 result line in `text`, or None."""
    m = W1_RE.search(text or "")
    if not m:
        return None
    return {"seed": int(m.group(1)), "units": int(m.group(2)), "checksum": m.group(3)}


def tasks_per_second(tasks, wall_s):
    if wall_s <= 0:
        raise ValueError("wall clock must be positive")
    return tasks / wall_s


def best_level(levels):
    """The level entry with the highest median throughput (ties: fewer processes)."""
    if not levels:
        raise ValueError("no local level")
    return max(levels, key=lambda e: (e["tasks_per_s"]["median"], -e["parallelism"]))


def scaling(grid_tasks_per_s, local_bests):
    """Speedup against the fastest machine alone and efficiency against the sum of the machines (D1 rule 3)."""
    if not local_bests:
        raise ValueError("no local baseline")
    fastest, total = max(local_bests), sum(local_bests)
    return {
        "fastest_local_tasks_per_s": fastest,
        "sum_local_tasks_per_s": total,
        "speedup": grid_tasks_per_s / fastest,
        "efficiency": grid_tasks_per_s / total,
        "machines": len(local_bests),
    }


def compare_checksums(local, grid):
    """Compare grid checksums with local ones, per seed. `local` maps seed -> checksum; `grid` is a list
    of {"seed", "checksum"} (checksum None when the task failed or printed no W1 line)."""
    mismatches, missing, matched = [], [], 0
    for row in grid:
        seed, got = row["seed"], row.get("checksum")
        if got is None:
            missing.append(seed)
            continue
        want = local.get(seed)
        if want is None:
            missing.append(seed)
        elif got == want:
            matched += 1
        else:
            mismatches.append({"seed": seed, "local": want, "grid": got})
    return {"compared": len(grid), "matched": matched, "mismatched": len(mismatches),
            "missing": len(missing), "mismatches": mismatches, "missing_seeds": missing}


def sanitize_worker(worker, label):
    """Hardware and link facts of one `workers --json` entry, never its name, id or an address."""
    tags = {}
    for t in worker.get("tags") or []:
        if isinstance(t, str) and ":" in t:
            k, v = t.split(":", 1)
            if k in ("os", "arch", "slots", "iso", "link", "proto"):
                tags[k] = v
    slots = tags.get("slots")
    return {
        "label": label,
        "os": tags.get("os", "unknown"),
        "arch": tags.get("arch", "unknown"),
        "slots": int(slots) if slots and slots.isdigit() else None,
        "isolation": tags.get("iso", "off"),
        "link": tags.get("link") or worker.get("link"),
        "protocol": tags.get("proto"),
        "cpu_cores": worker.get("cpu_cores"),
        "ram_mb": worker.get("ram_mb"),
        "has_gpu": worker.get("has_gpu"),
    }


def worker_labels(workers):
    """Map each worker's name and id to a neutral label (worker-1, worker-2, ...), in name order."""
    labels = {}
    ordered = sorted(workers, key=lambda w: str(w.get("name") or w.get("id") or ""))
    for i, w in enumerate(ordered, 1):
        label = "worker-%d" % i
        for key in ("name", "id"):
            if w.get(key):
                labels[str(w[key])] = label
        for t in w.get("tags") or []:
            if isinstance(t, str) and t.startswith("id:"):
                labels[t[3:]] = label
    return labels


def scrub(text, labels):
    """Replace worker names and ids with their labels, and any IPv4 address with <address>."""
    out = text or ""
    for raw in sorted(labels, key=len, reverse=True):
        if len(raw) >= 2:
            out = re.sub(r"(?<![\w-])%s(?![\w-])" % re.escape(raw), labels[raw], out)
    return IPV4_RE.sub("<address>", out)


def sanitize_doctor(doc, labels):
    """The link facts and check outcomes of `doctor --json`, without the master address or any name."""
    workers = []
    for w in doc.get("workers") or []:
        workers.append({
            "label": labels.get(str(w.get("name")), labels.get(str(w.get("id")), "worker-?")),
            "os": w.get("os"), "arch": w.get("arch"), "cores": w.get("cores"), "ram_mb": w.get("ram_mb"),
            "isolation": w.get("isolation"), "link": w.get("link"), "link_kind": w.get("link_kind"),
            "link_rtt_ms": w.get("link_rtt_ms"),
        })
    checks = []
    for c in doc.get("checks") or []:
        checks.append({
            "id": c.get("id"), "name": c.get("name"), "outcome": c.get("outcome"),
            "worker": labels.get(str(c.get("worker"))) if c.get("worker") else None,
            "detail": scrub(str(c.get("detail") or ""), labels),
        })
    return {"workers": workers, "checks": checks}


def load_local_record(path, units, workload_sha256):
    """The best local throughput of another machine's record of the same workload and task size."""
    with open(path, encoding="utf-8") as f:
        rec = json.load(f)
    wl = rec.get("workload") or {}
    if wl.get("units_per_task") != units:
        raise ValueError("%s: %s units per task, this run uses %s" % (path, wl.get("units_per_task"), units))
    if wl.get("sha256") != workload_sha256:
        raise ValueError("%s: another version of workload_cpu.py" % path)
    best = ((rec.get("local") or {}).get("best") or {}).get("tasks_per_s", {}).get("median")
    if not best:
        raise ValueError("%s holds no local result" % path)
    m = rec.get("machine") or {}
    return {"source": os.path.basename(path), "best_tasks_per_s": best,
            "os": (m.get("os") or {}).get("system"), "machine": (m.get("os") or {}).get("machine"),
            "cpu_model": (m.get("hardware") or {}).get("cpu_model"),
            "cores": (m.get("hardware") or {}).get("cores")}


def slugify(text):
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-") or "run"


def result_path(directory, when, slug):
    """<UTC date>-<slug>.json, with a numeric suffix if it exists."""
    base = "%s-%s" % (when.strftime("%Y-%m-%d"), slugify(slug))
    path, n = os.path.join(directory, base + ".json"), 1
    while os.path.exists(path):
        n += 1
        path = os.path.join(directory, "%s-%d.json" % (base, n))
    return path


def file_sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def read_plan(path):
    """The plan file as JSON when it parses, else as text (design 0093 D1 rule 1)."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    try:
        return json.loads(text)
    except ValueError:
        return text


# ---------------------------------------------------------------- environment facts

def sh(cmd, timeout=20):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout.strip()
    except Exception:
        return ""


def hardware():
    info = {"cpu_model": platform.processor() or platform.machine(), "cores": os.cpu_count(), "ram_bytes": None}
    system = platform.system()
    if system == "Darwin":
        info["cpu_model"] = sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or info["cpu_model"]
        mem = sh(["sysctl", "-n", "hw.memsize"])
        info["ram_bytes"] = int(mem) if mem.isdigit() else None
    elif system == "Linux":
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
    elif system == "Windows":
        out = sh(["powershell", "-NoProfile", "-Command",
                  "(Get-CimInstance Win32_Processor).Name; (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"])
        lines = [l.strip() for l in out.splitlines() if l.strip()]
        if len(lines) >= 2 and lines[-1].isdigit():
            info["cpu_model"], info["ram_bytes"] = lines[0], int(lines[-1])
    return info


def power_state():
    system = platform.system()
    if system == "Darwin":
        return sh(["pmset", "-g", "batt"]).splitlines()[0:2]
    if system == "Linux":
        base = "/sys/class/power_supply"
        try:
            return {n: open(os.path.join(base, n, "status")).read().strip()
                    for n in os.listdir(base) if os.path.exists(os.path.join(base, n, "status"))} or "no battery found"
        except OSError:
            return "unknown"
    if system == "Windows":
        out = sh(["powershell", "-NoProfile", "-Command",
                  "$b = Get-CimInstance Win32_Battery; if ($b) { 'battery ' + $b.BatteryStatus } else { 'no battery found' }"])
        return out or "unknown"
    return "unknown"


def os_info():
    """Generic OS facts only. Never the host name, the user name or a path."""
    return {"system": platform.system(), "release": platform.release(), "version": platform.version(),
            "machine": platform.machine()}


def load_avg():
    try:
        return [round(x, 2) for x in os.getloadavg()]
    except (AttributeError, OSError):
        return None


def git_info():
    commit = sh(["git", "-C", REPO, "rev-parse", "HEAD"])
    dirty = sh(["git", "-C", REPO, "status", "--porcelain", "--", ":!benchmarks/sweep/results"])
    return {"commit": commit or "unknown", "dirty_tree": bool(dirty)}


def build_profile(binary):
    p = binary.replace("\\", "/").lower()
    return "release" if "/release/" in p else "debug" if "/debug/" in p else "unknown"


# ---------------------------------------------------------------- local runs

def run_local_task(python, seed, units, cwd):
    r = subprocess.run([python, WORKLOAD, "--seed", str(seed), "--units", str(units)],
                       capture_output=True, text=True, cwd=cwd, timeout=3600)
    parsed = parse_w1_line(r.stdout) if r.returncode == 0 else None
    return {"seed": seed, "exit_code": r.returncode, "checksum": parsed["checksum"] if parsed else None}


def run_local_batch(python, seeds, units, parallelism, cwd):
    """Run every seed as its own process, `parallelism` at a time; return wall clock and results."""
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(parallelism) as ex:
        results = list(ex.map(lambda s: run_local_task(python, s, units, cwd), seeds))
    wall = time.perf_counter() - t0
    failed = sum(1 for r in results if r["checksum"] is None)
    return {"parallelism": parallelism, "tasks": len(seeds), "wall_s": round(wall, 4),
            "tasks_per_s": round(tasks_per_second(len(seeds), wall), 4), "failed_tasks": failed}, results


def measure_local(python, seeds, units, levels, runs, cwd, log):
    out, checksums, failed = [], {}, 0
    for p in levels:
        log("[local] parallelism %d: warm-up" % p)
        warm, _ = run_local_batch(python, seeds, units, p, cwd)
        measured = []
        for i in range(runs):
            log("[local] parallelism %d: run %d/%d" % (p, i + 1, runs))
            row, results = run_local_batch(python, seeds, units, p, cwd)
            measured.append(row)
            failed += row["failed_tasks"]
            for r in results:
                if r["checksum"] is not None:
                    checksums.setdefault(r["seed"], r["checksum"])
        out.append({"parallelism": p, "warmup": warm, "runs": measured,
                    "tasks_per_s": summarize([r["tasks_per_s"] for r in measured]),
                    "wall_s": summarize([r["wall_s"] for r in measured])})
    return {"levels": out, "best": best_level(out), "failed_tasks": failed}, checksums


# ---------------------------------------------------------------- grid plumbing

def client_env(home, empty_config, secret_file=None):
    os.makedirs(home, exist_ok=True)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("RUSTY_GRID_")
           and k not in ("OXIDESWARM_SECRET", "OXIDESWARM_SECRET_FILE", "OXIDE_DEV_MODE", "OXIDESWARM_MASTER",
                         "OXIDESWARM_P2P_TICKET", "OXIDE_SWARM_DASHBOARD_PORT", "OXIDE_SWARM_DASHBOARD_PORT_FILE")}
    env.update(HOME=home, USERPROFILE=home, OXIDESWARM_HOME=os.path.join(home, ".oxideswarm"),
               RUSTY_GRID_CONFIG=empty_config, RUSTY_GRID_NO_BATTERY="1")
    if secret_file:
        env["OXIDESWARM_SECRET_FILE"] = os.path.abspath(secret_file)
    return env


class LoopbackCluster:
    """One master and `workers` workers on 127.0.0.1, every process with its own private HOME and TEMP."""

    def __init__(self, binary, workdir, workers, slots):
        self.binary, self.workdir, self.n_workers, self.slots = binary, workdir, workers, slots
        self.procs, self.addr = [], None
        self.empty_config = os.path.join(workdir, "empty.toml")
        with open(self.empty_config, "w") as f:
            f.write("")
        self.env = self._env("client")

    def _env(self, name):
        env = client_env(os.path.join(self.workdir, "home-" + name), self.empty_config)
        tmp = os.path.join(self.workdir, "tmp-" + name)
        os.makedirs(tmp, exist_ok=True)
        env.update(TEMP=tmp, TMP=tmp, TMPDIR=tmp)
        return env

    def _spawn(self, name, args):
        log = open(os.path.join(self.workdir, name + ".log"), "wb")
        kw = {} if IS_WINDOWS else {"start_new_session": True}
        p = subprocess.Popen([self.binary] + args, stdout=log, stderr=subprocess.STDOUT, env=self._env(name), **kw)
        self.procs.append((name, p, log))
        return p

    def start(self):
        port_file = os.path.join(self.workdir, "master.port")
        master = self._spawn("master", ["master", "--listen", "127.0.0.1:0", "--port-file", port_file,
                                        "--dashboard-port", "0", "--discovery-port", "0",
                                        "--blob-dir", os.path.join(self.workdir, "blobs")])
        deadline = time.time() + 30
        while time.time() < deadline:
            if master.poll() is not None:
                raise SystemExit("master exited early; see " + os.path.join(self.workdir, "master.log"))
            if os.path.exists(port_file):
                txt = open(port_file).read().strip()
                if txt:
                    self.addr = txt if ":" in txt else "127.0.0.1:" + txt
                    break
            time.sleep(0.05)
        else:
            raise SystemExit("master did not publish its port within 30 s")
        for i in range(self.n_workers):
            sandbox = os.path.join(self.workdir, "sandbox-w%d" % i)
            os.makedirs(sandbox, exist_ok=True)
            self._spawn("worker%d" % i, ["worker", "--master", self.addr, "--name", "w%d" % i, "--no-portal",
                                         "--no-gpu", "--max-concurrency", str(self.slots),
                                         "--sandbox-base-dir", sandbox])
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

    def run(self, args, timeout=600):
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


class ExistingMaster:
    """A master someone else runs; this script only submits to it."""

    def __init__(self, binary, workdir, addr, secret_file):
        self.binary, self.addr = binary, addr
        empty = os.path.join(workdir, "empty.toml")
        with open(empty, "w") as f:
            f.write("")
        self.env = client_env(os.path.join(workdir, "home-client"), empty, secret_file)

    def run(self, args, timeout=600):
        return subprocess.run([self.binary] + args[:1] + ["--master", self.addr] + args[1:],
                              capture_output=True, text=True, env=self.env, timeout=timeout)

    def stop(self):
        pass


def submit_task(cluster, worker_python, seed, units, timeout_secs, labels):
    args = ["submit", "--json", "--wait", "--timeout-secs", str(timeout_secs),
            "--input", WORKLOAD + ":workload_cpu.py", "--command", worker_python,
            "--", "workload_cpu.py", "--seed", str(seed), "--units", str(units)]
    t0 = time.perf_counter()
    r = cluster.run(args, timeout=timeout_secs + 120)
    dt = time.perf_counter() - t0
    row = {"seed": seed, "submit_to_result_s": round(dt, 4), "cli_exit_code": r.returncode,
           "exit_code": None, "checksum": None, "worker": None, "execution_time_ms": None}
    try:
        doc = json.loads(r.stdout)
    except ValueError:
        doc = None
    if isinstance(doc, dict):
        row["exit_code"] = doc.get("exit_code")
        row["execution_time_ms"] = doc.get("execution_time_ms")
        row["worker"] = labels.get(str(doc.get("worker_id")), labels.get(str(doc.get("worker_name")), "worker-?"))
        parsed = parse_w1_line(doc.get("stdout"))
        if parsed and doc.get("exit_code") == 0:
            row["checksum"] = parsed["checksum"]
    return row


def run_grid_batch(cluster, worker_python, seeds, units, concurrency, timeout_secs, labels):
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(concurrency) as ex:
        rows = list(ex.map(lambda s: submit_task(cluster, worker_python, s, units, timeout_secs, labels), seeds))
    wall = time.perf_counter() - t0
    failed = sum(1 for r in rows if r["checksum"] is None)
    per_worker = {}
    for r in rows:
        per_worker[r["worker"] or "none"] = per_worker.get(r["worker"] or "none", 0) + 1
    lat = sorted(r["submit_to_result_s"] for r in rows)
    return {"tasks": len(seeds), "wall_s": round(wall, 4),
            "tasks_per_s": round(tasks_per_second(len(seeds), wall), 4), "failed_tasks": failed,
            "tasks_per_worker": per_worker, "submit_to_result_p50_s": statistics.median(lat),
            "submit_to_result_max_s": lat[-1]}, rows


def list_workers(cluster):
    r = cluster.run(["workers", "--json"], timeout=60)
    if r.returncode != 0:
        raise SystemExit("workers --json failed (exit %d): %s%s" % (r.returncode, r.stdout, r.stderr))
    return [w for w in json.loads(r.stdout) if isinstance(w, dict)]


def doctor(cluster, n_workers, labels):
    try:
        r = cluster.run(["doctor", "--json", "--expect-workers", str(n_workers)], timeout=300)
        return sanitize_doctor(json.loads(r.stdout), labels)
    except Exception as e:  # the record says why instead of failing the run
        return {"error": scrub(str(e)[:200], labels)}


# ---------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    default_bin = os.path.join(REPO, "target", "release", "oxideswarm" + (".exe" if IS_WINDOWS else ""))
    ap.add_argument("--bin", default=default_bin, help="oxideswarm binary (default: target/release/oxideswarm)")
    ap.add_argument("--plan", help="file holding the plan of this record (D1 rule 1); required unless --no-write")
    ap.add_argument("--tasks", type=int, default=40, help="tasks in the sweep (default 40)")
    ap.add_argument("--units", type=int, help="W1 units per task (default: --task-seconds calibrated here)")
    ap.add_argument("--task-seconds", type=float, default=1.0,
                    help="target seconds per task on this machine when --units is not given (default 1.0)")
    ap.add_argument("--runs", type=int, default=3, help="measured runs after one warm-up run (default 3)")
    ap.add_argument("--seed-base", type=int, default=0, help="seed of the first task (default 0)")
    ap.add_argument("--skip-local", action="store_true", help="do not run the local baseline on this machine")
    ap.add_argument("--local-only", action="store_true",
                    help="run only the local baseline (no cluster, no binary); its record is what another "
                         "run takes with --local-results")
    ap.add_argument("--local-results", action="append", default=[],
                    help="local record of another machine for the same workload and units (repeatable)")
    ap.add_argument("--master", help="an existing master (HOST:PORT or ticket); default: a loopback cluster")
    ap.add_argument("--secret-file", help="cluster secret of the existing master")
    ap.add_argument("--workers", type=int, default=1, help="workers of the loopback cluster (default 1)")
    ap.add_argument("--slots", type=int, help="slots per loopback worker (default: the best local parallelism)")
    ap.add_argument("--client-concurrency", type=int,
                    help="submit processes in flight (default: the total slots of the cluster)")
    ap.add_argument("--worker-python", help="Python command on the workers (default: this interpreter for a "
                    "loopback cluster, python3 for --master)")
    ap.add_argument("--timeout-secs", type=int, default=3600, help="timeout of each grid task (default 3600)")
    ap.add_argument("--skip-doctor", action="store_true", help="do not record `doctor --json`")
    ap.add_argument("--slug", help="file name part after the date (default: w1-<os>-<arch>-<mode>)")
    ap.add_argument("--out", help="record path (default benchmarks/sweep/results/<UTC date>-<slug>.json)")
    ap.add_argument("--no-write", action="store_true", help="print the record instead of writing it")
    ap.add_argument("--quick", action="store_true", help="smoke run: 8 tasks of about 0.05 s, 1 run")
    ap.add_argument("--label", default="", help="free-text note stored in the record")
    args = ap.parse_args(argv)

    if args.quick:
        args.tasks, args.runs = 8, 1
        if args.units is None:
            args.task_seconds = 0.05
    if not args.no_write and not args.plan:
        raise SystemExit("--plan <file> is required for a record (design 0093 D1 rule 1); use --no-write to try")
    if args.skip_local and not args.local_results:
        raise SystemExit("--skip-local needs --local-results: a run needs at least one local baseline")
    if args.local_only and args.skip_local:
        raise SystemExit("--local-only and --skip-local exclude each other")
    binary = os.path.abspath(args.bin)
    if not args.local_only and not os.path.isfile(binary):
        raise SystemExit("binary not found: %s (build it: cargo build --release --bin oxideswarm)" % binary)
    log = lambda msg: print(msg, file=sys.stderr, flush=True)

    units = args.units
    if units is None:
        out = sh([sys.executable, WORKLOAD, "--calibrate", str(args.task_seconds)], timeout=120)
        units = int(out) if out.isdigit() else 100000
    workload_sha = file_sha256(WORKLOAD)
    others = [load_local_record(p, units, workload_sha) for p in args.local_results]
    seeds = task_seeds(args.tasks, args.seed_base)
    started = datetime.datetime.now(datetime.timezone.utc)
    load_before = load_avg()
    tmp = tempfile.mkdtemp(prefix="oxsweep-")
    cluster = None
    mode, workers, total_slots, concurrency, doc, warm, grid_runs, grid_rows = "local only", [], 0, 0, None, None, [], []
    try:
        local, local_sums = None, {}
        if not args.skip_local:
            local_cwd = os.path.join(tmp, "local")
            os.makedirs(local_cwd)
            local, local_sums = measure_local(sys.executable, seeds, units, parallelism_levels(os.cpu_count()),
                                              args.runs, local_cwd, log)
        if not args.local_only:
            if args.master:
                mode = "existing master"
                cluster = ExistingMaster(binary, tmp, args.master, args.secret_file)
                worker_python = args.worker_python or "python3"
            else:
                mode = "loopback"
                slots = args.slots or (local["best"]["parallelism"] if local else os.cpu_count())
                cluster = LoopbackCluster(binary, tmp, args.workers, slots)
                log("[grid] starting a loopback cluster: %d worker(s) x %d slots" % (args.workers, slots))
                cluster.start()
                worker_python = args.worker_python or sys.executable
            raw_workers = list_workers(cluster)
            labels = worker_labels(raw_workers)
            workers = [sanitize_worker(w, labels.get(str(w.get("name")), "worker-?")) for w in raw_workers]
            total_slots = sum(w["slots"] or 1 for w in workers) or 1
            concurrency = args.client_concurrency or total_slots
            doc = None if args.skip_doctor else doctor(cluster, len(raw_workers), labels)
            log("[grid] warm-up: %d tasks, %d in flight" % (len(seeds), concurrency))
            warm, _ = run_grid_batch(cluster, worker_python, seeds, units, concurrency, args.timeout_secs, labels)
            for i in range(args.runs):
                log("[grid] run %d/%d" % (i + 1, args.runs))
                row, rows = run_grid_batch(cluster, worker_python, seeds, units, concurrency,
                                           args.timeout_secs, labels)
                grid_runs.append(row)
                grid_rows.extend(rows)
    finally:
        if cluster is not None:
            cluster.stop()
        shutil.rmtree(tmp, ignore_errors=True)

    bests = ([local["best"]["tasks_per_s"]["median"]] if local else []) + [o["best_tasks_per_s"] for o in others]
    grid_tp, checks, scale = None, None, None
    if grid_runs:
        grid_tp = summarize([r["tasks_per_s"] for r in grid_runs])
        scale = scaling(grid_tp["median"], bests)
        if local_sums:
            checks = compare_checksums(local_sums, grid_rows)
        else:
            # No local sweep here: check every grid result against one local run of its seed.
            reference = {s: run_local_task(sys.executable, s, units, None)["checksum"] for s in seeds}
            checks = compare_checksums(reference, grid_rows)
    failed = (local["failed_tasks"] if local else 0) + sum(r["failed_tasks"] for r in grid_runs)
    plan = read_plan(args.plan) if args.plan else None
    system = os_info()
    record = {
        "benchmark": "BENCH-2 sweep (design 0093 D1)",
        "record_format": RECORD_FORMAT,
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "plan": plan,
        "oxideswarm": dict(git_info(), version=sh([binary, "--version"]), build_profile=build_profile(binary)),
        "workload": {"name": "W1", "file": "benchmarks/sweep/workload_cpu.py", "sha256": workload_sha,
                     "commit": git_info()["commit"], "units_per_task": units, "tasks": len(seeds),
                     "seeds": [seeds[0], seeds[-1]], "task_seconds_target": args.units is None and args.task_seconds or None},
        "machine": {"hardware": hardware(), "os": system, "power": power_state(),
                    "python": platform.python_version()},
        "cluster": {"mode": mode, "workers": workers, "total_slots": total_slots,
                    "client_concurrency": concurrency, "doctor": doc},
        "load_average_before": load_before,
        "load_average_after": load_avg(),
        "local": local,
        "local_results_used": others,
        "grid": ({"warmup": warm, "runs": grid_runs, "tasks_per_s": grid_tp,
                  "wall_s": summarize([r["wall_s"] for r in grid_runs])} if grid_runs else None),
        "scaling": scale,
        "checksums": checks,
        "failed_tasks": failed,
        "quick": args.quick,
        "note": args.label,
    }
    text = json.dumps(record, indent=2)
    if scale:
        summary = ("W1 %d tasks x %d units: grid %.3f tasks/s (median of %d), fastest local %.3f tasks/s, "
                   "speedup %.2fx, efficiency %.0f %% over %d machine(s); checksums %d/%d matched, %d failed task(s)"
                   % (len(seeds), units, grid_tp["median"], grid_tp["runs"], scale["fastest_local_tasks_per_s"],
                      scale["speedup"], 100 * scale["efficiency"], scale["machines"], checks["matched"],
                      checks["compared"], failed))
    else:
        summary = ("W1 %d tasks x %d units, local only: best %.3f tasks/s at parallelism %d; %d failed task(s)"
                   % (len(seeds), units, local["best"]["tasks_per_s"]["median"], local["best"]["parallelism"],
                      failed))
    if args.no_write:
        print(text)
        log(summary)
        return 0
    out = args.out or result_path(os.path.join(HERE, "results"), started,
                                  args.slug or "w1-%s-%s-%s" % (system["system"], system["machine"],
                                                                {"loopback": "loopback", "local only": "local"}
                                                                .get(mode, "grid")))
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write(text + "\n")
    print("wrote", out)
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
