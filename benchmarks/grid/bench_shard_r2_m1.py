#!/usr/bin/env python3
"""
benchmarks/grid/bench_shard_r2_m1.py - Spike R2 & Measure M1 for AX-2 sharded runs.

Measures on single-host loopback (127.0.0.1; NOT 3 physical machines):
  - Spike R2: wall-clock speedup across 1, 2, 3 workers using parallel `oxideswarm submit --wait`
    calls (`OX_SHARD_INDEX` / `OX_SHARD_TOTAL`) and evaluates the design 0056 `< 30%` stop rule.
  - Measure M1: wall-clock speedup and RunSummary metrics across 1, 2, 3 workers using
    `oxideswarm shard --count <K> --json` and evaluates the `<= 0.50x` (<= half of 1-worker time)
    target from design 0056.

Standard library only. Uses isolated private HOME/USERPROFILE directories, fake AC pmset on
macOS when needed, strict PID-handle cleanup, and removes its temporary workspace on exit.
"""

import argparse
import concurrent.futures
import datetime
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
IS_WINDOWS = os.name == "nt"


# ---------------------------------------------------------------- pure helpers (unit-tested)

def summarize(values):
    """Median, min, max, and run count of a non-empty list of numeric values."""
    if not values:
        raise ValueError("summarize requires at least one value")
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "runs": len(values),
    }


def partition_test_names(test_names, shard_index_1based, shard_total):
    """Deterministic 1-based shard partitioning: returns test_names for OX_SHARD_INDEX."""
    if shard_total < 1:
        raise ValueError("shard_total must be >= 1")
    if shard_index_1based < 1 or shard_index_1based > shard_total:
        raise ValueError(
            f"shard_index_1based ({shard_index_1based}) must be in 1..={shard_total}"
        )
    idx0 = shard_index_1based - 1
    return [name for i, name in enumerate(test_names) if (i % shard_total) == idx0]


def compute_r2_metrics(wall_1w_s, wall_3w_s):
    """Computes Spike R2 wall-time savings percentage and `< 30%` stop-rule flag."""
    if wall_1w_s <= 0:
        raise ValueError("wall_1w_s must be > 0")
    savings_pct = ((wall_1w_s - wall_3w_s) / wall_1w_s) * 100.0
    return {
        "wall_1w_s": round(wall_1w_s, 4),
        "wall_3w_s": round(wall_3w_s, 4),
        "savings_pct": round(savings_pct, 2),
        "stop_rule_threshold_pct": 30.0,
        "stop_rule_triggered": savings_pct < 30.0,
    }


def compute_m1_metrics(wall_1w_s, wall_3w_s):
    """Computes Measure M1 3w/1w ratio and `<= 0.50` target flag."""
    if wall_1w_s <= 0:
        raise ValueError("wall_1w_s must be > 0")
    ratio = wall_3w_s / wall_1w_s
    return {
        "wall_1w_s": round(wall_1w_s, 4),
        "wall_3w_s": round(wall_3w_s, 4),
        "ratio_3w_to_1w": round(ratio, 4),
        "target_max_ratio": 0.50,
        "target_met": ratio <= 0.50,
    }


def parse_shard_json(stdout_text):
    """Parses `oxideswarm shard --json` stdout and extracts RunSummary & shard counts."""
    data = json.loads(stdout_text)
    if not isinstance(data, dict) or not data.get("shard"):
        raise ValueError("JSON output is missing 'shard': true")
    return {
        "status": data.get("status"),
        "total_shards": data.get("total_shards", 0),
        "completed_shards": data.get("completed_shards", 0),
        "failed_shards": data.get("failed_shards", 0),
        "machines": data.get("machines", 0),
        "waited_ms": data.get("waited_ms", 0),
        "task_time_sum_ms": data.get("task_time_sum_ms", 0),
    }


def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return ""


def hardware():
    info = {
        "cpu_model": platform.processor() or platform.machine(),
        "cores": os.cpu_count(),
        "ram_bytes": None,
    }
    sysname = platform.system()
    if sysname == "Darwin":
        info["cpu_model"] = sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or info["cpu_model"]
        mem = sh(["sysctl", "-n", "hw.memsize"])
        info["ram_bytes"] = int(mem) if mem.isdigit() else None
    elif sysname == "Linux":
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        info["ram_bytes"] = int(line.split()[1]) * 1024
                        break
        except OSError:
            pass
    return info


def power_state():
    if platform.system() == "Darwin":
        return sh(["pmset", "-g", "batt"]).splitlines()[0:2]
    if platform.system() == "Linux":
        base = "/sys/class/power_supply"
        try:
            return {
                n: open(os.path.join(base, n, "status")).read().strip()
                for n in os.listdir(base)
                if os.path.exists(os.path.join(base, n, "status"))
            } or "no battery found"
        except OSError:
            return "unknown"
    return "unknown"


def os_info():
    """Generic OS facts only; never hostname, username, or local paths."""
    return {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
    }


def _slug(text):
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-") or "unknown"


def result_path(directory, when, os_facts, hw):
    """<UTC date>-r2-m1-<os>-<arch>-<cpu>.json without hostname or username."""
    system = {"darwin": "macos"}.get(os_facts["system"].lower(), os_facts["system"].lower())
    cpu = _slug(re.sub(r"\(R\)|\(TM\)|CPU|Processor|@.*$", " ", hw.get("cpu_model") or ""))[
        :30
    ].strip("-")
    base = "%s-r2-m1-%s-%s-%s" % (
        when.strftime("%Y-%m-%d"),
        _slug(system),
        _slug(os_facts["machine"]),
        cpu or "cpu",
    )
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


# ---------------------------------------------------------------- cluster plumbing

class Cluster:
    """Single-host loopback master + k workers with isolated HOME and fake AC pmset."""

    def __init__(self, binary, workdir, workers, fake_bin):
        self.binary = binary
        self.workdir = workdir
        self.fake_bin = fake_bin
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
            and not k.startswith("OXIDESWARM_")
            and not k.startswith("OXIDE_")
            and not k.startswith("OX_SHARD_")
        }
        path_sep = ";" if IS_WINDOWS else ":"
        env.update(
            HOME=home,
            USERPROFILE=home,
            OXIDESWARM_HOME=os.path.join(home, ".oxideswarm"),
            RUSTY_GRID_CONFIG=self.empty_config,
            RUSTY_GRID_NO_BATTERY="1",
            RUSTY_GRID_DISABLE_DISCOVERY="1",
            NO_COLOR="1",
            PATH=self.fake_bin + path_sep + env.get("PATH", ""),
        )
        return env

    def _spawn(self, name, args):
        log = open(os.path.join(self.workdir, name + ".log"), "wb")
        kw = {} if IS_WINDOWS else {"start_new_session": True}
        p = subprocess.Popen(
            [self.binary] + args,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=self._env(os.path.join(self.workdir, "home-" + name)),
            **kw,
        )
        self.procs.append((name, p, log))
        return p

    def start(self):
        port_file = os.path.join(self.workdir, "master.port")
        dash_file = os.path.join(self.workdir, "dash.port")
        master = self._spawn(
            "master",
            [
                "master",
                "--listen",
                "127.0.0.1:0",
                "--port-file",
                port_file,
                "--dashboard-port",
                "0",
                "--dashboard-port-file",
                dash_file,
                "--blob-dir",
                self.blob_dir,
            ],
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            if master.poll() is not None:
                raise SystemExit(
                    "master exited early; see " + os.path.join(self.workdir, "master.log")
                )
            if os.path.exists(port_file) and os.path.getsize(port_file) > 0:
                txt = open(port_file).read().strip()
                if txt:
                    self.addr = txt if ":" in txt else "127.0.0.1:" + txt
                    break
            time.sleep(0.05)
        else:
            raise SystemExit("master did not publish its port within 30 s")

        for i in range(self.n_workers):
            self._spawn(
                f"worker{i}",
                [
                    "worker",
                    "--master",
                    self.addr,
                    "--name",
                    f"worker-{i + 1}",
                    "--max-concurrency",
                    "1",
                    "--no-portal",
                    "--no-gpu",
                ],
            )
        deadline = time.time() + 60
        while time.time() < deadline:
            r = self.run(["workers", "--json"])
            if r.returncode == 0:
                try:
                    if len(json.loads(r.stdout)) >= self.n_workers:
                        return
                except ValueError:
                    pass
            for name, p, log in self.procs:
                if p.poll() is not None:
                    log.flush()
                    log_path = os.path.join(self.workdir, name + ".log")
                    tail = ""
                    try:
                        tail = open(log_path, errors="replace").read()[-1000:]
                    except OSError:
                        pass
                    raise SystemExit(f"{name} exited early ({p.returncode}); log:\n{tail}")
            time.sleep(0.1)
        raise SystemExit("workers did not register within 60 s")

    def run(self, args, cwd=None, timeout=600):
        cmd = [self.binary, args[0], "--master", self.addr] + args[1:]
        return subprocess.run(
            cmd,
            cwd=cwd or self.workdir,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def stop(self):
        for _name, p, log in reversed(self.procs):
            if p.poll() is None:
                try:
                    p.terminate()
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait(timeout=5)
                except Exception:
                    pass
            try:
                log.close()
            except Exception:
                pass
        self.procs.clear()


def install_fake_pmset(fake_bin):
    os.makedirs(fake_bin, exist_ok=True)
    if not IS_WINDOWS:
        pmset = os.path.join(fake_bin, "pmset")
        with open(pmset, "w") as f:
            f.write("#!/bin/sh\necho \"Now drawing from 'AC Power'\"\n")
        os.chmod(pmset, 0o755)


# ---------------------------------------------------------------- workload preparation

RUNNER_PY = """#!/usr/bin/env python3
import concurrent.futures
import json
import os
import subprocess
import sys

def main():
    manifest_path = sys.argv[1]
    threads = int(sys.argv[2])
    idx = int(os.environ.get("OX_SHARD_INDEX", "1"))
    total = int(os.environ.get("OX_SHARD_TOTAL", "1"))
    with open(manifest_path) as f:
        entries = json.load(f)
    mine = [e for i, e in enumerate(entries) if (i % total) == (idx - 1)]
    by_bin = {}
    for e in mine:
        by_bin.setdefault(e["bin"], []).append(e["test"])
    for bin_path, tests in by_bin.items():
        for start in range(0, len(tests), 400):
            chunk = tests[start:start + 400]
            cmd = [bin_path, "--exact", f"--test-threads={threads}"] + chunk
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                sys.stdout.write(res.stdout)
                sys.stderr.write(res.stderr)
                sys.exit(res.returncode)
    print(f"shard {idx}/{total} ran {len(mine)} tests ok")

if __name__ == "__main__":
    main()
"""


def compile_rust_libtest_manifest(packages, workdir, quick=False, release=True):
    cmd = ["cargo", "test", "--locked", "--lib", "--no-run", "--message-format=json"]
    if release:
        cmd.append("--release")
    for pkg in packages:
        cmd.extend(["-p", pkg])
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, check=True)
    binaries = []
    for line in proc.stdout.splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("profile", {}).get("test"):
            exe = msg.get("executable")
            if exe and os.path.isfile(exe):
                binaries.append(exe)
    if not binaries:
        raise SystemExit("no test binaries compiled")

    entries = []
    for exe in sorted(set(binaries)):
        listed = subprocess.run([exe, "--list", "--format", "terse"], capture_output=True, text=True, check=True)
        for raw in listed.stdout.splitlines():
            if raw.endswith(": test"):
                name = raw[: -len(": test")].strip()
                if name:
                    entries.append({"bin": exe, "test": name})

    if quick:
        entries = entries[:60]

    runner_path = os.path.join(workdir, "shard_runner.py")
    manifest_path = os.path.join(workdir, "tests_manifest.json")
    with open(runner_path, "w") as f:
        f.write(RUNNER_PY)
    os.chmod(runner_path, 0o755)
    with open(manifest_path, "w") as f:
        json.dump(entries, f)
    return runner_path, manifest_path, len(entries)


def run_r2_submit(cluster, n_shards, runner_path, manifest_path, worker_threads):
    t0 = time.perf_counter()

    def one(i):
        return cluster.run([
            "submit",
            "--wait",
            "--timeout-secs",
            "600",
            "--env",
            f"OX_SHARD_INDEX={i}",
            "--env",
            f"OX_SHARD_TOTAL={n_shards}",
            "--input",
            f"{runner_path}:shard_runner.py",
            "--input",
            f"{manifest_path}:tests_manifest.json",
            "--",
            sys.executable,
            "shard_runner.py",
            "tests_manifest.json",
            str(worker_threads),
        ])

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_shards) as pool:
        results = list(pool.map(one, range(1, n_shards + 1)))
    wall = time.perf_counter() - t0
    for idx, res in enumerate(results, start=1):
        if res.returncode != 0:
            raise RuntimeError(f"R2 submit shard {idx}/{n_shards} failed: {res.stderr}\n{res.stdout}")
    return wall


def run_m1_shard(cluster, n_shards, runner_path, manifest_path, worker_threads):
    t0 = time.perf_counter()
    res = cluster.run([
        "shard",
        "--count",
        str(n_shards),
        "--timeout-secs",
        "600",
        "--json",
        "--input",
        f"{runner_path}:shard_runner.py",
        "--input",
        f"{manifest_path}:tests_manifest.json",
        "--",
        sys.executable,
        "shard_runner.py",
        "tests_manifest.json",
        str(worker_threads),
    ])
    wall = time.perf_counter() - t0
    if res.returncode != 0:
        raise RuntimeError(f"M1 shard ({n_shards}) failed: {res.stderr}\n{res.stdout}")
    parsed = parse_shard_json(res.stdout)
    if parsed["failed_shards"] != 0 or parsed["completed_shards"] != n_shards:
        raise RuntimeError(f"M1 shard ({n_shards}) incomplete: {parsed}")
    return wall, parsed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    default_bin = os.path.join(REPO, "target", "release", "oxideswarm" + (".exe" if IS_WINDOWS else ""))
    ap.add_argument("--bin", default=default_bin, help="oxideswarm binary path")
    ap.add_argument("--packages", nargs="+", default=["rusty_grid_core", "rusty_grid_master"],
                    help="Cargo packages whose --lib unit tests form the sharded suite")
    ap.add_argument("--runs", type=int, default=3, help="measured runs per worker count (default 3)")
    ap.add_argument("--worker-threads", type=int, default=2,
                    help="libtest --test-threads per worker (default 2)")
    ap.add_argument("--quick", action="store_true", help="1 run on 1 and 3 workers with 60 tests")
    ap.add_argument("--no-write", action="store_true", help="print JSON record instead of saving")
    ap.add_argument("--out", help="override output JSON path")
    ap.add_argument("--label", default="", help="optional free-text note")
    args = ap.parse_args()

    binary = os.path.abspath(args.bin)
    if not os.path.isfile(binary):
        raise SystemExit(f"binary not found: {binary} (build with: cargo build --release --locked --bin oxideswarm)")

    worker_counts = [1, 3] if args.quick else [1, 2, 3]
    runs = 1 if args.quick else args.runs

    tmp = tempfile.mkdtemp(prefix="oxshard-bench-")
    fake_bin = os.path.join(tmp, "fake_bin")
    install_fake_pmset(fake_bin)
    started = datetime.datetime.now(datetime.timezone.utc)
    load_before = load_avg()

    try:
        runner_path, manifest_path, test_count = compile_rust_libtest_manifest(
            args.packages, tmp, quick=args.quick
        )
        print(f"Prepared {test_count} tests across {args.packages}", flush=True)

        r2_raw = {k: [] for k in worker_counts}
        m1_raw = {k: [] for k in worker_counts}

        for k in worker_counts:
            c_dir = os.path.join(tmp, f"cluster-{k}w")
            os.makedirs(c_dir, exist_ok=True)
            cluster = Cluster(binary, c_dir, k, fake_bin)
            try:
                cluster.start()
                # Uncounted warmup run (design 0093 D1)
                _ = run_m1_shard(cluster, k, runner_path, manifest_path, args.worker_threads)
                for r in range(runs):
                    print(f"[{k}w] run {r + 1}/{runs} (R2 submit + M1 shard)", flush=True)
                    r2_wall = run_r2_submit(cluster, k, runner_path, manifest_path, args.worker_threads)
                    m1_wall, m1_info = run_m1_shard(cluster, k, runner_path, manifest_path, args.worker_threads)
                    r2_raw[k].append(round(r2_wall, 4))
                    m1_raw[k].append({"wall_s": round(m1_wall, 4), **m1_info})
            finally:
                cluster.stop()

        r2_summary = {str(k): summarize(r2_raw[k]) for k in worker_counts}
        m1_summary = {
            str(k): {
                "wall_s": summarize([row["wall_s"] for row in m1_raw[k]]),
                "waited_ms": summarize([row["waited_ms"] for row in m1_raw[k]]),
                "task_time_sum_ms": summarize([row["task_time_sum_ms"] for row in m1_raw[k]]),
                "machines_median": statistics.median([row["machines"] for row in m1_raw[k]]),
            }
            for k in worker_counts
        }

        r2_eval = compute_r2_metrics(r2_summary["1"]["median"], r2_summary["3"]["median"])
        m1_eval = compute_m1_metrics(
            m1_summary["1"]["wall_s"]["median"], m1_summary["3"]["wall_s"]["median"]
        )

        hw = hardware()
        os_facts = os_info()
        record = {
            "benchmark": "ax2-shard-r2-m1",
            "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "plan": {
                "question_r2": "How much wall time does sharding this repository's unit tests over 3 workers save via today's `submit`?",
                "question_m1": "Does `oxideswarm shard --count 3` finish in <= 0.50x of 1-worker time?",
                "stop_rule": "R2 savings < 30% triggers guide-only stop rule",
                "scope": "single-host loopback (127.0.0.1); NOT 3 physical machines",
            },
            "git": git_info(),
            "binary": {"profile": build_profile(binary), "version": sh([binary, "--version"])},
            "hardware": hw,
            "os": os_facts,
            "network": "single-host loopback (127.0.0.1); NOT 3 physical machines",
            "power": power_state(),
            "load_average_before": load_before,
            "load_average_after": load_avg(),
            "python": platform.python_version(),
            "note": args.label or "Single-host loopback R2/M1 evaluation across 1..3 local workers sharing host CPU cores",
            "parameters": {
                "packages": args.packages,
                "test_count": test_count,
                "worker_counts": worker_counts,
                "runs_per_worker_count": runs,
                "warmup_runs": 1,
                "worker_threads": args.worker_threads,
                "quick": args.quick,
            },
            "r2_submit": {"by_workers": r2_summary, "evaluation": r2_eval},
            "m1_shard": {"by_workers": m1_summary, "evaluation": m1_eval},
            "raw": {"r2_submit": r2_raw, "m1_shard": m1_raw},
        }

        if args.no_write:
            print(json.dumps(record, indent=2))
        else:
            out_dir = os.path.join(HERE, "results")
            os.makedirs(out_dir, exist_ok=True)
            out_path = args.out or result_path(out_dir, started, os_facts, hw)
            with open(out_path, "w") as f:
                json.dump(record, f, indent=2)
                f.write("\n")
            print(f"Wrote record to {out_path}")
            print(f"R2 savings (3w vs 1w): {r2_eval['savings_pct']}% (stop rule triggered: {r2_eval['stop_rule_triggered']})")
            print(f"M1 ratio (3w / 1w): {m1_eval['ratio_3w_to_1w']}x (target <= 0.50 met: {m1_eval['target_met']})")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
