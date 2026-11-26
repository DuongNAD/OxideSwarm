#!/usr/bin/env python3
"""
benchmarks/grid/bench_shard_r2_m1.py - Spike R2 & Measure M1 for AX-2 sharded runs.

Supports both:
  - Single-host loopback mode (default when `--master` is omitted): spins up a local
    master and 1..3 loopback workers on 127.0.0.1.
  - Remote multi-worker cluster mode (`--master <ADDR>`): runs baseline (`--baseline-worker`)
    vs multi-worker (`--workers ...` and `oxideswarm shard --count ...`) against an
    existing multi-OS lab cluster (e.g. macOS + Windows + Linux Docker).

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
import socket
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


def parse_shard_report_full(stdout_text):
    """Parses full `oxideswarm shard --json` output including per-worker shard distribution."""
    summary = parse_shard_json(stdout_text)
    data = json.loads(stdout_text)
    per_worker_shards = {}
    shard_cargo_summaries = []
    for rec in data.get("shards") or []:
        wname = rec.get("worker_name") or rec.get("worker") or rec.get("assigned_worker") or "unknown"
        per_worker_shards[wname] = per_worker_shards.get(wname, 0) + 1
        cs = parse_ax2_cargo_summary(rec.get("stdout") or "")
        if cs is not None:
            shard_cargo_summaries.append(cs)
    summary["per_worker_shards"] = dict(sorted(per_worker_shards.items()))
    summary["cargo_summaries"] = shard_cargo_summaries
    return summary


def parse_ax2_cargo_summary(stdout_text):
    """Extracts the `AX2_SHARD_SUMMARY <json>` line emitted by CARGO_SHARD_RUNNER_PY."""
    for line in reversed((stdout_text or "").splitlines()):
        stripped = line.strip()
        if stripped.startswith("AX2_SHARD_SUMMARY "):
            return json.loads(stripped[len("AX2_SHARD_SUMMARY "):])
    return None


CARGO_SHARD_RUNNER_PY = """#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import platform


def _pick_target_dir(tag):
    shared = os.path.join(tempfile.gettempdir(), f"oxs-ax2-{tag}")
    try:
        os.makedirs(shared, exist_ok=True)
        probe = os.path.join(shared, f".probe-{os.getpid()}")
        with open(probe, "w") as pf:
            pf.write("ok")
        os.remove(probe)
        return shared, "shared_temp"
    except OSError:
        local = os.path.join(os.getcwd(), "target")
        os.makedirs(local, exist_ok=True)
        return local, "sandbox_local"


def _sanitized_env():
    return {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("RUSTY_GRID_")
        and not k.startswith("OXIDESWARM_")
        and not k.startswith("OXIDE_")
        and not k.startswith("OX_SHARD_")
    }


def main():
    src_dir = os.path.abspath(sys.argv[1])
    threads = int(sys.argv[2])
    build_mode = sys.argv[3]
    packages_csv = sys.argv[4]
    target_tag = sys.argv[5]
    idx = int(os.environ.get("OX_SHARD_INDEX", "1"))
    total = int(os.environ.get("OX_SHARD_TOTAL", "1"))
    if idx < 1 or idx > total:
        raise SystemExit(f"invalid OX_SHARD_INDEX={idx} for OX_SHARD_TOTAL={total}")

    target_dir, target_mode = _pick_target_dir(target_tag)
    cargo_env = _sanitized_env()
    cargo_env["CARGO_TARGET_DIR"] = target_dir
    cargo_env["NO_COLOR"] = "1"
    test_env = _sanitized_env()
    test_env["NO_COLOR"] = "1"

    cmd = ["cargo", "test", "--locked", "--lib", "--no-run", "--message-format=json"]
    if build_mode == "release":
        cmd.append("--release")
    if packages_csv == "workspace":
        cmd.append("--workspace")
    else:
        for pkg in packages_csv.split(","):
            if pkg:
                cmd.extend(["-p", pkg])

    t_prep0 = time.perf_counter()
    prep = subprocess.run(cmd, cwd=src_dir, env=cargo_env, capture_output=True, text=True)
    if prep.returncode != 0:
        sys.stderr.write(prep.stdout[-4000:])
        sys.stderr.write(prep.stderr[-4000:])
        sys.exit(prep.returncode)

    executables = {}
    for line in prep.stdout.splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("profile", {}).get("test"):
            exe = msg.get("executable")
            if exe and os.path.isfile(exe):
                stem = re.sub(r"-[0-9a-f]{16}(\\.exe)?$", "", os.path.basename(exe))
                executables[exe] = stem
    if not executables:
        raise SystemExit("no test binaries compiled")

    entries = []
    for exe, stem in sorted(executables.items(), key=lambda kv: (kv[1], kv[0])):
        listed = subprocess.run([exe, "--list", "--format", "terse"], env=test_env, capture_output=True, text=True)
        if listed.returncode != 0:
            sys.stderr.write(listed.stderr[-2000:])
            sys.exit(listed.returncode)
        for raw in listed.stdout.splitlines():
            if raw.endswith(": test"):
                name = raw[: -len(": test")].strip()
                if name:
                    entries.append((stem, exe, name))
    entries.sort(key=lambda item: (item[0], item[2]))
    cargo_prepare_s = time.perf_counter() - t_prep0

    mine = [item for i, item in enumerate(entries) if (i % total) == (idx - 1)]
    by_bin = {}
    for _stem, exe, tname in mine:
        by_bin.setdefault(exe, []).append(tname)

    t_exec0 = time.perf_counter()
    for exe, tests in by_bin.items():
        for start in range(0, len(tests), 400):
            chunk = tests[start : start + 400]
            res = subprocess.run(
                [exe, "--exact", f"--test-threads={threads}"] + chunk,
                cwd=src_dir,
                env=test_env,
                capture_output=True,
                text=True,
                timeout=600,
            )
            if res.returncode != 0:
                sys.stdout.write(res.stdout[-4000:])
                sys.stderr.write(res.stderr[-4000:])
                sys.exit(res.returncode)
    exec_s = time.perf_counter() - t_exec0

    summary = {
        "idx": idx,
        "total": total,
        "listed_tests": len(entries),
        "executed_tests": len(mine),
        "cargo_prepare_s": round(cargo_prepare_s, 4),
        "exec_s": round(exec_s, 4),
        "target_mode": target_mode,
        "os": platform.system().lower(),
        "arch": platform.machine().lower(),
    }
    print("AX2_SHARD_SUMMARY " + json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
"""


class RemoteCluster:
    """Wrapper around an already-running multi-machine OxideSwarm master."""

    def __init__(self, binary, workdir, master_addr, secret_file=None, insecure=False):
        self.binary = binary
        self.workdir = workdir
        self.master_addr = master_addr
        self.secret_file = os.path.abspath(secret_file) if secret_file else None
        self.insecure = insecure
        self.empty_config = os.path.join(workdir, "empty.toml")
        with open(self.empty_config, "w") as f:
            f.write("")
        home = os.path.join(workdir, "home-client")
        ox_home = os.path.join(home, ".oxideswarm")
        os.makedirs(ox_home, exist_ok=True)
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("RUSTY_GRID_")
            and not k.startswith("OXIDESWARM_")
            and not k.startswith("OXIDE_")
            and not k.startswith("OX_SHARD_")
        }
        env.update(
            HOME=home,
            USERPROFILE=home,
            OXIDESWARM_HOME=ox_home,
            RUSTY_GRID_CONFIG=self.empty_config,
            RUSTY_GRID_NO_BATTERY="1",
            RUSTY_GRID_DISABLE_DISCOVERY="1",
            NO_COLOR="1",
        )
        self.env = env

    def run(self, args, cwd=None, timeout=1800):
        cmd = [self.binary, args[0], "--master", self.master_addr]
        if self.secret_file:
            cmd.extend(["--secret-file", self.secret_file])
        elif self.insecure:
            cmd.append("--insecure")
        cmd.extend(args[1:])
        return subprocess.run(
            cmd,
            cwd=cwd or self.workdir,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def connected_workers(self):
        res = self.run(["workers", "--json"], timeout=60)
        if res.returncode != 0:
            raise RuntimeError(f"workers --json failed: {res.stderr}\n{res.stdout}")
        raw = json.loads(res.stdout)
        out = []
        for w in raw:
            tags = list(w.get("tags") or [])
            if "state:disconnected" in tags:
                continue
            tag_map = {}
            for t in tags:
                if ":" in t:
                    k, v = t.split(":", 1)
                    tag_map[k] = v
            out.append({
                "name": w.get("name"),
                "os": tag_map.get("os", "unknown"),
                "arch": tag_map.get("arch", "unknown"),
                "cpu_cores": w.get("cpu_cores"),
                "ram_mb": w.get("ram_mb"),
                "slots": int(tag_map["slots"]) if tag_map.get("slots", "").isdigit() else w.get("cpu_cores"),
                "iso": tag_map.get("iso", "unknown"),
                "has_workspaces": "feat:workspaces" in tags,
                "link": w.get("link"),
            })
        return sorted(out, key=lambda item: (item["os"], item["arch"], item["name"] or ""))


def export_clean_repo(repo_path, dest_dir):
    """Exports HEAD of repo_path without .git or target/ into dest_dir."""
    os.makedirs(dest_dir, exist_ok=True)
    archive = subprocess.run(
        ["git", "-C", repo_path, "archive", "--format=tar", "HEAD"],
        capture_output=True,
        check=True,
    )
    subprocess.run(["tar", "-xf", "-", "-C", dest_dir], input=archive.stdout, check=True)


PRIVATE_IPV4_RE = re.compile(
    r"\b(?:192\.168\.\d{1,3}\.\d{1,3}|10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b"
)
BRACKETED_IPV6_RE = re.compile(r"\[(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}\]")
INVITE_OR_TICKET_RE = re.compile(r"\boxs1-[A-Za-z0-9_-]{8,}\b")


def assert_no_private_identifiers(obj, extra_forbidden=()):
    """Raises ValueError if serialized record contains private IPs, tickets, or hostnames."""
    text = obj if isinstance(obj, str) else json.dumps(obj, sort_keys=True)
    if PRIVATE_IPV4_RE.search(text):
        raise ValueError("record contains private RFC1918 IPv4 address")
    if BRACKETED_IPV6_RE.search(text):
        raise ValueError("record contains bracketed IPv6 address")
    if INVITE_OR_TICKET_RE.search(text):
        raise ValueError("record contains oxs1-* invite/ticket token")
    candidates = [socket.gethostname(), platform.node()] + list(extra_forbidden)
    for token in candidates:
        if token and len(token) >= 4 and token in text:
            raise ValueError(f"record contains forbidden identifier ({len(token)} chars)")


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


def run_remote_r2_submit(cluster, workers, src_dir, runner_path, worker_threads, build_mode, packages_csv, remote_python, target_tag, timeout_secs=1800):
    n_shards = len(workers)
    t0 = time.perf_counter()

    def one(pair):
        idx, wname = pair
        return cluster.run(
            [
                "submit",
                "--wait",
                "--json",
                "--worker",
                wname,
                "--timeout-secs",
                str(timeout_secs),
                "--env",
                f"OX_SHARD_INDEX={idx}",
                "--env",
                f"OX_SHARD_TOTAL={n_shards}",
                "--input",
                f"{src_dir}:src",
                "--input",
                f"{runner_path}:shard_cargo_runner.py",
                "--",
                remote_python,
                "shard_cargo_runner.py",
                "src",
                str(worker_threads),
                build_mode,
                packages_csv,
                target_tag,
            ],
            timeout=timeout_secs + 120,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_shards) as pool:
        results = list(pool.map(one, enumerate(workers, start=1)))
    wall = time.perf_counter() - t0
    shard_summaries = []
    for idx, (wname, res) in enumerate(zip(workers, results), start=1):
        if res.returncode != 0:
            raise RuntimeError(
                f"Remote R2 submit shard {idx}/{n_shards} ({wname}) failed rc={res.returncode}:\n{res.stderr}\n{res.stdout}"
            )
        stdout_text = res.stdout
        try:
            payload = json.loads(res.stdout)
            stdout_text = payload.get("stdout") or res.stdout
        except ValueError:
            pass
        cs = parse_ax2_cargo_summary(stdout_text)
        if cs is not None:
            cs["worker_name"] = wname
            shard_cargo = cs
            shard_summaries.append(shard_cargo)
    return wall, shard_summaries


def run_remote_m1_shard(cluster, n_shards, src_dir, runner_path, worker_threads, build_mode, packages_csv, remote_python, target_tag, worker_pin=None, timeout_secs=1800):
    args = [
        "shard",
        "--count",
        str(n_shards),
        "--timeout-secs",
        str(timeout_secs),
        "--json",
        "--input",
        f"{src_dir}:src",
        "--input",
        f"{runner_path}:shard_cargo_runner.py",
    ]
    if worker_pin:
        args.extend(["--worker", worker_pin])
    args.extend([
        "--",
        remote_python,
        "shard_cargo_runner.py",
        "src",
        str(worker_threads),
        build_mode,
        packages_csv,
        target_tag,
    ])
    t0 = time.perf_counter()
    res = cluster.run(args, timeout=timeout_secs + 120)
    wall = time.perf_counter() - t0
    if res.returncode != 0:
        raise RuntimeError(f"Remote M1 shard ({n_shards}) failed rc={res.returncode}:\n{res.stderr}\n{res.stdout}")
    parsed = parse_shard_report_full(res.stdout)
    if parsed["failed_shards"] != 0 or parsed["completed_shards"] != n_shards:
        raise RuntimeError(f"Remote M1 shard ({n_shards}) incomplete: {parsed}")
    return wall, parsed


def run_remote_mode(args, binary, tmp, started, load_before):
    if not args.baseline_worker:
        raise SystemExit("--baseline-worker is required in --master remote mode")
    workers = args.workers or [args.baseline_worker]
    shard_counts = args.shard_counts or [3, 6]
    if 3 not in shard_counts and len(workers) == 3:
        shard_counts = [3] + shard_counts
    packages = args.packages if args.packages else ["workspace"]
    packages_csv = ",".join(packages)
    build_mode = args.build_mode or "debug"
    target_tag = f"v1-{build_mode}"

    src_dir = os.path.join(tmp, "repo_src")
    runner_path = os.path.join(tmp, "shard_cargo_runner.py")
    export_clean_repo(REPO, src_dir)
    with open(runner_path, "w") as f:
        f.write(CARGO_SHARD_RUNNER_PY)
    os.chmod(runner_path, 0o755)

    cluster = RemoteCluster(
        binary,
        tmp,
        args.master,
        secret_file=args.secret_file,
        insecure=args.insecure,
    )
    connected = cluster.connected_workers()
    by_name = {w["name"]: w for w in connected}
    required_names = [args.baseline_worker] + [w for w in workers if w != args.baseline_worker]
    missing = [w for w in required_names if w not in by_name]
    if missing:
        raise SystemExit(f"Required workers not connected on remote master: {missing}; connected={list(by_name)}")

    selected_workers = [by_name[w] for w in required_names]
    print(f"Remote workers ready: {[ (w['name'], w['os'], w['arch'], w['slots']) for w in selected_workers ]}", flush=True)

    # Warmup compile on every worker (1 uncounted pass each)
    warmup = {}
    for w in required_names:
        print(f"[warmup] compiling/running {packages_csv} ({build_mode}) on {w}...", flush=True)
        w_wall, w_sums = run_remote_r2_submit(
            cluster, [w], src_dir, runner_path, args.worker_threads, build_mode, packages_csv, args.remote_python, target_tag
        )
        warmup[w] = {
            "wall_s": round(w_wall, 4),
            **(w_sums[0] if w_sums else {}),
        }
        print(f"[warmup] {w}: {w_wall:.2f}s summary={warmup[w]}", flush=True)

    r2_1w_raw = []
    r2_mw_raw = []
    m1_1w_raw = []
    m1_counts_raw = {sc: [] for sc in shard_counts}

    for r in range(args.runs):
        print(f"[remote run {r + 1}/{args.runs}] 1w baseline ({args.baseline_worker}) vs {len(workers)}w parallel submit & shard {shard_counts}", flush=True)
        w_1w, s_1w = run_remote_r2_submit(
            cluster, [args.baseline_worker], src_dir, runner_path, args.worker_threads, build_mode, packages_csv, args.remote_python, target_tag
        )
        r2_1w_raw.append({"wall_s": round(w_1w, 4), "shards": s_1w})

        w_mw, s_mw = run_remote_r2_submit(
            cluster, workers, src_dir, runner_path, args.worker_threads, build_mode, packages_csv, args.remote_python, target_tag
        )
        r2_mw_raw.append({"wall_s": round(w_mw, 4), "shards": s_mw})

        m_1w_wall, m_1w_info = run_remote_m1_shard(
            cluster, 1, src_dir, runner_path, args.worker_threads, build_mode, packages_csv, args.remote_python, target_tag, worker_pin=args.baseline_worker
        )
        m1_1w_raw.append({"wall_s": round(m_1w_wall, 4), **m_1w_info})

        for sc in shard_counts:
            m_sc_wall, m_sc_info = run_remote_m1_shard(
                cluster, sc, src_dir, runner_path, args.worker_threads, build_mode, packages_csv, args.remote_python, target_tag
            )
            m1_counts_raw[sc].append({"wall_s": round(m_sc_wall, 4), **m_sc_info})

    r2_1w_summary = summarize([row["wall_s"] for row in r2_1w_raw])
    r2_mw_summary = summarize([row["wall_s"] for row in r2_mw_raw])
    r2_eval = compute_r2_metrics(r2_1w_summary["median"], r2_mw_summary["median"])

    m1_1w_summary = {
        "wall_s": summarize([row["wall_s"] for row in m1_1w_raw]),
        "waited_ms": summarize([row["waited_ms"] for row in m1_1w_raw]),
        "task_time_sum_ms": summarize([row["task_time_sum_ms"] for row in m1_1w_raw]),
        "machines_median": statistics.median([row["machines"] for row in m1_1w_raw]),
    }
    m1_by_count = {"1": m1_1w_summary}
    m1_evals = {}
    for sc in shard_counts:
        rows = m1_counts_raw[sc]
        sc_sum = {
            "wall_s": summarize([row["wall_s"] for row in rows]),
            "waited_ms": summarize([row["waited_ms"] for row in rows]),
            "task_time_sum_ms": summarize([row["task_time_sum_ms"] for row in rows]),
            "machines_median": statistics.median([row["machines"] for row in rows]),
            "per_worker_shards_sample": rows[-1].get("per_worker_shards") if rows else {},
        }
        m1_by_count[str(sc)] = sc_sum
        m1_evals[str(sc)] = compute_m1_metrics(m1_1w_summary["wall_s"]["median"], sc_sum["wall_s"]["median"])

    primary_eval_key = "3" if "3" in m1_evals else str(shard_counts[0])
    primary_m1_eval = m1_evals[primary_eval_key]

    hw = hardware()
    os_facts = os_info()
    record = {
        "benchmark": "ax2-shard-r2-m1",
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "plan": {
            "question_r2": "How much wall time does sharding this repository's workspace --lib unit tests over 3 workers save via today's `submit`?",
            "question_m1": "Does `oxideswarm shard` finish in <= 0.50x of 1-worker time?",
            "stop_rule": "R2 savings < 30% triggers guide-only stop rule",
            "scope": "multi-machine 3-worker lab cluster (macOS arm64 + Windows x86_64 + Linux arm64 Docker on Mac)",
        },
        "git": git_info(),
        "binary": {"profile": build_profile(binary), "version": sh([binary, "--version"])},
        "coordinator_hardware": hw,
        "coordinator_os": os_facts,
        "workers": selected_workers,
        "network": "LAN TCP (Mac master + Windows case PC + Linux arm64 Docker container on Mac)",
        "power": power_state(),
        "load_average_before": load_before,
        "load_average_after": load_avg(),
        "python": platform.python_version(),
        "note": args.label or "Multi-machine AX-2 R2/M1 evaluation across Mac, Windows case PC, and Linux arm64 Docker worker on Mac",
        "parameters": {
            "packages": packages,
            "build_mode": build_mode,
            "baseline_worker": args.baseline_worker,
            "r2_workers": workers,
            "shard_counts": shard_counts,
            "runs_per_config": args.runs,
            "warmup_runs": 1,
            "worker_threads": args.worker_threads,
            "remote_python": args.remote_python,
        },
        "warmup": warmup,
        "r2_submit": {
            "by_workers": {"1": r2_1w_summary, str(len(workers)): r2_mw_summary},
            "evaluation": r2_eval,
        },
        "m1_shard": {
            "by_shards": m1_by_count,
            "evaluation": primary_m1_eval,
            "evaluations_by_count": m1_evals,
        },
        "raw": {
            "r2_submit": {"1": r2_1w_raw, str(len(workers)): r2_mw_raw},
            "m1_shard": {"1": m1_1w_raw, **{str(sc): m1_counts_raw[sc] for sc in shard_counts}},
        },
    }
    assert_no_private_identifiers(record)
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
        print(f"R2 savings ({len(workers)}w vs 1w): {r2_eval['savings_pct']}% (stop rule triggered: {r2_eval['stop_rule_triggered']})")
        for sc_key, ev in m1_evals.items():
            print(f"M1 ratio (shard --count {sc_key} / 1w): {ev['ratio_3w_to_1w']}x (target <= 0.50 met: {ev['target_met']}, machines_median={m1_by_count[sc_key]['machines_median']})")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    default_bin = os.path.join(REPO, "target", "release", "oxideswarm" + (".exe" if IS_WINDOWS else ""))
    ap.add_argument("--bin", default=default_bin, help="oxideswarm binary path")
    ap.add_argument("--packages", nargs="+", default=None,
                    help="Cargo packages whose --lib unit tests form the sharded suite")
    ap.add_argument("--runs", type=int, default=3, help="measured runs per worker count (default 3)")
    ap.add_argument("--worker-threads", type=int, default=2,
                    help="libtest --test-threads per worker (default 2)")
    ap.add_argument("--quick", action="store_true", help="1 run on 1 and 3 workers with 60 tests")
    ap.add_argument("--master", help="Existing master address to benchmark remotely instead of loopback")
    ap.add_argument("--secret-file", help="Cluster secret file for --master")
    ap.add_argument("--insecure", action="store_true", help="Pass --insecure with --master")
    ap.add_argument("--baseline-worker", help="Baseline 1-worker name in --master mode")
    ap.add_argument("--workers", nargs="+", help="Worker names for R2 parallel submit in --master mode")
    ap.add_argument("--shard-counts", nargs="+", type=int, help="Shard counts to test in --master mode (default: 3 6)")
    ap.add_argument("--build-mode", choices=["debug", "release"], help="Cargo test profile (default: release in loopback, debug in remote)")
    ap.add_argument("--remote-python", default="python3", help="Python executable on remote workers")
    ap.add_argument("--no-write", action="store_true", help="print JSON record instead of saving")
    ap.add_argument("--out", help="override output JSON path")
    ap.add_argument("--label", default="", help="optional free-text note")
    args = ap.parse_args()

    binary = os.path.abspath(args.bin)
    if not os.path.isfile(binary):
        raise SystemExit(f"binary not found: {binary} (build with: cargo build --release --locked --bin oxideswarm)")

    started = datetime.datetime.now(datetime.timezone.utc)
    load_before = load_avg()
    tmp = tempfile.mkdtemp(prefix="oxshard-bench-")

    try:
        if args.master:
            run_remote_mode(args, binary, tmp, started, load_before)
            return

        packages = args.packages or ["rusty_grid_core", "rusty_grid_master"]
        worker_counts = [1, 3] if args.quick else [1, 2, 3]
        runs = 1 if args.quick else args.runs
        fake_bin = os.path.join(tmp, "fake_bin")
        install_fake_pmset(fake_bin)

        runner_path, manifest_path, test_count = compile_rust_libtest_manifest(
            packages, tmp, quick=args.quick, release=(args.build_mode or "release") == "release"
        )
        print(f"Prepared {test_count} tests across {packages}", flush=True)

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
                "packages": packages,
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
