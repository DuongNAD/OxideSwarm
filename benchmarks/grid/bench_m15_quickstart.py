#!/usr/bin/env python3
"""
benchmarks/grid/bench_m15_quickstart.py - scripted M15 time-to-first-result benchmark (ADOPT-3).

Measures Design 0059 Measure M15 from a fresh empty home directory on loopback using a local
HTTP server backed by `--archive-dir`:
  1. Run scripts/install.sh (macOS/Linux) or scripts/install.ps1 (Windows) into a temp bin dir.
  2. Start `oxideswarm master --listen 127.0.0.1:0` and wait for `--port-file`.
  3. Start `oxideswarm worker --workspaces` and wait until `oxideswarm workers --json` shows 1 worker.
  4. Run `oxideswarm submit --wait -- hostname` and verify exit 0 and non-empty stdout.

Standard library only. Never touches the real ~/.oxideswarm or %USERPROFILE%\\.oxideswarm, stops
started processes by handle, removes temporary directories, and omits hostnames/usernames/IPs
from recorded JSON.
"""

import argparse
import datetime
import http.server
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
IS_WINDOWS = os.name == "nt"


def expected_target(sysname=None, machine=None):
    """Map (system, machine) to the OxideSwarm release target triple and archive extension."""
    s = (sysname or platform.system()).lower()
    m = (machine or platform.machine()).lower()
    if s == "darwin" and m in ("arm64", "aarch64"):
        return "aarch64-apple-darwin", ".tar.gz"
    if s == "darwin" and m in ("x86_64", "amd64"):
        return "x86_64-apple-darwin", ".tar.gz"
    if s == "linux" and m in ("x86_64", "amd64"):
        return "x86_64-unknown-linux-gnu", ".tar.gz"
    if s == "windows" and m in ("amd64", "x86_64"):
        return "x86_64-pc-windows-msvc", ".zip"
    raise ValueError("Unsupported platform for M15 benchmark: %s / %s" % (sysname or platform.system(), machine or platform.machine()))


def summarize(values):
    """Median, min, max over per-run values."""
    return {
        "median": round(statistics.median(values), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "runs": len(values),
    }


def _sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return ""


def hardware():
    info = {"cpu_model": platform.processor() or platform.machine(), "cores": os.cpu_count(), "ram_bytes": None}
    sysname = platform.system()
    if sysname == "Darwin":
        info["cpu_model"] = _sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or info["cpu_model"]
        mem = _sh(["sysctl", "-n", "hw.memsize"])
        info["ram_bytes"] = int(mem) if mem.isdigit() else None
    elif sysname == "Linux":
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        info["ram_bytes"] = int(line.split()[1]) * 1024
                        break
        except OSError:
            pass
    elif sysname == "Windows":
        out = _sh([
            "powershell", "-NoProfile", "-Command",
            "(Get-CimInstance Win32_Processor).Name; (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory",
        ])
        lines = [l for l in out.splitlines() if l.strip()]
        if len(lines) >= 2 and lines[-1].strip().isdigit():
            info["cpu_model"], info["ram_bytes"] = lines[0].strip(), int(lines[-1])
    return info


def os_info():
    """Generic OS facts only. Never hostname, username, or paths."""
    return {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
    }


def power_state():
    if platform.system() == "Darwin":
        return _sh(["pmset", "-g", "batt"]).splitlines()[0:2]
    if platform.system() == "Linux":
        base = "/sys/class/power_supply"
        try:
            res = {}
            for name in os.listdir(base):
                st = os.path.join(base, name, "status")
                if os.path.exists(st):
                    with open(st, encoding="utf-8") as f:
                        res[name] = f.read().strip()
            return res or "no battery found"
        except OSError:
            return "unknown"
    return "unknown (not recorded on this platform)"


def git_info():
    commit = _sh(["git", "-C", REPO, "rev-parse", "HEAD"])
    dirty = _sh(["git", "-C", REPO, "status", "--porcelain", "--", ":!benchmarks/grid/results"])
    return {"commit": commit or "unknown", "dirty_tree": bool(dirty)}


def load_avg():
    try:
        return [round(x, 2) for x in os.getloadavg()]
    except (AttributeError, OSError):
        return None


def _slug(text):
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-") or "unknown"


def result_path(directory, when, os_facts):
    """<UTC date>-m15-<os>-<arch>.json with numeric suffix if present. Never hostname."""
    system = {"darwin": "macos"}.get(os_facts["system"].lower(), os_facts["system"].lower())
    arch = os_facts["machine"].lower()
    if arch in ("x86_64",):
        arch = "amd64"
    base = "%s-m15-%s-%s" % (when.strftime("%Y-%m-%d"), _slug(system), _slug(arch))
    path = os.path.join(directory, base + ".json")
    n = 1
    while os.path.exists(path):
        n += 1
        path = os.path.join(directory, "%s-%d.json" % (base, n))
    return path


class _ArchiveHttpServer:
    """Serves files from archive_dir under /v<version>/<filename> on 127.0.0.1:0."""

    def __init__(self, archive_dir, version):
        self.archive_dir = os.path.abspath(archive_dir)
        self.prefix = "/v%s/" % version.lstrip("v")
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if not self.path.startswith(outer.prefix):
                    self.send_response(404)
                    self.end_headers()
                    return
                rel = self.path[len(outer.prefix):]
                if "/" in rel or "\\" in rel or rel in ("", ".", ".."):
                    self.send_response(404)
                    self.end_headers()
                    return
                full = os.path.join(outer.archive_dir, rel)
                if not os.path.isfile(full):
                    self.send_response(404)
                    self.end_headers()
                    return
                with open(full, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_args):
                pass

        self._httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = "http://127.0.0.1:%d" % self._httpd.server_address[1]
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def _isolated_env(home_dir, tmp_dir, bin_dir, download_base, empty_config):
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("RUSTY_GRID_")
        and not k.startswith("OXIDESWARM_")
        and not k.startswith("OXIDE_")
        and k.upper() != "PSMODULEPATH"
    }
    local_app = os.path.join(home_dir, "AppData", "Local")
    os.makedirs(local_app, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)
    env.update({
        "HOME": home_dir,
        "USERPROFILE": home_dir,
        "LOCALAPPDATA": local_app,
        "OXIDESWARM_HOME": os.path.join(home_dir, ".oxideswarm"),
        "RUSTY_GRID_CONFIG": empty_config,
        "RUSTY_GRID_NO_BATTERY": "1",
        "OXIDESWARM_DOWNLOAD_BASE": download_base,
        "TMPDIR": tmp_dir,
        "TEMP": tmp_dir,
        "TMP": tmp_dir,
        "PATH": bin_dir + os.pathsep + env.get("PATH", ""),
    })
    return env


def _tail_text(path, max_bytes=4096):
    try:
        with open(path, "rb") as f:
            data = f.read()
        return data[-max_bytes:].decode("utf-8", errors="replace")
    except OSError:
        return ""


def run_single_m15(version, download_base, run_idx, work_parent=None):
    """Execute one cold M15 Quick start sequence from a fresh empty HOME."""
    if work_parent:
        os.makedirs(work_parent, exist_ok=True)
    root = tempfile.mkdtemp(prefix="ox_m15_r%d_" % run_idx, dir=work_parent)
    procs = []
    logs = []
    master_log_path = os.path.join(root, "master.log")
    worker_log_path = os.path.join(root, "worker.log")
    try:
        bin_dir = os.path.join(root, "bin")
        home_install = os.path.join(root, "home-install")
        home_master = os.path.join(root, "home-master")
        home_worker = os.path.join(root, "home-worker")
        home_client = os.path.join(root, "home-client")
        for d in (home_install, home_master, home_worker, home_client):
            os.makedirs(d, exist_ok=True)
        empty_cfg = os.path.join(root, "empty.toml")
        with open(empty_cfg, "w", encoding="utf-8") as f:
            f.write("")

        total_t0 = time.perf_counter()

        # 1. Install via scripts/install.ps1 or scripts/install.sh
        env_install = _isolated_env(home_install, os.path.join(root, "tmp-install"), bin_dir, download_base, empty_cfg)
        if IS_WINDOWS:
            install_cmd = [
                "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", os.path.join(REPO, "scripts", "install.ps1"),
                "--version", version, "--dir", bin_dir,
            ]
        else:
            install_cmd = [
                "bash", os.path.join(REPO, "scripts", "install.sh"),
                "--version", version, "--dir", bin_dir,
            ]
        t_inst0 = time.perf_counter()
        inst_res = subprocess.run(
            install_cmd, env=env_install, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=180,
        )
        install_s = time.perf_counter() - t_inst0
        if inst_res.returncode != 0:
            raise RuntimeError("installer failed (exit %d):\n%s" % (inst_res.returncode, inst_res.stdout))
        attestation_checked = "Provenance attestation verified." in inst_res.stdout

        exe_suffix = ".exe" if IS_WINDOWS else ""
        oxideswarm = os.path.join(bin_dir, "oxideswarm" + exe_suffix)
        if not os.path.isfile(oxideswarm):
            raise RuntimeError("expected installed binary missing: %s" % oxideswarm)

        spawn_kw = {}
        if IS_WINDOWS and hasattr(subprocess, "CREATE_NO_WINDOW"):
            spawn_kw["creationflags"] = subprocess.CREATE_NO_WINDOW
        elif not IS_WINDOWS:
            spawn_kw["start_new_session"] = True

        # 2. Start master on loopback
        port_file = os.path.join(root, "master.port")
        dash_file = os.path.join(root, "dash.port")
        blob_dir = os.path.join(root, "blobs")
        env_master = _isolated_env(home_master, os.path.join(root, "tmp-master"), bin_dir, download_base, empty_cfg)
        mlog = open(master_log_path, "wb")
        logs.append(mlog)
        t_m0 = time.perf_counter()
        master_proc = subprocess.Popen(
            [
                oxideswarm, "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--dashboard-port", "0",
                "--dashboard-port-file", dash_file,
                "--blob-dir", blob_dir,
            ],
            env=env_master, stdin=subprocess.DEVNULL, stdout=mlog, stderr=subprocess.STDOUT, **spawn_kw
        )
        procs.append(master_proc)

        master_addr = None
        deadline = time.perf_counter() + 60.0
        while time.perf_counter() < deadline:
            if master_proc.poll() is not None:
                mlog.flush()
                raise RuntimeError("master exited early (code %s):\n%s" % (
                    master_proc.returncode, _tail_text(master_log_path)
                ))
            if os.path.exists(port_file) and os.path.getsize(port_file) > 0:
                with open(port_file, encoding="utf-8") as f:
                    txt = f.read().strip()
                if txt:
                    master_addr = txt if ":" in txt else ("127.0.0.1:" + txt)
                    break
            time.sleep(0.05)
        if not master_addr:
            raise RuntimeError("master did not publish port within 60 s")
        master_ready_s = time.perf_counter() - t_m0

        # 3. Start worker (--workspaces) and wait for registration
        sandbox_dir = os.path.join(root, "sandboxes")
        ws_dir = os.path.join(root, "workspaces")
        os.makedirs(sandbox_dir, exist_ok=True)
        os.makedirs(ws_dir, exist_ok=True)
        if not IS_WINDOWS:
            os.chmod(sandbox_dir, 0o700)
            os.chmod(ws_dir, 0o700)
        env_worker = _isolated_env(home_worker, os.path.join(root, "tmp-worker"), bin_dir, download_base, empty_cfg)
        env_client = _isolated_env(home_client, os.path.join(root, "tmp-client"), bin_dir, download_base, empty_cfg)
        wlog = open(worker_log_path, "wb")
        logs.append(wlog)
        t_w0 = time.perf_counter()
        worker_proc = subprocess.Popen(
            [
                oxideswarm, "worker",
                "--master", master_addr,
                "--workspaces",
                "--workspace-root", ws_dir,
                "--sandbox-base-dir", sandbox_dir,
                "--no-portal",
                "--no-gpu",
            ],
            env=env_worker, stdin=subprocess.DEVNULL, stdout=wlog, stderr=subprocess.STDOUT, **spawn_kw
        )
        procs.append(worker_proc)

        w_deadline = time.perf_counter() + 60.0
        connected = False
        while time.perf_counter() < w_deadline:
            if worker_proc.poll() is not None:
                wlog.flush()
                raise RuntimeError("worker exited early (code %s):\n%s" % (
                    worker_proc.returncode, _tail_text(worker_log_path)
                ))
            chk = subprocess.run(
                [oxideswarm, "workers", "--master", master_addr, "--json"],
                env=env_client, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30,
            )
            if chk.returncode == 0:
                try:
                    data = json.loads(chk.stdout)
                    if isinstance(data, list) and len(data) >= 1:
                        connected = True
                        break
                except ValueError:
                    pass
            time.sleep(0.1)
        if not connected:
            raise RuntimeError("worker did not register within 60 s")
        worker_ready_s = time.perf_counter() - t_w0

        # 4. Run first Quick start task: oxideswarm submit --wait -- hostname
        t_s0 = time.perf_counter()
        sub = subprocess.run(
            [oxideswarm, "submit", "--master", master_addr, "--wait", "--", "hostname"],
            env=env_client, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60,
        )
        first_submit_s = time.perf_counter() - t_s0
        total_s = time.perf_counter() - total_t0
        if sub.returncode != 0 or not sub.stdout.strip():
            raise RuntimeError("first submit failed (exit %d)" % sub.returncode)

        bin_version = _sh([oxideswarm, "--version"])
        return {
            "run": run_idx,
            "install_s": round(install_s, 3),
            "master_ready_s": round(master_ready_s, 3),
            "worker_ready_s": round(worker_ready_s, 3),
            "first_submit_s": round(first_submit_s, 3),
            "total_s": round(total_s, 3),
            "attestation_checked": attestation_checked,
            "submit_exit_code": sub.returncode,
            "submit_stdout_bytes": len(sub.stdout.encode("utf-8")),
            "installed_binary_version": bin_version,
        }
    finally:
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for f in logs:
            f.close()
        shutil.rmtree(root, ignore_errors=True)


def aggregate_runs(runs):
    return {
        "install_s": summarize([r["install_s"] for r in runs]),
        "master_ready_s": summarize([r["master_ready_s"] for r in runs]),
        "worker_ready_s": summarize([r["worker_ready_s"] for r in runs]),
        "first_submit_s": summarize([r["first_submit_s"] for r in runs]),
        "total_s": summarize([r["total_s"] for r in runs]),
        "all_attestation_checked": all(bool(r["attestation_checked"]) for r in runs),
        "all_succeeded": all(r["submit_exit_code"] == 0 and r["submit_stdout_bytes"] > 0 for r in runs),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive-dir", required=True, help="Directory holding release archive and SHA256SUMS")
    ap.add_argument("--version", default="0.4.4", help="Release version in --archive-dir (default: 0.4.4)")
    ap.add_argument("--runs", type=int, default=3, help="Number of cold runs (default: 3)")
    ap.add_argument("--out", help="Output JSON path (default: benchmarks/grid/results/<date>-m15-<os>-<arch>.json)")
    ap.add_argument("--work-dir", help="Parent directory for temporary run sandboxes (default on Windows: LOCALAPPDATA when set)")
    ap.add_argument("--no-write", action="store_true", help="Print JSON instead of writing file")
    ap.add_argument("--label", default="", help="Optional note recorded in JSON")
    args = ap.parse_args()

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")
    work_parent = args.work_dir or (os.environ.get("LOCALAPPDATA") if IS_WINDOWS else None) or None
    version = args.version.lstrip("v")
    target, ext = expected_target()
    archive_name = "oxideswarm-%s-%s%s" % (version, target, ext)
    archive_path = os.path.join(args.archive_dir, archive_name)
    sums_path = os.path.join(args.archive_dir, "SHA256SUMS")
    if not os.path.isfile(archive_path):
        raise SystemExit("missing archive in --archive-dir: %s" % archive_path)
    if not os.path.isfile(sums_path):
        raise SystemExit("missing SHA256SUMS in --archive-dir: %s" % sums_path)

    started = datetime.datetime.now(datetime.timezone.utc)
    load_before = load_avg()
    runs = []
    with _ArchiveHttpServer(args.archive_dir, version) as srv:
        for idx in range(1, args.runs + 1):
            print("[m15] cold quickstart run %d/%d (%s)..." % (idx, args.runs, target), flush=True)
            row = run_single_m15(version, srv.base_url, idx, work_parent=work_parent)
            print("[m15] run %d: total=%.3fs (install=%.3fs master=%.3fs worker=%.3fs submit=%.3fs, attest=%s)" % (
                idx, row["total_s"], row["install_s"], row["master_ready_s"],
                row["worker_ready_s"], row["first_submit_s"], row["attestation_checked"],
            ), flush=True)
            runs.append(row)

    os_facts = os_info()
    hw_facts = hardware()
    record = {
        "benchmark": "ADOPT-3 M15 scripted Quick start from empty HOME",
        "measure": "M15",
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": git_info(),
        "release_version": version,
        "target": target,
        "local_archive_http": True,
        "attestation_checked": all(r["attestation_checked"] for r in runs),
        "installed_binary_version": runs[0]["installed_binary_version"] if runs else "",
        "hardware": hw_facts,
        "os": os_facts,
        "network": "loopback (127.0.0.1), single machine",
        "power": power_state(),
        "load_average_before": load_before,
        "load_average_after": load_avg(),
        "python": platform.python_version(),
        "note": args.label,
        "summary": aggregate_runs(runs),
        "runs": runs,
    }
    text = json.dumps(record, indent=2)
    if args.no_write:
        print(text)
        return
    out = args.out or result_path(os.path.join(HERE, "results"), started, os_facts)
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print("wrote", out)


if __name__ == "__main__":
    main()
