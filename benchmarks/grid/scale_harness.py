#!/usr/bin/env python3
"""SCALE-1 fleet benchmark harness for OxideSwarm (design 0050).

Orchestrates one or more invocations of the `scale_load_driver` Rust harness
against a real `oxideswarm master` subprocess inside isolated temporary
directories, aggregates per-run metrics by median, prints a concise summary to
stderr, and writes a sanitized JSON artifact under `benchmarks/grid/results/`.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO_ROOT / "benchmarks" / "grid" / "results"


def default_target_release_dir() -> Path:
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    base = Path(cargo_target) if cargo_target else (REPO_ROOT / "target")
    return base / "release"


def default_master_bin() -> Path:
    ext = ".exe" if os.name == "nt" else ""
    return default_target_release_dir() / f"oxideswarm{ext}"


def default_driver_bin() -> Path:
    ext = ".exe" if os.name == "nt" else ""
    return default_target_release_dir() / "examples" / f"scale_load_driver{ext}"


def sanitize_slug(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.strip().lower()).strip("-")
    return slug or "unknown"


def detect_cpu_model() -> str:
    system = platform.system()
    if system == "Windows":
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            ) as key:
                val, _ = winreg.QueryValueEx(key, "ProcessorNameString")
                if isinstance(val, str) and val.strip():
                    return val.strip()
        except OSError:
            pass
    elif system == "Linux":
        try:
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.lower().startswith("model name") and ":" in line:
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
    elif system == "Darwin":
        try:
            out = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            if out:
                return out
        except Exception:
            pass
    proc = platform.processor().strip()
    return proc or platform.machine() or "unknown"


def detect_ram_gb() -> float | None:
    system = platform.system()
    if system == "Windows":
        class MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(MemoryStatusEx)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return round(status.ullTotalPhys / (1024**3), 2)
    elif system == "Linux":
        try:
            for line in Path("/proc/meminfo").read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        kb = int(parts[1])
                        return round(kb / (1024**2), 2)
        except OSError:
            pass
    elif system == "Darwin":
        try:
            out = subprocess.check_output(
                ["sysctl", "-n", "hw.memsize"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            return round(int(out) / (1024**3), 2)
        except Exception:
            pass
    return None


def _git_output(git_args: list[str]) -> str:
    try:
        res = subprocess.run(
            ["git", "-c", "safe.directory=*", "-C", str(REPO_ROOT), *git_args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return res.stdout.strip() if res.returncode == 0 else ""
    except Exception:
        return ""


def git_info() -> dict[str, Any]:
    """Returns git commit and cleanliness excluding committed benchmark result JSONs."""
    commit = _git_output(["rev-parse", "HEAD"])
    dirty = _git_output(["status", "--porcelain", "--", ":!benchmarks/grid/results"])
    return {"commit": commit or "unknown", "dirty_tree": bool(dirty)}


def binary_info(bin_path: Path) -> dict[str, str]:
    """Returns binary version and profile without recording any local filesystem path."""
    version = "unknown"
    try:
        res = subprocess.run(
            [str(bin_path), "--version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            version = res.stdout.strip().splitlines()[0].strip()
    except Exception:
        pass
    normalized = bin_path.as_posix().lower()
    profile = "release" if "/release/" in normalized else "debug" if "/debug/" in normalized else "unknown"
    return {"version": version, "build_profile": profile}


def power_state() -> Any:
    """Returns sanitized host power/battery state without machine-identifying strings."""
    system = platform.system()
    if system == "Windows":
        try:
            res = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "(Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue "
                    "| Select-Object -First 1 BatteryStatus,EstimatedChargeRemaining "
                    "| ConvertTo-Json -Compress)",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            out = res.stdout.strip() if res.returncode == 0 else ""
            if not out:
                return "AC power (no battery detected)"
            return json.loads(out)
        except Exception:
            return "unknown"
    if system == "Darwin":
        try:
            res = subprocess.run(
                ["pmset", "-g", "batt"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            lines = res.stdout.strip().splitlines()[:2] if res.returncode == 0 else []
            return lines if lines else "unknown"
        except Exception:
            return "unknown"
    if system == "Linux":
        base = Path("/sys/class/power_supply")
        try:
            if not base.is_dir():
                return "no battery found"
            statuses = {}
            for entry in sorted(base.iterdir(), key=lambda p: p.name):
                status_file = entry / "status"
                if status_file.is_file():
                    statuses[entry.name] = status_file.read_text(encoding="utf-8", errors="ignore").strip()
            return statuses or "no battery found"
        except OSError:
            return "unknown"
    return "unknown"


def network_info(cfg: dict[str, Any]) -> dict[str, Any]:
    """Returns network mode and simulated link parameters for the recorded run."""
    return {
        "mode": "loopback (127.0.0.1), master subprocess and in-process simulated workers on one host",
        "base_latency_ms": cfg.get("base_latency_ms", 0),
        "jitter_ms": cfg.get("jitter_ms", 0),
        "loss_prob": cfg.get("loss_prob", 0.0),
    }


def collect_sanitized_host_info() -> dict[str, Any]:
    """Returns hardware/OS metadata strictly free of hostnames, usernames, IPs, or local paths."""
    return {
        "os": platform.system().lower(),
        "os_release": platform.release(),
        "arch": platform.machine().lower(),
        "cpu_model": detect_cpu_model(),
        "cpu_logical": os.cpu_count() or 1,
        "ram_gb": detect_ram_gb(),
        "python_version": platform.python_version(),
    }


def build_isolated_env(env_root: Path) -> dict[str, str]:
    """Constructs a minimal subprocess environment keeping system toolchain vars and temp isolation."""
    keep_keys = (
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "SystemRoot",
        "WINDIR",
        "COMSPEC",
        "ComSpec",
        "RUSTUP_HOME",
        "CARGO_HOME",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "LANG",
        "LC_ALL",
    )
    env: dict[str, str] = {}
    for key in keep_keys:
        val = os.environ.get(key)
        if val:
            env[key] = val

    home_dir = env_root / "home"
    tmp_dir = env_root / "tmp"
    home_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    env["HOME"] = str(home_dir)
    env["USERPROFILE"] = str(home_dir)
    env["TMP"] = str(tmp_dir)
    env["TEMP"] = str(tmp_dir)
    env["TMPDIR"] = str(tmp_dir)
    return env


def _aggregate_with(runs: list[dict[str, Any]], fn: Any) -> dict[str, Any]:
    if not runs:
        return {}
    if len(runs) == 1:
        return runs[0]

    def combine(values: list[Any], key: str = "") -> Any:
        first = values[0]
        if key == "config":
            return first
        if isinstance(first, dict):
            return {k: combine([v[k] for v in values], k) for k in first}
        if isinstance(first, bool):
            return first
        if isinstance(first, (int, float)):
            agg = fn(values)
            if all(isinstance(v, int) and not isinstance(v, bool) for v in values):
                if isinstance(agg, int) or (isinstance(agg, float) and agg.is_integer()):
                    return int(agg)
            return round(float(agg), 4)
        return first

    return combine(runs)


def aggregate_median(runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Recursively computes per-key medians across parsed run JSON dicts."""
    return _aggregate_with(runs, statistics.median)


def aggregate_extremum(runs: list[dict[str, Any]], fn: Any) -> dict[str, Any]:
    """Recursively computes per-key extremum (`min` or `max`) across parsed run JSON dicts."""
    return _aggregate_with(runs, fn)


def run_once(args: argparse.Namespace, run_idx: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix=f"oxideswarm-scale1-py-{run_idx}-") as td:
        env_root = Path(td)
        child_env = build_isolated_env(env_root)
        driver_tmp = env_root / "driver_tmp"
        driver_tmp.mkdir(parents=True, exist_ok=True)

        cmd = [
            str(args.driver),
            "--master-bin",
            str(args.bin),
            "--temp-root",
            str(driver_tmp),
            "--base-latency-ms",
            str(args.base_latency_ms),
            "--jitter-ms",
            str(args.jitter_ms),
            "--loss-prob",
            str(args.loss_prob),
        ]
        if args.quick:
            cmd.append("--quick")
        if args.workers is not None:
            cmd.extend(["--workers", str(args.workers)])
        if args.submitters is not None:
            cmd.extend(["--submitters", str(args.submitters)])
        if args.tasks is not None:
            cmd.extend(["--tasks", str(args.tasks)])

        proc = subprocess.run(
            cmd,
            env=child_env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.stderr:
            for line in proc.stderr.splitlines():
                print(f"[run {run_idx}] {line}", file=sys.stderr)
        if proc.returncode != 0:
            raise RuntimeError(
                f"scale_load_driver failed (exit={proc.returncode})"
            )
        return json.loads(proc.stdout)


def format_summary_lines(median: dict[str, Any]) -> list[str]:
    cfg = median.get("config", {})
    reg = median.get("registration", {})
    unl = median.get("unloaded", {})
    burst = median.get("burst", {})
    hb = median.get("heartbeat_quiet", {})
    fan = median.get("blob_fanout", {})
    rec = median.get("reconnect", {})
    proc = median.get("master_process", {})
    pers = median.get("persistence", {})

    return [
        "=== SCALE-1 Summary (median) ===",
        f"Config: workers={cfg.get('workers')} submitters={cfg.get('submitters')} "
        f"tasks={cfg.get('tasks')} quick={cfg.get('quick')} "
        f"net=({cfg.get('base_latency_ms')}ms+{cfg.get('jitter_ms')}ms, loss={cfg.get('loss_prob')})",
        f"Registration: {reg.get('convergence_ms', 0):.1f} ms "
        f"({reg.get('workers_registered')} workers, {reg.get('total_handshakes')} handshakes)",
        f"Unloaded latency: assign p50={unl.get('submit_to_assign_ms', {}).get('p50', 0):.2f}ms "
        f"p99={unl.get('submit_to_assign_ms', {}).get('p99', 0):.2f}ms | "
        f"complete p50={unl.get('submit_to_complete_ms', {}).get('p50', 0):.2f}ms "
        f"p99={unl.get('submit_to_complete_ms', {}).get('p99', 0):.2f}ms",
        f"Burst: {burst.get('tasks_per_sec', 0):.1f} tasks/s in {burst.get('wall_ms', 0):.1f} ms "
        f"(failed={burst.get('failed')}) | "
        f"assign p95={burst.get('submit_to_assign_ms', {}).get('p95', 0):.2f}ms "
        f"complete p95={burst.get('submit_to_complete_ms', {}).get('p95', 0):.2f}ms",
        f"Heartbeat quiet: {hb.get('acks_per_sec', 0):.1f} acks/s, "
        f"rx={hb.get('rx_bytes_per_sec', 0):.0f} B/s, tx={hb.get('tx_bytes_per_sec', 0):.0f} B/s",
        f"Blob fan-out: {fan.get('workers')}x{fan.get('blob_size_bytes')}B "
        f"in {fan.get('wall_ms', 0):.1f} ms ({fan.get('throughput_mb_per_sec', 0):.2f} MiB/s)",
        f"Reconnect storm: {rec.get('convergence_ms', 0):.1f} ms "
        f"(handshakes {rec.get('handshakes_before')} -> {rec.get('handshakes_after')}, "
        f"post_failed={rec.get('post_failed')}, loss_events={rec.get('loss_events')})",
        f"Master process: peak_rss={proc.get('peak_rss_bytes', 0) / (1024**2):.1f} MiB "
        f"steady_rss={proc.get('steady_rss_bytes', 0) / (1024**2):.1f} MiB "
        f"peak_cpu={proc.get('peak_cpu_pct', 0):.1f}%",
        f"Persistence: journal={pers.get('journal_bytes', 0)}B state_dir={pers.get('state_dir_bytes', 0)}B "
        f"replay={pers.get('in_process_replay_ms', 0):.1f} ms "
        f"restart_ready={pers.get('master_restart_ready_ms', 0):.1f} ms",
    ]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OxideSwarm SCALE-1 fleet benchmark harness.")
    parser.add_argument("--bin", type=Path, default=default_master_bin(), help="Path to oxideswarm binary.")
    parser.add_argument(
        "--driver",
        type=Path,
        default=default_driver_bin(),
        help="Path to compiled scale_load_driver example binary.",
    )
    parser.add_argument("--workers", type=int, default=None, help="Override simulated worker count.")
    parser.add_argument("--submitters", type=int, default=None, help="Override concurrent submitter count.")
    parser.add_argument("--tasks", type=int, default=None, help="Override burst task count.")
    parser.add_argument("--runs", type=int, default=1, help="Number of iterations to aggregate by median.")
    parser.add_argument("--quick", action="store_true", help="Use quick smoke-test scale defaults.")
    parser.add_argument("--base-latency-ms", type=int, default=0, help="One-way simulated link latency (ms).")
    parser.add_argument("--jitter-ms", type=int, default=0, help="Uniform one-way FIFO jitter (ms).")
    parser.add_argument("--loss-prob", type=float, default=0.0, help="Simulated per-chunk packet loss probability.")
    parser.add_argument("--out", type=Path, default=None, help="Explicit output JSON file path.")
    parser.add_argument("--no-write", action="store_true", help="Do not write JSON report to disk.")
    parser.add_argument("--label", default="", help="Optional note stored in the JSON record.")
    return parser.parse_args(argv)


def default_result_path(
    results_dir: Path,
    today: str,
    host: dict[str, Any],
    cfg: dict[str, Any],
    workers_fallback: int = 0,
) -> Path:
    """Computes the canonical sanitized results path for loopback or impaired runs."""
    workers_label = cfg.get("workers", workers_fallback)
    os_slug = sanitize_slug(str(host.get("os", "unknown")))
    arch_slug = sanitize_slug(str(host.get("arch", "unknown")))
    base_latency = cfg.get("base_latency_ms", 0)
    jitter = cfg.get("jitter_ms", 0)
    loss_prob = cfg.get("loss_prob", 0.0)
    stem = f"{today}-scale1-{os_slug}-{arch_slug}-{workers_label}w"
    if base_latency > 0 or jitter > 0 or loss_prob > 0:
        stem = f"{stem}-imp-{base_latency}ms-{jitter}ms"
    return results_dir / f"{stem}.json"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.runs < 1:
        print("error: --runs must be >= 1", file=sys.stderr)
        return 2
    if not args.bin.is_file():
        print(f"error: master binary not found: {args.bin}", file=sys.stderr)
        return 2
    if not args.driver.is_file():
        print(f"error: driver binary not found: {args.driver}", file=sys.stderr)
        return 2

    started = dt.datetime.now(dt.timezone.utc)
    runs: list[dict[str, Any]] = []
    for idx in range(1, args.runs + 1):
        runs.append(run_once(args, idx))

    completed = dt.datetime.now(dt.timezone.utc)
    median = aggregate_median(runs)
    min_run = aggregate_extremum(runs, min)
    max_run = aggregate_extremum(runs, max)
    host = collect_sanitized_host_info()
    cfg = median.get("config", {})
    today = started.strftime("%Y-%m-%d")

    payload = {
        "schema_version": 1,
        "benchmark": "SCALE-1 simulated worker fleet load harness",
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "completed_utc": completed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": git_info(),
        "binary": binary_info(args.bin),
        "host": host,
        "network": network_info(cfg),
        "power": power_state(),
        "note": args.label,
        "runs_count": len(runs),
        "median": median,
        "min": min_run,
        "max": max_run,
        "runs": runs,
    }

    for line in format_summary_lines(median):
        print(line, file=sys.stderr)

    if not args.no_write:
        out_path = args.out or default_result_path(
            RESULTS_DIR,
            today,
            host,
            cfg,
            workers_fallback=args.workers or 0,
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_bytes = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
        out_path.write_bytes(out_bytes)
        print(f"Wrote SCALE-1 report to {out_path}", file=sys.stderr)

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
