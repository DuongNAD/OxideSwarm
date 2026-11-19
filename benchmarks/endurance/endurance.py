#!/usr/bin/env python3
"""
benchmarks/endurance/endurance.py - Large-transfer and resource endurance harness
for OxideSwarm (TEST-24, design 0065).

Python 3 standard library only. Follows the isolation and privacy rules of
benchmarks/grid/bench_grid.py: starts its own master and worker(s) on 127.0.0.1
with ephemeral ports (--port-file, --dashboard-port 0, --discovery-port 0), gives
every child process a private HOME/USERPROFILE/OXIDESWARM_HOME/TEMP/TMP, stops
spawned processes strictly via their Popen handles (never by process name), cleans
up its temporary working directory, enforces a free-disk safety margin before
starting, and writes one JSON record under benchmarks/endurance/results/ with
no host name, user name, private path, or non-loopback IP address.

Subcommands:
  transfer   Run large-transfer scenarios (T1..T9) and write a JSON record
  throttle   Run a standalone loopback TCP bandwidth/delay proxy on 127.0.0.1
  sample     Sample memory, open handles, threads, CPU time, and store directories
"""

from __future__ import annotations

import argparse
import base64
import datetime
import getpass
import hashlib
import json
import os
import platform
import queue
import random
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
IS_WINDOWS = os.name == "nt"

KIB = 1024
MIB = 1024 * 1024
GIB = 1024 * 1024 * 1024
DISK_RESERVE_BYTES = 10 * GIB
DEFAULT_SEED = 20261119
ALL_SCENARIOS = ("T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9")
DEFAULT_LOOPBACK_SCENARIOS = ("T1", "T2", "T3", "T4", "T6", "T7", "T8")

UPLOAD_RE = re.compile(
    r"Inputs: (\d+) files \(([^)]*)\); sent (\d+) of (\d+) blobs \(([^)]*)\), (\d+) already on the master"
)
EXEC_TIME_RE = re.compile(r"Execution Time:\s*(\d+)ms")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

_DRIVE_PATH_RE = re.compile(r"[A-Za-z]:\\[^\s\"'<>|,;\)\]]+")
_USER_UNIX_PATH_RE = re.compile(r"/(?:Users|home)/[^/\s\"'<>|,;\)\]]+(?:/[^\s\"'<>|,;\)\]]*)*")
_IPV4_PORT_RE = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3})(:\d+)?\b")

_SAFE_ENV_STRIP_EXACT = {
    "OXIDESWARM_SECRET",
    "OXIDESWARM_SECRET_FILE",
    "OXIDESWARM_HOME",
    "OXIDESWARM_MASTER",
    "OXIDESWARM_P2P_TICKET",
    "OXIDESWARM_RENDEZVOUS",
    "OXIDE_DEV_MODE",
    "OXIDE_SWARM_DASHBOARD_PORT",
    "OXIDE_SWARM_DASHBOARD_PORT_FILE",
}


# ---------------------------------------------------------------- disk & threshold math (pure)


def required_free_bytes(largest_file_bytes: int) -> int:
    """Design 0065 D1 #7: 3x the largest file plus 10 GiB headroom."""
    if largest_file_bytes < 0:
        raise ValueError("largest_file_bytes must be non-negative")
    return 3 * int(largest_file_bytes) + DISK_RESERVE_BYTES


def check_free_space(path: str, largest_file_bytes: int) -> Dict[str, Any]:
    """Refuse to start when free disk space is below 3 * largest_file + 10 GiB."""
    target = os.path.abspath(path)
    probe = target
    while probe and not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    usage = shutil.disk_usage(probe or target)
    need = required_free_bytes(largest_file_bytes)
    info = {
        "free_bytes": int(usage.free),
        "total_bytes": int(usage.total),
        "required_bytes": int(need),
        "largest_file_bytes": int(largest_file_bytes),
        "ok": usage.free >= need,
    }
    if not info["ok"]:
        raise RuntimeError(
            "insufficient free disk space: have %.2f GiB, need %.2f GiB "
            "(3 * %.2f GiB + 10.00 GiB)"
            % (
                usage.free / GIB,
                need / GIB,
                largest_file_bytes / GIB,
            )
        )
    return info


def check_t1_memory_flat(
    peak_small_bytes: Optional[int],
    peak_large_bytes: Optional[int],
    limit_bytes: int = 128 * MIB,
) -> Dict[str, Any]:
    """Design 0065 D2 T1: peak memory growth stays within 128 MiB across sizes."""
    if peak_small_bytes is None or peak_large_bytes is None:
        return {
            "peak_small_bytes": peak_small_bytes,
            "peak_large_bytes": peak_large_bytes,
            "delta_bytes": None,
            "limit_bytes": limit_bytes,
            "passed": False,
        }
    delta = int(peak_large_bytes) - int(peak_small_bytes)
    return {
        "peak_small_bytes": int(peak_small_bytes),
        "peak_large_bytes": int(peak_large_bytes),
        "delta_bytes": delta,
        "limit_bytes": limit_bytes,
        "passed": delta <= limit_bytes,
    }


def check_memory_plateau(
    second_hour_mean: float,
    last_hour_mean: float,
    pct: float = 0.10,
    floor_bytes: int = 32 * MIB,
) -> Dict[str, Any]:
    """Design 0065 D3 #2: last-hour mean RSS within max(10%, 32 MiB) of hour 2."""
    if second_hour_mean < 0 or last_hour_mean < 0:
        raise ValueError("memory means must be non-negative")
    allowance = max(second_hour_mean * pct, float(floor_bytes))
    limit = second_hour_mean + allowance
    return {
        "second_hour_mean_bytes": second_hour_mean,
        "last_hour_mean_bytes": last_hour_mean,
        "limit_bytes": limit,
        "passed": last_hour_mean <= limit,
    }


def check_handle_plateau(
    second_hour_mean: float,
    last_hour_mean: float,
    pct: float = 0.10,
) -> Dict[str, Any]:
    """Design 0065 D3 #2: last-hour open handles within 10% of hour 2."""
    if second_hour_mean < 0 or last_hour_mean < 0:
        raise ValueError("handle means must be non-negative")
    limit = second_hour_mean * (1.0 + pct)
    return {
        "second_hour_mean": second_hour_mean,
        "last_hour_mean": last_hour_mean,
        "limit": limit,
        "passed": last_hour_mean <= limit,
    }


def check_p99_ratio(
    second_hour_p99: float,
    last_hour_p99: float,
    max_ratio: float = 1.5,
) -> Dict[str, Any]:
    """Design 0065 D3 #4: last-hour p99 latency at most 1.5x hour 2."""
    if second_hour_p99 <= 0 or last_hour_p99 < 0:
        raise ValueError("p99 latencies must be positive / non-negative")
    ratio = last_hour_p99 / second_hour_p99
    return {
        "second_hour_p99": second_hour_p99,
        "last_hour_p99": last_hour_p99,
        "ratio": round(ratio, 4),
        "max_ratio": max_ratio,
        "passed": last_hour_p99 <= second_hour_p99 * max_ratio,
    }


def theoretical_single_blob_bound_mib_per_s(
    rtt_ms: Optional[float],
    chunk_mib: float = 4.0,
) -> Optional[float]:
    """Design 0065 Context/D2: max single-blob rate is 4 MiB per RTT."""
    if rtt_ms is None or rtt_ms <= 0:
        return None
    return round(chunk_mib / (rtt_ms / 1000.0), 3)


# ---------------------------------------------------------------- task ledger (D3 / TEST-25 reuse)


class TaskLedger:
    """Tracks submitted tasks and verifies exactly-once terminal accounting."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._submitted: Dict[str, Dict[str, Any]] = {}
        self._results: Dict[str, List[Dict[str, Any]]] = {}

    def record_submit(
        self,
        task_id: str,
        expected_hash: Optional[str] = None,
        expected_outcome: str = "ok",
    ) -> None:
        with self._lock:
            self._submitted[task_id] = {
                "expected_hash": expected_hash,
                "expected_outcome": expected_outcome,
            }

    def record_result(
        self,
        task_id: str,
        actual_hash: Optional[str] = None,
        outcome: str = "ok",
    ) -> None:
        with self._lock:
            self._results.setdefault(task_id, []).append(
                {"actual_hash": actual_hash, "outcome": outcome}
            )

    def audit(self) -> Dict[str, Any]:
        with self._lock:
            lost: List[str] = []
            duplicates: List[str] = []
            hash_mismatches: List[Dict[str, Any]] = []
            unexpected_outcomes: List[Dict[str, Any]] = []
            untracked_results: List[str] = []

            for tid, sub in self._submitted.items():
                res_list = self._results.get(tid, [])
                if not res_list:
                    lost.append(tid)
                    continue
                if len(res_list) > 1:
                    duplicates.append(tid)
                res = res_list[0]
                if res["outcome"] != sub["expected_outcome"]:
                    unexpected_outcomes.append(
                        {
                            "task_id": tid,
                            "expected": sub["expected_outcome"],
                            "actual": res["outcome"],
                        }
                    )
                if (
                    sub["expected_hash"] is not None
                    and res["outcome"] == "ok"
                    and res["actual_hash"] != sub["expected_hash"]
                ):
                    hash_mismatches.append(
                        {
                            "task_id": tid,
                            "expected": sub["expected_hash"],
                            "actual": res["actual_hash"],
                        }
                    )

            for tid in self._results:
                if tid not in self._submitted:
                    untracked_results.append(tid)

            ok = (
                not lost
                and not duplicates
                and not hash_mismatches
                and not unexpected_outcomes
                and not untracked_results
            )
            return {
                "submitted": len(self._submitted),
                "completed": sum(1 for r in self._results.values() if r),
                "lost_tasks": sorted(lost),
                "duplicate_results": sorted(duplicates),
                "hash_mismatches": hash_mismatches,
                "unexpected_outcomes": unexpected_outcomes,
                "untracked_results": sorted(untracked_results),
                "ok": ok,
            }


# ---------------------------------------------------------------- seeded data & verification


def generate_seeded_file(
    path: str,
    size_bytes: int,
    seed: int,
    chunk_bytes: int = 4 * MIB,
) -> Dict[str, Any]:
    """Stream deterministic random bytes to `path` while hashing with SHA-256."""
    if size_bytes < 0:
        raise ValueError("size_bytes must be non-negative")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    rng = random.Random(seed)
    hasher = hashlib.sha256()
    remaining = int(size_bytes)
    with open(path, "wb") as f:
        while remaining > 0:
            n = min(chunk_bytes, remaining)
            buf = rng.randbytes(n) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(n))
            f.write(buf)
            hasher.update(buf)
            remaining -= n
    return {
        "size_bytes": int(size_bytes),
        "sha256": hasher.hexdigest(),
        "seed": int(seed),
    }


def generate_seeded_tree(
    root: str,
    file_count: int,
    seed: int,
    min_bytes: int = KIB,
    max_bytes: int = 16 * KIB,
    shards: int = 100,
) -> Dict[str, Any]:
    """Create `file_count` deterministic files (1..16 KiB) and return manifest SHA-256."""
    if file_count < 0 or min_bytes < 0 or max_bytes < min_bytes or shards < 1:
        raise ValueError("invalid tree parameters")
    os.makedirs(root, exist_ok=True)
    for s in range(shards):
        os.makedirs(os.path.join(root, "d%02d" % s), exist_ok=True)
    rng = random.Random(seed)
    total_bytes = 0
    manifest_rows: List[str] = []
    for i in range(file_count):
        sub = "d%02d" % (i % shards)
        name = "f%06d.bin" % i
        rel = "%s/%s" % (sub, name)
        fpath = os.path.join(root, sub, name)
        size = rng.randint(min_bytes, max_bytes)
        buf = rng.randbytes(size) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(size))
        with open(fpath, "wb") as f:
            f.write(buf)
        digest = hashlib.sha256(buf).hexdigest()
        total_bytes += size
        manifest_rows.append("%s\t%d\t%s\n" % (rel, size, digest))
    manifest_rows.sort()
    tree_hash = hashlib.sha256("".join(manifest_rows).encode("utf-8")).hexdigest()
    return {
        "files": int(file_count),
        "total_bytes": int(total_bytes),
        "tree_sha256": tree_hash,
        "seed": int(seed),
    }


def sha256_file(path: str, chunk_bytes: int = 4 * MIB) -> Tuple[int, str]:
    hasher = hashlib.sha256()
    total = 0
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk_bytes)
            if not buf:
                break
            total += len(buf)
            hasher.update(buf)
    return total, hasher.hexdigest()


# Worker-side verification/generation scripts executed via `[python, "-c", b64_runner, b64, ...]`
_B64_EXEC = "import base64,sys;exec(base64.b64decode(sys.argv[1]).decode('utf-8'))"

_WORKER_VERIFY_FILE_SRC = """
import hashlib, os, sys
path, exp_size, exp_sha = sys.argv[2], int(sys.argv[3]), sys.argv[4]
if not os.path.isfile(path):
    print("VERIFY_MISSING", path)
    sys.exit(3)
h = hashlib.sha256()
total = 0
with open(path, "rb") as f:
    while True:
        chunk = f.read(4 * 1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        h.update(chunk)
got_sha = h.hexdigest()
if total != exp_size or got_sha != exp_sha:
    print(f"VERIFY_MISMATCH size={total}/{exp_size} sha={got_sha}/{exp_sha}")
    sys.exit(2)
print(f"VERIFY_OK {total} {got_sha}")
""".strip()

_WORKER_VERIFY_TREE_SRC = """
import hashlib, os, sys
root, exp_count, exp_bytes, exp_sha = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
rows = []
total = 0
for dirpath, _dirs, files in os.walk(root):
    for fn in files:
        full = os.path.join(dirpath, fn)
        rel = os.path.relpath(full, root).replace(os.sep, "/")
        h = hashlib.sha256()
        sz = 0
        with open(full, "rb") as f:
            while True:
                b = f.read(1024 * 1024)
                if not b:
                    break
                sz += len(b)
                h.update(b)
        total += sz
        rows.append(f"{rel}\\t{sz}\\t{h.hexdigest()}\\n")
rows.sort()
tree_sha = hashlib.sha256("".join(rows).encode("utf-8")).hexdigest()
if len(rows) != exp_count or total != exp_bytes or tree_sha != exp_sha:
    print(f"VERIFY_TREE_MISMATCH count={len(rows)}/{exp_count} bytes={total}/{exp_bytes} sha={tree_sha}/{exp_sha}")
    sys.exit(2)
print(f"VERIFY_TREE_OK {len(rows)} {total} {tree_sha}")
""".strip()

_WORKER_GEN_OUTPUT_SRC = """
import hashlib, random, sys
out_path, size_bytes, seed = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
verify_in = sys.argv[5] if len(sys.argv) > 5 else ""
if verify_in and verify_in != "-":
    in_path, exp_in_sz, exp_in_sha = verify_in.split("|", 2)
    h_in = hashlib.sha256()
    got_sz = 0
    with open(in_path, "rb") as f:
        while True:
            b = f.read(4 * 1024 * 1024)
            if not b:
                break
            got_sz += len(b)
            h_in.update(b)
    if got_sz != int(exp_in_sz) or h_in.hexdigest() != exp_in_sha:
        print(f"VERIFY_INPUT_MISMATCH {got_sz}/{exp_in_sz}")
        sys.exit(2)
    print(f"VERIFY_OK {got_sz} {h_in.hexdigest()}")
rng = random.Random(seed)
h_out = hashlib.sha256()
rem = size_bytes
with open(out_path, "wb") as f:
    while rem > 0:
        n = min(4 * 1024 * 1024, rem)
        b = rng.randbytes(n) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(n))
        f.write(b)
        h_out.update(b)
        rem -= n
print(f"GEN_OK {size_bytes} {h_out.hexdigest()}")
""".strip()


def _b64(src: str) -> str:
    return base64.b64encode(src.encode("utf-8")).decode("ascii")


# ---------------------------------------------------------------- CLI parsers & environment metadata


def parse_inputs_line(text: str) -> Optional[Dict[str, Any]]:
    """Parse `Inputs: ...` line printed on stderr by `oxideswarm submit --input`."""
    m = UPLOAD_RE.search(text or "")
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


def parse_execution_time_ms(text: str) -> Optional[int]:
    m = EXEC_TIME_RE.search(text or "")
    return int(m.group(1)) if m else None


def decode_bytes_field(val: Any) -> str:
    if isinstance(val, str):
        return val
    if isinstance(val, list):
        return bytes(int(x) & 0xFF for x in val).decode("utf-8", errors="replace")
    return ""


def extract_json_payload(stdout: str) -> Optional[Dict[str, Any]]:
    text = (stdout or "").strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except ValueError:
        pass
    idx = text.find("{")
    if idx >= 0:
        try:
            obj = json.loads(text[idx:])
            return obj if isinstance(obj, dict) else None
        except ValueError:
            return None
    return None


def list_blob_objects(blob_root: str) -> Dict[str, int]:
    """Return `{blake3_hex: size_bytes}` for verified files in `<blob_root>/objects/`."""
    objects_dir = os.path.join(blob_root, "objects")
    out: Dict[str, int] = {}
    if not os.path.isdir(objects_dir):
        return out
    for entry in os.scandir(objects_dir):
        if not entry.is_dir():
            continue
        for leaf in os.scandir(entry.path):
            if HEX64_RE.match(leaf.name) and leaf.is_file():
                try:
                    out[leaf.name] = leaf.stat().st_size
                except OSError:
                    pass
    return out


def list_tmp_entries(blob_root: str) -> List[str]:
    """Return sorted entry names in `<blob_root>/tmp` (empty if absent)."""
    tmp_dir = os.path.join(blob_root, "tmp")
    if not os.path.isdir(tmp_dir):
        return []
    return sorted(os.listdir(tmp_dir))


def dir_bytes(path: str) -> int:
    total = 0
    if not path or not os.path.exists(path):
        return 0
    for root, _dirs, files in os.walk(path):
        for fn in files:
            try:
                total += os.path.getsize(os.path.join(root, fn))
            except OSError:
                pass
    return total


def sh(cmd: List[str], timeout: float = 10.0) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout.strip()
    except Exception:
        return ""


def hardware() -> Dict[str, Any]:
    info: Dict[str, Any] = {
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
            with open("/proc/cpuinfo", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        info["ram_bytes"] = int(line.split()[1]) * 1024
                        break
        except OSError:
            pass
    elif sysname == "Windows":
        out = sh(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_Processor).Name; "
                "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory",
            ],
            timeout=15.0,
        )
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        if len(lines) >= 2 and lines[-1].isdigit():
            info["cpu_model"] = lines[0]
            info["ram_bytes"] = int(lines[-1])
    return info


def os_info() -> Dict[str, str]:
    """Generic OS facts only. Never host name, user name, or path."""
    return {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
    }


def windows_realtime_av_enabled() -> Optional[bool]:
    """Design 0065 D1 #5: record whether Windows real-time AV scanning is enabled."""
    if platform.system() != "Windows":
        return None
    out = sh(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            "try { (Get-MpComputerStatus).RealTimeProtectionEnabled } catch { '' }",
        ],
        timeout=15.0,
    ).lower()
    if out == "true":
        return True
    if out == "false":
        return False
    return None


def power_state() -> Any:
    sysname = platform.system()
    if sysname == "Darwin":
        return sh(["pmset", "-g", "batt"]).splitlines()[0:2]
    if sysname == "Linux":
        base = "/sys/class/power_supply"
        try:
            res = {}
            for n in os.listdir(base):
                st = os.path.join(base, n, "status")
                if os.path.exists(st):
                    with open(st, encoding="utf-8", errors="replace") as f:
                        res[n] = f.read().strip()
            return res or "no battery found"
        except OSError:
            return "unknown"
    return "unknown (not recorded on this platform)"


def git_info() -> Dict[str, Any]:
    safe_repo = REPO.replace(os.sep, "/")
    commit = sh(["git", "-c", "safe.directory=" + safe_repo, "-C", REPO, "rev-parse", "HEAD"])
    dirty = sh(
        [
            "git",
            "-c",
            "safe.directory=" + safe_repo,
            "-C",
            REPO,
            "status",
            "--porcelain",
            "--",
            ":!benchmarks/endurance/results",
        ]
    )
    return {"commit": commit or "unknown", "dirty_tree": bool(dirty)}


def build_profile(binary: str) -> str:
    p = binary.replace("\\", "/").lower()
    return "release" if "/release/" in p else "debug" if "/debug/" in p else "unknown"


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-") or "unknown"


def result_path(
    directory: str,
    when: datetime.datetime,
    link: str,
    os_facts: Dict[str, str],
    hw: Dict[str, Any],
) -> str:
    """<UTC date>-<link>-<os>-<arch>-<cpu short>.json; never contains host/user."""
    system = {"darwin": "macos"}.get(os_facts["system"].lower(), os_facts["system"].lower())
    cpu = _slug(re.sub(r"\(R\)|\(TM\)|CPU|Processor|@.*$", " ", hw.get("cpu_model") or ""))[:30].strip("-")
    base = "%s-%s-%s-%s-%s" % (
        when.strftime("%Y-%m-%d"),
        _slug(link),
        _slug(system),
        _slug(os_facts["machine"]),
        cpu or "cpu",
    )
    path = os.path.join(directory, base + ".json")
    n = 1
    while os.path.exists(path):
        n += 1
        path = os.path.join(directory, "%s-%d.json" % (base, n))
    return path


def _sensitive_tokens(extra_secrets: Iterable[str] = ()) -> List[str]:
    tokens: List[str] = []
    for cand in (
        socket.gethostname(),
        platform.node(),
        os.environ.get("COMPUTERNAME", ""),
        os.environ.get("HOSTNAME", ""),
        os.environ.get("USERNAME", ""),
        os.environ.get("USER", ""),
        REPO,
        os.path.expanduser("~"),
    ):
        if cand:
            tokens.append(cand)
            if "." in cand and not cand.replace(".", "").isdigit():
                tokens.append(cand.split(".", 1)[0])
    try:
        user = getpass.getuser()
        if user:
            tokens.append(user)
    except Exception:
        pass
    for item in extra_secrets:
        if item:
            tokens.append(str(item))
            tokens.append(str(item).replace("\\", "/"))
            tokens.append(str(item).replace("/", "\\"))
    dedup = {t for t in tokens if len(t.strip()) >= 3}
    return sorted(dedup, key=len, reverse=True)


def _replace_non_loopback_ipv4(m: re.Match) -> str:
    ip = m.group(1)
    port = m.group(2) or ""
    parts = ip.split(".")
    try:
        octets = [int(p) for p in parts]
    except ValueError:
        return m.group(0)
    if any(o < 0 or o > 255 for o in octets):
        return m.group(0)
    if ip == "127.0.0.1" or ip == "0.0.0.0":
        return m.group(0)
    # Ignore short version-looking triples only; 4-octet non-loopback IPs are redacted.
    return "<ip>" + (":<port>" if port else "")


def _whole_token_pattern(token: str) -> re.Pattern:
    """Compile a case-insensitive whole-token matcher so short tokens do not hit substrings."""
    escaped = re.escape(token.strip())
    return re.compile(r"(?<![A-Za-z0-9_-])" + escaped + r"(?![A-Za-z0-9_-])", re.IGNORECASE)


def redact_text(text: str, extra_secrets: Iterable[str] = ()) -> str:
    """Scrub host names, user names, local paths, and non-loopback IPv4 addresses."""
    if not text:
        return text
    out = str(text)
    out = _DRIVE_PATH_RE.sub("<path>", out)
    out = _USER_UNIX_PATH_RE.sub("<path>", out)
    for tok in _sensitive_tokens(extra_secrets):
        out = _whole_token_pattern(tok).sub("<redacted>", out)
    out = _IPV4_PORT_RE.sub(_replace_non_loopback_ipv4, out)
    return out


def _scan_string_privacy(
    value: str,
    field_path: str,
    token_patterns: List[re.Pattern],
) -> List[Dict[str, str]]:
    hits: List[Dict[str, str]] = []
    lower = value.lower()
    for pat in token_patterns:
        if pat.search(value):
            hits.append({"field": field_path, "reason": "matched local host/user/path token"})
            break
    if re.search(r"[a-z]:[/\\]{1,2}(?:users|documents and settings)[/\\]", lower) or re.search(
        r"/(?:users|home)/[a-z0-9_.-]+", lower
    ):
        hits.append({"field": field_path, "reason": "matched user home path"})
    for m in _IPV4_PORT_RE.finditer(value):
        ip = m.group(1)
        if ip in ("127.0.0.1", "0.0.0.0"):
            continue
        octets = [int(x) for x in ip.split(".")]
        if any(o > 255 for o in octets):
            continue
        is_rfc1918 = (
            octets[0] == 10
            or (octets[0] == 172 and 16 <= octets[1] <= 31)
            or (octets[0] == 192 and octets[1] == 168)
            or (octets[0] == 100 and 64 <= octets[1] <= 127)
        )
        if is_rfc1918:
            hits.append({"field": field_path, "reason": "matched non-loopback IP (%s)" % ip})
    return hits


def find_privacy_hits(
    obj: Any,
    forbidden: Iterable[str] = (),
    _path: str = "$",
    _patterns: Optional[List[re.Pattern]] = None,
) -> List[Dict[str, str]]:
    """Inspect serialized JSON string values for private tokens, home paths, or non-loopback IPs."""
    pats = _patterns if _patterns is not None else [_whole_token_pattern(t) for t in _sensitive_tokens(forbidden)]
    hits: List[Dict[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            hits.extend(find_privacy_hits(v, forbidden=forbidden, _path="%s.%s" % (_path, k), _patterns=pats))
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            hits.extend(find_privacy_hits(v, forbidden=forbidden, _path="%s[%d]" % (_path, idx), _patterns=pats))
    elif isinstance(obj, str):
        hits.extend(_scan_string_privacy(obj, _path, pats))
    return hits


def assert_record_private(record_text: str, forbidden: Iterable[str] = ()) -> None:
    """Raise ValueError naming the offending field(s) if a record leaks host, user, path, or private IP."""
    try:
        parsed = json.loads(record_text)
    except ValueError:
        pats = [_whole_token_pattern(t) for t in _sensitive_tokens(forbidden)]
        hits = _scan_string_privacy(record_text, "$", pats)
    else:
        hits = find_privacy_hits(parsed, forbidden=forbidden)
    if hits:
        summary = "; ".join("%s (%s)" % (h["field"], h["reason"]) for h in hits[:5])
        raise ValueError("privacy check failed: %s" % summary)


def _quarantine_path(path: str) -> str:
    abs_path = os.path.abspath(path)
    stem, ext = os.path.splitext(abs_path)
    cand = "%s.quarantine%s" % (stem, ext or ".json")
    n = 1
    while os.path.exists(cand):
        n += 1
        cand = "%s.quarantine-%d%s" % (stem, n, ext or ".json")
    return cand


def write_record(path: str, record: Dict[str, Any], forbidden: Iterable[str] = ()) -> Dict[str, Any]:
    """Write record to `path`, or to `<stem>.quarantine.json` naming offending fields on a privacy hit."""
    text = json.dumps(record, indent=2) + "\n"
    hits = find_privacy_hits(record, forbidden=forbidden)
    if hits:
        qpath = _quarantine_path(path)
        os.makedirs(os.path.dirname(os.path.abspath(qpath)), exist_ok=True)
        with open(qpath, "w", encoding="utf-8") as f:
            f.write(text)
        summary = "; ".join("%s (%s)" % (h["field"], h["reason"]) for h in hits)
        print(
            "PRIVACY WARNING: quarantined record with %d hit(s) [%s] to %s" % (len(hits), summary, qpath),
            file=sys.stderr,
            flush=True,
        )
        return {"path": qpath, "quarantined": True, "hits": hits, "text": text}
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return {"path": path, "quarantined": False, "hits": [], "text": text}


# ---------------------------------------------------------------- loopback TCP throttle proxy (D1 #6)


class TokenBucket:
    """Thread-safe byte-rate limiter allowing adaptive sleep-oversleep forgiveness."""

    def __init__(self, rate_bps: Optional[float]) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._rate_bps = float(rate_bps) if rate_bps and rate_bps > 0 else None
        self._tokens = 0.0
        self._last = time.monotonic()
        self._closed = False

    @property
    def rate_bps(self) -> Optional[float]:
        with self._lock:
            return self._rate_bps

    def set_rate_bps(self, rate_bps: Optional[float]) -> None:
        with self._lock:
            self._rate_bps = float(rate_bps) if rate_bps and rate_bps > 0 else None
            self._tokens = 0.0
            self._last = time.monotonic()
            self._cond.notify_all()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._cond.notify_all()

    def suggested_chunk_size(self) -> int:
        with self._lock:
            if self._rate_bps is None:
                return 65536
            # ~35 ms per chunk balances Windows timer resolution against smooth pacing.
            return max(4096, min(32768, int(self._rate_bps * 0.035)))

    def consume(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        with self._lock:
            if self._rate_bps is None or self._closed:
                return
            now = time.monotonic()
            elapsed = max(0.0, now - self._last)
            # Keep tiny idle burst credit so short transfers stay within +-10%, while preserving
            # recently earned oversleep forgiveness between active chunks.
            max_credit = float(max(1024, min(4096, int(self._rate_bps * 0.02))))
            if elapsed >= 0.5:
                self._tokens = min(max_credit, self._tokens + elapsed * self._rate_bps)
            else:
                self._tokens = max(self._tokens, min(max_credit, self._tokens + elapsed * self._rate_bps))
            self._last = now
            self._tokens -= float(nbytes)
            while self._tokens < -1e-6 and not self._closed and self._rate_bps is not None:
                wait = (-self._tokens) / self._rate_bps
                self._cond.wait(timeout=wait)
                if self._closed or self._rate_bps is None:
                    return
                now2 = time.monotonic()
                dt = max(0.0, now2 - self._last)
                self._tokens += dt * self._rate_bps
                self._last = now2


class ThrottleProxy:
    """User-space TCP proxy on 127.0.0.1 with bandwidth shaping, delay, and link cut."""

    def __init__(
        self,
        target_host: str,
        target_port: int,
        listen_host: str = "127.0.0.1",
        listen_port: int = 0,
        rate_mbps: Optional[float] = None,
        delay_ms: float = 0.0,
    ) -> None:
        if listen_host != "127.0.0.1":
            raise ValueError("throttle proxy binds 127.0.0.1 only (got %r)" % listen_host)
        self.target_host = target_host
        self.target_port = int(target_port)
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.one_way_s = max(0.0, float(delay_ms) / 2000.0)
        rate_bps = (float(rate_mbps) * 125_000.0) if rate_mbps and rate_mbps > 0 else None
        self._c2s_bucket = TokenBucket(rate_bps)
        self._s2c_bucket = TokenBucket(rate_bps)

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._paused_until = 0.0
        self._srv: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._worker_threads: List[threading.Thread] = []
        self._active_pairs: List[Tuple[socket.socket, socket.socket]] = []
        self._pair_done: Dict[int, int] = {}

        self.bytes_c2s = 0
        self.bytes_s2c = 0
        self.connections_accepted = 0
        self.cuts = 0
        self.port = 0
        self.bound_addr = ""

    def start(self) -> "ThrottleProxy":
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.listen_host, self.listen_port))
        srv.listen(128)
        srv.settimeout(0.2)
        self._srv = srv
        self.port = srv.getsockname()[1]
        self.bound_addr = "%s:%d" % (self.listen_host, self.port)
        self._accept_thread = threading.Thread(target=self._accept_loop, name="throttle-accept", daemon=True)
        self._accept_thread.start()
        return self

    def set_rate(self, rate_mbps: Optional[float]) -> None:
        rate_bps = (float(rate_mbps) * 125_000.0) if rate_mbps and rate_mbps > 0 else None
        self._c2s_bucket.set_rate_bps(rate_bps)
        self._s2c_bucket.set_rate_bps(rate_bps)

    def cut_links(self, pause_s: float = 0.0) -> None:
        with self._lock:
            self.cuts += 1
            if pause_s > 0:
                self._paused_until = max(self._paused_until, time.monotonic() + pause_s)
            pairs = list(self._active_pairs)
            self._active_pairs.clear()
        for a, b in pairs:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass

    def stop(self) -> None:
        self._stop_event.set()
        self._c2s_bucket.close()
        self._s2c_bucket.close()
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass
        with self._lock:
            pairs = list(self._active_pairs)
            self._active_pairs.clear()
        for a, b in pairs:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=3.0)
        for t in list(self._worker_threads):
            t.join(timeout=2.0)

    def _accept_loop(self) -> None:
        assert self._srv is not None
        while not self._stop_event.is_set():
            try:
                client, _addr = self._srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._lock:
                paused_until = self._paused_until
            if time.monotonic() < paused_until:
                try:
                    client.close()
                except OSError:
                    pass
                continue
            try:
                upstream = socket.create_connection((self.target_host, self.target_port), timeout=5.0)
                upstream.settimeout(None)
                client.settimeout(None)
            except OSError:
                try:
                    client.close()
                except OSError:
                    pass
                continue
            for s in (client, upstream):
                try:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
            pair = (client, upstream)
            with self._lock:
                self.connections_accepted += 1
                self._active_pairs.append(pair)
                self._worker_threads = [t for t in self._worker_threads if t.is_alive()]
            t1 = threading.Thread(
                target=self._bridge_one_way,
                args=(client, upstream, self._c2s_bucket, "c2s", pair),
                daemon=True,
            )
            t2 = threading.Thread(
                target=self._bridge_one_way,
                args=(upstream, client, self._s2c_bucket, "s2c", pair),
                daemon=True,
            )
            with self._lock:
                self._worker_threads.extend([t1, t2])
            t1.start()
            t2.start()

    def _bridge_one_way(
        self,
        src: socket.socket,
        dst: socket.socket,
        bucket: TokenBucket,
        direction: str,
        pair: Tuple[socket.socket, socket.socket],
    ) -> None:
        # Pipeline rate-limiting (reader -> queue) with one-way propagation delay (queue -> writer)
        # so propagation latency does not multiply per-chunk serialization waits.
        q: "queue.Queue[Optional[Tuple[float, bytes]]]" = queue.Queue(maxsize=256)
        local_stop = threading.Event()

        def writer() -> None:
            try:
                while not self._stop_event.is_set() and not local_stop.is_set():
                    try:
                        item = q.get(timeout=0.2)
                    except queue.Empty:
                        continue
                    if item is None:
                        break
                    deliver_at, data = item
                    rem = deliver_at - time.monotonic()
                    if rem > 0:
                        if self._stop_event.wait(rem):
                            break
                    if not data:
                        try:
                            dst.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
                        break
                    dst.sendall(data)
            except OSError:
                local_stop.set()

        wt = threading.Thread(target=writer, daemon=True)
        wt.start()
        src.settimeout(0.5)
        try:
            while not self._stop_event.is_set() and not local_stop.is_set():
                try:
                    data = src.recv(bucket.suggested_chunk_size())
                except socket.timeout:
                    continue
                if not data:
                    while not self._stop_event.is_set() and not local_stop.is_set():
                        try:
                            q.put((time.monotonic() + self.one_way_s, b""), timeout=0.2)
                            break
                        except queue.Full:
                            continue
                    break
                bucket.consume(len(data))
                if self._stop_event.is_set() or local_stop.is_set():
                    break
                with self._lock:
                    if direction == "c2s":
                        self.bytes_c2s += len(data)
                    else:
                        self.bytes_s2c += len(data)
                deliver_at = time.monotonic() + self.one_way_s
                while not self._stop_event.is_set() and not local_stop.is_set():
                    try:
                        q.put((deliver_at, data), timeout=0.2)
                        break
                    except queue.Full:
                        continue
        except OSError:
            local_stop.set()
        finally:
            wt.join(timeout=2.0)
            with self._lock:
                pid = id(pair)
                done = self._pair_done.get(pid, 0) + 1
                self._pair_done[pid] = done
                should_close = local_stop.is_set() or self._stop_event.is_set() or done >= 2
                if should_close:
                    self._pair_done.pop(pid, None)
                    if pair in self._active_pairs:
                        self._active_pairs.remove(pair)
            if should_close:
                for s in pair:
                    try:
                        s.close()
                    except OSError:
                        pass


# ---------------------------------------------------------------- process & disk sampler (D1 #5)


def _parse_ps_time(raw: str) -> float:
    """Parse `ps -o time=` (MM:SS.xx, HH:MM:SS, or D-HH:MM:SS) to seconds."""
    text = (raw or "").strip()
    if not text:
        return 0.0
    days = 0
    if "-" in text:
        dpart, text = text.split("-", 1)
        days = int(dpart)
    parts = [float(p) for p in text.split(":")]
    if len(parts) == 2:
        return days * 86400 + parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return days * 86400 + parts[0] * 3600 + parts[1] * 60 + parts[2]
    return float(parts[0])


def sample_processes(pid_map: Dict[str, int]) -> Dict[str, Dict[str, Any]]:
    """Sample RSS bytes, open handles, thread count, and CPU seconds per role."""
    clean_map = {role: int(pid) for role, pid in pid_map.items() if pid and int(pid) > 0}
    if not clean_map:
        return {}
    sysname = platform.system()
    results: Dict[str, Dict[str, Any]] = {}

    if sysname == "Windows":
        pids_csv = ",".join(str(p) for p in clean_map.values())
        ps_cmd = (
            "$procs = Get-Process -Id " + pids_csv + " -ErrorAction SilentlyContinue; "
            "$out = @($procs | ForEach-Object { "
            "[pscustomobject]@{id=$_.Id; rss=$_.WorkingSet64; handles=$_.HandleCount; "
            "threads=$_.Threads.Count; cpu=[double]$_.TotalProcessorTime.TotalSeconds} "
            "}); ConvertTo-Json -InputObject $out -Compress"
        )
        raw = sh(["powershell", "-NoProfile", "-Command", ps_cmd], timeout=15.0)
        by_pid: Dict[int, Dict[str, Any]] = {}
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    parsed = [parsed]
                if isinstance(parsed, list):
                    for item in parsed:
                        if isinstance(item, dict) and "id" in item:
                            by_pid[int(item["id"])] = {
                                "rss_bytes": int(item.get("rss") or 0),
                                "handles": int(item.get("handles") or 0),
                                "threads": int(item.get("threads") or 0),
                                "cpu_seconds": round(float(item.get("cpu") or 0.0), 3),
                            }
            except ValueError:
                pass
        for role, pid in clean_map.items():
            if pid in by_pid:
                results[role] = by_pid[pid]
        return results

    if sysname == "Darwin":
        for role, pid in clean_map.items():
            ps_out = sh(["ps", "-o", "rss=,time=", "-p", str(pid)], timeout=5.0)
            if not ps_out:
                continue
            toks = ps_out.split()
            if len(toks) < 2:
                continue
            rss_bytes = int(toks[0]) * 1024
            cpu_s = round(_parse_ps_time(toks[1]), 3)
            threads_out = sh(["ps", "-M", "-p", str(pid)], timeout=5.0)
            threads = max(1, len(threads_out.splitlines()) - 1) if threads_out else 1
            lsof_out = sh(["lsof", "-n", "-P", "-p", str(pid)], timeout=10.0)
            handles = max(0, len(lsof_out.splitlines()) - 1) if lsof_out else 0
            results[role] = {
                "rss_bytes": rss_bytes,
                "handles": handles,
                "threads": threads,
                "cpu_seconds": cpu_s,
            }
        return results

    # Linux (/proc)
    clk_tck = float(os.sysconf("SC_CLK_TCK")) if hasattr(os, "sysconf") else 100.0
    for role, pid in clean_map.items():
        proc_dir = "/proc/%d" % pid
        if not os.path.isdir(proc_dir):
            continue
        rss_bytes = 0
        threads = 0
        cpu_s = 0.0
        handles = 0
        try:
            with open(os.path.join(proc_dir, "status"), encoding="utf-8", errors="replace") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            rss_bytes = int(parts[1]) * 1024
                    elif line.startswith("Threads:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            threads = int(parts[1])
            with open(os.path.join(proc_dir, "stat"), encoding="utf-8", errors="replace") as f:
                stat_text = f.read()
            rparen = stat_text.rfind(")")
            if rparen >= 0:
                fields = stat_text[rparen + 2 :].split()
                if len(fields) > 12:
                    utime = int(fields[11])
                    stime = int(fields[12])
                    cpu_s = round((utime + stime) / clk_tck, 3)
            handles = len(os.listdir(os.path.join(proc_dir, "fd")))
            results[role] = {
                "rss_bytes": rss_bytes,
                "handles": handles,
                "threads": threads,
                "cpu_seconds": cpu_s,
            }
        except OSError:
            continue
    return results


def sample_dirs(dir_map: Dict[str, str]) -> Dict[str, int]:
    return {name: dir_bytes(path) for name, path in dir_map.items()}


class BackgroundSampler:
    """Polls process resources and storage directory sizes at `interval_s`."""

    def __init__(
        self,
        pid_provider: Callable[[], Dict[str, int]],
        dir_map: Dict[str, str],
        interval_s: float = 1.0,
    ) -> None:
        self._pid_provider = pid_provider
        self._dir_map = dict(dir_map)
        self._interval_s = max(0.25, float(interval_s))
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._samples: List[Dict[str, Any]] = []
        self._t0 = time.monotonic()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "BackgroundSampler":
        self._t0 = time.monotonic()
        self.take_sample()
        self._thread = threading.Thread(target=self._loop, name="endurance-sampler", daemon=True)
        self._thread.start()
        return self

    def take_sample(self) -> None:
        pids = self._pid_provider()
        procs = sample_processes(pids)
        dirs = sample_dirs(self._dir_map)
        row = {
            "t_s": round(time.monotonic() - self._t0, 2),
            "processes": procs,
            "dirs": dirs,
        }
        with self._lock:
            self._samples.append(row)

    def _loop(self) -> None:
        while not self._stop.wait(self._interval_s):
            self.take_sample()

    def stop(self) -> List[Dict[str, Any]]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)
        self.take_sample()
        with self._lock:
            return list(self._samples)

    @staticmethod
    def summarize(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        proc_summary: Dict[str, Dict[str, Any]] = {}
        dir_summary: Dict[str, Dict[str, int]] = {}
        for row in samples:
            for role, stats in row.get("processes", {}).items():
                entry = proc_summary.setdefault(
                    role,
                    {
                        "peak_rss_bytes": 0,
                        "min_rss_bytes": stats["rss_bytes"],
                        "peak_handles": 0,
                        "peak_threads": 0,
                        "max_cpu_seconds": 0.0,
                        "samples": 0,
                    },
                )
                entry["peak_rss_bytes"] = max(entry["peak_rss_bytes"], stats["rss_bytes"])
                entry["min_rss_bytes"] = min(entry["min_rss_bytes"], stats["rss_bytes"])
                entry["peak_handles"] = max(entry["peak_handles"], stats["handles"])
                entry["peak_threads"] = max(entry["peak_threads"], stats["threads"])
                entry["max_cpu_seconds"] = max(entry["max_cpu_seconds"], stats["cpu_seconds"])
                entry["samples"] += 1
            for dname, sz in row.get("dirs", {}).items():
                dentry = dir_summary.setdefault(dname, {"peak_bytes": 0, "final_bytes": 0})
                dentry["peak_bytes"] = max(dentry["peak_bytes"], sz)
                dentry["final_bytes"] = sz
        return {
            "sample_count": len(samples),
            "processes": proc_summary,
            "dirs": dir_summary,
        }


# ---------------------------------------------------------------- isolated cluster management


class EnduranceCluster:
    """Isolated master + worker(s) on 127.0.0.1 using ephemeral ports and private homes."""

    def __init__(
        self,
        binary: str,
        workdir: str,
        workers: int = 1,
        blob_store_mb: Optional[int] = None,
        blob_cache_mb: Optional[int] = None,
        output_limit_mb: Optional[int] = None,
        state_dir: Optional[str] = None,
        worker_via_proxy: Optional[ThrottleProxy] = None,
    ) -> None:
        self.binary = binary
        self.workdir = os.path.abspath(workdir)
        os.makedirs(self.workdir, exist_ok=True)
        self.n_workers = int(workers)
        self.blob_store_mb = blob_store_mb
        self.blob_cache_mb = blob_cache_mb
        self.output_limit_mb = output_limit_mb
        self.state_dir = state_dir
        self.worker_via_proxy = worker_via_proxy

        self.blob_dir = os.path.join(self.workdir, "master-blobs")
        self.sandbox_dirs = [os.path.join(self.workdir, "sandbox-w%d" % i) for i in range(self.n_workers)]
        self.empty_config = os.path.join(self.workdir, "empty.toml")
        with open(self.empty_config, "w", encoding="utf-8") as f:
            f.write("")

        self.procs: List[Tuple[str, subprocess.Popen, Any]] = []
        self.pids: Dict[str, int] = {}
        self._extra_pids_lock = threading.Lock()
        self._extra_pids: Dict[str, int] = {}
        self.master_addr = ""
        self.master_host = "127.0.0.1"
        self.master_port = 0
        self.client_env = self._build_env(os.path.join(self.workdir, "home-client"))

    def _build_env(self, home: str) -> Dict[str, str]:
        os.makedirs(home, exist_ok=True)
        tmp_dir = os.path.join(home, "tmp")
        ox_home = os.path.join(home, "oxideswarm")
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(ox_home, exist_ok=True)
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("RUSTY_GRID_") and k not in _SAFE_ENV_STRIP_EXACT
        }
        env.update(
            HOME=home,
            USERPROFILE=home,
            OXIDESWARM_HOME=ox_home,
            TEMP=tmp_dir,
            TMP=tmp_dir,
            TMPDIR=tmp_dir,
            RUSTY_GRID_CONFIG=self.empty_config,
            RUSTY_GRID_NO_BATTERY="1",
        )
        return env

    def _spawn(self, name: str, args: List[str]) -> subprocess.Popen:
        log = open(os.path.join(self.workdir, name + ".log"), "wb")
        kw: Dict[str, Any] = {} if IS_WINDOWS else {"start_new_session": True}
        p = subprocess.Popen(
            [self.binary] + args,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=self._build_env(os.path.join(self.workdir, "home-" + name)),
            **kw,
        )
        self.procs.append((name, p, log))
        self.pids[name] = p.pid
        return p

    def active_pids(self) -> Dict[str, int]:
        out = {name: p.pid for name, p, _log in self.procs if p.poll() is None}
        with self._extra_pids_lock:
            out.update(self._extra_pids)
        return out

    def set_extra_pid(self, role: str, pid: Optional[int]) -> None:
        with self._extra_pids_lock:
            if pid is None:
                self._extra_pids.pop(role, None)
            else:
                self._extra_pids[role] = int(pid)

    def start(self) -> "EnduranceCluster":
        port_file = os.path.join(self.workdir, "master.port")
        dash_file = os.path.join(self.workdir, "dash.port")
        master_args = [
            "master",
            "--listen",
            "127.0.0.1:0",
            "--port-file",
            port_file,
            "--dashboard-port",
            "0",
            "--dashboard-port-file",
            dash_file,
            "--discovery-port",
            "0",
            "--blob-dir",
            self.blob_dir,
        ]
        if self.blob_store_mb is not None:
            master_args += ["--blob-store-mb", str(int(self.blob_store_mb))]
        if self.state_dir:
            master_args += ["--state-dir", self.state_dir]

        master = self._spawn("master", master_args)
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if master.poll() is not None:
                raise RuntimeError("master exited early (code %s)" % master.returncode)
            if os.path.exists(port_file) and os.path.getsize(port_file) > 0:
                with open(port_file, encoding="utf-8", errors="replace") as f:
                    txt = f.read().strip()
                if txt:
                    self.master_addr = txt if ":" in txt else "127.0.0.1:" + txt
                    host, pstr = self.master_addr.rsplit(":", 1)
                    self.master_host = host
                    self.master_port = int(pstr)
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError("master did not publish its port within 30 s")

        worker_target = self.master_addr
        if self.worker_via_proxy is not None:
            self.worker_via_proxy.target_host = self.master_host
            self.worker_via_proxy.target_port = self.master_port
            if not self.worker_via_proxy.bound_addr:
                self.worker_via_proxy.start()
            worker_target = self.worker_via_proxy.bound_addr

        for i in range(self.n_workers):
            os.makedirs(self.sandbox_dirs[i], exist_ok=True)
            wargs = [
                "worker",
                "--master",
                worker_target,
                "--name",
                "w%d" % i,
                "--no-portal",
                "--no-gpu",
                "--sandbox-base-dir",
                self.sandbox_dirs[i],
            ]
            if self.blob_cache_mb is not None:
                wargs += ["--blob-cache-mb", str(int(self.blob_cache_mb))]
            if self.output_limit_mb is not None:
                wargs += ["--output-limit-mb", str(int(self.output_limit_mb))]
            self._spawn("worker%d" % i, wargs)

        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            r = self.run_client(["workers", "--json"], timeout=15)
            if r.returncode == 0:
                try:
                    listed = json.loads(r.stdout)
                    if isinstance(listed, list) and len(listed) >= self.n_workers:
                        return self
                except ValueError:
                    pass
            for name, p, _log in self.procs:
                if p.poll() is not None:
                    raise RuntimeError("%s exited early (code %s)" % (name, p.returncode))
            time.sleep(0.1)
        raise RuntimeError("workers did not register within 60 s")

    def dir_map(self) -> Dict[str, str]:
        dirs = {"blob_store_bytes": self.blob_dir}
        if self.sandbox_dirs:
            dirs["sandbox_base_bytes"] = self.sandbox_dirs[0]
            dirs["worker_cache_bytes"] = os.path.join(self.sandbox_dirs[0], "cache", "blobs")
        if self.state_dir:
            dirs["state_dir_bytes"] = self.state_dir
        return dirs

    def run_client(
        self,
        args: List[str],
        timeout: float = 120.0,
        master_addr: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        addr = master_addr or self.master_addr
        cmd = [self.binary] + args[:1] + ["--master", addr] + args[1:]
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=self.client_env,
            timeout=timeout,
        )

    def spawn_submit(
        self,
        submit_args: List[str],
        master_addr: Optional[str] = None,
    ) -> subprocess.Popen:
        addr = master_addr or self.master_addr
        cmd = [self.binary, "submit", "--master", addr] + submit_args
        kw: Dict[str, Any] = {} if IS_WINDOWS else {"start_new_session": True}
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=self.client_env,
            **kw,
        )

    def run_submit_sampled(
        self,
        submit_args: List[str],
        timeout: float = 600.0,
        master_addr: Optional[str] = None,
    ) -> Tuple[int, str, str, float]:
        t0 = time.perf_counter()
        p = self.spawn_submit(submit_args, master_addr=master_addr)
        self.set_extra_pid("client", p.pid)
        try:
            stdout, stderr = p.communicate(timeout=timeout)
            dt = time.perf_counter() - t0
            return p.returncode, stdout or "", stderr or "", dt
        except subprocess.TimeoutExpired:
            p.kill()
            stdout, stderr = p.communicate()
            dt = time.perf_counter() - t0
            return 124, stdout or "", (stderr or "") + "\n[harness] submit timed out after %.1f s" % dt, dt
        finally:
            self.set_extra_pid("client", None)

    def read_log(self, role: str) -> str:
        path = os.path.join(self.workdir, role + ".log")
        if not os.path.isfile(path):
            return ""
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()

    def stop(self) -> None:
        for _name, p, _log in reversed(self.procs):
            if p.poll() is None:
                p.terminate()
        for _name, p, log in self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            try:
                log.close()
            except Exception:
                pass
        self.procs = []


def measure_tcp_rtt_ms(host: str, port: int, attempts: int = 5) -> Optional[float]:
    samples: List[float] = []
    for _ in range(attempts):
        t0 = time.perf_counter()
        try:
            with socket.create_connection((host, int(port)), timeout=3.0):
                samples.append((time.perf_counter() - t0) * 1000.0)
        except OSError:
            pass
    return round(statistics.median(samples), 3) if samples else None


# ---------------------------------------------------------------- scenarios (D2 T1..T9)


def _worker_python_args(worker_python: str, src: str, extra_args: List[str]) -> List[str]:
    return [
        "--command",
        worker_python,
        "--",
        "-c",
        _B64_EXEC,
        _b64(src),
    ] + extra_args


def _extract_outputs_info(parsed_json: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not parsed_json or not isinstance(parsed_json.get("outputs"), list):
        return []
    out = []
    for item in parsed_json["outputs"]:
        if isinstance(item, dict):
            out.append(
                {
                    "path": item.get("path"),
                    "id": item.get("id"),
                    "size": item.get("size"),
                }
            )
    return out


def _detect_reaped_lines(master_log: str) -> List[str]:
    hits = []
    for line in (master_log or "").splitlines():
        if "marked dead workers as Disconnected" in line or "Heartbeat timeout (reaper detected)" in line:
            hits.append(redact_text(line.strip()))
    return hits


def run_t1(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sizes_gib = [float(x.strip()) for x in args.t1_sizes_gib.split(",") if x.strip()]
    entries = []
    for idx, gib_val in enumerate(sizes_gib):
        size_bytes = int(round(gib_val * GIB))
        sc_dir = os.path.join(work_root, "t1-%d" % idx)
        os.makedirs(sc_dir, exist_ok=True)
        payload = os.path.join(sc_dir, "payload.bin")
        store_mb = int(max(4096, round(gib_val * 1024) + 2048)) if gib_val > 3.0 else None
        meta = generate_seeded_file(payload, size_bytes, args.seed + 100 + idx)

        c = EnduranceCluster(
            args.bin,
            os.path.join(sc_dir, "cluster"),
            workers=1,
            blob_store_mb=store_mb,
        )
        sampler: Optional[BackgroundSampler] = None
        try:
            c.start()
            sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
            before_objs = list_blob_objects(c.blob_dir)
            cmd_args = [
                "--input",
                payload + ":payload.bin",
                "--timeout-secs",
                "1800",
                "--wait",
                "--json",
            ] + _worker_python_args(
                args.worker_python,
                _WORKER_VERIFY_FILE_SRC,
                ["payload.bin", str(size_bytes), meta["sha256"]],
            )
            rc, stdout, stderr, wall_s = c.run_submit_sampled(cmd_args, timeout=2100.0)
            after_objs = list_blob_objects(c.blob_dir)
            cache_objs = list_blob_objects(os.path.join(c.sandbox_dirs[0], "cache", "blobs"))
            parsed_json = extract_json_payload(stdout)
            task_stdout = decode_bytes_field((parsed_json or {}).get("stdout"))
            parsed_inputs = parse_inputs_line(stderr)
            matching_master_ids = [
                bid for bid, bsz in after_objs.items() if bsz == size_bytes and bid not in before_objs
            ]
            matching_cache_ids = [bid for bid, bsz in cache_objs.items() if bsz == size_bytes]
            verified = rc == 0 and ("VERIFY_OK %d %s" % (size_bytes, meta["sha256"])) in task_stdout
            samples = sampler.stop()
            summary = BackgroundSampler.summarize(samples)
            entries.append(
                {
                    "size_gib": gib_val,
                    "size_bytes": size_bytes,
                    "blob_store_mb": store_mb or 4096,
                    "passed": bool(verified and matching_master_ids),
                    "wall_s": round(wall_s, 3),
                    "throughput_mib_per_s": round((size_bytes / MIB) / wall_s, 3) if wall_s > 0 else None,
                    "sha256_expected": meta["sha256"],
                    "worker_verified": verified,
                    "master_blake3_ids": matching_master_ids,
                    "worker_cache_blake3_ids": matching_cache_ids,
                    "input_upload": (parsed_json or {}).get("input_upload") or parsed_inputs,
                    "error": redact_text(
                        str((parsed_json or {}).get("error") or ("" if rc == 0 else stderr[-400:])),
                        [sc_dir, work_root],
                    ),
                    "resource_summary": summary,
                    "samples": samples,
                }
            )
        finally:
            if sampler is not None and not sampler._stop.is_set():
                sampler.stop()
            c.stop()
            shutil.rmtree(sc_dir, ignore_errors=True)

    mem_comparisons = {}
    if len(entries) >= 2:
        small, large = entries[0], entries[-1]
        for role in ("master", "worker0", "client"):
            p_small = small["resource_summary"]["processes"].get(role, {}).get("peak_rss_bytes")
            p_large = large["resource_summary"]["processes"].get(role, {}).get("peak_rss_bytes")
            mem_comparisons[role] = check_t1_memory_flat(p_small, p_large)

    all_verified = all(e["passed"] for e in entries)
    mem_ok = all(v["passed"] for v in mem_comparisons.values()) if mem_comparisons else True
    return {
        "id": "T1",
        "name": "Large single-file transfer and memory flatness",
        "passed": bool(all_verified and mem_ok),
        "runs": entries,
        "memory_flatness_128mib": mem_comparisons,
    }


def run_t2(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t2")
    os.makedirs(sc_dir, exist_ok=True)
    tree_dir = os.path.join(sc_dir, "tree")
    tgen0 = time.perf_counter()
    tree_meta = generate_seeded_tree(tree_dir, int(args.t2_files), args.seed + 200)
    gen_s = time.perf_counter() - tgen0

    c = EnduranceCluster(args.bin, os.path.join(sc_dir, "cluster"), workers=1)
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
        cmd_args = [
            "--input",
            tree_dir + ":tree",
            "--timeout-secs",
            "1800",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_VERIFY_TREE_SRC,
            ["tree", str(tree_meta["files"]), str(tree_meta["total_bytes"]), tree_meta["tree_sha256"]],
        )
        rc, stdout, stderr, wall_s = c.run_submit_sampled(cmd_args, timeout=2100.0)
        parsed_json = extract_json_payload(stdout)
        task_stdout = decode_bytes_field((parsed_json or {}).get("stdout"))
        parsed_inputs = parse_inputs_line(stderr)
        verified = (
            rc == 0
            and ("VERIFY_TREE_OK %d %d %s" % (tree_meta["files"], tree_meta["total_bytes"], tree_meta["tree_sha256"]))
            in task_stdout
        )
        master_blobs = len(list_blob_objects(c.blob_dir))
        samples = sampler.stop()
        summary = BackgroundSampler.summarize(samples)
        return {
            "id": "T2",
            "name": "Many-file tree transfer",
            "passed": bool(verified),
            "files": tree_meta["files"],
            "total_bytes": tree_meta["total_bytes"],
            "tree_sha256": tree_meta["tree_sha256"],
            "tree_generate_s": round(gen_s, 3),
            "wall_s": round(wall_s, 3),
            "master_blobs_stored": master_blobs,
            "worker_verified": verified,
            "input_upload": (parsed_json or {}).get("input_upload") or parsed_inputs,
            "error": redact_text(
                str((parsed_json or {}).get("error") or ("" if rc == 0 else stderr[-400:])),
                [sc_dir, work_root],
            ),
            "resource_summary": summary,
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t3(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t3")
    os.makedirs(sc_dir, exist_ok=True)
    pass_bytes = int(args.t3_pass_mib) * MIB
    fail_bytes = int(args.t3_fail_mib) * MIB

    # Compute expected sha256 deterministically without keeping a local copy first
    def _expected_sha(sz: int, seed: int) -> str:
        rng = random.Random(seed)
        h = hashlib.sha256()
        rem = sz
        while rem > 0:
            n = min(4 * MIB, rem)
            h.update(rng.randbytes(n) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(n)))
            rem -= n
        return h.hexdigest()

    pass_seed = args.seed + 301
    pass_sha = _expected_sha(pass_bytes, pass_seed)
    c = EnduranceCluster(args.bin, os.path.join(sc_dir, "cluster"), workers=1)
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()

        out_dir_ok = os.path.join(sc_dir, "out_ok")
        os.makedirs(out_dir_ok, exist_ok=True)
        cmd_ok = [
            "--output",
            "out_ok.bin",
            "--output-dir",
            out_dir_ok,
            "--timeout-secs",
            "1200",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_GEN_OUTPUT_SRC,
            ["out_ok.bin", str(pass_bytes), str(pass_seed), "-"],
        )
        rc_ok, stdout_ok, stderr_ok, wall_ok = c.run_submit_sampled(cmd_ok, timeout=1500.0)
        json_ok = extract_json_payload(stdout_ok)
        outputs_ok = _extract_outputs_info(json_ok)
        dl_path = os.path.join(out_dir_ok, "out_ok.bin")
        dl_match = False
        if os.path.isfile(dl_path):
            dl_sz, dl_sha = sha256_file(dl_path)
            dl_match = dl_sz == pass_bytes and dl_sha == pass_sha
        step_ok_passed = (
            rc_ok == 0
            and dl_match
            and bool(outputs_ok)
            and HEX64_RE.match(str(outputs_ok[0].get("id") or "")) is not None
        )
        shutil.rmtree(out_dir_ok, ignore_errors=True)

        # 3b: over default 1024 MiB limit -> must fail mentioning --output-limit-mb
        out_dir_fail = os.path.join(sc_dir, "out_fail")
        os.makedirs(out_dir_fail, exist_ok=True)
        cmd_fail = [
            "--output",
            "out_fail.bin",
            "--output-dir",
            out_dir_fail,
            "--timeout-secs",
            "1200",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_GEN_OUTPUT_SRC,
            ["out_fail.bin", str(fail_bytes), str(args.seed + 302), "-"],
        )
        rc_fail, stdout_fail, stderr_fail, wall_fail = c.run_submit_sampled(cmd_fail, timeout=1500.0)
        json_fail = extract_json_payload(stdout_fail)
        err_text = str((json_fail or {}).get("error") or "") + "\n" + stderr_fail + "\n" + stdout_fail
        step_fail_passed = (rc_fail != 0 or bool((json_fail or {}).get("error"))) and ("--output-limit-mb" in err_text)
        tmp_left = list_tmp_entries(c.blob_dir)
        samples = sampler.stop()
        return {
            "id": "T3",
            "name": "Output limit pass (1000 MiB) and refusal (1100 MiB)",
            "passed": bool(step_ok_passed and step_fail_passed),
            "within_limit": {
                "size_mib": args.t3_pass_mib,
                "size_bytes": pass_bytes,
                "passed": bool(step_ok_passed),
                "rc": rc_ok,
                "wall_s": round(wall_ok, 3),
                "download_verified": dl_match,
                "outputs": outputs_ok,
                "error": redact_text(str((json_ok or {}).get("error") or ("" if rc_ok == 0 else stderr_ok[-400:])), [sc_dir]),
            },
            "over_limit": {
                "size_mib": args.t3_fail_mib,
                "size_bytes": fail_bytes,
                "passed": bool(step_fail_passed),
                "rc": rc_fail,
                "wall_s": round(wall_fail, 3),
                "mentions_output_limit_mb": "--output-limit-mb" in err_text,
                "error": redact_text(err_text.strip()[:500], [sc_dir, work_root]),
            },
            "master_tmp_leftovers": tmp_left,
            "resource_summary": BackgroundSampler.summarize(samples),
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t4(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t4")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(round(float(args.t4_size_gib) * GIB))
    payload = os.path.join(sc_dir, "payload.bin")
    meta = generate_seeded_file(payload, size_bytes, args.seed + 400)

    c = EnduranceCluster(args.bin, os.path.join(sc_dir, "cluster"), workers=1)
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
        cmd_args = [
            "--input",
            payload + ":payload.bin",
            "--timeout-secs",
            "1800",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_VERIFY_FILE_SRC,
            ["payload.bin", str(size_bytes), meta["sha256"]],
        )
        b0 = dir_bytes(c.blob_dir)
        rc1, out1, err1, w1 = c.run_submit_sampled(cmd_args, timeout=2100.0)
        b1 = dir_bytes(c.blob_dir)
        j1 = extract_json_payload(out1) or {}
        u1 = j1.get("input_upload") or parse_inputs_line(err1) or {}

        rc2, out2, err2, w2 = c.run_submit_sampled(cmd_args, timeout=1200.0)
        b2 = dir_bytes(c.blob_dir)
        j2 = extract_json_payload(out2) or {}
        u2 = j2.get("input_upload") or parse_inputs_line(err2) or {}

        zero_uploaded = (
            u2.get("uploaded") == 0
            or u2.get("blobs_sent") == 0
        ) and u2.get("bytes", 0) == 0 and (b2 - b1) == 0
        ok1 = rc1 == 0 and "VERIFY_OK" in decode_bytes_field(j1.get("stdout"))
        ok2 = rc2 == 0 and "VERIFY_OK" in decode_bytes_field(j2.get("stdout"))
        samples = sampler.stop()
        return {
            "id": "T4",
            "name": "Repeated 3 GiB submission uploads zero blob bytes",
            "passed": bool(ok1 and ok2 and zero_uploaded),
            "size_gib": float(args.t4_size_gib),
            "size_bytes": size_bytes,
            "first": {
                "rc": rc1,
                "wall_s": round(w1, 3),
                "verified": ok1,
                "master_blob_store_growth_bytes": b1 - b0,
                "input_upload": u1,
            },
            "second": {
                "rc": rc2,
                "wall_s": round(w2, 3),
                "verified": ok2,
                "master_blob_store_growth_bytes": b2 - b1,
                "input_upload": u2,
            },
            "resource_summary": BackgroundSampler.summarize(samples),
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t5(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    """Client upload cut at roughly half via ThrottleProxy, then resubmitted."""
    sc_dir = os.path.join(work_root, "t5")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(args.t5_size_mib) * MIB
    payload = os.path.join(sc_dir, "payload.bin")
    meta = generate_seeded_file(payload, size_bytes, args.seed + 500)

    c = EnduranceCluster(args.bin, os.path.join(sc_dir, "cluster"), workers=1)
    proxy: Optional[ThrottleProxy] = None
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
        proxy = ThrottleProxy(c.master_host, c.master_port, rate_mbps=args.t5_rate_mbps).start()
        cmd_args = [
            "--input",
            payload + ":payload.bin",
            "--timeout-secs",
            "600",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_VERIFY_FILE_SRC,
            ["payload.bin", str(size_bytes), meta["sha256"]],
        )
        proc = c.spawn_submit(cmd_args, master_addr=proxy.bound_addr)
        c.set_extra_pid("client", proc.pid)
        cut_at_bytes = 0
        cut_done = False
        half = size_bytes // 2
        deadline = time.monotonic() + 180.0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            if proxy.bytes_c2s >= half:
                cut_at_bytes = proxy.bytes_c2s
                proxy.cut_links(pause_s=1.0)
                cut_done = True
                break
            time.sleep(0.05)
        out1, err1 = proc.communicate(timeout=30.0)
        rc1 = proc.returncode
        c.set_extra_pid("client", None)
        proxy.set_rate(None)
        # Re-submit against direct master address on clean connection
        rc2, out2, err2, w2 = c.run_submit_sampled(cmd_args, timeout=600.0)
        j2 = extract_json_payload(out2) or {}
        u2 = j2.get("input_upload") or parse_inputs_line(err2) or {}
        ok2 = rc2 == 0 and "VERIFY_OK" in decode_bytes_field(j2.get("stdout"))
        samples = sampler.stop()
        return {
            "id": "T5",
            "name": "Client upload cut at half then resubmitted",
            "passed": bool(cut_done and ok2),
            "size_bytes": size_bytes,
            "cut_done": cut_done,
            "bytes_c2s_before_cut": cut_at_bytes,
            "first_attempt_rc": rc1,
            "first_attempt_stderr": redact_text(err1[-300:], [sc_dir]),
            "resubmit_rc": rc2,
            "resubmit_wall_s": round(w2, 3),
            "bytes_sent_on_resubmit": u2.get("bytes"),
            "input_upload": u2,
            "master_tmp_leftovers": list_tmp_entries(c.blob_dir),
            "resource_summary": BackgroundSampler.summarize(samples),
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        if proxy is not None:
            proxy.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t6(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t6")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(args.t6_size_mib) * MIB
    payload = os.path.join(sc_dir, "payload.bin")
    meta = generate_seeded_file(payload, size_bytes, args.seed + 600)

    proxy = ThrottleProxy("127.0.0.1", 0, rate_mbps=args.t6_rate_mbps, delay_ms=0.0)
    c = EnduranceCluster(
        args.bin,
        os.path.join(sc_dir, "cluster"),
        workers=1,
        worker_via_proxy=proxy,
    )
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        cache_dir = os.path.join(c.sandbox_dirs[0], "cache", "blobs")
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
        cmd_args = [
            "--input",
            payload + ":payload.bin",
            "--timeout-secs",
            "600",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_VERIFY_FILE_SRC,
            ["payload.bin", str(size_bytes), meta["sha256"]],
        )
        proc = c.spawn_submit(cmd_args)
        c.set_extra_pid("client", proc.pid)
        t0 = time.perf_counter()
        cut_triggered = False
        cut_reason = ""
        bytes_s2c_at_cut = 0
        baseline_s2c = proxy.bytes_s2c

        deadline = time.monotonic() + 180.0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            tmp_files = list_tmp_entries(cache_dir)
            delta_s2c = proxy.bytes_s2c - baseline_s2c
            if tmp_files or delta_s2c >= 4 * MIB:
                time.sleep(0.15)
                bytes_s2c_at_cut = proxy.bytes_s2c - baseline_s2c
                cut_reason = "tmp_file=%s delta_s2c=%d" % (bool(tmp_files), delta_s2c)
                proxy.cut_links(pause_s=3.0)
                proxy.set_rate(None)
                cut_triggered = True
                break
            time.sleep(0.05)

        try:
            stdout, stderr = proc.communicate(timeout=300.0)
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            rc = 124
        finally:
            c.set_extra_pid("client", None)
        wall_s = time.perf_counter() - t0
        # Give worker a brief moment to settle any cleanup before inspecting cache tmp
        time.sleep(0.5)
        tmp_left = list_tmp_entries(cache_dir)
        parsed_json = extract_json_payload(stdout) or {}
        verified = rc == 0 and "VERIFY_OK" in decode_bytes_field(parsed_json.get("stdout"))
        master_log = c.read_log("master")
        disconnect_lines = [
            redact_text(ln.strip(), [sc_dir])
            for ln in master_log.splitlines()
            if "disconnect" in ln.lower() or "requeue" in ln.lower() or "reap" in ln.lower()
        ]
        samples = sampler.stop()
        return {
            "id": "T6",
            "name": "Worker link cut during input fetch, recovery, clean cache tmp",
            "passed": bool(cut_triggered and verified and not tmp_left),
            "size_mib": int(args.t6_size_mib),
            "rate_mbps_before_cut": float(args.t6_rate_mbps),
            "cut_triggered": cut_triggered,
            "cut_reason": cut_reason,
            "bytes_s2c_at_cut": bytes_s2c_at_cut,
            "rc": rc,
            "wall_s": round(wall_s, 3),
            "worker_verified": verified,
            "worker_cache_tmp_leftovers": tmp_left,
            "master_disconnect_log_hits": len(disconnect_lines),
            "master_disconnect_log_sample": disconnect_lines[:10],
            "error": redact_text(
                str(parsed_json.get("error") or ("" if rc == 0 else stderr[-400:])),
                [sc_dir, work_root],
            ),
            "resource_summary": BackgroundSampler.summarize(samples),
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        proxy.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t7(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t7")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(args.t7_size_mib) * MIB
    in_file = os.path.join(sc_dir, "payload.bin")
    in_meta = generate_seeded_file(in_file, size_bytes, args.seed + 700)

    out_seed = args.seed + 701
    rng = random.Random(out_seed)
    h_out = hashlib.sha256()
    rem = size_bytes
    while rem > 0:
        n = min(4 * MIB, rem)
        h_out.update(rng.randbytes(n) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(n)))
        rem -= n
    expected_out_sha = h_out.hexdigest()

    proxy = ThrottleProxy("127.0.0.1", 0, rate_mbps=args.t7_rate_mbps, delay_ms=args.t7_delay_ms)
    c = EnduranceCluster(
        args.bin,
        os.path.join(sc_dir, "cluster"),
        workers=1,
        worker_via_proxy=proxy,
    )
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sample_interval = max(float(args.sample_interval), 5.0)
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=sample_interval).start()
        out_dir = os.path.join(sc_dir, "out")
        os.makedirs(out_dir, exist_ok=True)
        cmd_args = [
            "--input",
            in_file + ":payload.bin",
            "--output",
            "out_256m.bin",
            "--output-dir",
            out_dir,
            "--timeout-secs",
            "3600",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_GEN_OUTPUT_SRC,
            [
                "out_256m.bin",
                str(size_bytes),
                str(out_seed),
                "payload.bin|%d|%s" % (size_bytes, in_meta["sha256"]),
            ],
        )
        rc, stdout, stderr, wall_s = c.run_submit_sampled(cmd_args, timeout=4200.0)
        parsed_json = extract_json_payload(stdout) or {}
        task_stdout = decode_bytes_field(parsed_json.get("stdout"))
        outputs = _extract_outputs_info(parsed_json)
        dl_path = os.path.join(out_dir, "out_256m.bin")
        dl_match = False
        if os.path.isfile(dl_path):
            dl_sz, dl_sha = sha256_file(dl_path)
            dl_match = dl_sz == size_bytes and dl_sha == expected_out_sha

        reaped_lines = _detect_reaped_lines(c.read_log("master"))
        worker_reaped = bool(reaped_lines)
        input_ok = ("VERIFY_OK %d %s" % (size_bytes, in_meta["sha256"])) in task_stdout
        output_ok = (
            dl_match
            and bool(outputs)
            and HEX64_RE.match(str(outputs[0].get("id") or "")) is not None
        )
        passed = rc == 0 and input_ok and output_ok and not worker_reaped
        samples = sampler.stop()
        return {
            "id": "T7",
            "name": "Throttled 2 Mbit/s 50 ms RTT TCP link (256 MiB in + 256 MiB out)",
            "passed": bool(passed),
            "size_mib": int(args.t7_size_mib),
            "rate_mbps": float(args.t7_rate_mbps),
            "delay_ms": float(args.t7_delay_ms),
            "theoretical_single_blob_bound_mib_per_s": theoretical_single_blob_bound_mib_per_s(float(args.t7_delay_ms)),
            "effective_throttle_bound_mib_per_s": round(float(args.t7_rate_mbps) / 8.0, 3),
            "rc": rc,
            "wall_s": round(wall_s, 3),
            "input_verified": input_ok,
            "output_verified": output_ok,
            "worker_reaped": worker_reaped,
            "reap_log_lines": reaped_lines[:10],
            "proxy_bytes_c2s": proxy.bytes_c2s,
            "proxy_bytes_s2c": proxy.bytes_s2c,
            "outputs": outputs,
            "error": redact_text(
                str(parsed_json.get("error") or ("" if rc == 0 else stderr[-600:])),
                [sc_dir, work_root],
            ),
            "resource_summary": BackgroundSampler.summarize(samples),
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        proxy.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t8(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t8")
    os.makedirs(sc_dir, exist_ok=True)
    store_mb = int(args.t8_store_mb)
    too_big_bytes = (store_mb + 8) * MIB
    half_plus_bytes = int(store_mb * 0.625) * MIB  # 2 * 0.625 = 1.25x > 1.0x store

    f_big = os.path.join(sc_dir, "too_big.bin")
    f_a = os.path.join(sc_dir, "part_a.bin")
    f_b = os.path.join(sc_dir, "part_b.bin")
    generate_seeded_file(f_big, too_big_bytes, args.seed + 801)
    generate_seeded_file(f_a, half_plus_bytes, args.seed + 802)
    generate_seeded_file(f_b, half_plus_bytes, args.seed + 803)

    c = EnduranceCluster(args.bin, os.path.join(sc_dir, "cluster"), workers=1, blob_store_mb=store_mb)
    proxy: Optional[ThrottleProxy] = None
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()

        # 8a: single input larger than whole store
        rc_a, out_a, err_a, w_a = c.run_submit_sampled(
            ["--input", f_big + ":payload.bin", "--timeout-secs", "120", "--wait"]
            + ["--command", args.worker_python, "--", "-c", "print('ok')"],
            timeout=180.0,
        )
        msg_a = (err_a + "\n" + out_a).strip()
        tmp_after_a = list_tmp_entries(c.blob_dir)
        passed_a = rc_a != 0 and "--blob-store-mb" in msg_a and not tmp_after_a

        # 8b: two concurrent uploads that together exceed store_mb
        proxy = ThrottleProxy(c.master_host, c.master_port, rate_mbps=4.0, delay_ms=0.0).start()
        proc1 = c.spawn_submit(
            ["--input", f_a + ":a.bin", "--timeout-secs", "300", "--wait"]
            + ["--command", args.worker_python, "--", "-c", "print('a')"],
            master_addr=proxy.bound_addr,
        )
        c.set_extra_pid("client", proc1.pid)
        saw_tmp = False
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if proc1.poll() is not None:
                break
            if list_tmp_entries(c.blob_dir):
                saw_tmp = True
                break
            time.sleep(0.025)

        rc_b = 0
        out_b = ""
        err_b = ""
        if saw_tmp:
            r_b = c.run_client(
                ["submit", "--input", f_b + ":b.bin", "--timeout-secs", "120", "--wait"]
                + ["--command", args.worker_python, "--", "-c", "print('b')"],
                timeout=120.0,
            )
            rc_b, out_b, err_b = r_b.returncode, r_b.stdout or "", r_b.stderr or ""
        proxy.set_rate(None)
        try:
            proc1.communicate(timeout=120.0)
            rc_p1 = proc1.returncode
        except subprocess.TimeoutExpired:
            proc1.kill()
            proc1.communicate()
            rc_p1 = 124
        finally:
            c.set_extra_pid("client", None)

        time.sleep(0.3)
        tmp_after_b = list_tmp_entries(c.blob_dir)
        msg_b = (err_b + "\n" + out_b).strip()
        passed_b = saw_tmp and rc_b != 0 and "--blob-store-mb" in msg_b and not tmp_after_b
        samples = sampler.stop()
        return {
            "id": "T8",
            "name": "Oversize single blob and combined concurrent upload refusal",
            "passed": bool(passed_a and passed_b),
            "blob_store_mb": store_mb,
            "single_over_limit": {
                "size_bytes": too_big_bytes,
                "passed": bool(passed_a),
                "rc": rc_a,
                "wall_s": round(w_a, 3),
                "mentions_blob_store_mb": "--blob-store-mb" in msg_a,
                "tmp_leftovers": tmp_after_a,
                "error": redact_text(msg_a[:400], [sc_dir, work_root]),
            },
            "concurrent_over_limit": {
                "each_size_bytes": half_plus_bytes,
                "passed": bool(passed_b),
                "saw_first_upload_in_tmp": saw_tmp,
                "first_upload_rc": rc_p1,
                "second_upload_rc": rc_b,
                "mentions_blob_store_mb": "--blob-store-mb" in msg_b,
                "tmp_leftovers": tmp_after_b,
                "error": redact_text(msg_b[:400], [sc_dir, work_root]),
            },
            "resource_summary": BackgroundSampler.summarize(samples),
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        if proxy is not None:
            proxy.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


def run_t9(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    """3 GiB input (> 2 GiB default worker cache) used by 3 tasks in a row."""
    sc_dir = os.path.join(work_root, "t9")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(round(float(args.t9_size_gib) * GIB))
    payload = os.path.join(sc_dir, "payload.bin")
    meta = generate_seeded_file(payload, size_bytes, args.seed + 900)

    c = EnduranceCluster(args.bin, os.path.join(sc_dir, "cluster"), workers=1)
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
        runs = []
        cmd_args = [
            "--input",
            payload + ":payload.bin",
            "--timeout-secs",
            "1800",
            "--wait",
            "--json",
        ] + _worker_python_args(
            args.worker_python,
            _WORKER_VERIFY_FILE_SRC,
            ["payload.bin", str(size_bytes), meta["sha256"]],
        )
        for idx in range(int(args.t9_runs)):
            rc, stdout, stderr, wall_s = c.run_submit_sampled(cmd_args, timeout=2100.0)
            pj = extract_json_payload(stdout) or {}
            ok = rc == 0 and "VERIFY_OK" in decode_bytes_field(pj.get("stdout"))
            cache_bytes = dir_bytes(os.path.join(c.sandbox_dirs[0], "cache", "blobs"))
            runs.append(
                {
                    "run": idx + 1,
                    "rc": rc,
                    "wall_s": round(wall_s, 3),
                    "verified": ok,
                    "worker_cache_bytes_after": cache_bytes,
                    "input_upload": pj.get("input_upload") or parse_inputs_line(stderr),
                }
            )
        samples = sampler.stop()
        summary = BackgroundSampler.summarize(samples)
        peak_worker_disk = summary["dirs"].get("sandbox_base_bytes", {}).get("peak_bytes", 0)
        return {
            "id": "T9",
            "name": "3 GiB input larger than worker cache across 3 sequential tasks",
            "passed": all(r["verified"] for r in runs),
            "size_gib": float(args.t9_size_gib),
            "size_bytes": size_bytes,
            "worker_peak_disk_bytes": peak_worker_disk,
            "runs": runs,
            "resource_summary": summary,
            "samples": samples,
        }
    finally:
        if sampler is not None and not sampler._stop.is_set():
            sampler.stop()
        c.stop()
        shutil.rmtree(sc_dir, ignore_errors=True)


SCENARIO_RUNNERS = {
    "T1": run_t1,
    "T2": run_t2,
    "T3": run_t3,
    "T4": run_t4,
    "T5": run_t5,
    "T6": run_t6,
    "T7": run_t7,
    "T8": run_t8,
    "T9": run_t9,
}


def compute_largest_file_bytes(scenarios: List[str], args: argparse.Namespace) -> int:
    candidates = [0]
    if "T1" in scenarios:
        for tok in args.t1_sizes_gib.split(","):
            if tok.strip():
                candidates.append(int(round(float(tok.strip()) * GIB)))
    if "T2" in scenarios:
        candidates.append(16 * KIB)
    if "T3" in scenarios:
        candidates.append(max(int(args.t3_pass_mib), int(args.t3_fail_mib)) * MIB)
    if "T4" in scenarios:
        candidates.append(int(round(float(args.t4_size_gib) * GIB)))
    if "T5" in scenarios:
        candidates.append(int(args.t5_size_mib) * MIB)
    if "T6" in scenarios:
        candidates.append(int(args.t6_size_mib) * MIB)
    if "T7" in scenarios:
        candidates.append(int(args.t7_size_mib) * MIB)
    if "T8" in scenarios:
        candidates.append((int(args.t8_store_mb) + 8) * MIB)
    if "T9" in scenarios:
        candidates.append(int(round(float(args.t9_size_gib) * GIB)))
    return max(candidates)


# ---------------------------------------------------------------- CLI subcommands


def validate_transfer_args(args: argparse.Namespace) -> None:
    """Reject unimplemented multi-machine links and remote flags in step 1 (TEST-24)."""
    link = (getattr(args, "link", None) or "").strip().lower()
    if link != "loopback":
        raise ValueError(
            "only --link loopback is supported in TEST-24 step 1 (got --link %r; two-machine links arrive in step 2)"
            % getattr(args, "link", None)
        )
    bad_flags: List[str] = []
    if getattr(args, "master", None):
        bad_flags.append("--master")
    if getattr(args, "secret_file", None):
        bad_flags.append("--secret-file")
    if getattr(args, "insecure", False):
        bad_flags.append("--insecure")
    if bad_flags:
        raise ValueError(
            "%s require two-machine mode (TEST-24 step 2) and cannot be combined with loopback step 1"
            % ", ".join(bad_flags)
        )


def cmd_transfer(args: argparse.Namespace) -> int:
    try:
        validate_transfer_args(args)
    except ValueError as exc:
        raise SystemExit(str(exc))

    binary = os.path.abspath(args.bin)
    if not os.path.isfile(binary):
        raise SystemExit("binary not found: %s" % binary)

    raw_list = [s.strip().upper() for s in args.scenarios.split(",") if s.strip()]
    if raw_list == ["ALL"]:
        scenarios = list(ALL_SCENARIOS)
    else:
        bad = [s for s in raw_list if s not in SCENARIO_RUNNERS]
        if bad:
            raise SystemExit("unknown scenario id(s): %s" % ", ".join(bad))
        scenarios = raw_list

    largest_bytes = compute_largest_file_bytes(scenarios, args)
    if args.work_dir:
        os.makedirs(args.work_dir, exist_ok=True)
        work_root = tempfile.mkdtemp(prefix="oxendurance-", dir=os.path.abspath(args.work_dir))
    else:
        work_root = tempfile.mkdtemp(prefix="oxendurance-")

    disk_check = check_free_space(work_root, largest_bytes)
    started = datetime.datetime.now(datetime.timezone.utc)
    hw = hardware()
    os_facts = os_info()
    git = git_info()
    version = sh([binary, "--version"])
    av_realtime = windows_realtime_av_enabled()

    # Probe loopback TCP connect latency before heavy runs
    loopback_rtt_ms: Optional[float] = args.rtt_ms
    if loopback_rtt_ms is None and args.link == "loopback":
        probe_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe_srv.bind(("127.0.0.1", 0))
        probe_srv.listen(4)
        probe_port = probe_srv.getsockname()[1]
        stop_probe = threading.Event()

        def _accept_probe() -> None:
            probe_srv.settimeout(0.1)
            while not stop_probe.is_set():
                try:
                    conn, _ = probe_srv.accept()
                    conn.close()
                except socket.timeout:
                    continue
                except OSError:
                    break

        pt = threading.Thread(target=_accept_probe, daemon=True)
        pt.start()
        loopback_rtt_ms = measure_tcp_rtt_ms("127.0.0.1", probe_port)
        stop_probe.set()
        pt.join(timeout=2.0)
        probe_srv.close()

    results: List[Dict[str, Any]] = []
    try:
        for sc in scenarios:
            print("[%s] starting..." % sc, flush=True)
            res = SCENARIO_RUNNERS[sc](args, work_root)
            results.append(res)
            status = "PASS" if res["passed"] else "FAIL"
            print("[%s] %s" % (sc, status), flush=True)
    finally:
        if not args.keep_workdir:
            shutil.rmtree(work_root, ignore_errors=True)

    omitted_scenarios = [s for s in ALL_SCENARIOS if s not in scenarios]
    omitted_links = [lk for lk in ("loopback", "lan-tcp", "lan-quic", "quic-relay") if lk != args.link]

    record = {
        "benchmark": "TEST-24 large-transfer endurance suite",
        "date_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": git,
        "binary": {"version": version, "build_profile": build_profile(binary)},
        "hardware": hw,
        "os": os_facts,
        "windows_realtime_av_enabled": av_realtime,
        "power": power_state(),
        "python": platform.python_version(),
        "link": args.link,
        "parameters": {
            "seed": int(args.seed),
            "sample_interval_s": float(args.sample_interval),
        },
        "measured_rtt_ms": loopback_rtt_ms,
        "theoretical_single_blob_bound_mib_per_s": theoretical_single_blob_bound_mib_per_s(loopback_rtt_ms),
        "disk_check": disk_check,
        "verification_note": (
            "Client streams seeded random bytes while computing SHA-256 and the worker script re-reads the sandbox payload "
            "and checks SHA-256 independently of the code under test without keeping a second copy; oxideswarm checks BLAKE3 in "
            "Master BlobStore and Worker BlobCache, and master_blake3_ids / worker_cache_blake3_ids record matching-size store "
            "object names on disk without recomputing BLAKE3 in Python."
        ),
        "scenarios_requested": scenarios,
        "omitted_scenarios": omitted_scenarios,
        "omitted_links": omitted_links,
        "note": redact_text(args.label, [work_root]),
        "all_passed": all(r["passed"] for r in results),
        "scenarios": results,
    }

    quarantined = False
    if args.no_write:
        text = json.dumps(record, indent=2) + "\n"
        assert_record_private(text, forbidden=[work_root])
        print(text)
    else:
        out_path = args.out or result_path(os.path.join(HERE, "results"), started, args.link, os_facts, hw)
        write_res = write_record(out_path, record, forbidden=[work_root])
        quarantined = bool(write_res["quarantined"])
        if quarantined:
            print("quarantined", write_res["path"], flush=True)
        else:
            print("wrote", write_res["path"], flush=True)

    for r in results:
        print("  %-4s %-5s %s" % (r["id"], "PASS" if r["passed"] else "FAIL", r["name"]), flush=True)
    if quarantined:
        return 2
    return 0 if record["all_passed"] else 1


def cmd_throttle(args: argparse.Namespace) -> int:
    if ":" not in args.target:
        raise SystemExit("--target must be HOST:PORT")
    thost, tport = args.target.rsplit(":", 1)
    lhost, lport = args.listen.rsplit(":", 1) if ":" in args.listen else (args.listen, "0")
    proxy = ThrottleProxy(
        thost,
        int(tport),
        listen_host=lhost,
        listen_port=int(lport),
        rate_mbps=args.rate_mbps,
        delay_ms=args.delay_ms,
    ).start()
    if args.port_file:
        os.makedirs(os.path.dirname(os.path.abspath(args.port_file)), exist_ok=True)
        with open(args.port_file, "w", encoding="utf-8") as f:
            f.write(str(proxy.port) + "\n")
    print("listening=%s target=%s rate_mbps=%s delay_ms=%s" % (
        proxy.bound_addr, args.target, args.rate_mbps, args.delay_ms
    ), flush=True)
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        proxy.stop()
    return 0


def cmd_sample(args: argparse.Namespace) -> int:
    pid_map: Dict[str, int] = {}
    for spec in args.pid:
        if "=" in spec:
            role, pid_str = spec.split("=", 1)
        else:
            role, pid_str = "pid_" + spec, spec
        pid_map[role.strip()] = int(pid_str.strip())
    dir_map: Dict[str, str] = {}
    if args.blob_dir:
        dir_map["blob_store_bytes"] = args.blob_dir
    if args.cache_dir:
        dir_map["worker_cache_bytes"] = args.cache_dir
    if args.sandbox_dir:
        dir_map["sandbox_base_bytes"] = args.sandbox_dir
    if args.state_dir:
        dir_map["state_dir_bytes"] = args.state_dir

    rows: List[Dict[str, Any]] = []
    t0 = time.monotonic()
    for idx in range(max(1, int(args.count))):
        if idx > 0:
            time.sleep(max(0.1, float(args.interval)))
        rows.append(
            {
                "date_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "t_s": round(time.monotonic() - t0, 2),
                "windows_realtime_av_enabled": windows_realtime_av_enabled(),
                "processes": sample_processes(pid_map),
                "dirs": sample_dirs(dir_map),
            }
        )
    payload = {
        "samples": rows,
        "summary": BackgroundSampler.summarize(rows),
    }
    text = json.dumps(payload, indent=2) + "\n"
    if args.out:
        assert_record_private(text)
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        print(text, end="")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="subcommand", required=True)

    default_bin = os.path.join(REPO, "target", "release", "oxideswarm" + (".exe" if IS_WINDOWS else ""))
    tr = sub.add_parser("transfer", help="Run large-transfer endurance checks (T1..T9)")
    tr.add_argument("--bin", default=default_bin, help="Path to built oxideswarm binary")
    tr.add_argument("--worker-python", default=sys.executable, help="Python executable path on worker")
    tr.add_argument(
        "--scenarios",
        default=",".join(DEFAULT_LOOPBACK_SCENARIOS),
        help="Comma-separated scenarios (e.g. T1,T2,T3,T4,T6,T7,T8 or all)",
    )
    tr.add_argument("--t1-sizes-gib", default="1,3", help="Comma-separated T1 file sizes in GiB (default 1,3)")
    tr.add_argument("--t2-files", type=int, default=20000, help="Files in T2 input tree (default 20000)")
    tr.add_argument("--t3-pass-mib", type=int, default=1000, help="T3 under-limit output size in MiB")
    tr.add_argument("--t3-fail-mib", type=int, default=1100, help="T3 over-limit output size in MiB")
    tr.add_argument("--t4-size-gib", type=float, default=3.0, help="T4 repeated file size in GiB")
    tr.add_argument("--t5-size-mib", type=int, default=64, help="T5 half-cut upload size in MiB")
    tr.add_argument("--t5-rate-mbps", type=float, default=8.0, help="T5 proxy rate in Mbit/s")
    tr.add_argument("--t6-size-mib", type=int, default=64, help="T6 worker-cut payload size in MiB")
    tr.add_argument("--t6-rate-mbps", type=float, default=12.0, help="T6 pre-cut proxy rate in Mbit/s")
    tr.add_argument("--t7-size-mib", type=int, default=256, help="T7 throttled payload size in MiB")
    tr.add_argument("--t7-rate-mbps", type=float, default=2.0, help="T7 proxy rate in Mbit/s")
    tr.add_argument("--t7-delay-ms", type=float, default=50.0, help="T7 proxy RTT delay in ms")
    tr.add_argument("--t8-store-mb", type=int, default=64, help="T8 master --blob-store-mb cap")
    tr.add_argument("--t9-size-gib", type=float, default=3.0, help="T9 oversize-cache file size in GiB")
    tr.add_argument("--t9-runs", type=int, default=3, help="T9 consecutive task count")
    tr.add_argument("--work-dir", help="Base directory for temporary stores and payloads")
    tr.add_argument("--keep-workdir", action="store_true", help="Keep temporary workdir on exit")
    tr.add_argument("--link", default="loopback", help="Link label (loopback, lan-tcp, lan-quic, quic-relay)")
    tr.add_argument("--master", help="Optional remote master address or P2P ticket (for two-machine runs)")
    tr.add_argument("--secret-file", help="Cluster secret file path when using --master")
    tr.add_argument("--insecure", action="store_true", help="Pass --insecure to client submits with --master")
    tr.add_argument("--rtt-ms", type=float, help="Measured link RTT in ms")
    tr.add_argument("--sample-interval", type=float, default=1.0, help="Resource poll interval in seconds")
    tr.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Base PRNG seed")
    tr.add_argument("--out", help="Output JSON record path")
    tr.add_argument("--no-write", action="store_true", help="Print JSON record instead of writing file")
    tr.add_argument("--label", default="", help="Free-text note stored in the record")

    th = sub.add_parser("throttle", help="Run loopback TCP throttle proxy (127.0.0.1 only)")
    th.add_argument("--listen", default="127.0.0.1:0", help="Listen address (127.0.0.1:PORT)")
    th.add_argument("--target", required=True, help="Upstream HOST:PORT")
    th.add_argument("--rate-mbps", type=float, help="Bandwidth cap per direction in Mbit/s")
    th.add_argument("--delay-ms", type=float, default=0.0, help="Simulated RTT in ms (split equally each way)")
    th.add_argument("--port-file", help="Write bound listen port to this file")

    sp = sub.add_parser("sample", help="Sample process RSS, handles, threads, CPU time, and dir bytes")
    sp.add_argument("--pid", action="append", default=[], help="ROLE=PID (repeatable)")
    sp.add_argument("--blob-dir", help="Master blob store directory")
    sp.add_argument("--cache-dir", help="Worker blob cache directory")
    sp.add_argument("--sandbox-dir", help="Worker sandbox base directory")
    sp.add_argument("--state-dir", help="Master state directory")
    sp.add_argument("--interval", type=float, default=1.0, help="Seconds between samples")
    sp.add_argument("--count", type=int, default=1, help="Number of samples to collect")
    sp.add_argument("--out", help="Write JSON output to file instead of stdout")

    return ap


def main(argv: Optional[List[str]] = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)
    if args.subcommand == "transfer":
        return cmd_transfer(args)
    if args.subcommand == "throttle":
        return cmd_throttle(args)
    if args.subcommand == "sample":
        return cmd_sample(args)
    raise SystemExit("unknown subcommand: %s" % args.subcommand)


if __name__ == "__main__":
    raise SystemExit(main())
