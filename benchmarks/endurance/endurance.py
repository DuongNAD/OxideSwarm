#!/usr/bin/env python3
"""
benchmarks/endurance/endurance.py - Large-transfer and resource endurance harness
for OxideSwarm (TEST-24 & TEST-25, design 0065).

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
  soak       Run multi-worker fault-injection soak test (D3) and write a JSON record
  throttle   Run a standalone loopback TCP bandwidth/delay proxy on 127.0.0.1
  sample     Sample memory, open handles, threads, CPU time, and store directories
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import ctypes
import datetime
import getpass
import hashlib
import json
import math
import ntpath
import os
import platform
import posixpath
import queue
import random
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
import urllib.error
import urllib.request

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
TRANSFER_LINKS = ("loopback", "lan-tcp", "lan-quic", "quic-relay")
LINK_SEED_OFFSETS = {
    "loopback": 0,
    "lan-tcp": 10000,
    "lan-quic": 20000,
    "quic-relay": 30000,
}

UPLOAD_RE = re.compile(
    r"Inputs: (\d+) files \(([^)]*)\); sent (\d+) of (\d+) blobs \(([^)]*)\), (\d+) already on the master"
)
EXEC_TIME_RE = re.compile(r"Execution Time:\s*(\d+)ms")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

_DRIVE_PATH_RE = re.compile(r"[A-Za-z]:\\[^\s\"'<>|,;\)\]]+")
_USER_UNIX_PATH_RE = re.compile(r"/(?:Users|home)/[^/\s\"'<>|,;\)\]]+(?:/[^\s\"'<>|,;\)\]]*)*")
_IPV4_PORT_RE = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3})(:\d+)?\b")
_P2P_TICKET_JSON_RE = re.compile(r'\{"id":"[0-9a-f]{32,128}","addrs":\[[^\n]*?\]\}')
_BRACKET_IPV6_RE = re.compile(r"\[(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}\](?::\d+)?")

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


# ---------------------------------------------------------------- task ledger & D3 soak checks


_DURATION_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([smhdSMHD]?)\s*$")
_DURATION_UNITS = {"": 1.0, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def parse_duration_seconds(raw: Any) -> float:
    """Parse durations like '45m', '24h', '180s', '1.5h', or numeric seconds (> 0)."""
    if isinstance(raw, (int, float)):
        val = float(raw)
    else:
        m = _DURATION_RE.match(str(raw or ""))
        if not m:
            raise ValueError("invalid duration %r (expected e.g. '180s', '45m', '24h')" % (raw,))
        val = float(m.group(1)) * _DURATION_UNITS[m.group(2).lower()]
    if not math.isfinite(val) or val <= 0.0:
        raise ValueError("duration must be positive (got %r)" % (raw,))
    return val


def percentile(values: Iterable[float], pct: float) -> float:
    """Compute linear-interpolated percentile (0..100) over a non-empty numeric sequence."""
    ordered = sorted(float(v) for v in values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    if pct <= 0.0:
        return ordered[0]
    if pct >= 100.0:
        return ordered[-1]
    pos = (pct / 100.0) * (len(ordered) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def is_queue_full_error(message: str) -> bool:
    """Return True strictly for master queue-capacity rejections (GridError::QueueFull)."""
    low = (message or "").lower()
    return ("task queue is full" in low) or ("queue is full" in low)


def classify_soak_submit_error(kind: str, message: str, returncode: Optional[int] = None) -> str:
    """Classify a non-ok submit: only queue-full during queue_burst counts as a refusal."""
    _ = returncode
    if kind == "queue_burst" and is_queue_full_error(message):
        return "refusal"
    return "failure"


class TaskLedger:
    """Tracks submitted tasks and verifies exactly-once terminal accounting."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._submitted: Dict[str, Dict[str, Any]] = {}
        self._results: Dict[str, List[Dict[str, Any]]] = {}
        self._retries: Dict[str, int] = {}
        self._refusals: List[Dict[str, str]] = []
        self._submit_failures: List[Dict[str, Any]] = []

    def submitted_task_ids(self) -> Set[str]:
        with self._lock:
            return set(self._submitted.keys())

    def record_submit(
        self,
        task_id: str,
        expected_hash: Optional[str] = None,
        expected_outcome: str = "ok",
        kind: str = "generic",
        allowed_outcomes: Optional[Iterable[str]] = None,
    ) -> None:
        allowed = tuple(allowed_outcomes) if allowed_outcomes is not None else (expected_outcome,)
        with self._lock:
            self._submitted[task_id] = {
                "expected_hash": expected_hash,
                "expected_outcome": expected_outcome,
                "allowed_outcomes": allowed,
                "kind": kind,
            }

    def record_result(
        self,
        task_id: str,
        actual_hash: Optional[str] = None,
        outcome: str = "ok",
        worker_id: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._results.setdefault(task_id, []).append(
                {
                    "actual_hash": actual_hash,
                    "outcome": outcome,
                    "worker_id": worker_id,
                }
            )

    def record_retry(self, task_id: str, count: int = 1) -> None:
        if count <= 0:
            return
        with self._lock:
            self._retries[task_id] = self._retries.get(task_id, 0) + int(count)

    def set_task_retries(self, task_id: str, count: int) -> None:
        """Record observed total retry count for a task (idempotent across snapshots)."""
        if count <= 0:
            return
        with self._lock:
            prev = self._retries.get(task_id, 0)
            if int(count) > prev:
                self._retries[task_id] = int(count)

    def record_submit_refusal(self, kind: str, reason: str) -> None:
        with self._lock:
            self._refusals.append({"kind": str(kind), "reason": str(reason)})

    def record_submit_failure(self, kind: str, reason: str, rc: Optional[int] = None) -> None:
        with self._lock:
            entry: Dict[str, Any] = {"kind": str(kind), "reason": str(reason)}
            if rc is not None:
                entry["rc"] = int(rc)
            self._submit_failures.append(entry)

    def audit(self) -> Dict[str, Any]:
        with self._lock:
            lost: List[str] = []
            duplicates: List[str] = []
            hash_mismatches: List[Dict[str, Any]] = []
            unexpected_outcomes: List[Dict[str, Any]] = []
            untracked_results: List[str] = []
            by_kind: Dict[str, Dict[str, int]] = {}
            outcome_counts: Dict[str, int] = {}

            for tid, sub in self._submitted.items():
                kind = sub.get("kind", "generic")
                kentry = by_kind.setdefault(kind, {"submitted": 0, "terminal": 0, "failed_check": 0})
                kentry["submitted"] += 1
                res_list = self._results.get(tid, [])
                if not res_list:
                    lost.append(tid)
                    kentry["failed_check"] += 1
                    continue
                kentry["terminal"] += 1
                if len(res_list) > 1:
                    duplicates.append(tid)
                    kentry["failed_check"] += 1
                res = res_list[0]
                outcome = res["outcome"]
                outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
                bad = False
                if outcome not in sub["allowed_outcomes"]:
                    bad = True
                    unexpected_outcomes.append(
                        {
                            "task_id": tid,
                            "kind": kind,
                            "expected": list(sub["allowed_outcomes"]),
                            "actual": outcome,
                        }
                    )
                if (
                    sub["expected_hash"] is not None
                    and outcome == "ok"
                    and res["actual_hash"] != sub["expected_hash"]
                ):
                    bad = True
                    hash_mismatches.append(
                        {
                            "task_id": tid,
                            "kind": kind,
                            "expected": sub["expected_hash"],
                            "actual": res["actual_hash"],
                        }
                    )
                if bad:
                    kentry["failed_check"] += 1

            for tid in self._results:
                if tid not in self._submitted:
                    untracked_results.append(tid)

            refusals_by_kind: Dict[str, int] = {}
            refusal_reason_counts: Dict[str, int] = {}
            for r in self._refusals:
                rk = str(r.get("kind") or "generic")
                rr = str(r.get("reason") or "")
                refusals_by_kind[rk] = refusals_by_kind.get(rk, 0) + 1
                refusal_reason_counts[rr] = refusal_reason_counts.get(rr, 0) + 1

            submit_failures_by_kind: Dict[str, int] = {}
            for sf in self._submit_failures:
                sk = str(sf.get("kind") or "generic")
                submit_failures_by_kind[sk] = submit_failures_by_kind.get(sk, 0) + 1

            ok = (
                not lost
                and not duplicates
                and not hash_mismatches
                and not unexpected_outcomes
                and not untracked_results
                and not self._submit_failures
            )
            return {
                "submitted": len(self._submitted),
                "completed": sum(1 for tid in self._submitted if self._results.get(tid)),
                "lost_tasks": sorted(lost),
                "duplicate_results": sorted(duplicates),
                "hash_mismatches": hash_mismatches,
                "unexpected_outcomes": unexpected_outcomes,
                "untracked_results": sorted(untracked_results),
                "retries_total": sum(self._retries.values()),
                "retried_tasks": len(self._retries),
                "refusals": len(self._refusals),
                "refusals_by_kind": refusals_by_kind,
                "refusal_reason_counts": refusal_reason_counts,
                "refusals_detail": list(self._refusals),
                "submit_failures_count": len(self._submit_failures),
                "submit_failures_by_kind": submit_failures_by_kind,
                "submit_failures": list(self._submit_failures),
                "outcome_counts": outcome_counts,
                "by_kind": by_kind,
                "ok": ok,
            }


def plan_soak_minute(
    minute_idx: int,
    seed: int,
    load_scale: float = 1.0,
    workspaces: bool = True,
    reused_pool_size: int = 5,
) -> List[Dict[str, Any]]:
    """Design 0065 D3 #2 deterministic per-minute seeded soak load."""
    if load_scale <= 0.0:
        raise ValueError("load_scale must be positive")
    rng = random.Random(int(seed) + 1009 * (int(minute_idx) + 1))

    def _scaled(base: int) -> int:
        if load_scale >= 1.0:
            return int(round(base * load_scale))
        return max(1, int(round(base * load_scale)))

    n_noop = _scaled(30)
    n_new = _scaled(5)
    n_reused = _scaled(5)
    n_output = _scaled(2)
    n_ws = 1 if workspaces else 0
    n_cancel = 1

    items: List[Dict[str, Any]] = []
    for _ in range(n_noop):
        items.append({"kind": "noop"})
    for _ in range(n_new):
        base_mib = rng.randint(10, 100)
        sz = max(1 * MIB, int(round(base_mib * MIB * min(1.0, load_scale))))
        items.append({"kind": "new_input", "size_bytes": sz, "seed": rng.randint(1, 2**31 - 1)})
    for _ in range(n_reused):
        items.append({"kind": "reused_input", "pool_index": rng.randrange(max(1, int(reused_pool_size)))})
    for _ in range(n_output):
        out_sz = max(1 * MIB, int(round(50 * MIB * min(1.0, load_scale))))
        items.append({"kind": "output", "size_bytes": out_sz, "seed": rng.randint(1, 2**31 - 1)})
    for _ in range(n_ws):
        items.append({"kind": "workspace_sync", "seed": rng.randint(1, 2**31 - 1)})
    for _ in range(n_cancel):
        items.append({"kind": "cancel", "sleep_s": 20.0})

    rng.shuffle(items)
    for seq, item in enumerate(items):
        item["minute"] = int(minute_idx)
        item["seq"] = seq
    return items


def plan_soak_faults(
    duration_s: float,
    fault_scale: float = 1.0,
    n_workers: int = 3,
) -> List[Dict[str, Any]]:
    """Design 0065 D3 #3 deterministic fault schedule scaled by --fault-scale."""
    if duration_s <= 0.0:
        raise ValueError("duration_s must be positive")
    if fault_scale <= 0.0:
        raise ValueError("fault_scale must be positive")
    workers = max(1, int(n_workers))
    events: List[Dict[str, Any]] = []
    # Keep final drain buffer near the end of the run so faults have time to recover.
    margin = min(duration_s * 0.10, max(5.0, 60.0 * fault_scale))

    specs = (
        ("worker_kill", 20.0 * 60.0 * fault_scale, max(2.0, 60.0 * fault_scale)),
        ("link_cut", 30.0 * 60.0 * fault_scale, max(1.0, 30.0 * fault_scale)),
        ("queue_burst", 60.0 * 60.0 * fault_scale, 0.0),
        ("master_restart", 120.0 * 60.0 * fault_scale, 0.0),
    )
    for kind, period_s, down_s in specs:
        idx = 0
        t = period_s
        while t < duration_s - margin:
            events.append(
                {
                    "at_s": round(t, 3),
                    "kind": kind,
                    "worker_index": idx % workers if kind in ("worker_kill", "link_cut") else None,
                    "down_s": round(down_s, 3),
                    "seq": idx,
                }
            )
            idx += 1
            t = period_s * (idx + 1)

    events.sort(key=lambda e: (e["at_s"], e["kind"]))
    return events


def split_soak_windows(
    rows: List[Dict[str, Any]],
    duration_s: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    """Return (baseline_rows, late_rows, mode_label) using D3 hour-2 vs last-hour or early/late fallback."""
    ordered = sorted(rows, key=lambda r: float(r.get("t_s", 0.0)))
    if duration_s >= 3.0 * 3600.0:
        base = [r for r in ordered if 3600.0 <= float(r.get("t_s", 0.0)) < 7200.0]
        late = [r for r in ordered if float(r.get("t_s", 0.0)) >= duration_s - 3600.0]
        if base and late:
            return base, late, "hour2_vs_last_hour"

    # Short soak (< 3 h) or sparse trace: skip initial warmup row(s) and compare early vs late halves.
    effective_dur = max(float(duration_s), max((float(r.get("t_s", 0.0)) for r in ordered), default=0.0))
    if len(ordered) >= 4:
        post_warmup = ordered[1:]
    else:
        post_warmup = ordered
    base = [r for r in post_warmup if 0.15 * effective_dur <= float(r.get("t_s", 0.0)) < 0.45 * effective_dur]
    late = [r for r in post_warmup if float(r.get("t_s", 0.0)) >= 0.70 * effective_dur]
    if not base or not late:
        mid = max(1, len(post_warmup) // 2)
        base = post_warmup[:mid]
        late = post_warmup[mid:]
    return base, late, "short_run_early_vs_late"


def summarize_process_lifetimes(samples: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group sample rows into contiguous per-PID process lifetimes for each role."""
    ordered = sorted(samples, key=lambda r: float(r.get("t_s", 0.0)))
    series_map: Dict[str, List[Dict[str, Any]]] = {}
    active_by_role: Dict[str, Tuple[Optional[int], Dict[str, Any]]] = {}

    for row in ordered:
        t_s = float(row.get("t_s", 0.0))
        for role, stats in row.get("processes", {}).items():
            if role == "client":
                continue
            pid_raw = stats.get("pid")
            pid = int(pid_raw) if pid_raw is not None else None
            rss = int(stats.get("rss_bytes", 0))
            handles = int(stats.get("handles", 0))
            cur = active_by_role.get(role)
            if cur is None or cur[0] != pid:
                entry: Dict[str, Any] = {
                    "pid": pid,
                    "generation": len(series_map.get(role, [])),
                    "first_t_s": t_s,
                    "last_t_s": t_s,
                    "samples": 1,
                    "first_rss_bytes": rss,
                    "peak_rss_bytes": rss,
                    "last_rss_bytes": rss,
                    "rss_delta_bytes": 0,
                    "first_handles": handles,
                    "peak_handles": handles,
                    "last_handles": handles,
                    "handles_delta": 0,
                }
                series_map.setdefault(role, []).append(entry)
                active_by_role[role] = (pid, entry)
            else:
                entry = cur[1]
                entry["last_t_s"] = t_s
                entry["samples"] += 1
                entry["peak_rss_bytes"] = max(entry["peak_rss_bytes"], rss)
                entry["last_rss_bytes"] = rss
                entry["rss_delta_bytes"] = rss - entry["first_rss_bytes"]
                entry["peak_handles"] = max(entry["peak_handles"], handles)
                entry["last_handles"] = handles
                entry["handles_delta"] = handles - entry["first_handles"]

    return series_map


def check_soak_resource_plateau(
    samples: List[Dict[str, Any]],
    duration_s: float,
) -> Dict[str, Any]:
    """Design 0065 D3 #6.2: memory within max(10%, 32 MiB) and handles within 10% across baseline vs late."""
    base_rows, late_rows, mode = split_soak_windows(samples, duration_s)
    lifetimes = summarize_process_lifetimes(samples)
    if not base_rows or not late_rows:
        return {"mode": mode, "roles": {}, "process_lifetimes": lifetimes, "passed": False, "reason": "insufficient_samples"}

    roles_seen: Set[str] = set()
    for row in base_rows:
        roles_seen.update(row.get("processes", {}).keys())
    late_roles: Set[str] = set()
    for row in late_rows:
        late_roles.update(row.get("processes", {}).keys())
    roles = sorted(r for r in (roles_seen & late_roles) if r != "client")
    if not roles:
        return {"mode": mode, "roles": {}, "process_lifetimes": lifetimes, "passed": False, "reason": "no_common_process_roles"}

    role_checks: Dict[str, Any] = {}
    all_ok = True
    for role in roles:
        b_rss = [row["processes"][role]["rss_bytes"] for row in base_rows if role in row.get("processes", {})]
        l_rss = [row["processes"][role]["rss_bytes"] for row in late_rows if role in row.get("processes", {})]
        b_h = [row["processes"][role]["handles"] for row in base_rows if role in row.get("processes", {})]
        l_h = [row["processes"][role]["handles"] for row in late_rows if role in row.get("processes", {})]
        b_pids = sorted({int(row["processes"][role]["pid"]) for row in base_rows if role in row.get("processes", {}) and row["processes"][role].get("pid") is not None})
        l_pids = sorted({int(row["processes"][role]["pid"]) for row in late_rows if role in row.get("processes", {}) and row["processes"][role].get("pid") is not None})
        mem = check_memory_plateau(statistics.mean(b_rss), statistics.mean(l_rss))
        hnd = check_handle_plateau(statistics.mean(b_h), statistics.mean(l_h))
        role_ok = bool(mem["passed"] and hnd["passed"])
        all_ok = all_ok and role_ok
        same_pid = bool(b_pids and l_pids and b_pids == l_pids and len(b_pids) == 1)
        role_checks[role] = {
            "baseline_samples": len(b_rss),
            "late_samples": len(l_rss),
            "baseline_pids": b_pids,
            "late_pids": l_pids,
            "same_process": same_pid,
            "memory": mem,
            "handles": hnd,
            "passed": role_ok,
        }

    return {
        "mode": mode,
        "baseline_window_s": [base_rows[0]["t_s"], base_rows[-1]["t_s"]],
        "late_window_s": [late_rows[0]["t_s"], late_rows[-1]["t_s"]],
        "roles": role_checks,
        "process_lifetimes": lifetimes,
        "passed": all_ok,
    }


def check_soak_disk_and_journal(
    samples: List[Dict[str, Any]],
    blob_store_limit_bytes: int,
    worker_cache_limit_bytes: int,
    pinned_headroom_bytes: int,
    idle_sandbox_entries: Dict[str, List[str]],
    tmp_leftovers: Dict[str, List[str]],
    compactions: List[Dict[str, Any]],
    idle_blob_store_bytes: Optional[int] = None,
    idle_worker_cache_bytes: Optional[Dict[str, int]] = None,
    worker_pinned_headroom_bytes: int = 0,
) -> Dict[str, Any]:
    """Design 0065 D3 #6.3: store/cache bounds, idle sandboxes hold cache only, journal shrinks on compaction."""
    blob_peaks = [
        int(row.get("dirs", {}).get("blob_store_bytes", 0))
        for row in samples
        if "blob_store_bytes" in row.get("dirs", {})
    ]
    peak_blob_bytes = max(blob_peaks) if blob_peaks else 0
    if idle_blob_store_bytes is None:
        idle_blob_store_bytes = int(samples[-1]["dirs"]["blob_store_bytes"]) if samples and "blob_store_bytes" in samples[-1].get("dirs", {}) else 0

    store_peak_ok = peak_blob_bytes <= int(blob_store_limit_bytes) + int(pinned_headroom_bytes)
    store_idle_ok = int(idle_blob_store_bytes) <= int(blob_store_limit_bytes)

    if idle_worker_cache_bytes is None:
        idle_worker_cache_bytes = {}
        if samples:
            for k, v in samples[-1].get("dirs", {}).items():
                if k == "worker_cache_bytes" or k.startswith("worker_cache_w"):
                    idle_worker_cache_bytes[k] = int(v)

    worker_cache_peak_bytes: Dict[str, int] = {}
    worker_cache_over_base_limit_samples: Dict[str, List[Dict[str, Any]]] = {}
    for row in samples:
        t_s = float(row.get("t_s", 0.0))
        for k, v in row.get("dirs", {}).items():
            if k == "worker_cache_bytes" or k.startswith("worker_cache_w"):
                sz = int(v)
                worker_cache_peak_bytes[k] = max(worker_cache_peak_bytes.get(k, 0), sz)
                if sz > int(worker_cache_limit_bytes):
                    worker_cache_over_base_limit_samples.setdefault(k, []).append({"t_s": t_s, "bytes": sz})

    cache_idle_ok = all(int(v) <= int(worker_cache_limit_bytes) for v in idle_worker_cache_bytes.values())
    cache_peak_limit = int(worker_cache_limit_bytes) + int(worker_pinned_headroom_bytes)
    cache_peak_ok = all(int(v) <= cache_peak_limit for v in worker_cache_peak_bytes.values())
    cache_ok = cache_idle_ok and cache_peak_ok

    unexpected_sandbox_entries = {
        w: sorted(e for e in entries if e not in ("cache", ".locks"))
        for w, entries in idle_sandbox_entries.items()
        if any(e not in ("cache", ".locks") for e in entries)
    }
    non_empty_tmp = {k: sorted(v) for k, v in tmp_leftovers.items() if v}
    sandbox_ok = (not unexpected_sandbox_entries) and (not non_empty_tmp)

    journal_ok = bool(compactions) and all(
        int(c["after_bytes"]) < int(c["before_bytes"])
        for c in compactions
        if int(c["before_bytes"]) > 24
    )

    return {
        "blob_store_peak_bytes": peak_blob_bytes,
        "blob_store_idle_bytes": int(idle_blob_store_bytes),
        "blob_store_limit_bytes": int(blob_store_limit_bytes),
        "pinned_headroom_bytes": int(pinned_headroom_bytes),
        "blob_store_ok": bool(store_peak_ok and store_idle_ok),
        "worker_cache_peak_bytes": worker_cache_peak_bytes,
        "worker_cache_over_base_limit_samples": worker_cache_over_base_limit_samples,
        "worker_cache_idle_bytes": dict(idle_worker_cache_bytes),
        "worker_cache_limit_bytes": int(worker_cache_limit_bytes),
        "worker_pinned_headroom_bytes": int(worker_pinned_headroom_bytes),
        "worker_cache_peak_ok": bool(cache_peak_ok),
        "worker_cache_idle_ok": bool(cache_idle_ok),
        "worker_cache_ok": bool(cache_ok),
        "unexpected_sandbox_entries": unexpected_sandbox_entries,
        "tmp_leftovers": non_empty_tmp,
        "sandbox_ok": bool(sandbox_ok),
        "compactions": list(compactions),
        "journal_compaction_ok": bool(journal_ok),
        "passed": bool(store_peak_ok and store_idle_ok and cache_ok and sandbox_ok and journal_ok),
    }


def _fault_interval_s(fault: Dict[str, Any], pad_s: float = 15.0) -> Tuple[float, float]:
    start = float(fault.get("t_s", fault.get("at_s", 0.0)))
    if "end_s" in fault and fault["end_s"] is not None:
        end = float(fault["end_s"])
    else:
        dur = float(fault.get("down_s") or fault.get("pause_s") or 0.0)
        end = start + dur
    return start, max(start, end) + float(pad_s)


def probe_overlaps_fault(
    probe: Dict[str, Any],
    faults: Iterable[Dict[str, Any]],
    pad_s: float = 15.0,
) -> bool:
    """Return True if a probe's execution interval [t_s, t_s + latency] overlaps any fault window."""
    p_start = float(probe.get("t_s", 0.0))
    p_end = p_start + max(0.0, float(probe.get("latency_ms", 0.0)) / 1000.0)
    for f in faults:
        f_start, f_end = _fault_interval_s(f, pad_s=pad_s)
        if p_end >= f_start and p_start <= f_end:
            return True
    return False


def check_soak_probe_p99(
    probes: List[Dict[str, Any]],
    duration_s: float,
    max_ratio: float = 1.5,
    faults: Optional[Iterable[Dict[str, Any]]] = None,
    fault_pad_s: float = 15.0,
    min_samples_per_window: int = 10,
) -> Dict[str, Any]:
    """Design 0065 D3 #6.4: clean non-fault probe p99 in late window <= 1.5x baseline window."""
    fault_list = list(faults) if faults is not None else []
    ok_probes = [p for p in probes if p.get("ok") and p.get("latency_ms") is not None]
    if fault_list:
        clean_probes = [p for p in ok_probes if not probe_overlaps_fault(p, fault_list, pad_s=fault_pad_s)]
    else:
        clean_probes = list(ok_probes)
    excluded_fault_probes = len(ok_probes) - len(clean_probes)

    eff_dur = max(float(duration_s), max((float(p.get("t_s", 0.0)) for p in ok_probes), default=0.0))
    ordered = sorted(clean_probes, key=lambda r: float(r.get("t_s", 0.0)))
    if duration_s >= 3.0 * 3600.0:
        mode = "hour2_vs_last_hour"
        base_rows = [r for r in ordered if 3600.0 <= float(r.get("t_s", 0.0)) < 7200.0]
        late_rows = [r for r in ordered if float(r.get("t_s", 0.0)) >= duration_s - 3600.0]
    else:
        mode = "short_run_early_vs_late"
        base_rows = [r for r in ordered if 0.10 * eff_dur <= float(r.get("t_s", 0.0)) < 0.50 * eff_dur]
        late_rows = [r for r in ordered if float(r.get("t_s", 0.0)) >= 0.55 * eff_dur]

    if len(base_rows) < int(min_samples_per_window) or len(late_rows) < int(min_samples_per_window):
        return {
            "mode": mode,
            "total_probes": len(probes),
            "ok_probes": len(ok_probes),
            "excluded_fault_probes": excluded_fault_probes,
            "fault_windows_used": len(fault_list),
            "fault_pad_s": float(fault_pad_s),
            "min_samples_required": int(min_samples_per_window),
            "baseline_count": len(base_rows),
            "late_count": len(late_rows),
            "passed": False,
            "reason": "insufficient_probes_per_window",
        }
    base_p99 = percentile([float(r["latency_ms"]) for r in base_rows], 99.0)
    late_p99 = percentile([float(r["latency_ms"]) for r in late_rows], 99.0)
    ratio_res = check_p99_ratio(max(0.001, base_p99), late_p99, max_ratio=max_ratio)
    return {
        "mode": mode,
        "total_probes": len(probes),
        "ok_probes": len(ok_probes),
        "excluded_fault_probes": excluded_fault_probes,
        "fault_windows_used": len(fault_list),
        "fault_pad_s": float(fault_pad_s),
        "min_samples_required": int(min_samples_per_window),
        "baseline_count": len(base_rows),
        "late_count": len(late_rows),
        "baseline_p99_ms": round(base_p99, 3),
        "late_p99_ms": round(late_p99, 3),
        "ratio": ratio_res["ratio"],
        "max_ratio": max_ratio,
        "passed": bool(ratio_res["passed"] and len(ok_probes) == len(probes)),
    }


def check_soak_faults(
    fault_plan: List[Dict[str, Any]],
    faults_executed: List[Dict[str, Any]],
    fault_errors: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Verify every planned fault executed cleanly with zero silent or unhandled failures."""
    errors = list(fault_errors or [])
    planned_keys = sorted(
        (str(e.get("kind")), int(e.get("seq", -1)), e.get("worker_index"))
        for e in fault_plan
    )
    executed_ok = [e for e in faults_executed if e.get("ok", True)]
    executed_keys = sorted(
        (str(e.get("kind")), int(e.get("seq", -1)), e.get("worker_index"))
        for e in executed_ok
    )
    ok = (
        len(errors) == 0
        and len(faults_executed) == len(fault_plan)
        and len(executed_ok) == len(fault_plan)
        and planned_keys == executed_keys
    )
    return {
        "planned_count": len(fault_plan),
        "executed_count": len(faults_executed),
        "ok_count": len(executed_ok),
        "error_count": len(errors),
        "errors": errors,
        "passed": bool(ok),
    }


def evaluate_soak_pass_checks(
    *,
    ledger_audit: Dict[str, Any],
    samples: List[Dict[str, Any]],
    probes: List[Dict[str, Any]],
    duration_s: float,
    blob_store_limit_bytes: int,
    worker_cache_limit_bytes: int,
    pinned_headroom_bytes: int,
    idle_sandbox_entries: Dict[str, List[str]],
    tmp_leftovers: Dict[str, List[str]],
    compactions: List[Dict[str, Any]],
    leftover_pids: List[int],
    idle_blob_store_bytes: Optional[int] = None,
    idle_worker_cache_bytes: Optional[Dict[str, int]] = None,
    worker_pinned_headroom_bytes: int = 0,
    master_audit: Optional[Dict[str, Any]] = None,
    fault_plan: Optional[List[Dict[str, Any]]] = None,
    faults_executed: Optional[List[Dict[str, Any]]] = None,
    fault_errors: Optional[List[Dict[str, Any]]] = None,
    leftover_task_trees: Optional[List[Dict[str, Any]]] = None,
    min_probe_samples_per_window: int = 10,
) -> Dict[str, Any]:
    """Compute all five D3 item 6 pass/fail criteria plus fault completeness over soak telemetry."""
    submitted = int(ledger_audit.get("submitted", 0))
    if master_audit is not None:
        multi_result = dict(master_audit.get("multi_result_tasks") or {})
        unique_result_tasks = int(master_audit.get("unique_result_tasks", 0))
        queue_record_errors = int(master_audit.get("queue_record_errors", 0))
        missing_master_results = list(master_audit.get("missing_from_master_results") or [])
        untracked_master_results = list(master_audit.get("untracked_master_results") or [])
        master_evidence_present = True
        master_ok = (
            len(multi_result) == 0
            and unique_result_tasks == submitted
            and queue_record_errors == 0
            and not missing_master_results
            and not untracked_master_results
        )
    else:
        multi_result = {}
        unique_result_tasks = 0
        queue_record_errors = 0
        missing_master_results = []
        untracked_master_results = []
        master_evidence_present = False
        master_ok = False

    c1_ok = bool(ledger_audit.get("ok")) and master_ok
    c1 = {
        "passed": c1_ok,
        "ledger": ledger_audit,
        "master_evidence_present": master_evidence_present,
        "master_duplicate_executions": len(multi_result),
        "master_multi_result_tasks": multi_result,
        "master_unique_result_tasks": unique_result_tasks,
        "master_expected_submitted": submitted,
        "master_queue_record_errors": queue_record_errors,
        "master_missing_results": missing_master_results,
        "master_untracked_results": untracked_master_results,
    }
    c2 = check_soak_resource_plateau(samples, duration_s)
    c3 = check_soak_disk_and_journal(
        samples,
        blob_store_limit_bytes=blob_store_limit_bytes,
        worker_cache_limit_bytes=worker_cache_limit_bytes,
        pinned_headroom_bytes=pinned_headroom_bytes,
        idle_sandbox_entries=idle_sandbox_entries,
        tmp_leftovers=tmp_leftovers,
        compactions=compactions,
        idle_blob_store_bytes=idle_blob_store_bytes,
        idle_worker_cache_bytes=idle_worker_cache_bytes,
        worker_pinned_headroom_bytes=worker_pinned_headroom_bytes,
    )
    faults_for_p99 = faults_executed if faults_executed is not None else (fault_plan or [])
    c4 = check_soak_probe_p99(
        probes,
        duration_s,
        faults=faults_for_p99,
        min_samples_per_window=min_probe_samples_per_window,
    )
    trees = list(leftover_task_trees or [])
    c5 = {
        "leftover_pids": [int(p) for p in leftover_pids],
        "leftover_task_trees": trees,
        "passed": len(leftover_pids) == 0 and len(trees) == 0,
    }
    if fault_plan is not None or faults_executed is not None or fault_errors is not None:
        c6 = check_soak_faults(list(fault_plan or []), list(faults_executed or []), fault_errors)
    else:
        c6 = {"planned_count": 0, "executed_count": 0, "ok_count": 0, "error_count": 0, "errors": [], "passed": True}

    all_passed = bool(
        c1["passed"] and c2["passed"] and c3["passed"] and c4["passed"] and c5["passed"] and c6["passed"]
    )
    return {
        "all_passed": all_passed,
        "checks": {
            "1_accounting": c1,
            "2_resource_plateau": c2,
            "3_disk_and_journal": c3,
            "4_probe_p99": c4,
            "5_clean_stop": c5,
            "6_fault_injection": c6,
        },
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
    if not path or not os.path.exists(path):
        return 0
    if os.path.isfile(path):
        try:
            return os.path.getsize(path)
        except OSError:
            return 0
    total = 0
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


def _replace_non_loopback_ipv6(m: re.Match) -> str:
    txt = m.group(0)
    if txt.startswith("[::1]"):
        return txt
    return "<ipv6>"


def _whole_token_pattern(token: str) -> re.Pattern:
    """Compile a case-insensitive whole-token matcher so short tokens do not hit substrings."""
    escaped = re.escape(token.strip())
    return re.compile(r"(?<![A-Za-z0-9_-])" + escaped + r"(?![A-Za-z0-9_-])", re.IGNORECASE)


def redact_text(text: str, extra_secrets: Iterable[str] = ()) -> str:
    """Scrub host names, user names, local paths, P2P tickets, and non-loopback IPs."""
    if not text:
        return text
    out = str(text)
    out = _P2P_TICKET_JSON_RE.sub("<p2p-ticket>", out)
    out = _BRACKET_IPV6_RE.sub(_replace_non_loopback_ipv6, out)
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
    if _P2P_TICKET_JSON_RE.search(value):
        hits.append({"field": field_path, "reason": "matched P2P ticket"})
    for m6 in _BRACKET_IPV6_RE.finditer(value):
        if not m6.group(0).startswith("[::1]"):
            hits.append({"field": field_path, "reason": "matched non-loopback IPv6 address"})
            break
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

    @staticmethod
    def _close_pairs(pairs: List[Tuple[socket.socket, socket.socket]]) -> None:
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

    def retarget(self, target_host: str, target_port: int) -> None:
        """Update upstream target host/port (e.g. after master restart) and reset active links."""
        with self._lock:
            self.target_host = target_host
            self.target_port = int(target_port)
            pairs = list(self._active_pairs)
            self._active_pairs.clear()
        self._close_pairs(pairs)

    def cut_links(self, pause_s: float = 0.0) -> None:
        with self._lock:
            self.cuts += 1
            if pause_s > 0:
                self._paused_until = max(self._paused_until, time.monotonic() + pause_s)
            pairs = list(self._active_pairs)
            self._active_pairs.clear()
        self._close_pairs(pairs)

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
        self._close_pairs(pairs)
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
                entry = dict(by_pid[pid])
                entry["pid"] = int(pid)
                results[role] = entry
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
                "pid": int(pid),
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
                "pid": int(pid),
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


def _isolated_process_env(home: str, empty_config: str) -> Dict[str, str]:
    """Isolated process environment with private HOME/OXIDESWARM_HOME while preserving Rust toolchains."""
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
    orig_home = os.environ.get("HOME") or os.environ.get("USERPROFILE")
    if "RUSTUP_HOME" not in env and orig_home:
        cand = os.path.join(orig_home, ".rustup")
        if os.path.isdir(cand):
            env["RUSTUP_HOME"] = cand
    if "CARGO_HOME" not in env and orig_home:
        cand = os.path.join(orig_home, ".cargo")
        if os.path.isdir(cand):
            env["CARGO_HOME"] = cand
    env.update(
        HOME=home,
        USERPROFILE=home,
        OXIDESWARM_HOME=ox_home,
        TEMP=tmp_dir,
        TMP=tmp_dir,
        TMPDIR=tmp_dir,
        RUSTY_GRID_CONFIG=empty_config,
        RUSTY_GRID_NO_BATTERY="1",
        NO_COLOR="1",
    )
    return env


class EnduranceCluster:
    """Isolated master + worker(s) on 127.0.0.1 using ephemeral ports and private homes."""
    is_remote = False

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
        return _isolated_process_env(home, self.empty_config)

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


def resolve_master_arg(raw: Optional[str]) -> Optional[str]:
    """Resolve --master value, reading UTF-8 ticket file when prefixed with '@'."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.startswith("@"):
        path = os.path.expanduser(text[1:].strip())
        if not path or not os.path.isfile(path):
            raise ValueError("master ticket file not found: %s" % path)
        with open(path, encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            raise ValueError("master ticket file is empty: %s" % path)
        return content
    return text


def _parse_host_port(addr: str) -> Tuple[str, int]:
    s = (addr or "").strip()
    if not s or s.startswith("{") or ":" not in s:
        return "", 0
    if s.startswith("["):
        if "]:" not in s:
            return "", 0
        host, pstr = s[1:].rsplit("]:", 1)
    else:
        host, pstr = s.rsplit(":", 1)
    if not pstr.isdigit():
        return "", 0
    return host, int(pstr)


def _sum_tmp_bytes(blob_dir: Optional[str]) -> int:
    if not blob_dir:
        return 0
    tmp_dir = os.path.join(blob_dir, "tmp")
    if not os.path.isdir(tmp_dir):
        return 0
    total = 0
    try:
        with os.scandir(tmp_dir) as it:
            for entry in it:
                try:
                    total += entry.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
    except OSError:
        return 0
    return total


def _summarize_remote_worker(w: Dict[str, Any]) -> Dict[str, Any]:
    tags = [str(t) for t in w.get("tags") or []]
    keep_tags = [
        t for t in tags
        if t.startswith(("os:", "arch:", "iso:", "slots:", "proto:", "link:", "state:", "tool:"))
    ]
    return {
        "name": str(w.get("name") or ""),
        "cpu_cores": w.get("cpu_cores"),
        "ram_mb": w.get("ram_mb"),
        "has_gpu": bool(w.get("has_gpu")),
        "is_simulated_gpu": bool(w.get("is_simulated_gpu")),
        "link": str(w.get("link") or ""),
        "tags": keep_tags,
    }


class RemoteEnduranceCluster:
    """Adapter for TEST-24 two-machine runs against an already-running remote master and worker."""
    is_remote = True

    def __init__(
        self,
        binary: str,
        workdir: str,
        master_addr: str,
        *,
        secret_file: Optional[str] = None,
        insecure: bool = False,
        require_tags: Iterable[str] = (),
        target_worker: Optional[str] = None,
        master_pid: Optional[int] = None,
        master_blob_dir: Optional[str] = None,
        master_log: Optional[str] = None,
    ) -> None:
        self.binary = binary
        self.workdir = os.path.abspath(workdir)
        os.makedirs(self.workdir, exist_ok=True)
        self.master_addr = str(master_addr).strip()
        self.master_host, self.master_port = _parse_host_port(self.master_addr)
        self.secret_file = os.path.abspath(secret_file) if secret_file else None
        self.insecure = bool(insecure)
        self.require_tags = [str(t).strip() for t in require_tags if str(t).strip()]
        self.target_worker = str(target_worker).strip() if (target_worker and str(target_worker).strip()) else None
        self.master_pid = int(master_pid) if master_pid else None
        self.blob_dir = os.path.abspath(master_blob_dir) if master_blob_dir else None
        self.master_log = os.path.abspath(master_log) if master_log else None
        self.sandbox_dirs: List[str] = []
        self.empty_config = os.path.join(self.workdir, "empty.toml")
        with open(self.empty_config, "w", encoding="utf-8") as f:
            f.write("")
        self._extra_pids_lock = threading.Lock()
        self._extra_pids: Dict[str, int] = {}
        self.client_env = self._build_env(os.path.join(self.workdir, "home-client"))
        self._log_start_offset = 0
        self.remote_workers: List[Dict[str, Any]] = []
        self.forbidden_tokens = [
            tok for tok in (self.master_addr, self.secret_file, self.blob_dir, self.master_log) if tok
        ]

    def _build_env(self, home: str) -> Dict[str, str]:
        return _isolated_process_env(home, self.empty_config)

    def _auth_flags(self) -> List[str]:
        if self.secret_file:
            return ["--secret-file", self.secret_file]
        if self.insecure:
            return ["--insecure"]
        return []

    def build_cli_cmd(self, args: List[str], master_addr: Optional[str] = None) -> List[str]:
        subcmd = args[:1]
        rest = list(args[1:])
        addr = master_addr or self.master_addr
        extra = list(self._auth_flags())
        if subcmd == ["submit"]:
            if self.target_worker:
                extra.extend(["--worker", self.target_worker])
            for tag in self.require_tags:
                extra.extend(["--require-tag", tag])
        return [self.binary] + subcmd + ["--master", addr] + extra + rest

    def active_pids(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if self.master_pid and pid_alive(self.master_pid):
            out["master"] = self.master_pid
        with self._extra_pids_lock:
            out.update(self._extra_pids)
        return out

    def set_extra_pid(self, role: str, pid: Optional[int]) -> None:
        with self._extra_pids_lock:
            if pid is None:
                self._extra_pids.pop(role, None)
            else:
                self._extra_pids[role] = int(pid)

    def _matches_tags(self, worker: Dict[str, Any]) -> bool:
        wtags = set(str(t) for t in (worker.get("tags") or []))
        if any(t.startswith("state:") for t in wtags) and "state:connected" not in wtags:
            return False
        if self.target_worker:
            wname = str(worker.get("name") or "")
            wid = ""
            for t in wtags:
                if t.startswith("id:"):
                    wid = t[3:]
                    break
            if self.target_worker.lower() not in (wname.lower(), wid.lower()):
                return False
        if not self.require_tags:
            return True
        return all(t in wtags for t in self.require_tags)

    def start(self) -> "RemoteEnduranceCluster":
        if self.master_log and os.path.isfile(self.master_log):
            try:
                self._log_start_offset = os.path.getsize(self.master_log)
            except OSError:
                self._log_start_offset = 0
        deadline = time.monotonic() + 20.0
        last_err = ""
        while time.monotonic() < deadline:
            r = self.run_client(["workers", "--json"], timeout=15.0)
            if r.returncode == 0:
                try:
                    listed = json.loads(r.stdout)
                except ValueError as exc:
                    last_err = "invalid workers JSON: %s" % exc
                else:
                    matched = [w for w in listed if self._matches_tags(w)] if isinstance(listed, list) else []
                    if matched:
                        self.remote_workers = [_summarize_remote_worker(w) for w in matched]
                        return self
                    last_err = "no connected worker matching tags=%r (saw %d workers)" % (
                        self.require_tags,
                        len(listed) if isinstance(listed, list) else 0,
                    )
            else:
                last_err = redact_text((r.stderr or r.stdout or "").strip()[-300:], self.forbidden_tokens)
            time.sleep(0.5)
        raise RuntimeError("remote cluster readiness failed: %s" % (last_err or "timeout"))

    def dir_map(self) -> Dict[str, str]:
        if self.blob_dir:
            return {"blob_store_bytes": self.blob_dir}
        return {}

    def run_client(
        self,
        args: List[str],
        timeout: float = 120.0,
        master_addr: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        cmd = self.build_cli_cmd(args, master_addr=master_addr)
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
        cmd = self.build_cli_cmd(["submit"] + submit_args, master_addr=master_addr)
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
        if role != "master" or not self.master_log or not os.path.isfile(self.master_log):
            return ""
        with open(self.master_log, encoding="utf-8", errors="replace") as f:
            if self._log_start_offset > 0:
                try:
                    if os.path.getsize(self.master_log) >= self._log_start_offset:
                        f.seek(self._log_start_offset)
                except OSError:
                    pass
            return f.read()

    def stop(self) -> None:
        # Remote mode never stops the external master or remote worker; only remove client workdir.
        shutil.rmtree(self.workdir, ignore_errors=True)


def pid_alive(pid: int) -> bool:
    """Return True if process `pid` is currently running (checked strictly by exact PID)."""
    if pid <= 0:
        return False
    if IS_WINDOWS:
        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
        if not handle:
            return False
        try:
            code = ctypes.c_ulong(0)
            ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
            return bool(ok) and int(code.value) == 259
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.waitpid(int(pid), os.WNOHANG)
    except OSError:
        pass
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def expected_seeded_sha256(size_bytes: int, seed: int) -> str:
    """Compute SHA-256 of deterministic seeded payload bytes without writing a second copy on disk."""
    rng = random.Random(int(seed))
    h = hashlib.sha256()
    rem = int(size_bytes)
    while rem > 0:
        n = min(4 * MIB, rem)
        h.update(rng.randbytes(n) if hasattr(rng, "randbytes") else bytes(rng.getrandbits(8) for _ in range(n)))
        rem -= n
    return h.hexdigest()


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
_MASTER_RESULT_RE = re.compile(
    r"Task execution result received.*?\btask_id=([0-9a-f-]+)"
    r"|\btask_id=([0-9a-f-]+).*?Task execution result received"
)
_REAPER_FAILOVER_RE = re.compile(
    r"Reaper triggered task failover reassignment.*?\bcount=(\d+)"
    r"|\bcount=(\d+).*?Reaper triggered task failover reassignment"
)
_ORPHAN_REQUEUE_RE = re.compile(r"Re-enqueued orphaned tasks for reassignment")
_EVACUATED_REQUEUE_RE = re.compile(r"Processed evacuated tasks from draining worker")
_COUNT_FIELD_RE = re.compile(r"\bcount=(\d+)\b")
_TASK_ID_TAG_RE = re.compile(r"TaskId\(([0-9a-f-]+)\)")
_DEBUG_LINE_RE = re.compile(r"(?:^|\s)DEBUG(?:\s|$)")


def audit_master_log_texts(
    log_texts: Iterable[str],
    expected_task_ids: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Parse master log generations (stripping ANSI escapes) for results, failovers, requeues, and errors."""
    texts = list(log_texts)
    recv_counts: Dict[str, int] = {}
    failovers = 0
    superseded_failovers = 0
    reaper_failovers = 0
    orphan_requeues = 0
    evacuations_logged = 0
    requeued_task_ids: Set[str] = set()
    evacuated_task_ids: Set[str] = set()
    stale_ignored = 0
    late_ignored = 0
    queue_record_errors = 0
    has_debug_logs = False

    for raw in texts:
        txt = _ANSI_ESCAPE_RE.sub("", raw or "")
        for ln in txt.splitlines():
            if _DEBUG_LINE_RE.search(ln):
                has_debug_logs = True
            m = _MASTER_RESULT_RE.search(ln)
            if m:
                tid = m.group(1) or m.group(2)
                recv_counts[tid] = recv_counts.get(tid, 0) + 1
            if "Worker disconnected; executing failover sweep" in ln:
                failovers += 1
            if "Worker re-registered while its previous session was live; failing over the superseded session" in ln:
                superseded_failovers += 1
            rm = _REAPER_FAILOVER_RE.search(ln)
            if rm:
                reaper_failovers += int(rm.group(1) or rm.group(2) or 0)
            if _ORPHAN_REQUEUE_RE.search(ln):
                cm = _COUNT_FIELD_RE.search(ln)
                ids = _TASK_ID_TAG_RE.findall(ln)
                orphan_requeues += int(cm.group(1)) if cm else max(1, len(ids))
                requeued_task_ids.update(ids)
            if _EVACUATED_REQUEUE_RE.search(ln):
                cm = _COUNT_FIELD_RE.search(ln)
                ids = _TASK_ID_TAG_RE.findall(ln)
                evacuations_logged += int(cm.group(1)) if cm else max(1, len(ids))
                evacuated_task_ids.update(ids)
            if "Ignored result from a worker/session that does not own the task" in ln:
                has_debug_logs = True
                stale_ignored += 1
            if "Ignoring result for terminal task" in ln:
                has_debug_logs = True
                late_ignored += 1
            if "Failed to record task result in queue" in ln:
                queue_record_errors += 1

    multi_result = {tid: cnt for tid, cnt in recv_counts.items() if cnt > 1}
    missing_from_master: List[str] = []
    untracked_master: List[str] = []
    if expected_task_ids is not None:
        exp_set = set(expected_task_ids)
        seen_set = set(recv_counts.keys())
        missing_from_master = sorted(exp_set - seen_set)
        untracked_master = sorted(seen_set - exp_set)

    out: Dict[str, Any] = {
        "master_generations": len(texts),
        "result_messages_logged": sum(recv_counts.values()),
        "unique_result_tasks": len(recv_counts),
        "multi_result_tasks": multi_result,
        "failover_sweeps": failovers,
        "superseded_failovers": superseded_failovers,
        "reaper_failovers": reaper_failovers,
        "orphan_requeues": orphan_requeues,
        "evacuations_logged": evacuations_logged,
        "requeues_logged": orphan_requeues + reaper_failovers,
        "requeued_task_ids": sorted(requeued_task_ids),
        "evacuated_task_ids": sorted(evacuated_task_ids),
        "stale_late_results_measured": has_debug_logs,
        "stale_results_ignored": stale_ignored if has_debug_logs else None,
        "late_results_ignored": late_ignored if has_debug_logs else None,
        "queue_record_errors": queue_record_errors,
    }
    if not has_debug_logs:
        out["stale_late_results_note"] = (
            "Ignored stale/late results are logged at DEBUG level in crates/master/src "
            "and are not measured under default INFO logging"
        )
    if expected_task_ids is not None:
        out["missing_from_master_results"] = missing_from_master
        out["untracked_master_results"] = untracked_master
    return out


def apply_master_task_retries_to_ledger(
    ledger: TaskLedger,
    master_task_stats: Dict[str, Dict[str, int]],
    fallback_requeued_task_ids: Iterable[str] = (),
) -> Dict[str, Any]:
    """Populate TaskLedger retries from master /api/tasks telemetry (or INFO log task IDs)."""
    if master_task_stats:
        failovers_total = 0
        evacuations_total = 0
        retried_tasks = 0
        retries_total = 0
        for tid, st in master_task_stats.items():
            rc = int(st.get("retry_count", 0))
            fc = int(st.get("failover_count", 0))
            ec = int(st.get("evacuation_count", 0))
            failovers_total += fc
            evacuations_total += ec
            if rc > 0:
                ledger.set_task_retries(tid, rc)
                retried_tasks += 1
                retries_total += rc
        return {
            "source": "master_task_api",
            "tasks_observed": len(master_task_stats),
            "retried_tasks": retried_tasks,
            "retries_total": retries_total,
            "failovers_total": failovers_total,
            "evacuations_total": evacuations_total,
        }

    fb_ids = sorted(set(fallback_requeued_task_ids))
    for tid in fb_ids:
        ledger.set_task_retries(tid, 1)
    return {
        "source": "master_info_logs",
        "tasks_observed": len(fb_ids),
        "retried_tasks": len(fb_ids),
        "retries_total": len(fb_ids),
        "failovers_total": len(fb_ids),
        "evacuations_total": 0,
    }


def _list_system_processes() -> List[Dict[str, Any]]:
    """Read-only snapshot of running OS processes (pid, ppid, cmd, created_epoch)."""
    sysname = platform.system()
    out: List[Dict[str, Any]] = []
    if sysname == "Windows":
        ps_cmd = (
            "$me = $PID; "
            "$out = @(Get-CimInstance Win32_Process | Where-Object { $_.ProcessId -ne $me } | "
            "ForEach-Object { $c = 0; if ($_.CreationDate) { try { $c = [int64]([DateTimeOffset]$_.CreationDate).ToUnixTimeSeconds() } catch {} }; "
            "[pscustomobject]@{pid=[int]$_.ProcessId; ppid=[int]$_.ParentProcessId; cmd=$_.CommandLine; created_epoch=$c} }); "
            "ConvertTo-Json -InputObject $out -Compress"
        )
        raw = sh(["powershell", "-NoProfile", "-Command", ps_cmd], timeout=20.0)
        if not raw:
            return out
        try:
            parsed = json.loads(raw)
        except ValueError:
            return out
        if isinstance(parsed, dict):
            parsed = [parsed]
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and "pid" in item:
                    out.append(
                        {
                            "pid": int(item.get("pid") or 0),
                            "ppid": int(item.get("ppid") or 0),
                            "cmd": str(item.get("cmd") or ""),
                            "created_epoch": float(item.get("created_epoch") or 0.0) or None,
                        }
                    )
        return out

    ps_out = sh(["ps", "-eo", "pid=,ppid=,args="], timeout=10.0)
    if not ps_out:
        return out
    for line in ps_out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 2:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        cmd = parts[2] if len(parts) > 2 else ""
        out.append({"pid": pid, "ppid": ppid, "cmd": cmd, "created_epoch": None})
    return out


def find_leftover_process_trees(
    proc_table: Iterable[Dict[str, Any]],
    root_pids: Iterable[int],
    workdir: str,
    *,
    ignore_pids: Optional[Iterable[int]] = None,
    min_created_epoch: Optional[float] = None,
    root_pid_windows: Optional[Dict[int, List[Dict[str, Optional[float]]]]] = None,
) -> List[Dict[str, Any]]:
    """Find any live processes descended from started PIDs or referencing `workdir` in argv."""
    ignored = {int(p) for p in (ignore_pids or ())}
    by_pid: Dict[int, Dict[str, Any]] = {}
    children: Dict[int, List[Dict[str, Any]]] = {}
    for row in proc_table:
        pid = int(row.get("pid") or 0)
        if pid <= 0 or pid in ignored:
            continue
        ppid = int(row.get("ppid") or 0)
        by_pid[pid] = dict(row)
        children.setdefault(ppid, []).append(dict(row))

    found: Dict[int, str] = {}
    bfs_queue: List[Tuple[int, Optional[float]]] = []

    if root_pid_windows is not None:
        all_roots = sorted(int(p) for p in root_pid_windows if int(p) > 0 and int(p) not in ignored)
        for rpid in all_roots:
            windows = list(root_pid_windows.get(rpid) or ())
            if rpid in by_pid:
                r_created = by_pid[rpid].get("created_epoch")
                still_ours = any(
                    w.get("end_epoch") is None
                    and (
                        r_created is None
                        or w.get("start_epoch") is None
                        or float(r_created) >= float(w["start_epoch"]) - 2.0
                    )
                    for w in windows
                )
                if still_ours:
                    found[rpid] = "started_pid_alive"
            for ch in children.get(rpid, ()):
                cpid = int(ch.get("pid") or 0)
                if cpid <= 0 or cpid in ignored:
                    continue
                c_created = ch.get("created_epoch")
                if c_created is not None:
                    matched_window = any(
                        (w.get("start_epoch") is None or float(c_created) >= float(w["start_epoch"]) - 2.0)
                        and (w.get("end_epoch") is None or float(c_created) <= float(w["end_epoch"]) + 1.0)
                        for w in windows
                    )
                else:
                    matched_window = any(w.get("end_epoch") is None for w in windows) or (rpid not in by_pid)
                if not matched_window:
                    continue
                found.setdefault(cpid, "descendant_of_started_pid")
                bfs_queue.append((cpid, float(c_created) if c_created is not None else None))
    else:
        roots = {int(p) for p in root_pids if int(p) > 0} - ignored
        for rpid in sorted(roots):
            r_created: Optional[float] = None
            if rpid in by_pid:
                raw_c = by_pid[rpid].get("created_epoch")
                r_created = float(raw_c) if raw_c is not None else None
                if min_created_epoch is None or r_created is None or r_created >= float(min_created_epoch) - 2.0:
                    found[rpid] = "started_pid_alive"
            bfs_queue.append((rpid, r_created if r_created is not None else min_created_epoch))

    visited: Set[int] = {pid for pid, _ in bfs_queue}
    while bfs_queue:
        cur, cur_created = bfs_queue.pop(0)
        for ch in children.get(cur, ()):
            cpid = int(ch.get("pid") or 0)
            if cpid <= 0 or cpid in ignored:
                continue
            created = ch.get("created_epoch")
            c_epoch = float(created) if created is not None else None
            if c_epoch is not None:
                if min_created_epoch is not None and c_epoch < float(min_created_epoch) - 2.0:
                    continue
                if cur_created is not None and c_epoch < cur_created - 2.0:
                    continue
            found.setdefault(cpid, "descendant_of_started_pid")
            if cpid not in visited:
                visited.add(cpid)
                bfs_queue.append((cpid, c_epoch))

    wd_cmp = ""
    if workdir:
        if posixpath.isabs(workdir):
            wd_raw = posixpath.normpath(workdir)
        elif ntpath.isabs(workdir):
            wd_raw = ntpath.normpath(workdir)
        else:
            wd_raw = os.path.abspath(workdir)
        wd_cmp = wd_raw.replace("\\", "/")
        if IS_WINDOWS:
            wd_cmp = wd_cmp.lower()
    if wd_cmp:
        for pid, row in by_pid.items():
            cmd_cmp = str(row.get("cmd") or "").replace("\\", "/")
            if IS_WINDOWS:
                cmd_cmp = cmd_cmp.lower()
            if wd_cmp in cmd_cmp:
                found.setdefault(pid, "cmdline_references_workdir")

    redact_paths = [workdir] if workdir else []
    leftovers: List[Dict[str, Any]] = []
    for pid in sorted(found):
        row = by_pid[pid]
        leftovers.append(
            {
                "pid": pid,
                "ppid": int(row.get("ppid") or 0),
                "reason": found[pid],
                "cmd": redact_text(str(row.get("cmd") or "")[:240], redact_paths),
            }
        )
    return leftovers


class SoakCluster(EnduranceCluster):
    """Multi-worker soak cluster with --state-dir, per-worker loopback proxies, and fault injection."""

    def __init__(
        self,
        binary: str,
        workdir: str,
        workers: int = 3,
        slots: int = 4,
        blob_store_mb: int = 4096,
        blob_cache_mb: int = 2048,
        max_queue_size: int = 64,
        workspaces: bool = True,
        session_resume_hold_secs: Optional[int] = None,
        rate_mbps: Optional[float] = None,
        delay_ms: float = 0.0,
    ) -> None:
        state_dir = os.path.join(os.path.abspath(workdir), "master-state")
        super().__init__(
            binary,
            workdir,
            workers=workers,
            blob_store_mb=blob_store_mb,
            blob_cache_mb=blob_cache_mb,
            state_dir=state_dir,
        )
        self.slots = int(slots)
        self.max_queue_size = int(max_queue_size)
        self.workspaces = bool(workspaces)
        self.session_resume_hold_secs = session_resume_hold_secs
        self.rate_mbps = rate_mbps
        self.delay_ms = float(delay_ms)
        self.workspace_dirs = [os.path.join(self.workdir, "workspaces-w%d" % i) for i in range(self.n_workers)]
        self.proxies: List[ThrottleProxy] = []

        self._lifecycle_lock = threading.RLock()
        self._addr_lock = threading.Lock()
        self._master_restarting = threading.Event()
        self._sync_lock = threading.Lock()
        self._active_sync_submits = 0
        self._sync_cond = threading.Condition(self._sync_lock)

        self._master_gen = 0
        self._master_entry: Optional[Tuple[int, subprocess.Popen, Any]] = None
        self._worker_gen = [0] * self.n_workers
        self._worker_entries: Dict[int, Tuple[subprocess.Popen, Any]] = {}
        self._worker_down: Set[int] = set()
        self._tracked_clients_lock = threading.Lock()
        self._tracked_clients: Set[subprocess.Popen] = set()
        self._pid_windows_lock = threading.Lock()
        self._started_pid_windows: Dict[int, List[Dict[str, Optional[float]]]] = {}
        self._start_epoch_s: float = time.time()
        self.dashboard_addr: str = ""
        self._master_task_stats: Dict[str, Dict[str, int]] = {}
        self._api_snapshots_taken: int = 0
        self._api_snapshot_errors: List[str] = []
        self._unclean_pids: List[int] = []
        self.compactions: List[Dict[str, Any]] = []

    def _record_pid_start(self, pid: int) -> None:
        now = time.time()
        with self._pid_windows_lock:
            self._started_pid_windows.setdefault(int(pid), []).append(
                {"start_epoch": now, "end_epoch": None}
            )

    def _record_pid_exit(self, pid: int) -> None:
        now = time.time()
        with self._pid_windows_lock:
            windows = self._started_pid_windows.get(int(pid))
            if windows and windows[-1].get("end_epoch") is None:
                windows[-1]["end_epoch"] = now

    def _snapshot_pid_windows(self) -> Dict[int, List[Dict[str, Optional[float]]]]:
        with self._pid_windows_lock:
            return {pid: [dict(w) for w in wins] for pid, wins in self._started_pid_windows.items()}

    def current_master_addr(self) -> str:
        with self._addr_lock:
            return self.master_addr

    def wait_if_restarting(self, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        while self._master_restarting.is_set() and time.monotonic() < deadline:
            time.sleep(0.1)

    def enter_submit(self, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        with self._sync_cond:
            while self._master_restarting.is_set():
                rem = deadline - time.monotonic()
                if rem <= 0:
                    break
                self._sync_cond.wait(timeout=min(0.25, rem))
            self._active_sync_submits += 1

    def exit_submit(self) -> None:
        with self._sync_cond:
            self._active_sync_submits = max(0, self._active_sync_submits - 1)
            self._sync_cond.notify_all()

    def enter_sync_submit(self) -> None:
        self.enter_submit(timeout=60.0)

    def exit_sync_submit(self) -> None:
        self.exit_submit()

    def active_pids(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        with self._lifecycle_lock:
            if self._master_entry is not None and self._master_entry[1].poll() is None:
                out["master"] = self._master_entry[1].pid
            for idx, (proc, _log) in self._worker_entries.items():
                if proc.poll() is None:
                    out["worker%d" % idx] = proc.pid
        with self._extra_pids_lock:
            out.update(self._extra_pids)
        return out

    def _spawn_role_proc(self, log_name: str, home_name: str, args: List[str], new_group: bool = False) -> Tuple[subprocess.Popen, Any]:
        log = open(os.path.join(self.workdir, log_name + ".log"), "wb")
        kw: Dict[str, Any] = {}
        if IS_WINDOWS:
            if new_group:
                kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kw["start_new_session"] = True
        p = subprocess.Popen(
            [self.binary] + args,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=self._build_env(os.path.join(self.workdir, "home-" + home_name)),
            **kw,
        )
        self._record_pid_start(p.pid)
        return p, log

    def _start_master_locked(self) -> None:
        assert self.state_dir is not None
        os.makedirs(self.state_dir, exist_ok=True)
        os.makedirs(self.blob_dir, exist_ok=True)
        port_file = os.path.join(self.workdir, "master.port")
        dash_file = os.path.join(self.workdir, "dash.port")
        for fp in (port_file, dash_file):
            if os.path.exists(fp):
                try:
                    os.remove(fp)
                except OSError:
                    pass
        args = [
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
            "--blob-store-mb",
            str(int(self.blob_store_mb or 4096)),
            "--state-dir",
            self.state_dir,
            "--max-queue-size",
            str(self.max_queue_size),
        ]
        if self.session_resume_hold_secs is not None:
            args += ["--session-resume-hold-secs", str(int(self.session_resume_hold_secs))]

        gen = self._master_gen
        self._master_gen += 1
        p, log = self._spawn_role_proc("master-%d" % gen, "master", args, new_group=True)
        self._master_entry = (gen, p, log)
        self.pids["master"] = p.pid

        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if p.poll() is not None:
                raise RuntimeError("master exited early (gen=%d code=%s)" % (gen, p.returncode))
            if os.path.exists(port_file) and os.path.getsize(port_file) > 0:
                with open(port_file, encoding="utf-8", errors="replace") as f:
                    txt = f.read().strip()
                if txt:
                    addr = txt if ":" in txt else "127.0.0.1:" + txt
                    host, pstr = addr.rsplit(":", 1)
                    with self._addr_lock:
                        self.master_addr = addr
                        self.master_host = host
                        self.master_port = int(pstr)
                    dash_deadline = time.monotonic() + 5.0
                    self.dashboard_addr = ""
                    while time.monotonic() < dash_deadline:
                        if os.path.exists(dash_file) and os.path.getsize(dash_file) > 0:
                            with open(dash_file, encoding="utf-8", errors="replace") as df:
                                dtxt = df.read().strip()
                            if dtxt:
                                self.dashboard_addr = dtxt if ":" in dtxt else "127.0.0.1:" + dtxt
                                break
                        time.sleep(0.05)
                    return
            time.sleep(0.05)
        raise RuntimeError("master did not publish its port within 30 s")

    def _start_worker_locked(self, idx: int) -> None:
        os.makedirs(self.sandbox_dirs[idx], exist_ok=True)
        target_addr = self.proxies[idx].bound_addr if idx < len(self.proxies) else self.current_master_addr()
        args = [
            "worker",
            "--master",
            target_addr,
            "--name",
            "w%d" % idx,
            "--no-portal",
            "--no-gpu",
            "--max-concurrency",
            str(self.slots),
            "--sandbox-base-dir",
            self.sandbox_dirs[idx],
            "--blob-cache-mb",
            str(int(self.blob_cache_mb or 2048)),
        ]
        if self.workspaces:
            os.makedirs(self.workspace_dirs[idx], exist_ok=True)
            args += ["--workspaces", "--workspace-root", self.workspace_dirs[idx]]
        gen = self._worker_gen[idx]
        self._worker_gen[idx] += 1
        p, log = self._spawn_role_proc("worker%d-%d" % (idx, gen), "worker%d" % idx, args, new_group=False)
        self._worker_entries[idx] = (p, log)
        self.pids["worker%d" % idx] = p.pid
        self._worker_down.discard(idx)

    def _wait_workers_connected(self, expected_indices: Iterable[int], timeout: float = 60.0) -> None:
        idx_list = list(expected_indices)
        want = {"w%d" % idx for idx in idx_list}
        if not want:
            return
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lifecycle_lock:
                if self._master_entry is not None and self._master_entry[1].poll() is not None:
                    gen, mp, _log = self._master_entry
                    tail = "\n".join(self.read_log("master-%d" % gen).splitlines()[-10:])
                    raise RuntimeError("master exited early (code=%s): %s" % (mp.returncode, tail))
                for idx in idx_list:
                    entry = self._worker_entries.get(idx)
                    if entry is not None and entry[0].poll() is not None:
                        wlog = "worker%d-%d" % (idx, max(0, self._worker_gen[idx] - 1))
                        tail = "\n".join(self.read_log(wlog).splitlines()[-10:])
                        raise RuntimeError("worker%d exited early (code=%s): %s" % (idx, entry[0].returncode, tail))
            r = self.run_client(["workers", "--json"], timeout=15)
            if r.returncode == 0:
                try:
                    listed = json.loads(r.stdout)
                except ValueError:
                    listed = []
                if isinstance(listed, list):
                    connected_names = set()
                    for item in listed:
                        tags = item.get("tags") or []
                        state = next((t.split(":", 1)[1] for t in tags if str(t).startswith("state:")), "connected")
                        if state == "connected":
                            connected_names.add(item.get("name"))
                    if want.issubset(connected_names):
                        return
            time.sleep(0.15)
        raise RuntimeError("expected workers %s did not connect within %.0f s" % (sorted(want), timeout))

    def start(self) -> "SoakCluster":
        with self._lifecycle_lock:
            self._start_master_locked()
            self.proxies = [
                ThrottleProxy(
                    self.master_host,
                    self.master_port,
                    rate_mbps=self.rate_mbps,
                    delay_ms=self.delay_ms,
                ).start()
                for _ in range(self.n_workers)
            ]
            for i in range(self.n_workers):
                self._start_worker_locked(i)
            self._wait_workers_connected(range(self.n_workers), timeout=60.0)
        return self

    def kill_worker(self, idx: int) -> int:
        with self._lifecycle_lock:
            entry = self._worker_entries.pop(idx, None)
            self._worker_down.add(idx)
            if entry is None:
                return 0
            p, log = entry
            pid = p.pid
            if p.poll() is None:
                p.kill()
                p.wait(timeout=10.0)
            if pid_alive(pid):
                self._unclean_pids.append(pid)
            else:
                self._record_pid_exit(pid)
            try:
                log.close()
            except Exception:
                pass
            return pid

    def start_worker(self, idx: int) -> int:
        with self._lifecycle_lock:
            self._start_worker_locked(idx)
            new_pid = self._worker_entries[idx][0].pid
        if not self._master_restarting.is_set():
            self._wait_workers_connected([idx], timeout=45.0)
        return new_pid

    def cut_worker_link(self, idx: int, pause_s: float) -> None:
        if 0 <= idx < len(self.proxies):
            self.proxies[idx].cut_links(pause_s=pause_s)

    def stop_master(self, reason: str) -> Optional[Dict[str, Any]]:
        assert self.state_dir is not None
        with self._lifecycle_lock:
            if self._master_entry is None:
                return None
            gen, p, log = self._master_entry
            journal_path = os.path.join(self.state_dir, "journal.bin")
            before_bytes = dir_bytes(journal_path)
            pid = p.pid
            if p.poll() is None:
                if IS_WINDOWS:
                    p.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    p.terminate()
                try:
                    p.wait(timeout=25.0)
                except subprocess.TimeoutExpired:
                    p.terminate()
                    try:
                        p.wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        p.kill()
                        p.wait(timeout=5.0)
            if pid_alive(pid):
                self._unclean_pids.append(pid)
            else:
                self._record_pid_exit(pid)
            after_bytes = dir_bytes(journal_path)
            try:
                log.close()
            except Exception:
                pass
            self._master_entry = None
            comp = {
                "reason": reason,
                "generation": gen,
                "exit_code": p.returncode,
                "before_bytes": int(before_bytes),
                "after_bytes": int(after_bytes),
                "shrank": int(after_bytes) < int(before_bytes),
            }
            self.compactions.append(comp)
            return comp

    def snapshot_master_task_retries(self) -> int:
        """Query the live master /api/tasks dashboard API to record per-task retry and failover counts."""
        if not self.dashboard_addr:
            return 0
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        limit = 1000
        offset = 0
        seen_in_pass = 0
        try:
            for _page in range(25):
                url = "http://%s/api/tasks?limit=%d&offset=%d" % (self.dashboard_addr, limit, offset)
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with opener.open(req, timeout=10.0) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                if not isinstance(payload, list) or not payload:
                    break
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    tid = str(item.get("task_id") or "")
                    if not tid:
                        continue
                    seen_in_pass += 1
                    prev = self._master_task_stats.get(tid, {})
                    self._master_task_stats[tid] = {
                        "retry_count": max(int(prev.get("retry_count", 0)), int(item.get("retry_count", 0) or 0)),
                        "failover_count": max(int(prev.get("failover_count", 0)), int(item.get("failover_count", 0) or 0)),
                        "evacuation_count": max(int(prev.get("evacuation_count", 0)), int(item.get("evacuation_count", 0) or 0)),
                    }
                if len(payload) < limit:
                    break
                offset += len(payload)
            self._api_snapshots_taken += 1
        except Exception as exc:
            self._api_snapshot_errors.append(redact_text(str(exc), [self.workdir]))
        return seen_in_pass

    def restart_master(self) -> Dict[str, Any]:
        with self._sync_cond:
            self._master_restarting.set()
            wait_deadline = time.monotonic() + 60.0
            while self._active_sync_submits > 0 and time.monotonic() < wait_deadline:
                self._sync_cond.wait(timeout=0.2)
        try:
            self.snapshot_master_task_retries()
            comp = self.stop_master("scheduled_restart")
            with self._lifecycle_lock:
                self._start_master_locked()
                for prx in self.proxies:
                    prx.retarget(self.master_host, self.master_port)
                expected = [i for i in range(self.n_workers) if i not in self._worker_down]
            self._wait_workers_connected(expected, timeout=60.0)
            return comp or {}
        finally:
            with self._sync_cond:
                self._master_restarting.clear()
                self._sync_cond.notify_all()

    def run_client(
        self,
        args: List[str],
        timeout: float = 120.0,
        master_addr: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        addr = master_addr or self.current_master_addr()
        cmd = [self.binary] + args[:1] + ["--master", addr] + args[1:]
        kw: Dict[str, Any] = {} if IS_WINDOWS else {"start_new_session": True}
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=self.client_env,
            **kw,
        )
        self._record_pid_start(p.pid)
        with self._tracked_clients_lock:
            self._tracked_clients.add(p)
        try:
            stdout, stderr = p.communicate(timeout=timeout)
            return subprocess.CompletedProcess(cmd, p.returncode, stdout or "", stderr or "")
        except subprocess.TimeoutExpired:
            p.kill()
            stdout, stderr = p.communicate()
            return subprocess.CompletedProcess(
                cmd,
                124,
                stdout or "",
                (stderr or "") + "\n[harness] client timed out after %.1f s" % timeout,
            )
        finally:
            if p.poll() is not None and not pid_alive(p.pid):
                self._record_pid_exit(p.pid)
            with self._tracked_clients_lock:
                self._tracked_clients.discard(p)

    def spawn_submit(
        self,
        submit_args: List[str],
        master_addr: Optional[str] = None,
    ) -> subprocess.Popen:
        addr = master_addr or self.current_master_addr()
        return super().spawn_submit(submit_args, master_addr=addr)

    def dir_map(self) -> Dict[str, str]:
        assert self.state_dir is not None
        dirs: Dict[str, str] = {
            "blob_store_bytes": self.blob_dir,
            "state_dir_bytes": self.state_dir,
            "journal_bin_bytes": os.path.join(self.state_dir, "journal.bin"),
            "snapshot_bin_bytes": os.path.join(self.state_dir, "snapshot.bin"),
        }
        for i, sd in enumerate(self.sandbox_dirs):
            dirs["sandbox_w%d_bytes" % i] = sd
            dirs["worker_cache_w%d_bytes" % i] = os.path.join(sd, "cache", "blobs")
        return dirs

    def idle_disk_snapshot(self) -> Dict[str, Any]:
        sandbox_entries = {
            "w%d" % i: sorted(os.listdir(sd))
            for i, sd in enumerate(self.sandbox_dirs)
            if os.path.isdir(sd)
        }
        tmp_leftovers: Dict[str, List[str]] = {
            "master_blobs_tmp": list_tmp_entries(self.blob_dir),
        }
        worker_caches: Dict[str, int] = {}
        for i, sd in enumerate(self.sandbox_dirs):
            cache_root = os.path.join(sd, "cache", "blobs")
            worker_caches["worker_cache_w%d_bytes" % i] = dir_bytes(cache_root)
            tmp_leftovers["w%d_cache_tmp" % i] = list_tmp_entries(cache_root)
            locks_dir = os.path.join(sd, ".locks")
            if os.path.isdir(locks_dir):
                tmp_leftovers["w%d_locks" % i] = sorted(os.listdir(locks_dir))
        return {
            "idle_blob_store_bytes": dir_bytes(self.blob_dir),
            "idle_worker_cache_bytes": worker_caches,
            "idle_sandbox_entries": sandbox_entries,
            "tmp_leftovers": tmp_leftovers,
        }

    def audit_master_logs(self, expected_task_ids: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        return audit_master_log_texts(
            (self.read_log("master-%d" % gen) for gen in range(self._master_gen)),
            expected_task_ids=expected_task_ids,
        )

    def leftover_process_trees(self) -> List[Dict[str, Any]]:
        table = _list_system_processes()
        pid_windows = self._snapshot_pid_windows()
        return find_leftover_process_trees(
            table,
            pid_windows.keys(),
            self.workdir,
            ignore_pids={os.getpid()},
            min_created_epoch=self._start_epoch_s,
            root_pid_windows=pid_windows,
        )

    def stop(self) -> None:
        with self._lifecycle_lock:
            for idx in sorted(list(self._worker_entries.keys())):
                p, log = self._worker_entries.pop(idx)
                pid = p.pid
                if p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=10.0)
                    except subprocess.TimeoutExpired:
                        p.kill()
                        p.wait(timeout=5.0)
                if pid_alive(pid):
                    self._unclean_pids.append(pid)
                else:
                    self._record_pid_exit(pid)
                try:
                    log.close()
                except Exception:
                    pass
        self.stop_master("final_stop")
        for prx in self.proxies:
            prx.stop()
        with self._tracked_clients_lock:
            for p in list(self._tracked_clients):
                if p.poll() is None:
                    p.kill()
                    p.wait(timeout=5.0)
                if pid_alive(p.pid):
                    self._unclean_pids.append(p.pid)
                else:
                    self._record_pid_exit(p.pid)
        self.procs = []


def resolve_master_arg(raw: Optional[str]) -> Optional[str]:
    """Resolve --master value, reading UTF-8 ticket file when prefixed with '@'."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.startswith("@"):
        path = os.path.expanduser(text[1:].strip())
        if not path or not os.path.isfile(path):
            raise ValueError("master ticket file not found: %s" % path)
        with open(path, encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            raise ValueError("master ticket file is empty: %s" % path)
        return content
    return text


def _parse_host_port(addr: str) -> Tuple[str, int]:
    s = (addr or "").strip()
    if not s or s.startswith("{") or ":" not in s:
        return "", 0
    if s.startswith("["):
        if "]:" not in s:
            return "", 0
        host, pstr = s[1:].rsplit("]:", 1)
    else:
        host, pstr = s.rsplit(":", 1)
    if not pstr.isdigit():
        return "", 0
    return host, int(pstr)


def _sum_tmp_bytes(blob_dir: Optional[str]) -> int:
    if not blob_dir:
        return 0
    tmp_dir = os.path.join(blob_dir, "tmp")
    if not os.path.isdir(tmp_dir):
        return 0
    total = 0
    try:
        with os.scandir(tmp_dir) as it:
            for entry in it:
                try:
                    total += entry.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
    except OSError:
        return 0
    return total


def _summarize_remote_worker(w: Dict[str, Any]) -> Dict[str, Any]:
    tags = [str(t) for t in w.get("tags") or []]
    keep_tags = [
        t for t in tags
        if t.startswith(("os:", "arch:", "iso:", "slots:", "proto:", "link:", "state:", "tool:"))
    ]
    return {
        "name": str(w.get("name") or ""),
        "cpu_cores": w.get("cpu_cores"),
        "ram_mb": w.get("ram_mb"),
        "has_gpu": bool(w.get("has_gpu")),
        "is_simulated_gpu": bool(w.get("is_simulated_gpu")),
        "link": str(w.get("link") or ""),
        "tags": keep_tags,
    }


class RemoteEnduranceCluster:
    """Adapter for TEST-24 two-machine runs against an already-running remote master and worker."""
    is_remote = True

    def __init__(
        self,
        binary: str,
        workdir: str,
        master_addr: str,
        *,
        secret_file: Optional[str] = None,
        insecure: bool = False,
        require_tags: Iterable[str] = (),
        target_worker: Optional[str] = None,
        master_pid: Optional[int] = None,
        master_blob_dir: Optional[str] = None,
        master_log: Optional[str] = None,
    ) -> None:
        self.binary = binary
        self.workdir = os.path.abspath(workdir)
        os.makedirs(self.workdir, exist_ok=True)
        self.master_addr = str(master_addr).strip()
        self.master_host, self.master_port = _parse_host_port(self.master_addr)
        self.secret_file = os.path.abspath(secret_file) if secret_file else None
        self.insecure = bool(insecure)
        self.require_tags = [str(t).strip() for t in require_tags if str(t).strip()]
        self.target_worker = str(target_worker).strip() if (target_worker and str(target_worker).strip()) else None
        self.master_pid = int(master_pid) if master_pid else None
        self.blob_dir = os.path.abspath(master_blob_dir) if master_blob_dir else None
        self.master_log = os.path.abspath(master_log) if master_log else None
        self.sandbox_dirs: List[str] = []
        self.empty_config = os.path.join(self.workdir, "empty.toml")
        with open(self.empty_config, "w", encoding="utf-8") as f:
            f.write("")
        self._extra_pids_lock = threading.Lock()
        self._extra_pids: Dict[str, int] = {}
        self.client_env = self._build_env(os.path.join(self.workdir, "home-client"))
        self._log_start_offset = 0
        self.remote_workers: List[Dict[str, Any]] = []
        self.forbidden_tokens = [
            tok for tok in (self.master_addr, self.secret_file, self.blob_dir, self.master_log) if tok
        ]

    def _build_env(self, home: str) -> Dict[str, str]:
        return _isolated_process_env(home, self.empty_config)

    def _auth_flags(self) -> List[str]:
        if self.secret_file:
            return ["--secret-file", self.secret_file]
        if self.insecure:
            return ["--insecure"]
        return []

    def build_cli_cmd(self, args: List[str], master_addr: Optional[str] = None) -> List[str]:
        subcmd = args[:1]
        rest = list(args[1:])
        addr = master_addr or self.master_addr
        extra = list(self._auth_flags())
        if subcmd == ["submit"]:
            if self.target_worker:
                extra.extend(["--worker", self.target_worker])
            for tag in self.require_tags:
                extra.extend(["--require-tag", tag])
        return [self.binary] + subcmd + ["--master", addr] + extra + rest

    def active_pids(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if self.master_pid and pid_alive(self.master_pid):
            out["master"] = self.master_pid
        with self._extra_pids_lock:
            out.update(self._extra_pids)
        return out

    def set_extra_pid(self, role: str, pid: Optional[int]) -> None:
        with self._extra_pids_lock:
            if pid is None:
                self._extra_pids.pop(role, None)
            else:
                self._extra_pids[role] = int(pid)

    def _matches_tags(self, worker: Dict[str, Any]) -> bool:
        wtags = set(str(t) for t in (worker.get("tags") or []))
        if any(t.startswith("state:") for t in wtags) and "state:connected" not in wtags:
            return False
        if self.target_worker:
            wname = str(worker.get("name") or "")
            wid = ""
            for t in wtags:
                if t.startswith("id:"):
                    wid = t[3:]
                    break
            if self.target_worker.lower() not in (wname.lower(), wid.lower()):
                return False
        if not self.require_tags:
            return True
        return all(t in wtags for t in self.require_tags)

    def start(self) -> "RemoteEnduranceCluster":
        if self.master_log and os.path.isfile(self.master_log):
            try:
                self._log_start_offset = os.path.getsize(self.master_log)
            except OSError:
                self._log_start_offset = 0
        deadline = time.monotonic() + 20.0
        last_err = ""
        while time.monotonic() < deadline:
            r = self.run_client(["workers", "--json"], timeout=15.0)
            if r.returncode == 0:
                try:
                    listed = json.loads(r.stdout)
                except ValueError as exc:
                    last_err = "invalid workers JSON: %s" % exc
                else:
                    matched = [w for w in listed if self._matches_tags(w)] if isinstance(listed, list) else []
                    if matched:
                        self.remote_workers = [_summarize_remote_worker(w) for w in matched]
                        return self
                    last_err = "no connected worker matching tags=%r (saw %d workers)" % (
                        self.require_tags,
                        len(listed) if isinstance(listed, list) else 0,
                    )
            else:
                last_err = redact_text((r.stderr or r.stdout or "").strip()[-300:], self.forbidden_tokens)
            time.sleep(0.5)
        raise RuntimeError("remote cluster readiness failed: %s" % (last_err or "timeout"))

    def dir_map(self) -> Dict[str, str]:
        if self.blob_dir:
            return {"blob_store_bytes": self.blob_dir}
        return {}

    def run_client(
        self,
        args: List[str],
        timeout: float = 120.0,
        master_addr: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        cmd = self.build_cli_cmd(args, master_addr=master_addr)
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
        cmd = self.build_cli_cmd(["submit"] + submit_args, master_addr=master_addr)
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
        if role != "master" or not self.master_log or not os.path.isfile(self.master_log):
            return ""
        with open(self.master_log, encoding="utf-8", errors="replace") as f:
            if self._log_start_offset > 0:
                try:
                    if os.path.getsize(self.master_log) >= self._log_start_offset:
                        f.seek(self._log_start_offset)
                except OSError:
                    pass
            return f.read()

    def stop(self) -> None:
        # Remote mode never stops the external master or remote worker; only remove client workdir.
        shutil.rmtree(self.workdir, ignore_errors=True)

    def leftover_pids(self) -> List[int]:
        alive = list(self._unclean_pids)
        with self._lifecycle_lock:
            if self._master_entry is not None and self._master_entry[1].poll() is None:
                alive.append(self._master_entry[1].pid)
            for p, _log in self._worker_entries.values():
                if p.poll() is None:
                    alive.append(p.pid)
        with self._tracked_clients_lock:
            for p in self._tracked_clients:
                if p.poll() is None:
                    alive.append(p.pid)
        return sorted(set(alive))


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


def _effective_seed(args: argparse.Namespace) -> int:
    link = (getattr(args, "link", None) or "loopback").strip().lower()
    return int(args.seed) + int(LINK_SEED_OFFSETS.get(link, 0))


def _make_transfer_cluster(
    args: argparse.Namespace,
    sc_dir: str,
    *,
    blob_store_mb: Optional[int] = None,
    worker_via_proxy: Optional[ThrottleProxy] = None,
) -> Any:
    resolved_master = resolve_master_arg(getattr(args, "master", None))
    if resolved_master:
        return RemoteEnduranceCluster(
            args.bin,
            os.path.join(sc_dir, "remote-client"),
            resolved_master,
            secret_file=getattr(args, "secret_file", None),
            insecure=bool(getattr(args, "insecure", False)),
            require_tags=getattr(args, "require_tag", None) or (),
            target_worker=getattr(args, "target_worker", None),
            master_pid=getattr(args, "master_pid", None),
            master_blob_dir=getattr(args, "master_blob_dir", None),
            master_log=getattr(args, "master_log", None),
        )
    return EnduranceCluster(
        args.bin,
        os.path.join(sc_dir, "cluster"),
        workers=1,
        blob_store_mb=blob_store_mb,
        worker_via_proxy=worker_via_proxy,
    )


def _cluster_forbidden(cluster: Any, *extra: Optional[str]) -> List[str]:
    out = [str(x) for x in extra if x]
    for tok in getattr(cluster, "forbidden_tokens", ()) or ():
        if tok:
            out.append(str(tok))
    return out


def run_t1(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sizes_gib = [float(x.strip()) for x in args.t1_sizes_gib.split(",") if x.strip()]
    base_seed = _effective_seed(args)
    entries = []
    for idx, gib_val in enumerate(sizes_gib):
        size_bytes = int(round(gib_val * GIB))
        sc_dir = os.path.join(work_root, "t1-%d" % idx)
        os.makedirs(sc_dir, exist_ok=True)
        payload = os.path.join(sc_dir, "payload.bin")
        store_mb = int(max(4096, round(gib_val * 1024) + 2048)) if gib_val > 3.0 else None
        meta = generate_seeded_file(payload, size_bytes, base_seed + 100 + idx)

        c = _make_transfer_cluster(args, sc_dir, blob_store_mb=store_mb)
        sampler: Optional[BackgroundSampler] = None
        try:
            c.start()
            sampler = BackgroundSampler(c.active_pids, c.dir_map(), interval_s=args.sample_interval).start()
            before_objs = list_blob_objects(c.blob_dir) if c.blob_dir else {}
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
            after_objs = list_blob_objects(c.blob_dir) if c.blob_dir else {}
            cache_objs = (
                list_blob_objects(os.path.join(c.sandbox_dirs[0], "cache", "blobs"))
                if c.sandbox_dirs
                else {}
            )
            parsed_json = extract_json_payload(stdout)
            task_stdout = decode_bytes_field((parsed_json or {}).get("stdout"))
            parsed_inputs = parse_inputs_line(stderr)
            matching_master_ids = [
                bid for bid, bsz in after_objs.items() if bsz == size_bytes and bid not in before_objs
            ]
            matching_cache_ids = [bid for bid, bsz in cache_objs.items() if bsz == size_bytes]
            verified = rc == 0 and ("VERIFY_OK %d %s" % (size_bytes, meta["sha256"])) in task_stdout
            master_blob_ok = bool(matching_master_ids) if c.blob_dir else True
            samples = sampler.stop()
            summary = BackgroundSampler.summarize(samples)
            entries.append(
                {
                    "size_gib": gib_val,
                    "size_bytes": size_bytes,
                    "blob_store_mb": None if c.is_remote else (store_mb or 4096),
                    "passed": bool(verified and master_blob_ok),
                    "wall_s": round(wall_s, 3),
                    "throughput_mib_per_s": round((size_bytes / MIB) / wall_s, 3) if wall_s > 0 else None,
                    "sha256_expected": meta["sha256"],
                    "worker_verified": verified,
                    "master_blake3_ids": matching_master_ids,
                    "worker_cache_blake3_ids": matching_cache_ids,
                    "input_upload": (parsed_json or {}).get("input_upload") or parsed_inputs,
                    "error": redact_text(
                        str((parsed_json or {}).get("error") or ("" if rc == 0 else stderr[-400:])),
                        _cluster_forbidden(c, sc_dir, work_root),
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
        roles = ("master", "client") if getattr(args, "master", None) else ("master", "worker0", "client")
        for role in roles:
            p_small = small["resource_summary"]["processes"].get(role, {}).get("peak_rss_bytes")
            p_large = large["resource_summary"]["processes"].get(role, {}).get("peak_rss_bytes")
            if getattr(args, "master", None) and p_small is None and p_large is None and role == "master":
                continue
            mem_comparisons[role] = check_t1_memory_flat(p_small, p_large)

    all_verified = all(e["passed"] for e in entries)
    mem_ok = (all(v["passed"] for v in mem_comparisons.values()) and bool(mem_comparisons)) if len(entries) >= 2 else True
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
    tree_meta = generate_seeded_tree(tree_dir, int(args.t2_files), _effective_seed(args) + 200)
    gen_s = time.perf_counter() - tgen0

    c = _make_transfer_cluster(args, sc_dir)
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
        before_objs = set(list_blob_objects(c.blob_dir)) if c.blob_dir else set()
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
        after_objs = set(list_blob_objects(c.blob_dir)) if c.blob_dir else set()
        master_blobs = len(after_objs) if c.blob_dir else None
        master_new_blobs = len(after_objs - before_objs) if c.blob_dir else None
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
            "master_new_blobs": master_new_blobs,
            "worker_verified": verified,
            "input_upload": (parsed_json or {}).get("input_upload") or parsed_inputs,
            "error": redact_text(
                str((parsed_json or {}).get("error") or ("" if rc == 0 else stderr[-400:])),
                _cluster_forbidden(c, sc_dir, work_root),
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

    base_seed = _effective_seed(args)
    pass_seed = base_seed + 301
    pass_sha = expected_seeded_sha256(pass_bytes, pass_seed)
    c = _make_transfer_cluster(args, sc_dir)
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
            ["out_fail.bin", str(fail_bytes), str(base_seed + 302), "-"],
        )
        rc_fail, stdout_fail, stderr_fail, wall_fail = c.run_submit_sampled(cmd_fail, timeout=1500.0)
        json_fail = extract_json_payload(stdout_fail)
        err_text = str((json_fail or {}).get("error") or "") + "\n" + stderr_fail + "\n" + stdout_fail
        step_fail_passed = (rc_fail != 0 or bool((json_fail or {}).get("error"))) and ("--output-limit-mb" in err_text)
        tmp_left = list_tmp_entries(c.blob_dir) if c.blob_dir else []
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
                "error": redact_text(
                    str((json_ok or {}).get("error") or ("" if rc_ok == 0 else stderr_ok[-400:])),
                    _cluster_forbidden(c, sc_dir, work_root),
                ),
            },
            "over_limit": {
                "size_mib": args.t3_fail_mib,
                "size_bytes": fail_bytes,
                "passed": bool(step_fail_passed),
                "rc": rc_fail,
                "wall_s": round(wall_fail, 3),
                "mentions_output_limit_mb": "--output-limit-mb" in err_text,
                "error": redact_text(err_text.strip()[:500], _cluster_forbidden(c, sc_dir, work_root)),
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
    meta = generate_seeded_file(payload, size_bytes, _effective_seed(args) + 400)

    c = _make_transfer_cluster(args, sc_dir)
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
        b0 = dir_bytes(c.blob_dir) if c.blob_dir else None
        rc1, out1, err1, w1 = c.run_submit_sampled(cmd_args, timeout=2100.0)
        b1 = dir_bytes(c.blob_dir) if c.blob_dir else None
        j1 = extract_json_payload(out1) or {}
        u1 = j1.get("input_upload") or parse_inputs_line(err1) or {}

        rc2, out2, err2, w2 = c.run_submit_sampled(cmd_args, timeout=1200.0)
        b2 = dir_bytes(c.blob_dir) if c.blob_dir else None
        j2 = extract_json_payload(out2) or {}
        u2 = j2.get("input_upload") or parse_inputs_line(err2) or {}

        g1 = (b1 - b0) if (b0 is not None and b1 is not None) else None
        g2 = (b2 - b1) if (b1 is not None and b2 is not None) else None
        zero_uploaded = (
            u2.get("uploaded") == 0
            or u2.get("blobs_sent") == 0
        ) and u2.get("bytes", 0) == 0 and (g2 in (None, 0))
        first_uploaded = u1.get("uploaded", 0) >= 1 or u1.get("blobs_sent", 0) >= 1 or u1.get("bytes", 0) > 0
        ok1 = rc1 == 0 and "VERIFY_OK" in decode_bytes_field(j1.get("stdout"))
        ok2 = rc2 == 0 and "VERIFY_OK" in decode_bytes_field(j2.get("stdout"))
        samples = sampler.stop()
        return {
            "id": "T4",
            "name": "Repeated 3 GiB submission uploads zero blob bytes",
            "passed": bool(ok1 and ok2 and first_uploaded and zero_uploaded),
            "size_gib": float(args.t4_size_gib),
            "size_bytes": size_bytes,
            "first": {
                "rc": rc1,
                "wall_s": round(w1, 3),
                "verified": ok1,
                "master_blob_store_growth_bytes": g1,
                "input_upload": u1,
            },
            "second": {
                "rc": rc2,
                "wall_s": round(w2, 3),
                "verified": ok2,
                "master_blob_store_growth_bytes": g2,
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
    """Client upload cut at roughly half via TCP proxy or mid-upload kill, then resubmitted."""
    sc_dir = os.path.join(work_root, "t5")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(args.t5_size_mib) * MIB
    payload = os.path.join(sc_dir, "payload.bin")
    meta = generate_seeded_file(payload, size_bytes, _effective_seed(args) + 500)

    c = _make_transfer_cluster(args, sc_dir)
    proxy: Optional[ThrottleProxy] = None
    sampler: Optional[BackgroundSampler] = None
    try:
        c.start()
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
        half = size_bytes // 2
        cut_at_bytes = 0
        cut_done = False
        use_tcp_proxy = bool(c.master_host and c.master_port > 0) and not getattr(args, "skip_t5_proxy", False)
        cut_method = "tcp_proxy" if use_tcp_proxy else "client_sigkill"

        if use_tcp_proxy:
            proxy = ThrottleProxy(c.master_host, c.master_port, rate_mbps=args.t5_rate_mbps).start()
            proc = c.spawn_submit(cmd_args, master_addr=proxy.bound_addr)
            c.set_extra_pid("client", proc.pid)
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
        else:
            if not c.blob_dir:
                raise RuntimeError("remote QUIC/relay T5 requires --master-blob-dir to detect half-upload")
            proc = c.spawn_submit(cmd_args)
            c.set_extra_pid("client", proc.pid)
            deadline = time.monotonic() + 300.0
            max_tmp = 0
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    break
                cur_tmp = _sum_tmp_bytes(c.blob_dir)
                if cur_tmp > max_tmp:
                    max_tmp = cur_tmp
                if cur_tmp >= half:
                    cut_at_bytes = cur_tmp
                    proc.kill()
                    cut_done = True
                    break
                time.sleep(0.01)
            out1, err1 = proc.communicate(timeout=30.0)
            rc1 = proc.returncode if proc.returncode is not None else -9
            c.set_extra_pid("client", None)
            # Wait briefly for the master to finish cleaning the dropped QUIC upload stream tmp file.
            clean_deadline = time.monotonic() + 10.0
            while time.monotonic() < clean_deadline and list_tmp_entries(c.blob_dir):
                time.sleep(0.1)

        # Re-submit against direct master address on clean connection
        rc2, out2, err2, w2 = c.run_submit_sampled(cmd_args, timeout=600.0)
        j2 = extract_json_payload(out2) or {}
        u2 = j2.get("input_upload") or parse_inputs_line(err2) or {}
        ok2 = rc2 == 0 and "VERIFY_OK" in decode_bytes_field(j2.get("stdout"))
        tmp_left = list_tmp_entries(c.blob_dir) if c.blob_dir else []
        samples = sampler.stop()
        return {
            "id": "T5",
            "name": "Client upload cut at half then resubmitted",
            "passed": bool(cut_done and ok2 and not tmp_left),
            "cut_method": cut_method,
            "size_bytes": size_bytes,
            "cut_done": cut_done,
            "bytes_c2s_before_cut": cut_at_bytes,
            "first_attempt_rc": rc1,
            "first_attempt_stderr": redact_text(err1[-300:], _cluster_forbidden(c, sc_dir, work_root)),
            "resubmit_rc": rc2,
            "resubmit_wall_s": round(w2, 3),
            "bytes_sent_on_resubmit": u2.get("bytes"),
            "input_upload": u2,
            "master_tmp_leftovers": tmp_left,
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
    if getattr(args, "master", None):
        return {
            "id": "T6",
            "name": "Worker link cut during input fetch, recovery, clean cache tmp",
            "passed": False,
            "skipped": True,
            "reason": "Remote T6 requires starting the target worker behind an external cuttable proxy; test separately over TCP proxy or loopback.",
            "samples": [],
        }
    sc_dir = os.path.join(work_root, "t6")
    os.makedirs(sc_dir, exist_ok=True)
    size_bytes = int(args.t6_size_mib) * MIB
    payload = os.path.join(sc_dir, "payload.bin")
    meta = generate_seeded_file(payload, size_bytes, _effective_seed(args) + 600)

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
    base_seed = _effective_seed(args)
    in_meta = generate_seeded_file(in_file, size_bytes, base_seed + 700)

    out_seed = base_seed + 701
    expected_out_sha = expected_seeded_sha256(size_bytes, out_seed)

    is_remote = bool(getattr(args, "master", None))
    proxy = None if is_remote else ThrottleProxy("127.0.0.1", 0, rate_mbps=args.t7_rate_mbps, delay_ms=args.t7_delay_ms)
    c = _make_transfer_cluster(args, sc_dir, worker_via_proxy=proxy)
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
            "throttle_mode": "external_worker_proxy" if is_remote else "embedded_loopback_proxy",
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
            "reap_log_lines": [redact_text(x, _cluster_forbidden(c, sc_dir, work_root)) for x in reaped_lines[:10]],
            "proxy_bytes_c2s": proxy.bytes_c2s if proxy is not None else None,
            "proxy_bytes_s2c": proxy.bytes_s2c if proxy is not None else None,
            "outputs": outputs,
            "error": redact_text(
                str(parsed_json.get("error") or ("" if rc == 0 else stderr[-600:])),
                _cluster_forbidden(c, sc_dir, work_root),
            ),
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


def run_t8(args: argparse.Namespace, work_root: str) -> Dict[str, Any]:
    sc_dir = os.path.join(work_root, "t8")
    os.makedirs(sc_dir, exist_ok=True)
    store_mb = int(args.t8_store_mb)
    too_big_bytes = (store_mb + 8) * MIB
    half_plus_bytes = int(store_mb * 0.625) * MIB  # 2 * 0.625 = 1.25x > 1.0x store

    f_big = os.path.join(sc_dir, "too_big.bin")
    f_a = os.path.join(sc_dir, "part_a.bin")
    f_b = os.path.join(sc_dir, "part_b.bin")
    base_seed = _effective_seed(args)
    generate_seeded_file(f_big, too_big_bytes, base_seed + 801)
    generate_seeded_file(f_a, half_plus_bytes, base_seed + 802)
    generate_seeded_file(f_b, half_plus_bytes, base_seed + 803)

    c = _make_transfer_cluster(args, sc_dir, blob_store_mb=store_mb)
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
        tmp_after_a = list_tmp_entries(c.blob_dir) if c.blob_dir else []
        passed_a = rc_a != 0 and "--blob-store-mb" in msg_a and not tmp_after_a

        # 8b: two concurrent uploads that together exceed store_mb
        use_proxy = bool(c.master_host and c.master_port > 0)
        if use_proxy:
            proxy = ThrottleProxy(c.master_host, c.master_port, rate_mbps=4.0, delay_ms=0.0).start()
            proc1 = c.spawn_submit(
                ["--input", f_a + ":a.bin", "--timeout-secs", "300", "--wait"]
                + ["--command", args.worker_python, "--", "-c", "print('a')"],
                master_addr=proxy.bound_addr,
            )
        else:
            proc1 = c.spawn_submit(
                ["--input", f_a + ":a.bin", "--timeout-secs", "300", "--wait"]
                + ["--command", args.worker_python, "--", "-c", "print('a')"],
            )
        c.set_extra_pid("client", proc1.pid)
        saw_tmp = False
        if c.blob_dir:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if proc1.poll() is not None:
                    break
                if list_tmp_entries(c.blob_dir):
                    saw_tmp = True
                    break
                time.sleep(0.025)
        else:
            time.sleep(0.2)
            saw_tmp = proc1.poll() is None

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
        if proxy is not None:
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
        tmp_after_b = list_tmp_entries(c.blob_dir) if c.blob_dir else []
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
                "error": redact_text(msg_a[:400], _cluster_forbidden(c, sc_dir, work_root)),
            },
            "concurrent_over_limit": {
                "each_size_bytes": half_plus_bytes,
                "passed": bool(passed_b),
                "saw_first_upload_in_tmp": saw_tmp,
                "first_upload_rc": rc_p1,
                "second_upload_rc": rc_b,
                "mentions_blob_store_mb": "--blob-store-mb" in msg_b,
                "tmp_leftovers": tmp_after_b,
                "error": redact_text(msg_b[:400], _cluster_forbidden(c, sc_dir, work_root)),
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
    meta = generate_seeded_file(payload, size_bytes, _effective_seed(args) + 900)

    c = _make_transfer_cluster(args, sc_dir)
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
            cache_bytes = dir_bytes(os.path.join(c.sandbox_dirs[0], "cache", "blobs")) if c.sandbox_dirs else None
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
        peak_worker_disk = (
            summary["dirs"].get("sandbox_base_bytes", {}).get("peak_bytes", 0) if c.sandbox_dirs else None
        )
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


_WORKER_SOAK_SLEEP_SRC = """
import sys, time
print("SOAK_STARTED", flush=True)
time.sleep(float(sys.argv[2]))
print("SOAK_DONE", flush=True)
""".strip()

_WORKER_SOAK_WS_SRC = """
import hashlib, os, sys
target = sys.argv[2]
if not os.path.isfile(target):
    print("WS_MISSING", target)
    sys.exit(3)
h = hashlib.sha256()
with open(target, "rb") as f:
    h.update(f.read())
print("WS_OK " + h.hexdigest())
""".strip()


# ---------------------------------------------------------------- CLI subcommands


def _prepare_work_root(work_dir: Optional[str], largest_bytes: int, keep_workdir: bool = False) -> Tuple[str, Dict[str, Any]]:
    if work_dir:
        os.makedirs(work_dir, exist_ok=True)
        work_root = tempfile.mkdtemp(prefix="oxendurance-", dir=os.path.abspath(work_dir))
    else:
        work_root = tempfile.mkdtemp(prefix="oxendurance-")
    try:
        disk_check = check_free_space(work_root, largest_bytes)
    except Exception:
        if not keep_workdir:
            shutil.rmtree(work_root, ignore_errors=True)
        raise
    return work_root, disk_check


def _collect_env_metadata(
    binary: str,
    rtt_ms: Optional[float],
    link: str,
    resolved_master: Optional[str] = None,
) -> Dict[str, Any]:
    started = datetime.datetime.now(datetime.timezone.utc)
    hw = hardware()
    os_facts = os_info()
    git = git_info()
    version = sh([binary, "--version"])
    av_realtime = windows_realtime_av_enabled()

    loopback_rtt_ms = rtt_ms
    if loopback_rtt_ms is None and link == "loopback":
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
    elif loopback_rtt_ms is None and link == "lan-tcp" and resolved_master:
        mhost, mport = _parse_host_port(resolved_master)
        if mhost and mport > 0:
            loopback_rtt_ms = measure_tcp_rtt_ms(mhost, mport)

    return {
        "started": started,
        "hw": hw,
        "os_facts": os_facts,
        "git": git,
        "version": version,
        "av_realtime": av_realtime,
        "loopback_rtt_ms": loopback_rtt_ms,
    }


def _save_or_print_record(
    record: Dict[str, Any],
    *,
    out_path: Optional[str],
    default_link: str,
    started: datetime.datetime,
    os_facts: Dict[str, str],
    hw: Dict[str, Any],
    no_write: bool,
    work_root: str,
    extra_forbidden: Iterable[str] = (),
) -> bool:
    forbidden = [work_root] + [str(x) for x in extra_forbidden if x]
    quarantined = False
    if no_write:
        text = json.dumps(record, indent=2) + "\n"
        assert_record_private(text, forbidden=forbidden)
        print(text)
    else:
        target = out_path or result_path(os.path.join(HERE, "results"), started, default_link, os_facts, hw)
        write_res = write_record(target, record, forbidden=forbidden)
        quarantined = bool(write_res["quarantined"])
        if quarantined:
            print("quarantined", write_res["path"], flush=True)
        else:
            print("wrote", write_res["path"], flush=True)
    return quarantined


def validate_transfer_args(args: argparse.Namespace) -> None:
    """Validate transfer --link and remote master/auth flags (TEST-24 steps 1 & 2)."""
    link = (getattr(args, "link", None) or "").strip().lower()
    if link not in TRANSFER_LINKS:
        raise ValueError(
            "unsupported --link %r (expected one of: %s)"
            % (getattr(args, "link", None), ", ".join(TRANSFER_LINKS))
        )
    has_master = bool((getattr(args, "master", None) or "").strip())
    has_secret = bool((getattr(args, "secret_file", None) or "").strip())
    has_insecure = bool(getattr(args, "insecure", False))

    if link == "loopback":
        bad_flags: List[str] = []
        if has_master:
            bad_flags.append("--master")
        if has_secret:
            bad_flags.append("--secret-file")
        if has_insecure:
            bad_flags.append("--insecure")
        if bad_flags:
            raise ValueError(
                "%s require two-machine mode (TEST-24 step 2) and cannot be combined with loopback step 1"
                % ", ".join(bad_flags)
            )
    else:
        if not has_master:
            raise ValueError("--link %s requires --master (<host:port>, ticket, or @ticket_file)" % link)
        if not (has_secret or has_insecure):
            raise ValueError("--link %s requires either --secret-file <path> or --insecure" % link)


def validate_soak_args(args: argparse.Namespace) -> Dict[str, Any]:
    """Validate and parse TEST-25 step 1 soak CLI parameters."""
    link = (getattr(args, "link", None) or "").strip().lower()
    if link != "loopback":
        raise ValueError(
            "only --link loopback is supported in TEST-25 step 1 (got --link %r; multi-machine soak runs in step 2)"
            % getattr(args, "link", None)
        )
    duration_s = parse_duration_seconds(args.duration)
    sample_interval_s = parse_duration_seconds(args.sample_interval)
    fault_scale = float(args.fault_scale)
    load_scale = float(args.load_scale)
    workers = int(args.workers)
    slots = int(args.slots)
    max_queue_size = int(args.max_queue_size)
    blob_store_mb = int(args.blob_store_mb)
    blob_cache_mb = int(args.blob_cache_mb)
    if fault_scale <= 0.0:
        raise ValueError("--fault-scale must be positive (got %r)" % args.fault_scale)
    if load_scale <= 0.0:
        raise ValueError("--load-scale must be positive (got %r)" % args.load_scale)
    if workers < 2 or workers > 3:
        raise ValueError("--workers must be 2 or 3 per design 0065 D3 (got %d)" % workers)
    if slots < 1:
        raise ValueError("--slots must be >= 1 (got %d)" % slots)
    if max_queue_size < 1:
        raise ValueError("--max-queue-size must be >= 1 (got %d)" % max_queue_size)
    if blob_store_mb < 64:
        raise ValueError("--blob-store-mb must be >= 64 (got %d)" % blob_store_mb)
    if blob_cache_mb < 32:
        raise ValueError("--blob-cache-mb must be >= 32 (got %d)" % blob_cache_mb)
    return {
        "duration_s": duration_s,
        "sample_interval_s": sample_interval_s,
        "fault_scale": fault_scale,
        "load_scale": load_scale,
        "workers": workers,
        "slots": slots,
        "max_queue_size": max_queue_size,
        "blob_store_mb": blob_store_mb,
        "blob_cache_mb": blob_cache_mb,
    }


def _classify_soak_terminal(rc: int, parsed_json: Optional[Dict[str, Any]]) -> Optional[str]:
    if not parsed_json:
        return None
    if "exit_code" in parsed_json:
        code = int(parsed_json["exit_code"])
        err = str(parsed_json.get("error") or "").lower()
        if code == 0:
            return "ok"
        if code == 130 or "cancel" in err:
            return "cancelled"
        return "failed"
    if "status" in parsed_json:
        st = str(parsed_json["status"]).lower()
        if st in ("completed", "ok"):
            return "ok"
        if st == "cancelled":
            return "cancelled"
        if st == "failed":
            return "failed"
    if "error" in parsed_json:
        err = str(parsed_json["error"] or "").lower()
        if "not found" in err:
            return "not_found"
        if "cancel" in err:
            return "cancelled"
    return None


def _wait_for_soak_result(
    cluster: SoakCluster,
    tid: str,
    timeout_s: float = 420.0,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    deadline = time.monotonic() + timeout_s
    last_json: Optional[Dict[str, Any]] = None
    while time.monotonic() < deadline:
        cluster.wait_if_restarting(timeout=min(45.0, max(1.0, deadline - time.monotonic())))
        rem = deadline - time.monotonic()
        if rem <= 0:
            break
        res = cluster.run_client(
            ["result", tid, "--wait", "--json"],
            timeout=max(5.0, min(45.0, rem)),
        )
        parsed = extract_json_payload(res.stdout)
        if parsed:
            last_json = parsed
        outcome = _classify_soak_terminal(res.returncode, parsed)
        if outcome is not None:
            return outcome, parsed
        time.sleep(0.35)
    return "timeout", last_json


def _run_async_soak_task(
    cluster: SoakCluster,
    ledger: TaskLedger,
    submit_args: List[str],
    *,
    kind: str,
    expected_hash: Optional[str] = None,
    hash_extractor: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
    cleanup_paths: Iterable[str] = (),
    work_root: str = "",
) -> str:
    tid = ""
    status_str = ""
    res: Optional[subprocess.CompletedProcess] = None
    parsed: Optional[Dict[str, Any]] = None
    max_attempts = 1 if kind == "queue_burst" else 3
    try:
        for attempt in range(max_attempts):
            cluster.enter_submit(timeout=60.0)
            try:
                res = cluster.run_client(["submit", "--json"] + submit_args, timeout=120.0)
            finally:
                cluster.exit_submit()
            parsed = extract_json_payload(res.stdout)
            tid = str((parsed or {}).get("task_id") or "")
            status_str = str((parsed or {}).get("status") or "").lower()
            if res.returncode == 0 and tid and status_str == "submitted":
                break
            err_field = str((parsed or {}).get("error") or "")
            if (
                kind != "queue_burst"
                and attempt + 1 < max_attempts
                and is_queue_full_error(err_field)
            ):
                time.sleep(2.5)
                continue
            break

        if res is None or res.returncode != 0 or not tid or status_str != "submitted":
            msg = (
                (parsed or {}).get("error")
                or (res.stderr if res else "")
                or (res.stdout if res else "")
                or ("exit %d" % (res.returncode if res else -1))
            )
            cleaned = redact_text(str(msg).strip(), [work_root])
            rc = res.returncode if res else None
            if classify_soak_submit_error(kind, cleaned, rc) == "refusal":
                ledger.record_submit_refusal(kind, cleaned)
                return "refusal"
            ledger.record_submit_failure(kind, cleaned, rc=rc)
            return "failure"

        ledger.record_submit(tid, expected_hash=expected_hash, expected_outcome="ok", kind=kind)
    finally:
        for p in cleanup_paths:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    outcome, term_json = _wait_for_soak_result(cluster, tid, timeout_s=480.0)
    got_hash = hash_extractor(term_json) if (hash_extractor and term_json and outcome == "ok") else None
    wid = str(term_json.get("worker_id")) if term_json and term_json.get("worker_id") else None
    ledger.record_result(tid, actual_hash=got_hash, outcome=outcome, worker_id=wid)
    return "completed"


def _run_cancel_soak_task(
    cluster: SoakCluster,
    ledger: TaskLedger,
    worker_python: str,
    sleep_s: float,
    work_root: str,
) -> None:
    cmd_args = [
        "--timeout-secs",
        "60",
    ] + _worker_python_args(worker_python, _WORKER_SOAK_SLEEP_SRC, [str(sleep_s)])
    res: Optional[subprocess.CompletedProcess] = None
    parsed: Optional[Dict[str, Any]] = None
    tid = ""
    status_str = ""
    for attempt in range(3):
        cluster.enter_submit(timeout=60.0)
        try:
            res = cluster.run_client(["submit", "--json"] + cmd_args, timeout=30.0)
        finally:
            cluster.exit_submit()
        parsed = extract_json_payload(res.stdout)
        tid = str((parsed or {}).get("task_id") or "")
        status_str = str((parsed or {}).get("status") or "").lower()
        if res.returncode == 0 and tid and status_str == "submitted":
            break
        err_field = str((parsed or {}).get("error") or "")
        if attempt + 1 < 3 and is_queue_full_error(err_field):
            time.sleep(2.5)
            continue
        break

    if res is None or res.returncode != 0 or not tid or status_str != "submitted":
        msg = (
            (parsed or {}).get("error")
            or (res.stderr if res else "")
            or (res.stdout if res else "")
            or ("exit %d" % (res.returncode if res else -1))
        )
        ledger.record_submit_failure("cancel", redact_text(str(msg).strip(), [work_root]), rc=(res.returncode if res else None))
        return

    ledger.record_submit(
        tid,
        expected_outcome="cancelled",
        kind="cancel",
        allowed_outcomes=("cancelled",),
    )

    # Wait until task is running on a worker so CancelTask reaches the worker and produces a worker result
    wait_deadline = time.monotonic() + 60.0
    while time.monotonic() < wait_deadline:
        cluster.wait_if_restarting(timeout=30.0)
        st_res = cluster.run_client(["status", "--task-id", tid, "--json"], timeout=15.0)
        st_json = extract_json_payload(st_res.stdout)
        st_val = str((st_json or {}).get("state") or (st_json or {}).get("status") or "").lower()
        if "running" in st_val or "cancel" in st_val or "completed" in st_val or "failed" in st_val:
            break
        time.sleep(0.2)

    for _ in range(3):
        cluster.wait_if_restarting(timeout=30.0)
        c_res = cluster.run_client(["cancel", tid, "--json"], timeout=15.0)
        c_json = extract_json_payload(c_res.stdout)
        if c_res.returncode == 0 and (c_json or {}).get("cancelled"):
            break
        time.sleep(0.3)

    outcome, term_json = _wait_for_soak_result(cluster, tid, timeout_s=60.0)
    wid = str(term_json.get("worker_id")) if term_json and term_json.get("worker_id") else None
    ledger.record_result(tid, outcome=outcome, worker_id=wid)


def _run_output_soak_task(
    cluster: SoakCluster,
    ledger: TaskLedger,
    worker_python: str,
    size_bytes: int,
    seed: int,
    out_dir: str,
    work_root: str,
) -> None:
    os.makedirs(out_dir, exist_ok=True)
    exp_sha = expected_seeded_sha256(size_bytes, seed)
    cmd_args = [
        "submit",
        "--output",
        "out.bin",
        "--output-dir",
        out_dir,
        "--timeout-secs",
        "600",
        "--wait",
        "--json",
    ] + _worker_python_args(
        worker_python,
        _WORKER_GEN_OUTPUT_SRC,
        ["out.bin", str(size_bytes), str(seed), "-"],
    )
    res: Optional[subprocess.CompletedProcess] = None
    parsed: Optional[Dict[str, Any]] = None
    tid = ""
    for attempt in range(3):
        cluster.enter_submit(timeout=60.0)
        try:
            res = cluster.run_client(cmd_args, timeout=600.0)
        finally:
            cluster.exit_submit()
        parsed = extract_json_payload(res.stdout)
        tid = str((parsed or {}).get("task_id") or "")
        if tid:
            break
        err_field = str((parsed or {}).get("error") or "")
        if attempt + 1 < 3 and is_queue_full_error(err_field):
            time.sleep(2.5)
            continue
        break

    if res is None or not tid:
        msg = (
            (parsed or {}).get("error")
            or (res.stderr if res else "")
            or (res.stdout if res else "")
            or ("exit %d" % (res.returncode if res else -1))
        )
        ledger.record_submit_failure("output", redact_text(str(msg).strip(), [work_root]), rc=(res.returncode if res else None))
        shutil.rmtree(out_dir, ignore_errors=True)
        return

    ledger.record_submit(tid, expected_hash=exp_sha, expected_outcome="ok", kind="output")
    dl_path = os.path.join(out_dir, "out.bin")
    outputs_info = _extract_outputs_info(parsed)
    got_sha: Optional[str] = None
    if (
        res.returncode == 0
        and os.path.isfile(dl_path)
        and outputs_info
        and HEX64_RE.match(str(outputs_info[0].get("id") or ""))
    ):
        got_sz, sha = sha256_file(dl_path)
        if got_sz == size_bytes:
            got_sha = sha
    outcome = "ok" if res.returncode == 0 else (_classify_soak_terminal(res.returncode, parsed) or "failed")
    wid = str(parsed.get("worker_id")) if parsed and parsed.get("worker_id") else None
    ledger.record_result(tid, actual_hash=got_sha, outcome=outcome, worker_id=wid)
    shutil.rmtree(out_dir, ignore_errors=True)


def run_soak(args: argparse.Namespace, work_root: str, parsed_params: Dict[str, Any]) -> Dict[str, Any]:
    duration_s = float(parsed_params["duration_s"])
    sample_interval_s = float(parsed_params["sample_interval_s"])
    fault_scale = float(parsed_params["fault_scale"])
    load_scale = float(parsed_params["load_scale"])
    n_workers = int(parsed_params["workers"])
    slots = int(parsed_params["slots"])
    max_queue_size = int(parsed_params["max_queue_size"])
    blob_store_mb = int(parsed_params["blob_store_mb"])
    blob_cache_mb = int(parsed_params["blob_cache_mb"])
    workspaces = bool(args.workspaces)

    sc_dir = os.path.join(work_root, "soak")
    inputs_dir = os.path.join(sc_dir, "inputs")
    reused_dir = os.path.join(sc_dir, "reused")
    outputs_dir = os.path.join(sc_dir, "outputs")
    ws_src_dir = os.path.join(sc_dir, "ws-src")
    for d in (inputs_dir, reused_dir, outputs_dir, ws_src_dir):
        os.makedirs(d, exist_ok=True)

    # Create 5-file rotating reused-input pool + static workspace files
    reused_pool: List[Dict[str, Any]] = []
    reused_sz = max(1 * MIB, int(round(16 * MIB * min(1.0, load_scale))))
    for ridx in range(5):
        rp = os.path.join(reused_dir, "reused-%d.bin" % ridx)
        meta = generate_seeded_file(rp, reused_sz, int(args.seed) + 700 + ridx)
        meta["path"] = rp
        reused_pool.append(meta)

    with open(os.path.join(ws_src_dir, "README.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write("OxideSwarm TEST-25 workspace soak fixture\n")

    fault_plan = plan_soak_faults(duration_s, fault_scale=fault_scale, n_workers=n_workers)
    fault_events_log: List[Dict[str, Any]] = []
    fault_errors: List[Dict[str, Any]] = []
    fault_lock = threading.Lock()
    burst_restart_lock = threading.Lock()
    probe_samples: List[Dict[str, Any]] = []
    probe_lock = threading.Lock()
    ledger = TaskLedger()
    ws_lock = threading.Lock()
    verify_re = re.compile(r"VERIFY_OK\s+\d+\s+([0-9a-f]{64})")
    ws_re = re.compile(r"WS_OK\s+([0-9a-f]{64})")

    cluster = SoakCluster(
        args.bin,
        os.path.join(sc_dir, "cluster"),
        workers=n_workers,
        slots=slots,
        blob_store_mb=blob_store_mb,
        blob_cache_mb=blob_cache_mb,
        max_queue_size=max_queue_size,
        workspaces=workspaces,
        session_resume_hold_secs=args.session_resume_hold_secs,
        rate_mbps=args.rate_mbps,
        delay_ms=args.delay_ms,
    )

    sampler: Optional[BackgroundSampler] = None
    probe_stop = threading.Event()
    probe_thread: Optional[threading.Thread] = None
    fault_threads: List[Tuple[Dict[str, Any], threading.Thread]] = []
    soak_t0 = time.monotonic()

    def _run_probe_once() -> None:
        max_attempts = 3
        res: Optional[subprocess.CompletedProcess] = None
        dt_ms = 0.0
        t_now = round(time.monotonic() - soak_t0, 2)
        parsed: Optional[Dict[str, Any]] = None
        for attempt in range(max_attempts):
            if cluster._master_restarting.is_set():
                return
            cluster.enter_submit(timeout=60.0)
            t_now = round(time.monotonic() - soak_t0, 2)
            t_sub = time.perf_counter()
            try:
                res = cluster.run_client(
                    ["submit", "--wait", "--json", "--timeout-secs", "60", "--command", args.worker_python, "--", "-c", "print('PROBE_OK')"],
                    timeout=90.0,
                )
                dt_ms = round((time.perf_counter() - t_sub) * 1000.0, 3)
            finally:
                cluster.exit_submit()

            parsed = extract_json_payload(res.stdout)
            if res.returncode == 0:
                break
            err_msg = str((parsed or {}).get("error") or res.stderr or res.stdout or "")
            if is_queue_full_error(err_msg) and attempt + 1 < max_attempts:
                time.sleep(2.5)
                continue
            break

        if res is None:
            return
        txt = decode_bytes_field((parsed or {}).get("stdout"))
        ok = res.returncode == 0 and "PROBE_OK" in txt
        tid = str((parsed or {}).get("task_id") or "")
        if tid:
            ledger.record_submit(tid, expected_outcome="ok", kind="probe")
            ledger.record_result(tid, outcome="ok" if ok else "failed")
        else:
            msg = (parsed or {}).get("error") or res.stderr or res.stdout or ("exit %d" % res.returncode)
            ledger.record_submit_failure("probe", redact_text(str(msg).strip(), [work_root]), rc=res.returncode)
        with probe_lock:
            probe_samples.append({"t_s": t_now, "latency_ms": dt_ms, "ok": bool(ok), "rc": res.returncode})

    def _probe_loop() -> None:
        _run_probe_once()
        while not probe_stop.wait(sample_interval_s):
            _run_probe_once()

    def _extract_verify_sha(term_json: Dict[str, Any]) -> Optional[str]:
        txt = decode_bytes_field(term_json.get("stdout"))
        m = verify_re.search(txt)
        return m.group(1) if m else None

    def _extract_ws_sha(term_json: Dict[str, Any]) -> Optional[str]:
        txt = decode_bytes_field(term_json.get("stdout"))
        m = ws_re.search(txt)
        return m.group(1) if m else None

    def _dispatch_item(item: Dict[str, Any]) -> None:
        kind = item["kind"]
        seq_tag = "%d_%d" % (item["minute"], item["seq"])
        if kind == "noop":
            _run_async_soak_task(
                cluster,
                ledger,
                ["--timeout-secs", "60", "--command", args.worker_python, "--", "-c", "print('NOOP_OK')"],
                kind="noop",
                work_root=work_root,
            )
        elif kind == "new_input":
            p = os.path.join(inputs_dir, "new_%s.bin" % seq_tag)
            meta = generate_seeded_file(p, int(item["size_bytes"]), int(item["seed"]))
            submit_args = [
                "--input",
                p + ":payload.bin",
                "--timeout-secs",
                "300",
            ] + _worker_python_args(
                args.worker_python,
                _WORKER_VERIFY_FILE_SRC,
                ["payload.bin", str(meta["size_bytes"]), meta["sha256"]],
            )
            _run_async_soak_task(
                cluster,
                ledger,
                submit_args,
                kind="new_input",
                expected_hash=meta["sha256"],
                hash_extractor=_extract_verify_sha,
                cleanup_paths=(p,),
                work_root=work_root,
            )
        elif kind == "reused_input":
            meta = reused_pool[int(item["pool_index"]) % len(reused_pool)]
            submit_args = [
                "--input",
                meta["path"] + ":payload.bin",
                "--timeout-secs",
                "300",
            ] + _worker_python_args(
                args.worker_python,
                _WORKER_VERIFY_FILE_SRC,
                ["payload.bin", str(meta["size_bytes"]), meta["sha256"]],
            )
            _run_async_soak_task(
                cluster,
                ledger,
                submit_args,
                kind="reused_input",
                expected_hash=meta["sha256"],
                hash_extractor=_extract_verify_sha,
                work_root=work_root,
            )
        elif kind == "output":
            out_dir = os.path.join(outputs_dir, "out_%s" % seq_tag)
            _run_output_soak_task(
                cluster,
                ledger,
                args.worker_python,
                int(item["size_bytes"]),
                int(item["seed"]),
                out_dir,
                work_root,
            )
        elif kind == "workspace_sync":
            with ws_lock:
                marker_path = os.path.join(ws_src_dir, "sync_marker.txt")
                body_bytes = ("SOAK_WS|%d|%d|%d\n" % (item["minute"], item["seq"], item["seed"])).encode("utf-8")
                with open(marker_path, "wb") as mf:
                    mf.write(body_bytes)
                exp_sha = hashlib.sha256(body_bytes).hexdigest()
                submit_args = [
                    "--workspace",
                    "soak-ws",
                    "--sync",
                    ws_src_dir,
                    "--timeout-secs",
                    "180",
                ] + _worker_python_args(args.worker_python, _WORKER_SOAK_WS_SRC, ["sync_marker.txt"])
                res = None
                parsed = None
                tid = ""
                status_str = ""
                for attempt in range(3):
                    cluster.enter_submit(timeout=60.0)
                    try:
                        res = cluster.run_client(["submit", "--json"] + submit_args, timeout=120.0)
                    finally:
                        cluster.exit_submit()
                    parsed = extract_json_payload(res.stdout)
                    tid = str((parsed or {}).get("task_id") or "")
                    status_str = str((parsed or {}).get("status") or "").lower()
                    if res.returncode == 0 and tid and status_str == "submitted":
                        break
                    err_field = str((parsed or {}).get("error") or "")
                    if attempt + 1 < 3 and is_queue_full_error(err_field):
                        time.sleep(2.5)
                        continue
                    break
            if res is None or res.returncode != 0 or not tid or status_str != "submitted":
                msg = (
                    (parsed or {}).get("error")
                    or (res.stderr if res else "")
                    or (res.stdout if res else "")
                    or ("exit %d" % (res.returncode if res else -1))
                )
                ledger.record_submit_failure("workspace_sync", redact_text(str(msg).strip(), [work_root]), rc=(res.returncode if res else None))
                return
            ledger.record_submit(tid, expected_hash=exp_sha, expected_outcome="ok", kind="workspace_sync")
            outcome, term_json = _wait_for_soak_result(cluster, tid, timeout_s=300.0)
            got_sha = _extract_ws_sha(term_json) if (term_json and outcome == "ok") else None
            wid = str(term_json.get("worker_id")) if term_json and term_json.get("worker_id") else None
            ledger.record_result(tid, actual_hash=got_sha, outcome=outcome, worker_id=wid)
        elif kind == "cancel":
            _run_cancel_soak_task(cluster, ledger, args.worker_python, float(item["sleep_s"]), work_root)

    def _trigger_fault(ev: Dict[str, Any]) -> None:
        fkind = ev["kind"]
        seq = int(ev.get("seq", -1))
        widx = ev.get("worker_index")
        t_start = round(time.monotonic() - soak_t0, 2)
        try:
            if fkind == "worker_kill":
                wi = int(widx)
                old_pid = cluster.kill_worker(wi)
                if old_pid <= 0:
                    raise RuntimeError("worker_kill worker%d had no live PID" % wi)
                time.sleep(float(ev["down_s"]))
                new_pid = cluster.start_worker(wi)
                if new_pid <= 0:
                    raise RuntimeError("worker_kill worker%d failed to restart" % wi)
                t_end = round(time.monotonic() - soak_t0, 2)
                entry = {
                    "kind": "worker_kill",
                    "seq": seq,
                    "planned_at_s": ev.get("at_s"),
                    "t_s": t_start,
                    "end_s": t_end,
                    "worker_index": wi,
                    "old_pid": old_pid,
                    "new_pid": new_pid,
                    "down_s": ev["down_s"],
                    "ok": True,
                }
            elif fkind == "link_cut":
                wi = int(widx)
                cluster.cut_worker_link(wi, pause_s=float(ev["down_s"]))
                t_end = max(round(time.monotonic() - soak_t0, 2), round(t_start + float(ev["down_s"]), 2))
                entry = {
                    "kind": "link_cut",
                    "seq": seq,
                    "planned_at_s": ev.get("at_s"),
                    "t_s": t_start,
                    "end_s": t_end,
                    "worker_index": wi,
                    "pause_s": ev["down_s"],
                    "ok": True,
                }
            elif fkind == "master_restart":
                with burst_restart_lock:
                    t_start = round(time.monotonic() - soak_t0, 2)
                    comp = cluster.restart_master()
                    if not comp or int(comp.get("exit_code", -1)) != 0 or cluster.master_port <= 0:
                        raise RuntimeError("master_restart failed: %r" % (comp,))
                    t_end = round(time.monotonic() - soak_t0, 2)
                entry = {
                    "kind": "master_restart",
                    "seq": seq,
                    "planned_at_s": ev.get("at_s"),
                    "t_s": t_start,
                    "end_s": t_end,
                    "worker_index": None,
                    "compaction": comp,
                    "new_master_port": cluster.master_port,
                    "ok": True,
                }
            elif fkind == "queue_burst":
                total_burst = max_queue_size + 16
                b_args = [
                    "--timeout-secs",
                    "120",
                ] + _worker_python_args(args.worker_python, _WORKER_SOAK_SLEEP_SRC, ["5.0"])
                with burst_restart_lock:
                    t_start = round(time.monotonic() - soak_t0, 2)
                    with ThreadPoolExecutor(max_workers=24) as bpool:
                        futs = [
                            bpool.submit(
                                _run_async_soak_task,
                                cluster,
                                ledger,
                                b_args,
                                kind="queue_burst",
                                work_root=work_root,
                            )
                            for _ in range(total_burst)
                        ]
                        outcomes = [bf.result() for bf in futs]
                    t_end = round(time.monotonic() - soak_t0, 2)
                b_completed = sum(1 for o in outcomes if o == "completed")
                b_refused = sum(1 for o in outcomes if o == "refusal")
                b_failed = sum(1 for o in outcomes if o == "failure")
                if b_failed > 0:
                    raise RuntimeError("queue_burst recorded %d non-queue-full submit failures" % b_failed)
                entry = {
                    "kind": "queue_burst",
                    "seq": seq,
                    "planned_at_s": ev.get("at_s"),
                    "t_s": t_start,
                    "end_s": t_end,
                    "worker_index": None,
                    "burst_size": total_burst,
                    "max_queue_size": max_queue_size,
                    "accepted": b_completed,
                    "refused": b_refused,
                    "failed": b_failed,
                    "ok": True,
                }
            else:
                raise ValueError("unknown fault kind: %r" % (fkind,))
            with fault_lock:
                fault_events_log.append(entry)
        except Exception as exc:
            t_err = round(time.monotonic() - soak_t0, 2)
            err_entry = {
                "kind": fkind,
                "seq": seq,
                "planned_at_s": ev.get("at_s"),
                "t_s": t_start,
                "end_s": t_err,
                "worker_index": widx,
                "error": redact_text(str(exc), [work_root]),
            }
            with fault_lock:
                fault_errors.append(err_entry)

    samples: List[Dict[str, Any]] = []
    idle_snap: Dict[str, Any] = {}
    master_audit: Dict[str, Any] = {}
    leftover: List[int] = []
    leftover_trees: List[Dict[str, Any]] = []
    try:
        cluster.start()
        soak_t0 = time.monotonic()
        sampler = BackgroundSampler(cluster.active_pids, cluster.dir_map(), interval_s=sample_interval_s).start()
        probe_thread = threading.Thread(target=_probe_loop, name="soak-probe", daemon=True)
        probe_thread.start()

        end_deadline = soak_t0 + duration_s
        next_fault_idx = 0
        minute_idx = 0

        with ThreadPoolExecutor(max_workers=12) as pool:
            pending_futures = []
            while time.monotonic() < end_deadline:
                minute_start = time.monotonic()
                minute_items = plan_soak_minute(
                    minute_idx,
                    int(args.seed),
                    load_scale=load_scale,
                    workspaces=workspaces,
                )
                minute_window = min(60.0, max(1.0, end_deadline - minute_start))
                spacing = (minute_window * 0.85) / max(1, len(minute_items))

                for item in minute_items:
                    while time.monotonic() < end_deadline and next_fault_idx < len(fault_plan):
                        elapsed = time.monotonic() - soak_t0
                        if elapsed >= float(fault_plan[next_fault_idx]["at_s"]):
                            ev = fault_plan[next_fault_idx]
                            next_fault_idx += 1
                            ft = threading.Thread(target=_trigger_fault, args=(ev,), daemon=True)
                            ft.start()
                            fault_threads.append((ev, ft))
                        else:
                            break
                    if time.monotonic() >= end_deadline:
                        break
                    pending_futures.append(pool.submit(_dispatch_item, item))
                    pending_futures = [f for f in pending_futures if not f.done()]
                    if spacing > 0.05:
                        time.sleep(min(spacing, max(0.0, end_deadline - time.monotonic())))

                while time.monotonic() < min(end_deadline, minute_start + 60.0):
                    while next_fault_idx < len(fault_plan) and (time.monotonic() - soak_t0) >= float(fault_plan[next_fault_idx]["at_s"]):
                        ev = fault_plan[next_fault_idx]
                        next_fault_idx += 1
                        ft = threading.Thread(target=_trigger_fault, args=(ev,), daemon=True)
                        ft.start()
                        fault_threads.append((ev, ft))
                    time.sleep(0.25)

                minute_idx += 1

            # Wait for any scheduled fault threads still finishing (with remaining budget)
            for ev, ft in fault_threads:
                ft.join(timeout=120.0)
                if ft.is_alive():
                    with fault_lock:
                        fault_errors.append(
                            {
                                "kind": ev.get("kind"),
                                "seq": ev.get("seq"),
                                "planned_at_s": ev.get("at_s"),
                                "error": "fault thread did not finish within 120s join timeout",
                            }
                        )
            for fut in pending_futures:
                fut.result()

        # Wait until cluster queue is completely idle before taking idle disk snapshot
        idle_deadline = time.monotonic() + 90.0
        while time.monotonic() < idle_deadline:
            st = cluster.run_client(["status", "--json"], timeout=15.0)
            sj = extract_json_payload(st.stdout)
            if sj and int(sj.get("pending_tasks", 1)) == 0 and int(sj.get("running_tasks", 1)) == 0:
                break
            time.sleep(0.5)
        time.sleep(1.5)

        probe_stop.set()
        if probe_thread is not None:
            probe_thread.join(timeout=60.0)
        if sampler is not None and not sampler._stop.is_set():
            samples = sampler.stop()
        idle_snap = cluster.idle_disk_snapshot()
        cluster.snapshot_master_task_retries()
    finally:
        probe_stop.set()
        if probe_thread is not None:
            probe_thread.join(timeout=5.0)
        if sampler is not None and not sampler._stop.is_set():
            samples = sampler.stop()
        cluster.stop()
        master_audit = cluster.audit_master_logs(expected_task_ids=ledger.submitted_task_ids())
        leftover = cluster.leftover_pids()
        leftover_trees = cluster.leftover_process_trees()
        if not getattr(args, "keep_workdir", False):
            shutil.rmtree(sc_dir, ignore_errors=True)

    retry_stats = apply_master_task_retries_to_ledger(
        ledger,
        cluster._master_task_stats if cluster._api_snapshots_taken > 0 else {},
        fallback_requeued_task_ids=master_audit.get("requeued_task_ids", ()),
    )
    master_audit["task_api_snapshots"] = cluster._api_snapshots_taken
    master_audit["task_api_errors"] = list(cluster._api_snapshot_errors)
    master_audit["task_api_tasks_observed"] = retry_stats["tasks_observed"]
    master_audit["task_api_retries_total"] = retry_stats["retries_total"]
    master_audit["task_api_retried_tasks"] = retry_stats["retried_tasks"]
    master_audit["task_api_failovers_total"] = retry_stats["failovers_total"]
    master_audit["task_api_evacuations_total"] = retry_stats["evacuations_total"]
    master_audit["retries_source"] = retry_stats["source"]

    ledger_audit = ledger.audit()
    ledger_audit["master_failover_sweeps"] = master_audit.get("failover_sweeps", 0)
    ledger_audit["master_requeues_logged"] = master_audit.get("requeues_logged", 0)
    ledger_audit["master_failovers_total"] = retry_stats["failovers_total"]
    ledger_audit["retries_source"] = retry_stats["source"]

    # Pinned headroom covers one minute of concurrent new-input/reused/output blobs in flight
    pinned_headroom = int((5 * 100 + 2 * 50) * MIB * min(1.0, load_scale) + 128 * MIB)
    worker_pinned_headroom = int((slots * 100 + 2 * 50) * MIB * min(1.0, load_scale) + 64 * MIB)
    actual_duration_s = round(time.monotonic() - soak_t0, 2)
    pass_eval = evaluate_soak_pass_checks(
        ledger_audit=ledger_audit,
        samples=samples,
        probes=probe_samples,
        duration_s=duration_s,
        blob_store_limit_bytes=blob_store_mb * MIB,
        worker_cache_limit_bytes=blob_cache_mb * MIB,
        pinned_headroom_bytes=pinned_headroom,
        idle_sandbox_entries=idle_snap.get("idle_sandbox_entries", {}),
        tmp_leftovers=idle_snap.get("tmp_leftovers", {}),
        compactions=cluster.compactions,
        leftover_pids=leftover,
        idle_blob_store_bytes=idle_snap.get("idle_blob_store_bytes"),
        idle_worker_cache_bytes=idle_snap.get("idle_worker_cache_bytes"),
        worker_pinned_headroom_bytes=worker_pinned_headroom,
        master_audit=master_audit,
        fault_plan=fault_plan,
        faults_executed=fault_events_log,
        fault_errors=fault_errors,
        leftover_task_trees=leftover_trees,
    )
    return {
        "passed": pass_eval["all_passed"],
        "actual_duration_s": actual_duration_s,
        "fault_plan": fault_plan,
        "faults_executed": fault_events_log,
        "fault_errors": fault_errors,
        "master_audit": master_audit,
        "compactions": cluster.compactions,
        "pass_checks": pass_eval["checks"],
        "probe_summary": {
            "count": len(probe_samples),
            "ok_count": sum(1 for p in probe_samples if p.get("ok")),
            "samples": probe_samples,
        },
        "resource_summary": BackgroundSampler.summarize(samples),
        "samples": samples,
    }


def cmd_transfer(args: argparse.Namespace) -> int:
    try:
        validate_transfer_args(args)
        resolved_master = resolve_master_arg(getattr(args, "master", None))
    except ValueError as exc:
        raise SystemExit(str(exc))

    secret_file = getattr(args, "secret_file", None)
    if secret_file:
        secret_file = os.path.abspath(os.path.expanduser(secret_file))
        if not os.path.isfile(secret_file):
            raise SystemExit("secret file not found: %s" % secret_file)
        args.secret_file = secret_file

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
    work_root, disk_check = _prepare_work_root(args.work_dir, largest_bytes, keep_workdir=args.keep_workdir)
    meta = _collect_env_metadata(binary, args.rtt_ms, args.link, resolved_master=resolved_master)
    extra_forbidden = [
        tok
        for tok in (
            resolved_master,
            getattr(args, "master", None),
            getattr(args, "secret_file", None),
            getattr(args, "master_blob_dir", None),
            getattr(args, "master_log", None),
        )
        if tok
    ]

    remote_workers: Optional[List[Dict[str, Any]]] = None
    if resolved_master:
        probe_dir = os.path.join(work_root, "remote-preflight")
        probe_cluster = RemoteEnduranceCluster(
            binary,
            probe_dir,
            resolved_master,
            secret_file=getattr(args, "secret_file", None),
            insecure=bool(getattr(args, "insecure", False)),
            require_tags=getattr(args, "require_tag", None) or (),
            target_worker=getattr(args, "target_worker", None),
            master_pid=getattr(args, "master_pid", None),
            master_blob_dir=getattr(args, "master_blob_dir", None),
            master_log=getattr(args, "master_log", None),
        )
        try:
            probe_cluster.start()
            remote_workers = probe_cluster.remote_workers
        finally:
            probe_cluster.stop()

    results: List[Dict[str, Any]] = []
    try:
        for sc in scenarios:
            print("[%s] starting..." % sc, flush=True)
            res = SCENARIO_RUNNERS[sc](args, work_root)
            results.append(res)
            status = "SKIP" if res.get("skipped") else ("PASS" if res["passed"] else "FAIL")
            print("[%s] %s" % (sc, status), flush=True)
    finally:
        if not args.keep_workdir:
            shutil.rmtree(work_root, ignore_errors=True)

    omitted_scenarios = [s for s in ALL_SCENARIOS if s not in scenarios]
    omitted_links = [lk for lk in TRANSFER_LINKS if lk != args.link]

    record: Dict[str, Any] = {
        "benchmark": "TEST-24 large-transfer endurance suite",
        "date_utc": meta["started"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": meta["git"],
        "binary": {"version": meta["version"], "build_profile": build_profile(binary)},
        "hardware": meta["hw"],
        "os": meta["os_facts"],
        "windows_realtime_av_enabled": meta["av_realtime"],
        "power": power_state(),
        "python": platform.python_version(),
        "link": args.link,
        "parameters": {
            "seed": _effective_seed(args),
            "base_seed": int(args.seed),
            "sample_interval_s": float(args.sample_interval),
            "require_tags": list(getattr(args, "require_tag", None) or []),
            "target_worker": getattr(args, "target_worker", None),
        },
        "measured_rtt_ms": meta["loopback_rtt_ms"],
        "theoretical_single_blob_bound_mib_per_s": theoretical_single_blob_bound_mib_per_s(meta["loopback_rtt_ms"]),
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
        "note": redact_text(args.label, [work_root] + extra_forbidden),
        "all_passed": all(r["passed"] for r in results),
        "scenarios": results,
    }
    if resolved_master:
        record["link_target_kind"] = "p2p-ticket" if resolved_master.startswith("{") else "tcp-addr"
        record["remote_workers"] = remote_workers or []

    quarantined = _save_or_print_record(
        record,
        out_path=args.out,
        default_link=args.link,
        started=meta["started"],
        os_facts=meta["os_facts"],
        hw=meta["hw"],
        no_write=args.no_write,
        work_root=work_root,
        extra_forbidden=extra_forbidden,
    )
    for r in results:
        status = "SKIP" if r.get("skipped") else ("PASS" if r["passed"] else "FAIL")
        print("  %-4s %-5s %s" % (r["id"], status, r["name"]), flush=True)
    if quarantined:
        return 2
    return 0 if record["all_passed"] else 1


def cmd_soak(args: argparse.Namespace) -> int:
    try:
        parsed_params = validate_soak_args(args)
    except ValueError as exc:
        raise SystemExit(str(exc))

    binary = os.path.abspath(args.bin)
    if not os.path.isfile(binary):
        raise SystemExit("binary not found: %s" % binary)

    largest_bytes = max(100 * MIB, int(round(100 * MIB * min(1.0, parsed_params["load_scale"]))))
    work_root, disk_check = _prepare_work_root(args.work_dir, largest_bytes, keep_workdir=args.keep_workdir)
    meta = _collect_env_metadata(binary, args.rtt_ms, args.link)

    try:
        print(
            "[soak] starting duration=%.1fs fault_scale=%s load_scale=%s workers=%d slots=%d max_queue=%d"
            % (
                parsed_params["duration_s"],
                parsed_params["fault_scale"],
                parsed_params["load_scale"],
                parsed_params["workers"],
                parsed_params["slots"],
                parsed_params["max_queue_size"],
            ),
            flush=True,
        )
        soak_res = run_soak(args, work_root, parsed_params)
    finally:
        if not args.keep_workdir:
            shutil.rmtree(work_root, ignore_errors=True)

    record = {
        "benchmark": "TEST-25 soak endurance suite",
        "date_utc": meta["started"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git": meta["git"],
        "binary": {"version": meta["version"], "build_profile": build_profile(binary)},
        "hardware": meta["hw"],
        "os": meta["os_facts"],
        "windows_realtime_av_enabled": meta["av_realtime"],
        "power": power_state(),
        "python": platform.python_version(),
        "link": args.link,
        "parameters": {
            "seed": int(args.seed),
            "duration": str(args.duration),
            "duration_s": float(parsed_params["duration_s"]),
            "fault_scale": float(parsed_params["fault_scale"]),
            "load_scale": float(parsed_params["load_scale"]),
            "sample_interval_s": float(parsed_params["sample_interval_s"]),
            "workers": int(parsed_params["workers"]),
            "slots": int(parsed_params["slots"]),
            "workspaces": bool(args.workspaces),
            "max_queue_size": int(parsed_params["max_queue_size"]),
            "blob_store_mb": int(parsed_params["blob_store_mb"]),
            "blob_cache_mb": int(parsed_params["blob_cache_mb"]),
            "session_resume_hold_secs": args.session_resume_hold_secs,
            "rate_mbps": args.rate_mbps,
            "delay_ms": float(args.delay_ms),
        },
        "measured_rtt_ms": meta["loopback_rtt_ms"],
        "disk_check": disk_check,
        "verification_note": (
            "Client streams seeded random bytes while computing SHA-256 and worker task scripts re-read staged inputs "
            "or workspace marker files and check SHA-256 independently of the code under test; oxideswarm computes and verifies "
            "BLAKE3 in Master BlobStore and Worker BlobCache."
        ),
        "note": redact_text(args.label, [work_root]),
        "all_passed": bool(soak_res["passed"]),
        "soak": soak_res,
    }

    dur_label = "soak-%s-%s" % (_slug(str(args.duration)), args.link)
    quarantined = _save_or_print_record(
        record,
        out_path=args.out,
        default_link=dur_label,
        started=meta["started"],
        os_facts=meta["os_facts"],
        hw=meta["hw"],
        no_write=args.no_write,
        work_root=work_root,
    )
    for cname, cval in soak_res["pass_checks"].items():
        print("  %-20s %s" % (cname, "PASS" if cval.get("passed") else "FAIL"), flush=True)
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
    tr.add_argument("--master", help="Remote master address, P2P ticket JSON, or @/path/to/ticket file")
    tr.add_argument("--secret-file", help="Cluster secret file path when using --master")
    tr.add_argument("--insecure", action="store_true", help="Pass --insecure to client submits with --master")
    tr.add_argument("--require-tag", action="append", default=[], help="Worker placement tag filter (repeatable)")
    tr.add_argument("--target-worker", help="Restrict submissions to one worker name or id (--worker)")
    tr.add_argument("--master-pid", type=int, help="External local master PID for resource sampling")
    tr.add_argument("--master-blob-dir", help="External local master blob dir for store/tmp inspection")
    tr.add_argument("--master-log", help="External local master log path for T6/T7 inspection")
    tr.add_argument("--skip-t5-proxy", action="store_true", help="Use client SIGKILL cut instead of TCP proxy in T5")
    tr.add_argument("--rtt-ms", type=float, help="Measured link RTT in ms")
    tr.add_argument("--sample-interval", type=float, default=1.0, help="Resource poll interval in seconds")
    tr.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Base PRNG seed")
    tr.add_argument("--out", help="Output JSON record path")
    tr.add_argument("--no-write", action="store_true", help="Print JSON record instead of writing file")
    tr.add_argument("--label", default="", help="Free-text note stored in the record")

    sk = sub.add_parser("soak", help="Run multi-worker fault-injection soak test (TEST-25, D3)")
    sk.add_argument("--bin", default=default_bin, help="Path to built oxideswarm binary")
    sk.add_argument("--worker-python", default=sys.executable, help="Python executable path on worker")
    sk.add_argument("--duration", default="60m", help="Soak duration (e.g. 3m, 45m, 60m, 24h)")
    sk.add_argument("--fault-scale", type=float, default=1.0, help="Multiply D3 fault intervals (<1 compresses faults)")
    sk.add_argument("--load-scale", type=float, default=1.0, help="Scale per-minute task counts and blob sizes")
    sk.add_argument("--sample-interval", default="60s", help="Resource and timed-probe interval (e.g. 15s, 60s)")
    sk.add_argument("--workers", type=int, default=3, help="Number of workers (2 or 3)")
    sk.add_argument("--slots", type=int, default=4, help="Execution slots per worker")
    sk.add_argument(
        "--workspaces",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable persistent worker workspaces and per-minute sync tasks (default: on)",
    )
    sk.add_argument("--max-queue-size", type=int, default=64, help="Master --max-queue-size cap for burst test")
    sk.add_argument("--blob-store-mb", type=int, default=4096, help="Master --blob-store-mb cap")
    sk.add_argument("--blob-cache-mb", type=int, default=2048, help="Worker --blob-cache-mb cap")
    sk.add_argument("--session-resume-hold-secs", type=int, help="Optional master --session-resume-hold-secs override")
    sk.add_argument("--rate-mbps", type=float, help="Optional per-worker ThrottleProxy bandwidth cap in Mbit/s")
    sk.add_argument("--delay-ms", type=float, default=0.0, help="Optional per-worker ThrottleProxy RTT delay in ms")
    sk.add_argument("--work-dir", help="Base directory for temporary stores, state dirs, and payloads")
    sk.add_argument("--keep-workdir", action="store_true", help="Keep temporary workdir on exit")
    sk.add_argument("--link", default="loopback", help="Link label (loopback only in step 1)")
    sk.add_argument("--rtt-ms", type=float, help="Measured link RTT in ms")
    sk.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Base PRNG seed")
    sk.add_argument("--out", help="Output JSON record path")
    sk.add_argument("--no-write", action="store_true", help="Print JSON record instead of writing file")
    sk.add_argument("--label", default="", help="Free-text note stored in the record")

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
    if args.subcommand == "soak":
        return cmd_soak(args)
    if args.subcommand == "throttle":
        return cmd_throttle(args)
    if args.subcommand == "sample":
        return cmd_sample(args)
    raise SystemExit("unknown subcommand: %s" % args.subcommand)


if __name__ == "__main__":
    raise SystemExit(main())
