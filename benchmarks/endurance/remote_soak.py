#!/usr/bin/env python3
"""
benchmarks/endurance/remote_soak.py - Authenticated remote-worker soak helpers for OxideSwarm (TEST-25 D4.3).

Python 3 standard library only. Coordinates a `--master-only` soak driver on one machine with
one or more remote `worker-agent` processes over an HMAC-SHA256 authenticated HTTP control
channel keyed from a shared `--secret-file` copied out-of-band.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import hmac
import http.server
import json
import math
import os
import re
import secrets
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

KIB = 1024
MIB = 1024 * 1024

ALLOWED_CONTROL_OPS = frozenset({"kill_restart", "link_cut", "idle_snapshot", "shutdown"})
MAX_CONTROL_BODY_BYTES = 32 * MIB
DEFAULT_REPLAY_WINDOW_S = 300.0

_WORKER_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,32}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9._-]{8,128}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")


def validate_worker_name(name: str) -> str:
    """Validate a remote worker name (ASCII [A-Za-z0-9._-], 1..=32 chars, not '.'/'..')."""
    if not isinstance(name, str) or name in (".", "..") or not _WORKER_NAME_RE.match(name):
        raise ValueError(
            "worker name must match ^[A-Za-z0-9._-]{1,32}$ and not be '.' or '..' (got %r)" % (name,)
        )
    return name


def load_secret_file(path: str) -> Tuple[str, str]:
    """Read a trimmed cluster secret from `path` without logging or exposing the secret."""
    if not path or not str(path).strip():
        raise ValueError("--secret-file is required")
    abs_path = os.path.abspath(str(path))
    if not os.path.isfile(abs_path):
        raise ValueError("secret file does not exist or is not a regular file: %s" % abs_path)
    with open(abs_path, "r", encoding="utf-8") as f:
        secret_text = f.read().strip()
    if len(secret_text) < 16:
        raise ValueError("secret file must contain at least 16 characters (%s)" % abs_path)
    return abs_path, secret_text


def parse_host_port(addr: str, default_port: int = 0) -> Tuple[str, int]:
    """Parse HOST:PORT, [IPv6]:PORT, or bare HOST into (host, port)."""
    raw = str(addr or "").strip()
    if not raw:
        raise ValueError("address must not be empty")
    if raw.startswith("["):
        end = raw.find("]")
        if end <= 1:
            raise ValueError("invalid bracketed host:port %r" % (addr,))
        host = raw[1:end]
        rest = raw[end + 1 :]
        if not rest:
            port = int(default_port)
        elif rest.startswith(":"):
            port = int(rest[1:])
        else:
            raise ValueError("invalid bracketed host:port %r" % (addr,))
    elif raw.count(":") == 1:
        host, pstr = raw.rsplit(":", 1)
        port = int(pstr) if pstr else int(default_port)
    elif ":" in raw:
        # Unbracketed bare IPv6 (e.g. '::1') without port
        host = raw
        port = int(default_port)
    else:
        host = raw
        port = int(default_port)
    if not host:
        raise ValueError("host must not be empty in %r" % (addr,))
    if port < 0 or port > 65535:
        raise ValueError("port must be 0..=65535 in %r" % (addr,))
    return host, port


def format_host_port(host: str, port: int) -> str:
    clean = host.strip("[]")
    if ":" in clean:
        return "[%s]:%d" % (clean, int(port))
    return "%s:%d" % (clean, int(port))


def connectable_host(bind_host: str) -> str:
    clean = bind_host.strip("[]")
    if clean in ("", "0.0.0.0"):
        return "127.0.0.1"
    if clean == "::":
        return "::1"
    return clean


def derive_control_key(secret_text: str) -> bytes:
    """Derive a domain-separated HMAC-SHA256 key from the cluster secret."""
    if not secret_text:
        raise ValueError("secret text must not be empty")
    return hmac.new(
        secret_text.encode("utf-8"),
        b"oxideswarm-endurance-control-v1",
        hashlib.sha256,
    ).digest()


def _header_token_ok(value: str) -> bool:
    return isinstance(value, str) and bool(value) and ("\n" not in value) and ("\r" not in value)


def canonical_request_bytes(
    method: str,
    path: str,
    worker: str,
    ts: str,
    nonce: str,
    body_bytes: bytes,
) -> bytes:
    for part in (method, path, worker, ts, nonce):
        if not _header_token_ok(part):
            raise ValueError("invalid header token in request signature")
    prefix = ("%s\n%s\n%s\n%s\n%s\n" % (method, path, worker, ts, nonce)).encode("utf-8")
    return prefix + body_bytes


def canonical_response_bytes(
    status: int,
    path: str,
    worker: str,
    ts: str,
    nonce: str,
    body_bytes: bytes,
) -> bytes:
    for part in (path, worker, ts, nonce):
        if not _header_token_ok(part):
            raise ValueError("invalid header token in response signature")
    prefix = ("RESP\n%d\n%s\n%s\n%s\n%s\n" % (int(status), path, worker, ts, nonce)).encode("utf-8")
    return prefix + body_bytes


def sign_request(
    key: bytes,
    method: str,
    path: str,
    worker: str,
    ts: str,
    nonce: str,
    body_bytes: bytes,
) -> str:
    msg = canonical_request_bytes(method, path, worker, ts, nonce, body_bytes)
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def verify_request(
    key: bytes,
    method: str,
    path: str,
    worker: str,
    ts: str,
    nonce: str,
    body_bytes: bytes,
    sig: str,
) -> bool:
    if not isinstance(sig, str) or not _HEX64_RE.match(sig.lower()):
        return False
    try:
        expected = sign_request(key, method, path, worker, ts, nonce, body_bytes)
    except ValueError:
        return False
    return hmac.compare_digest(expected, sig.lower())


def sign_response(
    key: bytes,
    status: int,
    path: str,
    worker: str,
    ts: str,
    nonce: str,
    body_bytes: bytes,
) -> str:
    msg = canonical_response_bytes(status, path, worker, ts, nonce, body_bytes)
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def verify_response(
    key: bytes,
    status: int,
    path: str,
    worker: str,
    ts: str,
    nonce: str,
    body_bytes: bytes,
    sig: str,
) -> bool:
    if not isinstance(sig, str) or not _HEX64_RE.match(sig.lower()):
        return False
    try:
        expected = sign_response(key, status, path, worker, ts, nonce, body_bytes)
    except ValueError:
        return False
    return hmac.compare_digest(expected, sig.lower())


class ReplayGuard:
    """Thread-safe timestamp window and per-nonce replay detector."""

    def __init__(self, window_s: float = DEFAULT_REPLAY_WINDOW_S, max_entries: int = 16384) -> None:
        self.window_s = float(window_s)
        self.max_entries = max(64, int(max_entries))
        self._lock = threading.Lock()
        self._seen: Dict[str, float] = {}

    def check_and_record(self, nonce: str, ts: float, now: Optional[float] = None) -> bool:
        if not isinstance(nonce, str) or not _NONCE_RE.match(nonce.replace(":", "")):
            return False
        if len(nonce) < 8 or len(nonce) > 192:
            return False
        if not isinstance(ts, (int, float)) or not math.isfinite(float(ts)):
            return False
        cur_now = float(now if now is not None else time.time())
        if abs(cur_now - float(ts)) > self.window_s:
            return False

        exp = float(ts) + self.window_s
        with self._lock:
            stale = [k for k, v in self._seen.items() if v < cur_now]
            for k in stale:
                del self._seen[k]
            if nonce in self._seen:
                return False
            while len(self._seen) >= self.max_entries:
                oldest_key = next(iter(self._seen))
                del self._seen[oldest_key]
            self._seen[nonce] = exp
            return True


def merge_remote_samples(
    master_samples: List[Dict[str, Any]],
    remote_samples_by_worker: Dict[str, List[Dict[str, Any]]],
    sample_interval_s: float,
) -> List[Dict[str, Any]]:
    """Merge per-worker remote sampler rows into the master's timeline sorted by `t_s`."""
    merged: List[Dict[str, Any]] = []
    for row in master_samples:
        merged.append(
            {
                "t_s": float(row.get("t_s", 0.0)),
                "processes": copy.deepcopy(row.get("processes") or {}),
                "dirs": copy.deepcopy(row.get("dirs") or {}),
            }
        )
    merged.sort(key=lambda r: float(r["t_s"]))
    tol = max(1.0, float(sample_interval_s) * 0.55)

    for _wname, wrows in sorted(remote_samples_by_worker.items()):
        for wrow in sorted(wrows, key=lambda r: float(r.get("t_s", 0.0))):
            wt = float(wrow.get("t_s", 0.0))
            wprocs = wrow.get("processes") or {}
            wdirs = wrow.get("dirs") or {}
            best_row: Optional[Dict[str, Any]] = None
            best_diff = tol + 1.0
            for mrow in merged:
                diff = abs(float(mrow["t_s"]) - wt)
                if diff <= tol and diff < best_diff:
                    # Do not overwrite if this slot already carries any overlapping role keys
                    if any(k in mrow["processes"] for k in wprocs):
                        continue
                    best_diff = diff
                    best_row = mrow
            if best_row is not None:
                best_row["processes"].update(copy.deepcopy(wprocs))
                best_row["dirs"].update(copy.deepcopy(wdirs))
            else:
                merged.append(
                    {
                        "t_s": wt,
                        "processes": copy.deepcopy(wprocs),
                        "dirs": copy.deepcopy(wdirs),
                    }
                )
                merged.sort(key=lambda r: float(r["t_s"]))
    return merged


class _ThreadingHTTPServerV4(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class _ThreadingHTTPServerV6(socketserver.ThreadingMixIn, http.server.HTTPServer):
    address_family = socket.AF_INET6
    daemon_threads = True
    allow_reuse_address = True


class SoakControlServer:
    """Authenticated HMAC-SHA256 HTTP control plane hosted by `soak --master-only`."""

    def __init__(
        self,
        listen_addr: str,
        control_key: bytes,
        *,
        expected_workers: Optional[List[str]] = None,
        expected_count: int = 2,
        sample_interval_s: float = 60.0,
        port_file: Optional[str] = None,
    ) -> None:
        self.listen_host, self.listen_port = parse_host_port(listen_addr, default_port=0)
        self.control_key = bytes(control_key)
        self.expected_workers = [validate_worker_name(w) for w in (expected_workers or [])]
        self.expected_count = max(1, int(expected_count))
        self.sample_interval_s = float(sample_interval_s)
        self.port_file = port_file
        self.run_id = secrets.token_hex(16)

        self._replay = ReplayGuard(window_s=DEFAULT_REPLAY_WINDOW_S)
        self._cond = threading.Condition()
        self._closed = False
        self._soak_t0: Optional[float] = None
        self._workers: Dict[str, Dict[str, Any]] = {}
        self._pending_by_worker: Dict[str, List[Dict[str, Any]]] = {}
        self._cmd_results: Dict[str, Dict[str, Any]] = {}

        owner = self

        class _Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                return

            def _send_unauth(self, status: int = 401) -> None:
                self.close_connection = True
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()

            def _send_signed(self, status: int, path: str, worker: str, payload: Dict[str, Any]) -> None:
                raw = (json.dumps(payload) + "\n").encode("utf-8")
                ts_str = "%.3f" % time.time()
                nonce = secrets.token_hex(16)
                sig = sign_response(owner.control_key, status, path, worker, ts_str, nonce, raw)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("X-Soak-Ts", ts_str)
                self.send_header("X-Soak-Nonce", nonce)
                self.send_header("X-Soak-Sig", sig)
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self) -> None:  # noqa: N802
                self.close_connection = True
                raw_len = self.headers.get("Content-Length")
                try:
                    length = int(raw_len) if raw_len is not None else -1
                except ValueError:
                    self._send_unauth(400)
                    return
                if length < 0 or length > MAX_CONTROL_BODY_BYTES:
                    self._send_unauth(413)
                    return
                body_bytes = self.rfile.read(length) if length > 0 else b""
                if len(body_bytes) != length:
                    self._send_unauth(400)
                    return

                worker_hdr = self.headers.get("X-Soak-Worker", "")
                ts_hdr = self.headers.get("X-Soak-Ts", "")
                nonce_hdr = self.headers.get("X-Soak-Nonce", "")
                sig_hdr = self.headers.get("X-Soak-Sig", "")
                try:
                    worker = validate_worker_name(worker_hdr)
                    ts_val = float(ts_hdr)
                except ValueError:
                    self._send_unauth(401)
                    return
                if not _NONCE_RE.match(nonce_hdr):
                    self._send_unauth(401)
                    return

                path = self.path.split("?", 1)[0]
                # Verify HMAC before inserting nonce into ReplayGuard so unauthenticated traffic cannot poison cache.
                if not verify_request(owner.control_key, "POST", path, worker, ts_hdr, nonce_hdr, body_bytes, sig_hdr):
                    self._send_unauth(401)
                    return
                if not owner._replay.check_and_record("%s:%s" % (worker, nonce_hdr), ts_val):
                    self._send_unauth(401)
                    return

                try:
                    body = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
                except (UnicodeDecodeError, ValueError):
                    self._send_signed(400, path, worker, {"error": "invalid JSON body"})
                    return
                if not isinstance(body, dict):
                    self._send_signed(400, path, worker, {"error": "JSON body must be an object"})
                    return

                status, resp = owner._handle_authenticated(path, worker, body)
                self._send_signed(status, path, worker, resp)

        srv_cls = _ThreadingHTTPServerV6 if ":" in self.listen_host.strip("[]") else _ThreadingHTTPServerV4
        self._httpd = srv_cls((self.listen_host.strip("[]"), self.listen_port), _Handler)
        self.port = int(self._httpd.server_address[1])
        self.bound_addr = format_host_port(self.listen_host, self.port)
        self.connect_addr = format_host_port(connectable_host(self.listen_host), self.port)
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="soak-control-srv", daemon=True)
        self._thread.start()

        if self.port_file:
            os.makedirs(os.path.dirname(os.path.abspath(self.port_file)), exist_ok=True)
            with open(self.port_file, "w", encoding="utf-8") as f:
                f.write(self.connect_addr + "\n")

    def _elapsed_now(self) -> Tuple[bool, float]:
        if self._soak_t0 is None:
            return False, 0.0
        return True, max(0.0, round(time.monotonic() - self._soak_t0, 2))

    def _handle_authenticated(self, path: str, worker: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        with self._cond:
            soak_running, elapsed_s = self._elapsed_now()
            if path == "/v1/register":
                if self.expected_workers and worker not in self.expected_workers:
                    return 403, {"error": "worker %r not in expected_workers" % (worker,)}
                if worker not in self._workers and len(self._workers) >= self.expected_count:
                    return 403, {"error": "all %d worker slots are already registered" % self.expected_count}
                self._workers[worker] = {
                    "name": worker,
                    "os": body.get("os") if isinstance(body.get("os"), dict) else {},
                    "hardware": body.get("hardware") if isinstance(body.get("hardware"), dict) else {},
                    "windows_realtime_av_enabled": body.get("windows_realtime_av_enabled"),
                    "binary_version": str(body.get("binary_version") or ""),
                    "registered_at_monotonic": time.monotonic(),
                    "last_seen_monotonic": time.monotonic(),
                    "idle_disk_snapshot": None,
                    "samples": [],
                    "leftover_pids": [],
                    "leftover_task_trees": [],
                }
                self._pending_by_worker.setdefault(worker, [])
                self._cond.notify_all()
                return 200, {
                    "run_id": self.run_id,
                    "worker_name": worker,
                    "sample_interval_s": self.sample_interval_s,
                    "soak_running": soak_running,
                    "elapsed_s": elapsed_s,
                }

            if worker not in self._workers:
                return 403, {"error": "worker %r is not registered" % (worker,)}
            if str(body.get("run_id") or "") != self.run_id:
                return 400, {"error": "run_id mismatch"}

            self._workers[worker]["last_seen_monotonic"] = time.monotonic()

            if path == "/v1/poll":
                q = self._pending_by_worker.setdefault(worker, [])
                cmd = dict(q[0]) if q else None
                return 200, {
                    "run_id": self.run_id,
                    "soak_running": soak_running,
                    "elapsed_s": elapsed_s,
                    "command": cmd,
                }

            if path == "/v1/ack":
                cmd_id = str(body.get("cmd_id") or "")
                op = str(body.get("op") or "")
                if not cmd_id or op not in ALLOWED_CONTROL_OPS:
                    return 400, {"error": "invalid cmd_id or disallowed op"}
                ok = bool(body.get("ok"))
                res_obj = body.get("result") if isinstance(body.get("result"), dict) else {}
                err = body.get("error")

                q = self._pending_by_worker.setdefault(worker, [])
                if q and q[0].get("cmd_id") == cmd_id:
                    q.pop(0)
                else:
                    self._pending_by_worker[worker] = [item for item in q if item.get("cmd_id") != cmd_id]

                wrec = self._workers[worker]
                if ok and op == "idle_snapshot":
                    if isinstance(res_obj.get("samples"), list):
                        wrec["samples"] = list(res_obj["samples"])
                    if isinstance(res_obj.get("idle_disk_snapshot"), dict):
                        wrec["idle_disk_snapshot"] = dict(res_obj["idle_disk_snapshot"])
                elif ok and op == "shutdown":
                    if isinstance(res_obj.get("leftover_pids"), list):
                        wrec["leftover_pids"] = [int(p) for p in res_obj["leftover_pids"]]
                    if isinstance(res_obj.get("leftover_task_trees"), list):
                        wrec["leftover_task_trees"] = list(res_obj["leftover_task_trees"])
                    if isinstance(res_obj.get("samples"), list) and not wrec["samples"]:
                        wrec["samples"] = list(res_obj["samples"])

                self._cmd_results[cmd_id] = {
                    "cmd_id": cmd_id,
                    "op": op,
                    "worker": worker,
                    "ok": ok,
                    "result": res_obj,
                    "error": err,
                }
                self._cond.notify_all()
                return 200, {"ok": True, "cmd_id": cmd_id}

            return 404, {"error": "unknown endpoint"}

    def wait_for_agents(self, count: int, timeout_s: float = 600.0) -> List[str]:
        deadline = time.monotonic() + float(timeout_s)
        with self._cond:
            while not self._closed:
                if self.expected_workers:
                    if all(w in self._workers for w in self.expected_workers):
                        return list(self.expected_workers)
                elif len(self._workers) >= int(count):
                    return sorted(self._workers.keys())[: int(count)]
                rem = deadline - time.monotonic()
                if rem <= 0:
                    have = sorted(self._workers.keys())
                    want = self.expected_workers if self.expected_workers else ("%d workers" % int(count))
                    raise RuntimeError(
                        "timed out waiting for remote worker-agent registration (want=%r have=%r)" % (want, have)
                    )
                self._cond.wait(timeout=min(0.5, rem))
        raise RuntimeError("control server closed while waiting for worker-agents")

    def mark_soak_started(self, soak_t0: float) -> None:
        with self._cond:
            self._soak_t0 = float(soak_t0)
            self._cond.notify_all()

    def worker_metadata(self) -> List[Dict[str, Any]]:
        with self._cond:
            out: List[Dict[str, Any]] = []
            for name in sorted(self._workers.keys()):
                rec = self._workers[name]
                out.append(
                    {
                        "name": rec["name"],
                        "os": copy.deepcopy(rec["os"]),
                        "hardware": copy.deepcopy(rec["hardware"]),
                        "windows_realtime_av_enabled": rec["windows_realtime_av_enabled"],
                        "binary_version": rec["binary_version"],
                    }
                )
            return out

    def remote_worker_artifacts(self) -> Dict[str, Dict[str, Any]]:
        with self._cond:
            return {
                name: {
                    "samples": copy.deepcopy(rec["samples"]),
                    "idle_disk_snapshot": copy.deepcopy(rec["idle_disk_snapshot"]),
                    "leftover_pids": list(rec["leftover_pids"]),
                    "leftover_task_trees": copy.deepcopy(rec["leftover_task_trees"]),
                }
                for name, rec in self._workers.items()
            }

    def dispatch_command(
        self,
        worker_name: str,
        op: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        timeout_s: float = 120.0,
    ) -> Dict[str, Any]:
        if op not in ALLOWED_CONTROL_OPS:
            raise ValueError("disallowed control op %r" % (op,))
        cmd_id = secrets.token_hex(12)
        cmd_obj = {
            "cmd_id": cmd_id,
            "op": op,
            "payload": dict(payload or {}),
        }
        deadline = time.monotonic() + float(timeout_s)
        with self._cond:
            if self._closed:
                raise RuntimeError("control server is closed")
            if worker_name not in self._workers:
                raise RuntimeError("unknown remote worker %r" % (worker_name,))
            self._pending_by_worker.setdefault(worker_name, []).append(cmd_obj)
            self._cond.notify_all()
            while cmd_id not in self._cmd_results:
                if self._closed:
                    raise RuntimeError("control server closed while waiting for %s ack" % op)
                rem = deadline - time.monotonic()
                if rem <= 0:
                    q = self._pending_by_worker.get(worker_name, [])
                    self._pending_by_worker[worker_name] = [c for c in q if c.get("cmd_id") != cmd_id]
                    raise RuntimeError(
                        "remote worker %r timed out waiting for %s ack after %.1fs" % (worker_name, op, float(timeout_s))
                    )
                self._cond.wait(timeout=min(0.5, rem))
            ack = self._cmd_results.pop(cmd_id)
        if not ack.get("ok"):
            raise RuntimeError(
                "remote worker %r failed %s: %s" % (worker_name, op, ack.get("error") or "unknown error")
            )
        return ack

    def stop(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        try:
            self._httpd.shutdown()
        except Exception:
            pass
        try:
            self._httpd.server_close()
        except Exception:
            pass
        if self._thread.is_alive():
            self._thread.join(timeout=3.0)


class SignedControlClient:
    """Signed HTTP client used by `WorkerAgent` that rejects unauthenticated server replies."""

    def __init__(self, base_url: str, worker_name: str, control_key: bytes, extra_secrets: Iterable[str] = ()) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_name = validate_worker_name(worker_name)
        self.control_key = bytes(control_key)
        self.extra_secrets = tuple(extra_secrets)
        self._resp_replay = ReplayGuard(window_s=DEFAULT_REPLAY_WINDOW_S)
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def post(self, path: str, payload: Dict[str, Any], timeout_s: float = 15.0) -> Tuple[int, Dict[str, Any]]:
        body_bytes = json.dumps(payload).encode("utf-8")
        ts_str = "%.3f" % time.time()
        nonce = secrets.token_hex(16)
        sig = sign_request(self.control_key, "POST", path, self.worker_name, ts_str, nonce, body_bytes)
        req = urllib.request.Request(
            self.base_url + path,
            data=body_bytes,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Soak-Worker": self.worker_name,
                "X-Soak-Ts": ts_str,
                "X-Soak-Nonce": nonce,
                "X-Soak-Sig": sig,
            },
        )
        status = 0
        resp_headers: Any = {}
        resp_raw = b""
        try:
            with self._opener.open(req, timeout=timeout_s) as resp:
                status = int(resp.status)
                resp_headers = resp.headers
                resp_raw = resp.read()
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            resp_headers = exc.headers or {}
            resp_raw = exc.read() if exc.fp is not None else b""

        if status == 401:
            raise RuntimeError("control server rejected authentication (401) on %s" % path)

        r_ts = resp_headers.get("X-Soak-Ts", "")
        r_nonce = resp_headers.get("X-Soak-Nonce", "")
        r_sig = resp_headers.get("X-Soak-Sig", "")
        try:
            r_ts_val = float(r_ts)
        except ValueError as exc:
            raise RuntimeError("control server reply missing valid X-Soak-Ts on %s" % path) from exc

        if not verify_response(self.control_key, status, path, self.worker_name, r_ts, r_nonce, resp_raw, r_sig):
            raise RuntimeError("control server response HMAC verification failed on %s" % path)
        if not self._resp_replay.check_and_record("srv:%s:%s" % (self.worker_name, r_nonce), r_ts_val):
            raise RuntimeError("control server response replay/timestamp check failed on %s" % path)

        parsed = json.loads(resp_raw.decode("utf-8")) if resp_raw else {}
        if not isinstance(parsed, dict):
            raise RuntimeError("control server reply was not a JSON object")
        return status, parsed


def create_remote_soak_cluster(
    base_module: Any,
    *,
    binary: str,
    workdir: str,
    workers: int,
    slots: int,
    blob_store_mb: int,
    blob_cache_mb: int,
    max_queue_size: int,
    workspaces: bool,
    session_resume_hold_secs: Optional[int],
    master_listen: str,
    control_listen: str,
    secret_file: str,
    expect_workers: Optional[List[str]],
    register_timeout_s: float,
    sample_interval_s: float,
    master_port_file: Optional[str] = None,
    control_port_file: Optional[str] = None,
) -> Any:
    """Construct a `RemoteSoakCluster` inheriting from `base_module.SoakCluster`."""

    class RemoteSoakCluster(base_module.SoakCluster):  # type: ignore[misc]
        """Master-side soak cluster coordinating external authenticated `worker-agent` nodes."""

        def __init__(self) -> None:
            self._secret_file, secret_text = load_secret_file(secret_file)
            self._control_key = derive_control_key(secret_text)
            self._master_listen_cfg = str(master_listen or "127.0.0.1:0")
            self._control_listen_cfg = str(control_listen or "127.0.0.1:0")
            self._master_port_file = master_port_file
            self._control_port_file = control_port_file
            self._expect_workers = list(expect_workers or [])
            self._register_timeout_s = float(register_timeout_s)
            self._sample_interval_s = float(sample_interval_s)
            self._fixed_listen_addr: Optional[str] = None
            self.worker_names: List[str] = []
            self.dashboard_token: str = self._resolve_dashboard_token(binary, self._secret_file)
            self.forbidden_secrets: List[str] = [
                tok for tok in (secret_text, self.dashboard_token, self._secret_file) if tok
            ]
            self.control_server: Optional[SoakControlServer] = None
            self._remote_idle_snaps: Dict[str, Dict[str, Any]] = {}
            self._remote_leftover_pids: List[Dict[str, Any]] = []
            self._remote_leftover_trees: List[Dict[str, Any]] = []
            super().__init__(
                binary,
                workdir,
                workers=workers,
                slots=slots,
                blob_store_mb=blob_store_mb,
                blob_cache_mb=blob_cache_mb,
                max_queue_size=max_queue_size,
                workspaces=workspaces,
                session_resume_hold_secs=session_resume_hold_secs,
                rate_mbps=None,
                delay_ms=0.0,
            )

        @staticmethod
        def _resolve_dashboard_token(bin_path: str, sec_path: str) -> str:
            env = dict(os.environ)
            env.pop("OXIDESWARM_SECRET", None)
            env["OXIDESWARM_SECRET_FILE"] = sec_path
            try:
                res = subprocess.run(
                    [bin_path, "secret", "show", "--secret-file", sec_path, "--dashboard-token"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                    timeout=10.0,
                )
                if res.returncode == 0:
                    return res.stdout.strip()
            except Exception:
                pass
            return ""

        def _build_env(self, home: str) -> Dict[str, str]:
            env = super()._build_env(home)
            env["OXIDESWARM_SECRET_FILE"] = self._secret_file
            return env

        def redact(self, text: str, extra: Iterable[str] = ()) -> str:
            tokens = list(extra) + list(self.forbidden_secrets) + [self.workdir]
            return base_module.redact_text(text, tokens)

        def _start_master_locked(self) -> None:
            assert self.state_dir is not None
            os.makedirs(self.state_dir, exist_ok=True)
            os.makedirs(self.blob_dir, exist_ok=True)
            port_file = os.path.join(self.workdir, "master.port")
            dash_file = os.path.join(self.workdir, "dash.port")

            listen_arg = self._fixed_listen_addr or self._master_listen_cfg
            args = [
                "master",
                "--listen",
                listen_arg,
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
                "--secret-file",
                self._secret_file,
            ]
            if self.session_resume_hold_secs is not None:
                args += ["--session-resume-hold-secs", str(int(self.session_resume_hold_secs))]

            max_attempts = 5 if self._fixed_listen_addr else 1
            for attempt in range(max_attempts):
                for fp in (port_file, dash_file):
                    if os.path.exists(fp):
                        try:
                            os.remove(fp)
                        except OSError:
                            pass

                gen = self._master_gen
                self._master_gen += 1
                p, log = self._spawn_role_proc("master-%d" % gen, "master", args, new_group=True)
                self._master_entry = (gen, p, log)
                self.pids["master"] = p.pid

                retrying_addr_in_use = False
                deadline = time.monotonic() + 30.0
                while time.monotonic() < deadline:
                    if p.poll() is not None:
                        try:
                            log.close()
                        except Exception:
                            pass
                        self._record_pid_exit(p.pid)
                        self._master_entry = None
                        tail = "\n".join(self.read_log("master-%d" % gen).splitlines()[-10:])
                        low_tail = tail.lower()
                        is_addr_in_use = (
                            "10048" in low_tail
                            or "address already in use" in low_tail
                            or "addrinuse" in low_tail
                        )
                        if self._fixed_listen_addr and is_addr_in_use and attempt + 1 < max_attempts:
                            retrying_addr_in_use = True
                            time.sleep(0.5)
                            break
                        raise RuntimeError("master exited early (gen=%d code=%s): %s" % (gen, p.returncode, tail))
                    if os.path.exists(port_file) and os.path.getsize(port_file) > 0:
                        with open(port_file, encoding="utf-8", errors="replace") as f:
                            txt = f.read().strip()
                        if txt:
                            bind_host, _ = parse_host_port(listen_arg, default_port=0)
                            if ":" in txt:
                                parsed_host, bound_port = parse_host_port(txt, default_port=0)
                                if parsed_host not in ("", "0.0.0.0", "::", "[::]"):
                                    bind_host = parsed_host
                            else:
                                bound_port = int(txt)
                            if not self._fixed_listen_addr:
                                self._fixed_listen_addr = format_host_port(bind_host, bound_port)
                            conn_host = connectable_host(bind_host)
                            with self._addr_lock:
                                self.master_host = conn_host
                                self.master_port = int(bound_port)
                                self.master_addr = format_host_port(conn_host, bound_port)
                            if self._master_port_file:
                                os.makedirs(os.path.dirname(os.path.abspath(self._master_port_file)), exist_ok=True)
                                with open(self._master_port_file, "w", encoding="utf-8") as mpf:
                                    mpf.write(self.master_addr + "\n")
                            dash_deadline = time.monotonic() + 5.0
                            self.dashboard_addr = ""
                            while time.monotonic() < dash_deadline:
                                if os.path.exists(dash_file) and os.path.getsize(dash_file) > 0:
                                    with open(dash_file, encoding="utf-8", errors="replace") as df:
                                        dtxt = df.read().strip()
                                    if dtxt:
                                        dhost, dport = parse_host_port(dtxt if ":" in dtxt else "127.0.0.1:" + dtxt)
                                        self.dashboard_addr = format_host_port(connectable_host(dhost), dport)
                                        break
                                time.sleep(0.05)
                            return
                    time.sleep(0.05)
                if not retrying_addr_in_use:
                    raise RuntimeError("master did not publish its port within 30 s")
            raise RuntimeError("master did not rebind fixed address within %d attempts" % max_attempts)

        def _wait_workers_connected(
            self,
            expected_indices: Iterable[int],
            timeout: float = 60.0,
            *,
            pause_on_master_restart: bool = False,
        ) -> None:
            idx_list = list(expected_indices)
            want = {self.worker_names[idx] for idx in idx_list if 0 <= idx < len(self.worker_names)}
            if not want:
                return
            deadline = time.monotonic() + float(timeout)
            while time.monotonic() < deadline:
                if pause_on_master_restart and self._master_restarting.is_set():
                    t_pause = time.monotonic()
                    self.wait_if_restarting(timeout=min(90.0, max(1.0, deadline - t_pause)))
                    deadline += max(0.0, time.monotonic() - t_pause)
                    continue
                master_restarting_now = False
                with self._lifecycle_lock:
                    if self._master_entry is not None and self._master_entry[1].poll() is not None:
                        if pause_on_master_restart and self._master_restarting.is_set():
                            master_restarting_now = True
                        else:
                            gen, mp, _log = self._master_entry
                            tail = "\n".join(self.read_log("master-%d" % gen).splitlines()[-10:])
                            raise RuntimeError("master exited early (code=%s): %s" % (mp.returncode, tail))
                if master_restarting_now:
                    t_pause = time.monotonic()
                    self.wait_if_restarting(timeout=90.0)
                    deadline += max(0.0, time.monotonic() - t_pause)
                    continue
                r = self.run_client(["workers", "--json"], timeout=15)
                if r.returncode == 0:
                    try:
                        listed = json.loads(r.stdout)
                    except ValueError:
                        listed = []
                    if isinstance(listed, list):
                        connected_names: Set[str] = set()
                        for item in listed:
                            if base_module.is_worker_entry_connected(item):
                                wname = item.get("name")
                                if wname:
                                    connected_names.add(str(wname))
                        if want.issubset(connected_names):
                            return
                time.sleep(0.2)
            raise RuntimeError("expected remote workers %s did not connect within %.0f s" % (sorted(want), timeout))

        def start(self) -> "RemoteSoakCluster":
            self.control_server = SoakControlServer(
                self._control_listen_cfg,
                self._control_key,
                expected_workers=self._expect_workers,
                expected_count=self.n_workers,
                sample_interval_s=self._sample_interval_s,
                port_file=self._control_port_file,
            )
            with self._lifecycle_lock:
                self._start_master_locked()
            self.worker_names = self.control_server.wait_for_agents(
                self.n_workers,
                timeout_s=self._register_timeout_s,
            )
            self._wait_workers_connected(range(len(self.worker_names)), timeout=min(self._register_timeout_s, 120.0))
            return self

        def mark_soak_started(self, soak_t0: float) -> None:
            if self.control_server is not None:
                self.control_server.mark_soak_started(soak_t0)

        def remote_worker_metadata(self) -> List[Dict[str, Any]]:
            if self.control_server is None:
                return []
            return self.control_server.worker_metadata()

        def restart_worker_fault(self, idx: int, down_s: float) -> Tuple[int, int]:
            assert self.control_server is not None
            wname = self.worker_names[int(idx)]
            with self._lifecycle_lock:
                self._worker_down.add(int(idx))
            try:
                ack = self.control_server.dispatch_command(
                    wname,
                    "kill_restart",
                    {"down_s": float(down_s)},
                    timeout_s=max(120.0, float(down_s) + 90.0),
                )
                res = ack.get("result") or {}
                old_pid = int(res.get("old_pid") or 0)
                new_pid = int(res.get("new_pid") or 0)
                if old_pid <= 0 or new_pid <= 0:
                    raise RuntimeError("worker_kill worker %s returned invalid PIDs (%r)" % (wname, res))
            finally:
                with self._lifecycle_lock:
                    self._worker_down.discard(int(idx))
            self._wait_workers_connected([int(idx)], timeout=60.0, pause_on_master_restart=True)
            return old_pid, new_pid

        def cut_worker_link(self, idx: int, pause_s: float) -> None:
            assert self.control_server is not None
            wname = self.worker_names[int(idx)]
            self.control_server.dispatch_command(
                wname,
                "link_cut",
                {"down_s": float(pause_s)},
                timeout_s=max(150.0, float(pause_s) + 120.0),
            )

        def snapshot_master_task_retries(self) -> int:
            if not self.dashboard_addr:
                return 0
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            limit = 1000
            offset = 0
            seen_in_pass = 0
            headers = {"Accept": "application/json"}
            if self.dashboard_token:
                headers["Authorization"] = "Bearer " + self.dashboard_token
            try:
                for _page in range(25):
                    url = "http://%s/api/tasks?limit=%d&offset=%d" % (self.dashboard_addr, limit, offset)
                    req = urllib.request.Request(url, headers=headers)
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
                            "failover_count": max(
                                int(prev.get("failover_count", 0)), int(item.get("failover_count", 0) or 0)
                            ),
                            "evacuation_count": max(
                                int(prev.get("evacuation_count", 0)), int(item.get("evacuation_count", 0) or 0)
                            ),
                        }
                    if len(payload) < limit:
                        break
                    offset += len(payload)
                self._api_snapshots_taken += 1
            except Exception as exc:
                self._api_snapshot_errors.append(self.redact(str(exc)))
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
                    expected = [i for i in range(len(self.worker_names)) if i not in self._worker_down]
                self._wait_workers_connected(expected, timeout=90.0)
                return comp or {}
            finally:
                with self._sync_cond:
                    self._master_restarting.clear()
                    self._sync_cond.notify_all()

        def dir_map(self) -> Dict[str, str]:
            assert self.state_dir is not None
            return {
                "blob_store_bytes": self.blob_dir,
                "state_dir_bytes": self.state_dir,
                "journal_bin_bytes": os.path.join(self.state_dir, "journal.bin"),
                "snapshot_bin_bytes": os.path.join(self.state_dir, "snapshot.bin"),
            }

        def collect_remote_idle_and_samples(self) -> Dict[str, List[Dict[str, Any]]]:
            if self.control_server is None or not self.worker_names:
                return {}
            out: Dict[str, List[Dict[str, Any]]] = {}

            def _pull_one(wname: str) -> Tuple[str, Dict[str, Any]]:
                ack = self.control_server.dispatch_command(wname, "idle_snapshot", {}, timeout_s=120.0)
                return wname, dict(ack.get("result") or {})

            with ThreadPoolExecutor(max_workers=len(self.worker_names)) as pool:
                for wname, res in pool.map(_pull_one, self.worker_names):
                    if isinstance(res.get("idle_disk_snapshot"), dict):
                        self._remote_idle_snaps[wname] = dict(res["idle_disk_snapshot"])
                    if isinstance(res.get("samples"), list):
                        out[wname] = list(res["samples"])
            return out

        def idle_disk_snapshot(self) -> Dict[str, Any]:
            sandbox_entries: Dict[str, List[str]] = {}
            tmp_leftovers: Dict[str, List[str]] = {
                "master_blobs_tmp": base_module.list_tmp_entries(self.blob_dir),
            }
            worker_caches: Dict[str, int] = {}
            for wname in self.worker_names:
                rs = self._remote_idle_snaps.get(wname)
                if not rs:
                    sandbox_entries[wname] = ["missing_remote_idle_snapshot"]
                    continue
                sandbox_entries[wname] = list(rs.get("sandbox_entries") or [])
                worker_caches["worker_cache_%s_bytes" % wname] = int(rs.get("worker_cache_bytes", 0))
                tmp_leftovers["%s_cache_tmp" % wname] = list(rs.get("cache_tmp") or [])
                locks = list(rs.get("locks") or [])
                if locks:
                    tmp_leftovers["%s_locks" % wname] = locks
            return {
                "idle_blob_store_bytes": base_module.dir_bytes(self.blob_dir),
                "idle_worker_cache_bytes": worker_caches,
                "idle_sandbox_entries": sandbox_entries,
                "tmp_leftovers": tmp_leftovers,
            }

        def stop(self) -> None:
            if self.control_server is not None and self.worker_names:
                def _stop_one(wname: str) -> Optional[Dict[str, Any]]:
                    try:
                        ack = self.control_server.dispatch_command(wname, "shutdown", {}, timeout_s=45.0)
                        return dict(ack.get("result") or {})
                    except Exception as exc:
                        return {"shutdown_error": self.redact(str(exc)), "leftover_pids": []}

                with ThreadPoolExecutor(max_workers=len(self.worker_names)) as pool:
                    for wname, res in zip(self.worker_names, pool.map(_stop_one, self.worker_names)):
                        if not res:
                            continue
                        for lp in res.get("leftover_pids") or []:
                            self._remote_leftover_pids.append({"worker": wname, "pid": int(lp)})
                        for lt in res.get("leftover_task_trees") or []:
                            row = dict(lt)
                            row["worker"] = wname
                            self._remote_leftover_trees.append(row)
                        if res.get("shutdown_error"):
                            self._api_snapshot_errors.append("worker %s shutdown: %s" % (wname, res["shutdown_error"]))

            self.stop_master("final_stop")
            with self._tracked_clients_lock:
                for p in list(self._tracked_clients):
                    if p.poll() is None:
                        p.kill()
                        p.wait(timeout=5.0)
                    if base_module.pid_alive(p.pid):
                        self._unclean_pids.append(p.pid)
                    else:
                        self._record_pid_exit(p.pid)
            if self.control_server is not None:
                self.control_server.stop()
            self.procs = []

        def leftover_pids(self) -> List[int]:
            alive = list(self._unclean_pids)
            with self._lifecycle_lock:
                if self._master_entry is not None and self._master_entry[1].poll() is None:
                    alive.append(self._master_entry[1].pid)
            with self._tracked_clients_lock:
                for p in self._tracked_clients:
                    if p.poll() is None:
                        alive.append(p.pid)
            for item in self._remote_leftover_pids:
                alive.append(int(item["pid"]))
            return sorted(set(alive))

        def leftover_process_trees(self) -> List[Dict[str, Any]]:
            local = super().leftover_process_trees()
            return local + list(self._remote_leftover_trees)

    return RemoteSoakCluster()


class WorkerAgent:
    """Runs an isolated `oxideswarm worker`, samples it locally, and executes signed soak faults."""

    def __init__(
        self,
        base_module: Any,
        *,
        binary: str,
        master_addr: str,
        control_addr: str,
        secret_file: str,
        name: str,
        slots: int = 4,
        workspaces: bool = True,
        blob_cache_mb: int = 2048,
        sample_interval_s: float = 60.0,
        rate_mbps: Optional[float] = None,
        delay_ms: float = 0.0,
        work_dir: Optional[str] = None,
        keep_workdir: bool = False,
        poll_interval_s: float = 1.0,
        connect_timeout_s: float = 600.0,
    ) -> None:
        self.base = base_module
        self.binary = os.path.abspath(binary)
        if not os.path.isfile(self.binary):
            raise ValueError("binary not found: %s" % self.binary)
        self.master_host, self.master_port = parse_host_port(master_addr, default_port=8088)
        ctrl_host, ctrl_port = parse_host_port(control_addr, default_port=18501)
        self.control_url = "http://" + format_host_port(connectable_host(ctrl_host), ctrl_port)
        self.secret_file, secret_text = load_secret_file(secret_file)
        self.control_key = derive_control_key(secret_text)
        self.secret_text = secret_text
        self.name = validate_worker_name(name)
        self.slots = max(1, int(slots))
        self.workspaces = bool(workspaces)
        self.blob_cache_mb = max(32, int(blob_cache_mb))
        self.sample_interval_s = max(0.5, float(sample_interval_s))
        self.rate_mbps = rate_mbps
        self.delay_ms = float(delay_ms)
        self.keep_workdir = bool(keep_workdir)
        self.poll_interval_s = max(0.1, float(poll_interval_s))
        self.connect_timeout_s = max(5.0, float(connect_timeout_s))

        self.work_root, _disk_check = self.base._prepare_work_root(
            work_dir,
            100 * MIB,
            keep_workdir=self.keep_workdir,
        )
        # Keep home/oxideswarm and tmp separate sibling directories for Windows SEC-17/SEC-18 DACLs.
        self.home_dir = os.path.join(self.work_root, "home")
        self.ox_home = os.path.join(self.home_dir, ".oxideswarm")
        self.tmp_dir = os.path.join(self.work_root, "tmp")
        self.sandbox_dir = os.path.join(self.work_root, "sandbox")
        self.workspace_dir = os.path.join(self.work_root, "workspaces")
        self.empty_config = os.path.join(self.work_root, "empty.toml")
        for d in (self.home_dir, self.ox_home, self.tmp_dir, self.sandbox_dir):
            os.makedirs(d, exist_ok=True)
        if self.workspaces:
            os.makedirs(self.workspace_dir, exist_ok=True)
        with open(self.empty_config, "w", encoding="utf-8") as f:
            f.write("")

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._proxy: Optional[Any] = None
        self._worker_proc: Optional[subprocess.Popen] = None
        self._worker_log: Any = None
        self._worker_gen = 0
        self._unclean_pids: List[int] = []
        self._pid_windows: Dict[int, List[Dict[str, Optional[float]]]] = {}
        self._start_epoch_s = time.time()
        self._sampler: Optional[Any] = None
        self._sampler_t0_offset = 0.0
        self._run_id = ""
        self._client = SignedControlClient(self.control_url, self.name, self.control_key)

    def _worker_env(self) -> Dict[str, str]:
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("RUSTY_GRID_") and not k.startswith("OXIDESWARM_")
        }
        env.update(
            {
                "HOME": self.home_dir,
                "USERPROFILE": self.home_dir,
                "OXIDESWARM_HOME": self.ox_home,
                "OXIDESWARM_SECRET_FILE": self.secret_file,
                "TEMP": self.tmp_dir,
                "TMP": self.tmp_dir,
                "TMPDIR": self.tmp_dir,
                "RUSTY_GRID_CONFIG": self.empty_config,
                "RUSTY_GRID_NO_BATTERY": "1",
                "NO_COLOR": "1",
            }
        )
        return env

    def _record_pid_start(self, pid: int) -> None:
        now = time.time()
        with self._lock:
            self._pid_windows.setdefault(int(pid), []).append({"start_epoch": now, "end_epoch": None})

    def _record_pid_exit(self, pid: int) -> None:
        now = time.time()
        with self._lock:
            wins = self._pid_windows.get(int(pid))
            if wins and wins[-1].get("end_epoch") is None:
                wins[-1]["end_epoch"] = now

    def active_pids(self) -> Dict[str, int]:
        with self._lock:
            if self._worker_proc is not None and self._worker_proc.poll() is None:
                return {"worker_%s" % self.name: int(self._worker_proc.pid)}
            return {}

    def dir_map(self) -> Dict[str, str]:
        return {
            "sandbox_%s_bytes" % self.name: self.sandbox_dir,
            "worker_cache_%s_bytes" % self.name: os.path.join(self.sandbox_dir, "cache", "blobs"),
        }

    def _redact(self, text: str) -> str:
        return self.base.redact_text(text, [self.work_root, self.secret_file, self.secret_text])

    def _start_worker(self) -> int:
        assert self._proxy is not None
        gen = self._worker_gen
        self._worker_gen += 1
        log_path = os.path.join(self.work_root, "worker-%s-%d.log" % (self.name, gen))
        log_f = open(log_path, "wb")
        args = [
            self.binary,
            "worker",
            "--master",
            self._proxy.bound_addr,
            "--name",
            self.name,
            "--secret-file",
            self.secret_file,
            "--no-portal",
            "--no-gpu",
            "--max-concurrency",
            str(self.slots),
            "--sandbox-base-dir",
            self.sandbox_dir,
            "--blob-cache-mb",
            str(self.blob_cache_mb),
        ]
        if self.workspaces:
            args += ["--workspaces", "--workspace-root", self.workspace_dir]
        kw: Dict[str, Any] = {} if self.base.IS_WINDOWS else {"start_new_session": True}
        p = subprocess.Popen(
            args,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            env=self._worker_env(),
            **kw,
        )
        self._record_pid_start(p.pid)
        with self._lock:
            self._worker_proc = p
            self._worker_log = log_f
        time.sleep(0.25)
        if p.poll() is not None:
            try:
                log_f.close()
            except Exception:
                pass
            tail = ""
            if os.path.isfile(log_path):
                with open(log_path, encoding="utf-8", errors="replace") as rf:
                    tail = "\n".join(rf.read().splitlines()[-10:])
            raise RuntimeError(
                "worker %s exited early (code=%s): %s" % (self.name, p.returncode, self._redact(tail))
            )
        return int(p.pid)

    def _kill_worker(self) -> int:
        with self._lock:
            p = self._worker_proc
            log_f = self._worker_log
            self._worker_proc = None
            self._worker_log = None
        if p is None:
            return 0
        pid = int(p.pid)
        if p.poll() is None:
            p.kill()
            p.wait(timeout=10.0)
        if self.base.pid_alive(pid):
            self._unclean_pids.append(pid)
        else:
            self._record_pid_exit(pid)
        if log_f is not None:
            try:
                log_f.close()
            except Exception:
                pass
        return pid

    def _stop_worker_graceful(self) -> None:
        with self._lock:
            p = self._worker_proc
            log_f = self._worker_log
            self._worker_proc = None
            self._worker_log = None
        if p is None:
            return
        pid = int(p.pid)
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5.0)
        if self.base.pid_alive(pid):
            self._unclean_pids.append(pid)
        else:
            self._record_pid_exit(pid)
        if log_f is not None:
            try:
                log_f.close()
            except Exception:
                pass

    def _aligned_samples(self) -> List[Dict[str, Any]]:
        if self._sampler is None:
            return []
        if not self._sampler._stop.is_set():
            rows = self._sampler.stop()
        else:
            with self._sampler._lock:
                rows = list(self._sampler._samples)
        out: List[Dict[str, Any]] = []
        offset = float(self._sampler_t0_offset)
        for row in rows:
            out.append(
                {
                    "t_s": round(float(row.get("t_s", 0.0)) + offset, 2),
                    "processes": copy.deepcopy(row.get("processes") or {}),
                    "dirs": copy.deepcopy(row.get("dirs") or {}),
                }
            )
        return out

    def _ensure_sampler_started(self, soak_running: bool, elapsed_s: float) -> None:
        if soak_running and self._sampler is None:
            self._sampler_t0_offset = max(0.0, float(elapsed_s))
            self._sampler = self.base.BackgroundSampler(
                self.active_pids,
                self.dir_map(),
                interval_s=self.sample_interval_s,
            ).start()

    def _idle_snapshot_payload(self) -> Dict[str, Any]:
        samples = self._aligned_samples()
        cache_root = os.path.join(self.sandbox_dir, "cache", "blobs")
        locks_dir = os.path.join(self.sandbox_dir, ".locks")
        sandbox_entries = sorted(os.listdir(self.sandbox_dir)) if os.path.isdir(self.sandbox_dir) else []
        locks = sorted(os.listdir(locks_dir)) if os.path.isdir(locks_dir) else []
        return {
            "samples": samples,
            "idle_disk_snapshot": {
                "sandbox_entries": sandbox_entries,
                "worker_cache_bytes": int(self.base.dir_bytes(cache_root)),
                "cache_tmp": list(self.base.list_tmp_entries(cache_root)),
                "locks": locks,
            },
        }

    def _shutdown_payload(self) -> Dict[str, Any]:
        samples = self._aligned_samples()
        self._stop_worker_graceful()
        if self._proxy is not None:
            self._proxy.stop()
            self._proxy = None
        alive = list(self._unclean_pids)
        table = self.base._list_system_processes()
        with self._lock:
            pid_windows = {pid: [dict(w) for w in wins] for pid, wins in self._pid_windows.items()}
        trees = self.base.find_leftover_process_trees(
            table,
            pid_windows.keys(),
            self.work_root,
            ignore_pids={os.getpid()},
            min_created_epoch=self._start_epoch_s,
            root_pid_windows=pid_windows,
        )
        for row in trees:
            if isinstance(row.get("cmd"), str):
                row["cmd"] = self._redact(row["cmd"])
        return {
            "samples": samples,
            "leftover_pids": sorted(set(alive)),
            "leftover_task_trees": trees,
        }

    def _execute_command(self, cmd: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        op = str(cmd.get("op") or "")
        payload = cmd.get("payload") if isinstance(cmd.get("payload"), dict) else {}
        if op not in ALLOWED_CONTROL_OPS:
            return False, {}, "disallowed op %r" % (op,)
        try:
            if op == "kill_restart":
                down_s = float(payload.get("down_s", 0.0))
                if not math.isfinite(down_s) or down_s < 0.0 or down_s > 600.0:
                    raise ValueError("invalid kill_restart down_s=%r" % (down_s,))
                old_pid = self._kill_worker()
                if old_pid <= 0:
                    raise RuntimeError("worker %s had no live PID" % self.name)
                if down_s > 0:
                    time.sleep(down_s)
                new_pid = self._start_worker()
                return True, {"old_pid": old_pid, "new_pid": new_pid}, None

            if op == "link_cut":
                down_s = float(payload.get("down_s", 0.0))
                if not math.isfinite(down_s) or down_s < 0.0 or down_s > 600.0:
                    raise ValueError("invalid link_cut down_s=%r" % (down_s,))
                assert self._proxy is not None
                self._proxy.cut_links(pause_s=down_s)
                return True, {"cuts": int(self._proxy.cuts)}, None

            if op == "idle_snapshot":
                return True, self._idle_snapshot_payload(), None

            if op == "shutdown":
                res = self._shutdown_payload()
                self._stop_event.set()
                return True, res, None

            return False, {}, "unhandled op"
        except Exception as exc:
            return False, {}, self._redact(str(exc))

    def request_stop(self) -> None:
        self._stop_event.set()

    def run(self) -> int:
        try:
            self._proxy = self.base.ThrottleProxy(
                self.master_host,
                self.master_port,
                rate_mbps=self.rate_mbps,
                delay_ms=self.delay_ms,
            ).start()
            self._start_worker()

            reg_payload = {
                "name": self.name,
                "os": self.base.os_info(),
                "hardware": self.base.hardware(),
                "windows_realtime_av_enabled": self.base.windows_realtime_av_enabled(),
                "binary_version": self.base.sh([self.binary, "--version"]),
            }
            deadline = time.monotonic() + self.connect_timeout_s
            while not self._stop_event.is_set():
                try:
                    status, resp = self._client.post("/v1/register", reg_payload, timeout_s=15.0)
                    if status == 200 and resp.get("run_id"):
                        self._run_id = str(resp["run_id"])
                        if resp.get("sample_interval_s"):
                            self.sample_interval_s = max(0.5, float(resp["sample_interval_s"]))
                        self._ensure_sampler_started(
                            bool(resp.get("soak_running")),
                            float(resp.get("elapsed_s") or 0.0),
                        )
                        break
                    if status in (400, 403):
                        raise RuntimeError("registration rejected (%d): %s" % (status, resp.get("error")))
                except urllib.error.URLError:
                    pass
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError("worker-agent %s failed to register within %.0f s" % (self.name, self.connect_timeout_s))
                self._stop_event.wait(0.5)

            while not self._stop_event.is_set():
                try:
                    status, resp = self._client.post("/v1/poll", {"run_id": self._run_id}, timeout_s=15.0)
                except (OSError, urllib.error.URLError):
                    self._stop_event.wait(self.poll_interval_s)
                    continue
                if status != 200:
                    self._stop_event.wait(self.poll_interval_s)
                    continue
                self._ensure_sampler_started(
                    bool(resp.get("soak_running")),
                    float(resp.get("elapsed_s") or 0.0),
                )
                cmd = resp.get("command")
                if isinstance(cmd, dict) and cmd.get("cmd_id"):
                    ok, res_obj, err = self._execute_command(cmd)
                    ack_payload = {
                        "run_id": self._run_id,
                        "cmd_id": cmd["cmd_id"],
                        "op": cmd.get("op"),
                        "ok": ok,
                        "result": res_obj,
                        "error": err,
                    }
                    last_ack_exc: Optional[Exception] = None
                    for attempt in range(5):
                        try:
                            self._client.post("/v1/ack", ack_payload, timeout_s=30.0)
                            last_ack_exc = None
                            break
                        except (OSError, urllib.error.URLError) as exc:
                            last_ack_exc = exc
                            if attempt + 1 < 5:
                                time.sleep(1.0)
                    if last_ack_exc is not None:
                        raise last_ack_exc
                    if cmd.get("op") == "shutdown":
                        break
                else:
                    self._stop_event.wait(self.poll_interval_s)
            return 0
        finally:
            if self._sampler is not None and not self._sampler._stop.is_set():
                try:
                    self._sampler.stop()
                except Exception:
                    pass
            self._stop_worker_graceful()
            if self._proxy is not None:
                try:
                    self._proxy.stop()
                except Exception:
                    pass
                self._proxy = None
            if not self.keep_workdir and os.path.exists(self.work_root):
                shutil.rmtree(self.work_root, ignore_errors=True)
