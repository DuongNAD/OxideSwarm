#!/usr/bin/env python3
"""
benchmarks/campaigns/driver.py - research campaign sweep driver (`APP-6`, design 0094).

Standard-library-only Python driver that reads a campaign manifest, refuses any
secret inputs (`.env`, `.env.*`, `*.secret`, `invite.oxs`, including inside
directory inputs), optionally verifies the runner binary SHA-256 against the
manifest, pins `(os, arch)` from `--runner-system`, submits each work unit with:

  oxideswarm submit --json --wait --verify <N> [--verify-file <PATH>] ...

When a unit reports a verification mismatch (`verify.status == "mismatch"`),
the driver reruns that unit once with the same verification flags and records
both attempts. A task's own non-zero exit code (even exit code `4` without
`verify.status == "mismatch"`) is recorded as `failed`, not `mismatch`.

Interrupted units (`SIGINT`/`SIGTERM`, subprocess signal exit) are recorded as
`status = "interrupted"` (not `failed`), any known `task_id` is cancelled on the
master (or tracked in `<record>.orphans.json` for cancellation on `--resume`),
and plain `--resume` resubmits every `interrupted`, `verify_error`,
`mismatch_rerun_failed`, and pending unit (`--retry-failed` also retries
`failed`). All JSON records scrub personal names, hostnames, `--master` hosts,
CLI hints, raw worker names (including late joiners), worker UUIDs, IP
addresses, and `oxs1-*` secrets.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import datetime
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

RECORD_FORMAT = 1
FINISHED_STATUSES_DEFAULT = frozenset({"verified", "verified_on_rerun", "mismatch_persisted"})
IPV4_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
IPV6_BRACKET_RE = re.compile(r"\[[0-9a-fA-F:]+\](?::\d+)?")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
TASK_ID_LINE_RE = re.compile(
    r"(?:task[_ ]id\s*[:=]\s*|waiting for task\s+|task\s+)"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
    re.IGNORECASE,
)
OXS_TOKEN_RE = re.compile(r"oxs1-[A-Za-z0-9:._=-]+")
URL_RE = re.compile(r"\b(?:https?|wss?)://[^\s)>'\"\\]+", re.IGNORECASE)
REPLICA_WORKER_RE = re.compile(r"(replica on worker\s+)(\S+)(\s+failed\b)", re.IGNORECASE)
NOTE_FIRST_WORKER_RE = re.compile(
    r"(Note:\s+worker\s+)(\S+)(\s+that ran the first copy)", re.IGNORECASE
)
WORKER_LABEL_LINE_RE = re.compile(r"(Worker:\s+)([^\s(]+)")
MASTER_AT_RE = re.compile(r"(\bMaster\s+at\s+)([^\s),;]+)")
MASTER_PAREN_RE = re.compile(r"(\(Master:\s*)([^)]+)(\))")
SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")

FORBIDDEN_INPUT_EXACT = frozenset({".env", "cluster.secret", "invite.oxs"})


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_os(value: str) -> str:
    """Match `crates/core/src/placement.rs::normalize_os`."""
    lower = value.strip().lower()
    if lower in ("mac", "darwin", "osx", "macosx"):
        return "macos"
    if lower in ("win", "win32", "win64"):
        return "windows"
    return lower


def normalize_arch(value: str) -> str:
    """Match `crates/core/src/placement.rs::normalize_arch`."""
    lower = value.strip().lower()
    if lower in ("amd64", "x64", "x86-64"):
        return "x86_64"
    if lower == "arm64":
        return "aarch64"
    if lower in ("i386", "i686"):
        return "x86"
    return lower


def parse_system_key(system: str) -> tuple[str, str]:
    """Parse and normalize `<os>/<arch>` (for example `macos/aarch64`)."""
    raw = system.strip()
    if "/" not in raw:
        raise ValueError(f"--runner-system must have the form <os>/<arch>, got {system!r}")
    os_raw, arch_raw = raw.split("/", 1)
    if not os_raw.strip() or not arch_raw.strip() or "/" in arch_raw:
        raise ValueError(f"--runner-system must have the form <os>/<arch>, got {system!r}")
    return normalize_os(os_raw), normalize_arch(arch_raw)


def parse_shard_spec(spec: str) -> tuple[int, int]:
    """Parse a 1-based `K/N` shard specification."""
    raw = spec.strip()
    if "/" not in raw:
        raise ValueError(f"--shard must have the form K/N (for example 1/2), got {spec!r}")
    k_s, n_s = raw.split("/", 1)
    try:
        k = int(k_s)
        n = int(n_s)
    except ValueError as exc:
        raise ValueError(f"--shard must have integer K/N, got {spec!r}") from exc
    if n < 1 or k < 1 or k > n:
        raise ValueError(f"--shard requires 1 <= K <= N, got {spec!r}")
    return k, n


def slice_shard(units: list[dict[str, Any]], index: int, total: int) -> list[dict[str, Any]]:
    """Return contiguous 1-based slice `index` of `total` partitions."""
    if total == 1:
        return list(units)
    n = len(units)
    base, rem = divmod(n, total)
    start = (index - 1) * base + min(index - 1, rem)
    end = start + base + (1 if index <= rem else 0)
    return list(units[start:end])


def file_sha256(path: str | os.PathLike[str]) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _input_spec_parts(spec: str) -> tuple[str, str]:
    """Split a `--input LOCAL[:SANDBOX]` spec without breaking Windows drive letters like `C:\\...`."""
    if ":" not in spec:
        return spec, ""
    if len(spec) >= 2 and spec[1] == ":" and spec[0].isalpha():
        rest = spec[2:]
        if ":" in rest:
            local_rest, sandbox = rest.rsplit(":", 1)
            return spec[:2] + local_rest, sandbox
        return spec, ""
    local, sandbox = spec.rsplit(":", 1)
    return local, sandbox


def is_forbidden_filename(name: str) -> bool:
    """Return True if a filename/component matches `.env`, `.env.*`, `*.secret`, or `invite.oxs`."""
    base = name.strip().lower()
    if not base:
        return False
    return base in FORBIDDEN_INPUT_EXACT or base.startswith(".env.") or base.endswith(".secret")


def _path_has_forbidden_component(path_str: str) -> bool:
    norm = path_str.replace("\\", "/").strip("/")
    if not norm:
        return False
    for part in norm.split("/"):
        if is_forbidden_filename(part):
            return True
    return False


def is_forbidden_input_spec(spec: str) -> bool:
    """Return True if an `--input` spec names `.env`, `.env.*`, `*.secret`, or `invite.oxs`."""
    local, sandbox = _input_spec_parts(spec.strip())
    return any(bool(cand) and _path_has_forbidden_component(cand) for cand in (local, sandbox))


def _resolve_local_input_path(local: str, base_dir: str | None) -> str:
    if os.path.isabs(local) or base_dir is None:
        return os.path.normpath(local)
    return os.path.normpath(os.path.join(base_dir, local))


def validate_no_secret_inputs(
    specs: list[str],
    context: str,
    *,
    base_dir: str | None = None,
) -> None:
    """Validate `--input` specs lexically AND by walking existing local directories recursively."""
    for spec in specs:
        if not isinstance(spec, str) or not spec.strip():
            raise ValueError(f"{context}: input spec must be a non-empty string")
        if is_forbidden_input_spec(spec):
            raise ValueError(
                f"{context}: refused secret/credential input spec {spec!r} (.env, *.secret, invite.oxs)"
            )
        local, _ = _input_spec_parts(spec.strip())
        local_path = _resolve_local_input_path(local, base_dir)
        if os.path.islink(local_path):
            target_name = os.path.basename(os.path.realpath(local_path))
            if is_forbidden_filename(target_name):
                raise ValueError(
                    f"{context}: refused symlinked secret input {spec!r} -> {target_name!r}"
                )
        if os.path.isdir(local_path):
            seen_dirs: set[tuple[int, int]] = set()
            for dirpath, dirnames, filenames in os.walk(local_path, followlinks=True):
                try:
                    st = os.stat(dirpath)
                    key = (st.st_dev, st.st_ino)
                    if key in seen_dirs:
                        dirnames[:] = []
                        continue
                    seen_dirs.add(key)
                except OSError:
                    dirnames[:] = []
                    continue

                for dname in dirnames:
                    full_d = os.path.join(dirpath, dname)
                    rel_d = os.path.relpath(full_d, local_path)
                    if is_forbidden_filename(dname):
                        raise ValueError(
                            f"{context}: directory input {spec!r} contains forbidden directory {rel_d!r}"
                        )
                    if os.path.islink(full_d) and is_forbidden_filename(
                        os.path.basename(os.path.realpath(full_d))
                    ):
                        raise ValueError(
                            f"{context}: directory input {spec!r} symlinks secret target at {rel_d!r}"
                        )

                for fname in filenames:
                    full_f = os.path.join(dirpath, fname)
                    rel_f = os.path.relpath(full_f, local_path)
                    if is_forbidden_filename(fname):
                        raise ValueError(
                            f"{context}: directory input {spec!r} contains forbidden file {rel_f!r}"
                        )
                    if os.path.islink(full_f) and is_forbidden_filename(
                        os.path.basename(os.path.realpath(full_f))
                    ):
                        raise ValueError(
                            f"{context}: directory input {spec!r} symlinks secret file at {rel_f!r}"
                        )


def _format_template(
    value: Any,
    unit_id: str,
    seed: Any,
    params: dict[str, Any] | None = None,
) -> Any:
    if isinstance(value, str):
        out = value.replace("{unit_id}", unit_id).replace("{seed}", "" if seed is None else str(seed))
        if params:
            for k, v in params.items():
                if v is not None and not isinstance(v, (dict, list)):
                    out = out.replace("{" + str(k) + "}", str(v))
        return out
    if isinstance(value, list):
        return [_format_template(item, unit_id, seed, params) for item in value]
    if isinstance(value, dict):
        return {str(k): _format_template(v, unit_id, seed, params) for k, v in value.items()}
    return value


def _require_str_list(val: Any, context: str) -> list[str]:
    if val is None:
        return []
    if not isinstance(val, list) or not all(isinstance(x, str) for x in val):
        raise ValueError(f"{context} must be a list of strings")
    return list(val)


def _require_str_dict(val: Any, context: str) -> dict[str, str]:
    if val is None:
        return {}
    if not isinstance(val, dict):
        raise ValueError(f"{context} must be an object")
    out: dict[str, str] = {}
    for k, v in val.items():
        if not isinstance(k, str) or isinstance(v, (dict, list)):
            raise ValueError(f"{context}[{k!r}] must be a scalar string value")
        out[k] = "" if v is None else str(v)
    return out


def _as_positive_int(val: Any, context: str, minimum: int = 1) -> int:
    if isinstance(val, bool) or not isinstance(val, int) or val < minimum:
        raise ValueError(f"{context} must be an integer >= {minimum}, got {val!r}")
    return val


def load_manifest(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Load, validate, and normalize a campaign manifest JSON file."""
    manifest_dir = os.path.dirname(os.path.abspath(path))
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("manifest root must be a JSON object")
    if raw.get("schema_version") != 1:
        raise ValueError(f"unsupported schema_version {raw.get('schema_version')!r} (expected 1)")
    campaign_id = raw.get("campaign_id")
    if not isinstance(campaign_id, str) or not campaign_id.strip():
        raise ValueError("manifest.campaign_id must be a non-empty string")
    workload = raw.get("workload")
    if not isinstance(workload, dict):
        raise ValueError("manifest.workload must be an object")

    defaults = raw.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValueError("manifest.defaults must be an object")

    raw_units = raw.get("units")
    if not isinstance(raw_units, list) or not raw_units:
        raise ValueError("manifest.units must be a non-empty list")

    default_inputs = _require_str_list(defaults.get("inputs"), "defaults.inputs")
    validate_no_secret_inputs(default_inputs, "defaults.inputs", base_dir=manifest_dir)

    default_command = defaults.get("command")
    default_args = _require_str_list(defaults.get("args"), "defaults.args")
    default_outputs = _require_str_list(defaults.get("outputs"), "defaults.outputs")
    default_env = _require_str_dict(defaults.get("env"), "defaults.env")
    default_tags = _require_str_list(defaults.get("require_tags"), "defaults.require_tags")
    default_verify = _as_positive_int(defaults.get("verify", 1), "defaults.verify", minimum=1)
    default_verify_file = defaults.get("verify_file")
    if default_verify_file is not None and not isinstance(default_verify_file, str):
        raise ValueError("defaults.verify_file must be a string or null")
    default_verify_any = bool(defaults.get("verify_any_system", False))
    default_isolation = defaults.get("require_isolation")
    default_os = normalize_os(defaults["os"]) if isinstance(defaults.get("os"), str) and defaults["os"].strip() else None
    default_arch = normalize_arch(defaults["arch"]) if isinstance(defaults.get("arch"), str) and defaults["arch"].strip() else None
    default_timeout = defaults.get("timeout_secs", 1800)
    if default_timeout is not None:
        default_timeout = _as_positive_int(default_timeout, "defaults.timeout_secs", minimum=1)
    default_mismatch_retries = _as_positive_int(
        defaults.get("mismatch_retries", 1), "defaults.mismatch_retries", minimum=0
    )

    seen_ids: set[str] = set()
    normalized_units: list[dict[str, Any]] = []
    for i, u in enumerate(raw_units):
        if not isinstance(u, dict):
            raise ValueError(f"units[{i}] must be an object")
        unit_id = u.get("id")
        if not isinstance(unit_id, str) or not unit_id.strip():
            raise ValueError(f"units[{i}].id must be a non-empty string")
        if unit_id in seen_ids:
            raise ValueError(f"duplicate unit id {unit_id!r}")
        seen_ids.add(unit_id)

        seed = u.get("seed")
        params = u.get("params")
        if params is not None and not isinstance(params, dict):
            raise ValueError(f"unit {unit_id!r}.params must be an object")

        command = u.get("command", default_command)
        if not isinstance(command, str) or not command.strip():
            raise ValueError(f"unit {unit_id!r}: command must be a non-empty string")

        base_args = (
            _require_str_list(u.get("args"), f"unit {unit_id!r}.args")
            if "args" in u
            else list(default_args)
        )
        extra_args = _require_str_list(u.get("extra_args"), f"unit {unit_id!r}.extra_args")
        args = _format_template(base_args + extra_args, unit_id, seed, params)

        unit_inputs = (
            _require_str_list(u.get("inputs"), f"unit {unit_id!r}.inputs")
            if "inputs" in u
            else list(default_inputs)
        )
        extra_inputs = _require_str_list(u.get("extra_inputs"), f"unit {unit_id!r}.extra_inputs")
        merged_inputs = _format_template(unit_inputs + extra_inputs, unit_id, seed, params)
        validate_no_secret_inputs(merged_inputs, f"unit {unit_id!r}.inputs", base_dir=manifest_dir)

        resolved_inputs: list[str] = []
        for spec in merged_inputs:
            local, sandbox = _input_spec_parts(spec)
            if not os.path.isabs(local):
                local = os.path.normpath(os.path.join(manifest_dir, local))
            resolved_inputs.append(f"{local}:{sandbox}" if sandbox else local)

        outputs = (
            _require_str_list(u.get("outputs"), f"unit {unit_id!r}.outputs")
            if "outputs" in u
            else list(default_outputs)
        )
        outputs = _format_template(outputs, unit_id, seed, params)

        env = dict(default_env)
        env.update(_require_str_dict(u.get("env"), f"unit {unit_id!r}.env"))
        env = _format_template(env, unit_id, seed, params)
        if "ANIMA_SIM_SEED" not in env and seed is not None:
            env["ANIMA_SIM_SEED"] = str(seed)

        verify = (
            _as_positive_int(u["verify"], f"unit {unit_id!r}.verify", minimum=1)
            if "verify" in u
            else default_verify
        )
        verify_file = u.get("verify_file", default_verify_file)
        if verify_file is not None:
            if not isinstance(verify_file, str):
                raise ValueError(f"unit {unit_id!r}.verify_file must be a string or null")
            verify_file = _format_template(verify_file, unit_id, seed, params)
        if not outputs and not verify_file:
            raise ValueError(f"unit {unit_id!r}: --verify needs outputs or verify_file")

        verify_any_system = bool(u.get("verify_any_system", default_verify_any))
        require_isolation = u.get("require_isolation", default_isolation)
        require_tags = (
            _require_str_list(u.get("require_tags"), f"unit {unit_id!r}.require_tags")
            if "require_tags" in u
            else list(default_tags)
        )
        raw_os = u.get("os", default_os)
        raw_arch = u.get("arch", default_arch)
        os_target = normalize_os(raw_os) if isinstance(raw_os, str) and raw_os.strip() else None
        arch_target = normalize_arch(raw_arch) if isinstance(raw_arch, str) and raw_arch.strip() else None
        timeout_secs = u.get("timeout_secs", default_timeout)
        if timeout_secs is not None:
            timeout_secs = _as_positive_int(
                timeout_secs, f"unit {unit_id!r}.timeout_secs", minimum=1
            )
        mismatch_retries = (
            _as_positive_int(u["mismatch_retries"], f"unit {unit_id!r}.mismatch_retries", minimum=0)
            if "mismatch_retries" in u
            else default_mismatch_retries
        )

        normalized_units.append({
            "id": unit_id,
            "seed": seed,
            "params": copy.deepcopy(params) if params is not None else {},
            "command": command,
            "args": args,
            "inputs": resolved_inputs,
            "outputs": outputs,
            "env": env,
            "verify": verify,
            "verify_file": verify_file,
            "verify_any_system": verify_any_system,
            "require_isolation": require_isolation,
            "require_tags": require_tags,
            "os": os_target,
            "arch": arch_target,
            "timeout_secs": timeout_secs,
            "mismatch_retries": mismatch_retries,
        })

    return {
        "schema_version": 1,
        "campaign_id": campaign_id.strip(),
        "manifest_dir": manifest_dir,
        "workload": copy.deepcopy(workload),
        "units": normalized_units,
    }


def apply_unit_overrides(
    units: list[dict[str, Any]],
    *,
    pin_os: str | None = None,
    pin_arch: str | None = None,
    command_override: str | None = None,
    command_prefix_inputs: list[str] | None = None,
) -> None:
    """Apply CLI `(os, arch)` pin and `--command` overrides to units in place."""
    for u in units:
        if pin_os:
            if u.get("os") and normalize_os(u["os"]) != pin_os:
                raise ValueError(
                    f"unit {u['id']!r} specifies os={u['os']!r}, conflicting with pinned os={pin_os!r}"
                )
            u["os"] = pin_os
        if pin_arch:
            if u.get("arch") and normalize_arch(u["arch"]) != pin_arch:
                raise ValueError(
                    f"unit {u['id']!r} specifies arch={u['arch']!r}, conflicting with pinned arch={pin_arch!r}"
                )
            u["arch"] = pin_arch
        if command_override:
            u["command"] = command_override
        if command_prefix_inputs:
            u["inputs"] = list(command_prefix_inputs) + [
                s for s in u["inputs"] if s not in command_prefix_inputs
            ]


def verify_runner_hash(
    workload: dict[str, Any],
    runner_path: str | os.PathLike[str],
    runner_system: str | None,
) -> tuple[str, str]:
    """Verify `--runner` SHA-256 against `workload.runner_sha256[runner_system]`."""
    runner_abs = os.path.abspath(runner_path)
    if not os.path.isfile(runner_abs):
        raise ValueError(f"runner binary does not exist: {runner_abs}")
    if is_forbidden_input_spec(runner_abs) or is_forbidden_filename(
        os.path.basename(os.path.realpath(runner_abs))
    ):
        raise ValueError(f"refused forbidden runner path: {runner_abs}")

    hashes = workload.get("runner_sha256")
    if not isinstance(hashes, dict) or not hashes:
        raise ValueError("manifest.workload.runner_sha256 must map <os>/<arch> to SHA-256 hex")
    if not runner_system:
        raise ValueError("--runner requires --runner-system <os>/<arch> (for example macos/aarch64)")
    sys_os, sys_arch = parse_system_key(runner_system)
    norm_key = f"{sys_os}/{sys_arch}"
    expected = hashes.get(runner_system) or hashes.get(norm_key)
    if not isinstance(expected, str) or not SHA256_HEX_RE.match(expected.strip().lower()):
        raise ValueError(
            f"manifest.workload.runner_sha256[{runner_system!r}] must be a 64-char SHA-256 hex digest, got {expected!r}"
        )
    actual = file_sha256(runner_abs).lower()
    if actual != expected.strip().lower():
        raise ValueError(
            f"runner SHA-256 mismatch for {norm_key}: expected {expected.strip().lower()}, got {actual}"
        )
    sandbox_name = workload.get("binary_name") or os.path.basename(runner_abs)
    return f"{runner_abs}:{sandbox_name}", actual


def load_participant_map(path: str | os.PathLike[str] | None) -> dict[str, str]:
    """Load participant mapping file.

    Accepts either:
      - {"worker-name-or-uuid": "P1", ...}
      - {"P1": ["worker-a", "worker-b"], ...}
    Returns raw worker key -> participant label.
    """
    if path is None:
        return {}
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("participant map must be a JSON object")
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError(f"invalid participant map key {k!r}")
        if isinstance(v, str) and v.strip():
            out[k.strip()] = v.strip()
        elif isinstance(v, list):
            for worker_key in v:
                if not isinstance(worker_key, str) or not worker_key.strip():
                    raise ValueError(f"invalid worker entry {worker_key!r} under {k!r}")
                out[worker_key.strip()] = k.strip()
        else:
            raise ValueError(f"invalid participant map value for {k!r}: {v!r}")
    return out


class WorkerDirectory:
    """Thread-safe directory mapping raw worker names/UUIDs to participant & neutral labels."""

    def __init__(
        self,
        workers_json: list[dict[str, Any]] | None = None,
        participant_map: dict[str, str] | None = None,
        *,
        refresh_callback: Callable[[], list[dict[str, Any]] | None] | None = None,
        min_refresh_interval_s: float = 1.0,
    ) -> None:
        self._lock = threading.Lock()
        self._participant_map = dict(participant_map or {})
        self._by_raw: dict[str, dict[str, Any]] = {}
        self._records: list[dict[str, Any]] = []
        self._secret_map: dict[str, str] = {}
        self._known_labels: set[str] = set()
        self._per_participant_seq: dict[str, int] = {}
        self._refresh_callback = refresh_callback
        self._min_refresh_interval_s = min_refresh_interval_s
        # Allow the first unknown-worker or error refresh immediately after startup.
        self._last_refresh_mono = 0.0

        # Always scrub local machine/user names out of free-form diagnostic strings.
        host = platform.node() or ""
        for alias in {host, host.split(".", 1)[0]}:
            if len(alias) >= 2:
                self._secret_map[alias] = "<host>"
        for env_key in ("USER", "USERNAME", "LOGNAME"):
            user_val = os.environ.get(env_key, "")
            if len(user_val) >= 2:
                self._secret_map[user_val] = "<user>"
        home_dir = os.path.expanduser("~")
        if len(home_dir) >= 3 and home_dir not in ("/", "\\"):
            self._secret_map[home_dir] = "<home>"

        self._ingest_workers_locked(workers_json or [])

    def set_refresh_callback(
        self, callback: Callable[[], list[dict[str, Any]] | None] | None
    ) -> None:
        with self._lock:
            self._refresh_callback = callback

    def add_secret(self, raw: str | None, replacement: str) -> None:
        if not raw or not isinstance(raw, str):
            return
        val = raw.strip()
        if len(val) < 2 or val.lower() == "auto":
            return
        with self._lock:
            self._secret_map.setdefault(val, replacement)

    def register_endpoint_secrets(
        self,
        *,
        master: str | None = None,
        relay_url: str | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        """Register master and relay hosts so they can never leak into `record.json`."""
        env_src = env if env is not None else os.environ
        master_candidates = [
            master,
            env_src.get("OXIDESWARM_MASTER"),
            env_src.get("RUSTY_GRID_MASTER"),
            env_src.get("RUSTY_GRID_MASTER_ADDR"),
        ]
        for m in master_candidates:
            if not m or not isinstance(m, str):
                continue
            val = m.strip()
            if not val or val.lower() == "auto" or val.lower().startswith("auto:"):
                continue
            self.add_secret(val, "<master>")
            host_only = val.rsplit(":", 1)[0].strip("[]") if ":" in val else val.strip("[]")
            if len(host_only) >= 2 and host_only.lower() not in ("localhost", "127.0.0.1", "::1"):
                self.add_secret(host_only, "<master>")

        relay_candidates = [
            relay_url,
            env_src.get("OXIDESWARM_RELAY_URL"),
            env_src.get("RUSTY_GRID_RELAY_URL"),
        ]
        for r in relay_candidates:
            if not r or not isinstance(r, str):
                continue
            val = r.strip()
            if len(val) < 4:
                continue
            self.add_secret(val, "<relay>")
            parsed = urlparse(val)
            if parsed.hostname and parsed.hostname.lower() not in ("localhost", "127.0.0.1", "::1"):
                self.add_secret(parsed.hostname, "<relay>")

    def _ingest_workers_locked(self, workers_json: list[dict[str, Any]]) -> None:
        ordered = sorted(
            workers_json,
            key=lambda w: (str(w.get("name") or ""), str(w.get("id") or "")),
        )
        for w in ordered:
            if isinstance(w, dict):
                self._register_locked(w)

    def refresh_workers(self, workers_json: list[dict[str, Any]]) -> None:
        """Merge newly discovered workers without renumbering existing worker labels."""
        with self._lock:
            self._ingest_workers_locked(workers_json)
            self._last_refresh_mono = time.monotonic()

    def maybe_refresh(self, *, force: bool = False) -> bool:
        """Invoke the configured refresh callback at most once per `min_refresh_interval_s`."""
        with self._lock:
            cb = self._refresh_callback
            if cb is None:
                return False
            now = time.monotonic()
            if not force and (now - self._last_refresh_mono) < self._min_refresh_interval_s:
                return False
            self._last_refresh_mono = now
        try:
            fresh = cb()
        except Exception:
            return False
        if isinstance(fresh, list):
            self.refresh_workers(fresh)
            return True
        return False

    def has_worker(self, raw_id: str | None = None, raw_name: str | None = None) -> bool:
        with self._lock:
            return any(
                key and key in self._by_raw for key in (raw_id, raw_name)
            )

    def _resolve_participant_locked(
        self, raw_id: str | None, raw_name: str | None, tags: list[str]
    ) -> str:
        for key in (raw_id, raw_name):
            if key and key in self._participant_map:
                return self._participant_map[key]
        for t in tags:
            if isinstance(t, str) and t.startswith("participant:"):
                label = t.split(":", 1)[1].strip()
                if label:
                    return label
        return "unassigned"

    def _register_locked(self, worker: dict[str, Any]) -> dict[str, Any]:
        tags = [t for t in (worker.get("tags") or []) if isinstance(t, str)]
        tag_map: dict[str, str] = {}
        for t in tags:
            if ":" in t:
                k, v = t.split(":", 1)
                tag_map[k] = v
        raw_id = str(worker.get("id") or tag_map.get("id") or "").strip() or None
        raw_name = str(worker.get("name") or "").strip() or None

        for key in (raw_id, raw_name):
            if key and key in self._by_raw:
                return self._by_raw[key]

        participant = self._resolve_participant_locked(raw_id, raw_name, tags)
        seq = self._per_participant_seq.get(participant, 0) + 1
        self._per_participant_seq[participant] = seq
        worker_label = f"{participant}-w{seq}"

        slots_raw = tag_map.get("slots")
        info = {
            "worker_label": worker_label,
            "participant": participant,
            "os": worker.get("os") or tag_map.get("os") or "unknown",
            "arch": worker.get("arch") or tag_map.get("arch") or "unknown",
            "slots": int(slots_raw) if slots_raw and slots_raw.isdigit() else None,
            "isolation": tag_map.get("iso") or worker.get("isolation") or "off",
            "link": tag_map.get("link") or worker.get("link"),
            "protocol": tag_map.get("proto"),
            "cpu_cores": worker.get("cpu_cores"),
            "ram_mb": worker.get("ram_mb"),
            "has_gpu": worker.get("has_gpu"),
        }
        self._records.append(info)
        self._known_labels.add(worker_label)
        for key in (raw_id, raw_name):
            if key:
                self._by_raw[key] = info
                if len(key) >= 2:
                    self._secret_map[key] = worker_label
        return info

    def ensure_worker(
        self,
        raw_id: str | None = None,
        raw_name: str | None = None,
        os_name: str | None = None,
        arch: str | None = None,
    ) -> tuple[str, str]:
        """Return `(worker_label, participant)` for raw identifiers, registering dynamically if needed."""
        with self._lock:
            for key in (raw_id, raw_name):
                if key and key in self._by_raw:
                    rec = self._by_raw[key]
                    return rec["worker_label"], rec["participant"]
            if not raw_id and not raw_name:
                return "unassigned-w0", "unassigned"
            rec = self._register_locked({
                "id": raw_id,
                "name": raw_name,
                "os": os_name or "unknown",
                "arch": arch or "unknown",
                "tags": [],
            })
            return rec["worker_label"], rec["participant"]

    def snapshot_workers(self) -> list[dict[str, Any]]:
        with self._lock:
            return [copy.deepcopy(r) for r in self._records]

    def scrub(self, text: str | None) -> str:
        with self._lock:
            secret_map = dict(self._secret_map)
            known_labels = set(self._known_labels)
        return scrub_text(text, secret_map, known_labels=known_labels)


def strip_cli_hints(text: str | None) -> str:
    """Drop interactive CLI lines that repeat `--master` hosts or replica worker lists."""
    if not text:
        return ""
    kept: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        lower = s.lower()
        if lower.startswith("hint:"):
            continue
        if lower.startswith("verifying on ") and "other worker(s):" in lower:
            continue
        kept.append(line)
    return "\n".join(kept)


def scrub_text(
    text: str | None,
    secret_map: dict[str, str],
    *,
    known_labels: set[str] | None = None,
) -> str:
    """Replace worker names (known & unknown), `--master` hosts, UUIDs, IPs, URLs, and `oxs1-*` tokens."""
    out = text or ""
    labels = known_labels or set()
    for raw in sorted(secret_map, key=len, reverse=True):
        if len(raw) < 2:
            continue
        if re.match(r"^[\w-]+$", raw):
            out = re.sub(r"(?<![\w-])%s(?![\w-])" % re.escape(raw), secret_map[raw], out)
        else:
            out = out.replace(raw, secret_map[raw])

    def _redact_worker(prefix_group: str, candidate: str, suffix_group: str) -> str:
        clean = candidate.rstrip(":.,;)")
        if clean in labels or clean in ("<worker>", "<id>"):
            return prefix_group + candidate + suffix_group
        return prefix_group + "<worker>" + suffix_group

    out = REPLICA_WORKER_RE.sub(
        lambda m: _redact_worker(m.group(1), m.group(2), m.group(3)), out
    )
    out = NOTE_FIRST_WORKER_RE.sub(
        lambda m: _redact_worker(m.group(1), m.group(2), m.group(3)), out
    )
    out = WORKER_LABEL_LINE_RE.sub(
        lambda m: _redact_worker(m.group(1), m.group(2), ""), out
    )
    out = MASTER_AT_RE.sub(lambda m: m.group(1) + "<master>", out)
    out = MASTER_PAREN_RE.sub(lambda m: m.group(1) + "<master>" + m.group(3), out)
    out = URL_RE.sub("<url>", out)
    out = UUID_RE.sub("<id>", out)
    out = OXS_TOKEN_RE.sub("<token>", out)
    out = IPV4_RE.sub("<address>", out)
    out = IPV6_BRACKET_RE.sub("<address>", out)
    return out


def sanitize_doctor(doc: dict[str, Any] | None, directory: WorkerDirectory) -> dict[str, Any]:
    """Sanitize `oxideswarm doctor --json` output without hostnames, worker names, UUIDs, or IPs."""
    if not isinstance(doc, dict):
        return {"workers": [], "checks": []}
    if doc.get("master"):
        directory.add_secret(str(doc["master"]), "<master>")
    workers_out: list[dict[str, Any]] = []
    for w in doc.get("workers") or []:
        if not isinstance(w, dict):
            continue
        label, participant = directory.ensure_worker(
            raw_id=str(w.get("id") or "") or None,
            raw_name=str(w.get("name") or "") or None,
            os_name=w.get("os"),
            arch=w.get("arch"),
        )
        workers_out.append({
            "worker_label": label,
            "participant": participant,
            "os": w.get("os"),
            "arch": w.get("arch"),
            "cores": w.get("cores"),
            "ram_mb": w.get("ram_mb"),
            "isolation": w.get("isolation"),
            "link": w.get("link"),
            "link_kind": w.get("link_kind"),
            "link_rtt_ms": w.get("link_rtt_ms"),
        })
    checks_out: list[dict[str, Any]] = []
    for c in doc.get("checks") or []:
        if not isinstance(c, dict):
            continue
        worker_label = None
        if c.get("worker"):
            worker_label, _ = directory.ensure_worker(raw_name=str(c.get("worker")))
        checks_out.append({
            "id": c.get("id"),
            "name": c.get("name"),
            "outcome": c.get("outcome"),
            "worker_label": worker_label,
            "detail": directory.scrub(strip_cli_hints(str(c.get("detail") or ""))),
        })
    return {"workers": workers_out, "checks": checks_out}


def sanitize_verify_report(
    raw_verify: dict[str, Any] | None,
    directory: WorkerDirectory,
) -> dict[str, Any] | None:
    """Convert raw `submit --json` `verify` field into a label-only report."""
    if not isinstance(raw_verify, dict):
        return None
    status = raw_verify.get("status")
    if status == "error":
        return {
            "status": "error",
            "error": directory.scrub(strip_cli_hints(str(raw_verify.get("error") or ""))),
        }
    workers_out: list[dict[str, Any]] = []
    for w in raw_verify.get("workers") or []:
        if not isinstance(w, dict):
            continue
        label, participant = directory.ensure_worker(
            raw_id=str(w.get("id") or "") or None,
            raw_name=str(w.get("name") or "") or None,
            os_name=w.get("os"),
            arch=w.get("arch"),
        )
        workers_out.append({
            "worker_label": label,
            "participant": participant,
            "os": w.get("os"),
            "arch": w.get("arch"),
        })
    differing_out: list[dict[str, Any]] = []
    for diff in raw_verify.get("differing") or []:
        if not isinstance(diff, dict):
            continue
        digests_out: list[dict[str, Any]] = []
        for entry in diff.get("workers") or []:
            if not isinstance(entry, dict):
                continue
            label, participant = directory.ensure_worker(raw_name=str(entry.get("worker") or "") or None)
            digests_out.append({
                "worker_label": label,
                "participant": participant,
                "blake3": entry.get("blake3"),
            })
        differing_out.append({
            "path": diff.get("path"),
            "digests": digests_out,
        })
    return {
        "status": status,
        "replicas": raw_verify.get("replicas"),
        "workers": workers_out,
        "compared": list(raw_verify.get("compared") or []),
        "differing": differing_out,
    }


def _collect_doc_worker_refs(doc: dict[str, Any] | None) -> list[tuple[str | None, str | None]]:
    refs: list[tuple[str | None, str | None]] = []
    if not isinstance(doc, dict):
        return refs
    wid = str(doc.get("worker_id") or "").strip() or None
    wname = str(doc.get("worker_name") or "").strip() or None
    if wid or wname:
        refs.append((wid, wname))
    verify = doc.get("verify")
    if isinstance(verify, dict):
        for w in verify.get("workers") or []:
            if isinstance(w, dict):
                r_id = str(w.get("id") or "").strip() or None
                r_name = str(w.get("name") or "").strip() or None
                if r_id or r_name:
                    refs.append((r_id, r_name))
        for diff in verify.get("differing") or []:
            if isinstance(diff, dict):
                for entry in diff.get("workers") or []:
                    if isinstance(entry, dict):
                        r_name = str(entry.get("worker") or "").strip() or None
                        if r_name:
                            refs.append((None, r_name))
    return refs


def extract_task_id(doc: dict[str, Any] | None, stdout: str, stderr: str) -> str | None:
    """Extract task UUID from `submit --json` output or timeout diagnostics if present."""
    if isinstance(doc, dict):
        tid = doc.get("task_id")
        if isinstance(tid, str) and UUID_RE.fullmatch(tid.strip()):
            return tid.strip()
    for text in (stdout, stderr):
        if text:
            m = TASK_ID_LINE_RE.search(text)
            if m:
                return m.group(1)
    return None


def build_submit_command(
    binary: str,
    unit: dict[str, Any],
    *,
    master: str | None = None,
    relay_url: str | None = None,
    config_file: str | None = None,
    insecure: bool = False,
    output_dir: str | None = None,
    extra_inputs: list[str] | None = None,
    require_isolation_override: str | None = None,
) -> list[str]:
    """Build an exact `oxideswarm submit --json --wait --verify ...` CLI invocation."""
    cmd = [binary, "submit"]
    if master:
        cmd.extend(["--master", master])
    if relay_url:
        cmd.extend(["--relay-url", relay_url])
    if config_file:
        cmd.extend(["--config", config_file])
    if insecure:
        cmd.append("--insecure")

    cmd.extend(["--json", "--wait", "--verify", str(unit["verify"])])
    if unit.get("verify_file"):
        cmd.extend(["--verify-file", unit["verify_file"]])
    if unit.get("verify_any_system"):
        cmd.append("--verify-any-system")

    iso = require_isolation_override if require_isolation_override is not None else unit.get("require_isolation")
    if iso:
        cmd.extend(["--require-isolation", iso])
    for tag in unit.get("require_tags") or []:
        cmd.extend(["--require-tag", tag])
    if unit.get("os"):
        cmd.extend(["--os", unit["os"]])
    if unit.get("arch"):
        cmd.extend(["--arch", unit["arch"]])
    if unit.get("timeout_secs"):
        cmd.extend(["--timeout-secs", str(unit["timeout_secs"])])

    for k in sorted(unit.get("env") or {}):
        cmd.extend(["--env", f"{k}={unit['env'][k]}"])

    seen_inputs: set[str] = set()
    for spec in list(extra_inputs or []) + list(unit.get("inputs") or []):
        if spec not in seen_inputs:
            seen_inputs.add(spec)
            cmd.extend(["--input", spec])

    for pattern in unit.get("outputs") or []:
        cmd.extend(["--output", pattern])

    if output_dir:
        cmd.extend(["--output-dir", output_dir])

    cmd.extend(["--command", unit["command"]])
    if unit.get("args"):
        cmd.append("--")
        cmd.extend(unit["args"])
    return cmd


def classify_attempt(
    cli_exit_code: int,
    doc: dict[str, Any] | None,
    expected_verify: int,
    *,
    interrupted: bool = False,
) -> str:
    """Classify one `submit` invocation without confusing a task exit code 4 with mismatch."""
    if interrupted or cli_exit_code < 0 or cli_exit_code in (130, 143):
        return "interrupted"
    verify = doc.get("verify") if isinstance(doc, dict) else None
    v_status = verify.get("status") if isinstance(verify, dict) else None
    if v_status == "mismatch":
        return "mismatch"
    if v_status == "error":
        return "verify_error"
    if cli_exit_code == 0 and (expected_verify == 0 or v_status == "verified"):
        if isinstance(doc, dict) and doc.get("exit_code", 0) == 0 and not doc.get("error"):
            return "verified"
    return "failed"


def cancel_task_on_master(
    binary: str,
    task_id: str,
    sub_env: dict[str, str],
    *,
    master: str | None = None,
    relay_url: str | None = None,
    config_file: str | None = None,
    insecure: bool = False,
) -> bool:
    """Best-effort `oxideswarm cancel <task_id> --json` for interrupted/timed-out attempts."""
    cmd = [binary, "cancel"]
    if master:
        cmd.extend(["--master", master])
    if relay_url:
        cmd.extend(["--relay-url", relay_url])
    if config_file:
        cmd.extend(["--config", config_file])
    if insecure:
        cmd.append("--insecure")
    cmd.extend([task_id, "--json"])
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=sub_env,
            stdin=subprocess.DEVNULL,
            timeout=15,
        )
        return proc.returncode == 0
    except Exception:
        return False


class _ActiveProcRegistry:
    """Thread-safe tracker allowing the interrupt handler to terminate active child groups."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[int, tuple[subprocess.Popen[str], threading.Event]] = {}

    def register(self, proc: subprocess.Popen[str], aborted_event: threading.Event) -> None:
        with self._lock:
            self._entries[id(proc)] = (proc, aborted_event)

    def unregister(self, proc: subprocess.Popen[str]) -> None:
        with self._lock:
            self._entries.pop(id(proc), None)

    def terminate_all(self) -> int:
        with self._lock:
            snapshot = list(self._entries.values())
        count = 0
        for proc, aborted_event in snapshot:
            if proc.poll() is not None:
                continue
            aborted_event.set()
            count += 1
            try:
                if os.name != "nt" and hasattr(os, "killpg"):
                    os.killpg(proc.pid, signal.SIGTERM)
                else:
                    proc.terminate()
            except OSError:
                pass
        for proc, _ in snapshot:
            if proc.poll() is not None:
                continue
            try:
                proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                try:
                    if os.name != "nt" and hasattr(os, "killpg"):
                        os.killpg(proc.pid, signal.SIGKILL)
                    else:
                        proc.kill()
                except OSError:
                    pass
        return count

    def active_count(self) -> int:
        with self._lock:
            return len(self._entries)


def execute_submit_attempt(
    binary: str,
    unit: dict[str, Any],
    attempt_num: int,
    reason: str,
    directory: WorkerDirectory,
    sub_env: dict[str, str],
    *,
    master: str | None = None,
    relay_url: str | None = None,
    config_file: str | None = None,
    insecure: bool = False,
    base_output_dir: str | None = None,
    extra_inputs: list[str] | None = None,
    require_isolation_override: str | None = None,
    proc_registry: _ActiveProcRegistry | None = None,
    force_stop_event: threading.Event | None = None,
    submit_timeout_override: float | None = None,
    orphan_recorder: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    """Invoke `oxideswarm submit` once in its own session and return a sanitized attempt record."""
    attempt_out_dir = (
        os.path.join(base_output_dir, unit["id"], f"attempt-{attempt_num}")
        if base_output_dir
        else None
    )
    if attempt_out_dir:
        os.makedirs(attempt_out_dir, exist_ok=True)

    cmd = build_submit_command(
        binary,
        unit,
        master=master,
        relay_url=relay_url,
        config_file=config_file,
        insecure=insecure,
        output_dir=attempt_out_dir,
        extra_inputs=extra_inputs,
        require_isolation_override=require_isolation_override,
    )
    started_utc = utc_now_iso()
    t0 = time.perf_counter()
    if submit_timeout_override is not None:
        timeout_limit = float(submit_timeout_override)
    else:
        unit_to = unit.get("timeout_secs") or 1800
        margin = min(180, max(2, unit_to))
        timeout_limit = unit_to * (1 + unit.get("verify", 1)) + margin

    popen_kwargs: dict[str, Any] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "env": sub_env,
        "stdin": subprocess.DEVNULL,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
    else:
        popen_kwargs["start_new_session"] = True

    aborted_event = threading.Event()
    timed_out = False
    stdout = ""
    stderr = ""
    cli_rc = 1

    proc = subprocess.Popen(cmd, **popen_kwargs)
    if proc_registry is not None:
        proc_registry.register(proc, aborted_event)
    try:
        if force_stop_event is not None and force_stop_event.is_set():
            aborted_event.set()
            try:
                proc.terminate()
            except OSError:
                pass
        try:
            stdout, stderr = proc.communicate(timeout=timeout_limit)
            cli_rc = proc.returncode if proc.returncode is not None else 1
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                if os.name != "nt" and hasattr(os, "killpg"):
                    os.killpg(proc.pid, signal.SIGKILL)
                else:
                    proc.kill()
            except OSError:
                pass
            stdout_rem, stderr_rem = proc.communicate()
            stdout = stdout_rem or ""
            stderr = (stderr_rem or "") + f"\nsubprocess timed out after {timeout_limit}s"
            cli_rc = 124
    finally:
        if proc_registry is not None:
            proc_registry.unregister(proc)

    dt = round(time.perf_counter() - t0, 4)
    doc = None
    if stdout and stdout.strip():
        try:
            parsed = json.loads(stdout)
            if isinstance(parsed, dict):
                doc = parsed
        except ValueError:
            doc = None

    was_interrupted = aborted_event.is_set() or cli_rc < 0 or cli_rc in (130, 143)
    outcome = classify_attempt(cli_rc, doc, unit["verify"], interrupted=was_interrupted)

    # Refresh `WorkerDirectory` if a late-joining worker appears or before scrubbing errors.
    unknown_ref = any(not directory.has_worker(rid, rname) for rid, rname in _collect_doc_worker_refs(doc))
    if unknown_ref or outcome in ("failed", "verify_error"):
        directory.maybe_refresh()

    primary_label, primary_participant = (
        directory.ensure_worker(
            raw_id=str(doc.get("worker_id") or "") or None if doc else None,
            raw_name=str(doc.get("worker_name") or "") or None if doc else None,
        )
        if doc and (doc.get("worker_id") or doc.get("worker_name"))
        else (None, None)
    )
    sanitized_verify = sanitize_verify_report(doc.get("verify") if doc else None, directory)

    is_timeout = timed_out or (isinstance(doc, dict) and doc.get("status") == "timeout")
    cancelled_on_master: bool | None = None
    if outcome == "interrupted" or is_timeout:
        tid = extract_task_id(doc, stdout, stderr)
        if tid:
            ok = cancel_task_on_master(
                binary,
                tid,
                sub_env,
                master=master,
                relay_url=relay_url,
                config_file=config_file,
                insecure=insecure,
            )
            cancelled_on_master = ok
            if not ok and orphan_recorder is not None:
                orphan_recorder(unit["id"], tid)
        elif outcome == "interrupted":
            print(
                f"warning: [{unit['id']}] interrupted before submit printed task_id; "
                "check `oxideswarm status` before --resume if an orphaned task may still be queued or running",
                file=sys.stderr,
                flush=True,
            )

    error_parts = []
    if doc and doc.get("error"):
        error_parts.append(strip_cli_hints(str(doc["error"])))
    if doc and doc.get("artifact_save_error"):
        error_parts.append(f"artifact_save_error: {doc['artifact_save_error']}")
    cleaned_stderr = strip_cli_hints(stderr)
    if outcome in ("failed", "verify_error", "interrupted") and cleaned_stderr.strip():
        error_parts.append(cleaned_stderr.strip()[:600])
    error_text = directory.scrub(" | ".join(p for p in error_parts if p)) if error_parts else None

    attempt_rec: dict[str, Any] = {
        "attempt": attempt_num,
        "reason": reason,
        "started_at_utc": started_utc,
        "duration_s": dt,
        "cli_exit_code": cli_rc,
        "task_exit_code": doc.get("exit_code") if isinstance(doc, dict) else None,
        "execution_time_ms": doc.get("execution_time_ms") if isinstance(doc, dict) else None,
        "waited_ms": doc.get("waited_ms") if isinstance(doc, dict) else None,
        "outcome": outcome,
        "primary_worker_label": primary_label,
        "primary_participant": primary_participant,
        "verify": sanitized_verify,
        "error": error_text,
    }
    if cancelled_on_master is not None:
        attempt_rec["cancelled_on_master"] = cancelled_on_master
    return attempt_rec


def run_unit_with_mismatch_rerun(
    binary: str,
    unit: dict[str, Any],
    directory: WorkerDirectory,
    sub_env: dict[str, str],
    *,
    stop_event: threading.Event | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Run one work unit; on verification mismatch, rerun once unless interrupted."""
    attempts: list[dict[str, Any]] = []
    a1 = execute_submit_attempt(binary, unit, 1, "initial", directory, sub_env, **kwargs)
    attempts.append(a1)

    if a1["outcome"] == "interrupted":
        status = "interrupted"
        had_mismatch = False
        needs_investigation = False
    elif a1["outcome"] == "verified":
        status = "verified"
        had_mismatch = False
        needs_investigation = False
    elif a1["outcome"] == "mismatch":
        had_mismatch = True
        needs_investigation = True
        if stop_event is not None and stop_event.is_set():
            status = "interrupted"
        elif unit.get("mismatch_retries", 1) >= 1:
            a2 = execute_submit_attempt(
                binary, unit, 2, "mismatch_rerun", directory, sub_env, **kwargs
            )
            attempts.append(a2)
            if a2["outcome"] == "interrupted":
                status = "interrupted"
            elif a2["outcome"] == "verified":
                status = "verified_on_rerun"
            elif a2["outcome"] == "mismatch":
                status = "mismatch_persisted"
            else:
                status = "mismatch_rerun_failed"
        else:
            status = "mismatch_persisted"
    elif a1["outcome"] == "verify_error":
        status = "verify_error"
        had_mismatch = False
        needs_investigation = True
    else:
        status = "failed"
        had_mismatch = False
        needs_investigation = True

    return {
        "id": unit["id"],
        "seed": unit.get("seed"),
        "params": unit.get("params") or {},
        "status": status,
        "had_mismatch": had_mismatch,
        "needs_investigation": needs_investigation,
        "attempts": attempts,
    }


def compute_summary(unit_records: list[dict[str, Any]], units_total: int) -> dict[str, Any]:
    counts = {
        "verified": 0,
        "verified_on_rerun": 0,
        "mismatch_persisted": 0,
        "mismatch_rerun_failed": 0,
        "verify_error": 0,
        "failed": 0,
        "interrupted": 0,
    }
    total_attempts = 0
    total_mismatch_attempts = 0
    needs_investigation = 0
    for rec in unit_records:
        st = rec.get("status")
        if st in counts:
            counts[st] += 1
        if rec.get("needs_investigation"):
            needs_investigation += 1
        for a in rec.get("attempts") or []:
            total_attempts += 1
            if a.get("outcome") == "mismatch":
                total_mismatch_attempts += 1
    not_recorded = max(0, units_total - len(unit_records))
    return {
        "units_total": units_total,
        "units_recorded": len(unit_records),
        "verified": counts["verified"],
        "verified_on_rerun": counts["verified_on_rerun"],
        "mismatch_persisted": counts["mismatch_persisted"],
        "mismatch_rerun_failed": counts["mismatch_rerun_failed"],
        "verify_error": counts["verify_error"],
        "failed": counts["failed"],
        "interrupted": counts["interrupted"],
        "pending": not_recorded + counts["interrupted"] + counts["verify_error"] + counts["mismatch_rerun_failed"],
        "total_attempts": total_attempts,
        "total_mismatch_attempts": total_mismatch_attempts,
        "units_needing_investigation": needs_investigation,
    }


def compute_per_participant(
    unit_records: list[dict[str, Any]],
    directory_workers: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Aggregate execution and verification counts per participant label (`P1`, `P2`, ...)."""
    buckets: dict[str, dict[str, Any]] = {}

    def ensure_bucket(participant: str) -> dict[str, Any]:
        if participant not in buckets:
            buckets[participant] = {
                "machines": 0,
                "workers": [],
                "primary_tasks": 0,
                "verify_replicas": 0,
                "total_executions": 0,
                "verified_units": 0,
                "mismatch_attempts": 0,
                "failed_attempts": 0,
                "execution_time_ms_sum": 0,
                "_worker_set": set(),
                "_verified_unit_ids": set(),
            }
        return buckets[participant]

    for w in directory_workers:
        p = str(w.get("participant") or "unassigned")
        b = ensure_bucket(p)
        wl = w.get("worker_label")
        if wl:
            b["_worker_set"].add(str(wl))

    for rec in unit_records:
        unit_id = rec.get("id")
        unit_verified = rec.get("status") in ("verified", "verified_on_rerun")
        for a in rec.get("attempts") or []:
            p_primary = a.get("primary_participant")
            w_primary = a.get("primary_worker_label")
            attempt_participants: set[str] = set()
            if p_primary:
                b = ensure_bucket(str(p_primary))
                b["primary_tasks"] += 1
                if w_primary:
                    b["_worker_set"].add(str(w_primary))
                exec_ms = a.get("execution_time_ms")
                if isinstance(exec_ms, int) and not isinstance(exec_ms, bool) and exec_ms >= 0:
                    b["execution_time_ms_sum"] += exec_ms
                if a.get("outcome") in ("failed", "verify_error"):
                    b["failed_attempts"] += 1
                attempt_participants.add(str(p_primary))

            verify = a.get("verify")
            if isinstance(verify, dict):
                v_workers = verify.get("workers") or []
                # In `crates/cli/src/verify.rs` (`Report::to_json`), index 0 is the first/primary
                # worker and indices 1.. are the verification replicas.
                for replica in v_workers[1:]:
                    if not isinstance(replica, dict):
                        continue
                    rp = str(replica.get("participant") or "unassigned")
                    rw = replica.get("worker_label")
                    rb = ensure_bucket(rp)
                    rb["verify_replicas"] += 1
                    if rw:
                        rb["_worker_set"].add(str(rw))
                    attempt_participants.add(rp)
                if a.get("outcome") == "mismatch":
                    mismatch_parts: set[str] = set(attempt_participants)
                    for diff in verify.get("differing") or []:
                        for d_entry in diff.get("digests") or []:
                            if isinstance(d_entry, dict) and d_entry.get("participant"):
                                mismatch_parts.add(str(d_entry["participant"]))
                    for mp in mismatch_parts:
                        ensure_bucket(mp)["mismatch_attempts"] += 1
            elif a.get("outcome") == "mismatch" and p_primary:
                ensure_bucket(str(p_primary))["mismatch_attempts"] += 1

            if unit_verified and a.get("outcome") == "verified":
                for ap in attempt_participants:
                    ensure_bucket(ap)["_verified_unit_ids"].add(unit_id)

    out: dict[str, dict[str, Any]] = {}
    for participant in sorted(buckets):
        b = buckets[participant]
        workers_sorted = sorted(b.pop("_worker_set"))
        verified_units = len(b.pop("_verified_unit_ids"))
        b["workers"] = workers_sorted
        b["machines"] = len(workers_sorted)
        b["verified_units"] = verified_units
        b["total_executions"] = b["primary_tasks"] + b["verify_replicas"]
        out[participant] = b
    return out


def atomic_write_json(path: str | os.PathLike[str], payload: dict[str, Any]) -> None:
    target = os.path.abspath(path)
    parent = os.path.dirname(target)
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".record-", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _orphans_sidecar_path(record_path: str | os.PathLike[str]) -> str:
    return os.path.abspath(record_path) + ".orphans.json"


def _load_orphans(record_path: str | os.PathLike[str]) -> dict[str, list[str]]:
    sidecar = _orphans_sidecar_path(record_path)
    if not os.path.exists(sidecar):
        return {}
    try:
        with open(sidecar, encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        out: dict[str, list[str]] = {}
        for k, v in raw.items():
            if isinstance(k, str) and isinstance(v, list):
                tids = [t for t in v if isinstance(t, str) and UUID_RE.fullmatch(t)]
                if tids:
                    out[k] = tids
        return out
    except (OSError, ValueError):
        return {}


def _save_orphans(record_path: str | os.PathLike[str], orphans: dict[str, list[str]]) -> None:
    sidecar = _orphans_sidecar_path(record_path)
    if not orphans:
        try:
            os.unlink(sidecar)
        except OSError:
            pass
        return
    atomic_write_json(sidecar, orphans)


def load_resume_units(
    record_path: str | os.PathLike[str],
    campaign_id: str,
    *,
    retry_failed: bool = False,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load completed units from an existing record file for `--resume`.

    Always excludes `interrupted`, `verify_error`, and `mismatch_rerun_failed`
    so a plain `--resume` resubmits them automatically. Excludes `failed` when
    `retry_failed=True`.
    """
    with open(record_path, encoding="utf-8") as f:
        existing = json.load(f)
    if not isinstance(existing, dict):
        raise ValueError("existing record must be a JSON object")
    if existing.get("campaign_id") != campaign_id:
        raise ValueError(
            f"record campaign_id {existing.get('campaign_id')!r} != manifest {campaign_id!r}"
        )
    keep = set(FINISHED_STATUSES_DEFAULT)
    if not retry_failed:
        keep.add("failed")
    retained: dict[str, dict[str, Any]] = {}
    for rec in existing.get("units") or []:
        if isinstance(rec, dict) and isinstance(rec.get("id"), str) and rec.get("status") in keep:
            retained[rec["id"]] = rec
    return retained, existing


def _cluster_env(secret_file: str | None) -> dict[str, str]:
    env = dict(os.environ)
    if secret_file:
        env["OXIDESWARM_SECRET_FILE"] = os.path.abspath(secret_file)
    return env


def _base_cluster_args(args: argparse.Namespace) -> list[str]:
    out: list[str] = []
    if args.master:
        out.extend(["--master", args.master])
    if args.relay_url:
        out.extend(["--relay-url", args.relay_url])
    if args.config:
        out.extend(["--config", args.config])
    if args.insecure:
        out.append("--insecure")
    return out


def fetch_workers_list(
    binary: str,
    common_args: list[str],
    sub_env: dict[str, str],
) -> list[dict[str, Any]]:
    r_workers = subprocess.run(
        [binary, "workers", *common_args, "--json"],
        capture_output=True,
        text=True,
        env=sub_env,
        stdin=subprocess.DEVNULL,
        timeout=60,
    )
    if r_workers.returncode != 0:
        raise RuntimeError(
            f"`oxideswarm workers --json` failed (exit {r_workers.returncode}): {strip_cli_hints(r_workers.stderr).strip()}"
        )
    workers_raw = json.loads(r_workers.stdout)
    if not isinstance(workers_raw, list):
        raise RuntimeError("`oxideswarm workers --json` did not return a list")
    return workers_raw


def fetch_cluster_facts(
    args: argparse.Namespace,
    sub_env: dict[str, str],
    participant_map: dict[str, str],
) -> tuple[WorkerDirectory, dict[str, Any] | None]:
    common = _base_cluster_args(args)
    workers_raw = fetch_workers_list(args.bin, common, sub_env)
    directory = WorkerDirectory(
        workers_raw,
        participant_map,
        refresh_callback=lambda: fetch_workers_list(args.bin, common, sub_env),
    )
    directory.register_endpoint_secrets(
        master=args.master, relay_url=args.relay_url, env=sub_env
    )

    doctor_data: dict[str, Any] | None = None
    if not args.skip_doctor:
        doc_cmd = [args.bin, "doctor", *common, "--json"]
        if args.expect_workers:
            doc_cmd.extend(["--expect-workers", str(args.expect_workers)])
        r_doc = subprocess.run(
            doc_cmd,
            capture_output=True,
            text=True,
            env=sub_env,
            stdin=subprocess.DEVNULL,
            timeout=120,
        )
        try:
            doc_parsed = json.loads(r_doc.stdout) if r_doc.stdout.strip() else None
        except ValueError:
            doc_parsed = None
        if isinstance(doc_parsed, dict):
            doctor_data = sanitize_doctor(doc_parsed, directory)
        else:
            cleaned = strip_cli_hints(r_doc.stderr).strip()[:200]
            doctor_data = {
                "error": directory.scrub(f"doctor exited {r_doc.returncode}: {cleaned}")
            }
    return directory, doctor_data


def _build_record_payload(
    manifest: dict[str, Any],
    unit_order: list[str],
    units_by_id: dict[str, dict[str, Any]],
    directory: WorkerDirectory,
    doctor_data: dict[str, Any] | None,
    *,
    started_at: str,
    resumed_at: list[str],
    finished_at: str | None,
    interrupted: bool,
    runner_sha256: str | None,
    runner_system: str | None,
) -> dict[str, Any]:
    ordered_records = [units_by_id[uid] for uid in unit_order if uid in units_by_id]
    workers_snapshot = directory.snapshot_workers()
    workload_rec = {
        "name": manifest["workload"].get("name"),
        "commit": manifest["workload"].get("commit"),
        "deterministic": manifest["workload"].get("deterministic"),
        "gpu": manifest["workload"].get("gpu"),
    }
    if runner_sha256:
        workload_rec["runner_sha256"] = runner_sha256
    if runner_system:
        workload_rec["runner_system"] = runner_system
    return {
        "record_format": RECORD_FORMAT,
        "campaign_id": manifest["campaign_id"],
        "workload": workload_rec,
        "started_at_utc": started_at,
        "resumed_at_utc": list(resumed_at),
        "updated_at_utc": utc_now_iso(),
        "finished_at_utc": finished_at,
        "interrupted": interrupted,
        "workers": workers_snapshot,
        "doctor": doctor_data,
        "summary": compute_summary(ordered_records, len(unit_order)),
        "per_participant": compute_per_participant(ordered_records, workers_snapshot),
        "units": ordered_records,
    }


def _default_record_path(manifest_path: str) -> str:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(manifest_path)), "results")
    return os.path.join(results_dir, f"{stamp}-campaign.json")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", required=True, help="Campaign manifest JSON file")
    ap.add_argument("--bin", default="oxideswarm", help="oxideswarm executable (default: oxideswarm)")
    ap.add_argument("--master", help="Master address host:port (passed to oxideswarm --master)")
    ap.add_argument("--relay-url", help="Relay URL passed to oxideswarm --relay-url")
    ap.add_argument("--config", help="Config file passed to oxideswarm --config")
    ap.add_argument("--secret-file", help="Cluster secret file (passed via OXIDESWARM_SECRET_FILE)")
    ap.add_argument("--insecure", action="store_true", help="Pass --insecure to oxideswarm subcommands")
    ap.add_argument("--runner", help="Local runner binary path; verified against manifest SHA-256 and staged")
    ap.add_argument(
        "--runner-system",
        help="OS/arch key (e.g. macos/aarch64); pins every submitted unit with --os/--arch",
    )
    ap.add_argument("--os", dest="target_os", help="Explicitly pin every unit with `submit --os <OS>`")
    ap.add_argument("--arch", dest="target_arch", help="Explicitly pin every unit with `submit --arch <ARCH>`")
    ap.add_argument(
        "--shard",
        help="Run a disjoint 1-based slice K/N of manifest units (e.g. 1/2 for macOS, 2/2 for Windows)",
    )
    ap.add_argument("--command", dest="command_override", help="Override unit command (e.g. /usr/bin/python3)")
    ap.add_argument(
        "--extra-input",
        action="append",
        default=[],
        help="Additional --input LOCAL[:SANDBOX] spec (repeatable; secret-checked)",
    )
    ap.add_argument("--participants", help="JSON file mapping worker names or IDs to P1..P5 labels")
    ap.add_argument("--output-dir", help="Local directory where verified unit outputs are saved")
    ap.add_argument("--record", help="Output JSON record path (default: <manifest_dir>/results/<date>-campaign.json)")
    ap.add_argument("--concurrency", type=int, default=4, help="Max concurrent unit submissions (default: 4)")
    ap.add_argument("--limit", type=int, help="Run at most N pending units in this invocation")
    ap.add_argument("--expect-workers", type=int, help="Pass --expect-workers N to `doctor --json`")
    ap.add_argument("--skip-doctor", action="store_true", help="Skip `oxideswarm doctor --json` before running")
    ap.add_argument("--require-isolation", help="Override `defaults.require_isolation` for every unit")
    ap.add_argument(
        "--interrupt-grace-secs",
        type=float,
        default=5.0,
        help="Seconds to let in-flight units finish on first Ctrl-C before terminating (default: 5.0)",
    )
    ap.add_argument(
        "--submit-timeout-secs",
        type=float,
        help="Override driver wait watchdog timeout in seconds for each `submit` child process",
    )
    ap.add_argument(
        "--allow-blocked-workload",
        action="store_true",
        help="Allow live run even when manifest.workload.runner_ready is false",
    )
    ap.add_argument("--resume", action="store_true", help="Skip completed units in --record (always resubmits interrupted/verify_error)")
    ap.add_argument("--retry-failed", action="store_true", help="With --resume, also retry units in state `failed`")
    ap.add_argument("--dry-run", action="store_true", help="Validate manifest and print planned commands without running")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.concurrency < 1:
        print("error: --concurrency must be >= 1", file=sys.stderr)
        return 2
    if args.limit is not None and args.limit < 1:
        print("error: --limit must be >= 1", file=sys.stderr)
        return 2
    if args.interrupt_grace_secs < 0:
        print("error: --interrupt-grace-secs must be >= 0", file=sys.stderr)
        return 2
    if args.submit_timeout_secs is not None and args.submit_timeout_secs <= 0:
        print("error: --submit-timeout-secs must be > 0", file=sys.stderr)
        return 2

    try:
        manifest = load_manifest(args.manifest)
        participant_map = load_participant_map(args.participants)

        pin_os = normalize_os(args.target_os) if args.target_os else None
        pin_arch = normalize_arch(args.target_arch) if args.target_arch else None
        normalized_runner_system: str | None = None
        if args.runner_system:
            sys_os, sys_arch = parse_system_key(args.runner_system)
            if pin_os and pin_os != sys_os:
                raise ValueError(f"--os {pin_os!r} conflicts with --runner-system {args.runner_system!r}")
            if pin_arch and pin_arch != sys_arch:
                raise ValueError(f"--arch {pin_arch!r} conflicts with --runner-system {args.runner_system!r}")
            pin_os, pin_arch = sys_os, sys_arch
            normalized_runner_system = f"{sys_os}/{sys_arch}"

        validate_no_secret_inputs(list(args.extra_input), "--extra-input", base_dir=os.getcwd())
        extra_inputs: list[str] = []
        for spec in args.extra_input:
            local, sandbox = _input_spec_parts(spec.strip())
            if not os.path.isabs(local):
                local = os.path.normpath(os.path.abspath(local))
            extra_inputs.append(f"{local}:{sandbox}" if sandbox else local)

        runner_sha256: str | None = None
        if args.runner:
            runner_spec, runner_sha256 = verify_runner_hash(
                manifest["workload"], args.runner, args.runner_system
            )
            extra_inputs.append(runner_spec)

        apply_unit_overrides(
            manifest["units"],
            pin_os=pin_os,
            pin_arch=pin_arch,
            command_override=args.command_override,
        )

        selected_units = list(manifest["units"])
        if args.shard:
            shard_idx, shard_total = parse_shard_spec(args.shard)
            selected_units = slice_shard(selected_units, shard_idx, shard_total)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    workload_ready = manifest["workload"].get("runner_ready", True)
    blocker_msg = manifest["workload"].get("blocker") or "manifest marks workload.runner_ready = false"

    record_path = args.record or _default_record_path(args.manifest)
    retained: dict[str, dict[str, Any]] = {}
    started_at = utc_now_iso()
    resumed_at: list[str] = []
    if args.resume:
        if not os.path.exists(record_path):
            print(f"error: --resume needs an existing record file at {record_path}", file=sys.stderr)
            return 1
        try:
            retained, existing = load_resume_units(
                record_path, manifest["campaign_id"], retry_failed=args.retry_failed
            )
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        started_at = existing.get("started_at_utc") or started_at
        resumed_at = list(existing.get("resumed_at_utc") or []) + [utc_now_iso()]

    pending_units = [u for u in selected_units if u["id"] not in retained]
    if args.limit is not None:
        pending_units = pending_units[: args.limit]

    if args.dry_run:
        print(
            f"[dry-run] campaign={manifest['campaign_id']} total={len(manifest['units'])} "
            f"selected={len(selected_units)} resumed_skipped={len(retained)} scheduled={len(pending_units)} "
            f"concurrency={args.concurrency}"
        )
        if pin_os or pin_arch:
            print(f"[dry-run] system_pin={pin_os or '*'}/{pin_arch or '*'}")
        if runner_sha256:
            print(f"[dry-run] runner verified ({normalized_runner_system}): sha256={runner_sha256}")
        if workload_ready is False:
            print(f"[dry-run] workload blocker: {blocker_msg}")
        for u in pending_units[:3]:
            preview = build_submit_command(
                args.bin,
                u,
                master=args.master,
                relay_url=args.relay_url,
                config_file=args.config,
                insecure=args.insecure,
                output_dir=os.path.join(args.output_dir, u["id"], "attempt-1")
                if args.output_dir
                else None,
                extra_inputs=extra_inputs,
                require_isolation_override=args.require_isolation,
            )
            print(f"[dry-run] {u['id']}: {' '.join(preview)}")
        if len(pending_units) > 3:
            print(f"[dry-run] ... ({len(pending_units) - 3} more unit(s))")
        return 0

    if workload_ready is False and not args.allow_blocked_workload:
        print(f"error: workload is blocked: {blocker_msg}", file=sys.stderr)
        return 1

    sub_env = _cluster_env(args.secret_file)
    try:
        directory, doctor_data = fetch_cluster_facts(args, sub_env, participant_map)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    orphan_lock = threading.Lock()
    orphans_by_unit = _load_orphans(record_path) if args.resume else {}

    def record_orphan(unit_id: str, task_id: str) -> None:
        with orphan_lock:
            tids = orphans_by_unit.setdefault(unit_id, [])
            if task_id not in tids:
                tids.append(task_id)
            _save_orphans(record_path, orphans_by_unit)

    if args.resume and orphans_by_unit:
        for uid in [u["id"] for u in pending_units]:
            tids = orphans_by_unit.pop(uid, [])
            remaining_tids = []
            for tid in tids:
                if not cancel_task_on_master(
                    args.bin,
                    tid,
                    sub_env,
                    master=args.master,
                    relay_url=args.relay_url,
                    config_file=args.config,
                    insecure=args.insecure,
                ):
                    remaining_tids.append(tid)
            if remaining_tids:
                orphans_by_unit[uid] = remaining_tids
        _save_orphans(record_path, orphans_by_unit)

    unit_order = [u["id"] for u in selected_units]
    units_by_id: dict[str, dict[str, Any]] = dict(retained)
    state_lock = threading.Lock()
    stop_event = threading.Event()
    force_stop_event = threading.Event()
    signal_notify_event = threading.Event()
    all_done_event = threading.Event()
    received_signal = [0]
    signal_count = [0]
    proc_registry = _ActiveProcRegistry()

    def flush_checkpoint(finished_at: str | None = None, interrupted: bool = False) -> dict[str, Any]:
        payload = _build_record_payload(
            manifest,
            unit_order,
            units_by_id,
            directory,
            doctor_data,
            started_at=started_at,
            resumed_at=resumed_at,
            finished_at=finished_at,
            interrupted=interrupted,
            runner_sha256=runner_sha256,
            runner_system=normalized_runner_system,
        )
        atomic_write_json(record_path, payload)
        return payload

    def handle_signal(signum: int, _frame: Any) -> None:
        received_signal[0] = signum
        signal_count[0] += 1
        stop_event.set()
        if signal_count[0] >= 2 or args.interrupt_grace_secs <= 0:
            force_stop_event.set()
        signal_notify_event.set()
        if signal_count[0] >= 3:
            signal.signal(signum, signal.SIG_DFL)

    def interrupt_watcher() -> None:
        signal_notify_event.wait()
        if all_done_event.is_set():
            return
        if not force_stop_event.is_set() and args.interrupt_grace_secs > 0:
            force_stop_event.wait(timeout=args.interrupt_grace_secs)
        if not all_done_event.is_set():
            force_stop_event.set()
            proc_registry.terminate_all()

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    watcher_thread = threading.Thread(target=interrupt_watcher, daemon=True)
    if threading.current_thread() is threading.main_thread():
        watcher_thread.start()
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

    try:
        flush_checkpoint(finished_at=None, interrupted=False)

        def run_one(unit: dict[str, Any]) -> None:
            if stop_event.is_set():
                return
            rec = run_unit_with_mismatch_rerun(
                args.bin,
                unit,
                directory,
                sub_env,
                stop_event=stop_event,
                master=args.master,
                relay_url=args.relay_url,
                config_file=args.config,
                insecure=args.insecure,
                base_output_dir=args.output_dir,
                extra_inputs=extra_inputs,
                require_isolation_override=args.require_isolation,
                proc_registry=proc_registry,
                force_stop_event=force_stop_event,
                submit_timeout_override=args.submit_timeout_secs,
                orphan_recorder=record_orphan,
            )
            with state_lock:
                units_by_id[unit["id"]] = rec
                flush_checkpoint(finished_at=None, interrupted=stop_event.is_set())
            print(
                f"[{rec['id']}] status={rec['status']} attempts={len(rec['attempts'])}",
                flush=True,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futures = [ex.submit(run_one, u) for u in pending_units]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()

        interrupted = stop_event.is_set() or any(
            r.get("status") == "interrupted" for r in units_by_id.values()
        )
        final_payload = flush_checkpoint(
            finished_at=None if interrupted else utc_now_iso(),
            interrupted=interrupted,
        )
    finally:
        all_done_event.set()
        force_stop_event.set()
        signal_notify_event.set()
        if watcher_thread.is_alive():
            watcher_thread.join(timeout=2.0)
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)

    if received_signal[0]:
        return 128 + received_signal[0]

    scheduled_ids = {u["id"] for u in pending_units}
    scheduled_records = [units_by_id[uid] for uid in unit_order if uid in scheduled_ids and uid in units_by_id]
    if any(r["status"] == "interrupted" for r in scheduled_records):
        return 130
    if any(r["status"] in ("mismatch_persisted", "mismatch_rerun_failed") for r in scheduled_records):
        return 4
    if any(r["status"] in ("failed", "verify_error") for r in scheduled_records):
        return 1
    if final_payload["summary"]["pending"] > 0 and args.limit is None and not args.shard:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
