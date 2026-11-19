#!/usr/bin/env python3
"""
benchmarks/campaigns/driver.py - research campaign sweep driver (`APP-6`, design 0094).

Standard-library-only Python driver that reads a campaign manifest, refuses any
secret inputs (`.env`, `.secret`, `invite.oxs`), optionally verifies the runner
binary SHA-256 against the manifest, submits each work unit with:

  oxideswarm submit --json --wait --verify <N> [--verify-file <PATH>] ...

When a unit reports a verification mismatch (exit code 4 or `verify.status ==
"mismatch"`), the driver reruns that unit once with the same verification flags
and records both attempts. It writes one atomic JSON record per run with
per-participant-label counts and zero personal names, hostnames, raw worker
names, worker UUIDs, IP addresses, or secrets, and supports `--resume` after an
interrupt without resubmitting completed units as well as `--dry-run`.
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
from typing import Any

RECORD_FORMAT = 1
FINISHED_STATUSES_DEFAULT = frozenset({"verified", "verified_on_rerun", "mismatch_persisted"})
IPV4_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
IPV6_BRACKET_RE = re.compile(r"\[[0-9a-fA-F:]+\](?::\d+)?")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
OXS_TOKEN_RE = re.compile(r"oxs1-[A-Za-z0-9:._=-]+")
SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")

FORBIDDEN_INPUT_EXACT = frozenset({".env", "cluster.secret", "invite.oxs"})


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def is_forbidden_input_spec(spec: str) -> bool:
    """Return True if an `--input` spec points to `.env`, `.env.*`, `*.secret`, or `invite.oxs`."""
    local, sandbox = _input_spec_parts(spec.strip())
    for candidate in (local, sandbox):
        if not candidate:
            continue
        norm = candidate.replace("\\", "/").rstrip("/")
        base = os.path.basename(norm).lower()
        if not base:
            continue
        if base in FORBIDDEN_INPUT_EXACT or base.startswith(".env.") or base.endswith(".secret"):
            return True
    return False


def validate_no_secret_inputs(specs: list[str], context: str) -> None:
    for spec in specs:
        if not isinstance(spec, str) or not spec.strip():
            raise ValueError(f"{context}: input spec must be a non-empty string")
        if is_forbidden_input_spec(spec):
            raise ValueError(
                f"{context}: refused secret/credential input spec {spec!r} (.env, *.secret, invite.oxs)"
            )


def _format_template(value: Any, unit_id: str, seed: Any) -> Any:
    if isinstance(value, str):
        return value.replace("{unit_id}", unit_id).replace("{seed}", "" if seed is None else str(seed))
    if isinstance(value, list):
        return [_format_template(item, unit_id, seed) for item in value]
    if isinstance(value, dict):
        return {str(k): _format_template(v, unit_id, seed) for k, v in value.items()}
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
    validate_no_secret_inputs(default_inputs, "defaults.inputs")

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
    default_os = defaults.get("os")
    default_arch = defaults.get("arch")
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
        command = u.get("command", default_command)
        if not isinstance(command, str) or not command.strip():
            raise ValueError(f"unit {unit_id!r}: command must be a non-empty string")

        base_args = (
            _require_str_list(u.get("args"), f"unit {unit_id!r}.args")
            if "args" in u
            else list(default_args)
        )
        extra_args = _require_str_list(u.get("extra_args"), f"unit {unit_id!r}.extra_args")
        args = _format_template(base_args + extra_args, unit_id, seed)

        unit_inputs = (
            _require_str_list(u.get("inputs"), f"unit {unit_id!r}.inputs")
            if "inputs" in u
            else list(default_inputs)
        )
        extra_inputs = _require_str_list(u.get("extra_inputs"), f"unit {unit_id!r}.extra_inputs")
        merged_inputs = _format_template(unit_inputs + extra_inputs, unit_id, seed)
        validate_no_secret_inputs(merged_inputs, f"unit {unit_id!r}.inputs")

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
        outputs = _format_template(outputs, unit_id, seed)

        env = dict(default_env)
        env.update(_require_str_dict(u.get("env"), f"unit {unit_id!r}.env"))
        env = _format_template(env, unit_id, seed)
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
            verify_file = _format_template(verify_file, unit_id, seed)
        if not outputs and not verify_file:
            raise ValueError(f"unit {unit_id!r}: --verify needs outputs or verify_file")

        verify_any_system = bool(u.get("verify_any_system", default_verify_any))
        require_isolation = u.get("require_isolation", default_isolation)
        require_tags = (
            _require_str_list(u.get("require_tags"), f"unit {unit_id!r}.require_tags")
            if "require_tags" in u
            else list(default_tags)
        )
        os_target = u.get("os", default_os)
        arch_target = u.get("arch", default_arch)
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

        params = u.get("params")
        if params is not None and not isinstance(params, dict):
            raise ValueError(f"unit {unit_id!r}.params must be an object")

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


def verify_runner_hash(
    workload: dict[str, Any],
    runner_path: str | os.PathLike[str],
    runner_system: str | None,
) -> tuple[str, str]:
    """Verify `--runner` SHA-256 against `workload.runner_sha256[runner_system]`."""
    runner_abs = os.path.abspath(runner_path)
    if not os.path.isfile(runner_abs):
        raise ValueError(f"runner binary does not exist: {runner_abs}")
    if is_forbidden_input_spec(runner_abs):
        raise ValueError(f"refused forbidden runner path: {runner_abs}")

    hashes = workload.get("runner_sha256")
    if not isinstance(hashes, dict) or not hashes:
        raise ValueError("manifest.workload.runner_sha256 must map <os>/<arch> to SHA-256 hex")
    if not runner_system:
        raise ValueError("--runner requires --runner-system <os>/<arch> (for example macos/aarch64)")
    expected = hashes.get(runner_system)
    if not isinstance(expected, str) or not SHA256_HEX_RE.match(expected.strip().lower()):
        raise ValueError(
            f"manifest.workload.runner_sha256[{runner_system!r}] must be a 64-char SHA-256 hex digest, got {expected!r}"
        )
    actual = file_sha256(runner_abs).lower()
    if actual != expected.strip().lower():
        raise ValueError(
            f"runner SHA-256 mismatch for {runner_system}: expected {expected.strip().lower()}, got {actual}"
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
    ) -> None:
        self._lock = threading.Lock()
        self._participant_map = dict(participant_map or {})
        self._by_raw: dict[str, dict[str, Any]] = {}
        self._records: list[dict[str, Any]] = []
        self._secret_map: dict[str, str] = {}
        self._per_participant_seq: dict[str, int] = {}

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

        ordered = sorted(
            workers_json or [],
            key=lambda w: (str(w.get("name") or ""), str(w.get("id") or "")),
        )
        for w in ordered:
            if isinstance(w, dict):
                self._register_locked(w)

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
        return scrub_text(text, secret_map)


def scrub_text(text: str | None, secret_map: dict[str, str]) -> str:
    """Replace raw worker names, UUIDs, host/user strings, IP addresses, and oxs1 tokens."""
    out = text or ""
    for raw in sorted(secret_map, key=len, reverse=True):
        if len(raw) < 2:
            continue
        if re.match(r"^[\w-]+$", raw):
            out = re.sub(r"(?<![\w-])%s(?![\w-])" % re.escape(raw), secret_map[raw], out)
        else:
            out = out.replace(raw, secret_map[raw])
    out = UUID_RE.sub("<id>", out)
    out = OXS_TOKEN_RE.sub("<token>", out)
    out = IPV4_RE.sub("<address>", out)
    out = IPV6_BRACKET_RE.sub("<address>", out)
    return out


def sanitize_doctor(doc: dict[str, Any] | None, directory: WorkerDirectory) -> dict[str, Any]:
    """Sanitize `oxideswarm doctor --json` output without hostnames, worker names, UUIDs, or IPs."""
    if not isinstance(doc, dict):
        return {"workers": [], "checks": []}
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
            "detail": directory.scrub(str(c.get("detail") or "")),
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
            "error": directory.scrub(str(raw_verify.get("error") or "")),
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
) -> str:
    verify = doc.get("verify") if isinstance(doc, dict) else None
    v_status = verify.get("status") if isinstance(verify, dict) else None
    if cli_exit_code == 4 or v_status == "mismatch":
        return "mismatch"
    if v_status == "error":
        return "verify_error"
    if cli_exit_code == 0 and (expected_verify == 0 or v_status == "verified"):
        if isinstance(doc, dict) and doc.get("exit_code", 0) == 0 and not doc.get("error"):
            return "verified"
    return "failed"


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
) -> dict[str, Any]:
    """Invoke `oxideswarm submit` once and return a sanitized attempt record."""
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
    timeout_limit = (unit.get("timeout_secs") or 1800) * (1 + unit.get("verify", 1)) + 180
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=sub_env,
            stdin=subprocess.DEVNULL,
            timeout=timeout_limit,
        )
        cli_rc = proc.returncode
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
    except subprocess.TimeoutExpired as exc:
        cli_rc = 124
        stdout = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = (
            exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        ) + f"\nsubprocess timed out after {timeout_limit}s"

    dt = round(time.perf_counter() - t0, 4)
    doc = None
    if stdout.strip():
        try:
            parsed = json.loads(stdout)
            if isinstance(parsed, dict):
                doc = parsed
        except ValueError:
            doc = None

    primary_label, primary_participant = (
        directory.ensure_worker(
            raw_id=str(doc.get("worker_id") or "") or None if doc else None,
            raw_name=str(doc.get("worker_name") or "") or None if doc else None,
        )
        if doc and (doc.get("worker_id") or doc.get("worker_name"))
        else (None, None)
    )
    sanitized_verify = sanitize_verify_report(doc.get("verify") if doc else None, directory)
    outcome = classify_attempt(cli_rc, doc, unit["verify"])

    error_parts = []
    if doc and doc.get("error"):
        error_parts.append(str(doc["error"]))
    if doc and doc.get("artifact_save_error"):
        error_parts.append(f"artifact_save_error: {doc['artifact_save_error']}")
    if outcome in ("failed", "verify_error") and stderr.strip():
        error_parts.append(stderr.strip()[:600])
    error_text = directory.scrub(" | ".join(error_parts)) if error_parts else None

    return {
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


def run_unit_with_mismatch_rerun(
    binary: str,
    unit: dict[str, Any],
    directory: WorkerDirectory,
    sub_env: dict[str, str],
    **kwargs: Any,
) -> dict[str, Any]:
    """Run one work unit; on verification mismatch, rerun once and record both attempts."""
    attempts: list[dict[str, Any]] = []
    a1 = execute_submit_attempt(binary, unit, 1, "initial", directory, sub_env, **kwargs)
    attempts.append(a1)

    if a1["outcome"] == "verified":
        status = "verified"
        had_mismatch = False
        needs_investigation = False
    elif a1["outcome"] == "mismatch":
        had_mismatch = True
        needs_investigation = True
        if unit.get("mismatch_retries", 1) >= 1:
            a2 = execute_submit_attempt(
                binary, unit, 2, "mismatch_rerun", directory, sub_env, **kwargs
            )
            attempts.append(a2)
            if a2["outcome"] == "verified":
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
    return {
        "units_total": units_total,
        "units_recorded": len(unit_records),
        "verified": counts["verified"],
        "verified_on_rerun": counts["verified_on_rerun"],
        "mismatch_persisted": counts["mismatch_persisted"],
        "mismatch_rerun_failed": counts["mismatch_rerun_failed"],
        "verify_error": counts["verify_error"],
        "failed": counts["failed"],
        "pending": max(0, units_total - len(unit_records)),
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


def load_resume_units(
    record_path: str | os.PathLike[str],
    campaign_id: str,
    *,
    retry_failed: bool = False,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load completed units from an existing record file for `--resume`."""
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


def fetch_cluster_facts(
    args: argparse.Namespace,
    sub_env: dict[str, str],
    participant_map: dict[str, str],
) -> tuple[WorkerDirectory, dict[str, Any] | None]:
    common = _base_cluster_args(args)
    r_workers = subprocess.run(
        [args.bin, "workers", *common, "--json"],
        capture_output=True,
        text=True,
        env=sub_env,
        stdin=subprocess.DEVNULL,
        timeout=60,
    )
    if r_workers.returncode != 0:
        raise RuntimeError(
            f"`oxideswarm workers --json` failed (exit {r_workers.returncode}): {r_workers.stderr.strip()}"
        )
    workers_raw = json.loads(r_workers.stdout)
    if not isinstance(workers_raw, list):
        raise RuntimeError("`oxideswarm workers --json` did not return a list")
    directory = WorkerDirectory(workers_raw, participant_map)

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
            doctor_data = {
                "error": directory.scrub(f"doctor exited {r_doc.returncode}: {r_doc.stderr.strip()[:200]}")
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
    ap.add_argument("--runner-system", help="OS/arch key in workload.runner_sha256 (e.g. macos/aarch64)")
    ap.add_argument("--participants", help="JSON file mapping worker names or IDs to P1..P5 labels")
    ap.add_argument("--output-dir", help="Local directory where verified unit outputs are saved")
    ap.add_argument("--record", help="Output JSON record path (default: <manifest_dir>/results/<date>-campaign.json)")
    ap.add_argument("--concurrency", type=int, default=4, help="Max concurrent unit submissions (default: 4)")
    ap.add_argument("--limit", type=int, help="Run at most N pending units in this invocation")
    ap.add_argument("--expect-workers", type=int, help="Pass --expect-workers N to `doctor --json`")
    ap.add_argument("--skip-doctor", action="store_true", help="Skip `oxideswarm doctor --json` before running")
    ap.add_argument("--require-isolation", help="Override `defaults.require_isolation` for every unit")
    ap.add_argument("--resume", action="store_true", help="Skip units already completed in --record")
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

    try:
        manifest = load_manifest(args.manifest)
        participant_map = load_participant_map(args.participants)
        extra_inputs: list[str] = []
        runner_sha256: str | None = None
        if args.runner:
            runner_spec, runner_sha256 = verify_runner_hash(
                manifest["workload"], args.runner, args.runner_system
            )
            extra_inputs.append(runner_spec)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

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

    pending_units = [u for u in manifest["units"] if u["id"] not in retained]
    if args.limit is not None:
        pending_units = pending_units[: args.limit]

    if args.dry_run:
        print(
            f"[dry-run] campaign={manifest['campaign_id']} total={len(manifest['units'])} "
            f"resumed_skipped={len(retained)} scheduled={len(pending_units)} "
            f"concurrency={args.concurrency}"
        )
        if runner_sha256:
            print(f"[dry-run] runner verified ({args.runner_system}): sha256={runner_sha256}")
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

    sub_env = _cluster_env(args.secret_file)
    try:
        directory, doctor_data = fetch_cluster_facts(args, sub_env, participant_map)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    unit_order = [u["id"] for u in manifest["units"]]
    units_by_id: dict[str, dict[str, Any]] = dict(retained)
    state_lock = threading.Lock()
    stop_event = threading.Event()
    received_signal = [0]

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
            runner_system=args.runner_system,
        )
        atomic_write_json(record_path, payload)
        return payload

    def handle_signal(signum: int, _frame: Any) -> None:
        received_signal[0] = signum
        stop_event.set()
        signal.signal(signum, signal.SIG_DFL)

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    if threading.current_thread() is threading.main_thread():
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
                master=args.master,
                relay_url=args.relay_url,
                config_file=args.config,
                insecure=args.insecure,
                base_output_dir=args.output_dir,
                extra_inputs=extra_inputs,
                require_isolation_override=args.require_isolation,
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

        interrupted = stop_event.is_set()
        final_payload = flush_checkpoint(
            finished_at=None if interrupted else utc_now_iso(),
            interrupted=interrupted,
        )
    finally:
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)

    if received_signal[0]:
        return 128 + received_signal[0]

    scheduled_ids = {u["id"] for u in pending_units}
    scheduled_records = [units_by_id[uid] for uid in unit_order if uid in scheduled_ids and uid in units_by_id]
    if any(r["status"] in ("mismatch_persisted", "mismatch_rerun_failed") for r in scheduled_records):
        return 4
    if any(r["status"] in ("failed", "verify_error") for r in scheduled_records):
        return 1
    if final_payload["summary"]["pending"] > 0 and args.limit is None:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
