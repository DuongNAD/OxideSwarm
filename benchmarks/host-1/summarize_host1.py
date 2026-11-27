#!/usr/bin/env python3
"""
Summarize HOST-1 llama-bench JSON outputs and evaluate the Q106 10% gate.

Pure Python standard library. Emits no host names, user names, or IP addresses.
"""

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


IPV4_RE = re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b")


def sanitize_llama_bench_json(data: Any) -> Any:
    """Strip directory prefixes from model_filename fields in-place and return data."""
    if isinstance(data, list):
        for item in data:
            sanitize_llama_bench_json(item)
    elif isinstance(data, dict):
        if "model_filename" in data and isinstance(data["model_filename"], str):
            val = data["model_filename"].replace("\\", "/")
            data["model_filename"] = val.rsplit("/", 1)[-1]
    return data


def find_pii_leaks(text: str, forbidden_tokens: List[str]) -> List[str]:
    """Return any leaked host/user/IPv4 tokens detected in text (case-insensitive)."""
    leaks: List[str] = []
    lowered = text.lower()
    for tok in forbidden_tokens:
        cleaned = (tok or "").strip()
        if cleaned and len(cleaned) >= 3 and cleaned.lower() in lowered:
            leaks.append(cleaned)
    for m in IPV4_RE.finditer(text):
        addr = m.group(0)
        # Allow version-like tokens such as 10.0.26300 only when explicitly excluded by caller;
        # llama-bench / machine_facts should not contain IPv4 quads (<=255 octets).
        octets = addr.split(".")
        if all(0 <= int(o) <= 255 for o in octets):
            leaks.append(addr)
    return leaks


def _classify_row(filename: str, row: Dict[str, Any]) -> Tuple[str, str]:
    ngl = int(row.get("n_gpu_layers", 0))
    backends = str(row.get("backends", "CPU"))
    if ngl == 0:
        return ("Qwen3-0.6B Q8_0 (CPU)", "CPU")
    if "CUDA" in backends.upper():
        return ("Qwen3-8B Q4_K_M (GPU)", "CUDA")
    if "VULKAN" in backends.upper():
        return ("Qwen3-8B Q4_K_M (GPU)", "Vulkan")
    return ("Qwen3-8B Q4_K_M (GPU)", backends)


def _group_rows(state_name: str, json_path: str) -> List[Dict[str, Any]]:
    with open(json_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    rows = raw if isinstance(raw, list) else [raw]
    grouped: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}
    base = os.path.basename(json_path)

    for row in rows:
        if not isinstance(row, dict):
            continue
        workload, backend = _classify_row(base, row)
        load_mode = str(row.get("load_mode", "auto"))
        if "_nommap" in base and load_mode == "auto":
            load_mode = "none"
        elif "_mlock" in base and load_mode == "auto":
            load_mode = "mlock"
        threads = int(row.get("n_threads", 0))
        key = (workload, backend, load_mode, threads)
        entry = grouped.setdefault(
            key,
            {
                "state": state_name,
                "workload": workload,
                "backend": backend,
                "load_mode": load_mode,
                "threads": threads,
                "pp_ts": None,
                "pp_sd": None,
                "tg_ts": None,
                "tg_sd": None,
            },
        )
        n_prompt = int(row.get("n_prompt", 0))
        n_gen = int(row.get("n_gen", 0))
        avg_ts = float(row.get("avg_ts", 0.0))
        stddev_ts = float(row.get("stddev_ts", 0.0))
        if n_prompt > 0 and n_gen == 0:
            entry["pp_ts"] = avg_ts
            entry["pp_sd"] = stddev_ts
        elif n_gen > 0 and n_prompt == 0:
            entry["tg_ts"] = avg_ts
            entry["tg_sd"] = stddev_ts

    return list(grouped.values())


def summarize_dir(state_dir: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    facts_path = os.path.join(state_dir, "machine_facts.json")
    facts: Dict[str, Any] = {}
    if os.path.isfile(facts_path):
        with open(facts_path, "r", encoding="utf-8") as fh:
            facts = json.load(fh)
    state_name = str(facts.get("state") or os.path.basename(os.path.abspath(state_dir)))

    summaries: List[Dict[str, Any]] = []
    for name in sorted(os.listdir(state_dir)):
        if not name.endswith(".json") or name == "machine_facts.json":
            continue
        summaries.extend(_group_rows(state_name, os.path.join(state_dir, name)))
    return facts, summaries


def compute_q106_gate(entries: List[Dict[str, Any]], threshold_pct: float = 10.0) -> Dict[str, Any]:
    """
    Evaluate Q106: whether tuned headless Linux (state3) is at least `threshold_pct` (10%)
    faster than Windows desktop as used (state1) on CPU tg128 or GPU tg128.
    """
    def best_tg(state_label: str, is_cpu: bool) -> Optional[float]:
        vals = [
            e["tg_ts"]
            for e in entries
            if e["state"] == state_label
            and ((e["backend"] == "CPU") == is_cpu)
            and e["tg_ts"] is not None
        ]
        return max(vals) if vals else None

    s1_cpu = best_tg("state1", True)
    s3_cpu = best_tg("state3", True)
    s1_gpu = best_tg("state1", False)
    s3_gpu = best_tg("state3", False)

    def pct(base: Optional[float], cand: Optional[float]) -> Optional[float]:
        if base is None or cand is None or base <= 0:
            return None
        return ((cand - base) / base) * 100.0

    cpu_gain = pct(s1_cpu, s3_cpu)
    gpu_gain = pct(s1_gpu, s3_gpu)

    if cpu_gain is None and gpu_gain is None:
        status = "PENDING"
        passed: Optional[bool] = None
    else:
        passed = any(g is not None and g >= threshold_pct for g in (cpu_gain, gpu_gain))
        status = "PASS" if passed else "FAIL"

    return {
        "status": status,
        "passed": passed,
        "threshold_pct": threshold_pct,
        "cpu_state1_tg128": s1_cpu,
        "cpu_state3_tg128": s3_cpu,
        "cpu_gain_pct": cpu_gain,
        "gpu_state1_tg128": s1_gpu,
        "gpu_state3_tg128": s3_gpu,
        "gpu_gain_pct": gpu_gain,
    }


def _fmt_rate(mean: Optional[float], sd: Optional[float]) -> str:
    if mean is None:
        return "Pending"
    if sd is None:
        return f"{mean:.2f}"
    return f"{mean:.2f} ± {sd:.2f}"


def render_markdown(entries: List[Dict[str, Any]]) -> str:
    lines = [
        "| State | Workload | Backend | Load mode | Threads | pp512 tok/s (mean ± stddev) | tg128 tok/s (mean ± stddev) |",
        "|---|---|---|---|---|---|---|",
    ]
    for e in entries:
        lines.append(
            f"| {e['state']} | {e['workload']} | {e['backend']} | {e['load_mode']} | {e['threads']} "
            f"| {_fmt_rate(e['pp_ts'], e['pp_sd'])} | {_fmt_rate(e['tg_ts'], e['tg_sd'])} |"
        )
    gate = compute_q106_gate(entries)
    lines.append("")
    if gate["status"] == "PENDING":
        lines.append("Q106 gate (>= 10% gain in state3 vs state1 on CPU or GPU): **Pending** (Linux state3 results not yet recorded).")
    else:
        cpu_s = f"{gate['cpu_gain_pct']:+.2f}%" if gate["cpu_gain_pct"] is not None else "n/a"
        gpu_s = f"{gate['gpu_gain_pct']:+.2f}%" if gate["gpu_gain_pct"] is not None else "n/a"
        lines.append(
            f"Q106 gate (>= 10% gain in state3 vs state1 on CPU or GPU): **{gate['status']}** "
            f"(CPU best tg128 delta: {cpu_s}; GPU best tg128 delta: {gpu_s})."
        )
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize HOST-1 llama-bench outputs.")
    parser.add_argument("dirs", nargs="+", help="One or more state output directories")
    args = parser.parse_args(argv)

    all_entries: List[Dict[str, Any]] = []
    for d in args.dirs:
        if not os.path.isdir(d):
            print(f"Directory not found: {d}", file=sys.stderr)
            return 2
        _, rows = summarize_dir(d)
        all_entries.extend(rows)

    print(render_markdown(all_entries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
