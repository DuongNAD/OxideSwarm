#!/usr/bin/env python3
"""
benchmarks/model-bringup/laya_session1.py - session 1 of the model bring-up study (spike R14).

Question: on a machine that holds no model, how long do the fetch and the load of a small decision
model take, how long does one decision take, and does the model read Vietnamese?

The model is Laya (https://huggingface.co/convaiinnovations/laya), subfolder "multilingual": an
encoder that answers typed questions about a text in one forward pass. It generates no text. The
script asks it one question per sentence: is this a command the assistant can carry out with
ordinary code, or a question that needs an answer in words?

Three phases, each in a fresh Python process so that nothing stays loaded between them:

  1. cold load   an empty Hugging Face cache: the weights are downloaded, then the model is built;
  2. warm loads  the same cache, with access to the hub switched off: the model is built again;
  3. decisions   every sentence of the data file, once (first pass) and then --repeats more times.

The script itself uses only the standard library. The `laya` package (and PyTorch, which it needs)
must already be installed in the interpreter that runs the script; the script installs nothing.
It records no host name, no user name and no path.

  python benchmarks/model-bringup/laya_session1.py                 # the real run
  python benchmarks/model-bringup/laya_session1.py --backend stub  # checks the script, no model

Read benchmarks/model-bringup/README.md for what each number means and what it does not show.
"""

import argparse
import datetime
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

DEFAULT_REPO = "convaiinnovations/laya"
DEFAULT_SUBFOLDER = "multilingual"
QUESTION_ID = "kind"
LABELS = ("command", "question")
QUESTIONS = {
    QUESTION_ID: {
        "type": "choice",
        "instructions": (
            "Is the sentence a command that asks the assistant to carry out an action on the "
            "computer, or a question that asks for an answer in words?"
        ),
        "criteria": {
            "command": (
                "an action to carry out on the computer: open, play, close, set, turn on or "
                "off, send, save, create"
            ),
            "question": (
                "a request for information, an explanation, a summary, advice or a "
                "calculation, answered in words"
            ),
        },
    }
}
WARM_UP_TEXT = "Open the settings window."
HERE = os.path.dirname(os.path.abspath(__file__))
PACKAGES = ("laya", "torch", "transformers", "tokenizers", "huggingface_hub", "numpy")

MISSING_PACKAGE = """\
The Python package `laya` is not installed for this interpreter:
  {exe}
Install it into a virtual environment first (see benchmarks/model-bringup/README.md), then run
this script with that environment's python. To check the script alone, add: --backend stub
"""


# --------------------------------------------------------------------------- small helpers


def fail(message):
    sys.stderr.write(message.rstrip() + "\n")
    raise SystemExit(2)


def jsonable(value):
    """Turns a prediction into plain JSON types (model libraries return their own scalars)."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    to_list = getattr(value, "tolist", None)
    if callable(to_list):
        try:
            return jsonable(to_list())
        except Exception:  # noqa: BLE001 - any failure falls through to the text form
            pass
    try:
        return jsonable(float(value))
    except (TypeError, ValueError):
        return str(value)


def stats(values):
    if not values:
        return None
    return {
        "median": round(statistics.median(values), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "n": len(values),
    }


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def dir_bytes(path):
    """Bytes of the regular files under `path`. Links are not followed, so nothing counts twice."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                info = os.lstat(os.path.join(root, name))
            except OSError:
                continue
            if not os.path.islink(os.path.join(root, name)):
                total += info.st_size
    return total


def load_sentences(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if not isinstance(row.get("text"), str) or row.get("expected") not in LABELS:
                    fail(f"{path}:{number}: need \"text\" and \"expected\" in {LABELS}")
                rows.append({"id": row.get("id", number), "text": row["text"],
                             "expected": row["expected"]})
    except (OSError, json.JSONDecodeError) as err:
        fail(f"cannot read the sentences file {path}: {err}")
    if not rows:
        fail(f"{path} holds no sentence")
    return rows


def run_text(command):
    try:
        done = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() if done.returncode == 0 and done.stdout.strip() else None


def machine_info():
    system = platform.system()
    cpu, ram, power = None, None, None
    if system == "Darwin":
        cpu = run_text(["sysctl", "-n", "machdep.cpu.brand_string"])
        size = run_text(["sysctl", "-n", "hw.memsize"])
        ram = int(size) if size and size.isdigit() else None
        battery = run_text(["pmset", "-g", "batt"])
        if battery:
            power = "ac" if "AC Power" in battery.splitlines()[0] else "battery"
    elif system == "Linux":
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as handle:
                for line in handle:
                    if line.lower().startswith("model name"):
                        cpu = line.split(":", 1)[1].strip()
                        break
            ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        except (OSError, ValueError):
            pass
    elif system == "Windows":
        cpu = platform.processor() or None
        try:
            import ctypes

            class MemoryStatus(ctypes.Structure):
                _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong),
                            ("total", ctypes.c_ulonglong), ("avail", ctypes.c_ulonglong),
                            ("total_page", ctypes.c_ulonglong), ("avail_page", ctypes.c_ulonglong),
                            ("total_virtual", ctypes.c_ulonglong),
                            ("avail_virtual", ctypes.c_ulonglong),
                            ("avail_extended", ctypes.c_ulonglong)]

            status = MemoryStatus()
            status.length = ctypes.sizeof(MemoryStatus)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
                ram = int(status.total)
        except (AttributeError, OSError):
            pass
    return {
        "os": system,
        "os_release": platform.release(),
        "arch": platform.machine(),
        "cpu": cpu or platform.processor() or None,
        "cores": os.cpu_count(),
        "ram_bytes": ram,
        "power": power,
    }


def load_averages():
    """The 1, 5 and 15 minute load averages, or None where the system has none (Windows)."""
    try:
        return [round(value, 2) for value in os.getloadavg()]
    except (AttributeError, OSError):
        return None


def git_info():
    commit = run_text(["git", "-C", HERE, "rev-parse", "--short", "HEAD"])
    if not commit:
        return None
    status = run_text(["git", "-C", HERE, "status", "--porcelain"])
    return {"commit": commit, "other_changes": bool(status)}


def package_versions():
    from importlib import metadata

    versions = {}
    for name in PACKAGES:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = None
    return versions


# --------------------------------------------------------------------------- the model


class StubAgent:
    """Stands in for the model so that the script can be checked without one. Not a measurement."""

    VERBS = ("mở", "tắt", "bật", "hẹn", "đặt", "tăng", "giảm", "chụp", "gửi", "phát", "lưu", "tạo")

    def predict(self, state, _questions):
        time.sleep(0.002)
        first_word = state.strip().lower().split(" ", 1)[0]
        label = "command" if first_word in self.VERBS else "question"
        return {"answers": {QUESTION_ID: {"choice": label}}}


def build_agent(args):
    """Returns (agent, seconds to import the package, seconds to load the model, versions)."""
    if args.backend == "stub":
        started = time.perf_counter()
        time.sleep(0.05)
        return StubAgent(), 0.0, time.perf_counter() - started, {}
    started = time.perf_counter()
    try:
        import laya
    except ImportError as err:
        fail(MISSING_PACKAGE.format(exe=sys.executable) + f"({err})")
    import_s = time.perf_counter() - started
    options = {}
    if args.subfolder:
        options["subfolder"] = args.subfolder
    if args.device:
        options["device"] = args.device
    started = time.perf_counter()
    agent = laya.load(args.repo, **options)
    load_s = time.perf_counter() - started
    return agent, import_s, load_s, package_versions()


def answer_of(result):
    try:
        return result["answers"][QUESTION_ID]
    except (KeyError, TypeError):
        return None


def label_of(result):
    answer = answer_of(result)
    label = answer.get("choice") if isinstance(answer, dict) else None
    return label if isinstance(label, str) else None


# --------------------------------------------------------------------------- phases (child)


def phase_load(args):
    agent, import_s, load_s, versions = build_agent(args)
    # One prediction after the load, so that anything fetched on first use arrives while the
    # hub is still reachable. It is timed on its own and is not part of load_s.
    started = time.perf_counter()
    agent.predict(WARM_UP_TEXT, QUESTIONS)
    first_predict_s = time.perf_counter() - started
    device = getattr(agent, "device", None)
    return {
        "import_s": round(import_s, 3),
        "load_s": round(load_s, 3),
        "first_predict_s": round(first_predict_s, 3),
        "device_reported": str(device) if device is not None else None,
        "versions": versions,
    }


def phase_decide(args):
    agent, import_s, load_s, versions = build_agent(args)
    sentences = load_sentences(args.sentences)
    passes = []
    for number in range(args.repeats + 1):
        rows = []
        for sentence in sentences:
            started = time.perf_counter()
            result = agent.predict(sentence["text"], QUESTIONS)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            row = {"id": sentence["id"], "ms": round(elapsed_ms, 3), "label": label_of(result)}
            if number == 0:
                row["answer"] = jsonable(answer_of(result))
            rows.append(row)
        passes.append(rows)
    return {
        "import_s": round(import_s, 3),
        "load_s": round(load_s, 3),
        "passes": passes,
        "versions": versions,
    }


def run_child(args):
    try:
        result = phase_load(args) if args.phase == "load" else phase_decide(args)
    except SystemExit:
        raise
    except Exception as err:  # noqa: BLE001 - the parent reports whatever the library raised
        result = {"error": f"{type(err).__name__}: {err}"}
    with open(args.result_file, "w", encoding="utf-8") as handle:
        json.dump(result, handle)
    return 1 if "error" in result else 0


# --------------------------------------------------------------------------- orchestration


def spawn_phase(phase, args, env, work_dir):
    """Runs one phase in a fresh interpreter. Returns its result with the process wall time."""
    result_file = os.path.join(work_dir, f"{phase}-{time.monotonic_ns()}.json")
    command = [sys.executable, os.path.abspath(__file__), "--phase", phase,
               "--result-file", result_file, "--backend", args.backend, "--repo", args.repo,
               "--subfolder", args.subfolder, "--sentences", args.sentences,
               "--repeats", str(args.repeats)]
    if args.device:
        command += ["--device", args.device]
    started = time.perf_counter()
    done = subprocess.run(command, env=env, check=False)
    wall_s = time.perf_counter() - started
    if not os.path.exists(result_file):
        fail(f"the {phase} phase ended with exit code {done.returncode} and left no result")
    with open(result_file, encoding="utf-8") as handle:
        result = json.load(handle)
    if "error" in result:
        fail(f"the {phase} phase failed: {result['error']}")
    result["process_wall_s"] = round(wall_s, 3)
    return result


def summarize_decisions(decide, sentences):
    first, repeats = decide["passes"][0], decide["passes"][1:]
    expected = {sentence["id"]: sentence for sentence in sentences}
    misses, per_label = [], {label: {"correct": 0, "total": 0} for label in LABELS}
    for row in first:
        sentence = expected[row["id"]]
        per_label[sentence["expected"]]["total"] += 1
        if row["label"] == sentence["expected"]:
            per_label[sentence["expected"]]["correct"] += 1
        else:
            misses.append({"id": row["id"], "text": sentence["text"],
                           "expected": sentence["expected"], "got": row["label"]})
    unstable = sorted({row["id"] for later in repeats for row, base in zip(later, first)
                       if row["label"] != base["label"]})
    correct = sum(item["correct"] for item in per_label.values())
    return {
        "first_call_ms": first[0]["ms"],
        "first_pass_ms": stats([row["ms"] for row in first]),
        "repeat_ms": stats([row["ms"] for later in repeats for row in later]),
        "accuracy": {"correct": correct, "total": len(first), "per_label": per_label},
        "misses": misses,
        "labels_changed_between_passes": unstable,
        "first_pass": first,
        "repeat_passes_ms": [[row["ms"] for row in later] for later in repeats],
    }


def default_out_path(machine, backend):
    cpu = re.sub(r"[^a-z0-9]+", "-", (machine["cpu"] or "unknown-cpu").lower()).strip("-")
    date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    stem = f"{date}-laya-{machine['os'].lower()}-{machine['arch'].lower()}-{cpu}"
    if backend != "laya":
        stem += f"-{backend}"
    base = os.path.join(HERE, "results", stem)
    path, number = base + ".json", 1
    while os.path.exists(path):  # never write over an earlier record
        number += 1
        path = f"{base}-{number}.json"
    return path


def print_summary(record):
    cold, warm, decisions = record["cold_load"], record["warm_loads"], record["decisions"]
    accuracy = decisions["accuracy"]
    print()
    if record["backend"] != "laya":
        print("STUB RUN: no model was used. The numbers below are not measurements.")
    print(f"model            {record['model']['repo']} / {record['model']['subfolder']}")
    print(f"machine          {record['machine']['cpu']}, {record['machine']['os']} "
          f"{record['machine']['arch']}, power: {record['machine']['power']}")
    print(f"load averages    before {record['load_averages']['before']}, "
          f"after {record['load_averages']['after']} ({record['machine']['cores']} cores)")
    if cold["fetch_measured"]:
        megabytes = record["cache"]["bytes_after_cold_load"] / 1e6
        print(f"cold load        {cold['load_s']:.1f} s for {megabytes:.0f} MB fetched and built "
              f"(process {cold['process_wall_s']:.1f} s)")
    elif record["backend"] != "laya":
        print(f"first load       {cold['load_s']:.1f} s (stub: nothing fetched)")
    else:
        print(f"first load       {cold['load_s']:.1f} s (the cache was not empty: no fetch)")
    if warm["load_s"]:
        print(f"warm load        median {warm['load_s']['median']:.2f} s "
              f"({warm['load_s']['min']:.2f} to {warm['load_s']['max']:.2f}, "
              f"n={warm['load_s']['n']}); whole process median "
              f"{warm['process_wall_s']['median']:.2f} s")
    if record["fetch_estimate"]:
        print(f"fetch, estimate  {record['fetch_estimate']['seconds']:.1f} s "
              "(cold load minus the median warm load)")
    print(f"first decision   {decisions['first_call_ms']:.1f} ms")
    if decisions["repeat_ms"]:
        print(f"one decision     median {decisions['repeat_ms']['median']:.1f} ms "
              f"({decisions['repeat_ms']['min']:.1f} to {decisions['repeat_ms']['max']:.1f}, "
              f"n={decisions['repeat_ms']['n']})")
    print(f"correct          {accuracy['correct']} of {accuracy['total']}"
          + "".join(f"; {label}: {item['correct']}/{item['total']}"
                    for label, item in accuracy["per_label"].items()))
    for miss in decisions["misses"]:
        print(f"  wrong: [{miss['id']}] expected {miss['expected']}, got {miss['got']}: "
              f"{miss['text']}")
    if decisions["labels_changed_between_passes"]:
        print(f"  labels changed between passes for: {decisions['labels_changed_between_passes']}")


def run_parent(args):
    if args.backend == "laya" and importlib.util.find_spec("laya") is None:
        fail(MISSING_PACKAGE.format(exe=sys.executable))
    sentences = load_sentences(args.sentences)
    started_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    load_before = load_averages()

    created_cache = not args.hf_home
    hf_home = args.hf_home or tempfile.mkdtemp(prefix="oxideswarm-model-bringup-hf-")
    os.makedirs(hf_home, exist_ok=True)
    cache_was_empty = dir_bytes(hf_home) == 0
    work_dir = tempfile.mkdtemp(prefix="oxideswarm-model-bringup-run-")

    env = dict(os.environ)
    env.update({"HF_HOME": hf_home, "USE_TF": "0", "TOKENIZERS_PARALLELISM": "false",
                "PYTHONUNBUFFERED": "1"})
    for name in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HUB_CACHE", "TRANSFORMERS_CACHE"):
        env.pop(name, None)
    offline = dict(env)
    if not args.online_warm:
        offline.update({"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1"})

    try:
        print(f"[1/3] cold load (Hugging Face cache: {hf_home})", flush=True)
        cold = spawn_phase("load", args, env, work_dir)
        cold["fetch_measured"] = cache_was_empty and args.backend == "laya"
        cache_bytes = dir_bytes(hf_home)

        print(f"[2/3] {args.warm_loads} warm loads", flush=True)
        warm_runs = [spawn_phase("load", args, offline, work_dir) for _ in range(args.warm_loads)]

        print(f"[3/3] decisions: {len(sentences)} sentences, 1 + {args.repeats} passes",
              flush=True)
        decide = spawn_phase("decide", args, offline, work_dir)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    warm = {
        "runs": warm_runs,
        "load_s": stats([run["load_s"] for run in warm_runs]),
        "process_wall_s": stats([run["process_wall_s"] for run in warm_runs]),
    }
    fetch_estimate = None
    if cold["fetch_measured"] and warm["load_s"]:
        fetch_estimate = {
            "seconds": round(max(cold["load_s"] - warm["load_s"]["median"], 0.0), 3),
            "note": "cold load_s minus the median warm load_s; an estimate, not a timed download",
        }
    versions = decide.pop("versions", {})
    for run in [cold] + warm_runs:
        run.pop("versions", None)

    record = {
        "schema": 1,
        "study": "model bring-up, session 1 (design 0056, spike R14)",
        "backend": args.backend,
        "started_utc": started_utc,
        "git": git_info(),
        "machine": machine_info(),
        "load_averages": {"before": load_before, "after": load_averages()},
        "python": platform.python_version(),
        "packages": versions,
        "model": {"repo": args.repo, "subfolder": args.subfolder,
                  "device_requested": args.device or "library default",
                  "device_reported": cold.get("device_reported")},
        "parameters": {"warm_loads": args.warm_loads, "repeats": args.repeats,
                       "warm_phases_offline": not args.online_warm,
                       "sentences_file": os.path.basename(args.sentences),
                       "sentences_sha256": sha256_file(args.sentences),
                       "questions": QUESTIONS},
        "cache": {"empty_at_start": cache_was_empty, "bytes_after_cold_load": cache_bytes},
        "cold_load": cold,
        "warm_loads": warm,
        "fetch_estimate": fetch_estimate,
        "decisions": summarize_decisions(decide, sentences),
    }
    print_summary(record)

    out_path = args.out
    if not out_path and args.backend == "laya" and not args.no_write:
        out_path = default_out_path(record["machine"], args.backend)
    if out_path and not args.no_write:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        print(f"\nrecord           {out_path}")
    else:
        print("\nrecord           not written")

    if args.delete_cache and created_cache:
        shutil.rmtree(hf_home, ignore_errors=True)
        print("cache            deleted")
    elif args.backend == "laya":
        print(f"cache            kept at {hf_home} ({cache_bytes / 1e6:.0f} MB); "
              "delete that folder when you are done")
    elif created_cache:
        shutil.rmtree(hf_home, ignore_errors=True)
    return 0


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Session 1 of the model bring-up study: fetch, load and decision times of a "
                    "small decision model, and its answers on Vietnamese sentences.")
    parser.add_argument("--backend", choices=("laya", "stub"), default="laya",
                        help="`stub` checks the script without any model; its numbers mean nothing")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="Hugging Face repository")
    parser.add_argument("--subfolder", default=DEFAULT_SUBFOLDER,
                        help="checkpoint inside the repository (default: %(default)s)")
    parser.add_argument("--device", default=None,
                        help="passed to laya.load(device=...): cpu, mps or cuda "
                             "(default: the package chooses)")
    parser.add_argument("--sentences", default=os.path.join(HERE, "vi_sentences.jsonl"),
                        help="JSON lines with \"text\" and \"expected\" (command or question)")
    parser.add_argument("--warm-loads", type=int, default=5,
                        help="loads from the filled cache, each in a new process (default: 5)")
    parser.add_argument("--repeats", type=int, default=5,
                        help="passes over the sentences after the first one (default: 5)")
    parser.add_argument("--hf-home", default=None,
                        help="Hugging Face cache directory. Default: a new, empty temporary "
                             "directory, so that the cold load really downloads")
    parser.add_argument("--online-warm", action="store_true",
                        help="leave hub access on in the warm phases (default: switched off, so "
                             "that a hidden download fails instead of changing the time)")
    parser.add_argument("--delete-cache", action="store_true",
                        help="delete the cache directory at the end, if this run created it")
    parser.add_argument("--out", default=None,
                        help="record file (default: "
                             "results/<UTC date>-laya-<os>-<arch>-<cpu>.json; a stub run writes "
                             "none unless this is given)")
    parser.add_argument("--no-write", action="store_true", help="print the summary only")
    parser.add_argument("--phase", choices=("load", "decide"), default=None,
                        help=argparse.SUPPRESS)
    parser.add_argument("--result-file", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.warm_loads < 0 or args.repeats < 0:
        parser.error("--warm-loads and --repeats must not be negative")
    if args.phase and not args.result_file:
        parser.error("--phase needs --result-file")
    args.sentences = os.path.abspath(args.sentences)
    return args


def main(argv=None):
    if sys.version_info < (3, 10):
        fail("Python 3.10 or newer is needed (the `laya` package requires it).")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    return run_child(args) if args.phase else run_parent(args)


if __name__ == "__main__":
    sys.exit(main())
