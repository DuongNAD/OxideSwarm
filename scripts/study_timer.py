#!/usr/bin/env python3
"""
scripts/study_timer.py - Round-one study session timer and record summarizer (ADOPT-1).

Standard library only. Records session timings, pause/help observations, own-job
outcomes, and 14-day follow-up answers with participant labels only (`P1`..`P5`),
and rejects personal data, IP addresses, invite strings, or secret paths before writing.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import os
import re
import statistics
import sys
import time
from typing import Any, Callable, Iterable, TextIO

SCHEMA_VERSION = "adopt1-session/1"
ALLOWED_PARTICIPANTS = ("P1", "P2", "P3", "P4", "P5")
ORDERED_STEPS = (
    "install",
    "join",
    "first_task",
    "file_round_trip",
    "own_job",
    "stop_worker",
)
STEP_LABELS = {
    "install": "Install oxideswarm from README.md (or USB kit) and verify checksum/quarantine",
    "join": "Join the temporary study cluster with an invite and start the worker",
    "first_task": "Submit first command task with --wait and observe output",
    "file_round_trip": "Submit a task sending an input file and collecting an output file",
    "own_job": "Run one job of the participant's own choice (optional if none)",
    "stop_worker": "Stop the worker cleanly and confirm active tasks end",
}
FIRST_RESULT_STEPS = ("install", "join", "first_task")
ALLOWED_INSTALL_METHODS = ("readme", "usb-kit")
ALLOWED_OUTCOMES = ("completed", "failed", "skipped")
ALLOWED_EVENT_KINDS = ("pause", "help")
ALLOWED_HELP_LEVELS = ("none", "hint", "command", "hands_on")
ALLOWED_OWN_JOB_RAN = ("yes", "no", "skipped")
ALLOWED_REPEAT_USE = ("yes", "no", "unknown")

_IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_USER_PATH_RE = re.compile(
    r"(?:^|[\s\"'(=])(?:/Users/[^/\s\"']+|/home/[^/\s\"']+|[A-Za-z]:\\Users\\[^\\\s\"']+)",
    re.IGNORECASE,
)


def default_now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def format_utc(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_participant(participant: str) -> str:
    if participant not in ALLOWED_PARTICIPANTS:
        allowed = ", ".join(ALLOWED_PARTICIPANTS)
        raise ValueError(f"participant must be one of ({allowed}), got {participant!r}")
    return participant


def validate_safe_text(field_name: str, text: str) -> str:
    """Rejects secrets, invite tokens, IPs, emails, and user home paths from study records."""
    if not text:
        return text
    lower = text.lower()
    if "oxs1-" in lower:
        raise ValueError(f"{field_name} must not contain an invite or join token (oxs1-)")
    if "cluster.secret" in lower or "node.key" in lower or "invite.oxs" in lower:
        raise ValueError(f"{field_name} must not name secret or key files")
    if _IPV4_RE.search(text):
        raise ValueError(f"{field_name} must not contain an IPv4 address")
    if _EMAIL_RE.search(text):
        raise ValueError(f"{field_name} must not contain an email address")
    if _USER_PATH_RE.search(text):
        raise ValueError(f"{field_name} must not contain a user home path with a username")
    return text


def validate_step(step_id: str) -> str:
    if step_id not in ORDERED_STEPS:
        allowed = ", ".join(ORDERED_STEPS)
        raise ValueError(f"step must be one of ({allowed}), got {step_id!r}")
    return step_id


def atomic_write_json(path: str | os.PathLike[str], data: dict[str, Any]) -> None:
    target = os.fspath(path)
    parent = os.path.dirname(os.path.abspath(target))
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = f"{target}.tmp.{os.getpid()}"
    payload = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, target)


def load_record(path: str | os.PathLike[str]) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        record = json.load(fh)
    if not isinstance(record, dict) or record.get("schema") != SCHEMA_VERSION:
        raise ValueError(f"unsupported or corrupt session record: {path}")
    validate_participant(str(record.get("participant", "")))
    return record


def _empty_steps() -> dict[str, dict[str, Any]]:
    return {
        step: {
            "status": "pending",
            "outcome": None,
            "started_at_utc": None,
            "ended_at_utc": None,
            "elapsed_s": None,
            "note": "",
            "events": [],
        }
        for step in ORDERED_STEPS
    }


def recompute_measures(record: dict[str, Any]) -> dict[str, Any]:
    steps = record["steps"]

    first_result_done = all(
        steps[s]["outcome"] == "completed" and steps[s]["elapsed_s"] is not None
        for s in FIRST_RESULT_STEPS
    )
    if first_result_done:
        total_first_s = sum(float(steps[s]["elapsed_s"]) for s in FIRST_RESULT_STEPS)
        m15_min: float | None = round(total_first_s / 60.0, 2)
    else:
        m15_min = None

    first_task_outcome = steps["first_task"]["outcome"]
    help_to_first = 0
    for s in FIRST_RESULT_STEPS:
        help_to_first += sum(1 for ev in steps[s]["events"] if ev.get("kind") == "help")

    if first_task_outcome is None:
        m16_unaided: bool | None = None
    elif first_result_done and help_to_first == 0:
        m16_unaided = True
    else:
        m16_unaided = False

    help_total = 0
    pause_total = 0
    total_elapsed_s = 0.0
    for s in ORDERED_STEPS:
        st = steps[s]
        if st["elapsed_s"] is not None:
            total_elapsed_s += float(st["elapsed_s"])
        for ev in st["events"]:
            if ev.get("kind") == "help":
                help_total += 1
            elif ev.get("kind") == "pause":
                pause_total += 1

    followup = record.get("followup_14d") or {}
    measures = {
        "m15_first_result_min": m15_min,
        "m16_unaided_first_result": m16_unaided,
        "m17_repeat_use_14d": followup.get("repeat_use"),
        "help_count_to_first_result": help_to_first,
        "help_count_total": help_total,
        "pause_count_total": pause_total,
        "total_elapsed_s": round(total_elapsed_s, 2),
    }
    record["measures"] = measures
    return measures


def init_session(
    record_path: str | os.PathLike[str],
    participant: str,
    primary_os: str,
    install_method: str,
    release: str,
    consent: bool,
    second_os: str = "",
    force: bool = False,
    now_utc: Callable[[], datetime.datetime] = default_now_utc,
) -> dict[str, Any]:
    if not consent:
        raise ValueError("consent must be confirmed (--consent) before initializing a session")
    if os.path.exists(record_path) and not force:
        raise FileExistsError(f"record already exists (pass --force to overwrite): {record_path}")
    validate_participant(participant)
    if install_method not in ALLOWED_INSTALL_METHODS:
        raise ValueError(
            f"install_method must be one of {ALLOWED_INSTALL_METHODS}, got {install_method!r}"
        )
    validate_safe_text("os", primary_os)
    validate_safe_text("second_os", second_os)
    validate_safe_text("release", release)

    dt = now_utc()
    record: dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "participant": participant,
        "date": dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "created_at_utc": format_utc(dt),
        "consent_confirmed": True,
        "os": primary_os.strip(),
        "second_os": second_os.strip(),
        "install_method": install_method,
        "release": release.strip(),
        "steps": _empty_steps(),
        "own_job": {"category": "", "ran": None, "note": ""},
        "followup_14d": {"repeat_use": None, "reason": "", "date": None},
        "measures": {},
    }
    recompute_measures(record)
    atomic_write_json(record_path, record)
    return record


def begin_step(
    record_path: str | os.PathLike[str],
    step_id: str,
    now_utc: Callable[[], datetime.datetime] = default_now_utc,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    validate_step(step_id)
    record = load_record(record_path)
    step = record["steps"][step_id]
    dt = now_utc()
    step["status"] = "in_progress"
    step["outcome"] = None
    step["started_at_utc"] = format_utc(dt)
    step["ended_at_utc"] = None
    step["elapsed_s"] = None
    step["started_mono_s"] = float(monotonic())
    step["started_epoch_s"] = float(dt.timestamp())
    recompute_measures(record)
    atomic_write_json(record_path, record)
    return record


def end_step(
    record_path: str | os.PathLike[str],
    step_id: str,
    outcome: str = "completed",
    note: str = "",
    now_utc: Callable[[], datetime.datetime] = default_now_utc,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    validate_step(step_id)
    if outcome not in ALLOWED_OUTCOMES:
        raise ValueError(f"outcome must be one of {ALLOWED_OUTCOMES}, got {outcome!r}")
    validate_safe_text("note", note)
    record = load_record(record_path)
    step = record["steps"][step_id]
    dt = now_utc()

    if outcome == "skipped" and step["started_at_utc"] is None:
        step["started_at_utc"] = format_utc(dt)
        elapsed_s = 0.0
    elif "started_mono_s" in step:
        mono_delta = float(monotonic()) - float(step["started_mono_s"])
        if mono_delta >= 0.0:
            elapsed_s = round(mono_delta, 2)
        elif "started_epoch_s" in step:
            elapsed_s = round(max(0.0, float(dt.timestamp()) - float(step["started_epoch_s"])), 2)
        else:
            elapsed_s = 0.0
    elif "started_epoch_s" in step:
        elapsed_s = round(max(0.0, float(dt.timestamp()) - float(step["started_epoch_s"])), 2)
    else:
        raise ValueError(f"step {step_id!r} was not started; call begin first or use outcome 'skipped'")

    step.pop("started_mono_s", None)
    step.pop("started_epoch_s", None)
    step["status"] = outcome
    step["outcome"] = outcome
    step["ended_at_utc"] = format_utc(dt)
    step["elapsed_s"] = elapsed_s
    if note:
        step["note"] = note.strip()
    recompute_measures(record)
    atomic_write_json(record_path, record)
    return record


def record_event(
    record_path: str | os.PathLike[str],
    step_id: str,
    kind: str,
    note: str,
    help_level: str = "none",
    now_utc: Callable[[], datetime.datetime] = default_now_utc,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    validate_step(step_id)
    if kind not in ALLOWED_EVENT_KINDS:
        raise ValueError(f"kind must be one of {ALLOWED_EVENT_KINDS}, got {kind!r}")
    if help_level not in ALLOWED_HELP_LEVELS:
        raise ValueError(f"help_level must be one of {ALLOWED_HELP_LEVELS}, got {help_level!r}")
    if kind == "help" and help_level == "none":
        raise ValueError("help_level must be hint, command, or hands_on when kind is 'help'")
    if kind == "pause" and help_level != "none":
        raise ValueError("help_level must be 'none' when kind is 'pause'")
    cleaned_note = validate_safe_text("note", note.strip())
    if not cleaned_note:
        raise ValueError("event note must not be empty")

    record = load_record(record_path)
    step = record["steps"][step_id]
    dt = now_utc()
    if "started_mono_s" in step:
        offset_s = round(max(0.0, float(monotonic()) - float(step["started_mono_s"])), 2)
    elif "started_epoch_s" in step:
        offset_s = round(max(0.0, float(dt.timestamp()) - float(step["started_epoch_s"])), 2)
    else:
        offset_s = 0.0

    step["events"].append(
        {
            "kind": kind,
            "help_level": help_level,
            "at_utc": format_utc(dt),
            "offset_s": offset_s,
            "note": cleaned_note,
        }
    )
    recompute_measures(record)
    atomic_write_json(record_path, record)
    return record


def record_own_job(
    record_path: str | os.PathLike[str],
    category: str,
    ran: str,
    note: str = "",
) -> dict[str, Any]:
    if ran not in ALLOWED_OWN_JOB_RAN:
        raise ValueError(f"ran must be one of {ALLOWED_OWN_JOB_RAN}, got {ran!r}")
    cleaned_cat = validate_safe_text("category", category.strip())
    cleaned_note = validate_safe_text("note", note.strip())
    record = load_record(record_path)
    record["own_job"] = {
        "category": cleaned_cat,
        "ran": ran,
        "note": cleaned_note,
    }
    atomic_write_json(record_path, record)
    return record


def record_followup(
    record_path: str | os.PathLike[str],
    repeat_use: str,
    reason: str = "",
    date: str | None = None,
    now_utc: Callable[[], datetime.datetime] = default_now_utc,
) -> dict[str, Any]:
    if repeat_use not in ALLOWED_REPEAT_USE:
        raise ValueError(f"repeat_use must be one of {ALLOWED_REPEAT_USE}, got {repeat_use!r}")
    cleaned_reason = validate_safe_text("reason", reason.strip())
    if date is not None:
        validate_safe_text("date", date.strip())
        followup_date = date.strip()
    else:
        followup_date = now_utc().astimezone(datetime.timezone.utc).strftime("%Y-%m-%d")

    record = load_record(record_path)
    record["followup_14d"] = {
        "repeat_use": repeat_use,
        "reason": cleaned_reason,
        "date": followup_date,
    }
    recompute_measures(record)
    atomic_write_json(record_path, record)
    return record


def run_interactive(
    record_path: str | os.PathLike[str],
    participant: str,
    primary_os: str,
    install_method: str,
    release: str,
    consent: bool,
    second_os: str = "",
    force: bool = False,
    now_utc: Callable[[], datetime.datetime] = default_now_utc,
    monotonic: Callable[[], float] = time.monotonic,
    input_stream: TextIO | None = None,
    output_stream: TextIO | None = None,
) -> dict[str, Any]:
    inp = input_stream if input_stream is not None else sys.stdin
    out = output_stream if output_stream is not None else sys.stdout

    record = init_session(
        record_path=record_path,
        participant=participant,
        primary_os=primary_os,
        install_method=install_method,
        release=release,
        consent=consent,
        second_os=second_os,
        force=force,
        now_utc=now_utc,
    )
    out.write(f"Session {participant} initialized at {record_path}\n")
    out.flush()

    for step_id in ORDERED_STEPS:
        out.write(f"\n[{step_id}] {STEP_LABELS[step_id]}\n")
        out.write("Press Enter to START, 's [note]' to skip, or 'q' to quit: ")
        out.flush()
        raw_start = inp.readline()
        if raw_start == "":
            break
        cmd_start = raw_start.strip()
        if cmd_start.lower() == "q":
            break
        if cmd_start.lower() == "s" or cmd_start.lower().startswith("s "):
            note = cmd_start[1:].strip() if len(cmd_start) > 1 else ""
            record = end_step(
                record_path,
                step_id,
                outcome="skipped",
                note=note,
                now_utc=now_utc,
                monotonic=monotonic,
            )
            continue

        record = begin_step(record_path, step_id, now_utc=now_utc, monotonic=monotonic)
        out.write(
            f"Started '{step_id}'. Commands: Enter=done | p <note> | "
            "h <hint|command|hands_on> <note> | f [note] | s [note] | q\n"
        )
        out.flush()

        quit_all = False
        while True:
            out.write(f"  ({step_id})> ")
            out.flush()
            raw_action = inp.readline()
            if raw_action == "":
                quit_all = True
                break
            action = raw_action.strip()
            if action == "":
                record = end_step(
                    record_path,
                    step_id,
                    outcome="completed",
                    now_utc=now_utc,
                    monotonic=monotonic,
                )
                break
            lower = action.lower()
            if lower == "q":
                quit_all = True
                break
            if lower == "p" or lower.startswith("p "):
                note = action[1:].strip() if len(action) > 1 else ""
                try:
                    record = record_event(
                        record_path,
                        step_id,
                        kind="pause",
                        note=note,
                        help_level="none",
                        now_utc=now_utc,
                        monotonic=monotonic,
                    )
                except ValueError as exc:
                    out.write(f"Error: {exc}\n")
                continue
            if lower.startswith("h "):
                parts = action[2:].strip().split(None, 1)
                if len(parts) < 2:
                    out.write("Usage: h <hint|command|hands_on> <note>\n")
                    continue
                level, note = parts[0], parts[1]
                try:
                    record = record_event(
                        record_path,
                        step_id,
                        kind="help",
                        help_level=level,
                        note=note,
                        now_utc=now_utc,
                        monotonic=monotonic,
                    )
                except ValueError as exc:
                    out.write(f"Error: {exc}\n")
                continue
            if lower == "f" or lower.startswith("f "):
                note = action[1:].strip() if len(action) > 1 else ""
                record = end_step(
                    record_path,
                    step_id,
                    outcome="failed",
                    note=note,
                    now_utc=now_utc,
                    monotonic=monotonic,
                )
                break
            if lower == "s" or lower.startswith("s "):
                note = action[1:].strip() if len(action) > 1 else ""
                record = end_step(
                    record_path,
                    step_id,
                    outcome="skipped",
                    note=note,
                    now_utc=now_utc,
                    monotonic=monotonic,
                )
                break
            out.write(
                "Unrecognized input. Use Enter, 'p <note>', 'h <level> <note>', 'f [note]', 's [note]', or 'q'.\n"
            )
        if quit_all:
            break

    return load_record(record_path)


def _validate_record_privacy(record: dict[str, Any], source_label: str) -> None:
    validate_participant(str(record.get("participant", "")))
    validate_safe_text(f"{source_label}.os", str(record.get("os", "")))
    validate_safe_text(f"{source_label}.second_os", str(record.get("second_os", "")))
    validate_safe_text(f"{source_label}.release", str(record.get("release", "")))
    for step_id in ORDERED_STEPS:
        st = record.get("steps", {}).get(step_id, {})
        validate_safe_text(f"{source_label}.{step_id}.note", str(st.get("note", "")))
        for idx, ev in enumerate(st.get("events", [])):
            validate_safe_text(
                f"{source_label}.{step_id}.events[{idx}].note", str(ev.get("note", ""))
            )
    own_job = record.get("own_job") or {}
    validate_safe_text(f"{source_label}.own_job.category", str(own_job.get("category", "")))
    validate_safe_text(f"{source_label}.own_job.note", str(own_job.get("note", "")))
    followup = record.get("followup_14d") or {}
    validate_safe_text(f"{source_label}.followup_14d.reason", str(followup.get("reason", "")))


def _session_stops_summary(record: dict[str, Any]) -> str:
    items: list[str] = []
    for step_id in ORDERED_STEPS:
        st = record["steps"][step_id]
        for ev in st["events"]:
            kind = ev["kind"]
            level = f"/{ev['help_level']}" if kind == "help" else ""
            items.append(f"{step_id} ({kind}{level}: {ev['note']})")
        if st["outcome"] == "failed":
            reason = f": {st['note']}" if st["note"] else ""
            items.append(f"{step_id} (failed{reason})")
    return "; ".join(items) if items else "none"


def summarize_records(paths: Iterable[str | os.PathLike[str]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    m15_values: list[float] = []
    m16_eligible = 0
    m16_unaided = 0
    m17_answered = 0
    m17_yes = 0

    for raw_path in paths:
        record = load_record(raw_path)
        _validate_record_privacy(record, os.fspath(raw_path))
        measures = recompute_measures(record)

        m15 = measures["m15_first_result_min"]
        if m15 is not None:
            m15_values.append(float(m15))

        m16 = measures["m16_unaided_first_result"]
        if m16 is not None:
            m16_eligible += 1
            if m16:
                m16_unaided += 1

        m17 = measures["m17_repeat_use_14d"]
        if m17 in ("yes", "no"):
            m17_answered += 1
            if m17 == "yes":
                m17_yes += 1

        own = record.get("own_job") or {}
        if own.get("category"):
            ran = own.get("ran") or "unknown"
            own_summary = f"{own['category']} ({ran})"
        else:
            own_summary = "none"

        rows.append(
            {
                "participant": record["participant"],
                "date": record.get("date", ""),
                "os": record.get("os", ""),
                "second_os": record.get("second_os", ""),
                "install_method": record.get("install_method", ""),
                "release": record.get("release", ""),
                "m15_first_result_min": m15,
                "m16_unaided_first_result": m16,
                "m17_repeat_use_14d": m17,
                "help_count_to_first_result": measures["help_count_to_first_result"],
                "pause_count_total": measures["pause_count_total"],
                "stops_summary": _session_stops_summary(record),
                "own_job_summary": own_summary,
            }
        )

    rows.sort(key=lambda r: ALLOWED_PARTICIPANTS.index(r["participant"]))
    return {
        "schema": SCHEMA_VERSION,
        "session_count": len(rows),
        "m15_completed_count": len(m15_values),
        "m15_median_min": round(statistics.median(m15_values), 2) if m15_values else None,
        "m15_min_min": round(min(m15_values), 2) if m15_values else None,
        "m15_max_min": round(max(m15_values), 2) if m15_values else None,
        "m16_eligible_count": m16_eligible,
        "m16_unaided_count": m16_unaided,
        "m17_answered_count": m17_answered,
        "m17_repeat_yes_count": m17_yes,
        "sessions": rows,
    }


def format_markdown_summary(summary: dict[str, Any]) -> str:
    def _esc(val: Any) -> str:
        return str(val).replace("|", "/")

    lines = [
        "| Participant | Date | OS pair | Start method | Release | M15 first result (min) | M16 unaided | Own job (step 4) | Stops or help | M17 repeat (14 d) |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in summary["sessions"]:
        os_pair = r["os"] if not r["second_os"] else f"{r['os']} + {r['second_os']}"
        m15_str = f"{r['m15_first_result_min']:.2f}" if r["m15_first_result_min"] is not None else "incomplete"
        if r["m16_unaided_first_result"] is True:
            m16_str = "yes"
        elif r["m16_unaided_first_result"] is False:
            m16_str = f"no ({r['help_count_to_first_result']} help)"
        else:
            m16_str = "incomplete"
        m17_str = r["m17_repeat_use_14d"] or "pending"
        lines.append(
            f"| {_esc(r['participant'])} | {_esc(r['date'])} | {_esc(os_pair)} | "
            f"{_esc(r['install_method'])} | {_esc(r['release'])} | {m15_str} | {m16_str} | "
            f"{_esc(r['own_job_summary'])} | {_esc(r['stops_summary'])} | {_esc(m17_str)} |"
        )

    lines.append("")
    if summary["m15_completed_count"] > 0:
        lines.append(
            f"- **M15 (time to first result):** median {summary['m15_median_min']:.2f} min "
            f"(min {summary['m15_min_min']:.2f}, max {summary['m15_max_min']:.2f}; "
            f"n={summary['m15_completed_count']})."
        )
    else:
        lines.append("- **M15 (time to first result):** no completed first-result session.")

    lines.append(
        f"- **M16 (unaided completion):** {summary['m16_unaided_count']} / {summary['m16_eligible_count']}."
    )
    if summary["m17_answered_count"] > 0:
        lines.append(
            f"- **M17 (14-day repeat use):** {summary['m17_repeat_yes_count']} / {summary['m17_answered_count']} "
            f"(out of {summary['session_count']} sessions)."
        )
    else:
        lines.append(
            f"- **M17 (14-day repeat use):** pending ({summary['session_count']} sessions recorded)."
        )
    return "\n".join(lines) + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ADOPT-1 round-one study timer and anonymized summary tool."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_init_flags(p: argparse.ArgumentParser) -> None:
        p.add_argument("--record", required=True, help="Output JSON session record path.")
        p.add_argument(
            "--participant",
            required=True,
            choices=ALLOWED_PARTICIPANTS,
            help="Participant label only (P1..P5).",
        )
        p.add_argument("--os", dest="primary_os", required=True, help="Primary OS/arch (e.g. windows/x86_64).")
        p.add_argument("--second-os", default="", help="Second machine OS/arch (optional).")
        p.add_argument(
            "--install-method",
            required=True,
            choices=ALLOWED_INSTALL_METHODS,
            help="Whether participant starts from README release install or USB kit.",
        )
        p.add_argument("--release", required=True, help="OxideSwarm release tested (e.g. 0.4.4).")
        p.add_argument(
            "--consent",
            action="store_true",
            help="Confirm verbal/written informed consent was given beforehand.",
        )
        p.add_argument("--force", action="store_true", help="Overwrite an existing record file.")

    p_init = sub.add_parser("init", help="Initialize an empty session record.")
    add_init_flags(p_init)

    p_run = sub.add_parser("run", help="Run an interactive timed facilitator session.")
    add_init_flags(p_run)

    p_begin = sub.add_parser("begin", help="Start timing a study step.")
    p_begin.add_argument("--record", required=True)
    p_begin.add_argument("--step", required=True, choices=ORDERED_STEPS)

    p_end = sub.add_parser("end", help="Finish timing a study step.")
    p_end.add_argument("--record", required=True)
    p_end.add_argument("--step", required=True, choices=ORDERED_STEPS)
    p_end.add_argument("--outcome", default="completed", choices=ALLOWED_OUTCOMES)
    p_end.add_argument("--note", default="")

    p_ev = sub.add_parser("event", help="Log a pause or facilitator help event on a step.")
    p_ev.add_argument("--record", required=True)
    p_ev.add_argument("--step", required=True, choices=ORDERED_STEPS)
    p_ev.add_argument("--kind", required=True, choices=ALLOWED_EVENT_KINDS)
    p_ev.add_argument("--help-level", default="none", choices=ALLOWED_HELP_LEVELS)
    p_ev.add_argument("--note", required=True)

    p_own = sub.add_parser("own-job", help="Record the participant's chosen job at step 4.")
    p_own.add_argument("--record", required=True)
    p_own.add_argument("--category", required=True)
    p_own.add_argument("--ran", required=True, choices=ALLOWED_OWN_JOB_RAN)
    p_own.add_argument("--note", default="")

    p_fu = sub.add_parser("followup", help="Record 14-day repeat-use follow-up (M17).")
    p_fu.add_argument("--record", required=True)
    p_fu.add_argument("--repeat-use", required=True, choices=ALLOWED_REPEAT_USE)
    p_fu.add_argument("--reason", default="")
    p_fu.add_argument("--date", default=None)

    p_sum = sub.add_parser("summarize", help="Summarize anonymized session JSON records.")
    p_sum.add_argument("records", nargs="+", help="One or more session JSON files.")
    p_sum.add_argument("--format", default="markdown", choices=("markdown", "json"))

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.cmd == "init":
            init_session(
                args.record,
                args.participant,
                args.primary_os,
                args.install_method,
                args.release,
                args.consent,
                second_os=args.second_os,
                force=args.force,
            )
            return 0
        if args.cmd == "run":
            run_interactive(
                args.record,
                args.participant,
                args.primary_os,
                args.install_method,
                args.release,
                args.consent,
                second_os=args.second_os,
                force=args.force,
            )
            return 0
        if args.cmd == "begin":
            begin_step(args.record, args.step)
            return 0
        if args.cmd == "end":
            end_step(args.record, args.step, outcome=args.outcome, note=args.note)
            return 0
        if args.cmd == "event":
            record_event(
                args.record,
                args.step,
                args.kind,
                args.note,
                help_level=args.help_level,
            )
            return 0
        if args.cmd == "own-job":
            record_own_job(args.record, args.category, args.ran, note=args.note)
            return 0
        if args.cmd == "followup":
            record_followup(args.record, args.repeat_use, reason=args.reason, date=args.date)
            return 0
        if args.cmd == "summarize":
            summary = summarize_records(args.records)
            if args.format == "json":
                sys.stdout.write(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
            else:
                sys.stdout.write(format_markdown_summary(summary))
            return 0
    except (ValueError, FileExistsError, FileNotFoundError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
