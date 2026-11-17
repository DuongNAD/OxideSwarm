#!/usr/bin/env python3
"""Run one command for CI, show its output unchanged, and print a heartbeat line every minute.

TEST-29: `workspace tests (windows-latest)` twice lost its hosted runner at the 60-minute mark, and
no log survived to say which test was running. This wrapper:

  * runs the command with its output (stdout and stderr merged, as a CI log shows them) copied
    line by line to this process's stdout and to `--log`, so a later step can print the tail;
  * prints `[heartbeat <UTC time> +<minutes> min] still running; last output line: <line>` every
    `--interval` seconds while the command runs;
  * exits with the command's exit code.

It changes nothing about the command: same arguments, same environment, same working directory.
A CI log is never a terminal, so the command sees a pipe either way.

Usage:  python scripts/ci_heartbeat.py --log <FILE> [--interval 60] -- <command> [args...]
"""

import argparse
import datetime
import os
import subprocess
import sys
import threading
import time


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--log", required=True, help="file that receives a copy of the output")
    parser.add_argument("--interval", type=float, default=60.0, help="seconds between heartbeats")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- <command> [args...]")
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("no command given (put it after --)")

    log_dir = os.path.dirname(os.path.abspath(args.log))
    os.makedirs(log_dir, exist_ok=True)
    out = sys.stdout.buffer
    lock = threading.Lock()
    last_line = [""]
    started = time.monotonic()
    done = threading.Event()

    def heartbeat():
        while not done.wait(args.interval):
            minutes = (time.monotonic() - started) / 60.0
            now = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
            with lock:
                line = f"[heartbeat {now} UTC +{minutes:.0f} min] still running; last output line: {last_line[0]}\n"
                out.write(line.encode("utf-8", "replace"))
                out.flush()

    with open(args.log, "wb") as log:
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except OSError as e:
            print(f"ci_heartbeat: cannot start {command[0]!r}: {e}", file=sys.stderr)
            return 127
        beat = threading.Thread(target=heartbeat, daemon=True)
        beat.start()
        for raw in proc.stdout:
            with lock:
                out.write(raw)
                out.flush()
                log.write(raw)
                log.flush()
                text = raw.decode("utf-8", "replace").strip()
                if text:
                    last_line[0] = text[:200]
        code = proc.wait()
        done.set()
    if code < 0:  # killed by a signal (Unix)
        return 128 - code
    return code


if __name__ == "__main__":
    sys.exit(main())
