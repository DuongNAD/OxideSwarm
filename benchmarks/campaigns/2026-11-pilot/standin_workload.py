#!/usr/bin/env python3
"""
benchmarks/campaigns/2026-11-pilot/standin_workload.py

Deterministic stand-in workload for APP-6 step 2 (dry run on released 0.4.4)
while `Anima-Engine`'s `run_esel1_experiment` runner is updated to accept seed
and grid parameters and emit a deterministic checksum file.

Standard library only. Executes three synthetic deterministic arms (`control`,
`treatment`, `drift`) from (`--seed`, `--founders`, `--food-cap-abundant`,
`--food-cap-scarce`, `--duration-ticks`) and writes:
  - `<out>/smoke/verify_checksums.txt`: deterministic `seed,arm,final_checksum`
    rows (target of `oxideswarm submit --verify-file`).
  - `<out>/smoke/runs.csv`: per-arm summary including wall-clock `us_per_tick`,
    intentionally non-deterministic across runs just like `Anima-Engine`.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import struct
import time

FORMAT_VERSION = 1
ARMS = ("control", "treatment", "drift")


def arm_final_checksum(
    seed: int,
    arm: str,
    founders: int,
    food_abundant: int,
    food_scarce: int,
    duration_ticks: int,
    work_units: int,
) -> int:
    """Compute a deterministic 32-bit unsigned integer checksum for one arm."""
    header = (
        f"oxideswarm-app6-standin-v{FORMAT_VERSION}:"
        f"seed={seed}:arm={arm}:n0={founders}:"
        f"fa={food_abundant}:fs={food_scarce}:ticks={duration_ticks}"
    ).encode("utf-8")
    state = hashlib.sha256(header).digest()
    sha256 = hashlib.sha256
    for _ in range(work_units):
        state = sha256(state).digest()
    return struct.unpack(">I", state[:4])[0]


def run_trial(
    *,
    seed: int,
    founders: int,
    food_abundant: int,
    food_scarce: int,
    duration_ticks: int,
    out_dir: str,
    work_units: int,
) -> tuple[str, str]:
    """Run all three arms and write `verify_checksums.txt` and `runs.csv`."""
    smoke_dir = os.path.join(out_dir, "smoke")
    os.makedirs(smoke_dir, exist_ok=True)

    verify_lines: list[str] = []
    csv_lines: list[str] = ["seed,arm,founders,food_cap_abundant,food_cap_scarce,duration_ticks,us_per_tick,final_checksum"]

    for arm in sorted(ARMS):
        t0 = time.perf_counter()
        checksum = arm_final_checksum(
            seed, arm, founders, food_abundant, food_scarce, duration_ticks, work_units
        )
        elapsed_us = max(1.0, (time.perf_counter() - t0) * 1_000_000.0)
        us_per_tick = elapsed_us / max(1, duration_ticks)
        verify_lines.append(f"{seed},{arm},{checksum}")
        csv_lines.append(
            f"{seed},{arm},{founders},{food_abundant},{food_scarce},{duration_ticks},{us_per_tick:.4f},{checksum}"
        )

    verify_path = os.path.join(smoke_dir, "verify_checksums.txt")
    with open(verify_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(verify_lines) + "\n")

    runs_csv_path = os.path.join(smoke_dir, "runs.csv")
    with open(runs_csv_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(csv_lines) + "\n")

    return verify_path, runs_csv_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, required=True, help="Deterministic simulation seed")
    ap.add_argument("--founders", type=int, default=30, help="Initial founder count (default: 30)")
    ap.add_argument("--food-cap-abundant", type=int, default=50, help="Abundant food cap (default: 50)")
    ap.add_argument("--food-cap-scarce", type=int, default=13, help="Scarce food cap (default: 13)")
    ap.add_argument("--duration-ticks", type=int, default=18000, help="Simulated ticks parameter (default: 18000)")
    ap.add_argument("--out", default="out", help="Output base directory (default: out)")
    ap.add_argument(
        "--work-units",
        type=int,
        default=40000,
        help="SHA-256 steps per arm for bounded CPU work in the dry run (default: 40000)",
    )
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.work_units < 1 or args.duration_ticks < 1:
        print("error: --work-units and --duration-ticks must be >= 1", file=sys.stderr)
        return 2
    verify_path, _ = run_trial(
        seed=args.seed,
        founders=args.founders,
        food_abundant=args.food_cap_abundant,
        food_scarce=args.food_cap_scarce,
        duration_ticks=args.duration_ticks,
        out_dir=args.out,
        work_units=args.work_units,
    )
    print(f"standin_workload: wrote {verify_path} (seed={args.seed})")
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main())
