#!/usr/bin/env python3
"""
benchmarks/sweep/workload_cpu.py - W1, the synthetic workload of design 0093 (BENCH-2).

A deterministic, CPU-bound task in the Python standard library. It chains SHA-256 over a 32-byte state
`--units` times, starting from a state derived from `--seed`, and prints one line:

    W1 seed=<seed> units=<units> checksum=<64 hex digits>

The checksum depends only on the seed and the number of units, so the same task gives the same checksum
on every operating system and architecture, and a grid run can be checked against a local run. The work
per task is the `--units` parameter; `--calibrate SECONDS` prints how many units take about that long on
this machine.

  python3 workload_cpu.py --seed 7 --units 200000
  python3 workload_cpu.py --calibrate 1.0
"""

import argparse
import hashlib
import sys
import time

FORMAT_VERSION = 1


def initial_state(seed):
    """The 32-byte starting state of task `seed`."""
    return hashlib.sha256(b"oxideswarm-w1-v%d:%d" % (FORMAT_VERSION, seed)).digest()


def run(seed, units):
    """Chain SHA-256 `units` times from the state of `seed`; return the final digest as hex."""
    if units < 0:
        raise ValueError("units must be >= 0")
    state = initial_state(seed)
    sha256 = hashlib.sha256
    for _ in range(units):
        state = sha256(state).digest()
    return state.hex()


def result_line(seed, units, checksum):
    return "W1 seed=%d units=%d checksum=%s" % (seed, units, checksum)


def calibrate(seconds, probe_units=20000):
    """Units that take about `seconds` here, from the time of a probe of `probe_units` units."""
    t0 = time.perf_counter()
    run(0, probe_units)
    per_unit = (time.perf_counter() - t0) / probe_units
    return max(1, int(round(seconds / per_unit))) if per_unit > 0 else probe_units


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=0, help="task seed (default 0)")
    ap.add_argument("--units", type=int, default=100000, help="SHA-256 rounds, the work of one task")
    ap.add_argument("--calibrate", type=float, metavar="SECONDS",
                    help="print the units that take about SECONDS on this machine, and exit")
    args = ap.parse_args(argv)
    if args.calibrate is not None:
        print(calibrate(args.calibrate))
        return 0
    if args.units < 0:
        ap.error("--units must be >= 0")
    print(result_line(args.seed, args.units, run(args.seed, args.units)))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
