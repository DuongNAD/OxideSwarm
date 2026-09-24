#!/usr/bin/env python3
"""
OxideSwarm Cross-Platform Coding Agent Mesh - Master Test Runner
File: tests/run_all_mesh_tests.py

Authoritative Verification Runner for Acceptance Criteria (ORIGINAL_REQUEST.md ## 2026-09-23T09:50:43Z):
1. Runs Suite 1: 3-Node Mesh Simulation & Targeted Command Routing (test_agent_mesh_simulation.py)
   - In-process ephemeral OxideRelay Hub
   - 3 Heterogeneous nodes: Node 1 (Windows), Node 2 (macOS), Node 3 (Android)
   - Active Directory Catalog registration verification
   - Node 1 -> Node 3 targeted command routing & stdout/stderr/duration response
   - Node 2 non-interference isolation verification
   - Error handling: DeliveryNack on nonexistent node
2. Runs Suite 2: Bidirectional Zero-Data-Loss & Burst Verification (test_agent_mesh_data_loss.py)
   - Forward 64 KB binary payload transmission with bit-for-bit SHA-256 check
   - Reverse 64 KB binary payload transmission with bit-for-bit SHA-256 check
   - Boundary tests: 0-byte and 128 KB binary transfers
   - Concurrent burst stress test (50 forward + 50 reverse = 100 packets) verifying 0.00% data loss
   - Intermediary node isolation under high packet load
"""

import asyncio
import os
import sys
import time

# Ensure tests and root directory are in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from test_agent_mesh_simulation import run_simulation_tests
    from test_agent_mesh_data_loss import run_data_loss_tests
except ImportError:
    from tests.test_agent_mesh_simulation import run_simulation_tests
    from tests.test_agent_mesh_data_loss import run_data_loss_tests


def print_banner():
    banner = """
================================================================================
          OXIDESWARM AGENT MESH - COMPREHENSIVE ACCEPTANCE TEST SUITE           
                  Cross-Platform Coding Agent Communication PoC                 
               Governing Mandate: ORIGINAL_REQUEST.md (2026-09-23)              
================================================================================
"""
    print(banner)


async def main_async():
    print_banner()
    overall_start = time.time()
    results = []

    # --------------------------------------------------------------------------
    # Suite 1: 3-Node Simulation & Command Routing
    # --------------------------------------------------------------------------
    print("\n>>> Executing Suite 1: 3-Node Mesh Simulation & Command Routing")
    suite1_start = time.time()
    suite1_passed = False
    try:
        suite1_passed = await run_simulation_tests()
    except Exception as e:
        print(f"[FAILED] Suite 1 raised exception: {e}")
        suite1_passed = False
    suite1_duration = time.time() - suite1_start
    results.append(("Suite 1: 3-Node Simulation & Command Routing", suite1_passed, suite1_duration))

    # Small pause between suites
    await asyncio.sleep(0.2)

    # --------------------------------------------------------------------------
    # Suite 2: Bidirectional Data Transmission & Zero Data Loss
    # --------------------------------------------------------------------------
    print("\n>>> Executing Suite 2: Bidirectional Zero-Data-Loss & Burst Verification")
    suite2_start = time.time()
    suite2_passed = False
    try:
        suite2_passed = await run_data_loss_tests()
    except Exception as e:
        print(f"[FAILED] Suite 2 raised exception: {e}")
        suite2_passed = False
    suite2_duration = time.time() - suite2_start
    results.append(("Suite 2: Bidirectional Zero-Data-Loss & Burst", suite2_passed, suite2_duration))

    total_duration = time.time() - overall_start

    # --------------------------------------------------------------------------
    # Summary Report
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("                      ACCEPTANCE TEST EXECUTION SUMMARY                         ")
    print("=" * 80)
    print(f"{'Test Suite Name':<52} | {'Duration':<10} | {'Result':<10}")
    print("-" * 80)

    all_passed = True
    for name, passed, duration in results:
        status_str = "PASSED" if passed else "FAILED"
        if not passed:
            all_passed = False
        print(f"{name:<52} | {duration:>8.3f}s | {status_str:<10}")

    print("-" * 80)
    print(f"Total Execution Time: {total_duration:.3f} seconds")
    print(f"Final Verification Verdict: {'ALL SUITES PASSED (100%)' if all_passed else 'SOME SUITES FAILED'}")
    print("=" * 80 + "\n")

    return 0 if all_passed else 1


def main():
    exit_code = asyncio.run(main_async())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
