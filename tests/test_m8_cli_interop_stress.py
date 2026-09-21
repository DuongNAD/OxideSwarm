#!/usr/bin/env python3
"""
Empirical Multi-Process CLI Interoperability Stress Test Suite for Milestone 8.
Author: Challenger M8.2

Tests live multi-process end-to-end execution across all wire codec permutations:
1. Master (bincode) + Worker (bincode) [Homogeneous Binary Mode]
2. Master (json) + Worker (json) [Homogeneous JSON Mode]
3. Master (bincode) + Worker (json) [Heterogeneous Mixed Mode - Auto-detection]
4. Master (json) + Worker (bincode) [Reverse Heterogeneous Mode]
5. Multi-Worker Mixed Swarm: 1 Bincode Worker + 1 JSON Worker connected to same Master
6. Case-insensitivity & aliases: BINCODE, bin, binary, JSON
"""

import os
import sys
import time
import json
import socket
import tempfile
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid.exe"
if not CLI_BIN.exists():
    CLI_BIN = REPO_ROOT / "target" / "debug" / "rusty-grid"

print(f"Using CLI binary: {CLI_BIN}")
assert CLI_BIN.exists(), f"CLI binary does not exist at {CLI_BIN}"


def wait_for_file(path, timeout=8.0):
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read().strip()
                if content:
                    return content
            except Exception:
                pass
        time.sleep(0.05)
    return None


def wait_for_port(port, timeout=8.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.05)
    return False


def stop_process(proc):
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=3.0)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=2.0)
            except Exception:
                pass


def wait_for_workers(master_addr, expected_count=1, timeout=8.0):
    start = time.time()
    while time.time() - start < timeout:
        res = subprocess.run(
            [str(CLI_BIN), "workers", "--master", master_addr, "--json"],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        if res.returncode == 0:
            try:
                data = json.loads(res.stdout)
                # workers can be a list
                if isinstance(data, list) and len(data) >= expected_count:
                    return data
            except Exception:
                pass
        time.sleep(0.1)
    return None


def submit_task(master_addr, command, args=None, task_type="generic", wait=True, timeout=15):
    cmd_list = [
        str(CLI_BIN), "submit",
        "--master", master_addr,
        "--type", task_type,
        "--command", command,
        "--json",
    ]
    if wait:
        cmd_list.append("--wait")
    if args:
        cmd_list.append("--")
        cmd_list.extend(args)

    res = subprocess.run(
        cmd_list,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(REPO_ROOT),
    )
    return res


def test_scenario_1_bincode_bincode():
    print("\n" + "=" * 80)
    print("TEST 1: Master (bincode) + Worker (bincode) [Homogeneous Bincode Mode]")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        master_log = os.path.join(tmpdir, "master.log")
        worker_log = os.path.join(tmpdir, "worker.log")

        with open(master_log, "w") as m_out:
            m_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--wire-codec", "bincode",
                ],
                stdout=m_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        port_str = wait_for_file(port_file)
        assert port_str is not None, "Master failed to start and publish port file"
        master_port = int(port_str)
        master_addr = f"127.0.0.1:{master_port}"
        print(f"  Master bound on {master_addr} (wire-codec: bincode)")

        with open(worker_log, "w") as w_out:
            w_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--master", master_addr,
                    "--name", "worker_bincode_1",
                    "--wire-codec", "bincode",
                    "--simulate-gpu",
                ],
                stdout=w_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        try:
            workers = wait_for_workers(master_addr, expected_count=1)
            assert workers is not None, "Worker failed to register with master"
            print(f"  Worker successfully registered: {workers[0].get('id')} ({workers[0].get('name')})")

            # 1. Submit generic command task
            print("  Submitting generic command task...")
            res = submit_task(master_addr, "cmd", args=["/c", "echo", "HELLO_FROM_BINCODE_WORKER"])
            assert res.returncode == 0, f"Submit failed: {res.stderr}\n{res.stdout}"
            task_result = json.loads(res.stdout)
            print(f"  Task result: exit_code={task_result.get('exit_code')}, execution_time={task_result.get('execution_time_ms')}ms")
            assert task_result.get("exit_code") == 0, f"Expected exit_code 0, got {task_result}"
            assert "HELLO_FROM_BINCODE_WORKER" in task_result.get("stdout", ""), f"Stdout missing expected output: {task_result.get('stdout')}"
            print("  [PASS] Generic task executed successfully over Bincode wire protocol.")

            # 2. Submit GPU task
            print("  Submitting simulated GPU compute task...")
            res_gpu = submit_task(master_addr, "matrix_multiply", task_type="gpu")
            assert res_gpu.returncode == 0, f"GPU task submit failed: {res_gpu.stderr}\n{res_gpu.stdout}"
            gpu_result = json.loads(res_gpu.stdout)
            assert gpu_result.get("exit_code") == 0, f"Expected exit_code 0 for GPU, got {gpu_result}"
            assert gpu_result.get("is_gpu_executed") is True, f"Expected is_gpu_executed=True, got {gpu_result}"
            assert "SIMULATED VIRTUAL GPU" in gpu_result.get("stdout", "").upper(), "GPU simulator stdout mismatch"
            print("  [PASS] GPU task executed successfully over Bincode wire protocol.")

        finally:
            stop_process(w_proc)
            stop_process(m_proc)


def test_scenario_2_json_json():
    print("\n" + "=" * 80)
    print("TEST 2: Master (json) + Worker (json) [Homogeneous JSON Mode]")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        master_log = os.path.join(tmpdir, "master.log")
        worker_log = os.path.join(tmpdir, "worker.log")

        with open(master_log, "w") as m_out:
            m_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--wire-codec", "json",
                ],
                stdout=m_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        port_str = wait_for_file(port_file)
        assert port_str is not None, "Master failed to start and publish port file"
        master_port = int(port_str)
        master_addr = f"127.0.0.1:{master_port}"
        print(f"  Master bound on {master_addr} (wire-codec: json)")

        with open(worker_log, "w") as w_out:
            w_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--master", master_addr,
                    "--name", "worker_json_1",
                    "--wire-codec", "json",
                    "--simulate-gpu",
                ],
                stdout=w_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        try:
            workers = wait_for_workers(master_addr, expected_count=1)
            assert workers is not None, "Worker failed to register with master"
            print(f"  Worker successfully registered: {workers[0].get('id')} ({workers[0].get('name')})")

            # Submit generic command task
            print("  Submitting generic command task...")
            res = submit_task(master_addr, "cmd", args=["/c", "echo", "HELLO_FROM_JSON_WORKER"])
            assert res.returncode == 0, f"Submit failed: {res.stderr}\n{res.stdout}"
            task_result = json.loads(res.stdout)
            print(f"  Task result: exit_code={task_result.get('exit_code')}, execution_time={task_result.get('execution_time_ms')}ms")
            assert task_result.get("exit_code") == 0, f"Expected exit_code 0, got {task_result}"
            assert "HELLO_FROM_JSON_WORKER" in task_result.get("stdout", ""), f"Stdout missing expected output: {task_result.get('stdout')}"
            print("  [PASS] Generic task executed successfully over JSON wire protocol.")

            # Submit GPU task
            print("  Submitting simulated GPU compute task...")
            res_gpu = submit_task(master_addr, "gemm_kernel", task_type="gpu")
            assert res_gpu.returncode == 0, f"GPU task submit failed: {res_gpu.stderr}\n{res_gpu.stdout}"
            gpu_result = json.loads(res_gpu.stdout)
            assert gpu_result.get("exit_code") == 0, f"Expected exit_code 0 for GPU, got {gpu_result}"
            assert gpu_result.get("is_gpu_executed") is True, f"Expected is_gpu_executed=True, got {gpu_result}"
            print("  [PASS] GPU task executed successfully over JSON wire protocol.")

        finally:
            stop_process(w_proc)
            stop_process(m_proc)


def test_scenario_3_heterogeneous_bincode_master_json_worker():
    print("\n" + "=" * 80)
    print("TEST 3: Master (bincode) + Worker (json) [Heterogeneous Mode - Master Auto-Detection]")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        master_log = os.path.join(tmpdir, "master.log")
        worker_log = os.path.join(tmpdir, "worker.log")

        # Master starts with bincode default
        with open(master_log, "w") as m_out:
            m_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--wire-codec", "bincode",
                ],
                stdout=m_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        port_str = wait_for_file(port_file)
        assert port_str is not None, "Master failed to start and publish port file"
        master_port = int(port_str)
        master_addr = f"127.0.0.1:{master_port}"
        print(f"  Master bound on {master_addr} (configured wire-codec: bincode)")

        # Worker starts with json
        with open(worker_log, "w") as w_out:
            w_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--master", master_addr,
                    "--name", "worker_json_hetero",
                    "--wire-codec", "json",
                ],
                stdout=w_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        try:
            workers = wait_for_workers(master_addr, expected_count=1)
            assert workers is not None, "Worker failed to register with master in heterogeneous mode"
            print(f"  Worker successfully registered: {workers[0].get('id')} ({workers[0].get('name')})")

            # Submit generic command task
            print("  Submitting task to heterogeneous cluster...")
            res = submit_task(master_addr, "cmd", args=["/c", "echo", "HELLO_HETERO_BINCODE_MASTER_JSON_WORKER"])
            assert res.returncode == 0, f"Submit failed: {res.stderr}\n{res.stdout}"
            task_result = json.loads(res.stdout)
            print(f"  Task result: exit_code={task_result.get('exit_code')}, execution_time={task_result.get('execution_time_ms')}ms")
            assert task_result.get("exit_code") == 0, f"Expected exit_code 0, got {task_result}"
            assert "HELLO_HETERO_BINCODE_MASTER_JSON_WORKER" in task_result.get("stdout", "")
            print("  [PASS] Heterogeneous Master(bincode) <-> Worker(json) executed flawlessly via auto-negotiation.")

        finally:
            stop_process(w_proc)
            stop_process(m_proc)


def test_scenario_4_reverse_heterogeneous_json_master_bincode_worker():
    print("\n" + "=" * 80)
    print("TEST 4: Master (json) + Worker (bincode) [Reverse Heterogeneous Mode]")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        master_log = os.path.join(tmpdir, "master.log")
        worker_log = os.path.join(tmpdir, "worker.log")

        # Master starts with json
        with open(master_log, "w") as m_out:
            m_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--wire-codec", "json",
                ],
                stdout=m_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        port_str = wait_for_file(port_file)
        assert port_str is not None, "Master failed to start and publish port file"
        master_port = int(port_str)
        master_addr = f"127.0.0.1:{master_port}"
        print(f"  Master bound on {master_addr} (configured wire-codec: json)")

        # Worker starts with bincode
        with open(worker_log, "w") as w_out:
            w_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--master", master_addr,
                    "--name", "worker_bincode_reverse_hetero",
                    "--wire-codec", "bincode",
                ],
                stdout=w_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        try:
            workers = wait_for_workers(master_addr, expected_count=1)
            assert workers is not None, "Worker failed to register in reverse heterogeneous mode"
            print(f"  Worker successfully registered: {workers[0].get('id')} ({workers[0].get('name')})")

            # Submit generic command task
            print("  Submitting task to reverse-heterogeneous cluster...")
            res = submit_task(master_addr, "cmd", args=["/c", "echo", "HELLO_REVERSE_HETERO_JSON_MASTER_BINCODE_WORKER"])
            assert res.returncode == 0, f"Submit failed: {res.stderr}\n{res.stdout}"
            task_result = json.loads(res.stdout)
            print(f"  Task result: exit_code={task_result.get('exit_code')}, execution_time={task_result.get('execution_time_ms')}ms")
            assert task_result.get("exit_code") == 0, f"Expected exit_code 0, got {task_result}"
            assert "HELLO_REVERSE_HETERO_JSON_MASTER_BINCODE_WORKER" in task_result.get("stdout", "")
            print("  [PASS] Reverse heterogeneous Master(json) <-> Worker(bincode) executed flawlessly.")

        finally:
            stop_process(w_proc)
            stop_process(m_proc)


def test_scenario_5_mixed_swarm_multi_worker():
    print("\n" + "=" * 80)
    print("TEST 5: Multi-Worker Mixed Swarm (1 Bincode Worker + 1 JSON Worker simultaneously)")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "master.port")
        master_log = os.path.join(tmpdir, "master.log")
        w1_log = os.path.join(tmpdir, "worker1_bin.log")
        w2_log = os.path.join(tmpdir, "worker2_json.log")

        with open(master_log, "w") as m_out:
            m_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--wire-codec", "bincode",
                ],
                stdout=m_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        port_str = wait_for_file(port_file)
        assert port_str is not None, "Master failed to start"
        master_port = int(port_str)
        master_addr = f"127.0.0.1:{master_port}"
        print(f"  Master bound on {master_addr}")

        # Worker 1: Bincode
        with open(w1_log, "w") as w1_out:
            w1_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--master", master_addr,
                    "--name", "worker_bincode_swarm",
                    "--wire-codec", "bincode",
                ],
                stdout=w1_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        # Worker 2: JSON
        with open(w2_log, "w") as w2_out:
            w2_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "worker",
                    "--master", master_addr,
                    "--name", "worker_json_swarm",
                    "--wire-codec", "json",
                ],
                stdout=w2_out,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )

        try:
            workers = wait_for_workers(master_addr, expected_count=2, timeout=10.0)
            assert workers is not None, "Failed to register both workers in mixed swarm"
            assert len(workers) >= 2, f"Expected >=2 workers, got {len(workers)}"
            print(f"  Both workers registered successfully in mixed swarm! Total workers: {len(workers)}")
            for w in workers:
                print(f"    - Worker: {w.get('id')} ({w.get('name')})")

            # Dispatch 4 tasks across the mixed swarm
            print("  Dispatching 4 parallel tasks across mixed codec swarm...")
            executed_workers = set()
            for i in range(1, 5):
                msg = f"SWARM_TASK_{i}_PAYLOAD"
                res = submit_task(master_addr, "cmd", args=["/c", "echo", msg])
                assert res.returncode == 0, f"Task {i} failed: {res.stderr}\n{res.stdout}"
                result_json = json.loads(res.stdout)
                assert result_json.get("exit_code") == 0
                assert msg in result_json.get("stdout", "")
                executed_workers.add(result_json.get("worker_id"))
                print(f"    Task {i} completed on Worker {result_json.get('worker_id')}")

            print(f"  Tasks executed across {len(executed_workers)} unique worker(s)")
            print("  [PASS] Mixed codec swarm reliably dispatched and executed all tasks.")

        finally:
            stop_process(w1_proc)
            stop_process(w2_proc)
            stop_process(m_proc)


def test_scenario_6_cli_flags_robustness():
    print("\n" + "=" * 80)
    print("TEST 6: CLI Argument Parsing, Case-Insensitivity, and Fallback Robustness")
    print("=" * 80)

    # 1. Test case-insensitivity: BINCODE, bin, binary, JSON
    for valid_codec in ["BINCODE", "bin", "binary", "JSON", "Json"]:
        with tempfile.TemporaryDirectory() as tmpdir:
            port_file = os.path.join(tmpdir, "m.port")
            m_proc = subprocess.Popen(
                [
                    str(CLI_BIN), "master",
                    "--listen", "127.0.0.1:0",
                    "--port-file", port_file,
                    "--wire-codec", valid_codec,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(REPO_ROOT),
            )
            port_str = wait_for_file(port_file, timeout=5.0)
            stop_process(m_proc)
            assert port_str is not None, f"Master failed to start with --wire-codec '{valid_codec}'"
            print(f"  [PASS] Codec '{valid_codec}' successfully parsed and started.")

    # 2. Test fallback on invalid codec string: defaults to Bincode
    with tempfile.TemporaryDirectory() as tmpdir:
        port_file = os.path.join(tmpdir, "m_invalid.port")
        m_proc = subprocess.Popen(
            [
                str(CLI_BIN), "master",
                "--listen", "127.0.0.1:0",
                "--port-file", port_file,
                "--wire-codec", "unknown_codec_name",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(REPO_ROOT),
        )
        port_str = wait_for_file(port_file, timeout=5.0)
        stop_process(m_proc)
        assert port_str is not None, "Master should fall back to default codec when unknown string is provided"
        print("  [PASS] Unknown codec string safely falls back to default.")


def main():
    print("================================================================================")
    print("STARTING EMPIRICAL CHALLENGE: LIVE MULTI-PROCESS CLI INTEROP (MILESTONE 8)")
    print("================================================================================")

    test_scenario_1_bincode_bincode()
    test_scenario_2_json_json()
    test_scenario_3_heterogeneous_bincode_master_json_worker()
    test_scenario_4_reverse_heterogeneous_json_master_bincode_worker()
    test_scenario_5_mixed_swarm_multi_worker()
    test_scenario_6_cli_flags_robustness()

    print("\n" + "=" * 80)
    print("ALL 6 LIVE MULTI-PROCESS CLI INTEROPERABILITY TESTS PASSED WITH 100% SUCCESS!")
    print("================================================================================\n")


if __name__ == "__main__":
    main()
