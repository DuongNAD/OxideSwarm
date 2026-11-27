# HOST-1: Operating System Overhead for Local Model Inference

Measure how much the operating system costs local LLM decode and prefill speed on the **same machine**, using the **same pinned inference engine** and **exact same GGUF model files**, across four OS states (design 0114, [PR #495](https://github.com/DuongNAD/OxideSwarm/pull/495), method of [design 0093](../../docs/design/0093-benchmarks-worth-showing.md)).

## Plan (written before measurements)

- **Question:** How much faster (or slower) is `llama.cpp` prefill (`pp512`) and decode (`tg128`) on CPU (`Qwen3-0.6B` Q8_0) and GPU (`Qwen3-8B` Q4_K_M, CUDA and Vulkan) on tuned headless Linux versus the Windows 11 desktop?
- **Decision gate (Q106):** `HOST-2`, `HOST-3`, and `HOST-5` move forward only if **tuned headless Linux (`state3`)** beats **Windows 11 desktop as used (`state1`)** by **at least 10 %** on CPU or GPU.
- **Schedule discipline:** Windows measurements (`state1`, `state1b`) run **only after** Task 20 (`TEST-25` 24 h LAN soak) stops cleanly, with no background builds or CPU loops running. Linux measurements (`state2`, `state3`) follow [LINUX_STEPS.md](LINUX_STEPS.md).
- **Privacy rule:** Zero host names, user names, or IP addresses in any record or JSON output.

## Machine & Pinned Software

### Machine under test (Windows case PC)

| Component | Specification |
|---|---|
| CPU | `Intel(R) Core(TM) i5-14600KF` (14 physical cores, 20 logical processors) |
| RAM | 47.79 GiB (`48941 MiB`) |
| GPU | `NVIDIA GeForce RTX 5060 Ti` (`16311 MiB` VRAM, Windows driver `621.24`) |
| Windows OS | `Microsoft Windows 11 Pro` (`10.0.26300` Build `26300`) |
| Linux OS | Ubuntu Server LTS on spare USB/disk (see [LINUX_STEPS.md](LINUX_STEPS.md)) |

### Pinned upstream binaries (`llama.cpp` tag `b12810`)

Every asset comes from `https://github.com/ggml-org/llama.cpp/releases/tag/b12810`, unpacked into scratch folders with nothing installed system-wide:

| OS / Backend | Archive | GitHub Asset SHA-256 | Local Verified SHA-256 |
|---|---|---|---|
| Windows CPU | `llama-b12810-bin-win-cpu-x64.zip` | `845e25154ae4051659cf7363908a663cf15f99c57f8d5157d0f38f468acf660d` | Pending |
| Windows Vulkan | `llama-b12810-bin-win-vulkan-x64.zip` | `134e518ee19a7c9d27b478a0d0817ce4e5f11b4f0d4f721dee8cbde2fc8017ee` | Pending |
| Windows CUDA 13.5 | `llama-b12810-bin-win-cuda-13.5-x64.zip` | `f7646d459e27ad64282f43b612f86fd1cf87e208e6da90b27d1dac92a17e7a23` | Pending |
| Windows CUDA 13.5 runtime | `cudart-llama-bin-win-cuda-13.5-x64.zip` | `c2bf4cc202bfbaaa7a0b75f5082457c3811326947bb216059b72839d844e9cba` | Pending |
| Ubuntu CPU | `llama-b12810-bin-ubuntu-x64.tar.gz` | `63b94be37dcc34cb4933554d4f998e9649841b23da7ffc71d1f66d63762464d7` | Pending (owner) |
| Ubuntu Vulkan | `llama-b12810-bin-ubuntu-vulkan-x64.tar.gz` | `c583739e85d158c93ee0b9fc8d1140736a1d8acdbe52d647bc5eb7cf0a73e214` | Pending (owner) |
| Ubuntu CUDA 13.5 | `llama-b12810-bin-ubuntu-cuda-13.5-x64.tar.gz` | `1268e6b216a5648706ada2bc86f06d2df29d6565c06e3d2b289482b07d9711b3` | Pending (owner) |
| Ubuntu CUDA 13.5 runtime | `cudart-llama-b12810-bin-ubuntu-cuda-13.5-x64.tar.gz` | `b0ccca8408318539c110d7b84e4bd828f538d80800abfe2d82007826d0b781ae` | Pending (owner) |

### Models (Hugging Face official Qwen repositories)

| Model | File | Upstream LFS SHA-256 | Local Verified SHA-256 |
|---|---|---|---|
| Qwen3-0.6B (CPU) | `Qwen3-0.6B-Q8_0.gguf` | `9465e63a22add5354d9bb4b99e90117043c7124007664907259bd16d043bb031` | Pending |
| Qwen3-8B (GPU) | `Qwen3-8B-Q4_K_M.gguf` | `d98cdcbd03e17ce47681435b5150e34c1417f50b5c0019dd560e4882c5745785` | Pending |

## Commands

- **State 1 (Windows as used):**
  ```powershell
  .\benchmarks\host-1\run_host1.ps1 -State state1
  ```
- **State 1b (Windows tidied — High performance plan, browsers and tray closed):**
  ```powershell
  .\benchmarks\host-1\run_host1.ps1 -State state1b
  ```
- **Second machine, CPU-only (Dell laptop via `run_host1.ps1 -CpuOnly`):**
  ```powershell
  .\benchmarks\host-1\run_host1.ps1 -State state1 -CpuOnly -CpuBenchDir <CPU_DIR> -Model06B <MODEL_06B>
  ```
- **State 2 (Headless Linux defaults)** and **State 3 (Headless Linux tuned):** see [LINUX_STEPS.md](LINUX_STEPS.md) and [`run_host1.sh`](run_host1.sh) (including `--cpu-only` for a CPU-only laptop). Note that in `b12810`, `llama-bench` exposes `-lm none` (`--load-mode none`) rather than `--no-mmap`.

Each state executes `llama-bench -p 512 -n 128 -r 5 -o json`:
- CPU: `-ngl 0 -t 1,4,8,14` on `Qwen3-0.6B-Q8_0.gguf`
- GPU CUDA & Vulkan: `-ngl 99` on `Qwen3-8B-Q4_K_M.gguf`
- Tuned state (`state3`): adds `-C <physical-core-mask> --cpu-strict 1 --prio 2` on CPU, `--prio 2` on GPU, and repeats CPU/GPU with `-lm none` (plus `-lm mlock` on CPU if `ulimit -l` permits).

## Results

Status: **Planned / Pending** (scripts prepared; awaiting completion of Task 20 24 h `TEST-25` soak for Windows States 1/1b, and owner Linux boot for States 2/3).

| State | Workload | Backend | Load mode | Threads | pp512 tok/s (mean ± stddev) | tg128 tok/s (mean ± stddev) |
|---|---|---|---|---|---|---|
| `state1` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` | `1,4,8,14` | Pending | Pending |
| `state1` | Qwen3-8B Q4_K_M (GPU) | CUDA 13.5 | `auto` | default | Pending | Pending |
| `state1` | Qwen3-8B Q4_K_M (GPU) | Vulkan | `auto` | default | Pending | Pending |
| `state1b` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` | `1,4,8,14` | Pending | Pending |
| `state1b` | Qwen3-8B Q4_K_M (GPU) | CUDA 13.5 | `auto` | default | Pending | Pending |
| `state1b` | Qwen3-8B Q4_K_M (GPU) | Vulkan | `auto` | default | Pending | Pending |
| `state2` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` | `1,4,8,14` | Pending (Linux) | Pending (Linux) |
| `state2` | Qwen3-8B Q4_K_M (GPU) | CUDA 13.5 | `auto` | default | Pending (Linux) | Pending (Linux) |
| `state2` | Qwen3-8B Q4_K_M (GPU) | Vulkan | `auto` | default | Pending (Linux) | Pending (Linux) |
| `state3` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` / `none` / `mlock` | `1,4,8,14` | Pending (Linux) | Pending (Linux) |
| `state3` | Qwen3-8B Q4_K_M (GPU) | CUDA 13.5 | `auto` / `none` | default | Pending (Linux) | Pending (Linux) |
| `state3` | Qwen3-8B Q4_K_M (GPU) | Vulkan | `auto` / `none` | default | Pending (Linux) | Pending (Linux) |

### Second machine (Dell laptop, CPU-only, `-CpuOnly`)

Results from the Dell laptop (run by `agent-win-2` and folded from hub `test_runs/host1_win-2/`):

| State | Workload | Backend | Load mode | Threads | pp512 tok/s (mean ± stddev) | tg128 tok/s (mean ± stddev) |
|---|---|---|---|---|---|---|
| `state1` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` | `1,4,8,<phys>` | Pending | Pending |
| `state1b` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` | `1,4,8,<phys>` | Pending | Pending |
| `state2` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` | `1,4,8,<phys>` | Pending (Linux) | Pending (Linux) |
| `state3` | Qwen3-0.6B Q8_0 (CPU) | CPU | `auto` / `none` / `mlock` | `1,4,8,<phys>` | Pending (Linux) | Pending (Linux) |

## Q106 Gate (10 % Threshold)

Status: **Pending** — cannot be evaluated until Windows `state1` and Linux `state3` measurements exist.

## What was not measured

- Windows States 1 and 1b have not run yet (deferred until Task 20 `TEST-25` soak finishes cleanly).
- Linux States 2 and 3 have not run yet (requires owner physical boot from a spare USB/disk).
- `llm.asm` bare-metal / QEMU-KVM comparison (optional item in design 0114 D1).
- Multi-request server concurrency (`llama-server` batching), power draw, or multi-GPU setups.
