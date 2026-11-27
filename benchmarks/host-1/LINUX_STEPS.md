# Running HOST-1 States 2 and 3 on Linux (Owner Steps)

These steps run the **same** pinned `llama.cpp` build (`b12810`) and **exact same** Qwen3 GGUF files on headless Ubuntu Server LTS on the RTX 5060 Ti PC so we can compare Windows 11 (`state1` / `state1b`) against default headless Linux (`state2`) and tuned headless Linux (`state3`) under [design 0114](https://github.com/DuongNAD/OxideSwarm/pull/495).

> **Safety rules**
> - Use a **spare USB stick or spare disk** only. Do **not** touch the OxideSwarm developer kit USB stick and do **not** modify or repartition the PC's internal Windows drive.
> - Keep scratch models and binaries in `/tmp/host1` (outside `$HOME` and outside the repo) so no username appears in tool logs.

## 1. Install Ubuntu Server LTS onto a spare USB stick or disk

1. Flash current **Ubuntu Server 24.04 LTS** (or current LTS) onto a spare USB drive / spare SSD and boot this PC from it.
2. Connect Ethernet on the home LAN and update apt indexes:
   ```bash
   sudo apt-get update
   ```

## 2. Install the distribution NVIDIA open driver and Vulkan runtime

Blackwell GPUs (RTX 5060 Ti) require the open kernel module flavour from the Ubuntu repository:

```bash
sudo apt-get install -y ubuntu-drivers-common pciutils libvulkan1 curl jq
sudo ubuntu-drivers install
sudo reboot
```

After rebooting cleanly into headless Linux (no display manager running):

```bash
nvidia-smi --query-gpu=name,driver_version,memory.total,memory.free --format=csv,noheader
```

Confirm the output lists `NVIDIA GeForce RTX 5060 Ti`, driver version, total `16311 MiB`, and nearly full free VRAM (no desktop session holding 1.4–2.4 GiB).

## 3. Stage the pinned `b12810` Linux archives and models in `/tmp/host1`

Download (or copy over LAN from the Windows scratch folder) the exact files below into `/tmp/host1`:

| Asset | Expected SHA-256 |
|---|---|
| `llama-b12810-bin-ubuntu-x64.tar.gz` | `63b94be37dcc34cb4933554d4f998e9649841b23da7ffc71d1f66d63762464d7` |
| `llama-b12810-bin-ubuntu-vulkan-x64.tar.gz` | `c583739e85d158c93ee0b9fc8d1140736a1d8acdbe52d647bc5eb7cf0a73e214` |
| `llama-b12810-bin-ubuntu-cuda-13.5-x64.tar.gz` | `1268e6b216a5648706ada2bc86f06d2df29d6565c06e3d2b289482b07d9711b3` |
| `cudart-llama-b12810-bin-ubuntu-cuda-13.5-x64.tar.gz` | `b0ccca8408318539c110d7b84e4bd828f538d80800abfe2d82007826d0b781ae` |
| `Qwen3-0.6B-Q8_0.gguf` (`Qwen/Qwen3-0.6B-GGUF`) | `9465e63a22add5354d9bb4b99e90117043c7124007664907259bd16d043bb031` |
| `Qwen3-8B-Q4_K_M.gguf` (`Qwen/Qwen3-8B-GGUF`) | `d98cdcbd03e17ce47681435b5150e34c1417f50b5c0019dd560e4882c5745785` |

Extract each archive cleanly (note that CUDA and `cudart` unpack into the **same** directory):

```bash
mkdir -p /tmp/host1/bin/ubuntu-x64 \
         /tmp/host1/bin/ubuntu-vulkan-x64 \
         /tmp/host1/bin/ubuntu-cuda-13.5-x64 \
         /tmp/host1/models

sha256sum /tmp/host1/downloads/*
tar -xzf /tmp/host1/downloads/llama-b12810-bin-ubuntu-x64.tar.gz -C /tmp/host1/bin/ubuntu-x64
tar -xzf /tmp/host1/downloads/llama-b12810-bin-ubuntu-vulkan-x64.tar.gz -C /tmp/host1/bin/ubuntu-vulkan-x64
tar -xzf /tmp/host1/downloads/llama-b12810-bin-ubuntu-cuda-13.5-x64.tar.gz -C /tmp/host1/bin/ubuntu-cuda-13.5-x64
tar -xzf /tmp/host1/downloads/cudart-llama-b12810-bin-ubuntu-cuda-13.5-x64.tar.gz -C /tmp/host1/bin/ubuntu-cuda-13.5-x64
```

Copy `benchmarks/host-1/run_host1.sh` (from this repository/branch) to `/tmp/host1/run_host1.sh` and ensure `chmod +x /tmp/host1/run_host1.sh`.

## 4. Run State 2 (Headless Linux, OS Defaults)

Leave CPU governor and transparent huge pages at their Ubuntu defaults, close background tasks, and run:

```bash
/tmp/host1/run_host1.sh \
  --state state2 \
  --cpu-dir /tmp/host1/bin/ubuntu-x64 \
  --vulkan-dir /tmp/host1/bin/ubuntu-vulkan-x64 \
  --cuda-dir /tmp/host1/bin/ubuntu-cuda-13.5-x64 \
  --model-06b /tmp/host1/models/Qwen3-0.6B-Q8_0.gguf \
  --model-8b /tmp/host1/models/Qwen3-8B-Q4_K_M.gguf \
  --out-dir /tmp/host1-results/state2
```

## 5. Run State 3 (Headless Linux, Tuned)

Apply the OS-level tunings from design 0114 D1 (`governor=performance`, Transparent Huge Pages `always`, and unlocked `memlock` if testing `-lm mlock`):

> **Flag note for `b12810`:** In `llama.cpp b12810`, `--no-mmap` was replaced by `-lm none` (`--load-mode none`). `run_host1.sh --tuned` runs default `-lm auto`, `-lm none`, and optional `-lm mlock`, auto-detects the physical-core hex mask from `/proc/cpuinfo`, and sets `--cpu-strict 1 --prio 2`. You may also test THP `madvise` in a second `--state custom --out-dir /tmp/host1-results/state3-madvise` run if desired.

```bash
sudo sh -c 'for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -w "$g" ] && echo performance > "$g"; done'
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag

# Optional: raise locked-memory limit in this shell so `-lm mlock` can run cleanly
ulimit -l unlimited || true

/tmp/host1/run_host1.sh \
  --state state3 \
  --tuned \
  --cpu-dir /tmp/host1/bin/ubuntu-x64 \
  --vulkan-dir /tmp/host1/bin/ubuntu-vulkan-x64 \
  --cuda-dir /tmp/host1/bin/ubuntu-cuda-13.5-x64 \
  --model-06b /tmp/host1/models/Qwen3-0.6B-Q8_0.gguf \
  --model-8b /tmp/host1/models/Qwen3-8B-Q4_K_M.gguf \
  --out-dir /tmp/host1-results/state3
```

## 6. Copy the JSON folders back cleanly

Copy `/tmp/host1-results/state2` and `/tmp/host1-results/state3` back to the Windows workspace / hub handoff location. Verify zero host names, user names, or IP addresses appear in the JSON files before handing them back to `agent-win` to update `benchmarks/host-1/README.md` and run `summarize_host1.py`.
