# GPU tasks across two machines

A dated record of real GPU work sent through the grid from one machine to another. The master runs
on a Mac. One worker runs on a Windows machine with an NVIDIA GPU. The tasks are the built-in
`wgpu` matrix multiply and two `--command` tasks that use CUDA through programs installed on the
worker: PyTorch and Blender Cycles.

This record is the isolation-off baseline. It is not the run under `--isolation fs` that spike R20
of [design 0064](../../docs/design/0064-research-of-2026-11-13-and-route.md) asks for, and it
does not change the status of R20 or FEAT-9.

| Run | Isolation | Status |
|---|---|---|
| 1. Three GPU tasks from a macOS master to a Windows worker | `off` | One record, 2026-11-16 |
| 2. The same tasks under `--isolation fs`, with the runtime allowlist of design 0074 | `fs` | Planned |

## Run 1: three GPU tasks, isolation off

### Setup

| Role | Machine | Software |
|---|---|---|
| Master and client | Apple M5, macOS 27.2.1 | `oxideswarm` release build of `6a88eff`, protocol version 9 |
| Worker | Windows 11, NVIDIA GeForce RTX 5060 Ti (16 GiB), driver 621.24, 20 CPU cores, 48941 MiB RAM | `oxideswarm.exe` release build of `6a88eff` with `--features gpu-wgpu`; Python 3.11 with PyTorch `2.8.0+cu128`; Blender 4.5.4 LTS |

Both machines were on one LAN and held the same cluster secret. The master used a private `HOME`,
its own state directory and port 9088, so it did not touch another master on the same Mac.

### Commands

On the Mac, start the master:

```bash
HOME=<PRIVATE_HOME> oxideswarm master --listen 0.0.0.0:9088 --dashboard-port 9080 \
  --state-dir <STATE_DIR> --secret-file <SECRET_FILE>
```

On the Windows machine, in PowerShell, start the worker:

```powershell
oxideswarm.exe worker --master <MASTER_IP>:9088 --name rtx5060ti-gpurun --gpu --isolation off --max-concurrency 1 --no-portal --sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes" --secret-file $HOME\.oxideswarm\cluster.secret
```

`--sandbox-base-dir` was needed only because of the [finding](#finding-the-default-sandbox-base-on-windows) below.

On the Mac, in this directory, submit the three tasks. Each command also takes
`--master 127.0.0.1:9088 --secret-file <SECRET_FILE>`.

```bash
oxideswarm submit --type gpu --gpu --matrix-dim 4096 --worker rtx5060ti-gpurun --timeout-secs 120 --wait
```

```bash
oxideswarm submit --vram-mb 2000 --worker rtx5060ti-gpurun --input torch_step.py --timeout-secs 300 --wait \
  --command '<PYTHON_EXE>' -- torch_step.py
```

```bash
oxideswarm submit --vram-mb 2000 --worker rtx5060ti-gpurun --input render.py --output render.png \
  --output-dir out --timeout-secs 600 \
  --command 'C:\Program Files\Blender Foundation\Blender 4.5\blender.exe' -- -b --factory-startup --python render.py
```

`<PYTHON_EXE>` is the full path of the worker's `python.exe`. Both scripts exit with a non-zero
code when they find no CUDA or OptiX device, so a task cannot pass on the CPU.

### Results

All three tasks ended with exit code 0 on the worker `rtx5060ti-gpurun`. The values come from the
task output. [results/2026-11-16-macos-master-to-windows-rtx5060ti.json](results/2026-11-16-macos-master-to-windows-rtx5060ti.json)
holds the same values.

| Task | Result | Task time |
|---|---|---|
| `wgpu` GEMM, 4096 × 4096, Vulkan | 342 ms for 137.4 GFLOP, about 400 GFLOP/s. 4096 entries recomputed in `f64` on the CPU, maximum relative error 5.3e-6, `VERIFIED_OK` | 342 ms |
| PyTorch CUDA, MLP 4096-4096-10, batch 1024, AdamW, 200 steps (`torch_step.py`) | First step 9754 ms, which includes CUDA start-up. Then 19.61 ms per step. Peak memory 673 MiB. Loss 2.3060 to 0.0000 | 20495 ms |
| Blender Cycles, OptiX, factory scene, 1920 × 1080, 256 samples (`render.py`) | Render 10502 ms. `render.png` (1583316 bytes) came back to the Mac through `--output` | 12040 ms |

The SHA-256 of the returned `render.png` is
`6a300109467308322e17ed4106e10e59c8ffc3dddfff77fb7506de00b5d08fd4`. The image is not in the
repository.

### What the run shows

- A client on one machine can run CUDA and OptiX work on another machine's GPU through the grid,
  with input files sent in and an output file brought back.
- `--vram-mb` (design 0068) works with `--command` tasks. The run says nothing about placement:
  there was one worker, and `--worker` named it.
- The PyTorch loss reaching 0.0000 only shows that the network memorised 1024 random samples. It is
  not a measure of model quality.

### What the run does not show

- Nothing ran under `--isolation fs`. The worker ran each task as its own user, with full access to
  that user's files.
- One worker, one task at a time. No two GPU tasks shared the device.
- One run of each task. The times are single samples, not a benchmark.
- The worker binary was built from `6a88eff`. Later commits are not covered.

### Finding: the default sandbox base on Windows

The first start of the worker failed before it connected. Without `--sandbox-base-dir` the worker
uses `%TEMP%\rusty_grid\sandboxes`. On this machine `%TEMP%` gave rights to two accounts other than
the worker's user: another local account and a SID that no longer resolves. The check of
`ensure_private_dir` in `crates/worker/src/sandbox.rs` (design 0021) refused the directory because
those accounts could rename or delete it. It printed an `icacls` command that would make `%TEMP%`
private, and named `--sandbox-base-dir` as the other choice.

The run used `--sandbox-base-dir "%LOCALAPPDATA%\OxideSwarm\sandboxes"`. The check walks the
ancestors from the drive root down, and every ancestor up to `%LOCALAPPDATA%` passed. The worker
then created the missing directories with its owner-only DACL. The ACL of `%TEMP%` was not changed.

The refusal is the intended behaviour. The cost is that a worker on such a machine does not start
with its defaults.
