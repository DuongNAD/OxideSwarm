# Run GPU workloads under OS isolation

## Goal

Run user GPU command workloads (CUDA, OptiX, Metal, `wgpu`, Blender Cycles, and PyTorch) on an OxideSwarm worker under `--isolation fs` by allowlisting external runtime directories read-only with `--isolation-read-dir` and reserving GPU memory with `--vram-mb`.

## Prerequisites

- A running OxideSwarm master and cluster authentication (`--secret-file`, `OXIDESWARM_SECRET_FILE`, or the default cluster secret file; under `--isolation fs` / `fs+net`, passing the secret via `--secret` or `OXIDESWARM_SECRET` is refused).
- An `oxideswarm` binary built with `--features gpu-wgpu` on GPU workers so the worker enables native GPU compute (`cargo build --release --locked -p rusty_grid_cli --bin oxideswarm --features gpu-wgpu`; see [docs/reference.md](../reference.md#build-from-source)).
- Host GPU drivers and any workload runtimes installed on the worker (for example NVIDIA drivers and CUDA Toolkit, Blender, Python with PyTorch CUDA on Windows/Linux, or macOS Metal with PyTorch MPS).

## Steps

### 1. Start the GPU worker with narrow runtime allowlists

When all external tools live under system or `/opt` paths outside `$HOME`, `--isolation fs` runs `wgpu` Metal (macOS) and native Vulkan/CUDA/OptiX (Windows) directly. When a Python or ML runtime lives inside your home folder or another non-standard directory (such as `~/.pyenv/versions/<PY_VER>` on macOS or `%LOCALAPPDATA%\Programs\Python\Python311` on Windows), pass `--isolation-read-dir <DIR>` (repeatable; alias `--allow-read-dir`, env `RUSTY_GRID_ISOLATION_READ_DIRS`, config `[worker].isolation_read_dirs`; see [design 0074](../design/0074-external-runtime-allowlist-and-gpu-workloads.md) and [docs/reference.md](../reference.md#oxideswarm-worker-flags)).

Broad home directories, `%APPDATA%`, `%LOCALAPPDATA%`, `~/.ssh`, `~/.oxideswarm`, sandbox bases, workspace roots, Cargo credential dirs, and symlinks or NTFS junctions are refused at start-up. On Windows, AppContainer read grants are recorded in `<OXIDESWARM_HOME>\windows_acl_grants.txt` (or `<user_home>\.oxideswarm\windows_acl_grants.txt`) so `windows/revoke-isolation-acls.ps1` can remove them later.

On a macOS GPU worker (Apple Silicon Metal + a pyenv runtime):

```bash
./target/release/oxideswarm worker \
  --master <MASTER_IP>:8088 \
  --gpu \
  --vram-mb <VRAM_MB> \
  --max-concurrency 1 \
  --isolation fs \
  --isolation-read-dir /Users/<USER>/.pyenv/versions/<PY_VER> \
  --secret-file ~/.oxideswarm/cluster.secret
```

On a Windows GPU worker (NVIDIA CUDA/OptiX + an AppData Python 3.11 runtime, PowerShell):

```powershell
.\target\release\oxideswarm.exe worker `
  --master <MASTER_IP>:8088 `
  --gpu `
  --max-concurrency 1 `
  --isolation fs `
  --isolation-read-dir "$env:LOCALAPPDATA\Programs\Python\Python311" `
  --secret-file "$env:USERPROFILE\.oxideswarm\cluster.secret"
```

On Windows machines where `%TEMP%` grants access to another local account or a leftover SID, `ensure_private_dir` refuses the default `%TEMP%\rusty_grid\sandboxes` at start-up. Pass `--sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes"` so the worker creates a private sandbox directory without changing `%TEMP%` permissions (see [benchmarks/gpu-run/README.md](../../benchmarks/gpu-run/README.md#finding-the-default-sandbox-base-on-windows)).

Note for Linux GPU workers: the current Landlock ruleset in `crates/worker/src/isolation/linux.rs` only permits basic character devices (`/dev/null`, `/dev/zero`, `/dev/urandom`, `/dev/random`) and does not yet open `/dev/nvidia*` or `/dev/dri/*`. Run owner-trusted Linux GPU workers with `--isolation off --max-concurrency 1` until the Linux `R20` GPU probe adds device rules ([design 0074](../design/0074-external-runtime-allowlist-and-gpu-workloads.md)).

### 2. Verify advertised GPU and isolation capabilities

On the client machine:

```bash
./target/release/oxideswarm workers --master <MASTER_IP>:8088 --json
```

Confirm the worker entry reports `"has_gpu": true`, your `gpu_device_name`, non-zero `vram_mb` (pass `--vram-mb <VRAM_MB>` on workers whose driver/unified memory does not report fixed dedicated VRAM), and `"iso:fs"` in `tags`.

### 3. Submit GPU workloads with `--gpu`, `--vram-mb`, and `--require-isolation`

On a client machine, run a PyTorch GPU check in the allowlisted Python environment while confining it to `fs` isolation and reserving VRAM on the master scheduler (`crates/master/src/scheduler.rs`):

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --gpu \
  --vram-mb 2048 \
  --require-isolation fs \
  --wait \
  -- /Users/<USER>/.pyenv/versions/<PY_VER>/bin/python3 -c "import torch; print(torch.__version__, torch.backends.mps.is_available())"
```

Targeting a Windows worker with the allowlisted AppData Python runtime:

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --os windows \
  --gpu \
  --vram-mb 2048 \
  --require-isolation fs \
  --wait \
  -- "C:\Users\<USER>\AppData\Local\Programs\Python\Python311\python.exe" -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

Running Blender Cycles on GPU and bringing back the rendered frame with `--output`:

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --gpu \
  --vram-mb 4096 \
  --require-isolation fs \
  --input ./scene.blend:scene.blend \
  --output "render_*.png" \
  --output-dir ./renders \
  --wait \
  -- blender -b scene.blend -o //render_#### -F PNG -f 1 -- --cycles-device CUDA
```

On Windows workers where `blender.exe` is not on the system `PATH`, pass the full executable path under `Program Files` after `--` (for example `"C:\Program Files\Blender Foundation\Blender 4.5\blender.exe"`).

## Verify

On the client machine, run the built-in `wgpu` verification task with explicit VRAM reservation and isolation matching:

```bash
./target/release/oxideswarm submit \
  --master <MASTER_IP>:8088 \
  --type gpu \
  --gpu \
  --vram-mb 512 \
  --require-isolation fs \
  --matrix-dim 512 \
  --wait
```

Expected: output includes `Exit Code: 0` and reports the worker GPU matrix-multiply check succeeded against the CPU sample. Confirm with `oxideswarm workers --master <MASTER_IP>:8088 --json` that `"has_gpu": true` and `"iso:fs"` remain healthy with `state:connected`.

For a recorded two-machine baseline (macOS master to Windows NVIDIA worker running `wgpu`, PyTorch CUDA, and Blender Cycles OptiX), see [benchmarks/gpu-run/README.md](../../benchmarks/gpu-run/README.md).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| macOS task fails with exit code `71` (`sandbox-exec: execvp() ... Operation not permitted`) | Python or runtime binary lives under `$HOME` without a read grant | Pass `--isolation-read-dir` pointing to the specific runtime version directory (for example `~/.pyenv/versions/<PY_VER>`) |
| Windows worker rejects task before launch with executable outside sandbox/toolchain | Runtime directory inside `%LOCALAPPDATA%` or custom folder is not allowlisted | Restart worker with `--isolation-read-dir "<PATH>"`, or use `--isolation off --max-concurrency 1` on a dedicated owner-trusted machine |
| Worker refuses to start after adding `--isolation-read-dir` | Path is `$HOME`, `%LOCALAPPDATA%`, `~/.ssh`, `~/.oxideswarm`, a sandbox/workspace/Cargo dir, missing, or a symlink/junction | Point `--isolation-read-dir` to the exact canonical version or toolkit subfolder |
| Windows staged binary `--input ./app.exe:app.exe --command ./app.exe` fails to spawn on a worker older than 0.3.0 | Before BUG-21 ([design 0077](../design/0077-windows-relative-task-executables.md)) a relative path resolved against the worker's working directory instead of the task sandbox | Upgrade the worker to 0.3.0: `./app.exe`, `.\app.exe` and `sub/app.exe` then run directly under `--isolation off`, `fs` and `fs+net`, and `..\app.exe` is refused. On an older worker, invoke via `cmd.exe -- /d /c .\app.exe` |
| Task stays queued or fails as unfulfillable when `--vram-mb` is set | Connected GPU worker reports `vram_mb: null` (unified/simulated) or less free VRAM than requested | Set `--vram-mb <MB>` on the worker start command (or pass a smaller `--vram-mb`) |
| Need to remove AppContainer ACLs added by `--isolation-read-dir` on Windows | Start-up granted `oxideswarm.toolchain` read/execute ACEs recorded in `windows_acl_grants.txt` | Run `powershell -NoProfile -ExecutionPolicy Bypass -File windows\revoke-isolation-acls.ps1` |
| Windows worker refuses default `%TEMP%\rusty_grid\sandboxes` at start-up | `%TEMP%` DACL, or the DACL of an ancestor such as `AppData`, grants rights to another account or leftover SID (`ensure_private_dir`); the error names the SID and the directory | Pass `--sandbox-base-dir "$env:LOCALAPPDATA\OxideSwarm\sandboxes"`. If the error names `AppData` itself, `%LOCALAPPDATA%` is refused too: use a directory outside `AppData` that grants only your user, `SYSTEM` and `Administrators` (for example, in `cmd.exe`: `icacls <dir> /inheritance:r /grant:r "%USERNAME%:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F"`), and find out what added the foreign entry to `AppData` |
