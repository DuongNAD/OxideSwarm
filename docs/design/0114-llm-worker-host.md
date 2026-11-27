# 0114: A worker host tuned for local models: measure first, then huge pages, pinned cores and a headless Linux image

Status: Draft (plan; builds nothing; Q104 to Q107 open)
Created: 2026-11-27    Updated: 2026-11-27

## Summary

On 2026-11-27 the owner asked whether OxideSwarm needs an operating system of its own to run local
models faster, after reading about `llm.asm`, a 24 KB bare-metal x86-64 kernel that runs Qwen3-0.6B.
The author's own figures show that most of its speed comes from a hand-tuned engine that is just as
fast as a plain Linux program, and that dropping the OS adds about 10 %. This record keeps the OS
question closed (Q104), uses upstream engines (Q105), and plans five items: measure the gap on our own
machines first (HOST-1), then report host factors (HOST-2), give model services a tuned process
profile (HOST-3), write a guide (HOST-4), and build a headless Linux worker image (HOST-5).

## Context

### What the owner read (checked on 2026-11-27)

- [`alesha-pro/llm.asm`](https://github.com/alesha-pro/llm.asm), created 2026-11-25: a 14,712-byte
  static Linux ELF and a 24,252-byte bootable kernel, both in NASM, running `Qwen/Qwen3-0.6B` (Q8_0)
  on CPU only. Its README measures decode (34 prompt tokens, 64 generated, 3-run average) on one AMD
  EPYC 7642 against `llama.cpp` (`-ngl 0`):

| Threads | `llama.cpp` on Linux | `llm.asm` ELF on Linux | `llm.asm` bare-metal (KVM) |
|---|---|---|---|
| 1 | 16.63 tok/s | 20.70 tok/s (+24.5 %) | 23.67 tok/s |
| 4 | 51.08 tok/s | 69.24 tok/s (+35.6 %) | 78.89 tok/s |
| 16 | 103.03 tok/s | 121.86 tok/s (+18.3 %) | 134.94 tok/s |

  The README credits the ELF's lead to INT8×INT8 AVX2 kernels, FP16 block scales, prefetch and a
  head-major KV cache, and the bare-metal build's extra "~10 %" to 2 MiB huge-page identity
  mapping. Prefill is token by token, where `llama.cpp` uses batched GEMM. The bare-metal build
  shares the ELF's assembly core, gets its weights from the boot loader as Multiboot modules and
  drives VGA, PS/2 and a serial port: no network or disk driver, no GPU.
- [`silicatetech/zero`](https://github.com/silicatetech/zero), created 2026-06-17, no stars: a Rust
  bare-metal kernel that reports 194.5 tok/s for a 1.7B model on an EPYC 9354P, +27.3 % over
  `llama.cpp` on the same host, and under 1.5 GB of RAM where Linux plus `llama.cpp` needs about
  2.5 GB. Its own roadmap lists GPU offload as planned. Unverified by anyone else.
- Articles comparing Windows and Linux on the same GPU disagree, from 1–5 %
  ([PromptQuorum](https://www.promptquorum.com/local-llms/mac-vs-windows-vs-linux-local-llm)) to a
  single 2x result on older hardware
  ([aiproductivity.ai](https://aiproductivity.ai/news/local-llm-inference-linux-vs-windows-speed/)).
  The mechanism they agree on is memory: a Windows desktop holds 1.4–2.4 GB of VRAM
  ([besttechguidepro](https://www.besttechguidepro.com/2026/11/wsl2-vs-native-ubuntu-local-ai-cuda-benchmark.html),
  one source), and WSL2 adds a layer.

### Where the speed of a local model comes from

| Factor | Size of effect (from the sources above) | Who controls it | In OxideSwarm today |
|---|---|---|---|
| Engine kernels for the model and quantisation | +18 % to +36 % (`llm.asm` ELF vs `llama.cpp`) | The engine | Not ours: `LLM-12` (design 0062) runs an upstream model server |
| Memory bandwidth of the device | Sets the ceiling for decode | Hardware | Placement by speed (design 0060), VRAM by `vram_mb` (design 0068) |
| Usable VRAM (desktop, driver reservations) | 1.4–2.4 GB on a Windows desktop (one source) | OS and session | `FEAT-9b` (design 0067 D2) plans to report usable, not total |
| Huge pages for weights and KV cache | About 10 % (`llm.asm` bare-metal vs ELF) | OS setting plus engine | Not set or reported |
| Threads on physical cores, no co-runners | Not measured here | OS scheduler plus engine flags | Task priority is below-normal by default (`process_tree.rs`) |
| Removing the OS altogether | What is left after the rows above: small | A new kernel | Out of scope (Q104) |

### What a worker does today

`TaskRunner` starts every task at `ProcessPriority::BelowNormal` (`nice` 10 on Unix, the
below-normal class on Windows; `RUSTY_GRID_TASK_PRIORITY` overrides it, `process_tree.rs`). That is
right on a machine somebody is using, and wrong for a model service on a machine that does nothing
else. The worker does not pin threads, does not look at huge pages, and does not say whether a
desktop session holds the GPU. Model services themselves (`LLM-9` `worker --serve`, `LLM-12` the
runner) are Planned for 0.6.0.

### For ordinary grid tasks

The OS question matters little for builds, tests and batch jobs: their time goes into the task
itself and into per-task overhead, which `BENCH-5` and `BENCH-9` (design 0093) measure, and into
placement (design 0060) and caching (`CACHE-1`). This record covers model work only.

## Goals

- G1. Know the real gap between a Windows desktop, a headless Linux and a tuned headless Linux on our
  own machines, for CPU and GPU decode, before building anything (HOST-1).
- G2. A worker says which host factors it has, so a person and later the scheduler can see why one
  machine is slower (HOST-2).
- G3. A model service on a machine marked for it runs with the profile that measured fastest:
  priority, cores, huge pages, memory locking (HOST-3).
- G4. A person with a spare machine can turn it into a fast model worker in one boot from the USB
  kit, with network and GPU working (HOST-5), or tune an existing install by following a guide
  (HOST-4).

## Non-goals

- An OxideSwarm operating system, kernel or unikernel (Q104). A grid worker needs a network stack,
  storage, GPU drivers (CUDA and Metal are tied to Linux, Windows and macOS), and the contributor's
  own machine stays as it is: the research grid of design 0094 depends on people lending what they
  already use.
- An inference engine of our own (Q105). A hand-written engine per research problem stays possible
  as an ordinary task.
- Tuning for machines a person is using. The contributor controls of design 0113 (NET-9, draft) and
  the tray app of design 0112 (draft) cover those; HOST-3 applies only where the owner opted in.
- Android and macOS images. macOS has no hard CPU affinity; HOST-2 and HOST-3 report and use what it
  has.

## Design

### D1 Measure first (HOST-1, spike)

Run the same model and engine on the same machine in three states, three runs each, decode and
prefill separately, recording machine, driver, engine commit and flags in a benchmark record under
`benchmarks/` by the method of design 0093:

1. Windows 11 desktop as used today (the RTX 5060 Ti case PC).
2. Headless Linux on the same machine, booted from a USB stick (current Ubuntu Server LTS, NVIDIA
   driver from the distribution).
3. The same Linux, tuned: huge pages for the weights (transparent huge pages on `madvise` or
   `always`, `llama.cpp` with and without `--no-mmap` and `--mlock`), threads on physical cores
   (`llama.cpp` CPU mask and `--cpu-strict`; flag names checked against the engine commit used),
   CPU governor `performance`.

Models: Qwen3-0.6B Q8_0 on CPU (to compare with the `llm.asm` table) and a 7–8B Q4 model on GPU.
On the Dell win-2 (CPU only) only states 1 and 3. Optional: boot `llm.asm` in QEMU/KVM on the Linux
state for one CPU data point. Exit: a record with the numbers, and a yes or no for each of HOST-3 and
HOST-5 by Q106 and Q107.

### D2 Host factors in the worker report (HOST-2)

At start-up the worker reads, without root:

- Linux: `/sys/kernel/mm/transparent_hugepage/enabled`, free huge pages from `/proc/meminfo`,
  the CPU governor, `isolcpus` from `/proc/cmdline`, physical versus logical cores, whether a
  display server holds the GPU (NVML process list where the driver exposes it).
- Windows: whether an interactive desktop holds the adapter (from `FEAT-9b`'s usable-budget query),
  whether the account has `SeLockMemoryPrivilege` (needed for large pages).
- macOS: physical cores and `recommendedMaxWorkingSetSize` (design 0067 D2).

It logs one line and `oxideswarm doctor` prints it. Whether these also become worker tags (a new
reserved prefix, so placement can prefer tuned hosts) is decided after HOST-1: a tag nobody places on
is noise.

### D3 A process profile for model services (HOST-3)

A model service (`LLM-9`, `LLM-12`) on a worker started with an explicit opt-in runs with the
profile HOST-1 found fastest: normal priority instead of below-normal, threads limited to physical
cores and pinned where the OS allows (Linux `sched_setaffinity`, Windows CPU sets; macOS none), huge
pages where available, memory locked when the account may. Most of this is flags and environment
handed to the upstream engine, not code in the worker. The opt-in is the existing
`RUSTY_GRID_TASK_PRIORITY=normal` plus one worker setting for pinning (named in the HOST-3 pull
request, with its design record entry and `docs/reference.md` row). Ordinary tasks keep today's
behaviour.

### D4 Guide (HOST-4)

`docs/guides/fast-model-worker.md`: when a dedicated Linux boot is worth it (from HOST-1's numbers),
what to set on Windows (no desktop GPU users, large-page privilege) and on Linux (huge pages,
governor), and how to check with `doctor`. Can ship before any code.

### D5 Headless Linux worker image (HOST-5)

A bootable image for a machine lent full time: current Ubuntu Server LTS, minimal, no desktop,
distribution NVIDIA driver, OxideSwarm installed as a systemd service under a non-root user (design
rules: never root, no hard-coded addresses), joining on first boot from the USB kit's `invite.oxs`
(`oxideswarm usb create`, `join --invite-file`). Tuned as in D3, with
`RUSTY_GRID_TASK_PRIORITY=normal` (Q107). Built by a script in `packaging/linux-image/` from the
distribution's own autoinstall format, so it rebuilds for each release; images are attested like the
release archives. The machine's original OS stays untouched when the image runs from the stick; an
install to disk is a separate, explicit step.

## Compatibility

No wire change. HOST-2 adds log and `doctor` output; a tag prefix, if Q106's follow-up adds one, is
a placement change with its own record. HOST-3 changes nothing unless the opt-in is set. HOST-5 is
new packaging.

## Security

HOST-2 reads only world-readable files and APIs. HOST-3 raises task priority and locks memory only
on opt-in; locked memory is capped by the OS limit (`RLIMIT_MEMLOCK`), which the worker does not
raise. HOST-5 ships a worker that runs as a dedicated non-root user, takes the cluster secret only
from the kit's encrypted invite, and keeps `--isolation` as configured; the image has no SSH server
unless the person adds one.

## Test plan

- HOST-1: the benchmark record itself, with the scripts that produced it (design 0093 method).
- HOST-2: unit tests that parse sample `/sys` and `/proc` contents; `doctor` end-to-end test checks
  the line appears.
- HOST-3: a Linux integration test that a service task under the opt-in runs at nice 0 with the
  expected affinity mask, and an ordinary task does not.
- HOST-5: boot the image in QEMU in CI (no GPU), check that the worker registers with a test master;
  a live run on the case PC with GPU.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| HOST-1 | Spike: OS overhead of model decode on our machines | `benchmarks/`, hub `test_runs/` | Record with three states on the case PC and two on win-2 | Planned |
| HOST-2 | Host factors in the worker log and `doctor` | `crates/worker/src/`, `crates/cli/src/doctor.rs` | One line on start-up and in `doctor` on Linux, Windows, macOS | Planned |
| HOST-3 | Process profile for model services on an opted-in worker | `crates/worker/src/process_tree.rs`, runner of `LLM-12` | Integration test above passes; HOST-1 gain reproduced through the grid | Planned |
| HOST-4 | Guide: a fast model worker | `docs/guides/fast-model-worker.md` | Guide cites HOST-1 numbers | Planned |
| HOST-5 | Headless Linux worker image | `packaging/linux-image/` | Image boots, joins from the kit, serves a model on the case PC | Planned |

Order: HOST-1 and HOST-4 need no code and can run in 0.4.x; HOST-2, HOST-3 and HOST-5 go with
model serving in 0.6.0 (Q101), after `LLM-9`. `docs/BACKLOG.md` stays unchanged until the owner
decides Q104 to Q107.

## Decisions for the owner

| ID | Question | Recommended |
|---|---|---|
| Q104 | Build an OxideSwarm operating system or bare-metal kernel? | No. Reopen only if HOST-1 shows a gap of more than 25 % that a tuned headless Linux does not close |
| Q105 | Write our own inference engine? | No: run upstream engines (`llama.cpp`, vLLM on Linux GPUs) through `LLM-12`; custom kernels only as ordinary tasks for a given research problem |
| Q106 | Go ahead with HOST-2, HOST-3 and HOST-5 after HOST-1? | Yes if HOST-1 shows at least 10 % for the tuned headless state over the Windows desktop on either CPU or GPU; otherwise only HOST-4 |
| Q107 | Should the HOST-5 image run tasks at normal priority by default? | Yes: the image is only for machines lent full time; ordinary installs keep below-normal |

## Status log

- 2026-11-27: Draft. Written after the owner's question about a dedicated OS; `llm.asm` and `zero`
  READMEs read on GitHub the same day. Design 0114, Q104 to Q107 and HOST-1 to HOST-5 claimed on the
  hub.
