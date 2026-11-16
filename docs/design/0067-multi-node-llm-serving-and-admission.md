# 0067: Multi-node model serving evidence, concurrency knee-point admission and usable GPU memory

Status: Draft (research and plan; builds nothing; Q60 to Q63 are open)
Created: 2026-11-15    Updated: 2026-11-16

## Summary

On 2026-11-15 the owner supplied seven slides summarizing a two-station vLLM benchmark and asked
what research directions and evidence follow for OxideSwarm. This record checks the primary NVIDIA
tutorial, six distributed-serving papers, and upstream engine and OS documentation against the
current `master` tree (`cd0f2c1`). It rules out intra-layer tensor parallelism over commodity
networks, refines `FEAT-9b`, `LLM-2`, `LLM-5` and `Q47`, adds spike `R22` and item `LLM-14` (Design
only), and leaves `BACKLOG.md` unchanged until the owner decides `Q60` to `Q63`.

## Context

### What the owner supplied

Seven Vietnamese summary slides (`donniechublog`) covering NVIDIA's technical walkthrough of
2026-11-13, *When One GPU Isn't Enough: Serving GLM-5.3 Across Two NVIDIA DGX Stations with vLLM*
by Fatima Fall. The primary English tutorial was fetched and read in full on 2026-11-15 so every
figure and operational detail below rests on the primary page rather than on slide captions.

### Primary benchmark record (checked from the source page on 2026-11-15)

- **Source:**
  [When One GPU Isn't Enough: Serving GLM-5.3 Across Two NVIDIA DGX Stations with vLLM](https://developer.nvidia.com/blog/when-one-gpu-isnt-enough-serving-glm-5-3-across-two-nvidia-dgx-stations-with-vllm/)
  (published 2026-11-13, read 2026-11-15).
- **Model and memory constraint:** [`nvidia/GLM-5.3-NVFP4`](https://huggingface.co/nvidia/GLM-5.3-NVFP4)
  (Z.ai GLM-5.3 Mixture-of-Experts, 753 billion total parameters, 40 billion active per token,
  50 safetensor shards totaling about 464 GB on disk). One NVIDIA DGX Station carries a single
  GB300 GPU with 252 GB of HBM3e, so one station cannot hold all weights in GPU memory without CPU
  offload.
- **Interconnect and topology:** Two Linux GB300 DGX Stations linked point-to-point over one
  ConnectX-8 RoCEv2 rail (`400,000 Mb/s`, MTU `9000`, `GPUDirect RDMA` over NCCL). Station A runs
  `vllm/vllm-openai:v0.30.0-aarch64` as rank 0 (OpenAI-compatible HTTP API on port `8000`); Station
  B runs rank 1 with `--headless` (no HTTP server). Tensor parallelism (`TP=2`) places about
  `218 GiB` of weights on each GPU. With `--max-model-len 98304`, `--gpu-memory-utilization 0.93`,
  FP8 KV-cache storage, prefix caching, and 4-token multi-token prediction (MTP), idle GPU memory
  rises from `0.01 GiB` to `231.35 GiB` on Station A and `0.00 GiB` to `231.34 GiB` on Station B
  (Table 1 of the post).
- **Concurrency sweep and knee-point collapse:** Measured with
  [`AIPerf` 0.12.0](https://github.com/ai-dynamo/aiperf) replaying the 80 Coding conversations
  (89 turns) of [`SPEED-Bench`](https://github.com/NVIDIA-NeMo/Skills/tree/main/nemo_skills/dataset/speed-bench)
  at configured concurrencies `C = 1, 8, 16, 32` (`3 * C` warmup requests excluded, `10 * C`
  measured requests, `8,192`-token output ceiling):

| Configured concurrency (`C`) | Overall output throughput (`tok/s`) | Mean per-user throughput (`tok/s`, slide 4 & Fig. 2) | Mean request latency (`s`) | Mean TTFT (`ms`) | Mean inter-token latency (`ms/token`) |
|---|---|---|---|---|---|
| `1` | `107.4` | `115.3` | `49.84` | `479.75` | `8.78` |
| `8` | `386.0` | ~`53` | `114.17` | `433.79` | `19.01` |
| `16` | `607.8` | ~`42` | `136.46` | `482.90` | `23.77` |
| `32` | `385.0` | `13.0` | `453.54` | `899.36` | `77.90` |

Above `C = 16`, adding twice as many concurrent coding requests does not plateau: **aggregate
throughput drops by 36.7 % (`607.8` to `385.0 tok/s`), mean request latency triples (`136.46 s` to
`453.54 s`), and inter-token latency rises 3.28x (`23.77` to `77.90 ms/token`)** because all
in-flight sequences contend for a fixed KV-cache pool (`231.35 GiB - 218 GiB` minus runtime
workspace per rank).

### Basis of a finding

Following [design 0062](0062-model-weights-into-remote-memory.md) and
[design 0064](0064-research-of-2026-11-13-and-route.md):

- `source`: a file in this repository at `cd0f2c1` was read on 2026-11-15.
- `page`: the linked external documentation page or technical article was fetched and read on
  2026-11-15.
- `abstract`: the arXiv metadata, abstract, and published architectural result of the cited paper
  were checked on 2026-11-15.

### Findings from literature, upstream documentation, and this codebase

| # | Finding | Reference | Basis | Consequence for OxideSwarm |
|---|---|---|---|---|
| 1 | Step 4 of NVIDIA's two-station guide inspects `/tmp/nccl_*.log` for `NET/IB.*GDRDMA` and exits with failure (`exit 1`) if `NET/Socket` appears | [NVIDIA Technical Blog](https://developer.nvidia.com/blog/when-one-gpu-isnt-enough-serving-glm-5-3-across-two-nvidia-dgx-stations-with-vllm/); [DGX two-station playbook](https://build.nvidia.com/station/connect-two-stations) | `page` | Even on two identical workstations, tensor parallelism treats TCP socket fallback as a broken deployment, not a degraded mode |
| 2 | Intra-layer tensor parallelism inserts two `AllReduce` collectives per Transformer layer (self-attention and MLP) on every forward pass | Shoeybi et al., *Megatron-LM: Training Multi-Billion Parameter Language Models Using Model Parallelism*, [arXiv:1909.08053](https://arxiv.org/abs/1909.08053) | `abstract` | An `L`-layer decode step needs `2 * L` synchronous network collectives per token. On 1-2.5 Gb/s Ethernet or Wi-Fi (`0.3` to `2 ms` RTT), network latency alone dominates token time. Confirms [design 0038](0038-large-language-model-inference.md) Physical limits |
| 3 | Across consumer Ethernet, Wi-Fi, or WAN links, inter-layer pipeline partitioning and pipelined-ring execution avoid per-layer `AllReduce` collectives and outperform tensor parallelism when a model exceeds single-node memory | Borzunov et al., *Petals: Collaborative Inference and Fine-tuning of Large Models* (ACL 2023), [arXiv:2209.01188](https://arxiv.org/abs/2209.01188); Li et al., *Prima.cpp: Fast 30-70B LLM Inference on Heterogeneous and Low-Resource Home Clusters*, [arXiv:2504.08791](https://arxiv.org/abs/2504.08791) | `abstract` | Keeps tensor parallelism as a non-goal on home/office LANs. Where a model fits one machine, whole-request routing (`LLM-1`, `LLM-9`) wins; where it does not, pipeline/RPC splitting (`LLM-4`, `R18`) applies |
| 4 | PagedAttention cuts internal KV-cache fragmentation to ~4 % and raises batch size, but once active sequences exhaust physical KV blocks, the scheduler must preempt running sequences via PCIe swap or full prompt+output recomputation | Kwon et al., *Efficient Memory Management for Large Language Model Serving with PagedAttention* (SOSP 2023), [arXiv:2309.06180](https://arxiv.org/abs/2309.06180) | `abstract` | Explains the backward-bending curve from `C = 16` (`607.8 tok/s`) to `C = 32` (`385.0 tok/s`): admitting requests past KV block capacity destroys cluster goodput |
| 5 | Compute-heavy prefill iterations stall concurrent memory-bound decode iterations unless prefills are chunked and budgeted against decode slack; naive batching trades tail inter-token latency for throughput | Agrawal et al., *Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve* (OSDI 2024), [arXiv:2403.02310](https://arxiv.org/abs/2403.02310) | `abstract` | Interactive voice/chat clients such as LIVA ([design 0061](0061-first-application-model-tunnel.md)) suffer severe TBT/ITL stalls if long-prompt agent jobs saturate a shared model worker unchecked |
| 6 | Mixed interactive and batch LLM workloads suffer head-of-line queueing and KV contention under static connection-count routing; live KV/load-aware scheduling and priority separation lower tail latency without sacrificing batch throughput | Sun et al., *Llumnix: Dynamic Scheduling for Large Language Model Serving* (OSDI 2024), [arXiv:2406.03243](https://arxiv.org/abs/2406.03243) | `abstract` | Directly challenges "fewest open tunnels" in [design 0061](0061-first-application-model-tunnel.md) D4 (`LLM-9`): an idle open tunnel uses zero KV blocks, whereas one coding benchmark client can saturate an instance (`D1`) |
| 7 | vLLM's Prometheus endpoint (`GET /metrics`) exports `vllm:kv_cache_usage_perc`, `vllm:num_requests_running`, `vllm:request_queue_time_seconds`, `vllm:inter_token_latency_seconds`, and `vllm:admission_rejections_total`. The same page calls `vllm:num_requests_swapped` a legacy metric of a swap mode that is no longer relevant in v1, and marks `vllm:time_in_queue_requests` as a duplicate of `vllm:request_queue_time_seconds` (checked again on 2026-11-16) | [vLLM Metrics documentation](https://docs.vllm.ai/en/stable/design/metrics/); [vLLM Parallelism and Scaling](https://docs.vllm.ai/en/v0.30.0/serving/parallelism_scaling/) | `page` | A local worker sidecar can read KV saturation and admission rejections from loopback HTTP without inspecting prompt or completion bytes (`D1`). Swap counts are not a saturation signal in v1 |
| 8 | Upstream `llama-server` (`--metrics` or `LLAMA_ARG_ENDPOINT_METRICS=1`) exports `llamacpp:requests_processing`, `llamacpp:requests_deferred`, `llamacpp:n_busy_slots_per_decode`, and `llamacpp:predicted_tokens_seconds` on `GET /metrics` | [`ggml-org/llama.cpp` `tools/server/README.md`](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md) | `page` | `llamacpp:requests_deferred > 0` and slot occupancy give an engine-native saturation signal on loopback. Note: older third-party guides name `llamacpp:kv_cache_usage_ratio`, which is absent from current upstream `tools/server/README.md` |
| 9 | Usable GPU memory differs from physical VRAM on every desktop OS: Windows WDDM enforces a dynamic per-process `Budget` (`IDXGIAdapter3::QueryVideoMemoryInfo`); macOS unified memory exposes `MTLDevice.recommendedMaxWorkingSetSize`; Linux/Windows NVIDIA exposes `nvmlDeviceGetMemoryInfo` while vLLM caps allocation at `gpu_memory_utilization` (`0.93` in the GB300 run) | [Microsoft Learn: `IDXGIAdapter3::QueryVideoMemoryInfo`](https://learn.microsoft.com/en-us/windows/win32/api/dxgi1_4/nf-dxgi1_4-idxgiadapter3-queryvideomemoryinfo); [Apple Metal: `recommendedMaxWorkingSetSize`](https://developer.apple.com/documentation/metal/mtldevice/recommendedmaxworkingsetsize); [NVIDIA NVML API](https://docs.nvidia.com/deploy/nvml-api/index.html) | `page` | Refines Finding 7 of [design 0064](0064-research-of-2026-11-13-and-route.md) and `FEAT-9b`: `TaskRequirements::vram_mb` (`FEAT-9a`, [design 0068](0068-task-vram-accounting.md)) is checked against the VRAM a worker advertises, while placement needs usable free budget (`Budget - CurrentUsage` on WDDM, working-set cap on Metal), not static total VRAM (`D2`) |
| 10 | Step 1 to Step 3 of NVIDIA's walkthrough require a third management computer over SSH, hand-edited `~/glm53.env` files (`RANK`, `HEAD_IP`, `WORKER_IP`, `RAIL_IFACE`), passwordless `ssh-keygen` + `rsync` of 464 GB, `sha256sum` over SSH, `docker` group root-equivalent rights, and `--host 0.0.0.0 --network host` with an explicit warning that port `8000` is unauthenticated on all interfaces | [NVIDIA Technical Blog](https://developer.nvidia.com/blog/when-one-gpu-isnt-enough-serving-glm-5-3-across-two-nvidia-dgx-stations-with-vllm/) | `page` | Even this workstation setup has no authenticated service orchestration, no content-addressed artifact check, and no authenticated tunnel to a loopback port (`D3`) |
| 11 | OxideSwarm already ships per-machine Ed25519 identity and Noise `IK` (`ENROLL-1b`), SPAKE2 LAN pairing (`JOIN-2`), BLAKE3 `BlobStore`/`BlobCache`, speed-aware `SpeedBook` scheduling (`SCHED-4a`/`4b`), and Prometheus `GET /metrics` + OTLP spans (`ECO-4`) | `crates/core/src/noise_auth.rs`, `crates/cli/src/pair.rs`, `crates/core/src/blob.rs`, `crates/master/src/speed.rs`, `crates/master/src/metrics.rs` | `source` | The building blocks for authenticated model tunnels, content-addressed shard checks, online performance books, and Prometheus exposition already exist in tree |
| 12 | Every grid data path today traverses the master (`Client -> Master -> Worker` and `Worker -> Master -> Worker`), whereas multi-machine model serving separates the low-rate management link from a direct point-to-point data rail (`cx8r1` in the blog, or Thunderbolt bridge / direct 2.5G-10G Ethernet on personal workstations) | `crates/master/src/server.rs`, `crates/cli/src/connect.rs`; [design 0062](0062-model-weights-into-remote-memory.md) `Q47`; [design 0050](0050-scale-out-architecture.md) `SCALE-3` | `source`, `page` | Grounds `Q47`: master issues short-lived authenticated session tickets while bulk tensor/blob streams prefer a verified direct fast rail between peers (`D4`) |

## Goals

- **G1** Separate what transfers from data-center/workstation multi-GPU tutorials to personal and
  lab clusters from what physical network latency forbids.
- **G2** Prevent the concurrency throughput cliff (`C = 16 -> 32` in the benchmark) when multiple
  coding agents (`Direction 1` of [design 0063](0063-two-directions-in-parallel.md)), LIVA voice
  sessions ([design 0061](0061-first-application-model-tunnel.md)), or a 30-student classroom
  (`EDU-1` of [design 0063](0063-two-directions-in-parallel.md)) share one or two strong model
  workers.
- **G3** Feed concrete, source-checked inputs into existing planned items (`FEAT-9b`,
  `LLM-2`, `LLM-5`, `LLM-6`, `LLM-9`, `Q47`) instead of inventing a parallel inference stack.
- **G4** Measure the `Throughput vs. interactivity` Pareto curve on the owner's own machines (`R22`)
  before writing scheduler or gateway code for `LLM-14`.

## Non-goals

- Implementing an inference engine, custom CUDA/Metal attention kernels, or intra-layer tensor
  parallelism over consumer Ethernet, Wi-Fi, or WAN links (Findings 1 to 3).
- Inspecting prompt text or model outputs inside the master for routing or accounting (preserves
  `G3` of [design 0061](0061-first-application-model-tunnel.md)).
- Changing any existing CLI flag, wire message, or `BACKLOG.md` item in this record.

## Design

### D1 Direction 1: Knee-point admission control and interactive-vs-batch QoS (`LLM-14`, Design only)

[Design 0061](0061-first-application-model-tunnel.md) D4 (`LLM-9`) proposes choosing the worker
with the fewest open tunnels. Findings 4 to 8 show two flaws with raw tunnel count:

1. **Idle tunnels vs. active KV blocks.** A desktop assistant (`LIVA`) or coding IDE keeps an HTTP
   keep-alive connection open across minutes of human reading while consuming zero KV blocks.
   Conversely, a single benchmark or agent sweep (`SPEED-Bench` in the blog, or `APP-2` / `APP-4`
   in [design 0063](0063-two-directions-in-parallel.md)) can open 32 concurrent long-context
   generations and push the worker past its KV-cache knee point, cutting total cluster token output
   by 36.7 % (`607.8` to `385.0 tok/s`) and tripling inter-token latency (`23.77` to `77.90 ms`).
2. **Interactive vs. batch SLO conflict.** A spoken LIVA reply requires steady low inter-token
   latency (`C = 1..8` regime, `< 25 ms/token`), whereas background coding agents or batch sweeps
   seek peak aggregate tokens per second (`C = 16` knee point).

**Proposed mechanism (`LLM-14`, layered on `LLM-6` / `LLM-9`):**

- **Worker-local saturation probe (optional, loopback-only).** When a worker advertises
  `--serve <NAME>=127.0.0.1:<PORT>`, an optional `--serve-metrics <NAME>=http://127.0.0.1:<MPORT>/metrics`
  lets the worker poll its own loopback engine every few seconds and summarize three scalars in its
  heartbeat: `active_requests`, `deferred_or_rejected` (the increase of
  `vllm:admission_rejections_total` since the last poll, or `llamacpp:requests_deferred`), and `kv_usage_permille` (`vllm:kv_cache_usage_perc * 1000` when
  present). No prompt or completion payload is read.
- **Zero-scrape fallback and knee cap.** When engine metrics are absent, each named service carries
  a configured concurrency cap `max_inflight` (defaulting to a conservative per-service value
  measured by `R22`) and the gateway tracks active in-flight requests rather than idle TCP sockets.
- **Two-band admission queue on the master/gateway:**
  - `interactive` band (used by LIVA and human chat): admitted up to `max_inflight` while
    `deferred_or_rejected == 0` and `kv_usage_permille < 900`.
  - `batch` band (used by coding agents and classroom bursts): admitted only up to a lower
    watermark `batch_cap < max_inflight` (for example `0.75 * max_inflight`, leaving immediate
    headroom for an arriving `interactive` request without triggering KV preemption). Excess batch
    requests wait FIFO at the gateway or route to a second matching worker, keeping every worker on
    the rising side of its `Throughput vs. interactivity` curve (`C <= C_opt`) and exporting queue
    depth through `crates/master/src/metrics.rs` (`ECO-4`).

### D2 Direction 2: Usable GPU memory and KV-cache budget model (input to `FEAT-9b`, `LLM-3`)

Table 1 of the NVIDIA benchmark illustrates why `checkpoint_bytes <= total_vram_bytes` is an
unsafe placement rule: `218 GiB` of weights on a `252 GB` GPU immediately consumed `231.35 GiB`
at idle (`0` active requests) once the KV-cache reservation (`--gpu-memory-utilization 0.93`,
`--max-model-len 98304`, FP8 KV cache) and CUDA Graphs (`cudagraph_capture_sizes`) were allocated.

`Q56` is decided and built: since `FEAT-9a` ([design 0068](0068-task-vram-accounting.md), pull request
232, protocol 9) a task can ask for `vram_mb`, and the master does not place more than the VRAM a
worker advertises. `FEAT-9b` (live VRAM polling) and `LLM-3`
([design 0038](0038-large-language-model-inference.md)) are still open. When they add live GPU
memory reporting, the model should distinguish **total hardware VRAM** from **OS-usable free GPU
budget**:

1. **Worker probe (`usable_vram_mb`):**
   - **Windows (WDDM):** call `IDXGIAdapter3::QueryVideoMemoryInfo` for
     `DXGI_MEMORY_SEGMENT_GROUP_LOCAL` and report `saturating_sub(Budget, CurrentUsage)` (Finding
     9). Physical adapter size ignores WDDM desktop and driver reservations.
   - **macOS (Apple Silicon unified memory):** read `MTLDevice.recommendedMaxWorkingSetSize`
     bounded by host free/reclaimable memory pressure (`SCHED-5` in
     [design 0064](0064-research-of-2026-11-13-and-route.md)), never total system RAM.
   - **Linux / NVIDIA TCC:** read NVML `nvmlDeviceGetMemoryInfo` `free` (or `/sys/class/drm`
     `mem_info_vram_total - mem_info_vram_used` on AMD) multiplied by a configurable safety ceiling
     (default `0.90`, matching the `0.90-0.93` reserve needed for runtime workspaces and CUDA
     graphs).
2. **Task / service requirement:** a GPU task declares `vram_mb` (`FEAT-9a`). For a long-lived
   model service (`LLM-2` / `LLM-5`), the helper that sizes placement computes:
   `vram_mb = weights_mb + kv_cache_mb(max_model_len, kv_dtype_bytes, max_seqs) + workspace_margin_mb`.

### D3 Direction 3: Authenticated paired service orchestration (input to `LLM-2` and `LLM-5`)

Comparing NVIDIA's two-station launch script (Finding 10) against OxideSwarm's primitives shows a
clear control-plane role for `LLM-2` and `LLM-5` ([design 0038](0038-large-language-model-inference.md))
without touching inference kernels:

| Manual step in the two-station tutorial | Operational / security gap | OxideSwarm control-plane equivalent (Planned) |
|---|---|---|
| Laptop SSHes to both stations and hand-edits `~/glm53.env` (`RANK`, `HEAD_IP`, `WORKER_IP`, `GPU_UUID`) | Human copy-paste drift; requires inbound SSH on workers | Master schedules a **paired service group** (`rank 0` head + `rank 1..N` headless workers), injects `OXIDESWARM_RANK`, peer endpoints, and probed GPU selector automatically |
| Passwordless `ssh-keygen` + `rsync` of 464 GB from A to B, followed by ad-hoc `find \| sha256sum` over SSH | Permanent lateral SSH trust between machines; slow unverified copy | Content-addressed BLAKE3 verification via `BlobCache` (`crates/worker/src/blob_cache.rs`) or direct tensor streaming (`LLM-12` in [design 0062](0062-model-weights-into-remote-memory.md)) |
| Rank 1 and Rank 0 started manually in order (`B, then A`); manual `curl /health` check | Hanging startup if one rank dies (`llama.cpp` issue 31847 in design 0062 row 29) | Ordered readiness state machine in `LLM-2`: headless worker ranks report `Ready` first, then `rank 0` starts with bounded startup timeout and coordinated group teardown (`process_tree.rs`) if any rank exits |
| `--host 0.0.0.0 --network host` exposes unauthenticated port `8000` on every interface | Prompt/output eavesdropping and unauthenticated model use on LAN | `rank 0` binds strictly to `127.0.0.1:<PORT>`; clients reach it only through the Noise/AEAD authenticated tunnel of `LLM-9` ([design 0061](0061-first-application-model-tunnel.md)) with `ENROLL-1` per-machine identity |

### D4 Direction 4: Control-plane vs. direct fast-rail data-plane separation (input to `Q47` and `SCALE-3`)

In the NVIDIA topology, control commands travel over ordinary management networking while tensor
collectives use a dedicated point-to-point link (`cx8r1` at `400 Gb/s`, MTU `9000`, verified before
launch). On personal and small-lab hardware, the direct equivalents are:

- **macOS pairs:** Thunderbolt 4 / 5 IP bridge (`20` to `80 Gb/s`, sub-`100 us` latency). Only
  Thunderbolt 5 adds RDMA, from macOS 26.2 ([design 0062](0062-model-weights-into-remote-memory.md)
  row 15).
- **Linux / Windows workstations:** direct `2.5G` / `10G` / `25G` Ethernet DAC or CAT6A patch
  between two machines sitting under the same desk, while the master runs over Wi-Fi or a `1 Gb/s`
  switch.

If `LLM-4`, `LLM-9`, `LLM-12` or `SCALE-3` hair-pins every byte through the master (`Client ->
Master -> Worker`), a `40 Gb/s` Thunderbolt cable between two workers is throttled down to the
master's Wi-Fi link. As input to `Q47` ([design 0062](0062-model-weights-into-remote-memory.md)):

1. Each worker advertises candidate local interface addresses and link speeds in registration
   metadata (never trusted without authentication).
2. When two enrolled machines (`ENROLL-1`) in the same cluster need a high-volume stream (blob
   fetch `SCALE-3`, tensor load `LLM-12`, or RPC tunnel `LLM-4`), the master mints a short-lived,
   single-use **session ticket** bound to both machine public keys and the transfer/tunnel ID.
3. The two peers probe candidate direct links in descending speed order (direct cable / Thunderbolt
   subnet first, LAN second, `iroh` QUIC hole-punched path third, master relay fallback last),
   complete a mutual Noise/AEAD handshake under the ticket, and keep heartbeats and accounting
   records (`LLM-10`) on the master control stream.

### D5 Direction 5: Multi-concurrency Pareto benchmark spike (`R22`)

[Design 0061](0061-first-application-model-tunnel.md) (`APP-1`) currently specifies timing `liva_eval`
as a single sequential client (`C = 1`). The `SPEED-Bench` curve in Table 2 shows that `C = 1`
hides both the batching gain (`5.6x` from `C = 1` to `C = 16`) and the preemption cliff (`-36.7 %`
from `C = 16` to `C = 32`). Both numbers govern whether `EDU-1` (a class sharing one strong
machine) and multi-agent coding (`agent-mac`, `agent-win`, `agent-win-2` querying one local coder
model) work in practice.

| ID | Question | Method | Stop rule |
|---|---|---|---|
| `R22` | Where is the `Throughput vs. interactivity` knee point (`C_opt`) on the owner's actual GPU machine when reached from the Mac over loopback tunnel (`APP-1` setup)? | Run `llama-server --metrics` (or `vLLM` where supported) bound to `127.0.0.1` on the owner's discrete-GPU Windows machine (`DESKTOP` class), forwarded to the Mac as in `APP-1`. Replay a fixed coding/assistant prompt set at `C = 1, 2, 4, 8, 16, 32` (`3 * C` warmup, `10 * C` measured, fixed output cap and seed). Record aggregate `tok/s`, per-user `tok/s`, mean/p95 TTFT, mean/p95 ITL, and loopback `/metrics` counters (`requests_processing`, `requests_deferred` or `kv_cache_usage_perc`) | Ends with one dated record and script under `benchmarks/model-bringup/`. If aggregate `tok/s` stays flat (`C_opt == 1` on single-user GGUF settings) or never degrades up to `C = 32`, `LLM-14` simplifies to a static per-service slot cap |

## Compatibility

Nothing changes on the wire or CLI today. When later designed:

- `TaskRequirements::vram_mb` shipped with protocol 9 (`FEAT-9a`, design 0068). A worker report of
  `usable_vram_mb` (`FEAT-9b`) would be a further wire addition governed by
  [design 0040](0040-protocol-versioning.md) (`Human*` and `Binary*` mirrors in
  `crates/core/src/protocol.rs`, no `skip_serializing_if` on bincode types).
- `LLM-14` admission queuing lives in the master/gateway (`LLM-6` / `LLM-9`) and remains
  transparent to standard OpenAI-compatible HTTP clients (`LIVA`, `AIPerf`, coding agents).

## Security

- **No unauthenticated model or metrics ports.** In `D1` and `D3`, both the inference port and the
  engine's `/metrics` port bind strictly to `127.0.0.1` on the worker, avoiding the `0.0.0.0:8000`
  exposure warned about in NVIDIA's Step 3 security note and `llama.cpp` `CVE-2026-34159`
  ([design 0061](0061-first-application-model-tunnel.md) Context).
- **Content-blind admission.** `D1` reads only aggregate numeric gauges (`active_requests`,
  `deferred_or_rejected`, `kv_usage_permille`), never prompt text, token IDs, or completions.
- **No lateral SSH keys.** `D3` and `D4` replace passwordless `ssh-copy-id` between workers with
  master-issued, short-lived Noise/AEAD session tickets scoped to one service or blob transfer and
  revocable via `ENROLL-1` (`crates/core/src/noise_auth.rs`).

## Test plan

No product code is added by this record.

- Verify internal and external links and absence of carriage returns or typographic dashes with
  `python3 tests/test_m1_challenger_hygiene.py`.
- Spike `R22` produces a reproducible benchmark script and dated hardware record in
  [benchmarks/model-bringup/README.md](../../benchmarks/model-bringup/README.md), following the
  same evidence standard as [benchmarks/grid/README.md](../../benchmarks/grid/README.md).

## Work items

Sizes follow `BACKLOG.md` (`S`, `M`, `L`). Nothing enters [BACKLOG.md](../BACKLOG.md) until the
owner decides `Q60` to `Q63`.

| ID | Item | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|
| `R22` | Spike: measure the `C = 1..32` `Throughput vs. interactivity` curve and engine `/metrics` knee point across two machines (`D5`) | S | `APP-1` setup on the owner's Mac + GPU PC | `benchmarks/model-bringup/` | Dated record gives aggregate `tok/s`, per-user `tok/s`, TTFT, ITL, and saturation counters at `C = 1, 2, 4, 8, 16, 32` | Planned |
| `LLM-14` | Knee-point admission control and `interactive` vs. `batch` priority bands for model tunnels/gateway (`D1`) | M | `R22`, `LLM-9` or `LLM-6` | `crates/master/src/`, `crates/worker/src/` | Under a `C = 32` burst in an integration test, worker in-flight count stays at `<= max_inflight`, zero requests are dropped, and an `interactive` request bypasses queued `batch` requests | Design only |

## Decisions for the owner

The right column is a recommended default, not a decision. Once accepted, these join
[ROADMAP.md](../ROADMAP.md) as `Q60` to `Q63`.

| ID | Question | Recommended default |
|---|---|---|
| `Q60` | Does `APP-1` ([design 0061](0061-first-application-model-tunnel.md)) expand via spike `R22` to measure `C = 1, 2, 4, 8, 16, 32` (`Throughput vs. interactivity`) before `LLM-9` or `EDU-1` concurrency limits are fixed? | Yes |
| `Q61` | Should `LLM-6` / `LLM-9` replace raw "fewest open tunnels" placement with active in-flight / knee-point admission and `interactive` (LIVA) vs. `batch` (agents, class) priority bands (`LLM-14`), gated on `R22`? | Yes, after `R22` shows the knee on the owner's hardware |
| `Q62` | When `FEAT-9b` adds live GPU memory reporting, should workers report OS-usable GPU memory budget (`DXGI Budget - CurrentUsage` on Windows, `recommendedMaxWorkingSetSize` on Metal, safety-capped `NVML free` on Linux) rather than raw physical VRAM? | Yes |
| `Q63` | For `Q47` ([design 0062](0062-model-weights-into-remote-memory.md)) and `SCALE-3`, should direct worker-to-worker / client-to-worker fast rails (Thunderbolt bridge, direct 2.5G/10G Ethernet) use master-minted Noise/AEAD session tickets with automatic fallback to LAN/QUIC/master? | Yes, after `v0.2.0` and single-hop `LLM-9` work |

## Status log

- 2026-11-15: Draft written at the owner's request from seven slides summarizing NVIDIA's two-station
  GLM-5.3 vLLM benchmark. Primary NVIDIA blog post (published 2026-11-13), six arXiv papers
  (`1909.08053`, `2209.01188`, `2309.06180`, `2403.02310`, `2406.03243`, `2504.08791`), upstream
  `vLLM` and `llama-server` metrics docs, and Windows DXGI / Apple Metal / NVIDIA NVML API docs
  were checked on 2026-11-15. Nothing is built, and `BACKLOG.md` is unchanged.
- 2026-11-16: Brought up to date before its first commit. `Q56` is built as `FEAT-9a` (`vram_mb`,
  design 0068), so D2 and `Q62` now feed `FEAT-9b`. The vLLM metrics page was read again: D1 uses
  `vllm:admission_rejections_total` instead of the legacy `vllm:num_requests_swapped`, and
  `vllm:request_queue_time_seconds` instead of its duplicate. D4 limits RDMA to Thunderbolt 5. The
  figures of the NVIDIA post were checked again against the page and are unchanged.
