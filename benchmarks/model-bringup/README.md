# Model bring-up study (spikes R14 to R18, and R22)

Scripts for spike R14 of [design 0056](../../docs/design/0056-frontier-program.md): where the time
goes between a machine with no model on it and the first result. Spikes R15 to R18 of
[design 0062](../../docs/design/0062-model-weights-into-remote-memory.md) continue the study:
a model whose file is on another machine. Spike R22 of
[design 0067](../../docs/design/0067-multi-node-llm-serving-and-admission.md) measures how a model
server on one GPU behaves as more clients use it at once. A spike is throwaway code. It builds nothing in the
product and ends with a dated record.

Spike R14 was numbered R11 on 2026-11-11. The record of that day carries the old number in its
`study` field.

| Session | Script | Status |
|---|---|---|
| 1. A small decision model on one machine | `laya_session1.py` | One record, 2026-11-11 |
| 2. A generative model on a second machine, called from the first | | Planned |
| 3. Fetch, load and first token with `llama-server`, the method of R14 | | Planned |
| 4. The push of weights through llama.cpp's RPC backend (R15) | | Planned |
| 5. The RPC server behind a relay (R16) | | Planned |
| 6. A model built from memory with `llama_model_init_from_user` (R17) | | Planned |
| 7. A split model with the head on the stronger machine (R18) | | Planned |
| 8. The concurrency knee of a model server on the GPU machine (R22) | None: commands only | One record, 2026-11-16 |

## Session 1: a small decision model on one machine

### Goal

One record that states, for one machine: the time to fetch and load a small decision model, the
time of one decision, and how many of 20 Vietnamese sentences it sorts correctly into "command"
and "question".

The model is Laya ([model card](https://huggingface.co/convaiinnovations/laya)), checkpoint
`multilingual`. It answers typed questions about a text in one forward pass and generates no text.
Session 1 uses it because it is small, so one run is short. It is not the generative model that
R14 names.

### Prerequisites

- Python 3.10 or newer.
- Network access to PyPI and to Hugging Face, for steps 2 and 4.
- Free disk space for PyTorch and for the weights. The model card gives about 647 MB for the
  weights of this checkpoint.
- A machine on AC power, with no other heavy job running.

### Steps

All steps run on the machine under test, from the root of the repository.

1. Create a virtual environment outside the repository:

   ```bash
   python3 -m venv ~/venvs/model-bringup
   ```

2. Install the model's package. This downloads it and its dependencies, PyTorch among them, from
   PyPI:

   ```bash
   ~/venvs/model-bringup/bin/pip install laya
   ```

3. Check the script without any model. This downloads nothing and writes no record:

   ```bash
   ~/venvs/model-bringup/bin/python benchmarks/model-bringup/laya_session1.py --backend stub
   ```

4. Run the session. The cold load downloads the weights from Hugging Face into a new, empty
   cache directory:

   ```bash
   ~/venvs/model-bringup/bin/python benchmarks/model-bringup/laya_session1.py
   ```

5. Delete the cache directory that the last lines of the output name, or run step 4 with
   `--delete-cache` instead.

### Verify

Step 3 prints `STUB RUN` and `correct 20 of 20`. Step 4 prints a summary without the `STUB RUN`
line and the path of the record, `results/<UTC date>-laya-<os>-<arch>-<cpu>.json`. An earlier
record is never overwritten: a second run on the same day gets `-2` in its name.

### What is measured

Each phase runs in a new Python process, so nothing stays loaded between phases.

| Number | What it is |
|---|---|
| Cold load | `laya.load()` with an empty cache: the download of the weights and the build of the model, in seconds. The record also holds the bytes that the cache held afterwards |
| Warm load | `laya.load()` again from the filled cache, 5 times, each in a new process, with access to the hub switched off. Median, minimum and maximum |
| Fetch, estimate | The cold load minus the median warm load. It is an estimate: the download is not timed alone |
| Whole process | Wall time of the process around a load, which adds the start of Python and the import of the package |
| First decision | The first `predict` call of a new process, in milliseconds |
| One decision | Every `predict` call of 5 more passes over the 20 sentences. Median, minimum and maximum |
| Correct | Sentences of the first pass whose label equals the expected one, in total and per label, with every wrong one listed |

The record also holds the date, the git commit, the processor, cores, memory, the operating
system, the power state on macOS, the package versions, the parameters, the question that was
asked, and every raw run. It holds no host name, no user name and no path.

### What the numbers do not show

- **Twenty sentences are few.** One sentence is five points of the score. The set shows whether
  Vietnamese works at all, not how well.
- **The warm load is not a first load after a restart.** The operating system still holds the
  files in memory. A load from a cold disk needs a restart before it.
- **The fetch depends on the network of that hour.** One run is one sample.
- **The question is asked in English** about a Vietnamese sentence, as the model card's examples
  do. Other wordings of the question are not tried.
- **Laya's model card does not name Vietnamese.** It also says that its probabilities are too
  confident and that its base checkpoints need fine-tuning for typed decisions. The record stores
  the model's whole answer for each sentence, so the probabilities can be read later.

### Records

One run so far:
[the record of 2026-11-11](results/2026-11-11-laya-darwin-arm64-apple-m5.json).

| | 2026-11-11, Apple M5, 10 cores, 32 GiB, macOS, AC power |
|---|---|
| Packages, device | `laya` 0.4.31, `torch` 2.15.0, `transformers` 5.22.2. The package chose `mps` |
| Cold load | 19.0 s. The cache held 678 MB afterwards. Whole process 20.6 s |
| First prediction after the cold load | 1.1 s |
| Warm load | median 2.09 s (2.07 to 2.22, n=5). Whole process: median 2.49 s |
| Fetch, estimate | 16.9 s |
| First decision of a new process | 97.9 ms |
| One decision | median 17.7 ms (15.3 to 24.1, n=100) |
| Correct | 18 of 20. Command: 10 of 10. Question: 8 of 10 |

- **The machine was not idle.** The load averages were 4.56, 7.74 and 8.3 before the run.
- **The fetch is one sample** on one network at one hour.
- **The two wrong answers** were sentences 13 and 19, both labelled `command`. The model gave
  the wrong label a probability of 0.51 and 0.57.
- **The probability does not separate right from wrong here.** Four sentences that were sorted
  correctly (2, 3, 5 and 20) also had a probability under 0.6 for their label.
- **The labels did not change** over the six passes.

### Options

| Option | Effect |
|---|---|
| `--device cpu`, `mps` or `cuda` | Passed to `laya.load(device=...)`. Without it the package chooses |
| `--subfolder <NAME>` | Another checkpoint of the repository. Default `multilingual` |
| `--sentences <FILE>` | Another file of JSON lines with `text` and `expected` (`command` or `question`) |
| `--warm-loads <N>`, `--repeats <N>` | Number of warm loads and of passes after the first. Default 5 each |
| `--hf-home <DIR>` | Use this cache directory. If it is not empty, the cold load measures no fetch, and the record says so |
| `--online-warm` | Leave hub access on in the warm phases |
| `--out <FILE>`, `--no-write` | Where the record goes, or no record |

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `The Python package laya is not installed for this interpreter` | The script was started with another Python than the one of step 2 | Start it with `~/venvs/model-bringup/bin/python` |
| A warm load fails with a message about offline mode | The package asks the hub for something on every load | Add `--online-warm`. The record then notes that the warm phases were online |
| The load never returns | The model card describes a deadlock when TensorFlow is installed. The script already sets `USE_TF=0` | Use a virtual environment without TensorFlow |
| A line `Redirects are currently not supported in MacOs` per process | A notice from PyTorch on macOS | None needed |
| `first load ... (the cache was not empty: no fetch)` | `--hf-home` named a directory that already held files | Leave `--hf-home` out to measure the fetch |

## Session 8: the concurrency knee of a model server on the GPU machine (R22)

Status: one record, 2026-11-16 (see "Records"). This section is the plan of the run. It adds no
script: every step is a command.

### Goal

One dated record that states, for one model server on the owner's GPU machine, how total and
per-client output speed change as the number of clients that ask at the same time grows: 1, 2, 4,
8, 16 and 32. The question is where the curve stops rising (the knee, `C_opt`) and whether it
falls after it, as it does in NVIDIA's two-station run in design 0067 (607.8 tok/s at 16 clients,
385.0 tok/s at 32).

The answer decides item `LLM-14` of design 0067:
- **The curve falls after a peak.** Admission at the knee is worth building, and `C_opt` is its
  first default.
- **The curve flattens and does not fall.** A fixed cap per service is enough. Take the smallest
  `C` that reaches 90 % of the peak.
- **The curve is flat from `C = 1`.** Batching gains nothing on this machine, and `LLM-14` waits.

### What is measured, and with what

| Part | Choice | Why |
|---|---|---|
| Machine | The Windows machine with the RTX 5060 Ti (16 GiB) that ran [the GPU run](../gpu-run/README.md) | It is the owner's strongest GPU |
| Server | `llama-server` from a llama.cpp release for Windows with CUDA | It runs natively on Windows. vLLM supports Linux, not Windows |
| Model | Qwen2.5-Coder-7B-Instruct, GGUF `Q4_K_M` (about 4.7 GB; confirm on the model page) | A coding model of the size a 16 GiB card serves with room for a large KV cache |
| Load generator | NVIDIA's [AIPerf](https://github.com/ai-dynamo/aiperf), the tool of the NVIDIA post | Its numbers (TTFT, inter-token latency, per-user and total throughput) compare directly with design 0067 |
| Engine counters | `llama-server --metrics`: `llamacpp:requests_processing`, `llamacpp:requests_deferred`, `llamacpp:n_busy_slots_per_decode`, `llamacpp:n_tokens_max`, `llamacpp:predicted_tokens_seconds` | They show from inside the server when requests start to wait |

Two workloads, each at `C = 1, 2, 4, 8, 16, 32`:

| Workload | Input tokens | Output tokens | Stands for |
|---|---|---|---|
| `chat` | 1000 | 256 | A person or LIVA asking a question |
| `coding` | 4000 | 512 | A coding agent sending a file and asking for a change |

Each level sends `3 * C` warm-up requests that are not counted and `10 * C` measured requests,
as the NVIDIA post did. `--random-seed 7` fixes the prompts. `ignore_eos` makes every answer the
full output length, so levels compare.

**KV-cache estimate.** Qwen2.5-7B has 28 layers, 4 key-value heads and a head size of 128, so one
token of context takes 2 x 28 x 4 x 128 x 2 bytes = 56 KiB in the default `f16` cache. A pool of
131072 tokens is then about 7 GiB, and with the model and buffers it should fit in 16 GiB. At
`C = 32` the `coding` workload needs about 32 x 4512 = 144384 tokens, more than the pool. So the
`coding` sweep should reach the point where requests wait for cache, as in the NVIDIA run. The
`chat` workload (32 x 1256 = 40192 tokens) should not. Step 4 checks the real memory use.

### Prerequisites

- On the GPU machine: the NVIDIA driver (621.24 was used for the GPU run), Python 3.11, about
  10 GB free on the disk used below, and AC power.
- No other GPU work during the run. Stop any `oxideswarm worker` on that machine first.
- Network access to GitHub and Hugging Face for steps 1 and 2.
- The paths below use `D:\r22`. Any folder outside the repository works.

### Steps

All steps run on the GPU machine, in PowerShell.

1. Download a llama.cpp release for Windows with CUDA from
   [the releases page](https://github.com/ggml-org/llama.cpp/releases), and unpack it into
   `D:\r22\llama`. The RTX 5060 Ti is a Blackwell card (compute capability 12.0), so take the
   build for the newest CUDA version offered, and its matching `cudart` archive if the page lists
   one. Check that the server sees the card:

   ```powershell
   D:\r22\llama\llama-server.exe --list-devices
   ```

2. Download the model and record its SHA-256:

   ```powershell
   py -3.11 -m venv D:\r22\venv
   D:\r22\venv\Scripts\pip install aiperf huggingface_hub
   D:\r22\venv\Scripts\hf download Qwen/Qwen2.5-Coder-7B-Instruct-GGUF qwen2.5-coder-7b-instruct-q4_k_m.gguf --local-dir D:\r22\models
   Get-FileHash -Algorithm SHA256 D:\r22\models\qwen2.5-coder-7b-instruct-q4_k_m.gguf
   ```

   If the file name differs on the model page, use the `Q4_K_M` file listed there.

3. Start the server in a window of its own. It listens on loopback only:

   ```powershell
   D:\r22\llama\llama-server.exe -m D:\r22\models\qwen2.5-coder-7b-instruct-q4_k_m.gguf --alias coder7b --host 127.0.0.1 --port 8080 -ngl all -fa on -c 131072 --parallel 32 --kv-unified --metrics
   ```

   `--parallel 32` gives one slot per client at the highest level. `--kv-unified` lets all slots
   share one cache pool of 131072 tokens, as vLLM's paged cache does.

4. In a second window, check that the server is ready, and record the memory it holds with no
   request running (the counterpart of Table 1 of the NVIDIA post):

   ```powershell
   Invoke-RestMethod http://127.0.0.1:8080/health
   nvidia-smi --query-gpu=name,driver_version,memory.used,memory.total --format=csv
   ```

   The first command prints `status: ok`. If the server failed to start for lack of memory, start
   it again with `-c 98304`, and write the change in the record.

5. Check that AIPerf runs on Windows, with two requests:

   ```powershell
   D:\r22\venv\Scripts\aiperf profile --model coder7b --url http://127.0.0.1:8080 --endpoint-type chat --streaming --tokenizer Qwen/Qwen2.5-Coder-7B-Instruct --isl 1000 --osl 256 --extra-inputs ignore_eos:true --random-seed 7 --concurrency 1 --request-count 2 --output-artifact-dir D:\r22\out\smoke
   ```

   It prints a table with Time to First Token and Output Token Throughput, and its output
   sequence length is close to 256. If AIPerf does not start on Windows, use the fallback
   described below.

6. Run the sweep of the `chat` workload. Before and after each level, save the server's counters:

   ```powershell
   foreach ($c in 1,2,4,8,16,32) { Invoke-WebRequest http://127.0.0.1:8080/metrics -OutFile "D:\r22\out\chat-c$c-before.prom"; D:\r22\venv\Scripts\aiperf profile --model coder7b --url http://127.0.0.1:8080 --endpoint-type chat --streaming --tokenizer Qwen/Qwen2.5-Coder-7B-Instruct --isl 1000 --osl 256 --extra-inputs ignore_eos:true --random-seed 7 --concurrency $c --warmup-request-count (3*$c) --request-count (10*$c) --output-artifact-dir "D:\r22\out\chat-c$c"; Invoke-WebRequest http://127.0.0.1:8080/metrics -OutFile "D:\r22\out\chat-c$c-after.prom" }
   ```

7. Run the same loop for the `coding` workload: replace `chat` with `coding` in the four paths,
   `--isl 1000` with `--isl 4000`, and `--osl 256` with `--osl 512`.

8. Run `nvidia-smi` once more while the `coding` level `C = 32` runs, to record the memory in use
   under load. Then stop the server with Ctrl+C.

**Fallback if AIPerf does not run on Windows.** Leave the server on loopback. On the Mac, install
AIPerf in a virtual environment and reach the server through an SSH tunnel to the GPU machine
(`ssh -L 8080:127.0.0.1:8080 <USER>@<GPU_HOST>`, which needs the Windows OpenSSH server). Run
steps 5 to 7 on the Mac against `http://127.0.0.1:8080`. The record then says that the client was
on the Mac. One LAN round trip (about 5 ms in the GPU run) adds to TTFT but not to the knee.

### What to record

For each workload and each `C`, from AIPerf's `profile_export_aiperf.json` and from the two
`.prom` files:

| Column | Source |
|---|---|
| Output throughput, tok/s | AIPerf "Output Token Throughput" |
| Per-user throughput, tok/s | AIPerf "Output Token Throughput Per User", average |
| TTFT, ms | AIPerf "Time to First Token", average and p90 |
| Inter-token latency, ms | AIPerf "Inter Token Latency", average and p90 |
| Request latency, s | AIPerf "Request Latency", average |
| Deferred requests | `llamacpp:requests_deferred`, highest value seen |
| Busy slots per decode | `llamacpp:n_busy_slots_per_decode`, after minus before |
| Highest context in use, tokens | `llamacpp:n_tokens_max` after the level |

Also record the date, the git commit, the llama.cpp build number and its CUDA version, the model
file and its SHA-256, the driver, the server command line, the memory of steps 4 and 8, and
whether the client ran on the GPU machine or on the Mac.

The record goes in this section under "Records", as a table per workload, and as
`results/<UTC date>-r22-windows-rtx5060ti.json`. It holds no host name, no user name and no path.
AIPerf's raw exports contain paths, so they stay outside the repository.

### What the numbers will not show

- **One machine, one model, one quantization.** Another model or a longer context moves the knee.
- **Synthetic prompts.** AIPerf's random prompts are not real code. They set the length of a
  request. For a dense model like this one, the cost of a token depends on lengths, not content.
- **`ignore_eos` forces full-length answers.** Real answers are often shorter, so real load is
  lighter than the `coding` sweep at the same `C`.
- **The knee of llama.cpp is not the knee of vLLM.** llama.cpp defers a request when no slot is
  free. What it does when the shared cache fills is part of what this run shows. vLLM preempts
  running requests (design 0067, Finding 4), and the NVIDIA figures came from vLLM.

### Safety

- The server listens on `127.0.0.1` only. The fallback reaches it through SSH, never through a
  port open on the LAN.
- The run needs no cluster secret and touches no `~/.oxideswarm` file.
- It downloads about 5 GB of model and about 1 GB of programs and Python packages from GitHub,
  Hugging Face and PyPI. Delete `D:\r22` afterwards to free the space.

### Records

One run so far:
[the record of 2026-11-16](results/2026-11-16-r22-windows-rtx5060ti.json), on `master` at
`b83a562`. The figures below were recomputed from AIPerf's `profile_export_aiperf.json` and the
server's `/metrics` files of each level.

| | 2026-11-16, GeForce RTX 5060 Ti (16 GiB), Windows, driver 621.24 |
|---|---|
| Server | llama.cpp build b12428 for Windows with CUDA 13.5. The command of step 3 with one change: `--port 18080`, because another program held port 8080 on that machine. Still on `127.0.0.1` |
| Model | `qwen2.5-coder-7b-instruct-q4_k_m.gguf`, 4,683,073,536 bytes, SHA-256 `509287f78cb4d4cf6b3843734733b914b2c158e43e22a7f4bf5e963800894d3c` |
| Client | AIPerf 0.13.0 on the GPU machine. The fallback through the Mac was not needed |
| GPU memory | 1,717 MiB before the server started, 13,416 MiB with the server idle, at most 13,442 MiB during `coding` `C = 32`, of 16,311 MiB. The cache pool is reserved when the server starts |
| Smoke check (step 5) | 2 of 2 requests, 256 output tokens each, 71.8 tok/s, TTFT 303 ms |

**`chat`** (1000 input tokens, 256 output tokens):

| C | Output, tok/s | Per user, tok/s | TTFT avg / p90, ms | ITL avg / p90, ms | Request latency, s | Busy slots per decode |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 73.7 | 78.3 | 215 / 298 | 12.8 / 12.9 | 3.47 | 1.00 |
| 2 | 132.9 | 72.5 | 327 / 582 | 13.9 / 13.7 | 3.80 | 2.00 |
| 4 | 229.9 | 64.3 | 473 / 904 | 15.6 / 17.0 | 4.45 | 3.99 |
| 8 | 363.5 | 52.1 | 642 / 1180 | 19.6 / 22.8 | 5.63 | 7.98 |
| 16 | **423.7** | 31.9 | 1198 / 1897 | 33.2 / 38.6 | 9.64 | 15.96 |
| 32 | 379.8 | 13.1 | 1610 / 2285 | 78.1 / 84.2 | 21.53 | 31.78 |

**`coding`** (4000 input tokens, 512 output tokens):

| C | Output, tok/s | Per user, tok/s | TTFT avg / p90, ms | ITL avg / p90, ms | Request latency, s | Busy slots per decode |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 68.7 | 75.5 | 676 / 926 | 13.3 / 13.4 | 7.45 | 1.00 |
| 2 | 117.9 | 65.6 | 865 / 1760 | 15.3 / 16.6 | 8.68 | 2.00 |
| 4 | 165.1 | 48.0 | 1549 / 1921 | 21.2 / 23.5 | 12.35 | 3.99 |
| 8 | **202.5** | 29.0 | 2153 / 2780 | 35.3 / 38.3 | 20.21 | 7.98 |
| 16 | 193.7 | 13.6 | 3607 / 3022 | 75.7 / 79.1 | 42.20 | 15.90 |
| 32 | 33.7 | 2.9 | 27642 / 54727 | 508.4 / 648.6 | 64.53 | 32.00 |

"Busy slots per decode" is the mean of the level itself: the change of
`n_busy_slots_per_decode x n_decode_total` over the change of `n_decode_total`. The server
publishes a mean since its start, so the plain difference of the counter before and after a level
is not that level's value. At every level of both workloads, `llamacpp:requests_deferred` was 0
each time it was read (once a second), and `llamacpp:n_tokens_max` was 1284 for `chat` and 4540
for `coding`.

- **`chat` peaks at `C = 16` and falls after it.** Output rises 5.7 times from `C = 1` to
  423.7 tok/s at `C = 16`, then falls 10.3 % to 379.8 tok/s at `C = 32`, while the inter-token
  latency more than doubles (33.2 to 78.1 ms). The cache pool was far from full: 32 x 1284 =
  41,088 of 131,072 tokens. The step from 16 to 32 cost NVIDIA's vLLM run 36.7 % (design 0067).
- **`coding` peaks at `C = 8`.** At `C = 16` output is 4.4 % lower at twice the inter-token
  latency, and a few requests wait long for their first token (average above p90, highest
  25.0 s).
- **At `coding` `C = 32` the shared cache ran out, and the server cut the answers short instead
  of making requests wait.** All 32 slots were busy in every decode and `requests_deferred` stayed
  0. AIPerf counted all 320 requests as completed and reported no error, yet the answers held 68
  tokens on average (37 to 99) of the 512 asked for, none ended with the usage block of a finished
  stream, and output fell to 33.7 tok/s. The sizes agree: 32 x (4,029 prompt tokens + 68) =
  131,104 tokens, the size of the pool. The report of the run quotes 442 lines "Context size has
  been exceeded" from the server log. That log is not among the files kept.
- **Inter-token latency stays under 25 ms**, the interactive target of design 0067 D1, up to
  `C = 8` for `chat` (22.8 ms at p90) and up to `C = 4` for `coding` (23.5 ms).
- **The server reused cached prompt tokens at every level**: 22 to 62 % of prompt tokens at
  `C <= 8`, 8 to 38 % at `C >= 16` (counter `llamacpp:prompt_tokens_cached_total`). AIPerf's
  synthetic prompts share their beginnings, and a slot keeps its last prompt. TTFT, above all at
  low `C`, is therefore lower than for prompts the server has never seen.
- **One run, one server process.** The levels ran in order without a restart, and none was
  repeated.

**What this answers.** By the rule of the goal, the curve falls after a peak on both workloads:
admission at the knee is worth building, with `C_opt` = 16 for `chat`-sized and 8 for
`coding`-sized requests as first defaults. The run adds one condition: on llama.cpp with a shared
pool, a count of requests is not enough. The requests admitted at once must also fit the pool
(prompt plus the longest answer allowed, summed), because when the pool runs out the server
neither defers nor refuses a request: it ends the answers early. `requests_deferred`, the
llama.cpp signal named in design 0067 D1, stays 0 in that case. Whether to build `LLM-14` is
decision `Q61`, which is the owner's.
