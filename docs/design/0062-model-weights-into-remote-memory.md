# 0062: Model weights into the memory of another machine, and the head of a split model

Status: Draft (plan; builds nothing; the owner decided Q44 on 2026-11-11; Q45 to Q47 are open)
Created: 2026-11-11    Updated: 2026-11-12

## Summary

A worker that holds no model file should be able to run a model whose file is on another
machine, without a copy of the file written to its disk first. The owner chose this path on
2026-11-11 (Q44) and asked for a plan to work from. This record holds:

1. what llama.cpp does on that path today, read from its source on 2026-11-11 and 2026-11-12;
2. four spikes that measure it on the owner's machines (R15 to R18);
3. for a model that one worker can hold: a model server that takes its tensors from a stream
   (LLM-12) and a client command that sends them (LLM-13);
4. for a model split over machines: the rule that the head runs on a worker.

It builds nothing, and no item is in the backlog.

## Context

### What the owner said

Four statements of 2026-11-11, paraphrased from Vietnamese:

1. Is there a way to bring a model from one machine quickly into the RAM or VRAM of a machine
   that does not have it?
2. Of the ways named in the answer, the second is the one to develop, to completion and with the
   best performance it can have: the weights go from the file on one machine straight into the
   memory of the other.
3. A model split over several machines sounds fairly good.
4. Write it down so that the work can be done.

### Words used here

| Word | Meaning |
|---|---|
| Source | The machine that holds the model file (GGUF) |
| Target | A worker that computes the model and does not hold the file |
| Head | The process that drives the computation of each token: `llama-server`, or the runner of D3 |
| Asking machine | The machine whose program wants the reply. It may be weak, and it may also be the source |

### What llama.cpp does today

Rows 1 to 15 were read on 2026-11-11 from the `master` branch of
[llama.cpp](https://github.com/ggml-org/llama.cpp), which was at `f7b886fe6fe5`. Rows 16 to 26
were read on 2026-11-12, when it was at `cb44fdb1892e`. Line numbers are those of that commit.
"Basis" says how:

- `source`: the file was read.
- `run`: a command was run on the owner's Mac.
- `issue`: the title, state and dates of the issue or pull request were read on GitHub.
- `discussion`: a maintainer's reply in a GitHub discussion was read.
- `advisory`: the entry was read through GitHub's API.
- `outside`: a measurement that someone else published. It was read from its page and was not
  repeated on the owner's machines.

| # | Finding | Where | Basis | Consequence |
|---|---|---|---|---|
| 1 | The main host holds the model file and runs `llama-server --rpc <HOST:PORT>,...`. `ggml-rpc-server` on each remote host exposes its devices. Weights and the KV cache are spread over local and remote devices in proportion to their memory, and `--tensor-split` overrides that | `tools/rpc/README.md` | source | The push of weights into another machine's memory exists upstream. On that path the source is also the head |
| 2 | Without its cache, the RPC server reads a tensor's bytes from the socket in parts of at most 16 MiB and sets each part in the device buffer. It writes no file | `rpc_server::set_tensor`, `ggml/src/ggml-rpc/ggml-rpc.cpp` | source | "No file written first" already holds on this path |
| 3 | With the cache (`ggml-rpc-server -c`), the sender hashes each weight tensor over 10 MiB (`HASH_THRESHOLD`) with a 64-bit FNV hash, and a comment in the source calls that hash slow. On a miss the server receives the whole tensor, hashes it again, writes the cache file and then sets the tensor. On a hit it reads the cached file | same file | source | The cache makes the first load slower and saves the transfer on the next. A 64-bit hash that is not cryptographic is its only check |
| 4 | A failed RPC call aborts the process of the head: `RPC_STATUS_ASSERT` calls `GGML_ABORT`. [Issue 25938](https://github.com/ggml-org/llama.cpp/issues/25938) reports it and was closed as not planned on 2026-10-08 | same file | source, issue | A head needs a supervisor that starts the whole pipeline again (D2 of [design 0038](0038-large-language-model-inference.md)) |
| 5 | [Issue 26680](https://github.com/ggml-org/llama.cpp/issues/26680): `rpc-server` ends without a log line when a connection reaches it through a relay such as `ssh -L` or a port proxy. The report says that a connection which sends no bytes is enough, and that a real client fails too. It names RPC server v4.0.0 on Windows with WSL2. Closed as not planned on 2026-09-20, cause not confirmed | GitHub | issue, and the start of its text | The tunnel of LLM-4 is such a relay. Spike R16 tests it before LLM-4 is sized |
| 6 | [Issue 30633](https://github.com/ggml-org/llama.cpp/issues/30633), open: a cache file that a full disk cut short is later served as a hit | GitHub | issue | A cache here verifies an entry when it reads it, as `BlobCache::read_verified` does (`crates/worker/src/blob_cache.rs`) |
| 7 | Reports of memory errors in the RPC server: issues 25289, 25299, 25703 and 26825 were closed as not planned, and 29914 is open | GitHub | issue | As design 0038 says: the RPC port never leaves loopback, and every machine of a split belongs to people who trust each other |
| 8 | The loader moves to an offset in the model file for each tensor (`file->seek(..., SEEK_SET)`). `llama_model_load_from_file_ptr` reads a GGUF that is "embedded in a larger file" | `src/llama-model-loader.cpp`, `include/llama.h` | source | A one-way stream cannot stand in for the file |
| 9 | `llama_model_init_from_user(metadata, set_tensor_data, userdata, params)` builds a model from GGUF metadata and a callback. With no file open, `load_all_data` calls the callback once for each tensor, in the order of its context, and returns. The header says that tensors are created as F32 unless the metadata names another type | `include/llama.h`, `src/llama-model-loader.cpp` | source | This call is the entry for a model built from a stream. Rows 16 to 19 say what the source shows about it, and spike R17 runs it |
| 10 | The loading code that `llama-server` shares with the other tools calls `llama_model_load_from_file` or `llama_model_load_from_splits`. A search of that file and of three files of the server found no call of `llama_model_init_from_user` | `common/common.cpp`, `tools/server/` | source | A server around a model built from a stream does not exist upstream. D3 lists three ways to get one |
| 11 | RPC devices are placed first in the device list, "to minimize network transfers" | `src/llama.cpp` | source | In a split whose head has a GPU of its own, the first layers go to the remote machines. What crosses per token is measured in R18 |
| 12 | The public header has no function that runs a range of layers or returns the state between two layers | `include/llama.h`, searched for "layer" | source | A relay of hidden states between machines, outside the RPC backend, needs changes inside llama.cpp |
| 13 | `llama-server --help` lists `--backend-sampling`, marked experimental | build 9430 of Homebrew | run | It may spare the head the logits of each token. R15 runs with and without it |
| 14 | The `llama-server` of Homebrew has no `--rpc` option, and the package has no `ggml-rpc-server` | build 9430 of Homebrew | run | Every spike builds llama.cpp from source with `-DGGML_RPC=ON` |
| 15 | The RPC backend can use RDMA on Linux (RoCEv2) and on Macs with Thunderbolt 5 (macOS 26.2 or later). The README names no provider for Windows | `tools/rpc/README.md` | source | Between a Mac and a Windows PC the transport is TCP |
| 16 | With no file open, `create_tensor` takes the type of each tensor from the metadata (`gguf_get_tensor_type`). It falls back to F32 only for a name that the metadata lacks. The shape comes from the model's own code, and this branch does not compare it with the metadata | `src/llama-model-loader.cpp`, lines 1387 to 1420 | source | By the source, a quantised model keeps its types on this path. A file whose shapes differ from what the architecture expects is not caught here, so the runner compares sizes itself (D4) |
| 17 | The device buffers are allocated (`ggml_backend_alloc_ctx_tensors_from_buft`) before `load_all_data` runs the callbacks. `ggml_backend_tensor_set` takes an offset and a size. `llama_model_init_from_user` sets the load mode to `LLAMA_LOAD_MODE_NONE` | `src/llama-model.cpp`, lines 2069 and 2146; `ggml/include/ggml-backend.h`; `src/llama.cpp`, lines 446 to 457 | source | When the callback runs, the tensor has its buffer on its device, and the callback can fill it part by part, as the RPC server does (row 2). A load then needs no second copy of the model in RAM. R17 confirms it in a run |
| 18 | In that branch `load_all_data` returns after the callbacks. It calls no progress callback and skips the check of tensor data that a load from a file can run. The callback returns nothing. There is one context per buffer type, and the callbacks follow the order in which the model's code created the tensors | `src/llama-model-loader.cpp`, lines 1220 to 1232 and 1647 to 1658; `include/llama.h` | source | The order is not the order of the file, so the target asks for each tensor by name (D4). Progress and the check of the data are the runner's work. A callback cannot report a failed fetch, so the runner ends its process on one (D3) |
| 19 | GGUF metadata can be read from memory, with `gguf_init_from_buffer` or `gguf_init_from_callback`. [PR 22341](https://github.com/ggml-org/llama.cpp/pull/22341) added both and was merged on 2026-05-25 | `ggml/include/gguf.h`, lines 85 to 88; GitHub | source, issue | The target builds the metadata from the first bytes of the file, which the source sends. No file is needed for it |
| 20 | `llama-server` fits its parameters to the free memory by default (`fit_params = true`). That step takes the paths of the model files, and the model is then loaded by path (row 10) | `common/common.h`, line 497; `common/common.cpp`, lines 1239 to 1271 and 1727 to 1735 | source | A patch for way 1 of D3 touches at least the options, the fit step and the load. With a model from a stream the fit step is off, and the number of layers on the GPU is given by hand |
| 21 | The reason for row 11, in a maintainer's reply: RPC devices go first so that logits are not copied over the network. The output layer is placed as the layer after the last one (`dev_output`), which is the last device when every layer is offloaded. The input embedding stays on the CPU of the head (`dev_input`) | [Discussion 16625](https://github.com/ggml-org/llama.cpp/discussions/16625); `src/llama-model.cpp`, lines 1766 to 1791 | discussion, source | A head with a GPU of its own holds the last layers and the output, and the state at the boundary crosses the network. A head with no device of its own gets the logits of every token from the remote machine, unless the token is sampled there (row 13). The head always holds the embedding table in RAM |
| 22 | The RPC client reports every operation as supported by the remote device: `ggml_backend_rpc_device_supports_op` returns true, with a TODO. Issues [24177](https://github.com/ggml-org/llama.cpp/issues/24177) and [27865](https://github.com/ggml-org/llama.cpp/issues/27865) report an RPC server on an AMD GPU that ends on a top-k operation. The first names sampling on the backend. Both were closed as not planned | `ggml/src/ggml-rpc/ggml-rpc.cpp`; GitHub | source, issue | Step 6 of R15 may end the RPC server. The record of R15 names the GPU of the target |
| 23 | The major version of the RPC protocol is 9. Issue 26680 named version 4.0.0 on 2026-08-06. A client refuses a server whose major version differs | `ggml/include/ggml-rpc.h`, `ggml/src/ggml-rpc/ggml-rpc.cpp` | source | The head and every RPC server come from one upstream commit (Compatibility). Nothing in the grid speaks this protocol: a tunnel carries its bytes unchanged |
| 24 | The RPC server listens with a backlog of one connection (`listen(sockfd, 1)`). The report of row 5 names this as its most specific suspect and says that it is not proven. The reporter's build had no RDMA. A search of `transport.cpp`, `ggml-rpc.cpp` and `tools/rpc/rpc-server.cpp` for `SIGPIPE`, `MSG_NOSIGNAL` and `SO_NOSIGPIPE` found nothing | `ggml/src/ggml-rpc/transport.cpp`, line 760; GitHub | source, issue | The cause is still not known. R16 gains two variants. `GGML_RPC_NO_RDMA` is no remedy for that report |
| 25 | The repository's own list of published advisories has 13 entries. The newest, CVE-2026-34159 of 2026-03-26, is a remote code execution in the RPC backend. GitHub's advisory database also holds six entries, marked unreviewed, that name the RPC server: CVE-2026-39909, CVE-2026-78147, CVE-2026-78148, CVE-2026-86317, CVE-2026-118375 and CVE-2026-119052, published from 2026-08-21 to 2026-10-29. [PR 30399](https://github.com/ggml-org/llama.cpp/pull/30399), a bounds check in the RDMA receive path, was merged on 2026-10-24 | GitHub | advisory, issue | Row 7 stands. The spikes build a commit of `master` from November 2026 or later |
| 26 | A split against one machine, in [an outside repository](https://github.com/kjaiswal/llama-cpp-distributed-benchmarks) of 2026-03-30. The head is a Mac on Metal, the one remote device is on CUDA, and the direct cable was measured at 9.41 Gbit/s. Both models fit the head alone. For a 7B model its README gives 91.8 reply tokens per second on the head alone and 52.7 through RPC, and the prompt was processed 4.2 times faster through RPC. For a 72B model it gives 11.1 and 5.9. The commands are in the README | its README | outside | One source, from before the asynchronous commands of the RPC backend ([PR 18626](https://github.com/ggml-org/llama.cpp/pull/18626), merged 2026-08-26). It agrees with D1: a model that fits one machine is not split. G2 is a target for case 1 only. R15 and R18 measure on the owner's machines |

### What OxideSwarm has

1. **Every path passes the master.** A client and a worker both connect to it, and no two
   workers connect to each other ([design 0061](0061-first-application-model-tunnel.md),
   Context).
2. **A file sent with `submit --input` is written three times before a task reads it.** The
   client uploads it to the master's `BlobStore`, the worker fetches it into its `BlobCache`,
   and the worker copies it into the task's sandbox (design 0006). The defaults of
   `--blob-store-mb` and `--blob-cache-mb` (4096 and 2048) are at or below the size of a 4 GiB
   model file.
3. **The pieces of a content-addressed cache exist.** `BlobId` is a BLAKE3 digest
   (`crates/core/src/blob.rs`), transfers run in parts of 4 MiB (`BLOB_CHUNK_BYTES`,
   `crates/core/src/blob_transfer.rs`), and the worker's cache is bounded, evicts the least
   recently used entry and verifies an entry when it reads it.
4. **On TCP, records between machines are sealed with ChaCha20-Poly1305** after the
   cluster-auth handshake (`crates/core/src/protocol.rs`).
5. **The plan.** Design 0038 plans a split model through llama.cpp's RPC backend: service tasks
   (LLM-2), placement by memory (LLM-3), an authenticated tunnel (LLM-4) and `llm up` (LLM-5).
   Design 0061 plans a client tunnel to a model server on loopback (LLM-9). Spike R14 of
   [design 0056](0056-frontier-program.md) times the path that copies the file first. None is
   built.

### What an outside document adds

On 2026-11-11 the owner supplied a research document that reviews the first answer to statement
1. Its author could not read this repository, and the document says so.

- **Checked and used above:** rows 5, 8, 9 and 13, and the titles of the issues in rows 4 to 7.
- **Found wrong:** issue 29914 was opened on 2026-10-03 and is open, not from August. Issue
  30633 is about the cache (row 6), not about a crash. Issue 26825 was closed as not planned,
  and no fix was found.
- **Not checked, and not repeated here:** its throughput figures for ciphers and hashes, its CVE
  and advisory numbers, its figures for other projects, and the build number it gives for
  `llama_model_init_from_user`.
- **Not a result:** its "1.09 times the floor" restates an assumed link efficiency of 92 %.

| Its proposal | Where it stands here |
|---|---|
| Build the model with `llama_model_init_from_user`, not from a file pointer | Taken: D3. R17 tests it first |
| A store per tensor, addressed by BLAKE3, with a list of what the target lacks and a write to disk after the load | Taken, with the blob code that exists: D4 |
| In a split, the head runs on a strong machine | Taken: D5 |
| Send whole requests to machines that each hold the model | Planned already: D1 of design 0038, LLM-6, LLM-9 |
| A direct data channel between two workers, with session tickets from the master | Q47. First the rule that the master runs on the head's machine |
| AES-256-GCM for model traffic | Not proposed. A second cipher changes the handshake. R15 measures the cost of the present one first, which is the open question of design 0038 D4 |
| A relay of hidden states of our own in place of the RPC backend | Non-goal (row 12) |
| Speculative decoding across machines | Not examined |
| A schedule of twelve weeks | Not taken. It rests on no reading of this code base |

### Three more outside documents

On 2026-11-12 the owner supplied three more research documents. Two answer questions that were
put to a research tool after this record was written: how a model is loaded from memory, and
what the RPC backend sends. The third is a second answer to statement 1. Their authors could
not read this repository.

- **Checked against upstream and used:** rows 16 to 26. Of 15 issues whose state and dates
  were checked, 14 match. So do the titles and states of 13 pull requests, and 10 of 11 CVE
  numbers.
- **Found wrong:** CVE-2026-76905 belongs to another project. Issue 27964 was closed as not
  planned on 2026-11-11 and is not open. PR 22341 has another author than the one named.
  `gguf_init_from_callback` does take an expected size. The report of row 5 does not name
  `SIGPIPE`, and its build had no RDMA.
- **Not checked, and not repeated here:** the byte counts of each RPC command, the number of
  tensors that the head sends for each token, every measurement but row 26, the statements
  about four other programs, the file systems in memory of the three desktop systems, the
  throughput of ciphers and hashes, and the statement that a process in a Windows AppContainer
  cannot use CUDA. The page given for the last one is about Windows containers.

| Its proposal | Where it stands here |
|---|---|
| The target asks the source for each tensor by its range of bytes, in the order of the loader's callbacks | Taken: D4, step 4 (row 18) |
| The first bytes of the file, up to the tensor data, go first, and the target builds the metadata from them in memory | Taken: D4, step 2 (row 19) |
| The callback fills a tensor part by part, so that the target holds no tensor whole in RAM | Taken as the second way of D4, step 5 (row 17). R17 tries it |
| A runner of our own on the library, in Rust, and no patch to `llama-server` | It is way 2 of D3. Q46 stays open, and the recommendation is unchanged |
| The head's own GPU is last in the device order | It is upstream's default (rows 11 and 21). R18 also runs the other order |
| Against issue 26680: a relay that connects only when bytes arrive, sends no probe and holds one connection | Not taken as a remedy. R16 tries it as a variant |
| One commit of llama.cpp on both ends, sent to the machines through the blob store | The first half is in Compatibility. The second is against a non-goal of design 0038: no llama.cpp binaries are shipped |
| The model is written to a file of full size in the target's page cache and loaded with `mmap` while the disk catches up | Not taken. It writes the file on the target first, which Q44 decided against. It is a variant of the path that R14 times |
| An engine of our own that runs a range of layers on each machine | Non-goal (row 12) |
| A lower precision of the KV cache before a model is split | Not examined. It belongs to LLM-3 of design 0038 |
| A signature on the list of tensors, by the owner's key | Not examined. It needs the keys of ENROLL-1 |
| The link is measured with one stream and with several, and the baseline with the file in the page cache and without | Taken: R15, steps 1 and 2 |

## Goals

The numbers are targets. None is a result.

- **G1** A target that holds no model file is ready after a time close to what the link needs
  for the bytes: at most 1.1 times the bytes sent divided by the measured throughput of the
  link.
- **G2** After the load the target computes every token itself. Tokens per second through the
  tunnel are at least 95 % of `llama-server` on the target with a local file.
- **G3** A second load of the same model sends no tensor bytes.
- **G4** No tensor is used before its bytes are checked against a BLAKE3 digest.
- **G5** The loss of the link, of the source or of the model process leaves the worker running
  and its GPU memory free.
- **G6** For a model that no single worker can hold, the asking machine never drives the
  computation.

## Non-goals

- An inference engine or kernels of our own (design 0038).
- A relay of hidden states of our own, tensor parallelism, or speculative decoding across
  machines.
- One request that is faster on several machines than on one (design 0038, Physical limits).
- RDMA, and a second cipher.
- Weights or prompts on the machines of strangers. A target reads both. This record stays
  inside clusters of people who trust each other (Q13, model A).
- A comparison with the path that copies the file first. The owner decided Q44. Spike R14 still
  times that path, and its record stands beside the records of this one.
- A change to the text of LLM-4, LLM-5 or LLM-9. This record proposes, and Q45 decides.

## Design

### D1 Three cases

| Case | Condition | Who computes | What this record adds |
|---|---|---|---|
| 1 | One worker can hold the whole model and lacks the file | That worker alone | D3 and D4 |
| 2 | No worker can hold the model, and several together can | A head on one worker, RPC servers on the others | D5. The rest is D2 to D5 of design 0038 |
| 3 | Several workers each hold the model, and there are many requests | Each worker alone | Nothing. It is D1 of design 0038, LLM-6 and LLM-9 |

A model is never split when one worker can hold it, because a split adds capacity and no speed
(design 0038, Physical limits). Until LLM-3 reports free memory, a person chooses the case.

### D2 Measure first (R15 to R18)

A spike is throwaway code on a branch, or a script under `benchmarks/model-bringup/`. It builds
nothing in the product and ends with a dated record. Each is sized S. The numbers continue those
of design 0056 (R1 to R10, R14) and of [design 0058](0058-lasting-value.md) (R11 to R13).

Conditions for all four:

- llama.cpp is built from source at one upstream commit on every machine, with `-DGGML_RPC=ON`
  and the GPU backend of that machine. The commit is from November 2026 or later (row 25), and
  the record names it.
- The RPC port has no authentication. The machines share a network that nothing else is on (a
  direct cable, or a switch with no other device), and a firewall rule limits the port to the
  peer. `GGML_RPC_NO_RDMA=1` keeps the transport on TCP.
- AC power, and no other heavy job on either machine.
- Each number comes from a script that is committed with the record. A record holds the date,
  the commit, both machines (processor, memory, GPU), the link, the model file with its size
  and quantisation, and every command. It holds no host name, user name or address.

| ID | Question | Method | Stop rule |
|---|---|---|---|
| R15 | What does the push of weights cost as upstream does it, against the link and against a local file, and what crosses the network for each token? | The source is the head, and the target runs `ggml-rpc-server`. (1) The throughput of the link in both directions, with one stream and with four. (2) Baseline on the target: `llama-server` from a local file, from its start to an answer on `/health`, once after a restart of the machine and once with the file already in the cache of the operating system, and tokens per second for one fixed prompt and a fixed number of reply tokens. (3) The head on the source, limited to the remote device with `--device` (names from `--list-devices`), all layers offloaded, the RPC server without `-c`: the same two numbers. (4) The same with `-c`, on a first and on a second start. (5) One short run with `GGML_RPC_DEBUG=3` on both sides and `-lv 5` on the head: the bytes of each command for one reply token. (6) Step 3 with `--backend-sampling`, which may end the RPC server (row 22). The record names the GPU of the target | If step 3 or step 6 reaches 95 % of the baseline's tokens per second, LLM-12 is not needed for speed, and LLM-4 serves case 1 |
| R16 | Does `ggml-rpc-server` survive a connection that arrives through a relay (row 5)? | The RPC server listens on loopback on the target. Through an `ssh -L` forward from the source: a connection that sends nothing and closes, then a real load with `llama-server --rpc`. The same through a second relay that is not SSH. Each system that will be a target takes the server's role once. The record says, after each step, whether the server process is alive, and holds its last log lines. If the server ends, two variants follow (row 24): a relay that connects to the server only when the first bytes arrive and holds one connection, and, on a Unix target, a server that is started with `SIGPIPE` ignored | If the server ends in any step, LLM-4 of design 0038 is not sized until the cause is known |
| R17 | Does `llama_model_init_from_user` build a usable quantised model from memory? | By the source, the types are kept and each tensor has its device buffer when the callback runs (rows 16 and 17). The run confirms it. A throwaway program on one machine builds the metadata of a quantised GGUF with `gguf_init_from_buffer` from the first bytes of the file (row 19), and builds the model with a callback that fills each tensor, part by part, from bytes held in memory. The record holds: the order in which the callback asks for tensors, against the order in the file; the type and the byte size of each tensor, against the file; whether the tensor has a device buffer when the callback runs; the peak memory of the process, against a load from the file; and the first 64 tokens of a greedy reply, against the same model loaded from the file. It runs on each system that will be a target. The record also lists the places in `llama-server` that a patch for D3 would touch (row 20) | If a quantised model does not load this way, or the load needs a second full copy of the weights in RAM, LLM-12 closes, and case 1 uses a file: the path that R14 times |
| R18 | What does a boundary cost in a split whose head is on a worker with a GPU? | Two machines. The head is on the one with more memory, and the other runs `ggml-rpc-server`. With a model that fits the head alone: the whole model on the head as the baseline, then split in halves with `--tensor-split`. With a model that fits only when split, if the two machines allow one. The record holds the time to the first token for a prompt of about 4096 tokens, tokens per second, and the bytes for one reply token as in R15 step 5. The same split then runs with the head on the other machine, and once with the head's own GPU first in the device order (`--device`), which upstream's default avoids (row 21) | If the head on the stronger machine is not faster than the head on the other, the rule of D5 is dropped |

### D3 Case 1: a model server that takes its model from a stream (LLM-12)

This record calls it the runner.

- **What it is.** A process on the target that holds llama.cpp. It listens on loopback on two
  ports: the OpenAI-compatible interface of `llama-server`, and a load port. It builds its model
  through `llama_model_init_from_user` (row 9). Each tensor comes from the stream of D4 or from
  the cache.
- **What the source shows.** A quantised model keeps its types on this path, and each tensor
  has its device buffer when the callback runs (rows 16 and 17). The path reports no progress
  and does not check the data, and the callback cannot report a failure (row 18). The runner
  counts the bytes for a progress line and checks them (D4). It ends its own process when a
  tensor cannot be fetched or a digest differs, and it is started again as after any other
  end: by hand at first, later by the supervisor of LLM-2.
- **Its own process.** The worker never links llama.cpp. An abort inside llama.cpp (row 4), or a
  fault in its GGUF parser, ends the runner and not the worker. When the runner ends, the
  operating system returns its GPU memory, which is G5.
- **Who starts it.** At first the worker's owner, by hand, as with the model servers of LLM-1
  and LLM-9. Later a service task (LLM-2).
- **How it is reached.** `worker --serve` names both ports (LLM-9). After the load, a reply does
  not involve the source, which is G2.
- **Isolation.** Whether a process under `--isolation fs` or `fs+net` can use the GPU is not
  known on any of the three desktop systems. The guide for LLM-12 states what was tried.

Row 10 means that such a server has to be made. Three ways, for Q46:

| Way | What it is | Cost |
|---|---|---|
| 1 | A patch to `llama-server` that adds a load from a stream, offered upstream and kept here as a patch file for one named upstream commit until it is merged | The smallest change. It touches at least the options, the fit step and the load (row 20), and it has to follow upstream. Whether upstream takes it is not ours to decide |
| 2 | A server of our own on the library, through a foreign function interface | Chat templates, sampling, slots and the HTTP interface are written a second time. It moves toward the engine that design 0038 rules out |
| 3 | No runner. The file is written to a file system held in memory, and a stock `llama-server` loads it | No change to llama.cpp. The file stays in RAM beside the copy in the device until the load ends, which is twice the model on a machine with unified memory. Linux has such a file system (tmpfs). This record knows of none built into Windows |

Recommended: way 1, after R17. Way 3 is a trial step for a Linux target.

What upstream has said about a load from memory, read on 2026-11-12. In discussions
[7560](https://github.com/ggml-org/llama.cpp/discussions/7560) and
[10990](https://github.com/ggml-org/llama.cpp/discussions/10990), both of 2024, the project's
author preferred a buffer that appears as a file, and wrote that a load from a buffer would be
good to have. A draft pull request for it,
[9125](https://github.com/ggml-org/llama.cpp/pull/9125), was closed unmerged.
`llama_model_init_from_user` came in March 2026 with the end-to-end tests
([PR 19802](https://github.com/ggml-org/llama.cpp/pull/19802)), and the metadata calls of row
19 in May. An outside document recommends way 2, so that no patch has to be carried. The
recommendation here stays, because way 2 writes the server's interface a second time.

In each way the runner is built from source by the worker's owner. Design 0038 lists "shipping
model weights or llama.cpp binaries" as a non-goal, and that stays (Q46).

### D4 Case 1: weights by tensor (LLM-13)

A client command sends the model. The name is a proposal:
`oxideswarm llm push --service <NAME> --model <GGUF>`. It runs on the source, which may be the
asking machine.

1. **Index.** The command reads the GGUF header: the metadata, and for each tensor its name,
   type, shape, offset and size. It computes one `BlobId` per tensor and keeps the list, keyed
   by the file's size and modification time, so that a file is hashed once.
2. **Offer.** Through a tunnel of LLM-9 to the runner's load port, it sends the first bytes of
   the file, up to the start of the tensor data, and the list of names, digests and sizes. The
   runner builds the metadata from those bytes in memory (row 19).
3. **Need.** The runner answers with the digests that its cache lacks.
4. **Request by tensor.** The loader asks for one tensor at a time, in the order in which the
   model's code created them, which is not the order of the file (row 18). For each one the
   runner compares the size that the loader expects with the size in the list, and ends the
   load on a difference (row 16). It then asks the source for that tensor. The source reads it
   from its offset in the file and sends it. Between two tensors there is one round trip. More
   than one stream is added only if R15 shows that one stream does not fill the link.
5. **Check.** Two ways, and R17 chooses. In the first the runner receives a tensor whole,
   compares its BLAKE3 digest, and only then sets it. The extra memory is the largest tensor.
   In the second the runner sets each part as it arrives (row 17) and hashes on the way. When
   the digest differs at the end of a tensor, the runner ends the load, and no token is
   computed from that tensor. The extra memory is one part. Both keep G4.
6. **Write after the load.** The bytes of a tensor that is set go to a cache on disk through a
   bounded queue, at low priority, and the file enters the cache only when its digest matched.
   When the queue is full the tensor is skipped, and the load does not wait. The cache follows
   the rules of the worker's `BlobCache`: addressed by content, bounded, least recently used
   out first, a temporary file renamed into place, verified when read.
7. **Second load.** The need list is empty, and the tensors come from the cache (G3). The cache
   bound has to exceed the model for that, and the default of `--blob-cache-mb` does not for a
   4 GiB file. The guide says so.

- **No new grid message.** The exchange runs inside the tunnel, between the command and the
  runner. The grid change is LLM-9 alone. The master relays the bytes and stores nothing, where
  `submit --input` stores the file on the master and twice on the worker.
- **No retry in the middle.** When the tunnel closes, the load fails and the runner ends its
  process (D3). The tensors that reached the cache stay, so the next push sends only the rest.

### D5 Case 2: the head of a split runs on a worker

Design 0038 D5 starts `llama-server --rpc` "on the head node" and does not say which machine
that is. The proposal for the text of LLM-5:

1. The head is a chosen worker with a GPU of its own: the one with the most free memory. It is
   not the asking machine, unless the asking machine is that worker (G6).
2. The head's `llama-server` listens on loopback and is a named service. The asking machine
   reaches it through a tunnel of LLM-9.
3. Every other chosen worker runs `ggml-rpc-server` on loopback. The head reaches each through a
   tunnel of LLM-4.
4. In the first version the master runs on the head's machine. Clients and workers connect to
   the master, so each path is then one hop on the network. With the master elsewhere, every
   exchange between the head and an RPC server passes the master in both directions. A direct
   channel between two nodes would lift this rule (Q47).
5. Under upstream RPC the head reads the model and sends each remote device its tensors (row
   1). The head therefore needs the model: from a file, or from a stream once LLM-12 exists.
   The other workers receive their tensors from the head, not from the source.
6. One failed RPC call ends the head (row 4). The supervisor of LLM-2 starts the whole pipeline
   again. A request that was in flight fails and is not sent again, as in LLM-9.

By the source, and not measured (rows 11 and 21): upstream places the RPC devices first and the
output with the last layer. A head with a GPU of its own then holds the last layers and the
output. For each reply token the embedding goes out to the first remote device, the state at
each boundary crosses the network, and the logits stay on the head. A head with no device of
its own receives the logits of every token over the network. That is the reason for item 1.
The head also holds the embedding table in RAM. R18 reads the byte counts.

### D6 Order

1. R15, R16 and R17, in any order. They need two machines and llama.cpp built from source, and
   nothing from the grid. R18 runs with them or after them.
2. The trial APP-1 and decision Q41 of design 0061, then LLM-9.
3. LLM-12 and LLM-13, if the stop rules of R15 and R17 leave them open.
4. The rule of D5 enters LLM-5 when LLM-2 to LLM-4 are built. Design 0038 places them after
   FEAT-1.

Step 1 can start now. Steps 2 to 4 are proposals (Q45).

## Compatibility

- The spikes change nothing.
- LLM-12 and LLM-13 add no grid message. They need LLM-9, whose wire change design 0061
  describes. LLM-13 adds one subcommand, which enters the `README.md` reference when it is
  built.
- The command and the runner speak a small protocol of their own inside the tunnel. Its first
  frame carries a version, and a mismatch is refused before any tensor is sent.
- The patch of way 1 names one upstream commit. The head and the RPC servers of a split have to
  come from one commit as well: the major version of the RPC protocol was 4 in August 2026 and
  is 9 in November (row 23), and a client refuses another major version. LLM-5 should refuse a
  mixed set.

## Security

- **The target's owner can read the weights and every prompt** (design 0061, Security).
- **A GGUF is untrusted input to a C++ parser** (design 0038). The runner parses metadata that a
  cluster member sent. It is a process of its own, and it runs under `--isolation` where the GPU
  still works.
- **The load port** listens on loopback and is reachable only as a named service. Any client
  that may open a tunnel can replace the loaded model. One load runs at a time. Rights per
  client need the machine keys of ENROLL-1.
- **Memory.** An offer states the sizes before any byte arrives. The runner refuses an offer
  above a bound that the worker's owner sets, before it allocates. The bytes in flight are
  bounded.
- **The cache** verifies an entry when it reads it, which is the lesson of row 6.
- **The RPC ports of case 2** never listen beyond loopback (rows 4 to 7 and 25).
- **The master relays the weights** and can read them. Encryption is per hop (Q42 of design
  0061).
- **The spikes** run the unauthenticated RPC port between two machines. D2 limits where.

## Test plan

- **Unit.** The index of a small GGUF file: stable digests, the order of the file. The need
  list. Refusal of a tensor with a wrong digest, and of an offer above the bound. The bounded
  queue.
- **Integration**, with a stub runner that links no llama.cpp, through a tunnel on loopback. A
  first push sends every tensor. A second sends none. A tunnel cut in the middle leaves the
  finished tensors in the cache. A changed byte is refused.
- **Live.** G1, G2 and G3 on two machines, each number from a script, in a dated record. For G5
  the runner, the source and the link are each stopped during a load and during a reply.
- Tests give every child process a temporary `HOME`, as the repository's rules require.

## Work items

"Needs" lists items of this record, of other design records and decisions of the roadmap. Sizes
are the backlog's S, M and L. No item is in [BACKLOG.md](../BACKLOG.md).

| ID | Item | Size | Needs | Files | Done when | Status |
|---|---|---|---|---|---|---|
| R15 to R18 | Spikes (D2) | S each | Two machines, llama.cpp built from source | `benchmarks/model-bringup/` | The dated record and the result of each stop rule are in this record's status log | Planned |
| LLM-12 | The runner: a model server that takes its model from a stream (D3) | L | R17, Q46, LLM-9 | A directory outside the Cargo workspace, `docs/guides/` | A quantised model loads from a stream on each target system, a record shows G2, and a stopped runner leaves the GPU memory free | Planned |
| LLM-13 | `llm push`: weights by tensor through the tunnel, with a cache on the target (D4) | M | LLM-9, LLM-12 | `crates/cli/src/`, `crates/core/src/blob.rs`, `README.md` | The integration tests of the test plan pass, and a record shows G1 and G3 | Planned |

The rule of D5 is a proposed change to the text of LLM-5 and has no ID of its own.

## Decisions for the owner

The right column is a recommended default, not a decision, unless it says "Decided". They are
listed in [ROADMAP.md](../ROADMAP.md) as Q44 to Q47.

| ID | Question | Recommended default |
|---|---|---|
| Q44 | Is the path to develop the one where weights go from the file on one machine into the memory of another, with no file written there first? | Decided on 2026-11-11: yes, the owner chose it |
| Q45 | In which order: the spikes, then one worker that runs the whole model (LLM-12, LLM-13), then the head rule for a split? | Yes, as in D6. The split keeps its place in design 0038 |
| Q46 | How does the runner exist: a patch to `llama-server` that is offered upstream and built from source by the worker's owner, with no binary in a release? | Yes, after R17. No server of our own. An outside document recommends a server of our own, and D3 says why the default stays |
| Q47 | For a split: the rule that the master runs on the head's machine, or a direct data channel between two nodes? | The rule. A channel gets a record of its own if R18 shows that the extra hop matters and the master cannot move |

## Risks

| Risk | Answer |
|---|---|
| The RPC backend changes, or its reports stay closed as not planned | Each spike names its commit. LLM-4 waits for R16. After LLM-12, case 1 uses no RPC |
| Upstream does not take the patch | It stays a patch file for a named commit, and each update has a cost that the status log records |
| `llama_model_init_from_user` was made for tests (PR 19802). It reports no progress, does not check the data, and its callback cannot report a failure (row 18) | The runner does the first two itself and ends its process on a failed fetch (D3). The stop rule of R17 |
| The RPC path does not reach G2: one outside measurement gives a split a little over half the reply rate of one machine (row 26) | G2 is a target for case 1, which has no RPC in its path once LLM-12 exists. R15 and R18 measure the RPC path on the owner's machines |
| The patch of way 1 is more than one call: the fit step also reads the file by path (row 20) | The first patch covers the main model only, with the fit step off |
| The records show that the link is the only limit | Then any path meets G1, and what remains worth building is the cache (G3) |
| A split is slow on a home network (design 0038, status log of 2026-11-10) | The rule of D1, and R18 before LLM-5 |
| The target is a Windows PC: no RDMA, and no file system in memory that this record knows of | TCP, and way 1 |
| A tensor cache beside the blob cache fills the disk | One bound for both |

## Records

None yet.

## Status log

- 2026-11-11: Written at the owner's request, from four statements of the same day (see
  Context), from a reading of llama.cpp's source and from a research document that the owner
  supplied. The owner chose the path (Q44). Nothing is built, nothing is measured, no item is
  in the backlog, and the texts of LLM-4, LLM-5 and LLM-9 are unchanged.
- 2026-11-12: Three more research documents from the owner were read, and their main statements
  were checked against llama.cpp at `cb44fdb1892e` and against GitHub (Context, rows 16 to 26).
  By the source, a quantised model keeps its types on the path of `llama_model_init_from_user`,
  and each tensor has its device buffer when the callback runs. R17 still has to run it. D4 now
  asks for each tensor by name, in the loader's order. D5 names a head with a GPU of its own.
  R15 to R18 each gained a step or a variant. Nothing is measured or built, and Q45 to Q47
  stay open.
