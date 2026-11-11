# 0038: Large language model inference on the grid

Status: Planned
Created: 2026-11-07    Updated: 2026-11-07

## Summary

OxideSwarm runs no language model today. This record fixes the direction for when that work starts:

- **Expert routing.** Each worker keeps its own model server loaded, and the grid sends a prompt to
  the worker whose tag names the model (D1).
- **One model over several machines.** The grid starts and supervises llama.cpp RPC servers, and
  carries their traffic over the authenticated, encrypted grid channel (D2 to D5).

OxideSwarm does not write its own inference engine. Nothing in this record is built.

## Context

What exists on `master` at `d021094` that this work would build on, and what is missing:

| Area | Today | Code |
|---|---|---|
| Task model | A task is one process, started by the worker and finished before its result is sent. No task kind runs as a long-lived service. | `TaskSpec` (`crates/core/src/task.rs`), `TaskRunner` (`crates/worker/src/runner.rs`) |
| Output | stdout and stderr are captured up to `DEFAULT_MAX_OUTPUT_BYTES` (10 MiB) and returned with `TaskResult`. `WorkerMessage::TaskProgress` carries only a `TaskStatus`, so output is not streamed. | `runner.rs`, `WorkerMessage` (`crates/core/src/protocol.rs`) |
| Placement | A worker advertises user tags with `worker --tag`. `submit --require-tag` places a task on a worker with that tag. | `crates/core/src/placement.rs`, `crates/cli/src/cli_app.rs` |
| Memory | `WorkerCapabilities` has `ram_mb` and an optional `vram_mb`. `vram_mb` comes from configuration (`WorkerConfig::with_vram_mb`), not from a probe. Only 3D tasks ask for VRAM (`required_vram_mb`). | `crates/core/src/capabilities.rs`, `crates/master/src/scheduler.rs` |
| GPU | `TaskSpec::GpuCompute` runs a seeded GEMM benchmark through wgpu. There is no model loading, no quantized kernel and no attention. | `crates/worker/src/wgpu_engine.rs`, `shaders/gemm.wgsl` |
| Transport | TCP frames of at most `MAX_FRAME_SIZE` (64 MiB), AEAD after the cluster-auth handshake, `TCP_NODELAY` set by `configure_tcp_stream`. iroh QUIC uses ALPN `rusty-grid/v1` with a control and a data stream. Nothing forwards an arbitrary TCP port. | `crates/core/src/transport.rs`, `crates/core/src/cluster_auth.rs` |

### Physical limits

These limits decide what the grid can and cannot gain. They come from how decoding works, not from
this code base.

- **Memory bandwidth sets the speed.** For one request, each new token reads every active weight
  once. Tokens per second are therefore at most the memory bandwidth divided by the bytes of active
  weights per token.
- **More machines add capacity, not speed.** Splitting the layers of one model over machines
  (pipeline parallelism) lets a model larger than one machine's memory run. The stages still run one
  after another for each token, and every stage boundary adds a network round trip. One request is
  not faster than on one machine with the same bandwidth.
- **Mixture-of-experts sparsity does not remove the network.** A sparse model reads fewer weights
  per token, which helps on every machine. Placing the experts of every layer on different machines
  (expert parallelism) needs a round trip per layer per token. That works over data-centre
  interconnects, not over home Ethernet or Wi-Fi.
- **Throughput across many requests does scale.** Independent requests on independent copies of a
  model run in parallel. That is the case D1 serves.
- **Phones do not help.** Their memory is small, they throttle when hot, and Android may kill their
  child processes (`ANDROID_ARCHITECTURE.md`).

### Existing tools

llama.cpp already splits one model over machines. `ggml-rpc-server` exposes a device on a port, and
`llama-server --rpc <HOST:PORT>,...` spreads weights and the KV cache over the local and remote
devices. Its README calls the RPC backend fragile and insecure and says never to run it on an open
network. It has no authentication. The grid already has what that path lacks: a cluster secret,
encrypted transport, NAT traversal over iroh, and placement across mixed machines.

## Goals

- **G1** A client (CLI or MCP) sends a prompt to a worker that holds a named model, chosen by tag,
  and gets the completion back.
- **G2** A model whose weights exceed the memory of any single machine runs split over several
  workers.
- **G3** No unauthenticated model port is reachable beyond loopback. Model traffic between machines
  travels over the cluster-authenticated, encrypted grid channel.
- **G4** The master places model work by free RAM and VRAM.

## Non-goals

- An inference engine or kernels of our own. llama.cpp (or Ollama, which builds on it) does the
  compute.
- Expert parallelism across machines, layer by layer.
- Training or fine-tuning.
- Phones as model nodes.
- Being faster than one machine for one request (see Physical limits).
- Shipping model weights or llama.cpp binaries in this repository.

## Design

### D1 Expert routing through local model servers (LLM-1)

1. The owner of a worker starts a model server on that machine, bound to loopback: `ollama serve`
   or `llama-server --host 127.0.0.1`. It stays loaded between requests.
2. The worker advertises the model with a user tag, for example `--tag model:qwen-coder-14b`.
3. A client submits a `Command` task with `--require-tag model:<name>`. The task posts the prompt to
   the local server's HTTP API and prints the reply. Loading happens once, in the server, not per
   task.
4. Routing between experts and merging their answers (mixture of agents) runs in the client: a
   script or an MCP client. The master stays a scheduler.

D1 needs no wire change. It ships as a guide (`docs/guides/`), an example script, and an MCP usage
note. Its limits are the current ones: the reply arrives when the task ends, and is capped by the
output limit.

### D2 Service tasks (LLM-2)

A new task kind that runs until it is cancelled, for `ggml-rpc-server` and `llama-server`.

- It declares a readiness check: a loopback TCP port that accepts, or an HTTP path that answers.
- The worker reports `Starting`, `Ready` or `Failed`, and restarts the process under a bounded
  policy.
- A cancel stops the whole process tree, as for other tasks (`process_tree.rs`).
- When the worker is lost, the master requeues the service like any task. The client of a split
  model (D5) then has to rebuild the pipeline.

This is a wire change: a new `TaskSpec` variant with both mirror enums and all four `From` impls,
gated by a `ProtocolCapabilities` bit.

### D3 Memory-aware placement (LLM-3)

- Workers report free RAM at each heartbeat. They report VRAM from a probe where the platform has
  one, else from configuration.
- Tasks state a memory need, for example `require:min_ram_mb=<N>` or `require:min_vram_mb=<N>`.
- The scheduler checks it in the static stage, as other placement constraints are checked today.

This overlaps 3D-2 (VRAM routing for 3D tasks). Both should use one mechanism.

### D4 Authenticated tunnel for model traffic (LLM-4)

- `ggml-rpc-server` binds to loopback on its worker.
- The head node runs `llama-server --rpc` against local tunnel endpoints, one per remote worker.
- The grid carries the bytes between the two:
  - on TCP, a new connection type that runs the cluster-auth handshake and then AEAD records;
  - on iroh, a stream kind or ALPN of its own, so model bytes never share the control stream.
- Model loading moves large tensors, so the tunnel must not use the 64 MiB message frame as its unit.
  `ggml-rpc-server -c` caches tensors on the worker, which avoids resending them after the first
  load.

Open question: the cost of AEAD on this path, measured against plain loopback. It needs a benchmark
script before any number is written down.

### D5 One command to run a split model (LLM-5)

`oxideswarm llm up --model <GGUF> [--workers <N>]`:

1. picks workers by free memory (D3);
2. starts `ggml-rpc-server` as a service task on each (D2);
3. opens the tunnels (D4);
4. starts `llama-server --rpc ...` on the head node;
5. prints the local URL of its OpenAI-compatible API.

`llm down` stops all of it. llama.cpp spreads the layers in proportion to each device's memory.

## Compatibility

- D1 changes nothing on the wire and works with any current master and worker.
- D2 and D4 add wire variants. They are gated by `ProtocolCapabilities` bits, so a peer that lacks
  them never receives them. Upgrade the master first (`DEPLOYMENT.md` 3.5).
- D3 adds placement keys. `Placement` already refuses an unknown `require:` key
  (`crates/core/src/placement.rs`), so an older master refuses such a task at submit instead of
  placing it anywhere.

## Security

- Model servers run as the worker's OS user, like every task (`docs/SECURITY.md`).
- Ollama, `llama-server` and `ggml-rpc-server` listen on loopback only. The worker refuses to start
  a service task whose readiness port is bound to a non-loopback address.
- The tunnel of D4 is the only path between machines. It needs the cluster secret.
- Prompts and replies pass through the master, like task arguments and output today. Anyone with
  the cluster secret can read them.
- A GGUF file is untrusted input to a C++ parser. Load only models the owner chose. Treat a model
  path in a submitted task like any other file input.

## Test plan

- **D1:** an integration test with a stub HTTP model server behind a tagged worker. It checks that
  the prompt reaches the tagged worker only, and that the reply comes back.
- **D2:** start, readiness, restart limit, cancel of the process tree, and requeue after worker loss.
- **D3:** placement tests for memory constraints, including a task no worker can hold.
- **D4:** an echo test through the tunnel with and without the secret. A connection without the
  secret is refused. A test shows the remote RPC port is not reachable without the tunnel.
- **Live:** one run with llama.cpp on a Mac and a Windows machine, with a dated record of the
  commands and their output, before any document says it works.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| LLM-1 | Expert routing through local model servers (D1). Size S. | `docs/guides/`, `scripts/`, `README.md` | A guide and an example script send a prompt to the tagged worker and print the reply. The D1 integration test passes. | Planned |
| LLM-2 | Service tasks (D2). Size L. Needs FEAT-1 and FEAT-8. | `crates/core/src/task.rs`, `protocol.rs`, `crates/worker/src/runner.rs`, `crates/master/src/queue.rs` | The D2 tests pass. A service survives a worker restart by requeue. | Planned |
| LLM-3 | Memory-aware placement (D3). Size M. Do it together with 3D-2. | `crates/core/src/placement.rs`, `crates/master/src/scheduler.rs`, `crates/worker/src/heartbeat.rs` | The D3 tests pass. | Planned |
| LLM-4 | Authenticated tunnel for model traffic (D4). Size L. Needs LLM-2. | `crates/core/src/transport.rs`, `cluster_auth.rs`, a new tunnel module | The D4 tests pass, and a benchmark script records the tunnel's throughput. | Planned |
| LLM-5 | `oxideswarm llm up` and `llm down` (D5). Size M. Needs LLM-3 and LLM-4. | `crates/cli/src/`, `README.md` | The live run of the test plan is recorded. | Planned |

Order: LLM-1 can start at any time. LLM-2 to LLM-5 wait until the grid itself is durable (FEAT-1).

## Status log

- 2026-11-07: Created as Planned at the owner's request, from a discussion of mixture-of-experts
  and mixture-of-agents designs. Nothing is built.
- 2026-11-10: The research in [design 0056](0056-frontier-program.md) found the split of one model
  slow on ordinary networks: about 1.5 tokens per second for a 70B model over Wi-Fi in the
  prima.cpp paper. Design 0056 adds LLM-6 to LLM-8 on the request path of D1. LLM-2 to LLM-5 keep
  their order. Nothing is built.
- 2026-11-11: The owner asked whether a mixture-of-experts model can run over several machines
  with the answer arriving on a weaker one. The physical limits in Context stand: placing the
  experts of one model on different machines needs a round trip per layer per token. The case that
  fits the question is D1, whole models on separate machines and one request per answer.
  [Design 0059](0059-first-users.md) names LIVA as a first application for it. Nothing is built.
