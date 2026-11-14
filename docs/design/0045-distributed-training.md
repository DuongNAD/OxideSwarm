# 0045: Distributed training across workers

Status: Planned
Created: 2026-11-08    Updated: 2026-11-12

## Summary

OxideSwarm trains no model today. This record fixes the direction for when that work starts:

- **The grid places and moves; a framework trains.** PyTorch, or another framework the owner
  installs, does the compute inside ordinary `Command` tasks. The grid picks workers, moves data and
  weights as blobs, and collects results.
- **Independent runs first** (D1), then **low-communication rounds** in the style of DiLoCo (D2),
  with **adapter-only rounds** for fine-tuning (D3).
- **A first workload for independent runs** is named in D6: grading edits that another model
  wrote into the weights of a small model (spike R19).
- Work starts after the core of the roadmap: the Phase D exit check of `ROADMAP.md` (decision Q6).
  Nothing in this record is built.

## Context

What exists on `master` at `01a9786` that this work would build on, and what is missing:

| Area | Today | Code |
|---|---|---|
| Task model | A task is one process that ends before its result is sent. `submit --timeout-secs` sets its limit (default 60 s). | `TaskSpec` (`crates/core/src/task.rs`), `TaskRunner` (`crates/worker/src/runner.rs`) |
| Files in | `submit --input LOCAL[:SANDBOX]` sends files and directories as BLAKE3 blobs. One submission holds at most 1,000 files and 256 MiB (`DEFAULT_MAX_INPUT_FILES`, `DEFAULT_MAX_INPUT_BYTES`), with no flag to raise them. | `crates/cli/src/blobs.rs` |
| Files out | `submit --output PATTERN` brings files back. A task may return at most 1024 MiB (`worker --output-limit-mb`). `TaskResult::outputs` lists each output as a `BlobFile` in the master's store. | `crates/worker/src/staging.rs`, `crates/core/src/task.rs` |
| Blob storage | Blobs move in chunks of 4 MiB (`BLOB_CHUNK_BYTES`). The master keeps at most 4096 MiB (`master --blob-store-mb`) and a worker caches at most 2048 MiB (`worker --blob-cache-mb`), both with least-recently-used eviction. | `crates/core/src/blob_transfer.rs`, `crates/master/src/blob_store.rs`, `crates/worker/src/blob_cache.rs` |
| Placement | `worker --tag` advertises user tags. `submit --require-tag` and `submit --worker` place a task. | `crates/core/src/placement.rs`, `crates/cli/src/cli_app.rs` |
| GPU | A worker advertises a GPU only when it runs the wgpu engine and an adapter started. A machine with PyTorch and CUDA, or PyTorch and Metal (MPS), is not a GPU worker to the grid unless it also runs a `gpu-wgpu` build. | `crates/worker/src/gpu.rs` |
| Worker loss | The master requeues the task, with at most 3 retries by default (`RetryPolicy`). A process task starts again from zero, because only in-process tasks have checkpoints (FEAT-5). | `crates/master/src/queue.rs` |
| Map/Reduce | Map output is merged in master memory, so this path suits streams of records, not files of several hundred megabytes. | `crates/master/src/mapreduce.rs` |
| Isolation | A task runs as the worker's OS user, without OS isolation (FEAT-3). | `docs/SECURITY.md` |

### Physical limits

These limits come from how training works, not from this code base.

- **Synchronous data parallelism exchanges the gradients at every step.** With ring all-reduce,
  each machine sends and receives about twice the gradient size per step. One billion parameters in
  16-bit floats are 2 GB of gradients. A 1 Gbit/s link (about 125 MB/s) then needs about 32 s per
  step for the exchange alone, while the compute of one step takes a fraction of that on a current
  GPU. Data centres use links of 100 Gbit/s and more.
- **Splitting a model over machines** (tensor or pipeline parallelism) exchanges activations several
  times per step and is bound by latency as well as bandwidth.
- **Mixed machines slow a synchronous step to the slowest one.**
- **Low-communication training exchanges weights every H steps instead of every step.** DiLoCo
  (arXiv 2311.08105) runs H inner steps of AdamW on each machine, averages the change of the
  weights, and applies it with an outer optimizer that uses Nesterov momentum. The traffic falls by
  the factor H, and the paper reports quality close to synchronous training for its language model
  experiments. This is the regime home links can carry.
- **Phones do not help.** Their memory is small, they throttle when hot, and Android may kill their
  child processes (`ANDROID_ARCHITECTURE.md`).

### Existing tools

| Tool | Built for | Why the grid does not replace it |
|---|---|---|
| PyTorch DDP and FSDP with `torchrun` | Synchronous training on machines joined by a fast network | It is the trainer inside a task. Its rendezvous port has no cluster authentication, so the grid does not open it between machines |
| Ray Train | Distributed training inside a Ray cluster | It needs a Ray cluster. The grid runs any executable with one binary per machine |
| Hivemind | Decentralised training of one model over the Internet | It joins volunteers through a DHT. The grid targets machines that one person owns |

## Goals

- **G1** A script runs many independent training runs, such as a hyperparameter sweep, over the
  workers that have a framework, and collects each run's checkpoint and metrics.
- **G2** One model trains on several workers with an exchange every H inner steps. Weights move as
  blobs over the authenticated grid transport.
- **G3** A round finishes when a worker is lost or late: the coordinator averages the workers that
  reported and goes on.
- **G4** A large pretrained model is fine-tuned by exchanging only adapter weights.

## Non-goals

- A training engine, kernels or automatic differentiation of our own.
- Synchronous all-reduce at every step, and tensor or pipeline parallelism across machines.
- Shipping frameworks, CUDA, model weights or datasets in this repository.
- Contributors the owner does not trust. Averaging does not detect a worker that returns false
  weights.
- Phones as training nodes.
- GPU kernels in wgpu (decision D6 of `BACKLOG.md`).
- Being faster than one machine with a fast GPU for a model that fits on it.

## Design

### D1 Independent runs (TRAIN-1)

1. The owner of a worker installs the framework and advertises it with a user tag, for example
   `worker --tag torch:cuda`, `--tag torch:mps` or `--tag torch:cpu`.
2. A client submits one `Command` task per run:

   ```bash
   oxideswarm submit --require-tag torch:cuda --input data:data --input train.py \
     --output 'out/**' --timeout-secs 7200 -- python train.py --lr 3e-4 --out out
   ```

3. An example script in `scripts/` submits one task per configuration, waits for all of them, and
   writes a table of the metrics each run printed.

D1 needs no code change. It ships as a guide in `docs/guides/` and the example script. Its limits
are the current ones: 256 MiB of inputs per submission, 1024 MiB of outputs per task, and a run
that starts again from zero when its worker is lost.

### D2 Low-communication rounds (TRAIN-2)

A coordinator program on the client runs the outer loop. The master stays a scheduler.

1. The coordinator holds the global weights θ in a file, written in the `safetensors` format.
2. Each round, it submits one task per worker. The inputs are θ, the training script and that
   worker's data shard. `--worker` keeps each shard on the same worker, so the worker's blob cache
   serves the shard from the second round on, and the master stores θ once for all workers of the
   round.
3. Each task runs the inner loop under a time budget, for example 10 minutes, and writes its weights
   θᵢ and the number of steps it ran.
4. At the round deadline the coordinator cancels every task of the round that has not finished,
   including one the master requeued after a worker loss. It averages the weight changes of the
   tasks that finished, weighted by their steps, and applies the outer optimizer. That gives the
   next θ.
5. The coordinator keeps θ, the round number and the outer optimizer state on its own disk, so it
   can stop and resume between rounds.

A time budget instead of a fixed H lets fast and slow machines take part in the same round.

Traffic per round and worker: θ in and θ out. A model of 100 million parameters in 16-bit floats is
200 MB, about 1.6 s each way at 1 Gbit/s. With ten-minute rounds the exchange takes under 1 % of
the time.

The current limits cap θ until D4 lands. One submission carries at most 256 MiB, θ and the data
shard together, and a task returns at most 1024 MiB. Per round the master's blob store holds θ, the
shards and one output per worker, so with the default 4096 MiB and three workers θ must stay well
under 1 GiB unless `--blob-store-mb` is raised.

### D3 Adapter rounds (TRAIN-3)

For fine-tuning, the base weights stay on each worker and only adapters, such as LoRA matrices,
move. The base model arrives once as a blob input pinned to its worker, or the owner installs it and
advertises it with a tag such as `model:<name>`. Each round then moves only the adapter, typically
tens of megabytes. The D2 loop is otherwise unchanged. For most fine-tuning this makes the D2 limits
irrelevant.

### D4 Outputs as inputs (TRAIN-4)

- `submit --input-from <TASK_ID>:<PATTERN>` uses the output blobs of a finished task as inputs of a
  new one. `TaskResult::outputs` already names them, so the client builds the inputs without
  downloading the files. The master's admission check (`admit_blob_task` in
  `crates/master/src/server.rs`) already refuses a task whose input blobs are missing and pins those
  it accepts. A blob evicted between the two tasks therefore gives a clear refusal, not a wrong run.
- With it, the averaging of D2 runs as a task on a worker, and weights never pass through the
  client.
- `submit --max-input-mb` and `--max-input-files` raise the client's input limits, in line with the
  workspace limits of design 0039.

### D5 Rounds that survive the master (TRAIN-5)

Until FEAT-1 lands, a master restart loses the tasks of the current round. The coordinator then
resubmits the round from its own state. Until FEAT-5 lands, a lost task restarts its inner loop
from zero. Short rounds keep that loss small. After both land, a round resumes from the journal and
from the last checkpoint of each task.

### D6 A workload for independent runs: grading edits to the weights of a small model

**What was reported.** A blog post of 2026-11-02 by researchers at MIT CSAIL describes a large
hosted model that reads the weights of a small open model and writes new values for a small
share of them, with no gradient step
([post](https://rikiyat.github.io/blog/weight-space-programming/),
[logs](https://github.com/RikiyaT/lm_understand-logs)). It was read on 2026-11-12 through one
news article about the post, on VentureBeat. The post itself was not read. By that article:

- The patch to a model of 0.8 billion parameters touched about 0.016 % of its weights.
- The scores on three tests rose. The authors put the gain down to less repetition in the
  answers, and not to new skill.
- A second small model, which did not repeat itself, did not improve.
- No cycle of self-improvement was shown, and the patch still needed practice problems and
  tests to be found.

**What the owner proposed** on 2026-11-12: to use many machines for work of this kind.

**Reading.** The writer of an edit is the hosted model. A grid of personal machines cannot take
its place. The writing does not split over machines either, because edits act on each other.
What does split is the test of each candidate. Every candidate needs a full pass over a test
set, the passes do not depend on each other, and each returns one number. That is D1.

**One task.**

- **On the worker,** installed by its owner as in D1: the framework and the base model, each
  advertised with a user tag.
- **Inputs:** one patch file, which holds the changed values and where they go, one shard of
  the test set, and the grading script. The base model is not an input. A model of that class
  is larger than the 256 MiB that one submission may carry.
- **Output:** one small file with the score of the shard and the answers.
- **The client** submits one task for each pair of patch and shard, and adds up the scores.

**Limits.**

- A hosted model that writes patches costs money for each attempt. The first step uses only
  patches that others have published (Q51 in [ROADMAP.md](../ROADMAP.md)).
- The test has to grade itself.
- The report is early and rests on one model. This record claims nothing about
  self-improvement.
- The grid checks no result here. A worker that returns a wrong score is not caught. Until the
  receipts of [design 0056](0056-frontier-program.md) exist, the only check is a second run of
  the same task on another worker.

**Spike R19**, sized S, before TRAIN-1 is sized. The question: do two workers together grade
as many items per hour as the two do alone, added up? The method: a small open model on each of
the owner's two machines, and one public test set cut into shards, with one task for each
shard. It runs on the first worker, on the second, and on both. If a published patch can be
applied, the run is repeated with the patched model, and the record holds both scores. The
record holds the date, both machines, the model, the test set, every command and the items
graded per hour. The stop rule: if the two together grade less than 90 % of the sum, the cause
is found before TRAIN-1 is sized.

## Compatibility

- D1, D2, D3 and D5 need no change to the wire format. They use `Command` tasks, user tags, blob
  inputs and outputs, and cancellation, which every current master and worker supports.
- D4 changes only the client. It works with any master that runs a blob store.

## Security

- Training code is a program the owner chose, run as the worker's OS user like any task. Until
  FEAT-3 lands it can read `cluster.secret` and other sandboxes on that worker.
- Weights cross machines only in the `safetensors` format. `torch.load` of a pickled checkpoint can
  run code from the file, so the coordinator and the example scripts never load one from another
  machine.
- No framework port opens between machines. `torchrun` and other rendezvous servers stay on one
  machine or are not used, and all traffic between machines is blob transfer over the grid.
- Datasets and weights pass through the master's blob store. Anyone with the cluster secret can read
  them.
- Averaging trusts every worker (see Non-goals).

## Test plan

- **D1:** an integration test with a stub trainer, a Python script that uses only the standard
  library and writes an output file from its arguments. The sweep script places each run on a
  worker with the right tag and collects every output.
- **D2:** linear regression trained by plain gradient descent in a standard-library script, over
  three local workers. The test checks that the rounds converge to the known solution, that a slow
  worker is cut at the deadline and the round still completes, that a lost worker's shard is
  skipped, and that each worker fetches its shard once across rounds.
- **D3:** the D2 loop with an adapter file only. The base blob is fetched once per worker.
- **D4:** `--input-from` builds inputs without a download to the client, and refuses an evicted
  blob with a clear error.
- **Live:** one D2 run with PyTorch on a Mac (MPS) and a Windows machine (CUDA), with the loss
  curve next to a single-machine run, recorded with its date before any document says it works.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| TRAIN-1 | Independent runs (D1). Size S. | `docs/guides/`, `scripts/` | The guide and the example script exist, and the D1 test passes | Planned |
| TRAIN-2 | Low-communication rounds (D2). Size M. | `scripts/`, `docs/guides/` | The D2 tests pass, and the live run is recorded | Planned |
| TRAIN-3 | Adapter rounds (D3). Size S. Needs TRAIN-2. | `scripts/`, `docs/guides/` | The D3 test passes | Planned |
| TRAIN-4 | Outputs as inputs and input limits (D4). Size M. | `crates/cli/src/blobs.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/src/mcp.rs` | The D4 tests pass | Planned |
| TRAIN-5 | Rounds that survive the master (D5). Size S. Needs FEAT-1 and FEAT-5. | `scripts/` | A test restarts the master during a round, and the round completes | Planned |
| R19 | Spike: two workers grade one test set (D6). Size S. | `benchmarks/` | A dated record holds the items graded per hour on each worker alone and on both, from a committed script | Planned |

Order: every item waits for the Phase D exit check of `ROADMAP.md` (decision Q6). TRAIN-1 comes
first. TRAIN-4 comes before TRAIN-2 when θ is larger than 256 MiB.

## Status log

- 2026-11-08: Created as Planned at the owner's request, from a discussion of training one model on
  several machines. The owner decided that it waits until the core of the roadmap is done. Nothing
  is built.
- 2026-11-12: Added D6 and spike R19 at the owner's word, after a report that a large model
  wrote edits into the weights of a small one. The owner proposed to use many machines for such
  work. D6 reads that as grading candidates in parallel, which is D1. The report was read through
  one news article, and the post itself was not read. Nothing is built or measured.
