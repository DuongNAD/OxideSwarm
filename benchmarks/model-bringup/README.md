# Model bring-up study (spikes R14 to R18)

Scripts for spike R14 of [design 0056](../../docs/design/0056-frontier-program.md): where the time
goes between a machine with no model on it and the first result. Spikes R15 to R18 of
[design 0062](../../docs/design/0062-model-weights-into-remote-memory.md) continue the study:
a model whose file is on another machine. A spike is throwaway code. It builds nothing in the
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
