"""Short PyTorch CUDA training run for the cross-machine GPU check.

Trains a small MLP on random data for a fixed number of steps on the first
CUDA device and prints device, timings, loss and peak memory. Exits 2 when
CUDA is unavailable, so the task fails instead of silently using the CPU.
"""

import sys
import time

import torch

if not torch.cuda.is_available():
    print("NO_CUDA torch", torch.__version__)
    sys.exit(2)

dev = torch.device("cuda:0")
print("torch", torch.__version__, "cuda", torch.version.cuda, "device", torch.cuda.get_device_name(0))

torch.manual_seed(0)
model = torch.nn.Sequential(
    torch.nn.Linear(4096, 4096),
    torch.nn.ReLU(),
    torch.nn.Linear(4096, 4096),
    torch.nn.ReLU(),
    torch.nn.Linear(4096, 10),
).to(dev)
opt = torch.optim.AdamW(model.parameters(), lr=1e-3)
x = torch.randn(1024, 4096, device=dev)
y = torch.randint(0, 10, (1024,), device=dev)
loss_fn = torch.nn.CrossEntropyLoss()

torch.cuda.synchronize()
t0 = time.perf_counter()
for step in range(200):
    opt.zero_grad(set_to_none=True)
    loss = loss_fn(model(x), y)
    loss.backward()
    opt.step()
    if step == 0:
        torch.cuda.synchronize()
        first = time.perf_counter() - t0
        print(f"step0_loss {loss.item():.4f} first_step_ms {first * 1000:.0f}")
torch.cuda.synchronize()
total = time.perf_counter() - t0
print(f"final_loss {loss.item():.4f} steps 200 total_ms {total * 1000:.0f} ms_per_step {(total - first) / 199 * 1000:.2f}")
print(f"peak_mem_mib {torch.cuda.max_memory_allocated() / 2**20:.0f}")
print("PYTORCH_CUDA_TRAIN_OK")
