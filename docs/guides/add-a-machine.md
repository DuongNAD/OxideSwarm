# Add a machine on a LAN with a short code

## Goal

Enrol a second machine on the same local network using a three-part spoken pairing code (`<nameplate>-<word>-<word>`) with zero copy-and-paste, and run a test task on it.

## Prerequisites

- An OxideSwarm master running on a LAN-reachable address (`--listen 0.0.0.0:8088`, and `--auth members` or `--auth both`).
- An admin credential (`cluster.secret` or the master host's auto-enrolled `node.key`) on the machine where you run `oxideswarm invite --code`.
- The `oxideswarm` binary built or installed on both machines (`README.md`).
- Inbound **UDP port `8087`** and an ephemeral **TCP port** allowed through the inviting machine's local firewall on your private LAN while `invite --code` is waiting.
- For machines on different networks (not on the same LAN), use an `oxs1-invite:...` string instead ([design 0051](../design/0051-machine-identity-and-invites.md)).

## Steps

### 1. Start a pairing session on the admin machine

On the master (or another enrolled `admin` machine on the LAN):

```bash
./target/release/oxideswarm invite --code
```

If the master runs on another machine or non-default port, pass `--master <MASTER_IP>:8088`.
Pass `--roles worker,client` if the new machine also needs to submit tasks.
The command creates a single-use, approval-required invite (maximum 10 minutes), listens on UDP port `8087` and an ephemeral TCP port, and prints a short code such as:

```text
Pairing code: 7-rocket-violin
On the new machine run: oxideswarm join --code 7-rocket-violin
Fallback if broadcasts are blocked: oxideswarm join --code 7-rocket-violin --pair-host <MASTER_IP>:<PAIR_PORT>
```

Keep this terminal open.

### 2. Join from the new machine

On the new machine (macOS or Linux):

```bash
./target/release/oxideswarm join --code <CODE>
```

On the new machine (Windows PowerShell):

```powershell
.\target\release\oxideswarm.exe join --code <CODE>
```

If Wi-Fi client isolation or a firewall drops UDP broadcasts on port `8087`, pass the `--pair-host <MASTER_IP>:<PAIR_PORT>` hint printed on the first terminal:

```bash
./target/release/oxideswarm join --code <CODE> --pair-host <MASTER_IP>:<PAIR_PORT>
```

If you mistype a word, `join --code` exits before touching the network and suggests the closest word from the 2048-word BIP-0039 list.

### 3. Compare the 6-digit pairing check and confirm on both terminals

When the SPAKE2 exchange succeeds, the inviting terminal shows the joining machine's name, OS, architecture, IP address, and a 6-digit pairing check (`NNN NNN`):

```text
<WORKER_NAME> (<OS>/<ARCH>, <PEER_IP>:<PEER_PORT>) wants to join. Pairing check 418 302. Approve? [y/N]
```

At the same time, the joining terminal shows:

```text
Pairing code: 418302  (Master key fingerprint: <FINGERPRINT>)
Pending enrollment ID: <PENDING_ID> ...
WARNING: Joining a cluster gives that cluster's master and clients code execution on this machine (use `oxideswarm worker --isolation fs` where supported).
Confirm that the cluster admin's master shows this pairing code and fingerprint [y/N]:
```

Verify that all six digits match across both screens, then enter `y` on **both** terminals.
Only after both confirmations succeed does the joining machine write its `~/.oxideswarm/node.key` and `~/.oxideswarm/rusty-grid.toml` (it does **not** copy `cluster.secret`).

### 4. Start the worker

On the new machine (macOS or Linux; omit `--master`, `join` saved it in `rusty-grid.toml`):

```bash
./target/release/oxideswarm worker
```

On the new machine (Windows PowerShell):

```powershell
.\target\release\oxideswarm.exe worker
```

## Verify

On the master or an enrolled client machine, confirm the worker is online and runs a command:

```bash
./target/release/oxideswarm workers
./target/release/oxideswarm submit --worker <WORKER_NAME> --wait -- echo paired-ok
```

Expected output: `<WORKER_NAME>` appears in `workers`, and `submit --wait` prints `paired-ok`.

### Recorded two-machine run

Run on 2026-11-14 between 12:06 and 12:14 UTC on one private LAN (`Link: TCP/LAN`) with isolated temporary home directories on both machines at commit `917c032` (on `master` `8596c5fd`):

| Field | Value |
|---|---|
| Master and inviter | MacBook Air, Apple M5 (10 cores), 32 GiB, macOS (Darwin 27.2.0). Master started with `--listen <MASTER_IP>:18088 --auth both` (master key fingerprint `bb45-78c6-d1fd-23a5`). Inviter ran `oxideswarm invite --code --roles worker,client --master <MASTER_IP>:18088`. |
| Joining worker | Laptop with an Intel Core i7-1355U (10 cores, 12 threads), 16 GB, Windows 11 (`x86_64`). Ran `oxideswarm.exe join --code 422-tool-humor` with no `--pair-host` flag. |

1. On the inviting machine (`macOS`), `oxideswarm invite --code --roles worker,client --master <MASTER_IP>:18088` printed:

```text
Invite 885cc5ca9f2ce341d97e8f5dda6c7872 ready (roles: worker,client, expires in 600s).
Pairing code: 422-tool-humor
On the new machine run: oxideswarm join --code 422-tool-humor
Fallback if broadcasts are blocked: oxideswarm join --code 422-tool-humor --pair-host <MASTER_IP>:56924
```

2. On the joining machine (`Windows 11`), `oxideswarm.exe join --code 422-tool-humor` found the inviter on the first UDP port `8087` broadcast query without `--pair-host`. Both terminals printed the matching 6-digit pairing check (`711 627`):
   - Inviter terminal: `<WORKER_NAME> (windows/x86_64, <JOINER_IP>:55177) wants to join. Pairing check 711 627. Approve? [y/N]`
   - Joiner terminal: `Pairing code: 711627  (Master key fingerprint: bb45-78c6-d1fd-23a5)` and `Pending enrollment ID: 5c41330f4b2572a9`
3. Confirming `y` on both terminals enrolled member `269b134d8914e3de` (`roles: worker,client`), wrote `<TEMP_HOME>\.oxideswarm\node.key` and `<TEMP_HOME>\.oxideswarm\rusty-grid.toml` (with no `cluster.secret` on the Windows machine), and ran `doctor`:

```text
PASS  C1 Connect                                 connected without a cluster secret in 34 ms
Summary: 3 passed, 0 warnings, 0 failed, 2 skipped
```

4. Starting `oxideswarm.exe worker --no-portal --no-gpu` on the Windows machine registered worker `592f86b2-51cf-4e1f-b338-d1539f2c8cf9` (`Cores: 12, RAM: 16068MB, GPU: false, Simulated: false`, `Link: TCP/LAN`). On the master machine (`macOS`), `oxideswarm submit --master <MASTER_IP>:18088 --worker <WORKER_NAME> --wait -- echo paired-ok` completed:

```text
Task 0518c0f0-c8ce-421d-82c5-585ff97ffbd0 completed successfully.
Worker: <WORKER_NAME> (592f86b2-51cf-4e1f-b338-d1539f2c8cf9)
Exit Code: 0
Execution Time: 348ms
Stdout:
paired-ok
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Cannot bind pairing UDP listener on 0.0.0.0:8087` | Another `oxideswarm invite --code` process is already running on this host | Finish or Ctrl+C the other `invite --code` session before starting a new one |
| `no inviter answered for pairing code nameplate ... on UDP port 8087 within 30s` | UDP broadcast is dropped by Wi-Fi client isolation, a VLAN boundary, or the inviter's firewall on port `8087/udp` | Allow inbound UDP `8087` on the inviter's private LAN profile, or pass `--pair-host <MASTER_IP>:<PAIR_PORT>` copied from the inviter's output |
| Inviter exits with `the code was used by someone who did not know it; nothing was shared; make a new code` | The joining machine typed a wrong code after the SPAKE2 exchange began, timed out (5 s step limit), or closed early; the burn rule revoked the invite after one guess | Run `oxideswarm invite --code` again to generate a fresh nameplate and word pair |
| `did you mean '...'?` from `join --code` | One of the two words is not in the 2048-word BIP-0039 list | Re-run with the corrected word; typos fail locally before any network packet is sent |
| `A join token must not be given as an argument` | An invite or cluster join token (`oxs1-...`) was passed on the command line instead of piped on stdin | Pipe `oxs1-...` tokens into `oxideswarm join` with no `--code` and no positional arguments |
