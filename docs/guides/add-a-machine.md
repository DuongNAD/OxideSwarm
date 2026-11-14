# Add a machine on a LAN with a short code

## Goal

Enrol a second machine on the same local network using a three-part spoken pairing code (`<nameplate>-<word>-<word>`) with zero copy-and-paste, and run a test task on it.

Status note: automated tests (`tests/cli_pair_e2e.rs`) pass on loopback; the live two-machine LAN rehearsal between Windows and macOS (`JOIN-2b`) is pending.

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

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Cannot bind pairing UDP listener on 0.0.0.0:8087` | Another `oxideswarm invite --code` process is already running on this host | Finish or Ctrl+C the other `invite --code` session before starting a new one |
| `no inviter answered for pairing code nameplate ... on UDP port 8087 within 30s` | UDP broadcast is dropped by Wi-Fi client isolation, a VLAN boundary, or the inviter's firewall on port `8087/udp` | Allow inbound UDP `8087` on the inviter's private LAN profile, or pass `--pair-host <MASTER_IP>:<PAIR_PORT>` copied from the inviter's output |
| Inviter exits with `the code was used by someone who did not know it; nothing was shared; make a new code` | The joining machine typed a wrong code after the SPAKE2 exchange began, timed out (5 s step limit), or closed early; the burn rule revoked the invite after one guess | Run `oxideswarm invite --code` again to generate a fresh nameplate and word pair |
| `did you mean '...'?` from `join --code` | One of the two words is not in the 2048-word BIP-0039 list | Re-run with the corrected word; typos fail locally before any network packet is sent |
| `A join token must not be given as an argument` | An invite or cluster join token (`oxs1-...`) was passed on the command line instead of piped on stdin | Pipe `oxs1-...` tokens into `oxideswarm join` with no `--code` and no positional arguments |
