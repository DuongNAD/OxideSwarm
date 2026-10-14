# OxideSwarm Cross-Machine Reachability Probe (shared-folder mailbox)

## What this is (and is not)
`sync_network.py` is a small, zero-dependency Python 3 tool that lets two machines which share a cloud
folder (Google Drive, OneDrive, a network mount) check that they can reach each other over the LAN:

1. Each machine writes a *claim* file (`claim_<platform>.json`) with its LAN IP into the shared folder.
2. They pick roles: the machine with the older, still fresh claim becomes the SERVER (ties: macOS wins).
   Claims older than 180 s are ignored, and the default observation window is 15 s because cloud sync
   is slower than a local disk.
3. The SERVER answers HTTP `GET /ping` on **port 8085**; the CLIENT dials it, and on success writes
   `ping_success.txt`. Failures are written to an error log in the shared folder.

It is **not** part of the grid: it never starts, joins or queries a master or a worker, it does not
exchange P2P tickets (it only notes whether a `p2p_ticket_<platform>.txt` file exists), and a successful
ping says nothing about the grid ports 8088 (cluster), 8080 (dashboard) or 8089 (UDP discovery). To check
those, use `oxideswarm status --master HOST:8088` (needs the cluster secret, see `docs/SECURITY.md`).

Never put a P2P ticket or the cluster secret into a cloud-synced folder: anyone with access to the folder
(or to the cloud account) could use it.

## Files
```
scripts/sync_network/
├── sync_network.py      # the engine (Python 3.6+, standard library only)
├── sync_network.sh      # macOS / Linux launcher
├── sync_network.cmd     # Windows launcher (py / python auto-detection)
├── protocol_v1.json     # design sketch of a larger mailbox (nodes/, verifications/, state machine); only part of it is implemented
├── ping_success.txt     # HISTORICAL sample output (2026-09-23, the author's machines)
├── SUCCESS_CONFIRMED.md # HISTORICAL hand-written record of one session; its ticket was redacted
└── README.md
```

Files the script writes into the shared folder:
- `claim_<platform>.json`, `<platform>_ip.txt`: the role claim and the LAN IP.
- `server_ready.txt`: written by the SERVER; honoured for 180 s.
- `ping_success.txt`: written by the CLIENT after a successful ping.
- `<platform>_error_log.txt`: connection errors with a remediation hint.

## Usage
```bash
./scripts/sync_network/sync_network.sh                     # macOS / Linux, auto role
scripts\sync_network\sync_network.cmd                       # Windows
python3 scripts/sync_network/sync_network.py --sync-dir /path/to/shared/folder
python3 scripts/sync_network/sync_network.py --mode server # force a role (server | client | scan | publish)
python3 scripts/sync_network/sync_network.py --observe-time 30   # longer wait for slow folder sync
```
The shared folder is found automatically for Google Drive (`~/Library/CloudStorage/GoogleDrive-*`,
`~/Google Drive`, `G:\`), or set `OXIDESWARM_SYNC_DIR` / `--sync-dir`.

## Known limitations
- The peer is assumed to be one macOS and one Windows machine (`PEER_PLATFORM`); Linux and phones are not modelled.
- Role selection relies on both machines seeing each other's claim within the observation window. On a
  very slow folder both can still elect SERVER; raise `--observe-time`, or force the roles with `--mode`.
- The verification table of an earlier version of this README (TCP, HTTP API, UDP discovery, worker
  registration, submit) was a hand-recorded session on the author's machines and is not something this
  script performs; it was removed.
