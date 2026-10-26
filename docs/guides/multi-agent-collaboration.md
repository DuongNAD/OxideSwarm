# Coordinate two coding agents across macOS and Windows via Google Drive

## Goal

Two coding agents (`agent-mac` on macOS and `agent-win` on Windows) exchange telemetry, detect configuration or git drift, file root-cause incident reports, and coordinate fixes through a shared `OxideSwarm_Agent_Hub` folder on Google Drive without file-sync collisions or secret exposure.

## Prerequisites

- On both machines: Python 3.7 or later and a checkout of this repository.
- Google Drive Desktop mounted on both machines (`~/Library/CloudStorage/GoogleDrive-*` on macOS, `G:\My Drive` on Windows), or a custom shared path in `OXIDESWARM_AGENT_HUB`.

## Steps

### 1. Publish node state and check the inbox

On the Mac (Terminal, repository root):

```bash
python3 scripts/agent_bridge.py heartbeat
python3 scripts/agent_bridge.py inbox
```

On Windows (PowerShell, repository root):

```powershell
python scripts/agent_bridge.py heartbeat
python scripts/agent_bridge.py inbox
```

Each agent writes only its own state file (`nodes/mac_state.json` or `nodes/win_state.json`) using atomic temporary-file replacement (`atomic_write` in `scripts/agent_bridge.py`) so concurrent syncs never collide.

### 2. Run cross-machine root-cause diagnostics

On the Mac (Terminal, repository root):

```bash
python3 scripts/agent_bridge.py diagnose
```

On Windows (PowerShell, repository root):

```powershell
python scripts/agent_bridge.py diagnose
```

`diagnose` compares `mac_state.json` and `win_state.json` and checks:
- **Git commit parity**: `git_head` and `git_dirty` across both checkouts.
- **Cluster secret parity**: compares the 12-character SHA-256 prefix (`secret_sha256_12`) of `~/.oxideswarm/cluster.secret` without writing secret bytes to cloud storage.
- **Stale P2P ticket hazard**: flags an active `p2p_ticket` in `~/.oxideswarm/rusty-grid.toml` that would cause a worker to ignore `--master <MASTER_IP>:8088`.
- **TCP reachability**: probes `<MASTER_IP>:8088` when the peer reports an active Master listener.

### 3. Send coordination tasks and record root-cause incidents

On the Mac (Terminal, repository root):

```bash
python3 scripts/agent_bridge.py send --priority high --title "Verify Windows clippy and GPU worker" --body "Pull latest master, run cargo clippy --workspace --all-targets -- -D warnings, and join <MASTER_IP>:8088."
python3 scripts/agent_bridge.py incident --id INC-001 --status OPEN --title "Worker registration timeout" --symptom "Worker retries every 5s" --root-cause "Firewall blocked TCP 8088" --fix "Allow inbound TCP 8088"
```

On Windows (PowerShell, repository root):

```powershell
python scripts/agent_bridge.py inbox
python scripts/agent_bridge.py inbox --ack all
```

### 4. Watch for peer messages and git updates automatically

Run `watch --once-on-message --auto-pull` in the background so the agent refreshes its heartbeat every 10 seconds, pulls new peer commits when the working tree is clean, and wakes up immediately when a new message or incident arrives:

On the Mac (Terminal, repository root):

```bash
python3 scripts/agent_bridge.py watch --interval 10 --once-on-message --auto-pull
```

On Windows (PowerShell, repository root):

```powershell
python scripts/agent_bridge.py watch --interval 10 --once-on-message --auto-pull
```

## Verify

On either machine (repository root):

```bash
python3 scripts/agent_bridge.py diagnose
```

Expected output when both nodes are synchronized:

```text
  - Git Commit    : MATCH (<COMMIT_SHA> on branch master)
  - Secret Hash   : MATCH (sha256[:12] = <HASH_PREFIX>)
------------------------------------------------------------------------------
  STATUS: HEALTHY (No configuration, git drift, or auth conflicts detected)
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `[GIT DRIFT] Commit mismatch` | One machine has not pulled the latest commit | Run `git pull` and rebuild release binaries on the behind node |
| `[AUTH MISMATCH] cluster.secret SHA-256 fingerprint differs` | `~/.oxideswarm/cluster.secret` differs between Master and Worker | Run `python3 scripts/agent_bridge.py serve-secret` on Master and `python scripts/agent_bridge.py pull-secret` on Worker over LAN TCP (never via Google Drive) |
| `[LOCAL CRITICAL] .../rusty-grid.toml contains an active p2p_ticket` | A leftover `p2p_ticket` in `rusty-grid.toml` overrides `--master` | Remove or comment out `p2p_ticket` in `rusty-grid.toml` |
| `[NETWORK BLOCKED] Peer Master is listening on <MASTER_IP>:8088, but TCP connection failed` | Host firewall or Wi-Fi client isolation blocks TCP `8088` | Allow inbound TCP `8088` on the Master host or start the Master with `--p2p` |
