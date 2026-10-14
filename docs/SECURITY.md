# OxideSwarm security model

This page says what OxideSwarm protects, what it deliberately does not, which port exposes what, and how to
handle the shared cluster secret. It describes the code as it is in this repository; anything planned is
marked "not implemented".

## 1. In one paragraph

A master hands arbitrary commands (shell scripts, programs, compile jobs) to workers, and workers run them
with the rights of the account they were started under. Whoever can talk to the master's cluster port as a
valid peer can therefore run code on every worker, and whoever can impersonate the master can run code on
every worker that follows it. OxideSwarm closes both doors with a **shared cluster secret**: a client or
worker must prove it holds the secret before the master accepts anything from it, and the master must prove
it holds the secret before a worker or client obeys or trusts it. The secret **authenticates**; it does
**not encrypt**. Over plain TCP the traffic after the handshake is readable (and hijackable) by anyone on the
path. Use the P2P/QUIC transport (`--p2p`, encrypted by iroh) or a VPN / SSH tunnel on any network you do not
control.

## 2. Threat model

| Considered | How it is handled |
| :--- | :--- |
| Another host on the same LAN / Wi-Fi sends `SubmitTask` to the cluster port | Refused: the handshake runs before the first message and a peer without the secret gets a generic `authentication failed/required` reply. |
| A rogue host answers `--master auto` (UDP beacon) or sits at the master's address | The worker/CLI verifies the master's proof of the secret. Without a secret they refuse to connect to a remote, auto-discovered or P2P-ticket master unless `--insecure` is given. |
| Someone learns a P2P ticket (it is an address, not a credential) | The same handshake runs inside the QUIC stream; the ticket alone is not enough. |
| A web page in your browser calls `http://127.0.0.1:8080` (CSRF / DNS rebinding) | The dashboard is same-origin only (no permissive CORS), every route needs a token, and the dev bypass checks the peer, the `Host` header, `X-Forwarded-*` and `Origin`. |
| A leaked dashboard / agent-mesh token | Rotate it (section 5). Dashboard tokens derived from the cluster secret change when the secret changes. |

| Not considered / not provided | Consequence |
| :--- | :--- |
| Confidentiality and integrity of plain TCP after the handshake | Anyone on the path can read task payloads and results, and could hijack an authenticated connection. Use `--p2p`, a VPN or an SSH tunnel on untrusted networks. |
| A malicious or compromised cluster member (anyone holding the secret) | Members are mutually trusted: a holder can submit tasks to all workers. There is no per-user permission model on the cluster port. |
| Sandboxing of tasks | A task runs as the worker's OS account. The "sandbox" is a per-task working directory, not a container. A task can read everything that account can read, **including the worker's cluster secret file** (and, on Linux, the worker's environment through `/proc`). The secret variables (`OXIDESWARM_SECRET`, `OXIDESWARM_SECRET_FILE`, `OXIDE_MESH_TOKEN`) are not *passed on* to task processes, but use a secret *file* anyway, and see section 7. |
| Denial of service | A peer that cannot authenticate is cut off after a 5 s handshake timeout and a 250 ms penalty delay, but there is no rate limiting or quota. |
| Replay across machines of recorded TCP sessions | Each handshake uses fresh 32-byte nonces from both sides, so a recorded handshake cannot be replayed; the session after it is not protected (see above). |

## 3. The cluster secret

* A random value of at least 16 bytes; the generated one is 32 bytes, stored as one line of lowercase hex
  (64 characters). It is never logged or printed except by `oxideswarm secret show`.
* Default location `~/.oxideswarm/cluster.secret` (`%USERPROFILE%\.oxideswarm\cluster.secret` on Windows),
  created with mode `600`.
* **A master that is reachable beyond its own machine (non-loopback `--listen`, or `--p2p`) creates this file
  on its first start** and prints where it is and how to copy it. A master bound to loopback only (the
  default `127.0.0.1:8088`) needs no secret. `oxideswarm master --insecure` starts an exposed master without
  one and prints a warning on every start; do not use it outside an isolated test network.
* Resolution order on every machine (first hit wins; an unreadable or malformed source is an error, never a
  silent fall-back to something weaker):
  `--secret <hex>` (visible in the process list; avoid) > `--secret-file PATH` > `OXIDESWARM_SECRET` >
  `OXIDESWARM_SECRET_FILE` > the `secret` / `secret_file` key of the config file > the default file.

```bash
oxideswarm secret generate            # new random secret in ~/.oxideswarm/cluster.secret (refuses to overwrite)
oxideswarm secret generate --force    # replace it: every other machine must then get the new file
oxideswarm secret generate --out ./cluster.secret
oxideswarm secret show                # print the secret this machine would use
oxideswarm secret show --dashboard-token   # print the derived dashboard login token (section 6)
```

**Moving the secret to another machine** (workers, other clients) needs a channel you trust. Copy the *file*:

```bash
scp user@master:~/.oxideswarm/cluster.secret ~/.oxideswarm/cluster.secret   # Mac / Linux / Termux (pkg install openssh)
chmod 600 ~/.oxideswarm/cluster.secret
```

On Windows use `scp` from the built-in OpenSSH client or carry the file on a USB stick to
`%USERPROFILE%\.oxideswarm\cluster.secret`. Do not paste it into chat, e-mail or a cloud-synced folder
(`scripts/sync_network` writes into one, so never put the secret there). Showing it as a QR code is not
implemented.

## 4. What each port exposes

| Port | Where it listens | What it exposes | Protection |
| :--- | :--- | :--- | :--- |
| **8088/tcp** (cluster) | `127.0.0.1` by default; `0.0.0.0` only with `--listen 0.0.0.0:8088` (what `switch_role.sh master` and `windows\start_master.cmd` pass) | Worker registration, task submission, status. This is the port that runs code on your workers. | Cluster-secret handshake; **not encrypted**. |
| **8080/tcp** (dashboard) | Loopback unless `--web-ui-addr` says otherwise (the launchers pass `--web-ui-addr 127.0.0.1:8080`, or `0.0.0.0:8080` only when you ask for phone / LAN access) | Web UI, REST (`/api/*`), WebSocket `/ws`, SSE. Can submit and cancel tasks. | Bearer token / `X-API-Key` / `?token=` (section 6); same-origin only. |
| **8080/tcp** on a *worker* | All interfaces (`0.0.0.0`); the CLI has no switch to turn it off. If the port is taken, e.g. by a master on the same machine, the portal just does not start. | A tiny "standby" page and `/api/status` that say which master this worker follows and redirect browsers to it. | None. It only reveals the master's address. |
| **8089/udp** | Master broadcasts / answers on the LAN | Discovery beacon (`--master auto`): the master's cluster address and, only if the dashboard listens on a non-loopback address, its URL (empty otherwise). | With a secret the beacon carries a MAC and workers/clients ignore beacons without a valid one. Not encrypted. |
| iroh QUIC (UDP, random port) + HTTPS to the public n0 relay servers | Outbound from master and workers when `--p2p` is on | The P2P transport used for machines on different networks. The master identity key is `~/.oxideswarm/master_key.bin`. | TLS from iroh **plus** the cluster-secret handshake inside the stream. Needs outbound Internet. |
| **8085/tcp** | `scripts/sync_network` only | An HTTP `/ping` reachability probe. Not part of the grid. | None. Stop it when you are done. |
| agent-mesh hub (**127.0.0.1:8088** by default) | Loopback unless `--listen` says otherwise | WebSocket relay that lets any registered node run commands on any other node (`shell_exec`). | Shared hub token (`--token` / `OXIDE_MESH_TOKEN`). A non-loopback `--listen` without a token is refused unless `--insecure`. `ws://` is not encrypted. |

The agent-mesh hub and the grid master both default to port 8088. Run them on the same machine only with
different ports (for example `agent-mesh hub --listen 127.0.0.1:8090`).

Open the cluster port on your firewall only for the networks that should reach it, and never forward 8088
(or 8080) from the Internet. For machines on other networks use `--p2p`, which needs no inbound port.

## 5. Rotation and revocation

* **Cluster secret.** On the master: `oxideswarm secret generate --force`, copy the new file to every worker
  and client, restart the master and the workers (a running process keeps the secret it started with; there is
  no hot reload). From that moment the old secret is useless and so is the dashboard token derived from it.
  Do this whenever a machine that had the secret is lost, sold or suspected of compromise.
* **P2P identity.** The ticket of a P2P master is stable because `~/.oxideswarm/master_key.bin` persists. To
  invalidate every ticket ever shown, delete that file (or start with `--ephemeral-key`) and restart; the next
  start issues a new identity and ticket. The cluster secret is still what actually protects the master.
* **agent-mesh hub token.** Restart the hub with a new `--token` / `OXIDE_MESH_TOKEN` and restart every node.
* **`--auth-token TOKEN:NAME:SCOPES`** tokens passed to `master` live only in that process; restart without
  the flag to drop them.

## 6. The dashboard token

`oxideswarm secret show --dashboard-token` prints a 64-hex-character **Admin** token derived from the cluster
secret (`HMAC-SHA256(secret, "oxs1-dashboard-admin")`). The dashboard shows a login prompt on any `401`; paste
the token there (kept in `sessionStorage`, sent as `Authorization: Bearer`). The same token works for
`curl -H "Authorization: Bearer <token>" http://HOST:8080/api/status`.

Extra tokens with a narrower scope can be registered when starting the master:
`--auth-token <token>:<name>:<scopes>` where scopes are a comma-separated list of `observer`, `worker`,
`submitter`, `admin` (for example `--auth-token phone123:phone:observer` for a read-only phone view).

Listener and bypass policy:

1. **A bare loopback master** (`oxideswarm master`) serves its dashboard on `http://127.0.0.1:8080` for the
   local user; if it asks for a token anyway, use the command above or start with `--dev`.
2. **The dashboard only listens on a non-loopback interface when `--web-ui-addr` says so explicitly**
   (for example `--web-ui-addr 0.0.0.0:8080` or `switch_role.sh master --dashboard-lan`). Binding the cluster port
   to `0.0.0.0` does not publish the dashboard.
3. **The dev bypass (`--dev`, or `OXIDE_DEV_MODE=1|true`) never applies when the dashboard listener is bound to
   a non-loopback address.** Even on loopback it needs all of: a loopback peer, a loopback `Host` header, no
   `X-Forwarded-*` header and an absent or loopback `Origin`.

The dashboard is served over plain HTTP: its token crosses the network in clear text. Use it on the LAN you
trust, through a VPN / SSH tunnel (`ssh -L 8080:127.0.0.1:8080 master`), or keep it on loopback.

## 7. Who runs the tasks

Tasks inherit the identity (and the environment) of the worker process. The shipped installers therefore do
not use the most powerful account:

| Installer | Runs the worker as | Override |
| :--- | :--- | :--- |
| `packaging/macos/install_mac_daemon.sh` (LaunchDaemon) | The user who ran `sudo` (or `--user NAME`); asks when it cannot tell. The plist has a `UserName` key. | `--run-as-root` (prints a warning) |
| `packaging/windows/install_windows_service.ps1` | The virtual service account `NT SERVICE\OxideSwarmWorker`, no password. | `-ServiceAccount LocalService`, or `-ServiceAccount LocalSystem -AcceptSystemRisk` |
| Termux (`start_worker.sh`) / Android app | The app or Termux user (an unprivileged Android UID) | none |
| `switch_role.sh`, `windows\*.cmd` | The user who runs them | n/a |

A dedicated, otherwise empty account is better still. Whatever the account can read or write, every task
can read or write. Do not run a worker on a machine whose files you would not hand to every member of the
cluster, and do not use your everyday account on a PC with credentials you care about unless you trust every
holder of the secret.

What the worker does limit, so that tasks stay out of the owner's way:

| Resource | Behaviour |
| :--- | :--- |
| CPU priority | Task processes run at below-normal priority (`nice 10`; `BELOW_NORMAL_PRIORITY_CLASS` on Windows). `--task-priority normal\|below-normal\|idle` or `RUSTY_GRID_TASK_PRIORITY` changes it. |
| Concurrency | Without `--max-concurrency` or `--cores`, a worker runs `cores - 1` tasks at once (at least 1). `--cores N` offers N; `--max-concurrency N` sets the limit directly. |
| Wall-clock time | The task's `timeout_secs` (default 60 s). |
| Process tree | Each task leads its own process group (Windows: Job Object). Timeout, cancellation, the end of the task and a stopped worker (Ctrl-C, SIGTERM, console close) kill the whole tree, grandchildren and background helpers included. A worker that is *killed* (SIGKILL, power loss) cannot do this on Unix: its task processes keep running until they finish. On Windows the Job Object ends them with the worker. |
| Memory, disk | **Not limited.** A task can use all the RAM and fill the disk of the account it runs as. |
| Sandbox directory | `--sandbox-base-dir`, else `RUSTY_GRID_SANDBOX_BASE_DIR`, else `$TMPDIR/rusty_grid/sandboxes`, else the platform temp dir. The worker refuses to start, with the fix in the message, when that directory cannot be written (on Android pass the app's cache dir). |

## 8. Files that contain secrets

| File | Contains | Mode |
| :--- | :--- | :--- |
| `~/.oxideswarm/cluster.secret` | the cluster secret | 600 |
| `~/.oxideswarm/master_key.bin` | the P2P node identity of a master | should be 600, but it is currently created with the process umask (usually 644): run `chmod 600 ~/.oxideswarm/master_key.bin` |
| `~/.oxideswarm/remote-worker.toml` (`connect_remote.sh`) | a P2P ticket and the path of the secret file | 600 |
| `/etc/oxideswarm/worker.toml`, `/etc/oxideswarm/cluster.secret` (macOS daemon) | ticket / master address, secret copy | 600, owned by the service account |
| `C:\ProgramData\OxideSwarm\rusty-grid.toml`, `cluster.secret` (Windows service) | ticket / master address, secret copy | ACL: SYSTEM, Administrators, the service account |
| `~/.oxideswarm/worker_id.<name>` (or the file named by `RUSTY_GRID_WORKER_ID_FILE`) | the id a named worker keeps across restarts; a worker without a name or id file gets a new id at each start. It is an identity, not a credential: the cluster secret is what admits a worker. Two workers with the same id displace each other at the master, so give each worker that shares a home directory its own `--name` | 600, in a 700 directory |
| `~/.oxideswarm/run/*.log` | worker / master logs (no secrets, but the master log shows addresses) | 700 directory |

Tickets and secrets are never put on a command line by the shipped launchers, in a `LaunchDaemon` plist or in
the Windows registry. If you pass `--secret` or `--p2p-ticket` yourself, they are visible to every local user
in the process list.

## 9. Reporting a problem

Open an issue without a working secret, token, ticket or address in it. If you accidentally published one,
rotate it (section 5) before anything else.
