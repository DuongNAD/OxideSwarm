# Run a self-hosted iroh relay

## Goal

Run your own `iroh-relay` server on a host with a public IP address, a DNS name, and TLS, and point `oxideswarm master`, `oxideswarm worker`, and CLI client commands at it with `--relay-url` ([design 0049](../design/0049-custom-relay.md)).

## Prerequisites

- **A public server** with a public IPv4 (and optionally IPv6) address reachable from every machine in the cluster.
- **A DNS name** (for example `<RELAY_HOST>`) with `A` / `AAAA` records pointing to that server's public IP address ([iroh hole punching documentation](https://docs.iroh.computer/concepts/holepunching)).
- **TLS certificates** for `<RELAY_HOST>`, either provisioned automatically by `iroh-relay` via Let's Encrypt or supplied as PEM files (`fullchain.pem` and `privkey.pem`). OxideSwarm requires `https://` for `--relay-url` and refuses plaintext `http://`.
- **Firewall rules** on the relay server allowing inbound:
  - `TCP 443` (HTTPS relay streams and WebSocket connections)
  - `TCP 80` (HTTP endpoint and Let's Encrypt HTTP-01 challenge when `cert_mode = "LetsEncrypt"` is used)
  - `UDP 7842` (QUIC address discovery when `enable_quic_addr_discovery = true`)
- **The Rust toolchain** on the relay server to install the upstream `iroh-relay` binary from crates.io, matching the `iroh-relay` `1.4.1` crate in `Cargo.lock`.

## Steps

### 1. Install `iroh-relay` on the relay server

On the relay server:

```bash
cargo install iroh-relay --version 1.4.1 --features="server" --locked
```

This builds and installs the `iroh-relay` binary with its `server` feature enabled (`iroh-relay` crate `README.md`).

### 2. Write the `iroh-relay` configuration file

`iroh-relay` reads a TOML configuration file passed with `--config-path <PATH>` (`-c <PATH>`). Create `/etc/iroh-relay/relay.toml` on the relay server using either manual TLS certificates or Let's Encrypt.

On the relay server (manual TLS certificate files):

```bash
sudo mkdir -p /etc/iroh-relay
sudo tee /etc/iroh-relay/relay.toml > /dev/null <<'EOF'
enable_relay = true
http_bind_addr = "[::]:80"
enable_quic_addr_discovery = true

[tls]
https_bind_addr = "[::]:443"
quic_bind_addr = "[::]:7842"
cert_mode = "Manual"
manual_cert_path = "/etc/letsencrypt/live/<RELAY_HOST>/fullchain.pem"
manual_key_path = "/etc/letsencrypt/live/<RELAY_HOST>/privkey.pem"
EOF
```

Alternatively, on the relay server (automatic Let's Encrypt certificates via `iroh-relay`):

```bash
sudo mkdir -p /etc/iroh-relay
sudo tee /etc/iroh-relay/relay.toml > /dev/null <<'EOF'
enable_relay = true
http_bind_addr = "[::]:80"
enable_quic_addr_discovery = true

[tls]
https_bind_addr = "[::]:443"
quic_bind_addr = "[::]:7842"
cert_mode = "LetsEncrypt"
hostname = "<RELAY_HOST>"
contact = "<ADMIN_EMAIL>"
prod_tls = true
EOF
```

### 3. Start `iroh-relay`

On the relay server:

```bash
iroh-relay --config-path /etc/iroh-relay/relay.toml
```

Do not use `iroh-relay --dev`: development mode serves plaintext HTTP on port `3340` without TLS, and OxideSwarm refuses `http://` relay URLs.

### 4. Start the master with `--relay-url`

Pass `--relay-url https://<RELAY_HOST>`, set `RUSTY_GRID_RELAY_URL=https://<RELAY_HOST>`, or set `relay_url = "https://<RELAY_HOST>"` under `[master]` in `~/.oxideswarm/rusty-grid.toml` ([README CLI reference](../../README.md#oxideswarm-master-flags)).

On the master machine:

```bash
oxideswarm master --p2p --relay-url https://<RELAY_HOST> --p2p-ticket-file ~/.oxideswarm/master_ticket.txt
```

When `--relay-url` is set, the master binds its P2P endpoint with `iroh::RelayMode::Custom` pointing at `https://<RELAY_HOST>` and writes that relay URL into `~/.oxideswarm/master_ticket.txt`.

### 5. Connect workers and clients with `--relay-url`

Set the same relay URL on workers and clients via `--relay-url`, `RUSTY_GRID_RELAY_URL`, or `~/.oxideswarm/rusty-grid.toml` (`[worker].relay_url` and `[client].relay_url`).

On the worker machine:

```bash
oxideswarm worker --p2p-ticket <TICKET> --relay-url https://<RELAY_HOST>
```

Or save the relay URL once in `~/.oxideswarm/rusty-grid.toml` on each machine:

On any cluster machine:

```bash
cat >> ~/.oxideswarm/rusty-grid.toml <<'EOF'
[master]
relay_url = "https://<RELAY_HOST>"

[worker]
relay_url = "https://<RELAY_HOST>"

[client]
relay_url = "https://<RELAY_HOST>"
EOF
```

## Verify

Run `oxideswarm doctor` from the master or a client machine against the master:

On the master or client machine:

```bash
oxideswarm doctor --master <TICKET> --relay-url https://<RELAY_HOST>
```

Expected output:

- Check `C1 (master reachable)` and `C2 (worker count)` pass (`[OK]`).
- The worker table shows each P2P worker with its `LINK` column (`direct (<RTT>ms)` when QUIC hole punching succeeded via the relay, or `relay (<RTT>ms)` when traffic stays relayed through `https://<RELAY_HOST>`).
- Check `C8 (throughput)` transfers an 8 MiB blob to each worker and reports MB/s.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Invalid --relay-url: scheme must be 'https'` | The URL starts with `http://` (for example `iroh-relay --dev` on port `3340`) | Configure TLS on `iroh-relay` (`cert_mode = "Manual"` or `"LetsEncrypt"`) and pass an `https://` URL |
| `Invalid --relay-url: URL must not contain user:password credentials` | The URL includes `user@...` or `user:pass@...` | Remove credentials from the URL; authenticate nodes with the OxideSwarm cluster secret (`~/.oxideswarm/cluster.secret`) |
| `--relay-url / RUSTY_GRID_RELAY_URL cannot be combined with minimal/no-relay P2P preset (RUSTY_GRID_P2P_MINIMAL)` | Both `RUSTY_GRID_P2P_MINIMAL` and `--relay-url` / `RUSTY_GRID_RELAY_URL` are set | Unset `RUSTY_GRID_P2P_MINIMAL` when using a custom relay |
| `Master P2P endpoint did not finish initial relay/network online check within 5s` | The relay server is unreachable on `TCP 443` or its TLS certificate is invalid for `<RELAY_HOST>` | Verify DNS resolution for `<RELAY_HOST>`, check firewall rules for `TCP 443` and `UDP 7842`, and confirm the TLS certificate chain matches `<RELAY_HOST>` |
| `doctor` check `C7` warns that a worker link is `relay` instead of `direct` | UDP hole punching between the worker and master was blocked by NAT or firewall, or `enable_quic_addr_discovery` is disabled on the relay | Set `enable_quic_addr_discovery = true` in `relay.toml`, open `UDP 7842` on the relay server, and allow outbound UDP on worker and master firewalls |
