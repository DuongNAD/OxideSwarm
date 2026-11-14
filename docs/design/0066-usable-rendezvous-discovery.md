# 0066: Usable rendezvous discovery

Status: Implemented
Created: 2026-11-14    Updated: 2026-11-14

## Summary

Adds `oxideswarm rendezvous` as a standalone multi-cluster HTTP rendezvous registry on `:8090`,
separates keys per cluster with deterministic Ed25519 lease slots and read-authorization headers,
includes `p2p_ticket` under the beacon and lease MACs, and fails closed on `https://` rendezvous
URLs. Workers and CLI clients fall back from unreachable discovered TCP addresses to the
verified `p2p_ticket`.

## Context

Previously (`FEAT-7` in `docs/BACKLOG.md`):

- Tier 2 HTTP discovery queried the master's dashboard (`:8080`), which binds `127.0.0.1` by
  default.
- Tier 3 HTTP rendezvous stored a single unkeyed `Option<RendezvousLease>` slot in
  `crates/master/src/web_ui.rs` (`WebUiState::rendezvous_registry`), and no CLI command existed to
  host a standalone registry.
- `p2p_ticket` was excluded from `RendezvousLease::mac_input` and dropped by
  `RendezvousLease::to_beacon` (`crates/core/src/discovery.rs`).
- `parse_http_url` stripped `https://` and dialed plain TCP instead of rejecting `https://`.
- No end-to-end test exercised a worker registering with `--master auto --rendezvous <URL>` when
  Tier 1 and Tier 2 were unreachable.

## Goals

- **G1**: Provide a dedicated `oxideswarm rendezvous` subcommand (`--listen 127.0.0.1:8090`,
  `--port-file`, `--max-leases`, `--max-ttl-secs`, `--allow-unsigned`, `--insecure`).
- **G2**: Isolate multiple independent clusters on one shared rendezvous server without giving the
  rendezvous operator access to any cluster's `ClusterSecret`.
- **G3**: Protect `p2p_ticket` end to end under both the shared-secret HMAC and the slot Ed25519
  signature, preserve `p2p_ticket` in `MasterBeacon`, and fall back to P2P when discovered TCP is
  unreachable.
- **G4**: Refuse `https://` rendezvous URLs up front on both master and worker startup and in URL
  parsing.

## Non-goals

- Implementing native TLS termination in the standalone rendezvous server; operators who want HTTPS
  terminate TLS in a reverse proxy in front of `http://127.0.0.1:8090`.
- Rewriting the single-slot `/api/rendezvous/master` route on an individual master's dashboard beyond
  verifying signatures, bounding TTL/metadata, and honoring `X-OxideSwarm-Lease-Auth`.

## Design

### D1 Keyed multi-cluster leases and anti-harvesting reads

- `derive_rendezvous_key(secret)` (`crates/core/src/discovery.rs`) derives a deterministic Ed25519
  keypair seed via `secret.mac(&[RENDEZVOUS_ED25519_SEED_LABEL])` and encodes the 32-byte public
  key as a 64-char hex `lease_key`.
- `RendezvousLease::sign` sets `lease_key`, computes the HMAC `signature`, and signs the same
  length-prefixed `mac_input()` (including `lease_key` and `p2p_ticket`) with the derived Ed25519
  key into `key_signature`.
- `KeyedRendezvousStore` (`crates/master/src/rendezvous.rs`) verifies `key_signature` using the
  lease's own `lease_key` (or `verify(secret)` when the registry was explicitly pinned with
  `--secret` or `--secret-file`), keeping the registry operator blind to cluster secrets.
- `GET /api/rendezvous/master?key=<lease_key>` requires a fresh `X-OxideSwarm-Lease-Auth:
  <unix_ts>:<ed25519_sig_hex>` header signed over the slot key and timestamp, preventing passive
  enumeration of cluster addresses and P2P tickets.

### D2 Ticket authentication and P2P fallback

- `MasterBeacon::mac_input` and `RendezvousLease::mac_input` include `p2p_ticket` whenever present,
  and `RendezvousLease::to_beacon` copies `p2p_ticket` into the returned `MasterBeacon`.
- On an individual master dashboard (`crates/master/src/web_ui.rs`), `api_rendezvous_master`
  returns the full lease to requests carrying an `Observer+` Bearer token or a valid
  `X-OxideSwarm-Lease-Auth` header; unauthenticated dashboard readers still receive a lease with
  `p2p_ticket = None` and a refreshed MAC (`SEC-7` compatibility).
- `WorkerClient::establish_tcp` (`crates/worker/src/client.rs`) and `open_raw_master_stream`
  (`crates/cli/src/connect.rs`) dial the discovered `cluster_addr` over TCP first and fall back to
  the verified `p2p_ticket` via iroh QUIC if TCP fails or times out.

### D3 Fail-closed HTTP URL parsing and startup retries

- `parse_rendezvous_url` (`crates/core/src/discovery.rs`) rejects `https://` with an explicit error
  recommending `http://HOST:PORT` or a local TLS reverse proxy, and rejects whitespace, control
  characters, queries, fragments, userinfo, and port `0`.
- `crates/cli/src/cli_app.rs` (`run_master_inner`, `run_worker`) and `crates/cli/src/connect.rs`
  (`resolve_and_validate_rendezvous_url`) validate `--rendezvous` / `OXIDESWARM_RENDEZVOUS` /
  `RUSTY_GRID_RENDEZVOUS` before opening sockets.
- `spawn_master_rendezvous_registration` (`crates/master/src/server.rs`) retries every `2 s` after
  failed registrations and renews every `30 s` on success.

## Compatibility

- `MasterBeacon` and `RendezvousLease` serialize with JSON (`serde(default)` on `lease_key`,
  `p2p_ticket`, and `key_signature`), keeping wire compatibility on discovery endpoints. No binary
  wire protocol messages changed.

## Security

- **Fail-closed standalone registry**: `allow_unsigned` defaults to `false`; `--allow-unsigned` on
  a non-loopback `--listen` address is refused unless `--insecure` is passed.
- **No accidental secret pinning**: `oxideswarm rendezvous` never reads `~/.oxideswarm/cluster.secret`
  unless `--secret` or `--secret-file` is passed explicitly.
- **Bounded memory and bodies**: `DefaultBodyLimit::max(16 KiB)`, `max_leases = 256`, `max_ttl_secs
  = 3600`, clock-skew bound `30 s`, and `security_headers_middleware` on every response.
- **Cleartext transport explicitly rejected for `https://`**: clients refuse `https://` rather than
  downgrading to plain HTTP.

## Test plan

- Unit tests in `crates/core/src/discovery.rs` and `crates/master/src/rendezvous.rs` covering ticket
  tampering, Ed25519 read auth, `parse_rendezvous_url` (`https://` and header injection rejection),
  slot isolation, and capacity/TTL bounds.
- Integration suite `tests/rendezvous_discovery_e2e.rs`:
  1. Subprocess test with standalone `oxideswarm rendezvous`, `master`, `worker --master
     auto:<silent_udp> --rendezvous <url>`, and `submit --master auto:<silent_udp> --wait` with
     Tiers 1 and 2 disabled.
  2. Multi-cluster keyed isolation, anti-harvesting `401` on unauthenticated keyed read, unsigned
     rejection, and `p2p_ticket` tamper rejection.
  3. Immediate CLI exit failure on `https://` for `master`/`worker` and non-loopback
     `--allow-unsigned` without `--insecure`.
- Regression suites: `discovery_fallback_e2e`, `dashboard_security_test`, `m5c_challenger_2_stress`,
  and `test_mcp_concurrent_and_deadlines` (`test_mcp1_rendezvous`).

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| FEAT-7 | Keyed rendezvous registry, MAC'd tickets, P2P fallback, `oxideswarm rendezvous` CLI, `https://` rejection, E2E suite | `crates/core/src/discovery.rs`, `crates/master/src/rendezvous.rs`, `crates/master/src/lib.rs`, `crates/master/src/web_ui.rs`, `crates/master/src/server.rs`, `crates/worker/src/client.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/rendezvous_cmd.rs`, `crates/cli/src/lib.rs`, `crates/cli/src/cli_app.rs`, `crates/cli/Cargo.toml`, `tests/rendezvous_discovery_e2e.rs` | `rendezvous_discovery_e2e`, `discovery_fallback_e2e`, `dashboard_security_test`, and `m5c_challenger_2_stress` pass | Implemented |

## Status log

- 2026-11-14: Implemented `FEAT-7` across `core`, `master`, `worker`, and `cli`, documented in
  `README.md`, `CLAUDE.md`, `CHANGELOG.md`, and `docs/BACKLOG.md`.
