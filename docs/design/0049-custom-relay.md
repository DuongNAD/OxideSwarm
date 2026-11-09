# 0049: Custom iroh relay URL (`--relay-url`)

Status: Implemented
Created: 2026-11-09    Updated: 2026-11-09

## Summary

Master, worker, and client P2P endpoints accept one custom relay URL option across all roles: CLI flag `--relay-url <URL>`, environment variable `RUSTY_GRID_RELAY_URL`, and `rusty-grid.toml` key `relay_url` in `[master]`, `[worker]`, and `[client]` (plus `[submit]` and `[status]` or top-level `relay_url`). When set, the endpoint uses `iroh::endpoint::presets::Minimal` with `iroh::RelayMode::Custom(iroh::RelayMap::from(relay_url))` instead of `presets::N0`. Combining `--relay-url` with the minimal test preset (`RUSTY_GRID_P2P_MINIMAL`) or passing a non-`https://`, credential-carrying, or malformed URL fails before any network activity (`LINK-1`, steps 3 and 4).

## Context

Before this change, every P2P endpoint bound either `iroh::endpoint::presets::N0` (the public n0 relays and pkarr/DNS lookup) or `iroh::endpoint::presets::Minimal` (no relay and no DNS lookup, selected by `RUSTY_GRID_P2P_MINIMAL` in `crates/core/src/transport.rs`, `crates/master/src/server.rs`, `crates/worker/src/client.rs`, and `crates/cli/src/connect.rs`). The public n0 relays are rate-limited and intended for development, so a cluster that outgrows them or runs in an isolated environment had no way to point its endpoints at an owner-operated `iroh-relay` server (`docs/BACKLOG.md`, item `LINK-1`).

## Goals

- **G1** One option name across master, worker, and client commands: `--relay-url <URL>`, `RUSTY_GRID_RELAY_URL`, and `relay_url` in `rusty-grid.toml`, following `CLI flag > Env > Config > Default` precedence.
- **G2** When a relay URL is configured, construct the iroh `Endpoint::builder` from `iroh::endpoint::presets::Minimal` with `.relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::from(relay_url)))` so traffic uses only the owner's relay and never contacts n0's public relays or public pkarr/DNS servers.
- **G3** Refuse `--relay-url` combined with the minimal/no-relay test mode (`RUSTY_GRID_P2P_MINIMAL` / `p2p_minimal_preset`) before binding a socket.
- **G4** Validate the URL before any network activity: `https` scheme only, no `user:password` credentials (without echoing credentials in the error message), non-empty host, no query or fragment, and parseable as `iroh::RelayUrl`.
- **G5** Leave default behaviour unchanged when the option is not set.

## Non-goals

- Embedding a relay server inside `oxideswarm`. Operators run the upstream `iroh-relay` binary (`docs/guides/self-hosted-relay.md`).
- Supporting multiple custom relay URLs or plaintext `http://` relays (`iroh-relay --dev`).

## Design

### D1 Configuration and precedence

Each role resolves its relay URL in `crates/cli/src/cli_app.rs` (`resolve_relay_url_from`):

1. `--relay-url <URL>` on `oxideswarm master`, `worker`, `submit`, `status`, `workers`, `workspaces`, `result`, `cancel`, `mapreduce`, and `doctor`.
2. `RUSTY_GRID_RELAY_URL` (`crates/core/src/transport.rs`, `RELAY_URL_ENV`).
3. `rusty-grid.toml` (`crates/cli/src/config.rs`):
   - Master: `[master].relay_url`, falling back to top-level `relay_url`.
   - Worker: `[worker].relay_url`, falling back to top-level `relay_url`.
   - Submit / MapReduce: `[submit].relay_url`, falling back to `[client].relay_url`, then top-level `relay_url`.
   - Status / Workers / Workspaces / Result / Cancel / Doctor: `[status].relay_url`, falling back to `[client].relay_url`, then `[submit].relay_url`, then top-level `relay_url`.
4. Default: `None` (`presets::N0`, or `presets::Minimal` when `RUSTY_GRID_P2P_MINIMAL` is enabled).

An empty or whitespace-only `--relay-url` or `RUSTY_GRID_RELAY_URL` is kept as `Some("")` so validation rejects it instead of silently falling back to a lower tier.

### D2 URL validation and custom `RelayMap`

`validate_relay_url` in `crates/core/src/transport.rs` parses the string with `iroh::RelayUrl::from_str` and checks:

- The input is non-empty and contains no whitespace.
- The scheme is `https` (case-insensitive); `http://` and other schemes are refused.
- `url.username()` is empty and `url.password()` is `None`. If an `@` appears in the authority before parsing fails, the refusal message never echoes the raw string.
- `url.host_str()` is present and non-empty.
- `url.query()` and `url.fragment()` are `None`.

`build_custom_relay_map` wraps the validated `RelayUrl` in `iroh::RelayMap::from(relay_url)`. `resolve_p2p_relay_mode(minimal_preset, relay_url)` returns an error when both `minimal_preset` and `relay_url` are set, and `p2p_endpoint_builder(minimal_preset, relay_url)` builds the `iroh::endpoint::Builder`:

- `(false, None)` -> `Endpoint::builder(presets::N0)`
- `(true, None)` -> `Endpoint::builder(presets::Minimal)`
- `(false, Some(url))` -> `Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Custom(RelayMap::from(url)))`
- `(true, Some(_))` -> error before binding any socket

### D3 Master, worker, and client plumbing

- **Master (`crates/master/src/server.rs`):** `ServerConfig` gains `p2p_relay_url: Option<String>` and `.with_p2p_relay_url(...)` / `.with_relay_url(...)`. At the start of `MasterServer::bind_full` (under `#[cfg(feature = "p2p")]`), `resolve_p2p_relay_mode` validates `config.p2p_minimal_preset` and `config.p2p_relay_url` before opening the TCP listener or spawning background tasks, and the P2P setup block calls `p2p_endpoint_builder`.
- **Worker (`crates/worker/src/client.rs`):** `WorkerConfig` gains `p2p_relay_url: Option<String>` and `.with_p2p_relay_url(...)` / `.with_relay_url(...)`. `WorkerClient::run` calls `resolve_p2p_relay_mode` before the connection loop, and `WorkerClient::establish_p2p` calls `bind_p2p_endpoint_with_relay`.
- **Client (`crates/cli/src/connect.rs`):** `connect_master_with_relay` validates `resolve_p2p_relay_mode(client_p2p_minimal_requested(), relay_url)` before any TCP or P2P dial, and passes the relay URL to `connect_p2p_client_stream_with_relay`. `with_client_relay_url` scopes the resolved URL across multi-step client commands (`submit`, `mapreduce`, `doctor`).

## Compatibility

No wire protocol or message layout changes. Nodes that do not pass `--relay-url` continue using `presets::N0`. All nodes that communicate through a custom relay must be able to reach that relay's HTTPS and QUIC ports, and the master's generated P2P ticket embeds the custom relay URL as the master's home relay.

## Security

- Only `https://` relay URLs are accepted; plaintext `http://` is refused so relay traffic always uses TLS and QUIC encryption in addition to OxideSwarm's own Ed25519/QUIC and HMAC-SHA256 cluster authentication (`docs/SECURITY.md`).
- URLs carrying `user:password@` credentials are refused and redacted from error messages so secrets cannot leak into logs or terminal output.
- The relay forwards encrypted QUIC datagrams and streams; it does not hold the cluster secret and cannot pass the `cluster_auth` handshake.

## Test plan

- Unit tests in `crates/core/src/transport.rs`:
  - `test_validate_relay_url_accepts_https_and_builds_custom_relay_map`
  - `test_validate_relay_url_refuses_http_credentials_and_garbage`
  - `test_resolve_p2p_relay_mode_refuses_minimal_plus_custom_relay_url`
- Unit test in `crates/master/src/server.rs`:
  - `test_master_bind_refuses_invalid_relay_url_and_minimal_conflict`
- Unit tests in `crates/cli/src/config.rs`, `crates/cli/src/connect.rs`, and `crates/cli/src/cli_app.rs`:
  - `test_relay_url_parses_across_master_worker_client_sections`
  - `test_connect_master_refuses_invalid_relay_url_and_minimal_conflict_before_network`
  - `test_relay_url_flag_parses_and_precedence_holds_across_roles`
  - `test_run_master_worker_and_client_refuse_invalid_relay_url_before_network`

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| LINK-1.3 | Custom relay URL option and validation across master, worker, and client | `crates/core/src/transport.rs`, `crates/master/src/server.rs`, `crates/worker/src/client.rs`, `crates/cli/src/config.rs`, `crates/cli/src/connect.rs`, `crates/cli/src/cli_app.rs`, `README.md` | Unit tests pass; `README.md` flag check passes | Implemented |
| LINK-1.4 | Self-hosted relay guide | `docs/guides/self-hosted-relay.md`, `docs/README.md`, `docs/BACKLOG.md` | Guide and backlog entries landed | Implemented |

## Status log

- 2026-11-09: Implemented `LINK-1` steps 3 and 4 (`--relay-url`, `RUSTY_GRID_RELAY_URL`, `relay_url`, URL validation, custom `RelayMap`, design record, and self-hosted relay guide).
