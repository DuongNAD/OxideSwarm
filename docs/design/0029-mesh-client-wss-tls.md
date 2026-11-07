# 0029: Native Rust Agent Mesh Client `wss://` TLS Support (`MESH-1`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Enables TLS (`wss://`) in the Rust `agent_mesh` WebSocket client (`AgentMeshClient` and `agent-mesh`
CLI) by unifying workspace `tokio-tungstenite` on `0.24` with `rustls-tls-native-roots` and `tokio-rustls`
(`ring` provider), and adds `--tls-ca <PATH>` (`OXIDE_MESH_TLS_CA`) plus
`AgentMeshClient::with_tls_ca_pem` / `AgentMeshClient::with_tls_ca_file` so nodes can verify a
TLS-terminating proxy signed by either a system root CA or a private LAN CA.

## Context

Previously (`MESH-1` in `docs/BACKLOG.md`):
- Root `Cargo.toml` declared `tokio-tungstenite = { version = "0.21" }` with no TLS feature enabled,
  while `axum 0.7` pulled in `tokio-tungstenite 0.24.0`.
- Although `README.md`, `DEPLOYMENT.md`, and `crates/agent_mesh/src/main.rs` advised operators to put
  `agent-mesh hub` behind a `wss://` TLS proxy on untrusted networks so `OXIDE_MESH_TOKEN` would not
  travel in cleartext, calling `AgentMeshClient::connect` with a `wss://` URL failed immediately with
  `Url(TlsFeatureNotEnabled)`.

## Decision

1. **Workspace TLS dependency unification (`Cargo.toml` & `crates/agent_mesh/Cargo.toml`):**
   - Upgrade workspace `tokio-tungstenite` from `0.21` to `0.24` with
     `features = ["rustls-tls-native-roots", "url"]`, eliminating the duplicate `0.21` package from
     `Cargo.lock`.
   - Use `tokio-rustls` (`0.26`, `features = ["ring", "tls12", "logging"]`) and `rustls-native-certs`
     (`0.8`) already present in `Cargo.lock`.
2. **Certificate verification & custom CA support (`crates/agent_mesh/src/client.rs` & `main.rs`):**
   - For `ws://` URLs, `AgentMeshClient::connect` continues to open plain WebSocket connections.
   - For `wss://` URLs, `AgentMeshClient::build_tls_connector` constructs a `rustls::ClientConfig`
     backed by the `ring` crypto provider, loads OS native root certificates via
     `rustls_native_certs::load_native_certs()`, and appends any custom root CA certificates configured
     via `with_tls_ca_file` (`--tls-ca` / `OXIDE_MESH_TLS_CA`) or `with_tls_ca_pem` (accepting PEM or
     DER). Invalid custom CA files fail closed before connecting.
   - All client subcommands of `agent-mesh` (`node`, `send`, `list`, `data`) accept
     `--tls-ca <PATH>` (`OXIDE_MESH_TLS_CA`).
