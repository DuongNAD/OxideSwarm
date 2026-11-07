# 0029: Enable `wss://` TLS in the Rust `agent-mesh` client (`MESH-1`)

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

Enable `wss://` TLS connections in the Rust `agent_mesh` WebSocket client (`AgentMeshClient` and the `agent-mesh` CLI) by unifying workspace `tokio-tungstenite` on `0.24` with `rustls-tls-native-roots` and `tokio-rustls` (`ring` provider):
- `AgentMeshClient` verifies `wss://` server certificates against the operating system's native root store plus any custom CA certificate configured via `with_tls_ca_file` (`--tls-ca` / `OXIDE_MESH_TLS_CA`) or `with_tls_ca_pem`;
- `parse_certificates_pem_or_der` strips an optional UTF-8 BOM and parses PEM certificates via `CertificateDer::pem_slice_iter` (handling `openssl x509 -text`, `openssl pkcs12` `Bag Attributes`, and multi-certificate bundles) or raw DER;
- `agent-mesh node`, `send`, `list`, and `data` accept `--tls-ca <PATH>` (`OXIDE_MESH_TLS_CA`), treat an empty `OXIDE_MESH_TLS_CA=""` as unset, validate `--tls-ca` upfront before entering the daemon loop, and warn if `--tls-ca` is passed with a plain `ws://` URL.

## Context

Behaviour before this change (`MESH-1` in `docs/BACKLOG.md`):

| Area | Behaviour | Code |
|---|---|---|
| Workspace dependency | Root `Cargo.toml` declared `tokio-tungstenite = { version = "0.21" }` with no TLS feature enabled, while `axum 0.7` already pulled in `tokio-tungstenite 0.24.0`. | `Cargo.toml`, `crates/agent_mesh/Cargo.toml` |
| `wss://` connections | `AgentMeshClient::connect` called `tokio_tungstenite::connect_async(&self.hub_url)` without a TLS connector; any `wss://` URL failed immediately with `Url(TlsFeatureNotEnabled)` even though `README.md`, `DEPLOYMENT.md`, and `crates/agent_mesh/src/main.rs:351` advised operators to run `agent-mesh hub` behind a `wss://` TLS proxy on untrusted networks. | `AgentMeshClient::connect` (`crates/agent_mesh/src/client.rs`) |

## Goals

- **G1** Enable `wss://` connections in `AgentMeshClient` and all client subcommands of `agent-mesh` (`node`, `send`, `list`, `data`) while eliminating the duplicate `tokio-tungstenite 0.21` crate from `Cargo.lock`.
- **G2** Support private TLS-terminating proxies via `--tls-ca <PATH>` (`OXIDE_MESH_TLS_CA`), `AgentMeshClient::with_tls_ca_file`, and `AgentMeshClient::with_tls_ca_pem`, loading PEM (including `openssl x509 -text`, `openssl pkcs12` `Bag Attributes`, UTF-8 BOM, CRLF, and multi-certificate bundles) or DER.
- **G3** Fail closed at startup when a configured `--tls-ca` file is missing or malformed (including in `agent-mesh node` and on `ws://` URLs), and treat `OXIDE_MESH_TLS_CA=""` as unset.

## Non-goals

- Adding an embedded TLS listener inside `agent-mesh hub` itself (TLS termination remains in front of the hub via a reverse proxy such as Caddy, nginx, or HAProxy).
- Supporting mutual TLS (mTLS) client certificate authentication.

## Design

### D1 Unify `tokio-tungstenite 0.24` with `rustls-tls-native-roots` and `tokio-rustls`

Root `Cargo.toml` upgrades `tokio-tungstenite` from `0.21` to `0.24` with `features = ["rustls-tls-native-roots", "url"]`. `crates/agent_mesh/Cargo.toml` depends on workspace `tokio-tungstenite`, `tokio-rustls` (`ring` provider), and `rustls-native-certs`.

### D2 TLS connector construction and PEM/DER certificate loading in `crates/agent_mesh/src/client.rs`

1. `parse_certificates_pem_or_der(raw: &[u8])`:
   - strips an optional UTF-8 BOM (`\xEF\xBB\xBF`);
   - if the input contains `-----BEGIN`, iterates `CertificateDer::pem_slice_iter` (`rustls_pki_types::pem::PemObject`) to extract every `-----BEGIN CERTIFICATE-----` block, skipping any `openssl x509 -text` or `openssl pkcs12` `Bag Attributes` preamble;
   - otherwise returns the raw bytes as a single `CertificateDer` so raw DER files continue to load.
2. `AgentMeshClient::build_tls_connector(&self)` (and `validate_tls_config(&self)`):
   - reads and validates `self.tls_ca_file` and `self.tls_ca_bytes` first against a `RootCertStore` so unreadable or invalid CA inputs fail immediately even when `hub_url` uses `ws://`;
   - when `hub_url` does not start with `wss://`, logs a warning if a custom CA was configured and returns `Ok(None)`;
   - when `hub_url` starts with `wss://`, loads OS native root certificates via `rustls_native_certs::load_native_certs()`, appends all custom CA certificates, and builds a `rustls::ClientConfig` with `ring::default_provider()` and `with_no_client_auth()`, wrapped in `Connector::Rustls`.

### D3 CLI `--tls-ca` / `OXIDE_MESH_TLS_CA` handling in `crates/agent_mesh/src/main.rs`

- `Commands::Node`, `Commands::Send`, `Commands::List`, and `Commands::Data` accept `--tls-ca <PATH>` bound to `OXIDE_MESH_TLS_CA` using `parse_tls_ca_arg` and `client_tls_ca` so `OXIDE_MESH_TLS_CA=""` parses cleanly and resolves to `None`.
- `Commands::Node` calls `client.validate_tls_config()?` before `client.run_daemon().await` (and `run_daemon` also checks `validate_tls_config()` before its loop) so `agent-mesh node --tls-ca missing.pem` exits `1` immediately instead of retrying forever.

## Compatibility

- Plain `ws://` connections and hub wire envelopes are unchanged.
- `OXIDE_MESH_TLS_CA=""` behaves identically to an unset `OXIDE_MESH_TLS_CA`, matching `OXIDE_MESH_TOKEN=""`.

## Security

- **Trusted roots:** For `wss://` connections, `AgentMeshClient` trusts the union of the OS native root store (`rustls_native_certs::load_native_certs()`) and any custom CA certificates supplied via `--tls-ca` / `OXIDE_MESH_TLS_CA` / `with_tls_ca_file` / `with_tls_ca_pem`.
- **`SSL_CERT_FILE` and `SSL_CERT_DIR`:** `rustls-native-certs` honours the standard OpenSSL environment variables `SSL_CERT_FILE` and `SSL_CERT_DIR`; when set, they replace the OS native root certificate store.
- **Server name verification:** `rustls` enforces strict RFC 6125 / RFC 5280 `subjectAltName` verification (DNS name or IP address) against the host in `hub_url`; a certificate without a matching SAN is rejected with `NotValidForName`, and an untrusted issuer is rejected with `UnknownIssuer`.
- **2-tier PKI requirement (`CaUsedAsEndEntity`):** `rustls-webpki` enforces basic constraints strictly: a single self-signed certificate generated with `openssl req -x509` (`basicConstraints = CA:TRUE`) used directly as the proxy's server certificate is refused with `CaUsedAsEndEntity` even when passed to `--tls-ca`. Private TLS proxies must use a 2-tier PKI where a `CA:TRUE` root certificate signs a `CA:FALSE` server leaf certificate carrying a `subjectAltName` (`DNS:...` or `IP:...`).

## Test plan

- **Unit (`crates/agent_mesh/src/client.rs`):** `test_parse_certificates_pem_variants_bom_text_pkcs12_der_and_bundle` verifies loading `openssl x509 -text` output, `openssl pkcs12` `Bag Attributes` output, UTF-8 BOM + CRLF PEM, raw DER, and a 2-certificate PEM bundle.
- **Unit (`crates/agent_mesh/src/main.rs`):**
  - `client_commands_accept_tls_ca_flag_and_empty_env_value` verifies `--tls-ca`, `OXIDE_MESH_TLS_CA=""` handling, and that `node`, `send`, `list`, and `data` all bind `--tls-ca` to `OXIDE_MESH_TLS_CA`;
  - `build_client_forwards_tls_ca_and_rejects_missing_ca_file` verifies that `build_client` forwards `tls_ca` and rejects a missing CA file on both `wss://` and `ws://`.
- **Integration (`crates/agent_mesh/src/client.rs` & `crates/agent_mesh/tests/grid_master_integration.rs`):**
  - `test_client_wss_connect_via_tls_terminating_proxy` verifies `UnknownIssuer` on untrusted CA, failure on a missing CA file, `NotValidForName` when dialling `localhost` against a leaf for `other.example`, and end-to-end `wss://127.0.0.1:<port>/ws` registration and command routing with a 2-certificate PEM bundle where the signing CA is second;
  - `test_agent_mesh_cli_tls_ca_missing_and_empty_env` verifies that `agent-mesh node` exits non-zero at startup when `--tls-ca` or `OXIDE_MESH_TLS_CA` points to a missing file, and that `agent-mesh list` succeeds when `OXIDE_MESH_TLS_CA=""`.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p agent_mesh --locked`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Unify `tokio-tungstenite 0.24` with `rustls-tls-native-roots` and `tokio-rustls` | `Cargo.toml`, `crates/agent_mesh/Cargo.toml`, `Cargo.lock` | Only `tokio-tungstenite 0.24.0` remains in `Cargo.lock` and `wss://` connections are enabled | Implemented |
| W2 | Implement `build_tls_connector`, `validate_tls_config`, and `CertificateDer::pem_slice_iter` parsing | `crates/agent_mesh/src/client.rs` | `UnknownIssuer`, `NotValidForName`, missing CA file, BOM/text/pkcs12 PEM, and multi-cert bundles are verified by tests | Implemented |
| W3 | Wire `--tls-ca` / `OXIDE_MESH_TLS_CA` across CLI subcommands with empty-env handling and startup validation | `crates/agent_mesh/src/main.rs`, `crates/agent_mesh/tests/grid_master_integration.rs` | `OXIDE_MESH_TLS_CA=""` is treated as unset and `agent-mesh node --tls-ca missing.pem` exits non-zero at startup | Implemented |
| W4 | Document `--tls-ca`, `OXIDE_MESH_TLS_CA`, and the 2-tier PKI requirement | `docs/design/0029-mesh-client-wss-tls.md`, `docs/README.md`, `README.md`, `DEPLOYMENT.md` | Design 0029 follows the template, is indexed in `docs/README.md`, and documents `CaUsedAsEndEntity` | Implemented |

## Status log

- 2026-11-04: Implemented `MESH-1` in `crates/agent_mesh/src/client.rs` and `crates/agent_mesh/src/main.rs`, added mutation-resistant TLS and CLI tests, and documented the change in design record 0029.
