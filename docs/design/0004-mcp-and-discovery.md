# 0004: Model Context Protocol server and subnet-isolated discovery fallback

Status: Implemented
Created: 2026-10-23    Updated: 2026-10-23

## Summary

Integrate AI agent tooling and cross-subnet coordinator discovery:

- run a Model Context Protocol (MCP) server over JSON-RPC 2.0 stdio (`oxideswarm mcp`) compliant with specification `2024-11-05`;
- expose five core tools: `status`, `workers`, `submit`, `task_result`, and `doctor`;
- enforce strict stdout hygiene by redirecting all tracing and logging exclusively to `stderr`;
- provide authenticated HTTP discovery probing (`GET /api/discovery/probe?nonce=<hex>`) with HMAC-SHA256 nonce verification;
- coordinate across isolated subnets via a signed rendezvous registry (`OXIDESWARM_RENDEZVOUS` / `--rendezvous`).

## Context

Behaviour before this change:

| Area | Behaviour | Code |
|---|---|---|
| AI Tooling | AI coding agents (such as Claude Code) had to execute CLI commands (`submit`, `doctor`, `status`) by invoking child processes and parsing human-readable text or terminal tables. No standard MCP stdio protocol server existed. | `crates/cli/src/cli_app.rs` |
| Stdout Output | CLI commands printed diagnostic logs and tracing output to standard output (`stdout`), which corrupts JSON-RPC 2.0 message framing if parsed by an MCP client. | `crates/cli/src/main.rs` |
| UDP Discovery | Nodes located on different IP subnets or behind Wi-Fi access points with client isolation could not discover the master node because routers drop UDP broadcast (`:8089`). | `crates/core/src/discovery.rs` |
| HTTP Fallback | The pre-existing HTTP fallback (`/api/status`) was unauthenticated and was disabled when a cluster secret was present, leaving secret-protected clusters with no fallback if UDP was blocked. | `crates/core/src/discovery.rs` |

## Goals

- **G1** Run an MCP server over standard input and output conforming to specification version `2024-11-05`.
- **G2** Expose five cluster orchestration tools (`status`, `workers`, `submit`, `task_result`, `doctor`) with JSON schemas and structured outputs.
- **G3** Guarantee that all non-JSON tracing, logging, and diagnostics write strictly to standard error (`stderr`), keeping `stdout` reserved for protocol frames.
- **G4** Authenticate cross-subnet discovery over HTTP using fresh random nonces and HMAC-SHA256 signatures derived from the shared cluster secret.
- **G5** Register and query master lease records from a centralized rendezvous registry service when candidate hosts are unknown.

## Non-goals

- Implementing MCP resource or prompt templates.
- Operating an MCP stdio server without a valid cluster secret against non-loopback master nodes (fail-closed security).
- Hosting public multi-tenant rendezvous registries on the internet without shared secret authentication.

## Design

### D1 MCP server over stdio

The command `oxideswarm mcp` starts the MCP server over standard input and output:

On the agent host:
```bash
oxideswarm mcp --master 127.0.0.1:8088
```

Supported lifecycle endpoints:
- `initialize`: Returns protocol version (`2024-11-05`), server capabilities (`tools: {}`), and server info (`name: "oxideswarm-mcp"`, `version: "0.1.0"`).
- `notifications/initialized`: Confirms client initialization.
- `ping`: Liveness check responding with `{}`.
- `tools/list`: Returns JSON schemas for all five tools.
- `tools/call`: Executes the requested tool against the master cluster.

Connection resilience: `McpState` reuses `crates/cli/src/connect.rs::connect_master` with lazy connection caching and transparent reconnection if the socket drops.

Implemented in `crates/cli/src/mcp.rs`.

### D2 Five core tools

| Tool | Parameters | Functionality |
|---|---|---|
| `status` | `task_id` (optional string) | Returns overall cluster status, active task counts, and worker counts, or execution state of a specific task. |
| `workers` | `connected_only`, `os`, `arch`, `gpu_only` | Lists registered compute workers, hardware capabilities, and tags matching the given filters. |
| `submit` | `task_type`, `command`, `args`, `script`, `interpreter`, `stdin`, `env`, `os`, `arch`, `worker`, `require_tags`, `gpu`, `matrix_dim`, `staged_files`, `wait`, `timeout_secs` | Submits a computation task to the grid with optional file staging and placement constraints. |
| `task_result` | `task_id`, `wait`, `timeout_secs` | Retrieves execution outcome, exit code, stdout, stderr, execution duration, and output artifacts. |
| `doctor` | `expect_workers`, `probe_timeout_secs`, `gpu_dim`, `spread_tasks`, `skip_gpu`, `skip_spread` | Runs cluster diagnostic checks C1–C6 and returns complete health check results. |

Implemented in `crates/cli/src/mcp.rs`.

### D3 Strict standard output hygiene

MCP transports communicate through newline-delimited JSON-RPC frames on `stdout`. Any non-JSON text emitted on `stdout` breaks client framing parsers.

When `oxideswarm mcp` starts, `crates/cli/src/cli_app.rs` configures the logging subscriber:

```rust
tracing_subscriber::fmt()
    .with_writer(std::io::stderr)
    .init();
```

All log entries, informational notices, and internal error traces write strictly to `stderr`. Standard output carries only newline-terminated JSON-RPC 2.0 responses.

### D4 Authenticated HTTP discovery probe

To discover masters across subnets where UDP broadcast is blocked:
1. Client generates a fresh 16-byte random probe nonce (`DISCOVERY_PROBE_NONCE_LEN = 16`).
2. Client sends an HTTP request to candidate IPs (configured candidates, default gateway, and loopback):
   ```http
   GET /api/discovery/probe?nonce=<32-hex-chars> HTTP/1.1
   ```
3. The master validates the nonce format, signs its `MasterBeacon` with HMAC-SHA256:
   $$\text{HMAC-SHA256}(\text{cluster\_secret}, \text{"oxs1-beacon"} \parallel \text{nonce} \parallel \text{cluster\_addr} \parallel \text{web\_ui\_url})$$
4. Returns the signed `MasterBeacon` as JSON.
5. The client parses the response and calls `beacon.verify(&cluster_secret, &nonce)`.
6. Only beacons whose signature matches the fresh nonce are accepted, preventing replay attacks and forged responses.

Implemented in `crates/core/src/discovery.rs` (`probe_http_candidate_with_secret`) and `crates/master/src/web_ui.rs` (`api_discovery_probe`).

### D5 Rendezvous registry protocol (`OXIDESWARM_RENDEZVOUS`)

When master and workers reside on different subnets with no shared broadcast domain and unknown IP addresses:
1. **Master lease registration:** The master periodically posts its signed lease to the rendezvous URL:
   ```http
   POST /api/rendezvous/register HTTP/1.1
   Content-Type: application/json
   ```
   Carries `cluster_addr`, `web_ui_url`, `hostname`, `p2p_ticket`, and HMAC-SHA256 signature.
2. The registration runs every 30 seconds with a 120-second lease TTL.
3. Unspecified bind addresses (`0.0.0.0`) are sanitized to the host's actual local interface IP prior to registration.
4. **Worker / Client discovery:** Workers query the rendezvous service:
   ```http
   GET /api/rendezvous/master HTTP/1.1
   ```
   The returned `RendezvousLease` is verified against the local `cluster.secret`.

Implemented in `crates/core/src/discovery.rs` and `crates/master/src/server.rs`.

### D6 Three-tier discovery hierarchy

`discover_master_tiered` in `crates/core/src/discovery.rs` executes discovery in priority order:
- **Tier 1 (LAN broadcast):** Fast UDP broadcast with fresh nonce to port 8089 (sub-5 ms).
- **Tier 2 (HTTP probe):** Authenticated probe to candidate hosts and default gateway (`GET /api/discovery/probe?nonce=<hex>`).
- **Tier 3 (Rendezvous):** Query centralized rendezvous registry from `OXIDESWARM_RENDEZVOUS` / `--rendezvous`.

## Compatibility

- The MCP protocol conforms to Anthropic's `2024-11-05` standard and interoperates with standard MCP clients.
- Discovery probe endpoints (`/api/discovery/probe`, `/api/rendezvous/*`) are public endpoints that verify signatures internally, allowing discovery before Bearer tokens are issued.
- Mixed versions: masters without discovery probe support fall back to direct IP configuration without protocol errors.

## Security

- All discovery probes and rendezvous leases require HMAC-SHA256 validation using `cluster.secret`. Malicious actors on the network cannot impersonate the master.
- Fresh 16-byte nonces ensure that discovery probes are single-use and cannot be replayed.
- The MCP server strictly forbids unauthenticated connections to non-loopback addresses unless `--insecure` is explicitly passed.

## Test plan

- **Unit:** JSON-RPC parsing, tool schema validation, rendezvous lease HMAC signing/verification, and URL parsing (`crates/core`, `crates/cli`).
- **Integration:** MCP server stdio lifecycle, tool execution (`status`, `workers`, `submit`, `task_result`, `doctor`), and stdout hygiene verification asserting zero non-JSON frames on stdout (`tests/mcp_server_e2e.rs`).
- **Integration:** HTTP discovery probe verification, nonce replay rejection, rendezvous registration and lookup, and tiered discovery fallback (`tests/discovery_fallback_e2e.rs`).
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | MCP stdio protocol engine | `crates/cli/src/mcp.rs` | JSON-RPC 2.0 lifecycle and 5 tools implemented | Implemented |
| W2 | CLI MCP subcommand and stdout hygiene | `crates/cli/src/cli_app.rs` | `oxideswarm mcp` command added, tracing routed to `stderr` | Implemented |
| W3 | Authenticated HTTP discovery probe | `crates/core/src/discovery.rs`, `crates/master/src/web_ui.rs` | `/api/discovery/probe` handler and client probe with nonce HMAC | Implemented |
| W4 | Rendezvous registry service | `crates/core/src/discovery.rs`, `crates/master/src/web_ui.rs`, `crates/master/src/server.rs` | Signed lease registration and lookup with sanitised address | Implemented |
| W5 | End-to-end MCP verification suite | `tests/mcp_server_e2e.rs` | Stdio lifecycle and tool calls verified with clean stdout | Implemented |
| W6 | Discovery fallback integration suite | `tests/discovery_fallback_e2e.rs` | Probing, tampering detection, and tiered fallback pass | Implemented |

## Status log

- 2026-10-23: Created and accepted for Milestone M5C.
- 2026-10-23: Implemented W1–W6 on branch `feat/m5c-mcp-and-discovery`.
- 2026-10-23: Verified via `cargo test -p rusty_grid_cli --test mcp_server_e2e` and `discovery_fallback_e2e` (5 passing tests). Status marked Implemented.
