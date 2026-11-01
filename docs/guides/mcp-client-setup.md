# Connect an MCP coding client to OxideSwarm

## Goal

Claude Code, Codex, or Cursor connects to an OxideSwarm cluster over standard input and standard
output (`oxideswarm mcp`, implemented in `crates/cli/src/mcp.rs`), lists the cluster tools, and
queries cluster status.

## Prerequisites

- An OxideSwarm binary (`oxideswarm`) built from this repository (`cargo build --release --bin oxideswarm`).
- A running OxideSwarm master (`oxideswarm master` on loopback `127.0.0.1:8088`, or a remote master
  at `<MASTER_IP>:8088`).
- When connecting to a non-loopback or auto-discovered master: the cluster secret file copied to
  `~/.oxideswarm/cluster.secret` (`%USERPROFILE%\.oxideswarm\cluster.secret` on Windows) with mode
  `600` ([SECURITY.md](../SECURITY.md)). `run_mcp` (`crates/cli/src/mcp.rs`) refuses a remote or
  `auto` master without a secret and exits with code `6` before reading standard input.

## Steps

### 1. Locate the `oxideswarm` binary

On the client machine (macOS or Linux):

```bash
pwd
ls -l target/release/oxideswarm
```

On the client machine (Windows PowerShell):

```powershell
(Get-Item .\target\release\oxideswarm.exe).FullName.Replace('\', '/')
```

Use the full path to the binary as `<OXIDESWARM_BIN>` in the client configuration below. On Windows,
always use forward slashes (for example `C:/Users/alice/OxideSwarm/target/release/oxideswarm.exe`) or
doubled backslashes (`C:\\Users\\alice\\OxideSwarm\\target\\release\\oxideswarm.exe`) in TOML and
JSON configuration files; single backslashes (`C:\Users\...`) are invalid escape sequences in both
TOML and JSON. When the master runs on another machine, replace `127.0.0.1:8088` with
`<MASTER_IP>:8088` (or `auto` on the same LAN).

### 2. Configure Claude Code

On the client machine, register the stdio MCP server with `claude mcp add`:

```bash
claude mcp add --transport stdio oxideswarm -- <OXIDESWARM_BIN> mcp --master 127.0.0.1:8088
```

To scope the registration to the user profile across all repositories, pass `--scope user` before
`oxideswarm`.

### 3. Configure Codex

On the client machine, add an `[mcp_servers.oxideswarm]` section to `~/.codex/config.toml`:

```toml
[mcp_servers.oxideswarm]
command = "<OXIDESWARM_BIN>"
args = ["mcp", "--master", "127.0.0.1:8088"]
```

### 4. Configure Cursor

On the client machine, add an `oxideswarm` entry under `mcpServers` in `~/.cursor/mcp.json` (or
`.cursor/mcp.json` in the workspace):

```json
{
  "mcpServers": {
    "oxideswarm": {
      "command": "<OXIDESWARM_BIN>",
      "args": ["mcp", "--master", "127.0.0.1:8088"]
    }
  }
}
```

### 5. Optional: store the master address in `~/.oxideswarm/rusty-grid.toml`

`oxideswarm mcp` reads `master`, `secret`, `secret_file`, and `insecure` from the `[status]`
section of `~/.oxideswarm/rusty-grid.toml` (`crates/cli/src/mcp.rs`). It intentionally ignores
`./rusty-grid.toml` and `./rusty-grid.json` in the current working directory unless `--config` or
`RUSTY_GRID_CONFIG` names the file explicitly (`crates/cli/src/config.rs`).

On the client machine, in `~/.oxideswarm/rusty-grid.toml`:

```toml
[status]
master = "<MASTER_IP>:8088"
secret_file = "/Users/<USER>/.oxideswarm/cluster.secret"
```

When `~/.oxideswarm/rusty-grid.toml` sets `master`, the client configuration only needs `["mcp"]` in
`args`.

## Verify

On the client machine, send a JSON-RPC `initialize` request, `tools/list`, and a `tools/call` for
`status` to `oxideswarm mcp`:

```bash
printf '%s\n%s\n%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"verify","version":"1.0"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"status","arguments":{}}}' \
  | <OXIDESWARM_BIN> mcp --master 127.0.0.1:8088
```

Expected output on `stdout` when the master is running (three JSON-RPC `2.0` lines, with
`"cluster_status": "ok"` in response `id: 3`):

```text
{"jsonrpc":"2.0","id":1,"result":{"capabilities":{"tools":{}},"protocolVersion":"2024-11-05","serverInfo":{"name":"oxideswarm","version":"0.1.0"}}}
{"jsonrpc":"2.0","id":2,"result":{"tools":[{"description":"Query cluster status, worker counts, or execution state of a specific task ID.","inputSchema":...,"name":"status"},...]}}
{"jsonrpc":"2.0","id":3,"result":{"content":[{"text":"{\n  \"cluster_status\": \"ok\",\n  \"tasks\": ...,\n  \"workers_connected\": ...\n}","type":"text"}],"isError":false}}
```

If the master is stopped or unreachable, response `id: 3` reports `"isError": true`:

```text
{"jsonrpc":"2.0","id":3,"result":{"content":[{"text":"Failed to connect to Master at 127.0.0.1:8088: Connection error: ...","type":"text"}],"isError":true}}
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `oxideswarm mcp` exits immediately with code `6` (`Authentication required: refusing to connect to the master at <addr> without a cluster secret` or `Authentication required: refusing to connect to a master found by LAN auto-discovery without a cluster secret`) | `--master` points to a non-loopback address or `auto`, and no cluster secret was found | Copy `~/.oxideswarm/cluster.secret` from the master (`chmod 600 ~/.oxideswarm/cluster.secret`), or pass `--secret-file <PATH>` ([SECURITY.md](../SECURITY.md)). |
| Calling `status` or `submit` on loopback returns `Failed to receive response from Master: Unsupported wire format discriminator: 0x03` | The loopback master (`127.0.0.1:8088`) was started with a cluster secret, while `oxideswarm mcp` connected without one (so the master sent a `0x03` auth handshake frame) | Pass `--secret-file ~/.oxideswarm/cluster.secret` (or set `secret_file` under `[status]` in `~/.oxideswarm/rusty-grid.toml`) so `oxideswarm mcp` authenticates with the same secret. |
| `oxideswarm mcp` prints `Skipping config file ./rusty-grid.toml in the working directory` on `stderr` | A config file exists in the repository folder where the coding agent launched `oxideswarm mcp` | Move the `[status]` settings to `~/.oxideswarm/rusty-grid.toml`, or pass `--config <PATH>` explicitly. |
| Calling `status` or `submit` returns `Failed to connect to Master at ...` | The master is not running or a firewall blocks TCP port `8088` | Start the master (`oxideswarm master`) and verify reachability with `oxideswarm status --master <MASTER_IP>:8088`. |
| `oxideswarm mcp` exits with code `2` (`unexpected argument '--sse'`) | An old client configuration passes the removed `--sse` or `--sse-bind` flag | Use stdio transport (`oxideswarm mcp --master <MASTER_IP>:8088`). The HTTP/SSE MCP transport was removed (`SEC-1`). |
