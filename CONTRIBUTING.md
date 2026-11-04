# Contributing to OxideSwarm

Thank you for contributing to OxideSwarm. Before making changes, read [`CLAUDE.md`](CLAUDE.md), [`docs/README.md`](docs/README.md), and [`docs/SECURITY.md`](docs/SECURITY.md).

## 1. Prerequisites

- **Rust toolchain**: Rust `1.91` or newer (`rustfmt`, `clippy`). CI pins `1.96.0` in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- **Python 3**: Required for repository hygiene, launcher, and Agent Mesh test suites (`pip install websockets` for the mesh interop suites).
- **`cargo-audit`**: Required to check `Cargo.lock` against the RustSec advisory database (`cargo install cargo-audit`).

## 2. Development & Architecture Rules

1. **Workspace layout**:
   - `crates/core` (`rusty_grid_core`): wire framing, codec, cluster authentication (`v2`), AEAD record layer, discovery, and shared types.
   - `crates/master` (`rusty_grid_master`): task scheduler, worker registry, blob store, and Axum web dashboard (`8080/tcp`).
   - `crates/worker` (`rusty_grid_worker`): task runner, sandbox management, CPU/GPU compute, 3D (`gen3d`), and auto-rigging (`autorig`).
   - `crates/cli` (`rusty_grid_cli`): `oxideswarm` / `rusty-grid` / `ox-mode` binaries and MCP stdio server (`oxideswarm mcp`).
   - `crates/android_bridge` (`rusty_grid_android_bridge`, library `oxideworker`): Android JNI bridge.
   - `crates/agent_mesh` (`agent_mesh`): WebSocket relay hub and agent client.
2. **Wire protocol compatibility (`CLAUDE.md`)**:
   - Any new variant or field in `TaskSpec`, `WorkerMessage`, `MasterMessage`, `ClientMessage`, or `ClientResponse` must be added to the public enum, both compatibility mirror enums (`Human*` and `Binary*`) in `crates/core/src/protocol.rs`, and all four `From` implementations.
   - When the wire layout changes, update the golden layout test (`crates/core/src/wire_layout_tests.rs` and `crates/core/tests/data/wire_layout.txt`); a breaking change needs a version bump (`PROTOCOL_VERSION_CURRENT` and `PROTOCOL_VERSION_MIN_COMPATIBLE`).
3. **Test isolation**:
   - Never read or write the real `~/.oxideswarm` (`%USERPROFILE%\.oxideswarm`) directory in tests. Always point child processes at a temporary directory via `Command::env("HOME", ...)` and `Command::env("USERPROFILE", ...)`.
4. **Line endings (`.gitattributes`)**:
   - `.sh` and `.py` files must use LF (`\n`) line endings.
   - `.cmd`, `.ps1`, `.vbs`, and `.bat` files must use CRLF (`\r\n`) line endings.
5. **Documentation standards (`docs/README.md`)**:
   - Document only what the code currently implements. Use the fixed status vocabulary (`Implemented`, `Partial`, `Planned`, `Design only`).
   - Never commit real LAN IP addresses, hostnames, serial numbers, user paths, or credentials. Use placeholders (`<MASTER_IP>`) or RFC 5737 documentation addresses (`192.0.2.x`, `198.51.100.x`, `203.0.113.x`).
   - Any change to behaviour, CLI flags, ports, environment variables, or defaults must update `README.md` and include a design record under `docs/design/` in the same pull request.

## 3. Pre-PR Verification Gates

Run all of the following checks before opening or updating a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p rusty_grid_master --no-default-features --locked
cargo test --workspace --lib --locked
cargo audit
python3 tests/test_m1_challenger_hygiene.py
python3 tests/test_launcher_scripts.py
python3 tests/test_agent_bridge.py
```

## 4. Reporting Security Issues

Do **not** open a public GitHub issue for security vulnerabilities. Follow the private reporting instructions in [`docs/SECURITY.md`](docs/SECURITY.md#9-reporting-a-problem).
