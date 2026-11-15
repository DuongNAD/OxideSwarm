# 0071: Remove dashboard chat route (`/api/chat`)

Status: Implemented
Created: 2026-11-15    Updated: 2026-11-15

## Summary

Remove the `POST /api/chat` route, its `agy` child-process runner, its RBAC scope rule, and the dashboard chat UI (`SEC-14`, owner decision D9):

- `crates/master/src/web_ui.rs` drops `.route("/api/chat", post(api_chat))`, `api_chat`, `run_agy`, `resolve_agy_binary`, `ChatRequest`, `ChatResponse`, `AgyError`, `ProcessGroupGuard`, `sanitize_for_prompt`, `chat_slots`, and their constants and unit tests;
- `crates/master/src/auth.rs` removes `/api/chat` from the `AuthScope::Submitter` match arm in `route_required_scope`, so unknown `POST` paths fall through to `AuthScope::Admin` and return `404 Not Found` from the router;
- `crates/master/src/dashboard.html` removes the AI chat mode controls, preset button, and `/api/chat` fetch branch while keeping the shell task console (`POST /api/tasks`).

## Context

Behaviour before this change (at `c2462e3`):

| Area | Behaviour | Code |
|---|---|---|
| Dashboard router and handler | `create_web_ui_router_full` registered `.route("/api/chat", post(api_chat))`. `api_chat` formatted live worker telemetry and the caller's prompt, located an `agy` binary in `~/.local/bin`, `/usr/local/bin`, `/usr/bin`, or `PATH` via `resolve_agy_binary`, and spawned `agy -p <prompt>` on the master host via `run_agy`. | `create_web_ui_router_full`, `api_chat`, `run_agy`, `resolve_agy_binary` (`crates/master/src/web_ui.rs`) |
| Dashboard RBAC scope mapping | `auth::route_required_scope` (a free function in `mod middleware`, re-exported by `auth`) mapped `POST /api/chat` to `Some(AuthScope::Submitter)` alongside `POST /api/tasks`. | `auth::route_required_scope` (`crates/master/src/auth.rs`) |
| Embedded dashboard SPA | `crates/master/src/dashboard.html` defaulted the console input to AI chat mode (`setChatMode('ai')`, `quickAI`) and sent non-shell prompts to `POST /api/chat` inside `sendCmd`. | `setChatMode`, `quickAI`, `sendCmd` (`crates/master/src/dashboard.html`) |

Although `api_chat` bounded concurrency (`MAX_CONCURRENT_CHATS = 2`), sanitized worker telemetry lines, enforced a 60 s timeout (`CHAT_TIMEOUT`), and killed the Unix process group on drop, what `agy` may read, write, or execute on the master host depends on that external CLI's own configuration. Invoking developer agent tooling inside the master process does not belong in the production coordinator (`SEC-14`, owner decision D9).

## Goals

- **G1** Remove `POST /api/chat` and all `agy` process-spawning code from `rusty_grid_master`.
- **G2** Remove `/api/chat` from `auth::route_required_scope` (a free function in `mod middleware`, re-exported by `auth`) so unknown `POST` routes require `AuthScope::Admin` and answer `404 Not Found` once authenticated.
- **G3** Remove the AI chat panel controls and `/api/chat` client code from `crates/master/src/dashboard.html` while keeping dashboard shell task submission (`POST /api/tasks`), workflow modes, and device telemetry intact.

## Non-goals

- Changing `POST /api/tasks`, `/api/modes/*`, or any cluster wire message.
- Changing `--consumer agy` in `scripts/agent_bridge.py` and [guides/multi-agent-collaboration.md](../guides/multi-agent-collaboration.md), which is a mailbox consumer name for the cross-machine file bridge.

## Design

### D1 Remove `POST /api/chat` and `run_agy` from `crates/master/src/web_ui.rs`

Delete `.route("/api/chat", post(api_chat))` from `create_web_ui_router_full`, `chat_slots` from `WebUiState`, the constants `MAX_CONCURRENT_CHATS`, `CHAT_MAX_MESSAGE_CHARS`, and `CHAT_TIMEOUT`, the types `ChatRequest`, `ChatResponse`, `AgyError`, and `ProcessGroupGuard`, the functions `sanitize_for_prompt`, `run_agy`, `api_chat`, and `resolve_agy_binary`, and their unit tests.

### D2 Remove `/api/chat` from `crates/master/src/auth.rs`

In `auth::route_required_scope` (a free function in `mod middleware`, re-exported by `auth`), restrict the `AuthScope::Submitter` `POST` arm to `p.starts_with("/api/tasks")`. Any other `POST` path (except the public `/api/rendezvous/register`) falls through to `_ => Some(AuthScope::Admin)`, and the Axum router returns `404 Not Found` when no route matches.

### D3 Remove the chat UI from `crates/master/src/dashboard.html`

Rename the first tab from `Chat` (`#btnTabChat`, `#tabChat`, `#chatFeed`) to `Console` (`#btnTabConsole`, `#tabConsole`, `#consoleFeed`), remove the AI mode toggle chips (`#btnModeAI`, `#btnModeShell`), the AI prompt chip (`quickAI`), `setChatMode`, and the `/api/chat` branch in `sendCmd`, and send console commands directly to `POST /api/tasks`.

## Compatibility

- A dashboard user loses the chat panel; nothing on the wire changes (`PROTOCOL_VERSION_CURRENT = 8`, `PROTOCOL_VERSION_MIN_COMPATIBLE = 8`).
- Callers that send `POST /api/chat` receive `404 Not Found` when authenticated as `Admin` (or on a local dev-bypass dashboard), `403 Forbidden` with a `Submitter` or `Observer` token, and `401 Unauthorized` without credentials when authentication is enforced.

## Security

- Eliminates spawning the external `agy` binary on the master host from an HTTP dashboard endpoint (`SEC-14`).

## Test plan

- **Unit (`crates/master/src/auth.rs`):** `test_route_required_scope_covers_every_dashboard_route` (`mod tests::dashboard_gate`) verifies `POST /api/tasks` requires `AuthScope::Submitter` and unknown `POST` paths require `AuthScope::Admin`.
- **Unit (`crates/master/src/web_ui.rs`):** `test_dashboard_html_remediation_integrity` verifies the embedded `dashboard.html` keeps executor lookup, `Cancelled` state handling in `sendCmd`, and workflow mode controls.
- **Integration (`tests/dashboard_security_test.rs`):** `test_registered_token_reaches_every_route_class` verifies `POST /api/chat` with an `Admin` bearer token returns `404 Not Found`, and `test_a_bare_loopback_master_has_a_usable_local_dashboard` verifies `POST /api/chat` without a token on a local dashboard returns `404 Not Found`.
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo check -p rusty_grid_master --no-default-features --locked`, `cargo test -p rusty_grid_master --lib --locked`, `cargo test -p rusty_grid_master --locked --test dashboard_security_test --test dashboard_api_test --test dashboard_integration --test rbac_security_test`, and `python3 tests/test_m1_challenger_hygiene.py`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| SEC-14 | Remove `POST /api/chat`, `run_agy`, RBAC scope mapping, and dashboard chat UI | `crates/master/src/web_ui.rs`, `crates/master/src/auth.rs`, `crates/master/src/dashboard.html`, `tests/dashboard_security_test.rs`, `docs/design/0071-remove-dashboard-chat.md`, `docs/README.md`, `CHANGELOG.md` | `POST /api/chat` returns `404 Not Found` with an `Admin` token and without one on a local dashboard, `grep -rn "api/chat\|run_agy" crates tests` finds only the 404 assertions, and all dashboard/RBAC tests pass | Implemented |

## Status log

- 2026-11-15: Implemented `SEC-14` (owner decision D9 default) by removing `POST /api/chat`, `run_agy`, its RBAC rule, and the dashboard chat UI.
