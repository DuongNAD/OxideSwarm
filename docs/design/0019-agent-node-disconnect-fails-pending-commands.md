# 0019: Fail pending Python Agent Mesh commands on socket disconnect

Status: Implemented
Created: 2026-11-04    Updated: 2026-11-04

## Summary

When the WebSocket connection in `OxideAgentNode.listen()` (`scripts/agent_node.py:293-312`) closes for any reason, resolve every remaining unfinished future in `self.pending_commands` and `self.pending_acks` with `ConnectionError("WebSocket connection closed")` inside a `finally` block so callers awaiting `send_command()` fail fast instead of waiting for their full command timeout.

## Context

- `OxideAgentNode.send_command()` (`scripts/agent_node.py`) registers two `asyncio.Future` entries per outbound command: one in `self.pending_acks[corr_id]` and one in `self.pending_commands[corr_id]`, then awaits the response future under `asyncio.wait_for(..., timeout=timeout)`.
- Before `TEST-7` (`39f925c`), `OxideAgentNode._fail_pending()` resolved pending futures only when the hub explicitly returned a registration `DeliveryNack`. If the WebSocket closed abruptly mid-command (for example when the hub rejected an oversize frame or dropped the connection without a registration NACK), `OxideAgentNode.listen()` exited its `async for raw_msg in self.ws:` loop while leaving `self.pending_commands` and `self.pending_acks` unresolved until each caller's `asyncio.wait_for` timeout expired.

## Goals

- **G1** Fail all unresolved `pending_commands` and `pending_acks` futures immediately with `ConnectionError("WebSocket connection closed")` whenever `OxideAgentNode.listen()` exits.
- **G2** Preserve `_fail_pending(msg)` for explicit hub `DeliveryNack` frames received before the socket closes (`fut.done()` is checked before calling `fut.set_exception(...)`).

## Non-goals

- Changing the Rust `agent_mesh` client or hub wire protocol.
- Automatic command replay across WebSocket reconnections.

## Design

### D1 `finally` cleanup in `OxideAgentNode.listen()`

In `scripts/agent_node.py:293-312`, `OxideAgentNode.listen()` wraps `async for raw_msg in self.ws:` in a `try ... finally:` block:
1. If the hub sends a `DeliveryNack` with a registration error code before closing the socket, `_dispatch_incoming(msg)` still runs inline and calls `_fail_pending(msg)`, setting a result on all pending futures.
2. When the `async for` loop terminates (normal close or `websockets.exceptions.ConnectionClosed`), the `finally` block pops every remaining correlation ID from `self.pending_commands` and `self.pending_acks` and, if `not fut.done()`, calls `fut.set_exception(ConnectionError("WebSocket connection closed"))`.

## Compatibility

- Wire format and hub behaviour are unchanged.
- Callers of `OxideAgentNode.send_command()` that previously waited until `asyncio.TimeoutError` when the hub closed the WebSocket mid-flight now receive `ConnectionError("WebSocket connection closed")` immediately on disconnect.

## Security

- Prevents callers and test harnesses from stalling up to the per-command timeout when the hub disconnects a misbehaving or unauthenticated peer (e.g., on an oversized WebSocket frame).

## Test plan

- `tests/test_agent_mesh_adversarial_stress.py` (`test_oversize_frame_disconnect`): verifies that sending a frame exceeding the hub's size limit causes the hub to close the socket and the Python node to fail the in-flight command immediately with `(ConnectionError, asyncio.TimeoutError)`.
- `tests/test_agent_mesh_token_auth.py` and `tests/test_agent_mesh_rust_hub_cross_interop.py`.

## Work items

| ID | Item | Files | Done when | Status |
|---|---|---|---|---|
| W1 | Fail pending futures on WebSocket close | `scripts/agent_node.py` | Unfinished `pending_commands` and `pending_acks` futures raise `ConnectionError` on `listen()` exit | Implemented |
| W2 | Adversarial stress and interop verification | `tests/test_agent_mesh_adversarial_stress.py`, `tests/test_agent_mesh_rust_hub_cross_interop.py` | Interop and adversarial stress suites pass against the Rust `agent-mesh` hub | Implemented |

## Status log

- 2026-11-04: Implemented in `scripts/agent_node.py:294-312` (`39f925c`, `TEST-7`) and documented in design record `0019`.
