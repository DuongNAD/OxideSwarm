#!/usr/bin/env bash
# ==============================================================================
# run_agent_node.sh — OxideSwarm Agent Node Runner for macOS
#
# Supports: macOS (Apple Silicon arm64 & Intel x86_64)
# Features: Outbound WebSocket connection, automatic reconnection, launchd setup
# ==============================================================================

set -euo pipefail

HUB_URL="${1:-ws://127.0.0.1:8088/ws}"
DEFAULT_NODE_ID="node-mac-$(hostname -s | tr '[:upper:]' '[:lower:]')"
NODE_ID="${2:-$DEFAULT_NODE_ID}"

echo "========================================================"
echo "   OxideSwarm macOS Agent Node Runner                   "
echo "========================================================"
echo "Hub URL:  $HUB_URL"
echo "Node ID:  $NODE_ID"
echo "Platform: macOS ($(uname -m))"
echo ""

# A hub that is reachable beyond this machine requires its token (OXIDE_MESH_TOKEN, or --token).
case "$HUB_URL" in
    ws://127.0.0.1:*|ws://localhost:*|ws://\[::1\]:*) ;;
    *)
        if [[ -z "${OXIDE_MESH_TOKEN:-}" ]]; then
            echo "[WARN] OXIDE_MESH_TOKEN is not set; a hub bound beyond localhost will reject this node (export OXIDE_MESH_TOKEN=<hub token>)."
        fi
        if [[ "$HUB_URL" == ws://* ]]; then
            echo "[WARN] ws:// is not encrypted: the token and every command cross the network in clear text."
            echo "       Use wss:// behind a TLS proxy, or a VPN / SSH tunnel; never expose the hub to the Internet."
        fi
        ;;
esac

# Find binary
BINARY=""
if [[ -f "./target/release/agent-mesh" ]]; then
    BINARY="./target/release/agent-mesh"
elif [[ -f "./target/debug/agent-mesh" ]]; then
    BINARY="./target/debug/agent-mesh"
fi

while true; do
    if [[ -n "$BINARY" && -x "$BINARY" ]]; then
        echo "[INFO] Starting native agent: $BINARY node --hub $HUB_URL --id $NODE_ID --platform macos"
        "$BINARY" node --hub "$HUB_URL" --id "$NODE_ID" --platform macos || true
    else
        echo "[INFO] Native binary not found. Launching Python agent node fallback..."
        PYTHON_BIN="python3"
        if ! command -v python3 >/dev/null 2>&1; then
            PYTHON_BIN="python"
        fi
        "$PYTHON_BIN" scripts/agent_node.py --hub "$HUB_URL" --id "$NODE_ID" --platform macos || true
    fi

    echo "[WARN] Agent stopped or disconnected. Reconnecting in 3 seconds..."
    sleep 3
done
