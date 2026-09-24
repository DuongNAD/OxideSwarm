#!/usr/bin/env bash
# ==============================================================================
# run_agent_node.sh — OxideSwarm Agent Node Runner for Ubuntu / Debian Linux
#
# Supports: Ubuntu 20.04, 22.04, 24.04 LTS (x86_64 & aarch64)
# Features: Outbound WebSocket mesh connection, auto-restart, systemd integration
# ==============================================================================

set -euo pipefail

HUB_URL="${1:-ws://127.0.0.1:8088/ws}"
DEFAULT_NODE_ID="node-ubuntu-$(hostname -s | tr '[:upper:]' '[:lower:]')"
NODE_ID="${2:-$DEFAULT_NODE_ID}"

echo "========================================================"
echo "   OxideSwarm Ubuntu / Linux Agent Node Runner          "
echo "========================================================"
echo "Hub URL:  $HUB_URL"
echo "Node ID:  $NODE_ID"
echo "Platform: Ubuntu / Linux ($(uname -m))"
echo ""

# Find binary
BINARY=""
if [[ -f "./target/release/agent-mesh" ]]; then
    BINARY="./target/release/agent-mesh"
elif [[ -f "./target/debug/agent-mesh" ]]; then
    BINARY="./target/debug/agent-mesh"
fi

while true; do
    if [[ -n "$BINARY" && -x "$BINARY" ]]; then
        echo "[INFO] Starting native agent: $BINARY node --hub $HUB_URL --id $NODE_ID --platform ubuntu"
        "$BINARY" node --hub "$HUB_URL" --id "$NODE_ID" --platform ubuntu || true
    else
        echo "[INFO] Native binary not found. Launching Python agent node fallback..."
        PYTHON_BIN="python3"
        if ! command -v python3 >/dev/null 2>&1; then
            PYTHON_BIN="python"
        fi
        "$PYTHON_BIN" scripts/agent_node.py --hub "$HUB_URL" --id "$NODE_ID" --platform ubuntu || true
    fi

    echo "[WARN] Agent stopped or disconnected. Reconnecting in 3 seconds..."
    sleep 3
done
