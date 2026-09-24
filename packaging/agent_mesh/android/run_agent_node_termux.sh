#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# run_agent_node_termux.sh — OxideSwarm Agent Node Runner for Android (Termux)
#
# Supports: Android 7.0+ (ARM64-v8a / aarch64) without root
# Features: Termux wake-lock integration, outbound WebSocket, auto-reconnect
# ==============================================================================

set -euo pipefail

HUB_URL="${1:-ws://192.168.1.100:8088/ws}"
DEFAULT_NODE_ID="node-android-termux"
NODE_ID="${2:-$DEFAULT_NODE_ID}"

echo "========================================================"
echo "   OxideSwarm Android (Termux) Agent Node Runner        "
echo "========================================================"
echo "Hub URL:  $HUB_URL"
echo "Node ID:  $NODE_ID"
echo "Platform: Android (Termux userspace, $(uname -m))"
echo ""

# 1. Acquire Termux wake lock to prevent Doze Mode CPU freeze
if command -v termux-wake-lock >/dev/null 2>&1; then
    echo "[INFO] Acquiring Termux Wake Lock..."
    termux-wake-lock
fi

# 2. Check for python & websockets
if ! command -v python >/dev/null 2>&1; then
    echo "[INFO] Installing python in Termux..."
    pkg update -y && pkg install -y python python-pip
fi

if ! python -c "import websockets" >/dev/null 2>&1; then
    echo "[INFO] Installing websockets python package..."
    pip install websockets
fi

# 3. Execution loop
while true; do
    echo "[INFO] Launching OxideSwarm Agent Node..."
    python scripts/agent_node.py --hub "$HUB_URL" --id "$NODE_ID" --platform android || true
    echo "[WARN] Agent stopped or disconnected. Reconnecting in 3 seconds..."
    sleep 3
done
