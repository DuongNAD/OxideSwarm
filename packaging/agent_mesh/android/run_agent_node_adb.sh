#!/usr/bin/env bash
# ==============================================================================
# run_agent_node_adb.sh — Host script to cross-compile, push and run on Android via ADB
#
# Prerequisite: Android device connected via ADB with USB debugging enabled.
# Architecture: aarch64-linux-android (ARM64)
# ==============================================================================

set -euo pipefail

HUB_URL="${1:-ws://192.168.1.100:8088/ws}"
DEFAULT_NODE_ID="node-android-adb"
NODE_ID="${2:-$DEFAULT_NODE_ID}"

echo "========================================================"
echo "   OxideSwarm Android Standalone Binary Deployer (ADB)  "
echo "========================================================"
echo "Target Hub: $HUB_URL"
echo "Node ID:    $NODE_ID"
echo ""

# 1. Check ADB connection
echo "[INFO] Checking ADB devices..."
adb devices
DEVICE_COUNT=$(adb devices | grep -v "List" | grep "device$" | wc -l || true)
if [[ "$DEVICE_COUNT" -eq 0 ]]; then
    echo "[ERROR] No authorized Android device detected via ADB."
    exit 1
fi

# 2. Check or build binary
TARGET_BIN="target/aarch64-linux-android/release/agent-mesh"
if [[ ! -f "$TARGET_BIN" ]]; then
    if [[ -f "target/aarch64-linux-android/release/rusty-grid" ]]; then
        TARGET_BIN="target/aarch64-linux-android/release/rusty-grid"
    else
        echo "[INFO] Compiling agent-mesh for aarch64-linux-android..."
        cargo build --release --target aarch64-linux-android -p agent_mesh || true
        if [[ ! -f "$TARGET_BIN" && -f "build_android_worker.sh" ]]; then
            bash build_android_worker.sh --release || true
            TARGET_BIN="target/aarch64-linux-android/release/rusty-grid"
        fi
    fi
fi

# 3. Push to Android /data/local/tmp/
REMOTE_PATH="/data/local/tmp/agent-mesh"
echo "[INFO] Pushing binary to Android: $REMOTE_PATH..."
adb push "$TARGET_BIN" "$REMOTE_PATH"
adb shell "chmod +x $REMOTE_PATH"

# 4. Launch on Android device
echo "[INFO] Starting OxideSwarm Agent on Android device..."
adb shell "$REMOTE_PATH node --hub '$HUB_URL' --id '$NODE_ID' --platform android"
