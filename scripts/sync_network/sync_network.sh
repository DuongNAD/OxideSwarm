#!/usr/bin/env bash
# ==============================================================================
# sync_network.sh — macOS Launcher for OxideSwarm Cross-Machine Protocol
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export PYTHONDONTWRITEBYTECODE=1

PYTHON_CMD=""
for cand in python3 /usr/bin/python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
        PYTHON_CMD="$cand"
        break
    fi
done

if [[ -z "$PYTHON_CMD" ]]; then
    echo "[ERROR] Python 3 is required but not found in PATH." >&2
    exit 1
fi

chmod +x "${SCRIPT_DIR}/sync_network.py" 2>/dev/null || true
exec "$PYTHON_CMD" -B "${SCRIPT_DIR}/sync_network.py" "$@"
