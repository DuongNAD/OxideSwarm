#!/usr/bin/env bash
# Top-level POSIX runner detecting macOS vs Linux
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNAME_S="$(uname -s)"

if [[ "$UNAME_S" == "Darwin" ]]; then
    exec "$SCRIPT_DIR/macos/run_agent_node.sh" "$@"
else
    exec "$SCRIPT_DIR/ubuntu/run_agent_node.sh" "$@"
fi
