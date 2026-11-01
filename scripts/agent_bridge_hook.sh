#!/bin/sh
# Runs agent_bridge.py for Claude Code hooks with whichever Python this machine has: Windows has
# `python`, and its `python3` may be a Store stub that does nothing. Stays silent without a usable
# Python, so a hook never disturbs a session.
dir=$(dirname "$0")
for py in python3 python; do
    if "$py" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 7) else 1)' >/dev/null 2>&1; then
        exec "$py" "$dir/agent_bridge.py" "$@"
    fi
done
exit 0
