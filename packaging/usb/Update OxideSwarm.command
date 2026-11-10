#!/bin/bash
# Double-click on macOS: updates the OxideSwarm worker this stick installed, in place. Keeps its
# folder, name, GPU choice and cluster membership; nothing is asked.
cd "$(dirname "$0")" || exit 1
bash ./install.sh --update "$@"
status=$?
echo
read -r -p "Press Return to close this window. " _
exit $status
