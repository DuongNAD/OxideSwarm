#!/bin/bash
# Double-click on macOS: runs uninstall.sh from this folder in a Terminal window.
cd "$(dirname "$0")" || exit 1
bash ./uninstall.sh "$@"
status=$?
echo
read -r -p "Press Return to close this window. " _
exit $status
