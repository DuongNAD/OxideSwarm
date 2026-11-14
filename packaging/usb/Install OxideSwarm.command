#!/bin/bash
# Double-click on macOS: runs install.sh from this folder in a Terminal window, then offers to
# eject the stick.
cd "$(dirname "$0")" || exit 1
bash ./install.sh "$@"
status=$?
echo
# Offer to eject the stick: the installed worker does not run from it. The prompt runs in a new shell
# after `cd /`, so neither this script (read from the stick) nor the window keeps the volume busy.
VOL=$(df -P . | awk 'NR == 2 { v = $6; for (i = 7; i <= NF; i++) v = v " " $i; print v }')
cd / || exit $status
exec /bin/bash -c '
  case "$1" in
    /Volumes/?*)
      read -r -p "Eject the stick ($1) so it can be removed safely? [Y/n] " a
      case "$a" in
        [Nn]*) ;;
        *) ok=0
           # Spotlight may still be reading a freshly mounted stick: try a few times.
           for try in 1 2 3; do
             if diskutil eject "$1" >/dev/null 2>&1; then ok=1; break; fi
             sleep 2
           done
           if [ "$ok" = 1 ]; then echo "Ejected: you can remove the stick now."
           else echo "Could not eject $1: eject it in Finder before you remove the stick."; fi ;;
      esac ;;
  esac
  read -r -p "Press Return to close this window. " _
  exit "$2"' eject "$VOL" "$status"
