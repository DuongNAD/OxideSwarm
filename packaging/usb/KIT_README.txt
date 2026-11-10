OxideSwarm setup stick (kit format v0)
=======================================

This stick adds a computer to an OxideSwarm cluster as a worker, for the
current user, without administrator rights.

  macOS    double-click "Install OxideSwarm.command"
  Windows  double-click "Install OxideSwarm.cmd"
  Linux    open a terminal in this folder and run:  bash install.sh

The installer:
  1. checks the program against archives/SHA256SUMS,
  2. copies it to a per-user folder
       macOS    ~/Library/Application Support/OxideSwarm
       Windows  %LOCALAPPDATA%\OxideSwarm
       Linux    ~/.local/share/oxideswarm
  3. joins the cluster with join.token,
  4. starts the worker and starts it again at every login,
  5. runs "oxideswarm doctor" and prints the result.

If the stick also holds a GPU build, the installer first asks whether the
cluster may use this computer's GPU as well. Answer n to keep the GPU out.

It never changes ~/.oxideswarm or any other OxideSwarm installation on the
computer. To remove it, run "Uninstall OxideSwarm" from the same stick.

IMPORTANT: join.token on this stick lets a computer join the cluster and run
work for it. Keep the stick with people you trust. If it is lost, the
cluster owner must change the cluster secret. (Kits made with "oxideswarm
usb create", design 0053, will carry an invite encrypted with a passphrase
that works a limited number of times instead.)

Anyone who joins the cluster can run commands on this computer through the
worker. Only join clusters of people you trust.
