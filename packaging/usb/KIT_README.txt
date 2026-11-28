OxideSwarm setup stick (kit format v1 / v0)
===========================================

This stick adds a computer to an OxideSwarm cluster for the current user,
without administrator rights.

  macOS    double-click "Install OxideSwarm.command"
  Windows  double-click "Install OxideSwarm.cmd"
  Linux    open a terminal in this folder and run:  bash install.sh

The installer:
  1. checks the program against archives/SHA256SUMS (and checks the online
     release SHA256SUMS when reachable or configured),
  2. copies it to a per-user folder:
       macOS    ~/Library/Application Support/OxideSwarm
       Windows  %LOCALAPPDATA%\OxideSwarm-USB
       Linux    ~/.local/share/oxideswarm
  3. joins the cluster:
       - v1 kits ("oxideswarm usb create"): decrypts invite.oxs using the
         four-word passphrase printed when the stick was created, prints the
         master fingerprint and pairing check digits, and waits for admin
         approval when required,
       - v0 kits ("make_kit.sh"): joins with join.token,
  4. configures kit.json options (roles, --workspaces, optional GPU build),
     starts the worker (when "worker" is in roles) and registers logon start,
  5. runs "oxideswarm doctor" when the "client" role is present (skipped on
     worker-only v1 kits).

If the stick also holds a GPU build, the installer first asks whether the
cluster may use this computer's GPU as well. Answer n to keep the GPU out.

If the stick holds developer tools (Python, Rust, MinGW-w64 C toolchain,
Git), the Windows installer asks once whether to install them privately
inside OxideSwarm's own folder (your system PATH and registry are never
changed, and Uninstall removes them with everything else).

On Windows with a second NTFS/ReFS drive (for example D:), the installer asks
where to keep its files. A folder on that drive also holds the task folders
and the file cache, and only your Windows account can open it.

When the installer is done, the window offers to eject the stick; say yes
and remove it when it says so. The worker does not run from the stick.

To update a computer that already runs OxideSwarm, double-click "Update
OxideSwarm" instead (Linux: bash install.sh --update). It keeps the name, the
GPU choice, isolation and cluster membership, asks nothing and joins nothing.

It never changes ~/.oxideswarm (%USERPROFILE%\.oxideswarm) or any other
OxideSwarm installation on the computer. To remove it, run "Uninstall
OxideSwarm" from the same stick (pass --keep-key / -KeepKey or answer yes
when prompted to keep node.key so rejoining keeps the same machine identity).

IMPORTANT:
  - v1 kits hold invite.oxs encrypted with the creator's passphrase (for up to
    N machines before expiry). If a v1 stick is lost or compromised, revoke its
    invite immediately on the master with:  oxideswarm invite --revoke <invite_id>
    Check kit.json for the invite_id. If a whole stick is swapped offline,
    comparing archives/SHA256SUMS against the online release (or verifying
    the master fingerprint and pairing check during join) catches tampering.
  - v0 kits hold a plain join.token; if lost, change the cluster secret.

Anyone who joins the cluster can run commands on a worker computer. Only join
clusters of people you trust.
