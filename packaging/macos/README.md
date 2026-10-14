# OxideSwarm macOS LaunchDaemon Packaging (R2)

This directory contains the production-ready macOS service packaging for the **OxideSwarm** distributed grid worker node (`rusty-grid` / `oxideswarm`).

The worker is packaged as a native macOS **System LaunchDaemon**, managed by Apple's `launchd` subsystem (PID 1). This ensures that the compute node runs silently in the background, starts automatically on system boot prior to user login, executes headlessly without requiring an interactive window session, and automatically restarts if the worker process crashes or exits.

---

## 1. Directory & File Inventory

| File | Purpose |
|---|---|
| `com.oxideswarm.worker.plist` | Primary Apple XML property list (`launchd.plist`) defining the LaunchDaemon service, startup behavior, process supervision, I/O log redirection, and high-concurrency file limits. |
| `install_mac_daemon.sh` | Production bash installer script. Validates root privileges, deploys binary and symlink, sets strict permissions, writes/configures the plist, bootstraps into `launchd`, and verifies silent execution. |
| `uninstall_mac_daemon.sh` | Automated uninstaller script. Safely tears down and boots out the daemon from `launchd`, deletes `/Library/LaunchDaemons/com.oxideswarm.worker.plist`, and supports optional `--purge` to delete binaries and logs. |
| `verify_mac_daemon.sh` | Comprehensive automated test suite. Validates XML syntax via `plutil -lint`, asserts presence and types of all required keys (`Label`, `ProgramArguments`, `RunAtLoad`, `KeepAlive`), and tests `launchctl` registration (supporting dry-run / non-root lint validation and actual live root validation). |
| `README.md` | Complete architectural, installation, operational, and troubleshooting documentation (this document). |

---

## 2. Architecture & Design Principles

### 2.1 System LaunchDaemon vs. User LaunchAgent
In macOS, background processes can run either as **LaunchAgents** (in the `gui/<UID>` user domain) or **LaunchDaemons** (in the `system` domain):
- **User LaunchAgents** (`~/Library/LaunchAgents/`): Only launch when a user logs in via the Aqua GUI, run under that user's identity, and are suspended or killed when the user logs out.
- **System LaunchDaemons** (`/Library/LaunchDaemons/`): Launch immediately during kernel boot before any login window is displayed, run as a headless system service, survive desktop logouts, and continue computing 24/7.

A Mac that should be a persistent, uninterrupted grid node is therefore deployed as a System LaunchDaemon. **The daemon is loaded by launchd at boot, but it does not have to run as root, and by default it does not:** the plist carries a `UserName` key, so the worker (and every task the master sends it) runs with that account's rights.

### 2.1b Who runs the worker (read this before installing)
A worker executes whatever commands the master dispatches, with the identity of the process. `install_mac_daemon.sh` therefore runs it as an **unprivileged account**: the user who invoked `sudo` (or `--user NAME`; a dedicated account is better still), and it refuses `root` unless you pass `--run-as-root`. The account's home is exported as `HOME` so it finds `~/.oxideswarm/cluster.secret`. Pick an account whose files you would be willing to expose to every holder of the cluster secret.

### 2.2 Strict File Permissions Policy
macOS `launchd` enforces strict ownership and permission boundaries for security. If permissions are too permissive, `launchctl bootstrap` will reject the service with `Load failed: 5: Input/output error`:
- **Plist Location**: `/Library/LaunchDaemons/com.oxideswarm.worker.plist`
- **Ownership**: `root:wheel` (`chown root:wheel`)
- **Permissions**: `0644` (`-rw-r--r--`) — non-root users must **not** have write access. The plist contains no secrets.
- **Executable Location**: `/usr/local/bin/rusty-grid` (`0755`, `root:wheel`)
- **Brand Symlink**: `/usr/local/bin/oxideswarm -> /usr/local/bin/rusty-grid`
- **Worker config**: `/etc/oxideswarm/worker.toml` (`0600`, owned by the service account) holds the master address or P2P ticket and the secret path; `/etc/oxideswarm/cluster.secret` (`0600`, same owner) is the copy of the cluster secret. The plist's `ProgramArguments` are just `rusty-grid worker --config /etc/oxideswarm/worker.toml`, so neither the ticket nor any secret shows up in `ps` or in a world-readable file.

### 2.3 Process Supervision & Resiliency
- **`RunAtLoad` (`<true/>`)**: Instructs `launchd` to launch the worker immediately upon daemon load and whenever macOS boots up.
- **`KeepAlive` (`<true/>`)**: Instructs `launchd` to monitor the PID and automatically relaunch `rusty-grid worker` if it exits unexpectedly or crashes.
- **`ThrottleInterval` (`5` seconds)**: Prevents high-frequency crash restart storms if there is a fatal networking or configuration issue.
- **`SoftResourceLimits` / `HardResourceLimits` (`NumberOfFiles = 65536`)**: Elevates macOS default file descriptor limits (`ulimit -n 256`) to 65,536, preventing descriptor exhaustion during intensive network I/O and parallel distributed compilation tasks.
- **`StandardOutPath` / `StandardErrorPath`**: Redirects stdout and stderr to `/var/log/oxideswarm/worker.log` and `/var/log/oxideswarm/worker.err.log` so the process runs 100% headlessly with full observability.

---

## 3. Quick Start Installation

### Step 1: Compile Native Binary (if not already built)
From the repository root:
```bash
cargo build --release --bin rusty-grid --features gpu-wgpu
```
`--features gpu-wgpu` is what lets the worker use this Mac's GPU (Metal): it then finds the GPU itself and prints `GPU compute enabled on <chip>` in the log. Without the feature the worker advertises no GPU (also with `--gpu`).

### Step 2: Get the master's address and cluster secret
* **Address**: the master's *cluster* port, `HOST:8088` (the dashboard on 8080 is never a worker target).
* **Secret**: copy `~/.oxideswarm/cluster.secret` from the master (created on its first start when it listens beyond
  loopback) over a trusted channel: `scp user@master:~/.oxideswarm/cluster.secret ~/cluster.secret`, or a USB stick.
  A worker refuses to follow a remote master without it (`--insecure` overrides, not recommended).

### Step 3: Install and Start the Daemon
Run the automated installer with `sudo`:
```bash
sudo bash packaging/macos/install_mac_daemon.sh --master 192.168.1.100:8088 --secret-file ~/cluster.secret
```

The installer will:
1. Pick the service account (the user behind `sudo`, `--user NAME`, or ask) and confirm it is not root.
2. Copy the compiled binary to `/usr/local/bin/rusty-grid` (mode `0755`) and stop with an error if there is none (no crash-looping daemon).
3. Create the brand alias symlink `/usr/local/bin/oxideswarm`.
4. Create `/var/log/oxideswarm` and `/var/lib/oxideswarm` owned by the service account, and `/etc/oxideswarm/` with the `0600` worker config and secret copy.
5. Install `/Library/LaunchDaemons/com.oxideswarm.worker.plist` with `root:wheel` and `0644` and a `UserName` key.
6. Lint the plist syntax using `plutil -lint`.
7. Bootstrap the service via modern `launchctl bootstrap system`.
8. Report whether launchd started the process. To confirm that the worker really joined the grid, look for `Registration accepted` in `/var/log/oxideswarm/worker.log`.

---

## 4. Advanced Installer Configuration

The `install_mac_daemon.sh` script accepts flexible options:

```bash
sudo bash packaging/macos/install_mac_daemon.sh [OPTIONS]
```

### Supported Flags:
- `--master <ADDR>`: the Master's **cluster** address `HOST:8088`. One of `--master`, `--p2p-ticket-file`, `--p2p-ticket` or `--config` is required (no default: a loopback default silently connected to nothing).
- `--p2p-ticket-file <FILE>`: file holding the P2P ticket of a master started with `--p2p`, for `iroh` NAT traversal across the internet without port forwarding. Preferred over `--p2p-ticket <TICKET>`, which is visible in `ps` while the installer runs.
- `--secret-file <FILE>`: the cluster secret copied from the master (default: `$OXIDESWARM_SECRET_FILE`, then `<service user's home>/.oxideswarm/cluster.secret`). Missing and required for a remote master: the installer stops.
- `--insecure`: install without a secret (the worker then obeys any host that answers as the master). Not recommended.
- `--user <NAME>`: account that runs the worker and its tasks (default: the user behind `sudo`; asked interactively when ambiguous).
- `--run-as-root`: run as root. Every task the master sends then controls this Mac. Prints a warning.
- `--yes`: do not ask for confirmation.
- `--name <NAME>`: Custom human-readable worker identifier advertised to Master.
- `--config <PATH>`: use this worker config as is instead of generating `/etc/oxideswarm/worker.toml` (the service account must be able to read it).
- `--bin <PATH>`: Custom path to source `rusty-grid` binary.
- `--bin-dir <DIR>`: Target binary install directory (default: `/usr/local/bin`).
- `--log-dir <DIR>`: Target log output directory (default: `/var/log/oxideswarm`).
- `--work-dir <DIR>`: Working sandbox directory (default: `/var/lib/oxideswarm`).
- `--dry-run`: Preview all configuration, file operations, and generated XML plist without modifying system state (safe to run without root).
- `-h`, `--help`: Display CLI help.

### Examples:
```bash
# Connect to remote master via direct IP
sudo bash packaging/macos/install_mac_daemon.sh --master 192.168.1.100:8088 --secret-file ~/cluster.secret --name macbook-pro-m3

# Connect via P2P NAT Traversal ticket (Iroh); the master must run with --p2p
sudo bash packaging/macos/install_mac_daemon.sh --p2p-ticket-file ticket.txt --secret-file ~/cluster.secret

# Dedicated account instead of your own
sudo bash packaging/macos/install_mac_daemon.sh --master 192.168.1.100:8088 --secret-file ~/cluster.secret --user _oxideswarm

# Non-root dry run simulation (writes nothing; shows the config and plist it would create)
bash packaging/macos/install_mac_daemon.sh --dry-run --master 10.0.0.1:8088 --insecure
```

---

## 5. Verification & Testing

The repository provides `verify_mac_daemon.sh` to validate the LaunchDaemon configuration.

### Non-Root / Dry-Run Syntax & Structural Validation
To validate XML syntax, launchd schema requirements, and key definitions without requiring root privileges:
```bash
bash packaging/macos/verify_mac_daemon.sh
```
Or specify `--dry-run` / `--lint-only`:
```bash
bash packaging/macos/verify_mac_daemon.sh --dry-run
```

Output:
```text
========================================================
   OxideSwarm macOS LaunchDaemon Verification Suite
========================================================

[INFO] Target Plist       : packaging/macos/com.oxideswarm.worker.plist
[INFO] Verification Mode  : auto
[CHECK 1] Plist file existence and readability... [PASS] Found file at packaging/macos/com.oxideswarm.worker.plist
[CHECK 2] XML Syntax & structure linting (plutil -lint)... [PASS] plutil lint passed: packaging/macos/com.oxideswarm.worker.plist: OK
[CHECK 3] Validating required key 'Label'... [PASS] Label = 'com.oxideswarm.worker'
[CHECK 4] Validating required key 'ProgramArguments'... [PASS] Executable: /usr/local/bin/rusty-grid | Subcommand: worker (Count: 4 items)
[CHECK 5] Validating required key 'RunAtLoad'... [PASS] RunAtLoad = true (daemon will start automatically on boot/load)
[CHECK 6] Validating required key 'KeepAlive'... [PASS] KeepAlive = true (daemon will automatically restart on exit/crash)
[CHECK 7] Validating key 'ThrottleInterval'... [PASS] ThrottleInterval = 5s (crash storm protection active)
[CHECK 8] Validating I/O log redirection keys... [PASS] StandardOutPath='/var/log/oxideswarm/worker.log', StandardErrorPath='/var/log/oxideswarm/worker.err.log'
[CHECK 9] Validating ResourceLimits (SoftResourceLimits / HardResourceLimits)... [PASS] NumberOfFiles limits configured (Soft: 65536, Hard: 65536)
[CHECK 10] Validating execution environment & WorkingDirectory... [PASS] WorkingDirectory = '/var/lib/oxideswarm'
[CHECK 10b] Validating service account (UserName, not root)... [PASS] UserName is the template placeholder (install_mac_daemon.sh fills in the real account)

--- Dry-Run / Non-Root Summary ---
[INFO] Static property list validation complete.
[INFO] Target was evaluated for syntax, types, and schema compliance without modifying system state.

========================================================
  VERIFICATION RESULT: ALL CHECKS PASSED [OK] (Score: 11/11)
========================================================
```

### Live System Audit
After installing the daemon with root privileges, run:
```bash
bash packaging/macos/verify_mac_daemon.sh --live
```
This performs all structural tests (including that the plist names a non-root `UserName`) and additionally verifies:
- Plist file ownership is strictly `root:wheel` with octal permissions `0644`.
- Service is actively registered in `launchctl list com.oxideswarm.worker` or `launchctl print system/com.oxideswarm.worker`.
- The PID launchd reports for the supervised worker (it does not search the process table by name).
- Log directory `/var/log/oxideswarm` presence and file write status.

---

## 6. Service Management (`launchctl`)

Once installed, manage the worker using native `launchctl` commands:

### Modern Subcommands (macOS 10.10+ through macOS 15+):
```bash
# Check daemon status and metadata
sudo launchctl print system/com.oxideswarm.worker

# Stop the daemon temporarily
sudo launchctl kill SIGTERM system/com.oxideswarm.worker

# Manually trigger immediate start / restart
sudo launchctl kickstart -k system/com.oxideswarm.worker

# Boot out (stop and deregister)
sudo launchctl bootout system /Library/LaunchDaemons/com.oxideswarm.worker.plist

# Bootstrap (register and start)
sudo launchctl bootstrap system /Library/LaunchDaemons/com.oxideswarm.worker.plist
```

### Legacy Subcommands:
```bash
# Query status (displays PID and LastExitStatus)
sudo launchctl list | grep com.oxideswarm.worker

# Unload daemon
sudo launchctl unload -w /Library/LaunchDaemons/com.oxideswarm.worker.plist

# Load daemon
sudo launchctl load -w /Library/LaunchDaemons/com.oxideswarm.worker.plist
```

---

## 7. Logging & Observability

Standard output and standard error from the background daemon are automatically redirected to dedicated files:

```bash
# Follow live standard output
tail -f /var/log/oxideswarm/worker.log

# Follow live error output
tail -f /var/log/oxideswarm/worker.err.log

# View launchd system event logs
log show --predicate 'subsystem == "com.apple.launchd" and process == "com.oxideswarm.worker"' --info --last 30m
```

---

## 8. Uninstallation

### Standard Uninstallation:
Deregisters the daemon from `launchctl` and removes `/Library/LaunchDaemons/com.oxideswarm.worker.plist`:
```bash
sudo bash packaging/macos/uninstall_mac_daemon.sh
```

### Full Purge:
Stops and unloads the daemon, and deletes all binaries (`/usr/local/bin/rusty-grid`, `/usr/local/bin/oxideswarm`), logs (`/var/log/oxideswarm`), the worker config and secret copy (`/etc/oxideswarm`), and sandbox working directories. Without `--purge` the `0600` config and secret copy are kept:
```bash
sudo bash packaging/macos/uninstall_mac_daemon.sh --purge
```

---

## 9. Troubleshooting

### Issue: `Load failed: 5: Input/output error`
- **Cause**: Plist file ownership or permissions violate launchd security policy (e.g. owned by non-root or group-writable).
- **Fix**: Run:
  ```bash
  sudo chown root:wheel /Library/LaunchDaemons/com.oxideswarm.worker.plist
  sudo chmod 644 /Library/LaunchDaemons/com.oxideswarm.worker.plist
  ```

### Issue: `Bootstrap failed: 17: File exists`
- **Cause**: The daemon is already registered in launchd.
- **Fix**: Boot out the existing registration first:
  ```bash
  sudo launchctl bootout system /Library/LaunchDaemons/com.oxideswarm.worker.plist
  sudo launchctl bootstrap system /Library/LaunchDaemons/com.oxideswarm.worker.plist
  ```

### Issue: `worker.err.log` / `worker.log` shows connection refused, authentication errors or nothing
- **Cause**: The Master is not reachable at the configured address, the address uses the dashboard port 8080 instead of 8088, or the cluster secret does not match the master's (look for `Authentication required` / `Authentication failed`).
- **Fix**: Re-install with the correct cluster address and the master's current secret file (the installer replaces the running instance):
  ```bash
  sudo bash packaging/macos/install_mac_daemon.sh --master <MASTER_IP>:8088 --secret-file ~/cluster.secret
  ```
