<#
.SYNOPSIS
    Starts, stops and inspects the OxideSwarm master / worker that the launchers in this folder run.

.DESCRIPTION
    Shared engine behind start_master.cmd, start_worker.cmd, stop_worker.cmd, status_worker.cmd and
    switch_role.cmd. Processes are tracked through PID files in <state dir>\run, so nothing is ever
    stopped by image name: a worker installed as a Windows service (install_service.cmd) or a master
    you started by hand is left alone.

    Launch flow (see docs\SECURITY.md):
      master : rusty-grid master --listen 0.0.0.0:8088 --web-ui-addr 127.0.0.1:8080
               The first start creates %USERPROFILE%\.oxideswarm\cluster.secret; copy that file
               to every worker. The dashboard stays on loopback unless -DashboardLan is given
               (then log in with the token from: rusty-grid secret show --dashboard-token).
               -P2p additionally publishes a P2P ticket for machines on other networks.
      worker : rusty-grid worker --master HOST:8088   (the CLUSTER port, never the dashboard's 8080)
               The worker needs the same cluster secret file.

.PARAMETER Action
    start, stop or status.

.PARAMETER Role
    master or worker (start), master / worker / all (stop, status).

.PARAMETER MasterAddr
    Worker only: master cluster address HOST:8088, or "auto" for LAN discovery. Default: auto.

.PARAMETER WorkerName
    Worker only: name advertised to the master. Default: <computer name>-worker.

.PARAMETER SecretFile
    Cluster secret file. Default: the CLI's own lookup (OXIDESWARM_SECRET[_FILE], then
    %USERPROFILE%\.oxideswarm\cluster.secret). When OXIDESWARM_HOME is set, <it>\cluster.secret.

.PARAMETER DashboardLan
    Master only: serve the dashboard on 0.0.0.0:8080 (phones / other PCs) instead of loopback.

.PARAMETER P2p
    Master only: enable the P2P (QUIC) endpoint and write the ticket to <state dir>\master_ticket.txt.

.PARAMETER NoWorker
    Master only: do not start the local worker next to the master.

.PARAMETER ExtraArgs
    Extra arguments appended verbatim to the rusty-grid command line (for example "--gpu").

.EXAMPLE
    .\oxctl.ps1 start master -DashboardLan
    .\oxctl.ps1 start worker -MasterAddr 192.168.1.100:8088
    .\oxctl.ps1 stop all
#>

[CmdletBinding()]
param (
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("start", "stop", "status")]
    [string]$Action,

    [Parameter(Position = 1)]
    [ValidateSet("master", "worker", "all")]
    [string]$Role = "all",

    [string]$MasterAddr = "auto",
    [string]$WorkerName = "",
    [string]$SecretFile = "",
    [switch]$DashboardLan,
    [switch]$P2p,
    [switch]$NoWorker,
    [string]$ExtraArgs = ""
)

$ErrorActionPreference = "Stop"

# The .cmd launchers hand extra arguments over in OX_EXTRA_ARGS (a value starting with "--" is
# awkward to pass as a -File parameter).
if (-not $ExtraArgs -and $env:OX_EXTRA_ARGS) { $ExtraArgs = $env:OX_EXTRA_ARGS }

$ClusterPort = if ($env:OXIDESWARM_CLUSTER_PORT) { $env:OXIDESWARM_CLUSTER_PORT } else { "8088" }
$DashboardPort = if ($env:OXIDESWARM_DASHBOARD_PORT) { $env:OXIDESWARM_DASHBOARD_PORT } else { "8080" }

# OXIDESWARM_HOME only moves this launcher's state; when set, the secret file is passed explicitly.
$StateDir = if ($env:OXIDESWARM_HOME) { $env:OXIDESWARM_HOME } else { Join-Path $env:USERPROFILE ".oxideswarm" }
$RunDir = Join-Path $StateDir "run"
$TicketFile = Join-Path $StateDir "master_ticket.txt"
if (-not $SecretFile -and $env:OXIDESWARM_HOME) { $SecretFile = Join-Path $StateDir "cluster.secret" }
$DefaultSecretPath = Join-Path $StateDir "cluster.secret"

function Find-Binary {
    if ($env:OXIDESWARM_BIN -and (Test-Path -LiteralPath $env:OXIDESWARM_BIN)) { return $env:OXIDESWARM_BIN }
    $candidates = @(
        (Join-Path $PSScriptRoot "oxideswarm.exe"),
        (Join-Path $PSScriptRoot "rusty-grid.exe"),
        (Join-Path $PSScriptRoot "..\target\release\oxideswarm.exe"),
        (Join-Path $PSScriptRoot "..\target\release\rusty-grid.exe"),
        (Join-Path $PSScriptRoot "target\release\rusty-grid.exe")
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { return (Resolve-Path -LiteralPath $c).Path }
    }
    Write-Host "[INFO] rusty-grid.exe not found next to the launchers. Compiling via cargo (release)..."
    Push-Location (Join-Path $PSScriptRoot "..")
    try {
        # gpu-wgpu lets the worker use this PC's graphics card; OXIDESWARM_CARGO_FEATURES="" builds without it.
        $features = if ($null -ne $env:OXIDESWARM_CARGO_FEATURES) { $env:OXIDESWARM_CARGO_FEATURES } else { "gpu-wgpu" }
        $cargoArgs = @("build", "--release", "--bin", "rusty-grid")
        if ($features) { $cargoArgs += @("--features", $features) }
        & cargo @cargoArgs
        if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
    } finally {
        Pop-Location
    }
    return (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\target\release\rusty-grid.exe")).Path
}

function Get-TrackedPid {
    param ([string]$Name)
    $pidFile = Join-Path $RunDir "$Name.pid"
    if (-not (Test-Path -LiteralPath $pidFile)) { return $null }
    $text = (Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    $parsed = 0
    if (-not [int]::TryParse(("$text").Trim(), [ref]$parsed)) { return $null }
    $proc = Get-Process -Id $parsed -ErrorAction SilentlyContinue
    if (-not $proc) { return $null }
    # Guard against a recycled PID: it must still be one of our binaries.
    if ($proc.ProcessName -notmatch '^(rusty-grid|oxideswarm)$') { return $null }
    return $parsed
}

# Seconds a tracked process gets to exit after the Ctrl+C event before taskkill /F /T ends it. A master
# compacts its --state-dir journal on that clean stop, and a worker cancels its tasks and ends their
# process trees; both took about a second on a desktop (BUG-23, docs\design\0086-graceful-windows-stop.md).
$StopGraceSeconds = 15

# Sends a console Ctrl+C event to the process tracked as $TargetPid. Every process Start-Tracked starts
# has its own (hidden) console, so the event goes to that console only. A separate hidden powershell
# detaches from its own console, attaches to the target's, ignores the event itself and raises it;
# the console this script runs in is never touched. Returns $true when the event was sent.
function Send-ConsoleCtrlC {
    param ([int]$TargetPid)
    $helper = @"
Add-Type -Namespace OxCtl -Name Console -MemberDefinition '
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool FreeConsole();
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool AttachConsole(uint dwProcessId);
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool SetConsoleCtrlHandler(System.IntPtr handlerRoutine, bool add);
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool GenerateConsoleCtrlEvent(uint dwCtrlEvent, uint dwProcessGroupId);
'
[void][OxCtl.Console]::FreeConsole()
if (-not [OxCtl.Console]::AttachConsole($TargetPid)) { exit 2 }
[void][OxCtl.Console]::SetConsoleCtrlHandler([System.IntPtr]::Zero, `$true)
if (-not [OxCtl.Console]::GenerateConsoleCtrlEvent(0, 0)) { exit 3 }
Start-Sleep -Milliseconds 200
exit 0
"@
    $encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($helper))
    try {
        $sender = Start-Process -FilePath "powershell.exe" -WindowStyle Hidden -PassThru `
            -ArgumentList "-NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand $encoded"
    } catch {
        return $false
    }
    $null = $sender.Handle   # keeps the exit code readable after the process ends
    if (-not $sender.WaitForExit(20000)) {
        Stop-Process -Id $sender.Id -Force -ErrorAction SilentlyContinue
        return $false
    }
    return ($sender.ExitCode -eq 0)
}

# Stops a tracked process: first the console Ctrl+C its shutdown handler answers (a master compacts its
# journal, a worker cancels its tasks), then, if it is still running after $StopGraceSeconds, taskkill /F /T.
# Says which way it stopped.
function Stop-Tracked {
    param ([string]$Name)
    $trackedPid = Get-TrackedPid -Name $Name
    if ($trackedPid) {
        Write-Host "[INFO] Stopping $Name (PID $trackedPid) and its task processes..."
        $proc = Get-Process -Id $trackedPid -ErrorAction SilentlyContinue
        $clean = $false
        if ($proc) {
            $null = $proc.Handle
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            if (Send-ConsoleCtrlC -TargetPid $trackedPid) {
                if ($proc.WaitForExit($StopGraceSeconds * 1000)) {
                    $clean = $true
                    Write-Host ("[OK] {0} stopped cleanly after Ctrl+C in {1:N1} s." -f $Name, $sw.Elapsed.TotalSeconds)
                } else {
                    Write-Warning "$Name (PID $trackedPid) did not exit within $StopGraceSeconds s after Ctrl+C; ending it with taskkill /F /T."
                }
            } else {
                Write-Warning "Could not send Ctrl+C to $Name (PID $trackedPid); ending it with taskkill /F /T."
            }
        }
        if (-not $clean -and (Get-TrackedPid -Name $Name)) {
            & taskkill.exe /F /T /PID $trackedPid | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "taskkill reported exit code $LASTEXITCODE for PID $trackedPid (it may already have exited)."
            } elseif ($Name -eq "master") {
                Write-Host "[OK] master ended with taskkill /F /T. With --state-dir its journal was not compacted: before a protocol upgrade, start it again and stop it with Ctrl+C in its console (DEPLOYMENT.md section 3.5)."
            } else {
                Write-Host "[OK] $Name ended with taskkill /F /T."
            }
        }
    }
    Remove-Item -LiteralPath (Join-Path $RunDir "$Name.pid") -Force -ErrorAction SilentlyContinue
}

function Start-Tracked {
    param (
        [string]$Name,
        [string]$Binary,
        [string]$ArgString
    )
    New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
    $out = Join-Path $RunDir "$Name.log"
    $err = Join-Path $RunDir "$Name.err.log"
    foreach ($f in @($out, $err)) {
        if (Test-Path -LiteralPath $f) { Move-Item -LiteralPath $f -Destination "$f.old" -Force }
    }
    $proc = Start-Process -FilePath $Binary -ArgumentList $ArgString -WindowStyle Hidden `
        -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
    Set-Content -LiteralPath (Join-Path $RunDir "$Name.pid") -Value $proc.Id -Encoding ASCII
    return $proc
}

# Whether the log file contains the pattern. The launched process keeps its redirected log open for
# writing, so open it with shared access (Select-String would fail with "used by another process").
function Test-LogContains {
    param (
        [string]$Path,
        [string]$Pattern
    )
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $stream = $null
    try {
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        $reader = New-Object System.IO.StreamReader($stream)
        return (($reader.ReadToEnd()) -match $Pattern)
    } catch {
        return $false
    } finally {
        if ($stream) { $stream.Dispose() }
    }
}

# Waits up to $Seconds for the worker log to show that the master accepted the registration
# (a running process is not a connected worker).
function Wait-Registered {
    param (
        [string]$Path,
        [int]$Seconds,
        $Proc
    )
    for ($i = 0; $i -lt ($Seconds * 2); $i++) {
        if (Test-LogContains -Path $Path -Pattern "Registration accepted") { return $true }
        if ($Proc -and $Proc.HasExited) { return $false }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Show-LogTail {
    param ([string]$Path)
    if (Test-Path -LiteralPath $Path) {
        Write-Host "---- last lines of $Path ----"
        Get-Content -LiteralPath $Path -Tail 15
        Write-Host "---------------------------------"
    }
}

function Get-LanIp {
    try {
        $ip = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop |
            Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254.*' -and $_.PrefixOrigin -ne 'WellKnown' } |
            Select-Object -First 1
        if ($ip) { return $ip.IPAddress }
    } catch { }
    return "<IP-of-this-PC>"
}

function Get-SecretArgs {
    if ($SecretFile) { return " --secret-file `"$SecretFile`"" }
    return ""
}

function Start-Master {
    $bin = Find-Binary
    Stop-Tracked -Name "worker"
    Stop-Tracked -Name "master"

    $listening = Get-NetTCPConnection -State Listen -LocalPort ([int]$ClusterPort) -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($listening) {
        Write-Error "Port $ClusterPort is already in use (PID $($listening.OwningProcess)). Stop that process, or a master started by hand, and retry."
    }

    # Explicit loopback keeps the dashboard private even for binaries whose default follows --listen.
    $dashAddr = if ($DashboardLan) { "0.0.0.0:$DashboardPort" } else { "127.0.0.1:$DashboardPort" }
    $argString = "master --listen 0.0.0.0:$ClusterPort --web-ui-addr $dashAddr"
    if ($P2p) {
        if (Test-Path -LiteralPath $TicketFile) { Remove-Item -LiteralPath $TicketFile -Force }
        $argString += " --p2p --p2p-ticket-file `"$TicketFile`""
    }
    $argString += Get-SecretArgs
    if ($ExtraArgs) { $argString += " $ExtraArgs" }

    if ($SecretFile -and -not (Test-Path -LiteralPath $SecretFile)) {
        & $bin secret generate --out $SecretFile
    }

    Write-Host "[OK] Starting OxideSwarm Master silently in background..."
    Write-Host "     $bin $argString"
    $proc = Start-Tracked -Name "master" -Binary $bin -ArgString $argString

    $masterLog = Join-Path $RunDir "master.log"
    $started = $false
    for ($i = 0; $i -lt 50; $i++) {
        if ($proc.HasExited) { break }
        if (Test-LogContains -Path $masterLog -Pattern "RustyGrid Master started") {
            $started = $true
            break
        }
        Start-Sleep -Milliseconds 200
    }
    if (-not $started) {
        Write-Host "[ERROR] The master did not start (see below)." -ForegroundColor Red
        Show-LogTail -Path $masterLog
        Show-LogTail -Path (Join-Path $RunDir "master.err.log")
        Stop-Tracked -Name "master"
        exit 1
    }

    $workerNote = "not started (-NoWorker)"
    if (-not $NoWorker) {
        $name = if ($WorkerName) { $WorkerName } else { "$($env:COMPUTERNAME.ToLower())-worker" }
        $wargs = "worker --master 127.0.0.1:$ClusterPort --name `"$name`"" + (Get-SecretArgs)
        $wproc = Start-Tracked -Name "worker" -Binary $bin -ArgString $wargs
        Start-Sleep -Seconds 1
        if ($wproc.HasExited) {
            $workerNote = "FAILED to start, see $RunDir\worker.err.log"
            Show-LogTail -Path (Join-Path $RunDir "worker.err.log")
        } elseif (Wait-Registered -Path (Join-Path $RunDir "worker.log") -Seconds 5 -Proc $wproc) {
            $workerNote = "running and registered with the master (PID $($wproc.Id))"
        } else {
            $workerNote = "running (PID $($wproc.Id)) but NOT registered after 5 s, see $RunDir\worker.log"
            Show-LogTail -Path (Join-Path $RunDir "worker.log")
        }
    }

    $lanIp = Get-LanIp
    if ($P2p) {
        for ($i = 0; $i -lt 40; $i++) {
            if ((Test-Path -LiteralPath $TicketFile) -and ((Get-Item -LiteralPath $TicketFile).Length -gt 0)) { break }
            Start-Sleep -Milliseconds 250
        }
    }
    $secretShown = if ($SecretFile) { $SecretFile } else { $DefaultSecretPath }

    Write-Host ""
    Write-Host "=============================================================================="
    Write-Host "[SUCCESS] OxideSwarm Master is running (PID $($proc.Id))"
    Write-Host "=============================================================================="
    Write-Host "  Cluster port   : ${lanIp}:$ClusterPort   (workers connect here, NOT to $DashboardPort)"
    if ($DashboardLan) {
        Write-Host "  Dashboard      : http://${lanIp}:$DashboardPort   (open to the whole LAN)"
    } else {
        Write-Host "  Dashboard      : http://127.0.0.1:$DashboardPort   (this PC only; add -DashboardLan for phones/LAN)"
    }
    Write-Host "  Dashboard token: $bin secret show --dashboard-token"
    Write-Host "  Cluster secret : $secretShown"
    Write-Host "      Copy this file to every worker (scp or USB), to %USERPROFILE%\.oxideswarm\cluster.secret."
    Write-Host "  Local worker   : $workerNote"
    Write-Host "  Master log     : $masterLog"
    if ($P2p) {
        if (Test-Path -LiteralPath $TicketFile) {
            Write-Host "  P2P ticket     : $((Get-Content -LiteralPath $TicketFile -Raw).Trim())"
        } else {
            Write-Host "  P2P ticket     : not ready yet; it will appear in $TicketFile (needs Internet to the relay)"
        }
    } else {
        Write-Host "  Other networks : start again with -P2p (switch_role.cmd master --p2p) to get a P2P ticket."
    }
    Write-Host "  Note: port $ClusterPort is authenticated by the secret but NOT encrypted; use --p2p or a VPN on untrusted networks."
    Write-Host "=============================================================================="
}

function Start-Worker {
    $bin = Find-Binary
    Stop-Tracked -Name "worker"
    $name = if ($WorkerName) { $WorkerName } else { "$($env:COMPUTERNAME.ToLower())-worker" }
    $argString = "worker --master $MasterAddr --name `"$name`"" + (Get-SecretArgs)
    if ($ExtraArgs) { $argString += " $ExtraArgs" }

    Write-Host "[OK] Starting OxideSwarm Worker silently in background..."
    Write-Host "     Target Master : $MasterAddr  (cluster port $ClusterPort; 'auto' = LAN discovery)"
    Write-Host "     Worker Name   : $name"
    $proc = Start-Tracked -Name "worker" -Binary $bin -ArgString $argString
    Start-Sleep -Seconds 2
    if ($proc.HasExited) {
        Write-Host "[ERROR] The worker exited right after starting (see below)." -ForegroundColor Red
        Write-Host "        If it complains about the cluster secret: copy the master's cluster.secret to"
        Write-Host "        %USERPROFILE%\.oxideswarm\cluster.secret (scp or USB) or pass -SecretFile."
        Show-LogTail -Path (Join-Path $RunDir "worker.err.log")
        Show-LogTail -Path (Join-Path $RunDir "worker.log")
        Remove-Item -LiteralPath (Join-Path $RunDir "worker.pid") -Force -ErrorAction SilentlyContinue
        exit 1
    }
    Write-Host "[STARTED] Worker running (PID $($proc.Id)). Log: $RunDir\worker.log"
    if (Wait-Registered -Path (Join-Path $RunDir "worker.log") -Seconds 10 -Proc $proc) {
        Write-Host "          Registered with the master ('Registration accepted' in the log)."
    } else {
        Write-Host "          NOT registered after 10 s. The worker keeps retrying; check the master address (port $ClusterPort), that the master is running and the cluster secret matches." -ForegroundColor Yellow
        Show-LogTail -Path (Join-Path $RunDir "worker.log")
    }
    Write-Host "          GPU: see the 'GPU:' line in the worker log. It is used only if rusty-grid.exe was built with --features gpu-wgpu (build_windows_worker.sh --gpu); -ExtraArgs '--no-gpu' keeps the card out of the grid."
}

function Show-Status {
    Write-Host "=============================================================================="
    Write-Host "                 OxideSwarm Windows Node Status"
    Write-Host "=============================================================================="
    $any = $false
    foreach ($n in @("master", "worker")) {
        $trackedPid = Get-TrackedPid -Name $n
        if ($trackedPid) {
            $any = $true
            $proc = Get-Process -Id $trackedPid
            $mb = [Math]::Round($proc.WorkingSet64 / 1MB, 1)
            Write-Host "  $n : RUNNING  PID $trackedPid  working set $mb MB  log: $RunDir\$n.log"
        } else {
            Write-Host "  $n : not running (via these launchers)"
        }
    }
    $svc = Get-Service -Name "OxideSwarmWorker" -ErrorAction SilentlyContinue
    if ($svc) {
        Write-Host "  Windows service OxideSwarmWorker: $($svc.Status)"
    }
    if (Test-Path -LiteralPath $DefaultSecretPath) {
        Write-Host "  Cluster secret: $DefaultSecretPath"
    } else {
        Write-Host "  Cluster secret: missing ($DefaultSecretPath); a master creates it on first start, a worker needs a copy."
    }
    $workerLog = Join-Path $RunDir "worker.log"
    if (Test-Path -LiteralPath $workerLog) {
        Write-Host ""
        Write-Host "--- Last 15 lines of worker.log ---"
        Get-Content -LiteralPath $workerLog -Tail 15
    }
    Write-Host "=============================================================================="
}

switch ($Action) {
    "start" {
        if ($Role -eq "master") { Start-Master }
        elseif ($Role -eq "worker") { Start-Worker }
        else { Write-Error "start needs a role: master or worker." }
    }
    "stop" {
        if ($Role -eq "master" -or $Role -eq "all") { Stop-Tracked -Name "master" }
        if ($Role -eq "worker" -or $Role -eq "all") { Stop-Tracked -Name "worker" }
        Write-Host "[OK] Stopped what these launchers had started (PID files in $RunDir)."
    }
    "status" {
        Show-Status
    }
}
