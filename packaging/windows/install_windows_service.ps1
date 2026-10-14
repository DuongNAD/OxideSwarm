<#
.SYNOPSIS
    Installs and configures OxideSwarm Distributed Grid Worker as a Windows Background Service.

.DESCRIPTION
    Production installer for OxideSwarm on Windows.
    - Registers rusty-grid.exe as a true Windows Service in Service Control Manager (SCM).
    - Sets StartupType = Automatic for silent boot-time execution in Session 0.
    - Configures failure recovery actions to automatically restart on crash after 5000ms.
    - Supports dual wrapping:
        a) NativeWrapper: Compiles zero-dependency ServiceWrapper.cs via built-in csc.exe.
        b) WinSW: Industry-standard XML-configured service wrapper (winsw.xml).
    - Deploys rusty-grid.exe and creates an oxideswarm.exe alias for brand identity.
    - Generates TOML configuration and initializes log directories in ProgramData.
    - Runs the service as a low-privilege account (virtual service account by default),
      NOT as LocalSystem: every task the master sends executes with this identity.
    - Keeps the cluster secret and any P2P ticket in ACL-restricted files, never on the
      command line, in the registry or in the SCM definition.
    - Re-running the installer with -Force upgrades or re-pairs an installed service.

.PARAMETER InstallDir
    Target installation directory. Default: "C:\Program Files\OxideSwarm"

.PARAMETER Master
    Address of the Master's CLUSTER port, e.g. "192.168.1.100:8088" (8088, never the
    dashboard port 8080). Required unless a P2P ticket is given.

.PARAMETER P2pTicket
    P2P connection ticket string from Master for NAT traversal. Prefer -P2pTicketFile.

.PARAMETER P2pTicketFile
    File containing the P2P ticket (read once; the file is not modified or deleted).

.PARAMETER SecretFile
    Cluster secret file copied from the master (default: $env:OXIDESWARM_SECRET_FILE, then
    %USERPROFILE%\.oxideswarm\cluster.secret). Required for any remote/P2P master unless
    -Insecure. Installed as <ConfigPath dir>\cluster.secret with an ACL limited to
    Administrators, SYSTEM and the service account.

.PARAMETER Insecure
    Allow a remote master WITHOUT a cluster secret (the worker then obeys any host that
    answers as the master). Not recommended.

.PARAMETER ServiceAccount
    Identity of the service and therefore of every task it runs:
      VirtualAccount (default)  NT SERVICE\<ServiceName>, no password, minimal rights
      LocalService              NT AUTHORITY\LocalService
      LocalSystem               full control of the machine; needs -AcceptSystemRisk

.PARAMETER AcceptSystemRisk
    Required together with -ServiceAccount LocalSystem.

.PARAMETER WorkerBin
    Path to compiled rusty-grid.exe binary.

.PARAMETER ServiceMethod
    Wrapping mechanism: "NativeWrapper" (default, zero downloads) or "WinSW".

.PARAMETER ServiceName
    SCM service identifier name. Default: "OxideSwarmWorker"

.PARAMETER DisplayName
    SCM service display name. Default: "OxideSwarm Distributed Grid Worker"

.PARAMETER WorkerName
    Custom worker identifier advertised to Master. Default: hostname

.PARAMETER Cores
    CPU cores override (0 = auto-detect host cores).

.PARAMETER RamMb
    RAM override in Megabytes (0 = auto-detect host RAM).

.PARAMETER Gpu
    Advertise physical GPU presence.

.PARAMETER SimulateGpu
    Advertise simulated GPU capability.

.PARAMETER GpuName
    Custom GPU model description string.

.PARAMETER MaxConcurrency
    Maximum concurrent task executions permitted.

.PARAMETER HeartbeatInterval
    Heartbeat interval in seconds. Default: 3

.PARAMETER LogDir
    Directory for worker log files. Default: "C:\ProgramData\OxideSwarm\logs"

.PARAMETER DataDir
    Directory for sandbox task scratch data. Default: "C:\ProgramData\OxideSwarm\data"

.PARAMETER ConfigPath
    Path to generated worker.toml configuration file. Default: "C:\ProgramData\OxideSwarm\rusty-grid.toml"

.PARAMETER WinSWBin
    Path to WinSW executable (required if ServiceMethod is WinSW and not found locally).

.PARAMETER StartImmediately
    Start the service immediately after registration. Default: $true

.PARAMETER Force
    Stop and replace an existing service of the same name (upgrade / re-pair).

.PARAMETER NoElevate
    Do not attempt automatic elevation if running without Administrator privileges.

.EXAMPLE
    .\install_windows_service.ps1 -Master "192.168.1.100:8088" -SecretFile "C:\path\cluster.secret" -WorkerBin ".\rusty-grid.exe"
.EXAMPLE
    .\install_windows_service.ps1 -P2pTicketFile "C:\path\ticket.txt" -SecretFile "C:\path\cluster.secret" -Force
#>

[CmdletBinding()]
param (
    [Parameter()]
    [string]$InstallDir = "C:\Program Files\OxideSwarm",

    [Parameter()]
    [string]$Master = "",

    [Parameter()]
    [string]$P2pTicket = "",

    [Parameter()]
    [string]$P2pTicketFile = "",

    [Parameter()]
    [string]$SecretFile = "",

    [Parameter()]
    [switch]$Insecure,

    [Parameter()]
    [ValidateSet("VirtualAccount", "LocalService", "LocalSystem")]
    [string]$ServiceAccount = "VirtualAccount",

    [Parameter()]
    [switch]$AcceptSystemRisk,

    [Parameter()]
    [string]$WorkerBin = "",

    [Parameter()]
    [ValidateSet("NativeWrapper", "WinSW")]
    [string]$ServiceMethod = "NativeWrapper",

    [Parameter()]
    [string]$ServiceName = "OxideSwarmWorker",

    [Parameter()]
    [string]$DisplayName = "OxideSwarm Distributed Grid Worker",

    [Parameter()]
    [string]$Description = "Automated background compute worker for the OxideSwarm distributed grid, executing general master-worker tasks, distributed compilation, and GPU workloads.",

    [Parameter()]
    [string]$WorkerName = "",

    [Parameter()]
    [int]$Cores = 0,

    [Parameter()]
    [long]$RamMb = 0,

    [Parameter()]
    [switch]$Gpu,

    [Parameter()]
    [switch]$NoGpu,

    [Parameter()]
    [switch]$SimulateGpu,

    [Parameter()]
    [string]$GpuName = "",

    [Parameter()]
    [int]$MaxConcurrency = 0,

    [Parameter()]
    [int]$HeartbeatInterval = 3,

    [Parameter()]
    [string]$LogDir = "C:\ProgramData\OxideSwarm\logs",

    [Parameter()]
    [string]$DataDir = "C:\ProgramData\OxideSwarm\data",

    [Parameter()]
    [string]$ConfigPath = "C:\ProgramData\OxideSwarm\rusty-grid.toml",

    [Parameter()]
    [string]$WinSWBin = "",

    [Parameter()]
    [bool]$StartImmediately = $true,

    [Parameter()]
    [switch]$Force,

    [Parameter()]
    [switch]$NoElevate
)

$ErrorActionPreference = "Stop"

# Trim trailing backslashes in path arguments before string quoting to prevent \" escaping issues under Windows CommandLineToArgvW
if ($InstallDir) { $InstallDir = $InstallDir.TrimEnd('\') }
if ($WorkerBin) { $WorkerBin = $WorkerBin.TrimEnd('\') }
if ($LogDir) { $LogDir = $LogDir.TrimEnd('\') }
if ($DataDir) { $DataDir = $DataDir.TrimEnd('\') }
if ($ConfigPath) { $ConfigPath = $ConfigPath.TrimEnd('\') }
if ($WinSWBin) { $WinSWBin = $WinSWBin.TrimEnd('\') }
if ($SecretFile) { $SecretFile = $SecretFile.TrimEnd('\') }
if ($P2pTicketFile) { $P2pTicketFile = $P2pTicketFile.TrimEnd('\') }

# ==============================================================================
# 1. Administrator Elevation Check
# ==============================================================================
function Assert-Administrator {
    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
    $isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

    if (-not $isAdmin) {
        if (-not $NoElevate -and [Environment]::UserInteractive) {
            Write-Host "[INFO] Requesting Administrator privileges to configure Windows Service..." -ForegroundColor Yellow
            $scriptPath = $MyInvocation.PSCommandPath
            if (-not $scriptPath) {
                $scriptPath = $PSCommandPath
            }

            # Build argument string passing forward all parameters
            $argList = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
            foreach ($key in $script:PSBoundParameters.Keys) {
                $val = $script:PSBoundParameters[$key]
                if ($val -is [System.Management.Automation.SwitchParameter]) {
                    if ($val.IsPresent) { $argList += " -$key" }
                } elseif ($val -is [bool]) {
                    $argList += " -$key `$$val"
                } else {
                    $valStr = if ($val -is [string]) { $val.TrimEnd('\') } else { [string]$val }
                    # Embedded double quotes (a JSON ticket given via -P2pTicket) must survive CommandLineToArgvW.
                    $valStr = $valStr.Replace('"', '\"')
                    $argList += " -$key `"$valStr`""
                }
            }

            try {
                Start-Process powershell.exe -Verb RunAs -ArgumentList $argList
                exit 0
            } catch {
                Write-Error "Failed to elevate privileges: $($_.Exception.Message)"
                exit 1
            }
        } else {
            Write-Error "Administrator privileges are required to register and manage Windows Services. Please launch PowerShell as Administrator."
            exit 1
        }
    }
}

Assert-Administrator

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host "  OxideSwarm Windows Background Service Installer" -ForegroundColor Cyan
Write-Host "========================================================`n" -ForegroundColor Cyan

# ==============================================================================
# 2. Resolve Pairing Inputs (ticket, master, cluster secret, service identity)
# ==============================================================================

# Whether a worker must hold the cluster secret to follow this master (mirrors the CLI rule):
# everything except a master on this very machine needs one, because without it any host that
# answers at that address could become the master and run commands on this PC.
function Test-MasterNeedsSecret {
    param (
        [string]$Address,
        [bool]$ViaTicket
    )
    if ($ViaTicket) { return $true }
    if (-not $Address) { return $true }
    $hostPart = $Address.Trim()
    if ($hostPart -match '^\[(.*)\](:\d+)?$') {
        $hostPart = $Matches[1]
    } elseif ($hostPart -match '^(.*):\d+$') {
        $hostPart = $Matches[1]
    }
    $hostPart = $hostPart.TrimEnd('.').ToLowerInvariant()
    if ($hostPart -eq 'localhost' -or $hostPart.EndsWith('.localhost') -or $hostPart -eq '::1' -or $hostPart -match '^127\.') {
        return $false
    }
    return $true
}

if ($P2pTicketFile) {
    if (-not (Test-Path -LiteralPath $P2pTicketFile)) {
        Write-Error "P2P ticket file not found: $P2pTicketFile"
        exit 1
    }
    $P2pTicket = (Get-Content -LiteralPath $P2pTicketFile -Raw).Trim().TrimStart([char]0xFEFF).Trim()
}
$P2pTicket = $P2pTicket.Trim()

if ($P2pTicket) {
    if ($P2pTicket.Contains("'")) {
        Write-Error "The P2P ticket contains a single quote and cannot be stored in the TOML configuration."
        exit 1
    }
    $ticketObject = $null
    try { $ticketObject = $P2pTicket | ConvertFrom-Json } catch { $ticketObject = $null }
    if (-not $ticketObject -or -not $ticketObject.id) {
        Write-Error "Invalid P2P ticket: expected a JSON object with an 'id' field, e.g. {`"id`":`"...`",`"addrs`":[...]}."
        exit 1
    }
}

if (-not $Master -and -not $P2pTicket) {
    Write-Error "Nothing to connect to: pass -Master HOST:8088 (the master's CLUSTER port) or a P2P ticket (-P2pTicketFile)."
    exit 1
}
if ($Master -match ':8080$') {
    Write-Warning "Port 8080 is the master's DASHBOARD, not its cluster port. Workers must connect to port 8088, e.g. -Master 192.168.1.100:8088."
}

$resolvedSecret = ""
$secretCandidate = $SecretFile
if (-not $secretCandidate) { $secretCandidate = $env:OXIDESWARM_SECRET_FILE }
if (-not $secretCandidate -and $env:USERPROFILE) { $secretCandidate = Join-Path $env:USERPROFILE ".oxideswarm\cluster.secret" }
if ($secretCandidate -and (Test-Path -LiteralPath $secretCandidate)) {
    $resolvedSecret = (Resolve-Path -LiteralPath $secretCandidate).Path
} elseif ($SecretFile) {
    Write-Error "Cluster secret file not found: $SecretFile"
    exit 1
}

$needsSecret = Test-MasterNeedsSecret -Address $Master -ViaTicket ([bool]$P2pTicket)
if ($needsSecret -and -not $resolvedSecret -and -not $Insecure) {
    $secretMsg = @"
No cluster secret found. A master address or P2P ticket only says WHERE the master is; the secret is what proves it is the real one.
Copy the master's secret to this PC over a trusted channel (scp, USB stick; never chat or email), for example:
  scp user@master:~/.oxideswarm/cluster.secret "$env:USERPROFILE\.oxideswarm\cluster.secret"
then re-run with -SecretFile "<path>". To continue WITHOUT authenticating the master, pass -Insecure (not recommended).
"@
    Write-Error $secretMsg
    exit 1
}
if ($Insecure) {
    Write-Warning "-Insecure: this worker will NOT authenticate its master and will obey any host that answers as the master."
}

# Identity of the service == identity of every task process it spawns.
$serviceStartName = ""
$aclPrincipal = ""
switch ($ServiceAccount) {
    "VirtualAccount" {
        $serviceStartName = "NT SERVICE\$ServiceName"
        $aclPrincipal = "NT SERVICE\$ServiceName"
    }
    "LocalService" {
        $serviceStartName = "NT AUTHORITY\LocalService"
        $aclPrincipal = "*S-1-5-19"
    }
    "LocalSystem" {
        if (-not $AcceptSystemRisk) {
            Write-Error "Running as LocalSystem gives every task the master dispatches full control of this PC. Re-run with -AcceptSystemRisk to confirm, or keep the default low-privilege -ServiceAccount VirtualAccount."
            exit 1
        }
        Write-Warning "Service will run as LocalSystem: every task the master sends executes with full control of this machine."
        $aclPrincipal = "*S-1-5-18"
    }
}
Write-Host "[INFO] Service identity: $(if ($serviceStartName) { $serviceStartName } else { 'LocalSystem' })" -ForegroundColor Cyan

# ==============================================================================
# 3. Resolve Worker Executable Binary
# ==============================================================================
$resolvedWorkerBin = ""

if ($WorkerBin -and (Test-Path $WorkerBin)) {
    $resolvedWorkerBin = (Resolve-Path $WorkerBin).Path
} else {
    # Check default relative paths from script directory or repo layout
    $candidates = @(
        (Join-Path $PSScriptRoot "rusty-grid.exe"),
        (Join-Path $PSScriptRoot "..\..\target\x86_64-pc-windows-gnu\release\rusty-grid.exe"),
        (Join-Path $PSScriptRoot "..\..\target\x86_64-pc-windows-gnu\debug\rusty-grid.exe"),
        (Join-Path $PSScriptRoot "..\..\target\release\rusty-grid.exe"),
        (Join-Path $InstallDir "bin\rusty-grid.exe"),
        ".\rusty-grid.exe"
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            $resolvedWorkerBin = (Resolve-Path $candidate).Path
            break
        }
    }
}

if (-not $resolvedWorkerBin -or -not (Test-Path $resolvedWorkerBin)) {
    Write-Warning "Could not find 'rusty-grid.exe' automatically."
    Write-Warning "Specify the binary path using: -WorkerBin <Path\to\rusty-grid.exe>"
    if (-not (Test-Path (Join-Path $InstallDir "bin\rusty-grid.exe"))) {
        Write-Error "Cannot proceed without a valid worker binary. Cross-compile first using build_windows_worker.sh or provide -WorkerBin."
        exit 1
    }
    $resolvedWorkerBin = Join-Path $InstallDir "bin\rusty-grid.exe"
    Write-Host "[INFO] Using previously installed binary at: $resolvedWorkerBin" -ForegroundColor Yellow
} else {
    Write-Host "[OK] Located worker binary: $resolvedWorkerBin" -ForegroundColor Green
}

# ==============================================================================
# 4. Stop and Remove an Existing Service BEFORE touching any file
#    (a running service locks rusty-grid.exe, so copying over it first would abort the script
#    halfway and leave a new config next to an old service)
# ==============================================================================
$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingService) {
    if (-not $Force) {
        Write-Error "Service '$ServiceName' already exists (Status: $($existingService.Status)). Nothing was changed. Re-run with -Force to stop and replace it (upgrade or re-pair)."
        exit 1
    }
    Write-Host "[INFO] Existing service '$ServiceName' detected (Status: $($existingService.Status)). -Force specified, replacing it..." -ForegroundColor Yellow
    if ($existingService.Status -ne 'Stopped') {
        Write-Host "[INFO] Stopping existing service '$ServiceName'..." -ForegroundColor Yellow
        Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
        try {
            $existingService.WaitForStatus('Stopped', (New-TimeSpan -Seconds 20))
        } catch {
            Write-Warning "Service '$ServiceName' did not report Stopped within 20 s; continuing."
        }
    }
    Write-Host "[INFO] Removing existing service from SCM..." -ForegroundColor Yellow
    sc.exe delete $ServiceName | Out-Null
    for ($i = 0; $i -lt 15; $i++) {
        if (-not (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue)) { break }
        Start-Sleep -Seconds 1
    }
    if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        Write-Warning "Service '$ServiceName' is still marked for deletion (open handles). If creating it fails below, close Services/Event Viewer windows and re-run."
    }
}

# ==============================================================================
# 5. Create Filesystem Directory Hierarchy
# ==============================================================================
$binDir = Join-Path $InstallDir "bin"
$configParent = Split-Path -Path $ConfigPath -Parent

$requiredDirs = @($InstallDir, $binDir, $LogDir, $DataDir, $configParent) | Select-Object -Unique

foreach ($dir in $requiredDirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "[OK] Created directory: $dir" -ForegroundColor Green
    }
}

# ==============================================================================
# 6. Deploy Executable Binaries
# ==============================================================================
$destWorkerBin = Join-Path $binDir "rusty-grid.exe"
$destAliasBin  = Join-Path $binDir "oxideswarm.exe"

function Copy-FileWithRetry {
    param (
        [string]$Source,
        [string]$Destination,
        [int]$Attempts = 10
    )
    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            Copy-Item -LiteralPath $Source -Destination $Destination -Force -ErrorAction Stop
            return
        } catch {
            if ($i -eq $Attempts) { throw }
            Start-Sleep -Seconds 1
        }
    }
}

# Copy primary binary
if ((Test-Path $resolvedWorkerBin) -and ($resolvedWorkerBin -ne $destWorkerBin)) {
    Write-Host "[INFO] Deploying $resolvedWorkerBin -> $destWorkerBin..."
    Copy-FileWithRetry -Source $resolvedWorkerBin -Destination $destWorkerBin
}
Write-Host "[OK] Primary worker binary: $destWorkerBin" -ForegroundColor Green

# Create brand alias (oxideswarm.exe)
if (Test-Path $destWorkerBin) {
    try {
        Copy-FileWithRetry -Source $destWorkerBin -Destination $destAliasBin
        Write-Host "[OK] Deployed brand alias: $destAliasBin" -ForegroundColor Green
    } catch {
        Write-Warning "Could not create oxideswarm.exe alias: $($_.Exception.Message)"
    }
}

# ==============================================================================
# 7. Generate Configuration File (rusty-grid.toml) and install the cluster secret
# ==============================================================================
Write-Host "[INFO] Generating configuration at $ConfigPath..."

$escapedDataDir = $DataDir.Replace('\', '/')

$secretTarget = ""
if ($resolvedSecret) {
    $secretTarget = Join-Path $configParent "cluster.secret"
    if ($resolvedSecret -ne $secretTarget) {
        Copy-Item -LiteralPath $resolvedSecret -Destination $secretTarget -Force
    }
    Write-Host "[OK] Cluster secret installed at: $secretTarget" -ForegroundColor Green
}

$tomlLines = @(
    "# OxideSwarm Distributed Grid Worker Configuration",
    "# Auto-generated by install_windows_service.ps1 (contains the P2P ticket: keep the ACL restricted)",
    "",
    "[worker]"
)

if ($Master) {
    $tomlLines += "master = `"$Master`""
}

$tomlLines += @(
    "heartbeat_interval_secs = $HeartbeatInterval",
    "sandbox_base_dir = `"$escapedDataDir`"",
    "keep_sandboxes = false"
)

# A worker keeps the same worker id across restarts only when it has a name; a service the SCM
# respawns must not show up as a new machine each time.
if (-not $WorkerName) { $WorkerName = $env:COMPUTERNAME }
if ($WorkerName) {
    $tomlLines += "name = `"$WorkerName`""
}

# The ticket lives only in this ACL-restricted file. It is deliberately NOT passed to the
# service as a --p2p-ticket command-line argument or stored in the registry: the command line is
# visible to every local user, and CommandLineToArgvW strips the quotes of a JSON ticket.
if ($P2pTicket) {
    $tomlLines += "p2p_ticket = '$P2pTicket'"
}

if ($secretTarget) {
    $tomlLines += "secret_file = '$secretTarget'"
}

if ($Insecure) {
    $tomlLines += "insecure = true"
}

if ($MaxConcurrency -gt 0) {
    $tomlLines += "max_concurrency = $MaxConcurrency"
}

if ($NoGpu) {
    $tomlLines += "no_gpu = true"
}

# Hardware overrides section
$hasHardware = ($Cores -gt 0) -or ($RamMb -gt 0) -or $Gpu.IsPresent -or $SimulateGpu.IsPresent -or $GpuName
if ($hasHardware) {
    $tomlLines += ""
    $tomlLines += "[worker.hardware]"
    if ($Cores -gt 0) { $tomlLines += "cores = $Cores" }
    if ($RamMb -gt 0) { $tomlLines += "ram_mb = $RamMb" }
    if ($Gpu)         { $tomlLines += "gpu = true" }
    if ($SimulateGpu) { $tomlLines += "simulate_gpu = true" }
    if ($GpuName)     { $tomlLines += "gpu_name = `"$GpuName`"" }
}

$tomlContent = ($tomlLines -join "`r`n") + "`r`n"
# UTF-8 without BOM: Set-Content -Encoding UTF8 would prepend one on Windows PowerShell 5.1.
[System.IO.File]::WriteAllText($ConfigPath, $tomlContent, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "[OK] Configuration written to: $ConfigPath" -ForegroundColor Green

# ==============================================================================
# 8. Service Wrapper Setup (NativeWrapper or WinSW)
# ==============================================================================
$serviceExePath = ""

if ($ServiceMethod -eq "WinSW") {
    Write-Host "`n--- Configuring WinSW Service Wrapper ---" -ForegroundColor Cyan
    $serviceExePath = Join-Path $InstallDir "$ServiceName.exe"
    $serviceXmlPath = Join-Path $InstallDir "$ServiceName.xml"

    # Locate WinSW binary
    $resolvedWinSW = ""
    if ($WinSWBin -and (Test-Path $WinSWBin)) {
        $resolvedWinSW = (Resolve-Path $WinSWBin).Path
    } else {
        $candidateWinSW = @(
            (Join-Path $PSScriptRoot "WinSW-x64.exe"),
            (Join-Path $PSScriptRoot "winsw.exe"),
            (Join-Path $InstallDir "$ServiceName.exe")
        )
        foreach ($c in $candidateWinSW) {
            if (Test-Path $c) {
                $resolvedWinSW = (Resolve-Path $c).Path
                break
            }
        }
    }

    if (-not $resolvedWinSW) {
        Write-Warning "WinSW executable not provided or found locally."
        Write-Host "[INFO] Falling back automatically to zero-dependency NativeWrapper..." -ForegroundColor Yellow
        $ServiceMethod = "NativeWrapper"
    } else {
        Write-Host "[INFO] Copying WinSW binary to $serviceExePath..."
        if ($resolvedWinSW -ne $serviceExePath) {
            Copy-FileWithRetry -Source $resolvedWinSW -Destination $serviceExePath
        }

        # Prepare WinSW XML config
        $templateXml = Join-Path $PSScriptRoot "winsw.xml"

        # Configuration only: the ticket and the secret path come from $ConfigPath.
        $serviceArguments = "worker --config `"$ConfigPath`""

        if (Test-Path $templateXml) {
            [xml]$xmlDoc = Get-Content $templateXml
            $xmlDoc.service.id = $ServiceName
            $xmlDoc.service.name = $DisplayName
            $xmlDoc.service.description = $Description
            $xmlDoc.service.executable = $destWorkerBin
            $xmlDoc.service.arguments = $serviceArguments
            $xmlDoc.service.workingdirectory = $InstallDir
            if ($xmlDoc.service.log) {
                $xmlDoc.service.log.logpath = $LogDir
            }
            $xmlDoc.Save($serviceXmlPath)
        } else {
            # Generate XML directly
            $xmlContent = @"
<service>
  <id>$ServiceName</id>
  <name>$DisplayName</name>
  <description>$Description</description>
  <executable>$destWorkerBin</executable>
  <arguments>$serviceArguments</arguments>
  <workingdirectory>$InstallDir</workingdirectory>
  <priority>Normal</priority>
  <stoptimeout>15 sec</stoptimeout>
  <stopparentprocessfirst>true</stopparentprocessfirst>
  <startmode>Automatic</startmode>
  <delayedAutoStart>false</delayedAutoStart>
  <onfailure action="restart" delay="5 sec"/>
  <onfailure action="restart" delay="5 sec"/>
  <onfailure action="restart" delay="10 sec"/>
  <resetfailure>1 day</resetfailure>
  <log mode="roll-by-size">
    <logpath>$LogDir</logpath>
    <sizeThreshold>10240</sizeThreshold>
    <keepFiles>5</keepFiles>
  </log>
  <env name="RUST_LOG" value="info" />
</service>
"@
            [System.IO.File]::WriteAllText($serviceXmlPath, $xmlContent, (New-Object System.Text.UTF8Encoding($false)))
        }
        Write-Host "[OK] Configured WinSW XML: $serviceXmlPath" -ForegroundColor Green

        # Register service via WinSW install
        Write-Host "[INFO] Registering service with WinSW..."
        $installOut = & "$serviceExePath" install 2>&1 | Out-String
        Write-Host $installOut
    }
}

if ($ServiceMethod -eq "NativeWrapper") {
    Write-Host "`n--- Compiling Zero-Dependency Native C# Service Wrapper ---" -ForegroundColor Cyan
    $serviceExePath = Join-Path $InstallDir "OxideSwarmService.exe"

    # Locate csc.exe
    $cscCandidates = @(
        "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe",
        "C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe",
        (Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\csc.exe"),
        (Join-Path $env:WINDIR "Microsoft.NET\Framework\v4.0.30319\csc.exe")
    )

    $cscPath = ""
    foreach ($c in $cscCandidates) {
        if (Test-Path $c) {
            $cscPath = $c
            break
        }
    }

    if (-not $cscPath) {
        # Try finding csc in PATH
        $cscCmd = Get-Command "csc.exe" -ErrorAction SilentlyContinue
        if ($cscCmd) {
            $cscPath = $cscCmd.Source
        }
    }

    if (-not $cscPath) {
        Write-Error "Could not locate .NET Framework C# compiler (csc.exe). Verify .NET Framework 4.x is installed."
        exit 1
    }

    Write-Host "[OK] Located C# compiler: $cscPath" -ForegroundColor Green

    # Locate ServiceWrapper.cs source
    $csSourceFile = Join-Path $PSScriptRoot "ServiceWrapper.cs"
    if (-not (Test-Path $csSourceFile)) {
        $csSourceFile = Join-Path $InstallDir "ServiceWrapper.cs"
    }

    if (-not (Test-Path $csSourceFile)) {
        Write-Error "Could not locate ServiceWrapper.cs in '$PSScriptRoot' or '$InstallDir'."
        exit 1
    }

    # Copy source to install directory for reference
    $deployedCs = Join-Path $InstallDir "ServiceWrapper.cs"
    if ($csSourceFile -ne $deployedCs) {
        Copy-Item -Path $csSourceFile -Destination $deployedCs -Force
    }

    Write-Host "[INFO] Compiling $deployedCs -> $serviceExePath..."
    $cscArgs = @(
        "/target:winexe",
        "/optimize+",
        "/platform:anycpu",
        "/reference:System.dll",
        "/reference:System.Core.dll",
        "/reference:System.ServiceProcess.dll",
        "/out:`"$serviceExePath`"",
        "`"$deployedCs`""
    )

    $compileProcess = Start-Process -FilePath $cscPath -ArgumentList ($cscArgs -join " ") -Wait -NoNewWindow -PassThru
    if ($compileProcess.ExitCode -ne 0 -or -not (Test-Path $serviceExePath)) {
        Write-Error "Compilation of ServiceWrapper.cs failed with exit code $($compileProcess.ExitCode)."
        exit 1
    }

    Write-Host "[OK] Compiled native Windows SCM wrapper: $serviceExePath" -ForegroundColor Green

    # Register Service in SCM via sc.exe create
    Write-Host "[INFO] Registering '$ServiceName' in Service Control Manager..."
    $binPathArg = "`"$serviceExePath`""
    $createResult = sc.exe create $ServiceName binPath= $binPathArg start= auto DisplayName= $DisplayName 2>&1 | Out-String
    Write-Host $createResult

    if ($LASTEXITCODE -ne 0) {
        Write-Error "sc.exe create failed with exit code $LASTEXITCODE. Output: $createResult"
        exit 1
    }

    # Set service description
    sc.exe description $ServiceName $Description | Out-Null
}

# ==============================================================================
# 9. Service Identity and Access Control
#    (after registration: the NT SERVICE\<name> account only resolves once the service exists)
# ==============================================================================
if (-not (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue)) {
    Write-Error "Service '$ServiceName' is not registered in the SCM after installation; see the output above."
    exit 1
}

if ($serviceStartName) {
    Write-Host "[INFO] Configuring service identity: $serviceStartName" -ForegroundColor Cyan
    $identityResult = sc.exe config $ServiceName obj= $serviceStartName 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        Write-Error "sc.exe config obj= failed with exit code $LASTEXITCODE. Output: $identityResult"
        exit 1
    }
    Write-Host "[OK] Service runs as $serviceStartName (tasks inherit this low-privilege identity)." -ForegroundColor Green
}

# Config and secret: readable by SYSTEM, Administrators and the service account only.
function Set-PrivateFileAcl {
    param (
        [string]$Path,
        [string]$Principal
    )
    $grants = @("*S-1-5-18:(F)", "*S-1-5-32-544:(F)")
    if ($Principal -and $Principal -ne "*S-1-5-18") {
        $grants += ($Principal + ":(R)")
    }
    & icacls.exe $Path /inheritance:r /grant:r $grants | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Could not restrict the ACL of $Path (icacls exit code $LASTEXITCODE). Restrict it manually: it holds the cluster secret / P2P ticket."
    }
}

# Log and sandbox directories: the service account must be able to write there.
function Grant-ServiceModify {
    param (
        [string]$Path,
        [string]$Principal
    )
    if (-not $Principal -or $Principal -eq "*S-1-5-18") { return }
    & icacls.exe $Path /grant ($Principal + ":(OI)(CI)M") | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Could not grant the service account write access to $Path (icacls exit code $LASTEXITCODE)."
    }
}

Set-PrivateFileAcl -Path $ConfigPath -Principal $aclPrincipal
if ($secretTarget) {
    Set-PrivateFileAcl -Path $secretTarget -Principal $aclPrincipal
}
Grant-ServiceModify -Path $LogDir -Principal $aclPrincipal
Grant-ServiceModify -Path $DataDir -Principal $aclPrincipal
Write-Host "[OK] Applied ACLs (config/secret private; log and sandbox directories writable by the service account)." -ForegroundColor Green

# ==============================================================================
# 10. Configure SCM Recovery Actions & Auto-Start
# ==============================================================================
Write-Host "[INFO] Configuring SCM failure recovery (auto-restart after 5000ms)..."
# Reset failure counter after 86400 seconds (1 day)
# Action: restart on 1st failure (5000ms), 2nd failure (5000ms), subsequent (5000ms)
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null
sc.exe failureflag $ServiceName 1 | Out-Null
Write-Host "[OK] SCM Failure Recovery configured successfully." -ForegroundColor Green

# Set Windows Environment Variables for wrapper discovery if needed (paths only: no secrets here).
[Environment]::SetEnvironmentVariable("OXIDESWARM_BIN", $destWorkerBin, [EnvironmentVariableTarget]::Machine)
[Environment]::SetEnvironmentVariable("OXIDESWARM_CONFIG", $ConfigPath, [EnvironmentVariableTarget]::Machine)
[Environment]::SetEnvironmentVariable("OXIDESWARM_LOG_DIR", $LogDir, [EnvironmentVariableTarget]::Machine)
# Older installers stored the ticket machine-wide, where it overrode every later configuration.
[Environment]::SetEnvironmentVariable("OXIDESWARM_P2P_TICKET", $null, [EnvironmentVariableTarget]::Machine)
Write-Host "[OK] Configured system environment variables for worker." -ForegroundColor Green

# ==============================================================================
# 11. Start Service and Verify Health
# ==============================================================================
$overallPassed = $true
$registered = $false

# Only log output written after this point counts as evidence from this start.
$workerOutLog = Join-Path $LogDir "worker.out.log"
$logOffset = 0
if (Test-Path -LiteralPath $workerOutLog) {
    $logOffset = (Get-Item -LiteralPath $workerOutLog).Length
}

function Test-LogContainsSince {
    param (
        [string]$Path,
        [long]$Offset,
        [string]$Pattern
    )
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $stream = $null
    try {
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        if ($stream.Length -le $Offset) { return $false }
        $null = $stream.Seek($Offset, [System.IO.SeekOrigin]::Begin)
        $reader = New-Object System.IO.StreamReader($stream)
        $text = $reader.ReadToEnd()
        return ($text -match $Pattern)
    } catch {
        return $false
    } finally {
        if ($stream) { $stream.Dispose() }
    }
}

if ($StartImmediately) {
    Write-Host "`n[INFO] Starting service '$ServiceName'..." -ForegroundColor Cyan
    try {
        Start-Service -Name $ServiceName -ErrorAction Stop
    } catch {
        Write-Warning "Start-Service returned an error: $($_.Exception.Message)"
    }

    # Polling wait for Running status
    $maxWaitSec = 10
    $elapsed = 0
    $svc = $null
    while ($elapsed -lt $maxWaitSec) {
        Start-Sleep -Seconds 1
        $elapsed++
        $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($svc -and $svc.Status -eq 'Running') {
            break
        }
    }

    if ($svc -and $svc.Status -eq 'Running') {
        Write-Host "[OK] Service '$ServiceName' is active and RUNNING in Session 0." -ForegroundColor Green

        # A running service is not a connected worker: wait for the master to accept the registration.
        Write-Host "[INFO] Waiting up to 20 s for the master to accept this worker..." -ForegroundColor Cyan
        for ($i = 0; $i -lt 20; $i++) {
            if (Test-LogContainsSince -Path $workerOutLog -Offset $logOffset -Pattern "Registration accepted") {
                $registered = $true
                break
            }
            Start-Sleep -Seconds 1
        }
        if ($registered) {
            Write-Host "[OK] Worker registered with the master ('Registration accepted' in $workerOutLog)." -ForegroundColor Green
        } else {
            Write-Warning "The service is running but no 'Registration accepted' appeared in $workerOutLog yet."
            Write-Warning "Check the log for 'Invalid P2P ticket', 'Authentication' or connection errors; confirm the master address/port (8088), the cluster secret and that the master is reachable."
        }
    } else {
        $overallPassed = $false
        Write-Error "Service status after $elapsed seconds: $($svc.Status). Failed to start service '$ServiceName'."
        Write-Warning "Check logs at '$LogDir' or Windows Event Viewer (Application log) for diagnostics."
    }
}

# ==============================================================================
# 12. Summary Banner
# ==============================================================================
if ($overallPassed) {
    Write-Host "`n========================================================" -ForegroundColor Green
    Write-Host "  OxideSwarm Service Installation Succeeded" -ForegroundColor Green
    Write-Host "========================================================" -ForegroundColor Green
    Write-Host "  Service Name:    $ServiceName"
    Write-Host "  Display Name:    $DisplayName"
    Write-Host "  Startup Type:    Automatic (Boot-time Session 0)"
    Write-Host "  Runs As:         $(if ($serviceStartName) { $serviceStartName } else { 'LocalSystem' })"
    Write-Host "  Binary Path:     $destWorkerBin"
    Write-Host "  Service Wrapper: $serviceExePath"
    Write-Host "  Configuration:   $ConfigPath"
    Write-Host "  Logs Directory:  $LogDir"
    Write-Host "  Data Sandbox:    $DataDir"
    Write-Host "  Master:          $(if ($P2pTicket) { 'P2P ticket (see configuration)' } else { $Master })"
    Write-Host "  Cluster secret:  $(if ($secretTarget) { $secretTarget } else { 'none (-Insecure or loopback master)' })"
    Write-Host "  Registered:      $(if ($registered) { 'yes' } else { 'not confirmed yet' })"
    Write-Host "========================================================`n" -ForegroundColor Green
    Write-Host "To verify installation, run:"
    Write-Host "  powershell -ExecutionPolicy Bypass -File .\verify_windows_service.ps1`n" -ForegroundColor Cyan
} else {
    Write-Error "OxideSwarm service installation completed with errors (service failed to start)."
    exit 1
}
