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

.PARAMETER InstallDir
    Target installation directory. Default: "C:\Program Files\OxideSwarm"

.PARAMETER Master
    Address of the Master coordinator node. Default: "127.0.0.1:8080"

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
    Overwrite existing installation and stop any conflicting service.

.PARAMETER NoElevate
    Do not attempt automatic elevation if running without Administrator privileges.

.EXAMPLE
    .\install_windows_service.ps1 -Master "192.168.1.100:8080" -WorkerBin ".\rusty-grid.exe"
#>

[CmdletBinding()]
param (
    [Parameter()]
    [string]$InstallDir = "C:\Program Files\OxideSwarm",

    [Parameter()]
    [string]$Master = "127.0.0.1:8080",

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
                    $valStr = if ($val -is [string]) { $val.TrimEnd('\') } else { $val }
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
# 2. Resolve Worker Executable Binary
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
# 3. Create Filesystem Directory Hierarchy
# ==============================================================================
$binDir = Join-Path $InstallDir "bin"
$configDir = Join-Path $InstallDir "config"
$configParent = Split-Path -Path $ConfigPath -Parent

$requiredDirs = @($InstallDir, $binDir, $configDir, $LogDir, $DataDir, $configParent) | Select-Object -Unique

foreach ($dir in $requiredDirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "[OK] Created directory: $dir" -ForegroundColor Green
    }
}

# ==============================================================================
# 4. Deploy Executable Binaries
# ==============================================================================
$destWorkerBin = Join-Path $binDir "rusty-grid.exe"
$destAliasBin  = Join-Path $binDir "oxideswarm.exe"

# Copy primary binary
if ((Test-Path $resolvedWorkerBin) -and ($resolvedWorkerBin -ne $destWorkerBin)) {
    Write-Host "[INFO] Deploying $resolvedWorkerBin -> $destWorkerBin..."
    Copy-Item -Path $resolvedWorkerBin -Destination $destWorkerBin -Force
}
Write-Host "[OK] Primary worker binary: $destWorkerBin" -ForegroundColor Green

# Create brand alias (oxideswarm.exe)
if (Test-Path $destWorkerBin) {
    try {
        Copy-Item -Path $destWorkerBin -Destination $destAliasBin -Force
        Write-Host "[OK] Deployed brand alias: $destAliasBin" -ForegroundColor Green
    } catch {
        Write-Warning "Could not create oxideswarm.exe alias: $($_.Exception.Message)"
    }
}

# ==============================================================================
# 5. Generate Configuration File (rusty-grid.toml)
# ==============================================================================
Write-Host "[INFO] Generating configuration at $ConfigPath..."

$escapedDataDir = $DataDir.Replace('\', '/')

$tomlLines = @(
    "# OxideSwarm Distributed Grid Worker Configuration",
    "# Auto-generated by install_windows_service.ps1",
    "",
    "[worker]",
    "master = `"$Master`"",
    "heartbeat_interval_secs = $HeartbeatInterval",
    "sandbox_base_dir = `"$escapedDataDir`"",
    "keep_sandboxes = false"
)

if ($WorkerName) {
    $tomlLines += "name = `"$WorkerName`""
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
Set-Content -Path $ConfigPath -Value $tomlContent -Encoding UTF8 -Force
Write-Host "[OK] Configuration written to: $ConfigPath" -ForegroundColor Green

# Also mirror configuration in $InstallDir\config\rusty-grid.toml for redundancy
$mirrorConfig = Join-Path $configDir "rusty-grid.toml"
if ($mirrorConfig -ne $ConfigPath) {
    Copy-Item -Path $ConfigPath -Destination $mirrorConfig -Force
    Write-Host "[OK] Mirrored configuration at: $mirrorConfig" -ForegroundColor Green
}

# ==============================================================================
# 6. Stop and Remove Existing Service if Present
# ==============================================================================
$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingService) {
    if ($Force) {
        Write-Host "[INFO] Existing service '$ServiceName' detected (Status: $($existingService.Status)). -Force specified, removing..." -ForegroundColor Yellow
        if ($existingService.Status -eq 'Running') {
            Write-Host "[INFO] Stopping existing service '$ServiceName'..." -ForegroundColor Yellow
            Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 2
        }

        Write-Host "[INFO] Removing existing service from SCM..." -ForegroundColor Yellow
        sc.exe delete $ServiceName | Out-Null
        Start-Sleep -Seconds 1
    } else {
        Write-Error "Service '$ServiceName' already exists (Status: $($existingService.Status)). Specify -Force to stop and overwrite the existing service."
        exit 1
    }
}

# ==============================================================================
# 7. Service Wrapper Setup (NativeWrapper or WinSW)
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
        Copy-Item -Path $resolvedWinSW -Destination $serviceExePath -Force

        # Prepare WinSW XML config
        $templateXml = Join-Path $PSScriptRoot "winsw.xml"
        if (-not (Test-Path $templateXml)) {
            $templateXml = Join-Path $configDir "winsw.xml"
        }

        if (Test-Path $templateXml) {
            [xml]$xmlDoc = Get-Content $templateXml
            $xmlDoc.service.id = $ServiceName
            $xmlDoc.service.name = $DisplayName
            $xmlDoc.service.description = $Description
            $xmlDoc.service.executable = $destWorkerBin
            $xmlDoc.service.arguments = "worker --config `"$ConfigPath`""
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
  <arguments>worker --config "$ConfigPath"</arguments>
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
            Set-Content -Path $serviceXmlPath -Value $xmlContent -Encoding UTF8
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
# 8. Configure SCM Recovery Actions & Auto-Start
# ==============================================================================
Write-Host "[INFO] Configuring SCM failure recovery (auto-restart after 5000ms)..."
# Reset failure counter after 86400 seconds (1 day)
# Action: restart on 1st failure (5000ms), 2nd failure (5000ms), subsequent (5000ms)
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null
sc.exe failureflag $ServiceName 1 | Out-Null
Write-Host "[OK] SCM Failure Recovery configured successfully." -ForegroundColor Green

# Set Windows Environment Variables for wrapper discovery if needed
[Environment]::SetEnvironmentVariable("OXIDESWARM_BIN", $destWorkerBin, [EnvironmentVariableTarget]::Machine)
[Environment]::SetEnvironmentVariable("OXIDESWARM_CONFIG", $ConfigPath, [EnvironmentVariableTarget]::Machine)
[Environment]::SetEnvironmentVariable("OXIDESWARM_LOG_DIR", $LogDir, [EnvironmentVariableTarget]::Machine)
Write-Host "[OK] Configured system environment variables for worker." -ForegroundColor Green

# ==============================================================================
# 9. Start Service and Verify Health
# ==============================================================================
$overallPassed = $true

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
    } else {
        $overallPassed = $false
        Write-Error "Service status after $elapsed seconds: $($svc.Status). Failed to start service '$ServiceName'."
        Write-Warning "Check logs at '$LogDir' or Windows Event Viewer (Application log) for diagnostics."
    }
}

# ==============================================================================
# 10. Summary Banner
# ==============================================================================
if ($overallPassed) {
    Write-Host "`n========================================================" -ForegroundColor Green
    Write-Host "  OxideSwarm Service Installation Succeeded" -ForegroundColor Green
    Write-Host "========================================================" -ForegroundColor Green
    Write-Host "  Service Name:    $ServiceName"
    Write-Host "  Display Name:    $DisplayName"
    Write-Host "  Startup Type:    Automatic (Boot-time Session 0)"
    Write-Host "  Binary Path:     $destWorkerBin"
    Write-Host "  Service Wrapper: $serviceExePath"
    Write-Host "  Configuration:   $ConfigPath"
    Write-Host "  Logs Directory:  $LogDir"
    Write-Host "  Data Sandbox:    $DataDir"
    Write-Host "  Master Node:     $Master"
    Write-Host "========================================================`n" -ForegroundColor Green
    Write-Host "To verify installation, run:"
    Write-Host "  powershell -ExecutionPolicy Bypass -File .\verify_windows_service.ps1`n" -ForegroundColor Cyan
} else {
    Write-Error "OxideSwarm service installation completed with errors (service failed to start)."
    exit 1
}
