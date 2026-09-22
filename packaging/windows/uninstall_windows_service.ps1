<#
.SYNOPSIS
    Uninstalls and deregisters the OxideSwarm Windows Background Service.

.DESCRIPTION
    Stops the OxideSwarmWorker service, cleanly removes it from Windows Service
    Control Manager (SCM), removes wrapper and binary files, and optionally purges
    all logs, configuration, and data sandboxes.

.PARAMETER ServiceName
    SCM service identifier name. Default: "OxideSwarmWorker"

.PARAMETER InstallDir
    Target installation directory. Default: "C:\Program Files\OxideSwarm"

.PARAMETER LogDir
    Directory where log files are stored. Default: "C:\ProgramData\OxideSwarm\logs"

.PARAMETER DataDir
    Directory where sandbox data is stored. Default: "C:\ProgramData\OxideSwarm\data"

.PARAMETER ConfigPath
    Path to configuration file. Default: "C:\ProgramData\OxideSwarm\rusty-grid.toml"

.PARAMETER PurgeData
    If specified, deletes all log files, configuration files, and data directories.
    If omitted, logs and configurations are safely preserved.

.PARAMETER Force
    Forcefully terminates any worker processes if the service fails to stop gracefully.

.PARAMETER NoElevate
    Do not attempt automatic elevation if running without Administrator privileges.

.EXAMPLE
    .\uninstall_windows_service.ps1
    .\uninstall_windows_service.ps1 -PurgeData -Force
#>

[CmdletBinding()]
param (
    [Parameter()]
    [string]$ServiceName = "OxideSwarmWorker",

    [Parameter()]
    [string]$InstallDir = "C:\Program Files\OxideSwarm",

    [Parameter()]
    [string]$LogDir = "C:\ProgramData\OxideSwarm\logs",

    [Parameter()]
    [string]$DataDir = "C:\ProgramData\OxideSwarm\data",

    [Parameter()]
    [string]$ConfigPath = "C:\ProgramData\OxideSwarm\rusty-grid.toml",

    [Parameter()]
    [switch]$PurgeData,

    [Parameter()]
    [switch]$Force,

    [Parameter()]
    [switch]$NoElevate
)

$ErrorActionPreference = "Stop"

# Trim trailing backslashes in path arguments before string quoting to prevent \" escaping issues under Windows CommandLineToArgvW
if ($InstallDir) { $InstallDir = $InstallDir.TrimEnd('\') }
if ($LogDir) { $LogDir = $LogDir.TrimEnd('\') }
if ($DataDir) { $DataDir = $DataDir.TrimEnd('\') }
if ($ConfigPath) { $ConfigPath = $ConfigPath.TrimEnd('\') }

# ==============================================================================
# 1. Administrator Elevation Check
# ==============================================================================
function Assert-Administrator {
    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
    $isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

    if (-not $isAdmin) {
        if (-not $NoElevate -and [Environment]::UserInteractive) {
            Write-Host "[INFO] Requesting Administrator privileges to uninstall Windows Service..." -ForegroundColor Yellow
            $scriptPath = $MyInvocation.PSCommandPath
            if (-not $scriptPath) {
                $scriptPath = $PSCommandPath
            }

            $argList = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
            foreach ($key in $script:PSBoundParameters.Keys) {
                $val = $script:PSBoundParameters[$key]
                if ($val -is [System.Management.Automation.SwitchParameter]) {
                    if ($val.IsPresent) { $argList += " -$key" }
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
            Write-Error "Administrator privileges are required to deregister Windows Services. Please launch PowerShell as Administrator."
            exit 1
        }
    }
}

Assert-Administrator

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host "  OxideSwarm Windows Background Service Uninstaller" -ForegroundColor Cyan
Write-Host "========================================================`n" -ForegroundColor Cyan

# ==============================================================================
# 2. Stop Service if Running
# ==============================================================================
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if ($svc) {
    Write-Host "[INFO] Found service '$ServiceName' in SCM (Status: $($svc.Status))."

    if ($svc.Status -ne 'Stopped') {
        Write-Host "[INFO] Stopping service '$ServiceName'..." -ForegroundColor Yellow
        try {
            Stop-Service -Name $ServiceName -Force -ErrorAction Stop
        } catch {
            Write-Warning "Stop-Service reported: $($_.Exception.Message)"
        }

        # Wait up to 10 seconds for process to exit cleanly
        $waitSec = 10
        $elapsed = 0
        while ($elapsed -lt $waitSec) {
            Start-Sleep -Seconds 1
            $elapsed++
            $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
            if (-not $svc -or $svc.Status -eq 'Stopped') {
                break
            }
        }

        if ($svc -and $svc.Status -ne 'Stopped') {
            if ($Force) {
                Write-Host "[WARN] Service did not stop within $waitSec seconds. Forcing termination of worker processes..." -ForegroundColor Red
                Get-Process -Name "rusty-grid", "oxideswarm", "OxideSwarmService" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
                Start-Sleep -Seconds 1
            } else {
                Write-Warning "Service '$ServiceName' is still running. Pass -Force to terminate forcefully."
            }
        } else {
            Write-Host "[OK] Service '$ServiceName' stopped cleanly." -ForegroundColor Green
        }
    }
} else {
    Write-Host "[INFO] Service '$ServiceName' is not currently registered in SCM."
}

# ==============================================================================
# 3. Unregister / Delete Service from SCM
# ==============================================================================
if ($svc) {
    Write-Host "[INFO] Deregistering service '$ServiceName' from SCM..."

    # Check for WinSW wrapper uninstallation
    $winswExe = Join-Path $InstallDir "$ServiceName.exe"
    $winswXml = Join-Path $InstallDir "$ServiceName.xml"
    $uninstalledViaWinSW = $false

    if ((Test-Path $winswExe) -and (Test-Path $winswXml)) {
        try {
            Write-Host "[INFO] Executing WinSW uninstall..."
            $uOut = & "$winswExe" uninstall 2>&1 | Out-String
            Write-Host $uOut
            $uninstalledViaWinSW = $true
        } catch {
            Write-Warning "WinSW uninstall failed, falling back to sc.exe delete: $($_.Exception.Message)"
        }
    }

    if (-not $uninstalledViaWinSW) {
        $deleteOut = sc.exe delete $ServiceName 2>&1 | Out-String
        Write-Host $deleteOut
    }

    # Verify removal
    Start-Sleep -Seconds 1
    $verifySvc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if (-not $verifySvc) {
        Write-Host "[OK] Service '$ServiceName' removed from Service Control Manager." -ForegroundColor Green
    } else {
        Write-Warning "Service '$ServiceName' is marked for deletion and will be removed once all open handles close."
    }
}

# ==============================================================================
# 4. Clean System Environment Variables
# ==============================================================================
Write-Host "[INFO] Removing system environment variables..."
try {
    [Environment]::SetEnvironmentVariable("OXIDESWARM_BIN", $null, [EnvironmentVariableTarget]::Machine)
    [Environment]::SetEnvironmentVariable("OXIDESWARM_CONFIG", $null, [EnvironmentVariableTarget]::Machine)
    [Environment]::SetEnvironmentVariable("OXIDESWARM_LOG_DIR", $null, [EnvironmentVariableTarget]::Machine)
    Write-Host "[OK] Environment variables cleaned." -ForegroundColor Green
} catch {
    Write-Warning "Could not clear environment variables: $($_.Exception.Message)"
}

# ==============================================================================
# 5. Clean Installation & Data Files
# ==============================================================================
if (Test-Path $InstallDir) {
    if ($PurgeData) {
        Write-Host "[WARN] -PurgeData specified. Removing complete installation directory: $InstallDir" -ForegroundColor Yellow
        try {
            Remove-Item -Path $InstallDir -Recurse -Force -ErrorAction Stop
            Write-Host "[OK] Removed installation directory: $InstallDir" -ForegroundColor Green
        } catch {
            Write-Warning "Could not remove entire install directory: $($_.Exception.Message)"
        }
    } else {
        Write-Host "[INFO] Cleaning application binaries while preserving configuration and logs..."
        $binDir = Join-Path $InstallDir "bin"
        if (Test-Path $binDir) {
            Remove-Item -Path $binDir -Recurse -Force -ErrorAction SilentlyContinue
        }

        # Remove wrapper executables
        Remove-Item -Path (Join-Path $InstallDir "$ServiceName.exe") -Force -ErrorAction SilentlyContinue
        Remove-Item -Path (Join-Path $InstallDir "OxideSwarmService.exe") -Force -ErrorAction SilentlyContinue
        Remove-Item -Path (Join-Path $InstallDir "$ServiceName.xml") -Force -ErrorAction SilentlyContinue
        Remove-Item -Path (Join-Path $InstallDir "ServiceWrapper.cs") -Force -ErrorAction SilentlyContinue

        Write-Host "[OK] Binaries removed from $InstallDir." -ForegroundColor Green
        Write-Host "[INFO] Preserved config ($ConfigPath) and logs ($LogDir)." -ForegroundColor Cyan
        Write-Host "       Pass -PurgeData to permanently remove all log files and data sandboxes." -ForegroundColor Cyan
    }
}

if ($PurgeData) {
    if (Test-Path $LogDir) {
        Write-Host "[WARN] Purging log directory: $LogDir" -ForegroundColor Yellow
        Remove-Item -Path $LogDir -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "[OK] Log directory purged." -ForegroundColor Green
    }

    if (Test-Path $DataDir) {
        Write-Host "[WARN] Purging data sandbox directory: $DataDir" -ForegroundColor Yellow
        Remove-Item -Path $DataDir -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "[OK] Data sandbox directory purged." -ForegroundColor Green
    }

    $programDataOxide = "C:\ProgramData\OxideSwarm"
    if (Test-Path $programDataOxide) {
        Remove-Item -Path $programDataOxide -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "[OK] Purged $programDataOxide." -ForegroundColor Green
    }
}

# ==============================================================================
# 6. Summary
# ==============================================================================
Write-Host "`n========================================================" -ForegroundColor Green
Write-Host "  OxideSwarm Windows Service Uninstallation Complete" -ForegroundColor Green
Write-Host "========================================================`n" -ForegroundColor Green
