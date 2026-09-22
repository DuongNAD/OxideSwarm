<#
.SYNOPSIS
    Verifies that the OxideSwarm Worker service is correctly registered and configured in Windows SCM.

.DESCRIPTION
    Automated verification suite for OxideSwarm Windows Background Service.
    Validates:
      1. Service exists in Windows Service Control Manager (SCM).
      2. StartupType / StartMode is configured for Automatic (Auto) startup on boot.
      3. Service state is Running (or valid transitioning state).
      4. BinaryPathName points to a valid, existing wrapper or executable.
      5. Failure recovery actions are configured to restart the service on crash.
      6. Process executes headlessly in Session 0 without an interactive desktop window.
      7. Log files are generated in the specified log directory.

    Returns exit code 0 if all critical checks pass, non-zero if any check fails.
    Supports both human-readable console output and structured JSON output for CI/CD automation.

.PARAMETER ServiceName
    The name of the Windows Service to query. Default: "OxideSwarmWorker"

.PARAMETER LogDir
    Directory path where logs are expected. Default: "C:\ProgramData\OxideSwarm\logs"

.PARAMETER Format
    Output format: "Text" (default, color-coded) or "Json" (structured JSON).

.PARAMETER TimeoutSec
    Seconds to wait for service to transition if currently starting. Default: 5

.PARAMETER RequireRunning
    If set, verification fails if the service is currently Stopped. Default: $true

.EXAMPLE
    .\verify_windows_service.ps1
    .\verify_windows_service.ps1 -Format Json
#>

[CmdletBinding()]
param (
    [Parameter()]
    [string]$ServiceName = "OxideSwarmWorker",

    [Parameter()]
    [string]$LogDir = "C:\ProgramData\OxideSwarm\logs",

    [Parameter()]
    [ValidateSet("Text", "Json")]
    [string]$Format = "Text",

    [Parameter()]
    [int]$TimeoutSec = 5,

    [Parameter()]
    [bool]$RequireRunning = $true
)

$ErrorActionPreference = "Continue"

if ($LogDir) { $LogDir = $LogDir.TrimEnd('\') }

$checks = [System.Collections.Generic.List[PSObject]]::new()
$failCount = 0
$warnCount = 0

function Add-CheckResult {
    param (
        [string]$TestId,
        [string]$Name,
        [bool]$Passed,
        [string]$Severity, # "Error" or "Warning"
        [string]$Details,
        [hashtable]$Metadata = @{}
    )

    $obj = [PSCustomObject]@{
        TestId   = $TestId
        Name     = $Name
        Passed   = $Passed
        Severity = $Severity
        Details  = $Details
        Metadata = $Metadata
    }

    $script:checks.Add($obj)

    if (-not $Passed) {
        if ($Severity -eq "Error") {
            $script:failCount++
        } else {
            $script:warnCount++
        }
    }
}

# ==============================================================================
# Helper: Extract executable path from SCM BinaryPathName string
# ==============================================================================
function Get-ExecutableFromPathName {
    param ([string]$PathName)

    if (-not $PathName) { return "" }
    $trimmed = $PathName.Trim()

    if ($trimmed.StartsWith('"')) {
        $secondQuote = $trimmed.IndexOf('"', 1)
        if ($secondQuote -gt 1) {
            return $trimmed.Substring(1, $secondQuote - 1)
        }
    }

    # Unquoted space separated path
    $parts = $trimmed.Split(' ')
    return $parts[0]
}

# Wait for service stabilization if starting
$initialSvc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($initialSvc -and $initialSvc.Status -eq 'StartPending') {
    $elapsed = 0
    while ($elapsed -lt $TimeoutSec) {
        Start-Sleep -Seconds 1
        $elapsed++
        $initialSvc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($initialSvc.Status -ne 'StartPending') { break }
    }
}

# ==============================================================================
# Test 1: SCM Registration via Get-Service
# ==============================================================================
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if ($svc) {
    Add-CheckResult -TestId "SCM-01" -Name "SCM Registration Exists" -Passed $true -Severity "Error" `
        -Details "Service '$ServiceName' found in SCM. DisplayName: '$($svc.DisplayName)', Status: '$($svc.Status)', StartType: '$($svc.StartType)'." `
        -Metadata @{ Name = $svc.Name; DisplayName = $svc.DisplayName; Status = "$($svc.Status)"; StartType = "$($svc.StartType)" }
} else {
    Add-CheckResult -TestId "SCM-01" -Name "SCM Registration Exists" -Passed $false -Severity "Error" `
        -Details "Service '$ServiceName' is NOT registered in the Windows Service Control Manager."
}

# ==============================================================================
# Test 2: CIM / WMI Service Configuration (Win32_Service)
# ==============================================================================
$cim = Get-CimInstance -ClassName Win32_Service -Filter "Name='$ServiceName'" -ErrorAction SilentlyContinue

if ($cim) {
    # 2a. StartMode Check (Automatic / Auto)
    $isAutoStart = ($cim.StartMode -eq "Auto")
    Add-CheckResult -TestId "SCM-02" -Name "Automatic Startup Configuration" -Passed $isAutoStart -Severity "Error" `
        -Details "StartMode is '$($cim.StartMode)' (Expected: 'Auto')." `
        -Metadata @{ StartMode = $cim.StartMode }

    # 2b. State / Status Check
    $isRunning = ($cim.State -eq "Running")
    $stateSeverity = if ($RequireRunning) { "Error" } else { "Warning" }
    Add-CheckResult -TestId "SCM-03" -Name "Service State (Running)" -Passed $isRunning -Severity $stateSeverity `
        -Details "Current service state is '$($cim.State)', ProcessId: $($cim.ProcessId), ExitCode: $($cim.ExitCode)." `
        -Metadata @{ State = $cim.State; ProcessId = $cim.ProcessId; ExitCode = $cim.ExitCode }

    # 2c. BinaryPathName Check
    $rawPath = $cim.PathName
    $exePath = Get-ExecutableFromPathName $rawPath
    $exeExists = $false
    if ($exePath -and (Test-Path $exePath)) {
        $exeExists = $true
    }

    Add-CheckResult -TestId "SCM-04" -Name "Service Binary Path Valid" -Passed $exeExists -Severity "Error" `
        -Details "BinaryPathName: '$rawPath'. Resolved executable: '$exePath' (Exists: $exeExists)." `
        -Metadata @{ RawPathName = $rawPath; ExecutablePath = $exePath; Exists = $exeExists }
} else {
    Add-CheckResult -TestId "SCM-02" -Name "Automatic Startup Configuration" -Passed $false -Severity "Error" `
        -Details "Could not query Win32_Service for '$ServiceName'."
    Add-CheckResult -TestId "SCM-03" -Name "Service State (Running)" -Passed $false -Severity "Error" `
        -Details "Could not query Win32_Service state."
    Add-CheckResult -TestId "SCM-04" -Name "Service Binary Path Valid" -Passed $false -Severity "Error" `
        -Details "Could not query Win32_Service PathName."
}

# ==============================================================================
# Test 3: Low-Level SCM Verification (sc.exe qc & sc.exe qfailure)
# ==============================================================================
$scQc = sc.exe qc $ServiceName 2>&1 | Out-String
$isScAuto = ($scQc -match "START_TYPE\s+:\s+2\s+AUTO_START")

Add-CheckResult -TestId "SCM-05" -Name "sc.exe Low-Level AUTO_START Verification" -Passed $isScAuto -Severity "Error" `
    -Details $(if ($isScAuto) { "Confirmed START_TYPE: 2 AUTO_START via sc.exe qc." } else { "sc.exe qc output did not confirm AUTO_START." })

# Check failure recovery configuration
$scFail = sc.exe qfailure $ServiceName 2>&1 | Out-String
$hasFailureRecovery = ($scFail -match "RESTART")

Add-CheckResult -TestId "SCM-06" -Name "SCM Crash Recovery Actions" -Passed $hasFailureRecovery -Severity "Warning" `
    -Details $(if ($hasFailureRecovery) { "Auto-restart recovery actions confirmed configured via sc.exe qfailure." } else { "No restart failure action detected via sc.exe qfailure." })

# ==============================================================================
# Test 4: Headless Session 0 Isolation Check
# ==============================================================================
$workerProcesses = @(Get-Process -Name "rusty-grid", "oxideswarm" -ErrorAction SilentlyContinue)
$wrapperProcesses = @(Get-Process -Name "OxideSwarmService", "$ServiceName" -ErrorAction SilentlyContinue)

$allRelated = $workerProcesses + $wrapperProcesses
$sessionZeroCount = 0
$guiCount = 0
$totalRelated = $allRelated.Count

if ($totalRelated -gt 0) {
    foreach ($p in $allRelated) {
        if ($p.SessionId -eq 0) {
            $sessionZeroCount++
        }
        if ($p.MainWindowHandle -and $p.MainWindowHandle -ne 0) {
            $guiCount++
        }
    }

    $isSessionZero = ($sessionZeroCount -gt 0 -and $sessionZeroCount -eq $totalRelated)
    $isHeadless = ($guiCount -eq 0)

    Add-CheckResult -TestId "ISO-01" -Name "Session 0 Headless Execution" -Passed ($isSessionZero -and $isHeadless) -Severity "Error" `
        -Details "Detected $totalRelated process(es): $sessionZeroCount in Session 0 (Session ID 0), $guiCount interactive window handles." `
        -Metadata @{ SessionZeroCount = $sessionZeroCount; GuiWindowCount = $guiCount; TotalProcesses = $totalRelated }
} else {
    $procPassed = (-not $RequireRunning)
    $procSeverity = if ($RequireRunning) { "Error" } else { "Warning" }
    Add-CheckResult -TestId "ISO-01" -Name "Session 0 Headless Execution" -Passed $procPassed -Severity $procSeverity `
        -Details "No active 'rusty-grid' or service wrapper processes currently detected in process table."
}

# ==============================================================================
# Test 5: Log File Output Generation Check
# ==============================================================================
$logDirExists = Test-Path $LogDir
$logFilesFound = 0
$logFileDetails = @()

if ($logDirExists) {
    $foundFiles = Get-ChildItem -Path $LogDir -Filter "*.log" -ErrorAction SilentlyContinue
    if ($foundFiles) {
        $logFilesFound = $foundFiles.Count
        foreach ($f in $foundFiles) {
            $logFileDetails += "$($f.Name) ($($f.Length) bytes, LastWrite: $($f.LastWriteTime))"
        }
    }
}

$logsValid = ($logDirExists -and ($logFilesFound -gt 0 -or -not $RequireRunning))
Add-CheckResult -TestId "LOG-01" -Name "Log Directory and Output Streams" -Passed $logsValid -Severity "Warning" `
    -Details "Log directory '$LogDir' (Exists: $logDirExists, Log files found: $logFilesFound). $([string]::Join('; ', $logFileDetails))" `
    -Metadata @{ LogDirExists = $logDirExists; LogFilesFound = $logFilesFound }

# ==============================================================================
# Output Formatting
# ==============================================================================
$overallPassed = ($failCount -eq 0)

if ($Format -eq "Json") {
    $report = [PSCustomObject]@{
        Timestamp     = (Get-Date -Format "o")
        ServiceName   = $ServiceName
        OverallPassed = $overallPassed
        FailCount     = $failCount
        WarningCount  = $warnCount
        ServiceInfo   = if ($cim) {
            @{
                Name        = $cim.Name
                DisplayName = $cim.DisplayName
                State       = $cim.State
                StartMode   = $cim.StartMode
                PathName    = $cim.PathName
                ProcessId   = $cim.ProcessId
                ExitCode    = $cim.ExitCode
            }
        } else { $null }
        Checks        = $checks
    }

    $report | ConvertTo-Json -Depth 5
} else {
    Write-Host "`n==================================================================" -ForegroundColor Cyan
    Write-Host "  OxideSwarm Windows Service Verification Suite" -ForegroundColor Cyan
    Write-Host "  Target Service: $ServiceName" -ForegroundColor Cyan
    Write-Host "==================================================================`n" -ForegroundColor Cyan

    foreach ($c in $checks) {
        $statusBadge = if ($c.Passed) { "[PASS]" } elseif ($c.Severity -eq "Warning") { "[WARN]" } else { "[FAIL]" }
        $color = if ($c.Passed) { "Green" } elseif ($c.Severity -eq "Warning") { "Yellow" } else { "Red" }

        Write-Host "  $statusBadge " -NoNewline -ForegroundColor $color
        Write-Host "$($c.TestId) - $($c.Name)" -ForegroundColor White
        Write-Host "         $($c.Details)" -ForegroundColor Gray
    }

    Write-Host "`n------------------------------------------------------------------" -ForegroundColor Cyan
    if ($overallPassed) {
        Write-Host "  RESULT: VERIFICATION PASSED ($($checks.Count) checks executed, $warnCount warning(s))" -ForegroundColor Green
    } else {
        Write-Host "  RESULT: VERIFICATION FAILED ($failCount error(s), $warnCount warning(s))" -ForegroundColor Red
    }
    Write-Host "==================================================================`n" -ForegroundColor Cyan
}

if ($overallPassed) {
    exit 0
} else {
    exit 1
}
