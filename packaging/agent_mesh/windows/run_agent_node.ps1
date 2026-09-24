<#
.SYNOPSIS
    OxideSwarm Agent Node Runner for Windows (PowerShell)
.DESCRIPTION
    Launches an OxideSwarm Agent Node on Windows 10/11 or Windows Server.
    Connects outbound to the OxideRelay Hub via WebSocket (ws:// or wss://).
    Prioritizes native Rust binary ('agent-mesh.exe' or 'rusty-grid.exe') with automatic fallback to Python client.
.PARAMETER Hub
    WebSocket URL of the OxideRelay Hub (default: ws://127.0.0.1:8088/ws)
.PARAMETER NodeId
    Unique identifier for this node (default: node-windows-<hostname>)
.PARAMETER BinaryPath
    Explicit path to executable. If omitted, searches target directories.
#>
param(
    [Parameter(Mandatory=$false)]
    [string]$Hub = "ws://127.0.0.1:8088/ws",

    [Parameter(Mandatory=$false)]
    [string]$NodeId = ("node-windows-" + $env:COMPUTERNAME.ToLower()),

    [Parameter(Mandatory=$false)]
    [string]$BinaryPath = ""
)

$ErrorActionPreference = "Continue"

Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "   OxideSwarm Windows Agent Node Runner                " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "Node ID:  $NodeId" -ForegroundColor Yellow
Write-Host "Hub URL:  $Hub" -ForegroundColor Yellow
Write-Host "Platform: Windows ($([System.Environment]::OSVersion.VersionString))" -ForegroundColor Gray
Write-Host ""

# 1. Resolve Native Binary
if (-not $BinaryPath) {
    $Candidates = @(
        ".\target\release\agent-mesh.exe",
        ".\target\debug\agent-mesh.exe",
        "..\..\target\release\agent-mesh.exe",
        "..\..\target\debug\agent-mesh.exe",
        ".\target\release\rusty-grid.exe",
        ".\target\debug\rusty-grid.exe",
        "..\..\target\release\rusty-grid.exe",
        "..\..\target\debug\rusty-grid.exe"
    )

    foreach ($Cand in $Candidates) {
        if (Test-Path $Cand) {
            $BinaryPath = (Resolve-Path $Cand).Path
            break
        }
    }
}

# 2. Execution Loop with Auto-Restart
while ($true) {
    if ($BinaryPath -and (Test-Path $BinaryPath)) {
        Write-Host "[INFO] Starting native agent: $BinaryPath node --hub $Hub --id $NodeId --platform windows" -ForegroundColor Green
        try {
            & $BinaryPath node --hub $Hub --id $NodeId --platform windows
        } catch {
            Write-Warning "Agent process exited: $_"
        }
    } else {
        # Fallback to Python Script
        $ScriptPath = "scripts\agent_node.py"
        if (-not (Test-Path $ScriptPath)) {
            $ScriptPath = "..\..\scripts\agent_node.py"
        }

        Write-Host "[INFO] Native binary not found. Launching Python agent node fallback ($ScriptPath)..." -ForegroundColor Green
        try {
            python $ScriptPath --hub $Hub --id $NodeId --platform windows
        } catch {
            Write-Warning "Python agent terminated: $_"
        }
    }

    Write-Host "[WARN] Node disconnected. Reconnecting in 3 seconds (Ctrl+C to abort)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 3
}
