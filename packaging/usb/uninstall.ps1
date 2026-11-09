# OxideSwarm USB uninstaller for Windows (kit format v0).
# Stops the worker this kit installed, removes its HKCU Run entry and its folder (binaries, the
# cluster secret it received, logs). Nothing else is touched.
#   powershell -ExecutionPolicy Bypass -File uninstall.ps1 [-InstallRoot DIR] [-Yes]
param(
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA "OxideSwarm"),
    [switch]$Yes
)
$ErrorActionPreference = 'Stop'
$Exe = Join-Path $InstallRoot 'bin\oxideswarm.exe'
if (-not (Test-Path $InstallRoot)) { Write-Host "Nothing to remove: $InstallRoot does not exist."; exit 0 }
if (-not (Test-Path $Exe)) { Write-Host "ERROR: $InstallRoot does not look like a USB-kit install; not touching it." -ForegroundColor Red; exit 1 }
if (-not $Yes) {
    $ans = Read-Host "Remove the OxideSwarm worker installed in $InstallRoot? [y/N]"
    if ($ans -notmatch '^(y|yes)$') { Write-Host "Nothing removed."; exit 0 }
}
Remove-ItemProperty -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run' -Name 'OxideSwarmUsbWorker' -ErrorAction SilentlyContinue
# Stop the worker by the exact path of this install's binary (never by process name).
Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -eq $Exe } | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds 2
Remove-Item -Recurse -Force $InstallRoot
Write-Host "Removed. The cluster owner can also remove this machine from the cluster."
