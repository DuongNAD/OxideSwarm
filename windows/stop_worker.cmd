@echo off
setlocal EnableExtensions

rem ==============================================================================
rem  Stop what start_worker.cmd / start_master.cmd started (tracked by PID file).
rem  A Windows service installed with install_service.cmd is NOT touched: stop it
rem  with "Stop-Service OxideSwarmWorker" from an elevated PowerShell.
rem ==============================================================================

set "SCRIPT_DIR=%~dp0"
set "OX_TARGET=%~1"
if "%OX_TARGET%"=="" set "OX_TARGET=all"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%oxctl.ps1" stop %OX_TARGET%
set "OX_EXIT=%errorlevel%"
timeout /t 2 >nul
exit /b %OX_EXIT%
