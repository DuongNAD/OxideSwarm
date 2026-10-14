@echo off
setlocal EnableExtensions

rem ==============================================================================
rem  OxideSwarm Background Worker / Master Status (PID-file based, real values)
rem ==============================================================================

set "SCRIPT_DIR=%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%oxctl.ps1" status
echo.
pause
