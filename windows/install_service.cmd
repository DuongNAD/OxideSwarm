@echo off
:: ==============================================================================
:: OxideSwarm Windows Service 1-Click Installer (Auto-Elevate & Register in SCM)
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

echo [INFO] Requesting Administrator elevation to install Windows Service...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process powershell -ArgumentList '-NoProfile -ExecutionPolicy Bypass -File \"%SCRIPT_DIR%..\packaging\windows\install_windows_service.ps1\" -Master 192.168.1.144:8088 -WorkerName windows-case-gpu -Gpu' -Verb RunAs"
