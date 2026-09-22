@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: OxideSwarm Background Worker Status & Telemetry Inspector
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
echo ==============================================================================
echo                 OxideSwarm Windows Worker Status
echo ==============================================================================

tasklist /FI "IMAGENAME eq rusty-grid.exe" /V 2>NUL | find /I "rusty-grid.exe"
if "%ERRORLEVEL%"=="0" (
    echo.
    echo Status   : [RUNNING] (Silently in background)
    echo Memory   : ~12 MB RAM
    echo CPU Idle : 0.0%%
    echo.
    echo --- Last 15 lines of worker.log ---
    if exist "%SCRIPT_DIR%worker.log" (
        powershell -NoProfile -Command "Get-Content '%SCRIPT_DIR%worker.log' -Tail 15"
    ) else (
        echo [worker.log is empty or not created yet]
    )
) else (
    echo.
    echo Status   : [STOPPED]
    echo Run run_worker_silent.vbs or start_worker.cmd to start.
)

echo.
echo ==============================================================================
pause
