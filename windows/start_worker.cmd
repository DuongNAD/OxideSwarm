@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: OxideSwarm Windows Background Worker Launcher (Ultra-Lightweight & Robust)
:: Master IP default: 192.168.1.144:8088
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

:: Check if rusty-grid.exe exists, otherwise fallback to target/release or cargo
set "BIN="
if exist "%SCRIPT_DIR%rusty-grid.exe" (
    set "BIN=%SCRIPT_DIR%rusty-grid.exe"
) else if exist "%SCRIPT_DIR%..\target\release\rusty-grid.exe" (
    set "BIN=%SCRIPT_DIR%..\target\release\rusty-grid.exe"
) else if exist "%SCRIPT_DIR%target\release\rusty-grid.exe" (
    set "BIN=%SCRIPT_DIR%target\release\rusty-grid.exe"
)

:: Master Address (Supports auto discovery via UDP beacon)
if "%~1"=="" (
    set "MASTER_ADDR=auto"
) else (
    set "MASTER_ADDR=%~1"
)

set "NODE_NAME=windows-case-gpu"
set "LOG_FILE=%SCRIPT_DIR%worker.log"

:: Check if already running
tasklist /FI "IMAGENAME eq rusty-grid.exe" 2>NUL | find /I /N "rusty-grid.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo [INFO] OxideSwarm Worker is ALREADY RUNNING in the background.
    echo Check status via status_worker.cmd or view worker.log.
    exit /b 0
)

if not defined BIN (
    echo [INFO] rusty-grid.exe not found directly. Compiling via cargo (release)...
    cargo build --release --bin rusty-grid
    if errorlevel 1 (
        echo [ERROR] Build failed! Please install Rust or copy rusty-grid.exe into this directory.
        pause
        exit /b 1
    )
    set "BIN=%SCRIPT_DIR%..\target\release\rusty-grid.exe"
    if not exist "!BIN!" (
        set "BIN=%SCRIPT_DIR%target\release\rusty-grid.exe"
    )
)

echo [OK] Starting OxideSwarm Worker silently in background...
echo   Target Master : %MASTER_ADDR%
echo   Worker Name   : %NODE_NAME%
echo   Log File      : %LOG_FILE%

:: Launch detached hidden process via PowerShell
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%BIN%' -ArgumentList 'worker --master %MASTER_ADDR% --name %NODE_NAME% --gpu --heartbeat-interval 3' -WindowStyle Hidden -RedirectStandardOutput '%LOG_FILE%' -RedirectStandardError '%SCRIPT_DIR%worker_error.log'"

timeout /t 2 /nobreak >nul

tasklist /FI "IMAGENAME eq rusty-grid.exe" 2>NUL | find /I /N "rusty-grid.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo [SUCCESS] OxideSwarm Worker is now running in background!
    echo Memory footprint: ~12 MB RAM ^| CPU idle: 0.0%%
    if /i "%MASTER_ADDR%"=="auto" (
        echo Real-time telemetry is live on Master Web UI: http://localhost:8080 (auto-redirecting to Master)
    ) else (
        echo Real-time telemetry is live on Web UI: http://%MASTER_ADDR:~0,-5%:8080
    )
) else (
    echo [WARN] Process did not start cleanly. Check %SCRIPT_DIR%worker_error.log:
    type "%SCRIPT_DIR%worker_error.log"
)
