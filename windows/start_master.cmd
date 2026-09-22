@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: OxideSwarm Windows Background MASTER Launcher
:: Binds Cluster on 0.0.0.0:8088 | Web UI on 0.0.0.0:8080
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "BIN="
if exist "%SCRIPT_DIR%rusty-grid.exe" (
    set "BIN=%SCRIPT_DIR%rusty-grid.exe"
) else if exist "%SCRIPT_DIR%..\target\release\rusty-grid.exe" (
    set "BIN=%SCRIPT_DIR%..\target\release\rusty-grid.exe"
) else if exist "%SCRIPT_DIR%target\release\rusty-grid.exe" (
    set "BIN=%SCRIPT_DIR%target\release\rusty-grid.exe"
)

if not defined BIN (
    echo [INFO] Compiling rusty-grid master via cargo...
    cargo build --release --bin rusty-grid
    set "BIN=%SCRIPT_DIR%..\target\release\rusty-grid.exe"
)

set "LOG_FILE=%SCRIPT_DIR%master.log"

echo [OK] Starting OxideSwarm Master silently in background...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%BIN%' -ArgumentList 'master --listen 0.0.0.0:8088 --web-ui-addr 0.0.0.0:8080' -WindowStyle Hidden -RedirectStandardOutput '%LOG_FILE%' -RedirectStandardError '%SCRIPT_DIR%master_error.log'"

timeout /t 2 /nobreak >nul
echo [SUCCESS] OxideSwarm Master is running! Open http://localhost:8080
