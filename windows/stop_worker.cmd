@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: Stop OxideSwarm Background Worker
:: ==============================================================================

echo [INFO] Stopping OxideSwarm Worker process...
tasklist /FI "IMAGENAME eq rusty-grid.exe" 2>NUL | find /I /N "rusty-grid.exe">NUL
if "%ERRORLEVEL%"=="0" (
    taskkill /F /T /IM rusty-grid.exe >nul 2>&1
    echo [OK] OxideSwarm Worker has been stopped successfully.
) else (
    echo [INFO] No running OxideSwarm Worker process found.
)
timeout /t 2 >nul
