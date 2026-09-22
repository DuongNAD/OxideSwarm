@echo off
setlocal EnableDelayedExpansion
title OxideSwarm 1-Click Remote Worker Pairing (Windows)

echo ========================================================
echo   OxideSwarm 1-Click Remote Worker Pairing (Windows)
echo ========================================================
echo.
echo Seamlessly connects this Windows PC as an OxideSwarm worker
echo node across the internet via native P2P NAT Traversal (iroh QUIC).
echo.

:: 1. Extract or Prompt for P2P Ticket
set "TICKET=%~1"

:: Check for existing configuration if no ticket argument passed
if "%TICKET%"=="" (
    set "SAVED_CONFIG=%PROGRAMDATA%\OxideSwarm\rusty-grid.toml"
    if not exist "!SAVED_CONFIG!" (
        set "SAVED_CONFIG=%USERPROFILE%\.oxideswarm\rusty-grid.toml"
    )
    if exist "!SAVED_CONFIG!" (
        for /f "tokens=2 delims='" %%A in ('findstr /i "p2p_ticket" "!SAVED_CONFIG!" 2^>nul') do (
            set "SAVED_TICKET=%%A"
        )
        if "!SAVED_TICKET!"=="" (
            for /f "tokens=2 delims=^"" %%B in ('findstr /i "p2p_ticket" "!SAVED_CONFIG!" 2^>nul') do (
                set "SAVED_TICKET=%%B"
            )
        )
        if not "!SAVED_TICKET!"=="" (
            echo Found previously saved ticket:
            echo   !SAVED_TICKET:~0,45!...
            set /p "USE_SAVED=Use saved ticket? [Y/n]: "
            if /i "!USE_SAVED!"=="" set "USE_SAVED=Y"
            if /i "!USE_SAVED!"=="Y" set "TICKET=!SAVED_TICKET!"
        )
    )
)

if "%TICKET%"=="" (
    set /p "TICKET=Paste your OxideSwarm P2P Ticket from Master: "
)

:: Trim spaces and quotes
set "TICKET=%TICKET:"=%"
if "%TICKET%"=="" (
    echo [ERROR] No P2P ticket provided. Exiting.
    pause
    exit /b 1
)

:: 2. Validate Ticket Format
echo "%TICKET%" | findstr /i "\"id\"" >nul 2>&1
if %errorlevel% neq 0 (
    echo "%TICKET%" | findstr /i "id:" >nul 2>&1
    if %errorlevel% neq 0 (
        echo [ERROR] Invalid ticket format! Expected JSON containing an "id" field.
        pause
        exit /b 1
    )
)
echo [OK] Validated P2P Ticket format.

:: 3. Administrator Privilege Verification & Self-Elevation
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [INFO] Requesting Administrator privileges to register Windows Background Service...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd.exe -ArgumentList '/c \"\"%~f0\" \"!TICKET!\"\"' -Verb RunAs"
    exit /b 0
)

:: 4. Locate and Execute Windows Service Installer
set "SCRIPT_DIR=%~dp0"
set "INSTALLER=%SCRIPT_DIR%packaging\windows\install_windows_service.ps1"

if exist "%INSTALLER%" (
    echo.
    echo [INFO] Installing OxideSwarm as a native Windows Background Service...
    powershell -NoProfile -ExecutionPolicy Bypass -File "%INSTALLER%" -P2pTicket "!TICKET!" -StartImmediately $true
    set "PS_EXIT=!errorlevel!"

    if !PS_EXIT! equ 0 (
        echo.
        echo ========================================================
        echo [OK] OxideSwarm Worker successfully paired and running!
        echo - Service Name  : OxideSwarmWorker
        echo - Startup Type  : Automatic (starts on boot in Session 0)
        echo - NAT Traversal : Native iroh QUIC + Relay
        echo - Auto-Reconnect: Active (Exponential backoff ^< 3s)
        echo ========================================================
    ) else (
        echo.
        echo [WARN] Windows Service registration encountered a non-zero exit code (!PS_EXIT!).
        goto :FALLBACK_FOREGROUND
    )
) else (
    echo.
    echo [WARN] Windows Service installer not found at %INSTALLER%.
    goto :FALLBACK_FOREGROUND
)

echo.
pause
exit /b 0

:FALLBACK_FOREGROUND
echo [INFO] Launching worker directly in foreground mode...
set "BIN="
if exist "%SCRIPT_DIR%rusty-grid.exe" set "BIN=%SCRIPT_DIR%rusty-grid.exe"
if exist "%SCRIPT_DIR%target\release\rusty-grid.exe" set "BIN=%SCRIPT_DIR%target\release\rusty-grid.exe"
if exist "%SCRIPT_DIR%target\debug\rusty-grid.exe" set "BIN=%SCRIPT_DIR%target\debug\rusty-grid.exe"

if not "!BIN!"=="" (
    echo Starting worker process: !BIN!
    "!BIN!" worker --p2p-ticket "!TICKET!"
) else (
    echo [ERROR] Could not locate rusty-grid.exe binary.
    echo Please compile via 'cargo build --release --bin rusty-grid' or place rusty-grid.exe in this folder.
)

echo.
pause
exit /b 1
