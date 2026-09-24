@echo off
setlocal enabledelayedexpansion
title OxideSwarm Cross-Machine Network Protocol (Windows)

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "PYTHONDONTWRITEBYTECODE=1"

set "PYTHON_CMD="
where py >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON_CMD=py -3"
) else (
    where python >nul 2>&1
    if %errorlevel% equ 0 (
        set "PYTHON_CMD=python"
    )
)

if "%PYTHON_CMD%"=="" (
    echo [ERROR] Python 3 was not found in PATH.
    echo Please install Python 3 or add it to PATH.
    pause
    exit /b 1
)

echo [INFO] Launching sync_network.py via %PYTHON_CMD%...
%PYTHON_CMD% -B "%SCRIPT_DIR%sync_network.py" %*
set "EXIT_CODE=%errorlevel%"

if %EXIT_CODE% equ 0 (
    echo [OK] Execution completed successfully.
) else (
    echo [WARN] Execution terminated with code %EXIT_CODE%.
)
exit /b %EXIT_CODE%
