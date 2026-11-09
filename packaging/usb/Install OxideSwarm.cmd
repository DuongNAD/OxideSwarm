@echo off
rem Double-click on Windows: runs install.ps1 from this folder for the current user.
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" %*
echo.
pause
