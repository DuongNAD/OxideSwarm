@echo off
rem Double-click on Windows: runs uninstall.ps1 from this folder.
setlocal
set PSModulePath=
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0uninstall.ps1" %*
echo.
pause
