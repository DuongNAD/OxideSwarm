@echo off
rem Double-click on Windows: updates the OxideSwarm worker this stick installed, in place.
rem Keeps its folder, name, GPU choice and cluster membership; nothing is asked.
setlocal
set PSModulePath=
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" -Update %*
echo.
pause
