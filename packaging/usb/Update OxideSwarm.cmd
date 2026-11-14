@echo off
rem Double-click on Windows: updates the OxideSwarm worker this stick installed, in place.
rem Keeps its folder, name, GPU choice and cluster membership; nothing is asked. Then offers to
rem eject the stick.
setlocal
set PSModulePath=
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" -Update %*
set status=%errorlevel%
echo.
rem Offer to eject the stick: the installed worker does not run from it. cmd reads this file line by
rem line from the stick, so everything after the question is one line, parsed before the eject.
set drive=%~d0
cd /d "%SystemRoot%"
choice /c YN /n /m "Eject the stick (%drive%) so it can be removed safely? [Y/N] "
if errorlevel 2 (echo. & pause & exit /b %status%) else (powershell -NoProfile -Command "$i = (New-Object -ComObject Shell.Application).Namespace(17).ParseName('%drive%\'); if ($i) { $i.InvokeVerb('Eject') }" & echo Remove the stick once Windows says it is safe, or use Safely Remove Hardware. & echo. & pause & exit /b %status%)
