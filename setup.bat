@echo off
REM Restreaming Automation - one-click setup
REM Runs the PowerShell setup script so you never have to open PowerShell yourself.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\setup.ps1"
echo.
echo (You can close this window now)
pause
