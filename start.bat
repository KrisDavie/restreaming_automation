@echo off
REM Restreaming Automation - one-click start
REM Starts the server. Keep this window open while using the dashboard:
REM   http://localhost:8008/dashboard
REM Press Ctrl+C in this window (or just close it) to stop the server.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\start.ps1"
echo.
echo Server stopped.
pause
