<#
    Restreaming Automation - Windows Setup Script
    Run from the project root in PowerShell.
#>

$ErrorActionPreference = "Stop"

# Always run from the project root, no matter how the script was launched
# (e.g. right-click -> Run with PowerShell starts in the scripts\ folder)
Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "=== Restreaming Automation - Setup ===" -ForegroundColor Cyan

# ---- Check prerequisites ----
function Test-Command($cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

$missing = @()
if (-not (Test-Command "python"))     { $missing += "Python 3.10+  ->  winget install Python.Python.3.12    (or python.org - tick 'Add python.exe to PATH')" }
if (-not (Test-Command "ffmpeg"))     { $missing += "FFmpeg        ->  winget install Gyan.FFmpeg" }
if (-not (Test-Command "streamlink")) { $missing += "Streamlink    ->  winget install Streamlink.Streamlink (or the installer at streamlink.github.io/install.html)" }

if ($missing.Count -gt 0) {
    Write-Host "`nMissing prerequisites:" -ForegroundColor Yellow
    $missing | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "`nInstall the above, close and reopen PowerShell, then re-run this script." -ForegroundColor Yellow
    exit 1
}

Write-Host "All prerequisites found." -ForegroundColor Green

# ---- Python environment ----
Write-Host "`n[1/2] Setting up Python virtual environment..." -ForegroundColor Cyan
if (-not (Test-Path "venv")) {
    python -m venv venv
}
& .\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"

# ---- Env file ----
Write-Host "`n[2/2] Environment configuration..." -ForegroundColor Cyan
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example - edit it with your OBS WebSocket password." -ForegroundColor Yellow
} else {
    Write-Host ".env already exists, skipping." -ForegroundColor Green
}

Write-Host "`n=== Setup complete! ===" -ForegroundColor Green
Write-Host @"

Next steps:
  1. Edit .env with your OBS WebSocket password (open it in Notepad)
  2. Start the server:    double-click start.bat   (or .\scripts\start.ps1)
  3. Open dashboard:      http://localhost:8008/dashboard

"@ -ForegroundColor White
