<# 
    Restreaming Automation – Windows Setup Script
    Run from the project root as Administrator.
#>

$ErrorActionPreference = "Stop"

Write-Host "=== Restreaming Automation – Setup ===" -ForegroundColor Cyan

# ---- Check prerequisites ----
function Test-Command($cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

$missing = @()
if (-not (Test-Command "python"))    { $missing += "Python 3.10+ (https://python.org)" }
if (-not (Test-Command "node"))      { $missing += "Node.js 18+ (https://nodejs.org)" }
if (-not (Test-Command "ffmpeg"))    { $missing += "FFmpeg (https://ffmpeg.org)" }
if (-not (Test-Command "streamlink")){ $missing += "Streamlink (pip install streamlink)" }

if ($missing.Count -gt 0) {
    Write-Host "`nMissing prerequisites:" -ForegroundColor Yellow
    $missing | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "`nInstall the above and re-run this script." -ForegroundColor Yellow
    exit 1
}

Write-Host "All prerequisites found." -ForegroundColor Green

# ---- Python environment ----
Write-Host "`n[1/4] Setting up Python virtual environment…" -ForegroundColor Cyan
if (-not (Test-Path "venv")) {
    python -m venv venv
}
& .\venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -e ".[dev]"

# ---- Node dependencies ----
Write-Host "`n[2/4] Installing Node dependencies…" -ForegroundColor Cyan
npm install

# ---- NodeCG ----
Write-Host "`n[3/4] Setting up NodeCG…" -ForegroundColor Cyan
if (-not (Test-Path "nodecg\package.json")) {
    npx nodecg-cli setup ./nodecg
}
Push-Location nodecg
npm install
Pop-Location

# ---- Env file ----
Write-Host "`n[4/4] Environment configuration…" -ForegroundColor Cyan
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example – edit it with your OBS WebSocket password." -ForegroundColor Yellow
} else {
    Write-Host ".env already exists, skipping." -ForegroundColor Green
}

Write-Host "`n=== Setup complete! ===" -ForegroundColor Green
Write-Host @"

Next steps:
  1. Edit .env with your OBS WebSocket password
  2. Place template images (hearts.png etc.) in ./templates/
  3. Start the backend:   python -m src
  4. Start NodeCG:        cd nodecg && node index.js
  5. Open dashboard:      http://localhost:9090

"@ -ForegroundColor White
