<#
    Restreaming Automation - Start API server
#>

$ErrorActionPreference = "Stop"

Write-Host "=== Starting Restreaming Automation ===" -ForegroundColor Cyan

# Activate venv
if (Test-Path ".\venv\Scripts\Activate.ps1") {
    & .\venv\Scripts\Activate.ps1
}

# Start Python backend
Write-Host "Starting Python API server..." -ForegroundColor Green
$apiJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    & .\venv\Scripts\python.exe -m src
}

Write-Host @"

Services started:
  - API Backend  -> http://localhost:8008        (Job: $($apiJob.Id))
  - Dashboard    -> http://localhost:8008/dashboard
  - API Docs     -> http://localhost:8008/docs

Press Ctrl+C to stop.
"@ -ForegroundColor White

try {
    while ($true) {
        Start-Sleep -Seconds 2
        if ($apiJob.State -eq 'Failed') {
            Write-Host "API job failed:" -ForegroundColor Red
            Receive-Job $apiJob
        }
    }
} finally {
    Write-Host "`nStopping services..." -ForegroundColor Yellow
    Stop-Job $apiJob -ErrorAction SilentlyContinue
    Remove-Job $apiJob -Force -ErrorAction SilentlyContinue
    Write-Host "All services stopped." -ForegroundColor Green
}
