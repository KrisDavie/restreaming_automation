<#
    Restreaming Automation – Start all services
    Launches the Python API backend and NodeCG in parallel.
#>

$ErrorActionPreference = "Stop"

Write-Host "=== Starting Restreaming Automation ===" -ForegroundColor Cyan

# Activate venv
if (Test-Path ".\venv\Scripts\Activate.ps1") {
    & .\venv\Scripts\Activate.ps1
}

# Start Python backend
Write-Host "Starting Python API server…" -ForegroundColor Green
$apiJob = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    & .\venv\Scripts\python.exe -m src
}

# Start NodeCG
Write-Host "Starting NodeCG…" -ForegroundColor Green
$nodecgJob = Start-Job -ScriptBlock {
    Set-Location (Join-Path $using:PWD "nodecg")
    node index.js
}

Write-Host @"

Services started:
  - API Backend  → http://localhost:8008        (Job: $($apiJob.Id))
  - NodeCG       → http://localhost:9090        (Job: $($nodecgJob.Id))
  - API Docs     → http://localhost:8008/docs

Press Ctrl+C to stop all services.
"@ -ForegroundColor White

try {
    # Keep running until interrupted
    while ($true) {
        Start-Sleep -Seconds 2
        # Check if jobs are still running
        @($apiJob, $nodecgJob) | ForEach-Object {
            if ($_.State -eq 'Failed') {
                Write-Host "Job $($_.Id) failed:" -ForegroundColor Red
                Receive-Job $_
            }
        }
    }
} finally {
    Write-Host "`nStopping services…" -ForegroundColor Yellow
    Stop-Job $apiJob -ErrorAction SilentlyContinue
    Stop-Job $nodecgJob -ErrorAction SilentlyContinue
    Remove-Job $apiJob -Force -ErrorAction SilentlyContinue
    Remove-Job $nodecgJob -Force -ErrorAction SilentlyContinue
    Write-Host "All services stopped." -ForegroundColor Green
}
