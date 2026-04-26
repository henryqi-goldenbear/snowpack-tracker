$ErrorActionPreference = "Stop"

Write-Host "Starting Postgres via docker compose..."
docker compose up -d

Write-Host "Waiting for Postgres healthcheck..."
$started = Get-Date
while ($true) {
  $status = docker inspect --format='{{.State.Health.Status}}' snowpack-tracker-postgres 2>$null
  if ($status -eq "healthy") { break }
  if (((Get-Date) - $started).TotalSeconds -gt 120) {
    throw "Timed out waiting for Postgres to become healthy. Current status: $status"
  }
  Start-Sleep -Seconds 2
}

$env:DATABASE_URL = "postgresql://snowpack:snowpack@localhost:54321/snowpack"
$env:PYTHONPATH = (Resolve-Path .deps)

Write-Host "Running LivePostgresTest..."
& "C:\Users\zhiha\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" run_live_postgres_integration.py

