$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    python -m unittest discover app/ingest/tests
}
finally {
    Pop-Location
}
