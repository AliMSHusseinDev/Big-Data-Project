# Run from the repository root using PowerShell after the standard image build.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
function Stack {
    docker compose -p kitchen-e2e -f docker-compose.yml -f tests/isolated.yml @args
    if ($LASTEXITCODE -ne 0) { throw "Test stack command failed" }
}
$session = [guid]::NewGuid().ToString()
Stack up -d --wait --wait-timeout 600
try {
    Stack exec -T dashboard python -m scripts.readiness
    Stack exec -T dashboard python -m tests.sql_replay start $session
    Stack restart spark
    Stack exec -T dashboard python -m tests.sql_replay finish $session
    Stack exec -T dashboard python -m tests.full_pipeline
    Stack stop
    Stack up -d --wait --wait-timeout 600
    Stack exec -T dashboard python -m scripts.readiness
    Stack exec -T dashboard python -c "from database.connection import connect; db=connect(); c=db.__enter__(); assert c.execute('SELECT COUNT(*) FROM ProcessedEvents WHERE session_id=?','$session').fetchone()[0]==5"
} finally {
    Stack stop
}
