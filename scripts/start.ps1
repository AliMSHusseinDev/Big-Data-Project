$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
function Check-Exit($step) {
    if ($LASTEXITCODE -ne 0) { throw "$step failed. Inspect docker compose ps -a and docker compose logs --tail 80." }
}
function Test-Engine {
    try {
        docker info --format "{{.ServerVersion}}" 2>$null | Out-Null
        return ($LASTEXITCODE -eq 0)
    } catch { return $false }
}
try {
    Write-Host "Restaurant Kitchen Order Monitoring"
    Get-Command docker -ErrorAction Stop | Out-Null
    docker compose version
    Check-Exit "Docker Compose"
    if (!(Test-Engine)) {
        $desktop = Join-Path $env:ProgramFiles "Docker\Docker\Docker Desktop.exe"
        Start-Process $desktop
        $deadline = (Get-Date).AddMinutes(3)
        do {
            Start-Sleep 3
        } until ((Test-Engine) -or (Get-Date) -gt $deadline)
        if (!(Test-Engine)) { throw "Docker Engine did not become ready within three minutes" }
    }
    if (!(Test-Path ".env")) {
        $bytes = New-Object byte[] 20
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        $rng.GetBytes($bytes)
        $rng.Dispose()
        $password = "K9a!" + ([BitConverter]::ToString($bytes)).Replace("-","")
        [System.IO.File]::WriteAllText((Join-Path (Get-Location) ".env"), "MSSQL_SA_PASSWORD=$password" + [Environment]::NewLine)
    }
    docker compose config --quiet
    Check-Exit "Compose configuration"
    docker compose build
    Check-Exit "Image build"
    docker compose up -d --wait --wait-timeout 600
    Check-Exit "Service startup"
    docker compose exec -T dashboard python -m scripts.readiness
    Check-Exit "Pipeline readiness"
    $pages = [ordered]@{"Dashboard"="http://localhost:8501"; "NiFi canvas"="http://localhost:8081/nifi/";
                        "Spark application"="http://localhost:4040"; "Kafka UI"="http://localhost:8085"}
    foreach ($entry in $pages.GetEnumerator()) {
        $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 15 -Uri $entry.Value
        Write-Host ("[OK] {0}   {1}   HTTP {2}" -f $entry.Key,$entry.Value,$response.StatusCode)
    }
    Write-Host "SQL Server (SSMS)   localhost,1433   RestaurantDB"
    Start-Process $pages.Dashboard
} catch {
    Write-Error $_ -ErrorAction Continue
    exit 1
}
