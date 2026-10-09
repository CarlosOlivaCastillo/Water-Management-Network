# Script para levantar las 7 Water Stations completas (Monitores + Engines)

$projectDir = $PSScriptRoot
if (-not $projectDir) {
    $projectDir = (Get-Location).Path
}

Clear-Host
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "     ARRANCANDO LAS 7 ESTACIONES DE RIEGO (WS_01..07)    " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

$cp = ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar"

$estaciones = @(
    @{ id = "WS_01"; port = 5001; loc = "Parque Central - Norte" },
    @{ id = "WS_02"; port = 5002; loc = "Parque Central - Sur" },
    @{ id = "WS_03"; port = 5003; loc = "Jardines del Prado" },
    @{ id = "WS_04"; port = 5004; loc = "Av. de las Palmeras" },
    @{ id = "WS_05"; port = 5005; loc = "Plaza Mayor" },
    @{ id = "WS_06"; port = 5006; loc = "Parque Fluvial Este" },
    @{ id = "WS_07"; port = 5007; loc = "Vivero Municipal" }
)

foreach ($est in $estaciones) {
    $id = $est.id
    $port = $est.port
    $loc = $est.loc

    Write-Host "`n-> Lanzando $id ($loc) en puerto $port..." -ForegroundColor Yellow

    # Monitor
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== MONITOR $id ===' -ForegroundColor Yellow; java -cp '$cp' WM_WS_M $port localhost 8080 $id"
    Start-Sleep -Milliseconds 800

    # Engine
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== ENGINE $id ===' -ForegroundColor Green; java -cp '$cp' WM_WS_E localhost 9092 localhost $port $id"
    Start-Sleep -Milliseconds 800
}

Write-Host "`n[OK] Las 7 estaciones de riego están en marcha y registradas." -ForegroundColor Green
Write-Host "🌐 Abre el Dashboard Web para ver el mapa interactivo: http://localhost:8081/" -ForegroundColor Cyan
