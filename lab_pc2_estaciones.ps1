# =============================================================================
# SCRIPT PARA ORDENADOR 2 (PC LAB 2): ESTACIONES DE RIEGO (WM_WS_M + WM_WS_E)
# =============================================================================
param(
    [string]$IpCentral = ""
)

$projectDir = $PSScriptRoot
if (-not $projectDir) { $projectDir = (Get-Location).Path }

Clear-Host
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "   ORDENADOR 2: ESTACIONES DE RIEGO (WS_01 y WS_02)              " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

if ([string]::IsNullOrWhiteSpace($IpCentral)) {
    $IpCentral = Read-Host "Introduce la IP de AWS o del Ordenador 1 (donde corre Central y Kafka)"
    if ([string]::IsNullOrWhiteSpace($IpCentral)) {
        $IpCentral = "localhost"
    } else {
        $IpCentral = $IpCentral.Trim()
    }
}

Write-Host "`n-> Conectando estaciones contra Central ($IpCentral:8080) y Kafka ($IpCentral:9092)`n" -ForegroundColor Green

$modo = Read-Host "¿Cómo deseas lanzar las estaciones? [1 = Docker Compose (Recomendado), 2 = Terminales Java interactivas]"

if ($modo -eq "2") {
    $cp = ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar"
    Write-Host "`n[1/3] Compilando clases Java..." -ForegroundColor Yellow
    javac -encoding UTF-8 -cp $cp *.java

    Write-Host "[2/3] Lanzando Estación WS_01 (Monitor :5001 + Engine)..." -ForegroundColor Green
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== MONITOR WS_01 ===' -ForegroundColor Yellow; java -cp '$cp' WM_WS_M 5001 $IpCentral 8080 WS_01"
    Start-Sleep -Seconds 1
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== ENGINE WS_01 (Pulsa ENTER para simular FUGA KO / OK) ===' -ForegroundColor Green; java -cp '$cp' WM_WS_E $IpCentral 9092 localhost 5001 WS_01"

    Write-Host "[3/3] Lanzando Estación WS_02 (Monitor :5002 + Engine)..." -ForegroundColor Green
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== MONITOR WS_02 ===' -ForegroundColor Yellow; java -cp '$cp' WM_WS_M 5002 $IpCentral 8080 WS_02"
    Start-Sleep -Seconds 1
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== ENGINE WS_02 (Pulsa ENTER para simular FUGA KO / OK) ===' -ForegroundColor Green; java -cp '$cp' WM_WS_E $IpCentral 9092 localhost 5002 WS_02"
} else {
    $env:IP_CENTRAL = $IpCentral
    $env:PUERTO_CENTRAL = "8080"
    $env:IP_KAFKA = $IpCentral
    $env:PUERTO_KAFKA = "9092"

    Write-Host "`n[1/1] Levantando contenedores de WS_01 y WS_02 con Docker Compose..." -ForegroundColor Green
    docker compose -f docker-compose.lab-ws.yml up -d --build
    Write-Host "`n💡 TRUCO: Para simular una fuga (KO) en WS_01 pulsando ENTER, ejecuta:" -ForegroundColor Yellow
    Write-Host "   docker attach ws01_engine" -ForegroundColor White
}

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host " [OK] ESTACIONES WS_01 y WS_02 CONECTADAS A $IpCentral" -ForegroundColor Green
Write-Host " -> Verifica que están en VERDE (DISPONIBLE) en: http://${IpCentral}:8081" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Green
