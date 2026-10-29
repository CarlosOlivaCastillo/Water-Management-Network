# =============================================================================
# SCRIPT PARA ORDENADOR 3 (PC LAB 1): OPERARIOS DE CAMPO (WM_FO 1 y WM_FO 2)
# =============================================================================
param(
    [string]$IpKafka = ""
)

$projectDir = $PSScriptRoot
if (-not $projectDir) { $projectDir = (Get-Location).Path }

Clear-Host
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "   ORDENADOR 3: OPERARIOS DE CAMPO (FO_01 y FO_02)               " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

if ([string]::IsNullOrWhiteSpace($IpKafka)) {
    $IpKafka = Read-Host "Introduce la IP de AWS o del Ordenador 1 (donde corre Kafka)"
    if ([string]::IsNullOrWhiteSpace($IpKafka)) {
        $IpKafka = "localhost"
    } else {
        $IpKafka = $IpKafka.Trim()
    }
}

Write-Host "`n-> Conectando Operarios contra Kafka en $IpKafka:9092`n" -ForegroundColor Green

$modo = Read-Host "¿Cómo deseas lanzar los operarios? [1 = Docker Compose (Recomendado), 2 = Terminales Java interactivas]"

if ($modo -eq "2") {
    $cp = ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar"
    Write-Host "`n[1/2] Compilando clases Java..." -ForegroundColor Yellow
    javac -encoding UTF-8 -cp $cp *.java

    Write-Host "[2/2] Abriendo consolas interactivas para FO_01 y FO_02..." -ForegroundColor Magenta
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== OPERARIO FO_01 (Ej: WS_01#10 o FILE#tareas.txt) ===' -ForegroundColor Magenta; java -cp '$cp' WM_FO $IpKafka 9092 FO_01"
    Start-Sleep -Seconds 1
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== OPERARIO FO_02 (Ej: WS_02#10 o STOP#WS_02) ===' -ForegroundColor Magenta; java -cp '$cp' WM_FO $IpKafka 9092 FO_02"
} else {
    $env:IP_KAFKA = $IpKafka
    $env:PUERTO_KAFKA = "9092"

    Write-Host "`n[1/1] Abriendo consolas Docker para FO_01 y FO_02..." -ForegroundColor Magenta
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "`$env:IP_KAFKA='$IpKafka'; `$env:PUERTO_KAFKA='9092'; docker compose -f docker-compose.lab-fo.yml run --rm --build fo_01"
    Start-Sleep -Seconds 2
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "`$env:IP_KAFKA='$IpKafka'; `$env:PUERTO_KAFKA='9092'; docker compose -f docker-compose.lab-fo.yml run --rm fo_02"
}

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host " [OK] OPERARIOS FO_01 y FO_02 EN EJECUCIÓN" -ForegroundColor Green
Write-Host " Comandos dentro de la consola del operario:" -ForegroundColor Yellow
Write-Host "   WS_01#10        -> Regar WS_01 durante 10 segundos" -ForegroundColor White
Write-Host "   WS_02           -> Regar WS_02 indefinidamente" -ForegroundColor White
Write-Host "   STOP#WS_02      -> Cortar riego manual en WS_02" -ForegroundColor White
Write-Host "   FILE#tareas.txt -> Ejecutar lote automático con pausas de 4s" -ForegroundColor White
Write-Host "=================================================================" -ForegroundColor Green
