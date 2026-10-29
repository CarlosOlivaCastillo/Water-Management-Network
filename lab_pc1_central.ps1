# =============================================================================
# SCRIPT PARA ORDENADOR 1 (O CLOUD LOCAL): CENTRAL + KAFKA + SQLITE + WEB
# =============================================================================
param(
    [string]$IpHost = ""
)

$projectDir = $PSScriptRoot
if (-not $projectDir) { $projectDir = (Get-Location).Path }

Clear-Host
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "   ORDENADOR 1: WM_Central + Apache Kafka + SQLite + Web (:8081) " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# Auto-detectar IP local de red (LAN) si no se ha pasado por parámetro
if ([string]::IsNullOrWhiteSpace($IpHost)) {
    $detectedIp = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notmatch "Loopback|vEthernet|WSL|Docker" -and $_.IPAddress -notmatch "^169\.254" } | Select-Object -First 1).IPAddress
    if (-not $detectedIp) { $detectedIp = "localhost" }
    $inputIp = Read-Host "Introduce la IP de este PC para anunciar en Kafka [Enter para usar $detectedIp]"
    if ([string]::IsNullOrWhiteSpace($inputIp)) {
        $IpHost = $detectedIp
    } else {
        $IpHost = $inputIp.Trim()
    }
}

Write-Host "`n-> Usando IP del servidor Central / Kafka: $IpHost" -ForegroundColor Green
Write-Host "   (Apunta esta IP: la necesitarás en el Ordenador 2 y Ordenador 3)`n" -ForegroundColor Yellow

$modo = Read-Host "¿Cómo deseas arrancar Central + Kafka? [1 = Docker Compose (Recomendado), 2 = Kafka en Docker + Central en Java]"

if ($modo -eq "2") {
    Write-Host "`n[1/3] Levantando Broker Kafka en Docker anunciando $IpHost:9092..." -ForegroundColor Yellow
    docker rm -f kafka_lab 2>$null | Out-Null
    docker run -d --name kafka_lab -p 9092:9092 `
      -e KAFKA_HEAP_OPTS="-Xmx512M -Xms512M" `
      -e KAFKA_CFG_NODE_ID=0 `
      -e KAFKA_CFG_PROCESS_ROLES=controller,broker `
      -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 `
      -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT `
      -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 `
      -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER `
      -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://${IpHost}:9092 `
      bitnamilegacy/kafka:3.7.0

    Start-Sleep -Seconds 5
    $cp = ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar"
    Write-Host "[2/3] Compilando código Java..." -ForegroundColor Yellow
    javac -encoding UTF-8 -cp $cp *.java

    Write-Host "[3/3] Iniciando WM_Central en nueva ventana..." -ForegroundColor Green
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", "Write-Host '=== WM_CENTRAL ($IpHost) ===' -ForegroundColor Cyan; java -cp '$cp' WM_Central 8080 $IpHost 9092 8081"
} else {
    $env:IP_PUBLICA_HOST = $IpHost
    Write-Host "`n[1/1] Levantando docker-compose.cloud.yml con IP_PUBLICA_HOST=$IpHost..." -ForegroundColor Green
    docker compose -f docker-compose.cloud.yml up -d --build
}

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host " [OK] ORDENADOR 1 LISTO" -ForegroundColor Green
Write-Host " -> IP para poner en PC 2 y PC 3 : $IpHost" -ForegroundColor Yellow
Write-Host " -> Puerto Sockets Central       : 8080" -ForegroundColor Cyan
Write-Host " -> Puerto Kafka                 : 9092" -ForegroundColor Cyan
Write-Host " -> Dashboard Web en Tiempo Real : http://${IpHost}:8081" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Green
