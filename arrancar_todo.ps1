# Script para compilar y arrancar Water Management Network en 4 terminales independientes

$projectDir = $PSScriptRoot
if (-not $projectDir) {
    $projectDir = (Get-Location).Path
}

Clear-Host
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "   INICIANDO WATER MANAGEMENT NETWORK (4 TERMINALES)     " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Compilar todas las clases Java antes de arrancar
Write-Host "`n[1/5] Compilando clases Java..." -ForegroundColor Yellow
javac -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" *.java
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Falló la compilación de Java. Revisa los errores." -ForegroundColor Red
    exit 1
}
Write-Host "-> Compilación correcta." -ForegroundColor Green

# 2. Terminal 1: Central de Gestión (Sockets + Kafka + Web Dashboard)
Write-Host "[2/5] Lanzando Terminal 1: Central de Gestión (WM_Central)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$projectDir'; Write-Host '=== CENTRAL DE GESTIÓN (WM_Central) ===' -ForegroundColor Cyan; java -cp '.;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar' WM_Central 8080 localhost 9092"

Start-Sleep -Seconds 2

# 3. Terminal 2: Monitor de la Estación (WM_WS_M)
Write-Host "[3/5] Lanzando Terminal 2: Monitor WS_01 (WM_WS_M)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$projectDir'; Write-Host '=== MONITOR DE ESTACIÓN (WM_WS_M) ===' -ForegroundColor Yellow; java WM_WS_M 5000 localhost 8080 WS_01"

Start-Sleep -Seconds 2

# 4. Terminal 3: Engine de la Estación (WM_WS_E)
Write-Host "[4/5] Lanzando Terminal 3: Engine WS_01 (WM_WS_E)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$projectDir'; Write-Host '=== ENGINE DE ESTACIÓN (WM_WS_E) ===' -ForegroundColor Green; java -cp '.;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar' WM_WS_E localhost 9092 localhost 5000 WS_01"

Start-Sleep -Seconds 2

# 5. Terminal 4: Field Operator (WM_FO - Modo Interactivo)
Write-Host "[5/5] Lanzando Terminal 4: Field Operator (WM_FO)..." -ForegroundColor Magenta
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$projectDir'; Write-Host '=== FIELD OPERATOR (WM_FO) ===' -ForegroundColor Magenta; java -cp '.;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar' WM_FO localhost 9092 FO_01"

Write-Host "`n[OK] Las 4 terminales han sido iniciadas con éxito." -ForegroundColor Green
Write-Host "🌐 Puedes abrir el Dashboard Web en: http://localhost:8081/" -ForegroundColor Cyan
