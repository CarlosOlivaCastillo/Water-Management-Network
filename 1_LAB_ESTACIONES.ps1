# =============================================================================
# 1_LAB_ESTACIONES.ps1 - ORDENADOR DE ESTACIONES DE RIEGO (WM_WS)
# =============================================================================
$host.UI.RawUI.WindowTitle = "PANEL CONTROL ESTACIONES (PC LAB)"
$projectDir = $PSScriptRoot
if (-not $projectDir) { $projectDir = (Get-Location).Path }
$cp = ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar"

Clear-Host
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "   ORDENADOR DE ESTACIONES DE RIEGO (WM_WS_M y WM_WS_E)          " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

$IpAws = Read-Host "Introduce la IP de Amazon AWS (o pulsa ENTER para localhost)"
if ([string]::IsNullOrWhiteSpace($IpAws)) {
    $IpAws = "localhost"
} else {
    $IpAws = $IpAws.Trim()
}

function Lanzar-Monitor($idWS, $puertoLocal) {
    $titulo = "[MONITOR $idWS] - Puerto $puertoLocal"
    $cmd = "`$host.UI.RawUI.WindowTitle = '$titulo'; Write-Host '================================================' -ForegroundColor Yellow; Write-Host '   $titulo' -ForegroundColor Yellow; Write-Host '================================================' -ForegroundColor Yellow; java -cp '$cp' WM_WS_M $puertoLocal $IpAws 8080 $idWS"
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", $cmd
}

function Lanzar-Engine($idWS, $puertoLocal) {
    $titulo = "[ENGINE $idWS] - PULSA ENTER PARA FUGA"
    $cmd = "`$host.UI.RawUI.WindowTitle = '$titulo'; Write-Host '================================================' -ForegroundColor Green; Write-Host '   $titulo' -ForegroundColor Green; Write-Host '================================================' -ForegroundColor Green; java -cp '$cp' WM_WS_E $IpAws 9092 localhost $puertoLocal $idWS"
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", $cmd
}

Write-Host "`n-> Abriendo Panel Web de Central en http://${IpAws}:8081 ..." -ForegroundColor Cyan
Start-Process "http://${IpAws}:8081"

Write-Host "-> Lanzando Estacion WS_01 (Puerto 5001)..." -ForegroundColor Green
Lanzar-Monitor "WS_01" 5001
Start-Sleep -Seconds 1
Lanzar-Engine "WS_01" 5001

Start-Sleep -Seconds 1
Write-Host "-> Lanzando Estacion WS_02 (Puerto 5002)..." -ForegroundColor Green
Lanzar-Monitor "WS_02" 5002
Start-Sleep -Seconds 1
Lanzar-Engine "WS_02" 5002

while ($true) {
    Write-Host "`n=================================================================" -ForegroundColor Cyan
    Write-Host "   ACCIONES RAPIDAS PARA LA CORRECCION (Conectado a $IpAws)" -ForegroundColor Cyan
    Write-Host "=================================================================" -ForegroundColor Cyan
    Write-Host "  1 : Desplegar nueva estacion WS_03 (Puerto 5003)" -ForegroundColor White
    Write-Host "  2 : Probar estacion NO registrada WS_99 (Muestra rechazo <NACK>)" -ForegroundColor Yellow
    Write-Host "  3 : Reabrir SOLO un Monitor caido (ej. tras cerrarlo con Ctrl+C)" -ForegroundColor White
    Write-Host "  4 : Reabrir SOLO un Engine caido  (ej. tras cerrarlo con Ctrl+C)" -ForegroundColor White
    Write-Host "  5 : Desplegar otra estacion personalizada (WS_04, WS_05...)" -ForegroundColor White
    Write-Host "  0 : Salir de este menu" -ForegroundColor Gray
    Write-Host "=================================================================" -ForegroundColor Cyan

    $op = Read-Host "Selecciona opcion"
    if ($op -eq "1") {
        Lanzar-Monitor "WS_03" 5003
        Start-Sleep -Seconds 1
        Lanzar-Engine "WS_03" 5003
    } elseif ($op -eq "2") {
        Lanzar-Monitor "WS_99" 5099
    } elseif ($op -eq "3") {
        $ws = Read-Host "ID de la estacion cuyo Monitor quieres reabrir (ej. WS_01)"
        $puerto = Read-Host "Puerto local de esa estacion (5001 para WS_01, 5002 para WS_02, 5003 para WS_03)"
        Lanzar-Monitor $ws.Trim() $puerto.Trim()
    } elseif ($op -eq "4") {
        $ws = Read-Host "ID de la estacion cuyo Engine quieres reabrir (ej. WS_01)"
        $puerto = Read-Host "Puerto local de esa estacion (5001 para WS_01, 5002 para WS_02, 5003 para WS_03)"
        Lanzar-Engine $ws.Trim() $puerto.Trim()
    } elseif ($op -eq "5") {
        $ws = Read-Host "ID de la estacion (ej. WS_04)"
        $puerto = Read-Host "Puerto local libre (ej. 5004)"
        Lanzar-Monitor $ws.Trim() $puerto.Trim()
        Start-Sleep -Seconds 1
        Lanzar-Engine $ws.Trim() $puerto.Trim()
    } elseif ($op -eq "0") {
        break
    }
}
