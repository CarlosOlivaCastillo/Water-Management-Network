# =============================================================================
# 2_LAB_OPERARIOS.ps1 - ORDENADOR DE OPERARIOS DE CAMPO (WM_FO)
# =============================================================================
$host.UI.RawUI.WindowTitle = "PANEL CONTROL OPERARIOS (PC LAB)"
$projectDir = $PSScriptRoot
if (-not $projectDir) { $projectDir = (Get-Location).Path }
$cp = "lib/WaterManagement.jar;.;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar"

Clear-Host
Write-Host "=================================================================" -ForegroundColor Magenta
Write-Host "   ORDENADOR DE OPERARIOS DE CAMPO (WM_FO 1 y WM_FO 2)           " -ForegroundColor Magenta
Write-Host "=================================================================" -ForegroundColor Magenta

$IpAws = Read-Host "Introduce la IP de Amazon AWS (o pulsa ENTER para localhost)"
if ([string]::IsNullOrWhiteSpace($IpAws)) {
    $IpAws = "localhost"
} else {
    $IpAws = $IpAws.Trim()
}

function Lanzar-Operario($idFO, $argExtra = "") {
    $titulo = "[FIELD OPERATOR $idFO] - CONSOLA OPERARIO"
    $cmd = "`$host.UI.RawUI.WindowTitle = '$titulo'; Write-Host '================================================' -ForegroundColor Magenta; Write-Host '   $titulo' -ForegroundColor Magenta; Write-Host '   Comandos: LISTAR | WS_01#10 | STOP#WS_01 | FILE#tareas.txt' -ForegroundColor Cyan; Write-Host '================================================' -ForegroundColor Magenta; java -cp '$cp' WM_FO $IpAws 9092 $idFO $argExtra"
    Start-Process powershell -WorkingDirectory $projectDir -ArgumentList "-NoExit", "-Command", $cmd
}

Write-Host "`n-> Abriendo ventana para FIELD OPERATOR FO_01..." -ForegroundColor Magenta
Lanzar-Operario "FO_01"

Start-Sleep -Seconds 1
Write-Host "-> Abriendo ventana para FIELD OPERATOR FO_02..." -ForegroundColor Magenta
Lanzar-Operario "FO_02"

while ($true) {
    Write-Host "`n=================================================================" -ForegroundColor Magenta
    Write-Host "   ACCIONES RAPIDAS OPERARIOS (Conectado a Kafka en $IpAws:9092)" -ForegroundColor Magenta
    Write-Host "=================================================================" -ForegroundColor Magenta
    Write-Host "  1 : Reabrir FO_01 (ej. tras cerrarlo a mitad de riego para ver recuperacion)" -ForegroundColor White
    Write-Host "  2 : Reabrir FO_02" -ForegroundColor White
    Write-Host "  3 : Lanzar tercer operario FO_03" -ForegroundColor White
    Write-Host "  4 : Lanzar FO_01 ejecutando automaticamente tareas.txt (sin tocar teclado)" -ForegroundColor Yellow
    Write-Host "  0 : Salir de este menu" -ForegroundColor Gray
    Write-Host "=================================================================" -ForegroundColor Magenta

    $op = Read-Host "Selecciona opcion"
    if ($op -eq "1") {
        Lanzar-Operario "FO_01"
    } elseif ($op -eq "2") {
        Lanzar-Operario "FO_02"
    } elseif ($op -eq "3") {
        Lanzar-Operario "FO_03"
    } elseif ($op -eq "4") {
        Lanzar-Operario "FO_01" "tareas.txt"
    } elseif ($op -eq "0") {
        break
    }
}
